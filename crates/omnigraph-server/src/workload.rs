//! Per-actor admission control for the HTTP server (MR-686 §VII.A).
//!
//! The HTTP server's previous global `RwLock<Omnigraph>` serialized every
//! mutating request across all actors. PR 2 removes that lock — engine
//! APIs are now `&self`, so concurrent calls from different actors can
//! run against `Arc<Omnigraph>` simultaneously. Without admission
//! control, one heavy actor can exhaust shared capacity (Lance I/O
//! threads, manifest churn, network) and starve other actors.
//!
//! This module provides:
//!
//! - **Per-actor in-flight count cap**: each actor has a
//!   `tokio::sync::Semaphore` with `OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX`
//!   permits (default 16). `try_acquire_owned()` returns `Err` when
//!   exhausted; the server maps this to HTTP 429.
//!
//! - **Per-actor in-flight byte budget**: each actor accumulates an
//!   `AtomicU64` byte estimate. `fetch_add(est_bytes)` then a check
//!   against `byte_cap` is race-free via decrement-on-rejection. The
//!   server maps an over-budget result to HTTP 429 as well.
//!
//! Counts are governed by the semaphore (race-free `try_acquire_owned()`
//! enforces the cap atomically); bytes use `fetch_add` + decrement-on-
//! rejection. Both checks are atomic compare-and-act, never
//! load-then-act — the test
//! `actor_admission_race_does_not_exceed_cap` pins this contract by
//! spawning 32 concurrent `try_admit` calls against a cap of 16 and
//! asserting exactly 16 succeed.
//!
//! Acquisition order against the engine's per-(table, branch) write
//! queue: admission FIRST (the HTTP handler reserves capacity before
//! calling into the engine), engine queue SECOND (acquired inside
//! `MutationStaging::commit_all`). This composes cleanly because
//! admission is a single per-actor count + budget check, never
//! cross-actor; nothing the engine does can change a peer actor's
//! admission state.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Default per-actor in-flight count cap. Override via
/// `OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX`.
pub const DEFAULT_PER_ACTOR_INFLIGHT_MAX: u32 = 16;

/// Default per-actor in-flight byte budget (4 GiB). Override via
/// `OMNIGRAPH_PER_ACTOR_BYTES_MAX`.
pub const DEFAULT_PER_ACTOR_BYTES_MAX: u64 = 4 * 1024 * 1024 * 1024;

/// Why a `try_admit` call returned `Err`. The server maps each variant
/// to a specific HTTP response code; see `WorkloadController` docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// Actor exceeded the per-actor in-flight count cap. HTTP 429.
    InFlightCountExceeded { cap: u32 },
    /// Actor exceeded the per-actor in-flight byte budget. HTTP 429.
    ByteBudgetExceeded { cap: u64, attempted: u64 },
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::InFlightCountExceeded { cap } => {
                write!(f, "actor in-flight count cap {} exceeded", cap)
            }
            RejectReason::ByteBudgetExceeded { cap, attempted } => write!(
                f,
                "actor byte budget exceeded: would use {} bytes against cap {}",
                attempted, cap
            ),
        }
    }
}

/// Per-actor counters. One instance per actor_id, lazily created on
/// first admission attempt.
#[derive(Debug)]
pub(crate) struct ActorState {
    /// Counts the number of concurrent in-flight requests for this
    /// actor. `try_acquire_owned()` is the count-cap gate.
    in_flight_sem: Arc<Semaphore>,
    /// Total bytes estimated to be in flight for this actor across
    /// concurrent requests. `fetch_add` + check + decrement-on-failure
    /// keeps the cap atomic.
    bytes: AtomicU64,
    /// Per-actor byte cap (snapshot of `WorkloadController.byte_cap`
    /// at construction; cap mutations don't propagate to existing
    /// ActorStates by design — controller config changes apply on
    /// next ActorState construction).
    byte_cap: u64,
    /// Per-actor count cap (same snapshot semantics as `byte_cap`).
    inflight_cap: u32,
}

impl ActorState {
    fn new(inflight_cap: u32, byte_cap: u64) -> Self {
        Self {
            in_flight_sem: Arc::new(Semaphore::new(inflight_cap as usize)),
            bytes: AtomicU64::new(0),
            byte_cap,
            inflight_cap,
        }
    }
}

/// Server-side per-actor admission controller. Constructed once at
/// server startup and shared via `Arc<WorkloadController>` on
/// `AppState`.
pub struct WorkloadController {
    per_actor: DashMap<Arc<str>, Arc<ActorState>>,
    inflight_cap: u32,
    byte_cap: u64,
}

impl WorkloadController {
    /// Construct from explicit caps. Tests can override.
    pub fn new(inflight_cap: u32, byte_cap: u64) -> Self {
        Self {
            per_actor: DashMap::new(),
            inflight_cap,
            byte_cap,
        }
    }

    /// Construct from environment variables, falling back to defaults.
    /// Bad env values fall back to the default with a `tracing::warn!`.
    pub fn from_env() -> Self {
        let inflight_cap = parse_env_u32(
            "OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX",
            DEFAULT_PER_ACTOR_INFLIGHT_MAX,
        );
        let byte_cap = parse_env_u64("OMNIGRAPH_PER_ACTOR_BYTES_MAX", DEFAULT_PER_ACTOR_BYTES_MAX);
        Self::new(inflight_cap, byte_cap)
    }

    /// Construct with default caps. Suitable for tests / single-tenant
    /// deployments without explicit configuration.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_PER_ACTOR_INFLIGHT_MAX, DEFAULT_PER_ACTOR_BYTES_MAX)
    }

    fn actor_state(&self, actor_id: &Arc<str>) -> Arc<ActorState> {
        if let Some(existing) = self.per_actor.get(actor_id) {
            return existing.clone();
        }
        // Race-on-construct is benign: DashMap's `entry().or_insert_with`
        // serializes per-key construction; the loser's freshly-built
        // ActorState gets dropped without observable effect.
        self.per_actor
            .entry(actor_id.clone())
            .or_insert_with(|| Arc::new(ActorState::new(self.inflight_cap, self.byte_cap)))
            .clone()
    }

    /// Reserve admission for one in-flight request from `actor_id`
    /// estimated to consume `est_bytes`. Returns an `AdmissionGuard`
    /// that releases the count permit + decrements the byte total
    /// when dropped.
    ///
    /// On rejection, the byte counter is decremented before returning
    /// — callers can retry without leaking budget.
    pub fn try_admit(
        &self,
        actor_id: &Arc<str>,
        est_bytes: u64,
    ) -> Result<AdmissionGuard, RejectReason> {
        let state = self.actor_state(actor_id);

        // Count gate: race-free via `try_acquire_owned()`. If exhausted,
        // immediately reject — no byte accounting needed for this request.
        let permit = match Arc::clone(&state.in_flight_sem).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return Err(RejectReason::InFlightCountExceeded {
                    cap: state.inflight_cap,
                });
            }
            Err(TryAcquireError::Closed) => {
                return Err(RejectReason::InFlightCountExceeded {
                    cap: state.inflight_cap,
                });
            }
        };

        // Byte gate: atomic fetch_add then check; decrement on overflow.
        // `Ordering::SeqCst` is conservative; per-actor accounting is
        // not on the hot path of read queries.
        let prior = state.bytes.fetch_add(est_bytes, Ordering::SeqCst);
        let attempted = prior.saturating_add(est_bytes);
        if attempted > state.byte_cap {
            // Roll back the byte add. The permit drops with `permit`
            // going out of scope below.
            state.bytes.fetch_sub(est_bytes, Ordering::SeqCst);
            return Err(RejectReason::ByteBudgetExceeded {
                cap: state.byte_cap,
                attempted,
            });
        }

        Ok(AdmissionGuard {
            _permit: permit,
            actor_state: state,
            est_bytes,
        })
    }
}

/// Drop-on-completion guard for an admitted request. Dropping releases
/// the in-flight count permit (via `Drop` on the underlying semaphore
/// permit) and decrements the actor's byte counter.
#[derive(Debug)]
pub struct AdmissionGuard {
    _permit: OwnedSemaphorePermit,
    actor_state: Arc<ActorState>,
    est_bytes: u64,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.actor_state
            .bytes
            .fetch_sub(self.est_bytes, Ordering::SeqCst);
    }
}

fn parse_env_u32(name: &str, default: u32) -> u32 {
    match std::env::var(name) {
        Ok(v) => v.parse::<u32>().unwrap_or_else(|err| {
            tracing::warn!(
                env = name,
                value = %v,
                error = %err,
                default,
                "invalid env value, using default"
            );
            default
        }),
        Err(_) => default,
    }
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(v) => v.parse::<u64>().unwrap_or_else(|err| {
            tracing::warn!(
                env = name,
                value = %v,
                error = %err,
                default,
                "invalid env value, using default"
            );
            default
        }),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn try_admit_admits_under_cap() {
        let controller = WorkloadController::new(2, 1024);
        let actor: Arc<str> = "alice".into();
        let g1 = controller.try_admit(&actor, 100).expect("first admit");
        let _g2 = controller.try_admit(&actor, 100).expect("second admit");
        let err = controller
            .try_admit(&actor, 100)
            .expect_err("third should reject on count");
        assert!(matches!(err, RejectReason::InFlightCountExceeded { cap: 2 }));
        drop(g1);
        // After drop, a new admit succeeds again.
        let _g3 = controller
            .try_admit(&actor, 100)
            .expect("admit after drop");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn byte_budget_caps_admission() {
        let controller = WorkloadController::new(16, 1000);
        let actor: Arc<str> = "alice".into();
        let _g1 = controller.try_admit(&actor, 600).expect("first admit");
        let err = controller
            .try_admit(&actor, 600)
            .expect_err("second should reject on bytes");
        match err {
            RejectReason::ByteBudgetExceeded { cap, attempted } => {
                assert_eq!(cap, 1000);
                assert_eq!(attempted, 1200);
            }
            other => panic!("expected ByteBudgetExceeded, got {:?}", other),
        }
        // Verify the byte counter was rolled back: a smaller request fits.
        let _g2 = controller.try_admit(&actor, 300).expect("smaller admit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn actor_admission_race_does_not_exceed_cap() {
        // Pin master plan §"WorkloadController" Finding 6: independent
        // atomic load + check + add allows two concurrent callers to
        // both pass a cap-N check. The Semaphore-based gate is
        // race-free — exactly cap_count callers succeed.
        //
        // Each task holds its admission guard until released via a
        // oneshot channel; this forces real contention because guards
        // can't drop and free permits before all 32 calls have raced.
        let controller = Arc::new(WorkloadController::new(16, u64::MAX / 4));
        let actor: Arc<str> = "racer".into();

        let (release_tx, _) = tokio::sync::broadcast::channel::<()>(1);

        let mut handles = Vec::with_capacity(32);
        for _ in 0..32 {
            let controller = Arc::clone(&controller);
            let actor = actor.clone();
            let mut release_rx = release_tx.subscribe();
            handles.push(tokio::spawn(async move {
                let result = controller.try_admit(&actor, 1);
                let success = result.is_ok();
                // Hold the guard (if any) until the test signals release,
                // so the cap-16 contention is observable across all 32
                // tasks instead of permits being recycled task-by-task.
                let _guard = result.ok();
                let _ = release_rx.recv().await;
                success
            }));
        }

        // Give all 32 tasks a chance to hit `try_admit` before any can
        // drop their guard. 50ms is plenty for tokio's scheduler on a
        // 4-worker runtime.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Release every task; collect succeed/reject counts.
        let _ = release_tx.send(());

        let mut accepted = 0u32;
        let mut rejected = 0u32;
        for h in handles {
            if h.await.unwrap() {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
        assert_eq!(accepted, 16, "expected exactly 16 successful admits");
        assert_eq!(rejected, 16, "expected exactly 16 rejections");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn per_actor_caps_independent() {
        let controller = WorkloadController::new(1, 1024);
        let alice: Arc<str> = "alice".into();
        let bob: Arc<str> = "bob".into();
        let _ga = controller.try_admit(&alice, 100).expect("alice ok");
        // Alice over count cap, Bob unaffected.
        let err = controller.try_admit(&alice, 100).expect_err("alice rejected");
        assert!(matches!(err, RejectReason::InFlightCountExceeded { .. }));
        let _gb = controller.try_admit(&bob, 100).expect("bob ok");
    }
}
