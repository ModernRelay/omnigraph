//! Immediate admission for bounded server-owned operations and request bodies.
//!
//! One mutex checks and changes aggregate and per-actor counters together.
//! Guards move with operation ownership; disconnect cannot recycle their count
//! or retained-input allowance. Read and write input have independent capacity.
//! Ingress leases are shared by collection and the operation that receives its
//! bytes, and release only with their last owner.
//! These counters bound named server resources, not engine memory or native I/O.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

pub const DEFAULT_PER_ACTOR_INFLIGHT_MAX: u32 = 16;
pub const DEFAULT_PER_ACTOR_BYTES_MAX: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_GLOBAL_INFLIGHT_MAX: u32 = 64;
pub const DEFAULT_GLOBAL_BYTES_MAX: u64 = 256 * 1024 * 1024;
pub const DEFAULT_ACTIVE_ACTORS_MAX: usize = 1024;
pub const DEFAULT_INGRESS_INFLIGHT_MAX: u32 = 64;
pub const DEFAULT_INGRESS_BYTES_MAX: u64 = 256 * 1024 * 1024;
pub const DEFAULT_READ_INGRESS_INFLIGHT_MAX: u32 = 64;
pub const DEFAULT_READ_INGRESS_BYTES_MAX: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BODY_TIMEOUT_SECONDS: u64 = 30;
pub const MAX_BODY_TIMEOUT_SECONDS: u64 = 24 * 60 * 60;

/// Immutable process-wide limits. Zero capacity deliberately refuses that lane.
#[derive(Clone, Debug)]
pub struct WorkloadLimits {
    pub per_actor_inflight_max: u32,
    pub per_actor_bytes_max: u64,
    pub global_inflight_max: u32,
    pub global_bytes_max: u64,
    pub active_actor_max: usize,
    /// Write request input, retained through the last operation/response owner.
    pub ingress_inflight_max: u32,
    pub ingress_bytes_max: u64,
    /// Body-bearing read input; bodyless reads use only the response observer.
    pub read_ingress_inflight_max: u32,
    pub read_ingress_bytes_max: u64,
    pub body_timeout: Duration,
}

impl Default for WorkloadLimits {
    fn default() -> Self {
        Self {
            per_actor_inflight_max: DEFAULT_PER_ACTOR_INFLIGHT_MAX,
            per_actor_bytes_max: DEFAULT_PER_ACTOR_BYTES_MAX,
            global_inflight_max: DEFAULT_GLOBAL_INFLIGHT_MAX,
            global_bytes_max: DEFAULT_GLOBAL_BYTES_MAX,
            active_actor_max: DEFAULT_ACTIVE_ACTORS_MAX,
            ingress_inflight_max: DEFAULT_INGRESS_INFLIGHT_MAX,
            ingress_bytes_max: DEFAULT_INGRESS_BYTES_MAX,
            read_ingress_inflight_max: DEFAULT_READ_INGRESS_INFLIGHT_MAX,
            read_ingress_bytes_max: DEFAULT_READ_INGRESS_BYTES_MAX,
            body_timeout: Duration::from_secs(DEFAULT_BODY_TIMEOUT_SECONDS),
        }
    }
}

/// A refusal occurs before acquiring any operation or ingress capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    InFlightCountExceeded {
        cap: u32,
    },
    ByteBudgetExceeded {
        cap: u64,
        attempted: u64,
    },
    GlobalInFlightCountExceeded {
        cap: u32,
    },
    GlobalByteBudgetExceeded {
        cap: u64,
        attempted: u64,
    },
    ActiveActorLimitExceeded {
        cap: usize,
    },
    IngressInFlightCountExceeded {
        cap: u32,
    },
    IngressByteBudgetExceeded {
        cap: u64,
        attempted: u64,
    },
    ReadIngressInFlightCountExceeded {
        cap: u32,
    },
    ReadIngressByteBudgetExceeded {
        cap: u64,
        attempted: u64,
    },
    /// A caller attempted to grow a precollection lease using `shrink`.
    IngressReservationExceeded {
        reserved: u64,
        attempted: u64,
    },
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InFlightCountExceeded { cap } => {
                write!(f, "actor in-flight count cap {cap} exceeded")
            }
            Self::ByteBudgetExceeded { cap, attempted } => write!(
                f,
                "actor byte budget exceeded: would use {attempted} bytes against cap {cap}"
            ),
            Self::GlobalInFlightCountExceeded { cap } => {
                write!(f, "global operation count cap {cap} exceeded")
            }
            Self::GlobalByteBudgetExceeded { cap, attempted } => write!(
                f,
                "global operation input budget exceeded: would use {attempted} bytes against cap {cap}"
            ),
            Self::ActiveActorLimitExceeded { cap } => {
                write!(f, "active actor count cap {cap} exceeded")
            }
            Self::IngressInFlightCountExceeded { cap } => {
                write!(f, "write ingress count cap {cap} exceeded")
            }
            Self::IngressByteBudgetExceeded { cap, attempted } => write!(
                f,
                "write ingress byte budget exceeded: would reserve {attempted} bytes against cap {cap}"
            ),
            Self::ReadIngressInFlightCountExceeded { cap } => {
                write!(f, "read ingress count cap {cap} exceeded")
            }
            Self::ReadIngressByteBudgetExceeded { cap, attempted } => write!(
                f,
                "read ingress byte budget exceeded: would reserve {attempted} bytes against cap {cap}"
            ),
            Self::IngressReservationExceeded {
                reserved,
                attempted,
            } => write!(
                f,
                "ingress lease cannot grow from {reserved} to {attempted} bytes"
            ),
        }
    }
}

#[derive(Debug, Default)]
struct ActorState {
    count: u64,
    bytes: u64,
}

#[derive(Debug, Default)]
struct State {
    actors: HashMap<Arc<str>, ActorState>,
    operation_count: u64,
    operation_bytes: u64,
    write_ingress: IngressState,
    read_ingress: IngressState,
}

#[derive(Debug, Default)]
struct IngressState {
    count: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum IngressLane {
    Read,
    Write,
}

impl State {
    fn ingress_mut(&mut self, lane: IngressLane) -> &mut IngressState {
        match lane {
            IngressLane::Read => &mut self.read_ingress,
            IngressLane::Write => &mut self.write_ingress,
        }
    }
}

#[derive(Debug)]
struct Inner {
    limits: WorkloadLimits,
    state: Mutex<State>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadSnapshot {
    pub operation_count: u64,
    pub operation_bytes: u64,
    pub active_actors: usize,
    /// Retained write inputs.
    pub ingress_count: u64,
    pub ingress_bytes: u64,
    /// Retained read inputs; excludes bodyless reads.
    pub read_ingress_count: u64,
    pub read_ingress_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct WorkloadController {
    inner: Arc<Inner>,
}

impl WorkloadController {
    /// Preserve the focused per-actor constructor; aggregate limits use defaults.
    pub fn new(inflight_cap: u32, byte_cap: u64) -> Self {
        Self::with_limits(WorkloadLimits {
            per_actor_inflight_max: inflight_cap,
            per_actor_bytes_max: byte_cap,
            ..WorkloadLimits::default()
        })
    }

    pub fn with_limits(mut limits: WorkloadLimits) -> Self {
        if limits.body_timeout > Duration::from_secs(MAX_BODY_TIMEOUT_SECONDS) {
            tracing::warn!(
                env = "OMNIGRAPH_BODY_TIMEOUT_SECONDS",
                value = ?limits.body_timeout,
                maximum = MAX_BODY_TIMEOUT_SECONDS,
                default = DEFAULT_BODY_TIMEOUT_SECONDS,
                "body timeout exceeds the supported maximum, using default"
            );
            limits.body_timeout = Duration::from_secs(DEFAULT_BODY_TIMEOUT_SECONDS);
        }
        Self {
            inner: Arc::new(Inner {
                limits,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Invalid environment values retain the existing warn-and-default behavior.
    pub fn from_env() -> Self {
        Self::with_limits(WorkloadLimits {
            per_actor_inflight_max: parse_env_u32(
                "OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX",
                DEFAULT_PER_ACTOR_INFLIGHT_MAX,
            ),
            per_actor_bytes_max: parse_env_u64(
                "OMNIGRAPH_PER_ACTOR_BYTES_MAX",
                DEFAULT_PER_ACTOR_BYTES_MAX,
            ),
            global_inflight_max: parse_env_u32(
                "OMNIGRAPH_GLOBAL_INFLIGHT_MAX",
                DEFAULT_GLOBAL_INFLIGHT_MAX,
            ),
            global_bytes_max: parse_env_u64("OMNIGRAPH_GLOBAL_BYTES_MAX", DEFAULT_GLOBAL_BYTES_MAX),
            active_actor_max: parse_env_u32(
                "OMNIGRAPH_ACTIVE_ACTORS_MAX",
                DEFAULT_ACTIVE_ACTORS_MAX as u32,
            ) as usize,
            ingress_inflight_max: parse_env_u32(
                "OMNIGRAPH_INGRESS_INFLIGHT_MAX",
                DEFAULT_INGRESS_INFLIGHT_MAX,
            ),
            ingress_bytes_max: parse_env_u64(
                "OMNIGRAPH_INGRESS_BYTES_MAX",
                DEFAULT_INGRESS_BYTES_MAX,
            ),
            read_ingress_inflight_max: parse_env_u32(
                "OMNIGRAPH_READ_INGRESS_INFLIGHT_MAX",
                DEFAULT_READ_INGRESS_INFLIGHT_MAX,
            ),
            read_ingress_bytes_max: parse_env_u64(
                "OMNIGRAPH_READ_INGRESS_BYTES_MAX",
                DEFAULT_READ_INGRESS_BYTES_MAX,
            ),
            body_timeout: Duration::from_secs(parse_env_u64(
                "OMNIGRAPH_BODY_TIMEOUT_SECONDS",
                DEFAULT_BODY_TIMEOUT_SECONDS,
            )),
        })
    }

    pub fn with_defaults() -> Self {
        Self::with_limits(WorkloadLimits::default())
    }

    pub fn limits(&self) -> &WorkloadLimits {
        &self.inner.limits
    }

    pub fn snapshot(&self) -> WorkloadSnapshot {
        let state = locked(&self.inner.state);
        WorkloadSnapshot {
            operation_count: state.operation_count,
            operation_bytes: state.operation_bytes,
            active_actors: state.actors.len(),
            ingress_count: state.write_ingress.count,
            ingress_bytes: state.write_ingress.bytes,
            read_ingress_count: state.read_ingress.count,
            read_ingress_bytes: state.read_ingress.bytes,
        }
    }

    /// Reserve counts and input bytes atomically, without queuing or partial charge.
    pub fn try_admit(
        &self,
        actor_id: &Arc<str>,
        est_bytes: u64,
    ) -> Result<AdmissionGuard, RejectReason> {
        let limits = self.limits();
        let mut state = locked(&self.inner.state);
        let actor = state.actors.get(actor_id);
        let actor_count = actor.map_or(0, |actor| actor.count);
        let actor_bytes = actor.map_or(0, |actor| actor.bytes);
        if actor_count >= u64::from(limits.per_actor_inflight_max) {
            return Err(RejectReason::InFlightCountExceeded {
                cap: limits.per_actor_inflight_max,
            });
        }
        let next_actor_bytes = actor_bytes.checked_add(est_bytes);
        if next_actor_bytes.is_none_or(|bytes| bytes > limits.per_actor_bytes_max) {
            return Err(RejectReason::ByteBudgetExceeded {
                cap: limits.per_actor_bytes_max,
                attempted: next_actor_bytes.unwrap_or(u64::MAX),
            });
        }
        if state.operation_count >= u64::from(limits.global_inflight_max) {
            return Err(RejectReason::GlobalInFlightCountExceeded {
                cap: limits.global_inflight_max,
            });
        }
        let next_global_bytes = state.operation_bytes.checked_add(est_bytes);
        if next_global_bytes.is_none_or(|bytes| bytes > limits.global_bytes_max) {
            return Err(RejectReason::GlobalByteBudgetExceeded {
                cap: limits.global_bytes_max,
                attempted: next_global_bytes.unwrap_or(u64::MAX),
            });
        }
        if actor.is_none() && state.actors.len() >= limits.active_actor_max {
            return Err(RejectReason::ActiveActorLimitExceeded {
                cap: limits.active_actor_max,
            });
        }
        state.operation_count += 1;
        state.operation_bytes = next_global_bytes.expect("checked operation bytes");
        let actor = state.actors.entry(Arc::clone(actor_id)).or_default();
        actor.count += 1;
        actor.bytes = next_actor_bytes.expect("checked actor bytes");
        Ok(AdmissionGuard {
            inner: Arc::clone(&self.inner),
            actor_id: Arc::clone(actor_id),
            est_bytes,
        })
    }

    /// Reserve write input independently of reads before collecting any bytes.
    /// The successful collector shrinks it to actual retained wire bytes.
    pub fn try_ingress(&self, reserved_bytes: u64) -> Result<IngressLease, RejectReason> {
        self.try_ingress_in_lane(IngressLane::Write, reserved_bytes)
    }

    /// Read input has separate capacity, so stalled reads cannot consume a
    /// writer's input allowance. Bodyless reads need only a response observer.
    pub fn try_read_ingress(&self, reserved_bytes: u64) -> Result<IngressLease, RejectReason> {
        self.try_ingress_in_lane(IngressLane::Read, reserved_bytes)
    }

    fn try_ingress_in_lane(
        &self,
        lane: IngressLane,
        reserved_bytes: u64,
    ) -> Result<IngressLease, RejectReason> {
        let limits = self.limits();
        let (count_cap, byte_cap) = match lane {
            IngressLane::Read => (
                limits.read_ingress_inflight_max,
                limits.read_ingress_bytes_max,
            ),
            IngressLane::Write => (limits.ingress_inflight_max, limits.ingress_bytes_max),
        };
        let mut state = locked(&self.inner.state);
        let ingress = state.ingress_mut(lane);
        if ingress.count >= u64::from(count_cap) {
            return Err(match lane {
                IngressLane::Read => {
                    RejectReason::ReadIngressInFlightCountExceeded { cap: count_cap }
                }
                IngressLane::Write => RejectReason::IngressInFlightCountExceeded { cap: count_cap },
            });
        }
        let next_bytes = ingress.bytes.checked_add(reserved_bytes);
        if next_bytes.is_none_or(|bytes| bytes > byte_cap) {
            let attempted = next_bytes.unwrap_or(u64::MAX);
            return Err(match lane {
                IngressLane::Read => RejectReason::ReadIngressByteBudgetExceeded {
                    cap: byte_cap,
                    attempted,
                },
                IngressLane::Write => RejectReason::IngressByteBudgetExceeded {
                    cap: byte_cap,
                    attempted,
                },
            });
        }
        ingress.count += 1;
        ingress.bytes = next_bytes.expect("checked ingress bytes");
        Ok(IngressLease(Some(Arc::new(IngressReservation {
            inner: Arc::clone(&self.inner),
            lane,
            bytes: AtomicU64::new(reserved_bytes),
        }))))
    }
}

#[derive(Debug)]
pub struct AdmissionGuard {
    inner: Arc<Inner>,
    actor_id: Arc<str>,
    est_bytes: u64,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let mut state = locked(&self.inner.state);
        let actor = state
            .actors
            .get_mut(&self.actor_id)
            .expect("admitted actor remains registered");
        actor.count -= 1;
        actor.bytes -= self.est_bytes;
        let idle = actor.count == 0;
        if idle {
            state.actors.remove(&self.actor_id);
        }
        state.operation_count -= 1;
        state.operation_bytes -= self.est_bytes;
    }
}

/// Shared wire-input reservation; clones transfer ownership without extra charge.
#[derive(Debug, Clone)]
pub struct IngressLease(Option<Arc<IngressReservation>>);

#[derive(Debug)]
struct IngressReservation {
    inner: Arc<Inner>,
    lane: IngressLane,
    // Changes only under inner.state. Atomic permits shared lease access without
    // a second lock or retaining a separate per-lease map in the controller.
    bytes: AtomicU64,
}

impl IngressLease {
    /// A bodyless read holds only its bounded response observation.
    pub(crate) fn empty() -> Self {
        Self(None)
    }

    pub fn shrink(&self, actual_bytes: u64) -> Result<(), RejectReason> {
        let Some(reservation) = &self.0 else {
            return if actual_bytes == 0 {
                Ok(())
            } else {
                Err(RejectReason::IngressReservationExceeded {
                    reserved: 0,
                    attempted: actual_bytes,
                })
            };
        };
        let mut state = locked(&reservation.inner.state);
        let reserved = reservation.bytes.load(Ordering::Relaxed);
        if actual_bytes > reserved {
            return Err(RejectReason::IngressReservationExceeded {
                reserved,
                attempted: actual_bytes,
            });
        }
        state.ingress_mut(reservation.lane).bytes -= reserved - actual_bytes;
        reservation.bytes.store(actual_bytes, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for IngressReservation {
    fn drop(&mut self) {
        let mut state = locked(&self.inner.state);
        let ingress = state.ingress_mut(self.lane);
        ingress.count -= 1;
        ingress.bytes -= self.bytes.load(Ordering::Relaxed);
    }
}

fn parse_env_u32(name: &str, default: u32) -> u32 {
    parse_env(name, default)
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    parse_env(name, default)
}

fn parse_env<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr + Copy + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value.parse().unwrap_or_else(|error| {
            tracing::warn!(env = name, value = %value, error = %error, default = %default, "invalid env value, using default");
            default
        }),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_deadline_is_finite_and_oversized_configuration_uses_default() {
        for (configured, expected) in [
            (Duration::ZERO, Duration::ZERO),
            (Duration::from_secs(1), Duration::from_secs(1)),
            (
                Duration::from_secs(MAX_BODY_TIMEOUT_SECONDS),
                Duration::from_secs(MAX_BODY_TIMEOUT_SECONDS),
            ),
            (
                Duration::from_secs(MAX_BODY_TIMEOUT_SECONDS + 1),
                Duration::from_secs(DEFAULT_BODY_TIMEOUT_SECONDS),
            ),
            (
                Duration::MAX,
                Duration::from_secs(DEFAULT_BODY_TIMEOUT_SECONDS),
            ),
        ] {
            let controller = WorkloadController::with_limits(WorkloadLimits {
                body_timeout: configured,
                ..WorkloadLimits::default()
            });
            assert_eq!(controller.limits().body_timeout, expected);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn try_admit_admits_under_cap() {
        let controller = WorkloadController::new(2, 1024);
        let actor: Arc<str> = "alice".into();
        let g1 = controller.try_admit(&actor, 100).expect("first admit");
        let _g2 = controller.try_admit(&actor, 100).expect("second admit");
        let err = controller
            .try_admit(&actor, 100)
            .expect_err("third should reject on count");
        assert!(matches!(
            err,
            RejectReason::InFlightCountExceeded { cap: 2 }
        ));
        drop(g1);
        // After drop, a new admit succeeds again.
        let _g3 = controller.try_admit(&actor, 100).expect("admit after drop");
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
        let controller = Arc::new(WorkloadController::new(16, u64::MAX / 4));
        let actor: Arc<str> = "racer".into();

        let all_attempted = Arc::new(tokio::sync::Barrier::new(33));

        let mut handles = Vec::with_capacity(32);
        for _ in 0..32 {
            let controller = Arc::clone(&controller);
            let actor = actor.clone();
            let all_attempted = Arc::clone(&all_attempted);
            handles.push(tokio::spawn(async move {
                let result = controller.try_admit(&actor, 1);
                let success = result.is_ok();
                let _guard = result.ok();
                all_attempted.wait().await;
                success
            }));
        }

        all_attempted.wait().await;

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
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn per_actor_caps_independent() {
        let controller = WorkloadController::new(1, 1024);
        let alice: Arc<str> = "alice".into();
        let bob: Arc<str> = "bob".into();
        let _ga = controller.try_admit(&alice, 100).expect("alice ok");
        // Alice over count cap, Bob unaffected.
        let err = controller
            .try_admit(&alice, 100)
            .expect_err("alice rejected");
        assert!(matches!(err, RejectReason::InFlightCountExceeded { .. }));
        let _gb = controller.try_admit(&bob, 100).expect("bob ok");
    }

    #[test]
    fn input_byte_overflow_refuses_without_changing_live_accounting() {
        let controller = WorkloadController::with_limits(WorkloadLimits {
            per_actor_bytes_max: u64::MAX,
            global_bytes_max: u64::MAX,
            ..WorkloadLimits::default()
        });
        let alice: Arc<str> = "alice".into();
        let bob: Arc<str> = "bob".into();
        let guard = controller.try_admit(&alice, u64::MAX).unwrap();
        let full = controller.snapshot();
        assert!(matches!(
            controller.try_admit(&alice, 1),
            Err(RejectReason::ByteBudgetExceeded { .. })
        ));
        assert!(matches!(
            controller.try_admit(&bob, 1),
            Err(RejectReason::GlobalByteBudgetExceeded { .. })
        ));
        assert_eq!(controller.snapshot(), full);
        drop(guard);
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
        assert!(controller.try_admit(&bob, 1).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_actor_admission_respects_each_aggregate_bound() {
        for (case, global_count, global_bytes, actors) in [
            ("count", 4, 1024, 32),
            ("bytes", 32, 40, 32),
            ("actors", 32, 1024, 4),
        ] {
            let controller = Arc::new(WorkloadController::with_limits(WorkloadLimits {
                global_inflight_max: global_count,
                global_bytes_max: global_bytes,
                active_actor_max: actors,
                ..WorkloadLimits::default()
            }));
            let admitted = Arc::new(tokio::sync::Barrier::new(33));
            let release = Arc::new(tokio::sync::Barrier::new(33));
            let mut tasks = Vec::new();
            for index in 0..32 {
                let controller = Arc::clone(&controller);
                let admitted = Arc::clone(&admitted);
                let release = Arc::clone(&release);
                tasks.push(tokio::spawn(async move {
                    let actor: Arc<str> = format!("actor-{index}").into();
                    let result = controller.try_admit(&actor, 10);
                    admitted.wait().await;
                    release.wait().await;
                    result.map(drop)
                }));
            }
            admitted.wait().await;
            assert_eq!(controller.snapshot().operation_count, 4, "{case}");
            assert_eq!(controller.snapshot().operation_bytes, 40, "{case}");
            assert_eq!(controller.snapshot().active_actors, 4, "{case}");
            release.wait().await;
            let mut refused = 0;
            for task in tasks {
                if let Err(error) = task.await.unwrap() {
                    refused += 1;
                    assert!(
                        matches!(
                            (case, error),
                            ("count", RejectReason::GlobalInFlightCountExceeded { .. })
                                | ("bytes", RejectReason::GlobalByteBudgetExceeded { .. })
                                | ("actors", RejectReason::ActiveActorLimitExceeded { .. })
                        ),
                        "refusal must identify the saturated resource"
                    );
                }
            }
            assert_eq!(refused, 28, "{case}");
            assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
        }
    }

    #[test]
    fn actor_record_lasts_through_all_guards_and_idle_actors_do_not_accumulate() {
        let controller = WorkloadController::with_limits(WorkloadLimits {
            active_actor_max: 1,
            ..WorkloadLimits::default()
        });
        let alice: Arc<str> = "alice".into();
        let first = controller.try_admit(&alice, 10).unwrap();
        let second = controller.try_admit(&alice, 20).unwrap();
        drop(first);
        assert_eq!(controller.snapshot().operation_bytes, 20);
        assert!(matches!(
            controller.try_admit(&Arc::from("bob"), 1),
            Err(RejectReason::ActiveActorLimitExceeded { cap: 1 })
        ));
        drop(second);
        for index in 0..1000 {
            let actor: Arc<str> = format!("new-actor-{index}").into();
            let guard = controller.try_admit(&actor, 1).unwrap();
            assert_eq!(controller.snapshot().active_actors, 1);
            drop(guard);
            assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
        }
    }

    #[test]
    fn ingress_shrink_and_last_owner_drop_preserve_the_shared_charge() {
        let controller = WorkloadController::with_limits(WorkloadLimits {
            ingress_inflight_max: 2,
            ingress_bytes_max: 100,
            ..WorkloadLimits::default()
        });
        let collecting = controller.try_ingress(80).unwrap();
        assert!(matches!(
            controller.try_ingress(30),
            Err(RejectReason::IngressByteBudgetExceeded {
                cap: 100,
                attempted: 110
            })
        ));
        let operation = collecting.clone();
        collecting.shrink(20).unwrap();
        drop(collecting);
        assert_eq!(controller.snapshot().ingress_count, 1);
        assert_eq!(controller.snapshot().ingress_bytes, 20);
        let peer = controller.try_ingress(80).unwrap();
        assert!(matches!(
            controller.try_ingress(0),
            Err(RejectReason::IngressInFlightCountExceeded { cap: 2 })
        ));
        assert!(matches!(
            operation.shrink(21),
            Err(RejectReason::IngressReservationExceeded {
                reserved: 20,
                attempted: 21
            })
        ));
        assert_eq!(controller.snapshot().ingress_bytes, 100);
        drop(operation);
        assert_eq!(controller.snapshot().ingress_count, 1);
        assert_eq!(controller.snapshot().ingress_bytes, 80);
        drop(peer);
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
    }

    #[test]
    fn ingress_overflow_never_admits_or_wraps_live_bytes() {
        let controller = WorkloadController::with_limits(WorkloadLimits {
            ingress_bytes_max: u64::MAX,
            read_ingress_bytes_max: u64::MAX,
            ..WorkloadLimits::default()
        });
        let lease = controller.try_ingress(u64::MAX).unwrap();
        assert!(matches!(
            controller.try_ingress(1),
            Err(RejectReason::IngressByteBudgetExceeded { .. })
        ));
        assert_eq!(controller.snapshot().ingress_count, 1);
        assert_eq!(controller.snapshot().ingress_bytes, u64::MAX);
        lease.shrink(1).unwrap();
        assert_eq!(controller.snapshot().ingress_bytes, 1);
        drop(lease);
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());

        let lease = controller.try_read_ingress(u64::MAX).unwrap();
        assert!(matches!(
            controller.try_read_ingress(1),
            Err(RejectReason::ReadIngressByteBudgetExceeded { .. })
        ));
        assert_eq!(controller.snapshot().read_ingress_count, 1);
        assert_eq!(controller.snapshot().read_ingress_bytes, u64::MAX);
        assert_eq!(controller.snapshot().ingress_bytes, 0);
        lease.shrink(1).unwrap();
        assert_eq!(controller.snapshot().read_ingress_bytes, 1);
        drop(lease);
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
    }

    #[test]
    fn saturated_read_input_preserves_write_capacity_and_last_owner_charge() {
        let controller = WorkloadController::with_limits(WorkloadLimits {
            ingress_inflight_max: 1,
            ingress_bytes_max: 8,
            read_ingress_inflight_max: 2,
            read_ingress_bytes_max: 8,
            ..WorkloadLimits::default()
        });
        let read = controller.try_read_ingress(8).unwrap();
        assert!(matches!(
            controller.try_read_ingress(1),
            Err(RejectReason::ReadIngressByteBudgetExceeded {
                cap: 8,
                attempted: 9
            })
        ));
        let empty_read = controller.try_read_ingress(0).unwrap();
        assert!(matches!(
            controller.try_read_ingress(0),
            Err(RejectReason::ReadIngressInFlightCountExceeded { cap: 2 })
        ));
        let write = controller.try_ingress(8).unwrap();
        assert_eq!(controller.snapshot().read_ingress_bytes, 8);
        assert_eq!(controller.snapshot().ingress_bytes, 8);
        let producer = read.clone();
        read.shrink(3).unwrap();
        drop(read);
        drop(empty_read);
        assert_eq!(controller.snapshot().read_ingress_count, 1);
        assert_eq!(controller.snapshot().read_ingress_bytes, 3);
        assert_eq!(controller.snapshot().ingress_bytes, 8);
        assert!(matches!(
            producer.shrink(4),
            Err(RejectReason::IngressReservationExceeded {
                reserved: 3,
                attempted: 4
            })
        ));
        drop(write);
        assert_eq!(controller.snapshot().ingress_count, 0);
        assert_eq!(controller.snapshot().read_ingress_count, 1);
        drop(producer);
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
        let empty = IngressLease::empty();
        assert!(empty.shrink(0).is_ok());
        assert!(empty.shrink(1).is_err());
        assert_eq!(controller.snapshot(), WorkloadSnapshot::default());
    }
}
