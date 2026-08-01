//! Hidden resident fold supervisor for RFC-026 F5a.
//!
//! The supervisor is orchestration only. It owns no durable queue and adds no
//! persisted grammar: readiness is derived from the manifest-selected
//! lifecycle plus the current Lance MemWAL authority, and every effect still
//! goes through the existing recovery-v14 fold adapter.

use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::db::manifest::{StreamLifecycle, StreamProfileMode, TableIdentity};
use crate::db::{Omnigraph, ReadTarget};
use crate::error::{OmniError, Result};

use super::stream_ingest::ResidentFoldOutcome;

/// A non-full generation becomes visible on this cadence. Capacity pressure
/// shortens the same pending entry to `now`; it never creates another job.
const STREAM_FOLD_MAX_STALENESS: Duration = Duration::from_secs(1);
const STREAM_FOLD_RETRY_BASE: Duration = Duration::from_millis(100);
const STREAM_FOLD_RETRY_MAX: Duration = Duration::from_secs(5);
const STREAM_FOLD_IDLE_WAIT: Duration = Duration::from_secs(60);
const STREAM_FOLD_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy)]
struct PendingFold {
    sequence: u64,
    due_at: Instant,
    failures: u32,
}

#[derive(Debug, Default)]
struct DriverHealth {
    running: bool,
    unexpected_stop: bool,
    published_folds: u64,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct DriverShared {
    pending: BTreeMap<TableIdentity, PendingFold>,
    health: DriverHealth,
    last_node: Option<TableIdentity>,
    last_edge: Option<TableIdentity>,
}

/// One weakly root-scoped supervisor registry. The task is deliberately
/// process-local; the EXP profile still requires an externally enforced sole
/// writer process.
pub(super) struct StreamFoldDriverRegistry {
    shared: Mutex<DriverShared>,
    wake: Notify,
    stop: AtomicBool,
    task: AsyncMutex<Option<JoinHandle<Result<()>>>>,
}

impl StreamFoldDriverRegistry {
    pub(super) fn for_root(root_identity: &str) -> Arc<Self> {
        static REGISTRY: OnceLock<Mutex<HashMap<String, Weak<StreamFoldDriverRegistry>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut roots = registry
            .lock()
            .expect("stream fold driver root registry poisoned");
        if let Some(existing) = roots.get(root_identity).and_then(Weak::upgrade) {
            return existing;
        }
        roots.retain(|_, driver| driver.strong_count() > 0);
        let driver = Arc::new(Self {
            shared: Mutex::new(DriverShared::default()),
            wake: Notify::new(),
            stop: AtomicBool::new(false),
            task: AsyncMutex::new(None),
        });
        roots.insert(root_identity.to_string(), Arc::downgrade(&driver));
        driver
    }

    fn notify(&self, identity: TableIdentity, urgent: bool) {
        let now = Instant::now();
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        let overflow = {
            let pending = shared.pending.entry(identity).or_insert(PendingFold {
                sequence: 0,
                due_at: if urgent {
                    now
                } else {
                    now + STREAM_FOLD_MAX_STALENESS
                },
                failures: 0,
            });
            let overflow = match pending.sequence.checked_add(1) {
                Some(next) => {
                    pending.sequence = next;
                    false
                }
                None => true,
            };
            if urgent {
                pending.due_at = now;
            }
            overflow
        };
        if overflow {
            shared.health.unexpected_stop = true;
            shared.health.last_error = Some(format!(
                "stream fold trigger sequence overflow for table identity {identity}"
            ));
            tracing::error!(
                table_identity = %identity,
                "stream fold trigger sequence overflow"
            );
        }
        drop(shared);
        self.wake.notify_one();
    }

    fn due_round(&self, now: Instant) -> Vec<(TableIdentity, u64)> {
        let stopping = self.stop.load(Ordering::Acquire);
        self.shared
            .lock()
            .expect("stream fold driver state poisoned")
            .pending
            .iter()
            .filter_map(|(identity, pending)| {
                (stopping || pending.due_at <= now).then_some((*identity, pending.sequence))
            })
            .collect()
    }

    fn pending_is_empty(&self) -> bool {
        self.shared
            .lock()
            .expect("stream fold driver state poisoned")
            .pending
            .is_empty()
    }

    fn next_wait(&self, now: Instant) -> Duration {
        self.shared
            .lock()
            .expect("stream fold driver state poisoned")
            .pending
            .values()
            .map(|pending| pending.due_at.saturating_duration_since(now))
            .min()
            .unwrap_or(STREAM_FOLD_IDLE_WAIT)
    }

    fn complete(&self, identity: TableIdentity, observed_sequence: u64, published: bool) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        if published {
            shared.health.published_folds = shared.health.published_folds.saturating_add(1);
            shared.health.last_error = None;
        }
        let remove = shared
            .pending
            .get(&identity)
            .is_some_and(|pending| pending.sequence == observed_sequence);
        if remove {
            shared.pending.remove(&identity);
        } else if let Some(pending) = shared.pending.get_mut(&identity) {
            // A new acknowledgement arrived while the prior cut was folding.
            // Preserve both that trigger and its original deadline. In
            // particular, never turn a newer pressure wake back into a timer.
            pending.failures = 0;
        }
    }

    fn blocked(&self, identity: TableIdentity, observed_sequence: u64, error: &OmniError) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        if shared
            .pending
            .get(&identity)
            .is_some_and(|pending| pending.sequence == observed_sequence)
        {
            shared.pending.remove(&identity);
        }
        shared.health.last_error = Some(error.to_string());
        tracing::warn!(
            table_identity = %identity,
            error = %error,
            "stream fold lane is durably blocked; automatic retry is parked"
        );
    }

    fn failed(&self, identity: TableIdentity, observed_sequence: u64, error: &OmniError) {
        let now = Instant::now();
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        let retry_in = {
            let pending = shared.pending.entry(identity).or_insert(PendingFold {
                sequence: observed_sequence,
                due_at: now,
                failures: 0,
            });
            if pending.sequence != observed_sequence {
                // A newer acknowledgement/cap wake arrived while this older
                // attempt was failing. Preserve its original deadline exactly;
                // the stale failure must not postpone new pressure work.
                pending.failures = 0;
                None
            } else {
                pending.failures = pending.failures.saturating_add(1);
                let shift = pending.failures.saturating_sub(1).min(16);
                let multiplier = 1_u32 << shift;
                let retry_in = STREAM_FOLD_RETRY_BASE
                    .saturating_mul(multiplier)
                    .min(STREAM_FOLD_RETRY_MAX);
                pending.due_at = now + retry_in;
                Some(retry_in)
            }
        };
        shared.health.last_error = Some(error.to_string());
        if let Some(retry_in) = retry_in {
            tracing::error!(
                table_identity = %identity,
                retry_in_ms = retry_in.as_millis() as u64,
                error = %error,
                "stream fold driver attempt failed"
            );
        } else {
            tracing::error!(
                table_identity = %identity,
                error = %error,
                "stale stream fold attempt failed; a newer trigger keeps its deadline"
            );
        }
    }

    fn mark_running(&self, running: bool) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        shared.health.running = running;
        if running {
            // A deliberate restart is the acknowledgement boundary for a
            // prior unexpected task stop. Transient attempt failures remain in
            // `last_error` until a successful publish.
            shared.health.unexpected_stop = false;
        }
    }

    fn mark_unexpected_stop(&self, message: String) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        shared.health.running = false;
        shared.health.unexpected_stop = true;
        shared.health.last_error = Some(message.clone());
        tracing::error!(error = %message, "stream fold driver stopped unexpectedly");
    }

    fn order_round(&self, candidates: Vec<DriverCandidate>) -> Vec<DriverCandidate> {
        let shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        let (mut nodes, mut edges): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|candidate| candidate.kind == DriverCandidateKind::Node);
        fn rotate_after(candidates: &mut Vec<DriverCandidate>, cursor: Option<TableIdentity>) {
            candidates.sort_by_key(|candidate| candidate.identity);
            let Some(cursor) = cursor else {
                return;
            };
            let start = candidates
                .iter()
                .position(|candidate| candidate.identity > cursor)
                .unwrap_or(0);
            candidates.rotate_left(start);
        }
        rotate_after(&mut nodes, shared.last_node);
        rotate_after(&mut edges, shared.last_edge);
        nodes.extend(edges);
        nodes
    }

    fn mark_attempted(&self, kind: DriverCandidateKind, identity: TableIdentity) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        match kind {
            DriverCandidateKind::Node => shared.last_node = Some(identity),
            DriverCandidateKind::Edge => shared.last_edge = Some(identity),
        }
    }

    fn existing_task_start_result(&self) -> Result<()> {
        if self.stop.load(Ordering::Acquire) {
            return Err(OmniError::manifest(
                "stream fold driver shutdown is still in progress; join that owned task before restarting",
            ));
        }
        Ok(())
    }

    async fn start(self: &Arc<Self>, db: &Arc<Omnigraph>) -> Result<()> {
        let mut task_slot = self.task.lock().await;
        if let Some(existing) = task_slot.as_ref()
            && !existing.is_finished()
        {
            return self.existing_task_start_result();
        }
        if let Some(finished) = task_slot.take() {
            finished.await.map_err(|error| {
                OmniError::manifest_internal(format!(
                    "prior stream fold driver task failed to join: {error}"
                ))
            })??;
        }

        // Make cold-start discovery part of server startup, rather than the
        // first fallible action in a detached task. A graph whose manifest
        // cannot establish its eligible OPEN lanes must refuse startup instead
        // of briefly serving beside an already-dead supervisor.
        let initial = db.stream_driver_open_identities().await?;
        self.stop.store(false, Ordering::Release);
        self.mark_running(true);
        for identity in initial {
            self.notify(identity, true);
        }
        let weak_db = Arc::downgrade(db);
        let driver = Arc::clone(self);
        let task = tokio::spawn(async move {
            let outcome = AssertUnwindSafe(run_driver(weak_db, Arc::clone(&driver)))
                .catch_unwind()
                .await;
            match outcome {
                Ok(Ok(())) => {
                    driver.mark_running(false);
                    Ok(())
                }
                Ok(Err(error)) => {
                    driver.mark_unexpected_stop(error.to_string());
                    Err(error)
                }
                Err(_) => {
                    let error = OmniError::manifest_internal("stream fold driver panicked");
                    driver.mark_unexpected_stop(error.to_string());
                    Err(error)
                }
            }
        });
        *task_slot = Some(task);
        Ok(())
    }

    async fn shutdown(&self, deadline: Instant) -> Result<()> {
        // Start and stop serialize through the task slot before either changes
        // the shared stop flag. Otherwise a concurrent start could reset a
        // pre-lock stop request and leave shutdown joining a live idle task.
        let mut task_slot = tokio::time::timeout_at(deadline, self.task.lock())
            .await
            .map_err(|_| {
                OmniError::manifest(format!(
                    "stream fold driver ownership transition did not settle within {} seconds; its task remains owned and running",
                    STREAM_FOLD_SHUTDOWN_DEADLINE.as_secs()
                ))
            })?;
        self.stop.store(true, Ordering::Release);
        // `notify_one` retains a permit when the task is between its stop check
        // and construction of the wait future. `notify_waiters` would lose
        // that race and leave an idle driver asleep past the shutdown bound.
        self.wake.notify_one();
        // Retain this mutex across the join. Start and stop are ownership
        // transitions, not mere slot observations; releasing it while the
        // first owner is still being joined would allow a concurrent start to
        // install a second task.
        let Some(task) = task_slot.as_mut() else {
            self.mark_running(false);
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, task).await {
            Ok(joined) => {
                task_slot.take();
                joined
                    .map_err(|error| {
                        OmniError::manifest_internal(format!(
                            "stream fold driver task failed during shutdown: {error}"
                        ))
                    })
                    .and_then(|outcome| outcome)
            }
            Err(_) => {
                // The handle never left the slot, so timeout—or cancellation
                // of this shutdown future—cannot detach it and permit a second
                // supervisor beside the still-running owner.
                Err(OmniError::manifest(format!(
                    "stream fold driver did not settle within {} seconds; its live task remains owned and any armed fold remains recovery-owned",
                    STREAM_FOLD_SHUTDOWN_DEADLINE.as_secs()
                )))
            }
        }
    }

    #[cfg(feature = "failpoints")]
    fn status_json(&self) -> String {
        let shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        serde_json::json!({
            "running": shared.health.running,
            "unexpected_stop": shared.health.unexpected_stop,
            "pending_tables": shared.pending.len(),
            "published_folds": shared.health.published_folds,
            "last_error": shared.health.last_error,
        })
        .to_string()
    }
}

#[derive(Debug)]
struct DriverCandidate {
    identity: TableIdentity,
    observed_sequence: u64,
    table_key: String,
    kind: DriverCandidateKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverCandidateKind {
    Node,
    Edge,
}

async fn run_driver(weak_db: Weak<Omnigraph>, driver: Arc<StreamFoldDriverRegistry>) -> Result<()> {
    loop {
        if driver.stop.load(Ordering::Acquire) && driver.pending_is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        let due = driver.due_round(now);
        if due.is_empty() {
            let wait = driver.next_wait(now);
            tokio::select! {
                _ = driver.wake.notified() => {}
                _ = tokio::time::sleep(wait) => {}
            }
            continue;
        }

        let Some(db) = weak_db.upgrade() else {
            return Ok(());
        };
        let candidates = match db.stream_driver_candidates(&due).await {
            Ok(candidates) => driver.order_round(candidates),
            Err(error) => {
                let stopping = driver.stop.load(Ordering::Acquire);
                for (identity, observed_sequence) in &due {
                    driver.failed(*identity, *observed_sequence, &error);
                }
                if stopping {
                    return Err(error);
                }
                drop(db);
                continue;
            }
        };
        let active = candidates
            .iter()
            .map(|candidate| candidate.identity)
            .collect::<std::collections::BTreeSet<_>>();
        for (identity, sequence) in &due {
            if !active.contains(identity) {
                driver.complete(*identity, *sequence, false);
            }
        }

        if let Err(error) = db.heal_pending_recovery_sidecars_for_write(&[None]).await {
            let stopping = driver.stop.load(Ordering::Acquire);
            for candidate in candidates {
                driver.failed(candidate.identity, candidate.observed_sequence, &error);
            }
            if stopping {
                return Err(error);
            }
            drop(db);
            continue;
        }

        // `candidates` is one finite manifest-derived round. New triggers do
        // not enter it, so continuously active node lanes cannot starve an edge
        // that was ready when this round began.
        for candidate in candidates {
            driver.mark_attempted(candidate.kind, candidate.identity);
            match db
                .stream_fold_from_resident_driver(candidate.identity, &candidate.table_key)
                .await
            {
                Ok(ResidentFoldOutcome::Published) => {
                    driver.complete(candidate.identity, candidate.observed_sequence, true)
                }
                Ok(ResidentFoldOutcome::Idle | ResidentFoldOutcome::Inactive) => {
                    driver.complete(candidate.identity, candidate.observed_sequence, false)
                }
                Err(error @ OmniError::StreamDataBlocked { .. }) => {
                    driver.blocked(candidate.identity, candidate.observed_sequence, &error)
                }
                Err(error) => {
                    driver.failed(candidate.identity, candidate.observed_sequence, &error);
                    if driver.stop.load(Ordering::Acquire) {
                        return Err(error);
                    }
                }
            }
        }
        drop(db);
    }
}

impl Omnigraph {
    pub(super) fn notify_stream_fold_pending(&self, identity: TableIdentity) {
        self.stream_fold_driver.notify(identity, false);
    }

    pub(super) fn notify_stream_fold_pressure(&self, identity: TableIdentity) {
        self.stream_fold_driver.notify(identity, true);
    }

    async fn stream_driver_open_identities(&self) -> Result<Vec<TableIdentity>> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        if snapshot.stream_profile().mode() != StreamProfileMode::Enabled {
            return Ok(Vec::new());
        }
        Ok(snapshot
            .stream_lifecycles()
            .filter_map(|(identity, lifecycle)| {
                (lifecycle.lifecycle == StreamLifecycle::Open && lifecycle.strict_block.is_none())
                    .then_some(*identity)
            })
            .collect())
    }

    async fn stream_driver_candidates(
        &self,
        due: &[(TableIdentity, u64)],
    ) -> Result<Vec<DriverCandidate>> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        if snapshot.stream_profile().mode() != StreamProfileMode::Enabled {
            return Ok(Vec::new());
        }
        let due = due.iter().copied().collect::<BTreeMap<_, _>>();
        let lifecycles = snapshot.stream_lifecycles().collect::<BTreeMap<_, _>>();
        let catalog = self.catalog();
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "stream fold driver requires the identity-bound accepted catalog",
            )
        })?;
        let nodes = schema_ir
            .nodes
            .iter()
            .map(|node| TableIdentity::new(node.type_id.get(), node.table_incarnation_id.get()))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        let edges = schema_ir
            .edges
            .iter()
            .map(|edge| TableIdentity::new(edge.type_id.get(), edge.table_incarnation_id.get()))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        let candidates = snapshot
            .entries()
            .map(|entry| -> Result<Option<DriverCandidate>> {
                let Some(observed_sequence) = due.get(&entry.identity).copied() else {
                    return Ok(None);
                };
                let Some(lifecycle) = lifecycles.get(&entry.identity) else {
                    return Ok(None);
                };
                if lifecycle.lifecycle != StreamLifecycle::Open || lifecycle.strict_block.is_some()
                {
                    return Ok(None);
                }
                let kind = if nodes.contains(&entry.identity) {
                    DriverCandidateKind::Node
                } else if edges.contains(&entry.identity) {
                    DriverCandidateKind::Edge
                } else {
                    return Err(OmniError::manifest_internal(format!(
                        "stream fold candidate {} is absent from the accepted schema identity set",
                        entry.identity
                    )));
                };
                Ok(Some(DriverCandidate {
                    identity: entry.identity,
                    observed_sequence,
                    table_key: entry.table_key.clone(),
                    kind,
                }))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(candidates)
    }

    /// Start the sole in-process resident fold supervisor for this checked
    /// cluster runtime. This is a doc-hidden server bridge, not a stable SDK
    /// management surface.
    #[doc(hidden)]
    pub async fn start_stream_fold_driver(self: &Arc<Self>) -> Result<()> {
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        self.stream_fold_driver.start(self).await
    }

    /// After the caller has stopped transport admission, fence detached
    /// trigger creation, request stop, and join the resident supervisor under
    /// one deadline. An already armed fold remains owned by its detached
    /// recovery adapter until it settles; timeout is loud and never aborts
    /// that task.
    #[doc(hidden)]
    pub async fn shutdown_stream_fold_driver(&self) -> Result<()> {
        if self.stream_runtime_authority.is_none() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "resident fold driver shutdown requires the checked cluster-served runtime handle that owns it"
                .to_string(),
            });
        }
        let deadline = Instant::now() + STREAM_FOLD_SHUTDOWN_DEADLINE;
        // Axum has already stopped admission and settled request futures. The
        // production B2 path transfers this profile-shared guard into every
        // detached worker, so one exclusive acquire/release proves that no
        // canceled pre-invocation owner can create a fold trigger after stop.
        // Drop it before joining: the driver needs shared profile admission to
        // drain triggers that became ready ahead of this barrier.
        let producers_settled = tokio::time::timeout_at(
            deadline,
            self.write_queue().acquire_stream_profile_exclusive(),
        )
        .await
        .map_err(|_| {
            OmniError::manifest(format!(
                "detached stream producers did not settle within {} seconds; the resident fold driver remains owned and running",
                STREAM_FOLD_SHUTDOWN_DEADLINE.as_secs()
            ))
        })?;
        drop(producers_settled);
        self.stream_fold_driver.shutdown(deadline).await
    }

    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub fn failpoint_stream_fold_driver_status_for_test(&self) -> String {
        self.stream_fold_driver.status_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(stable: u64) -> TableIdentity {
        TableIdentity::new(stable, stable + 100).unwrap()
    }

    fn candidate(stable: u64, kind: DriverCandidateKind) -> DriverCandidate {
        DriverCandidate {
            identity: identity(stable),
            observed_sequence: 1,
            table_key: format!("diagnostic:{stable}"),
            kind,
        }
    }

    fn driver() -> StreamFoldDriverRegistry {
        StreamFoldDriverRegistry {
            shared: Mutex::new(DriverShared::default()),
            wake: Notify::new(),
            stop: AtomicBool::new(false),
            task: AsyncMutex::new(None),
        }
    }

    #[test]
    fn finite_round_is_node_first_and_rotates_after_the_last_attempt() {
        let driver = driver();
        let first = driver.order_round(vec![
            candidate(4, DriverCandidateKind::Edge),
            candidate(2, DriverCandidateKind::Node),
            candidate(3, DriverCandidateKind::Edge),
            candidate(1, DriverCandidateKind::Node),
        ]);
        assert_eq!(
            first.iter().map(|item| item.identity).collect::<Vec<_>>(),
            vec![identity(1), identity(2), identity(3), identity(4)]
        );

        driver.mark_attempted(DriverCandidateKind::Node, identity(1));
        driver.mark_attempted(DriverCandidateKind::Edge, identity(3));
        let second = driver.order_round(vec![
            candidate(1, DriverCandidateKind::Node),
            candidate(3, DriverCandidateKind::Edge),
            candidate(2, DriverCandidateKind::Node),
            candidate(4, DriverCandidateKind::Edge),
        ]);
        assert_eq!(
            second.iter().map(|item| item.identity).collect::<Vec<_>>(),
            vec![identity(2), identity(1), identity(4), identity(3)]
        );
    }

    #[test]
    fn completing_an_old_attempt_preserves_a_newer_pressure_deadline() {
        let driver = driver();
        let table = identity(7);
        driver.notify(table, false);
        let observed_sequence = driver.shared.lock().unwrap().pending[&table].sequence;
        driver.notify(table, true);
        driver.complete(table, observed_sequence, true);

        let shared = driver.shared.lock().unwrap();
        let pending = shared.pending[&table];
        assert!(pending.sequence > observed_sequence);
        assert!(pending.due_at <= Instant::now());
        assert_eq!(pending.failures, 0);
    }

    #[test]
    fn independently_opened_handles_share_one_root_driver_registry() {
        static NEXT_ROOT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = format!(
            "stream-driver-test:{}",
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        );
        let first = StreamFoldDriverRegistry::for_root(&root);
        let second = StreamFoldDriverRegistry::for_root(&root);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let driver = driver();
        let table = identity(9);
        driver.notify(table, true);
        let error = OmniError::manifest("transient fold failure");
        for expected_failures in 1_u32..=10 {
            let observed_sequence = driver.shared.lock().unwrap().pending[&table].sequence;
            let multiplier = 1_u32 << (expected_failures - 1).min(16);
            let expected_wait = STREAM_FOLD_RETRY_BASE
                .saturating_mul(multiplier)
                .min(STREAM_FOLD_RETRY_MAX);
            let before = Instant::now();
            driver.failed(table, observed_sequence, &error);
            let after = Instant::now();
            let shared = driver.shared.lock().unwrap();
            let pending = shared.pending[&table];
            assert_eq!(pending.failures, expected_failures);
            assert!(pending.due_at >= before + expected_wait);
            assert!(pending.due_at <= after + expected_wait);
        }
        assert_eq!(driver.shared.lock().unwrap().pending[&table].failures, 10);
    }

    #[test]
    fn start_refuses_an_unfinished_task_after_stop_was_requested() {
        let driver = driver();
        assert!(driver.existing_task_start_result().is_ok());
        driver.stop.store(true, Ordering::Release);
        let error = driver
            .existing_task_start_result()
            .expect_err("a canceled or timed-out shutdown must not look like an idempotent start");
        assert!(error.to_string().contains("shutdown is still in progress"));
    }

    #[test]
    fn failing_an_old_attempt_preserves_a_newer_pressure_deadline() {
        let driver = driver();
        let table = identity(10);
        driver.notify(table, false);
        let observed_sequence = driver.shared.lock().unwrap().pending[&table].sequence;
        driver.notify(table, true);
        driver.failed(
            table,
            observed_sequence,
            &OmniError::manifest("stale attempt failed"),
        );

        let shared = driver.shared.lock().unwrap();
        let pending = shared.pending[&table];
        assert!(pending.sequence > observed_sequence);
        assert!(pending.due_at <= Instant::now());
        assert_eq!(pending.failures, 0);
    }
}
