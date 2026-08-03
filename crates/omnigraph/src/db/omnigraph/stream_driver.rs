//! Hidden resident fold supervisor for RFC-026 F5a.
//!
//! The supervisor is orchestration only. It owns no durable queue and adds no
//! persisted grammar: readiness is derived from the manifest-selected
//! lifecycle plus the current Lance MemWAL authority, and every effect still
//! goes through the existing recovery-v14 fold or lifecycle adapter.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::FutureExt;
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::db::manifest::stream::DrainGoal;
use crate::db::manifest::{
    StreamLifecycle, StreamLifecycleEntry, StreamProfileMode, TableIdentity,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::db::{Omnigraph, ReadTarget, Snapshot};
use crate::error::{OmniError, Result};
use crate::failpoints::{maybe_fail, names};
use crate::table_store::mem_wal::{CheckedRuntimeShutdownAuthority, StreamFoldProducerPermit};

use super::stream_ingest::{ResidentFoldOutcome, ResidentQuiesceOutcome};

/// A non-full generation becomes visible on this cadence. Capacity pressure
/// shortens the same pending entry to `now`; it never creates another job.
const STREAM_FOLD_MAX_STALENESS: Duration = Duration::from_secs(1);
const STREAM_FOLD_RETRY_BASE: Duration = Duration::from_millis(100);
const STREAM_FOLD_RETRY_MAX: Duration = Duration::from_secs(5);
const STREAM_FOLD_IDLE_WAIT: Duration = Duration::from_secs(60);
const STREAM_FOLD_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

fn duration_millis_saturating(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy)]
struct PendingFold {
    sequence: u64,
    due_at: Instant,
    failures: u32,
}

/// Process-local supervisor state is operational evidence only. It must never
/// be used as lifecycle, recovery, or MemWAL authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // Production-compiled F6b6 status is attached by F7.
pub(super) enum StreamFoldDriverSnapshotScope {
    ProcessLocalAdvisory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StreamFoldDriverRunState {
    Stopped,
    Running,
    Stopping,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StreamFoldCompletionOutcome {
    PublishedOpenFold,
    Idle,
    Inactive,
    Sealed,
    NoLongerEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StreamFoldDriverErrorKind {
    RetryScheduled,
    StaleAttemptFailed,
    DataBlocked,
    TriggerSequenceOverflow,
    UnexpectedStop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(dead_code)] // Production-compiled F6b6 status is attached by F7.
pub(super) struct StreamFoldPendingTriggerSnapshot {
    pub(super) table_identity: TableIdentity,
    pub(super) trigger_sequence: u64,
    pub(super) consecutive_failures: u32,
    pub(super) due_in_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct StreamFoldCompletionEvent {
    pub(super) event_sequence: u64,
    pub(super) table_identity: TableIdentity,
    pub(super) trigger_sequence: u64,
    pub(super) outcome: StreamFoldCompletionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct StreamFoldDriverErrorEvent {
    pub(super) event_sequence: u64,
    pub(super) kind: StreamFoldDriverErrorKind,
    pub(super) table_identity: Option<TableIdentity>,
    pub(super) trigger_sequence: Option<u64>,
    pub(super) retry_in_ms: Option<u64>,
    pub(super) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(dead_code)] // Production-compiled F6b6 status is attached by F7.
pub(super) struct StreamFoldDriverSnapshot {
    pub(super) scope: StreamFoldDriverSnapshotScope,
    pub(super) authoritative: bool,
    pub(super) state: StreamFoldDriverRunState,
    pub(super) pending_triggers: Vec<StreamFoldPendingTriggerSnapshot>,
    pub(super) published_open_folds: u64,
    pub(super) last_completion: Option<StreamFoldCompletionEvent>,
    pub(super) last_error: Option<StreamFoldDriverErrorEvent>,
}

#[derive(Debug)]
struct DriverHealth {
    run_state: StreamFoldDriverRunState,
    published_open_folds: u64,
    latest_event_sequence: u64,
    last_completion: Option<StreamFoldCompletionEvent>,
    last_error: Option<StreamFoldDriverErrorEvent>,
}

impl Default for DriverHealth {
    fn default() -> Self {
        Self {
            run_state: StreamFoldDriverRunState::Stopped,
            published_open_folds: 0,
            latest_event_sequence: 0,
            last_completion: None,
            last_error: None,
        }
    }
}

impl DriverHealth {
    fn next_event_sequence(&mut self) -> u64 {
        self.latest_event_sequence = self.latest_event_sequence.saturating_add(1);
        self.latest_event_sequence
    }

    fn record_completion(
        &mut self,
        table_identity: TableIdentity,
        trigger_sequence: u64,
        outcome: StreamFoldCompletionOutcome,
    ) {
        let event_sequence = self.next_event_sequence();
        self.last_completion = Some(StreamFoldCompletionEvent {
            event_sequence,
            table_identity,
            trigger_sequence,
            outcome,
        });
    }

    fn record_error(
        &mut self,
        kind: StreamFoldDriverErrorKind,
        table_identity: Option<TableIdentity>,
        trigger_sequence: Option<u64>,
        retry_in: Option<Duration>,
        message: String,
    ) {
        let event_sequence = self.next_event_sequence();
        self.last_error = Some(StreamFoldDriverErrorEvent {
            event_sequence,
            kind,
            table_identity,
            trigger_sequence,
            retry_in_ms: retry_in.map(duration_millis_saturating),
            message,
        });
    }
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
        let (overflow, trigger_sequence) = {
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
            (overflow, pending.sequence)
        };
        if overflow {
            let message =
                format!("stream fold trigger sequence overflow for table identity {identity}");
            shared.health.record_error(
                StreamFoldDriverErrorKind::TriggerSequenceOverflow,
                Some(identity),
                Some(trigger_sequence),
                None,
                message,
            );
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

    /// Re-read the finite round after the exclusive producer fence closes.
    /// A producer that began before the fence may have installed the one
    /// resident writer while its ordinary timer is not due yet. Include that
    /// exact pending identity; the empty-owner housekeeping prepass releases
    /// the sole root slot before the ordinary node-before-edge round.
    /// Unrelated failures retain their backoff.
    fn fenced_round(
        &self,
        now: Instant,
        resident: &BTreeSet<TableIdentity>,
    ) -> Vec<(TableIdentity, u64)> {
        let stopping = self.stop.load(Ordering::Acquire);
        self.shared
            .lock()
            .expect("stream fold driver state poisoned")
            .pending
            .iter()
            .filter_map(|(identity, pending)| {
                (stopping || pending.due_at <= now || resident.contains(identity))
                    .then_some((*identity, pending.sequence))
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

    fn complete(
        &self,
        identity: TableIdentity,
        observed_sequence: u64,
        outcome: StreamFoldCompletionOutcome,
    ) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        if outcome == StreamFoldCompletionOutcome::PublishedOpenFold {
            shared.health.published_open_folds =
                shared.health.published_open_folds.saturating_add(1);
        }
        shared
            .health
            .record_completion(identity, observed_sequence, outcome);
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
        shared.health.record_error(
            StreamFoldDriverErrorKind::DataBlocked,
            Some(identity),
            Some(observed_sequence),
            None,
            error.to_string(),
        );
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
        let error_kind = if retry_in.is_some() {
            StreamFoldDriverErrorKind::RetryScheduled
        } else {
            StreamFoldDriverErrorKind::StaleAttemptFailed
        };
        shared.health.record_error(
            error_kind,
            Some(identity),
            Some(observed_sequence),
            retry_in,
            error.to_string(),
        );
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

    fn mark_running(&self) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        // A deliberate restart is the acknowledgement boundary for a prior
        // failed task. Historical errors remain visible and are ordered
        // against later completions by their event sequence.
        shared.health.run_state = StreamFoldDriverRunState::Running;
    }

    fn mark_stopping(&self) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        if shared.health.run_state != StreamFoldDriverRunState::Failed {
            shared.health.run_state = StreamFoldDriverRunState::Stopping;
        }
    }

    fn mark_stopped(&self) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        if shared.health.run_state != StreamFoldDriverRunState::Failed {
            shared.health.run_state = StreamFoldDriverRunState::Stopped;
        }
    }

    fn mark_unexpected_stop(&self, message: String) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        shared.health.run_state = StreamFoldDriverRunState::Failed;
        shared.health.record_error(
            StreamFoldDriverErrorKind::UnexpectedStop,
            None,
            None,
            None,
            message.clone(),
        );
        tracing::error!(error = %message, "stream fold driver stopped unexpectedly");
    }

    fn order_round(&self, candidates: Vec<DriverCandidate>) -> Vec<DriverCandidate> {
        let shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
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
        let (mut nodes, mut edges): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|candidate| candidate.kind == StreamLaneKind::Node);
        rotate_after(&mut nodes, shared.last_node);
        rotate_after(&mut edges, shared.last_edge);
        nodes.extend(edges);
        nodes
    }

    fn split_empty_owner_cleanup(
        candidates: Vec<DriverCandidate>,
        idle_resident: &BTreeSet<TableIdentity>,
    ) -> (Vec<DriverCandidate>, Vec<DriverCandidate>) {
        let (mut cleanup, remaining): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(|candidate| {
                candidate.action == DriverCandidateAction::FoldOpen
                    && idle_resident.contains(&candidate.identity)
            });
        cleanup.sort_by_key(|candidate| candidate.identity);
        (cleanup, remaining)
    }

    fn mark_attempted(&self, kind: StreamLaneKind, identity: TableIdentity) {
        let mut shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        match kind {
            StreamLaneKind::Node => shared.last_node = Some(identity),
            StreamLaneKind::Edge => shared.last_edge = Some(identity),
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
            match finished.await {
                Ok(outcome) => outcome?,
                Err(join_error) => {
                    let error = OmniError::manifest_internal(format!(
                        "prior stream fold driver task failed to join: {join_error}"
                    ));
                    self.mark_unexpected_stop(error.to_string());
                    return Err(error);
                }
            }
        }

        // Make cold-start discovery part of server startup, rather than the
        // first fallible action in a detached task. A graph whose manifest
        // cannot establish its eligible OPEN or DRAINING lanes must refuse
        // startup instead of briefly serving beside an already-dead supervisor.
        let initial = db.stream_driver_eligible_identities().await?;
        self.stop.store(false, Ordering::Release);
        self.mark_running();
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
                    driver.mark_stopped();
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

    async fn shutdown(&self) -> Result<()> {
        // Start and stop serialize through the task slot before either changes
        // the shared stop flag. Otherwise a concurrent start could reset a
        // pre-lock stop request and leave shutdown joining a live idle task.
        let mut task_slot = self.task.lock().await;
        self.stop.store(true, Ordering::Release);
        self.mark_stopping();
        // `notify_one` retains a permit when the task is between its stop check
        // and construction of the wait future. `notify_waiters` would lose
        // that race and leave an idle driver asleep past the shutdown bound.
        self.wake.notify_one();
        // Retain this mutex across the join. Start and stop are ownership
        // transitions, not mere slot observations; releasing it while the
        // first owner is still being joined would allow a concurrent start to
        // install a second task.
        let Some(task) = task_slot.as_mut() else {
            self.mark_stopped();
            return Ok(());
        };
        let joined = task.await;
        task_slot.take();
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(join_error) => {
                let error = OmniError::manifest_internal(format!(
                    "stream fold driver task failed during shutdown: {join_error}"
                ));
                self.mark_unexpected_stop(error.to_string());
                Err(error)
            }
        };
        if outcome.is_ok() {
            self.mark_stopped();
        }
        outcome
    }

    /// Capture process-local advisory state without consulting the task lock,
    /// manifest, MemWAL, or any other authority-bearing source.
    pub(super) fn snapshot(&self) -> StreamFoldDriverSnapshot {
        let now = Instant::now();
        let shared = self
            .shared
            .lock()
            .expect("stream fold driver state poisoned");
        let pending_triggers = shared
            .pending
            .iter()
            .map(
                |(table_identity, pending)| StreamFoldPendingTriggerSnapshot {
                    table_identity: *table_identity,
                    trigger_sequence: pending.sequence,
                    consecutive_failures: pending.failures,
                    due_in_ms: duration_millis_saturating(
                        pending.due_at.saturating_duration_since(now),
                    ),
                },
            )
            .collect();
        StreamFoldDriverSnapshot {
            scope: StreamFoldDriverSnapshotScope::ProcessLocalAdvisory,
            authoritative: false,
            state: shared.health.run_state,
            pending_triggers,
            published_open_folds: shared.health.published_open_folds,
            last_completion: shared.health.last_completion.clone(),
            last_error: shared.health.last_error.clone(),
        }
    }

    #[cfg(feature = "failpoints")]
    fn status_json(&self) -> String {
        serde_json::to_string(&self.snapshot())
            .expect("stream fold driver advisory snapshot must serialize")
    }
}

#[derive(Debug)]
struct DriverCandidate {
    identity: TableIdentity,
    observed_sequence: u64,
    table_key: String,
    kind: StreamLaneKind,
    action: DriverCandidateAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamLaneKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverCandidateAction {
    FoldOpen,
    ContinueDrain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OrderedEnrolledStreamLane {
    pub(super) identity: TableIdentity,
    pub(super) table_key: String,
    pub(super) kind: StreamLaneKind,
}

fn driver_candidate_action(lifecycle: &StreamLifecycleEntry) -> Option<DriverCandidateAction> {
    if lifecycle.strict_block.is_some() {
        return None;
    }
    match lifecycle.lifecycle {
        StreamLifecycle::Open => Some(DriverCandidateAction::FoldOpen),
        StreamLifecycle::Draining => lifecycle
            .drain
            .as_ref()
            .filter(|drain| drain.goal == DrainGoal::Sealed)
            .map(|_| DriverCandidateAction::ContinueDrain),
        StreamLifecycle::Sealed => None,
    }
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
        // Close the producer side before selecting the actual finite round.
        // A put which started before this writer queued may finish and notify
        // while we wait; the fresh selection below includes that resident
        // identity even when its ordinary timer is not due yet. Later puts
        // remain queued until every captured node and edge has had one attempt.
        maybe_fail(names::STREAM_DRIVER_BEFORE_ROUND_ACQUIRE)?;
        let _round_admission = db.stream_workers.acquire_stream_fold_round().await;
        let resident = db
            .stream_workers
            .served_runtime_resident_identities()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        let idle_resident = db
            .stream_workers
            .driver_idle_resident_identities()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        let due = driver.fenced_round(Instant::now(), &resident);
        if due.is_empty() {
            drop(db);
            continue;
        }
        let (empty_cleanup, candidates) = match db.stream_driver_candidates(&due).await {
            Ok(candidates) => {
                let (empty_cleanup, candidates) =
                    StreamFoldDriverRegistry::split_empty_owner_cleanup(candidates, &idle_resident);
                (empty_cleanup, driver.order_round(candidates))
            }
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
        let active = empty_cleanup
            .iter()
            .chain(candidates.iter())
            .map(|candidate| candidate.identity)
            .collect::<std::collections::BTreeSet<_>>();
        for (identity, sequence) in &due {
            if !active.contains(identity) {
                driver.complete(
                    *identity,
                    *sequence,
                    StreamFoldCompletionOutcome::NoLongerEligible,
                );
            }
        }

        if let Err(error) = db.heal_pending_recovery_sidecars_for_write(&[None]).await {
            let stopping = driver.stop.load(Ordering::Acquire);
            for candidate in empty_cleanup.iter().chain(candidates.iter()) {
                driver.failed(candidate.identity, candidate.observed_sequence, &error);
            }
            if stopping {
                return Err(error);
            }
            drop(db);
            continue;
        }

        // Empty-owner housekeeping plus the ordered candidate vector are the
        // complete finite round. This failpoints-only rendezvous proves that a
        // trigger arriving after this cut cannot enter ahead of an edge already
        // selected below.
        maybe_fail(names::STREAM_DRIVER_POST_ROUND_FREEZE)?;

        // Resume publishes OPEN with an intentionally empty writer. Retire
        // only owners observed empty under the root producer fence before the
        // semantic candidate order, so a cold lane can claim the sole slot
        // without allowing a productive edge to overtake a runnable node.
        let mut cleanup_failed = false;
        for candidate in empty_cleanup {
            match db
                .stream_retire_empty_from_resident_driver(candidate.identity, &candidate.table_key)
                .await
            {
                Ok(ResidentFoldOutcome::Idle) => driver.complete(
                    candidate.identity,
                    candidate.observed_sequence,
                    StreamFoldCompletionOutcome::Idle,
                ),
                Ok(ResidentFoldOutcome::Inactive) => driver.complete(
                    candidate.identity,
                    candidate.observed_sequence,
                    StreamFoldCompletionOutcome::Inactive,
                ),
                Ok(ResidentFoldOutcome::Published) => {
                    let error = OmniError::manifest_internal(
                        "empty stream owner housekeeping unexpectedly published a fold",
                    );
                    driver.failed(candidate.identity, candidate.observed_sequence, &error);
                    if driver.stop.load(Ordering::Acquire) {
                        return Err(error);
                    }
                    cleanup_failed = true;
                }
                Err(error) => {
                    driver.failed(candidate.identity, candidate.observed_sequence, &error);
                    if driver.stop.load(Ordering::Acquire) {
                        return Err(error);
                    }
                    cleanup_failed = true;
                }
            }
        }
        if cleanup_failed {
            drop(db);
            continue;
        }

        // `candidates` is one finite manifest-derived round. New triggers do
        // not enter it, so continuously active node lanes cannot starve an edge
        // that was ready when this round began.
        for candidate in candidates {
            driver.mark_attempted(candidate.kind, candidate.identity);
            match candidate.action {
                DriverCandidateAction::FoldOpen => {
                    match db
                        .stream_fold_from_resident_driver(candidate.identity, &candidate.table_key)
                        .await
                    {
                        Ok(ResidentFoldOutcome::Published) => driver.complete(
                            candidate.identity,
                            candidate.observed_sequence,
                            StreamFoldCompletionOutcome::PublishedOpenFold,
                        ),
                        Ok(ResidentFoldOutcome::Idle) => driver.complete(
                            candidate.identity,
                            candidate.observed_sequence,
                            StreamFoldCompletionOutcome::Idle,
                        ),
                        Ok(ResidentFoldOutcome::Inactive) => driver.complete(
                            candidate.identity,
                            candidate.observed_sequence,
                            StreamFoldCompletionOutcome::Inactive,
                        ),
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
                DriverCandidateAction::ContinueDrain => {
                    match db
                        .stream_quiesce_from_resident_driver(
                            candidate.identity,
                            &candidate.table_key,
                        )
                        .await
                    {
                        Ok(ResidentQuiesceOutcome::Sealed) => driver.complete(
                            candidate.identity,
                            candidate.observed_sequence,
                            StreamFoldCompletionOutcome::Sealed,
                        ),
                        Ok(ResidentQuiesceOutcome::Inactive) => driver.complete(
                            candidate.identity,
                            candidate.observed_sequence,
                            StreamFoldCompletionOutcome::Inactive,
                        ),
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
            }
        }
        drop(db);
    }
}

impl Omnigraph {
    /// Join the shared side of the process-local finite-round fence before
    /// taking profile authority. The guard must transfer into detached put
    /// ownership so cancellation cannot reopen the scheduling race.
    pub(super) async fn acquire_stream_fold_producer_guard(&self) -> StreamFoldProducerPermit {
        self.stream_workers.acquire_stream_fold_producer().await
    }

    pub(super) fn notify_stream_fold_pending(&self, identity: TableIdentity) {
        self.stream_fold_driver.notify(identity, false);
    }

    pub(super) fn notify_stream_fold_pressure(&self, identity: TableIdentity) {
        self.stream_fold_driver.notify(identity, true);
    }

    pub(super) fn ordered_enrolled_stream_lanes(
        &self,
        snapshot: &Snapshot,
    ) -> Result<Vec<OrderedEnrolledStreamLane>> {
        let catalog = self.catalog();
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "stream lane ordering requires the identity-bound accepted catalog",
            )
        })?;
        let nodes = schema_ir
            .nodes
            .iter()
            .map(|node| TableIdentity::new(node.type_id.get(), node.table_incarnation_id.get()))
            .collect::<Result<BTreeSet<_>>>()?;
        let edges = schema_ir
            .edges
            .iter()
            .map(|edge| TableIdentity::new(edge.type_id.get(), edge.table_incarnation_id.get()))
            .collect::<Result<BTreeSet<_>>>()?;
        let mut lanes = snapshot
            .entries()
            .filter(|entry| snapshot.stream_lifecycle(entry.identity).is_some())
            .map(|entry| {
                let kind = if nodes.contains(&entry.identity) {
                    StreamLaneKind::Node
                } else if edges.contains(&entry.identity) {
                    StreamLaneKind::Edge
                } else {
                    return Err(OmniError::manifest_internal(format!(
                        "enrolled stream lane {} is absent from the accepted schema identity set",
                        entry.identity
                    )));
                };
                Ok(OrderedEnrolledStreamLane {
                    identity: entry.identity,
                    table_key: entry.table_key.clone(),
                    kind,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        lanes.sort_by_key(|lane| {
            (
                match lane.kind {
                    StreamLaneKind::Node => 0_u8,
                    StreamLaneKind::Edge => 1_u8,
                },
                lane.identity,
            )
        });
        Ok(lanes)
    }

    async fn stream_driver_eligible_identities(&self) -> Result<Vec<TableIdentity>> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        if snapshot.stream_profile().mode() != StreamProfileMode::Enabled {
            return Ok(Vec::new());
        }
        Ok(self
            .ordered_enrolled_stream_lanes(&snapshot)?
            .into_iter()
            .filter_map(|lane| {
                snapshot
                    .stream_lifecycle(lane.identity)
                    .and_then(driver_candidate_action)
                    .map(|_| lane.identity)
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
        let candidates = self
            .ordered_enrolled_stream_lanes(&snapshot)?
            .into_iter()
            .filter_map(|lane| {
                let observed_sequence = due.get(&lane.identity).copied()?;
                let lifecycle = snapshot.stream_lifecycle(lane.identity)?;
                let action = driver_candidate_action(lifecycle)?;
                Some(DriverCandidate {
                    identity: lane.identity,
                    observed_sequence,
                    table_key: lane.table_key,
                    kind: lane.kind,
                    action,
                })
            })
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

    async fn shutdown_stream_runtime_background(self: &Arc<Self>) -> Result<()> {
        // Axum has already stopped admission and settled request futures. A
        // producer now takes the root opportunity before profile authority and
        // transfers both into detached ownership. Close those gates in the
        // same order: root-exclusive first, then profile-exclusive. Together
        // they prove that no canceled pre-invocation owner can create a fold
        // trigger after this barrier.
        //
        // Drop both before joining: the driver needs root-exclusive plus
        // shared profile admission to drain triggers that became ready ahead
        // of the barrier.
        let producer_round = self.stream_workers.acquire_stream_fold_round().await;
        let producers_settled = self.write_queue().acquire_stream_profile_exclusive().await;
        drop(producers_settled);
        drop(producer_round);
        self.stream_fold_driver.shutdown().await?;

        // The supervisor needed profile-shared admission to settle its final
        // finite round. Close the profile again after it has joined, then take
        // every still-resident table admission domain in stable identity
        // order. No new slot can appear while this graph-global guard is held.
        let runtime_profile = self.write_queue().acquire_stream_profile_exclusive().await;
        let identities = self
            .stream_workers
            .served_runtime_resident_identities()
            .await;
        let mut admission_guards = Vec::with_capacity(identities.len());
        for identity in identities {
            let admission_key = StreamAdmissionKey::for_resolved_ref(identity, None);
            let guard = self
                .write_queue()
                .acquire_stream_exclusive(&admission_key)
                .await;
            admission_guards.push((identity, guard));
        }
        let authority = CheckedRuntimeShutdownAuthority::new(runtime_profile, admission_guards);
        self.stream_workers
            .shutdown_served_runtime_owned(authority)
            .await
            .map_err(|error| {
                OmniError::manifest(format!("served stream runtime shutdown failed: {error}"))
            })
    }

    /// After the caller has stopped transport admission, fence detached
    /// trigger creation, join the resident supervisor, then abort and join all
    /// process-local MemWAL owners under one deadline. The complete handoff is
    /// detached and retains the checked engine registration, so cancellation
    /// or timeout can never reopen offline authority while cleanup is live.
    #[doc(hidden)]
    pub async fn shutdown_stream_fold_driver(self: &Arc<Self>) -> Result<()> {
        if self.stream_runtime_authority.is_none() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "resident fold driver shutdown requires the checked cluster-served runtime handle that owns it"
                .to_string(),
            });
        }
        let deadline = Instant::now() + STREAM_FOLD_SHUTDOWN_DEADLINE;
        let db = Arc::clone(self);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let outcome = AssertUnwindSafe(db.shutdown_stream_runtime_background())
                .catch_unwind()
                .await;
            match outcome {
                Ok(Ok(())) => {
                    let _ = result_tx.send(Ok(()));
                }
                Ok(Err(error)) => {
                    let _ = result_tx.send(Err(error));
                    // Cleanup may have failed before or after transferring its
                    // checked gates to the worker owner. Retain the engine so
                    // the exclusive local runtime registration cannot
                    // disappear after either failure.
                    let _db = db;
                    std::future::pending::<()>().await;
                }
                Err(_) => {
                    let error = OmniError::manifest_internal(
                        "served stream runtime shutdown owner panicked",
                    );
                    let _ = result_tx.send(Err(error));
                    let _db = db;
                    std::future::pending::<()>().await;
                }
            }
        });

        match tokio::time::timeout_at(deadline, result_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(OmniError::manifest_internal(
                "served stream runtime shutdown owner exited without reporting an outcome",
            )),
            Err(_) => Err(OmniError::manifest(format!(
                "served stream runtime did not settle within {} seconds; its detached owner still retains runtime and admission authority",
                STREAM_FOLD_SHUTDOWN_DEADLINE.as_secs()
            ))),
        }
    }

    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    /// Test-only process-local advisory evidence. Durable status and recovery
    /// decisions must continue to use the manifest-selected stream authority.
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

    fn candidate(stable: u64, kind: StreamLaneKind) -> DriverCandidate {
        DriverCandidate {
            identity: identity(stable),
            observed_sequence: 1,
            table_key: format!("diagnostic:{stable}"),
            kind,
            action: DriverCandidateAction::FoldOpen,
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
            candidate(4, StreamLaneKind::Edge),
            candidate(2, StreamLaneKind::Node),
            candidate(3, StreamLaneKind::Edge),
            candidate(1, StreamLaneKind::Node),
        ]);
        assert_eq!(
            first.iter().map(|item| item.identity).collect::<Vec<_>>(),
            vec![identity(1), identity(2), identity(3), identity(4)]
        );

        driver.mark_attempted(StreamLaneKind::Node, identity(1));
        driver.mark_attempted(StreamLaneKind::Edge, identity(3));
        let second = driver.order_round(vec![
            candidate(1, StreamLaneKind::Node),
            candidate(3, StreamLaneKind::Edge),
            candidate(2, StreamLaneKind::Node),
            candidate(4, StreamLaneKind::Edge),
        ]);
        assert_eq!(
            second.iter().map(|item| item.identity).collect::<Vec<_>>(),
            vec![identity(2), identity(1), identity(4), identity(3)]
        );
    }

    #[test]
    fn empty_owner_cleanup_does_not_reorder_productive_candidates() {
        let driver = driver();
        let idle_resident = BTreeSet::from([identity(4)]);
        let (cleanup, candidates) = StreamFoldDriverRegistry::split_empty_owner_cleanup(
            vec![
                candidate(1, StreamLaneKind::Node),
                candidate(4, StreamLaneKind::Edge),
                candidate(2, StreamLaneKind::Node),
            ],
            &idle_resident,
        );
        let ordered = driver.order_round(candidates);

        assert_eq!(
            cleanup
                .iter()
                .map(|candidate| candidate.identity)
                .collect::<Vec<_>>(),
            vec![identity(4)]
        );
        assert_eq!(
            ordered
                .iter()
                .map(|candidate| candidate.identity)
                .collect::<Vec<_>>(),
            vec![identity(1), identity(2)]
        );
    }

    #[test]
    fn completing_an_old_attempt_preserves_a_newer_pressure_deadline() {
        let driver = driver();
        let table = identity(7);
        driver.notify(table, false);
        let observed_sequence = driver.shared.lock().unwrap().pending[&table].sequence;
        driver.notify(table, true);
        driver.complete(
            table,
            observed_sequence,
            StreamFoldCompletionOutcome::PublishedOpenFold,
        );

        let shared = driver.shared.lock().unwrap();
        let pending = shared.pending[&table];
        assert!(pending.sequence > observed_sequence);
        assert!(pending.due_at <= Instant::now());
        assert_eq!(pending.failures, 0);
    }

    #[test]
    fn advisory_snapshot_is_explicit_and_orders_pending_triggers_by_identity() {
        let driver = driver();
        driver.notify(identity(12), false);
        driver.notify(identity(11), false);

        let snapshot = driver.snapshot();
        assert_eq!(
            snapshot.scope,
            StreamFoldDriverSnapshotScope::ProcessLocalAdvisory
        );
        assert!(!snapshot.authoritative);
        assert_eq!(snapshot.state, StreamFoldDriverRunState::Stopped);
        assert_eq!(
            snapshot
                .pending_triggers
                .iter()
                .map(|trigger| trigger.table_identity)
                .collect::<Vec<_>>(),
            vec![identity(11), identity(12)]
        );
        assert!(
            snapshot
                .pending_triggers
                .iter()
                .all(|trigger| trigger.trigger_sequence == 1
                    && trigger.consecutive_failures == 0
                    && trigger.due_in_ms <= duration_millis_saturating(STREAM_FOLD_MAX_STALENESS))
        );

        let json = serde_json::to_value(snapshot).unwrap();
        assert_eq!(json["scope"], "process_local_advisory");
        assert_eq!(json["authoritative"], false);
        assert_eq!(json["state"], "stopped");
        assert_eq!(json["pending_triggers"].as_array().unwrap().len(), 2);
        assert!(json.get("pending_tables").is_none());
        assert!(json.get("published_folds").is_none());
    }

    #[test]
    fn retry_then_publish_preserves_error_history_and_sequences_events() {
        let driver = driver();
        let table = identity(13);
        driver.mark_running();
        driver.notify(table, true);
        let observed_sequence = driver.shared.lock().unwrap().pending[&table].sequence;
        driver.failed(
            table,
            observed_sequence,
            &OmniError::manifest("transient fold failure"),
        );

        let failed = driver.snapshot();
        assert_eq!(failed.state, StreamFoldDriverRunState::Running);
        assert_eq!(failed.pending_triggers.len(), 1);
        assert_eq!(failed.pending_triggers[0].consecutive_failures, 1);
        assert!(
            failed.pending_triggers[0].due_in_ms
                <= duration_millis_saturating(STREAM_FOLD_RETRY_BASE)
        );
        let error = failed.last_error.as_ref().unwrap();
        assert_eq!(error.event_sequence, 1);
        assert_eq!(error.kind, StreamFoldDriverErrorKind::RetryScheduled);
        assert_eq!(error.table_identity, Some(table));
        assert_eq!(error.trigger_sequence, Some(observed_sequence));
        assert_eq!(
            error.retry_in_ms,
            Some(duration_millis_saturating(STREAM_FOLD_RETRY_BASE))
        );
        assert!(failed.last_completion.is_none());

        driver.complete(
            table,
            observed_sequence,
            StreamFoldCompletionOutcome::PublishedOpenFold,
        );
        let published = driver.snapshot();
        assert!(published.pending_triggers.is_empty());
        assert_eq!(published.published_open_folds, 1);
        let completion = published.last_completion.as_ref().unwrap();
        assert_eq!(completion.event_sequence, 2);
        assert_eq!(completion.table_identity, table);
        assert_eq!(completion.trigger_sequence, observed_sequence);
        assert_eq!(
            completion.outcome,
            StreamFoldCompletionOutcome::PublishedOpenFold
        );
        assert_eq!(
            published.last_error, failed.last_error,
            "a later success must not erase historical failure evidence"
        );
    }

    #[test]
    fn advisory_run_state_tracks_stopping_and_preserves_failure() {
        let driver = driver();
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Stopped);

        driver.mark_running();
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Running);
        driver.mark_stopping();
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Stopping);
        driver.mark_stopped();
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Stopped);

        driver.mark_unexpected_stop("driver task exited".to_string());
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Failed);
        assert_eq!(
            driver.snapshot().last_error.unwrap().kind,
            StreamFoldDriverErrorKind::UnexpectedStop
        );
        driver.mark_stopping();
        driver.mark_stopped();
        assert_eq!(
            driver.snapshot().state,
            StreamFoldDriverRunState::Failed,
            "shutdown must not hide a prior failed task"
        );

        driver.mark_running();
        assert_eq!(driver.snapshot().state, StreamFoldDriverRunState::Running);
    }

    #[test]
    fn trigger_sequence_overflow_is_reported_without_falsifying_run_state() {
        let driver = driver();
        let table = identity(14);
        driver.mark_running();
        driver.shared.lock().unwrap().pending.insert(
            table,
            PendingFold {
                sequence: u64::MAX,
                due_at: Instant::now(),
                failures: 0,
            },
        );

        driver.notify(table, true);

        let snapshot = driver.snapshot();
        assert_eq!(snapshot.state, StreamFoldDriverRunState::Running);
        assert_eq!(snapshot.pending_triggers[0].trigger_sequence, u64::MAX);
        let error = snapshot.last_error.unwrap();
        assert_eq!(
            error.kind,
            StreamFoldDriverErrorKind::TriggerSequenceOverflow
        );
        assert_eq!(error.table_identity, Some(table));
        assert_eq!(error.trigger_sequence, Some(u64::MAX));
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
        assert_eq!(
            shared.health.last_error.as_ref().unwrap().kind,
            StreamFoldDriverErrorKind::StaleAttemptFailed
        );
    }
}
