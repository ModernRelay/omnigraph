//! The failure model every maintenance run shares (RFC "Served graph
//! maintenance", Phase A): its budgets, the per-graph maintenance lane that
//! runs one maintenance operation at a time, the cancel points that stop a
//! run before its publication, the completion evidence its failures carry,
//! and the runtime that keeps Lance's CPU-heavy maintenance work off the
//! threads that serve requests.
//!
//! The lane is process-local derived state: it orders maintenance runs on
//! one graph root inside one process, as the write queue orders writers, and
//! it is not writer fencing. Fencing remains the publication's exact
//! compare-and-swap; a maintenance run in another process loses that
//! compare-and-swap as it does today.

use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use lance::dataset::optimize::CompactionOptions;
use lance_index::scalar::InvertedIndexParams;
use tokio::sync::{OwnedMutexGuard, watch};

use crate::error::{CompletionEvidence, OmniError, Result};

/// Maintenance units (one table's compaction or index work) a run executes at
/// once. Read by [`MaintenanceBudget::from_env`].
pub const MAINTENANCE_CONCURRENCY_ENV: &str = "OMNIGRAPH_MAINTENANCE_CONCURRENCY";
/// Bytes a compaction scan may queue in Lance's I/O buffer.
pub const MAINTENANCE_SCAN_IO_BUFFER_ENV: &str = "OMNIGRAPH_MAINTENANCE_SCAN_IO_BUFFER_BYTES";
/// Memory, in MiB, one full-text index build splits across its workers.
pub const MAINTENANCE_FULL_TEXT_MEMORY_ENV: &str = "OMNIGRAPH_MAINTENANCE_FULL_TEXT_MEMORY_MB";
/// Workers one full-text index build tokenizes with.
pub const MAINTENANCE_FULL_TEXT_WORKERS_ENV: &str = "OMNIGRAPH_MAINTENANCE_FULL_TEXT_WORKERS";

/// Rows per compaction scan batch where no Blob sizing derives one. It is the
/// value Lance falls back to (`BATCH_SIZE_FALLBACK`), set explicitly so that
/// `LANCE_DEFAULT_BATCH_SIZE`, a process-wide variable that also sizes every
/// read, does not size maintenance.
pub(crate) const COMPACTION_BATCH_ROWS: usize = 8192;

/// The resource budget of one maintenance run: how many units it executes at
/// once and the caps it sets on Lance's buffers. Every value has a default
/// and an environment variable, read by [`Self::from_env`]; a served graph
/// can set it on its handle with [`crate::db::Omnigraph::with_maintenance_budget`].
///
/// The caps bound Lance's own buffers; they are declared bounds, not measured
/// accounting of the process heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    unit_concurrency: usize,
    scan_io_buffer_bytes: u64,
    full_text_memory_mb: u64,
    full_text_workers: usize,
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl MaintenanceBudget {
    /// The most units one run may execute at once.
    pub const MAX_UNIT_CONCURRENCY: usize = 2;

    /// One unit at a time, a 64 MiB scan I/O buffer, and full-text builds of
    /// 512 MiB over two workers.
    pub const DEFAULT: Self = Self {
        unit_concurrency: 1,
        scan_io_buffer_bytes: 64 * 1024 * 1024,
        full_text_memory_mb: 512,
        full_text_workers: 2,
    };

    /// The defaults, overridden by the `OMNIGRAPH_MAINTENANCE_*` variables.
    /// A value that does not parse, or is zero, keeps its default; a unit
    /// concurrency above [`Self::MAX_UNIT_CONCURRENCY`] is lowered to it.
    /// Each such correction is logged.
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub(crate) fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let mut budget = Self::DEFAULT;
        if let Some(concurrency) = positive::<usize>(&lookup, MAINTENANCE_CONCURRENCY_ENV) {
            if concurrency > Self::MAX_UNIT_CONCURRENCY {
                tracing::warn!(
                    target: "omnigraph::maintenance",
                    variable = MAINTENANCE_CONCURRENCY_ENV,
                    requested = concurrency,
                    applied = Self::MAX_UNIT_CONCURRENCY,
                    "maintenance unit concurrency is at most {}",
                    Self::MAX_UNIT_CONCURRENCY
                );
            }
            budget.unit_concurrency = concurrency.min(Self::MAX_UNIT_CONCURRENCY);
        }
        if let Some(bytes) = positive(&lookup, MAINTENANCE_SCAN_IO_BUFFER_ENV) {
            budget.scan_io_buffer_bytes = bytes;
        }
        if let Some(megabytes) = positive(&lookup, MAINTENANCE_FULL_TEXT_MEMORY_ENV) {
            budget.full_text_memory_mb = megabytes;
        }
        if let Some(workers) = positive(&lookup, MAINTENANCE_FULL_TEXT_WORKERS_ENV) {
            budget.full_text_workers = workers;
        }
        budget
    }

    /// This budget with `units` executed at once, `1..=MAX_UNIT_CONCURRENCY`.
    pub fn with_unit_concurrency(mut self, units: usize) -> Result<Self> {
        if !(1..=Self::MAX_UNIT_CONCURRENCY).contains(&units) {
            return Err(OmniError::manifest(format!(
                "maintenance unit concurrency must be from 1 to {}, got {units}",
                Self::MAX_UNIT_CONCURRENCY
            )));
        }
        self.unit_concurrency = units;
        Ok(self)
    }

    /// This budget with a compaction scan I/O buffer of `bytes`, at least 1.
    pub fn with_scan_io_buffer_bytes(mut self, bytes: u64) -> Result<Self> {
        if bytes == 0 {
            // Lance reads a zero buffer as no backpressure at all.
            return Err(OmniError::manifest(
                "the maintenance scan I/O buffer must be at least one byte",
            ));
        }
        self.scan_io_buffer_bytes = bytes;
        Ok(self)
    }

    /// This budget with full-text builds of `memory_mb` MiB over `workers`
    /// workers, both at least 1.
    pub fn with_full_text_build(mut self, memory_mb: u64, workers: usize) -> Result<Self> {
        if memory_mb == 0 || workers == 0 {
            return Err(OmniError::manifest(
                "a full-text build needs at least 1 MiB and one worker",
            ));
        }
        self.full_text_memory_mb = memory_mb;
        self.full_text_workers = workers;
        Ok(self)
    }

    pub fn unit_concurrency(&self) -> usize {
        self.unit_concurrency
    }

    pub fn scan_io_buffer_bytes(&self) -> u64 {
        self.scan_io_buffer_bytes
    }

    pub fn full_text_memory_mb(&self) -> u64 {
        self.full_text_memory_mb
    }

    pub fn full_text_workers(&self) -> usize {
        self.full_text_workers
    }

    /// The compaction options a run plans and executes with: Lance's
    /// defaults with this budget's scan I/O buffer and an explicit batch
    /// size. `TableStore::stage_compaction` lowers the batch size of a Blob
    /// table's task to the size its rows derive.
    pub(crate) fn compaction_options(&self) -> CompactionOptions {
        CompactionOptions {
            batch_size: Some(COMPACTION_BATCH_ROWS),
            io_buffer_size: Some(self.scan_io_buffer_bytes),
            ..CompactionOptions::default()
        }
    }

    /// The parameters of a full-text index build: the engine's analyzer
    /// defaults with this budget's memory and workers. Lance splits the
    /// memory evenly across the workers and persists neither value with the
    /// index, so they change how a build runs, not what it writes.
    pub(crate) fn full_text_params(&self) -> InvertedIndexParams {
        InvertedIndexParams::default()
            .memory_limit_mb(self.full_text_memory_mb)
            .num_workers(self.full_text_workers)
    }
}

/// A positive integer from `name`, or `None` (with a warning when set but
/// unusable).
fn positive<T>(lookup: &impl Fn(&str) -> Option<String>, name: &str) -> Option<T>
where
    T: std::str::FromStr + PartialEq + Default + Copy,
{
    let raw = lookup(name)?;
    match raw.trim().parse::<T>() {
        Ok(value) if value != T::default() => Some(value),
        _ => {
            tracing::warn!(
                target: "omnigraph::maintenance",
                variable = name,
                "ignoring a maintenance budget variable that is not a positive integer"
            );
            None
        }
    }
}

/// The kind of maintenance run a lane holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MaintenanceOperation {
    Optimize,
    Cleanup,
    IndexBuild,
    FullTextRebuild,
}

impl MaintenanceOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Optimize => "optimize",
            Self::Cleanup => "cleanup",
            Self::IndexBuild => "index build",
            Self::FullTextRebuild => "full-text rebuild",
        }
    }
}

/// The run a graph's maintenance lane holds: its operation and the branch it
/// maintains (`None` for cleanup, which covers every branch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceInProgress {
    pub operation: MaintenanceOperation,
    pub branch: Option<String>,
}

/// One graph root's maintenance lane in this process: at most one
/// maintenance run at a time, the others waiting in arrival order in memory.
/// Nothing about it is stored. Shared by every handle on the root through
/// the root's write queue manager.
pub(crate) struct MaintenanceLane {
    turn: Arc<tokio::sync::Mutex<()>>,
    /// Bumped by every cancel; a run entered at generation `g` is cancelled
    /// once the generation passes `g`, so a cancel stops the run that holds
    /// the lane and every run waiting for it, and none that arrives later.
    cancels: watch::Sender<u64>,
    active: Mutex<Option<MaintenanceInProgress>>,
}

impl Default for MaintenanceLane {
    fn default() -> Self {
        Self {
            turn: Arc::new(tokio::sync::Mutex::new(())),
            cancels: watch::Sender::new(0),
            active: Mutex::new(None),
        }
    }
}

impl MaintenanceLane {
    /// Wait for the lane and hold it for one run. A cancel while waiting
    /// refuses the run before any effect.
    pub(crate) async fn enter(
        self: &Arc<Self>,
        operation: MaintenanceOperation,
        branch: Option<&str>,
    ) -> Result<MaintenanceRun> {
        let mut cancel = self.cancels.subscribe();
        let entered = *cancel.borrow_and_update();
        let turn = tokio::select! {
            biased;
            () = cancelled_after(&mut cancel, entered) => {
                return Err(OmniError::MaintenanceCancelled {
                    operation: operation.as_str(),
                }
                .with_completion_evidence(CompletionEvidence::BeforeEffect));
            }
            turn = Arc::clone(&self.turn).lock_owned() => turn,
        };
        *self.active.lock().expect("maintenance lane poisoned") = Some(MaintenanceInProgress {
            operation,
            branch: branch.map(str::to_string),
        });
        Ok(MaintenanceRun {
            lane: Arc::clone(self),
            _turn: turn,
            cancel,
            entered,
            operation,
            phase: AtomicU8::new(BEFORE_EFFECT),
        })
    }

    /// Cancel the run holding the lane and every run waiting for it. Each
    /// stops at its next cancel point; a publication in progress finishes.
    pub(crate) fn cancel(&self) {
        self.cancels.send_modify(|generation| *generation += 1);
    }

    pub(crate) fn in_progress(&self) -> Option<MaintenanceInProgress> {
        self.active
            .lock()
            .expect("maintenance lane poisoned")
            .clone()
    }
}

/// Resolves once the lane's cancel generation passes `entered`.
async fn cancelled_after(cancel: &mut watch::Receiver<u64>, entered: u64) {
    if cancel
        .wait_for(|generation| *generation > entered)
        .await
        .is_err()
    {
        // The lane outlives every run it holds; a closed channel cancels
        // nothing.
        std::future::pending::<()>().await;
    }
}

const BEFORE_EFFECT: u8 = 0;
const STAGED: u8 = 1;
const PUBLISHING: u8 = 2;
const PUBLISHED: u8 = 3;

/// One maintenance run holding its graph's lane. It records how far the run
/// has gone, so a failure carries the completion evidence of where it
/// stopped ([`Self::classify`]), and it answers the run's cancel points.
/// Dropping it releases the lane.
pub(crate) struct MaintenanceRun {
    lane: Arc<MaintenanceLane>,
    _turn: OwnedMutexGuard<()>,
    cancel: watch::Receiver<u64>,
    entered: u64,
    operation: MaintenanceOperation,
    phase: AtomicU8,
}

impl Drop for MaintenanceRun {
    fn drop(&mut self) {
        *self.lane.active.lock().expect("maintenance lane poisoned") = None;
    }
}

impl MaintenanceRun {
    /// The run may leave durable effects from here on: detached commits,
    /// staged files or deletions that no `__manifest` version names. Nothing
    /// is published.
    pub(crate) fn staged(&self) {
        self.phase.fetch_max(STAGED, Ordering::SeqCst);
    }

    /// The run hands its batch to the publisher, whose own evidence (or a
    /// publication in doubt) its failure carries.
    pub(crate) fn publishing(&self) {
        self.phase.fetch_max(PUBLISHING, Ordering::SeqCst);
    }

    /// The run's publication succeeded; a later failure follows it.
    pub(crate) fn published(&self) {
        self.phase.fetch_max(PUBLISHED, Ordering::SeqCst);
    }

    /// Give `error` the completion evidence of where the run stopped. Before
    /// any effect an opaque failure is `BeforeEffect`; after staged effects
    /// it is `Unpublished`; the publisher's own failure keeps what the
    /// publisher said, so a publication in doubt stays uncertain; after the
    /// publication every failure is `PartlyPublished`. A typed outcome
    /// before the publication keeps its shape.
    pub(crate) fn classify(&self, error: OmniError) -> OmniError {
        match self.phase.load(Ordering::SeqCst) {
            BEFORE_EFFECT => error.before_effect_if_opaque(),
            STAGED => error.unpublished(),
            PUBLISHING => error,
            _ => error.after_earlier_publication(),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self.cancel.borrow() > self.entered
    }

    /// A cancel point: refuse when the lane was cancelled since the run
    /// entered it.
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(self.cancelled());
        }
        Ok(())
    }

    /// Run `work` until it finishes or the lane is cancelled, in which case
    /// `work` is dropped where it stands. Only work whose durable effects
    /// stay unreachable may run here: reads, detached commits and staged
    /// files, never a publication or a deletion.
    pub(crate) async fn unless_cancelled<F: Future>(&self, work: F) -> Result<F::Output> {
        let mut cancel = self.cancel.clone();
        tokio::select! {
            biased;
            () = cancelled_after(&mut cancel, self.entered) => Err(self.cancelled()),
            output = work => Ok(output),
        }
    }

    fn cancelled(&self) -> OmniError {
        let evidence = match self.phase.load(Ordering::SeqCst) {
            BEFORE_EFFECT => CompletionEvidence::BeforeEffect,
            STAGED | PUBLISHING => CompletionEvidence::Unpublished,
            _ => CompletionEvidence::PartlyPublished {
                step_unpublished: true,
            },
        };
        OmniError::MaintenanceCancelled {
            operation: self.operation.as_str(),
        }
        .with_completion_evidence(evidence)
    }
}

/// Run `work`, a Lance call that does CPU-heavy maintenance work (a
/// compaction task, an index build), on the maintenance runtime, a small
/// runtime of its own, rather than on the caller's.
///
/// Lance decodes the batches a compaction reads in tasks it spawns with
/// `tokio::spawn` (`lance-encoding` `decoder.rs`), tokenizes a full-text
/// build's documents in tasks of a `JoinSet` (`lance-index`
/// `scalar/inverted/builder.rs`), and sorts a BTREE's training data in the
/// DataFusion stream it polls, all on whichever runtime polls the call.
/// Neither `spawn_blocking` nor Lance's CPU pool can hold that work: tasks
/// spawned from a blocking thread still land on the runtime's workers, and
/// Lance's pool takes only synchronous closures. Polling the call here puts
/// those tasks on this runtime's workers, so a server's request handlers and
/// its lease keeper keep theirs. Dropping the returned future aborts the
/// call, as dropping it in place would.
///
/// A thread running under the deterministic simulation harness polls `work`
/// in place: the harness schedules one thread per actor.
pub(crate) async fn off_serving_runtime<F>(work: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if harness_thread() {
        return work.await;
    }
    let task = maintenance_runtime().spawn(tracing::Instrument::in_current_span(work));
    let _abort = AbortOnDrop(task.abort_handle());
    match task.await {
        Ok(output) => output,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            // Only this future's own drop aborts the task, and then nothing
            // awaits it.
            Err(error) => panic!("maintenance runtime task failed: {error}"),
        },
    }
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether this thread runs under the deterministic simulation harness.
fn harness_thread() -> bool {
    crate::dst_clock::installed() || crate::dst_ids::installed()
}

/// The maintenance runtime: one worker per unit a run may execute at once,
/// built on first use and kept for the life of the process.
fn maintenance_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(MaintenanceBudget::MAX_UNIT_CONCURRENCY)
            .thread_name("omnigraph-maintenance")
            .enable_all()
            .build()
            .expect("the maintenance runtime starts")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn budget_defaults_and_overrides() {
        let defaults = MaintenanceBudget::from_lookup(lookup(&[]));
        assert_eq!(defaults, MaintenanceBudget::DEFAULT);
        assert_eq!(defaults.unit_concurrency(), 1);
        assert_eq!(defaults.scan_io_buffer_bytes(), 64 * 1024 * 1024);
        assert_eq!(defaults.full_text_memory_mb(), 512);
        assert_eq!(defaults.full_text_workers(), 2);

        let set = MaintenanceBudget::from_lookup(lookup(&[
            (MAINTENANCE_CONCURRENCY_ENV, "2"),
            (MAINTENANCE_SCAN_IO_BUFFER_ENV, "1048576"),
            (MAINTENANCE_FULL_TEXT_MEMORY_ENV, " 256 "),
            (MAINTENANCE_FULL_TEXT_WORKERS_ENV, "1"),
        ]));
        assert_eq!(set.unit_concurrency(), 2);
        assert_eq!(set.scan_io_buffer_bytes(), 1_048_576);
        assert_eq!(set.full_text_memory_mb(), 256);
        assert_eq!(set.full_text_workers(), 1);

        // The old default of eight units is lowered to the most a run takes.
        let clamped = MaintenanceBudget::from_lookup(lookup(&[(MAINTENANCE_CONCURRENCY_ENV, "8")]));
        assert_eq!(clamped.unit_concurrency(), 2);

        for unusable in ["0", "-1", "two", ""] {
            let kept = MaintenanceBudget::from_lookup(lookup(&[
                (MAINTENANCE_CONCURRENCY_ENV, unusable),
                (MAINTENANCE_SCAN_IO_BUFFER_ENV, unusable),
                (MAINTENANCE_FULL_TEXT_MEMORY_ENV, unusable),
                (MAINTENANCE_FULL_TEXT_WORKERS_ENV, unusable),
            ]));
            assert_eq!(kept, MaintenanceBudget::DEFAULT, "{unusable:?}");
        }

        assert!(MaintenanceBudget::DEFAULT.with_unit_concurrency(0).is_err());
        assert!(MaintenanceBudget::DEFAULT.with_unit_concurrency(3).is_err());
        assert!(
            MaintenanceBudget::DEFAULT
                .with_scan_io_buffer_bytes(0)
                .is_err()
        );
        assert!(
            MaintenanceBudget::DEFAULT
                .with_full_text_build(0, 2)
                .is_err()
        );
        assert!(
            MaintenanceBudget::DEFAULT
                .with_full_text_build(512, 0)
                .is_err()
        );
    }

    #[test]
    fn compaction_options_carry_the_budget() {
        let budget = MaintenanceBudget::DEFAULT
            .with_scan_io_buffer_bytes(4096)
            .unwrap();
        let options = budget.compaction_options();
        assert_eq!(options.io_buffer_size, Some(4096));
        assert_eq!(options.batch_size, Some(COMPACTION_BATCH_ROWS));
        let defaults = CompactionOptions::default();
        assert_eq!(
            options.target_rows_per_fragment,
            defaults.target_rows_per_fragment
        );
        assert_eq!(options.max_source_fragments, None);
        assert_eq!(options.max_source_rows, None);
        assert_eq!(options.max_source_bytes, None);
    }

    fn storage_error() -> OmniError {
        OmniError::Io(std::io::Error::other("storage failed"))
    }

    #[tokio::test]
    async fn a_run_classifies_failures_by_how_far_it_went() {
        let lane = Arc::new(MaintenanceLane::default());
        let run = lane
            .enter(MaintenanceOperation::Optimize, Some("main"))
            .await
            .unwrap();
        assert_eq!(
            run.classify(storage_error()).completion_evidence(),
            Some(CompletionEvidence::BeforeEffect)
        );
        let typed = run.classify(OmniError::manifest_conflict("pin moved"));
        assert!(
            matches!(typed, OmniError::Manifest(_)),
            "a typed outcome keeps its shape"
        );

        run.staged();
        assert_eq!(
            run.classify(storage_error()).completion_evidence(),
            Some(CompletionEvidence::Unpublished)
        );

        run.publishing();
        assert_eq!(run.classify(storage_error()).completion_evidence(), None);
        assert_eq!(
            run.classify(OmniError::manifest_publish_in_doubt("lost"))
                .completion_evidence(),
            Some(CompletionEvidence::Uncertain),
            "a publication in doubt stays uncertain"
        );

        run.published();
        assert_eq!(
            run.classify(storage_error()).completion_evidence(),
            Some(CompletionEvidence::PartlyPublished {
                step_unpublished: false
            })
        );
        // The phase only moves forward.
        run.staged();
        assert!(matches!(
            run.classify(storage_error()).completion_evidence(),
            Some(CompletionEvidence::PartlyPublished { .. })
        ));
    }

    #[tokio::test]
    async fn the_lane_holds_one_run_and_a_cancel_stops_it_and_its_waiters() {
        let lane = Arc::new(MaintenanceLane::default());
        let first = lane
            .enter(MaintenanceOperation::Optimize, Some("main"))
            .await
            .unwrap();
        assert_eq!(
            lane.in_progress(),
            Some(MaintenanceInProgress {
                operation: MaintenanceOperation::Optimize,
                branch: Some("main".to_string()),
            })
        );

        let waiting_lane = Arc::clone(&lane);
        let mut waiter = tokio::spawn(async move {
            waiting_lane
                .enter(MaintenanceOperation::Cleanup, None)
                .await
                .map(|_| ())
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiter)
                .await
                .is_err(),
            "a second run waits for the lane"
        );

        first.staged();
        assert!(!first.is_cancelled());
        lane.cancel();
        let refused = first.check().unwrap_err();
        assert_eq!(
            refused.completion_evidence(),
            Some(CompletionEvidence::Unpublished)
        );
        assert!(matches!(
            refused.into_completion_evidence().0,
            OmniError::MaintenanceCancelled {
                operation: "optimize"
            }
        ));
        let waited = waiter.await.unwrap().unwrap_err();
        assert_eq!(
            waited.completion_evidence(),
            Some(CompletionEvidence::BeforeEffect),
            "a waiting run is refused before any effect"
        );
        let pending = first
            .unless_cancelled(std::future::pending::<()>())
            .await
            .unwrap_err();
        assert!(matches!(
            pending.into_completion_evidence().0,
            OmniError::MaintenanceCancelled { .. }
        ));

        drop(first);
        assert_eq!(lane.in_progress(), None);
        let later = lane
            .enter(MaintenanceOperation::IndexBuild, Some("feature"))
            .await
            .expect("a run that arrives after the cancel is not cancelled");
        assert!(later.check().is_ok());
        assert_eq!(
            later.unless_cancelled(async { 7 }).await.unwrap(),
            7,
            "work finishes when nothing cancels it"
        );
    }

    #[tokio::test]
    async fn work_off_the_serving_runtime_returns_its_output_and_aborts_on_drop() {
        let caller = std::thread::current().id();
        let (ran_on, value) =
            off_serving_runtime(async { (std::thread::current().id(), 41 + 1) }).await;
        assert_eq!(value, 42);
        assert_ne!(ran_on, caller, "the work ran on the maintenance runtime");
        let name =
            off_serving_runtime(async { std::thread::current().name().map(str::to_string) }).await;
        assert_eq!(name.as_deref(), Some("omnigraph-maintenance"));

        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        struct SignalOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let work = tokio::spawn(off_serving_runtime(async move {
            let _signal = SignalOnDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.unwrap();
        work.abort();
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped_rx)
            .await
            .expect("dropping the caller drops the work")
            .unwrap();
    }
}
