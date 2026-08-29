//! Read/write cost instrumentation (test and benchmark seam).
//!
//! Two boundary instruments let cost-budget tests assert that a warm read does
//! no redundant IO, the way LanceDB's IO-counted tests do (see
//! `docs/dev/testing.md`, "Cost-budget tests"):
//!
//! - **Lance object store** — a per-query [`WrappingObjectStore`] attached to the
//!   datasets a query opens, so a test counts real `read_iops`. Delivered through
//!   a task-local ([`QueryIoProbes`]) set by the test; production leaves it unset,
//!   so the open helpers attach nothing (one unset-`Option` check per open).
//! - **omnigraph `StorageAdapter`** — [`CountingStorageAdapter`], a decorator that
//!   counts per-method calls (the schema-contract reads on the query path).
//! - **branch merge** — [`MergeWriteProbes`] reports structural route counters
//!   and completed timing intervals without reading the clock when unset.
//!
//! The probes themselves only observe, and the decorator delegates every call.
//! The shared dataset opener also supplies the process control session when a
//! caller has no graph-scoped data session, so detached opens still reuse the
//! process object-store registry without caching mutable metadata. `IOTracker`
//! (the concrete counter) lives in tests via the `lance-io` dev-dependency; this
//! module stays generic over the `lance::io`-re-exported trait, so it adds no
//! production dependency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::io::WrappingObjectStore;

use crate::error::{OmniError, Result};
use crate::storage::{ListDirBounds, StorageAdapter};

macro_rules! declare_engine_cargo_features {
    ($($feature:literal),+ $(,)?) => {
        /// Every Cargo feature declared by `omnigraph-engine`.
        ///
        /// Benchmark admission compares this registry to the crate manifest
        /// captured by its build script. Adding a Cargo feature without adding
        /// it here therefore fails closed instead of collapsing two builds into
        /// one benchmark identity.
        #[doc(hidden)]
        pub const fn declared_engine_cargo_features() -> &'static [&'static str] {
            &[$($feature),+]
        }

        /// Cargo features compiled into this exact `omnigraph-engine` artifact.
        ///
        /// This read-only build seam lets benchmark and diagnostic binaries
        /// report dependency features from the crate that owns them. A
        /// dependent crate's `cfg(feature = ...)` namespace cannot observe
        /// features enabled directly on `omnigraph-engine` by Cargo's workspace
        /// feature graph.
        #[doc(hidden)]
        pub const fn enabled_engine_cargo_features() -> &'static [&'static str] {
            &[
                $(
                    #[cfg(feature = $feature)]
                    $feature,
                )+
            ]
        }
    };
}

// Keep this list sorted. Benchmark admission independently derives the same
// registry from Cargo.toml and refuses execution on any mismatch.
declare_engine_cargo_features!("default", "dst", "failpoints");

/// Per-query IO probes, installed for a query's task via [`with_query_io_probes`].
///
/// Each wrapper is attached (when present) to the datasets that category opens,
/// so a test reads `read_iops` off its own `IOTracker` handle. `probe_count`
/// records calls to the version probe (which runs on the coordinator's already-open
/// handle, so it is counted by invocation rather than by the per-query wrappers).
#[derive(Clone, Default)]
pub struct QueryIoProbes {
    pub manifest_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    /// Attached to the per-table data opens a query performs (the cache-miss
    /// path in `DatasetEntry::open`). Lets a cost test assert how many tables
    /// a query actually opened — N on a cold read, 0 on a warm repeat once the
    /// handle cache (Fix 3) serves them.
    pub table_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    pub probe_count: Arc<AtomicU64>,
    /// Counts DATA-table open CALLS through the one instrumented chokepoint
    /// (`open_dataset`), classified by URI so the
    /// internal/system tables (`__manifest`) are EXCLUDED — the publisher CAS
    /// opens those every write, and counting them would make the
    /// `data_open_count <= |touched_tables|` write gate
    /// (RFC-013 step 3b) unreachable by threading alone. Unlike the opener-read
    /// term (which mixes with the merge-insert/RI scan on the write path), this is
    /// an exact open-invocation count. `forbidden_apis` keeps engine code OUTSIDE the
    /// storage layer (`exec/`, `db/omnigraph/`, `loader/`, `changes/`) from opening
    /// datasets except through these chokepoints, so the count is complete for the
    /// keyed-write data path the gate measures. (Since the dataset-opener
    /// unification, `table_store.rs`'s branch-management ops also route through
    /// the one chokepoint, so the count covers them too.)
    pub data_open_count: Arc<AtomicU64>,
    /// Internal/system-table (`__manifest`) open CALLS — the complement of
    /// `data_open_count`, kept for symmetry and debugging.
    pub internal_open_count: Arc<AtomicU64>,
    /// Full `__manifest` row-scan invocations. Counted at the shared state scan
    /// and the dedicated lineage scan, so a coordinator that opens one handle
    /// but scans state and lineage separately still reports two.
    pub manifest_scan_count: Arc<AtomicU64>,
    /// Counts topology-index builds (the `RuntimeCache::graph_index` cache-miss
    /// path). A cost test asserts a fresh branch whose edge tables are unchanged
    /// from main reuses main's cached index (0 builds) rather than rebuilding it.
    pub graph_build_count: Arc<AtomicU64>,
    /// Edge tables included in topology builds this query (summed over build
    /// invocations). A cost test asserts a query referencing one edge builds only
    /// that edge, not every catalog edge (the cold-build shrink A2 ships).
    pub graph_edges_built: Arc<AtomicU64>,
    /// IR filters lowered into a scan-level DataFusion `filter_expr` (summed
    /// over `build_lance_filter_expr` calls). Lets a test assert a standalone
    /// string-match predicate was HOISTED into the NodeScan (where Lance can
    /// probe a covering index) rather than silently degrading to the
    /// in-memory arm — a result-only assertion passes either way.
    pub pushed_filter_exprs: Arc<AtomicU64>,
    /// Filters evaluated by the in-memory arm (`projection.rs::apply_filter`),
    /// the complement of `pushed_filter_exprs` for hoist assertions.
    pub in_memory_filters: Arc<AtomicU64>,
    /// Commits the change-feed poll walked into its first-parent chain (the
    /// `chain_after` head→cursor clone). This is the CPU/allocation term that
    /// grows with the *backlog* even when the page ceiling is small, and it is
    /// invisible to the manifest/data IO counters — a cost test asserts it so a
    /// future forward-child projection (the bounded-visit fix) is measurable.
    pub feed_commits_visited: Arc<AtomicU64>,
    /// Adjacent-version transaction files read while classifying CDC candidate
    /// intervals. Wider intervals must fall back before incrementing this
    /// counter, keeping stateless tiny-page resumes constant in history depth.
    pub candidate_transaction_reads: Arc<AtomicU64>,
    /// Manifest fragment entries compared or validated while deriving a CDC
    /// candidate plan. This exposes the metadata CPU term that object-store I/O
    /// counters cannot see.
    pub candidate_fragment_metadata_steps: Arc<AtomicU64>,
    /// Candidate child rows pulled by the pruned emitter. A max-changes=1 page
    /// over all-changing rows should inspect only the emitted row plus one
    /// continuation sentinel.
    pub candidate_rows_examined: Arc<AtomicU64>,
    /// Largest row/byte scanner target requested by a candidate emitter in the
    /// measured operation. Both are maxima (not sums) because parent and child
    /// streams use the same current-page target.
    pub candidate_scan_target_rows_peak: Arc<AtomicU64>,
    pub candidate_scan_target_bytes_peak: Arc<AtomicU64>,
    /// Complete logical change images materialized (and therefore eligible to
    /// read managed Blob payloads). A continuation sentinel must not increment
    /// this counter.
    pub change_images_materialized: Arc<AtomicU64>,
    /// Manifest-projection refreshes served by the incremental projection fold
    /// (only appended catalog fragments read) vs the full O(history) scan.
    /// Cost tests assert the incremental path engages so a silent
    /// always-full-scan regression is structurally visible.
    pub projection_incremental_refreshes: Arc<AtomicU64>,
    pub projection_full_refreshes: Arc<AtomicU64>,
}

tokio::task_local! {
    static QUERY_IO_PROBES: QueryIoProbes;
}

/// Run `fut` with per-query IO probes installed. Test-only entry point; nothing
/// in production sets the probes, so the accessors below return `None`/no-op.
pub async fn with_query_io_probes<F>(probes: QueryIoProbes, fut: F) -> F::Output
where
    F: std::future::Future,
{
    QUERY_IO_PROBES.scope(probes, fut).await
}

fn current<R>(f: impl FnOnce(&QueryIoProbes) -> R) -> Option<R> {
    QUERY_IO_PROBES.try_with(f).ok()
}

tokio::task_local! {
    static TRAVERSAL_MODE_OVERRIDE: Option<&'static str>;
}

/// Force the Expand execution mode (`"indexed"` | `"csr"`) for the scope of `fut`
/// WITHOUT mutating the process-global `OMNIGRAPH_TRAVERSAL_MODE` env var. This is
/// the general traversal-mode test seam: scope-bound (so it cannot leak — the
/// override is gone when `fut` resolves or unwinds) and process-safe (it never
/// touches shared state, so a forced-mode test never affects a concurrent test in
/// the same binary, removing the need for `#[serial]` + a dedicated all-serial
/// binary). Mirrors [`with_query_io_probes`]. The env var stays the production/ops
/// escape hatch; this scoped override takes precedence over it
/// (`exec::query::traversal_indexed_override`).
pub async fn with_traversal_mode<F>(mode: &'static str, fut: F) -> F::Output
where
    F: std::future::Future,
{
    TRAVERSAL_MODE_OVERRIDE.scope(Some(mode), fut).await
}

/// The scoped traversal-mode override active for this task, if any. `None` in
/// production (no scope installed), so the env var is consulted instead.
pub(crate) fn traversal_mode_override() -> Option<&'static str> {
    TRAVERSAL_MODE_OVERRIDE.try_with(|m| *m).ok().flatten()
}

tokio::task_local! {
    static STAGE_WRITE_CONCURRENCY_OVERRIDE: Option<usize>;
    static STAGE_WRITE_PROBES: StageWriteProbes;
}

/// Deterministic probe for the number of table-fragment staging futures that
/// are inside their storage call at once.
///
/// `release_after` is a test rendezvous: the first staged tables wait until
/// that many participants have entered. A concurrency regression therefore
/// times out instead of passing from result equivalence alone. Production
/// leaves this task-local unset, so staging only pays the unset lookup.
#[derive(Clone)]
pub struct StageWriteProbes {
    state: Arc<StageWriteProbeState>,
}

struct StageWriteProbeState {
    active: AtomicU64,
    entered: AtomicU64,
    peak: AtomicU64,
    rendezvous: tokio::sync::Barrier,
}

impl StageWriteProbes {
    /// Create a probe that releases each group after `release_after` staged
    /// tables have entered the storage-call boundary.
    pub fn rendezvous(release_after: usize) -> Self {
        assert!(release_after > 0, "stage-write rendezvous must be non-zero");
        Self {
            state: Arc::new(StageWriteProbeState {
                active: AtomicU64::new(0),
                entered: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                rendezvous: tokio::sync::Barrier::new(release_after),
            }),
        }
    }

    /// Number of table-storage staging calls that entered the probe.
    pub fn entered(&self) -> u64 {
        self.state.entered.load(Ordering::Relaxed)
    }

    /// Maximum table-storage staging calls simultaneously inside the probe.
    pub fn peak_in_flight(&self) -> u64 {
        self.state.peak.load(Ordering::Relaxed)
    }
}

pub(crate) struct StageWriteProbeGuard {
    state: Arc<StageWriteProbeState>,
}

impl Drop for StageWriteProbeGuard {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Run `fut` with deterministic table-staging probes installed.
pub async fn with_stage_write_probes<F>(probes: StageWriteProbes, fut: F) -> F::Output
where
    F: std::future::Future,
{
    STAGE_WRITE_PROBES.scope(probes, fut).await
}

pub(crate) async fn enter_stage_write_probe() -> Option<StageWriteProbeGuard> {
    let state = STAGE_WRITE_PROBES
        .try_with(|probes| probes.state.clone())
        .ok()?;
    state.entered.fetch_add(1, Ordering::Relaxed);
    // Count only after every participant is released. The first staging call
    // must then remain pending in its real storage future for a second call to
    // raise the peak above one; parked rendezvous waiters do not count.
    state.rendezvous.wait().await;
    let active = state.active.fetch_add(1, Ordering::Relaxed) + 1;
    state.peak.fetch_max(active, Ordering::Relaxed);
    let guard = StageWriteProbeGuard {
        state: state.clone(),
    };
    Some(guard)
}

/// Force the fragment-writing stage width for the scope of `fut` WITHOUT
/// mutating the process-global `OMNIGRAPH_LOAD_CONCURRENCY` env var. Same seam
/// as [`with_traversal_mode`], for the same reason: a width-forcing test stays
/// scope-bound and process-safe, so it never perturbs a concurrent test in the
/// same binary and needs no `#[serial]`. The env var stays the production/ops
/// escape hatch; this scoped override takes precedence over it
/// (`exec::staging::stage_write_concurrency`).
///
/// `0` is not a concurrency: it is ignored in favour of the default, matching
/// the env parse rules.
pub async fn with_stage_write_concurrency<F>(concurrency: usize, fut: F) -> F::Output
where
    F: std::future::Future,
{
    STAGE_WRITE_CONCURRENCY_OVERRIDE
        .scope(Some(concurrency), fut)
        .await
}

/// The scoped staging-width override active for this task, if any. `None` in
/// production (no scope installed), so the env var is consulted instead.
pub(crate) fn stage_write_concurrency_override() -> Option<usize> {
    STAGE_WRITE_CONCURRENCY_OVERRIDE
        .try_with(|c| *c)
        .ok()
        .flatten()
}

pub(crate) fn manifest_wrapper() -> Option<Arc<dyn WrappingObjectStore>> {
    current(|p| p.manifest_wrapper.clone()).flatten()
}

pub(crate) fn table_wrapper() -> Option<Arc<dyn WrappingObjectStore>> {
    current(|p| p.table_wrapper.clone()).flatten()
}

/// Record one version-probe invocation against the active per-query probes.
/// No-op when no probes are installed (production).
pub(crate) fn record_probe() {
    let _ = current(|p| p.probe_count.fetch_add(1, Ordering::Relaxed));
}

/// Internal/system table directory names. An open of one of these is a metadata
/// open (publisher CAS, recovery audit), NOT a data-table open. Kept in sync with
/// the dir constants in `db/manifest/layout.rs` and `db/recovery_audit.rs`.
const INTERNAL_TABLE_DIRS: [&str; 2] = ["__manifest", "_graph_commit_recoveries.lance"];

/// True when `uri`'s last path segment names an internal/system table.
fn open_is_internal(uri: &str) -> bool {
    let trimmed = uri.trim_end_matches('/');
    let last = trimmed.rsplit('/').next().unwrap_or(trimmed);
    INTERNAL_TABLE_DIRS.contains(&last)
}

/// Record one table-open call against the active per-query probes, classified by
/// table class (the URI's last segment) so the write gate counts DATA-table opens
/// only and ignores the publisher metadata opens. No-op in production
/// (the classification runs only inside the probe closure, which `current` skips
/// when no probes are installed). Called at the open chokepoint.
pub(crate) fn record_open(uri: &str) {
    let _ = current(|p| {
        if open_is_internal(uri) {
            p.internal_open_count.fetch_add(1, Ordering::Relaxed);
        } else {
            p.data_open_count.fetch_add(1, Ordering::Relaxed);
        }
    });
}

/// Record one full `__manifest` row scan. No-op unless a cost probe is active.
pub(crate) fn record_manifest_scan() {
    let _ = current(|p| {
        p.manifest_scan_count.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one manifest-projection refresh served by the incremental
/// fold. No-op unless a cost probe is active.
pub(crate) fn record_projection_incremental_refresh() {
    let _ = current(|p| {
        p.projection_incremental_refreshes
            .fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one manifest-projection refresh that fell back to (or started as)
/// the full O(history) scan. No-op unless a cost probe is active.
pub(crate) fn record_projection_full_refresh() {
    let _ = current(|p| {
        p.projection_full_refreshes.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one topology-index build over `edges` edge tables (the
/// `RuntimeCache::graph_index` cache-miss path). No-op when no probes are
/// installed (production).
pub(crate) fn record_graph_build(edges: usize) {
    let _ = current(|p| {
        p.graph_build_count.fetch_add(1, Ordering::Relaxed);
        p.graph_edges_built
            .fetch_add(edges as u64, Ordering::Relaxed);
    });
}

/// Record `n` IR filters lowered into a scan-level `filter_expr`. No-op when
/// no probes are installed (production) and when nothing was pushed.
pub(crate) fn record_pushed_filter_exprs(n: u64) {
    if n > 0 {
        let _ = current(|p| p.pushed_filter_exprs.fetch_add(n, Ordering::Relaxed));
    }
}

/// Record one in-memory filter application (`apply_filter`). No-op when no
/// probes are installed (production).
pub(crate) fn record_in_memory_filter() {
    let _ = current(|p| p.in_memory_filters.fetch_add(1, Ordering::Relaxed));
}

/// Record `commits` walked into a change-feed poll's first-parent chain. No-op
/// when no probes are installed (production).
pub(crate) fn record_feed_commits_visited(commits: usize) {
    let _ = current(|p| {
        p.feed_commits_visited
            .fetch_add(commits as u64, Ordering::Relaxed)
    });
}

pub(crate) fn record_candidate_transaction_read() {
    let _ = current(|p| {
        p.candidate_transaction_reads
            .fetch_add(1, Ordering::Relaxed)
    });
}

pub(crate) fn record_candidate_fragment_metadata_steps(steps: u64) {
    if steps > 0 {
        let _ = current(|p| {
            p.candidate_fragment_metadata_steps
                .fetch_add(steps, Ordering::Relaxed)
        });
    }
}

pub(crate) fn record_candidate_row_examined() {
    let _ = current(|p| p.candidate_rows_examined.fetch_add(1, Ordering::Relaxed));
}

pub(crate) fn record_candidate_scan_targets(rows: usize, bytes: u64) {
    let _ = current(|p| {
        p.candidate_scan_target_rows_peak
            .fetch_max(rows as u64, Ordering::Relaxed);
        p.candidate_scan_target_bytes_peak
            .fetch_max(bytes, Ordering::Relaxed);
    });
}

pub(crate) fn record_change_image_materialized() {
    let _ = current(|p| p.change_images_materialized.fetch_add(1, Ordering::Relaxed));
}

/// One internal branch-merge timing bucket.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MergeTimingPhase {
    OuterPrepare,
    ProvenInsertHistory,
    ProvenInsertPlanScan,
    /// One general three-way ordered table walk plus staging its merged rows.
    /// Scalar and Blob tables each record one interval; proven-insert routes
    /// bypass this phase entirely.
    TableWalk,
    CandidateValidation,
    FinalRevalidation,
    RecoveryArm,
    PhysicalPublish,
    KeyedStage,
    KeyedCommit,
    RecoveryConfirm,
    ManifestPublish,
    RecoveryCleanup,
    OuterRestoreRefresh,
}

impl MergeTimingPhase {
    const COUNT: usize = 14;

    const fn index(self) -> usize {
        self as usize
    }

    const ALL: [Self; Self::COUNT] = [
        Self::OuterPrepare,
        Self::ProvenInsertHistory,
        Self::ProvenInsertPlanScan,
        Self::TableWalk,
        Self::CandidateValidation,
        Self::FinalRevalidation,
        Self::RecoveryArm,
        Self::PhysicalPublish,
        Self::KeyedStage,
        Self::KeyedCommit,
        Self::RecoveryConfirm,
        Self::ManifestPublish,
        Self::RecoveryCleanup,
        Self::OuterRestoreRefresh,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::OuterPrepare => "OuterPrepare",
            Self::ProvenInsertHistory => "ProvenInsertHistory",
            Self::ProvenInsertPlanScan => "ProvenInsertPlanScan",
            Self::TableWalk => "TableWalk",
            Self::CandidateValidation => "CandidateValidation",
            Self::FinalRevalidation => "FinalRevalidation",
            Self::RecoveryArm => "RecoveryArm",
            Self::PhysicalPublish => "PhysicalPublish",
            Self::KeyedStage => "KeyedStage",
            Self::KeyedCommit => "KeyedCommit",
            Self::RecoveryConfirm => "RecoveryConfirm",
            Self::ManifestPublish => "ManifestPublish",
            Self::RecoveryCleanup => "RecoveryCleanup",
            Self::OuterRestoreRefresh => "OuterRestoreRefresh",
        }
    }
}

/// One diagnostic merge phase returned by
/// [`MergeWriteProbes::merge_timing_snapshot`]. Times are accumulated in
/// microseconds; `interval_count` remains exact even when a duration rounds
/// down to zero microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MergeTimingReading {
    /// Stable, additive diagnostic identifier. Existing identifiers are never
    /// renamed or repurposed; callers must tolerate new identifiers.
    pub phase: &'static str,
    /// Sum of every explicitly completed interval, rounded down to microseconds.
    pub total_us: u64,
    /// Largest explicitly completed interval, rounded down to microseconds.
    pub max_us: u64,
    /// Exact number of explicitly completed intervals. Failed and cancelled
    /// phases drop their unfinished span and do not increment this count.
    pub interval_count: u64,
}

#[derive(Default)]
struct MergeTimingCounters {
    total_ns: [AtomicU64; MergeTimingPhase::COUNT],
    max_ns: [AtomicU64; MergeTimingPhase::COUNT],
    interval_count: [AtomicU64; MergeTimingPhase::COUNT],
}

/// Per-operation branch-merge route and timing counters.
///
/// Install a fresh instance with [`with_merge_write_probes`] for each measured
/// repetition. Counters accumulate for the lifetime of this value; read a
/// timing snapshot after the scoped future completes.
#[derive(Clone, Default)]
pub struct MergeWriteProbes {
    pub stage_append_calls: Arc<AtomicU64>,
    pub stage_append_rows: Arc<AtomicU64>,
    pub stage_merge_insert_calls: Arc<AtomicU64>,
    pub stage_merge_insert_rows: Arc<AtomicU64>,
    /// Update-only keyed stages whose ids were proven present by merge
    /// classification. Kept separate from insertion-capable Upsert.
    pub stage_known_present_update_calls: Arc<AtomicU64>,
    pub stage_known_present_update_rows: Arc<AtomicU64>,
    /// Strict-insert transactions that write new fragments directly and carry
    /// Lance's inserted-row key filter without running a target merge join.
    pub stage_fenced_insert_calls: Arc<AtomicU64>,
    pub stage_fenced_insert_rows: Arc<AtomicU64>,
    /// Exact target-absence probes performed before staging a strict insert.
    /// Proven branch-merge inserts discharge this check from durable source
    /// provenance; general strict writes must still invoke it.
    pub strict_insert_preflight_calls: Arc<AtomicU64>,
    /// Full-table vector-index (IVF) artifact builds. These count successful
    /// staging, not HEAD publication; a stale prepared attempt may abandon the
    /// immutable artifact before commit.
    pub stage_vector_index_calls: Arc<AtomicU64>,
    /// Legacy whole-delta materializations. RFC-023's bounded keyed path must
    /// keep this at zero; retaining the probe makes regressions observable.
    pub scan_staged_combined_calls: Arc<AtomicU64>,
    /// Blob payload reads performed while rebuilding descriptor rows into a
    /// logical keyed-write source. Resource-limit tests use this to prove an
    /// oversized descriptor is rejected from `BlobFile::size()` before the
    /// payload allocation/read begins.
    pub blob_payload_read_calls: Arc<AtomicU64>,
    /// Payload reads issued against external sources specifically. Unlike the
    /// aggregate Blob counter, this excludes managed Lance `BlobFile::read`
    /// calls so normalized-alias GET deduplication is directly observable.
    pub external_blob_payload_read_calls: Arc<AtomicU64>,
    /// External Blob cells presented to one operation-wide preflight and the
    /// distinct normalized object metadata probes that preflight performed.
    /// Their difference is the observable de-duplication contract: repeated
    /// cells and equivalent URI spellings must not create one HEAD per row.
    pub external_blob_probe_inputs: Arc<AtomicU64>,
    pub external_blob_probe_calls: Arc<AtomicU64>,
    /// Ordered branch-merge cursor scans and the exact per-batch limits they
    /// requested. These make the production row/byte scanner configuration a
    /// structural test assertion instead of an inferred memory claim.
    pub ordered_cursor_scan_calls: Arc<AtomicU64>,
    pub ordered_cursor_batch_rows: Arc<AtomicU64>,
    pub ordered_cursor_batch_bytes: Arc<AtomicU64>,
    /// Projected scalar batches fetched by merge validation before the shared
    /// aggregate-retention budget decides whether each one may be kept.
    pub validation_scan_batches: Arc<AtomicU64>,
    pub validation_scan_projected_bytes: Arc<AtomicU64>,
    /// Raw batches returned by Lance before the proven-insert interval
    /// normalizer copies/splits them. The byte maximum keeps the substrate's
    /// approximate decode term visible instead of conflating it with the hard
    /// normalized writer-chunk cap.
    pub proven_insert_raw_batch_calls: Arc<AtomicU64>,
    pub proven_insert_raw_batch_max_bytes: Arc<AtomicU64>,
    /// Diagnostic-only elapsed-time buckets. They are non-overlapping at the
    /// top level; `KeyedStage` and `KeyedCommit` are intentional sub-buckets of
    /// `PhysicalPublish`. Production pays only the unset task-local probe.
    merge_timing: Arc<MergeTimingCounters>,
}

impl MergeWriteProbes {
    pub fn stage_append_calls(&self) -> u64 {
        self.stage_append_calls.load(Ordering::Relaxed)
    }
    pub fn stage_append_rows(&self) -> u64 {
        self.stage_append_rows.load(Ordering::Relaxed)
    }
    pub fn stage_merge_insert_calls(&self) -> u64 {
        self.stage_merge_insert_calls.load(Ordering::Relaxed)
    }
    pub fn stage_merge_insert_rows(&self) -> u64 {
        self.stage_merge_insert_rows.load(Ordering::Relaxed)
    }
    pub fn stage_known_present_update_calls(&self) -> u64 {
        self.stage_known_present_update_calls
            .load(Ordering::Relaxed)
    }
    pub fn stage_known_present_update_rows(&self) -> u64 {
        self.stage_known_present_update_rows.load(Ordering::Relaxed)
    }
    pub fn stage_fenced_insert_calls(&self) -> u64 {
        self.stage_fenced_insert_calls.load(Ordering::Relaxed)
    }
    pub fn stage_fenced_insert_rows(&self) -> u64 {
        self.stage_fenced_insert_rows.load(Ordering::Relaxed)
    }
    pub fn strict_insert_preflight_calls(&self) -> u64 {
        self.strict_insert_preflight_calls.load(Ordering::Relaxed)
    }
    pub fn stage_vector_index_calls(&self) -> u64 {
        self.stage_vector_index_calls.load(Ordering::Relaxed)
    }
    pub fn scan_staged_combined_calls(&self) -> u64 {
        self.scan_staged_combined_calls.load(Ordering::Relaxed)
    }
    pub fn blob_payload_read_calls(&self) -> u64 {
        self.blob_payload_read_calls.load(Ordering::Relaxed)
    }
    pub fn external_blob_payload_read_calls(&self) -> u64 {
        self.external_blob_payload_read_calls
            .load(Ordering::Relaxed)
    }
    pub fn external_blob_probe_inputs(&self) -> u64 {
        self.external_blob_probe_inputs.load(Ordering::Relaxed)
    }
    pub fn external_blob_probe_calls(&self) -> u64 {
        self.external_blob_probe_calls.load(Ordering::Relaxed)
    }
    pub fn ordered_cursor_scan_calls(&self) -> u64 {
        self.ordered_cursor_scan_calls.load(Ordering::Relaxed)
    }
    pub fn ordered_cursor_batch_rows(&self) -> u64 {
        self.ordered_cursor_batch_rows.load(Ordering::Relaxed)
    }
    pub fn ordered_cursor_batch_bytes(&self) -> u64 {
        self.ordered_cursor_batch_bytes.load(Ordering::Relaxed)
    }
    pub fn validation_scan_batches(&self) -> u64 {
        self.validation_scan_batches.load(Ordering::Relaxed)
    }
    pub fn validation_scan_projected_bytes(&self) -> u64 {
        self.validation_scan_projected_bytes.load(Ordering::Relaxed)
    }
    pub fn proven_insert_raw_batch_calls(&self) -> u64 {
        self.proven_insert_raw_batch_calls.load(Ordering::Relaxed)
    }
    pub fn proven_insert_raw_batch_max_bytes(&self) -> u64 {
        self.proven_insert_raw_batch_max_bytes
            .load(Ordering::Relaxed)
    }
    fn merge_timing_total_us(&self, phase: MergeTimingPhase) -> u64 {
        self.merge_timing.total_ns[phase.index()].load(Ordering::Relaxed) / 1_000
    }
    fn merge_timing_max_us(&self, phase: MergeTimingPhase) -> u64 {
        self.merge_timing.max_ns[phase.index()].load(Ordering::Relaxed) / 1_000
    }
    fn merge_timing_interval_count(&self, phase: MergeTimingPhase) -> u64 {
        self.merge_timing.interval_count[phase.index()].load(Ordering::Relaxed)
    }
    pub fn outer_prepare_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::OuterPrepare)
    }
    pub fn proven_insert_history_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::ProvenInsertHistory)
    }
    pub fn proven_insert_plan_scan_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::ProvenInsertPlanScan)
    }
    pub fn table_walk_total_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::TableWalk)
    }
    pub fn table_walk_max_us(&self) -> u64 {
        self.merge_timing_max_us(MergeTimingPhase::TableWalk)
    }
    pub fn table_walk_interval_count(&self) -> u64 {
        self.merge_timing_interval_count(MergeTimingPhase::TableWalk)
    }
    pub fn candidate_validation_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::CandidateValidation)
    }
    pub fn final_revalidation_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::FinalRevalidation)
    }
    pub fn recovery_arm_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::RecoveryArm)
    }
    pub fn physical_publish_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::PhysicalPublish)
    }
    pub fn keyed_stage_total_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::KeyedStage)
    }
    pub fn keyed_stage_max_us(&self) -> u64 {
        self.merge_timing_max_us(MergeTimingPhase::KeyedStage)
    }
    pub fn keyed_commit_total_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::KeyedCommit)
    }
    pub fn keyed_commit_max_us(&self) -> u64 {
        self.merge_timing_max_us(MergeTimingPhase::KeyedCommit)
    }
    pub fn recovery_confirm_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::RecoveryConfirm)
    }
    pub fn manifest_publish_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::ManifestPublish)
    }
    pub fn recovery_cleanup_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::RecoveryCleanup)
    }
    pub fn outer_restore_refresh_us(&self) -> u64 {
        self.merge_timing_total_us(MergeTimingPhase::OuterRestoreRefresh)
    }

    /// Snapshot all completed merge timing intervals in deterministic order.
    ///
    /// Take the snapshot after the [`with_merge_write_probes`] future completes.
    /// `phase` is the stable identifier: callers must match by name rather than
    /// position and tolerate additive phases. This observational read uses the
    /// same relaxed counters as the individual accessors.
    pub fn merge_timing_snapshot(&self) -> Vec<MergeTimingReading> {
        MergeTimingPhase::ALL
            .into_iter()
            .map(|phase| MergeTimingReading {
                phase: phase.name(),
                total_us: self.merge_timing_total_us(phase),
                max_us: self.merge_timing_max_us(phase),
                interval_count: self.merge_timing_interval_count(phase),
            })
            .collect()
    }
}

tokio::task_local! {
    static MERGE_WRITE_PROBES: MergeWriteProbes;
}

/// Run `fut` with branch-merge test/benchmark probes installed.
///
/// Production leaves this scope unset. Use a fresh [`MergeWriteProbes`] per
/// measured repetition and inspect it only after this future completes.
pub async fn with_merge_write_probes<F>(probes: MergeWriteProbes, fut: F) -> F::Output
where
    F: std::future::Future,
{
    MERGE_WRITE_PROBES.scope(probes, fut).await
}

/// Record one `stage_append` of `rows` rows against the active probes. No-op in
/// production (no probes installed).
#[cfg(test)]
pub(crate) fn record_stage_append(rows: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.stage_append_calls.fetch_add(1, Ordering::Relaxed);
        p.stage_append_rows.fetch_add(rows, Ordering::Relaxed);
    });
}

/// Record one `stage_merge_insert` of `rows` rows against the active probes.
/// No-op in production (no probes installed).
pub(crate) fn record_stage_merge_insert(rows: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.stage_merge_insert_calls.fetch_add(1, Ordering::Relaxed);
        p.stage_merge_insert_rows.fetch_add(rows, Ordering::Relaxed);
    });
}

/// Record one update-only keyed stage whose ids were proven present by merge
/// classification. No-op when no test or benchmark probe is installed.
pub(crate) fn record_stage_known_present_update(rows: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.stage_known_present_update_calls
            .fetch_add(1, Ordering::Relaxed);
        p.stage_known_present_update_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

/// Record one join-free, filter-bearing strict insert of `rows` rows against
/// the active probes. This is distinct from `stage_merge_insert`: both commit
/// a fenced Lance `Operation::Update`, but only the latter runs a target join.
pub(crate) fn record_stage_fenced_insert(rows: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.stage_fenced_insert_calls.fetch_add(1, Ordering::Relaxed);
        p.stage_fenced_insert_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

/// Record one exact target-absence preflight for a strict insert. No-op when
/// no test or benchmark probe is installed.
pub(crate) fn record_strict_insert_preflight() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.strict_insert_preflight_calls
            .fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one successfully staged vector-index artifact build against the
/// active probes. No-op in production (no probes installed).
pub(crate) fn record_stage_vector_index() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.stage_vector_index_calls.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one impending `BlobFile::read` while logical blob arrays are rebuilt.
/// No-op in production (no probes installed).
pub(crate) fn record_blob_payload_read() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.blob_payload_read_calls.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one external object payload read. Call this alongside the aggregate
/// Blob read probe at the exact object-store request site.
pub(crate) fn record_external_blob_payload_read() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.external_blob_payload_read_calls
            .fetch_add(1, Ordering::Relaxed);
    });
}

/// Record the URI-bearing cells accepted by one bounded external-Blob
/// admission pass. No-op unless a focused test or benchmark installed probes.
pub(crate) fn record_external_blob_preflight_inputs(inputs: usize) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.external_blob_probe_inputs
            .fetch_add(inputs as u64, Ordering::Relaxed);
    });
}

/// Record one metadata request actually issued for a normalized external Blob
/// object. Counting at the request site keeps fail-fast concurrent preflights
/// from reporting planned-but-never-polled probes.
pub(crate) fn record_external_blob_probe() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.external_blob_probe_calls.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record the explicit production bounds applied to one ordered merge cursor.
/// No-op when no test probe is installed.
pub(crate) fn record_ordered_cursor_scan(batch_rows: usize, batch_bytes: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.ordered_cursor_scan_calls.fetch_add(1, Ordering::Relaxed);
        p.ordered_cursor_batch_rows
            .store(batch_rows as u64, Ordering::Relaxed);
        p.ordered_cursor_batch_bytes
            .store(batch_bytes, Ordering::Relaxed);
    });
}

/// Record one projected scalar validation batch before it is charged to the
/// operation-wide retention budget. No-op when no test probe is installed.
pub(crate) fn record_merge_validation_batch(projected_bytes: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.validation_scan_batches.fetch_add(1, Ordering::Relaxed);
        p.validation_scan_projected_bytes
            .fetch_add(projected_bytes, Ordering::Relaxed);
    });
}

/// Record one raw Lance emission before the proven-insert interval normalizer.
/// No-op when no test or benchmark probe is installed.
pub(crate) fn record_proven_insert_raw_batch(bytes: u64) {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.proven_insert_raw_batch_calls
            .fetch_add(1, Ordering::Relaxed);
        p.proven_insert_raw_batch_max_bytes
            .fetch_max(bytes, Ordering::Relaxed);
    });
}

/// An explicitly completed diagnostic timing interval. The disabled variant
/// carries no timestamp, so production performs only the task-local probe.
#[must_use = "call finish after the timed phase succeeds"]
pub(crate) struct MergeTimingSpan {
    active: Option<ActiveMergeTimingSpan>,
}

struct ActiveMergeTimingSpan {
    phase: MergeTimingPhase,
    started: Instant,
    counters: Arc<MergeTimingCounters>,
}

impl MergeTimingSpan {
    /// Record this interval. Dropping a span without finishing preserves the
    /// existing success-only behavior for failed or cancelled phases.
    pub(crate) fn finish(self) {
        let Some(ActiveMergeTimingSpan {
            phase,
            started,
            counters,
        }) = self.active
        else {
            return;
        };
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        counters.total_ns[phase.index()].fetch_add(nanos, Ordering::Relaxed);
        counters.max_ns[phase.index()].fetch_max(nanos, Ordering::Relaxed);
        counters.interval_count[phase.index()].fetch_add(1, Ordering::Relaxed);
    }
}

/// Start one diagnostic merge phase. No clock is read unless a test or
/// benchmark installed merge probes for this task.
pub(crate) fn start_merge_timing(phase: MergeTimingPhase) -> MergeTimingSpan {
    let active = MERGE_WRITE_PROBES
        .try_with(|probes| ActiveMergeTimingSpan {
            phase,
            counters: Arc::clone(&probes.merge_timing),
            started: Instant::now(),
        })
        .ok();
    MergeTimingSpan { active }
}

/// Which version [`open_dataset`] resolves.
///
/// `Latest` re-resolves the dataset's current head (the substrate's cheap
/// latest-location probe); `At(v)` is a list-free pinned open. The choice is
/// a correctness decision — strict read-modify-write ops need `Latest`,
/// snapshot reads need `At(v)` — so it is an explicit parameter of the one
/// opener rather than a property of which helper a caller happened to reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionResolution {
    Latest,
    At(u64),
}

/// THE dataset-open chokepoint. Every engine `Dataset` open routes through
/// here so three things hold uniformly, on every path:
///
/// 1. `record_open` feeds the per-query cost probes — an open that bypasses
///    this function is invisible to the cost gates.
/// 2. The per-query IO `wrapper` (manifest- or table-class) is set via
///    `ObjectStoreParams` on the builder, so the open itself is counted
///    (`Dataset::with_object_store_wrappers` only wraps an already-open
///    store). No wrapper (production) adds nothing.
/// 3. A caller-provided graph data `Session` warms Lance's metadata/index
///    caches across data-table opens. When absent (for example a detached
///    historical snapshot or recovery helper), the process-wide zero-cache
///    control session is attached instead. Every open therefore reuses the
///    shared object-store registry/client pool without letting mutable control
///    metadata become stale in a session cache.
pub(crate) async fn open_dataset(
    uri: &str,
    version: VersionResolution,
    session: Option<&Arc<lance::session::Session>>,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Dataset> {
    record_open(uri);
    let mut builder = DatasetBuilder::from_uri(uri);
    if let VersionResolution::At(version) = version {
        builder = builder.with_version(version);
    }
    let session = session
        .cloned()
        .unwrap_or_else(crate::lance_access::control_session);
    builder = builder.with_session(session);
    if let Some(wrapper) = wrapper {
        let mut store_params = crate::storage::lance_store_params_for_uri(uri)?;
        store_params.object_store_wrapper = Some(wrapper);
        builder = builder.with_store_params(store_params);
    } else {
        builder = builder.with_store_params(crate::storage::lance_store_params_for_uri(uri)?);
    }
    builder.load().await.map_err(|error| match error {
        // Only the two shapes cleanup/drop legitimately leaves behind for a
        // pinned historical read count as reclaimed history:
        //   - VersionNotFound: the dataset exists, that version was GC'd.
        //   - DatasetNotFound: the whole dataset directory is gone (a dropped
        //     table's history fully GC'd).
        // A bare NotFound is NOT a cleanup shape: it is a live manifest
        // referencing a missing object — corruption or an object-store
        // inconsistency — so it must stay loud rather than be masked as a benign
        // retention gap (which the change feed would surface as a 410 "reset via
        // baseline"). Residual: this cannot tell a corrupt CURRENT table's
        // DatasetNotFound from a legitimately dropped historical table's; that
        // needs caller context (whether the version is the table's current one),
        // and the baseline handshake the gap points to still fails loudly on
        // genuine current-state loss.
        lance::Error::VersionNotFound { .. } | lance::Error::DatasetNotFound { .. }
            if matches!(version, VersionResolution::At(_)) =>
        {
            OmniError::HistoricalVersionReclaimed {
                published_dataset_version: match version {
                    VersionResolution::At(version) => version,
                    VersionResolution::Latest => 0,
                },
            }
        }
        error => OmniError::storage(error),
    })
}

/// Per-method call counts for [`CountingStorageAdapter`].
#[derive(Debug, Default)]
pub struct StorageReadCounts {
    pub read_text: AtomicU64,
    pub read_text_if_exists: AtomicU64,
    pub exists: AtomicU64,
    pub read_text_versioned: AtomicU64,
    pub list_dir: AtomicU64,
    pub mutation_calls: AtomicU64,
    pub write_text: AtomicU64,
    pub delete: AtomicU64,
}

impl StorageReadCounts {
    pub fn read_text(&self) -> u64 {
        self.read_text.load(Ordering::Relaxed)
    }
    pub fn read_text_if_exists(&self) -> u64 {
        self.read_text_if_exists.load(Ordering::Relaxed)
    }
    pub fn exists(&self) -> u64 {
        self.exists.load(Ordering::Relaxed)
    }
    pub fn read_text_versioned(&self) -> u64 {
        self.read_text_versioned.load(Ordering::Relaxed)
    }
    pub fn list_dir(&self) -> u64 {
        self.list_dir.load(Ordering::Relaxed)
    }
    pub fn mutation_calls(&self) -> u64 {
        self.mutation_calls.load(Ordering::Relaxed)
    }
    pub fn write_text(&self) -> u64 {
        self.write_text.load(Ordering::Relaxed)
    }
    pub fn delete(&self) -> u64 {
        self.delete.load(Ordering::Relaxed)
    }
}

/// Boundary decorator over a [`StorageAdapter`] that counts every method call.
/// Calls delegate after incrementing. Construct with
/// [`CountingStorageAdapter::new`] and open an engine via
/// `Omnigraph::open_with_storage` to count its non-Lance storage IO.
#[derive(Debug)]
pub struct CountingStorageAdapter {
    inner: Arc<dyn StorageAdapter>,
    counts: Arc<StorageReadCounts>,
}

impl CountingStorageAdapter {
    /// Wrap `inner`, returning the adapter and a shared handle to its counts.
    // Returns the erased `Arc<dyn StorageAdapter>` the engine consumes plus the
    // counts handle; a bare `Self` would leave the caller unable to read them.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        inner: Arc<dyn StorageAdapter>,
    ) -> (Arc<dyn StorageAdapter>, Arc<StorageReadCounts>) {
        let counts = Arc::new(StorageReadCounts::default());
        let adapter: Arc<dyn StorageAdapter> = Arc::new(Self {
            inner,
            counts: Arc::clone(&counts),
        });
        (adapter, counts)
    }
}

#[async_trait]
impl StorageAdapter for CountingStorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        self.counts.read_text.fetch_add(1, Ordering::Relaxed);
        self.inner.read_text(uri).await
    }

    async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>> {
        self.counts
            .read_text_if_exists
            .fetch_add(1, Ordering::Relaxed);
        self.inner.read_text_if_exists(uri).await
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<String>> {
        self.counts
            .read_text_if_exists
            .fetch_add(1, Ordering::Relaxed);
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.counts.write_text.fetch_add(1, Ordering::Relaxed);
        self.inner.write_text(uri, contents).await
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.write_text_if_absent(uri, contents).await
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        self.counts.exists.fetch_add(1, Ordering::Relaxed);
        self.inner.exists(uri).await
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.rename_text(from_uri, to_uri).await
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.counts.delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(uri).await
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        self.counts.list_dir.fetch_add(1, Ordering::Relaxed);
        self.inner.list_dir(dir_uri).await
    }

    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> Result<Vec<String>> {
        self.counts.list_dir.fetch_add(1, Ordering::Relaxed);
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }

    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        self.counts
            .read_text_versioned
            .fetch_add(1, Ordering::Relaxed);
        self.inner.read_text_versioned(uri).await
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        self.counts.mutation_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_prefix(prefix_uri).await
    }
}

#[cfg(test)]
mod merge_timing_phase_tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn benchmark_feature_attestation_covers_every_engine_feature() {
        let manifest = toml::from_str::<toml::Value>(include_str!("../Cargo.toml"))
            .expect("engine Cargo.toml parses as TOML");
        let declared = manifest["features"]
            .as_table()
            .expect("engine Cargo.toml has a features table")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let registry = declared_engine_cargo_features()
            .iter()
            .map(|feature| (*feature).to_string())
            .collect::<BTreeSet<_>>();

        assert_eq!(declared, registry, "update the benchmark feature registry");

        let suppressed_optional_dependencies = manifest["features"]
            .as_table()
            .unwrap()
            .values()
            .filter_map(toml::Value::as_array)
            .flatten()
            .filter_map(toml::Value::as_str)
            .filter_map(|feature| feature.strip_prefix("dep:"))
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        let mut optional_dependencies = BTreeSet::new();
        let mut inspect_dependencies = |value: Option<&toml::Value>| {
            let Some(table) = value.and_then(toml::Value::as_table) else {
                return;
            };
            optional_dependencies.extend(
                table
                    .iter()
                    .filter(|(_name, specification)| {
                        specification
                            .as_table()
                            .and_then(|fields| fields.get("optional"))
                            .and_then(toml::Value::as_bool)
                            .is_some_and(|optional| optional)
                    })
                    .map(|(name, _specification)| name.clone()),
            );
        };
        for table in ["dependencies", "build-dependencies"] {
            inspect_dependencies(manifest.get(table));
        }
        if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
            for target in targets.values() {
                for table in ["dependencies", "build-dependencies"] {
                    inspect_dependencies(target.get(table));
                }
            }
        }
        assert!(
            optional_dependencies
                .difference(&suppressed_optional_dependencies)
                .next()
                .is_none(),
            "optional dependencies must use dep:name so no implicit feature escapes attestation"
        );
    }

    #[test]
    fn all_lists_every_phase_in_counter_order() {
        for (index, phase) in MergeTimingPhase::ALL.into_iter().enumerate() {
            assert_eq!(phase.index(), index);
        }
    }

    #[tokio::test]
    async fn timing_spans_record_only_when_explicitly_finished() {
        assert!(
            start_merge_timing(MergeTimingPhase::TableWalk)
                .active
                .is_none()
        );

        let probes = MergeWriteProbes::default();
        with_merge_write_probes(probes.clone(), async {
            start_merge_timing(MergeTimingPhase::TableWalk).finish();
            drop(start_merge_timing(MergeTimingPhase::TableWalk));
        })
        .await;

        assert_eq!(probes.table_walk_interval_count(), 1);
        let readings = probes.merge_timing_snapshot();
        assert_eq!(readings.len(), MergeTimingPhase::COUNT);
        let table_walk = readings
            .iter()
            .find(|reading| reading.phase == "TableWalk")
            .expect("TableWalk timing reading");
        assert_eq!(table_walk.interval_count, 1);
    }
}
