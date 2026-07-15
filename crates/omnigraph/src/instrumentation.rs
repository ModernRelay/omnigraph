//! Read-path cost instrumentation (test seam).
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
//!
//! Nothing here changes runtime behavior: the wrappers only observe, and the
//! decorator delegates every call. `IOTracker` (the concrete counter) lives in
//! tests via the `lance-io` dev-dependency; this module stays generic over the
//! `lance::io`-re-exported trait, so it adds no production dependency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::io::{ObjectStoreParams, WrappingObjectStore};

use crate::error::{OmniError, Result};
use crate::storage::StorageAdapter;

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
    /// path in `SubTableEntry::open`). Lets a cost test assert how many tables
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
    /// Counts topology-index builds (the `RuntimeCache::graph_index` cache-miss
    /// path). A cost test asserts a fresh branch whose edge tables are unchanged
    /// from main reuses main's cached index (0 builds) rather than rebuilding it.
    pub graph_build_count: Arc<AtomicU64>,
    /// Edge tables included in topology builds this query (summed over build
    /// invocations). A cost test asserts a query referencing one edge builds only
    /// that edge, not every catalog edge (the cold-build shrink A2 ships).
    pub graph_edges_built: Arc<AtomicU64>,
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

/// Record one topology-index build over `edges` edge tables (the
/// `RuntimeCache::graph_index` cache-miss path). No-op when no probes are
/// installed (production).
pub(crate) fn record_graph_build(edges: usize) {
    let _ = current(|p| {
        p.graph_build_count.fetch_add(1, Ordering::Relaxed);
        p.graph_edges_built.fetch_add(edges as u64, Ordering::Relaxed);
    });
}

/// Per-operation staged-write counts, installed for a task via
/// [`with_merge_write_probes`]. Lets a cost-budget test assert WHICH staged-write
/// primitive an operation invokes — e.g. that an append-only fast-forward merge
/// routes new rows through the exact-id fenced merge adapter and does **zero**
/// bare appends. Counts the publish-path primitives only; merge-staging temp
/// tables use `append_or_create_batch`, not these.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MergeTimingPhase {
    OuterPrepare,
    ProvenInsertHistory,
    ProvenInsertPlanScan,
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
    const COUNT: usize = 13;

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Default)]
pub struct MergeWriteProbes {
    pub stage_append_calls: Arc<AtomicU64>,
    pub stage_append_rows: Arc<AtomicU64>,
    pub stage_merge_insert_calls: Arc<AtomicU64>,
    pub stage_merge_insert_rows: Arc<AtomicU64>,
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
    merge_timing_total_ns: Arc<[AtomicU64; MergeTimingPhase::COUNT]>,
    merge_timing_max_ns: Arc<[AtomicU64; MergeTimingPhase::COUNT]>,
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
        self.merge_timing_total_ns[phase.index()].load(Ordering::Relaxed) / 1_000
    }
    fn merge_timing_max_us(&self, phase: MergeTimingPhase) -> u64 {
        self.merge_timing_max_ns[phase.index()].load(Ordering::Relaxed) / 1_000
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
}

tokio::task_local! {
    static MERGE_WRITE_PROBES: MergeWriteProbes;
}

/// Run `fut` with staged-write probes installed. Test-only entry point; nothing
/// in production sets the probes, so `record_stage_*` below are no-ops.
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

/// Add one elapsed interval to a diagnostic merge phase. No-op when no test or
/// benchmark probe is installed.
pub(crate) fn record_merge_timing(phase: MergeTimingPhase, elapsed: Duration) {
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.merge_timing_total_ns[phase.index()].fetch_add(nanos, Ordering::Relaxed);
        p.merge_timing_max_ns[phase.index()].fetch_max(nanos, Ordering::Relaxed);
    });
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
/// 3. The shared per-graph `Session` (LanceDB's one-session-per-connection
///    pattern; warms Lance's metadata/index caches across opens) is attached
///    whenever the caller has one. `None` is for genuinely session-less
///    contexts (a `Snapshot` detached from its graph's read caches) — owners
///    that hold a session (`TableStore`, the handle cache) pass it
///    unconditionally, so it cannot be silently dropped on one path.
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
    if let Some(session) = session {
        builder = builder.with_session(session.clone());
    }
    if let Some(wrapper) = wrapper {
        builder = builder.with_store_params(ObjectStoreParams {
            object_store_wrapper: Some(wrapper),
            ..Default::default()
        });
    }
    builder
        .load()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Per-method read counts for [`CountingStorageAdapter`].
#[derive(Debug, Default)]
pub struct StorageReadCounts {
    pub read_text: AtomicU64,
    pub exists: AtomicU64,
    pub read_text_versioned: AtomicU64,
    pub list_dir: AtomicU64,
    pub write_text: AtomicU64,
    pub delete: AtomicU64,
}

impl StorageReadCounts {
    pub fn read_text(&self) -> u64 {
        self.read_text.load(Ordering::Relaxed)
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
    pub fn write_text(&self) -> u64 {
        self.write_text.load(Ordering::Relaxed)
    }
    pub fn delete(&self) -> u64 {
        self.delete.load(Ordering::Relaxed)
    }
}

/// Boundary decorator over a [`StorageAdapter`] that counts read-facing calls.
/// Reads delegate after incrementing; writes delegate unchanged. Construct with
/// [`CountingStorageAdapter::new`] and open an engine via
/// `Omnigraph::open_with_storage` to count its non-Lance storage IO.
#[derive(Debug)]
pub struct CountingStorageAdapter {
    inner: Arc<dyn StorageAdapter>,
    counts: Arc<StorageReadCounts>,
}

impl CountingStorageAdapter {
    /// Wrap `inner`, returning the adapter and a shared handle to its counts.
    pub fn new(inner: Arc<dyn StorageAdapter>) -> (Arc<dyn StorageAdapter>, Arc<StorageReadCounts>) {
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

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        self.counts.write_text.fetch_add(1, Ordering::Relaxed);
        self.inner.write_text(uri, contents).await
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        self.inner.write_text_if_absent(uri, contents).await
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        self.counts.exists.fetch_add(1, Ordering::Relaxed);
        self.inner.exists(uri).await
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        self.inner.rename_text(from_uri, to_uri).await
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        self.counts.delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(uri).await
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        self.counts.list_dir.fetch_add(1, Ordering::Relaxed);
        self.inner.list_dir(dir_uri).await
    }

    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        self.counts.read_text_versioned.fetch_add(1, Ordering::Relaxed);
        self.inner.read_text_versioned(uri).await
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        self.inner.delete_prefix(prefix_uri).await
    }
}
