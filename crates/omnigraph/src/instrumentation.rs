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
    pub commit_graph_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    /// Attached to the per-table data opens a query performs (the cache-miss
    /// path in `SubTableEntry::open`). Lets a cost test assert how many tables
    /// a query actually opened — N on a cold read, 0 on a warm repeat once the
    /// handle cache (Fix 3) serves them.
    pub table_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    pub probe_count: Arc<AtomicU64>,
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

pub(crate) fn manifest_wrapper() -> Option<Arc<dyn WrappingObjectStore>> {
    current(|p| p.manifest_wrapper.clone()).flatten()
}

pub(crate) fn commit_graph_wrapper() -> Option<Arc<dyn WrappingObjectStore>> {
    current(|p| p.commit_graph_wrapper.clone()).flatten()
}

pub(crate) fn table_wrapper() -> Option<Arc<dyn WrappingObjectStore>> {
    current(|p| p.table_wrapper.clone()).flatten()
}

/// Record one version-probe invocation against the active per-query probes.
/// No-op when no probes are installed (production).
pub(crate) fn record_probe() {
    let _ = current(|p| p.probe_count.fetch_add(1, Ordering::Relaxed));
}

/// Per-operation staged-write counts, installed for a task via
/// [`with_merge_write_probes`]. Lets a cost-budget test assert WHICH staged-write
/// primitive an operation invokes — e.g. that an append-only fast-forward merge
/// routes new rows through `stage_append` and does **zero** `stage_merge_insert`
/// (the full-outer hash join). Counts the publish-path primitives only;
/// merge-staging temp tables use `append_or_create_batch`, not these.
#[derive(Clone, Default)]
pub struct MergeWriteProbes {
    pub stage_append_calls: Arc<AtomicU64>,
    pub stage_append_rows: Arc<AtomicU64>,
    pub stage_merge_insert_calls: Arc<AtomicU64>,
    pub stage_merge_insert_rows: Arc<AtomicU64>,
    /// Inline vector-index (IVF) builds. The fast-forward adopt path defers
    /// index coverage to the reconciler, so an adopt merge must do 0 of these.
    pub create_vector_index_calls: Arc<AtomicU64>,
    /// Times the merge materialized a staged delta into one in-memory batch
    /// (`scan_staged_combined`). The append path streams instead, so an
    /// append-only fast-forward merge must do 0 of these.
    pub scan_staged_combined_calls: Arc<AtomicU64>,
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
    pub fn create_vector_index_calls(&self) -> u64 {
        self.create_vector_index_calls.load(Ordering::Relaxed)
    }
    pub fn scan_staged_combined_calls(&self) -> u64 {
        self.scan_staged_combined_calls.load(Ordering::Relaxed)
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

/// Record one inline vector-index build against the active probes. No-op in
/// production (no probes installed).
pub(crate) fn record_create_vector_index() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.create_vector_index_calls.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record one `scan_staged_combined` materialization against the active probes.
/// No-op in production (no probes installed).
pub(crate) fn record_scan_staged_combined() {
    let _ = MERGE_WRITE_PROBES.try_with(|p| {
        p.scan_staged_combined_calls.fetch_add(1, Ordering::Relaxed);
    });
}

/// Open a Lance dataset at `uri`, attaching `wrapper` (for IO counting) when
/// present. With no wrapper this is exactly `Dataset::open(uri)`. The wrapper is
/// set via `ObjectStoreParams` on the builder so the open itself is counted
/// (`Dataset::with_object_store_wrappers` only wraps an already-open store).
pub(crate) async fn open_dataset_tracked(
    uri: &str,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Dataset> {
    let result = match wrapper {
        None => Dataset::open(uri).await,
        Some(wrapper) => {
            DatasetBuilder::from_uri(uri)
                .with_store_params(ObjectStoreParams {
                    object_store_wrapper: Some(wrapper),
                    ..Default::default()
                })
                .load()
                .await
        }
    };
    result.map_err(|e| OmniError::Lance(e.to_string()))
}

/// Open a data-table dataset at `location` pinned to `version` — the cache-miss
/// path of the data-read boundary (`SubTableEntry::open`). Attaches the shared
/// per-graph `Session` (warms metadata/index caches across opens, LanceDB's
/// one-session-per-connection pattern) and the per-query `table_wrapper` (for IO
/// counting) when present. With neither, this is exactly the Fix-2
/// `from_uri(location).with_version(version)` open.
pub(crate) async fn open_table_dataset(
    location: &str,
    version: u64,
    session: Option<&Arc<lance::session::Session>>,
) -> Result<Dataset> {
    let mut builder = DatasetBuilder::from_uri(location).with_version(version);
    if let Some(session) = session {
        builder = builder.with_session(session.clone());
    }
    if let Some(wrapper) = table_wrapper() {
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
