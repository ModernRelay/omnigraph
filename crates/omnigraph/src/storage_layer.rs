//! Storage trait surface — MR-793.
//!
//! `TableStorage` is the engine-internal trait that exposes the
//! staged-write primitives (`stage_append`, `stage_merge_insert`,
//! `stage_overwrite`, `stage_create_btree_index`,
//! `stage_create_inverted_index`) plus `commit_staged` as the canonical
//! way for new engine writers to advance Lance HEAD without coupling
//! "write bytes" with "advance HEAD" in one Lance API call.
//!
//! ## Transitional residuals on the trait
//!
//! Several inline-commit methods remain on the trait surface as
//! documented residuals: `delete_where` (Lance 4.0.0's `DeleteJob` is
//! `pub(crate)` — see [#6658](https://github.com/lance-format/lance/issues/6658)),
//! `create_vector_index` (segment-commit-path requires
//! `build_index_metadata_from_segments` which is `pub(crate)` — see
//! [#6666](https://github.com/lance-format/lance/issues/6666)), and the
//! legacy `append_batch` / `merge_insert_batches` / `overwrite_batch` /
//! `create_btree_index` / `create_inverted_index` paths kept while
//! engine call sites finish migrating off of them (Phase 1b / Phase 9
//! of MR-793). These are named honestly at every call site; the
//! forbidden-API guard test catches direct lance::* misuse outside the
//! storage layer.
//!
//! ## Sealed
//!
//! `TableStorage: sealed::Sealed`. Only types in this crate can implement
//! the trait, so a downstream crate cannot subvert the contract by
//! providing its own impl.
//!
//! ## Opaque handles
//!
//! `SnapshotHandle` and `StagedHandle` wrap `lance::Dataset` and
//! `StagedWrite` respectively. Their inner Lance types are
//! `pub(crate)` — engine code outside `table_store` cannot reach
//! through. This is the §III.9 alignment: `lance::Dataset` does not
//! appear in trait signatures.
//!
//! ## Migration status (MR-793 PR #70)
//!
//! Phases 1a / 2 / 4 / 5 / 6 are landed: trait scaffolding, three new
//! staged primitives (`stage_overwrite`, scalar index staging), and
//! migration of `ensure_indices`, `branch_merge`, `schema_apply` onto
//! the staged surface. Phase 1b (call-site conversion to
//! `Arc<dyn TableStorage>`), Phase 9 (demote unused inline-commit
//! methods to `pub(crate)`), Phase 7 (recovery reconciler — MR-847),
//! and Phase 8 (index reconciler — MR-848) are deferred to follow-ups.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance::dataset::{WhenMatched, WhenNotMatched};

use crate::db::{Snapshot, SubTableEntry};
use crate::error::Result;
use crate::table_store::{DeleteState, StagedWrite, TableState, TableStore};

// ─── sealed module ──────────────────────────────────────────────────────────

pub(crate) mod sealed {
    /// Sealed marker — only types defined in `omnigraph-engine` can
    /// implement `TableStorage`. Combined with the trait being the only
    /// route to write APIs from engine code, this gives type-system
    /// enforcement of the staged-write invariant.
    pub trait Sealed {}

    impl Sealed for crate::table_store::TableStore {}
}

// ─── opaque handles ────────────────────────────────────────────────────────

/// Opaque handle to a snapshot of a single sub-table dataset at a
/// specific version.
///
/// Engine code never sees `lance::Dataset` directly; it holds
/// `SnapshotHandle` and passes it back to `TableStorage` methods.
/// Inside this crate, `pub(crate)` accessors expose the inner
/// `Arc<Dataset>` to the `TableStorage` impl.
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    pub(crate) inner: Arc<Dataset>,
}

impl SnapshotHandle {
    /// Construct from a Lance dataset. `pub(crate)` — only
    /// `TableStore` should produce these.
    pub(crate) fn new(ds: Dataset) -> Self {
        Self { inner: Arc::new(ds) }
    }

    /// Borrow the underlying Lance dataset. `pub(crate)` so only the
    /// `TableStorage` impl in this crate can reach through.
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.inner
    }

    /// Take ownership of the inner `Arc<Dataset>`. Used when committing
    /// staged writes (the call needs to consume the snapshot).
    pub(crate) fn into_arc(self) -> Arc<Dataset> {
        self.inner
    }

    // ── public, lance-free accessors ──

    /// Current Lance manifest version of the snapshot.
    pub fn version(&self) -> u64 {
        self.inner.version().version
    }

    /// Whether the underlying dataset uses stable row IDs.
    pub fn uses_stable_row_ids(&self) -> bool {
        self.inner.manifest.uses_stable_row_ids()
    }
}

/// Opaque handle to a staged Lance transaction (data write or scalar
/// index build) that has not yet advanced HEAD.
///
/// Produced by `TableStorage::stage_*`, consumed by
/// `TableStorage::commit_staged`. Carries the underlying `StagedWrite`
/// (transaction + read-your-writes deltas) behind `pub(crate)`.
#[derive(Debug, Clone)]
pub struct StagedHandle {
    pub(crate) inner: StagedWrite,
}

impl StagedHandle {
    pub(crate) fn new(staged: StagedWrite) -> Self {
        Self { inner: staged }
    }

    /// Take ownership of the inner `StagedWrite`. Used by
    /// `commit_staged`.
    pub(crate) fn into_staged(self) -> StagedWrite {
        self.inner
    }
}

/// Helper: clone the inner `StagedWrite` out of each `StagedHandle` and
/// collect into a `Vec<StagedWrite>` for handing to
/// `TableStore::stage_append`'s `prior_stages` parameter. The result is
/// owned (not borrowed) — callers that already had a `&[StagedHandle]`
/// pay a clone cost per element. `StagedWrite::clone` is cheap because
/// `Transaction` and `Vec<Fragment>` are shallow-clone friendly.
pub(crate) fn staged_handles_as_writes(handles: &[StagedHandle]) -> Vec<StagedWrite> {
    handles.iter().map(|h| h.inner.clone()).collect()
}

// ─── TableStorage trait ────────────────────────────────────────────────────

/// Engine-internal trait covering every Lance dataset operation an
/// `omnigraph` engine call site might perform.
///
/// `TableStore` is the only `impl`. The trait is sealed; the inline
/// Lance APIs are not reachable through trait dispatch. New writers that
/// might advance Lance HEAD MUST add a staged-shape method here.
#[async_trait]
pub trait TableStorage: sealed::Sealed + Send + Sync + Debug {
    // ── Snapshot opens (no HEAD advance) ────────────────────────────────

    async fn open_snapshot_at_entry(&self, entry: &SubTableEntry) -> Result<SnapshotHandle>;

    async fn open_snapshot_at_table(
        &self,
        snapshot: &Snapshot,
        table_key: &str,
    ) -> Result<SnapshotHandle>;

    async fn open_dataset_head(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<SnapshotHandle>;

    async fn open_dataset_head_for_write(
        &self,
        table_key: &str,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<SnapshotHandle>;

    async fn open_dataset_at_state(
        &self,
        table_path: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<SnapshotHandle>;

    async fn fork_branch_from_state(
        &self,
        dataset_uri: &str,
        source_branch: Option<&str>,
        table_key: &str,
        source_version: u64,
        target_branch: &str,
    ) -> Result<SnapshotHandle>;

    async fn delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()>;

    async fn reopen_for_mutation(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
        table_key: &str,
        expected_version: u64,
    ) -> Result<SnapshotHandle>;

    fn ensure_expected_version(
        &self,
        snapshot: &SnapshotHandle,
        table_key: &str,
        expected_version: u64,
    ) -> Result<()>;

    // ── Reads (no HEAD advance) ────────────────────────────────────────

    async fn scan(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
    ) -> Result<Vec<RecordBatch>>;

    async fn scan_with_row_id(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
    ) -> Result<Vec<RecordBatch>>;

    async fn scan_batches(&self, snapshot: &SnapshotHandle) -> Result<Vec<RecordBatch>>;

    async fn scan_batches_for_rewrite(
        &self,
        snapshot: &SnapshotHandle,
    ) -> Result<Vec<RecordBatch>>;

    async fn count_rows(
        &self,
        snapshot: &SnapshotHandle,
        filter: Option<String>,
    ) -> Result<usize>;

    async fn count_rows_with_staged(
        &self,
        snapshot: &SnapshotHandle,
        staged: &[StagedHandle],
        filter: Option<String>,
    ) -> Result<usize>;

    async fn scan_with_staged(
        &self,
        snapshot: &SnapshotHandle,
        staged: &[StagedHandle],
        projection: Option<&[&str]>,
        filter: Option<&str>,
    ) -> Result<Vec<RecordBatch>>;

    async fn scan_with_pending(
        &self,
        snapshot: &SnapshotHandle,
        pending: &[RecordBatch],
        pending_schema: Option<SchemaRef>,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        key_column: Option<&str>,
    ) -> Result<Vec<RecordBatch>>;

    async fn first_row_id_for_filter(
        &self,
        snapshot: &SnapshotHandle,
        filter: &str,
    ) -> Result<Option<u64>>;

    async fn table_state(
        &self,
        dataset_uri: &str,
        snapshot: &SnapshotHandle,
    ) -> Result<TableState>;

    // ── Staged writes (no HEAD advance) ────────────────────────────────

    async fn stage_append(
        &self,
        snapshot: &SnapshotHandle,
        batch: RecordBatch,
        prior_stages: &[StagedHandle],
    ) -> Result<StagedHandle>;

    async fn stage_merge_insert(
        &self,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<StagedHandle>;

    async fn commit_staged(
        &self,
        snapshot: SnapshotHandle,
        staged: StagedHandle,
    ) -> Result<SnapshotHandle>;

    /// Stage an overwrite (Operation::Overwrite). MR-793 Phase 2.
    async fn stage_overwrite(
        &self,
        snapshot: &SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<StagedHandle>;

    /// Stage a BTREE scalar index build. MR-793 Phase 2.
    async fn stage_create_btree_index(
        &self,
        snapshot: &SnapshotHandle,
        columns: &[&str],
    ) -> Result<StagedHandle>;

    /// Stage an INVERTED (FTS) scalar index build. MR-793 Phase 2.
    async fn stage_create_inverted_index(
        &self,
        snapshot: &SnapshotHandle,
        column: &str,
    ) -> Result<StagedHandle>;

    // ── Inline-commit residuals (named honestly per MR-793 §3.2) ──────
    //
    // These methods advance Lance HEAD as a side effect of writing.
    // They stay on the trait until the corresponding upstream Lance API
    // ships:
    //
    // * `delete_where` — Lance #6658 (two-phase delete).
    // * `create_*_index` — `build_index_metadata_from_segments` is
    //   `pub(crate)` for vector indices in lance-4.0.0; scalar indices
    //   migrate to staged in MR-793 Phase 2.
    // * `append_batch`, `merge_insert_batches`, `overwrite_batch` —
    //   legacy paths that will be demoted to `pub(crate)` in MR-793
    //   Phase 9 once all engine sites route through the staged
    //   primitives.

    async fn append_batch(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<(SnapshotHandle, TableState)>;

    async fn merge_insert_batches(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batches: Vec<RecordBatch>,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<TableState>;

    async fn overwrite_batch(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<(SnapshotHandle, TableState)>;

    async fn delete_where(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        filter: &str,
    ) -> Result<(SnapshotHandle, DeleteState)>;

    async fn has_btree_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;
    async fn has_fts_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;
    async fn has_vector_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;

    async fn create_btree_index(
        &self,
        snapshot: SnapshotHandle,
        columns: &[&str],
    ) -> Result<SnapshotHandle>;

    async fn create_inverted_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle>;

    async fn create_vector_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle>;

    // ── URI helpers ────────────────────────────────────────────────────
    //
    // These are pure string formatting; they live on the trait so engine
    // code holding `Arc<dyn TableStorage>` can compute dataset URIs
    // without importing the concrete struct.

    fn root_uri(&self) -> &str;
    fn dataset_uri(&self, table_path: &str) -> String;

    // ── Streaming access (used by the export path) ────────────────────
    //
    // Engine code that needs a `DatasetRecordBatchStream` (rather than a
    // collected `Vec<RecordBatch>`) goes through this trait method.
    // Useful for the JSONL exporter that streams rows to a writer
    // without materializing the whole result.

    async fn scan_stream(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
    ) -> Result<DatasetRecordBatchStream>;
}

// ─── single impl: TableStore ──────────────────────────────────────────────

#[async_trait]
impl TableStorage for TableStore {
    async fn open_snapshot_at_entry(&self, entry: &SubTableEntry) -> Result<SnapshotHandle> {
        self.open_at_entry(entry).await.map(SnapshotHandle::new)
    }

    async fn open_snapshot_at_table(
        &self,
        snapshot: &Snapshot,
        table_key: &str,
    ) -> Result<SnapshotHandle> {
        self.open_snapshot_table(snapshot, table_key)
            .await
            .map(SnapshotHandle::new)
    }

    async fn open_dataset_head(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<SnapshotHandle> {
        TableStore::open_dataset_head(self, dataset_uri, branch)
            .await
            .map(SnapshotHandle::new)
    }

    async fn open_dataset_head_for_write(
        &self,
        table_key: &str,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<SnapshotHandle> {
        TableStore::open_dataset_head_for_write(self, table_key, dataset_uri, branch)
            .await
            .map(SnapshotHandle::new)
    }

    async fn open_dataset_at_state(
        &self,
        table_path: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<SnapshotHandle> {
        TableStore::open_dataset_at_state(self, table_path, branch, version)
            .await
            .map(SnapshotHandle::new)
    }

    async fn fork_branch_from_state(
        &self,
        dataset_uri: &str,
        source_branch: Option<&str>,
        table_key: &str,
        source_version: u64,
        target_branch: &str,
    ) -> Result<SnapshotHandle> {
        TableStore::fork_branch_from_state(
            self,
            dataset_uri,
            source_branch,
            table_key,
            source_version,
            target_branch,
        )
        .await
        .map(SnapshotHandle::new)
    }

    async fn delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()> {
        TableStore::delete_branch(self, dataset_uri, branch).await
    }

    async fn reopen_for_mutation(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
        table_key: &str,
        expected_version: u64,
    ) -> Result<SnapshotHandle> {
        TableStore::reopen_for_mutation(self, dataset_uri, branch, table_key, expected_version)
            .await
            .map(SnapshotHandle::new)
    }

    fn ensure_expected_version(
        &self,
        snapshot: &SnapshotHandle,
        table_key: &str,
        expected_version: u64,
    ) -> Result<()> {
        TableStore::ensure_expected_version(self, snapshot.dataset(), table_key, expected_version)
    }

    async fn scan(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
    ) -> Result<Vec<RecordBatch>> {
        TableStore::scan(self, snapshot.dataset(), projection, filter, order_by).await
    }

    async fn scan_with_row_id(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
    ) -> Result<Vec<RecordBatch>> {
        TableStore::scan_with(
            self,
            snapshot.dataset(),
            projection,
            filter,
            order_by,
            with_row_id,
            |_| Ok(()),
        )
        .await
    }

    async fn scan_batches(&self, snapshot: &SnapshotHandle) -> Result<Vec<RecordBatch>> {
        TableStore::scan_batches(self, snapshot.dataset()).await
    }

    async fn scan_batches_for_rewrite(
        &self,
        snapshot: &SnapshotHandle,
    ) -> Result<Vec<RecordBatch>> {
        TableStore::scan_batches_for_rewrite(self, snapshot.dataset()).await
    }

    async fn count_rows(
        &self,
        snapshot: &SnapshotHandle,
        filter: Option<String>,
    ) -> Result<usize> {
        TableStore::count_rows(self, snapshot.dataset(), filter).await
    }

    async fn count_rows_with_staged(
        &self,
        snapshot: &SnapshotHandle,
        staged: &[StagedHandle],
        filter: Option<String>,
    ) -> Result<usize> {
        let staged_writes = staged_handles_as_writes(staged);
        TableStore::count_rows_with_staged(self, snapshot.dataset(), &staged_writes, filter).await
    }

    async fn scan_with_staged(
        &self,
        snapshot: &SnapshotHandle,
        staged: &[StagedHandle],
        projection: Option<&[&str]>,
        filter: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        let staged_writes = staged_handles_as_writes(staged);
        TableStore::scan_with_staged(
            self,
            snapshot.dataset(),
            &staged_writes,
            projection,
            filter,
        )
        .await
    }

    async fn scan_with_pending(
        &self,
        snapshot: &SnapshotHandle,
        pending: &[RecordBatch],
        pending_schema: Option<SchemaRef>,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        key_column: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        TableStore::scan_with_pending(
            self,
            snapshot.dataset(),
            pending,
            pending_schema,
            projection,
            filter,
            key_column,
        )
        .await
    }

    async fn first_row_id_for_filter(
        &self,
        snapshot: &SnapshotHandle,
        filter: &str,
    ) -> Result<Option<u64>> {
        TableStore::first_row_id_for_filter(self, snapshot.dataset(), filter).await
    }

    async fn table_state(
        &self,
        dataset_uri: &str,
        snapshot: &SnapshotHandle,
    ) -> Result<TableState> {
        TableStore::table_state(self, dataset_uri, snapshot.dataset()).await
    }

    async fn stage_append(
        &self,
        snapshot: &SnapshotHandle,
        batch: RecordBatch,
        prior_stages: &[StagedHandle],
    ) -> Result<StagedHandle> {
        let staged_writes = staged_handles_as_writes(prior_stages);
        TableStore::stage_append(self, snapshot.dataset(), batch, &staged_writes)
            .await
            .map(StagedHandle::new)
    }

    async fn stage_merge_insert(
        &self,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<StagedHandle> {
        let ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        TableStore::stage_merge_insert(
            self,
            ds,
            batch,
            key_columns,
            when_matched,
            when_not_matched,
        )
        .await
        .map(StagedHandle::new)
    }

    async fn commit_staged(
        &self,
        snapshot: SnapshotHandle,
        staged: StagedHandle,
    ) -> Result<SnapshotHandle> {
        let ds_arc = snapshot.into_arc();
        let transaction = staged.into_staged().transaction;
        TableStore::commit_staged(self, ds_arc, transaction)
            .await
            .map(SnapshotHandle::new)
    }

    async fn stage_overwrite(
        &self,
        snapshot: &SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<StagedHandle> {
        TableStore::stage_overwrite(self, snapshot.dataset(), batch)
            .await
            .map(StagedHandle::new)
    }

    async fn stage_create_btree_index(
        &self,
        snapshot: &SnapshotHandle,
        columns: &[&str],
    ) -> Result<StagedHandle> {
        TableStore::stage_create_btree_index(self, snapshot.dataset(), columns)
            .await
            .map(StagedHandle::new)
    }

    async fn stage_create_inverted_index(
        &self,
        snapshot: &SnapshotHandle,
        column: &str,
    ) -> Result<StagedHandle> {
        TableStore::stage_create_inverted_index(self, snapshot.dataset(), column)
            .await
            .map(StagedHandle::new)
    }

    async fn append_batch(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<(SnapshotHandle, TableState)> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        let state = TableStore::append_batch(self, dataset_uri, &mut ds, batch).await?;
        Ok((SnapshotHandle::new(ds), state))
    }

    async fn merge_insert_batches(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batches: Vec<RecordBatch>,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<TableState> {
        let ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        TableStore::merge_insert_batches(
            self,
            dataset_uri,
            ds,
            batches,
            key_columns,
            when_matched,
            when_not_matched,
        )
        .await
    }

    async fn overwrite_batch(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        batch: RecordBatch,
    ) -> Result<(SnapshotHandle, TableState)> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        let state = TableStore::overwrite_batch(self, dataset_uri, &mut ds, batch).await?;
        Ok((SnapshotHandle::new(ds), state))
    }

    async fn delete_where(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        filter: &str,
    ) -> Result<(SnapshotHandle, DeleteState)> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        let state = TableStore::delete_where(self, dataset_uri, &mut ds, filter).await?;
        Ok((SnapshotHandle::new(ds), state))
    }

    async fn has_btree_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_btree_index(self, snapshot.dataset(), column).await
    }

    async fn has_fts_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_fts_index(self, snapshot.dataset(), column).await
    }

    async fn has_vector_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_vector_index(self, snapshot.dataset(), column).await
    }

    async fn create_btree_index(
        &self,
        snapshot: SnapshotHandle,
        columns: &[&str],
    ) -> Result<SnapshotHandle> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        TableStore::create_btree_index(self, &mut ds, columns).await?;
        Ok(SnapshotHandle::new(ds))
    }

    async fn create_inverted_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        TableStore::create_inverted_index(self, &mut ds, column).await?;
        Ok(SnapshotHandle::new(ds))
    }

    async fn create_vector_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc())
            .unwrap_or_else(|arc| (*arc).clone());
        TableStore::create_vector_index(self, &mut ds, column).await?;
        Ok(SnapshotHandle::new(ds))
    }

    fn root_uri(&self) -> &str {
        TableStore::root_uri(self)
    }

    fn dataset_uri(&self, table_path: &str) -> String {
        TableStore::dataset_uri(self, table_path)
    }

    async fn scan_stream(
        &self,
        snapshot: &SnapshotHandle,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
    ) -> Result<DatasetRecordBatchStream> {
        // Note: existing TableStore::scan_stream is an associated fn that
        // takes &Dataset, so we delegate via the dataset reference held by
        // the snapshot.
        TableStore::scan_stream(snapshot.dataset(), projection, filter, order_by, with_row_id).await
    }
}
