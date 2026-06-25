//! Storage trait surface — MR-793.
//!
//! `TableStorage` is the engine-internal trait that exposes the
//! staged-write primitives (`stage_append`, `stage_merge_insert`,
//! `stage_overwrite`, `stage_create_btree_index`,
//! `stage_create_inverted_index`) plus `commit_staged` as the canonical
//! way for new engine writers to advance Lance HEAD without coupling
//! "write bytes" with "advance HEAD" in one Lance API call.
//!
//! ## Inline-commit residuals live on a separate trait
//!
//! The inline-commit writes that Lance cannot yet express as
//! stage-then-commit are NOT on `TableStorage`. They sit on
//! [`InlineCommitResidual`], reachable only via
//! `Omnigraph::storage_inline_residual()`, so the default `db.storage()`
//! surface is staged-only and cannot couple "write bytes" with "advance
//! HEAD" — MR-793 acceptance §1 closes by construction. The residuals:
//!
//! * `delete_where` — Lance #6658 (`DeleteBuilder::execute_uncommitted`)
//!   did not backport to the 6.x line; it first ships in `v7.0.0-beta.10`.
//!   Migration to staged two-phase delete is tracked as MR-A, gated on the
//!   Lance v7.x bump.
//! * `create_vector_index` — segment-commit-path needs
//!   `build_index_metadata_from_segments`, still `pub(crate)` in Lance
//!   6.0.1 ([#6666](https://github.com/lance-format/lance/issues/6666),
//!   open). Scalar indices already stage.
//!
//! Each is named honestly at its call site; the forbidden-API guard test
//! catches direct lance::* misuse outside the storage layer.
//!
//! ## Sealed
//!
//! Both `TableStorage` and `InlineCommitResidual` are `: sealed::Sealed`.
//! Only types in this crate can implement them, so a downstream crate
//! cannot subvert the contract by providing its own impl.
//!
//! ## Opaque handles
//!
//! `SnapshotHandle` and `StagedHandle` wrap `lance::Dataset` and
//! `StagedWrite` respectively. Their inner Lance types are
//! `pub(crate)` — engine code outside `table_store` cannot reach
//! through. This aligns with the storage-boundary invariant:
//! `lance::Dataset` does not appear in trait signatures.
//!
//! ## Migration status
//!
//! Phases 1a / 2 / 4 / 5 / 6 landed in MR-793 PR #70 (trait scaffolding,
//! staged primitives, migration of `ensure_indices` / `branch_merge` /
//! `schema_apply` onto the staged surface). Phase 1b (call-site
//! conversion) and Phase 9 landed in MR-854, which also split the
//! inline-commit residuals onto `InlineCommitResidual` so `db.storage()`
//! is staged-only. Phase 7 (recovery reconciler) shipped as MR-847;
//! Phase 8 (index reconciler) is tracked as MR-848.

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
        Self {
            inner: Arc::new(ds),
        }
    }

    /// Borrow the underlying Lance dataset. `pub(crate)` so only the
    /// `TableStorage` impl in this crate can reach through.
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.inner
    }

    /// Take ownership of the inner `Arc<Dataset>`. Used by the
    /// `TableStorage` impl when an op needs to mutate the dataset in
    /// place (commit a staged write, append, overwrite, …).
    ///
    /// Performance note: callers consume the returned `Arc` via
    /// `Arc::try_unwrap(...).unwrap_or_else(|arc| (*arc).clone())`. The
    /// fast path (no clone) only fires when the snapshot is single-ref
    /// — i.e. the caller dropped every other `SnapshotHandle` clone
    /// before calling. Holding parallel clones (e.g. across an `await`
    /// point or stashed in a struct) forces a deep `Dataset` clone on
    /// every mutating op. Engine callers should pass `SnapshotHandle`
    /// by value into the mutating method, not keep a side copy.
    pub(crate) fn into_arc(self) -> Arc<Dataset> {
        self.inner
    }

    /// Take ownership of the inner `Dataset` by unwrapping the `Arc`
    /// (or cloning if the snapshot is shared). `pub(crate)` — used
    /// only by the maintenance path (`optimize`, `cleanup`) which
    /// must hand `&mut Dataset` to Lance compaction / cleanup APIs
    /// that the `TableStorage` trait does not (and should not)
    /// surface. Engine code that participates in the staged-write
    /// invariant must stay on the trait methods.
    ///
    /// Single-ref invariant: same fast-path/clone behavior as
    /// `into_arc` — see that method's doc. Drop sibling
    /// `SnapshotHandle` clones before calling.
    pub(crate) fn into_dataset(self) -> Dataset {
        Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone())
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

/// Outcome of a per-table branch fork (`fork_branch_from_state`).
///
/// `RefAlreadyExists` means a Lance branch ref for the target already exists
/// on the dataset, so `create_branch` could not create it cleanly. By the
/// fork caller's contract — the caller re-checks the live manifest under the
/// held per-`(table, branch)` write queue and only forks when the manifest
/// does *not* place the table on the branch — such a ref is a
/// manifest-unreferenced fork (the residue of an interrupted prior fork, or a
/// delete+recreate), which the caller reclaims and re-forks. The fork
/// operation does not editorialize ("incomplete prior delete"); it returns
/// this typed signal and lets the db layer decide.
// `pub` (not `pub(crate)`) to match the visibility of the sealed
// `TableStorage::fork_branch_from_state` that returns it (and the already-`pub`
// `SnapshotHandle`); avoids a private-interfaces warning. The trait is sealed,
// so this widening does not let external code construct or branch on it.
pub enum ForkOutcome<D> {
    Created(D),
    RefAlreadyExists,
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
    ) -> Result<ForkOutcome<SnapshotHandle>>;

    async fn delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()>;

    /// Idempotent variant of `delete_branch` used by the best-effort fork
    /// reclaim under branch delete (`db/omnigraph.rs::cleanup_deleted_branch_tables`)
    /// and by the orphan-fork reconciler in `optimize`. Tolerates an
    /// already-absent branch (both Lance's `RefNotFound` and the local-store
    /// `NotFound` quirk on a missing `tree/{branch}/` dir). A still-referenced
    /// branch (`RefConflict`) still surfaces as `OmniError::Lance`.
    async fn force_delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()>;

    /// List the named Lance branches present on the dataset at `dataset_uri`.
    /// The `cleanup` orphan reconciler diffs this against the manifest
    /// branch set to find orphaned per-table forks. `main`/default is not a
    /// named branch and never appears here.
    async fn list_branches(&self, dataset_uri: &str) -> Result<Vec<String>>;

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

    async fn scan_batches_for_rewrite(&self, snapshot: &SnapshotHandle)
    -> Result<Vec<RecordBatch>>;

    async fn count_rows(&self, snapshot: &SnapshotHandle, filter: Option<String>) -> Result<usize>;

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

    async fn table_state(&self, dataset_uri: &str, snapshot: &SnapshotHandle)
    -> Result<TableState>;

    // ── Staged writes (no HEAD advance) ────────────────────────────────

    async fn stage_append(
        &self,
        snapshot: &SnapshotHandle,
        batch: RecordBatch,
        prior_stages: &[StagedHandle],
    ) -> Result<StagedHandle>;

    /// Append `source`'s rows into `snapshot`'s table, streaming so the whole
    /// row set is never materialized in memory (see `TableStore::stage_append_stream`).
    async fn stage_append_stream(
        &self,
        snapshot: &SnapshotHandle,
        source: &SnapshotHandle,
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

    /// Stage a delete (two-phase, no HEAD advance). `None` when 0 rows match —
    /// the table is not touched (no transaction, no version). See
    /// `TableStore::stage_delete`.
    async fn stage_delete(
        &self,
        snapshot: &SnapshotHandle,
        filter: &str,
    ) -> Result<Option<StagedHandle>>;

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

    // ── Index presence (reads, no HEAD advance) ──────────────────────
    //
    // The inline-commit writes (`delete_where`, `create_vector_index`) are
    // deliberately NOT on this trait. They live on
    // the separate `InlineCommitResidual` trait, reachable only through
    // `Omnigraph::storage_inline_residual()`. As a result the default
    // `db.storage()` surface cannot couple "write bytes" with "advance HEAD"
    // — closing MR-793 acceptance §1 by construction rather than by review.

    async fn has_btree_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;
    async fn has_fts_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;
    async fn has_vector_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool>;

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

// ─── InlineCommitResidual trait ────────────────────────────────────────────

/// Inline-commit residual surface: the writes Lance cannot yet express as a
/// stage-then-commit pair, so they advance Lance HEAD as a side effect of
/// writing. Kept OFF `TableStorage` and reachable only through
/// `Omnigraph::storage_inline_residual()`, so the default `db.storage()` path
/// is staged-only and a new writer cannot reintroduce the write+commit coupling
/// by accident (MR-793 acceptance §1, by construction).
///
/// Residual reasons (each is named honestly at its call site):
/// * `delete_where` — Lance has no public two-phase delete on the 6.x line
///   (`DeleteBuilder::execute_uncommitted` first ships in v7.x; MR-A / Lance
///   #6658). The D2 parse-time rule + recovery sidecars cover the gap meanwhile.
/// * `create_vector_index` — vector-index segment-commit needs
///   `build_index_metadata_from_segments`, still `pub(crate)` in Lance 6.0.1
///   (Lance #6666). Scalar indices already stage.
#[async_trait]
pub(crate) trait InlineCommitResidual: sealed::Sealed + Send + Sync + Debug {
    async fn delete_where(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        filter: &str,
    ) -> Result<(SnapshotHandle, DeleteState)>;

    async fn create_vector_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle>;
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
    ) -> Result<ForkOutcome<SnapshotHandle>> {
        Ok(
            match TableStore::fork_branch_from_state(
                self,
                dataset_uri,
                source_branch,
                table_key,
                source_version,
                target_branch,
            )
            .await?
            {
                ForkOutcome::Created(ds) => ForkOutcome::Created(SnapshotHandle::new(ds)),
                ForkOutcome::RefAlreadyExists => ForkOutcome::RefAlreadyExists,
            },
        )
    }

    async fn delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()> {
        TableStore::delete_branch(self, dataset_uri, branch).await
    }

    async fn force_delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()> {
        TableStore::force_delete_branch(self, dataset_uri, branch).await
    }

    async fn list_branches(&self, dataset_uri: &str) -> Result<Vec<String>> {
        TableStore::list_branches(self, dataset_uri).await
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

    async fn count_rows(&self, snapshot: &SnapshotHandle, filter: Option<String>) -> Result<usize> {
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
        TableStore::scan_with_staged(self, snapshot.dataset(), &staged_writes, projection, filter)
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

    async fn stage_append_stream(
        &self,
        snapshot: &SnapshotHandle,
        source: &SnapshotHandle,
        prior_stages: &[StagedHandle],
    ) -> Result<StagedHandle> {
        let staged_writes = staged_handles_as_writes(prior_stages);
        TableStore::stage_append_stream(self, snapshot.dataset(), source.dataset(), &staged_writes)
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
        let ds = Arc::try_unwrap(snapshot.into_arc()).unwrap_or_else(|arc| (*arc).clone());
        TableStore::stage_merge_insert(self, ds, batch, key_columns, when_matched, when_not_matched)
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

    async fn stage_delete(
        &self,
        snapshot: &SnapshotHandle,
        filter: &str,
    ) -> Result<Option<StagedHandle>> {
        Ok(TableStore::stage_delete(self, snapshot.dataset(), filter)
            .await?
            .map(StagedHandle::new))
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

    async fn has_btree_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_btree_index(self, snapshot.dataset(), column).await
    }

    async fn has_fts_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_fts_index(self, snapshot.dataset(), column).await
    }

    async fn has_vector_index(&self, snapshot: &SnapshotHandle, column: &str) -> Result<bool> {
        TableStore::has_vector_index(self, snapshot.dataset(), column).await
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
        TableStore::scan_stream(
            snapshot.dataset(),
            projection,
            filter,
            order_by,
            with_row_id,
        )
        .await
    }
}

#[async_trait]
impl InlineCommitResidual for TableStore {
    async fn delete_where(
        &self,
        dataset_uri: &str,
        snapshot: SnapshotHandle,
        filter: &str,
    ) -> Result<(SnapshotHandle, DeleteState)> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc()).unwrap_or_else(|arc| (*arc).clone());
        let state = TableStore::delete_where(self, dataset_uri, &mut ds, filter).await?;
        Ok((SnapshotHandle::new(ds), state))
    }

    async fn create_vector_index(
        &self,
        snapshot: SnapshotHandle,
        column: &str,
    ) -> Result<SnapshotHandle> {
        let mut ds = Arc::try_unwrap(snapshot.into_arc()).unwrap_or_else(|arc| (*arc).clone());
        TableStore::create_vector_index(self, &mut ds, column).await?;
        Ok(SnapshotHandle::new(ds))
    }
}
