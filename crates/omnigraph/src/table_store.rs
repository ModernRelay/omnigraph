use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_schema::SchemaRef;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream, Scanner};
use lance::dataset::transaction::{Operation, Transaction, TransactionBuilder};
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{
    CommitBuilder, DeleteBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched,
    WriteMode, WriteParams,
};
use lance::datatypes::{BlobKind, Schema as LanceSchema};
use lance::index::DatasetIndexExt;
use lance::index::scalar::IndexDetails;
use lance_file::version::LanceFileVersion;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::{IndexType, is_system_index};
use lance_linalg::distance::MetricType;
use lance_select::mask::RowAddrTreeMap;
use lance_table::format::{Fragment, IndexMetadata, RowIdMeta};
use lance_table::rowids::{RowIdSequence, write_row_ids};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::db::manifest::TableVersionMetadata;
use crate::db::{Snapshot, SubTableEntry};
use crate::error::{OmniError, Result};
use crate::storage_layer::ForkOutcome;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    pub version: u64,
    pub row_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
}

/// Whether a `key_col IN (...)` scan on a dataset will be served by the
/// persisted scalar (BTREE) index, or silently fall back to a full filtered
/// scan. Detection-only (metadata, no IO); the scan returns the correct rows
/// either way. Surfaced by the indexed traversal path so the silent perf
/// fallback is observable, and available to a future cost-based planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexCoverage {
    /// The column has a usable BTREE and every fragment records `physical_rows`.
    Indexed,
    /// Lance will not use the scalar index for this scan (correct, full scan).
    Degraded { reason: String },
}

/// Stable identity of one Lance transaction.
///
/// Lance persists both fields in the transaction file referenced by the
/// committed manifest. Recovery uses the pair, rather than a numeric table
/// version alone, to prove that an observed HEAD was produced by the staged
/// effect named in a recovery sidecar. The UUID distinguishes two writers that
/// started from the same version. Lance may preserve both fields while
/// rebasing, so enrolled callers must also require the achieved table version
/// to be exactly `read_version + 1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedTransactionIdentity {
    pub read_version: u64,
    pub uuid: String,
}

impl From<&Transaction> for StagedTransactionIdentity {
    fn from(transaction: &Transaction) -> Self {
        Self {
            read_version: transaction.read_version,
            uuid: transaction.uuid.clone(),
        }
    }
}

/// A Lance write that has produced fragment files on object storage but is
/// not yet committed to the dataset's manifest. The staged-write primitives
/// are consumed by `MutationStaging` (`exec/staging.rs`,
/// `exec/mutation.rs`) and the bulk loader (`loader/mod.rs`). The
/// intent: defer Lance commits to end-of-query so a mid-query failure
/// leaves the touched table at the pre-mutation HEAD instead of
/// drifting ahead. See `docs/dev/writes.md` for the publisher-CAS contract
/// this builds on.
///
/// `transaction` and `commit_metadata` are opaque from our side — Lance owns
/// their semantics. They must travel together so `commit_staged` can preserve
/// Lance's row-level conflict resolution metadata for staged deletes/updates.
///
/// For read-your-writes within the same query, `new_fragments` and
/// `removed_fragment_ids` together describe the post-stage view delta:
/// `scan_with_staged` (and `count_rows_with_staged`) compose
/// `committed - removed + new` so subsequent reads see the staged result
/// without double-counting fragments that `Operation::Update` rewrote.
/// Without `removed_fragment_ids`, a `stage_merge_insert` that rewrites
/// existing fragments would yield duplicate rows (the original fragment
/// stays in the committed manifest while its rewrite shows up in `new_fragments`).
#[derive(Debug, Clone)]
pub struct StagedWrite {
    transaction: Transaction,
    commit_metadata: StagedCommitMetadata,
    /// Fragments to surface alongside the committed manifest in
    /// `Scanner::with_fragments(committed - removed + new)`. For
    /// `Operation::Append` these are the freshly-appended fragments. For
    /// `Operation::Update` (merge_insert) these are
    /// `updated_fragments + new_fragments` (rewrites + freshly-inserted
    /// rows).
    new_fragments: Vec<Fragment>,
    /// Fragment IDs that this staged write supersedes. The committed
    /// manifest must filter these out before being combined with
    /// `new_fragments` for read-your-writes scans, otherwise rewrites
    /// yield duplicate rows. Empty for `stage_append` (`Operation::Append`
    /// adds without removing anything); populated from
    /// `Operation::Update.removed_fragment_ids` for `stage_merge_insert`.
    removed_fragment_ids: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
struct StagedCommitMetadata {
    affected_rows: Option<RowAddrTreeMap>,
}

impl StagedCommitMetadata {
    fn affected_rows(affected_rows: Option<RowAddrTreeMap>) -> Self {
        Self { affected_rows }
    }
}

impl StagedWrite {
    fn new(
        transaction: Transaction,
        new_fragments: Vec<Fragment>,
        removed_fragment_ids: Vec<u64>,
    ) -> Self {
        Self {
            transaction,
            commit_metadata: StagedCommitMetadata::default(),
            new_fragments,
            removed_fragment_ids,
        }
    }

    fn with_commit_metadata(
        transaction: Transaction,
        commit_metadata: StagedCommitMetadata,
        new_fragments: Vec<Fragment>,
        removed_fragment_ids: Vec<u64>,
    ) -> Self {
        Self {
            transaction,
            commit_metadata,
            new_fragments,
            removed_fragment_ids,
        }
    }

    pub fn new_fragments(&self) -> &[Fragment] {
        &self.new_fragments
    }

    pub fn removed_fragment_ids(&self) -> &[u64] {
        &self.removed_fragment_ids
    }

    /// Identity Lance assigned when this effect was staged.
    pub fn transaction_identity(&self) -> StagedTransactionIdentity {
        StagedTransactionIdentity::from(&self.transaction)
    }

    /// Bind a pre-minted recovery identity to a transaction staged after a
    /// deferred branch fork. The operation and read version still come from
    /// Lance; only its otherwise-random UUID is replaced so the sidecar can be
    /// durable before the target ref (and its branch-local fragment paths)
    /// exist.
    pub(crate) fn bind_transaction_identity(
        &mut self,
        planned: &StagedTransactionIdentity,
    ) -> Result<()> {
        if self.transaction.read_version != planned.read_version {
            return Err(OmniError::manifest_internal(format!(
                "staged transaction read version {} does not match pre-minted recovery pin {}",
                self.transaction.read_version, planned.read_version
            )));
        }
        self.transaction.uuid.clone_from(&planned.uuid);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TableStore {
    root_uri: String,
    /// The graph's shared Lance `Session` (one per graph — LanceDB's
    /// one-session-per-connection pattern). Held non-optionally so every open
    /// this store performs attaches it; the read path's handle cache shares
    /// the same instance via `ReadCaches`.
    session: Arc<lance::session::Session>,
}

impl TableStore {
    pub fn new(root_uri: &str, session: Arc<lance::session::Session>) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            session,
        }
    }

    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }

    pub fn dataset_uri(&self, table_path: &str) -> String {
        format!("{}/{}", self.root_uri, table_path)
    }

    fn table_path_from_dataset_uri(&self, dataset_uri: &str) -> Result<String> {
        let prefix = format!("{}/", self.root_uri.trim_end_matches('/'));
        let table_path = dataset_uri
            .strip_prefix(&prefix)
            .map(|path| path.to_string())
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "dataset uri '{}' is not under root '{}'",
                    dataset_uri, self.root_uri
                ))
            })?;
        Ok(table_path
            .split_once("/tree/")
            .map(|(path, _)| path.to_string())
            .unwrap_or(table_path))
    }

    fn dataset_version_metadata(
        &self,
        dataset_uri: &str,
        ds: &Dataset,
    ) -> Result<TableVersionMetadata> {
        let table_path = self.table_path_from_dataset_uri(dataset_uri)?;
        TableVersionMetadata::from_dataset(&self.root_uri, &table_path, ds)
    }

    pub async fn open_snapshot_table(
        &self,
        snapshot: &Snapshot,
        table_key: &str,
    ) -> Result<Dataset> {
        snapshot.open(table_key).await
    }

    pub async fn open_at_entry(&self, entry: &SubTableEntry) -> Result<Dataset> {
        entry.open(&self.root_uri, Some(&self.session)).await
    }

    pub async fn open_dataset_head(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<Dataset> {
        // Direct open by URI (O(1) latest-resolution). Routed through the one
        // opener so a cost test counts it via the per-query `table_wrapper`
        // (no-op in production — the task-local is unset, so this is exactly
        // `Dataset::open(uri)`).
        let ds = crate::instrumentation::open_dataset(
            dataset_uri,
            crate::instrumentation::VersionResolution::Latest,
            Some(&self.session),
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        match branch {
            Some(branch) if branch != "main" => ds
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string())),
            _ => Ok(ds),
        }
    }

    /// List the named Lance branches present on the dataset at `dataset_uri`.
    /// The `cleanup` orphan reconciler diffs this against the manifest branch
    /// set to find orphaned per-table forks. `main`/default is not a named
    /// branch and never appears here.
    pub async fn list_branches(&self, dataset_uri: &str) -> Result<Vec<String>> {
        let ds = crate::instrumentation::open_dataset(
            dataset_uri,
            crate::instrumentation::VersionResolution::Latest,
            Some(&self.session),
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        let branches = ds
            .list_branches()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(branches.into_keys().collect())
    }

    /// Idempotently drop `branch` from the dataset at `dataset_uri`.
    ///
    /// This tolerates an already-absent branch — both a missing contents ref (Lance's
    /// `force_delete_branch` handles that) and a missing `tree/{branch}/`
    /// directory (the local-store `NotFound` quirk pinned by
    /// `lance_surface_guards::force_delete_branch_semantics`). Safe to call on a
    /// possibly-orphaned or already-reclaimed fork.
    ///
    /// A branch that still has referencing descendants (`RefConflict`) or a
    /// live physical path-child is NOT tolerated: those are real ordering
    /// errors. The graph namespace prevents new path-prefix overlaps; surfacing
    /// legacy ones keeps cleanup from falsely reporting a reclaim Lance skipped.
    /// Used by the eager best-effort reclaim in `cleanup_deleted_branch_tables`
    /// and the `cleanup` orphan reconciler.
    pub async fn force_delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()> {
        let mut ds = crate::instrumentation::open_dataset(
            dataset_uri,
            crate::instrumentation::VersionResolution::Latest,
            Some(&self.session),
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        crate::branch_control::force_delete_branch_idempotent(&mut ds, branch).await
    }

    pub fn ensure_expected_version(
        &self,
        ds: &Dataset,
        table_key: &str,
        expected_version: u64,
    ) -> Result<()> {
        let actual = ds.version().version;
        if actual != expected_version {
            // Use the structured ExpectedVersionMismatch variant so callers
            // (and the HTTP server) can match on details rather than parsing
            // the message. This drift is a publisher-style OCC failure: the
            // caller's pre-write view of the table version is stale relative
            // to the on-disk Lance head.
            return Err(OmniError::manifest_expected_version_mismatch(
                table_key,
                expected_version,
                actual,
            ));
        }
        Ok(())
    }

    pub async fn reopen_for_mutation(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
        table_key: &str,
        expected_version: u64,
    ) -> Result<Dataset> {
        let ds = self.open_dataset_head(dataset_uri, branch).await?;
        self.ensure_expected_version(&ds, table_key, expected_version)?;
        Ok(ds)
    }

    pub async fn fork_branch_from_state(
        &self,
        dataset_uri: &str,
        source_branch: Option<&str>,
        table_key: &str,
        source_version: u64,
        target_branch: &str,
    ) -> Result<ForkOutcome<Dataset>> {
        let mut source_ds = self
            .open_dataset_head(dataset_uri, source_branch)
            .await?
            .checkout_version(source_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.ensure_expected_version(&source_ds, table_key, source_version)?;

        let created = match crate::branch_control::create_branch_recoverably(
            &mut source_ds,
            target_branch,
            source_version,
        )
        .await?
        {
            crate::branch_control::BranchCreateOutcome::Created(dataset) => dataset,
            crate::branch_control::BranchCreateOutcome::RefAlreadyExists => {
                return Ok(ForkOutcome::RefAlreadyExists);
            }
        };

        // The ref is now independently durable. Any error from this point is an
        // ambiguous/post-effect outcome to the caller and must retain an armed
        // recovery intent rather than being treated as a safe pre-effect retry.
        crate::failpoints::maybe_fail(crate::failpoints::names::FORK_POST_CREATE_PRE_OPEN)?;

        // Re-open through the shared session for normal cache behavior. The
        // returned handle above is used only as proof that the matching branch
        // dataset was openable during classification.
        drop(created);
        let ds = self
            .open_dataset_head(dataset_uri, Some(target_branch))
            .await?;
        self.ensure_expected_version(&ds, table_key, source_version)?;
        Ok(ForkOutcome::Created(ds))
    }

    pub async fn scan_batches(&self, ds: &Dataset) -> Result<Vec<RecordBatch>> {
        self.scan(ds, None, None, None).await
    }

    pub async fn scan_batches_for_rewrite(&self, ds: &Dataset) -> Result<Vec<RecordBatch>> {
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if !has_blob_columns {
            return self.scan_batches(ds).await;
        }

        let batches = Self::scan_stream(ds, None, None, None, true)
            .await?
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let mut materialized = Vec::with_capacity(batches.len());
        for batch in batches {
            materialized.push(Self::materialize_blob_batch(ds, batch).await?);
        }
        Ok(materialized)
    }

    /// Streaming, blob-aware sibling of [`Self::scan_batches_for_rewrite`].
    /// Yields the dataset's rows lazily as a `SendableRecordBatchStream` so a
    /// downstream writer (`stage_append_stream`) never materializes the whole
    /// table in memory. Blob columns still need per-row rebuild, so those tables
    /// fall back to the materialized path and are re-streamed from the `Vec`
    /// (rare — only tables with a `Blob` property; bounded-memory blob streaming
    /// is a follow-up). The non-blob path is a true lazy scan.
    pub async fn scan_stream_for_rewrite(&self, ds: &Dataset) -> Result<SendableRecordBatchStream> {
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if has_blob_columns {
            let arrow_schema: SchemaRef = Arc::new(ds.schema().into());
            let batches = self.scan_batches_for_rewrite(ds).await?;
            let reader =
                arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), arrow_schema);
            return Ok(lance_datafusion::utils::reader_to_stream(Box::new(reader)));
        }
        // Non-blob: a true lazy scan. `DatasetRecordBatchStream` converts to the
        // `SendableRecordBatchStream` that `execute_uncommitted_stream` consumes.
        Ok(Self::scan_stream(ds, None, None, None, false).await?.into())
    }

    pub(crate) async fn materialize_blob_batch(
        ds: &Dataset,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if !has_blob_columns {
            return Ok(batch);
        }

        let row_ids = batch
            .column_by_name("_rowid")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::Lance("expected _rowid column when materializing blobs".to_string())
            })?
            .values()
            .iter()
            .copied()
            .collect::<Vec<_>>();

        Self::materialize_blob_batch_with_row_ids(ds, batch, &row_ids).await
    }

    /// Rebuild the blob columns in `batch` using explicit stable row ids.
    ///
    /// Most rewrite callers scan with `_rowid` and use
    /// [`Self::materialize_blob_batch`]. A predicate-filtered blob mutation
    /// cannot include blob descriptors in that scan on the pinned Lance
    /// revision (the filter projection panics), so it first scans only
    /// non-blob columns + `_rowid`, takes the full descriptor rows by id, and
    /// calls this sibling with the ids captured by the safe scan.
    async fn materialize_blob_batch_with_row_ids(
        ds: &Dataset,
        batch: RecordBatch,
        row_ids: &[u64],
    ) -> Result<RecordBatch> {
        if batch.num_rows() != row_ids.len() {
            return Err(OmniError::Lance(format!(
                "blob materialization row count {} does not match {} row ids",
                batch.num_rows(),
                row_ids.len()
            )));
        }

        let schema: SchemaRef = Arc::new(ds.schema().into());
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let lance_field = lance::datatypes::Field::try_from(field.as_ref())
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let column = batch.column_by_name(field.name()).ok_or_else(|| {
                OmniError::Lance(format!("batch missing column '{}'", field.name()))
            })?;
            if lance_field.is_blob() {
                let descriptions =
                    column
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            OmniError::Lance(format!(
                                "expected blob descriptions for '{}'",
                                field.name()
                            ))
                        })?;
                columns.push(
                    Self::rebuild_blob_column(ds, field.name(), descriptions, &row_ids).await?,
                );
            } else {
                columns.push(column.clone());
            }
        }

        RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
    }

    async fn rebuild_blob_column(
        ds: &Dataset,
        column_name: &str,
        descriptions: &StructArray,
        row_ids: &[u64],
    ) -> Result<ArrayRef> {
        let mut builder = BlobArrayBuilder::new(row_ids.len());
        let mut non_null_row_ids = Vec::new();
        let mut row_has_blob = Vec::with_capacity(row_ids.len());

        for row in 0..row_ids.len() {
            let is_null = Self::blob_description_is_null(descriptions, row)?;
            row_has_blob.push(!is_null);
            if !is_null {
                non_null_row_ids.push(row_ids[row]);
            }
        }

        let blob_files = if non_null_row_ids.is_empty() {
            Vec::new()
        } else {
            Arc::new(ds.clone())
                .take_blobs(&non_null_row_ids, column_name)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
        };

        let mut files = blob_files.into_iter();
        for has_blob in row_has_blob {
            if !has_blob {
                builder
                    .push_null()
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
                continue;
            }

            let blob = files.next().ok_or_else(|| {
                OmniError::Lance(format!(
                    "blob rewrite for '{}' lost alignment with source rows",
                    column_name
                ))
            })?;
            builder
                .push_bytes(
                    blob.read()
                        .await
                        .map_err(|e| OmniError::Lance(e.to_string()))?,
                )
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }

        if files.next().is_some() {
            return Err(OmniError::Lance(format!(
                "blob rewrite for '{}' produced extra source blobs",
                column_name
            )));
        }

        builder
            .finish()
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    fn blob_description_is_null(descriptions: &StructArray, row: usize) -> Result<bool> {
        if descriptions.is_null(row) {
            return Ok(true);
        }

        let position = descriptions
            .column_by_name("position")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "unrecognized blob description schema {:?}: missing UInt64 position field",
                    descriptions.fields()
                ))
            })?;
        let size = descriptions
            .column_by_name("size")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "unrecognized blob description schema {:?}: missing UInt64 size field",
                    descriptions.fields()
                ))
            })?;

        let Some(kind_column) = descriptions.column_by_name("kind") else {
            return Ok(position.is_null(row) || size.is_null(row));
        };
        let kind = if let Some(kind) = kind_column.as_any().downcast_ref::<UInt8Array>() {
            if kind.is_null(row) {
                return Ok(true);
            }
            kind.value(row)
        } else if let Some(kind) = kind_column.as_any().downcast_ref::<UInt32Array>() {
            if kind.is_null(row) {
                return Ok(true);
            }
            kind.value(row) as u8
        } else {
            return Err(OmniError::Lance(format!(
                "unrecognized blob description schema {:?}: kind field must be UInt8 or UInt32",
                descriptions.fields()
            )));
        };

        let kind = BlobKind::try_from(kind).map_err(|e| OmniError::Lance(e.to_string()))?;
        if kind != BlobKind::Inline {
            return Ok(false);
        }
        let blob_uri = descriptions
            .column_by_name("blob_uri")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)));

        Ok((position.is_null(row) || position.value(row) == 0)
            && (size.is_null(row) || size.value(row) == 0)
            && blob_uri.unwrap_or("").is_empty())
    }

    pub async fn scan_stream(
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
    ) -> Result<DatasetRecordBatchStream> {
        Self::scan_stream_with(ds, projection, filter, order_by, with_row_id, |_| Ok(())).await
    }

    pub async fn scan_stream_with<F>(
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
        configure: F,
    ) -> Result<DatasetRecordBatchStream>
    where
        F: FnOnce(&mut Scanner) -> Result<()>,
    {
        let mut scanner = ds.scan();
        if with_row_id {
            scanner.with_row_id();
        }
        if let Some(columns) = projection {
            scanner
                .project(columns)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        if let Some(filter_sql) = filter {
            scanner
                .filter(filter_sql)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        if let Some(ordering) = order_by {
            scanner
                .order_by(Some(ordering))
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        configure(&mut scanner)?;
        scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn scan(
        &self,
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
    ) -> Result<Vec<RecordBatch>> {
        Self::scan_stream(ds, projection, filter, order_by, false)
            .await?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn scan_with<F>(
        &self,
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
        configure: F,
    ) -> Result<Vec<RecordBatch>>
    where
        F: FnOnce(&mut Scanner) -> Result<()>,
    {
        Self::scan_stream_with(ds, projection, filter, order_by, with_row_id, configure)
            .await?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    /// Indexed neighbor lookup for graph traversal. Given an edge dataset and a
    /// set of endpoint keys on `key_col` (`"src"` for out-traversal, `"dst"` for
    /// in-traversal), return the matching edge rows projected to
    /// `[key_col, opposite_col]`.
    ///
    /// The `key_col IN (keys)` predicate is built as a structured DataFusion
    /// `Expr` and applied via `Scanner::filter_expr`, so Lance routes it through
    /// the persisted BTREE on `key_col` (index-search → take). Cost scales with
    /// the frontier size, not |E| — the basis for serving selective traversals
    /// without building the whole in-memory CSR. Empty `keys` returns empty
    /// without scanning.
    ///
    /// Note: like any indexed scan, this observes only fragments the BTREE
    /// covers plus an unindexed-fragment scan fallback; it reads the committed
    /// snapshot `ds` was opened at.
    pub async fn scan_edges_by_endpoint(
        ds: &Dataset,
        key_col: &str,
        opposite_col: &str,
        keys: &[String],
    ) -> Result<Vec<RecordBatch>> {
        use datafusion::prelude::{col, lit};

        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let key_list: Vec<datafusion::prelude::Expr> =
            keys.iter().map(|k| lit(k.clone())).collect();
        let filter_expr = col(key_col).in_list(key_list, false);
        Self::scan_stream_with(
            ds,
            Some(&[key_col, opposite_col]),
            None,
            None,
            false,
            |scanner| {
                scanner.filter_expr(filter_expr);
                Ok(())
            },
        )
        .await?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
    }

    /// Metadata-only check (no IO) of whether `scan_edges_by_endpoint` — a
    /// `key_col IN (...)` filter — on `ds` will be served by the persisted BTREE
    /// on `column`, or silently fall back to a full filtered scan. Mirrors
    /// Lance's own decision: scalar indices are disabled for the whole scan if
    /// ANY fragment lacks `physical_rows` (lance `dataset/scanner.rs`
    /// `create_filter_plan`), and are obviously unused if no BTREE on the
    /// column exists. The scan is correct (returns all rows) either way — this
    /// only surfaces the perf cliff so the indexed traversal can warn on it.
    pub async fn key_column_index_coverage(ds: &Dataset, column: &str) -> Result<IndexCoverage> {
        let Some(field_id) = ds.schema().field(column).map(|field| field.id) else {
            return Ok(IndexCoverage::Degraded {
                reason: format!("column '{}' not in schema", column),
            });
        };
        let indices = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let btree = indices
            .iter()
            .filter(|index| !is_system_index(index))
            .filter(|index| index.fields.len() == 1 && index.fields[0] == field_id)
            .find(|index| {
                index
                    .index_details
                    .as_ref()
                    .map(|details| details.type_url.ends_with("BTreeIndexDetails"))
                    .unwrap_or(false)
            });
        let Some(btree) = btree else {
            return Ok(IndexCoverage::Degraded {
                reason: format!("no BTREE index on '{}'", column),
            });
        };
        // Same check Lance runs: a fragment missing physical_rows disables
        // scalar indices for the entire scan (all-or-nothing).
        if ds.fragments().iter().any(|f| f.physical_rows.is_none()) {
            return Ok(IndexCoverage::Degraded {
                reason: "a fragment is missing physical_rows".to_string(),
            });
        }
        // An index only covers the fragments it was built over; fragments
        // appended afterward (edge-index creation is skipped once a BTREE exists)
        // are scanned unindexed. If any CURRENT fragment is absent from the
        // index's `fragment_bitmap`, the scan is partly a full scan — so the
        // chooser must not price it as fully indexed. A `None` bitmap means Lance
        // can't report coverage; don't over-degrade in that case.
        if let Some(bitmap) = btree.fragment_bitmap.as_ref() {
            let uncovered = ds
                .fragments()
                .iter()
                .filter(|f| !bitmap.contains(f.id as u32))
                .count();
            if uncovered > 0 {
                return Ok(IndexCoverage::Degraded {
                    reason: format!(
                        "{} fragment(s) not covered by the index on '{}'",
                        uncovered, column
                    ),
                });
            }
        }
        Ok(IndexCoverage::Indexed)
    }

    /// True if any non-system index on `ds` leaves at least one current
    /// fragment uncovered, i.e. rows that the index does not yet account for
    /// (appended after the index was built, or rewritten by compaction). Such
    /// fragments are scanned unindexed until a reindex (`optimize_indices`)
    /// folds them in. Returns false when every index covers every fragment, or
    /// when the table has no (non-system) indices to optimize. A `None`
    /// `fragment_bitmap` means Lance cannot report coverage for that index, so
    /// we do not treat it as uncovered (mirrors `key_column_index_coverage`).
    ///
    /// Used by `optimize` to decide whether an otherwise-already-compacted
    /// table still has index work to do.
    pub async fn has_unindexed_fragments(ds: &Dataset) -> Result<bool> {
        let indices = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let frag_ids: Vec<u32> = ds.fragments().iter().map(|f| f.id as u32).collect();
        for index in indices.iter() {
            if is_system_index(index) {
                continue;
            }
            if let Some(bitmap) = index.fragment_bitmap.as_ref() {
                if frag_ids.iter().any(|id| !bitmap.contains(*id)) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub async fn count_rows(&self, ds: &Dataset, filter: Option<String>) -> Result<usize> {
        ds.count_rows(filter)
            .await
            .map(|count| count as usize)
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub fn dataset_version(&self, ds: &Dataset) -> u64 {
        ds.version().version
    }

    pub async fn table_state(&self, dataset_uri: &str, ds: &Dataset) -> Result<TableState> {
        Ok(TableState {
            version: self.dataset_version(ds),
            row_count: self.count_rows(ds, None).await? as u64,
            version_metadata: self.dataset_version_metadata(dataset_uri, ds)?,
        })
    }

    /// Legacy inline-commit append: writes fragments AND commits in one
    /// call, advancing Lance HEAD as a side effect. Not on the
    /// `TableStorage` trait surface — the staged primitive `stage_append`
    /// + `commit_staged` is the engine write path. This inherent method
    /// survives only for in-source recovery test setup, so it is
    /// `#[cfg(test)]`-gated: engine code physically cannot call it (which
    /// enforces "no new call sites" by construction and silences the
    /// dead-code warning the non-test lib build would otherwise emit).
    #[cfg(test)]
    pub(crate) async fn append_batch(
        &self,
        dataset_uri: &str,
        ds: &mut Dataset,
        batch: RecordBatch,
    ) -> Result<TableState> {
        if batch.num_rows() == 0 {
            return self.table_state(dataset_uri, ds).await;
        }
        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let params = WriteParams {
            mode: WriteMode::Append,
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        ds.append(reader, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.table_state(dataset_uri, ds).await
    }

    pub async fn append_or_create_batch(
        dataset_uri: &str,
        dataset: Option<Dataset>,
        batch: RecordBatch,
    ) -> Result<Dataset> {
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        match dataset {
            Some(mut ds) => {
                let params = WriteParams {
                    mode: WriteMode::Append,
                    allow_external_blob_outside_bases: true,
                    auto_cleanup: None,
                    skip_auto_cleanup: true,
                    ..Default::default()
                };
                ds.append(reader, Some(params))
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
                Ok(ds)
            }
            None => {
                let params = WriteParams {
                    mode: WriteMode::Create,
                    enable_stable_row_ids: true,
                    data_storage_version: Some(LanceFileVersion::V2_2),
                    allow_external_blob_outside_bases: true,
                    auto_cleanup: None,
                    skip_auto_cleanup: true,
                    ..Default::default()
                };
                Dataset::write(reader, dataset_uri, Some(params))
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))
            }
        }
    }

    /// Stage a delete without advancing Lance HEAD — the two-phase analogue of
    /// `stage_merge_insert`. `DeleteBuilder::execute_uncommitted` writes the
    /// per-fragment deletion files to object storage (Phase A) and returns an
    /// uncommitted `Operation::Delete` transaction; HEAD does NOT advance until
    /// `commit_staged`. A 0-row delete is a TRUE no-op: `None` (no transaction,
    /// no fragments, no version). For a non-empty delete the returned
    /// `StagedWrite` carries the deletion-vector-bearing `updated_fragments` as
    /// `new_fragments` and the superseded originals (+ any fully-removed
    /// fragments) as `removed_fragment_ids`, so `combine_committed_with_staged`
    /// (`committed - removed + new`) makes an in-query read see the deletion.
    /// Like `stage_merge_insert`, this must carry Lance's `affected_rows`
    /// metadata through to `commit_staged`; otherwise a staged transaction loses
    /// the row-level conflict information Lance's rebase path needs.
    pub async fn stage_delete(&self, ds: &Dataset, filter: &str) -> Result<Option<StagedWrite>> {
        let uncommitted = DeleteBuilder::new(Arc::new(ds.clone()), filter)
            .execute_uncommitted()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        if uncommitted.num_deleted_rows == 0 {
            return Ok(None);
        }

        let (new_fragments, removed_fragment_ids) = match &uncommitted.transaction.operation {
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            } => {
                // The originals superseded by their deletion-vector rewrites must
                // be filtered out of the read view; `deleted_fragment_ids` are
                // whole-fragment removals.
                let mut removed = deleted_fragment_ids.clone();
                removed.extend(updated_fragments.iter().map(|f| f.id));
                (updated_fragments.clone(), removed)
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_delete: expected Operation::Delete, got {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };

        Ok(Some(StagedWrite::with_commit_metadata(
            uncommitted.transaction,
            StagedCommitMetadata::affected_rows(uncommitted.affected_rows),
            new_fragments,
            removed_fragment_ids,
        )))
    }

    // ─── Staged-write API ────────────────────────────────────────────────────
    //
    // These primitives wrap Lance's distributed-write API: each call writes
    // fragment files to object storage but does NOT advance the dataset's
    // HEAD or commit a manifest entry. The returned `Transaction` is held by
    // the caller (typically `MutationStaging` or the loader's accumulator)
    // and committed at end-of-query via `commit_staged`. On failure the
    // fragments remain unreferenced and are reclaimed by `cleanup_old_versions`.
    //
    // The extracted `Vec<Fragment>` is for read-your-writes within the same
    // query: subsequent ops construct a `Scanner` and call
    // `scanner.with_fragments(staged.clone())` to see staged data alongside
    // the committed snapshot. Lance's filter pushdown, vector search, and
    // FTS all respect the supplied fragment list.

    /// Stage an append: write fragment files for `batch`, return the
    /// uncommitted Lance transaction plus the new fragments for
    /// read-your-writes.
    ///
    /// `prior_stages` is the slice of staged writes already accumulated
    /// against the **same dataset** in the same query. Pass `&[]` for the
    /// first call; pass the accumulated stages for subsequent calls. The
    /// primitive uses this to offset row-ID assignment so chained
    /// `stage_append` calls don't produce overlapping `_rowid` ranges.
    /// Mirrors `scan_with_staged`'s `&[StagedWrite]` shape — the same
    /// slice gets passed to both.
    ///
    /// On stable-row-id datasets we manually populate `row_id_meta` on
    /// the cloned `new_fragments` we expose for `scan_with_staged`.
    /// Lance's `InsertBuilder::execute_uncommitted` produces fragments
    /// with `row_id_meta = None`; row IDs are normally assigned by
    /// `Transaction::assign_row_ids` during commit. Because
    /// `scan_with_staged` reads the staged fragments *before* commit,
    /// the scanner trips on a stable-row-id dataset
    /// (`Error::internal("Missing row id meta")` from
    /// `dataset/rowids.rs:22`). The transaction's internal fragment copy
    /// stays untouched — Lance assigns IDs there independently at commit
    /// time, and the two ID assignments don't have to agree because no
    /// caller threads `_rowid` from the staged scan into the commit
    /// path.
    ///
    /// **Contract: `prior_stages` must contain only previous
    /// `stage_append` results against the same dataset.** Mixing
    /// stage_merge_insert into `prior_stages` would over-count because
    /// merge_insert's `new_fragments` include rewrites that don't add
    /// rows. The engine's parse-time D₂′ check (per touched table: all
    /// stage_append OR exactly one stage_merge_insert) guarantees this
    /// upstream; on the primitive layer it's the caller's responsibility.
    pub async fn stage_append(
        &self,
        ds: &Dataset,
        batch: RecordBatch,
        prior_stages: &[StagedWrite],
    ) -> Result<StagedWrite> {
        if batch.num_rows() == 0 {
            return Err(OmniError::manifest_internal(
                "stage_append called with empty batch".to_string(),
            ));
        }
        let appended_rows = batch.num_rows() as u64;
        let params = WriteParams {
            mode: WriteMode::Append,
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let transaction = InsertBuilder::new(Arc::new(ds.clone()))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        // Record only after the staging write succeeds, so a failed write does
        // not inflate the probe (matches `stage_append_stream`'s ordering).
        crate::instrumentation::record_stage_append(appended_rows);
        let mut new_fragments = match &transaction.operation {
            Operation::Append { fragments } => fragments.clone(),
            Operation::Overwrite { fragments, .. } => fragments.clone(),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_append: unexpected Lance operation {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };
        // Assign real fragment IDs. Lance's `InsertBuilder::execute_uncommitted`
        // returns fragments with `id = 0` ("Temporary ID" — see lance-6.0.1
        // `dataset/write.rs:1044/1712`); the real assignment happens during
        // commit via `Transaction::fragments_with_ids`. Because we expose
        // these fragments to `scan_with_staged` *before* commit, two staged
        // fragments (or one staged + the seed) would collide on `id = 0`,
        // causing Lance's scanner to mishandle the combined list (silent
        // duplicates / dropped rows). Mirror the commit-time renumbering
        // here, using `ds.manifest.max_fragment_id() + 1` as the base and
        // accounting for prior stages.
        // ds.manifest.max_fragment_id is Option<u32>; cast up to u64 because
        // Lance's Fragment::id (and the commit-time renumbering counter in
        // Transaction::fragments_with_ids) operate on u64.
        let next_id_base = ds.manifest.max_fragment_id.unwrap_or(0) as u64
            + 1
            + prior_stages_fragment_count(prior_stages);
        assign_fragment_ids(&mut new_fragments, next_id_base);
        if ds.manifest.uses_stable_row_ids() {
            let prior_rows = prior_stages_row_count(prior_stages)?;
            let start_row_id = ds.manifest.next_row_id + prior_rows;
            assign_row_id_meta(&mut new_fragments, start_row_id)?;
        }
        Ok(StagedWrite::new(
            transaction,
            new_fragments,
            // Append never supersedes existing fragments.
            Vec::new(),
        ))
    }

    /// Streaming variant of [`Self::stage_append`]: appends the rows of `source`
    /// into `ds` without materializing them in memory. It scans `source` lazily
    /// (`scan_stream_for_rewrite`) and hands the stream to Lance's
    /// `execute_uncommitted_stream`, which rolls fragments at `max_rows_per_file`
    /// — bounded memory, one Append transaction. This is the substrate-blessed
    /// bulk-append path (the same one LanceDB's `Table::add` uses). Identical
    /// fragment-id / stable-row-id staging as `stage_append`.
    ///
    /// TRANSITIONAL caller — its only caller is the row-level merge append
    /// (`publish_adopted_delta`, see `AdoptDelta`), which the fragment-adopt work
    /// (Lance #7263/#7185) removes: a fragment graft re-appends no rows. This
    /// primitive and `scan_stream_for_rewrite` are then dead unless re-adopted as
    /// a general bulk-append path (the `Table::add` shape makes that plausible).
    pub async fn stage_append_stream(
        &self,
        ds: &Dataset,
        source: &Dataset,
        prior_stages: &[StagedWrite],
    ) -> Result<StagedWrite> {
        let stream = self.scan_stream_for_rewrite(source).await?;
        let params = WriteParams {
            mode: WriteMode::Append,
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let transaction = InsertBuilder::new(Arc::new(ds.clone()))
            .with_params(&params)
            .execute_uncommitted_stream(stream)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let mut new_fragments = match &transaction.operation {
            Operation::Append { fragments } => fragments.clone(),
            Operation::Overwrite { fragments, .. } => fragments.clone(),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_append_stream: unexpected Lance operation {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };
        let appended_rows: u64 = new_fragments
            .iter()
            .filter_map(|f| f.physical_rows)
            .map(|r| r as u64)
            .sum();
        crate::instrumentation::record_stage_append(appended_rows);
        // Same commit-time fragment-id / row-id renumbering as `stage_append`.
        let next_id_base = ds.manifest.max_fragment_id.unwrap_or(0) as u64
            + 1
            + prior_stages_fragment_count(prior_stages);
        assign_fragment_ids(&mut new_fragments, next_id_base);
        if ds.manifest.uses_stable_row_ids() {
            let prior_rows = prior_stages_row_count(prior_stages)?;
            let start_row_id = ds.manifest.next_row_id + prior_rows;
            assign_row_id_meta(&mut new_fragments, start_row_id)?;
        }
        Ok(StagedWrite::new(transaction, new_fragments, Vec::new()))
    }

    /// Stage a merge_insert (upsert): write fragment files describing the
    /// merge result, return the uncommitted transaction plus the new
    /// fragments. The transaction's `Operation::Update` carries the
    /// fragments-to-remove and fragments-to-add; for read-your-writes we
    /// expose `new_fragments` (rows that will be visible after commit).
    ///
    /// **Contract: do not chain `stage_merge_insert` calls on the same
    /// table within one query.** Each call's `MergeInsertBuilder` runs
    /// against the supplied dataset's committed view — it does not see
    /// fragments produced by a previous staged merge on the same table.
    /// Two chained `stage_merge_insert`s whose source rows share keys will
    /// each independently produce `Operation::Update` transactions whose
    /// `new_fragments` contain a row for the shared key. `scan_with_staged`
    /// (and `count_rows_with_staged`) will then return both — i.e.
    /// **duplicates by key**.
    ///
    /// This is intrinsic to the underlying Lance API: there is no public
    /// way to make `MergeInsertBuilder` see uncommitted fragments. The
    /// engine's `MutationStaging` accumulator works around this by
    /// concatenating per-table batches in memory and issuing exactly
    /// one `stage_merge_insert` per touched table at end-of-query (with
    /// last-write-wins dedupe by id) — see `exec/staging.rs`. Direct
    /// callers of this primitive must respect the contract themselves.
    ///
    /// Lift path: either a Lance API extension that lets
    /// `MergeInsertBuilder` accept additional staged fragments, or an
    /// in-memory pre-merge here that folds prior staged batches into the
    /// input stream. See `docs/dev/writes.md`.
    pub async fn stage_merge_insert(
        &self,
        ds: Dataset,
        batch: RecordBatch,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<StagedWrite> {
        if batch.num_rows() == 0 {
            return Err(OmniError::manifest_internal(
                "stage_merge_insert called with empty batch".to_string(),
            ));
        }
        let merged_rows = batch.num_rows() as u64;
        let inserts_unmatched = matches!(when_not_matched, WhenNotMatched::InsertAll);

        // Precondition for the FirstSeen workaround below: every call path that
        // reaches stage_merge_insert (load, MutationStaging::finalize,
        // branch_merge::publish_rewritten_merge_table) must hand in a source
        // batch that is unique by `key_columns`. Without this check,
        // `SourceDedupeBehavior::FirstSeen` would silently collapse genuine
        // duplicates instead of erroring.
        check_batch_unique_by_keys(&batch, &key_columns, "stage_merge_insert")?;

        let ds = Arc::new(ds);
        let mut builder = MergeInsertBuilder::try_new(ds, key_columns)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        builder.when_matched(when_matched);
        builder.when_not_matched(when_not_matched);
        // Workaround for a Lance bug class where sequential merge_insert calls
        // against rows previously rewritten by merge_insert produce a spurious
        // "Ambiguous merge inserts: multiple source rows match the same target
        // row on (id = ...)" error. Lance's `processed_row_ids:
        // Mutex<HashSet<u64>>` (lance-6.0.1 `src/dataset/write/merge_insert.rs`)
        // double-processes the same source/target match against datasets
        // previously rewritten by merge_insert, and the default
        // `SourceDedupeBehavior::Fail` errors on the second insertion; FirstSeen
        // makes Lance skip the duplicate match instead. Correctness-preserving
        // because every call path pre-dedupes the source batch by id or surfaces
        // a real source dup via `check_batch_unique_by_keys` above (load:
        // `enforce_unique_constraints_intra_batch`; mutate:
        // `MutationStaging::finalize`; branch-merge: the `OrderedTableCursor`
        // walk in `exec/merge.rs`). Retire when upstream Lance fixes the bug
        // class. Tracked at MR-957; upstream: lance-format/lance#6877.
        builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
        let job = builder
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
        let uncommitted = job
            .execute_uncommitted(stream)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        // Record only after the staging write succeeds, so a failed write does
        // not inflate the probe (matches `stage_append`/`stage_append_stream`).
        crate::instrumentation::record_stage_merge_insert(
            merged_rows,
            &schema,
            inserts_unmatched,
        );
        // Operation::Update { removed_fragment_ids, updated_fragments, new_fragments, .. } —
        // `new_fragments` are the freshly inserted rows; `updated_fragments`
        // are rewrites of existing fragments that include both retained and
        // updated rows; `removed_fragment_ids` lists the committed-manifest
        // fragments that those rewrites supersede. For read-your-writes we
        // expose `updated_fragments + new_fragments` and the
        // `removed_fragment_ids` so `scan_with_staged` can filter the
        // superseded committed fragments before combining — otherwise a
        // single merge_insert appears as duplicate rows (original committed
        // version + rewritten staged version).
        let (new_fragments, removed_fragment_ids) = match &uncommitted.transaction.operation {
            Operation::Update {
                new_fragments,
                updated_fragments,
                removed_fragment_ids,
                ..
            } => {
                let mut all = updated_fragments.clone();
                all.extend(new_fragments.iter().cloned());
                (all, removed_fragment_ids.clone())
            }
            Operation::Append { fragments } => (fragments.clone(), Vec::new()),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_merge_insert: unexpected Lance operation {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };
        Ok(StagedWrite::with_commit_metadata(
            uncommitted.transaction,
            StagedCommitMetadata::affected_rows(uncommitted.affected_rows),
            new_fragments,
            removed_fragment_ids,
        ))
    }

    /// Commit a previously-staged write onto `ds`, returning the new dataset
    /// (with HEAD advanced). The staged packet owns the Lance transaction plus
    /// any commit metadata (`affected_rows` for delete/merge rebase). Used by
    /// the publisher at end-of-query to materialize all staged writes before
    /// the meta-manifest commit.
    pub async fn commit_staged(&self, ds: Arc<Dataset>, staged: StagedWrite) -> Result<Dataset> {
        self.commit_staged_with_retry_budget(ds, staged, None)
            .await
            .map(|(dataset, _)| dataset)
    }

    /// Commit an RFC-022-enrolled staged effect with no commit-conflict retry.
    ///
    /// `CommitBuilder::with_max_retries(0)` gives Lance one commit attempt. It
    /// can still perform its initial conflict-resolution pass before that
    /// attempt, so this method also reads back and returns the identity of the
    /// transaction that actually landed. Callers must compare it with
    /// [`StagedWrite::transaction_identity`] AND require the returned dataset
    /// version to equal `read_version + 1`: Lance's preflight rebase can
    /// preserve the transaction fields while committing at a later version.
    /// Either mismatch is a post-effect recovery case, not permission to widen
    /// the prepared plan.
    pub async fn commit_staged_exact(
        &self,
        ds: Arc<Dataset>,
        staged: StagedWrite,
    ) -> Result<(Dataset, StagedTransactionIdentity)> {
        let (dataset, committed_identity) = self
            .commit_staged_with_retry_budget(ds, staged, Some(0))
            .await?;
        let committed_identity = committed_identity.ok_or_else(|| {
            OmniError::manifest_internal(
                "Lance committed a staged effect without a readable transaction identity",
            )
        })?;
        Ok((dataset, committed_identity))
    }

    async fn commit_staged_with_retry_budget(
        &self,
        ds: Arc<Dataset>,
        staged: StagedWrite,
        max_retries: Option<u32>,
    ) -> Result<(Dataset, Option<StagedTransactionIdentity>)> {
        // Skip Lance's auto-cleanup hook on every commit. OmniGraph owns version
        // GC explicitly (optimize.rs::cleanup_all_tables); Lance's hook fires off
        // the *dataset's stored* `lance.auto_cleanup.*` config, which graphs
        // created before the v7 bump (6.0.1 defaulted auto_cleanup ON) still
        // carry — so `WriteParams::auto_cleanup = None` alone does NOT stop it on
        // upgraded graphs. Skipping here covers the staged write path (the main
        // data path) for new and legacy datasets alike, preventing Lance from
        // GC'ing versions the __manifest still pins for snapshots/time-travel.
        let mut builder = CommitBuilder::new(ds).with_skip_auto_cleanup(true);
        if let Some(max_retries) = max_retries {
            builder = builder.with_max_retries(max_retries);
        }
        if let Some(affected_rows) = staged.commit_metadata.affected_rows {
            builder = builder.with_affected_rows(affected_rows);
        }
        let dataset = builder
            .execute(staged.transaction)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let committed_identity = if max_retries.is_some() {
            dataset
                .read_transaction()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
                .as_ref()
                .map(StagedTransactionIdentity::from)
        } else {
            None
        };
        Ok((dataset, committed_identity))
    }

    /// Stage an overwrite (write_fragments + Operation::Overwrite { schema, fragments }).
    /// Returns a StagedWrite carrying the replacement fragments. HEAD does
    /// NOT advance.
    ///
    /// Lance shape: `InsertBuilder::with_params(WriteParams { mode: Overwrite, .. })
    /// .execute_uncommitted(vec![batch])` produces a `Transaction` whose
    /// `Operation::Overwrite` carries the new schema + fragments. The
    /// transaction is committed via `commit_staged` (same call as
    /// `stage_append`).
    ///
    /// MR-793 Phase 2: introduces this for the schema_apply rewrite path.
    /// Lance API verified in `.context/mr-793-design.md` Appendix A.1.
    pub async fn stage_overwrite(&self, ds: &Dataset, batch: RecordBatch) -> Result<StagedWrite> {
        // `enable_stable_row_ids: true` is defensive — empirically Lance 6.0.1
        // preserves the source dataset's flag through `Operation::Overwrite`
        // when WriteParams omits it (pinned by
        // `stage_overwrite_preserves_stable_row_ids` in tests/staged_writes.rs),
        // but setting it explicitly keeps the invariant documented at every Overwrite site
        // (see docs/storage.md "Stable row IDs"). Setting it on an existing
        // dataset that was created without stable row IDs is a no-op per
        // Lance's row-id-lineage spec, so this stays correct for legacy
        // datasets.
        let (transaction, mut new_fragments) = if batch.num_rows() == 0 {
            let schema = LanceSchema::try_from(batch.schema().as_ref())
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let transaction = TransactionBuilder::new(
                ds.manifest.version,
                Operation::Overwrite {
                    fragments: Vec::new(),
                    schema,
                    config_upsert_values: None,
                    initial_bases: None,
                },
            )
            .build();
            (transaction, Vec::new())
        } else {
            let params = WriteParams {
                mode: WriteMode::Overwrite,
                enable_stable_row_ids: true,
                allow_external_blob_outside_bases: true,
                auto_cleanup: None,
                skip_auto_cleanup: true,
                ..Default::default()
            };
            let transaction = InsertBuilder::new(Arc::new(ds.clone()))
                .with_params(&params)
                .execute_uncommitted(vec![batch])
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let new_fragments = match &transaction.operation {
                Operation::Overwrite { fragments, .. } => fragments.clone(),
                other => {
                    return Err(OmniError::manifest_internal(format!(
                        "stage_overwrite: unexpected Lance operation {:?}",
                        std::mem::discriminant(other)
                    )));
                }
            };
            (transaction, new_fragments)
        };
        // Overwrite REPLACES every committed fragment, and Lance restarts
        // fragment-ID and row-ID counters at the post-commit version.
        // For our pre-commit staged view we need to:
        //   1) Renumber temporary fragment IDs (Lance returns them as
        //      `id = 0` from `execute_uncommitted` — see stage_append
        //      for the same fix). For Overwrite there are no committed
        //      fragments to collide with (they're all in
        //      removed_fragment_ids below), so start at 1.
        //   2) For stable-row-id datasets, assign row_id_meta starting
        //      at 0 (Overwrite is a fresh-start) so `scan_with_staged`
        //      doesn't hit the "Missing row id meta" panic in
        //      lance-6.0.1 dataset/rowids.rs:22.
        assign_fragment_ids(&mut new_fragments, 1);
        if ds.manifest.uses_stable_row_ids() {
            assign_row_id_meta(&mut new_fragments, 0)?;
        }
        // Overwrite REPLACES every committed fragment. For
        // read-your-writes via scan_with_staged, list every committed
        // fragment in removed_fragment_ids so the post-stage view shows
        // ONLY the staged fragments.
        let removed_fragment_ids: Vec<u64> = ds.manifest.fragments.iter().map(|f| f.id).collect();
        Ok(StagedWrite::new(
            transaction,
            new_fragments,
            removed_fragment_ids,
        ))
    }

    /// Stage a BTREE scalar index build. Returns a StagedWrite whose
    /// transaction commits via `commit_staged`. HEAD does NOT advance.
    ///
    /// Lance shape: `CreateIndexBuilder::execute_uncommitted` returns
    /// `IndexMetadata`; we manually wrap it in `Operation::CreateIndex
    /// { new_indices, removed_indices }` via the public `TransactionBuilder`,
    /// replicating the simple (non-segment-commit-path) branch of Lance's
    /// `CreateIndexBuilder::execute` (lance-6.0.1 `src/index/create.rs:502-512`).
    ///
    /// `removed_indices` mirrors `execute()` lines 466-476: when the
    /// build replaces an existing same-named index, those entries are
    /// listed for tombstoning by the manifest commit.
    ///
    /// MR-793 Phase 2: scalar index types (BTree, Inverted) are
    /// stage-able. Vector indices are NOT (segment-commit-path requires
    /// `build_index_metadata_from_segments` which is `pub(crate)` in
    /// lance-6.0.1); see `create_vector_index` and Appendix A.3.
    pub async fn stage_create_btree_index(
        &self,
        ds: &Dataset,
        columns: &[&str],
    ) -> Result<StagedWrite> {
        let params = ScalarIndexParams::default();
        let mut ds_clone = ds.clone();
        let new_idx = ds_clone
            .create_index_builder(columns, IndexType::BTree, &params)
            .replace(true)
            .execute_uncommitted()
            .await
            .map_err(|e| OmniError::Lance(format!("stage_create_btree_index: {}", e)))?;
        let removed_indices: Vec<IndexMetadata> = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .iter()
            .filter(|idx| idx.name == new_idx.name)
            .cloned()
            .collect();
        let transaction = TransactionBuilder::new(
            new_idx.dataset_version,
            Operation::CreateIndex {
                new_indices: vec![new_idx],
                removed_indices,
            },
        )
        .build();
        Ok(StagedWrite::new(transaction, Vec::new(), Vec::new()))
    }

    /// Stage an INVERTED (FTS) scalar index build. Same shape as
    /// `stage_create_btree_index`; see its docs for the Lance API
    /// citation and contract notes.
    pub async fn stage_create_inverted_index(
        &self,
        ds: &Dataset,
        column: &str,
    ) -> Result<StagedWrite> {
        let params = InvertedIndexParams::default();
        let mut ds_clone = ds.clone();
        let new_idx = ds_clone
            .create_index_builder(&[column], IndexType::Inverted, &params)
            .replace(true)
            .execute_uncommitted()
            .await
            .map_err(|e| OmniError::Lance(format!("stage_create_inverted_index: {}", e)))?;
        let removed_indices: Vec<IndexMetadata> = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .iter()
            .filter(|idx| idx.name == new_idx.name)
            .cloned()
            .collect();
        let transaction = TransactionBuilder::new(
            new_idx.dataset_version,
            Operation::CreateIndex {
                new_indices: vec![new_idx],
                removed_indices,
            },
        )
        .build();
        Ok(StagedWrite::new(transaction, Vec::new(), Vec::new()))
    }

    /// Run a scan with optional uncommitted staged writes visible
    /// alongside the committed snapshot. When `staged` is empty this is
    /// identical to `scan(...)`.
    ///
    /// Composes the visible fragment list as `committed - removed + new`:
    /// the committed manifest's fragments, minus any fragment IDs that
    /// staged `Operation::Update`s (merge_insert rewrites) have superseded,
    /// plus the staged new/updated fragments. Without the `removed`
    /// filter, a merge_insert that rewrites an existing fragment would
    /// surface twice — once via the original committed fragment, once via
    /// the rewrite in `new_fragments`.
    ///
    /// **Filter contract is incomplete on staged fragments.** When `filter`
    /// is `Some(...)`, Lance pushes the predicate to per-fragment scans
    /// with stats-based pruning. Uncommitted fragments produced by
    /// `write_fragments_internal` lack the per-column statistics that
    /// committed fragments carry; Lance's optimizer drops them from the
    /// filtered scan even when their data would match. Staged-fragment
    /// rows are silently absent from the result. `scanner.use_stats(false)`
    /// does not fix this in lance 6.0.1. Callers needing correct filtered
    /// reads against staged data should use a different strategy — the
    /// engine's `MutationStaging` accumulator unions in-memory pending
    /// batches with the committed scan via DataFusion `MemTable` (see
    /// `scan_with_pending`).
    ///
    /// This method remains on the surface for primitive-level testing
    /// (basic stage + scan correctness without filters works) and for
    /// callers that don't need filter pushdown.
    pub async fn scan_with_staged(
        &self,
        ds: &Dataset,
        staged: &[StagedWrite],
        projection: Option<&[&str]>,
        filter: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        if staged.is_empty() {
            return self.scan(ds, projection, filter, None).await;
        }
        let mut scanner = ds.scan();
        if let Some(cols) = projection {
            let owned: Vec<String> = cols.iter().map(|s| s.to_string()).collect();
            scanner
                .project(&owned)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        if let Some(f) = filter {
            scanner
                .filter(f)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        scanner.with_fragments(combine_committed_with_staged(ds, staged));
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        stream
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    /// Scan committed via Lance + apply the same filter to in-memory
    /// pending batches via DataFusion `MemTable`, concat the two result
    /// streams. The replacement for `scan_with_staged` in engine code:
    /// the staged-write writer accumulates input batches in memory and
    /// unions them with the committed snapshot at read time,
    /// sidestepping the `Scanner::with_fragments` filter-pushdown
    /// limitation documented on `scan_with_staged`.
    ///
    /// `committed_ds` should be opened at the pre-mutation
    /// `expected_version` (the same version captured in `MutationStaging::expected_versions`
    /// at first touch of the table). `pending_batches` are the per-table
    /// accumulator's batches in their input shape. `pending_schema` is
    /// the schema of the accumulated batches; passing `None` falls back
    /// to the schema of the first pending batch.
    ///
    /// `filter` is the Lance / DataFusion SQL predicate. It is applied
    /// to both sides — Lance pushes it down on the committed side; the
    /// pending side runs it through a fresh DataFusion `SessionContext`
    /// with the batches registered as a `MemTable` named `pending`.
    ///
    /// `key_column` controls how committed and pending are unioned:
    /// - **`None` (union semantics)**: every committed row that matches
    ///   the filter and every pending row that matches the filter is
    ///   returned. Correct when committed and pending cannot share a
    ///   primary key — e.g., Append-mode loads with ULID-generated ids,
    ///   or any read where pending hasn't been used to update committed
    ///   rows.
    /// - **`Some(col)` (merge / shadow semantics)**: committed rows whose
    ///   `col` value appears in any pending batch are EXCLUDED from the
    ///   result; only pending's view of those rows is returned. Required
    ///   for Merge-mode reads (e.g., `execute_update` on the engine path)
    ///   so a chained `update` doesn't see stale committed values that
    ///   a prior op already updated in pending. Without this, a predicate
    ///   like `where age > 30` can match a row that an earlier
    ///   `set age = 20` already moved out of range.
    ///
    /// When `pending_batches` is empty this delegates to the regular
    /// scan path.
    pub async fn scan_with_pending(
        &self,
        committed_ds: &Dataset,
        pending_batches: &[RecordBatch],
        pending_schema: Option<SchemaRef>,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        key_column: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        // Contract: when merge-shadow semantics are requested via
        // `key_column`, the committed-side projection MUST include that
        // column so we can filter committed rows whose key appears in
        // pending. Silently dropping the shadow when projection omits
        // the key would re-introduce union semantics behind the
        // caller's back. Reject up front with a clear error so callers
        // either (a) include the key in projection or (b) drop
        // `key_column` if union is what they wanted.
        if let (Some(key_col), Some(cols)) = (key_column, projection) {
            if !cols.iter().any(|c| *c == key_col) {
                return Err(OmniError::Lance(format!(
                    "scan_with_pending: key_column '{}' must appear in projection \
                     when merge-shadow semantics are requested (got projection = {:?})",
                    key_col, cols
                )));
            }
        }

        let committed = self.scan(committed_ds, projection, filter, None).await?;
        combine_committed_with_pending(
            committed,
            pending_batches,
            pending_schema,
            projection,
            filter,
            key_column,
        )
        .await
    }

    /// Read a blob-bearing table as a full logical merge source while retaining
    /// [`Self::scan_with_pending`]'s merge-shadow semantics.
    ///
    /// Lance normally scans blob-v2 columns as physical descriptor structs.
    /// Those descriptors are a read representation, not valid writer input: a
    /// full-row merge sends them through the blob writer, which requires the
    /// logical `Struct<data, uri>` shape and fails with `Blob struct missing
    /// data field`. This remained hidden while an `id` BTREE forced Lance's
    /// partial-column merge plan; once index creation became reconciler-owned,
    /// an index-absent table correctly selected the full-scan plan and exposed
    /// the representation mismatch.
    ///
    /// A first scan evaluates `filter` with blob columns excluded and retains
    /// stable row ids. Full descriptor rows are then taken by those ids without
    /// a filter, and only their payloads are rebuilt as logical
    /// [`BlobArrayBuilder`] columns. Pending rows already carry logical blob
    /// arrays; both sides have the dataset's full logical schema before the
    /// existing shadow union. The resulting batch is therefore valid for either
    /// of Lance's merge plans, making physical index presence a performance
    /// detail rather than a correctness precondition.
    pub async fn scan_with_pending_materialized_blobs(
        &self,
        committed_ds: &Dataset,
        pending_batches: &[RecordBatch],
        pending_schema: Option<SchemaRef>,
        filter: Option<&str>,
        key_column: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        let blob_columns = committed_ds
            .schema()
            .fields
            .iter()
            .filter(|field| field.is_blob())
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        if blob_columns.is_empty() {
            return self
                .scan_with_pending(
                    committed_ds,
                    pending_batches,
                    pending_schema,
                    None,
                    filter,
                    key_column,
                )
                .await;
        }

        // The pinned Lance revision cannot combine a predicate filter with a
        // full blob-v2 projection: `FilteredReadExec` applies the descriptor
        // child projection to the logical blob field and panics. Select the
        // matched rows without blob columns, retaining stable `_rowid`, then
        // take exactly those full descriptor rows without a filter and rebuild
        // their logical blobs. Thus payload I/O remains proportional to matched
        // rows while avoiding any dependence on which merge/index plan Lance
        // selects later.
        let non_blob_columns = committed_ds
            .schema()
            .fields
            .iter()
            .filter(|field| !field.is_blob())
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let matched = Self::scan_stream(committed_ds, Some(&non_blob_columns), filter, None, true)
            .await?
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;

        let mut committed = Vec::with_capacity(matched.len());
        for batch in matched {
            let row_ids = batch
                .column_by_name("_rowid")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::Lance("expected _rowid in predicate-matched blob scan".to_string())
                })?
                .values()
                .to_vec();
            if row_ids.is_empty() {
                continue;
            }
            let descriptors = committed_ds
                .take_rows(&row_ids, committed_ds.schema().clone())
                .await
                .map_err(|error| OmniError::Lance(error.to_string()))?;
            committed.push(
                Self::materialize_blob_batch_with_row_ids(committed_ds, descriptors, &row_ids)
                    .await?,
            );
        }

        let combined = combine_committed_with_pending(
            committed,
            pending_batches,
            pending_schema,
            None,
            filter,
            key_column,
        )
        .await?;
        Ok(combined)
    }

    /// `count_rows` variant that respects staged writes. Used for
    /// edge-cardinality validation that needs to see staged edges before
    /// commit. Same `committed - removed + new` composition as
    /// `scan_with_staged`.
    pub async fn count_rows_with_staged(
        &self,
        ds: &Dataset,
        staged: &[StagedWrite],
        filter: Option<String>,
    ) -> Result<usize> {
        if staged.is_empty() {
            return self.count_rows(ds, filter).await;
        }
        let mut scanner = ds.scan();
        if let Some(f) = filter {
            scanner
                .filter(&f)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        scanner.with_fragments(combine_committed_with_staged(ds, staged));
        let count = scanner
            .count_rows()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(count as usize)
    }

    async fn user_indices_for_column(
        &self,
        ds: &Dataset,
        column: &str,
    ) -> Result<Vec<IndexMetadata>> {
        let field_id = ds
            .schema()
            .field(column)
            .map(|field| field.id)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "dataset is missing expected index column '{}'",
                    column
                ))
            })?;
        let indices = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(indices
            .iter()
            .filter(|index| !is_system_index(index))
            .filter(|index| index.fields.len() == 1 && index.fields[0] == field_id)
            .cloned()
            .collect())
    }

    pub async fn has_btree_index(&self, ds: &Dataset, column: &str) -> Result<bool> {
        let indices = self.user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| details.type_url.ends_with("BTreeIndexDetails"))
                .unwrap_or(false)
        }))
    }

    pub async fn has_fts_index(&self, ds: &Dataset, column: &str) -> Result<bool> {
        let indices = self.user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| IndexDetails(details.clone()).supports_fts())
                .unwrap_or(false)
        }))
    }

    pub async fn has_vector_index(&self, ds: &Dataset, column: &str) -> Result<bool> {
        let indices = self.user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| IndexDetails(details.clone()).is_vector())
                .unwrap_or(false)
        }))
    }

    pub(crate) async fn create_vector_index(&self, ds: &mut Dataset, column: &str) -> Result<()> {
        let params = lance::index::vector::VectorIndexParams::ivf_flat(1, MetricType::L2);
        ds.create_index_builder(&[column], IndexType::Vector, &params)
            .replace(true)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        // Record only after the index build succeeds, so a failed build does not
        // inflate the probe (matches the `stage_*` probes).
        crate::instrumentation::record_create_vector_index();
        Ok(())
    }

    pub async fn create_empty_dataset(dataset_uri: &str, schema: &SchemaRef) -> Result<Dataset> {
        let batch = RecordBatch::new_empty(schema.clone());
        Self::write_dataset(dataset_uri, batch).await
    }

    pub async fn first_row_id_for_filter(&self, ds: &Dataset, filter: &str) -> Result<Option<u64>> {
        let batches = Self::scan_stream(ds, Some(&["id"]), Some(filter), None, true)
            .await?
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(batches.iter().find_map(|batch| {
            batch
                .column_by_name("_rowid")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .and_then(|arr| (arr.len() > 0).then(|| arr.value(0)))
        }))
    }

    pub async fn write_dataset(dataset_uri: &str, batch: RecordBatch) -> Result<Dataset> {
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        Dataset::write(reader, dataset_uri, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }
}

/// Build the `Scanner::with_fragments` argument for read-your-writes:
/// committed manifest fragments minus any fragment IDs superseded by the
/// staged writes, plus the staged `new_fragments`. Order is:
///   1. committed fragments whose IDs are NOT in any staged
///      `removed_fragment_ids` (preserves committed order),
///   2. all staged `new_fragments` in stage order.
///
/// Lance's `Scanner` does not require any particular ordering between
/// committed and staged fragments — `with_fragments` scopes the scan to
/// exactly the supplied list. The dedup matters because merge_insert
/// rewrites a fragment in place at the Lance layer: the rewritten
/// fragment is in `new_fragments`, the original (which it supersedes) is
/// in `committed` until manifest commit, and including both would yield
/// duplicate rows.
///
/// **Inter-stage supersession is not handled here.** Each StagedWrite's
/// `removed_fragment_ids` lists committed-manifest fragment IDs only; a
/// later staged merge cannot know about an earlier staged merge's
/// fragments (Lance's `MergeInsertBuilder` runs against the committed
/// view). If two `stage_merge_insert`s on the same table produce rows
/// with the same key, the combined view returns duplicates by key. The
/// engine's mutation path enforces "per touched table: all stage_append
/// OR exactly one stage_merge_insert" at parse time (D₂′ in
/// `exec/mutation.rs`) so this primitive's caller never chains merges.
/// See `stage_merge_insert` for the full contract.
/// Sum `physical_rows` across all fragments in the supplied stages.
/// Used by `stage_append` to compute the row-ID offset for chained
/// `stage_append` calls against the same dataset.
///
/// Assumes `prior_stages` contains only `stage_append` results — see
/// `stage_append`'s D₂′ contract. For `stage_merge_insert` results the
/// `new_fragments` include rewrites that don't add new rows, so this
/// would over-count.
fn prior_stages_fragment_count(prior_stages: &[StagedWrite]) -> u64 {
    prior_stages
        .iter()
        .map(|s| s.new_fragments.len() as u64)
        .sum()
}

/// Assign sequential fragment IDs starting at `start_id`. Mirrors Lance's
/// commit-time `Transaction::fragments_with_ids` (lance-6.0.1
/// `dataset/transaction.rs:1456`) — fragments produced by
/// `InsertBuilder::execute_uncommitted` start with `id = 0` as a temporary
/// placeholder; we renumber here so they don't collide with committed
/// fragments (or with each other across chained stages) when the slice is
/// passed to `Scanner::with_fragments`.
fn assign_fragment_ids(fragments: &mut [Fragment], start_id: u64) {
    for (i, fragment) in fragments.iter_mut().enumerate() {
        if fragment.id == 0 {
            fragment.id = start_id + i as u64;
        }
    }
}

fn prior_stages_row_count(prior_stages: &[StagedWrite]) -> Result<u64> {
    let mut total: u64 = 0;
    for stage in prior_stages {
        for fragment in &stage.new_fragments {
            let physical_rows = fragment.physical_rows.ok_or_else(|| {
                OmniError::manifest_internal(
                    "prior_stages_row_count: fragment is missing physical_rows".to_string(),
                )
            })? as u64;
            total += physical_rows;
        }
    }
    Ok(total)
}

/// Assign sequential row IDs to fragments that lack them, starting from
/// `start_row_id`. Mirrors the relevant arm of Lance's
/// `Transaction::assign_row_ids` (lance-6.0.1 `dataset/transaction.rs:2682`)
/// for the `row_id_meta = None` case — fragments produced by
/// `InsertBuilder::execute_uncommitted` against a stable-row-id dataset.
///
/// Used only by `stage_append` for read-your-writes — see its docstring
/// for why pre-commit assignment is needed and why diverging from Lance's
/// commit-time IDs is safe.
fn assign_row_id_meta(fragments: &mut [Fragment], start_row_id: u64) -> Result<()> {
    let mut next_row_id = start_row_id;
    for fragment in fragments {
        if fragment.row_id_meta.is_some() {
            continue;
        }
        let physical_rows = fragment.physical_rows.ok_or_else(|| {
            OmniError::manifest_internal(
                "stage_append: fragment is missing physical_rows".to_string(),
            )
        })? as u64;
        let row_ids = next_row_id..(next_row_id + physical_rows);
        let sequence = RowIdSequence::from(row_ids);
        let serialized = write_row_ids(&sequence);
        fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
        next_row_id += physical_rows;
    }
    Ok(())
}

/// Apply the in-memory half of `scan_with_pending` to an already-scanned
/// committed view. Kept as one helper so the ordinary descriptor scan and the
/// blob-materializing scan cannot drift in their key-shadow semantics.
async fn combine_committed_with_pending(
    committed: Vec<RecordBatch>,
    pending_batches: &[RecordBatch],
    pending_schema: Option<SchemaRef>,
    projection: Option<&[&str]>,
    filter: Option<&str>,
    key_column: Option<&str>,
) -> Result<Vec<RecordBatch>> {
    if pending_batches.is_empty() {
        return Ok(committed);
    }

    // Shadow committed rows whose key value also appears in pending. This is
    // deliberately applied before filtering pending: a pending row that no
    // longer matches must still suppress its stale committed predecessor.
    let committed = match key_column {
        Some(key_col) => {
            let pending_keys = collect_string_column_values(pending_batches, key_col)?;
            if pending_keys.is_empty() {
                committed
            } else {
                filter_out_rows_where_string_in(committed, key_col, &pending_keys)?
            }
        }
        None => committed,
    };

    let pending = scan_pending_batches(pending_batches, pending_schema, projection, filter).await?;
    let mut out = committed;
    out.extend(pending);
    Ok(out)
}

/// Collect the set of values in a Utf8 column across multiple batches.
/// Used by `scan_with_pending`'s merge-semantic path to identify
/// committed rows that are shadowed by pending writes. NULL values are
/// skipped.
fn collect_string_column_values(
    batches: &[RecordBatch],
    column: &str,
) -> Result<std::collections::HashSet<String>> {
    use arrow_array::{Array, StringArray};
    let mut out = std::collections::HashSet::new();
    for batch in batches {
        let Some(col) = batch.column_by_name(column) else {
            return Err(OmniError::Lance(format!(
                "scan_with_pending: pending batch missing key column '{}'",
                column
            )));
        };
        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            OmniError::Lance(format!(
                "scan_with_pending: key column '{}' is not Utf8",
                column
            ))
        })?;
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                out.insert(arr.value(i).to_string());
            }
        }
    }
    Ok(out)
}

/// Drop rows from `batches` whose Utf8 `column` value is in `excluded`.
/// Used by `scan_with_pending`'s merge-semantic path to shadow committed
/// rows that pending has already updated. Returns the surviving rows.
///
/// `scan_with_pending` validates up front that the projection contains
/// `column`, so a missing column here is a programmer error — error
/// loudly instead of silently passing batches through (which would
/// re-introduce the union semantics the caller asked us to avoid).
fn filter_out_rows_where_string_in(
    batches: Vec<RecordBatch>,
    column: &str,
    excluded: &std::collections::HashSet<String>,
) -> Result<Vec<RecordBatch>> {
    use arrow_array::{Array, BooleanArray, StringArray};
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            out.push(batch);
            continue;
        }
        let col = batch.column_by_name(column).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "scan_with_pending: committed batch missing key column '{}' \
                 (the up-front projection check should have rejected this)",
                column
            ))
        })?;
        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            OmniError::Lance(format!(
                "scan_with_pending: committed column '{}' is not Utf8",
                column
            ))
        })?;
        let mask: BooleanArray = (0..arr.len())
            .map(|i| {
                if arr.is_valid(i) {
                    Some(!excluded.contains(arr.value(i)))
                } else {
                    Some(true)
                }
            })
            .collect();
        let filtered = arrow_select::filter::filter_record_batch(&batch, &mask)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        out.push(filtered);
    }
    Ok(out)
}

/// Apply `projection` and `filter` to in-memory pending batches via a
/// fresh DataFusion `SessionContext`. Used by `scan_with_pending` for
/// the read-your-writes side of the in-memory staging accumulator.
///
/// `pending_batches` must be non-empty (the caller short-circuits on
/// empty).
///
/// **SQL dialect contract.** `filter` is also passed to Lance's scanner
/// on the committed side. Lance and DataFusion both accept standard
/// SQL comparison predicates (`col op literal`) and OmniGraph's
/// `predicate_to_sql` only emits those shapes today (`=`, `!=`, `>`,
/// `<`, `>=`, `<=`). If a future caller introduces a Lance-specific
/// scanner extension (vector search, FTS, `_rowid` references) into
/// the filter, this function will need explicit translation — DataFusion
/// won't recognize those operators against the in-memory `MemTable`.
async fn scan_pending_batches(
    pending_batches: &[RecordBatch],
    pending_schema: Option<SchemaRef>,
    projection: Option<&[&str]>,
    filter: Option<&str>,
) -> Result<Vec<RecordBatch>> {
    let schema = pending_schema.unwrap_or_else(|| pending_batches[0].schema());
    // #283: disable SQL identifier normalization so an unquoted camelCase
    // column in `filter` (e.g. `repoName = 'acme'`, emitted unquoted by
    // `predicate_to_sql` because the committed Lance scan needs it unquoted)
    // is matched case-preserving against the case-sensitive MemTable schema.
    // Without this, DataFusion lowercases `repoName` → `reponame` and fails to
    // resolve. Quoted identifiers (the projection list below) are unaffected.
    let mut config = datafusion::execution::context::SessionConfig::new();
    config.options_mut().sql_parser.enable_ident_normalization = false;
    let ctx = datafusion::execution::context::SessionContext::new_with_config(config);
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![pending_batches.to_vec()])
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    ctx.register_table("pending", Arc::new(mem))
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let proj = projection
        .map(|cols| {
            cols.iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "*".to_string());
    let where_clause = filter.map(|f| format!("WHERE {f}")).unwrap_or_default();
    let sql = format!("SELECT {proj} FROM pending {where_clause}");
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    df.collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn combine_committed_with_staged(ds: &Dataset, staged: &[StagedWrite]) -> Vec<Fragment> {
    let removed: std::collections::HashSet<u64> = staged
        .iter()
        .flat_map(|w| w.removed_fragment_ids.iter().copied())
        .collect();
    let mut combined: Vec<Fragment> = ds
        .manifest
        .fragments
        .iter()
        .filter(|f| !removed.contains(&f.id))
        .cloned()
        .collect();
    for write in staged {
        combined.extend(write.new_fragments.iter().cloned());
    }
    combined
}

/// Precondition guard for `stage_merge_insert`.
/// Both opt into `SourceDedupeBehavior::FirstSeen` to suppress the Lance
/// `processed_row_ids` bug (MR-957). FirstSeen would *also* silently
/// collapse genuine duplicate source keys; this check restores fail-fast
/// behavior on real dups by erroring before the builder gets a chance to
/// silently skip them.
///
/// Today only single-column string keys are used at the call sites
/// (`vec!["id".to_string()]`). The check restricts itself to that shape
/// and surfaces an internal error if a future caller passes anything
/// else — keeping the assumption explicit instead of silently degrading.
fn check_batch_unique_by_keys(
    batch: &RecordBatch,
    key_columns: &[String],
    context: &'static str,
) -> Result<()> {
    if key_columns.len() != 1 {
        return Err(OmniError::manifest_internal(format!(
            "{}: check_batch_unique_by_keys currently supports single-column keys only, got {:?}",
            context, key_columns
        )));
    }
    let key_col_name = &key_columns[0];
    let column = batch.column_by_name(key_col_name).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "{}: source batch missing key column '{}'",
            context, key_col_name
        ))
    })?;
    let strs = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{}: key column '{}' is not a StringArray (got {:?})",
                context,
                key_col_name,
                column.data_type()
            ))
        })?;

    let mut seen: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(batch.num_rows());
    for i in 0..strs.len() {
        if !strs.is_valid(i) {
            continue;
        }
        let v = strs.value(i);
        if !seen.insert(v) {
            return Err(OmniError::manifest(format!(
                "{}: duplicate source row for key '{}' (column '{}'); \
                 callers must hand in a batch unique by `key_columns` \
                 — see MR-957",
                context, v, key_col_name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    fn batch_with_ids(ids: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let col = Arc::new(StringArray::from(ids.to_vec())) as ArrayRef;
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn check_batch_unique_by_keys_passes_when_all_unique() {
        let batch = batch_with_ids(&["a", "b", "c"]);
        check_batch_unique_by_keys(&batch, &["id".to_string()], "test").unwrap();
    }

    #[test]
    fn check_batch_unique_by_keys_errors_on_duplicate_id() {
        let batch = batch_with_ids(&["a", "b", "a"]);
        let err = check_batch_unique_by_keys(&batch, &["id".to_string()], "test").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate source row for key 'a'"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("MR-957"),
            "error should reference MR-957: {msg}"
        );
    }

    #[test]
    fn check_batch_unique_by_keys_rejects_multi_column_keys() {
        let batch = batch_with_ids(&["a"]);
        let err =
            check_batch_unique_by_keys(&batch, &["id".to_string(), "other".to_string()], "test")
                .unwrap_err();
        assert!(err.to_string().contains("single-column keys only"));
    }
}
