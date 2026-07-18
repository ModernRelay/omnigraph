pub(crate) mod mem_wal;

use arrow_array::{
    Array, ArrayRef, LargeBinaryArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use arrow_schema::SchemaRef;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream, Scanner};
use lance::dataset::transaction::{Operation, Transaction, TransactionBuilder, UpdateMode};
use lance::dataset::write::merge_insert::inserted_rows::{KeyExistenceFilterBuilder, KeyValue};
use lance::dataset::write::merge_insert::{
    MergeStats, SourceDedupeBehavior, UncommittedMergeInsert,
};
use lance::dataset::{
    CommitBuilder, DeleteBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched,
    WriteMode, WriteParams,
};
use lance::datatypes::Schema as LanceSchema;
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
use std::collections::HashMap;
use std::sync::Arc;

use crate::db::manifest::TableVersionMetadata;
use crate::db::{Snapshot, SubTableEntry};
use crate::error::{OmniError, Result};
use crate::storage_layer::{
    ForkOutcome, IndexBuildSpec, KEYED_WRITE_MAX_BYTES, KEYED_WRITE_MAX_ROWS, KeyedWriteSemantics,
    PendingScanBudget, ProvenInsertChunk,
};

/// Durable proof carried by an OmniGraph insertion-only transaction.
///
/// `v1` means that every key encoded by the transaction's exact-id conflict
/// filter was proven absent from the transaction's effective parent.  The
/// branch-merge pure-insert adapter accepts the no-target-probe route only when
/// every transaction in the complete source interval carries this exact
/// certificate and independently passes the structural history proof. A
/// strict insert may mint it after its exact preflight; an upsert may mint it
/// only when Lance's completed merge statistics and transaction shape prove
/// that the actual effect inserted every source row and updated nothing.
pub(crate) const INSERT_ABSENCE_PROPERTY: &str = "omnigraph.insert_absence";
pub(crate) const INSERT_ABSENCE_V1: &str = "v1";

pub(crate) fn has_insert_absence_certificate(transaction: &Transaction) -> bool {
    transaction
        .transaction_properties
        .as_ref()
        .and_then(|properties| properties.get(INSERT_ABSENCE_PROPERTY))
        .is_some_and(|value| value == INSERT_ABSENCE_V1)
}

/// Verify one persisted link of the insertion-absence proof chain and return
/// its exact physical row contribution. This is intentionally stricter than a
/// property lookup: the caller must also supply the expected parent version,
/// exact primary-key field, and complete schema preorder observed for the
/// source interval.
pub(crate) fn certified_insert_absence_rows(
    transaction: &Transaction,
    expected_read_version: u64,
    id_field_id: i32,
    expected_schema_preorder_ids: &[u32],
) -> Option<u64> {
    if transaction.read_version != expected_read_version
        || transaction.uuid.is_empty()
        || !has_insert_absence_certificate(transaction)
    {
        return None;
    }
    let Operation::Update {
        removed_fragment_ids,
        updated_fragments,
        new_fragments,
        fields_modified,
        merged_generations,
        fields_for_preserving_frag_bitmap,
        update_mode,
        inserted_rows_filter,
        updated_fragment_offsets,
    } = &transaction.operation
    else {
        return None;
    };
    if !removed_fragment_ids.is_empty()
        || !updated_fragments.is_empty()
        || new_fragments.is_empty()
        || !fields_modified.is_empty()
        || !merged_generations.is_empty()
        || fields_for_preserving_frag_bitmap != expected_schema_preorder_ids
        || update_mode != &Some(UpdateMode::RewriteRows)
        || updated_fragment_offsets.is_some()
        || !inserted_rows_filter
            .as_ref()
            .is_some_and(|filter| filter.field_ids == vec![id_field_id])
    {
        return None;
    }
    new_fragments.iter().try_fold(0_u64, |rows, fragment| {
        rows.checked_add(fragment.physical_rows? as u64)
    })
}

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
    /// Exact ids carried by a production strict-insert batch. Kept only until
    /// commit so an effect-free substrate conflict can be re-probed against
    /// fresh manifest authority before it is normalized to `KeyConflict`.
    strict_source_ids: Option<Vec<String>>,
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
            strict_source_ids: None,
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
            strict_source_ids: None,
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

    fn set_strict_source_ids(&mut self, source_ids: Vec<String>) {
        self.strict_source_ids = Some(source_ids);
    }

    pub(crate) fn take_strict_source_ids(&mut self) -> Option<Vec<String>> {
        self.strict_source_ids.take()
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
        snapshot.open_dataset(table_key).await
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
    /// This tolerates an already-absent branch — pinned Lance's native
    /// `force_delete_branch` treats both a missing contents ref and a missing
    /// `tree/{branch}/` directory as success, while OmniGraph also normalizes a
    /// raced `RefNotFound` / `NotFound` around the non-atomic contents delete.
    /// Safe to call on a possibly-orphaned or already-reclaimed fork.
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
    /// downstream writer never materializes the whole table in memory. Blob
    /// columns are rebuilt asynchronously one scanner batch at a time; ordinary
    /// columns pass through the native lazy scan.
    pub async fn scan_stream_for_rewrite(&self, ds: &Dataset) -> Result<SendableRecordBatchStream> {
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if has_blob_columns {
            let arrow_schema: SchemaRef = Arc::new(ds.schema().into());
            let raw: SendableRecordBatchStream =
                Self::scan_stream(ds, None, None, None, true).await?.into();
            return Ok(Self::materialize_blob_stream(
                ds.clone(),
                arrow_schema,
                raw,
                None,
            ));
        }
        // Non-blob: a true lazy scan. `DatasetRecordBatchStream` converts to the
        // `SendableRecordBatchStream` that `execute_uncommitted_stream` consumes.
        Ok(Self::scan_stream(ds, None, None, None, false).await?.into())
    }

    /// Explicitly batch-bounded variant used by RFC-023's branch-adopt chain.
    /// Unlike the environment-controlled default scanner size, this ceiling is
    /// part of the recovery plan: one emitted batch becomes one pre-minted
    /// strict keyed transaction.
    pub async fn scan_stream_for_rewrite_bounded(
        &self,
        ds: &Dataset,
        batch_rows: usize,
        batch_bytes: u64,
    ) -> Result<SendableRecordBatchStream> {
        if batch_rows == 0 || batch_bytes == 0 {
            return Err(OmniError::manifest_internal(
                "bounded rewrite stream requires non-zero row and byte ceilings",
            ));
        }
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if has_blob_columns {
            let arrow_schema: SchemaRef = Arc::new(ds.schema().into());
            let raw: SendableRecordBatchStream =
                Self::scan_stream_with(ds, None, None, None, true, |scanner| {
                    // The byte cap sees compact blob descriptors, not payload.
                    // Materialize one descriptor row at a time; the downstream
                    // chunk assembler combines only writer-proven bounded rows.
                    scanner.batch_size(1);
                    scanner.batch_size_bytes(batch_bytes);
                    Ok(())
                })
                .await?
                .into();
            return Ok(Self::materialize_blob_stream(
                ds.clone(),
                arrow_schema,
                raw,
                Some(batch_bytes),
            ));
        }
        Ok(
            Self::scan_stream_with(ds, None, None, None, false, |scanner| {
                scanner.batch_size(batch_rows);
                scanner.batch_size_bytes(batch_bytes);
                Ok(())
            })
            .await?
            .into(),
        )
    }

    fn materialize_blob_stream(
        ds: Dataset,
        schema: SchemaRef,
        raw: SendableRecordBatchStream,
        max_blob_bytes: Option<u64>,
    ) -> SendableRecordBatchStream {
        if let Some(limit) = max_blob_bytes {
            // `LANCE_DEFAULT_BATCH_SIZE` overrides Scanner::batch_size on the
            // pinned Lance revision. Split descriptor batches ourselves so an
            // environment setting cannot make one materialization read across
            // writer-defined recovery chunks. `try_unfold` is sequential: at
            // most one row's blob payload is read before downstream consumes it.
            let materialized = futures::stream::try_unfold(
                (raw, None::<RecordBatch>, 0_usize, ds),
                move |(mut raw, mut current, mut offset, ds)| async move {
                    loop {
                        if let Some(batch) = current.as_ref()
                            && offset < batch.num_rows()
                        {
                            let row = batch.slice(offset, 1);
                            offset += 1;
                            let materialized =
                                Self::materialize_blob_batch_with_limit(&ds, row, Some(limit))
                                    .await
                                    .map_err(|error| {
                                        datafusion::error::DataFusionError::Execution(
                                            error.to_string(),
                                        )
                                    })?;
                            return Ok(Some((materialized, (raw, current, offset, ds))));
                        }

                        match raw.try_next().await? {
                            Some(batch) => {
                                current = Some(batch);
                                offset = 0;
                            }
                            None => return Ok(None),
                        }
                    }
                },
            );
            return Box::pin(RecordBatchStreamAdapter::new(schema, materialized));
        }

        let materialized = raw.and_then(move |batch| {
            let ds = ds.clone();
            async move {
                Self::materialize_blob_batch_with_limit(&ds, batch, None)
                    .await
                    .map_err(|error| {
                        datafusion::error::DataFusionError::Execution(error.to_string())
                    })
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, materialized))
    }

    pub(crate) async fn materialize_blob_batch(
        ds: &Dataset,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        Self::materialize_blob_batch_with_limit(ds, batch, None).await
    }

    /// Blob-aware materialization with a pre-read payload ceiling. Lance's
    /// scanner accounts for descriptor bytes, so the referenced file sizes
    /// must be checked separately before `BlobFile::read` allocates them.
    pub(crate) async fn materialize_blob_batch_bounded(
        ds: &Dataset,
        batch: RecordBatch,
        max_blob_bytes: u64,
    ) -> Result<RecordBatch> {
        Self::materialize_blob_batch_with_limit(ds, batch, Some(max_blob_bytes)).await
    }

    async fn materialize_blob_batch_with_limit(
        ds: &Dataset,
        batch: RecordBatch,
        max_blob_bytes: Option<u64>,
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

        Self::materialize_blob_batch_with_row_ids(ds, batch, &row_ids, max_blob_bytes).await
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
        max_blob_bytes: Option<u64>,
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
        let mut materialized_blob_bytes = 0_u64;
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
                    Self::rebuild_blob_column(
                        ds,
                        field.name(),
                        descriptions,
                        row_ids,
                        max_blob_bytes,
                        &mut materialized_blob_bytes,
                    )
                    .await?,
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
        max_blob_bytes: Option<u64>,
        materialized_blob_bytes: &mut u64,
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
            let next_blob_bytes = materialized_blob_bytes
                .checked_add(blob.size())
                .ok_or_else(|| OmniError::manifest_internal("blob byte count overflow"))?;
            if let Some(limit) = max_blob_bytes
                && next_blob_bytes > limit
            {
                return Err(OmniError::resource_limit(
                    "materialized blob payload bytes",
                    limit,
                    next_blob_bytes,
                ));
            }
            *materialized_blob_bytes = next_blob_bytes;
            crate::instrumentation::record_blob_payload_read();
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
        Ok(matches!(
            crate::blob_descriptor::decode_blob_descriptor(descriptions, row)?,
            crate::blob_descriptor::BlobDescriptor::Null
        ))
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

    /// Streaming scan with explicit row and decoded-byte ceilings per emitted
    /// batch. Callers that retain or transform batches as part of a bounded
    /// write plan must use this instead of inheriting Lance's environment-
    /// controlled defaults.
    pub async fn scan_stream_bounded(
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
        batch_rows: usize,
        batch_bytes: u64,
    ) -> Result<DatasetRecordBatchStream> {
        if batch_rows == 0 || batch_bytes == 0 {
            return Err(OmniError::manifest_internal(
                "bounded scan requires non-zero row and byte ceilings",
            ));
        }
        Self::scan_stream_with(ds, projection, filter, order_by, with_row_id, |scanner| {
            scanner.batch_size(batch_rows);
            scanner.batch_size_bytes(batch_bytes);
            Ok(())
        })
        .await
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
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
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
                let control_session = crate::lance_access::control_session();
                let params = WriteParams {
                    mode: WriteMode::Create,
                    enable_stable_row_ids: true,
                    data_storage_version: Some(LanceFileVersion::V2_2),
                    allow_external_blob_outside_bases: true,
                    auto_cleanup: None,
                    skip_auto_cleanup: true,
                    session: Some(control_session),
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
    #[cfg(test)]
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

    /// Test-only streaming variant of [`Self::stage_append`]. It retains the old
    /// substrate primitive for direct Lance-shape coverage, but production graph
    /// writes cannot select it: RFC-023 branch adoption consumes a bounded rewrite
    /// stream as exact-`id` keyed chunks instead.
    #[cfg(test)]
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

    /// Stage one RFC-023 keyed write from an in-memory batch.
    ///
    /// Unlike the legacy generic merge primitive below, this adapter fixes the
    /// join key to `id`, derives Lance actions from a closed logical enum, and
    /// forces the non-index v2 route that emits `inserted_rows_filter`. This is
    /// the only production insertion-bearing route for keyed graph tables.
    pub async fn stage_keyed_write(
        &self,
        ds: Dataset,
        table_key: &str,
        batch: RecordBatch,
        semantics: KeyedWriteSemantics,
    ) -> Result<StagedWrite> {
        if batch.num_rows() == 0 {
            return Err(OmniError::manifest_internal(
                "stage_keyed_write called with empty batch",
            ));
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| OmniError::manifest_internal("keyed write batch bytes exceed u64"))?;
        if batch.num_rows() > KEYED_WRITE_MAX_ROWS {
            return Err(OmniError::resource_limit(
                format!("keyed write rows for {table_key}"),
                KEYED_WRITE_MAX_ROWS as u64,
                batch.num_rows() as u64,
            ));
        }
        if batch_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("keyed write bytes for {table_key}"),
                KEYED_WRITE_MAX_BYTES,
                batch_bytes,
            ));
        }
        let id_field_id = exact_id_primary_key_field_id(&ds, "stage_keyed_write")?;
        let expected_read_version = ds.version().version;
        let expected_schema_preorder_ids = schema_preorder_field_ids(&ds, "stage_keyed_write")?;
        let source_ids = validate_keyed_write_batch_ids(&batch, table_key, "stage_keyed_write")?;
        if semantics == KeyedWriteSemantics::StrictInsert {
            Self::preflight_strict_insert_ids(&ds, table_key, &source_ids).await?;
        }

        // MergeInsertBuilder does not expose WriteParams and therefore cannot
        // opt into Lance's `allow_external_blob_outside_bases` reference
        // policy. Materialize URI-bearing source cells under the same byte
        // ceiling before handing the still-logical blob array to Lance. This
        // retains keyed fencing without an Append side door; Overwrite keeps
        // Lance's external-reference behavior because it accepts WriteParams.
        let batch = self.prepare_keyed_write_batch(table_key, batch).await?;

        let merged_rows = batch.num_rows() as u64;
        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
        let (mut staged, stats) = self
            .stage_keyed_write_from_stream(
                ds,
                stream,
                merged_rows,
                semantics,
                id_field_id,
                "stage_keyed_write",
            )
            .await?;
        if semantics == KeyedWriteSemantics::StrictInsert {
            certify_insert_absence(
                &mut staged.transaction,
                expected_read_version,
                id_field_id,
                &expected_schema_preorder_ids,
                &source_ids,
                "stage_keyed_write",
            )?;
        } else if merge_stats_prove_pure_insert(&stats, merged_rows)
            && let Err(error) = certify_insert_absence(
                &mut staged.transaction,
                expected_read_version,
                id_field_id,
                &expected_schema_preorder_ids,
                &source_ids,
                "stage_keyed_write",
            )
        {
            // The certificate is an optional optimization for Upsert. Lance's
            // completed statistics prove this execution inserted every row,
            // but an unfamiliar future transaction shape must disable the
            // history shortcut rather than fail an otherwise valid logical
            // write. StrictInsert keeps the same check mandatory because its
            // exact filter is part of the requested conflict semantics.
            tracing::debug!(
                table_key,
                error = %error,
                "all-new upsert is not eligible for the insertion-absence certificate"
            );
        }
        if semantics == KeyedWriteSemantics::StrictInsert {
            staged.set_strict_source_ids(source_ids);
        }
        Ok(staged)
    }

    /// Stage the narrow RFC-023 pure-insert fast path.
    ///
    /// The complete source history carries a durable proof that every batch key
    /// was absent from its effective parent. The caller's final authority and
    /// physical-baseline gates establish that the pinned target still equals
    /// that parent, so this adapter does not repeat the exact target probe. It
    /// uses Lance's public insert writer for immutable data fragments and
    /// replaces only its uncommitted `Append` operation with the same
    /// filter-bearing, insert-only `Update` shape emitted by pinned Lance
    /// merge-insert. The resulting transaction inherits the certificate, so
    /// the proof composes across later branch generations without creating a
    /// graph-visible Append side door.
    pub async fn stage_proven_strict_insert(
        &self,
        ds: Dataset,
        chunk: ProvenInsertChunk,
    ) -> Result<StagedWrite> {
        let (
            table_key,
            batch,
            expected_target_version,
            expected_schema_preorder_ids,
            expected_stable_row_ids,
            chunk_index,
        ) = chunk.into_parts();
        let table_key = table_key.as_str();
        if ds.version().version != expected_target_version {
            return Err(OmniError::manifest_read_set_changed(
                format!("proven_insert_target:{table_key}:chunk:{chunk_index}"),
                Some(expected_target_version.to_string()),
                Some(ds.version().version.to_string()),
            ));
        }
        if !expected_stable_row_ids || !ds.manifest.uses_stable_row_ids() {
            return Err(OmniError::manifest_internal(format!(
                "stage_proven_strict_insert requires stable target row ids for {table_key} chunk {chunk_index}"
            )));
        }
        if batch.num_rows() == 0 {
            return Err(OmniError::manifest_internal(
                "stage_proven_strict_insert called with empty batch",
            ));
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("proven strict insert batch bytes exceed u64")
        })?;
        if batch.num_rows() > KEYED_WRITE_MAX_ROWS {
            return Err(OmniError::resource_limit(
                format!("keyed write rows for {table_key}"),
                KEYED_WRITE_MAX_ROWS as u64,
                batch.num_rows() as u64,
            ));
        }
        if batch_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("keyed write bytes for {table_key}"),
                KEYED_WRITE_MAX_BYTES,
                batch_bytes,
            ));
        }

        let id_field_id = exact_id_primary_key_field_id(&ds, "stage_proven_strict_insert")?;
        let source_ids =
            validate_keyed_write_batch_ids(&batch, table_key, "stage_proven_strict_insert")?;
        let batch = self.prepare_keyed_write_batch(table_key, batch).await?;
        ensure_proven_insert_blobs_are_materialized(&batch, table_key)?;

        let mut filter_builder = KeyExistenceFilterBuilder::new(vec![id_field_id]);
        for id in &source_ids {
            filter_builder
                .insert(KeyValue::String(id.clone()))
                .map_err(|error| OmniError::Lance(error.to_string()))?;
        }
        if filter_builder.len() != batch.num_rows()
            || source_ids
                .iter()
                .any(|id| !filter_builder.contains(&KeyValue::String(id.clone())))
        {
            return Err(OmniError::manifest_internal(
                "stage_proven_strict_insert did not encode every source id in Lance's key filter",
            ));
        }
        let inserted_rows_filter = filter_builder.build();

        // This full pre-order field list is load-bearing. Lance's Update
        // manifest builder uses it to keep every existing user index from
        // claiming coverage of these newly written, not-yet-indexed fragments.
        // An empty or top-level-only list can cause silent missing query rows.
        let fields_for_preserving_frag_bitmap =
            schema_preorder_field_ids(&ds, "stage_proven_strict_insert")?;
        if fields_for_preserving_frag_bitmap != expected_schema_preorder_ids {
            return Err(OmniError::manifest_internal(format!(
                "stage_proven_strict_insert target schema changed for {table_key} chunk {chunk_index}"
            )));
        }
        let expected_read_version = ds.version().version;
        let ds = Arc::new(ds);
        let params = WriteParams {
            mode: WriteMode::Append,
            allow_external_blob_outside_bases: false,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let mut transaction = InsertBuilder::new(ds.clone())
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if transaction.read_version != expected_read_version {
            return Err(OmniError::manifest_internal(format!(
                "stage_proven_strict_insert wrote against version {}, expected {expected_read_version}",
                transaction.read_version
            )));
        }
        let transaction_fragments = match &transaction.operation {
            Operation::Append { fragments } => fragments.clone(),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_proven_strict_insert: expected Lance Append staging operation, got {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };
        transaction.operation = Operation::Update {
            removed_fragment_ids: Vec::new(),
            updated_fragments: Vec::new(),
            new_fragments: transaction_fragments.clone(),
            fields_modified: Vec::new(),
            merged_generations: Vec::new(),
            fields_for_preserving_frag_bitmap,
            update_mode: Some(UpdateMode::RewriteRows),
            inserted_rows_filter: Some(inserted_rows_filter),
            updated_fragment_offsets: None,
        };
        validate_transaction_exact_id_filter(
            &transaction,
            id_field_id,
            "stage_proven_strict_insert",
        )?;
        certify_insert_absence(
            &mut transaction,
            expected_read_version,
            id_field_id,
            &expected_schema_preorder_ids,
            &source_ids,
            "stage_proven_strict_insert",
        )?;

        // The transaction keeps Lance's temporary fragment ids and lets commit
        // assign target-local stable row ids. Only the read-your-writes copy is
        // normalized now, exactly as in the test-only staged Append adapter.
        let mut visible_fragments = transaction_fragments;
        let next_id_base = ds.manifest.max_fragment_id.unwrap_or(0) as u64 + 1;
        assign_fragment_ids(&mut visible_fragments, next_id_base);
        if ds.manifest.uses_stable_row_ids() {
            assign_row_id_meta(&mut visible_fragments, ds.manifest.next_row_id)?;
        }

        crate::instrumentation::record_stage_fenced_insert(source_ids.len() as u64);
        let mut staged = StagedWrite::with_commit_metadata(
            transaction,
            StagedCommitMetadata::affected_rows(Some(RowAddrTreeMap::new())),
            visible_fragments,
            Vec::new(),
        );
        staged.set_strict_source_ids(source_ids);
        Ok(staged)
    }

    /// Resolve any URI-bearing logical blobs into a bounded in-memory keyed
    /// source batch without writing Lance files or advancing HEAD.
    ///
    /// Deferred first-touch writes invoke this before arming recovery because
    /// their actual `MergeInsertBuilder` stage must wait until the target ref
    /// exists. Existing-table writes reach the same helper from
    /// [`Self::stage_keyed_write`].
    pub async fn prepare_keyed_write_batch(
        &self,
        table_key: &str,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        self.validate_keyed_write_batch(table_key, &batch)?;
        let batch = materialize_external_blob_inputs(batch, KEYED_WRITE_MAX_BYTES).await?;
        let materialized_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("materialized keyed write batch bytes exceed u64")
        })?;
        if materialized_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("keyed write bytes for {table_key}"),
                KEYED_WRITE_MAX_BYTES,
                materialized_bytes,
            ));
        }
        Ok(batch)
    }

    /// Validate one physical v6 graph-table batch without staging files or
    /// touching any Lance/manifest authority. This is deliberately separate
    /// from [`Self::stage_keyed_write`]: Overwrite and deferred first-touch
    /// plans must fail before recovery arm / native-ref creation too.
    pub fn validate_keyed_write_batch(&self, table_key: &str, batch: &RecordBatch) -> Result<()> {
        validate_keyed_write_batch_ids(batch, table_key, "prepare keyed write batch")?;
        Ok(())
    }

    /// Test-only streaming-source sibling of [`Self::stage_keyed_write`].
    ///
    /// The source must itself be an already-valid keyed graph table with exact
    /// `id` PK metadata.  Its narrow `id` projection is walked one record batch
    /// at a time; strict inserts probe the pinned target with a structured
    /// batch-sized `IN` predicate.  A non-blob full source is then scanned lazily
    /// into Lance's merge job, so vectors and other wide ordinary columns remain
    /// bounded by record-batch width rather than delta width. Blob tables are
    /// materialized one scanner batch at a time by `scan_stream_for_rewrite`.
    /// Cross-batch source uniqueness comes from the trusted keyed graph-table
    /// invariant; this sealed primitive does not accept an arbitrary external
    /// dataset as its source.
    #[cfg(test)]
    pub async fn stage_keyed_write_stream(
        &self,
        ds: Dataset,
        table_key: &str,
        source: &Dataset,
        semantics: KeyedWriteSemantics,
    ) -> Result<StagedWrite> {
        let id_field_id = exact_id_primary_key_field_id(&ds, "stage_keyed_write_stream")?;
        exact_id_primary_key_field_id(source, "stage_keyed_write_stream source")?;
        let mut id_stream = Self::scan_stream(source, Some(&["id"]), None, None, false).await?;
        let mut merged_rows = 0_u64;
        while let Some(batch) = id_stream
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            merged_rows = merged_rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stage_keyed_write_stream source row count overflow",
                    )
                })?;
            let source_ids =
                validate_keyed_write_batch_ids(&batch, table_key, "stage_keyed_write_stream")?;
            if semantics == KeyedWriteSemantics::StrictInsert {
                Self::preflight_strict_insert_ids(&ds, table_key, &source_ids).await?;
            }
        }
        if merged_rows == 0 {
            return Err(OmniError::manifest_internal(
                "stage_keyed_write_stream called with empty source dataset",
            ));
        }
        let stream = self.scan_stream_for_rewrite(source).await?;
        self.stage_keyed_write_from_stream(
            ds,
            stream,
            merged_rows,
            semantics,
            id_field_id,
            "stage_keyed_write_stream",
        )
        .await
        .map(|(staged, _stats)| staged)
    }

    /// Exact existing-id check for one bounded source batch.  This probes the
    /// same pinned `Dataset` later handed to `MergeInsertBuilder`; it neither
    /// opens HEAD nor parses Lance/DataFusion error strings.  A structured
    /// expression lets Lance use covered scalar-index fragments while retaining
    /// its correctness fallback over uncovered fragments.
    async fn preflight_strict_insert_ids(
        ds: &Dataset,
        table_key: &str,
        source_ids: &[String],
    ) -> Result<()> {
        crate::instrumentation::record_strict_insert_preflight();
        if let Some(id) = Self::first_existing_id(ds, source_ids).await? {
            return Err(OmniError::key_conflict(table_key, id));
        }
        Ok(())
    }

    /// Probe a manifest-pinned table image for one exact member of
    /// `source_ids`. This is shared by strict preflight and the post-conflict
    /// fresh-authority discriminator; both paths therefore use the same
    /// structured, scalar-index-eligible predicate and uncovered-fragment
    /// fallback.
    pub async fn first_existing_id(ds: &Dataset, source_ids: &[String]) -> Result<Option<String>> {
        use datafusion::prelude::{col, lit};

        exact_id_primary_key_field_id(ds, "first_existing_id")?;
        if source_ids.is_empty() {
            return Ok(None);
        }
        let filter =
            col("id").in_list(source_ids.iter().map(|id| lit(id.clone())).collect(), false);
        let mut target_ids =
            Self::scan_stream_with(ds, Some(&["id"]), None, None, false, |scanner| {
                scanner.filter_expr(filter);
                Ok(())
            })
            .await?;
        while let Some(batch) = target_ids
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            let ids = string_id_column(&batch, "stage_keyed_write strict preflight")?;
            for row in 0..ids.len() {
                if ids.is_valid(row) {
                    return Ok(Some(ids.value(row).to_string()));
                }
            }
        }
        Ok(None)
    }

    async fn stage_keyed_write_from_stream(
        &self,
        ds: Dataset,
        stream: SendableRecordBatchStream,
        merged_rows: u64,
        semantics: KeyedWriteSemantics,
        id_field_id: i32,
        context: &'static str,
    ) -> Result<(StagedWrite, MergeStats)> {
        let mut builder = MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".to_string()])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        builder.when_matched(match semantics {
            KeyedWriteSemantics::StrictInsert => WhenMatched::Fail,
            KeyedWriteSemantics::Upsert => WhenMatched::UpdateAll,
        });
        builder.when_not_matched(WhenNotMatched::InsertAll);
        // Beta.21's scalar-index v1 route omits the key filter.  Force v2 and
        // assert the resulting transaction shape below so a future Lance
        // routing change fails closed.
        builder.use_index(false);
        builder.conflict_retries(0);
        // FirstSeen works around Lance #6877.  The batch entry point proves
        // source-id uniqueness; the streaming entry point accepts only a
        // trusted exact-id-PK graph dataset and also rejects duplicates within
        // each physical record batch.
        builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
        let uncommitted = builder
            .try_build()
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .execute_uncommitted(stream)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;

        validate_exact_id_filter(&uncommitted, id_field_id, context)?;
        if semantics == KeyedWriteSemantics::StrictInsert {
            validate_strict_insert_merge_stats(&uncommitted, merged_rows, context)?;
        }
        let stats = uncommitted.stats.clone();
        crate::instrumentation::record_stage_merge_insert(merged_rows);
        Ok((staged_keyed_merge_result(uncommitted, context)?, stats))
    }

    /// Stream a provenance-proven pure-insert source interval in the same
    /// 8,192-row / 32-MiB chunks accepted by [`Self::stage_keyed_write`].
    ///
    /// The caller proves from Lance transaction history that every version in
    /// `(begin_version, end_version]` is insertion-only. This adapter evaluates
    /// the row-version predicate against the pinned source and exposes only
    /// those rows; it neither writes files nor advances HEAD. Every emitted
    /// batch is compacted away from a retained parent allocation when needed
    /// and rechecked against both hard ceilings before it reaches the per-chunk
    /// strict keyed writer.
    pub async fn scan_proven_insert_delta_bounded(
        source: &Dataset,
        table_key: &str,
        begin_version: u64,
        end_version: u64,
    ) -> Result<SendableRecordBatchStream> {
        use datafusion::prelude::{col, lit};

        if begin_version >= end_version {
            return Err(OmniError::manifest_internal(format!(
                "scan_proven_insert_delta_bounded for {table_key} requires begin_version < end_version, got {begin_version}..={end_version}"
            )));
        }
        if source.version().version != end_version {
            return Err(OmniError::manifest_internal(format!(
                "scan_proven_insert_delta_bounded for {table_key} received source version {}, expected pinned end version {end_version}",
                source.version().version
            )));
        }
        exact_id_primary_key_field_id(source, "scan_proven_insert_delta_bounded source")?;

        let created_in_interval = col("_row_created_at_version")
            .gt(lit(begin_version))
            .and(col("_row_created_at_version").lt_eq(lit(end_version)));
        let output_schema: SchemaRef = Arc::new(source.schema().into());
        let has_blob_columns = source
            .schema()
            .fields_pre_order()
            .any(|field| field.is_blob());

        if !has_blob_columns {
            let raw: SendableRecordBatchStream =
                Self::scan_stream_with(source, None, None, None, false, move |scanner| {
                    scanner.filter_expr(created_in_interval);
                    scanner.batch_size(KEYED_WRITE_MAX_ROWS);
                    scanner.batch_size_bytes(KEYED_WRITE_MAX_BYTES);
                    Ok(())
                })
                .await?
                .into();
            let raw = observe_proven_insert_raw_stream(raw);
            return Ok(bounded_proven_insert_stream(
                output_schema,
                raw,
                table_key.to_string(),
            ));
        }

        // Beta.21 cannot safely combine a predicate with a full blob-v2
        // descriptor projection. Select the interval using only ordinary
        // columns plus stable row ids, then take and materialize each exact
        // matched row. The one-row materialization shape is intentionally
        // conservative for blobs: it bounds payload allocation; the stream
        // normalizer below then coalesces those rows into ordinary bounded
        // recovery-transaction chunks.
        let non_blob_columns = source
            .schema()
            .fields
            .iter()
            .filter(|field| !field.is_blob())
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let raw: SendableRecordBatchStream = Self::scan_stream_with(
            source,
            Some(&non_blob_columns),
            None,
            None,
            true,
            move |scanner| {
                scanner.filter_expr(created_in_interval);
                scanner.batch_size(KEYED_WRITE_MAX_ROWS);
                scanner.batch_size_bytes(KEYED_WRITE_MAX_BYTES);
                Ok(())
            },
        )
        .await?
        .into();
        let raw = observe_proven_insert_raw_stream(raw);
        let materialized = futures::stream::try_unfold(
            (raw, None::<RecordBatch>, 0_usize, source.clone()),
            |(mut raw, mut current, mut offset, source)| async move {
                loop {
                    if let Some(batch) = current.as_ref()
                        && offset < batch.num_rows()
                    {
                        let row = batch.slice(offset, 1);
                        offset += 1;
                        let row_id = row
                                .column_by_name("_rowid")
                                .and_then(|column| {
                                    column.as_any().downcast_ref::<UInt64Array>()
                                })
                                .ok_or_else(|| {
                                    datafusion::error::DataFusionError::Execution(
                                        "scan_proven_insert_delta_bounded expected stable _rowid in blob source scan"
                                            .to_string(),
                                    )
                                })?
                                .value(0);
                        let descriptors = source
                            .take_rows(&[row_id], source.schema().clone())
                            .await
                            .map_err(|error| {
                                datafusion::error::DataFusionError::Execution(error.to_string())
                            })?;
                        let materialized = Self::materialize_blob_batch_with_row_ids(
                            &source,
                            descriptors,
                            &[row_id],
                            Some(KEYED_WRITE_MAX_BYTES),
                        )
                        .await
                        .map_err(|error| {
                            datafusion::error::DataFusionError::Execution(error.to_string())
                        })?;
                        return Ok(Some((materialized, (raw, current, offset, source))));
                    }

                    match raw.try_next().await? {
                        Some(batch) => {
                            current = Some(batch);
                            offset = 0;
                        }
                        None => return Ok(None),
                    }
                }
            },
        );
        let materialized: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            output_schema.clone(),
            materialized,
        ));
        Ok(bounded_proven_insert_stream(
            output_schema,
            materialized,
            table_key.to_string(),
        ))
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
    #[cfg(test)]
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
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
        let uncommitted = job
            .execute_uncommitted(stream)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        // Record only after the staging write succeeds, so a failed write does
        // not inflate the probe (matches `stage_append`/`stage_append_stream`).
        crate::instrumentation::record_stage_merge_insert(merged_rows);
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

    /// Commit a staged first-touch dataset creation with no conflict retry.
    ///
    /// A create transaction is Lance's `Operation::Overwrite` at
    /// `read_version = 0`. `with_max_retries(0)` selects Lance's strict
    /// overwrite path: if another writer created the dataset after this
    /// transaction was staged, Lance refuses the stale read-version-0 commit
    /// instead of rebasing it over the winner. If both writers still observe
    /// absence, the object store's atomic manifest create admits exactly one.
    pub async fn commit_staged_create_exact(
        &self,
        dataset_uri: &str,
        staged: StagedWrite,
    ) -> Result<(Dataset, StagedTransactionIdentity)> {
        if staged.transaction.read_version != 0
            || !matches!(staged.transaction.operation, Operation::Overwrite { .. })
        {
            return Err(OmniError::manifest_internal(
                "staged dataset creation must be an Overwrite transaction at read version 0",
            ));
        }
        if staged.commit_metadata.affected_rows.is_some() {
            return Err(OmniError::manifest_internal(
                "staged dataset creation cannot carry affected-row metadata",
            ));
        }

        let dataset = CommitBuilder::new(dataset_uri)
            .use_stable_row_ids(true)
            .with_storage_format(LanceFileVersion::V2_2)
            .enable_v2_manifest_paths(true)
            .with_session(self.session.clone())
            .with_skip_auto_cleanup(true)
            .with_max_retries(0)
            .execute(staged.transaction)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let committed_identity = dataset
            .read_transaction()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .as_ref()
            .map(StagedTransactionIdentity::from)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "Lance created a dataset without a readable transaction identity",
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
            .map_err(map_lance_commit_error)?;
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

    /// Stage creation of a new dataset without publishing its first manifest.
    ///
    /// Lance models creation as an `Operation::Overwrite` transaction based on
    /// version 0. Data files may be written by this call, but the dataset is not
    /// readable until [`Self::commit_staged_create_exact`] atomically creates
    /// version 1. The transaction UUID can therefore be bound to a recovery
    /// identity before that first visible effect.
    pub async fn stage_create(&self, dataset_uri: &str, batch: RecordBatch) -> Result<StagedWrite> {
        validate_external_blob_references(&batch).await?;
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            allow_external_blob_outside_bases: true,
            session: Some(self.session.clone()),
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let transaction = InsertBuilder::new(dataset_uri)
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        if transaction.read_version != 0 {
            return Err(OmniError::manifest_internal(format!(
                "stage_create resolved '{}' at existing version {}; expected an absent dataset",
                dataset_uri, transaction.read_version
            )));
        }
        let new_fragments = match &transaction.operation {
            Operation::Overwrite { fragments, .. } => fragments.clone(),
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "stage_create: unexpected Lance operation {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };
        Ok(StagedWrite::new(transaction, new_fragments, Vec::new()))
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
        validate_external_blob_references(&batch).await?;
        // `enable_stable_row_ids: true` is defensive — empirically Lance 6.0.1
        // preserves the source dataset's flag through `Operation::Overwrite`
        // when WriteParams omits it (pinned by
        // `stage_overwrite_preserves_stable_row_ids` in table_store/staged_tests.rs),
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

    /// Stage a batch of full-table index builds as one Lance transaction.
    ///
    /// Each builder writes its immutable index artifact and returns complete
    /// `IndexMetadata` through pinned Lance's public `execute_uncommitted` surface.
    /// All metadata is based on the same pinned dataset version and is wrapped
    /// in one `Operation::CreateIndex`, so committing any number of requested
    /// BTREE, FTS, and vector indexes advances the table exactly once. HEAD does
    /// not move during this method.
    ///
    /// This intentionally covers OmniGraph's current one-segment full-table
    /// vector shape. Lance's generic multi-segment commit helper remains an
    /// inline-commit API and is not used here.
    pub async fn stage_create_indices(
        &self,
        ds: &Dataset,
        specs: &[IndexBuildSpec],
    ) -> Result<StagedWrite> {
        if specs.is_empty() {
            return Err(OmniError::manifest_internal(
                "stage_create_indices requires at least one index specification",
            ));
        }

        let read_version = ds.manifest.version;
        let existing_indices = ds
            .load_indices()
            .await
            .map_err(|e| OmniError::Lance(format!("stage_create_indices: {e}")))?;
        let mut new_indices = Vec::with_capacity(specs.len());
        let mut new_names = std::collections::HashSet::with_capacity(specs.len());
        let mut vector_builds = 0usize;

        for spec in specs {
            let (column, index_type) = match spec {
                IndexBuildSpec::BTree { column } => (column, "BTREE"),
                IndexBuildSpec::FullText { column } => (column, "FTS"),
                IndexBuildSpec::Vector { column } => (column, "Vector"),
            };
            if column.is_empty() {
                return Err(OmniError::manifest_internal(format!(
                    "stage_create_indices received an empty {index_type} column name"
                )));
            }

            let mut ds_clone = ds.clone();
            let new_idx = match spec {
                IndexBuildSpec::BTree { column } => {
                    let params = ScalarIndexParams::default();
                    ds_clone
                        .create_index_builder(&[column.as_str()], IndexType::BTree, &params)
                        .replace(true)
                        .execute_uncommitted()
                        .await
                }
                IndexBuildSpec::FullText { column } => {
                    let params = InvertedIndexParams::default();
                    ds_clone
                        .create_index_builder(&[column.as_str()], IndexType::Inverted, &params)
                        .replace(true)
                        .execute_uncommitted()
                        .await
                }
                IndexBuildSpec::Vector { column } => {
                    let params =
                        lance::index::vector::VectorIndexParams::ivf_flat(1, MetricType::L2);
                    let new_idx = ds_clone
                        .create_index_builder(&[column.as_str()], IndexType::Vector, &params)
                        .replace(true)
                        .execute_uncommitted()
                        .await;
                    if new_idx.is_ok() {
                        vector_builds += 1;
                    }
                    new_idx
                }
            }
            .map_err(|e| {
                OmniError::Lance(format!(
                    "stage_create_indices: build {index_type} index on '{column}': {e}"
                ))
            })?;

            if new_idx.dataset_version != read_version {
                return Err(OmniError::manifest_internal(format!(
                    "staged index '{}' was built from dataset version {}, expected {}",
                    new_idx.name, new_idx.dataset_version, read_version
                )));
            }
            if !new_names.insert(new_idx.name.clone()) {
                return Err(OmniError::manifest_internal(format!(
                    "stage_create_indices produced duplicate index name '{}'",
                    new_idx.name
                )));
            }
            new_indices.push(new_idx);
        }

        let removed_indices: Vec<IndexMetadata> = existing_indices
            .iter()
            .filter(|idx| new_names.contains(&idx.name))
            .cloned()
            .collect();
        let transaction = TransactionBuilder::new(
            read_version,
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            },
        )
        .build();

        // Preserve the existing build-count probe while moving the vector
        // operation from inline commit to staged publication. Record only once
        // the entire batch staged successfully.
        for _ in 0..vector_builds {
            crate::instrumentation::record_stage_vector_index();
        }
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
    /// The committed side is always streamed so this mutation-only helper can
    /// enforce its resource budget incrementally, including when there are no
    /// pending batches.
    pub async fn scan_with_pending(
        &self,
        committed_ds: &Dataset,
        pending_batches: &[RecordBatch],
        pending_schema: Option<SchemaRef>,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        key_column: Option<&str>,
        budget: PendingScanBudget,
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

        // Pending is already bounded by MutationStaging, so evaluate its
        // predicate first. Its complete key set (including pending rows that
        // do not match this predicate) is retained separately and shadows the
        // committed side before that side is charged to the output budget.
        let pending_keys = match key_column {
            Some(key_col) => collect_string_column_values(pending_batches, key_col)?,
            None => std::collections::HashSet::new(),
        };
        let pending = if pending_batches.is_empty() {
            Vec::new()
        } else {
            scan_pending_batches(pending_batches, pending_schema, projection, filter).await?
        };
        let mut account = PendingScanAccount::new(budget)?;
        account.add_batches(&pending)?;

        let scan_rows = account.next_scan_rows();
        let scan_bytes = account.next_scan_bytes();
        let mut stream =
            Self::scan_stream_with(committed_ds, projection, filter, None, false, |scanner| {
                // Scanner byte batches are approximate, so correctness comes
                // from the per-emission accounting below. These values keep a
                // normal scan from decoding the entire match set before that
                // accounting can return the typed limit.
                scanner.batch_size(scan_rows);
                scanner.batch_size_bytes(scan_bytes);
                Ok(())
            })
            .await?;

        let mut committed = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            let batch = if let Some(key_col) = key_column
                && !pending_keys.is_empty()
            {
                filter_out_rows_where_string_in(vec![batch], key_col, &pending_keys)?
                    .into_iter()
                    .next()
                    .expect("one committed input batch produces one shadow-filtered batch")
            } else {
                batch
            };
            if batch.num_rows() == 0 {
                continue;
            }
            account.add_batch(&batch)?;
            committed.push(batch);
        }

        committed.extend(pending);
        Ok(committed)
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
        budget: PendingScanBudget,
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
                    budget,
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
        let pending_keys = match key_column {
            Some(key_col) => collect_string_column_values(pending_batches, key_col)?,
            None => std::collections::HashSet::new(),
        };
        let pending = if pending_batches.is_empty() {
            Vec::new()
        } else {
            scan_pending_batches(pending_batches, pending_schema, None, filter).await?
        };
        let mut account = PendingScanAccount::new(budget)?;
        account.add_batches(&pending)?;

        let scan_rows = account.next_scan_rows();
        let scan_bytes = account.next_scan_bytes();
        let mut matched = Self::scan_stream_with(
            committed_ds,
            Some(&non_blob_columns),
            filter,
            None,
            true,
            |scanner| {
                scanner.batch_size(scan_rows);
                scanner.batch_size_bytes(scan_bytes);
                Ok(())
            },
        )
        .await?;

        let mut committed = Vec::new();
        while let Some(batch) = matched
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            // Shadow before row charging and before any blob handle/read. A
            // prior pending row owns the logical id even when it no longer
            // matches this update predicate.
            let batch = if let Some(key_col) = key_column
                && !pending_keys.is_empty()
            {
                filter_out_rows_where_string_in(vec![batch], key_col, &pending_keys)?
                    .into_iter()
                    .next()
                    .expect("one committed input batch produces one shadow-filtered batch")
            } else {
                batch
            };
            if batch.num_rows() == 0 {
                continue;
            }
            account.ensure_additional_rows(batch.num_rows())?;
            // This predicate scan contains every logical non-blob column and
            // `_rowid`, but no blob descriptors/payloads. Charge the logical
            // columns now so an already-overwide match fails before
            // `take_rows` performs the second full-row read.
            let non_blob_bytes = non_blob_column_bytes(committed_ds, &batch)?;
            let payload_budget = account.remaining_bytes_after(non_blob_bytes)?;

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
            let materialized = match Self::materialize_blob_batch_with_row_ids(
                committed_ds,
                descriptors,
                &row_ids,
                Some(payload_budget),
            )
            .await
            {
                Err(OmniError::ResourceLimitExceeded { actual, .. }) => {
                    let actual = account
                        .bytes_with(non_blob_bytes)?
                        .checked_add(actual)
                        .ok_or_else(|| {
                            OmniError::manifest_internal("pending scan byte count overflow")
                        })?;
                    return Err(OmniError::resource_limit(
                        format!("keyed bytes for {}", account.table_key()),
                        KEYED_WRITE_MAX_BYTES,
                        actual,
                    ));
                }
                Err(error) => return Err(error),
                Ok(materialized) => materialized,
            };
            account.add_batch(&materialized)?;
            committed.push(materialized);
        }

        committed.extend(pending);
        Ok(committed)
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
        Self::has_btree_index_on(ds, column).await
    }

    pub(crate) async fn has_btree_index_on(ds: &Dataset, column: &str) -> Result<bool> {
        let indices = Self::user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| details.type_url.ends_with("BTreeIndexDetails"))
                .unwrap_or(false)
        }))
    }

    pub async fn has_fts_index(&self, ds: &Dataset, column: &str) -> Result<bool> {
        Self::has_fts_index_on(ds, column).await
    }

    pub(crate) async fn has_fts_index_on(ds: &Dataset, column: &str) -> Result<bool> {
        let indices = Self::user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| IndexDetails(details.clone()).supports_fts())
                .unwrap_or(false)
        }))
    }

    pub async fn has_vector_index(&self, ds: &Dataset, column: &str) -> Result<bool> {
        Self::has_vector_index_on(ds, column).await
    }

    pub(crate) async fn has_vector_index_on(ds: &Dataset, column: &str) -> Result<bool> {
        let indices = Self::user_indices_for_column(ds, column).await?;
        Ok(indices.iter().any(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| IndexDetails(details.clone()).is_vector())
                .unwrap_or(false)
        }))
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
        Ok(Self::first_row_id_in_batches(&batches))
    }

    /// Exact-`id` point-lookup sibling of `first_row_id_for_filter`, built on a
    /// typed DataFusion predicate instead of flattening the caller's id into
    /// SQL text — ids containing quotes need no escaping, and the exact-`id`
    /// BTREE stays usable when reconciled.
    pub async fn first_row_id_for_id(&self, ds: &Dataset, id: &str) -> Result<Option<u64>> {
        use datafusion::prelude::{col, lit};

        let filter = col("id").eq(lit(id.to_string()));
        let batches = Self::scan_stream_with(ds, Some(&["id"]), None, None, true, |scanner| {
            scanner.filter_expr(filter);
            Ok(())
        })
        .await?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self::first_row_id_in_batches(&batches))
    }

    fn first_row_id_in_batches(batches: &[RecordBatch]) -> Option<u64> {
        batches.iter().find_map(|batch| {
            batch
                .column_by_name("_rowid")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .and_then(|arr| (arr.len() > 0).then(|| arr.value(0)))
        })
    }

    pub async fn write_dataset(dataset_uri: &str, batch: RecordBatch) -> Result<Dataset> {
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let control_session = crate::lance_access::control_session();
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            session: Some(control_session),
            ..Default::default()
        };
        Dataset::write(reader, dataset_uri, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }
}

fn map_lance_commit_error(error: lance::Error) -> OmniError {
    match error {
        error @ (lance::Error::RetryableCommitConflict { .. }
        | lance::Error::TooMuchWriteContention { .. }) => {
            OmniError::RetryableCommitConflict(error.to_string())
        }
        error => OmniError::Lance(error.to_string()),
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

/// Incremental resource accounting for a pending-aware update scan.
///
/// The retained `Vec<RecordBatch>` never grows past this account: pending
/// results are charged first, and each committed scanner emission is charged
/// only after pending-key shadowing. `initial_*` represents batches already
/// retained by `MutationStaging`, so a chained update receives only the
/// remaining transaction budget.
struct PendingScanAccount {
    table_key: String,
    rows: u64,
    bytes: u64,
}

impl PendingScanAccount {
    fn new(budget: PendingScanBudget) -> Result<Self> {
        let mut account = Self {
            table_key: budget.table_key,
            rows: 0,
            bytes: 0,
        };
        account.add_usage(budget.initial_rows, budget.initial_bytes)?;
        Ok(account)
    }

    fn table_key(&self) -> &str {
        &self.table_key
    }

    fn add_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.add_batch(batch)?;
        }
        Ok(())
    }

    fn add_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| OmniError::manifest_internal("pending scan row count exceeds u64"))?;
        let bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| OmniError::manifest_internal("pending scan bytes exceed u64"))?;
        self.add_usage(rows, bytes)
    }

    fn add_usage(&mut self, rows: u64, bytes: u64) -> Result<()> {
        let next_rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| OmniError::manifest_internal("pending scan row count overflow"))?;
        if next_rows > KEYED_WRITE_MAX_ROWS as u64 {
            return Err(OmniError::resource_limit(
                format!("keyed rows for {}", self.table_key),
                KEYED_WRITE_MAX_ROWS as u64,
                next_rows,
            ));
        }
        let next_bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| OmniError::manifest_internal("pending scan byte count overflow"))?;
        if next_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("keyed bytes for {}", self.table_key),
                KEYED_WRITE_MAX_BYTES,
                next_bytes,
            ));
        }
        self.rows = next_rows;
        self.bytes = next_bytes;
        Ok(())
    }

    fn ensure_additional_rows(&self, rows: usize) -> Result<()> {
        let rows = u64::try_from(rows)
            .map_err(|_| OmniError::manifest_internal("pending scan row count exceeds u64"))?;
        let actual = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| OmniError::manifest_internal("pending scan row count overflow"))?;
        if actual > KEYED_WRITE_MAX_ROWS as u64 {
            return Err(OmniError::resource_limit(
                format!("keyed rows for {}", self.table_key),
                KEYED_WRITE_MAX_ROWS as u64,
                actual,
            ));
        }
        Ok(())
    }

    fn bytes_with(&self, bytes: u64) -> Result<u64> {
        self.bytes
            .checked_add(bytes)
            .ok_or_else(|| OmniError::manifest_internal("pending scan byte count overflow"))
    }

    fn remaining_bytes_after(&self, bytes: u64) -> Result<u64> {
        let actual = self.bytes_with(bytes)?;
        if actual > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("keyed bytes for {}", self.table_key),
                KEYED_WRITE_MAX_BYTES,
                actual,
            ));
        }
        Ok(KEYED_WRITE_MAX_BYTES - actual)
    }

    fn next_scan_rows(&self) -> usize {
        let remaining = (KEYED_WRITE_MAX_ROWS as u64).saturating_sub(self.rows);
        usize::try_from(remaining.saturating_add(1).min(KEYED_WRITE_MAX_ROWS as u64))
            .unwrap_or(KEYED_WRITE_MAX_ROWS)
            .max(1)
    }

    fn next_scan_bytes(&self) -> u64 {
        KEYED_WRITE_MAX_BYTES.saturating_sub(self.bytes).max(1)
    }
}

/// Exact Arrow bytes already present in the eventual logical output before
/// blob payload arrays are rebuilt. This lets the blob-size preflight consume
/// only the true remaining table budget before any `BlobFile::read`.
fn non_blob_column_bytes(ds: &Dataset, batch: &RecordBatch) -> Result<u64> {
    ds.schema()
        .fields
        .iter()
        .filter(|field| !field.is_blob())
        .try_fold(0_u64, |total, field| {
            let column = batch.column_by_name(&field.name).ok_or_else(|| {
                OmniError::Lance(format!("batch missing column '{}'", field.name))
            })?;
            let bytes = u64::try_from(column.get_array_memory_size()).map_err(|_| {
                OmniError::manifest_internal("non-blob pending scan bytes exceed u64")
            })?;
            total.checked_add(bytes).ok_or_else(|| {
                OmniError::manifest_internal("non-blob pending scan byte count overflow")
            })
        })
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

/// Validate blob input columns at ingest on reference-retaining writes.
///
/// Two checks, both at the one moment the offending write can still be
/// refused. First, every blob column's input must be the canonical
/// `{data, uri}` logical struct: a create/overwrite persists the batch
/// schema as the table's logical blob shape, and a divergent shape (Lance
/// also accepts a 4-child logical variant and a prepared descriptor
/// variant) knocks every later keyed write off Lance's v2 merge fast path
/// (strict inserts lose the inserted-row key filter; upserts fall into the
/// legacy merger, which cannot decode external-bearing blob fragments).
/// Second, each external URI is probed once (a size call against the
/// external store) so an unreadable reference fails the load loudly
/// instead of deferring the failure to the first read.
///
/// It deliberately records NOTHING into the logical blob input; external
/// descriptors keep Lance's `BlobRange { 0, 0 }` "size unknown" encoding,
/// which the shared descriptor decoder already treats as unknown rather
/// than authoritative.
async fn validate_external_blob_references(batch: &RecordBatch) -> Result<()> {
    let schema = batch.schema();
    let registry = Arc::new(lance::io::ObjectStoreRegistry::default());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let lance_field = lance::datatypes::Field::try_from(field.as_ref())
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !lance_field.is_blob() {
            continue;
        }
        // Boundary assertion for the canonical logical blob input shape.
        // Lance also accepts a 4-child {data, uri, position, size} logical
        // variant and a prepared descriptor variant, but a create/overwrite
        // persists the batch schema as the table's blob shape, and any
        // divergence from {data, uri} knocks every later keyed write off the
        // v2 merge fast path (see keyed_writes_survive_committed_external_
        // blob_reference). Every engine batch builder emits the canonical
        // shape via Lance's BlobArrayBuilder, so a non-canonical shape here
        // is engine code regressing — fail the offending write loudly
        // instead of poisoning the table for every later writer.
        let canonical = matches!(
            field.data_type(),
            arrow_schema::DataType::Struct(children)
                if children.len() == 2
                    && children[0].name() == "data"
                    && children[0].data_type() == &arrow_schema::DataType::LargeBinary
                    && children[1].name() == "uri"
                    && children[1].data_type() == &arrow_schema::DataType::Utf8
        );
        if !canonical {
            return Err(OmniError::manifest_internal(format!(
                "blob column '{}' input must be the canonical {{data: LargeBinary, uri: Utf8}} \
                 struct; got {:?} — a divergent shape would persist into the table schema and \
                 break every later keyed write",
                field.name(),
                field.data_type(),
            )));
        }
        let Some(descriptions) = column.as_any().downcast_ref::<StructArray>() else {
            return Err(OmniError::manifest_internal(format!(
                "blob column '{}' input array is not a StructArray",
                field.name(),
            )));
        };
        let Some(uris) = descriptions
            .column_by_name("uri")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        else {
            return Err(OmniError::manifest_internal(format!(
                "blob column '{}' input struct has no Utf8 'uri' child array",
                field.name(),
            )));
        };
        for row in 0..descriptions.len() {
            if !descriptions.is_valid(row) || !uris.is_valid(row) || uris.value(row).is_empty() {
                continue;
            }
            let uri = uris.value(row);
            let (store, path) = lance::io::ObjectStore::from_uri_and_params(
                registry.clone(),
                uri,
                &lance::io::ObjectStoreParams::default(),
            )
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
            store.size(&path).await.map_err(|error| {
                OmniError::manifest(format!(
                    "external blob URI '{uri}' is not readable at ingest: {error}"
                ))
            })?;
        }
    }
    Ok(())
}

/// Materialize logical external-URI blob cells before keyed merge-insert.
///
/// Lance's keyed builder currently has no `WriteParams` hook for allowing an
/// absolute external reference. Size is resolved and charged before the read,
/// and columns are processed sequentially under one aggregate payload budget.
/// The returned arrays retain the target's logical blob schema, so Lance can
/// still choose inline, packed, or dedicated physical storage normally.
async fn materialize_external_blob_inputs(
    batch: RecordBatch,
    max_payload_bytes: u64,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut materialized_payload_bytes = 0_u64;
    let mut changed = false;
    let registry = Arc::new(lance::io::ObjectStoreRegistry::default());

    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let lance_field = lance::datatypes::Field::try_from(field.as_ref())
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !lance_field.is_blob() {
            columns.push(column.clone());
            continue;
        }
        let descriptions = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                OmniError::Lance(format!("expected blob struct input for '{}'", field.name()))
            })?;
        // Dataset scans can already yield prepared descriptors. Those are
        // handled by the branch-merge materializer before this adapter.
        if descriptions.column_by_name("kind").is_some() {
            columns.push(column.clone());
            continue;
        }
        let uris = descriptions
            .column_by_name("uri")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "logical blob input '{}' is missing Utf8 child 'uri'",
                    field.name()
                ))
            })?;
        if !(0..descriptions.len()).any(|row| {
            descriptions.is_valid(row) && uris.is_valid(row) && !uris.value(row).is_empty()
        }) {
            columns.push(column.clone());
            continue;
        }
        let data = descriptions
            .column_by_name("data")
            .and_then(|array| array.as_any().downcast_ref::<LargeBinaryArray>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "logical blob input '{}' is missing LargeBinary child 'data'",
                    field.name()
                ))
            })?;
        let positions = descriptions
            .column_by_name("position")
            .map(|array| {
                array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                    OmniError::Lance(format!(
                        "logical blob input '{}' has non-UInt64 child 'position'",
                        field.name()
                    ))
                })
            })
            .transpose()?;
        let sizes = descriptions
            .column_by_name("size")
            .map(|array| {
                array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                    OmniError::Lance(format!(
                        "logical blob input '{}' has non-UInt64 child 'size'",
                        field.name()
                    ))
                })
            })
            .transpose()?;

        let mut builder = BlobArrayBuilder::new(descriptions.len());
        for row in 0..descriptions.len() {
            if descriptions.is_null(row) {
                builder
                    .push_null()
                    .map_err(|error| OmniError::Lance(error.to_string()))?;
                continue;
            }
            let has_data = data.is_valid(row);
            let has_uri = uris.is_valid(row) && !uris.value(row).is_empty();
            match (has_data, has_uri) {
                (true, false) => builder
                    .push_bytes(data.value(row))
                    .map_err(|error| OmniError::Lance(error.to_string()))?,
                (false, true) => {
                    let uri = uris.value(row);
                    url::Url::parse(uri).map_err(|_| {
                        OmniError::manifest(format!(
                            "external blob URI '{uri}' is not a valid absolute URI"
                        ))
                    })?;
                    let (store, path) = lance::io::ObjectStore::from_uri_and_params(
                        registry.clone(),
                        uri,
                        &lance::io::ObjectStoreParams::default(),
                    )
                    .await
                    .map_err(|error| OmniError::Lance(error.to_string()))?;
                    let object_size = store
                        .size(&path)
                        .await
                        .map_err(|error| OmniError::Lance(error.to_string()))?;
                    let position =
                        positions.and_then(|array| array.is_valid(row).then(|| array.value(row)));
                    let declared_size =
                        sizes.and_then(|array| array.is_valid(row).then(|| array.value(row)));
                    let (offset, payload_size) = match (position, declared_size) {
                        (Some(offset), Some(size)) => {
                            let end = offset.checked_add(size).ok_or_else(|| {
                                OmniError::manifest("external blob range overflows u64")
                            })?;
                            if end > object_size {
                                return Err(OmniError::manifest(format!(
                                    "external blob range {offset}..{end} exceeds object size {object_size}"
                                )));
                            }
                            (offset, size)
                        }
                        (None, None) => (0, object_size),
                        _ => {
                            return Err(OmniError::manifest(format!(
                                "external blob URI '{uri}' must set both position and size"
                            )));
                        }
                    };
                    materialized_payload_bytes = materialized_payload_bytes
                        .checked_add(payload_size)
                        .ok_or_else(|| {
                            OmniError::manifest_internal("external blob payload byte overflow")
                        })?;
                    if materialized_payload_bytes > max_payload_bytes {
                        return Err(OmniError::resource_limit(
                            "materialized external blob payload bytes",
                            max_payload_bytes,
                            materialized_payload_bytes,
                        ));
                    }
                    let start = usize::try_from(offset).map_err(|_| {
                        OmniError::manifest("external blob offset does not fit usize")
                    })?;
                    let end_u64 = offset
                        .checked_add(payload_size)
                        .ok_or_else(|| OmniError::manifest("external blob range overflows u64"))?;
                    let end = usize::try_from(end_u64).map_err(|_| {
                        OmniError::manifest("external blob range does not fit usize")
                    })?;
                    // Read the exact size proven above. `read_one_all` could
                    // exceed the budget if a mutable external object grows
                    // between HEAD and GET.
                    let bytes = store
                        .read_one_range(&path, start..end)
                        .await
                        .map_err(|error| OmniError::Lance(error.to_string()))?;
                    builder
                        .push_bytes(bytes)
                        .map_err(|error| OmniError::Lance(error.to_string()))?;
                }
                (true, true) => {
                    return Err(OmniError::manifest(format!(
                        "blob '{}' row {row} cannot contain both inline data and a URI",
                        field.name()
                    )));
                }
                (false, false) => {
                    return Err(OmniError::manifest(format!(
                        "non-null blob '{}' row {row} contains neither data nor a URI",
                        field.name()
                    )));
                }
            }
        }
        columns.push(
            builder
                .finish()
                .map_err(|error| OmniError::Lance(error.to_string()))?,
        );
        changed = true;
    }

    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new(schema, columns).map_err(|error| OmniError::Lance(error.to_string()))
}

/// The provenance scanner copies every source blob into a logical in-memory
/// value before the proof-only adapter is selected. Refuse any prepared blob-v2
/// descriptor or remaining URI here: Lance treats prepared descriptors as a
/// passthrough, so `allow_external_blob_outside_bases = false` alone would not
/// stop a future caller from smuggling a source-branch file reference into the
/// target dataset.
fn ensure_proven_insert_blobs_are_materialized(batch: &RecordBatch, table_key: &str) -> Result<()> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let lance_field = lance::datatypes::Field::try_from(field.as_ref())
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if !lance_field.is_blob() {
            continue;
        }
        let descriptions = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "proven insert blob '{}' on {table_key} is not a logical blob struct",
                    field.name()
                ))
            })?;
        if descriptions.column_by_name("kind").is_some() {
            return Err(OmniError::manifest_internal(format!(
                "proven insert blob '{}' on {table_key} retained a prepared descriptor",
                field.name()
            )));
        }
        let data = descriptions
            .column_by_name("data")
            .and_then(|array| array.as_any().downcast_ref::<LargeBinaryArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "proven insert blob '{}' on {table_key} is missing materialized data",
                    field.name()
                ))
            })?;
        let uris = descriptions
            .column_by_name("uri")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "proven insert blob '{}' on {table_key} is missing its logical URI field",
                    field.name()
                ))
            })?;
        for row in 0..descriptions.len() {
            if descriptions.is_null(row) {
                continue;
            }
            if !data.is_valid(row) {
                return Err(OmniError::manifest_internal(format!(
                    "proven insert blob '{}' on {table_key} row {row} has no materialized bytes",
                    field.name()
                )));
            }
            if uris.is_valid(row) && !uris.value(row).is_empty() {
                return Err(OmniError::manifest_internal(format!(
                    "proven insert blob '{}' on {table_key} row {row} still references an external URI",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn exact_id_primary_key_field_id(ds: &Dataset, context: &'static str) -> Result<i32> {
    let primary_key = ds.schema().unenforced_primary_key();
    if primary_key.len() == 1
        && primary_key[0].name == "id"
        && !primary_key[0].nullable
        && primary_key[0].data_type() == arrow_schema::DataType::Utf8
    {
        return Ok(primary_key[0].id);
    }
    let actual: Vec<&str> = primary_key
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    Err(OmniError::manifest_internal(format!(
        "{context}: dataset must declare exactly ['id'] as a non-null Utf8 Lance unenforced \
         primary key, got {actual:?}"
    )))
}

fn schema_preorder_field_ids(ds: &Dataset, context: &'static str) -> Result<Vec<u32>> {
    ds.schema()
        .fields_pre_order()
        .map(|field| {
            u32::try_from(field.id).map_err(|_| {
                OmniError::manifest_internal(format!(
                    "{context}: dataset schema contains negative field id {}",
                    field.id
                ))
            })
        })
        .collect()
}

fn string_id_column<'a>(batch: &'a RecordBatch, context: &'static str) -> Result<&'a StringArray> {
    let column = batch.column_by_name("id").ok_or_else(|| {
        OmniError::manifest_internal(format!("{context}: source batch missing 'id' column"))
    })?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{context}: 'id' column is not Utf8 (got {:?})",
                column.data_type()
            ))
        })
}

fn validate_keyed_write_batch_ids(
    batch: &RecordBatch,
    table_key: &str,
    context: &'static str,
) -> Result<Vec<String>> {
    let ids = string_id_column(batch, context)?;
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    let mut source_ids = Vec::with_capacity(ids.len());
    for row in 0..ids.len() {
        if !ids.is_valid(row) {
            return Err(OmniError::manifest(format!(
                "{context}: source row has a null 'id'"
            )));
        }
        let id = ids.value(row);
        if !seen.insert(id) {
            return Err(OmniError::key_conflict(table_key, id));
        }
        source_ids.push(id.to_string());
    }
    Ok(source_ids)
}

/// Assert the exact RFC-023 substrate proof carried by pinned Lance's v2
/// merge-insert route.  Checking both public copies makes a future Lance
/// refactor that drops the transaction-level conflict filter fail closed.
fn validate_exact_id_filter(
    uncommitted: &UncommittedMergeInsert,
    id_field_id: i32,
    context: &'static str,
) -> Result<()> {
    let transaction_filter =
        transaction_exact_id_filter(&uncommitted.transaction, id_field_id, context)?;
    if uncommitted.inserted_rows_filter.as_ref() != Some(transaction_filter) {
        return Err(OmniError::manifest_internal(format!(
            "{context}: Lance uncommitted result and transaction disagree on the inserted-row key filter"
        )));
    }
    Ok(())
}

fn validate_strict_insert_merge_stats(
    uncommitted: &UncommittedMergeInsert,
    expected_rows: u64,
    context: &'static str,
) -> Result<()> {
    let stats = &uncommitted.stats;
    if !merge_stats_prove_pure_insert(stats, expected_rows) {
        return Err(OmniError::manifest_internal(format!(
            "{context}: strict insert merge stats were inserted={}, updated={}, deleted={}, skipped={}, attempts={}; expected inserted={expected_rows}, updated=0, deleted=0, skipped=0, attempts=1",
            stats.num_inserted_rows,
            stats.num_updated_rows,
            stats.num_deleted_rows,
            stats.num_skipped_duplicates,
            stats.num_attempts,
        )));
    }
    Ok(())
}

fn merge_stats_prove_pure_insert(stats: &MergeStats, expected_rows: u64) -> bool {
    stats.num_inserted_rows == expected_rows
        && stats.num_updated_rows == 0
        && stats.num_deleted_rows == 0
        && stats.num_skipped_duplicates == 0
        && stats.num_attempts == 1
}

fn validate_transaction_exact_id_filter(
    transaction: &Transaction,
    id_field_id: i32,
    context: &'static str,
) -> Result<()> {
    transaction_exact_id_filter(transaction, id_field_id, context).map(|_| ())
}

/// Validate and mint the inductive insertion-absence certificate.
///
/// A strict preflight or an all-new MergeInsert result proves absence at the
/// transaction's pinned parent. This post-stage check binds that proof to the
/// exact transaction that can later be observed in Lance history: it must be a
/// pure insertion-only `Update`, carry an exact-id filter containing precisely
/// every source key, and describe the same number of physical rows. The proven
/// branch-merge adapter invokes the same validator from an inherited history
/// proof. We preserve any unrelated Lance properties.
fn certify_insert_absence(
    transaction: &mut Transaction,
    expected_read_version: u64,
    id_field_id: i32,
    expected_schema_preorder_ids: &[u32],
    source_ids: &[String],
    context: &'static str,
) -> Result<()> {
    if source_ids.is_empty() {
        return Err(OmniError::manifest_internal(format!(
            "{context}: cannot certify an empty strict insert"
        )));
    }

    if transaction.read_version != expected_read_version {
        return Err(OmniError::manifest_internal(format!(
            "{context}: strict insert read version {} does not match pinned parent {expected_read_version}",
            transaction.read_version
        )));
    }

    let (new_fragments, filter) = match &transaction.operation {
        Operation::Update {
            removed_fragment_ids,
            updated_fragments,
            new_fragments,
            fields_modified,
            merged_generations,
            fields_for_preserving_frag_bitmap,
            update_mode,
            inserted_rows_filter,
            updated_fragment_offsets,
            ..
        } if removed_fragment_ids.is_empty()
            && updated_fragments.is_empty()
            && !new_fragments.is_empty()
            && fields_modified.is_empty()
            && merged_generations.is_empty()
            && fields_for_preserving_frag_bitmap == expected_schema_preorder_ids
            && update_mode == &Some(UpdateMode::RewriteRows)
            && updated_fragment_offsets.is_none() =>
        {
            let filter = inserted_rows_filter.as_ref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "{context}: insertion-only effect cannot be certified without an exact-id filter"
                ))
            })?;
            (new_fragments, filter)
        }
        _ => {
            return Err(OmniError::manifest_internal(format!(
                "{context}: strict insert did not stage a pure insertion-only Update"
            )));
        }
    };

    let mut expected_filter = KeyExistenceFilterBuilder::new(vec![id_field_id]);
    for id in source_ids {
        expected_filter
            .insert(KeyValue::String(id.clone()))
            .map_err(|error| OmniError::Lance(error.to_string()))?;
    }
    if expected_filter.len() != source_ids.len()
        || source_ids
            .iter()
            .any(|id| !expected_filter.contains(&KeyValue::String(id.clone())))
        || filter != &expected_filter.build()
    {
        return Err(OmniError::manifest_internal(format!(
            "{context}: strict-insert filter does not encode exactly every source id"
        )));
    }

    let physical_rows = new_fragments.iter().try_fold(0_u64, |rows, fragment| {
        let fragment_rows = fragment.physical_rows.ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{context}: strict-insert fragment is missing physical_rows"
            ))
        })?;
        rows.checked_add(fragment_rows as u64).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{context}: strict-insert physical row count overflow"
            ))
        })
    })?;
    if physical_rows != source_ids.len() as u64 {
        return Err(OmniError::manifest_internal(format!(
            "{context}: strict-insert transaction describes {physical_rows} physical rows for {} source ids",
            source_ids.len()
        )));
    }

    if transaction
        .transaction_properties
        .as_ref()
        .and_then(|properties| properties.get(INSERT_ABSENCE_PROPERTY))
        .is_some_and(|value| value != INSERT_ABSENCE_V1)
    {
        return Err(OmniError::manifest_internal(format!(
            "{context}: insertion-absence certificate property already contains an unsupported value"
        )));
    }
    let properties = transaction
        .transaction_properties
        .get_or_insert_with(|| Arc::new(HashMap::new()));
    Arc::make_mut(properties).insert(
        INSERT_ABSENCE_PROPERTY.to_string(),
        INSERT_ABSENCE_V1.to_string(),
    );
    Ok(())
}

fn transaction_exact_id_filter<'a>(
    transaction: &'a Transaction,
    id_field_id: i32,
    context: &'static str,
) -> Result<&'a lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilter> {
    let transaction_filter = match &transaction.operation {
        Operation::Update {
            inserted_rows_filter,
            ..
        } => inserted_rows_filter.as_ref(),
        other => {
            return Err(OmniError::manifest_internal(format!(
                "{context}: keyed merge produced unexpected Lance operation {:?}",
                std::mem::discriminant(other)
            )));
        }
    };
    let filter = transaction_filter.ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "{context}: exact-id keyed merge did not produce an inserted-row key filter"
        ))
    })?;
    if filter.field_ids != vec![id_field_id] {
        return Err(OmniError::manifest_internal(format!(
            "{context}: keyed merge filter covers field ids {:?}, expected exact id field [{id_field_id}]",
            filter.field_ids
        )));
    }
    Ok(filter)
}

/// Normalize scanner output into the exact per-transaction boundaries accepted
/// by the keyed writer. Lance's byte target is approximate, strict row sizing
/// can be overridden by process configuration, and blob materialization emits
/// one safe row at a time. This layer therefore splits oversized emissions,
/// compacts retained-parent slices, and coalesces small emissions before the
/// recovery planner observes any boundary.
fn bounded_proven_insert_stream(
    schema: SchemaRef,
    raw: SendableRecordBatchStream,
    table_key: String,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let bounded = futures::stream::try_unfold(
        (
            raw,
            None::<RecordBatch>,
            0_usize,
            None::<(RecordBatch, usize)>,
            Vec::<RecordBatch>::new(),
            0_usize,
            0_u64,
            schema,
            table_key,
        ),
        |(
            mut raw,
            mut current,
            mut current_offset,
            mut pending,
            mut accumulated,
            mut accumulated_rows,
            mut accumulated_bytes,
            schema,
            table_key,
        )| async move {
            loop {
                if let Some((batch, consumed_rows)) = pending.take() {
                    let batch_rows = batch.num_rows();
                    let batch_bytes =
                        u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                            datafusion::error::DataFusionError::Execution(
                                "proven insert delta batch bytes exceed u64".to_string(),
                            )
                        })?;
                    let fits = accumulated_rows
                        .checked_add(batch_rows)
                        .is_some_and(|rows| rows <= KEYED_WRITE_MAX_ROWS)
                        && accumulated_bytes
                            .checked_add(batch_bytes)
                            .is_some_and(|bytes| bytes <= KEYED_WRITE_MAX_BYTES);
                    if accumulated.is_empty() || fits {
                        accumulated_rows += batch_rows;
                        accumulated_bytes += batch_bytes;
                        accumulated.push(batch);
                        current_offset += consumed_rows;
                        if current
                            .as_ref()
                            .is_some_and(|batch| current_offset == batch.num_rows())
                        {
                            current = None;
                            current_offset = 0;
                        }
                        if accumulated_rows == KEYED_WRITE_MAX_ROWS
                            || accumulated_bytes == KEYED_WRITE_MAX_BYTES
                        {
                            let output = finish_proven_insert_batch(
                                &schema,
                                std::mem::take(&mut accumulated),
                                &table_key,
                            )
                            .map_err(|error| {
                                datafusion::error::DataFusionError::Execution(error.to_string())
                            })?;
                            return Ok(Some((
                                output,
                                (
                                    raw,
                                    current,
                                    current_offset,
                                    None,
                                    Vec::new(),
                                    0,
                                    0,
                                    schema,
                                    table_key,
                                ),
                            )));
                        }
                        continue;
                    }

                    pending = Some((batch, consumed_rows));
                    let output = finish_proven_insert_batch(
                        &schema,
                        std::mem::take(&mut accumulated),
                        &table_key,
                    )
                    .map_err(|error| {
                        datafusion::error::DataFusionError::Execution(error.to_string())
                    })?;
                    return Ok(Some((
                        output,
                        (
                            raw,
                            current,
                            current_offset,
                            pending,
                            Vec::new(),
                            0,
                            0,
                            schema,
                            table_key,
                        ),
                    )));
                }

                if let Some(batch) = current.as_ref() {
                    if current_offset < batch.num_rows() {
                        pending = Some(
                            next_proven_insert_batch(batch, current_offset, &table_key).map_err(
                                |error| {
                                    datafusion::error::DataFusionError::Execution(error.to_string())
                                },
                            )?,
                        );
                        continue;
                    }
                    current = None;
                    current_offset = 0;
                    continue;
                }

                match raw.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() != 0 {
                            current = Some(batch);
                        }
                    }
                    None if accumulated.is_empty() => return Ok(None),
                    None => {
                        let output = finish_proven_insert_batch(
                            &schema,
                            std::mem::take(&mut accumulated),
                            &table_key,
                        )
                        .map_err(|error| {
                            datafusion::error::DataFusionError::Execution(error.to_string())
                        })?;
                        return Ok(Some((
                            output,
                            (
                                raw,
                                current,
                                current_offset,
                                pending,
                                Vec::new(),
                                0,
                                0,
                                schema,
                                table_key,
                            ),
                        )));
                    }
                }
            }
        },
    );
    Box::pin(RecordBatchStreamAdapter::new(output_schema, bounded))
}

fn observe_proven_insert_raw_stream(raw: SendableRecordBatchStream) -> SendableRecordBatchStream {
    let schema = raw.schema();
    let observed = raw.map_ok(|batch| {
        crate::instrumentation::record_proven_insert_raw_batch(
            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
        );
        batch
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, observed))
}

/// Produce only the next bounded candidate from one scanner emission.
///
/// A scanner batch can be wider than either hard writer boundary, and an Arrow
/// slice can retain every buffer owned by its parent. Copy partial ranges as
/// they are requested so downstream retention keeps only the selected rows.
/// Crucially, the caller holds at most this one copied candidate: it never
/// materializes a queue containing every split of a large source batch.
fn next_proven_insert_batch(
    batch: &RecordBatch,
    offset: usize,
    table_key: &str,
) -> Result<(RecordBatch, usize)> {
    if offset >= batch.num_rows() {
        return Err(OmniError::manifest_internal(format!(
            "proven insert delta offset {offset} is outside {table_key} batch with {} rows",
            batch.num_rows()
        )));
    }

    let max_rows = (batch.num_rows() - offset).min(KEYED_WRITE_MAX_ROWS);
    let (mut rows, mut logical_bytes) = largest_proven_insert_prefix(batch, offset, max_rows)?;
    loop {
        let is_whole_batch = offset == 0 && rows == batch.num_rows();
        let retained_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        // ArrayData's logical slice measure excludes the backing capacity held
        // by an Arrow slice. Preserve an already-compact whole scanner batch,
        // but copy any selected prefix or obvious retained-parent emission.
        let retained_parent_slop = 64_u64 * 1024;
        let keeps_only_logical_slice =
            retained_bytes <= logical_bytes.saturating_add(retained_parent_slop);
        let candidate = if is_whole_batch
            && retained_bytes <= KEYED_WRITE_MAX_BYTES
            && keeps_only_logical_slice
        {
            batch.clone()
        } else {
            copy_proven_insert_batch_range(batch, offset, rows)?
        };
        let bytes = u64::try_from(candidate.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("proven insert delta batch bytes exceed u64")
        })?;
        if bytes <= KEYED_WRITE_MAX_BYTES {
            validate_proven_insert_source_batch(&candidate, table_key)?;
            return Ok((candidate, rows));
        }
        if rows == 1 {
            return Err(OmniError::resource_limit(
                format!("proven insert delta bytes for {table_key}"),
                KEYED_WRITE_MAX_BYTES,
                bytes,
            ));
        }
        drop(candidate);
        rows = ((u128::try_from(rows).unwrap() * u128::from(KEYED_WRITE_MAX_BYTES))
            / u128::from(bytes))
        .try_into()
        .unwrap_or(1_usize)
        .clamp(1, rows - 1);
        logical_bytes = proven_insert_slice_memory_size(batch, offset, rows)?;
    }
}

/// Select the largest row prefix whose logical Arrow buffers fit the byte
/// ceiling without copying it first. Physical `get_array_memory_size` counts a
/// sliced array's complete backing buffers; `ArrayData::get_slice_memory_size`
/// instead approximates the compact buffers for just the selected rows.
fn largest_proven_insert_prefix(
    batch: &RecordBatch,
    offset: usize,
    max_rows: usize,
) -> Result<(usize, u64)> {
    let one_row_bytes = proven_insert_slice_memory_size(batch, offset, 1)?;
    if max_rows == 1 || one_row_bytes > KEYED_WRITE_MAX_BYTES {
        return Ok((1, one_row_bytes));
    }

    let max_bytes = proven_insert_slice_memory_size(batch, offset, max_rows)?;
    if max_bytes <= KEYED_WRITE_MAX_BYTES {
        return Ok((max_rows, max_bytes));
    }

    let mut low = 1_usize;
    let mut high = max_rows - 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if proven_insert_slice_memory_size(batch, offset, middle)? <= KEYED_WRITE_MAX_BYTES {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok((low, proven_insert_slice_memory_size(batch, offset, low)?))
}

fn proven_insert_slice_memory_size(batch: &RecordBatch, offset: usize, rows: usize) -> Result<u64> {
    batch.columns().iter().try_fold(0_u64, |total, column| {
        let bytes = column
            .slice(offset, rows)
            .to_data()
            .get_slice_memory_size()
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        total
            .checked_add(u64::try_from(bytes).map_err(|_| {
                OmniError::manifest_internal("proven insert logical slice bytes exceed u64")
            })?)
            .ok_or_else(|| {
                OmniError::manifest_internal("proven insert logical slice bytes overflow")
            })
    })
}

fn finish_proven_insert_batch(
    schema: &SchemaRef,
    mut batches: Vec<RecordBatch>,
    table_key: &str,
) -> Result<RecordBatch> {
    let batch = if batches.len() == 1 {
        batches.pop().expect("length checked")
    } else {
        arrow_select::concat::concat_batches(schema, &batches)
            .map_err(|error| OmniError::Lance(error.to_string()))?
    };
    validate_proven_insert_source_batch(&batch, table_key)?;
    Ok(batch)
}

fn copy_proven_insert_batch_range(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
) -> Result<RecordBatch> {
    let end = offset
        .checked_add(rows)
        .ok_or_else(|| OmniError::manifest_internal("proven insert delta range overflow"))?;
    let offset = u64::try_from(offset)
        .map_err(|_| OmniError::manifest_internal("proven insert delta offset exceeds u64"))?;
    let end = u64::try_from(end)
        .map_err(|_| OmniError::manifest_internal("proven insert delta end exceeds u64"))?;
    let indices = UInt64Array::from_iter_values(offset..end);
    arrow_select::take::take_record_batch(batch, &indices)
        .map_err(|error| OmniError::Lance(error.to_string()))
}

fn validate_proven_insert_source_batch(batch: &RecordBatch, table_key: &str) -> Result<()> {
    if batch.num_rows() > KEYED_WRITE_MAX_ROWS {
        return Err(OmniError::resource_limit(
            format!("proven insert delta rows for {table_key}"),
            KEYED_WRITE_MAX_ROWS as u64,
            batch.num_rows() as u64,
        ));
    }
    let bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| OmniError::manifest_internal("proven insert delta batch bytes exceed u64"))?;
    if bytes > KEYED_WRITE_MAX_BYTES {
        return Err(OmniError::resource_limit(
            format!("proven insert delta bytes for {table_key}"),
            KEYED_WRITE_MAX_BYTES,
            bytes,
        ));
    }
    Ok(())
}

/// Preserve all conflict metadata Lance returned while exposing the physical
/// fragment delta needed by read-your-writes scans.
fn staged_keyed_merge_result(
    uncommitted: UncommittedMergeInsert,
    context: &'static str,
) -> Result<StagedWrite> {
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
        other => {
            return Err(OmniError::manifest_internal(format!(
                "{context}: keyed merge produced unexpected Lance operation {:?}",
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

#[cfg(test)]
mod staged_tests;
