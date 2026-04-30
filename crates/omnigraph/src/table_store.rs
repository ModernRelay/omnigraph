use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream, Scanner};
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{
    CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode,
    WriteParams,
};
use lance::datatypes::BlobHandling;
use lance::index::scalar::IndexDetails;
use lance_file::version::LanceFileVersion;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType, is_system_index};
use lance_linalg::distance::MetricType;
use lance_table::format::{Fragment, IndexMetadata, RowIdMeta};
use lance_table::rowids::{RowIdSequence, write_row_ids};
use std::sync::Arc;

use crate::db::manifest::{TableVersionMetadata, open_table_head_for_write};
use crate::db::{Snapshot, SubTableEntry};
use crate::error::{OmniError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    pub version: u64,
    pub row_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteState {
    pub version: u64,
    pub row_count: u64,
    pub deleted_rows: usize,
    pub(crate) version_metadata: TableVersionMetadata,
}

/// A Lance write that has produced fragment files on object storage but is
/// not yet committed to the dataset's manifest. The staged-write primitives
/// are defined here for later integration in `MutationStaging`
/// (`exec/mutation.rs`) and the loader (`loader/mod.rs`) — those rewires
/// land in [MR-794](https://linear.app/modernrelay/issue/MR-794) step 2+.
/// The intent: defer Lance commits to end-of-query so a mid-query failure
/// leaves the touched table at the pre-mutation HEAD instead of drifting
/// ahead.
///
/// `transaction` is opaque from our side — Lance owns its semantics. We
/// commit it via `CommitBuilder::execute(transaction)` (see
/// `TableStore::commit_staged`).
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
    pub transaction: Transaction,
    /// Fragments to surface alongside the committed manifest in
    /// `Scanner::with_fragments(committed - removed + new)`. For
    /// `Operation::Append` these are the freshly-appended fragments. For
    /// `Operation::Update` (merge_insert) these are
    /// `updated_fragments + new_fragments` (rewrites + freshly-inserted
    /// rows).
    pub new_fragments: Vec<Fragment>,
    /// Fragment IDs that this staged write supersedes. The committed
    /// manifest must filter these out before being combined with
    /// `new_fragments` for read-your-writes scans, otherwise rewrites
    /// yield duplicate rows. Empty for `stage_append` (`Operation::Append`
    /// adds without removing anything); populated from
    /// `Operation::Update.removed_fragment_ids` for `stage_merge_insert`.
    pub removed_fragment_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct TableStore {
    root_uri: String,
}

impl TableStore {
    pub fn new(root_uri: &str) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
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
        entry.open(&self.root_uri).await
    }

    pub async fn open_dataset_head(
        &self,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<Dataset> {
        let ds = Dataset::open(dataset_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        match branch {
            Some(branch) if branch != "main" => ds
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string())),
            _ => Ok(ds),
        }
    }

    pub async fn open_dataset_head_for_write(
        &self,
        table_key: &str,
        dataset_uri: &str,
        branch: Option<&str>,
    ) -> Result<Dataset> {
        let table_path = self.table_path_from_dataset_uri(dataset_uri)?;
        open_table_head_for_write(&self.root_uri, table_key, &table_path, branch).await
    }

    pub async fn delete_branch(&self, dataset_uri: &str, branch: &str) -> Result<()> {
        let mut ds = Dataset::open(dataset_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        ds.delete_branch(branch)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn open_dataset_at_state(
        &self,
        table_path: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<Dataset> {
        let ds = self
            .open_dataset_head(&self.dataset_uri(table_path), branch)
            .await?;
        ds.checkout_version(version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
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
        let ds = self
            .open_dataset_head_for_write(table_key, dataset_uri, branch)
            .await?;
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
    ) -> Result<Dataset> {
        let mut source_ds = self
            .open_dataset_head(dataset_uri, source_branch)
            .await?
            .checkout_version(source_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.ensure_expected_version(&source_ds, table_key, source_version)?;

        match source_ds
            .create_branch(target_branch, source_version, None)
            .await
        {
            Ok(_) => {}
            Err(create_err) => match self
                .open_dataset_head(dataset_uri, Some(target_branch))
                .await
            {
                Ok(ds) => {
                    self.ensure_expected_version(&ds, table_key, source_version)?;
                    return Ok(ds);
                }
                Err(_) => return Err(OmniError::Lance(create_err.to_string())),
            },
        }

        let ds = self
            .open_dataset_head(dataset_uri, Some(target_branch))
            .await?;
        self.ensure_expected_version(&ds, table_key, source_version)?;
        Ok(ds)
    }

    pub async fn scan_batches(&self, ds: &Dataset) -> Result<Vec<RecordBatch>> {
        self.scan(ds, None, None, None).await
    }

    pub async fn scan_batches_for_rewrite(&self, ds: &Dataset) -> Result<Vec<RecordBatch>> {
        let has_blob_columns = ds.schema().fields_pre_order().any(|field| field.is_blob());
        if !has_blob_columns {
            return self.scan_batches(ds).await;
        }

        let mut scanner = ds.scan();
        scanner.blob_handling(BlobHandling::AllBinary);
        scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
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

    pub async fn append_batch(
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
                    ..Default::default()
                };
                Dataset::write(reader, dataset_uri, Some(params))
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))
            }
        }
    }

    pub async fn overwrite_batch(
        &self,
        dataset_uri: &str,
        ds: &mut Dataset,
        batch: RecordBatch,
    ) -> Result<TableState> {
        ds.truncate_table()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.append_batch(dataset_uri, ds, batch).await
    }

    pub async fn overwrite_dataset(dataset_uri: &str, batch: RecordBatch) -> Result<Dataset> {
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            allow_external_blob_outside_bases: true,
            ..Default::default()
        };
        Dataset::write(reader, dataset_uri, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn merge_insert_batch(
        &self,
        dataset_uri: &str,
        ds: Dataset,
        batch: RecordBatch,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<TableState> {
        if batch.num_rows() == 0 {
            return self.table_state(dataset_uri, &ds).await;
        }

        // TODO(lance-upstream): MergeInsertBuilder does not accept WriteParams,
        // so allow_external_blob_outside_bases cannot be set here. External URI
        // blobs via merge_insert (LoadMode::Merge, mutations) are unsupported
        // until Lance exposes WriteParams on MergeInsertBuilder.
        let ds = Arc::new(ds);
        let job = MergeInsertBuilder::try_new(ds, key_columns)
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .when_matched(when_matched)
            .when_not_matched(when_not_matched)
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let (new_ds, _stats) = job
            .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.table_state(dataset_uri, &new_ds).await
    }

    pub async fn merge_insert_batches(
        &self,
        dataset_uri: &str,
        ds: Dataset,
        batches: Vec<RecordBatch>,
        key_columns: Vec<String>,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<TableState> {
        if batches.is_empty() {
            return self.table_state(dataset_uri, &ds).await;
        }
        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let schema = batches[0].schema();
            concat_batches(&schema, &batches).map_err(|e| OmniError::Lance(e.to_string()))?
        };
        self.merge_insert_batch(
            dataset_uri,
            ds,
            batch,
            key_columns,
            when_matched,
            when_not_matched,
        )
        .await
    }

    pub async fn delete_where(
        &self,
        dataset_uri: &str,
        ds: &mut Dataset,
        filter: &str,
    ) -> Result<DeleteState> {
        let delete_result = ds
            .delete(filter)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(DeleteState {
            version: delete_result.new_dataset.version().version,
            row_count: self.count_rows(&delete_result.new_dataset, None).await? as u64,
            deleted_rows: delete_result.num_deleted_rows as usize,
            version_metadata: self
                .dataset_version_metadata(dataset_uri, &delete_result.new_dataset)?,
        })
    }

    // ─── Staged-write API (MR-794) ───────────────────────────────────────────
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
        let params = WriteParams {
            mode: WriteMode::Append,
            allow_external_blob_outside_bases: true,
            ..Default::default()
        };
        let transaction = InsertBuilder::new(Arc::new(ds.clone()))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
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
        // returns fragments with `id = 0` ("Temporary ID" — see lance-4.0.0
        // `dataset/write.rs:1044/1712`); the real assignment happens during
        // commit via `Transaction::fragments_with_ids`. Because we expose
        // these fragments to `scan_with_staged` *before* commit, two staged
        // fragments (or one staged + the seed) would collide on `id = 0`,
        // causing Lance's scanner to mishandle the combined list (silent
        // duplicates / dropped rows). Mirror the commit-time renumbering
        // here, using `ds.manifest.max_fragment_id() + 1` as the base and
        // accounting for prior stages.
        let next_id_base = ds.manifest.max_fragment_id.unwrap_or(0)
            + 1
            + prior_stages_fragment_count(prior_stages);
        assign_fragment_ids(&mut new_fragments, next_id_base);
        if ds.manifest.uses_stable_row_ids() {
            let prior_rows = prior_stages_row_count(prior_stages)?;
            let start_row_id = ds.manifest.next_row_id + prior_rows;
            assign_row_id_meta(&mut new_fragments, start_row_id)?;
        }
        Ok(StagedWrite {
            transaction,
            new_fragments,
            // Append never supersedes existing fragments.
            removed_fragment_ids: Vec::new(),
        })
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
    /// engine's mutation path enforces the rule "per touched table: all
    /// stage_append OR exactly one stage_merge_insert" at parse time
    /// (the D₂′ check landing with [MR-794](https://linear.app/modernrelay/issue/MR-794)
    /// step 2+ in `exec/mutation.rs`). Multi-table queries and append-chains
    /// remain safe; only chained merges on a single table are rejected.
    ///
    /// Lift path: either a Lance API extension that lets
    /// `MergeInsertBuilder` accept additional staged fragments, or an
    /// in-memory pre-merge here that folds prior staged batches into the
    /// input stream. See `docs/runs.md` and MR-793.
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
        let ds = Arc::new(ds);
        let job = MergeInsertBuilder::try_new(ds, key_columns)
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .when_matched(when_matched)
            .when_not_matched(when_not_matched)
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
        let uncommitted = job
            .execute_uncommitted(stream)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
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
        Ok(StagedWrite {
            transaction: uncommitted.transaction,
            new_fragments,
            removed_fragment_ids,
        })
    }

    /// Commit a previously-staged transaction onto `ds`, returning the new
    /// dataset (with HEAD advanced). Wraps `CommitBuilder::execute`. Used by
    /// the publisher at end-of-query to materialize all staged writes before
    /// the meta-manifest commit.
    pub async fn commit_staged(
        &self,
        ds: Arc<Dataset>,
        transaction: Transaction,
    ) -> Result<Dataset> {
        CommitBuilder::new(ds)
            .execute(transaction)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
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

    pub async fn create_btree_index(&self, ds: &mut Dataset, columns: &[&str]) -> Result<()> {
        let params = ScalarIndexParams::default();
        ds.create_index_builder(columns, IndexType::BTree, &params)
            .replace(true)
            .await
            .map(|_| ())
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn create_inverted_index(&self, ds: &mut Dataset, column: &str) -> Result<()> {
        let params = InvertedIndexParams::default();
        ds.create_index_builder(&[column], IndexType::Inverted, &params)
            .replace(true)
            .await
            .map(|_| ())
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    pub async fn create_vector_index(&self, ds: &mut Dataset, column: &str) -> Result<()> {
        let params = lance::index::vector::VectorIndexParams::ivf_flat(1, MetricType::L2);
        ds.create_index_builder(&[column], IndexType::Vector, &params)
            .replace(true)
            .await
            .map(|_| ())
            .map_err(|e| OmniError::Lance(e.to_string()))
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
/// commit-time `Transaction::fragments_with_ids` (lance-4.0.0
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
/// `Transaction::assign_row_ids` (lance-4.0.0 `dataset/transaction.rs:2682`)
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
