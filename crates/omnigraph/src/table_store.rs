use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream, Scanner};
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::datatypes::BlobHandling;
use lance::index::scalar::IndexDetails;
use lance_file::version::LanceFileVersion;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType, is_system_index};
use lance_linalg::distance::MetricType;
use lance_table::format::IndexMetadata;
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
        if ds.version().version != expected_version {
            return Err(OmniError::manifest_conflict(format!(
                "version drift on {}: snapshot pinned v{} but dataset is at v{} — call sync_branch() and retry",
                table_key,
                expected_version,
                ds.version().version
            )));
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
