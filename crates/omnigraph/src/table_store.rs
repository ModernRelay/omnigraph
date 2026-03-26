use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::ColumnOrdering;

use crate::db::{Snapshot, SubTableEntry};
use crate::error::{OmniError, Result};

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

    pub async fn open_snapshot_table(
        &self,
        snapshot: &Snapshot,
        table_key: &str,
    ) -> Result<Dataset> {
        snapshot.open(table_key).await
    }

    pub async fn open_at_entry(&self, entry: &SubTableEntry) -> Result<Dataset> {
        self.open_dataset_at_state(
            &entry.table_path,
            entry.table_branch.as_deref(),
            entry.table_version,
        )
        .await
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

    pub async fn scan_batches(&self, ds: &Dataset) -> Result<Vec<RecordBatch>> {
        self.scan(ds, None, None, None).await
    }

    pub async fn scan(
        &self,
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
    ) -> Result<Vec<RecordBatch>> {
        let mut scanner = ds.scan();
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
        scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
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
}
