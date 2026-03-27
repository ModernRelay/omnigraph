use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

use crate::error::{OmniError, Result};

const GRAPH_RUNS_DIR: &str = "_graph_runs.lance";
pub(crate) const INTERNAL_RUN_BRANCH_PREFIX: &str = "__run__";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(String);

impl RunId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Published,
    Failed,
    Aborted,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Published => "published",
            RunStatus::Failed => "failed",
            RunStatus::Aborted => "aborted",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "published" => Ok(Self::Published),
            "failed" => Ok(Self::Failed),
            "aborted" => Ok(Self::Aborted),
            other => Err(OmniError::Manifest(format!(
                "invalid run status '{}'",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub run_id: RunId,
    pub target_branch: String,
    pub run_branch: String,
    pub base_snapshot_id: String,
    pub base_manifest_version: u64,
    pub operation_hash: Option<String>,
    pub status: RunStatus,
    pub published_snapshot_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl RunRecord {
    pub fn new(
        target_branch: impl Into<String>,
        base_snapshot_id: impl Into<String>,
        base_manifest_version: u64,
        operation_hash: Option<String>,
    ) -> Result<Self> {
        let now = now_micros()?;
        let run_id = RunId::new(ulid::Ulid::new().to_string());
        Ok(Self {
            run_branch: internal_run_branch_name(&run_id),
            run_id,
            target_branch: target_branch.into(),
            base_snapshot_id: base_snapshot_id.into(),
            base_manifest_version,
            operation_hash,
            status: RunStatus::Running,
            published_snapshot_id: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn with_status(
        &self,
        status: RunStatus,
        published_snapshot_id: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            run_id: self.run_id.clone(),
            target_branch: self.target_branch.clone(),
            run_branch: self.run_branch.clone(),
            base_snapshot_id: self.base_snapshot_id.clone(),
            base_manifest_version: self.base_manifest_version,
            operation_hash: self.operation_hash.clone(),
            status,
            published_snapshot_id,
            created_at: self.created_at,
            updated_at: now_micros()?,
        })
    }
}

pub struct RunRegistry {
    dataset: Dataset,
}

impl RunRegistry {
    pub async fn init(root_uri: &str) -> Result<Self> {
        let uri = graph_runs_uri(root_uri);
        let batch = RecordBatch::new_empty(run_registry_schema());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], run_registry_schema());
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, &uri as &str, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self { dataset })
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let dataset = Dataset::open(&graph_runs_uri(root_uri))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self { dataset })
    }

    pub async fn refresh(&mut self, root_uri: &str) -> Result<()> {
        self.dataset = Dataset::open(&graph_runs_uri(root_uri))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(())
    }

    pub async fn append_record(&mut self, record: &RunRecord) -> Result<()> {
        let batch = runs_to_batch(&[record.clone()])?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], run_registry_schema());
        let mut ds = self.dataset.clone();
        ds.append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.dataset = ds;
        Ok(())
    }

    pub async fn get_run(&self, run_id: &RunId) -> Result<Option<RunRecord>> {
        Ok(self
            .load_runs()
            .await?
            .into_iter()
            .find(|record| record.run_id == *run_id))
    }

    pub async fn list_runs(&self) -> Result<Vec<RunRecord>> {
        self.load_runs().await
    }

    pub async fn load_runs(&self) -> Result<Vec<RunRecord>> {
        let batches: Vec<RecordBatch> = self
            .dataset
            .scan()
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let mut latest_by_id: HashMap<String, RunRecord> = HashMap::new();
        for batch in &batches {
            let run_ids = batch
                .column_by_name("run_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let target_branches = batch
                .column_by_name("target_branch")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let run_branches = batch
                .column_by_name("run_branch")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let base_snapshot_ids = batch
                .column_by_name("base_snapshot_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let base_manifest_versions = batch
                .column_by_name("base_manifest_version")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let operation_hashes = batch
                .column_by_name("operation_hash")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let statuses = batch
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let published_snapshot_ids = batch
                .column_by_name("published_snapshot_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let created_ats = batch
                .column_by_name("created_at")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let updated_ats = batch
                .column_by_name("updated_at")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let record = RunRecord {
                    run_id: RunId::new(run_ids.value(row)),
                    target_branch: target_branches.value(row).to_string(),
                    run_branch: run_branches.value(row).to_string(),
                    base_snapshot_id: base_snapshot_ids.value(row).to_string(),
                    base_manifest_version: base_manifest_versions.value(row),
                    operation_hash: if operation_hashes.is_null(row) {
                        None
                    } else {
                        Some(operation_hashes.value(row).to_string())
                    },
                    status: RunStatus::parse(statuses.value(row))?,
                    published_snapshot_id: if published_snapshot_ids.is_null(row) {
                        None
                    } else {
                        Some(published_snapshot_ids.value(row).to_string())
                    },
                    created_at: created_ats.value(row),
                    updated_at: updated_ats.value(row),
                };

                match latest_by_id.get(record.run_id.as_str()) {
                    Some(existing)
                        if existing.updated_at > record.updated_at
                            || (existing.updated_at == record.updated_at
                                && existing.created_at >= record.created_at) => {}
                    _ => {
                        latest_by_id.insert(record.run_id.as_str().to_string(), record);
                    }
                }
            }
        }

        let mut runs = latest_by_id.into_values().collect::<Vec<_>>();
        runs.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(runs)
    }
}

pub(crate) fn is_internal_run_branch(name: &str) -> bool {
    name.trim_start_matches('/')
        .starts_with(INTERNAL_RUN_BRANCH_PREFIX)
}

pub(crate) fn internal_run_branch_name(run_id: &RunId) -> String {
    format!("{}{}", INTERNAL_RUN_BRANCH_PREFIX, run_id.as_str())
}

pub(crate) fn graph_runs_uri(root_uri: &str) -> String {
    format!("{}/{}", root_uri.trim_end_matches('/'), GRAPH_RUNS_DIR)
}

fn run_registry_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("target_branch", DataType::Utf8, false),
        Field::new("run_branch", DataType::Utf8, false),
        Field::new("base_snapshot_id", DataType::Utf8, false),
        Field::new("base_manifest_version", DataType::UInt64, false),
        Field::new("operation_hash", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("published_snapshot_id", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn runs_to_batch(records: &[RunRecord]) -> Result<RecordBatch> {
    let run_ids: Vec<&str> = records
        .iter()
        .map(|record| record.run_id.as_str())
        .collect();
    let target_branches: Vec<&str> = records
        .iter()
        .map(|record| record.target_branch.as_str())
        .collect();
    let run_branches: Vec<&str> = records
        .iter()
        .map(|record| record.run_branch.as_str())
        .collect();
    let base_snapshot_ids: Vec<&str> = records
        .iter()
        .map(|record| record.base_snapshot_id.as_str())
        .collect();
    let base_manifest_versions: Vec<u64> = records
        .iter()
        .map(|record| record.base_manifest_version)
        .collect();
    let operation_hashes: Vec<Option<&str>> = records
        .iter()
        .map(|record| record.operation_hash.as_deref())
        .collect();
    let statuses: Vec<&str> = records
        .iter()
        .map(|record| record.status.as_str())
        .collect();
    let published_snapshot_ids: Vec<Option<&str>> = records
        .iter()
        .map(|record| record.published_snapshot_id.as_deref())
        .collect();
    let created_ats: Vec<i64> = records.iter().map(|record| record.created_at).collect();
    let updated_ats: Vec<i64> = records.iter().map(|record| record.updated_at).collect();

    RecordBatch::try_new(
        run_registry_schema(),
        vec![
            Arc::new(StringArray::from(run_ids)),
            Arc::new(StringArray::from(target_branches)),
            Arc::new(StringArray::from(run_branches)),
            Arc::new(StringArray::from(base_snapshot_ids)),
            Arc::new(UInt64Array::from(base_manifest_versions)),
            Arc::new(StringArray::from(operation_hashes)),
            Arc::new(StringArray::from(statuses)),
            Arc::new(StringArray::from(published_snapshot_ids)),
            Arc::new(TimestampMicrosecondArray::from(created_ats)),
            Arc::new(TimestampMicrosecondArray::from(updated_ats)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

fn now_micros() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| OmniError::Manifest(format!("system clock error: {}", e)))?;
    Ok(duration.as_micros() as i64)
}
