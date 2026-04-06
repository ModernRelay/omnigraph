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
const GRAPH_RUN_ACTORS_DIR: &str = "_graph_run_actors.lance";
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
            other => Err(OmniError::manifest(format!(
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
    pub actor_id: Option<String>,
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
        actor_id: Option<String>,
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
            actor_id,
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
            actor_id: self.actor_id.clone(),
            status,
            published_snapshot_id,
            created_at: self.created_at,
            updated_at: now_micros()?,
        })
    }
}

pub struct RunRegistry {
    dataset: Dataset,
    actor_dataset: Option<Dataset>,
    latest_by_id: HashMap<String, RunRecord>,
    actor_by_run_id: HashMap<String, String>,
    root_uri: String,
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
        let actor_dataset = create_run_actor_dataset(root_uri).await?;
        Ok(Self {
            dataset,
            actor_dataset: Some(actor_dataset),
            latest_by_id: HashMap::new(),
            actor_by_run_id: HashMap::new(),
            root_uri: root_uri.to_string(),
        })
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let dataset = Dataset::open(&graph_runs_uri(root_uri))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let actor_dataset = Dataset::open(&graph_run_actors_uri(root_uri)).await.ok();
        let actor_by_run_id = match &actor_dataset {
            Some(dataset) => load_run_actor_cache(dataset).await?,
            None => HashMap::new(),
        };
        let latest_by_id = load_run_cache(&dataset, &actor_by_run_id).await?;
        Ok(Self {
            dataset,
            actor_dataset,
            latest_by_id,
            actor_by_run_id,
            root_uri: root_uri.to_string(),
        })
    }

    pub async fn refresh(&mut self, root_uri: &str) -> Result<()> {
        self.dataset = Dataset::open(&graph_runs_uri(root_uri))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.actor_dataset = Dataset::open(&graph_run_actors_uri(root_uri)).await.ok();
        self.actor_by_run_id = match &self.actor_dataset {
            Some(dataset) => load_run_actor_cache(dataset).await?,
            None => HashMap::new(),
        };
        self.latest_by_id = load_run_cache(&self.dataset, &self.actor_by_run_id).await?;
        self.root_uri = root_uri.to_string();
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
        if let Some(actor_id) = &record.actor_id {
            self.append_actor(record.run_id.as_str(), actor_id).await?;
        }
        let mut record = record.clone();
        if record.actor_id.is_none() {
            record.actor_id = self.actor_by_run_id.get(record.run_id.as_str()).cloned();
        }
        merge_latest_run(&mut self.latest_by_id, record);
        Ok(())
    }

    pub async fn get_run(&self, run_id: &RunId) -> Result<Option<RunRecord>> {
        Ok(self.latest_by_id.get(run_id.as_str()).cloned())
    }

    pub async fn list_runs(&self) -> Result<Vec<RunRecord>> {
        self.load_runs().await
    }

    pub async fn load_runs(&self) -> Result<Vec<RunRecord>> {
        let mut runs = self.latest_by_id.values().cloned().collect::<Vec<_>>();
        runs.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(runs)
    }

    async fn append_actor(&mut self, run_id: &str, actor_id: &str) -> Result<()> {
        if self
            .actor_by_run_id
            .get(run_id)
            .is_some_and(|existing| existing == actor_id)
        {
            return Ok(());
        }

        let record = RunActorRecord {
            run_id: run_id.to_string(),
            actor_id: actor_id.to_string(),
            created_at: now_micros()?,
        };
        let batch = run_actors_to_batch(&[record])?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], run_actor_schema());
        let mut dataset = match self.actor_dataset.take() {
            Some(dataset) => dataset,
            None => create_run_actor_dataset(&self.root_uri).await?,
        };
        dataset
            .append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.actor_by_run_id
            .insert(run_id.to_string(), actor_id.to_string());
        self.actor_dataset = Some(dataset);
        Ok(())
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

fn graph_run_actors_uri(root_uri: &str) -> String {
    format!(
        "{}/{}",
        root_uri.trim_end_matches('/'),
        GRAPH_RUN_ACTORS_DIR
    )
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

fn run_actor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("actor_id", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

async fn create_run_actor_dataset(root_uri: &str) -> Result<Dataset> {
    let batch = RecordBatch::new_empty(run_actor_schema());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], run_actor_schema());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    Dataset::write(
        reader,
        &graph_run_actors_uri(root_uri) as &str,
        Some(params),
    )
    .await
    .map_err(|e| OmniError::Lance(e.to_string()))
}

async fn load_run_cache(
    dataset: &Dataset,
    actor_by_run_id: &HashMap<String, String>,
) -> Result<HashMap<String, RunRecord>> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut latest_by_id = HashMap::new();
    for mut record in load_runs_from_batches(&batches)? {
        record.actor_id = actor_by_run_id.get(record.run_id.as_str()).cloned();
        merge_latest_run(&mut latest_by_id, record);
    }
    Ok(latest_by_id)
}

async fn load_run_actor_cache(dataset: &Dataset) -> Result<HashMap<String, String>> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut actors = HashMap::new();
    for batch in batches {
        let run_ids = string_column(&batch, "run_id", "run actor registry")?;
        let actor_ids = string_column(&batch, "actor_id", "run actor registry")?;
        for row in 0..batch.num_rows() {
            actors.insert(
                run_ids.value(row).to_string(),
                actor_ids.value(row).to_string(),
            );
        }
    }
    Ok(actors)
}

fn load_runs_from_batches(batches: &[RecordBatch]) -> Result<Vec<RunRecord>> {
    let mut runs = Vec::new();
    for batch in batches {
        let run_ids = string_column(batch, "run_id", "run registry")?;
        let target_branches = string_column(batch, "target_branch", "run registry")?;
        let run_branches = string_column(batch, "run_branch", "run registry")?;
        let base_snapshot_ids = string_column(batch, "base_snapshot_id", "run registry")?;
        let base_manifest_versions = u64_column(batch, "base_manifest_version", "run registry")?;
        let operation_hashes = string_column(batch, "operation_hash", "run registry")?;
        let statuses = string_column(batch, "status", "run registry")?;
        let published_snapshot_ids = string_column(batch, "published_snapshot_id", "run registry")?;
        let created_ats = timestamp_micros_column(batch, "created_at", "run registry")?;
        let updated_ats = timestamp_micros_column(batch, "updated_at", "run registry")?;

        for row in 0..batch.num_rows() {
            runs.push(RunRecord {
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
                actor_id: None,
                status: RunStatus::parse(statuses.value(row))?,
                published_snapshot_id: if published_snapshot_ids.is_null(row) {
                    None
                } else {
                    Some(published_snapshot_ids.value(row).to_string())
                },
                created_at: created_ats.value(row),
                updated_at: updated_ats.value(row),
            });
        }
    }
    Ok(runs)
}

fn merge_latest_run(latest_by_id: &mut HashMap<String, RunRecord>, record: RunRecord) {
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

fn string_column<'a>(batch: &'a RecordBatch, name: &str, context: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("{context} batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("{context} column '{name}' is not Utf8"))
        })
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str, context: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("{context} batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("{context} column '{name}' is not UInt64"))
        })
}

fn timestamp_micros_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    context: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("{context} batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{context} column '{name}' is not Timestamp(Microsecond)"
            ))
        })
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunActorRecord {
    run_id: String,
    actor_id: String,
    created_at: i64,
}

fn run_actors_to_batch(records: &[RunActorRecord]) -> Result<RecordBatch> {
    let run_ids: Vec<&str> = records
        .iter()
        .map(|record| record.run_id.as_str())
        .collect();
    let actor_ids: Vec<&str> = records
        .iter()
        .map(|record| record.actor_id.as_str())
        .collect();
    let created_ats: Vec<i64> = records.iter().map(|record| record.created_at).collect();

    RecordBatch::try_new(
        run_actor_schema(),
        vec![
            Arc::new(StringArray::from(run_ids)),
            Arc::new(StringArray::from(actor_ids)),
            Arc::new(TimestampMicrosecondArray::from(created_ats)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

fn now_micros() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| OmniError::manifest(format!("system clock error: {}", e)))?;
    Ok(duration.as_micros() as i64)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn load_runs_from_batches_returns_error_for_bad_schema() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("run_id", DataType::UInt64, false),
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
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["__run__1"])),
                Arc::new(StringArray::from(vec!["snap-1"])),
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["running"])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_i64])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_i64])),
            ],
        )
        .unwrap();

        let err = load_runs_from_batches(&[batch]).unwrap_err();
        assert!(err.to_string().contains("run_id"));
    }
}
