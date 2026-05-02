//! MR-847 Phase 4 — Recovery audit row storage in `_graph_commit_recoveries.lance`.
//!
//! Sibling to `_graph_commits.lance` (`commit_graph.rs`). Each successful
//! recovery sweep — roll-forward or roll-back — records one row here so
//! operators investigating a sidecar-attributed mutation can correlate
//! `omnigraph commit list --filter actor=omnigraph:recovery` with the
//! original actor whose mutation was rolled forward / back.
//!
//! The schema-migration alternative (adding `recovery_for_actor` and
//! `recovery_kind` columns to `_graph_commits.lance` itself) was
//! considered and rejected for MR-847 — see `.context/mr-847-design.md`
//! § "Recovery audit model". Sibling-table is additive, doesn't bump
//! `INTERNAL_MANIFEST_SCHEMA_VERSION`, and can be removed in favor of a
//! schema migration later if the join cost matters.
//!
//! Atomicity caveat: append to `_graph_commit_recoveries.lance` is
//! sequential w.r.t. the `CommitGraph::append_commit` write. A crash
//! between the two leaves an orphan commit-graph row with no audit row.
//! Same shape as the existing `_graph_commits` + `_graph_commit_actors`
//! split; the recovery sweep tolerates it the same way (re-entry sees
//! `NoMovement` for already-restored / already-published tables; the
//! audit append is retried).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};

const RECOVERIES_DIR: &str = "_graph_commit_recoveries.lance";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) enum RecoveryKind {
    RolledForward,
    RolledBack,
}

impl RecoveryKind {
    fn as_str(self) -> &'static str {
        match self {
            RecoveryKind::RolledForward => "RolledForward",
            RecoveryKind::RolledBack => "RolledBack",
        }
    }

    fn parse(s: &str) -> Result<Self> {
        match s {
            "RolledForward" => Ok(RecoveryKind::RolledForward),
            "RolledBack" => Ok(RecoveryKind::RolledBack),
            other => Err(OmniError::manifest_internal(format!(
                "unknown recovery_kind '{}' in _graph_commit_recoveries.lance",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableOutcome {
    pub table_key: String,
    /// For RolledForward: the prior manifest pin (== sidecar.expected_version).
    /// For RolledBack: same.
    pub from_version: u64,
    /// For RolledForward: the new manifest pin (== sidecar.post_commit_pin).
    /// For RolledBack: == sidecar.expected_version (Lance HEAD reverted).
    pub to_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryAuditRecord {
    pub graph_commit_id: String,
    pub recovery_kind: RecoveryKind,
    pub recovery_for_actor: Option<String>,
    pub operation_id: String,
    pub sidecar_writer_kind: String,
    pub per_table_outcomes: Vec<TableOutcome>,
    pub created_at: i64,
}

pub(crate) struct RecoveryAudit {
    root_uri: String,
    dataset: Option<Dataset>,
}

impl RecoveryAudit {
    /// Open the recovery-audit dataset for the repo, or return a handle
    /// with no dataset yet (created on first append). Mirrors the
    /// optional-dataset pattern from `_graph_commit_actors.lance`.
    pub(crate) async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/').to_string();
        let dataset = Dataset::open(&recoveries_uri(&root)).await.ok();
        Ok(Self {
            root_uri: root,
            dataset,
        })
    }

    /// Append one recovery audit record. Lazily initializes the dataset
    /// on first call (idempotent under racy creation via the same
    /// `Dataset already exists` rebound as `_graph_commit_actors.lance`).
    pub(crate) async fn append(&mut self, record: RecoveryAuditRecord) -> Result<()> {
        let batch = recovery_record_to_batch(&record)?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], recoveries_schema());
        let mut dataset = match self.dataset.take() {
            Some(dataset) => dataset,
            None => create_recoveries_dataset(&self.root_uri).await?,
        };
        dataset
            .append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.dataset = Some(dataset);
        Ok(())
    }

    /// Read every recorded recovery (test + audit-CLI surface). Ordered by
    /// `created_at` ascending.
    pub(crate) async fn list(&self) -> Result<Vec<RecoveryAuditRecord>> {
        let dataset = match &self.dataset {
            Some(dataset) => dataset,
            None => return Ok(Vec::new()),
        };
        let batches: Vec<RecordBatch> = dataset
            .scan()
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let mut out = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                out.push(decode_row(&batch, row)?);
            }
        }
        out.sort_by_key(|r| r.created_at);
        Ok(out)
    }
}

fn recoveries_uri(root_uri: &str) -> String {
    format!("{}/{}", root_uri.trim_end_matches('/'), RECOVERIES_DIR)
}

fn recoveries_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("graph_commit_id", DataType::Utf8, false),
        Field::new("recovery_kind", DataType::Utf8, false),
        Field::new("recovery_for_actor", DataType::Utf8, true),
        Field::new("operation_id", DataType::Utf8, false),
        Field::new("sidecar_writer_kind", DataType::Utf8, false),
        // per_table_outcomes is serialized as a JSON string. The audit
        // table is queried infrequently; a JSON column avoids needing
        // a list-of-struct schema, which would make schema evolution
        // (adding fields per outcome) more painful.
        Field::new("per_table_outcomes_json", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

async fn create_recoveries_dataset(root_uri: &str) -> Result<Dataset> {
    let uri = recoveries_uri(root_uri);
    let batch = RecordBatch::new_empty(recoveries_schema());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], recoveries_schema());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    match Dataset::write(reader, &uri as &str, Some(params)).await {
        Ok(dataset) => Ok(dataset),
        Err(err) if err.to_string().contains("Dataset already exists") => Dataset::open(&uri)
            .await
            .map_err(|open_err| OmniError::Lance(open_err.to_string())),
        Err(err) => Err(OmniError::Lance(err.to_string())),
    }
}

fn recovery_record_to_batch(record: &RecoveryAuditRecord) -> Result<RecordBatch> {
    let outcomes_json = serde_json::to_string(&record.per_table_outcomes).map_err(|e| {
        OmniError::manifest_internal(format!(
            "failed to serialize per_table_outcomes for recovery audit: {}",
            e
        ))
    })?;
    RecordBatch::try_new(
        recoveries_schema(),
        vec![
            Arc::new(StringArray::from(vec![record.graph_commit_id.clone()])),
            Arc::new(StringArray::from(vec![record.recovery_kind.as_str()])),
            Arc::new(StringArray::from(vec![record
                .recovery_for_actor
                .clone()])),
            Arc::new(StringArray::from(vec![record.operation_id.clone()])),
            Arc::new(StringArray::from(vec![record.sidecar_writer_kind.clone()])),
            Arc::new(StringArray::from(vec![outcomes_json])),
            Arc::new(TimestampMicrosecondArray::from(vec![record.created_at])),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

fn decode_row(batch: &RecordBatch, row: usize) -> Result<RecoveryAuditRecord> {
    let str_col = |name: &str| -> Result<&StringArray> {
        batch
            .column_by_name(name)
            .ok_or_else(|| OmniError::manifest_internal(format!("missing column '{}' in recovery audit", name)))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OmniError::manifest_internal(format!("column '{}' has wrong type", name)))
    };
    let ts_col = batch
        .column_by_name("created_at")
        .ok_or_else(|| OmniError::manifest_internal("missing 'created_at' column".to_string()))?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal("'created_at' column has wrong type".to_string())
        })?;

    let graph_commit_ids = str_col("graph_commit_id")?;
    let kinds = str_col("recovery_kind")?;
    let for_actors = str_col("recovery_for_actor")?;
    let op_ids = str_col("operation_id")?;
    let writers = str_col("sidecar_writer_kind")?;
    let outcomes_json = str_col("per_table_outcomes_json")?;

    let outcomes: Vec<TableOutcome> =
        serde_json::from_str(outcomes_json.value(row)).map_err(|e| {
            OmniError::manifest_internal(format!(
                "failed to deserialize per_table_outcomes_json from recovery audit: {}",
                e
            ))
        })?;

    Ok(RecoveryAuditRecord {
        graph_commit_id: graph_commit_ids.value(row).to_string(),
        recovery_kind: RecoveryKind::parse(kinds.value(row))?,
        recovery_for_actor: if for_actors.is_null(row) {
            None
        } else {
            Some(for_actors.value(row).to_string())
        },
        operation_id: op_ids.value(row).to_string(),
        sidecar_writer_kind: writers.value(row).to_string(),
        per_table_outcomes: outcomes,
        created_at: ts_col.value(row),
    })
}

pub(crate) fn now_micros() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .map_err(|e| {
            OmniError::manifest_internal(format!("system clock before unix epoch: {}", e))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> RecoveryAuditRecord {
        RecoveryAuditRecord {
            graph_commit_id: "01H000000000000000000000XX".to_string(),
            recovery_kind: RecoveryKind::RolledForward,
            recovery_for_actor: Some("act-alice".to_string()),
            operation_id: "01H000000000000000000000OP".to_string(),
            sidecar_writer_kind: "Mutation".to_string(),
            per_table_outcomes: vec![
                TableOutcome {
                    table_key: "node:Person".to_string(),
                    from_version: 5,
                    to_version: 6,
                },
                TableOutcome {
                    table_key: "edge:Knows".to_string(),
                    from_version: 12,
                    to_version: 13,
                },
            ],
            created_at: 1_700_000_000_000_000,
        }
    }

    #[tokio::test]
    async fn recovery_audit_round_trips_through_lance() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();

        let mut audit = RecoveryAudit::open(root).await.unwrap();
        // Empty repo: list returns empty.
        assert!(audit.list().await.unwrap().is_empty());

        // Append + list.
        let record = sample_record();
        audit.append(record.clone()).await.unwrap();
        let listed = audit.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], record);

        // Append a second record; both visible, sorted by created_at.
        let mut second = sample_record();
        second.graph_commit_id = "01H000000000000000000000YY".to_string();
        second.recovery_kind = RecoveryKind::RolledBack;
        second.recovery_for_actor = None;
        second.created_at = record.created_at + 1;
        audit.append(second.clone()).await.unwrap();

        let listed = audit.list().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0], record);
        assert_eq!(listed[1], second);
    }

    #[tokio::test]
    async fn recovery_audit_persists_across_open_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();

        {
            let mut audit = RecoveryAudit::open(root).await.unwrap();
            audit.append(sample_record()).await.unwrap();
        }

        let audit = RecoveryAudit::open(root).await.unwrap();
        let listed = audit.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], sample_record());
    }

    #[test]
    fn recovery_kind_round_trips_through_string() {
        assert_eq!(
            RecoveryKind::parse("RolledForward").unwrap(),
            RecoveryKind::RolledForward,
        );
        assert_eq!(
            RecoveryKind::parse("RolledBack").unwrap(),
            RecoveryKind::RolledBack,
        );
        assert!(RecoveryKind::parse("Garbage").is_err());
    }
}
