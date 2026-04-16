use omnigraph::db::{
    GraphCommit, MergeOutcome, ReadTarget, RunRecord, SchemaApplyResult, Snapshot,
};
use omnigraph::error::{MergeConflict, MergeConflictKind};
use omnigraph::loader::{IngestResult, LoadMode};
use omnigraph_compiler::SchemaMigrationStep;
use omnigraph_compiler::result::QueryResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

/// Shadow enum for documenting [`LoadMode`] in the OpenAPI schema.
#[derive(ToSchema)]
#[schema(as = LoadMode)]
#[allow(dead_code)]
enum LoadModeSchema {
    /// Overwrite existing data.
    #[schema(rename = "overwrite")]
    Overwrite,
    /// Append to existing data.
    #[schema(rename = "append")]
    Append,
    /// Merge by id key (upsert).
    #[schema(rename = "merge")]
    Merge,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotTableOutput {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotOutput {
    pub branch: String,
    pub manifest_version: u64,
    pub tables: Vec<SnapshotTableOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunOutput {
    pub run_id: String,
    pub target_branch: String,
    pub run_branch: String,
    pub base_snapshot_id: String,
    pub base_manifest_version: u64,
    pub operation_hash: Option<String>,
    pub actor_id: Option<String>,
    pub status: String,
    pub published_snapshot_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunListOutput {
    pub runs: Vec<RunOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchCreateRequest {
    pub from: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchCreateOutput {
    pub uri: String,
    pub from: String,
    pub name: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchListOutput {
    pub branches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchDeleteOutput {
    pub uri: String,
    pub name: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchMergeRequest {
    pub source: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BranchMergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

impl From<MergeOutcome> for BranchMergeOutcome {
    fn from(value: MergeOutcome) -> Self {
        match value {
            MergeOutcome::AlreadyUpToDate => Self::AlreadyUpToDate,
            MergeOutcome::FastForward => Self::FastForward,
            MergeOutcome::Merged => Self::Merged,
        }
    }
}

impl BranchMergeOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyUpToDate => "already_up_to_date",
            Self::FastForward => "fast_forward",
            Self::Merged => "merged",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BranchMergeOutput {
    pub source: String,
    pub target: String,
    pub outcome: BranchMergeOutcome,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MergeConflictKindOutput {
    DivergentInsert,
    DivergentUpdate,
    DeleteVsUpdate,
    OrphanEdge,
    UniqueViolation,
    CardinalityViolation,
    ValueConstraintViolation,
}

impl MergeConflictKindOutput {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DivergentInsert => "divergent_insert",
            Self::DivergentUpdate => "divergent_update",
            Self::DeleteVsUpdate => "delete_vs_update",
            Self::OrphanEdge => "orphan_edge",
            Self::UniqueViolation => "unique_violation",
            Self::CardinalityViolation => "cardinality_violation",
            Self::ValueConstraintViolation => "value_constraint_violation",
        }
    }
}

impl From<MergeConflictKind> for MergeConflictKindOutput {
    fn from(value: MergeConflictKind) -> Self {
        match value {
            MergeConflictKind::DivergentInsert => Self::DivergentInsert,
            MergeConflictKind::DivergentUpdate => Self::DivergentUpdate,
            MergeConflictKind::DeleteVsUpdate => Self::DeleteVsUpdate,
            MergeConflictKind::OrphanEdge => Self::OrphanEdge,
            MergeConflictKind::UniqueViolation => Self::UniqueViolation,
            MergeConflictKind::CardinalityViolation => Self::CardinalityViolation,
            MergeConflictKind::ValueConstraintViolation => Self::ValueConstraintViolation,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MergeConflictOutput {
    pub table_key: String,
    pub row_id: Option<String>,
    pub kind: MergeConflictKindOutput,
    pub message: String,
}

impl From<&MergeConflict> for MergeConflictOutput {
    fn from(value: &MergeConflict) -> Self {
        Self {
            table_key: value.table_key.clone(),
            row_id: value.row_id.clone(),
            kind: value.kind.into(),
            message: value.message.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadTargetOutput {
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadOutput {
    pub query_name: String,
    pub target: ReadTargetOutput,
    pub row_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    pub rows: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeOutput {
    pub branch: String,
    pub query_name: String,
    pub affected_nodes: usize,
    pub affected_edges: usize,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IngestTableOutput {
    pub table_key: String,
    pub rows_loaded: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IngestOutput {
    pub uri: String,
    pub branch: String,
    pub base_branch: String,
    pub branch_created: bool,
    #[schema(value_type = LoadModeSchema)]
    pub mode: LoadMode,
    pub tables: Vec<IngestTableOutput>,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CommitOutput {
    pub graph_commit_id: String,
    pub manifest_branch: Option<String>,
    pub manifest_version: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CommitListOutput {
    pub commits: Vec<CommitOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SchemaApplyRequest {
    pub schema_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SchemaApplyOutput {
    pub uri: String,
    pub supported: bool,
    pub applied: bool,
    pub step_count: usize,
    pub manifest_version: u64,
    #[schema(value_type = Vec<Value>)]
    pub steps: Vec<SchemaMigrationStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SchemaGetOutput {
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IngestRequest {
    pub branch: Option<String>,
    pub from: Option<String>,
    #[schema(value_type = Option<LoadModeSchema>)]
    pub mode: Option<LoadMode>,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExportRequest {
    pub branch: Option<String>,
    #[serde(default)]
    pub type_names: Vec<String>,
    #[serde(default)]
    pub table_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct SnapshotQuery {
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct CommitListQuery {
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthOutput {
    pub status: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    BadRequest,
    NotFound,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorOutput {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<ErrorCode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merge_conflicts: Vec<MergeConflictOutput>,
}

pub fn snapshot_payload(branch: &str, snapshot: &Snapshot) -> SnapshotOutput {
    let mut entries: Vec<_> = snapshot.entries().cloned().collect();
    entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));
    let tables = entries
        .iter()
        .map(|entry| SnapshotTableOutput {
            table_key: entry.table_key.clone(),
            table_path: entry.table_path.clone(),
            table_version: entry.table_version,
            table_branch: entry.table_branch.clone(),
            row_count: entry.row_count,
        })
        .collect::<Vec<_>>();
    SnapshotOutput {
        branch: branch.to_string(),
        manifest_version: snapshot.version(),
        tables,
    }
}

pub fn schema_apply_output(uri: &str, result: SchemaApplyResult) -> SchemaApplyOutput {
    SchemaApplyOutput {
        uri: uri.to_string(),
        supported: result.supported,
        applied: result.applied,
        step_count: result.steps.len(),
        manifest_version: result.manifest_version,
        steps: result.steps,
    }
}

pub fn run_output(run: &RunRecord) -> RunOutput {
    RunOutput {
        run_id: run.run_id.as_str().to_string(),
        target_branch: run.target_branch.clone(),
        run_branch: run.run_branch.clone(),
        base_snapshot_id: run.base_snapshot_id.as_str().to_string(),
        base_manifest_version: run.base_manifest_version,
        operation_hash: run.operation_hash.clone(),
        actor_id: run.actor_id.clone(),
        status: run.status.as_str().to_string(),
        published_snapshot_id: run.published_snapshot_id.clone(),
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

pub fn commit_output(commit: &GraphCommit) -> CommitOutput {
    CommitOutput {
        graph_commit_id: commit.graph_commit_id.clone(),
        manifest_branch: commit.manifest_branch.clone(),
        manifest_version: commit.manifest_version,
        parent_commit_id: commit.parent_commit_id.clone(),
        merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
        actor_id: commit.actor_id.clone(),
        created_at: commit.created_at,
    }
}

pub fn read_output(query_name: String, target: &ReadTarget, result: QueryResult) -> ReadOutput {
    let columns = result
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    ReadOutput {
        query_name,
        target: read_target_output(target),
        row_count: result.num_rows(),
        columns,
        rows: result.to_rust_json(),
    }
}

pub fn ingest_output(uri: &str, result: &IngestResult, actor_id: Option<String>) -> IngestOutput {
    IngestOutput {
        uri: uri.to_string(),
        branch: result.branch.clone(),
        base_branch: result.base_branch.clone(),
        branch_created: result.branch_created,
        mode: result.mode,
        tables: result
            .tables
            .iter()
            .map(|table| IngestTableOutput {
                table_key: table.table_key.clone(),
                rows_loaded: table.rows_loaded,
            })
            .collect(),
        actor_id,
    }
}

pub fn read_target_output(target: &ReadTarget) -> ReadTargetOutput {
    match target {
        ReadTarget::Branch(branch) => ReadTargetOutput {
            branch: Some(branch.clone()),
            snapshot: None,
        },
        ReadTarget::Snapshot(snapshot) => ReadTargetOutput {
            branch: None,
            snapshot: Some(snapshot.as_str().to_string()),
        },
    }
}
