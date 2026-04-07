use omnigraph::db::{GraphCommit, MergeOutcome, ReadTarget, RunRecord, Snapshot};
use omnigraph::error::{MergeConflict, MergeConflictKind};
use omnigraph_compiler::result::QueryResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTableOutput {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotOutput {
    pub branch: String,
    pub manifest_version: u64,
    pub tables: Vec<SnapshotTableOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunListOutput {
    pub runs: Vec<RunOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchCreateRequest {
    pub from: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchCreateOutput {
    pub uri: String,
    pub from: String,
    pub name: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchListOutput {
    pub branches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMergeRequest {
    pub source: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMergeOutput {
    pub source: String,
    pub target: String,
    pub outcome: BranchMergeOutcome,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadTargetOutput {
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOutput {
    pub query_name: String,
    pub target: ReadTargetOutput,
    pub row_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    pub rows: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeOutput {
    pub branch: String,
    pub query_name: String,
    pub affected_nodes: usize,
    pub affected_edges: usize,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOutput {
    pub graph_commit_id: String,
    pub manifest_branch: Option<String>,
    pub manifest_version: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitListOutput {
    pub commits: Vec<CommitOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequest {
    pub branch: Option<String>,
    #[serde(default)]
    pub type_names: Vec<String>,
    #[serde(default)]
    pub table_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotQuery {
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommitListQuery {
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthOutput {
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    BadRequest,
    NotFound,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
