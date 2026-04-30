use omnigraph::db::{GraphCommit, MergeOutcome, ReadTarget, SchemaApplyResult, Snapshot};
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
pub struct BranchCreateRequest {
    /// Parent branch to fork from. Defaults to `main`.
    pub from: Option<String>,
    /// Name of the new branch. Must not already exist.
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
    /// Source branch whose commits will be merged.
    pub source: String,
    /// Target branch that will receive the merge. Defaults to `main`.
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
    /// Commit creation time as Unix epoch microseconds.
    #[schema(example = 1714000000000000i64)]
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CommitListOutput {
    pub commits: Vec<CommitOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadRequest {
    /// GQ query source. May declare one or more named queries; pick one with
    /// `query_name` if there is more than one.
    #[schema(example = "query get_person($name: String) {\n    match {\n        $p: Person { name: $name }\n    }\n    return { $p.name, $p.age }\n}")]
    pub query_source: String,
    /// Name of the query to run when `query_source` declares multiple. Optional
    /// when only one query is declared.
    pub query_name: Option<String>,
    /// JSON object whose keys match the query's declared parameters.
    pub params: Option<Value>,
    /// Branch to read from. Mutually exclusive with `snapshot`. Defaults to `main`.
    pub branch: Option<String>,
    /// Snapshot id to read from. Mutually exclusive with `branch`.
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeRequest {
    /// GQ mutation source containing `insert`, `update`, or `delete` statements.
    /// May declare multiple named mutations; pick one with `query_name`.
    #[schema(example = "query insert_person($name: String, $age: I32) {\n    insert Person { name: $name, age: $age }\n}")]
    pub query_source: String,
    /// Name of the mutation to run when `query_source` declares multiple.
    pub query_name: Option<String>,
    /// JSON object whose keys match the mutation's declared parameters.
    pub params: Option<Value>,
    /// Target branch. Defaults to `main`.
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SchemaApplyRequest {
    /// Project schema in `.pg` source form. The diff against the current
    /// schema produces the migration steps that will be applied.
    #[schema(example = "node Person {\n    name: String @key\n    age: I32?\n}\n\nedge Knows: Person -> Person")]
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
pub struct SchemaOutput {
    pub schema_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IngestRequest {
    /// Target branch. Created from `from` if it does not yet exist. Defaults to `main`.
    pub branch: Option<String>,
    /// Parent branch used to create `branch` if it does not exist. Defaults to `main`.
    pub from: Option<String>,
    /// How existing rows are handled. Defaults to `merge`.
    #[schema(value_type = Option<LoadModeSchema>)]
    pub mode: Option<LoadMode>,
    /// NDJSON payload: one record per line, each shaped
    /// `{"type": "<TypeName>", "data": {...}}`.
    #[schema(example = "{\"type\": \"Person\", \"data\": {\"name\": \"Alice\", \"age\": 30}}\n{\"type\": \"Person\", \"data\": {\"name\": \"Bob\", \"age\": 25}}")]
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExportRequest {
    /// Branch to export. Defaults to `main`.
    pub branch: Option<String>,
    /// Restrict the export to these node/edge type names. Empty exports all types.
    #[serde(default)]
    pub type_names: Vec<String>,
    /// Restrict the export to these table keys. Empty exports all tables.
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

/// Structured details for a publisher-level OCC failure. Surfaces alongside
/// HTTP 409 when a write was rejected because the caller's pre-write view of
/// one table's manifest version was stale relative to the current head. The
/// expected/actual fields tell the client which table to refresh.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ManifestConflictOutput {
    pub table_key: String,
    pub expected: u64,
    pub actual: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorOutput {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<ErrorCode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merge_conflicts: Vec<MergeConflictOutput>,
    /// Set when the conflict is a publisher CAS rejection
    /// (`ManifestConflictDetails::ExpectedVersionMismatch`). The caller's
    /// pre-write view of `table_key` was at version `expected` but the
    /// manifest is now at `actual`. Refresh and retry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_conflict: Option<ManifestConflictOutput>,
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
