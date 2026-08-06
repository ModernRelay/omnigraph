//! Shared HTTP wire DTOs (RFC-009 Phase 2) — moved from
//! omnigraph-server's api module so server and CLI share one definition
//! and one engine-result -> DTO mapping per verb. Plain serde/utoipa
//! types; no transport, no server internals.

use omnigraph::db::{
    GraphCommit, GraphStreamDeclaration, GraphStreamDeclarationStatus,
    GraphStreamDriverErrorStatus, GraphStreamDriverStatus, GraphStreamEnsureIndicesResult,
    GraphStreamOperationalStatus, GraphStreamOptimizeResult, GraphStreamPendingStatus,
    GraphStreamRebuildBlocker, GraphStreamRebuildStatus, GraphStreamResumeResult,
    GraphStreamTokenCounts, MergeOutcome, ReadTarget, SchemaApplyResult, Snapshot,
};
use omnigraph::error::{MergeConflict, MergeConflictKind};
use omnigraph::loader::{LoadMode, LoadResult};
use omnigraph_compiler::SchemaMigrationStep;
use omnigraph_compiler::query::ast::Param;
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::types::{PropType, ScalarType};
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
    /// The on-disk internal-schema (storage-format) version this graph's branch
    /// is stamped at.
    pub internal_schema_version: u32,
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
    /// Delete the source branch after a successful merge. The deletion runs
    /// under its own `branch_delete` policy check; a refusal or failure is
    /// reported via `branch_deleted` / `branch_delete_error` on the response
    /// and never fails the already-landed merge.
    #[serde(default)]
    pub delete_branch: bool,
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
    /// Result of the requested post-merge source-branch deletion. Absent when
    /// `delete_branch` was not requested; `true` when the source branch was
    /// deleted; `false` when the deletion was refused or failed (the merge
    /// itself still succeeded — see `branch_delete_error`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_deleted: Option<bool>,
    /// Why the requested source-branch deletion did not happen. Present iff
    /// `branch_deleted` is `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_delete_error: Option<String>,
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
    /// Base branch a fork was requested from (the request's `from`), echoed
    /// even when the branch already existed. `null` when `from` was absent.
    pub base_branch: Option<String>,
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
    #[schema(
        example = "query get_person($name: String) {\n    match {\n        $p: Person { name: $name }\n    }\n    return { $p.name, $p.age }\n}"
    )]
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

/// Inline read-query request for `POST /query`.
///
/// Friendlier-named alternative to [`ReadRequest`] for ad-hoc reads and
/// AI-agent integration. Mutations are rejected with 400 — use `POST
/// /mutate` (or its deprecated alias `POST /change`) for write queries.
/// Field names are deliberately short (`query`, `name`) to match the GQ
/// keyword and the CLI `-e` flag.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// GQ read-query source. May declare one or more named queries; pick one
    /// with `name` when more than one is declared. Mutations
    /// (`insert`/`update`/`delete`) get 400 — use `POST /mutate` (or its
    /// deprecated alias `POST /change`) instead.
    #[schema(
        example = "query get_person($name: String) {\n    match {\n        $p: Person { name: $name }\n    }\n    return { $p.name, $p.age }\n}"
    )]
    pub query: String,
    /// Name of the query to run when `query` declares multiple. Optional when
    /// only one query is declared.
    pub name: Option<String>,
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
    /// May declare multiple named mutations; pick one with `name`.
    ///
    /// Accepts the legacy field name `query_source` as a deserialization alias.
    #[schema(
        example = "query insert_person($name: String, $age: I32) {\n    insert Person { name: $name, age: $age }\n}"
    )]
    #[serde(alias = "query_source")]
    pub query: String,
    /// Name of the mutation to run when `query` declares multiple.
    ///
    /// Accepts the legacy field name `query_name` as a deserialization alias.
    #[serde(default, alias = "query_name")]
    pub name: Option<String>,
    /// JSON object whose keys match the mutation's declared parameters.
    #[serde(default)]
    pub params: Option<Value>,
    /// Target branch. Defaults to `main`.
    #[serde(default)]
    pub branch: Option<String>,
}

/// Body for `POST /queries/{name}` — invokes the server-side stored query
/// named in the path. The query source and name come from the registry,
/// never the body; only the runtime inputs are supplied here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct InvokeStoredQueryRequest {
    /// JSON object whose keys match the stored query's declared parameters.
    #[serde(default)]
    pub params: Option<Value>,
    /// Branch to run against. Defaults to `main`; for a stored mutation the
    /// write targets this branch.
    #[serde(default)]
    pub branch: Option<String>,
    /// Snapshot id to read from (read queries only — rejected for a stored
    /// mutation). Mutually exclusive with `branch`.
    #[serde(default)]
    pub snapshot: Option<String>,
    /// The kind the caller expects: `Some(false)` for
    /// `omnigraph query <name>`, `Some(true)` for `omnigraph mutate <name>`.
    /// When set and it disagrees with the stored query's actual kind, the
    /// server rejects the call (400) so the verb asserts the kind. `None`
    /// (the default) skips the check — preserving older clients and aliases.
    #[serde(default)]
    pub expect_mutation: Option<bool>,
}

/// Response for `POST /queries/{name}`: the read envelope for a stored
/// read, or the mutation envelope for a stored mutation. Serialized
/// **untagged**, so the wire shape is exactly [`ReadOutput`] or
/// [`ChangeOutput`] — classification follows the stored query, not a
/// wrapper field.
#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum InvokeStoredQueryResponse {
    Read(ReadOutput),
    Change(ChangeOutput),
}

/// The kind of a stored-query parameter, decomposed so a client (e.g. an
/// MCP server) can build a typed input schema with a closed `match` and
/// never re-parse omnigraph's type spelling. `bigint`/`date`/`datetime`/
/// `blob` are carried as JSON strings on the wire: a 64-bit integer past
/// 2^53 loses precision as a JSON number, and Date/DateTime are ISO
/// strings, Blob a blob-URI string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParamKind {
    String,
    Bool,
    Int,
    #[serde(rename = "bigint")]
    BigInt,
    Float,
    Date,
    #[serde(rename = "datetime")]
    DateTime,
    Blob,
    Vector,
    List,
}

/// One declared parameter of a stored query, projected for the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ParamDescriptor {
    pub name: String,
    pub kind: ParamKind,
    /// Element kind when `kind == list` (always a scalar — the grammar
    /// forbids lists of vectors or nested lists).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_kind: Option<ParamKind>,
    /// Dimension when `kind == vector`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_dim: Option<u32>,
    /// `false` → the caller must supply it; `true` → optional.
    pub nullable: bool,
}

/// One entry in the stored-query catalog (`GET /queries`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryCatalogEntry {
    /// Registry key / invoke path segment (`POST /queries/{name}`).
    pub name: String,
    /// MCP tool id (the `tool_name` override, else `name`).
    pub tool_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    /// `true` for a stored mutation → an MCP read-only hint of `false`.
    pub mutation: bool,
    pub params: Vec<ParamDescriptor>,
}

/// Response for `GET /queries`: every stored query in a graph's
/// registry, each with typed parameters.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueriesCatalogOutput {
    pub queries: Vec<QueryCatalogEntry>,
}

/// Total map from a resolved scalar to its catalog kind. Exhaustive on
/// purpose: a new `ScalarType` is a compile error here until catalogued.
fn scalar_kind(scalar: ScalarType) -> ParamKind {
    match scalar {
        ScalarType::String => ParamKind::String,
        ScalarType::Bool => ParamKind::Bool,
        ScalarType::I32 | ScalarType::U32 => ParamKind::Int,
        ScalarType::I64 | ScalarType::U64 => ParamKind::BigInt,
        ScalarType::F32 | ScalarType::F64 => ParamKind::Float,
        ScalarType::Date => ParamKind::Date,
        ScalarType::DateTime => ParamKind::DateTime,
        ScalarType::Blob => ParamKind::Blob,
        ScalarType::Vector(_) => ParamKind::Vector,
    }
}

pub fn param_descriptor(param: &Param) -> ParamDescriptor {
    match PropType::from_param_type_name(&param.type_name, param.nullable) {
        Some(pt) if pt.list => ParamDescriptor {
            name: param.name.clone(),
            kind: ParamKind::List,
            item_kind: Some(scalar_kind(pt.scalar)),
            vector_dim: None,
            nullable: param.nullable,
        },
        Some(pt) => {
            let (kind, vector_dim) = match pt.scalar {
                ScalarType::Vector(dim) => (ParamKind::Vector, Some(dim)),
                other => (scalar_kind(other), None),
            };
            ParamDescriptor {
                name: param.name.clone(),
                kind,
                item_kind: None,
                vector_dim,
                nullable: param.nullable,
            }
        }
        // Unreachable for a parsed query (every declared param type is
        // grammatical); fall back to an opaque string so the field is still
        // usable rather than dropped.
        None => ParamDescriptor {
            name: param.name.clone(),
            kind: ParamKind::String,
            item_kind: None,
            vector_dim: None,
            nullable: param.nullable,
        },
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SchemaApplyRequest {
    /// Project schema in `.pg` source form. The diff against the current
    /// schema produces the migration steps that will be applied.
    #[schema(
        example = "node Person {\n    name: String @key\n    age: I32?\n}\n\nedge Knows: Person -> Person"
    )]
    pub schema_source: String,
    /// When true, promote every `DropMode::Soft` step in the plan to
    /// `DropMode::Hard`, making the prior column data unreachable
    /// after the apply. Matches the CLI's `--allow-data-loss` flag.
    /// Defaults to `false` (drops remain reversible via time travel).
    #[serde(default)]
    pub allow_data_loss: bool,
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
    /// Target branch. Defaults to `main`. Without `from`, the branch must
    /// already exist — a missing branch is a 404, never an implicit fork.
    pub branch: Option<String>,
    /// Parent branch used to create `branch` if it does not exist. Branch
    /// creation is opt-in by presence of this field; omit it to require an
    /// existing branch.
    pub from: Option<String>,
    /// How existing rows are handled. Defaults to `merge`.
    #[schema(value_type = Option<LoadModeSchema>)]
    pub mode: Option<LoadMode>,
    /// NDJSON payload: one record per line, each shaped
    /// `{"type": "<TypeName>", "data": {...}}`.
    #[schema(
        example = "{\"type\": \"Person\", \"data\": {\"name\": \"Alice\", \"age\": 30}}\n{\"type\": \"Person\", \"data\": {\"name\": \"Bob\", \"age\": 25}}"
    )]
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

/// Effect-free precondition challenge for graph-native streaming ingest.
///
/// The same value is returned as a strong `ETag` response header. Clients
/// retry the request with that tag in `If-Match`; this convenience copy keeps
/// the graph authority token distinct from the per-row sequencing
/// [`StreamIngestLineOutput::stream_token`].
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StreamIngestChallenge {
    pub graph_token: String,
}

/// Logical declaration kind selected by one graph-native stream row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamIngestKindOutput {
    Node,
    Edge,
}

/// Whether one stream result applies to a single row or blocks the graph-wide
/// remainder of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamIngestScopeOutput {
    Row,
    Graph,
}

/// Stable, graph-logical status vocabulary for one streaming-ingest line.
///
/// Several private physical-authority transitions intentionally collapse to
/// `stream_authority_changed`; the transport never exposes lane, binding,
/// shard, epoch, generation, dataset, or recovery-sidecar identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamIngestStatusOutput {
    Durable,
    AckUnknown,
    AlreadyDurable,
    Withdrawn,
    DeadLettered,
    Invalid,
    StreamInputTooLarge,
    StreamAuthorityChanged,
    StreamSequenceConflict,
    StreamIdempotencyConflict,
    StreamFoldRequired,
    StreamBackpressure,
    RecoveryRequired,
    StreamRetryRequired,
}

/// One ordered, newline-delimited result from graph-native streaming ingest.
///
/// Every field is caller-logical or directly actionable retry evidence.
/// Physical table and MemWAL identities are deliberately absent.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StreamIngestLineOutput {
    pub ordinal: u64,
    pub status: StreamIngestStatusOutput,
    pub scope: StreamIngestScopeOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<StreamIngestKindOutput>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_id: Option<String>,
    /// Confirmed per-row sequencing token. This is not the graph-level ETag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_token: Option<String>,
    /// Candidate token whose durability acknowledgement is unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unconfirmed_candidate_token: Option<String>,
    /// Current per-row token returned with a sequencing or terminal outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocking_ordinal: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocking_status: Option<StreamIngestStatusOutput>,
}

/// Stable graph-level streaming profile state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamProfileModeOutput {
    Disabled,
    Enabled,
    Disabling,
    Retired,
}

/// Logical schema declaration with streaming state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamDeclarationOutput {
    pub kind: StreamIngestKindOutput,
    #[serde(rename = "type")]
    pub type_name: String,
}

/// Stable graph-level lifecycle state for one logical declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamLifecycleOutput {
    Open,
    Draining,
    Sealed,
}

/// Active drain operation for one logical declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamDrainStatusOutput {
    pub goal: String,
    pub phase: String,
    pub initiated_at: i64,
}

/// Current strict validation block for one logical declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamStrictBlockStatusOutput {
    pub kind: String,
    pub violation_code: String,
}

/// Most recent durable fold summary, with physical operation and generation
/// coordinates removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamLastFoldStatusOutput {
    pub outcome: String,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub visible_rows: u64,
    pub visible_bytes: u64,
    pub recorded_at: i64,
}

/// Pending acknowledged work for one logical declaration.
///
/// Counts are returned only when the checked status cut can observe them
/// without claiming a writer or advancing replay state. An unavailable result
/// names only the graph-safe reason classes; it never exposes a shard,
/// generation, dataset, or recovery operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StreamPendingStatusOutput {
    Exact {
        rows: u64,
        arrow_bytes: u64,
        batches: u64,
    },
    Unavailable {
        cold_replay: bool,
        flushed: bool,
        recovery: bool,
    },
}

/// Checked status for one logical node or edge declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamDeclarationStatusOutput {
    #[serde(flatten)]
    pub declaration: StreamDeclarationOutput,
    pub lifecycle: StreamLifecycleOutput,
    pub lifecycle_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain: Option<StreamDrainStatusOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict_block: Option<StreamStrictBlockStatusOutput>,
    pub pending: StreamPendingStatusOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fold: Option<StreamLastFoldStatusOutput>,
}

/// Current graph-wide sequencing-authority counts. Generic status deliberately
/// omits the sampled logical IDs and per-key stream tokens used by the
/// internal proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamTokenCountsOutput {
    pub present: u64,
    pub withdrawn: u64,
    pub dead_lettered: u64,
}

/// Process-local fold-driver run state. Driver health is advisory and never
/// substitutes for durable lifecycle or recovery authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamDriverStateOutput {
    Stopped,
    Running,
    Stopping,
    Failed,
}

/// Redacted most-recent driver error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamDriverErrorOutput {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_in_ms: Option<u64>,
}

/// Advisory health of the fold driver serving this graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamDriverStatusOutput {
    pub scope: String,
    pub authoritative: bool,
    pub state: StreamDriverStateOutput,
    pub pending_count: u64,
    pub published_open_folds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_completion_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<StreamDriverErrorOutput>,
}

/// One graph-safe reason an export/import rebuild is not currently allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum StreamRebuildBlockerOutput {
    ProfileNotTerminal,
    DeclarationNotSealed {
        #[serde(flatten)]
        declaration: StreamDeclarationOutput,
    },
    StrictBlock {
        #[serde(flatten)]
        declaration: StreamDeclarationOutput,
    },
    PendingWork {
        #[serde(flatten)]
        declaration: StreamDeclarationOutput,
    },
    PendingWorkUnavailable {
        #[serde(flatten)]
        declaration: StreamDeclarationOutput,
    },
    RecoveryPending {
        count: u64,
    },
    TerminalTokenAuthority {
        withdrawn_count: u64,
        dead_lettered_count: u64,
    },
}

/// Whether the checked graph cut can be rebuilt without discarding streaming
/// sequencing authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamRebuildStatusOutput {
    pub ready: bool,
    pub blockers: Vec<StreamRebuildBlockerOutput>,
}

/// One coherent, graph-redacted operational status cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamStatusOutput {
    pub manifest_version: u64,
    pub profile_mode: StreamProfileModeOutput,
    pub profile_revision: u64,
    /// Logical node/edge declarations whose streaming state has initialized.
    /// Absence here does not mean the graph schema has no declarations.
    pub enrolled_declarations: Vec<StreamDeclarationStatusOutput>,
    pub token_counts: StreamTokenCountsOutput,
    pub recovery_pending_count: u64,
    pub driver: StreamDriverStatusOutput,
    pub rebuild: StreamRebuildStatusOutput,
}

/// Aggregate result of reopening every sealed streaming declaration in a
/// graph. Declaration, table, lane, dataset, and recovery identities are
/// deliberately absent from this graph-level control-plane shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamResumeOutput {
    pub profile_revision: u64,
    pub enrolled_declarations: u64,
    pub resumed_declarations: u64,
    pub already_open_declarations: u64,
}

/// Aggregate result of graph-wide checked index refresh. Any enrolled
/// declaration changed by the operation is required to be sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamEnsureIndicesOutput {
    pub changed: bool,
    pub pending_index_count: u64,
}

/// Aggregate result of graph-wide checked stream optimization. Any enrolled
/// declaration changed by the operation is required to be sealed. Physical
/// fragment and dataset details stay inside the engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamOptimizeOutput {
    pub changed: bool,
    pub pending_index_count: u64,
    pub requires_repair: bool,
}

pub fn stream_resume_output(value: GraphStreamResumeResult) -> StreamResumeOutput {
    StreamResumeOutput {
        profile_revision: value.profile_revision,
        enrolled_declarations: value.enrolled_declarations,
        resumed_declarations: value.resumed_declarations,
        already_open_declarations: value.already_open_declarations,
    }
}

pub fn stream_ensure_indices_output(
    value: GraphStreamEnsureIndicesResult,
) -> StreamEnsureIndicesOutput {
    StreamEnsureIndicesOutput {
        changed: value.changed,
        pending_index_count: value.pending_index_count,
    }
}

pub fn stream_optimize_output(value: GraphStreamOptimizeResult) -> StreamOptimizeOutput {
    StreamOptimizeOutput {
        changed: value.changed,
        pending_index_count: value.pending_index_count,
        requires_repair: value.requires_repair,
    }
}

fn stream_profile_mode_output(
    value: &str,
) -> std::result::Result<StreamProfileModeOutput, &'static str> {
    match value {
        "DISABLED" => Ok(StreamProfileModeOutput::Disabled),
        "ENABLED" => Ok(StreamProfileModeOutput::Enabled),
        "DISABLING" => Ok(StreamProfileModeOutput::Disabling),
        "RETIRED" => Ok(StreamProfileModeOutput::Retired),
        _ => Err("unknown stream profile mode"),
    }
}

fn stream_declaration_output(
    value: GraphStreamDeclaration,
) -> std::result::Result<StreamDeclarationOutput, &'static str> {
    let kind = match value.kind {
        "node" => StreamIngestKindOutput::Node,
        "edge" => StreamIngestKindOutput::Edge,
        _ => return Err("unknown stream declaration kind"),
    };
    Ok(StreamDeclarationOutput {
        kind,
        type_name: value.type_name,
    })
}

fn stream_lifecycle_output(
    value: &str,
) -> std::result::Result<StreamLifecycleOutput, &'static str> {
    match value {
        "OPEN" => Ok(StreamLifecycleOutput::Open),
        "DRAINING" => Ok(StreamLifecycleOutput::Draining),
        "SEALED" => Ok(StreamLifecycleOutput::Sealed),
        _ => Err("unknown stream lifecycle"),
    }
}

fn stream_declaration_status_output(
    value: GraphStreamDeclarationStatus,
) -> std::result::Result<StreamDeclarationStatusOutput, &'static str> {
    let declaration = stream_declaration_output(value.declaration)?;
    let lifecycle = stream_lifecycle_output(value.lifecycle)?;
    let drain = value.drain.map(|drain| StreamDrainStatusOutput {
        goal: drain.goal.to_ascii_lowercase(),
        phase: drain.phase.to_ascii_lowercase(),
        initiated_at: drain.initiated_at,
    });
    let strict_block = value
        .strict_block
        .map(|block| StreamStrictBlockStatusOutput {
            kind: block.kind.to_ascii_lowercase(),
            violation_code: block.violation_code,
        });
    let pending = match value.pending {
        GraphStreamPendingStatus::Exact {
            rows,
            arrow_bytes,
            batches,
        } => StreamPendingStatusOutput::Exact {
            rows,
            arrow_bytes,
            batches,
        },
        GraphStreamPendingStatus::Unavailable {
            cold_replay,
            flushed,
            recovery,
        } => StreamPendingStatusOutput::Unavailable {
            cold_replay,
            flushed,
            recovery,
        },
    };
    let last_fold = value.last_fold.map(|fold| StreamLastFoldStatusOutput {
        outcome: fold.outcome.to_ascii_lowercase(),
        input_rows: fold.input_rows,
        input_bytes: fold.input_bytes,
        visible_rows: fold.visible_rows,
        visible_bytes: fold.visible_bytes,
        recorded_at: fold.recorded_at,
    });
    Ok(StreamDeclarationStatusOutput {
        declaration,
        lifecycle,
        lifecycle_revision: value.lifecycle_revision,
        drain,
        strict_block,
        pending,
        last_fold,
    })
}

fn stream_driver_status_output(
    value: GraphStreamDriverStatus,
) -> std::result::Result<StreamDriverStatusOutput, &'static str> {
    let state = match value.state {
        "STOPPED" => StreamDriverStateOutput::Stopped,
        "RUNNING" => StreamDriverStateOutput::Running,
        "STOPPING" => StreamDriverStateOutput::Stopping,
        "FAILED" => StreamDriverStateOutput::Failed,
        _ => return Err("unknown stream driver state"),
    };
    let last_error = value
        .last_error
        .map(
            |GraphStreamDriverErrorStatus { kind, retry_in_ms }| StreamDriverErrorOutput {
                kind: kind.to_ascii_lowercase(),
                retry_in_ms,
            },
        );
    Ok(StreamDriverStatusOutput {
        scope: value.scope.to_ascii_lowercase(),
        authoritative: value.authoritative,
        state,
        pending_count: value.pending_count,
        published_open_folds: value.published_open_folds,
        last_completion_kind: value
            .last_completion_kind
            .map(|kind| kind.to_ascii_lowercase()),
        last_error,
    })
}

fn stream_rebuild_blocker_output(
    value: GraphStreamRebuildBlocker,
) -> std::result::Result<StreamRebuildBlockerOutput, &'static str> {
    Ok(match value {
        GraphStreamRebuildBlocker::ProfileNotTerminal => {
            StreamRebuildBlockerOutput::ProfileNotTerminal
        }
        GraphStreamRebuildBlocker::DeclarationNotSealed { declaration } => {
            StreamRebuildBlockerOutput::DeclarationNotSealed {
                declaration: stream_declaration_output(declaration)?,
            }
        }
        GraphStreamRebuildBlocker::StrictBlock { declaration } => {
            StreamRebuildBlockerOutput::StrictBlock {
                declaration: stream_declaration_output(declaration)?,
            }
        }
        GraphStreamRebuildBlocker::PendingWork { declaration } => {
            StreamRebuildBlockerOutput::PendingWork {
                declaration: stream_declaration_output(declaration)?,
            }
        }
        GraphStreamRebuildBlocker::PendingWorkUnavailable { declaration } => {
            StreamRebuildBlockerOutput::PendingWorkUnavailable {
                declaration: stream_declaration_output(declaration)?,
            }
        }
        GraphStreamRebuildBlocker::RecoveryPending { count } => {
            StreamRebuildBlockerOutput::RecoveryPending { count }
        }
        GraphStreamRebuildBlocker::TerminalTokenAuthority {
            withdrawn_count,
            dead_lettered_count,
        } => StreamRebuildBlockerOutput::TerminalTokenAuthority {
            withdrawn_count,
            dead_lettered_count,
        },
    })
}

fn stream_rebuild_status_output(
    value: GraphStreamRebuildStatus,
) -> std::result::Result<StreamRebuildStatusOutput, &'static str> {
    Ok(StreamRebuildStatusOutput {
        ready: value.ready,
        blockers: value
            .blockers
            .into_iter()
            .map(stream_rebuild_blocker_output)
            .collect::<std::result::Result<Vec<_>, _>>()?,
    })
}

/// Convert the engine's graph-redacted checked cut into the stable HTTP/CLI
/// shape. This mapping is deliberately explicit: adding a physical field to
/// the engine bridge cannot make it appear on the wire by accident.
pub fn stream_status_output(
    value: GraphStreamOperationalStatus,
) -> std::result::Result<StreamStatusOutput, &'static str> {
    let enrolled_declarations = value
        .enrolled_declarations
        .into_iter()
        .map(stream_declaration_status_output)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let GraphStreamTokenCounts {
        present,
        withdrawn,
        dead_lettered,
    } = value.token_counts;
    Ok(StreamStatusOutput {
        manifest_version: value.manifest_version,
        profile_mode: stream_profile_mode_output(value.profile_mode)?,
        profile_revision: value.profile_revision,
        enrolled_declarations,
        token_counts: StreamTokenCountsOutput {
            present,
            withdrawn,
            dead_lettered,
        },
        recovery_pending_count: value.recovery_pending_count,
        driver: stream_driver_status_output(value.driver)?,
        rebuild: stream_rebuild_status_output(value.rebuild)?,
    })
}

#[cfg(test)]
mod stream_status_tests {
    use omnigraph::db::{
        GraphStreamDrainStatus, GraphStreamLastFoldStatus, GraphStreamStrictBlockStatus,
    };

    use super::*;

    fn complete_graph_status(profile_mode: &'static str) -> GraphStreamOperationalStatus {
        let declaration = GraphStreamDeclaration {
            kind: "edge",
            type_name: "Knows".to_string(),
        };
        GraphStreamOperationalStatus {
            manifest_version: 21,
            profile_mode,
            profile_revision: 8,
            enrolled_declarations: vec![GraphStreamDeclarationStatus {
                declaration: declaration.clone(),
                lifecycle: "DRAINING",
                lifecycle_revision: 13,
                drain: Some(GraphStreamDrainStatus {
                    goal: "SEALED",
                    phase: "FOLDING",
                    initiated_at: 1_700_000_000,
                }),
                strict_block: Some(GraphStreamStrictBlockStatus {
                    kind: "DATA_BLOCK",
                    violation_code: "OG-CARD-MIN".to_string(),
                }),
                pending: GraphStreamPendingStatus::Unavailable {
                    cold_replay: true,
                    flushed: true,
                    recovery: true,
                },
                last_fold: Some(GraphStreamLastFoldStatus {
                    outcome: "PUBLISHED".to_string(),
                    input_rows: 9,
                    input_bytes: 90,
                    visible_rows: 7,
                    visible_bytes: 70,
                    recorded_at: 1_700_000_010,
                }),
            }],
            token_counts: GraphStreamTokenCounts {
                present: 3,
                withdrawn: 2,
                dead_lettered: 1,
            },
            recovery_pending_count: 4,
            driver: GraphStreamDriverStatus {
                scope: "GRAPH",
                authoritative: false,
                state: "FAILED",
                pending_count: 5,
                published_open_folds: 6,
                last_completion_kind: Some("FOLD_PUBLISHED"),
                last_error: Some(GraphStreamDriverErrorStatus {
                    kind: "RETRYABLE",
                    retry_in_ms: Some(250),
                }),
            },
            rebuild: GraphStreamRebuildStatus {
                ready: false,
                blockers: vec![
                    GraphStreamRebuildBlocker::ProfileNotTerminal,
                    GraphStreamRebuildBlocker::DeclarationNotSealed {
                        declaration: declaration.clone(),
                    },
                    GraphStreamRebuildBlocker::StrictBlock {
                        declaration: declaration.clone(),
                    },
                    GraphStreamRebuildBlocker::PendingWork {
                        declaration: declaration.clone(),
                    },
                    GraphStreamRebuildBlocker::PendingWorkUnavailable { declaration },
                    GraphStreamRebuildBlocker::RecoveryPending { count: 4 },
                    GraphStreamRebuildBlocker::TerminalTokenAuthority {
                        withdrawn_count: 2,
                        dead_lettered_count: 1,
                    },
                ],
            },
        }
    }

    #[test]
    fn complete_graph_status_maps_without_physical_identity() {
        let output = stream_status_output(complete_graph_status("DISABLING")).unwrap();
        assert_eq!(output.profile_mode, StreamProfileModeOutput::Disabling);
        assert_eq!(output.enrolled_declarations.len(), 1);
        let declaration = &output.enrolled_declarations[0];
        assert_eq!(declaration.declaration.kind, StreamIngestKindOutput::Edge);
        assert_eq!(declaration.declaration.type_name, "Knows");
        assert_eq!(declaration.lifecycle, StreamLifecycleOutput::Draining);
        assert_eq!(declaration.drain.as_ref().unwrap().phase, "folding");
        assert_eq!(
            declaration.strict_block.as_ref().unwrap().violation_code,
            "OG-CARD-MIN"
        );
        assert!(matches!(
            declaration.pending,
            StreamPendingStatusOutput::Unavailable {
                cold_replay: true,
                flushed: true,
                recovery: true,
            }
        ));
        assert_eq!(declaration.last_fold.as_ref().unwrap().visible_rows, 7);
        assert_eq!(output.token_counts.dead_lettered, 1);
        assert_eq!(output.recovery_pending_count, 4);
        assert_eq!(output.driver.state, StreamDriverStateOutput::Failed);
        assert_eq!(
            output.driver.last_error.as_ref().unwrap().retry_in_ms,
            Some(250)
        );
        assert_eq!(output.rebuild.blockers.len(), 7);
        assert!(output.rebuild.blockers.iter().any(|blocker| matches!(
            blocker,
            StreamRebuildBlockerOutput::TerminalTokenAuthority {
                withdrawn_count: 2,
                dead_lettered_count: 1,
            }
        )));

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("enrolled_declarations"));
        for forbidden in [
            "table_key",
            "table_id",
            "dataset",
            "binding",
            "shard",
            "epoch",
            "generation",
            "lance",
            "receipt",
            "operation_id",
            "recovery_id",
        ] {
            assert!(
                !json.contains(forbidden),
                "wire status leaked private vocabulary: {forbidden}"
            );
        }
    }

    #[test]
    fn every_current_profile_mode_maps_explicitly() {
        for (raw, expected) in [
            ("DISABLED", StreamProfileModeOutput::Disabled),
            ("ENABLED", StreamProfileModeOutput::Enabled),
            ("DISABLING", StreamProfileModeOutput::Disabling),
            ("RETIRED", StreamProfileModeOutput::Retired),
        ] {
            assert_eq!(
                stream_status_output(complete_graph_status(raw))
                    .unwrap()
                    .profile_mode,
                expected
            );
        }
    }

    #[test]
    fn unknown_driver_state_fails_closed() {
        let status = GraphStreamOperationalStatus {
            manifest_version: 1,
            profile_mode: "ENABLED",
            profile_revision: 2,
            enrolled_declarations: Vec::new(),
            token_counts: GraphStreamTokenCounts {
                present: 0,
                withdrawn: 0,
                dead_lettered: 0,
            },
            recovery_pending_count: 0,
            driver: GraphStreamDriverStatus {
                scope: "GRAPH",
                authoritative: true,
                state: "FUTURE_STATE",
                pending_count: 0,
                published_open_folds: 0,
                last_completion_kind: None,
                last_error: None,
            },
            rebuild: GraphStreamRebuildStatus {
                ready: false,
                blockers: vec![GraphStreamRebuildBlocker::ProfileNotTerminal],
            },
        };

        assert_eq!(
            stream_status_output(status),
            Err("unknown stream driver state")
        );
    }
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
    /// The internal-schema (storage-format) version this binary writes and reads.
    pub internal_schema_version: u32,
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
    /// 405 Method Not Allowed — the route exists but the active server
    /// mode doesn't serve this method (e.g. `GET /graphs` in single-graph
    /// mode). Distinct from 404 so clients can tell "wrong context" from
    /// "no such resource."
    MethodNotAllowed,
    Conflict,
    /// 429 Too Many Requests — per-actor admission cap exceeded.
    /// Clients should respect the `Retry-After` header.
    TooManyRequests,
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

/// Structured authority mismatch for a prepared write. Values are
/// strings because members include optional graph commit ids and future
/// authority tokens, not only numeric table versions.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadSetConflictOutput {
    pub member: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
}

/// A strict insert rejected because `key` already names a row in the keyed
/// graph table.  The operation is effect-free when this output is returned;
/// partial or ambiguous attempts surface `recovery_required` instead.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KeyConflictOutput {
    pub table_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// A write rejected before durable recovery ownership because its bounded
/// physical plan exceeded an explicit row, byte, or transaction-chain ceiling.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ResourceLimitOutput {
    pub resource: String,
    pub limit: u64,
    pub actual: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecoveryRequiredOutput {
    pub operation_id: String,
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
    /// Set when a prepared write's logical authority changed before effects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_set_conflict: Option<ReadSetConflictOutput>,
    /// Set when a strict keyed insert found an existing or concurrently
    /// inserted logical id.  The caller may choose a different id; replaying
    /// the same strict operation will not convert it into an upsert.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_conflict: Option<KeyConflictOutput>,
    /// Set when the request must be split into smaller graph commits. The
    /// rejected attempt has no durable sidecar and no table effect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_limit: Option<ResourceLimitOutput>,
    /// Set when an overlapping durable recovery intent must be resolved before
    /// retry. Its table effects may or may not have started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_required: Option<RecoveryRequiredOutput>,
}

pub fn snapshot_payload(
    branch: &str,
    snapshot: &Snapshot,
    internal_schema_version: u32,
) -> SnapshotOutput {
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
        internal_schema_version,
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

pub fn ingest_output(
    uri: &str,
    result: &LoadResult,
    mode: LoadMode,
    actor_id: Option<String>,
) -> IngestOutput {
    IngestOutput {
        uri: uri.to_string(),
        branch: result.branch.clone(),
        base_branch: result.base_branch.clone(),
        branch_created: result.branch_created,
        mode,
        tables: result
            .to_ingest_tables()
            .into_iter()
            .map(|table| IngestTableOutput {
                table_key: table.table_key,
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

// ─── MR-668 — management endpoint shapes ──────────────────────────────────

/// One entry in the response from `GET /graphs`. Cluster operators
/// consume this list to discover which graphs the server is currently
/// serving. The shape is intentionally minimal — `graph_id` and `uri`
/// are the only fields a routing client needs.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphInfo {
    pub graph_id: String,
    pub uri: String,
}

/// Response from `GET /graphs`. Lists every graph registered with the
/// server in alphabetical order by `graph_id` (sorted server-side so
/// clients get deterministic output across requests).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphListResponse {
    pub graphs: Vec<GraphInfo>,
}
