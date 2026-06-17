//! Shared HTTP wire DTOs (RFC-009 Phase 2) — moved from
//! omnigraph-server's api module so server and CLI share one definition
//! and one engine-result -> DTO mapping per verb. Plain serde/utoipa
//! types; no transport, no server internals.

use omnigraph::db::{GraphCommit, MergeOutcome, ReadTarget, SchemaApplyResult, Snapshot};
use omnigraph::error::{MergeConflict, MergeConflictKind};
use omnigraph::loader::{LoadMode, LoadResult};
use omnigraph_compiler::SchemaMigrationStep;
use omnigraph_compiler::query::ast::Param;
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::types::{PropType, ScalarType};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
    #[schema(example = "query get_person($name: String) {\n    match {\n        $p: Person { name: $name }\n    }\n    return { $p.name, $p.age }\n}")]
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
    /// The kind the caller expects (RFC-011 Decision 3): `Some(false)` for
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

/// Response for `GET /queries`: the `mcp.expose` subset of a graph's
/// stored-query registry, each with typed parameters.
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

/// JSON Schema (2020-12) for a scalar param kind. **Superset of the engine
/// coercer** (`omnigraph_compiler::coerce_param_typed`, Standard mode): a
/// too-narrow schema would make a strict client reject inputs the engine
/// accepts; a too-wide one reaches the coercer and surfaces as an `isError`
/// tool result for model self-correction (SEP-1303). Locked to the coercer by
/// `tests/schema_equivalence.rs`. Exhaustive + wildcard-free: adding a
/// `ParamKind` is a compile error until its arm (and corpus row) exist.
fn scalar_schema(kind: ParamKind) -> Value {
    match kind {
        ParamKind::String => json!({ "type": "string" }),
        ParamKind::Bool => json!({ "type": "boolean" }),
        // Standard-mode integer coercion accepts a JSON number OR a numeric
        // string (i64/u64 lose precision past 2^53 as a JSON number), so the
        // schema accepts both; range/sign are the coercer's to enforce.
        ParamKind::Int | ParamKind::BigInt => json!({
            "anyOf": [ { "type": "integer" }, { "type": "string", "pattern": r"^-?\d+$" } ]
        }),
        ParamKind::Float => json!({ "type": "number" }),
        // Date/DateTime/Blob coerce from any string; `format` is an advisory
        // annotation (non-asserting in 2020-12), so the schema accepts exactly
        // what the coercer does while still hinting the shape to clients.
        ParamKind::Date => json!({ "type": "string", "format": "date" }),
        ParamKind::DateTime => json!({ "type": "string", "format": "date-time" }),
        ParamKind::Blob => json!({ "type": "string", "format": "uri" }),
        ParamKind::Vector | ParamKind::List => {
            unreachable!("composite kinds are handled in param_json_schema")
        }
    }
}

/// The JSON Schema (2020-12) for a stored-query parameter — the single mapping
/// both the OpenAPI catalog and the MCP tool projection consume, applying the
/// nullable rule uniformly. See [`scalar_schema`] for the superset contract.
pub fn param_json_schema(p: &ParamDescriptor) -> Value {
    let base = match p.kind {
        ParamKind::Vector => {
            let mut schema = json!({ "type": "array", "items": { "type": "number" } });
            if let Some(dim) = p.vector_dim {
                schema["minItems"] = json!(dim);
                schema["maxItems"] = json!(dim);
            }
            schema
        }
        ParamKind::List => {
            let item = p
                .item_kind
                .map(scalar_schema)
                .unwrap_or_else(|| json!({ "type": "string" }));
            json!({ "type": "array", "items": item })
        }
        scalar => scalar_schema(scalar),
    };
    // The coercer accepts explicit `null` for a nullable param (and its
    // omission); a strict client would reject `null` against the bare scalar.
    // Allow null at the schema level for nullable params.
    if p.nullable {
        json!({ "anyOf": [ base, { "type": "null" } ] })
    } else {
        base
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
