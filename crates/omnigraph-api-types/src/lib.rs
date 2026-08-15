//! Shared HTTP wire DTOs (RFC-009 Phase 2) — moved from
//! omnigraph-server's api module so server and CLI share one definition
//! and one engine-result -> DTO mapping per verb. Plain serde/utoipa
//! types; no transport, no server internals.

use omnigraph::db::{GraphCommit, MergeOutcome, ReadTarget, SchemaApplyResult, Snapshot};
use omnigraph::error::{MergeConflict, MergeConflictKind};
use omnigraph::loader::{LoadMode, LoadReceipt, LoadResult};
use omnigraph_compiler::SchemaMigrationStep;
use omnigraph_compiler::query::ast::Param;
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::types::{PropType, ScalarType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

/// Lowercase wire name for the raw graph-head conditional-write token.
/// Documentation presents the canonical spelling
/// `Omnigraph-If-Graph-Commit`; HTTP header names are case-insensitive.
pub const GRAPH_COMMIT_PRECONDITION_HEADER: &str = "omnigraph-if-graph-commit";

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
    /// Effective graph head commit id of the exact snapshot this read was
    /// served from. On a fresh named branch this is the inherited source head,
    /// so it is immediately usable as `Omnigraph-If-Graph-Commit` (CLI:
    /// `--if-commit`) for the branch's first conditional write. The id and rows
    /// come from one pinned version, so no separate id fetch is needed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_commit_id: Option<String>,
}

/// Indefinitely byte-stable response shape for the deprecated `POST /read`
/// route. The canonical [`ReadOutput`] may grow additive fields; this legacy
/// envelope deliberately cannot carry them.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LegacyReadOutput {
    pub query_name: String,
    pub target: ReadTargetOutput,
    pub row_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    pub rows: Value,
}

impl From<ReadOutput> for LegacyReadOutput {
    fn from(value: ReadOutput) -> Self {
        Self {
            query_name: value.query_name,
            target: value.target,
            row_count: value.row_count,
            columns: value.columns,
            rows: value.rows,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeOutput {
    pub branch: String,
    pub query_name: String,
    pub affected_nodes: usize,
    pub affected_edges: usize,
    pub actor_id: Option<String>,
    pub commit: Option<CommitOutput>,
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
    pub commit: Option<CommitOutput>,
}

/// One logical declaration touched by a graph-batch load.
///
/// This deliberately carries the accepted-schema name, not the backing
/// manifest table key, dataset path, or Lance identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GraphBatchDeclarationOutput {
    pub name: String,
    pub rows_loaded: usize,
}

/// Terminal result for the raw graph-level NDJSON load surface.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphBatchLoadOutput {
    pub branch: String,
    /// Base branch a fork was requested from, even when the target already
    /// existed. `null` when the request omitted `from`.
    pub base_branch: Option<String>,
    pub branch_created: bool,
    #[schema(value_type = LoadModeSchema)]
    pub mode: LoadMode,
    /// Logical node declarations touched by this batch, sorted by name.
    pub nodes: Vec<GraphBatchDeclarationOutput>,
    /// Logical edge declarations touched by this batch, sorted by name.
    pub edges: Vec<GraphBatchDeclarationOutput>,
    pub total_rows: usize,
    pub actor_id: Option<String>,
    pub commit: Option<CommitOutput>,
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

/// Logical entity namespace of one change. Graph vocabulary only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeEntityKind {
    Node,
    Edge,
}

impl ChangeEntityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "node" => Some(Self::Node),
            "edge" => Some(Self::Edge),
            _ => None,
        }
    }
}

/// Logical operation of one change. Ordering rank is frozen:
/// insert before update before delete within one entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOpOutput {
    Insert,
    Update,
    Delete,
}

impl ChangeOpOutput {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "insert" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Graph-scoped type identity. `id` is opaque: it survives a supported rename
/// and changes after drop/re-add. It is never a table, dataset, or path
/// identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ChangeTypeOutput {
    pub id: String,
    pub name: String,
}

/// Edge endpoints as graph references. Endpoints belong to each image, so an
/// endpoint-moving update has distinct before and after endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ChangeEndpointsOutput {
    pub from: String,
    pub to: String,
}

/// One exact logical entity image, decoded with the commit-era schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ChangeImageOutput {
    /// Exact logical property values; user-schema keys verbatim.
    pub properties: Value,
    /// Present for edge images only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<ChangeEndpointsOutput>,
}

/// One entity change. Cause is stated once on the enclosing block, never here.
/// An insert carries only `after`, an update exact `before` and `after`, a
/// delete only `before`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EntityChangeOutput {
    pub kind: ChangeEntityKind,
    pub r#type: ChangeTypeOutput,
    pub id: String,
    pub op: ChangeOpOutput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<ChangeImageOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<ChangeImageOutput>,
}

/// The commit cause of one change block, stated once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ChangeCauseOutput {
    pub graph_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_parent_commit_id: Option<String>,
    /// The branch the commit originally landed on (not the requested branch).
    pub authored_branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    /// Authorship time as Unix epoch microseconds — minted before table
    /// effects and stable across retries; deliberately not labeled a commit or
    /// publication time.
    #[schema(example = 1714000000000000i64)]
    pub authored_at: i64,
}

/// One bounded page of the finite commit entity diff.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CommitChangesOutput {
    pub cause: ChangeCauseOutput,
    pub changes: Vec<EntityChangeOutput>,
    /// Continue THIS bounded response. Absent on the final page. Never a feed
    /// cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// One commit block inside a feed page.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeBlockOutput {
    pub cause: ChangeCauseOutput,
    pub changes: Vec<EntityChangeOutput>,
}

/// One bounded feed poll result.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeFeedOutput {
    pub blocks: Vec<ChangeBlockOutput>,
    /// Continue this poll's captured cut. Absent on a terminal page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    /// Durable caller-owned cursor, advanced only over complete commits and
    /// returned only on a terminal page — an interrupted poll never advances
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Present with `cursor` on a terminal page: true when the page reached
    /// its captured head, false when more complete commits already wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caught_up: Option<bool>,
}

/// Query parameters for the finite commit entity diff.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct CommitChangesQuery {
    /// Opaque continuation from the preceding page of this response.
    pub page_token: Option<String>,
    /// Maximum changes per page. Server default applies when absent; above
    /// the public ceiling the request fails with 413.
    pub limit: Option<usize>,
    /// Repeatable filter: node | edge.
    #[serde(default)]
    pub kind: Vec<ChangeEntityKind>,
    /// Repeatable filter: accepted-schema type name.
    #[serde(default)]
    pub r#type: Vec<String>,
    /// Repeatable filter: insert | update | delete.
    #[serde(default)]
    pub op: Vec<ChangeOpOutput>,
}

/// Query parameters for the change feed poll.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ChangeFeedQuery {
    /// Branch whose first-parent history is polled. Defaults to `main`.
    pub branch: Option<String>,
    /// Durable cursor from a prior terminal page. Mutually exclusive with
    /// `start` and `page_token`.
    pub cursor: Option<String>,
    /// Explicit start mode: `now` (default) | `beginning` |
    /// `after:<commit_id>`. Mutually exclusive with `cursor` and `page_token`.
    pub start: Option<String>,
    /// Continuation of one bounded poll (keeps its captured cut). Mutually
    /// exclusive with `cursor` and `start`.
    pub page_token: Option<String>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub kind: Vec<ChangeEntityKind>,
    #[serde(default)]
    pub r#type: Vec<String>,
    #[serde(default)]
    pub op: Vec<ChangeOpOutput>,
}

/// Body for the change baseline handshake.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ChangeBaselineRequest {
    /// Branch to capture. Defaults to `main`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Feed scope the resume cursor is bound to. The snapshot honors `kind`
    /// and `type`; `op` constrains only subsequent polls.
    #[serde(default)]
    pub kind: Vec<ChangeEntityKind>,
    #[serde(default)]
    pub r#type: Vec<String>,
    #[serde(default)]
    pub op: Vec<ChangeOpOutput>,
}

/// Terminal payload of a baseline stream: the captured snapshot commit and
/// the cursor that resumes the feed immediately after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ChangeBaselineOutput {
    pub snapshot_commit_id: String,
    pub resume_cursor: String,
}

/// Wire envelope of the FINAL baseline stream line: `{"baseline": {...}}`,
/// distinguishable from snapshot records (which carry `type`/`edge` keys).
/// Emitted exactly once, only after every snapshot record — an interrupted
/// stream has no terminal record and therefore no usable cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ChangeBaselineRecord {
    pub baseline: ChangeBaselineOutput,
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

/// Logical graph entity selected by the Blob delivery surface.
///
/// This is intentionally graph vocabulary. The wire contract never exposes a
/// Lance dataset, table key, stable row id, or per-table lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BlobEntityKind {
    Node,
    Edge,
}

/// Query parameters shared by `GET` and `HEAD /graphs/{graph_id}/blob`.
#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct BlobReadQuery {
    /// Select a logical node or edge cell.
    pub entity: BlobEntityKind,
    /// Accepted-schema node or edge type name.
    pub r#type: String,
    /// Logical entity id within the selected type.
    pub id: String,
    /// Accepted-schema Blob property name.
    pub property: String,
    /// Branch to read. Mutually exclusive with `snapshot`; defaults to `main`.
    pub branch: Option<String>,
    /// Immutable graph snapshot id. Mutually exclusive with `branch`.
    pub snapshot: Option<String>,
}

/// One logical graph Blob cell, without exposing its backing Lance table or
/// physical row identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobSelectorOutput {
    pub entity: BlobEntityKind,
    pub r#type: String,
    pub id: String,
    pub property: String,
}

impl From<&BlobReadQuery> for BlobSelectorOutput {
    fn from(query: &BlobReadQuery) -> Self {
        Self {
            entity: query.entity,
            r#type: query.r#type.clone(),
            id: query.id.clone(),
            property: query.property.clone(),
        }
    }
}

/// Descriptor classification returned by `blob stat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BlobContentKindOutput {
    Managed,
    External,
}

/// The caller's requested read target together with the immutable graph
/// snapshot that was actually resolved.
///
/// `branch` and `snapshot` echo the request and are mutually exclusive. Both
/// are absent when the caller accepted the default branch. `resolved_snapshot`
/// is always present so embedded and remote clients can identify the exact
/// graph view with the same output shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobResolvedTargetOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
    pub resolved_snapshot: String,
}

impl BlobResolvedTargetOutput {
    pub fn from_read_query(query: &BlobReadQuery, resolved_snapshot: impl Into<String>) -> Self {
        Self {
            branch: query.branch.clone(),
            snapshot: query.snapshot.clone(),
            resolved_snapshot: resolved_snapshot.into(),
        }
    }
}

/// Transport-neutral metadata for one non-null Blob cell.
///
/// Managed content carries `size` and `etag`; external content carries `uri`.
/// Inapplicable fields are omitted rather than serialized as JSON nulls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobStatOutput {
    pub selector: BlobSelectorOutput,
    pub kind: BlobContentKindOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    pub target: BlobResolvedTargetOutput,
}

impl BlobStatOutput {
    pub fn managed(
        query: &BlobReadQuery,
        resolved_snapshot: impl Into<String>,
        size: u64,
        etag: impl Into<String>,
    ) -> Self {
        Self {
            selector: query.into(),
            kind: BlobContentKindOutput::Managed,
            size: Some(size),
            etag: Some(etag.into()),
            uri: None,
            target: BlobResolvedTargetOutput::from_read_query(query, resolved_snapshot),
        }
    }

    pub fn external(
        query: &BlobReadQuery,
        resolved_snapshot: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            selector: query.into(),
            kind: BlobContentKindOutput::External,
            size: None,
            etag: None,
            uri: Some(uri.into()),
            target: BlobResolvedTargetOutput::from_read_query(query, resolved_snapshot),
        }
    }
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

/// Query parameters for `POST /load/ndjson`.
#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct GraphBatchLoadQuery {
    /// Target branch. Defaults to `main`. Without `from`, it must exist.
    pub branch: Option<String>,
    /// Parent branch used to create a missing target branch.
    pub from: Option<String>,
    /// How existing rows are handled. Defaults to `merge`.
    #[param(value_type = Option<LoadModeSchema>)]
    pub mode: Option<LoadMode>,
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

/// Normalized half-open range details for an unsatisfiable managed Blob read.
///
/// HTTP also returns `Content-Range: bytes */N`; these fields let SDKs inspect
/// the failure without parsing either that header or the human-readable text.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlobRangeOutput {
    pub start: u64,
    pub end: u64,
    pub length: u64,
}

/// Structured details for an allowed external Blob source that could not be
/// probed or read. The top-level `code` remains optional so this additive
/// detail can roll out without extending the closed [`ErrorCode`] enum.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExternalBlobSourceOutput {
    /// Normalized, credential-free URI spelling (or a redacted placeholder).
    pub uri: String,
    /// Source-side failure diagnosis. Clients should branch on the presence of
    /// `external_blob_source`, not parse this human-readable text.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecoveryRequiredOutput {
    pub operation_id: String,
}

/// Structured details for a caller write-precondition failure: HTTP 412, a
/// mutation carried `Omnigraph-If-Graph-Commit: <commit_id>`, and the branch
/// head no longer matches that id. The write had no effect; the caller re-reads
/// the branch and decides again. `actual` is `None` on a branch with no commits.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PreconditionFailureOutput {
    pub expected: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

/// A change continuation can no longer be reconstructed from retained history
/// (HTTP 410). Recovery is the baseline handshake; retrying the same cursor
/// cannot succeed. `code` stays unset: [`ErrorCode`] is closed and this
/// additive detail is the machine-readable discriminator (the same rolling
/// contract as `external_blob_source`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeFeedGapOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub first_unreadable_commit_id: String,
}

/// Why a well-formed entity-diff request was refused (HTTP 409).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeDiffRefusalReason {
    /// The commit has no first parent; bootstrap from a baseline instead.
    ParentlessCommit,
    /// The parent/child pair crosses an unprovable schema boundary.
    SchemaBoundary,
    /// A reason added by a newer server; treat like a schema boundary.
    #[serde(other)]
    Unknown,
}

/// A well-formed entity-diff request this commit cannot satisfy (HTTP 409).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChangeDiffRefusalOutput {
    pub reason: ChangeDiffRefusalReason,
    pub graph_commit_id: String,
    /// The graph type at the schema boundary, when the reason names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
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
    /// Set with HTTP 416 for a valid but unsatisfiable managed Blob byte range.
    /// `start..end` is half-open and `length` is the selected Blob length.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_range: Option<BlobRangeOutput>,
    /// Set with HTTP 424 when an external Blob URI passed admission policy but
    /// its source could not be probed or read. This optional detail is the
    /// rolling-safe machine-readable discriminator; `code` is omitted because
    /// [`ErrorCode`] is a closed compatibility contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_blob_source: Option<ExternalBlobSourceOutput>,
    /// Set when an overlapping durable recovery intent must be resolved before
    /// retry. Its table effects may or may not have started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_required: Option<RecoveryRequiredOutput>,
    /// Set when a mutation's graph-commit precondition failed
    /// (HTTP 412). Like `recovery_required`, the meaning rides this additive
    /// field — `ErrorCode` is a closed rolling wire contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precondition_failure: Option<PreconditionFailureOutput>,
    /// Set with HTTP 410 when retained history can no longer reconstruct a
    /// change continuation. Recover via the baseline handshake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_feed_gap: Option<ChangeFeedGapOutput>,
    /// Set with HTTP 409 when a commit entity diff is refused (parentless
    /// commit or an unprovable schema boundary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_diff_refusal: Option<ChangeDiffRefusalOutput>,
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

impl From<omnigraph::changes::ChangeEntityKind> for ChangeEntityKind {
    fn from(kind: omnigraph::changes::ChangeEntityKind) -> Self {
        match kind {
            omnigraph::changes::ChangeEntityKind::Node => Self::Node,
            omnigraph::changes::ChangeEntityKind::Edge => Self::Edge,
        }
    }
}

impl From<ChangeEntityKind> for omnigraph::changes::ChangeEntityKind {
    fn from(kind: ChangeEntityKind) -> Self {
        match kind {
            ChangeEntityKind::Node => Self::Node,
            ChangeEntityKind::Edge => Self::Edge,
        }
    }
}

impl From<omnigraph::changes::ChangeOpKind> for ChangeOpOutput {
    fn from(op: omnigraph::changes::ChangeOpKind) -> Self {
        match op {
            omnigraph::changes::ChangeOpKind::Insert => Self::Insert,
            omnigraph::changes::ChangeOpKind::Update => Self::Update,
            omnigraph::changes::ChangeOpKind::Delete => Self::Delete,
        }
    }
}

impl From<ChangeOpOutput> for omnigraph::changes::ChangeOpKind {
    fn from(op: ChangeOpOutput) -> Self {
        match op {
            ChangeOpOutput::Insert => Self::Insert,
            ChangeOpOutput::Update => Self::Update,
            ChangeOpOutput::Delete => Self::Delete,
        }
    }
}

/// Wire filter vocabulary → the engine's feed scope. Shared by the server
/// handlers and the CLI's embedded arm so both translate identically.
pub fn change_scope(
    kinds: &[ChangeEntityKind],
    type_names: &[String],
    ops: &[ChangeOpOutput],
) -> omnigraph::changes::ChangeFeedScope {
    omnigraph::changes::ChangeFeedScope {
        kinds: (!kinds.is_empty()).then(|| kinds.iter().map(|kind| (*kind).into()).collect()),
        type_names: (!type_names.is_empty()).then(|| type_names.to_vec()),
        ops: (!ops.is_empty()).then(|| ops.iter().map(|op| (*op).into()).collect()),
    }
}

pub fn change_cause_output(cause: &omnigraph::changes::ChangeCause) -> ChangeCauseOutput {
    ChangeCauseOutput {
        graph_commit_id: cause.graph_commit_id.clone(),
        parent_commit_id: cause.parent_commit_id.clone(),
        merged_parent_commit_id: cause.merged_parent_commit_id.clone(),
        authored_branch: cause
            .authored_branch
            .clone()
            .unwrap_or_else(|| "main".to_string()),
        actor_id: cause.actor_id.clone(),
        authored_at: cause.authored_at,
    }
}

fn change_image_output(image: &omnigraph::changes::EntityImage) -> ChangeImageOutput {
    ChangeImageOutput {
        properties: Value::Object(image.properties.clone()),
        endpoints: image
            .endpoints
            .as_ref()
            .map(|endpoints| ChangeEndpointsOutput {
                from: endpoints.from.clone(),
                to: endpoints.to.clone(),
            }),
    }
}

pub fn entity_change_output(change: &omnigraph::changes::GraphEntityChange) -> EntityChangeOutput {
    EntityChangeOutput {
        kind: change.kind.into(),
        r#type: ChangeTypeOutput {
            id: change.entity_type.id.clone(),
            name: change.entity_type.name.clone(),
        },
        id: change.id.clone(),
        op: change.op.into(),
        before: change.before.as_ref().map(change_image_output),
        after: change.after.as_ref().map(change_image_output),
    }
}

pub fn change_block_output(block: &omnigraph::changes::GraphChangeBlock) -> ChangeBlockOutput {
    ChangeBlockOutput {
        cause: change_cause_output(&block.cause),
        changes: block.changes.iter().map(entity_change_output).collect(),
    }
}

pub fn commit_changes_output(page: &omnigraph::changes::CommitChangesPage) -> CommitChangesOutput {
    CommitChangesOutput {
        cause: change_cause_output(&page.block.cause),
        changes: page
            .block
            .changes
            .iter()
            .map(entity_change_output)
            .collect(),
        next_page_token: page.next_page_token.clone(),
    }
}

pub fn change_feed_output(page: &omnigraph::changes::ChangeFeedPage) -> ChangeFeedOutput {
    let (next_page_token, cursor, caught_up) = match &page.continuation {
        omnigraph::changes::ChangeFeedContinuation::MidBlock { page_token } => {
            (Some(page_token.clone()), None, None)
        }
        omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, caught_up } => {
            (None, Some(cursor.clone()), Some(*caught_up))
        }
    };
    ChangeFeedOutput {
        blocks: page.blocks.iter().map(change_block_output).collect(),
        next_page_token,
        cursor,
        caught_up,
    }
}

pub fn change_baseline_output(
    baseline: &omnigraph::changes::ChangeBaseline,
) -> ChangeBaselineOutput {
    ChangeBaselineOutput {
        snapshot_commit_id: baseline.snapshot_commit_id.clone(),
        resume_cursor: baseline.resume_cursor.clone(),
    }
}

pub fn read_output(
    query_name: String,
    target: &ReadTarget,
    result: QueryResult,
    graph_commit_id: Option<String>,
) -> ReadOutput {
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
        graph_commit_id,
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
        commit: None,
    }
}

pub fn ingest_receipt_output(
    uri: &str,
    receipt: &LoadReceipt,
    mode: LoadMode,
    actor_id: Option<String>,
) -> IngestOutput {
    let mut output = ingest_output(uri, &receipt.result, mode, actor_id);
    output.commit = Some(commit_output(&receipt.commit));
    output
}

pub fn graph_batch_load_output(
    result: &LoadResult,
    mode: LoadMode,
    actor_id: Option<String>,
) -> GraphBatchLoadOutput {
    let mut nodes = result
        .nodes_loaded
        .iter()
        .map(|(name, rows_loaded)| GraphBatchDeclarationOutput {
            name: name.clone(),
            rows_loaded: *rows_loaded,
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.name.cmp(&right.name));

    let mut edges = result
        .edges_loaded
        .iter()
        .map(|(name, rows_loaded)| GraphBatchDeclarationOutput {
            name: name.clone(),
            rows_loaded: *rows_loaded,
        })
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| left.name.cmp(&right.name));

    let total_rows = nodes
        .iter()
        .chain(&edges)
        .map(|declaration| declaration.rows_loaded)
        .sum();
    GraphBatchLoadOutput {
        branch: result.branch.clone(),
        base_branch: result.base_branch.clone(),
        branch_created: result.branch_created,
        mode,
        nodes,
        edges,
        total_rows,
        actor_id,
        commit: None,
    }
}

pub fn graph_batch_load_receipt_output(
    receipt: &LoadReceipt,
    mode: LoadMode,
    actor_id: Option<String>,
) -> GraphBatchLoadOutput {
    let mut output = graph_batch_load_output(&receipt.result, mode, actor_id);
    output.commit = Some(commit_output(&receipt.commit));
    output
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn blob_stat_output_has_one_shared_shape_and_omits_inapplicable_fields() {
        let managed_query = BlobReadQuery {
            entity: BlobEntityKind::Node,
            r#type: "Document".to_string(),
            id: "doc-1".to_string(),
            property: "payload".to_string(),
            branch: Some("review".to_string()),
            snapshot: None,
        };
        let managed = BlobStatOutput::managed(
            &managed_query,
            "snapshot-review-exact",
            0,
            "\"etag-managed\"",
        );
        assert_eq!(
            serde_json::to_value(managed).unwrap(),
            json!({
                "selector": {
                    "entity": "node",
                    "type": "Document",
                    "id": "doc-1",
                    "property": "payload"
                },
                "kind": "managed",
                "size": 0,
                "etag": "\"etag-managed\"",
                "target": {
                    "branch": "review",
                    "resolved_snapshot": "snapshot-review-exact"
                }
            })
        );

        let external_query = BlobReadQuery {
            entity: BlobEntityKind::Edge,
            r#type: "Attachment".to_string(),
            id: "edge-1".to_string(),
            property: "payload".to_string(),
            branch: None,
            snapshot: Some("snapshot-requested".to_string()),
        };
        let external = BlobStatOutput::external(
            &external_query,
            "snapshot-requested",
            "s3://example/blob.bin",
        );
        assert_eq!(
            serde_json::to_value(external).unwrap(),
            json!({
                "selector": {
                    "entity": "edge",
                    "type": "Attachment",
                    "id": "edge-1",
                    "property": "payload"
                },
                "kind": "external",
                "uri": "s3://example/blob.bin",
                "target": {
                    "snapshot": "snapshot-requested",
                    "resolved_snapshot": "snapshot-requested"
                }
            })
        );
    }
}
