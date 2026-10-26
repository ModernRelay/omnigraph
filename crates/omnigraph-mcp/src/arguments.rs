//! Typed MCP input without graph or server authority.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArguments {}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphArguments {
    pub graph: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchArguments {
    pub graph: String,
    #[serde(default)]
    pub branch: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessArguments {
    pub graph: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub target_branch: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitsListArguments {
    pub graph: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub listing: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub page_token: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitArguments {
    pub graph: String,
    pub commit_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryArguments {
    pub graph: String,
    pub name: String,
    #[serde(default)]
    pub params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    pub branch: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadArguments {
    pub graph: String,
    pub query: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub snapshot: Option<String>,
}
/// The change vocabulary's filters and page continuation, sent to the route
/// as the same repeated query parameters an HTTP caller sends.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ChangeFilters {
    #[serde(default)]
    pub kind: Vec<String>,
    #[serde(default)]
    pub r#type: Vec<String>,
    #[serde(default)]
    pub op: Vec<String>,
    #[serde(default)]
    pub page_token: Option<String>,
    #[serde(default)]
    pub limit: Option<u64>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitChangesArguments {
    pub graph: String,
    pub commit_id: String,
    #[serde(flatten)]
    pub filters: ChangeFilters,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesPollArguments {
    pub graph: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(flatten)]
    pub filters: ChangeFilters,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffArguments {
    pub graph: String,
    pub from: String,
    pub to: String,
    #[serde(flatten)]
    pub filters: ChangeFilters,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchDiffArguments {
    pub graph: String,
    pub source: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(flatten)]
    pub filters: ChangeFilters,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeBaseArguments {
    pub graph: String,
    pub source: String,
    #[serde(default)]
    pub target: Option<String>,
}
/// One Blob cell and its read target, as the Blob read routes' query names them.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobStatArguments {
    pub graph: String,
    pub entity: String,
    pub r#type: String,
    pub id: String,
    pub property: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub snapshot: Option<String>,
}
/// One Blob cell and the branch to write, with the route's `If-Match` value.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobClearArguments {
    pub graph: String,
    pub entity: String,
    pub r#type: String,
    pub id: String,
    pub property: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub if_match: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutateArguments {
    pub graph: String,
    /// Ad-hoc GQ source. Without it, `name` names a stored mutation.
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub if_graph_commit: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadArguments {
    pub graph: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    pub mode: String,
    pub data: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchCreateArguments {
    pub graph: String,
    pub name: String,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub from_commit: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchDeleteArguments {
    pub graph: String,
    pub name: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchMergeArguments {
    pub graph: String,
    pub source: String,
    #[serde(default)]
    pub target: Option<String>,
}

/// A request after the protocol has checked its tool name and boot gate.
/// The host reserves write ingress before calling [`Self::parse`], preserving
/// the same admission and validation order as the corresponding HTTP handlers.
#[derive(Debug)]
pub struct ToolRequest {
    name: &'static str,
    arguments: Value,
    write: bool,
}

impl ToolRequest {
    pub(crate) fn new(name: &'static str, arguments: Value, write: bool) -> Self {
        Self {
            name,
            arguments,
            write,
        }
    }

    /// Whether the tool uses the write lane.
    pub fn is_write(&self) -> bool {
        self.write
    }

    /// The argument bytes charged to the host's write-ingress reservation.
    pub fn argument_bytes(&self) -> Result<usize, InvalidArguments> {
        serde_json::to_vec(&self.arguments)
            .map(|bytes| bytes.len())
            .map_err(|_| InvalidArguments)
    }

    /// Decode only after the host has retained operation and input ownership.
    /// Unknown fields and malformed values retain the existing HTTP refusal.
    pub fn parse(self) -> Result<ToolCall, InvalidArguments> {
        let arguments = self.arguments;
        match self.name {
            "health" => {
                let _: NoArguments = arguments_as(arguments)?;
                Ok(ToolCall::Health)
            }
            "graphs" => {
                let _: NoArguments = arguments_as(arguments)?;
                Ok(ToolCall::Graphs)
            }
            "schema" => Ok(ToolCall::Schema(arguments_as(arguments)?)),
            "queries" => Ok(ToolCall::Queries(arguments_as(arguments)?)),
            "query" => Ok(ToolCall::Query(arguments_as(arguments)?)),
            "read" => Ok(ToolCall::Read(arguments_as(arguments)?)),
            "snapshot" => Ok(ToolCall::Snapshot(arguments_as(arguments)?)),
            "access" => Ok(ToolCall::Access(arguments_as(arguments)?)),
            "branches_list" => Ok(ToolCall::BranchesList(arguments_as(arguments)?)),
            "commits_list" => Ok(ToolCall::CommitsList(arguments_as(arguments)?)),
            "commits_get" => Ok(ToolCall::CommitsGet(arguments_as(arguments)?)),
            "commits_changes" => Ok(ToolCall::CommitsChanges(arguments_as(arguments)?)),
            "changes_poll" => Ok(ToolCall::ChangesPoll(arguments_as(arguments)?)),
            "diff" => Ok(ToolCall::Diff(arguments_as(arguments)?)),
            "branches_diff" => Ok(ToolCall::BranchesDiff(arguments_as(arguments)?)),
            "branches_merge_base" => Ok(ToolCall::BranchesMergeBase(arguments_as(arguments)?)),
            "blobs_stat" => Ok(ToolCall::BlobsStat(arguments_as(arguments)?)),
            "mutate" => Ok(ToolCall::Mutate(arguments_as(arguments)?)),
            "load" => Ok(ToolCall::Load(arguments_as(arguments)?)),
            "blobs_clear" => Ok(ToolCall::BlobsClear(arguments_as(arguments)?)),
            "branches_create" => Ok(ToolCall::BranchesCreate(arguments_as(arguments)?)),
            "branches_delete" => Ok(ToolCall::BranchesDelete(arguments_as(arguments)?)),
            "branches_merge" => Ok(ToolCall::BranchesMerge(arguments_as(arguments)?)),
            "commits_publish" => Ok(ToolCall::CommitsPublish(arguments_as(arguments)?)),
            "commits_drop" => Ok(ToolCall::CommitsDrop(arguments_as(arguments)?)),
            _ => Err(InvalidArguments),
        }
    }
}

/// A malformed MCP argument object; the host maps it through its canonical
/// HTTP bad-request constructor instead of maintaining a second error table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidArguments;

impl std::fmt::Display for InvalidArguments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid MCP tool arguments")
    }
}
impl std::error::Error for InvalidArguments {}

/// Typed transport arguments. The host executes these through its existing
/// graph handlers; this crate never resolves graph authority or actors.
#[derive(Debug)]
pub enum ToolCall {
    Health,
    Graphs,
    Schema(GraphArguments),
    Queries(GraphArguments),
    Query(QueryArguments),
    Read(ReadArguments),
    Snapshot(BranchArguments),
    Access(AccessArguments),
    BranchesList(GraphArguments),
    CommitsList(CommitsListArguments),
    CommitsGet(CommitArguments),
    CommitsChanges(CommitChangesArguments),
    ChangesPoll(ChangesPollArguments),
    Diff(DiffArguments),
    BranchesDiff(BranchDiffArguments),
    BranchesMergeBase(MergeBaseArguments),
    BlobsStat(BlobStatArguments),
    Mutate(MutateArguments),
    Load(LoadArguments),
    BlobsClear(BlobClearArguments),
    BranchesCreate(BranchCreateArguments),
    BranchesDelete(BranchDeleteArguments),
    BranchesMerge(BranchMergeArguments),
    CommitsPublish(CommitArguments),
    CommitsDrop(CommitArguments),
}

fn arguments_as<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, InvalidArguments> {
    serde_json::from_value(value).map_err(|_| InvalidArguments)
}

/// Encode repeated change filters exactly as the corresponding HTTP query.
pub fn change_query(scalars: &[(&str, Option<&str>)], filters: &ChangeFilters) -> Option<String> {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in scalars {
        if let Some(value) = value {
            query.append_pair(name, value);
        }
    }
    for kind in &filters.kind {
        query.append_pair("kind", kind);
    }
    for name in &filters.r#type {
        query.append_pair("type", name);
    }
    for op in &filters.op {
        query.append_pair("op", op);
    }
    if let Some(token) = &filters.page_token {
        query.append_pair("page_token", token);
    }
    if let Some(limit) = filters.limit {
        query.append_pair("limit", &limit.to_string());
    }
    let query = query.finish();
    (!query.is_empty()).then_some(query)
}
