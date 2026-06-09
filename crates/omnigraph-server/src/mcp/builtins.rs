//! Built-in MCP tools — RFC-003 §5.2.
//!
//! Each tool reuses the EXACT `do_*` / `run_query` / `run_mutate` path (and the
//! exact Cedar action) its HTTP route uses, so there is no new business logic
//! and no second authorization surface. `list_tools` Cedar-filters the set;
//! `call_tool` enforces the same gate (deny ≡ unknown tool) then dispatches.

use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use rmcp::model::{CallToolResult, Content, JsonObject, Tool, ToolAnnotations};
use rmcp::service::RequestContext;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    AppState, Authz, GraphHandle, PolicyAction, PolicyRequest, ResolvedActor, api, authorize,
    do_branch_create, do_branch_delete, do_branch_merge, do_branches_list, do_commit_show,
    do_commits_list, do_graphs_list, do_ingest, do_schema_apply, do_schema_get, do_snapshot,
    run_mutate, run_query,
};
use omnigraph::loader::LoadMode;

/// Per-request context for a tool call. `state` comes from the shared handler;
/// `handle` (per-graph) and `actor` are read back out of the request extensions
/// that the `resolve_graph_handle` / `require_bearer_auth` middleware attached
/// before rmcp ran (RFC-003 §5.8 — the handler consumes a resolved actor and
/// branches on nothing about how it was verified).
pub(crate) struct ToolCx {
    pub state: AppState,
    pub handle: Option<Arc<GraphHandle>>,
    pub actor: Option<ResolvedActor>,
}

impl ToolCx {
    fn graph(&self) -> Result<&Arc<GraphHandle>, McpError> {
        self.handle.as_ref().ok_or_else(|| {
            McpError::internal_error("graph handle missing from MCP request context", None)
        })
    }

    fn actor_ref(&self) -> Option<&ResolvedActor> {
        self.actor.as_ref()
    }
}

/// Pull the per-request actor + graph handle out of the `http::request::Parts`
/// rmcp threads into `RequestContext.extensions`.
pub(crate) fn resolve_cx(
    state: &AppState,
    ctx: &RequestContext<RoleServer>,
) -> Result<ToolCx, McpError> {
    let parts = ctx
        .extensions
        .get::<axum::http::request::Parts>()
        .ok_or_else(|| {
            McpError::internal_error("request parts missing from MCP request context", None)
        })?;
    Ok(ToolCx {
        state: state.clone(),
        handle: parts.extensions.get::<Arc<GraphHandle>>().cloned(),
        actor: parts.extensions.get::<ResolvedActor>().cloned(),
    })
}

/// Which Cedar entity an action's gate binds to.
enum Gate {
    /// Per-graph action gated against `handle.policy`.
    Graph(PolicyAction),
    /// Server-scoped action gated against `state.server_policy`.
    Server(PolicyAction),
    /// No Cedar gate (liveness/version).
    None,
}

/// The built-in tool set. One variant per RFC-003 §5.2 tool; the dynamic
/// stored-query tools are a separate populator (Phase 4).
#[derive(Clone, Copy)]
pub(crate) enum Builtin {
    Health,
    Snapshot,
    SchemaGet,
    BranchesList,
    CommitsList,
    CommitsGet,
    GraphsList,
    Query,
    Mutate,
    Ingest,
    BranchesCreate,
    BranchesDelete,
    BranchesMerge,
    SchemaApply,
}

impl Builtin {
    pub(crate) fn all() -> &'static [Builtin] {
        use Builtin::*;
        &[
            Health,
            Snapshot,
            SchemaGet,
            BranchesList,
            CommitsList,
            CommitsGet,
            GraphsList,
            Query,
            Mutate,
            Ingest,
            BranchesCreate,
            BranchesDelete,
            BranchesMerge,
            SchemaApply,
        ]
    }

    pub(crate) fn from_name(name: &str) -> Option<Builtin> {
        Builtin::all().iter().copied().find(|b| b.name() == name)
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Builtin::Health => "health",
            Builtin::Snapshot => "snapshot",
            Builtin::SchemaGet => "schema_get",
            Builtin::BranchesList => "branches_list",
            Builtin::CommitsList => "commits_list",
            Builtin::CommitsGet => "commits_get",
            Builtin::GraphsList => "graphs_list",
            Builtin::Query => "query",
            Builtin::Mutate => "mutate",
            Builtin::Ingest => "ingest",
            Builtin::BranchesCreate => "branches_create",
            Builtin::BranchesDelete => "branches_delete",
            Builtin::BranchesMerge => "branches_merge",
            Builtin::SchemaApply => "schema_apply",
        }
    }

    /// The MCP `Tool` descriptor (name, description, JSON-Schema input).
    pub(crate) fn descriptor(self) -> Tool {
        let (description, schema) = match self {
            Builtin::Health => (
                "Liveness + server version. No graph access.",
                json!({"type": "object", "additionalProperties": false}),
            ),
            Builtin::Snapshot => (
                "Read a branch's current snapshot: manifest version plus per-table metadata. Read-only.",
                json!({
                    "type": "object",
                    "properties": {"branch": {"type": "string", "description": "Branch to read (default `main`)."}},
                    "additionalProperties": false
                }),
            ),
            Builtin::SchemaGet => (
                "Return the graph's `.pg` schema source. Read-only.",
                json!({"type": "object", "additionalProperties": false}),
            ),
            Builtin::BranchesList => (
                "List all branch names (sorted). Read-only.",
                json!({"type": "object", "additionalProperties": false}),
            ),
            Builtin::CommitsList => (
                "List commits, optionally filtered to one branch (most recent first). Read-only.",
                json!({
                    "type": "object",
                    "properties": {"branch": {"type": "string", "description": "Filter to this branch; omit for all."}},
                    "additionalProperties": false
                }),
            ),
            Builtin::CommitsGet => (
                "Get a single commit's metadata by id. Read-only.",
                json!({
                    "type": "object",
                    "properties": {"commit_id": {"type": "string"}},
                    "required": ["commit_id"],
                    "additionalProperties": false
                }),
            ),
            Builtin::GraphsList => (
                "List the graphs registered with this server (multi-graph mode only).",
                json!({"type": "object", "additionalProperties": false}),
            ),
            Builtin::Query => (
                "Run an ad-hoc read-only GQ query. Mutations are rejected — use `mutate`.",
                json!({
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "GQ query source."},
                        "branch": {"type": "string", "description": "Branch to read (default `main`)."},
                        "snapshot": {"type": "string", "description": "Snapshot id to read (mutually exclusive with `branch`)."},
                        "params": {"type": "object", "description": "Named query parameters."}
                    },
                    "required": ["source"],
                    "additionalProperties": false
                }),
            ),
            Builtin::Mutate => (
                "Run an ad-hoc GQ mutation (insert/update/delete) on a branch. Writes are branchable and policy-gated.",
                json!({
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "GQ mutation source."},
                        "branch": {"type": "string", "description": "Branch to write (default `main`)."},
                        "params": {"type": "object", "description": "Named query parameters."}
                    },
                    "required": ["source"],
                    "additionalProperties": false
                }),
            ),
            Builtin::Ingest => (
                "Bulk-ingest NDJSON into a branch. `mode` controls existing rows: merge (upsert, default), append, or overwrite (replaces table contents). Creates the branch from `from` if absent.",
                json!({
                    "type": "object",
                    "properties": {
                        "data": {"type": "string", "description": "NDJSON, one record per line."},
                        "mode": {"type": "string", "enum": ["merge", "append", "overwrite"], "description": "Default `merge`."},
                        "branch": {"type": "string", "description": "Branch to ingest into (default `main`)."},
                        "from": {"type": "string", "description": "Parent branch to fork from when `branch` does not exist (default `main`)."}
                    },
                    "required": ["data"],
                    "additionalProperties": false
                }),
            ),
            Builtin::BranchesCreate => (
                "Create a branch, forking `name` off `from` (default `main`). Additive — shares parent data until written.",
                json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "New branch name."},
                        "from": {"type": "string", "description": "Parent branch (default `main`)."}
                    },
                    "required": ["name"],
                    "additionalProperties": false
                }),
            ),
            Builtin::BranchesDelete => (
                "Delete a branch pointer. Irreversible.",
                json!({
                    "type": "object",
                    "properties": {"branch": {"type": "string", "description": "Branch to delete."}},
                    "required": ["branch"],
                    "additionalProperties": false
                }),
            ),
            Builtin::BranchesMerge => (
                "Merge `source` into `target` (default `main`). Returns the outcome (already_up_to_date / fast_forward / merged) or a conflict; the target is unchanged on conflict.",
                json!({
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "Branch to merge from."},
                        "target": {"type": "string", "description": "Branch to merge into (default `main`)."}
                    },
                    "required": ["source"],
                    "additionalProperties": false
                }),
            ),
            Builtin::SchemaApply => (
                "Apply a schema migration. Diffs `schema_source` against the current schema and applies the steps. Destructive: some steps drop data — set `allow_data_loss` to permit those.",
                json!({
                    "type": "object",
                    "properties": {
                        "schema_source": {"type": "string", "description": "The target `.pg` schema source."},
                        "allow_data_loss": {"type": "boolean", "description": "Permit data-dropping steps (default false)."}
                    },
                    "required": ["schema_source"],
                    "additionalProperties": false
                }),
            ),
        };
        Tool::new(self.name(), description, schema_object(schema)).with_annotations(self.annotations())
    }

    /// MCP behavior hints. rmcp defaults `destructive_hint` and `open_world_hint`
    /// to `true`, so every field is set explicitly. The engine operates on its
    /// own datasets, so `open_world` is always `false`.
    fn annotations(self) -> ToolAnnotations {
        let read = || ToolAnnotations::new().read_only(true).open_world(false);
        let additive = || {
            ToolAnnotations::new()
                .read_only(false)
                .destructive(false)
                .open_world(false)
        };
        let destructive = || {
            ToolAnnotations::new()
                .read_only(false)
                .destructive(true)
                .open_world(false)
        };
        match self {
            Builtin::Health
            | Builtin::Snapshot
            | Builtin::SchemaGet
            | Builtin::BranchesList
            | Builtin::CommitsList
            | Builtin::CommitsGet
            | Builtin::GraphsList
            | Builtin::Query => read(),
            Builtin::BranchesCreate => additive(),
            Builtin::Mutate
            | Builtin::Ingest
            | Builtin::BranchesDelete
            | Builtin::BranchesMerge
            | Builtin::SchemaApply => destructive(),
        }
    }

    /// The Cedar gate for list-time visibility. Uses `branch: None`; the actual
    /// `do_*` / `run_*` call re-authorizes with the real branch, so for
    /// branch-scoped policies the listed set is a best-effort approximation and
    /// `call_tool` is the authoritative gate (RFC-003 §5.4 R7 caveat).
    fn gate(self) -> Gate {
        match self {
            Builtin::Health => Gate::None,
            Builtin::Snapshot
            | Builtin::SchemaGet
            | Builtin::BranchesList
            | Builtin::CommitsList
            | Builtin::CommitsGet
            | Builtin::Query => Gate::Graph(PolicyAction::Read),
            Builtin::Mutate | Builtin::Ingest => Gate::Graph(PolicyAction::Change),
            Builtin::BranchesCreate => Gate::Graph(PolicyAction::BranchCreate),
            Builtin::BranchesDelete => Gate::Graph(PolicyAction::BranchDelete),
            Builtin::BranchesMerge => Gate::Graph(PolicyAction::BranchMerge),
            Builtin::SchemaApply => Gate::Graph(PolicyAction::SchemaApply),
            Builtin::GraphsList => Gate::Server(PolicyAction::GraphList),
        }
    }

    async fn call(self, cx: &ToolCx, args: &JsonObject) -> Result<CallToolResult, McpError> {
        match self {
            Builtin::Health => ok_json(&json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
            })),
            Builtin::Snapshot => {
                let branch = opt_str(args, "branch").unwrap_or_else(|| "main".to_string());
                to_tool(do_snapshot(cx.graph()?, cx.actor_ref(), branch).await)
            }
            Builtin::SchemaGet => to_tool(do_schema_get(cx.graph()?, cx.actor_ref()).await),
            Builtin::BranchesList => to_tool(do_branches_list(cx.graph()?, cx.actor_ref()).await),
            Builtin::CommitsList => to_tool(
                do_commits_list(cx.graph()?, cx.actor_ref(), opt_str(args, "branch")).await,
            ),
            Builtin::CommitsGet => {
                let commit_id = req_str(args, "commit_id")?;
                to_tool(do_commit_show(cx.graph()?, cx.actor_ref(), &commit_id).await)
            }
            Builtin::GraphsList => to_tool(do_graphs_list(&cx.state, cx.actor_ref()).await),
            Builtin::Query => {
                let source = req_str(args, "source")?;
                let result = run_query(
                    Arc::clone(cx.graph()?),
                    cx.actor_ref(),
                    &source,
                    None,
                    args.get("params"),
                    opt_str(args, "branch"),
                    opt_str(args, "snapshot"),
                    true,
                )
                .await;
                match result {
                    Ok((name, target, qr)) => ok_json(&api::read_output(name, &target, qr)),
                    Err(err) => Ok(api_error_to_tool(err)),
                }
            }
            Builtin::Mutate => {
                let source = req_str(args, "source")?;
                let branch = opt_str(args, "branch").unwrap_or_else(|| "main".to_string());
                to_tool(
                    run_mutate(
                        cx.state.clone(),
                        Arc::clone(cx.graph()?),
                        cx.actor_ref(),
                        &source,
                        None,
                        args.get("params"),
                        branch,
                    )
                    .await,
                )
            }
            Builtin::Ingest => {
                let data = req_str(args, "data")?;
                let mode = match args.get("mode") {
                    Some(value) => serde_json::from_value::<LoadMode>(value.clone())
                        .map_err(|err| McpError::invalid_params(format!("invalid `mode`: {err}"), None))?,
                    None => LoadMode::Merge,
                };
                let branch = opt_str(args, "branch").unwrap_or_else(|| "main".to_string());
                let from = opt_str(args, "from").unwrap_or_else(|| "main".to_string());
                to_tool(
                    do_ingest(
                        &cx.state,
                        cx.graph()?,
                        cx.actor_ref(),
                        &data,
                        mode,
                        branch,
                        from,
                    )
                    .await,
                )
            }
            Builtin::BranchesCreate => {
                let name = req_str(args, "name")?;
                let from = opt_str(args, "from").unwrap_or_else(|| "main".to_string());
                to_tool(do_branch_create(&cx.state, cx.graph()?, cx.actor_ref(), name, from).await)
            }
            Builtin::BranchesDelete => {
                let branch = req_str(args, "branch")?;
                to_tool(do_branch_delete(&cx.state, cx.graph()?, cx.actor_ref(), branch).await)
            }
            Builtin::BranchesMerge => {
                let source = req_str(args, "source")?;
                let target = opt_str(args, "target").unwrap_or_else(|| "main".to_string());
                to_tool(do_branch_merge(&cx.state, cx.graph()?, cx.actor_ref(), source, target).await)
            }
            Builtin::SchemaApply => {
                let schema_source = req_str(args, "schema_source")?;
                let allow_data_loss = args
                    .get("allow_data_loss")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                to_tool(
                    do_schema_apply(
                        &cx.state,
                        cx.graph()?,
                        cx.actor_ref(),
                        &schema_source,
                        allow_data_loss,
                    )
                    .await,
                )
            }
        }
    }
}

/// Is `tool` callable by this actor (argument-independent gate)? Used by both
/// `list_tools` (visibility) and `call_tool` (deny ≡ unknown masking). `Err` is
/// an operational failure (propagates as a JSON-RPC error); `Ok(false)` hides.
pub(crate) fn is_visible(tool: Builtin, cx: &ToolCx) -> Result<bool, McpError> {
    let (policy, action) = match tool.gate() {
        Gate::None => return Ok(true),
        Gate::Graph(action) => (cx.handle.as_ref().and_then(|h| h.policy.as_deref()), action),
        Gate::Server(action) => (cx.state.server_policy.as_deref(), action),
    };
    let request = PolicyRequest {
        action,
        branch: None,
        target_branch: None,
    };
    match authorize(cx.actor_ref(), policy, request) {
        Ok(Authz::Allowed) => Ok(true),
        Ok(Authz::Denied(_)) => Ok(false),
        Err(err) => Err(api_operational_error(err)),
    }
}

/// Dispatch a `tools/call` to a built-in. The caller has already enforced the
/// visibility gate (deny ≡ unknown), so this only parses args and runs.
pub(crate) async fn dispatch(
    tool: Builtin,
    cx: &ToolCx,
    args: &JsonObject,
) -> Result<CallToolResult, McpError> {
    tool.call(cx, args).await
}

// ---- helpers ---------------------------------------------------------------

fn schema_object(value: Value) -> Arc<JsonObject> {
    Arc::new(
        value
            .as_object()
            .expect("tool input schema literal must be a JSON object")
            .clone(),
    )
}

fn opt_str(args: &JsonObject, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

fn req_str(args: &JsonObject, key: &str) -> Result<String, McpError> {
    opt_str(args, key)
        .ok_or_else(|| McpError::invalid_params(format!("missing required argument `{key}`"), None))
}

fn ok_json<T: Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|err| McpError::internal_error(format!("serialize tool result: {err}"), None))?;
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

/// Map a `do_*` result into a tool result: `Ok` → JSON success; `Err` → either a
/// JSON-RPC error (operational 5xx) or an `isError` tool result (4xx/409, which
/// the model can self-correct on — the 2025-11-25 SEP-1303 split).
fn to_tool<T: Serialize>(result: Result<T, crate::ApiError>) -> Result<CallToolResult, McpError> {
    match result {
        Ok(value) => ok_json(&value),
        Err(err) => Ok(api_error_to_tool(err)),
    }
}

fn api_error_to_tool(err: crate::ApiError) -> CallToolResult {
    CallToolResult::error(vec![Content::text(err.message_str().to_string())])
}

/// 5xx engine/policy failures are operational → surface as a JSON-RPC error so
/// the client (not the model) sees them; anything else that reaches here is
/// treated as internal too (callers route 4xx through `api_error_to_tool`).
fn api_operational_error(err: crate::ApiError) -> McpError {
    use axum::http::StatusCode;
    match err.status_code() {
        StatusCode::UNAUTHORIZED => McpError::invalid_request(err.message_str().to_string(), None),
        _ => McpError::internal_error(err.message_str().to_string(), None),
    }
}
