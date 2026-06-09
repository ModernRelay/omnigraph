//! Built-in MCP tools — RFC-003 §5.2.
//!
//! Each tool reuses the EXACT `do_*` / `run_query` / `run_mutate` path (and the
//! exact Cedar action) its HTTP route uses, so there is no new business logic
//! and no second authorization surface. `list_tools` Cedar-filters the set;
//! `call_tool` enforces the same gate (deny ≡ unknown tool) then dispatches.

use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use rmcp::model::{
    Annotated, CallToolResult, Content, JsonObject, RawResource, ReadResourceResult, Resource,
    ResourceContents, Tool, ToolAnnotations,
};
use rmcp::service::RequestContext;
use serde::Serialize;
use serde_json::{Map, Value, json};

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
    gate_allowed(&tool.gate(), cx)
}

/// Evaluate a gate against the actor's Cedar policy (the right policy source per
/// scope). Uses `branch: None`; the actual `do_*`/`run_*` call re-authorizes
/// with the real branch. Shared by tool visibility and resource visibility.
fn gate_allowed(gate: &Gate, cx: &ToolCx) -> Result<bool, McpError> {
    let (policy, action) = match gate {
        Gate::None => return Ok(true),
        Gate::Graph(action) => (cx.handle.as_ref().and_then(|h| h.policy.as_deref()), *action),
        Gate::Server(action) => (cx.state.server_policy.as_deref(), *action),
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

// ---- stored-query tools (RFC-003 §5.1 / §5.3) ------------------------------
//
// One MCP tool per `mcp.expose` entry in the graph's stored-query registry,
// projected from the same `query_catalog_entry` the `GET /queries` catalog
// uses. Double-gated like `POST /queries/{name}`: the coarse outer
// `InvokeQuery` action gates reaching the tool (all exposed queries on the
// graph, or none — per-query scope is deferred), then the inner `Read`/`Change`
// in `run_query`/`run_mutate` gates the body.

/// Is the outer `InvokeQuery` gate open for this actor? `Err` is operational
/// (propagates); `Ok(false)` hides every stored-query tool.
pub(crate) fn stored_invoke_visible(cx: &ToolCx) -> Result<bool, McpError> {
    let policy = cx.handle.as_ref().and_then(|h| h.policy.as_deref());
    let request = PolicyRequest {
        action: PolicyAction::InvokeQuery,
        branch: None,
        target_branch: None,
    };
    match authorize(cx.actor_ref(), policy, request) {
        Ok(Authz::Allowed) => Ok(true),
        Ok(Authz::Denied(_)) => Ok(false),
        Err(err) => Err(api_operational_error(err)),
    }
}

/// Descriptors for the graph's exposed stored queries. A name that collides
/// with a built-in is skipped (built-ins win — avoids a duplicate tool name in
/// the catalog).
pub(crate) fn stored_descriptors(cx: &ToolCx) -> Vec<Tool> {
    let Some(registry) = cx.handle.as_ref().and_then(|h| h.queries.as_ref()) else {
        return Vec::new();
    };
    registry
        .iter()
        .filter(|q| q.expose)
        .filter_map(|q| {
            let entry = api::query_catalog_entry(q);
            if Builtin::from_name(&entry.tool_name).is_some() {
                return None;
            }
            Some(stored_descriptor(&entry))
        })
        .collect()
}

/// Does this graph expose a stored-query tool named `name` (not shadowed by a
/// built-in)?
pub(crate) fn is_stored_tool(cx: &ToolCx, name: &str) -> bool {
    if Builtin::from_name(name).is_some() {
        return false;
    }
    cx.handle
        .as_ref()
        .and_then(|h| h.queries.as_ref())
        .is_some_and(|reg| reg.iter().any(|q| q.expose && q.effective_tool_name() == name))
}

/// Dispatch an exposed stored-query tool. The caller has already enforced the
/// outer `InvokeQuery` gate (deny ≡ unknown); this runs the registry source
/// through `run_query` / `run_mutate`, whose inner `Read` / `Change` gate the
/// body.
pub(crate) async fn call_stored_tool(
    cx: &ToolCx,
    name: &str,
    args: &JsonObject,
) -> Result<CallToolResult, McpError> {
    let handle = cx.graph()?;
    let unknown = || McpError::invalid_params(format!("unknown tool: {name}"), None);
    let registry = handle.queries.as_ref().ok_or_else(unknown)?;
    let stored = registry
        .iter()
        .find(|q| q.expose && q.effective_tool_name() == name)
        .ok_or_else(unknown)?;
    let source = Arc::clone(&stored.source);
    let query_name = stored.name.clone();
    let mutation = stored.is_mutation();

    // The query parameters are top-level tool args; `branch`/`snapshot` are
    // invocation knobs. Peel the knobs off and pass the rest as the params
    // object `run_query` / `run_mutate` expect.
    let mut params = args.clone();
    let branch = params
        .remove("branch")
        .and_then(|v| v.as_str().map(str::to_string));
    let snapshot = params
        .remove("snapshot")
        .and_then(|v| v.as_str().map(str::to_string));
    let params = Value::Object(params);

    if mutation {
        let branch = branch.unwrap_or_else(|| "main".to_string());
        to_tool(
            run_mutate(
                cx.state.clone(),
                Arc::clone(handle),
                cx.actor_ref(),
                &source,
                Some(&query_name),
                Some(&params),
                branch,
            )
            .await,
        )
    } else {
        match run_query(
            Arc::clone(handle),
            cx.actor_ref(),
            &source,
            Some(&query_name),
            Some(&params),
            branch,
            snapshot,
            true,
        )
        .await
        {
            Ok((selected, target, qr)) => ok_json(&api::read_output(selected, &target, qr)),
            Err(err) => Ok(api_error_to_tool(err)),
        }
    }
}

fn stored_descriptor(entry: &api::QueryCatalogEntry) -> Tool {
    let mut props = Map::new();
    let mut required: Vec<Value> = Vec::new();
    for p in &entry.params {
        props.insert(p.name.clone(), param_schema(p));
        if !p.nullable {
            required.push(Value::String(p.name.clone()));
        }
    }
    props.insert(
        "branch".to_string(),
        json!({"type": "string", "description": "Branch to target (default `main`)."}),
    );
    if !entry.mutation {
        props.insert(
            "snapshot".to_string(),
            json!({"type": "string", "description": "Snapshot id to read (mutually exclusive with `branch`)."}),
        );
    }
    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(props));
    if !required.is_empty() {
        schema.insert("required".to_string(), Value::Array(required));
    }
    schema.insert("additionalProperties".to_string(), json!(false));

    let mut description = entry
        .description
        .clone()
        .unwrap_or_else(|| format!("Stored query `{}`.", entry.name));
    if let Some(instruction) = &entry.instruction {
        description.push_str("\n\n");
        description.push_str(instruction);
    }
    let annotations = if entry.mutation {
        ToolAnnotations::new()
            .read_only(false)
            .destructive(true)
            .open_world(false)
    } else {
        ToolAnnotations::new().read_only(true).open_world(false)
    };
    Tool::new(entry.tool_name.clone(), description, Arc::new(schema)).with_annotations(annotations)
}

/// Map a stored-query parameter to its JSON Schema (RFC-003 §5.3).
fn param_schema(p: &api::ParamDescriptor) -> Value {
    match p.kind {
        api::ParamKind::List => {
            let item = p
                .item_kind
                .map(scalar_schema)
                .unwrap_or_else(|| json!({"type": "string"}));
            json!({"type": "array", "items": item})
        }
        other => kind_schema(other, p.vector_dim),
    }
}

fn scalar_schema(kind: api::ParamKind) -> Value {
    kind_schema(kind, None)
}

fn kind_schema(kind: api::ParamKind, vector_dim: Option<u32>) -> Value {
    use api::ParamKind::*;
    match kind {
        String => json!({"type": "string"}),
        Bool => json!({"type": "boolean"}),
        Int => json!({"type": "integer"}),
        // JSON numbers lose precision past 2^53, so i64/u64 ride as strings.
        BigInt => json!({"type": "string", "pattern": "^-?\\d+$"}),
        Float => json!({"type": "number"}),
        Date => json!({"type": "string", "format": "date"}),
        DateTime => json!({"type": "string", "format": "date-time"}),
        Blob => json!({"type": "string", "contentEncoding": "base64"}),
        Vector => {
            let dim = vector_dim.unwrap_or(0);
            json!({"type": "array", "items": {"type": "number"}, "minItems": dim, "maxItems": dim})
        }
        // The grammar forbids lists/vectors of lists, so a bare List here is
        // unreachable; fall back to an untyped array.
        List => json!({"type": "array"}),
    }
}

// ---- resources (RFC-003 §5.5) ----------------------------------------------
//
// Three read-only resources, each gated by the same action as its tool/route:
// the schema source, the branch list, and (multi-graph) the graph registry. A
// locked-down agent denied the gate never sees the resource (list-filtered) and
// a read is masked as "unknown resource" (deny ≡ missing).

const RESOURCE_SCHEMA: &str = "omnigraph://schema";
const RESOURCE_BRANCHES: &str = "omnigraph://branches";
const RESOURCE_GRAPHS: &str = "omnigraph://graphs";

struct ResourceDef {
    uri: &'static str,
    name: &'static str,
    description: &'static str,
    mime: &'static str,
    gate: Gate,
}

fn resource_defs() -> [ResourceDef; 3] {
    [
        ResourceDef {
            uri: RESOURCE_SCHEMA,
            name: "schema",
            description: "The graph's `.pg` schema source.",
            mime: "text/plain",
            gate: Gate::Graph(PolicyAction::Read),
        },
        ResourceDef {
            uri: RESOURCE_BRANCHES,
            name: "branches",
            description: "The graph's branch names, as JSON.",
            mime: "application/json",
            gate: Gate::Graph(PolicyAction::Read),
        },
        ResourceDef {
            uri: RESOURCE_GRAPHS,
            name: "graphs",
            description: "The graphs registered with this server, as JSON (multi-graph mode).",
            mime: "application/json",
            gate: Gate::Server(PolicyAction::GraphList),
        },
    ]
}

/// The resources this actor may read (Cedar-filtered, same gate as the matching
/// tool).
pub(crate) fn list_resources(cx: &ToolCx) -> Result<Vec<Resource>, McpError> {
    let mut out = Vec::new();
    for def in resource_defs() {
        if gate_allowed(&def.gate, cx)? {
            let mut raw = RawResource::new(def.uri, def.name);
            raw.description = Some(def.description.to_string());
            raw.mime_type = Some(def.mime.to_string());
            out.push(Annotated::new(raw, None));
        }
    }
    Ok(out)
}

/// Read a resource by URI: enforce the gate (deny ≡ unknown resource), then
/// return the schema source / branch list / graph registry as text.
pub(crate) async fn read_resource(cx: &ToolCx, uri: &str) -> Result<ReadResourceResult, McpError> {
    let unknown = || McpError::invalid_params(format!("unknown resource: {uri}"), None);
    let def = resource_defs()
        .into_iter()
        .find(|d| d.uri == uri)
        .ok_or_else(unknown)?;
    if !gate_allowed(&def.gate, cx)? {
        return Err(unknown());
    }
    let text = match uri {
        RESOURCE_SCHEMA => {
            do_schema_get(cx.graph()?, cx.actor_ref())
                .await
                .map_err(api_operational_error)?
                .schema_source
        }
        RESOURCE_BRANCHES => {
            let out = do_branches_list(cx.graph()?, cx.actor_ref())
                .await
                .map_err(api_operational_error)?;
            json_text(&out)?
        }
        RESOURCE_GRAPHS => {
            let out = do_graphs_list(&cx.state, cx.actor_ref())
                .await
                .map_err(api_operational_error)?;
            json_text(&out)?
        }
        _ => return Err(unknown()),
    };
    Ok(ReadResourceResult::new(vec![ResourceContents::text(
        text, uri,
    )]))
}

fn json_text<T: Serialize>(value: &T) -> Result<String, McpError> {
    serde_json::to_string_pretty(value)
        .map_err(|err| McpError::internal_error(format!("serialize resource: {err}"), None))
}
