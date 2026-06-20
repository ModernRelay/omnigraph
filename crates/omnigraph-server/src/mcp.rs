//! The server's MCP backend: projects built-in operations and the per-graph
//! stored-query registry as MCP tools + resources, each Cedar-gated by the same
//! `authorize` path the REST routes use. Adds **no business logic** — every
//! tool delegates to the same engine/handler functions the routes call.
//!
//! The `rmcp` transport lives in `omnigraph-mcp`; this module fills its
//! `McpBackend` seam. It reads the request's resolved actor + graph handle out
//! of `parts.extensions` (injected by `require_bearer_auth` /
//! `resolve_graph_handle` before rmcp sees the request).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use omnigraph::db::{ReadTarget, SchemaApplyOptions};
use omnigraph_mcp::{
    CallToolResult, Content, Implementation, JsonObject, McpBackend, McpError, RawResource,
    ReadResourceResult, Resource, ResourceContents, ServerCapabilities, ServerInfo, Tool,
    ToolAnnotations,
};

use crate::handlers::{
    Authz, authorize, authorize_any_branch, authorize_request, invoke_query_request, run_ingest,
    run_mutate, run_query,
};
use crate::queries::{QueryRegistry, StoredQuery};
use crate::{
    ApiError, AppState, GraphHandle, INGEST_REQUEST_BODY_LIMIT_BYTES, PolicyAction, PolicyRequest,
    ResolvedActor, SERVER_VERSION, api, validate_registry_against_catalog,
};

const SCHEMA_URI: &str = "omnigraph://schema";
const BRANCHES_URI: &str = "omnigraph://branches";

/// `auto` projection flips from one-tool-per-query to the discovery+execute
/// meta pair at or above this many exposed queries (model accuracy degrades as
/// a single client's tool count climbs past a few dozen).
const STORED_QUERY_AUTO_THRESHOLD: usize = 24;
const STORED_QUERY_LIST_TOOL: &str = "stored_query_list";
const STORED_QUERY_RUN_TOOL: &str = "stored_query_run";

/// The closed set of built-in tool names — reserved graph-wide. Folded into the
/// stored-query registry's uniqueness check (`QueryRegistry::from_specs`) so a
/// stored query that shadows a built-in fails loudly at load instead of being
/// silently un-served. Kept in sync with `Builtin::ALL` by
/// `builtin_tool_names_match_enum`.
pub(crate) const BUILTIN_TOOL_NAMES: &[&str] = &[
    "graph_health",
    "graph_query",
    "graph_snapshot",
    "schema_get",
    "branch_list",
    "commit_list",
    "commit_get",
    "graph_mutate",
    "graph_load",
    "branch_create",
    "branch_delete",
    "branch_merge",
    "schema_apply",
];

/// Max MCP tool-name length. Matches the constraint major MCP clients enforce
/// (Anthropic/OpenAI cap tool names at 64 chars over `[A-Za-z0-9_-]`).
const MAX_TOOL_NAME_LEN: usize = 64;

/// Every tool name the MCP surface itself generates and therefore reserves
/// graph-wide: the built-ins **plus** the `meta`-mode discovery/execute pair.
/// A stored query whose effective tool name claims one of these is refused at
/// load (`QueryRegistry::from_specs`) — otherwise it would be silently shadowed
/// (a built-in always wins at dispatch; the meta pair takes over the name once
/// the catalog crosses [`STORED_QUERY_AUTO_THRESHOLD`], a silent
/// threshold-crossing meaning change). Single source of "names the surface
/// emits", so the reservation can't drift from what's generated.
pub(crate) fn reserved_tool_names() -> impl Iterator<Item = &'static str> {
    BUILTIN_TOOL_NAMES
        .iter()
        .copied()
        .chain([STORED_QUERY_LIST_TOOL, STORED_QUERY_RUN_TOOL])
}

/// The MCP tool-name contract every published name must satisfy: non-empty,
/// `[A-Za-z0-9_-]`, at most [`MAX_TOOL_NAME_LEN`] — the intersection of what
/// MCP clients accept. The `.gq` grammar already constrains query *names* to a
/// subset of this; this guards the free-form `@mcp(tool_name: …)` override so a
/// malformed name fails loudly at load instead of producing a tool a client
/// silently rejects at call time. Returns the operator-facing reason on failure.
pub(crate) fn validate_mcp_tool_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("MCP tool name must not be empty".to_string());
    }
    if name.len() > MAX_TOOL_NAME_LEN {
        return Err(format!(
            "MCP tool name '{name}' exceeds {MAX_TOOL_NAME_LEN} characters"
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "MCP tool name '{name}' contains an unsupported character '{bad}' \
             (allowed: ASCII letters, digits, '_', '-')"
        ));
    }
    Ok(())
}

/// The server's thin wrapper over `omnigraph_mcp::mcp_router`: derives the
/// fail-closed host policy from the bound socket and passes the 32 MiB body
/// limit (`/load` parity). Merged into `per_graph_protected` in `build_app`.
pub(crate) fn mcp_router(state: AppState) -> axum::Router<AppState> {
    let host_policy = state.mcp_host_policy();
    omnigraph_mcp::mcp_router::<_, AppState>(
        OmnigraphMcpBackend::new(state),
        INGEST_REQUEST_BODY_LIMIT_BYTES,
        host_policy,
    )
}

#[derive(Clone)]
pub(crate) struct OmnigraphMcpBackend {
    state: AppState,
}

impl OmnigraphMcpBackend {
    pub(crate) fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Pull the resolved actor (absent in `--unauthenticated` mode) and the
    /// graph handle out of the request extensions. The handle is always present
    /// on a `/graphs/{id}/mcp` route (injected by `resolve_graph_handle`); its
    /// absence is an internal wiring error.
    fn ctx<'a>(
        &self,
        parts: &'a http::request::Parts,
    ) -> Result<(Option<&'a ResolvedActor>, &'a Arc<GraphHandle>), McpError> {
        let actor = parts.extensions.get::<ResolvedActor>();
        let handle = parts
            .extensions
            .get::<Arc<GraphHandle>>()
            .ok_or_else(|| McpError::internal_error("graph handle missing from request extensions", None))?;
        Ok((actor, handle))
    }
}

#[async_trait]
impl McpBackend for OmnigraphMcpBackend {
    fn server_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new("omnigraph", SERVER_VERSION))
        .with_instructions(
            "Omnigraph graph database. Every tool operates on a single graph — the one in \
             the URL path — so the graph id never appears in tool arguments or output.",
        )
    }

    async fn list_tools(&self, parts: &http::request::Parts) -> Result<Vec<Tool>, McpError> {
        let (actor, handle) = self.ctx(parts)?;
        let mut tools = Vec::new();
        for builtin in Builtin::ALL {
            // Visibility is derived from what the tool's `call` authorizes
            // (`list_gate`): a fixed-scope tool is gated on that exact request
            // (faithful, and consistent with its resource twin); a
            // caller-chosen-branch tool is shown iff the actor could invoke it on
            // *some* branch (a relaxation — never hide a callable tool). The
            // per-call gate inside `call` stays authoritative.
            let visible = match builtin.list_gate() {
                ListGate::Always => true,
                ListGate::Exact(request) => allowed(actor, handle, request)?,
                ListGate::AnyBranch(action) => {
                    authorize_any_branch(actor, handle.policy.as_deref(), action).map_err(api_to_mcp)?
                }
            };
            if visible {
                tools.push(builtin.descriptor());
            }
        }
        // Stored queries are graph-scoped behind one coarse `invoke_query` gate
        // (the catalog can't be probed without it). Projection mode is chosen
        // from this graph's exposed-query count.
        if let Some(registry) = handle.queries.as_deref() {
            let can_invoke = allowed(actor, handle, invoke_query_request())?;
            if can_invoke {
                match stored_mode(registry) {
                    StoredMode::PerQuery => {
                        for query in registry.exposed() {
                            tools.push(stored_query_tool(query));
                        }
                    }
                    StoredMode::Meta => {
                        tools.push(stored_query_list_descriptor());
                        tools.push(stored_query_run_descriptor());
                    }
                }
            }
        }
        Ok(tools)
    }

    async fn call_tool(
        &self,
        parts: &http::request::Parts,
        name: &str,
        args: JsonObject,
    ) -> Result<CallToolResult, McpError> {
        let (actor, handle) = self.ctx(parts)?;

        // 1. Built-in. Its names are a fixed public set, so a denial surfaces as
        //    `isError` via `classify` (no masking needed).
        if let Some(builtin) = Builtin::by_name(name) {
            return classify(builtin.call(&self.state, actor, handle, args).await);
        }

        // 2 & 3. Stored-query paths (the meta pair and per-query tools) share the
        //    coarse `invoke_query` outer gate, deny-masked as an unknown tool so
        //    the stored-query catalog (whose names reveal business logic) can't
        //    be probed. The inner Read/Change gate runs in run_query/run_mutate.
        //    `resolve_stored_tool` is the single membership test: a name is
        //    callable iff `tools/list` would have advertised it (same mode +
        //    `expose` filter), so call and list cannot diverge — the meta pair is
        //    callable only in `Meta` mode and per-query tools only in `PerQuery`.
        if let Some(dispatch) = handle
            .queries
            .as_deref()
            .and_then(|registry| resolve_stored_tool(registry, name))
        {
            match authorize(actor, handle.policy.as_deref(), invoke_query_request()).map_err(api_to_mcp)? {
                Authz::Allowed => {}
                Authz::Denied(_) => return Err(unknown_tool(name)),
            }
            return classify(self.dispatch_stored(actor, handle, name, dispatch, args).await);
        }

        Err(unknown_tool(name))
    }

    async fn list_resources(&self, parts: &http::request::Parts) -> Result<Vec<Resource>, McpError> {
        let (actor, handle) = self.ctx(parts)?;
        if allowed(actor, handle, read_request(None))? {
            Ok(vec![
                resource(SCHEMA_URI, "schema", "This graph's schema as .pg source."),
                resource(BRANCHES_URI, "branches", "Branch names on this graph (JSON)."),
            ])
        } else {
            Ok(Vec::new())
        }
    }

    async fn read_resource(
        &self,
        parts: &http::request::Parts,
        uri: &str,
    ) -> Result<ReadResourceResult, McpError> {
        let (actor, handle) = self.ctx(parts)?;
        if uri != SCHEMA_URI && uri != BRANCHES_URI {
            return Err(McpError::invalid_params(format!("unknown resource: {uri}"), None));
        }
        // Read-gated; a denial masks identically to an unknown URI so the
        // resource set can't be probed. Operational failures stay 5xx.
        match authorize(actor, handle.policy.as_deref(), read_request(None)).map_err(api_to_mcp)? {
            Authz::Allowed => {}
            Authz::Denied(_) => {
                return Err(McpError::invalid_params(format!("unknown resource: {uri}"), None));
            }
        }
        let contents = if uri == SCHEMA_URI {
            ResourceContents::text(handle.engine.schema_source().to_string(), SCHEMA_URI)
        } else {
            let mut branches = handle
                .engine
                .branch_list()
                .await
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            branches.sort();
            let json = serde_json::to_string(&api::BranchListOutput { branches })
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            ResourceContents::text(json, BRANCHES_URI)
        };
        Ok(ReadResourceResult::new(vec![contents]))
    }
}

impl OmnigraphMcpBackend {
    /// Dispatch a stored-query tool call (already resolved by
    /// [`resolve_stored_tool`] and past the `invoke_query` gate).
    /// `stored_query_list` enumerates the exposed catalog; `stored_query_run`
    /// and the per-query tools invoke a query through `run_query`/`run_mutate`
    /// (the inner Read/Change gate). Both invoke paths resolve **exposed-only**,
    /// so a query hidden from the tool list is unreachable even by name.
    async fn dispatch_stored(
        &self,
        actor: Option<&ResolvedActor>,
        handle: &Arc<GraphHandle>,
        name: &str,
        dispatch: StoredDispatch,
        args: JsonObject,
    ) -> Result<CallToolResult, ApiError> {
        let registry = handle
            .queries
            .as_deref()
            .ok_or_else(|| ApiError::not_found("no stored queries for this graph"))?;

        let stored = match dispatch {
            StoredDispatch::List => return stored_query_list(registry, &args),
            StoredDispatch::Run => {
                let query_name = req_str(&args, "name")?;
                registry
                    .exposed_by_name(&query_name)
                    .ok_or_else(|| ApiError::not_found("stored query not found"))?
            }
            // Per-query tool: resolve by effective tool name, exposed-only.
            StoredDispatch::PerQuery => registry
                .exposed()
                .find(|q| q.effective_tool_name() == name)
                .ok_or_else(|| ApiError::not_found("stored query not found"))?,
        };

        self.invoke_stored(
            actor,
            handle,
            stored,
            opt_obj(&args, "params")?,
            opt_str(&args, "branch")?,
            opt_str(&args, "snapshot")?,
        )
        .await
    }

    async fn invoke_stored(
        &self,
        actor: Option<&ResolvedActor>,
        handle: &Arc<GraphHandle>,
        stored: &StoredQuery,
        params: Option<Value>,
        branch: Option<String>,
        snapshot: Option<String>,
    ) -> Result<CallToolResult, ApiError> {
        if stored.is_mutation() {
            if snapshot.is_some() {
                return Err(ApiError::bad_request(
                    "a mutation cannot target a snapshot",
                ));
            }
            let output = run_mutate(
                self.state.clone(),
                Arc::clone(handle),
                actor,
                &stored.source,
                Some(&stored.name),
                params.as_ref(),
                branch.unwrap_or_else(|| "main".to_string()),
            )
            .await?;
            json_result(&api::InvokeStoredQueryResponse::Change(output))
        } else {
            let (selected, target, result) = run_query(
                Arc::clone(handle),
                actor,
                &stored.source,
                Some(&stored.name),
                params.as_ref(),
                branch,
                snapshot,
                true,
            )
            .await?;
            json_result(&api::InvokeStoredQueryResponse::Read(api::read_output(
                selected, &target, result,
            )))
        }
    }
}

// ===== shared helpers =====

/// Map a handler `ApiError` into an MCP tool outcome (SEP-1303): a semantic
/// 4xx (bad params, validation, 404/409) becomes an `isError` tool *result*
/// fed back to the model; an operational 5xx becomes a JSON-RPC protocol error.
fn classify(result: Result<CallToolResult, ApiError>) -> Result<CallToolResult, McpError> {
    match result {
        Ok(out) => Ok(out),
        Err(e) if e.status_code().is_client_error() => {
            Ok(CallToolResult::error(vec![Content::text(e.message_str().to_owned())]))
        }
        Err(e) => Err(McpError::internal_error(e.message_str().to_owned(), None)),
    }
}

/// Map an *authorization* `ApiError` (operational only — 401/500) onto an MCP
/// protocol error. Used on paths where a denial is consumed as `Authz`.
fn api_to_mcp(e: ApiError) -> McpError {
    if e.status_code().is_client_error() {
        McpError::invalid_params(e.message_str().to_owned(), None)
    } else {
        McpError::internal_error(e.message_str().to_owned(), None)
    }
}

fn unknown_tool(name: &str) -> McpError {
    McpError::invalid_params(format!("unknown tool: {name}"), None)
}

/// Argument-independent Cedar check used to filter `tools/list` /
/// `resources/list`. `call_tool` is authoritative; over-showing a branch-scoped
/// grant is the safe direction.
fn allowed(
    actor: Option<&ResolvedActor>,
    handle: &Arc<GraphHandle>,
    request: PolicyRequest,
) -> Result<bool, McpError> {
    let decision = authorize(actor, handle.policy.as_deref(), request).map_err(api_to_mcp)?;
    Ok(matches!(decision, Authz::Allowed))
}

fn read_request(branch: Option<String>) -> PolicyRequest {
    PolicyRequest { action: PolicyAction::Read, branch, target_branch: None }
}

fn actor_id<'a>(actor: Option<&'a ResolvedActor>) -> Option<&'a str> {
    actor.map(|a| a.actor_id.as_ref())
}

fn actor_arc(actor: Option<&ResolvedActor>) -> Arc<str> {
    actor
        .map(|a| Arc::clone(&a.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"))
}

/// Serialize a DTO into a tool result with **structured output** —
/// `structuredContent` (typed returns for code-mode runtimes) plus a text
/// mirror for clients that don't parse it. `outputSchema` declaration is a
/// tracked follow-up (R2: utoipa `ToSchema` → JSON-Schema-2020-12 fidelity).
fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ApiError> {
    let value = serde_json::to_value(value)
        .map_err(|e| ApiError::internal(format!("serialize tool result: {e}")))?;
    Ok(CallToolResult::structured(value))
}

fn opt_str(args: &JsonObject, key: &str) -> Result<Option<String>, ApiError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ApiError::bad_request(format!("'{key}' must be a string"))),
    }
}

fn req_str(args: &JsonObject, key: &str) -> Result<String, ApiError> {
    opt_str(args, key)?.ok_or_else(|| ApiError::bad_request(format!("'{key}' is required")))
}

fn opt_obj(args: &JsonObject, key: &str) -> Result<Option<Value>, ApiError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v @ Value::Object(_)) => Ok(Some(v.clone())),
        Some(_) => Err(ApiError::bad_request(format!("'{key}' must be an object"))),
    }
}

fn from_args<T: serde::de::DeserializeOwned>(args: JsonObject, what: &str) -> Result<T, ApiError> {
    serde_json::from_value(Value::Object(args))
        .map_err(|e| ApiError::bad_request(format!("invalid {what}: {e}")))
}

fn read_only_annotations() -> ToolAnnotations {
    ToolAnnotations::new().read_only(true).open_world(false)
}

fn write_annotations(destructive: bool) -> ToolAnnotations {
    ToolAnnotations::new()
        .read_only(false)
        .destructive(destructive)
        .open_world(false)
}

fn resource(uri: &str, name: &str, description: &str) -> Resource {
    let raw = RawResource {
        uri: uri.to_string(),
        name: name.to_string(),
        title: None,
        description: Some(description.to_string()),
        mime_type: Some("text/plain".to_string()),
        size: None,
        icons: None,
        meta: None,
    };
    Resource::new(raw, None)
}

fn schema(value: Value) -> Arc<JsonObject> {
    Arc::new(value.as_object().cloned().unwrap_or_default())
}

// ===== stored-query projection =====

enum StoredMode {
    PerQuery,
    Meta,
}

fn stored_mode(registry: &QueryRegistry) -> StoredMode {
    let exposed = registry.exposed().count();
    if exposed < STORED_QUERY_AUTO_THRESHOLD {
        StoredMode::PerQuery
    } else {
        StoredMode::Meta
    }
}

/// Which stored-query tool a `call_tool` name resolves to. The membership test
/// for the stored surface: it returns `Some` **only** for a name `tools/list`
/// would have advertised under the same projection, so call and list cannot
/// diverge.
enum StoredDispatch {
    /// `stored_query_list` — Meta projection only.
    List,
    /// `stored_query_run` — Meta projection only.
    Run,
    /// A per-query tool (PerQuery projection only); resolved exposed-only.
    PerQuery,
}

/// Map a `call_tool` name to its stored dispatch, honoring the projection mode
/// and `expose`. In `Meta` mode only the meta pair resolves (per-query names →
/// `None`); in `PerQuery` mode only an exposed per-query tool name resolves (the
/// meta names → `None`). The exact inverse of what [`list_tools`] advertises.
fn resolve_stored_tool(registry: &QueryRegistry, name: &str) -> Option<StoredDispatch> {
    match stored_mode(registry) {
        StoredMode::Meta => match name {
            STORED_QUERY_LIST_TOOL => Some(StoredDispatch::List),
            STORED_QUERY_RUN_TOOL => Some(StoredDispatch::Run),
            _ => None,
        },
        StoredMode::PerQuery => registry
            .exposed()
            .any(|q| q.effective_tool_name() == name)
            .then_some(StoredDispatch::PerQuery),
    }
}

/// Per-query input schema: query params nested under `params` (so a param named
/// `branch`/`snapshot` can't collide with the knobs), plus `branch` and —
/// for reads only — `snapshot`. Each param's schema is the shared
/// `param_json_schema` (locked to the engine coercer).
fn stored_query_input_schema(stored: &StoredQuery) -> Value {
    let mut props = serde_json::Map::new();
    let mut required = Vec::new();
    for param in &stored.decl.params {
        let descriptor = api::param_descriptor(param);
        props.insert(param.name.clone(), api::param_json_schema(&descriptor));
        if !param.nullable {
            required.push(Value::String(param.name.clone()));
        }
    }
    let mut params_schema = json!({
        "type": "object",
        "properties": Value::Object(props),
        "additionalProperties": false
    });
    if !required.is_empty() {
        params_schema["required"] = Value::Array(required);
    }
    let mut top = json!({
        "type": "object",
        "properties": { "params": params_schema, "branch": { "type": "string" } },
        "additionalProperties": false
    });
    if !stored.is_mutation() {
        top["properties"]["snapshot"] = json!({ "type": "string" });
    }
    top
}

fn stored_query_tool(stored: &StoredQuery) -> Tool {
    // The MCP tool description folds `@description` and `@instruction` (the
    // agent-facing "how to use" guidance) into the one description slot MCP
    // tools have. Instruction-only queries still surface their instruction
    // (appended to the fallback base).
    let mut description = stored
        .decl
        .description
        .clone()
        .unwrap_or_else(|| format!("Stored query '{}'.", stored.name));
    if let Some(instruction) = &stored.decl.instruction {
        description.push_str("\n\n");
        description.push_str(instruction);
    }
    let annotations = if stored.is_mutation() {
        write_annotations(true)
    } else {
        read_only_annotations()
    };
    Tool::new(
        stored.effective_tool_name().to_string(),
        description,
        schema(stored_query_input_schema(stored)),
    )
    .with_annotations(annotations)
}

fn stored_query_list_descriptor() -> Tool {
    Tool::new(
        STORED_QUERY_LIST_TOOL,
        "List this graph's stored queries (names, descriptions, params). Use this to \
         discover queries, then run one with stored_query_run.",
        schema(json!({
            "type": "object",
            "properties": {
                "filter": { "type": "string", "description": "Case-insensitive substring over name/description." },
                "detail_level": { "type": "string", "enum": ["summary", "full"], "description": "`full` includes typed params (default summary)." }
            },
            "additionalProperties": false
        })),
    )
    .with_annotations(read_only_annotations())
}

fn stored_query_run_descriptor() -> Tool {
    Tool::new(
        STORED_QUERY_RUN_TOOL,
        "Run a stored query by name. A read returns rows; a mutation applies a write.",
        schema(json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Stored query name (from stored_query_list)." },
                "params": { "type": "object", "description": "Query parameters." },
                "branch": { "type": "string", "description": "Branch (default main)." },
                "snapshot": { "type": "string", "description": "Snapshot id (reads only; exclusive with branch)." }
            },
            "required": ["name"],
            "additionalProperties": false
        })),
    )
    .with_annotations(
        // Mixed read/write population — annotate conservatively as a writer so
        // clients prompt for confirmation; the inner gate enforces per query.
        write_annotations(true),
    )
}

fn stored_query_list(registry: &QueryRegistry, args: &JsonObject) -> Result<CallToolResult, ApiError> {
    let filter = opt_str(args, "filter")?.map(|f| f.to_lowercase());
    let full = matches!(opt_str(args, "detail_level")?.as_deref(), Some("full"));
    let mut entries = Vec::new();
    for query in registry.exposed() {
        let entry = api::query_catalog_entry(query);
        if let Some(filter) = &filter {
            let hay = format!(
                "{} {}",
                entry.name.to_lowercase(),
                entry.description.as_deref().unwrap_or("").to_lowercase()
            );
            if !hay.contains(filter) {
                continue;
            }
        }
        if full {
            entries.push(serde_json::to_value(&entry).unwrap_or(Value::Null));
        } else {
            entries.push(json!({
                "name": entry.name,
                "tool_name": entry.tool_name,
                "description": entry.description,
                "mutation": entry.mutation,
            }));
        }
    }
    json_result(&json!({ "queries": entries }))
}

// ===== built-in tools =====

/// How `tools/list` gates a built-in, derived from what its `call` authorizes.
/// See [`Builtin::list_gate`].
enum ListGate {
    /// Ungated — always visible.
    Always,
    /// The call authorizes this exact request regardless of arguments; list iff
    /// it is allowed (faithful, no over-show).
    Exact(PolicyRequest),
    /// The call authorizes against a caller-chosen branch; list iff the action
    /// is permitted on *some* branch (a relaxation that never hides a callable
    /// tool).
    AnyBranch(PolicyAction),
}

/// Built-in operational tools. One variant per tool; `descriptor`/`list_gate`/
/// `call` are match arms (lower liability than a `dyn` zoo).
#[derive(Clone, Copy)]
enum Builtin {
    GraphHealth,
    GraphQuery,
    GraphSnapshot,
    SchemaGet,
    BranchList,
    CommitList,
    CommitGet,
    GraphMutate,
    GraphLoad,
    BranchCreate,
    BranchDelete,
    BranchMerge,
    SchemaApply,
}

impl Builtin {
    const ALL: [Builtin; 13] = [
        Builtin::GraphHealth,
        Builtin::GraphQuery,
        Builtin::GraphSnapshot,
        Builtin::SchemaGet,
        Builtin::BranchList,
        Builtin::CommitList,
        Builtin::CommitGet,
        Builtin::GraphMutate,
        Builtin::GraphLoad,
        Builtin::BranchCreate,
        Builtin::BranchDelete,
        Builtin::BranchMerge,
        Builtin::SchemaApply,
    ];

    fn name(self) -> &'static str {
        match self {
            Builtin::GraphHealth => "graph_health",
            Builtin::GraphQuery => "graph_query",
            Builtin::GraphSnapshot => "graph_snapshot",
            Builtin::SchemaGet => "schema_get",
            Builtin::BranchList => "branch_list",
            Builtin::CommitList => "commit_list",
            Builtin::CommitGet => "commit_get",
            Builtin::GraphMutate => "graph_mutate",
            Builtin::GraphLoad => "graph_load",
            Builtin::BranchCreate => "branch_create",
            Builtin::BranchDelete => "branch_delete",
            Builtin::BranchMerge => "branch_merge",
            Builtin::SchemaApply => "schema_apply",
        }
    }

    fn by_name(name: &str) -> Option<Builtin> {
        Builtin::ALL.into_iter().find(|b| b.name() == name)
    }

    /// How `tools/list` decides this tool's visibility — **derived from what the
    /// tool's `call` actually authorizes**, so listing never diverges from
    /// callability:
    /// - `Always` — ungated (`graph_health` liveness).
    /// - `Exact(req)` — the call authorizes a *fixed* request regardless of
    ///   arguments (`schema_get`/`branch_list`/`commit_get` read branchlessly;
    ///   `schema_apply` always targets `main`). Listed iff that exact request is
    ///   allowed — faithful, no over-show, and consistent with the resource
    ///   twins (`omnigraph://schema` etc.) which use the same branchless read.
    /// - `AnyBranch(action)` — the call authorizes against a *caller-chosen*
    ///   branch. Listed iff the actor could perform `action` on *some* branch (a
    ///   relaxation: never hide a tool the caller could invoke — e.g.
    ///   `graph_mutate` shows for an unprotected-branch writer even when `main`
    ///   is protected). The authoritative per-call gate runs inside `call`.
    fn list_gate(self) -> ListGate {
        match self {
            Builtin::GraphHealth => ListGate::Always,
            // Fixed branchless read — same gate as the schema/branches resources.
            Builtin::SchemaGet | Builtin::BranchList | Builtin::CommitGet => {
                ListGate::Exact(read_request(None))
            }
            // Always targets `main`.
            Builtin::SchemaApply => ListGate::Exact(PolicyRequest {
                action: PolicyAction::SchemaApply,
                branch: None,
                target_branch: Some("main".to_string()),
            }),
            // Caller-chosen branch → relaxation.
            Builtin::GraphQuery | Builtin::GraphSnapshot | Builtin::CommitList => {
                ListGate::AnyBranch(PolicyAction::Read)
            }
            Builtin::GraphMutate | Builtin::GraphLoad => ListGate::AnyBranch(PolicyAction::Change),
            Builtin::BranchCreate => ListGate::AnyBranch(PolicyAction::BranchCreate),
            Builtin::BranchDelete => ListGate::AnyBranch(PolicyAction::BranchDelete),
            Builtin::BranchMerge => ListGate::AnyBranch(PolicyAction::BranchMerge),
        }
    }

    fn descriptor(self) -> Tool {
        let (description, input, annotations): (&'static str, Value, ToolAnnotations) = match self {
            Builtin::GraphHealth => (
                "Liveness/identity probe for this graph. No arguments.",
                json!({ "type": "object", "properties": {}, "additionalProperties": false }),
                read_only_annotations(),
            ),
            Builtin::GraphQuery => (
                "Run an ad-hoc read-only GQ query against this graph. Mutations are rejected.",
                json!({
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "GQ query source." },
                        "name": { "type": "string", "description": "Select one query by name when the source defines several." },
                        "params": { "type": "object", "description": "Query parameters." },
                        "branch": { "type": "string", "description": "Branch to read (default main; exclusive with snapshot)." },
                        "snapshot": { "type": "string", "description": "Snapshot id to read (exclusive with branch)." }
                    },
                    "required": ["query"],
                    "additionalProperties": false
                }),
                read_only_annotations(),
            ),
            Builtin::GraphSnapshot => (
                "Read the current snapshot (manifest version + per-table metadata) of a branch.",
                json!({
                    "type": "object",
                    "properties": { "branch": { "type": "string", "description": "Branch (default main)." } },
                    "additionalProperties": false
                }),
                read_only_annotations(),
            ),
            Builtin::SchemaGet => (
                "Get this graph's schema as .pg source.",
                json!({ "type": "object", "properties": {}, "additionalProperties": false }),
                read_only_annotations(),
            ),
            Builtin::BranchList => (
                "List all branch names (sorted).",
                json!({ "type": "object", "properties": {}, "additionalProperties": false }),
                read_only_annotations(),
            ),
            Builtin::CommitList => (
                "List commits, optionally scoped to a branch.",
                json!({
                    "type": "object",
                    "properties": { "branch": { "type": "string", "description": "Restrict to a branch's history." } },
                    "additionalProperties": false
                }),
                read_only_annotations(),
            ),
            Builtin::CommitGet => (
                "Get a single commit by id.",
                json!({
                    "type": "object",
                    "properties": { "commit_id": { "type": "string" } },
                    "required": ["commit_id"],
                    "additionalProperties": false
                }),
                read_only_annotations(),
            ),
            Builtin::GraphMutate => (
                "Run an ad-hoc GQ mutation (insert/update/delete) against a branch.",
                json!({
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "GQ mutation source." },
                        "name": { "type": "string", "description": "Select one query by name when the source defines several." },
                        "params": { "type": "object", "description": "Query parameters." },
                        "branch": { "type": "string", "description": "Branch to write (default main)." }
                    },
                    "required": ["query"],
                    "additionalProperties": false
                }),
                write_annotations(true),
            ),
            Builtin::GraphLoad => (
                "Bulk-load NDJSON into a branch. Without `from`, the branch must exist (a \
                 missing branch is an error, never an implicit fork).",
                json!({
                    "type": "object",
                    "properties": {
                        "data": { "type": "string", "description": "NDJSON, one record per line." },
                        "branch": { "type": "string", "description": "Target branch (default main)." },
                        "from": { "type": "string", "description": "Parent to fork `branch` from if it doesn't exist." },
                        "mode": { "type": "string", "enum": ["merge", "append", "overwrite"], "description": "On existing rows (default merge)." }
                    },
                    "required": ["data"],
                    "additionalProperties": false
                }),
                write_annotations(true),
            ),
            Builtin::BranchCreate => (
                "Create a branch by forking `from` (default main).",
                json!({
                    "type": "object",
                    "properties": {
                        "name": { "type": "string", "description": "New branch name." },
                        "from": { "type": "string", "description": "Parent branch (default main)." }
                    },
                    "required": ["name"],
                    "additionalProperties": false
                }),
                // Additive — not destructive.
                write_annotations(false),
            ),
            Builtin::BranchDelete => (
                "Delete a branch.",
                json!({
                    "type": "object",
                    "properties": { "branch": { "type": "string" } },
                    "required": ["branch"],
                    "additionalProperties": false
                }),
                write_annotations(true),
            ),
            Builtin::BranchMerge => (
                "Merge `source` into `target` (default main).",
                json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "target": { "type": "string", "description": "Default main." }
                    },
                    "required": ["source"],
                    "additionalProperties": false
                }),
                write_annotations(true),
            ),
            Builtin::SchemaApply => (
                "Apply a schema migration (.pg source). Disabled (409) on cluster-backed \
                 serving — use `omnigraph cluster apply` and restart.",
                json!({
                    "type": "object",
                    "properties": {
                        "schema_source": { "type": "string", "description": "Target schema as .pg source." },
                        "allow_data_loss": { "type": "boolean", "description": "Permit data-dropping steps (default false)." }
                    },
                    "required": ["schema_source"],
                    "additionalProperties": false
                }),
                write_annotations(true),
            ),
        };
        Tool::new(self.name(), description, schema(input)).with_annotations(annotations)
    }

    async fn call(
        self,
        state: &AppState,
        actor: Option<&ResolvedActor>,
        handle: &Arc<GraphHandle>,
        args: JsonObject,
    ) -> Result<CallToolResult, ApiError> {
        match self {
            Builtin::GraphHealth => json_result(&json!({
                "graph_id": handle.key.graph_id.as_str(),
                "status": "ok",
                "version": SERVER_VERSION,
            })),
            Builtin::GraphQuery => {
                // run_query self-authorizes Read (per its real target branch).
                let (selected, target, result) = run_query(
                    Arc::clone(handle),
                    actor,
                    &req_str(&args, "query")?,
                    opt_str(&args, "name")?.as_deref(),
                    opt_obj(&args, "params")?.as_ref(),
                    opt_str(&args, "branch")?,
                    opt_str(&args, "snapshot")?,
                    true,
                )
                .await?;
                json_result(&api::read_output(selected, &target, result))
            }
            Builtin::GraphSnapshot => {
                let branch = opt_str(&args, "branch")?.unwrap_or_else(|| "main".to_string());
                authorize_request(actor, handle.policy.as_deref(), read_request(Some(branch.clone())))?;
                let snapshot = handle
                    .engine
                    .snapshot_of(ReadTarget::branch(branch.as_str()))
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::snapshot_payload(&branch, &snapshot))
            }
            Builtin::SchemaGet => {
                authorize_request(actor, handle.policy.as_deref(), read_request(None))?;
                json_result(&api::SchemaOutput {
                    schema_source: handle.engine.schema_source().to_string(),
                })
            }
            Builtin::BranchList => {
                authorize_request(actor, handle.policy.as_deref(), read_request(None))?;
                let mut branches = handle.engine.branch_list().await.map_err(ApiError::from_omni)?;
                branches.sort();
                json_result(&api::BranchListOutput { branches })
            }
            Builtin::CommitList => {
                let branch = opt_str(&args, "branch")?;
                authorize_request(actor, handle.policy.as_deref(), read_request(branch.clone()))?;
                let commits = handle
                    .engine
                    .list_commits(branch.as_deref())
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::CommitListOutput {
                    commits: commits.iter().map(api::commit_output).collect(),
                })
            }
            Builtin::CommitGet => {
                let commit_id = req_str(&args, "commit_id")?;
                authorize_request(actor, handle.policy.as_deref(), read_request(None))?;
                let commit = handle
                    .engine
                    .get_commit(&commit_id)
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::commit_output(&commit))
            }
            Builtin::GraphMutate => {
                // run_mutate self-authorizes Change + admission.
                let branch = opt_str(&args, "branch")?.unwrap_or_else(|| "main".to_string());
                let output = run_mutate(
                    state.clone(),
                    Arc::clone(handle),
                    actor,
                    &req_str(&args, "query")?,
                    opt_str(&args, "name")?.as_deref(),
                    opt_obj(&args, "params")?.as_ref(),
                    branch,
                )
                .await?;
                json_result(&output)
            }
            Builtin::GraphLoad => {
                // run_ingest self-authorizes (BranchCreate iff `from`, then
                // Change) + admission, and 404s a missing branch with no `from`.
                let request: api::IngestRequest = from_args(args, "load request")?;
                let output = run_ingest(state.clone(), Arc::clone(handle), actor, request).await?;
                json_result(&output)
            }
            Builtin::BranchCreate => {
                let request: api::BranchCreateRequest = from_args(args, "branch-create request")?;
                let from = request.from.unwrap_or_else(|| "main".to_string());
                authorize_request(
                    actor,
                    handle.policy.as_deref(),
                    PolicyRequest {
                        action: PolicyAction::BranchCreate,
                        branch: Some(from.clone()),
                        target_branch: Some(request.name.clone()),
                    },
                )?;
                let _admission = state
                    .workload
                    .try_admit(&actor_arc(actor), 256)
                    .map_err(ApiError::from_workload_reject)?;
                handle
                    .engine
                    .branch_create_from_as(ReadTarget::branch(&from), &request.name, actor_id(actor))
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::BranchCreateOutput {
                    uri: handle.uri.clone(),
                    from,
                    name: request.name,
                    actor_id: actor_id(actor).map(str::to_string),
                })
            }
            Builtin::BranchDelete => {
                let branch = req_str(&args, "branch")?;
                authorize_request(
                    actor,
                    handle.policy.as_deref(),
                    PolicyRequest {
                        action: PolicyAction::BranchDelete,
                        branch: None,
                        target_branch: Some(branch.clone()),
                    },
                )?;
                let _admission = state
                    .workload
                    .try_admit(&actor_arc(actor), 256)
                    .map_err(ApiError::from_workload_reject)?;
                handle
                    .engine
                    .branch_delete_as(&branch, actor_id(actor))
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::BranchDeleteOutput {
                    uri: handle.uri.clone(),
                    name: branch,
                    actor_id: actor_id(actor).map(str::to_string),
                })
            }
            Builtin::BranchMerge => {
                let request: api::BranchMergeRequest = from_args(args, "branch-merge request")?;
                let target = request.target.unwrap_or_else(|| "main".to_string());
                authorize_request(
                    actor,
                    handle.policy.as_deref(),
                    PolicyRequest {
                        action: PolicyAction::BranchMerge,
                        branch: Some(request.source.clone()),
                        target_branch: Some(target.clone()),
                    },
                )?;
                let _admission = state
                    .workload
                    .try_admit(&actor_arc(actor), 256)
                    .map_err(ApiError::from_workload_reject)?;
                let outcome = handle
                    .engine
                    .branch_merge_as(&request.source, &target, actor_id(actor))
                    .await
                    .map_err(ApiError::from_omni)?;
                json_result(&api::BranchMergeOutput {
                    source: request.source,
                    target,
                    outcome: outcome.into(),
                    actor_id: actor_id(actor).map(str::to_string),
                })
            }
            Builtin::SchemaApply => {
                let request: api::SchemaApplyRequest = from_args(args, "schema-apply request")?;
                authorize_request(
                    actor,
                    handle.policy.as_deref(),
                    PolicyRequest {
                        action: PolicyAction::SchemaApply,
                        branch: None,
                        target_branch: Some("main".to_string()),
                    },
                )?;
                // Disable on cluster-backed serving AFTER the Cedar gate, so an
                // unauthorized actor gets 403, not a topology-disclosing 409.
                if state.routing().config_path.is_some() {
                    return Err(ApiError::conflict(
                        "server-side schema apply is disabled for cluster-backed serving; \
                         update the cluster config, run `omnigraph cluster apply`, and restart \
                         the server.",
                    ));
                }
                let _admission = state
                    .workload
                    .try_admit(&actor_arc(actor), request.schema_source.len() as u64)
                    .map_err(ApiError::from_workload_reject)?;
                let registry = handle.queries.as_deref();
                let label = handle.key.graph_id.as_str().to_string();
                let result = handle
                    .engine
                    .apply_schema_as_with_catalog_check(
                        &request.schema_source,
                        SchemaApplyOptions { allow_data_loss: request.allow_data_loss },
                        actor_id(actor),
                        |catalog| {
                            if let Some(registry) = registry {
                                validate_registry_against_catalog(registry, catalog, &label)?;
                            }
                            Ok(())
                        },
                    )
                    .await
                    .map_err(ApiError::from_omni)?;
                if result.applied {
                    let engine = Arc::clone(&handle.engine);
                    tokio::spawn(async move {
                        if let Err(err) = engine.ensure_indices().await {
                            tracing::warn!(error = %err, "post-apply ensure_indices failed");
                        }
                    });
                }
                json_result(&api::schema_apply_output(handle.uri.as_str(), result))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_tool_names_match_enum() {
        let from_enum: Vec<&str> = Builtin::ALL.iter().map(|b| b.name()).collect();
        assert_eq!(
            from_enum, BUILTIN_TOOL_NAMES,
            "BUILTIN_TOOL_NAMES must stay in sync with Builtin::ALL (the collision-check reserves these)"
        );
    }
}
