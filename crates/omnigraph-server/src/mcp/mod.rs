//! In-server MCP (Model Context Protocol) surface — RFC-003.
//!
//! Projects omnigraph-server operations as MCP tools and resources over
//! Streamable HTTP (rmcp), Cedar-gated through the same `authorize` path the
//! REST routes use. Stateless POST-only: rmcp's `stateful_mode = false` gives
//! `GET`/`DELETE` → 405 and `MCP-Protocol-Version` validation (400 on
//! unsupported, default `2025-03-26` when absent) for free. Host/Origin
//! DNS-rebinding checks use rmcp's loopback `allowed_hosts` default until a
//! server-config knob to widen them for non-loopback deploys lands with the
//! OAuth fast-follow.
//!
//! Auth is decoupled (RFC-003 §5.8): the `require_bearer_auth` /
//! `resolve_graph_handle` middleware run before the MCP service and attach
//! `ResolvedActor` + `Arc<GraphHandle>` to the request; the handler reads them
//! back from `RequestContext.extensions` → `http::request::Parts.extensions`.

use std::sync::Arc;

use axum::Router;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ListResourcesResult, ListToolsResult,
        PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult, ServerCapabilities,
        ServerInfo,
    },
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};

use tower_http::limit::RequestBodyLimitLayer;

use crate::{AppState, INGEST_REQUEST_BODY_LIMIT_BYTES};
use builtins::Builtin;

mod builtins;

/// Server-level guidance returned in the MCP `initialize` response.
const MCP_INSTRUCTIONS: &str = "OmniGraph is a versioned, branchable property graph. \
Reads run typed GQ queries; writes are branchable and policy-gated. The tools mirror the \
HTTP API and are authorized per-actor by Cedar policy — a tool you cannot see is one you \
are not permitted to call.";

/// Shared MCP handler. Cheap to clone (holds the `Arc`-backed [`AppState`]); the
/// streamable-HTTP service constructs one per request in stateless mode.
#[derive(Clone)]
pub(crate) struct OmnigraphMcpHandler {
    // The handler resolves the per-request actor + graph handle from the
    // request extensions (`resolve_cx`) and routes tool calls through the shared
    // `do_*` / `run_query` / `run_mutate` paths. `state` supplies workload
    // admission, the server-level policy, and graph routing.
    state: AppState,
}

impl OmnigraphMcpHandler {
    fn new(state: AppState) -> Self {
        Self { state }
    }
}

impl ServerHandler for OmnigraphMcpHandler {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` (`InitializeResult`) is `#[non_exhaustive]`; build from
        // `Default` and set the fields we own. We advertise `tools` and
        // `resources` with neither `listChanged` nor `subscribe` — stateless,
        // no server push.
        let mut info = ServerInfo::default();
        // Advertise `tools` and `resources` (no `listChanged`/`subscribe` —
        // stateless, no server push). Both are backed by real handlers below.
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info.server_info.name = "omnigraph-server".to_string();
        info.server_info.version = env!("CARGO_PKG_VERSION").to_string();
        info.instructions = Some(MCP_INSTRUCTIONS.to_string());
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let cx = builtins::resolve_cx(&self.state, &context)?;
        let mut tools = Vec::new();
        for &tool in Builtin::all() {
            // Emit only tools the actor's Cedar policy permits. An operational
            // failure (policy-engine error) propagates; a denial just hides.
            if builtins::is_visible(tool, &cx)? {
                tools.push(tool.descriptor());
            }
        }
        // Stored-query tools: gated as a group by the coarse `InvokeQuery`
        // action (all exposed queries, or none).
        if builtins::stored_invoke_visible(&cx)? {
            tools.extend(builtins::stored_descriptors(&cx));
        }
        let mut result = ListToolsResult::default();
        result.tools = tools;
        Ok(result)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let cx = builtins::resolve_cx(&self.state, &context)?;
        let name = request.name.to_string();
        let args = request.arguments.unwrap_or_default();
        // Deny ≡ unknown: a denied tool and an unknown one return the identical
        // error so the catalog isn't probeable without the grant (the same
        // principle as `POST /queries/{name}`). The inner `do_*` / `run_*`
        // re-authorizes against the real branch.
        let unknown = || McpError::invalid_params(format!("unknown tool: {name}"), None);

        if let Some(tool) = Builtin::from_name(&name) {
            if !builtins::is_visible(tool, &cx)? {
                return Err(unknown());
            }
            return builtins::dispatch(tool, &cx, &args).await;
        }
        if builtins::is_stored_tool(&cx, &name) {
            // Outer InvokeQuery gate (coarse); the inner Read/Change runs in
            // run_query/run_mutate — the double-gate of POST /queries/{name}.
            if !builtins::stored_invoke_visible(&cx)? {
                return Err(unknown());
            }
            return builtins::call_stored_tool(&cx, &name, &args).await;
        }
        Err(unknown())
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let cx = builtins::resolve_cx(&self.state, &context)?;
        let mut result = ListResourcesResult::default();
        result.resources = builtins::list_resources(&cx)?;
        Ok(result)
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let cx = builtins::resolve_cx(&self.state, &context)?;
        builtins::read_resource(&cx, &request.uri).await
    }
}

/// Build the `/mcp` route: a stateless Streamable-HTTP MCP service, body-capped.
///
/// Merged into the `per_graph_protected` route group so the bearer-auth and
/// graph-handle middleware run first; in multi-graph mode the same service is
/// reached at `/graphs/{graph_id}/mcp`.
pub(crate) fn mcp_router(state: AppState) -> Router<AppState> {
    let handler = OmnigraphMcpHandler::new(state);
    // `StreamableHttpServerConfig` is `#[non_exhaustive]`: start from `Default`,
    // then flip to stateless JSON. Keep rmcp's loopback `allowed_hosts` default
    // (DNS-rebinding protection for local servers); a server-config knob to
    // widen `allowed_hosts` / `allowed_origins` for non-loopback deployments
    // lands with the OAuth fast-follow.
    let config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true);
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    // rmcp reads the request body directly (it doesn't go through axum's
    // `Bytes`/`Json` extractor), so the router's `DefaultBodyLimit` does NOT
    // bound `/mcp`. Cap it explicitly at the ingest limit (the largest tool
    // payload) so an MCP `ingest`/`schema_apply` call can't stream unbounded.
    Router::new()
        .route_service("/mcp", service)
        .layer(RequestBodyLimitLayer::new(INGEST_REQUEST_BODY_LIMIT_BYTES))
}
