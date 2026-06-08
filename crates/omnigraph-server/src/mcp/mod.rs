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

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo},
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};

use crate::AppState;

/// Server-level guidance returned in the MCP `initialize` response.
const MCP_INSTRUCTIONS: &str = "OmniGraph is a versioned, branchable property graph. \
Reads run typed GQ queries; writes are branchable and policy-gated. The tools mirror the \
HTTP API and are authorized per-actor by Cedar policy — a tool you cannot see is one you \
are not permitted to call.";

/// Shared MCP handler. Cheap to clone (holds the `Arc`-backed [`AppState`]); the
/// streamable-HTTP service constructs one per request in stateless mode.
#[derive(Clone)]
pub(crate) struct OmnigraphMcpHandler {
    // Wired in Phase 3 (tools) / Phase 5 (resources): the handler resolves the
    // per-request actor + graph handle from the request extensions and routes
    // tool calls through the shared `do_*` / `run_query` / `run_mutate` paths.
    #[allow(dead_code)]
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
        // Advertise only `tools` for now. The resources phase adds
        // `list_resources`/`read_resource`; advertising a `resources`
        // capability whose `resources/read` returns method-not-found would be a
        // dishonest contract, so `.enable_resources()` lands with that phase.
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "omnigraph-server".to_string();
        info.server_info.version = env!("CARGO_PKG_VERSION").to_string();
        info.instructions = Some(MCP_INSTRUCTIONS.to_string());
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        // Phase 3 populates this with the Cedar-filtered built-in tools; Phase 4
        // adds the dynamic stored-query tools.
        Ok(ListToolsResult::default())
    }
}

/// Build the stateless Streamable-HTTP MCP service mounted at `/mcp`.
///
/// Mounted inside the `per_graph_protected` route group so the bearer-auth and
/// graph-handle middleware run first; in multi-graph mode the same service is
/// reached at `/graphs/{graph_id}/mcp`.
pub(crate) fn mcp_service(
    state: AppState,
) -> StreamableHttpService<OmnigraphMcpHandler, LocalSessionManager> {
    let handler = OmnigraphMcpHandler::new(state);
    // `StreamableHttpServerConfig` is `#[non_exhaustive]`: start from `Default`,
    // then flip to stateless JSON. Keep rmcp's loopback `allowed_hosts` default
    // (DNS-rebinding protection for local servers); a server-config knob to
    // widen `allowed_hosts` / `allowed_origins` for non-loopback deployments
    // lands with the OAuth fast-follow.
    let config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true);
    StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    )
}
