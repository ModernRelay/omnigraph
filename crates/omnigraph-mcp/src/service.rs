//! `McpService<B>` — the rmcp `ServerHandler` adapter. Pulls the request's
//! `http::request::Parts` out of the context once and delegates each method to
//! the [`McpBackend`]. Maps the backend's non-paginated `Vec<T>` returns to
//! rmcp's `List*Result` with `next_cursor: None`.

use rmcp::ServerHandler;
use rmcp::ErrorData as McpError;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ListResourcesResult, ListToolsResult,
    PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult, ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};

use crate::McpBackend;

#[derive(Clone)]
pub(crate) struct McpService<B: McpBackend> {
    backend: B,
}

impl<B: McpBackend> McpService<B> {
    pub(crate) fn new(backend: B) -> Self {
        Self { backend }
    }

    /// The HTTP `Parts` injected by `StreamableHttpService` into the request
    /// context extensions (`tower.rs` does `request.into_parts()` then
    /// `req.request.extensions_mut().insert(part)`). Absent only on an internal
    /// wiring error, not a client-reachable path.
    fn parts(ctx: &RequestContext<RoleServer>) -> Result<&http::request::Parts, McpError> {
        ctx.extensions
            .get::<http::request::Parts>()
            .ok_or_else(|| McpError::internal_error("request parts missing from MCP context", None))
    }
}

impl<B: McpBackend> ServerHandler for McpService<B> {
    fn get_info(&self) -> ServerInfo {
        self.backend.server_info()
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let parts = Self::parts(&context)?;
        let tools = self.backend.list_tools(parts).await?;
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let parts = Self::parts(&context)?;
        let args = request.arguments.unwrap_or_default();
        self.backend.call_tool(parts, &request.name, args).await
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let parts = Self::parts(&context)?;
        let resources = self.backend.list_resources(parts).await?;
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let parts = Self::parts(&context)?;
        self.backend.read_resource(parts, &request.uri).await
    }
}
