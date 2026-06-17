//! MCP (Model Context Protocol) server surface for Omnigraph, served over
//! **stateless Streamable HTTP**.
//!
//! This crate owns the `rmcp` dependency and the transport wiring. It defines a
//! single seam — the [`McpBackend`] trait — that the server crate implements.
//! The crate **never names an omnigraph type**: the backend reads its own types
//! (resolved actor, graph handle, …) out of `parts.extensions`, so the
//! dependency edge is `server → mcp` (never the reverse — a `mcp → server` edge
//! would cycle the binary at `server-bin → omnigraph-mcp → server-lib`).
//!
//! The transport is **stateless JSON over a single `/mcp` POST**: no SSE stream,
//! no `Mcp-Session-Id`, every request independent. See [`transport`].

use async_trait::async_trait;

mod service;
pub mod transport;

pub use transport::{McpHostPolicy, OriginPolicy, mcp_router};

// rmcp model types re-exported so the server speaks rmcp via `omnigraph_mcp::…`
// and carries no direct rmcp dependency.
pub use rmcp::ErrorData as McpError; // JSON-RPC error: invalid_params=-32602, internal_error=-32603
pub use rmcp::model::{
    CallToolResult, Content, Extensions, Implementation, RawResource, ReadResourceResult, Resource,
    ResourceContents, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};

/// A JSON object — the shape of tool arguments and JSON Schema documents.
/// Identical to `rmcp::model::JsonObject` (`serde_json::Map<String, Value>`).
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

/// The seam the server fills. One implementor (`OmnigraphMcpBackend`); the boxed
/// future from `#[async_trait]` is negligible at MCP QPS.
///
/// **The list seam is non-paginated by contract.** `list_tools`/`list_resources`
/// return the *full* set, so the service always emits `nextCursor: null`. The
/// catalog is bounded by construction (a fixed set of built-ins; large
/// stored-query catalogs collapse to a discovery + execute meta-tool pair rather
/// than leaning on `tools/list` paging). The `Vec<T>` return type *is* that
/// contract; a future paging need is a signature change, not a doc promise.
///
/// Each method receives the request's [`http::request::Parts`]; the backend reads
/// its own injected extensions (`parts.extensions.get::<T>()`) — the decoupling
/// mechanism that keeps this crate free of omnigraph types and auth-method
/// agnostic.
#[async_trait]
pub trait McpBackend: Clone + Send + Sync + 'static {
    /// Server identity + advertised capabilities (`initialize` response).
    fn server_info(&self) -> ServerInfo;

    /// The full, Cedar-filtered tool set for this request's actor + graph.
    async fn list_tools(&self, parts: &http::request::Parts) -> Result<Vec<Tool>, McpError>;

    /// Dispatch a tool call. The authoritative authorization gate.
    async fn call_tool(
        &self,
        parts: &http::request::Parts,
        name: &str,
        args: JsonObject,
    ) -> Result<CallToolResult, McpError>;

    /// The full, Cedar-filtered resource set for this request's actor + graph.
    async fn list_resources(&self, parts: &http::request::Parts)
    -> Result<Vec<Resource>, McpError>;

    /// Read one resource by URI.
    async fn read_resource(
        &self,
        parts: &http::request::Parts,
        uri: &str,
    ) -> Result<ReadResourceResult, McpError>;
}
