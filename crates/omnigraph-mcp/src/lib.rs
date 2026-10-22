//! MCP protocol support mounted by the existing OmniGraph server.
//!
//! This crate owns SDK negotiation, tool/resource descriptions and bounded MCP
//! results. A [`Host`] supplies authenticated request context and starts work
//! through its existing graph handlers and operation ownership. There is no
//! listener, graph opener, authentication authority or separate serving runtime.

mod arguments;
mod catalog;
mod resources;
mod result;
mod settings;

pub use arguments::{
    AccessArguments, BranchArguments, BranchCreateArguments, BranchDeleteArguments,
    BranchMergeArguments, ChangeFilters, ChangesPollArguments, CommitArguments,
    CommitChangesArguments, CommitsListArguments, GraphArguments, InvalidArguments, LoadArguments,
    MutateArguments, QueryArguments, ReadArguments, ToolCall, ToolRequest, change_query,
};
pub use resources::{ResourceOutput, ResourceRequest};
pub use result::{HostError, bounded_result};
pub use rmcp::model::CallToolResult as ToolResult;
pub use settings::{MCP_ALLOW_ADHOC_ENV, MCP_ALLOW_WRITES_ENV, MCP_ALLOWED_HOSTS_ENV, McpSettings};

use axum::Router;
use axum::http::{StatusCode, request::Parts};
use catalog::*;
use resources::*;
use result::check_result;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ErrorData, Implementation,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
    ResourceContents, ResourceTemplate, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{RoleServer, ServerHandler};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Wire-input bound reserved by the host before it collects an MCP body.
pub const REQUEST_BYTES: usize = 64 * 1024;
/// Maximum encoded tool result, including the SDK's text compatibility copy.
pub const RESULT_BYTES: usize = 1024 * 1024;
const READ_DEADLINE: Duration = Duration::from_secs(30);
// Observation ends before common clients' 60-second timeout; owned work continues.
const WRITE_DEADLINE: Duration = READ_DEADLINE;
#[cfg(test)]
const COMMON_CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

/// An observation of work already retained by the host. Dropping it must not
/// cancel that work or release its reservations before execution completes.
pub type Pending<T> = Pin<Box<dyn Future<Output = Result<T, HostError>> + Send + 'static>>;

/// Missing trusted context in an SDK request. The host checks the actor first,
/// then the admitted request's existing observer and ingress reservations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    Unauthenticated,
    MissingContext,
}

/// A protocol failure for the host to judge using its canonical error model.
/// These events are not wire codes: the host owns their status and disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolError {
    Unauthenticated,
    MissingContext,
    Capacity,
    Cancelled,
    Deadline,
    ResultTooLarge,
    InvalidResult,
}

/// The narrow connection to the embedding server's existing request runtime.
///
/// `start_*` must synchronously start owned work before returning its response
/// future. Execution must retain the admitted context and `permit` until it
/// finishes, even after MCP cancellation, timeout or disconnect. Implementations
/// reuse their existing graph, policy and write handlers rather than opening
/// storage or acquiring another admission lease.
pub trait Host: Clone + Send + Sync + 'static {
    type Context: Send + 'static;
    /// Project a protocol failure through the same error contract as handlers.
    /// `write` preserves the need to reconcile an unfinished write or a write
    /// whose result cannot be delivered. The returned error must be bounded.
    fn error(&self, error: ProtocolError, write: bool) -> HostError;
    fn admit(&self, parts: Option<&Parts>) -> Result<Self::Context, AdmissionError>;
    fn start_tool(
        &self,
        context: Self::Context,
        request: ToolRequest,
        permit: OwnedSemaphorePermit,
    ) -> Pending<ToolResult>;
    fn start_resource(
        &self,
        context: Self::Context,
        request: ResourceRequest,
        permit: OwnedSemaphorePermit,
    ) -> Pending<ResourceOutput>;
}

/// Build only the `/mcp` protocol route. The host mounts this behind its
/// existing authentication, expected-actor, Origin and ingress middleware.
/// The public OIDC resource, when present, supplies only fixed allowed hosts
/// and origins; inbound headers never determine those allowlists.
pub fn router<S, H>(
    host: H,
    settings: McpSettings,
    resource_url: Option<&str>,
    version: String,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    H: Host,
{
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    config.max_request_body_bytes = REQUEST_BYTES;
    config
        .allowed_hosts
        .extend(settings.allowed_hosts.iter().cloned());
    if let Some(resource) = resource_url.and_then(|resource| url::Url::parse(resource).ok()) {
        config
            .allowed_hosts
            .push(resource[url::Position::BeforeHost..url::Position::AfterPort].into());
        config
            .allowed_origins
            .push(resource.origin().ascii_serialization());
    }
    let handler = GraphTools {
        host,
        settings,
        version,
        slots: Arc::new(Semaphore::new(16)),
    };
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    Router::new().route_service("/mcp", service)
}

#[derive(Clone)]
struct GraphTools<H> {
    host: H,
    settings: McpSettings,
    version: String,
    slots: Arc<Semaphore>,
}

/// Why a tool call or resource read produced no handler result.
enum Refusal {
    Protocol(ProtocolError),
    Api(HostError),
}

impl Refusal {
    fn tool_result(self, host: &impl Host, write: bool) -> CallToolResult {
        match self {
            Self::Protocol(error) => host.error(error, write),
            Self::Api(error) => error,
        }
        .into_tool_result()
    }

    /// A resource read carries the same canonical error envelope in `data`.
    fn resource_error(self, host: &impl Host) -> ErrorData {
        let status = match &self {
            Self::Api(error) => error.status(),
            Self::Protocol(_) => None,
        };
        let refused = status.is_some_and(|status| status.is_client_error())
            || matches!(self, Self::Protocol(ProtocolError::Unauthenticated));
        let message = match &self {
            Self::Api(_) if status == Some(StatusCode::NOT_FOUND) => "resource not found",
            Self::Api(_) if refused => "resource read refused",
            Self::Api(_) => "resource read failed",
            Self::Protocol(ProtocolError::Unauthenticated) => {
                "a verified bearer credential is required"
            }
            Self::Protocol(ProtocolError::MissingContext) => "MCP admission context is missing",
            Self::Protocol(ProtocolError::Capacity) => {
                "the server is handling its maximum concurrent MCP calls"
            }
            Self::Protocol(ProtocolError::Cancelled) => "the read was cancelled",
            Self::Protocol(ProtocolError::Deadline) => "the read exceeded its 30 second deadline",
            Self::Protocol(ProtocolError::ResultTooLarge) => "the resource exceeds 1 MiB",
            Self::Protocol(ProtocolError::InvalidResult) => "could not encode MCP resource",
        };
        let data = self.tool_result(host, false).structured_content;
        if status == Some(StatusCode::NOT_FOUND) {
            ErrorData::resource_not_found(message, data)
        } else if refused {
            ErrorData::invalid_request(message, data)
        } else {
            ErrorData::internal_error(message, data)
        }
    }
}

impl From<AdmissionError> for Refusal {
    fn from(error: AdmissionError) -> Self {
        Self::Protocol(match error {
            AdmissionError::Unauthenticated => ProtocolError::Unauthenticated,
            AdmissionError::MissingContext => ProtocolError::MissingContext,
        })
    }
}

/// Bound the caller's wait, never the lifetime of the host's owned work.
async fn observe<T, C>(pending: Pending<T>, cancelled: C, deadline: Duration) -> Result<T, Refusal>
where
    C: Future<Output = ()>,
{
    tokio::select! {
        _ = cancelled => Err(Refusal::Protocol(ProtocolError::Cancelled)),
        result = tokio::time::timeout(deadline, pending) => {
            match result {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(Refusal::Api(error)),
                Err(_) => Err(Refusal::Protocol(ProtocolError::Deadline)),
            }
        }
    }
}

impl<H: Host> GraphTools<H> {
    fn fit_result(&self, result: ToolResult, write: bool) -> ToolResult {
        check_result(result)
            .unwrap_or_else(|error| self.host.error(error, write).into_tool_result())
    }

    async fn run_tool<C>(
        &self,
        context: H::Context,
        request: ToolRequest,
        cancelled: C,
        deadline: Duration,
    ) -> Result<ToolResult, Refusal>
    where
        C: Future<Output = ()>,
    {
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| Refusal::Protocol(ProtocolError::Capacity))?;
        // Transfer ownership before cancellation can drop the observation.
        let pending = self.host.start_tool(context, request, permit);
        observe(pending, cancelled, deadline).await
    }
}

impl<H: Host> ServerHandler for GraphTools<H> {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(
            "omnigraph-server",
            self.version.clone(),
        ))
        .with_instructions(instructions(&self.settings))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools(&self.settings)))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools(&self.settings)
            .into_iter()
            .find(|tool| tool.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(spec) = spec(&self.settings, &request.name) else {
            return Err(ErrorData::invalid_params("unknown MCP tool", None));
        };
        let write = spec.gate == Gate::Writes;
        let admitted = match self.host.admit(context.extensions.get::<Parts>()) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                return Ok(Refusal::from(refusal).tool_result(&self.host, write).into());
            }
        };
        let request = ToolRequest::new(
            spec.name,
            Value::Object(request.arguments.unwrap_or_default()),
            write,
        );
        let result = self
            .run_tool(
                admitted,
                request,
                context.ct.cancelled(),
                if write { WRITE_DEADLINE } else { READ_DEADLINE },
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(refusal) => refusal.tool_result(&self.host, write),
        };
        Ok(self.fit_result(result, write).into())
    }

    async fn list_resources(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mut resources = vec![
            Resource::new(GRAPHS_RESOURCE, "graphs")
                .with_title("Graphs")
                .with_description("The graphs this server serves, as `graphs` returns them.")
                .with_mime_type("application/json"),
            Resource::new(BEST_PRACTICES_INDEX, "best-practices/index")
                .with_title("Best-practices index")
                .with_description("Lists every omnigraph://best-practices/* reference and what it covers. Read this first if you are not sure which reference applies.")
                .with_mime_type("text/markdown"),
        ];
        resources.extend(BEST_PRACTICES.iter().map(|topic| {
            Resource::new(topic.uri(), format!("best-practices/{}", topic.key))
                .with_title(topic.title)
                .with_description(topic.description)
                .with_mime_type("text/markdown")
        }));
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn list_resource_templates(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(ListResourceTemplatesResult::with_all_items(vec![
            ResourceTemplate::new("omnigraph://graphs/{graph}/schema", "schema")
                .with_title("Graph schema")
                .with_description("A graph's schema as `.pg` source, as `schema` returns it. Read it before writing a query.")
                .with_mime_type("text/plain"),
            ResourceTemplate::new("omnigraph://graphs/{graph}/branches", "branches")
                .with_title("Graph branches")
                .with_description("A graph's branch names, as `branches_list` returns them.")
                .with_mime_type("application/json"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let Some(resource) = GraphResource::parse(&request.uri) else {
            return Err(ErrorData::resource_not_found("resource not found", None));
        };
        // References are compiled into the binary: no graph, policy or I/O.
        if let Some(text) = resource.static_text() {
            return Ok(ReadResourceResult::new(vec![
                ResourceContents::text(text?, request.uri).with_mime_type("text/markdown"),
            ])
            .into());
        }
        let admitted = self
            .host
            .admit(context.extensions.get::<Parts>())
            .map_err(Refusal::from);
        let admitted = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => return Err(refusal.resource_error(&self.host)),
        };
        let permit = match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Err(Refusal::Protocol(ProtocolError::Capacity).resource_error(&self.host));
            }
        };
        let resource = resource.into_request().ok_or_else(|| {
            ErrorData::internal_error("static resources are answered without a graph read", None)
        })?;
        let uri = request.uri;
        let pending = self.host.start_resource(admitted, resource, permit);
        let result = observe(pending, context.ct.cancelled(), READ_DEADLINE).await;
        match result {
            Ok((mime_type, text)) => {
                if text.len() > RESULT_BYTES {
                    return Err(ErrorData::internal_error(
                        "the resource exceeds 1 MiB",
                        None,
                    ));
                }
                Ok(ReadResourceResult::new(vec![
                    ResourceContents::text(text, uri).with_mime_type(mime_type),
                ])
                .into())
            }
            Err(refusal) => Err(refusal.resource_error(&self.host)),
        }
    }
}

#[cfg(test)]
mod tests;
