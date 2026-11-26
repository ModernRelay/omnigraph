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
    AccessArguments, BlobClearArguments, BlobStatArguments, BranchArguments, BranchCreateArguments,
    BranchDeleteArguments, BranchDiffArguments, BranchMergeArguments, BranchesListArguments,
    ChangeFilters, ChangesPollArguments, CommitArguments, CommitChangesArguments,
    CommitsListArguments, DiffArguments, GraphArguments, InvalidArguments, LoadArguments,
    MergeBaseArguments, MutateArguments, QueryArguments, ToolCall, ToolRequest, change_query,
};
pub use resources::{ResourceOutput, ResourceRequest};
pub use result::{Encoded, HostError, ReadBudget};
pub use settings::{MCP_ALLOWED_HOSTS_ENV, MCP_SETUP_URL_ENV, McpSettings, format_bytes};

use axum::Router;
use axum::http::{StatusCode, request::Parts};
use catalog::*;
use resources::*;
use result::encodes_within;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData,
    Implementation, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult,
    PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult,
    Resource, ResourceContents, ResourceTemplate, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{RoleServer, ServerHandler};
use serde::Serialize;
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
    /// The encoded result exceeded [`RESULT_BYTES`], the memory cap.
    ResultTooLarge,
    /// A successful read's encoded result exceeded the read budget.
    ReadOverBudget(ReadBudget),
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
    ) -> Pending<Encoded>;
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
    docs_revision: Option<&str>,
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
        version,
        slots: Arc::new(Semaphore::new(16)),
        read_budget: settings.read_budget,
        instructions: Arc::from(instructions(&settings)),
        tools: Arc::from(tools(docs_revision)),
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
    version: String,
    slots: Arc<Semaphore>,
    /// [`McpSettings::read_budget`], which [`GraphTools::limit`] applies.
    read_budget: ReadBudget,
    /// Built once at boot from the settings.
    instructions: Arc<str>,
    /// Built once at boot; their guide links name the build's revision.
    tools: Arc<[Tool]>,
}

/// What a tool call produced, before its size is judged.
enum Answer {
    Output(Encoded),
    Refusal(HostError),
}

/// Why a tool call or resource read produced no handler result.
enum Refusal {
    Protocol(ProtocolError),
    Api(HostError),
}

impl Refusal {
    /// The host's canonical error for this refusal, in `write`'s context.
    fn host_error(self, host: &impl Host, write: bool) -> HostError {
        match self {
            Self::Protocol(error) => host.error(error, write),
            Self::Api(error) => error,
        }
    }

    /// What a resource read says about this refusal, beside its error body.
    fn resource_message(&self, status: Option<StatusCode>) -> &'static str {
        let refused = status.is_some_and(|status| status.is_client_error());
        match self {
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
            Self::Protocol(ProtocolError::ReadOverBudget(_)) => {
                "the resource exceeds the read budget"
            }
            Self::Protocol(ProtocolError::InvalidResult) => "could not encode MCP resource",
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
    /// The size rule: the bytes a complete response may take, text copy
    /// included, and the refusal past them. A successful read must fit the
    /// read budget, so a client takes it in whole and the model is told to
    /// narrow a broad read rather than handed a truncated one. A write's
    /// result and every refusal keep the memory cap, so the budget never
    /// hides what a call did. The compiled-in references are fixed text and
    /// never reach it.
    fn limit(&self, write: bool, refused: bool) -> (usize, ProtocolError) {
        if write || refused {
            (RESULT_BYTES, ProtocolError::ResultTooLarge)
        } else {
            (
                self.read_budget.bytes(),
                ProtocolError::ReadOverBudget(self.read_budget),
            )
        }
    }

    /// The one judgement of a complete response's size, by [`Self::limit`]:
    /// every tool result and resource read, successful or refused, passes
    /// here and nowhere else.
    fn fit<T: Serialize>(
        &self,
        response: &T,
        write: bool,
        refused: bool,
    ) -> Result<(), ProtocolError> {
        let (bytes, over) = self.limit(write, refused);
        if encodes_within(response, bytes) {
            Ok(())
        } else {
            Err(over)
        }
    }

    /// The response for a tool call's answer, with the status its refusal
    /// carries. An answer that does not fit is replaced by the host's
    /// refusal for the same call, which is fixed and small.
    fn deliver(&self, answer: Answer, write: bool) -> (CallToolResult, Option<StatusCode>) {
        match self.encode(answer, write) {
            Ok(response) => response,
            Err(error) => self
                .encode(Answer::Refusal(self.host.error(error, write)), write)
                .unwrap_or_else(|_| {
                    // Unreachable for a host that keeps `Host::error`'s
                    // bound; never send what the rule refused.
                    let text = ContentBlock::text("the MCP refusal could not be encoded");
                    (CallToolResult::error(vec![text]), None)
                }),
        }
    }

    fn encode(
        &self,
        answer: Answer,
        write: bool,
    ) -> Result<(CallToolResult, Option<StatusCode>), ProtocolError> {
        let refused = matches!(answer, Answer::Refusal(_));
        let (_, over) = self.limit(write, refused);
        let (result, status) = match answer {
            Answer::Output(output) => (CallToolResult::structured(output.value(over)?), None),
            Answer::Refusal(error) => {
                let status = error.status();
                (error.tool_result(over)?, status)
            }
        };
        self.fit(&result, write, refused)?;
        Ok((result, status))
    }

    /// A refused call's tool result.
    fn refusal_result(&self, refusal: Refusal, write: bool) -> CallToolResult {
        let error = refusal.host_error(&self.host, write);
        self.deliver(Answer::Refusal(error), write).0
    }

    /// A refused resource read: the same canonical error envelope in `data`,
    /// sized as a tool refusal is.
    fn resource_error(&self, refusal: Refusal) -> ErrorData {
        let unauthenticated = matches!(refusal, Refusal::Protocol(ProtocolError::Unauthenticated));
        let error = match &refusal {
            Refusal::Protocol(error) => self.host.error(*error, false),
            Refusal::Api(error) => error.clone(),
        };
        let (result, status) = self.deliver(Answer::Refusal(error), false);
        let message = refusal.resource_message(status);
        let data = result.structured_content;
        if status == Some(StatusCode::NOT_FOUND) {
            ErrorData::resource_not_found(message, data)
        } else if unauthenticated || status.is_some_and(|status| status.is_client_error()) {
            ErrorData::invalid_request(message, data)
        } else {
            ErrorData::internal_error(message, data)
        }
    }

    async fn run_tool<C>(
        &self,
        context: H::Context,
        request: ToolRequest,
        cancelled: C,
        deadline: Duration,
    ) -> Result<Encoded, Refusal>
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
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(
            "omnigraph-server",
            self.version.clone(),
        ))
        .with_instructions(self.instructions.to_string())
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(self.tools.to_vec()))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools.iter().find(|tool| tool.name == name).cloned()
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(spec) = spec(&request.name) else {
            return Err(ErrorData::invalid_params("unknown MCP tool", None));
        };
        let write = spec.writes();
        let admitted = match self.host.admit(context.extensions.get::<Parts>()) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                return Ok(self.refusal_result(Refusal::from(refusal), write).into());
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
            Ok(output) => self.deliver(Answer::Output(output), write).0,
            Err(refusal) => self.refusal_result(refusal, write),
        };
        Ok(result.into())
    }

    async fn list_resources(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mut resources = vec![
            Resource::new(GRAPHS_RESOURCE, "graphs")
                .with_title("Graphs")
                .with_description("Who your credential is and the graphs it holds any permission on, as `graphs` returns them.")
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
            Err(refusal) => return Err(self.resource_error(refusal)),
        };
        let permit = match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Err(self.resource_error(Refusal::Protocol(ProtocolError::Capacity)));
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
                let result = ReadResourceResult::new(vec![
                    ResourceContents::text(text, uri).with_mime_type(mime_type),
                ]);
                match self.fit(&result, false, false) {
                    Ok(()) => Ok(result.into()),
                    Err(error) => Err(self.resource_error(Refusal::Protocol(error))),
                }
            }
            Err(refusal) => Err(self.resource_error(refusal)),
        }
    }
}

#[cfg(test)]
mod tests;
