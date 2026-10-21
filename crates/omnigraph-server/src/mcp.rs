//! MCP transport over the existing HTTP handlers: every tool and resource calls
//! the handler its route calls, under the same actor and policy action.
//! The official SDK owns protocol framing, negotiation and cancellation.
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ErrorData, Implementation,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
    ResourceContents, ResourceTemplate, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{RoleServer, ServerHandler};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::api::{ErrorCode, InvokeStoredQueryRequest};
use crate::extract::{ApiJson, ApiPath, ApiQuery};
use crate::handlers::{self, QueryNamePath};
use crate::ingress::McpGraphRequest;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, GraphId, GraphKey};

pub(crate) const REQUEST_BYTES: usize = 64 * 1024;
const RESULT_BYTES: usize = 1024 * 1024;
const READ_DEADLINE: Duration = Duration::from_secs(30);
/// How long a caller waits for a write. The write itself is owned by the
/// server and continues past this; the caller then learns its outcome is
/// unknown. It stays below common client request timeouts (the MCP
/// TypeScript SDK defaults to 60 seconds), so a client on such a default
/// receives that answer instead of giving up first.
const WRITE_DEADLINE: Duration = READ_DEADLINE;
/// The MCP TypeScript SDK's default request timeout.
#[cfg(test)]
const COMMON_CLIENT_TIMEOUT: Duration = Duration::from_secs(60);
const RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// Environment equivalent of `--mcp-allow-adhoc`.
pub const MCP_ALLOW_ADHOC_ENV: &str = "OMNIGRAPH_MCP_ALLOW_ADHOC";
/// Environment equivalent of `--mcp-allow-writes`.
pub const MCP_ALLOW_WRITES_ENV: &str = "OMNIGRAPH_MCP_ALLOW_WRITES";
/// Comma-separated environment equivalent of repeated `--mcp-allowed-host`.
pub const MCP_ALLOWED_HOSTS_ENV: &str = "OMNIGRAPH_MCP_ALLOWED_HOSTS";

/// Process boot settings for `/mcp`. They select which opt-in tools are listed
/// and admitted; they never grant anything, since every tool still runs only
/// what the applied policy allows its caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpSettings {
    /// Lists and admits the ad-hoc `read` tool.
    pub allow_adhoc: bool,
    /// Lists and admits the write tools.
    pub allow_writes: bool,
    /// `Host` values admitted besides the SDK's loopback names and the OIDC
    /// resource authority, as `host` or `host:port`.
    pub allowed_hosts: Vec<String>,
}

impl McpSettings {
    /// The boot flags, each also set by its environment variable; allowed
    /// hosts from both sources are combined.
    ///
    /// # Errors
    ///
    /// An allowed host that is not a bare `host` or `host:port`.
    pub fn resolve(
        allow_adhoc: bool,
        allow_writes: bool,
        allowed_hosts: Vec<String>,
    ) -> color_eyre::eyre::Result<Self> {
        let mut hosts = allowed_hosts;
        if let Ok(value) = std::env::var(MCP_ALLOWED_HOSTS_ENV) {
            hosts.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|host| !host.is_empty())
                    .map(str::to_owned),
            );
        }
        for host in &hosts {
            validate_allowed_host(host)?;
        }
        Ok(Self {
            allow_adhoc: allow_adhoc || env_flag(MCP_ALLOW_ADHOC_ENV),
            allow_writes: allow_writes || env_flag(MCP_ALLOW_WRITES_ENV),
            allowed_hosts: hosts,
        })
    }
}

/// Accept exactly an authority the SDK compares with `Host`: a host name or
/// address with an optional port, and nothing else.
fn validate_allowed_host(host: &str) -> color_eyre::eyre::Result<()> {
    let parsed = url::Url::parse(&format!("http://{host}"));
    let valid = parsed.as_ref().is_ok_and(|url| {
        url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && !host.contains(['/', '?', '#', '@'])
    });
    if valid {
        Ok(())
    } else {
        Err(color_eyre::eyre::eyre!(
            "--mcp-allowed-host must be a host or host:port, got `{host}`"
        ))
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        let value = value.trim();
        !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false")
    })
}

/// Whether `path` is protected-resource metadata: the root document or the
/// path-suffixed location the challenge advertises.
pub(crate) fn is_resource_metadata_path(path: &str) -> bool {
    path.strip_prefix(RESOURCE_METADATA_PATH)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// The metadata URL a client derives from the resource identifier (RFC 9728
/// §3.1): the well-known prefix inserted before the resource's path.
fn advertised_metadata_url(resource: &str) -> Option<url::Url> {
    let mut url = url::Url::parse(resource).ok()?;
    let path = format!(
        "{RESOURCE_METADATA_PATH}{}",
        url.path().trim_end_matches('/')
    );
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

/// The metadata path this server advertises, fixed by its boot binding.
#[derive(Clone)]
pub(crate) struct AdvertisedMetadataPath(Arc<str>);

/// Mounted when the server has a bearer credential source. Discovery metadata
/// and the OAuth challenge exist only with an OIDC resource. Every HTTP request
/// authenticates again; no MCP session retains an actor.
pub(crate) fn router(state: AppState) -> Router<AppState> {
    let handler = GraphTools {
        state: state.clone(),
        slots: Arc::new(Semaphore::new(16)),
    };
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    config.max_request_body_bytes = REQUEST_BYTES;
    config
        .allowed_hosts
        .extend(state.mcp_settings().allowed_hosts.iter().cloned());
    let oidc = state.oidc_resource_url().is_some();
    if let Some(resource) = state
        .oidc_resource_url()
        .and_then(|url| url::Url::parse(&url).ok())
    {
        // Retain the SDK's localhost protection; add only the configured public
        // authority, never an origin or Host copied from an inbound request.
        config
            .allowed_hosts
            .push(resource[url::Position::BeforeHost..url::Position::AfterPort].into());
        config
            .allowed_origins
            .push(resource.origin().ascii_serialization());
    }
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let protected = Router::new()
        .route_service("/mcp", service)
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            |State(state): State<AppState>, request: Request, next: Next| async move {
                crate::ingress::admit(&state, request, next).await
            },
        ))
        .route_layer(middleware::from_fn(
            move |request: Request, next: Next| async move {
                // Without an OIDC resource there is no origin to allow. The
                // SDK treats an empty origin list as unrestricted, so refuse
                // browser requests here; native clients send no `Origin`.
                if !oidc && request.headers().contains_key(header::ORIGIN) {
                    return (
                        StatusCode::FORBIDDEN,
                        "Forbidden: Origin header is not allowed",
                    )
                        .into_response();
                }
                next.run(request).await
            },
        ))
        .route_layer(middleware::from_fn(handlers::require_expected_actor))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            handlers::require_bearer_auth,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            resource_challenge,
        ));
    // Computed once from the fixed binding. A wildcard route compared with the
    // raw request path keeps the configured path out of the router's syntax.
    let advertised = state
        .oidc_resource_url()
        .and_then(|resource| advertised_metadata_url(&resource))
        .map(|url| AdvertisedMetadataPath(Arc::from(url.path())));
    let mut discovery = Router::new().route(RESOURCE_METADATA_PATH, get(resource_metadata));
    if let Some(advertised) = advertised {
        discovery = discovery.route(
            &format!("{RESOURCE_METADATA_PATH}/{{*resource_path}}"),
            get(resource_metadata_at_resource_path).layer(Extension(advertised)),
        );
    }
    discovery.merge(protected)
}

#[utoipa::path(get, path = "/.well-known/oauth-protected-resource", tag = "management",
    operation_id = "oauthProtectedResourceMetadata",
    responses((status = 200, description = "Public OAuth protected resource metadata", body = Value),
              (status = 404, description = "OAuth resource identity is not configured")))]
pub(crate) async fn resource_metadata(State(state): State<AppState>) -> Response {
    match state.oidc_resource_metadata() {
        Some(metadata) => ([(header::CACHE_CONTROL, "no-store")], Json(metadata)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The same metadata at the location the `/mcp` challenge advertises for a
/// resource identifier with a path: the well-known prefix followed by that
/// path (`/.well-known/oauth-protected-resource/mcp` for `https://host/mcp`).
/// Every other suffix is 404.
#[utoipa::path(get, path = "/.well-known/oauth-protected-resource/{resource_path}", tag = "management",
    operation_id = "oauthProtectedResourceMetadataAtResourcePath",
    params(("resource_path" = String, Path, description = "The configured resource identifier's path, without its leading `/`")),
    responses((status = 200, description = "Public OAuth protected resource metadata", body = Value),
              (status = 404, description = "Not the configured resource's path, or OAuth resource identity is not configured")))]
pub(crate) async fn resource_metadata_at_resource_path(
    State(state): State<AppState>,
    Extension(AdvertisedMetadataPath(advertised)): Extension<AdvertisedMetadataPath>,
    uri: Uri,
) -> Response {
    if uri.path() == &*advertised {
        resource_metadata(State(state)).await
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn resource_challenge(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // RFC 6750 §3.1: `invalid_token` answers a presented bearer credential
    // that was refused; a request without one gets no error code.
    let presented = request
        .headers()
        .get_all(header::AUTHORIZATION)
        .iter()
        .any(|value| {
            value
                .as_bytes()
                .get(..7)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case(b"bearer "))
        });
    let mut response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        if let Some(url) = state
            .oidc_resource_url()
            .and_then(|resource| advertised_metadata_url(&resource))
        {
            let error = if presented {
                "error=\"invalid_token\", "
            } else {
                ""
            };
            if let Ok(value) =
                HeaderValue::from_str(&format!("Bearer {error}resource_metadata=\"{url}\""))
            {
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, value);
            }
        }
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Clone)]
struct GraphTools {
    state: AppState,
    slots: Arc<Semaphore>,
}

/// Why a tool call or resource read produced no handler result.
enum Refusal {
    Unauthenticated,
    MissingContext,
    Capacity,
    Cancelled,
    Deadline,
    Api(ApiError),
}

impl Refusal {
    /// The refusal as the error an HTTP route would answer. A write's caller
    /// learns when its wait ended before the owned write did, or when the
    /// server could not tell whether the write completed: the code is
    /// `outcome_unknown`, to be reconciled, never replayed blindly.
    fn into_error(self, write: bool) -> ApiError {
        let unknown = "The write keeps running and its outcome is unknown. Before replaying it, read the target (`read` or `snapshot`, comparing `graph_commit_id`) or list the branch with `commits_list`, and replay a `mutate` with `if_graph_commit`.";
        let wait_ended = |code, reason: &str| {
            let mut error = ApiError::new(code, format!("{reason} {unknown}")).after_owned_write();
            error.completion_uncertain = true;
            error
        };
        match self {
            Refusal::Unauthenticated => {
                ApiError::unauthorized("A verified bearer credential is required.")
            }
            Refusal::MissingContext => ApiError::internal("MCP admission context is missing."),
            Refusal::Capacity => ApiError::too_many_requests(
                "The server is handling its maximum concurrent MCP tool calls.",
            ),
            Refusal::Cancelled if write => {
                wait_ended(ErrorCode::Cancelled, "The write's call was cancelled.")
            }
            Refusal::Cancelled => ApiError::new(ErrorCode::Cancelled, "The read was cancelled."),
            Refusal::Deadline if write => wait_ended(
                ErrorCode::DeadlineExceeded,
                "The write passed its 30 second wait.",
            ),
            Refusal::Deadline => ApiError::new(
                ErrorCode::DeadlineExceeded,
                "The read exceeded its 30 second deadline.",
            ),
            // A write tool's failure the server cannot settle may follow the
            // write, wherever in the call it arose, and a result it could not
            // deliver was built after the write ran.
            Refusal::Api(error)
                if write
                    && (error.completion_uncertain()
                        || error.code == ErrorCode::ResultTooLarge) =>
            {
                error.after_owned_write()
            }
            Refusal::Api(error) => error,
        }
    }

    /// The `isError` tool result.
    fn tool_result(self, write: bool) -> CallToolResult {
        tool_error(self.into_error(write))
    }

    /// The JSON-RPC error a resource read answers with, carrying the same
    /// error body as `data`.
    fn resource_error(self) -> ErrorData {
        let status = match &self {
            Refusal::Api(error) => Some(error.status()),
            _ => None,
        };
        let refused = status.is_some_and(|status| status.is_client_error())
            || matches!(self, Refusal::Unauthenticated);
        let message = match &self {
            Refusal::Api(_) if status == Some(StatusCode::NOT_FOUND) => "resource not found",
            Refusal::Api(_) if refused => "resource read refused",
            Refusal::Api(_) => "resource read failed",
            Refusal::Unauthenticated => "a verified bearer credential is required",
            Refusal::MissingContext => "MCP admission context is missing",
            Refusal::Capacity => "the server is handling its maximum concurrent MCP calls",
            Refusal::Cancelled => "the read was cancelled",
            Refusal::Deadline => "the read exceeded its 30 second deadline",
        };
        let data = tool_error(self.into_error(false)).structured_content;
        if status == Some(StatusCode::NOT_FOUND) {
            ErrorData::resource_not_found(message, data)
        } else if refused {
            ErrorData::invalid_request(message, data)
        } else {
            ErrorData::internal_error(message, data)
        }
    }
}

/// The admitted HTTP request's actor and reservations, which the SDK carries
/// into its task as request extensions.
struct Admitted {
    actor: AuthenticatedActor,
    observer: crate::operations::ReadObserver,
    input: IngressLease,
    graph: McpGraphRequest,
}

fn admitted(context: &RequestContext<RoleServer>) -> Result<Admitted, Refusal> {
    let parts = context.extensions.get::<axum::http::request::Parts>();
    let actor = parts
        .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
        .cloned()
        .ok_or(Refusal::Unauthenticated)?;
    // Reuse the admitted request's one lease and observer instead of admitting
    // again after collection or releasing them before the SDK task finishes.
    let parts = parts.ok_or(Refusal::MissingContext)?;
    let observer = parts.extensions.get::<crate::operations::ReadObserver>();
    let input = parts.extensions.get::<IngressLease>();
    let graph = parts.extensions.get::<McpGraphRequest>();
    match (observer, input, graph) {
        (Some(observer), Some(input), Some(graph)) => Ok(Admitted {
            actor,
            observer: observer.clone(),
            input: input.clone(),
            graph: graph.clone(),
        }),
        _ => Err(Refusal::MissingContext),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArguments {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphArguments {
    graph: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchArguments {
    graph: String,
    #[serde(default)]
    branch: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessArguments {
    graph: String,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    target_branch: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitsListArguments {
    graph: String,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    page_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitArguments {
    graph: String,
    commit_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryArguments {
    graph: String,
    name: String,
    #[serde(default)]
    params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    branch: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArguments {
    graph: String,
    query: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    snapshot: Option<String>,
}
/// The change vocabulary's filters and page continuation, sent to the route
/// as the same repeated query parameters an HTTP caller sends.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ChangeFilters {
    #[serde(default)]
    kind: Vec<String>,
    #[serde(default)]
    r#type: Vec<String>,
    #[serde(default)]
    op: Vec<String>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitChangesArguments {
    graph: String,
    commit_id: String,
    #[serde(flatten)]
    filters: ChangeFilters,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesPollArguments {
    graph: String,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(flatten)]
    filters: ChangeFilters,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MutateArguments {
    graph: String,
    query: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    params: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    if_graph_commit: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadArguments {
    graph: String,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    from: Option<String>,
    mode: String,
    data: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchCreateArguments {
    graph: String,
    name: String,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    from_commit: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchDeleteArguments {
    graph: String,
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchMergeArguments {
    graph: String,
    source: String,
    #[serde(default)]
    target: Option<String>,
}

impl ServerHandler for GraphTools {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(
            "omnigraph-server",
            crate::api::build::reported_version(env!("CARGO_PKG_VERSION")),
        ))
        .with_instructions(instructions(self.state.mcp_settings()))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools(
            self.state.mcp_settings(),
        )))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools(self.state.mcp_settings())
            .into_iter()
            .find(|tool| tool.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(spec) = spec(self.state.mcp_settings(), &request.name) else {
            return Err(ErrorData::invalid_params("unknown MCP tool", None));
        };
        let write = spec.gate == Gate::Writes;
        let admitted = match admitted(&context) {
            Ok(admitted) => admitted,
            Err(refusal) => return Ok(refusal.tool_result(write).into()),
        };
        let service = self.clone();
        let execution_input = admitted.input.clone();
        let execution_graph = admitted.graph.clone();
        let actor = admitted.actor;
        let result = self
            .run_owned(
                admitted.observer,
                admitted.input,
                admitted.graph,
                async move {
                    service
                        .execute(request, actor, execution_input, execution_graph)
                        .await
                },
                context.ct.cancelled(),
                if write { WRITE_DEADLINE } else { READ_DEADLINE },
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(refusal) => refusal.tool_result(write),
        };
        Ok(fit_result(result, write).into())
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
        let admitted = match admitted(&context) {
            Ok(admitted) => admitted,
            Err(refusal) => return Err(refusal.resource_error()),
        };
        let service = self.clone();
        let execution_graph = admitted.graph.clone();
        let actor = admitted.actor;
        let uri = request.uri;
        let result = self
            .run_owned(
                admitted.observer,
                admitted.input,
                admitted.graph,
                async move { service.read(resource, actor, execution_graph).await },
                context.ct.cancelled(),
                READ_DEADLINE,
            )
            .await;
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
            Err(refusal) => Err(refusal.resource_error()),
        }
    }
}

const GRAPHS_RESOURCE: &str = "omnigraph://graphs";
const BEST_PRACTICES_INDEX: &str = "omnigraph://best-practices/index";

/// One skill reference served as a resource. The body is the agent skill's
/// own `references/<key>.md`, embedded by `omnigraph-skill`; the description
/// is what a model reads at `resources/list` time to decide whether to fetch it.
struct BestPractice {
    key: &'static str,
    title: &'static str,
    description: &'static str,
}

impl BestPractice {
    fn uri(&self) -> String {
        format!("omnigraph://best-practices/{}", self.key)
    }

    /// The embedded reference, or `None` when the skill no longer carries it
    /// as UTF-8; a unit test keeps every listed key present.
    fn body(&self) -> Option<&'static str> {
        let path = format!("references/{}.md", self.key);
        omnigraph_skill::file(&path).and_then(|file| std::str::from_utf8(file.contents).ok())
    }
}

/// The references an MCP caller can act on. The skill's other references
/// cover the CLI, cluster configuration, server policy and upgrades, which are
/// operator work rather than MCP tools.
const BEST_PRACTICES: &[BestPractice] = &[
    BestPractice {
        key: "queries",
        title: "GQ query best practices",
        description: "Read before writing a query for `read`, `query` or `mutate`. Covers .gq grammar, choosing a declared query by name, parameter declarations, the PascalCase to lowerCamelCase edge casing trap, search operators that need a trailing `limit`, and the non-nullable lint rule.",
    },
    BestPractice {
        key: "data",
        title: "Data load patterns",
        description: "Read before calling `load`. Covers choosing `merge`, `append` or `overwrite`, the branch, load, verify, merge loop for large or risky writes, embeddings that `merge` does not recompute, and when to use `mutate` instead.",
    },
    BestPractice {
        key: "schema",
        title: "Schema authoring and evolution",
        description: "Read to interpret `schema` and .pg declarations. Covers decorators, inline enums, stable renames, and add-optional then backfill evolution. Schema changes are operator-owned through cluster apply, not an MCP tool.",
    },
    BestPractice {
        key: "search",
        title: "Vector and full-text search",
        description: "Read before using `nearest`, `bm25` or `rrf`. Covers scoping first and ranking second, schema-declared embeddings, and the trailing `limit` requirement. Match vector dimensions and models to the live schema; CLI examples are not MCP tools.",
    },
    BestPractice {
        key: "changes",
        title: "Commits, change feeds and branch statements",
        description: "Read before `commits_changes`, `changes_poll` or a conditional `mutate`. Covers exact read and write receipts, 412 refusals, paginated feeds, baseline resets, and branch statement outcomes.",
    },
];

fn best_practices_index() -> String {
    let mut text = String::from(
        "# OmniGraph best-practices index\n\nThese references come from the OmniGraph agent skill shipped with this server.\n\n| Resource | Read before |\n|---|---|\n",
    );
    for topic in BEST_PRACTICES {
        text.push_str(&format!("| `{}` | {} |\n", topic.uri(), topic.description));
    }
    text
}

/// A resource URI this server answers.
enum GraphResource {
    Graphs,
    Schema(String),
    Branches(String),
    BestPracticesIndex,
    BestPractice(&'static BestPractice),
}

impl GraphResource {
    fn parse(uri: &str) -> Option<Self> {
        if uri == GRAPHS_RESOURCE {
            return Some(Self::Graphs);
        }
        if uri == BEST_PRACTICES_INDEX {
            return Some(Self::BestPracticesIndex);
        }
        if let Some(key) = uri.strip_prefix("omnigraph://best-practices/") {
            return BEST_PRACTICES
                .iter()
                .find(|topic| topic.key == key)
                .map(Self::BestPractice);
        }
        let rest = uri.strip_prefix("omnigraph://graphs/")?;
        let (graph, kind) = rest.split_once('/')?;
        if graph.is_empty() {
            return None;
        }
        match kind {
            "schema" => Some(Self::Schema(graph.to_owned())),
            "branches" => Some(Self::Branches(graph.to_owned())),
            _ => None,
        }
    }

    fn static_text(&self) -> Option<Result<String, ErrorData>> {
        match self {
            Self::BestPracticesIndex => Some(Ok(best_practices_index())),
            Self::BestPractice(topic) => Some(topic.body().map(str::to_owned).ok_or_else(|| {
                ErrorData::internal_error("the embedded skill does not carry this reference", None)
            })),
            Self::Graphs | Self::Schema(_) | Self::Branches(_) => None,
        }
    }
}

impl GraphTools {
    /// Tool-shaped wrapper over [`Self::run_owned`] for a read.
    #[cfg(test)]
    async fn run_read<F, C>(
        &self,
        observer: crate::operations::ReadObserver,
        input: IngressLease,
        graph: McpGraphRequest,
        operation: F,
        cancelled: C,
        deadline: Duration,
    ) -> CallToolResult
    where
        F: Future<Output = Result<CallToolResult, ApiError>> + Send + 'static,
        C: Future<Output = ()>,
    {
        match self
            .run_owned(observer, input, graph, operation, cancelled, deadline)
            .await
        {
            Ok(result) => result,
            Err(refusal) => refusal.tool_result(false),
        }
    }

    /// MCP's timeout and cancellation bound the caller's observation. The
    /// execution retains the SDK-independent observer, input and MCP slot
    /// until it completes, including its owned query-worker joins and any
    /// write it registered through the owned-write boundary.
    async fn run_owned<T, F, C>(
        &self,
        observer: crate::operations::ReadObserver,
        input: IngressLease,
        graph: McpGraphRequest,
        operation: F,
        cancelled: C,
        deadline: Duration,
    ) -> Result<T, Refusal>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ApiError>> + Send + 'static,
        C: Future<Output = ()>,
    {
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            return Err(Refusal::Capacity);
        };
        let read = observer.spawn_read((input, permit, graph), operation);
        tokio::select! {
            _ = cancelled => Err(Refusal::Cancelled),
            result = tokio::time::timeout(deadline, read.result()) => {
                match result {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(error)) => Err(Refusal::Api(error)),
                    Err(_) => Err(Refusal::Deadline),
                }
            }
        }
    }

    async fn execute(
        &self,
        request: CallToolRequestParams,
        actor: AuthenticatedActor,
        input: IngressLease,
        graph: McpGraphRequest,
    ) -> Result<CallToolResult, ApiError> {
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        let state = || State(self.state.clone());
        match request.name.as_ref() {
            "health" => {
                let _: NoArguments = arguments_as(arguments)?;
                bounded_result(&handlers::server_health().await.0)
            }
            "graphs" => {
                let _: NoArguments = arguments_as(arguments)?;
                bounded_result(&self.discover(actor).await?)
            }
            "schema" => {
                let args: GraphArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output =
                    handlers::server_schema_get(Extension(handle), Some(Extension(actor))).await?;
                bounded_result(&output.0)
            }
            "queries" => {
                let args: GraphArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let mut output =
                    handlers::server_list_queries(Extension(handle), Some(Extension(actor)))
                        .await?
                        .0;
                output.queries.retain(|query| !query.mutation);
                bounded_result(&output)
            }
            "query" => {
                let args: QueryArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let body = serde_json::to_vec(&InvokeStoredQueryRequest {
                    params: args.params.map(Value::Object),
                    branch: args.branch,
                    snapshot: None,
                    expect_mutation: Some(false),
                })
                .map_err(|_| ApiError::bad_request("invalid query parameters"))?;
                let output = handlers::server_invoke_query(
                    state(),
                    Extension(handle),
                    Extension(input),
                    Some(Extension(actor)),
                    ApiPath(QueryNamePath { name: args.name }),
                    HeaderMap::new(),
                    Bytes::from(body),
                )
                .await?;
                bounded_result(&output.0)
            }
            "read" => {
                let args: ReadArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request = request_body(json!({
                    "query": args.query,
                    "name": args.name,
                    "params": args.params,
                    "branch": args.branch,
                    "snapshot": args.snapshot,
                }))?;
                let output = handlers::server_query(
                    state(),
                    Extension(handle),
                    Some(Extension(actor)),
                    ApiJson(request),
                )
                .await?;
                bounded_result(&output.0)
            }
            "snapshot" => {
                let args: BranchArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_snapshot(
                    Extension(handle),
                    Some(Extension(actor)),
                    ApiQuery(crate::api::SnapshotQuery {
                        branch: args.branch,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            "access" => {
                let args: AccessArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::access_body(
                    &handle,
                    Some(&actor),
                    crate::api::AccessQuery {
                        branch: args.branch,
                        target_branch: args.target_branch,
                    },
                )?;
                bounded_result(&output)
            }
            "branches_list" => {
                let args: GraphArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output =
                    handlers::server_branch_list(Extension(handle), Some(Extension(actor))).await?;
                bounded_result(&output.0)
            }
            "commits_list" => {
                let args: CommitsListArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_commit_list(
                    Extension(handle),
                    Some(Extension(actor)),
                    ApiQuery(crate::api::CommitListQuery {
                        branch: args.branch,
                        unpublished: false,
                        unattached: false,
                        limit: args.limit,
                        page_token: args.page_token,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            "commits_get" => {
                let args: CommitArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_commit_show(
                    Extension(handle),
                    Some(Extension(actor)),
                    ApiPath(handlers::CommitPath {
                        commit_id: args.commit_id,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            "commits_changes" => {
                let args: CommitChangesArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let query = change_query(&[], &args.filters);
                let output = handlers::server_commit_changes(
                    Extension(handle),
                    Some(Extension(actor)),
                    ApiPath(handlers::CommitPath {
                        commit_id: args.commit_id,
                    }),
                    RawQuery(query),
                )
                .await?;
                bounded_result(&output.0)
            }
            "changes_poll" => {
                let args: ChangesPollArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let query = change_query(
                    &[
                        ("branch", args.branch.as_deref()),
                        ("cursor", args.cursor.as_deref()),
                        ("start", args.start.as_deref()),
                    ],
                    &args.filters,
                );
                let output = handlers::server_changes_feed(
                    Extension(handle),
                    Some(Extension(actor)),
                    RawQuery(query),
                )
                .await?;
                bounded_result(&output.0)
            }
            "mutate" => {
                let ingress = self.write_ingress(&arguments)?;
                let args: MutateArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request = ApiJson(request_body(json!({
                    "query": args.query,
                    "name": args.name,
                    "params": args.params,
                    "branch": args.branch,
                }))?);
                let output = match args.if_graph_commit {
                    Some(head) => {
                        let mut headers = HeaderMap::new();
                        headers.insert(
                            crate::api::GRAPH_COMMIT_PRECONDITION_HEADER,
                            HeaderValue::from_str(&head).map_err(|_| {
                                ApiError::bad_request("if_graph_commit is not a valid commit id")
                            })?,
                        );
                        handlers::server_mutate_if_graph_commit(
                            state(),
                            Extension(handle),
                            Extension(ingress),
                            Some(Extension(actor)),
                            headers,
                            request,
                        )
                        .await?
                    }
                    None => {
                        handlers::server_mutate(
                            state(),
                            Extension(handle),
                            Extension(ingress),
                            Some(Extension(actor)),
                            HeaderMap::new(),
                            request,
                        )
                        .await?
                    }
                };
                bounded_result(&output.0)
            }
            "load" => {
                let ingress = self.write_ingress(&arguments)?;
                let args: LoadArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request = request_body(json!({
                    "branch": args.branch,
                    "from": args.from,
                    "mode": args.mode,
                    "data": args.data,
                }))?;
                let output = handlers::server_load(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    ApiJson(request),
                )
                .await?;
                bounded_result(&output.0)
            }
            "branches_create" => {
                let ingress = self.write_ingress(&arguments)?;
                let args: BranchCreateArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_branch_create(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    ApiJson(crate::api::BranchCreateRequest {
                        from: args.from,
                        from_commit: args.from_commit,
                        name: args.name,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            "branches_delete" => {
                let ingress = self.write_ingress(&arguments)?;
                let args: BranchDeleteArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_branch_delete(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    ApiPath(handlers::BranchPath { branch: args.name }),
                )
                .await?;
                bounded_result(&output.0)
            }
            "branches_merge" => {
                let ingress = self.write_ingress(&arguments)?;
                let args: BranchMergeArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request = request_body(json!({
                    "source": args.source,
                    "target": args.target,
                }))?;
                let output = handlers::server_branch_merge(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    ApiJson(request),
                )
                .await?;
                bounded_result(&output.0)
            }
            _ => Err(ApiError::bad_request("unknown MCP tool")),
        }
    }

    /// One resource body and its media type, from the same handler and policy
    /// action as the matching tool.
    async fn read(
        &self,
        resource: GraphResource,
        actor: AuthenticatedActor,
        graph: McpGraphRequest,
    ) -> Result<(&'static str, String), ApiError> {
        match resource {
            GraphResource::Graphs => {
                Ok(("application/json", pretty(&self.discover(actor).await?)?))
            }
            GraphResource::Schema(name) => {
                let (handle, actor) = self.graph(&name, actor, &graph)?;
                let output =
                    handlers::server_schema_get(Extension(handle), Some(Extension(actor))).await?;
                Ok(("text/plain", output.0.schema_source))
            }
            GraphResource::Branches(name) => {
                let (handle, actor) = self.graph(&name, actor, &graph)?;
                let output =
                    handlers::server_branch_list(Extension(handle), Some(Extension(actor))).await?;
                Ok(("application/json", pretty(&output.0)?))
            }
            GraphResource::BestPracticesIndex | GraphResource::BestPractice(_) => Err(
                ApiError::internal("static resources are answered without a graph read"),
            ),
        }
    }

    /// Graph ids and names. An identity credential uses `GET /graphs/discovery`,
    /// which admits every identity; any other credential uses `GET /graphs`
    /// under the `graph_list` action, narrowed to the same two fields.
    async fn discover(
        &self,
        actor: AuthenticatedActor,
    ) -> Result<crate::api::GraphDiscoveryResponse, ApiError> {
        let state = State(self.state.clone());
        if actor.is_identity() {
            return Ok(
                handlers::server_graphs_discovery(state, Some(Extension(actor)))
                    .await?
                    .0,
            );
        }
        let listed = handlers::server_graphs_list(state, Some(Extension(actor)))
            .await?
            .0;
        Ok(crate::api::GraphDiscoveryResponse {
            graphs: listed
                .graphs
                .into_iter()
                .map(|graph| crate::api::GraphDiscoveryEntry {
                    display_name: graph.graph_id.clone(),
                    graph_id: graph.graph_id,
                })
                .collect(),
        })
    }

    /// A write reserves write ingress for its argument bytes, as the write's
    /// HTTP route does at admission; the handler's per-actor admission and
    /// owned-write registration follow unchanged.
    fn write_ingress(&self, arguments: &Value) -> Result<IngressLease, ApiError> {
        let bytes = serde_json::to_vec(arguments)
            .map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))?
            .len();
        self.state
            .workload
            .try_ingress(bytes as u64)
            .map_err(ApiError::from_workload_reject)
    }

    fn graph(
        &self,
        name: &str,
        actor: AuthenticatedActor,
        graph: &McpGraphRequest,
    ) -> Result<(GraphRequest, AuthenticatedActor), ApiError> {
        let id = GraphId::try_from(name.to_string())
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let handle =
            handlers::resolve_registered_graph(&self.state, &GraphKey::cluster(id), Some(&actor))?;
        if graph.set(handle.clone()).is_err() {
            return Err(ApiError::internal(
                "MCP request selected its graph more than once",
            ));
        }
        Ok((handle, actor))
    }
}

fn pretty<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serde_json::to_string_pretty(value)
        .map_err(|_| ApiError::internal("could not encode MCP resource"))
}

fn arguments_as<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))
}

/// A route's typed request body, built from tool arguments through the same
/// deserializer its JSON extractor uses.
fn request_body<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))
}

/// The query string an HTTP caller would send for these change arguments.
fn change_query(scalars: &[(&str, Option<&str>)], filters: &ChangeFilters) -> Option<String> {
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

/// Which boot setting, if any, lists and admits a tool.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Gate {
    Always,
    Adhoc,
    Writes,
}

impl Gate {
    fn open(self, settings: &McpSettings) -> bool {
        match self {
            Gate::Always => true,
            Gate::Adhoc => settings.allow_adhoc,
            Gate::Writes => settings.allow_writes,
        }
    }
}

/// What a tool does to the graph, as MCP annotation hints.
#[derive(Clone, Copy)]
enum Effect {
    Read,
    /// Creates state without replacing any.
    Additive,
    Destructive {
        idempotent: bool,
    },
}

struct ToolSpec {
    name: &'static str,
    title: &'static str,
    description: &'static str,
    input: fn() -> Value,
    /// Success shape, for tools whose result is fixed. The declared output
    /// schema also admits the error envelope: clients validate any structured
    /// content against it, including an `isError` result's.
    output: Option<fn() -> Value>,
    effect: Effect,
    gate: Gate,
}

const WRITE_CONTRACT: &str = " Never retried by the server. If the result says `outcome_unknown`, the write may or may not have happened: read the target and reconcile before replaying.";

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "health",
        title: "Server health",
        description: "Return the server's status, version and the storage format version it serves.",
        input: no_arguments_schema,
        output: Some(health_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "graphs",
        title: "List graphs",
        description: "List the graphs this server serves: each graph's `graph_id` and display name. Every other tool takes one of these ids as `graph`. Listing a graph does not mean your credential may read it. A credential that is not an identity needs the `graph_list` permission.",
        input: no_arguments_schema,
        output: Some(graphs_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "schema",
        title: "Read a graph's schema",
        description: "Return a graph's schema as `.pg` source (`schema_source`): its node and edge types, properties with their types, keys, edge endpoints and declared embeddings. Call this before writing any query: GQ names types, properties and edges exactly as declared here, and edges are matched in lowerCamelCase.",
        input: graph_schema,
        output: Some(schema_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "queries",
        title: "List stored reads",
        description: "List a graph's stored reads: each `name`, its description and instruction, and typed `params` (`name`, `kind`, `nullable`; `item_kind` for lists, `vector_dim` for vectors). A non-nullable parameter is required. Run one with `query`. Invocation is authorized separately, so a listed read can still be refused.",
        input: graph_schema,
        output: Some(queries_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "query",
        title: "Run a stored read",
        description: "Run a stored read by `name` with `params` keyed by its declared parameter names (see `queries`). Reads `branch`, `main` when omitted. Returns `rows`, `row_count` and the read's `graph_commit_id`. Stored mutations are refused.",
        input: query_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "read",
        title: "Run a GQ read",
        description: "Run a GQ read query you write. Call `schema` first. Declare every value as a typed parameter (`query people($min: I32) { match { $p: Person $p.age >= $min } return { $p.name } }`) and pass the values in `params`; pick one declared query with `name` when the source declares several. Reads `branch` (default `main`) or a `snapshot`, never both. Search operators (`nearest`, `bm25`, `rrf`) need a trailing `limit`. Also answers `branch list`, `branch diff <source> [into <target>]`, `branch merge-base …`, `branch merge … dry run` and `explain query …`, sent without `branch`, `snapshot`, `name` or `params`; and `explain load csv …`, which requires `change` permission before probing source metadata and answers the accepted load plan without reading payload bytes or publishing (`branch` allowed, never `snapshot`). Mutations and branch writes are refused. Returns `rows`, `row_count` and `graph_commit_id`.",
        input: read_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Adhoc,
    },
    ToolSpec {
        name: "snapshot",
        title: "Branch snapshot",
        description: "Return a branch's current snapshot (default `main`): the graph manifest version, the storage format version and every node and edge table with its entity count. Use it to judge a graph's size before querying.",
        input: branch_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "access",
        title: "Your access",
        description: "Return what your credential may do on a graph: your `actor`, the actions allowed whatever the branch (`graph_wide`), and, for a `branch` you name, the actions allowed on it (`actions`). Name a `target_branch` too to learn whether you may create it from `branch` or merge `branch` into it. Check this before a write instead of learning from a refusal. Branches are judged by name and need not exist.",
        input: access_schema,
        output: Some(access_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "branches_list",
        title: "List branches",
        description: "List a graph's branch names.",
        input: graph_schema,
        output: Some(branches_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_list",
        title: "List commits",
        description: "List a branch's commits (default `main`), newest first, each with its `graph_commit_id`, parents, actor and time. Pass `limit` to page a long history: the page names `head_commit_id` and continues with its `next_page_token` as `page_token`, keeping the same `branch`.",
        input: commits_list_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_get",
        title: "Get a commit",
        description: "Return one commit by its `graph_commit_id`.",
        input: commit_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_changes",
        title: "A commit's entity changes",
        description: "Return one bounded page of a commit's exact entity changes relative to its first parent, each with `before` and `after`. Filter by `kind`, `type` and `op`. Continue with the returned `next_page_token` as `page_token`, keeping the same filters; a page token is not a feed cursor.",
        input: commit_changes_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "changes_poll",
        title: "Poll a change feed",
        description: "Read one bounded page of a branch's change feed (default `main`), at least once, in first-parent commit order. Start from `start` (`now`, the default, `beginning`, or `after:<commit id>`), a durable `cursor` from an earlier terminal page, or a `page_token`; these three are mutually exclusive. Keep filters unchanged across pages. Only a terminal page's `cursor` is durable: apply its complete commit blocks idempotently by `graph_commit_id`, then keep the cursor. A 410 `change_feed_gap` needs a new baseline, which this tool does not take.",
        input: changes_poll_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "mutate",
        title: "Run a GQ mutation",
        description: "Run a GQ mutation (`insert`, `update`, `delete`) on `branch` (default `main`) as one atomic commit, with every value passed as a declared, typed parameter in `params`. For read-modify-write, pass the `graph_commit_id` of your read as `if_graph_commit`: if the branch moved, the call fails with 412 and nothing is written; read again and reconsider, never retry without it. Returns affected counts and the exact `commit` (null for a successful no-op). Also runs `branch create`, `branch delete` and `branch merge` statements, sent without `branch`, `name` or `params`.",
        input: mutate_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "load",
        title: "Load NDJSON",
        description: "Load NDJSON records (`{\"type\": \"<Type>\", \"data\": {...}}` per line) into `branch` (default `main`) as one commit. `mode`: `merge` upserts by key, `append` refuses an existing key, `overwrite` replaces the supplied types. Without `from` the branch must exist; with `from` a missing branch is created from it. The whole call must fit in 64 KiB; split larger loads into separate commits.",
        input: load_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_create",
        title: "Create a branch",
        description: "Create branch `name` from `from` (default `main`). Fails with 409 if it already exists.",
        input: branch_create_schema,
        output: None,
        effect: Effect::Additive,
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_delete",
        title: "Delete a branch",
        description: "Delete branch `name`. Its commits stay reachable only through other branches. Fails with 404 if it does not exist.",
        input: branch_delete_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_merge",
        title: "Merge a branch",
        description: "Merge branch `source` into `target` (default `main`). A merge that publishes returns its exact commit; an already up-to-date merge returns none. A conflict fails with 409 and `merge_conflicts`, writing nothing. To preview, run `branch merge <source> into <target> dry run` with `read`.",
        input: branch_merge_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
];

fn spec(settings: &McpSettings, name: &str) -> Option<&'static ToolSpec> {
    TOOLS
        .iter()
        .find(|spec| spec.name == name && spec.gate.open(settings))
}

fn instructions(settings: &McpSettings) -> String {
    let mut text = String::from(
        "OmniGraph serves typed property graphs. Every call runs under the applied policy for your credential; a listed tool or graph can still be refused.\n\
         1. Call `graphs` for graph ids. With a credential that is not an identity, it needs a `graph_list` permit, which a server without a cluster policy refuses; then ask the user for the graph id.\n\
         2. Call `schema` for the graph (or read `omnigraph://graphs/{graph}/schema`) before writing a query: it declares the types, properties, keys and edges a query may name.\n\
         3. Prefer a stored read: `queries` lists them with typed parameters, and `query` runs one.\n",
    );
    if settings.allow_adhoc {
        text.push_str(
            "4. Otherwise write a GQ read and run it with `read`, with every value passed as a declared, typed parameter. `read` refuses mutations.\n",
        );
    }
    if settings.allow_writes {
        text.push_str(
            "Writes: `mutate`, `load` and the `branches_*` tools each publish at most one commit. For read-modify-write, pass your read's `graph_commit_id` to `mutate` as `if_graph_commit`; a 412 means nothing was written. The server never retries a write. A result with `outcome_unknown`, or no response at all (a client timeout, a cancellation or a dropped connection), means the write may or may not have happened: read the target or list its commits and reconcile before replaying, and replay a `mutate` with `if_graph_commit`. Schema and deployment changes are operator-owned and not available here.\n",
        );
    }
    text.push_str(
        "Reads carry their `graph_commit_id`. A result is limited to 1 MiB and a read to 30 seconds; narrow the query with filters or `limit` when a result is too large. A refused call returns `isError` with the HTTP `status` and the server's `error` body, or an MCP `code` and `message`.",
    );
    text
}

fn tools(settings: &McpSettings) -> Vec<Tool> {
    TOOLS
        .iter()
        .filter(|spec| spec.gate.open(settings))
        .map(|spec| {
            let annotations = match spec.effect {
                Effect::Read => ToolAnnotations::new().read_only(true),
                Effect::Additive => ToolAnnotations::new()
                    .read_only(false)
                    .destructive(false)
                    .idempotent(false),
                Effect::Destructive { idempotent } => ToolAnnotations::new()
                    .read_only(false)
                    .destructive(true)
                    .idempotent(idempotent),
            };
            let description = if spec.gate == Gate::Writes {
                format!("{}{WRITE_CONTRACT}", spec.description)
            } else {
                spec.description.to_owned()
            };
            let mut tool = Tool::new(spec.name, description, object((spec.input)()))
                .with_title(spec.title)
                .with_annotations(annotations.open_world(false));
            if let Some(output) = spec.output {
                tool = tool.with_raw_output_schema(Arc::new(object(json!({
                    "type": "object",
                    "anyOf": [output(), error_schema()],
                }))));
            }
            tool
        })
        .collect()
}

fn object(value: Value) -> serde_json::Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => unreachable!("tool schemas are JSON object literals"),
    }
}

fn text_schema(description: &str) -> Value {
    json!({"type": "string", "minLength": 1, "description": description})
}

fn graph_argument() -> Value {
    text_schema("Graph id, from `graphs`.")
}

fn arguments(properties: Value, required: &[&str]) -> Value {
    let mut properties = object(properties);
    properties.insert("graph".into(), graph_argument());
    let mut required: Vec<&str> = required.to_vec();
    required.insert(0, "graph");
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn no_arguments_schema() -> Value {
    json!({"type": "object", "properties": {}, "additionalProperties": false})
}

fn graph_schema() -> Value {
    arguments(json!({}), &[])
}

fn branch_schema() -> Value {
    arguments(
        json!({"branch": text_schema("Branch. Defaults to `main`.")}),
        &[],
    )
}

fn access_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("A branch whose actions to report."),
            "target_branch": text_schema("With `branch`, the branch it would be created into or merged into."),
        }),
        &[],
    )
}

fn commits_list_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("Branch. Defaults to `main`."),
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Commits in this page; without it and `page_token`, the whole history."},
            "page_token": text_schema("The previous page's `next_page_token`."),
        }),
        &[],
    )
}

fn commit_schema() -> Value {
    arguments(
        json!({"commit_id": text_schema("A `graph_commit_id`.")}),
        &["commit_id"],
    )
}

fn query_schema() -> Value {
    arguments(
        json!({
            "name": text_schema("Stored read name, from `queries`."),
            "params": {"type": "object", "description": "Values keyed by the read's declared parameter names."},
            "branch": text_schema("Branch to read. Defaults to `main`."),
        }),
        &["name"],
    )
}

fn read_schema() -> Value {
    arguments(
        json!({
            "query": text_schema("GQ source declaring one or more read queries, or one branch read or `explain` statement (`explain query …` or `explain load csv …`)."),
            "name": text_schema("Which declared query to run when `query` declares several."),
            "params": {"type": "object", "description": "Values keyed by the query's declared parameter names, without `$`."},
            "branch": text_schema("Branch to read. Defaults to `main`. Not with `snapshot`."),
            "snapshot": text_schema("Graph commit id to read instead of a branch head."),
        }),
        &["query"],
    )
}

fn change_filter_properties() -> serde_json::Map<String, Value> {
    object(json!({
        "kind": {"type": "array", "items": {"type": "string", "enum": ["node", "edge"]}, "description": "Only these entity kinds."},
        "type": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Only these schema type names."},
        "op": {"type": "array", "items": {"type": "string", "enum": ["insert", "update", "delete"]}, "description": "Only these operations."},
        "page_token": text_schema("The previous page's `next_page_token`."),
        "limit": {"type": "integer", "minimum": 1, "description": "Maximum changes in this page."},
    }))
}

fn commit_changes_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert("commit_id".into(), text_schema("A `graph_commit_id`."));
    arguments(Value::Object(properties), &["commit_id"])
}

fn changes_poll_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert(
        "branch".into(),
        text_schema("Branch whose feed to read. Defaults to `main`."),
    );
    properties.insert(
        "cursor".into(),
        text_schema("A durable cursor from an earlier terminal page."),
    );
    properties.insert(
        "start".into(),
        json!({"type": "string", "pattern": "^(now|beginning|after:.+)$", "description": "`now` (default), `beginning`, or `after:<commit id>`."}),
    );
    arguments(Value::Object(properties), &[])
}

fn mutate_schema() -> Value {
    arguments(
        json!({
            "query": text_schema("GQ source declaring one or more mutations, or one branch write statement."),
            "name": text_schema("Which declared mutation to run when `query` declares several."),
            "params": {"type": "object", "description": "Values keyed by the mutation's declared parameter names, without `$`."},
            "branch": text_schema("Branch to write. Defaults to `main`."),
            "if_graph_commit": text_schema("Write only while the branch head is still this `graph_commit_id`."),
        }),
        &["query"],
    )
}

fn load_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("Branch to load into. Defaults to `main`."),
            "from": text_schema("Create `branch` from this branch when it does not exist."),
            "mode": {"type": "string", "enum": ["merge", "append", "overwrite"], "description": "How existing keys are handled."},
            "data": text_schema("NDJSON: one `{\"type\": \"<Type>\", \"data\": {...}}` record per line."),
        }),
        &["mode", "data"],
    )
}

fn branch_create_schema() -> Value {
    arguments(
        json!({
            "name": text_schema("The new branch's name."),
            "from": text_schema("Branch to fork; the new branch starts at its head. Defaults to `main`."),
            "from_commit": text_schema("A published graph commit id to start the branch at instead of a branch head; excludes `from`."),
        }),
        &["name"],
    )
}

fn branch_delete_schema() -> Value {
    arguments(
        json!({"name": text_schema("The branch to delete.")}),
        &["name"],
    )
}

fn branch_merge_schema() -> Value {
    arguments(
        json!({
            "source": text_schema("The branch to merge."),
            "target": text_schema("The branch that receives the merge. Defaults to `main`."),
        }),
        &["source"],
    )
}

/// The `isError` envelope: the error body an HTTP route answers under
/// `error`, with the HTTP `status` the same failure answers with; a refusal
/// only MCP makes carries no status.
fn error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"type": "integer"},
            "error": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"},
                    "code": {"type": "string"},
                    "disposition": {"type": "object"},
                },
                "required": ["error", "code", "disposition"],
            },
        },
        "required": ["error"],
        "additionalProperties": false,
    })
}

fn health_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "version": {"type": "string"},
            "internal_schema_version": {"type": "integer"},
            "source_version": {"type": "string"},
        },
        "required": ["status", "version", "internal_schema_version"],
    })
}

fn graphs_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"graphs": {"type": "array", "items": {
            "type": "object",
            "properties": {"graph_id": {"type": "string"}, "display_name": {"type": "string"}},
            "required": ["graph_id", "display_name"],
        }}},
        "required": ["graphs"],
    })
}

fn access_output_schema() -> Value {
    let actions = json!({
        "type": "array",
        "items": {"type": "string", "enum": [
            "read", "export", "change", "schema_apply", "branch_create", "branch_delete",
            "branch_merge", "commit_publish", "commit_drop", "invoke_query"
        ]},
    });
    json!({
        "type": "object",
        "properties": {
            "actor": {"type": ["string", "null"]},
            "graph_id": {"type": "string"},
            "graph_wide": actions,
            "branch": {"type": ["string", "null"]},
            "target_branch": {"type": ["string", "null"]},
            "actions": actions,
        },
        "required": ["actor", "graph_id", "graph_wide", "branch", "target_branch", "actions"],
    })
}

fn branches_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"branches": {"type": "array", "items": {"type": "string"}}},
        "required": ["branches"],
    })
}

fn schema_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_source": {"type": "string"},
            "system_columns": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "src": {"type": "string"}, "dst": {"type": "string"}},
                "required": ["id", "src", "dst"],
            },
        },
        "required": ["schema_source"],
    })
}

fn queries_output_schema() -> Value {
    let kind = json!({"type": "string", "enum": ["string", "bool", "int", "bigint", "float", "date", "datetime", "blob", "vector", "list"]});
    json!({
        "type": "object",
        "properties": {"queries": {"type": "array", "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "tool_name": {"type": "string"},
                "description": {"type": "string"},
                "instruction": {"type": "string"},
                "mutation": {"type": "boolean"},
                "params": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "kind": kind,
                        "item_kind": kind,
                        "vector_dim": {"type": "integer"},
                        "nullable": {"type": "boolean"},
                    },
                    "required": ["name", "kind", "nullable"],
                }},
            },
            "required": ["name", "tool_name", "mutation", "params"],
        }}},
        "required": ["queries"],
    })
}

struct LimitedBytes(Vec<u8>);
impl std::io::Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > RESULT_BYTES - self.0.len() {
            return Err(std::io::Error::other("MCP result exceeds byte limit"));
        }
        self.0.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A handler's result, refused as `result_too_large` when it does not fit;
/// the refusal is the call's error, so a write tool's says the write ran.
fn bounded_result<T: Serialize>(value: &T) -> Result<CallToolResult, ApiError> {
    let mut bytes = LimitedBytes(Vec::new());
    if serde_json::to_writer(&mut bytes, value).is_err() {
        return Err(result_too_large());
    }
    let value = serde_json::from_slice(&bytes.0)
        .map_err(|_| ApiError::internal("could not encode MCP result"))?;
    let result = CallToolResult::structured(value);
    if exceeds_result_limit(&result) {
        return Err(result_too_large());
    }
    Ok(result)
}

/// Whether the complete tool result, including the text copy structured
/// results carry for older clients, exceeds the result limit.
fn exceeds_result_limit(result: &CallToolResult) -> bool {
    serde_json::to_writer(&mut LimitedBytes(Vec::new()), result).is_err()
}

/// Bound the complete tool result. A write's result that does not fit leaves
/// the caller without its receipt, so the write must be reconciled.
fn fit_result(result: CallToolResult, write: bool) -> CallToolResult {
    if !exceeds_result_limit(&result) {
        return result;
    }
    let error = result_too_large();
    tool_error(if write {
        error.after_owned_write()
    } else {
        error
    })
}

fn result_too_large() -> ApiError {
    ApiError::new(
        ErrorCode::ResultTooLarge,
        "The result exceeds 1 MiB; use a narrower stored query.",
    )
}

/// The `isError` result of a refusal: the error body an HTTP route answers,
/// under `error`, beside the HTTP status the same failure answers with. A
/// refusal only MCP makes has no status.
fn tool_error(error: ApiError) -> CallToolResult {
    let output = error.into_output();
    let status = output.http_status();
    let mut body = json!({ "error": output });
    if let Some(status) = status {
        body["status"] = json!(status);
    }
    CallToolResult::structured_error(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_or_expired_mcp_read_retains_capacity_until_execution_finishes() {
        tokio::time::timeout(Duration::from_secs(10), async {
            use crate::operations::OperationRuntime;
            use crate::workload::{WorkloadController, WorkloadSnapshot};

            for cancelled in [false, true] {
                let workload = WorkloadController::with_defaults();
                let operations = OperationRuntime::with_read_limit(2);
                let temp = tempfile::TempDir::new().unwrap();
                let uri = temp.path().join("graph").to_string_lossy().into_owned();
                let engine =
                    omnigraph::db::Omnigraph::init(&uri, "node Person { name: String @key }\n")
                        .await
                        .unwrap();
                let service = GraphTools {
                    state: AppState::new_single(uri, engine, Vec::new(), None, workload.clone())
                        .with_operations(operations.clone()),
                    slots: Arc::new(Semaphore::new(1)),
                };
                let key = GraphKey::cluster(GraphId::try_from("default").unwrap());
                let registry = service.state.routing.registry.clone();
                let execution_key = key.clone();
                let execution_runtime = operations.clone();
                let graph = McpGraphRequest::default();
                let execution_graph = graph.clone();
                let (select, selected) = tokio::sync::oneshot::channel();
                let (entered, started) = tokio::sync::oneshot::channel();
                let (release, held) = tokio::sync::oneshot::channel();
                let result = service
                    .run_read(
                        operations.try_observe().unwrap(),
                        workload.try_read_ingress(17).unwrap(),
                        graph,
                        async move {
                            // Graph resolution can happen after MCP cancellation
                            // has already returned its error to the HTTP task.
                            selected.await.unwrap();
                            let crate::registry::RegistryCapture::Ready(request) = registry
                                .capture(&execution_runtime, &execution_key)
                                .unwrap()
                            else {
                                panic!("fixture graph must be available");
                            };
                            assert!(execution_graph.set(request).is_ok());
                            entered.send(()).unwrap();
                            held.await.unwrap();
                            bounded_result(&json!({"finished": true}))
                        },
                        async move {
                            if !cancelled {
                                std::future::pending::<()>().await;
                            }
                        },
                        if cancelled {
                            READ_DEADLINE
                        } else {
                            Duration::ZERO
                        },
                    )
                    .await;
                let refused = result.structured_content.unwrap();
                assert_eq!(
                    refused["error"]["code"],
                    if cancelled {
                        "cancelled"
                    } else {
                        "deadline_exceeded"
                    }
                );
                // Only MCP sends these, so no HTTP status rides them.
                assert!(refused.get("status").is_none(), "{refused}");
                select.send(()).unwrap();
                started.await.unwrap();
                let transition = service
                    .state
                    .prepare_same_view(&key, tokio::time::Instant::now() + Duration::from_secs(2))
                    .unwrap()
                    .close()
                    .unwrap();
                let settled = transition.wait_requests();
                tokio::pin!(settled);
                assert!(futures::poll!(&mut settled).is_pending());
                assert_eq!(operations.snapshot().active_reads, 1);
                assert_eq!(workload.snapshot().read_ingress_bytes, 17);
                assert_eq!(service.slots.available_permits(), 0);
                let refused = service
                    .run_read(
                        operations.try_observe().unwrap(),
                        workload.try_read_ingress(3).unwrap(),
                        McpGraphRequest::default(),
                        async { panic!("refused MCP read must not execute") },
                        std::future::pending(),
                        READ_DEADLINE,
                    )
                    .await;
                let refused = refused.structured_content.unwrap();
                assert_eq!(refused["status"], 429);
                assert_eq!(refused["error"]["code"], "too_many_requests");
                assert_eq!(
                    refused["error"]["disposition"],
                    json!({"effects": "none", "action": "retry"})
                );
                assert_eq!(operations.snapshot().active_reads, 1);
                assert_eq!(workload.snapshot().read_ingress_bytes, 17);
                operations.close();
                assert!(matches!(
                    settled.await,
                    Err(crate::serving::ServingTransitionError::ProcessClosed)
                ));
                let shutdown = operations.wait_logical_owners();
                tokio::pin!(shutdown);
                assert!(futures::poll!(&mut shutdown).is_pending());
                release.send(()).unwrap();
                assert!(shutdown.await);
                assert_eq!(service.slots.available_permits(), 1);
                assert_eq!(workload.snapshot(), WorkloadSnapshot::default());
            }
        })
        .await
        .expect("MCP read did not settle");
    }

    #[tokio::test]
    async fn a_write_whose_wait_ends_first_reports_an_unknown_outcome() {
        // The server's answer must arrive before a client on a common default
        // timeout gives up and never sees `outcome_unknown`.
        assert!(WRITE_DEADLINE < COMMON_CLIENT_TIMEOUT);
        let unknown = json!({"effects": "unknown", "action": "reconcile"});
        for refusal in [Refusal::Cancelled, Refusal::Deadline] {
            let result = refusal.tool_result(true);
            assert_eq!(result.is_error, Some(true));
            let body = result.structured_content.unwrap();
            assert_eq!(body["status"], 500, "{body}");
            assert_eq!(body["error"]["code"], "outcome_unknown", "{body}");
            assert_eq!(body["error"]["disposition"], unknown, "{body}");
            assert!(body.get("outcome_unknown").is_none(), "{body}");
        }
        let read = Refusal::Deadline
            .tool_result(false)
            .structured_content
            .unwrap();
        assert_eq!(read["error"]["code"], "deadline_exceeded");
        assert_eq!(
            read["error"]["disposition"],
            json!({"effects": "none", "action": "retry"})
        );
        // A handler failure the server cannot settle is an unknown outcome
        // for a write only; a refusal before effects is a known one.
        let uncertain = Refusal::Api(ApiError::internal("lost"))
            .tool_result(true)
            .structured_content
            .unwrap();
        assert_eq!(uncertain["status"], 500);
        assert_eq!(uncertain["error"]["code"], "outcome_unknown");
        assert_eq!(uncertain["error"]["disposition"], unknown);
        for (error, write, code, disposition) in [
            (
                ApiError::bad_request("refused"),
                true,
                "bad_request",
                json!({"effects": "none", "action": "fix"}),
            ),
            (
                ApiError::internal("lost"),
                false,
                "internal",
                json!({"effects": "none", "action": "refresh"}),
            ),
        ] {
            let body = Refusal::Api(error)
                .tool_result(write)
                .structured_content
                .unwrap();
            assert_eq!(body["error"]["code"], code, "{body}");
            assert_eq!(body["error"]["disposition"], disposition, "{body}");
        }
        // The MCP layer's own refusals carry the same body: a registry code,
        // a disposition and the HTTP status the same refusal answers.
        let unauthenticated = Refusal::Unauthenticated
            .tool_result(false)
            .structured_content
            .unwrap();
        assert_eq!(unauthenticated["status"], 401);
        assert_eq!(unauthenticated["error"]["code"], "unauthorized");
        let data = Refusal::Unauthenticated.resource_error().data.unwrap();
        assert_eq!(data, unauthenticated);
    }

    #[test]
    fn every_best_practice_resource_is_an_embedded_skill_reference() {
        for topic in BEST_PRACTICES {
            let body = topic
                .body()
                .unwrap_or_else(|| panic!("{} is not embedded", topic.key));
            assert!(body.starts_with('#'), "{}", topic.key);
            assert!(best_practices_index().contains(&topic.uri()));
        }
    }

    #[test]
    fn allowed_hosts_are_bare_authorities() {
        for host in [
            "graphs.internal",
            "graphs.internal:8080",
            "10.0.0.7",
            "[::1]:8080",
        ] {
            assert!(validate_allowed_host(host).is_ok(), "{host}");
        }
        for host in [
            "",
            "https://graphs.internal",
            "graphs.internal/mcp",
            "user@graphs.internal",
            "graphs.internal?x",
            "a b",
        ] {
            assert!(validate_allowed_host(host).is_err(), "{host}");
        }
    }

    #[test]
    fn complete_tool_result_including_legacy_text_has_one_byte_bound() {
        let small = json!({"rows":[{"name":"Alice"}]});
        let result = bounded_result(&small).unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.structured_content, Some(small));
        // The source JSON fits, but the SDK's structured + legacy text
        // compatibility representation exceeds the complete result bound.
        let duplicate = json!({"text":"x".repeat(RESULT_BYTES * 3 / 4)});
        let too_large = json!({"text":"x".repeat(RESULT_BYTES+1)});
        for value in [&duplicate, &too_large] {
            let refused = Refusal::Api(bounded_result(value).unwrap_err())
                .tool_result(false)
                .structured_content
                .unwrap();
            assert_eq!(refused["error"]["code"], "result_too_large");
            assert_eq!(
                refused["error"]["disposition"],
                json!({"effects": "none", "action": "fix"})
            );
        }
        // A write's result that does not fit leaves its receipt unread.
        let unread = json!({"effects": "unknown", "action": "reconcile"});
        let write = Refusal::Api(bounded_result(&duplicate).unwrap_err())
            .tool_result(true)
            .structured_content
            .unwrap();
        assert_eq!(write["error"]["disposition"], unread);
        let write = fit_result(CallToolResult::structured(duplicate), true)
            .structured_content
            .unwrap();
        assert_eq!(write["error"]["code"], "result_too_large");
        assert_eq!(write["error"]["disposition"], unread);
    }
}
