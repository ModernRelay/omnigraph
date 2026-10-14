//! Read-only MCP transport over the existing graph discovery, schema, stored-query
//! and inline-query handlers.
//! The official SDK owns protocol framing, negotiation and cancellation.
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Bytes, to_bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ErrorData, Implementation,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{RoleServer, ServerHandler};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::api::InvokeStoredQueryRequest;
use crate::handlers::{self, QueryNamePath};
use crate::ingress::McpGraphRequest;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, GraphId, GraphKey};

pub(crate) const REQUEST_BYTES: usize = 64 * 1024;
const RESULT_BYTES: usize = 1024 * 1024;
const READ_DEADLINE: Duration = Duration::from_secs(30);
const RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// Environment equivalent of `--mcp-allow-adhoc`.
pub const MCP_ALLOW_ADHOC_ENV: &str = "OMNIGRAPH_MCP_ALLOW_ADHOC";

/// Process boot settings for `/mcp`. They select which opt-in tools are listed
/// and admitted; they never grant anything, since every tool still runs only
/// what the applied policy allows its caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpSettings {
    /// Lists and admits the ad-hoc `read` tool.
    pub allow_adhoc: bool,
}

impl McpSettings {
    /// The boot flags, each enabled also by its environment variable.
    pub fn resolve(allow_adhoc: bool) -> Self {
        Self {
            allow_adhoc: allow_adhoc || env_flag(MCP_ALLOW_ADHOC_ENV),
        }
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

/// Mount only when an explicitly configured OIDC resource enables this surface.
/// Every HTTP request authenticates again; no MCP session retains an actor.
pub(crate) fn router(state: AppState) -> Router<AppState> {
    let handler = GraphTools {
        state: state.clone(),
        slots: Arc::new(Semaphore::new(16)),
    };
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    config.max_request_body_bytes = REQUEST_BYTES;
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

impl ServerHandler for GraphTools {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "omnigraph-server",
                env!("CARGO_PKG_VERSION"),
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
        if self.get_tool(&request.name).is_none() {
            return Err(ErrorData::invalid_params("unknown MCP tool", None));
        }
        let parts = context.extensions.get::<axum::http::request::Parts>();
        let actor = parts
            .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
            .cloned();
        let Some(actor) = actor else {
            return Ok(failure(
                "unauthenticated",
                "A verified bearer credential is required.",
            )
            .into());
        };
        // The SDK transports the admitted HTTP request's extensions into its
        // task. Reuse that one lease and observer instead of admitting again
        // after collection or releasing them before the SDK task finishes.
        let Some((observer, input, graph)) = parts.and_then(|parts| {
            parts
                .extensions
                .get::<crate::operations::ReadObserver>()
                .zip(parts.extensions.get::<IngressLease>())
                .zip(parts.extensions.get::<McpGraphRequest>())
                .map(|((observer, input), graph)| (observer.clone(), input.clone(), graph.clone()))
        }) else {
            return Ok(failure("internal_error", "MCP admission context is missing.").into());
        };
        let service = self.clone();
        let execution_input = input.clone();
        let execution_graph = graph.clone();
        let result = self
            .run_read(
                observer,
                input,
                graph,
                async move {
                    service
                        .execute(request, actor, execution_input, execution_graph)
                        .await
                },
                context.ct.cancelled(),
                READ_DEADLINE,
            )
            .await;
        Ok(fit_result(result).into())
    }
}

impl GraphTools {
    /// MCP's timeout and cancellation bound the caller's observation. The
    /// engine execution retains the SDK-independent observer, input and MCP
    /// slot until it completes, including its owned query-worker joins.
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
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            return failure(
                "capacity_exceeded",
                "The server is handling its maximum concurrent MCP reads.",
            );
        };
        let read = observer.spawn_read((input, permit, graph), operation);
        tokio::select! {
            _ = cancelled => failure("cancelled", "The read was cancelled."),
            result = tokio::time::timeout(deadline, read.result()) => {
                match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => api_failure(error).await,
                    Err(_) => failure("deadline_exceeded", "The read exceeded its 30 second deadline."),
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
        match request.name.as_ref() {
            "graphs" => {
                let _: NoArguments = arguments_as(arguments)?;
                let output = handlers::server_graphs_discovery(
                    State(self.state.clone()),
                    Some(Extension(actor)),
                )
                .await?
                .0;
                bounded_result(&output)
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
                    State(self.state.clone()),
                    Extension(handle),
                    Extension(input),
                    Some(Extension(actor)),
                    Path(QueryNamePath { name: args.name }),
                    HeaderMap::new(),
                    Bytes::from(body),
                )
                .await?
                .0;
                bounded_result(&output)
            }
            "schema" => {
                let args: GraphArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_schema_get(Extension(handle), Some(Extension(actor)))
                    .await?
                    .0;
                bounded_result(&output)
            }
            "read" if self.state.mcp_settings().allow_adhoc => {
                let args: ReadArguments = arguments_as(arguments)?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request: crate::api::QueryRequest = serde_json::from_value(json!({
                    "query": args.query,
                    "name": args.name,
                    "params": args.params,
                    "branch": args.branch,
                    "snapshot": args.snapshot,
                }))
                .map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))?;
                let output = handlers::server_query(
                    State(self.state.clone()),
                    Extension(handle),
                    Some(Extension(actor)),
                    Ok(Json(request)),
                )
                .await?
                .0;
                bounded_result(&output)
            }
            _ => Err(ApiError::bad_request("unknown MCP tool")),
        }
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

fn arguments_as<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))
}

/// Which boot setting, if any, lists and admits a tool.
#[derive(Clone, Copy)]
enum Gate {
    Always,
    Adhoc,
}

impl Gate {
    fn open(self, settings: &McpSettings) -> bool {
        match self {
            Gate::Always => true,
            Gate::Adhoc => settings.allow_adhoc,
        }
    }
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
    gate: Gate,
}

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "graphs",
        title: "List graphs",
        description: "List the graphs this server serves: each graph's `graph_id` and display name. Every other tool takes one of these ids as `graph`. Listing a graph does not mean your credential may read it.",
        input: no_arguments_schema,
        output: Some(graphs_output_schema),
        gate: Gate::Always,
    },
    ToolSpec {
        name: "schema",
        title: "Read a graph's schema",
        description: "Return a graph's schema as `.pg` source (`schema_source`): its node and edge types, properties with their types, keys, edge endpoints and declared embeddings. Call this before writing any query: GQ names types, properties and edges exactly as declared here, and edges are matched in lowerCamelCase.",
        input: graph_schema,
        output: Some(schema_output_schema),
        gate: Gate::Always,
    },
    ToolSpec {
        name: "queries",
        title: "List stored reads",
        description: "List a graph's stored reads: each `name`, its description and instruction, and typed `params` (`name`, `kind`, `nullable`; `item_kind` for lists, `vector_dim` for vectors). A non-nullable parameter is required. Run one with `query`. Invocation is authorized separately, so a listed read can still be refused.",
        input: graph_schema,
        output: Some(queries_output_schema),
        gate: Gate::Always,
    },
    ToolSpec {
        name: "query",
        title: "Run a stored read",
        description: "Run a stored read by `name` with `params` keyed by its declared parameter names (see `queries`). Reads `branch`, `main` when omitted. Returns `rows`, `row_count` and the read's `graph_commit_id`. Stored mutations are refused.",
        input: query_schema,
        output: None,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "read",
        title: "Run a GQ read",
        description: "Run a GQ read query you write. Call `schema` first. Declare every value as a typed parameter (`query people($min: I32) { match { $p: Person $p.age >= $min } return { $p.name } }`) and pass the values in `params`; pick one declared query with `name` when the source declares several. Reads `branch` (default `main`) or a `snapshot`, never both. Search operators (`nearest`, `bm25`, `rrf`) need a trailing `limit`. Also answers `branch list`, `branch diff <source> [into <target>]`, `branch merge-base …`, `branch merge … dry run` and `explain query …`, sent without `branch`, `snapshot`, `name` or `params`. Mutations and branch writes are refused. Returns `rows`, `row_count` and `graph_commit_id`.",
        input: read_schema,
        output: None,
        gate: Gate::Adhoc,
    },
];

fn instructions(settings: &McpSettings) -> String {
    let mut text = String::from(
        "OmniGraph serves typed property graphs. Every call runs under the applied policy for your credential; a listed tool or graph can still be refused.\n\
         1. Call `graphs` for graph ids.\n\
         2. Call `schema` for the graph before writing a query: it declares the types, properties, keys and edges a query may name.\n\
         3. Prefer a stored read: `queries` lists them with typed parameters, and `query` runs one.\n",
    );
    if settings.allow_adhoc {
        text.push_str(
            "4. Otherwise write a GQ read and run it with `read`, with every value passed as a declared, typed parameter. `read` refuses mutations.\n",
        );
    }
    text.push_str(
        "Results carry the read's `graph_commit_id`. A result is limited to 1 MiB and a read to 30 seconds; narrow the query with filters or `limit` when a result is too large. A refused call returns `isError` with the HTTP `status` and the server's `error` body, or an MCP `code` and `message`.",
    );
    text
}

fn tools(settings: &McpSettings) -> Vec<Tool> {
    TOOLS
        .iter()
        .filter(|spec| spec.gate.open(settings))
        .map(|spec| {
            let mut tool = Tool::new(spec.name, spec.description, object((spec.input)()))
                .with_title(spec.title)
                .with_annotations(ToolAnnotations::new().read_only(true).open_world(false));
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

fn no_arguments_schema() -> Value {
    json!({"type": "object", "properties": {}, "additionalProperties": false})
}

fn graph_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"graph": graph_argument()},
        "required": ["graph"],
        "additionalProperties": false,
    })
}

fn query_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "graph": graph_argument(),
            "name": text_schema("Stored read name, from `queries`."),
            "params": {"type": "object", "description": "Values keyed by the read's declared parameter names."},
            "branch": text_schema("Branch to read. Defaults to `main`."),
        },
        "required": ["graph", "name"],
        "additionalProperties": false,
    })
}

fn read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "graph": graph_argument(),
            "query": text_schema("GQ source declaring one or more read queries, or one branch read or `explain` statement."),
            "name": text_schema("Which declared query to run when `query` declares several."),
            "params": {"type": "object", "description": "Values keyed by the query's declared parameter names, without `$`."},
            "branch": text_schema("Branch to read. Defaults to `main`. Not with `snapshot`."),
            "snapshot": text_schema("Graph commit id to read instead of a branch head."),
        },
        "required": ["graph", "query"],
        "additionalProperties": false,
    })
}

/// The `isError` envelope: an HTTP refusal (`status` and the route's `error`
/// body) or an MCP-side refusal (`code` and `message`).
fn error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"type": "integer"},
            "error": {"type": "object"},
            "code": {"type": "string"},
            "message": {"type": "string"},
        },
        "additionalProperties": false,
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

fn bounded_result<T: Serialize>(value: &T) -> Result<CallToolResult, ApiError> {
    let mut bytes = LimitedBytes(Vec::new());
    if serde_json::to_writer(&mut bytes, value).is_err() {
        return Ok(failure(
            "result_too_large",
            "The result exceeds 1 MiB; use a narrower stored query.",
        ));
    }
    let value = serde_json::from_slice(&bytes.0)
        .map_err(|_| ApiError::internal("could not encode MCP result"))?;
    Ok(fit_result(CallToolResult::structured(value)))
}

fn fit_result(result: CallToolResult) -> CallToolResult {
    // Structured results also carry a text representation for older clients.
    // Bound the complete tool result, including that duplicate representation.
    if serde_json::to_writer(&mut LimitedBytes(Vec::new()), &result).is_err() {
        return failure(
            "result_too_large",
            "The result exceeds 1 MiB; use a narrower stored query.",
        );
    }
    result
}

fn failure(code: &str, message: &str) -> CallToolResult {
    CallToolResult::structured_error(json!({"code":code,"message":message}))
}

async fn api_failure(error: ApiError) -> CallToolResult {
    let response = error.into_response();
    let status = response.status().as_u16();
    match to_bytes(response.into_body(), RESULT_BYTES).await {
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(error) => CallToolResult::structured_error(json!({"status":status,"error":error})),
            Err(_) => failure("request_failed", "The graph request failed."),
        },
        Err(_) => failure("request_failed", "The graph request failed."),
    }
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
                assert_eq!(
                    result.structured_content.unwrap()["code"],
                    if cancelled {
                        "cancelled"
                    } else {
                        "deadline_exceeded"
                    }
                );
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
                assert_eq!(
                    refused.structured_content.unwrap()["code"],
                    "capacity_exceeded"
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

    #[test]
    fn complete_tool_result_including_legacy_text_has_one_byte_bound() {
        let small = json!({"rows":[{"name":"Alice"}]});
        let result = bounded_result(&small).unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.structured_content, Some(small));
        // The source JSON fits, but the SDK's structured + legacy text
        // compatibility representation exceeds the complete result bound.
        let duplicate = json!({"text":"x".repeat(RESULT_BYTES * 3 / 4)});
        let result = bounded_result(&duplicate).unwrap();
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content.unwrap()["code"],
            "result_too_large"
        );
        let too_large = json!({"text":"x".repeat(RESULT_BYTES+1)});
        assert_eq!(bounded_result(&too_large).unwrap().is_error, Some(true));
    }
}
