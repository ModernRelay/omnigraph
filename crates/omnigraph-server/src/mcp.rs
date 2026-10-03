//! Read-only MCP transport over the existing graph discovery and stored-query handlers.
//! The official SDK owns protocol framing, negotiation and cancellation.
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Bytes, to_bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
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
        .route_layer(middleware::from_fn_with_state(state, resource_challenge));
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(resource_metadata),
        )
        .merge(protected)
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

async fn resource_challenge(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        if let Some(mut url) = state
            .oidc_resource_url()
            .and_then(|url| url::Url::parse(&url).ok())
        {
            let path = format!(
                "/.well-known/oauth-protected-resource{}",
                url.path().trim_end_matches('/')
            );
            url.set_path(&path);
            url.set_query(None);
            url.set_fragment(None);
            if let Ok(value) = HeaderValue::from_str(&format!("Bearer resource_metadata=\"{url}\""))
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

impl ServerHandler for GraphTools {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("omnigraph-server", env!("CARGO_PKG_VERSION")))
            .with_instructions("Discover graph names, list readable stored queries, then invoke a named read. Applied graph policy governs every read. Mutation tools are unavailable.")
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools()))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools().into_iter().find(|tool| tool.name == name)
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
            _ => Err(ApiError::bad_request("unknown MCP tool")),
        }
    }

    fn graph(
        &self,
        name: &str,
        mut actor: AuthenticatedActor,
        graph: &McpGraphRequest,
    ) -> Result<(GraphRequest, AuthenticatedActor), ApiError> {
        let id = GraphId::try_from(name.to_string())
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
        if !actor.select_graph(&id) {
            return Err(ApiError::forbidden("credential does not permit this graph"));
        }
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

fn tools() -> Vec<Tool> {
    let text = json!({"type":"string","minLength":1});
    [
        ("graphs", "List effective graph identifiers and display names.", json!({"type":"object","properties":{},"additionalProperties":false})),
        ("queries", "List stored reads visible under the graph's applied read policy. Invocation is separately authorized.", json!({"type":"object","properties":{"graph":text},"required":["graph"],"additionalProperties":false})),
        ("query", "Invoke a named stored read under the graph's applied policy. Defaults to the main branch; mutations are refused.", json!({"type":"object","properties":{"graph":text,"name":text,"params":{"type":"object"},"branch":text},"required":["graph","name"],"additionalProperties":false})),
    ].into_iter().map(|(name, description, schema)| Tool::new(name, description, schema.as_object().expect("literal object").clone())
        .with_annotations(ToolAnnotations::new().read_only(true))).collect()
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
