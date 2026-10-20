//! Server adapter for the MCP protocol crate. HTTP authentication, admission,
//! handler dispatch, and execution ownership stay on the existing server.
use std::future::Future;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use omnigraph_mcp::{
    AdmissionError, Host, HostError, Pending, ResourceOutput, ResourceRequest, ToolCall,
    ToolRequest, ToolResult, change_query,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::OwnedSemaphorePermit;

use crate::api::InvokeStoredQueryRequest;
use crate::handlers::{self, QueryNamePath};
use crate::ingress::McpGraphRequest;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, GraphId, GraphKey};

pub use omnigraph_mcp::McpSettings;
pub(crate) use omnigraph_mcp::REQUEST_BYTES;
const RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

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
    let resource_url = state.oidc_resource_url();
    let oidc = resource_url.is_some();
    let protected = omnigraph_mcp::router(
        ServerHost {
            state: state.clone(),
        },
        state.mcp_settings().clone(),
        resource_url.as_deref(),
        crate::api::build::reported_version(env!("CARGO_PKG_VERSION")),
    )
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

/// The admitted HTTP request's actor and reservations, which the SDK carries
/// into its task as request extensions.
struct Admitted {
    actor: AuthenticatedActor,
    observer: crate::operations::ReadObserver,
    input: IngressLease,
    graph: McpGraphRequest,
}

fn admitted(parts: Option<&axum::http::request::Parts>) -> Result<Admitted, AdmissionError> {
    let actor = parts
        .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
        .cloned()
        .ok_or(AdmissionError::Unauthenticated)?;
    // Reuse the admitted request's one lease and observer instead of admitting
    // again after collection or releasing them before the SDK task finishes.
    let parts = parts.ok_or(AdmissionError::MissingContext)?;
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
        _ => Err(AdmissionError::MissingContext),
    }
}

#[derive(Clone)]
struct ServerHost {
    state: AppState,
}

impl Host for ServerHost {
    type Context = Admitted;

    fn admit(
        &self,
        parts: Option<&axum::http::request::Parts>,
    ) -> Result<Admitted, AdmissionError> {
        admitted(parts)
    }

    fn start_tool(
        &self,
        context: Admitted,
        request: ToolRequest,
        permit: OwnedSemaphorePermit,
    ) -> Pending<ToolResult> {
        let host = self.clone();
        let actor = context.actor.clone();
        let input = context.input.clone();
        let graph = context.graph.clone();
        start_owned(context, permit, async move {
            host.execute(request, actor, input, graph).await
        })
    }

    fn start_resource(
        &self,
        context: Admitted,
        request: ResourceRequest,
        permit: OwnedSemaphorePermit,
    ) -> Pending<ResourceOutput> {
        let host = self.clone();
        let actor = context.actor.clone();
        let graph = context.graph.clone();
        start_owned(context, permit, async move {
            host.read(request, actor, graph).await
        })
    }
}

/// Register before returning the observation future: a caller can cancel or
/// expire without ever polling that future. Execution retains every reservation
/// until its owned work settles, including graph selection after cancellation.
fn start_owned<T, F>(context: Admitted, permit: OwnedSemaphorePermit, operation: F) -> Pending<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T, ApiError>> + Send + 'static,
{
    let read = context
        .observer
        .spawn_read((context.input, permit, context.graph), operation);
    Box::pin(async move { read.result().await.map_err(host_error) })
}

fn host_error(error: ApiError) -> HostError {
    let status = error.status;
    let uncertain = error.completion_uncertain();
    HostError::from_output(status, &error.into_output(), uncertain)
}

fn bounded_result<T: Serialize>(value: &T) -> Result<ToolResult, ApiError> {
    omnigraph_mcp::bounded_result(value)
        .map_err(|_| ApiError::internal("could not encode MCP result"))
}

impl ServerHost {
    async fn execute(
        &self,
        request: ToolRequest,
        actor: AuthenticatedActor,
        input: IngressLease,
        graph: McpGraphRequest,
    ) -> Result<ToolResult, ApiError> {
        // Reserve write ingress before decoding arguments, matching HTTP
        // admission and preserving refusal precedence.
        let ingress = if request.is_write() {
            let bytes = request
                .argument_bytes()
                .map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))?;
            Some(self.write_ingress(bytes)?)
        } else {
            None
        };
        let write_ingress =
            || ingress.ok_or_else(|| ApiError::internal("MCP write admission is missing"));
        let call = request
            .parse()
            .map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))?;
        let state = || State(self.state.clone());
        match call {
            ToolCall::Health => bounded_result(&handlers::server_health().await.0),
            ToolCall::Graphs => bounded_result(&self.discover(actor).await?),
            ToolCall::Schema(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output =
                    handlers::server_schema_get(Extension(handle), Some(Extension(actor))).await?;
                bounded_result(&output.0)
            }
            ToolCall::Queries(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let mut output =
                    handlers::server_list_queries(Extension(handle), Some(Extension(actor)))
                        .await?
                        .0;
                output.queries.retain(|query| !query.mutation);
                bounded_result(&output)
            }
            ToolCall::Query(args) => {
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
                    Path(QueryNamePath { name: args.name }),
                    HeaderMap::new(),
                    Bytes::from(body),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::Read(args) => {
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
                    Ok(Json(request)),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::Snapshot(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_snapshot(
                    Extension(handle),
                    Some(Extension(actor)),
                    Query(crate::api::SnapshotQuery {
                        branch: args.branch,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::Access(args) => {
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
            ToolCall::BranchesList(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output =
                    handlers::server_branch_list(Extension(handle), Some(Extension(actor))).await?;
                bounded_result(&output.0)
            }
            ToolCall::CommitsList(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_commit_list(
                    Extension(handle),
                    Some(Extension(actor)),
                    Query(crate::api::CommitListQuery {
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
            ToolCall::CommitsGet(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_commit_show(
                    Extension(handle),
                    Some(Extension(actor)),
                    Path(handlers::CommitPath {
                        commit_id: args.commit_id,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::CommitsChanges(args) => {
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let query = change_query(&[], &args.filters);
                let output = handlers::server_commit_changes(
                    Extension(handle),
                    Some(Extension(actor)),
                    Path(handlers::CommitPath {
                        commit_id: args.commit_id,
                    }),
                    RawQuery(query),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::ChangesPoll(args) => {
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
            ToolCall::Mutate(args) => {
                let ingress = write_ingress()?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let request = Json(request_body(json!({
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
                            Ok(request),
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
                            Ok(request),
                        )
                        .await?
                    }
                };
                bounded_result(&output.0)
            }
            ToolCall::Load(args) => {
                let ingress = write_ingress()?;
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
                    Json(request),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::BranchesCreate(args) => {
                let ingress = write_ingress()?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_branch_create(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    Json(crate::api::BranchCreateRequest {
                        from: args.from,
                        from_commit: args.from_commit,
                        name: args.name,
                    }),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::BranchesDelete(args) => {
                let ingress = write_ingress()?;
                let (handle, actor) = self.graph(&args.graph, actor, &graph)?;
                let output = handlers::server_branch_delete(
                    state(),
                    Extension(handle),
                    Extension(ingress),
                    Some(Extension(actor)),
                    Path(handlers::BranchPath { branch: args.name }),
                )
                .await?;
                bounded_result(&output.0)
            }
            ToolCall::BranchesMerge(args) => {
                let ingress = write_ingress()?;
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
                    Ok(Json(request)),
                )
                .await?;
                bounded_result(&output.0)
            }
        }
    }

    /// One resource body and its media type, from the same handler and policy
    /// action as the matching tool.
    async fn read(
        &self,
        resource: ResourceRequest,
        actor: AuthenticatedActor,
        graph: McpGraphRequest,
    ) -> Result<(&'static str, String), ApiError> {
        match resource {
            ResourceRequest::Graphs => {
                Ok(("application/json", pretty(&self.discover(actor).await?)?))
            }
            ResourceRequest::Schema(name) => {
                let (handle, actor) = self.graph(&name, actor, &graph)?;
                let output =
                    handlers::server_schema_get(Extension(handle), Some(Extension(actor))).await?;
                Ok(("text/plain", output.0.schema_source))
            }
            ResourceRequest::Branches(name) => {
                let (handle, actor) = self.graph(&name, actor, &graph)?;
                let output =
                    handlers::server_branch_list(Extension(handle), Some(Extension(actor))).await?;
                Ok(("application/json", pretty(&output.0)?))
            }
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
    fn write_ingress(&self, bytes: usize) -> Result<IngressLease, ApiError> {
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

/// A route's typed request body, built from tool arguments through the same
/// deserializer its JSON extractor uses.
fn request_body<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|_| ApiError::bad_request("invalid MCP tool arguments"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Semaphore;

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
                let service = ServerHost {
                    state: AppState::new_single(uri, engine, Vec::new(), None, workload.clone())
                        .with_operations(operations.clone()),
                };
                let slots = Arc::new(Semaphore::new(1));
                let key = GraphKey::cluster(GraphId::try_from("default").unwrap());
                let registry = service.state.routing.registry.clone();
                let execution_key = key.clone();
                let execution_runtime = operations.clone();
                let graph = McpGraphRequest::default();
                let execution_graph = graph.clone();
                let (select, selected) = tokio::sync::oneshot::channel();
                let (entered, started) = tokio::sync::oneshot::channel();
                let (release, held) = tokio::sync::oneshot::channel();
                let response = start_owned(
                    Admitted {
                        actor: AuthenticatedActor::cluster_static(Arc::from("mcp-test")),
                        observer: operations.try_observe().unwrap(),
                        input: workload.try_read_ingress(17).unwrap(),
                        graph,
                    },
                    slots.clone().try_acquire_owned().unwrap(),
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
                );
                // The protocol crate owns cancellation/deadline replies. The
                // adapter must retain work even if observation is never polled.
                if cancelled {
                    drop(response);
                } else {
                    assert!(
                        tokio::time::timeout(Duration::ZERO, response)
                            .await
                            .is_err()
                    );
                }
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
                assert_eq!(slots.available_permits(), 0);
                assert!(slots.clone().try_acquire_owned().is_err());
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
                assert_eq!(slots.available_permits(), 1);
                assert_eq!(workload.snapshot(), WorkloadSnapshot::default());
            }
        })
        .await
        .expect("MCP read did not settle");
    }
}
