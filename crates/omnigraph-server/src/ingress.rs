//! Reserve wire-input capacity before collection and retain response observers
//! through bodies and yielded chunks. These are server lifetimes, not native-I/O
//! settlement or a bound on decoded engine allocations.

use axum::body::{Body, Bytes};
use axum::extract::{FromRequestParts, MatchedPath, Path, Request};
use axum::http::request::Parts;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use futures::Stream;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use tokio::time::Instant;

use crate::operations::ReadObserver;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{
    ApiError, AppState, AuthenticatedActor, DEFAULT_REQUEST_BODY_LIMIT_BYTES,
    INGEST_REQUEST_BODY_LIMIT_BYTES, PolicyAction, PolicyRequest,
};

#[derive(Clone, Copy)]
pub(crate) struct BodyDeadline(pub(crate) Instant);

/// MCP selects its graph after HTTP body collection. The response and owned
/// producer retain the same slot even when selection happens after disconnect.
pub(crate) type McpGraphRequest = Arc<OnceLock<GraphRequest>>;

pub(crate) fn body_timeout() -> ApiError {
    let mut error =
        ApiError::bad_request("request body deadline exceeded before operation admission");
    error.status = StatusCode::REQUEST_TIMEOUT;
    error
}

#[derive(Clone, Copy)]
enum AdmissionClass {
    Read,
    Write,
}

async fn classify(parts: &mut Parts, route: &str) -> Result<AdmissionClass, ApiError> {
    if parts.method == Method::POST
        && matches!(route, "/queries/{name}" | "/queries/{name}/if-graph-commit")
    {
        let handle = parts
            .extensions
            .get::<GraphRequest>()
            .ok_or_else(|| ApiError::internal("stored query admission is missing its graph"))?
            .clone();
        // Resolve permission before kind: otherwise saturated lanes could
        // reveal a denied query's existence through a different refusal.
        let actor = parts.extensions.get::<AuthenticatedActor>();
        match crate::handlers::authorize(
            actor,
            handle.policy.as_deref(),
            PolicyRequest {
                action: PolicyAction::InvokeQuery,
                branch: None,
                target_branch: None,
            },
        )? {
            crate::handlers::Authz::Allowed => {}
            crate::handlers::Authz::Denied(_) => {
                return Err(ApiError::not_found("stored query not found"));
            }
        }
        let Path(crate::handlers::QueryNamePath { name }) = Path::from_request_parts(parts, &())
            .await
            .map_err(|error| {
                ApiError::bad_request(format!("invalid stored query path: {error}"))
            })?;
        return Ok(
            if handle
                .queries
                .as_ref()
                .and_then(|registry| registry.lookup(&name))
                .is_some_and(|query| query.is_mutation())
            {
                AdmissionClass::Write
            } else {
                AdmissionClass::Read
            },
        );
    }
    Ok(
        if matches!(parts.method, Method::GET | Method::HEAD)
            || route == "/mcp"
            || (parts.method == Method::POST
                && matches!(route, "/query" | "/export" | "/changes/baseline"))
        {
            AdmissionClass::Read
        } else {
            AdmissionClass::Write
        },
    )
}

pub(crate) async fn admit(
    state: &AppState,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let (mut parts, body) = request.into_parts();
    // Match the registered route, not parameter values such as a stored query
    // named `load`. Axum's MatchedPath includes the router's nesting prefix.
    let matched = parts
        .extensions
        .get::<MatchedPath>()
        .ok_or_else(|| ApiError::internal("request admission is missing its matched route"))?
        .clone();
    let route = matched
        .as_str()
        .strip_prefix("/graphs/{graph_id}")
        .unwrap_or(matched.as_str());
    let class = classify(&mut parts, route).await?;
    let graph = parts.extensions.get::<GraphRequest>().cloned();
    let mcp_graph = if route == "/mcp" {
        let slot = McpGraphRequest::default();
        parts.extensions.insert(slot.clone());
        Some(slot)
    } else {
        None
    };
    let observer = match class {
        AdmissionClass::Read => state.operations.try_observe()?,
        AdmissionClass::Write => state.operations.try_observe_write()?,
    };
    let receives_body = parts.method == Method::POST || parts.method == Method::PUT;
    let raw = receives_body && route == "/load/ndjson";
    let limit = if !receives_body {
        0
    } else if route == "/cluster/deployments" {
        crate::deployment::REQUEST_BYTES
    } else if route == "/mcp" {
        crate::mcp::REQUEST_BYTES
    } else if matches!(route, "/load/ndjson" | "/load") {
        INGEST_REQUEST_BODY_LIMIT_BYTES
    } else {
        DEFAULT_REQUEST_BODY_LIMIT_BYTES
    };
    let lease = match (class, receives_body) {
        (AdmissionClass::Read, false) => IngressLease::empty(),
        (AdmissionClass::Read, true) => state
            .workload
            .try_read_ingress(limit as u64)
            .map_err(ApiError::from_workload_reject)?,
        (AdmissionClass::Write, _) => state
            .workload
            .try_ingress(limit as u64)
            .map_err(ApiError::from_workload_reject)?,
    };
    let deadline = Instant::now()
        .checked_add(state.workload.limits().body_timeout)
        .ok_or_else(|| ApiError::internal("request body timeout exceeds supported clock range"))?;
    parts.extensions.insert(BodyDeadline(deadline));
    parts.extensions.insert(observer.clone());
    parts.extensions.insert(lease.clone());
    let body = if receives_body && !raw {
        let bytes = tokio::time::timeout_at(deadline, axum::body::to_bytes(body, limit))
            .await
            .map_err(|_| body_timeout())?
            .map_err(|error| {
                let mut failure = ApiError::bad_request(format!("request body refused: {error}"));
                // The existing extractor limits use 413 for this same bound.
                // Other body transport failures remain a malformed request.
                use std::error::Error;
                if error
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                {
                    failure.status = StatusCode::PAYLOAD_TOO_LARGE;
                }
                failure
            })?;
        lease
            .shrink(bytes.len() as u64)
            .map_err(ApiError::from_workload_reject)?;
        Body::from(bytes)
    } else {
        body
    };
    // Raw NDJSON authorizes its branch scope before polling any body bytes.
    // Its collector consumes BodyDeadline and the same retained lease.
    let request = Request::from_parts(parts, body);
    match class {
        AdmissionClass::Read => {
            // Wrap before offering the owned result: even an unpolled oneshot
            // response must retain its graph through the stream's destruction.
            let response_observer = observer.clone();
            let response_input = lease.clone();
            let response_graph = graph.clone();
            let response_mcp_graph = mcp_graph.clone();
            observer
                .spawn_read((lease, graph, mcp_graph), async move {
                    Ok(observe_response(
                        next.run(request).await,
                        response_observer,
                        response_input,
                        response_graph,
                        response_mcp_graph,
                    ))
                })
                .result()
                .await
        }
        // Effectful handlers register through the owned-write boundary once
        // their typed input, actor and operation reservations are captured.
        AdmissionClass::Write => Ok(observe_response(
            next.run(request).await,
            observer,
            lease,
            graph,
            mcp_graph,
        )),
    }
}

fn observe_response(
    response: Response,
    observer: ReadObserver,
    input: IngressLease,
    graph: Option<GraphRequest>,
    mcp_graph: Option<McpGraphRequest>,
) -> Response {
    let (parts, body) = response.into_parts();
    let stream = ObservedBody {
        stream: Box::pin(body.into_data_stream()),
        observer,
        input,
        graph,
        mcp_graph,
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

struct ObservedBody<S> {
    stream: Pin<Box<S>>,
    observer: ReadObserver,
    input: IngressLease,
    // Guards follow the stream and its resources, so their final drop cannot
    // release logical ownership before wrapped response destructors have run.
    graph: Option<GraphRequest>,
    mcp_graph: Option<McpGraphRequest>,
}

impl<S> Stream for ObservedBody<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>>,
{
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx).map(|item| {
            item.map(|result| {
                result.map(|bytes| {
                    Bytes::from_owner(ObservedBytes {
                        bytes,
                        _observer: self.observer.clone(),
                        _input: self.input.clone(),
                        _graph: self.graph.clone(),
                        _mcp_graph: self.mcp_graph.clone(),
                    })
                })
            })
        })
    }
}

struct ObservedBytes {
    bytes: Bytes,
    _observer: ReadObserver,
    _input: IngressLease,
    _graph: Option<GraphRequest>,
    _mcp_graph: Option<McpGraphRequest>,
}

impl AsRef<[u8]> for ObservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::State;
    use axum::middleware;
    use axum::routing::{MethodRouter, get, post};
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tower::ServiceExt;

    use crate::operations::OperationRuntime;
    use crate::workload::{WorkloadController, WorkloadLimits, WorkloadSnapshot};

    fn router(limits: WorkloadLimits) -> (Router, AppState, Arc<AtomicUsize>) {
        router_with_read(limits, post(|| async { "read response" }))
    }

    fn router_with_read(
        limits: WorkloadLimits,
        read: MethodRouter,
    ) -> (Router, AppState, Arc<AtomicUsize>) {
        // Admission itself needs no graph fixture: an independent handler
        // census proves whether the refused request reached execution.
        let state = AppState::new_multi(
            Vec::new(),
            Vec::new(),
            None,
            WorkloadController::with_limits(limits),
            None,
        )
        .unwrap()
        .with_operations(OperationRuntime::with_read_limit(1));
        let entered = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&entered);
        let app = Router::new()
            .route("/snapshot", get(|| async { "read response" }))
            .route("/query", read)
            .route(
                "/write",
                post(move || {
                    let observed = Arc::clone(&observed);
                    async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NO_CONTENT
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                |State(state): State<AppState>, request: Request, next: Next| async move {
                    admit(&state, request, next).await
                },
            ));
        (app, state, entered)
    }

    fn request(body: Body) -> Request {
        Request::builder()
            .method(Method::POST)
            .uri("/write")
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn disconnected_read_keeps_handler_input_and_shutdown_ownership() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let completed = Arc::new(AtomicUsize::new(0));
            let (app, state, _) = router_with_read(
                WorkloadLimits::default(),
                post({
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let completed = Arc::clone(&completed);
                    move |body: Bytes| {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        let completed = Arc::clone(&completed);
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            assert_eq!(body.as_ref(), b"query input");
                            completed.fetch_add(1, Ordering::SeqCst);
                            "read response"
                        }
                    }
                }),
            );
            let caller = tokio::spawn(
                app.clone().oneshot(
                    Request::post("/query")
                        .body(Body::from("query input"))
                        .unwrap(),
                ),
            );
            entered.notified().await;
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert_eq!(state.operations.snapshot().active_reads, 1);
            assert_eq!(state.workload.snapshot().read_ingress_bytes, 11);
            assert_eq!(state.workload.snapshot().read_ingress_count, 1);
            let refused = app
                .oneshot(Request::get("/snapshot").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
            state.operations.close();
            let shutdown = state.operations.wait_logical_owners();
            tokio::pin!(shutdown);
            assert!(futures::poll!(&mut shutdown).is_pending());
            release.notify_one();
            assert!(shutdown.await);
            assert_eq!(completed.load(Ordering::SeqCst), 1);
            assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
        })
        .await
        .expect("disconnected read did not settle");
    }

    #[tokio::test]
    async fn held_read_response_does_not_consume_write_admission() {
        for body_bearing in [false, true] {
            let (app, state, entered) = router(WorkloadLimits {
                ingress_inflight_max: 1,
                read_ingress_inflight_max: 1,
                ..WorkloadLimits::default()
            });
            let request_builder = if body_bearing {
                Request::post("/query")
            } else {
                Request::get("/snapshot")
            };
            let read = app
                .clone()
                .oneshot(request_builder.body(Body::from("payload")).unwrap())
                .await
                .unwrap();
            assert_eq!(read.status(), StatusCode::OK);
            let mut body = read.into_body().into_data_stream();
            let retained = body.next().await.unwrap().unwrap();
            drop(body);
            let refused = app
                .clone()
                .oneshot(Request::get("/snapshot").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(state.workload.snapshot().ingress_count, 0);
            assert_eq!(
                state.workload.snapshot().read_ingress_count,
                u64::from(body_bearing)
            );
            let response = app.oneshot(request(Body::from("payload"))).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::NO_CONTENT,
                "a held read must not consume the write input or response lane"
            );
            assert_eq!(entered.load(Ordering::SeqCst), 1);
            assert_eq!(state.operations.snapshot().active_write_responses, 1);
            drop(response);
            assert_eq!(state.operations.snapshot().active_reads, 1);
            assert_eq!(state.operations.snapshot().active_write_responses, 0);
            drop(retained);
            assert_eq!(state.operations.snapshot().active_reads, 0);
            assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
        }
    }

    #[tokio::test]
    async fn yielded_bytes_keep_observer_and_input_after_response_body_drops() {
        use crate::registry::{GraphHandle, GraphRegistry, RegistryCapture};
        use crate::{GraphId, GraphKey};

        for deferred in [false, true] {
            let temp = tempfile::TempDir::new().unwrap();
            let uri = temp.path().join("graph").to_string_lossy().into_owned();
            let key = GraphKey::cluster(GraphId::try_from("alpha").unwrap());
            let handle = Arc::new(GraphHandle {
                key: key.clone(),
                uri: uri.clone(),
                engine: Arc::new(
                    omnigraph::db::Omnigraph::init(&uri, "node Person { name: String @key }\n")
                        .await
                        .unwrap(),
                ),
                policy: None,
                queries: None,
            });
            let registry = Arc::new(GraphRegistry::from_handles(vec![handle]).unwrap());
            let operations = OperationRuntime::with_read_limit(1);
            let RegistryCapture::Ready(graph) = registry.capture(&operations, &key).unwrap() else {
                panic!("fixture graph must be available");
            };
            let workload = WorkloadController::with_limits(WorkloadLimits {
                read_ingress_inflight_max: 1,
                ..WorkloadLimits::default()
            });
            let input = workload.try_read_ingress(8).unwrap();
            let observer = operations.try_observe().unwrap();
            let slot = McpGraphRequest::default();
            let mut late_graph = Some(graph);
            let mut body = ObservedBody {
                stream: Box::pin(futures::stream::iter([
                    Ok::<_, axum::Error>(Bytes::from_static(b"first")),
                    Ok(Bytes::from_static(b"second")),
                ])),
                observer,
                input,
                graph: if deferred { None } else { late_graph.take() },
                mcp_graph: deferred.then(|| slot.clone()),
            };
            assert!(operations.try_observe().is_err());
            assert!(workload.try_read_ingress(0).is_err());
            let first = body.next().await.unwrap().unwrap();
            let second = body.next().await.unwrap().unwrap();
            let retained_slice = first.slice(1..4);
            assert_eq!(retained_slice.as_ref(), b"irs");
            drop(body);
            drop(first);
            if deferred {
                // MCP may select a graph after the HTTP response disappeared.
                // Even bytes yielded before selection retain this shared slot.
                assert!(slot.set(late_graph.take().unwrap()).is_ok());
            }
            drop(slot);
            let transition = registry
                .prepare_same_view(&operations, &key, Instant::now() + Duration::from_secs(2))
                .unwrap()
                .close()
                .unwrap();
            let settled = transition.wait_requests();
            tokio::pin!(settled);
            assert!(futures::poll!(&mut settled).is_pending());
            assert_eq!(operations.snapshot().active_reads, 1);
            assert_eq!(workload.snapshot().read_ingress_bytes, 8);
            drop(second);
            assert!(operations.try_observe().is_err());
            assert!(workload.try_read_ingress(0).is_err());
            assert!(futures::poll!(&mut settled).is_pending());
            drop(retained_slice);
            settled.await.unwrap();
            assert_eq!(operations.snapshot().active_reads, 0);
            assert_eq!(workload.snapshot(), WorkloadSnapshot::default());
            assert!(operations.try_observe().is_ok());
            assert!(workload.try_read_ingress(0).is_ok());
        }
    }

    #[tokio::test]
    async fn abandoned_body_does_not_release_a_running_producers_registration() {
        let operations = OperationRuntime::with_read_limit(1);
        let workload = WorkloadController::with_defaults();
        let observer = operations.try_observe().unwrap();
        let input = workload.try_read_ingress(8).unwrap();
        let producer_observer = observer.clone();
        let producer_input = input.clone();
        let (release, wait) = tokio::sync::oneshot::channel();
        let producer = tokio::spawn(async move {
            let _observer = producer_observer;
            let _input = producer_input;
            wait.await.unwrap();
        });
        let body = ObservedBody {
            stream: Box::pin(futures::stream::pending::<Result<Bytes, axum::Error>>()),
            observer,
            input,
            graph: None,
            mcp_graph: None,
        };
        operations.close();
        drop(body);
        assert_eq!(operations.snapshot().active_reads, 1);
        assert_eq!(workload.snapshot().read_ingress_count, 1);
        assert_eq!(workload.snapshot().read_ingress_bytes, 8);
        release.send(()).unwrap();
        producer.await.unwrap();
        assert!(operations.wait_logical_owners().await);
        assert_eq!(workload.snapshot(), WorkloadSnapshot::default());
    }

    #[tokio::test]
    async fn completed_handler_retains_actual_input_until_response_body_drops() {
        let (app, state, entered) = router(WorkloadLimits::default());
        let response = app.oneshot(request(Body::from("payload"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert_eq!(state.operations.snapshot().active_reads, 0);
        assert_eq!(state.operations.snapshot().active_write_responses, 1);
        assert_eq!(state.workload.snapshot().ingress_count, 1);
        assert_eq!(state.workload.snapshot().ingress_bytes, 7);
        drop(response);
        assert_eq!(state.operations.snapshot().active_reads, 0);
        assert_eq!(state.operations.snapshot().active_write_responses, 0);
        assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
    }

    #[tokio::test]
    async fn ordinary_body_deadline_refuses_without_dispatch_or_retained_capacity() {
        let (app, state, entered) = router(WorkloadLimits {
            body_timeout: Duration::from_millis(1),
            ..WorkloadLimits::default()
        });
        let body = Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let response = tokio::time::timeout(Duration::from_secs(2), app.oneshot(request(body)))
            .await
            .expect("the input deadline must bound an incomplete body")
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert_eq!(state.operations.snapshot().active_reads, 0);
        assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
    }

    #[tokio::test]
    async fn oversized_body_refuses_before_handler_execution_and_releases_capacity() {
        let (app, state, entered) = router(WorkloadLimits::default());
        let response = app
            .oneshot(request(Body::from(vec![
                b' ';
                DEFAULT_REQUEST_BODY_LIMIT_BYTES
                    + 1
            ])))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert_eq!(state.operations.snapshot().active_reads, 0);
        assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
    }

    #[tokio::test]
    async fn saturated_ingress_never_polls_the_refused_body() {
        let (app, state, entered) = router(WorkloadLimits {
            ingress_inflight_max: 1,
            ..WorkloadLimits::default()
        });
        let retained = state.workload.try_ingress(8).unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let body = Body::from_stream(futures::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(None::<Result<Bytes, std::io::Error>>)
        }));
        let response = app.oneshot(request(body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert_eq!(state.operations.snapshot().active_reads, 0);
        assert_eq!(state.workload.snapshot().ingress_count, 1);
        assert_eq!(state.workload.snapshot().ingress_bytes, 8);
        drop(retained);
        assert_eq!(state.workload.snapshot(), WorkloadSnapshot::default());
    }
}
