//! Optional vendor-neutral tracing and safe HTTP request diagnostics.
//!
//! Export failure never changes request admission, execution, or ownership.

use std::fmt;
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use color_eyre::eyre::{Result, bail, eyre};
use opentelemetry::KeyValue;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{Status, TraceContextExt, TracerProvider};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, SdkTracer, SdkTracerProvider,
};
use tracing::{Instrument, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, MakeWriter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const TARGET: &str = "omnigraph_server::telemetry";
const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Retain until serving has finished. Its bounded flush is telemetry-only,
/// never native cleanup or a reason to delay the native shutdown deadline.
pub struct TelemetryGuard(Option<SdkTracerProvider>);

impl TelemetryGuard {
    /// Start a best-effort flush without delaying process exit. An embedder
    /// without a native shutdown deadline may call [`Self::shutdown`] instead.
    pub fn shutdown_in_background(self) {
        if self.0.is_none() {
            return;
        }
        // If spawning fails, leave the provider for process exit rather than
        // running its blocking Drop on the native shutdown path.
        let guard = std::mem::ManuallyDrop::new(self);
        if std::thread::Builder::new()
            .name("telemetry-shutdown".into())
            .spawn(move || std::mem::ManuallyDrop::into_inner(guard).shutdown())
            .is_err()
        {
            tracing::warn!(target: TARGET, "trace exporter shutdown worker could not start");
        }
    }

    /// Flush on a blocking thread, bounded independently of native cleanup.
    pub fn shutdown(self) {
        if let Some(provider) = self.0
            && provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT).is_err()
        {
            tracing::warn!(target: TARGET, "trace exporter shutdown did not complete");
        }
    }
}

/// Install the binary's logging and optional OTLP exporter. Call from a
/// blocking thread: the exporter owns a bounded worker and blocking HTTP client.
/// An explicit OTLP endpoint opts in; no default network destination is used.
pub fn init() -> Result<TelemetryGuard> {
    let config = Config::read(|key| std::env::var(key).ok())?;
    let provider = config.endpoint.as_deref().map(build_provider).transpose()?;
    let tracer = provider
        .as_ref()
        .map(|provider| provider.tracer("omnigraph-server"));
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if subscriber(filter, std::io::stdout, config.json, tracer)
        .try_init()
        .is_err()
    {
        if let Some(provider) = provider {
            let _ = provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
        bail!("server telemetry subscriber is already installed");
    }
    Ok(TelemetryGuard(provider))
}

#[derive(Debug, PartialEq, Eq)]
struct Config {
    json: bool,
    endpoint: Option<String>,
}

impl Config {
    fn read(env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let json = match env("OMNIGRAPH_LOG_FORMAT").as_deref().unwrap_or("text") {
            "text" => false,
            "json" => true,
            _ => bail!("OMNIGRAPH_LOG_FORMAT must be text or json"),
        };
        let disabled =
            env("OTEL_SDK_DISABLED").is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let specific = env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").filter(|value| !value.is_empty());
        let general = env("OTEL_EXPORTER_OTLP_ENDPOINT").filter(|value| !value.is_empty());
        let endpoint = if disabled {
            None
        } else if let Some(endpoint) = specific {
            Some(endpoint)
        } else {
            general.map(|endpoint| format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        };
        if let Some(endpoint) = &endpoint {
            let protocol = env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
                .or_else(|| env("OTEL_EXPORTER_OTLP_PROTOCOL"));
            if protocol.is_some_and(|value| value != "http/protobuf") {
                bail!("server trace export supports only OTLP http/protobuf");
            }
            // Never put a potentially credential-bearing endpoint in errors.
            let url =
                url::Url::parse(endpoint).map_err(|_| eyre!("invalid OTLP trace endpoint"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                bail!("OTLP trace endpoint must be HTTP(S) without userinfo, query or fragment");
            }
        }
        Ok(Self { json, endpoint })
    }
}

fn build_provider(endpoint: &str) -> Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::HttpBinary)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|_| eyre!("could not initialize OTLP trace exporter"))?;
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(2048)
                .with_max_export_batch_size(256)
                .with_scheduled_delay(Duration::from_secs(5))
                .build(),
        )
        .build();
    // Resource::builder reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES.
    let detected = Resource::builder().build();
    let resource = Resource::builder_empty()
        .with_service_name("omnigraph-server")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attributes(
            detected
                .iter()
                .filter(|(key, value)| {
                    key.as_str() != "service.name" || value.to_string() != "unknown_service"
                })
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        )
        .build();
    Ok(SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build())
}

pub(crate) fn subscriber<W>(
    filter: EnvFilter,
    writer: W,
    json: bool,
    tracer: Option<SdkTracer>,
) -> Box<dyn Subscriber + Send + Sync>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let telemetry = tracer.map(|tracer| {
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            // Export only explicitly authored spans/events. A library's DEBUG
            // request body or storage path must never become a trace payload.
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() == TARGET
            }))
    });
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::filter::filter_fn(
            super::safe_log_metadata,
        ))
        .with(telemetry);
    if json {
        Box::new(
            subscriber.with(
                tracing_subscriber::fmt::layer()
                    .fmt_fields(JsonFields::new())
                    .event_format(CorrelatedJson)
                    .with_writer(writer),
            ),
        )
    } else {
        Box::new(subscriber.with(tracing_subscriber::fmt::layer().with_writer(writer)))
    }
}

struct CorrelatedJson;

impl<S> FormatEvent<S, JsonFields> for CorrelatedJson
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        // Keep tracing-subscriber's field/time handling; add only standard
        // correlation IDs. Never recover IDs from raw incoming header text.
        let mut encoded = String::new();
        tracing_subscriber::fmt::format()
            .json()
            .flatten_event(true)
            .format_event(ctx, Writer::new(&mut encoded), event)?;
        let mut record: serde_json::Value =
            serde_json::from_str(&encoded).map_err(|_| fmt::Error)?;
        let context = Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        if span_context.is_valid() {
            record["trace_id"] = span_context.trace_id().to_string().into();
            record["span_id"] = span_context.span_id().to_string().into();
        }
        writeln!(writer, "{record}")
    }
}

struct TraceHeaders<'a>(&'a HeaderMap);

impl Extractor for TraceHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        // W3C context is correlation only. Never import baggage, credentials or
        // arbitrary request headers into a span or log.
        matches!(key, "traceparent" | "tracestate")
            .then(|| {
                self.0
                    .get(key)?
                    .to_str()
                    .ok()
                    .filter(|value| value.len() <= 512)
            })
            .flatten()
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
}

pub(crate) async fn http_request(request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("<unmatched>", MatchedPath::as_str);
    let method = match request.method().as_str() {
        "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD" | "OPTIONS" | "CONNECT" | "TRACE" => {
            request.method().as_str()
        }
        _ => "_OTHER",
    };
    let span = tracing::info_span!(
        target: TARGET,
        parent: None,
        "http.server",
        otel.name = %format!("{method} {route}"),
        otel.kind = "server",
        http.request.method = method,
        http.route = route,
        http.response.status_code = tracing::field::Empty,
        deployment.id = tracing::field::Empty,
    );
    let parent = TraceContextPropagator::new().extract(&TraceHeaders(request.headers()));
    let _ = span.set_parent(parent);
    async move {
        let started = Instant::now();
        let response = next.run(request).await;
        let status = response.status();
        let current = Span::current();
        current.record("http.response.status_code", status.as_u16());
        if status.is_server_error() {
            current.set_status(Status::error("HTTP server error"));
        }
        tracing::info!(target: TARGET, status_code = status.as_u16(), response_headers_ms = started.elapsed().as_secs_f64() * 1000.0, "http request completed");
        response
    }
    .instrument(span)
    .await
}

pub(crate) fn operation_span(kind: &'static str) -> Span {
    tracing::info_span!(target: TARGET, "native.operation", otel.kind = "internal", operation.kind = kind)
}

pub(crate) fn operation_finished(kind: &'static str, succeeded: bool, uncertain: bool) {
    let outcome = if uncertain {
        "uncertain"
    } else if succeeded {
        "succeeded"
    } else {
        "failed"
    };
    if !succeeded || uncertain {
        Span::current().set_status(Status::error(outcome));
    }
    tracing::info!(target: TARGET, "operation.kind" = kind, "operation.outcome" = outcome, "native operation completed");
}

pub(crate) fn deployment_id(id: &str) {
    if !id.is_empty()
        && id.len() <= 75
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_:".contains(&byte))
    {
        Span::current().record("deployment.id", id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, Bytes};
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Router, middleware};
    use opentelemetry::trace::{SpanKind, Tracer};
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use prost::Message;
    use tower::ServiceExt;
    use tracing::instrument::WithSubscriber;

    use crate::operations::{OperationRuntime, OwnedResult};
    use crate::test_log_capture::Capture;

    fn config(entries: &[(&str, &str)]) -> Result<Config> {
        Config::read(|key| {
            entries
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn exporter_requires_an_explicit_endpoint_and_valid_safe_configuration() {
        assert_eq!(
            config(&[]).unwrap(),
            Config {
                json: false,
                endpoint: None
            }
        );
        let general = ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318/");
        let specific = (
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "https://collector/custom-traces",
        );
        assert_eq!(
            config(&[general]).unwrap().endpoint.as_deref(),
            Some("http://collector:4318/v1/traces")
        );
        assert_eq!(
            config(&[general, specific]).unwrap().endpoint.as_deref(),
            Some(specific.1)
        );
        assert!(
            config(&[general, ("OTEL_SDK_DISABLED", "true")])
                .unwrap()
                .endpoint
                .is_none()
        );
        assert!(config(&[("OMNIGRAPH_LOG_FORMAT", "json")]).unwrap().json);
        assert!(config(&[("OMNIGRAPH_LOG_FORMAT", "invalid")]).is_err());
        assert!(config(&[general, ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")]).is_err());
        for endpoint in [
            "not-a-url",
            "file:///private",
            "http://user:PRIVATE@collector",
            "https://collector?token=PRIVATE",
            "https://collector/#PRIVATE",
        ] {
            let error = config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)])
                .unwrap_err()
                .to_string();
            assert!(!error.contains("PRIVATE"), "{error}");
        }
    }

    #[tokio::test]
    async fn http_and_owned_operations_preserve_parent_context_without_payloads() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let captured = Capture::default();
        let writer = captured.clone();
        let subscriber = subscriber(
            EnvFilter::new("info"),
            move || writer.clone(),
            true,
            Some(provider.tracer("test")),
        );
        async {
            let app = Router::new()
                .route(
                    "/graphs/{graph_id}/query",
                    post(|| async {
                        let runtime = OperationRuntime::new();
                        runtime
                            .submit((), async {
                                OwnedResult {
                                    result: Ok(()),
                                    uncertain: false,
                                }
                            })
                            .unwrap()
                            .result()
                            .await
                            .unwrap();
                        runtime
                            .try_observe()
                            .unwrap()
                            .spawn_read((), async { Ok(()) })
                            .result()
                            .await
                            .unwrap();
                        StatusCode::SERVICE_UNAVAILABLE
                    }),
                )
                .layer(middleware::from_fn(http_request));
            let response = app
                .oneshot(
                    Request::post("/graphs/PRIVATE_GRAPH/query?secret=PRIVATE_QUERY")
                        .header(
                            "traceparent",
                            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                        )
                        .header("authorization", "Bearer PRIVATE_TOKEN")
                        .header("baggage", "secret=PRIVATE_BAGGAGE")
                        .body(Body::from("PRIVATE_BODY"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        .with_subscriber(subscriber)
        .await;
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 3, "{spans:?}");
        let http = spans
            .iter()
            .find(|span| span.span_kind == SpanKind::Server)
            .unwrap();
        assert_eq!(http.name, "POST /graphs/{graph_id}/query");
        assert_eq!(
            http.span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(http.parent_span_id.to_string(), "b7ad6b7169203331");
        assert!(matches!(http.status, Status::Error { .. }));
        for native in spans.iter().filter(|span| span.name == "native.operation") {
            assert_eq!(native.parent_span_id, http.span_context.span_id());
            assert_eq!(native.span_context.trace_id(), http.span_context.trace_id());
        }
        let logs = captured.output();
        let completion: serde_json::Value = logs
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|line| line["message"] == "http request completed")
            .unwrap();
        assert_eq!(
            completion["trace_id"],
            http.span_context.trace_id().to_string()
        );
        assert_eq!(
            completion["span_id"],
            http.span_context.span_id().to_string()
        );
        assert_eq!(completion["status_code"], 503);
        assert!(completion["response_headers_ms"].is_number());
        for private in [
            "PRIVATE_GRAPH",
            "PRIVATE_QUERY",
            "PRIVATE_TOKEN",
            "PRIVATE_BAGGAGE",
            "PRIVATE_BODY",
        ] {
            assert!(!logs.contains(private), "{logs}");
            assert!(!format!("{spans:?}").contains(private));
        }
        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn malformed_context_and_unknown_routes_never_echo_request_input() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let captured = Capture::default();
        let writer = captured.clone();
        let subscriber = subscriber(
            EnvFilter::new("info"),
            move || writer.clone(),
            true,
            Some(provider.tracer("test")),
        );
        async {
            let response = Router::new()
                .layer(middleware::from_fn(http_request))
                .oneshot(
                    Request::get("/PRIVATE_PATH?PRIVATE_QUERY")
                        .header("traceparent", "PRIVATE_INVALID_CONTEXT")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        .with_subscriber(subscriber)
        .await;
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "GET <unmatched>");
        assert!(!spans[0].parent_span_id.is_valid());
        assert!(spans[0].span_context.is_valid());
        assert!(!captured.output().contains("PRIVATE"));
        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn bounded_worker_exports_real_otlp_protobuf_and_flushes_before_shutdown() {
        let (sent, received) = tokio::sync::oneshot::channel();
        let sender = std::sync::Arc::new(std::sync::Mutex::new(Some(sent)));
        let app = Router::new().route(
            "/v1/traces",
            post(move |headers: HeaderMap, body: Bytes| {
                let sender = sender.clone();
                async move {
                    let _ = sender.lock().unwrap().take().unwrap().send((headers, body));
                    StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::task::spawn_blocking(move || {
            let provider = build_provider(&format!("http://{address}/v1/traces")).unwrap();
            provider
                .tracer("otlp-proof")
                .in_span("export-proof", |_| {});
            TelemetryGuard(Some(provider)).shutdown();
        })
        .await
        .unwrap();
        let (headers, body) = tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(headers["content-type"], "application/x-protobuf");
        let payload =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(
                body,
            )
            .unwrap();
        let span = &payload.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.name, "export-proof");
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
        server.abort();
    }
}
