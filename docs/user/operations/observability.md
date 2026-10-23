# Server logs and traces

The server writes text logs to stdout by default. Set
`OMNIGRAPH_LOG_FORMAT=json` for structured logs. HTTP completion records include
the route template, method, status and `response_headers_ms`: elapsed time until
response headers are available, not the duration of a streaming body. Unknown
routes use `<unmatched>`; request URLs, query strings, headers and bodies are not
included in these records.

To export traces, point the server at an OpenTelemetry collector:

```bash
OMNIGRAPH_LOG_FORMAT=json \
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 \
OTEL_SERVICE_NAME=omnigraph-server \
OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=production,service.version=build-123 \
  omnigraph-server --cluster /srv/omnigraph/company-brain --bind 0.0.0.0:8080
```

Export is off unless an endpoint is explicitly set. The supported transport is
OTLP HTTP/protobuf; a general endpoint gets `/v1/traces` appended.
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` overrides it with an exact trace URL.
Endpoints must use HTTP or HTTPS without userinfo, query parameters or fragments.
Use HTTPS outside a trusted private network. `OTEL_SDK_DISABLED=true` disables
export without disabling logs. Normal authentication configuration still applies.

Incoming W3C `traceparent` and `tracestate` connect HTTP requests to their callers.
Owned read/write spans follow the work even when the caller stops waiting;
JSON logs include their `trace_id` and `span_id`. Context is correlation only and
never supplies an actor or changes permissions. Baggage is not imported.
The SDK's `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` control sampling;
the default follows the parent, or samples a root request.

Only explicitly authored request and native-operation spans are exported.
`RUST_LOG` controls stdout log events (`info` by default), independently of
trace export and sampling. Setting it to `warn`, `error` or `off` does not
disable traces; warnings and errors still retain trace correlation when INFO
logs are suppressed. The `rmcp`, `lance::dataset_events` and
`lance::file_audit` targets remain restricted to warnings/errors, even with
debug overrides. These targets can include query arguments/results, full storage
URIs and deletion predicates. `omnigraph_server::init_tracing()` also enforces
the restrictions; embedders using their own subscriber must preserve them.
A failed Blob delivery, change-route request or streamed response is logged by
its error class, never by its message; see [HTTP errors](troubleshooting.md#http-errors).
Collect stdout separately for logs; the server does not send
the same logs over OTLP. Resource attributes are operator-provided metadata;
do not place secrets or graph contents in them.

Export uses a bounded queue of 2,048 spans, batches of up to 256, a five-second
flush interval and a three-second HTTP timeout. A full queue drops telemetry;
collector failure never blocks a graph operation. After serving stops, a
best-effort flush starts in the background with a five-second bound; it never
delays process exit or changes native shutdown deadlines. Process exit can
lose buffered spans; telemetry is not durable operation evidence.
