- The server supports optional OTLP HTTP/protobuf tracing and structured JSON
  logs with trace correlation. Request diagnostics use route templates and
  exclude request payloads and credentials. Log verbosity does not disable
  tracing or warning/error correlation. See [logs and traces][server-otel-guide].

[server-otel-guide]: ../docs/user/operations/observability.md
