- Server logs no longer carry engine error text outside the Blob routes. An
  internal failure on a change route logs `error_kind="change_route_internal"`,
  and an export or change-baseline response that fails after its 200 headers
  logs `error_kind="served_stream_failed"`, each with the error's class
  (`error_variant`, `storage_kind`, `manifest_kind`) instead of its message,
  which can hold object URIs or credentials. A stored-query invocation logs
  the graph's ID instead of its storage root, a graph that fails to open at
  startup logs its root without userinfo, query or fragment, and a refused
  inline embedding `api_key` is no longer echoed. Responses are unchanged. See
  [HTTP errors][server-logs-error-class].

[server-logs-error-class]: ../docs/user/operations/troubleshooting.md#http-errors
