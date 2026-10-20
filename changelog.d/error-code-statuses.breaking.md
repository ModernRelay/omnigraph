- **Every error code names the statuses it is sent under.** A Blob `If-Match` that fails
  answers `412` `precondition_failed` instead of `conflict`, and `408`, `413`,
  `415` and `416` answer `request_timeout`, `payload_too_large`,
  `unsupported_media_type` and `range_not_satisfiable` instead of
  `bad_request`, a resource limit's `413` included. A write the server could
  not settle answers `500` `outcome_unknown` whatever failed underneath, and
  every `503` that asks for a retry, a graph that is not served or closed
  admission, carries `Retry-After` as a `429` does. `ErrorCode` in
  `openapi.json` names each code's status and meaning, every protected
  operation documents the `404` and `405` its router answers, and header
  parameters are published as `Omnigraph-Http-Api` and
  `Omnigraph-Expect-Actor`. Update clients that matched those refusals by
  `conflict` or `bad_request`, and regenerate clients that key on header
  parameter names. See [HTTP errors][error-code-statuses-guide].

[error-code-statuses-guide]: ../docs/user/operations/troubleshooting.md#http-errors
