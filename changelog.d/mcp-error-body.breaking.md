- **MCP tool errors carry the HTTP error body.** A refused tool call's
  structured content is `{"status": <HTTP status>, "error": <error body>}`,
  the same `error`, `code`, `disposition` and details an HTTP route answers,
  also for the refusals MCP makes itself, which answered `{code, message}`
  before: an unverified credential is `unauthorized`, a full endpoint
  `too_many_requests`, and `cancelled`, `deadline_exceeded` and
  `result_too_large`, which only MCP sends, carry no `status`. A write that may
  have happened answers `code: outcome_unknown` with `effects: unknown`, in
  place of the `outcome_unknown: true` flag, and a resource read's JSON-RPC
  error carries the same body as `data`. Deployment routes on a server without
  authentication now refuse with a `403` error body instead of a plain-text
  `500`, and their bodies keep a `413` or `415` status where they answered
  `400`. Update MCP clients that read `code`, `message` or `outcome_unknown` at
  the top of an error result. See [MCP results and errors][mcp-error-body-guide].

[mcp-error-body-guide]: ../docs/user/operations/mcp.md#results-and-errors
