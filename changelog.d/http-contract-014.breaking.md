- The CLI and server now require exactly `Omnigraph-Http-Api: 0.14`; `0.13`,
  missing, repeated and combined values are refused before graph access or body
  decoding, and the CLI refuses a server that answers discovery with `0.13`
  before sending. Contract `0.14` adds the `Omnigraph-If-Actor` request
  header. Refresh the CLI, server, Studio and HTTP integrations together:
  earlier prerelease 0.14 builds share this header, so header admission cannot
  detect their different response shapes and selectors. See the
  [HTTP contract][http-contract-014].
- Regenerate SDKs for the finalized unreleased 0.14 API: graph operation IDs
  now use the shared operation names without `cluster_`. Generated method
  names change; HTTP methods and URLs do not.
- Regenerate SDK path arguments: `branch` becomes `branch_name`, stored-query
  `name` becomes `query_name`, and deployment `id` becomes `deployment_id`.
  The request URLs are unchanged.
- Update generated SDK types: the five `*Response` schemas become `*Output`,
  the six published string `*Output` enums lose that suffix, and
  `BlobEntityKind` and `EntityKindOutput` become one `EntityKind`. Serialized
  enum values are unchanged.
- Read `actor_id` instead of `actor` from HTTP and MCP access results and
  `omnigraph access --json`; the CLI access-report protocol is now 2. After
  refreshing the server, reconnect MCP clients and refresh cached tool schemas
  through `tools/list`; clients with fixed response types must update those
  types before reconnecting. Old MCP clients are not automatically refused:
  `/mcp` does not use the HTTP contract header, and MCP protocol negotiation
  is unchanged. See [MCP upgrades][http-014-mcp].
- Replace the HTTP commit-list booleans with `listing=unpublished` or
  `listing=unattached`. Omit `listing`, or use `listing=history`, for ordinary
  history. Either old query key returns `400 bad_request` even when false or
  combined with `listing`; non-history listings still refuse `limit` and
  `page_token`. CLI `--unpublished` and `--unattached` flags are unchanged.

[http-contract-014]: ../docs/user/operations/server.md#http-contract
[http-014-mcp]: ../docs/user/operations/mcp.md#upgrade-tool-schemas
