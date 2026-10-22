- Upgrade CLI, server and HTTP integrations together to contract 0.15, then
  regenerate SDKs: graph operation IDs now use the shared operation names
  without `cluster_`. Generated method names change; HTTP methods and URLs do
  not. Protected HTTP requests carrying 0.14 are refused. See the
  [HTTP contract guide][http-015-contract].
- Regenerate SDK path arguments: `branch` becomes `branch_name`, stored-query
  `name` becomes `query_name`, and deployment `id` becomes `deployment_id`.
  The request URLs are unchanged.
- Update generated SDK types: the five `*Response` schemas become `*Output`,
  the six published string `*Output` enums lose that suffix, and
  `BlobEntityKind` and `EntityKindOutput` become one `EntityKind`. Serialized
  enum values are unchanged.
- Read `actor_id` instead of `actor` from HTTP and MCP access results and
  `omnigraph access --json`; the CLI access-report protocol is now 2. After
  upgrading the server, reconnect MCP clients and refresh cached tool schemas
  through `tools/list`; clients with fixed response types must update those
  types before reconnecting. Old MCP clients are not automatically refused:
  `/mcp` does not use the HTTP contract header, and MCP protocol negotiation
  is unchanged. See [MCP upgrades][http-015-mcp].
- Replace the HTTP commit-list booleans with `listing=unpublished` or
  `listing=unattached`. Omit `listing`, or use `listing=history`, for ordinary
  history. Either old query key returns `400 bad_request` even when false or
  combined with `listing`; non-history listings still refuse `limit` and
  `page_token`. CLI `--unpublished` and `--unattached` flags are unchanged.

[http-015-contract]: ../docs/user/operations/server.md#http-contract
[http-015-mcp]: ../docs/user/operations/mcp.md#upgrade-tool-schemas
