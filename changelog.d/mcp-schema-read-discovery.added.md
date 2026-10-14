- The server's `/mcp` endpoint gains a `schema` tool, which returns a graph's
  `.pg` schema under the `read` action, and an opt-in `read` tool, which runs an
  ad-hoc GQ read through `POST /query`, refuses mutations, and is listed only
  when the server starts with `--mcp-allow-adhoc` (or
  `OMNIGRAPH_MCP_ALLOW_ADHOC=1`). Tools now carry titles, fuller descriptions
  and, where the result shape is fixed, an output schema, and `initialize`
  returns instructions that tell a model to read the schema first. See
  [MCP][mcp-schema-read-guide].
- Protected-resource metadata is also served at the path the `/mcp` challenge
  advertises (`/.well-known/oauth-protected-resource/<resource path>`), so an
  OAuth client such as Claude connects to a resource identifier with a path
  without a proxy rewrite, and a refused bearer token now gets
  `error="invalid_token"` in that challenge.

[mcp-schema-read-guide]: ../docs/user/operations/mcp.md
