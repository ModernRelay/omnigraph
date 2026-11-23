# MCP server surface

**Decides:** `omnigraph-server` is the one MCP implementation: a stateless `/mcp` endpoint whose every tool calls the HTTP handler of its route, under the same actor and Cedar action, so policy alone decides what a credential can do.
**Contract:** keeps E4, E5, E6, write:3
**Code:** `crates/omnigraph-mcp`, `crates/omnigraph-server/src/mcp.rs`, `crates/omnigraph-server/src/challenge.rs`, `crates/omnigraph-server/tests/integration/mcp.rs`
**Cited as:** RFC 0003

## Context

Agents reach a graph through MCP clients, some of which speak only Streamable HTTP and cannot run a local stdio process. A second MCP implementation beside the HTTP routes drifts in tools, errors and policy meaning. An MCP layer that filters tools by boot settings protects nothing, because the same credential runs the same operation over HTTP. Discovery must work without a proxy rewrite, and a refused token must be distinguishable from a missing one.

## Decision

- One endpoint, `POST /mcp`, per server. Graph-scoped tools take a `graph` argument and resolve it as an HTTP request does: unknown or undisclosed is 404, authorized but unavailable is 503.
- `/mcp` is mounted whenever the server has a bearer credential source (static tokens, signed data tokens or OIDC), and is never mounted without one.
- The transport is the official Rust SDK in stateless mode. Every HTTP request authenticates again; no MCP session holds an actor.
- Each tool is a static catalog entry that decodes its arguments (`deny_unknown_fields`) and calls the route's handler. The tool list is the shared operation inventory; listing a tool grants nothing.
- Every tool is listed. The only MCP boot setting is the allowed `Host` list (`--mcp-allowed-host`).
- Byte streams (Blob payloads, `export`, the change-feed baseline) and schema apply, deployment and cluster configuration are never tools.
- `query` takes GQ source or a stored read's `name`; `mutate` takes source or a stored mutation's `name`. Given `if_graph_commit`, `mutate` uses the fail-closed conditional handler.
- A success's `structuredContent` equals the route's body, except named projections. A failure is `isError` with the route's status and `ErrorOutput`, or an MCP-side code such as `capacity_exceeded`.
- Limits: 64 KiB per request, 1 MiB per result, a 30 second wait, 16 concurrent tool executions. The wait ends observation, not execution: the owned work keeps its slot and reservations until it finishes. A write whose wait ends answers `outcome_unknown`, and the server never replays a tool call.
- Writes reserve write ingress and per-actor admission as their route does, but share the read lane and the 16 slots, so read load can refuse them with a retryable `capacity_exceeded`.
- With OIDC, protected-resource metadata is served at the root path and at the exact path the challenge advertises; other suffixes are 404. Every 401 carries `WWW-Authenticate: Bearer`, with `error="invalid_token"` when a presented bearer was refused.
- Without OIDC the server refuses any request carrying `Origin` with 403, in its own layer, because the SDK treats an empty origin list as unrestricted.
- Resources expose the graph list, each graph's schema and branches, and the agent skill's references as best-practices documents.

## Rejected

- **Keep the npm package as a stdio proxy:** a second package to release for no capability the server lacks; web clients cannot use stdio.
- **Per-graph MCP endpoints:** one endpoint lets a client discover graphs without reconfiguration and keeps one OAuth resource per server.
- **One tool per stored query now:** without per-query `invoke_query` scope every actor would see every stored query as a tool.
- **Boot opt-ins for ad-hoc reads and writes:** they restrict only the MCP route of an access the credential holds over HTTP, and they hide tools a caller may use.
- **A hand-rolled JSON-RPC layer:** the SDK owns framing, negotiation and cancellation.
