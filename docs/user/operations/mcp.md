# MCP Surface (`POST /graphs/{id}/mcp`)

Every graph a cluster server serves is also exposed as a [Model Context
Protocol](https://modelcontextprotocol.io) server, so an MCP-capable agent
(Claude Code/Desktop, Cursor, the OpenAI Responses `mcp` tool, …) can operate the
graph directly — no bespoke client, no `.gq` source on the wire. The MCP surface
adds **no new capability or business logic**: every tool delegates to the same
engine/handler path the REST routes use and is gated by the same Cedar policy.

Available since **v0.8.0**. It is served automatically by `omnigraph-server
--cluster …` — there is no separate flag to enable it.

## Transport

One endpoint per served graph:

```
POST /graphs/{id}/mcp
```

It is a **stateless Streamable-HTTP** MCP transport: each call returns a single
`application/json` JSON-RPC response — no SSE, no session id. `GET`/`DELETE`
return `405` (`Allow: POST`).

```bash
curl -sS https://graph.example.com/graphs/sales/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
       "params":{"protocolVersion":"2025-11-25","capabilities":{},
                 "clientInfo":{"name":"my-agent","version":"0"}}}'
```

A typical MCP client is configured with just the URL and the bearer token; it
then drives `initialize` → `tools/list` / `resources/list` → `tools/call` /
`resources/read` itself.

Every tool operates on the single graph in the URL path, so the graph id never
appears in tool arguments or output.

## Tools

### Built-in tools

| Tool | Action | Notes |
|---|---|---|
| `graph_health` | — | liveness/identity probe; always visible |
| `graph_query` | `read` | run an ad-hoc read-only GQ query (mutations rejected) |
| `graph_snapshot` | `read` | manifest version + per-table metadata of a branch |
| `schema_get` | `read` | the graph's `.pg` source |
| `branch_list` | `read` | branch names |
| `commit_list` / `commit_get` | `read` | commit history |
| `graph_mutate` | `change` | ad-hoc GQ insert/update/delete against a branch |
| `graph_load` | `change` (+ `branch_create` with `from`) | bulk NDJSON load |
| `branch_create` / `branch_delete` / `branch_merge` | `branch_create` / `branch_delete` / `branch_merge` | branch ops |
| `schema_apply` | `schema_apply` | **disabled (409) under cluster-backed serving** — evolve via `cluster apply` + restart |

Tool names are domain-qualified `snake_case`. Read tools are annotated
`readOnly`; writers are annotated `destructive` so clients can prompt for
confirmation (annotations are advisory hints — Cedar is the enforcement
boundary).

### Stored-query tools

The graph's stored-query registry (declared in `cluster.yaml`, see
[stored queries](server.md#stored-query-catalog-get-queries)) is projected as
tools too, in one of two modes chosen automatically from the count of exposed
queries:

- **`per_query`** (fewer than 24 exposed queries) — each exposed query is its own
  tool, named by its `@mcp(tool_name: …)` (default: the query name), with a typed
  input schema. Query parameters are nested under a `params` object; `branch`
  (and, for reads, `snapshot`) sit alongside it.
- **`meta`** (24 or more) — the per-query tools collapse to a discovery + execute
  pair, `stored_query_list` (filter/inspect the catalog) and
  `stored_query_run(name, params, branch?, snapshot?)`, so a client's tool count
  stays bounded.

A stored-query tool's metadata comes from the `.gq` source (see
[query annotations](../queries/index.md#annotations)):

- **description** = `@description`, with `@instruction` folded in after a blank
  line (so the agent sees both in `tools/list`).
- **tool name** = `@mcp(tool_name: …)`, else the query name.
- **parameter docs** = each parameter's `@description`, surfaced into the input
  schema's per-property `description`.

Only **exposed** queries are reachable on the MCP surface in either mode. Set
`@mcp(expose: false)` to hide a query from `tools/list`, from
`stored_query_list`, and from `stored_query_run` (by name). This is presentation
only — the query stays HTTP/service-callable via `POST /queries/{name}` for any
caller with the `invoke_query` grant.

## Resources

| URI | Gate | Contents |
|---|---|---|
| `omnigraph://schema` | `read` | the graph's `.pg` schema source |
| `omnigraph://branches` | `read` | branch names as JSON |

## Structured output

Tool results carry **structured output**: `structuredContent` (the typed result
DTO — the same shape as the REST `ReadOutput` / `ChangeOutput` envelopes) plus a
text mirror for clients that don't parse it.

## Authorization

Authorization is identical to the REST routes — the bearer token resolves to a
server-side actor, and every tool/resource hits the same Cedar gate:

- **Calls are authoritative.** A built-in tool runs only if the actor's Cedar
  grant permits the action on the *actual* branch argument; a denial comes back
  as a tool error (`isError`), not a silent success.
- **Listing is a relaxation.** `tools/list` shows a tool if the actor could
  invoke it on *some* branch, so a tool you can call is never hidden. Under the
  canonical "protect `main`, write feature branches" policy, `graph_mutate` is
  listed for an actor who can change unprotected branches even though a write to
  `main` would be denied. An actor with no write grant at all does not see the
  write tools.
- **Stored queries** sit behind one coarse `invoke_query` gate (a stored
  *mutation* is additionally `change`-gated). For a caller lacking `invoke_query`,
  a stored tool masks as an **unknown tool**, so the catalog can't be probed.

## Host & Origin policy

The transport derives a fail-closed DNS-rebinding / browser posture from the bind
address at startup:

- **Loopback bind** (`127.0.0.1` or `::1`) — the `Host` allow-list is the full
  loopback set `127.0.0.1`, `::1`, `localhost` (so either IP stack works
  regardless of which one the server bound), and `Origin` is unchecked (local-dev
  convenience).
- **Non-loopback bind** — the `Host` allow-list is the configured public host(s)
  (or unrestricted if none are configured — rely on the bearer token there), and
  any *present* browser `Origin` is rejected (`403`) unless it is in the
  configured browser-origins list. A non-browser MCP client sends no `Origin` and
  passes.

A disallowed `Host` is `403`.

## Protocol version

The `MCP-Protocol-Version` header is validated on follow-up requests (an
unsupported version → `400`). The `initialize` request is exempt by design — it
negotiates the version in its JSON-RPC body (`protocolVersion`), so the header is
not checked there.

## Not supported

MCP prompts, elicitation, sampling, tasks, and `tools/list_changed` /
`resources/list_changed` subscriptions are not implemented — the surface is
`initialize` + `tools` + `resources` over the stateless POST transport.
