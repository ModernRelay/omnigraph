# MCP Surface (`POST /graphs/{id}/mcp`)

Since **v0.8.0** every graph a `--cluster` server serves is also an
[MCP](https://modelcontextprotocol.io) server, so an MCP agent (Claude
Code/Desktop, Cursor, the OpenAI Responses `mcp` tool) can operate the graph
directly — no bespoke client, no `.gq` source on the wire. It is **served
automatically** by `omnigraph-server --cluster …`; there is no flag to enable
it. The surface adds **no new capability**: every tool delegates to the same
engine/handler path the REST routes use and is gated by the same Cedar policy.

## Transport

One endpoint per served graph: `POST /graphs/{id}/mcp`. Stateless
Streamable-HTTP — each call returns a single `application/json` JSON-RPC
response (no SSE, no session id). `GET`/`DELETE` → `405`. Every tool operates on
the single graph in the URL path, so the graph id never appears in tool
arguments.

## Connecting an agent

Configure the client with the URL and the **same bearer token** you use for
REST:

```bash
# Claude Code
claude mcp add og --transport http \
  https://graph.example.com/graphs/<id>/mcp \
  --header "Authorization: Bearer $TOKEN"
```

```bash
# Raw probe
curl -sS https://graph.example.com/graphs/<id>/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
       "params":{"protocolVersion":"2025-11-25","capabilities":{},
                 "clientInfo":{"name":"probe","version":"0"}}}'
```

The client then drives `initialize` → `tools/list` / `resources/list` →
`tools/call` / `resources/read`.

## Tools

**Built-ins** (one per operation, delegating to the REST handlers): `graph_query`,
`graph_mutate`, `graph_load`, `graph_snapshot`, `schema_get`, `branch_list`,
`branch_create` / `branch_delete` / `branch_merge`, `commit_list` / `commit_get`,
`schema_apply` (returns 409 under cluster serving — evolve via `cluster apply`
and restart), and a `graph_health` liveness probe.

**Stored-query tools** — the graph's registry, projected in one of two modes
chosen automatically from the exposed-query count:

- **`per_query`** (fewer than 24 exposed) — each exposed query is its own tool,
  named by `@mcp(tool_name: …)` (default: the query name), with a typed input
  schema (params nested under `params`; `branch`, and for reads `snapshot`,
  alongside).
- **`meta`** (24+) — the per-query tools collapse to a `stored_query_list` +
  `stored_query_run(name, params, branch?, snapshot?)` pair, so the client's
  tool count stays bounded.

## Authoring stored-query tools (`.gq`)

A stored-query tool's metadata comes entirely from the `.gq` source (see
[`queries.md`](queries.md) and [`stored-queries.md`](stored-queries.md)):

```gq
query find_user(@description("the user's slug") $slug: String)
  @description("Look up a user by slug.")
  @instruction("Use for an exact slug; for fuzzy names use search_users.")
  @mcp(tool_name: "lookup_user", expose: true)
{ match { $u: User { slug: $slug } } return { $u.name, $u.email } }
```

- **description** = `@description`, with `@instruction` folded in after a blank
  line (so the agent reads the how/when-to-use guidance in `tools/list`).
- **tool name** = `@mcp(tool_name: …)`, else the query name. Validated at load:
  `[A-Za-z0-9_-]`, ≤64 chars, unique, and may not shadow a built-in **or** the
  `stored_query_list`/`stored_query_run` meta names — a violation **fails the
  server boot** loudly, never a silently-broken catalog.
- **parameter docs** = each parameter's leading `@description`, surfaced into the
  input-schema property `description`.
- **visibility** = `@mcp(expose: false)` hides a query from `tools/list`,
  `stored_query_list`, and `stored_query_run` (by name); default is exposed.

## Resources

| URI | Contents |
|-----|----------|
| `omnigraph://schema` | the graph's `.pg` schema source |
| `omnigraph://branches` | branch names (JSON) |

Tool results carry **structured output** (`structuredContent`, the same typed
envelopes as the REST routes) plus a text mirror.

## Authorization — two axes, do not conflate

Auth is identical to the REST routes — the bearer resolves to a server-side
actor and every tool/resource hits the same Cedar gate.

- **Calls are authoritative.** A tool runs only if Cedar permits the action on
  the *actual* branch argument; a denial is a tool error (`isError`), not a
  silent success.
- **Listing is a relaxation of the call gate.** `tools/list` shows a tool if the
  actor could invoke it on *some* branch, so a callable tool is never hidden
  (under "protect `main`, write feature branches", `graph_mutate` is listed for a
  branch-writer even though writing `main` is denied). Fixed-scope tools whose
  call is branchless (`schema_get`/`branch_list`/`commit_get`, and the schema and
  branches resources) are gated on that exact branchless read, so a tool and its
  resource twin are consistent.
- **Stored-query discovery + invocation share the `invoke_query` gate** — the
  same authority as REST `GET /queries` and `POST /queries/{name}`. A caller
  without `invoke_query` gets a stored tool masked as an **unknown tool** (the
  catalog can't be probed). Stored mutations are additionally `change`-gated.

**`expose` is presentation, not authorization.** `@mcp(expose: false)` only keeps
a query off the agent tool surface; it stays HTTP/service-callable by name for
any caller with `invoke_query`. Who *may* call a query is governed by Cedar
(`invoke_query` + the inner `read`/`change`), never by `expose`.

## Host / Origin and protocol version

Fail-closed posture derived from the server bind at startup:

- **Loopback bind** (`127.0.0.1` or `::1`) — `Host` allow-list is the full
  loopback set (`127.0.0.1`, `::1`, `localhost`), Origin unchecked (local dev).
- **Non-loopback bind** — `Host` restricted to the configured public host(s) (or
  unrestricted if none — rely on the bearer); any *present* browser `Origin` is
  rejected unless allow-listed. Non-browser MCP clients send no `Origin` and
  pass.

A disallowed `Host` → `403`. The `MCP-Protocol-Version` header is validated on
follow-up requests (unsupported → `400`); `initialize` is exempt (it negotiates
the version in its body).

## Not supported

MCP prompts, elicitation, sampling, tasks, and `*_list_changed` subscriptions —
the surface is `initialize` + `tools` + `resources` over the stateless POST
transport.

Full user-facing reference: `docs/user/operations/mcp.md`.
