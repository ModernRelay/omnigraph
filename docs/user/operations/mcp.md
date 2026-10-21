# MCP

`omnigraph-server` serves the Model Context Protocol at `/mcp`, so Claude,
ChatGPT, Cursor and other MCP clients can work with a graph with a URL and the
caller's own credential. It is the only OmniGraph MCP implementation; it
replaces the `@modernrelay/omnigraph-mcp` npm package.

Every tool calls the same handler as its HTTP route, under the same actor and
the same [policy](policy.md) action. A tool appearing in the list is not
permission to use it: a call the policy refuses returns the route's refusal.

## Enable it

`/mcp` is mounted whenever the server has a bearer credential source: static
tokens, signed data tokens, or an
[OIDC resource identity](server.md#oidc-resource-identities-and-mcp)
(`--oidc-identity-trust`). A server without credentials has no MCP.

| Credentials | `/mcp` | OAuth discovery and challenge |
|---|---|---|
| Static or signed data tokens | mounted | none |
| OIDC resource identity, alone or with tokens | mounted | served |

With static or signed tokens, configure the client with the URL and an
`Authorization: Bearer <token>` header; Claude Code and Cursor support this
directly. With OIDC, the resource identifier in the trust file is the URL users
enter in their MCP client, for example `https://graphs.example.com/mcp`, and the
client signs in through OAuth.

The endpoint is stateless Streamable HTTP built on the official Rust MCP SDK:
each request is a `POST` that carries its own bearer credential and gets a JSON
response. There is no MCP session, and a successful `initialize` never admits a
later unauthenticated request.

### Sign-in and discovery

A request without a valid credential is refused with 401 before its body is
read. With OIDC, the response's challenge names the protected-resource metadata:

```text
WWW-Authenticate: Bearer resource_metadata="https://graphs.example.com/.well-known/oauth-protected-resource/mcp"
```

When a bearer credential was presented and refused (expired, wrong audience,
unknown subject), the challenge also carries `error="invalid_token"`, which tells
a client to refresh or sign in again.

The server answers the metadata both at `/.well-known/oauth-protected-resource`
and at the advertised path: the well-known prefix followed by the resource
identifier's path (`/mcp` above). Any other path below the prefix is 404. No
proxy rewrite is needed.

### Host and Origin

The SDK refuses a request whose `Host` is not allowed with 403. Allowed hosts
are `localhost`, `127.0.0.1`, `::1`, the OIDC resource identifier's authority,
and every `--mcp-allowed-host`. A server reached by another name, for example
behind a load balancer without OIDC, needs that name listed.

With OIDC, a browser `Origin` must be the resource identifier's origin. Without
OIDC there is no origin to allow, so a request carrying any `Origin` is refused
with 403. Native clients send no `Origin`.

## Settings

| Flag | Environment | Default | Effect |
|---|---|---|---|
| `--mcp-allow-adhoc` | `OMNIGRAPH_MCP_ALLOW_ADHOC=1` | off | Lists and admits the `read` tool |
| `--mcp-allow-writes` | `OMNIGRAPH_MCP_ALLOW_WRITES=1` | off | Lists and admits the write tools |
| `--mcp-allowed-host <host[:port]>`, repeatable | `OMNIGRAPH_MCP_ALLOWED_HOSTS=a,b` | none | Adds allowed `Host` values |

Settings are read once at boot. They enable tools; they grant nothing. A tool
that is off is absent from `tools/list`, and a call to it is refused as an
unknown tool. The opt-ins stay off even when a policy is applied: turn them on
for a deployment whose policy was written with an LLM caller in mind.

## Tools

### Read tools

| Tool | Arguments | Runs | Policy action |
|---|---|---|---|
| `health` | none | `GET /healthz` | none |
| `graphs` | none | `GET /graphs/discovery` for an identity credential; otherwise `GET /graphs`, narrowed to ids and names | identity, or `graph_list` |
| `schema` | `graph` | `GET /graphs/{graph}/schema` | `read` |
| `queries` | `graph` | `GET /graphs/{graph}/queries`, stored mutations removed | `read` |
| `query` | `graph`, `name`, optional `params`, `branch` | `POST /graphs/{graph}/queries/{name}` as a read | `invoke_query`, then `read` |
| `read` (opt-in) | `graph`, `query`, optional `name`, `params`, `branch`, `snapshot` | `POST /graphs/{graph}/query` | `read`; `change` for `explain load csv` |
| `snapshot` | `graph`, optional `branch` | `GET /graphs/{graph}/snapshot` | `read` |
| `access` | `graph`, optional `branch`, `target_branch` | `GET /graphs/{graph}/access` | none |
| `branches_list` | `graph` | `GET /graphs/{graph}/branches` | `read` |
| `commits_list` | `graph`, optional `branch`, `limit`, `page_token` | `GET /graphs/{graph}/commits` | `read` |
| `commits_get` | `graph`, `commit_id` | `GET /graphs/{graph}/commits/{commit_id}` | `read` |
| `commits_changes` | `graph`, `commit_id`, optional `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph}/commits/{commit_id}/changes` | `read` |
| `changes_poll` | `graph`, optional `branch`, `cursor`, `start`, `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph}/changes` | `read` |

`graph_list` is a server-scoped action, and a server without an applied cluster
policy denies every server-scoped action. So with a static or other non-identity
token, `graphs` returns 403 until the cluster policy permits `graph_list`; the
other tools still work with a graph id the user supplies.

The server's `initialize` instructions tell a model to call `graphs`, then
`schema`, then a stored read with `query` or, when enabled, its own GQ read with
`read`.

- `queries` lists each stored read with its typed parameters (`name`, `kind`,
  `nullable`, plus `item_kind` for lists and `vector_dim` for vectors).
- `query` refuses stored mutations.
- `read` takes GQ source with typed parameter declarations, the same body as
  `POST /query`. It also answers `branch list`, `branch diff`,
  `branch merge-base`, `branch merge … dry run` and `explain query …`.
  `explain load csv …` requires `change` on its target branch before probing
  source metadata; it reads no payload byte and publishes nothing. It accepts
  `branch`, while refusing `snapshot`, `name` and `params`.
  The tool refuses `insert`, `update`, `delete` and branch writes with the route's 400.

### Write tools (opt-in)

| Tool | Arguments | Runs | Policy action |
|---|---|---|---|
| `mutate` | `graph`, `query`, optional `name`, `params`, `branch`, `if_graph_commit` | `POST /graphs/{graph}/mutate`, or `/mutate/if-graph-commit` with `if_graph_commit` | `change`; a branch statement takes its branch action |
| `load` | `graph`, `mode`, `data`, optional `branch`, `from` | `POST /graphs/{graph}/load` | `change`, plus `branch_create` when `from` creates the branch |
| `branches_create` | `graph`, `name`, optional `from` | `POST /graphs/{graph}/branches` | `branch_create` |
| `branches_delete` | `graph`, `name` | `DELETE /graphs/{graph}/branches/{name}` | `branch_delete` |
| `branches_merge` | `graph`, `source`, optional `target` | `POST /graphs/{graph}/branches/merge` | `branch_merge` |

Write tools follow one contract:

- **Conditional writes.** Pass a read's `graph_commit_id` to `mutate` as
  `if_graph_commit`. If the branch has moved, the call fails with 412 and nothing
  is written.
- **No retries.** The server never replays a tool call.
- **A lost response is an unknown outcome.** A write runs to completion even when
  the caller stops waiting. A write still running after 30 seconds, or one whose
  completion the server cannot confirm, returns `isError` with
  `code: outcome_unknown`. A write the client cancelled, timed out or lost the
  connection for returns nothing, and means the same. Before replaying it, read
  the target with `read` or `snapshot` and compare `graph_commit_id`, or list the
  branch with `commits_list`; replay a `mutate` with `if_graph_commit`, so a write
  that did land fails with 412 instead of applying twice. The 30 second wait is
  below the MCP TypeScript SDK's default 60 second request timeout; a client with
  a shorter timeout loses the answer.
- **Size.** The whole call, including `load` data, fits in the 64 KiB request.
  Use `POST /load/ndjson` for larger loads.

Schema changes and cluster deployments are not MCP tools; operators run them with
`omnigraph cluster plan` and `apply`.

## Resources

| URI | Content |
|---|---|
| `omnigraph://graphs` | The `graphs` result, JSON, under the same rule |
| `omnigraph://graphs/{graph}/schema` (template) | The graph's `.pg` source |
| `omnigraph://graphs/{graph}/branches` (template) | The `branches_list` result, JSON |
| `omnigraph://best-practices/index` | Which reference to read for which task, Markdown |
| `omnigraph://best-practices/{topic}` for `queries`, `data`, `schema`, `search`, `changes` | The agent skill's reference of that name, Markdown |

The best-practices references are the `omnigraph` agent skill's own files,
compiled into the server, so they always match its version.

A resource read runs the same handler and policy action as its tool. A refusal
is a JSON-RPC error whose `data` carries the refused tool call's content below;
an unknown resource or graph is error `-32002`.

## Results and errors

A successful call returns the route's JSON response body as structured content,
plus a text copy for older clients. `health`, `graphs`, `schema`, `queries`,
`access` and `branches_list` declare an output schema.

A refused call returns `isError: true` with
`{"status": <HTTP status>, "error": <error body>}`: the error body every HTTP
route answers, with its `code`, `disposition` and details, and the status the
same failure answers over HTTP, for example 403 for a policy denial, 404 for an
unknown or hidden graph, 503 for a graph that is not available. The MCP layer's
own refusals use the same body: `unauthorized` without a verified credential,
`too_many_requests` beyond the concurrent limit, and `cancelled`,
`deadline_exceeded` or `result_too_large`, which only MCP sends and which carry
no `status`. A write that may have happened, one whose call was cancelled or
passed its wait included, answers `outcome_unknown` with `effects: unknown`.

## Limits

| Limit | Value |
|---|---|
| Request body | 64 KiB |
| Complete tool result, including the text copy | 1 MiB |
| Wait for one read | 30 seconds |
| Wait for one write | 30 seconds; the write continues past it |
| Concurrent tool executions | 16 |

A cancelled or expired call stops the caller's wait. The execution keeps its
input reservation and slot until it finishes, and there is no later result
lookup. See [admission and shutdown](../deployment.md).

## Logs

Verbose MCP SDK logs contain query arguments and results, so the server keeps
them out of its log; see [Logs](server.md#logs).
