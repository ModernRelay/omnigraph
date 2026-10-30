# MCP

`omnigraph-server` serves the Model Context Protocol at `/mcp`, so Claude,
ChatGPT, Cursor and other MCP clients can work with a graph with a URL and the
caller's own credential. MCP shares the server's listener and lifecycle; no
separate service is needed.

Every tool calls the same handler as its HTTP route, under the same actor and
the same [policy](policy.md) action. A tool appearing in the list is not
permission to use it: a call the policy refuses returns the route's refusal.

## Enable it

`/mcp` is mounted whenever the server has a bearer credential source: static
tokens, signed data tokens, or an
[OIDC resource identity](server.md#oidc-resource-identities-and-mcp)
(`--oidc-identity-trust`). A server without credentials has no MCP.

| Credentials | `/mcp` | OAuth discovery | Challenge on a 401 |
|---|---|---|---|
| Static or signed data tokens | mounted | none | `Bearer` |
| OIDC resource identity, alone or with tokens | mounted | served | `Bearer resource_metadata="<url>"` |

With static or signed tokens, configure the client with the URL and an
`Authorization: Bearer <token>` header; Claude Code and Cursor support this
directly. With OIDC, the resource identifier in the trust file is the URL users
enter in their MCP client, for example `https://graphs.example.com/mcp`, and the
client signs in through OAuth. With an identifier naming the service root,
clients that accept a path prefix can also sign in from other routes; see
[Sign-in and discovery](#sign-in-and-discovery). A client that can only start a
command uses [`omnigraph mcp`](#connect-a-stdio-agent).

The endpoint is stateless Streamable HTTP built on the official Rust MCP SDK:
each request is a `POST` that carries its own bearer credential and gets a JSON
response. There is no MCP session, and a successful `initialize` never admits a
later unauthenticated request.

### Upgrade tool schemas

After upgrading the server, reconnect clients and refresh cached tool schemas
with `tools/list`. The `access` tool now returns `actor_id` instead of `actor`;
clients with fixed response types must update those types before reconnecting.
Its output matches the [HTTP access report](policy.md#learn-your-access).
`queries` now lists stored mutations too, marked `mutation: true`, and
`mutate` no longer requires `query`.

MCP does not use the `Omnigraph-Http-Api` header, so an old MCP client is not
automatically refused.
MCP protocol negotiation is unchanged, and the old field has no alias.

### Sign-in and discovery

A request without a valid credential is refused with 401 before its body is
read, with the same [challenge](server.md#authentication) as every other route.
With OIDC, the challenge names the protected-resource metadata:

```text
WWW-Authenticate: Bearer resource_metadata="https://graphs.example.com/.well-known/oauth-protected-resource/mcp"
```

When a bearer credential was presented and refused (bad signature, expired,
wrong audience or organization), the challenge also carries
`error="invalid_token"`, which tells a client to refresh or sign in again.
Without OIDC the challenge is `Bearer`, which advertises no sign-in flow.

Two refusals of a valid token carry no challenge, because signing in again
issues a token for the same person and cannot help:

- the admission file names no principal for the token's subject: 403
  `not_admitted`. The server's operator must admit the identity;
- the admission snapshot has expired, for example during a publisher outage:
  503 `service_unavailable` with `Retry-After`. Access returns with the next
  publication.

A token's `client_id` is kept beside the actor, which stays one `principal:<id>`
for every client a person uses. The [access route](policy.md#learn-your-access)
and the `policy decision` log line name it; commits do not yet. It is opaque
client-chosen text: render it escaped.

The server has one resource identifier, so every route's challenge names the
same metadata. RFC 9728 §3.3 has a client that followed a challenge require the
metadata's `resource` to equal the URL it requested, so a strict client uses the
metadata only on the URL the identifier names. Some clients accept an identifier
whose path is a prefix of that URL, as the MCP TypeScript SDK does
(`checkResourceAllowed`). For them, an identifier naming the service root, such
as `https://graphs.example.com`, covers every route, and one naming
`https://graphs.example.com/mcp` covers only this endpoint. Name the service root
when clients that accept a prefix should discover sign-in from any route.

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

## Connect a stdio agent

An agent that starts its MCP servers as child processes, such as Claude Code or
Codex in a terminal, connects to an existing server through the CLI:

```json
{"command": "omnigraph", "args": ["mcp", "--target", "staging"]}
```

`omnigraph mcp --target NAME [--graph G] [--write]` relays each JSON-RPC message
on stdin to a server or managed target's `/mcp`, one `POST` each, and writes each answer
as one line on stdout; diagnostics go to stderr. Tools, their logic and their
refusals stay the server's.

- **Credential.** Requests use the same credential acquisition as served graph
  commands. Server targets use their `auth`; managed targets exchange the
  pinned profile's credential for an identity bound to that cluster ([authentication providers](../cli/targets.md#authentication-providers));
  it never appears in the agent's configuration or output. A target whose
  `auth` is `provider: anonymous` sends none. On server targets, `--as ACTOR` sends
  `Omnigraph-If-Actor` with every request, so another actor's token is refused
  with `412 actor_mismatch`. Managed targets refuse `--as` before any I/O.
- **One graph.** `--graph`, else the target's `default_graph`; without either the
  command refuses before any request. A tool call that omits `graph` gets it, a
  call or resource URI naming another graph is refused with a JSON-RPC error, and
  `graphs` and `omnigraph://graphs` list only this graph. Run one bridge per
  graph.
- **Writes need `--write`.** Without it, the bridge lists and admits only the
  tools the server annotates `readOnlyHint: true`, and refuses a call to any
  other before sending it. The applied policy still decides every call.
- **Existing endpoints only.** A server or managed target must name its existing
  endpoint. Direct graph, root and catalogue targets refuse with
  `target_kind_unsupported` before any I/O. The bridge opens no storage and never
  starts a server or enables its MCP route.

The bridge relays up to 16 requests at once, the server's own limit, and
answers them as they complete. It applies `notifications/cancelled` itself: the
cancelled request's exchange is dropped and it gets no answer, unless its answer
was already written. A cancelled write may still have landed: read the target
before running it again. It never sends a request again:

| Failure | JSON-RPC error `data` |
|---|---|
| The server refused the request over HTTP (401, 412, 429, …) | `status` and the server's error body as `error`. A 401 also carries the server's `WWW-Authenticate` challenge as `www_authenticate` when it sends one. Managed credentials reflected in error bodies or challenges are redacted |
| Nothing was sent: the server could not be reached, or speaks another HTTP contract | `error.code` `service_unavailable`, or `api_contract_mismatch`, with `effects: none` |
| The answer was lost, or took longer than 45 seconds | `outcome_unknown` with `effects: unknown` for a call to a tool that may write; otherwise `service_unavailable`, `deadline_exceeded` or `result_too_large` with `effects: none` |

These errors carry code `-32000`; refusals by the graph binding or write consent
carry `-32602`, with a `bad_request` body. After a 401 the bridge reads the
credential from its source again once, for later requests: a token rewritten
in a `file` source, or returned anew by an `exec` helper, reaches them without
a restart. An `env` source is fixed when the bridge starts, and a token
replaced with `omnigraph credentials set` belongs to a new credential
generation, so both take a restart; the error says how to replace the token. A
server that stays unreachable fails each request; the bridge keeps running
until stdin closes. Managed resource credentials renew through the selected
profile's normal cache before expiry. A refused managed credential is removed
only if it is still the cached one, and renewed for later messages; the refused
message is never replayed. If an expiring credential runs out before a message
is sent, including during discovery, the bridge renews it once before sending.
If the replacement also expires, the message fails without being sent. Renewal
never starts an interactive login.

An expiring managed profile's `exec` credential is also renewed before later
requests. The verified organization and principal must remain the same for the
life of the bridge; a changed identity requires restarting it. Organization
service permissions remain current provider facts. Managed error messages,
including JSON-RPC tool errors, never expose a reflected resource credential.

## Settings

| Flag | Environment | Default | Effect |
|---|---|---|---|
| `--mcp-allowed-host <host[:port]>`, repeatable | `OMNIGRAPH_MCP_ALLOWED_HOSTS=a,b` | none | Adds allowed `Host` values |

Settings are read once at boot. `/mcp` lists every tool below, and each call
runs under the same policy action as its HTTP route, so a caller can do over
MCP exactly what its credential may do over HTTP. To restrict what an agent
may do, narrow the policy for the agent's credential.

The removed `--mcp-allow-adhoc` and `--mcp-allow-writes` flags make
`omnigraph-server` refuse to start; leftover `OMNIGRAPH_MCP_ALLOW_*` variables
are ignored, so such a deployment lists every tool. A token with no applied
policy still only reads: `read` answers, and a write tool returns 403.

## Tools

### Read tools

| Tool | Arguments | Runs | Policy action |
|---|---|---|---|
| `health` | none | `GET /healthz` | none |
| `graphs` | none | `GET /graphs/discovery` for an identity credential; otherwise `GET /graphs`, narrowed to ids and names | identity, or `graph_list` |
| `schema` | `graph` | `GET /graphs/{graph_id}/schema` | `read` |
| `queries` | `graph` | `GET /graphs/{graph_id}/queries` | `read` |
| `query` | `graph`, `name`, optional `params`, `branch` | `POST /graphs/{graph_id}/queries/{query_name}` as a read | `invoke_query`, then `read` |
| `read` | `graph`, `query`, optional `name`, `params`, `branch`, `snapshot` | `POST /graphs/{graph_id}/query` | `read`; `change` for `explain load csv` |
| `snapshot` | `graph`, optional `branch` | `GET /graphs/{graph_id}/snapshot` | `read` |
| `access` | `graph`, optional `branch`, `target_branch` | `GET /graphs/{graph_id}/access` | none |
| `branches_list` | `graph`, optional `detailed` | `GET /graphs/{graph_id}/branches` | `read`; with `detailed`, details only for branches the caller may `read` |
| `branches_diff` | `graph`, `source`, optional `target`, `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph_id}/branches/diff` | `read` on both branches |
| `branches_merge_base` | `graph`, `source`, optional `target` | `GET /graphs/{graph_id}/branches/merge-base` | `read` on both branches |
| `commits_list` | `graph`, optional `branch`, `listing`, `limit`, `page_token` | `GET /graphs/{graph_id}/commits` | `read` |
| `commits_get` | `graph`, `commit_id` | `GET /graphs/{graph_id}/commits/{commit_id}` | `read` |
| `commits_changes` | `graph`, `commit_id`, optional `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph_id}/commits/{commit_id}/changes` | `read` |
| `diff` | `graph`, `from`, `to`, optional `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph_id}/diff` | `read` on the branch each side resolves to |
| `changes_poll` | `graph`, optional `branch`, `cursor`, `start`, `kind`, `type`, `op`, `page_token`, `limit` | `GET /graphs/{graph_id}/changes` | `read` |
| `blobs_stat` | `graph`, `entity`, `type`, `id`, `property`, optional `branch`, `snapshot` | `HEAD /graphs/{graph_id}/blob` | `read` |

`graph_list` is a server-scoped action, and a server without an applied cluster
policy denies every server-scoped action. So with a static or other non-identity
token, `graphs` returns 403 until the cluster policy permits `graph_list`; the
other tools still work with a graph id the user supplies.

The server's `initialize` instructions tell a model to call `graphs`, then
`schema`, then a stored read with `query` or its own GQ read with `read`.

- `queries` lists every stored query with its typed parameters (`name`,
  `kind`, `nullable`, plus `item_kind` for lists and `vector_dim` for
  vectors). An entry with `mutation: true` is a stored mutation, which
  `mutate` runs.
- `query` refuses stored mutations.
- `commits_list` takes `listing`: `history` (the default), `unpublished` for
  proposals written `as commit` that await `commits_publish`, or `unattached`
  for snapshots written `as unattached branch`. Only `history` pages.
- `diff`, `branches_diff` and `commits_changes` page as their routes do:
  continue with the returned `next_page_token` as `page_token`, keeping the
  other arguments. A `diff` side the caller cannot read answers the 404 of a
  side that names nothing.
- `blobs_stat` answers what `HEAD /blob` answers in its headers, as the body
  `omnigraph blob stat --json` prints: the `selector`, `kind` (`managed`, with
  `size` and `etag`, or `external`, with `uri`) and the resolved `target`. It
  reads no payload byte. The route's conditional headers are not arguments.
- `read` takes GQ source with typed parameter declarations, the same body as
  `POST /query`. It also answers `branch list`, `branch diff`,
  `branch merge-base`, `branch merge … dry run` and `explain query …`.
  `explain load csv …` requires `change` on its target branch before probing
  source metadata; it reads no payload byte and publishes nothing. It accepts
  `branch`, while refusing `snapshot`, `name` and `params`.
  The tool refuses `insert`, `update`, `delete` and branch writes with the route's 400.

### Write tools

| Tool | Arguments | Runs | Policy action |
|---|---|---|---|
| `mutate` | `graph`, `query`, optional `name`, `params`, `branch`, `if_graph_commit` | `POST /graphs/{graph_id}/mutate`, or `/mutate/if-graph-commit` with `if_graph_commit` | `change`; a branch statement takes its branch action |
| `mutate`, stored | `graph`, `name`, optional `params`, `branch`, `if_graph_commit` | `POST /graphs/{graph_id}/queries/{query_name}`, or `/queries/{query_name}/if-graph-commit` with `if_graph_commit` | `invoke_query`, then `change` |
| `load` | `graph`, `mode`, `data`, optional `branch`, `from` | `POST /graphs/{graph_id}/load` | `change`, plus `branch_create` when `from` creates the branch |
| `blobs_clear` | `graph`, `entity`, `type`, `id`, `property`, optional `branch`, `if_match` | `DELETE /graphs/{graph_id}/blob` | `change` |
| `branches_create` | `graph`, `name`, optional `from`, `from_commit` | `POST /graphs/{graph_id}/branches` | `branch_create` |
| `branches_delete` | `graph`, `name` | `DELETE /graphs/{graph_id}/branches/{branch_name}` | `branch_delete` |
| `branches_merge` | `graph`, `source`, optional `target` | `POST /graphs/{graph_id}/branches/merge` | `branch_merge` |
| `commits_publish` | `graph`, `commit_id` | `POST /graphs/{graph_id}/commits/{commit_id}/publish` | `commit_publish` on the commit's recorded target |
| `commits_drop` | `graph`, `commit_id` | `DELETE /graphs/{graph_id}/commits/{commit_id}` | `commit_drop` on the commit's recorded target |

`mutate` runs a stored mutation when it is given `name` and no `query`, as
`omnigraph mutate NAME` does; `query` still refuses one. `commits_publish` and
`commits_drop` answer a commit whose control the policy denies with the 404 of
an unknown id. `commits_drop` answers an empty object, since its route answers
204 with no body. `blobs_clear` takes the route's `If-Match` value as
`if_match`; a value that does not hold fails with 412 and
`blob_precondition_failure`.

Write tools follow one contract:

- **Conditional writes.** Pass a read's `graph_commit_id` to `mutate` as
  `if_graph_commit`, for an ad-hoc or a stored mutation. If the branch has
  moved, the call fails with 412 and nothing is written. `if_graph_commit`
  always selects the route that requires it, so an empty or malformed value is
  refused with 400 and never becomes an unconditional write.
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
`omnigraph cluster plan` and `apply`. Byte streams are not tools either: Blob
payloads (`GET` and `PUT /blob`), `export`, and the change-feed baseline, whose
NDJSON snapshot must be installed before its cursor. Use the HTTP routes or the
CLI for them.

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
plus a text copy for older clients. Three tools answer otherwise: `graphs`
narrows `GET /graphs` for a credential that is not an identity, `blobs_stat`
answers the `HEAD /blob` headers as `blob stat` JSON, and `commits_drop`
answers an empty object for the route's 204. `health`, `graphs`, `schema`,
`queries`, `access` and `branches_list` declare an output schema.

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
