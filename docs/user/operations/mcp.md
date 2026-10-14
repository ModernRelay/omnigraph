# MCP

`omnigraph-server` serves the Model Context Protocol at `/mcp`, so Claude,
ChatGPT, Cursor and other MCP clients can read a graph with a URL and the
caller's own credential. It is the only OmniGraph MCP implementation.

Every tool calls the same handler as its HTTP route, under the same actor and
the same [policy](policy.md) action. A tool appearing in the list is not
permission to use it: a call the policy refuses returns the route's refusal.

## Enable it

`/mcp` is mounted when the server is started with an
[OIDC resource identity](server.md#oidc-resource-identities-and-mcp)
(`--oidc-identity-trust`). The resource identifier in that file is the URL users
enter in their MCP client, for example `https://graphs.example.com/mcp`.

The endpoint is stateless Streamable HTTP built on the official Rust MCP SDK:
each request is a `POST` that carries its own bearer credential and gets a JSON
response. There is no MCP session, and a successful `initialize` never admits a
later unauthenticated request.

### Sign-in and discovery

A request without a valid credential is refused with 401 before its body is
read. The response's challenge names the protected-resource metadata:

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
are `localhost`, `127.0.0.1`, `::1` and the resource identifier's authority.
A browser `Origin` must be the resource identifier's origin; native clients send
no `Origin`.

## Settings

| Flag | Environment | Default | Effect |
|---|---|---|---|
| `--mcp-allow-adhoc` | `OMNIGRAPH_MCP_ALLOW_ADHOC=1` | off | Lists and admits the `read` tool |

The setting is read once at boot. It enables a tool; it grants nothing. When it
is off, `read` is absent from `tools/list` and a call to it is refused as an
unknown tool.

## Tools

| Tool | Arguments | Runs | Policy action |
|---|---|---|---|
| `graphs` | none | `GET /graphs/discovery` | any authenticated identity |
| `schema` | `graph` | `GET /graphs/{graph}/schema` | `read` |
| `queries` | `graph` | `GET /graphs/{graph}/queries`, stored mutations removed | `read` |
| `query` | `graph`, `name`, optional `params`, `branch` | `POST /graphs/{graph}/queries/{name}` as a read | `invoke_query`, then `read` |
| `read` (opt-in) | `graph`, `query`, optional `name`, `params`, `branch`, `snapshot` | `POST /graphs/{graph}/query` | `read` |

All tools are read-only. The server's `initialize` instructions tell a model to
call `graphs`, then `schema`, then a stored read with `query` or, when enabled,
its own GQ read with `read`.

- `queries` lists each stored read with its typed parameters (`name`, `kind`,
  `nullable`, plus `item_kind` for lists and `vector_dim` for vectors).
- `query` refuses stored mutations.
- `read` takes GQ source with typed parameter declarations, the same body as
  `POST /query`. It also answers `branch list`, `branch diff`,
  `branch merge-base`, `branch merge … dry run` and `explain query …`. It refuses
  `insert`, `update`, `delete` and branch writes with the route's 400.

## Results and errors

A successful call returns the route's JSON response body as structured content,
plus a text copy for older clients. `graphs`, `schema` and `queries` declare an
output schema.

A refused call returns `isError: true` with one of two shapes:

- `{"status": <HTTP status>, "error": <the route's error body>}` when the route
  refused it, for example 403 for a policy denial, 404 for an unknown or hidden
  graph, 503 for a graph that is not available;
- `{"code": "...", "message": "..."}` when the MCP layer refused it:
  `capacity_exceeded`, `cancelled`, `deadline_exceeded` or `result_too_large`.

## Limits

| Limit | Value |
|---|---|
| Request body | 64 KiB |
| Complete tool result, including the text copy | 1 MiB |
| Wait for one read | 30 seconds |
| Concurrent tool executions | 16 |

A cancelled or expired call stops the caller's wait. The execution keeps its
input reservation and slot until it finishes, and there is no later result
lookup. See [admission and shutdown](../deployment.md).

## Logs

Verbose MCP SDK logs contain query arguments and results, so the server keeps
them out of its log; see [Logs](server.md#logs).
