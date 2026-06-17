# Stored-Query Registries

A **stored query** is a `.gq` query that the *server* loads, type-checks at startup, and exposes by name — without ever accepting ad-hoc query source from the client. It's how you publish a vetted, typed query surface to remote callers and MCP tools.

This is a server-side feature introduced in **v0.6.1**. It is distinct from CLI `aliases:` (see [`aliases.md`](aliases.md)): an alias is local client ergonomics; a stored query is a server-published, policy-gated endpoint.

## Declaring stored queries (`cluster.yaml`)

Stored queries are declared in the cluster's `cluster.yaml` — every `query <name>` in the listed `.gq` files registers:

```yaml
graphs:
  <id>:
    schema: schema.pg
    queries: queries/            # discover every `query <name>` in queries/*.gq
```

`queries` also accepts an explicit file list (`[a.gq, b.gq]`) or a fine-grained `name: { file: … }` map; an unparseable `.gq` or a duplicate query name across files fails `cluster validate`. `cluster apply` publishes them to the content-addressed catalog, and the `--cluster` server type-checks and serves every applied query. Per-query MCP presentation (`expose`, `tool_name`) is set in the `.gq` source via the `@mcp(...)` annotation (see *MCP exposure* below), so it travels content-addressed with the query — no `cluster.yaml` per-query block needed.

## CLI

```bash
omnigraph queries validate     # type-check every stored query against the live schema (offline; opens the graph; exits non-zero on drift)
omnigraph queries list         # print the addressed graph's registry: query names and typed params
```

- `validate` catches schema drift **without restarting the server** — run it after a `schema apply` or before deploying a config change. The server also runs this check at startup and **refuses to boot** on drift or on a duplicate MCP tool name.
- `validate` opens the graph (address with `--store <uri>` or a positional URI); `list` reads the addressed graph's catalog.
- `queries` is distinct from `lint` — `lint` validates a single `.gq` file you point it at; `queries validate` validates the registry the server will actually serve.

## HTTP surface

| Route | Gate | Purpose |
|-------|------|---------|
| `GET /graphs/{id}/queries` | `read` | Typed tool catalog of the served queries. Graph-wide (branch-independent; `read` authorized against `main`). |
| `POST /graphs/{id}/queries/{name}` | `invoke_query` (+ `change` for a stored mutation) | Invoke a named query. Body carries params only — **never** `.gq` source. A stored mutation cannot target a `snapshot` (`400`); a param type error is a structured `400` naming the param. |

`?branch=` / `?snapshot=` query params apply to `POST /graphs/{id}/queries/{name}` reads; branch/snapshot access stays enforced by the inner `read`/`change` gate (`invoke_query` itself is graph-scoped, not branch-scoped).

## Policy gating (`invoke_query`)

- **`invoke_query`** is a per-graph Cedar action gating the whole stored-query invocation surface. Grant it like any other action (see [`server-policy.md`](server-policy.md)).
- **Stored mutations are double-gated:** the caller needs `invoke_query` to reach the query **and** `change` for the write. An actor with `invoke_query` but not `change` gets `403` on a stored mutation.
- **Deny == unknown:** for a caller *lacking* `invoke_query`, a denial and an unknown query name return the **same 404** (identical body) — the catalog can't be probed. A caller who *holds* `invoke_query` may still get a `403` from the inner gate for a query it can't `read`/`change`, so existence is visible to grant-holders by design.
- **Default-deny mode** (bearer tokens, no `policy.file`) permits only `read`, so *every* `/graphs/{id}/queries/{name}` call returns `404` until an `invoke_query` rule is configured.

## MCP exposure

Stored queries are surfaced two ways: the REST catalog `GET /graphs/{id}/queries` (typed entries for a client to register), and — since v0.8.0 — **live MCP tools** at `POST /graphs/{id}/mcp` (per-query tools below an exposed-count threshold, or a `stored_query_list` + `stored_query_run` pair above it). See [`server-policy.md`](server-policy.md#mcp-surface) and `docs/user/operations/mcp.md`.

Per-query MCP presentation is set in the `.gq` source via `@mcp(...)`:

```gq
query find_user(@description("the user's slug") $slug: String)
  @description("Look up a user by slug.")
  @instruction("Use for exact slugs; for fuzzy names use search_users.")
  @mcp(tool_name: "lookup_user", expose: true)
{ match { $u: User { slug: $slug } } return { $u.name } }
```

- `@mcp(tool_name: "…")` — the MCP tool id (default: the query name). Must be unique across exposed queries and must not shadow a built-in, or the server refuses to boot.
- `@mcp(expose: false)` — hide from `tools/list`, `stored_query_list`, and `stored_query_run` (by name). Presentation only: the query stays HTTP/service-callable via `POST /queries/{name}` for any caller with `invoke_query` (which is the *authorization* gate — Cedar, not `expose`).
- The MCP tool description folds `@instruction` after `@description`; per-param `@description` documents each argument in the tool input schema.

Defaults (no `@mcp`): exposed, tool name = query name. There is no `cluster.yaml` per-query block — the source annotation is the single home.

## Note on per-query authorization

The catalog is **not** Cedar-filtered per query yet: a caller with `read` but not `invoke_query` can *list* a query it cannot *invoke* (invocation would 404). Per-query authorization is future work; for now the catalog is a discovery surface and `invoke_query` is the invocation gate.

