# HTTP Server (`omnigraph-server`)

Axum 0.8 + tokio + utoipa-generated OpenAPI. **Two modes** (v0.6.0+): single-graph (legacy) and multi-graph (MR-668). Mode is inferred from CLI args + config shape.

## Modes

### Single-graph mode (legacy)

`omnigraph-server <URI>` or `omnigraph-server --target <name> --config omnigraph.yaml`. Routes are flat — `/snapshot`, `/read`, `/branches`, etc. Behavior unchanged from v0.6.0.

### Multi-graph mode (v0.6.0+)

`omnigraph-server --config omnigraph.yaml` with a non-empty `graphs:` map and **no** single-mode selector (no `server.graph`, no `<URI>`, no `--target`). The server opens every configured graph in parallel at startup (bounded concurrency = 4, fail-fast on the first open error). Routes are nested under `/graphs/{graph_id}/...`. Bare flat paths return 404 in multi mode.

Mode inference (four-rule matrix):

1. CLI positional `<URI>` → single
2. CLI `--target <name>` → single
3. `server.graph` in config → single
4. `--config` + non-empty `graphs:` + no single-mode selector → **multi**
5. otherwise → error with migration hint

## Endpoint inventory

Per-graph endpoints — same body shape across modes; URLs differ:

| Method | Single-mode path | Multi-mode path | Auth | Action | Handler |
|---|---|---|---|---|---|
| GET | `/healthz` | `/healthz` | none | — | `server_health` |
| GET | `/openapi.json` | `/openapi.json` | none | — | `server_openapi` (strips security if auth disabled; in multi mode emits cluster paths with `cluster_` operation-id prefix) |
| GET | `/snapshot?branch=` | `/graphs/{id}/snapshot?branch=` | bearer + `read` | snapshot of branch | `server_snapshot` |
| POST | `/read` | `/graphs/{id}/read` | bearer + `read` | run named query | `server_read` |
| POST | `/export` | `/graphs/{id}/export` | bearer + `export` | NDJSON stream | `server_export` |
| POST | `/change` | `/graphs/{id}/change` | bearer + `change` | mutation | `server_change` |
| GET | `/schema` | `/graphs/{id}/schema` | bearer + `read` | get current `.pg` source | `server_schema_get` |
| POST | `/schema/apply` | `/graphs/{id}/schema/apply` | bearer + `schema_apply` (target=`main`) | migrate | `server_schema_apply` |
| POST | `/ingest` | `/graphs/{id}/ingest` | bearer + `branch_create` (if new) + `change` | bulk load | `server_ingest` (32 MB body limit) |
| GET | `/branches` | `/graphs/{id}/branches` | bearer + `read` | list branches | `server_branch_list` |
| POST | `/branches` | `/graphs/{id}/branches` | bearer + `branch_create` | create | `server_branch_create` |
| DELETE | `/branches/{branch}` | `/graphs/{id}/branches/{branch}` | bearer + `branch_delete` | delete | `server_branch_delete` |
| POST | `/branches/merge` | `/graphs/{id}/branches/merge` | bearer + `branch_merge` | merge `source → target` | `server_branch_merge` |
| GET | `/commits?branch=` | `/graphs/{id}/commits?branch=` | bearer + `read` | list | `server_commit_list` |
| GET | `/commits/{commit_id}` | `/graphs/{id}/commits/{commit_id}` | bearer + `read` | show | `server_commit_show` |

Server-level management endpoints (v0.6.0+):

| Method | Path | Auth | Action | Handler |
|---|---|---|---|---|
| GET | `/graphs` | bearer + `graph_list` on `Server::"root"` | list registered graphs | `server_graphs_list` (405 in single mode) |

## Adding and removing graphs (multi mode)

Runtime add/remove via API is **not** exposed in v0.6.0 — neither
`POST /graphs` nor `DELETE /graphs/{id}` is implemented. Operators add
or remove graphs by stopping the server, editing the `graphs:` map in
`omnigraph.yaml`, then restarting. The server treats `omnigraph.yaml`
as operator-owned configuration and never writes it.

A future release may introduce a managed registry (Lance-backed,
catalog-style: reserve → init → publish with recovery sidecars) and
re-expose runtime mutation on top of it.

## Streaming

Only `/export` streams (`application/x-ndjson`, MPSC channel + `Body::from_stream`). Everything else is buffered JSON.

## Error model

Uniform `ErrorOutput { error, code?, merge_conflicts[], manifest_conflict? }` with `code ∈ unauthorized | forbidden | bad_request | not_found | conflict | too_many_requests | internal`. Merge conflicts attach structured `MergeConflictOutput { table_key, row_id?, kind, message }`.

`manifest_conflict` is set on **publisher CAS rejections** (HTTP 409): the
caller's pre-write view of one table's manifest version was stale.
`ManifestConflictOutput { table_key, expected, actual }` tells the client
which table to refresh and retry. This is the conflict shape produced by
concurrent `/change` or `/ingest` calls landing the same `(table, branch)`
race.

HTTP status codes used: 200, 400, 401, 403, 404, 409, 429, 500.

## Per-actor admission control

Disjoint
`(table, branch)` writes from different actors now run concurrently,
guarded only by the engine's per-(table, branch) write queue. To keep
one heavy actor from exhausting shared capacity (Lance I/O, manifest
churn, network), the server gates mutating handlers through a
`WorkloadController` configured per-process from environment variables:

| Env var | Default | Purpose |
|---|---|---|
| `OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX` | 16 | Concurrent in-flight mutations per actor |
| `OMNIGRAPH_PER_ACTOR_BYTES_MAX` | 4 GiB | In-flight estimated bytes per actor |

When an actor exceeds its in-flight count or byte budget, the server
returns **HTTP 429 Too Many Requests** with `code: too_many_requests`
and a `Retry-After` header (seconds). The actor should back off; other
actors are unaffected.

Cedar policy authorization runs **before** admission accounting so
denied requests don't consume admission slots.

Today admission gates every mutating handler: `/change`, `/ingest`,
`/branches/{create,delete,merge}`, and `/schema/apply`. Read-only
endpoints (`/snapshot`, `/read`, `/export`, `/branches` GET, `/commits`,
`/schema` GET) are not admission-gated.

## Body limits

- Default: 1 MB
- `/ingest`: 32 MB

## Auth model (`bearer + SHA-256`)

- Tokens are SHA-256 hashed on startup; plaintext is never persisted in memory.
- Constant-time comparison via `subtle::ConstantTimeEq`.
- Three sources, in precedence:
  1. `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` — AWS Secrets Manager (build with `--features aws`)
  2. `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — JSON `{actor_id: token, …}`
  3. `OMNIGRAPH_SERVER_BEARER_TOKEN` — single legacy token, actor `default`
- If no tokens and no policy are configured, startup refuses unless
  `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1` explicitly opts into
  open local-dev mode. In that mode `/openapi.json` strips the security scheme.

See [deployment.md](deployment.md) for token-source operational details.

## Tracing & observability

- `tower_http::TraceLayer::new_for_http()`
- Policy decisions logged at INFO level with actor, action, branch, decision, matched rule
- Startup logs: token source name, graph URI, bind address
- Graceful SIGINT shutdown

## Not implemented (by design or "TBD")

- CORS — not configured; add `tower_http::cors` if needed.
- Rate limiting — per-actor admission control gates `/change`, `/ingest`,
  `/branches/{create,delete,merge}`, `/schema/apply` (see "Per-actor
  admission control" above). No global rate limiter is configured;
  add `tower_http::limit` if a graph-wide cap is needed.
- Pagination — none (commits/branches return everything; export streams).
- Runtime graph add/remove — edit `omnigraph.yaml` and restart.
