# HTTP Server (`omnigraph-server`)

Axum 0.8 + tokio + utoipa-generated OpenAPI. Single repo per process; deploy multiple processes for multi-tenant.

## Endpoint inventory

| Method | Path | Auth | Action | Handler |
|---|---|---|---|---|
| GET | `/healthz` | none | — | `server_health` |
| GET | `/openapi.json` | none | — | `server_openapi` (strips security if auth disabled) |
| GET | `/snapshot?branch=` | bearer + `read` | snapshot of branch | `server_snapshot` |
| POST | `/read` | bearer + `read` | run named query | `server_read` |
| POST | `/export` | bearer + `export` | NDJSON stream | `server_export` |
| POST | `/change` | bearer + `change` | mutation | `server_change` |
| GET | `/schema` | bearer + `read` | get current `.pg` source | `server_schema_get` |
| POST | `/schema/apply` | bearer + `schema_apply` (target=`main`) | migrate | `server_schema_apply` |
| POST | `/ingest` | bearer + `branch_create` (if new) + `change` | bulk load | `server_ingest` (32 MB body limit) |
| GET | `/branches` | bearer + `read` | list branches | `server_branch_list` |
| POST | `/branches` | bearer + `branch_create` | create | `server_branch_create` |
| DELETE | `/branches/{branch}` | bearer + `branch_delete` | delete | `server_branch_delete` |
| POST | `/branches/merge` | bearer + `branch_merge` | merge `source → target` | `server_branch_merge` |
| GET | `/runs` | bearer + `read` | list | `server_run_list` |
| GET | `/runs/{run_id}` | bearer + `read` | show | `server_run_show` |
| POST | `/runs/{run_id}/publish` | bearer + `run_publish` | publish | `server_run_publish` |
| POST | `/runs/{run_id}/abort` | bearer + `run_abort` | abort | `server_run_abort` |
| GET | `/commits?branch=` | bearer + `read` | list | `server_commit_list` |
| GET | `/commits/{commit_id}` | bearer + `read` | show | `server_commit_show` |

## Streaming

Only `/export` streams (`application/x-ndjson`, MPSC channel + `Body::from_stream`). Everything else is buffered JSON.

## Error model

Uniform `ErrorOutput { error, code?, merge_conflicts[] }` with `code ∈ unauthorized | forbidden | bad_request | not_found | conflict | internal`. Merge conflicts attach structured `MergeConflictOutput { table_key, row_id?, kind, message }`.

HTTP status codes used: 200, 400, 401, 403, 404, 409, 500.

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
- If no tokens configured, server runs unauthenticated (local dev) and `/openapi.json` strips the security scheme.

See [deployment.md](deployment.md) for token-source operational details.

## Tracing & observability

- `tower_http::TraceLayer::new_for_http()`
- Policy decisions logged at INFO level with actor, action, branch, decision, matched rule
- Startup logs: token source name, repo URI, bind address
- Graceful SIGINT shutdown

## Not implemented (by design or "TBD")

- CORS — not configured; add `tower_http::cors` if needed.
- Rate limiting — none.
- Pagination — none (commits/branches/runs return everything; export streams).
- Multi-tenant routing — one repo per process.
