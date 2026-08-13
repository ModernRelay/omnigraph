# HTTP Server (`omnigraph-server`)

Axum 0.8 + tokio + utoipa-generated OpenAPI. **Cluster-only boot**: the server always boots from a cluster (`--cluster <dir | s3://…>`) and serves N graphs (N ≥ 1) under cluster routes. There is no longer a single-graph flat-route mode, no positional `<URI>` boot, no `--target`, and no `omnigraph.yaml`-`graphs:`-map boot. All HTTP is nested under `/graphs/{graph_id}/...`; `/healthz` and the management `/graphs` enumeration stay flat.

## Boot

### Cluster boot (the only boot)

```bash
omnigraph-server --cluster <dir | s3://…> --bind 0.0.0.0:8080
```

Passing port `0` lets the operating system select an available port. After the
listener binds, the server writes the actual address to stdout as one
machine-readable line:

```text
OMNIGRAPH_LISTEN_ADDR=127.0.0.1:54321
```

Process supervisors and test harnesses can use this record instead of
reserving and releasing a port before starting the server. When the bind host
is a wildcard such as `0.0.0.0` or `[::]`, the record identifies the listener
but is not itself a portable client endpoint; use the selected port with a
host that is reachable from the client.

`omnigraph-server --cluster <dir-or-uri>` boots from the cluster catalog's
**applied revision**. The server resolves that revision into per-graph
startup configs (id, URI, optional per-graph policy, stored-query
registry) plus an optional server-level policy, then opens every
configured graph in parallel at startup (bounded concurrency = 4,
quarantining graph-specific open failures). Routing is always multi-graph —
requests to bare flat protected paths (`/read`, `/snapshot`, …) return
404; the served surface is `/graphs/{graph_id}/...`. See
[cluster-config.md](../clusters/config.md#serving-from-the-cluster-the-mode-switch)
for what is read and the readiness rules.

Readiness is fail-fast for cluster-global problems: missing or unreadable
state, invalid/unattributable recovery sidecars, unreadable shared catalog
payloads, cluster policy errors, or zero healthy graphs. Graph-attributed
pending recovery sidecars and graph-specific startup failures quarantine
that graph instead; the server logs startup diagnostics and serves the
remaining healthy graphs. `GET /graphs` enumerates ready/served graphs only,
so quarantined graphs are absent and their routes return 404.

Operators who want the original all-or-nothing boot contract can pass
`--require-all-graphs` or set `OMNIGRAPH_REQUIRE_ALL_GRAPHS=1`. In that mode,
any graph quarantine, graph-open failure, stored-query startup failure, or
embedding-provider resolution failure aborts startup.

A scheme-qualified argument (`s3://…`) reads the ledger straight from the
storage root, with no local config directory. `--bind`,
`--unauthenticated`, and the bearer-token env vars all apply.

### Stored-query validation at startup

If a graph declares a `queries:` registry (see [cli-reference](../cli/reference.md)), the server **loads and type-checks every stored query against that graph's live schema at startup**. Query parse/type failures quarantine that graph; if no graph remains healthy, startup refuses. Two MCP-exposed queries claiming the same tool name are likewise graph-local startup failures. Non-blocking advisories (e.g. an MCP-exposed query with a vector parameter an agent cannot supply) are logged. Validate offline before deploying with `omnigraph queries validate`. Discover the stored queries as a typed tool catalog with `GET /queries`, and invoke one over HTTP with `POST /queries/{name}` (both below).

## Endpoint inventory

Per-graph endpoints — all nested under `/graphs/{id}/...`. `{id}` is the
graph id from the cluster's applied revision:

| Method | Path | Auth | Action |
|---|---|---|---|
| GET | `/healthz` | none | — |
| GET | `/openapi.json` | none | — (strips security if auth disabled; emits the nested cluster paths with `cluster_` operation-id prefix) |
| GET | `/graphs/{id}/snapshot?branch=` | bearer + `read` | snapshot of branch |
| GET / HEAD | `/graphs/{id}/blob?entity=&type=&id=&property=&branch=|snapshot=` | bearer + `read` | stream one logical node/edge Blob cell, or return its metadata without payload bytes |
| POST | `/graphs/{id}/query` | bearer + `read` | inline read query (canonical; clean field names `query`/`name`; mutations → 400) |
| POST | `/graphs/{id}/read` | bearer + `read` | **deprecated** alias of `/query` (legacy field names `query_source`/`query_name`, byte-stable response; carries `Deprecation: true` + `Link: <query>; rel="successor-version"`) |
| POST | `/graphs/{id}/export` | bearer + `export` | NDJSON stream |
| POST | `/graphs/{id}/mutate` | bearer + `change` | mutation (canonical; `query`/`name`; accepts legacy `query_source`/`query_name` as serde aliases) |
| POST | `/graphs/{id}/mutate/if-graph-commit` | bearer + `change` | conditional mutation; requires `Omnigraph-If-Graph-Commit` |
| POST | `/graphs/{id}/change` | bearer + `change` | **deprecated** alias of `/mutate` (carries `Deprecation: true` + `Link: <mutate>; rel="successor-version"`) |
| GET | `/graphs/{id}/queries` | bearer + `read` | list the graph's stored queries as a typed tool catalog |
| POST | `/graphs/{id}/queries/{name}` | bearer + `invoke_query` (+ `change` for a stored mutation) | invoke a named query from the `queries:` registry; deny == 404 |
| POST | `/graphs/{id}/queries/{name}/if-graph-commit` | bearer + `invoke_query` + `change` | invoke a stored mutation conditionally; requires `Omnigraph-If-Graph-Commit` |
| GET | `/graphs/{id}/schema` | bearer + `read` | get current `.pg` source |
| POST | `/graphs/{id}/schema/apply` | bearer + `schema_apply` (target=`main`) | disabled for cluster-backed serving; returns 409 and points operators at `omnigraph cluster apply` + restart |
| POST | `/graphs/{id}/load` | bearer + `branch_create` (only when `from` is set and the branch is created) + `change` | JSON-envelope load (`data` contains NDJSON), retained for compatibility (32 MB body limit) |
| POST | `/graphs/{id}/load/ndjson?branch=&from=&mode=` | bearer + `branch_create` (only when `from` is set and the branch is created) + `change` | strict raw `application/x-ndjson` graph batch; one request publishes one graph commit before success (32 MB body limit) |
| POST | `/graphs/{id}/ingest` | bearer + `branch_create` (only when `from` is set and the branch is created) + `change` | **deprecated** alias of `/load` (carries `Deprecation: true` + `Link: <load>; rel="successor-version"`) (32 MB body limit) |
| GET | `/graphs/{id}/branches` | bearer + `read` | list branches |
| POST | `/graphs/{id}/branches` | bearer + `branch_create` | create |
| DELETE | `/graphs/{id}/branches/{branch}` | bearer + `branch_delete` | delete |
| POST | `/graphs/{id}/branches/merge` | bearer + `branch_merge` (+ `branch_delete` only when `delete_branch` is set) | merge `source → target`; `delete_branch: true` also deletes the source after the merge lands — a delete refusal is reported via `branch_deleted`/`branch_delete_error` on the 200 response, never as an error |
| GET | `/graphs/{id}/commits?branch=` | bearer + `read` | list |
| GET | `/graphs/{id}/commits/{commit_id}` | bearer + `read` | show |
| GET | `/graphs/{id}/commits/{commit_id}/changes?cursor=&limit=&max_bytes=` | bearer + `read` | bounded ordered first-parent changes |

Server-level management endpoints:

| Method | Path | Auth | Action |
|---|---|---|---|
| GET | `/graphs` | bearer + `graph_list` on `Server::"root"` | list ready/served graphs |

> The per-graph subsections below name routes in shorthand (`GET /queries`,
> `POST /query`, `POST /mutate`, `POST /queries/{name}`); every one is served
> under the `/graphs/{id}/…` prefix shown in the table — only `/graphs` and
> `/healthz` are flat.

### Stored-query catalog (`GET /queries`)

List the graph's stored queries as a typed tool catalog — enough for a client (e.g. an MCP server) to register each as a tool without fetching `.gq` source. Each entry: `{ name, tool_name, description, instruction, mutation, params }`, where each param is `{ name, kind, item_kind?, vector_dim?, nullable }`. `kind` is one of `string | bool | int | bigint | float | date | datetime | blob | vector | list` (decomposed so a consumer maps it with a closed `switch`, never re-parsing GQ type spelling). `bigint` (I64/U64), `date`, `datetime`, and `blob` are carried as JSON **strings** — a 64-bit integer loses precision as a JSON number, dates are ISO strings, and a blob is a URI string.

- **Read-gated** (works in default-deny mode). The catalog is **graph-wide** (branch-independent; `read` is authorized against `main`).
- **Every stored query in the applied registry is listed.** Cluster-served graphs have no per-query expose flag today — every query in the cluster `queries:` registry appears in the catalog. (Per-query exposure may become a Cedar-policy decision in a later release; see [cluster-config](../clusters/config.md).)
- **Not Cedar-filtered per query (yet).** A caller with `read` but not `invoke_query` can *list* a query they can't *invoke* (which would 404). Closing that gap is future per-query authorization; for now the catalog is a discovery surface and `invoke_query` remains the invocation gate.

### Stored-query invocation (`POST /queries/{name}`)

Invoke a curated, server-side stored query by **name** — the source comes from the graph's `queries:` registry, so the client never sends `.gq`. The request body itself is optional; omit it for no-param queries, or send `{ "params": { … }, "branch": "main", "snapshot": null }`, where every field is optional and `params` keys match the query's declared parameters. The response is the **read envelope** (`ReadOutput`) for a stored read or the **mutation envelope** (`ChangeOutput`) for a stored mutation — serialized untagged, so the wire shape is identical to `/query` / `/mutate`.

- **Gate:** `invoke_query` (per-graph, graph-scoped) at the boundary. A stored *mutation* is **double-gated** — it also passes the engine's `change` gate, so an actor with `invoke_query` but not `change` gets `403`.
- **Deny == unknown, for callers without `invoke_query`:** for a caller lacking the grant, an `invoke_query` denial and an unknown query name return the **same `404`** (identical body), so the catalog can't be probed. A caller that *holds* `invoke_query` may still get the inner gate's `403` for an existing query it can't `read`/`change` (the double-gate, above) — so existence is visible to grant-holders by design.
- **Requires an explicit policy grant when auth is on.** In default-deny mode (bearer tokens but no `policy.file`), only `read` is permitted, so *every* `/queries/{name}` call returns `404` until an `invoke_query` rule is configured.
- A stored mutation cannot target a `snapshot` (`400`); a parameter type error is a structured `400` naming the parameter.

## Adding and removing graphs

Runtime add/remove via API is **not** exposed — neither `POST /graphs`
nor `DELETE /graphs/{id}` is implemented. Operators add or remove graphs
by running `cluster apply` against the cluster (which publishes a new
applied revision) and restarting the server so it boots from the new
revision. The server treats the cluster source as operator-owned and
never writes it.

A future release may introduce a managed registry and re-expose runtime
mutation on top of it.

## Inline read queries (`POST /query`)

`POST /query` is the read-only, agent-friendly twin of `POST /read`. The
request body uses clean field names that match the CLI `-e` flag and the GQ
`query` keyword:

```json
{
  "query":    "query find($n: String) { match { $p: Person { name: $n } } return { $p.name } }",
  "name":     "find",
  "params":   { "n": "Alice" },
  "branch":   "main",
  "snapshot": null
}
```

The response uses `ReadOutput`: it shares `/read`'s query rows and target
fields and additionally carries the pinned `graph_commit_id`. The deprecated
`/read` route intentionally keeps its older token-free body byte-stable. If
the inline source contains mutations (`insert` / `update` / `delete`), the
request is rejected with HTTP 400 and an error pointing the caller at
`POST /mutate` — the read-only contract is enforced at the URL.

`POST /mutate` is the canonical mutation endpoint. It accepts the same clean
field names (`query`, `name`); the legacy field names `query_source` and
`query_name` continue to deserialize as serde aliases so existing clients keep
working without changes. Successful mutation and load responses carry the exact
published `commit`; a mutation that changes no rows carries `commit: null`.

## Deprecated names (`/read`, `/change`)

`POST /read` and `POST /change` are kept for back-compat indefinitely. They
retain their legacy request shapes and otherwise share `/query` / `/mutate`
execution semantics. `/read` also retains its legacy token-free response body;
only `/query` exposes `graph_commit_id`. They are flagged as deprecated through
three independent channels:

- **OpenAPI**: the operations carry `deprecated: true` in `openapi.json`, so
  every OpenAPI codegen (typescript-fetch, openapi-generator, oapi-codegen,
  …) emits a `@deprecated` marker on the generated SDK method.
- **Response headers (RFC 9745)**: every response carries `Deprecation: true`.
- **Response headers (RFC 8288)**: every response carries a `Link` header
  pointing at the canonical successor:
  `Link: <query>; rel="successor-version"` for `/read`, and
  `Link: <mutate>; rel="successor-version"` for `/change`. SDKs and HTTP
  proxies can pick the successor up automatically.

Migration keeps the same query and row semantics, but `/query` adds the
optional `graph_commit_id` response field. Permissive JSON decoders can ignore
it; strict-schema or byte-sensitive clients must update their response model
when swapping the URL path.

## Bounded graph-batch ingestion

`POST /graphs/{graph_id}/load/ndjson` accepts a raw
`application/x-ndjson` body. Targeting stays at graph level: `branch`, optional
`from`, and `mode=append|merge|overwrite` are query parameters; there is no
table or dataset selector. One request may mix logical node and edge
declarations:

```http
POST /graphs/knowledge/load/ndjson?branch=main&mode=merge
Content-Type: application/x-ndjson

{"type":"Person","data":{"name":"Ada"}}
{"edge":"Knows","from":"Ada","to":"Grace","data":{}}
```

The server authenticates and completes Cedar `branch_create`/`change`
authorization before polling the body. It then buffers at most 32 MiB, applies
the ordinary admission limits, strictly validates the complete batch, and calls
the actor-aware graph-batch loader. All touched declarations publish through
the existing multi-dataset transaction. A successful response is terminal: the
single graph commit is already visible. Its `nodes` and `edges` arrays contain
only logical accepted-schema names and row counts; physical table and Lance
identities are not exposed.

`append` is strict insert, `merge` is upsert, and `overwrite` replaces each
touched declaration's image. A malformed batch has no effect. Use multiple
bounded requests for a larger feed; each successful request is one graph
commit.

The existing `POST /load` JSON envelope remains available: its `data` string
contains the NDJSON and its other fields carry the same branch options. The
canonical remote `omnigraph load` command uses the raw `/load/ndjson` route.

The legacy JSON `POST /ingest` endpoint remains a deprecated alias of
JSON `POST /load`.

## Export streaming

The `/export` route streams `application/x-ndjson`; other routes remain
buffered JSON. Export authorization, recovery settlement, and branch/filter
validation all finish before the server sends `200`.

The engine incrementally scans exact pinned Lance versions using an initial
8,192-row estimate and Lance's approximate 32-MiB decoded-byte target; these
are scheduling targets, not hard scanner-memory ceilings. Blob descriptors are
explicitly sliced to one logical row before its complete Blob-property set is
materialized. One row's Blob values and encoded JSON are indivisible scratch
outside the transport reservation. Encoded JSONL is split into independently
owned chunks of at most 64 KiB.

Each response uses a two-chunk bounded queue and reserves 256 KiB for the two
queued chunks, one producer chunk awaiting admission, and one consumer-current
chunk. This is a complete transport-queue envelope, not a cap on the whole
response or process RSS. The production process holds a 2-MiB aggregate queue
budget (eight reservations) and waits at most 250 ms for one. An occupied graph
export cut or saturated transport returns the ordinary structured HTTP 413
response before success headers. A stalled client backpressures production;
the response body and producer jointly retain the queue lease, so disconnect
closes the receiver immediately but does not recycle the permit until the
producer has unwound. The immutable cut remains in the producer or a terminal
frame queued after all data. A storage failure after `200` terminates the
response body as a stream error; it cannot be rewritten into a JSON error after
headers. Clients must discard a partial artifact whenever body consumption
fails.

## Blob delivery

`GET /graphs/{graph_id}/blob` and its explicit `HEAD` twin select one logical
node or edge property; they never expose a Lance dataset, table key, row
address, or physical Blob placement:

```http
GET /graphs/knowledge/blob?entity=node&type=Document&id=manual&property=content&branch=main
```

`entity` is `node` or `edge`. `type`, `id`, and `property` are required.
Choose at most one of `branch` and `snapshot`; omitting both reads `main`.
Snapshot requests use the same snapshot-to-policy-branch resolution and Cedar
`read` authorization as `/query`.

A managed value returns `application/octet-stream`, its exact
`Content-Length`, `Accept-Ranges: bytes`, a strong `ETag`, and
`Omnigraph-Snapshot-Id`, which identifies the exact graph snapshot selected by
the request. The body is pulled from the snapshot-pinned engine reader in
chunks no larger than 4 MiB. Transport backpressure retains no more than two
chunks (8 MiB) for one response; disconnect cancels the reader promptly. A
storage failure after success headers terminates the body loudly, so clients
must discard a partial artifact.

GET supports one `bytes` range (`start-end`, `start-`, or `-suffix`) and returns
`206` with `Content-Range`. A valid but unsatisfiable range—including
`bytes=0-0` on a valid empty Blob—returns `416`, `Content-Range: bytes */N`, and
structured `blob_range { start, end, length }` details. Multiple ranges,
malformed ranges, and unknown range units are intentionally ignored in V1, so
the complete representation is returned instead of multipart output.

`If-Match` uses strong comparison over an entity-tag list and supports `*`; it
is evaluated first, and failure returns `412` with `code: conflict` and the
current managed `ETag` before any payload read.
`If-None-Match` uses weak comparison over an entity-tag list and supports `*`;
a match returns `304`. `If-Range` honors a Range only when it contains the one
matching strong validator; otherwise GET returns the complete representation.
HEAD deliberately ignores `Range` and `If-Range`, honors `If-Match` and
`If-None-Match`, and
returns full-representation headers without reading payload bytes. It is a
dedicated handler, not an automatic GET fallback.

A null cell or unknown entity is `404`; invalid selectors and non-Blob
properties are `400`; a failed managed `If-Match` is `412`. An external
descriptor is never opened or proxied by the
server: GET and HEAD return `302`, its exact stored absolute URI in `Location`,
`Cache-Control: no-store`, and the selected `Omnigraph-Snapshot-Id`, with no
ETag or asserted external-object length. A `Content-Length: 0` may frame the
empty redirect response body; it says nothing about the target. The server does
not issue an external HEAD/GET, resolve credentials, sign the URI, or translate
ranges. Phase 2A redirects only whole-object external descriptors; a persisted
descriptor whose logical value is an external sub-range fails loudly with 500
rather than widening that value to the whole target object.

## Error model

Uniform
`ErrorOutput { error, code?, merge_conflicts[], manifest_conflict?, key_conflict?, read_set_conflict?, resource_limit?, external_blob_source?, blob_range?, recovery_required?, precondition_failure? }`
with
`code ∈ unauthorized | forbidden | bad_request | not_found | method_not_allowed | conflict | too_many_requests | internal`.
Merge conflicts attach structured
`MergeConflictOutput { table_key, row_id?, kind, message }`.

`manifest_conflict` is set on legacy per-table manifest-version rejections
(HTTP 409). `ManifestConflictOutput { table_key, expected, actual }` tells the
client which table was stale. Mutation and load use the unified coarse-OCC
adapter described next; other writers retain this older conflict shape until
they are enrolled.

`read_set_conflict` is set when a prepared write is rejected before any table
effect because its branch authority changed. The HTTP status is 409 and
`ReadSetConflictOutput { member, expected, actual }` identifies the stale
authority member. The engine already performs a bounded full-attempt retry for
mutation inserts and load `append`/`merge`. Strict mutation updates/deletes and
load `overwrite` return the 409 to the caller instead of being replayed.

`external_blob_source` is set when a URI passed the graph's external-Blob
admission policy but its source could not be probed or read. The HTTP status is
424 and `ExternalBlobSourceOutput { uri, reason }` carries the normalized,
credential-free URI (or redacted placeholder) plus a human-readable diagnosis.
Clients identify this failure from the presence of `external_blob_source`; they
must not parse `reason`. The optional `code` field is omitted because adding a
new value to the closed error-code enum would break older clients, while the
optional structured field is additive and rolling-safe. Policy or URI-shape
refusals remain HTTP 400 with `code: bad_request`.

`blob_range` is set on a Blob GET whose single valid byte range is
unsatisfiable. HTTP 416 also carries `Content-Range: bytes */N`;
`BlobRangeOutput { start, end, length }` records the normalized attempted
half-open range and logical representation length without requiring clients to
parse the human-readable error string.

`recovery_required` is set when an overlapping durable recovery intent remains
unresolved; its table effects may or may not have started. The HTTP status is 503 and
`RecoveryRequiredOutput { operation_id }` names the durable recovery intent.
The optional `code` field is omitted for this response: adding a new value to
the closed error-code enum would break older clients, while the optional
structured field is additive and rolling-safe.
Do not blindly resubmit the write: let a read-write open or the recovery sweep
resolve that operation first, then retry from a fresh snapshot.

`precondition_failure` is set when a mutation carried an
`Omnigraph-If-Graph-Commit: <commit_id>` branch-head precondition and the
branch's head no longer matches that id. The HTTP status is 412 and
`PreconditionFailureOutput { expected, actual? }` carries the id the caller
named and the current head (`actual` is absent on a branch with no commits).
The write had no effect and is never internally retried — losing the
compare-and-swap is the signal the caller asked for; re-read the branch and
decide again. Like `recovery_required`, the `code` field is omitted (closed
enum); detect this outcome by the 412 status or the presence of the field.
`Omnigraph-If-Graph-Commit` is required on the dedicated
`POST /mutate/if-graph-commit` and
`POST /queries/{name}/if-graph-commit` routes; the id comes directly from a
canonical read response or from `GET /commits`. Ordinary mutation routes and
stored reads reject the header. A distinct route makes rolling upgrades fail
closed: an older server returns 404 before execution rather than silently
ignoring a new optional header.
The value is one raw graph commit id; HTTP entity-tag forms such as `*`, lists,
quoted tags, and weak (`W/"..."`) tags are rejected with 400.

This 412 contract is enforced across concurrent requests within the supported
single-writer-process topology. The gate is not a distributed lease. If an
unsupported foreign writer advances the branch after local pre-effect
arbitration, the exact publisher still prevents a silent lost update, but the
losing request may already own durable table effects and therefore returns
`recovery_required` (503) for recovery instead of 412.

HTTP status codes used include 200, 206, 302, 304, 400, 401, 403, 404, 405,
409, 410, 412, 413, 415, 416, 424, 429, 500, and 503.

## Per-actor admission control

RFC-022-enrolled mutation/load preparation runs outside the effect gates, so
parsing, validation, and reclaimable fragment staging can overlap across branches.
Readers acquire none of these gates. Before the first durable effect, however, an
attempt acquires the exclusive root schema gate, then its branch-effect gate and
sorted table queues, and holds all of them through manifest publication. The root
schema gate means enrolled effect windows on one graph currently serialize
in-process even across different branches; the branch gate preserves one atomic
graph-head validation authority, while table queues protect each concrete Lance
effect and legacy writer. These are process-local ordering gates, not a
cross-process lock. To keep one heavy actor from exhausting shared capacity
(Lance I/O, manifest churn, network), the server gates mutating handlers through
per-process admission limits configured from environment variables:

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

Today admission gates every mutating handler: `/mutate` (and its
deprecated alias `/change`), `/load` (and its deprecated alias `/ingest`),
`/load/ndjson`,
`/branches/{create,delete,merge}`,
and `/schema/apply`. Read-only endpoints (`/snapshot`, `/blob`, `/query`,
`/read`, `/export`, `/branches` GET, `/commits`, `/schema` GET) are not
admission-gated.


## Body limits

- Default: 1 MB
- `/load`, `/load/ndjson`, and the deprecated `/ingest` alias: 32 MB

## Auth model (`bearer + SHA-256`)

- Tokens are SHA-256 hashed on startup; plaintext is never persisted in memory.
- Constant-time comparison.
- Three sources, in precedence:
  1. `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` — AWS Secrets Manager (build with `--features aws`)
  2. `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — JSON `{actor_id: token, …}`
  3. `OMNIGRAPH_SERVER_BEARER_TOKEN` — single legacy token, actor `default`
- If no tokens are configured, startup refuses unless `--unauthenticated` or
  `OMNIGRAPH_UNAUTHENTICATED=1` explicitly opts into open local-dev mode. A
  policy file without tokens is also rejected at startup. In open mode
  `/openapi.json` strips the security scheme.

See [deployment.md](../deployment.md) for token-source operational details.

## Tracing & observability

- `tower_http::TraceLayer::new_for_http()`
- Policy decisions logged at INFO level with actor, action, branch, decision, matched rule
- Startup logs: token source name, graph URI, bind address
- Graceful SIGINT shutdown

## Not implemented (by design or "TBD")

- CORS — not configured; add `tower_http::cors` if needed.
- Rate limiting — per-actor admission control gates `/mutate` (alias
  `/change`), `/load` (alias `/ingest`), `/branches/{create,delete,merge}`,
  `/load/ndjson`,
  `/schema/apply` (see "Per-actor
  admission control" above). No global rate limiter is configured;
  add `tower_http::limit` if a graph-wide cap is needed.
- Pagination — commit changes use an opaque bounded cursor; commit and branch listings still return everything; export streams.
- Runtime graph add/remove — run `cluster apply` and restart.
