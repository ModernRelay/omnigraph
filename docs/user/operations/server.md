# HTTP server

`omnigraph-server` serves every healthy graph in one applied cluster under
`/graphs/{graph_id}/…`. It has no single-graph boot mode. Directory boot reads
the current `cluster.yaml` to resolve storage and validate the source location;
graph, query, and policy resources come from applied state. URI boot is
config-free.

The checked-in [OpenAPI document](../../../openapi.json) is the canonical schema
for the documented graph API in this source tree. A running server also returns
it from `GET /openapi.json`; that discovery route is not self-listed in the
document.

## Start a server

From a local cluster bundle:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

From an object-storage cluster root:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server \
    --cluster s3://company-data/omnigraph/company-brain \
    --bind 0.0.0.0:8080
```

The default bind address is `127.0.0.1:8080`. `--require-all-graphs` makes any
graph startup failure fatal. Without it, an unhealthy graph is quarantined and
healthy graphs continue to serve.

Applied changes become active after restart. Add or remove graphs with
`cluster.yaml` and `cluster apply`; there are no runtime graph-create/delete
routes. An unapplied resource edit does not activate it, although changing or
breaking the directory's config can change where boot looks for applied state.

## Authentication

Choose one static token source:

```bash
# One token, actor name "default"
export OMNIGRAPH_SERVER_BEARER_TOKEN='secret'

# Actor-to-token mapping
export OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret-a","act-bob":"secret-b"}'

# File containing the same JSON object
export OMNIGRAPH_SERVER_BEARER_TOKENS_FILE=/run/secrets/omnigraph-tokens.json
```

The AWS-enabled build can also read the mapping from Secrets Manager through
`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`.

Send the token as:

```http
Authorization: Bearer secret-a
```

The token selects the actor used by authorization and commit attribution.
Clients cannot claim another actor. See [Authorization and actors](policy.md).

A server with neither static tokens, signed-token trust, nor policy refuses to start unless you explicitly
pass `--unauthenticated` (or set `OMNIGRAPH_UNAUTHENTICATED=1`). Use that only
on a trusted development network. Tokens without a policy allow only the
`read` action. Stored-query invocation, export, graph listing, writes, and other
actions remain denied.

### Signed data credentials

To accept short-lived credentials from an issuer, mount its public trust file
and select it explicitly:

```bash
omnigraph-server --cluster s3://company-data/company-brain \
  --data-token-trust /run/omnigraph/data-token-trust.json
```

The file binds public signing keys to the exact storage root, issuer, account,
cluster id, and cluster incarnation. Invalid trust or a root mismatch refuses
startup before graphs open. Trust alone requires bearer authentication. The
server verifies tokens locally; it does not contact an identity or control
service. The provisioning operator owns supplying the correct identity binding.
The [trust and credential format](../../rfcs/0053-offline-data-token-verification.md#public-trust-and-root-binding)
defines the machine-written file.

Signed credentials use the actor `principal:<immutable-principal-id>`. Apply a
Cedar policy permitting that exact actor through the ordinary cluster loop
before using the credential. Its graph/action grants only narrow that policy:
no policy, an unknown actor, or an action absent from either permission source
is denied. A caller cannot change its actor through request headers or JSON.
Graph listing reveals only graphs with an explicit `graph_list` grant.

Tokens live for 60–86,400 seconds from issuance. The server permits an issuance
clock up to 30 seconds ahead, so at most 86,430 seconds can remain on admission.
Expiry has no grace period. Logout or a permission change at the issuer does
not revoke an issued token; already accepted operations can finish after
expiry. Stored-query calls need `invoke_query` plus `read` or `change` for the
body. Schema changes still use `cluster apply`; data tokens cannot grant
`schema_apply` or `admin`.

Static credentials can coexist for operator recovery. An exact configured
static credential keeps its existing authority, including credentials with
dots; an invalid signed credential never falls back to static or anonymous
access. Restart to change public trust. Install new and old keys together
before issuing with a new key, and retain the old key for at least 86,430
seconds after its final issuance before removing it with another restart.

## Route families

| Route family | Purpose |
|---|---|
| `GET /healthz` | Process health |
| `GET /openapi.json` | Runtime copy of the OpenAPI document |
| `GET /graphs` | List served graphs; requires `graph_list` policy |
| `/graphs/{id}/query`, `/mutate` | Run inline GQ source |
| `/graphs/{id}/mutate/if-graph-commit` | Run an inline conditional mutation |
| `/graphs/{id}/queries` | List and invoke stored queries, including conditional mutations |
| `/graphs/{id}/load`, `/load/ndjson` | Bounded batch loading |
| `/graphs/{id}/blob` | GET/HEAD one Blob cell |
| `/graphs/{id}/branches` | Branch management and merge |
| `/graphs/{id}/snapshot`, `/commits` | Snapshot, history, and per-commit changes |
| `/graphs/{id}/changes` | Poll a branch feed or establish a baseline |
| `/graphs/{id}/schema` | Show the accepted schema |
| `/graphs/{id}/export` | Stream a branch snapshot as JSONL |

`/read`, `/change`, and `/ingest` are deprecated compatibility routes. New
clients should use `/query`, `/mutate`, and `/load`.

`POST /graphs/{id}/schema/apply` remains in the wire surface for compatibility,
but a cluster-only server rejects it with `409`. Change a managed graph's
schema through `cluster apply`.

## Run an inline query

```bash
curl -sS http://localhost:8080/graphs/knowledge/query \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -d '{
    "query":"query find($name: String) { match { $p: Person { name: $name } } return { $p.name } }",
    "name":"find",
    "params":{"name":"Ada"}
  }'
```

Use `branch` or `snapshot` to select a read view; they are mutually exclusive.
When the read snapshot has an effective graph head, the canonical `/query`
response includes its `graph_commit_id`, pinned with the returned rows. Inline
writes go to `/mutate` and may select a target `branch`.

The deprecated `/read` compatibility response does not include
`graph_commit_id`; clients that need a read position must use `/query`.

## Invoke a stored query

Stored queries are part of the applied cluster revision:

```bash
curl -sS http://localhost:8080/graphs/knowledge/queries/find_person \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -d '{"params":{"name":"Ada"}}'
```

Authorization denials for stored-query invocation appear as `404`, preventing
callers from probing registry names. A stored query then receives the normal
`read` or `change` authorization check for its body.

## Conditional mutations

Use a dedicated route and the commit returned by the read whose result you are
acting on:

```bash
curl -sS http://localhost:8080/graphs/knowledge/mutate/if-graph-commit \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -H 'Omnigraph-If-Graph-Commit: <graph_commit_id>' \
  -d '{"query":"query rename($name: String) { update Person set { name: $name } where email = \"ada@example.com\" }","name":"rename","params":{"name":"Ada"}}'
```

For stored mutations, use
`POST /graphs/{id}/queries/{name}/if-graph-commit` with the same header.
Ordinary `/mutate`, deprecated `/change`, and `/queries/{name}` routes
reject the header, so a client cannot accidentally send a condition that is
ignored. An older server does not have the dedicated routes and therefore
returns `404`; clients must not fall back to an unconditional route.

The header must contain one raw commit id; wildcard, quoted, weak-ETag, and
comma-list forms are invalid. The condition covers the whole target branch.
If its effective head differs, the server returns `412` with
`precondition_failure { expected, actual? }` and writes nothing. Re-read and
decide again.

Successful mutation and load responses contain `commit`, the exact commit
receipt for that attempt. A successful mutation with no matching entities
returns `"commit": null`.

## Load NDJSON

`POST /graphs/{id}/load/ndjson` accepts logical node and edge records with
`Content-Type: application/x-ndjson`. The request is one bounded atomic graph
batch; it is not a durable stream or an unbounded ingestion session. Split a
larger feed into batches and wait for each response before acknowledging it
upstream.

HTTP loading defaults to `mode=merge` and branch `main`. A missing target branch
is an error unless the request supplies `from`. This differs from the CLI,
where `--mode` is always required.

See [Mutations and loading](../mutations/index.md) for the record shape and load
modes.

## Deliver Blob values

`GET` and `HEAD /graphs/{id}/blob` select a cell with `entity`, `type`, `id`,
and `property` query parameters. The route supports single byte ranges and
ETag preconditions for managed values. It reports external references without
fetching their target.

See [Blob values](../blobs.md) for examples and limits.

## Changes and baselines

`GET /graphs/{id}/commits/{commit_id}/changes` reports one commit relative to
its first parent. `GET /graphs/{id}/changes` polls complete commits on one
branch; its terminal response supplies the durable cursor. If cleanup makes a
cursor unreadable, the route returns `410 change_feed_gap`.

`POST /graphs/{id}/changes/baseline` streams an entity snapshot followed by a
terminal snapshot commit and resume cursor. See
[Changes and Change Feeds](../branching/changes.md) for pagination,
checkpointing, and recovery.

## Errors and retries

Application errors are JSON and preserve a stable HTTP status plus structured
details where available. Routing and request-extraction errors may be plain
responses. Admission-limit responses use `429` and include `Retry-After`.
Request/operation limits use `413`; interrupted writes that must recover use
`503`.

See [Troubleshooting](troubleshooting.md) before implementing retry logic.

## Deployment notes

Terminate TLS at a trusted reverse proxy or platform edge. Keep storage
credentials and bearer tokens in a secret manager, not cluster source files.
For S3 and Azure credential requirements, container examples, and Azure's
single-writer admission requirement, see [Deployment](../deployment.md).
