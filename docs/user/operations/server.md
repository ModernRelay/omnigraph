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

An applied empty cluster can serve too: `/readyz` reports its actual applied
digest, ledger revision and CAS with zero served and quarantined graphs.
Authorized `GET /graphs` returns an empty inventory; graph requests still
require an existing graph. No default graph is created. Missing or unapplied
state refuses startup, as does a nonempty cluster whose graphs all fail.
Authentication, policy and managed data-token root checks still apply.

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
on a trusted development network. Static tokens without a policy allow only the
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

Signed credentials use the actor `principal:<immutable-principal-id>`. A caller
cannot change its actor through request headers or JSON. The server accepts
two explicit profiles:

- **Identity credentials (version 2)** bind the principal to the cluster and
  contain no permissions. Applied Cedar policy decides graph operations;
  missing policy or an unknown policy actor denies protected access.
- **Legacy restricted credentials (version 1)** additionally limit access to
  their exact graph/action grants. Both the grant and applied policy must
  allow the request. These credentials cannot grant `schema_apply`,
  `config_manage`, or `admin`.

Every valid identity credential can call `GET /graphs/discovery` for graph IDs
and display names from the server's applied inventory, including quarantined
graphs. Display names currently equal graph IDs. This route returns no storage
locations, availability, schema, query definitions, or graph data, and does not
require policy membership. It accepts neither static nor restricted
credentials. `GET /graphs` remains a separate metadata catalog requiring
`graph_list` policy permission; restricted credentials also filter it to
graphs with a signed `graph_list` grant. Discovery does not make an unavailable
server reachable or grant access to a listed graph.

See [managed data access](../cli/managed-data.md) for issuance and CLI discovery.

Tokens live for 60–86,400 seconds from issuance. The server permits an issuance
clock up to 30 seconds ahead, so at most 86,430 seconds can remain on admission.
Expiry has no grace period. Logout or a permission change at the issuer does
not revoke an issued token; already accepted operations can finish after
expiry. Stored-query calls need `invoke_query` plus `read` or `change` for the
body. An applied policy change takes effect on the next request after server
activation, using the same identity credential. Schema changes still use
`cluster apply` and its [current-policy authorization](policy.md#actions);
the identity credential supplies no permission or ownership bypass.

Static credentials can coexist for operator recovery. An exact configured
static credential keeps its existing authority, including credentials with
dots; an invalid signed credential never falls back to static or anonymous
access. Restart to change public trust. Install new and old keys together
before issuing with a new key, and retain the old key for at least 86,430
seconds after its final issuance before removing it with another restart.

### OIDC resource identities and MCP

Enable OIDC with a public admission file alongside signed or static credentials:

```bash
omnigraph-server --cluster s3://company-data/company-brain \
  --oidc-identity-trust /run/omnigraph/provider-access.json
```

The file binds an exact HTTPS issuer, resource audience, organization, account,
cluster incarnation and canonical root. Public RSA keys verify RS256 tokens;
explicit subject mappings select stable `principal:<id>` actors. Tokens carry
no graph permissions: applied Cedar governs graph and schema operations, while
every admitted identity can discover graph IDs and names.

Human access tokens must name exactly one configured resource audience, the
configured organization and an admitted subject, and expire within 300 seconds
of issuance. OAuth-client ID tokens, delegated or impersonated credentials and
unqualified machine identities refuse. Every refresh must request the exact
resource again and check the returned audience.

Supply at most four RSA keys, 1,000 subject mappings and 256 KiB per public file.
The [resource identity proposal](../../rfcs/0064-identity-credentials-and-applied-policy.md#proposed-provider-native-access-and-standard-clients)
defines the versioned format. Authority is held by whoever can publish this
file; it contains no provider secret or graph policy. Protect its filesystem
permissions and publish complete updates atomically.

Boot validates identity and root before opening graphs. Local refresh runs every
five seconds: the binding stays fixed, revisions increase, and equal revisions
require identical bytes. Every request checks the original snapshot deadline,
at most 300 seconds after capture; invalid updates cannot extend it. Requests
never fetch keys or call a control service, so publisher outages eventually
prevent new OIDC access; accepted operations can finish. Admission refresh does
not restart writers. Graph configuration retains its normal activation.

With this profile configured, the server additionally exposes:

- `GET /.well-known/oauth-protected-resource`: public resource identifier,
  authorization server and supported bearer delivery, without identity lists.
- `/mcp`: Streamable HTTP MCP using the maintained Rust SDK. Authentication
  failures advertise protected-resource metadata for standard OAuth clients.

The initial MCP tools are `graphs` (IDs and names), `queries` (permitted stored
read names for one graph), and `query` (a named stored read with parameters and
an optional branch). Mutation definitions are excluded and cannot be invoked
through a read tool. These tools use the same actor and Cedar checks as HTTP
graph requests. A 30-second deadline, 16 concurrent tool calls, 64 KiB request
body and 1 MiB complete tool result bound this interface. Client cancellation
cancels the waiting tool call; it does not create a background operation.

`omnigraph_server::init_tracing()` limits `rmcp` and `rmcp::*` logging to warnings
and errors even with `RUST_LOG=trace`: verbose SDK logs contain query arguments
and results. Other targets keep their configured levels. Embedders using their
own subscriber must enforce the same SDK filter.

Requests require the resource authority or a loopback host; browsers must use
the resource origin, while native clients can omit `Origin`. The deployment
must separately supply a reachable server URL and public metadata at the
advertised resource location. Direct/static deployments without this option
keep their existing routes and do not expose MCP.

## Route families

| Route family | Purpose |
|---|---|
| `GET /healthz` | Process health |
| `GET /openapi.json` | Runtime copy of the OpenAPI document |
| `GET /graphs` | Graph metadata catalog; requires `graph_list` policy |
| `GET /graphs/discovery` | Graph IDs and display names only; requires an identity credential |
| `GET /.well-known/oauth-protected-resource` | Public OIDC resource metadata; only when OIDC trust is configured |
| `/mcp` | Stored reads and discovery over MCP; only when OIDC trust is configured |
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

`POST /query` and `POST /mutate` also serve the GQ branch statements:
`branch list` on `/query`, and `branch create`, `branch delete`, and
`branch merge` on `/mutate`. See [Branching](../branching/index.md).

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
