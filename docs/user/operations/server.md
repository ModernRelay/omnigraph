# HTTP server

`omnigraph-server` serves every healthy graph in one applied cluster under
`/graphs/{graph_id}/…`. It has no single-graph boot mode. Directory boot reads
the current `cluster.yaml` to resolve storage and validate the source location;
graph, query, and policy resources come from applied state. URI boot is
config-free.

The [OpenAPI document](../../../openapi.json) defines the graph API. A running
server returns it from `GET /openapi.json`, which is not self-listed.

## Start a server

From a local cluster bundle:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

For object storage, replace `./company-brain` with the cluster root, for example
`s3://company-data/omnigraph/company-brain`.

The default bind address is `127.0.0.1:8080`. `--require-all-graphs` makes any
graph startup failure fatal. Otherwise failed graphs remain in the authorized
inventory as blocked while healthy graphs serve; see [readiness](../deployment.md).

An applied empty cluster creates no default graph and serves an empty inventory.
`/readyz` reports its applied digest, ledger revision/CAS and zero graph counts.
Missing or unapplied state, or a nonempty cluster whose graphs all fail, refuses
startup. Authentication, policy and data-token root checks still apply.

Use `cluster apply --target URL --config DIR` for schema, query, policy,
provider and Blob-rule changes without restart. Graph additions and removals
use the same deployment; removal deletes the graph's managed storage and history. Direct apply requires stopped serving and a subsequent
start; see [cluster deployments](../clusters/index.md).
An unapplied resource edit does not activate it, although changing or breaking
the directory's config can change where boot looks for applied state.

For first boot of a fresh, empty S3 cluster initialized by the library, use
`--bootstrap-handoff FILE` with its exact receipt. This is a one-time ownership
claim, not a restart or recovery path; see
[first boot from a bootstrap receipt](../clusters/index.md#first-boot-from-a-bootstrap-receipt).

### Logs

The server logs to standard output at `info` unless `RUST_LOG` sets other
levels. `omnigraph_server::init_tracing()` limits three dependency targets, and
their `::` children, to warnings and errors even with `RUST_LOG=trace`: `rmcp`,
whose verbose SDK logs contain query arguments and results, and Lance's
`lance::dataset_events` and `lance::file_audit`, which at `info` name every
table's full storage URI, every file Lance creates or deletes, and each delete
predicate with its entity IDs. Other targets keep their configured levels.
Embedders using their own subscriber must enforce the same filter.

A failed Blob delivery, change-route request or streamed response is logged by
its error class, never by its message; see
[HTTP errors](troubleshooting.md#http-errors).

## HTTP contract

Upgrade the CLI, server and HTTP integrations together ([0.15 migration](../../../changelog.d/http-contract-015.breaking.md)). Every protected
request requires exactly one `Omnigraph-Http-Api: 0.15`: graph, registry and
cluster-deployment calls, including `/graphs/discovery`, JSON, streams and Blob GET/HEAD. Missing,
duplicate, combined or unsupported values return `400 api_contract_mismatch`
after authentication but before graph lookup, body decoding or execution. This HTTP identifier is
separate from package/storage versions and grants no permission. The previous `0.14` value is
refused; update integrations to the current routes, shapes and credentials before their header.

Ordinary responses, including errors and streams, carry the same header. Check
it before decoding or emitting the body. A missing/incompatible response header
means unknown effects, never permission to replay. Proxies must preserve it both ways.

Server URLs name the service root, including its proxy prefix. Select a graph
with `--graph`, `default_graph` or `alias.graph`. Migrate graph-qualified server
URLs to that root plus graph selection; the CLI never guesses a root from a path. A `unix:///absolute/path.sock` URL names a Unix-domain socket instead: its whole path is the socket, so it has no proxy prefix, and requests carry `Host: localhost`; see [Unix-socket access](../deployment.md#unix-socket-access).

Server targets bind a credential owner or anonymous access to an endpoint scope.
Every composed route must retain that captured choice. A more specific target
with another owner or anonymous access blocks the request before discovery;
the CLI never switches credentials to match the nested route.

Public `/healthz`, `/readyz` and `/openapi.json` need no header and identify the
contract in their responses. Before every graph, registry or deployment request, the CLI sends
`HEAD /healthz` to that base with a five-second bound, no bearer, body consumption
or cache. Failure prevents data dispatch. Each request adds one discovery round
trip; its existing deadline stays separate. Neither request follows redirects
or retries automatically. Older clients/servers without this contract are unsupported.

MCP/OAuth metadata neither require nor return this header; managed control-plane
APIs keep their separate protocol. The accepted [HTTP admission decision](../../rfcs/2026-09-30-v012-http-admission.md)
defines exact request/response and refusal rules.

## Authentication

Choose one static token source:

| Environment variable | Value |
|---|---|
| `OMNIGRAPH_SERVER_BEARER_TOKEN` | One bearer token; actor is `default` |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` | Actor-to-token object, e.g. `{"act-alice":"secret-a"}` |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` | Path to that JSON object |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` | Secrets Manager mapping; requires an AWS-enabled build: the container images, the release archives from v0.14.0 and every edge build, or a source build with `cargo build -p omnigraph-server --features aws` |

Send `Authorization: Bearer secret-a` (the scheme is case-insensitive). The token selects the
actor for authorization and commit attribution; clients cannot claim another actor. See
[Authorization and actors](policy.md). Every 401 carries a `WWW-Authenticate: Bearer` challenge,
with `error="invalid_token"` when the server refused the token sent.

A server with neither static tokens, signed-token trust, nor policy refuses to start unless you explicitly
pass `--unauthenticated` (or set `OMNIGRAPH_UNAUTHENTICATED=1`). Use that only
on a trusted development network. Static tokens without a policy allow only the
`read` action. Stored-query invocation, export, graph listing, writes, and other
actions remain denied.

### Assert the actor

A client that means to act as a particular actor sends `Omnigraph-If-Actor: <actor>` with any
protected request, `/mcp` included. After authentication and the contract check (on `/mcp`,
before admission), and before graph lookup or any effect, a match changes nothing and a mismatch
is `412 actor_mismatch` with nothing run; a server without authentication resolves no actor. A
repeated, padded or empty header is `400 bad_request`. The header never supplies the actor, and a
server with an incompatible HTTP contract refuses that contract first. The CLI sends it for `--as` on served
writes, served `cluster plan` and `apply`, and every [`mcp`](mcp.md#connect-a-stdio-agent) request.

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
version-2 identity credentials, which bind the principal to the cluster and
contain no permissions. Applied Cedar policy decides graph operations;
missing policy or an unknown policy actor denies protected access.

Every valid identity credential can call `GET /graphs/discovery` for applied
graph IDs and names (currently identical), including blocked graphs. It
requires no policy membership and returns no locations, availability, schema,
queries or data. Static credentials cannot use it. `GET /graphs`
requires `graph_list` permission. Discovery grants no graph access or reachability.

See [managed data access](../cli/managed-data.md) for issuance and CLI discovery.

Tokens live for 60–86,400 seconds from issuance. The server permits an issuance
clock up to 30 seconds ahead, so at most 86,430 seconds can remain on admission.
Expiry has no grace period. Logout or a permission change at the issuer does
not revoke an issued token; already accepted operations can finish after
expiry. Stored-query calls need `invoke_query` plus `read` or `change` for the
body. Apply policy changes through `cluster apply --target`; editing a source
file alone does not change permissions. Deployment uses
[current-policy authorization](policy.md#actions); the identity credential supplies no bypass.

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
The [resource identity contract](../../rfcs/2026-09-09-identity-credentials-and-applied-policy.md#provider-native-access-and-standard-clients)
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

With this profile configured, the server also serves public protected-resource metadata and names it
in every 401's challenge, so standard OAuth clients such as Claude can sign in. The [MCP guide](mcp.md)
describes sign-in, which resource identifier to choose, and the endpoint's tools, settings and limits.

## Route families

| Route family | Purpose |
|---|---|
| `GET /healthz` | Process health |
| `GET /readyz` | Replica readiness and booted applied revision |
| `GET /openapi.json` | Runtime copy of the OpenAPI document |
| `GET /graphs` | Graph metadata catalog; requires `graph_list` policy |
| `GET /graphs/discovery` | Graph IDs and display names only; requires an identity credential |
| `POST /cluster/deployments` | Submit an exact-ID deployment: graph creation/deletion or schema, query, policy, provider and Blob-binding changes |
| `GET /cluster/deployments`, `GET /cluster/deployments/{deployment_id}` | Authorized deployment status and current-process activation observation |
| `GET /.well-known/oauth-protected-resource`, also at the resource's path suffix | Public OIDC resource metadata; only when OIDC trust is configured |
| `/mcp` | The [MCP endpoint](mcp.md); when static tokens, signed data tokens or OIDC trust are configured |
| `/graphs/{graph_id}/query`, `/mutate` | Run inline GQ source |
| `/graphs/{graph_id}/mutate/if-graph-commit` | Run an inline conditional mutation |
| `/graphs/{graph_id}/queries` | List and invoke stored queries, including conditional mutations |
| `/graphs/{graph_id}/load`, `/load/ndjson` | Bounded batch loading |
| `/graphs/{graph_id}/blob` | GET/HEAD one Blob cell; PUT/DELETE to replace or clear it |
| `/graphs/{graph_id}/branches` | Branch management and merge, a merge dry run (`dry_run: true`), a branch's diff against its merge base (`GET /branches/diff`) and that merge base (`GET /branches/merge-base`) |
| `/graphs/{graph_id}/snapshot`, `/commits` | Snapshot (`internal_schema_version`, `graph_manifest_version`, and the datasets of one captured graph version), history (paged with `limit` and `page_token`), per-commit changes, and the unpublished commits of a branch (`GET /commits?listing=unpublished`, `POST /commits/{commit_id}/publish`, `DELETE /commits/{commit_id}`) |
| `/graphs/{graph_id}/changes`, `/diff` | Poll a branch feed, establish a baseline, or diff two points (branches or commit ids) |
| `/graphs/{graph_id}/schema` | Show the accepted schema |
| `GET /graphs/{graph_id}/access` | The caller's own actor and the actions its credential allows; see [Learn your access](policy.md#learn-your-access) |
| `/graphs/{graph_id}/export` | Stream a branch snapshot as JSONL |

`/query` also serves `branch list`, the branch reads (`branch diff`, `branch merge-base`, `branch merge … dry run`), `show`, and [explain](../queries/explain.md).
`/mutate` serves `branch create`, `branch delete`, `branch merge`, `commit publish`,
and `commit drop`. See [Branching](../branching/index.md).
Each of `/query`, `/mutate`, `/mutate/if-graph-commit` and `/branches/merge`
takes an optional `settings` field, and the two GET change routes a `set=`
parameter; see [Session settings](../queries/index.md#session-settings).

Change a managed graph's schema through `cluster apply --target URL --config DIR`; see
[cluster deployments](../clusters/index.md#deploy-without-restarting).

## Run an inline query

```bash
curl -sS http://localhost:8080/graphs/knowledge/query \
  -H 'Omnigraph-Http-Api: 0.15' \
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

## Invoke a stored query

```bash
curl -sS http://localhost:8080/graphs/knowledge/queries/find_person \
  -H 'Omnigraph-Http-Api: 0.15' \
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
  -H 'Omnigraph-Http-Api: 0.15' \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -H 'Omnigraph-If-Graph-Commit: <graph_commit_id>' \
  -d '{"query":"query rename($name: String) { update Person set { name: $name } where email = \"ada@example.com\" }","name":"rename","params":{"name":"Ada"}}'
```

Stored mutations use `POST /graphs/{graph_id}/queries/{query_name}/if-graph-commit` with
that header. Ordinary `/mutate` and `/queries/{query_name}`
reject it rather than ignore the condition. Never fall back to an unconditional
route after a conditional request fails.

The header must contain one raw commit id; wildcard, quoted, weak-ETag, and
comma-list forms are invalid. The condition covers the whole target branch.
If its effective head differs, the server returns `412` with
`precondition_failure { expected, actual? }` and writes nothing. Re-read and
decide again.

Successful mutation and load responses contain `commit`, the exact commit
receipt for that attempt. A successful mutation with no matching entities
returns `"commit": null`.

## Load NDJSON

`POST /graphs/{graph_id}/load/ndjson` accepts logical node and edge records with
`Content-Type: application/x-ndjson`. The request is one bounded atomic graph
batch; it is not a durable stream or an unbounded ingestion session. Split a
larger feed into batches and wait for each response before acknowledging it
upstream.

HTTP loading defaults to `mode=merge` and branch `main`. A missing target branch
is an error unless the request supplies `from`. This differs from the CLI,
where `--mode` is always required.

See [Mutations and loading](../mutations/index.md) for the record shape and load
modes, including [CSV statements on `/mutate`](../mutations/index.md#csv-loading).

## Deliver Blob values

`GET`/`HEAD /graphs/{graph_id}/blob` select a cell by `entity`, `type`, `id` and `property`,
support managed ranges/ETag conditions, and report external references without fetching
them. `PUT` and `DELETE` replace or clear the cell; see [Blob values](../blobs.md),
[Replacing one Blob value](../blobs.md#replacing-one-blob-value) and [Blob limits](../blobs.md#limits).

## Changes and baselines

`GET /graphs/{graph_id}/commits/{commit_id}/changes` compares a commit with its first
parent, `GET /graphs/{graph_id}/diff?from=&to=` any two points, and `/branches/diff`
a branch with its merge base. `GET /graphs/{graph_id}/changes` polls branch commits
and returns a durable cursor; unreadable history returns `410 change_feed_gap`.
`POST /graphs/{graph_id}/changes/baseline` streams entities, a snapshot commit and a
resume cursor ([Changes, Diffs and Change Feeds](../branching/changes.md)).

## Errors and retries

Every error body, an unknown route's and a malformed request's included, carries
a `code`, its details and a `disposition` (`effects`, `action`; see
[failed writes](troubleshooting.md#failed-data-write-commands)); a write the
server could not settle is `outcome_unknown`, with `500`. Admission limits use `429`, size limits `413`, blocked graphs or closed admission `503`, each refusal
that asks for a retry with `Retry-After`. Admitted writes continue after disconnect.

For TLS, secrets, storage credentials, container examples and Azure admission,
see [Deployment](../deployment.md).
