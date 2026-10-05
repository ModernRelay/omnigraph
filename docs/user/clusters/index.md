# Operating a cluster

An OmniGraph cluster is a declarative bundle of graphs, schemas, stored queries
and authorization policies. Apply converges graph inventory, schemas, queries,
policies and provider/Blob settings. Submit to the running server to activate
changes without a restart.

Use a cluster for a multi-graph server or shared operational configuration. For
one local graph, the [quickstart](../quickstart.md) is simpler.

## Create a bundle

```text
company-brain/
├── cluster.yaml
├── knowledge.pg
├── queries/
│   └── people.gq
├── cluster.policy.yaml
└── graph.policy.yaml
```

```yaml
version: 1
metadata:
  name: company-brain
graphs:
  knowledge:
    schema: knowledge.pg
    queries: queries/
policies:
  cluster-access:
    file: cluster.policy.yaml
    applies_to: [cluster]
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge]
```

Paths are relative to `cluster.yaml`. Its configuration version is independent
of the deployment ledger version. The [configuration reference](config.md)
covers storage roots, embedding providers, external Blob policy and limits.

For server-owned deployment, the applied policy must grant the operator
`config_manage` at cluster scope. Schema changes additionally require `read` and
`schema_apply` on the affected graphs. Deployment status reveals management
metadata under `config_manage`; it does not require data access on unrelated
graphs. New graphs need suitable declared policies too.
The server derives the actor from its bearer token; `--as` is for direct access.

## Bootstrap a cluster

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice --json
```

Fresh apply creates the deployment ledger and declared graphs. It captures all
source bytes before execution, prints the original `Deployment-ID`, and records
exact outcomes. It does not load rows; use `load` or `mutate` for data changes.

Direct apply retains its admission lock after completion. Establish that the
owner and its accepted I/O have settled, then follow
[ownership transfer](../deployment.md#writer-topology) using the exact printed
lock ID before starting the server:

```bash
omnigraph --cluster file:///srv/company-brain cluster force-unlock '<LOCK_ID>'
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server --cluster file:///srv/company-brain --bind 0.0.0.0:8080
```

Use the actual root printed by apply. A directory boot resolves its storage root
through `cluster.yaml`; a root URI boots directly from applied resources. Editing
local files alone never changes serving behavior. See
[HTTP server](../operations/server.md) for authentication and routes.

## Deploy without restarting

Edit and validate the bundle, then submit it to the running owner:

```bash
omnigraph cluster validate --config ./company-brain
OMNIGRAPH_BEARER_TOKEN='secret' omnigraph cluster apply \
  --server https://graph.example.com --config ./company-brain --json
```

With `--server`, omitted `storage` binds the bundle to the selected server’s
canonical root. An explicit absolute `storage` must match that root; relative
storage paths refuse. The CLI reads only local source files and needs no local
mount or storage credentials for the server’s root.

The CLI prints the deployment ID before submission. The server keeps its PID,
listener and writer ownership. It briefly closes admission on affected graphs,
finishes their admitted requests, publishes schema changes, and activates
matching schemas, queries and runtime permissions together. Unaffected graphs
keep serving.
Graph additions become available through the same deployment.

The response separates the durable deployment result from `active`, which means
that result is currently serving in this process. A successful response requires
both convergence and activation. Each graph publishes atomically; deployment
across multiple graphs is not one transaction. Query-only changes create no graph
commit and also work with multiple branches. Schema changes remain main-only
and require a single live branch.

Policies, provider definitions and graph bindings, and external-Blob rules can
change on existing graphs. Current permissions authorize the deployment; proposed
permissions cannot authorize themselves. Provider changes do not re-embed stored
vectors. Roots, format and credential/trust configuration stay fixed.
A refusal before effects restores unchanged serving views, including after a
drain timeout. There is one deployment protocol and no legacy execution fallback.

## Direct deployments and conversion

Without `--server`, apply executes under its own exclusive admission and requires
the serving owner to have stopped and handed off the lock. Start the server after
settlement to activate the applied revision. Direct apply never takes over a live
server or writes around its lock.

An existing v1 ledger requires explicit conversion before ordinary operation:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster upgrade-ledger --writers-stopped --json
```

Stop serving, writers and maintenance and establish prior graph/control I/O
quiescence first. Conversion preserves rows, graph identities, branches, history
and applied resources. It does not reset graphs, replay old work or convert graph
storage formats. There is no automatic migration or v1 execution fallback.
Unsupported formats and unresolved legacy work refuse conversion.

A completed read-only preflight refusal releases a newly acquired direct lock.
Accepted work, cancellation and uncertain effects retain it for reconciliation.
`--as` labels a storage-owning operator; installed graph policies still govern
schema effects. It is not remote authentication.

## Inspect and recover a deployment

A lost connection does not mean failure. Observe the original ID:

```bash
OMNIGRAPH_BEARER_TOKEN='secret' omnigraph cluster status \
  --server https://graph.example.com --deployment-id '<DEPLOYMENT_ID>' --json
```

Status is read-only. `active` is false for an older result after a newer revision
activates, and a recorded witness from another server process does not prove
current activation. Repeating apply with the same ID and identical captured input
returns its recorded outcome; it never executes again. Different input refuses.

If the server stopped with an outstanding deployment, inspect the storage root:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster status --deployment-id '<DEPLOYMENT_ID>' --json
```

Establish prior-owner and accepted-I/O quiescence, exclude concurrent admissions
and unlocks, then reconcile that exact ID:

```bash
omnigraph --cluster file:///srv/company-brain cluster force-unlock '<LOCK_ID>'
omnigraph --cluster file:///srv/company-brain \
  cluster apply --deployment-id '<DEPLOYMENT_ID>' --writers-stopped --json
```

Reconciliation uses captured input and exact publication evidence; it never
replays an uncertain schema or graph-creation invocation. Work that never
started is recorded as not attempted. A partial graph birth can be abandoned
only when its exact unpublished, empty artifacts are proved to belong to that
attempt. A foreign or committed graph is never reset. Unknown outcomes stay
outstanding and block new writes.

A settled partial result permits a corrective successor from achieved state;
it does not roll back graphs that committed. Recovery retains a new admission
lock, so perform ownership transfer before starting another owner. Never
allocate a new ID to retry an unknown outcome. Result eviction reports acceptance
and outcome as unknown; it never authorizes replay. See [limits](config.md#limits).

## Correct deliberate schema drift

Every affected graph is checked against its achieved schema identity, even when
source text changes. A recreated graph or out-of-band schema change refuses with
`applied_schema_drift`. To accept a reviewed observed contract, save the exact
graph-to-contract JSON map from that refusal and submit:

```bash
omnigraph cluster apply --config ./company-brain --as act-alice \
  --schema-correction correction.json --json
```

The same option is available with `--server`. It must match the observed source
hash, accepted IR hash, identity domain and version, and requires schema-apply
permission. Unknown, stale or unnecessary entries refuse. Correction accepts the
identified current graph; it does not restore missing data or history. An
original-ID resubmission must retain the same correction input.

## Explicit lifecycle and repair

Use the same bounded JSON file with `cluster plan --lifecycle FILE` and
`cluster apply --lifecycle FILE` (also accepted with `apply --server`). Plan
refusals name the exact observed confirmations to inspect and copy. Apply checks
those observations again; a stale confirmation fails before effects.

| JSON field | Meaning |
|---|---|
| `delete_graphs` | Map graph IDs to `{ "contract": <observed contract>, "graph_manifest_version": <observed version> }`; remove those graphs from the desired configuration |
| `adopt_graphs` | Same confirmation shape; declare an existing graph at its derived root with its exact current schema |
| `recreate_graphs` | Map missing graph IDs to their previous achieved schema contracts; the entire root must be absent |
| `repair_catalog` | Array of exact policy or stored-query resource addresses to repair from verified source bytes |
| `schema_corrections` | The exact correction map described above; cannot combine this file with `--schema-correction` |

Removal makes the graph unavailable through the cluster but **retains its
storage, rows, branches and history**. Reuse requires explicit adoption. The
running server retains removed graph owners for safe readoption; active and
retained roots together are limited to 2,048 per process. Restart releases
retained owners. Recreation refuses while that process still owns the old
engine; a graph blocked at startup because its root is missing can be recreated
online.
Recreation creates an empty graph with a new identity; it never restores lost
data or overwrites partial storage. Adoption preserves existing identity/history;
apply schema changes in a subsequent deployment.

For example, to restore one corrupt stored-query payload:

```json
{"repair_catalog": ["query.knowledge.people"]}
```

Use the actual resource address from plan/observe. Repairing an unreadable
applied policy requires direct storage ownership and source bytes matching its
recorded digest; a remote caller cannot grant itself authority from new files.
Plan runs effect-free engine and ledger checks and reports unavailable affected graphs,
schema drift and migration restrictions as errors. An unavailable unrelated
graph does not block an independent change. Live apply additionally validates
provider secrets and serving settings on the server.

## Operational boundaries

- One mutation-capable process owns a cluster. Online deployment runs inside it;
  direct maintenance requires an ownership handoff.
- Object-storage clusters may boot directly from `s3://bucket/prefix` or
  `az://container/prefix`; source files are not needed for serving or recovery.
- Azure remains a qualification preview. Its running server retains the
  mandatory [admission wrapper](../deployment.md#azure-blob-preview) during HTTP
  deployment; submission does not acquire a second storage-writer lease.
- A schema drop removes data from the branch head without reclaiming storage.
  Retained historical commits remain readable until cleanup removes them.
