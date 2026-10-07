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
`config_manage` at cluster scope. Schema changes and graph deletions additionally
require `read` and `schema_apply` on the affected graphs. Direct deletion also
enforces an installed graph policy and requires an authorized `--as` actor.
Deployment status reveals management
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

### First boot from a bootstrap receipt

Administrators using the `omnigraph-cluster` library can prepare a fresh S3
cluster with `bootstrap_serving`, containing only its initial cluster management
policy. After that initializer has stopped, supply its exact receipt at first boot:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server --cluster s3://company-data/company-brain \
  --bootstrap-handoff /run/omnigraph/bootstrap-receipt.json
```

The receipt must be a regular file containing strict JSON, at most 16 KiB.
It must match the selected root and its completed empty bootstrap. The server
claims ownership once before listening; replay, mismatched state and uncertain
claims refuse. Any failure after the claim retains ownership, including invalid
trust or server settings. Investigate that exact attempt; do not retry by
removing the lock or omitting the flag.

The receipt supplies no user permissions. Configure static tokens or identity
trust normally; the applied policy controls who can create the first graph
through `cluster apply --server`. Protect the receipt and boot configuration as
administrator inputs. This path supports only first boot of a fresh S3 cluster;
it is not a restart, recovery or writer-fencing mechanism. Local and Azure roots
are unsupported, and other writers must remain excluded.

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

Preview the same local configuration through the server before applying:

```bash
omnigraph cluster plan --server production --config . --json
omnigraph cluster apply --server production --config . --timeout 1800 --json
```

The served plan lists resource changes and schema migrations, including the exact
managed root and history that a graph removal deletes. It writes nothing and keeps
serving admission open. Its ledger CAS and input digest identify an observation;
apply rechecks current authority and physical execution eligibility.

The CLI prints the deployment ID before submission. The server keeps its PID,
listener and writer ownership. It closes admission on affected graphs,
finishes their admitted requests, publishes schema changes, and activates
matching schemas, queries and runtime permissions together. Unaffected graphs
keep serving.
Graph additions and removals use the same deployment; see
[deletion semantics](#deployment-boundaries) before removing a declaration.

The response separates the durable deployment result from `active`, which means
that result's affected bindings are installed in this process. An unrelated
blocked graph does not invalidate that activation. Default apply waits for
convergence and activation by polling the original ID. `--no-wait` instead returns
after durable acceptance; drain and preparation may precede that acknowledgment.
`--timeout SECONDS` bounds caller waiting, including acceptance (default 300,
maximum 3600). Expiry exits 5 with the original ID and last observation; it does
not cancel execution or prove failure. Resume observation with:

```bash
omnigraph cluster status --server production --deployment-id ID --wait --timeout 1800 --json
```

The exact response contains `deployment`, `active` and `in_progress`. General
cluster status retains its aggregate `status` object. An authenticated submitter
can read its own durable receipt even after its deployment removes its management
permission; general status and later deployments still require current permission.
Lost submission responses are followed only by original-ID reads, never automatic
resubmission. While waiting, observation retries transient HTTP 429/503 responses
and interrupted response bodies within the same budget; malformed receipts fail immediately.
Expired receipts and unknown outcomes require investigation.

Each graph publishes atomically; deployment
across multiple graphs is not one transaction. Query-only changes create no graph
commit and also work with multiple branches. Schema changes remain main-only
and require a single live branch.

Policies, provider definitions and graph bindings, and external-Blob rules can
change on existing graphs. Current permissions authorize the deployment; proposed
permissions cannot authorize themselves. Provider changes do not re-embed stored
vectors. Roots, format and credential/trust configuration stay fixed.
A refusal before effects restores unchanged serving views, including after a
drain timeout. That timeout bounds draining admitted requests; once drained,
the server owns preparation, completion and activation through the existing
shutdown boundary. There is one deployment protocol and no legacy execution fallback.

Upgrade the CLI, server and cluster tools together. Finish outstanding deployments
with the build that accepted them before upgrading: captured inputs are exact
and are not translated into a different request. Use the stopped-ledger upgrade
command to remove obsolete completed-result runtime fields; graph data and exact
achieved receipts are preserved. The graph storage format is unchanged.

## Direct deployments and conversion

Without `--server`, apply executes under its own exclusive admission and requires
the serving owner to have stopped and handed off the lock. Start the server after
settlement to activate the applied revision. Direct apply never takes over a live
server or writes around its lock.

Use explicit stopped-ledger conversion for a v1 ledger or a v2 ledger whose
completed receipts still contain obsolete runtime activation fields:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster upgrade-ledger --writers-stopped --json
```

Stop serving, writers and maintenance and establish prior graph/control I/O
quiescence first. Conversion preserves rows, graph identities, branches, history
and applied resources. It does not reset graphs, replay old work or convert graph
storage formats. There is no automatic migration or v1 execution fallback.
For v2, conversion removes only those obsolete runtime fields, preserving the
ledger identity, next deployment sequence and exact achieved receipts. Outstanding
deployments and unsupported receipt shapes refuse; finish accepted work with
its originating build before upgrading.
Normal reads report `ledger_upgrade_required` with the stopped-upgrade command
for a recognized prior receipt; malformed or unknown state remains an error.

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
activates. Only the current runtime can prove activation; a durable completion
receipt alone cannot. Repeating apply with the same ID and identical captured input
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
replays an uncertain schema or graph-creation invocation. Unstarted schema or
graph-creation work can be recorded as not attempted. A partial graph birth can
be abandoned only when its exact unpublished, empty artifacts belong to that
attempt. A foreign or committed graph is never reset. Unknown outcomes stay
outstanding and block new writes. A later creation may reuse an empty local
directory tree left by that cleanup; files, symlinks and cloud markers still refuse.

A settled partial result permits a corrective successor from achieved state;
it does not roll back graphs that committed. Recovery retains a new admission
lock, so perform ownership transfer before starting another owner. Never
allocate a new ID to retry an unknown outcome. Result eviction reports acceptance
and outcome as unknown; it never authorizes replay. See [limits](config.md#limits).

## Deployment boundaries

`cluster apply` creates graphs, deletes removed graphs, and updates schemas,
stored queries, policies, providers and Blob rules. Removing a graph declaration
and applying is destructive: it deletes that graph's managed storage, including
all branches, retained history and managed Blob bytes. Review the deletion in
`cluster plan` and keep any required backup before applying. There is no separate
delete flag or unregister mode. External Blob source objects are not deleted.
On object stores, deletion removes the active namespace; provider version history
and retention policies may keep older objects. This is not secure erasure.

Live apply closes the affected graph to new requests and waits for its admitted
work and response bodies before deletion. Other graphs continue serving. If
deletion is interrupted, reconcile the original deployment ID; the recorded
operation resumes removal of the exact root. Editing the desired configuration
does not cancel accepted deletion, and absence during recovery does not create a
replacement graph.

Importing an existing root, recreating a missing managed graph, accepting schema
drift and repairing catalog payloads are not apply modes. Restore missing or
damaged authoritative data from a verified backup before deploying changes to
that graph.

Every present affected root is checked against its achieved schema identity,
even when source text changes. Removing an already-absent graph still records a
completed deletion; it never recreates the root. An out-of-band replacement or schema change refuses with
`applied_schema_drift`; matching text cannot authorize a different graph identity.

Plan always observes without taking the cluster writer lock. It reports the
existing owner and runs the same effect-free preparation used by apply, with
the intended actor. Its migration steps come from that preparation, not a second
schema-file read. Apply prepares again under writer admission because a preview
reserves nothing. Unavailable affected graphs, schema drift and migration
restrictions are errors, except that deletion accepts an already-absent root. An unavailable unrelated graph does not block an
independent change. Live apply also validates provider secrets and serving
settings on the server.

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
