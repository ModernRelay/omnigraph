# Operating a cluster

An OmniGraph cluster is a declarative bundle of graphs, schemas, stored
queries, and authorization policies. Operators edit the bundle, preview the
change, apply it, and restart serving processes to activate the new revision.

Use a cluster when you need a multi-graph server or a shared operational
configuration. For one local graph, the [quickstart](../quickstart.md) is
simpler.

## Create a bundle

```text
company-brain/
├── cluster.yaml
├── knowledge.pg
├── queries/
│   └── people.gq
└── graph.policy.yaml
```

```yaml
# company-brain/cluster.yaml
version: 1
metadata:
  name: company-brain

graphs:
  knowledge:
    schema: knowledge.pg
    queries: queries/

policies:
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge]
```

Paths are relative to the directory containing `cluster.yaml`. The
[configuration reference](config.md) covers storage roots, embedding providers,
external Blob policy, and every supported field.

## Bootstrap and inventory changes

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster import --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice
```

- `validate` parses and type-checks the complete bundle.
- `plan` shows the difference between the desired and applied revisions.
- `apply` creates graphs, applies supported schema changes, and publishes stored
  queries and policies.

These commands initialize and manage a v1 cluster ledger. Its apply is
idempotent: rerunning after convergence leaves the applied revision unchanged.
It does not load graph data. Use `load` or `mutate` for data changes.

Directory boot reads the current `cluster.yaml` to validate the bundle location
and resolve `storage`; it then serves graph, query, and policy resources from
the applied revision. An unapplied resource edit does not become active, but a
malformed config or changed storage root can still affect startup. Start the
server after apply:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-reader":"secret"}' \
  omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

See [HTTP server](../operations/server.md) for authentication and routes.

## Durable offline deployments

Convert an existing applied cluster after stopping its server, writers and
maintenance, and establishing that earlier graph and control-store I/O has
settled. Conversion preserves graphs, rows, branches, history and applied
resources; it does not reread the source bundle:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster upgrade-ledger --writers-stopped --json
```

This explicitly enables the v2 ledger. There is no automatic conversion or
downgrade. Before conversion, complete graph inventory, policy, embedding
provider and external-Blob binding changes through the bootstrap workflow.
After conversion, deployments change only existing graphs' schemas and stored
queries. The root, inventory, policies, provider bindings and graph format stay
fixed. `approve`, `refresh`, `import` and the old apply/sweep path refuse v2.
Online activation remains unavailable; deployment requires stopped serving.

Edit, validate and preview the bundle, then submit it:

```bash
$EDITOR company-brain/knowledge.pg
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice
```

Apply captures immutable source bytes and prints `Deployment-ID`, `Cluster-root`
and `Admission lock` to stderr before acceptance and schema effects. Save all
three. One deployment may be outstanding. Each graph publishes its schema
atomically; the cluster result records which graphs actually converged. Query
updates become applied with their graph's accepted result. Query-only changes
also work on graphs with multiple branches and create no graph commit.

Successful apply retains admission. Follow
[ownership transfer](../deployment.md#writer-topology), unlock the exact lock,
then start the server to activate the applied revision. `--as` labels the
storage-owning operator; it is not a remotely authenticated identity. Installed
graph policies still govern schema effects.

## Inspect and recover a deployment

Status needs only the original root and ID, even if local files changed or were
deleted:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster status --deployment-id '<DEPLOYMENT_ID>' --json
```

For an outstanding result, establish prior-owner and accepted-I/O quiescence,
serialize the unlock with other operators, and keep new admissions excluded
until it finishes. Then reconcile the original ID:

```bash
omnigraph --cluster file:///srv/company-brain cluster force-unlock '<LOCK_ID>'
omnigraph --cluster file:///srv/company-brain \
  cluster apply --deployment-id '<DEPLOYMENT_ID>' --writers-stopped --json
```

Reconciliation never replays the original schema invocation. It records work
that never started as not attempted and uses durable publication evidence to
settle work that started. An unknown outcome stays outstanding and blocks
ordinary graph work. Reconciliation itself retains a new admission lock; query
status again and perform ownership transfer before another operation.

A terminal partial result permits a new corrective deployment from the achieved
state after exact unlock; it does not roll back graphs that committed. Submit
the corrected bundle normally. Do not allocate a new ID to retry an unknown
outcome. Repeating `apply --config ... --deployment-id ...` observes an existing
ID only when its captured input matches. Advanced callers may preallocate the
full next ID as `<ledger_ULID>:<sequence>:<nonce_ULID>` from status's ledger
identity and next sequence, adding a fresh nonce before exposing it. An already
consumed sequence can never execute again. A retained different nonce returns `identity_mismatch`; after result
eviction, `result_expired` reports both acceptance and outcome as unknown.
See [limits](config.md#limits) for the bounded result window.

## V1 lifecycle and control-state recovery

A schema drop applied through the cluster removes the data from the branch head
and reclaims no storage at apply. Older commits still read the dropped data
until `omnigraph cleanup` stops retaining them; after that it cannot be
recovered. Destructive graph deletion is blocked until an actor approves the
exact planned change:

```bash
omnigraph cluster plan --config ./company-brain
omnigraph cluster approve graph.scratch \
  --config ./company-brain --as act-alice
omnigraph cluster apply --config ./company-brain --as act-alice
```

If the declaration changes after approval, the approval no longer matches and
the delete is blocked again.

```bash
omnigraph cluster status  --config ./company-brain
omnigraph cluster observe --config ./company-brain
omnigraph cluster refresh --config ./company-brain
omnigraph cluster import  --config ./company-brain
```

- `status` reads recorded state without changing resources.
- `observe` reports what `refresh` would record, without taking the lock or
  writing anything; the output is labeled `observed` and names the ledger
  version it read. `plan --observe` does the same for a plan.
- `refresh` updates observations for an existing state record.
- `import` initializes state from declared resources when adopting an existing
  cluster.

On v1, an interrupted operator process may leave a lock. Establish prior-owner
and accepted-I/O quiescence, exclude concurrent admissions and unlocks, then copy
the exact lock ID from the diagnostic:

```bash
omnigraph cluster force-unlock <LOCK_ID> --config ./company-brain
```

Never guess a lock ID or force-unlock a live operation.

## Object-storage clusters

Set [`storage`](config.md#storage) to keep applied state and graph data under one
object-storage root, for example `s3://company-data/omnigraph/company-brain`.

An object-storage deployment can boot from the root without the source bundle:

```bash
omnigraph-server \
  --cluster s3://company-data/omnigraph/company-brain \
  --bind 0.0.0.0:8080
```

`az://container/prefix` remains a qualification preview and requires the
admission wrapper for every writer. See [Deployment](../deployment.md#azure-blob-preview).

## Operational boundaries

- Servers activate applied changes on restart; there is no hot reload.
- HTTP does not add or remove graphs. V1 apply owns inventory changes; v2 freezes inventory.
- Run only one mutation-capable writer process for a cluster. V2 adds shared cluster admission;
  Azure still requires its admission wrapper.
- Run maintenance out of band with `--cluster <root> --graph <id>`; see
  [Maintenance](../operations/maintenance.md).
