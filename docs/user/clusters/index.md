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

## Validate, plan, and apply

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice
```

- `validate` parses and type-checks the complete bundle.
- `plan` shows the difference between the desired and applied revisions.
- `apply` creates graphs, applies supported schema changes, and publishes stored
  queries and policies.

Apply is idempotent: rerunning it after convergence is safe. It does not load
graph data. Use `load` or `mutate` for data changes.

Directory boot reads the current `cluster.yaml` to validate the bundle location
and resolve `storage`; it then serves graph, query, and policy resources from
the applied revision. An unapplied resource edit does not become active, but a
malformed config or changed storage root can still affect startup. Restart the
server after an apply:

```jsonl
{"type":"Person","data":{"name":"Ada"}}
```

Save that record as `seed.jsonl`, then run:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-reader":"secret"}' \
  omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

See [HTTP server](../operations/server.md) for authentication and routes.

## Day-two workflow

For schemas, queries, policies, and graph declarations, use the same loop:

```bash
$EDITOR company-brain/cluster.yaml
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice
# restart each server using this cluster
```

Schema drops applied through the cluster are soft. Destructive graph deletion
is blocked until an actor approves the exact planned change:

```bash
omnigraph cluster plan --config ./company-brain
omnigraph cluster approve graph.scratch \
  --config ./company-brain --as act-alice
omnigraph cluster apply --config ./company-brain --as act-alice
```

If the declaration changes after approval, the approval no longer matches and
the delete is blocked again.

## Inspect and recover control state

```bash
omnigraph cluster status  --config ./company-brain
omnigraph cluster refresh --config ./company-brain
omnigraph cluster import  --config ./company-brain
```

- `status` reads recorded state without changing resources.
- `refresh` updates observations for an existing state record.
- `import` initializes state from declared resources when adopting an existing
  cluster.

If an interrupted operator process leaves a lock, first prove that no plan,
apply, refresh, or import is still running. Then copy the exact lock ID from the
diagnostic:

```bash
omnigraph cluster force-unlock <LOCK_ID> --config ./company-brain
```

Never guess a lock ID or force-unlock a live operation.

## Object-storage clusters

Set `storage` to keep applied state and graph data under one object-storage
root:

```yaml
version: 1
storage: s3://company-data/omnigraph/company-brain
graphs:
  knowledge:
    schema: knowledge.pg
```

An object-storage deployment can boot from the root without the source bundle:

```bash
omnigraph-server \
  --cluster s3://company-data/omnigraph/company-brain \
  --bind 0.0.0.0:8080
```

`az://container/prefix` roots are a qualification preview: implementation,
Azurite testing, and a live managed-identity smoke deployment are complete,
but the adversarial live-Azure matrix is still pending. Every mutation-capable
Azure server, apply job, direct writer, and maintenance process must run through
`omnigraph-azure-admission`. See
[Deployment](../deployment.md#azure-blob-preview).

## Operational boundaries

- Servers activate applied changes on restart; there is no hot reload.
- HTTP does not add or remove graphs. Change `cluster.yaml`, apply, and restart.
- Run only one mutation-capable writer process for a cluster unless your
  deployment provides an external writer fence. The Azure reference topology
  uses the admission wrapper for that purpose.
- Run maintenance out of band with `--cluster <root> --graph <id>`; see
  [Maintenance](../operations/maintenance.md).
