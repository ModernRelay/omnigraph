# Operating a cluster

A cluster is a declarative bundle of graphs, schemas, stored queries, policies
and provider/Blob settings. Apply the bundle to create or update the cluster.
Submit changes to its running server to activate them without a restart.
For one local graph, start with the [quickstart](../quickstart.md).

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

Paths are relative to `cluster.yaml`. See the [configuration reference](config.md)
for storage, providers, Blob rules and limits. The configuration version is
independent of the storage and deployment-ledger versions.

The applied cluster policy must grant the deploying actor `config_manage`.
Schema changes and graph deletion also require `read` and `schema_apply` on
those graphs. Proposed permissions cannot authorize their own installation.
The server resolves the actor from the bearer token; `--as` selects an actor
only for direct access. See [authorization](../operations/policy.md).

## Bootstrap a cluster

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice --json
```

Fresh apply creates the ledger and declared graphs, records exact outcomes and
prints the deployment and lock IDs. Use `load` or `mutate` to add rows afterward.

Direct apply retains its writer lock. Establish that its process and accepted
storage I/O have settled, then follow [ownership transfer](../deployment.md#writer-topology)
with the exact lock ID before starting the server:

```bash
omnigraph --cluster file:///srv/company-brain cluster force-unlock '<LOCK_ID>'
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"secret"}' \
  omnigraph-server --cluster file:///srv/company-brain --bind 0.0.0.0:8080
```

Use the actual root printed by apply. A root URI boots from applied resources;
a directory resolves its root through `cluster.yaml`. Editing source files alone
never changes serving behavior. Wait for `/readyz` before sending requests.

## Deploy without restarting

Edit the bundle, validate it, preview through the server, then apply:

```bash
export OMNIGRAPH_BEARER_TOKEN='secret'
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --server https://graph.example.com \
  --config ./company-brain --json
omnigraph cluster apply --server https://graph.example.com \
  --config ./company-brain --timeout 1800 --json
```

With `--server`, omitted `storage` binds to that server's canonical root.
Explicit storage must be an absolute matching root; relative paths refuse.
The CLI needs the source files and bearer credential, not storage credentials.
Plan changes nothing and reserves nothing; apply rechecks the current state.
**Review graph removals carefully: apply permanently deletes their managed
storage and history.** See [deployment boundaries](#deployment-boundaries).

The server closes affected graphs to new requests, drains admitted work, applies
changes and activates the matching configuration. Other graphs keep serving;
the PID, listener and writer ownership remain unchanged. Each graph publishes
atomically, but a multi-graph deployment is not one transaction.

Apply prints an ID before submission and waits for both durable completion and
activation. `--no-wait` returns after durable acceptance instead. `--timeout`
bounds caller waiting (default 300 seconds, maximum 3600); expiry exits 5 and
does not cancel execution or prove failure. Continue observing the original ID:

```bash
omnigraph cluster status --server https://graph.example.com \
  --deployment-id '<DEPLOYMENT_ID>' --wait --timeout 1800 --json
```

Exact status returns `deployment`, `active` and `in_progress`. `active` means
this result's affected bindings are installed in the current process; an
unrelated blocked graph does not invalidate it. A completed receipt can precede
activation. The authenticated submitter retains access to its exact receipt
after losing management permission; aggregate status still requires permission.

The CLI follows a lost submission response with original-ID reads, never an
automatic resubmission. Observation retries transient 429/503 responses and
interrupted bodies within its waiting budget. Malformed or expired receipts
require investigation. See [recovery](#inspect-and-recover-a-deployment).

## Direct deployments and conversion

Without `--server`, apply requires stopped serving and exclusive writer
ownership. Transfer ownership before running it, then settle and hand off its
lock before starting the server. Use this path for bootstrap and offline work.
A completed read-only preflight refusal releases a newly acquired lock;
accepted work and uncertain effects retain it.

Upgrade the CLI, server and integrations together. Finish outstanding work with
the build that accepted it. If status reports `ledger_upgrade_required`, stop
writers, establish prior graph/control I/O quiescence and run:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster upgrade-ledger --writers-stopped --json
```

Explicit conversion accepts a supported older ledger or completed receipt
shape. It preserves graph identities, rows, history, resources and exact achieved
outcomes. Outstanding work and unknown shapes refuse. It does not upgrade graph
storage; use the [upgrade guide](../operations/upgrade.md) for format changes.

## Inspect and recover a deployment

A lost connection does not mean failure. Use the original-ID server status
command above first. If the server stopped with outstanding work, inspect its
root directly:

```bash
omnigraph --cluster file:///srv/company-brain \
  cluster status --deployment-id '<DEPLOYMENT_ID>' --json
```

After establishing prior-owner and accepted-I/O quiescence, exclude competing
admissions and unlocks, then reconcile the exact ID:

```bash
omnigraph --cluster file:///srv/company-brain cluster force-unlock '<LOCK_ID>'
omnigraph --cluster file:///srv/company-brain \
  cluster apply --deployment-id '<DEPLOYMENT_ID>' --writers-stopped --json
```

Recovery uses captured inputs and publication evidence. It does not replay an
uncertain schema or graph-creation call, reset foreign data, or roll back graphs
that committed. Interrupted deletion resumes removal of its recorded root.
Unknown outcomes remain outstanding and block new writes. Never allocate a new
ID to retry them; receipt eviction also does not authorize replay.

A settled partial result permits a corrective deployment from the achieved
state. Recovery retains its lock, so complete ownership transfer before starting
the next owner. Only a running server can report activation; an older receipt's
`active` becomes false when a newer deployment activates.

## Deployment boundaries

| Change | Behavior |
|---|---|
| Add a graph | Creates a new managed root and activates its declared configuration. |
| Remove a graph | Deletes its managed root, branches, history and managed Blob bytes. |
| Schema | Publishes atomically; requires `main` to be the only live branch. |
| Stored queries | Activates the new registry; permits existing branches and creates no graph commit. |
| Policies | Current permissions authorize the change; later requests use the new rules. |
| Providers and Blob rules | Activates new bindings; provider changes do not re-embed existing vectors. |
| Roots, storage format or credential/trust configuration | Requires the corresponding offline or runtime procedure. |

Back up anything needed before deleting a graph. Removal does not delete
external Blob source objects; object-store versioning or retention may preserve
older objects. Accepted deletion cannot be canceled by editing YAML.

Apply refuses unexpected graph identities, schema drift and missing managed
roots. It cannot adopt, recreate or repair them. Restore damaged authoritative
data from a verified backup before updating that graph. Deleting an already
absent graph is allowed and does not recreate it. Unavailable unrelated graphs
do not block an independent change.

## Operational boundaries

- One mutation-capable process owns the cluster; direct maintenance requires
  [ownership transfer](../deployment.md#writer-topology).
- Source files are unnecessary for serving or recovery from an applied root.
- A refusal before effects restores unchanged serving views. The drain timeout
  bounds waiting for admitted work; it does not expire a deployment after effects.
- Azure deployments retain the mandatory [admission wrapper](../deployment.md#azure-blob-preview)
  and qualification-preview boundary.
- Dropping schema content removes it from the current head; retained historical
  data remains until cleanup. This differs from deleting an entire graph.
