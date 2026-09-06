# CLI guide

The `omnigraph` CLI can work directly with a graph store, through a running
server, or with a cluster definition. Start with the form that matches the
resource you have:

```bash
# One graph, opened directly
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' --store ./graph.omni

# One graph on a multi-graph server
omnigraph query sources_for_claim --params '{"claim":"lower-latency"}' \
  --server prod --graph knowledge

# A reusable scope from ~/.omnigraph/config.yaml
omnigraph query sources_for_claim --params '{"claim":"lower-latency"}' \
  --profile prod-knowledge
```

Run `omnigraph <command> --help` for the flags supported by your installed
version. The [CLI reference](reference.md) summarizes addressing, commands,
configuration, and output formats.

For a managed cluster, first select it with `use` and request a scoped data
credential with `cluster token`. Then run `query` or `mutate` from that folder
with an explicit `--graph`. See [managed data access](managed-data.md)
for permissions, expiry, offline operation, and local credential clearing.

## Create, load, and query a graph

```bash
omnigraph init --schema schema.pg ./graph.omni
omnigraph load --data evidence.jsonl --mode overwrite ./graph.omni

omnigraph query sources_for_claim \
  --query queries.gq \
  --params '{"claim":"lower-latency"}' \
  --format table \
  --store ./graph.omni
```

`load` always requires a mode:

- `overwrite` replaces each node or edge type represented in the batch. Types
  absent from the batch are unchanged.
- `append` inserts new entities and rejects duplicate IDs.
- `merge` inserts new IDs and updates existing IDs.

Use `mutate` for `.gq` insert, update, and delete queries:

```bash
omnigraph mutate add_source \
  --query mutations.gq \
  --params '{"slug":"incident-review","title":"Incident review"}' \
  --store ./graph.omni
```

For a stored server query, omit `--query`; the positional name selects the
query from the server's registry:

```bash
omnigraph query sources_for_claim --server prod --graph knowledge \
  --params '{"claim":"lower-latency"}'
```

## Work with branches

```bash
omnigraph branch create review/new-data --from main --store ./graph.omni
omnigraph load --data batch.jsonl --mode merge \
  --branch review/new-data ./graph.omni
omnigraph query inspect --query review.gq \
  --branch review/new-data --store ./graph.omni
omnigraph branch merge review/new-data --into main --store ./graph.omni
```

See [Branches and commits](../branching/index.md) for isolation, history, and
merge behavior.

## Read Blob values

Read a managed Blob cell to a file or inspect its metadata:

```bash
omnigraph blob get node Document doc-42 body \
  --store ./graph.omni --out body.bin

omnigraph blob stat node Document doc-42 body \
  --store ./graph.omni --json
```

`blob get` writes bytes to stdout when `--out` is omitted. Add `--offset` and
`--length` for a range. The CLI reports external references but does not follow
them. See [Blob values](../blobs.md) for the complete contract.

## Inspect and maintain a graph

```bash
omnigraph snapshot ./graph.omni --json
omnigraph commit list ./graph.omni --json
omnigraph schema show ./graph.omni

omnigraph optimize ./graph.omni
omnigraph repair ./graph.omni                 # preview only
omnigraph cleanup --keep 10 --older-than 7d ./graph.omni
omnigraph cleanup --keep 10 --older-than 7d --confirm ./graph.omni
```

Maintenance commands open storage directly; they do not run through
`--server`. For a cluster-managed graph, address it with
`--cluster <root> --graph <id>`. Read the
[maintenance guide](../operations/maintenance.md) before repair or cleanup.

## Use a server

Declare the server once, store its token, then address graphs by ID:

```yaml
# ~/.omnigraph/config.yaml
servers:
  prod:
    url: https://graph.example.com
```

```bash
printf '%s' "$OMNIGRAPH_TOKEN" | omnigraph login prod
omnigraph graphs list --server prod
omnigraph query sources_for_claim --params '{"claim":"lower-latency"}' \
  --server prod --graph knowledge
```

The token is stored separately from `config.yaml`. A server resolves the actor
from the token; clients cannot override it with `--as`.

## Manage a cluster

Without a managed context, cluster commands read a directory containing
`cluster.yaml`:

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain
omnigraph cluster apply --config ./company-brain --as act-alice
```

They manage graph definitions, schemas, stored queries, and policies—not graph
data. See [Operating a cluster](../clusters/index.md).

For a managed cluster, log in to its Intent API and select the cluster for
your config directory:

```bash
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example --config ./company-brain
omnigraph cluster plan --config ./company-brain --json > plan.json
omnigraph cluster apply --config ./company-brain --plan "$(jq -r .data.run_id plan.json)" --json
omnigraph cluster status --config ./company-brain --json
omnigraph cluster history --config ./company-brain --json
omnigraph logout --api https://control.example
```

Commit and push external configuration before planning. The API plans its
bound head, or the pushed revision selected with `--rev`. Apply uses the exact
saved plan and your current permissions. To release an unused plan, run
`omnigraph cluster cancel PLAN_RUN_ID --config ./company-brain`; its result
remains in history and cannot be applied afterward.

The folder's `.omnigraph/context` selects the managed API. An unavailable API
or malformed context causes an error. To intentionally use the direct
`cluster.yaml` path, pass `--direct`. See [Managed cluster commands](reference.md#managed-cluster-commands)
for credential storage, automation, bounded waits, and exit codes.

## Validate source before running it

```bash
omnigraph lint --schema schema.pg --query queries.gq
omnigraph queries validate --cluster ./company-brain --graph knowledge
```

`lint` checks one `.gq` source. `queries validate` checks the applied stored
query registry for a cluster graph.

## Deprecated names

`read`, `change`, `check`, and `ingest` remain compatibility shims. New scripts
should use `query`, `mutate`, `lint`, and `load`.
