# Run a Cluster on S3

This guide takes a cluster from a local config directory to a server that boots
**config-free from an object-storage bucket** — the bucket is the whole
deployment artifact. For the full control-plane reference, see
[operating a cluster](../clusters/index.md) and
[cluster config](../clusters/config.md).

## 1. Declare the cluster

Lay out a config directory. The one S3-specific line is `storage:` — it puts the
state ledger, catalog, and graph data on the bucket instead of in the folder:

```
company-brain/
├── cluster.yaml
├── people.pg
├── queries/
│   └── people.gq
└── base.policy.yaml
```

```yaml
# cluster.yaml
version: 1
storage: s3://my-bucket/clusters/company-brain   # the deployment lives here
metadata:
  name: company-brain
graphs:
  knowledge:
    schema: people.pg
    queries: queries/
policies:
  base:
    file: base.policy.yaml
    applies_to: [knowledge]
```

Set the S3 credentials in the environment (for a non-AWS S3-compatible store such
as MinIO or RustFS, also set `AWS_ENDPOINT_URL_S3`):

```bash
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=us-east-1
# export AWS_ENDPOINT_URL_S3=https://...   # non-AWS S3-compatible stores
```

## 2. Validate, plan, apply

`apply` is the only command that changes the world; `plan` previews it:

```bash
omnigraph cluster validate --config company-brain   # parse + typecheck
omnigraph cluster import   --config company-brain   # create the state ledger
omnigraph cluster plan     --config company-brain   # preview the diff
omnigraph cluster apply    --config company-brain   # converge onto the bucket
```

`apply` creates the graph at the derived root
(`s3://my-bucket/clusters/company-brain/graphs/knowledge.omni`), applies its
schema, and publishes the query and policy into the content-addressed catalog.
`converged: true` means there is nothing left to do — re-running `apply` is always
safe.

## 3. Load data

The control plane manages *definitions*; rows go through the normal data plane.
Address the graph by its storage URI (the derived `graphs/<id>.omni` root):

```bash
omnigraph load --data seed.jsonl --mode overwrite \
  s3://my-bucket/clusters/company-brain/graphs/knowledge.omni
```

## 4. Serve config-free from the bucket

A serving host needs only the storage-root URI and credentials — no checkout of
the config repo:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-reader":"s3cret"}' \
  omnigraph-server --cluster s3://my-bucket/clusters/company-brain --bind 0.0.0.0:8080
```

The server boots from the **applied revision** recorded in the ledger — never from
config that was merely written. Roll out a change by `apply`-ing again, then
restarting replicas.

## 5. Maintain it

Storage maintenance runs out-of-band, addressed by cluster + graph name (it
resolves the graph's storage URI from the served state):

```bash
omnigraph optimize --cluster company-brain --graph knowledge
omnigraph repair   --cluster company-brain --graph knowledge          # preview uncovered drift
omnigraph cleanup  --cluster company-brain --graph knowledge --keep 10 --confirm
```

See [maintenance](../operations/maintenance.md) for what each command does.
