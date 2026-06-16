# Cluster Mode — Declarative Deployments

## Contents
- The model
- The loop (validate → import → plan → apply → serve)
- The config contract (`cluster.yaml` vs `~/.omnigraph/config.yaml`)
- Serving (`--cluster`, config-free bucket boot)
- Recovery cheat-sheet

The cluster control plane (omnigraph >= 0.7.0) manages a whole deployment —
graphs, schemas, stored queries, Cedar policies — as **declared files in one
directory**, converged Terraform-style. It is the **only way to serve** a
graph (the server is cluster-only); the data-plane operations in the other
references work against the cluster's graphs unchanged.

## The model

```
company-brain/
├── cluster.yaml        # the deployment: graphs, schemas, queries, policies
├── schema.pg
├── queries/*.gq
├── *.policy.yaml
├── graphs/<id>.omni    # DERIVED — created by apply, never by hand (gitignore)
└── __cluster/          # ledger + catalog + approvals — local state (gitignore)
```

```yaml
# cluster.yaml
version: 1
# storage: s3://my-bucket/clusters/company-brain   # optional — put ledger,
#   catalog, and graph roots on S3 object storage (default: this folder)
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: schema.pg
    queries: queries/    # the .gq files ARE the declaration — every `query <name>` registers
policies:
  base: { file: base.policy.yaml, applies_to: [knowledge] }  # or [cluster] for server-level
```

`queries` also accepts a file list (`[a.gq, b.gq]`) or a fine-grained
`name: { file: ... }` map. Discovery is loud: unparseable files and duplicate
names across files fail validation.

## The loop (memorize this)

```bash
omnigraph cluster validate --config .              # parse + typecheck everything
omnigraph cluster import   --config .              # one-time: create the state ledger
omnigraph cluster plan     --config .              # preview — REQUIRED reading before apply
omnigraph cluster apply    --config . --as <you>   # converge (idempotent)
omnigraph-server --cluster . --bind 127.0.0.1:8080 --unauthenticated  # serve (local dev)
```

- **`apply` creates graphs** at `graphs/<id>.omni` — there is no separate
  `omnigraph init` in cluster mode.
- **Schema changes**: edit the `.pg`, `plan` shows the engine's real migration
  steps (`add_property`, `drop_property [soft]`, `unsupported: …`), `apply`
  migrates the live graph. **Soft drops only** — data-loss migrations are not
  reachable from cluster apply (prior versions retain dropped columns).
- **Applied = serving on the next server restart.** No hot reload.
- **`storage: s3://bucket/prefix`** (optional) puts the entire cluster — state
  ledger, lock, content-addressed catalog, recovery sidecars, approval
  artifacts, and the derived graph roots (`<storage>/graphs/<id>.omni`) — on
  S3-compatible object storage. The ledger CAS uses S3 conditional writes and
  the lock becomes genuinely cross-machine. Absent, everything defaults to the
  config directory (byte-compatible with pre-existing clusters). Credentials
  come from the standard `AWS_*` env contract, never `cluster.yaml`.
- **`--as <actor>` attributes every run** (sidecars, audit, engine commits).
  Defaults from your operator config's `operator.actor`; required for `approve`.
- **Destructive changes are gated**: removing a graph from `cluster.yaml`
  blocks with `approval_required` until
  `omnigraph cluster approve graph.<id> --config . --as <you>` records a
  digest-bound approval. Any config/state drift after approving invalidates it.
- **Drift**: `cluster refresh` re-observes live graphs and marks out-of-band
  changes `drifted`; the next `apply` converges them back to the declaration.
- **Data is NOT cluster's job**: rows flow through `omnigraph load / mutate`
  against the derived roots, with branches as usual.

## The config contract (do not blur this)

| File | Owns | Read by |
|---|---|---|
| `cluster.yaml` | the deployment: graph set, schemas, stored queries, policy bindings, storage | `cluster` commands; the `--cluster` server |
| `~/.omnigraph/config.yaml` | per-operator: identity (`operator.actor`), named `servers:`, output defaults, personal aliases | data-plane CLI commands (tokens live in `~/.omnigraph/credentials` via `omnigraph login`) |

Cluster commands read the operator config for **exactly one thing**: the actor
default when `--as` is omitted (`--as` > `operator.actor`). A `--cluster` server
reads it for **nothing** — boot from cluster state XOR the operator file, never
a merge.
Address a cluster-managed graph's data directly with `--store <storage>/graphs/<id>.omni`,
or via `--server`/aliases against a serving instance — that is ergonomics, not
coupling.

## Serving

`omnigraph-server --cluster <dir>` is exclusive (cannot combine with a URI,
`--target`, or `--config`), always multi-graph (`/graphs/{id}/...`), and
fail-fast: missing/pending/tampered state refuses boot with a remedy. Every
declared query is exposed (`GET /graphs/<id>/queries`, `POST
/graphs/<id>/queries/<name>`); Cedar bundles attach via `applies_to`
(`cluster` → server-level gate incl. `graph_list`; `graph.<id>` → that
graph's gate incl. `invoke_query`). Bearer tokens and bind stay process-level
(env/flags).

**Config-free serving.** `--cluster` also accepts the storage-root URI
directly — `omnigraph-server --cluster s3://bucket/prefix` boots from the
applied revision on the bucket with **no checkout of the config repo**. The
ledger and catalog on the bucket are the whole deployment artifact; policy
bundles serve as digest-verified content from the catalog. The preferred
container shape is **bucket, no volume** (AWS ECS / Railway recipes in the
omnigraph repo's `docs/user/deployment.md`). For a mounted config directory
instead, `OMNIGRAPH_CLUSTER=<dir>` works and the image ships the CLI for
in-container `cluster apply`.

## Recovery cheat-sheet

| Symptom | Fix |
|---|---|
| Apply crashed mid-run | run `cluster apply` again — sidecars + sweep reconcile |
| Held lock | `cluster status` (shows lock id) → `cluster force-unlock <LOCK_ID> --config .` |
| Lost/corrupt `state.json` | `cluster import` rebuilds from config + live graphs, then `apply` |
| Server refuses to boot | the error names its remedy (usually `cluster refresh` + `apply`, restart) |
| `approval_stale` warning | re-run `cluster approve` — the plan changed since you approved |

Full reference: the omnigraph repo's `docs/user/clusters/index.md` (operator guide)
and `docs/user/clusters/config.md` (every key, flag, and diagnostic).
