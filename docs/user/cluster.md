# Operating an OmniGraph Cluster

This is the operator's guide to the cluster control plane: how to go from an
empty directory to a served deployment, and how to run it day to day —
evolving schemas, rotating queries and policies, healing drift, approving
destructive changes, and recovering from crashes.

It is a **how-to**. The reference for every `cluster.yaml` key, command flag,
state-file field, and diagnostic code is
[cluster-config.md](cluster-config.md); the HTTP surface is
[server.md](server.md).

## The model in one paragraph

You declare the entire deployment — graphs, schemas, stored queries, Cedar
policies — as files in one directory (`cluster.yaml` plus the `.pg`/`.gq`/
`.yaml` files it references). `cluster apply` converges reality to that
declaration and records what it did in a state ledger
(`__cluster/state.json`); `cluster plan` previews exactly what apply would
do, including real schema-migration steps. A server started with
`omnigraph-server --cluster <dir>` serves what was applied — never what is
merely written in config. Terraform users will recognize the shape: config
is desired state, the ledger is recorded state, plan is the diff, apply is
the only thing that changes the world, and irreversible changes require an
explicitly recorded approval.

## 1. Deploy a cluster from zero

Lay out a config directory:

```
company-brain/
├── cluster.yaml
├── people.pg            # schema for the "knowledge" graph
├── queries/             # stored queries — the .gq files ARE the declaration
│   └── people.gq
└── base.policy.yaml     # a Cedar policy bundle
```

```yaml
# cluster.yaml
version: 1
metadata:
  name: company-brain
graphs:
  knowledge:
    schema: people.pg
    queries: queries/            # every `query <name>` in queries/*.gq registers
policies:
  base:
    file: base.policy.yaml
    applies_to: [knowledge]      # graph-bound; use [cluster] for server-level
```

Bring it to life:

```bash
omnigraph cluster validate --config company-brain   # parse + typecheck everything
omnigraph cluster import   --config company-brain   # create the state ledger
omnigraph cluster plan     --config company-brain   # preview: what would apply do?
omnigraph cluster apply    --config company-brain   # converge
```

That single `apply` **creates the graph** (at the derived root
`company-brain/graphs/knowledge.omni`), applies its schema, and publishes
the query and policy into the content-addressed catalog
(`__cluster/resources/…`). The output lists every change with its
disposition; `converged: true` means there is nothing left to do — re-running
`apply` is always safe and idempotent.

Load data through the normal graph plane (the control plane manages
*definitions*, not rows):

```bash
omnigraph load --data seed.jsonl company-brain/graphs/knowledge.omni
```

Serve it:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-reader":"s3cret"}' \
  omnigraph-server --cluster company-brain --bind 0.0.0.0:8080
```

`--cluster` is an **exclusive boot source**: it cannot be combined with a
graph URI, `--target`, or `--config`, and `omnigraph.yaml` is never read in
this mode. Routing is always multi-graph:

```bash
curl -H 'authorization: Bearer s3cret' \
  -X POST http://localhost:8080/graphs/knowledge/queries/find_person \
  -H 'content-type: application/json' -d '{"params":{"name":"Ada"}}'
```

Bearer tokens and the bind address are deliberately *not* cluster facts —
they are per-replica, set by flag or environment
([server.md](server.md#modes) for the token sources).

## 2. The day-2 loop: edit → plan → apply → restart

Every change follows the same loop, whatever its kind:

```bash
$EDITOR company-brain/people.pg          # or any .gq / policy / cluster.yaml edit
omnigraph cluster plan  --config company-brain
omnigraph cluster apply --config company-brain --as andrew
# restart cluster-booted servers to pick it up
```

`--as <actor>` attributes the run: it is recorded in recovery sidecars and
audit entries and threaded into the engine's commit history. Set
`cli: { actor: <you> }` in your per-operator `omnigraph.yaml` to make it the
default when `--as` is omitted (the flag always wins; `approve` requires one
of the two).

What each change kind does:

| You edit | Plan shows | Apply does |
|---|---|---|
| a `.gq` file or `queries:` entry | `Update query.<g>.<n>` | publishes the new content-addressed blob, updates the ledger |
| a policy file | `Update policy.<n>` | same — new blob, ledger update |
| a policy's `applies_to` | `Update policy.<n> [bindings]` | records the new bindings (the file digest is unchanged; bindings are first-class changes) |
| a `.pg` schema | `Update schema.<g>` **with the real migration steps embedded** | runs the engine's schema apply on the live graph — soft drops only, sidecar-fenced |
| `graphs:` gains an entry | `Create graph.<g>` (+ schema, queries) | initializes the graph at its derived root; dependents apply in the same run |
| `graphs:` loses an entry | `Delete graph.<g>` — **blocked, `approval_required`** | nothing, until approved (see §4) |

Two properties worth internalizing:

- **One apply, ordered correctly.** Creates run first, then schema
  migrations, then catalog writes, then (approved) deletes — so a schema
  change plus a query that uses the new field converge together in one run.
- **Soft drops only.** A removed schema property disappears from the current
  version while prior versions retain the data (reversible until `cleanup`).
  Data-loss migrations are not reachable from cluster apply.

Read the plan before applying when the change is non-trivial — for schema
updates it embeds the engine's actual migration plan (`add_property`,
`drop_property [soft]`, `unsupported: …`), so you see data impact before
anything runs.

## 3. Inspect: status, refresh, drift

```bash
omnigraph cluster status  --config company-brain --json   # ledger only, read-only
omnigraph cluster refresh --config company-brain          # re-observe live graphs
```

`status` never touches the graphs; `refresh` opens them read-only and
records what it finds — manifest versions, live schema digests, catalog blob
integrity. If someone changed a graph behind the control plane's back (a
direct `omnigraph schema apply`, a tampered catalog file), refresh marks the
resource **`drifted`**.

**Drift is converged, not just reported.** After a refresh records drift,
the next `plan` proposes migrating the live graph back to the declared
schema — with the steps visible, including the soft drops of out-of-band
fields — and `apply` executes it like any other change. If the out-of-band
change is the one you want, change the *config* to match instead, and apply
converges the ledger.

## 4. Destructive changes: the approval gate

Removing a graph from `cluster.yaml` never executes silently:

```bash
omnigraph cluster apply --config company-brain
#   Delete graph.scratch [Blocked: approval_required]

omnigraph cluster approve graph.scratch --config company-brain --as andrew
#   cluster approve: delete graph.scratch approved by andrew (approval 01KT…)

omnigraph cluster apply --config company-brain --as andrew
#   Delete graph.scratch [Applied]   ← root removed, subtree tombstoned
```

The approval artifact (`__cluster/approvals/<id>.json`) is **digest-bound**:
it authorizes exactly the change you saw when you approved it. Any config or
state movement afterwards invalidates it automatically (`approval_stale`
warning) — a stale approval can never authorize a different delete. One
approval covers the graph's whole subtree (its schema and queries ride
along). Consumed artifacts are kept (rewritten with `consumed_at`) and
summarized in the ledger's `approval_records`, so the audit trail of *who
approved what* survives the loss of either store.

## 5. When things go wrong

**Crashes are designed for.** Every graph-moving operation (create, schema
apply, delete) writes a recovery sidecar before acting. If an apply dies
mid-run, the next state-mutating command sweeps the sidecars and reconciles
— rolling the ledger forward when the operation completed on the graph,
retiring stale intent when nothing moved, and flagging anything it cannot
verify. You generally fix a crashed run by **running `cluster apply`
again**.

**A held lock** (a crashed process left `__cluster/lock.json`):

```bash
omnigraph cluster status --config company-brain      # shows the lock holder + id
omnigraph cluster force-unlock <LOCK_ID> --config company-brain
```

Force-unlock requires the exact lock id (from status) — there is no blind
unlock.

**A lost or corrupted state ledger**: the cluster is self-describing.
`cluster import` rebuilds `state.json` from the config plus read-only
observation of the live graphs; the next `apply` re-converges onto the same
content-addressed catalog.

**A server that refuses to boot** with `--cluster` is telling you the
applied revision is not safely servable. Each refusal names its remedy:

| Boot error | Meaning | Remedy |
|---|---|---|
| `cluster_state_missing` | no ledger | `cluster import`, then `apply` |
| `cluster_recovery_pending` | interrupted operation awaiting sweep | run `cluster apply` (or any state-mutating command), restart |
| `catalog_payload_missing` / `…_digest_mismatch` | catalog blob lost or tampered | `cluster refresh`, then `apply`, restart |
| `policy_bindings_missing` | ledger predates binding metadata | re-run `cluster apply` (backfills), restart |
| `cluster_empty` | applied revision has no graphs | apply a cluster with ≥1 graph |
| multiple bundles bind one scope | serving holds one policy bundle per graph + one server-level | split or merge bundles |

A held *state lock* is deliberately **not** a boot error — the server reads
the atomically-replaced ledger without locking, so serving never contends
with an in-flight apply.

## 6. Deployment patterns

- **Replicas**: any number of `--cluster` servers can serve the same config
  directory; boot is read-only. Roll out a change by `apply` once, then
  restarting replicas (serving is static per process — there is no hot
  reload yet). Container/cloud recipes (AWS ECS+EFS, Railway volumes):
  [deployment.md](deployment.md#cluster-mode-in-containers-aws-railway).
- **The directory is the deployable unit**: config, catalog, ledger,
  approvals, and graph data all live under it. Back it up as a whole;
  version the *config files* (not `__cluster/` or `graphs/`) in git.
- **CI-driven convergence**: `validate` and `plan --json` are read-only and
  safe in pipelines; gate `apply --as ci` on plan review. Approvals are the
  human step by design — keep `cluster approve` out of automation.
- **`omnigraph.yaml` still has a job**: per-operator settings — your
  `cli.actor` default for `--as`, CLI defaults, credentials, and data-plane
  ergonomics (point `graphs.<name>.uri` at a derived root like
  `company-brain/graphs/knowledge.omni` to use `--target <name>` for
  loads). It just no longer describes the deployment — a server boots from
  one source or the other, never a merge of both.

## What the control plane does not do (yet)

- **No hot reload** — applied changes serve on the next restart.
- **No S3-hosted cluster directories** — the config dir, ledger, catalog,
  and derived graph roots are local-filesystem paths today. (Individual
  *graphs* on S3 are a server feature outside cluster mode.)
- **No data operations** — rows move through `omnigraph load / ingest /
  mutate` against the graph roots, with branches and merges as usual.
- **Stored-query exposure is all-or-nothing per cluster** — every applied
  query is listed and invokable (subject to Cedar `invoke_query`); per-query
  exposure policy is a planned phase.
- **Pipelines (ETL)** are a separate project; the `pipelines:` key is
  reserved and rejected loudly.

For the full reference — every key, flag, status, disposition, and
diagnostic — see [cluster-config.md](cluster-config.md).
