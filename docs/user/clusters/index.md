# Operating an OmniGraph Cluster

This is the operator's guide to the cluster control plane: how to go from an
empty directory to a served deployment, and how to run it day to day —
evolving schemas, rotating queries and policies, healing drift, approving
destructive changes, and recovering from crashes.

It is a **how-to**. The reference for every `cluster.yaml` key, command flag,
state-file field, and diagnostic code is
[cluster-config.md](config.md); the HTTP surface is
[server.md](../operations/server.md).

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
# storage: s3://omnigraph-local/clusters/company-brain   # optional: put the
#   ledger, catalog, and graph data on object storage (default: this folder)
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

`--cluster` accepts either a **config directory** (the storage root resolves
through `cluster.yaml`'s `storage:` key) or a **storage-root URI directly**
(`--cluster s3://bucket/prefix`) — config-free serving: a serving box needs
only the URI and credentials, no checkout of the config repo. The ledger and
catalog on the bucket are the deployment artifact.

`--cluster` is an **exclusive boot source**: it cannot be combined with a
graph URI or `--config`, and `omnigraph.yaml` is never read in
this mode. Routing is always multi-graph:

```bash
curl -H 'authorization: Bearer s3cret' \
  -X POST http://localhost:8080/graphs/knowledge/queries/find_person \
  -H 'content-type: application/json' -d '{"params":{"name":"Ada"}}'
```

Bearer tokens and the bind address are deliberately *not* cluster facts —
they are per-replica, set by flag or environment
([server.md](../operations/server.md#modes) for the token sources).

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
`operator: { actor: <you> }` in your `~/.omnigraph/config.yaml` to make it the
default when `--as` is omitted (the flag always wins; `approve` requires one
of the two).

**`apply` runs out-of-band, with direct storage access — there are no server
routes for it.** Like `init`/`load` and the maintenance verbs (§7),
`cluster apply` reaches the object store directly: it reads and writes the
cluster ledger under `__cluster/` *and* opens each graph's Lance datasets to
create, migrate, or delete them. It never goes through a running
`omnigraph-server`, so the host that runs it (an operator or CI) needs storage
access — the `AWS_*` credential contract for an `s3://` cluster. This is by
design, not a missing feature: the control plane is **declarative** (config →
cluster), not a runtime mutation API on the serving process — intent lives in
the config files, outside the running system (the reasoning is
[cluster-axioms.md](../../dev/cluster-axioms.md) §3 and §4). The server only ever
*reads* the converged ledger, which is why a held apply lock never blocks
serving (see §5 below, in this guide).

What each change kind does:

| You edit | Plan shows | Apply does |
|---|---|---|
| a `.gq` file or `queries:` entry | `Update query.<g>.<n>` | publishes the new content-addressed blob, updates the ledger |
| a policy file | `Update policy.<n>` | same — new blob, ledger update |
| a policy's `applies_to` | `Update policy.<n> [bindings]` | records the new bindings (the file digest is unchanged; bindings are first-class changes) |
| a `.pg` schema | `Update schema.<g>` **with the real migration steps embedded** | runs the engine's schema apply on the live graph — soft drops only, sidecar-fenced |
| `graphs.<g>.streaming` changes | `Create/Update streaming.<g>` | only after writers are stopped: requires `--as <actor> --confirm-stream-offline` and the state lock; publishes graph-owned profile authority |
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

### Experimental streaming profile: stop → apply → restart

A streaming-profile change has a stricter process topology than an ordinary
catalog edit:

```bash
# 1. Gracefully stop every writer-capable process for the affected graph.
# 2. Apply while holding the normal state lock and attest the offline handoff.
omnigraph cluster apply --config company-brain \
  --as andrew --confirm-stream-offline
# 3. Restart the cluster server after apply exits.
```

The confirmation flag is an explicit operator attestation, not a distributed
lease. `state.lock: false` refuses a profile change, and running this apply
concurrently with a writer server is unsupported.

The apply actor must pass `stream_manage` under both the currently applied
graph policy and the desired graph policy. If only one revision binds a
policy, that policy governs; a simultaneous policy change must be allowed by
both sides until the state CAS publishes the desired revision. If one side
would deny the profile transition, split the work: grant first and change the
profile second, or change the profile first and revoke the grant second. A
blocked profile transition also blocks current- or desired-bound policy
changes for that graph, preserving the currently applied policy authority for
the retry instead of landing a simultaneous revoke.

The retained profile receipt is also bound to the original apply actor. Retry
a lost result with the same `--as` value. If that identity is unavailable
after the graph effect landed but the state CAS did not, use `cluster refresh`
to reconcile the ledger from manifest truth before replanning under another
actor; a different actor cannot adopt the original receipt directly.

In this release, `streaming: true` is not additive to the existing direct
writer surfaces: it makes embedded SDK and direct `--store`
Mutation/Load/delete fail before input reads or durable
effects. Existing served mutations work only through the restarted
cluster-booted server's checked runtime authority. Graph-native producers use
the served [`/stream/ingest` firehose](../operations/server.md#streaming), which
keeps physical datasets and lanes private. Ingest admits absent or `OPEN`
internal declarations. After disable/re-enable, run the graph-wide served
`omnigraph stream resume`; it opens every `SEALED` declaration and exposes no
per-type, table, or lane selector. Branch merge remains refused while the profile is `ENABLED` or
`DISABLING`, including through the checked server runtime. A later explicit
`streaming: false` offline apply publishes `DISABLING`, derives one finite
manifest lane cut, and serially drains `OPEN`, goal-`SEALED`, and adopted
`OPEN_AFTER_FOLD` lanes. A selected `DataBlock` leaves the apply pending until
stopped/offline correction and a retry. Only the no-lane case restores the
direct physical lane. A disabled enrolled graph remains a checked
served/export state; resume is available only after the profile is enabled and
the cluster-booted server is restarted.

The sealed window is also the supported maintenance window. After applying
`streaming: true` and restarting—but before `stream resume`—run
`stream maintenance ensure-indices` and/or `stream maintenance optimize`.
These controls operate on the whole graph through the existing coordinated
manifest/recovery paths. They do not accept a declaration selector and return
only aggregate results. Resume is convergent rather than a new all-dataset
transaction: it preflights the complete graph, then reopens internal
declarations in deterministic order; if an unexpected race interrupts the
sequence, retrying the same graph-level command skips declarations already
`OPEN` and continues the remainder.

### Strict drain blocks: inspect → correct → retry

When a strict drain reports a `DataBlock`, stop every writer-capable process
and inspect the exact blocked cut through the cluster control plane:

```bash
omnigraph --graph knowledge --as andrew \
  cluster stream block show \
  --config company-brain --block-token <token> \
  --confirm-stream-offline --json

omnigraph --graph knowledge --as andrew \
  cluster stream block correct \
  --config company-brain --block-token <token> \
  --correction-id <uuid> --expected-lifecycle-revision <revision> \
  --plan correction.json --confirm-stream-offline --json
```

`show` reconstructs validator evidence from the retained immutable WAL
generation and returns a bounded page; the opaque block token resolves the
affected internal declaration, so the user never supplies a type/table/lane
selector. Follow `next_cursor` until it is absent.
Build an ordered plan that chooses `REPLACE` or `WITHDRAW` for the entries it
changes; unmentioned keys retain their blocked winner, and the resulting
complete overlay must clear every violation. `correct` revalidates the block,
profile revision, predecessor tokens, and complete corrected overlay before
any graph effect. It then publishes the
base rows, token dispositions, lifecycle state, and graph commit together under
one correction UUID. Repeating the same actor, UUID, and plan returns the
durable receipt; reusing the UUID for a different plan is refused. The cluster
state lock plus `--confirm-stream-offline` bind the stopped-writer protocol,
but do not replace the operator's responsibility to stop all processes first.

### Current dead letters: list or export payloads

When a fold diverts a data conflict, the key's selected current token becomes
`DEAD_LETTERED`. With writers stopped, list the sequencing evidence or export
descriptor-verified canonical payloads in bounded pages:

```bash
omnigraph --graph knowledge --as andrew \
  cluster stream dead-letter list \
  --config company-brain --confirm-stream-offline --json

omnigraph --graph knowledge --as andrew \
  cluster stream dead-letter export \
  --config company-brain --confirm-stream-offline --json
```

Follow `next_cursor` with `--cursor` until it is absent. Both commands pin the
manifest-selected token version; export verifies the recovery-owned object
descriptor and does not prefix-list storage. Payload export is an inspection
artifact, not replay or import. Each entry names its logical node/edge
declaration while keeping the physical table and dataset private. A fresh ordinary stream occurrence can restore
`PRESENT` by naming the terminal token as predecessor.
The graph-native `stream ingest` command and
`POST /graphs/{graph_id}/stream/ingest` route can submit that occurrence while
the enabled lane is absent or `OPEN`; payload export itself does not replay it
automatically. If the declaration is `SEALED`, re-enable/restart the served
graph and run graph-wide `stream resume` before submitting the successor.
Retirement and rebuild remain the irreversible exit when terminal sequencing
authority must be discarded for export.

### Terminal authority retirement: plan → confirm → rebuild

If a graph has current `WITHDRAWN` or `DEAD_LETTERED` sequencing authority,
ordinary export refuses rather than silently discarding it. After stopping every writer and
reaching exact `DISABLED` with every enrolled lane `SEALED`, use the separate
cluster-only retirement handshake:

```bash
omnigraph --graph knowledge --as andrew \
  cluster stream retire-for-rebuild plan \
  --config company-brain --confirm-stream-offline --json

omnigraph --graph knowledge --as andrew \
  cluster stream retire-for-rebuild confirm \
  --config company-brain \
  --retirement-id <uuid> \
  --expected-plan-digest <sha256:...> \
  --confirm-stream-offline --json
```

The plan is read-only and also proves the state lock, applied graph mapping,
settled recovery, base/token parity, and exact frozen graph cut. Confirmation
is irreversible: it records one actor- and plan-bound receipt and makes the
source permanently read/query/status/export-only. Export then includes that
root receipt and a closed witness naming the selected frozen branch member as
provenance for loading logical rows into a fresh graph identity; it does not
transfer live sequencing authority. A graph with only `PRESENT`
tokens uses ordinary export. See the [upgrade guide](../operations/upgrade.md)
for the full procedure.

For an enrolled source, terminal state is necessary but direct storage access
is not sufficient authority. Keep `streaming: false` applied, restart
`omnigraph-server` from that exact cluster directory, and run `omnigraph export
--server <name-or-url> --graph <id> > graph.jsonl`. The server re-proves the
`DISABLED | RETIRED` cut before `200`; `RETIRED` emits its provenance first.
Discard any partial file if body streaming fails. Initialize and load a fresh
target root—never load the artifact back over the enrolled source.

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
| `cluster_recovery_pending` | graph was quarantined because an interrupted operation awaits sweep | run `cluster apply` (or any state-mutating command), restart |
| `cluster_no_healthy_graphs` | every applied graph is quarantined or failed startup | sweep/fix the graph-specific failures, then restart |
| `catalog_payload_missing` / `…_digest_mismatch` | catalog blob lost or tampered | `cluster refresh`, then `apply`, restart |
| `policy_bindings_missing` | ledger predates binding metadata | re-run `cluster apply` (backfills), restart |
| `cluster_empty` | applied revision has no graphs | apply a cluster with ≥1 graph |
| multiple bundles bind one scope | serving holds one policy bundle per graph + one server-level | split or merge bundles |

A held *state lock* is deliberately **not** a boot error — the server reads
the atomically-replaced ledger without locking, so serving never contends
with an in-flight apply.

When at least one graph is healthy, graph-attributed recovery sidecars and
graph-local startup failures do not block the whole server. The affected
graph is skipped, its graph-only policy bindings and queries are omitted,
and `/graphs` lists only the ready graphs. Pass
`omnigraph-server --require-all-graphs` or set
`OMNIGRAPH_REQUIRE_ALL_GRAPHS=1` to make any such quarantine fail startup.

## 6. Deployment patterns

- **Replicas**: any number of `--cluster` servers can serve the same config
  directory; boot is read-only. Roll out a change by `apply` once, then
  restarting replicas (serving is static per process — there is no hot
  reload yet). Container/cloud recipes (AWS ECS+EFS, Railway volumes):
  [deployment.md](../deployment.md#cluster-mode-in-containers-aws-railway).
- **The directory is the deployable unit**: config, catalog, ledger,
  approvals, and graph data all live under it. Back it up as a whole;
  version the *config files* (not `__cluster/` or `graphs/`) in git.
- **CI-driven convergence**: `validate` and `plan --json` are read-only and
  safe in pipelines; gate `apply --as ci` on plan review. Approvals are the
  human step by design — keep `cluster approve` out of automation.
- **`~/.omnigraph/config.yaml` is the per-operator config**: your
  `operator.actor` default for `--as`, named servers/clusters, credentials,
  profiles, and data-plane ergonomics (address a cluster graph by its derived
  root like `company-brain/graphs/knowledge.omni` with `--store` for loads). The
  cluster directory's `cluster.yaml` is the **sole deployment declaration** — the
  server boots from the cluster only.

## 7. Maintaining a cluster graph

Storage maintenance (`optimize` / `repair` / `cleanup`) is **not** a control-plane
operation — it runs out-of-band, with direct storage access, against the graph's
roots. Address a cluster graph by name instead of hand-typing its storage path:

```bash
omnigraph optimize --cluster ./company-brain --graph knowledge
omnigraph cleanup  --cluster ./company-brain --graph knowledge --keep 10 --confirm
# --cluster also takes the storage-root URI directly (config-free), and a
# `clusters:` name from ~/.omnigraph/config.yaml:
omnigraph optimize --cluster s3://bucket/clusters/company-brain --graph knowledge
```

The graph's storage URI is resolved from the **served cluster state** (the same
truth a `--cluster` server boots from); a graph that hasn't been applied yet is
not resolvable. Run these from a host with storage access — there are no server
routes for them. Conversely, **`init` refuses** a cluster-managed path: graphs in
a cluster are created by `cluster apply`, not by hand.

If the cluster has exactly **one** applied graph you can omit `--graph` — it is
used automatically. With **several**, omitting `--graph` errors and lists the
candidates; it never picks one for you.

Against an **`s3://`-backed cluster** the resolved graph storage is non-local, so a
destructive `cleanup` additionally requires **`--yes`** (an interactive prompt
otherwise, refusal without a TTY) on top of `--confirm` — see [cli-reference.md](../cli/reference.md)'s
*Write diagnostics & destructive confirmation*. Every maintenance run also echoes
its resolved target to stderr (suppress with `--quiet`).

## What the control plane does not do (yet)

- **No hot reload** — applied changes serve on the next restart.
- **No data operations** — rows move through `omnigraph load / ingest /
  mutate` against the graph roots, with branches and merges as usual.
- **Stored-query exposure is all-or-nothing per cluster** — every applied
  query is listed and invokable (subject to Cedar `invoke_query`); per-query
  exposure policy is a planned phase.
- **Pipelines (ETL)** are a separate project; the `pipelines:` key is
  reserved and rejected loudly.

For the full reference — every key, flag, status, disposition, and
diagnostic — see [cluster-config.md](config.md).
