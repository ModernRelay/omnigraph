# Cluster Mode — Declarative Deployments

## Contents
- The model
- The loop (validate → plan → apply → unlock → serve)
- The config contract (`cluster.yaml` vs `~/.omnigraph/config.yaml`)
- Serving (`--cluster`, config-free bucket boot, readiness, shutdown)
- Managed clusters
- Recovery cheat-sheet

The cluster control plane manages a whole deployment —
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
└── __cluster/          # ledger + lock + catalog — local state (gitignore)
```

```yaml
# cluster.yaml
version: 1
# storage: s3://my-bucket/clusters/company-brain   # optional object-store root;
# preview Azure roots use az://container/prefix (default: this folder)
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: schema.pg
    queries: queries/    # the .gq files ARE the declaration — every `query <name>` registers
    external_blobs:      # omitted means deny new external references
      allow:
        - { base: s3://company-assets/knowledge/, scope: server_safe }
```

`queries` also accepts a file list (`[a.gq, b.gq]`) or a fine-grained
`name: { file: ... }` map. Discovery is loud: unparseable files and duplicate
names across files fail validation.

Relative `schema`, `queries`, and policy `file` paths must stay inside the
config directory: a `..` segment fails with `config_path_escape`, and a
symbolic link anywhere on the path (including a discovered `.gq` file) fails
with `config_path_symlink`. Absolute paths are not checked; prefer relative
paths.

## The loop (memorize this)

```bash
omnigraph cluster validate --config .              # parse + typecheck everything
omnigraph cluster plan     --config .              # preview — REQUIRED reading before apply
omnigraph cluster apply    --config . --as <you>   # converge; the first run creates ledger + graphs
omnigraph cluster force-unlock <LOCK_ID> --config .  # clear the lock apply printed, once its work has settled
omnigraph-server --cluster . --bind 127.0.0.1:8080 --unauthenticated  # serve (local dev)
```

- **`apply` creates graphs** at `graphs/<id>.omni` — there is no separate
  `omnigraph init` in cluster mode.
- **Schema changes**: edit the `.pg`, `plan` shows the engine's real migration
  steps (`add_property`, `drop_property`, `unsupported: …`), `apply`
  migrates the live graph. **Drops reclaim nothing at apply** — a drop removes
  the data from the branch head; older commits still read it until
  `omnigraph cleanup` stops retaining them, and only then is it gone for good.
- **Two ways to apply.** `cluster apply --server <name|url> --config .` submits
  the bundle to the running server, which activates schema and stored-query
  changes and new graphs without a restart. It needs a bearer token whose
  actor the applied policy grants `config_manage` at cluster scope, `read` on
  disclosed graphs, and `schema_apply` on each graph whose schema changes. A
  direct `cluster apply --config .` writes storage itself: stop the server
  first and start it afterwards.
- **Every writer leaves the cluster lock behind.** A direct apply, a direct
  `--store` write to a cluster graph, and a server (even after a clean exit)
  keep `__cluster/lock.json`, and the next writer or server start refuses with
  `state_lock_held`. Once the previous owner has stopped and its I/O has
  settled, clear that exact id with `cluster force-unlock <LOCK_ID> --config .`.
  While the server owns the lock, preview with `cluster plan --server <name|url>`.
- **`storage: s3://bucket/prefix`** (optional) puts the entire cluster — state
  ledger, lock, content-addressed catalog, and the derived graph roots
  (`<storage>/graphs/<id>.omni`) — on
  S3-compatible object storage. The ledger CAS uses S3 conditional writes and
  the lock becomes genuinely cross-machine. Absent, a direct apply keeps
  everything in the config directory, and `cluster apply --server` uses the
  server's own root. Credentials
  come from the standard `AWS_*` env contract, never `cluster.yaml`.
- **`storage: az://container/prefix` is preview-only.** Every writer, server,
  apply job, and maintenance process for an Azure root must run through
  `omnigraph-azure-admission`; the preview lease is external admission, not an
  engine-level distributed-writer fence.
- **One mutation-capable process per cluster is the supported topology.** The
  server, direct applies and direct CLI writes all take the same cluster lock,
  so they exclude each other. The lock does not fence older binaries, raw
  storage tools or embedded writers; keep those stopped.
- **External Blob ingress is default-deny.** `graphs.<id>.external_blobs.allow`
  lists normalized URI bases. `scope: server_safe` can be installed by the
  server; `embedded_only` is never installed by the server or direct-store CLI.
  Cedar chooses who may write; this list chooses which source objects a writer
  may cause the process to inspect. A base must lie outside the cluster storage
  root (`storage`, or the config directory), which holds every graph and the
  ledger: `cluster validate`, `plan` and `apply` refuse an overlapping base in
  either scope with `external_blob_base_overlaps_storage_root`, and a server
  quarantines a graph whose applied `server_safe` base overlaps. Use a sibling
  prefix, e.g. `s3://company-assets/cluster-external/` beside
  `storage: s3://company-assets/cluster`. A same-kind base against a storage
  root spelled with an empty or percent-sign path component is refused with
  `external_blob_storage_root_uncomparable`; moving the base does not clear it,
  so re-spell the root or remove those bases.
- **`--as <actor>` labels a direct `cluster apply` or `cluster upgrade-ledger`**
  (deployment record and engine commits where applicable). It defaults from
  operator config's `operator.actor`. `cluster apply --server` refuses it,
  because the server takes the actor from the bearer token; the other cluster
  subcommands reject this flag.
- **Deployment scope**: apply creates graphs and updates schemas, queries,
  policies, provider definitions/bindings, and `external_blobs` rules. Removing
  a graph declaration deletes its exact managed root, including retained
  history, after affected admission closes and work drains. Current permissions
  authorize changes; proposed permissions cannot authorize themselves. Shared
  external Blob objects are not deleted, and provider retention may keep object
  versions. Root changes, adoption and missing-root recreation refuse.
- **Drift**: apply refuses an out-of-band schema change with
  `applied_schema_drift`; it never adopts it by matching schema text. To inspect
  without effects, `cluster observe --config .` reports current ledger, catalog
  and graph observations; `cluster plan --config .` previews desired changes.
  Both are read-only, report `authority: "observed"` and the `state_cas` they
  read, and take no writer lock. Apply revalidates current authority.
- **Data is NOT cluster's job**: rows flow through `omnigraph load / mutate`
  against the served graph (`--server … --graph <id>`), with branches as usual.

For a running server, preview and submit the same local bundle:

```bash
omnigraph cluster plan --server production --config . --json
omnigraph cluster apply --server production --config . --timeout 1800 --json
omnigraph cluster status --server production --deployment-id ID --wait --timeout 1800 --json
```

Apply prints its deployment ID before submission and normally polls that ID
until convergence and activation. `--no-wait` returns after durable acceptance;
`--timeout` bounds caller waiting, including acceptance (default 300 seconds,
range 1–3600). Expiry exits 5 without cancelling work. Resume with the exact
`status --deployment-id ID --wait`; these wait flags require `--server`.
A lost submission response triggers original-ID reads, never automatic replay.
The exact response contains `deployment`, `active` and `in_progress`; `active`
means the result's affected bindings are installed in this process. An
unrelated blocked graph does not invalidate that activation. The authenticated
submitter can still read its own retained receipt after losing management
permission; aggregate status and new deployments require current permission.
Deployment across graphs is not one transaction.

## The config contract (do not blur this)

| File | Owns | Read by |
|---|---|---|
| `cluster.yaml` | the deployment: graph set, schemas, stored queries, policy bindings, storage | `cluster` commands; the `--cluster` server |
| `~/.omnigraph/config.yaml` | per-operator: identity (`operator.actor`), named `servers:`, output defaults, personal aliases | data-plane CLI commands (tokens live in `~/.omnigraph/credentials` via `omnigraph login`) |

Direct cluster commands use the operator actor default when `--as` is omitted
(`--as` > `operator.actor`). `cluster` selects self-hosted deployment unless
`--managed` explicitly selects the service API described below. Folder context
never selects the mode.
A `--cluster` server never reads the operator file: its configuration comes from
applied cluster state.
Address a cluster-managed graph's data via `--server`/aliases against the
serving instance. A direct `--store <storage>/graphs/<id>.omni` read takes no
lock. On a ledger-v2 cluster a direct write takes the cluster lock: it is
refused while a server or another owner holds it, and the lock stays held after
the command until an exact-ID `cluster force-unlock`.

## Serving

`omnigraph-server --cluster <dir-or-uri>` is the exclusive boot source (there
is no separate `--config` merge) and is always multi-graph
(`/graphs/{id}/...`). By default, graph-attributed recovery, query-registry, or
provider failures quarantine only the affected graph and healthy graphs still
serve. Cluster-global or unattributable failures are fatal, as are any graph
failures with `--require-all-graphs`. Every healthy graph's applied query is
exposed (`GET /graphs/<id>/queries`, `POST
/graphs/<id>/queries/<name>`); Cedar bundles attach via `applies_to`
(`cluster` → server-level gate incl. `graph_list`; a graph id → that
graph's gate incl. `invoke_query`). Bearer tokens and bind stay process-level
(env/flags).

`GET /readyz` reports the booted applied digest, ledger revision/CAS and
`served_graph_count` (the whole registry) with `ready_graph_count`,
`loading_graph_count` and `blocked_graph_count`; it is HTTP 503 while any graph
is still loading, when a nonempty inventory has no ready graph, and when
draining. An applied empty cluster can serve a ready zero-graph inventory. A
nonempty cluster with no healthy graphs still refuses startup. `GET /graphs`
requires `graph_list` and returns one `graphs` list whose entries carry `state`
(`loading`, `ready`, `transitioning`, `blocked`, `stopping`), `read_available`,
`write_available`, `action` and, when blocked, a sanitized `failure`; readiness
itself exposes counts only. A request to a loading, blocked or transitioning
graph from a caller allowed to read `main` or list graphs answers `503
graph_unavailable`, which never authorizes replaying a write.

**Bounded shutdown.** `--shutdown-grace-seconds` (else
`OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`, default 25; `0` cuts off immediately)
bounds the drain: at SIGTERM readiness turns off and in-flight requests drain;
a clean drain exits 0, reaching the deadline exits 2. Set the orchestrator's
termination grace longer than this value.

**Config-free serving.** `--cluster` also accepts a `file://`, `s3://`, or
preview `az://` storage-root URI
directly — `omnigraph-server --cluster s3://bucket/prefix` boots from the
applied revision on the bucket with **no checkout of the config repo**. The
ledger and catalog on the bucket are the whole deployment artifact; policy
bundles serve as digest-verified content from the catalog. The preferred
container shape is **bucket, no volume** (see the container section of the
omnigraph repo's `docs/user/deployment.md`). The container entrypoint reads
`OMNIGRAPH_CLUSTER` (a storage URI or a mounted config directory) and passes
it as `--cluster`; the server binary itself reads only `--cluster`. The image
ships the CLI for in-container `cluster apply`.

## Managed clusters

An Intent API can own the control plane while the same CLI operates it:

```bash
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example --config .
omnigraph cluster plan --managed --config . --json
omnigraph cluster apply --managed --plan PLAN_RUN_ID --config . --json
omnigraph query find_person --graph knowledge --params '{"name":"Alice"}' --json
```

`use` writes `.omnigraph/context` in the selected directory. API sessions and
data credentials are separate OS-keychain entries, never plaintext config.
Managed `query`, `mutate`, `load`, `commit list`/`show` and `graphs list` read
context only in the current directory (all but `graphs list` require
`--graph`) and use the cached data endpoint/credential, acquiring a missing or
expired identity credential first. They can operate during
a control-API outage until that credential expires. Other data commands keep
ordinary addressing. Explicit `--server`/`--profile`/`--store`/`--cluster` selects
ordinary routing; global `--direct` selects ordinary ambient defaults. Missing
or malformed managed authority refuses without fallback, and competing ambient
targets require an explicit choice.

Managed creation, config upload, deletion and undo use `cluster create`, `push`,
`delete` and `undo-delete`, each with `--managed`.
`cluster status --managed [RUN_ID]` reads cluster projections or one run;
`cluster operation --managed ID [--wait]` observes a lifecycle operation. Durable
operation records bind uncertain submissions to their exact identity. Reconcile
the existing operation before issuing another.

- **Schema/config changes**: commit, `cluster push --managed --expected-revision <rev>
  --message …`, `cluster plan --managed --rev <new>`, then `cluster apply --managed --plan <run>`.
  The Intent API executes the apply; there is no local `--as` or approval step.
- **Explicit mode**: `--managed` requires context, except creation and
  explicit-origin operation lookup. Without it, `cluster` ignores context and needs no
  `--direct` escape. Managed operations reject `--direct`, `--as`, `--server`,
  `--profile`, `--graph`, `--store`, and `--cluster`; failures never fall back to
  local deployment. Service-only verbs require `--managed`; `validate`,
  `observe`, `force-unlock`, and `upgrade-ledger` reject it.
- **Sessions** from `login --api` hold an access credential of at most 15
  minutes that renews silently for up to eight hours after sign-in; then log
  in again. For unattended runs, set `OMNIGRAPH_CONTROL_API` and
  `OMNIGRAPH_CONTROL_TOKEN` together; reuse the same `--idempotency-key` after
  an uncertain response.
- **Data credentials**: graph commands acquire an identity credential on
  their own; `cluster token --managed [--ttl 1h]` issues one explicitly (TTL 60s–24h,
  default 1h) and `--clear` forgets the local copy without revoking it. Only
  identity credentials are supported: they carry no graph or action grants,
  and the applied Cedar policy must permit the authenticated actor.
- **Exit codes** for plan/apply/lifecycle runs: 0 converged, 1 failed or
  transport error, 2 refused or blocked, 3 partially converged, 4 recovery
  required, 5 stalled or wait deadline, 6 cancelled.
See the authoritative [managed command reference](https://github.com/ModernRelay/omnigraph/blob/v0.12.0/docs/user/cli/reference.md#managed-cluster-commands),
[lifecycle](https://github.com/ModernRelay/omnigraph/blob/v0.12.0/docs/user/cli/managed-lifecycle.md) and
[data-access guide](https://github.com/ModernRelay/omnigraph/blob/v0.12.0/docs/user/cli/managed-data.md) for flags and limits.

## Recovery cheat-sheet

| Symptom | Fix |
|---|---|
| Served apply timed out or lost its response | continue original-ID observation with `cluster status --server <name\|url> --deployment-id <ID> --wait --json`; timeout does not cancel the owner. Never resubmit under a new ID |
| Direct apply interrupted, or accepted work needs stopped recovery | never retry under a new deployment id. Read the original deployment with `omnigraph --cluster <root> cluster status --deployment-id <ID> --json` (apply prints the id and root). Stop the prior owner, prove its I/O has settled, clear the held lock with `omnigraph --cluster <root> cluster force-unlock <LOCK_ID>`, then reconcile that same id: `omnigraph --cluster <root> cluster apply --deployment-id <ID> --writers-stopped` |
| Held lock (`state_lock_held`) | expected after any apply, direct write or server run. `cluster observe` shows state and the holder without refusing. First stop the owner and prove its I/O has settled; then use `cluster status` and clear that exact id with `cluster force-unlock <LOCK_ID> --config .` |
| `config_path_escape` / `config_path_symlink` | move the referenced file inside the config directory and reference it by a plain relative path |
| Missing `state.json` | new cluster: `cluster apply` creates the ledger. Bootstrap never adopts graph roots that already exist (`graph_already_exists`); for those, restore a trusted cluster-state backup |
| Corrupt `state.json` | restore a trusted cluster-state backup or follow the diagnostic |
| Server refuses to boot | the error names its code and remedy; common ones: `state_lock_held` (see Held lock above), `ledger_upgrade_required` (stop serving and writers, then `cluster upgrade-ledger --writers-stopped`), `cluster_deployment_outstanding` (reconcile that exact id with `cluster apply --deployment-id <ID> --writers-stopped`) |
| `ledger_upgrade_required` (supported legacy ledger or obsolete completed-result fields) | stop every server, writer and maintenance job, then `omnigraph --cluster <root> cluster upgrade-ledger --writers-stopped`; it converts the ledger only and keeps rows, branches and history |

Full reference: the omnigraph repo's `docs/user/clusters/index.md` (operator guide)
and `docs/user/clusters/config.md` (every key, flag, and diagnostic).
