# Cluster Deployments

A `cluster.yaml` and its referenced schemas, queries and policies declare the
whole deployment. Apply creates and manages its graphs. The server serves that
applied state; there is no standalone-graph server mode.

## Configuration

```yaml
version: 1
# storage: s3://my-bucket/clusters/company-brain  # default: config directory
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: schema.pg
    queries: queries/
    external_blobs:
      allow:
        - { base: s3://company-assets/knowledge/, scope: server_safe }
```

`queries` accepts a directory, file list or `name: { file: ... }` map. Duplicate
names and unparseable files fail validation. Relative schema/query/policy paths
must stay inside the config directory: `..` and symlink components are refused.
Keep `cluster.yaml` and referenced source files in Git; ignore generated
`__cluster/` state and `graphs/` roots. Credentials belong outside the declaration.

The operator's `~/.omnigraph/config.yaml` separately owns named servers,
profiles, actor defaults and aliases. A server never reads that operator file.
`--config` selects source files; `--cluster` selects applied cluster storage.

## Update a running server

```bash
omnigraph cluster validate --config .
omnigraph cluster plan --server production --config . --json
omnigraph cluster apply --server production --config . --timeout 1800 --json
```

Review the preview before applying, especially graph removals. Apply supports:

- schema and stored-query updates;
- graph and cluster policy grants, revocations and binding changes;
- embedding provider definitions/bindings and external-Blob ingress rules;
- graph creation and deletion.

Affected graphs close admission, drain their admitted work and activate the new
configuration without restarting the process. Unrelated healthy graphs keep
serving. Across graphs, deployment is not one transaction. Schema changes still
require only `main` to be live; merge and delete feature branches first.

Removing a graph declaration authorizes permanent deletion of its exact managed
root and all retained history. Shared external Blob source objects are not
deleted; object-store retention can keep historical object versions. A schema
property/type drop is different: older graph commits retain its data until
explicit cleanup stops retaining them.

The current applied policy must grant `config_manage` at cluster scope, `read`
on disclosed graphs and `schema_apply` on existing graphs whose schema changes
or whose declaration is removed.
Proposed permissions cannot authorize their own installation. Served apply
refuses `--as`; the bearer token supplies the actor.

Root changes, adoption of existing roots, recreation of missing managed roots
and acceptance of out-of-band schema drift are unsupported. A refusal such as
`applied_schema_drift` needs investigation, not a force or correction flag.
Rows are managed with `mutate`/`load`, separately from configuration apply.

### Observe the original deployment

Apply prints its deployment ID before submission and normally polls it until
convergence and activation. `--no-wait` returns after durable acceptance.
`--timeout` bounds caller waiting, including acceptance (default 300 seconds,
range 1–3600); expiry exits 5 without cancelling server work.

```bash
omnigraph cluster apply --server production --config . --no-wait --json
omnigraph cluster status --server production --deployment-id ID --wait \
  --timeout 1800 --json
```

A lost reply triggers reads of the original ID, never automatic resubmission.
Exact status returns `deployment`, `active` and `in_progress`. `active` describes
that result's affected bindings; an unrelated blocked graph does not invalidate
it. The authenticated submitter can read its own retained receipt after losing
management permission. Aggregate status and new deployments require current
permissions. See [remote outcomes](remote-ops.md) for data writes, whose recovery
is not deployment-ID polling.

## Bootstrap or update while stopped

Direct apply writes storage itself. Stop serving and overlapping writers first:

```bash
omnigraph cluster validate --config .
omnigraph cluster plan --config . --json
omnigraph cluster apply --config . --as operator --json
# After the apply owner has stopped and its I/O has settled:
omnigraph cluster force-unlock <LOCK_ID> --config .
omnigraph-server --cluster . --bind 127.0.0.1:8080 --unauthenticated
```

The final flag is for trusted local development. For authenticated serving,
configure tokens and policy as described in [server and policy](server-policy.md).
There is no separate `init` step for a cluster graph.

One mutation-capable process per cluster is supported. Direct apply, direct
cluster writes and the server share exclusive writer admission. They leave the
lock after exit, including a clean exit; a successful shutdown does not itself
prove all native I/O has settled. Clear only the exact retained lock after that
proof. Older binaries, raw storage tools and embedded writers are not fenced
by this lock and must remain stopped.

`cluster plan`, `observe`, `status` and direct graph reads take no writer lock.
Plan/observe report `authority: "observed"` and the `state_cas` read; apply
revalidates authority. Use served plan while the running server owns the root.

## Storage and serving

- `storage: s3://bucket/prefix` places state and derived graph roots together;
  the writer uses standard `AWS_*` credentials and conditional storage writes.
- Azure `az://` roots remain a qualification preview. Every write, apply,
  server and maintenance process must use `omnigraph-azure-admission`.
- New external Blob references are denied unless allowed by the graph's
  `external_blobs` configuration. Servers install `server_safe` bases only;
  `embedded_only` is not installed by the server or direct CLI. Allowed bases
  must be outside cluster storage, never inside another managed graph root.
- `omnigraph-server --cluster <dir|file://|s3://|az://>` boots applied state
  without needing a checkout of the source bundle. The container entrypoint
  accepts `OMNIGRAPH_CLUSTER`; the server binary takes `--cluster`.
- Use `/readyz` for rollout readiness and authorized `/graphs` for per-graph
  availability. Process auth, bind settings and signed-token trust remain
  startup configuration; applied graph configuration is live. See
  [server and policy](server-policy.md).

## Managed clusters

Select the managed service explicitly with `--managed`:

```bash
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example --config .
omnigraph cluster push --managed --expected-revision REV --message "Update configuration"
omnigraph cluster plan --managed --rev NEW_REV --json
omnigraph cluster apply --managed --plan PLAN_RUN_ID --json
omnigraph query find_person --graph knowledge --params '{"name":"Alice"}' --json
```

`use` writes `.omnigraph/context`. API sessions and data credentials are separate
OS-keychain entries. Supported data commands (`query`, `mutate`, `load`,
`commit list`/`show`, `graphs list`) use that folder's endpoint and acquire/cache
an identity credential; all but `graphs list` need `--graph`. A cached credential
can work during a control-API outage until expiry. Other data commands retain
ordinary addressing. Explicit `--server`/`--profile`/`--store`/`--cluster`, or
`--direct` for ordinary ambient defaults, selects ordinary routing.

Without `--managed`, cluster commands ignore folder context. Managed commands
reject ordinary target flags, `--direct` and `--as`; errors do not fall back to
local deployment. Lifecycle commands include `create`, `delete`, `undo-delete`
and `operation`; `status` observes a run, and `history`/`cancel` manage runs.
Use the same idempotency key to reconcile an uncertain managed submission.

`cluster token --managed` issues an identity credential explicitly; it contains
no graph/action grants. Applied Cedar policy decides access. `--clear` forgets
the local credential without revoking it. For unattended control access, set
`OMNIGRAPH_CONTROL_API` and `OMNIGRAPH_CONTROL_TOKEN` together.

See the [managed command reference](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/cli/reference.md#managed-cluster-commands),
[lifecycle](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/cli/managed-lifecycle.md)
and [data access](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/cli/managed-data.md)
for exact flags, session limits and exit codes.

## Recovery

| Situation | Action |
|---|---|
| Served apply timed out or lost its response | Continue `cluster status --server … --deployment-id ID --wait`; do not submit a new ID. |
| Direct apply interrupted or accepted work needs stopped recovery | Read `omnigraph --cluster ROOT cluster status --deployment-id ID --json`. Stop the prior owner, prove its I/O settled, clear its exact lock, then use `omnigraph --cluster ROOT cluster apply --deployment-id ID --writers-stopped`. |
| `state_lock_held` | `cluster observe` shows the holder. Stop it and prove I/O settlement before `cluster force-unlock LOCK_ID`; a lock's age is not proof. |
| `ledger_upgrade_required` | With all writers stopped, run `omnigraph --cluster ROOT cluster upgrade-ledger --writers-stopped`. Conversion preserves graph data and history. |
| Missing/corrupt ledger or unexpected graph root | Restore a trusted consistent backup or follow the diagnostic. Bootstrap never adopts an existing root; do not recreate state by hand. |

Canonical guide: [clusters](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/clusters/index.md)
and [configuration](https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/clusters/config.md).
