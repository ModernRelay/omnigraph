# Cluster deployments

A cluster declares graphs, schemas, stored queries, Cedar policy and runtime
bindings in a directory containing `cluster.yaml`. One `--target` selects where
commands execute. Target kind determines which operations are available.
`--config DIR` supplies source files; it never selects a destination.

## Source and destination

```text
company-brain/                 # declared source, safe to version in Git
├── cluster.yaml
├── schema.pg
├── queries/people.gq
└── graph.policy.yaml

company-data/                  # selected direct root, never version in Git
├── graphs/<id>.omni            # created by apply
└── __cluster/                 # deployment authority
```

```yaml
# company-brain/cluster.yaml
version: 1
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: schema.pg
    queries: queries/
```

Referenced source paths are relative to the configuration directory. Query
folders discover immediate `.gq` files; malformed sources and duplicate names
refuse. A declared `storage:` is an optional equality assertion against the
selected root. Direct commands resolve a relative assertion against the source
directory. Server submission requires an absolute assertion or no `storage:`.
Omitting `storage:` never sends writes to the source directory.

Registered direct targets pin storage URI and provider destination settings.
S3 region, endpoint and addressing style, and Azure account/endpoint/emulator
settings are destination identity; credentials continue through the appropriate
provider chain. Ambient cloud settings cannot redirect a registered target.
Inline storage addresses capture their provider settings for that invocation.

## Direct bootstrap

```bash
mkdir -p ./company-data
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --target ./company-data --config ./company-brain --json
omnigraph cluster apply --target ./company-data --config ./company-brain --as act-alice --json
# After the previous owner has stopped and its accepted I/O has settled:
omnigraph cluster force-unlock '<LOCK_ID>' --target ./company-data
omnigraph-server --cluster file:///absolute/company-data --bind 127.0.0.1:8080 --unauthenticated
```

`validate` is source-only and takes no target. Every backend command needs an
explicit target or the configured `defaults.target`; there is no folder fallback.
The CLI prints the resolved target on stderr before execution. `.` in
`--config .` means the current source directory. Source-bearing direct/server
plan and apply default that source path to `.`.

Direct apply creates graphs, not rows; use `load` or `mutate` for data. It retains
its exact admission lock after completion. Exclude other writers and establish
prior I/O settlement before an exact-ID unlock. A lock is cooperative exclusion,
not fencing for older binaries or raw storage tools. Azure writers also require
`omnigraph-azure-admission` and remain a qualification preview.

## Live server deployment

Configure `production` as a server target with `auth: {provider: token, source: {store: {}}}`;
see [operator configuration](../../../docs/user/cli/targets.md).

```bash
printf '%s' "$TOKEN" | omnigraph credentials set --target production --stdin
omnigraph cluster plan --target production --config . --json
omnigraph cluster apply --target production --config . --timeout 1800 --json
omnigraph cluster status --target production --deployment-id ID --wait --timeout 1800 --json
omnigraph query find_person --target production --params '{"name":"Alice"}' --json
```

A named server target uses only its declared auth provider. Remote
inline URLs and Unix socket targets are unsupported.
The server derives the actor from authentication;
`--as` on a server `plan` or `apply` only asserts it, and another actor is
refused with `actor_mismatch` and exit 4 before anything is submitted. The
applied policy must grant `config_manage`, plus graph
permissions required by schema changes or deletion. Proposed permissions cannot
authorize their own installation.

Live apply keeps the process and writer owner, drains affected requests, applies
schemas and installs matching queries/policies/runtime bindings. Other graphs
keep serving. Across graphs this is not one transaction. Schema changes remain
main-only and require one live branch; query-only changes create no graph commit.
Removing a graph declaration deletes its managed root and retained history.
Review plan output and retain backups before applying a removal. External Blob
source objects are not deleted; provider version retention may keep older bytes.

Apply reports its original deployment ID before submission. It normally waits
for convergence and activation; `--no-wait` returns after acceptance. A timeout
(default 300 seconds, maximum 3600) stops local waiting, not execution. Observe
the same ID instead of creating a new attempt. `active` describes this runtime;
a durable completion receipt alone does not prove activation. Wait for
`in_progress: false` before another deployment. An initiating actor can observe
its retained receipt after losing management permission; aggregate status and
new deployments still require current permission.

## Source-free inspection and recovery

```bash
omnigraph cluster status --target file:///srv/company-data --deployment-id ID --json
# Only after prior-owner and native/control-I/O quiescence:
omnigraph cluster force-unlock LOCK_ID --target file:///srv/company-data
omnigraph cluster apply --target file:///srv/company-data --deployment-id ID --writers-stopped --json
```

On a direct root, an ID-bearing apply without `--config` uses captured deployment
evidence. It observes completed results and needs explicit `--target` plus
`--writers-stopped` for recovery effects. An ID-bearing fresh source submission
requires `--config DIR`; it cannot also carry the recovery attestation. Reusing
an existing ID with identical captured source only observes; it never executes
again. Different input refuses. Server ID-only apply refuses: use status.

Reconciliation never replays uncertain schema execution or graph creation.
Unknown results stay outstanding. Accepted deletion can continue removing only
its originally recorded root. A settled partial result permits a corrective
successor from achieved state; it does not roll back completed graphs. Output
failure also leaves outcome uncertain, so retain the ID, root and lock.

For recognized prior ledger formats or obsolete completed-result fields, stop
all owners and run `cluster upgrade-ledger --target ROOT --writers-stopped`.
This changes ledger representation only. Finish outstanding work with its
originating build before upgrading. No legacy executor or automatic migration
remains. Missing graph roots and schema drift require authoritative repair or
backup restoration; apply does not adopt or recreate them.

## Managed clusters

A profile pins API, organization and one auth provider for one identity. A managed
target references it and pins cluster ID/incarnation plus an optional endpoint.
Graph work needs that endpoint and an explicit/default graph. See
[registration](../../../docs/user/cli/targets.md#login-and-registration).

```bash
omnigraph login --profile work
omnigraph cluster list --profile work --json
omnigraph cluster create demo --profile work --no-wait --json
omnigraph target add production --profile work --cluster CLUSTER_ID
omnigraph cluster pull --target production --out source --json
omnigraph cluster push --target production --config source --expected-revision REV --message 'Update schema'
omnigraph cluster plan --target production --rev NEW_REV --json
omnigraph cluster apply --target production --plan PREVIEW_ID --json
omnigraph cluster status DELIVERY_ID --target production --json
omnigraph cluster operation OPERATION_ID --profile work --wait --json
```

Managed plan/apply use pushed source revisions/previews and reject local
`--config`. Only push captures local source. Pull creates a new directory with
source files and no destination metadata. Create returns identity without
changing any target or directory. Delete/undo require the selected cluster;
`--incarnation` is an equality attestation, never an override.

Lifecycle pending and accepted identities live under the operator home's
`lifecycle/`, partitioned by API and organization. Retry an uncertain request
with the same principal, exact arguments and key. Accepted history is bounded;
full custody refuses instead of evicting evidence. Preserve old journals and
preserve matching legacy folder evidence explicitly with `config recover-operations
--source DIR --profile work`. This does not convert configuration or
submit operations; resume with the original verified principal and original key.

Managed graph commands acquire missing identity credentials without replaying
submitted mutations. Credential issuance grants connectivity, not graph
permissions; applied Cedar policy remains authoritative. `cluster token --target
production --ttl 1h` administers the cached credential explicitly.

## Serving and references

The server binary keeps its independent `--cluster DIR|URI` boot contract. A URI
boots from applied resources without a source checkout. It never reads the CLI
operator target file. Boot errors attributable to one graph quarantine that graph
unless `--require-all-graphs` is set; global failures refuse startup. `/readyz`
reports graph counts and `/graphs` requires `graph_list`. The bounded shutdown
uses `--shutdown-grace-seconds` (default 25) and exits 2 if drain expires.

See the repository's `docs/user/clusters/index.md` for deployment and recovery,
`docs/user/clusters/config.md` for configuration and limits, and
`docs/user/cli/managed-lifecycle.md` for service operations and custody.
