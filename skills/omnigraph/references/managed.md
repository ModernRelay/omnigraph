# Managed Clusters and Data Access

Use this reference when a directory has `.omnigraph/context`, or when creating
or selecting a cluster through a managed control API. Direct storage operations
are covered in [cluster.md](cluster.md). Managed operations use service
permissions and do not require local cloud storage credentials.

## Login and selection

```bash
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example --config ./company-brain
```

Complete the browser device login shown on stderr. The control session lives
in the OS keychain under its API origin, expires within 15 minutes, and has no
refresh token or plaintext fallback. Run login again after expiry. `use`
writes a non-secret context containing `version`, `cluster`, and `api`.
Cluster commands read it only from their `--config` directory (default `.`);
data commands read only the current directory. Neither searches parents.

Use an HTTPS origin without a path, credentials, query, or fragment. HTTP is
accepted only for the exact local hosts `localhost`, `127.0.0.1`, and `[::1]`.
Malformed context or an unavailable keychain/API refuses; it never triggers a
direct-storage fallback.

For unattended control operations, supply both `OMNIGRAPH_CONTROL_API` and
`OMNIGRAPH_CONTROL_TOKEN` through the deployment's secret mechanism. The origin
must match the context. Named-server tokens and operator profiles are separate.

## Target selection

An explicit `--server`, `--profile`, `--store`, or `--cluster` selects ordinary
data routing, even beside malformed managed context. Global `--direct` selects
ordinary ambient resolution, ignoring managed context; use it only when that
is the intended target.

Implicit `query`/`mutate` in a managed folder require `--graph`. If
`OMNIGRAPH_PROFILE` or an operator default server/store competes with that
folder, the CLI refuses with `managed_target_ambiguous` before contacting either
target. Select the ordinary target explicitly, or unset the competing setting
to use the managed folder. Merely defined profiles and presentation defaults
do not conflict. An explicit `--as` is refused on managed requests.

Other data verbs, dedicated `branch` commands, aliases, and maintenance retain
their ordinary routing and credentials. They do not borrow the managed token.
Managed cluster commands also reject actor/storage selectors; direct-only
cluster verbs such as `import`, `observe`, `refresh`, and `force-unlock` refuse
when managed context is selected.

## Data access

From the bound directory, mint data authority separately from the control login:

```bash
omnigraph cluster token --graph knowledge --actions read,change,invoke_query --ttl 1h
omnigraph query find_person --graph knowledge --params '{"name":"Alice"}' --json
omnigraph mutate add_person --graph knowledge --params '{"name":"Alice"}' --json
```

Choose only the actions the task requires. A stored query requires
`invoke_query` plus `read` or `change`; ad-hoc data source needs `read` or
`change`. Grants also support `export`, `branch_create`, `branch_delete`,
`branch_merge`, and `graph_list`; wildcards, duplicates, `admin`, and
`schema_apply` are refused. Every request still needs current Cedar permission
for the signed principal; token grants only narrow that policy.

`--graph` and `--actions` are required when minting. TTL defaults to one hour
and accepts 60 seconds through 24 hours (seconds or `s`/`m`/`h`/`d` suffixes).
The service can shorten it. One data credential per API origin and cluster is
kept in a separate keychain entry; minting replaces that entry. CLI output
contains metadata, never the credential. Query/mutate contact its cached data
endpoint directly, including during a control-API outage, until expiry. Missing,
expired, or insufficient credentials refuse without fallback. Requests refuse
redirects and have a 10-second deadline and an 8 MiB response limit.

**Current branch-statement limitation:** the managed CLI precheck treats every
`mutate` as requiring `change`, before classifying branch source. A token with
only `branch_create`, `branch_delete`, or `branch_merge` is therefore refused
locally even when the server would authorize that statement. This is not a
Cedar denial; do not broaden authority automatically to work around it. An
explicit named-server route uses its own separately configured credential.

`cluster token --clear [--config DIR]` forgets only the local data credential;
do not combine it with graph, action, or TTL options. `logout --api ORIGIN`
revokes the control session and removes its local entry. Neither action revokes
already issued data credentials, which remain subject to expiry and signing
trust. Accepted operations can finish after expiry.

## Prepare configuration and apply a saved plan

```bash
omnigraph cluster status --config ./company-brain --json
# Copy data.requested.revision into EXPECTED_COMMIT.
omnigraph cluster push --config ./company-brain --expected-revision EXPECTED_COMMIT \
  --message "Update graph configuration" --json
# Copy data.revision from push into NEW_COMMIT.
omnigraph cluster plan --config ./company-brain --rev NEW_COMMIT --json
# Copy data.run_id from the converged plan into PLAN_RUN_ID.
omnigraph cluster apply --config ./company-brain --plan PLAN_RUN_ID --json
```

Plan/apply do not upload local edits. `push` uploads `cluster.yaml` and its
referenced schemas, queries, and policies; it never executes them. The expected
revision is a full 40-character lowercase Git commit ID. A stale revision
requires reconciling the edit, not retrying blindly. The declared storage root
must remain the cluster's exact canonical root.

Upload paths must remain inside the directory, with no symlinks or special
files. Query directories discover immediate `.gq` files only. Files must be
UTF-8, at most 2 MiB each, with at most 4096 files and a 32 MiB request. Upload
currently requires Unix. Unrelated files and credentials are not uploaded.

A saved plan retains a service lease until applied, abandoned, or expired.
`cluster cancel PLAN_RUN_ID` abandons an unused converged plan; cancel a pending
run with its run ID. Status and history are available through
`cluster status [RUN_ID]` and `cluster history [--limit N] [--since RFC3339]`.

Plan/apply accept `--idempotency-key KEY`, `--no-wait`, and `--timeout SECONDS`.
Without a supplied key they print a generated key before submission. Reuse that
key with the same request after an uncertain response. A timeout stops only the
local wait; inspect the same run's status. Default wait is 300 seconds, allowed
range 1–3600. JSON is one envelope on stdout; progress and keys go to stderr.

| Run outcome | Exit |
|---|---|
| Converged / accepted with `--no-wait` | 0 |
| Failed / transport error | 1 |
| Refused / blocked | 2 |
| Partially converged | 3 |
| Recovery required | 4 |
| Stalled / local wait deadline | 5 |
| Cancelled | 6 |

Successful status/history reads exit 0; abandoning an unused converged plan
also exits 0. A successful submission is not proof of completed deployment.

## Create, retire, and undo

```bash
mkdir ./customer-demo
omnigraph cluster create customer-demo --api https://control.example \
  --config ./customer-demo --no-wait --json
omnigraph cluster status --operation OPERATION_ID --config ./customer-demo --json
omnigraph cluster delete --config ./customer-demo --incarnation INCARNATION --json
omnigraph cluster undo-delete --config ./customer-demo --incarnation INCARNATION \
  --deletion-id DELETE_OPERATION_ID --json
```

Creation needs an unbound directory and never replaces existing context. Save
`data.canonical_root`, `data.incarnation`, and `data.operation_id` from the
accepted response. Creation does not create local configuration files; after
readiness, author a bundle using that exact root, then push/plan/apply. An
applied empty cluster may serve before the first graph is added. Readiness does
not imply a publicly reachable endpoint.

Delete targets an exact incarnation; undo targets its exact retained deletion
operation. The default undo interval is 86,400 seconds; `--retention-seconds`
accepts 0–2,592,000, with zero requesting immediate retirement after quiescence.
`tombstoned` is not `purged`: operation `data.state` and
`data.lifecycle.phase` report different facts. Undo requires current delete,
plan, and apply permission and must precede irreversible retirement.

Lifecycle commands preserve a pending request digest, idempotency key, origin,
principal, and account before sending. After an unknown outcome, rerun the same
command as the same principal in the same directory; the pending key is reused.
Do not discard that record or change the request to escape its refusal. After
validated acceptance, `.omnigraph/last-lifecycle.json` retains the operation
identity. These non-secret records support recovery; service status remains
authoritative. Lifecycle commands refuse `--direct`.
