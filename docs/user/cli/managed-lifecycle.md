# Create and retire a managed cluster

These commands use your managed service identity and permissions. They do not
open graph storage or require cloud credentials. The service must support
cluster creation and lifecycle operations; an older service returns a refusal.

## Create an empty cluster

Log in to your control API, then use an unbound configuration directory:

```sh
omnigraph login --api https://control.example
mkdir customer-demo
omnigraph cluster create customer-demo --api https://control.example \
  --config customer-demo --no-wait --json
```

The service reserves a fresh cluster and operation. The CLI writes its managed
context only after checking the returned identity. An existing context is never
overwritten. Keep the response's `data.canonical_root`, `data.incarnation` and
`data.operation_id` for the following steps. Without `--no-wait`, creation
waits for the normal plan/apply bootstrap and serving readiness. The JSON keeps
`data.state` (the operation outcome) separate from
`data.lifecycle.phase` (provisioning progress).

`--no-wait` returns as soon as the operation is accepted. `--timeout SECONDS`
sets a local 1–3600-second deadline; the default is 300. A timeout does not
cancel creation. Follow the saved operation explicitly:

```sh
omnigraph cluster status --operation OPERATION_ID --config customer-demo --json
omnigraph cluster status --operation OPERATION_ID --api https://control.example \
  --wait --timeout 600 --json
```

An operation can be inspected with `--api` before a folder is bound. If both an
origin and an existing folder context are present, they must agree. Private
clusters still require the connection reported by your service, such as an
explicit port forward; `ready` does not imply a public endpoint.

## Prepare and apply configuration

Creation binds the folder; it does not create local configuration files. After
the operation reaches `ready`, create `customer-demo/cluster.yaml`, replacing
`CANONICAL_ROOT` with the exact root from the accepted create response:

```yaml
version: 1
storage: CANONICAL_ROOT
graphs:
  catalog:
    schema: ./catalog.pg
```

Create `customer-demo/catalog.pg` with your graph schema, then read the current
managed revision before submitting the edit:

```sh
omnigraph cluster status --config customer-demo --json
# Use data.requested.revision from that response.
omnigraph cluster push --config customer-demo --expected-revision COMMIT \
  --message "Add the knowledge graph" --json
# Use data.revision returned by push.
omnigraph cluster plan --config customer-demo --rev NEW_COMMIT --json
# Use data.run_id from the converged plan.
omnigraph cluster apply --config customer-demo --plan PLAN_RUN_ID --json
```

`push` uploads only `cluster.yaml`, referenced schemas, stored queries and
policy files. Query-directory declarations discover immediate `.gq` files;
they do not recurse. Unrelated files, folder context and credentials are not
uploaded. Paths must stay within the config directory, with no symlinks or
special files. Files must be UTF-8, at most 2 MiB each, with at most 4096 files
and a 32 MiB request. Path length is limited to 512 bytes and capture to
120 seconds. Managed upload currently requires a Unix platform with
descriptor-relative file traversal; other platforms refuse before submission.

The expected revision is a full 40-character lowercase Git commit ID. A stale
head is refused; read the current revision and reconcile your edit before
retrying. The storage root cannot change. A saved plan holds the change lease;
apply or abandon that plan before another upload. `push` requires plan
permission and never executes a plan; `apply` requires its separate permission.

## Delete and undo

Use the exact incarnation from your cluster's response:

```sh
omnigraph cluster delete --config customer-demo --incarnation INCARNATION \
  --idempotency-key customer-demo-delete --json
```

Delete needs current scoped delete permission. It closes run and data-token
issuance; the service then withdraws routing and verifies quiescence. The
default undo interval is 86,400 seconds. With a nonzero interval, the command
returns when the service confirms `tombstoned`, including its deadline and
operation ID. Canonical `data.state` remains `running` until the service's
cleanup finishes; a tombstone does not mean purged.

`--retention-seconds` accepts 0–2,592,000. Zero requests immediate retirement
after quiescence and waits toward final cleanup within the local timeout.
`--no-wait` always returns the accepted operation without polling.

Before the deadline and irreversible retirement, request undo of the exact
deletion operation:

```sh
omnigraph cluster undo-delete --config customer-demo --incarnation INCARNATION \
  --deletion-id DELETE_OPERATION_ID --json
```

Undo needs current delete, plan and apply permissions. It preserves the cluster
identity and restores service through the normal bootstrap. The service decides
whether the retained window is still open; the CLI never clears retirement or
creates a replacement. The folder keeps its context after deletion so status
and history remain addressable.

## Recover an uncertain response

Lifecycle commands save their origin, exact request digest and idempotency key
with the authenticated principal and account before sending. Rerun the same
command as the same principal in the same directory after a network
failure; its pending key is reused. A different request or conflicting explicit
key refuses rather than guessing whether a prior operation was accepted.
`--idempotency-key KEY` lets automation supply its own stable identity.
Renewing the same principal's session is supported; switching principals or
accounts refuses the unresolved retry.
Timeouts and unknown gateway responses keep the pending record, as does a
permission refusal on a retry after an uncertain response. A recognized
definitive refusal on the first attempt releases it so the request can be
corrected.

After a validated acceptance, `.omnigraph/last-lifecycle.json` records the
operation and cluster identity. Its pending record is then cleared. These files
contain no credential and grant no permission. They are recovery records;
operation status from the service remains authoritative. Do not discard a
pending record while acceptance is uncertain.

JSON output is one response on stdout; progress and idempotency information use
stderr. A local deadline exits 5 and retains the latest operation response.
Other outcomes use the [managed exit codes](reference.md#managed-cluster-commands).
All lifecycle commands refuse `--direct` and conflicting actor or storage
selectors. Authentication and data credentials remain separate.
