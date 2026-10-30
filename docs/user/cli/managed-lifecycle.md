# Discover, configure and retire a managed cluster

These commands use your managed service identity and permissions. They do not
open graph storage or require cloud credentials. The service must support
cluster creation and lifecycle operations; an older service returns a refusal.
Account commands select a profile and resource commands select a target.
Configure `work` as a managed profile and `production` as its target; see
[managed target configuration](reference.md#managed-cluster-commands).

## Find a cluster and recover its configuration

After signing in, list the clusters visible to your selected organization:

```sh
omnigraph login --profile work
omnigraph cluster list --profile work --json
omnigraph target add production --profile work --cluster CLUSTER_ID
omnigraph cluster pull --target production --out customer-config --json
```

`pull` downloads the selected cluster's configuration into a **new** directory.
It does not write a folder binding or change the default target. Keep
`data.revision` for the next `push --expected-revision`; add `--rev FULL_COMMIT_ID`
to retrieve a historical source commit.

The destination's parent must exist. An existing directory, even empty, a file
or a symlink refuses. Download into another new directory to compare remote
changes with local edits; the CLI never merges or overwrites a working tree.
All source files appear together after validation. A failed download
leaves no partial destination. Source contents are not printed in the result.

Download is currently supported on Linux and macOS. The export is bounded to
4096 UTF-8 files, 1 MiB per file, 8 MiB total file contents, 512-byte relative
paths and a 64 MiB JSON response (allowing JSON escaping). Its HTTP deadline is
120 seconds. Traversal, symlinks, conflicting paths and reserved directories
are refused. `.git`, credentials and server-side control records are not
downloaded, and no local routing context is created.
The service requires active membership and organization-wide `source_read`
permission from WorkOS. An external repository binding must be checked out
through its own Git provider instead.

## List cluster members

Use the selected cluster target to list active organization members:

```sh
omnigraph cluster members list --target production
omnigraph cluster members list --target production --json
```

The command requires the organization's current `status` permission. It shows
provider user and membership IDs, available stable principal IDs, identity
eligibility and organization-wide service permissions. Names and emails may be
unavailable. A null principal means no stable identity mapping exists yet; it
is not a missing enrollment requirement. `--json` returns the API envelope,
including the provider observation time and expiry.

The list derives from the cluster's owning organization, with a limit of 1,000
provider memberships. An overflow or incomplete provider read refuses instead
of truncating. This read-only command does not calculate effective graph
permissions: applied Cedar policy authorizes those separately. Organization
administrators use [team administration](managed-team.md) to invite and remove
people and WorkOS to assign service roles.

## Create an empty cluster

Use a complete profile:

```sh
omnigraph login --profile work
omnigraph cluster create customer-demo --profile work --no-wait --json
```

The service reserves a cluster and operation. Creation returns its identity;
it never creates configuration files, binds a directory or changes the selected
target. Keep `data.canonical_root`, `data.incarnation` and `data.operation_id`,
then register it with `target add production --profile work --cluster CLUSTER_ID`. Without `--no-wait`, the
command waits for initial bootstrap and serving readiness. `data.state` is the
operation outcome; `data.lifecycle.phase` reports provisioning progress.

`--no-wait` returns as soon as the operation is accepted. `--timeout SECONDS`
sets a local 1–3600-second deadline; the default is 300. A timeout does not
cancel creation. Follow the saved operation explicitly:

```sh
omnigraph cluster operation OPERATION_ID --profile work --json
omnigraph cluster operation OPERATION_ID --profile work --wait --timeout 600 --json
```

Profiles can observe creation before a cluster target exists. A cluster
target can observe only an operation for its pinned cluster and incarnation.
A `ready` cluster does not imply a reachable endpoint. The selected service must
attest the route; this configuration rewrite does not qualify private/BYOC routes
or tunnel transports.

## Prepare and apply configuration

Creation returns identity without creating local configuration files. After
the operation reaches `ready`, download its initial configuration, including
the policy that grants its creator management access:

```sh
omnigraph cluster pull --target production --out customer-config --json
# Keep data.revision as COMMIT below.
```

Edit `customer-config/cluster.yaml` to add a graph under `graphs`, keeping its
existing storage root and policy declarations:

```yaml
graphs:
  catalog:
    schema: ./catalog.pg
```

Create `customer-config/catalog.pg` with your graph schema, then submit the edit
against the exact revision downloaded:

```sh
omnigraph cluster push --target production --config customer-config --expected-revision COMMIT \
  --message "Add the knowledge graph" --json
# Use data.revision returned by push.
omnigraph cluster plan --target production --rev NEW_COMMIT --json
# Use data.preview_id from the ready preview.
omnigraph cluster apply --target production --plan PREVIEW_ID --json
```

`plan` and `apply` keep their submission open until the `--timeout` deadline
(default 300 seconds, maximum 3600), since the service may answer a plan only
once its preview is captured. Without `--no-wait`, reaching the deadline exits 5 and
prints the idempotency key; acceptance is then unknown. Each source lookup and
delivery poll keeps its own 10-second bound.

`push` uploads only `cluster.yaml`, referenced schemas, stored queries and
policy files. Query-directory declarations discover immediate `.gq` files;
they do not recurse. Unrelated files, folder context and credentials are not
uploaded. Paths must stay within the config directory, with no symlinks or
special files. Files must be UTF-8, at most 2 MiB each, with at most 4096 files
and a 32 MiB request. Path length is limited to 512 bytes and capture to
120 seconds. Managed upload currently requires a Unix platform with
descriptor-relative file traversal; other platforms refuse before submission.

The expected revision is a full 40-character lowercase Git commit ID. A stale
head is refused; pull into another new directory and reconcile your edit before
retrying against that revision. The storage root cannot change. `push` requires
`source_write` and changes only the managed repository. Source-head observation requires
`source_read`; it grants no native management permission. The applied cluster
policy decides whether the caller may plan and deploy configuration. A preview
does not hold a change lease or prevent another source edit.

## Delete and undo

Use the exact incarnation from your cluster's response. `--incarnation` must
equal the configured target and cannot redirect deletion:

```sh
omnigraph cluster delete --target production --incarnation INCARNATION \
  --idempotency-key customer-demo-delete --json
```

Delete needs current organization-wide `delete` permission. It closes new
deployment and data-token issuance; the service then withdraws routing and verifies quiescence. The
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
omnigraph cluster undo-delete --target production --incarnation INCARNATION \
  --deletion-id DELETE_OPERATION_ID --json
```

Undo needs current organization-wide `delete` permission. It preserves the
cluster identity and restores the retained service; it does not authorize a new
configuration deployment. The service decides
whether the retained window is still open; the CLI never clears retirement or
creates a replacement. The configured target remains available after deletion for status and history.

## Recover an uncertain response

Lifecycle commands save their origin, exact request digest and idempotency key
with the authenticated principal and account before sending. Rerun the same
command as the same principal using the same operator home and target after a network
failure; its pending key is reused. A different request or conflicting explicit
key refuses rather than guessing whether a prior operation was accepted.
`--idempotency-key KEY` lets automation supply its own stable identity.
Renewing the same principal's session is supported; switching principals or
accounts refuses the unresolved retry.
Timeouts and unknown gateway responses keep the pending record, as does a
permission refusal on a retry after an uncertain response. A recognized
definitive refusal on the first attempt releases it so the request can be
corrected.

After validated acceptance, the operator home's `lifecycle/` directory retains
the exact request, key, operation and cluster identity before clearing the
pending record. Records are partitioned by API and organization, contain no
credential, and grant no permission. History is bounded to 128 accepted requests
per account and 1 MiB per file; reaching a limit refuses instead of discarding
recovery evidence. A completed request without an explicit key is not silently
submitted again. Use `cluster operation ID` to observe; supplying the retained
key explicitly resubmits only that same idempotent request.

Preserve legacy pending and accepted lifecycle evidence. Use the explicit
[operation-evidence recovery command](targets.md#upgrade-from-the-old-format)
for old folder records; it does not convert config or submit operations. Resume
with the original verified principal and original idempotency key. Never delete
uncertain evidence to force a new submission.

JSON output is one response on stdout; progress and idempotency information use
stderr. A local deadline exits 5 and retains the latest operation response.
Lifecycle outcomes keep their own mapping: ready or purged exits 0, failed 1,
refused or blocked 2, partially converged 3, recovery required 4, stalled or a
local deadline 5, and cancelled 6. A retained tombstone is an observation goal
for deletion with a nonzero undo interval, not completed physical cleanup.
Lifecycle commands refuse `--as` and unsupported target kinds. Authentication and data credentials remain separate.
