# Managed data access

This guide covers data credentials for an existing managed cluster. Complete
[managed login and cluster selection](reference.md#managed-cluster-commands)
first; data authority is separate from that control-plane session.

After login and selecting a managed cluster with `use`, run graph commands:

```bash
omnigraph graphs list
omnigraph query find_person --graph knowledge --params '{"name":"Alice"}' --json
omnigraph mutate add_person --graph knowledge --params '{"name":"Alice"}' --json
```

The CLI acquires a missing or expired identity credential before submitting
the operation and caches it in the OS keychain. No separate token command is
needed. A valid cached graph credential for your cached sign-in avoids an
unrelated API session check; an explicit automation token verifies its own
principal before reusing that cache. Authentication refresh never replays a
submitted mutation. Explicit
`cluster token --ttl 1h` remains available for credential administration.

The issuer must support identity credentials and admit your principal to the
selected cluster. The credential proves who you are and which cluster you
can connect to; it contains no graph or action permissions. The cluster's
applied [Cedar policy](../operations/policy.md) supplies those permissions.
For the example's stored queries, it must permit `invoke_query` plus `read`
or `change`. Ad-hoc query and mutation source uses `read` or `change`
respectively. Control-plane admin or apply permission does not confer graph
permission.

When a changed policy is applied and activated by a server restart, it governs
the next request using the same credential. Editing a source file alone has
no effect. Schema changes retain the [cluster configuration
workflow](../operations/policy.md#actions); identity credentials do not bypass
its ownership or permission checks.

Normal issuance takes neither `--graph` nor `--actions`; choose a graph on the
operation that needs it. `--ttl` accepts seconds or an `s`, `m`, `h`, or `d`
suffix, defaults to one hour, and must be between 60 seconds and 24 hours.
The service can shorten the requested lifetime. Issuer clock tolerance is
30 seconds; a credential is never accepted after its stated expiry.
`--json` and human output contain metadata only, never the signed credential.

## Discover graphs

With a cached identity credential, `graphs list` from the managed folder
shows every graph ID and display name in the server's applied inventory,
including graphs that failed to open. It does not reveal availability,
storage locations, schema, stored queries, or graph contents. A display name
currently equals its graph ID. Listing a graph does not grant access to it;
an absent policy or unknown policy actor still denies protected operations.

For an explicitly addressed server whose configured credential is an identity
credential, select this minimal inventory with `--discovery`:

```bash
omnigraph graphs list --server prod --discovery --json
```

Without `--discovery`, an explicit server keeps the existing graph-metadata
listing and requires `graph_list` policy permission. The CLI does not guess
the credential type from a token's appearance. A server that lacks discovery
support, or a static or legacy credential, refuses this route without falling
back to another inventory.

## Cached access and routing

The CLI saves one data credential per API origin and cluster in a separate
OS-keychain entry, replacing that cluster's previous entry. The versioned
cache binds the fixed data endpoint, cluster incarnation, expiry, key ID, and
actor to the identity credential. There is no plaintext cache and no fallback
to a control-plane session or a named-server token. Automation can use the
origin-bound control credential to run this command with an available
keychain; unattended clients needing a raw token use the issuance API directly.

Managed `query`, `mutate`, `load`, `commit list`/`show`, and `graphs list`
read `.omnigraph/context` only in the current directory; graph-specific
commands require `--graph`. After
`cluster token --config DIR`, run these commands from `DIR`; no parent
directory is searched. Ordinary data requests go directly to the cached
endpoint without contacting the control
API. They keep working during an API outage until the token expires or its
signing trust is retired. Each request refuses redirects and accepts at most
8 MiB of response data. Queries, commit reads, and mutations, including
stored mutations and branch create/delete/merge statements, have a
30-second deadline with at most 10 seconds to connect. A lost write response
never triggers an automatic retry. Managed loads have the bounds described below.

Use `commit list --branch main --graph GRAPH --json` and
`commit show COMMIT_ID --graph GRAPH --json` with your cached identity and
`read` permission in applied policy to inspect native graph lineage after an
uncertain write. Commit identity,
parents and actor attribution are evidence to reconcile; a timeout alone
never proves that a mutation failed to commit.

Missing, malformed, or expired cached credentials refuse before a request;
the server decides policy permissions. An explicit `--server`, `--profile`,
`--store`, or `--cluster` selects ordinary addressing and follows that
command's existing support
rules, even in a managed folder or beside malformed context. A positional
load/commit-list URI or `commit show --uri` also selects ordinary addressing.
Other data commands, aliases, and storage maintenance keep their ordinary behavior;
this does not give them managed credentials. Explicit `--as` alone is not a
target and remains prohibited on managed requests.

These implicit data commands refuse with `managed_target_ambiguous` when valid
folder context competes with `OMNIGRAPH_PROFILE` or an operator default server
or store. No credential is read and neither destination is contacted. Choose
the ordinary target explicitly, or use `--direct` to select ordinary ambient
resolution. To use the managed folder, unset the environment profile and
remove the competing default target, using a separate operator home if needed.
Presentation defaults and profiles that are merely defined do not conflict.

For example, this keeps using staging from a folder bound to production:

```bash
omnigraph query find_person --profile staging --graph knowledge --json
```

Ordinary token settings never supply managed authority. Global `--direct`
continues to select ordinary addressing and credentials, including when the
context is malformed. Existing `cluster --direct` remains valid. Without
managed context, existing data commands retain their behavior.

## Legacy restricted credentials

For an issuer that still supports the older restricted profile, request it
explicitly by supplying both a graph and
the exact action ceiling:

```bash
omnigraph cluster token --graph knowledge --actions read,change,invoke_query --ttl 1h
```

The provider-authenticated service uses version-2 identity issuance only and
rejects this legacy request. The CLI retains the syntax for compatible older
issuers; it never converts requested action restrictions into identity-only
authority.

Accepted actions are `read`, `export`, `change`, `branch_create`,
`branch_delete`, `branch_merge`, `invoke_query`, and `graph_list`. Duplicate
actions, wildcards, `admin`, `config_manage`, and `schema_apply` refuse. Both
the signed ceiling and applied Cedar policy must allow an operation. The CLI
rejects operations outside the cached ceiling before a request. Managed
`graphs list` requires an identity credential; it never upgrades a restricted
credential to gain discovery access. The legacy HTTP metadata catalog retains
its graph filtering.

Existing restricted caches keep their version and exact grants. An explicit
`--actions` request is never ignored or silently changed to an identity
credential. An unsupported issuance profile refuses and preserves the previous
cache. Running normal `cluster token` is an explicit request to replace it
with the identity profile, subject to the issuer's admission check. Older CLI
versions that only understand restricted credentials cannot use that cache;
they must be upgraded or use explicit restricted issuance.

## Clear a credential

`cluster token --clear [--config DIR]` forgets that cluster's local data
entry, independently of the control-plane session. Do not combine `--clear`
with `--graph`, `--actions`, or `--ttl`. Clearing is not server revocation:
copies remain usable until expiry or signing-key retirement. Likewise,
`logout --api` revokes only the control-plane login and does not invalidate
already issued data credentials.

## Bulk loading

Use the ordinary `load` syntax from the selected folder, with an explicit
graph and mode. The native CLI sends the exact UTF-8 NDJSON body to the
authenticated data endpoint; it does not write storage directly.

```bash
omnigraph load --graph knowledge --data batch-01.jsonl --mode append --branch review --from main --json
omnigraph load --graph knowledge --data batch-02.jsonl --mode append --branch review --json
```

Every load requires `change` for that graph. `--from` additionally requires
`branch_create` when creating the target branch. Applied Cedar policy owns
target change permission and the base-to-target creation permission.
Identity credentials add no action ceiling. The legacy restricted profile
additionally requires `change` and, whenever `--from` is present,
`branch_create` in the same graph grant. `invoke_query` is not required for loading.
Review and merge use the existing branch operations and their separate
permissions; a load does not merge its target branch.

One managed load accepts at most **32 MiB (33,554,432 bytes)** of input. The
CLI checks this before sending the request. Incremental `append` and `merge`
also retain the engine's **8,192 rows and 32 MiB per keyed table** bounds;
strict loads check projected in-memory size too. The NDJSON byte bound does
not prove that a batch fits those [engine limits](../mutations/index.md#limits-and-conflicts).
Split prepared inputs into suitable batches, preserving endpoint dependencies
and recording each batch's returned commit.

The request has a **300-second deadline**, including receipt download, with
at most **10 seconds to connect** and **8 MiB of response data**. The CLI
never follows redirects or automatically retries the load. A timeout,
interrupted connection, response-limit failure, or invalid receipt can occur
after the server created a branch or committed data. Preserve the batch,
target and any response, then inspect that branch before deciding whether to
retry. `--from` is not an idempotency key. A success receipt's
`commit.actor_id` records the authenticated actor.

These managed bounds do not change ordinary direct, named-server or profile
loading. Explicit addressing and `--direct` continue to use their existing
credentials and transport.
