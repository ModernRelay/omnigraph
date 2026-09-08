# Managed data access

This guide covers data credentials for an existing managed cluster. Complete
[managed login and cluster selection](reference.md#managed-cluster-commands)
first; data authority is separate from that control-plane session.

After selecting a managed cluster with `use`, request the data permissions
needed for the intended graph operations:

```bash
omnigraph cluster token --graph knowledge --actions read,change,invoke_query --ttl 1h
omnigraph query find_person --graph knowledge --params '{"name":"Alice"}' --json
omnigraph mutate add_person --graph knowledge --params '{"name":"Alice"}' --json
```

The example invokes stored queries, which require `invoke_query` as well as
`read` or `change`. Ad-hoc query and mutation source uses `read` or `change`
respectively. Every request also passes the graph's Cedar policy; a token
cannot grant an action the policy denies. Control-plane admin or apply
permission does not confer data permission.

`cluster token` requires explicit `--graph` and `--actions`. Accepted actions
are `read`, `export`, `change`, `branch_create`, `branch_delete`, `branch_merge`,
`invoke_query`, and `graph_list`; duplicate actions, wildcards, `admin`, and
`schema_apply` refuse. `--ttl` accepts seconds or an `s`, `m`, `h`, or `d`
suffix, defaults to one hour, and must be between 60 seconds and 24 hours.
The service can shorten the requested lifetime. Issuer clock tolerance is
30 seconds; a credential is never accepted after its stated expiry.
`--json` and human output
contain metadata only, never the signed credential.

The CLI saves one data credential per API origin and cluster in a separate
OS-keychain entry, replacing that cluster's previous entry. It contains the
fixed data endpoint, expiry, key id, actor, and exact grants. There is no
plaintext cache and no fallback to a control-plane session or a named-server
token. Automation can use the origin-bound control credential to run this
command with an available keychain; unattended clients needing a raw token
use the issuance API directly.

Managed `query`, `mutate`, `load`, and `commit list`/`show` read
`.omnigraph/context` only in the current directory and always require
`--graph`. After `cluster token --config DIR`, run data commands from `DIR`;
no parent directory is searched. Ordinary data
requests go directly to the cached endpoint without contacting the control
API. They keep working during an API outage until the token expires or its
signing trust is retired. Each request refuses redirects and accepts at most
8 MiB of response data. Reads have a 10-second deadline. Mutations, including
stored mutations and branch create/delete/merge statements, have a
300-second deadline with at most 10 seconds to connect. A lost write response
never triggers an automatic retry. Managed loads have the bounds described below.

Use `commit list --branch main --graph GRAPH --json` and
`commit show COMMIT_ID --graph GRAPH --json` with a cached `read` grant to
inspect native graph lineage after an uncertain write. Commit identity,
parents and actor attribution are evidence to reconcile; a timeout alone
never proves that a mutation failed to commit.

Missing, malformed, expired, or insufficient cached authority refuses before
a request. An explicit `--server`, `--profile`, `--store`, or `--cluster`
selects ordinary addressing and follows that command's existing support
rules, even in a managed folder or beside malformed context. A positional
load/commit-list URI or `commit show --uri` also selects ordinary addressing.
Other data commands, aliases, and
storage maintenance also keep their ordinary behavior;
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

Legacy token settings never supply managed authority. Global `--direct`
continues to select ordinary addressing and credentials, including when the
context is malformed. Existing `cluster --direct` remains valid. Without
managed context, existing data commands retain their behavior.

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
omnigraph cluster token --graph knowledge --actions read,change,branch_create --ttl 1h
omnigraph load --graph knowledge --data batch-01.jsonl --mode append --branch review --from main --json
omnigraph load --graph knowledge --data batch-02.jsonl --mode append --branch review --json
```

Every load requires `change` for that graph. `--from` additionally requires
`branch_create`, even if the target branch already exists. The same graph
grant must cover both actions. The server checks Cedar's target change
permission and, when creating a branch, its base-to-target creation
permission. `invoke_query` is not required for loading.
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
