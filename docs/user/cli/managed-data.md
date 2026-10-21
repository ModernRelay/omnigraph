# Managed data access

This guide covers data credentials for an existing managed cluster. Complete
[managed login and cluster selection](reference.md#managed-cluster-commands)
first; data authority is separate from that control-plane session.

After configuring and logging in to a complete managed cluster target, run:

```bash
omnigraph graphs list --target production
omnigraph query find_person --target production --graph knowledge --params '{"name":"Alice"}' --json
omnigraph mutate add_person --target production --graph knowledge --params '{"name":"Alice"}' --json
```

The target pins the API, organization, cluster ID, incarnation, control-authentication
mechanism and data endpoint. A graph operation requires `--graph` or that target's
`default_graph`. Discovery lists graph IDs; it never selects one.

The CLI acquires a missing or expired identity credential before submitting
the operation and caches it in the OS keychain. No separate token command is
needed. A valid cached graph credential for your cached sign-in avoids an
unrelated API session check; an explicit automation token verifies its own
principal before reusing that cache. Authentication refresh never replays a
submitted mutation. Explicit
`cluster token --target production --ttl 1h` remains available for credential administration.

The issuer must support identity credentials and admit your principal to the
selected cluster. The credential proves who you are and which cluster you
can connect to; it contains no graph or action permissions. The cluster's
applied [Cedar policy](../operations/policy.md) supplies those permissions.
For the example's stored queries, it must permit `invoke_query` plus `read`
or `change`. Ad-hoc query and mutation source uses `read` or `change`
respectively. Control-plane admin or apply permission does not confer graph
permission.

Apply graph policy grants, revocations and binding changes through the cluster
configuration workflow; editing a source file alone has no effect. Changes use the
[cluster configuration workflow](../clusters/index.md#deploy-without-restarting);
identity credentials do not bypass its ownership or permission checks.

Issuance takes no `--graph`; choose a graph on the
operation that needs it. `--ttl` accepts seconds or an `s`, `m`, `h`, or `d`
suffix, defaults to one hour, and must be between 60 seconds and 24 hours.
The service can shorten the requested lifetime. Issuer clock tolerance is
30 seconds; a credential is never accepted after its stated expiry.
`--json` and human output contain metadata only, never the signed credential.

## Discover graphs

With a cached identity credential, `graphs list --target production`
shows every graph ID and display name in the server's applied inventory,
including graphs that failed to open. It does not reveal availability,
storage locations, schema, stored queries, or graph contents. A display name
currently equals its graph ID. Listing a graph does not grant access to it;
an absent policy or unknown policy actor still denies protected operations.

For an explicitly addressed server whose configured credential is an identity
credential, select this minimal inventory with `--discovery`:

```bash
omnigraph graphs list --target prod --discovery --json
```

Without `--discovery`, an explicit server keeps the existing graph-metadata
listing and requires `graph_list` policy permission. The CLI does not guess
the credential type from a token's appearance. A server that lacks discovery
support, or a static credential, refuses this route without falling
back to another inventory.

## Cached access and routing

The CLI saves one data credential per API origin and cluster in a separate
OS-keychain entry, replacing that cluster's previous entry. The versioned
cache binds the fixed data endpoint, cluster incarnation, expiry, key ID, and
actor to the identity credential. There is no plaintext cache and no fallback
to a control-plane session or a named-server token. Automation can use an
origin-bound control credential to acquire identity credentials with an available
keychain; unattended clients needing a raw token use the issuance API directly.

Managed `query`, `mutate`, `load`, `commit list/show`, and `graphs list` use the
selected target. No directory search or folder context supplies authority.
Other graph wrappers and storage maintenance are unavailable for managed
targets. `--as` is refused because the data credential supplies the actor.

A valid cached data credential must match the configured endpoint and
incarnation. Ordinary data requests then go directly to that endpoint without a
control API request. They keep working during a control API outage until expiry
or signing-trust retirement. A malformed cache or conflicting identity refuses
without selecting another credential or modifying the target.

Human-device targets use only their human session for issuance; automation-env
targets use only the origin-bound control environment pair. The mechanisms never
fall back to each other. An explicit automation credential verifies its principal
before cache reuse. A valid independent data cache can remain usable without a
control credential; an absent or expired data credential requires the configured
control mechanism to obtain another one.

Requests refuse redirects and accept at most 8 MiB of response data. Queries,
commit reads and mutations have a 30-second deadline with at most 10 seconds to
connect. A lost write response never triggers an automatic retry. Managed loads
have the bounds below.

Use `commit list --target production --branch main --graph knowledge --json`
and `commit show COMMIT_ID --target production --graph knowledge --json` to
inspect native lineage after an uncertain write. Commit identity, parents and
actor attribution are evidence; a timeout alone never proves that a mutation
failed to commit.

An explicit `--target staging` selects staging even beside old production
folder context. If no target is specified, a legacy context refuses instead of
silently supplying or competing with a default. Import old recovery evidence
before performing new lifecycle operations.

## Clear a credential

`cluster token --target production --clear` forgets that cluster's local data
entry independently of its control session. Do not combine `--clear` with
`--graph` or `--ttl`. Clearing is not server revocation: copies remain usable
until expiry or signing-key retirement. `logout --target account` clears the
selected human control session and requests provider-session revocation;
`provider_revocation_confirmed` reports its outcome. Already issued data
credentials remain independent.

## Bulk loading

Use the ordinary `load` syntax with the selected target, an explicit
graph and mode. The native CLI sends the exact UTF-8 NDJSON body to the
authenticated data endpoint; it does not write storage directly.

```bash
omnigraph load --target production --graph knowledge --data batch-01.jsonl --mode append --branch review --from main --json
omnigraph load --target production --graph knowledge --data batch-02.jsonl --mode append --branch review --json
```

Every load requires `change` for that graph. `--from` additionally requires
`branch_create` when creating the target branch. Applied Cedar policy owns
target change permission and the base-to-target creation permission.
Identity credentials add no action ceiling. `invoke_query` is not required for loading.
Review and merge use the existing branch operations and their separate
permissions; a load does not merge its target branch.

One managed load accepts at most **32 MiB (33,554,432 bytes)** of input. The
CLI checks this before sending the request. Incremental `append` and `merge`
also retain the engine's **8,192 rows and 32 MiB per keyed table** bounds, plus
**32 MiB across retained keyed batches** and a separate parsed-payload estimate
across types. Strict loads check projected in-memory size too. The NDJSON byte
bound does not prove that a batch fits those
[engine limits](../mutations/index.md#limits-and-conflicts).
Split prepared `append` and `merge` inputs into suitable batches, preserving
endpoint dependencies and recording each batch's returned commit. Blob values carry
their own [limits](../blobs.md#limits).

`overwrite` also refuses when the IDs it removes exceed **32 MiB per
operation**, summed over all touched types. Each removed ID is charged its UTF-8
length plus 24 bytes, so the allowance holds 671,088 IDs of 26 bytes, the
length of a generated ID. Entities loaded without a `@key` and without an
explicit `id` get a new generated ID on every load, so overwriting them removes
every committed ID of that type. A delete and the edges it cascades to draw on
the same allowance. No setting changes it. An overwrite cannot be split; a
delete can be split into several commits.

The request has a **300-second deadline**, including receipt download, with
at most **10 seconds to connect** and **8 MiB of response data**. The CLI
never follows redirects or automatically retries the load. A timeout,
interrupted connection, response-limit failure, or invalid receipt can occur
after the server created a branch or committed data. Preserve the batch,
target and any response, then inspect that branch before deciding whether to
retry. `--from` is not an idempotency key. A success receipt's
`commit.actor_id` records the authenticated actor.

These managed bounds apply only to managed targets. Direct and server targets
use their own credential and transport contracts.
