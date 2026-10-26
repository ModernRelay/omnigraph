# Managed data access

This guide covers data credentials for an existing managed cluster. Complete
[managed login and cluster selection](reference.md#managed-cluster-commands)
first; data authority is separate from that control-plane session.

After configuring a complete managed target and supplying its selected profile's
credential (login for an `omnigraph` profile), run:

```bash
omnigraph graphs list --target production --discovery
omnigraph query find_person --target production --graph knowledge --params '{"name":"Alice"}' --json
omnigraph mutate add_person --target production --graph knowledge --params '{"name":"Alice"}' --json
```

The target references a profile for API, organization and one auth provider, and
pins cluster ID, incarnation and data endpoint. See [operator configuration](targets.md). A graph operation requires `--graph` or that target's
`default_graph`. Discovery lists graph IDs; it never selects one.

The CLI acquires a missing or expired data credential before submitting the
operation. Human profiles and stored-token profiles use the OS secure store.
Explicit env/file/exec profiles instead keep exchanged credentials only in the
current invocation's memory; they do not probe the OS store or select this path
after a store failure. Managed-target credential export follows the same rule.
External sources still require control-plane identity verification and exchange.
Authentication refresh never replays a submitted mutation.
`cluster token --target production --ttl 1h` is explicit persistent credential
administration and requires the OS secure store, even with an external source.

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

With a cached identity credential, `graphs list --target production --discovery`
shows every graph ID and display name in the server's applied inventory,
including graphs that failed to open. It does not reveal availability,
storage locations, schema, stored queries, or graph contents. A display name
currently equals its graph ID. Listing a graph does not grant access to it;
an absent policy or unknown policy actor still denies protected operations.

Use `--discovery` for this minimal inventory on either managed or standalone
server targets. Without it, `graphs list` requests graph metadata and requires
`graph_list` policy permission. The CLI does not guess the credential type from
its appearance. A server without discovery support, or an incompatible static
credential, refuses without falling back to another inventory.

## Cached access and routing

Data credentials belong to the selected profile and resource binding,
including endpoint and incarnation. They remain separate from control sessions;
there is no plaintext cache or control-token fallback at the graph endpoint.
Use [credential export](targets.md#manage-credentials) to explicitly release a
cluster-bound data token to another process through a pipe.

Managed and standalone access use the shared served graph client: query/mutation,
load, schema reads, branches, snapshots, history, export, Blob operations and
change feeds remain subject to server capabilities and policy. Direct-only
maintenance, schema changes and root administration do not acquire storage
access. `--as` remains refused on managed targets.

A valid data cache must match the captured endpoint, incarnation and identity.
A cached human sign-in or unchanged stored-token profile can support data access
during a control outage until expiry or signing-trust retirement. Replacing or
removing a stored token invalidates its cache generation. External env/file/exec
profiles require control verification and invocation-only exchange; do not assume
universal control-plane independence. Missing or expired access uses only the
selected provider; auth never falls back or replays an uncertain write.

Managed JSON requests refuse redirects and accept at most 8 MiB of response data,
with a 30-second deadline and at most 10 seconds to connect.
A lost write response never triggers an automatic retry. Managed loads
have the bounds below; Blob and streaming operations keep their own bounds.

Use `commit list --target production --branch main --graph knowledge --json`
and `commit show COMMIT_ID --target production --graph knowledge --json` to
inspect native lineage after an uncertain write. Commit identity, parents and
actor attribution are evidence; a timeout alone never proves that a mutation
failed to commit.

An explicit `--target staging` selects staging independently of source directories.
Configuration and cached credentials cannot silently reroute an invocation.
Preserve original recovery evidence when changing targets or credentials.

## macOS Keychain access failures

An upgrade can change the executable identity trusted by a Keychain item.
Ad-hoc signing does not provide a stable identity across builds. Stable release
trust requires consistent Developer ID signing and a fixed signing identifier;
an existing item approved for an ad-hoc build can still require authorization
when moving to a signed release. Several items can share the same service label,
so approving one item does not approve every session and data-cache entry.

Inspect the installed executable without reading credentials:

```bash
codesign -dv --verbose=4 "$(command -v omnigraph)"
codesign -d -r- "$(command -v omnigraph)"
```

On a secure-store failure, retain the operation, classified reason and numeric
OSStatus when present, together with `omnigraph version`. Do not export secrets
to diagnose it. An authorization failure alone does not prove that a password
is wrong, a keychain is locked, or a credential is missing. Do not repeatedly
approve prompts, delete or reset the keychain, allow all applications, or disable
signature checks as a workaround. Report repeated prompts to the maintainer.
See Apple's [code identity requirements](https://developer.apple.com/documentation/technotes/tn3127-inside-code-signing-requirements).

## Clear a credential

`cluster token --target production --clear` forgets that cluster's persistent local data
entry independently of its control session. Do not combine `--clear` with
`--graph` or `--ttl`. Clearing is not server revocation: copies remain usable
until expiry or signing-key retirement. `logout --profile work` clears the
selected local session and its derived data cache, then requests provider-session revocation;
`provider_revocation_confirmed` reports its outcome. Copies of issued data
credentials can remain valid remotely until expiry or signing-key retirement.

Local clearing does not require network transport. Logout also removes local
custody when the configured CA is unavailable or proxy selection fails, reporting
remote revocation as unconfirmed rather than retaining the session.

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
