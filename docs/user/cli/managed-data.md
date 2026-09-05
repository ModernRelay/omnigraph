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

Managed `query` and `mutate` read `.omnigraph/context` only in the current
directory and always require `--graph`. After `cluster token --config DIR`,
run data commands from `DIR`; no parent directory is searched. Ordinary data
requests go directly to the cached endpoint without contacting the control
API. They keep working during an API outage until the token expires or its
signing trust is retired. Each request refuses redirects, has a 10-second
deadline, and accepts at most 8 MiB of response data.

Missing, malformed, expired, or insufficient cached authority refuses before
a request. In a managed folder, other data commands, aliases, and storage
maintenance are unsupported. Explicit `--server`, `--profile`, `--store`,
`--cluster`, and `--as` refuse.
Inherited profiles and legacy token settings are ignored. Use global
`--direct` to explicitly select legacy addressing and credentials, including
when the context is malformed. Existing `cluster --direct` remains valid.
Without managed context, existing data commands retain their behavior.

`cluster token --clear [--config DIR]` forgets that cluster's local data
entry, independently of the control-plane session. Do not combine `--clear`
with `--graph`, `--actions`, or `--ttl`. Clearing is not server revocation:
copies remain usable until expiry or signing-key retirement. Likewise,
`logout --api` revokes only the control-plane login and does not invalidate
already issued data credentials.
