# HTTP Server and Cedar Policy

Use this reference for a cluster-served graph. The server is cluster-only; CLI
commands can instead open graph storage directly when their help permits it.

## Start a server

```bash
export OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-alice":"replace-with-a-secret"}'
omnigraph-server --cluster ./company-brain --bind 127.0.0.1:8080
omnigraph-server --cluster s3://bucket/prefix --bind 0.0.0.0:8080
```

The server loads the cluster's applied revision and serves each graph under
`/graphs/{id}`. Apply changes, then restart; there is no hot reload or
single-graph server mode.

## Route families

| Route family | Purpose |
|---|---|
| `GET /healthz`, `/openapi.json` | Process metadata |
| `GET /graphs` | List served graphs (`graph_list`) |
| `/graphs/{id}/query`, `/mutate` | Inline GQ reads and writes |
| `/graphs/{id}/mutate/if-graph-commit` | Conditional inline mutation |
| `/graphs/{id}/queries` | List/invoke stored queries, including conditional writes |
| `/graphs/{id}/load`, `/load/ndjson` | Bounded atomic loads |
| `/graphs/{id}/blob` | GET/HEAD one Blob cell |
| `/graphs/{id}/branches` | Branch operations and merge |
| `/graphs/{id}/snapshot`, `/commits` | Snapshot and history |
| `/graphs/{id}/commits/{commit}/changes` | One first-parent commit diff |
| `/graphs/{id}/changes` | Poll a feed or capture a baseline |
| `/graphs/{id}/schema` | Read the accepted schema |
| `/graphs/{id}/export` | Stream a branch snapshot |

`/read`, `/change`, and `/ingest` are deprecated compatibility routes. A
cluster-only server retains `POST /graphs/{id}/schema/apply` for wire
compatibility but rejects it with `409`; use `cluster plan`/`cluster apply`.

Canonical `/query` JSON includes the graph commit pinned with its rows when the
read snapshot has an effective graph head; a fresh pre-commit graph can omit it. Exact
write receipts and conditional semantics are summarized in
[commit changes and feeds](changes.md). Blob delivery is in [Blob values](blobs.md).
The deprecated `/read` response does not carry that commit position; consumers
that need conditional writes must use `/query`.

## Authentication and actor identity

Bearer tokens map actors at the server boundary. Request headers and bodies
cannot choose another actor. Configure token sources on the server process;
clients can store a named server token with:

```bash
echo "$TOKEN" | omnigraph login production
omnigraph query get_person --server production --graph knowledge
```

A server with neither tokens nor policy refuses to start unless explicitly
given `--unauthenticated` (or `OMNIGRAPH_UNAUTHENTICATED=1`). Use that only on a
trusted development network. Tokens without a policy allow only `read`; other
actions remain denied.

## Cedar actions

Graph-scoped actions are:

| Action | Covers |
|---|---|
| `read` | Queries, snapshots, branches, commits, Blob reads, and change polling |
| `export` | Export and change-feed baseline capture |
| `change` | Mutations and loads |
| `schema_apply` | Schema changes in an engine host that exposes them |
| `branch_create`, `branch_delete`, `branch_merge` | Corresponding branch operation |
| `invoke_query` | Entry to a stored query |
| `admin` | Reserved; no current public operation |

`graph_list` is cluster-scoped and controls `GET /graphs`. A stored read needs
`invoke_query` plus `read`; a stored mutation needs `invoke_query` plus
`change`. A denied and unknown stored-query name both appear as `404` to a
caller lacking `invoke_query`.

## Declare and test policy

Bind one bundle to graph ids and another, if needed, to the cluster scope:

```yaml
# cluster.yaml
policies:
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge]
  server-access:
    file: server.policy.yaml
    applies_to: [cluster]
```

Policies are allow-only; omit a grant to deny it:

```yaml
version: 1
groups:
  readers: [act-alice, act-bob]
rules:
  - id: readers-can-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
  - id: readers-can-invoke
    allow:
      actors: { group: readers }
      actions: [invoke_query]
```

`branch_scope` protects source branches; `target_branch_scope` protects
destinations. Values are `any`, `protected`, or `unprotected`; one rule cannot
set both scopes. `invoke_query` and server actions take no branch scope.

```bash
omnigraph policy validate --cluster . --graph knowledge
omnigraph policy test --cluster . --graph knowledge --tests policy.tests.yaml
omnigraph policy explain --cluster . --graph knowledge \
  --actor act-alice --action read --branch main
```

Run `cluster apply` and restart servers after policy changes.

## Direct access is a separate trust boundary

Served requests always use the server's policy engine. Embedded engine hosts
can also install a policy engine, and every mutating engine entry point then
enforces it.

The standalone CLI opening `--store` or a positional URI does **not** load the
cluster server's Cedar bundle. Its `--as` value records actor attribution but
does not recreate server authorization. Protect raw graph storage with object
store IAM/ACLs and restrict who can run direct maintenance. Served writes reject
client-supplied actor identity because only the token may select it.

Canonical contracts: [server operations](../../../docs/user/operations/server.md)
and [authorization](../../../docs/user/operations/policy.md).
