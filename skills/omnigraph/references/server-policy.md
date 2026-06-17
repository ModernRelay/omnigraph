# HTTP Server & Cedar Policy

## Contents
- Starting the server (boot sources)
- HTTP routes
- Auth
- Setup operations bypass the server
- Cedar policy
- Multi-graph mode
- MCP surface
- Server + policy together
- Cluster-booted servers

How to run `omnigraph-server` and gate operations with Cedar policies.

## Starting the Server

The server is the canonical runtime entry point — all CLI queries, mutations, and admin ops go through it. **Boot is cluster-only** (RFC-011): the server boots from a cluster and serves N graphs (N ≥ 1) under nested routes. There is **no** single-graph / bare-URI / `omnigraph.yaml` boot.

```bash
omnigraph-server --cluster ./company-brain --bind 127.0.0.1:8080   # a config directory …
omnigraph-server --cluster s3://bucket/prefix --bind 0.0.0.0:8080  # … or a storage-root URI (config-free)
```

`--cluster` boots from the cluster's applied revision (see *Cluster-Booted Servers* below). Run it in a separate terminal or background process.

## HTTP Routes

All per-graph routes are nested under `/graphs/{id}/...` (`{id}` = a graph id from the applied cluster); bare flat paths (`/query`, `/snapshot`, …) return **404**. `/healthz` and `/graphs` stay flat.

| Route | Purpose |
|-------|---------|
| `GET /healthz` | liveness probe (flat) |
| `GET /graphs` | enumerate served graphs (flat; `graph_list`-gated) |
| `GET /graphs/{id}/snapshot?branch=` | table state + row counts |
| `POST /graphs/{id}/query` | read query (canonical; `/read` = deprecated alias) |
| `POST /graphs/{id}/mutate` | mutation (`/change` = deprecated alias) |
| `POST /graphs/{id}/load` | bulk JSONL load, 32 MB; branch creation opt-in via `from` (`/ingest` = deprecated alias) |
| `POST /graphs/{id}/export` | NDJSON stream of a branch |
| `GET /graphs/{id}/queries` · `POST /graphs/{id}/queries/{name}` | stored-query catalog (`read`) + invocation (`invoke_query`, +`change` for a stored mutation; deny == 404) |
| `POST /graphs/{id}/mcp` | MCP surface — built-ins + stored queries as tools, schema/branches as resources (same per-tool Cedar gate; see *MCP surface* below) |
| `GET /graphs/{id}/schema` · `POST /graphs/{id}/schema/apply` | read `.pg` · migrate (`schema_apply`) |
| `GET/POST /graphs/{id}/branches` · `DELETE …/branches/{b}` · `POST …/branches/merge` | branch ops |
| `GET /graphs/{id}/commits?branch=` · `…/commits/{commit_id}` | history |

Read routes take `?branch=main` or `?snapshot=<id>`. Writes publish directly and commit atomically via `__manifest`; use the commits route for write/audit history.

## Auth

Set bearer tokens on the server process. Three sources, in precedence: `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` (AWS Secrets Manager) → `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`/`_FILE` (JSON `{actor_id: token}`) → `OMNIGRAPH_SERVER_BEARER_TOKEN` (single token, actor `default`):

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-reader":"s3cret"}' \
  omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

On the client side (0.7.0), register the server once and store its token out of band:

```bash
echo "s3cret" | omnigraph login remote          # → ~/.omnigraph/credentials (0600)
omnigraph query get_signal --server remote --graph spike --params '{"slug":"sig-foo"}'
```

`--server remote` resolves the URL from `~/.omnigraph/config.yaml`'s `servers:` and the token via `OMNIGRAPH_TOKEN_REMOTE` or the credentials file. A token is only ever sent to the server it is keyed to.

### Running without auth requires an explicit opt-in

You can no longer just "leave auth off." Since v0.6.0 the server **refuses to start** when it has neither bearer tokens nor a policy file, unless you explicitly opt in:

```bash
omnigraph-server --cluster . --unauthenticated
# or: OMNIGRAPH_UNAUTHENTICATED=1 omnigraph-server --cluster .
```

This is a guardrail against accidentally shipping an open server. For pure local dev, pass `--unauthenticated` deliberately.

## Setup Operations Bypass the Server

`init` and **local** `load` write storage directly — they don't go through the server (a **remote** `load` is server-orchestrated, POSTing `/load`). Pass the repo URI:

```bash
omnigraph init --schema schema.pg s3://my-bucket/repos/<name>
omnigraph load --data seed.jsonl --mode overwrite s3://my-bucket/repos/<name>
```

Everything else — `query`, `mutate`, `snapshot`, `schema plan/apply`, `branch`, `commit` — goes through the running server.

## Cedar Policy

Omnigraph can gate sensitive actions with [Cedar](https://www.cedarpolicy.com/) policies.

### Default-deny posture

Policy is enforced engine-wide (every authoring path calls the same gate), and the default is **closed**, not open:

| Server state | Bearer tokens | Policy file | Behavior |
|---|---|---|---|
| **Open** | no | no | Every request permitted — but the server refuses to start without `--unauthenticated` / `OMNIGRAPH_UNAUTHENTICATED=1`. |
| **DefaultDeny** | yes | no | Every authenticated request for an action other than `read` is rejected (HTTP 403). "Tokens but forgot the policy file" no longer ships the illusion of protection. |
| **PolicyEnabled** | yes | yes | Requests are evaluated against your Cedar rules. |

So configuring a policy file is what *enables* writes — there is no "permit everything by default" mode once tokens are set.

### Gated actions

Per-graph actions (evaluated against the graph being addressed):

| Action | Protects |
|--------|----------|
| `read` | query execution |
| `export` | data export |
| `change` | mutations |
| `invoke_query` | stored-query invocation via `POST /graphs/{id}/queries/{name}` (graph-scoped, not branch-scoped). A stored **mutation** is double-gated — it also passes `change`. For a caller without the grant, a denial and an unknown query name both return the same **404** so the catalog can't be probed. |
| `schema_apply` | schema migrations |
| `branch_create` | branch creation |
| `branch_delete` | branch deletion |
| `branch_merge` | merges (especially into protected branches) |

`admin` exists but is reserved (no call site yet — don't write rules for it). A server-scoped `graph_list` action gates `GET /graphs`; declare it in a `[cluster]`-scoped bundle.

For any shared repo, gate at least `schema_apply` and `branch_merge`.

### Where policy is declared

Cedar bundles are declared in `cluster.yaml` and attach via `applies_to`: `[cluster]` is the server-level engine (gates `graph_list` / `GET /graphs`); `[<graph-id>]` is that graph's engine (gates `invoke_query`, `read`, `change`, `branch_*`, `schema_apply`). `cluster apply` publishes them and the `--cluster` server enforces the applied revision. The `policy.yaml` rule format (below) is the bundle content.

### `policy.yaml` shape

The policy model is **allow-only**: every rule is a `permit`. You grant capabilities to groups; anything ungranted is denied by default. There is **no `deny` / `effect` key** — to forbid something, simply don't grant it.

```yaml
version: 1                          # required; must be 1

groups:
  admins: [act-alice, act-bob]
  team:   [act-carol, act-dan]

protected_branches:
  - main

rules:
  - id: admins-can-apply-schema     # rules use `id`, not `name`
    allow:                          # required `allow:` block
      actors: { group: admins }     # references a group by name
      actions: [schema_apply]
      target_branch_scope: protected

  - id: team-can-merge-to-protected
    allow:
      actors: { group: team }
      actions: [branch_merge]
      target_branch_scope: protected

  - id: team-can-read-write-unprotected
    allow:
      actors: { group: team }
      actions: [read, change]
      branch_scope: unprotected
```

To "block unreviewed schema applies," you don't write a deny rule — you just don't grant `schema_apply` to that group. Default-deny does the rest.

Scope rules (a rule's `allow` block may use **at most one**):

- `branch_scope: any | protected | unprotected` — for `read`, `export`, `change` (matches the source branch).
- `target_branch_scope: any | protected | unprotected` — for `schema_apply`, `branch_create`, `branch_delete`, `branch_merge` (matches the destination branch).

### Validate, test, explain

```bash
# Compile Cedar + check the cluster's applied policies
omnigraph policy validate --cluster .

# Run declarative test cases
omnigraph policy test --cluster . --tests policy.tests.yaml

# Debug a single decision
omnigraph policy explain \
  --actor act-alice \
  --action schema_apply \
  --target-branch main \
  --cluster .
```

### Test cases (`policy.tests.yaml`)

```yaml
version: 1                          # required; must be 1
cases:
  - id: alice-can-apply-schema      # cases use `id`, not `name`
    actor: act-alice
    action: schema_apply
    target_branch: main             # schema_apply is target-branch scoped
    expect: allow                   # `allow` / `deny` (not `permit`)

  - id: random-user-cannot-merge-to-main
    actor: act-random
    action: branch_merge
    target_branch: main
    expect: deny
```

Run `policy test` after every policy edit. Tests are cheap.

## Multi-graph serving

A `--cluster` server serves every graph in the applied cluster, each under `/graphs/{id}/...`. `GET /graphs` enumerates them (sorted by id), gated by the cluster-level `graph_list` action — even under `--unauthenticated`, topology stays closed until a `[cluster]` policy grants it. `omnigraph graphs list` mirrors it (remote servers only).

Policy attaches at two levels via `cluster.yaml` `applies_to`:
- `[<graph-id>]` — per-graph rules (`read`, `change`, `branch_*`, `schema_apply`, `invoke_query`).
- `[cluster]` — server-level rules (`graph_list`).

There is no runtime add/remove of graphs — edit `cluster.yaml`, `cluster apply`, restart.

## MCP surface

Since **v0.8.0**, every served graph is also an MCP (Model Context Protocol) server at `POST /graphs/{id}/mcp` — mounted automatically by the `--cluster` server, no extra flag. An MCP agent (Claude, Cursor, OpenAI Responses `mcp` tool) connects with just the URL and the graph's bearer token, and operates the graph through tools:

- **Built-in tools** — `graph_query`, `graph_mutate`, `graph_load`, `graph_snapshot`, `schema_get`, `branch_list`, `branch_create`/`delete`/`merge`, `commit_list`/`get`, `schema_apply` (returns 409 under cluster serving — evolve via `cluster apply`), and `graph_health`.
- **Stored-query tools** — the graph's registry, projected per-query below a threshold (default 24 exposed) or as a `stored_query_list` + `stored_query_run` pair above it. Honors `expose`/`tool_name` (see [`stored-queries.md`](stored-queries.md#mcp-exposure)).
- **Resources** — `omnigraph://schema` and `omnigraph://branches`.

It adds **no new capability**: every tool delegates to the same engine/handler path as the REST routes and passes the **same Cedar gate** (resolved from the same bearer token). Two MCP-specific behaviors to know:

- **`tools/list` is a relaxation of the per-call gate.** A tool is listed if the actor could invoke it on *some* branch, so a callable tool is never hidden — under "protect `main`, write unprotected branches", `graph_mutate` is listed for a branch-writer even though writing `main` is denied. The per-call gate stays authoritative (a denied call returns a tool error). An actor with no write grant at all sees no write tools.
- **Stored-query denials mask as unknown tools.** Behind the coarse `invoke_query` gate (a stored mutation is additionally `change`-gated), so the catalog can't be probed by a caller lacking the grant.

Host/Origin posture is fail-closed and derived from the bind: a loopback bind accepts the full loopback `Host` set (`127.0.0.1`/`::1`/`localhost`); a remote bind rejects unexpected browser `Origin`s. Full client guide: `docs/user/operations/mcp.md`.

## Server + Policy Together

When the server is running with a policy file:
1. Every request resolves the actor from the bearer token (the client cannot set actor identity) and checks it against Cedar rules.
2. Unauthorized requests return `403 Forbidden`.
3. The CLI doesn't bypass policy when it connects over HTTP — it's enforced at the server. Enforcement is also engine-wide, so CLI direct-engine writes and embedded SDK consumers hit the same gate.

Setup ops (`init`, `load`) write storage directly. With a policy configured they still flow through the engine-layer enforce gate for the actor you pass via `--as` (or `operator.actor` in `~/.omnigraph/config.yaml`); gate the raw storage layer too (S3 bucket ACLs, object locks) if the bucket is shared.

## Cluster-Booted Servers

`omnigraph-server --cluster <dir|s3://>` is the only boot source (covered above). It serves the cluster's **applied revision**: `cluster apply` changes take effect on the next restart (no hot reload), and boot is fail-fast with named remedies for missing/pending/tampered state. Bearer tokens and bind stay process-level (env/flags). See `references/cluster.md`.
