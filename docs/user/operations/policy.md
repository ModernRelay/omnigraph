# Authorization (Cedar policy)

OmniGraph integrates AWS Cedar (`cedar-policy = 4.9`) for ABAC.

## Policy actions

Per-graph actions (bind to `Omnigraph::Graph::"<graph_id>"`):

1. `read` — query / snapshot / list branches & commits
2. `export` — NDJSON export
3. `change` — mutations
4. `schema_apply` — apply schema migrations
5. `branch_create`
6. `branch_delete`
7. `branch_merge`
8. `admin` — reserved for policy-management surfaces (hot reload, audit log, approvals). No call site today.
9. `invoke_query` — gates invoking a server-side stored query (the `queries:` registry). Graph-scoped (like `admin`) — per-branch access is enforced by the inner `read` / `change` gate, so a rule that sets `branch_scope` on `invoke_query` is rejected. Coarse in this release: an `invoke_query` allow rule permits any stored query on the graph; a future, additive refinement adds an optional per-query-name scope without changing rules written against the coarse action. Enforced at `POST /queries/{name}` (see [server](server.md)). A stored *mutation* is double-gated: `invoke_query` to reach the tool, plus `change` for the write itself (the engine `_as` writers still enforce per the query body).

Server-scoped action (v0.6.0+; binds to `Omnigraph::Server::"root"`):

10. `graph_list` — `GET /graphs` registry enumeration (multi-graph mode)

Server-scoped actions cannot use `branch_scope` or `target_branch_scope` — they operate on the registry, not on a graph's branches. A rule cannot mix server-scoped and per-graph actions; split into separate rules. (Runtime `graph_create` / `graph_delete` over HTTP are reserved but not shipped; operators add/remove graphs by editing the cluster's `cluster.yaml`, running `omnigraph cluster apply`, and restarting the server.)

## Scope kinds

- `branch_scope` — applied to source branch (`read`, `export`, `change`)
- `target_branch_scope` — applied to destination (`schema_apply`, branch ops, run ops)
- `protected_branches` — named list with special rules; rule scopes are `any | protected | unprotected`

## Per-graph vs. server-level policy

A server boots from a cluster (`--cluster <dir>`), and the cluster's
`cluster.yaml` declares its policy bundles in a `policies:` section. Each bundle
names the scopes it `applies_to`: a graph id (per-graph rules — `read`, `change`,
`branch_*`, `schema_apply`) or the literal `cluster` (server-level rules —
`graph_list`).

```yaml
# cluster.yaml
policies:
  base:
    file: base.policy.yaml
    applies_to: [cluster, knowledge]   # cluster-level + the `knowledge` graph
  alpha:
    file: policies/alpha.yaml
    applies_to: [alpha]                # per-graph: alpha only
```

A graph with no bundle bound to it has no engine-layer Cedar enforcement. Each
graph's HTTP request flows through its bound bundle; the management endpoint
(`GET /graphs`) flows through the `cluster`-scoped bundle. When no bundle binds
`cluster`, `GET /graphs` is denied in every runtime state, including
`--unauthenticated`; with bearer tokens configured it returns 403 after admission
control because `graph_list` is not a `read`-equivalent action. The operator must
bind a `cluster`-scoped bundle granting `graph_list` to expose `/graphs`.

Example `cluster`-scoped bundle:

```yaml
version: 1
groups:
  admins: [act-andrew]
rules:
  - id: admins-can-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
```

Each per-graph rule may use at most one of `branch_scope` or
`target_branch_scope`. Server-scoped rules (`graph_list`) take neither — they
have no branch context.

## Actor for direct-engine writes

The default actor identity for CLI direct-engine (`--store`) writes is
`operator.actor` in `~/.omnigraph/config.yaml`. Override per-invocation with
`--as <ACTOR>` — `--as` wins, otherwise `operator.actor`, otherwise no actor.
Remote HTTP writes ignore both — they resolve their actor server-side from the
bearer token. (Direct-store access carries no Cedar policy; policy
lives in the cluster/server.)

## CLI

Policy tooling reads a cluster's applied policy bundles: pass `--cluster <dir>`,
and `--graph <id>` to pick a graph's bundle when several apply.

- `omnigraph policy validate` — parse + count actors, exit 1 on parse error.
- `omnigraph policy test --tests <file>` — run the declarative cases in `<file>` against the selected bundle, exit 1 on any expectation mismatch.
- `omnigraph policy explain --actor … --action … [--branch …] [--target-branch …]` — show decision and matched rule.
- `omnigraph --as <ACTOR> <subcommand>` — set the actor for the duration of one invocation. Effective for `change`, `load` (and its deprecated `ingest` alias), `branch create|delete|merge`, and `schema apply` against a direct (`--store`) graph. **Rejected** on a served write (`--server`): the actor is bearer-token-resolved server-side, so `--as` can't set it there.

## Enforcement

Policy is a property of the **engine**, not the transport. Every mutating
write — `mutate_as`, `load_as` (the deprecated `ingest_as` shims route
through it), `apply_schema_as`,
`branch_create_as`, `branch_create_from_as`, `branch_delete_as`,
`branch_merge_as` — consults the policy gate at the head of the method.
The gate fires identically whether the call
originates from the HTTP server, the CLI, or an embedded SDK consumer.
When no policy is installed (the dev/embedded default) the gate
is a strict no-op; when one is installed and the call site forgets to
thread an actor through, the gate fails closed rather than silently
bypassing.

## Server runtime states

The HTTP server classifies its startup configuration into one of three
states based on whether bearer tokens are configured and whether a
policy file is set. The state determines what happens to a request that
reaches the authorization gate without a matching policy permit.

| State | Tokens | Policy file | Behavior |
|---|---|---|---|
| **Open** | no | no | Every request is permitted. Refuses to start unless `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1` is set — the operator must explicitly opt in. |
| **DefaultDeny** | yes | no | Every authenticated request for an action other than `read` is rejected with HTTP 403. Closes the "tokens but forgot the policy file" trap — an operator who sets up auth and forgot to point at a policy file used to ship the illusion of protection. |
| **PolicyEnabled** | yes | yes | Authenticated requests that reach a configured policy engine are evaluated by Cedar. Server-scoped actions still require a `cluster`-scoped policy bundle. |

The server refuses to start for the "no tokens, no policy, no flag" cell
and for "policy file, no tokens" — instead of silently shipping an open
instance or a policy-protected server that can only 401.

Server-side, request authorization still runs at the HTTP boundary —
that's where actor identity is resolved from the bearer token and where
admission control / per-actor rate limits live. Engine-layer enforcement
is the **defense in depth** layer: it catches CLI direct-engine writes,
embedded SDK consumers, and any future transport that hasn't (or won't)
re-implement the HTTP boundary's authorization. Both layers consult the same
Cedar policy, so decisions cannot disagree.

## Coarse vs. fine enforcement

There are two enforcement points, each with non-overlapping
responsibilities:

| Layer | Question it answers | Where it fires |
|---|---|---|
| **Engine-layer (coarse)** | Can this actor invoke this action against this branch / branch-transition? | The policy gate at the head of every `_as` writer; one Cedar decision per call. |
| **Query-layer (fine)** | For the rows / types this action actually touches, which can the actor see or modify? | Per-row predicates pushed into the query plan. **Not yet implemented.** |

The engine-layer gate keeps its resource scope deliberately at branch
granularity (graph, branch, target branch, branch transition).
Per-type and per-row authority is the query-layer's job; conflating them
in the engine-layer scope would create two places per-type policy could be
evaluated and a drift surface between them.

## Actor identity (signed-claim-only)

The actor identity used for every policy decision comes from the matched bearer token — never from a client-supplied request header, query parameter, or body field. The server resolves the token at the auth middleware boundary, looks up the actor it was minted for, and overwrites whatever the handler may have placed in the policy request. Clients cannot set `actor_id` directly.

This is intentional. Trusting client-supplied identity for authorization is "asking the attacker if they're an admin" — Supabase's RLS history names the same footgun. The chokepoint lives at the server's auth boundary: a request with `Authorization: Bearer <token-for-actor-A>` plus `X-Actor-Id: actor-B` always evaluates as actor A, never as actor B.

If you find yourself wanting to let clients override `actor_id` for impersonation, delegation, or service-account flows — that's a feature, but it needs explicit design (e.g., signed delegation claims, an `On-Behalf-Of` audit trail). It is not a convenience knob.
