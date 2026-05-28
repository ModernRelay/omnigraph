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
8. `admin` — reserved for policy-management surfaces (hot reload, audit log, approvals). No call site today; see MR-724 for the reservation rationale.

Server-scoped action (v0.6.0+; binds to `Omnigraph::Server::"root"`):

9. `graph_list` — `GET /graphs` registry enumeration (multi-graph mode)

Server-scoped actions cannot use `branch_scope` or `target_branch_scope` — they operate on the registry, not on a graph's branches. A rule cannot mix server-scoped and per-graph actions; split into separate rules. (Runtime `graph_create` / `graph_delete` are reserved but not shipped in v0.6.0; operators add/remove graphs by editing `omnigraph.yaml` and restarting.)

## Scope kinds

- `branch_scope` — applied to source branch (`read`, `export`, `change`)
- `target_branch_scope` — applied to destination (`schema_apply`, branch ops, run ops)
- `protected_branches` — named list with special rules; rule scopes are `any | protected | unprotected`

## Per-graph vs. server-level policy (multi-graph mode)

In multi mode (`omnigraph.yaml` with a non-empty `graphs:` map), policy files attach at two levels:

```yaml
server:
  policy:
    file: ./server-policy.yaml          # server-level: graph_list

graphs:
  alpha:
    uri: s3://tenant-bucket/alpha
    policy:
      file: ./policies/alpha.yaml       # per-graph: read, change, branch_*, schema_apply
  beta:
    uri: s3://tenant-bucket/beta
    # no per-graph policy → no engine-layer Cedar enforcement on beta
```

Top-level `policy.file` is single-graph / CLI-local policy only. Multi-graph
server startup rejects it because applying one graph policy to every configured
graph is ambiguous. Move per-graph rules to `graphs.<graph_id>.policy.file` and
move `graph_list` rules to `server.policy.file`.

Each graph's HTTP request flows through its own per-graph policy. The management endpoint (`GET /graphs`) flows through the server-level policy. When `server.policy.file` is unset, `GET /graphs` is denied in every runtime state, including `--unauthenticated`; with bearer tokens configured, it returns 403 after admission control because `graph_list` is not a `read`-equivalent action. The operator must explicitly authorize via `server-policy.yaml` to expose `/graphs`.

Example server-level policy:

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

## Configuration

`omnigraph.yaml`:

```yaml
policy:
  file: ./policy.yaml          # Cedar rules + groups
  tests: ./policy.tests.yaml   # declarative test cases

cli:
  actor: act-andrew            # default actor for CLI direct-engine writes
```

Each per-graph rule must use exactly one of `branch_scope` or `target_branch_scope`. Server-scoped rules (`graph_list`) take neither — they have no branch context.

`cli.actor` is the default actor identity for CLI direct-engine writes
when `policy.file` is configured. Override per-invocation with `--as
<ACTOR>` (top-level flag) — `--as` wins, otherwise `cli.actor` is used,
otherwise no actor. With policy configured and neither set, the
engine-layer footgun guard intentionally denies the write (silent bypass
via "I forgot the actor" is exactly what the guard prevents). Remote
HTTP writes ignore both — they resolve their actor server-side from the
bearer token.

## CLI

- `omnigraph policy validate` — parse + count actors, exit 1 on parse error.
- `omnigraph policy test` — run cases in `policy.tests.yaml`, exit 1 on any expectation mismatch.
- `omnigraph policy explain --actor … --action … [--branch …] [--target-branch …]` — show decision and matched rule.
- `omnigraph --as <ACTOR> <subcommand>` — set the actor for the duration of one invocation. Effective for `change`, `load`, `ingest`, `branch create|delete|merge`, and `schema apply` against local URIs. No-op against remote HTTP URIs (actor is bearer-token-resolved server-side).

## Enforcement

Policy is a property of the **engine**, not the transport. Every mutating
write — `mutate_as`, `load_as`, `ingest_as`, `apply_schema_as`,
`branch_create_as`, `branch_create_from_as`, `branch_delete_as`,
`branch_merge_as` — calls `Omnigraph::enforce(action, scope, actor)` at
the head of the method. The gate fires identically whether the call
originates from the HTTP server, the CLI, or an embedded SDK consumer.
When no `PolicyChecker` is installed (the dev/embedded default) the gate
is a strict no-op; when one is installed and the call site forgets to
thread an actor through, the gate fails closed rather than silently
bypassing.

## Server runtime states (MR-723)

The HTTP server classifies its startup configuration into one of three
states based on whether bearer tokens are configured and whether a
policy file is set. The state determines what happens to a request that
reaches `authorize_request()` without a matching policy permit.

| State | Tokens | Policy file | Behavior |
|---|---|---|---|
| **Open** | no | no | Every request is permitted. Refuses to start unless `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1` is set — the operator must explicitly opt in. |
| **DefaultDeny** | yes | no | Every authenticated request for an action other than `read` is rejected with HTTP 403. Closes the "tokens but forgot the policy file" trap — an operator who sets up auth and forgot to point at a policy file used to ship the illusion of protection. |
| **PolicyEnabled** | yes | yes | Every request is evaluated by Cedar against the configured policy. |

The classifier is `classify_server_runtime_state` in
`crates/omnigraph-server/src/lib.rs`; it returns `Err` for the "no
tokens, no policy, no flag" cell and for "policy file, no tokens" so the
server refuses to start instead of silently shipping an open instance or
a policy-protected server that can only 401. Tests pin every cell of the
matrix and the State-2 deny path.

Server-side, `authorize_request()` still runs at the HTTP boundary —
that's where actor identity is resolved from the bearer token and where
admission control / per-actor rate limits live. Engine-layer enforcement
is the **defense in depth** layer: it catches CLI direct-engine writes,
embedded SDK consumers, and any future transport that hasn't (or won't)
re-implement HTTP's authorize_request. Both layers consult the same
Cedar policy via the same `PolicyChecker` trait, so decisions cannot
disagree.

## Coarse vs. fine enforcement

There are two enforcement points, each with non-overlapping
responsibilities:

| Layer | Question it answers | Where it fires |
|---|---|---|
| **Engine-layer (coarse)** | Can this actor invoke this action against this branch / branch-transition? | `Omnigraph::enforce(action, scope, actor)` at the head of every `_as` writer; one Cedar decision per call. |
| **Query-layer (fine)** | For the rows / types this action actually touches, which can the actor see or modify? | Per-row predicates pushed into DataFusion at plan time. **Not yet implemented — see MR-725.** |

The engine-layer gate keeps `ResourceScope` deliberately at branch
granularity (`Graph`, `Branch`, `TargetBranch`, `BranchTransition`).
Per-type and per-row authority is the query-layer's job; conflating them
in `ResourceScope` would create two places per-type policy could be
evaluated and a drift surface between them.

## Actor identity (signed-claim-only)

The actor identity used for every policy decision comes from the matched bearer token — never from a client-supplied request header, query parameter, or body field. The server resolves the token at the auth middleware boundary, looks up the actor it was minted for, and overwrites whatever the handler may have placed in the policy request. Clients cannot set `actor_id` directly.

This is intentional. Trusting client-supplied identity for authorization is "asking the attacker if they're an admin" — Supabase's RLS history names the same footgun. The chokepoint lives in `authorize_request` in `crates/omnigraph-server/src/lib.rs` and is named in `docs/dev/invariants.md` Hard Invariant 11. A regression test asserts the contract: a request with `Authorization: Bearer <token-for-actor-A>` plus `X-Actor-Id: actor-B` always evaluates as actor A, never as actor B.

If you find yourself wanting to let clients override `actor_id` for impersonation, delegation, or service-account flows — that's a feature, but it needs explicit design (e.g., signed delegation claims, an `On-Behalf-Of` audit trail). It is not a convenience knob.
