# Authorization (Cedar policy)

OmniGraph integrates AWS Cedar (`cedar-policy = 4.9`) for ABAC.

## Policy actions

1. `read` — query / snapshot / list branches & commits
2. `export` — NDJSON export
3. `change` — mutations
4. `schema_apply` — apply schema migrations
5. `branch_create`
6. `branch_delete`
7. `branch_merge`
8. `admin` — reserved for policy-management surfaces (hot reload, audit log, approvals). No call site today; see MR-724 for the reservation rationale.

## Scope kinds

- `branch_scope` — applied to source branch (`read`, `export`, `change`)
- `target_branch_scope` — applied to destination (`schema_apply`, branch ops, run ops)
- `protected_branches` — named list with special rules; rule scopes are `any | protected | unprotected`

## Configuration

`omnigraph.yaml`:

```yaml
policy:
  file: ./policy.yaml          # Cedar rules + groups
  tests: ./policy.tests.yaml   # declarative test cases
```

Each rule must use exactly one of `branch_scope` or `target_branch_scope`.

## CLI

- `omnigraph policy validate` — parse + count actors, exit 1 on parse error.
- `omnigraph policy test` — run cases in `policy.tests.yaml`, exit 1 on any expectation mismatch.
- `omnigraph policy explain --actor … --action … [--branch …] [--target-branch …]` — show decision and matched rule.

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
