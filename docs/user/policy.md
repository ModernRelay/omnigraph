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
8. `run_publish`
9. `run_abort`
10. `admin` — reserved

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

## Server enforcement

Every mutating endpoint calls `authorize_request()` *before* the handler runs; decisions are logged with actor / action / branch / outcome / matched rule.

## Actor identity (signed-claim-only)

The actor identity used for every policy decision comes from the matched bearer token — never from a client-supplied request header, query parameter, or body field. The server resolves the token at the auth middleware boundary, looks up the actor it was minted for, and overwrites whatever the handler may have placed in the policy request. Clients cannot set `actor_id` directly.

This is intentional. Trusting client-supplied identity for authorization is "asking the attacker if they're an admin" — Supabase's RLS history names the same footgun. The chokepoint lives in `authorize_request` in `crates/omnigraph-server/src/lib.rs` and is named in `docs/dev/invariants.md` Hard Invariant 11. A regression test asserts the contract: a request with `Authorization: Bearer <token-for-actor-A>` plus `X-Actor-Id: actor-B` always evaluates as actor A, never as actor B.

If you find yourself wanting to let clients override `actor_id` for impersonation, delegation, or service-account flows — that's a feature, but it needs explicit design (e.g., signed delegation claims, an `On-Behalf-Of` audit trail). It is not a convenience knob.
