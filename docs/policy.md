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
