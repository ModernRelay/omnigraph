# Code ownership

`.github/CODEOWNERS` is **generated** — not hand-edited. The source of truth is `.github/codeowners-roles.yml`, expanded by `.github/scripts/render-codeowners.py`. CI rejects drift between the two and rejects direct edits to `CODEOWNERS` that don't accompany a yml change.

This setup gives every role change a reviewable PR and a permanent in-repository audit trail (`git log .github/codeowners-roles.yml`).

## Current roles

| Role | Members | Scope |
|---|---|---|
| `engineering` | `@ragnorc` | All code under `crates/**`, repository infrastructure, default for unmapped paths |
| `docs` | `@ragnorc` | `docs/**`, README.md, AGENTS.md, CLAUDE.md, SECURITY.md |

GitHub treats multiple owners in a CODEOWNERS line as **"any one of them satisfies the review requirement"**. To require N distinct approvers on a specific path, layer a CI check on top (not currently configured).

## How to change role membership or path mappings

1. Edit `.github/codeowners-roles.yml`.
2. Run `python3 .github/scripts/render-codeowners.py` (requires PyYAML; `pip install pyyaml`).
3. Commit both files in the same PR.

CI fails the PR if:
- `CODEOWNERS` was edited without a corresponding yml change, or
- The yml was changed but the rendered `CODEOWNERS` doesn't match.

## How to add a new role

1. Add a new entry to `roles:` in the yml with a `description` and `members` list.
2. Reference the role from `paths:` (or `default:`).
3. Regenerate + commit as above.

## Why a generator, not direct CODEOWNERS edits?

- **Audit trail**: `git log .github/codeowners-roles.yml` is the canonical record of every role change. The rendered `CODEOWNERS` is a derived artifact.
- **Roles are first-class**: paths reference roles, not raw handles. Renaming a person or rotating a role updates one place, not every path.
- **Future extension**: scheduled rotation (weekly on-call, quarterly leads) plugs into the same yml without changing the path mappings. Not enabled today.
- **Consistency with the product**: omnigraph itself enforces auditable Cedar policy. The repository's code-owner policy follows the same "policy as reviewed code" pattern.
