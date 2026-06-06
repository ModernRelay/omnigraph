# Code ownership

`.github/CODEOWNERS` is **generated** — not hand-edited. The source of truth is `.github/codeowners-roles.yml`, expanded by `.github/scripts/render-codeowners.py`. CI rejects drift between the two and rejects direct edits to `CODEOWNERS` that don't accompany a yml change.

This setup gives every role change a reviewable PR and a permanent in-repository audit trail (`git log .github/codeowners-roles.yml`).

## Who owns what

The tables below are **generated** from `.github/codeowners-roles.yml` by `.github/scripts/render-codeowners.py` (the same render that produces `.github/CODEOWNERS`). They are the always-current "who owns what at this commit" view — don't edit them by hand; edit the yml and re-render.

<!-- BEGIN GENERATED OWNERSHIP — edit codeowners-roles.yml + run render-codeowners.py -->

**Path → owners** (GitHub applies *last match wins*; the `*` catch-all is listed first and is overridden by the specific patterns below it):

| Path | Owners | Role(s) |
|---|---|---|
| `*` | @ragnorc | engineering |
| `crates/**` | @ragnorc | engineering |
| `docs/**` | @ragnorc | docs |
| `README.md` | @ragnorc | docs |
| `AGENTS.md` | @ragnorc | docs |
| `CLAUDE.md` | @ragnorc | docs |
| `SECURITY.md` | @ragnorc | docs |

**Roles**:

| Role | Members | Description |
|---|---|---|
| `engineering` | @ragnorc | All production code under crates/**. Engine, CLI, server, compiler. |
| `docs` | @ragnorc | Documentation under docs/**, plus repo-level docs (README.md, AGENTS.md, CLAUDE.md symlink, SECURITY.md). |

<!-- END GENERATED OWNERSHIP -->

GitHub treats multiple owners on a CODEOWNERS line as **"any one of them satisfies the review requirement"**. To require N distinct approvers on a specific path, layer a CI check on top (not currently configured).

## How to change role membership or path mappings

1. Edit `.github/codeowners-roles.yml`.
2. Open a PR. **CI re-renders for you**: the `CODEOWNERS` workflow regenerates `.github/CODEOWNERS` and the ownership tables above and auto-commits them back to your PR branch on same-repository PRs — you don't have to run the script locally (though you can: `python3 .github/scripts/render-codeowners.py`, requires PyYAML).

On a fork (where CI can't push back), the workflow instead fails with the diff so you can run the script and commit it yourself.

CI fails the PR if:
- a fork PR left a generated artifact out of sync, or
- `CODEOWNERS` was edited without a corresponding yml change (the `CODEOWNERS not hand-edited` check).

## How to add a new role

1. Add a new entry to `roles:` in the yml with a `description` and `members` list.
2. Reference the role from `paths:` (or `default:`).
3. Regenerate + commit as above.

## Why a generator, not direct CODEOWNERS edits?

- **Audit trail**: `git log .github/codeowners-roles.yml` is the canonical record of every role change. The rendered `CODEOWNERS` is a derived artifact.
- **Roles are first-class**: paths reference roles, not raw handles. Renaming a person or rotating a role updates one place, not every path.
- **Future extension**: scheduled rotation (weekly on-call, quarterly leads) plugs into the same yml without changing the path mappings. Not enabled today.
- **Consistency with the product**: omnigraph itself enforces auditable Cedar policy. The repository's code-owner policy follows the same "policy as reviewed code" pattern.
