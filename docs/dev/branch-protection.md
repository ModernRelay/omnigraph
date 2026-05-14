# Branch protection on `main`

`main` is gated by a declarative branch-protection policy. The source of truth is `.github/branch-protection.json`; the apply mechanism is `scripts/apply-branch-protection.sh`. Re-running the script with a changed JSON is idempotent.

This page explains what the policy says and how to change it.

## Current policy

| Setting | Value | Why |
|---|---|---|
| **Required status checks (strict)** | `Classify Changes`, `Check AGENTS.md Links`, `Test Workspace`, `Test omnigraph-server --features aws`, `CODEOWNERS / drift`, `CODEOWNERS / noedit` | Every PR must pass workspace tests, AGENTS.md link integrity, and the CODEOWNERS hygiene checks. `strict: true` requires the branch to be up-to-date with `main` before merge. |
| **Required approving reviews** | `1` | At least one reviewer. With a 2-person team, going higher would block all merges when one person is unavailable. |
| **Require code-owner reviews** | `true` | The reviewer must be a code owner per `.github/CODEOWNERS`. This is what makes the codeowners chassis enforced. |
| **Dismiss stale reviews on new commits** | `true` | A push after approval invalidates the prior review. Prevents the "approve, then sneak in unreviewed changes" pattern. |
| **Require linear history** | `true` | No merge commits — squash or rebase only. Matches recent practice. |
| **Disallow force pushes** | `true` | No history rewrites on `main`. |
| **Disallow branch deletions** | `true` | `main` cannot be deleted. |
| **Required conversation resolution** | `true` | All review comment threads must be resolved before merge. |
| **Enforce on admins** | `true` | Even repo admins go through the gates. The point is no bypasses. |
| **Required signed commits** | not yet | Not enabled. Would lock out maintainers until everyone enrolls GPG/SSH commit signing. Tracked as a follow-up. |

## How to apply

Run from the repo root:

```bash
./scripts/apply-branch-protection.sh
```

The script reads `.github/branch-protection.json`, strips the human-readable `_comment` field (the GitHub API rejects unknown keys), and PUTs to `repos/ModernRelay/omnigraph/branches/main/protection`.

Requires `gh` authenticated with a token that has admin permissions on the repo.

To preview without applying:

```bash
DRY_RUN=1 ./scripts/apply-branch-protection.sh
```

## How to change the policy

1. Edit `.github/branch-protection.json`.
2. Open a PR. The JSON change goes through normal review.
3. After the PR merges, an admin runs `./scripts/apply-branch-protection.sh` to push the new policy to GitHub.

The script is **not run automatically** by CI. Branch-protection changes are admin actions that should be applied deliberately — a CI-driven automatic apply would mean any merged PR could rewrite protection rules, which defeats the purpose. The script's existence makes the apply reproducible; the admin's manual invocation is the audit point.

## How to read the current GitHub state

```bash
gh api repos/ModernRelay/omnigraph/branches/main/protection
```

Outputs the live policy. Compare against `.github/branch-protection.json` to detect drift.

## Why declared as code

- **Audit trail**: `git log .github/branch-protection.json` shows every change with a reviewable diff and a merge commit.
- **Disaster recovery**: if branch protection is accidentally removed or weakened via the UI, the JSON is the canonical recovery point.
- **Consistency**: pairs with `.github/codeowners-roles.yml` (the CODEOWNERS source of truth). Repo policy lives in the repo.

## What this gates

After branch protection is applied, every PR targeting `main` must:

1. Pass all listed status checks.
2. Be up-to-date with `main` (rebase or merge-from-main).
3. Have at least one approving review from a code owner for the touched paths.
4. Have all review conversations resolved.
5. Be squash- or rebase-merged (no merge commits).

Even repo admins are subject to these rules.

## Subsequent hardening (not in this PR)

The branch-protection policy is the foundation. Future hardening adds:

- **Required signed commits** (`required_signatures: true`) — once maintainers enroll GPG/SSH signing.
- **Tag protection** for `v*` tags via `repos/.../tags/protection`.
- **Required reviewers from specific teams** for high-leverage paths (e.g., `docs/dev/invariants.md`) via CODEOWNERS tier expansion + the N-unique-approvers CI workaround.
- **More required CI checks**: `cargo deny`, `cargo audit`, `cargo fmt --check`, `cargo clippy -D warnings`, CodeQL, secret scanning, schema-lint (MR-946).

See the hardening playbook for the full plan.
