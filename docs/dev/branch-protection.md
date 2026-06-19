# Branch protection on `main`

`main` is gated by a declarative branch-protection policy. The source of truth is `.github/branch-protection.json`; the apply mechanism is `scripts/apply-branch-protection.sh`. Re-running the script with a changed JSON is idempotent.

This page explains what the policy says and how to change it.

## Current policy

| Setting | Value | Why |
|---|---|---|
| **Required status checks (strict)** | `Classify Changes`, `Check AGENTS.md Links`, `Test omnigraph-server --features aws` | Every PR must pass the AWS-feature build/test and AGENTS.md link integrity. **`Test Workspace` is deliberately NOT required** — it runs only on push to `main` (post-merge), tags, and manual `workflow_dispatch`, to keep PR turnaround fast (it was the ~15min+ slow gate). It is therefore *not* listed here: a required check that never reports on PRs (the `test` job is `if: github.event_name != 'pull_request'`) would leave every PR permanently pending — the job-never-reports trap. The trade-off (a regression lands on `main` and is caught by the post-merge run, so `main` can briefly go red) and its mitigations are documented in [ci.md](ci.md). Each required context must equal a job `name:` that actually reports on PRs **verbatim** — a context naming a job that never reports leaves every PR permanently pending and forces admin overrides. `strict: true` requires the branch to be up-to-date with `main` before merge. |
| **Required approving reviews** | `0` | No human-review gate. With a 2-person team where both maintainers own everything, requiring an approval meant every PR needed the *other* person (or an admin/bypass override) — friction with no real review value. CI checks are the gate; maintainers merge their own PRs once checks pass. Raise this to `1` if an outside-contributor flow ever needs a review gate. |
| **Require code-owner reviews** | `false` | CODEOWNERS was removed entirely (see the git history of `.github/`); there is no code-owner review requirement. |
| **Require linear history** | `true` | No merge commits — squash or rebase only. Matches recent practice. |
| **Disallow force pushes** | `true` | No history rewrites on `main`. |
| **Disallow branch deletions** | `true` | `main` cannot be deleted. |
| **Required conversation resolution** | `true` | All review comment threads must be resolved before merge. |
| **Enforce on admins** | `false` | Admins can override the gates (`enforce_admins: false` in the JSON). This is the intended escape hatch for the 2-person team; tightening to `true` is tracked under hardening below. |
| **Required signed commits** | not yet | Not enabled. Would lock out maintainers until everyone enrolls GPG/SSH commit signing. Tracked as a follow-up. |

## How to apply

Run from the repository root:

```bash
./scripts/apply-branch-protection.sh
```

The script reads `.github/branch-protection.json`, strips the human-readable `_comment` field (the GitHub API rejects unknown keys), and PUTs to `repos/ModernRelay/omnigraph/branches/main/protection`.

Requires `gh` authenticated with a token that has admin permissions on the repository.

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
- **Consistency**: repository policy lives in the repository, reviewed like code.

## What this gates

After branch protection is applied, every PR targeting `main` must:

1. Pass all listed status checks.
2. Be up-to-date with `main` (rebase or merge-from-main).
3. Have all review conversations resolved.
4. Be squash- or rebase-merged (no merge commits).

No human approval is required (`required_approving_review_count: 0`). Repository
admins can override the gates (`enforce_admins: false`).

## Subsequent hardening (not in this PR)

The branch-protection policy is the foundation. Future hardening adds:

- **Required signed commits** (`required_signatures: true`) — once maintainers enroll GPG/SSH signing.
- **Tag protection** for `v*` tags via `repos/.../tags/protection`.
- **Required reviewers from specific teams** for high-leverage paths (e.g., `docs/dev/invariants.md`) via a GitHub ruleset's path-scoped required-review rule, if a review gate is ever reintroduced.
- **More required CI checks**: `cargo deny`, `cargo audit`, `cargo fmt --check`, `cargo clippy -D warnings`, CodeQL, secret scanning, schema-lint (MR-946).

See the hardening playbook for the full plan.
