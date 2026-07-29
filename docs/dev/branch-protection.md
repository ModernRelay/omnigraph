# Branch protection on `main`

`main` is gated by a declarative branch-protection policy. The source of truth is `.github/branch-protection.json`; the apply mechanism is `scripts/apply-branch-protection.sh`. Re-running the script with a changed JSON is idempotent.

This page explains what the policy says and how to change it.

## Current policy

| Setting | Value | Why |
|---|---|---|
| **Required status checks (strict)** | `Classify Changes`, `Check AGENTS.md Links`, `Test omnigraph-server --features aws` | Every PR must pass the AWS-feature build/test and AGENTS.md link integrity. **`Test Workspace` is deliberately NOT required** — it runs only on push to `main` (post-merge), tags, and manual `workflow_dispatch`, to keep PR turnaround fast (it was the ~15min+ slow gate). It is therefore *not* listed here: a required check that never reports on PRs (the `test` job is `if: github.event_name != 'pull_request'`) would leave every PR permanently pending — the job-never-reports trap. The new `Firehose PR smoke` and `Firehose dependency rebuild` jobs do report on every PR, but remain shadow checks until their exact attested-artifact path satisfies the activation evidence below. The trade-off (a regression lands on `main` and is caught by the post-merge run, so `main` can briefly go red) and its mitigations are documented in [ci.md](ci.md). Each required context must equal a job `name:` that actually reports on PRs **verbatim** — a context naming a job that never reports leaves every PR permanently pending and forces admin overrides. `strict: true` requires the branch to be up-to-date with `main` before merge. |
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

## Firehose activation gate

Do not add the two shadow firehose contexts to
`.github/branch-protection.json` merely because the jobs exist. First satisfy
the evidence gate in [ci.md](ci.md): exercise the exact protected-main
publish, attestation, lookup, empty-runner restore, warm smoke, and cold
exception; collect at least 20 unchanged-key artifact-hit samples; and keep p95
end-to-end completion through `Firehose PR smoke` and the dependent evidence
reporter at or below 15 minutes. Close the retention-scoped write-once gap with
a non-expiring immutable key→digest binding before activation. Changed or
unavailable keys must complete the single cold compile plus smoke within the
separate 60-minute ceiling. A failure requires a smaller isolated harness, not
larger timeouts.

Once that evidence is recorded in the closing CI PR:

1. Add `Firehose PR smoke` and `Firehose dependency rebuild` verbatim to
   `required_status_checks.contexts`.
2. Run `DRY_RUN=1 ./scripts/apply-branch-protection.sh` and inspect the
   rendered payload.
3. Merge the declaration, then have an administrator run
   `./scripts/apply-branch-protection.sh`.
4. Read the live protection API and confirm both contexts are present before
   treating the F2 required-CI prerequisite as closed.

This staging is deliberate: current Rust-cache timing is useful sizing
evidence, but it is not proof of the immutable-artifact path and therefore
cannot justify activating a required context.

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
