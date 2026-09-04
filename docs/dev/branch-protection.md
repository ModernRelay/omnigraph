# Branch protection on `main`

`.github/branch-protection.json` is the reviewed policy. An administrator
applies it with `scripts/apply-branch-protection.sh`; CI does not mutate branch
protection.

## Current policy

`main` requires these exact PR status contexts:

- `Classify Changes`
- `Check AGENTS.md Links`
- `Check Workflow Action Pins`
- `Graph Vocabulary Guard`
- `Test omnigraph-server --features aws`
- `Format (rustfmt)`
- `Lint (clippy)`
- `GQ Logic Tests`
- `Fix Regression Gate`

Checks are strict, so a PR must be current with `main`. `Graph Vocabulary
Guard` remains as an always-reporting context but reports a successful skip on
pull requests; the full OpenAPI, Rust presentation-string, and public-Rust
audit runs after merge, on tags, and by manual dispatch. User documentation is
intentionally outside this exact-occurrence audit and is validated by the
documentation structure check.
Documentation-only PRs still receive every required context; work-heavy steps
may report as skipped.

`Test Workspace` runs on pull requests as a reporting context and is
deliberately not required: with strict checks every merge invalidates every
other open pull request's required contexts, so a required 60-minute context
would space merges an hour apart. The merger waits for `Test Workspace` to
report and reads a red result before merging. The same suite runs again on
every push to `main`, on tags, and by dispatch, where a red run makes `main`
stop-the-line until fixed or reverted. See [ci.md](ci.md).

The remaining policy is:

- zero required approvals and no code-owner review;
- all review conversations resolved;
- linear history only;
- force pushes and deletion of `main` disabled;
- administrator bypass retained (`enforce_admins: false`).

The JSON is authoritative for every exact setting.

## Preview and apply

From the repository root:

```bash
DRY_RUN=1 ./scripts/apply-branch-protection.sh
./scripts/apply-branch-protection.sh
```

The script removes the JSON's explanatory `_comment` and updates
`repos/ModernRelay/omnigraph/branches/main/protection`. It requires an
authenticated `gh` token with repository-administration permission.

To change the policy:

1. Update `.github/branch-protection.json` in a PR.
2. After merge, have an administrator run the apply script.
3. Compare the live state with the reviewed file:

```bash
gh api repos/ModernRelay/omnigraph/branches/main/protection
```

Keeping policy in the repository provides a reviewable audit trail and a
recovery source if the GitHub setting drifts.
