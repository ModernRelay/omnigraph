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

Checks are strict, so a PR must be current with `main`. `Graph Vocabulary
Guard` always reports and protects the reviewed vocabulary inventory across
OpenAPI, Rust presentation strings, and public Rust surfaces. User documentation
is intentionally outside this exact-occurrence gate and is validated by the
documentation structure check.
Documentation-only PRs still receive every required context; work-heavy steps
may report as skipped.

`Test Workspace` is deliberately not required because it runs after merge, on
tags, or by manual dispatch rather than on PRs. Making it required would leave
PRs waiting for a context they cannot receive. A post-merge failure makes
`main` stop-the-line until fixed or reverted. See [ci.md](ci.md).

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
