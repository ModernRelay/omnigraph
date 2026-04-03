# Dev Workflow Policy

1. **Triage Gate** — all agent-created issues enter through Triage; only humans close, merge, backlog, or commit to active work.
2. **No Slop** — humans keep issues clean; no stale specs, irrelevant comments, or dead references.
3. **Reconcile** — after milestones, sync issues with codebase changes, decisions, and priorities.
4. **Refresh** — when dependencies or ecosystem context changes, update affected issues.
5. **Link Everything** — issue ID in branch name, commit message, and PR title; one primary issue per PR.
6. **Small PRs** — one PR solves one issue-sized problem; split if too broad.
7. **Standardize PRs** — every PR uses the template: linked issue, summary, risk, tests, rollback.
8. **Protect Main** — never commit directly to main; all changes go through branches + PRs.
9. **CODEOWNERS** — merges to main require approval from designated owners.

---

## Issue Lifecycle

### Triage Gate

All agent-created issues must enter through Triage. Only humans decide whether an issue is closed, merged, backlogged, or committed to active work.

Agents can create and enrich issues, but only humans can prioritize.

### No Slop

Humans are responsible for keeping slop out of issues. Issues must be clean and actionable.

What counts as slop:

- Irrelevant comments or status-update noise
- Large spec files pasted inline (link to `docs/dev/` instead)
- Stale implementation details that reference code that no longer exists
- Granular sub-task breakdowns that belong in a plan, not in Linear
- Descriptions written for a different codebase (e.g. old Nanograph references)

If an issue accumulates slop, clean it or cancel it. Do not let issues rot.

### Reconcile

Issues must be synced with recent codebase changes and decisions.

After significant implementation work, review affected issues:

- Cancel issues that were completed as part of other work
- Update descriptions that reference old code paths or APIs
- Adjust priorities based on what shipped and what shifted
- Verify that "In Progress" issues have actual in-progress code
- Verify that "Done" issues are actually done — not partially done with the hard part deferred

Do this after each milestone, not continuously.

### Refresh

Issues must be synced with real-world changes outside the codebase.

When dependencies, tools, or ecosystem context changes, update affected issues:

- New library versions that change the implementation approach (e.g. Lance v4 namespaces replacing our custom manifest)
- New APIs or features in dependencies that make an issue easier or unnecessary
- Better practices discovered through benchmarks, production experience, or competitor analysis
- Platform changes (AWS, S3, Lance) that affect feasibility or priority

Add a comment with the relevant context. Do not let issues propose solutions against stale assumptions.

---

## Linear + GitHub Integration

### Put the Linear issue ID everywhere

Use the issue key in branch names, commit messages, and PR titles.

```
Branch:  feat/EQU-123-short-name
Commit:  EQU-123: add retry for webhook sync
PR title: EQU-123 Add retry for webhook sync
```

Linear links PRs to issues using branch names, PR titles, or magic words. It links commits through commit messages. Use the key consistently so all links are machine-resolvable in both directions.

### Start branches from the issue

The flow is:

1. Pick the issue in Linear
2. Create or name the branch from that issue key
3. Open the PR with the same key in the title

This keeps the issue, branch, commits, and PR all linked.

### One primary issue per PR

Do not let one PR loosely cover five issues.

- Choose one primary issue for the PR
- Mention secondary issues in the PR body
- Split work if the PR is too broad

This keeps status automation sane and preserves a clean audit trail.

### Let Linear automate status from PR state

With the GitHub integration connected, Linear moves issues automatically as PRs progress:

- Branch created → In Progress
- PR opened → In Progress
- PR merged → Done

Do not manually update issue status for work that has a linked PR. Let the automation handle it.

### Keep PRs small

One PR should solve one issue-sized problem. Small PRs are easier to review, easier to revert, and less likely to hide agent mistakes.

If a PR touches multiple concerns, split it. If you can't describe the change in one sentence, it's probably too big.

GitHub supports creating branches from issues, which helps keep work scoped from the start.

### Standardize PR descriptions

Every PR must use the repository PR template. The template must include:

- Linked issue (e.g. `Closes EQU-123`)
- Summary of change
- Risk assessment
- Test evidence
- Rollout / rollback notes

The template lives at `.github/pull_request_template.md`. Do not merge PRs with empty or boilerplate descriptions.

### Protect the default branch

Never commit directly to `main`. All changes go through feature branches and pull requests.

Enforce via GitHub branch protection rules on `main`:

- Require a pull request before merging
- Require status checks to pass (CI)
- Do not allow force pushes
- Do not allow deletions

This applies to humans and agents equally. No exceptions.

### CODEOWNERS

Merges to `main` require approval from designated code owners.

`CODEOWNERS` defines who is responsible for reviewing and approving changes to each part of the codebase. No PR merges to `main` without an approval from the relevant owner.

This is enforced via GitHub branch protection rules on `main`:

- Require pull request reviews before merging
- Require review from code owners
- Do not allow bypassing these requirements

The `CODEOWNERS` file lives at `.github/CODEOWNERS` or the repo root.

---

*Source of truth: this file (`docs/team/dev-policy.md`). Mirrored to Linear as [Dev Workflow Policy](https://linear.app/equator/document/dev-workflow-policy-78b76be6873e).*
