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
- `Test Workspace`
- `GQ Logic Tests`
- `Fix Regression Gate`
- `Storage Upgrade Compatibility`
- `Dependency Guard (cargo deny)`

Checks are strict; under the merge queue the entry is built on the current
`main`, and GitHub does not require the author to update the pull request
branch first. `Graph Vocabulary
Guard` remains as an always-reporting context but reports a successful skip on
pull requests; the full OpenAPI, Rust presentation-string, and public-Rust
audit runs after merge, on tags, and by manual dispatch. User documentation is
intentionally outside this exact-occurrence audit and is validated by the
documentation structure check.
PRs outside the engine-input class (documentation, `.gqt` cases, deployment
files; see the classes in [ci.md](ci.md)) still receive every required
context; work-heavy steps may report as skipped. `Storage Upgrade Compatibility` is an explicit exception:
both genuine predecessor migration journeys execute on every change, whatever
its class. Its fixture availability and required-context
contract are checked by `scripts/check-storage-upgrade-ci.py`. The repository
policy change must still be applied by an administrator to affect GitHub.
`Dependency Guard (cargo deny)` is required on every change: it builds nothing
and holds `Cargo.lock` to `deny.toml`. Its RustSec database is fetched at run
time, so an advisory published overnight reds every open pull request until a
lockfile bump or a `deny.toml` exemption lands on `main`; with strict checks
that one pull request unblocks the rest. `dependency-guard-nightly.yml` runs
the same check on `main` daily, so the red shows there first.

`Test Workspace` is required on pull requests and on the merge queue's branch.
Before the queue it was reporting-only because of strict checks: every merge
invalidated every other open pull request's required contexts, so a required
60-minute context would have spaced merges an hour apart. Under the queue the
entry waits for its slowest required context anyway (`Test omnigraph-server
--features aws` and `Storage Upgrade Compatibility` share the suite's
60-minute ceiling; ceilings bound the wait, they are not durations), so
requiring the suite adds runner minutes and, per entry, only the time by
which it outruns the other required contexts; in return a red suite can no
longer land. `V5 ↔ V9
Format Fence` and both `RustFS S3 Integration` shards stay reporting contexts:
the queue merges on the required contexts alone, so the merger clicks **Merge
when ready** only after they have reported on the pull request and none is
red. All of them run again on every push to `main` that changes engine input,
on tags, and by dispatch, where a red run makes `main` stop-the-line until
fixed or reverted. See [ci.md](ci.md).

The remaining policy is:

- zero required approvals and no code-owner review gate;
- all review conversations resolved;
- linear history only;
- force pushes and deletion of `main` disabled;
- no administrator bypass (`enforce_admins: true`): the rule binds
  administrators too, and the pull request page offers no bypass checkbox.

The JSON is authoritative for every exact setting it carries; the merge queue
and its parameters below are set on the rule page and not carried by the JSON.

## Merge queue

Every merge into `main` goes through the merge queue with the squash method.
The queue takes a pull request once its required contexts are green, builds a
temporary `gh-readonly-queue/main/…` branch from the current `main` plus the
entries ahead of it, fires the `merge_group` event there, and merges only when
every required context has reported green on that branch. A required context
that never reports on the merge-group branch holds the entry until the queue's
status-check timeout removes it, so `ci.yml`, `gq-logic-tests.yml`, and
`fix-regression-gate.yml` list `merge_group` under `on:`, the `Format
(rustfmt)` and `Lint (clippy)` job conditions accept the event beside
`pull_request`, and `scripts/check-merge-group-triggers.py`, a step of `Check
Workflow Action Pins`, fails the build when a workflow owning a required
context loses the trigger or a job condition admits `pull_request` without
`merge_group`. The classifier diffs the merge group's own `base_sha` and
`head_sha` from the event payload; GitHub does not document whether that base
is `main`'s tip or the entry ahead, so an entry may run the work of the
entries ahead as well as its own, never less. A
context required only on the rule page and absent from the JSON is invisible
to the validator; if its workflow lacks the trigger, every entry stalls until
the timeout.

The reporting contexts that are not required (`V5 ↔ V9 Format Fence`, the
`RustFS S3 Integration` shards, `Azurite Azure Integration`, `Container
Entrypoint`, `Azure Deployment Validation`, `Azure Contract Guards`) and the
vocabulary audit are gated off the merge-group branch: the queue merges on
the required contexts alone, so a run there would finish after the merge and
only repeat the post-merge run. `DST pinned suite` (`dst.yml`) runs on queue
entries as a reporting context; it does not hold the queue.

`Fix Regression Gate` reports a pass on the merge-group run without a check:
the merge-group commit carries no pull-request body, and the pull request
passed the gate on its own `pull_request_target` run before it could be
queued. The merge-group run executes the queued commit's own copy of the
workflow, so a check placed there could never be a policy gate. One window
stays open: a body or label edit after queueing re-runs the gate on the pull
request only; whether a red result there removes the entry is not documented
by GitHub, and the red context stays visible on the pull request either way.

Landing a pull request: click **Merge when ready** once the reporting
contexts have reported; GitHub queues the pull request when its required
contexts are green. A queued entry whose merge-group checks fail, or that hits
the timeout, is removed and is not re-queued by itself: push the fix, wait for
the pull-request checks, click again (a flaky failure re-queues on the click
alone). An entry ahead that fails makes the queue rebuild the entries behind
it. A queue that was unticked and not re-ticked shows itself on every pull
request page: the button reads Squash and merge instead of Merge when ready.

Rule-page values, not carried by the JSON and set by an administrator to
match this list: merge method Squash and merge; build concurrency 5; minimum
entries to merge 1, or after 5 minutes; maximum entries to merge 5; **Only
merge non-failing pull requests** on (the removal rule above holds only with
it on; off, a red entry lands behind a green one); status-check timeout 90
minutes, above the 60-minute ceilings of the slowest required contexts on a
cold cache (`Test Workspace`, `Test omnigraph-server --features aws`,
`Storage Upgrade Compatibility`).

A group merges as one push of `main` carrying one squash commit per entry, so
the post-merge runs (`Test Workspace`, the fence, the shards, Azurite, the
vocabulary audit, the edge release) cover the whole group at once, and a
revert picks the entry's own squash commit out of that push.

No administrator bypass means two situations need the rule page rather than
a pull request. Renaming or removing a required context: the old name never
reports again, so loosen the live rule first, land the change (the JSON edit
rides in the same pull request as the workflow edit; the validator refuses
the pair split), then re-apply the JSON; other pull requests landing inside
that window are not held to the loosened context. A required context red on
`main` for a cause the fix cannot remove
(a predecessor-release download, a dead action pin, a runner outage): an
administrator unticks that status requirement on the rule page, lands the
fix, and re-applies; unticking the queue alone changes nothing, because the
status requirement binds with or without the queue. Untick the queue only for
a queue-specific failure while the pull request's own checks are green;
without the queue, strict checks require the branch to be current with
`main`, so update it and let its checks re-run before Squash and merge. Both
are settings edits an administrator makes by hand and records in the pull
request that needed them.

The branch protection endpoint that `scripts/apply-branch-protection.sh` drives
has no field for the queue, so it is set on the rule page and checked there
after every apply.

## Code owners

`.github/CODEOWNERS` names the reviewers GitHub requests for each workspace
member: one anchored line per crate under `crates/` and under `tools/`, plus
one for `/deny.toml`, the dependency allowlist. Other paths request nobody. GitHub reads the copy on `main`,
requests the listed people when a pull request is opened ready for review,
marked ready, or gains a matching path in a later push, and never requests the
pull request's own author. Two handles on
one line means both are requested; when patterns overlap, the last matching
line wins. It is advisory only: `require_code_owner_reviews` stays false, so
nobody's approval is required and a green pull request merges as before.
Listed handles need write access to the repository. GitHub reports an invalid
line only on the file view and under Settings, never on a pull request, and
that line then requests nobody. Nothing in CI checks the file, so a crate added
to the workspace needs its line added by hand. Add or move your handle in a
normal pull request.

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

4. Open the rule page and confirm Require merge queue and the rule-page values
   in the Merge queue section are still set; the endpoint above does not show
   them. If any is unset, re-tick it and re-enter the values before the next
   merge.

Keeping policy in the repository provides a reviewable audit trail and a
recovery source if the GitHub setting drifts.
