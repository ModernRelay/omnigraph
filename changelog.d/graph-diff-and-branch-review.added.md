- **Compare two points, review a branch, and preview a merge.**
  `omnigraph diff --from <side> --to <side>` (`GET /graphs/{id}/diff`) reports
  the net entity changes between two branches or commits, in the
  `commit changes` change shape and without a cause. `branch diff <branch>`
  (`GET /branches/diff`) reports what a branch changed since its merge base
  with a target branch, and `branch merge-base` (`GET /branches/merge-base`)
  prints that base. `branch merge --dry-run` (`dry_run: true` on
  `POST /branches/merge`) reports the outcome or the conflicts a merge would
  have and writes nothing; it needs `read` on both branches, not
  `branch_merge`. See [changes, diffs and change feeds][graph-diff-changes]
  and [previewing a merge][graph-diff-merge-preview].

[graph-diff-changes]: ../docs/user/branching/changes.md
[graph-diff-merge-preview]: ../docs/user/branching/merge.md#preview-a-merge
