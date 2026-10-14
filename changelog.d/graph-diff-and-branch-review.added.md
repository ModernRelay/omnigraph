- **Compare two points, review a branch, and preview a merge.** Each is a GQ
  branch statement at `POST /query` and `omnigraph query`, with a CLI command
  and HTTP route that wrap the same engine call:
  - `branch diff <from> to <to>` (`omnigraph diff --from --to`, `GET /diff`)
    reports the net entity changes between two branches or commits, in the
    `commit changes` change shape and without a cause;
  - `branch diff <branch> [into <target>]` (`omnigraph branch diff`,
    `GET /branches/diff`) reports what a branch changed since its merge base;
  - `branch merge-base <branch> [into <target>]` (`omnigraph branch
    merge-base`, `GET /branches/merge-base`) names that base;
  - `branch merge <branch> [into <target>] dry run` (`omnigraph branch merge
    --dry-run`, `dry_run: true` on `POST /branches/merge`) reports the outcome
    or the conflicts a merge would have and writes nothing; it needs `read` on
    both branches, not `branch_merge`.

  A `branch diff` statement answers one row per change and carries the
  compared commits and its continuation token in a new optional `page` object
  on `ReadOutput`; `after page "<token>"` continues it. See
  [changes, diffs and change feeds][graph-diff-changes] and
  [previewing a merge][graph-diff-merge-preview].

[graph-diff-changes]: ../docs/user/branching/changes.md
[graph-diff-merge-preview]: ../docs/user/branching/merge.md#preview-a-merge
