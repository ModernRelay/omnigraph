- The embedded Rust `Omnigraph::diff_between` and `Omnigraph::diff_commits`
  and their table-shaped `ChangeSet`, `EntityChange`, `ChangeStats`,
  `ChangeFilter` and `ChangeOp` types are removed. Use `Omnigraph::diff_page`,
  which compares any two branches or commits page by page in the
  `commit changes` change shape with a `ChangeFeedScope` filter. Its updates
  compare row images, so a row rewritten with equal values is no longer
  reported, and a pair across a dropped and re-added type refuses with
  `ChangeSchemaBoundary` instead of reporting deletes and inserts. See
  [changes, diffs and change feeds][table-shaped-diff-removed-guide].

[table-shaped-diff-removed-guide]: ../docs/user/branching/changes.md#compare-two-points
