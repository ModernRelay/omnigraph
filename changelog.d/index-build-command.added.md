- `omnigraph index build --branch <branch>` brings one branch's declared
  indexes up to its rows: it builds every declared index a table lacks, such as
  those of a type whose first rows were loaded on the branch, and rebuilds BTREE
  and vector indexes whose coverage lags, in one graph commit on that branch.
  `optimize` still does this on `main` together with compaction. Embedded hosts
  call `Omnigraph::ensure_indices_on_as`, which requires `Change` on the branch
  when a policy is installed. See [Build indexes][index-build-guide].

[index-build-guide]: ../docs/user/operations/maintenance.md#build-indexes
