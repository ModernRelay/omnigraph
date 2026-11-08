- `omnigraph index build --branch <branch>` builds the indexes a branch's
  tables lack and rebuilds lagging BTREE/vector ones; `optimize` still does
  this on `main` with compaction. See [Build indexes][index-build-guide].

[index-build-guide]: ../docs/user/operations/maintenance.md#build-indexes
