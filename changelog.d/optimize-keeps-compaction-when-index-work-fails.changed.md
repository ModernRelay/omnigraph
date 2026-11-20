- If an index step or coverage check fails after compaction, [`optimize`][optimize-index-failed]
  keeps the rewrite and reports `optimized` with reason `index_failed` and the error. Direct reports keep known pending indexes.

[optimize-index-failed]: ../docs/user/operations/maintenance.md#bounding-a-run
