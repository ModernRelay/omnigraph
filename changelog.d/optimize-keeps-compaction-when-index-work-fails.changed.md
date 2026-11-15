- `optimize` publishes a table's compaction when its index work then fails, and
  reports the table `optimized` with the reason `index_failed`; the next run
  retries the index work. See [bounding a run][optimize-index-failed].

[optimize-index-failed]: ../docs/user/operations/maintenance.md#bounding-a-run
