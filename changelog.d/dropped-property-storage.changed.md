- Dropping a property no longer rewrites its table. Through v0.13.0 schema
  apply rewrote the table without the dropped values, so `omnigraph cleanup` alone
  erased them; now they stay in the table's data files until
  `omnigraph optimize` rewrites every fragment that holds them, whatever its
  size. To erase them, run optimize, delete every branch created from a commit
  before it and every tag naming such a commit (merging a branch does not
  release its files), then run `cleanup` with a retention that excludes the
  commits before the optimize, for example `--keep 1`. Dropped types are still
  reclaimed by `cleanup` alone. See
  [dropping declarations][dropped-property-storage-drops].

[dropped-property-storage-drops]: ../docs/user/schema/index.md#schema-changes
