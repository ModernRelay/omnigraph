- 0.17 reads and writes existing format-17 graphs without a rebuild. New graphs use format 18,
  which 0.16 cannot open. Upgrade all readers and writers before creating a graph or
  [upgrading a standalone graph][format-18-upgrade] to 18; the upgrade records every branch
  in the graph's catalog and unlinks the branch ref files.

[format-18-upgrade]: ../docs/user/operations/upgrade.md#storage-upgrade
