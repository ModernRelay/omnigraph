- A dropped property's values are no longer freed by `cleanup` alone. Dropping
  a property no longer rewrites its table, so the values stay in the table's
  current data files beside the remaining properties; they are removed when
  `optimize` rewrites the fragments that hold them, which it does for small
  fragments and fragments with many deleted rows. Dropped types are reclaimed
  by `cleanup` as before. See [dropping declarations][dropped-property-storage-drops].

[dropped-property-storage-drops]: ../docs/user/schema/index.md#schema-changes
