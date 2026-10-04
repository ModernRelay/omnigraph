- Dropping a property removes it from the schema without rewriting its table,
  and maintenance erases its values: every `omnigraph optimize` rewrites the
  table fragments that still store a dropped property's values, whatever
  their size, and the next `omnigraph cleanup` that no longer retains the
  commits before that optimize deletes the old files. Dropped types are
  reclaimed by `cleanup` alone, as before. See
  [dropping declarations][dropped-property-storage-drops].

[dropped-property-storage-drops]: ../docs/user/schema/index.md#schema-changes
