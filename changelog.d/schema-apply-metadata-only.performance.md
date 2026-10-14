- Adding a nullable property, renaming a property and dropping a property no
  longer rewrite the table. Schema apply changes only the table's metadata, so
  its memory and I/O no longer grow with the rows and Blob values the table
  stores, only with its metadata (fragments, columns and indexes): a schema
  change on a large Blob table, which previously held every managed Blob value
  of the table in memory and could exhaust a server running the deployment,
  now reads no row and no Blob value. Existing indexes keep covering their rows
  instead of waiting for the next `optimize`, and a stored external Blob
  reference to a byte range, which schema apply previously refused, is kept as
  stored. See [schema changes][schema-apply-metadata-only-changes].

[schema-apply-metadata-only-changes]: ../docs/user/schema/index.md#schema-changes
