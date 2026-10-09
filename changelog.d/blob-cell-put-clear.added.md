- An embedded `Session` can replace or clear one Blob value of an existing
  node or edge by exact id with `put_blob_at_as` and `clear_blob_at_as`. Each
  write is one graph commit under the `change` action, never reads the old
  value, and carries the row's other cells as an `update` does. An optional
  precondition, `BlobPrecondition::Tags` (the current ETag is one of the given
  tags) or `BlobPrecondition::AnyExisting` (the cell is not null), fails with
  `BlobWritePreconditionFailed`, which names the current ETag, and changes
  nothing. A put returns the value's length, its ETag and the commit; the ETag
  is the one a read at that commit reports. One put holds at most 32 MiB,
  inclusive; a larger one fails before any table is opened with resource
  `Blob write payload bytes`. The HTTP server exposes them as `PUT` and
  `DELETE /blob`; the CLI does not expose them yet.
