- Export and the change-feed baseline read the managed Blob values of each
  batch of rows with one planned read per Blob column instead of one read per
  row, and change-feed images and entity reads use the same reader for their
  row. Memory stays bounded by one row's values plus, per Blob column, an
  8 MiB read buffer or one larger value. See
  [Blob maintenance and export][export-batched-blob-reads].

[export-batched-blob-reads]: ../docs/dev/blob.md#maintenance-and-export
