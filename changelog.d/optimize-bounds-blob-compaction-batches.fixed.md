- `omnigraph optimize` bounds the Blob payload of each compaction batch.
  Compaction reads every managed Blob value of a batch into memory, and a
  batch could span a whole fragment. Optimize now sizes batches from the
  largest row it compacts, summed over that row's Blob properties, so one
  batch holds at most 32 MiB of managed Blob payload; a row larger than that
  is compacted alone. This bounds the payload of a batch, not total memory,
  and `LANCE_DEFAULT_BATCH_SIZE` does not change it. See
  [Optimize][optimize-blob-compaction-batches].

[optimize-blob-compaction-batches]: ../docs/user/operations/maintenance.md#optimize
