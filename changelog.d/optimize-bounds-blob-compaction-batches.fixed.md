- `omnigraph optimize` bounds the Blob payload of each compaction batch.
  Compaction reads every managed Blob value of a batch into memory, and a
  batch could span a whole fragment. Optimize now sizes each compaction
  task's batches from that task's largest row, summed over the row's managed
  Blob values (external references are never read and count nothing), so one
  batch holds at most 32 MiB of managed Blob payload. A task holding a row
  over 16 MiB compacts one row per batch, and a row over 32 MiB is
  materialized whole. This bounds the payload of a batch, not total memory.
  Every task of a Blob table gets the derived size explicitly, 1 to 8192 rows,
  so it takes precedence over `LANCE_DEFAULT_BATCH_SIZE` there, including when
  the variable is smaller. When a fragment being compacted holds a Blob
  descriptor the engine's decoder rejects, sizing can refuse the compaction
  with a Blob integrity error before that table is rewritten, and nothing is
  committed; a table with nothing to compact is not scanned. See
  [Optimize][optimize-blob-compaction-batches].

[optimize-blob-compaction-batches]: ../docs/user/operations/maintenance.md#optimize
