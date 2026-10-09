- A read that projects a wide column no longer fails with `resource limit
  exceeded for query_memory_bytes` once the type's projected columns exceed the
  150 MiB query pool, even when it returns one row: every unranked node scan now
  streams bounded batches instead of holding the whole type, so an unordered
  `limit` stops reading after its first batches and a top-k holds only its kept
  rows. Under a `limit` over a type with more than four rows per kept row, the
  columns only the output reads are fetched by row address for the kept rows
  alone; `explain` shows a `HydrateColumns` node and the
  `late_materialization` pass. See [execution][hydrate-execution].

[hydrate-execution]: ../docs/dev/execution.md
