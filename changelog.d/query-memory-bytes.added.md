- The per-query memory pool is now the [`query_memory_bytes` session
  setting][query-memory-bytes] (`request` scope, at least `1`, unchanged
  150 MiB default, `OMNIGRAPH_QUERY_MEMORY_BYTES` supplying the process
  default) instead of a hardcoded constant. `set query_memory_bytes = …` in a
  statement prefix or the request's `settings` field sizes that one query's
  pool, and explain's `assumptions.memory_limit` reports the value in effect.
  Operators that cannot spill refuse work beyond the pool; spillable operators
  may use bounded scratch storage.

[query-memory-bytes]: ../docs/user/queries/settings.md
