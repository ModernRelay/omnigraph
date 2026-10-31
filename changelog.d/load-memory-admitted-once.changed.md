- A `load csv` overwrite admits `query_memory_bytes` once, one share per
  partition; a refusal reports the smallest admitting limit; a keyed overwrite
  near the limit may need more ([settings][load-memory-shares]).

[load-memory-shares]: ../docs/user/queries/settings.md
