- A `load csv` overwrite admits `query_memory_bytes` once, before any
  partition reads, one share per parse partition; a refusal reports the
  smallest limit that would have admitted the refused charge. A keyed
  overwrite near the limit may now need a higher `query_memory_bytes` than
  before, since a partition's share is a fixed part of the room. See
  [settings][load-memory-shares].

[load-memory-shares]: ../docs/user/queries/settings.md
