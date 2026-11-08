- A load statement now shares its committed-state reads: validation opens one
  statement-scoped Lance session with held table handles and a byte-range memo,
  so identical metadata, index and data reads are fetched once per statement
  instead of once per check. The reuse buffers are admitted against
  `query_memory_bytes`, which now also bounds this load-statement state; an
  allowance that cannot admit them keeps the previous uncached reads. See the
  [settings guide][load-reuse-settings].

[load-reuse-settings]: ../docs/user/queries/settings.md
