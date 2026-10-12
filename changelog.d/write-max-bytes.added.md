- Bounded writes now accept the [`write_max_bytes` session setting][write-max-bytes],
  from `1` through `33554432` bytes (the unchanged 32 MiB default), with
  `OMNIGRAPH_WRITE_MAX_BYTES` supplying the process default.
  Row data and logical Blob payloads have independent allowances, so a Blob exactly
  at the limit can accompany scalar metadata that fits its own allowance.
  The default now admits up to 32 MiB of row data plus 32 MiB of Blob payload
  per operation, where the previous single 32 MiB allowance covered both.
  HTTP body, Blob read, compaction and URI metadata limits remain independent.

[write-max-bytes]: ../docs/user/queries/settings.md
