- A managed Blob value of exactly 32 MiB now fits in a write. Incremental
  writes (inserts, updates, `append` and `merge` loads and branch merges)
  check managed Blob payloads and the rest of the row against separate
  32 MiB ceilings. The old single ceiling counted both, so a 32 MiB value
  was always refused for the row around it. A payload refusal names its
  resource with `Blob payload bytes` in place of `bytes`, for example
  `keyed write Blob payload bytes for node:Document`. An update that carries
  an oversized Blob cell now reports `retained keyed batch Blob payload bytes
  per operation`. One operation may now hold up to 32 MiB of Blob payload
  plus 32 MiB of other row data, where it held 32 MiB in all. Encoded limits
  stay: `omnigraph load` and `/load/ndjson` cap each line at 32 MiB, and
  HTTP request bodies keep their caps, so those paths carry about 24 MiB of
  `base64:` data per value.
