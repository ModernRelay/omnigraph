- A `load csv` overwrite into an edge type whose endpoint node types carry a
  single integer `@key` (`I32`, `I64` or `U32`) now checks every edge's `__src`
  and `__dst` against an integer key set built once per endpoint table, read
  straight from the held batches, instead of copying each edge's id and
  endpoints into strings and probing string sets row by row. Which edge is
  refused, and with which message, is unchanged; endpoint types keyed by text
  or `U64` keep the previous check. The planner now counts the key list a
  probe builds, one entry per input row, so an endpoint check whose node
  table has no usable `__id` index plans `scan` (one read of the id column)
  instead of a probe with one literal per edge endpoint; `explain` shows the
  access and both costs. A usable index with partial or unknown coverage is
  costed as a full one, so a small key list keeps its filtered probe and a
  large one scans. Integer sets, their build buffers and a probe's key list
  are admitted against `query_memory_bytes`; oversized hash buckets use
  bounded binary searches.
