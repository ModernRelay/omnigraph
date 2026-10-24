- An edge `load csv` whose endpoint checks read their committed node ids
  whole (`access scan`) over node types keyed by one `I32`, `I64` or `U32`
  `@key` probes them on the
  parse partitions as the rows arrive: the committed sets load while the
  source parses, once per endpoint table, and each partition keeps its
  earliest missing endpoint, so the check no longer runs over the whole delta
  on one thread after the parse. The refusal is unchanged, the first missing
  endpoint in row order, `from` before `to` within a row. The plan says so:
  `explain load` prints `probe` (`swept` or `judged`) on every `edge_ri`
  check, and a GQT `load validate edge_ri <edge> <from|to> <Node>: probe
  <swept|judged>` line pins it. See
  [Explaining a load statement][explain-load-probe].

[explain-load-probe]: ../docs/user/queries/explain-load.md
