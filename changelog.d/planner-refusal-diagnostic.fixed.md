- A query shape the planner refuses by design, a `nearest()` or `bm25()`
  ranking on a binding a traversal reaches, is a bad request with the planner
  code `P001`, the refused expression and a fix that answers the query:
  declare that binding first in `match`. An `rrf()` whose arms rank two
  bindings one traversal connects is refused as `P005` without a fix, since no
  declaration order serves both arms. See the [diagnostics guide][p001-diagnostics].

[p001-diagnostics]: ../docs/user/queries/diagnostics.md
