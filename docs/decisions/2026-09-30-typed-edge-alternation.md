# Typed edge alternation and bounded wildcard traversal

**Decides:** a GQ traversal can select several edge types, by name (`(knows | likes)`) or by wildcard (`*`), and searches their combined neighbors as one traversal, with omitted hop bounds meaning exactly `{1,1}` and a shared finite traversal-work budget.
**Contract:** keeps E1, E5, E7, read:1, read:3
**Code:** `crates/omnigraph-compiler/src/traversal.rs`, `crates/omnigraph-compiler/src/ir/untyped.rs`, `crates/omnigraph-planner/tests/query_plan/edge_selections.rs`, `crates/omnigraph/src/engine/plan_source.rs`, `crates/omnigraph/src/engine/operators`, `crates/omnigraph-gqt/cases/issue_659_edge_selection_semantics.gqt`

## Context

Exploring a graph across several relationships needs concrete edge identity
in the results. Running one traversal per type and combining the results
misses paths whose successive hops use different types, and gets the
combined shortest distance wrong. The traversal must search the union of
the members' neighbors. A wildcard also selects an unbounded set of tables,
so the work a query admits must be bounded by something other than its
result size.

## Decision

- A named edge, an alternative list and a wildcard are distinct typed
  selectors. Resolution records canonical members and per-member direction
  against the invocation's captured catalog; a wildcard is resolved once,
  and plans and saved plans carry the complete member set and pin every
  member dataset.
- Omitted bounds mean exactly `{1,1}`. More than one hop needs a finite
  `{min,max}` with `1 <= min <= max`. Undirected traversal and any bound above
  one hop need the same node type at both ends; a wider cross-type range is
  refused (`T5`), not capped.
- All members connect one source and one destination node type. A wildcard
  needs both endpoint types declared in the same block or a visible outer
  scope. Every traversal component needs an executable endpoint binding.
- An unbound traversal returns distinct endpoint pairs, with shortest-distance
  hop semantics and frontier and visited state shared across members. A
  bound single-hop edge returns one row per concrete edge, identified by
  canonical type then id, and exposes `@type`, `@id`, `@src`, `@dst`.
- A bound edge property must exist on every member with a compatible type:
  nullable if any member is, enum domains combined, enum plus String is
  String; anything else is a type error. An empty wildcard returns no rows
  with the declared columns and refuses ordinary edge properties.
- Statements with alternatives or wildcard share one `traversal_work_limit`
  (default 1,000,000) across operators, members and hops. Work is admitted
  before each scan from trusted physical row counts; a result `limit` does
  not bound it. Exhaustion is a typed terminal error, never a short result.
- The budgeted route uses pinned Lance scans. Forced CSR traversal is refused
  there, because the shared graph-index cache can decode more than the
  selected members and selected row counts cannot admit that work.
- A wildcard on an explicitly historical target is refused, since the
  captured catalog does not reconstruct historical membership. An explicit
  member absent at the snapshot is an error.
- Selection adds no type-scoped permission; existing graph, branch and
  stored-query gates apply.

## Rejected

- **Separate named queries:** cannot express a mixed-type path in one
  traversal.
- **Traverse each type, then combine:** misses mixed-type paths and gets the
  combined shortest distance wrong.
- **Nullable properties missing from some members:** viable, but strict
  common-property checking keeps the contract smaller.
- **Require `{1,1}` everywhere:** a fixed one-hop default has an exact
  meaning and keeps queries compact.
- **Use result `limit` as the work bound:** sorting, aggregation and scans do
  substantial work before any limited output.
