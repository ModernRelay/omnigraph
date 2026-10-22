- An `rrf()` whose arms rank two bindings one traversal connects is now a type
  error, `T59`, carrying the refused expression, instead of the planner code
  `P005`. The planner codes `P001` and `P005` are withdrawn; a client matching
  on either reads `T59` for the `rrf()` shape. See the [diagnostics
  guide][rrf-connected-diagnostics].

[rrf-connected-diagnostics]: ../docs/user/queries/diagnostics.md
