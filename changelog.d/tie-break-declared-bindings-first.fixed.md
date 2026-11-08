- Rows that tie on every `order` key are now ordered by the identities of the
  bindings the query declares before those of anonymous traversal endpoints,
  so a `limit` cuts them as the declared bindings' order says. See the
  [explain guide][tie-break-explain].

[tie-break-explain]: ../docs/user/queries/explain.md
