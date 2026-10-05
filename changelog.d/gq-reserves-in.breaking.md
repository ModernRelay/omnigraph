- GQ also reserves `in`, the sixth word beside `and`, `or`, `not`, `is` and
  `null`. Before upgrading, check every schema and stored query for the same
  three spellings: a property named `in` written bare as an operand of a
  mutation `where`, a return alias `as in`, and an edge type named `in`
  traversed bare. Rename the property or edge with `@rename_from`, or write the
  edge as a string (`$a "in" $b`). See the [query guide][gq-reserves-in-guide].

[gq-reserves-in-guide]: ../docs/user/queries/index.md
