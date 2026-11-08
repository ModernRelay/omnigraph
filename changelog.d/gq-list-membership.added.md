- GQ has a list-membership predicate: `$x.number in $numbers`, or
  `$x.number in ["A-1", "A-2"]` with a literal list. On a matched binding's
  property it filters that binding's scan. `in` is a reserved word as a
  result. See the [query guide][gq-list-membership-guide].

[gq-list-membership-guide]: ../docs/user/queries/index.md#strings-and-lists
