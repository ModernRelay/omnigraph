- `schema apply` widens an edge type's `@card` range in place, such as `@card(1..1)` to `@card(0..1)`, without rewriting data; tightening one is still refused ([guide][edge-cardinality], [#1403][1403]).

[edge-cardinality]: ../docs/user/schema/index.md#edge-cardinality
[1403]: https://github.com/ModernRelay/omnigraph/issues/1403
