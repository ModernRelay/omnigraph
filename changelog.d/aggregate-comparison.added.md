- A Boolean expression over aggregates, literals, `now()` and parameters in an aggregated `return`, such as `count($p) > 0 as has_member`, is evaluated per group after aggregation and projects a Bool column under its alias (`Bool?` when an operand can be null); the alias is an `order` key. A group value beside an aggregate in such an expression is refused (`T58`). See [Return, order, and limit][aggregate-comparison-guide].

[aggregate-comparison-guide]: ../docs/user/queries/index.md#return-order-and-limit
