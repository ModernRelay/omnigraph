- `count(distinct $x)` counts distinct nodes by identity and `count(distinct $x.prop)` counts distinct non-null values, grouped or ungrouped, in `return` and as an `order` key; plain `count` keeps counting rows. `distinct` under `sum`, `avg`, `min` or `max` is refused (`T57`). See [Return, order, and limit][count-distinct-guide].

[count-distinct-guide]: ../docs/user/queries/index.md#return-order-and-limit
