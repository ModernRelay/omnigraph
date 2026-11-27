- A `limit` with no `order` returns the first rows under the tie rule (each reference's identity in pattern order, a multi-type edge's type by stable type id, then other columns), not a scan's first rows; group rows sort by their keys. Where more rows qualify than the limit, it returns other rows and reads them all; add an `order`. See [order and limit][gq-3-tie-rule-guide].

[gq-3-tie-rule-guide]: ../docs/user/queries/index.md#return-order-and-limit
