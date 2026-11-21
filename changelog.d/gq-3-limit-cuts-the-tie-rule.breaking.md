- A `limit` with no `order` returns the first rows under the tie rule, each reference's id in the order the pattern binds it, then the other columns' values, rather than the first rows a scan yields; group rows sort by their keys. Reads where more rows qualify than the limit return different rows and read every qualifying row; add an `order` for the rows you mean. See [order and limit][gq-3-tie-rule-guide].

[gq-3-tie-rule-guide]: ../docs/user/queries/index.md#return-order-and-limit
