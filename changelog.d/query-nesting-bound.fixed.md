- A query nested past 2,048 levels, or with an expression deeper than 16,384, is refused with `Q006`; any query within those bounds compiles and runs on default-size thread stacks. See [parse codes][codes].

[codes]: ../docs/user/queries/diagnostics.md#parse-codes
