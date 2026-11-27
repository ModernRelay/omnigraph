- Arithmetic (`+ - * / %`, unary `-`), `if` and `coalesce` widen mixed numeric types to their common type, and `-3` takes the other operand's type; a zero divisor, `-0.0` included, is `E2`. See [values][arith-widen-guide].

[arith-widen-guide]: ../docs/user/queries/traversal.md#values-and-inline-reads
