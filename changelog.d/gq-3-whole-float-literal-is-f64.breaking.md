- A float literal is `F64` even when its value is a whole number: an integer property compared with one, such as `$x.n > 9007199254740992.0`, converts the integer to `F64` instead of narrowing the literal to the integer type. Rows change only for integers beyond 2^53, where distinct integers round to one `F64`, so `9007199254740993` now equals `9007199254740992.0`; write an integer literal to compare exactly. See [numeric comparisons][gq-3-float-literal-guide].

[gq-3-float-literal-guide]: ../docs/user/queries/index.md#boolean-expressions-and-nulls
