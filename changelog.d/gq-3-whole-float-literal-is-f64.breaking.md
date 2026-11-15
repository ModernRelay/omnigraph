- A whole-number float literal is `F64`: `$x.n > 9007199254740992.0` converts an integer property to `F64` instead of narrowing the literal, so rows change above 2^53, where integers round together. Write an integer literal to compare exactly. See [numeric comparisons][gq-3-float-literal-guide].

[gq-3-float-literal-guide]: ../docs/user/queries/index.md#boolean-expressions-and-nulls
