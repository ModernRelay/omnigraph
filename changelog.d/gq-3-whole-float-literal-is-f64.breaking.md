- Beside an integer property, a whole-number float literal is `F64`: `$x.n > 9007199254740992.0` converts the property to `F64` instead of narrowing the literal, so rows change above 2^53. Beside an `F32` property a literal takes `F32` when exact. Write an integer literal to compare exactly. See [numeric comparisons][gq-3-float-literal-guide].

[gq-3-float-literal-guide]: ../docs/user/queries/index.md#boolean-expressions-and-nulls
