- A read may write `null` where its context fixes its type: an `if` branch, a `coalesce` argument, an assigned value. `$p.x = null` is `T7`, fixed to `is null`. See [nulls][gq-3-typed-null-guide].

[gq-3-typed-null-guide]: ../docs/user/queries/index.md#boolean-expressions-and-nulls
