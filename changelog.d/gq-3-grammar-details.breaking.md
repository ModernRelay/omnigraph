- GQ 3 settles three grammar details: whitespace may follow a minus sign (`[- 1]` reads as `[-1]`), a keyword or reserved word is never a call name (`where(…)` and `and(a, b)` are `Q019`, with the form printed as the fix), and an annotation without arguments takes no parentheses (`@name()` is `Q012`, fixed to `@name`). See the [query guide][gq-3-grammar-details-guide].

[gq-3-grammar-details-guide]: ../docs/user/queries/index.md#declarations-and-parameters
