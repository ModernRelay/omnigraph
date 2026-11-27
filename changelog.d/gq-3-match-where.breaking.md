- GQ 3 replaces the query language. A read is `match { … }` with comma-separated elements, one `where` predicate, then `return`, `order` and `limit`; a write group is `match { … } where …` and its statements. Stored queries in the old syntax fail to compile, and each refusal prints its fix. See the [query guide][gq-3-match-where-guide].

[gq-3-match-where-guide]: ../docs/user/queries/index.md#match-patterns
