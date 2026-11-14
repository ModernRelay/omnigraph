- A query whose `match` holds two patterns that share no variable is refused with `T81` unless every pattern but one is a single binding pinned to one row by its `@key` or `@id` in `where`. Stored queries that relied on the product join the patterns through a triple, or write the product as a named relation drawn with `from`; the refusal prints the fix. See [match patterns][gq-3-t81-guide].

[gq-3-t81-guide]: ../docs/user/queries/traversal.md#patterns-that-share-no-variable
