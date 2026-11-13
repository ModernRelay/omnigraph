- A block or an inline read binds every name new to it: a triple naming a name it neither reads from the enclosing scope nor binds is `T84`, and binding an outer name inside a block is `T68`. Queries add the binding, `not exists { $q: Person, $p knows $q }`, and write a block that only filtered an outer binding as the filter, `not ($p.name = "bob")`; each refusal prints the fix. See [inner scopes][gq-3-inner-scopes-guide].

[gq-3-inner-scopes-guide]: ../docs/user/queries/traversal.md#inner-scopes
