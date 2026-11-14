- A block, an `optional` or an inline read binds every name new to it: a triple naming a name it neither reads from outside nor binds is `T84`, and binding an outer name inside a block or an `optional` is `T68`. Queries add the binding, `optional { $c: Company, $p worksAt $c }`, and write a block that only filtered an outer binding as that filter, `not ($p.name = "bob")`; each prints the fix. See [inner scopes][gq-3-inner-scopes-guide].

[gq-3-inner-scopes-guide]: ../docs/user/queries/traversal.md#inner-scopes
