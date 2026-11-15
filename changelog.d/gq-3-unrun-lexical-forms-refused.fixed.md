- A `bm25` over `terms(…)` with `mode: all` or an edit budget, alone or as an `rrf` arm, is refused at type check (`T86`): the scan ranked it as any-term with no edits, returning documents that hold only some of the terms. A `match_terms` predicate, which type-checked and then failed when it ran, is refused at type check (`T86`) too. Pass `bm25` a String for its any-term, zero-edit form. See [terms this release does not lower][gq-3-t86-lexical-guide].

[gq-3-t86-lexical-guide]: ../docs/user/queries/diagnostics.md#terms-this-release-does-not-lower
