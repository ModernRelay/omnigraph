- A `let`, `from` or `optional` whose value can fail runs on every row before `where`, as GQ 3 states, and no `where` conjunct runs before it; guard it with `if`. See [traversal][let-fail-guide].

[let-fail-guide]: ../docs/user/queries/traversal.md#values-and-inline-reads
