- Compiling a query takes work linear in its text: a `let` is computed once and read as a column, and a nested `optional` stores its pattern once, so deep nesting fits the plan budget. See [optional][nested-optional-budget].

[nested-optional-budget]: ../docs/user/queries/traversal.md
