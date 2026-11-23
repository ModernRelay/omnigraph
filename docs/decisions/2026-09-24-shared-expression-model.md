# Shared expression model

**Decides:** GQ has one expression tree, used by every clause that holds a value or a condition (match filter, mutation `where`, assignment, inline match, `return`, `order`), and what differs per clause is a context rule checked over that one type.
**Contract:** keeps write:1, read:10, E3; serves invariant 9
**Code:** `crates/omnigraph-compiler/src/query/query.pest`, `crates/omnigraph-compiler/src/query/ast.rs`, `crates/omnigraph-compiler/src/query/typecheck.rs`, `crates/omnigraph-compiler/src/ir/mod.rs`, `crates/omnigraph-planner/src/logical.rs`, `crates/omnigraph-planner/src/optimizer.rs`, `crates/omnigraph/src/engine/expr.rs`, `crates/omnigraph/src/engine/scan.rs`, `crates/omnigraph/src/table_store.rs`

## Context

Each clause had its own small condition language: a mutation `where` and a
match filter were each exactly one comparison, and an `order` key was a
property or an alias. So `delete Knows where @src = "a" and @dst = "b"`
could not be written, two statements gave the union instead, and `or` and
`order { count($d) }` were refused. Four AST condition shapes had four type
checkers, lowerings and evaluators. The mutation path printed its predicate
as a Lance SQL string, a deny-listed shape. Adding `and` to one shape would
have made a fifth.

## Decision

- The AST `Expr` and IR `IRExpr` gain `Binary { left, op, right }` (with
  `BinaryOp` of `Compare`, `And`, `Or`), `Not` and `IsNull`. The separate
  filter, match-value and mutation-predicate types are gone.
- Precedence, loosest first: `or`, `and`, `not`, comparison. A comparison is
  non-associative. `is null` and `is not null` sit at comparison level.
  `and`, `or`, `not`, `is` and `null` are reserved in operand and alias
  position; `$p.and` after a dot stays legal.
- Three-valued logic: a comparison with a null operand is null, Kleene
  `and` and `or`, and a filter or `where` keeps only rows whose expression is
  true. GQ has no `null` literal; `is null` is the only null test.
  `and`, `or`, `not` need `Bool` operands (`T41`).
- Context rules are checks, not types: a filter and `where` require `Bool`;
  a `where` refuses binding variables, aggregates and search or ranking
  calls (`T44`); assignments and inline matches take constants only, never
  a stored property (`T45`); a comparison in `return` needs an alias
  (`T43`); a search call stands only as a top-level conjunct (`T38`).
- A constant is a function of query text and parameters fixed before the
  retry loop, `now()` included, so every attempt computes the same value. A
  null result for a non-nullable property is a typed error naming it.
- An `order` key other than a property, a system field or the leading
  search key binds to the `return` list, by alias or by tree equality, or is
  refused (`T42`). No hidden item is added for it.
- A logical `Filter` holds its conjunct list, split once at construction.
  `place_query_filters` coalesces adjacent filters in one scope and places
  each conjunct; two filter clauses and one `and` give one plan.
- `IRExpr` derives `PartialEq`, `Eq` and `Hash`; float literals compare by
  bits.
- Reads and mutations lower through one `ir_expr_to_df_expr`. A delete
  hands Lance the typed expression through `DeleteBuilder::from_expr`, and
  the staging ledger composes expressions, so no SQL text is built on the
  mutation path.
- No expression in the model can fail at evaluation, so conjunct order is
  unobservable. A fallible node must not be pushed ahead of its guards.

## Rejected

- **Mutation-only `and`:** a fifth condition shape, still no `or` or
  grouping, and the SQL string path grows.
- **`and` only, as a list of comparisons:** cannot express `a or b`; `not`
  and grouping need the tree anyway.
- **DataFusion's `Expr` as the IR:** graph typing, `@id` roles and search
  calls that never lower to DataFusion live in `IRExpr`, and the language
  would follow a dependency's enum.
- **Extend `predicate_to_sql`:** keeps deny-listed string generation and a
  mutation-only evaluator that disagrees with reads on null and type rules.
- **Hidden `order` items for any key:** an aggregate key on a
  non-aggregated `return` would change grouping and rows.
- **A separate `where { }` clause after `match`:** drops the
  condition-beside-pattern form users expect and rewrites every query.
