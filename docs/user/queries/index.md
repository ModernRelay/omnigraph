# Query Language (`.gq`)

Pest grammar at `crates/omnigraph-compiler/src/query/query.pest`. AST in `query/ast.rs`. Type checker in `query/typecheck.rs`. Lowering in `ir/lower.rs`.

## Query declarations

```
query <name>($p1: T1, $p2: T2?, …)
  @description("…") @instruction("…") {
  …
}
```

Two body shapes:

- **Read**: `match { … } return { … } [order { … }] [limit N]` — covered on this page.
- **Mutation**: one or more of `insert | update | delete` statements — see [mutations](../mutations/index.md).

Multi-modal search functions (`nearest`, `bm25`, `rrf`, …) used inside `match`,
`return`, and `order` are documented on the [search](../search/index.md) page.

Param types reuse all schema scalars; trailing `?` makes a param optional. The compiler reserves `$__nanograph_now` for `now()`.

## MATCH clauses

- **Binding**: `$x: NodeType { prop: <literal | $param | now()>, … }`
- **Traversal**: `$src EDGE_NAME { min, max? } $dst` — variable-length paths via hop bounds; default 1..1 if bounds omitted.
- **Filter**: `<expr> <op> <expr>` with operators `>=`, `<=`, `!=`, `>`, `<`, `=`, and string `contains`.
- **Negation**: `not { clause+ }` — desugars to anti-join over the inner pipeline.

## RETURN clause

`return { <expr> [as <alias>], … }` with expressions:

- Variable / property access: `$x`, `$x.prop`
- Literals: string, int, float, bool, list
- `now()`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`
- [Search functions](../search/index.md) (so you can return a score column)
- `AliasRef` — re-use a previous projection alias

## ORDER & LIMIT

- `order { <expr> [asc|desc], … }` — supports plain expressions and `nearest(...)`.
- `limit <integer>` — required when there is a `nearest(...)` ordering.
- **Total, deterministic order.** Rows with equal user-sort keys are broken by the bound entities' key columns (`<var>.id`, ascending) appended as a final tie-break, so the result is a *total* order — reproducible across runs, and `order … limit N` returns a deterministic top-N even when ties straddle the cutoff. (Aggregate results have no entity-key columns; their group rows are already distinct on the projected group keys.)
- **NULL placement** is *nulls-first ascending, nulls-last descending* (i.e. `nulls_first = !descending`): a NULL sorts as if smaller than any value.

Write statements (`insert` / `update` / `delete`) are documented on the
[mutations](../mutations/index.md) page.

## IR (Intermediate Representation)

`QueryIR { name, params, pipeline: Vec<IROp>, return_exprs, order_by, limit }`

Pipeline operations:

- `NodeScan { variable, type_name, filters }`
- `Expand { src_var, dst_var, edge_type, direction (Out|In), dst_type, min_hops, max_hops, dst_filters }` — destination filters are pushed *into* the expand so Lance scalar pushdown can prune. Executed one of two ways, chosen per-expand by a cost model over cheap manifest counts (frontier size, |E|, source-vertex count, hops) plus index coverage: selective traversals (small frontier relative to the source set) resolve neighbors from the persisted `src`/`dst` BTREE (one indexed scan per hop); dense / deep / large-frontier traversals — or those whose BTREE coverage is degraded so a full scan would be paid per hop — use the in-memory CSR adjacency index. Both produce identical results. The `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` / `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` ceilings bound the *initial dispatch* frontier/hops (beyond them CSR is always used); the cost model estimates total indexed work as ~`hops × frontier × fanout` and prices dense fan-out toward CSR — they are not a hard per-hop bound. `OMNIGRAPH_TRAVERSAL_MODE=indexed|csr` forces a mode (see [constants](../reference/constants.md)).
- `Filter { left, op, right }`
- `AntiJoin { outer_var, inner: Vec<IROp> }` — for `not { … }`

Lowering:

1. Partition MATCH clauses (bindings, traversals, filters, negations).
2. Identify "deferred" bindings (a destination of a traversal that has filters) so the Expand can carry the filter as a pushdown.
3. Emit NodeScan for the first binding, then Expand operations, then remaining Filter operations, then AntiJoins for negations.
4. Translate RETURN / ORDER expressions; preserve LIMIT.

## Linting & validation (`query/lint.rs`)

Codes seen so far:

- **Q000** (Error): parse error
- **L201** (Warning): nullable property never set by any UPDATE — "{type}.{prop} exists in schema but no update query sets it"
- (Warning): mutation declares no params — hardcoded mutations are easy to miss
- Plus all type errors from `typecheck_query_decl()` (undefined types, mismatched operators, undefined edges, etc.)

Output:

```
QueryLintOutput { status, schema_source, query_path,
  queries_processed, errors, warnings, infos,
  results: [{ name, kind, status, error?, warnings[] }],
  findings: [{ severity, code, message, type_name?, property?, query_names[] }] }
```

CLI exits non-zero only on `status = Error`.
