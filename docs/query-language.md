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

- **Read**: `match { … } return { … } [order { … }] [limit N]`
- **Mutation**: one or more of `insert | update | delete` statements

Param types reuse all schema scalars; trailing `?` makes a param optional. The compiler reserves `$__nanograph_now` for `now()`.

## MATCH clauses

- **Binding**: `$x: NodeType { prop: <literal | $param | now()>, … }`
- **Traversal**: `$src EDGE_NAME { min, max? } $dst` — variable-length paths via hop bounds; default 1..1 if bounds omitted.
- **Filter**: `<expr> <op> <expr>` with operators `>=`, `<=`, `!=`, `>`, `<`, `=`, and string `contains`.
- **Negation**: `not { clause+ }` — desugars to anti-join over the inner pipeline.

## Search clauses (multi-modal)

Used inside MATCH or as expressions inside RETURN/ORDER:

| Function | Purpose | Underlying Lance facility |
|---|---|---|
| `nearest($x.vec, $q)` | k-NN vector search (cosine) | Lance vector index (IVF / HNSW) |
| `search(field, q)` | Generic FTS | Inverted index |
| `fuzzy(field, q [, max_edits])` | Levenshtein-tolerant text search | Inverted index |
| `match_text(field, q)` | Pattern match | Inverted index |
| `bm25(field, q)` | BM25 scoring | Inverted index |
| `rrf(rank_a, rank_b [, k])` | Reciprocal Rank Fusion of two rankings (default k=60) | OmniGraph fuses scored rankings |

`nearest()` requires a `LIMIT`; the compiler resolves the query vector via the param map (or via the runtime embedding client when bound to a text input).

## RETURN clause

`return { <expr> [as <alias>], … }` with expressions:

- Variable / property access: `$x`, `$x.prop`
- Literals: string, int, float, bool, list
- `now()`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`
- All search functions above (so you can return a score column)
- `AliasRef` — re-use a previous projection alias

## ORDER & LIMIT

- `order { <expr> [asc|desc], … }` — supports plain expressions and `nearest(...)`.
- `limit <integer>` — required when there is a `nearest(...)` ordering.

## Mutation statements

- `insert <Type> { prop: <value>, … }`
- `update <Type> set { prop: <value>, … } where <prop> <op> <value>`
- `delete <Type> where <prop> <op> <value>`

`<value>` is a literal, `$param`, or `now()`. Multi-statement mutations execute atomically (added in v0.2.0).

### D₂ — mixed insert/update + delete is rejected at parse time

A single mutation query must be **either insert/update-only or delete-only**. Mixed → rejected before any I/O with the message:

> `mutation '<name>' on the same query mixes inserts/updates and deletes; split into separate mutations: (1) inserts and updates, then (2) deletes. This restriction lifts when Lance exposes a two-phase delete API (tracked: MR-793 / Lance-upstream).`

Reason: under the staged-write rewire (MR-794), inserts and updates accumulate in memory and commit at end-of-query, while deletes still inline-commit (Lance 4.0.0 has no public two-phase delete). Mixing creates ordering hazards (same-row insert→delete becomes a no-op because the staged insert isn't visible to delete; cascading deletes of just-inserted edges break referential integrity by silent design). Until Lance exposes `DeleteJob::execute_uncommitted`, the parse-time rejection keeps both paths atomic and correct. See [docs/runs.md](runs.md) and [docs/invariants.md §VI.25](invariants.md).

## IR (Intermediate Representation)

`QueryIR { name, params, pipeline: Vec<IROp>, return_exprs, order_by, limit }`

Pipeline operations:

- `NodeScan { variable, type_name, filters }`
- `Expand { src_var, dst_var, edge_type, direction (Out|In), dst_type, min_hops, max_hops, dst_filters }` — destination filters are pushed *into* the expand so Lance scalar pushdown can prune.
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
