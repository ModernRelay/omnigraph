# Query Language (`.gq`)

## Query declarations

```
query <name>(@description("…") $p1: T1, $p2: T2?, …)
  @description("…") @instruction("…") @mcp(tool_name: "…", expose: true) {
  …
}
```

Two body shapes:

- **Read**: `match { … } return { … } [order { … }] [limit N]` — covered on this page.
- **Mutation**: one or more of `insert | update | delete` statements — see [mutations](../mutations/index.md).

Multi-modal search functions (`nearest`, `bm25`, `rrf`, …) used inside `match`,
`return`, and `order` are documented on the [search](../search/index.md) page.

Param types reuse all schema scalars; trailing `?` makes a param optional. The compiler reserves `$__nanograph_now` for `now()`.

### Annotations

Annotations after the param list are optional and order-independent:

- `@description("…")` — human-readable summary of the query (shown in the
  stored-query catalog and as the MCP tool description).
- `@instruction("…")` — agent-facing *how/when to use* guidance. It is folded
  into the [MCP](../operations/mcp.md) tool description (appended after
  `@description`), so an agent reading `tools/list` sees it.
- `@mcp(...)` — **MCP-presentation** controls for when the query is served as an
  agent tool (see [mcp.md](../operations/mcp.md)). Both keys are optional:
  - `tool_name: "<name>"` — the tool id to expose the query under (default: the
    query name). Must be unique across exposed queries and must not shadow a
    built-in tool, or the server refuses to boot.
  - `expose: <bool>` — whether the query appears on the agent tool surface
    (default `true`). `expose: false` keeps the query HTTP/service-callable by
    name but hides it from `tools/list` and the catalog. This is **presentation
    only** — not an authorization control (Cedar `invoke_query` governs who may
    call it).

A **per-parameter** `@description("…")` (written before the variable, e.g.
`@description("the user's slug") $slug: String`) documents that argument; it is
surfaced into the parameter's JSON-Schema `description` in the catalog and the
MCP tool input schema.

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

## Traversal execution

Variable-length traversals (`Expand`) are executed one of two ways, chosen per-expand by a cost model over cheap manifest counts (frontier size, edge count, source-vertex count, hops) plus index coverage: selective traversals (small frontier relative to the source set) resolve neighbors from the persisted `src`/`dst` BTREE (one indexed scan per hop); dense / deep / large-frontier traversals — or those whose BTREE coverage is degraded so a full scan would be paid per hop — use an in-memory CSR adjacency index. Both produce identical results. The `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` / `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` ceilings bound the *initial dispatch* frontier/hops (beyond them CSR is always used); the cost model estimates total indexed work as ~`hops × frontier × fanout` and prices dense fan-out toward CSR — they are not a hard per-hop bound. `OMNIGRAPH_TRAVERSAL_MODE=indexed|csr` forces a mode (see [constants](../reference/constants.md)).

## Linting & validation

Codes seen so far:

- **Q000** (Error): parse error
- **L201** (Warning): nullable property never set by any UPDATE — "{type}.{prop} exists in schema but no update query sets it"
- (Warning): mutation declares no params — hardcoded mutations are easy to miss
- Plus all type errors from type checking (undefined types, mismatched operators, undefined edges, etc.)

Lint output reports an overall status, per-query results (name, kind, status, any error and warnings), and structured findings (severity, code, message, and the type/property/query they apply to).

CLI exits non-zero only on `status = Error`.
