# Query Language (`.gq`)

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
- **Traversal**: `$src EDGE_NAME { min, max? } $dst` — variable-length reachability via hop bounds; default 1..1 if bounds omitted. Edge idents are lowercase-initial by grammar: a schema declares `edge Knows` (uppercase-initial, enforced), and a query traverses it as `knows` — the case flip is required, not stylistic, since the parser uses the leading case to tell a traversal from a node binding. Unbound traversal reports any destination at most once per source instead of enumerating every possible walk. Once a node has been visited, it is not added to the search frontier again. A stored self-loop still counts as one edge and can produce its node at the current hop when that hop is within bounds, but the node is not searched again; other returns to visited nodes are pruned.
- **Undirected traversal**: `$src <EDGE_NAME> $dst` — matches the edge in *either* direction with set semantics (a pair connected both ways, or a self-loop, appears once). Only valid on same-endpoint-type edges (e.g. `Related: Issue -> Issue`); an asymmetric edge is rejected at typecheck (`T22`) since it is well-typed in at most one orientation — use the directional form there. Composes with hop bounds (`$a <knows>{1,3} $b`) and `not { }` ("no edge in either direction").
- **Edge binding**: `$src $w:EDGE_NAME $dst` — an optional `$var:` prefix on the edge word binds the matched edge so its non-Blob declared properties become addressable anywhere a node property is: filters (`$w.confidence = "asserted"`), projections (`return { $w.role }`), ordering. Composes with the undirected form (`$a $w:<related> $b`) and inside `not { }` (usable within the block, never in `return`). Semantics change with a binding present: the traversal emits **one result row per matching edge**, so parallel edges between the same endpoints appear individually (unbound traversals keep their set-of-pairs semantics). Rejected with `T23` on multi-hop bounds (a `{min,max}` path matches many edges — no single edge to bind), on a name already bound, and on bare use (`return { $w }` — project a property instead).
- **Filter**: `<expr> <op> <expr>` with operators `>=`, `<=`, `!=`, `>`, `<`, `=`, plus the string predicates `contains` and `starts_with`.
- **Negation**: `not { clause+ }` — desugars to anti-join over the inner pipeline.

### String predicates

- `$x.prop contains <needle>` is overloaded on the left operand's type: a **list** property tests membership; a scalar **String** property tests exact substring containment.
- `$x.prop starts_with <needle>` tests an exact prefix on a scalar String property.
- Both are **exact and case-sensitive** — no tokenization, stemming, or case folding (for token-based relevance matching use the [search functions](../search/index.md); for case-insensitive matching store a normalized property). A `NULL` value on either side is never a match. `_` and `%` in the needle are literal characters, not wildcards.
- Operands are **positional**, like comparisons: `X contains Y` tests that X contains Y, and `X starts_with Y` tests that X begins with Y, whichever side each operand is on — `"a haystack" contains $p.name` asks whether the *literal* contains the entity's `name` property. Index acceleration applies to the canonical property-on-the-left form. One grammar caveat: a **bare variable** as the left operand (`$q contains $m`) parses as a traversal over an edge named `contains`, not as a filter — use a property access or literal on the left.
- Both predicates are correct with or without an index. A predicate referencing exactly one binding is pushed into the Lance scan that introduces that binding — a direct node scan or a traversal's destination scan — where a covering index accelerates it: BTREE for `starts_with` (exact prefix range), NGRAM for String `contains` (trigram probe + recheck). See [indexes](../search/indexes.md) for which properties get which index.

## RETURN clause

`return { <expr> [as <alias>], … }` with expressions:

- Variable / property access: `$x`, `$x.prop`
- Literals: string, int, float, bool, list
- `now()`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`
- [Search functions](../search/index.md) (so you can return a score column)
- `AliasRef` — re-use a previous projection alias

Blob-valued properties and parameters are not `.gq` read values: they cannot be
projected, ordered, or passed to aggregates. Those read-value uses return `T24`;
Blob match/filter and mutation-predicate uses are also rejected by their
context-specific diagnostics. Embedded callers read a node or edge Blob cell
through the dedicated `Omnigraph::read_blob_at` facade with an explicit branch
or snapshot target; it returns managed bytes through a bounded reader or an
external descriptor without exposing Lance types. This does not make Blob an
ordinary `.gq` value. Blob parameters remain valid for mutation assignment.

## ORDER & LIMIT

- `order { <expr> [asc|desc], … }` — supports plain expressions and `nearest(...)`.
- `limit <integer>` — required when there is a `nearest(...)` ordering.
- **Total, deterministic order.** Rows with equal user-sort keys are broken by the bound entities' physical `id` columns (ascending) appended as a final tie-break, so the result is a *total* order — reproducible across runs, and `order … limit N` returns a deterministic top-N even when ties straddle the cutoff. Bound edges participate through their internal physical edge ID, including parallel edges; that ID is not exposed as a queryable edge property. (Aggregate results have no entity-ID columns; their group rows are already distinct on the projected group keys.) Search orderings participate in the same contract: `order { nearest(...) }` ranks by ascending vector distance and `bm25(...)` by descending relevance score, each with the same id tie-break — so rows with equal scores come out id-ordered, and the score, never any internal scan or traversal emission order, is what the row order means. Additional keys after the search function apply as secondary sorts between the score and the id tie-break. Aggregated queries are outside search ordering: group results are not score-ranked.
- **NULL placement** is *nulls-first ascending, nulls-last descending* (i.e. `nulls_first = !descending`): a NULL sorts as if smaller than any value.
- **Unordered `limit` is a subset, not a slice.** Without an `order` clause (and with no search ordering or aggregation), `limit N` means *any* N valid rows: the engine may stop a traversal as soon as N results exist, so which N you get is not specified and may change between releases or runs. A query that needs specific rows must say so with `order`. (`order … limit N` is unchanged: the deterministic top-N described above.)

Write statements (`insert` / `update` / `delete`) are documented on the
[mutations](../mutations/index.md) page.

## Traversal execution

Variable-length traversals (`Expand`) are executed one of two ways, chosen per-expand by a cost model over cheap graph-manifest counts (frontier size, edge count, source-vertex count, hops) plus index coverage: selective traversals (small frontier relative to the source set) resolve neighbors from the persisted `src`/`dst` BTREE (one indexed scan per hop); dense / deep / large-frontier traversals — or those whose BTREE coverage is degraded so a full scan would be paid per hop — use an in-memory CSR adjacency index. Both produce identical results. An undirected traversal reads both adjacency directions — the CSR arm walks the outgoing and incoming index (both are always built), the indexed arm probes both the `src` and `dst` BTREE per hop — under the same per-source dedup, so its cost is roughly twice the directional equivalent. The decision is **re-made at every hop with the observed frontier**: a traversal that dispatched to the indexed path while selective (a single-seed traversal always looks selective at hop 1) switches to CSR mid-flight when its frontier outgrows the indexed path — either by crossing the `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` ceiling (which therefore bounds execution, not just dispatch) or because the remaining work, projected from the observed per-hop growth, exceeds what the CSR path costs. (`OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` needs no mid-flight check: the hop count is known at dispatch and fully enforced there.) The switch carries the traversal's visited state; no work is repeated, and results are unaffected. An unordered trailing `limit` additionally bounds the traversal itself: emission stops once the limit is reached (see ORDER & LIMIT above for the subset semantics). `OMNIGRAPH_TRAVERSAL_MODE=indexed|csr` forces a mode and disables the per-hop re-decision (see [constants](../reference/constants.md)).

After `optimize` has run, the CSR adjacency is also **persisted** to the store (`__graph_index/` under the graph root) and loaded with one read instead of rebuilt by scanning the edge tables; the artifact is derived, verified for freshness and integrity at load, and any mismatch falls back to the in-memory build.

A traversal with an **edge binding** always takes a third path: a single-hop edge-dataset scan (the CSR index holds topology only, not edge properties), which carries the edge's declared property columns into the result and keeps parallel edges distinct. Edge-property filters are applied after the expand; pushdown into the edge scan is a planned optimization.

## Linting & validation

Codes seen so far:

- **Q000** (Error): parse error
- **L201** (Warning): nullable property never set by any UPDATE — "{type}.{prop} exists in schema but no update query sets it"
- (Warning): mutation declares no params — hardcoded mutations are easy to miss
- Plus all type errors from type checking (undefined types, mismatched operators, undefined edges, etc.)

Lint output reports an overall status, per-query results (name, kind, status, any error and warnings), and structured findings (severity, code, message, and the type/property/query they apply to).

With `omnigraph lint --json`, each successfully compiled per-query result also
contains an `operation` object with `result`, `reads`, and `writes`:

- `result` preserves projection order. Each field has `name`, `kind`, and
  `nullable`; list fields also have `item_kind`, and vector fields also have
  `vector_dim`. The structured `kind` vocabulary is `string`, `bool`, `int`,
  `bigint`, `float`, `date`, `datetime`, `blob`, `vector`, `list`, and
  `object`, so consumers never need to parse a display string. Mutation results
  are empty.
- `reads` and `writes` are deduplicated, deterministically sorted arrays of
  `{kind, type_name}`. `kind` is lowercase `node` or `edge`, and `type_name` is
  the case-sensitive declared graph type.

`reads` is conservative: it lists every graph type that the supported
execution path may inspect. For read queries this includes explicit node
bindings, each traversed edge and both of its endpoint node types, including
traversals nested inside `not { ... }`. Every mutation target is both read and
written, including an insert target; an edge insert also reads its endpoint node
types. Deleting a node can cascade to every incident edge type declared in the
schema, so those edge types appear in both `reads` and `writes`.

`writes` is the exact set of graph types that the mutation may change; it is
empty for a read query. No operation descriptor is emitted for a parse or type
checking failure.

CLI exits non-zero only on `status = Error`.
