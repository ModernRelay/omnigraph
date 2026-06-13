# DataFusion: current state + future improvements

**Audience:** contributors thinking about query-execution performance,
predicate pushdown, or planner work.
**Companion:** [lance.md](lance.md) for upstream Lance pages and version
state; [invariants.md](invariants.md) for the relevant deny-list items;
[execution.md](execution.md) for query-execution context.

This file tracks DataFusion-related improvements that have landed in
OmniGraph, the passive wins active by virtue of the current DF version,
and the structural changes still on the table. Update it whenever we ship
a DF-related change or a DF upstream release changes the picture.

**Current pin:** DataFusion 53.1.0 (workspace dep `datafusion = "53"`,
default-features = false, features = `["nested_expressions"]`). Pulled
in transitively by Lance 6.0.1; our direct touchpoints are narrow.
Lance 7.0.0 (stable, 2026-05-28) is now available upstream and **still**
pins `datafusion = "^53"` / `arrow = "^58"` — so the pending 6.0.1 → 7.0.0
bump is *not* a DataFusion bump and leaves everything under "Passive wins"
below unchanged. See **Upstream cadence**.

## Direct touchpoints in our code

We have only two direct DataFusion consumers; everything else is
transitive through Lance.

| Site | Role | State |
|---|---|---|
| `crates/omnigraph/src/exec/query.rs::build_lance_filter_expr` (and helpers `ir_filter_to_expr`, `ir_expr_to_expr`, `literal_to_expr`) | Lower typed IR filters to a DataFusion `Expr` and apply via `Scanner::filter_expr` | **Structured** (PR #113) |
| `crates/omnigraph/src/table_store.rs::scan_pending_batches` | Run SQL against an in-memory `MemTable` registered in a fresh `SessionContext` to filter the in-flight `MutationStaging.pending` batches | String SQL — small enough that the migration cost outweighs the benefit; out of scope |

We have **no custom `impl ExecutionPlan`**, no exhaustive `match` on
`ScalarValue`, no direct `tantivy`/`tokenizer` imports. Three classes of
DF 53 breaking changes therefore do not reach us:
`Arc<PlanProperties>` wrapping (#19893), removed `ExecutionPlan::statistics()`
(#20319), the `ScalarValue::RunEndEncoded` variant (#19895).

## Shipped

| | Where | What it bought us |
|---|---|---|
| DataFusion 52 → 53 bump | PR #111 | Substrate baseline. Every passive DF 53 optimizer/perf win activated automatically. |
| `nested_expressions` feature enabled | PR #113 | Made `datafusion::functions_nested::expr_fn::array_has` (and the rest of the nested-type expr-fns) reachable from our code. |
| `execute_node_scan` → `Scanner::filter_expr(Expr)` | PR #113 | Killed string-flattened pushdown on the bulk of the read path. **`CompOp::Contains` now pushes down** (via `array_has`) — previously returned `None` from `ir_filter_to_sql` and fell through to in-memory post-scan filtering. DF 53 optimizer rules now act on our predicates instead of being short-circuited by the string SQL detour. |

## Passive wins active on DF 53

These activated automatically when PR #111 landed. They apply to any
predicate / plan that reaches DataFusion (now including our
`execute_node_scan` filters via the structured Expr path):

| DF PR | Win | Where it bites us |
|---|---|---|
| [#20528](https://github.com/apache/datafusion/pull/20528) | Vectorized `IN`-list eq kernel | `id IN (…)` predicates in cascade-delete (`exec/merge.rs:1016`) and the structured Expr path |
| [#20111](https://github.com/apache/datafusion/pull/20111) | `PhysicalExprSimplifier` constant-folds before exec | All predicates handed to Lance via `Scanner::filter_expr` |
| [#20097](https://github.com/apache/datafusion/pull/20097) | `CASE WHEN x THEN y ELSE NULL` shortcut | Any generated CASE expressions in our predicates |
| [#20228](https://github.com/apache/datafusion/pull/20228) | Push limit into hash join | Anti-join (`not { … }`) lowered to `JoinType::LeftAnti` with a query-level `LIMIT N` |
| [#19918](https://github.com/apache/datafusion/pull/19918) | `HashJoinExec::try_new` `null_aware` flag | Correct `NOT IN` semantics when our anti-join involves nullable columns |
| [#19625](https://github.com/apache/datafusion/pull/19625) | Optimize `Nullstate` / accumulators | `count` / `sum` / `avg` / `min` / `max` aggregations |
| [#19910](https://github.com/apache/datafusion/pull/19910) | Misc hash / hash-aggregation perf | Same — hash-aggregation hot paths |

## Still on the table

Ranked by leverage. Update when one ships.

### Tier 1 — structural, unblocked today

| Item | Effort | Notes |
|---|---|---|
| **`hydrate_nodes` (Expand-time pushdown) → `Expr`** | Medium (~2 days) | The Expand pipeline (`exec/query.rs::hydrate_nodes`) still serializes through its `extra_filter_sql: Option<&str>` parameter. Migrating it pushes structured pushdown into `TableStorage::scan_stream(filter: Option<&str>)` → `Option<Expr>`, which cascades through 6+ call sites (`scan_stream_with`, `count_rows`, `count_rows_with_staged`). Largest remaining tech-debt slice on the structured-Expr refactor. |

### Tier 2 — upstream-unblocked; gated on our Lance 6→7 bump

| Item | Trigger | Notes |
|---|---|---|
| **Mutation delete predicate → `Expr`** via `DeleteBuilder::execute_uncommitted` (Lance [#6658](https://github.com/lance-format/lance/issues/6658)) | Our 6.0.1 → 7.0.0 bump | **Upstream gate now satisfied:** the API shipped in `v7.0.0-beta.10` and is in Lance **7.0.0 stable** (2026-05-28). The only remaining gate is the repo's own Lance bump (still pinned 6.0.1). Couples with **MR-A** (delete two-phase migration — tracked at [issue #112](https://github.com/ModernRelay/omnigraph/issues/112)). The DF Expr move at this site is half the work; the rest is retiring the parse-time D₂ rule and extending recovery sidecar coverage. |
| **`DeleteBuilder::from_expr(...)`** (Lance #6343, v5.0) | Same | The structured Expr variant of the inline delete path. Useful only while the inline `delete_where` residual still exists; supplanted by the staged form above once MR-A lands. |

### Tier 3 — future-shape (require owning more of the planner)

| Item | DF PR | Notes |
|---|---|---|
| Extension planner for `TableScan` | [#20548](https://github.com/apache/datafusion/pull/20548) | Would let us plug a custom planner converting `IROp::NodeScan` directly into a DF logical plan, bypassing the Lance string-SQL detour entirely. Big change. Only worth it if we own more of the physical plan. |
| `ExpressionPlacement` enum | [#20065](https://github.com/apache/datafusion/pull/20065) | Optimizer-hint enum letting the planner decide whether an expression evaluates in scan / filter / projection. Relevant only if we own optimizer rules. We don't. |
| `ExtractLeafExpressions` optimizer rule for `get_field` pushdown | [#20117](https://github.com/apache/datafusion/pull/20117) | Applies automatically when we use struct projections in pushdown. We don't generate them today. |
| `feat: support Set Comparison Subquery` | [#19109](https://github.com/apache/datafusion/pull/19109) | New subquery shape. Not relevant today — we don't lower to subqueries. |
| `OuterReferenceColumn` non-adjacent outer relations | [#19930](https://github.com/apache/datafusion/pull/19930) | Grandparent-scope subqueries. Future-shape unlock. |
| `AggregateMode::PartialReduce` (tree-reduce aggregation) | [#20019](https://github.com/apache/datafusion/pull/20019) | Aggregation perf opt for very wide partitions. Could opt in; modest gain. |
| `Pushdown filters through UnionExec` | [#20145](https://github.com/apache/datafusion/pull/20145) | Applies automatically when planning multi-fragment unions. Future-shape for graph traversal. |

### Tier 4 — won't reach us without major changes

| Item | DF PR | Why it doesn't bite |
|---|---|---|
| `Wrap immutable plan parts into Arc` | #19893 | We have no custom `impl ExecutionPlan`. |
| `Cache PlanProperties, fast-path with_new_children` | #19792 | Same. |
| `Remove the statistics() api` | #20319 | Same. |
| `feat: support limited deletion` | #20137 | DF-native LIMIT-on-DELETE. Lance owns delete in our stack; orthogonal. |

## Upstream cadence

We don't choose our DataFusion version directly — Lance does. Lance 6.0.1
pins DF `^53`. Lance **7.0.0** (stable, 2026-05-28) **also** pins
`datafusion = "^53"` / `arrow = "^58"` — confirmed against the published
7.0.0 dependency manifest. So the 6.0.1 → 7.0.0 bump carries DataFusion
forward unchanged: nothing under "Passive wins" moves, and the only
DF-doc delta from that bump is the Tier 2 delete-`Expr` item un-gating
(above). A DF 54 / 55 jump will arrive with a **later** Lance (8.x or
beyond); when it does, refresh this doc with a new "Passive wins" row and
a fresh upgrade audit.

DataFusion 54.0.0 has shipped upstream (per the upgrade-guide index) but
is **not** in our stack — Lance has not picked it up as of 7.0.0. Treat
anything in 54 as a heads-up only, and verify Lance's DF pin before
acting; right now there's no urgency.

## Maintenance

- Bump the **Current pin** stanza on every Lance bump (Lance dictates
  the DF version).
- When a Tier 1 / Tier 2 item ships, move it to **Shipped** with a PR
  link and a one-line description of what it bought.
- When DF ships a new major, add a row to **Passive wins active on DF
  N** with a one-line note. Track Tier 3 / Tier 4 items only if a
  follow-up MR mentions them or invalidates the categorization.
- Don't track every passive perf improvement — only the ones that
  measurably touch our query/exec/mutation paths.
- This doc is a snapshot; for in-depth Lance-side context see
  [lance.md](lance.md) and its periodic alignment audits.
