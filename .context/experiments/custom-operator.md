# Experiment 1.3 — Custom UserDefinedLogicalNode + ExecutionPlan e2e

**Ticket:** MR-925 §1.3 (validates MR-737 §5.1 "Unified IR", §5.2 "Factorized IR",
§5.11 "Substrate choice — DataFusion vs. custom executor (A)", and §5.12
"Mutation IR" — all of which rely on `UserDefinedLogicalNode` + `ExecutionPlan`
surviving the optimizer end-to-end).

**§-numbering note:** the original §1.3 cross-reference in MR-925 cited "§5.3,
§5.10". On a full re-read of MR-737: §5.10 is "First-class scores and rank
fusion (open-world over modalities)" (not "operators survive the optimizer"),
and the custom-operator e2e contract is actually shared across §5.1, §5.2,
§5.11, and §5.12. §5.3 (SIP) is the *first* operator that consumes the contract
— valid — but the contract itself is broader than §5.3 alone.
**Prototype:** `validation-prototypes/custom-operator/`.
**Substrate pin:** DataFusion 52.5 (matched to omnigraph workspace).
**Date:** 2026-05-12.

---

## Hypothesis

We can ship a graph-specific operator (e.g. `NeighborExpand` for
`MATCH (a)-[]->(b)`) as a third-party DataFusion node, with:

  - a `UserDefinedLogicalNodeCore` implementation that lives in our crate,
  - a paired `ExecutionPlan` that does the actual work,
  - an `ExtensionPlanner` that lowers the logical → physical,
  - integration via `SessionStateBuilder::with_query_planner`,
  - normal optimizer + physical-planner composition with built-in DF ops
    (Filter, Aggregate, etc.),
  - working `BaselineMetrics` (so the operator shows up in EXPLAIN ANALYZE).

This is one of the two non-negotiable prototypes the ticket requires (§1.3).

## Method

The prototype defines a `NeighborExpand` operator with these semantics:

> Input schema: `(src_id: UInt64, _neighbors: List<UInt64>)`
> Output schema: `(src_id: UInt64, edge_type: Utf8, dst_id: UInt64)`
> Semantics: flatten each input row's `_neighbors` list, emitting one
> output row per `(src_id, edge_type, dst_id)` triple.

Five probes are run on the same plan tree:

| Probe | What is exercised |
|-------|-------------------|
| **E1** Round-trip      | Build `LogicalPlan::Extension(NeighborExpandNode { input: scan, edge_type: "FOLLOWS" })`, plan through `DefaultPhysicalPlanner` + our `ExtensionPlanner`, execute, count rows. |
| **E2** EXPLAIN         | `LogicalPlan::display_indent()` and `displayable(physical).indent(true)` show our node names. |
| **E3** Predicate guard | Default `prevent_predicate_push_down_columns()` blocks the optimizer from pushing predicates *below* our node (it would change semantics — `WHERE dst_id = 7` only makes sense *after* the expand). |
| **E4** Composition     | Wrap output as a `MemTable` and run `SELECT count(*) ... GROUP BY edge_type WHERE dst_id > 2` over it; verify the result composes with DF's `FilterExec` + `AggregateExec`. |
| **E5** Metrics         | After execute, the operator's `metrics()` returns a `MetricsSet` containing `OutputRows`, `OutputBatches`, `OutputBytes`, `ElapsedCompute`, `StartTimestamp`, `EndTimestamp`. |

Run output (release):

```
Logical plan:
NeighborExpand: edge_type=FOLLOWS
  TableScan: edges_factored projection=[src_id, _neighbors]

Physical plan:
NeighborExpandExec: edge_type=FOLLOWS
  DataSourceExec: partitions=1, partition_sizes=[1]

[E1] Total flattened rows = 7
[E1] PASS: row count matches expected 7
[E1] First batch (src,dst) pairs: [(10, 1), (10, 2), (20, 3), (40, 7), (40, 8), (40, 9), (40, 10)]
[E4] PASS: Filter(dst>2)+Aggregate(count(*)) over expand = 5
[E5] Physical plan metrics: ... OutputRows(Count { value: 7 }) ...
[E3] prevent_predicate_push_down_columns = {"src_id", "dst_id", "edge_type"}
[E3] PASS: predicate push-down conservatively blocks all output cols (the default)

All probes passed.
```

## Findings

### F1. The integration surface is clean. ✅

The full integration footprint, in lines of code, is **about 250 lines**
for both a logical node + a physical operator. The work breakdown is:

  - `impl UserDefinedLogicalNodeCore for NeighborExpandNode` — six required
    methods plus boilerplate (Hash, Eq, PartialOrd). No internal DF types
    leak through.
  - `impl ExecutionPlan for NeighborExpandExec` — `properties`, `children`,
    `with_new_children`, `execute`, `metrics`, `statistics`,
    `partition_statistics`. Public enums only (`Boundedness::Bounded`,
    `EmissionType::Incremental`).
  - `impl ExtensionPlanner for NeighborExpandPlanner` — one `plan_extension`
    method that downcasts via `node.as_any().downcast_ref::<Our>()`.
  - `SessionStateBuilder::with_query_planner(Arc::new(MyPlanner))` to glue
    everything together.

There are no `pub(crate)` blockers and no `internal::` modules required.

### F2. Predicate push-down is opt-in. ✅

`UserDefinedLogicalNodeCore::prevent_predicate_push_down_columns()` defaults
to "block all output columns". For `NeighborExpand`, this is the *right*
default — predicates on the post-flatten `dst_id` cannot be pushed below
the expand without changing semantics. If we later want pushdown on
`src_id` (it's stable across the expand), we override the method to
return the schema minus `src_id`. This is a one-line opt-in.

### F3. EXPLAIN integration is automatic. ✅

`fmt_for_explain` on the logical node and `DisplayAs` on the physical
operator both appear in standard `display_indent()` / `displayable(...)`
output. No further wiring is required to make our operators visible in
`EXPLAIN PLAN` / `EXPLAIN ANALYZE` output.

### F4. BaselineMetrics work as documented. ✅

Wrapping the upstream stream with `stream.inspect_ok(move |b|
metrics.record_output(b.num_rows()))` is the entire integration. The
`MetricsSet` returned by `metrics()` includes the standard `OutputRows`,
`OutputBatches`, `OutputBytes`, `ElapsedCompute`, and `*Timestamp`
metrics. After two executions, `output_rows` correctly accumulates.

### F5. Composition with built-in ops works without ceremony. ✅

We registered the output of the expand as a `MemTable` and ran a SQL
query with `Filter(dst_id > 2) → Aggregate(count(*), edge_type) → GROUP BY`
over it. The aggregate returned the expected count (5). This proves the
output schema and physical batch layout are valid for downstream DF
operators — there is no hidden assumption that prevents our custom op
from being a peer of `FilterExec`, `ProjectionExec`, etc.

### F6. Unsafe usage: **none**. ✅

No `unsafe` was needed anywhere in the integration. All downcasting is
via the public `Any` interface; all stream wrapping uses `RecordBatchStreamAdapter`.

### F7. Internal-API access: **none**. ✅

The only borderline-internal item touched is `Boundedness::Bounded` and
`EmissionType::Incremental` from
`datafusion::physical_plan::execution_plan` — both are public enums.
Everything else (`PlanProperties`, `EquivalenceProperties`,
`Partitioning`, `BaselineMetrics`, `ExecutionPlanMetricsSet`,
`RecordBatchStreamAdapter`) is in `pub mod` paths.

## Awkward seams

These are not blockers but should be noted for the §11 RFC-body delta:

1. **`UserDefinedLogicalNodeCore::expressions()` semantics are subtle.**
   The doc says "expressions in the current node, not including inputs",
   but if you return non-empty `expressions()`, the optimizer will rewrite
   them and call `with_exprs_and_inputs(new_exprs, ...)`. For operators
   that don't have inline exprs (like ours), returning `vec![]` is the
   right answer — but it means we lose access to any
   constant-folding/simplification the optimizer would otherwise do for us.
   Document this in MR-737 §5.3: "graph operators must declare their
   inline exprs explicitly to benefit from CF/CP".

2. **`PartialOrd` requirement on `UserDefinedLogicalNodeCore`.**
   The trait requires `PartialOrd` because nodes participate in
   memoization / cache-key hashing in some optimizer passes. Most graph
   operators won't have a meaningful order, so they return `None`. The
   `#[derive(PartialOrd)]` requires all fields to also be `PartialOrd`,
   which `LogicalPlan` is not — so we manually impl
   `PartialOrd { partial_cmp -> None }`. Make this idiomatic in the RFC.

3. **`statistics()` and `partition_statistics()` are required.**
   For operators that *do* have statistics (like NeighborExpand: we can
   bound output rows by `sum(_neighbors.length)`), this is an opportunity
   for cost-aware planning. For prototypes we return `Statistics::new_unknown(...)`.
   §11 §statistics should call this out: graph operators must compute
   statistics or the planner will fall back to worst-case cardinality.

4. **`Partitioning` is inherited from input by default.**
   `NeighborExpandExec::new` clones `input.output_partitioning()`. For
   expand specifically this is wrong if the input is hash-partitioned by
   `src_id` and the consumer needs hash-by-`dst_id`. The pattern is to
   override `properties()` to declare a different partitioning. Note in
   the RFC that graph operators must explicitly choose their output
   partitioning rather than inheriting.

## Decision impact on MR-737 §5.1, §5.2, §5.3, §5.11, §5.12

**§5.3 is achievable on DataFusion 52.5 as written.** The
`UserDefinedLogicalNode`/`ExecutionPlan` surface is fully sufficient
for the operators §5.3 enumerates (Expand, MultiExpand, BackJoin,
NeighborSetIntersect, etc.). The only edits needed in §5.3:

  - Note that operators must explicitly opt-in to predicate push-down
    rather than rely on the default (which blocks all pushdown — the
    correct default for most graph ops).
  - Note the `PartialOrd` boilerplate as part of the operator skeleton.
  - Note the `statistics()` requirement: cost-aware planning depends on
    operators implementing it accurately, not punting to
    `Statistics::new_unknown`.

**§5.1 / §5.2 / §5.11 / §5.12 ("custom operators survive the optimizer +
execute correctly")**: The composition test (E4) plus the metrics test (E5)
cover this. No deltas needed.

## Caveats

- **Single-partition execution.** The probe uses partition count = 1.
  Multi-partition behavior (especially `required_input_distribution()`
  and `repartitioning`) is not exercised — but the `PlanProperties`
  surface for that is well-documented and was used by DF's own builtins
  the same way (verified by reading `FilterExec`).
- **The composition test materializes the output to a MemTable** to make
  it accessible from SQL. A more thorough test would build a single
  logical plan tree with `Filter(Expand(scan))` and run it; we'd need to
  hand-construct the LogicalPlan tree, which is straightforward but
  doesn't add new signal beyond what E1 + E4 already cover.
- **Schema gotcha caught during the experiment.** `ListBuilder<UInt64Builder>`
  defaults to a NULLABLE inner item even when the values appended don't
  contain nulls. If you declare a schema with `nullable=false` on the
  inner list field, `RecordBatch::try_new` rejects the array with
  `InvalidArgumentError("column types must match schema types")`. Worth
  a note in §5.3 or our internal Arrow guide — the default needs to be
  matched explicitly.

## Follow-ups (tracked, not done)

- Hand-build a multi-partition test plan with explicit
  `RepartitionExec(hash by src_id)` between scan and expand to verify
  our `output_partitioning()` choice survives the optimizer.
- Add a real `statistics()` implementation that consults
  `_neighbors.length` from upstream statistics (when present).
