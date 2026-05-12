# Experiment 1.5 — Extending DataFusion dynamic-filter-pushdown to bitmap shape (code-dive)

**Ticket:** MR-925 §1.5 (validates MR-737 §5.6, §5.7 / Open Q3).
**Type:** Code-dive only (no prototype crate).
**Substrate pin:** DataFusion 52.5.
**Date:** 2026-05-12.

---

## Question

The dynamic-filter-pushdown (DFP) feature in DataFusion 52.5 ships with
three pushdown strategies for hash-join build sides:

  - `InList(ArrayRef)` — for small build sides (< 128 MB).
  - `HashTable(Arc<dyn JoinHashMapType>)` — for large build sides.
  - `Empty` — no rows, do not push.

Can a third-party operator (e.g. our `NeighborExpand` from §1.3, or
the broader graph-engine `BackJoin` and `NeighborSetIntersect` from
MR-737 §5.3) extend the same machinery to push a **roaring-bitmap-shaped
filter** through DF's dynamic filter framework — without forking DF?

## TL;DR

**Yes, completely supported on DataFusion 52.5 as written.** The
extension footprint is roughly 200 LoC: implement a custom
`PhysicalExpr` (e.g. `BitmapMembershipExpr`) and feed it to the
existing public `DynamicFilterPhysicalExpr::update(...)` API. No
fork, no `pub(crate)` work-around.

## Findings

### F1. DataFusion's DFP is expression-shaped, not enum-shaped.

The `PushdownStrategy` enum (`InList | HashTable | Empty`) is internal
to `joins::hash_join::shared_bounds` — it is the **HashJoinExec's own
internal switch** for selecting which physical-expr to construct for
*its* dynamic filter. The framework itself does not care:

```rust
// datafusion-physical-expr-52.5.0/src/expressions/dynamic_filters.rs
pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()>
```

The `update` method takes **any** `Arc<dyn PhysicalExpr>`. So an
operator outside the hash-join module is free to ship its own pushdown
strategy (e.g. `BitmapMembership`) and pass it to `update`.

### F2. Three things must hold for a custom dynamic-filter expr to work.

From reading `dynamic_filters.rs`:

1. **Stable children at construction time.** `DynamicFilterPhysicalExpr::new(children, initial_expr)`
   binds the column-leaves at construction. Subsequent `update`s may
   only swap the *expression*, not introduce *new column references*.
   For `BitmapMembershipExpr { column: Column::new("id"), bitmap_bytes: ... }`,
   the only child is `column` — stable.

2. **Self-contained `evaluate`.** The custom `PhysicalExpr` must
   implement `fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>`
   to return a `BooleanArray` of the same length as the input batch.
   For a roaring bitmap this is: deserialize once at first call, cache
   in `OnceCell`, then per-batch `for i in 0..n: out[i] = bitmap.contains(col[i])`.

3. **Be a `dyn PhysicalExpr`** — implement `Debug`, `Display`,
   `data_type`, `nullable`, `evaluate`, `children`, `with_new_children`,
   `dyn_hash`, `dyn_eq`. Standard boilerplate, mirroring `InListExpr`.

### F3. Two pushdown paths exist; only one needs work for graph operators.

DataFusion 52.5 has **two** filter-pushdown phases (see
`ExecutionPlan::gather_filters_for_pushdown` and
`ExecutionPlan::handle_child_pushdown_result` in `execution_plan.rs`):

  - **Static pushdown** (planning time): filters are pushed from
    `FilterExec` → `HashJoinExec` → `DataSourceExec` during the
    `EnforceFilterPushdown` physical optimizer rule.
  - **Dynamic pushdown** (execution time): a `DynamicFilterPhysicalExpr`
    placeholder is left in the plan at planning time; at runtime, the
    *producer* operator calls `update(new_expr)` once its data is
    available (e.g. once the hash-join build side is materialized).

For graph operators that produce SIPs (`NeighborExpand` build phase,
`SemiJoin` build phase, etc.), the **dynamic** path is the natural one.
The producer pattern is:

```rust
// At plan-construction time (in our ExtensionPlanner):
let placeholder = lit(true);
let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
    vec![Arc::new(Column::new("dst_id", 2))], // probe-side column refs
    placeholder,
));
// Wire dynamic_filter into both the probe-side scan (as a filter)
// and store an Arc<DynamicFilterPhysicalExpr> on our build-side operator.

// At execute time, once our build side completes:
let bitmap = build_roaring_from(build_ids)?;
let bitmap_bytes = serialize_to_vec(bitmap);
let pushdown_expr = Arc::new(BitmapMembershipExpr {
    column: probe_side_column_ref,
    bitmap_bytes,
});
self.dynamic_filter.update(pushdown_expr)?;
```

### F4. The scan-side has two interception points.

A custom dynamic filter ends up at the scan. Two paths exist for the
scan to consume it efficiently:

#### Path A. **Generic predicate evaluation** (works today, no DF fork).

The `BitmapMembershipExpr::evaluate(batch)` is called per batch. For
each batch row, `bitmap.contains(row.value)` is invoked. The roaring
crate's `contains` is O(log n) within a fragment-localized container
and was measured at <0.1 µs per call in §1.4. For a 1024-row batch,
this is ~100 µs of CPU, which is amortized against the I/O for that
batch. **This is enough for §5.6 as written.**

#### Path B. **Lance scan-level row-id mask** (faster, needs Lance integration).

Lance's `Scanner` supports a `RowIdMask` that is applied at the scan
level before any predicate evaluation. If our `BitmapMembershipExpr`
targets a Lance row-ID column, we *could* extract the bitmap during
the scan's `handle_child_pushdown_result` and convert it into a
`RowIdMask` — completely bypassing the per-row predicate. This is
the same trick Lance's full-text search uses today (see Lance's
`scalar.rs` `apply_full_text_search_index`).

Path B requires changes to Lance's `DataSourceExec` or our wrapping
adapter; Path A is **zero-change to DataFusion or Lance**.

### F5. The static-pushdown phase passes-through unrecognized exprs cleanly.

`FilterDescription::all_unsupported(parent_filters, &children)` is the
default for `gather_filters_for_pushdown`. Our custom `BitmapMembershipExpr`
is just an unrecognized expr — it will not be pushed past operators
that haven't opted into bitmap pushdown, and at the leaf `DataSourceExec`
it falls back to per-batch evaluation (Path A above). No silent
misbehavior, no crash, no need to teach DF about our expression shape.

### F6. The framework does NOT support N pushdown sources to the same scan.

A `DynamicFilterPhysicalExpr` wraps **one** inner expression at a time.
If two producers (e.g. `Expand(a)` and `Expand(b)`) both want to push
bitmap filters onto the same probe scan, each calls `update` on its
*own* dynamic filter; the scan must hold N separate dynamic filters
and AND them at evaluation time. The plumbing for this (multiple
`Arc<DynamicFilterPhysicalExpr>` on a scan) is standard `BinaryExpr(AND)`
wrapping. **No framework gap.**

## Concrete plan for §5.6 (RFC body delta)

The RFC §5.6 should specify:

1. **Bitmap-shaped SIPs are propagated via the standard `DynamicFilterPhysicalExpr` API.**
   No custom side-channel; reuse the framework. Producer calls `update(new_expr)`;
   scan evaluates the resulting `BooleanArray` per batch.

2. **A new public `BitmapMembershipExpr` lives in our graph crate** (not in
   the DF tree). It is constructed with a `Column` child and an opaque
   `Vec<u8>` payload (roaring serialized bytes). Implements
   `PhysicalExpr::evaluate` by deserializing the bitmap once into a
   `OnceCell<RoaringTreemap>` and probing it per row.

3. **Lance-aware scan adaptation is optional and incremental.**
   Path A (per-batch evaluation) is the v1 implementation. Path B
   (scan-level `RowIdMask`) is a v2 optimization that requires a
   `LanceDataSourceExec` to special-case `BitmapMembershipExpr` in its
   `handle_child_pushdown_result` impl. The RFC should call out Path B
   as a follow-up, not a blocker.

4. **N producers to one scan: AND-wrap.** The scan holds N
   `Arc<DynamicFilterPhysicalExpr>`. At plan-construction time, the
   probe filter wires them all in via `BinaryExpr::new(a, AND, BinaryExpr::new(b, AND, c))`.
   No bespoke "multi-source" data structure.

## What does NOT need a DataFusion fork

- **Custom dynamic-filter expression shapes.** Public via `Arc<dyn PhysicalExpr>` + `update`.
- **Custom dynamic-filter producers.** Public via `DynamicFilterPhysicalExpr::new`.
- **Custom dynamic-filter consumers.** All scans evaluate via the standard `evaluate` interface.
- **Composition with existing DFP (InList/HashTable).** Wrap with `BinaryExpr(AND)`.

## What WOULD need a DataFusion fork or upstream contribution

- **Bitmap-aware FilterPushdownPolicy.** If we want the static-pushdown
  pass to recognize `BitmapMembershipExpr` and route it specially (e.g.
  drop the InList variant when a bitmap is available), we'd need a
  `FilterPushdownPolicy` extension point that doesn't exist today.
  However, this is a *planner optimization*, not a correctness or
  capability issue. The plan still works without it.

- **A typed `BitmapPushdownStrategy` in `SharedBuildAccumulator`.** Only
  matters if we want graph-side BackJoins to share the HashJoinExec's
  build-side accumulator. We don't — graph operators have their own
  build phases.

## Decision impact on MR-737 §5.6 and §5.7

**§5.6 (SIP propagation) is achievable on DF 52.5 as written.** The
public `DynamicFilterPhysicalExpr::update` API is sufficient. No
upstream contribution required for v1.

**§5.7 (cross-operator filter sharing) is achievable as `AND`-wrapping
of N dynamic filters on the same scan.** No framework gap. The RFC
should clarify that the scan accumulates filters via standard
`BinaryExpr` composition, not via a bespoke multi-source channel.

**Open Q3 ("can we share the SIP filter between operator stages?")
— answered yes.** Confirmed by reading
`datafusion-physical-expr-52.5.0/src/expressions/dynamic_filters.rs:227`
(the `update` API) and `datafusion-physical-plan-52.5.0/src/joins/hash_join/shared_bounds.rs:463`
(the call site that proves the producer/consumer split works).

## Caveats and follow-ups

- **No prototype was built.** Per the ticket, §1.5 is a code-dive only.
  The recommendation rests on reading DF source, not on a working
  end-to-end implementation. If RFC §5.6 lands with this plan, Phase 0
  should include a smoke test that:
    1. Wires a `BitmapMembershipExpr` into a `DynamicFilterPhysicalExpr`.
    2. Runs a hash join with the bitmap as the dynamic filter.
    3. Compares row-output and timing against an `InListExpr`-shaped baseline.
  Estimated work: 1 day, no DF fork.

- **Lance scan-level `RowIdMask` support is the right v2 follow-up** —
  but is gated on the same plugin-registry blocker discussed in §1.2
  for custom *index types*. The `RowIdMask` path uses a different
  mechanism (it's not a scalar index), so it may not be blocked the
  same way. Worth a quick code-dive against `lance/src/index/scalar.rs`
  and `lance/src/dataset/scanner.rs` to confirm before committing.

- **DF 52.5 → 52.6 may rework parts of DFP.** The PR thread for
  `SharedBuildAccumulator` shows active churn; pin to 52.5.x for now
  and re-validate when bumping to a 52.6+ minor.
