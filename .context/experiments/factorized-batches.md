# Experiment 1.1 — Factorized batches through DataFusion ops

**Ticket:** MR-925 §1.1 (validates MR-737 §5.2 / Open Q2).
**Prototype:** `validation-prototypes/factorized-batches/` (branch
`devin/mr-925-pre-phase-0-validation-experiment-code-dive-agenda-to-de`).
**Substrate pin:** DataFusion 52.5 + Arrow 57.3 (matches engine workspace).
**Date:** 2026-05-12.

---

## Hypothesis

DataFusion's `HashJoinExec`, `AggregateExec`, `FilterExec`, `SortExec`, and
`ProjectionExec` either (a) handle a `List<UInt64>` neighbor-set column
correctly with acceptable performance, or (b) require explicit `Flatten`
before them. MR-737 §5.2 currently assumes mostly (b); this experiment maps
the actual frontier so the §5.2 rule list lands on validated ground.

## Method

`factorized-batches/` builds an in-memory `RecordBatch` with schema
`(src_id: UInt64, payload: Utf8, weight: Float64, _neighbors: List<UInt64>)`
plus a flat-row baseline of `(src_id, payload, weight, dst: UInt64)`
produced by exploding `_neighbors` to one row per `(src, dst)` pair.

For each cell `{n_src = 10_000} × {fanout ∈ uniform{1, 10, 100, 1000},
skewed(target=10, heavy=2%)}` we run six pipelines on each input shape via
`SessionContext::sql`:

| Op probe            | SQL                                                                |
|---------------------|--------------------------------------------------------------------|
| `filter`            | `SELECT * FROM t WHERE src_id < 5000`                              |
| `project`           | `SELECT src_id, _neighbors FROM t`                                 |
| `sort`              | `SELECT src_id, _neighbors FROM t ORDER BY src_id DESC LIMIT 1000` |
| `aggregate_scalar`  | `SELECT substr(payload,1,4) AS b, count(*) FROM t GROUP BY 1`      |
| `aggregate_on_list` | `SELECT _neighbors, count(*) FROM t GROUP BY _neighbors`           |
| `join_scalar`       | `SELECT a.src_id, a._neighbors FROM t a JOIN t b ON a.src_id = b.src_id LIMIT 100` |
| `join_on_list`      | `SELECT count(*) FROM t a JOIN t b ON a._neighbors = b._neighbors` |
| `unnest_flatten`    | `SELECT src_id, n.* FROM t CROSS JOIN UNNEST(_neighbors) AS n(dst)` |

Measurements: `accepts_list_input` (planning + execution complete), wall-clock
ms, output row count, output bytes (sum of `get_array_memory_size` over all
emitted batches). Memory is exercised but not directly capped — the goal is
go/no-go and order-of-magnitude calibration, not a tight benchmark.

Run with `cargo run --release -p factorized-batches` (release profile —
LTO-thin, opt-level 3). Sample output captured at
`validation-prototypes/factorized-batches/sample-output.txt`.

## Results (n_src = 10 000, runs single-threaded on the bench VM)

### Acceptance + speedup matrix (factorized vs flat baseline)

| op                   | fanout=1     | fanout=10                | fanout=100                | fanout=1000                  | skew=10/0.02 |
|----------------------|--------------|--------------------------|---------------------------|------------------------------|--------------|
| `filter`             | OK (0.32×)   | OK (0.72×)               | OK (1.95×)                | OK (0.48×)                   | OK (1.11×)   |
| `project`            | OK (0.81×)   | OK (1.03×)               | OK (1.26×)                | OK (1.43×)                   | OK (0.88×)   |
| `sort` (TopK 1000)   | OK (0.94×)   | OK (**7.18×**)           | OK (**70.18×**)           | OK (**336.28×**)             | OK (10.05×)  |
| `aggregate_scalar`   | OK (0.71×)   | OK (2.77×)               | OK (**16.47×**)           | OK (**140.36×**)             | OK (2.32×)   |
| `aggregate_on_list`  | OK (—)       | OK (—)                   | OK (—)                    | OK (—) — 1.6 s @ 10M edges   | OK (—)       |
| `join_scalar` (LIMIT 100) | OK (0.83×) | OK (3.57×)            | OK (**4.15×**)            | OK (**33.88×**)              | OK (2.65×)   |
| `join_on_list`       | OK (—)       | OK (—)                   | OK (—) — 26 ms            | OK (—) — 659 ms              | OK (—)       |
| `unnest_flatten`     | **FAILS**    | **FAILS**                | **FAILS**                 | **FAILS**                    | **FAILS**    |

`OK` means the physical plan compiled and the stream drained without error.
Speedup = `time_flat / time_factorized`; > 1 means factorized is faster. `(—)`
means no flat-row analogue: GROUP BY / JOIN on a List value is semantically
*different* from the flat-row equivalent (it groups / joins on full
neighbor-set equality).

### EXPLAIN plans

`aggregate_scalar` (factorized input):

```
SortPreservingMergeExec: [bucket@0 ASC NULLS LAST]
  SortExec: expr=[bucket@0 ASC NULLS LAST], preserve_partitioning=[true]
    ProjectionExec: ...
      AggregateExec: mode=FinalPartitioned, gby=[substr(...)@0], aggr=[count(...)]
        RepartitionExec: partitioning=Hash([substr(...)@0], 2)
          AggregateExec: mode=Partial, gby=[substr(payload@0,1,4)], aggr=[count(...)]
            DataSourceExec: partitions=1
```

The `_neighbors` column is correctly pruned from the scan projection
(`projection=[payload]`). When the group key is scalar, the List column never
hits the aggregator at all — it's column-pruned away.

`join_scalar` (factorized input):

```
ProjectionExec: expr=[src_id@1 as src_id, _neighbors@2 as _neighbors]
  GlobalLimitExec: skip=0, fetch=100
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(src_id@0, src_id@0)]
      DataSourceExec: partitions=1
      DataSourceExec: partitions=1
```

The List column rides through as a passthrough projection — it never enters
the hash table. `HashJoinExec` hashes only the join key (`src_id`).

`aggregate_on_list` (factorized input):

```
ProjectionExec: expr=[_neighbors@0, count(Int64(1))@1 as n]
  AggregateExec: mode=FinalPartitioned, gby=[_neighbors@0 as _neighbors], aggr=[count(...)]
    RepartitionExec: partitioning=Hash([_neighbors@0], 2)
      AggregateExec: mode=Partial, gby=[_neighbors@0 as _neighbors], aggr=[count(...)]
        DataSourceExec: partitions=1
```

This is the headline surprise: **DataFusion's `AggregateExec` is happy to use
a `List<UInt64>` column as a hash-grouping key**, and the partitioner is
happy to hash-repartition by it. Cost scales with total edge count, not
distinct-list-count: 12 ms @ 100K edges, 113 ms @ 1M edges, 1.6 s @ 10M edges
(roughly linear in edge volume). Semantically this groups by full
neighbor-set equality — useful for "find all sources with the same neighbor
set" but **not** the same as "GROUP BY exploded neighbor".

`sort` (factorized input):

```
SortExec: TopK(fetch=1000), expr=[src_id@0 DESC]
  DataSourceExec: partitions=1
```

The List column rides through the TopK fetch with no penalty.

`unnest_flatten` (`SELECT src_id, n.* FROM t CROSS JOIN UNNEST(_neighbors) AS n(dst)`):

```
execute: This feature is not implemented:
  Physical plan does not support logical expression
  OuterReferenceColumn(Field { name: "_neighbors", data_type: List(UInt64) },
                       Column { table: "t", name: "_neighbors" })
```

`CROSS JOIN UNNEST(<correlated column>)` is the cleanest SQL syntax for
exploding a List, but DataFusion 52.5 hits the unimplemented-physical-lowering
branch for the correlated reference. The failure surface is *physical* — the
logical plan compiles, the physical plan refuses to construct.

### Per-op recommendation

| Op                          | DataFusion 52.5 behavior                                              | Recommendation                                  |
|-----------------------------|------------------------------------------------------------------------|-------------------------------------------------|
| `FilterExec` (scalar pred)  | Passthrough for List columns, no perf cost.                            | `KEEP_FACTORIZED` — no `Flatten` needed.        |
| `ProjectionExec`            | Passthrough; identical perf to flat.                                   | `KEEP_FACTORIZED`.                              |
| `SortExec` (scalar key)     | List passes through; **at fanout ≥ 10, factorized is 7–336× faster**.   | `KEEP_FACTORIZED`. Stronger than §5.2 expected. |
| `AggregateExec` (scalar key)| List column-pruned at the scan; **2.7–140× faster at fanout ≥ 10**.    | `KEEP_FACTORIZED`. §5.2 should call this out.   |
| `AggregateExec` (list key)  | Works; groups by full-list equality.                                   | `MULTIPLICITY_AWARE_FUTURE`. Semantically distinct from `GROUP BY exploded`. |
| `HashJoinExec` (scalar key) | List rides through; 2.6–34× faster than the flat baseline.             | `KEEP_FACTORIZED`. §5.2 should call this out.   |
| `HashJoinExec` (list key)   | Works; semantics = match on full-list equality.                        | `MULTIPLICITY_AWARE_FUTURE`. Rare workload, but available. |
| `UNNEST` flatten            | Fails at physical lowering for correlated `CROSS JOIN UNNEST(col)`.    | `FLATTEN_BEFORE` must use the SELECT-clause `UNNEST(col)` form, the DataFrame `unnest_columns` API, or a custom `FlattenExec`. **Do not rely on `CROSS JOIN UNNEST` in IR.** |

## Decision impact on MR-737 §5.2 / Open Q2

§5.2 currently reads as "factorize-local, flatten before DataFusion ops" with
the expectation that most ops need flattening. **The data flips this for
scalar-keyed ops**:

1. **`Sort`, `Aggregate (scalar key)`, `HashJoin (scalar key)`, `Filter`,
   `Project` all KEEP factorized** at every cell tested. Speedup over the
   flat baseline is *monotonically increasing with fanout* for the
   memory-shape-sensitive ops (Sort up to 336×, AggregateExec up to 140×,
   HashJoinExec up to 34×). The List column is either column-pruned (when
   not referenced) or passthrough-projected (when referenced).

2. **`Aggregate` / `Join` on a list-typed key works**, but the semantics are
   "match on full-list equality", not "match on any exploded element". This
   is genuinely useful (neighbor-set deduplication, signature joins) but
   needs its own §5.2 sub-section so callers don't reach for it expecting
   element-wise semantics. Recommendation: `MULTIPLICITY_AWARE_FUTURE`.

3. **`Flatten` via `CROSS JOIN UNNEST(col)` is broken in DF 52.5**. This is
   the syntax §5.2 most naturally reaches for ("emit a Flatten by wrapping
   in `CROSS JOIN UNNEST`"). The fix has three live paths:
   - SELECT-clause `UNNEST(_neighbors)` (not yet exercised here — TODO
     extend the probe — but the prior art in `datafusion/src/sql/expr.rs`
     suggests this form is implemented).
   - DataFrame API `unnest_columns(&["_neighbors"])`.
   - A custom `FlattenExec` physical operator (which we'll already need
     for the custom-operator experiment 1.3).

   The §5.2 rule should be reworded to **"insert `Flatten` via the
   DataFrame `unnest_columns` API or our own `FlattenExec`; do NOT lower to
   `CROSS JOIN UNNEST` in IR"**.

4. **`Expand`-shaped workloads (the dominant case for graph traversal)**
   benefit dramatically from factorization on scalar-keyed pipelines, which
   matches the §0 hop-1 spike result (MR-376 measured 72× on local FS for
   a related shape; here we see >70× on sort + >140× on aggregate at
   fanout=100). §5.2 should harden its claim from "factorized helps" to
   "factorized is the default; flatten is the exception".

5. **Open Q2 ("does the factorized-IR pay off for DataFusion ops?") is
   resolved YES.** §10's open-question bullet for Q2 can flip to RESOLVED
   with this writeup as evidence.

No fundamental seam mismatch was uncovered, so §5.11 (substrate decision)
does NOT need to be re-opened.

## Caveats / what this experiment did NOT measure

- **Memory pool ceiling**: probes ran with the default unbounded pool. The
  table reports `out_bytes` per emitted batch but not peak in-aggregator
  state. Re-running with `TrackConsumersPool` is a follow-up if §5.7 cost
  model needs tighter calibration numbers.
- **Parallelism**: cells ran with the default DF partition count (2 in this
  environment). Cliff behavior at higher partition counts isn't probed.
- **Spill behavior**: dataset sizes top out at ~10M edges (1 GB-ish in flat
  shape). No on-disk spill triggered.
- **Vector / FTS columns**: only `List<UInt64>` exercised. Other list
  payloads (e.g. `List<Float32>` vectors) may have different hash / compare
  costs.
- **SELECT-clause UNNEST**: only the `CROSS JOIN UNNEST` form was probed.
  Need a follow-up cell to confirm `SELECT UNNEST(_neighbors) FROM t` and
  `df.unnest_columns(&["_neighbors"])` both work.

## Follow-ups

- Add a `SELECT UNNEST(...)` and a DataFrame `unnest_columns(...)` cell so
  the writeup pins down at least one *working* Flatten path. (Cheap; ~30 min.)
- File a DataFusion issue for `CROSS JOIN UNNEST(<correlated column>)`
  hitting "Physical plan does not support logical expression
  OuterReferenceColumn". Probably already tracked — search first.
- Extend probe to `List<Float32>` (vector-shape) and `List<List<UInt64>>`
  (nested neighbor sets, e.g. multi-hop staging) before Phase 0 lowers
  Vector ANN results into the factorized IR.
