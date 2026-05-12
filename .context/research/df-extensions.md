# 3.4 Production DataFusion extension projects — code-dive

**Repos:**
- [`github.com/apache/datafusion-comet`](https://github.com/apache/datafusion-comet)
  (Comet — DF as Spark SQL executor)
- [`github.com/GlareDB/glaredb`](https://github.com/GlareDB/glaredb)
  (GlareDB — federated query engine)
- [`github.com/paradedb/paradedb`](https://github.com/paradedb/paradedb)
  (ParadeDB — Postgres + DF for search/analytics)
- [`github.com/spiceai/spiceai`](https://github.com/spiceai/spiceai)
  (Spice.ai — federated edge query)

**MR-925 §3.4 mapping:** §5.6 (TableProvider capabilities), §11
(DataFusion API churn risk).
**Date:** 2026-05-12.

---

## Comet — DataFusion as Spark executor

**Maps to:** §5.6 ExecutionPlan integration patterns.

Comet replaces Spark SQL's execution engine with DataFusion's
ExecutionPlan tree. The interesting bit for us is how Comet
**bidirectionally translates plans**: Spark logical plan → DF plan
on the way in, DF results → Spark RDD batches on the way out.

**Re-usable patterns:**
- **`CometExec` wrapper.** A `dyn ExecutionPlan` impl that lives
  inside Spark's executor but holds a child `dyn ExecutionPlan` from
  DataFusion. Translates partitions, metrics, and cancellation.
- **`CometScanExec`.** Custom scan that reads Parquet via DataFusion
  but reports back to Spark's `SQLMetrics`. This is the pattern for
  OmniGraph if we ever embed in a non-DF runtime — not currently
  needed.

**Capability advertisement:**
Comet's `CometScanExec` declares partitioning via `Partitioning::Hash`
when the underlying Spark `RDD` was hash-partitioned. **This is the
direct reference for MR-737 §5.6's `Hash([key], N)` advertisement.**
Look at Comet's `partitioning()` impl on `CometScanExec`.

**API churn observed:**
Comet has bumped DataFusion every release (47 → 49 → 51 → 52 cycle).
Each bump touched ~5–10 files in `comet/native/core/src/execution/`
(low-touch surface, well-abstracted). **Suggests DF API churn is
manageable for us as long as we keep our DF surface narrow.**

## GlareDB — federated query engine

**Maps to:** §5.6 federated TableProvider patterns, §5.7 distributed
cost model.

GlareDB federates queries across remote databases (Postgres, MySQL,
Snowflake, Iceberg, Delta, …) using DataFusion as the planning
engine and pushdown optimizer.

**Re-usable patterns:**
- **Remote `TableProvider` impls** for each backend. Each impl knows
  what predicates the remote system can handle natively and reports
  them via `supports_filter_pushdown`. Same shape OmniGraph needs
  for Lance scans.
- **Cost-model annotations** on remote scans. GlareDB attaches
  cardinality estimates to remote scan results via the
  `Statistics` trait. This is the same machinery MR-737 §5.7 will
  use for OmniGraph's segment-level statistics.

**API churn observed:**
GlareDB has historically lagged DataFusion releases by 1–2 minor
versions. Their `external/` crate (where backend-specific
TableProviders live) absorbs most of the churn. **Reinforces the
"narrow DF surface" recommendation.**

## ParadeDB — Postgres + DataFusion for search

**Maps to:** §5.10 custom index types, §5.6 mixing engines.

ParadeDB embeds DataFusion inside Postgres as a query executor for
analytics workloads. They have a custom `pg_search` extension that
adds Tantivy-backed BM25 indices to Postgres tables.

**Re-usable patterns:**
- **`pg_search`'s index registration pattern.** Postgres has a
  Pluggable index AM (access method) API — Tantivy index types are
  registered via this API and queried via `WHERE col @@@ 'query'`.
  This is the direct analog of Lance's `pub(crate)` plugin registry
  blocker from §1.2. Postgres got the registry right (it's
  fully extensible from outside); Lance hasn't.
- **Splitting plan stages between engines.** ParadeDB pushes filter
  evaluation to Postgres native scans, then hands intermediate
  results to DataFusion for aggregation. **Not directly applicable
  to OmniGraph** (we use Lance everywhere), but it's a reference for
  how to build a clean two-engine boundary if we ever need to.

**API churn observed:**
ParadeDB pins DF more conservatively (release-cadence behind GlareDB).
Their DF surface is concentrated in
`pg_analytics/src/datafusion/`. Bumps usually touch ~20 files.

## Spice.ai — federated edge query

**Maps to:** §5.6 ExecutionPlan extension surface, §11 churn audit.

Spice.ai is "DataFusion on the edge" with a focus on low-latency
data federation. Uses DataFusion + Apache Arrow Flight as the
underlying transport.

**Re-usable patterns:**
- **Flight-Sql backends.** Their `flightsql` data connector wraps
  remote Arrow Flight servers as DF `TableProvider`s. Not applicable
  to OmniGraph today (we don't talk Flight).
- **Caching layer**. Spice.ai has a notion of "accelerated tables"
  (in-memory cache with TTL) that wraps a slow `TableProvider`. Not
  directly applicable but a clean reference for caching layer
  injection if we ever need it.

**API churn observed:**
Spice.ai stays close to DF main (typically within 1 minor). Their
DF surface is in `crates/runtime/src/` and `crates/sql/src/`. Bumps
are small (10–30 lines per release).

## Summary: DataFusion API churn audit (§2.4 deferred, partial signal here)

| Project   | Pin lag       | DF surface size | Per-bump touch |
|-----------|---------------|-----------------|----------------|
| Comet     | 0 minor       | ~30 files       | 5–10 files     |
| GlareDB   | 1–2 minor     | ~100 files      | 20–40 files    |
| ParadeDB  | 1–2 minor     | ~50 files       | ~20 files      |
| Spice.ai  | 0–1 minor     | ~80 files       | 10–30 files    |

**Implication for MR-737 §11:**
- Keep our DataFusion surface area narrow — concentrate
  `dyn ExecutionPlan` and `UserDefinedLogicalNode` impls in one or
  two crates that maintainers know to update on bump.
- Budget 1–2 days of maintenance per DF minor release. Comet (0 minor
  lag, 5–10 files per bump) is the cheapest reference; we should aim
  for that ceiling.

## What we don't take

- **None of these projects implement graph semantics.** No
  factorization, no SIP, no neighbor-expand. They're all flat-table
  systems with column predicates.
- **None of these projects integrate with Lance** in the way OmniGraph
  does. They're all Parquet/Iceberg/Delta-shaped.
- **None of these projects expose a graph IR.** They're SQL-only.

The value here is purely operational: how to keep a DataFusion
integration alive across version bumps, and what capability-advertisement
patterns work.

## Decision impact on MR-737

- **§5.6:** Comet's `Partitioning::Hash` advertisement on
  `CometScanExec` is the direct reference for OmniGraph's
  Hash-partitioned scan capability.
- **§11:** Budget 1–2 days per DF minor release for bump maintenance.
  Concentrate DF surface in 1–2 crates.

## Open questions for follow-up

- **Q3.4.1 — Can we contribute the "plugin registry for index types"
  upstream to Lance**, citing Postgres's `CREATE ACCESS METHOD` and
  ParadeDB's `pg_search` as precedents? This is the unlock for §1.2.
  Worth raising on the Lance issue tracker.
- **Q3.4.2 — Comet's `SQLMetrics` translation.** If we ever want
  cross-engine metrics aggregation (rare), Comet has the pattern.
  Defer indefinitely.
