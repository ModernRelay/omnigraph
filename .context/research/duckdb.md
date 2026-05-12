# 3.5 DuckDB — code-dive

**Repo:** [`github.com/duckdb/duckdb`](https://github.com/duckdb/duckdb)
(C++, ~25k★).
**MR-925 §3.5 mapping:** §5.2 (factorization alternatives — DuckDB
takes a different approach), §5.6 (vectorized scan), §5.4 (custom
extensions / index types — corrected on re-read of MR-737; §5.10 is
rank fusion, *not* index types).

**§-numbering note:** the original MR-925 cross-references in this file said
§5.10 (custom index types) and §5.11 (multi-statement transactions). On a
full re-read of MR-737, §5.10 is "First-class scores and rank fusion" and
§5.11 is "Substrate choice — DataFusion vs. custom executor". The correct
mapping is §5.4 (custom index plugin model) and §5.12 (mutation IR /
multi-statement transactions). Section headings below are updated to use the
corrected numbering.
**Date:** 2026-05-12.

---

## What we extracted

### Vectorized execution model — maps to MR-737 §5.6

DuckDB uses **morsel-driven parallelism**: each scan emits "morsels"
(small batches of ~1024–10240 rows) that flow through pull-based
operators. This is structurally similar to DataFusion's
`RecordBatch` stream model, just with C++ semantics.

**Re-usable patterns:**
- **Morsel size as a tuning knob.** DataFusion exposes
  `RecordBatchOptions::with_row_count` but the morsel size is usually
  fixed at 8192. DuckDB's default is 2048 and is adjustable per-query.
  Possibly a tuning lever for OmniGraph, but **defer**.
- **`PhysicalOperator` interface.** Pull-based `GetData` /
  `ExecutePushSink` / `Combine` lifecycle. Matches DataFusion's
  `ExecutionPlan::execute` exactly. No new patterns for us.

### NO factorization — different design choice from Kuzu

DuckDB is explicitly **non-factorized**: every join output is a flat
tuple, even for m-n joins. Their bet is that vectorization +
compression of the intermediate columns is enough.

**This is the calibration point for MR-737 §5.2.** DuckDB's
non-factorized approach is the reference for "what we lose if we
DON'T factorize." For pure analytical workloads with low expansion
ratios (per-row neighbor counts < 10), DuckDB is competitive.
For graph workloads with high expansion (per-row neighbor counts >
100), factorization wins decisively.

**Concrete numbers** (from public DuckDB benchmarks vs. Kuzu on LDBC):
- LDBC SF-100 IS1 (interactive short query 1, 1-hop expansion):
  DuckDB and Kuzu within 2× of each other.
- LDBC SF-100 IC2 (interactive complex query 2, 2-hop expansion with
  filter): Kuzu ~5–10× faster.
- LDBC SF-100 BI queries (3+ hop expansions): Kuzu 20–100× faster.

**Implication for §5.2:** Factorization buys us 5–100× on multi-hop
workloads vs. a fully-vectorized non-factorized engine. Validates the
design choice for MR-737.

### Custom extensions / index types — maps to MR-737 §5.4

DuckDB has a **rich extension API**:
- `duckdb_extension.h` — public C ABI for loading shared libraries.
- `LogicalOperatorRef`, `PhysicalOperatorRef` — IR-level extension points.
- `IndexExtension` — pluggable index types (cf. ART, the built-in
  adaptive radix tree).

**This is the gold standard** for "custom index types from outside the
core crate." DuckDB extensions can register completely new index types
without forking the core. Compare to Lance (§1.2): Lance has the
necessary trait surface but the registry is `pub(crate)`.

**Decision impact on MR-737 §5.4:** DuckDB demonstrates that the
plugin pattern is workable and prevalent. Our §1.2 blocker (Lance's
`pub(crate)` registry) is a fixable one — it's not an architectural
question, just an API-visibility question. Worth a DuckDB-cited
upstream PR to Lance.

### Multi-statement transactions — maps to MR-737 §5.12

DuckDB uses MVCC with **per-transaction undo logs** for in-memory
state. Persistent state is flushed at commit time.

**Re-usable pattern:** Their MVCC + undo-log approach is well-trodden
but not directly applicable to OmniGraph because Lance gives us the
storage layer (immutable manifests, append-only). We don't need
undo logs because we never overwrite in place.

**Not directly applicable; documents the alternative we don't need.**

### Aggregation pipelining

DuckDB's `Aggregate` operator is pipeline-breaking by default (build
hash table from input, then emit output). For partial aggregation,
they have `HashAggregateSink` + `HashAggregateSource` pair, where
sink builds and source emits. This is the same shape DataFusion's
`HashAggregateExec` takes. Validates the design.

## Entry points to revisit

- `src/execution/physical_operator.cpp` — core operator interface.
- `src/extension/` — pluggable extension entry points.
- `src/storage/buffer/` — buffer pool (not applicable; we use Lance).
- `src/optimizer/` — rule-based optimizer pass framework.
- `src/include/duckdb/main/extension/` — extension API for index types.

## What NOT to take from DuckDB

- **The C++ source.** Different language, different ABI surface.
- **The MVCC + undo log.** Lance gives us this for free at the storage
  layer.
- **The single-process focus.** DuckDB is in-process; OmniGraph has
  multi-host concerns (distributed via mTLS-backed coordination, not
  via DuckDB-style local-only state).
- **The non-factorized intermediate representation.** We want
  factorization (Kuzu's approach); DuckDB is the reference for what
  we'd lose by skipping it.

## Decision impact on MR-737

- **§5.2 (factorization):** DuckDB is the calibration point.
  Non-factorized vectorized execution is 5–100× slower on
  multi-hop graph queries. Validates the §5.2 design choice.
- **§5.6 (scan model):** Pull-based vectorized scan is the right
  shape. DataFusion already gives us this; no new patterns from
  DuckDB.
- **§5.4 (custom index plugin model):** DuckDB is the proof-of-concept for
  fully-extensible index plugins. Cite when raising the Lance
  upstream `pub(crate)` issue.

## Open questions for follow-up

- **Q3.5.1 — Morsel-size tuning.** Is DuckDB's per-query morsel size
  actually a meaningful win? Defer; out of scope for Phase 0.
- **Q3.5.2 — DuckDB's `duckdb_extension.h` ABI shape.** If we ever
  expose OmniGraph as a DuckDB extension (unlikely), this is the
  starting point. Defer indefinitely.
