# 3.2 LanceDB — code-dive

**Repo:** [`github.com/lancedb/lancedb`](https://github.com/lancedb/lancedb)
(OSS LanceDB, Rust + Python).
**MR-925 §3.2 mapping:** §5.6 (TableProvider integration), §5.4 (Lance index
plugin model — *not* §5.10, which is rank fusion; corrected on re-read of MR-737),
§5.12 (mutation-as-IR), §5.1 + §5.10 (vector search as
operator vs UDF).
**Date:** 2026-05-12.

---

## What we extracted

### TableProvider wrapping a Lance dataset — maps to MR-737 §5.6

LanceDB's `Table` type wraps a `lance::Dataset` and implements
DataFusion's `TableProvider` trait. Key files (Rust side):

- `rust/lancedb/src/query/datafusion_table.rs` — the actual
  `TableProvider` impl that hands DataFusion a scan plan.
- `rust/lancedb/src/connection.rs` — `Connection::open_table` returns
  the wrapper.

**Re-usable patterns:**
- The `TableProvider::scan(projection, filters, limit)` method
  routes through `Dataset::scan().filter(...).project(...).limit(...)`.
  Filter expressions are converted to Lance's
  `lance::dataset::scanner::Filter` and pushed at the scan level
  (using Lance's Substrait predicate machinery, which speaks DF expressions).
- LanceDB's filter-pushdown logic is identical to what we'd write for
  OmniGraph's `__node:Person.lance` → `TableProvider` adapter. Lift
  the conversion functions (`expr_to_lance_filter` or equivalent)
  rather than reimplementing.
- The `TableProvider` advertises capabilities via
  `supports_filter_pushdown`, returning `TableProviderFilterPushDown::Exact`
  for predicates Lance can fully handle and `::Inexact`/`::Unsupported`
  for the rest. This is the exact pattern §5.6 prescribes.

### Vector search as a query operator — maps to MR-737 §5.1 + §5.10

(§5.1 = unified IR with `VectorSearch` as `IROp`; §5.10 = first-class scores +
rank fusion; VectorSearch emits `_score`/`_rank` per §5.10.)

LanceDB's `Table::query(vector)` builds a `VectorQuery` and lowers it
to a Lance scan with a `nearest` filter. **At the DataFusion level**,
the vector-search call is just a `TableProvider::scan(filter=Nearest(...))`
— the operator surface is a regular filter.

**Re-usable pattern:** Vector search as a filter on the scan, not a
separate operator type. This matches MR-737 §5.1's approach (vector
ANN is a filter, not a top-level operator). LanceDB's
`NearestFilter` representation is the reference shape.

**Alternative:** LanceDB Enterprise (closed-source) reportedly has a
"segment-aware vector planning surface" per Ragnor 2026-05-02 (cited
in MR-925 §3.2). This is **not in the OSS LanceDB repo** (verified
2026-05-12). The segment-aware planning is a private extension.
**Decision: MR-737 §5.6 segment-aware capability needs to be
specified from scratch; we cannot copy from LanceDB OSS.**

### Mutation-as-IR — maps to MR-737 §5.12

LanceDB does NOT use `UserDefinedLogicalNodeCore` for writes — its
write path goes through `Table::add(...)`, `Table::update(...)`,
`Table::delete(...)`, etc., which are direct method calls that
bypass the DataFusion plan tree entirely.

**This is a difference from MR-737's design.** MR-737 §5.12 prescribes
that mutations are IR operators (Insert / Update / Delete / Merge)
that wrap read-shaped subplans, going through the same planner.
LanceDB takes the "imperative write API" approach instead.

**What this means for §5.12:**
- **No direct reference implementation in LanceDB.** The MR-737 §5.12
  design is more ambitious than what LanceDB does today.
- **Mutation IR is original work.** Look to DataFusion's
  `DmlExec`/`DmlNode` for the framework primitives.
- The MR-376 spike (`MutationStaging.pending`) is closer to the
  right shape than LanceDB's approach.

### TableProvider capability advertisement — maps to MR-737 §5.6

LanceDB's `TableProvider` advertises:
- `supports_filter_pushdown` per-filter via the standard DF API.
- `statistics` (returns sample-derived statistics from the manifest).
- `partitioning` (returns `Partitioning::UnknownPartitioning`).

**What's missing for graph workloads:**
- **No `Hash([key], N)` partitioning advertisement.** LanceDB doesn't
  know about hash-distributed scan layouts because Lance doesn't have
  hash-partitioned datasets.
- **No semi-mask / SIP acceptance advertisement.** LanceDB scans
  don't accept incoming SIPs from upstream operators.

**For MR-737:** The §5.6 partitioning and SIP-acceptance advertisements
are **OmniGraph-original additions**. LanceDB is a partial reference;
the rest must be designed fresh.

## Entry points to revisit

- `rust/lancedb/src/query.rs` — query DSL → DataFusion logical plan.
- `rust/lancedb/src/query/datafusion_table.rs` — `TableProvider` impl.
- `rust/lancedb/src/index/scalar.rs` / `rust/lancedb/src/index/vector.rs` —
  index lifecycle integrations.
- `rust/lancedb/src/connection.rs` — top-level handle, useful as the
  shape for our equivalent.

## What NOT to take from LanceDB

- **The imperative write API.** MR-737 §5.12 wants mutations to be
  IR operators; LanceDB does not.
- **The single-table assumption.** LanceDB exposes individual
  tables; OmniGraph coordinates many tables atomically. LanceDB's
  `Connection::open_table` is per-table; we have a `__manifest`-driven
  graph-wide view.
- **The Python-first API surface.** LanceDB's Rust crate is shaped
  to support a Python wrapper. We don't need Python; the Rust API
  can be plain ergonomic Rust.

## Decision impact on MR-737

- **§5.6 (TableProvider integration):** LanceDB's filter-pushdown
  conversion is directly reusable. Capability advertisement beyond
  filters (partitioning, SIP) is OmniGraph-original.
- **§5.4 (Lance index plugin model):** LanceDB integrates with Lance's
  built-in scalar/vector indices but does **not** add custom index
  types from outside the lance crate. Per Experiment 1.2, the plugin
  registry is `pub(crate)`. LanceDB is not a reference for solving
  this; it sidesteps it.
- **§5.12 (mutation-as-IR):** Mostly NOT reflected in LanceDB.
  MR-737's design is more ambitious; we have no production reference
  for it. **Surface this in the rollup comment as a §5.12 risk.**
- **§5.1 (vector search):** Validated. Treat vector search as a
  scan-level filter, not a top-level operator.

## Open questions for follow-up

- **Q3.2.1 — Segment-aware vector planning.** Does LanceDB Enterprise
  expose segment-aware planning hooks via a trait we could implement?
  Worth asking Lance team directly. Out of scope for the OSS code-dive.
- **Q3.2.2 — `supports_filter_pushdown` for graph predicates.** Our
  graph predicates (`(a)-[KNOWS]->(b) AND a.name = ...`) aren't a
  single DF filter expression — they're multi-table joins.
  LanceDB's pushdown is per-scan; we need cross-scan SIP. **No
  LanceDB precedent.**
