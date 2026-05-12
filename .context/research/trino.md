# 3.6 Trino — code-dive

**Repo:** [`github.com/trinodb/trino`](https://github.com/trinodb/trino)
(Java, ~10k★).
**MR-925 §3.6 mapping:** §5.7 (cost-based optimizer at scale), §5.12
(distributed transaction patterns — informative, NOT prescriptive; per-table
branches are §5.12, not §5.11 which is "DataFusion vs custom executor"),
§5.4 + §5.6 (connector SPI as the plugin/capability surface — corrected on
re-read of MR-737; §5.10 is rank fusion, *not* connector SPI).

**§-numbering note:** original MR-925 cross-references in this file used
§5.10 (connector SPI) and §5.11 (per-table txn branches / distributed
transactions). On a full re-read of MR-737, §5.10 is "First-class scores
and rank fusion" and §5.11 is "Substrate choice — DataFusion vs. custom
executor (A)". The correct mapping for the plugin/capability surface is
§5.4 (Lance index plugin model) + §5.6 (capability-bearing storage trait);
the correct mapping for per-table txn branches is §5.12 (mutation IR with
per-table Lance native branches). Section headings below are updated.
**Date:** 2026-05-12.

---

## What we extracted

### Cost-based optimizer (CBO) — maps to MR-737 §5.7

Trino has a mature CBO that combines:
- **Histograms** (per-column, from ANALYZE).
- **Cardinality estimates** with selectivity propagation.
- **CPU + memory + network cost model** with weighted scoring.

The optimizer enumerates join orderings via a **dynamic-programming
approach (DP-sized; bushy trees) for ≤ 8 tables, then falls back to
greedy** for larger queries.

**Re-usable patterns for MR-737:**
- **Three-component cost model** (CPU, memory, network) is the right
  granularity. MR-737 §5.7 currently lists rows-processed + I/O; we
  should add a third component for memory (especially relevant for
  factorized intermediate representations that can blow up).
- **DP-for-small / greedy-for-large.** The 8-table threshold is a
  reasonable default. Most graph queries touch 2–5 tables (one
  node-table per variable + one edge-table per relationship), so
  DP works.
- **Histograms for selectivity.** Lance's manifest already carries
  per-segment min/max + null-count statistics; histograms would be a
  follow-up. For graph queries, the *out-degree distribution* is the
  most useful histogram (informs neighbor-expansion cardinality).
  **Add to §5.7 spec.**

### Connector SPI — maps to MR-737 §5.4 + §5.6

Trino's "Connector SPI" is a Java interface for adding new data
sources (Postgres, MySQL, Hive, Iceberg, …). The interface is **fully
public** — connectors are JARs that get registered at server startup.

**Re-usable patterns:**
- **Capability metadata** is reported per-connector via
  `ConnectorMetadata.getTableLayouts()`. Layouts advertise
  partitioning, sort order, and pushdown capability per-table. This
  is the same shape MR-737 §5.6 prescribes for OmniGraph's
  `TableProvider`.
- **Stable interface vs. mainline churn.** Trino's Connector SPI is
  the most-stable public Java surface in the project; breaking
  changes are versioned. Compare to DataFusion's
  `TableProvider` (less stable, breaks ~once per release). Trino is
  the reference for "what a properly-versioned plugin API looks like."

### Distributed transactions — maps to MR-737 §5.12 + §10 Open Q9 (informative)

Trino is a **stateless query coordinator**: it does NOT manage
transactions itself. Each connector's underlying system manages its
own transactions. Trino's "transaction" is just a session-scoped
identifier that lets multiple queries see the same snapshot of each
connected system.

**Why this matters:** Trino's approach is the opposite of MR-737
§5.12 (per-table Lance native txn branches with cross-table coordination).
Trino punts on the cross-system coordination problem; MR-737 doesn't.

**Re-usable pattern:** None directly. Trino is the **anti-reference**
— what NOT to do if you want cross-table ACID. MR-737's design is
strictly more ambitious.

### Plan rewriter framework — maps to MR-737 §5.7

Trino's optimizer is a stack of `Rule<T>` rewrites applied in
sequence (top-down + bottom-up passes). Each rule has a pattern
(matches a plan-tree shape) and a rewrite (produces a replacement).
This is the same pattern DataFusion uses with
`OptimizerRule` / `PhysicalOptimizerRule`.

**Re-usable patterns:**
- **Rule ordering matters.** Trino has hard-coded sequence of
  ~50 rules; ordering is significant. DataFusion's defaults are
  fine for SQL; we'll need to insert graph-specific rules
  (`PushPredicateIntoExpand`, `FactorizeIntermediate`, etc.) at
  specific positions.
- **Rule namespace.** Trino prefixes rules by area
  (`io.trino.sql.planner.iterative.rule.*`). DataFusion uses a flat
  namespace. **Cosmetic; defer.**

### Dynamic filters (Trino's analog of DF's DFP) — maps to MR-737 §5.6

Trino's **dynamic filtering** feature pushes build-side join filters
to probe-side scans. Implementation:
- Build side computes a Bloom filter (or InList for small sets).
- Coordinator broadcasts the filter to all probe-side workers.
- Probe-side scans apply the filter at the table-scan level.

This is structurally identical to DataFusion 52.5's
`DynamicFilterPhysicalExpr` (§1.5). **Validates the design.**

**Difference:** Trino uses Bloom filters by default; DF uses InList
(< 128MB) or HashTable (≥ 128MB). Our §1.4 finding (roaring beats
Bloom on bits/elem for clustered row IDs) suggests roaring would be
the right addition. Trino does NOT have roaring as a dynamic-filter
encoding; we'd be ahead of Trino on this dimension.

## Entry points to revisit

- `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/` —
  rule-based optimizer (40+ rules).
- `core/trino-spi/src/main/java/io/trino/spi/connector/` — Connector
  SPI (the model for our `TableProvider` surface).
- `core/trino-main/src/main/java/io/trino/cost/` — cost estimation.
- `core/trino-main/src/main/java/io/trino/operator/join/` — dynamic
  filtering implementation.

## What NOT to take from Trino

- **The Java language.** Different ecosystem; ports are awkward.
- **The coordinator-worker split.** Trino is a distributed system;
  OmniGraph is currently in-process (Phase 0–2). The distributed
  patterns become relevant only at Phase 3+.
- **The stateless-coordinator approach to transactions.** MR-737
  §5.12 explicitly wants stateful per-table txn branches.

## Decision impact on MR-737

- **§5.7 (cost model):** Add memory cost as third component;
  out-degree histograms as the most-useful additional statistic.
- **§5.4 + §5.6 (connector SPI / plugin / capability surface):** Trino is
  the gold standard for a stable, versioned plugin API. Cite when designing
  OmniGraph's custom-index-type registration spec (§5.4) and the storage
  trait capability surface (§5.6).
- **§5.12 (transactions):** Trino is anti-reference; MR-737 is
  strictly more ambitious. Document this.
- **§5.6 (dynamic filters):** Validates the §1.5 design; we'd be
  ahead of Trino with roaring-encoded dynamic filters.

## Open questions for follow-up

- **Q3.6.1 — Out-degree histogram representation.** Per-node-type
  histograms of out-degree by edge type are O(node-types ×
  edge-types) histograms. Storage cost? Defer; calibrate during
  §5.7 spec.
- **Q3.6.2 — DP-vs-greedy threshold.** Is 8 tables the right
  threshold for graph queries (which tend to have fewer tables but
  more joins per table)? Defer; calibrate during §5.7 spec.
