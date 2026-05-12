# 3.1 Kuzu / LadybugDB — code-dive

**Repos:** [`github.com/kuzudb/kuzu`](https://github.com/kuzudb/kuzu) (active, ~4k★, C++);
LadybugDB at `github.com/LadybugDB/ladybug` (small fork).
**MR-925 §3.1 mapping:** §5.2 (factorization), §5.3 (SIP / semi-mask),
§5.12 / Open Q10 (mutation visibility), §5.13 (variable-length expansion).
**Date:** 2026-05-12.

---

## What we extracted

### Factorization (Kuzu's signature contribution) — maps to MR-737 §5.2

Kuzu's factorized query processor passes **factorized vectors** between
operators. A factorized vector is a tuple shape where one or more
columns carry per-source-row *lists* of values rather than per-row
scalars — exactly the shape we validated in our Experiment 1.1
factorized batches (List<UInt64> neighbor-set column).

Reference: [Kuzu blog post on factorization](https://blog.kuzudb.com/post/factorization/),
[CIDR 2023 paper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf).

The three design goals Kuzu's processor achieves simultaneously:

1. **Factorize intermediate results.** Don't flatten until necessary.
2. **Always perform sequential scans.** No random-access reads on hot path.
3. **Avoid scanning large chunks when possible.** Use SIP to prune scans.

**Re-usable patterns:**
- `FactorizedTable` / `DataChunk` representation — a multi-vector tuple
  where some vectors carry list values. Maps directly to our
  `List<UInt64>` neighbor-set column from §1.1.
- `Flat`/`Unflat` operator pair — flatten only at materialization /
  aggregation; keep factorized through expansion chains. Maps to our
  `FlattenList` operator in §1.1.
- Factorization-aware optimizer rules (e.g. "push Filter past Flatten
  if Filter only references the row-key column"). These should be
  ported to OmniGraph's planner as `FilterPastFlatten`,
  `ProjectionPastFlatten`.

### Semi-mask / SIP — maps to MR-737 §5.3, §5.6

Kuzu PR [#3651](https://github.com/kuzudb/kuzu/pull/3651) ("Enable semi
mask") and PR [#4050](https://github.com/kuzudb/kuzu/pull/4050) ("Fix
semi mask on scan node table") show the semi-mask wiring after a
recent refactor. The semi-mask is a roaring-bitmap-like structure
attached to a scan operator that says "only these row IDs are reachable
from the upstream join."

**Re-usable patterns:**
- Semi-mask is **set lazily** during the build phase of a HashJoin and
  **consumed eagerly** by the probe-side scan. This is the exact pattern
  we proposed in §1.5 for DataFusion `DynamicFilterPhysicalExpr`.
- Semi-mask **composes with predicate pushdown** — a scan that already
  has a Filter applied can additionally accept a semi-mask, and they
  AND together. Our §1.5 recommendation (BitmapMembershipExpr +
  BinaryExpr(AND)) is the same shape.
- Kuzu PR [#4460](https://github.com/kuzudb/kuzu/pull/4460) adds
  filter pushdown *into Extend* (their variable-length expansion
  operator) — informs our §5.13 approach where Filters must travel
  into the expansion subplan.

### Mutation visibility — maps to MR-737 §5.12, Open Q10

Kuzu uses a **dual-level hash index**:

- **Uncommitted-local layer** — in-memory hash maps that hold
  newly inserted/updated rows for the active transaction.
- **Committed-disk layer** — on-disk index that holds committed state.

Lookups consult both layers; the local layer shadows the disk layer.
This is the cleanest approach to "read-your-own-writes" inside a
multi-statement mutation.

**Re-usable pattern for OmniGraph:** When MR-737 §5.12 / Open Q10
gets to read-your-own-writes inside a mutation transaction, the
dual-level pattern is the prescription. We already have
`MutationStaging.pending` as the per-table in-memory accumulator;
we'd extend our scan operators to consult it before falling through
to the Lance scan.

### Variable-length expansion — maps to MR-737 §5.13

Kuzu's `RecursiveJoin` operator implements `(a)-[*1..k]->(b)`. Two
modes:

  1. **Bounded** (k ≤ small constant) — unrolled into k fixed-length
     joins, UNION ALL'd.
  2. **Unbounded** (k = ∞) — iterative pointer-chasing with a worklist.

**Re-usable patterns:**
- Unrolling for small k is the same approach lance-graph takes
  (capped at 20 hops; see §3.3). The 20-hop cap is the natural
  termination threshold.
- For unbounded, Kuzu uses a **worklist + visited-set** pattern with
  per-iteration result emission. OmniGraph already has a similar
  iteration pattern in `engine/exec/expand.rs` (TODO: re-verify
  exact file path).

## Entry points to revisit

- `src/optimizer/factorization_rewriter.cpp` — where the optimizer
  decides to insert `Flatten` / push past `Filter`.
- `src/processor/operator/hash_join/` — HashJoin's build phase where
  the semi-mask is constructed.
- `src/processor/operator/scan/scan_node_table.cpp` — probe-side scan
  that consumes the semi-mask.
- `src/transaction/transaction.cpp` — local-vs-disk layer composition.
- `src/processor/operator/recursive_extend/` — variable-length
  expansion.

## What NOT to take from Kuzu

- **Their custom storage format.** They use Kuzu-native columnar
  pages; we use Lance.
- **Their custom buffer pool.** They have a Kuzu-specific buffer
  pool; DataFusion + Arrow gives us memory pooling out of the box.
- **Their custom transaction subsystem.** They have their own WAL +
  recovery; we lean on Lance's manifest + our `__manifest` CAS layer.
- **Their custom executor.** Their executor is hand-rolled C++ vector-at-a-time;
  we use DataFusion's pull-based RecordBatch stream model.

## Decision impact on MR-737

- **§5.2 (factorization):** Validates the design choice. Factorized
  intermediate representation is standard practice in GDBMSs and
  Kuzu's implementation is the canonical reference.
- **§5.3 (SIP):** Validates the design choice. Semi-mask + lazy build
  + eager consume is exactly the shape DF DFP supports (per §1.5).
- **§5.12 / Open Q10:** Dual-level hash index pattern is the
  prescription. Worth lifting verbatim into MR-737 §5.12 spec.
- **§5.13:** Unrolled UNION-ALL for bounded k; iterative worklist for
  unbounded. The 20-hop cap from §3.3 (lance-graph) and Kuzu's
  matching default is the right cost-gate parameter.

## Open questions for follow-up

- **Q3.1.1 — Worst-case-optimal join (WCOJ) operators.** Kuzu also
  implements a "multiway join" operator that handles cyclic
  m-n joins better than chained binary HashJoins. This is referenced
  in Kuzu's CIDR paper but **not currently scoped in MR-737**. If
  cyclic patterns (triangle-finding, etc.) become a workload, WCOJ is
  the right approach. Defer to Phase 2+.
- **Q3.1.2 — Cypher parser.** Kuzu has a complete Cypher parser. If
  MR-737 §15 (Cypher frontend) becomes a priority, Kuzu's parser is
  the reference. Defer to Phase 3.
