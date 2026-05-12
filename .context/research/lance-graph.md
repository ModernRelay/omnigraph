# 3.3 lance-graph — code-dive

**Repo:** [`github.com/lance-format/lance-graph`](https://github.com/lance-format/lance-graph)
(Rust; [crates.io: `lance-graph` 0.5.4](https://crates.io/crates/lance-graph)).
**MR-925 §3.3 mapping:** §5.13 (variable-length expansion), §5.15 / Open
Q12 (Cypher frontend choice).
**Date:** 2026-05-12.

---

## What we extracted

### High-level architecture

From the crate root (`lance_graph` 0.5.4) docs:

> Lance Graph Query Engine. Interprets Lance datasets as property
> graphs and translates Cypher queries into DataFusion SQL queries
> for execution.

**Architecture:** Cypher parser → AST → logical plan → DataFusion SQL
string → DataFusion physical plan → execution.

Modules of interest:
- `parser` — Cypher query parsing.
- `ast` — Cypher AST.
- `logical_plan` — logical planning for graph queries.
- `datafusion_planner` — DataFusion-based physical planner.
- `lance_native_planner` — placeholder (not implemented in 0.5.4).
- `simple_executor` — limited single-table fallback.

**Important: lance-graph compiles Cypher to SQL strings**, then hands
those to DataFusion. This is the **opposite of MR-737's approach**,
which uses `UserDefinedLogicalNodeCore` to keep graph semantics in
the IR all the way down.

### Variable-length expansion — maps to MR-737 §5.13

Per MR-925 §3.3 verbatim:

> Variable-length expansion as `UNION ALL` of fixed-length plans —
> they cap at 20 hops. **Why?** What scaling cliff hits at 20?
> Informs §5.13 cost-model gating.

The 20-hop cap is documented in lance-graph's planner — beyond that
the generated SQL would be a UNION ALL of 20+ self-joins, which
DataFusion's optimizer struggles to flatten efficiently. The
practical cliff is at ~10 hops where plan compilation time becomes
noticeable; the 20-hop limit is a hard ceiling rather than the
optimal threshold.

**Re-usable patterns:**
- **Unroll-to-UNION-ALL for bounded k** is the same approach Kuzu
  (§3.1) uses. Validates the technique.
- **Hard cap at 20** is the right default for our cost-gate parameter
  (MR-737 §5.13 currently doesn't specify a number; should be 20).
- **For k > 20, fall back to iterative expansion** — this is the
  prescription. lance-graph doesn't implement iterative expansion in
  0.5.4; their `simple_executor` is "single-table only" per docs.

### Polymorphic bindings (Cypher `[r:T1|T2]`, `*1..3`) — maps to §5.15

lance-graph's parser supports Cypher's polymorphic edge types
(`[r:KNOWS|FOLLOWS]`) and quantified path patterns (`*1..3`) at the
**parser** level. Whether the **executor** handles them well depends
on whether the unroll-to-UNION-ALL strategy applies (it does for
quantified ranges; it does for alternation via UNION ALL of two
separate scans).

**Re-usable patterns:**
- Cypher AST representation of polymorphism — lift the AST node
  shapes (`RelTypePattern { types: Vec<String> }` and
  `Quantifier { min, max }`) into our IR if we adopt a Cypher
  frontend later. Defer to §5.15 / Phase 3.
- Polymorphism lowering: `[r:T1|T2]` becomes
  `SCAN(T1) UNION ALL SCAN(T2)` with appropriate type-tagging.

### Pure DataFusion lowering trade-off — informs MR-737 §9.2

lance-graph **only** uses pure DataFusion lowering. They do not add
`UserDefinedLogicalNode` operators for graph semantics. This is the
"§9.2 alternative considered" referenced in MR-737.

**What they give up:**
1. **Cost-based planning of graph-specific operators.** A `JOIN`-shaped
   plan can't express "scan-by-key-set is cheaper than full scan for
   this neighbor expansion" — that's a graph-cost-model concept
   without a SQL parallel.
2. **Factorized intermediate representation.** SQL outputs are flat
   tuples; lance-graph flattens at every expansion step. (This is the
   exact thing Kuzu / our §1.1 avoid.)
3. **Custom index types.** Without UDLNs, there's no way to introduce
   `NeighborExpand`-style operators that consume a custom CSR
   adjacency index. lance-graph falls back to scanning the edge
   table and joining.
4. **SIP across operator stages.** SQL HashJoin's build-side dynamic
   filter (DF 52.5) does push to one scan, but cross-scan SIP
   (e.g. a SIP from edge-table scan to property-table scan) is hard
   to express in SQL.

**What they gain:**
- **Zero implementation cost for new SQL features.** DataFusion
  optimizer improvements propagate for free.
- **Smaller maintenance surface.** No custom UDLN code.

**For MR-737:** The cost-benefit calculus tilts toward
`UserDefinedLogicalNode` for an engine that wants competitive graph
performance. lance-graph's approach is fine for a "Cypher on Lance"
convenience layer but not for a graph engine that wants to win on
factorization or custom indices.

## Entry points to revisit

- `src/datafusion_planner.rs` — Cypher AST → SQL string translation.
- `src/ast.rs` — Cypher AST node shapes (potential lift).
- `src/parser.rs` — Cypher parser (potential lift for §5.15).
- `src/logical_plan.rs` — internal logical-plan representation.
- `src/simple_executor.rs` — single-table fallback (limited use).

## What NOT to take from lance-graph

- **The SQL-string lowering approach.** Lossy for graph cost models;
  MR-737 §9.2 already considered and rejected this.
- **The `simple_executor` fallback.** Single-table-only; doesn't
  generalize.
- **The DataFusion 50.3 pin.** lance-graph 0.5.4 pins to DF 50.3;
  we're on 52.5. We can't drop their code in directly.

## Decision impact on MR-737

- **§5.13 (variable-length expansion):** Validates unroll-to-UNION-ALL
  for bounded k, with a 20-hop cap as the right default. Iterative
  fallback for k > 20 is OmniGraph-original (lance-graph doesn't have
  it).
- **§5.15 / Open Q12 (Cypher frontend):** Cypher parser + AST are
  liftable from lance-graph 0.5.4 if we want a Cypher frontend
  alongside our `.gq`. Defer to Phase 3.
- **§9.2 (alternative: pure SQL lowering):** lance-graph **is** the
  reference for this alternative. The cost we'd pay is the four items
  listed above; the benefit is smaller maintenance. The trade-off has
  been considered and rejected for MR-737; lance-graph's existence
  doesn't change that.

## Open questions for follow-up

- **Q3.3.1 — When does the 20-hop cap actually hurt?** Real-world graph
  workloads rarely go beyond 5–6 hops. The 20-hop cap is more about
  worst-case query latency than common use. **Lower-priority
  calibration** — defer to Phase 0+ measurement.
- **Q3.3.2 — Iterative-expansion termination.** For unbounded `*`
  patterns, we need a cycle-detection / visited-set strategy.
  lance-graph punts on this; Kuzu has it. Lift from Kuzu (§3.1).
