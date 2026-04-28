# Architectural Invariants & Policies

**Type:** Reference / standing document
**Status:** Living — updated as decisions accrue
**Audience:** anyone proposing, reviewing, or implementing a change to any part of OmniGraph

This document captures the invariants that hold across all of OmniGraph — storage, engine, server, schema, indexing, observability, and the OSS / Cloud product split. New RFCs, designs, and implementations are checked against this list.

These are not query-engine-specific. They apply to every layer.

> **Note on numbering.** §IV (Additivity / migration) and §VIII (OSS / Cloud kernel-product split) are referenced from §X but not yet drafted in this revision. Pending upstream fill-in.

## How to use

- **Writing an RFC or design proposal:** walk through the relevant sections and state how the proposal upholds each invariant — or why a documented exception is justified.
- **Reviewing a PR or design:** scan for invariants the change might violate. The deny-list (§IX) is the fastest first pass.
- **Debating a tradeoff:** invoke the relevant invariant and check whether the tradeoff respects it. Invariants here are load-bearing; breaking one is rare and requires explicit justification in the proposal.
- **Updating this document:** add to the deny-list freely. Removing or relaxing an invariant requires the same review process as any other architectural decision.

## I. Substrate boundaries — what we don't build

The most important question for any new component is "does the substrate already do this?" If yes, we don't.

1. **Lance is the storage substrate.** We do not build a competing storage format. We build above Lance via a trait. Where we want format-level behavior Lance doesn't have, we propose it upstream or work around it.
   *Check:* Does this proposal introduce a parallel storage format, custom on-disk pages, custom serialization?

2. **No own-format WAL or transaction manager.** Lance manifest MVCC plus (eventually) MemWAL is the durability story. We do not write durability code.
   *Check:* Does this proposal track its own write log, recovery state, or transaction journal?

3. **No own buffer pool.** Lance + `object_store` + Lance's scan-aware cache cover our needs.
   *Check:* Does this proposal cache Arrow batches or pages outside Lance's cache?

4. **The runtime substrate provides relational machinery.** Whether we choose DataFusion (current prior) or a custom executor, we do not rebuild joins, aggregations, parallelism, spill. We build only graph- or multi-modal-shaped operators on top.
   *Check:* Are we extending the substrate via traits, or reimplementing parts of it?

5. **Don't replace Lance's index lifecycle.** New fragments enter without index coverage; reads union indexed and scan paths via `fragment_bitmap`. Our reconciler observes the same primitive; we don't replicate it.
   *Check:* Does this proposal maintain its own version of "what's indexed and what isn't" parallel to Lance's?

## II. Layering — the seams that hold

6. **The IR is the contract between frontend and backend.** Frontends emit IR; planner / executor consume it. No frontend logic leaks downward; no executor concerns leak upward.
   *Check:* Does the proposal add to the IR, or to a layer? If to a layer, does it cross another layer's concern?

7. **Capabilities and statistics flow upward; data flows downward.** Lower layers expose what they can do (capabilities) and what they know (statistics). Upper layers consume both. Methods alone are insufficient — methods without capability advertisement force one-size-fits-all plans.
   *Check:* When adding a method to a layer trait, did we also expose the capability so the planner can reason about it?

8. **One trait boundary per layer.** Crossing a layer means going through its trait. Direct calls to lower-layer concrete types from upper layers are forbidden.
   *Check:* Does this code call `lance::Dataset` directly outside engine-storage? Call planner internals from the executor?

9. **No god modules.** Single-module concerns: storage, IR, planner, executor, frontend, reconciler, schema, policy. Each crate has a reference test suite that runs without the others.
   *Check:* Does this PR add a concern to a crate that already owns a different one?

## III. Distributability — kernel stays remote-friendly

These are technical constraints, independent of whether we ship a distributed product. They preserve the architectural seam.

10. Storage trait is `Send + Sync`. No in-process-only assumptions in `Dataset` impls.
11. `Dataset` impls accept remote descriptors (URI, snapshot ref, fragment ID) without requiring an open in-process Lance handle.
12. **IR is location-neutral.** No IR operator embeds an assumption about where data lives.
13. Cost model accepts a network-cost term as a future additive component. No place hard-codes "all cost is local I/O."
14. Reconciler trait admits alternate implementations. In-process tokio is the OSS default; a separable worker fleet is the distributed shape.

## IV. *(Additivity / migration — placeholder, not yet drafted)*

## V. Honesty — cost, observability, calibration

20. **Estimate-vs-actual logging on every estimator.** Cost models drift; calibration is a continuous process, not a one-off.
21. **Coverage and lag are first-class metrics.** Index coverage, reconciler lag, cost-model accuracy — surfaced through the storage trait's `capabilities()` and a unified observability API.
22. **Honest failure modes.** Cost-model misses degrade gracefully (spill, partial-result, bounded abort). No silent OOM.
23. **Per-query budgets propagate through operators.** Memory cap, wall-clock timeout, max-rows-scanned, max-fragments-scanned. Operators respect them; budgets exposed via explain.
24. **Plans are explainable.** Every executed query can be inspected as IR + physical plan + cost annotations. No "you'd have to read the source to know what this does."

## VI. Patterns — use the unified mechanism

When two features look similar, they probably share a mechanism. Use it.

25. **Reconciler pattern for derivable state.** Index coverage, statistics, anything derivable from manifest state — reconciled, not job-queued.
26. **Polymorphism via Union, not per-feature lowering.** Interfaces / wildcards / alternation on nodes and edges share one IR (`Polymorphism<T>`) and one lowering (Union of per-type concrete plans).
27. **Mutations wrap read subplans.** Insert / Update / Delete / Merge are operators that consume read-shaped subplans. Same planner, same cost model, same storage trait.
28. **SIP for cross-operator selectivity propagation.** Producers publish ID bitmaps; downstream scans consume them through structured pushdown. Don't ad-hoc IN-lists.
29. **Factorize multi-hop, flatten only at projection.** Lists carry multiplicity through intermediate operators. Flatten is inserted by the planner where required, not eagerly.
30. **Stable row IDs as dense graph IDs.** Don't maintain parallel string→u32 maps. Lance's stable row IDs are the substrate's identity layer; we use them directly.
31. **Rank and score are columns.** Retrieval operators emit `_score`, `_rank`. Fusion operators consume rank-bearing batches. Don't discard rank in pipeline-twice merges.
32. **Policy as predicates.** Authorization decisions are filter expressions injected into the planner, not enforcement at the API boundary.

## VII. Quality gates — every change passes

33. **Tests at every boundary.** `MemStorage` for engine tests; planner-only tests; executor-only tests with a stub storage. No layer tested only via end-to-end.
34. **Reference implementation per trait.** Every trait has a primary impl (Lance for storage) and at least a test impl.
35. **Documented capability surface.** New capabilities are documented with what they advertise, who consumes them, how the planner uses them.
36. **Benchmark before optimization.** New optimizations land with a benchmark that motivates them; if the motivating workload doesn't exist, the feature waits.

## VIII. *(OSS / Cloud kernel-product split — placeholder, not yet drafted)*

## IX. Anti-patterns — deny list

If a proposal fits one of these, the burden is on the proposer to justify why this case is the exception.

- Synchronous-inline index updates for indexes expensive to build (vector ANN, FTS). Reconciler pattern instead.
- Custom WAL / transaction manager / buffer pool. Lance owns these.
- Job queue for state derivable from manifest. Reconciler pattern instead.
- Per-feature lowering for shapes that share a structure (interfaces, wildcards, alternation). Use one mechanism.
- Eager materialization of cross-products in multi-hop. Factorize; flatten only when needed.
- Ad-hoc IN-list filtering when SIP fits.
- String-flattened SQL filter generation when structured pushdown is available.
- In-process-only `Dataset` impls. `Send + Sync`, remote descriptors.
- Cost-blind plan choice. Lowering-order execution is not a planner.
- Hidden statistics. If a metric matters for plan choice, it must be exposed through the trait surface.
- Side-channels for query semantics. Search modes, mutations, polymorphism — all first-class IR concepts.
- Discarding rank in retrieval. Score and rank propagate as columns.
- State that drifts from the manifest. Derive from observable state.
- Cloud-only correctness fixes. Correctness is always OSS.
- Forking the codebase for Cloud. Trait-extension only.
- Hand-rolling something Lance already does. Check the spec first.
- Mutating in place state that should be immutable (Lance fragments, index segments). New segments instead.
- Silent failures. OOM, timeout, partial result — all surfaced and bounded.

## X. Review checklist (use against any non-trivial change)

Print this when reviewing an RFC or PR. Each line is **yes / no / N/A**.

- Does it respect the substrate? (§I)
- Does it cross only one trait boundary per layer? (§II)
- Are capabilities and stats exposed for any new behavior? (§II.7)
- Storage trait stays `Send + Sync` and remote-friendly? (§III)
- Additive, not rewrite? Feature-flagged where behavior changes? (§IV)
- Any new estimator has estimate-vs-actual logging? (§V.20)
- Coverage / lag / budget metrics surfaced? (§V.21–23)
- Failure modes graceful, bounded, observable? (§V.22)
- Reuses an existing pattern (reconciler, Union, mutation-wrap-read, SIP, factorize) where applicable? (§VI)
- Tests at every boundary, not just end-to-end? (§VII.33)
- Reference impl + test impl for any new trait? (§VII.34)
- If commercial-relevant: kernel/product split preserved? (§VIII)
- None of the deny-list patterns apply? (§IX)

## XI. Living document policy

This document is updated when:

- A new architectural decision establishes a new invariant — add it.
- An existing invariant is challenged and either reaffirmed (with the case sharpened) or revised (with explicit migration of any affected code).
- A new anti-pattern surfaces in review and deserves a place on the deny-list — add it.

Updates require the same review process as code. Adding to the deny-list (§IX) is cheap; removing or relaxing an invariant (§I–VIII) requires explicit justification in the proposal.

When an invariant is contested in the moment, the resolution path is: (a) state the case in the relevant RFC or PR; (b) link it from this document; (c) update this document if the resolution changes the rule.

## XII. Source / origin

These invariants were extracted from the architectural decisions in:

- **MR-737** — Query Engine v2 RFC (the kernel scope and seams)
- **MR-738** — OSS / Cloud strategy (the commercial overlay)
- The schema migration program (**MR-694** family — additive evolution, safety tiers)
- The policy program (**MR-722 / MR-725** — predicate pushdown)
- The reconciler / index-lifecycle work (**MR-737 §5.16, MR-688, MR-679, MR-680**)
- The factorization and SIP work (**MR-737 §5.2, §5.3** — Kuzu / Ladybug inspiration)
- The polymorphic-bindings framing (**MR-737 §5.13** — one mechanism for eight cells)

General precedent: Lance + LanceDB Enterprise architecture; ClickHouse merge subsystem; Kubernetes controllers; Postgres autovacuum; the FDAL stack (Flight + DataFusion + Arrow + Lance).

Adding a new invariant here means we've learned something — either from a hard call we made and want to preserve, or from a mistake we don't want to repeat. Both are worth recording.
