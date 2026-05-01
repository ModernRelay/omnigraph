# Architectural Invariants & Patterns

**Type:** Reference / standing document
**Status:** Living — updated as decisions accrue
**Audience:** anyone proposing, reviewing, or implementing a change to any part of OmniGraph

This document captures two things:

- **Invariants** (Parts I–VI, VIII): load-bearing principles that hold across the architecture. Breaking one is rare and requires explicit justification.
- **Current architectural patterns** (Part VII): how we realize the invariants today. These are committed conventions, not eternal facts; they may evolve as the engine matures, but until they do, they constrain new work.

These are not query-engine-specific. They apply to every layer.

## Status legend

- *Status: decided.* No annotation needed; this is the default.
- *Status: open — see MR-X.* The principle is captured, but the concrete default or mechanism is still under discussion. Future work should follow the captured intent or update this document with the resolution.
- *Status: aspirational.* The invariant describes the target state; current code may not yet uphold it. PRs that move toward upholding it are welcome; PRs that drift away need explicit justification.

Capturing aspirational invariants on purpose: we'd rather record what we want to be true and have current code be measured against it than not have the rule at all.

## How to use

- **Writing an RFC or design proposal:** walk through the relevant sections and state how the proposal upholds each invariant — or why a documented exception is justified.
- **Reviewing a PR or design:** scan for invariants the change might violate. The deny-list (§IX) is the fastest first pass.
- **Debating a tradeoff:** invoke the relevant invariant and check whether the tradeoff respects it.
- **Updating this document:** add to the deny-list freely. Removing or relaxing an invariant requires the same review process as any other architectural decision.

---

## I. Substrate respect — delegate, don't rebuild

The first question for any new component: does the substrate already do this?

Current substrate is **Lance** for storage, indexes, and MVCC; **DataFusion** is the working assumption for relational machinery. These are committed choices (MR-737 §2.2, §5.11) but not eternal facts. The invariants below are about respecting *whatever* substrate we adopt.

1. **Don't rebuild what the substrate owns.** Storage format, durability (WAL, transaction journal), buffer pool, MVCC, index lifecycle — all delegated. Building parallel implementations turns the project into a different one and locks us out of substrate improvements.
   *Check:* Does this proposal introduce a parallel storage format, custom on-disk pages, custom serialization, custom WAL, custom buffer pool?

2. **Don't rebuild relational machinery** provided by the runtime substrate. Joins, aggregations, parallelism, spill — extension via the substrate's trait surfaces; never reimplementation.
   *Check:* Are we extending the substrate via traits, or reimplementing parts of it?

3. **Don't maintain state parallel to the substrate.** Observe substrate state and derive what we need. State that drifts from the substrate is a bug.
   *Check:* Does this proposal track index coverage, manifest versions, or fragment locations independently of the substrate?

## II. Layering — the seams hold

4. **The IR is the contract between frontend and backend.** Frontends emit IR; planner / executor consume it. No frontend logic leaks downward; no executor concerns leak upward.
   *Check:* Does the proposal add to the IR, or to a layer? If to a layer, does it cross another layer's concern?

5. **Capabilities and statistics flow upward; data flows downward.** Lower layers expose what they can do (capabilities) and what they know (statistics). Upper layers consume both. Methods alone are insufficient — methods without capability advertisement force one-size-fits-all plans.
   *Check:* When adding a method to a layer trait, did we also expose the capability so the planner can reason about it?

6. **One trait boundary per layer.** Crossing a layer means going through its trait. Direct calls to lower-layer concrete types from upper layers are forbidden.
   *Check:* Does this code call `lance::Dataset` directly outside engine-storage? Call planner internals from the executor?

7. **No god modules.** Single-module concerns: storage, IR, planner, executor, frontend, reconciler, schema, policy. Each crate has a reference test suite that runs without the others.
   *Check:* Does this PR add a concern to a crate that already owns a different one?

8. **Wire protocols are interchangeable; the IR is the contract.** The kernel produces `Stream<RecordBatch>` end-to-end; transports (HTTP/JSON, Arrow Flight, FlightSQL, future protocols) deliver them at the server boundary. No wire-protocol-specific code in kernel crates.
   *Status: aspirational — Flight not yet implemented; tracked in MR-765.*
   *Check:* Does this code import `arrow_flight` (or any transport crate) outside the server layer?

## III. Distributability — kernel stays remote-friendly

These are technical constraints, independent of whether we ship a distributed product. They preserve the architectural seam.

9. **The kernel admits parallel and remote implementations.** Trait surfaces are thread-safe; no in-process-only assumptions; remote dataset descriptors (URI, snapshot ref, fragment ID) are accepted without requiring an open in-process handle.

10. **IR is location-neutral.** No IR operator embeds an assumption about where data lives.

11. **Cost models accept new dimensions** (network, latency-tier) as additive extensions. No place hard-codes "all cost is local I/O."

12. **Background work admits alternate implementations.** In-process default; separable worker fleet for distributed deployment uses the same trait.
    *Status: aspirational — distributed deployment is out of scope today (MR-737 §2.2); these constraints preserve the seam.*

## IV. Evolution — additive over rewrite

13. **Additive over rewrite.** New IR variants and planner rules slot in. No "tear out and replace" PRs.

14. **Capabilities are additive enums.** New variants are additive. Existing implementations keep working.

15. **Feature-flag behavior changes.** Every change that alters runtime behavior ships behind a flag. Old code path stays until the new one is proven.

16. **No data drops without a migration.** When data needs to move (e.g., adopting stable row IDs), use in-place or dual-write windows. Never "drop and recreate."

17. **No breaking schema changes without a migration plan.** Schema-IR changes go through the migration planner with safety tier classification. See the MR-694 family.

## V. Honesty — what the system tells operators

18. **Estimate-vs-actual logging on every estimator.** Cost models drift; calibration is a continuous process, not a one-off.

19. **Operationally important state is observable.** Index coverage, reconciler lag, cost-model accuracy — surfaced through the storage trait's `capabilities()` and a unified observability API.

20. **Honest failure modes.** Cost-model misses degrade gracefully (spill, partial-result, bounded abort). No silent OOM.

21. **Per-query resource consumption is bounded and exposed.** Memory cap, wall-clock timeout, max-rows-scanned, max-fragments-scanned. Operators respect them; bounds exposed via explain.

22. **Plans are explainable.** Every executed query can be inspected as IR + physical plan + cost annotations. No "you'd have to read the source to know what this does." See MR-684.

## VI. Database guarantees — what OmniGraph promises as a system of record

These are user-visible commitments. They state what the engine guarantees and what it does not. For an "agent-native system of record," credibility lives here.

Specific defaults (timeout values, memory caps, TTL windows) are *configuration*, not invariants — see [docs/constants.md](constants.md) and per-deployment configuration. The invariant is that bounds and contracts exist, not their numerical values.

23. **Atomicity is per-query.** Every `.gq` query is atomic — multi-statement mutations are all-or-nothing via the substrate's atomic-commit primitive. No cross-query `BEGIN`/`COMMIT`; branches and merges fill that role for agent workflows.

24. **Schema integrity is strict at commit.** Type validation, required-field presence (auto-filled from `@default` if declared), uniqueness across batches and versions, and referential integrity — all enforced before commit succeeds. Per-write softening flags are opt-in, never default.
    *Status: aspirational — referential integrity at scale requires SIP-backed cross-table validation; not yet implemented. Cross-batch / cross-version uniqueness tracked in MR-714.*

25. **Isolation: per-query snapshot; read-your-writes within and across queries in a session.** Each query reads from one consistent manifest version. Within a multi-statement mutation, the read subplan inside each write operator sees the writes from earlier statements. Across queries in a session, reads always resolve the latest manifest version — no reader pinning to older snapshots.
    *Status: upheld for inserts/updates after MR-794 step 2+ — `MutationStaging`'s in-memory accumulator + `TableStore::scan_with_pending` (DataFusion `MemTable` union with the committed Lance scan, with merge-shadow semantics for chained updates) implements read-your-writes within a multi-statement mutation. Delete-touching mutations are limited to delete-only by parse-time D₂; closing the within-query RYW gap for deletes requires Lance's two-phase delete API (tracked: MR-793 / Lance-upstream lance-format/lance#6658). The "Lance HEAD ahead of `__manifest`" drift class is unreachable for op-execution failures (the partial-failure test pins this), but a narrowed residual remains at the finalize→publisher boundary because Lance has no multi-dataset commit primitive — see [docs/runs.md](runs.md) "Finalize → publisher residual".*

26. **Durability before acknowledgement.** Commit returns only after the substrate has confirmed durable persistence. No "fast" or "fire-and-forget" durability levels.

27. **Causal consistency across sessions.** If session A commits and session B subsequently reads, session B sees A's write. Single-coordinator: trivially via single-source manifest. Multi-coordinator: enforced via leader-for-writes plus session-token replica reads. Never weakened.
    *Status: aspirational on the multi-coordinator side.*

28. **Determinism within a snapshot.** Same query + same snapshot + same parameters → order-stable results (deterministic tie-breaks). Plan choice is deterministic given identical statistics. Cross-version determinism is best-effort, not guaranteed (statistics change, plans change).
    *Status: aspirational — current code may rely on HashMap iteration in some paths.*

29. **Writes are idempotent under retry.** Insert / Update / Merge take an explicit `on_conflict` policy. Clients may provide an idempotency key on writes; the server deduplicates retries within a configurable TTL window. Schema migrations are idempotent under replay.
    *Status: open — `on_conflict` policy lands with mutation IR (MR-737 Phase 8); idempotency-key TTL default is undecided.*

30. **No silent data loss or corruption.** Substrate-level checksums are trusted for storage integrity. Semantic-invariant checks at every commit catch higher-level cases (orphan edges, type drift, broken uniqueness). Every operation succeeds, fails loudly with cause, or degrades observably with metrics.

31. **Every operation has a documented bound.** "May run forever" is forbidden as a default. Defaults are configurable; the invariant is that bounds exist, are documented, and are enforced.

32. **Failure scope is bounded.** A failing query, fragment-level corruption, or background-task crash does not cascade. Per-table fragment isolation at the storage tier; per-query memory and timeout in the executor.
    *Status: aspirational on the per-query side — per-query memory cap not yet enforced; planned with MR-737 Phase 7.*

33. **Crash recovery via the same code paths as steady-state.** No special "recovery mode." On restart, the engine reads the manifest, finds the latest committed state, and resumes. Substrate atomicity ensures no partial writes survive.

34. **Strong consistency by default; relaxation is per-query, never per-default.** Strong (read-your-writes, monotonic, snapshot) is the default for every query. Eventual consistency is opt-in per read query for analytical workloads where staleness is acceptable. Never available on writes; always logged for audit.
    *Status: aspirational — eventual-consistency opt-in flag tracked in MR-425.*

## VII. Current architectural patterns

These are *how* we realize the invariants today. They are committed conventions — until we explicitly revise them, new code follows them. They are not eternal: a future architecture review may replace any of these with a different mechanism that upholds the same invariants. The deny-list (§IX) protects them in the meantime.

35. **Reconciler pattern for derivable state.** Index coverage, statistics, anything derivable from manifest state — reconciled, not job-queued. *Realizes the "don't maintain state parallel to the substrate" invariant.* See MR-737 §5.16.

36. **Polymorphism via Union, not per-feature lowering.** Interfaces / wildcards / alternation on nodes and edges share one IR (`Polymorphism<T>`) and one lowering (Union of per-type concrete plans). *Realizes "shared mechanism for shared shape."* See MR-737 §5.13.
    *Status: aspirational — node interfaces in MR-579; edge wildcards in MR-744.*

37. **Mutations wrap read subplans.** Insert / Update / Delete / Merge are operators that consume read-shaped subplans. Same planner, same cost model, same storage trait. *Realizes "writes share the planner with reads."* See MR-737 §5.12.
    *Status: aspirational — current mutation path is separate from reads.*

38. **SIP for cross-operator selectivity propagation.** Producers publish ID bitmaps; downstream scans consume them through structured pushdown. *Realizes "downstream operators prune via upstream selectivity."*
    *Status: aspirational — current code uses IN-list flattening in `Expand`.*

39. **Factorize multi-hop, flatten only at projection.** Lists carry multiplicity through intermediate operators. `Flatten` is inserted by the planner where required, not eagerly. *Realizes "intermediate state shouldn't materialize cross-products eagerly."*
    *Status: aspirational — current code materializes cross-products eagerly.*

40. **Stable row IDs as dense graph IDs.** Don't maintain parallel string→u32 maps. Lance's stable row IDs are the substrate's identity layer; we use them directly. *Realizes "use the substrate's identity layer."*
    *Status: aspirational — current code rebuilds `TypeIndex` per query.*

41. **Rank and score are columns.** Retrieval operators emit `_score`, `_rank`. Fusion operators consume rank-bearing batches. *Realizes "rank/score is data, not metadata."*
    *Status: aspirational — current RRF runs the pipeline twice and discards rank.*

42. **Policy as predicates.** Authorization decisions are filter expressions injected into the planner, not enforcement at the API boundary. *Realizes "authorization pushes down with other filters."*
    *Status: aspirational — Cedar enforcement currently at HTTP boundary only; tracked in MR-722 / MR-725.*

43. **Imports unify under `Source`; transport is interchangeable.** A single `Source` IR operator with provider variants (File, Flight, Lance, Stream) handles all imports. Lance-to-Lance is a fast-path that bypasses Arrow encode/decode. *Realizes "external data sources share one operator surface."*
    *Status: aspirational — current loader is JSONL-only; tracked in MR-765.*

## VIII. Quality gates — every change passes

44. **Tests at every boundary.** `MemStorage` for engine tests; planner-only tests; executor-only tests with a stub storage. No layer tested only via end-to-end.

45. **Reference implementation per trait.** Every trait has a primary impl (Lance for storage) and at least a test impl.

46. **Documented capability surface.** New capabilities are documented with what they advertise, who consumes them, how the planner uses them.

47. **Benchmark before optimization.** New optimizations land with a benchmark that motivates them; if the motivating workload doesn't exist, the feature waits.

## IX. Anti-patterns — deny-list

If a proposal fits one of these, the burden is on the proposer to justify why this case is the exception.

### Invariant violations (high bar to override)

- **Custom WAL / transaction manager / buffer pool.** Substrate owns these (§I.1).
- **Wire-protocol-specific code in kernel crates.** Kernel produces `Stream<RecordBatch>`; transport adapters live at the server boundary only (§II.8).
- **In-process-only `Dataset` impls.** Trait surfaces stay remote-friendly (§III.9).
- **State that drifts from the substrate / manifest.** Derive from observable state (§I.3).
- **Cross-query `BEGIN`/`COMMIT` transactions.** Branches replace them in OSS (§VI.23).
- **Acks before durable persistence.** "Best-effort commit" is forbidden (§VI.26).
- **Reads that see partial commits.** Atomicity is non-negotiable (§VI.23).
- **Operations without time bounds.** Every operation has a documented timeout or backoff (§VI.31).
- **"Recovery mode" code paths separate from steady-state.** Recovery uses the same code as ordinary reads (§VI.33).
- **Eventual consistency as a default.** Strong is default; eventual is opt-in per query, never on writes (§VI.34).
- **Schema migrations that are not idempotent under replay.** Idempotency is required for replay safety (§VI.29).
- **Plan choice that varies given identical input statistics.** Determinism is required (§VI.28).
- **HashMap iteration order in result ordering or plan choice.** Use deterministic tie-breaks (§VI.28).
- **Cost-blind plan choice.** Lowering-order execution is not a planner.
- **Hidden statistics.** If a metric matters for plan choice, it must be exposed through the trait surface (§II.5).
- **Side-channels for query semantics.** Search modes, mutations, polymorphism, imports — all first-class IR concepts (§II.4).
- **Hand-rolling something the substrate already does.** Check the spec first (§I.1).
- **Mutating in place** state that should be immutable (Lance fragments, index segments). New segments instead.
- **Silent failures.** OOM, timeout, partial result — all surfaced and bounded (§V.20).

### Pattern violations (overridable with justification)

These protect the *current* architectural patterns (§VII). A future review may revise them.

- **Synchronous-inline index updates** for indexes expensive to build (vector ANN, FTS). Reconciler pattern instead (§VII.35).
- **Job queue for state derivable from manifest.** Reconciler pattern instead (§VII.35).
- **Per-feature lowering for shapes that share a structure** (interfaces, wildcards, alternation). Use one mechanism (§VII.36).
- **Per-format import code paths** (one path for JSONL, another for Parquet, another for Flight). Use the `Source` IR operator (§VII.43).
- **Eager materialization of cross-products** in multi-hop. Factorize (§VII.39).
- **Ad-hoc `IN`-list filtering** when SIP fits (§VII.38).
- **String-flattened SQL filter generation** when structured pushdown is available.
- **Discarding rank in retrieval.** Score and rank propagate as columns (§VII.41).
- **Auto-creating placeholder nodes for orphan edges** (silent invention of data). Reject by default; opt-in per write (§VI.24).
- **Double-encoding data when both endpoints speak the same format** (e.g., Lance → Arrow → Lance when both are Lance). Use a fast-path (§VII.43).
- **Per-write durability fast paths** until MemWAL is stable AND a use case justifies the latency vs. risk tradeoff.

## X. Review checklist (use against any non-trivial change)

Print this when reviewing an RFC or PR. Each line is **yes / no / N/A**.

- Does it respect the substrate? (§I)
- Does it cross only one trait boundary per layer? (§II)
- Are capabilities and stats exposed for any new behavior? (§II.5)
- If touching the wire / transport surface, does kernel code stay protocol-agnostic? (§II.8)
- Do trait surfaces stay remote-friendly? (§III)
- Additive, not rewrite? Feature-flagged where behavior changes? (§IV)
- Any new estimator has estimate-vs-actual logging? (§V.18)
- Coverage / lag / budget metrics surfaced? (§V.19–21)
- Failure modes graceful, bounded, observable? (§V.20)
- Atomicity scope respected per query? (§VI.23)
- Schema integrity enforced strict at commit unless explicit opt-out? (§VI.24)
- Isolation level matches default (per-query snapshot, read-your-writes)? (§VI.25)
- Durability ack only after manifest commit? (§VI.26)
- Determinism preserved (order-stable, plan-deterministic)? (§VI.28)
- Idempotency: explicit `on_conflict`; idempotency keys honored if used? (§VI.29)
- Bounded operations: explicit timeout / memory / concurrency limits? (§VI.31)
- If touching imports / external data, does it go through `Source`? (§VII.43)
- If implementing a graph / retrieval feature: reuses an existing pattern (reconciler, Union, mutation-wrap-read, SIP, factorize, Source) where applicable? (§VII)
- Tests at every boundary, not just end-to-end? (§VIII.44)
- Reference impl + test impl for any new trait? (§VIII.45)
- None of the deny-list patterns apply? (§IX)

## XI. Living document policy

This document is updated when:

- A new architectural decision establishes a new invariant — add it.
- An existing invariant is challenged and either reaffirmed (with the case sharpened) or revised (with explicit migration of any affected code).
- A new architectural pattern is adopted — add to §VII.
- A current pattern (§VII) is replaced — update or remove the entry; update the deny-list.
- A new anti-pattern surfaces in review and deserves a place on the deny-list — add it.
- An *aspirational* invariant becomes upheld — remove the status annotation.
- An *open* invariant is decided — record the decision and remove the status annotation.

Updates require the same review process as code. Adding to the deny-list (§IX) is cheap; removing or relaxing an invariant (§I–VI, VIII) requires explicit justification in the proposal. Replacing a pattern (§VII) requires a design discussion linking to the new pattern; until that lands, the existing pattern stays.

When an invariant is contested in the moment, the resolution path is: (a) state the case in the relevant RFC or PR; (b) link it from this document; (c) update this document if the resolution changes the rule.

## XII. Source / origin

These invariants and patterns were extracted from the architectural decisions in:

- **MR-737** — Query Engine v2 RFC (the kernel scope and seams)
- **MR-744** — Edge wildcards / alternation (one cell of the polymorphic-bindings matrix)
- **MR-765** — Arrow Flight transport (query, import, export)
- The schema migration program (**MR-694** family — additive evolution, safety tiers, idempotent replay)
- The policy program (**MR-722** / **MR-725** — predicate pushdown)
- The reconciler / index-lifecycle work (**MR-737 §5.16**, **MR-688**, **MR-679**, **MR-680**)
- The factorization and SIP work (**MR-737 §5.2**, **§5.3** — Kuzu / Ladybug inspiration)
- The polymorphic-bindings framing (**MR-737 §5.13** — one mechanism for eight cells)
- The Source-operator framing (**MR-737 §5.12** — one mechanism for all imports)
- The database-guarantees discussion (§VI): ACID dimensions, CAP-style consistency model, scale-system precedents (ClickHouse, Turbopuffer, LanceDB, Postgres). Each invariant in §VI corresponds to a specific named decision; see prior architecture discussions for the option space considered.

General precedent: Lance + LanceDB Enterprise architecture; ClickHouse merge subsystem; Kubernetes controllers; Postgres autovacuum; the FDAL stack (Flight + DataFusion + Arrow + Lance).

Adding a new invariant or pattern here means we've learned something — either from a hard call we made and want to preserve, or from a mistake we don't want to repeat. Both are worth recording.
