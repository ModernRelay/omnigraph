# LLM Evolutionary Sampling — applicability to OmniGraph

**Type:** Research note (exploratory, not a committed plan)
**Status:** Draft for discussion
**Date:** 2026-05-14
**Author:** assigned via `claude/llm-evolutionary-sampling-research-UgKX8`

## TL;DR

Erol et al. (BauplanLabs / Stanford / TogetherAI / Bauplan, arXiv [2602.10387](https://arxiv.org/abs/2602.10387)) ship **DBPlanBench**, a harness that takes a DataFusion physical plan, serializes it to JSON, asks an LLM to propose RFC 6902 JSON-Patch edits (mostly hash-join build/probe-side swaps and multi-join reorderings), benchmarks each candidate end-to-end on Modal cloud sandboxes, and runs a small evolutionary loop (`n_steps`, `n_samples_per_step`, `top_k_patches`). They report up to **4.78× speedups** on TPC-DS queries and demonstrate that patches found on small scale factors transfer to larger ones via scan-signature matching.

The *direct* port — fork DataFusion, expose plans, prompt an LLM — is a poor fit for us. OmniGraph uses DataFusion only as a narrow `MemTable` utility in one function (`table_store::scan_pending_batches`, `crates/omnigraph/src/table_store.rs:1605`); we **own the IR** and the executor walks it directly (`crates/omnigraph/src/exec/query.rs:348`). Patching DataFusion would fight invariant [§I.1](../invariants.md) (substrate respect) and would make us maintain a fork.

The *adapted* form is interesting. The natural surface is **OmniGraph's own `QueryIR` and the lowering between `.gq` AST and engine execution** (`crates/omnigraph-compiler/src/ir/mod.rs:9`). The current lowering is deterministic by source order and has no cost model — the deny-list in [§IX](../invariants.md) calls this out explicitly ("cost-blind plan choice — lowering-order execution is not a planner"). Evolutionary plan search lets us close that gap by *measuring* good plans instead of building a hand-rolled cost model first. The mechanic also pairs naturally with the aspirational invariants [§V.18](../invariants.md) (estimate-vs-actual logging) and [§VII.41–42](../invariants.md) (SIP, factorize multi-hop): every evolutionary trial is a real measurement, and the search corpus surfaces the cases where SIP or factorization actually wins.

This note (a) summarizes what the paper does, (b) maps it onto our architecture honestly, (c) lists concrete application surfaces with cost/value, and (d) proposes a smallest experiment that would move us.

## What the paper does

The BauplanLabs system is an *offline plan optimizer with an online benchmarker*. Components:

1. **A patched DataFusion fork** (`datafusion_patched/` in their repo) that (a) emits a physical plan in a compact JSON form with node IDs as keys and edges as `input` / `left` / `right` references, and (b) accepts a patched plan and executes it. The "compact serialized representation" the paper refers to is this proto-derived JSON dialect plus a `succinct_table_info` blob carrying per-table cardinalities.

2. **An LLM sampler** (`src/sampling/gpt_plan_optimizer.py`) that takes the current plan + `succinct_table_info` + the SQL string and asks GPT-5 (default) to produce a JSON Patch array. The system prompt (`src/sampling/sql_optimization_prompts.py`) is highly specific: it walks the model through (i) cardinality estimation by *semantic reasoning over column names and predicates*, (ii) join-side swap rules (`left` should be the smaller input), (iii) multi-join reorder rules, and (iv) the projection-index recalculation needed after a swap. The model returns operations like `{"op": "replace", "path": "/6/hashJoin/on/0/left", "value": ...}`.

3. **An evaluator on Modal** that applies the patch with `jsonpatch`, hands the patched plan back to the patched DataFusion engine, runs it `n_runs` times against TPC-DS / TPC-H at the configured scale factor, validates result-set equality vs. the base plan, and reports `execution_time.min`.

4. **An evolutionary loop** (`src/sampling/orchestrator.py`) with three strategies:
   - `bol_evol` ("best-of-last evolutionary"): keep the best plan from the previous step, ask the LLM for `n_samples_per_step` further edits.
   - `pst_evol` ("post-evaluation evolutionary"): broader exploration, takes all last-step plans as bases.
   - `best_of`: single-step, no evolution (an N-of-1 LLM ablation).
   Selection is by `optimization_metric` (default `execution_time.min`).

5. **Cross-scale transfer** (`src/sampling/plan_scaler.py`, 740 lines): takes a patch found at SF=3 and rewrites it for SF=6/10/etc. by matching scan signatures and remapping internal node IDs. This is the headline reason the system is useful in practice — search is cheap on small data, payoff is on large data.

The fitness function is **end-to-end wall-clock**. There is no model of the optimizer or the executor; the LLM is steered by a hand-written system prompt encoding standard relational rules.

## Mapping onto OmniGraph

Three structural facts shape the answer.

**Fact 1: We are not a DataFusion consumer at the query level.** `omnigraph-compiler` lowers `.gq` to `QueryIR { pipeline: Vec<IROp>, ... }` (`crates/omnigraph-compiler/src/ir/mod.rs:9`) where `IROp` is `NodeScan | Expand | Filter | AntiJoin`. `exec::query::execute_query` walks the pipeline as a hand-rolled streaming interpreter and produces Arrow `RecordBatch`es directly (`crates/omnigraph/src/exec/query.rs:348`). DataFusion is touched only inside `scan_pending_batches` (`crates/omnigraph/src/table_store.rs:1612`) to apply SQL-style filters to in-memory pending batches for read-your-writes. We do not build `LogicalPlan` or `ExecutionPlan` trees anywhere. Therefore: **the surface the paper targets — DataFusion physical plans — does not exist in our hot path.** Forking DataFusion to add it would be the wrong direction.

**Fact 2: We have no planner.** The deny-list in [docs/invariants.md §IX](../invariants.md) lists "cost-blind plan choice — lowering-order execution is not a planner" as an explicit anti-pattern; the absence of a planner is acknowledged. Today, multi-hop traversal order, join-side selection, and ordering of `nearest()` / `bm25()` / `rrf()` retrievers are all determined by lexical order of the `.gq` query and lowering convention, not by any model of cost. This means there is **no existing decision surface to plug an LLM into**; we would be introducing one. That is a feature, not a bug: it means the IR is small enough that JSON-Patch on IR ops is a viable representation today, before the IR has accreted dozens of operator kinds.

**Fact 3: Lance and DataFusion are substrates, not our property.** Per [§I.1–3](../invariants.md) we do not rebuild what the substrate owns. The paper's approach to evolutionary search is *substrate-local*: they own the patched DataFusion and edit its physical plan. We don't, and we shouldn't. The right surface for us is *above* the substrate, at the IR / lowering layer where we already have authority. That maps cleanly to the paper's mechanic — JSON Patch on a serialized DAG of operators — even though the operators are ours, not DataFusion's.

The composite picture: the paper's *philosophy* (use an LLM with a real benchmarker as a search loop over plan variants, replacing or supplementing a cost model) is portable. The paper's *target* (DataFusion physical plans) is not. Where they patch `HashJoinExec` build/probe sides, we would patch `IROp::Expand` direction and order; where they tune `nprobes` on a SQL hint, we would tune Lance scan parameters at lowering time.

## Concrete application surfaces in OmniGraph

Listed roughly by value-to-difficulty ratio, best first.

### 1. Multi-hop `Expand` ordering and direction

**Surface.** A `.gq` query of the form `MATCH (a:A)-[r1:R1]->(b:B)-[r2:R2]->(c:C) WHERE … RETURN …` lowers today to `[NodeScan(a), Expand(a→b via R1), Expand(b→c via R2), Filter(…), …]` in source order (`crates/omnigraph-compiler/src/ir/lower.rs:11`). Two knobs that change runtime dramatically:

- **Hop order.** For a query that ends with a heavy filter on `c`, starting from `c` and expanding backward via CSC is usually faster than starting from `a` and expanding forward via CSR — because the filter prunes the seed set before traversal blows up. The IR already has `Direction` per `Expand`; the CSR/CSC indexes are built per edge type (`docs/indexes.md`); the topology to walk either direction is in place. The current lowering does not consider this.
- **Build-side for adjacency join.** `execute_expand` (`crates/omnigraph/src/exec/query.rs:770`) deduplicates destination IDs and passes them as a SQL `IN`-list to Lance for hydration. This is the [§IX](../invariants.md) "ad-hoc IN-list filtering when SIP fits" anti-pattern — the engine knows it. Evolutionary sampling could *demonstrate* the SIP win on a representative corpus before we commit code to it.

**LLM patch shape.** A small IR-Patch dialect: `{"op": "reverse", "path": "/pipeline/1/direction"}`, `{"op": "swap", "from": "/pipeline/1", "path": "/pipeline/2"}`, `{"op": "hint", "path": "/pipeline/1", "value": {"hydration_strategy": "sip"}}`. The system prompt would carry per-edge-type cardinality (we already have `__manifest` row counts) and per-type fanout statistics if we expose them.

**Fitness.** Wall-clock on representative `.gq` corpus + result-set equality (canonicalize `ORDER BY ... LIMIT` by sorting on the ordering columns before hash).

**Why this is the best target.** It is the surface the paper is closest to (join reorder, build-side swap), the underlying mechanics (CSR/CSC, direction) already exist, and the search is bounded — `pipeline.len()!` permutations is small for realistic queries.

### 2. Hybrid retrieval ordering and `k` tuning (`rrf` with `nearest` + `bm25`)

**Surface.** `IRExpr::Rrf { primary, secondary, k }` is one of our headline features (`crates/omnigraph-compiler/src/ir/mod.rs:122`). Today the engine runs both retrievers and fuses; the order, per-leg `k`, and any pre-filter pushdown into each leg are not adaptively chosen. Search-mode detection happens by scanning the `ORDER BY` list (`crates/omnigraph/src/exec/query.rs:111`).

**LLM patch shape.** Tunables per retriever leg: `nearest.nprobes`, `nearest.refine_factor`, `bm25.top_k`, and `rrf.k`. Plus the structural choice of *which* leg to run first and whether to use the first leg's results as a pre-filter to the second.

**Fitness.** Same wall-clock + result-set equality. The result-set equality check has to be careful here: top-K vector / BM25 ordering is sensitive to index parameters; the right oracle is *the user's chosen ranking metric* (recall@K on a labeled set, or rank-correlation with the unpruned plan), not bit-identical results. This is more delicate than the join case.

**Why this is the next best target.** Hybrid retrieval is exactly the workload OmniGraph sells as a differentiator. Any non-trivial tuning surface we can show speedup on is high-leverage. Lance's vector index already has the dials; we just don't expose them per-query yet.

### 3. Filter pushdown shape (Lance SQL string construction)

**Surface.** `build_lance_filter` translates IR filter trees into Lance SQL strings (`crates/omnigraph/src/table_store.rs:1159`). The translation today is structural — it doesn't consider how Lance's BTREE / inverted indexes will pick up the resulting expression. Two filters that are semantically equivalent (`x > 5 AND y = 'a'` vs `y = 'a' AND x > 5`) can hit different index paths.

**LLM patch shape.** Edits over the filter tree: reordering AND-clauses, factoring out a clause that's a BTREE prefix match, choosing between `IN (...)` and a join with a literal table.

**Fitness.** Wall-clock; the result-set check is straightforward (filters are deterministic).

**Why this is interesting but lower priority.** Lance's own scanner does some of this; the gap is narrower. But it's also the safest target — the search space is small, the validation is bit-identical, and the LLM is on familiar SQL ground (the paper's strength).

### 4. Vector index build parameters (offline, not per-query)

**Surface.** `ensure_indices` (`crates/omnigraph/src/table_store.rs:1349`) builds BTREE / FTS / vector indexes with default parameters. Lance's `IvfPqIndexParams` has `num_partitions`, `num_sub_vectors`, `metric_type`, etc.; we use defaults today.

**LLM patch shape.** Offline-only: per-vector-column index parameters. Search runs against a held-out query workload.

**Fitness.** Average query latency across the workload, traded against index size.

**Why this is interesting separately.** It's offline, the loop is slow, and the win is per-deployment rather than per-query. The paper's cross-scale transfer idea is directly applicable here: parameters tuned on a small scale factor often transfer to a larger one.

### 5. Per-table compaction / cleanup policy

**Surface.** `omnigraph optimize` and `omnigraph cleanup` (`docs/maintenance.md`) take global flags today. Per-table policy — small-row-count tables should compact aggressively, vector-index-bearing tables care about fragment alignment — is a per-deployment decision.

**LLM patch shape.** Per-table-type tuple: `(target_fragment_size, compaction_trigger, version_retention)`.

**Fitness.** A composite of read-latency-after-compact and storage-size-over-time.

**Why this is the weakest fit.** The decision rate is slow (hours/days), the LLM-in-the-loop is unjustified; a static heuristic or a small learned model would be cheaper. Listing for completeness.

### 6. (Not recommended) Forking DataFusion

Mentioned only to be explicit: we could fork DataFusion as the paper does. We should not. We touch DataFusion in one function and the paper's contribution is largely *because* of that fork. Reproducing it would commit us to maintaining a fork against an active upstream — and the marginal value is zero until we actually use DataFusion's planner, which we don't.

## Risks and open questions

**Hyrum's Law and shipped variance.** [§IX](../invariants.md) deny-list and [§VI.28](../invariants.md) require determinism: "Plan choice is deterministic given identical statistics." Evolutionary sampling *during search* is nondeterministic by design; *during serving* we must not expose that variance. The discipline is: search offline, freeze the winning plan as a cache keyed on canonicalized query shape + statistics-bucket, and serve from the cache. Same plan for same inputs.

**Semantic equivalence beyond bit-identity.** The paper validates result-set equality. We have queries where this is the right oracle (analytic queries with deterministic `ORDER BY`) and queries where it is not (top-K hybrid retrieval, where parameter changes shift ranking slightly but the user metric is recall@K, not bit-identity). We need a two-tier validator: bit-identical for deterministic ops, semantic for retrieval ops, with the retrieval oracle declared by query shape.

**Query corpus.** TPC-DS / TPC-H gave the paper a fixed, well-known target. We do not have a published `.gq` benchmark suite. The honest answer is that the first deliverable of any LLM-evo-sampling project on OmniGraph is *the corpus itself* — a representative set of `.gq` queries against a representative dataset, with provenance. This is a real bootstrap problem; without a corpus, "we got 4× on TPC-DS" doesn't translate.

**Compute cost.** The paper runs `n_samples=5 × n_steps=2 × n_runs=5 = 50` benchmark runs per query plus LLM calls, all on Modal sandboxes. For us, "Modal sandbox" maps to a containerized OmniGraph harness with a known fixture; the per-trial cost should be similar or lower (the engine is lighter), but the LLM bill is real and the wall-clock for a meaningful corpus is days, not hours.

**Invariant alignment.** The mechanic upholds several aspirational invariants in a clean way:

- [§V.18](../invariants.md) ("estimate-vs-actual logging on every estimator") — every evolutionary trial *is* the actual. The search output is a corpus of (plan, observed-cost) tuples that bootstraps a real cost model.
- [§V.19](../invariants.md) (observable state) — search results, frozen plans, and their statistics-buckets are auditable.
- [§VII.41–42](../invariants.md) (SIP, factorize) — the search will tell us *which* queries benefit from SIP or factorization before we commit to a uniform rule.
- [§VI.28](../invariants.md) (determinism) — *upheld at serving time* if we cache + freeze.

The mechanic does **not** violate the deny-list: we are not building a parallel storage or transaction layer, not bypassing the substrate, not introducing acks before durability, and not relaxing isolation. The substrate-respect line ([§I.1](../invariants.md)) is the one to watch: keep the search above the substrate, not inside a fork of it.

**Schema and `mutate` queries.** The paper's domain is read queries. Our `mutate_as` queries route through a different path (`MutationStaging` accumulator + `stage_*` / `commit_staged`, see [docs/runs.md](../runs.md) and [docs/transactions.md](../transactions.md)). Mutation plans should be out of scope for any first experiment — atomicity-critical paths are the wrong place to introduce LLM-proposed structural rewrites.

## Smallest experiment that would move us

The point of this note is to enable a decision, not commit to a project. The minimum experiment that produces signal:

1. **Pick one surface: multi-hop `Expand` ordering and direction.** Smallest patch dialect, clearest invariant (bit-identical results), surface we already know is suboptimal.

2. **Build a `.gq` corpus of ~30 queries** against the existing test fixtures (`crates/omnigraph/tests/fixtures/`). Mix: 2-hop and 3-hop traversals, with and without anti-join, with and without leaf filters. Document provenance.

3. **Add an `--explain ir` flag** to `omnigraph read` (or a `dump_ir` test helper) that serializes `QueryIR` to JSON. This is independently useful (the deny-list calls out "plans are explainable", [§V.22](../invariants.md)) and is the substrate the LLM edits.

4. **Wrap the existing engine in a benchmark harness** using `tempfile::tempdir()` (the pattern already in `tests/helpers/mod.rs`) and the `criterion` story (currently absent — see [docs/testing.md](../testing.md): "no `benches/` directories"). Per-trial cost is engine-init + run; `n_runs=5` should be sufficient.

5. **Implement a single LLM strategy: `bol_evol` with `n_steps=2, n_samples_per_step=5, top_k=1`.** Same as the paper's quick example. Use the same JSON-Patch primitive, restricted to a permutation + direction subset of operations.

6. **Measure:** geomean speedup, fraction of queries with ≥10% improvement, search cost in $ and wall-clock, transferability of winning patches to the same query shape on a different fixture.

If the geomean is ≥1.3× on a corpus we believe in, the next surface (hybrid retrieval) is justified. If it's ≤1.1×, we have learned something specific (probably: our IR is small enough that lexical order is already close to optimal) and the project sunsets cheaply.

What this experiment intentionally does *not* do: it does not introduce a runtime planner, does not change any `mutate` path, does not fork DataFusion, does not touch the manifest writer or recovery sweep. It is additive search over a serialized read-IR with offline freezing.

## References

- Paper: Erol, Hao, Bianchi, Greco, Tagliabue, Zou. *Making Databases Faster with LLM Evolutionary Sampling*. arXiv [2602.10387](https://arxiv.org/abs/2602.10387).
- Repo: [BauplanLabs/Making-Databases-Faster-with-LLM-Evolutionary-Sampling](https://github.com/BauplanLabs/Making-Databases-Faster-with-LLM-Evolutionary-Sampling).
- Key files in the upstream repo (read in preparing this note):
  - `src/sampling/sql_optimization_prompts.py` — system prompt (cardinality-by-semantics, join-side rules, projection-index recalculation).
  - `src/sampling/gpt_plan_optimizer.py` — LiteLLM driver with `n_samples` parallel calls.
  - `src/sampling/sample_plans.py` — `SamplingStrategy` (`bol_evol`, `pst_evol`, `best_of`), upstream-patch chain reconstruction.
  - `src/sampling/orchestrator.py` — multi-step loop, resume semantics.
  - `src/sampling/plan_scaler.py` — cross-scale transfer via scan signatures.
- OmniGraph internals referenced:
  - `crates/omnigraph-compiler/src/ir/mod.rs:9` — `QueryIR` / `IROp`.
  - `crates/omnigraph-compiler/src/ir/lower.rs:11` — `lower_query`, source-order lowering.
  - `crates/omnigraph/src/exec/query.rs:348` — `execute_query`, hand-rolled pipeline interpreter.
  - `crates/omnigraph/src/exec/query.rs:770` — `execute_expand`, the IN-list hydration path.
  - `crates/omnigraph/src/table_store.rs:1159` — `build_lance_filter`, IR-filter → Lance SQL.
  - `crates/omnigraph/src/table_store.rs:1349` — `ensure_indices`, index build parameters.
  - `crates/omnigraph/src/table_store.rs:1612` — `scan_pending_batches`, the only DataFusion `MemTable` site.
- Invariants engaged: [§I.1–3](../invariants.md) substrate respect, [§V.18–22](../invariants.md) honesty / observability / explainability, [§VI.28](../invariants.md) determinism, [§VII.41–42](../invariants.md) SIP / factorize, [§IX](../invariants.md) deny-list ("cost-blind plan choice", "ad-hoc IN-list filtering when SIP fits", "shipping observable behavior as if it weren't part of the contract").
