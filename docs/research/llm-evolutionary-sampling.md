# LLM Evolutionary Sampling — applicability to Lance directly

**Type:** Research note (exploratory, not a committed plan)
**Status:** Draft for discussion — revision 3 (first harness landed; see "First implementation landed" below)
**Date:** 2026-05-14
**Author:** assigned via `claude/llm-evolutionary-sampling-research-UgKX8`

## TL;DR

Erol et al. (BauplanLabs / Stanford / TogetherAI / Bauplan, arXiv [2602.10387](https://arxiv.org/abs/2602.10387)) ship **DBPlanBench**: take a DataFusion physical plan, serialize it to JSON, have an LLM propose RFC 6902 JSON-Patch edits (hash-join build/probe swaps, multi-join reorders), benchmark each candidate end-to-end, run a small evolutionary loop (`n_steps`, `n_samples_per_step`, `top_k_patches`). Up to **4.78× speedups** on TPC-DS, with patches found at small scale factors transferring to larger ones via scan-signature matching.

This note targets **Lance directly** rather than OmniGraph's IR. Lance is a better-shaped target for two reasons:

1. **Lance is parameter-heavy, not just plan-shape-heavy.** The biggest performance wins in Lance come from *configuration tuples* — index build parameters (`num_partitions`, `num_sub_vectors`, quantizer choice), scan-time knobs (`nprobes`, `refine_factor`, `batch_size`, `io_buffer_size`, `prefilter` vs postfilter), fragment layout, compaction policy. The Lance performance guide [openly admits](https://lance.org/guide/performance/) the defaults are "balanced" rather than tuned per workload, and the AIMD throttle starts at 2000 req/s with a 5000 cap — generic defaults that any specific deployment should re-tune. The BauplanLabs JSON-Patch-on-DAG mechanic transfers, but the substrate to patch is a config object, not a `HashJoinExec`.
2. **Cross-scale transfer matters more here than in the paper.** The paper's headline is "tune at SF=3, apply at SF=10." Lance has this problem *intrinsically* — you train an `IvfPq` index on a sample, you scan with parameters chosen on a development dataset, and the per-deployment differences (vector dimensionality, partition count, query selectivity) dominate any plan-shape effect. Cross-scale transfer of well-tuned config tuples is exactly what production users need.

The direct DataFusion-plan-patching angle still exists for Lance — `LanceTableProvider` lets DataFusion run SQL over Lance, and that produces an `ExecutionPlan` that could be patched the same way — but it depends on upstream features (JSON round-trip of `ExecutionPlan`) that Bauplan added in their fork. **Contributing those upstream to Lance + DataFusion** is a more durable bet than maintaining a fork. The parameter-search angle, by contrast, needs **no fork at all** — Lance already accepts these as config and produces measurable execution metrics.

This revision leads with the parameter-search angle as the primary target, treats the upstream-contribution plan-patching angle as the long-term play, and demotes the OmniGraph-IR angle (which is real but less novel) to a closing footnote.

## What the paper does (compact recap)

DBPlanBench has four pieces (`src/sampling/` in the upstream repo):

1. A **patched DataFusion fork** (`datafusion_patched/`) that serializes physical plans to JSON with node IDs as keys and `input` / `left` / `right` edges, plus a `succinct_table_info` blob with per-table cardinalities — and accepts a patched JSON plan and executes it.
2. An **LLM sampler** (`gpt_plan_optimizer.py`) that prompts GPT-5 with the plan, table info, and query. The system prompt (`sql_optimization_prompts.py`) walks the model through (i) cardinality estimation by semantic reasoning over column names and predicates, (ii) build-side selection rules (smaller input on left), (iii) multi-join reorder rules, (iv) projection-index recalculation after a swap. The model returns RFC 6902 patches.
3. A **Modal-sandboxed evaluator** that applies the patch with `jsonpatch`, runs the patched plan `n_runs` times, validates result-set equality vs. the base plan, and reports `execution_time.min`.
4. An **evolutionary loop** (`orchestrator.py`) with three strategies: `bol_evol` (keep best, mutate from there), `pst_evol` (broader exploration, take all last-step plans as bases), `best_of` (single-step, no evolution).

The cross-scale transfer (`plan_scaler.py`, 740 lines) is a separate machine: it walks the new SF's plan, matches scan signatures against the old SF's plan, remaps node IDs, and reapplies the patches. This is the practical-value lever.

The fitness function is end-to-end wall-clock. There is no internal model of the optimizer.

## Why Lance is the right target shape

Lance's tunable surface, drawn from the [performance guide](https://lance.org/guide/performance/), [read/write guide](https://lance.org/guide/read_and_write/), [index pages](https://lance.org/format/table/index/), and [DataFusion integration](https://lance.org/integrations/datafusion/):

**Vector index build (`IvfPq` / `IvfHnswSq` / `IvfHnswPq` / `RaBitQ`).** `num_partitions`, `num_sub_vectors`, `nbits`, quantizer choice (PQ vs SQ vs RQ), `sample_rate` (default 256), `metric_type` (L2 / cosine / dot), HNSW-specific `ef_construction` and `m`. Storage and recall trade off heavily across these; the perf guide lays out the math (e.g., `num_partitions * sample_rate * dimension * sizeof(data_type)` is the IVF training RAM, which is non-trivial — 768 MiB at 1024 partitions × 768-d float32).

**Scan-time vector search.** `nprobes`, `refine_factor`, pre-filter vs post-filter (pre is cheaper when predicates are selective; post is cheaper when they're not — currently a static decision).

**Scalar index choice per column.** BTree (range queries), Bitmap (equality, small-range, many-bitmap-overhead), Bloom-filter (membership, no range), Label-list (list columns), Zone-map (page-pruning), R-Tree (spatial), Ngram (LIKE), FTS (text). The right choice depends on column cardinality, value distribution, and the query workload — not on the schema. Today an operator picks one per column at index-build time; a workload-aware advisor is a clean LLM job.

**Scan parameters.** `batch_size` (default 8192 rows; recommended ~1MB-per-batch for scalar, smaller for high-dim vectors), `io_buffer_size` (default 2GB), `LANCE_IO_THREADS` (8 local / 64 cloud), `LANCE_CPU_THREADS` (cores), `index_cache_size_bytes` (default 6 GiB), AIMD throttle (initial 2000, max 5000, decrease 0.5, additive 300, burst 100). Every one of these has a deployment-specific optimum.

**Write parameters.** `max_rows_per_file`, `max_rows_per_group`, `max_bytes_per_file`, `data_storage_version` (v2 has different page sizes), `enable_v2_manifest_paths`, `enable_stable_row_ids` (perf doc notes this is "experimental" for indices).

**Compaction.** `target_rows_per_fragment` (default 1Mi), `materialize_deletions`, `materialize_deletions_threshold`, `num_threads`, `defer_index_remap` (Fragment Reuse Index — decouples compaction from index rebuilds, huge for continuous-ingest tables but adds an index-load-time cost). Frequency and timing of compaction.

**Plan-patching surface (via DataFusion).** `LanceTableProvider` registers a Lance dataset as a DataFusion table; DataFusion's standard `ExecutionPlan` covers joins, aggregates, sorts, while Lance contributes a custom `LanceScanExec`-style node with pushdown for column selection and simple filters. The Bauplan-style edit space (`HashJoinExec` build/probe swap, multi-join reorder) lives here.

The key structural observation: **the first six surfaces are configuration. The seventh is a plan.** Bauplan's contribution is for the seventh; for Lance, the first six are higher-leverage and don't need a fork.

## Application surfaces in Lance (ranked by value/difficulty)

### 1. Workload-conditioned vector index build (`IvfPq` / `IvfHnsw*`)

**Surface.** Per `vector` column, the choice of quantizer (PQ / SQ / RQ) and its parameters drives storage size by ~10× and recall by ~10 percentage points. Defaults are deliberately conservative. The decision is a tuple, not a tree.

**LLM patch shape.** JSON Patch over a `VectorIndexConfig` object:
```json
[
  {"op": "replace", "path": "/quantizer", "value": "IvfHnswSq"},
  {"op": "replace", "path": "/num_partitions", "value": 4096},
  {"op": "replace", "path": "/num_sub_vectors", "value": 96},
  {"op": "replace", "path": "/sample_rate", "value": 128},
  {"op": "replace", "path": "/ef_construction", "value": 200},
  {"op": "replace", "path": "/m", "value": 32}
]
```

**Prompt seeding.** Pass the column schema, vector dimensionality, dataset row count, sample query workload (top-k values), and the recall/latency target. The LLM has good priors here (PQ for storage-bound, HNSW for low-latency, RQ for streaming-friendly recall).

**Fitness.** Two-objective: `recall@K` against a labeled query set, and `p95_latency`. Combine via a deployment-specific weighting (or Pareto frontier).

**Cross-scale transfer.** Build at 1% sample, apply at full. Validate by re-measuring on full at the chosen tuple.

**Why this is the best target.** It is the surface Lance defaults explicitly under-tune. The decision is per-deployment, not per-query, so the harness can amortize cost. And the LLM's semantic reasoning (column name → vector type → likely quantizer) is on familiar ground.

### 2. Per-query scan tuning (`nprobes`, `refine_factor`, pre/post-filter)

**Surface.** Even with a fixed vector index, the right `nprobes` and `refine_factor` depend on the predicate selectivity. A highly-selective metadata predicate ("status = 'active'" eliminating 95% of rows) flips the pre-vs-post-filter trade-off; today this is a per-query knob, picked statically.

**LLM patch shape.** JSON Patch on a `QueryConfig`:
```json
[
  {"op": "replace", "path": "/nprobes", "value": 32},
  {"op": "replace", "path": "/refine_factor", "value": 10},
  {"op": "replace", "path": "/prefilter", "value": true}
]
```

**Fitness.** Recall@K on a labeled set + latency. **The result-set check matters here:** lowering `nprobes` lowers recall, so bit-identity is the wrong oracle — use rank correlation or labeled recall.

**Cross-scale transfer.** Tune on a slice; apply globally.

**Why this is the second-best target.** It is per-query, so search costs amortize less, but it's where Lance users actually see knobs they don't know how to set.

### 3. Scalar-index recommender across a workload

**Surface.** Given a representative SQL workload over a Lance dataset, choose which columns get indexes and which kind (BTree / Bitmap / Bloom / Zone-map / Label-list / Ngram). Lance lets you build one of each per column; the wrong choice costs index storage and build time. The Lance perf guide is explicit that "Queries against large ranges are currently extremely slow [on bitmap]" — index choice is non-obvious.

**LLM patch shape.** JSON Patch over a `Vec<{column, index_type, params}>` describing the full index set for a dataset.

**Fitness.** Geomean query latency across the workload, with a soft budget on total index size.

**Why this is interesting.** Index advising is a classic DBA problem; the LLM's column-name-semantic reasoning + workload-pattern detection is exactly what a human DBA does, slowly. This is the surface where the BauplanLabs prompting style (semantic cardinality estimation) transfers most directly.

### 4. Compaction & fragment policy

**Surface.** `target_rows_per_fragment`, FRI on/off (`defer_index_remap`), compaction frequency, materialize-deletions threshold. The right values depend on ingest rate, read pattern, and whether the table has indices. The perf guide notes compaction conflicts with index builds and that FRI was added specifically to decouple them — a deployment-specific knob no default handles well.

**LLM patch shape.** Configuration tuple per table or per table archetype (high-ingest fact table vs. slow-changing dimension).

**Fitness.** A composite — read-after-compact latency, write throughput, storage size over a synthetic week.

**Why this is a slower loop but high-value.** The benchmark runs over a *trajectory* (ingest then read), not a single query. Each candidate evaluation is minutes-to-hours. But the win is per-deployment and persists for the life of the schema.

### 5. AIMD throttle and thread-pool tuning per object store

**Surface.** `lance_aimd_initial_rate`, `lance_aimd_max_rate`, `lance_aimd_decrease_factor`, `lance_aimd_additive_increment`, `lance_aimd_burst_capacity`, `LANCE_IO_THREADS`, `LANCE_CPU_THREADS`, `io_buffer_size`, `batch_size`. The perf guide gives a target "S3 gets to 5000 req/s in ~10 seconds" — meaning these defaults are S3-shaped. RustFS, MinIO, GCS, R2 all behave differently.

**LLM patch shape.** Tuple of throttle + thread + buffer settings, conditioned on the object store type.

**Fitness.** Scan throughput, latency at p50/p95/p99, error rate under load.

**Why this is narrow but valuable.** It's per-environment, the search space is small, and the LLM's priors on object-store behavior are decent.

### 6. Plan-patching on LanceTableProvider + DataFusion (upstream contribution path)

**Surface.** `LanceTableProvider` registers a Lance dataset as a DataFusion table; queries hit DataFusion's planner and produce an `ExecutionPlan` tree that includes a Lance-scan node plus standard DataFusion operators (joins, aggregates, sorts). The Bauplan technique fits here directly — same `HashJoinExec` swap, same multi-join reorder, plus Lance-specific patches like "pull this filter down into the scan as a Lance `prefilter`."

**Why this is the long-term play, not the short-term.** The Bauplan technique needs a way to serialize `ExecutionPlan` to JSON and accept a patched one. That feature does not exist in upstream DataFusion; Bauplan added it in their fork. **The right move is to contribute that upstream** — it's independently useful (plan portability, RPC-shipped plans, observability) — and then layer evolutionary sampling on top. Forking Lance (or DataFusion via Lance) to ship this internally is the wrong investment; the maintenance burden against an active upstream is high, and the value is exactly the same as the open-source version.

Until that lands upstream, this surface is parked.

### 7. Note on `merge_insert` strategy

Lance's `merge_insert` has a small DAG of `WhenMatched` / `WhenNotMatched` decisions. The structural variation is small (4–6 shapes) and the right choice is usually obvious from the user's intent (upsert, insert-if-not-exists, replace-portion). LLM-evo doesn't add value here vs. a static rule.

## Risks and open questions

**Lance fork vs. external harness.** Surfaces 1–5 need **no fork** — Lance's API already accepts these as parameters and emits the metrics. The harness is "build dataset with config X, run workload, measure, repeat." Surface 6 (plan-patching) needs upstream features; until they land, parked.

**Result-set equality is the wrong oracle for retrieval surfaces.** For vector / FTS / hybrid search, parameter changes shift ranking. Use recall@K against a labeled set, or rank correlation against an exhaustive baseline. The BauplanLabs validator was bit-identical because they targeted analytic queries.

**Workload corpus is the bootstrap problem.** Surfaces 1, 2, 3, 4 are *workload-conditioned* — what's optimal for one query pattern is wrong for another. The first deliverable of any project here is a representative query workload (or a generator), with provenance. TPC-H / TPC-DS / SIFT / DEEP10M cover the analytic and vector cases; graph workloads are scarcer.

**Determinism at serving time.** Search introduces variance; serving must not. Discipline: search offline, freeze the chosen tuple as part of the deployment's configuration, version it. Same tuple → same plan → same answer. This is the same Hyrum's-Law point that applied to the OmniGraph framing.

**Compute cost.** The paper uses Modal cloud sandboxes; per-trial costs are real. For Lance, a typical surface-1 search (vector index params) is on the order of dozens of trials × index-build-time, where each build is minutes. Surface-4 (compaction) is hours per trial. Budget realistically; cross-scale transfer is what makes this affordable.

**Substrate respect (§I.1).** Surfaces 1–5 do not violate substrate respect — we're driving Lance from the outside, no fork. Surface 6 requires the upstream change first; until then, do not introduce a fork.

**Upstreaming the harness itself.** The natural home for this is **a `lance-tuner` crate** (or similar) contributed to the Lance project. It's a generic LLM-driven workload-conditioned configuration tuner; OmniGraph is one consumer. Shipping it externally to Lance is fine, but the project's value compounds if it lands in the Lance ecosystem where users find it.

## Smallest experiment that would produce signal

Pick **surface 1 (workload-conditioned vector index build)** for the first cut. It is the surface with the highest known gap between defaults and per-workload optima, the LLM has strong priors, and the validation oracle (recall@K) is well-defined.

1. **Pick a public dataset:** SIFT1M (128-d, 1M vectors) or LAION-400M-sample (768-d, ~1M vectors). Both have published recall benchmarks for sanity check.
2. **Define the workload:** a fixed set of 1000 query vectors + ground-truth top-100 neighbors (precomputed via brute force, cached).
3. **Define the patch dialect:** JSON Patch over `{quantizer, num_partitions, num_sub_vectors, nbits, sample_rate, metric_type, ef_construction?, m?}` with type-aware validation (e.g., `ef_construction` only valid for HNSW variants).
4. **Define fitness:** weighted `(recall@10, p95_latency)`. Use `recall@10 >= 0.95` as a hard floor and minimize `p95_latency` subject to it.
5. **Implement `bol_evol`:** `n_steps=3, n_samples_per_step=4, top_k=1`. Per step: build the index, run all 1000 queries, measure recall+latency, report. Each step is ~minutes-to-hours.
6. **Compare to baselines:** Lance defaults, the published best for the dataset, and a random-search baseline of equal compute budget.
7. **Measure cross-scale transfer:** take the winning tuple at 100k vectors, apply at 1M and 10M, see if the win persists.

If the winning tuple beats Lance defaults by ≥1.3× in latency at equal recall on the test dataset, surface 2 (scan tuning) is the next experiment. If it only beats by ≤1.1×, the conclusion is "Lance defaults are close to per-workload optimum for the tested workloads," which is itself publishable and sunsets the project cheaply.

**Out of scope for the first experiment:** Surface 6 (plan-patching) entirely. Surface 4 (compaction) because the per-trial cost is too high to learn fast. OmniGraph integration — make it a generic Lance tool first; an OmniGraph wrapper is a one-day port if the tool works.

## First implementation landed: PQ kernel autoresearch harness

The first harness committed against this research is **not** surface 1 above. It targets a related-but-different surface and adopts a different control loop. The harness lives at [`research/lance-autoresearch/`](../../research/lance-autoresearch/) (its own README + `program.md` document the contract).

**Target shifted from index-build tuning (surface 1) to PQ kernel optimization.** The kernels in `lance-linalg`'s `distance/pq` module — `compute_distance_table_l2` + `probe_pq_l2_top_k` — sit one layer below the parameter surface and are exercised on every ANN query. They're self-contained Rust (no DataFusion plumbing), the per-trial eval is seconds not minutes, and a winning kernel ports directly upstream as a `lance-format/lance` PR. Index-parameter tuning remains a valid surface; it's just a slower iteration loop and a longer upstream path.

**Control loop shifted from BauplanLabs evolutionary sampling (`bol_evol`, `n_samples_per_step`, tournament selection) to Karpathy's single-agent autoresearch contract.** With seconds-scale evaluation, parallel-sample tournaments don't pay for themselves; a single agent editing one file in a tight loop is more sample-efficient. The paper's tournament shape is the right answer when the eval is minutes-to-hours (its TPC-DS regime); when eval is seconds, the autoresearch shape wins. If we move to a slower surface later (index-build tuning, compaction tuning), the BauplanLabs control loop becomes the right choice again.

**Oracle shifted from recall@K vs. SIFT1M to bit-exact equivalence + multi-distribution speed.** The doc's surface 1 oracle is `recall@K ≥ floor, minimize p95_latency` on one fixed dataset. That conflates "kernel is mathematically correct" with "kernel preserves recall on this distribution"; it also gives the agent incentive to overfit lossy approximations to SIFT-shaped clusters. The harness instead requires `max_abs_err ≤ 1e-4` against a scalar reference kernel on a 5-distribution input battery × 3 PQ shapes (correctness phase), then measures geomean ns/query across 3 shapes × 3 distributions with a worst-case guard (speed phase). Any "improvement" generalizes across distributions and PQ shapes by construction. There is no fixed dataset; the harness is fully self-contained.

**What remains to validate against the paper's findings.** Whether autoresearch-shape LLM-driven kernel work actually produces meaningful Lance-upstreamable speedups is the open question; the harness exists to answer it empirically. If the answer is "yes, ≥10% geomean speedup with worst-case guard intact," the obvious next step is to spin the loop on surface 1 (index parameter tuning) with the BauplanLabs control shape, where the per-trial cost justifies parallel sampling. If the answer is "no meaningful win after a hundred trials," that's also a publishable conclusion — autoresearch-shape kernel optimization may already be at substrate-defaults optimum.

## Footnote: OmniGraph-IR as an alternative target

The previous revision of this note focused on patching OmniGraph's own `QueryIR` (`crates/omnigraph-compiler/src/ir/mod.rs:9`) — multi-hop `Expand` ordering and direction, hybrid retrieval (`rrf`) leg tuning, filter pushdown shape. That surface is real and the [§IX](../invariants.md) deny-list already calls out the gap ("cost-blind plan choice — lowering-order execution is not a planner").

Now that the framing is Lance-direct, the OmniGraph-IR angle is **secondary, not abandoned**:

- The two are complementary: a Lance-tuner output (e.g., a chosen `IvfPq` configuration) is consumed by OmniGraph at `ensure_indices` time anyway. Tuning Lance below us is the highest-leverage layer.
- The OmniGraph-IR surface remains the right answer for *plan-shape* decisions inside OmniGraph (`Expand` direction, hop order, hybrid-retrieval leg ordering) because those decisions live above Lance and Lance can't reason about them.
- Plan-shape and parameter-tuning can compose: pick the right hop order *and* the right `nprobes` for the resulting vector scan.

If a Lance-tuner project is built first and works, the OmniGraph-IR project can reuse most of the harness (corpus, LLM driver, evolutionary loop), swapping the patch dialect and the engine target. The reverse — building OmniGraph-IR first and porting to Lance — is also possible but less leveraged, because Lance's parameter surface generalizes beyond OmniGraph.

## References

- Paper: Erol, Hao, Bianchi, Greco, Tagliabue, Zou. *Making Databases Faster with LLM Evolutionary Sampling*. arXiv [2602.10387](https://arxiv.org/abs/2602.10387).
- Repo: [BauplanLabs/Making-Databases-Faster-with-LLM-Evolutionary-Sampling](https://github.com/BauplanLabs/Making-Databases-Faster-with-LLM-Evolutionary-Sampling).
- Key upstream files (read in preparing this note):
  - `src/sampling/sql_optimization_prompts.py` — system prompt: semantic cardinality estimation, join-side rules, projection-index recalculation.
  - `src/sampling/gpt_plan_optimizer.py` — LiteLLM driver, parallel sampling.
  - `src/sampling/sample_plans.py` — `SamplingStrategy` (`bol_evol`, `pst_evol`, `best_of`), patch-chain reconstruction.
  - `src/sampling/orchestrator.py` — multi-step loop, resume semantics.
  - `src/sampling/plan_scaler.py` — scan-signature-based cross-scale transfer.
- Lance documentation referenced:
  - [Performance guide](https://lance.org/guide/performance/) — thread pools, memory model, AIMD throttle, per-index-type characteristics, Fragment Reuse Index.
  - [Read and write](https://lance.org/guide/read_and_write/) — `WriteParams`, `compact_files`, `merge_insert`.
  - [DataFusion integration](https://lance.org/integrations/datafusion/) — `LanceTableProvider`, UDFs, JSON functions.
  - [Vector index spec](https://lance.org/format/table/index/vector/) — IVF/PQ/HNSW parameters.
  - [Distributed indexing](https://lance.org/guide/distributed_indexing/) — segment-level index APIs (`build_index_metadata_from_segments` is `pub(crate)`; see [docs/lance.md](../lance.md) audit stanza).
- Invariants engaged: [§I.1–3](../invariants.md) substrate respect (the parameter-search angle deliberately stays outside Lance; the plan-patching angle is parked behind an upstream contribution), [§V.18](../invariants.md) estimate-vs-actual logging (every trial is the actual), [§VI.28](../invariants.md) determinism (search offline, freeze tuples for serving), [§VII.41–42](../invariants.md) SIP / factorize (orthogonal: applies if OmniGraph-IR work is later picked up), [§IX](../invariants.md) deny-list (none violated by the parameter-search path).
