# Target: `pq-l2`

PQ L2 distance kernel for f32 dense vectors — the asymmetric-distance compute
that runs on every `IvfPq` / `IvfHnswPq` ANN query in Lance.

## Status

**Landed.** Baseline scalar kernel committed; the agent's job is to find
generalizable speedups against it.

## What's optimized

Two functions in `crates/pq-l2/src/kernels.rs`:

- `PqKernel::distance_table(query, &mut out)` — writes the asymmetric
  distance table (`[num_sub_vectors][num_centroids]`) for one query against
  the codebook into a caller-provided `&mut [f32]` buffer (the bench
  pre-allocates and reuses one buffer per workload so allocator cost stays
  out of the per-query timing). Cost:
  `num_sub_vectors × num_centroids × sub_vector_dim` MAC ops per query.
- `PqKernel::probe_top_k(table, codes, num_vectors, k)` — probes
  `num_vectors` PQ-encoded vectors, accumulates per-vector distance via
  `num_sub_vectors` table lookups, returns top-K. Cost:
  `num_vectors × num_sub_vectors` lookups + heap maintenance per query.
  This is the dominant cost at typical scales.

`PqKernel::new(shape, codebook)` is also editable — the agent may pre-process
the codebook (transpose layout, cache `c·c` for the FMA trick, pack the LUT)
and amortize over queries; build cost is excluded from per-query timing.

## Upstream Lance source

Algorithmically modeled on `lance-linalg::distance::l2` plus the PQ
asymmetric-distance compute in `lance::index::vector::pq`. Specifically the
f32 dense path; the byte / fixed-point variants are out of scope for this
target.

When porting a winning kernel upstream:
- File: `lance-linalg/src/distance/l2.rs` and the L2-specific path in
  `lance/src/index/vector/pq.rs`.
- License: Apache-2.0 (matches our dual MIT/Apache-2.0 → upstream takes
  the Apache half).

## Oracle

**Float-accumulator-tolerance match against scalar reference.** Per
`harness_common::MAX_ABS_ERR = 1e-4`:

- Distance table values must match the scalar reference within `1e-4` per
  element. Loose enough for legal SIMD-accumulator reordering, tight enough
  to catch real arithmetic bugs.
- Top-K results compared with `harness_common::TOPK_DIST_TOL = 1e-4` plus
  tie-tolerant id substitution (any permutation within a tied-distance band
  is accepted).

The correctness phase asserts both on every input combination — five input
distributions × three PQ shapes = 15 cases per trial.

## Speed workload

Three shapes:
- `(128, 16, 256)` — SIFT-like; sub_vector_dim = 8
- `(256, 16, 256)` — sub_vector_dim = 16
- `(768, 96, 256)` — BERT-base-like; large codebook

Three data distributions:
- `Clustered` — 32 cluster centers, low intra-cluster noise
- `Uniform` — uniform on [-1, 1]
- `Sparse` — 90% zeros + 10% Gaussian

Per (shape × distribution): 20,000 base vectors PQ-encoded, 32 queries
timed. Total trial wall-clock: ~30–60s on a developer laptop.

## Output fields

```
correctness:           pass | fail
shapes_tested:         (128,16,256) (256,16,256) (768,96,256)
distributions_tested:  clustered uniform sparse
geomean_ns_per_query:  <u64>
worst_ns_per_query:    <u64> (<shape>, <dist>)
best_ns_per_query:     <u64> (<shape>, <dist>)
per_combo_geomean_ns:
  (...)
peak_mem_mb:           <f64>
total_seconds:         <f64>
```

## Known headroom (priors for the agent)

See `crates/pq-l2/program.md` "Lance-PQ-specific priors" for the canonical
list. Highlights:

- Codebook layout transpose (`[m][k][d]` → `[m][d][k]`) for SIMD-broadcast
  table build.
- Cache `c·c` per centroid in `new()` so the inner loop is `q·q − 2qc + c·c`
  (one FMA chain).
- Probe-side code transpose so the inner loop processes 32+ vectors per
  iteration via gather.
- Top-K block-then-merge instead of per-vector heap insert.
- Prefetch on `codes[i+64]` ahead of gather.
