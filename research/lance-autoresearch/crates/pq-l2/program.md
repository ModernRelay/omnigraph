# Target: PQ L2 — agent instructions

This is the per-target overlay on top of [`../../HARNESS.md`](../../HARNESS.md).
Read **HARNESS.md first** for the universal loop contract (what's editable,
the metric, the loop, hygiene, never stop). This file adds the PQ-L2-specific
API spec and priors.

## Setup (once per session)

1. Read in this order:
   - `../../HARNESS.md`
   - `../../README.md`
   - `program.md` (this file)
   - `src/lib.rs`
   - `src/kernels.rs` *(the only file you may edit)*
   - `src/reference.rs`
   - `src/inputs.rs`
   - `src/bin/run_experiment.rs`
2. Ensure `results.tsv` exists. If not, create it with this header:
   ```
   commit	timestamp	correctness	geomean_ns	worst_ns	worst_combo	best_ns	best_combo	peak_mem_mb	total_seconds	keep	description
   ```
3. Baseline trial:
   ```
   cargo run --release --bin run_experiment > run.log 2>&1
   ```
   Append a row tagged `keep=baseline`, commit it.

## Public API contract (must remain stable)

The bench imports these from `crate::kernels`. You may NOT change their
signatures. You MAY add private helpers, internal data layouts, `unsafe`
blocks, `std::arch` intrinsics under `#[cfg(target_arch = ...)]` gates,
pre-computed state inside `PqKernel`, etc.

```rust
pub struct PqKernel { /* agent's private fields */ }

impl PqKernel {
    pub fn new(shape: PqShape, codebook: &[f32]) -> Self;
    pub fn distance_table(&self, query: &[f32]) -> Vec<f32>;
    pub fn probe_top_k(&self, table: &[f32], codes: &[u8], num_vectors: usize, k: usize) -> Vec<(u32, f32)>;
}
```

Pre-processing in `new` is free — the bench measures `distance_table +
probe_top_k` per query, not per (build + query). Codebook transposes,
cached `c·c`, packed LUTs, etc., should live in `new`.

## What you can / cannot do

(See HARNESS.md for the universal table; this is the PQ-L2 specific
addition.)

- **Cannot** change `PqShape` or the constants in `lib.rs`. They define
  the optimization target.
- **Cannot** introduce lossy techniques (LUT u8/u16 quantization, asymmetric
  approximation, anything that drops bits relative to the scalar reference).
  The correctness phase asserts `max_abs_err ≤ 1e-4` against the scalar
  reference; lossy techniques fail this gate. If you want to explore a lossy
  track, propose it to the human as a separate kernel surface.
- **Can** mark hot functions `#[inline]`, split them, add private helpers.
- **Can** add `#[cfg(test)] mod tests { ... }` inside `kernels.rs` for in-file
  property checks against the scalar path.

## Lance-PQ-specific priors

These are the directions that pay off on this kernel shape without
compromising arithmetic accuracy. Pick one hypothesis per trial; don't try
to combine multiple ideas at once.

- **Codebook layout transpose.** The reference layout is `[m][k][d]`.
  Transposing to `[m][d][k]` lets you SIMD-load 8 `(query - centroid)` lanes
  across `d` and broadcast over `k`. Do the transpose in `PqKernel::new` once.
- **Cache `c·c` per centroid.** The diff–square–sum is
  `(q - c)·(q - c) = q·q - 2qc + c·c`. Hoist `q·q` per sub-vector,
  precompute `c·c` once at `new()` time, store next to the codebook. Inner
  loop becomes one FMA. Watch sign / accumulator ordering so rounding stays
  within `MAX_ABS_ERR`.
- **Probe-side code transpose.** Probe is dominated by
  `acc += table[m][codes[off+m]]` × `num_sub_vectors`. Transposing codes to
  `[m][i]` (one row per sub-quantizer, contiguous over base index) lets you
  process 32+ vectors per inner iteration with `vpgatherdq`-style loads.
- **Top-K block-then-merge.** `push()` does a branch + heap sift on every
  code. At 20k probes per query × 9 (shape × dist) combos that's the
  second-biggest cost after the gather. Block the probe (e.g., 512 codes at
  a time), find the local top-K with a branchless pass, then merge into the
  global heap.
- **Prefetch.** `_mm_prefetch(codes.as_ptr().add(off + 64), _MM_HINT_T0)`
  ahead of the gather is usually pure win at 20k+ scale.
- **FMA chains for table build.** The diff–square–sum maps cleanly to FMA
  on AVX2/NEON. Even without intrinsics, structuring the inner loop so
  `rustc` emits FMA helps.
- **Avoid the `Vec` allocation in the hot path.** `distance_table` allocates
  a fresh `Vec<f32>` per call. The public API is fixed (returns `Vec<f32>`),
  but you can reuse a thread-local scratch buffer internally and copy to a
  `Vec` at the boundary if it speeds the build.
