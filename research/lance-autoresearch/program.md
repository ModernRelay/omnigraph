# Lance PQ L2 kernel research — agent instructions

You are an autonomous research assistant. Your job is to improve `src/kernels.rs`
so that `cargo run --release --bin run_experiment` reports a **lower
`geomean_ns_per_query`** while:

1. The **correctness phase passes** — your kernel's distance values must match the
   scalar reference within `MAX_ABS_ERR = 1e-4`, and the top-K must be
   tie-tolerant equivalent on every input the bench generates.
2. The `worst_ns_per_query` does **not regress more than 5%** against the
   last-kept kernel — if you win on one (shape × distribution) and lose
   significantly on another, the change isn't a generalizable improvement.

This bench is intentionally **dataset-independent**: there is no fixed dataset.
The correctness oracle is mathematical equivalence to the scalar reference,
checked across multiple PQ shapes and synthetic input distributions
(Gaussian / uniform / sparse / large-dynamic-range / mostly-zero). The speed
oracle is the geomean across multiple shapes × distributions, with worst-case
guarded. A win that depends on a specific data distribution or PQ shape will
fail to clear the bar by construction.

Read this file end-to-end before doing anything else. Then run setup, then the loop.

## Setup (do once at the start of every session)

1. Read these files, in this order:
   - `README.md`
   - `program.md` (this file)
   - `src/lib.rs`
   - `src/kernels.rs` *(the only file you may edit)*
   - `src/reference.rs`
   - `src/inputs.rs`
   - `src/bin/run_experiment.rs`
2. Ensure `results.tsv` exists. If not, create it with this header line:
   ```
   commit	timestamp	correctness	geomean_ns	worst_ns	worst_combo	best_ns	best_combo	peak_mem_mb	total_seconds	keep	description
   ```
3. Run the baseline trial: `cargo run --release --bin run_experiment > run.log 2>&1`.
   Confirm `correctness: pass`. Parse `run.log` and append a row to `results.tsv`
   with `keep=baseline` and `description="seeded scalar PQ-L2 baseline"`. This
   is your reference number.
4. Commit the baseline row with a one-line message like `baseline: <numbers>`.

## What you CAN do

- Modify **`src/kernels.rs`** freely. You may:
  - Pre-process the codebook in `PqKernel::new` (transpose layouts, cache
    `c·c` for the FMA trick, pack the codebook for register-resident lookup,
    etc.). This cost is paid once per dataset and amortized across queries —
    the bench measures per-query, not per-(build + query).
  - Reorder loops, switch internal data layouts, drop down to `std::arch`
    intrinsics under `#[cfg(target_arch = ...)]` gates. **Always keep a
    portable scalar fallback** so the kernel compiles everywhere.
  - Use `unsafe` if needed; document the invariants you're relying on.
  - Mark hot functions `#[inline]`; add private helpers freely.
  - Add `#[cfg(test)] mod tests { ... }` inside `src/kernels.rs` if you want
    in-file property checks.

## What you CANNOT do

- Do **not** modify `src/lib.rs` (`PqShape` and the tolerance constants are
  shared with the immutable scaffolding).
- Do **not** modify `src/bin/run_experiment.rs`, `src/reference.rs`,
  `src/inputs.rs`, `benches/pq_l2.rs`, or `Cargo.toml`.
- Do **not** add new crate dependencies.
- Do **not** alter the public API of `kernels::PqKernel`:
  - `PqKernel::new(shape: PqShape, codebook: &[f32]) -> Self`
  - `PqKernel::shape(&self) -> &PqShape`
  - `PqKernel::distance_table(&self, query: &[f32]) -> Vec<f32>`
  - `PqKernel::probe_top_k(&self, table: &[f32], codes: &[u8], num_vectors: usize, k: usize) -> Vec<(u32, f32)>`
- Do **not** introduce lossy techniques (LUT u8/u16 quantization, asymmetric-
  distance approximation, etc.) — the correctness phase asserts exact-up-to-ε
  match against the scalar reference. If you want to explore a lossy track,
  surface that in a separate kernel and propose a track extension.

## The metric

Minimize `geomean_ns_per_query` (geometric mean of per-query wall-clock across
all timed queries, all shapes, all distributions) subject to:

1. Correctness phase: **pass** (exit-2 otherwise).
2. `worst_ns_per_query` ≤ 1.05 × the last-kept kernel's worst.
3. `total_seconds` ≤ 600.
4. Build is clean: `cargo build --release` succeeds, `cargo clippy --release
   --all-targets -- -D warnings` reports zero issues.

Ties break toward simpler code. If two kernels report the same speed within
~3% noise, prefer fewer lines / less `unsafe`.

## Lance-PQ-specific priors (lossless directions)

These directions are known to pay off without compromising arithmetic accuracy.
Pick one hypothesis at a time; implement; measure; decide.

- **Codebook layout.** The reference layout is `[m][k][d]`. For a fixed query,
  iterating over centroids stays in cache, but the inner loop over `d` is
  short. Transposing to `[m][d][k]` lets you SIMD-load 8 `(query - centroid)`
  lanes across `d` and broadcast over `k`. Do the transpose in `PqKernel::new`
  once.
- **Cache `c·c`.** The diff–square–sum is `(q - c)·(q - c) = q·q - 2qc + c·c`.
  Hoist `q·q` per sub-vector, precompute `c·c` once at codebook-load time.
  Inner loop becomes one FMA (`-2qc + cc`). Watch the sign / accumulator
  ordering so the rounding stays within tolerance.
- **Probe layout.** The probe is dominated by `acc += table[m][codes[off+m]]`
  × `num_sub_vectors`. Transposing codes to `[m][i]` (one row per sub-quantizer,
  contiguous over base index) lets you process up to 32+ vectors per inner
  iteration with `vpgatherdq`-style loads.
- **Top-K integration.** `push()` does a branch + heap sift on every code.
  At 50k probes per query × 9 (shape × dist) combos that's the second-biggest
  cost after the gather. Block the probe (e.g., 512 codes at a time), find the
  local top-K with a branchless pass, then merge into the global heap.
- **Prefetch.** A `_mm_prefetch(codes.as_ptr().add(off + 64), _MM_HINT_T0)`
  ahead of the gather is usually pure win at 50k+ scale where codes don't all
  fit in L2.
- **FMA chains for table build.** The diff–square–sum maps cleanly to FMA on
  AVX2/NEON. Even without intrinsics, structuring the inner loop so `rustc`
  emits FMA helps.
- **Avoid the `Vec` allocation in the hot path.** `distance_table` allocates a
  fresh `Vec<f32>` per call. Returning a fixed-capacity buffer is a public-API
  change you can't make — but you can reuse a thread-local scratch buffer
  internally if it speeds the build.

## The loop

Once setup is done, repeat indefinitely:

1. **Observe state.** Read the last ~5 rows of `results.tsv`. Note which ideas
   have been tried, what won, what regressed. Form a hypothesis with one
   sentence stating the change and the predicted effect on speed and
   correctness.
2. **Edit `src/kernels.rs`.** Keep the diff focused on the one hypothesis.
3. **Build and lint.**
   ```
   cargo build --release
   cargo clippy --release --all-targets -- -D warnings
   ```
   If either fails, fix and try again — do not commit broken state.
4. **Run the trial.**
   ```
   cargo run --release --bin run_experiment > run.log 2>&1
   ```
5. **Parse the result.** Extract `correctness`, `geomean_ns_per_query`,
   `worst_ns_per_query` (with combo), `peak_mem_mb`, `total_seconds`. Compute
   deltas vs. baseline.
6. **Decide keep or revert.**
   - **Keep** iff: `correctness: pass`, geomean strictly better than the
     last-kept row (allow ~1% noise band), and `worst_ns_per_query` ≤ 1.05 ×
     last-kept's worst.
   - **Revert** otherwise: `git restore src/kernels.rs` (or commit and
     `git revert` if you want the revert in history). Note what failed.
7. **Log.** Append one row to `results.tsv`:
   ```
   <short_sha>	<iso8601>	<correctness>	<geomean_ns>	<worst_ns>	<worst_combo>	<best_ns>	<best_combo>	<peak_mem>	<elapsed>	<keep|revert>	<one-line description>
   ```
8. **Commit.** One-line message describing the change and the headline number,
   e.g. `transpose codebook in new(); 18.2k → 14.1k geomean ns (worst -8%)`.

## Hygiene

- Always commit `src/kernels.rs` changes; never commit `results.tsv` or
  `run.log` (they're gitignored).
- If a change fails to build, do not commit. Iterate until it builds, or
  revert cleanly.
- If two consecutive ideas regress, take a beat: re-read the last ~10 rows of
  `results.tsv` and update your mental model before proposing the next.
- Per-trial cap: 10 minutes. If `cargo run` is still going after 10 min, kill it
  and mark the trial as `timeout`.

## Never stop

Keep going until interrupted. Each loop iteration is one hypothesis, one edit,
one measurement, one commit. No multi-step plans across iterations.
