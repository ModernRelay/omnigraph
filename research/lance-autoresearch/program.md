# Lance PQ L2 kernel research — agent instructions

You are an autonomous research assistant. Your job is to improve the kernel(s) in
`src/kernels.rs` so that `cargo run --release --bin run_experiment` reports a
**lower `geomean_ns_per_query`** while keeping **`recall_at_10` within 0.005 of
the seeded baseline** (and never below the hard floor 0.50).

Read this file end-to-end before doing anything else. Then run setup, then the loop.

## Setup (do once at the start of every session)

1. Read these files, in this order:
   - `README.md`
   - `program.md` (this file)
   - `src/lib.rs`
   - `src/kernels.rs` *(the only file you may edit)*
   - `src/bin/run_experiment.rs`
   - `src/fixture.rs`
2. Confirm fixtures are present. SIFT1M lives under `~/.cache/lance-autoresearch/`.
   If it's missing, the bench will fall back to a deterministic synthetic dataset
   — that's fine for the loop; mention it in your log. If you want SIFT1M, run
   `bash scripts/prepare_fixtures.sh` (one-time, ~5–10 min, ~250 MB download).
3. Ensure `results.tsv` exists. If not, create it with this header line:
   ```
   commit	timestamp	source	num_base	recall_at_10	geomean_ns_per_query	peak_mem_mb	total_seconds	keep	description
   ```
4. Run the baseline trial: `cargo run --release --bin run_experiment > run.log 2>&1`.
   Parse `run.log` and append a row to `results.tsv` with `keep=baseline`,
   `description="seeded scalar PQ-L2 baseline"`. This is your reference number.
5. Commit the baseline row with a one-line message like `baseline: <numbers>`.

## What you CAN do

- Modify **`src/kernels.rs`** freely. You may:
  - Reorder loops, change iteration order over codes or sub-vectors.
  - Switch to SIMD via `std::arch` (`x86_64::_mm256_*`, `aarch64::neon::*`),
    behind `#[cfg(target_arch = "...")]` gates. Always keep a portable scalar
    fallback so the kernel compiles everywhere.
  - Reshape internal data: transpose the codebook, pack the distance LUT into
    `u8`/`u16` for `pshufb`-style lookup, group codes for SIMD gather.
  - Use `unsafe` if needed; document the invariants you're relying on.
  - Mark hot functions `#[inline]` or split them; add private helpers freely.
- Add `#[cfg(test)] mod tests { ... }` inside `src/kernels.rs` if you want
  property checks against the scalar path.

## What you CANNOT do

- Do **not** modify `src/lib.rs` (changes `DIM` / `NUM_SUB_VECTORS` / `NUM_CENTROIDS` /
  `TOP_K` — these pin the fixture geometry).
- Do **not** modify `src/bin/run_experiment.rs`, `src/reference.rs`, `src/fixture.rs`,
  `benches/pq_l2.rs`, `scripts/prepare_fixtures.sh`, or `Cargo.toml`.
- Do **not** add new crate dependencies (the bench's external surface is intentionally
  minimal — only `anyhow`, plus `criterion` as a dev-dep).
- Do **not** delete or alter the public API of `kernels.rs`:
  - `pub type DistanceTable`
  - `pub fn compute_distance_table_l2(query: &[f32], codebook: &[f32]) -> DistanceTable`
  - `pub fn probe_pq_l2_top_k(table: &DistanceTable, codes: &[u8], num_vectors: usize, out: &mut TopKHeap)`
  - `pub struct TopKHeap` with `new() / push / into_sorted`

## The metric

Minimize `geomean_ns_per_query` (geometric mean of per-query wall-clock from the
benched queries, rounded to a u64 ns) subject to:

1. `recall_at_10 >= baseline_recall_at_10 - 0.005`
2. `recall_at_10 >= 0.50` (hard floor; below this the bench exits non-zero)
3. `total_seconds <= 600`
4. Build is clean: `cargo build --release` succeeds, `cargo clippy --release -- -D warnings`
   reports zero issues. (Run `cargo clippy --release` before each commit.)

Ties break toward simpler code. If two kernels report the same speed within
noise (~3%), prefer the one with fewer lines or less `unsafe`.

## Lance-PQ-specific priors

These are the directions known to pay off on this kernel shape. Don't pursue all
of them at once — pick one hypothesis, implement, measure, decide.

- **Codebook layout for the table-build step.** The reference layout is
  `[m][k][d]`. For a fixed query, iterating over centroids stays in cache, but
  the inner loop over `d` is short (8 floats). An `[m][d][k]` transpose can let
  you SIMD-load 8 `(query - centroid)` lanes across `d` and broadcast over `k`.
- **LUT packing for the probe step.** The probe is dominated by `acc +=
  table[m][codes[off+m]]` × 16. Two well-known tricks:
  - Pack each `table[m]` row into 256 × `f16` or 256 × `u8` (quantized post-build)
    to fit the LUT in cache and enable `vpgatherdq` / `pshufb`.
  - Reorder code storage to `[m][i]` (transpose codes by sub-quantizer) so each
    `m` step is a contiguous gather over up to 32 vectors at once.
- **Top-K integration.** `push()` does a branch + heap sift on every code; for a
  1M-row probe this is the second-biggest cost after the gather. Consider:
  - Skip the heap entirely when the running `acc` is already `> current_max`
    (early termination, but only if your accumulator order makes that cheap).
  - Block the probe (e.g., 1024 codes at a time), find the local top-K with a
    branchless scan, then merge into the global heap.
- **Prefetch.** A `_mm_prefetch(codes.as_ptr().add(off + 64), _MM_HINT_T0)` ahead
  of the gather is usually pure win at 1M scale where codes don't all fit in L2.
- **FMA in the table build.** The diff–square–sum sequence is
  `(q - c)·(q - c)` per element — that's `(q*q) - 2qc + c*c`. You can hoist
  `q*q` once per sub-vector and precompute `c*c` once at codebook-load time
  (if you cache it as a side table), reducing the inner loop to one FMA.
  But: caching `c*c` requires a one-time setup step, which has to live in
  `kernels.rs` since you cannot touch the fixture; either lazy-init via
  `OnceLock<Vec<f32>>` or rebuild every call (probably not worth it).

## The loop

Once setup is done, repeat indefinitely:

1. **Observe state.** Read the last ~5 rows of `results.tsv`. Note which ideas
   have been tried, what won, what regressed. Form a hypothesis with one
   sentence stating the change and the predicted effect on speed and recall.
2. **Edit `src/kernels.rs`.** Keep the diff focused on the one hypothesis.
3. **Build and lint.** Run:
   ```
   cargo build --release
   cargo clippy --release --all-targets -- -D warnings
   ```
   If either fails, fix and try again — do not commit broken state.
4. **Run the trial.**
   ```
   cargo run --release --bin run_experiment > run.log 2>&1
   ```
5. **Parse the result.** Extract `recall_at_10`, `geomean_ns_per_query`,
   `peak_mem_mb`, `total_seconds` from `run.log`. Compute the deltas vs. baseline.
6. **Decide keep or revert.**
   - **Keep** iff: recall within tolerance, speed strictly better than the
     last-kept row (allow ~1% noise band), and total time within budget.
   - **Revert** otherwise: `git restore src/kernels.rs` (or commit and `git
     revert` if you want the revert in history). Note what failed.
7. **Log.** Append one row to `results.tsv`:
   ```
   <short_sha>	<iso8601>	<source>	<num_base>	<recall>	<geomean_ns>	<peak_mem>	<elapsed>	<keep|revert>	<one-line description>
   ```
8. **Commit.** Use a one-line message describing the change and the headline
   number, e.g. `transpose codebook; 184k → 142k ns/query (recall 0.94)`.

## Hygiene

- Always commit `src/kernels.rs` changes; never commit `results.tsv` or `run.log`
  (they're gitignored).
- If a change fails to build, do not commit. Iterate until it builds, or revert
  cleanly.
- If two consecutive ideas regress, take a beat: re-read the last ~10 rows of
  `results.tsv` and update your mental model before proposing the next.
- Per-trial cap: 10 minutes. If `cargo run` is still going after 10 min, kill it
  and mark the trial as `timeout`.

## Never stop

Keep going until interrupted. Each loop iteration is one hypothesis, one edit,
one measurement, one commit. No multi-step plans across iterations.
