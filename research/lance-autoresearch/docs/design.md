# Design — why the workspace is shaped this way

This document records the rationale for the multi-target workspace shape so
future contributors don't relitigate the early decisions.

## The thing we're building

A multi-target harness for LLM-driven optimization of Lance hot-path kernels.
"Multi-target" because Lance has many such kernels — distance kernels in
`lance-linalg`, decoders in `lance-encoding`, scan/merge kernels — and the
right harness shape is identical across them: bit-exact correctness oracle,
geomean-across-distributions speed metric, single-agent autoresearch loop.

The original [research note](../../docs/research/llm-evolutionary-sampling.md)
enumerates ten such candidates (A1–A10) clustered by Lance crate. The first
landed (`pq-l2`) proves the harness shape; the rest follow the same template.

## Decision: workspace, not single crate

A single crate exposing multiple binaries (`run_experiment_pq_l2`,
`run_experiment_bitpack`, ...) was the obvious-looking alternative. Rejected
for three reasons:

1. **Per-target deps differ.** FSST decode wants different deps than PQ
   kernels (a string-compression library vs. just `f32` math). A single
   `Cargo.toml` would either bundle every target's deps into every build or
   require fine-grained features. Workspaces give per-target `Cargo.toml`
   for free.

2. **Edit isolation.** The agent edits one target's `kernels.rs` at a time.
   In a single crate, `kernels.rs` files would collide on path or have to live
   in target-specific submodules with target-specific naming. Per-target
   crates put `src/kernels.rs` at the natural location every time and let the
   agent navigate one tree per session.

3. **Build / test isolation.** `cargo build -p pq-l2` builds only what's
   needed for the PQ L2 target; `cargo test -p pq-l2` runs only its tests.
   The agent's iteration loop is faster because it doesn't pay for unrelated
   targets' compile time.

The downside — workspace boilerplate, per-target `Cargo.toml`, the empty
`[workspace]` block at the workspace root that prevents cargo from walking up
to the parent omnigraph workspace — is a one-time cost. Per-target overhead
of adding a new target is one `cp -r` plus path edits.

## Decision: shared `harness-common` crate, no `Target` trait

A `Target` trait was the obvious-looking other alternative — express the
common loop generically, plug in target-specific types. Rejected because:

1. **Kernel signatures vary too much for a single trait shape.** PQ
   `probe_top_k` returns `Vec<(u32, f32)>`. Bitpack decode returns an
   `IntArray`. FSST decode returns `Vec<u8>`. Predicate evaluation returns a
   `BooleanArray`. A unifying trait would need erased boxing or a wide
   associated-type surface, both of which obscure the actual hot path the
   agent is editing.

2. **The orchestration that *is* shared is small.** A deterministic PRNG
   (~30 lines), a geomean (~10 lines), peak RSS readback (~20 lines), four
   tolerance constants. Total ~70 lines of shared code. Building a trait
   abstraction over 70 lines costs more than it saves.

3. **The output format isn't worth sharing.** Each target's
   `run_experiment.rs` prints a fixed-format result block; the *fields*
   differ per target (PQ shapes vs bit widths vs distribution kinds). A
   shared formatter would be either trivial wrapping of `println!` (no
   value) or a complicated builder API (negative value).

`harness-common` therefore exposes plumbing only: `SplitMix64`, `geomean`,
`peak_rss_mb`, `MAX_ABS_ERR`, `TOPK_DIST_TOL`, `TIME_BUDGET_SECS`. Each
target consumes what it needs. The shared loop contract is documented in
`HARNESS.md`, not encoded in code.

## Decision: per-target `program.md` + shared `HARNESS.md`

The agent reads two files at session start:

- `HARNESS.md` (workspace-level) — universal: the loop, the metric, the
  edit-permission table, hygiene rules.
- `crates/<target>/program.md` (per-target) — specific: the kernel API the
  agent must keep stable, target-specific priors (which SIMD intrinsics tend
  to win on this kernel shape), the `results.tsv` column header.

The shape mirrors how Karpathy's `nanochat-research` `program.md` works,
factored across the dimension that varies (per target) vs. doesn't (the loop
itself). Two files instead of one because copy-pasting the universal loop
into every `program.md` makes them drift.

## Decision: dataset-independent oracle every target

The first iteration of the harness used recall@K vs. SIFT1M as the
correctness oracle. We replaced it with bit-exact (or near-bit-exact for
floats) match against a scalar reference because:

1. The agent had incentive to overfit lossy approximations to the dataset's
   cluster structure, even though we didn't ask for that.
2. SIFT1M is 250 MB and a hassle to download; the harness benefited from
   being self-contained.
3. Mathematical equivalence is a strictly stronger contract than recall
   preservation: if the kernel is bit-equivalent to the scalar reference,
   recall is automatically identical because the distance values are the
   same. There's nothing recall@K catches that bit-exactness doesn't.

This decision generalizes to every target. Decode kernels get strict bitwise
equality (no float arithmetic involved). Distance and BM25 kernels get
`max_abs_err ≤ 1e-4` (loose enough for SIMD-accumulator reordering, tight
enough for real bugs). Targets that genuinely require lossy techniques to
get headroom — there might be some; LUT u8 quantization in PQ is one — go
in a separate "lossy track" with a recall-based oracle on diverse datasets,
not the bit-exact track.

## Decision: per-target speed measurement spans multiple shapes × distributions

A single dataset would let an agent overfit to that dataset's distribution.
Each target's `inputs.rs` therefore generates speed workloads across:

- Multiple **shapes** of the kernel's domain (PQ: `(dim, num_sub_vectors,
  num_centroids)`; bitpack: bit width; etc.). Captures how the kernel
  performs at different sizes Lance users actually encounter.
- Multiple **data distributions** (Gaussian / uniform / sparse for floats;
  uniform / skewed / clustered for integers; etc.). Captures whether the
  kernel's win is data-distribution-conditional.

The keep gate uses geomean across all (shape × distribution) combos with a
worst-case guard: a kernel that wins on one combo and regresses ≥5% on
another fails to keep, even if the geomean improves. This forces wins to
generalize.

## What's deliberately not abstracted

- **Output format.** Each target prints its own field block. See above.
- **`TopKHeap` and other small data structures.** When two targets need a
  `TopKHeap`, the second one copies the first's. Three copies of a 30-line
  struct is cheaper than one trait-erased indirection.
- **Test data shapes.** Each target's `inputs.rs` knows its own kernel's
  fixture shape. Sharing would require a generic `Fixture<Kernel>` trait,
  which would either be too narrow (forces every kernel into a `query +
  workload` shape) or too wide (gives up the type safety that makes the
  bench's correctness check obvious).

## When to revisit

If the workspace grows past ~6 active targets and we notice we're
copy-pasting more than ~50 lines of `run_experiment.rs` boilerplate per new
target, consider extracting a shared `RunExperiment` helper that takes
closures for the correctness and speed phases. Don't pre-extract — wait
until the duplication is real and visible.

If we add a target that genuinely doesn't fit the autoresearch loop (eval
crosses ~30s; tournament sampling becomes the right control loop), it
belongs in a separate workspace, not this one. The boundary line is the
loop shape, not the target type.
