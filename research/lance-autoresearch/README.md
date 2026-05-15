# lance-autoresearch

A multi-target workspace for evolving [Lance](https://github.com/lance-format/lance)
hot-path kernels via LLM coding agents (Claude Code, Codex, Cursor),
in the style of Andrej Karpathy's
[`nanochat-research`](https://x.com/karpathy/status/1855651423497650238)
single-agent autoresearch loop.

Each target is an independent Rust crate under `crates/`:

| Target | Status | Lance source area | What's optimized |
|---|---|---|---|
| [`crates/pq-l2`](crates/pq-l2) | landed | `lance-linalg::distance::l2`, PQ probe | PQ L2 distance: build LUT, probe codes, top-K |
| `crates/pq-cosine`     | candidate (A1) | `lance-linalg::distance::cosine` | PQ cosine distance |
| `crates/pq-dot`        | candidate (A1) | `lance-linalg::distance::dot` | PQ dot-product distance |
| `crates/ivf-partition` | candidate (A2) | `lance-index::vector::ivf` partition select | IVF partition selection (centroid scan) |
| `crates/fts-bm25`      | candidate (A3) | `lance-index::scalar::inverted` BM25 | FTS BM25 scoring inner loop |
| `crates/bitpack`       | candidate (A4) | `lance-encoding::encodings::bitpack` | Bitpack integer decode |
| `crates/dictionary`    | candidate (A5) | `lance-encoding::encodings::dictionary` | Dictionary decode |
| `crates/fsst`          | candidate (A6) | `lance-encoding::encodings::fsst` | FSST string decode |
| `crates/take`          | candidate (A7) | `lance-core::utils::take` | Take / gather kernel |
| `crates/predicate`     | candidate (A8) | `lance-datafusion` filter eval | Predicate evaluation kernels |
| `crates/posting-intersect` | candidate (A9) | `lance-index::scalar::inverted` | Posting list intersection (FTS AND) |
| `crates/topk-merge`    | candidate (A10) | scan-merge | Top-K k-way merge |

The candidate targets are documented in [`docs/targets/`](docs/targets/) and can
be added by following [`docs/adding-a-target.md`](docs/adding-a-target.md). The
single landed target (`pq-l2`) proves the harness shape; the candidates wait
for an agent to spin them up.

## The contract every target follows

Karpathy's three-file shape, applied per target:

| File (per target crate) | Mutability | Edited by |
|---|---|---|
| `src/kernels.rs` | **mutable** | the agent |
| `src/reference.rs`, `src/inputs.rs`, `src/lib.rs`, `src/bin/run_experiment.rs`, `benches/*.rs` | immutable | — |
| `program.md` | human-iterated | the human, between runs |
| `results.tsv` | append-only | the agent, per trial (gitignored) |

The shared utilities — deterministic PRNG, geomean, peak-RSS readback,
tolerance constants, time-budget — live in [`crates/harness-common`](crates/harness-common/src/lib.rs)
and are consumed by every target. There is intentionally **no `Target` trait**:
decode-kernel signatures and distance-kernel signatures are different enough
that a unifying trait would either bloat or require erased boxing. Each target
is its own natural shape; the shared crate is plumbing only.

The shared loop conventions every target's `program.md` inherits live in
[`HARNESS.md`](HARNESS.md). Per-target priors and API specifics live in each
target's own `program.md`.

## Dataset-independent by design

Every other ANN benchmark you've seen is "compete on this fixed dataset"
(SIFT1M, GIST1M, DEEP1B). That conflates two things: *kernel correctness* (the
math) and *kernel speed under one specific data distribution*. An LLM agent
given recall@K as the oracle has incentive to overfit to the dataset's quirks.

We split them, every target:

- **Correctness** = bit-equivalent (`max_abs_err ≤ 1e-4` for floats; bitwise for
  integer/byte kernels) match to a scalar reference, on diverse generated
  inputs. Mathematical equivalence; no dataset to overfit. Lossy techniques fail
  this gate.
- **Speed** = geomean ns/operation across multiple shape × distribution
  combinations, with worst-case guard. A kernel that wins on one distribution
  and regresses on another fails to keep.

By construction, an "improvement" generalizes across distributions and shapes.
There is no `wget sift.tar.gz` step; every target is fully self-contained.

## Why a separate repo (and a workspace, not a single crate)

OmniGraph (the graph engine that motivated this) pins Lance at a released
version and consumes its kernels via the public crate API. Improvements live
one layer below: in Lance itself. A standalone repo with no OmniGraph dep keeps
the optimization target pure (only the kernel changes), keeps the license clean
for upstream contribution (dual MIT/Apache-2.0 → Apache-2.0 PRs to Lance), and
keeps each agent's working set tiny.

**Workspace not single-crate** because per-target deps differ — FSST decode
will want a different dependency set than PQ kernels — and the agent's edits
to one target's `kernels.rs` must not collide with another's lib path. Each
target is buildable, testable, and runnable in isolation: `cd crates/<target>
&& cargo run --release --bin run_experiment`.

## Quick start

```bash
# Run the landed PQ L2 target's baseline.
cargo run --release --bin run_experiment -p pq-l2

# Or with Claude Code / Codex, working on one target:
cd crates/pq-l2
# Open in your agent of choice and prompt:
#   Hi, have a look at program.md and let's kick off a new experiment.

# Add a new target (see docs/adding-a-target.md):
cp -r crates/pq-l2 crates/pq-cosine
# ... edit Cargo.toml name, kernels.rs / reference.rs / inputs.rs / program.md
```

## Repo layout

```
lance-autoresearch/
├── Cargo.toml                         # workspace root
├── README.md                          # you are here
├── HARNESS.md                         # shared loop contract every target inherits
├── LICENSE-MIT, LICENSE-APACHE        # dual-licensed (Apache compat for Lance PRs)
├── crates/
│   ├── harness-common/                # shared: SplitMix64, geomean, peak RSS, tolerance, time budget
│   │   └── src/{lib,prng,stats,sysinfo,tolerance}.rs
│   └── pq-l2/                         # landed target
│       ├── Cargo.toml
│       ├── program.md                 # this target's agent skill
│       ├── src/
│       │   ├── lib.rs                 # PqShape + module wiring (immutable)
│       │   ├── kernels.rs             # MUTABLE — agent's playground
│       │   ├── reference.rs           # IMMUTABLE — scalar reference, oracle helpers
│       │   ├── inputs.rs              # IMMUTABLE — diverse test-data generators
│       │   └── bin/run_experiment.rs  # IMMUTABLE — per-trial entry point
│       └── benches/pq_l2.rs           # criterion benchmark (immutable)
└── docs/
    ├── design.md                      # rationale for the workspace shape
    ├── adding-a-target.md             # workflow for spinning up a new target
    └── targets/
        └── pq-l2.md                   # capsule: upstream Lance pointers, oracle, status
```

## Upstream contribution path

When a commit on any target clears the keep bar by a meaningful margin
(≥10% geomean speedup with worst-case guard intact), the human reviews the
diff, ports the technique against
[`lance-format/lance`](https://github.com/lance-format/lance) HEAD, runs
Lance's own test suite, and opens a PR. Because the workspace is dual
MIT/Apache-2.0 licensed and each target's kernel is algorithmically modeled on
Lance's existing path, the upstream PR inherits Apache-2.0 cleanly.

## License

Dual-licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
