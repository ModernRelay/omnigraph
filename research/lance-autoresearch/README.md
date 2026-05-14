# lance-autoresearch

An autoresearch-style harness for evolving [Lance](https://github.com/lance-format/lance)
PQ L2 distance kernels via LLM coding agents (Claude Code, Codex, Cursor).

Modeled on Andrej Karpathy's
[`nanochat-research`](https://x.com/karpathy/status/1855651423497650238)
three-file contract:

- **Immutable bench** — `src/bin/run_experiment.rs` + `src/inputs.rs` +
  `src/reference.rs`. The agent cannot touch these.
- **Mutable kernel** — `src/kernels.rs`. The agent's playground. Starts as a
  scalar baseline matching Lance's PQ L2 algorithm shape; the agent's job is to
  beat it.
- **Human-iterated program** — `program.md`. The "skill" the agent reads at
  the start of every session. The human refines it between runs.

## Dataset-independent by design

Every other ANN benchmark you've seen is "compete on this fixed dataset"
(SIFT1M, GIST1M, DEEP1B). That conflates two things: *kernel correctness*
(the math) and *kernel speed under one specific data distribution*. An LLM
agent given recall@K as the oracle has incentive to overfit to the dataset's
quirks.

We split them:

- **Correctness** = bit-equivalent (`max_abs_err ≤ 1e-4`) match to a scalar
  reference kernel, on diverse generated inputs (Gaussian, uniform, sparse,
  large-dynamic-range, mostly-zero) × multiple PQ shapes. This is mathematical
  equivalence; there's no dataset to overfit. Lossy techniques fail this gate.
- **Speed** = geomean ns/query across multiple PQ shapes ×
  multiple data distributions. A kernel that wins on one distribution and
  regresses on another fails the worst-case guard.

By construction, an "improvement" generalizes across distributions and shapes.
There is no `wget sift.tar.gz` step; the harness is fully self-contained.

## Why a separate repo

OmniGraph (the graph engine that motivated this) pins Lance at a released
version and consumes its kernels via the public crate API. Improvements live one
layer below: in Lance itself. A standalone repo with no OmniGraph dep keeps the
optimization target pure (only the kernel changes), keeps the license clean for
upstream contribution (dual MIT/Apache-2.0 → Apache-2.0 PRs to Lance), and
keeps the agent's working set tiny.

## Quick start

```bash
cargo run --release --bin run_experiment

# Or run with Claude Code / Codex:
#    Open the repo in your agent of choice and prompt:
#       Hi, have a look at program.md and let's kick off a new experiment.
```

## File ownership

| File | Mutability | Edited by |
|---|---|---|
| `src/kernels.rs` | **mutable** | the agent |
| `src/bin/run_experiment.rs` | immutable | — |
| `src/reference.rs` | immutable | — |
| `src/inputs.rs` | immutable | — |
| `src/lib.rs` | immutable (shared types) | — |
| `benches/pq_l2.rs` | immutable | — |
| `program.md` | human-iterated | the human, between runs |
| `results.tsv` | append-only | the agent, per trial (gitignored) |

## The metric

`run_experiment` runs two phases per trial: a correctness check and a
multi-shape × multi-distribution speed measurement. Output looks like:

```
correctness:           pass
---
correctness:           pass
shapes_tested:         (128,16,256) (256,16,256) (768,96,256)
distributions_tested:  clustered uniform sparse
geomean_ns_per_query:  18234
worst_ns_per_query:    24515 ((768,96,256), sparse)
best_ns_per_query:     12876 ((128,16,256), clustered)
per_combo_geomean_ns:
  (128,16,256) clustered  -> 12876 ns
  (128,16,256) uniform    -> 13441 ns
  ...
peak_mem_mb:           28.4
total_seconds:         12.3
```

A kernel is "kept" iff:

- Correctness phase passes (mathematical equivalence to scalar reference)
- `geomean_ns_per_query` strictly better than the previous best-kept kernel
- `worst_ns_per_query` ≤ 1.05 × the previous best-kept kernel's worst
- `total_seconds` ≤ 600

See `program.md` for the full loop spec.

## Upstream contribution path

When a commit clears the keep bar by a meaningful margin (≥10% geomean
speedup with worst-case guard intact), the human reviews the diff, ports the
technique against [`lance-format/lance`](https://github.com/lance-format/lance)
HEAD, runs Lance's own test suite, and opens a PR. Because `src/kernels.rs` is
dual MIT/Apache-2.0 licensed and algorithmically modeled on Lance's existing
path, the upstream PR inherits Apache-2.0 cleanly.

## License

Dual-licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
