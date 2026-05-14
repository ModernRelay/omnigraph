# lance-autoresearch

An autoresearch-style harness for evolving [Lance](https://github.com/lance-format/lance)
PQ L2 distance kernels via LLM coding agents (Claude Code, Codex, Cursor).

Modeled on Andrej Karpathy's
[`nanochat-research`](https://x.com/karpathy/status/1855651423497650238)
three-file contract:

- **Immutable bench** — `src/bin/run_experiment.rs` + `src/fixture.rs` + `src/reference.rs`.
  The agent cannot touch these.
- **Mutable kernel** — `src/kernels.rs`. The agent's playground. Starts as a clean
  scalar PQ L2 implementation matching Lance's algorithm; the agent's job is to
  beat it.
- **Human-iterated program** — `program.md`. The "skill" the agent reads at the
  start of every session. The human refines it between runs.

The optimization target is the PQ L2 distance kernel for f32 dense vectors on
SIFT1M-shaped data (128-d, 16 sub-vectors × 256 centroids, 8-bit codes, top-10
retrieval). The eval oracle is **recall@10 against SIFT1M's published ground
truth** at fixed kernel shape, with `geomean_ns_per_query` as the speed metric.

## Why a separate repo

OmniGraph (the graph engine that motivated this) pins Lance at a released
version and consumes its kernels via the public crate API. Improvements live one
layer below: in Lance itself. A standalone repo with no OmniGraph dep keeps the
optimization target pure (only the kernel changes), keeps the license clean for
upstream contribution (dual MIT/Apache-2.0 → Apache-2.0 PRs to Lance), and
keeps the agent's working set tiny (~600 lines).

## Quick start

```bash
# 1. (optional but recommended) Download SIFT1M + train + freeze the PQ codebook.
#    Takes ~5–10 min; ~250 MB on disk. Skipping it falls back to a synthetic
#    deterministic dataset (1024 base / 64 queries) — useful for smoke-testing
#    the harness but not representative of real workloads.
bash scripts/prepare_fixtures.sh

# 2. Run the baseline.
cargo run --release --bin run_experiment

# 3. Or run with Claude Code / Codex:
#    Open the repo in your agent of choice and prompt:
#       Hi, have a look at program.md and let's kick off a new experiment.
```

## File ownership

| File | Mutability | Edited by |
|---|---|---|
| `src/kernels.rs` | **mutable** | the agent |
| `src/bin/run_experiment.rs` | immutable | — |
| `src/reference.rs` | immutable | — |
| `src/fixture.rs` | immutable | — |
| `benches/pq_l2.rs` | immutable | — |
| `scripts/prepare_fixtures.sh` | immutable | — |
| `program.md` | human-iterated | the human, between runs |
| `results.tsv` | append-only | the agent, per trial (gitignored) |

## The metric

`run_experiment` prints a fixed-format block:

```
---
source:               sift1m
num_base:             1000000
num_queries:          1000
recall_at_10:         0.9421
geomean_ns_per_query: 184273
peak_mem_mb:          42.1
total_seconds:        21.7
```

A kernel is "kept" iff:

- `recall_at_10` is within 0.005 of the seeded scalar baseline (and ≥ 0.50 hard floor)
- `geomean_ns_per_query` is strictly better than the previous best-kept kernel
- `total_seconds` ≤ 600

See `program.md` for the full loop spec.

## Upstream contribution path

When a commit clears the keep bar by a meaningful margin (≥10% speedup with
recall in-band), the human reviews the diff, ports the technique against
[`lance-format/lance`](https://github.com/lance-format/lance) HEAD, runs Lance's
own test suite, and opens a PR. Because `src/kernels.rs` is dual MIT/Apache-2.0
licensed and algorithmically modeled on Lance's existing path, the upstream PR
inherits Apache-2.0 cleanly.

## License

Dual-licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
