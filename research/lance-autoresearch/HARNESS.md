# HARNESS — shared loop contract for every lance-autoresearch target

This document is the universal part of every target's agent instructions. Each
target's `program.md` is a thin layer of *target-specific priors and API spec*
on top of the conventions below. The agent reads `HARNESS.md` and the target's
`program.md` at the start of every session.

## What this harness is

A single agent (you) edits one file in one target crate to optimize a Lance
kernel. Per trial, you build, run a binary that exercises the kernel against
diverse inputs, parse a fixed-format output block, and decide keep-or-revert.

This is a Karpathy-style autoresearch loop. It assumes:

- Per-trial eval is **seconds-scale**. Long enough to measure, short enough to
  iterate hundreds of times in a session.
- The kernel has a **deterministic correctness oracle** — a scalar reference
  that produces the same answer to compare against.
- The optimization target is **dataset-independent**: the harness generates
  diverse inputs each trial, so wins generalize across distributions and
  shapes by construction.

Targets that don't fit these constraints (index-build parameter tuning,
plan-patching, anything where eval is minutes-to-hours) belong in the
BauplanLabs tournament-loop shape, not this harness. See `docs/design.md` for
the boundary.

## What's editable, per target

| Path | Mutability | Why |
|---|---|---|
| `crates/<target>/src/kernels.rs` | **mutable** | Your playground. The whole point. |
| `crates/<target>/src/reference.rs` | immutable | The oracle. Touching it makes wins meaningless. |
| `crates/<target>/src/inputs.rs` | immutable | The fixture generator. Touching it makes timings incomparable across trials. |
| `crates/<target>/src/lib.rs` | immutable | Shared types pinned by the bench (`PqShape` etc.). |
| `crates/<target>/src/bin/run_experiment.rs` | immutable | The trial harness. |
| `crates/<target>/benches/*.rs` | immutable | Criterion bench, optional read-only reference. |
| `crates/<target>/Cargo.toml` | immutable | Adding deps changes the optimization target. |
| `crates/<target>/program.md` | human-iterated between runs | Not edited by you in-loop; the human refines it. |
| `crates/<target>/results.tsv` | append-only | Your audit log. Gitignored. |
| `crates/harness-common/**` | immutable | Workspace-shared infrastructure. |
| `HARNESS.md` (this file) | immutable | Workspace-shared loop contract. |

You may add `#[cfg(test)] mod tests { ... }` inside `kernels.rs` for in-file
property checks. You may NOT add new crate dependencies. You may NOT use
unsafe-only-on-broken-assumptions tricks (e.g., assuming a fixture invariant
that holds today but isn't documented).

## The metric

Every target's `run_experiment` binary prints a fixed-format output block ending
with these universal fields:

- `correctness:` — `pass` or `fail`. Set by comparing your kernel against the
  scalar reference on every input the bench generates.
- `geomean_ns_per_*:` — geometric mean of per-operation wall-clock across all
  timed operations.
- `worst_ns_per_*:` — slowest combo's geomean.
- `peak_mem_mb:` — process RSS high-water-mark.
- `total_seconds:` — trial wall-clock.

A kernel is **kept** iff:

1. `correctness: pass` (any failure → `std::process::exit(2)`).
2. `geomean_ns_per_*` strictly better than the previous best-kept kernel
   (allow ~1% noise band).
3. `worst_ns_per_*` ≤ 1.05 × the previous best-kept kernel's worst.
4. `total_seconds` ≤ 600 (the per-trial cap; exceed it → `std::process::exit(3)`).
5. Build clean: `cargo build --release` and
   `cargo clippy --release --all-targets -- -D warnings` both succeed.

Exit codes summary for `run_experiment`: `0` on success, `1` on internal
error (panic, fixture load failure), `2` on correctness failure, `3` on
time-budget breach. The agent's loop should treat anything non-zero as
"revert; do not log as a measurement."

Ties break toward simpler code: same speed within ~3% noise → fewer lines /
less `unsafe` wins.

## The loop

After reading `HARNESS.md` and the target's `program.md`:

1. **Setup (once per session).** Confirm `results.tsv` exists; if not, create
   it with a per-target header (the target's `program.md` defines the columns).
   Run the baseline trial:
   ```
   cargo run --release --bin run_experiment -p <target> > run.log 2>&1
   ```
   Append a row tagged `keep=baseline` and commit it.

2. **Observe state.** Read the last ~5 rows of `results.tsv`. Note which ideas
   have been tried, what won, what regressed. Form one hypothesis with one
   sentence stating the change and the predicted effect on speed and
   correctness.

3. **Edit `kernels.rs`.** Keep the diff focused on the one hypothesis.

4. **Build and lint.**
   ```
   cargo build --release
   cargo clippy --release --all-targets -- -D warnings
   ```
   If either fails, fix and retry. Do not commit broken state.

5. **Run the trial.**
   ```
   cargo run --release --bin run_experiment -p <target> > run.log 2>&1
   ```

6. **Parse and decide.** Extract the universal fields plus any per-target
   fields. Compute deltas vs. the last-kept row. Apply the keep criteria above.

7. **Log.** Append one row to `results.tsv` matching the target's header.

8. **Commit.** One-line message describing the change and the headline number,
   e.g. `transpose codebook in new(); 18.2k → 14.1k geomean ns (worst -8%)`.

9. **Hygiene.**
   - Always commit `kernels.rs` changes; never commit `results.tsv` or
     `run.log` (gitignored).
   - If a change fails to build, do not commit. Iterate or revert cleanly.
   - If two consecutive ideas regress, take a beat: re-read the last ~10 rows
     and update your mental model before proposing the next.
   - Per-trial cap: 10 minutes. If `cargo run` is still going after 10 min,
     kill it and mark the trial as `timeout`.

## Never stop

Keep going until interrupted. Each loop iteration is one hypothesis, one edit,
one measurement, one commit. No multi-step plans across iterations.

## Working across multiple targets

If a session spans multiple targets, work on **one target per session**. Don't
edit `kernels.rs` in two crates between commits — the agent's mental model is
shared but the keep-decision is per-target. Pick a target, do a session there,
commit, switch.

The human is responsible for selecting which target to work on next. Don't
proactively switch targets unless the user asks.
