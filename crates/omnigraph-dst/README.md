# omnigraph-dst

Deterministic simulation testing for omnigraph. One **universe** = one seeded,
single-threaded, in-memory world driving a real `Omnigraph` through its
production write path. Everything — scheduler tie-breaks, identity (ULIDs),
the wall clock, the workload, storage faults, crash timing — derives from one
root seed, so a failing seed replays exactly (compared at report grain; see
Deferred work) — in the pinned suite and the deterministic nightly arms. The
`wild` concurrent arms and the real-process-death instruments (lane B, defined
under Failure models below) deliberately trade exact replay for real timing;
their failures are evidence (seed row, oplog, judge dump), not transcripts.

The engine keeps only three passthrough seams (`omnigraph::dst_ids`,
`omnigraph::dst_clock`, `omnigraph::dst_gate`); this crate owns everything
else (the `slatedb-dst` pattern). Default per-package builds compile the seam
override machinery out entirely (the non-default `dst` feature; a
`--workspace` invocation unifies it in through this crate, which is why CI
lints the `-p` shape — release artifacts build with `-p` and stay clean). The
one residual default-build footprint is the write queue's release-epoch
bookkeeping — the per-release counter that orders lock releases for the
scheduler seam (TODO(#527), a v2 issue).

## Run

```bash
cd crates/omnigraph-dst   # the crate-local .cargo/config.toml sets the flag
cargo test
# fast simulations that keep assertions:
cargo test --profile dst
# reproduce a CI failure (each universe logs its seed line):
OMNIGRAPH_DST_SEEDS=17689751483034105621 cargo test dst_v11_parallel_seed_fleet
```

Reproduction env vars by lane: `OMNIGRAPH_DST_SEEDS` (comma-separated
literal seeds) drives the concurrent fleet and the seed-fleet tests;
the nightly deterministic fleet uses `DST_FLEET_SEED_BASE` +
`DST_FLEET_SEEDS` (an interval) — both spellings appear verbatim in the
failing job's log line.

The required `--cfg tokio_unstable` (it gates tokio's seeded scheduler,
`Builder::rng_seed`) comes from this crate's own `.cargo/config.toml` when
cargo is invoked from the crate directory; from anywhere else, set
`RUSTFLAGS="--cfg tokio_unstable"` yourself. The `failpoints` feature is a
default of this crate.

## What a universe checks (the oracles)

The five GROUPS below are the reader's map; the enforced census counts
21 oracles at detector grain (`src/detectors.rs`, the generated
`detector_census.txt`).

1. **Differential model** — persons *and* edges mirrored in a `BTreeMap`/
   `BTreeSet`, updated only on `Ok` ops; the graph must equal the model
   **continuously** (every third op), at final reopen (durability), and
   through the read-only open path (third view). Edge equality doubles as the
   referential-integrity oracle.
2. **Two-sided crash contract** — a crashed op is atomic (state is exactly
   model or exactly model+op) and recovery may roll forward but never roll
   back a committed write (lost-acknowledgement semantics).
3. **OCC invariant** — no two commits share a `graph_commit_id`.
4. **Replay meta-test** — same scenario ⇒ byte-equal `UniverseReport`s,
   including commit ids and the raw row-ordered JSON of a full
   compiler→DataFusion query.
5. **Conservation** — batched two-statement transfers preserve the balance
   sum (multi-statement atomicity).

## Failure models (the three, plus the two lanes)

Three distinct instruments, complementary — none subsumes another:

1. **Fault plans** (weather): seeded per-call fault rates in BOTH storage
   realms (the `StorageAdapter` seam and Lance's interposed provider) —
   clean errors and latency, read-path corruption (bit rot, truncation,
   latent sector errors), write-side weather (corrupted, lost, misdirected
   writes), ack-loss, and bounded staleness. Unaimed — discovers handling
   bugs statistically across the whole run.
2. **Crash-window hunt**: a named failpoint (71 in `catalog::CRASH_WINDOWS`)
   armed at a seeded op index. Aimed — guarantees a specific dangerous
   moment is exercised.
3. **Crash-state enumeration** (ALICE-style): kill at completion #k for
   every k, judge each cut against the exact-worlds model. Complete over
   cuts, not probabilistic.

Two delivery lanes for the kill itself: **lane A** (in-process simulated
crash: the storage wrapper stops forwarding, same process judges) and
**lane B** (real death: a parent spawns `dst_child`, waits on the fsync'd
barrier line at completion #k, SIGKILLs, then recovers and judges from the
oplog). Lane B ships as a labeled preview; see `src/lane_b.rs`
and `tests/lane_b.rs`.

## Fault axes

- **Crash windows**: `Scenario::crash_at` arms any of the engine's 71 named
  failpoints (`catalog::CRASH_WINDOWS`) at a seeded op index. The ignored
  `dst_hunt_crash_window_sweep` test sweeps the whole catalog.
- **Injected storage faults**: `Scenario::faults` installs a seeded `FaultPlan`
  over both realms — marked errors + latency charged in VIRTUAL
  time (`start_paused` clock, so faulty universes still run in milliseconds),
  plus the corruption/ack-loss/staleness axes listed under Failure models.

## Known limits

Racing-writer determinism is blocked on the upstream Lance deterministic-mode
proposal (4 flag-gated items) — the
`#[ignore]`d instruments flip on when it lands. Crash-mode universes cannot
yet run in parallel threads (failpoint-registry threading is a planned follow-up).

## Deferred work (#527 review)

Known limits raised in the PR #527 review are deferred deliberately.
Three carry an in-repo mechanism that fires if their premise breaks: a
tripwire test (multipart bypass), a typed exclusion
(`write_census::CUT_COVERAGE_EXCLUSIONS`), and a source guard
(process-env mutation). The rest — instance-owned entropy/ID/clock
sources, full-trace replay comparison, logical turns for finish/timeout
edges, a compile-fail guard on the `dst` feature boundary — are tracked
by issue only; until those land, strict-replay claims are scoped to the
report projection and the labeled deterministic arms. The full gap index
is the "Deferred to v2" table in
`docs/rfcs/0037-deterministic-simulation-harness.md`.
