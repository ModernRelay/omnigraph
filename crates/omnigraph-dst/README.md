# omnigraph-dst

Deterministic simulation testing for omnigraph. One **universe** = one seeded,
single-threaded, in-memory world driving a real `Omnigraph` through its
production write path. Everything — scheduler tie-breaks, identity (ULIDs),
the wall clock, the workload, storage faults, crash timing — derives from one
root seed, so a failing seed replays exactly.

The engine keeps only three passthrough seams (`omnigraph::dst_ids`,
`omnigraph::dst_clock`, `omnigraph::dst_gate`); this crate owns everything
else and cannot affect production by construction (the `slatedb-dst` pattern).

## Run

```bash
cd crates/omnigraph-dst   # the crate-local .cargo/config.toml sets the flag
cargo test
# fast simulations that keep assertions:
cargo test --profile dst
# reproduce a CI failure (each universe logs its seed line):
OMNIGRAPH_DST_SEEDS=17689751483034105621 cargo test dst_v11_parallel_seed_fleet
```

The required `--cfg tokio_unstable` (it gates tokio's seeded scheduler,
`Builder::rng_seed`) comes from this crate's own `.cargo/config.toml` when
cargo is invoked from the crate directory; from anywhere else, set
`RUSTFLAGS="--cfg tokio_unstable"` yourself. The `failpoints` feature is a
default of this crate.

## What a universe checks (the oracles)

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

## Fault axes

- **Crash windows**: `Scenario::crash_at` arms any of the engine's 66 named
  failpoints (`catalog::CRASH_WINDOWS`) at a seeded op index. The ignored
  `dst_hunt_crash_window_sweep` test sweeps the whole catalog.
- **Injected storage faults**: `Scenario::faults` installs a seeded `FaultPlan` at the
  `StorageAdapter` seam — marked write errors + latency charged in VIRTUAL
  time (`start_paused` clock, so faulty universes still run in milliseconds).

## Known limits

Racing-writer determinism is blocked on the upstream Lance deterministic-mode
proposal (4 flag-gated items) — the
`#[ignore]`d instruments flip on when it lands. Crash-mode universes cannot
yet run in parallel threads (failpoint-registry threading is a planned follow-up).
