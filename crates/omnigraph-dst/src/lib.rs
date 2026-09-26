//! omnigraph-dst — deterministic simulation testing for omnigraph.
//!
//! One universe = one seeded, single-threaded, in-memory world running a real
//! `Omnigraph` through its production write path. The root seed derives
//! everything (seed tree): the tokio scheduler's RNG, the identity (ULID)
//! stream, the logical wall clock, and the workload. Faults are deterministic
//! crash windows scheduled over the engine's own failpoints. Oracle
//! highlights (among the census's 22): a differential model,
//! durability-after-crash, OCC commit-id uniqueness, and the rerun
//! meta-test (same scenario ⇒ equal reports INCLUDING commit ids);
//! the full verdict/detector census is generated from `detectors.rs`
//! (`detector_census.txt`).
//!
//! Architecture (the slatedb-dst pattern): the ENGINE owns only the seams
//! (`omnigraph::dst_ids`, `omnigraph::dst_clock`, `omnigraph::dst_gate` —
//! passthrough no-ops in production, gated behind the non-default `dst`
//! feature); THIS crate owns everything else.
//!
//! Build: needs `--cfg tokio_unstable` (seeded scheduler), set by the
//! workspace `.cargo/config.toml` for every build. Run from the crate
//! directory (`cd crates/omnigraph-dst && cargo test`): its `[env]`-only
//! `.cargo/config.toml` supplies the pool trio; elsewhere export the trio
//! yourself (the DST CI workflows do). `store_places` is the one module
//! built without the cfg: the GQT runner admits cases against its table on
//! every flag set.
//!
//! Known gaps (deliberate, TODO): fixture schema only (no schema
//! fuzzing); Lance-internal parallelism runs quiesced in-suite (the
//! unquiesced regime is fleet-only and makes no replay claim — full
//! determinism there waits on a Lance deterministic-mode upstream
//! change); no blob workload yet (its crash window enters the catalog
//! as never-reached).
#[cfg(tokio_unstable)]
pub mod catalog;
#[cfg(tokio_unstable)]
pub mod concurrent;
#[cfg(tokio_unstable)]
pub mod cost;
#[cfg(tokio_unstable)]
pub mod detectors;
#[cfg(tokio_unstable)]
pub mod entropy;
#[cfg(tokio_unstable)]
pub mod env_knobs;
#[cfg(tokio_unstable)]
pub mod environment;
#[cfg(tokio_unstable)]
pub mod fixtures;
#[cfg(tokio_unstable)]
pub mod harness;
#[cfg(tokio_unstable)]
pub mod lance_faults;
#[cfg(tokio_unstable)]
pub mod lane_b;
#[cfg(tokio_unstable)]
pub mod memory;
#[cfg(tokio_unstable)]
pub mod oplog;
#[cfg(tokio_unstable)]
pub mod rand;
pub mod store_places;
#[cfg(tokio_unstable)]
pub mod trace;
#[cfg(tokio_unstable)]
pub mod write_census;

#[cfg(tokio_unstable)]
pub use environment::{
    UniverseEnvironment, UniversePhase, UniverseProcess, UniverseRun, UniverseScenario,
    run_universe,
};
