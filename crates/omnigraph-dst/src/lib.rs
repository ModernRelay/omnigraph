//! omnigraph-dst — deterministic simulation testing for omnigraph.
//!
//! One universe = one seeded, single-threaded, in-memory world running a real
//! `Omnigraph` through its production write path. The root seed derives
//! everything (seed tree): the tokio scheduler's RNG, the identity (ULID)
//! stream, the logical wall clock, and the workload. Faults are deterministic
//! crash windows scheduled over the engine's own failpoints. Oracles: a
//! differential model, durability-after-crash, OCC commit-id uniqueness, and
//! the rerun meta-test (same scenario ⇒ equal reports INCLUDING commit ids);
//! the full verdict/detector census is generated from `detectors.rs`
//! (`detector_census.txt`).
//!
//! Architecture (the slatedb-dst pattern): the ENGINE owns only the seams
//! (`omnigraph::dst_ids`, `omnigraph::dst_clock`, `omnigraph::dst_gate` —
//! passthrough no-ops in
//! production); THIS crate owns everything else and cannot affect production
//! by construction.
//!
//! Build: needs `--cfg tokio_unstable` (seeded scheduler); this crate's own
//! `.cargo/config.toml` sets it when cargo runs from the crate directory:
//! `cd crates/omnigraph-dst && cargo test`. From elsewhere, set `RUSTFLAGS`
//! yourself (the DST CI workflows do).
//!
//! Known gaps (deliberate, TODO): fixture schema only (no schema
//! fuzzing); Lance-internal parallelism runs quiesced in-suite (the
//! unquiesced regime is fleet-only and makes no replay claim — full
//! determinism there waits on a Lance deterministic-mode upstream
//! change); no blob workload yet (its crash window enters the catalog
//! as never-reached).
#![cfg(tokio_unstable)]

pub mod catalog;
pub mod concurrent;
pub mod cost;
pub mod detectors;
pub mod entropy;
pub mod env_knobs;
pub mod fixtures;
pub mod harness;
pub mod lance_faults;
pub mod rand;
pub mod trace;
