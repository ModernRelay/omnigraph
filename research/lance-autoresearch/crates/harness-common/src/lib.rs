//! Shared utilities for lance-autoresearch per-target harnesses.
//!
//! Each target crate (`pq-l2`, future `pq-cosine`, `bitpack-decode`, etc.)
//! defines its own `kernels.rs` (mutable, the agent's playground), `reference.rs`
//! (immutable scalar reference), `inputs.rs` (immutable test-data generators),
//! and `bin/run_experiment.rs` (immutable per-trial entry point). They all need
//! the same handful of building blocks: a deterministic PRNG, a geomean
//! aggregator, peak-RSS readback, tolerance constants for the bit-exact oracle,
//! and a single shared time-budget constant. That's everything in this crate.
//!
//! What is **not** here, and intentionally not abstracted:
//!
//! - A `Target` trait. Decode kernels (`bitpack`, `dictionary`, `FSST`) have
//!   very different signatures than distance kernels (`PqKernel::probe_top_k`),
//!   and forcing them into one trait shape would either bloat the trait or
//!   require erased boxing. Keep each target's API natural to its kernel.
//!
//! - Output-format orchestration. Each target's `run_experiment.rs` prints its
//!   own fixed-format result block — different targets report different
//!   per-combo dimensions (PQ shapes vs bit widths vs distribution kinds vs ...).
//!   Sharing the format would make the per-target binaries less readable and
//!   gain very little — `println!` is cheap.

pub mod prng;
pub mod stats;
pub mod sysinfo;
pub mod tolerance;

pub use prng::SplitMix64;
pub use stats::geomean;
pub use sysinfo::peak_rss_mb;
pub use tolerance::{MAX_ABS_ERR, TOPK_DIST_TOL};

/// Per-trial wall-clock cap. Targets should `std::process::exit(3)` if exceeded
/// so the agent's loop logs the trial as a timeout instead of a measurement.
pub const TIME_BUDGET_SECS: u64 = 600;
