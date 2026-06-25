//! Deterministic-simulation (DST) test harness for Omnigraph.
//!
//! Shared, dev-only library holding the op alphabet, the reference model, the
//! white-box invariant battery, and a `Backend` trait so the SAME generative
//! walk can run against the embedded `Omnigraph` SDK and the `omnigraph` CLI
//! subprocess. Consumed as a `[dev-dependencies]` by both `omnigraph-engine`
//! (white-box walks + regressions) and `omnigraph-cli` (cross-backend walk).
//!
//! Not a default-member; never built in the normal `cargo build`.

// (skeleton — modules land in the extraction commits)
