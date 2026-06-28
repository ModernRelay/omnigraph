//! Deterministic-simulation (DST) test harness for Omnigraph.
//!
//! Shared, dev-only library holding the op alphabet, the reference model, the
//! white-box invariant battery, and a `Backend` trait so the SAME generative
//! walk can run against the embedded `Omnigraph` SDK and the `omnigraph` CLI
//! subprocess. Consumed as a `[dev-dependencies]` by both `omnigraph-engine`
//! (white-box walks + regressions) and `omnigraph-cli` (cross-backend walk).
//!
//! Not a default-member; never built in the normal `cargo build`.
//!
//! Consumers:
//!   * `omnigraph-engine` `tests/dst.rs` drives `Embedded` + the white-box
//!     battery (the harness's core value — embedded-only by construction).
//!   * `omnigraph-cli` `tests/cli_cross_backend_walk.rs` drives the SAME seeded
//!     walk against `Embedded` AND `Cli`, asserting per-step black-box agreement.

pub mod backend;
pub mod cohort;
pub mod convergence;
pub mod fault;
pub mod invariants;
pub mod model;
pub mod op;

pub use backend::{Backend, BackendError, Cli, Embedded};
pub use cohort::Cohort;
pub use convergence::{
    all_writers_converge, chain_len_is, lineage_is_linear_chain, run_convergence_battery,
};
pub use fault::FaultAdapter;
pub use invariants::{
    classify, classify_backend, classify_panic, panic_message, run_battery, Finding,
};
pub use model::Model;
pub use op::{step, OpKind, Rng};
