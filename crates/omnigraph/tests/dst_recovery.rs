//! Failpoint-gated DST cells, in their OWN binary so the process-global `fail`
//! registry cannot leak armed failpoints into the main `dst` suite's (parallel,
//! non-serial) generative walks. Run with `--features failpoints`; empty
//! otherwise. Shares the harness's `dst/` modules via `#[path]` (compiled per
//! binary — hence `allow(dead_code)` for the helpers this binary doesn't call).
//!
//! See `dst/recovery_walk.rs` for the cells; see `dst/MATRIX.md` for why this
//! closes the fault-seam gap.
#![cfg(feature = "failpoints")]
#![allow(dead_code)]

#[path = "dst/op.rs"]
mod op;
#[path = "dst/model.rs"]
mod model;
#[path = "dst/invariants.rs"]
mod invariants;
#[path = "dst/fault.rs"]
mod fault;
#[path = "dst/backend.rs"]
mod backend;
#[path = "dst/recovery_walk.rs"]
mod recovery_walk;
