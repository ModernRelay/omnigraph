//! Failpoint-gated DST cells, in their OWN binary so the process-global `fail`
//! registry cannot leak armed failpoints into the main `dst` suite's (parallel,
//! non-serial) generative walks. Run with `--features failpoints`; empty
//! otherwise. The reusable harness primitives (op alphabet, white-box
//! invariants, …) come from the shared `omnigraph-dst` crate; this binary adds
//! only the raw-handle helpers + the recovery cells.
//!
//! See `dst/recovery_walk.rs` for the cells; see `dst/MATRIX.md` for why this
//! closes the fault-seam gap.
#![cfg(feature = "failpoints")]
#![allow(dead_code)]

use omnigraph::db::Omnigraph;

#[path = "dst/recovery_walk.rs"]
mod recovery_walk;

/// A raw embedded graph — the recovery cells drive the `Omnigraph` handle
/// directly (white-box), so they don't go through the `Backend` abstraction.
async fn open_clean(uri: &str) -> Omnigraph {
    Omnigraph::init(uri, omnigraph_dst::op::SCHEMA)
        .await
        .expect("init")
}
async fn reopen(uri: &str) -> Omnigraph {
    Omnigraph::open(uri).await.expect("reopen")
}
