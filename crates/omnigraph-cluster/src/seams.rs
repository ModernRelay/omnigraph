//! The cluster's decision seams over `omnigraph_seams`: the apply-protocol
//! catalog and the one site helper that turns a fired decision into this
//! crate's [`Diagnostic`]. With the `failpoints` feature off, the helper
//! compiles to `Ok(())` and no slot is ever read.

use crate::Diagnostic;
use omnigraph_seams::DecideSeam;

/// The compile-checked catalog of every decision seam in this crate: one
/// static per site, named by the string a case file uses. `ALL` lists them
/// for the runner and the guard.
pub mod catalog {
    use omnigraph_seams::{DecideSeam, Effect, Global, Op, Seam, SeamEntry};

    pub static CLUSTER_APPLY_AFTER_GRAPH_CREATE: DecideSeam = Seam::decide(
        "cluster_apply.after_graph_create",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_AFTER_GRAPH_DELETE: DecideSeam = Seam::decide(
        "cluster_apply.after_graph_delete",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_AFTER_PAYLOAD_PHASE: DecideSeam = Seam::decide(
        "cluster_apply.after_payload_phase",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_AFTER_SCHEMA_APPLY: DecideSeam = Seam::decide(
        "cluster_apply.after_schema_apply",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_BEFORE_GRAPH_CREATE: DecideSeam = Seam::decide(
        "cluster_apply.before_graph_create",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_BEFORE_GRAPH_DELETE: DecideSeam = Seam::decide(
        "cluster_apply.before_graph_delete",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_BEFORE_SCHEMA_APPLY: DecideSeam = Seam::decide(
        "cluster_apply.before_schema_apply",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );
    pub static CLUSTER_APPLY_BEFORE_STATE_WRITE: DecideSeam = Seam::decide(
        "cluster_apply.before_state_write",
        Op::Unreachable,
        Effect::Fail,
        Global::new(),
    );

    /// Every decision seam in this crate, in declaration order.
    pub static ALL: &[&'static dyn SeamEntry] = &[
        &CLUSTER_APPLY_AFTER_GRAPH_CREATE,
        &CLUSTER_APPLY_AFTER_GRAPH_DELETE,
        &CLUSTER_APPLY_AFTER_PAYLOAD_PHASE,
        &CLUSTER_APPLY_AFTER_SCHEMA_APPLY,
        &CLUSTER_APPLY_BEFORE_GRAPH_CREATE,
        &CLUSTER_APPLY_BEFORE_GRAPH_DELETE,
        &CLUSTER_APPLY_BEFORE_SCHEMA_APPLY,
        &CLUSTER_APPLY_BEFORE_STATE_WRITE,
    ];

    /// The one string-keyed lookup: a case file or the harness names a seam, the catalog answers.
    pub fn decide(name: &str) -> Option<&'static DecideSeam> {
        ALL.iter()
            .find(|entry| entry.name() == name)
            .and_then(|entry| entry.as_decide())
    }

    /// Empty every decision seam (a scenario teardown).
    #[cfg(feature = "failpoints")]
    pub fn clear_all() {
        for entry in ALL {
            entry.clear();
        }
    }
}

/// The engine's scenario gate extended to this crate's catalog: one gate,
/// both catalogs cleared on entry and on drop. Cluster tests use this one so
/// a cluster seam left armed by one test cannot fire in the next.
#[cfg(feature = "failpoints")]
pub struct FailScenario {
    _engine: omnigraph::seams::FailScenario,
}

#[cfg(feature = "failpoints")]
impl FailScenario {
    pub fn setup() -> Self {
        let engine = omnigraph::seams::FailScenario::setup();
        catalog::clear_all();
        Self { _engine: engine }
    }

    pub fn teardown(self) {
        drop(self);
    }
}

#[cfg(feature = "failpoints")]
impl Drop for FailScenario {
    fn drop(&mut self) {
        catalog::clear_all();
    }
}

/// Site helper for this crate's `Effect::Fail` seams: a fired decision
/// becomes the injected [`Diagnostic`] whose code the apply tests match.
#[inline]
pub(crate) fn fail(seam: &'static DecideSeam) -> Result<(), Diagnostic> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effect(),
            Some(omnigraph_seams::Effect::Fail),
            "{} is not a fail seam",
            seam.name()
        );
        if seam.crossed() == omnigraph_seams::Decision::Fire {
            return Err(Diagnostic::error(
                "injected_failpoint",
                seam.name(),
                format!("injected failpoint triggered: {}", seam.name()),
            ));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}
