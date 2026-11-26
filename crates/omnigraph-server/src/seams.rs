//! The server's decision seams over `omnigraph_seams`: the catalog a test
//! arms them through and the one site helper that turns a fired decision
//! into the site's failure. A production build reads an empty slot, which
//! passes; only a test build can fill one (the seams crate's `install`).

use omnigraph_seams::{DecideSeam, Decision, Effect};

/// The compile-checked index of every decision seam in this crate: each
/// static is declared beside the site it guards and re-exported here under
/// the string a test uses; `ALL` lists them for the guard.
pub mod catalog {
    use omnigraph_seams::DecideSeam;

    omnigraph_seams::catalog! {
        crate::recorded_operations::RECORDED_OPERATION_START_ANSWER,
        crate::recorded_operations::RECORDED_OPERATION_CANCEL_ANSWER,
    }

    /// The one string-keyed lookup: a test names a seam, the catalog answers.
    pub fn decide(name: &str) -> Option<&'static DecideSeam> {
        ALL.iter()
            .find(|entry| entry.name() == name)
            .and_then(|entry| entry.as_decide())
    }
}

/// Site helper for this crate's `Effect::Fail` seams: whether a test fired
/// the seam's failure here.
#[inline]
#[track_caller]
pub(crate) fn fail(seam: &'static DecideSeam) -> bool {
    assert_eq!(
        seam.effects(),
        &[Effect::Fail],
        "{} is not a fail-only seam",
        seam.name()
    );
    matches!(seam.crossed(), Decision::Fire(_))
}
