//! The engine's seam catalog over the shared seam helpers in omnigraph_core.

pub mod catalog;

/// Serializes scenario-holding tests: the decision seams are process-wide,
/// so two scenarios in one test binary would clear and fire each other's
/// sites.
#[cfg(feature = "failpoints")]
static SCENARIO_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// RAII scenario over the catalog. Holds [`SCENARIO_GATE`] for its lifetime
/// and clears every decision seam on entry and on drop; a poisoned gate is
/// recovered, so one panicking test cannot wedge the rest of the suite.
#[cfg(feature = "failpoints")]
pub struct FailScenario {
    _gate: std::sync::MutexGuard<'static, ()>,
}

#[cfg(feature = "failpoints")]
impl FailScenario {
    /// Acquires the scenario gate, blocking until no other scenario is live,
    /// then starts from empty slots.
    pub fn setup() -> Self {
        let gate = SCENARIO_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        catalog::clear_all();
        Self { _gate: gate }
    }

    /// Clears every decision seam and releases the gate.
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

#[cfg(any(feature = "failpoints", feature = "dst"))]
pub use omnigraph_core::seams::Installed;
pub use omnigraph_core::seams::{
    Behavior, Counted, Decide, DecideSeam, Decision, Effect, FireAlways, FireOnceAt, Global, Hold,
    Observe, Op, PanicAt, Seam, SeamEntry, StoreEffect, ThreadLocal, decide_seam, effects_list,
    store_effects_list,
};
pub(crate) use omnigraph_core::seams::{fail, skip};

pub mod store {
    pub use omnigraph_core::seams::store::{SUBJECT_MAX_BYTES, StoreAction, Subject};
}
