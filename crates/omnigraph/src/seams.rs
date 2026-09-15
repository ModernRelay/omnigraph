//! The engine's seams over `omnigraph_seams`: the decision sites (the
//! catalog), the three site helpers that turn a fired decision into this
//! crate's error types, and the scenario gate that serializes tests holding
//! process-wide seams.
//!
//! With the `failpoints` feature off, every helper compiles to its default arm
//! and no slot is ever read.

use crate::error::Result;

#[cfg(any(feature = "failpoints", feature = "dst"))]
pub use omnigraph_seams::Installed;
pub use omnigraph_seams::{
    Behavior, Counted, Decide, DecideSeam, Decision, Effect, FireAlways, FireOnceAt, Global, Hold,
    Observe, Op, PanicAt, Seam, SeamEntry, ThreadLocal,
};

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

/// Site helper for an `Effect::Fail` seam: a fired decision becomes the
/// injected `Manifest` error, whose text the logic test corpus matches.
#[inline]
pub(crate) fn fail(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effect(),
            Some(Effect::Fail),
            "{} is not a fail seam",
            seam.name()
        );
        if seam.crossed() == Decision::Fire {
            return Err(crate::error::OmniError::manifest(format!(
                "injected failpoint triggered: {}",
                seam.name()
            )));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}

/// Site helper for an `Effect::Skip` seam: true when a fired decision asks
/// the site to take the branch it cannot reach on its own (an object store
/// that persists no e_tags, a confirm write that is never acknowledged).
#[inline]
pub(crate) fn skip(seam: &'static DecideSeam) -> bool {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effect(),
            Some(Effect::Skip),
            "{} is not a skip seam",
            seam.name()
        );
        seam.crossed() == Decision::Fire
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = seam;
        false
    }
}

/// Site helper for an `Effect::Contention` seam: a fired decision becomes the
/// retryable `RowLevelCasContention` error the manifest publisher's outer
/// retry treats as retryable, driving that path deterministically.
#[inline]
pub(crate) fn contention(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effect(),
            Some(Effect::Contention),
            "{} is not a contention seam",
            seam.name()
        );
        if seam.crossed() == Decision::Fire {
            return Err(crate::error::OmniError::manifest_row_level_cas_contention(
                format!("injected retryable contention failpoint: {}", seam.name()),
            ));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}
