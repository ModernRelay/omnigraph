//! The engine's seams over `omnigraph_seams`: the decision sites (the
//! catalog), the site helpers that turn a fired decision into this crate's
//! error types, and the scenario gate that serializes tests holding
//! process-wide seams.
//!
//! `fail`, `skip` and `contention` serve a site between two steps, which
//! declares one effect. `guarded` serves a site that wraps one operation and
//! declares every outcome that operation can have: the wrapped call runs,
//! is skipped, or is replaced by an injected error, as the installed decider
//! chooses per crossing.
//!
//! With the `failpoints` feature off, every helper compiles to its default arm
//! and no slot is ever read.

use crate::error::Result;

#[cfg(any(feature = "failpoints", feature = "dst"))]
pub use omnigraph_seams::Installed;
pub use omnigraph_seams::{
    Behavior, Counted, Decide, DecideSeam, Decision, Effect, FireAlways, FireOnceAt, Global, Hold,
    Observe, Op, PanicAt, Seam, SeamEntry, ThreadLocal, decide_seam, effects_list,
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

/// The injected `Manifest` error, whose text the logic test corpus matches.
#[cfg(feature = "failpoints")]
fn injected(seam: &'static DecideSeam) -> crate::error::OmniError {
    crate::error::OmniError::manifest(format!("injected failpoint triggered: {}", seam.name()))
}

/// The injected retryable `RowLevelCasContention` error the manifest
/// publisher's outer retry treats as retryable, driving that path
/// deterministically.
#[cfg(feature = "failpoints")]
fn injected_contention(seam: &'static DecideSeam) -> crate::error::OmniError {
    crate::error::OmniError::manifest_row_level_cas_contention(format!(
        "injected retryable contention failpoint: {}",
        seam.name()
    ))
}

/// Site helper for a seam declaring only `Effect::Fail`: a fired decision
/// becomes the injected `Manifest` error.
#[inline]
#[track_caller]
pub(crate) fn fail(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Fail],
            "{} is not a fail-only seam",
            seam.name()
        );
        if seam.crossed() != Decision::Pass {
            return Err(injected(seam));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}

/// Site helper for a seam declaring only `Effect::Skip`: true when a fired
/// decision asks the site to take the branch it cannot reach on its own (an
/// object store that persists no e_tags).
#[inline]
#[track_caller]
pub(crate) fn skip(seam: &'static DecideSeam) -> bool {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Skip],
            "{} is not a skip-only seam",
            seam.name()
        );
        seam.crossed() != Decision::Pass
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = seam;
        false
    }
}

/// Site helper for a seam declaring only `Effect::Contention`: a fired
/// decision becomes the injected retryable contention error.
#[inline]
#[track_caller]
pub(crate) fn contention(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Contention],
            "{} is not a contention-only seam",
            seam.name()
        );
        if seam.crossed() != Decision::Pass {
            return Err(injected_contention(seam));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}

/// Site helper for a seam that wraps one operation: run `op` under the
/// seam. `Ok(Some(_))` is the operation's own result, `Ok(None)` means the
/// decider skipped it, `Err` is the operation's own error or the injected
/// one. The seam lists every outcome the code after the call survives; a
/// `skip` is honest only when that code treats `None` as a real outcome.
#[inline]
#[track_caller]
#[cfg_attr(
    not(feature = "failpoints"),
    allow(
        clippy::manual_async_fn,
        reason = "The failpoints build captures the caller before constructing the future."
    )
)]
pub(crate) fn guarded<T>(
    seam: &'static DecideSeam,
    op: impl std::future::Future<Output = Result<T>>,
) -> impl std::future::Future<Output = Result<Option<T>>> {
    #[cfg(feature = "failpoints")]
    let caller = std::panic::Location::caller();
    async move {
        #[cfg(feature = "failpoints")]
        match seam.crossed_from(caller) {
            Decision::Pass => {}
            Decision::Fire(Effect::Skip) => return Ok(None),
            Decision::Fire(Effect::Fail) => return Err(injected(seam)),
            Decision::Fire(Effect::Contention) => return Err(injected_contention(seam)),
        }
        #[cfg(not(feature = "failpoints"))]
        let _ = seam;
        Ok(Some(op.await?))
    }
}
