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
    Observe, Op, PanicAt, Seam, SeamEntry, StoreEffect, ThreadLocal, decide_seam, effects_list,
    store_effects_list,
};

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
/// becomes the injected `Manifest` error. A fired store effect passes: the
/// storage decoration acts on the call that follows.
#[inline]
#[track_caller]
pub fn fail(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Fail],
            "{} is not a fail-only seam",
            seam.name()
        );
        if matches!(seam.crossed(), Decision::Fire(_)) {
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
pub fn skip(seam: &'static DecideSeam) -> bool {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Skip],
            "{} is not a skip-only seam",
            seam.name()
        );
        matches!(seam.crossed(), Decision::Fire(_))
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
pub fn contention(seam: &'static DecideSeam) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        assert_eq!(
            seam.effects(),
            &[Effect::Contention],
            "{} is not a contention-only seam",
            seam.name()
        );
        if matches!(seam.crossed(), Decision::Fire(_)) {
            return Err(injected_contention(seam));
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
    Ok(())
}

pub mod store;
