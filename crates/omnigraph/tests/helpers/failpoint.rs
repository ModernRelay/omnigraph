//! Deterministic rendezvous for concurrent seam tests.
//!
//! The pattern: park the FIRST thread that crosses a seam until the test
//! explicitly releases it, while later arrivals fall through. This replaces
//! fixed "guess" `sleep`s for cross-thread coordination — the test waits on
//! the *condition* (the seam was reached) with a bounded timeout that fails
//! loudly, instead of betting a fixed duration is long enough.
//!
//! Extracted from the open-coded `AtomicBool` + callback pattern that
//! `fork_collision_with_live_concurrent_fork_reprepares` proved out.
//!
//! The `reached` flag also doubles as a fired-assertion: a seam that is never
//! crossed makes [`Rendezvous::wait_until_reached`] panic, so a misplaced
//! seam cannot pass silently.

use std::sync::Arc;
use std::time::Duration;

use omnigraph::seams::{Decide, DecideSeam, Global, Hold, Installed};

/// A parked-on-first-arrival rendezvous bound to a seam. The underlying
/// behavior is RAII-cleaned when this guard drops, which also releases a
/// parked caller.
pub struct Rendezvous {
    name: &'static str,
    hold: Arc<Hold>,
    _installed: Installed<dyn Decide, Global<dyn Decide>>,
}

impl Rendezvous {
    /// Install a [`Hold`] on `seam` so the FIRST crossing records readiness
    /// and blocks until [`release`](Self::release); later crossings fall
    /// through immediately.
    pub fn park_first(seam: &'static DecideSeam) -> Self {
        let (installed, hold) = seam.hold();
        Self {
            name: seam.name(),
            hold,
            _installed: installed,
        }
    }

    /// Async-wait until the parked thread has reached the seam, polling the
    /// readiness condition with a bounded (~12s) timeout. Panics if the seam
    /// is never crossed — the fired-assertion.
    pub async fn wait_until_reached(&self) {
        for _ in 0..2400 {
            if self.hold.reached() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("rendezvous: seam '{}' was never reached", self.name);
    }

    /// Whether the parked thread has reached the seam yet.
    pub fn reached(&self) -> bool {
        self.hold.reached()
    }

    /// Release the parked thread so it resumes past the seam.
    pub fn release(&self) {
        self.hold.release();
    }
}
