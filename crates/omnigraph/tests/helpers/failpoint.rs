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

use std::sync::atomic::{AtomicU64, Ordering};

use omnigraph::seams::{Behavior, Decide, DecideSeam, Decision, Global, Hold, Installed};

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

    pub fn timed_out(&self) -> bool {
        self.hold.timed_out()
    }

    /// Release the parked thread so it resumes past the seam.
    pub fn release(&self) {
        self.hold.release();
    }
}

/// Parks chosen crossings of one seam (counted from 1), each until its own
/// release; every other crossing passes. One install covers them all, so a
/// test parks a writer's first and second attempts without re-arming the
/// seam in between. Dropping it releases every parked caller.
pub struct ParkedCrossings {
    name: &'static str,
    holds: Arc<Vec<(u64, Arc<Hold>)>>,
    _installed: Installed<dyn Decide, Global<dyn Decide>>,
}

struct ParkAt {
    crossings: AtomicU64,
    holds: Arc<Vec<(u64, Arc<Hold>)>>,
}

impl Behavior for ParkAt {
    fn uninstalling(&self) {
        for (_, hold) in self.holds.iter() {
            hold.release();
        }
    }
}

impl Decide for ParkAt {
    fn decide(&self, name: &'static str) -> Decision {
        let crossing = self.crossings.fetch_add(1, Ordering::SeqCst) + 1;
        match self.holds.iter().find(|(at, _)| *at == crossing) {
            Some((_, hold)) => hold.decide(name),
            None => Decision::Pass,
        }
    }
}

impl ParkedCrossings {
    pub fn new(seam: &'static DecideSeam, crossings: &[u64]) -> Self {
        assert!(
            crossings.iter().all(|at| *at >= 1),
            "crossings count from 1"
        );
        let holds = Arc::new(
            crossings
                .iter()
                .map(|at| (*at, Arc::new(Hold::default())))
                .collect::<Vec<_>>(),
        );
        let installed = seam.install(Arc::new(ParkAt {
            crossings: AtomicU64::new(0),
            holds: Arc::clone(&holds),
        }));
        Self {
            name: seam.name(),
            holds,
            _installed: installed,
        }
    }

    fn hold(&self, crossing: u64) -> &Hold {
        self.holds
            .iter()
            .find(|(at, _)| *at == crossing)
            .map(|(_, hold)| hold.as_ref())
            .unwrap_or_else(|| panic!("crossing {crossing} of '{}' is not parked", self.name))
    }

    /// As [`Rendezvous::wait_until_reached`], for one parked crossing.
    pub async fn wait_until_reached(&self, crossing: u64) {
        for _ in 0..2400 {
            if self.hold(crossing).reached() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("seam '{}' was never crossed a {crossing}th time", self.name);
    }

    pub fn release(&self, crossing: u64) {
        self.hold(crossing).release();
    }
}
