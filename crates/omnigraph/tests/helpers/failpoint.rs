//! Deterministic rendezvous for concurrent failpoint tests.
//!
//! The pattern: park the FIRST thread that hits a failpoint until the test
//! explicitly releases it, while later arrivals fall through. This replaces
//! fixed "guess" `sleep`s for cross-thread coordination — the test waits on
//! the *condition* (the point was reached) with a bounded timeout that fails
//! loudly, instead of betting a fixed duration is long enough.
//!
//! Extracted from the open-coded `AtomicBool` + callback pattern that
//! `fork_collision_with_live_concurrent_fork_is_retryable` proved out.
//!
//! The `reached` flag also doubles as a fired-assertion: a point that is
//! never hit makes [`Rendezvous::wait_until_reached`] panic, so a typo'd or
//! misplaced failpoint cannot pass silently.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

use omnigraph::failpoints::ScopedFailPoint;

/// A parked-on-first-arrival rendezvous bound to a failpoint name. The
/// underlying callback is RAII-cleaned when this guard drops.
pub struct Rendezvous {
    name: String,
    reached: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    _failpoint: ScopedFailPoint,
}

impl Rendezvous {
    /// Register `name` so the FIRST thread to hit it records readiness and
    /// blocks until [`release`](Self::release); later arrivals fall through
    /// immediately. The park is bounded (~30s) so a test bug cannot hang the
    /// suite forever.
    pub fn park_first(name: &str) -> Self {
        let reached = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let (cb_reached, cb_release) = (Arc::clone(&reached), Arc::clone(&release));
        let _failpoint = ScopedFailPoint::with_callback(name, move || {
            if cb_reached
                .compare_exchange(false, true, SeqCst, SeqCst)
                .is_ok()
            {
                // ~30s bound (6000 * 5ms); released earlier on the common path.
                for _ in 0..6000 {
                    if cb_release.load(SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });
        Self {
            name: name.to_string(),
            reached,
            release,
            _failpoint,
        }
    }

    /// Async-wait until the parked thread has reached the failpoint, polling
    /// the readiness condition with a bounded (~12s) timeout. Panics if the
    /// point is never hit — the fired-assertion.
    pub async fn wait_until_reached(&self) {
        for _ in 0..2400 {
            if self.reached.load(SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("rendezvous: failpoint '{}' was never reached", self.name);
    }

    /// Whether the parked thread has reached the failpoint yet.
    pub fn reached(&self) -> bool {
        self.reached.load(SeqCst)
    }

    /// Release the parked thread so it resumes past the failpoint.
    pub fn release(&self) {
        self.release.store(true, SeqCst);
    }
}
