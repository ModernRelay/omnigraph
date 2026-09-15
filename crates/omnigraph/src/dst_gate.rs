//! The lock seam: the write-gate acquisition hook, sibling of `dst_ids` and
//! `dst_clock`.
//!
//! Uninstalled (default, production): `write_queue` locks block normally.
//! Installed (a harness actor thread): acquisitions run a try-acquire loop
//! where every attempt happens inside one harness TURN (the hook returns an
//! opaque turn guard), so waiting at an engine write gate is VISIBLE to the
//! harness arbiter, a contender is pending at the arbiter instead of parked
//! invisibly in a tokio lock queue, and the handoff order among contenders
//! becomes the arbiter's seeded choice rather than the OS lock queue's. The
//! hook returning `None` means "behave as uninstalled" (the harness
//! scheduler exists but is not armed yet: setup, teardown).

use std::any::Any;

#[cfg(feature = "dst")]
use omnigraph_seams::{Behavior, Op};

/// One scheduled attempt token: `Some(guard)` scopes exactly one `try_lock`
/// attempt, `None` falls back to plain blocking acquisition.
pub type TurnToken = Box<dyn Any + Send>;

/// What the lock seam holds: called once per acquisition attempt on the
/// installing thread.
#[cfg(feature = "dst")]
pub trait GateHook: Behavior {
    fn turn(&self) -> Option<TurnToken>;
}

/// A closure as a gate hook (the orphan rule keeps the blanket impl behind a
/// local wrapper).
#[cfg(feature = "dst")]
pub struct TurnFn<F>(pub F);

#[cfg(feature = "dst")]
impl<F: Fn() -> Option<TurnToken> + 'static> Behavior for TurnFn<F> {}

#[cfg(feature = "dst")]
impl<F: Fn() -> Option<TurnToken> + 'static> GateHook for TurnFn<F> {
    fn turn(&self) -> Option<TurnToken> {
        (self.0)()
    }
}

#[cfg(feature = "dst")]
omnigraph_seams::thread_local_seam! {
    /// The lock seam. Install the harness's turn hook on each actor thread.
    pub static GATE: dyn GateHook = ("gate", Op::Unreachable);
}

/// One scheduled attempt token, or `None` (uninstalled, or hook declined).
/// Without the `dst` feature this inlines to `None`: the write queue's
/// hook call sites compile to nothing.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn turn() -> Option<TurnToken> {
    None
}

/// One scheduled attempt token, or `None` (uninstalled, or hook declined).
#[cfg(feature = "dst")]
pub(crate) fn turn() -> Option<TurnToken> {
    GATE.with(|hook| hook.turn()).flatten()
}
