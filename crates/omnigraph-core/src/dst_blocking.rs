//! The blocking-work seam: where a graph operator's body is polled, sibling
//! of `dst_gate`, `dst_clock` and `dst_ids`.
//!
//! Uninstalled (default, production): the engine polls each operator body on
//! a `spawn_blocking` thread, so CPU-heavy work stays off the async workers.
//! Installed (a harness thread running a seeded current-thread runtime): the
//! body is polled on that runtime, as one more task the seed orders. Two
//! bodies that charge one memory pool then interleave the same way on every
//! replay, so a refusal's reported size is a function of the seed.

#[cfg(feature = "dst")]
use omnigraph_seams::{Behavior, Op};

/// What the blocking seam holds: installing it is the whole signal.
#[cfg(feature = "dst")]
pub trait Placement: Behavior {}

/// The one placement: bodies run on the installing thread's runtime.
#[cfg(feature = "dst")]
pub struct OnSeededRuntime;

#[cfg(feature = "dst")]
impl Behavior for OnSeededRuntime {}

#[cfg(feature = "dst")]
impl Placement for OnSeededRuntime {}

#[cfg(feature = "dst")]
omnigraph_seams::thread_local_seam! {
    /// The blocking seam. Install [`OnSeededRuntime`] on the harness thread.
    pub static BLOCKING: dyn Placement = ("blocking", Op::Unreachable);
}

/// Whether operator bodies on this thread run on its runtime instead of a
/// blocking thread. Without the `dst` feature this inlines to `false`.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn on_runtime() -> bool {
    false
}

/// Whether operator bodies on this thread run on its runtime instead of a
/// blocking thread.
#[cfg(feature = "dst")]
pub fn on_runtime() -> bool {
    BLOCKING.with(|_| ()).is_some()
}
