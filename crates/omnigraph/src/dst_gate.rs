//! DST: thread-local injectable WRITE-GATE
//! acquisition hook — the lock seam, sibling of `dst_ids`/`dst_clock`.
//!
//! Uninstalled (default, production): `write_queue` locks block normally.
//! Installed (harness actor threads): acquisitions run a try-acquire loop
//! where every attempt happens inside one harness TURN (the hook returns an
//! opaque turn guard), so waiting at an engine write gate is VISIBLE to the
//! harness arbiter — a contender is *pending at the arbiter* instead of
//! parked invisibly in a tokio lock queue — and the handoff order among
//! contenders becomes the arbiter's seeded choice rather than the OS lock
//! queue's. The hook returning `None` means "behave as uninstalled" (the
//! harness scheduler exists but is not armed yet — setup/teardown).

use std::any::Any;
#[cfg(feature = "dst")]
use std::cell::RefCell;

#[cfg(feature = "dst")]
type TurnHook = Box<dyn Fn() -> Option<Box<dyn Any + Send>>>;

#[cfg(feature = "dst")]
thread_local! {
    static HOOK: RefCell<Option<TurnHook>> = const { RefCell::new(None) };
}

/// Install the acquisition hook on THIS thread (harness/test API). The hook
/// is called once per acquisition attempt; `Some(guard)` scopes exactly one
/// `try_lock` attempt, `None` falls back to plain blocking acquisition.
#[cfg(feature = "dst")]
pub fn install_gate_hook(hook: TurnHook) {
    HOOK.with(|h| *h.borrow_mut() = Some(hook));
}

/// Uninstall: this thread's acquisitions return to plain blocking locks.
#[cfg(feature = "dst")]
pub fn uninstall_gate_hook() {
    HOOK.with(|h| *h.borrow_mut() = None);
}

/// One scheduled attempt token, or `None` (uninstalled, or hook declined).
/// Without the `dst` feature this inlines to `None` — the write queue's
/// hook call sites compile to nothing.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn turn() -> Option<Box<dyn Any + Send>> {
    None
}

/// One scheduled attempt token, or `None` (uninstalled, or hook declined).
#[cfg(feature = "dst")]
pub(crate) fn turn() -> Option<Box<dyn Any + Send>> {
    HOOK.with(|h| h.borrow().as_ref().and_then(|hook| hook()))
}
