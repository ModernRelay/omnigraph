//! Stack growth for the walks that recurse once per level of a query.
//!
//! The parser, the type checker, the shaper, the planner and the engine's
//! evaluation each recurse once per level of an expression or a term, and a
//! query may nest thousands of levels (a long `or` chain is a left-deep tree
//! as deep as it is long). Each such walk enters every level through
//! [`deep`], which runs it on the current stack while enough of it remains
//! and otherwise on a new segment allocated for it, so no thread needs a
//! stack sized for the deepest query: the threads keep the platform's
//! default, and a deep query pays in heap for the levels it has.

/// The stack a level may use between two calls of [`deep`] on its walk: a
/// level's frames in a debug build, where they are largest, with room to
/// spare.
pub const RED_ZONE: usize = 256 * 1024;

/// The size of each segment [`deep`] allocates when the stack runs low.
pub const SEGMENT: usize = 4 * 1024 * 1024;

/// The most stack a thread's own stack is trusted to have left. Under an
/// unlimited stack size (`ulimit -s unlimited`), the platform reports the
/// main thread's stack as running to the next mapping, gigabytes away,
/// where the kernel or a sandbox may fault far sooner; a report above this
/// is not believed, and the thread's own stack is spent only to
/// [`UNTRUSTED_BUDGET`] past the shallowest point a walk entered it.
pub const TRUSTED: usize = 64 * 1024 * 1024;

/// How much of a thread's own stack a walk spends when its size is not
/// believed ([`TRUSTED`]): well within the smallest main-thread stack a
/// platform gives.
pub const UNTRUSTED_BUDGET: usize = 2 * 1024 * 1024;

thread_local! {
    /// The shallowest stack address a walk entered at on this thread's own
    /// stack, for [`UNTRUSTED_BUDGET`].
    static ENTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How much stack is left for `f` on the current stack, as far as it is
/// believed: what the platform reports, or, on a thread's own stack it
/// reports as larger than [`TRUSTED`], what remains of
/// [`UNTRUSTED_BUDGET`].
fn believed_remaining() -> usize {
    match stacker::remaining_stack() {
        None => 0,
        Some(remaining) if remaining <= TRUSTED => remaining,
        Some(_) => {
            let marker = 0u8;
            let here = std::ptr::addr_of!(marker) as usize;
            let entered = ENTERED.with(|entered| {
                let top = entered.get().max(here);
                entered.set(top);
                top
            });
            // The stack grows down: what this walk spent since its
            // shallowest entry.
            UNTRUSTED_BUDGET.saturating_sub(entered.saturating_sub(here))
        }
    }
}

/// `f`, on the current stack when at least [`RED_ZONE`] of it remains, else
/// on a new segment of [`SEGMENT`] bytes.
#[inline]
pub fn deep<R>(f: impl FnOnce() -> R) -> R {
    if believed_remaining() >= RED_ZONE {
        f()
    } else {
        stacker::grow(SEGMENT, f)
    }
}

/// The deepest expression handed to code that recurses once per level
/// without entering it through [`deep`]: DataFusion's and Lance's filter
/// planning, a derived trait of a mirror type, a JSON tree. It is the
/// expression depth a query could reach before its depth bound was raised,
/// so whatever was handed over then still is. A deeper expression stays with
/// the code that grows its stack: the engine's own filter evaluates it, and
/// it is outside plan acceptance's exact fragment.
pub const MAX_HANDED_OFF_DEPTH: usize = 256;

/// `f`, which recurses through code that does not enter its levels through
/// [`deep`] (a generated parser), with `bytes` of stack: on the current
/// stack when that much of it remains, else on a segment of its own.
pub fn with_room<R>(bytes: usize, f: impl FnOnce() -> R) -> R {
    let needed = bytes.saturating_add(RED_ZONE);
    if believed_remaining() >= needed {
        f()
    } else {
        stacker::grow(needed, f)
    }
}
