//! The lib test binary's global allocator: the system allocator, counting
//! the live bytes of a thread that asked, so a test can bound the peak
//! memory of one call.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// The system allocator, counting the live bytes of a thread that turned
/// `COUNTING` on; a reallocation counts the new block before the old goes.
/// Every `unsafe` block below forwards the call to `System` unchanged.
struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn count(bytes: isize) {
    if COUNTING.get() {
        let live = LIVE.get() + bytes;
        LIVE.set(live);
        PEAK.set(PEAK.get().max(live));
    }
}

// SAFETY: every method forwards the caller's arguments to `System` unchanged; the counters allocate nothing.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is the caller's, passed through unchanged.
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() {
            count(layout.size() as isize);
        }
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is the caller's, passed through unchanged.
        let block = unsafe { System.alloc_zeroed(layout) };
        if !block.is_null() {
            count(layout.size() as isize);
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        // SAFETY: `block` came from `System` through the methods above, with this same `layout`.
        unsafe { System.dealloc(block, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `block` came from `System` with `layout`; `new_size` is the caller's, unchanged.
        let moved = unsafe { System.realloc(block, layout, new_size) };
        if !moved.is_null() {
            count(new_size as isize);
            count(-(layout.size() as isize));
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// `f`'s result and the most bytes this thread held at once while it ran,
/// counted from zero at the call (bytes allocated before it are not counted,
/// and bytes it frees that were allocated before it lower the count).
pub(crate) fn peak_bytes<T>(f: impl FnOnce() -> T) -> (T, usize) {
    LIVE.set(0);
    PEAK.set(0);
    COUNTING.set(true);
    let value = f();
    COUNTING.set(false);
    (value, usize::try_from(PEAK.get()).unwrap_or(0))
}
