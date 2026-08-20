//! Process-env knobs for the DST suite. `std::env::set_var` is unsafe
//! since edition 2024 (setenv racing getenv on another thread can read
//! freed memory on POSIX); the shared safety argument lives here, and
//! the functions stay `unsafe` so every call site names the contract it
//! is relying on.
//!
//! The contract (each caller's `# Safety` obligation):
//! - Write knobs at the TOP of a test or of `run_universe` /
//!   `run_concurrent_universe`, before that universe's worker threads
//!   exist; the readers that matter (rayon/Lance pool builders, the
//!   backoff shim, trace loggers) run strictly later, inside those
//!   threads.
//! - Sibling tests in the same binary may still run concurrently; that
//!   residual race is the same hazard the per-site `set_var` calls this
//!   module replaced always carried, tolerated because writes cluster
//!   at test start. Two rules keep it from becoming a correctness bug:
//!   every writer of a shared knob stores the same bytes (whichever
//!   write wins, readers see one value — vary knobs per PROCESS, never
//!   per test within one binary), and test-local toggles
//!   (`DST_PREDICT_LOG`, `DST_OP_LOG`) are read only by the writing
//!   test's own universe.

/// Set a suite knob.
///
/// # Safety
/// See the module doc: call before the universe's worker threads exist,
/// and never write a value another test in the same binary writes
/// differently.
pub unsafe fn set(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) };
}

/// Clear a suite knob.
///
/// # Safety
/// As for [`set`]; only meaningful for test-local toggles read solely
/// by the writing test's own universe.
pub unsafe fn unset(key: &str) {
    unsafe { std::env::remove_var(key) };
}

/// The sequential campaign's standard quiesce trio: single-threaded
/// rayon and Lance pools plus deterministic Lance backoff.
///
/// # Safety
/// As for [`set`] (fixed keys and values, so the shared-knob rule holds
/// by construction; the before-worker-threads obligation remains the
/// caller's).
pub unsafe fn quiesce() {
    unsafe {
        set("RAYON_NUM_THREADS", "1");
        set("LANCE_CPU_THREADS", "1");
        set("LANCE_DETERMINISTIC_BACKOFF", "1");
    }
}
