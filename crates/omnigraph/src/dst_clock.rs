//! DST: thread-local injectable wall clock — the time seam,
//! twin of `dst_ids`. Routes the engine's behavior-feeding and stamp-feeding
//! wall-clock reads (optimize cutoff, schema-apply cutoff, recovery
//! `started_at` stamps) through one function.
//!
//! Uninstalled (default, production): real `Utc::now()` / `SystemTime::now()`.
//! Installed (harness thread): a logical clock — fixed epoch + strictly-monotonic
//! counter, one millisecond per read — deterministic across runs and
//! processes. Monotonic so stamp ordering matches event ordering, like real
//! time would.

use std::cell::Cell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};

/// 2026-01-01T00:00:00Z — the logical epoch when installed.
const LOGICAL_EPOCH_MS: u64 = 1_767_225_600_000;

thread_local! {
    static INSTALLED: Cell<bool> = const { Cell::new(false) };
    static TICKS: Cell<u64> = const { Cell::new(0) };
}

/// Install the logical clock on THIS thread (harness/test API).
pub fn install_logical_clock() {
    INSTALLED.with(|installed| installed.set(true));
    TICKS.with(|ticks| ticks.set(0));
}

/// Uninstall: reads on this thread return to the real clock.
pub fn uninstall_logical_clock() {
    INSTALLED.with(|installed| installed.set(false));
}

fn next_logical_ms() -> Option<u64> {
    if !INSTALLED.with(|installed| installed.get()) {
        return None;
    }
    TICKS.with(|ticks| {
        let t = ticks.get() + 1;
        ticks.set(t);
        Some(LOGICAL_EPOCH_MS + t)
    })
}

/// Seam for `chrono::Utc::now()` call sites.
pub(crate) fn now_utc() -> DateTime<Utc> {
    match next_logical_ms() {
        Some(ms) => Utc
            .timestamp_millis_opt(ms as i64)
            .single()
            .expect("logical epoch is a valid timestamp"),
        None => Utc::now(),
    }
}

/// Seam for `std::time::SystemTime::now()` call sites.
pub(crate) fn system_time_now() -> SystemTime {
    match next_logical_ms() {
        Some(ms) => UNIX_EPOCH + Duration::from_millis(ms),
        None => SystemTime::now(),
    }
}
