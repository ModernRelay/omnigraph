//! The time seam: every behavior-feeding and stamp-feeding wall-clock read
//! in the engine (optimize cutoff, schema-apply cutoff, recovery `started_at`
//! stamps) goes through [`now_utc`] or [`system_time_now`].
//!
//! Uninstalled (default, production): real `Utc::now()` / `SystemTime::now()`.
//! Installed (a harness thread): a [`LogicalClock`], a fixed epoch plus a
//! strictly monotonic counter, one millisecond per read, deterministic across
//! runs and processes. The slot is thread-local: each harness thread owns its
//! own clock, and a thread with no clock reads the real one.

#[cfg(feature = "dst")]
use std::cell::Cell;
use std::time::SystemTime;
#[cfg(feature = "dst")]
use std::time::{Duration, UNIX_EPOCH};

#[cfg(feature = "dst")]
use chrono::TimeZone;
use chrono::{DateTime, Utc};

#[cfg(feature = "dst")]
use omnigraph_seams::{Behavior, Op};

/// 2026-01-01T00:00:00Z, the logical epoch when installed.
#[cfg(feature = "dst")]
pub const LOGICAL_EPOCH_MS: u64 = 1_767_225_600_000;

/// What the clock seam holds: a source of milliseconds since the Unix epoch.
#[cfg(feature = "dst")]
pub trait Clock: Behavior {
    fn now_ms(&self) -> u64;
}

/// Fixed epoch plus one millisecond per read, so stamp order matches event
/// order the way real time would.
#[cfg(feature = "dst")]
#[derive(Default)]
pub struct LogicalClock {
    ticks: Cell<u64>,
}

#[cfg(feature = "dst")]
impl Behavior for LogicalClock {}

#[cfg(feature = "dst")]
impl Clock for LogicalClock {
    fn now_ms(&self) -> u64 {
        let t = self.ticks.get() + 1;
        self.ticks.set(t);
        LOGICAL_EPOCH_MS + t
    }
}

#[cfg(feature = "dst")]
omnigraph_seams::thread_local_seam! {
    /// The clock seam. Install a [`LogicalClock`] on the harness thread.
    pub static CLOCK: dyn Clock = ("clock", Op::Unreachable);
}

#[cfg(feature = "dst")]
fn next_logical_ms() -> Option<u64> {
    CLOCK.with(|clock| clock.now_ms())
}

/// Seam for `chrono::Utc::now()` call sites. Without the `dst` feature:
/// the real clock, directly.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

/// Seam for `std::time::SystemTime::now()` call sites. Without the
/// `dst` feature: the real clock, directly.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn system_time_now() -> SystemTime {
    SystemTime::now()
}

/// Seam for `chrono::Utc::now()` call sites.
#[cfg(feature = "dst")]
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
#[cfg(feature = "dst")]
pub(crate) fn system_time_now() -> SystemTime {
    match next_logical_ms() {
        Some(ms) => UNIX_EPOCH + Duration::from_millis(ms),
        None => SystemTime::now(),
    }
}
