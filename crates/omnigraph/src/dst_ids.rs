//! The identity seam: the ~20 production `ulid::Ulid::new()` sites route
//! through [`new_ulid`], a transparent passthrough unless a source is
//! installed.
//!
//! Uninstalled (default, production): `ulid::Ulid::new()`, byte-identical to
//! before the seam. Installed (a harness thread): [`SeededUlids`], a
//! SplitMix64 stream with a logical-counter timestamp, deterministic across
//! runs and processes, monotonic per thread so ULID-sorted listings keep
//! creation order as real timestamps would.
//!
//! The slot is thread-local on purpose: parallel tests in one binary cannot
//! drain each other's streams, and the DST harness is single-threaded by
//! construction, so one installed thread covers every mint in a simulation.

#[cfg(feature = "dst")]
use std::cell::Cell;

#[cfg(feature = "dst")]
use omnigraph_seams::{Behavior, Op};

/// What the identity seam holds: a ULID source.
#[cfg(feature = "dst")]
pub trait IdSource: Behavior {
    fn next_ulid(&self) -> ulid::Ulid;
}

/// Seeded SplitMix64 stream with a counter timestamp.
#[cfg(feature = "dst")]
pub struct SeededUlids {
    state: Cell<u64>,
    counter: Cell<u64>,
}

#[cfg(feature = "dst")]
impl SeededUlids {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Cell::new(seed),
            counter: Cell::new(0),
        }
    }
}

#[cfg(feature = "dst")]
fn splitmix64(state: &Cell<u64>) -> u64 {
    let next = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
    state.set(next);
    let mut z = next;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(feature = "dst")]
impl Behavior for SeededUlids {}

#[cfg(feature = "dst")]
impl IdSource for SeededUlids {
    fn next_ulid(&self) -> ulid::Ulid {
        self.counter.set(self.counter.get() + 1);
        let hi = splitmix64(&self.state) as u128;
        let lo = splitmix64(&self.state) as u128;
        let random = ((hi << 64) | lo) & ((1u128 << 80) - 1);
        ulid::Ulid::from_parts(self.counter.get(), random)
    }
}

#[cfg(feature = "dst")]
omnigraph_seams::thread_local_seam! {
    /// The identity seam. Install a [`SeededUlids`] on the harness thread.
    pub static IDS: dyn IdSource = ("ids", Op::Unreachable);
}

/// Every production identity mint in this crate comes through here.
/// Without the `dst` feature this is a direct `Ulid::new()`: no
/// thread-local probe, no override authority anywhere in the build.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn new_ulid() -> ulid::Ulid {
    ulid::Ulid::new()
}

/// Every production identity mint in this crate comes through here.
#[cfg(feature = "dst")]
pub(crate) fn new_ulid() -> ulid::Ulid {
    match IDS.with(|source| source.next_ulid()) {
        Some(ulid) => ulid,
        None => ulid::Ulid::new(),
    }
}
