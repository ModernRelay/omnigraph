//! DST: thread-local injectable ULID source — the identity
//! seam. The ~20 production `ulid::Ulid::new()` sites route through
//! [`new_ulid`], which is a transparent passthrough unless installed.
//!
//! Uninstalled (default, production): delegates to `ulid::Ulid::new()` — behavior
//! byte-identical to before the seam. Installed with a seed (harness thread only):
//! ULIDs come from a SplitMix64 stream with a logical-counter timestamp —
//! deterministic across runs and processes, monotonic per thread (so
//! ULID-sorted listings preserve creation order, as with real timestamps).
//!
//! Thread-local on purpose: parallel tests in one binary cannot drain each
//! other's streams, and the DST harness is single-threaded by construction so
//! one installed thread covers every mint in the simulation.

#[cfg(feature = "dst")]
use std::cell::RefCell;

#[cfg(feature = "dst")]
struct SeededUlids {
    state: u64,
    counter: u64,
}

#[cfg(feature = "dst")]
thread_local! {
    static INSTALLED: RefCell<Option<SeededUlids>> = const { RefCell::new(None) };
}

#[cfg(feature = "dst")]
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Install seeded ULID generation on THIS thread (harness/test API).
#[cfg(feature = "dst")]
pub fn install_seeded_ulids(seed: u64) {
    INSTALLED.with(|slot| {
        *slot.borrow_mut() = Some(SeededUlids {
            state: seed,
            counter: 0,
        })
    });
}

/// Uninstall: minting on this thread returns to real `Ulid::new()`.
#[cfg(feature = "dst")]
pub fn uninstall_seeded_ulids() {
    INSTALLED.with(|slot| *slot.borrow_mut() = None);
}

/// Every production identity mint in this crate comes through here.
/// Without the `dst` feature this is a direct `Ulid::new()` — no
/// thread-local probe, no override authority anywhere in the build.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub(crate) fn new_ulid() -> ulid::Ulid {
    ulid::Ulid::new()
}

/// Every production identity mint in this crate comes through here.
#[cfg(feature = "dst")]
pub(crate) fn new_ulid() -> ulid::Ulid {
    INSTALLED.with(|slot| match slot.borrow_mut().as_mut() {
        Some(installed) => {
            installed.counter += 1;
            let hi = splitmix64(&mut installed.state) as u128;
            let lo = splitmix64(&mut installed.state) as u128;
            let random = ((hi << 64) | lo) & ((1u128 << 80) - 1);
            ulid::Ulid::from_parts(installed.counter, random)
        }
        None => ulid::Ulid::new(),
    })
}
