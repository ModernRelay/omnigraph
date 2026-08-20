//! Link-time OS-entropy interposition — the mad-turmoil pattern
//! (studied 2026-08-10 from mad-turmoil @ f2dc755).
//!
//! Defining these `#[no_mangle]` C-ABI symbols in a crate that only sim/test
//! binaries link makes the linker resolve EVERY process-wide entropy request
//! (std's `HashMap` RandomState, `rand`'s thread-local init, Lance's
//! `rand::rng()` calls in transitive deps) to us instead of libc.
//!
//! Arming is ENV-GATED (`DST_ENTROPY_SEED=<u64>` before process start) OR
//! PROGRAMMATIC: [`arm`] (re)seeds the stream mid-process — added for
//! the jitter-leak fix, where `run_universe` arms a per-universe stream and
//! forces `rand::rng().reseed()` so Lance's jittered retry backoff
//! (`lance-core utils/backoff.rs` draws `rand::rng()` — REAL entropy)
//! becomes a replayable function of the universe seed. Unarmed, requests
//! pass through to real `/dev/urandom` (byte-for-byte the OS behavior).
//! Caveat that motivated the original env-only gate still holds for
//! FIRST-USE consumers (std's `HashMap` RandomState seeds once per process
//! and cannot be re-seeded) — programmatic arming only governs consumers
//! that can be forced to re-pull (ThreadRng via `reseed()`), which is
//! exactly the backoff case.
//!
//! Covers: `getrandom` (Linux), `getentropy` (Linux+macOS — Rust std and the
//! `getrandom` crate route here on macOS), `CCRandomGenerateBytes` (macOS
//! CommonCrypto path). Does NOT cover: raw `SYS_getrandom` syscalls (none
//! known in our dep tree — the cross-process probe is the empirical check),
//! thread scheduling, ASLR.

use std::sync::{Mutex, OnceLock};

use crate::rand::SplitMix64;

static STREAM: Mutex<Option<SplitMix64>> = Mutex::new(None);
static ENV_INIT: OnceLock<()> = OnceLock::new();

fn ensure_env_init() {
    ENV_INIT.get_or_init(|| {
        if let Some(seed) = std::env::var("DST_ENTROPY_SEED")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            *STREAM.lock().expect("entropy stream lock") = Some(SplitMix64(seed));
        }
    });
}

/// (Re)arm the shim with a fresh stream — every subsequent entropy request
/// in the process is filled from it. `run_universe` calls this per universe
/// (then `rand::rng().reseed()`), so replays of one seed draw identical
/// jitter streams regardless of what earlier universes consumed.
pub fn arm(seed: u64) {
    ensure_env_init();
    *STREAM.lock().expect("entropy stream lock") = Some(SplitMix64(seed));
}

/// Fill from the seeded stream; false when the shim is disarmed.
fn fill_seeded(dest: &mut [u8]) -> bool {
    ensure_env_init();
    let mut guard = STREAM.lock().expect("entropy stream lock");
    let Some(rng) = guard.as_mut() else {
        return false;
    };
    for chunk in dest.chunks_mut(8) {
        let bytes = rng.next_u64().to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    true
}

fn fill_with_dev_urandom(dest: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read;
    let mut file = std::fs::File::open("/dev/urandom")?;
    file.read_exact(dest)
}

/// <https://man7.org/linux/man-pages/man2/getrandom.2.html>
#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getrandom(buf: *mut u8, buflen: usize, _flags: u32) -> isize {
    if buf.is_null() || buflen == 0 {
        return -1;
    }
    let dest = unsafe { std::slice::from_raw_parts_mut(buf, buflen) };
    if fill_seeded(dest) {
        return buflen as isize;
    }
    if fill_with_dev_urandom(dest).is_err() {
        return -1;
    }
    buflen as isize
}

/// <https://man7.org/linux/man-pages/man3/getentropy.3.html>
#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getentropy(buf: *mut u8, buflen: usize) -> i32 {
    if buflen > 256 {
        return -1;
    }
    match unsafe { getrandom(buf, buflen, 0) } {
        -1 => -1,
        _ => 0,
    }
}

/// macOS CommonCrypto entry (see mad-turmoil's citation of
/// <https://blog.xoria.org/randomness-on-apple-platforms/>).
#[cfg(target_os = "macos")]
#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn CCRandomGenerateBytes(buf: *mut u8, buflen: usize) -> i32 {
    if unsafe { getrandom(buf, buflen, 0) } as i32 != -1 {
        0
    } else {
        -1
    }
}
