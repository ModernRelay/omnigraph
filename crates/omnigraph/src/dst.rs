//! Deterministic-replay seam for the DST test harness (Cargo feature `dst`).
//!
//! The engine mints ULIDs and microsecond timestamps at scattered, OBSERVABLE
//! sites (commit-graph ids/timestamps, keyless node/edge ids, recovery sidecar
//! op-ids). For a seeded harness run to be bit-reproducible, those sites call
//! [`next_ulid`] / [`now_micros`] here instead of `Ulid::new()` /
//! `SystemTime::now()`.
//!
//! Mirrors `failpoints.rs`: WITHOUT the `dst` feature (or with no provider in
//! scope) both functions fall back to the real source, and the feature block
//! compiles away — **zero production cost**. Under the feature, a per-task
//! seeded provider (`with_seed`) makes ids/timestamps deterministic and exposes
//! a rolling fingerprint for the replay-equality oracle.

use ulid::Ulid;

/// Production fallback: real micros since the Unix epoch.
fn real_now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// A ULID for an observable id. Deterministic under [`with_seed`]; otherwise a
/// real `Ulid::new()`.
pub(crate) fn next_ulid() -> Ulid {
    #[cfg(feature = "dst")]
    {
        if let Ok(u) = imp::DST.try_with(|s| s.next_ulid()) {
            return u;
        }
    }
    Ulid::new()
}

/// Micros since the epoch for an observable timestamp. Deterministic +
/// monotonic under [`with_seed`]; otherwise the real clock.
pub(crate) fn now_micros() -> i64 {
    #[cfg(feature = "dst")]
    {
        if let Ok(t) = imp::DST.try_with(|s| s.now_micros()) {
            return t;
        }
    }
    real_now_micros()
}

#[cfg(feature = "dst")]
mod imp {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::SeqCst};

    use ulid::Ulid;

    pub(super) fn splitmix64(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// Seeded, monotonic id/clock source + a rolling fingerprint of everything
    /// it has minted — the replay-equality signal.
    pub struct DstState {
        seed: u64,
        ids: AtomicU64,
        clock_micros: AtomicI64,
        fingerprint: AtomicU64,
    }

    impl DstState {
        pub fn new(seed: u64) -> Arc<Self> {
            Arc::new(Self {
                seed,
                ids: AtomicU64::new(0),
                // Fixed seed-derived epoch (~2023 in micros), advanced
                // monotonically so ULID timestamps stay sortable.
                clock_micros: AtomicI64::new(1_700_000_000_000_000 + (seed as i64) * 1_000_000),
                fingerprint: AtomicU64::new(splitmix64(seed)),
            })
        }
        fn mix(&self, v: u64) {
            let cur = self.fingerprint.load(SeqCst);
            self.fingerprint.store(splitmix64(cur ^ v), SeqCst);
        }
        pub fn next_ulid(&self) -> Ulid {
            let n = self.ids.fetch_add(1, SeqCst);
            let ms = (self.clock_micros.load(SeqCst) / 1000) as u64;
            // 80-bit random part derived from (seed, counter) — unique + deterministic.
            let random = ((splitmix64(self.seed ^ n) as u128) << 16) | (n as u128 & 0xFFFF);
            self.mix(n ^ ms);
            Ulid::from_parts(ms, random)
        }
        pub fn now_micros(&self) -> i64 {
            // 1ms per call → monotonic, deterministic.
            let t = self.clock_micros.fetch_add(1000, SeqCst);
            self.mix(t as u64);
            t
        }
        pub fn fingerprint(&self) -> u64 {
            self.fingerprint.load(SeqCst)
        }
    }

    tokio::task_local! {
        pub static DST: Arc<DstState>;
    }
}

/// Run `fut` with a seeded deterministic id/clock provider in scope; returns the
/// future's output and the engine's id/clock FINGERPRINT for that run. Two runs
/// of the same seeded workload must produce the same fingerprint — the
/// replay-equality oracle. Only available under the `dst` feature.
#[cfg(feature = "dst")]
pub async fn with_seed<F>(seed: u64, fut: F) -> (F::Output, u64)
where
    F: std::future::Future,
{
    let state = imp::DstState::new(seed);
    let handle = std::sync::Arc::clone(&state);
    let out = imp::DST.scope(state, fut).await;
    (out, handle.fingerprint())
}
