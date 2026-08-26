//! Process-env DISCIPLINE for the DST suite: nothing in this crate may
//! call `std::env::set_var`/`remove_var` after process startup (UB under
//! threads on POSIX; a source guard in scenarios.rs enforces the ban).
//! The pool-quiescing trio arrives as process-start env instead: the
//! crate-local `.cargo/config.toml` `[env]` table supplies it to every
//! cargo-spawned process, the lane B parents pass it via `Command::env`,
//! and the sharded fleet wrappers set their own (possibly unquiesced)
//! values per shard process.

/// The pool-quiescing trio with its quiesced values — the ONE Rust home
/// (the crate-local `.cargo/config.toml` `[env]` table is the TOML copy).
/// Spawn sites pass the pairs; `require_pool_env` asserts the keys.
pub const QUIESCE_ENV: [(&str, &str); 3] = [
    ("RAYON_NUM_THREADS", "1"),
    ("LANCE_CPU_THREADS", "1"),
    ("LANCE_DETERMINISTIC_BACKOFF", "1"),
];

/// Assert the pool env arrived before this process started. Presence,
/// not value: the fleet wrappers deliberately unquiesce per shard.
///
/// # Panics
/// When any `QUIESCE_ENV` key is absent from the process environment.
pub fn require_pool_env() {
    for (key, _) in QUIESCE_ENV {
        assert!(
            std::env::var_os(key).is_some(),
            "{key} is unset: the DST suite requires the pool env trio {:?} at \
             process start (run cargo from crates/omnigraph-dst so its .cargo \
             config [env] applies, or pass the trio to the spawned binary)",
            QUIESCE_ENV.map(|(k, _)| k)
        );
    }
}
