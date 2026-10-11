//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.
#![recursion_limit = "256"]

mod lane_b;
mod torn_init;
