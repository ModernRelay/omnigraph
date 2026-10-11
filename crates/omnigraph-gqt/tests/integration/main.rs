//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

mod effective_settings;
mod replay_coverage;
mod result_evidence;
mod review_regressions;
