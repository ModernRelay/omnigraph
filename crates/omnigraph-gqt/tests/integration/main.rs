//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

mod effective_settings;
mod fix_convergence;
mod law_engine_reference;
mod law_scalar_evaluators;
mod query_corpus;
mod replay_coverage;
mod result_evidence;
mod review_regressions;
