//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

mod effective_settings;
mod fix_convergence;
mod fix_coverage;
mod fix_edition_2;
mod law_canonical_forms;
mod law_engine_reference;
mod law_refusal_soundness;
mod law_scalar_evaluators;
mod law_two_editions;
mod layout_hunts;
mod query_corpus;
mod replay_coverage;
mod result_evidence;
mod review_regressions;
