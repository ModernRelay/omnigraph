//! Typed, versioned definitions for the OmniGraph benchmark harness.
//!
//! A config pairs fixture/workload GQT, expands defaults and selects experiments.
//! Each resolved case describes one experiment; sample counts remain outside
//! experiment identity. Legacy case/suite readers are retained. The local runner consumes those resolved plans, canonical JSON is
//! the durable telemetry authority, and OmniGraph provides a disposable query
//! projection over that archive. Cloud orchestration is a later harness slice.
//!
//! Three levels, each inside the one before. The harness is this crate: it
//! plans the points, prepares and resets the fixture, runs each repetition in
//! a fresh process, verifies, and writes the record. The host (`BenchHost` in
//! [`gqt_runner`]) runs one frozen GQT program inside that process and times
//! the selected step. The driver ([`driver`]) issues the statements of a
//! measured block, and runs only when the selected step is one.
#![recursion_limit = "512"]

pub mod archive;
pub mod branch_merge;
pub mod case;
pub mod catalog;
pub mod check;
pub mod counting;
pub mod dataset_cache;
pub mod dataset_identity;
#[doc(hidden)]
pub mod dataset_worker;
pub mod discovery;
pub mod driver;
pub mod environment;
pub mod export;
pub mod fixture_reference;
#[doc(hidden)]
#[cfg(test)]
pub mod fixture_worker;
pub mod gqt_case;
#[doc(hidden)]
pub mod gqt_evidence;
#[cfg(unix)]
mod gqt_owned_server;
#[doc(hidden)]
pub mod gqt_protocol;
pub mod gqt_record;
pub mod gqt_runner;
pub mod gqt_served;
#[cfg(unix)]
mod gqt_server_process;
mod gqt_supervisor;
#[doc(hidden)]
pub mod gqt_worker;
pub mod legacy;
pub mod machine;
pub mod model;
mod preparation;
pub mod projection;
pub mod real_graph;
#[cfg(test)]
pub mod real_graph_run;
pub mod record;
pub mod registered_fixture;
pub mod reset;
pub mod runner;
#[doc(hidden)]
pub mod source_provenance;
pub mod store;
pub mod suite;
#[cfg(test)]
mod supervisor;
#[doc(hidden)]
#[cfg(test)]
pub mod worker;
#[doc(hidden)]
#[cfg(test)]
pub mod worker_protocol;

pub use case::{CaseV1, PointIdentityV1, ValidatedCase, load_case, parse_case, validate_case};
pub use fixture_reference::{
    FixtureReferenceV1, NormalizedFixtureReferenceV1, load_fixture_reference,
    normalize_fixture_reference, parse_fixture_reference,
};
pub use model::{Diagnostic, DiagnosticSeverity, ValidationOutcome};
pub use runner::{BuildEvidence, RUNNER_OUTPUT_VERSION, RunnerError};
pub use suite::{
    ResolvedRun, ResolvedSuite, SuiteRunV1, SuiteV1, load_suite, parse_suite, validate_suite,
};

/// The only case-file version this crate understands.
pub const CASE_FORMAT_VERSION: u32 = 1;

/// The only suite-file version this crate understands.
pub const SUITE_FORMAT_VERSION: u32 = 1;

/// Version of the canonical typed experiment identity hashed into `point_id`.
pub const POINT_IDENTITY_VERSION: u32 = 1;

/// Version of the CLI's resolved, execution-free suite-plan projection.
pub const PLAN_FORMAT_VERSION: u32 = 1;

pub use gqt_runner::{RunExecution, RunOptions, SuiteExecution, execute_run, execute_suite};

#[cfg(test)]
mod gqt_tests;

#[cfg(test)]
mod catalog_tests;
