//! Typed, versioned definitions for the OmniGraph benchmark harness.
//!
//! A case describes exactly one experiment. A suite only selects checked-in
//! cases and says how many samples to collect; it cannot override experiment
//! identity. Execution and record persistence live in later harness slices.

pub mod case;
pub mod model;
pub mod suite;

pub use case::{CaseV1, PointIdentityV1, ValidatedCase, load_case, parse_case, validate_case};
pub use model::{Diagnostic, DiagnosticSeverity, ValidationOutcome};
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
