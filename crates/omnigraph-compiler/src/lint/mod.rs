//! Schema-lint chassis (MR-694).
//!
//! Stable diagnostic codes (`OG-XXX-NNN`) for schema-migration plans,
//! the foundation for per-rule severity config, suppression directives,
//! and pre-migration checks that subsequent PRs layer on.
//!
//! ## v0 surface
//!
//! - [`diagnostic`] defines [`Family`](diagnostic::Family),
//!   [`SafetyTier`](diagnostic::SafetyTier), and
//!   [`Severity`](diagnostic::Severity).
//! - [`codes`] holds the catalog of [`DiagnosticCode`](codes::DiagnosticCode)
//!   entries; the planner attaches `code: Option<&'static str>` to each
//!   `UnsupportedChange` emission.
//! - The CLI renders the code in `omnigraph schema plan` output; the
//!   apply path includes it in the user-visible error message.
//!
//! Future PRs add: severity config from `omnigraph.yaml`, `@allow(...)`
//! suppression annotations, pre-migration checks (MR-941), the CD / VE /
//! LK / NM families (MR-942..945), and CI integration (MR-946).
//!
//! See: docs/schema-lint.md, https://atlasgo.io/lint/analyzers

pub mod codes;
pub mod diagnostic;

pub use codes::{lookup, DiagnosticCode, ALL_CODES};
pub use diagnostic::{Family, SafetyTier, Severity};
