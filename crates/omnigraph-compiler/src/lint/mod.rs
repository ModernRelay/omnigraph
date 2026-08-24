//! Stable schema-migration diagnostics.
//!
//! Stable diagnostic codes (`OG-XXX-NNN`) for schema-migration plans,
//! attached to migration-plan rejections.
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
//! The user-visible code contract is documented in
//! `docs/user/schema/index.md`.

pub mod codes;
pub mod diagnostic;

pub use codes::{ALL_CODES, DiagnosticCode, lookup};
pub use diagnostic::{Family, SafetyTier, Severity};
