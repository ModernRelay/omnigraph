//! Diagnostic types for the schema-lint chassis (MR-694).
//!
//! Every schema-migration diagnostic carries a stable code (`OG-XXX-NNN`),
//! a family grouping (DS / MF / CD / …), and a safety tier
//! (safe / validated / destructive). The code is the public identity;
//! external tooling and operators reference rules by code, not by message.
//!
//! This module is the chassis-level vocabulary. The concrete code catalog
//! lives in [`super::codes`]; emission sites are in
//! `catalog::schema_plan` and (future) other lint passes.

/// Family groupings for schema-lint rules. Mirrors the Atlas analyzer
/// families (DS / MF / CD / BC / NM) plus four omnigraph-native families
/// for vector/embedding (VE), edge topology (ED), lock/cost (LK), and
/// non-linear branch divergence (NL). Ownership (OW) is reserved for
/// per-resource Cedar policy integration (MR-722).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    /// Destructive — data-loss class.
    DS,
    /// Maybe-fail — data-dependent, may fail on existing entities.
    MF,
    /// Constraint deletion — invariant relaxation; consumer-warning.
    CD,
    /// Backward incompatible — rename or shape change that breaks clients.
    BC,
    /// Naming conventions.
    NM,
    /// Ownership — per-resource access control.
    OW,
    /// Non-linear — branch-merge schema-state divergence.
    NL,
    /// Vector / embedding — omnigraph-native.
    VE,
    /// Edge / graph topology — omnigraph-native.
    ED,
    /// Lock duration / cost — omnigraph-native.
    LK,
}

impl Family {
    pub fn prefix(self) -> &'static str {
        match self {
            Self::DS => "DS",
            Self::MF => "MF",
            Self::CD => "CD",
            Self::BC => "BC",
            Self::NM => "NM",
            Self::OW => "OW",
            Self::NL => "NL",
            Self::VE => "VE",
            Self::ED => "ED",
            Self::LK => "LK",
        }
    }
}

/// Tier classification for a schema-lint rule. Plan output shows it beside
/// the code of an `UnsupportedChange` step
/// (`SchemaMigrationStep::diagnostic`); apply refuses those steps whatever
/// their tier, and no flag or option unlocks one:
/// - `Safe`: would apply without a scan.
/// - `Validated`: would require a single-pass scan of existing entities.
/// - `Destructive`: would lose data or narrow values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SafetyTier {
    Safe,
    Validated,
    Destructive,
}

/// Severity for a diagnostic at the user-facing surface. Defaults are set
/// per code in [`super::codes`]; operators override via `omnigraph.yaml`
/// (planned for a follow-up PR).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    /// Blocks apply.
    Error,
    /// Reported but doesn't block.
    Warn,
    /// Informational; doesn't block.
    Info,
}
