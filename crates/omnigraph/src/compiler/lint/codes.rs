//! Schema-lint code catalog (MR-694 v0).
//!
//! Codes have the form `OG-XXX-NNN` where `XXX` is the family prefix from
//! [`super::diagnostic::Family`]. Each code carries a default safety
//! tier, severity, and short description. Codes are stable: once
//! published, the meaning is frozen and `omnigraph.yaml` may override
//! severity but never the tier or family.
//!
//! ## v0 catalog
//!
//! This PR (chassis v0) seeds the catalog with the codes attached to
//! existing `UnsupportedChange` emissions in
//! `catalog::schema_plan`. Subsequent PRs add codes for new migration
//! variants (MR-695..702), the CD/VE/LK/NM families, and so on.

use super::diagnostic::{Family, SafetyTier, Severity};

/// Static catalog entry for a single diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticCode {
    /// The stable code string, e.g. `"OG-DS-104"`.
    pub code: &'static str,
    pub family: Family,
    pub tier: SafetyTier,
    pub default_severity: Severity,
    pub short: &'static str,
}

// ─── Destructive (DS) — data-loss; always requires explicit opt-in ──────────

/// Reserved: dropping an entire graph (schema-level). Not yet emitted.
pub const OG_DS_101: DiagnosticCode = DiagnosticCode {
    code: "OG-DS-101",
    family: Family::DS,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "drop graph type with rows",
};

/// Drop a node type that has rows.
pub const OG_DS_102: DiagnosticCode = DiagnosticCode {
    code: "OG-DS-102",
    family: Family::DS,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "drop node type with rows",
};

/// Drop an edge type that has rows.
pub const OG_DS_103: DiagnosticCode = DiagnosticCode {
    code: "OG-DS-103",
    family: Family::DS,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "drop edge type with rows",
};

/// Drop a property (column) that has data.
pub const OG_DS_104: DiagnosticCode = DiagnosticCode {
    code: "OG-DS-104",
    family: Family::DS,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "drop property with rows",
};

/// Reserved: dropping a populated vector / embedding column. Distinct
/// from a normal property drop because it invalidates downstream
/// `nearest()` / `@embed` references.
pub const OG_DS_105: DiagnosticCode = DiagnosticCode {
    code: "OG-DS-105",
    family: Family::DS,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "drop populated vector column",
};

// ─── Maybe-fail (MF) — data-dependent; may fail on existing rows ────────────

/// Add a required (non-nullable) property to a populated type without
/// `@default`. Existing rows have no value to fill in.
pub const OG_MF_103: DiagnosticCode = DiagnosticCode {
    code: "OG-MF-103",
    family: Family::MF,
    tier: SafetyTier::Validated,
    default_severity: Severity::Error,
    short: "add required property without @default to populated type",
};

/// Tighten nullable to non-nullable. May fail on existing null rows.
/// Reserved for a follow-up that wires the validated-tier scan; not
/// emitted in v0.
pub const OG_MF_104: DiagnosticCode = DiagnosticCode {
    code: "OG-MF-104",
    family: Family::MF,
    tier: SafetyTier::Validated,
    default_severity: Severity::Error,
    short: "tighten nullable to non-nullable",
};

/// Narrow a scalar (e.g. I64 → I32, F64 → F32). Lossy cast that may
/// truncate or overflow. Today emitted on any `prop_type` change since
/// the v1 planner doesn't yet distinguish widening from narrowing.
pub const OG_MF_106: DiagnosticCode = DiagnosticCode {
    code: "OG-MF-106",
    family: Family::MF,
    tier: SafetyTier::Destructive,
    default_severity: Severity::Error,
    short: "narrowing scalar type",
};

/// All v0 catalog entries. Used for chassis-level invariants
/// (uniqueness, family coverage).
pub const ALL_CODES: &[DiagnosticCode] = &[
    OG_DS_101, OG_DS_102, OG_DS_103, OG_DS_104, OG_DS_105, OG_MF_103, OG_MF_104, OG_MF_106,
];

/// Codes actually emitted by the planner in v0 (i.e. not reserved).
pub const EMITTED_IN_V0: &[&str] = &[
    "OG-DS-102",
    "OG-DS-103",
    "OG-DS-104",
    "OG-MF-103",
    "OG-MF-106",
];

/// Look up a code by its string identifier.
pub fn lookup(code: &str) -> Option<&'static DiagnosticCode> {
    ALL_CODES.iter().find(|c| c.code == code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for c in ALL_CODES {
            assert!(seen.insert(c.code), "duplicate code: {}", c.code);
        }
    }

    #[test]
    fn code_strings_match_family_prefix() {
        for c in ALL_CODES {
            let expected_prefix = format!("OG-{}-", c.family.prefix());
            assert!(
                c.code.starts_with(&expected_prefix),
                "{} doesn't start with {}",
                c.code,
                expected_prefix
            );
        }
    }

    #[test]
    fn code_strings_have_three_digit_suffix() {
        for c in ALL_CODES {
            let suffix = c.code.rsplit('-').next().unwrap();
            assert_eq!(suffix.len(), 3, "{}: suffix not 3 chars", c.code);
            assert!(
                suffix.chars().all(|ch| ch.is_ascii_digit()),
                "{}: suffix not all digits",
                c.code
            );
        }
    }

    #[test]
    fn destructive_tier_defaults_to_error_severity() {
        for c in ALL_CODES {
            if c.tier == SafetyTier::Destructive {
                assert_eq!(
                    c.default_severity,
                    Severity::Error,
                    "{}: destructive must default to Error",
                    c.code
                );
            }
        }
    }

    #[test]
    fn lookup_finds_known_codes() {
        assert_eq!(lookup("OG-DS-104"), Some(&OG_DS_104));
        assert_eq!(lookup("OG-MF-103"), Some(&OG_MF_103));
        assert!(lookup("OG-XX-999").is_none());
    }

    #[test]
    fn emitted_codes_exist_in_catalog() {
        for code in EMITTED_IN_V0 {
            assert!(
                lookup(code).is_some(),
                "EMITTED_IN_V0 contains {} but it isn't in ALL_CODES",
                code
            );
        }
    }
}
