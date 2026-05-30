//! `GraphId` — registry-level identity for a graph in multi-graph mode (MR-668).
//!
//! Validation lives in `GraphId::try_from(String)`; nothing else can construct a
//! `GraphId`. The newtype prevents `graph_id` strings from escaping the storage
//! root via path traversal or colliding with engine-reserved filenames.
//!
//! Regex: `^[a-zA-Z0-9-]{1,64}$`
//!
//! The engine reserves every filename starting with `_` at the graph root
//! (`_schema.pg`, `_schema.ir.json`, `__schema_state.json`, `__manifest/`,
//! `__recovery/`, etc.). Disallowing leading underscores at the regex level
//! means a `graph_id` can never collide with engine-managed files. Path
//! traversal (`..`, `/`) is unrepresentable.
//!
//! `policies` is additionally reserved as a future-proofing measure for a
//! potential `/graphs/policies/...` cluster route.

use std::fmt;
use std::sync::OnceLock;

use color_eyre::eyre::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Maximum length of a `GraphId` value.
pub const GRAPH_ID_MAX_LEN: usize = 64;

/// Validated registry-level identity for a graph.
///
/// Constructed only via `GraphId::try_from(String)` or
/// `GraphId::try_from(&str)`. The inner `String` is private to enforce the
/// validation contract.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphId(String);

impl GraphId {
    /// View the validated identifier as `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for GraphId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for GraphId {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: String) -> Result<Self> {
        validate(value.as_str())?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for GraphId {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: &str) -> Result<Self> {
        validate(value)?;
        Ok(Self(value.to_string()))
    }
}

// Custom Deserialize that re-runs validation. Otherwise a serde-derived impl
// would accept any String, defeating the newtype's guarantee.
impl<'de> Deserialize<'de> for GraphId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

fn validate(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("graph_id must not be empty");
    }
    if value.len() > GRAPH_ID_MAX_LEN {
        bail!(
            "graph_id '{}' is {} chars; max {}",
            value,
            value.len(),
            GRAPH_ID_MAX_LEN
        );
    }
    if !regex().is_match(value) {
        bail!(
            "graph_id '{}' must match ^[a-zA-Z0-9-]{{1,64}}$ — \
             no underscores (engine reserves them), no path separators, no unicode",
            value
        );
    }
    if is_reserved(value) {
        bail!(
            "graph_id '{}' is reserved (would collide with engine-managed names or \
             future cluster routes)",
            value
        );
    }
    Ok(())
}

fn regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9-]{1,64}$").expect("regex literal"))
}

/// Reserved `graph_id` values that the regex alone wouldn't catch.
/// The leading-underscore rule already excludes every engine-managed
/// filename pattern (`_schema.pg`, `__manifest`, etc.); the regex
/// `^[a-zA-Z0-9-]{1,64}$` (see `regex()`) additionally rejects every
/// dot-containing name structurally — `openapi.json` and friends
/// never reach this check.
///
/// This list only needs to cover route-prefix collisions and
/// top-level endpoint names whose spellings DO satisfy the regex
/// (no dots, no underscores).
fn is_reserved(value: &str) -> bool {
    matches!(value, "policies" | "healthz" | "openapi" | "graphs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_simple_alphanumeric_ids() {
        for ok in ["alpha", "beta", "tenant-001", "A", "g", "X-9-z"] {
            GraphId::try_from(ok).unwrap_or_else(|_| panic!("expected accept: {ok}"));
        }
    }

    #[test]
    fn accepts_64_char_max() {
        let max = "a".repeat(64);
        GraphId::try_from(max.as_str()).unwrap();
    }

    #[test]
    fn rejects_empty() {
        assert!(GraphId::try_from("").is_err());
    }

    #[test]
    fn rejects_over_64_chars() {
        let too_long = "a".repeat(65);
        assert!(GraphId::try_from(too_long.as_str()).is_err());
    }

    #[test]
    fn rejects_leading_underscore() {
        // Engine reserves every `_*` filename at the graph root.
        assert!(GraphId::try_from("_internal").is_err());
        assert!(GraphId::try_from("__manifest").is_err());
    }

    #[test]
    fn rejects_underscores_anywhere() {
        // The regex doesn't allow `_` at all — keeps the disallow-leading-`_`
        // rule cheap to enforce. If the rule changes later, we'd need to
        // distinguish "starts with `_`" from "contains `_`".
        assert!(GraphId::try_from("tenant_alpha").is_err());
    }

    #[test]
    fn rejects_path_separators() {
        for bad in ["alpha/beta", "../etc", "..", "alpha\\beta"] {
            assert!(GraphId::try_from(bad).is_err(), "expected reject: {bad}");
        }
    }

    #[test]
    fn rejects_unicode() {
        assert!(GraphId::try_from("αlpha").is_err());
        assert!(GraphId::try_from("graph-✨").is_err());
    }

    #[test]
    fn rejects_whitespace() {
        assert!(GraphId::try_from(" alpha").is_err());
        assert!(GraphId::try_from("alpha ").is_err());
        assert!(GraphId::try_from("alpha beta").is_err());
        assert!(GraphId::try_from("\talpha").is_err());
    }

    #[test]
    fn rejects_dots() {
        // Reserves the "extension"-shaped ids that look like filenames.
        assert!(GraphId::try_from(".").is_err());
        assert!(GraphId::try_from("alpha.beta").is_err());
        assert!(GraphId::try_from("alpha.").is_err());
    }

    #[test]
    fn rejects_reserved_route_names() {
        // Names that satisfy the regex but are still reserved because
        // they'd collide with top-level route prefixes / endpoint names.
        // Dot-containing names (e.g. `openapi.json`) are rejected by the
        // regex, not this list — `rejects_dots` above covers them.
        for bad in ["policies", "healthz", "openapi", "graphs"] {
            assert!(
                GraphId::try_from(bad).is_err(),
                "expected reject (reserved): {bad}"
            );
        }
    }

    #[test]
    fn display_returns_inner_string() {
        let id = GraphId::try_from("alpha").unwrap();
        assert_eq!(format!("{id}"), "alpha");
        assert_eq!(id.as_str(), "alpha");
    }

    #[test]
    fn serialize_round_trips_via_json() {
        let id = GraphId::try_from("tenant-007").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"tenant-007\"");
        let back: GraphId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn deserialize_runs_validation() {
        // Hostile payload must not produce a GraphId.
        let bad = serde_json::from_str::<GraphId>("\"_evil\"");
        assert!(bad.is_err());
        let bad = serde_json::from_str::<GraphId>("\"../../etc\"");
        assert!(bad.is_err());
    }

    #[test]
    fn hash_equality_works_for_use_as_map_key() {
        use std::collections::HashMap;
        let a = GraphId::try_from("alpha").unwrap();
        let b = GraphId::try_from("alpha").unwrap();
        let mut m = HashMap::new();
        m.insert(a, 1u32);
        assert_eq!(m.get(&b), Some(&1));
    }
}
