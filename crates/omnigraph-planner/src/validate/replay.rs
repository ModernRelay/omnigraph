//! Replay through acceptance (RFC 0047, "Execution and replay use the same
//! acceptance path"). A saved plan travels in a [`ReplayEnvelope`] with the
//! inputs of its query and the scope it was accepted under; replay decodes
//! it within the evidence byte limit, refuses unsupported versions before
//! decoding the plan, and accepts it again against requirements derived
//! afresh from the recompiled query. Deserialization never builds an
//! accepted plan, and a serialized type context or binding cannot validate
//! itself.

use omnigraph_compiler::CatalogIdentity;
use omnigraph_compiler::catalog::Catalog;
use serde::{Deserialize, Serialize};

use super::budget::Budget;
use super::subset::Derivation;
use super::{
    AcceptInput, AcceptedBoundPlan, Evidence, ValidationError, ValidationLimits, ValidationScope,
    check,
};
use crate::bound::BoundPlan;

/// The replay envelope's format. A change to its fields or their meaning
/// bumps it; an old envelope is then refused as replan-required, never
/// reinterpreted.
pub const REPLAY_VERSION: u32 = 1;

/// The catalogue of checks and rewrite rules acceptance applies. A rule id
/// keeps its meaning within one version.
pub const RULES_VERSION: u32 = 1;

/// The compiler and operator semantics a plan was built under.
pub const SEMANTICS_VERSION: u32 = 1;

/// The query a saved plan came from: its source and declaration name. Its
/// parameter values are the plan's own bound values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayQuery {
    pub source: String,
    pub name: String,
}

/// A saved plan with everything replay needs to accept it again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayEnvelope {
    pub replay_version: u32,
    pub rules_version: u32,
    pub semantics_version: u32,
    pub query: ReplayQuery,
    /// The scope the plan was accepted under; replay must reach it again.
    pub scope: ValidationScope,
    /// The digest of the accepted schema the plan was accepted under
    /// (`None` for a catalog built from source alone). Replay against a
    /// different schema is refused as incompatible facts.
    pub catalog: Option<String>,
    pub plan: BoundPlan,
    /// The checked derivation of a member of the exact fragment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation: Option<Derivation>,
}

/// The versions of an envelope, read before anything else is decoded.
#[derive(Deserialize)]
struct Header {
    replay_version: u32,
    rules_version: u32,
    semantics_version: u32,
}

impl ReplayEnvelope {
    /// The envelope of an accepted run: its query, its bound plan, the scope
    /// its acceptance reached and the schema it was accepted under.
    pub fn new(
        source: &str,
        name: &str,
        plan: BoundPlan,
        evidence: &Evidence,
        catalog: Option<String>,
    ) -> Self {
        Self {
            replay_version: REPLAY_VERSION,
            rules_version: RULES_VERSION,
            semantics_version: SEMANTICS_VERSION,
            query: ReplayQuery {
                source: source.to_string(),
                name: name.to_string(),
            },
            scope: evidence.scope(),
            catalog,
            plan,
            derivation: evidence.derivation().cloned(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a replay envelope serializes")
    }
}

/// Why replay refused a saved plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayRefusal {
    /// The envelope is absent, obsolete or of an unsupported version: the
    /// caller resupplies the query for fresh planning.
    ReplanRequired { reason: String },
    /// A fact the plan was accepted under differs on the replay target:
    /// replan against the current view.
    IncompatibleFacts { prerequisite: String },
    /// The envelope is malformed, or its plan fails a check against the
    /// query's requirements.
    InvalidEvidence { reason: String },
    /// A configured validation limit ran out.
    Exhausted { limit: &'static str, value: u64 },
}

impl std::fmt::Display for ReplayRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplanRequired { reason } => write!(f, "replan required: {reason}"),
            Self::IncompatibleFacts { prerequisite } => {
                write!(f, "incompatible facts: {prerequisite}")
            }
            Self::InvalidEvidence { reason } => write!(f, "invalid replay evidence: {reason}"),
            Self::Exhausted { limit, value } => {
                write!(f, "validation limit `{limit}` ({value}) exhausted")
            }
        }
    }
}

impl From<ValidationError> for ReplayRefusal {
    fn from(error: ValidationError) -> Self {
        match error {
            ValidationError::Exhausted { limit, value } => Self::Exhausted { limit, value },
            violated @ ValidationError::Violated { .. } => Self::InvalidEvidence {
                reason: violated.to_string(),
            },
        }
    }
}

/// Decode `bytes` as a replay envelope: its length within the evidence byte
/// limit first, then its versions, then the rest.
pub fn decode_replay(
    bytes: &[u8],
    limits: ValidationLimits,
) -> Result<ReplayEnvelope, ReplayRefusal> {
    Budget::new(limits).evidence_bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
    let header: Header =
        serde_json::from_slice(bytes).map_err(|error| ReplayRefusal::ReplanRequired {
            reason: format!("the bytes carry no replay envelope header: {error}"),
        })?;
    for (field, found, supported) in [
        ("replay_version", header.replay_version, REPLAY_VERSION),
        ("rules_version", header.rules_version, RULES_VERSION),
        (
            "semantics_version",
            header.semantics_version,
            SEMANTICS_VERSION,
        ),
    ] {
        if found != supported {
            return Err(ReplayRefusal::ReplanRequired {
                reason: format!(
                    "{field} {found} is not the supported {supported}; resubmit the query"
                ),
            });
        }
    }
    serde_json::from_slice(bytes).map_err(|error| ReplayRefusal::InvalidEvidence {
        reason: format!("the replay envelope does not decode: {error}"),
    })
}

/// Accept a saved plan again: the requirements come from `input`, the query
/// recompiled against the replay target's catalog and bound to the plan's
/// own parameter values, and the plan must reach the scope it was saved
/// under.
pub fn accept_replay(
    envelope: ReplayEnvelope,
    input: &AcceptInput<'_>,
) -> Result<AcceptedBoundPlan, ReplayRefusal> {
    let catalog = catalog_digest(input.catalog);
    if catalog != envelope.catalog {
        return Err(ReplayRefusal::IncompatibleFacts {
            prerequisite: format!(
                "the plan was accepted under schema {}; the replay target's schema is {}",
                envelope.catalog.as_deref().unwrap_or("<unbound>"),
                catalog.as_deref().unwrap_or("<unbound>")
            ),
        });
    }
    let mut budget = Budget::new(input.limits);
    let (scope, derivation) = check(
        &envelope.plan.plan,
        input,
        envelope.derivation.clone(),
        &mut budget,
    )?;
    if scope != envelope.scope {
        return Err(ReplayRefusal::InvalidEvidence {
            reason: format!(
                "the envelope claims scope {}; replay established {}",
                envelope.scope.as_str(),
                scope.as_str()
            ),
        });
    }
    Ok(AcceptedBoundPlan {
        bound: envelope.plan,
        evidence: Evidence {
            scope,
            limits: input.limits,
            derivation,
        },
    })
}

/// The digest of the accepted schema a catalog projects, `None` for a
/// catalog built from source alone.
pub fn catalog_digest(catalog: &Catalog) -> Option<String> {
    match &catalog.identity {
        CatalogIdentity::Bound(schema) => omnigraph_compiler::schema_ir_hash(schema).ok(),
        CatalogIdentity::SourceUnbound => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bound::ValueTable;
    use crate::physical::PhysicalPlan;

    fn envelope() -> ReplayEnvelope {
        ReplayEnvelope {
            replay_version: REPLAY_VERSION,
            rules_version: RULES_VERSION,
            semantics_version: SEMANTICS_VERSION,
            query: ReplayQuery {
                source: "query q() { match { $d: Doc } return { $d.slug } }".to_string(),
                name: "q".to_string(),
            },
            scope: ValidationScope::ExactSubset,
            catalog: None,
            plan: BoundPlan {
                plan: PhysicalPlan::new(),
                values: ValueTable::default(),
            },
            derivation: Some(Derivation::default()),
        }
    }

    #[test]
    fn an_envelope_reads_back_equal() {
        let bytes = envelope().to_bytes();
        let decoded = decode_replay(&bytes, ValidationLimits::DEFAULT).unwrap();
        assert_eq!(decoded.query, envelope().query);
        assert_eq!(decoded.scope, ValidationScope::ExactSubset);
        assert_eq!(decoded.derivation, Some(Derivation::default()));
    }

    /// The byte limit refuses before anything is decoded, so even bytes that
    /// are no JSON at all exhaust it rather than fail to parse.
    #[test]
    fn bytes_over_the_limit_exhaust_before_decoding() {
        let limits = ValidationLimits {
            evidence_bytes: 8,
            ..ValidationLimits::DEFAULT
        };
        assert_eq!(
            decode_replay(b"not json at all", limits).unwrap_err(),
            ReplayRefusal::Exhausted {
                limit: "evidence_bytes",
                value: 8
            }
        );
    }

    /// A version is read before the rest: an unsupported one asks for the
    /// query again even when the body would not decode, and a missing header
    /// is no envelope.
    #[test]
    fn versions_are_checked_before_the_body() {
        for field in ["replay_version", "rules_version", "semantics_version"] {
            let mut value = serde_json::to_value(envelope()).unwrap();
            value[field] = serde_json::json!(u32::MAX);
            value["plan"] = serde_json::json!("not a plan");
            let refusal = decode_replay(
                &serde_json::to_vec(&value).unwrap(),
                ValidationLimits::DEFAULT,
            )
            .unwrap_err();
            assert!(
                matches!(&refusal, ReplayRefusal::ReplanRequired { reason } if reason.contains(field)),
                "{refusal:?}"
            );
        }
        assert!(matches!(
            decode_replay(br#"{"plan": {}}"#, ValidationLimits::DEFAULT).unwrap_err(),
            ReplayRefusal::ReplanRequired { .. }
        ));
        let mut value = serde_json::to_value(envelope()).unwrap();
        value["plan"] = serde_json::json!("not a plan");
        assert!(matches!(
            decode_replay(
                &serde_json::to_vec(&value).unwrap(),
                ValidationLimits::DEFAULT
            )
            .unwrap_err(),
            ReplayRefusal::InvalidEvidence { .. }
        ));
    }
}
