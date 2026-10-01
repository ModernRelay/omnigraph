//! Plan acceptance (RFC 0047, "Validation across planning stages"). The one
//! door from a planned query to an executable one: [`accept_query`] plans a
//! read and validates the plan against what the checked declaration
//! requires, and only a successful check constructs an [`AcceptedPlan`],
//! whose fields are private. Ordinary execution, `explain` and the inspected
//! run all call the same planning and validation path; explain renders the
//! accepted plan and never validates more or less than a run does.
//!
//! A fresh plan that fails a check is a planner defect, reported as
//! [`Unrouted::PlannerError`]; it never becomes a language refusal. A check
//! that exhausts a [`ValidationLimits`] budget is a resource outcome
//! ([`Unrouted::ValidationExhausted`]), not evidence that the query or the
//! plan is invalid.

mod budget;
mod invariants;
mod replay;
mod requirements;

pub use budget::{Budget, ValidationLimits};
pub use replay::{
    REPLAY_VERSION, RULES_VERSION, ReplayEnvelope, ReplayQuery, ReplayRefusal, SEMANTICS_VERSION,
    accept_replay, catalog_digest, decode_replay,
};
pub use requirements::{ConstantEvaluator, Requirements};

use omnigraph_compiler::CheckedQuery;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{ParamMap, QueryIR};
use serde::{Deserialize, Serialize};

use crate::bound::{BoundPlan, ValueTable};
use crate::gate::Unrouted;
use crate::physical::PhysicalPlan;

/// How much of the plan's derivation acceptance checked. `exact_subset`:
/// the query is a member of the closed exact fragment and every rewrite from
/// its canonical form to the plan was reconstructed and compared;
/// `invariants_only`: the plan was checked against the query's requirements
/// (search identity, retained predicates, score origin, approximation,
/// order and cut, declared policies), which does not prove row-selection
/// equivalence. Only the validator chooses it; nothing upgrades it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationScope {
    ExactSubset,
    InvariantsOnly,
}

impl ValidationScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExactSubset => "exact_subset",
            Self::InvariantsOnly => "invariants_only",
        }
    }
}

/// What a successful explain document reports about acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ValidationSummary {
    pub scope: ValidationScope,
}

/// What a plan check found wrong, or why it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// The plan breaks a requirement of its query: on a fresh plan a planner
    /// defect, on a supplied plan invalid evidence.
    Violated { check: &'static str, detail: String },
    /// A configured validation limit ran out before the check finished.
    Exhausted { limit: &'static str, value: u64 },
}

impl ValidationError {
    pub(crate) fn violated(check: &'static str, detail: impl Into<String>) -> Self {
        Self::Violated {
            check,
            detail: detail.into(),
        }
    }

    /// The route of a fresh plan's failed check.
    pub(crate) fn into_unrouted(self) -> Unrouted {
        match self {
            Self::Violated { check, detail } => Unrouted::PlannerError {
                message: format!("plan validation failed ({check}): {detail}"),
            },
            Self::Exhausted { limit, value } => Unrouted::ValidationExhausted { limit, value },
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Violated { check, detail } => write!(f, "{check}: {detail}"),
            Self::Exhausted { limit, value } => {
                write!(f, "validation limit `{limit}` ({value}) exhausted")
            }
        }
    }
}

/// Everything acceptance reads beside the plan source: the checked
/// declaration the requirements come from, the catalog it was checked
/// against, the IR the planner plans (constants of filter positions already
/// folded to their bound values), the bound parameter values (`now()`
/// among them), the evaluator of a folded constant, and the limits.
pub struct AcceptInput<'a> {
    pub checked: &'a CheckedQuery,
    pub catalog: &'a Catalog,
    pub ir: &'a QueryIR,
    pub params: &'a ParamMap,
    pub constants: &'a dyn ConstantEvaluator,
    pub limits: ValidationLimits,
}

/// The internal record of one acceptance: its scope and the limits it ran
/// under. Inspection and replay read it; ordinary responses carry only the
/// scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    scope: ValidationScope,
    limits: ValidationLimits,
}

impl Evidence {
    pub fn scope(&self) -> ValidationScope {
        self.scope
    }

    pub fn limits(&self) -> ValidationLimits {
        self.limits
    }
}

/// A physical plan the validator accepted, with its evidence. The fields are
/// private and no deserializer builds one: a plan reaches execution only
/// through [`accept_query`] or the replay acceptance.
#[derive(Debug, Clone)]
pub struct AcceptedPlan {
    plan: PhysicalPlan,
    evidence: Evidence,
}

impl AcceptedPlan {
    pub fn plan(&self) -> &PhysicalPlan {
        &self.plan
    }

    pub fn scope(&self) -> ValidationScope {
        self.evidence.scope
    }

    pub fn evidence(&self) -> &Evidence {
        &self.evidence
    }

    pub fn summary(&self) -> ValidationSummary {
        ValidationSummary {
            scope: self.scope(),
        }
    }

    /// The accepted plan with the values of one run. Binding changes no node
    /// of the plan, so it needs no second validation.
    pub fn bind(self, values: ValueTable) -> AcceptedBoundPlan {
        AcceptedBoundPlan {
            bound: BoundPlan {
                plan: self.plan,
                values,
            },
            evidence: self.evidence,
        }
    }
}

/// An [`AcceptedPlan`] bound to its values: what execution takes.
#[derive(Debug, Clone)]
pub struct AcceptedBoundPlan {
    bound: BoundPlan,
    evidence: Evidence,
}

impl AcceptedBoundPlan {
    pub fn bound(&self) -> &BoundPlan {
        &self.bound
    }

    pub fn evidence(&self) -> &Evidence {
        &self.evidence
    }

    pub fn into_parts(self) -> (BoundPlan, Evidence) {
        (self.bound, self.evidence)
    }
}

/// Validate `plan`, built for `input`, and wrap it. Every check reads the
/// requirements the checked declaration states and the plan alone.
pub(crate) fn accept(
    plan: PhysicalPlan,
    input: &AcceptInput<'_>,
) -> Result<AcceptedPlan, ValidationError> {
    let mut budget = Budget::new(input.limits);
    let requirements = Requirements::derive(input, &mut budget)?;
    requirements.check(&plan, input, &mut budget)?;
    Ok(AcceptedPlan {
        plan,
        evidence: Evidence {
            scope: ValidationScope::InvariantsOnly,
            limits: input.limits,
        },
    })
}
