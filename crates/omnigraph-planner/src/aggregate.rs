use omnigraph_compiler::ir::{BlockAggregateExpr, IRExpr};
use omnigraph_compiler::query::value::AggFunc;
use omnigraph_compiler::types::{AggSignature, ExprType, ScalarType};
use serde::{Deserialize, Serialize};

use crate::{PhysicalNode, PhysicalPlan, PlanError};

/// The accumulator implementation selected by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Accumulator {
    Count,
    ExactInteger,
    Float64,
    Extremum,
    /// A group's non-null values in the fixed value order, held in the
    /// query's memory pool as they arrive: `collect`'s list, and the values
    /// `stddev` and `percentile` finish.
    List,
}

/// Conversion of the accumulated total to the declared result type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overflow {
    RoundToNearest,
    Error,
}

/// Physical arithmetic for an aggregate with a compiler-owned signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateSpec {
    pub accumulator: Accumulator,
    pub overflow: Overflow,
}

/// Select an implementation for the declared aggregate signature.
pub fn plan_aggregate(func: AggFunc, signature: &AggSignature) -> Result<AggregateSpec, PlanError> {
    let expected = func.result_type(&signature.arg).ok_or_else(|| {
        PlanError::Internal(format!(
            "invalid {func} argument {}",
            signature.arg.spelling()
        ))
    })?;
    if signature.result != expected {
        return Err(PlanError::Internal(format!(
            "{func} result must be {}, got {}",
            expected.spelling(),
            signature.result.spelling()
        )));
    }
    let (accumulator, overflow) = match func {
        AggFunc::Count => (Accumulator::Count, Overflow::Error),
        AggFunc::Sum => match &signature.arg {
            ExprType::Value {
                scalar: ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64,
                ..
            } => (Accumulator::ExactInteger, Overflow::RoundToNearest),
            ExprType::Value {
                scalar: ScalarType::F32 | ScalarType::F64,
                ..
            } => (Accumulator::Float64, Overflow::RoundToNearest),
            ExprType::Value {
                scalar:
                    ScalarType::String
                    | ScalarType::Bool
                    | ScalarType::Date
                    | ScalarType::DateTime
                    | ScalarType::Vector(_)
                    | ScalarType::Blob,
                ..
            }
            | ExprType::Node { .. }
            | ExprType::Edge { .. }
            | ExprType::ExactInteger { .. } => {
                return Err(PlanError::Internal("sum requires numeric input".into()));
            }
        },
        // An integer mean divides an exact total once; a float mean
        // accumulates in `F64` as `sum` does.
        AggFunc::Avg => match &signature.arg {
            ExprType::Value {
                scalar: ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64,
                ..
            } => (Accumulator::ExactInteger, Overflow::RoundToNearest),
            _ => (Accumulator::Float64, Overflow::RoundToNearest),
        },
        AggFunc::Min | AggFunc::Max => (Accumulator::Extremum, Overflow::Error),
        AggFunc::Collect => (Accumulator::List, Overflow::Error),
        // The group's values, held as `collect` holds them and finished in
        // `F64`: an integer value rounds to the nearest `F64`.
        AggFunc::Stddev | AggFunc::Percentile => (Accumulator::List, Overflow::RoundToNearest),
    };
    Ok(AggregateSpec {
        accumulator,
        overflow,
    })
}

/// Select arithmetic from a block's aggregate leaf, beneath any comparison casts.
pub fn plan_block_aggregate(left: &BlockAggregateExpr) -> Result<Option<AggregateSpec>, PlanError> {
    match left.leaf() {
        BlockAggregateExpr::CountRows { .. } => Ok(None),
        BlockAggregateExpr::Aggregate {
            func, signature, ..
        } => plan_aggregate(*func, signature).map(Some),
        BlockAggregateExpr::Cast { .. } => Err(PlanError::Internal(
            "block aggregate leaf cannot be a cast".into(),
        )),
    }
}

/// Refuse missing, surplus or inconsistent arithmetic in fresh and saved plans.
pub fn validate_aggregate_specs(plan: &PhysicalPlan) -> Result<(), PlanError> {
    for (_, node) in plan.live() {
        match node {
            PhysicalNode::Aggregate {
                return_exprs,
                aggregates,
                ..
            } => {
                if return_exprs.len() != aggregates.len() {
                    return Err(PlanError::Internal(
                        "aggregate specifications must align with return expressions".into(),
                    ));
                }
                for (projection, actual) in return_exprs.iter().zip(aggregates) {
                    projection
                        .expr
                        .check_types()
                        .map_err(|error| PlanError::Internal(error.to_string()))?;
                    let expected = match &projection.expr {
                        IRExpr::Aggregate {
                            func, signature, ..
                        } => Some(plan_aggregate(*func, signature)?),
                        _ => None,
                    };
                    if *actual != expected {
                        return Err(PlanError::Internal(format!(
                            "aggregate specification differs from its signature: expected {expected:?}, got {actual:?}"
                        )));
                    }
                }
            }
            PhysicalNode::AntiJoin {
                predicate,
                aggregate,
                ..
            } => {
                predicate
                    .check_types()
                    .map_err(|error| PlanError::Internal(error.to_string()))?;
                let expected = plan_block_aggregate(&predicate.left)?;
                if *aggregate != expected {
                    return Err(PlanError::Internal(format!(
                        "block aggregate specification differs from its signature: expected {expected:?}, got {aggregate:?}"
                    )));
                }
            }
            PhysicalNode::MetadataCount { spec, return_exprs } => {
                for projection in return_exprs {
                    projection
                        .expr
                        .check_types()
                        .map_err(|error| PlanError::Internal(error.to_string()))?;
                    let valid = match &projection.expr {
                        IRExpr::Aggregate {
                            func: AggFunc::Count,
                            arg,
                            signature,
                            ..
                        } => {
                            matches!((arg.as_ref(), &signature.arg),
                                (IRExpr::Variable(binding, _), ExprType::Node { type_name })
                                if spec.binding.as_ref() == Some(binding) && spec.table.type_key.strip_prefix("node:") == Some(type_name.as_str()))
                        }
                        _ => false,
                    };
                    if !valid {
                        return Err(PlanError::Internal(
                            "metadata count requires count of its scanned node binding".into(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
