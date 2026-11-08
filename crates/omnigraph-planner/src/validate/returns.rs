//! Validate the two-stage return of an aggregate comparison against the
//! checked query, including its group keys and every aggregate operand.

use std::collections::{BTreeMap, HashSet};

use omnigraph_compiler::ir::{IRExpr, IRProjection};

use super::{Budget, ValidationError};
use crate::optimizer::contains_aggregate;
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan};

pub(super) fn check_split_returns(
    required: &[IRProjection],
    plan: &PhysicalPlan,
    top: &[NodeId],
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    budget.visit(u64::try_from(top.len()).unwrap_or(u64::MAX))?;
    let pair = top.iter().find_map(|id| match plan.node(*id) {
        Some(PhysicalNode::Projection {
            input,
            return_exprs,
            ..
        }) => match plan.node(*input) {
            Some(
                PhysicalNode::Aggregate {
                    return_exprs: grouped,
                    ..
                }
                | PhysicalNode::MetadataCount {
                    return_exprs: grouped,
                    ..
                },
            ) => Some((return_exprs, grouped)),
            _ => None,
        },
        _ => None,
    });
    let Some((returned, grouped)) = pair else {
        return Err(mismatch(
            "the aggregate return needs an adjacent projection and aggregate",
        ));
    };
    budget
        .visit(u64::try_from(grouped.len().saturating_add(required.len())).unwrap_or(u64::MAX))?;
    let keys_match = grouped
        .iter()
        .filter(|p| !matches!(p.expr, IRExpr::Aggregate { .. }))
        .eq(required.iter().filter(|p| !contains_aggregate(&p.expr)));
    if !keys_match || returned.len() != required.len() {
        return Err(mismatch(
            "the aggregate return changes the query's group keys or items",
        ));
    }
    let mut columns = BTreeMap::new();
    for item in grouped {
        budget.visit(1)?;
        if columns.insert(item.column.as_str(), item).is_some() {
            return Err(mismatch("the aggregate return has ambiguous column names"));
        }
    }
    let mut used = HashSet::new();
    for (required, projected) in required.iter().zip(returned) {
        budget.visit(1)?;
        if required.alias != projected.alias
            || required.column != projected.column
            || required.ty != projected.ty
        {
            return Err(mismatch(
                "the aggregate return changes an item's name or type",
            ));
        }
        if matches!(required.expr, IRExpr::Aggregate { .. }) || !contains_aggregate(&required.expr)
        {
            if !matches!(&projected.expr, IRExpr::AliasRef(name, ty)
                if name == &required.column && ty == &required.ty
                    && columns.get(name.as_str()).is_some_and(|item| *item == required))
            {
                return Err(mismatch("the aggregate return changes a plain item"));
            }
            used.insert(required.column.as_str());
            continue;
        }
        let mut pending = vec![(&required.expr, &projected.expr)];
        while let Some((expected, actual)) = pending.pop() {
            budget.visit(1)?;
            let same = match (expected, actual) {
                (IRExpr::Aggregate { .. }, IRExpr::AliasRef(name, ty)) => {
                    used.insert(name.as_str());
                    columns.get(name.as_str()).is_some_and(|item| {
                        item.ty == *ty && expected.ty() == ty && item.expr == *expected
                    })
                }
                (IRExpr::Aggregate { .. }, _) => false,
                (
                    IRExpr::Binary {
                        left,
                        op,
                        right,
                        ty,
                    },
                    IRExpr::Binary {
                        left: actual_left,
                        op: actual_op,
                        right: actual_right,
                        ty: actual_ty,
                    },
                ) => {
                    pending.extend([
                        (left.as_ref(), actual_left.as_ref()),
                        (right.as_ref(), actual_right.as_ref()),
                    ]);
                    op == actual_op && ty == actual_ty
                }
                (IRExpr::Not(inner, ty), IRExpr::Not(actual_inner, actual_ty))
                | (
                    IRExpr::Cast { expr: inner, ty },
                    IRExpr::Cast {
                        expr: actual_inner,
                        ty: actual_ty,
                    },
                ) => {
                    pending.push((inner, actual_inner));
                    ty == actual_ty
                }
                (
                    IRExpr::IsNull { expr, negated, ty },
                    IRExpr::IsNull {
                        expr: actual_expr,
                        negated: actual_negated,
                        ty: actual_ty,
                    },
                ) => {
                    pending.push((expr, actual_expr));
                    negated == actual_negated && ty == actual_ty
                }
                _ => expected == actual && !contains_aggregate(expected),
            };
            if !same {
                return Err(mismatch(
                    "the aggregate return changes an expression or aggregate operand",
                ));
            }
        }
    }
    if used.len() != grouped.len() {
        return Err(mismatch("the aggregate return adds an unused item"));
    }
    Ok(())
}

fn mismatch(detail: &str) -> ValidationError {
    ValidationError::violated("projection", detail)
}
