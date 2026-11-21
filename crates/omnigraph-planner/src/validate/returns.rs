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
                // Any other node keeps its head (operator, function, types)
                // and its children pair up, under any operator: unary `-`
                // and a call (`coalesce(sum(…), 0)`) as arithmetic.
                _ if contains_aggregate(expected) => {
                    let (want, got) = (expected.children(), actual.children());
                    if expected.head() == actual.head() && want.len() == got.len() {
                        pending.extend(want.into_iter().zip(got));
                        true
                    } else {
                        false
                    }
                }
                _ => expected == actual,
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
