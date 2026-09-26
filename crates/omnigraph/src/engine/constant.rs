//! Constants of the shared expression model: an assignment value and the
//! right operand of an inline binding match are evaluated after parameter
//! binding, under the three-valued rules every expression follows (RFC
//! 2026-09-24-shared-expression-model). The rules are the compiler's
//! `fold::evaluate`, the one home the compile-time fold shares; this module
//! adds only what the compiler cannot decide, the order of Date and DateTime
//! literals through the loader's parsers.

use std::cmp::Ordering;

use omnigraph_compiler::ir::{IRExpr, IROp, ParamMap, QueryIR, fold};
use omnigraph_compiler::query::ast::{BinaryOp, CompOp, Literal};

use crate::error::{OmniError, Result};

/// The value of a constant: a literal, a bound parameter (`now()` included),
/// or comparisons and Boolean operators over those. `Literal::Null` is the
/// one null result, typed later by the property it reaches.
pub(crate) fn evaluate_constant(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    match expr {
        IRExpr::Literal(lit) => Ok(lit.clone()),
        IRExpr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| OmniError::manifest(format!("parameter '{name}' not provided"))),
        IRExpr::Not(inner) => match evaluate_constant(inner, params)? {
            Literal::Bool(value) => Ok(Literal::Bool(!value)),
            Literal::Null => Ok(Literal::Null),
            other => Err(not_boolean("not", &other)),
        },
        IRExpr::IsNull { expr, negated } => {
            let is_null = matches!(evaluate_constant(expr, params)?, Literal::Null);
            Ok(Literal::Bool(is_null != *negated))
        }
        IRExpr::Binary { left, op, right } => {
            let left = evaluate_constant(left, params)?;
            let right = evaluate_constant(right, params)?;
            if let Some(value) = fold::evaluate(*op, &left, &right) {
                return Ok(value);
            }
            match op {
                BinaryOp::And => Err(not_boolean("and", non_boolean(&left, &right))),
                BinaryOp::Or => Err(not_boolean("or", non_boolean(&left, &right))),
                BinaryOp::Compare(op) => compare_dates(*op, &left, &right),
            }
        }
        other => Err(OmniError::manifest(format!(
            "`{other}` is not a constant; an assignment value or a binding match reads only \
             literals, parameters and now()"
        ))),
    }
}

/// What `fold::evaluate` leaves to the engine: two Date or two DateTime
/// literals ordered through the loader's parsers; any other pair it declined
/// has no comparison.
fn compare_dates(op: CompOp, left: &Literal, right: &Literal) -> Result<Literal> {
    use crate::loader::{parse_date32_literal, parse_date64_literal};
    let ordering = match (left, right) {
        (Literal::Date(l), Literal::Date(r)) => {
            parse_date32_literal(l)?.cmp(&parse_date32_literal(r)?)
        }
        (Literal::DateTime(l), Literal::DateTime(r)) => {
            parse_date64_literal(l)?.cmp(&parse_date64_literal(r)?)
        }
        _ => return Err(incomparable(op, left, right)),
    };
    let holds = match op {
        CompOp::Eq => ordering == Ordering::Equal,
        CompOp::Ne => ordering != Ordering::Equal,
        CompOp::Lt => ordering == Ordering::Less,
        CompOp::Le => ordering != Ordering::Greater,
        CompOp::Gt => ordering == Ordering::Greater,
        CompOp::Ge => ordering != Ordering::Less,
        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => {
            return Err(incomparable(op, left, right));
        }
    };
    Ok(Literal::Bool(holds))
}

/// The operand an `and`/`or` the fold declined was refused for: the first
/// that is neither Bool nor null.
fn non_boolean<'a>(left: &'a Literal, right: &'a Literal) -> &'a Literal {
    if matches!(left, Literal::Bool(_) | Literal::Null) {
        right
    } else {
        left
    }
}

fn not_boolean(op: &str, operand: &Literal) -> OmniError {
    OmniError::manifest(format!("`{op}` needs Bool operands, got {operand}"))
}

fn incomparable(op: CompOp, left: &Literal, right: &Literal) -> OmniError {
    OmniError::manifest(format!("cannot evaluate {left} {op} {right}"))
}

/// `ir` with every constant subtree of a filter position evaluated to its
/// literal, so both engine arms see the bound value where the query wrote
/// `{ adult: true or $flag }`; a null result compares with null and selects
/// no row.
pub(crate) fn fold_query_constants(ir: &QueryIR, params: &ParamMap) -> Result<QueryIR> {
    let mut folded = ir.clone();
    fold_pipeline(&mut folded.pipeline, params)?;
    Ok(folded)
}

fn fold_pipeline(pipeline: &mut [IROp], params: &ParamMap) -> Result<()> {
    for op in pipeline {
        match op {
            IROp::NodeScan { filters, .. }
            | IROp::Expand {
                dst_filters: filters,
                ..
            } => {
                for filter in filters {
                    *filter = fold_expr(filter, params)?;
                }
            }
            IROp::Filter(filter) => *filter = fold_expr(filter, params)?,
            IROp::AntiJoin { inner, .. } => fold_pipeline(inner, params)?,
        }
    }
    Ok(())
}

fn fold_expr(expr: &IRExpr, params: &ParamMap) -> Result<IRExpr> {
    if is_constant(expr) && !matches!(expr, IRExpr::Literal(_) | IRExpr::Param(_)) {
        return Ok(IRExpr::Literal(evaluate_constant(expr, params)?));
    }
    Ok(match expr {
        IRExpr::Binary { left, op, right } => IRExpr::Binary {
            left: Box::new(fold_expr(left, params)?),
            op: *op,
            right: Box::new(fold_expr(right, params)?),
        },
        IRExpr::Not(inner) => IRExpr::Not(Box::new(fold_expr(inner, params)?)),
        IRExpr::IsNull { expr, negated } => IRExpr::IsNull {
            expr: Box::new(fold_expr(expr, params)?),
            negated: *negated,
        },
        other => other.clone(),
    })
}

fn is_constant(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::Literal(_) | IRExpr::Param(_) => true,
        IRExpr::Binary { left, right, .. } => is_constant(left) && is_constant(right),
        IRExpr::Not(inner) => is_constant(inner),
        IRExpr::IsNull { expr, .. } => is_constant(expr),
        _ => false,
    }
}
