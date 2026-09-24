//! Compile-time evaluation of literal-only expressions: a comparison, `and`,
//! `or`, `not` or null test whose operands are all `Literal` folds to its
//! value as the tree is built, so `{ adult: true or false }` reaches the IR as
//! `true` (RFC 2026-09-24-shared-expression-model). The rules are the
//! three-valued ones, and the engine's `evaluate_constant` applies the same
//! `evaluate` at run time to the parameter-bearing rest; what these rules do
//! not decide (a Date or DateTime comparison, an operand the type checker
//! refuses) stays as written here and is the engine's to order or refuse.

use std::cmp::Ordering;

use crate::query::ast::{BinaryOp, CompOp, Literal};

use super::IRExpr;

/// `left <op> right`, folded when both operands are literals.
pub(super) fn binary(left: IRExpr, op: BinaryOp, right: IRExpr) -> IRExpr {
    if let (IRExpr::Literal(l), IRExpr::Literal(r)) = (&left, &right)
        && let Some(value) = evaluate(op, l, r)
    {
        return IRExpr::Literal(value);
    }
    IRExpr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// `not <inner>`, folded over a Boolean or null literal.
pub(super) fn not(inner: IRExpr) -> IRExpr {
    match inner {
        IRExpr::Literal(Literal::Bool(value)) => IRExpr::Literal(Literal::Bool(!value)),
        IRExpr::Literal(Literal::Null) => IRExpr::Literal(Literal::Null),
        other => IRExpr::Not(Box::new(other)),
    }
}

/// `<expr> is null` / `is not null`, folded over any literal.
pub(super) fn is_null(expr: IRExpr, negated: bool) -> IRExpr {
    match expr {
        IRExpr::Literal(literal) => {
            IRExpr::Literal(Literal::Bool(matches!(literal, Literal::Null) != negated))
        }
        other => IRExpr::IsNull {
            expr: Box::new(other),
            negated,
        },
    }
}

/// `left <op> right` over two literals, or `None` where these rules do not
/// decide: a non-Boolean operand of `and`/`or`, a Date or DateTime order, an
/// operand pair with no comparison.
pub fn evaluate(op: BinaryOp, left: &Literal, right: &Literal) -> Option<Literal> {
    match op {
        BinaryOp::And => kleene(left, right, false),
        BinaryOp::Or => kleene(left, right, true),
        BinaryOp::Compare(op) => compare(op, left, right),
    }
}

/// `and` (`absorbing` false) and `or` (`absorbing` true) under Kleene's
/// rules: the absorbing value on either side decides, two known values give
/// the other one, and a null otherwise.
fn kleene(left: &Literal, right: &Literal, absorbing: bool) -> Option<Literal> {
    let side = |literal: &Literal| match literal {
        Literal::Bool(value) => Some(Some(*value)),
        Literal::Null => Some(None),
        _ => None,
    };
    Some(match (side(left)?, side(right)?) {
        (Some(value), _) | (_, Some(value)) if value == absorbing => Literal::Bool(absorbing),
        (Some(_), Some(_)) => Literal::Bool(!absorbing),
        _ => Literal::Null,
    })
}

/// A comparison with a null operand is null; list membership compares each
/// element by `=`, lists compare by equality only, numbers across `Integer`
/// and `Float`, floats in Arrow's total order.
fn compare(op: CompOp, left: &Literal, right: &Literal) -> Option<Literal> {
    if matches!(left, Literal::Null) || matches!(right, Literal::Null) {
        return Some(Literal::Null);
    }
    let holds = match (op, left, right) {
        (CompOp::Contains, Literal::List(items), needle) => items
            .iter()
            .any(|item| compare(CompOp::Eq, item, needle) == Some(Literal::Bool(true))),
        (
            CompOp::Contains | CompOp::StringContains,
            Literal::String(text),
            Literal::String(part),
        ) => text.contains(part.as_str()),
        (CompOp::StartsWith, Literal::String(text), Literal::String(prefix)) => {
            text.starts_with(prefix.as_str())
        }
        (CompOp::Eq, Literal::List(_), Literal::List(_)) => left == right,
        (CompOp::Ne, Literal::List(_), Literal::List(_)) => left != right,
        (CompOp::Eq, _, _) => ordering(left, right)? == Ordering::Equal,
        (CompOp::Ne, _, _) => ordering(left, right)? != Ordering::Equal,
        (CompOp::Lt, _, _) => ordering(left, right)? == Ordering::Less,
        (CompOp::Le, _, _) => ordering(left, right)? != Ordering::Greater,
        (CompOp::Gt, _, _) => ordering(left, right)? == Ordering::Greater,
        (CompOp::Ge, _, _) => ordering(left, right)? != Ordering::Less,
        _ => return None,
    };
    Some(Literal::Bool(holds))
}

/// The order of two scalar literals; `None` where the engine's parsers decide
/// (Date, DateTime) or no order exists.
fn ordering(left: &Literal, right: &Literal) -> Option<Ordering> {
    Some(match (left, right) {
        (Literal::Integer(l), Literal::Integer(r)) => l.cmp(r),
        (Literal::Float(l), Literal::Float(r)) => l.total_cmp(r),
        (Literal::Integer(l), Literal::Float(r)) => (*l as f64).total_cmp(r),
        (Literal::Float(l), Literal::Integer(r)) => l.total_cmp(&(*r as f64)),
        (Literal::String(l), Literal::String(r)) => l.cmp(r),
        (Literal::Bool(l), Literal::Bool(r)) => l.cmp(r),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(literal: Literal) -> IRExpr {
        IRExpr::Literal(literal)
    }

    fn compared(left: Literal, op: CompOp, right: Literal) -> IRExpr {
        binary(lit(left), BinaryOp::Compare(op), lit(right))
    }

    fn text(value: &str) -> Literal {
        Literal::String(value.to_string())
    }

    #[test]
    fn literal_only_trees_fold_under_three_valued_rules() {
        let t = Literal::Bool(true);
        let f = Literal::Bool(false);
        let null = Literal::Null;
        for (expression, expected) in [
            (
                binary(lit(t.clone()), BinaryOp::Or, lit(f.clone())),
                t.clone(),
            ),
            (
                binary(lit(t.clone()), BinaryOp::Or, lit(null.clone())),
                t.clone(),
            ),
            (
                binary(lit(null.clone()), BinaryOp::Or, lit(f.clone())),
                null.clone(),
            ),
            (
                binary(lit(f.clone()), BinaryOp::And, lit(null.clone())),
                f.clone(),
            ),
            (
                binary(lit(t.clone()), BinaryOp::And, lit(null.clone())),
                null.clone(),
            ),
            (
                binary(lit(t.clone()), BinaryOp::And, lit(t.clone())),
                t.clone(),
            ),
            (not(lit(t.clone())), f.clone()),
            (not(lit(null.clone())), null.clone()),
            (is_null(lit(null.clone()), false), t.clone()),
            (is_null(lit(Literal::Integer(1)), true), t.clone()),
            (
                compared(Literal::Integer(2), CompOp::Lt, Literal::Float(2.5)),
                t.clone(),
            ),
            (
                compared(Literal::Integer(2), CompOp::Eq, Literal::Float(2.7)),
                f.clone(),
            ),
            (
                compared(Literal::Float(2.0), CompOp::Ge, Literal::Integer(2)),
                t.clone(),
            ),
            (
                compared(Literal::Integer(1), CompOp::Gt, Literal::Null),
                null.clone(),
            ),
            (compared(text("b"), CompOp::Ne, text("a")), t.clone()),
            (
                compared(text("alice"), CompOp::Contains, text("li")),
                t.clone(),
            ),
            (
                compared(text("alice"), CompOp::StringContains, text("x")),
                f.clone(),
            ),
            (
                compared(text("alice"), CompOp::StartsWith, text("al")),
                t.clone(),
            ),
            (
                compared(
                    Literal::List(vec![Literal::Integer(1)]),
                    CompOp::Contains,
                    Literal::Float(1.0),
                ),
                t.clone(),
            ),
            (
                compared(
                    Literal::List(vec![Literal::Integer(1)]),
                    CompOp::Contains,
                    Literal::Float(2.0),
                ),
                f.clone(),
            ),
            (
                compared(
                    Literal::List(vec![Literal::Integer(1)]),
                    CompOp::Eq,
                    Literal::List(vec![Literal::Integer(1)]),
                ),
                t.clone(),
            ),
            (
                binary(
                    not(compared(
                        Literal::Integer(1),
                        CompOp::Eq,
                        Literal::Integer(1),
                    )),
                    BinaryOp::Or,
                    is_null(lit(Literal::Null), false),
                ),
                t,
            ),
        ] {
            assert_eq!(expression, lit(expected));
        }
    }

    #[test]
    fn what_the_rules_do_not_decide_stays_as_written() {
        let date = |value: &str| Literal::Date(value.to_string());
        let kept = compared(date("2026-01-01"), CompOp::Lt, date("2026-02-01"));
        assert!(matches!(kept, IRExpr::Binary { .. }));
        let kept = compared(Literal::Integer(1), CompOp::Eq, text("1"));
        assert!(matches!(kept, IRExpr::Binary { .. }));
        let kept = not(lit(Literal::Integer(1)));
        assert!(matches!(kept, IRExpr::Not(_)));
        let kept = binary(
            IRExpr::Param("flag".to_string()),
            BinaryOp::Or,
            lit(Literal::Bool(true)),
        );
        assert!(matches!(kept, IRExpr::Binary { .. }));
    }
}
