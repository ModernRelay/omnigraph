//! The compiler's sole comparison-domain selection. Decisions become explicit
//! Cast nodes; execution never chooses a domain from bound parameter values.

use crate::error::{CompilerError, Result};
use crate::query::ast::{BinaryOp, CompOp, Literal};
use crate::types::{ExprType, ScalarType};

use super::{BlockAggregateExpr, IRExpr, SubqueryPredicate, fold};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Domain {
    Scalar(ScalarType),
    Exact,
}

impl Domain {
    fn numeric(self) -> bool {
        match self {
            Self::Scalar(scalar) => scalar.is_numeric(),
            Self::Exact => true,
        }
    }

    fn ty(self, list: bool, nullable: bool) -> ExprType {
        match self {
            Self::Scalar(scalar) => ExprType::Value {
                scalar,
                list,
                nullable,
            },
            Self::Exact => ExprType::ExactInteger { list, nullable },
        }
    }
}

fn domain(ty: &ExprType) -> Option<Domain> {
    match ty {
        ExprType::Value { scalar, .. } => Some(Domain::Scalar(*scalar)),
        ExprType::ExactInteger { .. } => Some(Domain::Exact),
        ExprType::Node { .. } => None,
    }
}

fn error(detail: impl Into<String>) -> CompilerError {
    CompilerError::Plan(detail.into())
}

fn context_literal(literal: Option<&Literal>) -> bool {
    match literal {
        Some(Literal::Null) => true,
        Some(Literal::List(items)) => items.iter().all(|item| matches!(item, Literal::Null)),
        _ => false,
    }
}

fn literal(expr: &IRExpr) -> Option<&Literal> {
    match expr {
        IRExpr::Literal(value, _) => Some(value),
        _ => None,
    }
}

fn resolved_operator(
    op: CompOp,
    left: &ExprType,
    left_literal: Option<&Literal>,
    right: &ExprType,
) -> CompOp {
    if op == CompOp::Contains
        && matches!(
            left,
            ExprType::Value {
                scalar: ScalarType::String,
                list: false,
                ..
            }
        )
        && (!matches!(left_literal, Some(Literal::Null))
            || matches!(
                right,
                ExprType::Value {
                    scalar: ScalarType::String,
                    list: false,
                    ..
                }
            ))
    {
        CompOp::StringContains
    } else {
        op
    }
}

fn common_numeric(left: Domain, right: Domain) -> Domain {
    use Domain::{Exact, Scalar};
    use ScalarType::{F32, F64, I32, I64, U32, U64};
    if left == right {
        return left;
    }
    match (left, right) {
        (Scalar(F32 | F64), _) | (_, Scalar(F32 | F64)) => Scalar(F64),
        (Exact, _) | (_, Exact) => Exact,
        (Scalar(U64), Scalar(I32 | I64)) | (Scalar(I32 | I64), Scalar(U64)) => Exact,
        (Scalar(U32 | U64), Scalar(U32 | U64)) => Scalar(U64),
        _ => Scalar(I64),
    }
}

/// Select operand domains while preserving each operand's outer nullability.
/// Optional payloads are compile-time literals; bound parameter values are excluded.
pub fn comparison_types(
    op: CompOp,
    left: &ExprType,
    left_literal: Option<&Literal>,
    right: &ExprType,
    right_literal: Option<&Literal>,
) -> Result<(ExprType, ExprType)> {
    let op = resolved_operator(op, left, left_literal, right);
    let member = op == CompOp::Contains;
    let mut left_domain = domain(left).ok_or_else(|| error("comparison has a node operand"))?;
    let mut right_domain = domain(right).ok_or_else(|| error("comparison has a node operand"))?;
    if [left_domain, right_domain].iter().any(|domain| {
        matches!(
            domain,
            Domain::Scalar(ScalarType::Blob | ScalarType::Vector(_))
        )
    }) {
        return Err(error("Blob and Vector values are not comparison domains"));
    }
    let mut left_list = left.is_list();
    let mut right_list = right.is_list();
    if context_literal(left_literal) && !context_literal(right_literal) {
        left_domain = right_domain;
        if matches!(left_literal, Some(Literal::Null)) {
            left_list = member || right_list;
        }
    }
    if context_literal(right_literal) {
        right_domain = left_domain;
        if matches!(right_literal, Some(Literal::Null)) {
            right_list = !member && left_list;
        }
    }
    if member && (!left_list || right_list) || !member && left_list != right_list {
        return Err(error("comparison operands have incompatible list shapes"));
    }
    if matches!(op, CompOp::StringContains | CompOp::StartsWith)
        && (left_list
            || right_list
            || left_domain != Domain::Scalar(ScalarType::String)
            || right_domain != Domain::Scalar(ScalarType::String))
    {
        return Err(error("string predicate needs scalar String operands"));
    }
    let chosen = if left_domain == right_domain {
        left_domain
    } else if left_domain.numeric() && right_domain.numeric() {
        if right_literal.is_none()
            && left_literal
                .is_some_and(|value| literal_round_trips(value, left_domain, right_domain))
        {
            right_domain
        } else if left_literal.is_none()
            && right_literal
                .is_some_and(|value| literal_round_trips(value, right_domain, left_domain))
        {
            left_domain
        } else {
            common_numeric(left_domain, right_domain)
        }
    } else {
        return Err(error("comparison operands have incompatible value domains"));
    };
    Ok((
        chosen.ty(left_list, left.nullable()),
        chosen.ty(right_list, right.nullable()),
    ))
}

/// Nonliteral casts follow the compiler's common numeric domain. A narrowing
/// conversion needs the literal proof in `cast_expr_allowed` instead.
pub fn cast_allowed(source: &ExprType, target: &ExprType) -> bool {
    let (Some(source_domain), Some(target_domain)) = (domain(source), domain(target)) else {
        return false;
    };
    numeric_cast_shape(source, target)
        && common_numeric(source_domain, target_domain) == target_domain
}

fn numeric_cast_shape(source: &ExprType, target: &ExprType) -> bool {
    source != target
        && source.is_list() == target.is_list()
        && source.nullable() == target.nullable()
        && domain(source).is_some_and(Domain::numeric)
        && domain(target).is_some_and(Domain::numeric)
}

/// Literal narrowing is legal only when every value round-trips in its stored
/// source domain. Parameter payloads never participate in this proof.
pub fn cast_expr_allowed(expr: &IRExpr, target: &ExprType) -> bool {
    if cast_allowed(expr.ty(), target) {
        return true;
    }
    let (Some(source_domain), Some(target_domain), Some(value)) =
        (domain(expr.ty()), domain(target), literal(expr))
    else {
        return false;
    };
    numeric_cast_shape(expr.ty(), target)
        && literal_round_trips(value, source_domain, target_domain)
}

fn cast(expr: IRExpr, ty: ExprType) -> Result<IRExpr> {
    if expr.ty() == &ty {
        return Ok(expr);
    }
    if let IRExpr::Literal(value, _) = &expr
        && context_literal(Some(value))
    {
        if let ExprType::ExactInteger { list, nullable } = ty {
            let public_null = IRExpr::Literal(
                value.clone(),
                ExprType::Value {
                    scalar: ScalarType::I64,
                    list,
                    nullable,
                },
            );
            return Ok(IRExpr::Cast {
                expr: Box::new(public_null),
                ty,
            });
        }
        return Ok(IRExpr::Literal(value.clone(), ty));
    }
    if !cast_expr_allowed(&expr, &ty) {
        return Err(error(format!(
            "invalid cast from {} to {}",
            expr.ty().spelling(),
            ty.spelling()
        )));
    }
    Ok(IRExpr::Cast {
        expr: Box::new(expr),
        ty,
    })
}

/// Contextualize a direct null under logic without changing parameter types.
pub(super) fn boolean(expr: IRExpr) -> IRExpr {
    match expr {
        IRExpr::Literal(Literal::Null, _) => IRExpr::Literal(
            Literal::Null,
            ExprType::Value {
                scalar: ScalarType::Bool,
                list: false,
                nullable: true,
            },
        ),
        other => other,
    }
}

/// Coerce operands before folding, retaining Cast nodes for typed execution.
pub fn binary(left: IRExpr, op: BinaryOp, right: IRExpr) -> Result<IRExpr> {
    let (left, op, right) = match op {
        BinaryOp::And | BinaryOp::Or => (boolean(left), op, boolean(right)),
        BinaryOp::Compare(op) => {
            let op = resolved_operator(op, left.ty(), literal(&left), right.ty());
            let (lt, rt) =
                comparison_types(op, left.ty(), literal(&left), right.ty(), literal(&right))?;
            (cast(left, lt)?, BinaryOp::Compare(op), cast(right, rt)?)
        }
    };
    Ok(fold::binary(left, op, right))
}

/// Apply the same comparison decision to a block's restricted aggregate side.
pub fn block(left: BlockAggregateExpr, op: CompOp, right: IRExpr) -> Result<SubqueryPredicate> {
    let op = resolved_operator(op, left.ty(), None, right.ty());
    let (left_ty, right_ty) = comparison_types(op, left.ty(), None, right.ty(), literal(&right))?;
    let left = if left.ty() == &left_ty {
        left
    } else {
        if !cast_allowed(left.ty(), &left_ty) {
            return Err(error(
                "block aggregate needs an invalid narrowing conversion",
            ));
        }
        BlockAggregateExpr::Cast {
            expr: Box::new(left),
            ty: left_ty,
        }
    };
    let predicate = SubqueryPredicate {
        left,
        op,
        right: cast(right, right_ty)?,
    };
    predicate.check_types()?;
    Ok(predicate)
}

/// Round trips use the literal's already unified source domain. In particular,
/// an Integer element inside an F64 list is an F64 value before narrowing.
fn literal_round_trips(value: &Literal, source: Domain, target: Domain) -> bool {
    match value {
        Literal::Null => true,
        Literal::List(items) => items.iter().all(|item| {
            !matches!(item, Literal::List(_)) && literal_round_trips(item, source, target)
        }),
        Literal::Integer(value) => {
            if matches!(source, Domain::Scalar(ScalarType::F32 | ScalarType::F64)) {
                let value = if source == Domain::Scalar(ScalarType::F32) {
                    f64::from(*value as f32)
                } else {
                    *value as f64
                };
                float_round_trips(value, target)
            } else {
                integer_round_trips(i128::from(*value), target)
            }
        }
        Literal::Float(value) => {
            let value = if source == Domain::Scalar(ScalarType::F32) {
                f64::from(*value as f32)
            } else {
                *value
            };
            float_round_trips(value, target)
        }
        _ => false,
    }
}

fn integer_round_trips(value: i128, target: Domain) -> bool {
    use Domain::{Exact, Scalar};
    match target {
        Exact => true,
        Scalar(ScalarType::I32) => i32::try_from(value).is_ok(),
        Scalar(ScalarType::I64) => i64::try_from(value).is_ok(),
        Scalar(ScalarType::U32) => u32::try_from(value).is_ok(),
        Scalar(ScalarType::U64) => u64::try_from(value).is_ok(),
        Scalar(ScalarType::F32) => (value as f32) as i128 == value,
        Scalar(ScalarType::F64) => (value as f64) as i128 == value,
        _ => false,
    }
}

fn float_round_trips(value: f64, target: Domain) -> bool {
    if !value.is_finite() {
        return false;
    }
    use Domain::{Exact, Scalar};
    let range = match target {
        Scalar(ScalarType::F64) => return true,
        Scalar(ScalarType::F32) => return f64::from(value as f32).to_bits() == value.to_bits(),
        Scalar(ScalarType::I32) => (-2_f64.powi(31), 2_f64.powi(31)),
        Scalar(ScalarType::I64) => (-2_f64.powi(63), 2_f64.powi(63)),
        Scalar(ScalarType::U32) => (0.0, 2_f64.powi(32)),
        Scalar(ScalarType::U64) => (0.0, 2_f64.powi(64)),
        Exact => (-1e38, 1e38),
        _ => return false,
    };
    value.fract() == 0.0
        && value >= range.0
        && value < range.1
        && ((value as i128) as f64).to_bits() == value.to_bits()
}

/// `in` fixes the list role before overloaded `contains` dispatch sees a null.
pub(super) fn in_list(list: IRExpr, needle: IRExpr) -> Result<IRExpr> {
    let list = match list {
        IRExpr::Literal(Literal::Null, _) => {
            let domain =
                domain(needle.ty()).ok_or_else(|| error("membership has a node operand"))?;
            cast(
                IRExpr::Literal(
                    Literal::Null,
                    Domain::Scalar(ScalarType::String).ty(false, true),
                ),
                domain.ty(true, true),
            )?
        }
        other => other,
    };
    binary(list, BinaryOp::Compare(CompOp::Contains), needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PropType;

    fn ty(scalar: ScalarType, list: bool, nullable: bool) -> ExprType {
        ExprType::Value {
            scalar,
            list,
            nullable,
        }
    }

    fn param(name: &str, scalar: ScalarType, list: bool, nullable: bool) -> IRExpr {
        IRExpr::Param(name.into(), ty(scalar, list, nullable))
    }

    fn lit(value: Literal, scalar: ScalarType, list: bool) -> IRExpr {
        IRExpr::Literal(value, ty(scalar, list, false))
    }

    fn compared(left: IRExpr, op: CompOp, right: IRExpr) -> IRExpr {
        let result = binary(left, BinaryOp::Compare(op), right).unwrap();
        result.check_types().unwrap();
        result
    }

    #[test]
    fn declarations_choose_common_domains_without_parameter_payloads() {
        use ScalarType::*;
        for (left, right, expected) in [
            (I32, I32, ty(I32, false, false)),
            (I32, I64, ty(I64, false, false)),
            (U32, U64, ty(U64, false, false)),
            (I32, U32, ty(I64, false, false)),
            (I64, U32, ty(I64, false, false)),
            (
                I32,
                U64,
                ExprType::ExactInteger {
                    list: false,
                    nullable: false,
                },
            ),
            (
                I64,
                U64,
                ExprType::ExactInteger {
                    list: false,
                    nullable: false,
                },
            ),
            (F32, F32, ty(F32, false, false)),
            (I64, F32, ty(F64, false, false)),
            (U64, F64, ty(F64, false, false)),
        ] {
            for (left, right) in [(left, right), (right, left)] {
                let expression = compared(
                    param("a", left, false, false),
                    CompOp::Eq,
                    param("b", right, false, false),
                );
                let (left, _, right) = expression.comparison_parts().unwrap();
                assert_eq!(left.ty(), &expected);
                assert_eq!(right.ty(), &expected);
            }
        }
    }

    #[test]
    fn literal_narrowing_is_exact_and_keeps_a_cast() {
        use ScalarType::*;
        for (owner, literal, source, expected) in [
            (I32, Literal::Integer(i64::from(i32::MAX)), I64, I32),
            (I32, Literal::Integer(i64::from(i32::MAX) + 1), I64, I64),
            (U32, Literal::Integer(-1), I64, I64),
            (I64, Literal::Float(2_f64.powi(63)), F64, F64),
            (I64, Literal::Float(-2_f64.powi(63)), F64, I64),
            (F32, Literal::Float(0.5), F64, F32),
            (F32, Literal::Float(0.1), F64, F64),
            (F32, Literal::Float(-0.0), F64, F32),
            (I64, Literal::Float(-0.0), F64, F64),
            (F32, Literal::Integer(16_777_216), I64, F32),
            (F32, Literal::Integer(16_777_217), I64, F64),
        ] {
            let expression = compared(
                param("x", owner, false, true),
                CompOp::Eq,
                lit(literal, source, false),
            );
            let (left, _, right) = expression.comparison_parts().unwrap();
            assert_eq!(left.ty(), &ty(expected, false, true));
            assert_eq!(right.ty(), &ty(expected, false, false));
            assert_eq!(expression.ty(), &ty(Bool, false, true));
            if source != expected {
                assert!(matches!(right, IRExpr::Cast { .. }));
            }
        }
    }

    #[test]
    fn literal_only_numeric_expressions_keep_the_common_domain() {
        let expression = compared(
            lit(Literal::Integer(2), ScalarType::I64, false),
            CompOp::Gt,
            lit(Literal::Float(1.5), ScalarType::F64, false),
        );
        let (left, _, right) = expression.comparison_parts().unwrap();
        assert!(matches!(left, IRExpr::Cast { .. }));
        assert_eq!(left.ty(), &ty(ScalarType::F64, false, false));
        assert_eq!(right.ty(), left.ty());
        let expression = compared(
            lit(
                Literal::List(vec![Literal::Integer(2)]),
                ScalarType::I64,
                true,
            ),
            CompOp::Contains,
            lit(Literal::Float(2.0), ScalarType::F64, false),
        );
        let (left, _, right) = expression.comparison_parts().unwrap();
        assert_eq!(left.ty(), &ty(ScalarType::F64, true, false));
        assert_eq!(right.ty(), &ty(ScalarType::F64, false, false));
    }

    #[test]
    fn numeric_lists_narrow_as_a_whole_in_the_unified_source_domain() {
        for (items, expected) in [
            (
                vec![Literal::Integer(2), Literal::Null, Literal::Float(3.0)],
                ScalarType::I32,
            ),
            (
                vec![Literal::Float(3.5), Literal::Integer(2)],
                ScalarType::F64,
            ),
            (
                vec![Literal::Integer(2), Literal::Float(3.5)],
                ScalarType::F64,
            ),
        ] {
            let expression = compared(
                lit(Literal::List(items), ScalarType::F64, true),
                CompOp::Contains,
                param("x", ScalarType::I32, false, true),
            );
            let (list, _, needle) = expression.comparison_parts().unwrap();
            assert_eq!(list.ty(), &ty(expected, true, false));
            assert_eq!(needle.ty(), &ty(expected, false, true));
        }
        assert!(literal_round_trips(
            &Literal::List(vec![
                Literal::Integer(9_007_199_254_740_993),
                Literal::Float(1.0)
            ]),
            Domain::Scalar(ScalarType::F64),
            Domain::Scalar(ScalarType::I64)
        ));
    }

    #[test]
    fn exact_membership_retains_shape_and_nullable_operands() {
        let expression = compared(
            param("values", ScalarType::U64, true, true),
            CompOp::Contains,
            param("needle", ScalarType::I64, false, false),
        );
        let (list, _, needle) = expression.comparison_parts().unwrap();
        assert_eq!(
            list.ty(),
            &ExprType::ExactInteger {
                list: true,
                nullable: true
            }
        );
        assert_eq!(
            needle.ty(),
            &ExprType::ExactInteger {
                list: false,
                nullable: false
            }
        );
        assert_eq!(expression.ty(), &ty(ScalarType::Bool, false, true));
        assert_eq!(
            list.ty().to_arrow(),
            Some(arrow_schema::DataType::List(std::sync::Arc::new(
                arrow_schema::Field::new("item", arrow_schema::DataType::Decimal128(38, 0), true)
            )))
        );
        assert_eq!(list.ty().spelling(), "[exact_integer]?");
    }

    #[test]
    fn forged_narrowing_casts_cannot_use_properties_or_parameter_values() {
        let target = ty(ScalarType::F32, false, false);
        for child in [
            param("x", ScalarType::F64, false, false),
            IRExpr::PropAccess {
                variable: "p".into(),
                property: "value".into(),
                ty: ty(ScalarType::F64, false, false),
            },
        ] {
            let forged = IRExpr::Cast {
                expr: Box::new(child),
                ty: target.clone(),
            };
            assert!(forged.check_types().is_err());
        }
        for (value, accepted) in [
            (0.5, true),
            (0.1, false),
            (f64::MAX, false),
            (f64::INFINITY, false),
        ] {
            let cast = IRExpr::Cast {
                expr: Box::new(lit(Literal::Float(value), ScalarType::F64, false)),
                ty: target.clone(),
            };
            assert_eq!(cast.check_types().is_ok(), accepted);
        }
        let list = lit(
            Literal::List(vec![Literal::Float(0.5), Literal::Null]),
            ScalarType::F64,
            true,
        );
        assert!(cast_expr_allowed(&list, &ty(ScalarType::F32, true, false)));
        assert!(!cast_expr_allowed(&list, &ty(ScalarType::F32, true, true)));
        assert!(!cast_expr_allowed(
            &list,
            &ty(ScalarType::F32, false, false)
        ));
    }

    #[test]
    fn integer_float_round_trips_never_accept_saturating_upper_bounds() {
        for target in [ScalarType::F32, ScalarType::F64] {
            assert!(!integer_round_trips(
                i128::from(i64::MAX),
                Domain::Scalar(target)
            ));
            assert!(!integer_round_trips(
                i128::from(u64::MAX),
                Domain::Scalar(target)
            ));
            assert!(integer_round_trips(
                i128::from(i64::MIN),
                Domain::Scalar(target)
            ));
        }
        assert!(!float_round_trips(
            2_f64.powi(64),
            Domain::Scalar(ScalarType::U64)
        ));
        let below = f64::from_bits(2_f64.powi(64).to_bits() - 1);
        assert!(float_round_trips(below, Domain::Scalar(ScalarType::U64)));
    }

    #[test]
    fn context_never_changes_a_literal_lists_shape() {
        let empty = lit(Literal::List(vec![]), ScalarType::String, true);
        assert!(
            binary(
                empty.clone(),
                BinaryOp::Compare(CompOp::Eq),
                param("x", ScalarType::I32, false, false)
            )
            .is_err()
        );
        assert!(
            binary(
                param("xs", ScalarType::I32, true, false),
                BinaryOp::Compare(CompOp::Contains),
                empty
            )
            .is_err()
        );
        let value = param("x", ScalarType::I64, false, false);
        assert!(!cast_expr_allowed(&value, value.ty()));
    }

    #[test]
    fn contextual_nulls_stay_public_below_exact_casts() {
        let null = IRExpr::Literal(
            Literal::Null,
            ExprType::from_prop(&PropType::scalar(ScalarType::String, true)),
        );
        let expression = cast(
            null,
            ExprType::ExactInteger {
                list: false,
                nullable: true,
            },
        )
        .unwrap();
        expression.check_types().unwrap();
        let IRExpr::Cast { expr, .. } = expression else {
            panic!("expected Cast")
        };
        assert_eq!(expr.ty(), &ty(ScalarType::I64, false, true));
    }
}
