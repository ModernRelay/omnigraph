//! Arithmetic and the scalar registry rows over Arrow arrays
//! (docs/rfcs/2026-11-07-gq-3.md, section 2).
//!
//! Each arithmetic term `can_fail`: an integer overflow (`E1`), a zero divisor
//! (`E2`) and a non-finite float result (`E3`) refuse the row with a typed
//! execution diagnostic, never a wrapped value, a null or an infinity. Only
//! valid slots are computed, so a null operand gives a null result and is
//! never refused. Integer division truncates toward zero.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, StringArray, builder::StringBuilder,
};
use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::compute::kernels::{boolean, numeric};
use omnigraph_compiler::ir::expr::ScalarFunc;
use omnigraph_compiler::query::codes::{E1, E2, E3};
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::query::syntax::ArithOp;
use omnigraph_compiler::types::ExprType;

use crate::error::{OmniError, Result};

fn refused(code: omnigraph_compiler::query::diagnostic::QueryCode, message: String) -> OmniError {
    OmniError::Compiler(omnigraph_compiler::error::CompilerError::query(
        QueryDiagnostic::execution(code, message),
    ))
}

fn arrow_refusal(op: &str, error: ArrowError) -> OmniError {
    match error {
        ArrowError::ArithmeticOverflow(detail) => {
            refused(E1, format!("`{op}` overflowed its integer type: {detail}"))
        }
        ArrowError::DivideByZero => refused(E2, format!("`{op}` has a zero divisor")),
        other => OmniError::arrow_internal(other),
    }
}

/// Whether any valid slot of a numeric array is zero (`-0.0` included).
fn has_zero(values: &ArrayRef) -> Result<bool> {
    let literal: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![0]));
    let zero = arrow_cast::cast(&literal, values.data_type()).map_err(OmniError::arrow_internal)?;
    let matches = arrow_ord::cmp::eq(values, &arrow_array::Scalar::new(zero))
        .map_err(OmniError::arrow_internal)?;
    Ok(matches.true_count() > 0)
}

/// Refuse a float array holding a non-finite valid slot.
fn require_finite(op: &str, values: &ArrayRef) -> Result<()> {
    let finite = match values.data_type() {
        DataType::Float64 => values
            .as_any()
            .downcast_ref::<Float64Array>()
            .is_some_and(|a| a.iter().flatten().all(f64::is_finite)),
        DataType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .is_some_and(|a| a.iter().flatten().all(f32::is_finite)),
        _ => true,
    };
    if finite {
        Ok(())
    } else {
        Err(refused(
            E3,
            format!("`{op}` produced a float that is not finite"),
        ))
    }
}

/// `left <op> right` over two arrays of one numeric type.
pub(super) fn binary(op: ArithOp, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    if left.data_type() != right.data_type() {
        return Err(OmniError::manifest_internal(format!(
            "arithmetic operands have different types: {} and {}",
            left.data_type(),
            right.data_type()
        )));
    }
    let symbol = op.symbol();
    if matches!(op, ArithOp::Div | ArithOp::Rem) && has_zero(right)? {
        return Err(refused(E2, format!("`{symbol}` has a zero divisor")));
    }
    let values = match op {
        ArithOp::Add => numeric::add(left, right),
        ArithOp::Sub => numeric::sub(left, right),
        ArithOp::Mul => numeric::mul(left, right),
        ArithOp::Div => numeric::div(left, right),
        ArithOp::Rem => numeric::rem(left, right),
    }
    .map_err(|error| arrow_refusal(symbol, error))?;
    require_finite(symbol, &values)?;
    Ok(values)
}

/// Unary `-` over a signed integer or float array.
pub(super) fn negate(values: &ArrayRef) -> Result<ArrayRef> {
    numeric::neg(values).map_err(|error| arrow_refusal("-", error))
}

/// A scalar registry row over its evaluated arguments.
pub(super) fn call(func: ScalarFunc, args: &[ArrayRef], ty: &ExprType) -> Result<ArrayRef> {
    match (func, args) {
        (ScalarFunc::If, [cond, then, otherwise]) => {
            let cond = cond
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| OmniError::manifest_internal("`if` condition is not Boolean"))?;
            // A null condition takes the else branch.
            let mask: BooleanArray = cond.iter().map(|value| Some(value == Some(true))).collect();
            arrow_select::zip::zip(&mask, then, otherwise).map_err(OmniError::arrow_internal)
        }
        (ScalarFunc::Coalesce, [first, rest @ ..]) => {
            let mut values = Arc::clone(first);
            for next in rest {
                let present = boolean::is_not_null(&values).map_err(OmniError::arrow_internal)?;
                values = arrow_select::zip::zip(&present, &values, next)
                    .map_err(OmniError::arrow_internal)?;
            }
            Ok(values)
        }
        (ScalarFunc::Concat, [_, ..]) => {
            let strings = args
                .iter()
                .map(|arg| {
                    arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        OmniError::manifest_internal("`concat` argument is not a String array")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let rows = strings[0].len();
            let mut out = StringBuilder::with_capacity(rows, 0);
            for row in 0..rows {
                if strings.iter().any(|s| s.is_null(row)) {
                    out.append_null();
                } else {
                    out.append_value(strings.iter().map(|s| s.value(row)).collect::<String>());
                }
            }
            Ok(Arc::new(out.finish()))
        }
        (ScalarFunc::MatchTerms { .. }, _) => Err(OmniError::manifest_internal(
            "`match_terms` reaches no evaluator; the planner does not lower it",
        )),
        (func, _) => Err(OmniError::manifest_internal(format!(
            "`{}` has the wrong argument count for {}",
            func.name(),
            ty.spelling()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, UInt32Array};
    use omnigraph_compiler::types::{PropType, ScalarType};

    fn code(error: OmniError) -> String {
        error
            .diagnostic()
            .map(|d| d.code.as_str().to_string())
            .unwrap_or_else(|| format!("untyped: {error}"))
    }

    fn i64s(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn integer_arithmetic_keeps_its_type_and_propagates_nulls() {
        let left = i64s(&[Some(7), Some(-7), None]);
        let right = i64s(&[Some(2), Some(2), Some(2)]);
        let sum = binary(ArithOp::Add, &left, &right).unwrap();
        assert_eq!(
            sum.as_ref(),
            &Int64Array::from(vec![Some(9), Some(-5), None])
        );
        let quotient = binary(ArithOp::Div, &left, &right).unwrap();
        // Integer division truncates toward zero.
        assert_eq!(
            quotient.as_ref(),
            &Int64Array::from(vec![Some(3), Some(-3), None])
        );
        let remainder = binary(ArithOp::Rem, &left, &right).unwrap();
        assert_eq!(
            remainder.as_ref(),
            &Int64Array::from(vec![Some(1), Some(-1), None])
        );
        let narrow: ArrayRef = Arc::new(Int32Array::from(vec![Some(3)]));
        let product = binary(ArithOp::Mul, &narrow, &narrow).unwrap();
        assert_eq!(product.data_type(), &DataType::Int32);
    }

    #[test]
    fn overflow_a_zero_divisor_and_a_non_finite_float_are_typed_refusals() {
        let max = i64s(&[Some(i64::MAX)]);
        let one = i64s(&[Some(1)]);
        assert_eq!(code(binary(ArithOp::Add, &max, &one).unwrap_err()), "E1");
        let min = i64s(&[Some(i64::MIN)]);
        assert_eq!(code(negate(&min).unwrap_err()), "E1");
        let zero = i64s(&[Some(0)]);
        assert_eq!(code(binary(ArithOp::Div, &one, &zero).unwrap_err()), "E2");
        assert_eq!(code(binary(ArithOp::Rem, &one, &zero).unwrap_err()), "E2");
        let small: ArrayRef = Arc::new(UInt32Array::from(vec![Some(1)]));
        let large: ArrayRef = Arc::new(UInt32Array::from(vec![Some(2)]));
        assert_eq!(
            code(binary(ArithOp::Sub, &small, &large).unwrap_err()),
            "E1"
        );
        let huge: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::MAX)]));
        assert_eq!(code(binary(ArithOp::Mul, &huge, &huge).unwrap_err()), "E3");
        let fzero: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.0)]));
        assert_eq!(code(binary(ArithOp::Div, &huge, &fzero).unwrap_err()), "E2");
        // A null divisor gives a null row; it is never refused.
        let null = i64s(&[None]);
        assert_eq!(binary(ArithOp::Div, &one, &null).unwrap().null_count(), 1);
    }

    #[test]
    fn a_filter_evaluates_its_arithmetic_and_refuses_an_overflowing_row() {
        use arrow_array::RecordBatch;
        use arrow_schema::{Field, Schema};
        use omnigraph_compiler::ir::IRExpr;
        use omnigraph_compiler::query::value::{BinaryOp, CompOp, Literal};
        let ty = ExprType::from_prop(&PropType::scalar(ScalarType::I64, true));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "p.age",
                DataType::Int64,
                true,
            )])),
            vec![i64s(&[Some(1), Some(41), None])],
        )
        .unwrap();
        let age = IRExpr::PropAccess {
            variable: "p".into(),
            property: "age".into(),
            ty: ty.clone(),
        };
        let literal = |n| {
            IRExpr::Literal(
                Literal::Integer(n),
                ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
            )
        };
        let plus = |n| IRExpr::binary(age.clone(), BinaryOp::Arith(ArithOp::Add), literal(n));
        let filter = IRExpr::comparison(plus(1), CompOp::Gt, literal(10));
        let params = Default::default();
        let mask = super::super::expr::evaluate_filter(&batch, &filter, &params).unwrap();
        assert_eq!(
            mask,
            BooleanArray::from(vec![Some(false), Some(true), None])
        );
        let overflow = IRExpr::comparison(plus(i128::from(i64::MAX)), CompOp::Gt, literal(0));
        let error = super::super::expr::evaluate_filter(&batch, &overflow, &params).unwrap_err();
        assert_eq!(code(error), "E1");
    }

    #[test]
    fn if_coalesce_and_concat_over_nulls() {
        let ty = ExprType::from_prop(&PropType::scalar(ScalarType::I64, true));
        let cond: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
        let a = i64s(&[Some(1), Some(1), Some(1)]);
        let b = i64s(&[Some(2), None, Some(2)]);
        let picked = call(ScalarFunc::If, &[cond, a, Arc::clone(&b)], &ty).unwrap();
        // `if(null, a, b)` is `b`.
        assert_eq!(
            picked.as_ref(),
            &Int64Array::from(vec![Some(1), None, Some(2)])
        );
        let c = i64s(&[Some(3), Some(3), None]);
        let first = call(ScalarFunc::Coalesce, &[b, c], &ty).unwrap();
        assert_eq!(
            first.as_ref(),
            &Int64Array::from(vec![Some(2), Some(3), Some(2)])
        );
        let s = ExprType::from_prop(&PropType::scalar(ScalarType::String, true));
        let x: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let y: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("b")]));
        let joined = call(ScalarFunc::Concat, &[x, y], &s).unwrap();
        assert_eq!(joined.as_ref(), &StringArray::from(vec![Some("ab"), None]));
    }
}
