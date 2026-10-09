//! Constants execute the same typed expressions as batch filters. Arrow values
//! retain exact integer intermediates until a public result becomes a Literal.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use datafusion::scalar::ScalarValue;
use omnigraph_compiler::ir::{IRExpr, IROp, ParamMap, QueryIR};
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::types::{ExprType, ScalarType};

use crate::error::{OmniError, Result};

/// Evaluate one constant without converting typed intermediate values to Literal.
pub(super) fn evaluate_constant_array(expr: &IRExpr, params: &ParamMap) -> Result<ArrayRef> {
    expr.check_types()?;
    if !is_constant(expr) {
        return Err(OmniError::manifest(format!(
            "`{expr}` is not a constant; an assignment value or a binding match reads only \
             literals, parameters and now()"
        )));
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(OmniError::arrow_internal)?;
    super::expr::evaluate_expr(&batch, expr, params)
}

/// A final constant for assignment, keeping Blob URI leaves at their write boundary.
pub(crate) fn evaluate_constant(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    if matches!(expr.ty(), ExprType::ExactInteger { .. }) {
        return Err(OmniError::manifest_internal(
            "exact integer constant has no public literal representation",
        ));
    }
    if let ExprType::Value {
        scalar: ScalarType::Blob,
        list: false,
        nullable,
    } = expr.ty()
    {
        let value = match expr {
            IRExpr::Literal(value, _) => value,
            IRExpr::Param(name, _) => params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{name}' not provided")))?,
            IRExpr::PropAccess { .. }
            | IRExpr::Nearest { .. }
            | IRExpr::Search { .. }
            | IRExpr::Fuzzy { .. }
            | IRExpr::MatchText { .. }
            | IRExpr::Bm25 { .. }
            | IRExpr::Rrf { .. }
            | IRExpr::Variable(_, _)
            | IRExpr::Aggregate { .. }
            | IRExpr::AliasRef(_, _)
            | IRExpr::Binary { .. }
            | IRExpr::Not(_, _)
            | IRExpr::Cast { .. }
            | IRExpr::IsNull { .. } => {
                return Err(OmniError::manifest_internal(
                    "Blob constant is not a URI leaf",
                ));
            }
        };
        return if matches!(value, Literal::String(_)) || *nullable && matches!(value, Literal::Null)
        {
            Ok(value.clone())
        } else {
            Err(OmniError::manifest("expected blob URI string"))
        };
    }
    let array = evaluate_constant_array(expr, params)?;
    literal_from_scalar(
        ScalarValue::try_from_array(array.as_ref(), 0).map_err(OmniError::datafusion)?,
    )
}

fn literal_from_scalar(value: ScalarValue) -> Result<Literal> {
    if value.is_null() {
        return Ok(Literal::Null);
    }
    Ok(match value {
        ScalarValue::Boolean(Some(value)) => Literal::Bool(value),
        ScalarValue::Utf8(Some(value)) => Literal::String(value),
        ScalarValue::Int32(Some(value)) => Literal::Integer(i64::from(value)),
        ScalarValue::Int64(Some(value)) => Literal::Integer(value),
        ScalarValue::UInt32(Some(value)) => Literal::Integer(i64::from(value)),
        ScalarValue::UInt64(Some(value)) => {
            Literal::Integer(i64::try_from(value).map_err(|_| {
                OmniError::manifest("constant U64 result exceeds the public literal range")
            })?)
        }
        ScalarValue::Float32(Some(value)) => Literal::Float(f64::from(value)),
        ScalarValue::Float64(Some(value)) => Literal::Float(value),
        ScalarValue::Date32(Some(value)) => {
            let date =
                arrow_array::temporal_conversions::date32_to_datetime(value).ok_or_else(|| {
                    OmniError::manifest("constant Date result is outside the calendar range")
                })?;
            Literal::Date(date.date().format("%Y-%m-%d").to_string())
        }
        ScalarValue::Date64(Some(value)) => {
            let date =
                arrow_array::temporal_conversions::date64_to_datetime(value).ok_or_else(|| {
                    OmniError::manifest("constant DateTime result is outside the calendar range")
                })?;
            Literal::DateTime(
                date.and_utc()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
        }
        ScalarValue::List(values) => literal_list(&values.value(0))?,
        ScalarValue::FixedSizeList(values) => literal_list(&values.value(0))?,
        other => {
            return Err(OmniError::manifest_internal(format!(
                "constant result {} has no public literal representation",
                other.data_type()
            )));
        }
    })
}

fn literal_list(values: &ArrayRef) -> Result<Literal> {
    (0..values.len())
        .map(|row| {
            literal_from_scalar(
                ScalarValue::try_from_array(values.as_ref(), row).map_err(OmniError::datafusion)?,
            )
        })
        .collect::<Result<Vec<_>>>()
        .map(Literal::List)
}

/// Fold bound constant filters without changing their compiler-selected types.
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
    if matches!(expr, IRExpr::Cast { .. }) {
        return Ok(expr.clone());
    }
    if is_constant(expr) && !matches!(expr, IRExpr::Literal(_, _) | IRExpr::Param(_, _)) {
        if matches!(expr.ty(), ExprType::ExactInteger { .. }) {
            return Ok(expr.clone());
        }
        return Ok(IRExpr::Literal(
            evaluate_constant(expr, params)?,
            expr.ty().clone(),
        ));
    }
    Ok(match expr {
        IRExpr::Binary {
            left,
            op,
            right,
            ty,
        } => IRExpr::Binary {
            left: Box::new(fold_expr(left, params)?),
            op: *op,
            right: Box::new(fold_expr(right, params)?),
            ty: ty.clone(),
        },
        IRExpr::Not(inner, ty) => IRExpr::Not(Box::new(fold_expr(inner, params)?), ty.clone()),
        IRExpr::IsNull { expr, negated, ty } => IRExpr::IsNull {
            expr: Box::new(fold_expr(expr, params)?),
            negated: *negated,
            ty: ty.clone(),
        },
        IRExpr::Cast { expr, ty } => IRExpr::Cast {
            expr: Box::new(fold_expr(expr, params)?),
            ty: ty.clone(),
        },
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Param(_, _)
        | IRExpr::Literal(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => expr.clone(),
    })
}

pub(super) fn is_constant(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::Literal(_, _) | IRExpr::Param(_, _) => true,
        IRExpr::Binary { left, right, .. } => is_constant(left) && is_constant(right),
        IRExpr::Not(inner, _) => is_constant(inner),
        IRExpr::IsNull { expr, .. } | IRExpr::Cast { expr, .. } => is_constant(expr),
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array, Float64Array, ListArray};
    use arrow_schema::{DataType, Field};
    use omnigraph_compiler::query::ast::{BinaryOp, CompOp};
    use omnigraph_compiler::types::PropType;

    fn scalar(kind: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(kind, nullable))
    }

    #[test]
    fn constant_cast_executes_without_erasing_its_stored_witness() {
        let cast = IRExpr::Cast {
            expr: Box::new(IRExpr::Literal(
                Literal::Float(30.0),
                scalar(ScalarType::F64, false),
            )),
            ty: scalar(ScalarType::I64, false),
        };
        let params = ParamMap::new();
        let values = evaluate_constant_array(&cast, &params).expect("exact literal narrowing");
        assert_eq!(values.data_type(), &DataType::Int64);
        assert_eq!(
            evaluate_constant(&cast, &params).unwrap(),
            Literal::Integer(30)
        );
        assert_eq!(fold_expr(&cast, &params).unwrap(), cast);
    }

    #[test]
    fn constant_exact_intermediates_preserve_u64_values_and_list_nulls() {
        let exact = ExprType::ExactInteger {
            list: false,
            nullable: false,
        };
        let cast = |expr| IRExpr::Cast {
            expr: Box::new(expr),
            ty: exact.clone(),
        };
        let signed = cast(IRExpr::Literal(
            Literal::Integer(i64::MAX),
            scalar(ScalarType::I64, false),
        ));
        let unsigned = cast(IRExpr::Param("wide".into(), scalar(ScalarType::U64, false)));
        let params = ParamMap::from([("wide".into(), Literal::Float(2_f64.powi(63)))]);
        let values = evaluate_constant_array(&unsigned, &params).unwrap();
        assert_eq!(values.data_type(), &ExprType::exact_integer_arrow());
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            1_i128 << 63
        );
        assert!(evaluate_constant(&unsigned, &params).is_err());
        let comparison = IRExpr::Binary {
            left: Box::new(signed),
            op: BinaryOp::Compare(CompOp::Lt),
            right: Box::new(unsigned),
            ty: scalar(ScalarType::Bool, false),
        };
        assert_eq!(
            evaluate_constant(&comparison, &params).unwrap(),
            Literal::Bool(true)
        );

        let list = IRExpr::Cast {
            expr: Box::new(IRExpr::Literal(
                Literal::List(vec![Literal::Integer(i64::MAX), Literal::Null]),
                ExprType::from_prop(&PropType::list_of(ScalarType::I64, false)),
            )),
            ty: ExprType::ExactInteger {
                list: true,
                nullable: false,
            },
        };
        let values = evaluate_constant_array(&list, &params).unwrap();
        let values = values
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(values.value(0), i128::from(i64::MAX));
        assert!(values.is_null(1));
    }

    #[test]
    fn forged_nonconstant_float_narrowing_refuses_before_infinity_or_null() {
        let cast = IRExpr::Cast {
            expr: Box::new(IRExpr::PropAccess {
                variable: "p".into(),
                property: "amount".into(),
                ty: scalar(ScalarType::F64, false),
            }),
            ty: scalar(ScalarType::F32, false),
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "p.amount",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::MAX]))],
        )
        .unwrap();
        let error = super::super::expr::evaluate_expr(&batch, &cast, &ParamMap::new()).unwrap_err();
        assert!(
            error.to_string().contains("invalid recorded cast"),
            "{error}"
        );
        assert!(super::super::scan::ir_expr_to_df_expr(&cast, &ParamMap::new(), None).is_none());
    }
}
