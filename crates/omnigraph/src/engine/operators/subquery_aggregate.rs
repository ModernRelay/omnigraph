//! The decorrelated evaluation of a typed block predicate. Tagged inner rows
//! feed the stored aggregate implementation; finalization and explicit casts
//! precede the same-domain comparison used by ordinary expressions.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, RecordBatch, UInt32Array,
};
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use omnigraph_compiler::ir::{BlockAggregateExpr, IRExpr, ParamMap, SubqueryPredicate};
use omnigraph_compiler::query::ast::{AggFunc, CompOp};
use omnigraph_compiler::types::{AggSignature, ExprType};
use omnigraph_planner::{Accumulator, AggregateSpec, plan_block_aggregate};

use crate::engine::constant::evaluate_constant_array;
use crate::engine::expr::compare_arrays;
use crate::engine::typed_value::{cast_block_array, check_array_type};
use crate::error::{OmniError, Result};

/// An implementation selected by the planner, never by a batch's values.
#[derive(Debug, Clone, Copy)]
pub(super) enum Number {
    Exact(i128),
    Float(f64),
}

impl Number {
    fn add(&mut self, other: Self) -> Result<()> {
        match (self, other) {
            (Self::Exact(total), Self::Exact(value)) => {
                *total = total.checked_add(value).ok_or_else(|| {
                    OmniError::manifest("block integer sum exceeds the exact i128 range")
                })?;
            }
            (Self::Float(total), Self::Float(value)) => *total += value,
            (Self::Exact(_), Self::Float(_)) | (Self::Float(_), Self::Exact(_)) => {
                return Err(OmniError::manifest_internal(
                    "block accumulator input differs from its specification",
                ));
            }
        }
        Ok(())
    }

    fn finalize(self) -> ScalarValue {
        ScalarValue::Float64(Some(match self {
            Self::Exact(value) => value as f64,
            Self::Float(value) => value,
        }))
    }
}

/// One outer row's running value; `count` needs only the separate count.
#[derive(Debug, Clone)]
pub(super) enum ValueAccumulator {
    Sum(Number),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
}

impl ValueAccumulator {
    fn for_spec(func: AggFunc, spec: AggregateSpec) -> Result<Option<Self>> {
        Ok(match (func, spec.accumulator) {
            (AggFunc::Count, Accumulator::Count) => None,
            (AggFunc::Sum, Accumulator::ExactInteger) => Some(Self::Sum(Number::Exact(0))),
            (AggFunc::Sum | AggFunc::Avg, Accumulator::Float64) => {
                Some(Self::Sum(Number::Float(0.0)))
            }
            (AggFunc::Min, Accumulator::Extremum) => Some(Self::Min(None)),
            (AggFunc::Max, Accumulator::Extremum) => Some(Self::Max(None)),
            (
                AggFunc::Count,
                Accumulator::ExactInteger | Accumulator::Float64 | Accumulator::Extremum,
            )
            | (AggFunc::Sum, Accumulator::Count | Accumulator::Extremum)
            | (
                AggFunc::Avg,
                Accumulator::Count | Accumulator::ExactInteger | Accumulator::Extremum,
            )
            | (
                AggFunc::Min | AggFunc::Max,
                Accumulator::Count | Accumulator::ExactInteger | Accumulator::Float64,
            ) => {
                return Err(OmniError::manifest_internal(
                    "block function differs from its aggregate specification",
                ));
            }
        })
    }

    fn fold(&mut self, column: &Column, row: usize) -> Result<()> {
        match (self, column) {
            (Self::Sum(total), Column::Exact(values)) => {
                total.add(Number::Exact(values.value(row)))?
            }
            (Self::Sum(total), Column::Float(values)) => {
                total.add(Number::Float(values.value(row)))?
            }
            (Self::Min(extremum), Column::Scalar(values)) => {
                fold_extremum(extremum, values, row, Ordering::Greater)?;
            }
            (Self::Max(extremum), Column::Scalar(values)) => {
                fold_extremum(extremum, values, row, Ordering::Less)?;
            }
            (Self::Sum(_), Column::Scalar(_))
            | (Self::Min(_) | Self::Max(_), Column::Exact(_) | Column::Float(_)) => {
                return Err(OmniError::manifest_internal(
                    "block column differs from its accumulator",
                ));
            }
        }
        Ok(())
    }
}

fn fold_extremum(
    extremum: &mut Option<ScalarValue>,
    values: &ArrayRef,
    row: usize,
    replace: Ordering,
) -> Result<()> {
    let value = ScalarValue::try_from_array(values, row).map_err(OmniError::datafusion)?;
    if matches!(&value, ScalarValue::Float32(Some(v)) if v.is_nan())
        || matches!(&value, ScalarValue::Float64(Some(v)) if v.is_nan())
    {
        return Ok(());
    }
    let ordering = extremum
        .as_ref()
        .and_then(|current| match (current, &value) {
            (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => a.partial_cmp(b),
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a.partial_cmp(b),
            _ => current.partial_cmp(&value),
        });
    if extremum.is_none() || ordering == Some(replace) {
        *extremum = Some(value);
    }
    Ok(())
}

/// Bound once, retaining the left recipe and the right Arrow scalar domain.
struct BoundComparison {
    left: BlockAggregateExpr,
    op: CompOp,
    right: ArrayRef,
}

impl BoundComparison {
    fn new(predicate: &SubqueryPredicate, params: &ParamMap) -> Result<Self> {
        predicate.check_types()?;
        Ok(Self {
            left: predicate.left.clone(),
            op: predicate.op,
            right: evaluate_constant_array(&predicate.right, params)?,
        })
    }

    fn holds(&self, value: ScalarValue) -> Result<bool> {
        let value = value.to_array_of_size(1).map_err(OmniError::datafusion)?;
        let value = cast_left(&self.left, &value)?;
        let mask = compare_arrays(&value, self.op, &self.right)?;
        Ok(mask.is_valid(0) && mask.value(0))
    }
}

fn cast_left(expr: &BlockAggregateExpr, value: &ArrayRef) -> Result<ArrayRef> {
    let mut current = expr;
    let mut casts = Vec::new();
    while let BlockAggregateExpr::Cast { expr, ty } = current {
        casts.push((expr.ty(), ty));
        current = expr;
    }
    match current {
        BlockAggregateExpr::CountRows { .. } | BlockAggregateExpr::Aggregate { .. } => {
            check_array_type(value, current.ty(), "block aggregate result")?;
        }
        BlockAggregateExpr::Cast { .. } => {
            return Err(OmniError::manifest_internal(
                "block aggregate leaf is still a cast",
            ));
        }
    }
    let mut value = Arc::clone(value);
    for (source, target) in casts.into_iter().rev() {
        value = cast_block_array(&value, source, target)?;
    }
    Ok(value)
}

fn count_value(count: u64) -> Result<ScalarValue> {
    let count = i64::try_from(count)
        .map_err(|_| OmniError::manifest("block count exceeds the I64 range"))?;
    Ok(ScalarValue::Int64(Some(count)))
}

/// The bulk CSR degree path executes the same bound predicate as tagged rows.
pub(crate) struct RowCountPredicate {
    comparison: BoundComparison,
    existence_only: bool,
}

impl RowCountPredicate {
    pub(crate) fn resolve(
        predicate: &SubqueryPredicate,
        params: &ParamMap,
    ) -> Result<Option<Self>> {
        if !predicate.is_row_count() {
            return Ok(None);
        }
        Ok(Some(Self {
            comparison: BoundComparison::new(predicate, params)?,
            existence_only: predicate.is_existence_test(),
        }))
    }

    pub(crate) fn existence_only(&self) -> bool {
        self.existence_only
    }

    pub(crate) fn holds(&self, count: u64) -> Result<bool> {
        self.comparison.holds(count_value(count)?)
    }
}

/// One stored implementation and running aggregate per outer row.
pub(crate) struct SubqueryAggregate {
    comparison: BoundComparison,
    spec: Option<AggregateSpec>,
    counts: Vec<u64>,
    values: Option<Vec<ValueAccumulator>>,
}

impl SubqueryAggregate {
    pub(crate) fn tracks_values(spec: Option<AggregateSpec>) -> bool {
        spec.is_some_and(|spec| spec.accumulator != Accumulator::Count)
    }

    pub(crate) fn new(
        predicate: &SubqueryPredicate,
        spec: Option<AggregateSpec>,
        params: &ParamMap,
        outer_rows: usize,
    ) -> Result<Self> {
        let expected = plan_block_aggregate(&predicate.left)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if spec != expected {
            return Err(OmniError::manifest_internal(
                "block aggregate specification differs from its signature",
            ));
        }
        let accumulator = match (predicate.left.leaf(), spec) {
            (BlockAggregateExpr::CountRows { .. }, None) => None,
            (BlockAggregateExpr::Aggregate { func, .. }, Some(spec)) => {
                ValueAccumulator::for_spec(*func, spec)?
            }
            (BlockAggregateExpr::CountRows { .. }, Some(_))
            | (BlockAggregateExpr::Aggregate { .. }, None)
            | (BlockAggregateExpr::Cast { .. }, _) => {
                return Err(OmniError::manifest_internal(
                    "invalid block aggregate leaf or specification",
                ));
            }
        };
        Ok(Self {
            comparison: BoundComparison::new(predicate, params)?,
            spec,
            counts: vec![0; outer_rows],
            values: accumulator.map(|accumulator| vec![accumulator; outer_rows]),
        })
    }

    /// Null arguments do not contribute. Every nonnull argument is interpreted
    /// only in its signature's type and the planner's selected accumulator.
    pub(crate) fn absorb(&mut self, tags: &UInt32Array, values: Option<&ArrayRef>) -> Result<()> {
        let column = match (self.comparison.left.leaf(), self.spec, values) {
            (BlockAggregateExpr::CountRows { .. }, None, None) => None,
            (BlockAggregateExpr::Aggregate { signature, .. }, Some(spec), Some(values)) => {
                if values.len() != tags.len() {
                    return Err(OmniError::manifest_internal(
                        "block argument and tag lengths differ",
                    ));
                }
                check_array_type(values, &signature.arg, "block aggregate argument")?;
                if spec.accumulator == Accumulator::Count {
                    None
                } else {
                    Some(Column::for_spec(values, signature, spec)?)
                }
            }
            (BlockAggregateExpr::CountRows { .. }, _, _)
            | (BlockAggregateExpr::Aggregate { .. }, _, _)
            | (BlockAggregateExpr::Cast { .. }, _, _) => {
                return Err(OmniError::manifest_internal(
                    "block argument differs from its stored producer",
                ));
            }
        };
        let outer_rows = self.counts.len();
        for row in 0..tags.len() {
            if tags.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "block correlation tag is null",
                ));
            }
            let tag = tags.value(row) as usize;
            let count = self.counts.get_mut(tag).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "subquery inner row carries tag {tag} beyond the {outer_rows} outer rows"
                ))
            })?;
            if values.is_some_and(|values| values.is_null(row)) {
                continue;
            }
            *count = count
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest("block row counter overflow"))?;
            if let (Some(column), Some(accumulators)) = (&column, &mut self.values) {
                accumulators[tag].fold(column, row)?;
            }
        }
        Ok(())
    }

    /// Finalize in the leaf's declared result before any comparison cast.
    fn aggregate(&self, row: usize) -> Result<ScalarValue> {
        let leaf = self.comparison.left.leaf();
        let count = self.counts[row];
        let accumulator = self.values.as_ref().map(|values| &values[row]);
        let null =
            || {
                ScalarValue::try_from(&leaf.ty().to_arrow().ok_or_else(|| {
                    OmniError::manifest_internal("block result has no Arrow type")
                })?)
                .map_err(OmniError::datafusion)
            };
        let value = match (leaf, accumulator) {
            (BlockAggregateExpr::CountRows { .. }, None)
            | (
                BlockAggregateExpr::Aggregate {
                    func: AggFunc::Count,
                    ..
                },
                None,
            ) => count_value(count)?,
            (
                BlockAggregateExpr::Aggregate {
                    func: AggFunc::Sum, ..
                },
                Some(ValueAccumulator::Sum(sum)),
            ) => {
                if count == 0 {
                    null()?
                } else {
                    sum.finalize()
                }
            }
            (
                BlockAggregateExpr::Aggregate {
                    func: AggFunc::Avg, ..
                },
                Some(ValueAccumulator::Sum(Number::Float(sum))),
            ) => {
                if count == 0 {
                    null()?
                } else {
                    ScalarValue::Float64(Some(sum / count as f64))
                }
            }
            (
                BlockAggregateExpr::Aggregate {
                    func: AggFunc::Min, ..
                },
                Some(ValueAccumulator::Min(value)),
            )
            | (
                BlockAggregateExpr::Aggregate {
                    func: AggFunc::Max, ..
                },
                Some(ValueAccumulator::Max(value)),
            ) => match value {
                Some(value) => value.clone(),
                None => null()?,
            },
            (BlockAggregateExpr::CountRows { .. }, Some(_))
            | (BlockAggregateExpr::Aggregate { .. }, _)
            | (BlockAggregateExpr::Cast { .. }, _) => {
                return Err(OmniError::manifest_internal(
                    "block aggregate state differs from its stored producer",
                ));
            }
        };
        Ok(value)
    }

    pub(crate) fn keep_mask(&self) -> Result<BooleanArray> {
        (0..self.counts.len())
            .map(|row| self.comparison.holds(self.aggregate(row)?))
            .collect::<Result<Vec<_>>>()
            .map(BooleanArray::from)
    }
}

/// Fold tagged inner batches; the argument's expression executes in the inner scope.
pub(crate) fn absorb_inner_batches(
    aggregate: &mut SubqueryAggregate,
    batches: &[RecordBatch],
    tag_column: &str,
    arg: Option<&IRExpr>,
    evaluate: &dyn Fn(&RecordBatch, &IRExpr) -> Result<ArrayRef>,
) -> Result<()> {
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let tags = batch
            .column_by_name(tag_column)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "anti-join inner pipeline dropped the correlation column",
                )
            })?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                OmniError::manifest_internal(format!("'{tag_column}' column is not UInt32"))
            })?;
        let values = arg.map(|arg| evaluate(batch, arg)).transpose()?;
        aggregate.absorb(tags, values.as_ref())?;
    }
    Ok(())
}

/// Convert toward the stored accumulator, retaining original typed extrema.
enum Column {
    Exact(Decimal128Array),
    Float(Float64Array),
    Scalar(ArrayRef),
}

impl Column {
    fn for_spec(values: &ArrayRef, signature: &AggSignature, spec: AggregateSpec) -> Result<Self> {
        check_array_type(values, &signature.arg, "block accumulator argument")?;
        let target = match spec.accumulator {
            Accumulator::ExactInteger => ExprType::exact_integer_arrow(),
            Accumulator::Float64 => DataType::Float64,
            Accumulator::Extremum => return Ok(Self::Scalar(Arc::clone(values))),
            Accumulator::Count => {
                return Err(OmniError::manifest_internal(
                    "count needs no value accumulator",
                ));
            }
        };
        let converted = arrow_cast::cast::cast_with_options(
            values,
            &target,
            &arrow_cast::cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(OmniError::arrow_internal)?;
        match spec.accumulator {
            Accumulator::ExactInteger => converted
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .cloned()
                .map(Self::Exact)
                .ok_or_else(|| OmniError::manifest_internal("exact block input is not Decimal128")),
            Accumulator::Float64 => converted
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .map(Self::Float)
                .ok_or_else(|| OmniError::manifest_internal("floating block input is not Float64")),
            Accumulator::Count | Accumulator::Extremum => Err(OmniError::manifest_internal(
                "unexpected block accumulator conversion",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float32Array, Int64Array, UInt64Array};
    use omnigraph_compiler::query::ast::Literal;
    use omnigraph_compiler::types::{PropType, ScalarType};

    fn scalar(kind: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(kind, nullable))
    }

    fn column_predicate(func: AggFunc, kind: ScalarType, right: IRExpr) -> SubqueryPredicate {
        let arg = scalar(kind, true);
        let result = scalar(func.result_type(&arg).unwrap(), true);
        SubqueryPredicate {
            left: BlockAggregateExpr::Aggregate {
                func,
                arg: Box::new(IRExpr::PropAccess {
                    variable: "m".into(),
                    property: "value".into(),
                    ty: arg.clone(),
                }),
                signature: AggSignature { arg, result },
            },
            op: CompOp::Eq,
            right,
        }
    }

    fn aggregate(predicate: &SubqueryPredicate, rows: usize) -> SubqueryAggregate {
        SubqueryAggregate::new(
            predicate,
            plan_block_aggregate(&predicate.left).unwrap(),
            &ParamMap::new(),
            rows,
        )
        .unwrap()
    }

    /// GQT cannot reach internal i128 overflow or inspect the exact subtotal.
    #[test]
    fn exact_accumulator_checks_overflow_and_finalizes_once() {
        let mut total = Number::Exact(i128::MAX);
        assert!(
            total
                .add(Number::Exact(1))
                .unwrap_err()
                .to_string()
                .contains("i128 range")
        );
        let predicate = column_predicate(
            AggFunc::Sum,
            ScalarType::I64,
            IRExpr::Literal(Literal::Float(1.0), scalar(ScalarType::F64, false)),
        );
        for values in [
            vec![9_007_199_254_740_993, -9_007_199_254_740_992],
            vec![-9_007_199_254_740_992, 9_007_199_254_740_993],
        ] {
            let mut aggregate = aggregate(&predicate, 2);
            let values: ArrayRef = Arc::new(Int64Array::from(values));
            aggregate
                .absorb(&UInt32Array::from(vec![0, 0]), Some(&values))
                .unwrap();
            assert_eq!(
                aggregate.aggregate(0).unwrap(),
                ScalarValue::Float64(Some(1.0))
            );
            assert_eq!(aggregate.aggregate(1).unwrap(), ScalarValue::Float64(None));
            assert_eq!(
                aggregate.keep_mask().unwrap(),
                BooleanArray::from(vec![true, false])
            );
        }
        let mut total = aggregate(&predicate, 1);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![9_007_199_254_740_993]));
        total
            .absorb(&UInt32Array::from(vec![0]), Some(&values))
            .unwrap();
        assert_eq!(
            total.aggregate(0).unwrap(),
            ScalarValue::Float64(Some(9_007_199_254_740_992.0))
        );
    }

    /// GQT cannot inspect the intermediate extremum Arrow type.
    #[test]
    fn extrema_keep_u64_and_f32_arrow_types() {
        let predicate = column_predicate(
            AggFunc::Max,
            ScalarType::U64,
            IRExpr::Literal(Literal::Integer(0), scalar(ScalarType::U64, false)),
        );
        let mut wide = aggregate(&predicate, 1);
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX - 1, u64::MAX]));
        wide.absorb(&UInt32Array::from(vec![0, 0]), Some(&values))
            .unwrap();
        assert_eq!(
            wide.aggregate(0).unwrap(),
            ScalarValue::UInt64(Some(u64::MAX))
        );
        let exact_predicate = omnigraph_compiler::ir::coerce::block(
            predicate.left.clone(),
            CompOp::Gt,
            IRExpr::Param("bound".into(), scalar(ScalarType::I64, false)),
        )
        .unwrap();
        assert!(matches!(
            exact_predicate.left.ty(),
            ExprType::ExactInteger { .. }
        ));
        let mut exact = SubqueryAggregate::new(
            &exact_predicate,
            plan_block_aggregate(&exact_predicate.left).unwrap(),
            &ParamMap::from([("bound".into(), Literal::Integer(i64::MAX))]),
            1,
        )
        .unwrap();
        exact
            .absorb(&UInt32Array::from(vec![0, 0]), Some(&values))
            .unwrap();
        assert_eq!(exact.keep_mask().unwrap(), BooleanArray::from(vec![true]));

        let predicate = column_predicate(
            AggFunc::Min,
            ScalarType::F32,
            IRExpr::Literal(Literal::Float(0.1), scalar(ScalarType::F32, false)),
        );
        let mut narrow = aggregate(&predicate, 1);
        let values: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN, 0.2, 0.1]));
        narrow
            .absorb(&UInt32Array::from(vec![0, 0, 0]), Some(&values))
            .unwrap();
        assert_eq!(
            narrow.aggregate(0).unwrap(),
            ScalarValue::Float32(Some(0.1))
        );
        assert_eq!(narrow.keep_mask().unwrap(), BooleanArray::from(vec![true]));
    }

    /// GQT cannot seed non-finite values or inspect signed-zero accumulator bits.
    #[test]
    fn extrema_skip_nan_and_preserve_first_signed_zero() {
        for func in [AggFunc::Min, AggFunc::Max] {
            let predicate = column_predicate(
                func,
                ScalarType::F32,
                IRExpr::Literal(Literal::Float(0.0), scalar(ScalarType::F32, false)),
            );
            let mut aggregate = aggregate(&predicate, 2);
            let values: ArrayRef =
                Arc::new(Float32Array::from(vec![f32::NAN, -0.0, 0.0, f32::NAN]));
            aggregate
                .absorb(&UInt32Array::from(vec![0, 0, 0, 1]), Some(&values))
                .unwrap();
            let ScalarValue::Float32(Some(value)) = aggregate.aggregate(0).unwrap() else {
                panic!("extremum must preserve its F32 type");
            };
            assert_eq!(value.to_bits(), (-0.0_f32).to_bits());
            assert_eq!(aggregate.aggregate(1).unwrap(), ScalarValue::Float32(None));
        }
    }

    /// Synthetic degrees reach count overflow without constructing an impossible graph.
    #[test]
    fn bulk_and_tagged_counts_share_typed_casts_nulls_and_checked_range() {
        let count = BlockAggregateExpr::CountRows {
            ty: scalar(ScalarType::I64, false),
        };
        for (left, right, bound) in [
            (
                count.clone(),
                IRExpr::Param("bound".into(), scalar(ScalarType::I64, true)),
                Literal::Integer(1),
            ),
            (
                BlockAggregateExpr::Cast {
                    expr: Box::new(count.clone()),
                    ty: scalar(ScalarType::F64, false),
                },
                IRExpr::Param("bound".into(), scalar(ScalarType::F64, true)),
                Literal::Float(1.0),
            ),
            (
                BlockAggregateExpr::Cast {
                    expr: Box::new(count.clone()),
                    ty: ExprType::ExactInteger {
                        list: false,
                        nullable: false,
                    },
                },
                IRExpr::Cast {
                    expr: Box::new(IRExpr::Param("bound".into(), scalar(ScalarType::U64, true))),
                    ty: ExprType::ExactInteger {
                        list: false,
                        nullable: true,
                    },
                },
                Literal::Integer(1),
            ),
        ] {
            let predicate = SubqueryPredicate {
                left,
                op: CompOp::Ge,
                right,
            };
            for bound in [bound, Literal::Null] {
                let params = ParamMap::from([("bound".into(), bound)]);
                let bulk = RowCountPredicate::resolve(&predicate, &params)
                    .unwrap()
                    .unwrap();
                assert!(!bulk.existence_only());
                let mut tagged = SubqueryAggregate::new(&predicate, None, &params, 1).unwrap();
                for count in [0, 1, 2, i64::MAX as u64] {
                    tagged.counts[0] = count;
                    assert_eq!(
                        bulk.holds(count).unwrap(),
                        tagged.keep_mask().unwrap().value(0)
                    );
                }
                assert!(
                    bulk.holds(i64::MAX as u64 + 1)
                        .unwrap_err()
                        .to_string()
                        .contains("I64 range")
                );
                tagged.counts[0] = i64::MAX as u64 + 1;
                assert!(
                    tagged
                        .keep_mask()
                        .unwrap_err()
                        .to_string()
                        .contains("I64 range")
                );
            }
        }
    }

    #[test]
    fn bare_count_shortcuts_are_conservative_and_specs_are_required() {
        for (op, bound) in [(CompOp::Eq, 0), (CompOp::Gt, 0), (CompOp::Lt, 1)] {
            let predicate = SubqueryPredicate {
                left: BlockAggregateExpr::CountRows {
                    ty: scalar(ScalarType::I64, false),
                },
                op,
                right: IRExpr::Literal(Literal::Integer(bound), scalar(ScalarType::I64, false)),
            };
            assert!(
                RowCountPredicate::resolve(&predicate, &ParamMap::new())
                    .unwrap()
                    .unwrap()
                    .existence_only()
            );
        }
        let predicate = column_predicate(
            AggFunc::Sum,
            ScalarType::I64,
            IRExpr::Literal(Literal::Float(0.0), scalar(ScalarType::F64, false)),
        );
        assert!(SubqueryAggregate::new(&predicate, None, &ParamMap::new(), 0).is_err());
        let mut aggregate = aggregate(&predicate, 1);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        assert!(
            aggregate
                .absorb(&UInt32Array::from(vec![0]), Some(&values))
                .unwrap_err()
                .to_string()
                .contains("violates compiler type")
        );
    }
}
