//! The decorrelated evaluation of a `SubqueryPredicate`: every inner row
//! carries the tag of the outer row it came from, `SubqueryAggregate::absorb`
//! folds the inner batches into one running aggregate per outer row, and
//! `keep_mask` applies the predicate to each row's aggregate. Shared by
//! `AntiJoinMaskExec` and the v1 executor.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt32Array,
};
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use omnigraph_compiler::ir::{IRExpr, ParamMap, SubqueryPredicate};
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal};

use crate::error::{OmniError, Result};

/// A numeric aggregate value: integer while every input is an integer and
/// the sum fits, floating point otherwise.
#[derive(Debug, Clone, Copy)]
pub(super) enum Number {
    Int(i64),
    Float(f64),
}

impl Number {
    fn from_count(count: u64) -> Self {
        i64::try_from(count).map_or(Self::Float(count as f64), Self::Int)
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(v) => v as f64,
            Self::Float(v) => v,
        }
    }

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => match a.checked_add(b) {
                Some(sum) => Self::Int(sum),
                None => Self::Float(a as f64 + b as f64),
            },
            (a, b) => Self::Float(a.as_f64() + b.as_f64()),
        }
    }

    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => Some(a.cmp(&b)),
            (a, b) => a.as_f64().partial_cmp(&b.as_f64()),
        }
    }
}

/// An aggregate value: numbers compare across integer and float; every other
/// orderable scalar (`String`, `Bool`, `Date`, `DateTime`) compares within
/// its own Arrow type.
#[derive(Debug, Clone)]
pub(super) enum Value {
    Number(Number),
    Scalar(ScalarValue),
}

impl Value {
    /// `Ok(None)` is an unordered pair (a NaN); a number against a
    /// non-numeric scalar is a typing gap upstream and fails loudly.
    fn compare(&self, other: &Self) -> Result<Option<Ordering>> {
        match (self, other) {
            (Self::Number(a), Self::Number(b)) => Ok(a.compare(*b)),
            (Self::Scalar(a), Self::Scalar(b)) => Ok(a.partial_cmp(b)),
            (Self::Number(_), Self::Scalar(s)) | (Self::Scalar(s), Self::Number(_)) => {
                Err(OmniError::manifest_internal(format!(
                    "subquery aggregate compares a number with a {} value",
                    s.data_type()
                )))
            }
        }
    }

    /// A NaN is unordered, so `min` and `max` never take it as their value.
    fn is_nan(&self) -> bool {
        matches!(self, Self::Number(Number::Float(v)) if v.is_nan())
    }

    fn min(self, other: Self) -> Result<Self> {
        Ok(match self.compare(&other)? {
            Some(Ordering::Greater) => other,
            _ => self,
        })
    }

    fn max(self, other: Self) -> Result<Self> {
        Ok(match self.compare(&other)? {
            Some(Ordering::Less) => other,
            _ => self,
        })
    }
}

/// One outer row's running value over its inner rows, the one the
/// predicate's function reads (`avg` reads the sum).
#[derive(Debug, Clone)]
pub(super) enum ValueAccumulator {
    Sum(Number),
    Min(Option<Value>),
    Max(Option<Value>),
}

impl ValueAccumulator {
    /// `None` for `count`, which needs no value.
    fn for_func(func: AggFunc) -> Option<Self> {
        match func {
            AggFunc::Count => None,
            AggFunc::Sum | AggFunc::Avg => Some(Self::Sum(Number::Int(0))),
            AggFunc::Min => Some(Self::Min(None)),
            AggFunc::Max => Some(Self::Max(None)),
        }
    }

    fn fold(&mut self, value: Value) -> Result<()> {
        match self {
            Self::Sum(sum) => {
                if let Value::Number(number) = value {
                    *sum = sum.add(number);
                }
            }
            Self::Min(min) => {
                *min = Some(match min.take() {
                    None => value,
                    Some(current) => current.min(value)?,
                });
            }
            Self::Max(max) => {
                *max = Some(match max.take() {
                    None => value,
                    Some(current) => current.max(value)?,
                });
            }
        }
        Ok(())
    }
}

/// The predicate's right operand: a literal in the query or a parameter
/// bound at execution; `None` is a null bound.
fn predicate_bound(right: &IRExpr, params: &ParamMap) -> Result<Option<Value>> {
    let literal = match right {
        IRExpr::Literal(literal) => literal,
        IRExpr::Param(name) => params.get(name).ok_or_else(|| {
            OmniError::manifest(format!("subquery predicate parameter `${name}` is unbound"))
        })?,
        other => {
            return Err(OmniError::manifest_internal(format!(
                "subquery predicate compares with `{other}`, which is not a literal or parameter"
            )));
        }
    };
    Ok(match literal {
        Literal::Null => None,
        Literal::Integer(v) => Some(Value::Number(Number::Int(*v))),
        Literal::Float(v) => Some(Value::Number(Number::Float(*v))),
        other => {
            let array = crate::engine::expr::literal_to_array(other, 1)?;
            Some(Value::Scalar(
                ScalarValue::try_from_array(&array, 0).map_err(OmniError::datafusion)?,
            ))
        }
    })
}

/// Whether `value` satisfies `op bound`; an absent aggregate (no inner row
/// for a `sum`, `min`, `max` or `avg`) and a null bound satisfy nothing.
fn holds(value: Option<Value>, op: CompOp, bound: Option<&Value>) -> Result<bool> {
    let (Some(value), Some(bound)) = (value, bound) else {
        return Ok(false);
    };
    let Some(ordering) = value.compare(bound)? else {
        return Ok(false);
    };
    Ok(match op {
        CompOp::Eq => ordering == Ordering::Equal,
        CompOp::Ne => ordering != Ordering::Equal,
        CompOp::Gt => ordering == Ordering::Greater,
        CompOp::Lt => ordering == Ordering::Less,
        CompOp::Ge => ordering != Ordering::Less,
        CompOp::Le => ordering != Ordering::Greater,
        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => {
            return Err(OmniError::manifest_internal(format!(
                "subquery predicate operator `{op}` is not a comparison"
            )));
        }
    })
}

/// A row-count predicate with its bound resolved once, for the bulk CSR
/// degree path.
pub(crate) struct RowCountPredicate {
    op: CompOp,
    bound: Option<Value>,
    existence_only: bool,
}

impl RowCountPredicate {
    /// `None` when the predicate aggregates a column.
    pub(crate) fn resolve(
        predicate: &SubqueryPredicate,
        params: &ParamMap,
    ) -> Result<Option<Self>> {
        if !predicate.is_row_count() {
            return Ok(None);
        }
        Ok(Some(Self {
            op: predicate.op,
            bound: predicate_bound(&predicate.right, params)?,
            existence_only: predicate.is_existence_test(),
        }))
    }

    /// The predicate only asks whether any row matched.
    pub(crate) fn existence_only(&self) -> bool {
        self.existence_only
    }

    pub(crate) fn holds(&self, count: u64) -> Result<bool> {
        holds(
            Some(Value::Number(Number::from_count(count))),
            self.op,
            self.bound.as_ref(),
        )
    }
}

/// One running aggregate per outer row; `absorb` folds tagged inner batches
/// into it, `keep_mask` applies the predicate to each row's aggregate.
pub(crate) struct SubqueryAggregate {
    func: AggFunc,
    op: CompOp,
    bound: Option<Value>,
    counts: Vec<u64>,
    /// Present for `sum`, `avg`, `min` and `max`; a `count` needs the counts alone.
    values: Option<Vec<ValueAccumulator>>,
}

impl SubqueryAggregate {
    /// Whether `func` keeps a `ValueAccumulator` per outer row beside its count.
    pub(crate) fn tracks_values(func: AggFunc) -> bool {
        ValueAccumulator::for_func(func).is_some()
    }

    pub(crate) fn new(
        predicate: &SubqueryPredicate,
        params: &ParamMap,
        outer_rows: usize,
    ) -> Result<Self> {
        Ok(Self {
            func: predicate.func,
            op: predicate.op,
            bound: predicate_bound(&predicate.right, params)?,
            counts: vec![0; outer_rows],
            values: ValueAccumulator::for_func(predicate.func)
                .map(|accumulator| vec![accumulator; outer_rows]),
        })
    }

    /// Fold one inner batch: `tags` names each row's outer row, `values` is
    /// the aggregate's argument evaluated over the batch (`None` when the
    /// predicate counts rows). A null argument value contributes nothing, and
    /// a null bound keeps no row, so nothing is folded for it.
    pub(crate) fn absorb(&mut self, tags: &UInt32Array, values: Option<&ArrayRef>) -> Result<()> {
        if self.bound.is_none() {
            return Ok(());
        }
        let column = match values {
            Some(values) if self.values.is_some() => {
                Some(Column::widen(values, self.bound.as_ref())?)
            }
            _ => None,
        };
        let outer_rows = self.counts.len();
        for row in 0..tags.len() {
            if values.is_some_and(|values| values.is_null(row)) {
                continue;
            }
            let tag = tags.value(row) as usize;
            let count = self.counts.get_mut(tag).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "subquery inner row carries tag {tag} beyond the {outer_rows} outer rows"
                ))
            })?;
            *count += 1;
            let (Some(column), Some(accumulators)) = (&column, &mut self.values) else {
                continue;
            };
            let Some(value) = column.value_at(row)? else {
                continue;
            };
            let accumulator = &mut accumulators[tag];
            if value.is_nan() && !matches!(accumulator, ValueAccumulator::Sum(_)) {
                continue;
            }
            accumulator.fold(value)?;
        }
        Ok(())
    }

    /// The aggregate of one outer row; absent when a `sum`, `avg`, `min` or
    /// `max` saw no value.
    fn aggregate(&self, row: usize) -> Option<Value> {
        let count = self.counts[row];
        let accumulator = self.values.as_ref().map(|values| &values[row]);
        match (self.func, accumulator) {
            (AggFunc::Count, _) => Some(Value::Number(Number::from_count(count))),
            (AggFunc::Sum, Some(ValueAccumulator::Sum(sum))) => {
                (count > 0).then_some(Value::Number(*sum))
            }
            (AggFunc::Avg, Some(ValueAccumulator::Sum(sum))) => {
                (count > 0).then(|| Value::Number(Number::Float(sum.as_f64() / count as f64)))
            }
            (AggFunc::Min, Some(ValueAccumulator::Min(min))) => min.clone(),
            (AggFunc::Max, Some(ValueAccumulator::Max(max))) => max.clone(),
            _ => None,
        }
    }

    /// `true` for every outer row whose aggregate satisfies the predicate.
    pub(crate) fn keep_mask(&self) -> Result<BooleanArray> {
        let mut keep = Vec::with_capacity(self.counts.len());
        for row in 0..self.counts.len() {
            keep.push(Some(holds(
                self.aggregate(row),
                self.op,
                self.bound.as_ref(),
            )?));
        }
        Ok(keep.into_iter().collect())
    }
}

/// Fold every inner batch into `aggregate`: the tag column names each row's
/// outer row, `arg` (if any) is evaluated over the batch by `evaluate`.
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
                    "anti-join inner pipeline dropped the correlation column".to_string(),
                )
            })?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                OmniError::manifest_internal(format!("'{}' column is not UInt32", tag_column))
            })?;
        let values = arg.map(|arg| evaluate(batch, arg)).transpose()?;
        aggregate.absorb(tags, values.as_ref())?;
    }
    Ok(())
}

/// The aggregate's argument column: integers widened to Int64 (unsigned
/// 64-bit to Float64 only when a value exceeds the Int64 range), floats to
/// Float64, any other orderable scalar cast to the bound's Arrow type.
enum Column {
    Int(Int64Array),
    Float(Float64Array),
    Scalar(ArrayRef),
}

impl Column {
    fn widen(values: &ArrayRef, bound: Option<&Value>) -> Result<Self> {
        let target = match values.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => DataType::Int64,
            DataType::UInt64 => {
                let exact = arrow_cast::cast::CastOptions {
                    safe: false,
                    ..Default::default()
                };
                match arrow_cast::cast::cast_with_options(values, &DataType::Int64, &exact) {
                    Ok(_) => DataType::Int64,
                    Err(_) => DataType::Float64,
                }
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
            other => match bound {
                Some(Value::Scalar(scalar)) => scalar.data_type(),
                None => other.clone(),
                Some(Value::Number(_)) => {
                    return Err(OmniError::manifest_internal(format!(
                        "subquery aggregate compares a {other} column with a number"
                    )));
                }
            },
        };
        let widened = if values.data_type() == &target {
            Arc::clone(values)
        } else {
            arrow_cast::cast::cast(values, &target).map_err(OmniError::arrow_internal)?
        };
        Ok(match target {
            DataType::Int64 => Self::Int(
                widened
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        OmniError::manifest_internal("widened column is not Int64".to_string())
                    })?
                    .clone(),
            ),
            DataType::Float64 => Self::Float(
                widened
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        OmniError::manifest_internal("widened column is not Float64".to_string())
                    })?
                    .clone(),
            ),
            _ => Self::Scalar(widened),
        })
    }

    fn value_at(&self, row: usize) -> Result<Option<Value>> {
        Ok(match self {
            Self::Int(array) => {
                (!array.is_null(row)).then(|| Value::Number(Number::Int(array.value(row))))
            }
            Self::Float(array) => {
                (!array.is_null(row)).then(|| Value::Number(Number::Float(array.value(row))))
            }
            Self::Scalar(array) => {
                if array.is_null(row) {
                    None
                } else {
                    Some(Value::Scalar(
                        ScalarValue::try_from_array(array, row).map_err(OmniError::datafusion)?,
                    ))
                }
            }
        })
    }
}
