//! `collect`, the list aggregate: a group's non-null values as one list in
//! the fixed value order, `collect(distinct x)` without repeats, the empty
//! list over empty input. The values it holds are its accumulator's size,
//! which `AggregateExec` reserves in the query's memory pool as they arrive,
//! so a list beyond `query_memory_bytes` refuses the read.
//!
//! `stddev` and `percentile` hold a group's values the same way and finish
//! them in `F64` on GQ kernels, over the values ascending in the fixed value
//! order (the total order on floats, so `-0.0` before `0.0`), each converted
//! to the nearest `F64` (exact for every integer up to 2^53 and every `F32`):
//!
//! - `stddev(x)` is the sample standard deviation: the mean is the values'
//!   sum, added in that order, over n; the result is the square root of the
//!   squared differences from the mean, added in that order, over n − 1;
//!   null over fewer than two values. DataFusion's `stddev` folds Welford's
//!   running moments in arrival order and merges partitions by Chan's
//!   formula, so its last bits follow the physical layout; sorting first
//!   makes the answer a function of the group's values alone.
//! - `percentile(x, p)` is exact with linear interpolation (SQL's
//!   `PERCENTILE_CONT`): with n values and the index i = p · (n − 1), the
//!   value at ⌊i⌋ when i is whole, otherwise lower + (upper − lower) · (i −
//!   ⌊i⌋) between the values at ⌊i⌋ and ⌊i⌋ + 1; null over empty input.
//!   DataFusion's `percentile_cont` holds every value too, but rounds the
//!   interpolation fraction to 1e-6 and casts the result back to the
//!   argument's type, so it is not this row's implementation.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, UInt32Array};
use arrow_schema::{DataType, Field, FieldRef, SortOptions};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

/// How many arriving pieces an accumulator keeps before it folds them into
/// one sorted array (and, for `distinct`, drops the repeats).
const PIECES_BEFORE_FOLD: usize = 32;

/// What an accumulator makes of the group's values it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Finish {
    /// `collect`: the values as one list.
    List,
    /// `stddev`: their sample standard deviation.
    Stddev,
    /// `percentile`: the percentile at the fraction whose `F64` bits these
    /// are, checked to lie in [0, 1] before the read reads a row.
    Percentile(u64),
}

impl Finish {
    fn name(self) -> &'static str {
        match self {
            Self::List => "collect",
            Self::Stddev => "stddev",
            Self::Percentile(_) => "percentile",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CollectUdaf {
    signature: Signature,
    /// The list of the argument's values: the state every accumulator
    /// reports, and `collect`'s declared result.
    list: DataType,
    finish: Finish,
}

impl CollectUdaf {
    /// `collect` over an argument of type `element`, returning `result`, a
    /// `List` whose items are `element`.
    pub(super) fn new(element: DataType, result: DataType) -> Self {
        Self {
            signature: Signature::exact(vec![element], Volatility::Immutable),
            list: result,
            finish: Finish::List,
        }
    }

    /// `stddev` or `percentile` over a numeric argument of type `element`,
    /// returning `F64`.
    pub(super) fn finishing(element: DataType, finish: Finish) -> Self {
        Self {
            signature: Signature::exact(vec![element.clone()], Volatility::Immutable),
            list: DataType::List(Arc::new(Field::new("item", element, true))),
            finish,
        }
    }

    fn item(&self) -> Result<FieldRef> {
        match &self.list {
            DataType::List(item) => Ok(Arc::clone(item)),
            other => Err(DataFusionError::Internal(format!(
                "`collect` returns a list, declared {other}"
            ))),
        }
    }
}

impl AggregateUDFImpl for CollectUdaf {
    fn name(&self) -> &str {
        self.finish.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.finish {
            Finish::List => self.list.clone(),
            Finish::Stddev | Finish::Percentile(_) => DataType::Float64,
        })
    }

    /// `collect` is never null (empty input is the empty list); `stddev` and
    /// `percentile` are null over too few values.
    fn is_nullable(&self) -> bool {
        self.finish != Finish::List
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[values]", args.name),
            self.list.clone(),
            true,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CollectAccumulator {
            item: self.item()?,
            distinct: args.is_distinct,
            finish: self.finish,
            pieces: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct CollectAccumulator {
    item: FieldRef,
    distinct: bool,
    finish: Finish,
    /// The non-null values seen so far, each piece an owned copy, so its
    /// memory is exactly what the accumulator holds.
    pieces: Vec<ArrayRef>,
}

impl CollectAccumulator {
    /// Keep the non-null values of `values` as an owned copy.
    fn absorb(&mut self, values: &ArrayRef) -> Result<()> {
        let present = datafusion::arrow::compute::is_not_null(values.as_ref())?;
        let kept = arrow_select::filter::filter(values.as_ref(), &present)?;
        if !kept.is_empty() {
            self.pieces.push(kept);
        }
        if self.pieces.len() > PIECES_BEFORE_FOLD {
            self.fold()?;
        }
        Ok(())
    }

    /// Fold the pieces into one array in the fixed value order, without
    /// repeats under `distinct`.
    fn fold(&mut self) -> Result<()> {
        let values = match self.pieces.as_slice() {
            [] => return Ok(()),
            [one] => Arc::clone(one),
            many => {
                let parts: Vec<&dyn Array> = many.iter().map(|p| p.as_ref()).collect();
                arrow_select::concat::concat(&parts)?
            }
        };
        self.pieces = vec![ordered(&values, self.distinct)?];
        Ok(())
    }

    fn list(&mut self) -> Result<ScalarValue> {
        self.fold()?;
        let values = match self.pieces.first() {
            Some(values) => Arc::clone(values),
            None => arrow_array::new_empty_array(self.item.data_type()),
        };
        let list = ListArray::try_new(
            Arc::clone(&self.item),
            OffsetBuffer::from_lengths([values.len()]),
            values,
            None,
        )?;
        Ok(ScalarValue::List(Arc::new(list)))
    }

    /// The values held, ascending in the fixed value order, each as the
    /// nearest `F64`.
    fn sorted_f64(&mut self) -> Result<Vec<f64>> {
        self.fold()?;
        let Some(values) = self.pieces.first() else {
            return Ok(Vec::new());
        };
        let floats = arrow_cast::cast::cast(values.as_ref(), &DataType::Float64)?;
        let floats = floats
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("a numeric value is not an F64".into()))?;
        Ok(floats.values().to_vec())
    }

    /// The accumulator's result: `collect`'s list, or the value `stddev` or
    /// `percentile` makes of the held values.
    fn finish(&mut self) -> Result<ScalarValue> {
        Ok(match self.finish {
            Finish::List => return self.list(),
            Finish::Stddev => ScalarValue::Float64(sample_stddev(&self.sorted_f64()?)),
            Finish::Percentile(bits) => ScalarValue::Float64(interpolated_percentile(
                &self.sorted_f64()?,
                f64::from_bits(bits),
            )),
        })
    }
}

/// The sample standard deviation of `values`, ascending in the fixed value
/// order: the mean is their sum, added in that order, over n, and the result
/// the square root of the squared differences from the mean, added in that
/// order, over n − 1. `None` below two values.
fn sample_stddev(values: &[f64]) -> Option<f64> {
    let n = values.len();
    if n < 2 {
        return None;
    }
    let mean = values.iter().fold(0.0, |sum, v| sum + v) / n as f64;
    let squares = values.iter().fold(0.0, |sum, v| {
        let d = v - mean;
        sum + d * d
    });
    Some((squares / (n - 1) as f64).sqrt())
}

/// The percentile at `p` in [0, 1] of `values`, ascending in the fixed value
/// order, with linear interpolation: with the index i = p · (n − 1), the
/// value at ⌊i⌋ when i is whole, otherwise lower + (upper − lower) · (i −
/// ⌊i⌋). `None` over no value.
fn interpolated_percentile(values: &[f64], p: f64) -> Option<f64> {
    let last = values.len().checked_sub(1)?;
    let index = p * last as f64;
    let below = index.floor();
    let fraction = index - below;
    // `p` lies in [0, 1], so `index` lies in [0, last].
    let at = (below as usize).min(last);
    let lower = values[at];
    if fraction == 0.0 {
        return Some(lower);
    }
    let upper = values[(at + 1).min(last)];
    Some(lower + (upper - lower) * fraction)
}

/// `values` sorted in the fixed value order (the total order on floats, so
/// `-0.0` before `0.0`), with repeats dropped when `distinct`: two values are
/// one when the order ties them.
fn ordered(values: &ArrayRef, distinct: bool) -> Result<ArrayRef> {
    let options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let order = arrow_ord::sort::sort_to_indices(values.as_ref(), Some(options), None)?;
    let sorted = arrow_select::take::take(values.as_ref(), &order, None)?;
    if !distinct || sorted.len() < 2 {
        return Ok(sorted);
    }
    let compare = arrow_ord::ord::make_comparator(sorted.as_ref(), sorted.as_ref(), options)?;
    let kept: UInt32Array = (0..sorted.len())
        .filter(|&i| i == 0 || compare(i - 1, i) != std::cmp::Ordering::Equal)
        .map(|i| i as u32)
        .collect();
    Ok(arrow_select::take::take(sorted.as_ref(), &kept, None)?)
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.absorb(&values[0])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.finish()
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.pieces.capacity() * std::mem::size_of::<ArrayRef>()
            + self
                .pieces
                .iter()
                .map(|piece| piece.get_array_memory_size())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.list()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("`collect` state is a list".into()))?;
        for row in 0..lists.len() {
            if lists.is_valid(row) {
                // `value` is a slice of the state; `absorb` keeps a copy.
                self.absorb(&lists.value(row))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::Schema;
    use datafusion::physical_expr::{PhysicalExpr, expressions::Column};

    fn run(values: ArrayRef, distinct: bool) -> ScalarValue {
        let element = values.data_type().clone();
        let result = DataType::List(Arc::new(Field::new("item", element.clone(), true)));
        let udaf = CollectUdaf::new(element.clone(), result.clone());
        let input = Arc::new(Field::new("x", element, true));
        let schema = Schema::new(vec![input.clone()]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("x", 0))];
        let fields = [input];
        let args = || AccumulatorArgs {
            return_field: Arc::new(Field::new("c", result.clone(), true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "c",
            is_distinct: distinct,
            exprs: &exprs,
            expr_fields: &fields,
        };
        // Two partial accumulators merged, the second over the first's state.
        let mut partial = udaf.accumulator(args()).unwrap();
        partial.update_batch(&[values.slice(0, 2)]).unwrap();
        let state = partial.state().unwrap();
        let mut merged = udaf.accumulator(args()).unwrap();
        merged
            .merge_batch(&[state[0].to_array_of_size(1).unwrap()])
            .unwrap();
        merged
            .update_batch(&[values.slice(2, values.len() - 2)])
            .unwrap();
        let held = values.null_count() < values.len();
        assert_eq!(
            merged.size() > std::mem::size_of::<CollectAccumulator>(),
            held,
            "the accumulator charges exactly the values it holds"
        );
        merged.evaluate().unwrap()
    }

    fn items(list: ScalarValue) -> ArrayRef {
        let ScalarValue::List(list) = list else {
            panic!("collect returns a list");
        };
        assert_eq!(list.len(), 1);
        list.value(0)
    }

    #[test]
    fn collects_non_null_values_in_the_fixed_order() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("red"),
            None,
            Some("blue"),
            Some("red"),
        ]));
        let all = items(run(Arc::clone(&values), false));
        assert_eq!(
            all.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["blue", "red", "red"])
        );
        let distinct = items(run(values, true));
        assert_eq!(
            distinct.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["blue", "red"])
        );
    }

    #[test]
    fn floats_follow_the_total_order_and_signed_zeros_stay_distinct() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, -0.0, 2.5, -0.0, -1.5]));
        let distinct = items(run(values, true));
        let distinct = distinct.as_any().downcast_ref::<Float64Array>().unwrap();
        let bits: Vec<u64> = distinct.values().iter().map(|v| v.to_bits()).collect();
        let expected: Vec<u64> = [-1.5f64, -0.0, 0.0, 2.5]
            .iter()
            .map(|v| v.to_bits())
            .collect();
        assert_eq!(bits, expected);
    }

    /// `finish` over `values`, split at `at` into two partial accumulators
    /// merged in either order: the result must not depend on the split.
    fn finished(values: ArrayRef, finish: Finish, at: usize) -> Option<f64> {
        let element = values.data_type().clone();
        let udaf = CollectUdaf::finishing(element.clone(), finish);
        let input = Arc::new(Field::new("x", element, true));
        let schema = Schema::new(vec![input.clone()]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("x", 0))];
        let fields = [input];
        let args = || AccumulatorArgs {
            return_field: Arc::new(Field::new("c", DataType::Float64, true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "c",
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &fields,
        };
        let mut results = Vec::new();
        let (first, second) = (values.slice(0, at), values.slice(at, values.len() - at));
        for (early, late) in [(&first, &second), (&second, &first)] {
            let mut partial = udaf.accumulator(args()).unwrap();
            partial.update_batch(&[Arc::clone(early)]).unwrap();
            let state = partial.state().unwrap();
            let mut merged = udaf.accumulator(args()).unwrap();
            merged.update_batch(&[Arc::clone(late)]).unwrap();
            merged
                .merge_batch(&[state[0].to_array_of_size(1).unwrap()])
                .unwrap();
            let ScalarValue::Float64(value) = merged.evaluate().unwrap() else {
                panic!("an F64 result");
            };
            results.push(value.map(f64::to_bits));
        }
        assert_eq!(
            results[0], results[1],
            "the result depends on arrival order"
        );
        results[0].map(f64::from_bits)
    }

    fn percentile(p: f64) -> Finish {
        Finish::Percentile(p.to_bits())
    }

    #[test]
    fn stddev_is_the_sample_deviation_whatever_the_arrival_order() {
        let values: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![
            Some(9),
            Some(4),
            None,
            Some(2),
            Some(5),
            Some(4),
            Some(7),
            Some(4),
            Some(5),
        ]));
        // [2, 4, 4, 4, 5, 5, 7, 9]: squared differences from 5 sum to 32.
        let sd = finished(Arc::clone(&values), Finish::Stddev, 3).unwrap();
        assert_eq!(sd.to_bits(), (32.0f64 / 7.0).sqrt().to_bits());
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 0.7, 1e9, 0.2, -3.3]));
        let at_one = finished(Arc::clone(&floats), Finish::Stddev, 1);
        let at_four = finished(floats, Finish::Stddev, 4);
        assert_eq!(at_one.map(f64::to_bits), at_four.map(f64::to_bits));
        let one: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![Some(3), None]));
        assert_eq!(finished(one, Finish::Stddev, 1), None, "below two values");
    }

    #[test]
    fn percentile_interpolates_exactly_between_neighbors() {
        let values: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
            Some(4),
            None,
            Some(2),
            Some(9),
        ]));
        // [2, 4, 9]: the index p · 2.
        assert_eq!(finished(Arc::clone(&values), percentile(0.0), 2), Some(2.0));
        assert_eq!(finished(Arc::clone(&values), percentile(0.5), 2), Some(4.0));
        assert_eq!(finished(Arc::clone(&values), percentile(1.0), 2), Some(9.0));
        assert_eq!(
            finished(Arc::clone(&values), percentile(0.75), 1),
            Some(6.5)
        );
        // The fraction keeps every digit (DataFusion's `percentile_cont`
        // rounds it to 1e-6).
        let p = 0.123456789;
        let index = p * 2.0;
        assert_eq!(
            finished(values, percentile(p), 1).map(f64::to_bits),
            Some((2.0 + 2.0 * index).to_bits())
        );
        // An integer beyond 2^53 is its nearest `F64`; -0.0 sorts before 0.0.
        let wide: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![(1i64 << 53) + 1]));
        assert_eq!(
            finished(wide, percentile(1.0), 1),
            Some(9_007_199_254_740_992.0)
        );
        let zeros: ArrayRef = Arc::new(Float64Array::from(vec![0.0, -0.0]));
        assert_eq!(
            finished(zeros, percentile(0.0), 1).map(f64::to_bits),
            Some((-0.0f64).to_bits())
        );
        let none: ArrayRef = Arc::new(Float64Array::from(vec![None, None]));
        assert_eq!(finished(none, percentile(0.5), 1), None);
    }

    #[test]
    fn empty_input_is_the_empty_list() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None, None]));
        assert_eq!(items(run(values, false)).len(), 0);
    }
}
