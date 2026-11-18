//! `collect`, the list aggregate: a group's non-null values as one list in
//! the fixed value order, `collect(distinct x)` without repeats, the empty
//! list over empty input. The values it holds are its accumulator's size,
//! which `AggregateExec` reserves in the query's memory pool as they arrive,
//! so a list beyond `query_memory_bytes` refuses the read.

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

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CollectUdaf {
    signature: Signature,
    /// The declared result, `List` of the argument's type.
    result: DataType,
}

impl CollectUdaf {
    /// `collect` over an argument of type `element`, returning `result`, a
    /// `List` whose items are `element`.
    pub(super) fn new(element: DataType, result: DataType) -> Self {
        Self {
            signature: Signature::exact(vec![element], Volatility::Immutable),
            result,
        }
    }

    fn item(&self) -> Result<FieldRef> {
        match &self.result {
            DataType::List(item) => Ok(Arc::clone(item)),
            other => Err(DataFusionError::Internal(format!(
                "`collect` returns a list, declared {other}"
            ))),
        }
    }
}

impl AggregateUDFImpl for CollectUdaf {
    fn name(&self) -> &str {
        "collect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.result.clone())
    }

    /// Never null: empty input is the empty list.
    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[values]", args.name),
            self.result.clone(),
            true,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CollectAccumulator {
            item: self.item()?,
            distinct: args.is_distinct,
            pieces: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct CollectAccumulator {
    item: FieldRef,
    distinct: bool,
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
        self.list()
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

    #[test]
    fn empty_input_is_the_empty_list() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None, None]));
        assert_eq!(items(run(values, false)).len(), 0);
    }
}
