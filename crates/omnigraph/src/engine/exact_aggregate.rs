use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};

pub(super) const EXACT_CARRIER: DataType =
    omnigraph_compiler::types::ExprType::exact_integer_arrow();

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ExactIntegerUdaf {
    inner: Arc<AggregateUDF>,
    signature: Signature,
    result: DataType,
}

impl ExactIntegerUdaf {
    pub(super) fn new(result: DataType) -> Self {
        Self {
            inner: sum_udaf(),
            signature: Signature::exact(vec![EXACT_CARRIER], Volatility::Immutable),
            result,
        }
    }
}

fn carrier_field(field: &Field) -> FieldRef {
    Arc::new(field.clone().with_data_type(EXACT_CARRIER))
}

impl AggregateUDFImpl for ExactIntegerUdaf {
    fn name(&self) -> &str {
        "exact_integer_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.result.clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(StateFieldsArgs {
            return_field: carrier_field(&args.return_field),
            ..args
        })
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let inner = self.inner.accumulator(AccumulatorArgs {
            return_field: carrier_field(&args.return_field),
            ..args
        })?;
        Ok(Box::new(ExactAccumulator {
            inner,
            result: self.result.clone(),
        }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(AccumulatorArgs {
            return_field: carrier_field(&args.return_field),
            ..args
        })
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let inner = self.inner.create_groups_accumulator(AccumulatorArgs {
            return_field: carrier_field(&args.return_field),
            ..args
        })?;
        Ok(Box::new(ExactGroupsAccumulator {
            inner,
            result: self.result.clone(),
        }))
    }
}

#[derive(Debug)]
struct ExactAccumulator {
    inner: Box<dyn Accumulator>,
    result: DataType,
}

impl Accumulator for ExactAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()?.cast_to(&self.result)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
}

struct ExactGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    result: DataType,
}

impl GroupsAccumulator for ExactGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.inner.update_batch(values, groups, filter, total)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(arrow_cast::cast(
            &self.inner.evaluate(emit_to)?,
            &self.result,
        )?)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.inner.merge_batch(values, groups, filter, total)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array, Float64Array};
    use arrow_schema::Schema;
    use datafusion::physical_expr::{PhysicalExpr, expressions::Column};

    fn values(values: Vec<Option<i128>>) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        )
    }

    #[test]
    fn partial_states_preserve_exact_totals_and_null_groups() {
        let udaf = ExactIntegerUdaf::new(DataType::Float64);
        let input = Arc::new(Field::new("amount", EXACT_CARRIER, true));
        let result = Arc::new(Field::new("total", DataType::Float64, true));
        let schema = Schema::new(vec![input.clone()]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("amount", 0))];
        let fields = [input];
        let args = || AccumulatorArgs {
            return_field: result.clone(),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "total",
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &fields,
        };
        let states = udaf
            .state_fields(StateFieldsArgs {
                name: "total",
                input_fields: &fields,
                return_field: result.clone(),
                ordering_fields: &[],
                is_distinct: false,
            })
            .unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].data_type(), &EXACT_CARRIER);

        let mut partial = udaf.accumulator(args()).unwrap();
        partial
            .update_batch(&[values(vec![Some(9007199254740993)])])
            .unwrap();
        let state = partial.state().unwrap();
        assert_eq!(
            state,
            vec![ScalarValue::Decimal128(Some(9007199254740993), 38, 0)]
        );
        let mut merged = udaf.accumulator(args()).unwrap();
        merged
            .merge_batch(&[state[0].to_array_of_size(1).unwrap()])
            .unwrap();
        merged
            .update_batch(&[values(vec![Some(-9007199254740992)])])
            .unwrap();
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::Float64(Some(1.0)));

        assert!(udaf.groups_accumulator_supported(args()));
        let mut partial = udaf.create_groups_accumulator(args()).unwrap();
        partial
            .update_batch(
                &[values(vec![Some(9007199254740993), None])],
                &[0, 1],
                None,
                2,
            )
            .unwrap();
        let state = partial.state(EmitTo::All).unwrap();
        assert_eq!(state[0].data_type(), &EXACT_CARRIER);
        let mut merged = udaf.create_groups_accumulator(args()).unwrap();
        merged.merge_batch(&state, &[0, 1], None, 2).unwrap();
        merged
            .update_batch(&[values(vec![Some(-9007199254740992)])], &[0], None, 2)
            .unwrap();
        let result = merged.evaluate(EmitTo::All).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.value(0), 1.0);
        assert!(result.is_null(1));
    }
}
