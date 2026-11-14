//! `FilterExec`: the in-memory arm of a `Filter` node, every GQ filter it
//! holds evaluated over the wide batch and conjoined into one mask.

use std::fmt;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::ir::{IRExpr, ParamMap};

use super::external;
use super::memory::WorkMemory;
use super::shape::{Mapped, Streaming};
use crate::engine::expr::{evaluate_filter, evaluate_filter_where, kept_rows};
use crate::engine::lexical::LexicalColumns;

/// The refusal name the pool reports when a filtered batch does not fit.
const OUTPUT: &str = "filter output";

/// Every one of `filters` evaluated over `batch`, conjoined into one mask
/// (a null drops the row, as a false does); `None` when there is no filter.
/// Each filter after the first needs only the rows the earlier ones kept and
/// is evaluated as `evaluate_filter_where` describes: on those rows alone
/// when they are few, over the whole batch when they are many. The filters
/// read full-text calls as the membership columns `lexical` adds to the
/// batch while they are evaluated.
pub(super) fn conjoined_mask(
    batch: &RecordBatch,
    filters: &[IRExpr],
    params: &ParamMap,
    lexical: Option<&LexicalColumns>,
) -> DfResult<Option<BooleanArray>> {
    let widened;
    let batch = match lexical {
        Some(lexical) if !filters.is_empty() => {
            widened = lexical.widen(batch).map_err(external)?;
            &widened
        }
        _ => batch,
    };
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        mask = Some(match mask {
            None => evaluate_filter(batch, filter, params).map_err(external)?,
            Some(mask) => {
                let next = evaluate_filter_where(batch, filter, params, &kept_rows(&mask))
                    .map_err(external)?;
                datafusion::arrow::compute::and_kleene(&mask, &next)?
            }
        });
    }
    Ok(mask)
}

pub(crate) struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    lexical: Option<Arc<LexicalColumns>>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        filters: Vec<IRExpr>,
        params: Arc<ParamMap>,
        lexical: Option<Arc<LexicalColumns>>,
    ) -> Self {
        let shape = Streaming::new(input.schema());
        Self {
            input,
            filters,
            params,
            lexical,
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for FilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterExec")
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let filters: Vec<String> = self.filters.iter().map(ToString::to_string).collect();
        write!(f, "FilterExec: {}", filters.join(" AND "))
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &str {
        "FilterExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "FilterExec has one child");
        Ok(Arc::new(Self::new(
            children.pop().expect("one child"),
            self.filters.clone(),
            Arc::clone(&self.params),
            self.lexical.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "FilterExec has one partition");
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let memory = WorkMemory::new(ctx, OUTPUT)?;
        let filters = self.filters.clone();
        let params = Arc::clone(&self.params);
        let lexical = self.lexical.clone();
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let kept = match conjoined_mask(&batch, &filters, &params, lexical.as_deref())? {
                Some(mask) => arrow_select::filter::filter_record_batch(&batch, &mask)?,
                None => batch,
            };
            let lease = memory.child(OUTPUT)?;
            lease.hold(&kept)?;
            Ok(Mapped::Batch {
                batch: kept,
                lease: Some(lease),
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Float32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::query::value::{CompOp, Literal};
    use omnigraph_compiler::types::{ExprType, PropType, ScalarType};

    use super::*;

    fn ty(scalar: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(scalar, nullable))
    }

    fn prop(property: &str, scalar: ScalarType) -> IRExpr {
        IRExpr::PropAccess {
            variable: "a".to_string(),
            property: property.to_string(),
            ty: ty(scalar, true),
        }
    }

    /// A later conjunct of a `Filter` node runs only on the rows the earlier
    /// ones kept, when they are at most the pre-selection share: the two
    /// filters of `and_skips_the_rows_its_left_operand_removed_issue_1336`,
    /// as two conjuncts. Only row 1, which `keep = 0` removes, holds an F32
    /// infinity, so the cast to F64 that `measure > 25.0` records fails there
    /// and only there. Before pre-selection every conjunct ran over every
    /// row and the mask failed.
    #[test]
    fn later_conjunct_skips_the_rows_an_earlier_one_removed_issue_1336() {
        let rows = 100;
        let keep = Int64Array::from_iter_values((0..rows).map(|row| row % 10));
        let measure = Float32Array::from_iter_values(
            (0..rows).map(|row| if row == 1 { f32::INFINITY } else { row as f32 }),
        );
        let schema = Schema::new(vec![
            Field::new("a.keep", DataType::Int64, true),
            Field::new("a.measure", DataType::Float32, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(keep) as ArrayRef, Arc::new(measure) as ArrayRef],
        )
        .unwrap();
        let cheap = IRExpr::comparison(
            prop("keep", ScalarType::I64),
            CompOp::Eq,
            IRExpr::Literal(Literal::Integer(0), ty(ScalarType::I64, false)),
        );
        let expensive = IRExpr::comparison(
            IRExpr::Cast {
                expr: Box::new(prop("measure", ScalarType::F32)),
                ty: ty(ScalarType::F64, true),
            },
            CompOp::Gt,
            IRExpr::Literal(Literal::Float(25.0), ty(ScalarType::F64, false)),
        );
        let params = ParamMap::new();
        assert!(
            conjoined_mask(&batch, std::slice::from_ref(&expensive), &params, None).is_err(),
            "the infinite row fails the cast when it is evaluated"
        );

        let mask = conjoined_mask(&batch, &[cheap, expensive], &params, None)
            .unwrap()
            .unwrap();
        let kept: Vec<usize> = (0..rows as usize)
            .filter(|&row| mask.is_valid(row) && mask.value(row))
            .collect();
        assert_eq!(kept, vec![30, 40, 50, 60, 70, 80, 90]);
    }
}
