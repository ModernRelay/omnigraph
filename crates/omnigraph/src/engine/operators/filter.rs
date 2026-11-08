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
use crate::engine::expr::evaluate_filter;

/// The refusal name the pool reports when a filtered batch does not fit.
const OUTPUT: &str = "filter output";

/// Every one of `filters` evaluated over `batch`, conjoined into one mask
/// (a null drops the row); `None` when there is no filter.
pub(super) fn conjoined_mask(
    batch: &RecordBatch,
    filters: &[IRExpr],
    params: &ParamMap,
) -> DfResult<Option<BooleanArray>> {
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        let next = evaluate_filter(batch, filter, params).map_err(external)?;
        mask = Some(match mask {
            None => next,
            Some(mask) => datafusion::arrow::compute::and(&mask, &next)?,
        });
    }
    Ok(mask)
}

pub(crate) struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        filters: Vec<IRExpr>,
        params: Arc<ParamMap>,
    ) -> Self {
        let shape = Streaming::new(input.schema());
        Self {
            input,
            filters,
            params,
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
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let kept = match conjoined_mask(&batch, &filters, &params)? {
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
