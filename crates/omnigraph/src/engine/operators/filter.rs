//! `FilterExec`: the in-memory arm of a `Filter` node, every GQ filter it
//! holds evaluated over the wide batch and conjoined into one mask.

use std::fmt;
use std::sync::Arc;

use arrow_array::BooleanArray;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use omnigraph_compiler::ir::{IRFilter, ParamMap};

use super::memory::WorkMemory;
use super::{external, polled, streaming_properties};
use crate::engine::expr::evaluate_filter;

/// The refusal name the pool reports when a filtered batch does not fit.
const OUTPUT: &str = "filter output";

pub(crate) struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    filters: Vec<IRFilter>,
    params: Arc<ParamMap>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        filters: Vec<IRFilter>,
        params: Arc<ParamMap>,
    ) -> Self {
        let properties = streaming_properties(input.schema());
        Self {
            input,
            filters,
            params,
            properties,
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
        &self.properties
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
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let filters = self.filters.clone();
        let params = Arc::clone(&self.params);
        let stream = futures::stream::try_unfold(
            (input, memory, None::<WorkMemory>),
            move |(mut input, memory, held)| {
                let baseline = baseline.clone();
                let filters = filters.clone();
                let params = Arc::clone(&params);
                async move {
                    drop(held);
                    match input.next().await {
                        Some(Ok(batch)) => {
                            let mut mask: Option<BooleanArray> = None;
                            for filter in &filters {
                                let next =
                                    evaluate_filter(&batch, filter, &params).map_err(external)?;
                                mask = Some(match mask {
                                    None => next,
                                    Some(mask) => datafusion::arrow::compute::and(&mask, &next)?,
                                });
                            }
                            let kept = match mask {
                                Some(mask) => {
                                    arrow_select::filter::filter_record_batch(&batch, &mask)?
                                }
                                None => batch,
                            };
                            let lease = memory.child(OUTPUT)?;
                            lease.hold(&kept)?;
                            baseline.record_output(kept.num_rows());
                            Ok(Some((kept, (input, memory, Some(lease)))))
                        }
                        Some(Err(error)) => Err(error),
                        None => {
                            baseline.done();
                            Ok(None)
                        }
                    }
                }
            },
        );
        Ok(polled(
            &self.metrics,
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream)),
        ))
    }
}
