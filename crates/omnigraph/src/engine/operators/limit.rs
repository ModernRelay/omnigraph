//! `LimitExec`: the rows a `Limit` node admits. A limit of zero executes
//! nothing below it, so the plan's `limit 0` is a run of one operator.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::{empty_stream, polled, streaming_properties};

#[derive(Debug)]
pub(crate) struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    rows: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl LimitExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, rows: usize) -> Self {
        let properties = streaming_properties(input.schema());
        Self {
            input,
            rows,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for LimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LimitExec: rows={}", self.rows)
    }
}

impl ExecutionPlan for LimitExec {
    fn name(&self) -> &str {
        "LimitExec"
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
        assert_eq!(children.len(), 1, "LimitExec has one child");
        Ok(Arc::new(Self::new(
            children.pop().expect("one child"),
            self.rows,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn fetch(&self) -> Option<usize> {
        Some(self.rows)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "LimitExec has one partition");
        if self.rows == 0 {
            return Ok(polled(&self.metrics, empty_stream(self.schema())));
        }
        let input = self.input.execute(0, ctx)?;
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let stream =
            futures::stream::try_unfold((input, self.rows), move |(mut input, remaining)| {
                let baseline = baseline.clone();
                async move {
                    if remaining == 0 {
                        baseline.done();
                        return Ok(None);
                    }
                    match input.next().await {
                        Some(Ok(batch)) => {
                            let taken = batch.num_rows().min(remaining);
                            let batch = if taken < batch.num_rows() {
                                batch.slice(0, taken)
                            } else {
                                batch
                            };
                            baseline.record_output(taken);
                            Ok(Some((batch, (input, remaining - taken))))
                        }
                        Some(Err(error)) => Err(error),
                        None => {
                            baseline.done();
                            Ok(None)
                        }
                    }
                }
            });
        Ok(polled(
            &self.metrics,
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream)),
        ))
    }
}
