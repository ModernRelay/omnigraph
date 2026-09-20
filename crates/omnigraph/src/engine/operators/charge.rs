//! Account for newly allocated join, expression and aggregate output buffers.
//! Shared buffers keep their existing query-pool charge.

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

use super::memory::WorkMemory;

pub(crate) struct ChargeExec {
    input: Arc<dyn ExecutionPlan>,
    name: &'static str,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ChargeExec {
    /// `name` is the refusal name the pool reports when a batch does not fit.
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, name: &'static str) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            name,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for ChargeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChargeExec")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ChargeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChargeExec: {}", self.name)
    }
}

impl ExecutionPlan for ChargeExec {
    fn name(&self) -> &str {
        "ChargeExec"
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
        assert_eq!(children.len(), 1, "ChargeExec has one child");
        let input = children.pop().expect("one child");
        Ok(Arc::new(Self::new(input, self.name)))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ChargeExec has one partition");
        let schema = self.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let memory = WorkMemory::new(ctx, self.name)?;
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let name = self.name;
        let stream = futures::stream::try_unfold(
            (input, memory, None::<WorkMemory>),
            move |(mut input, memory, held)| {
                let baseline = baseline.clone();
                async move {
                    drop(held);
                    match input.next().await {
                        Some(Ok(batch)) => {
                            let lease = memory.child(name)?;
                            lease.hold(&batch)?;
                            baseline.record_output(batch.num_rows());
                            Ok(Some((batch, (input, memory, Some(lease)))))
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
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
