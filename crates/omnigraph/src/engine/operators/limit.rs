//! `LimitExec`: the rows a `Limit` node admits. A limit of zero executes
//! nothing below it, so the plan's `limit 0` is a run of one operator,
//! unless an expression below it can fail: then it pulls its input to the
//! end and keeps no row, so a failing row refuses the read as it would
//! under any other limit (A3).

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::shape::{Mapped, Streaming, drained, empty};

pub(crate) struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    rows: usize,
    /// A limit of zero pulls its input to the end: an expression below it
    /// can fail.
    drains: bool,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl LimitExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, rows: usize, drains: bool) -> Self {
        let shape = Streaming::new(input.schema());
        Self {
            input,
            rows,
            drains,
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for LimitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LimitExec")
            .field("rows", &self.rows)
            .field("drains", &self.drains)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LimitExec: rows={}", self.rows)?;
        if self.rows == 0 && self.drains {
            f.write_str(", drains")?;
        }
        Ok(())
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
        self.shape.properties()
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
            self.drains,
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
            if self.drains {
                let input = self.input.execute(0, ctx)?;
                return Ok(drained(&self.metrics, self.schema(), input));
            }
            return Ok(empty(&self.metrics, self.schema()));
        }
        let input = self.input.execute(0, ctx)?;
        let mut remaining = self.rows;
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let taken = batch.num_rows().min(remaining);
            let batch = if taken < batch.num_rows() {
                batch.slice(0, taken)
            } else {
                batch
            };
            remaining -= taken;
            Ok(if remaining == 0 {
                Mapped::Last(batch)
            } else {
                Mapped::Batch { batch, lease: None }
            })
        }))
    }
}
