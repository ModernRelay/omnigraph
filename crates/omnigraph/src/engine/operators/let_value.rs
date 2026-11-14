//! `LetExec`: a `let` whose value can fail, the core's `Project` adding one
//! column (docs/rfcs/2026-11-07-gq-3.md, section 3). The value is evaluated
//! on every input row, so a failure (`E1`, `E2`, `E3`) refuses the read on
//! the rows the elements before the `let` produce, whatever `where` later
//! removes (A3); every row passes through unchanged, in input order. The
//! value is pure, so a later read of the name evaluates it in place to the
//! same result on the rows that reach it.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::ir::{IRExpr, ParamMap};

use super::external;
use super::shape::{Mapped, Streaming};
use crate::engine::expr::evaluate_expr;

pub(crate) struct LetExec {
    input: Arc<dyn ExecutionPlan>,
    name: String,
    value: IRExpr,
    params: Arc<ParamMap>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl LetExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        name: String,
        value: IRExpr,
        params: Arc<ParamMap>,
    ) -> Self {
        let shape = Streaming::new(input.schema());
        Self {
            input,
            name,
            value,
            params,
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for LetExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LetExec")
            .field("name", &self.name)
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LetExec: ${} = {}", self.name, self.value)
    }
}

impl ExecutionPlan for LetExec {
    fn name(&self) -> &str {
        "LetExec"
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
        assert_eq!(children.len(), 1, "LetExec has one child");
        Ok(Arc::new(Self::new(
            children.pop().expect("one child"),
            self.name.clone(),
            self.value.clone(),
            Arc::clone(&self.params),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "LetExec has one partition");
        let input = self.input.execute(0, ctx)?;
        let value = self.value.clone();
        let params = Arc::clone(&self.params);
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            evaluate_expr(&batch, &value, &params).map_err(external)?;
            Ok(Mapped::Batch { batch, lease: None })
        }))
    }
}
