//! `ProjectionExec`: the return expressions of a `Projection` node over the
//! wide batch, each output batch charged to the query pool before it leaves.
//! When a `Sort` consumes the projection, the lowering adds the sort's
//! columns under the hidden prefix; the sort drops them.

use std::fmt;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::memory::WorkMemory;
use super::{polled, streaming_properties};

/// The refusal name the pool reports when an output batch does not fit.
const OUTPUT: &str = "projection output";

pub(crate) struct ProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ProjectionExec {
    pub(crate) fn try_new(
        exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Self> {
        let input_schema = input.schema();
        let fields = exprs
            .iter()
            .map(|(expr, name)| {
                let field = expr.return_field(&input_schema)?;
                Ok(Field::new(
                    name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            })
            .collect::<DfResult<Vec<Field>>>()?;
        let properties = streaming_properties(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            exprs,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for ProjectionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionExec")
            .field("exprs", &self.exprs)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ProjectionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exprs: Vec<String> = self
            .exprs
            .iter()
            .map(|(expr, name)| format!("{expr} as {name}"))
            .collect();
        write!(f, "ProjectionExec: expr=[{}]", exprs.join(", "))
    }
}

impl ExecutionPlan for ProjectionExec {
    fn name(&self) -> &str {
        "ProjectionExec"
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
        assert_eq!(children.len(), 1, "ProjectionExec has one child");
        Ok(Arc::new(Self::try_new(
            self.exprs.clone(),
            children.pop().expect("one child"),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ProjectionExec has one partition");
        let schema: SchemaRef = self.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let memory = WorkMemory::new(ctx, OUTPUT)?;
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let exprs = self.exprs.clone();
        let output_schema = Arc::clone(&schema);
        let stream = futures::stream::try_unfold(
            (input, memory, None::<WorkMemory>),
            move |(mut input, memory, held)| {
                let baseline = baseline.clone();
                let exprs = exprs.clone();
                let schema = Arc::clone(&output_schema);
                async move {
                    drop(held);
                    match input.next().await {
                        Some(Ok(batch)) => {
                            let rows = batch.num_rows();
                            let columns = exprs
                                .iter()
                                .map(|(expr, _)| expr.evaluate(&batch)?.into_array(rows))
                                .collect::<DfResult<Vec<_>>>()?;
                            let output = RecordBatch::try_new_with_options(
                                schema,
                                columns,
                                &RecordBatchOptions::new().with_row_count(Some(rows)),
                            )?;
                            let lease = memory.child(OUTPUT)?;
                            lease.hold(&output)?;
                            baseline.record_output(rows);
                            Ok(Some((output, (input, memory, Some(lease)))))
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
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
        ))
    }
}
