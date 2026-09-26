//! `CrossJoinExec`: every pair of a `CrossJoin` node's two inputs. The left
//! input is collected under the query pool; each right batch is paired with
//! every left row, one output batch per left row, each charged before it
//! leaves. An empty left input executes nothing on the right.

use std::fmt;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::memory::WorkMemory;
use super::producer::producer_stream;
use super::{conform, external, joined_schema, polled, streaming_properties};
use crate::engine::scan::hconcat_batches;

/// The refusal name the pool reports when an output batch does not fit.
const OUTPUT: &str = "cross join output";

/// The left input as one held batch; `input_rows` counts the streamed right
/// side alone, whose batches bound the output (one left row per right batch).
async fn collect_left(
    mut left: SendableRecordBatchStream,
    schema: &SchemaRef,
    memory: &WorkMemory,
) -> DfResult<RecordBatch> {
    let held = memory.child("cross join left")?;
    let mut batches = Vec::new();
    while let Some(batch) = left.next().await {
        let batch = batch?;
        held.hold(&batch)?;
        held.entries::<RecordBatch>(1)?;
        batches.push(batch);
    }
    let batch = held.concat(schema, &batches)?;
    memory.hold(&batch)?;
    Ok(batch)
}

#[derive(Debug)]
pub(crate) struct CrossJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CrossJoinExec {
    pub(crate) fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> crate::error::Result<Self> {
        let schema = joined_schema(&left.schema(), &right.schema())?;
        Ok(Self {
            left,
            right,
            properties: streaming_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CrossJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CrossJoinExec")
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &str {
        "CrossJoinExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2, "CrossJoinExec has two children");
        let right = children.pop().expect("right child");
        let left = children.pop().expect("left child");
        Ok(Arc::new(Self::try_new(left, right).map_err(external)?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CrossJoinExec has one partition");
        let schema: SchemaRef = self.schema();
        let declared = Arc::clone(&schema);
        let left_schema = self.left.schema();
        let left = self.left.execute(0, Arc::clone(&ctx))?;
        let right = Arc::clone(&self.right);
        let mut work = WorkMemory::new(ctx, "CrossJoinExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        work.metric("input_batches", 0);
        let memory = Arc::new(work);
        let stream = producer_stream(
            schema,
            memory,
            Some(&self.metrics),
            move |memory, sender| async move {
                let left = collect_left(left, &left_schema, &memory).await?;
                if left.num_rows() == 0 {
                    return Ok(());
                }
                let mut right = right.execute(0, Arc::clone(&memory.ctx))?;
                while let Some(batch) = right.next().await {
                    let batch = batch?;
                    memory.metric("input_rows", batch.num_rows());
                    memory.metric("input_batches", 1);
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    for row in 0..left.num_rows() {
                        let work = Arc::new(memory.child(OUTPUT)?);
                        work.entries::<u32>(batch.num_rows())?;
                        let indices = UInt32Array::from(vec![row as u32; batch.num_rows()]);
                        let left_rows = work.take_once(&left, &indices, OUTPUT)?;
                        let output = hconcat_batches(&left_rows, &batch).map_err(external)?;
                        let output = conform(output, &declared).map_err(external)?;
                        work.hold(&output)?;
                        sender.send(output, work).await?;
                    }
                }
                Ok(())
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}
