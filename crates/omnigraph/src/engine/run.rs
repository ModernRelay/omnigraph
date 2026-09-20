//! Executes a lowered read plan under its query context and collects the
//! result into one batch, charging retained buffers to the query's pool.

use super::operators::memory::WorkMemory;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

use super::context::QueryContext;
use super::*;

/// Every batch of the root's one partition, concatenated.
pub(super) async fn run_plan(
    root: &Arc<dyn ExecutionPlan>,
    ctx: &QueryContext,
) -> Result<RecordBatch> {
    let task_ctx = ctx.task_ctx();
    let reservation = WorkMemory::new(Arc::clone(&task_ctx), "engine::run collect")
        .map_err(|error| ctx.classify(error))?;
    let mut stream = root
        .execute(0, task_ctx)
        .map_err(|error| ctx.classify(error))?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| ctx.classify(error))?;
        reservation
            .hold(&batch)
            .map_err(|error| ctx.classify(error))?;
        reservation
            .entries::<RecordBatch>(1)
            .map_err(|error| ctx.classify(error))?;
        batches.push(batch);
    }
    crate::instrumentation::record_query_execution_metrics(root);
    reservation
        .concat(&root.schema(), &batches)
        .map_err(|error| ctx.classify(error))
}
