//! Executes a lowered read plan under its query context and collects the
//! result into one batch, charging retained buffers to the query's pool. After
//! a pass, walks the plan's nodes in lockstep with their operators and reads
//! each operator's metrics into that pass's report rows.

use super::operators::memory::WorkMemory;
use super::operators::{Switch, was_drained, was_polled};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use omnigraph_planner::PhysicalPlan;

use super::context::QueryContext;
use super::lower::{Lowered, in_order, return_order};
use super::report::{Ran, ReportRow};
use super::*;

/// Every batch of the root's one partition, concatenated, its columns in the
/// return's order.
pub(super) async fn run_plan(
    lowered: &Lowered,
    plan: &PhysicalPlan,
    ctx: &QueryContext,
) -> Result<RecordBatch> {
    let root = &lowered.root;
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
    let batch = reservation
        .concat(&root.schema(), &batches)
        .map_err(|error| ctx.classify(error))?;
    match return_order(plan)? {
        Some(names) => in_order(batch, &names),
        None => Ok(batch),
    }
}

/// The rows of pass `rung`: one per live node of `plan` in post-order, from
/// the node's one operator. Refuses a lowered tree whose root or shape is not
/// the plan's.
pub(super) fn pass_rows(
    lowered: &Lowered,
    plan: &PhysicalPlan,
    rung: usize,
) -> Result<Vec<ReportRow>> {
    let operator = |id: NodeId| -> Result<&Arc<dyn ExecutionPlan>> {
        lowered.operators.get(&id).ok_or_else(|| {
            OmniError::manifest_internal(format!("plan node {id} built no operator"))
        })
    };
    if !Arc::ptr_eq(&lowered.root, operator(plan.root())?) {
        return Err(OmniError::manifest_internal(
            "the lowered root is not the plan root's operator".to_string(),
        ));
    }
    let mut rows = Vec::new();
    for id in plan.post_order() {
        let node = plan.node(id).ok_or_else(|| {
            OmniError::manifest_internal(format!("plan node {id} is a tombstone"))
        })?;
        let op = operator(id)?;
        let inputs = node.inputs();
        let children = op.children();
        let same_shape = inputs.len() == children.len()
            && inputs
                .iter()
                .zip(children)
                .all(|(input, child)| operator(*input).is_ok_and(|op| Arc::ptr_eq(op, child)));
        if !same_shape {
            return Err(OmniError::manifest_internal(format!(
                "the lowered tree does not have the plan's shape at node {id} ({})",
                node.name()
            )));
        }
        let metrics = op.metrics().unwrap_or_else(MetricsSet::new);
        let polled = was_polled(&metrics);
        let ran = match Switch::read(&metrics) {
            Some(side) if polled => Ran::Took(side),
            _ => Ran::Polled(polled),
        };
        let actual_rows = metrics.output_rows().unwrap_or(0) as u64;
        let drained = was_drained(&metrics).map(|drained| polled && drained);
        rows.push(ReportRow::new(
            id,
            op.name(),
            rung,
            ran,
            actual_rows,
            drained,
        ));
    }
    Ok(rows)
}
