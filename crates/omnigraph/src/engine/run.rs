//! Executes a lowered read plan under its query context and collects the
//! result into one batch, charging retained buffers to the query's pool. After
//! a pass, walks the plan's nodes in lockstep with their operators and reads
//! each operator's metrics into that pass's report rows.

use super::operators::memory::WorkMemory;
use super::operators::{CsvSourceExec, Switch, was_drained, was_polled};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use futures::StreamExt;
use omnigraph_planner::PhysicalPlan;

use super::context::QueryContext;
use super::lower::{Lowered, in_order, return_order};
use super::report::{Ran, ReportRow};
use super::*;
use crate::loader::LoadOperatorExecuted;

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
    let batch = match return_order(plan) {
        Some(names) => in_order(batch, &names)?,
        None => batch,
    };
    let declared = plan
        .properties(plan.root())
        .ok_or_else(|| OmniError::manifest_internal("root has no declared schema"))?;
    let schema = batch.schema();
    if schema.fields().len() != declared.schema.fields().len()
        || schema
            .fields()
            .iter()
            .zip(declared.schema.fields())
            .any(|(actual, expected)| {
                actual.name() != expected.name() || actual.data_type() != expected.data_type()
            })
    {
        return Err(OmniError::manifest_internal(
            "root names, order, count or types differ from declared schema",
        ));
    }
    for field in declared.schema.fields() {
        if !field.is_nullable()
            && batch
                .column_by_name(field.name())
                .is_some_and(|column| column.null_count() != 0)
        {
            return Err(OmniError::manifest_internal(format!(
                "root column {} contains undeclared nulls",
                field.name()
            )));
        }
    }
    Ok(batch)
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

/// What each node of a load plan did, in post-order, from the one operator
/// the node built: its rows, its compute time and its wall span.
pub(super) fn load_operator_rows(
    lowered: &Lowered,
    plan: &PhysicalPlan,
) -> Result<Vec<LoadOperatorExecuted>> {
    let mut rows = Vec::new();
    for id in plan.post_order() {
        let node = plan.node(id).ok_or_else(|| {
            OmniError::manifest_internal(format!("plan node {id} is a tombstone"))
        })?;
        let op = lowered.operators.get(&id).ok_or_else(|| {
            OmniError::manifest_internal(format!("plan node {id} built no operator"))
        })?;
        let metrics = op.metrics().unwrap_or_else(MetricsSet::new);
        let source = op.downcast_ref::<CsvSourceExec>();
        rows.push(LoadOperatorExecuted {
            id: id as u64,
            node: node.name().to_string(),
            operator: op.name().to_string(),
            rows: metrics.output_rows().unwrap_or(0) as u64,
            elapsed_compute_ns: metrics.elapsed_compute().unwrap_or(0) as u64,
            wall_ns: wall_ns(&metrics),
            partitions: source.map_or(1, |source| {
                u32::try_from(source.partitions()).unwrap_or(u32::MAX)
            }),
            slices: source
                .map(CsvSourceExec::executed_slices)
                .unwrap_or_default(),
        });
    }
    Ok(rows)
}

/// The span from the earliest start timestamp to the latest end timestamp
/// among `metrics`, `0` without both.
fn wall_ns(metrics: &MetricsSet) -> u64 {
    let mut start = None;
    let mut end = None;
    for metric in metrics.iter() {
        match metric.value() {
            MetricValue::StartTimestamp(stamp) => {
                if let Some(at) = stamp.value() {
                    start = Some(start.map_or(at, |first| at.min(first)));
                }
            }
            MetricValue::EndTimestamp(stamp) => {
                if let Some(at) = stamp.value() {
                    end = Some(end.map_or(at, |last| at.max(last)));
                }
            }
            _ => {}
        }
    }
    match (start, end) {
        (Some(start), Some(end)) => end
            .signed_duration_since(start)
            .num_nanoseconds()
            .and_then(|nanos| u64::try_from(nanos).ok())
            .unwrap_or(0),
        _ => 0,
    }
}
