//! Expand aligns source rows with destination IDs in bounded output batches.
//! A single unbound hop streams through `single_hop`; a bound edge runs the
//! pair producer and its sort per input batch; multi-hop drains its frontier
//! into the BFS breaker.

use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array, builder::StringBuilder};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use super::expand::{ExpandStep, GraphEnv};
use super::memory::WorkMemory;
use super::producer::{BatchSender, producer_stream};
use super::{Switch, drain_one, external};
use crate::engine::graph::{bound_edge_pair_schema, execute_expand, produce_bound_edge_pairs};

/// The most rows of one aligned output chunk, on all three strategies: the
/// source columns of a chunk are one `WorkMemory::take`, reserved at twice
/// the picked bytes, so the chunk bounds that reservation by row width.
pub(super) const EXPAND_OUTPUT_ROWS: usize = 256;

/// The `switch` gauge on `metrics` records the mode the traversal ends on,
/// one side of the `Expand` node's declared switch.
pub(super) fn execute(
    input: SendableRecordBatchStream,
    input_schema: SchemaRef,
    schema: SchemaRef,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    ctx: Arc<TaskContext>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    if step.single_hop() && step.edge_binding.is_none() {
        return super::single_hop::execute(input, schema, step, env, ctx, metrics);
    }
    let switch = Switch::gauge(metrics);
    let ctx = Arc::new(TaskContext::new(
        ctx.task_id(),
        ctx.session_id(),
        ctx.session_config().clone().with_batch_size(
            ctx.session_config()
                .batch_size()
                .clamp(1, EXPAND_OUTPUT_ROWS),
        ),
        ctx.scalar_functions().clone(),
        ctx.higher_order_functions().clone(),
        ctx.aggregate_functions().clone(),
        ctx.window_functions().clone(),
        ctx.runtime_env(),
    ));
    let mut work = WorkMemory::new(ctx, "ExpandExec")?;
    work.set_metrics(metrics.clone());
    work.metric("input_rows", 0);
    let memory = Arc::new(work);
    let declared = Arc::clone(&schema);
    Ok(producer_stream(
        schema,
        memory,
        Some(metrics),
        move |memory, sender| async move {
            if step.edge_binding.is_none() {
                let wide = Arc::new(drain_one(input, &input_schema, &memory).await?);
                if wide.num_rows() == 0 {
                    return Ok(());
                }
                return emit_unbound(&wide, &step, &env, &switch, &memory, &sender, &declared)
                    .await;
            }
            let mut input = input;
            while let Some(batch) = input.next().await {
                let batch = batch?;
                memory.metric("input_rows", batch.num_rows());
                if batch.num_rows() == 0 {
                    continue;
                }
                let input_memory = memory.child("expand input batch")?;
                input_memory.hold(&batch)?;
                let wide = Arc::new(batch);
                emit_bound(&wide, &step, &env, &memory, &sender, &declared).await?;
            }
            Ok(())
        },
    ))
}

/// One input batch's bound-edge pairs, sorted by (source row, edge id,
/// destination id) and hydrated in `EXPAND_OUTPUT_ROWS` chunks. A source row's
/// pairs never cross an input batch, so input order keeps one global sort.
async fn emit_bound(
    wide: &Arc<RecordBatch>,
    step: &ExpandStep,
    env: &Arc<GraphEnv>,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
    declared: &SchemaRef,
) -> Result<()> {
    let pair_schema = bound_edge_pair_schema(&env.catalog, &step.edge_type).map_err(external)?;
    let partition = Arc::new(EdgePairs {
        schema: Arc::clone(&pair_schema),
        wide: Arc::clone(wide),
        step: step.clone(),
        env: Arc::clone(env),
        memory: Arc::clone(memory),
    });
    let pairs: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&pair_schema),
        vec![partition],
        None,
        [],
        false,
        None,
    )?);
    let ordering = LexOrdering::new([0, 2, 1].map(|index| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(pair_schema.field(index).name(), index)),
            datafusion::arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            },
        )
    }))
    .expect("bound edge pairs have source, edge and destination keys");
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, pairs));
    let (sort, mut stream) = memory.stream(sort)?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let sorted = memory.child("sorted expand pairs")?;
        sorted.hold(&batch)?;
        let rows = output_rows(memory);
        for offset in (0..batch.num_rows()).step_by(rows) {
            let work = Arc::new(memory.child("expand output chunk")?);
            let pairs = batch.slice(offset, rows.min(batch.num_rows() - offset));
            let source = pairs
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("expand source ordinal is not UInt32".into())
                })?;
            let output = align_sources(
                wide,
                source,
                Arc::clone(pairs.column(1)),
                &pairs.columns()[2..],
                declared,
                &work,
            )?;
            sender.send_bounded(output, work).await?;
        }
    }
    crate::instrumentation::record_query_execution_metrics(&sort);
    Ok(())
}

async fn emit_unbound(
    wide: &RecordBatch,
    step: &ExpandStep,
    env: &GraphEnv,
    switch: &Gauge,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
    schema: &SchemaRef,
) -> Result<()> {
    let pairs = execute_expand(
        wide,
        &env.graph_index,
        &env.snapshot,
        &env.catalog,
        step,
        switch,
        memory,
    )
    .await
    .map_err(external)?;
    let rows = output_rows(memory);
    for offset in (0..pairs.source_rows.len()).step_by(rows) {
        let work = Arc::new(memory.child("expand output chunk")?);
        let end = (offset + rows).min(pairs.source_rows.len());
        work.entries::<u32>(end - offset)?;
        let source = UInt32Array::from(pairs.source_rows[offset..end].to_vec());
        let ids = &pairs.destination_ids[offset..end];
        let bytes = ids.iter().map(String::len).sum();
        work.string(bytes)?;
        let mut destination = StringBuilder::with_capacity(ids.len(), bytes);
        for id in ids {
            work.check()?;
            destination.append_value(id);
        }
        let output = align_sources(
            wide,
            &source,
            Arc::new(destination.finish()),
            &[],
            schema,
            &work,
        )?;
        sender.send_bounded(output, work).await?;
    }
    Ok(())
}

pub(super) fn output_rows(memory: &WorkMemory) -> usize {
    memory
        .ctx
        .session_config()
        .batch_size()
        .clamp(1, EXPAND_OUTPUT_ROWS)
}

pub(super) fn align_sources(
    wide: &RecordBatch,
    source: &UInt32Array,
    destination: ArrayRef,
    edges: &[ArrayRef],
    schema: &SchemaRef,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let aligned = memory.take(wide, source)?;
    let mut columns = aligned.columns().to_vec();
    columns.push(destination);
    columns.extend(edges.iter().cloned());
    let output = RecordBatch::try_new(Arc::clone(schema), columns)?;
    memory.output(&output)?;
    Ok(output)
}

struct EdgePairs {
    schema: SchemaRef,
    wide: Arc<RecordBatch>,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    memory: Arc<WorkMemory>,
}

impl fmt::Debug for EdgePairs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EdgePairs")
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for EdgePairs {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let wide = Arc::clone(&self.wide);
        let step = self.step.clone();
        let env = Arc::clone(&self.env);
        let memory = Arc::clone(&self.memory);
        producer_stream(
            Arc::clone(&self.schema),
            memory,
            None,
            move |memory, sender: BatchSender| async move {
                let work = Arc::new(memory.child("bound edge producer")?);
                let sender = &sender;
                produce_bound_edge_pairs(
                    &wide,
                    &env.snapshot,
                    &env.catalog,
                    &step.src,
                    &step.edge_type,
                    step.direction,
                    &work,
                    |batch, lease| async move {
                        sender
                            .send_bounded(batch, lease)
                            .await
                            .map_err(crate::error::OmniError::datafusion)
                    },
                )
                .await
                .map_err(external)
            },
        )
    }
}
