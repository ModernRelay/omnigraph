//! Expand aligns source rows with destinations in bounded output batches.
//! Budgeted traversals stream source windows; legacy multi-hop uses a breaker.

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
use futures::future::BoxFuture;
use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;

use super::expand::{ExpandExecution, ExpandShape, ExpandStep, GraphEnv};
use super::memory::WorkMemory;
use super::shape::{BatchSender, Flow, Source, Streamed, work};
use super::{Switch, drain_one, external};
use crate::engine::graph::{
    ExpandedPairs, PreparedEdge, bound_edge_pair_schema, execute_expand, prepare_selected_edges,
    produce_bound_edge_pairs,
};
use crate::error::OmniError;
use crate::instrumentation::record_expand_path;
use crate::table_store::ORDERED_SCAN_EXECUTION_BATCH_ROWS;

/// The most rows of one aligned output chunk, on all three strategies: the
/// source columns of a chunk are one `WorkMemory::take`, reserved at twice
/// the picked bytes, so the chunk bounds that reservation by row width.
pub(super) const EXPAND_OUTPUT_ROWS: usize = 256;

/// The traversal's stream through the shape its operator holds: a one-hop
/// or budgeted step is driven one input batch at a time, a costed multi-hop
/// step holds its whole input first. The `switch` gauge on `metrics` records
/// the mode the traversal ends on, one side of the `Expand` node's declared
/// switch. The input executes under `ctx` as given; the traversal's own
/// reservation clamps its batch size to `EXPAND_OUTPUT_ROWS`.
pub(super) fn execute(
    shape: &ExpandShape,
    input: Arc<dyn ExecutionPlan>,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    ctx: Arc<TaskContext>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let schema = shape.properties().eq_properties.schema().clone();
    let input_schema = input.schema();
    if let (ExpandShape::Streaming(streaming), true, None, ExpandExecution::Named(named)) = (
        shape,
        step.single_hop(),
        &step.edge_binding,
        &step.execution,
    ) {
        let memory = work(Arc::clone(&ctx), "ExpandExec", metrics)?;
        memory.metric("input_rows", 0);
        let body =
            super::single_hop::SingleHop::new(schema, step.clone(), named.clone(), env, metrics);
        return Ok(streaming.produce(memory, metrics, move || input.execute(0, ctx), body));
    }
    let switch = Switch::gauge(metrics);
    let clamped = Arc::new(TaskContext::new(
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
    let memory = work(clamped, "ExpandExec", metrics)?;
    memory.metric("input_rows", 0);
    match shape {
        ExpandShape::Breaker(breaker) => {
            let input = input.execute(0, ctx)?;
            let emit_step = step.clone();
            let emit_env = Arc::clone(&env);
            let declared = Arc::clone(&schema);
            Ok(breaker.held(
                memory,
                metrics,
                move |memory| async move { drain_one(input, &input_schema, &memory).await },
                move |wide, memory, sender| async move {
                    if wide.num_rows() == 0 {
                        return Ok(());
                    }
                    emit_unbound(
                        &wide, &emit_step, &emit_env, None, &switch, &memory, &sender, &declared,
                    )
                    .await
                },
            ))
        }
        ExpandShape::Streaming(streaming) => {
            let body = Batches {
                declared: Arc::clone(&schema),
                input_schema,
                step,
                env,
                switch,
                windows: None,
                prepared: None,
            };
            Ok(streaming.produce(memory, metrics, move || input.execute(0, ctx), body))
        }
    }
}

/// The per-batch body of a streaming traversal that is not the named one
/// hop: a budgeted step re-cuts its input into exact source windows and
/// expands each as it fills; a bound-edge one hop expands each input batch.
struct Batches {
    declared: SchemaRef,
    input_schema: SchemaRef,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    switch: Gauge,
    /// The window cutter of a budgeted step, opened before the input runs.
    windows: Option<SourceWindows>,
    prepared: Option<Arc<Vec<PreparedEdge>>>,
}

impl Batches {
    /// One input batch or source window expanded; a budgeted step prepares
    /// its member datasets at the first.
    async fn expand(
        &mut self,
        wide: Arc<RecordBatch>,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> Result<()> {
        let Self {
            declared,
            step,
            env,
            switch,
            prepared,
            ..
        } = self;
        let prepared = if step.budgeted() {
            Some(match prepared {
                Some(prepared) => Arc::clone(prepared),
                None => {
                    let edges = Arc::new(
                        prepare_selected_edges(env, step, memory)
                            .await
                            .map_err(external)?,
                    );
                    *prepared = Some(Arc::clone(&edges));
                    edges
                }
            })
        } else {
            None
        };
        if step.edge_binding.is_some() {
            emit_bound(&wide, step, env, prepared, memory, sender, declared).await
        } else {
            emit_unbound(
                &wide,
                step,
                env,
                prepared.as_deref().map(Vec::as_slice),
                switch,
                memory,
                sender,
                declared,
            )
            .await
        }
    }

    /// The windows of a budgeted step that `batch` fills, each expanded as
    /// it is cut.
    async fn window(
        &mut self,
        batch: RecordBatch,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> Result<()> {
        let Some(windows) = self.windows.as_mut() else {
            return Err(DataFusionError::Internal(
                "budgeted expansion windowed before it opened".into(),
            ));
        };
        windows.feed(batch)?;
        loop {
            let Some(window) = self
                .windows
                .as_mut()
                .map(SourceWindows::next_full)
                .transpose()?
                .flatten()
            else {
                return Ok(());
            };
            self.expand(Arc::new(window.batch), memory, sender).await?;
        }
    }
}

impl Streamed for Batches {
    fn open<'a>(&'a mut self, memory: &'a Arc<WorkMemory>) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if !self.step.budgeted() {
                return Ok(true);
            }
            if !memory.traversal_limited() {
                return Err(external(OmniError::manifest_internal(
                    "budgeted expansion has no traversal work limit",
                )));
            }
            Switch::IndexedScan.record(&self.switch);
            record_expand_path(true);
            memory.metric("expand_indexed", 1);
            self.windows = Some(SourceWindows::new(
                Arc::clone(&self.input_schema),
                Arc::clone(memory),
            )?);
            Ok(true)
        })
    }

    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            if self.step.budgeted() {
                self.window(batch, memory, sender).await?;
                return Ok(Flow::Continue);
            }
            memory.metric("input_rows", batch.num_rows());
            if batch.num_rows() == 0 {
                return Ok(Flow::Continue);
            }
            let input_memory = memory.child("expand input batch")?;
            input_memory.hold(&batch)?;
            self.expand(Arc::new(batch), memory, sender).await?;
            Ok(Flow::Continue)
        })
    }

    fn finish<'a>(
        &'a mut self,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let last = match self.windows.as_mut() {
                Some(windows) => windows.flush()?,
                None => None,
            };
            if let Some(window) = last {
                self.expand(Arc::new(window.batch), memory, sender).await?;
            }
            Ok(())
        })
    }
}

/// Exact source windows make scan admission independent of upstream batches.
const SOURCE_WINDOW_ROWS: usize = ORDERED_SCAN_EXECUTION_BATCH_ROWS;

struct SourceWindow {
    batch: RecordBatch,
    _memory: WorkMemory,
}

/// The cutter of exact source windows: fed one input batch at a time, it
/// hands out each window as it fills (`next_full`), charged piece by piece
/// so a consumer that stops admits no more, and the last on `flush`.
struct SourceWindows {
    schema: SchemaRef,
    memory: Arc<WorkMemory>,
    pending: Option<(RecordBatch, usize, WorkMemory)>,
    pieces: Vec<RecordBatch>,
    rows: usize,
    work: WorkMemory,
}

impl SourceWindows {
    fn new(schema: SchemaRef, memory: Arc<WorkMemory>) -> Result<Self> {
        let work = memory.child("expand source window")?;
        Ok(Self {
            schema,
            memory,
            pending: None,
            pieces: Vec::new(),
            rows: 0,
            work,
        })
    }

    /// Hold `batch` as the input the next windows are cut from; the previous
    /// one must be used up (`next_full` returned `None`).
    fn feed(&mut self, batch: RecordBatch) -> Result<()> {
        if self.pending.is_some() {
            return Err(DataFusionError::Internal(
                "expand source window fed before its pending batch was cut".into(),
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let lease = self.memory.child("expand pending source batch")?;
        lease.hold(&batch)?;
        self.pending = Some((batch, 0, lease));
        Ok(())
    }

    /// The next full window cut from the pending input, `None` once the
    /// pending input is used up with the window still short.
    fn next_full(&mut self) -> Result<Option<SourceWindow>> {
        while self.rows < SOURCE_WINDOW_ROWS {
            let Some((batch, offset, lease)) = self.pending.take() else {
                return Ok(None);
            };
            self.memory.check()?;
            let take = (SOURCE_WINDOW_ROWS - self.rows).min(batch.num_rows() - offset);
            self.work.charge_traversal(take as u64)?;
            self.work.entries::<RecordBatch>(1)?;
            let piece = batch.slice(offset, take);
            self.work.hold(&piece)?;
            self.pieces.push(piece);
            self.memory.metric("input_rows", take);
            self.rows += take;
            if offset + take < batch.num_rows() {
                self.pending = Some((batch, offset + take, lease));
            }
        }
        self.cut().map(Some)
    }

    /// What is gathered, as the last window; `None` when nothing is.
    fn flush(&mut self) -> Result<Option<SourceWindow>> {
        if self.rows == 0 {
            return Ok(None);
        }
        self.cut().map(Some)
    }

    fn cut(&mut self) -> Result<SourceWindow> {
        let pieces = std::mem::take(&mut self.pieces);
        let batch = self.work.concat(&self.schema, &pieces)?;
        let work = std::mem::replace(&mut self.work, self.memory.child("expand source window")?);
        self.rows = 0;
        Ok(SourceWindow {
            batch,
            _memory: work,
        })
    }
}

/// One input batch's bound-edge pairs, sorted by (source row, type, edge id,
/// destination id) and hydrated in `EXPAND_OUTPUT_ROWS` chunks. A source row's
/// pairs never cross an input batch, so input order keeps one global sort.
async fn emit_bound(
    wide: &Arc<RecordBatch>,
    step: &ExpandStep,
    env: &Arc<GraphEnv>,
    prepared: Option<Arc<Vec<PreparedEdge>>>,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
    declared: &SchemaRef,
) -> Result<()> {
    let pair_schema = bound_edge_pair_schema(&env.catalog, step.members(), step.has_type_column())
        .map_err(external)?;
    let partition = Arc::new(EdgePairs {
        schema: Arc::clone(&pair_schema),
        wide: Arc::clone(wide),
        step: step.clone(),
        env: Arc::clone(env),
        memory: Arc::clone(memory),
        prepared,
    });
    let pairs: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&pair_schema),
        vec![partition],
        None,
        [],
        false,
        None,
    )?);
    let mut keys = vec![0];
    if let Ok(type_index) = pair_schema.index_of(EDGE_TYPE_COLUMN) {
        keys.push(type_index);
    }
    keys.extend([2, 1]);
    let ordering = LexOrdering::new(keys.into_iter().map(|index| {
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
    prepared: Option<&[PreparedEdge]>,
    switch: &Gauge,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
    schema: &SchemaRef,
) -> Result<()> {
    execute_expand(
        wide,
        env,
        step,
        prepared,
        switch,
        memory,
        move |pairs| async move {
            emit_pairs(wide, &pairs, memory, sender, schema)
                .await
                .map_err(|error| memory.error(error))
        },
    )
    .await
    .map_err(external)
}

async fn emit_pairs(
    wide: &RecordBatch,
    pairs: &ExpandedPairs,
    memory: &WorkMemory,
    sender: &BatchSender,
    schema: &SchemaRef,
) -> Result<()> {
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
    memory.batch_rows().min(EXPAND_OUTPUT_ROWS)
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
    prepared: Option<Arc<Vec<PreparedEdge>>>,
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
        let prepared = self.prepared.clone();
        let wide = Arc::clone(&self.wide);
        let step = self.step.clone();
        let env = Arc::clone(&self.env);
        let memory = Arc::clone(&self.memory);
        let schema = Arc::clone(&self.schema);
        Source::inner(
            Arc::clone(&self.schema),
            memory,
            move |memory, sender: BatchSender| async move {
                let sender = &sender;
                for (index, member) in step.members().iter().enumerate() {
                    let work = Arc::new(memory.child("bound edge producer")?);
                    produce_bound_edge_pairs(
                        &wide,
                        &env.snapshot,
                        &env.catalog,
                        &step.src,
                        member,
                        prepared.as_ref().map(|edges| &edges[index].dataset),
                        &schema,
                        &work,
                        |batch, lease| async move {
                            sender
                                .send_bounded(batch, lease)
                                .await
                                .map_err(OmniError::datafusion)
                        },
                    )
                    .await
                    .map_err(external)?;
                }
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod tests;
