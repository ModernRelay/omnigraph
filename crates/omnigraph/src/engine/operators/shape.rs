//! The shapes an operator's output stream can take, one per
//! `omnigraph_planner::PipelineShape` plus the leaf's. Each shape owns the
//! `PlanProperties` it declares and the only builders of a stream with that
//! emission, so an operator holding a [`Breaker`] cannot emit before its body
//! returns and an operator holding a [`Streaming`] never sees more of its
//! input than the batch in hand. A join drains a side whole only through
//! [`CollectedSide`]. The producer channel, the breaker's slicer and the poll
//! counter live here, private: no operator builds a stream beside a shape.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Timestamp,
};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

use super::memory::WorkMemory;
use crate::instrumentation::{with_query_io_probes, with_query_memory_probes};

/// The `polls` counter: one per `poll_next` of any stream the operator handed
/// out, so a zero says no consumer ever asked the operator for a batch.
pub(super) const POLLS: &str = "polls";

/// The `drained` counter: one per stream the operator handed out whose
/// consumer pulled it to its end, so a row count is complete only where this
/// is set.
pub(super) const DRAINED: &str = "drained";

/// The pool reservation of one execution, named after its operator and
/// carrying the operator's metrics.
pub(super) fn work(
    ctx: Arc<TaskContext>,
    name: &str,
    metrics: &ExecutionPlanMetricsSet,
) -> DfResult<Arc<WorkMemory>> {
    let mut memory = WorkMemory::new(ctx, name)?;
    memory.set_metrics(metrics.clone());
    Ok(Arc::new(memory))
}

fn properties(schema: SchemaRef, emission: EmissionType) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        emission,
        Boundedness::Bounded,
    ))
}

/// A stream that ends at once and counts its polls: the operator ran and
/// produced nothing, its input never executed.
pub(super) fn empty(
    metrics: &ExecutionPlanMetricsSet,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    polled(
        metrics,
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )),
    )
}

/// The shape of an operator that emits nothing until it has seen all of its
/// streamed input: one partition, `EmissionType::Final`, bounded.
#[derive(Debug)]
pub(super) struct Breaker {
    properties: Arc<PlanProperties>,
}

impl Breaker {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            properties: properties(schema, EmissionType::Final),
        }
    }

    pub(super) fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The stream of a one-batch breaker: `body` builds the whole output
    /// under `memory`, which then lives with the stream while the output
    /// leaves in `batch_size` slices; dropping the stream drops the body, the
    /// held batches and the reservation together. Nothing leaves before the
    /// body returns.
    pub(super) fn stream<F>(
        &self,
        memory: Arc<WorkMemory>,
        metrics: &ExecutionPlanMetricsSet,
        body: impl FnOnce(Arc<WorkMemory>) -> F,
    ) -> SendableRecordBatchStream
    where
        F: Future<Output = DfResult<RecordBatch>> + Send + 'static,
    {
        let schema = self.properties.eq_properties.schema().clone();
        let batch_size = memory.ctx.session_config().batch_size();
        memory.metric("input_rows", 0);
        let emitted = Arc::clone(&memory);
        let future = body(memory);
        let baseline = BaselineMetrics::new(metrics, 0);
        let stream = futures::stream::once(async move {
            let mut future = Box::pin(future);
            let batch = futures::future::poll_fn(|cx| {
                let _timer = baseline.elapsed_compute().timer();
                future.as_mut().poll(cx)
            })
            .await?;
            baseline.record_output(batch.num_rows());
            baseline.done();
            emitted.output(&batch)?;
            let held = Arc::clone(&emitted);
            Ok::<_, DataFusionError>(
                futures::stream::iter((0..batch.num_rows()).step_by(batch_size.max(1)).map(
                    move |offset| {
                        Ok(batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                    },
                ))
                .map(move |batch| {
                    let _held = &held;
                    batch
                }),
            )
        })
        .try_flatten();
        polled(
            metrics,
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
        )
    }

    /// The stream of a breaker whose output is many batches: `collect` runs
    /// first with no sender, so nothing leaves while it consumes the input;
    /// `emit` then sends what `collect` held, with no input in hand. Both run
    /// on the producer's owned worker.
    pub(super) fn held<T, C, CF, E, EF>(
        &self,
        memory: Arc<WorkMemory>,
        metrics: &ExecutionPlanMetricsSet,
        collect: C,
        emit: E,
    ) -> SendableRecordBatchStream
    where
        T: Send + 'static,
        C: FnOnce(Arc<WorkMemory>) -> CF + Send + 'static,
        CF: Future<Output = DfResult<T>> + Send + 'static,
        E: FnOnce(T, Arc<WorkMemory>, BatchSender) -> EF + Send + 'static,
        EF: Future<Output = DfResult<()>> + Send + 'static,
    {
        let schema = self.properties.eq_properties.schema().clone();
        polled(
            metrics,
            producer_stream(
                schema,
                memory,
                Some(metrics),
                move |memory, sender| async move {
                    let held = collect(Arc::clone(&memory)).await?;
                    emit(held, memory, sender).await
                },
            ),
        )
    }
}

/// The shape of an operator that emits as its streamed input arrives: one
/// partition, `EmissionType::Incremental`, bounded.
#[derive(Debug)]
pub(super) struct Streaming {
    properties: Arc<PlanProperties>,
}

/// What a [`Streaming::map`] body made of one input batch.
pub(super) enum Mapped {
    /// `batch` leaves; `lease` is released when the consumer takes the next.
    Batch {
        batch: RecordBatch,
        lease: Option<WorkMemory>,
    },
    /// `batch` leaves and the stream ends without pulling more input.
    Last(RecordBatch),
}

/// Whether a [`Streamed`] body wants the next input batch.
pub(super) enum Flow {
    Continue,
    /// The rest of the input is never pulled; `finish` runs next.
    Stop,
}

/// The per-batch work of a [`Streaming::produce`] operator: `open` runs
/// before the streamed input executes, with no sender, where the sides the
/// node collects are drained and a scan's runtime filter is filled; the
/// shape then pulls the input and hands each batch to `batch` with the
/// sender, and calls `finish` once after the last. The operator never holds
/// the input stream, so its first output cannot wait for the input's end;
/// only what `finish` flushes may.
pub(super) trait Streamed: Send + 'static {
    /// `false` ends the stream with no output; the input never executes.
    fn open<'a>(&'a mut self, _memory: &'a Arc<WorkMemory>) -> BoxFuture<'a, DfResult<bool>> {
        Box::pin(async { Ok(true) })
    }

    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<Flow>>;

    fn finish<'a>(
        &'a mut self,
        _memory: &'a Arc<WorkMemory>,
        _sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl Streaming {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            properties: properties(schema, EmissionType::Incremental),
        }
    }

    pub(super) fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The inline stream of a per-batch operator: `map` runs on the
    /// consumer's poll, once per input batch, and sees nothing else.
    pub(super) fn map<F>(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        input: SendableRecordBatchStream,
        map: F,
    ) -> SendableRecordBatchStream
    where
        F: FnMut(RecordBatch) -> DfResult<Mapped> + Send + 'static,
    {
        let schema = self.properties.eq_properties.schema().clone();
        let baseline = BaselineMetrics::new(metrics, 0);
        let stream = futures::stream::try_unfold(
            (input, map, None::<WorkMemory>, false),
            move |(mut input, mut map, held, ended)| {
                let baseline = baseline.clone();
                async move {
                    drop(held);
                    if ended {
                        baseline.done();
                        return Ok(None);
                    }
                    match input.next().await {
                        Some(Ok(batch)) => match map(batch)? {
                            Mapped::Batch { batch, lease } => {
                                baseline.record_output(batch.num_rows());
                                Ok(Some((batch, (input, map, lease, false))))
                            }
                            Mapped::Last(batch) => {
                                baseline.record_output(batch.num_rows());
                                Ok(Some((batch, (input, map, None, true))))
                            }
                        },
                        Some(Err(error)) => Err(error),
                        None => {
                            baseline.done();
                            Ok(None)
                        }
                    }
                }
            },
        );
        polled(
            metrics,
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
        )
    }

    /// The produced stream of a per-batch operator: on the producer's owned
    /// worker the shape opens `body`, executes `input` (so a side the body
    /// collected or a filter it filled is in place first), and drives the
    /// body one batch at a time.
    pub(super) fn produce(
        &self,
        memory: Arc<WorkMemory>,
        metrics: &ExecutionPlanMetricsSet,
        input: impl FnOnce() -> DfResult<SendableRecordBatchStream> + Send + 'static,
        mut body: impl Streamed,
    ) -> SendableRecordBatchStream {
        let schema = self.properties.eq_properties.schema().clone();
        polled(
            metrics,
            producer_stream(
                schema,
                memory,
                Some(metrics),
                move |memory, sender| async move {
                    if !body.open(&memory).await? {
                        return Ok(());
                    }
                    let mut input = input()?;
                    while let Some(batch) = input.next().await {
                        if let Flow::Stop = body.batch(batch?, &memory, &sender).await? {
                            break;
                        }
                    }
                    drop(input);
                    body.finish(&memory, &sender).await
                },
            ),
        )
    }
}

/// The shape of a leaf: no input to hold, its batches produced as the read
/// yields them; one partition, `EmissionType::Incremental`, bounded.
#[derive(Debug)]
pub(super) struct Source {
    properties: Arc<PlanProperties>,
}

impl Source {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            properties: properties(schema, EmissionType::Incremental),
        }
    }

    pub(super) fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The produced stream of a leaf: `body` reads and sends on the
    /// producer's owned worker.
    pub(super) fn stream<F>(
        &self,
        memory: Arc<WorkMemory>,
        metrics: &ExecutionPlanMetricsSet,
        body: impl FnOnce(Arc<WorkMemory>, BatchSender) -> F + Send + 'static,
    ) -> SendableRecordBatchStream
    where
        F: Future<Output = DfResult<()>> + Send + 'static,
    {
        let schema = self.properties.eq_properties.schema().clone();
        polled(
            metrics,
            producer_stream(schema, memory, Some(metrics), body),
        )
    }

    /// A produced stream that is not an operator's own output (a partition
    /// an operator feeds to a DataFusion sort it runs inside its body),
    /// counted on no metrics.
    pub(super) fn inner<F>(
        schema: SchemaRef,
        memory: Arc<WorkMemory>,
        body: impl FnOnce(Arc<WorkMemory>, BatchSender) -> F + Send + 'static,
    ) -> SendableRecordBatchStream
    where
        F: Future<Output = DfResult<()>> + Send + 'static,
    {
        producer_stream(schema, memory, None, body)
    }
}

/// A side a join drains whole before its streamed side runs, the one
/// `PhysicalNode::collected_sides` names: executed under `memory.ctx` and
/// held as one batch under a child reservation named `owner`, which lives
/// with the returned value. The pieces are released once concatenated.
pub(super) struct CollectedSide {
    pub(super) batch: RecordBatch,
    pub(super) held: WorkMemory,
}

impl CollectedSide {
    pub(super) async fn collect(
        side: &Arc<dyn ExecutionPlan>,
        memory: &WorkMemory,
        owner: &str,
    ) -> DfResult<Self> {
        let schema = side.schema();
        let mut stream = side.execute(0, Arc::clone(&memory.ctx))?;
        let held = memory.child(owner)?;
        let pieces = held.child(&format!("{owner} input"))?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            pieces.hold(&batch)?;
            pieces.entries::<RecordBatch>(1)?;
            batches.push(batch);
        }
        let batch = held.concat(&schema, &batches)?;
        drop(batches);
        drop(pieces);
        Ok(Self { batch, held })
    }
}

/// `stream` with every poll counted on `metrics`, its end when reached, and
/// the timestamps of its first poll and its end, the operator's wall span.
fn polled(
    metrics: &ExecutionPlanMetricsSet,
    stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    Box::pin(Polled {
        stream,
        polls: MetricBuilder::new(metrics).counter(POLLS, 0),
        drained: MetricBuilder::new(metrics).counter(DRAINED, 0),
        start: MetricBuilder::new(metrics).start_timestamp(0),
        end: MetricBuilder::new(metrics).end_timestamp(0),
    })
}

struct Polled {
    stream: SendableRecordBatchStream,
    polls: Count,
    drained: Count,
    start: Timestamp,
    end: Timestamp,
}

impl Stream for Polled {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.polls.value() == 0 {
            self.start.record();
        }
        self.polls.add(1);
        let polled = self.stream.as_mut().poll_next(cx);
        match &polled {
            Poll::Ready(None) => {
                self.drained.add(1);
                self.end.record();
            }
            Poll::Ready(Some(Err(_))) => self.end.record(),
            _ => {}
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl RecordBatchStream for Polled {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

/// A single producer sends each batch and its reservation in the same order.
pub(super) struct BatchSender {
    batches: mpsc::Sender<DfResult<RecordBatch>>,
    leases: mpsc::Sender<Arc<WorkMemory>>,
}

impl BatchSender {
    pub(super) async fn send(&self, batch: RecordBatch, memory: Arc<WorkMemory>) -> DfResult<()> {
        let queued = Arc::new(memory.child("queued producer batch")?);
        queued.output(&batch)?;
        self.leases.send(queued).await.map_err(|_| cancelled())?;
        self.batches.send(Ok(batch)).await.map_err(|_| cancelled())
    }

    /// Compact batches to the byte target; an indivisible row uses the query budget.
    pub(super) async fn send_bounded(
        &self,
        batch: RecordBatch,
        memory: Arc<WorkMemory>,
    ) -> DfResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let input = Arc::new(memory.child("producer batch input")?);
        input.hold(&batch)?;
        let target = memory.batch_bytes();
        if batch.get_array_memory_size() <= target {
            return self.send(batch, memory).await;
        }
        let mut offset = 0;
        while offset < batch.num_rows() {
            let mut rows =
                (batch.num_rows() - offset).min(memory.ctx.session_config().batch_size());
            while rows > 1 && memory.slice_bytes(&batch, offset, rows)? > target {
                rows = rows.div_ceil(2);
            }
            loop {
                memory.check()?;
                let work = Arc::new(memory.child("expand output chunk")?);
                work.entries::<u32>(rows)?;
                let indices = (offset..offset + rows)
                    .map(u32::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| {
                        DataFusionError::Execution("expand batch exceeds row index range".into())
                    })?;
                let output = work.take(&batch, &UInt32Array::from(indices))?;
                if output.get_array_memory_size() <= target || rows == 1 {
                    self.send(output, work).await?;
                    offset += rows;
                    break;
                }
                rows = rows.div_ceil(2);
            }
        }
        Ok(())
    }
}

fn cancelled() -> DataFusionError {
    DataFusionError::Execution("query batch receiver closed".into())
}

/// Spawn one producer. The receiving stream owns cancellation and queued leases;
/// a consumed batch stays charged until the next poll transfers ownership onward.
fn producer_stream<F>(
    schema: SchemaRef,
    memory: Arc<WorkMemory>,
    metrics: Option<&ExecutionPlanMetricsSet>,
    body: impl FnOnce(Arc<WorkMemory>, BatchSender) -> F + Send + 'static,
) -> SendableRecordBatchStream
where
    F: Future<Output = DfResult<()>> + Send + 'static,
{
    let owner = match memory.register_owned_work() {
        Ok(owner) => owner,
        Err(error) => {
            return Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::once(async move { Err(error) }),
            ));
        }
    };
    let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 2);
    let (leases, receiver) = mpsc::channel(2);
    let sender = BatchSender {
        batches: builder.tx(),
        leases,
    };
    let io = crate::instrumentation::capture_query_io_probes();
    let probes = crate::instrumentation::current_query_memory_probes();
    let worker = owner.child();
    builder.spawn(
        owner.own(Box::pin(
            async move {
                let work = async move {
                    let work = memory.blocking_owned(worker, move |memory| body(memory, sender));
                    match io {
                        Some(probes) => with_query_io_probes(probes, work).await,
                        None => work.await,
                    }
                };
                match probes {
                    Some(probes) => with_query_memory_probes(probes, work).await,
                    None => work.await,
                }
            }
            .in_current_span(),
        )),
    );
    let baseline = metrics.map(|metrics| BaselineMetrics::new(metrics, 0));
    let counts = metrics.map(|metrics| {
        datafusion::physical_plan::metrics::MetricBuilder::new(metrics).counter("output_batches", 0)
    });
    let stream = futures::stream::try_unfold(
        (builder.build(), receiver, None::<Arc<WorkMemory>>),
        move |(mut batches, mut leases, held)| {
            let baseline = baseline.clone();
            let counts = counts.clone();
            async move {
                drop(held);
                match batches.next().await {
                    Some(Ok(batch)) => {
                        let lease = leases.recv().await.ok_or_else(|| {
                            DataFusionError::Internal("producer batch has no memory lease".into())
                        })?;
                        if let Some(baseline) = &baseline {
                            baseline.record_output(batch.num_rows());
                        }
                        if let Some(counts) = &counts {
                            counts.add(1);
                        }
                        Ok(Some((batch, (batches, leases, Some(lease)))))
                    }
                    Some(Err(error)) => Err(error),
                    None => {
                        if let Some(baseline) = &baseline {
                            baseline.done();
                        }
                        Ok(None)
                    }
                }
            }
        },
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::FutureExt;

    use super::*;
    use crate::engine::context::{QueryContext, query_memory_limit};
    use crate::instrumentation::{
        QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
    };

    const ROWS: usize = 8_192;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![value; ROWS]))],
        )
        .unwrap()
    }

    async fn released(probes: &QueryMemoryProbes) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while probes.reserved_bytes() != 0 || probes.active_blocking_work() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer tasks and queued reservations must stop with their stream");
    }

    /// Observes a producer blocked on a full queue and its reserved bytes; a case sees final rows only.
    #[tokio::test]
    async fn bounded_queue_preserves_each_batch_lease_with_a_shared_parent() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(1024 * 1024, async {
                let context = QueryContext::new(query_memory_limit()).unwrap();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "producer test").unwrap());
                let completed = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&completed);
                let (queued, ready) = tokio::sync::oneshot::channel();
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, sender| async move {
                        let mut queued = Some(queued);
                        for value in 0..4 {
                            sender.send(batch(value), Arc::clone(&memory)).await?;
                            observed.fetch_add(1, Ordering::SeqCst);
                            if value == 1 {
                                queued.take().unwrap().send(()).unwrap();
                            }
                        }
                        Ok(())
                    });
                tokio::time::timeout(Duration::from_secs(3), ready)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    completed.load(Ordering::SeqCst),
                    2,
                    "the third send must await capacity"
                );
                assert!(
                    probes.reserved_bytes() >= 2 * ROWS * size_of::<i64>(),
                    "two queued batches must retain separate buffer charges, even with one parent"
                );
                for expected in 0..4 {
                    let received = tokio::time::timeout(Duration::from_secs(3), stream.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_eq!(received, batch(expected));
                }
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), stream.next())
                        .await
                        .unwrap()
                        .is_none()
                );
                assert_eq!(completed.load(Ordering::SeqCst), 4);
                drop(stream);
                drop(context);
                released(&probes).await;
                assert!(
                    probes.blocking_started() > 0,
                    "producer work must remain observable"
                );
            }),
        )
        .await;
    }

    /// Reads reserved bytes while the consumer has not polled; a case cannot pause a consumer.
    #[tokio::test]
    async fn compaction_keeps_its_input_charged_while_the_queue_is_full() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(4 * 1024 * 1024, async {
                let context = QueryContext::new(query_memory_limit()).unwrap();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "compact producer").unwrap());
                let target = memory.batch_bytes();
                let total_rows = ROWS * 16;
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, sender| async move {
                        let input = RecordBatch::try_new(
                            schema(),
                            vec![Arc::new(Int64Array::from(vec![7; total_rows]))],
                        )?;
                        sender.send_bounded(input, memory).await
                    });
                tokio::time::timeout(Duration::from_secs(3), async {
                    while probes.reserved_bytes() < (total_rows + 2 * ROWS) * size_of::<i64>() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the producer must retain its original input and queued compact batches");
                let mut rows = 0;
                while let Some(batch) = stream.next().await {
                    let batch = batch.unwrap();
                    assert!(batch.get_array_memory_size() <= target);
                    rows += batch.num_rows();
                }
                assert_eq!(rows, total_rows);
                drop(stream);
                drop(context);
                released(&probes).await;
            }),
        )
        .await;
    }

    /// Drops the stream while the producer is paused at a checkpoint; a case cannot drop a stream.
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_stream_cancels_charged_producer_work() {
        let probes = QueryMemoryProbes::default();
        let pause = probes.pause_blocking_work();
        with_query_memory_probes(probes.clone(), async {
            let context = QueryContext::new(query_memory_limit()).unwrap();
            let settlement = context.owned_workers();
            let memory =
                Arc::new(WorkMemory::new(context.task_ctx(), "producer cancellation").unwrap());
            let stream =
                producer_stream(schema(), memory, None, move |memory, _sender| async move {
                    memory.grow(4_096)?;
                    memory.checkpoint()?;
                    std::future::pending::<()>().await;
                    Ok(())
                });
            tokio::time::timeout(Duration::from_secs(3), async {
                while !pause.is_paused() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("producer must reach its charged checkpoint");
            assert!(probes.active_blocking_work() > 0);
            assert!(probes.reserved_bytes() >= 4_096);
            drop(stream);
            drop(context);
            let observer = settlement.clone();
            let waiter = tokio::spawn(async move { observer.wait().await });
            tokio::task::yield_now().await;
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            assert!(settlement.wait().now_or_never().is_none());
            assert!(probes.active_blocking_work() > 0);
            assert!(probes.reserved_bytes() >= 4_096);
            pause.release();
            tokio::time::timeout(Duration::from_secs(3), settlement.wait())
                .await
                .expect("cancelling a settlement observer must not release the worker");
            assert_eq!(probes.active_blocking_work(), 0);
            assert_eq!(probes.reserved_bytes(), 0);
        })
        .await;
    }

    /// Executes the same return boundary as a real query while a producer is
    /// still running; GQT cannot pause a worker or inspect an unreturned result.
    #[tokio::test(flavor = "current_thread")]
    async fn query_return_waits_for_owned_producers_on_success_and_error() {
        #[derive(Clone, Copy, Debug)]
        enum Completion {
            Success,
            Error,
            PollPanic,
            DropPanic,
        }

        struct Execution {
            stream: Option<SendableRecordBatchStream>,
            completion: Completion,
        }

        impl Future for Execution {
            type Output = std::result::Result<u32, &'static str>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                drop(self.stream.take());
                std::task::Poll::Ready(match self.completion {
                    Completion::Error => Err("query error sentinel"),
                    Completion::PollPanic => panic!("query poll panic sentinel"),
                    _ => Ok(42),
                })
            }
        }

        impl Drop for Execution {
            fn drop(&mut self) {
                if matches!(self.completion, Completion::DropPanic) {
                    panic!("query drop panic sentinel");
                }
            }
        }

        for completion in [
            Completion::Success,
            Completion::Error,
            Completion::PollPanic,
            Completion::DropPanic,
        ] {
            let probes = QueryMemoryProbes::default();
            let pause = probes.pause_blocking_work();
            with_query_memory_probes(probes.clone(), async {
                let context = QueryContext::new(query_memory_limit()).unwrap();
                let memory = Arc::new(
                    WorkMemory::new(context.task_ctx(), "query return ownership").unwrap(),
                );
                let stream =
                    producer_stream(schema(), memory, None, |memory, _sender| async move {
                        memory.grow(4_096)?;
                        memory.checkpoint()?;
                        std::future::pending::<()>().await;
                        Ok(())
                    });
                tokio::time::timeout(Duration::from_secs(3), async {
                    while !pause.entered() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("producer must reach its charged checkpoint");
                let returned = std::panic::AssertUnwindSafe(context.run_owned(Execution {
                    stream: Some(stream),
                    completion,
                }))
                .catch_unwind();
                tokio::pin!(returned);
                assert!(
                    futures::poll!(returned.as_mut()).is_pending(),
                    "{completion:?} must retain its result or panic until the worker releases"
                );
                assert!(probes.active_blocking_work() > 0);
                assert!(probes.reserved_bytes() >= 4_096);
                pause.release();
                let returned = tokio::time::timeout(Duration::from_secs(3), returned)
                    .await
                    .unwrap();
                match completion {
                    Completion::Success => assert_eq!(returned.unwrap(), Ok(42)),
                    Completion::Error => {
                        assert_eq!(returned.unwrap(), Err("query error sentinel"));
                    }
                    Completion::PollPanic | Completion::DropPanic => {
                        let expected = if matches!(completion, Completion::PollPanic) {
                            "query poll panic sentinel"
                        } else {
                            "query drop panic sentinel"
                        };
                        assert_eq!(
                            returned.unwrap_err().downcast_ref::<&str>(),
                            Some(&expected)
                        );
                    }
                }
                assert_eq!(probes.active_blocking_work(), 0);
                assert_eq!(probes.reserved_bytes(), 0);
            })
            .await;
        }
    }

    /// Marks the moment a stream is pulled past this point: `flag` is set
    /// when the consumer asks for the element after `first`.
    fn then_flagged(
        first: RecordBatch,
        flag: Arc<AtomicUsize>,
        rest: impl Future<Output = RecordBatch> + Send + 'static,
    ) -> SendableRecordBatchStream {
        let stream = futures::stream::once(async move { Ok(first) }).chain(futures::stream::once(
            async move {
                flag.fetch_add(1, Ordering::SeqCst);
                Ok::<_, DataFusionError>(rest.await)
            },
        ));
        Box::pin(RecordBatchStreamAdapter::new(schema(), stream))
    }

    async fn settled() {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }

    async fn eventually(counter: &AtomicUsize, value: usize, what: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while counter.load(Ordering::SeqCst) != value {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    async fn next(stream: &mut SendableRecordBatchStream) -> Option<RecordBatch> {
        tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("the stream answers within the timeout")
            .map(|batch| batch.unwrap())
    }

    /// A breaker's consumer sees nothing while the body still runs; the
    /// emission the planner's `Breaker` prints is this timing, which no case
    /// observes.
    #[tokio::test]
    async fn breaker_stream_yields_nothing_before_its_body_returns() {
        let context = QueryContext::new(query_memory_limit()).unwrap();
        let metrics = ExecutionPlanMetricsSet::new();
        let memory = work(context.task_ctx(), "breaker test", &metrics).unwrap();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let mut stream = Breaker::new(schema()).stream(memory, &metrics, |memory| async move {
            gate.await.map_err(|_| cancelled())?;
            let batch = batch(1);
            memory.hold(&batch)?;
            Ok(batch)
        });
        assert!(futures::poll!(stream.next()).is_pending());
        settled().await;
        assert!(futures::poll!(stream.next()).is_pending());
        release.send(()).unwrap();
        assert_eq!(next(&mut stream).await, Some(batch(1)));
        assert!(next(&mut stream).await.is_none());
    }

    /// A held breaker's collect stage has no sender, so its consumer sees
    /// nothing until the stage returns, and the emit stage sends what it held.
    #[tokio::test]
    async fn breaker_held_emits_only_after_collect_returns() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(probes.clone(), async {
            let context = QueryContext::new(query_memory_limit()).unwrap();
            let metrics = ExecutionPlanMetricsSet::new();
            let memory = work(context.task_ctx(), "held test", &metrics).unwrap();
            let (release, gate) = tokio::sync::oneshot::channel::<()>();
            let mut stream = Breaker::new(schema()).held(
                memory,
                &metrics,
                |memory| async move {
                    let held = vec![batch(1), batch(2)];
                    for batch in &held {
                        memory.hold(batch)?;
                    }
                    gate.await.map_err(|_| cancelled())?;
                    Ok(held)
                },
                |held, memory, sender| async move {
                    for batch in held {
                        sender.send(batch, Arc::clone(&memory)).await?;
                    }
                    Ok(())
                },
            );
            settled().await;
            assert!(futures::poll!(stream.next()).is_pending());
            release.send(()).unwrap();
            assert_eq!(next(&mut stream).await, Some(batch(1)));
            assert_eq!(next(&mut stream).await, Some(batch(2)));
            assert!(next(&mut stream).await.is_none());
            drop(stream);
            drop(context);
            released(&probes).await;
        })
        .await;
    }

    /// An echo of each input batch, stopping after `stop_after` batches and
    /// flushing one marker batch from `finish`.
    struct Echo {
        opened: Option<tokio::sync::oneshot::Receiver<()>>,
        stop_after: Option<usize>,
        seen: usize,
    }

    impl Streamed for Echo {
        fn open<'a>(&'a mut self, _: &'a Arc<WorkMemory>) -> BoxFuture<'a, DfResult<bool>> {
            Box::pin(async move {
                if let Some(gate) = self.opened.take() {
                    gate.await.map_err(|_| cancelled())?;
                }
                Ok(true)
            })
        }

        fn batch<'a>(
            &'a mut self,
            batch: RecordBatch,
            memory: &'a Arc<WorkMemory>,
            sender: &'a BatchSender,
        ) -> BoxFuture<'a, DfResult<Flow>> {
            Box::pin(async move {
                sender.send(batch, Arc::clone(memory)).await?;
                self.seen += 1;
                Ok(if self.stop_after == Some(self.seen) {
                    Flow::Stop
                } else {
                    Flow::Continue
                })
            })
        }

        fn finish<'a>(
            &'a mut self,
            memory: &'a Arc<WorkMemory>,
            sender: &'a BatchSender,
        ) -> BoxFuture<'a, DfResult<()>> {
            Box::pin(async move { sender.send(batch(9), Arc::clone(memory)).await })
        }
    }

    /// The streamed input executes only after `open` returns, and the first
    /// output leaves while the input is still stalled: the emission the
    /// planner's `Streaming` prints, which no case observes.
    #[tokio::test]
    async fn streaming_produce_opens_first_and_emits_before_its_input_ends() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(probes.clone(), async {
            let context = QueryContext::new(query_memory_limit()).unwrap();
            let metrics = ExecutionPlanMetricsSet::new();
            let memory = work(context.task_ctx(), "produce test", &metrics).unwrap();
            let (open, opened) = tokio::sync::oneshot::channel::<()>();
            let (unstall, stall) = tokio::sync::oneshot::channel::<()>();
            let executed = Arc::new(AtomicUsize::new(0));
            let pulled_past_first = Arc::new(AtomicUsize::new(0));
            let input = {
                let executed = Arc::clone(&executed);
                let flag = Arc::clone(&pulled_past_first);
                move || {
                    executed.fetch_add(1, Ordering::SeqCst);
                    Ok(then_flagged(batch(1), flag, async move {
                        stall.await.unwrap();
                        batch(2)
                    }))
                }
            };
            let mut stream = Streaming::new(schema()).produce(
                memory,
                &metrics,
                input,
                Echo {
                    opened: Some(opened),
                    stop_after: None,
                    seen: 0,
                },
            );
            settled().await;
            assert!(futures::poll!(stream.next()).is_pending());
            assert_eq!(
                executed.load(Ordering::SeqCst),
                0,
                "the input executes only after `open`"
            );
            open.send(()).unwrap();
            assert_eq!(next(&mut stream).await, Some(batch(1)));
            assert_eq!(executed.load(Ordering::SeqCst), 1);
            eventually(
                &pulled_past_first,
                1,
                "the body asks for the second batch, which is stalled",
            )
            .await;
            assert!(futures::poll!(stream.next()).is_pending());
            unstall.send(()).unwrap();
            assert_eq!(next(&mut stream).await, Some(batch(2)));
            assert_eq!(next(&mut stream).await, Some(batch(9)));
            assert!(next(&mut stream).await.is_none());
            drop(stream);
            drop(context);
            released(&probes).await;
        })
        .await;
    }

    /// `Flow::Stop` leaves the rest of the input unpulled and still runs
    /// `finish`.
    #[tokio::test]
    async fn streaming_produce_stops_pulling_on_flow_stop() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(probes.clone(), async {
            let context = QueryContext::new(query_memory_limit()).unwrap();
            let metrics = ExecutionPlanMetricsSet::new();
            let memory = work(context.task_ctx(), "stop test", &metrics).unwrap();
            let pulled_past_first = Arc::new(AtomicUsize::new(0));
            let flag = Arc::clone(&pulled_past_first);
            let mut stream = Streaming::new(schema()).produce(
                memory,
                &metrics,
                move || Ok(then_flagged(batch(1), flag, async { batch(2) })),
                Echo {
                    opened: None,
                    stop_after: Some(1),
                    seen: 0,
                },
            );
            assert_eq!(next(&mut stream).await, Some(batch(1)));
            assert_eq!(next(&mut stream).await, Some(batch(9)));
            assert!(next(&mut stream).await.is_none());
            assert_eq!(pulled_past_first.load(Ordering::SeqCst), 0);
            drop(stream);
            drop(context);
            released(&probes).await;
        })
        .await;
    }

    /// An inline `map` emits each batch as it arrives and `Mapped::Last`
    /// ends the stream without pulling more input.
    #[tokio::test]
    async fn streaming_map_emits_per_batch_and_last_ends_without_pulling() {
        let metrics = ExecutionPlanMetricsSet::new();
        let (unstall, stall) = tokio::sync::oneshot::channel::<()>();
        let pulled_past_first = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&pulled_past_first);
        let input = then_flagged(batch(1), flag, async move {
            stall.await.unwrap();
            batch(2)
        });
        let mut seen = 0;
        let mut stream = Streaming::new(schema()).map(&metrics, input, move |batch| {
            seen += 1;
            Ok(if seen == 2 {
                Mapped::Last(batch)
            } else {
                Mapped::Batch { batch, lease: None }
            })
        });
        assert_eq!(next(&mut stream).await, Some(batch(1)));
        assert!(futures::poll!(stream.next()).is_pending());
        eventually(
            &pulled_past_first,
            1,
            "the map pulls the second batch, which is stalled",
        )
        .await;
        unstall.send(()).unwrap();
        assert_eq!(next(&mut stream).await, Some(batch(2)));
        assert!(next(&mut stream).await.is_none());

        let pulled_past_first = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&pulled_past_first);
        let input = then_flagged(batch(1), flag, async { batch(2) });
        let mut stream =
            Streaming::new(schema()).map(&metrics, input, |batch| Ok(Mapped::Last(batch)));
        assert_eq!(next(&mut stream).await, Some(batch(1)));
        assert!(next(&mut stream).await.is_none());
        assert_eq!(pulled_past_first.load(Ordering::SeqCst), 0);
    }

    /// Injects a producer error and a panic; no query reaches either from a case.
    #[tokio::test]
    async fn producer_failures_reach_the_consumer_and_release_work() {
        for panic in [false, true] {
            let probes = QueryMemoryProbes::default();
            with_query_memory_probes(probes.clone(), async {
                let context = QueryContext::new(query_memory_limit()).unwrap();
                let settlement = context.owned_workers();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "producer failure").unwrap());
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, _sender| async move {
                        memory.grow(4_096)?;
                        assert!(!panic, "producer panic sentinel");
                        Err(DataFusionError::Execution(
                            "producer error sentinel".to_string(),
                        ))
                    });
                let error = tokio::time::timeout(Duration::from_secs(3), stream.next())
                    .await
                    .unwrap()
                    .expect("producer failure must reach the stream")
                    .unwrap_err();
                let expected = if panic {
                    "producer panic sentinel"
                } else {
                    "producer error sentinel"
                };
                assert!(error.to_string().contains(expected), "{error}");
                drop(stream);
                drop(context);
                tokio::time::timeout(Duration::from_secs(3), settlement.wait())
                    .await
                    .expect("failed or panicked producers must release their child ownership");
                assert_eq!(probes.active_blocking_work(), 0);
                assert_eq!(probes.reserved_bytes(), 0);
            })
            .await;
        }
    }
}
