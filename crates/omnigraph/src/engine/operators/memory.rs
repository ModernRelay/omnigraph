//! Query-owned admission for graph work and shared Arrow allocations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, SchemaRef};
use datafusion::arrow::array::ArrayData;
use datafusion::arrow::buffer::Buffer;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::{
    sort::SortExec, sort_preserving_merge::SortPreservingMergeExec,
};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance_datafusion::exec::HardCapBatchSizeExec;

use crate::error::OmniError;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug)]
pub(in crate::engine) struct QueryResources {
    pool: Arc<dyn MemoryPool>,
    limit: u64,
    probes: Option<crate::instrumentation::QueryMemoryProbes>,
    buffers: Mutex<BufferRegistry>,
    registry_memory: MemoryReservation,
}

/// Live charges by allocation start. Dead entries are swept when the map
/// reaches `sweep_at`, which is then set to twice the surviving count.
#[derive(Debug, Default)]
struct BufferRegistry {
    charges: HashMap<usize, Weak<BufferCharge>>,
    sweep_at: usize,
}

impl BufferRegistry {
    const FIRST_SWEEP: usize = 64;

    fn sweep(&mut self) {
        if self.charges.len() >= self.sweep_at.max(Self::FIRST_SWEEP) {
            self.charges.retain(|_, charge| charge.strong_count() != 0);
            self.sweep_at = self.charges.len().saturating_mul(2);
        }
    }
}

#[derive(Debug)]
struct BufferCharge {
    _buffer: Buffer,
    reservation: MemoryReservation,
}

fn charged_size(buffer: &Buffer) -> usize {
    buffer
        .capacity()
        .max(buffer.len())
        .saturating_add(std::mem::size_of::<BufferCharge>() + 32)
}

#[derive(Debug, Default)]
pub(in crate::engine) struct BatchLease {
    _charges: Vec<Arc<BufferCharge>>,
    _metadata: Option<MemoryReservation>,
}

impl QueryResources {
    pub(in crate::engine) fn new(pool: Arc<dyn MemoryPool>, limit: u64) -> Self {
        crate::instrumentation::record_query_memory_pool(&pool);
        let registry_memory = MemoryConsumer::new("graph allocation registry").register(&pool);
        Self {
            registry_memory,
            pool,
            limit,
            probes: crate::instrumentation::current_query_memory_probes(),
            buffers: Mutex::new(BufferRegistry::default()),
        }
    }

    fn refused(&self, owner: &str) {
        if let Some(probes) = &self.probes {
            probes.record_refusal(owner);
        }
    }

    fn reservation(&self, name: &str, bytes: usize) -> DfResult<MemoryReservation> {
        self.unrecorded(name, bytes)
            .inspect_err(|_| self.refused(name))
    }

    /// A reservation whose refusal the caller records under its own name.
    fn unrecorded(&self, name: &str, bytes: usize) -> DfResult<MemoryReservation> {
        let reservation = MemoryConsumer::new(name).register(&self.pool);
        reservation.try_grow(bytes)?;
        Ok(reservation)
    }

    fn batch(&self, batch: &RecordBatch) -> DfResult<BatchLease> {
        fn buffers(data: &ArrayData) -> usize {
            data.buffers().len()
                + usize::from(data.nulls().is_some())
                + data.child_data().iter().map(buffers).sum::<usize>()
        }
        let count: usize = batch
            .columns()
            .iter()
            .map(|column| buffers(&column.to_data()))
            .sum();
        let metadata = self.unrecorded(
            "graph batch handles",
            count.saturating_add(1).saturating_mul(64),
        )?;
        let mut leases = Vec::with_capacity(count);
        let mut registry = locked(&self.buffers);
        registry.sweep();
        for column in batch.columns() {
            self.array(&column.to_data(), &mut registry, &mut leases)?;
        }
        Ok(BatchLease {
            _charges: leases,
            _metadata: Some(metadata),
        })
    }

    fn array(
        &self,
        data: &ArrayData,
        registry: &mut BufferRegistry,
        leases: &mut Vec<Arc<BufferCharge>>,
    ) -> DfResult<()> {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|nulls| nulls.buffer()))
        {
            let key = buffer.data_ptr().as_ptr() as usize;
            let size = charged_size(buffer);
            if let Some(charge) = registry.charges.get(&key).and_then(Weak::upgrade) {
                let recorded = charge.reservation.size();
                if size > recorded {
                    charge.reservation.try_grow(size - recorded)?;
                }
                leases.push(charge);
            } else {
                let slots = registry
                    .charges
                    .len()
                    .saturating_add(1)
                    .checked_next_power_of_two()
                    .unwrap_or(usize::MAX);
                let target = slots
                    .saturating_mul(2)
                    .saturating_mul(std::mem::size_of::<(usize, Weak<BufferCharge>)>() + 1);
                if target > self.registry_memory.size() {
                    self.registry_memory
                        .try_grow(target - self.registry_memory.size())?;
                }
                let charge = Arc::new(BufferCharge {
                    _buffer: buffer.clone(),
                    reservation: self.unrecorded("graph Arrow allocation", size)?,
                });
                registry.charges.insert(key, Arc::downgrade(&charge));
                leases.push(charge);
            }
        }
        for child in data.child_data() {
            self.array(child, registry, leases)?;
        }
        Ok(())
    }
}

type WorkCheckpoint = Arc<Mutex<HashMap<std::thread::ThreadId, Box<dyn FnOnce() + Send>>>>;

struct WorkerCheckpoint {
    callbacks: WorkCheckpoint,
    worker: std::thread::ThreadId,
}

impl Drop for WorkerCheckpoint {
    fn drop(&mut self) {
        locked(&self.callbacks).remove(&self.worker);
    }
}

pub(in crate::engine) struct WorkMemory {
    pub(in crate::engine) ctx: Arc<TaskContext>,
    resources: Arc<QueryResources>,
    scratch: MemoryReservation,
    name: String,
    metrics: ExecutionPlanMetricsSet,
    metric_counts: Arc<Mutex<HashMap<&'static str, Count>>>,
    checkpoint: WorkCheckpoint,
    batches: Mutex<Vec<BatchLease>>,
    cancelled: Arc<QueryCancellation>,
}

impl WorkMemory {
    pub(in crate::engine) fn new(ctx: Arc<TaskContext>, name: &str) -> DfResult<Self> {
        let resources = ctx
            .session_config()
            .get_extension::<QueryResources>()
            .ok_or_else(|| {
                DataFusionError::Internal("graph operator has no query resource context".into())
            })?;
        let scratch = resources.reservation(name, 0)?;
        Ok(Self {
            ctx,
            resources,
            scratch,
            name: name.to_owned(),
            metrics: ExecutionPlanMetricsSet::new(),
            metric_counts: Arc::new(Mutex::new(HashMap::new())),
            checkpoint: Arc::new(Mutex::new(HashMap::new())),
            batches: Mutex::new(Vec::new()),
            cancelled: Arc::new(QueryCancellation::default()),
        })
    }

    /// Run each CPU poll on a blocking worker, releasing that thread on Pending.
    /// Retain wakes that race a poll. Dropping aborts queued jobs and signals
    /// running CPU chunks to stop cooperatively.
    pub(in crate::engine) async fn blocking<T, F>(
        self: &Arc<Self>,
        body: impl FnOnce(Arc<Self>) -> F + Send + 'static,
    ) -> DfResult<T>
    where
        T: Send + 'static,
        F: std::future::Future<Output = DfResult<T>> + Send + 'static,
    {
        let cancellation = self.cancel_on_drop();
        let memory = Arc::clone(self);
        let io_probes = crate::instrumentation::capture_query_io_probes();
        let memory_probes = self.resources.probes.clone();
        let mut work = Box::pin(async move {
            let body = body(memory);
            let observed = async move {
                match io_probes {
                    Some(probes) => {
                        crate::instrumentation::with_query_io_probes(probes, body).await
                    }
                    None => body.await,
                }
            };
            match memory_probes {
                Some(probes) => {
                    crate::instrumentation::with_query_memory_probes(probes, observed).await
                }
                None => observed.await,
            }
        });
        let wake = Arc::new(BlockingWake::default());
        let elapsed = MetricBuilder::new(&self.metrics).elapsed_compute(0);
        loop {
            self.check()?;
            let memory = Arc::clone(self);
            let worker_wake = Arc::clone(&wake);
            let elapsed = elapsed.clone();
            let mut guard = crate::instrumentation::query_blocking_work_guard();
            let job = tokio::task::spawn_blocking(move || {
                guard.started();
                let guard = Arc::new(guard);
                let checkpoint_guard = Arc::clone(&guard);
                let _checkpoint = memory.install_checkpoint(move || checkpoint_guard.checkpoint());
                let waker = futures::task::waker_ref(&worker_wake);
                let mut context = std::task::Context::from_waker(&waker);
                let _timer = elapsed.timer();
                let state = match memory.check() {
                    Ok(()) => work.as_mut().poll(&mut context),
                    Err(error) => std::task::Poll::Ready(Err(error)),
                };
                (work, state)
            });
            let abort = AbortBlockingOnDrop(job.abort_handle());
            let (returned, state) = job.await.map_err(|error| {
                DataFusionError::Execution(format!("graph worker failed: {error}"))
            })?;
            drop(abort);
            work = returned;
            match state {
                std::task::Poll::Ready(result) => {
                    cancellation.disarm();
                    return result;
                }
                std::task::Poll::Pending => wake.notify.notified().await,
            }
        }
    }

    pub(in crate::engine) fn set_metrics(&mut self, metrics: ExecutionPlanMetricsSet) {
        self.metrics = metrics;
    }

    pub(in crate::engine) fn metric(&self, name: &'static str, value: usize) {
        locked(&self.metric_counts)
            .entry(name)
            .or_insert_with(|| MetricBuilder::new(&self.metrics).counter(name, 0))
            .add(value);
    }

    pub(in crate::engine) fn error(&self, error: DataFusionError) -> OmniError {
        crate::engine::context::classify_failure(
            error,
            self.resources.pool.as_ref(),
            self.resources.limit,
            self.ctx
                .runtime_env()
                .disk_manager
                .max_temp_directory_size(),
        )
    }

    fn install_checkpoint(&self, callback: impl FnOnce() + Send + 'static) -> WorkerCheckpoint {
        let worker = std::thread::current().id();
        locked(&self.checkpoint).insert(worker, Box::new(callback));
        WorkerCheckpoint {
            callbacks: Arc::clone(&self.checkpoint),
            worker,
        }
    }

    pub(in crate::engine) fn checkpoint(&self) -> DfResult<()> {
        let callback = locked(&self.checkpoint).remove(&std::thread::current().id());
        if let Some(callback) = callback {
            callback();
        }
        self.check()
    }

    pub(in crate::engine) fn filter(
        &self,
        batch: &RecordBatch,
        mask: &arrow_array::BooleanArray,
    ) -> DfResult<RecordBatch> {
        self.entries::<u32>(mask.len())?;
        let indices = UInt32Array::from_iter_values(
            mask.iter()
                .enumerate()
                .filter_map(|(i, selected)| (selected == Some(true)).then_some(i as u32)),
        );
        self.take(batch, &indices)
    }

    pub(in crate::engine) fn child(&self, name: &str) -> DfResult<Self> {
        Ok(Self {
            ctx: Arc::clone(&self.ctx),
            resources: Arc::clone(&self.resources),
            scratch: self.resources.reservation(name, 0)?,
            name: name.to_owned(),
            metrics: self.metrics.clone(),
            metric_counts: Arc::clone(&self.metric_counts),
            checkpoint: Arc::clone(&self.checkpoint),
            batches: Mutex::new(Vec::new()),
            cancelled: Arc::clone(&self.cancelled),
        })
    }

    /// Target bytes per producer batch, leaving room for queued batches and consumers.
    pub(in crate::engine) fn batch_bytes(&self) -> usize {
        (self.resources.limit / 32).clamp(1, 1024 * 1024) as usize
    }

    pub(in crate::engine) fn check(&self) -> DfResult<()> {
        if self.cancelled.stopped.load(Ordering::Relaxed) {
            return Err(DataFusionError::Execution("query cancelled".into()));
        }
        Ok(())
    }

    pub(in crate::engine) fn cancel_on_drop(&self) -> CancelOnDrop {
        CancelOnDrop(Some(Arc::clone(&self.cancelled)))
    }

    pub(in crate::engine) fn grow(&self, bytes: usize) -> DfResult<()> {
        self.check()?;
        self.scratch
            .try_grow(bytes)
            .inspect_err(|_| self.resources.refused(&self.name))
    }

    /// Reserve conservative capacity for collection entries, including element
    /// storage, growth and hash-table overhead.
    pub(in crate::engine) fn entries<T>(&self, count: usize) -> DfResult<()> {
        self.grow(
            count
                .saturating_mul(std::mem::size_of::<T>().saturating_add(1))
                .saturating_mul(4),
        )
    }

    pub(in crate::engine) fn string(&self, bytes: usize) -> DfResult<()> {
        self.grow(bytes.saturating_mul(2).saturating_add(32))
    }

    pub(in crate::engine) fn hold(&self, batch: &RecordBatch) -> DfResult<()> {
        self.check()?;
        let lease = self
            .resources
            .batch(batch)
            .inspect_err(|_| self.resources.refused(&self.name))?;
        locked(&self.batches).push(lease);
        Ok(())
    }

    pub(in crate::engine) fn release_work(&self) {
        locked(&self.batches).clear();
        self.scratch.free();
    }

    pub(in crate::engine) fn output(&self, batch: &RecordBatch) -> DfResult<()> {
        let lease = self
            .resources
            .batch(batch)
            .inspect_err(|_| self.resources.refused(&self.name))?;
        *locked(&self.batches) = vec![lease];
        self.scratch.free();
        Ok(())
    }

    /// A Lance read's batches, held. The rows are the operator's own read, not
    /// its child's, so `input_rows` is not counted (`operators::drain` does).
    pub(in crate::engine) async fn collect(
        &self,
        mut stream: SendableRecordBatchStream,
    ) -> DfResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.hold(&batch)?;
            self.entries::<RecordBatch>(1)?;
            batches.push(batch);
        }
        Ok(batches)
    }

    pub(in crate::engine) fn concat(
        &self,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> DfResult<RecordBatch> {
        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }
        let bytes = batches
            .iter()
            .try_fold(0usize, |sum, batch| -> DfResult<usize> {
                batch.columns().iter().try_fold(sum, |sum, column| {
                    Ok(sum
                        .saturating_add(slice_memory_size(&column.to_data(), 0, column.len())?)
                        .saturating_add(128))
                })
            })?;
        let admitted = self
            .resources
            .reservation("graph concat output", bytes.saturating_mul(2))?;
        let batch = arrow_select::concat::concat_batches(schema, batches)?;
        drop(admitted);
        self.hold(&batch)?;
        Ok(batch)
    }

    pub(in crate::engine) fn stream(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<(Arc<dyn ExecutionPlan>, SendableRecordBatchStream)> {
        let input_limit = crate::table_store::sort_input_batch_bytes(self.resources.limit);
        let plan = plan
            .transform_down(|node| {
                if node.downcast_ref::<SortExec>().is_some() {
                    let children = node
                        .children()
                        .into_iter()
                        .map(|child| {
                            Arc::new(HardCapBatchSizeExec::new(Arc::clone(child), input_limit))
                                as Arc<dyn ExecutionPlan>
                        })
                        .collect();
                    Ok(Transformed::yes(node.with_new_children(children)?))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        let plan: Arc<dyn ExecutionPlan> = if plan.output_partitioning().partition_count() == 1 {
            plan
        } else if let Some(ordering) = plan.output_ordering() {
            Arc::new(SortPreservingMergeExec::new(ordering.clone(), plan))
        } else {
            Arc::new(CoalescePartitionsExec::new(plan))
        };
        let stream = plan.execute(0, Arc::clone(&self.ctx))?;
        Ok((plan, stream))
    }

    pub(in crate::engine) fn slice_bytes(
        &self,
        batch: &RecordBatch,
        offset: usize,
        len: usize,
    ) -> DfResult<usize> {
        batch.columns().iter().try_fold(0usize, |bytes, column| {
            Ok(bytes.saturating_add(slice_memory_size(&column.to_data(), offset, len)?))
        })
    }

    pub(in crate::engine) fn take(
        &self,
        batch: &RecordBatch,
        indices: &UInt32Array,
    ) -> DfResult<RecordBatch> {
        self.take_named(batch, indices, "graph take output")
    }

    /// The rows of `batch` at `indices` (a reordering, or its first rows under
    /// a fetch): the copy is admitted at the input's size while it is built,
    /// then only the result is held.
    pub(in crate::engine) fn permute(
        &self,
        batch: &RecordBatch,
        indices: &UInt32Array,
    ) -> DfResult<RecordBatch> {
        let admitted = self.resources.reservation(
            "sort permute",
            batch.get_array_memory_size().saturating_add(128),
        )?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow_select::take::take(column.as_ref(), indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let result = RecordBatch::try_new(batch.schema(), columns)?;
        drop(admitted);
        self.hold(&result)?;
        Ok(result)
    }

    /// `take` with the admission reservation named `name`, the refusal an
    /// operator reports for its own output.
    pub(in crate::engine) fn take_named(
        &self,
        batch: &RecordBatch,
        indices: &UInt32Array,
        name: &str,
    ) -> DfResult<RecordBatch> {
        self.take_admitted(batch, indices, name, 2)
    }

    /// `take_named` admitting the picked bytes once, for a join's output rows:
    /// the admission covers the copy while it is built; the holds the output
    /// then carries (the join's, the queue's, the consumer's) follow it.
    pub(in crate::engine) fn take_once(
        &self,
        batch: &RecordBatch,
        indices: &UInt32Array,
        name: &str,
    ) -> DfResult<RecordBatch> {
        self.take_admitted(batch, indices, name, 1)
    }

    fn take_admitted(
        &self,
        batch: &RecordBatch,
        indices: &UInt32Array,
        name: &str,
        factor: usize,
    ) -> DfResult<RecordBatch> {
        let mut bytes = 0usize;
        let mut scratch_bytes = 0usize;
        for column in batch.columns() {
            let data = column.to_data();
            scratch_bytes = scratch_bytes.saturating_add(take_scratch_size(&data, indices.len()));
            self.check()?;
            let picked = match flat_take_size(&data, indices) {
                Some(flat) => flat,
                None => {
                    let mut picked = 0usize;
                    for row in indices.values() {
                        self.check()?;
                        picked = picked
                            .saturating_add(slice_memory_size(&data, *row as usize, 1)?)
                            .saturating_add(16);
                    }
                    picked
                }
            };
            bytes = bytes.saturating_add(picked).saturating_add(128);
        }
        let admitted = self.resources.reservation(
            name,
            bytes.saturating_mul(factor).saturating_add(scratch_bytes),
        )?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow_select::take::take(column.as_ref(), indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let result = RecordBatch::try_new(batch.schema(), columns)?;
        drop(admitted);
        self.hold(&result)?;
        Ok(result)
    }
}

/// An upper bound on the picked bytes of a flat or fixed-size-list column
/// without a slice per row: 16 per row, 2 more when nullable, the row's own
/// bytes and offset. `None` for the variable nested and dictionary kinds.
fn flat_take_size(data: &ArrayData, indices: &UInt32Array) -> Option<usize> {
    let rows = indices.len();
    let per_row = if data.nulls().is_some() { 18 } else { 16 };
    let base = data.offset();
    let values = match data.data_type() {
        DataType::FixedSizeList(field, width) if data.child_data()[0].nulls().is_none() => rows
            .saturating_mul(*width as usize)
            .saturating_mul(field.data_type().primitive_width()?),
        DataType::Utf8 | DataType::Binary => {
            let offsets = data.buffers()[0].typed_data::<i32>();
            indices.values().iter().fold(0usize, |sum, &row| {
                let row = base + row as usize;
                sum.saturating_add((offsets[row + 1] - offsets[row]) as usize)
                    .saturating_add(4)
            })
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = data.buffers()[0].typed_data::<i64>();
            indices.values().iter().fold(0usize, |sum, &row| {
                let row = base + row as usize;
                sum.saturating_add((offsets[row + 1] - offsets[row]) as usize)
                    .saturating_add(8)
            })
        }
        DataType::Boolean => rows,
        DataType::Null => 0,
        other => rows.saturating_mul(other.primitive_width()?),
    };
    Some(values.saturating_add(rows.saturating_mul(per_row)))
}

/// Arrow 58.3 take builds fixed-list child indices and preallocates list
/// children using their average cardinality, even for short selected rows.
fn take_scratch_size(data: &ArrayData, rows: usize) -> usize {
    fn initial_size(data_type: &DataType, capacity: usize) -> usize {
        let buffers = match data_type {
            DataType::Null => 0,
            DataType::Boolean => capacity.div_ceil(8),
            DataType::Utf8 | DataType::Binary => capacity
                .saturating_add(1)
                .saturating_mul(4)
                .saturating_add(capacity),
            DataType::LargeUtf8 | DataType::LargeBinary => capacity
                .saturating_add(1)
                .saturating_mul(8)
                .saturating_add(capacity),
            DataType::List(field) | DataType::Map(field, _) => capacity
                .saturating_add(1)
                .saturating_mul(4)
                .saturating_add(initial_size(field.data_type(), capacity)),
            DataType::LargeList(field) => capacity
                .saturating_add(1)
                .saturating_mul(8)
                .saturating_add(initial_size(field.data_type(), capacity)),
            DataType::FixedSizeList(field, width) => {
                initial_size(field.data_type(), capacity.saturating_mul(*width as usize))
            }
            DataType::Struct(fields) => fields.iter().fold(0usize, |size, field| {
                size.saturating_add(initial_size(field.data_type(), capacity))
            }),
            DataType::Dictionary(key, _) => {
                capacity.saturating_mul(key.primitive_width().unwrap_or(16))
            }
            DataType::FixedSizeBinary(width) => capacity.saturating_mul(*width as usize),
            _ => capacity.saturating_mul(data_type.primitive_width().unwrap_or(16)),
        };
        buffers
            .saturating_add(capacity.div_ceil(8))
            .saturating_add(128)
    }
    match data.data_type() {
        DataType::FixedSizeList(_, width) => {
            let children = rows.saturating_mul(*width as usize);
            children
                .saturating_mul(4)
                .saturating_add(take_scratch_size(&data.child_data()[0], children))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
            let child = &data.child_data()[0];
            let capacity = child
                .len()
                .checked_div(data.len())
                .unwrap_or(0)
                .saturating_mul(rows);
            initial_size(child.data_type(), capacity)
        }
        DataType::Struct(_) => data.child_data().iter().fold(0usize, |size, child| {
            size.saturating_add(take_scratch_size(child, rows))
        }),
        _ => 0,
    }
}

/// Bytes copied from an Arrow range, including nested child ranges. Arrow's
/// ArrayData slice keeps list child buffers whole, so recursing through its
/// get_slice_memory_size would count the entire vector column per picked row.
fn slice_memory_size(data: &ArrayData, offset: usize, len: usize) -> DfResult<usize> {
    let data = data.slice(offset, len);
    let validity = if data.nulls().is_some() {
        len.div_ceil(8)
    } else {
        0
    };
    let bytes = match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let offsets = data.buffers()[0].typed_data::<i32>();
            let start = offsets[data.offset()] as usize;
            let end = offsets[data.offset() + len] as usize;
            (len + 1)
                .saturating_mul(4)
                .saturating_add(slice_memory_size(
                    &data.child_data()[0],
                    start,
                    end - start,
                )?)
        }
        DataType::LargeList(_) => {
            let offsets = data.buffers()[0].typed_data::<i64>();
            let start = offsets[data.offset()] as usize;
            let end = offsets[data.offset() + len] as usize;
            (len + 1)
                .saturating_mul(8)
                .saturating_add(slice_memory_size(
                    &data.child_data()[0],
                    start,
                    end - start,
                )?)
        }
        DataType::FixedSizeList(_, width) => {
            let width = *width as usize;
            slice_memory_size(&data.child_data()[0], data.offset() * width, len * width)?
        }
        DataType::Struct(_) => data.child_data().iter().try_fold(0usize, |size, child| {
            Ok::<_, DataFusionError>(size.saturating_add(slice_memory_size(child, 0, len)?))
        })?,
        _ => return Ok(data.get_slice_memory_size()?),
    };
    Ok(bytes.saturating_add(validity))
}

#[derive(Default)]
struct BlockingWake {
    notify: tokio::sync::Notify,
}
impl futures::task::ArcWake for BlockingWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.notify.notify_one();
    }
}

struct AbortBlockingOnDrop(tokio::task::AbortHandle);
impl Drop for AbortBlockingOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
struct QueryCancellation {
    stopped: AtomicBool,
}

pub(in crate::engine) struct CancelOnDrop(Option<Arc<QueryCancellation>>);
impl CancelOnDrop {
    pub(in crate::engine) fn disarm(mut self) {
        self.0.take();
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancelled) = &self.0 {
            cancelled.stopped.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    /// Reads the pool's reserved bytes between two holds; a case sees rows and errors only.
    #[test]
    fn shared_arrow_slices_reserve_the_backing_allocation_once() {
        for rows in [8_192, 8_193, 524_288] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
            )
            .unwrap();
            let backing_bytes = batch.column(0).to_data().buffers()[0].capacity();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(backing_bytes + 65_536));
            let resources = Arc::new(QueryResources::new(
                Arc::clone(&pool),
                (backing_bytes + 65_536) as u64,
            ));
            let ctx =
                Arc::new(TaskContext::default().with_session_config(
                    SessionConfig::new().with_extension(Arc::clone(&resources)),
                ));
            let producer = WorkMemory::new(Arc::clone(&ctx), "test producer").unwrap();
            let collector = WorkMemory::new(Arc::clone(&ctx), "test collector").unwrap();
            producer.output(&batch).unwrap();
            let slices: Vec<_> = (0..rows)
                .step_by(8_192)
                .map(|offset| batch.slice(offset, (rows - offset).min(8_192)))
                .collect();
            for slice in &slices {
                collector.hold(slice).unwrap();
            }
            assert!(pool.reserved() < backing_bytes + 65_536);
            drop(producer);
            assert!(pool.reserved() >= backing_bytes);
            drop(collector);
            drop(slices);
            drop(batch);
            drop(ctx);
            drop(resources);
            assert_eq!(pool.reserved(), 0);
        }
    }

    /// Reads the pool's reserved bytes after a refusal; the 1 KiB flag column
    /// is charged before the 64 KiB column is refused under the 4 KiB pool.
    #[test]
    fn failed_admission_releases_partial_buffer_leases() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4_096));
        let resources = Arc::new(QueryResources::new(Arc::clone(&pool), 4_096));
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_extension(Arc::clone(&resources))),
        );
        let work = WorkMemory::new(Arc::clone(&ctx), "test refusal").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("flag", DataType::Boolean, false),
                Field::new("n", DataType::Int64, false),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![true; 8_193])),
                Arc::new(Int64Array::from_iter_values(0..8_193)),
            ],
        )
        .unwrap();
        work.hold(&batch.project(&[0]).unwrap())
            .expect("the flag column alone fits the pool");
        work.release_work();
        assert!(matches!(
            work.hold(&batch).unwrap_err().find_root(),
            DataFusionError::ResourcesExhausted(_)
        ));
        drop(work);
        drop(ctx);
        drop(resources);
        assert_eq!(pool.reserved(), 0);
    }
    fn test_memory(limit: usize) -> (Arc<dyn MemoryPool>, Arc<WorkMemory>) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let resources = Arc::new(QueryResources::new(Arc::clone(&pool), limit as u64));
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_extension(resources)),
        );
        let memory = Arc::new(WorkMemory::new(ctx, "test CPU chunks").unwrap());
        (pool, memory)
    }

    /// Concurrent worker checkpoints are instrumentation state invisible to GQT.
    #[test]
    fn concurrent_workers_keep_their_own_checkpoints_through_children() {
        let (_, memory) = test_memory(1_048_576);
        let barrier = std::sync::Barrier::new(2);
        let first = std::sync::atomic::AtomicUsize::new(0);
        let second = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for counter in [&first, &second] {
                let memory = Arc::clone(&memory);
                let barrier = &barrier;
                scope.spawn(move || {
                    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let callback_calls = Arc::clone(&calls);
                    let _checkpoint = memory.install_checkpoint(move || {
                        callback_calls.fetch_add(1, Ordering::Relaxed);
                    });
                    barrier.wait();
                    let child = memory.child("worker child").unwrap();
                    child.checkpoint().unwrap();
                    child.checkpoint().unwrap();
                    counter.store(calls.load(Ordering::Relaxed), Ordering::Relaxed);
                });
            }
        });
        assert_eq!(first.load(Ordering::Relaxed), 1);
        assert_eq!(second.load(Ordering::Relaxed), 1);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let checkpoint = memory.install_checkpoint(move || {
            callback_calls.fetch_add(1, Ordering::Relaxed);
        });
        drop(checkpoint);
        memory.checkpoint().unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "a finished poll clears its unused callback"
        );
    }

    /// A struct-wrapped vector column under a 1 MiB pool: no schema type nests a vector in a struct.
    #[test]
    fn nested_array_take_and_concat_admit_only_selected_child_ranges() {
        use arrow_array::{FixedSizeListArray, Float32Array, ListArray, StructArray};
        use datafusion::arrow::buffer::OffsetBuffer;
        let rows = 8_193;
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let values: Arc<dyn Array> = Arc::new(Float32Array::from_iter_values(
            (0..rows * 4).map(|n| n as f32),
        ));
        let fixed: Arc<dyn Array> = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            4,
            Arc::clone(&values),
            None,
        ));
        let variable: Arc<dyn Array> = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new((0..=rows).map(|n| n * 4).collect::<Vec<_>>().into()),
            values,
            None,
        ));
        for list in [fixed, variable] {
            let field = Arc::new(Field::new("vector", list.data_type().clone(), false));
            let nested: Arc<dyn Array> = Arc::new(StructArray::from(vec![(field, list)]));
            let schema = Arc::new(Schema::new(vec![Field::new(
                "p",
                nested.data_type().clone(),
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![nested]).unwrap();
            let (pool, memory) = test_memory(1_048_576);
            memory.hold(&batch).unwrap();
            let indices = UInt32Array::from_iter_values(0..rows as u32);
            let taken = memory.take(&batch, &indices).unwrap();
            assert_eq!(taken, batch);
            let pieces = [taken.slice(0, 8_192), taken.slice(8_192, 1)];
            let combined = memory.concat(&batch.schema(), &pieces).unwrap();
            assert_eq!(combined, batch);
            drop(memory);
            assert_eq!(pool.reserved(), 0);
        }
    }

    /// Pins the runtime to one blocking thread; a case cannot size the runtime.
    #[test]
    fn one_blocking_thread_can_poll_nested_work_and_async_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_, memory) = test_memory(1_048_576);
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                memory.blocking(move |memory| async move {
                    memory
                        .blocking(move |_| async move {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            tokio::task::spawn_blocking(|| 42usize)
                                .await
                                .map_err(|error| DataFusionError::Execution(error.to_string()))
                        })
                        .await
                }),
            )
            .await
            .expect("one blocking worker must make forward progress")
            .unwrap();
            assert_eq!(result, 42);
        });
    }

    /// Aborts a query task mid-flight and reads the pool; a case cannot cancel a query.
    #[test]
    fn cancelling_pending_work_drops_its_future_and_reservations() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (pool, memory) = test_memory(1_048_576);
            let (started, ready) = tokio::sync::oneshot::channel();
            let query_memory = Arc::clone(&memory);
            let query = tokio::spawn(async move {
                query_memory
                    .blocking(move |memory| async move {
                        memory.grow(4_096)?;
                        let _ = started.send(());
                        std::future::pending::<()>().await;
                        Ok(())
                    })
                    .await
            });
            ready.await.unwrap();
            drop(memory);
            query.abort();
            let _ = query.await;
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while pool.reserved() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled pending chunk retained its future");
        });
    }
}
