//! `HydrateExec`: a `HydrateColumns` node. The rows that reached the output
//! carry each deferred binding's row address (the `^binding` column the
//! return projection below carries); this operator takes the deferred
//! columns of those rows from the binding's pinned table by address, one
//! chunk at a time under `hydrate_chunk_bytes`, and emits the return columns
//! in return order without the addresses. The bound covers what Lance may
//! read ahead of the decoder (a quarter, reserved in the pool), the decoded
//! rows, each charged before the fetch waits for the next, and their copies,
//! one per output row, which the pool admits before Arrow builds them.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array, new_null_array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::fragment::FragReadConfig;
use lance::datatypes::Schema as LanceSchema;
use lance_core::ROW_ADDR;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_planner::{HydratedBinding, ROW_ADDRESS_PREFIX, hydrate_chunk_bytes};

use super::memory::WorkMemory;
use super::producer::{BatchSender, producer_stream};
use super::{external, polled, streaming_properties};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};

/// The pool owner of one hydrated chunk: the fetched rows, then the output.
const CHUNK: &str = "hydrate chunk";

/// The admission of a chunk's copied values while Arrow builds them.
const OUTPUT: &str = "hydrate output";

/// The admission of a decoded row's copy at its own size: Lance's rows can
/// slice a buffer of the whole read.
const FETCH: &str = "hydrate fetch";

/// The pool owner of what Lance may read ahead of the decoder.
const READ_AHEAD: &str = "hydrate read-ahead";

/// Rows a fetch decodes at once, each a one-row task: the rows decoded but
/// not yet charged.
const ROWS_IN_FLIGHT: usize = 8;

/// Fragments whose reads a fetch schedules ahead of the one it drains.
const FRAGMENTS_AHEAD: usize = 4;

/// The counter of rows read one per request, from fragments holding a file
/// Lance reaches through a `base_id`.
const SINGLE_ROW_READS: &str = "single_row_reads";

/// The gauge of the most bytes one chunk held at once: its decoded rows and
/// their copies. With the read-ahead it stays under `hydrate_chunk_bytes`
/// and one row.
const PEAK_CHUNK_BYTES: &str = "peak_chunk_bytes";

/// Rows of the first chunk, before a hydrated row's width is measured.
const SEED_ROWS: usize = 4;

pub(crate) struct HydrateExec {
    input: Arc<dyn ExecutionPlan>,
    bindings: Vec<HydratedBinding>,
    /// Each binding's declared node schema, which the pinned table's stored
    /// fields must match for every column taken from it.
    tables: Vec<SchemaRef>,
    layout: Vec<Slot>,
    snapshot: Snapshot,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// Where one output column comes from: an input column (by index), or the
/// `column`th deferred column of the `binding`th hydrated binding.
#[derive(Debug, Clone, Copy)]
enum Slot {
    Input(usize),
    Hydrated { binding: usize, column: usize },
}

impl HydrateExec {
    /// The output is the return order: a deferred column sits at its return
    /// position under its return name with the field `declared` gives it,
    /// as the projection would have carried it; the input's other columns
    /// fill the remaining positions in their order, and the row addresses
    /// leave.
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        bindings: Vec<HydratedBinding>,
        snapshot: Snapshot,
        catalog: &Catalog,
        declared: &Schema,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut deferred: HashMap<usize, (Slot, Field)> = HashMap::new();
        let mut tables = Vec::with_capacity(bindings.len());
        for (index, binding) in bindings.iter().enumerate() {
            let address = address_column(&binding.binding);
            if input_schema.column_with_name(&address).is_none() {
                return Err(OmniError::manifest_internal(format!(
                    "the rows reaching `HydrateColumns` carry no row address for `${}`",
                    binding.binding
                )));
            }
            let node_type = binding
                .table
                .node_type_name()
                .and_then(|type_name| catalog.node_types.get(type_name))
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "`HydrateColumns` reads `{}`, which is no node type",
                        binding.table.type_key
                    ))
                })?;
            tables.push(Arc::clone(&node_type.arrow_schema));
            for (column_index, column) in binding.columns.iter().enumerate() {
                if node_type
                    .arrow_schema
                    .field_with_name(&column.property)
                    .is_err()
                {
                    return Err(OmniError::manifest_internal(format!(
                        "`{}` has no property `{}` to hydrate",
                        binding.table.type_key, column.property
                    )));
                }
                let field = declared.field_with_name(&column.output).map_err(|_| {
                    OmniError::manifest_internal(format!(
                        "`HydrateColumns` declares no return column `{}`",
                        column.output
                    ))
                })?;
                let slot = Slot::Hydrated {
                    binding: index,
                    column: column_index,
                };
                let field = field.clone();
                if deferred.insert(column.position, (slot, field)).is_some() {
                    return Err(OmniError::manifest_internal(format!(
                        "two hydrated columns fill return position {}",
                        column.position
                    )));
                }
            }
        }
        let mut carried = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !field.name().starts_with(ROW_ADDRESS_PREFIX));
        let width = carried.clone().count() + deferred.len();
        let mut layout = Vec::with_capacity(width);
        let mut fields = Vec::with_capacity(width);
        for position in 0..width {
            let (slot, field) = match deferred.remove(&position) {
                Some(hydrated) => hydrated,
                None => {
                    let (index, field) = carried.next().ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "`HydrateColumns` has no input column for return position {position}"
                        ))
                    })?;
                    (Slot::Input(index), field.as_ref().clone())
                }
            };
            layout.push(slot);
            fields.push(field);
        }
        if let Some(position) = deferred.keys().min() {
            return Err(OmniError::manifest_internal(format!(
                "a hydrated column fills return position {position}, past the return's {width}"
            )));
        }
        let properties = streaming_properties(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            bindings,
            tables,
            layout,
            snapshot,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The input column holding `binding`'s row address.
fn address_column(binding: &str) -> String {
    format!("{ROW_ADDRESS_PREFIX}{binding}")
}

impl fmt::Debug for HydrateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HydrateExec")
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for HydrateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bindings: Vec<String> = self
            .bindings
            .iter()
            .map(|binding| {
                let columns: Vec<&str> = binding
                    .columns
                    .iter()
                    .map(|column| column.property.as_str())
                    .collect();
                format!("${}: [{}]", binding.binding, columns.join(", "))
            })
            .collect();
        write!(f, "HydrateExec: {}", bindings.join(", "))
    }
}

impl ExecutionPlan for HydrateExec {
    fn name(&self) -> &str {
        "HydrateExec"
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
        assert_eq!(children.len(), 1, "HydrateExec has one child");
        Ok(Arc::new(Self {
            input: children.pop().expect("one child"),
            bindings: self.bindings.clone(),
            tables: self.tables.clone(),
            layout: self.layout.clone(),
            snapshot: self.snapshot.clone(),
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "HydrateExec has one partition");
        let schema: SchemaRef = self.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let mut work = WorkMemory::new(ctx, "HydrateExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        work.metric("hydrated_rows", 0);
        work.metric(SINGLE_ROW_READS, 0);
        let plan = Hydration {
            layout: self.layout.clone(),
            bindings: self.bindings.clone(),
            tables: self.tables.clone(),
            snapshot: self.snapshot.clone(),
            schema: Arc::clone(&schema),
            peak: MetricBuilder::new(&self.metrics).gauge(PEAK_CHUNK_BYTES, 0),
        };
        let stream = producer_stream(
            schema,
            Arc::new(work),
            Some(&self.metrics),
            move |memory, sender| async move { hydrate(input, plan, &memory, &sender).await },
        );
        Ok(polled(&self.metrics, stream))
    }
}

/// What one execution hydrates, moved into its producer.
struct Hydration {
    layout: Vec<Slot>,
    bindings: Vec<HydratedBinding>,
    tables: Vec<SchemaRef>,
    snapshot: Snapshot,
    schema: SchemaRef,
    peak: Gauge,
}

/// One deferred binding's pinned table and the columns taken from it.
struct Source {
    binding: String,
    address: String,
    dataset: Arc<Dataset>,
    /// The deferred properties' stored fields, which each fragment reader
    /// projects.
    schema: Arc<LanceSchema>,
    /// Caps how far Lance reads ahead of the decoder in the table's own base.
    scheduler: Arc<ScanScheduler>,
    /// The fragments holding a data file Lance reaches through a `base_id`
    /// (a Lance branch's inherited files, which only a table fork created
    /// before v11 has), read one row per request.
    inherited: Arc<HashSet<u32>>,
    /// `(return name, property)` for each deferred return item.
    columns: Vec<(String, String)>,
    /// The deferred columns under their return names.
    values: SchemaRef,
}

async fn hydrate(
    mut input: SendableRecordBatchStream,
    plan: Hydration,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
) -> DfResult<()> {
    let hard = usize::try_from(hydrate_chunk_bytes(memory.pool_bytes())).unwrap_or(usize::MAX);
    // A quarter of the bound is what Lance may read ahead of the decoder; the
    // rest holds a chunk's decoded rows and their copies, half of it the rows.
    let read_ahead = (hard / 4).max(1);
    let budget = hard.saturating_sub(read_ahead).max(1);
    let target = (budget / 2).max(1);
    let reserved = memory.child(READ_AHEAD)?;
    reserved.grow(read_ahead)?;
    let mut sources = Vec::with_capacity(plan.bindings.len());
    for (binding, declared) in plan.bindings.iter().zip(&plan.tables) {
        let storage = |error| {
            external(OmniError::storage_context(
                format!("hydrating the return columns of `${}`", binding.binding),
                error,
            ))
        };
        let dataset = Arc::new(
            plan.snapshot
                .open_lance_dataset(&binding.table.type_key)
                .await
                .map_err(external)?,
        );
        let mut properties: Vec<&str> = Vec::new();
        for column in &binding.columns {
            if !properties.contains(&column.property.as_str()) {
                properties.push(&column.property);
            }
        }
        crate::engine::typed_value::check_stored_schema(
            &dataset,
            declared,
            &binding.table.type_key,
            properties.iter().copied(),
        )
        .map_err(external)?;
        let schema = dataset.schema().project(&properties).map_err(storage)?;
        let store = dataset.object_store(None).await.map_err(storage)?;
        let inherited = dataset
            .get_fragments()
            .iter()
            .filter(|fragment| {
                fragment
                    .metadata()
                    .files
                    .iter()
                    .any(|file| file.base_id.is_some())
            })
            .map(|fragment| fragment.id() as u32)
            .collect();
        let values = binding
            .columns
            .iter()
            .map(|column| {
                let field = plan.schema.field_with_name(&column.output)?;
                Ok(Field::new(&column.output, field.data_type().clone(), true))
            })
            .collect::<DfResult<Vec<Field>>>()?;
        sources.push(Source {
            binding: binding.binding.clone(),
            address: address_column(&binding.binding),
            dataset,
            schema: Arc::new(schema),
            scheduler: ScanScheduler::new(store, SchedulerConfig::new(read_ahead as u64)),
            inherited: Arc::new(inherited),
            columns: binding
                .columns
                .iter()
                .map(|column| (column.output.clone(), column.property.clone()))
                .collect(),
            values: Arc::new(Schema::new(values)),
        });
    }
    // The costliest output row seen so far, its decoded row and its copy,
    // plans the next window's rows.
    let mut row_bytes = 0usize;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        memory.metric("input_rows", batch.num_rows());
        let mut start = 0;
        while start < batch.num_rows() {
            let planned = target
                .checked_div(row_bytes)
                .map_or(SEED_ROWS, |rows| rows.max(1));
            let mut rows = planned.min(batch.num_rows() - start);
            loop {
                memory.check()?;
                let window = batch.slice(start, rows);
                let chunk = Arc::new(memory.child(CHUNK)?);
                let mut fetched_bytes = 0usize;
                let fetched = match fetch_window(
                    &window,
                    &sources,
                    &chunk,
                    (rows > 1).then_some(target),
                    &mut fetched_bytes,
                    &plan.peak,
                )
                .await?
                {
                    Fetch::Complete(fetched) => fetched,
                    // The window's rows are wider than the rows that planned
                    // it: plan again from the widest row decoded.
                    Fetch::Stopped { widest } => {
                        let widest = widest.max(1);
                        row_bytes = row_bytes.max(widest.saturating_mul(2));
                        rows = (target / widest).clamp(1, rows.div_ceil(2));
                        continue;
                    }
                };
                // A row a join repeats is copied once per output row, so the
                // window keeps a prefix, halved until the pool's estimate of
                // its copies fits beside the decoded rows; the rest is the
                // next window.
                let mut keep = rows;
                let mut copied = copy_bytes(&fetched, keep, &chunk)?;
                while keep > 1 && fetched_bytes.saturating_add(copied) > budget {
                    keep = keep.div_ceil(2);
                    copied = copy_bytes(&fetched, keep, &chunk)?;
                }
                row_bytes = row_bytes
                    .max(fetched_bytes.saturating_add(copied) / keep)
                    .max(1);
                let taken = fetched
                    .iter()
                    .zip(&sources)
                    .map(|(fetched, source)| {
                        chunk
                            .interleave_once(
                                &source.values,
                                &fetched.rows,
                                &fetched.picks[..keep],
                                OUTPUT,
                            )
                            .map(|values| values.columns().to_vec())
                    })
                    .collect::<DfResult<Vec<_>>>()?;
                let copies = taken
                    .iter()
                    .flatten()
                    .map(|column| column.get_array_memory_size())
                    .sum::<usize>();
                plan.peak.set_max(fetched_bytes.saturating_add(copies));
                drop(fetched);
                let output = assemble(&window.slice(0, keep), &taken, &plan)?;
                chunk.output(&output)?;
                memory.metric("hydrated_rows", keep);
                sender.send(output, chunk).await?;
                start += keep;
                break;
            }
        }
    }
    drop(reserved);
    Ok(())
}

/// One source's rows for a window: `rows` holds the deferred columns of each
/// distinct row Lance decoded, one row per batch and each held by the chunk,
/// and a null row last when an address is null; `picks` names each window
/// row's `(batch, row)`.
struct Fetched {
    rows: Vec<RecordBatch>,
    picks: Vec<(usize, usize)>,
}

/// How a window's fetch ended.
enum Fetch {
    /// Every source's rows.
    Complete(Vec<Fetched>),
    /// The decoded rows passed the window's share of the bound first; the
    /// widest of them held `widest` bytes.
    Stopped { widest: usize },
}

/// What `interleave_once` admits to copy the first `rows` rows of the window
/// from every source.
fn copy_bytes(fetched: &[Fetched], rows: usize, chunk: &WorkMemory) -> DfResult<usize> {
    fetched.iter().try_fold(0usize, |sum, source| {
        Ok(sum.saturating_add(chunk.interleave_bytes(&source.rows, &source.picks[..rows])?))
    })
}

/// Each source's deferred columns for the distinct rows of `window`. Lance
/// decodes one row per task, and each row is charged to `chunk` before the
/// fetch waits for the next, so once the decoded rows pass `limit` (`None`
/// for a one-row window, which cannot shrink) the fetch stops and drops the
/// read with at most `ROWS_IN_FLIGHT` rows decoded but not charged. A row
/// address the pinned table does not hold is an integrity failure.
async fn fetch_window(
    window: &RecordBatch,
    sources: &[Source],
    chunk: &WorkMemory,
    limit: Option<usize>,
    fetched_bytes: &mut usize,
    peak: &Gauge,
) -> DfResult<Fetch> {
    let mut fetched = Vec::with_capacity(sources.len());
    let mut widest = 0usize;
    for source in sources {
        let addresses = window
            .column_by_name(&source.address)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "`HydrateColumns` input has no UInt64 '{}'",
                    source.address
                ))
            })?;
        let mut unique: Vec<u64> = addresses.iter().flatten().collect();
        unique.sort_unstable();
        unique.dedup();
        chunk.entries::<(u64, (usize, usize))>(unique.len())?;
        let mut rows = Vec::with_capacity(unique.len() + 1);
        let mut positions: HashMap<u64, (usize, usize)> = HashMap::with_capacity(unique.len());
        let mut decoded = source.rows(unique);
        while let Some(batch) = decoded.next().await {
            let batch = batch?;
            let row_addresses = batch
                .column_by_name(ROW_ADDR)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    DataFusionError::Internal("a row-address read returned no _rowaddr".into())
                })?;
            for (row, address) in row_addresses.values().iter().enumerate() {
                positions.insert(*address, (rows.len(), row));
                if source.inherited.contains(&((address >> 32) as u32)) {
                    chunk.metric(SINGLE_ROW_READS, 1);
                }
            }
            let columns = source
                .columns
                .iter()
                .map(|(_, property)| {
                    batch.column_by_name(property).cloned().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "a row-address read returned no '{property}'"
                        ))
                    })
                })
                .collect::<DfResult<Vec<ArrayRef>>>()?;
            let values = RecordBatch::try_new(Arc::clone(&source.values), columns)?;
            // The chunk holds a copy at the row's own size, so a buffer of the
            // whole read the row slices goes back to Lance with the read.
            let all = UInt32Array::from_iter_values(0..values.num_rows() as u32);
            let row = chunk.take_once(&values, &all, FETCH)?;
            drop((values, batch));
            let bytes = row.get_array_memory_size();
            *fetched_bytes = fetched_bytes.saturating_add(bytes);
            widest = widest.max(bytes / row.num_rows().max(1));
            peak.set_max(*fetched_bytes);
            rows.push(row);
            if limit.is_some_and(|limit| *fetched_bytes > limit) {
                return Ok(Fetch::Stopped { widest });
            }
        }
        drop(decoded);
        let mut null_row = None;
        let mut picks = Vec::with_capacity(window.num_rows());
        for address in addresses.iter() {
            let pick = match address {
                Some(address) => positions.get(&address).copied().ok_or_else(|| {
                    external(OmniError::manifest_internal(format!(
                        "row address {address} of `${}` is not in its pinned table",
                        source.binding
                    )))
                })?,
                None => match null_row {
                    Some(pick) => pick,
                    None => {
                        let columns = source
                            .values
                            .fields()
                            .iter()
                            .map(|field| new_null_array(field.data_type(), 1))
                            .collect();
                        let nulls = RecordBatch::try_new(Arc::clone(&source.values), columns)?;
                        chunk.hold(&nulls)?;
                        rows.push(nulls);
                        *null_row.insert((rows.len() - 1, 0))
                    }
                },
            };
            picks.push(pick);
        }
        fetched.push(Fetched { rows, picks });
    }
    Ok(Fetch::Complete(fetched))
}

impl Source {
    /// The deferred columns of the rows at `addresses` (sorted, distinct), in
    /// address order, one row per batch with its `_rowaddr`. The fragments'
    /// reads are scheduled `FRAGMENTS_AHEAD` at a time and at most
    /// `ROWS_IN_FLIGHT` rows decode at once.
    fn rows(&self, addresses: Vec<u64>) -> BoxStream<'static, DfResult<RecordBatch>> {
        let mut fragments: Vec<(u32, Vec<u32>)> = Vec::new();
        for address in addresses {
            let (fragment, offset) = ((address >> 32) as u32, address as u32);
            match fragments.last_mut() {
                Some((last, offsets)) if *last == fragment => offsets.push(offset),
                _ => fragments.push((fragment, vec![offset])),
            }
        }
        let dataset = Arc::clone(&self.dataset);
        let schema = Arc::clone(&self.schema);
        let scheduler = Arc::clone(&self.scheduler);
        let inherited = Arc::clone(&self.inherited);
        let binding = self.binding.clone();
        stream::iter(fragments.into_iter().enumerate())
            .map(move |(priority, (fragment, offsets))| {
                fragment_rows(
                    Arc::clone(&dataset),
                    Arc::clone(&schema),
                    Arc::clone(&scheduler),
                    binding.clone(),
                    u32::try_from(priority).unwrap_or(u32::MAX),
                    inherited.contains(&fragment),
                    fragment,
                    offsets,
                )
            })
            .buffered(FRAGMENTS_AHEAD)
            .try_flatten()
            .map(|task| async move { task?.await })
            .buffered(ROWS_IN_FLIGHT)
            .boxed()
    }
}

/// A decode task of one fragment row.
type RowTask = BoxFuture<'static, DfResult<RecordBatch>>;

/// The one-row decode tasks of `offsets`, strictly increasing physical
/// offsets in `fragment`. A fragment whose data files are all in the
/// table's own base is read in one request, which Lance coalesces, through
/// `scheduler`, which bounds its read-ahead. Lance 11 gives a data file in
/// another base (the files a Lance branch inherits) its own maximum-bandwidth
/// scheduler and ignores the one passed in, so an `inherited` fragment is
/// read one row per request and its read-ahead is the rows in flight.
async fn fragment_rows(
    dataset: Arc<Dataset>,
    schema: Arc<LanceSchema>,
    scheduler: Arc<ScanScheduler>,
    binding: String,
    priority: u32,
    inherited: bool,
    fragment: u32,
    offsets: Vec<u32>,
) -> DfResult<BoxStream<'static, DfResult<RowTask>>> {
    let storage = {
        let binding = binding.clone();
        move |error| {
            external(OmniError::storage_context(
                format!("hydrating the return columns of `${binding}`"),
                error,
            ))
        }
    };
    let file = dataset.get_fragment(fragment as usize).ok_or_else(|| {
        external(OmniError::manifest_internal(format!(
            "fragment {fragment} of a row address of `${binding}` is not in its pinned table"
        )))
    })?;
    let reader = file
        .open(
            &schema,
            FragReadConfig::default()
                .with_row_address(true)
                .with_scan_scheduler(scheduler)
                .with_reader_priority(priority),
        )
        .await
        .map_err(storage.clone())?;
    if !inherited {
        let tasks = reader
            .take(&offsets, 1, None)
            .await
            .map_err(storage.clone())?;
        return Ok(tasks
            .map(move |task| Ok(task.map_err(storage.clone()).boxed()))
            .boxed());
    }
    let reader = Arc::new(reader);
    Ok(stream::iter(offsets)
        .map(move |offset| {
            let reader = Arc::clone(&reader);
            let storage = storage.clone();
            Ok(async move {
                let mut tasks = reader
                    .take(&[offset], 1, None)
                    .await
                    .map_err(storage.clone())?;
                match tasks.next().await {
                    Some(task) => task.await.map_err(storage),
                    None => Err(DataFusionError::Internal(format!(
                        "a one-row read of fragment offset {offset} returned no task"
                    ))),
                }
            }
            .boxed())
        })
        .boxed())
}

/// The return columns of `window` in return order, as `plan.layout` places
/// them: a deferred one from its source's taken arrays, any other from the
/// window.
fn assemble(
    window: &RecordBatch,
    taken: &[Vec<ArrayRef>],
    plan: &Hydration,
) -> DfResult<RecordBatch> {
    let columns = plan
        .layout
        .iter()
        .map(|slot| match *slot {
            Slot::Input(index) => Arc::clone(window.column(index)),
            Slot::Hydrated { binding, column } => Arc::clone(&taken[binding][column]),
        })
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(Arc::clone(&plan.schema), columns)?)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatchIterator, StringArray};
    use arrow_schema::DataType;
    use lance::dataset::{WriteMode, WriteParams};
    use lance_file::version::LanceFileVersion;

    use super::*;

    fn rows(ids: &[&str], values: &[i32]) -> impl arrow_array::RecordBatchReader + Send + 'static {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap();
        RecordBatchIterator::new(vec![Ok(batch)], schema)
    }

    fn params(mode: WriteMode) -> WriteParams {
        WriteParams {
            mode,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        }
    }

    /// Both read shapes return every requested row, one per batch with its
    /// address: a Lance branch's inherited fragment (whose file the branch
    /// reaches through a `base_id`) one row per request, and the fragment it
    /// wrote in one request through the passed scheduler. A graph branch never
    /// holds an inherited fragment, so no case reaches the first shape.
    #[tokio::test]
    async fn fragment_rows_reads_inherited_and_own_fragments_by_address() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("rows.lance");
        // forbidden-api-allow: test-only raw Lance table, branched so a fragment reaches its file through a base_id.
        let mut main = Dataset::write(
            rows(&["a", "b", "c"], &[10, 11, 12]),
            uri.to_str().unwrap(),
            Some(params(WriteMode::Create)),
        )
        .await
        .unwrap();
        let mut branch = main
            .create_branch("forked", main.version().version, None)
            .await
            .unwrap();
        branch
            .append(
                rows(&["d", "e"], &[13, 14]),
                Some(params(WriteMode::Append)),
            )
            .await
            .unwrap();
        let store = branch.object_store(None).await.unwrap();
        let schema = Arc::new(branch.schema().project(&["value"]).unwrap());
        let dataset = Arc::new(branch);
        let mut shapes = Vec::new();
        for fragment in dataset.get_fragments() {
            let inherited = fragment
                .metadata()
                .files
                .iter()
                .any(|file| file.base_id.is_some());
            let scheduler = ScanScheduler::new(Arc::clone(&store), SchedulerConfig::new(1 << 20));
            let count = fragment.metadata().physical_rows.unwrap() as u32;
            let tasks = fragment_rows(
                Arc::clone(&dataset),
                Arc::clone(&schema),
                Arc::clone(&scheduler),
                "r".into(),
                0,
                inherited,
                fragment.id() as u32,
                (0..count).collect(),
            )
            .await
            .unwrap();
            let batches: Vec<RecordBatch> =
                tasks.and_then(|task| task).try_collect().await.unwrap();
            assert!(batches.iter().all(|batch| batch.num_rows() == 1));
            let read: Vec<(u64, i32)> = batches
                .iter()
                .map(|batch| {
                    let address = batch
                        .column_by_name(ROW_ADDR)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0);
                    let value = batch
                        .column_by_name("value")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0);
                    (address, value)
                })
                .collect();
            let first = if inherited { 10 } else { 13 };
            let expected: Vec<(u64, i32)> = (0..count)
                .map(|offset| {
                    (
                        ((fragment.id() as u64) << 32) | u64::from(offset),
                        first + offset as i32,
                    )
                })
                .collect();
            assert_eq!(read, expected, "inherited: {inherited}");
            assert_eq!(
                scheduler.stats().bytes_read > 0,
                !inherited,
                "only an own fragment reads through the passed scheduler"
            );
            shapes.push(inherited);
        }
        shapes.sort();
        assert_eq!(shapes, [false, true]);
    }
}
