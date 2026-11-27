//! Destination scans constrained by batches of graph identities: the per-slice
//! id lookup of a dependent `ScanExec`, and the fallback branch of
//! `HashJoinExec`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::engine::operators::LookupSpec;
use crate::engine::operators::memory::{WorkMemory, batch_bytes};
use crate::engine::operators::shape::{BatchSender, Flow, Streamed};
use crate::engine::scan::{
    ScanColumns, SearchColumns, add_null_blob_columns, hconcat_batches, id_in_list_expr,
    ir_expr_to_df_expr,
};
use crate::error::OmniError;
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use futures::future::BoxFuture;

impl ScanExec {
    /// The schema a dependent scan declares: the input's fields without the
    /// destination id, then the prefixed destination fields. Both access
    /// paths produce exactly this.
    pub(in crate::engine) fn input_schema(
        input: &Schema,
        type_name: &str,
        binding: &str,
        projection: Option<&NeededColumns>,
        catalog: &Catalog,
    ) -> Result<SchemaRef> {
        let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "destination scan names unknown type '{type_name}'"
            ))
        })?;
        let columns = ScanColumns::new(node_type, SearchColumns::default(), projection);
        let mut destination = columns.empty_batch(node_type);
        if columns.has_blobs {
            destination = add_null_blob_columns(&destination, node_type)?;
        }
        let id = format!("{binding}.{}", catalog.system_columns.id);
        input
            .index_of(&id)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let destination = prefix_batch(&destination, binding)?;
        let fields = input
            .fields()
            .iter()
            .filter(|field| field.name() != &id)
            .cloned()
            .chain(destination.schema().fields().iter().cloned())
            .collect::<Vec<_>>();
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(super) fn execute_input(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
        shape: &Streaming,
    ) -> DfResult<SendableRecordBatchStream> {
        let memory = work(Arc::clone(&ctx), "ScanExec", &self.metrics)?;
        let lookup = Lookup::new(
            Arc::new(LookupSpec {
                type_name: self.type_name.clone(),
                binding: self.binding.clone(),
                filters: self.filters.clone(),
                projection: self.projection.clone(),
                params: Arc::clone(&self.params),
                snapshot: self.snapshot.clone(),
                catalog: Arc::clone(&self.catalog),
            }),
            self.schema(),
        );
        let input = Arc::clone(input);
        Ok(shape.produce(memory, &self.metrics, move || input.execute(0, ctx), lookup))
    }
}

/// The body of a dependent scan, and of a hash join after its build fell
/// back: input batches gathered into slices, each slice resolved to
/// destination rows by one Lance read `id IN (slice ids)` with the pushed
/// filters, sent in `declared`'s shape, in input order.
///
/// With no index on the identity column every read scans the table, so the
/// number of slices sets the read's storage requests. The first slice holds
/// [`SLICE_FLOOR`] ids; a later one gathers input until it reaches the ids
/// an eighth of the query pool's free room holds, after twice the largest
/// batch a read has returned (its decoded pages, held while its rows are
/// copied out), at [`ID_BYTES`] plus four copies of an input row per id, and
/// never fewer than [`SLICE_FLOOR`]. The room is what the pool has free when
/// the batch arrives, so the operators beside the lookup keep what they hold;
/// the number of reads may differ between runs whose producers ran ahead by
/// different amounts, never the rows.
pub(in crate::engine::operators) struct Lookup {
    spec: Arc<LookupSpec>,
    declared: SchemaRef,
    /// The input gathered for the next slice, held by `held`.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    /// The ids of the next read.
    slice: usize,
    held: Option<WorkMemory>,
    /// The bytes of the largest batch a read returned, 0 before the first.
    read_peak: AtomicUsize,
}

/// The fewest ids a slice gathers before its read.
const SLICE_FLOOR: usize = 256;

/// What a slice charges per id beside its input row: the id's entries in the
/// candidate set, the filter list and the row maps.
const ID_BYTES: usize = 1024;

impl Lookup {
    pub(in crate::engine::operators) fn new(spec: Arc<LookupSpec>, declared: SchemaRef) -> Self {
        Self {
            spec,
            declared,
            pending: Vec::new(),
            pending_rows: 0,
            slice: SLICE_FLOOR,
            held: None,
            read_peak: AtomicUsize::new(0),
        }
    }

    /// Gather `batch` into the next slice, and read the slice once it is full.
    pub(in crate::engine::operators) async fn push(
        &mut self,
        batch: RecordBatch,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> DfResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let row = memory.slice_bytes(&batch, 0, batch.num_rows())? / batch.num_rows();
        let room = match self.read_peak.load(Ordering::Relaxed) {
            0 => 0,
            peak => {
                let free = usize::try_from(memory.free_bytes()).unwrap_or(usize::MAX);
                free.saturating_sub(peak.saturating_mul(2)) / 8
            }
        };
        self.slice = (room / row.saturating_mul(4).saturating_add(ID_BYTES))
            .max(SLICE_FLOOR.min(batch.num_rows()));
        let held = match &self.held {
            Some(held) => held,
            None => self.held.insert(memory.child("scan candidate input")?),
        };
        held.hold(&batch)?;
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        if self.pending_rows >= self.slice {
            self.flush(memory, sender).await?;
        }
        Ok(())
    }

    /// Read the gathered slice, if any.
    pub(in crate::engine::operators) async fn flush(
        &mut self,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> DfResult<()> {
        let Some(gathered) = self.held.take() else {
            return Ok(());
        };
        let pending = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let held = memory.child("scan candidate slice")?;
        let batch = held.concat(&pending[0].schema(), &pending)?;
        drop((pending, gathered));
        let mut offset = 0;
        while offset < batch.num_rows() {
            let rows = self.slice.min(batch.num_rows() - offset);
            let work = Arc::new(memory.child("destination scan batch")?);
            let output = read_candidates(
                &batch.slice(offset, rows),
                &self.spec.type_name,
                &self.spec.binding,
                &self.spec.filters,
                self.spec.projection.as_ref(),
                &self.spec.params,
                &self.spec.snapshot,
                &self.spec.catalog,
                &work,
                &self.read_peak,
            )
            .await
            .map_err(external)?;
            offset += rows;
            let output = conform(output, &self.declared).map_err(external)?;
            work.charge_produced(output.num_rows() as u64)?;
            sender.send_bounded(output, work).await?;
        }
        Ok(())
    }
}

impl Streamed for Lookup {
    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<Flow>> {
        Box::pin(async move {
            memory.metric("input_rows", batch.num_rows());
            self.push(batch, memory, sender).await?;
            Ok(Flow::Continue)
        })
    }

    fn finish<'a>(
        &'a mut self,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<()>> {
        Box::pin(self.flush(memory, sender))
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_candidates(
    candidates: &RecordBatch,
    type_name: &str,
    binding: &str,
    filters: &[IRExpr],
    projection: Option<&NeededColumns>,
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    memory: &WorkMemory,
    read_peak: &AtomicUsize,
) -> Result<RecordBatch> {
    let id_name = format!("{binding}.{}", catalog.system_columns.id);
    let id_index = candidates
        .schema()
        .index_of(&id_name)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let ids = candidates
        .column(id_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal("scan candidate IDs must be Utf8".to_string())
        })?;
    memory
        .entries::<(&str, String)>(ids.len())
        .map_err(|error| memory.error(error))?;
    memory
        .string(ids.iter().flatten().map(str::len).sum())
        .map_err(|error| memory.error(error))?;
    let mut seen = HashSet::with_capacity(ids.len());
    let unique = ids
        .iter()
        .flatten()
        .filter(|id| seen.insert(*id))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let destination = hydrate_nodes(
        snapshot, catalog, type_name, &unique, filters, projection, params, memory, read_peak,
    )
    .await?;
    memory
        .hold(&destination)
        .map_err(|error| memory.error(error))?;
    let destination_ids = destination
        .column_by_name(catalog.system_columns.id)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest_internal("destination scan must return Utf8 IDs".to_string())
        })?;
    memory
        .entries::<(&str, u32)>(destination_ids.len())
        .map_err(|error| memory.error(error))?;
    let mut row_by_id = HashMap::with_capacity(destination_ids.len());
    for (row, id) in destination_ids.iter().enumerate() {
        if let Some(id) = id {
            let row = u32::try_from(row).map_err(|_| {
                OmniError::manifest_internal("destination scan exceeds row index range".to_string())
            })?;
            row_by_id.insert(id, row);
        }
    }
    memory
        .entries::<u32>(ids.len().saturating_mul(2))
        .map_err(|error| memory.error(error))?;
    let mut input_rows = Vec::with_capacity(ids.len());
    let mut output_rows = Vec::with_capacity(ids.len());
    for (row, id) in ids.iter().enumerate() {
        if let Some(destination_row) = id.and_then(|id| row_by_id.get(id)) {
            input_rows.push(u32::try_from(row).map_err(|_| {
                OmniError::manifest_internal("candidate batch exceeds row index range".to_string())
            })?);
            output_rows.push(*destination_row);
        }
    }
    let input_columns = (0..candidates.num_columns())
        .filter(|index| *index != id_index)
        .collect::<Vec<_>>();
    let input = candidates
        .project(&input_columns)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let input = memory
        .take(&input, &UInt32Array::from(input_rows))
        .map_err(|error| memory.error(error))?;
    let destination = memory
        .take(&destination, &UInt32Array::from(output_rows))
        .map_err(|error| memory.error(error))?;
    hconcat_batches(&input, &prefix_batch(&destination, binding)?)
}

async fn hydrate_nodes(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    ids: &[String],
    dst_filters: &[IRExpr],
    projection: Option<&NeededColumns>,
    params: &ParamMap,
    memory: &WorkMemory,
    read_peak: &AtomicUsize,
) -> Result<RecordBatch> {
    let work = memory
        .child("hydrate_nodes")
        .map_err(|error| memory.error(error))?;
    let memory = &work;
    memory.check().map_err(|error| memory.error(error))?;
    let node_type = catalog
        .node_types
        .get(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;

    let columns = ScanColumns::new(node_type, SearchColumns::default(), projection);
    if ids.is_empty() {
        let empty = columns.empty_batch(node_type);
        return if columns.has_blobs {
            add_null_blob_columns(&empty, node_type)
        } else {
            Ok(empty)
        };
    }

    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open_lance_dataset(&table_key).await?;
    let contract_covers = snapshot.contract_covers(&table_key);
    crate::engine::typed_value::check_stored_schema(
        &ds,
        &node_type.arrow_schema,
        &table_key,
        contract_covers,
        columns
            .stored_columns()
            .into_iter()
            .chain([catalog.system_columns.id]),
    )?;
    crate::engine::typed_value::check_scan_leaves(
        &ds,
        &node_type.arrow_schema,
        &table_key,
        contract_covers,
        dst_filters,
    )?;

    memory
        .entries::<datafusion::prelude::Expr>(ids.len())
        .map_err(|error| memory.error(error))?;
    memory
        .string(ids.iter().map(String::len).sum())
        .map_err(|error| memory.error(error))?;
    let mut filter_expr = id_in_list_expr(ids, catalog.system_columns.id);
    for filter in dst_filters {
        if let Some(expr) = ir_expr_to_df_expr(filter, params, Some(&node_type.arrow_schema)) {
            crate::instrumentation::record_pushed_filter_exprs(1);
            filter_expr = filter_expr.and(expr);
        } else {
            return Err(OmniError::manifest_internal(format!(
                "destination scan received unsupported predicate: {filter}"
            )));
        }
    }
    let projection = columns.read_projection();
    let plan = crate::table_store::TableStore::scan_plan_with(
        &ds,
        projection.as_deref(),
        None,
        false,
        |scanner| {
            scanner.filter_expr(filter_expr);
            scanner.prefilter(true);
            Ok(())
        },
    )
    .await?;
    let (_, mut stream) = memory.stream(plan).map_err(|error| memory.error(error))?;
    // A read's batch shares the pages it decoded: its rows are copied out,
    // so the slice holds the rows it matched rather than every page read.
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| memory.error(error))?;
        let read = memory
            .child("hydrate_nodes")
            .map_err(|error| memory.error(error))?;
        read.hold(&batch).map_err(|error| memory.error(error))?;
        let shared = batch_bytes(&batch);
        read_peak.fetch_max(shared, Ordering::Relaxed);
        let own = memory
            .slice_bytes(&batch, 0, batch.num_rows())
            .map_err(|error| memory.error(error))?;
        let batch = if shared > own.saturating_mul(2) {
            let rows = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
            memory
                .take_once(&batch, &rows, "hydrate_nodes")
                .map_err(|error| memory.error(error))?
        } else {
            memory.hold(&batch).map_err(|error| memory.error(error))?;
            batch
        };
        memory
            .entries::<RecordBatch>(1)
            .map_err(|error| memory.error(error))?;
        batches.push(batch);
    }

    let scan_result = if batches.is_empty() {
        columns.empty_batch(node_type)
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        memory
            .concat(&schema, &batches)
            .map_err(|error| memory.error(error))?
    };

    let expected = columns.empty_batch(node_type).schema();
    let selected = expected
        .fields()
        .iter()
        .map(|field| scan_result.schema().index_of(field.name()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let scan_result = scan_result
        .project(&selected)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;

    if columns.has_blobs {
        memory
            .grow(
                node_type
                    .blob_properties
                    .len()
                    .saturating_mul(scan_result.num_rows().saturating_mul(8).saturating_add(128)),
            )
            .map_err(|error| memory.error(error))?;
        let result = add_null_blob_columns(&scan_result, node_type)?;
        memory.hold(&result).map_err(|error| memory.error(error))?;
        return Ok(result);
    }
    Ok(scan_result)
}
