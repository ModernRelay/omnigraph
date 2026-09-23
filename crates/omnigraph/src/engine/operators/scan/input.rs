//! Destination scans constrained by batches of graph identities: the per-slice
//! id lookup of a dependent `ScanExec`, and the fallback branch of
//! `HashJoinExec`.

use std::collections::{HashMap, HashSet};

use super::*;
use crate::engine::operators::LookupSpec;
use crate::engine::operators::memory::WorkMemory;
use crate::engine::operators::producer::{BatchSender, producer_stream};
use crate::engine::scan::{
    ScanColumns, SearchColumns, add_null_blob_columns, conjoin_fts_queries, hconcat_batches,
    id_in_list_expr, ir_filter_to_expr,
};
use crate::engine::search::search_filter_query;
use crate::error::OmniError;
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{PlanProperties, SendableRecordBatchStream};
use futures::StreamExt;

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

    pub(super) fn input_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        super::super::streaming_properties(schema)
    }

    pub(super) fn execute_input(
        &self,
        input: SendableRecordBatchStream,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let mut work = WorkMemory::new(ctx, "ScanExec")?;
        work.set_metrics(self.metrics.clone());
        let memory = Arc::new(work);
        let schema = self.schema();
        let declared = Arc::clone(&schema);
        let lookup = LookupSpec {
            type_name: self.type_name.clone(),
            binding: self.binding.clone(),
            filters: self.filters.clone(),
            projection: self.projection.clone(),
            params: Arc::clone(&self.params),
            snapshot: self.snapshot.clone(),
            catalog: Arc::clone(&self.catalog),
        };
        Ok(producer_stream(
            schema,
            memory,
            Some(&self.metrics),
            move |memory, sender| async move {
                lookup_candidates(input, &lookup, &declared, &memory, &sender).await
            },
        ))
    }
}

/// Every batch of `input` resolved to destination rows one slice of at most
/// 256 rows at a time, each slice one Lance read `id IN (slice ids)` with the
/// pushed filters, sent in `declared`'s shape.
pub(in crate::engine) async fn lookup_candidates(
    mut input: SendableRecordBatchStream,
    lookup: &LookupSpec,
    declared: &SchemaRef,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
) -> DfResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        let held = memory.child("scan candidate input")?;
        held.hold(&batch)?;
        memory.metric("input_rows", batch.num_rows());
        let rows = memory.ctx.session_config().batch_size().clamp(1, 256);
        for offset in (0..batch.num_rows()).step_by(rows) {
            let work = Arc::new(memory.child("destination scan batch")?);
            let candidates = batch.slice(offset, rows.min(batch.num_rows() - offset));
            let output = read_candidates(
                &candidates,
                &lookup.type_name,
                &lookup.binding,
                &lookup.filters,
                lookup.projection.as_ref(),
                &lookup.params,
                &lookup.snapshot,
                &lookup.catalog,
                &work,
            )
            .await
            .map_err(external)?;
            let output = conform(output, declared).map_err(external)?;
            sender.send_bounded(output, work).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_candidates(
    candidates: &RecordBatch,
    type_name: &str,
    binding: &str,
    filters: &[IRFilter],
    projection: Option<&NeededColumns>,
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    memory: &WorkMemory,
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
        snapshot, catalog, type_name, &unique, filters, projection, params, memory,
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
    dst_filters: &[IRFilter],
    projection: Option<&NeededColumns>,
    params: &ParamMap,
    memory: &WorkMemory,
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

    memory
        .entries::<datafusion::prelude::Expr>(ids.len())
        .map_err(|error| memory.error(error))?;
    memory
        .string(ids.iter().map(String::len).sum())
        .map_err(|error| memory.error(error))?;
    let mut filter_expr = id_in_list_expr(ids, catalog.system_columns.id);
    let mut queries = Vec::new();
    for filter in dst_filters {
        if let Some(query) = search_filter_query(filter, params)? {
            queries.push(query);
        } else if let Some(expr) = ir_filter_to_expr(filter, params, Some(&node_type.arrow_schema))
        {
            crate::instrumentation::record_pushed_filter_exprs(1);
            filter_expr = filter_expr.and(expr);
        } else {
            return Err(OmniError::manifest_internal(format!(
                "destination scan received unsupported predicate: {filter}"
            )));
        }
    }
    let fts = conjoin_fts_queries(queries);
    let projection = columns.read_projection();
    let plan = crate::table_store::TableStore::scan_plan_with(
        &ds,
        projection.as_deref(),
        None,
        false,
        |scanner| {
            scanner.filter_expr(filter_expr);
            scanner.prefilter(true);
            if let Some(query) = fts {
                scanner.full_text_search(query).map_err(|error| {
                    OmniError::storage_context("destination full-text search", error)
                })?;
            }
            Ok(())
        },
    )
    .await?;
    let (_, stream) = memory.stream(plan).map_err(|error| memory.error(error))?;
    let batches = memory
        .collect(stream)
        .await
        .map_err(|error| memory.error(error))?;

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
