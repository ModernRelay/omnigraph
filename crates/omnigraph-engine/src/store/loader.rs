use std::collections::BTreeMap;
use std::io::{BufRead, Cursor};
use std::path::Path;

use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};
use crate::store::database::cdc::record_batch_row_to_json_map;
use crate::store::database::mutation::{DatasetMutationPlan, EdgeDelta, NodeDelta};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;

use super::database::LoadMode;
use super::graph::DatasetAccumulator;

mod constraints;
mod embeddings;
mod jsonl;
mod merge;

pub(crate) use embeddings::{
    EmbedSpec, EmbedValueRequest, collect_embed_specs, resolve_embedding_requests,
};
pub(crate) use jsonl::{
    json_values_to_array, load_jsonl_reader_with_name_seed_at_path, parse_date32_literal,
    parse_date64_literal,
};

pub(crate) struct InternalLoadBuildResult {
    pub(crate) next_storage: DatasetAccumulator,
}

pub(crate) struct LoadPlanResult {
    pub(crate) plan: DatasetMutationPlan,
}

/// Build the next storage snapshot for a `Database::load` operation.
///
/// Behavior:
/// - `overwrite`: full replacement with incoming data.
/// - `append`: append incoming nodes/edges to existing data.
/// - `merge`: keyed node merge with stable IDs and edge endpoint remap.
/// - Always enforces node-level `@unique`/`@key` uniqueness before returning.
pub(crate) async fn build_next_storage_for_load(
    db_path: &Path,
    existing: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    data_source: &str,
    mode: LoadMode,
) -> Result<LoadPlanResult> {
    let reader = Cursor::new(data_source.as_bytes());
    build_next_storage_for_load_reader(db_path, existing, schema_ir, reader, mode).await
}

pub(crate) async fn build_next_storage_for_load_reader<R: BufRead>(
    db_path: &Path,
    existing: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    reader: R,
    mode: LoadMode,
) -> Result<LoadPlanResult> {
    build_next_storage_for_load_reader_with_options(
        db_path, existing, schema_ir, reader, mode, true,
    )
    .await
}

pub(crate) async fn build_next_storage_for_load_reader_with_options<R: BufRead>(
    db_path: &Path,
    existing: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    reader: R,
    mode: LoadMode,
    build_indices: bool,
) -> Result<LoadPlanResult> {
    let internal = build_next_storage_for_load_reader_internal(
        db_path,
        existing,
        schema_ir,
        reader,
        mode,
        build_indices,
    )
    .await?;
    let plan =
        build_dataset_mutation_plan_for_load(existing, &internal.next_storage, schema_ir, mode)?;
    Ok(LoadPlanResult { plan })
}

pub(crate) async fn build_next_storage_for_load_reader_internal<R: BufRead>(
    db_path: &Path,
    existing: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    reader: R,
    mode: LoadMode,
    _build_indices: bool,
) -> Result<InternalLoadBuildResult> {
    let annotations = constraints::load_node_constraint_annotations(schema_ir)?;
    let key_props = annotations.key_props;
    if mode == LoadMode::Merge && key_props.is_empty() {
        return Err(NanoError::Storage(
            "load mode 'merge' requires at least one node @key property in schema".to_string(),
        ));
    }

    let mut incoming_storage = DatasetAccumulator::new(existing.catalog.clone());
    if matches!(mode, LoadMode::Merge | LoadMode::Append) {
        incoming_storage.set_next_node_id(existing.next_node_id());
    }
    let key_seed = match mode {
        LoadMode::Overwrite => None,
        LoadMode::Merge => Some(constraints::build_name_seed_for_keyed_load(
            existing, &key_props,
        )?),
        LoadMode::Append => Some(constraints::build_name_seed_for_append(
            existing, &key_props,
        )?),
    };

    if embeddings::has_embedding_specs(schema_ir) {
        let materialized_path =
            embeddings::materialize_embeddings_for_load_to_tempfile(db_path, schema_ir, reader)
                .await?;
        let file = std::fs::File::open(&materialized_path)?;
        let buffered = std::io::BufReader::new(file);
        load_jsonl_reader_with_name_seed_at_path(
            &mut incoming_storage,
            db_path,
            buffered,
            &key_props,
            key_seed.as_ref(),
        )?;
        let _ = std::fs::remove_file(materialized_path);
    } else {
        load_jsonl_reader_with_name_seed_at_path(
            &mut incoming_storage,
            db_path,
            reader,
            &key_props,
            key_seed.as_ref(),
        )?;
    }

    let next_storage = match mode {
        LoadMode::Overwrite => incoming_storage,
        LoadMode::Merge => {
            let result = merge::merge_storage_with_node_keys(
                db_path,
                existing,
                &incoming_storage,
                schema_ir,
                &key_props,
            )
            .await?;
            result.storage
        }
        LoadMode::Append => {
            let result = merge::append_storage(existing, &incoming_storage, schema_ir)?;
            result.storage
        }
    };

    constraints::enforce_node_unique_constraints(&next_storage, &annotations.unique_props)?;
    Ok(InternalLoadBuildResult { next_storage })
}

fn build_dataset_mutation_plan_for_load(
    existing: &DatasetAccumulator,
    next: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    mode: LoadMode,
) -> Result<DatasetMutationPlan> {
    let mut plan = DatasetMutationPlan::new(
        load_mode_op_summary_for_plan(mode),
        next.next_node_id(),
        next.next_edge_id(),
    );

    for node_def in schema_ir.node_types() {
        let replacement = next.get_all_nodes(&node_def.name)?;
        let delta = build_node_delta(
            existing.get_all_nodes(&node_def.name)?,
            replacement.clone(),
            existing
                .catalog
                .node_types
                .get(&node_def.name)
                .ok_or_else(|| NanoError::Storage(format!("unknown node type {}", node_def.name)))?
                .arrow_schema
                .clone(),
        )?;
        let Some(delta) = delta else {
            continue;
        };
        plan.node_replacements
            .insert(node_def.name.clone(), replacement);
        plan.delta.node_changes.insert(node_def.name.clone(), delta);
    }

    for edge_def in schema_ir.edge_types() {
        let replacement = next.edge_batch_for_save(&edge_def.name)?;
        let delta = build_edge_delta(
            existing.edge_batch_for_save(&edge_def.name)?,
            replacement.clone(),
            existing
                .catalog
                .edge_types
                .get(&edge_def.name)
                .ok_or_else(|| NanoError::Storage(format!("unknown edge type {}", edge_def.name)))?
                .arrow_schema
                .clone(),
        )?;
        let Some(delta) = delta else {
            continue;
        };
        plan.edge_replacements
            .insert(edge_def.name.clone(), replacement);
        plan.delta.edge_changes.insert(edge_def.name.clone(), delta);
    }

    Ok(plan)
}

fn load_mode_op_summary_for_plan(mode: LoadMode) -> &'static str {
    match mode {
        LoadMode::Overwrite => "load:overwrite",
        LoadMode::Append => "load:append",
        LoadMode::Merge => "load:merge",
    }
}

fn build_node_delta(
    previous: Option<RecordBatch>,
    next: Option<RecordBatch>,
    schema: SchemaRef,
) -> Result<Option<NodeDelta>> {
    let previous_rows = collect_rows_by_id(previous.as_ref())?;
    let next_rows = collect_rows_by_id(next.as_ref())?;

    let mut insert_rows = Vec::new();
    let mut before_update_rows = Vec::new();
    let mut after_update_rows = Vec::new();
    let mut delete_rows = Vec::new();
    let mut delete_ids = Vec::new();

    for (id, before_row) in &previous_rows {
        match next_rows.get(id) {
            None => {
                delete_ids.push(*id);
                delete_rows.push(before_row.clone());
            }
            Some(after_row) if after_row != before_row => {
                before_update_rows.push(before_row.clone());
                after_update_rows.push(after_row.clone());
            }
            Some(_) => {}
        }
    }

    for (id, after_row) in &next_rows {
        if !previous_rows.contains_key(id) {
            insert_rows.push(after_row.clone());
        }
    }

    if insert_rows.is_empty()
        && before_update_rows.is_empty()
        && after_update_rows.is_empty()
        && delete_ids.is_empty()
    {
        return Ok(None);
    }

    Ok(Some(NodeDelta {
        inserts: build_batch_from_json_rows(schema.clone(), &insert_rows)?,
        upserts: build_batch_from_json_rows(schema.clone(), &after_update_rows)?,
        delete_ids,
        before_for_updates: build_batch_from_json_rows(schema.clone(), &before_update_rows)?,
        before_for_deletes: build_batch_from_json_rows(schema, &delete_rows)?,
    }))
}

fn build_edge_delta(
    previous: Option<RecordBatch>,
    next: Option<RecordBatch>,
    schema: SchemaRef,
) -> Result<Option<EdgeDelta>> {
    let previous_rows = collect_rows_by_id(previous.as_ref())?;
    let next_rows = collect_rows_by_id(next.as_ref())?;

    let mut insert_rows = Vec::new();
    let mut delete_rows = Vec::new();
    let mut delete_ids = Vec::new();

    for (id, before_row) in &previous_rows {
        match next_rows.get(id) {
            None => {
                delete_ids.push(*id);
                delete_rows.push(before_row.clone());
            }
            Some(after_row) if after_row != before_row => {
                delete_ids.push(*id);
                delete_rows.push(before_row.clone());
                insert_rows.push(after_row.clone());
            }
            Some(_) => {}
        }
    }

    for (id, after_row) in &next_rows {
        if !previous_rows.contains_key(id) {
            insert_rows.push(after_row.clone());
        }
    }

    if insert_rows.is_empty() && delete_ids.is_empty() {
        return Ok(None);
    }

    Ok(Some(EdgeDelta {
        inserts: build_batch_from_json_rows(schema.clone(), &insert_rows)?,
        delete_ids,
        before_for_deletes: build_batch_from_json_rows(schema, &delete_rows)?,
    }))
}

fn collect_rows_by_id(
    batch: Option<&RecordBatch>,
) -> Result<BTreeMap<u64, JsonMap<String, JsonValue>>> {
    let mut rows = BTreeMap::new();
    let Some(batch) = batch else {
        return Ok(rows);
    };
    let ids = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Storage("batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .ok_or_else(|| NanoError::Storage("batch id column must be UInt64".to_string()))?;
    for row in 0..batch.num_rows() {
        rows.insert(ids.value(row), record_batch_row_to_json_map(batch, row)?);
    }
    Ok(rows)
}

fn build_batch_from_json_rows(
    schema: SchemaRef,
    rows: &[JsonMap<String, JsonValue>],
) -> Result<Option<RecordBatch>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let key_name = if idx == 0 && field.name() == "id" {
            "__ng_id"
        } else {
            field.name()
        };
        let values: Vec<JsonValue> = rows
            .iter()
            .map(|row| row.get(key_name).cloned().unwrap_or(JsonValue::Null))
            .collect();
        columns.push(json_values_to_array(
            &values,
            field.data_type(),
            field.is_nullable(),
        )?);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| NanoError::Storage(format!("load delta batch build error: {}", e)))?;
    Ok(Some(batch))
}
