use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::UInt64Builder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::DataType;

use super::super::graph::DatasetAccumulator;
use super::constraints::{key_value_string, node_property_index};
use super::jsonl::json_values_to_array;
use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};

pub(crate) struct MergeStorageResult {
    pub(crate) storage: DatasetAccumulator,
}

pub(crate) async fn merge_storage_with_node_keys(
    _db_path: &Path,
    existing: &DatasetAccumulator,
    incoming: &DatasetAccumulator,
    schema_ir: &SchemaIR,
    key_props: &HashMap<String, String>,
) -> Result<MergeStorageResult> {
    let mut merged = DatasetAccumulator::new(existing.catalog.clone());
    let mut next_node_id = existing.next_node_id();
    let mut next_edge_id = existing.next_edge_id();
    let mut id_remap_by_type: HashMap<String, HashMap<u64, u64>> = HashMap::new();
    let mut replaced_unkeyed_types: HashSet<String> = HashSet::new();

    for node_def in schema_ir.node_types() {
        let existing_batch = existing.get_all_nodes(&node_def.name)?;
        let incoming_batch = incoming.get_all_nodes(&node_def.name)?;

        if let Some(key_prop) = key_props.get(&node_def.name) {
            let (merged_batch, remap) = merge_keyed_node_batches_storage_native(
                existing_batch.as_ref(),
                incoming_batch.as_ref(),
                key_prop,
                &mut next_node_id,
            )?;
            id_remap_by_type.insert(node_def.name.clone(), remap);
            if let Some(batch) = merged_batch {
                merged.load_node_batch(&node_def.name, batch)?;
            }
        } else {
            match (existing_batch.as_ref(), incoming_batch.as_ref()) {
                (_, Some(incoming_batch)) => {
                    let (reassigned, remap) = reassign_node_ids(incoming_batch, &mut next_node_id)?;
                    replaced_unkeyed_types.insert(node_def.name.clone());
                    id_remap_by_type.insert(node_def.name.clone(), remap);
                    merged.load_node_batch(&node_def.name, reassigned)?;
                }
                (Some(existing_batch), None) => {
                    id_remap_by_type.insert(node_def.name.clone(), HashMap::new());
                    merged.load_node_batch(&node_def.name, existing_batch.clone())?;
                }
                (None, None) => {
                    id_remap_by_type.insert(node_def.name.clone(), HashMap::new());
                }
            }
        }
    }

    for edge_def in schema_ir.edge_types() {
        let src_remap = id_remap_by_type
            .get(&edge_def.src_type_name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing source ID remap for node type {}",
                    edge_def.src_type_name
                ))
            })?;
        let dst_remap = id_remap_by_type
            .get(&edge_def.dst_type_name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "missing destination ID remap for node type {}",
                    edge_def.dst_type_name
                ))
            })?;
        let existing_batch = existing.edge_batch_for_save(&edge_def.name)?;
        let incoming_batch = incoming.edge_batch_for_save(&edge_def.name)?;
        let preserve_existing = !replaced_unkeyed_types.contains(&edge_def.src_type_name)
            && !replaced_unkeyed_types.contains(&edge_def.dst_type_name);

        let merged_edge_batch = merge_edge_batches(
            existing_batch.as_ref(),
            incoming_batch.as_ref(),
            src_remap,
            dst_remap,
            &edge_def.name,
            preserve_existing,
            &mut next_edge_id,
        )?;
        if let Some(batch) = merged_edge_batch {
            merged.load_edge_batch(&edge_def.name, batch)?;
        }
    }

    Ok(MergeStorageResult { storage: merged })
}

pub(crate) fn append_storage(
    existing: &DatasetAccumulator,
    incoming: &DatasetAccumulator,
    schema_ir: &SchemaIR,
) -> Result<MergeStorageResult> {
    let mut appended = DatasetAccumulator::new(existing.catalog.clone());
    let mut next_node_id = existing.next_node_id();
    let mut next_edge_id = existing.next_edge_id();
    let mut incoming_node_remap_by_type: HashMap<String, HashMap<u64, u64>> = HashMap::new();

    for node_def in schema_ir.node_types() {
        let existing_batch = existing.get_all_nodes(&node_def.name)?;
        let incoming_batch = incoming.get_all_nodes(&node_def.name)?;

        match (existing_batch.as_ref(), incoming_batch.as_ref()) {
            (Some(existing_batch), Some(incoming_batch)) => {
                let (incoming_reassigned, remap) =
                    reassign_node_ids(incoming_batch, &mut next_node_id)?;
                let schema = existing_batch.schema();
                let combined = arrow_select::concat::concat_batches(
                    &schema,
                    &[existing_batch.clone(), incoming_reassigned],
                )
                .map_err(|e| {
                    NanoError::Storage(format!(
                        "append node concat error for {}: {}",
                        node_def.name, e
                    ))
                })?;
                incoming_node_remap_by_type.insert(node_def.name.clone(), remap);
                appended.load_node_batch(&node_def.name, combined)?;
            }
            (Some(existing_batch), None) => {
                incoming_node_remap_by_type.insert(node_def.name.clone(), HashMap::new());
                appended.load_node_batch(&node_def.name, existing_batch.clone())?;
            }
            (None, Some(incoming_batch)) => {
                let (incoming_reassigned, remap) =
                    reassign_node_ids(incoming_batch, &mut next_node_id)?;
                incoming_node_remap_by_type.insert(node_def.name.clone(), remap);
                appended.load_node_batch(&node_def.name, incoming_reassigned)?;
            }
            (None, None) => {
                incoming_node_remap_by_type.insert(node_def.name.clone(), HashMap::new());
            }
        }
    }

    for edge_def in schema_ir.edge_types() {
        let existing_batch = existing.edge_batch_for_save(&edge_def.name)?;
        let incoming_batch = incoming.edge_batch_for_save(&edge_def.name)?;

        match incoming_batch.as_ref() {
            None => {
                if let Some(existing_batch) = existing_batch.as_ref() {
                    appended.load_edge_batch(&edge_def.name, existing_batch.clone())?;
                }
            }
            Some(_) => {
                let src_remap = incoming_node_remap_by_type
                    .get(&edge_def.src_type_name)
                    .ok_or_else(|| {
                        NanoError::Storage(format!(
                            "missing source ID remap for node type {}",
                            edge_def.src_type_name
                        ))
                    })?;
                let dst_remap = incoming_node_remap_by_type
                    .get(&edge_def.dst_type_name)
                    .ok_or_else(|| {
                        NanoError::Storage(format!(
                            "missing destination ID remap for node type {}",
                            edge_def.dst_type_name
                        ))
                    })?;
                let merged_batch = merge_edge_batches(
                    existing_batch.as_ref(),
                    incoming_batch.as_ref(),
                    src_remap,
                    dst_remap,
                    &edge_def.name,
                    true,
                    &mut next_edge_id,
                )?;
                if let Some(batch) = merged_batch {
                    appended.load_edge_batch(&edge_def.name, batch)?;
                }
            }
        }
    }

    Ok(MergeStorageResult { storage: appended })
}

fn merge_keyed_node_batches_storage_native(
    existing: Option<&RecordBatch>,
    incoming: Option<&RecordBatch>,
    key_prop: &str,
    next_node_id: &mut u64,
) -> Result<(Option<RecordBatch>, HashMap<u64, u64>)> {
    match (existing, incoming) {
        (None, None) => Ok((None, HashMap::new())),
        (Some(existing), None) => Ok((Some(existing.clone()), HashMap::new())),
        (None, Some(incoming)) => {
            let (reassigned, remap) = reassign_node_ids(incoming, next_node_id)?;
            Ok((Some(reassigned), remap))
        }
        (Some(existing), Some(incoming)) => {
            if existing.num_columns() != incoming.num_columns() {
                return Err(NanoError::Storage(format!(
                    "schema mismatch while merging keyed nodes on {}",
                    key_prop
                )));
            }

            let (source_batch, remap) =
                rewrite_incoming_keyed_ids(existing, incoming, key_prop, next_node_id)?;
            let merged_batch = run_keyed_merge_insert_in_memory(existing, source_batch, key_prop)?;
            Ok((Some(merged_batch), remap))
        }
    }
}

fn rewrite_incoming_keyed_ids(
    existing: &RecordBatch,
    incoming: &RecordBatch,
    key_prop: &str,
    next_node_id: &mut u64,
) -> Result<(RecordBatch, HashMap<u64, u64>)> {
    let existing_key_idx = node_property_index(existing.schema().as_ref(), key_prop)
        .ok_or_else(|| NanoError::Storage(format!("missing key property {}", key_prop)))?;
    let incoming_key_idx = node_property_index(incoming.schema().as_ref(), key_prop)
        .ok_or_else(|| NanoError::Storage(format!("missing key property {}", key_prop)))?;

    let existing_id_arr = existing
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            NanoError::Storage("existing node batch id column is not UInt64".to_string())
        })?;
    let incoming_id_arr = incoming
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            NanoError::Storage("incoming node batch id column is not UInt64".to_string())
        })?;

    let mut existing_key_to_id: HashMap<String, u64> = HashMap::new();
    for row in 0..existing.num_rows() {
        let key = key_value_string(existing.column(existing_key_idx), row, key_prop)?;
        if existing_key_to_id
            .insert(key.clone(), existing_id_arr.value(row))
            .is_some()
        {
            return Err(NanoError::Storage(format!(
                "existing data contains duplicate @key value '{}' for {}",
                key, key_prop
            )));
        }
    }

    let mut incoming_seen_keys: HashSet<String> = HashSet::new();
    let mut remap: HashMap<u64, u64> = HashMap::new();
    let mut id_builder = UInt64Builder::with_capacity(incoming.num_rows());
    for row in 0..incoming.num_rows() {
        let key = key_value_string(incoming.column(incoming_key_idx), row, key_prop)?;
        if !incoming_seen_keys.insert(key.clone()) {
            return Err(NanoError::Storage(format!(
                "incoming load contains duplicate @key value '{}' for {}",
                key, key_prop
            )));
        }

        let incoming_id = incoming_id_arr.value(row);
        let assigned_id = if let Some(existing_id) = existing_key_to_id.get(&key) {
            *existing_id
        } else {
            let next_id = *next_node_id;
            *next_node_id = next_node_id.saturating_add(1);
            next_id
        };
        remap.insert(incoming_id, assigned_id);
        id_builder.append_value(assigned_id);
    }

    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(incoming.num_columns());
    out_columns.push(Arc::new(id_builder.finish()) as ArrayRef);
    for col in incoming.columns().iter().skip(1) {
        out_columns.push(col.clone());
    }
    let rewritten = RecordBatch::try_new(incoming.schema(), out_columns)
        .map_err(|e| NanoError::Storage(format!("rewrite keyed source batch error: {}", e)))?;
    Ok((rewritten, remap))
}

fn run_keyed_merge_insert_in_memory(
    existing: &RecordBatch,
    source_batch: RecordBatch,
    key_prop: &str,
) -> Result<RecordBatch> {
    let schema = existing.schema();
    if source_batch.schema().fields() != schema.fields() {
        return Err(NanoError::Storage(format!(
            "schema mismatch while keyed merge on {}",
            key_prop
        )));
    }

    let key_idx = node_property_index(schema.as_ref(), key_prop)
        .ok_or_else(|| NanoError::Storage(format!("missing key property {}", key_prop)))?;

    let mut key_to_row: HashMap<String, usize> = HashMap::new();
    let mut out_rows: Vec<Vec<serde_json::Value>> =
        Vec::with_capacity(existing.num_rows() + source_batch.num_rows());

    for row in 0..existing.num_rows() {
        let key = key_value_string(existing.column(key_idx), row, key_prop)?;
        if key_to_row.insert(key.clone(), row).is_some() {
            return Err(NanoError::Storage(format!(
                "existing data contains duplicate @key value '{}' for {}",
                key, key_prop
            )));
        }
        let mut values = Vec::with_capacity(existing.num_columns());
        for col in existing.columns() {
            values.push(array_value_to_json(col, row));
        }
        out_rows.push(values);
    }

    for row in 0..source_batch.num_rows() {
        let key = key_value_string(source_batch.column(key_idx), row, key_prop)?;
        let mut values = Vec::with_capacity(source_batch.num_columns());
        for col in source_batch.columns() {
            values.push(array_value_to_json(col, row));
        }
        if let Some(existing_row) = key_to_row.get(&key).copied() {
            out_rows[existing_row] = values;
        } else {
            let new_row = out_rows.len();
            key_to_row.insert(key, new_row);
            out_rows.push(values);
        }
    }

    if out_rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let values = out_rows
            .iter()
            .map(|row| row[col_idx].clone())
            .collect::<Vec<_>>();
        let arr = json_values_to_array(&values, field.data_type(), field.is_nullable())?;
        out_columns.push(arr);
    }

    RecordBatch::try_new(schema, out_columns)
        .map_err(|e| NanoError::Storage(format!("merge keyed batch error: {}", e)))
}

fn reassign_node_ids(
    batch: &RecordBatch,
    next_node_id: &mut u64,
) -> Result<(RecordBatch, HashMap<u64, u64>)> {
    let id_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("node batch id column is not UInt64".to_string()))?;

    let mut remap = HashMap::new();
    let mut id_builder = UInt64Builder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let old_id = id_arr.value(row);
        let new_id = *next_node_id;
        *next_node_id = next_node_id.saturating_add(1);
        remap.insert(old_id, new_id);
        id_builder.append_value(new_id);
    }

    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    out_columns.push(Arc::new(id_builder.finish()) as ArrayRef);
    for col in batch.columns().iter().skip(1) {
        out_columns.push(col.clone());
    }

    let out_batch = RecordBatch::try_new(batch.schema(), out_columns)
        .map_err(|e| NanoError::Storage(format!("reassign node id batch error: {}", e)))?;
    Ok((out_batch, remap))
}

fn merge_edge_batches(
    existing: Option<&RecordBatch>,
    incoming: Option<&RecordBatch>,
    src_remap: &HashMap<u64, u64>,
    dst_remap: &HashMap<u64, u64>,
    edge_name: &str,
    preserve_existing: bool,
    next_edge_id: &mut u64,
) -> Result<Option<RecordBatch>> {
    let remapped_existing = if preserve_existing {
        existing.cloned()
    } else {
        None
    };
    let remapped_incoming = incoming
        .map(|batch| remap_edge_batch_endpoints(batch, src_remap, dst_remap, edge_name))
        .transpose()?;

    if remapped_incoming.is_none() {
        return Ok(remapped_existing);
    }

    let schema = remapped_incoming
        .as_ref()
        .map(|b| b.schema())
        .or_else(|| remapped_existing.as_ref().map(|b| b.schema()));
    let Some(schema) = schema else {
        return Ok(None);
    };

    // No multigraph support: keep one row per (src, dst) edge.
    // Existing rows are loaded first and incoming rows overwrite duplicates.
    let mut row_order: Vec<(u64, u64)> = Vec::new();
    let mut row_ids: HashMap<(u64, u64), u64> = HashMap::new();
    let mut row_props: HashMap<(u64, u64), Vec<serde_json::Value>> = HashMap::new();
    let prop_field_names: Vec<String> = schema
        .fields()
        .iter()
        .filter_map(|field| {
            if field.name() == "id" || field.name() == "src" || field.name() == "dst" {
                None
            } else {
                Some(field.name().clone())
            }
        })
        .collect();

    let mut ingest = |batch: &RecordBatch, overwrite: bool| -> Result<()> {
        let id_arr = batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("edge batch missing id column".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| NanoError::Storage("edge id column is not UInt64".to_string()))?;
        let src_arr = batch
            .column_by_name("src")
            .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
        let dst_arr = batch
            .column_by_name("dst")
            .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;

        for row in 0..batch.num_rows() {
            let key = (src_arr.value(row), dst_arr.value(row));
            let current_id = id_arr.value(row);
            let props = prop_field_names
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .ok_or_else(|| {
                            NanoError::Storage(format!(
                                "missing edge property column {} for {}",
                                name, edge_name
                            ))
                        })
                        .map(|col| array_value_to_json(col, row))
                })
                .collect::<Result<Vec<_>>>()?;
            match row_props.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if overwrite {
                        entry.insert(props);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    row_order.push(key);
                    let assigned_id = if overwrite {
                        let next_id = *next_edge_id;
                        *next_edge_id = next_edge_id.saturating_add(1);
                        next_id
                    } else {
                        current_id
                    };
                    row_ids.insert(key, assigned_id);
                    entry.insert(props);
                }
            }
        }

        Ok(())
    };

    if let Some(batch) = remapped_existing.as_ref() {
        ingest(batch, false)?;
    }
    if let Some(batch) = remapped_incoming.as_ref() {
        ingest(batch, true)?;
    }
    if row_order.is_empty() {
        return Ok(None);
    }

    let mut id_builder = UInt64Builder::with_capacity(row_order.len());
    let mut src_builder = UInt64Builder::with_capacity(row_order.len());
    let mut dst_builder = UInt64Builder::with_capacity(row_order.len());
    let mut prop_values: Vec<Vec<serde_json::Value>> = (0..prop_field_names.len())
        .map(|_| Vec::with_capacity(row_order.len()))
        .collect();

    for (src, dst) in &row_order {
        let edge_id = *row_ids.get(&(*src, *dst)).ok_or_else(|| {
            NanoError::Storage(format!(
                "internal edge id assignment error for {} at ({}, {})",
                edge_name, src, dst
            ))
        })?;
        id_builder.append_value(edge_id);
        src_builder.append_value(*src);
        dst_builder.append_value(*dst);

        let props = row_props.get(&(*src, *dst)).ok_or_else(|| {
            NanoError::Storage(format!(
                "internal edge dedup error for {} at ({}, {})",
                edge_name, src, dst
            ))
        })?;
        for (idx, prop) in props.iter().enumerate() {
            prop_values[idx].push(prop.clone());
        }
    }

    let mut built_props: HashMap<String, ArrayRef> = HashMap::new();
    for (prop_pos, field_name) in prop_field_names.iter().enumerate() {
        let field = schema.field_with_name(field_name).map_err(|e| {
            NanoError::Storage(format!(
                "missing merged edge property field {} for {}: {}",
                field_name, edge_name, e
            ))
        })?;
        let arr = json_values_to_array(
            &prop_values[prop_pos],
            field.data_type(),
            field.is_nullable(),
        )?;
        built_props.insert(field_name.clone(), arr);
    }

    let id_arr: ArrayRef = Arc::new(id_builder.finish());
    let src_arr: ArrayRef = Arc::new(src_builder.finish());
    let dst_arr: ArrayRef = Arc::new(dst_builder.finish());
    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match field.name().as_str() {
            "id" => out_columns.push(id_arr.clone()),
            "src" => out_columns.push(src_arr.clone()),
            "dst" => out_columns.push(dst_arr.clone()),
            name => {
                let arr = built_props.get(name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "missing merged edge property column {} for {}",
                        name, edge_name
                    ))
                })?;
                out_columns.push(arr.clone());
            }
        }
    }

    let batch = RecordBatch::try_new(schema, out_columns)
        .map_err(|e| NanoError::Storage(format!("edge merge batch error: {}", e)))?;
    Ok(Some(batch))
}

fn remap_edge_batch_endpoints(
    batch: &RecordBatch,
    src_remap: &HashMap<u64, u64>,
    dst_remap: &HashMap<u64, u64>,
    _edge_name: &str,
) -> Result<RecordBatch> {
    let src_arr = batch
        .column_by_name("src")
        .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
    let dst_arr = batch
        .column_by_name("dst")
        .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;

    let mut src_builder = UInt64Builder::with_capacity(batch.num_rows());
    let mut dst_builder = UInt64Builder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let src = src_arr.value(row);
        let dst = dst_arr.value(row);
        let mapped_src = src_remap.get(&src).copied().unwrap_or(src);
        let mapped_dst = dst_remap.get(&dst).copied().unwrap_or(dst);
        src_builder.append_value(mapped_src);
        dst_builder.append_value(mapped_dst);
    }
    let src_arr: ArrayRef = Arc::new(src_builder.finish());
    let dst_arr: ArrayRef = Arc::new(dst_builder.finish());

    let mut out_columns = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        match field.name().as_str() {
            "src" => out_columns.push(src_arr.clone()),
            "dst" => out_columns.push(dst_arr.clone()),
            _ => out_columns.push(batch.column(idx).clone()),
        }
    }

    RecordBatch::try_new(batch.schema(), out_columns)
        .map_err(|e| NanoError::Storage(format!("edge remap batch error: {}", e)))
}

fn array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(row) as f64).map(serde_json::Value::Number)
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| {
                let days = a.value(row);
                arrow_array::temporal_conversions::date32_to_datetime(days)
                    .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d").to_string()))
                    .unwrap_or_else(|| serde_json::Value::Number((days as i64).into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| {
                let ms = a.value(row);
                arrow_array::temporal_conversions::date64_to_datetime(ms)
                    .map(|dt| {
                        serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                    })
                    .unwrap_or_else(|| serde_json::Value::Number(ms.into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| {
                let values = a.value(row);
                serde_json::Value::Array(
                    (0..values.len())
                        .map(|idx| array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|a| fixed_size_list_value_to_json(a, row))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

fn fixed_size_list_value_to_json(array: &FixedSizeListArray, row: usize) -> serde_json::Value {
    let value_len = array.value_length() as usize;
    let values = array.values();
    if let Some(float_values) = values.as_any().downcast_ref::<Float32Array>() {
        let start = row.saturating_mul(value_len);
        return float32_json_array(float_values, start, value_len);
    }

    let values = array.value(row);
    serde_json::Value::Array(
        (0..values.len())
            .map(|idx| array_value_to_json(&values, idx))
            .collect(),
    )
}

fn float32_json_array(values: &Float32Array, start: usize, len: usize) -> serde_json::Value {
    let mut out = Vec::with_capacity(len);
    let end = start.saturating_add(len).min(values.len());
    for idx in start..end {
        if values.is_null(idx) {
            out.push(serde_json::Value::Null);
            continue;
        }
        let value = values.value(idx) as f64;
        out.push(
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Array(out)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::catalog::schema_ir::{build_catalog_from_ir, build_schema_ir};
    use crate::schema::parser::parse_schema;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_schema::{Field, Schema};

    use super::*;

    fn node_batch(ids: Vec<u64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn edge_batch(ids: Vec<u64>, src: Vec<u64>, dst: Vec<u64>, since: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("since", DataType::Int32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(UInt64Array::from(src)) as ArrayRef,
                Arc::new(UInt64Array::from(dst)) as ArrayRef,
                Arc::new(Int32Array::from(since)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn edge_batch_with_role_and_started_at(
        ids: Vec<u64>,
        src: Vec<u64>,
        dst: Vec<u64>,
        role: Vec<&str>,
        started_at: Vec<i32>,
        role_first: bool,
    ) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(UInt64Array::from(src)) as ArrayRef,
            Arc::new(UInt64Array::from(dst)) as ArrayRef,
        ];

        let role_field = Field::new("role", DataType::Utf8, false);
        let role_col = Arc::new(StringArray::from(role)) as ArrayRef;
        let started_at_field = Field::new("startedAt", DataType::Date32, false);
        let started_at_col = Arc::new(Date32Array::from(started_at)) as ArrayRef;

        if role_first {
            fields.push(role_field);
            fields.push(started_at_field);
            columns.push(role_col);
            columns.push(started_at_col);
        } else {
            fields.push(started_at_field);
            fields.push(role_field);
            columns.push(started_at_col);
            columns.push(role_col);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn node_batch_with_age(ids: Vec<u64>, names: Vec<&str>, ages: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(Int32Array::from(ages)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn node_batch_with_user_id(
        internal_ids: Vec<u64>,
        user_ids: Vec<&str>,
        names: Vec<&str>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(internal_ids)) as ArrayRef,
                Arc::new(StringArray::from(user_ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn node_batch_with_embedding(
        ids: Vec<u64>,
        slugs: Vec<&str>,
        embeddings: Vec<[f32; 3]>,
    ) -> RecordBatch {
        let mut builder =
            FixedSizeListBuilder::with_capacity(Float32Builder::new(), 3, embeddings.len());
        for embedding in embeddings {
            for value in embedding {
                builder.values().append_value(value);
            }
            builder.append(true);
        }

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("slug", DataType::Utf8, false),
                Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        3,
                    ),
                    false,
                ),
            ])),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(slugs)) as ArrayRef,
                Arc::new(builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn reassign_node_ids_rewrites_ids_and_returns_remap() {
        let batch = node_batch(vec![7, 8], vec!["Alice", "Bob"]);
        let mut next_id = 100;

        let (out, remap) = reassign_node_ids(&batch, &mut next_id).unwrap();
        let id_col = out
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(id_col.value(0), 100);
        assert_eq!(id_col.value(1), 101);
        assert_eq!(remap.get(&7), Some(&100));
        assert_eq!(remap.get(&8), Some(&101));
        assert_eq!(next_id, 102);
    }

    #[test]
    fn rewrite_incoming_keyed_ids_reuses_existing_and_allocates_new() {
        let existing = node_batch(vec![10], vec!["Alice"]);
        let incoming = node_batch(vec![1, 2], vec!["Alice", "Bob"]);
        let mut next_id = 50;

        let (rewritten, remap) =
            rewrite_incoming_keyed_ids(&existing, &incoming, "name", &mut next_id).unwrap();
        let ids = rewritten
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(ids.value(0), 10);
        assert_eq!(ids.value(1), 50);
        assert_eq!(remap.get(&1), Some(&10));
        assert_eq!(remap.get(&2), Some(&50));
        assert_eq!(next_id, 51);
    }

    #[test]
    fn rewrite_incoming_keyed_ids_rejects_duplicate_incoming_key() {
        let existing = node_batch(vec![10], vec!["Alice"]);
        let incoming = node_batch(vec![1, 2], vec!["Bob", "Bob"]);
        let mut next_id = 20;

        let err =
            rewrite_incoming_keyed_ids(&existing, &incoming, "name", &mut next_id).unwrap_err();
        assert!(err.to_string().contains("duplicate @key"));
    }

    #[test]
    fn rewrite_incoming_keyed_ids_uses_user_property_named_id() {
        let existing = node_batch_with_user_id(vec![10], vec!["user-1"], vec!["Alice"]);
        let incoming =
            node_batch_with_user_id(vec![1, 2], vec!["user-1", "user-2"], vec!["Alice", "Bob"]);
        let mut next_id = 50;

        let (rewritten, remap) =
            rewrite_incoming_keyed_ids(&existing, &incoming, "id", &mut next_id).unwrap();
        let ids = rewritten
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let user_ids = rewritten
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(ids.value(0), 10);
        assert_eq!(ids.value(1), 50);
        assert_eq!(user_ids.value(0), "user-1");
        assert_eq!(user_ids.value(1), "user-2");
        assert_eq!(remap.get(&1), Some(&10));
        assert_eq!(remap.get(&2), Some(&50));
        assert_eq!(next_id, 51);
    }

    #[test]
    fn run_keyed_merge_insert_in_memory_updates_and_inserts() {
        let existing = node_batch_with_age(vec![10, 11], vec!["Alice", "Bob"], vec![30, 40]);
        let incoming = node_batch_with_age(vec![1, 2], vec!["Alice", "Cara"], vec![31, 22]);
        let mut next_id = 50;
        let (source_batch, remap) =
            rewrite_incoming_keyed_ids(&existing, &incoming, "name", &mut next_id).unwrap();
        assert_eq!(remap.get(&1), Some(&10));
        assert_eq!(remap.get(&2), Some(&50));

        let merged = run_keyed_merge_insert_in_memory(&existing, source_batch, "name").unwrap();
        assert_eq!(merged.num_rows(), 3);

        let ids = merged
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let names = merged
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = merged
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut by_name = HashMap::new();
        for row in 0..merged.num_rows() {
            by_name.insert(
                names.value(row).to_string(),
                (ids.value(row), ages.value(row)),
            );
        }

        assert_eq!(by_name.get("Alice"), Some(&(10, 31)));
        assert_eq!(by_name.get("Bob"), Some(&(11, 40)));
        assert_eq!(by_name.get("Cara"), Some(&(50, 22)));
    }

    #[test]
    fn run_keyed_merge_insert_in_memory_preserves_fixed_size_list_values() {
        let existing = node_batch_with_embedding(vec![10], vec!["a"], vec![[1.0, 0.0, 0.0]]);
        let incoming = node_batch_with_embedding(
            vec![1, 2],
            vec!["a", "b"],
            vec![[0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        );
        let mut next_id = 50;
        let (source_batch, remap) =
            rewrite_incoming_keyed_ids(&existing, &incoming, "slug", &mut next_id).unwrap();
        assert_eq!(remap.get(&1), Some(&10));
        assert_eq!(remap.get(&2), Some(&50));

        let merged = run_keyed_merge_insert_in_memory(&existing, source_batch, "slug").unwrap();
        let slugs = merged
            .column_by_name("slug")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let embeddings = merged
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let values = embeddings.values();
        let values = values.as_any().downcast_ref::<Float32Array>().unwrap();

        let mut by_slug = HashMap::new();
        for row in 0..merged.num_rows() {
            let start = row * 3;
            let vector = vec![
                values.value(start),
                values.value(start + 1),
                values.value(start + 2),
            ];
            by_slug.insert(slugs.value(row).to_string(), vector);
        }

        assert_eq!(by_slug.get("a"), Some(&vec![0.0, 1.0, 0.0]));
        assert_eq!(by_slug.get("b"), Some(&vec![0.0, 0.0, 1.0]));
    }

    #[test]
    fn remap_edge_batch_endpoints_updates_src_and_dst() {
        let batch = edge_batch(vec![1], vec![10], vec![20], vec![1999]);
        let src_remap = HashMap::from([(10_u64, 100_u64)]);
        let dst_remap = HashMap::from([(20_u64, 200_u64)]);

        let out = remap_edge_batch_endpoints(&batch, &src_remap, &dst_remap, "Knows").unwrap();
        let src = out
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let dst = out
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(src.value(0), 100);
        assert_eq!(dst.value(0), 200);
    }

    #[test]
    fn merge_edge_batches_dedups_by_endpoints_and_overwrites_with_incoming() {
        let existing = edge_batch(vec![1, 2], vec![10, 10], vec![20, 30], vec![1999, 2000]);
        let incoming = edge_batch(vec![5, 6], vec![10, 11], vec![20, 31], vec![2024, 2025]);

        let src_remap = HashMap::from([(11_u64, 12_u64)]);
        let dst_remap = HashMap::from([(31_u64, 32_u64)]);
        let mut next_edge_id = 42;

        let merged = merge_edge_batches(
            Some(&existing),
            Some(&incoming),
            &src_remap,
            &dst_remap,
            "Knows",
            true,
            &mut next_edge_id,
        )
        .unwrap();
        let merged = merged.unwrap();

        assert_eq!(merged.num_rows(), 3);
        assert_eq!(next_edge_id, 43);

        let src = merged
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let dst = merged
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let since = merged
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let mut by_endpoint: HashMap<(u64, u64), i32> = HashMap::new();
        for row in 0..merged.num_rows() {
            by_endpoint.insert((src.value(row), dst.value(row)), since.value(row));
        }

        assert_eq!(by_endpoint.get(&(10, 20)), Some(&2024));
        assert_eq!(by_endpoint.get(&(10, 30)), Some(&2000));
        assert_eq!(by_endpoint.get(&(12, 32)), Some(&2025));
    }

    #[test]
    fn merge_edge_batches_matches_properties_by_name_not_column_position() {
        let existing = edge_batch_with_role_and_started_at(
            vec![1],
            vec![10],
            vec![20],
            vec!["lead"],
            vec![20_000],
            false,
        );
        let incoming = edge_batch_with_role_and_started_at(
            vec![2],
            vec![30],
            vec![40],
            vec!["contributor"],
            vec![20_100],
            true,
        );

        let mut next_edge_id = 100;
        let merged = merge_edge_batches(
            Some(&existing),
            Some(&incoming),
            &HashMap::new(),
            &HashMap::new(),
            "WorksOn",
            true,
            &mut next_edge_id,
        )
        .unwrap();
        let merged = merged.unwrap();

        let src = merged
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let dst = merged
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let role = merged
            .column_by_name("role")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let started_at = merged
            .column_by_name("startedAt")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let mut by_endpoint = HashMap::new();
        for row in 0..merged.num_rows() {
            by_endpoint.insert(
                (src.value(row), dst.value(row)),
                (role.value(row).to_string(), started_at.value(row)),
            );
        }

        assert_eq!(
            by_endpoint.get(&(10, 20)),
            Some(&("lead".to_string(), 20_000))
        );
        assert_eq!(
            by_endpoint.get(&(30, 40)),
            Some(&("contributor".to_string(), 20_100))
        );
    }

    #[test]
    fn append_storage_appends_nodes_and_remaps_new_edge_endpoints() {
        let schema_src = r#"node Person {
    name: String
}
edge Knows: Person -> Person"#;
        let schema = parse_schema(schema_src).unwrap();
        let schema_ir = build_schema_ir(&schema).unwrap();
        let catalog = build_catalog_from_ir(&schema_ir).unwrap();

        let mut existing = DatasetAccumulator::new(catalog.clone());
        let person_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let existing_people = RecordBatch::try_new(
            person_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["Alice"])) as ArrayRef],
        )
        .unwrap();
        let existing_ids = existing.insert_nodes("Person", existing_people).unwrap();
        existing
            .insert_edges("Knows", &[existing_ids[0]], &[existing_ids[0]], None)
            .unwrap();

        let mut incoming = DatasetAccumulator::new(catalog);
        let incoming_people = RecordBatch::try_new(
            person_schema,
            vec![Arc::new(StringArray::from(vec!["Bob"])) as ArrayRef],
        )
        .unwrap();
        let incoming_ids = incoming.insert_nodes("Person", incoming_people).unwrap();
        incoming
            .insert_edges("Knows", &[incoming_ids[0]], &[incoming_ids[0]], None)
            .unwrap();

        let appended = append_storage(&existing, &incoming, &schema_ir).unwrap();
        let nodes = appended.storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(nodes.num_rows(), 2);

        let names = nodes
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = nodes
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let mut id_by_name = HashMap::new();
        for row in 0..nodes.num_rows() {
            id_by_name.insert(names.value(row).to_string(), ids.value(row));
        }

        let edges = appended
            .storage
            .edge_batch_for_save("Knows")
            .unwrap()
            .unwrap();
        assert_eq!(edges.num_rows(), 2);
        let src = edges
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let dst = edges
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let mut endpoints = Vec::new();
        for row in 0..edges.num_rows() {
            endpoints.push((src.value(row), dst.value(row)));
        }

        assert!(endpoints.contains(&(
            *id_by_name.get("Alice").unwrap(),
            *id_by_name.get("Alice").unwrap()
        )));
        assert!(endpoints.contains(&(
            *id_by_name.get("Bob").unwrap(),
            *id_by_name.get("Bob").unwrap()
        )));
    }
}
