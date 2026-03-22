use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::DataType;
use tracing::instrument;

use super::mutation::{DatasetMutationPlan, EdgeDelta, NodeDelta};
use super::persist::{
    collect_ids_from_batch, filter_record_batch_by_delete_mask, read_sparse_edge_batch,
    read_sparse_node_batch, select_record_batch_rows,
};
use super::{Database, DatabaseWriteGuard, DeleteOp, DeletePredicate, DeleteResult, MutationPlan};
use crate::error::{NanoError, Result};
use crate::store::metadata::DatabaseMetadata;
use crate::store::txlog::CdcLogEntry;

impl Database {
    /// Delete nodes of a given type matching a predicate, cascading incident edges.
    #[instrument(skip(self), fields(type_name = type_name, property = %predicate.property))]
    pub async fn delete_nodes(
        &self,
        type_name: &str,
        predicate: &DeletePredicate,
    ) -> Result<DeleteResult> {
        let mut writer = self.lock_writer().await;
        self.delete_nodes_locked(type_name, predicate, &mut writer)
            .await
    }

    pub(crate) async fn delete_nodes_locked(
        &self,
        type_name: &str,
        predicate: &DeletePredicate,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<DeleteResult> {
        let metadata = DatabaseMetadata::open(self.path())?;
        let target_batch = match read_sparse_node_batch(&metadata, type_name).await? {
            Some(batch) => batch,
            None => return Ok(DeleteResult::default()),
        };

        let delete_mask = build_delete_mask_for_mutation(&target_batch, predicate)?;
        let deleted_node_rows: Vec<usize> = (0..target_batch.num_rows())
            .filter(|&row| !delete_mask.is_null(row) && delete_mask.value(row))
            .collect();
        let deleted_batch = select_record_batch_rows(&target_batch, &deleted_node_rows)?;
        let deleted_node_ids = collect_ids_from_batch(&deleted_batch)?;
        if deleted_node_ids.is_empty() {
            return Ok(DeleteResult::default());
        }
        let deleted_node_set: HashSet<u64> = deleted_node_ids.into_iter().collect();
        let filtered_target =
            filter_record_batch_by_delete_mask(&target_batch, &delete_mask, "node")?;
        let mut mutation_plan = DatasetMutationPlan::new(
            "mutation:delete_nodes",
            metadata.manifest().next_node_id,
            metadata.manifest().next_edge_id,
        );
        mutation_plan.node_replacements.insert(
            type_name.to_string(),
            (filtered_target.num_rows() > 0).then_some(filtered_target),
        );
        mutation_plan.delta.node_changes.insert(
            type_name.to_string(),
            NodeDelta {
                delete_ids: deleted_node_set.iter().copied().collect(),
                before_for_deletes: Some(deleted_batch),
                ..Default::default()
            },
        );

        let mut deleted_edges = 0usize;
        for edge_def in self.schema_ir.edge_types() {
            if edge_def.src_type_name != type_name && edge_def.dst_type_name != type_name {
                continue;
            }
            let Some(edge_batch) = read_sparse_edge_batch(&metadata, &edge_def.name).await? else {
                continue;
            };
            let src_arr = edge_batch
                .column_by_name("src")
                .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
            let dst_arr = edge_batch
                .column_by_name("dst")
                .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;
            let deleted_edge_rows: Vec<usize> = (0..edge_batch.num_rows())
                .filter(|&row| {
                    deleted_node_set.contains(&src_arr.value(row))
                        || deleted_node_set.contains(&dst_arr.value(row))
                })
                .collect();
            if deleted_edge_rows.is_empty() {
                continue;
            }
            let filtered = filter_edge_batch_by_deleted_nodes(&edge_batch, &deleted_node_set)?;
            let deleted_edge_batch = select_record_batch_rows(&edge_batch, &deleted_edge_rows)?;
            let deleted_edge_ids = collect_ids_from_batch(&deleted_edge_batch)?;
            deleted_edges += deleted_edge_ids.len();
            mutation_plan.edge_replacements.insert(
                edge_def.name.clone(),
                (filtered.num_rows() > 0).then_some(filtered),
            );
            mutation_plan.delta.edge_changes.insert(
                edge_def.name.clone(),
                EdgeDelta {
                    delete_ids: deleted_edge_ids,
                    before_for_deletes: Some(deleted_edge_batch),
                    ..Default::default()
                },
            );
        }

        self.apply_mutation_plan_locked(MutationPlan::prepared_datasets(mutation_plan), writer)
            .await?;

        Ok(DeleteResult {
            deleted_nodes: deleted_node_set.len(),
            deleted_edges,
        })
    }

    /// Delete edges of a given type matching a predicate.
    #[instrument(skip(self), fields(type_name = type_name, property = %predicate.property))]
    pub async fn delete_edges(
        &self,
        type_name: &str,
        predicate: &DeletePredicate,
    ) -> Result<DeleteResult> {
        let mut writer = self.lock_writer().await;
        self.delete_edges_locked(type_name, predicate, &mut writer)
            .await
    }

    pub(crate) async fn delete_edges_locked(
        &self,
        type_name: &str,
        predicate: &DeletePredicate,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<DeleteResult> {
        if !self.catalog.edge_types.contains_key(type_name) {
            return Err(NanoError::Storage(format!(
                "unknown edge type `{}`",
                type_name
            )));
        }

        let metadata = DatabaseMetadata::open(self.path())?;
        let target_batch = match read_sparse_edge_batch(&metadata, type_name).await? {
            Some(batch) => batch,
            None => return Ok(DeleteResult::default()),
        };

        let delete_mask = build_delete_mask_for_mutation(&target_batch, predicate)?;
        let filtered_target =
            filter_record_batch_by_delete_mask(&target_batch, &delete_mask, "edge")?;
        let deleted_edges = target_batch
            .num_rows()
            .saturating_sub(filtered_target.num_rows());
        if deleted_edges == 0 {
            return Ok(DeleteResult::default());
        }
        let deleted_edge_rows: Vec<usize> = (0..target_batch.num_rows())
            .filter(|&row| !delete_mask.is_null(row) && delete_mask.value(row))
            .collect();
        let deleted_batch = select_record_batch_rows(&target_batch, &deleted_edge_rows)?;
        let delete_ids = collect_ids_from_batch(&deleted_batch)?;
        let mut mutation_plan = DatasetMutationPlan::new(
            "mutation:delete_edges",
            metadata.manifest().next_node_id,
            metadata.manifest().next_edge_id,
        );
        mutation_plan.edge_replacements.insert(
            type_name.to_string(),
            (filtered_target.num_rows() > 0).then_some(filtered_target),
        );
        mutation_plan.delta.edge_changes.insert(
            type_name.to_string(),
            EdgeDelta {
                delete_ids,
                before_for_deletes: Some(deleted_batch),
                ..Default::default()
            },
        );
        self.apply_mutation_plan_locked(MutationPlan::prepared_datasets(mutation_plan), writer)
            .await?;

        Ok(DeleteResult {
            deleted_nodes: 0,
            deleted_edges,
        })
    }
}

fn parse_predicate_array(value: &str, dt: &DataType, num_rows: usize) -> Result<ArrayRef> {
    let trim_quotes = trim_surrounding_quotes(value);
    let arr: ArrayRef = match dt {
        DataType::Utf8 => Arc::new(StringArray::from(vec![trim_quotes; num_rows])),
        DataType::Boolean => {
            let parsed = trim_quotes.parse::<bool>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid boolean literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(BooleanArray::from(vec![parsed; num_rows]))
        }
        DataType::Int32 => {
            let parsed = trim_quotes.parse::<i32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid i32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Int32Array::from(vec![parsed; num_rows]))
        }
        DataType::Int64 => {
            let parsed = trim_quotes.parse::<i64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid i64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Int64Array::from(vec![parsed; num_rows]))
        }
        DataType::UInt32 => {
            let parsed = trim_quotes.parse::<u32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid u32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(UInt32Array::from(vec![parsed; num_rows]))
        }
        DataType::UInt64 => {
            let parsed = trim_quotes.parse::<u64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid u64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(UInt64Array::from(vec![parsed; num_rows]))
        }
        DataType::Float32 => {
            let parsed = trim_quotes.parse::<f32>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid f32 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Float32Array::from(vec![parsed; num_rows]))
        }
        DataType::Float64 => {
            let parsed = trim_quotes.parse::<f64>().map_err(|_| {
                NanoError::Storage(format!(
                    "invalid f64 literal '{}' for delete predicate",
                    value
                ))
            })?;
            Arc::new(Float64Array::from(vec![parsed; num_rows]))
        }
        DataType::Date32 => {
            let base: ArrayRef = if let Ok(parsed) = trim_quotes.parse::<i32>() {
                Arc::new(Int32Array::from(vec![parsed; num_rows]))
            } else {
                Arc::new(StringArray::from(vec![trim_quotes; num_rows]))
            };
            arrow_cast::cast(&base, &DataType::Date32).map_err(|e| {
                NanoError::Storage(format!(
                    "invalid Date32 literal '{}' for delete predicate (expected ISO date string or days since epoch): {}",
                    value, e
                ))
            })?
        }
        DataType::Date64 => {
            let base: ArrayRef = if let Ok(parsed) = trim_quotes.parse::<i64>() {
                Arc::new(Int64Array::from(vec![parsed; num_rows]))
            } else {
                Arc::new(StringArray::from(vec![trim_quotes; num_rows]))
            };
            arrow_cast::cast(&base, &DataType::Date64).map_err(|e| {
                NanoError::Storage(format!(
                    "invalid Date64 literal '{}' for delete predicate (expected ISO datetime string or ms since epoch): {}",
                    value, e
                ))
            })?
        }
        _ => {
            return Err(NanoError::Storage(format!(
                "delete predicate on unsupported data type {:?}",
                dt
            )));
        }
    };

    Ok(arr)
}

pub(super) fn trim_surrounding_quotes(s: &str) -> &str {
    if s.len() >= 2 {
        let bytes = s.as_bytes();
        let first = bytes[0];
        let last = bytes[s.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &s[1..s.len() - 1];
        }
    }
    s
}

fn compare_for_delete(left: &ArrayRef, right: &ArrayRef, op: DeleteOp) -> Result<BooleanArray> {
    use arrow_ord::cmp;

    match op {
        DeleteOp::Eq => cmp::eq(left, right),
        DeleteOp::Ne => cmp::neq(left, right),
        DeleteOp::Gt => cmp::gt(left, right),
        DeleteOp::Ge => cmp::gt_eq(left, right),
        DeleteOp::Lt => cmp::lt(left, right),
        DeleteOp::Le => cmp::lt_eq(left, right),
    }
    .map_err(|e| NanoError::Storage(format!("delete predicate compare error: {}", e)))
}

pub(super) fn build_delete_mask_for_mutation(
    batch: &RecordBatch,
    predicate: &DeletePredicate,
) -> Result<BooleanArray> {
    let left = batch
        .column_by_name(&predicate.property)
        .ok_or_else(|| {
            NanoError::Storage(format!(
                "property '{}' not found for delete predicate",
                predicate.property
            ))
        })?
        .clone();
    let right = parse_predicate_array(&predicate.value, left.data_type(), batch.num_rows())?;
    compare_for_delete(&left, &right, predicate.op)
}

fn filter_edge_batch_by_deleted_nodes(
    batch: &RecordBatch,
    deleted_node_ids: &HashSet<u64>,
) -> Result<RecordBatch> {
    if deleted_node_ids.is_empty() {
        return Ok(batch.clone());
    }

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

    let mut keep_builder = BooleanBuilder::with_capacity(batch.num_rows());
    let mut kept_rows = 0usize;
    for row in 0..batch.num_rows() {
        let keep = !deleted_node_ids.contains(&src_arr.value(row))
            && !deleted_node_ids.contains(&dst_arr.value(row));
        keep_builder.append_value(keep);
        if keep {
            kept_rows += 1;
        }
    }
    if kept_rows == batch.num_rows() {
        return Ok(batch.clone());
    }
    if kept_rows == 0 {
        return Ok(RecordBatch::new_empty(batch.schema()));
    }

    let keep_mask = keep_builder.finish();
    arrow_select::filter::filter_record_batch(batch, &keep_mask)
        .map_err(|e| NanoError::Storage(format!("edge delete filter error: {}", e)))
}

pub(super) fn build_delete_cdc_events_from_rows(
    batch: &RecordBatch,
    entity_kind: &str,
    type_name: &str,
    rows: &[usize],
) -> Result<Vec<CdcLogEntry>> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let payload = record_batch_row_to_json_map(batch, *row)?;
        let id = cdc_internal_entity_id_from_row(&payload).ok_or_else(|| {
            NanoError::Storage(format!(
                "missing id field while building delete CDC for {} {}",
                entity_kind, type_name
            ))
        })?;
        events.push(make_pending_cdc_event(
            "delete",
            entity_kind,
            type_name,
            id,
            &payload,
            serde_json::Value::Object(payload.clone()),
        ));
    }
    Ok(events)
}

pub(crate) fn build_delete_cdc_events_from_batch(
    batch: &RecordBatch,
    entity_kind: &str,
    type_name: &str,
) -> Result<Vec<CdcLogEntry>> {
    let rows: Vec<usize> = (0..batch.num_rows()).collect();
    build_delete_cdc_events_from_rows(batch, entity_kind, type_name, &rows)
}

pub(crate) fn build_insert_cdc_events_from_batch(
    batch: &RecordBatch,
    entity_kind: &str,
    type_name: &str,
) -> Result<Vec<CdcLogEntry>> {
    let mut events = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let payload = record_batch_row_to_json_map(batch, row)?;
        let id = cdc_internal_entity_id_from_row(&payload).ok_or_else(|| {
            NanoError::Storage(format!(
                "missing id field while building insert CDC for {} {}",
                entity_kind, type_name
            ))
        })?;
        events.push(make_pending_cdc_event(
            "insert",
            entity_kind,
            type_name,
            id,
            &payload,
            serde_json::Value::Object(payload.clone()),
        ));
    }
    Ok(events)
}

pub(crate) fn build_update_cdc_event(
    entity_kind: &str,
    type_name: &str,
    before: &serde_json::Map<String, serde_json::Value>,
    after: &serde_json::Map<String, serde_json::Value>,
) -> Option<CdcLogEntry> {
    if before == after {
        return None;
    }

    let id = cdc_internal_entity_id_from_row(after)
        .or_else(|| cdc_internal_entity_id_from_row(before))?;
    Some(make_pending_cdc_event(
        "update",
        entity_kind,
        type_name,
        id,
        after,
        serde_json::json!({
            "before": before,
            "after": after,
        }),
    ))
}

pub(crate) fn record_batch_row_to_json_map(
    batch: &RecordBatch,
    row: usize,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut map = serde_json::Map::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let value = cdc_array_value_to_json(batch.column(col_idx), row);
        if col_idx == 0 && field.name() == "id" {
            map.insert("__ng_id".to_string(), value.clone());
        }
        map.insert(field.name().clone(), value);
    }
    Ok(map)
}

fn cdc_array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
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
                        .map(|idx| cdc_array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|a| {
                let values = a.value(row);
                serde_json::Value::Array(
                    (0..values.len())
                        .map(|idx| cdc_array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::String(
            arrow_cast::display::array_value_to_string(array, row).unwrap_or_default(),
        ),
    }
}

pub(crate) fn make_pending_cdc_event(
    op: &str,
    entity_kind: &str,
    type_name: &str,
    id: u64,
    row: &serde_json::Map<String, serde_json::Value>,
    payload: serde_json::Value,
) -> CdcLogEntry {
    CdcLogEntry {
        tx_id: String::new(),
        db_version: 0,
        seq_in_tx: 0,
        op: op.to_string(),
        entity_kind: entity_kind.to_string(),
        type_name: type_name.to_string(),
        entity_key: cdc_entity_key(entity_kind, id, row),
        payload,
        committed_at: String::new(),
    }
}

fn cdc_entity_key(
    entity_kind: &str,
    id: u64,
    row: &serde_json::Map<String, serde_json::Value>,
) -> String {
    if entity_kind == "edge" {
        let src = row.get("src").and_then(|v| v.as_u64());
        let dst = row.get("dst").and_then(|v| v.as_u64());
        if let (Some(src), Some(dst)) = (src, dst) {
            return format!("id={},src={},dst={}", id, src, dst);
        }
    }
    format!("id={}", id)
}

fn cdc_internal_entity_id_from_row(
    row: &serde_json::Map<String, serde_json::Value>,
) -> Option<u64> {
    row.get("__ng_id")
        .and_then(|value| value.as_u64())
        .or_else(|| row.get("id").and_then(|value| value.as_u64()))
}
