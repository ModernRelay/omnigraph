use std::collections::{HashMap, HashSet};

use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Date32Array, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, RecordBatchIterator, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use base64::Engine;
use futures::TryStreamExt;
use lance::blob::BlobArrayBuilder;
use omnigraph_compiler::catalog::NodeType;
use serde_json::Value as JsonValue;

use crate::db::Omnigraph;
use crate::error::{OmniError, Result};

/// Result of a load operation.
#[derive(Debug, Clone, Default)]
pub struct LoadResult {
    pub nodes_loaded: HashMap<String, usize>,
    pub edges_loaded: HashMap<String, usize>,
}

/// Load mode for data ingestion.
#[derive(Debug, Clone, Copy)]
pub enum LoadMode {
    /// Overwrite existing data.
    Overwrite,
    /// Append to existing data.
    Append,
    /// Merge by `id` key (upsert).
    Merge,
}

/// Load JSONL data into an Omnigraph database.
pub async fn load_jsonl(db: &mut Omnigraph, data: &str, mode: LoadMode) -> Result<LoadResult> {
    let reader = BufReader::new(Cursor::new(data.as_bytes()));
    load_jsonl_reader(db, reader, mode).await
}

/// Load JSONL data from a file path.
pub async fn load_jsonl_file(db: &mut Omnigraph, path: &str, mode: LoadMode) -> Result<LoadResult> {
    let file = std::fs::File::open(path).map_err(|e| OmniError::Io(e))?;
    let reader = BufReader::new(file);
    load_jsonl_reader(db, reader, mode).await
}

impl Omnigraph {
    pub async fn load(&mut self, branch: &str, data: &str, mode: LoadMode) -> Result<LoadResult> {
        let requested = Self::normalize_branch_name(branch)?;
        let current = self.active_branch().map(str::to_string);
        if requested == current {
            return load_jsonl(self, data, mode).await;
        }

        let mut other = self.reopen_for_branch(requested.as_deref()).await?;
        load_jsonl(&mut other, data, mode).await
    }

    pub async fn load_file(
        &mut self,
        branch: &str,
        path: &str,
        mode: LoadMode,
    ) -> Result<LoadResult> {
        let requested = Self::normalize_branch_name(branch)?;
        let current = self.active_branch().map(str::to_string);
        if requested == current {
            return load_jsonl_file(self, path, mode).await;
        }

        let mut other = self.reopen_for_branch(requested.as_deref()).await?;
        load_jsonl_file(&mut other, path, mode).await
    }
}

async fn load_jsonl_reader<R: BufRead>(
    db: &mut Omnigraph,
    reader: R,
    mode: LoadMode,
) -> Result<LoadResult> {
    let catalog = db.catalog().clone();

    // Phase 1: Parse all lines, spool into per-type collections
    let mut node_rows: HashMap<String, Vec<JsonValue>> = HashMap::new();
    let mut edge_rows: HashMap<String, Vec<(String, String, JsonValue)>> = HashMap::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(line).map_err(|e| {
            OmniError::Manifest(format!("invalid JSON on line {}: {}", line_num + 1, e))
        })?;

        if let Some(type_name) = value.get("type").and_then(|v| v.as_str()) {
            if !catalog.node_types.contains_key(type_name) {
                return Err(OmniError::Manifest(format!(
                    "line {}: unknown node type '{}'",
                    line_num + 1,
                    type_name
                )));
            }
            let data = value
                .get("data")
                .cloned()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            node_rows
                .entry(type_name.to_string())
                .or_default()
                .push(data);
        } else if let Some(edge_name) = value.get("edge").and_then(|v| v.as_str()) {
            if catalog.lookup_edge_by_name(edge_name).is_none() {
                return Err(OmniError::Manifest(format!(
                    "line {}: unknown edge type '{}'",
                    line_num + 1,
                    edge_name
                )));
            }
            let from = value
                .get("from")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::Manifest(format!("line {}: edge missing 'from'", line_num + 1))
                })?
                .to_string();
            let to = value
                .get("to")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::Manifest(format!("line {}: edge missing 'to'", line_num + 1))
                })?
                .to_string();
            let data = value
                .get("data")
                .cloned()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            let canonical = catalog.lookup_edge_by_name(edge_name).unwrap().name.clone();
            edge_rows
                .entry(canonical)
                .or_default()
                .push((from, to, data));
        } else {
            return Err(OmniError::Manifest(format!(
                "line {}: expected 'type' or 'edge' field",
                line_num + 1
            )));
        }
    }

    // Phase 2: Build per-type RecordBatches and write to Lance

    let mut updates = Vec::new();
    let mut result = LoadResult::default();
    let snapshot = db.snapshot();

    // Write nodes first (edges reference node IDs)
    for (type_name, rows) in &node_rows {
        let node_type = &catalog.node_types[type_name];
        let batch = build_node_batch(node_type, rows)?;

        // Validate value constraints before writing
        validate_value_constraints(&batch, node_type)?;

        let loaded_count = batch.num_rows();

        let table_key = format!("node:{}", type_name);
        snapshot
            .entry(&table_key)
            .ok_or_else(|| OmniError::Manifest(format!("no manifest entry for {}", table_key)))?;

        let (new_version, total_rows, table_branch) =
            write_batch_to_dataset(db, &table_key, batch, mode).await?;

        updates.push(crate::db::SubTableUpdate {
            table_key,
            table_version: new_version,
            table_branch,
            row_count: total_rows,
        });
        result.nodes_loaded.insert(type_name.clone(), loaded_count);
    }

    // Phase 2b: Validate edge referential integrity — every src/dst must
    // reference an existing node ID in the appropriate type.
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let from_ids =
            collect_node_ids(db, &edge_type.from_type, &node_rows, &catalog, &updates).await?;
        let to_ids =
            collect_node_ids(db, &edge_type.to_type, &node_rows, &catalog, &updates).await?;

        for (i, (src, dst, _)) in rows.iter().enumerate() {
            if !from_ids.contains(src.as_str()) {
                return Err(OmniError::Manifest(format!(
                    "edge {} row {}: src '{}' not found in {}",
                    edge_name,
                    i + 1,
                    src,
                    edge_type.from_type
                )));
            }
            if !to_ids.contains(dst.as_str()) {
                return Err(OmniError::Manifest(format!(
                    "edge {} row {}: dst '{}' not found in {}",
                    edge_name,
                    i + 1,
                    dst,
                    edge_type.to_type
                )));
            }
        }
    }

    // Write edges
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let batch = build_edge_batch(edge_type, rows)?;
        let loaded_count = batch.num_rows();

        let table_key = format!("edge:{}", edge_name);
        snapshot
            .entry(&table_key)
            .ok_or_else(|| OmniError::Manifest(format!("no manifest entry for {}", table_key)))?;

        let (new_version, total_rows, table_branch) =
            write_batch_to_dataset(db, &table_key, batch, mode).await?;

        updates.push(crate::db::SubTableUpdate {
            table_key,
            table_version: new_version,
            table_branch,
            row_count: total_rows,
        });
        result.edges_loaded.insert(edge_name.clone(), loaded_count);
    }

    // Phase 3: Validate edge cardinality constraints (before commit — invalid
    // data must not be committed). Opens edge sub-tables at their just-written
    // versions, not through the snapshot (which still pins to pre-write state).
    for (edge_name, _) in &edge_rows {
        let table_key = format!("edge:{}", edge_name);
        if let Some(update) = updates.iter().find(|u| u.table_key == table_key) {
            validate_edge_cardinality(
                db,
                edge_name,
                update.table_version,
                update.table_branch.as_deref(),
            )
            .await?;
        }
    }

    // Phase 4: Atomic manifest commit
    db.commit_updates(&updates).await?;

    // Phase 5: Ensure scalar indices on key columns
    db.ensure_indices().await?;

    Ok(result)
}

fn build_node_batch(node_type: &NodeType, rows: &[JsonValue]) -> Result<RecordBatch> {
    let schema = node_type.arrow_schema.clone();

    // Build id column: @key value or ULID
    let ids: Vec<String> = rows
        .iter()
        .map(|row| {
            if let Some(key_prop) = node_type.key_property() {
                row.get(key_prop)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        OmniError::Manifest(format!(
                            "node {} missing @key property '{}'",
                            node_type.name, key_prop
                        ))
                    })
            } else {
                Ok(generate_id())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)));

    // Build property columns (skip "id" field at index 0)
    for field in schema.fields().iter().skip(1) {
        if node_type.blob_properties.contains(field.name()) {
            let col = build_blob_column(field.name(), field.is_nullable(), rows)?;
            columns.push(col);
        } else {
            let col =
                build_column_from_json(field.name(), field.data_type(), field.is_nullable(), rows)?;
            columns.push(col);
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

fn build_edge_batch(
    edge_type: &omnigraph_compiler::catalog::EdgeType,
    rows: &[(String, String, JsonValue)],
) -> Result<RecordBatch> {
    let schema = edge_type.arrow_schema.clone();

    let ids: Vec<String> = (0..rows.len()).map(|_| generate_id()).collect();
    let srcs: Vec<&str> = rows.iter().map(|(from, _, _)| from.as_str()).collect();
    let dsts: Vec<&str> = rows.iter().map(|(_, to, _)| to.as_str()).collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)));
    columns.push(Arc::new(StringArray::from(srcs)));
    columns.push(Arc::new(StringArray::from(dsts)));

    // Build edge property columns (skip id, src, dst at indices 0-2)
    let data_values: Vec<JsonValue> = rows.iter().map(|(_, _, data)| data.clone()).collect();
    for field in schema.fields().iter().skip(3) {
        if edge_type.blob_properties.contains(field.name()) {
            let col = build_blob_column(field.name(), field.is_nullable(), &data_values)?;
            columns.push(col);
        } else {
            let col = build_column_from_json(
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &data_values,
            )?;
            columns.push(col);
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Append a blob value (URI or base64 bytes) to a BlobArrayBuilder.
pub(crate) fn append_blob_value(builder: &mut BlobArrayBuilder, value: &str) -> Result<()> {
    if let Some(encoded) = value.strip_prefix("base64:") {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| OmniError::Manifest(format!("invalid base64 blob data: {}", e)))?;
        builder
            .push_bytes(bytes)
            .map_err(|e| OmniError::Lance(e.to_string()))
    } else {
        // Treat as URI (file://, s3://, gs://, or any other scheme)
        builder
            .push_uri(value)
            .map_err(|e| OmniError::Lance(e.to_string()))
    }
}

/// Build a blob column from JSON values using Lance BlobArrayBuilder.
fn build_blob_column(name: &str, nullable: bool, rows: &[JsonValue]) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(rows.len());
    for row in rows {
        match row.get(name) {
            Some(JsonValue::String(s)) => {
                append_blob_value(&mut builder, s)?;
            }
            Some(JsonValue::Null) | None if nullable => {
                builder
                    .push_null()
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            }
            Some(JsonValue::Null) | None => {
                return Err(OmniError::Manifest(format!(
                    "non-nullable blob property '{}' has null values",
                    name
                )));
            }
            _ => {
                return Err(OmniError::Manifest(format!(
                    "blob property '{}' must be a URI string or base64: prefixed data",
                    name
                )));
            }
        }
    }
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn build_column_from_json(
    name: &str,
    data_type: &DataType,
    nullable: bool,
    rows: &[JsonValue],
) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(name)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            if !nullable && values.iter().any(|v| v.is_none()) {
                return Err(OmniError::Manifest(format!(
                    "non-nullable property '{}' has null values",
                    name
                )));
            }
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()).map(|v| v as i32))
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()).map(|v| v as u32))
                .collect();
            Ok(Arc::new(UInt32Array::from(values)))
        }
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()))
                .collect();
            Ok(Arc::new(UInt64Array::from(values)))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()).map(|v| v as f32))
                .collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()))
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_bool()))
                .collect();
            Ok(Arc::new(arrow_array::BooleanArray::from(values)))
        }
        DataType::Date32 => {
            // Expect ISO date strings like "2024-01-15"
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    row.get(name)
                        .and_then(|v| v.as_str())
                        .and_then(|s| parse_date32(s))
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::FixedSizeList(child_field, dim) => {
            // Vector type: parse JSON array of floats into FixedSizeList<Float32>
            let dim = *dim;
            let mut flat_values: Vec<f32> = Vec::with_capacity(rows.len() * dim as usize);
            for row in rows {
                if let Some(arr) = row.get(name).and_then(|v| v.as_array()) {
                    if arr.len() != dim as usize {
                        return Err(OmniError::Manifest(format!(
                            "vector property '{}' expects {} dimensions, got {}",
                            name,
                            dim,
                            arr.len()
                        )));
                    }
                    for val in arr {
                        flat_values.push(val.as_f64().unwrap_or(0.0) as f32);
                    }
                } else if nullable {
                    flat_values.extend(std::iter::repeat(0.0f32).take(dim as usize));
                } else {
                    return Err(OmniError::Manifest(format!(
                        "non-nullable vector property '{}' has null values",
                        name
                    )));
                }
            }
            let values_array = Arc::new(Float32Array::from(flat_values));
            let list_array =
                FixedSizeListArray::try_new(child_field.clone(), dim, values_array, None)
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            Ok(Arc::new(list_array))
        }
        _ => {
            // Unsupported type: fill with nulls
            let values: Vec<Option<&str>> = vec![None; rows.len()];
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

/// Write a batch to a Lance dataset, returning (new_version, total_row_count).
async fn write_batch_to_dataset(
    db: &Omnigraph,
    table_key: &str,
    batch: RecordBatch,
    mode: LoadMode,
) -> Result<(u64, u64, Option<String>)> {
    let (mut ds, _full_path, table_branch) = db.open_for_mutation(table_key).await?;
    if batch.num_rows() == 0 {
        let row_count = ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
        return Ok((ds.version().version, row_count, table_branch));
    }

    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

    match mode {
        LoadMode::Overwrite => {
            ds.truncate_table()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            ds.append(reader, None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let row_count = ds
                .count_rows(None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
            Ok((ds.version().version, row_count, table_branch))
        }
        LoadMode::Append => {
            ds.append(reader, None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            // Use the mutated handle directly — no re-open needed
            let row_count = ds
                .count_rows(None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
            Ok((ds.version().version, row_count, table_branch))
        }
        LoadMode::Merge => {
            // merge_insert keyed by "id"
            let ds = Arc::new(ds);
            let job = lance::dataset::MergeInsertBuilder::try_new(ds, vec!["id".to_string()])
                .map_err(|e| OmniError::Lance(e.to_string()))?
                .when_matched(lance::dataset::WhenMatched::UpdateAll)
                .when_not_matched(lance::dataset::WhenNotMatched::InsertAll)
                .try_build()
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            let (new_ds, _stats) = job
                .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let row_count = new_ds
                .count_rows(None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
            Ok((new_ds.version().version, row_count, table_branch))
        }
    }
}

fn generate_id() -> String {
    ulid::Ulid::new().to_string()
}

fn parse_date32(s: &str) -> Option<i32> {
    // Parse "YYYY-MM-DD" → days since epoch
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;

    // Days from 1970-01-01 using a simple calculation
    // (accurate enough for testing; production would use chrono)
    let days = days_from_civil(y, m, d);
    Some(days)
}

fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32 - 719468) as i32
}

// ─── Value constraint validation ─────────────────────────────────────────────

pub(crate) fn validate_value_constraints(
    batch: &RecordBatch,
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Result<()> {
    use arrow_array::Array;

    // Range constraints
    for rc in &node_type.range_constraints {
        let Some(col) = batch.column_by_name(&rc.property) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if col.is_null(row) {
                continue;
            }
            let value = extract_numeric_value(col, row);
            if let Some(val) = value {
                if let Some(ref min) = rc.min {
                    let min_f = literal_value_to_f64(min);
                    if val < min_f {
                        return Err(OmniError::Manifest(format!(
                            "@range violation on {}.{}: value {} < min {}",
                            node_type.name, rc.property, val, min_f
                        )));
                    }
                }
                if let Some(ref max) = rc.max {
                    let max_f = literal_value_to_f64(max);
                    if val > max_f {
                        return Err(OmniError::Manifest(format!(
                            "@range violation on {}.{}: value {} > max {}",
                            node_type.name, rc.property, val, max_f
                        )));
                    }
                }
            }
        }
    }

    // Check constraints (regex)
    for cc in &node_type.check_constraints {
        let re = regex::Regex::new(&cc.pattern).map_err(|e| {
            OmniError::Manifest(format!(
                "@check on {}.{} has invalid regex '{}': {}",
                node_type.name, cc.property, cc.pattern, e
            ))
        })?;
        let Some(col) = batch.column_by_name(&cc.property) else {
            continue;
        };
        let str_col = col.as_any().downcast_ref::<StringArray>();
        if let Some(str_col) = str_col {
            for row in 0..str_col.len() {
                if str_col.is_null(row) {
                    continue;
                }
                let val = str_col.value(row);
                if !re.is_match(val) {
                    return Err(OmniError::Manifest(format!(
                        "@check violation on {}.{}: value '{}' does not match pattern '{}'",
                        node_type.name, cc.property, val, cc.pattern
                    )));
                }
            }
        }
    }

    Ok(())
}

fn extract_numeric_value(col: &ArrayRef, row: usize) -> Option<f64> {
    use arrow_array::{
        Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
    };
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    None
}

fn literal_value_to_f64(v: &omnigraph_compiler::catalog::LiteralValue) -> f64 {
    use omnigraph_compiler::catalog::LiteralValue;
    match v {
        LiteralValue::Integer(n) => *n as f64,
        LiteralValue::Float(f) => *f,
    }
}

// ─── Edge cardinality validation ─────────────────────────────────────────────

async fn validate_edge_cardinality(
    db: &crate::db::Omnigraph,
    edge_name: &str,
    written_version: u64,
    written_branch: Option<&str>,
) -> Result<()> {
    use arrow_array::Array;
    let catalog = db.catalog();
    let edge_type = &catalog.edge_types[edge_name];
    if edge_type.cardinality.is_default() {
        return Ok(());
    }

    // Open edge sub-table at the just-written version, not the snapshot's
    // (the snapshot still pins to the pre-write version).
    let snapshot = db.snapshot();
    let table_key = format!("edge:{}", edge_name);
    let entry = snapshot
        .entry(&table_key)
        .ok_or_else(|| OmniError::Manifest(format!("no manifest entry for {}", table_key)))?;
    let ds = db
        .open_dataset_at_state(
            &entry.table_path,
            written_branch.or(entry.table_branch.as_deref()),
            written_version,
        )
        .await?;

    // Scan src column, count per source
    let batches: Vec<RecordBatch> = ds
        .scan()
        .project(&["src"])
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut counts: HashMap<String, u32> = HashMap::new();
    for batch in &batches {
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..srcs.len() {
            *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
        }
    }

    let card = &edge_type.cardinality;
    for (src, count) in &counts {
        if let Some(max) = card.max {
            if *count > max {
                return Err(OmniError::Manifest(format!(
                    "@card violation on edge {}: source '{}' has {} edges (max {})",
                    edge_name, src, count, max
                )));
            }
        }
        if *count < card.min {
            return Err(OmniError::Manifest(format!(
                "@card violation on edge {}: source '{}' has {} edges (min {})",
                edge_name, src, count, card.min
            )));
        }
    }

    Ok(())
}

/// Collect all valid node IDs for a given type. Union of:
/// - IDs from the just-loaded batch (in memory, from node_rows)
/// - IDs from the sub-table at the just-written version (if it was updated)
/// - IDs from the sub-table at the snapshot-pinned version (if it was not updated)
async fn collect_node_ids(
    db: &Omnigraph,
    type_name: &str,
    node_rows: &HashMap<String, Vec<JsonValue>>,
    catalog: &omnigraph_compiler::catalog::Catalog,
    updates: &[crate::db::SubTableUpdate],
) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();

    // IDs from the in-memory batch (just loaded in this operation)
    if let Some(rows) = node_rows.get(type_name) {
        if let Some(node_type) = catalog.node_types.get(type_name) {
            if let Some(key_prop) = node_type.key_property() {
                for row in rows {
                    if let Some(id) = row.get(key_prop).and_then(|v| v.as_str()) {
                        ids.insert(id.to_string());
                    }
                }
            }
        }
    }

    // IDs from the Lance sub-table
    let table_key = format!("node:{}", type_name);
    let snapshot = db.snapshot();
    let Some(entry) = snapshot.entry(&table_key) else {
        return Ok(ids);
    };
    // Use the just-written version if this type was updated, else snapshot version
    let updated = updates
        .iter()
        .find(|u| u.table_key == table_key)
        .map(|u| (u.table_version, u.table_branch.as_deref()));
    let (version, branch) = updated.unwrap_or((entry.table_version, entry.table_branch.as_deref()));
    let ds = db
        .open_dataset_at_state(&entry.table_path, branch, version)
        .await?;

    let batches: Vec<RecordBatch> = ds
        .scan()
        .project(&["id"])
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    for batch in &batches {
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            ids.insert(id_col.value(i).to_string());
        }
    }

    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Omnigraph;
    use arrow_array::Array;
    use futures::TryStreamExt;

    const TEST_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#;

    const TEST_DATA: &str = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
"#;

    #[tokio::test]
    async fn test_load_creates_data() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert_eq!(result.nodes_loaded["Person"], 2);
        assert_eq!(result.nodes_loaded["Company"], 1);
        assert_eq!(result.edges_loaded["Knows"], 1);
        assert_eq!(result.edges_loaded["WorksAt"], 1);
    }

    #[tokio::test]
    async fn test_load_data_readable_via_lance() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        // Read back via snapshot
        let snap = db.snapshot();
        let person_ds = snap.open("node:Person").await.unwrap();

        assert_eq!(person_ds.count_rows(None).await.unwrap(), 2);

        // Verify data
        let batches: Vec<RecordBatch> = person_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // @key=name, so ids should be "Alice" and "Bob"
        let id_values: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
        assert!(id_values.contains(&"Alice"));
        assert!(id_values.contains(&"Bob"));
    }

    #[tokio::test]
    async fn test_load_edges_reference_node_keys() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        let snap = db.snapshot();
        let knows_ds = snap.open("edge:Knows").await.unwrap();

        let batches: Vec<RecordBatch> = knows_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let batch = &batches[0];
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dsts = batch
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(srcs.value(0), "Alice");
        assert_eq!(dsts.value(0), "Bob");
    }

    #[tokio::test]
    async fn test_load_manifest_version_advances() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        let v1 = db.version();

        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert!(db.version() > v1);
    }

    #[tokio::test]
    async fn test_load_append_adds_rows() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let batch1 = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#;
        let batch2 = r#"{"type": "Person", "data": {"name": "Bob", "age": 25}}"#;

        load_jsonl(&mut db, batch1, LoadMode::Overwrite)
            .await
            .unwrap();
        load_jsonl(&mut db, batch2, LoadMode::Append).await.unwrap();

        let snap = db.snapshot();
        let person_ds = snap.open("node:Person").await.unwrap();
        assert_eq!(person_ds.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_load_unknown_type_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let bad = r#"{"type": "FakeType", "data": {"name": "x"}}"#;
        let result = load_jsonl(&mut db, bad, LoadMode::Overwrite).await;
        assert!(result.is_err());
    }
}
