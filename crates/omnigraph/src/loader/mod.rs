use std::collections::{HashMap, HashSet};

use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::DataType;
use base64::Engine;
use lance::blob::BlobArrayBuilder;
use omnigraph_compiler::catalog::NodeType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::db::Omnigraph;
use crate::error::{OmniError, Result};

/// Result of a load operation.
#[derive(Debug, Clone, Default)]
pub struct LoadResult {
    pub nodes_loaded: HashMap<String, usize>,
    pub edges_loaded: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestTableResult {
    pub table_key: String,
    pub rows_loaded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResult {
    pub branch: String,
    pub base_branch: String,
    pub branch_created: bool,
    pub mode: LoadMode,
    pub tables: Vec<IngestTableResult>,
}

/// Load mode for data ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    let current_branch = db.active_branch().map(str::to_string);
    let branch = current_branch.as_deref().unwrap_or("main");
    db.load(branch, data, mode).await
}

/// Load JSONL data from a file path.
pub async fn load_jsonl_file(db: &mut Omnigraph, path: &str, mode: LoadMode) -> Result<LoadResult> {
    let current_branch = db.active_branch().map(str::to_string);
    let branch = current_branch.as_deref().unwrap_or("main");
    db.load_file(branch, path, mode).await
}

impl Omnigraph {
    pub async fn ingest(
        &mut self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: LoadMode,
    ) -> Result<IngestResult> {
        self.ingest_as(branch, from, data, mode, None).await
    }

    pub async fn ingest_as(
        &mut self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<IngestResult> {
        let previous_actor = self.audit_actor_id.clone();
        self.audit_actor_id = actor_id.map(str::to_string);
        let result = self
            .ingest_with_current_actor(branch, from, data, mode)
            .await;
        self.audit_actor_id = previous_actor;
        result
    }

    pub async fn ingest_file(
        &mut self,
        branch: &str,
        from: Option<&str>,
        path: &str,
        mode: LoadMode,
    ) -> Result<IngestResult> {
        self.ingest_file_as(branch, from, path, mode, None).await
    }

    pub async fn ingest_file_as(
        &mut self,
        branch: &str,
        from: Option<&str>,
        path: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<IngestResult> {
        let data = std::fs::read_to_string(path).map_err(OmniError::Io)?;
        self.ingest_as(branch, from, &data, mode, actor_id).await
    }

    async fn ingest_with_current_actor(
        &mut self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: LoadMode,
    ) -> Result<IngestResult> {
        self.ensure_schema_state_valid().await?;
        let target_branch =
            Self::normalize_branch_name(branch)?.unwrap_or_else(|| "main".to_string());
        let base_branch = Self::normalize_branch_name(from.unwrap_or("main"))?
            .unwrap_or_else(|| "main".to_string());
        let branch_created = !self
            .branch_list()
            .await?
            .iter()
            .any(|name| name == &target_branch);
        if branch_created {
            self.branch_create_from(crate::db::ReadTarget::branch(&base_branch), &target_branch)
                .await?;
        }

        let result = self.load(&target_branch, data, mode).await?;
        Ok(IngestResult {
            branch: target_branch,
            base_branch,
            branch_created,
            mode,
            tables: result.to_ingest_tables(),
        })
    }

    pub async fn load(&mut self, branch: &str, data: &str, mode: LoadMode) -> Result<LoadResult> {
        self.ensure_schema_state_valid().await?;
        let requested = Self::normalize_branch_name(branch)?.unwrap_or_else(|| "main".to_string());
        if crate::db::is_internal_run_branch(&requested) {
            return self
                .load_direct_on_branch(Some(requested.as_str()), data, mode)
                .await;
        }

        let target_head_before = self.latest_branch_snapshot_id(&requested).await?;
        let op = format!("load_jsonl:branch={}:mode={}", requested, mode.as_str());
        let run = self.begin_run(&requested, Some(op.as_str())).await?;
        let staged_result = match self
            .load_direct_on_branch(Some(run.run_branch.as_str()), data, mode)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                let _ = self.fail_run(&run.run_id).await;
                return Err(err);
            }
        };

        let target_head_now = self.latest_branch_snapshot_id(&requested).await?;
        if target_head_now.as_str() != target_head_before.as_str() {
            let _ = self.fail_run(&run.run_id).await;
            return Err(OmniError::manifest_conflict(format!(
                "target branch '{}' advanced during transactional load; retry",
                requested
            )));
        }

        if let Err(err) = self.publish_run(&run.run_id).await {
            let _ = self.fail_run(&run.run_id).await;
            return Err(err);
        }

        // For S3 repos: indexes were deferred during the load/publish commit
        // (to avoid Lance's read-back-from-S3 which fails on RustFS). Build
        // them now via local staging. Index failure after publish is non-fatal:
        // data is committed, indexes will be built on next ensure_indices call.
        if crate::storage::storage_kind_for_uri(self.uri())
            == crate::storage::StorageKind::S3
        {
            if let Err(err) = self.ensure_indices_on(&requested).await {
                tracing::warn!(
                    "post-publish index build failed (data is committed, indexes pending): {}",
                    err
                );
            }
        }

        Ok(staged_result)
    }

    pub async fn load_file(
        &mut self,
        branch: &str,
        path: &str,
        mode: LoadMode,
    ) -> Result<LoadResult> {
        let data = std::fs::read_to_string(path).map_err(|e| OmniError::Io(e))?;
        self.load(branch, &data, mode).await
    }

    async fn load_direct_on_branch(
        &mut self,
        branch: Option<&str>,
        data: &str,
        mode: LoadMode,
    ) -> Result<LoadResult> {
        let reader = BufReader::new(Cursor::new(data.as_bytes()));
        load_jsonl_reader(self, branch, reader, mode).await
    }
}

impl LoadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LoadMode::Overwrite => "overwrite",
            LoadMode::Append => "append",
            LoadMode::Merge => "merge",
        }
    }
}

impl LoadResult {
    pub fn to_ingest_tables(&self) -> Vec<IngestTableResult> {
        let mut tables = self
            .nodes_loaded
            .iter()
            .map(|(type_name, rows_loaded)| IngestTableResult {
                table_key: format!("node:{type_name}"),
                rows_loaded: *rows_loaded,
            })
            .chain(
                self.edges_loaded
                    .iter()
                    .map(|(edge_name, rows_loaded)| IngestTableResult {
                        table_key: format!("edge:{edge_name}"),
                        rows_loaded: *rows_loaded,
                    }),
            )
            .collect::<Vec<_>>();
        tables.sort_by(|a, b| a.table_key.cmp(&b.table_key));
        tables
    }
}

async fn load_jsonl_reader<R: BufRead>(
    db: &mut Omnigraph,
    branch: Option<&str>,
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
            OmniError::manifest(format!("invalid JSON on line {}: {}", line_num + 1, e))
        })?;

        if let Some(type_name) = value.get("type").and_then(|v| v.as_str()) {
            if !catalog.node_types.contains_key(type_name) {
                return Err(OmniError::manifest(format!(
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
                return Err(OmniError::manifest(format!(
                    "line {}: unknown edge type '{}'",
                    line_num + 1,
                    edge_name
                )));
            }
            let from = value
                .get("from")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(format!("line {}: edge missing 'from'", line_num + 1))
                })?
                .to_string();
            let to = value
                .get("to")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(format!("line {}: edge missing 'to'", line_num + 1))
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
            return Err(OmniError::manifest(format!(
                "line {}: expected 'type' or 'edge' field",
                line_num + 1
            )));
        }
    }

    // Phase 2: Build per-type RecordBatches and write to Lance

    let mut updates = Vec::new();
    let mut result = LoadResult::default();
    let snapshot = db.snapshot_for_branch(branch).await?;

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
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;

        let (state, table_branch) =
            write_batch_to_dataset(db, branch, &table_key, batch, mode).await?;

        updates.push(crate::db::SubTableUpdate {
            table_key,
            table_version: state.version,
            table_branch,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
        });
        result.nodes_loaded.insert(type_name.clone(), loaded_count);
    }

    // Phase 2b: Validate edge referential integrity — every src/dst must
    // reference an existing node ID in the appropriate type.
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let from_ids = collect_node_ids(
            db,
            branch,
            &edge_type.from_type,
            &node_rows,
            &catalog,
            &updates,
        )
        .await?;
        let to_ids = collect_node_ids(
            db,
            branch,
            &edge_type.to_type,
            &node_rows,
            &catalog,
            &updates,
        )
        .await?;

        for (i, (src, dst, _)) in rows.iter().enumerate() {
            if !from_ids.contains(src.as_str()) {
                return Err(OmniError::manifest(format!(
                    "edge {} row {}: src '{}' not found in {}",
                    edge_name,
                    i + 1,
                    src,
                    edge_type.from_type
                )));
            }
            if !to_ids.contains(dst.as_str()) {
                return Err(OmniError::manifest(format!(
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
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;

        let (state, table_branch) =
            write_batch_to_dataset(db, branch, &table_key, batch, mode).await?;

        updates.push(crate::db::SubTableUpdate {
            table_key,
            table_version: state.version,
            table_branch,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
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
                branch,
                edge_name,
                update.table_version,
                update.table_branch.as_deref(),
            )
            .await?;
        }
    }

    // Phase 4: Atomic manifest commit
    db.commit_updates_on_branch(branch, &updates).await?;

    Ok(result)
}

fn build_node_batch(node_type: &NodeType, rows: &[JsonValue]) -> Result<RecordBatch> {
    let schema = node_type.arrow_schema.clone();

    // Build id column: explicit id, @key value, or generated ULID.
    let ids: Vec<String> = rows
        .iter()
        .map(|row| {
            let explicit_id = row.get("id").and_then(|v| v.as_str()).map(str::to_string);
            if let Some(key_prop) = node_type.key_property() {
                let key_value = row
                    .get(key_prop)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "node {} missing @key property '{}'",
                            node_type.name, key_prop
                        ))
                    })?;
                if let Some(explicit_id) = explicit_id {
                    if explicit_id != key_value {
                        return Err(OmniError::manifest(format!(
                            "node {} has explicit id '{}' that does not match @key property '{}' value '{}'",
                            node_type.name, explicit_id, key_prop, key_value
                        )));
                    }
                }
                Ok(key_value)
            } else if let Some(explicit_id) = explicit_id {
                Ok(explicit_id)
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

    let ids: Vec<String> = rows
        .iter()
        .map(|(_, _, data)| {
            data.get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(generate_id)
        })
        .collect();
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
            .map_err(|e| OmniError::manifest(format!("invalid base64 blob data: {}", e)))?;
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
                return Err(OmniError::manifest(format!(
                    "non-nullable blob property '{}' has null values",
                    name
                )));
            }
            _ => {
                return Err(OmniError::manifest(format!(
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
    let array: ArrayRef = match data_type {
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(name)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()).map(|v| v as i32))
                .collect();
            Arc::new(Int32Array::from(values))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()))
                .collect();
            Arc::new(Int64Array::from(values))
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()).map(|v| v as u32))
                .collect();
            Arc::new(UInt32Array::from(values))
        }
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()))
                .collect();
            Arc::new(UInt64Array::from(values))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()).map(|v| v as f32))
                .collect();
            Arc::new(Float32Array::from(values))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()))
                .collect();
            Arc::new(Float64Array::from(values))
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_bool()))
                .collect();
            Arc::new(BooleanArray::from(values))
        }
        DataType::Date32 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(parse_date32_json_value(
                    row.get(name).unwrap_or(&JsonValue::Null),
                )?);
            }
            Arc::new(Date32Array::from(values))
        }
        DataType::Date64 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(parse_date64_json_value(
                    row.get(name).unwrap_or(&JsonValue::Null),
                )?);
            }
            Arc::new(Date64Array::from(values))
        }
        DataType::List(field) => {
            let mut builder = ListBuilder::with_capacity(
                make_list_value_builder(field.data_type(), rows.len())?,
                rows.len(),
            )
            .with_field(field.clone());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                if value.is_null() {
                    builder.append(false);
                    continue;
                }
                let items = value.as_array().ok_or_else(|| {
                    OmniError::manifest(format!(
                        "list property '{}' expects a JSON array, got {}",
                        name, value
                    ))
                })?;
                for item in items {
                    append_json_list_item(builder.values(), field.data_type(), item)?;
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        DataType::FixedSizeList(child_field, dim) => {
            // Vector type: parse JSON array of floats into FixedSizeList<Float32>
            let dim = *dim;
            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(rows.len() * dim as usize),
                dim,
                rows.len(),
            )
            .with_field(child_field.clone());
            for row in rows {
                if let Some(arr) = row.get(name).and_then(|v| v.as_array()) {
                    if arr.len() != dim as usize {
                        return Err(OmniError::manifest(format!(
                            "vector property '{}' expects {} dimensions, got {}",
                            name,
                            dim,
                            arr.len()
                        )));
                    }
                    for val in arr {
                        builder
                            .values()
                            .append_value(val.as_f64().unwrap_or(0.0) as f32);
                    }
                    builder.append(true);
                } else if nullable {
                    for _ in 0..dim as usize {
                        builder.values().append_null();
                    }
                    builder.append(false);
                } else {
                    return Err(OmniError::manifest(format!(
                        "non-nullable vector property '{}' has null values",
                        name
                    )));
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Unsupported type: fill with nulls
            let values: Vec<Option<&str>> = vec![None; rows.len()];
            Arc::new(StringArray::from(values))
        }
    };

    if !nullable && array.null_count() > 0 {
        return Err(OmniError::manifest(format!(
            "non-nullable property '{}' has null or invalid values",
            name
        )));
    }

    Ok(array)
}

fn make_list_value_builder(data_type: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match data_type {
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 8)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Date64 => Box::new(Date64Builder::with_capacity(capacity)),
        other => {
            return Err(OmniError::manifest(format!(
                "unsupported list element data type {:?}",
                other
            )));
        }
    })
}

fn append_json_list_item(
    builder: &mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
    value: &JsonValue,
) -> Result<()> {
    match data_type {
        DataType::Utf8 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| OmniError::manifest("list Utf8 builder downcast failed"))?;
            if let Some(value) = value.as_str() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Boolean => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| OmniError::manifest("list Boolean builder downcast failed"))?;
            if let Some(value) = value.as_bool() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Int32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| OmniError::manifest("list Int32 builder downcast failed"))?;
            if let Some(value) = value.as_i64() {
                let value = i32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds Int32 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Int64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| OmniError::manifest("list Int64 builder downcast failed"))?;
            if let Some(value) = value.as_i64() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::UInt32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .ok_or_else(|| OmniError::manifest("list UInt32 builder downcast failed"))?;
            if let Some(value) = value.as_u64() {
                let value = u32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds UInt32 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::UInt64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .ok_or_else(|| OmniError::manifest("list UInt64 builder downcast failed"))?;
            if let Some(value) = value.as_u64() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Float32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| OmniError::manifest("list Float32 builder downcast failed"))?;
            if let Some(value) = value.as_f64() {
                builder.append_value(value as f32);
            } else {
                builder.append_null();
            }
        }
        DataType::Float64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| OmniError::manifest("list Float64 builder downcast failed"))?;
            if let Some(value) = value.as_f64() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Date32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| OmniError::manifest("list Date32 builder downcast failed"))?;
            if let Some(value) = parse_date32_json_value(value)? {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Date64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Date64Builder>()
                .ok_or_else(|| OmniError::manifest("list Date64 builder downcast failed"))?;
            if let Some(value) = parse_date64_json_value(value)? {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        other => {
            return Err(OmniError::manifest(format!(
                "unsupported list element data type {:?}",
                other
            )));
        }
    }

    Ok(())
}

fn parse_date32_json_value(value: &JsonValue) -> Result<Option<i32>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(days) = value.as_i64() {
        let days = i32::try_from(days)
            .map_err(|_| OmniError::manifest(format!("Date value out of range: {}", days)))?;
        return Ok(Some(days));
    }
    if let Some(days) = value.as_u64() {
        let days = i32::try_from(days)
            .map_err(|_| OmniError::manifest(format!("Date value out of range: {}", days)))?;
        return Ok(Some(days));
    }
    if let Some(value) = value.as_str() {
        return Ok(Some(parse_date32_literal(value)?));
    }
    Ok(None)
}

fn parse_date64_json_value(value: &JsonValue) -> Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(ms) = value.as_i64() {
        return Ok(Some(ms));
    }
    if let Some(ms) = value.as_u64() {
        let ms = i64::try_from(ms)
            .map_err(|_| OmniError::manifest(format!("DateTime value out of range: {}", ms)))?;
        return Ok(Some(ms));
    }
    if let Some(value) = value.as_str() {
        return Ok(Some(parse_date64_literal(value)?));
    }
    Ok(None)
}

/// Write a batch to a Lance dataset, returning (new_version, total_row_count).
async fn write_batch_to_dataset(
    db: &Omnigraph,
    branch: Option<&str>,
    table_key: &str,
    batch: RecordBatch,
    mode: LoadMode,
) -> Result<(crate::table_store::TableState, Option<String>)> {
    let (mut ds, full_path, table_branch) =
        db.open_for_mutation_on_branch(branch, table_key).await?;
    let table_store = db.table_store();

    match mode {
        LoadMode::Overwrite => {
            let state = table_store
                .overwrite_batch(&full_path, &mut ds, batch)
                .await?;
            Ok((state, table_branch))
        }
        LoadMode::Append => {
            let state = table_store.append_batch(&full_path, &mut ds, batch).await?;
            Ok((state, table_branch))
        }
        LoadMode::Merge => {
            let state = table_store
                .merge_insert_batch(
                    &full_path,
                    ds,
                    batch,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?;
            Ok((state, table_branch))
        }
    }
}

fn generate_id() -> String {
    ulid::Ulid::new().to_string()
}

pub(crate) fn parse_date32_literal(value: &str) -> Result<i32> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date32)
        .map_err(|e| OmniError::manifest(format!("invalid Date literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| OmniError::manifest("Date32 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid Date literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
}

pub(crate) fn parse_date64_literal(value: &str) -> Result<i64> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date64)
        .map_err(|e| OmniError::manifest(format!("invalid DateTime literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| OmniError::manifest("Date64 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid DateTime literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
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
                if val.is_nan() {
                    return Err(OmniError::manifest(format!(
                        "@range violation on {}.{}: value is NaN",
                        node_type.name, rc.property
                    )));
                }
                if let Some(ref min) = rc.min {
                    let min_f = literal_value_to_f64(min);
                    if val < min_f {
                        return Err(OmniError::manifest(format!(
                            "@range violation on {}.{}: value {} < min {}",
                            node_type.name, rc.property, val, min_f
                        )));
                    }
                }
                if let Some(ref max) = rc.max {
                    let max_f = literal_value_to_f64(max);
                    if val > max_f {
                        return Err(OmniError::manifest(format!(
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
            OmniError::manifest(format!(
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
                    return Err(OmniError::manifest(format!(
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
    branch: Option<&str>,
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
    let snapshot = db.snapshot_for_branch(branch).await?;
    let table_key = format!("edge:{}", edge_name);
    let entry = snapshot
        .entry(&table_key)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
    let ds = db
        .open_dataset_at_state(
            &entry.table_path,
            written_branch.or(entry.table_branch.as_deref()),
            written_version,
        )
        .await?;

    // Scan src column, count per source
    let batches = db
        .table_store()
        .scan(&ds, Some(&["src"]), None, None)
        .await?;

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
                return Err(OmniError::manifest(format!(
                    "@card violation on edge {}: source '{}' has {} edges (max {})",
                    edge_name, src, count, max
                )));
            }
        }
        if *count < card.min {
            return Err(OmniError::manifest(format!(
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
    branch: Option<&str>,
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
    let snapshot = db.snapshot_for_branch(branch).await?;
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

    let batches = db
        .table_store()
        .scan(&ds, Some(&["id"]), None, None)
        .await?;

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
    use std::collections::HashMap;

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

    #[tokio::test]
    async fn test_ingest_creates_branch_and_reports_tables() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = db
            .ingest("feature", Some("main"), TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert_eq!(result.branch, "feature");
        assert_eq!(result.base_branch, "main");
        assert!(result.branch_created);
        assert_eq!(result.mode, LoadMode::Overwrite);
        assert_eq!(
            result.tables,
            vec![
                IngestTableResult {
                    table_key: "edge:Knows".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "edge:WorksAt".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "node:Company".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "node:Person".to_string(),
                    rows_loaded: 2
                },
            ]
        );
        assert!(
            db.branch_list()
                .await
                .unwrap()
                .contains(&"feature".to_string())
        );
    }

    #[tokio::test]
    async fn test_ingest_existing_branch_ignores_from_and_merges_data() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
        db.branch_create_from(crate::db::ReadTarget::branch("main"), "feature")
            .await
            .unwrap();

        let result = db
            .ingest(
                "feature",
                Some("missing-base"),
                r#"{"type":"Person","data":{"name":"Bob","age":26}}
{"type":"Person","data":{"name":"Eve","age":31}}"#,
                LoadMode::Merge,
            )
            .await
            .unwrap();

        assert_eq!(result.branch, "feature");
        assert_eq!(result.base_branch, "missing-base");
        assert!(!result.branch_created);
        assert_eq!(result.mode, LoadMode::Merge);
        assert_eq!(
            result.tables,
            vec![IngestTableResult {
                table_key: "node:Person".to_string(),
                rows_loaded: 2
            }]
        );

        let snap = db
            .snapshot_of(crate::db::ReadTarget::branch("feature"))
            .await
            .unwrap();
        let person_ds = snap.open("node:Person").await.unwrap();
        assert_eq!(person_ds.count_rows(None).await.unwrap(), 3);

        let batches: Vec<RecordBatch> = person_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ages_by_id = HashMap::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ages = batch
                .column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for idx in 0..ids.len() {
                ages_by_id.insert(ids.value(idx).to_string(), ages.value(idx));
            }
        }

        assert_eq!(ages_by_id.get("Bob"), Some(&26));
        assert_eq!(ages_by_id.get("Eve"), Some(&31));
        assert_eq!(ages_by_id.get("Alice"), Some(&30));
    }

    #[tokio::test]
    async fn test_ingest_as_stamps_actor_on_branch_head_commit() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        db.ingest_as(
            "feature",
            Some("main"),
            TEST_DATA,
            LoadMode::Overwrite,
            Some("act-andrew"),
        )
        .await
        .unwrap();

        let head = db
            .list_commits(Some("feature"))
            .await
            .unwrap()
            .into_iter()
            .last()
            .unwrap();
        assert_eq!(head.actor_id.as_deref(), Some("act-andrew"));
    }

    #[test]
    fn test_range_constraint_rejects_nan() {
        use arrow_array::{Float64Array, RecordBatch, StringArray};
        use omnigraph_compiler::catalog::{LiteralValue, NodeType, RangeConstraint};
        use std::sync::Arc;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("score", arrow_schema::DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();

        let node_type = NodeType {
            name: "Test".to_string(),
            implements: vec![],
            properties: Default::default(),
            key: None,
            unique_constraints: vec![],
            indices: vec![],
            range_constraints: vec![RangeConstraint {
                property: "score".to_string(),
                min: Some(LiteralValue::Float(0.0)),
                max: Some(LiteralValue::Float(1.0)),
            }],
            check_constraints: vec![],
            embed_sources: Default::default(),
            blob_properties: Default::default(),
            arrow_schema: schema,
        };

        let result = validate_value_constraints(&batch, &node_type);
        assert!(result.is_err(), "expected NaN to be rejected");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("NaN"), "error should mention NaN: {}", err);
    }
}
