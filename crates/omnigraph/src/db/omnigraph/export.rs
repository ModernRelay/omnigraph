use super::*;

pub(super) async fn entity_at_target(
    db: &Omnigraph,
    target: impl Into<ReadTarget>,
    table_key: &str,
    id: &str,
) -> Result<Option<serde_json::Value>> {
    let resolved = db.resolved_target(target).await?;
    entity_from_snapshot(db, &resolved.snapshot, table_key, id).await
}

pub(super) async fn entity_at(
    db: &Omnigraph,
    table_key: &str,
    id: &str,
    version: u64,
) -> Result<Option<serde_json::Value>> {
    let snap = db.coordinator.snapshot_at_version(version).await?;
    entity_from_snapshot(db, &snap, table_key, id).await
}

pub(super) async fn export_jsonl(
    db: &Omnigraph,
    branch: &str,
    type_names: &[String],
    table_keys: &[String],
) -> Result<String> {
    let mut out = Vec::new();
    export_jsonl_to_writer(db, branch, type_names, table_keys, &mut out).await?;
    String::from_utf8(out)
        .map_err(|err| OmniError::manifest(format!("export produced invalid UTF-8: {}", err)))
}

pub(super) async fn export_jsonl_to_writer<W: Write>(
    db: &Omnigraph,
    branch: &str,
    type_names: &[String],
    table_keys: &[String],
    writer: &mut W,
) -> Result<()> {
    db.ensure_schema_state_valid().await?;
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await?;
    export_snapshot_jsonl_to_writer(db, &snapshot, type_names, table_keys, writer).await
}

async fn entity_from_snapshot(
    db: &Omnigraph,
    snapshot: &Snapshot,
    table_key: &str,
    id: &str,
) -> Result<Option<serde_json::Value>> {
    if snapshot.entry(table_key).is_none() {
        return Ok(None);
    }

    let ds = db
        .table_store
        .open_snapshot_table(snapshot, table_key)
        .await?;
    let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
    let batches = db
        .table_store
        .scan(&ds, None, Some(&filter_sql), None)
        .await?;
    let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
        return Ok(None);
    };
    Ok(Some(record_batch_row_to_json(batch, 0)?))
}

async fn export_snapshot_jsonl_to_writer<W: Write>(
    db: &Omnigraph,
    snapshot: &Snapshot,
    type_names: &[String],
    table_keys: &[String],
    writer: &mut W,
) -> Result<()> {
    let selected_tables = export_table_keys(snapshot, type_names, table_keys)?;
    for table_key in selected_tables {
        export_table_to_writer(db, snapshot, &table_key, writer).await?;
    }
    Ok(())
}

fn export_table_keys(
    snapshot: &Snapshot,
    type_names: &[String],
    table_keys: &[String],
) -> Result<Vec<String>> {
    let available = snapshot
        .entries()
        .map(|entry| entry.table_key.clone())
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();

    for table_key in table_keys {
        if !available.contains(table_key) {
            return Err(OmniError::manifest(format!(
                "unknown export table '{}'",
                table_key
            )));
        }
        selected.insert(table_key.clone());
    }

    for type_name in type_names {
        let mut matched = false;
        let node_key = format!("node:{}", type_name);
        if available.contains(&node_key) {
            selected.insert(node_key);
            matched = true;
        }
        let edge_key = format!("edge:{}", type_name);
        if available.contains(&edge_key) {
            selected.insert(edge_key);
            matched = true;
        }
        if !matched {
            return Err(OmniError::manifest(format!(
                "unknown export type '{}'",
                type_name
            )));
        }
    }

    if selected.is_empty() {
        return Ok(available.into_iter().collect());
    }

    Ok(selected.into_iter().collect())
}

async fn export_table_to_writer<W: Write>(
    db: &Omnigraph,
    snapshot: &Snapshot,
    table_key: &str,
    writer: &mut W,
) -> Result<()> {
    let ds = db
        .table_store
        .open_snapshot_table(snapshot, table_key)
        .await?;
    let ordering = Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]);
    let blob_properties = blob_properties_for_table_key(db.catalog(), table_key)?;

    if blob_properties.is_empty() {
        for batch in db.table_store.scan(&ds, None, None, ordering).await? {
            write_export_rows_from_batch(db, table_key, &batch, None, writer)?;
        }
        return Ok(());
    }

    let batches = db
        .table_store
        .scan_with(&ds, None, None, ordering, true, |_| Ok(()))
        .await?;
    for batch in batches {
        let row_ids = batch
            .column_by_name("_rowid")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "expected _rowid column when exporting '{}'",
                    table_key
                ))
            })?
            .values()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let blob_values = export_blob_values(&ds, &batch, &row_ids, blob_properties).await?;
        write_export_rows_from_batch(db, table_key, &batch, Some(&blob_values), writer)?;
    }
    Ok(())
}

async fn export_blob_values(
    source_ds: &Dataset,
    batch: &RecordBatch,
    row_ids: &[u64],
    blob_properties: &std::collections::HashSet<String>,
) -> Result<HashMap<String, Vec<Option<String>>>> {
    let mut values = HashMap::with_capacity(blob_properties.len());
    for property in blob_properties {
        let descriptions = batch
            .column_by_name(property)
            .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "expected blob descriptions for export column '{}'",
                    property
                ))
            })?;
        values.insert(
            property.clone(),
            export_blob_column_values(source_ds, property, descriptions, row_ids).await?,
        );
    }
    Ok(values)
}

fn write_export_rows_from_batch<W: Write>(
    db: &Omnigraph,
    table_key: &str,
    batch: &RecordBatch,
    blob_values: Option<&HashMap<String, Vec<Option<String>>>>,
    writer: &mut W,
) -> Result<()> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        let node_type = db
            .catalog
            .node_types
            .get(type_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
        for row in 0..batch.num_rows() {
            let mut data = serde_json::Map::new();
            data.insert(
                "id".to_string(),
                json_value_from_named_column(batch, "id", row)?,
            );
            for field in node_type.arrow_schema.fields().iter().skip(1) {
                data.insert(
                    field.name().clone(),
                    export_value_for_field(
                        batch,
                        field.name(),
                        row,
                        blob_values.and_then(|values| values.get(field.name())),
                    )?,
                );
            }
            write_export_jsonl_row(
                writer,
                table_key,
                &serde_json::json!({
                    "type": type_name,
                    "data": serde_json::Value::Object(data),
                }),
            )?;
        }
        return Ok(());
    }

    if let Some(edge_name) = table_key.strip_prefix("edge:") {
        let edge_type = db
            .catalog
            .edge_types
            .get(edge_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_name)))?;
        for row in 0..batch.num_rows() {
            let from = named_string_value(batch, "src", row)?;
            let to = named_string_value(batch, "dst", row)?;
            let mut data = serde_json::Map::new();
            data.insert(
                "id".to_string(),
                json_value_from_named_column(batch, "id", row)?,
            );
            for field in edge_type.arrow_schema.fields().iter().skip(3) {
                data.insert(
                    field.name().clone(),
                    export_value_for_field(
                        batch,
                        field.name(),
                        row,
                        blob_values.and_then(|values| values.get(field.name())),
                    )?,
                );
            }
            write_export_jsonl_row(
                writer,
                table_key,
                &serde_json::json!({
                    "edge": edge_name,
                    "from": from,
                    "to": to,
                    "data": serde_json::Value::Object(data),
                }),
            )?;
        }
        return Ok(());
    }

    Err(OmniError::manifest(format!(
        "invalid export table key '{}'",
        table_key
    )))
}

fn write_export_jsonl_row<W: Write>(
    writer: &mut W,
    table_key: &str,
    row: &serde_json::Value,
) -> Result<()> {
    serde_json::to_writer(&mut *writer, row).map_err(|err| {
        OmniError::manifest(format!(
            "failed to serialize export row for '{}': {}",
            table_key, err
        ))
    })?;
    writer.write_all(b"\n")?;
    Ok(())
}

async fn export_blob_column_values(
    source_ds: &Dataset,
    column_name: &str,
    descriptions: &StructArray,
    row_ids: &[u64],
) -> Result<Vec<Option<String>>> {
    let mut non_null_row_ids = Vec::new();
    let mut non_null_positions = Vec::new();
    let mut values = vec![None; row_ids.len()];

    for (row, row_id) in row_ids.iter().enumerate() {
        if blob_description_is_null(descriptions, row)? {
            continue;
        }
        non_null_row_ids.push(*row_id);
        non_null_positions.push(row);
    }

    if non_null_row_ids.is_empty() {
        return Ok(values);
    }

    let mut perm: Vec<usize> = (0..non_null_row_ids.len()).collect();
    perm.sort_by_key(|&i| non_null_row_ids[i]);
    let sorted_ids: Vec<u64> = perm.iter().map(|&i| non_null_row_ids[i]).collect();

    let sorted_blobs = Arc::new(source_ds.clone())
        .take_blobs(&sorted_ids, column_name)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    if sorted_blobs.len() != non_null_positions.len() {
        return Err(OmniError::Lance(format!(
            "blob export for '{}' lost alignment with selected rows",
            column_name
        )));
    }

    let mut inverse_perm = vec![0usize; perm.len()];
    for (sorted_pos, &orig_pos) in perm.iter().enumerate() {
        inverse_perm[orig_pos] = sorted_pos;
    }

    for (idx, position) in non_null_positions.into_iter().enumerate() {
        let blob = &sorted_blobs[inverse_perm[idx]];
        let value = if let Some(uri) = blob.uri() {
            uri.to_string()
        } else {
            let bytes = blob
                .read()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            format!(
                "base64:{}",
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
            )
        };
        values[position] = Some(value);
    }

    Ok(values)
}

fn export_value_for_field(
    batch: &RecordBatch,
    field_name: &str,
    row: usize,
    blob_values: Option<&Vec<Option<String>>>,
) -> Result<serde_json::Value> {
    if let Some(blob_values) = blob_values {
        return Ok(blob_values
            .get(row)
            .and_then(|value| value.clone())
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null));
    }
    json_value_from_named_column(batch, field_name, row)
}

fn json_value_from_named_column(
    batch: &RecordBatch,
    field_name: &str,
    row: usize,
) -> Result<serde_json::Value> {
    let column = batch.column_by_name(field_name).ok_or_else(|| {
        OmniError::Lance(format!("missing column '{}' in export batch", field_name))
    })?;
    json_value_from_array(column.as_ref(), row)
}

fn named_string_value(batch: &RecordBatch, field_name: &str, row: usize) -> Result<String> {
    let column = batch.column_by_name(field_name).ok_or_else(|| {
        OmniError::Lance(format!("missing column '{}' in export batch", field_name))
    })?;
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::Lance(format!("expected Utf8 column '{}'", field_name)))?;
    if array.is_null(row) {
        return Err(OmniError::Lance(format!(
            "unexpected null in export column '{}'",
            field_name
        )));
    }
    Ok(array.value(row).to_string())
}
