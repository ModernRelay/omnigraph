use super::*;
use futures::TryStreamExt;
use std::future::Future;

/// Initial row estimate used by Lance's byte-targeted export scanner.
pub(super) const EXPORT_SCAN_TARGET_ROWS: usize = 8_192;
/// Approximate decoded Arrow byte target for one export scanner batch.
pub(super) const EXPORT_SCAN_TARGET_BYTES: u64 = 32 * 1024 * 1024;
/// Maximum bytes passed to one asynchronous export transport emission.
#[doc(hidden)]
pub const EXPORT_CHUNK_MAX_BYTES: usize = 64 * 1024;

/// One immutable graph cut captured for served export.
///
/// The cut retains immutable Lance version pins and the sole exclusive root
/// export gate while bytes are produced. Ordinary writers may advance HEAD in
/// parallel; cooperative cleanup, schema, branch, and root controls cannot
/// remove or reuse the cut's exact coordinates until it is dropped.
/// Private fields and the absence of `Clone`/serde/default constructors keep
/// the cut non-forgeable.
#[doc(hidden)]
pub struct ExportCut {
    db: Arc<Omnigraph>,
    snapshot: Snapshot,
    catalog: Arc<Catalog>,
    selected_tables: Vec<String>,
    _slot: crate::db::write_queue::ExportCutPermit,
}

impl ExportCut {
    async fn emit_chunks<Emit, EmitFuture>(&self, emit: &mut Emit) -> Result<()>
    where
        Emit: FnMut(Vec<u8>) -> EmitFuture,
        EmitFuture: Future<Output = Result<()>>,
    {
        export_selected_tables(
            self.db.as_ref(),
            &self.snapshot,
            self.catalog.as_ref(),
            &self.selected_tables,
            emit,
        )
        .await
    }

    /// Emit this cut as independently owned bounded chunks.
    ///
    /// The returned cut retains its root slot and exact-version pins. A served
    /// transport keeps it in a terminal frame until every preceding data frame
    /// has drained; a disconnected receiver drops either the in-flight future
    /// or that terminal frame and therefore releases the cut promptly.
    #[doc(hidden)]
    pub async fn write_chunks<Emit, EmitFuture>(self, mut emit: Emit) -> (Self, Result<()>)
    where
        Emit: FnMut(Vec<u8>) -> EmitFuture,
        EmitFuture: Future<Output = Result<()>>,
    {
        let result = self.emit_chunks(&mut emit).await;
        (self, result)
    }

    /// Consume this cut and write its exact pinned contents as JSONL.
    ///
    /// A storage or writer failure after output starts is returned unchanged;
    /// dropping this future or any error path releases the root export gate.
    pub async fn write_to<W: Write>(self, writer: &mut W) -> Result<()> {
        let (_cut, result) = self
            .write_chunks(|chunk: Vec<u8>| {
                std::future::ready(writer.write_all(&chunk).map_err(OmniError::from))
            })
            .await;
        result
    }

    /// Consume this cut and return its exact pinned contents as one JSONL
    /// string. Intended for tests and non-transport callers; served export uses
    /// bounded asynchronous chunks instead of retaining the complete artifact.
    pub async fn into_jsonl(self) -> Result<String> {
        let mut out = Vec::new();
        self.write_to(&mut out).await?;
        String::from_utf8(out)
            .map_err(|err| OmniError::manifest(format!("export produced invalid UTF-8: {err}")))
    }
}

impl Omnigraph {
    /// Capture one immutable served-export cut.
    ///
    /// The exclusive root gate is non-waiting and this is the sole cut-capture
    /// surface used by served transport.
    #[doc(hidden)]
    pub async fn capture_served_export_cut(
        self: &Arc<Self>,
        branch: &str,
        type_names: &[String],
        table_keys: &[String],
    ) -> Result<ExportCut> {
        let slot = self.write_queue().try_acquire_export_cut().ok_or_else(|| {
            OmniError::ResourceLimitExceeded {
                resource: "stream_export_slots".to_string(),
                limit: 1,
                actual: 2,
            }
        })?;

        self.heal_pending_recovery_sidecars_outcome().await?;
        let (resolved, catalog) = self.capture_read_view(ReadTarget::branch(branch)).await?;
        let snapshot = resolved.snapshot;
        let selected_tables = export_table_keys(&snapshot, type_names, table_keys)?;

        Ok(ExportCut {
            db: Arc::clone(self),
            snapshot,
            catalog,
            selected_tables,
            _slot: slot,
        })
    }
}

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
    let snap = db
        .coordinator
        .read()
        .await
        .snapshot_at_version(version)
        .await?;
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
    // Reserve before the first manifest read. Cleanup, schema apply, branch
    // replacement, and root deletion must not remove or reuse the selected
    // coordinates while bytes are still being read.
    let _export_cut = db.write_queue().try_acquire_export_cut().ok_or_else(|| {
        OmniError::ResourceLimitExceeded {
            resource: "stream_export_slots".to_string(),
            limit: 1,
            actual: 2,
        }
    })?;
    let (resolved, catalog) = db.capture_read_view(ReadTarget::branch(branch)).await?;
    let selected_tables = export_table_keys(&resolved.snapshot, type_names, table_keys)?;
    let mut emit =
        |chunk: Vec<u8>| std::future::ready(writer.write_all(&chunk).map_err(OmniError::from));
    export_selected_tables(
        db,
        &resolved.snapshot,
        catalog.as_ref(),
        &selected_tables,
        &mut emit,
    )
    .await
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
        .storage()
        .open_snapshot_at_table(snapshot, table_key)
        .await?;
    let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
    let batches = db
        .storage()
        .scan(&ds, None, Some(&filter_sql), None)
        .await?;
    let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
        return Ok(None);
    };
    Ok(Some(record_batch_row_to_json(batch, 0)?))
}

async fn export_selected_tables<Emit, EmitFuture>(
    db: &Omnigraph,
    snapshot: &Snapshot,
    catalog: &Catalog,
    selected_tables: &[String],
    emit: &mut Emit,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    for table_key in selected_tables {
        export_table(db, snapshot, catalog, table_key, emit).await?;
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

async fn export_table<Emit, EmitFuture>(
    db: &Omnigraph,
    snapshot: &Snapshot,
    catalog: &Catalog,
    table_key: &str,
    emit: &mut Emit,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    let ds = db
        .storage()
        .open_snapshot_at_table(snapshot, table_key)
        .await?;
    let ordering = Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]);
    let blob_properties = blob_properties_for_table_key(catalog, table_key)?;

    if blob_properties.is_empty() {
        let mut batches = db
            .storage()
            .scan_stream_bounded(
                &ds,
                None,
                None,
                ordering,
                false,
                EXPORT_SCAN_TARGET_ROWS,
                EXPORT_SCAN_TARGET_BYTES,
            )
            .await?;
        while let Some(batch) = batches
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            emit_export_rows_from_batch(catalog, table_key, &batch, None, emit).await?;
        }
        return Ok(());
    }

    // Lance's byte target is approximate and overrides its row estimate, so a
    // scanner batch is not a hard memory bound. Slice each returned descriptor
    // batch explicitly and materialize only one logical row's complete Blob
    // property set before observing transport backpressure. One Blob value and
    // one row's encoded JSON remain indivisible scratch allocations.
    let mut batches = db
        .storage()
        .scan_stream_bounded(
            &ds,
            None,
            None,
            ordering,
            true,
            EXPORT_SCAN_TARGET_ROWS,
            EXPORT_SCAN_TARGET_BYTES,
        )
        .await?;
    while let Some(batch) = batches
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        for row_index in 0..batch.num_rows() {
            let row = batch.slice(row_index, 1);
            let row_id = row
                .column_by_name("_rowid")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::Lance(format!(
                        "expected _rowid column when exporting '{}'",
                        table_key
                    ))
                })?
                .value(0);
            // Blob materialization reaches through to the inner Lance
            // `Dataset` because `take_blobs` is a Lance-only API not lifted
            // onto the `TableStorage` trait surface (the trait covers
            // staged-write and snapshot-scan primitives; blob descriptor
            // materialization sits outside that surface).
            let blob_values =
                export_blob_values(ds.dataset(), &row, &[row_id], blob_properties).await?;
            emit_export_rows_from_batch(catalog, table_key, &row, Some(&blob_values), emit).await?;
        }
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

/// Convert one descriptor-scanned row into the same logical value shape used
/// by export, materializing at most that row's Blob values.
pub(crate) async fn logical_row_image(
    source_ds: &Dataset,
    catalog: &Catalog,
    table_key: &str,
    batch: &RecordBatch,
    row: usize,
) -> Result<serde_json::Value> {
    let row_batch = batch.slice(row, 1);
    let blob_properties = blob_properties_for_table_key(catalog, table_key)?;
    let blob_values = if blob_properties.is_empty() {
        None
    } else {
        let row_id = row_batch
            .column_by_name("_rowid")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| OmniError::Lance("change row is missing _rowid".to_string()))?
            .value(0);
        Some(export_blob_values(source_ds, &row_batch, &[row_id], blob_properties).await?)
    };

    let mut image = serde_json::Map::new();
    image.insert(
        "id".to_string(),
        json_value_from_named_column(&row_batch, "id", 0)?,
    );
    if table_key.starts_with("edge:") {
        image.insert(
            "src".to_string(),
            json_value_from_named_column(&row_batch, "src", 0)?,
        );
        image.insert(
            "dst".to_string(),
            json_value_from_named_column(&row_batch, "dst", 0)?,
        );
    }

    let schema = schema_for_table_key(catalog, table_key)?;
    let first_property = if table_key.starts_with("edge:") { 3 } else { 1 };
    for field in schema.fields().iter().skip(first_property) {
        image.insert(
            field.name().clone(),
            export_value_for_field(
                &row_batch,
                field.name(),
                0,
                blob_values
                    .as_ref()
                    .and_then(|values| values.get(field.name())),
            )?,
        );
    }
    Ok(serde_json::Value::Object(image))
}

async fn emit_export_rows_from_batch<Emit, EmitFuture>(
    catalog: &Catalog,
    table_key: &str,
    batch: &RecordBatch,
    blob_values: Option<&HashMap<String, Vec<Option<String>>>>,
    emit: &mut Emit,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    if let Some(type_name) = table_key.strip_prefix("node:") {
        let node_type = catalog
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
            emit_export_jsonl_row(
                emit,
                table_key,
                &serde_json::json!({
                    "type": type_name,
                    "data": serde_json::Value::Object(data),
                }),
            )
            .await?;
        }
        return Ok(());
    }

    if let Some(edge_name) = table_key.strip_prefix("edge:") {
        let edge_type = catalog
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
            emit_export_jsonl_row(
                emit,
                table_key,
                &serde_json::json!({
                    "edge": edge_name,
                    "from": from,
                    "to": to,
                    "data": serde_json::Value::Object(data),
                }),
            )
            .await?;
        }
        return Ok(());
    }

    Err(OmniError::manifest(format!(
        "invalid export table key '{}'",
        table_key
    )))
}

async fn emit_export_jsonl_row<Emit, EmitFuture>(
    emit: &mut Emit,
    table_key: &str,
    row: &serde_json::Value,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    let mut encoded = serde_json::to_vec(row).map_err(|err| {
        OmniError::manifest(format!(
            "failed to serialize export row for '{}': {}",
            table_key, err
        ))
    })?;
    encoded.push(b'\n');
    for chunk in encoded.chunks(EXPORT_CHUNK_MAX_BYTES) {
        emit(chunk.to_vec()).await?;
    }
    Ok(())
}

async fn export_blob_column_values(
    source_ds: &Dataset,
    column_name: &str,
    descriptions: &StructArray,
    row_ids: &[u64],
) -> Result<Vec<Option<String>>> {
    let decoder = crate::blob::BlobDescriptorDecoder::try_new(descriptions)?;
    let mut managed_row_ids = Vec::new();
    let mut managed_positions = Vec::new();
    let mut values = vec![None; row_ids.len()];

    for (row, row_id) in row_ids.iter().enumerate() {
        match decoder.classify(row)? {
            crate::blob::BlobDescriptor::Null => {}
            crate::blob::BlobDescriptor::Managed { .. } => {
                managed_row_ids.push(*row_id);
                managed_positions.push(row);
            }
            crate::blob::BlobDescriptor::External { uri, .. } => {
                // Export is descriptor-preserving. It must not open or probe a
                // caller-owned object merely to reproduce the stored URI.
                values[row] = Some(uri);
            }
        }
    }

    if managed_row_ids.is_empty() {
        return Ok(values);
    }

    let mut perm: Vec<usize> = (0..managed_row_ids.len()).collect();
    perm.sort_by_key(|&i| managed_row_ids[i]);
    let sorted_ids: Vec<u64> = perm.iter().map(|&i| managed_row_ids[i]).collect();

    let sorted_blobs = Arc::new(source_ds.clone())
        .take_blobs(&sorted_ids, column_name)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    if sorted_blobs.len() != managed_positions.len() {
        return Err(OmniError::Lance(format!(
            "blob export for '{}' lost alignment with selected rows",
            column_name
        )));
    }

    let mut inverse_perm = vec![0usize; perm.len()];
    for (sorted_pos, &orig_pos) in perm.iter().enumerate() {
        inverse_perm[orig_pos] = sorted_pos;
    }

    for (idx, position) in managed_positions.into_iter().enumerate() {
        let blob = sorted_blobs[inverse_perm[idx]].as_ref().ok_or_else(|| {
            OmniError::Lance(format!(
                "blob export for '{}' returned a null accessor for a managed description",
                column_name
            ))
        })?;
        if blob.uri().is_some() {
            return Err(OmniError::Lance(format!(
                "blob export for '{}' resolved a managed description as external",
                column_name
            )));
        }
        let bytes = blob
            .read()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let value = format!(
            "base64:{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
        );
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
