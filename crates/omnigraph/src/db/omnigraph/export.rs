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
        let selected_tables = export_type_keys(&snapshot, type_names)?;

        Ok(ExportCut {
            db: Arc::clone(self),
            snapshot,
            catalog,
            selected_tables,
            _slot: slot,
        })
    }

    /// Capture one served baseline handshake: the pre-minted handshake plus an
    /// export cut PINNED at the captured head commit. The transport must emit
    /// the terminal handshake record only after the cut's chunk stream
    /// completed Ok, so an interrupted stream never yields a usable cursor.
    #[doc(hidden)]
    pub async fn capture_served_change_baseline_cut(
        self: &Arc<Self>,
        branch: &str,
        scope: &crate::changes::ChangeFeedScope,
    ) -> Result<(crate::changes::ChangeBaseline, ExportCut)> {
        let parts = capture_baseline_parts(self, branch, scope, true).await?;
        Ok((
            parts.handshake,
            ExportCut {
                db: Arc::clone(self),
                snapshot: parts.snapshot,
                catalog: parts.catalog,
                selected_tables: parts.selected_tables,
                _slot: parts.slot,
            },
        ))
    }
}

pub(super) async fn entity_at_target(
    db: &Omnigraph,
    target: impl Into<ReadTarget>,
    type_key: &str,
    id: &str,
) -> Result<Option<serde_json::Value>> {
    let resolved = db.resolved_target(target).await?;
    entity_from_snapshot(db, &resolved.snapshot, type_key, id).await
}

pub(super) async fn entity_at(
    db: &Omnigraph,
    type_key: &str,
    id: &str,
    graph_manifest_version: u64,
) -> Result<Option<serde_json::Value>> {
    let snap = db
        .coordinator
        .read()
        .await
        .snapshot_at_graph_manifest_version(graph_manifest_version)
        .await?;
    entity_from_snapshot(db, &snap, type_key, id).await
}

pub(super) async fn export_jsonl(
    db: &Omnigraph,
    branch: &str,
    type_names: &[String],
) -> Result<String> {
    let mut out = Vec::new();
    export_jsonl_to_writer(db, branch, type_names, &mut out).await?;
    String::from_utf8(out)
        .map_err(|err| OmniError::manifest(format!("export produced invalid UTF-8: {}", err)))
}

pub(super) async fn export_jsonl_to_writer<W: Write>(
    db: &Omnigraph,
    branch: &str,
    type_names: &[String],
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
    let selected_tables = export_type_keys(&resolved.snapshot, type_names)?;
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

/// The baseline handshake: capture one branch head coherently, stream the
/// data-only entity snapshot PINNED at that head into `writer`, and mint the
/// cursor that resumes the change feed immediately after it.
///
/// The export-cut permit is held from before head capture until the last byte,
/// so cleanup, schema apply, branch replacement, and root deletion cannot
/// remove or reuse the selected coordinates mid-handshake — this closes the
/// head-capture/export race a bare head ID plus a later export would have. A
/// failed export returns `Err`, so a usable cursor structurally cannot outlive
/// a broken snapshot.
/// Everything one baseline handshake needs, captured coherently under the
/// export-cut permit the struct retains.
struct BaselineParts {
    slot: crate::db::write_queue::ExportCutPermit,
    snapshot: Snapshot,
    catalog: Arc<Catalog>,
    selected_tables: Vec<String>,
    handshake: crate::changes::ChangeBaseline,
}

async fn capture_baseline_parts(
    db: &Omnigraph,
    branch: &str,
    scope: &crate::changes::ChangeFeedScope,
    heal: bool,
) -> Result<BaselineParts> {
    let slot = db.write_queue().try_acquire_export_cut().ok_or_else(|| {
        OmniError::ResourceLimitExceeded {
            resource: "stream_export_slots".to_string(),
            limit: 1,
            actual: 2,
        }
    })?;
    if heal {
        db.heal_pending_recovery_sidecars_outcome().await?;
    }
    let normalized_branch = Some(branch).filter(|branch| *branch != "main");
    let cut = db
        .coordinator
        .read()
        .await
        .capture_change_cut(normalized_branch)
        .await?;

    // Pin the export at the captured head commit, not the live branch tip: a
    // commit landing after the capture is outside the snapshot and arrives on
    // the first poll from the returned cursor.
    let (resolved, catalog) = db
        .capture_read_view(ReadTarget::Snapshot(super::SnapshotId::new(
            cut.head.clone(),
        )))
        .await?;
    let type_names = scope.type_names.clone().unwrap_or_default();
    let selected_tables: Vec<String> = export_type_keys(&resolved.snapshot, &type_names)?
        .into_iter()
        .filter(|table_key| {
            let kind = if table_key.starts_with("edge:") {
                crate::changes::ChangeEntityKind::Edge
            } else {
                crate::changes::ChangeEntityKind::Node
            };
            scope.wants_kind(kind)
        })
        .collect();
    let resume_cursor = crate::changes::feed::mint_cursor_after(
        &db.schema_view.load().schema_identity_domain,
        &cut,
        scope,
        &cut.head,
    )?;
    Ok(BaselineParts {
        slot,
        snapshot: resolved.snapshot,
        catalog,
        selected_tables,
        handshake: crate::changes::ChangeBaseline {
            snapshot_commit_id: cut.head,
            resume_cursor,
        },
    })
}

/// The baseline handshake: capture one branch head coherently, stream the
/// data-only entity snapshot PINNED at that head into `writer`, and mint the
/// cursor that resumes the change feed immediately after it.
///
/// The export-cut permit is held from before head capture until the last byte,
/// so cleanup, schema apply, branch replacement, and root deletion cannot
/// remove or reuse the selected coordinates mid-handshake — this closes the
/// head-capture/export race a bare head ID plus a later export would have. A
/// failed export returns `Err`, so a usable cursor structurally cannot outlive
/// a broken snapshot.
pub(super) async fn capture_change_baseline<W: Write>(
    db: &Omnigraph,
    branch: &str,
    scope: &crate::changes::ChangeFeedScope,
    writer: &mut W,
) -> Result<crate::changes::ChangeBaseline> {
    let parts = capture_baseline_parts(db, branch, scope, false).await?;
    let _slot = parts.slot;
    let mut emit =
        |chunk: Vec<u8>| std::future::ready(writer.write_all(&chunk).map_err(OmniError::from));
    export_selected_tables(
        db,
        &parts.snapshot,
        parts.catalog.as_ref(),
        &parts.selected_tables,
        &mut emit,
    )
    .await?;
    Ok(parts.handshake)
}

async fn entity_from_snapshot(
    db: &Omnigraph,
    snapshot: &Snapshot,
    type_key: &str,
    id: &str,
) -> Result<Option<serde_json::Value>> {
    if snapshot.dataset(type_key).is_none() {
        return Ok(None);
    }

    let ds = db
        .storage()
        .open_snapshot_at_table(snapshot, type_key)
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

fn export_type_keys(snapshot: &Snapshot, type_names: &[String]) -> Result<Vec<String>> {
    let available = snapshot
        .datasets()
        .map(|entry| entry.type_key.clone())
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();

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
            .map_err(crate::table_store::TableStore::ordered_scan_error)?
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
        .map_err(crate::table_store::TableStore::ordered_scan_error)?
    {
        for row_index in 0..batch.num_rows() {
            let row = batch.slice(row_index, 1);
            let row_id = row
                .column_by_name("_rowid")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
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

pub(crate) async fn export_blob_values(
    source_ds: &Dataset,
    batch: &RecordBatch,
    row_ids: &[u64],
    blob_properties: &std::collections::HashSet<String>,
) -> Result<HashMap<String, Vec<Option<String>>>> {
    let mut values = HashMap::with_capacity(blob_properties.len());
    let mut __dst_props: Vec<_> = blob_properties.iter().collect();
    __dst_props.sort();
    for property in __dst_props {
        let descriptions = batch
            .column_by_name(property)
            .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| {
                OmniError::blob_integrity(format!(
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
///
/// A pinned Lance version is the commit-era schema authority: the image is
/// decoded from the batch's own schema, never the live catalog, so retained
/// commits stay readable after rename/add/drop. Only the exact reserved Lance
/// virtual columns are excluded — a legal user property whose name merely
/// starts with `_row` is preserved.
pub(crate) async fn logical_row_image(
    source_ds: &Dataset,
    batch: &RecordBatch,
    row: usize,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    use crate::changes::model::is_reserved_storage_system_column;

    let row_batch = batch.slice(row, 1);
    let blob_properties = row_batch
        .schema()
        .fields()
        .iter()
        .filter_map(|field| {
            let lance_field = lance::datatypes::Field::try_from(field.as_ref())
                .map_err(OmniError::lance_internal);
            match lance_field {
                Ok(field) if field.is_blob() => Some(Ok(field.name.clone())),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<Result<std::collections::HashSet<_>>>()?;
    let blob_values = if blob_properties.is_empty() {
        None
    } else {
        let row_id = row_batch
            .column_by_name("_rowid")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| OmniError::manifest_internal("change row is missing _rowid"))?
            .value(0);
        Some(export_blob_values(source_ds, &row_batch, &[row_id], &blob_properties).await?)
    };

    let mut image = serde_json::Map::new();
    for field in row_batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !is_reserved_storage_system_column(field.name()))
    {
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
    Ok(image)
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
        .map_err(OmniError::storage)?;

    if sorted_blobs.len() != managed_positions.len() {
        return Err(OmniError::blob_integrity(format!(
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
            OmniError::blob_integrity(format!(
                "blob export for '{}' returned a null accessor for a managed description",
                column_name
            ))
        })?;
        if blob.uri().is_some() {
            return Err(OmniError::blob_integrity(format!(
                "blob export for '{}' resolved a managed description as external",
                column_name
            )));
        }
        let bytes = blob.read().await.map_err(OmniError::storage)?;
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
        OmniError::manifest_internal(format!("missing column '{}' in export batch", field_name))
    })?;
    json_value_from_array(column.as_ref(), row)
}

fn named_string_value(batch: &RecordBatch, field_name: &str, row: usize) -> Result<String> {
    let column = batch.column_by_name(field_name).ok_or_else(|| {
        OmniError::manifest_internal(format!("missing column '{}' in export batch", field_name))
    })?;
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("expected Utf8 column '{}'", field_name))
        })?;
    if array.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "unexpected null in export column '{}'",
            field_name
        )));
    }
    Ok(array.value(row).to_string())
}
