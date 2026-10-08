use super::*;
use crate::changes::model::is_reserved_storage_system_column;
use crate::ordered_cursor::{KeyOrder, OrderedRowCursor, WalkSubject};
use futures::TryStreamExt;
use omnigraph_compiler::SYSTEM_COLUMNS_META;
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
    ranged: RangedExternalBlobs,
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
            ExportRowOrder::ById,
            self.ranged,
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
        let (resolved, catalog) = self.capture_read_view(ReadTarget::branch(branch)).await?;
        let snapshot = resolved.snapshot;
        let selected_tables = export_type_keys(&snapshot, type_names)?;

        Ok(ExportCut {
            db: Arc::clone(self),
            snapshot,
            catalog,
            selected_tables,
            ranged: RangedExternalBlobs::Refuse,
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
        let parts = capture_baseline_parts(self, branch, scope).await?;
        Ok((
            parts.handshake,
            ExportCut {
                db: Arc::clone(self),
                snapshot: parts.snapshot,
                catalog: parts.catalog,
                selected_tables: parts.selected_tables,
                ranged: RangedExternalBlobs::Describe,
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
        ExportRowOrder::ById,
        RangedExternalBlobs::Refuse,
        &mut emit,
    )
    .await
}

/// Stream the same logical row encoding as ordinary export without imposing a
/// physical row order. This is for commutative consumers such as fixture
/// multiset hashing; callers must not infer meaning from emission order.
pub(super) async fn export_jsonl_unordered_to_writer<W: Write>(
    db: &Omnigraph,
    branch: &str,
    type_names: &[String],
    writer: &mut W,
) -> Result<()> {
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
        ExportRowOrder::Unspecified,
        RangedExternalBlobs::Refuse,
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
) -> Result<BaselineParts> {
    let slot = db.write_queue().try_acquire_export_cut().ok_or_else(|| {
        OmniError::ResourceLimitExceeded {
            resource: "stream_export_slots".to_string(),
            limit: 1,
            actual: 2,
        }
    })?;
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
    let parts = capture_baseline_parts(db, branch, scope).await?;
    let _slot = parts.slot;
    let mut emit =
        |chunk: Vec<u8>| std::future::ready(writer.write_all(&chunk).map_err(OmniError::from));
    export_selected_tables(
        db,
        &parts.snapshot,
        parts.catalog.as_ref(),
        &parts.selected_tables,
        ExportRowOrder::ById,
        RangedExternalBlobs::Describe,
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
    let Some(entry) = snapshot.dataset(type_key) else {
        return Ok(None);
    };

    let ds = db
        .storage()
        .open_snapshot_at_table(snapshot, type_key)
        .await?;
    let system_columns =
        crate::db::manifest::system_columns_at_image(ds.dataset().schema(), &entry.type_key)?;
    let filter_sql = format!("{} = '{}'", system_columns.id, id.replace('\'', "''"));
    let mut batches = db
        .storage()
        .scan_stream_bounded(
            &ds,
            None,
            Some(&filter_sql),
            None,
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
        if batch.num_rows() > 0 {
            let mut image = logical_row_image(ds.dataset(), &batch, 0, system_columns.id).await?;
            for (physical, logical) in [(system_columns.id, SYSTEM_COLUMNS_META.id)]
                .into_iter()
                .chain(
                    type_key
                        .starts_with("edge:")
                        .then_some([
                            (system_columns.src, SYSTEM_COLUMNS_META.src),
                            (system_columns.dst, SYSTEM_COLUMNS_META.dst),
                        ])
                        .into_iter()
                        .flatten(),
                )
            {
                let value = image.remove(physical).ok_or_else(|| {
                    OmniError::manifest_internal(format!("entity image is missing '{physical}'"))
                })?;
                image.insert(logical.to_string(), value);
            }
            image.retain(|_, value| !value.is_null());
            return Ok(Some(serde_json::Value::Object(image)));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportRowOrder {
    ById,
    Unspecified,
}

/// Emit every selected table's rows in the export line format. `ranged`
/// decides a ranged external Blob reference: export refuses it, because a bare
/// URI reloads as the whole object; the change-feed baseline describes it as
/// change images do, because its consumer starts from exactly this state.
async fn export_selected_tables<Emit, EmitFuture>(
    db: &Omnigraph,
    snapshot: &Snapshot,
    catalog: &Catalog,
    selected_tables: &[String],
    row_order: ExportRowOrder,
    ranged: RangedExternalBlobs,
    emit: &mut Emit,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    let mut chunks = ExportChunks {
        emit,
        pending: Vec::new(),
    };
    for table_key in selected_tables {
        export_table(
            db,
            snapshot,
            catalog,
            table_key,
            row_order,
            ranged,
            &mut chunks,
        )
        .await?;
    }
    chunks.flush().await
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
    row_order: ExportRowOrder,
    ranged: RangedExternalBlobs,
    emit: &mut ExportChunks<'_, Emit>,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    let ds = db
        .storage()
        .open_snapshot_at_table(snapshot, table_key)
        .await?;
    // Blob materialization reaches through to the inner Lance `Dataset`
    // because `take_blobs` is a Lance-only API not lifted onto the
    // `TableStorage` trait surface (the trait covers staged-write and
    // snapshot-scan primitives; blob descriptor materialization sits outside
    // that surface). The ordered walk reads the same pinned dataset.
    let source_ds = ds.dataset();
    let blob_properties = blob_properties_for_table_key(catalog, table_key)?;

    if row_order == ExportRowOrder::ById {
        // Sort keys only and hydrate complete rows in bounded chunks: a
        // complete-row sort fails on a row wider than the ordered-scan sort
        // cap, and on an ordinary row arriving as a slice of a larger decoded
        // batch.
        let mut rows = OrderedRowCursor::open(
            Some(source_ds.clone()),
            KeyOrder {
                key_batch_rows: EXPORT_SCAN_TARGET_ROWS,
                key_batch_bytes: EXPORT_SCAN_TARGET_BYTES,
                chunk_rows: EXPORT_SCAN_TARGET_ROWS,
                chunk_bytes: EXPORT_SCAN_TARGET_BYTES,
                ..KeyOrder::full()
            },
            WalkSubject {
                operation: "export",
                table: table_key.to_string(),
                role: "export",
            },
            catalog.system_columns.id,
        )
        .await?;
        while let Some(batch) = rows.next_ordered_batch().await? {
            if blob_properties.is_empty() {
                emit_export_rows_from_batch(catalog, table_key, &batch, None, emit).await?;
                continue;
            }
            for row_index in 0..batch.num_rows() {
                let row = batch.slice(row_index, 1);
                emit_export_row(
                    source_ds,
                    catalog,
                    table_key,
                    &row,
                    blob_properties,
                    ranged,
                    emit,
                )
                .await?;
            }
        }
        return Ok(());
    }

    if blob_properties.is_empty() {
        let mut batches = db
            .storage()
            .scan_stream_bounded(
                &ds,
                None,
                None,
                None,
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
            None,
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
            emit_export_row(
                source_ds,
                catalog,
                table_key,
                &row,
                blob_properties,
                ranged,
                emit,
            )
            .await?;
        }
    }
    Ok(())
}

/// Emit one scanned row, materializing at most that row's Blob values. The
/// row must carry `_rowid` when the table has Blob properties.
async fn emit_export_row<Emit, EmitFuture>(
    source_ds: &Dataset,
    catalog: &Catalog,
    table_key: &str,
    row: &RecordBatch,
    blob_properties: &std::collections::HashSet<String>,
    ranged: RangedExternalBlobs,
    emit: &mut ExportChunks<'_, Emit>,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    if blob_properties.is_empty() {
        return emit_export_rows_from_batch(catalog, table_key, row, None, emit).await;
    }
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
    let blob_values =
        export_blob_values(source_ds, row, &[row_id], blob_properties, ranged).await?;
    emit_export_rows_from_batch(catalog, table_key, row, Some(&blob_values), emit).await
}

/// One logical Blob cell value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicalBlobValue {
    /// Managed bytes as `base64:…`, or an external reference to a whole
    /// object as its URI: the load format's own spelling.
    Text(String),
    /// An external descriptor naming a byte range of its object. The load
    /// format has no spelling for it; only a caller that asked to describe
    /// ranges receives one.
    RangedExternal(crate::blob::ExternalBlobRef),
}

/// What a Blob value reader does with a ranged external descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangedExternalBlobs {
    /// Fail: the output is reloadable, and a bare URI reloads as the whole
    /// object, which would widen the cell.
    Refuse,
    /// Return the exact reference as [`LogicalBlobValue::RangedExternal`],
    /// which the row renderers describe as `{"uri", "offset", "length"}`.
    Describe,
}

/// The ranged external Blob cells of row `row` of `blob_values`, by property.
fn ranged_blob_cells(
    blob_values: Option<&HashMap<String, Vec<Option<LogicalBlobValue>>>>,
    row: usize,
) -> impl Iterator<Item = (&String, &crate::blob::ExternalBlobRef)> {
    blob_values
        .into_iter()
        .flatten()
        .filter_map(move |(name, cells)| match cells.get(row) {
            Some(Some(LogicalBlobValue::RangedExternal(reference))) => Some((name, reference)),
            _ => None,
        })
}

/// Write the ranged external Blob cells of row `row` into that row's JSON
/// object, which [`row_json_lines`] rendered with null in their place, as the
/// exact `{"uri", "offset", "length"}` reference.
fn describe_ranged_blob_cells(
    image: &mut serde_json::Map<String, serde_json::Value>,
    blob_values: Option<&HashMap<String, Vec<Option<LogicalBlobValue>>>>,
    row: usize,
) -> Result<()> {
    for (name, reference) in ranged_blob_cells(blob_values, row) {
        // The descriptor decoder gives every ranged reference a positive
        // length: it refuses an offset with size 0, and offset 0 with size 0
        // is the whole object, never ranged.
        let length = reference.length.ok_or_else(|| {
            OmniError::manifest_internal("a ranged external Blob reference carries no length")
        })?;
        image.insert(
            name.clone(),
            serde_json::json!({
                "uri": reference.uri,
                "offset": reference.offset,
                "length": length,
            }),
        );
    }
    Ok(())
}

pub(crate) async fn export_blob_values(
    source_ds: &Dataset,
    batch: &RecordBatch,
    row_ids: &[u64],
    blob_properties: &std::collections::HashSet<String>,
    ranged: RangedExternalBlobs,
) -> Result<HashMap<String, Vec<Option<LogicalBlobValue>>>> {
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
            export_blob_column_values(source_ds, property, descriptions, row_ids, ranged).await?,
        );
    }
    Ok(values)
}

/// Convert one descriptor-scanned row into the same logical value shape used
/// by export, materializing at most that row's Blob values. A ranged external
/// descriptor, which export refuses, is described exactly as the object
/// `{"uri", "offset", "length"}` without contacting the object.
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
    id_col: &str,
) -> Result<serde_json::Map<String, serde_json::Value>> {
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
        Some(
            export_blob_values(
                source_ds,
                &row_batch,
                &[row_id],
                &blob_properties,
                RangedExternalBlobs::Describe,
            )
            .await?,
        )
    };
    let fields = row_batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !is_reserved_storage_system_column(field.name()))
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let lines = row_json_lines(&row_batch, &fields, blob_values.as_ref(), 0, id_col)?;
    let bytes = json_rows(&lines)
        .next()
        .ok_or_else(|| OmniError::manifest_internal("row image rendered no JSON object"))?;
    let mut image: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
        .map_err(|err| {
            OmniError::manifest_internal(format!("row image did not parse as a JSON object: {err}"))
        })?;
    for name in fields {
        image.entry(name).or_insert(serde_json::Value::Null);
    }
    describe_ranged_blob_cells(&mut image, blob_values.as_ref(), 0)?;
    Ok(image)
}

/// One rendered row's `data` object, with its ranged external Blob cells
/// described. Only a row that holds one is parsed and written again.
fn export_row_data<'a>(
    data: &'a [u8],
    blob_values: Option<&HashMap<String, Vec<Option<LogicalBlobValue>>>>,
    row: usize,
) -> Result<std::borrow::Cow<'a, [u8]>> {
    if ranged_blob_cells(blob_values, row).next().is_none() {
        return Ok(std::borrow::Cow::Borrowed(data));
    }
    let mut object: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(data)
        .map_err(|err| {
            OmniError::manifest_internal(format!(
                "export row did not parse as a JSON object: {err}"
            ))
        })?;
    describe_ranged_blob_cells(&mut object, blob_values, row)?;
    serde_json::to_vec(&object)
        .map(std::borrow::Cow::Owned)
        .map_err(|err| OmniError::manifest_internal(format!("export row did not encode: {err}")))
}

/// Emits one JSON line per row. The logical envelope keys the identity as `id`
/// beside `type` on every vintage, so `data` skips the leading system column and
/// carries user properties only (RFC 0040).
async fn emit_export_rows_from_batch<Emit, EmitFuture>(
    catalog: &Catalog,
    table_key: &str,
    batch: &RecordBatch,
    blob_values: Option<&HashMap<String, Vec<Option<LogicalBlobValue>>>>,
    emit: &mut ExportChunks<'_, Emit>,
) -> Result<()>
where
    Emit: FnMut(Vec<u8>) -> EmitFuture,
    EmitFuture: Future<Output = Result<()>>,
{
    let ids = named_string_column(batch, catalog.system_columns.id)?;
    if let Some(type_name) = table_key.strip_prefix("node:") {
        let node_type = catalog
            .node_types
            .get(type_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
        let fields = node_type
            .arrow_schema
            .fields()
            .iter()
            .skip(1)
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let mut prefix = b"{\"type\":".to_vec();
        json_string_into(&mut prefix, type_name)?;
        prefix.extend_from_slice(b",\"id\":");
        for rows in render_windows(batch.num_rows()) {
            let lines = row_json_lines(
                &batch.slice(rows.start, rows.len()),
                &fields,
                blob_values,
                rows.start,
                catalog.system_columns.id,
            )?;
            for (offset, data) in json_rows(&lines).enumerate() {
                let row = rows.start + offset;
                let data = export_row_data(data, blob_values, row)?;
                let mut line = Vec::with_capacity(prefix.len() + data.len() + 32);
                line.extend_from_slice(&prefix);
                json_string_into(&mut line, ids.value(row))?;
                line.extend_from_slice(b",\"data\":");
                line.extend_from_slice(&data);
                line.extend_from_slice(b"}\n");
                emit.line(line).await?;
            }
        }
        return Ok(());
    }

    if let Some(edge_name) = table_key.strip_prefix("edge:") {
        let sources = named_string_column(batch, catalog.system_columns.src)?;
        let destinations = named_string_column(batch, catalog.system_columns.dst)?;
        let edge_type = catalog
            .edge_types
            .get(edge_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_name)))?;
        let fields = edge_type
            .arrow_schema
            .fields()
            .iter()
            .skip(3)
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        for rows in render_windows(batch.num_rows()) {
            let lines = row_json_lines(
                &batch.slice(rows.start, rows.len()),
                &fields,
                blob_values,
                rows.start,
                catalog.system_columns.id,
            )?;
            for (offset, data) in json_rows(&lines).enumerate() {
                let row = rows.start + offset;
                let data = export_row_data(data, blob_values, row)?;
                let mut line = b"{\"edge\":".to_vec();
                json_string_into(&mut line, edge_name)?;
                line.extend_from_slice(b",\"id\":");
                json_string_into(&mut line, ids.value(row))?;
                line.extend_from_slice(b",\"from\":");
                json_string_into(&mut line, sources.value(row))?;
                line.extend_from_slice(b",\"to\":");
                json_string_into(&mut line, destinations.value(row))?;
                line.extend_from_slice(b",\"data\":");
                line.extend_from_slice(&data);
                line.extend_from_slice(b"}\n");
                emit.line(line).await?;
            }
        }
        return Ok(());
    }

    Err(OmniError::manifest(format!(
        "invalid export table key '{}'",
        table_key
    )))
}

fn json_string_into(out: &mut Vec<u8>, value: &str) -> Result<()> {
    serde_json::to_writer(out, value).map_err(|err| {
        OmniError::manifest_internal(format!("export string {value:?} did not encode: {err}"))
    })
}

/// One pending transport allocation, shared by consecutive JSON lines and
/// tables. A full chunk moves into the callback before another buffer is
/// allocated, so an awaited send never retains a second pending chunk here.
struct ExportChunks<'a, Emit> {
    emit: &'a mut Emit,
    pending: Vec<u8>,
}

impl<Emit> ExportChunks<'_, Emit> {
    async fn line<EmitFuture>(&mut self, line: Vec<u8>) -> Result<()>
    where
        Emit: FnMut(Vec<u8>) -> EmitFuture,
        EmitFuture: Future<Output = Result<()>>,
    {
        let mut remaining = line.as_slice();
        while !remaining.is_empty() {
            if self.pending.capacity() == 0 {
                self.pending = Vec::with_capacity(EXPORT_CHUNK_MAX_BYTES);
            }
            let count = remaining
                .len()
                .min(EXPORT_CHUNK_MAX_BYTES - self.pending.len());
            self.pending.extend_from_slice(&remaining[..count]);
            remaining = &remaining[count..];
            if self.pending.len() == EXPORT_CHUNK_MAX_BYTES {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn flush<EmitFuture>(&mut self) -> Result<()>
    where
        Emit: FnMut(Vec<u8>) -> EmitFuture,
        EmitFuture: Future<Output = Result<()>>,
    {
        if !self.pending.is_empty() {
            (self.emit)(std::mem::take(&mut self.pending)).await?;
        }
        Ok(())
    }
}

/// Rows rendered per JSON writer call before filling transport chunks, rather
/// than rendering a whole scanner batch into one scratch allocation.
const EXPORT_RENDER_ROWS: usize = 256;

fn render_windows(rows: usize) -> impl Iterator<Item = std::ops::Range<usize>> {
    (0..rows)
        .step_by(EXPORT_RENDER_ROWS)
        .map(move |start| start..(start + EXPORT_RENDER_ROWS).min(rows))
}

/// `batch` rendered by `QueryResult::to_json_lines` with its columns in `fields`
/// order; a column named in `blob_values` is emitted from those strings (indexed
/// from `first_row`) instead of the batch column. A ranged external value has no
/// string form and renders as null; its caller describes it with
/// [`describe_ranged_blob_cells`]. Iterate the rows with [`json_rows`].
fn row_json_lines(
    batch: &RecordBatch,
    fields: &[String],
    blob_values: Option<&HashMap<String, Vec<Option<LogicalBlobValue>>>>,
    first_row: usize,
    id_col: &str,
) -> Result<Vec<u8>> {
    let source = batch.schema();
    let mut schema_fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for name in fields {
        if let Some(values) = blob_values.and_then(|values| values.get(name)) {
            schema_fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
            let cells = values
                .iter()
                .skip(first_row)
                .take(batch.num_rows())
                .map(|cell| match cell {
                    Some(LogicalBlobValue::Text(text)) => Some(text.as_str()),
                    None | Some(LogicalBlobValue::RangedExternal(_)) => None,
                })
                .collect::<StringArray>();
            columns.push(Arc::new(cells));
            continue;
        }
        let (index, _) = source.column_with_name(name).ok_or_else(|| {
            OmniError::manifest_internal(format!("missing column '{}' in export batch", name))
        })?;
        schema_fields.push(source.fields()[index].clone());
        columns.push(batch.column(index).clone());
    }
    let schema = Arc::new(Schema::new(schema_fields));
    let projected = RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(OmniError::arrow_internal)?;
    if let Some(cell) =
        omnigraph_compiler::json_output::unformattable_date(std::slice::from_ref(&projected))
    {
        let id = batch
            .column_by_name(id_col)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .filter(|ids| ids.is_valid(cell.row))
            .map(|ids| ids.value(cell.row).to_string());
        return Err(OmniError::manifest(match id {
            Some(id) => format!("entity {id:?}: {cell}"),
            None => cell.to_string(),
        }));
    }
    let lines = omnigraph_compiler::result::QueryResult::new(schema, vec![projected])
        .to_json_lines()
        .map_err(|err| {
            OmniError::manifest_internal(format!("row did not render as JSON: {err}"))
        })?;
    let rendered = json_rows(&lines).count();
    if rendered != batch.num_rows() {
        return Err(OmniError::manifest_internal(format!(
            "rendered {} JSON rows for {} batch rows",
            rendered,
            batch.num_rows()
        )));
    }
    Ok(lines)
}

fn json_rows(lines: &[u8]) -> impl Iterator<Item = &[u8]> {
    lines
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
}

async fn export_blob_column_values(
    source_ds: &Dataset,
    column_name: &str,
    descriptions: &StructArray,
    row_ids: &[u64],
    ranged: RangedExternalBlobs,
) -> Result<Vec<Option<LogicalBlobValue>>> {
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
            crate::blob::BlobDescriptor::External {
                uri,
                offset,
                length,
            } => {
                // Descriptor-preserving: never open or probe a caller-owned
                // object merely to reproduce the stored reference. A bare URI
                // reloads as the whole object, so a ranged descriptor is
                // refused or described, never widened.
                let reference = crate::blob::ExternalBlobRef {
                    uri,
                    offset,
                    length,
                };
                values[row] = Some(match (reference.whole_object_uri(), ranged) {
                    (Ok(_), _) => LogicalBlobValue::Text(reference.uri),
                    (Err(_), RangedExternalBlobs::Describe) => {
                        LogicalBlobValue::RangedExternal(reference)
                    }
                    (Err(refused), RangedExternalBlobs::Refuse) => {
                        return Err(OmniError::manifest(format!(
                            "export cannot represent {refused} in '{column_name}': export writes an external Blob as a bare URI, which reloads as the whole object"
                        )));
                    }
                });
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
        values[position] = Some(LogicalBlobValue::Text(value));
    }

    Ok(values)
}

fn named_string_column<'a>(batch: &'a RecordBatch, field_name: &str) -> Result<&'a StringArray> {
    let column = batch.column_by_name(field_name).ok_or_else(|| {
        OmniError::manifest_internal(format!("missing column '{}' in export batch", field_name))
    })?;
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("expected Utf8 column '{}'", field_name))
        })?;
    if array.null_count() != 0 {
        return Err(OmniError::manifest_internal(format!(
            "unexpected null in export column '{}'",
            field_name
        )));
    }
    Ok(array)
}
