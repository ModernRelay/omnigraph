use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;

use arrow_array::{Array, RecordBatch, StringArray, StructArray, UInt64Array};
use arrow_cast::display::array_value_to_string;
use base64::Engine;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance_core::datatypes::BlobHandling;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ChangeOp, Endpoints, EntityChange, EntityKind, changed_table_intervals, parse_table_key,
};
use crate::blob::BlobDescriptorDecoder;
use crate::db::manifest::{Snapshot, TableIdentity};
use crate::db::{SubTableEntry, export_blob_values, logical_row_image};
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

pub const COMMIT_CHANGES_DEFAULT_ROWS: usize = 1_000;
pub const COMMIT_CHANGES_MAX_ROWS: usize = 8_192;
pub const COMMIT_CHANGES_DEFAULT_BYTES: u64 = 4 * 1024 * 1024;
pub const COMMIT_CHANGES_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// Ordered-scan batch shape, matching the other production ordered-by-id
/// scans (branch merge, export): row estimate plus byte target, never strict.
const COMMIT_CHANGES_SCAN_ROWS: usize = 8_192;

const CURSOR_VERSION: u8 = 1;
const CURSOR_CHECKSUM_BYTES: usize = 32;

#[derive(Debug, Clone)]
pub struct CommitChangesPage {
    pub changes: Vec<EntityChange>,
    pub next_cursor: Option<String>,
    pub commit_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitChangesCursor {
    version: u8,
    graph_identity: String,
    commit_id: String,
    table_key: String,
    stable_table_id: u64,
    table_incarnation_id: u64,
    id: String,
    operation_rank: u8,
    change_index: usize,
}

impl CommitChangesCursor {
    fn identity(&self) -> TableIdentity {
        TableIdentity {
            stable_table_id: self.stable_table_id,
            table_incarnation_id: self.table_incarnation_id,
        }
    }
}

/// One scanned row prepared for comparison. Holds display signatures and a
/// zero-copy one-row slice; no JSON image and no Blob payload I/O. Images are
/// materialized only for rows that actually enter the page.
#[derive(Debug, Clone)]
struct RawRow {
    id: String,
    /// One-row slice of the scanned batch, retaining `_rowid` and descriptor
    /// columns for lazy image and payload access.
    slice: RecordBatch,
    /// Display rendering of every non-Blob, non-storage column, joined with
    /// the same separator the diff comparator uses.
    scalar_signature: String,
    /// `(column, physical descriptor identity)` per Blob column, in schema
    /// order. Equal identities on one table lifetime imply equal payloads.
    blob_signatures: Vec<(String, String)>,
}

struct OrderedRows {
    dataset: Option<Dataset>,
    stream: Option<Pin<Box<DatasetRecordBatchStream>>>,
    pending: VecDeque<RawRow>,
}

impl OrderedRows {
    async fn open(
        store: &TableStore,
        entry: Option<&SubTableEntry>,
        after_id: Option<&str>,
    ) -> Result<Self> {
        let (dataset, stream) = if let Some(entry) = entry {
            let dataset = store.open_at_entry(entry).await?;
            let after_id = after_id.map(str::to_string);
            let stream = Some(Box::pin(
                TableStore::scan_stream_with(
                    &dataset,
                    None,
                    None,
                    Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    true,
                    move |scanner| {
                        if let Some(after_id) = after_id {
                            scanner.filter_expr(col("id").gt(lit(after_id)));
                        }
                        // Descriptor-sized batches bounded by rows AND bytes, the
                        // same shape as every other production ordered-by-id scan.
                        // strict_batch_size is deliberately absent: Lance's strict
                        // stream coalesces to a row count the environment can
                        // override and ignores the byte target while accumulating.
                        scanner.batch_size(COMMIT_CHANGES_SCAN_ROWS);
                        scanner.batch_size_bytes(COMMIT_CHANGES_MAX_BYTES);
                        scanner.blob_handling(BlobHandling::BlobsDescriptions);
                        Ok(())
                    },
                )
                .await?,
            ));
            (Some(dataset), stream)
        } else {
            (None, None)
        };
        Ok(Self {
            dataset,
            stream,
            pending: VecDeque::new(),
        })
    }

    async fn peek(&mut self) -> Result<Option<RawRow>> {
        self.fill().await?;
        Ok(self.pending.front().cloned())
    }

    async fn pop(&mut self) -> Result<Option<RawRow>> {
        self.fill().await?;
        Ok(self.pending.pop_front())
    }

    async fn fill(&mut self) -> Result<()> {
        while self.pending.is_empty() {
            let Some(stream) = self.stream.as_mut() else {
                return Ok(());
            };
            match stream.try_next().await {
                Ok(Some(batch)) => self.pending = prepare_batch(&batch)?,
                Ok(None) => {
                    self.stream = None;
                    return Ok(());
                }
                Err(error) => return Err(OmniError::Lance(error.to_string())),
            }
        }
        Ok(())
    }

    fn dataset(&self) -> &Dataset {
        self.dataset
            .as_ref()
            .expect("a side that produced a row has a dataset")
    }
}

/// Turn one scanned batch into comparison-ready rows: display signatures for
/// scalar columns and physical descriptor identities for Blob columns. This
/// is pure in-memory work — Blob payloads are never touched here.
fn prepare_batch(batch: &RecordBatch) -> Result<VecDeque<RawRow>> {
    let ids = batch
        .column_by_name("id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| OmniError::Lance("change row is missing string id".to_string()))?;

    enum ColumnClass<'a> {
        Scalar(&'a dyn Array),
        Blob(&'a str, BlobDescriptorDecoder<'a>),
    }

    let mut classes = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        if field.name().starts_with("_row") {
            continue;
        }
        let lance_field = lance::datatypes::Field::try_from(field.as_ref())
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if lance_field.is_blob() {
            let descriptions = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    OmniError::Lance(format!(
                        "expected blob descriptions for change column '{}'",
                        field.name()
                    ))
                })?;
            classes.push(ColumnClass::Blob(
                field.name().as_str(),
                BlobDescriptorDecoder::try_new(descriptions)?,
            ));
        } else {
            classes.push(ColumnClass::Scalar(column.as_ref()));
        }
    }

    let mut rows = VecDeque::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut scalars = Vec::new();
        let mut blob_signatures = Vec::new();
        for class in &classes {
            match class {
                ColumnClass::Scalar(column) => scalars.push(
                    array_value_to_string(*column, row)
                        .map_err(|error| OmniError::Lance(error.to_string()))?,
                ),
                ColumnClass::Blob(name, decoder) => {
                    blob_signatures.push((name.to_string(), decoder.physical_identity(row)?));
                }
            }
        }
        rows.push_back(RawRow {
            id: ids.value(row).to_string(),
            slice: batch.slice(row, 1),
            scalar_signature: scalars.join("\u{1f}"),
            blob_signatures,
        });
    }
    Ok(rows)
}

/// Payload-free row equality with an exact payload tie-break.
///
/// Scalar columns compare by display signature; Blob columns compare by
/// physical descriptor identity first. Only a descriptor tie — same scalars,
/// different descriptor — pays payload I/O, because compaction can relocate
/// identical bytes and must not surface as a phantom update. The payload
/// comparison uses the same logical values export emits (managed bytes,
/// external reference URI, null), so equality here is exactly image equality.
async fn rows_equal(
    from_dataset: &Dataset,
    left: &RawRow,
    to_dataset: &Dataset,
    right: &RawRow,
) -> Result<bool> {
    if left.scalar_signature != right.scalar_signature {
        return Ok(false);
    }
    if left.blob_signatures == right.blob_signatures {
        return Ok(true);
    }
    let column_sets_match = left.blob_signatures.len() == right.blob_signatures.len()
        && left
            .blob_signatures
            .iter()
            .zip(&right.blob_signatures)
            .all(|(l, r)| l.0 == r.0);
    if !column_sets_match {
        // The two pinned versions disagree on the Blob column set, so the
        // logical image necessarily changed shape.
        return Ok(false);
    }
    let differing: HashSet<String> = left
        .blob_signatures
        .iter()
        .zip(&right.blob_signatures)
        .filter(|(l, r)| l.1 != r.1)
        .map(|(l, _)| l.0.clone())
        .collect();
    let left_values = blob_values_for(from_dataset, &left.slice, &differing).await?;
    let right_values = blob_values_for(to_dataset, &right.slice, &differing).await?;
    Ok(left_values == right_values)
}

/// Logical Blob values (managed bytes, external URI, or null) for one row's
/// selected columns, read through the same helper export uses.
async fn blob_values_for(
    dataset: &Dataset,
    slice: &RecordBatch,
    columns: &HashSet<String>,
) -> Result<HashMap<String, Vec<Option<String>>>> {
    let row_id = slice
        .column_by_name("_rowid")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| OmniError::Lance("change row is missing _rowid".to_string()))?
        .value(0);
    export_blob_values(dataset, slice, &[row_id], columns).await
}

/// Materialize the exact logical image (and edge endpoints) for one row that
/// is entering the page. This is the only place Blob payloads are read for
/// emitted changes.
async fn emitted_image(
    dataset: &Dataset,
    raw: &RawRow,
    is_edge: bool,
) -> Result<(serde_json::Value, Option<Endpoints>)> {
    let image = logical_row_image(dataset, &raw.slice, 0).await?;
    let endpoints = if is_edge {
        let object = image
            .as_object()
            .ok_or_else(|| OmniError::Lance("edge image is not an object".to_string()))?;
        Some(Endpoints {
            src: object
                .get("src")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| OmniError::Lance("edge image is missing src".to_string()))?
                .to_string(),
            dst: object
                .get("dst")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| OmniError::Lance("edge image is missing dst".to_string()))?
                .to_string(),
        })
    } else {
        None
    };
    Ok((image, endpoints))
}

fn entity_change(
    table_key: &str,
    kind: EntityKind,
    type_name: &str,
    op: ChangeOp,
    manifest_version: u64,
    id: String,
    endpoints: Option<Endpoints>,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> EntityChange {
    EntityChange {
        table_key: table_key.to_string(),
        change_index: 0,
        kind,
        type_name: type_name.to_string(),
        id,
        op,
        manifest_version,
        endpoints,
        before,
        after,
    }
}

/// The next logical change in the ordered id merge, or `None` when both sides
/// are exhausted. Rows that compare equal are consumed without emitting and
/// without payload or image work.
async fn next_change(
    from: &mut OrderedRows,
    to: &mut OrderedRows,
    table_key: &str,
    kind: EntityKind,
    type_name: &str,
    manifest_version: u64,
) -> Result<Option<EntityChange>> {
    enum Emit {
        Delete(RawRow),
        Insert(RawRow),
        Update(RawRow),
    }
    let is_edge = kind == EntityKind::Edge;
    loop {
        let left = from.peek().await?;
        let right = to.peek().await?;
        let emit = match (left, right) {
            (None, None) => return Ok(None),
            (Some(_), None) => Emit::Delete(from.pop().await?.expect("peeked row present")),
            (None, Some(_)) => Emit::Insert(to.pop().await?.expect("peeked row present")),
            (Some(left), Some(right)) if left.id < right.id => {
                Emit::Delete(from.pop().await?.expect("peeked row present"))
            }
            (Some(left), Some(right)) if left.id > right.id => {
                Emit::Insert(to.pop().await?.expect("peeked row present"))
            }
            (Some(_), Some(_)) => {
                let left = from.pop().await?.expect("peeked row present");
                let right = to.pop().await?.expect("peeked row present");
                if rows_equal(from.dataset(), &left, to.dataset(), &right).await? {
                    continue;
                }
                Emit::Update(right)
            }
        };
        let change = match emit {
            Emit::Delete(raw) => {
                let (image, endpoints) = emitted_image(from.dataset(), &raw, is_edge).await?;
                entity_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Delete,
                    manifest_version,
                    raw.id,
                    endpoints,
                    Some(image),
                    None,
                )
            }
            Emit::Insert(raw) => {
                let (image, endpoints) = emitted_image(to.dataset(), &raw, is_edge).await?;
                entity_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Insert,
                    manifest_version,
                    raw.id,
                    endpoints,
                    None,
                    Some(image),
                )
            }
            Emit::Update(raw) => {
                let (image, endpoints) = emitted_image(to.dataset(), &raw, is_edge).await?;
                entity_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Update,
                    manifest_version,
                    raw.id,
                    endpoints,
                    None,
                    Some(image),
                )
            }
        };
        return Ok(Some(change));
    }
}

pub(crate) async fn commit_changes_page(
    store: &TableStore,
    from: &Snapshot,
    to: &Snapshot,
    graph_identity: &str,
    commit_id: &str,
    cursor: Option<&str>,
    limit: usize,
    max_bytes: u64,
) -> Result<CommitChangesPage> {
    validate_limits(limit, max_bytes)?;
    let cursor = cursor
        .map(decode_cursor)
        .transpose()?
        .map(|cursor| validate_cursor(cursor, graph_identity, commit_id))
        .transpose()?;
    let mut changes: Vec<EntityChange> = Vec::with_capacity(limit.min(256));
    let mut retained_bytes = 0_u64;
    let mut change_index = cursor.as_ref().map_or(0, |cursor| cursor.change_index + 1);
    let mut last_identity: Option<TableIdentity> = None;

    let mut cursor_identity_seen = cursor.is_none();
    for interval in changed_table_intervals(from, to) {
        let table_key = interval
            .to
            .or(interval.from)
            .expect("changed interval has one endpoint")
            .table_key
            .as_str();
        if cursor.as_ref().is_some_and(|cursor| {
            table_key < cursor.table_key.as_str()
                || (table_key == cursor.table_key.as_str() && interval.identity < cursor.identity())
        }) {
            continue;
        }
        if cursor.as_ref().is_some_and(|cursor| {
            table_key == cursor.table_key.as_str() && cursor.identity() == interval.identity
        }) {
            cursor_identity_seen = true;
        }
        let (kind, type_name) = parse_table_key(table_key);
        let after_id = cursor
            .as_ref()
            .filter(|cursor| {
                cursor.table_key == table_key && cursor.identity() == interval.identity
            })
            .map(|cursor| cursor.id.as_str());
        // ponytail: exact endpoint images currently scan each changed table lifetime;
        // use a stable bounded Lance change stream here when one becomes available.
        let mut left = OrderedRows::open(store, interval.from, after_id).await?;
        let mut right = OrderedRows::open(store, interval.to, after_id).await?;

        while let Some(mut change) = next_change(
            &mut left,
            &mut right,
            table_key,
            kind,
            type_name,
            to.version(),
        )
        .await?
        {
            change.change_index = change_index;
            let encoded_bytes = u64::try_from(
                serde_json::to_vec(&change)
                    .map_err(|error| OmniError::Lance(error.to_string()))?
                    .len(),
            )
            .map_err(|_| OmniError::manifest_internal("change image size exceeds u64"))?;
            if changes.len() == limit
                || retained_bytes
                    .checked_add(encoded_bytes)
                    .is_none_or(|bytes| bytes > max_bytes)
            {
                if changes.is_empty() {
                    return Err(OmniError::ResourceLimitExceeded {
                        resource: "commit_changes_page_bytes".to_string(),
                        limit: max_bytes,
                        actual: encoded_bytes,
                    });
                }
                let last = changes.last().expect("non-empty page");
                return Ok(CommitChangesPage {
                    next_cursor: Some(encode_cursor(&CommitChangesCursor {
                        version: CURSOR_VERSION,
                        graph_identity: cursor_graph_identity(graph_identity),
                        commit_id: commit_id.to_string(),
                        table_key: last.table_key.clone(),
                        stable_table_id: last_identity.expect("non-empty page").stable_table_id,
                        table_incarnation_id: last_identity
                            .expect("non-empty page")
                            .table_incarnation_id,
                        id: last.id.clone(),
                        operation_rank: operation_rank(last.op),
                        change_index: last.change_index,
                    })?),
                    changes,
                    commit_complete: false,
                });
            }
            retained_bytes += encoded_bytes;
            last_identity = Some(interval.identity);
            changes.push(change);
            change_index += 1;
        }
    }

    if !cursor_identity_seen {
        return Err(cursor_rejected(
            "commit changes cursor no longer names a changed table",
        ));
    }

    Ok(CommitChangesPage {
        changes,
        next_cursor: None,
        commit_complete: true,
    })
}

fn validate_limits(limit: usize, max_bytes: u64) -> Result<()> {
    if limit == 0 {
        return Err(OmniError::manifest(
            "commit changes limit must be greater than zero",
        ));
    }
    if limit > COMMIT_CHANGES_MAX_ROWS {
        return Err(OmniError::ResourceLimitExceeded {
            resource: "commit_changes_page_rows".to_string(),
            limit: COMMIT_CHANGES_MAX_ROWS as u64,
            actual: limit as u64,
        });
    }
    if max_bytes == 0 {
        return Err(OmniError::manifest(
            "commit changes max_bytes must be greater than zero",
        ));
    }
    if max_bytes > COMMIT_CHANGES_MAX_BYTES {
        return Err(OmniError::ResourceLimitExceeded {
            resource: "commit_changes_page_bytes".to_string(),
            limit: COMMIT_CHANGES_MAX_BYTES,
            actual: max_bytes,
        });
    }
    Ok(())
}

fn operation_rank(op: ChangeOp) -> u8 {
    match op {
        ChangeOp::Insert => 0,
        ChangeOp::Update => 1,
        ChangeOp::Delete => 2,
    }
}

fn cursor_rejected(reason: impl Into<String>) -> OmniError {
    OmniError::ChangeCursorRejected {
        reason: reason.into(),
    }
}

fn validate_cursor(
    cursor: CommitChangesCursor,
    graph_identity: &str,
    commit_id: &str,
) -> Result<CommitChangesCursor> {
    if cursor.version != CURSOR_VERSION {
        return Err(cursor_rejected("unsupported commit changes cursor version"));
    }
    if cursor.graph_identity != cursor_graph_identity(graph_identity)
        || cursor.commit_id != commit_id
    {
        return Err(cursor_rejected(
            "commit changes cursor does not match this graph and commit",
        ));
    }
    if cursor.table_key.is_empty()
        || cursor.stable_table_id == 0
        || cursor.table_incarnation_id == 0
        || cursor.operation_rank > operation_rank(ChangeOp::Delete)
    {
        return Err(cursor_rejected("invalid commit changes cursor identity"));
    }
    Ok(cursor)
}

fn cursor_graph_identity(graph_identity: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(graph_identity.as_bytes()))
}

fn encode_cursor(cursor: &CommitChangesCursor) -> Result<String> {
    let payload =
        serde_json::to_vec(cursor).map_err(|error| OmniError::manifest(error.to_string()))?;
    let digest = Sha256::digest(&payload);
    let mut encoded = Vec::with_capacity(payload.len() + digest.len());
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&digest);
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_cursor(cursor: &str) -> Result<CommitChangesCursor> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| cursor_rejected("invalid commit changes cursor encoding"))?;
    if bytes.len() <= CURSOR_CHECKSUM_BYTES {
        return Err(cursor_rejected("invalid commit changes cursor"));
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - CURSOR_CHECKSUM_BYTES);
    if Sha256::digest(payload).as_slice() != checksum {
        return Err(cursor_rejected("invalid commit changes cursor checksum"));
    }
    serde_json::from_slice(payload)
        .map_err(|_| cursor_rejected("invalid commit changes cursor payload"))
}
