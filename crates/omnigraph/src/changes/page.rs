use std::{pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, StringArray};
use base64::Engine;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance_core::datatypes::BlobHandling;
use omnigraph_compiler::catalog::Catalog;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ChangeOp, Endpoints, EntityChange, EntityKind, changed_table_intervals, parse_table_key,
};
use crate::db::manifest::{Snapshot, TableIdentity};
use crate::db::{SubTableEntry, logical_row_image};
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

pub const COMMIT_CHANGES_DEFAULT_ROWS: usize = 1_000;
pub const COMMIT_CHANGES_MAX_ROWS: usize = 8_192;
pub const COMMIT_CHANGES_DEFAULT_BYTES: u64 = 4 * 1024 * 1024;
pub const COMMIT_CHANGES_MAX_BYTES: u64 = 32 * 1024 * 1024;

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

#[derive(Debug, Clone)]
struct LogicalRow {
    id: String,
    image: serde_json::Value,
    encoded: Vec<u8>,
    endpoints: Option<Endpoints>,
}

struct OrderedRows {
    dataset: Option<Dataset>,
    stream: Option<Pin<Box<DatasetRecordBatchStream>>>,
    batch: Option<RecordBatch>,
    row: usize,
    peeked: Option<LogicalRow>,
    catalog: Arc<Catalog>,
    table_key: String,
}

impl OrderedRows {
    async fn open(
        store: &TableStore,
        entry: Option<&SubTableEntry>,
        after_id: Option<&str>,
        catalog: Arc<Catalog>,
        table_key: &str,
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
                        // One row per batch makes retained scan memory bounded by
                        // the maximum legal logical row image.
                        scanner.batch_size(1);
                        scanner.batch_size_bytes(COMMIT_CHANGES_MAX_BYTES);
                        scanner.strict_batch_size(true);
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
            batch: None,
            row: 0,
            peeked: None,
            catalog,
            table_key: table_key.to_string(),
        })
    }

    async fn peek(&mut self, is_edge: bool) -> Result<Option<LogicalRow>> {
        if self.peeked.is_none() {
            self.peeked = self.next(is_edge).await?;
        }
        Ok(self.peeked.clone())
    }

    async fn pop(&mut self, is_edge: bool) -> Result<Option<LogicalRow>> {
        if self.peeked.is_some() {
            return Ok(self.peeked.take());
        }
        self.next(is_edge).await
    }

    async fn next(&mut self, is_edge: bool) -> Result<Option<LogicalRow>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let row = logical_row(
                        self.dataset.as_ref().expect("stream has dataset"),
                        self.catalog.as_ref(),
                        &self.table_key,
                        batch,
                        self.row,
                        is_edge,
                    )
                    .await?;
                    self.row += 1;
                    return Ok(Some(row));
                }
            }
            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            match stream.try_next().await {
                Ok(Some(batch)) => {
                    self.batch = Some(batch);
                    self.row = 0;
                }
                Ok(None) => {
                    self.stream = None;
                    self.batch = None;
                    return Ok(None);
                }
                Err(error) => return Err(OmniError::Lance(error.to_string())),
            }
        }
    }
}

async fn logical_row(
    dataset: &Dataset,
    catalog: &Catalog,
    table_key: &str,
    batch: &RecordBatch,
    row: usize,
    is_edge: bool,
) -> Result<LogicalRow> {
    let id = batch
        .column_by_name("id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| OmniError::Lance("change row is missing string id".to_string()))?
        .value(row)
        .to_string();
    let image = logical_row_image(dataset, catalog, table_key, batch, row).await?;
    let encoded =
        serde_json::to_vec(&image).map_err(|error| OmniError::Lance(error.to_string()))?;
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
    Ok(LogicalRow {
        id,
        image,
        encoded,
        endpoints,
    })
}

fn make_change(
    table_key: &str,
    kind: EntityKind,
    type_name: &str,
    op: ChangeOp,
    manifest_version: u64,
    before: Option<LogicalRow>,
    after: Option<LogicalRow>,
) -> EntityChange {
    let row = after.as_ref().or(before.as_ref()).expect("one row image");
    EntityChange {
        table_key: table_key.to_string(),
        change_index: 0,
        kind,
        type_name: type_name.to_string(),
        id: row.id.clone(),
        op,
        manifest_version,
        endpoints: row.endpoints.clone(),
        before: before.map(|row| row.image),
        after: after.map(|row| row.image),
    }
}

async fn next_change(
    from: &mut OrderedRows,
    to: &mut OrderedRows,
    table_key: &str,
    kind: EntityKind,
    type_name: &str,
    manifest_version: u64,
) -> Result<Option<EntityChange>> {
    loop {
        let left = from.peek(kind == EntityKind::Edge).await?;
        let right = to.peek(kind == EntityKind::Edge).await?;
        let change = match (left, right) {
            (None, None) => return Ok(None),
            (Some(left), None) => {
                from.pop(kind == EntityKind::Edge).await?;
                Some(make_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Delete,
                    manifest_version,
                    Some(left),
                    None,
                ))
            }
            (None, Some(right)) => {
                to.pop(kind == EntityKind::Edge).await?;
                Some(make_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Insert,
                    manifest_version,
                    None,
                    Some(right),
                ))
            }
            (Some(left), Some(right)) if left.id < right.id => {
                from.pop(kind == EntityKind::Edge).await?;
                Some(make_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Delete,
                    manifest_version,
                    Some(left),
                    None,
                ))
            }
            (Some(left), Some(right)) if left.id > right.id => {
                to.pop(kind == EntityKind::Edge).await?;
                Some(make_change(
                    table_key,
                    kind,
                    type_name,
                    ChangeOp::Insert,
                    manifest_version,
                    None,
                    Some(right),
                ))
            }
            (Some(left), Some(right)) => {
                from.pop(kind == EntityKind::Edge).await?;
                to.pop(kind == EntityKind::Edge).await?;
                (left.encoded != right.encoded).then(|| {
                    make_change(
                        table_key,
                        kind,
                        type_name,
                        ChangeOp::Update,
                        manifest_version,
                        None,
                        Some(right),
                    )
                })
            }
        };
        if change.is_some() {
            return Ok(change);
        }
    }
}

pub(crate) async fn commit_changes_page(
    store: &TableStore,
    from: &Snapshot,
    to: &Snapshot,
    catalog: Arc<Catalog>,
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
        let mut left = OrderedRows::open(
            store,
            interval.from,
            after_id,
            Arc::clone(&catalog),
            table_key,
        )
        .await?;
        let mut right = OrderedRows::open(
            store,
            interval.to,
            after_id,
            Arc::clone(&catalog),
            table_key,
        )
        .await?;

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
        return Err(OmniError::manifest(
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

fn validate_cursor(
    cursor: CommitChangesCursor,
    graph_identity: &str,
    commit_id: &str,
) -> Result<CommitChangesCursor> {
    if cursor.version != CURSOR_VERSION {
        return Err(OmniError::manifest(
            "unsupported commit changes cursor version",
        ));
    }
    if cursor.graph_identity != cursor_graph_identity(graph_identity)
        || cursor.commit_id != commit_id
    {
        return Err(OmniError::manifest(
            "commit changes cursor does not match this graph and commit",
        ));
    }
    if cursor.table_key.is_empty()
        || cursor.stable_table_id == 0
        || cursor.table_incarnation_id == 0
        || cursor.operation_rank > operation_rank(ChangeOp::Delete)
    {
        return Err(OmniError::manifest(
            "invalid commit changes cursor identity",
        ));
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
        .map_err(|_| OmniError::manifest("invalid commit changes cursor encoding"))?;
    if bytes.len() <= CURSOR_CHECKSUM_BYTES {
        return Err(OmniError::manifest("invalid commit changes cursor"));
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - CURSOR_CHECKSUM_BYTES);
    if Sha256::digest(payload).as_slice() != checksum {
        return Err(OmniError::manifest(
            "invalid commit changes cursor checksum",
        ));
    }
    serde_json::from_slice(payload)
        .map_err(|_| OmniError::manifest("invalid commit changes cursor payload"))
}
