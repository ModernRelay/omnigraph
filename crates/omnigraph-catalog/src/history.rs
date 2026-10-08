//! Immutable history extents for settled graph commits.
//!
//! Each object holds contiguous slots of one history block, or one opaque-ID
//! record, as one Lance `V2_2` file whose columns are, in file order,
//! `tables`, `graph_commit_id` and the packed `record`. The lineage pages
//! therefore sit between the table pages and the footer, so a lineage read
//! fetches one suffix of the object and never the tables.
//!
//! A block the writer knows complete is one object under the name the commit
//! id gives, `blocks/<block>/all.lance`, which a reader GETs with no LIST.
//! Any other run of slots is `blocks/<block>/<start>-<end>.lance`, found by
//! the LIST a reader falls back to when the first name is absent.
//! Objects are capped at 64 MiB: a run that would exceed the cap is split
//! into several range extents, and a head whose record alone is over
//! [`MAX_RECORD_BYTES`] is refused by the publish that would acknowledge it.
//! Conditional creation preserves acknowledged history across duplicate
//! writers.
//!
//! The schema content a commit was written under is one immutable object,
//! `schemas/<sha256 hex>.schema`, named by the SHA-256 of its bytes: the
//! magic `OGSC0001`, three little-endian `u64` lengths, then the
//! `SchemaContractHead` JSON, the source text and the IR text. A commit
//! record names it in `schema_content_hash`.
//!
//! The commits a root held before the offline storage upgrade to stamp 14
//! live under `legacy/`, written once by that upgrade and never again: the
//! records of one writer's consecutive own commits in `legacy/data/<n>.lance`,
//! extents like any other, and under `legacy/locator/` the flat objects that
//! find them, id shards (`OGLI0001`, commit id to file and row), writer
//! shards (`OGLW0001`, one entry per writer with own commits) and the
//! directory (`OGLD0001`) that lists every file and shard with its digest.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat_batches;
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use lance::io::ObjectStore;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::deepsize::DeepSizeOf;
use lance_encoding::decoder::FilterExpression;
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::versions::{reader_projection_from_column_names, v2_2};
use lance_file::writer::{FileWriter, FileWriterOptions};
use lance_io::ReadBatchParams;
use lance_io::object_writer::WriteResult;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::traits::{Reader, Writer};
use object_store::{GetOptions, GetRange, GetResult, path::Path};
use omnigraph_core::graph_commit_id::{HISTORY_BLOCK_SLOTS, parse_history_block_id};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWrite;
use ulid::Ulid;

use crate::TableIdentity;
use crate::error::{OmniError, Result};
use crate::layout::history_uri;
use crate::record::{
    COMMIT_FIELDS, FieldType, PACKED_STRUCT_KEY, RECORD_COLUMN, TABLE_FIELDS, compact_record,
    expand_record, packed_children, packed_record,
};
use crate::row::{CommitColumns, CommitColumnsBuilder, TableColumns, TableColumnsBuilder};
use crate::seams::{decide_seam, fail};
use crate::state::{
    GraphLineageRow, SchemaContractHead, SchemaContractRow, TableRow, TableState, commit_bytes,
    table_bytes,
};

const COMMIT_ID_COLUMN: &str = "graph_commit_id";
const TABLES_COLUMN: &str = "tables";
const EXTENSION: &str = "lance";
const SCHEMAS_DIR: &str = "schemas";
const SCHEMA_EXTENSION: &str = "schema";
const SCHEMA_MAGIC: &[u8; 8] = b"OGSC0001";
const SCHEMA_HEADER_BYTES: usize = 32;
/// The bound of one record in the measure of the release budget, a quarter of
/// the object cap, so that a record alone always encodes under the cap.
pub(crate) const MAX_RECORD_BYTES: usize = MAX_OBJECT_BYTES / 4;
/// The suffix one lineage read fetches: the footer, the column metadata and
/// the lineage pages of any extent whose lineage fits in it.
pub(crate) const TAIL_BYTES: usize = 2 * crate::HISTORY_RELEASE_BYTES;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
const MAX_BLOCK_EXTENTS: usize = 1024;
/// Range extents read per requested slot: the narrowest that covers it and
/// two further copies it is compared with.
const COPIES_READ: usize = 3;
/// What one handle keeps of the immutable objects it has read.
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const FULL_BLOCK_FILE: &str = "all";
/// What a lineage budget is charged per row a read hands back.
const ROW_CHARGE: usize = 512;
const DEFAULT_LINEAGE_BYTES: usize = 32 * 1024 * 1024;
const KEY_BYTES: usize = 512;
/// Lineage columns in file order; `tables` precedes them in the file.
const LINEAGE_COLUMNS: [&str; 2] = [COMMIT_ID_COLUMN, RECORD_COLUMN];
const FILE_COLUMNS: [&str; 3] = [TABLES_COLUMN, COMMIT_ID_COLUMN, RECORD_COLUMN];
const LEGACY_DIR: &str = "legacy";
const LEGACY_DATA_DIR: &str = "data";
const LEGACY_LOCATOR_DIR: &str = "locator";
const LEGACY_IDS_DIR: &str = "ids";
const LEGACY_WRITERS_DIR: &str = "writers";
const LEGACY_DIRECTORY_FILE: &str = "directory";
const LOCATOR_EXTENSION: &str = "oglx";
const ID_SHARD_MAGIC: &[u8; 8] = b"OGLI0001";
const WRITER_SHARD_MAGIC: &[u8; 8] = b"OGLW0001";
const DIRECTORY_MAGIC: &[u8; 8] = b"OGLD0001";
/// The one locator layout this build writes and reads.
const LEGACY_LAYOUT_VERSION: u32 = 1;
/// Record bytes of one legacy data file, so that eight files fit the cache.
const LEGACY_FILE_RECORD_BYTES: usize = CACHE_BYTES / 8;
/// Legacy file names are eight decimal digits.
const LEGACY_MAX_FILES: usize = 100_000_000;
const LEGACY_FILE_DIGITS: usize = 8;
const SHARD_HEADER_BYTES: usize = 12;
const ID_ENTRY_BYTES: usize = 16 + 4 + 4;
/// Id entries of one shard, the most that keep the shard within one lineage read.
const LEGACY_SHARD_ENTRIES: usize = (TAIL_BYTES - SHARD_HEADER_BYTES) / ID_ENTRY_BYTES;
const DIRECTORY_HEADER_BYTES: usize = 8 + 4 + 4 + 16 + 8 + 4 * 4;
const DIRECTORY_FILE_BYTES: usize = 8 + 8 + 4 + 32;
const DIRECTORY_ID_SHARD_BYTES: usize = 16 + 16 + 4 + 32;
const DIRECTORY_WRITER_SHARD_BYTES: usize = 32 + 32 + 4 + 32;
const SHA256_BYTES: usize = 32;

/// One settled graph commit and the `table` rows of its `__manifest` version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRecord {
    pub commit: GraphLineageRow,
    pub tables: Vec<TableRow>,
}

/// A record [`settle_within`] archives, built when its extent is encoded.
pub(crate) trait RecordSource {
    fn commit_row(&self) -> &GraphLineageRow;
    /// [`record_bytes`] of the record, measured without building it.
    fn record_bytes(&self) -> usize;
    fn record(&self) -> HistoryRecord;
}

impl RecordSource for HistoryRecord {
    fn commit_row(&self) -> &GraphLineageRow {
        &self.commit
    }

    fn record_bytes(&self) -> usize {
        record_bytes(&self.commit, &self.tables)
    }

    fn record(&self) -> HistoryRecord {
        self.clone()
    }
}

impl From<crate::state::ManifestRows> for HistoryRecord {
    /// The record of the head commit of one `__manifest` version.
    fn from(rows: crate::state::ManifestRows) -> Self {
        Self {
            commit: rows.head,
            tables: rows.tables,
        }
    }
}

/// The table fields every row holds a value in; every other table field is nullable.
const REQUIRED: [&str; 4] = [
    "location",
    "table_key",
    "stable_table_id",
    "table_incarnation_id",
];

fn plain_field((name, field_type): &(&str, FieldType)) -> Field {
    Field::new(*name, field_type.data_type(), !REQUIRED.contains(name))
}

fn table_fields() -> Fields {
    TABLE_FIELDS.iter().map(plain_field).collect()
}

fn tables_item() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Struct(table_fields()), false))
}

/// The schema of `__history`: a filterable commit id, its packed record, and
/// `tables`, a list of structs holding the fields of a `table` row.
pub fn history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COMMIT_ID_COLUMN, DataType::Utf8, false),
        Field::new(
            RECORD_COLUMN,
            DataType::Struct(packed_children(&COMMIT_FIELDS)),
            false,
        )
        .with_metadata(HashMap::from([(
            PACKED_STRUCT_KEY.to_string(),
            "true".to_string(),
        )])),
        Field::new(TABLES_COLUMN, DataType::List(tables_item()), false),
    ]))
}

fn records_to_batch(records: &[HistoryRecord]) -> Result<RecordBatch> {
    let mut commits = CommitColumnsBuilder::default();
    let mut tables = TableColumnsBuilder::default();
    for record in records {
        commits.push(&record.commit);
        for table in &record.tables {
            tables.push(table)?;
        }
    }
    let tables = StructArray::try_new(table_fields(), tables.finish().to_vec(), None)
        .map_err(OmniError::arrow_internal)?;
    let tables = ListArray::try_new(
        tables_item(),
        OffsetBuffer::from_lengths(records.iter().map(|record| record.tables.len())),
        Arc::new(tables),
        None,
    )
    .map_err(OmniError::arrow_internal)?;

    let commits = compact_record(
        &COMMIT_FIELDS,
        records.len(),
        commits.finish().into_iter().map(Ok),
    )?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            records
                .iter()
                .map(|record| record.commit.graph_commit_id.as_str()),
        )),
        Arc::new(commits),
        Arc::new(tables),
    ];
    RecordBatch::try_new(history_schema(), columns).map_err(OmniError::arrow_internal)
}

fn commits_of(batch: &RecordBatch) -> Result<Vec<GraphLineageRow>> {
    let ids = crate::state::string_column(batch, COMMIT_ID_COLUMN)?;
    let record = batch.column_by_name(RECORD_COLUMN).ok_or_else(|| {
        OmniError::manifest_internal(format!("`__history` batch has no `{RECORD_COLUMN}` column"))
    })?;
    let (record, present) = packed_record(record, batch.num_rows())?;
    let columns = expand_record(&COMMIT_FIELDS, record, present)?;
    let lookup = |name: &str| {
        COMMIT_FIELDS
            .iter()
            .zip(&columns)
            .find_map(|((field, _), column)| (*field == name).then_some(column))
    };
    let commits = CommitColumns::new(&lookup)?;
    (0..batch.num_rows())
        .map(|row| match commits.decode(row, ids.value(row))? {
            (commit, false) => Ok(commit),
            (commit, true) => Err(OmniError::manifest_internal(format!(
                "history record '{}' inherits a schema contract; only a buffered row of \
                 `__manifest` may",
                commit.graph_commit_id
            ))),
        })
        .collect()
}

fn tables_of(batch: &RecordBatch) -> Result<Vec<Vec<TableRow>>> {
    let lists = batch
        .column_by_name(TABLES_COLUMN)
        .and_then(|column| column.as_list_opt::<i32>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "`__history` batch has no `{TABLES_COLUMN}` list column"
            ))
        })?;
    (0..batch.num_rows())
        .map(|row| {
            let tables = lists.value(row);
            let tables = tables.as_struct_opt().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "`__history` `{TABLES_COLUMN}` list does not hold structs"
                ))
            })?;
            let lookup = |name: &str| tables.column_by_name(name);
            let columns = TableColumns::new(&lookup)?;
            (0..tables.len())
                .map(|table| columns.decode(table, None))
                .collect()
        })
        .collect()
}

fn invalid(message: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!("invalid history extent: {message}"))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ExtentKey {
    /// Every commit of a block, from the one that opened it, under the one
    /// name its ids give.
    Full {
        block: Ulid,
    },
    Block {
        block: Ulid,
        start: u16,
        end: u16,
    },
    Singleton(String),
    /// The records of one writer's consecutive own commits from before the
    /// storage upgrade, under the file number the legacy directory lists.
    LegacyData(u32),
}

fn singleton_digest(id: &str) -> String {
    format!("{:x}", Sha256::digest(id.as_bytes()))
}

fn legacy_locator_path(base: &Path) -> Path {
    base.clone().join(LEGACY_DIR).join(LEGACY_LOCATOR_DIR)
}

fn legacy_directory_path(base: &Path) -> Path {
    legacy_locator_path(base).join(format!("{LEGACY_DIRECTORY_FILE}.{LOCATOR_EXTENSION}"))
}

fn legacy_shard_path(base: &Path, dir: &str, shard: u32) -> Path {
    legacy_locator_path(base).join(dir).join(format!(
        "{shard:0width$}.{LOCATOR_EXTENSION}",
        width = LEGACY_FILE_DIGITS
    ))
}

impl ExtentKey {
    fn path(&self, base: &Path) -> Path {
        match self {
            Self::LegacyData(file) => {
                base.clone()
                    .join(LEGACY_DIR)
                    .join(LEGACY_DATA_DIR)
                    .join(format!(
                        "{file:0width$}.{EXTENSION}",
                        width = LEGACY_FILE_DIGITS
                    ))
            }
            Self::Full { block } => base
                .clone()
                .join("blocks")
                .join(block.to_string())
                .join(format!("{FULL_BLOCK_FILE}.{EXTENSION}")),
            Self::Block { block, start, end } => base
                .clone()
                .join("blocks")
                .join(block.to_string())
                .join(format!("{start}-{end}.{EXTENSION}")),
            Self::Singleton(id) => base
                .clone()
                .join("singletons")
                .join(format!("{id}.{EXTENSION}")),
        }
    }

    fn from_path(base: &Path, path: &Path) -> Result<Option<Self>> {
        let prefix = format!("{base}/");
        let relative = path
            .as_ref()
            .strip_prefix(&prefix)
            .ok_or_else(|| invalid("object lies outside history"))?;
        let lance_create_temporary =
            relative
                .rsplit_once(".lance.tmp.")
                .is_some_and(|(_, suffix)| {
                    suffix.len() == 32 && suffix.bytes().all(|c| c.is_ascii_hexdigit())
                });
        if lance_create_temporary {
            return Ok(None);
        }
        let parts: Vec<_> = relative.split('/').collect();
        let key = match parts.as_slice() {
            ["blocks", block, file] => {
                let block =
                    Ulid::from_string(block).map_err(|_| invalid("block token is not a ULID"))?;
                let range = file
                    .strip_suffix(".lance")
                    .ok_or_else(|| invalid("unexpected block object"))?;
                if range == FULL_BLOCK_FILE {
                    Self::Full { block }
                } else {
                    let (start, end) = range
                        .split_once('-')
                        .ok_or_else(|| invalid("missing extent range"))?;
                    let start: u16 = start.parse().map_err(|_| invalid("invalid start slot"))?;
                    let end: u16 = end.parse().map_err(|_| invalid("invalid end slot"))?;
                    if start > end || end >= HISTORY_BLOCK_SLOTS {
                        return Err(invalid("extent slots out of bounds"));
                    }
                    Self::Block { block, start, end }
                }
            }
            ["singletons", file] => {
                let id = file
                    .strip_suffix(".lance")
                    .ok_or_else(|| invalid("unexpected singleton object"))?;
                if id.len() != 64
                    || !id
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                {
                    return Err(invalid("invalid singleton key"));
                }
                Self::Singleton(id.to_string())
            }
            [LEGACY_DIR, LEGACY_DATA_DIR, file] => {
                let number = file
                    .strip_suffix(".lance")
                    .ok_or_else(|| invalid("unexpected legacy data object"))?;
                if number.len() != LEGACY_FILE_DIGITS || !number.bytes().all(|c| c.is_ascii_digit())
                {
                    return Err(invalid("legacy data file name is not eight digits"));
                }
                Self::LegacyData(
                    number
                        .parse()
                        .map_err(|_| invalid("invalid legacy data file number"))?,
                )
            }
            _ => return Err(invalid("unexpected object key")),
        };
        if key.path(base) != *path {
            return Err(invalid("noncanonical object key"));
        }
        Ok(Some(key))
    }

    /// Whether the name admits a commit at one of `slots`; only a range extent names its slots.
    fn covers(&self, slots: &BTreeSet<u16>) -> bool {
        match self {
            Self::Block { start, end, .. } => slots.range(*start..=*end).next().is_some(),
            Self::Full { .. } | Self::Singleton(_) | Self::LegacyData(_) => true,
        }
    }

    fn validate(&self, batch: &RecordBatch) -> Result<()> {
        let ids = crate::state::string_column(batch, COMMIT_ID_COLUMN)?;
        if ids.null_count() != 0 {
            return Err(invalid("null commit ID"));
        }
        let address = match self {
            Self::Full { block } => {
                let first = ids.iter().next().flatten();
                let first = first.map(parse_history_block_id).transpose()?.flatten();
                let first = first.filter(|first| first.nonce == *block).ok_or_else(|| {
                    invalid(
                        "whole-block object does not start the block: its first row is not \
                             the commit whose nonce is the block token",
                    )
                })?;
                Some((block, first.slot, None))
            }
            Self::Block { block, start, end } => Some((block, *start, Some(*end))),
            Self::Singleton(_) | Self::LegacyData(_) => None,
        };
        if let Some((block, start, end)) = address {
            if end.is_some_and(|end| ids.len() != usize::from(end - start + 1)) {
                return Err(invalid("row count differs from extent range"));
            }
            for (offset, id) in ids.iter().enumerate() {
                let parsed = parse_history_block_id(id.ok_or_else(|| invalid("null commit ID"))?)?
                    .ok_or_else(|| invalid("opaque ID in block extent"))?;
                if parsed.block != *block || usize::from(parsed.slot) != usize::from(start) + offset
                {
                    return Err(invalid("commit ID does not match extent address"));
                }
            }
        }
        match self {
            Self::Full { .. } | Self::Block { .. } => {}
            Self::Singleton(expected) => {
                if ids.len() != 1
                    || singleton_digest(ids.value(0)) != *expected
                    || parse_history_block_id(ids.value(0))?.is_some()
                {
                    return Err(invalid("singleton does not match its commit ID"));
                }
            }
            Self::LegacyData(_) => {
                if ids.len() > usize::from(HISTORY_BLOCK_SLOTS) {
                    return Err(invalid("legacy data file holds more rows than a block"));
                }
                let commits = commits_of(batch)?;
                let mut seen = HashSet::new();
                for (row, commit) in commits.iter().enumerate() {
                    if parse_history_block_id(&commit.graph_commit_id)?.is_some() {
                        return Err(invalid("block ID in legacy data file"));
                    }
                    if !seen.insert(commit.graph_commit_id.as_str()) {
                        return Err(invalid("legacy data file repeats a commit ID"));
                    }
                    let Some(previous) = row.checked_sub(1).map(|row| &commits[row]) else {
                        continue;
                    };
                    if commit.native_branch != previous.native_branch {
                        return Err(invalid("legacy data file holds two writers"));
                    }
                    if commit.parent_commit_id.as_deref() != Some(previous.graph_commit_id.as_str())
                    {
                        return Err(invalid(
                            "legacy data file rows are not a first-parent chain",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// The in-memory sink of the Lance file writer: `put_if_absent` takes a whole
/// payload, so the file is assembled here and created in one request.
struct MemWriter {
    sink: Arc<Mutex<Sink>>,
}

/// The bytes of a file being assembled. Past `limit` the bytes are counted
/// and dropped, and the file is reported too large once the writer is done.
struct Sink {
    bytes: Vec<u8>,
    written: usize,
    limit: usize,
}

impl AsyncWrite for MemWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        chunk: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
        sink.written = sink.written.saturating_add(chunk.len());
        if sink.written <= sink.limit {
            sink.bytes.extend_from_slice(chunk);
        }
        Poll::Ready(Ok(chunk.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl Writer for MemWriter {
    async fn tell(&mut self) -> lance_core::Result<usize> {
        Ok(self
            .sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .written)
    }

    async fn shutdown(&mut self) -> lance_core::Result<WriteResult> {
        Ok(WriteResult {
            size: self.tell().await?,
            e_tag: None,
        })
    }
}

/// One fetched byte range of an extent, served to the Lance file reader.
/// A range outside it costs one ranged GET when `store` is set and is an
/// error otherwise.
#[derive(Debug)]
struct ExtentReader {
    path: Path,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    size: usize,
    start: usize,
    bytes: Bytes,
    refetched: Arc<AtomicUsize>,
}

impl DeepSizeOf for ExtentReader {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        self.bytes.len()
    }
}

fn outside(path: &Path, range: &Range<usize>) -> object_store::Error {
    object_store::Error::Generic {
        store: "history extent",
        source: Box::new(std::io::Error::other(format!(
            "range {range:?} lies outside the fetched bytes of {path}"
        ))),
    }
}

impl Reader for ExtentReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn block_size(&self) -> usize {
        self.bytes.len().max(1)
    }

    fn io_parallelism(&self) -> usize {
        1
    }

    fn size(&self) -> BoxFuture<'_, object_store::Result<usize>> {
        futures::future::ready(Ok(self.size)).boxed()
    }

    fn get_range(&self, range: Range<usize>) -> BoxFuture<'static, object_store::Result<Bytes>> {
        if range.start > range.end || range.end > self.size {
            return futures::future::ready(Err(outside(&self.path, &range))).boxed();
        }
        if range.start >= self.start && range.end <= self.start + self.bytes.len() {
            let held = self
                .bytes
                .slice(range.start - self.start..range.end - self.start);
            return futures::future::ready(Ok(held)).boxed();
        }
        let Some(store) = self.store.clone() else {
            return futures::future::ready(Err(outside(&self.path, &range))).boxed();
        };
        let path = self.path.clone();
        let refetched = Arc::clone(&self.refetched);
        async move {
            let wanted = range.end - range.start;
            let options = GetOptions {
                range: Some(GetRange::Bounded(range.start as u64..range.end as u64)),
                ..Default::default()
            };
            let bytes = store.get_opts(&path, options).await?.bytes().await?;
            if bytes.len() != wanted {
                return Err(outside(&path, &range));
            }
            refetched.fetch_add(wanted, Ordering::Relaxed);
            Ok(bytes)
        }
        .boxed()
    }

    fn get_all(&self) -> BoxFuture<'_, object_store::Result<Bytes>> {
        self.get_range(0..self.size)
    }
}

/// The size of a commit and the `table` rows of its record, in the measure of
/// the release budget.
fn record_bytes(commit: &GraphLineageRow, tables: &[TableRow]) -> usize {
    tables
        .iter()
        .map(table_bytes)
        .fold(commit_bytes(commit), usize::saturating_add)
}

/// Refuse `commit` as a head when its record alone is over [`MAX_RECORD_BYTES`]
/// or its commit fields over [`crate::HISTORY_RELEASE_BYTES`]. A publish checks
/// this before it writes: every acknowledged commit fits one history object.
pub(crate) fn check_head_record(commit: &GraphLineageRow, tables: &[TableRow]) -> Result<()> {
    let fields = commit_bytes(commit);
    if fields > crate::HISTORY_RELEASE_BYTES {
        return Err(OmniError::manifest(format!(
            "graph commit '{}' would record {fields} bytes of commit fields, above the \
             {}-byte bound of the commit fields of one history record; the commit is not \
             published",
            commit.graph_commit_id,
            crate::HISTORY_RELEASE_BYTES
        )));
    }
    let bytes = record_bytes(commit, tables);
    if bytes > MAX_RECORD_BYTES {
        return Err(OmniError::manifest(format!(
            "graph commit '{}' would record {bytes} bytes of commit fields and table rows, \
             above the {MAX_RECORD_BYTES}-byte bound of one history record; the commit is \
             not published",
            commit.graph_commit_id
        )));
    }
    Ok(())
}

/// The schema of one extent file: `tables` first, so its pages precede the
/// lineage pages and a suffix of the object holds the lineage without them.
fn file_schema() -> Result<Schema> {
    let logical = history_schema();
    FILE_COLUMNS
        .iter()
        .map(|name| logical.field_with_name(name).cloned())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map(Schema::new)
        .map_err(OmniError::arrow_internal)
}

/// The bytes of the extent file of `records`, or `None` when the file would be
/// over `limit` bytes or, for more than one record, its rows over half of
/// `limit` in memory: a reader bounds what it decodes by the object cap.
async fn encode(records: &[HistoryRecord], limit: usize) -> Result<Option<Vec<u8>>> {
    if records.is_empty() || records.len() > usize::from(HISTORY_BLOCK_SLOTS) {
        return Err(invalid("too many records"));
    }
    let measured = records
        .iter()
        .map(|record| record_bytes(&record.commit, &record.tables))
        .fold(0, usize::saturating_add);
    if records.len() > 1 && measured > limit / 2 {
        return Ok(None);
    }
    let batch = records_to_batch(records)?;
    if records.len() > 1 && batch.get_array_memory_size() > limit / 2 {
        return Ok(None);
    }
    let schema = Arc::new(file_schema()?);
    let columns = FILE_COLUMNS
        .iter()
        .map(|name| batch.column_by_name(name).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid("record batch lacks a history column"))?;
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(OmniError::arrow_internal)?;
    let sink = Arc::new(Mutex::new(Sink {
        bytes: Vec::new(),
        written: 0,
        limit,
    }));
    let writer = Box::new(MemWriter {
        sink: Arc::clone(&sink),
    });
    let schema = LanceSchema::try_from(schema.as_ref()).map_err(invalid)?;
    let mut writer = FileWriter::from(
        v2_2::Writer::try_new(writer, schema, FileWriterOptions::default()).map_err(invalid)?,
    );
    writer.write_batch(&batch).await.map_err(invalid)?;
    writer.finish().await.map_err(invalid)?;
    let mut sink = sink.lock().unwrap_or_else(PoisonError::into_inner);
    Ok((sink.written <= sink.limit).then(|| std::mem::take(&mut sink.bytes)))
}

fn scheduler(store: &Arc<ObjectStore>) -> Arc<ScanScheduler> {
    ScanScheduler::new(
        Arc::clone(store),
        SchedulerConfig::new(2 * MAX_OBJECT_BYTES as u64),
    )
}

const FOOTER_BYTES: usize = 40;
const GLOBAL_BUFFER_COUNT_AT: usize = 24;
const GLOBAL_BUFFER_ENTRY_BYTES: u64 = 16;

/// The Lance file reader allocates one entry per global buffer the footer
/// counts before reading any; a failed allocation aborts the process, which
/// `catch_unwind` cannot stop.
async fn refuse_oversized_global_buffer_count(reader: &ExtentReader) -> Result<()> {
    let footer = reader
        .size
        .checked_sub(FOOTER_BYTES)
        .ok_or_else(|| invalid("object is shorter than a Lance file footer"))?;
    let at = footer + GLOBAL_BUFFER_COUNT_AT;
    let count = reader.get_range(at..at + 4).await.map_err(invalid)?;
    let count = u32::from_le_bytes(
        count
            .as_ref()
            .try_into()
            .map_err(|_| invalid("truncated Lance file footer"))?,
    );
    if u64::from(count) * GLOBAL_BUFFER_ENTRY_BYTES > reader.size as u64 {
        return Err(invalid(format!(
            "global buffer count {count} exceeds what an object of {} bytes can hold",
            reader.size
        )));
    }
    Ok(())
}

/// The rows of one extent file in `__history` column order, without the
/// `tables` column when `lineage_only`.
async fn decode(
    scheduler: &Arc<ScanScheduler>,
    reader: ExtentReader,
    lineage_only: bool,
) -> Result<RecordBatch> {
    let expected = file_schema()?;
    refuse_oversized_global_buffer_count(&reader).await?;
    let read = async {
        let reader = FileReader::try_open(
            scheduler.open_reader(Arc::new(reader)),
            None,
            Default::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .map_err(invalid)?;
        if reader.version() != ConcreteFileVersion::V2_2 {
            return Err(invalid("unexpected Lance file version"));
        }
        let rows = reader.num_rows();
        if rows == 0 || rows > u64::from(HISTORY_BLOCK_SLOTS) {
            return Err(invalid("row count exceeds extent bounds"));
        }
        let stored = Schema::from(reader.schema().as_ref());
        if stored.fields().len() != expected.fields().len()
            || stored
                .fields()
                .iter()
                .zip(expected.fields())
                .any(|(stored, expected)| {
                    stored.name() != expected.name()
                        || stored.data_type() != expected.data_type()
                        || stored.is_nullable() != expected.is_nullable()
                })
        {
            return Err(invalid("unexpected file schema"));
        }
        let columns: &[&str] = if lineage_only {
            &LINEAGE_COLUMNS
        } else {
            &FILE_COLUMNS
        };
        let projection =
            reader_projection_from_column_names(reader.version(), reader.schema(), columns)
                .map_err(invalid)?;
        let batches: Vec<RecordBatch> = reader
            .read_stream_projected(
                ReadBatchParams::RangeFull,
                u32::from(HISTORY_BLOCK_SLOTS),
                1,
                projection,
                FilterExpression::no_filter(),
            )
            .await
            .map_err(invalid)?
            .try_collect()
            .await
            .map_err(invalid)?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| invalid("file holds no batch"))?;
        let batch = concat_batches(&schema, &batches).map_err(OmniError::arrow_internal)?;
        if batch.num_rows() as u64 != rows || batch.get_array_memory_size() > MAX_OBJECT_BYTES {
            return Err(invalid("unexpected batch row count or size"));
        }
        Ok(batch)
    };
    let batch = std::panic::AssertUnwindSafe(read)
        .catch_unwind()
        .await
        .map_err(|_| {
            invalid("the Lance file reader panicked on offsets it read from the object")
        })??;
    let logical = history_schema();
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for field in logical.fields() {
        if lineage_only && field.name() == TABLES_COLUMN {
            continue;
        }
        fields.push(
            schema
                .field_with_name(field.name())
                .map_err(OmniError::arrow_internal)?
                .clone(),
        );
        columns.push(
            batch
                .column_by_name(field.name())
                .ok_or_else(|| invalid("file lacks a history column"))?
                .clone(),
        );
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

async fn store(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
) -> Result<(Arc<ObjectStore>, Path)> {
    let uri = history_uri(root_uri);
    let wrapper = crate::instrumentation::history_wrapper();
    let mut params = crate::storage::lance_store_params_for_uri(&uri)?;
    params.object_store_wrapper = wrapper.clone();
    let (store, path) = ObjectStore::from_uri_and_params(session.store_registry(), &uri, &params)
        .await
        .map_err(OmniError::storage)?;
    if let Some(wrapper) = wrapper {
        crate::instrumentation::record_probed_store(&wrapper, Arc::clone(&store));
    }
    Ok((store, path))
}

struct Fetched {
    bytes: Bytes,
    start: usize,
    size: usize,
    /// Bytes an earlier GET of the same read fetched and these replaced.
    discarded: usize,
}

/// One GET of `range` of an object, or of all of it; `None` when it is absent.
async fn request(
    store: &ObjectStore,
    path: &Path,
    range: Option<GetRange>,
) -> object_store::Result<Option<GetResult>> {
    let options = GetOptions {
        range,
        ..Default::default()
    };
    match store.inner.get_opts(path, options).await {
        Ok(result) => Ok(Some(result)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn storage_error(error: object_store::Error) -> OmniError {
    OmniError::storage(error.into())
}

/// The last `TAIL_BYTES` of an extent when `tail`, else all of it, with no HEAD:
/// a response carries the object size. A store that refuses a suffix range
/// (Azure) is asked for the first `TAIL_BYTES`, then for the tail it sized.
async fn fetch(store: &ObjectStore, path: &Path, tail: bool) -> Result<Option<Fetched>> {
    let limit = TAIL_BYTES as u64;
    let tail_of = move |size: u64| size.saturating_sub(limit)..size;
    if !tail {
        return match request(store, path, None).await.map_err(storage_error)? {
            Some(result) => body(result, |size| 0..size).await.map(Some),
            None => Ok(None),
        };
    }
    let head = match request(store, path, Some(GetRange::Suffix(limit))).await {
        Ok(Some(result)) => return body(result, tail_of).await.map(Some),
        Ok(None) => return Ok(None),
        Err(object_store::Error::NotSupported { .. }) => {
            request(store, path, Some(GetRange::Bounded(0..limit)))
                .await
                .map_err(storage_error)?
        }
        Err(error) => return Err(storage_error(error)),
    };
    let Some(head) = head else {
        return Ok(None);
    };
    let head = body(head, |size| 0..size.min(limit)).await?;
    if head.size <= TAIL_BYTES {
        return Ok(Some(head));
    }
    let range = GetRange::Bounded(tail_of(head.size as u64));
    let result = request(store, path, Some(range))
        .await
        .map_err(storage_error)?
        .ok_or_else(|| invalid("immutable extent disappeared between two reads"))?;
    let fetched = body(result, tail_of).await?;
    if fetched.size != head.size {
        return Err(invalid("immutable extent changed size between two reads"));
    }
    Ok(Some(Fetched {
        discarded: head.bytes.len(),
        ..fetched
    }))
}

/// The body of a response that must cover `wanted` of the size it reports.
async fn body(result: GetResult, wanted: impl FnOnce(u64) -> Range<u64>) -> Result<Fetched> {
    let size = result.meta.size;
    let Range { start, end } = wanted(size);
    if size > MAX_OBJECT_BYTES as u64 || end > size || result.range != (start..end) {
        return Err(invalid(
            "object size or response range exceeds extent bounds",
        ));
    }
    let wanted = usize::try_from(end - start).map_err(|_| invalid("response size overflow"))?;
    let mut bytes = Vec::with_capacity(wanted);
    let mut stream = result.into_stream();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::storage(error.into()))?
    {
        if bytes.len().saturating_add(chunk.len()) > wanted {
            return Err(invalid("oversized response body"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != wanted {
        return Err(invalid("truncated response body"));
    }
    Ok(Fetched {
        bytes: bytes.into(),
        start: start as usize,
        size: size as usize,
        discarded: 0,
    })
}

async fn read_extent(
    store: &Arc<ObjectStore>,
    scheduler: &Arc<ScanScheduler>,
    base: &Path,
    key: &ExtentKey,
    lineage_only: bool,
    budget: Option<&mut usize>,
) -> Result<Option<RecordBatch>> {
    let path = key.path(base);
    if lineage_only && budget.as_deref().is_some_and(|left| *left < TAIL_BYTES) {
        return Err(invalid("selective lineage byte budget exhausted"));
    }
    let Some(fetched) = fetch(store, &path, lineage_only).await? else {
        return Ok(None);
    };
    let held = fetched.bytes.len().saturating_add(fetched.discarded);
    let refetched = Arc::new(AtomicUsize::new(0));
    let reader = ExtentReader {
        path,
        store: lineage_only.then(|| Arc::clone(&store.inner)),
        size: fetched.size,
        start: fetched.start,
        bytes: fetched.bytes,
        refetched: Arc::clone(&refetched),
    };
    let batch = decode(scheduler, reader, lineage_only).await?;
    if let Some(left) = budget {
        let charge = held.saturating_add(refetched.load(Ordering::Relaxed));
        *left = left.checked_sub(charge).ok_or_else(|| {
            invalid(
                "selective lineage byte budget exhausted by the fetched bytes of the extents read",
            )
        })?;
    }
    key.validate(&batch)?;
    Ok(Some(batch))
}

async fn list_extents(
    store: &ObjectStore,
    base: &Path,
    block: Option<(Ulid, &BTreeSet<u16>)>,
) -> Result<Vec<ExtentKey>> {
    let prefix = block.map_or_else(
        || base.clone(),
        |(block, _)| base.clone().join("blocks").join(block.to_string()),
    );
    let schemas = base.clone().join(SCHEMAS_DIR);
    let locator = legacy_locator_path(base);
    let mut stream = store.inner.list(Some(&prefix));
    let mut keys = Vec::new();
    let mut narrowest: BTreeMap<u16, Vec<(u16, ExtentKey)>> = BTreeMap::new();
    let mut temporary = 0;
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::storage(error.into()))?
    {
        if meta.location.prefix_match(&schemas).is_some()
            || meta.location.prefix_match(&locator).is_some()
        {
            continue;
        }
        let Some(key) = ExtentKey::from_path(base, &meta.location)? else {
            temporary += 1;
            if temporary > MAX_BLOCK_EXTENTS {
                return Err(invalid("too many incomplete extent creations"));
            }
            continue;
        };
        if let Some((block, slots)) = block {
            let in_block = matches!(
                &key,
                ExtentKey::Block { block: found, .. } | ExtentKey::Full { block: found }
                    if *found == block
            );
            if !in_block {
                return Err(invalid("block directory holds an object of another block"));
            }
            if !key.covers(slots) {
                continue;
            }
            if let ExtentKey::Block { start, end, .. } = &key {
                let slots_beyond_first = end - start;
                for slot in slots.range(*start..=*end) {
                    let copies = narrowest.entry(*slot).or_default();
                    copies.push((slots_beyond_first, key.clone()));
                    copies.sort();
                    copies.truncate(COPIES_READ);
                }
                continue;
            }
        }
        keys.push(key);
    }
    keys.extend(narrowest.into_values().flatten().map(|(_, key)| key));
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn canonical_record(record: &HistoryRecord) -> Result<HistoryRecord> {
    let mut record = record.clone();
    record
        .tables
        .sort_by_key(|table| table.registration.identity);
    if record
        .tables
        .windows(2)
        .any(|rows| rows[0].registration.identity == rows[1].registration.identity)
    {
        return Err(invalid("duplicate table identity"));
    }
    Ok(record)
}

/// Acknowledgement that every named commit is durable before its buffer is dropped.
#[derive(Debug)]
pub struct Appended {
    graph_commit_ids: HashSet<String>,
}

impl Appended {
    pub fn holds(&self, commit: &GraphLineageRow) -> bool {
        self.graph_commit_ids.contains(&commit.graph_commit_id)
    }
}

/// The blocks a writer knows complete, each with its last slot.
pub(crate) type ClosedBlocks = HashMap<Ulid, u16>;

/// Add the blocks `commits`, a first-parent run oldest first, proves complete:
/// a block whose last commit here is followed by a commit of the same native
/// branch outside it. A block grows only by the same-native child of its
/// last commit, and a native branch has one chain, so nothing joins it later.
pub(crate) fn closed_blocks<'a>(
    commits: impl IntoIterator<Item = &'a GraphLineageRow>,
    closed: &mut ClosedBlocks,
) -> Result<()> {
    let mut previous: Option<&GraphLineageRow> = None;
    for commit in commits {
        if let Some(last) = previous
            && last.native_branch == commit.native_branch
            && let Some(id) = parse_history_block_id(&last.graph_commit_id)?
            && parse_history_block_id(&commit.graph_commit_id)?
                .is_none_or(|next| next.block != id.block)
        {
            closed.insert(id.block, id.slot);
        }
        previous = Some(commit);
    }
    Ok(())
}

/// Archive contiguous committed extents using immutable conditional creates,
/// every run under its slot range.
#[cfg(test)]
pub(crate) async fn settle<R: RecordSource>(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    records: &[R],
) -> Result<Appended> {
    settle_closed(root_uri, session, records, &ClosedBlocks::default()).await
}

/// Archive contiguous committed extents, the run that is the whole of a block
/// in `closed` under the block's one deterministic name. A run whose object
/// would exceed the 64 MiB cap is written as several range extents.
pub(crate) async fn settle_closed<R: RecordSource>(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    records: &[R],
    closed: &ClosedBlocks,
) -> Result<Appended> {
    settle_within(root_uri, session, records, closed, MAX_OBJECT_BYTES).await
}

/// [`settle_closed`] with the object cap as a parameter.
pub(crate) async fn settle_within<R: RecordSource>(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    records: &[R],
    closed: &ClosedBlocks,
    limit: usize,
) -> Result<Appended> {
    let whole = |block: Ulid, end: u16, first: &GraphLineageRow| -> Result<bool> {
        let parent_in_block = match first.parent_commit_id.as_deref() {
            Some(parent) => {
                parse_history_block_id(parent)?.is_some_and(|parent| parent.block == block)
            }
            None => false,
        };
        let opens = !parent_in_block
            && parse_history_block_id(&first.graph_commit_id)?.is_some_and(|id| id.nonce == block);
        Ok(opens && closed.get(&block) == Some(&end))
    };
    let mut blocks: BTreeMap<Ulid, BTreeMap<u16, &R>> = BTreeMap::new();
    let mut singletons = BTreeMap::new();
    for record in records {
        let commit = record.commit_row();
        if let Some(id) = parse_history_block_id(&commit.graph_commit_id)? {
            if let Some(known) = blocks.entry(id.block).or_default().insert(id.slot, record)
                && canonical_record(&known.record())? != canonical_record(&record.record())?
            {
                return Err(copies_differ(&commit.graph_commit_id));
            }
        } else if let Some(known) = singletons.insert(commit.graph_commit_id.as_str(), record)
            && canonical_record(&known.record())? != canonical_record(&record.record())?
        {
            return Err(copies_differ(&commit.graph_commit_id));
        }
    }
    let (store, base) = store(root_uri, session).await?;
    for (block, slots) in blocks {
        let mut pending: Vec<&R> = Vec::new();
        let mut start = 0;
        let mut end = 0;
        for (slot, record) in slots {
            if !pending.is_empty() && slot != end + 1 {
                let whole = whole(block, end, pending[0].commit_row())?;
                write_run(&store, &base, block, start, &pending, whole, limit).await?;
                pending.clear();
            }
            if pending.is_empty() {
                start = slot;
            }
            end = slot;
            pending.push(record);
        }
        if !pending.is_empty() {
            let whole = whole(block, end, pending[0].commit_row())?;
            write_run(&store, &base, block, start, &pending, whole, limit).await?;
        }
    }
    for (id, record) in singletons {
        let key = ExtentKey::Singleton(singleton_digest(id));
        let alone = vec![canonical_record(&record.record())?];
        if !write_extent(&store, &base, &key, alone, limit).await? {
            return Err(over_limit(record.commit_row(), limit));
        }
    }
    Ok(Appended {
        graph_commit_ids: records
            .iter()
            .map(|record| record.commit_row().graph_commit_id.clone())
            .collect(),
    })
}

fn over_limit(commit: &GraphLineageRow, limit: usize) -> OmniError {
    invalid(format!(
        "the record of graph commit '{}' alone exceeds the {limit}-byte object limit",
        commit.graph_commit_id
    ))
}

/// Create the extents of `run`, contiguous slots of `block` from `start`: one
/// object when it fits `limit`, else halves under their slot ranges, halved
/// again until each fits. Only an unsplit run takes the whole-block name.
async fn write_run<R: RecordSource>(
    store: &Arc<ObjectStore>,
    base: &Path,
    block: Ulid,
    start: u16,
    run: &[&R],
    whole: bool,
    limit: usize,
) -> Result<()> {
    let measured: Vec<usize> = run.iter().map(|record| record.record_bytes()).collect();
    let mut parts = vec![(start, 0..run.len(), whole)];
    while let Some((start, part, whole)) = parts.pop() {
        let slots = u16::try_from(part.len()).map_err(|_| invalid("too many records"))?;
        let key = match whole {
            true => ExtentKey::Full { block },
            false => ExtentKey::Block {
                block,
                start,
                end: start + slots - 1,
            },
        };
        let rows_over_half = part.len() > 1
            && measured[part.clone()]
                .iter()
                .fold(0usize, |sum, bytes| sum.saturating_add(*bytes))
                > limit / 2;
        let written = !rows_over_half && {
            let records = run[part.clone()]
                .iter()
                .map(|record| canonical_record(&record.record()))
                .collect::<Result<Vec<_>>>()?;
            write_extent(store, base, &key, records, limit).await?
        };
        if written {
            continue;
        }
        if part.len() == 1 {
            return Err(over_limit(run[part.start].commit_row(), limit));
        }
        let middle = part.start + part.len() / 2;
        parts.push((start + slots / 2, middle..part.end, false));
        parts.push((start, part.start..middle, false));
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// The most `table` rows one [`write_extent`] call has held on this thread.
    pub(crate) static EXTENT_ROWS_HELD: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Create the object of `key` holding the canonical `records`, or accept an equal
/// one that exists. `false` when the object would be over `limit`; nothing is written.
async fn write_extent(
    store: &Arc<ObjectStore>,
    base: &Path,
    key: &ExtentKey,
    records: Vec<HistoryRecord>,
    limit: usize,
) -> Result<bool> {
    #[cfg(test)]
    EXTENT_ROWS_HELD.set(
        EXTENT_ROWS_HELD
            .get()
            .max(records.iter().map(|record| record.tables.len()).sum()),
    );
    let Some(bytes) = encode(&records, limit).await? else {
        return Ok(false);
    };
    if let Err(error) = store.put_if_absent(&key.path(base), bytes.into()).await {
        let Some(existing) = read_extent(store, &scheduler(store), base, key, false, None).await?
        else {
            return Err(OmniError::storage(error.into()));
        };
        if records_of(&existing)? != records {
            return Err(copies_differ(&records[0].commit.graph_commit_id));
        }
    }
    Ok(true)
}

fn unreadable_schema(message: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!("`__history` schema archive: {message}"))
}

fn valid_schema_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn schema_path(base: &Path, digest: &str) -> Path {
    base.clone()
        .join(SCHEMAS_DIR)
        .join(format!("{digest}.{SCHEMA_EXTENSION}"))
}

/// The archive object of `contract`: the magic, the three lengths, then the
/// head JSON, the source and the IR, byte-exact.
fn schema_object(contract: &SchemaContractRow) -> Result<Vec<u8>> {
    let head = serde_json::to_vec(&contract.head).map_err(unreadable_schema)?;
    let parts = [
        head.as_slice(),
        contract.source.as_bytes(),
        contract.ir.as_bytes(),
    ];
    let size = parts
        .iter()
        .map(|part| part.len())
        .fold(SCHEMA_HEADER_BYTES, usize::saturating_add);
    if size > MAX_OBJECT_BYTES {
        return Err(OmniError::manifest(format!(
            "schema content of {size} bytes exceeds the {MAX_OBJECT_BYTES}-byte object limit"
        )));
    }
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(SCHEMA_MAGIC);
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
    }
    for part in parts {
        bytes.extend_from_slice(part);
    }
    Ok(bytes)
}

/// The contract an archive object holds; the object must be its exact encoding.
fn schema_of_object(bytes: &[u8]) -> Result<SchemaContractRow> {
    let malformed = || unreadable_schema("archived schema content is malformed");
    let (header, mut rest) = bytes
        .split_at_checked(SCHEMA_HEADER_BYTES)
        .ok_or_else(malformed)?;
    let (magic, lengths) = header.split_at(SCHEMA_MAGIC.len());
    if magic != SCHEMA_MAGIC {
        return Err(malformed());
    }
    let mut parts = [""; 3];
    for (part, length) in parts.iter_mut().zip(lengths.chunks_exact(8)) {
        let length = u64::from_le_bytes(length.try_into().map_err(|_| malformed())?);
        let length = usize::try_from(length).map_err(|_| malformed())?;
        let (text, after) = rest.split_at_checked(length).ok_or_else(malformed)?;
        *part = std::str::from_utf8(text).map_err(|_| malformed())?;
        rest = after;
    }
    let [head, source, ir] = parts;
    let contract = SchemaContractRow {
        head: serde_json::from_str(head).map_err(|_| malformed())?,
        source: source.to_string(),
        ir: ir.to_string(),
    };
    if !rest.is_empty() || schema_object(&contract)? != bytes {
        return Err(malformed());
    }
    Ok(contract)
}

/// The name of the archived content of `contract`: the SHA-256 of its archive
/// object, which covers the head, the source text and the IR text.
pub fn schema_content_hash(contract: &SchemaContractRow) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(schema_object(contract)?)))
}

/// Archive the content of `contract` under its digest and return the digest.
/// An object already under that name must hold the same bytes.
#[doc(hidden)]
pub async fn archive_schema(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    contract: &SchemaContractRow,
) -> Result<String> {
    let bytes = schema_object(contract)?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let (store, base) = store(root_uri, session).await?;
    let path = schema_path(&base, &digest);
    if !put_immutable(&store, &path, bytes.into()).await? {
        return Err(unreadable_schema(format!(
            "archived schema content '{digest}' differs from the content it is named for"
        )));
    }
    Ok(digest)
}

/// Create the object at `path` holding `bytes`, or accept one that exists and
/// holds the same bytes; `false` when the existing object holds other bytes.
async fn put_immutable(store: &ObjectStore, path: &Path, bytes: Bytes) -> Result<bool> {
    if let Err(error) = store.put_if_absent(path, bytes.clone().into()).await {
        let existing = match request(store, path, None).await.map_err(storage_error)? {
            Some(result) => body(result, |size| 0..size).await?,
            None => return Err(OmniError::storage(error.into())),
        };
        return Ok(existing.bytes == bytes);
    }
    Ok(true)
}

/// The schema content archived under `digest`, for a commit whose accepted
/// identity is `expected`. Absent content, bytes that do not hash to `digest`
/// and content of another identity are errors.
pub async fn read_schema(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    digest: &str,
    expected: &SchemaContractHead,
) -> Result<SchemaContractRow> {
    if !valid_schema_digest(digest) {
        return Err(unreadable_schema(
            "a schema content digest is not lowercase SHA-256 hex",
        ));
    }
    let (store, base) = store(root_uri, session).await?;
    let path = schema_path(&base, digest);
    let fetched = match request(&store, &path, None).await.map_err(storage_error)? {
        Some(result) => body(result, |size| 0..size).await?,
        None => {
            return Err(unreadable_schema(format!(
                "archived schema content '{digest}' is missing"
            )));
        }
    };
    if format!("{:x}", Sha256::digest(&fetched.bytes)) != digest {
        return Err(unreadable_schema(format!(
            "archived schema content '{digest}' does not hash to its name"
        )));
    }
    let contract = schema_of_object(&fetched.bytes)?;
    if contract.head != *expected {
        return Err(unreadable_schema(format!(
            "archived schema content '{digest}' carries IR hash {} (identity v{} of domain {}), \
             the commit {} (identity v{} of domain {})",
            contract.head.schema_ir_hash,
            contract.head.schema_identity_version,
            contract.head.schema_identity_domain,
            expected.schema_ir_hash,
            expected.schema_identity_version,
            expected.schema_identity_domain,
        )));
    }
    Ok(contract)
}

fn legacy_error(message: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!("`__history` legacy locator: {message}"))
}

fn malformed_locator(object: &str) -> OmniError {
    legacy_error(format!("the {object} object is malformed"))
}

fn sha256(bytes: &[u8]) -> [u8; SHA256_BYTES] {
    Sha256::digest(bytes).into()
}

fn ulid_bytes(id: Ulid) -> [u8; 16] {
    id.0.to_be_bytes()
}

fn canonical_ulid(id: &str) -> Result<Ulid> {
    Ulid::from_string(id)
        .ok()
        .filter(|parsed| parsed.to_string() == id)
        .ok_or_else(|| legacy_error(format!("commit ID '{id}' is not a canonical ULID")))
}

fn fits_u32(count: usize, what: &str) -> Result<u32> {
    u32::try_from(count).map_err(|_| legacy_error(format!("{what} count {count} overflows u32")))
}

/// The bytes of one locator object as they are decoded, front to back.
struct LocatorBytes<'a> {
    rest: &'a [u8],
    object: &'static str,
}

impl<'a> LocatorBytes<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let (taken, rest) = self
            .rest
            .split_at_checked(len)
            .ok_or_else(|| malformed_locator(self.object))?;
        self.rest = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| malformed_locator(self.object))
    }

    fn u16(&mut self) -> Result<u16> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64> {
        self.array().map(u64::from_le_bytes)
    }

    fn ulid(&mut self) -> Result<Ulid> {
        self.array().map(|bytes| Ulid(u128::from_be_bytes(bytes)))
    }

    fn text(&mut self) -> Result<String> {
        let len = usize::from(self.u16()?);
        std::str::from_utf8(self.take(len)?)
            .map(str::to_string)
            .map_err(|_| malformed_locator(self.object))
    }

    fn done(&self) -> Result<()> {
        match self.rest.is_empty() {
            true => Ok(()),
            false => Err(malformed_locator(self.object)),
        }
    }
}

fn push_text(bytes: &mut Vec<u8>, text: &str) -> Result<()> {
    let len = u16::try_from(text.len()).map_err(|_| {
        legacy_error(format!(
            "name of {} bytes is over the u16 bound",
            text.len()
        ))
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}

/// Where a legacy commit's record lies: its data file and the row in it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyIdEntry {
    pub id: Ulid,
    pub file: u32,
    pub row: u32,
}

/// One `legacy/locator/ids/<n>.oglx` object: `OGLI0001`, a `u32` count, then
/// entries sorted by id, each the binary ULID, the file and the row.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyIdShard {
    pub entries: Vec<LegacyIdEntry>,
}

impl LegacyIdShard {
    fn check(&self) -> Result<()> {
        if self.entries.is_empty() || self.entries.len() > LEGACY_SHARD_ENTRIES {
            return Err(legacy_error(format!(
                "an id shard holds {} entries, outside 1..={LEGACY_SHARD_ENTRIES}",
                self.entries.len()
            )));
        }
        if self.entries.windows(2).any(|pair| pair[0].id >= pair[1].id) {
            return Err(legacy_error("id shard entries are not strictly ascending"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check()?;
        let mut bytes =
            Vec::with_capacity(SHARD_HEADER_BYTES + self.entries.len() * ID_ENTRY_BYTES);
        bytes.extend_from_slice(ID_SHARD_MAGIC);
        bytes.extend_from_slice(&fits_u32(self.entries.len(), "id entry")?.to_le_bytes());
        for entry in &self.entries {
            bytes.extend_from_slice(&ulid_bytes(entry.id));
            bytes.extend_from_slice(&entry.file.to_le_bytes());
            bytes.extend_from_slice(&entry.row.to_le_bytes());
        }
        Ok(bytes)
    }

    /// The shard an object holds; the object must be its exact encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = LocatorBytes {
            rest: bytes,
            object: "id shard",
        };
        if cursor.take(ID_SHARD_MAGIC.len())? != ID_SHARD_MAGIC {
            return Err(malformed_locator("id shard"));
        }
        let count = cursor.u32()? as usize;
        if count > LEGACY_SHARD_ENTRIES {
            return Err(malformed_locator("id shard"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(LegacyIdEntry {
                id: cursor.ulid()?,
                file: cursor.u32()?,
                row: cursor.u32()?,
            });
        }
        cursor.done()?;
        let shard = Self { entries };
        if shard.encode()? != bytes {
            return Err(malformed_locator("id shard"));
        }
        Ok(shard)
    }

    /// The file and row of `id`, when the shard lists it.
    pub fn find(&self, id: Ulid) -> Option<(u32, u32)> {
        self.entries
            .binary_search_by_key(&id, |entry| entry.id)
            .ok()
            .map(|index| (self.entries[index].file, self.entries[index].row))
    }
}

/// What kind of ref wrote a run of legacy commits; the order is the stored byte.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LegacyWriterKind {
    Main,
    Live,
    Retired,
    Orphaned,
}

impl LegacyWriterKind {
    fn from_byte(byte: u8) -> Option<Self> {
        [Self::Main, Self::Live, Self::Retired, Self::Orphaned]
            .into_iter()
            .find(|kind| *kind as u8 == byte)
    }
}

/// One writer with own legacy commits: its native name (`None` on main), the
/// native name it forked from (`None` for main), the version it forked at, its
/// pinned head version, its head commit and the data files holding its chain.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyWriter {
    pub kind: LegacyWriterKind,
    pub native: Option<String>,
    pub parent: Option<String>,
    pub parent_version: u64,
    pub head_version: u64,
    pub head: Ulid,
    pub first_file: u32,
    pub files: u32,
}

impl LegacyWriter {
    /// The SHA-256 of the native name, the sort key of writer shards.
    pub fn digest(&self) -> [u8; SHA256_BYTES] {
        sha256(self.native.as_deref().unwrap_or_default().as_bytes())
    }

    fn check(&self) -> Result<()> {
        let main = self.kind == LegacyWriterKind::Main;
        if main != self.native.is_none() {
            return Err(legacy_error(
                "a writer is main exactly when it has no native name",
            ));
        }
        if main && (self.parent.is_some() || self.parent_version != 0) {
            return Err(legacy_error("main has no parent"));
        }
        if self.native.as_deref() == Some("") || self.parent.as_deref() == Some("") {
            return Err(legacy_error("a native name is never empty"));
        }
        if self.files == 0 {
            return Err(legacy_error("a writer lists no data file"));
        }
        Ok(())
    }

    fn encode_into(&self, bytes: &mut Vec<u8>) -> Result<()> {
        self.check()?;
        bytes.extend_from_slice(&self.digest());
        bytes.push(self.kind as u8);
        push_text(bytes, self.native.as_deref().unwrap_or_default())?;
        push_text(bytes, self.parent.as_deref().unwrap_or_default())?;
        bytes.extend_from_slice(&self.parent_version.to_le_bytes());
        bytes.extend_from_slice(&self.head_version.to_le_bytes());
        bytes.extend_from_slice(&ulid_bytes(self.head));
        bytes.extend_from_slice(&self.first_file.to_le_bytes());
        bytes.extend_from_slice(&self.files.to_le_bytes());
        Ok(())
    }

    fn decode_from(cursor: &mut LocatorBytes<'_>) -> Result<Self> {
        let _digest: [u8; SHA256_BYTES] = cursor.array()?;
        let kind = LegacyWriterKind::from_byte(cursor.array::<1>()?[0])
            .ok_or_else(|| malformed_locator("writer shard"))?;
        let native = Some(cursor.text()?).filter(|name| !name.is_empty());
        let parent = Some(cursor.text()?).filter(|name| !name.is_empty());
        Ok(Self {
            kind,
            native,
            parent,
            parent_version: cursor.u64()?,
            head_version: cursor.u64()?,
            head: cursor.ulid()?,
            first_file: cursor.u32()?,
            files: cursor.u32()?,
        })
    }
}

/// One `legacy/locator/writers/<n>.oglx` object: `OGLW0001`, a `u32` count,
/// then entries sorted by `(digest, kind)`.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyWriterShard {
    pub entries: Vec<LegacyWriter>,
}

impl LegacyWriterShard {
    fn check(&self) -> Result<()> {
        if self.entries.is_empty() {
            return Err(legacy_error("a writer shard holds no entry"));
        }
        let key = |writer: &LegacyWriter| (writer.digest(), writer.kind);
        if self
            .entries
            .windows(2)
            .any(|pair| key(&pair[0]) >= key(&pair[1]))
        {
            return Err(legacy_error(
                "writer shard entries are not strictly ascending by digest and kind",
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WRITER_SHARD_MAGIC);
        bytes.extend_from_slice(&fits_u32(self.entries.len(), "writer entry")?.to_le_bytes());
        for writer in &self.entries {
            writer.encode_into(&mut bytes)?;
        }
        if bytes.len() > TAIL_BYTES {
            return Err(legacy_error(format!(
                "a writer shard of {} bytes is over the {TAIL_BYTES}-byte bound",
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    /// The shard an object holds; the object must be its exact encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > TAIL_BYTES {
            return Err(malformed_locator("writer shard"));
        }
        let mut cursor = LocatorBytes {
            rest: bytes,
            object: "writer shard",
        };
        if cursor.take(WRITER_SHARD_MAGIC.len())? != WRITER_SHARD_MAGIC {
            return Err(malformed_locator("writer shard"));
        }
        let count = cursor.u32()? as usize;
        if count > bytes.len() {
            return Err(malformed_locator("writer shard"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(LegacyWriter::decode_from(&mut cursor)?);
        }
        cursor.done()?;
        let shard = Self { entries };
        if shard.encode()? != bytes {
            return Err(malformed_locator("writer shard"));
        }
        Ok(shard)
    }

    /// The writers whose native name is `native`, in kind order.
    pub fn find<'a>(
        &'a self,
        native: Option<&str>,
    ) -> impl Iterator<Item = &'a LegacyWriter> + use<'a> {
        let digest = sha256(native.unwrap_or_default().as_bytes());
        let first = self
            .entries
            .partition_point(|writer| writer.digest() < digest);
        self.entries[first..]
            .iter()
            .take_while(move |writer| writer.digest() == digest)
    }
}

/// One legacy data file as the directory lists it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyFile {
    pub first_version: u64,
    pub last_version: u64,
    pub rows: u32,
    /// [`legacy_records_sha256`] of the file's records.
    pub records_sha256: [u8; SHA256_BYTES],
}

/// One id shard as the directory lists it: its first and last id, its entry
/// count and the SHA-256 of its bytes.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyIdShardRef {
    pub first: Ulid,
    pub last: Ulid,
    pub entries: u32,
    pub sha256: [u8; SHA256_BYTES],
}

/// One writer shard as the directory lists it: its first and last writer
/// digest, its entry count and the SHA-256 of its bytes.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyWriterShardRef {
    pub first: [u8; SHA256_BYTES],
    pub last: [u8; SHA256_BYTES],
    pub entries: u32,
    pub sha256: [u8; SHA256_BYTES],
}

/// The `legacy/locator/directory.oglx` object: `OGLD0001`, the layout version,
/// the source stamp, the attempt, the commit count, the four table lengths,
/// the file table, the id-shard table, the writer-shard table, the sorted
/// absent merged parents, then the SHA-256 of everything before it.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyDirectory {
    pub layout: u32,
    pub source_stamp: u32,
    pub attempt: Ulid,
    pub commits: u64,
    pub files: Vec<LegacyFile>,
    pub id_shards: Vec<LegacyIdShardRef>,
    pub writer_shards: Vec<LegacyWriterShardRef>,
    pub absent_parents: Vec<Ulid>,
}

impl LegacyDirectory {
    fn check(&self) -> Result<()> {
        if self.layout != LEGACY_LAYOUT_VERSION {
            return Err(legacy_error(format!(
                "layout version {} is not known to this build, which reads layout \
                 {LEGACY_LAYOUT_VERSION}",
                self.layout
            )));
        }
        if self.files.len() > LEGACY_MAX_FILES {
            return Err(legacy_error("more data files than eight digits name"));
        }
        let mut rows = 0u64;
        for file in &self.files {
            if file.rows == 0 || file.rows > u32::from(HISTORY_BLOCK_SLOTS) {
                return Err(legacy_error("a data file row count is outside a block"));
            }
            if file.first_version > file.last_version {
                return Err(legacy_error("a data file version range is reversed"));
            }
            rows = rows.saturating_add(u64::from(file.rows));
        }
        let listed = self.id_shards.iter().fold(0u64, |sum, shard| {
            sum.saturating_add(u64::from(shard.entries))
        });
        if rows != self.commits || listed != self.commits {
            return Err(legacy_error(
                "the commit count, the file rows and the id entries disagree",
            ));
        }
        let ids_ascend = self.id_shards.iter().all(|shard| {
            shard.entries > 0
                && shard.first <= shard.last
                && (shard.entries > 1 || shard.first == shard.last)
        }) && self
            .id_shards
            .windows(2)
            .all(|pair| pair[0].last < pair[1].first);
        if !ids_ascend {
            return Err(legacy_error(
                "id shard ranges are not ascending and disjoint",
            ));
        }
        let writers_ascend = self
            .writer_shards
            .iter()
            .all(|shard| shard.entries > 0 && shard.first <= shard.last)
            && self
                .writer_shards
                .windows(2)
                .all(|pair| pair[0].last <= pair[1].first);
        if !writers_ascend {
            return Err(legacy_error("writer shard ranges are not ascending"));
        }
        if self
            .absent_parents
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(legacy_error("absent parents are not strictly ascending"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check()?;
        let size = DIRECTORY_HEADER_BYTES
            + self.files.len() * DIRECTORY_FILE_BYTES
            + self.id_shards.len() * DIRECTORY_ID_SHARD_BYTES
            + self.writer_shards.len() * DIRECTORY_WRITER_SHARD_BYTES
            + self.absent_parents.len() * 16
            + SHA256_BYTES;
        if size > TAIL_BYTES {
            return Err(OmniError::resource_limit(
                "bytes of the `__history` legacy directory",
                TAIL_BYTES as u64,
                size as u64,
            ));
        }
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(DIRECTORY_MAGIC);
        bytes.extend_from_slice(&self.layout.to_le_bytes());
        bytes.extend_from_slice(&self.source_stamp.to_le_bytes());
        bytes.extend_from_slice(&ulid_bytes(self.attempt));
        bytes.extend_from_slice(&self.commits.to_le_bytes());
        bytes.extend_from_slice(&fits_u32(self.files.len(), "data file")?.to_le_bytes());
        bytes.extend_from_slice(&fits_u32(self.id_shards.len(), "id shard")?.to_le_bytes());
        bytes.extend_from_slice(&fits_u32(self.writer_shards.len(), "writer shard")?.to_le_bytes());
        bytes.extend_from_slice(
            &fits_u32(self.absent_parents.len(), "absent parent")?.to_le_bytes(),
        );
        for file in &self.files {
            bytes.extend_from_slice(&file.first_version.to_le_bytes());
            bytes.extend_from_slice(&file.last_version.to_le_bytes());
            bytes.extend_from_slice(&file.rows.to_le_bytes());
            bytes.extend_from_slice(&file.records_sha256);
        }
        for shard in &self.id_shards {
            bytes.extend_from_slice(&ulid_bytes(shard.first));
            bytes.extend_from_slice(&ulid_bytes(shard.last));
            bytes.extend_from_slice(&shard.entries.to_le_bytes());
            bytes.extend_from_slice(&shard.sha256);
        }
        for shard in &self.writer_shards {
            bytes.extend_from_slice(&shard.first);
            bytes.extend_from_slice(&shard.last);
            bytes.extend_from_slice(&shard.entries.to_le_bytes());
            bytes.extend_from_slice(&shard.sha256);
        }
        for parent in &self.absent_parents {
            bytes.extend_from_slice(&ulid_bytes(*parent));
        }
        let trailer = sha256(&bytes);
        bytes.extend_from_slice(&trailer);
        assert_eq!(
            bytes.len(),
            size,
            "directory size is computed from its tables"
        );
        Ok(bytes)
    }

    /// The directory an object holds; the object must be its exact encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let malformed = || malformed_locator("directory");
        if bytes.len() > TAIL_BYTES || bytes.len() < DIRECTORY_HEADER_BYTES + SHA256_BYTES {
            return Err(malformed());
        }
        let (body, trailer) = bytes.split_at(bytes.len() - SHA256_BYTES);
        if !body.starts_with(DIRECTORY_MAGIC) || sha256(body) != trailer {
            return Err(malformed());
        }
        let mut cursor = LocatorBytes {
            rest: &body[DIRECTORY_MAGIC.len()..],
            object: "directory",
        };
        let layout = cursor.u32()?;
        if layout != LEGACY_LAYOUT_VERSION {
            return Err(legacy_error(format!(
                "layout version {layout} is not known to this build, which reads layout \
                 {LEGACY_LAYOUT_VERSION}"
            )));
        }
        let source_stamp = cursor.u32()?;
        let attempt = cursor.ulid()?;
        let commits = cursor.u64()?;
        let counts = [cursor.u32()?, cursor.u32()?, cursor.u32()?, cursor.u32()?];
        if counts.iter().any(|count| *count as usize > bytes.len()) {
            return Err(malformed());
        }
        let [files, id_shards, writer_shards, absent] = counts.map(|count| count as usize);
        let mut directory = Self {
            layout,
            source_stamp,
            attempt,
            commits,
            files: Vec::with_capacity(files),
            id_shards: Vec::with_capacity(id_shards),
            writer_shards: Vec::with_capacity(writer_shards),
            absent_parents: Vec::with_capacity(absent),
        };
        for _ in 0..files {
            directory.files.push(LegacyFile {
                first_version: cursor.u64()?,
                last_version: cursor.u64()?,
                rows: cursor.u32()?,
                records_sha256: cursor.array()?,
            });
        }
        for _ in 0..id_shards {
            directory.id_shards.push(LegacyIdShardRef {
                first: cursor.ulid()?,
                last: cursor.ulid()?,
                entries: cursor.u32()?,
                sha256: cursor.array()?,
            });
        }
        for _ in 0..writer_shards {
            directory.writer_shards.push(LegacyWriterShardRef {
                first: cursor.array()?,
                last: cursor.array()?,
                entries: cursor.u32()?,
                sha256: cursor.array()?,
            });
        }
        for _ in 0..absent {
            directory.absent_parents.push(cursor.ulid()?);
        }
        cursor.done()?;
        if directory.encode()? != bytes {
            return Err(malformed());
        }
        Ok(directory)
    }

    /// The id shard that can list `id`, by the shard table.
    pub fn id_shard_of(&self, id: Ulid) -> Option<u32> {
        let index = self.id_shards.partition_point(|shard| shard.last < id);
        let shard = self.id_shards.get(index)?;
        (shard.first <= id).then_some(index as u32)
    }

    /// Whether the upgrade found `id` named as a merged parent and held by no
    /// ref, so no reader will find its commit.
    pub fn lists_absent(&self, id: &str) -> bool {
        canonical_ulid(id).is_ok_and(|id| self.absent_parents.binary_search(&id).is_ok())
    }

    /// The writer shards that can hold `native`, by the shard table.
    pub fn writer_shards_of(&self, native: Option<&str>) -> Range<u32> {
        let digest = sha256(native.unwrap_or_default().as_bytes());
        let first = self
            .writer_shards
            .partition_point(|shard| shard.last < digest);
        let end = self
            .writer_shards
            .partition_point(|shard| shard.first <= digest);
        first as u32..end.max(first) as u32
    }
}

#[derive(serde::Serialize)]
enum DigestedState<'a> {
    Registered,
    Pinned {
        table_version: u64,
        table_branch: Option<&'a str>,
        row_count: u64,
        metadata: String,
        manifest_version: u64,
    },
    Dropped {
        dropped_at: u64,
        sealed_version: u64,
    },
}

#[derive(serde::Serialize)]
struct DigestedTable<'a> {
    identity: TableIdentity,
    table_key: &'a str,
    table_path: &'a str,
    state: DigestedState<'a>,
}

#[derive(serde::Serialize)]
struct DigestedRecord<'a> {
    commit: &'a GraphLineageRow,
    tables: Vec<DigestedTable<'a>>,
}

/// The SHA-256 the directory lists for a data file: over the JSON of the
/// file's canonical records in field order, never over Lance bytes.
#[doc(hidden)]
pub fn legacy_records_sha256(records: &[HistoryRecord]) -> Result<[u8; SHA256_BYTES]> {
    let canonical = records
        .iter()
        .map(canonical_record)
        .collect::<Result<Vec<_>>>()?;
    let mut digested = Vec::with_capacity(canonical.len());
    for record in &canonical {
        let mut tables = Vec::with_capacity(record.tables.len());
        for table in &record.tables {
            let state = match &table.state {
                TableState::Registered => DigestedState::Registered,
                TableState::Pinned(pin) => DigestedState::Pinned {
                    table_version: pin.table_version,
                    table_branch: pin.table_branch.as_deref(),
                    row_count: pin.row_count,
                    metadata: pin.metadata.to_json_string()?,
                    manifest_version: pin.manifest_version,
                },
                TableState::Dropped {
                    dropped_at,
                    sealed_version,
                } => DigestedState::Dropped {
                    dropped_at: *dropped_at,
                    sealed_version: *sealed_version,
                },
            };
            tables.push(DigestedTable {
                identity: table.registration.identity,
                table_key: &table.registration.table_key,
                table_path: &table.registration.table_path,
                state,
            });
        }
        digested.push(DigestedRecord {
            commit: &record.commit,
            tables,
        });
    }
    let json = serde_json::to_vec(&digested).map_err(legacy_error)?;
    Ok(sha256(&json))
}

/// The bounds the legacy data files and id shards are cut under; the plan is
/// a pure function of the census and of this, so a resume under the layout
/// the intent carries rebuilds the same objects.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyLayout {
    pub version: u32,
    pub release_bytes: u64,
    pub file_record_bytes: u64,
    pub block_slots: u32,
    pub shard_entries: u32,
}

impl LegacyLayout {
    pub const CURRENT: Self = Self {
        version: LEGACY_LAYOUT_VERSION,
        release_bytes: crate::HISTORY_RELEASE_BYTES as u64,
        file_record_bytes: LEGACY_FILE_RECORD_BYTES as u64,
        block_slots: HISTORY_BLOCK_SLOTS as u32,
        shard_entries: LEGACY_SHARD_ENTRIES as u32,
    };

    fn check(&self) -> Result<()> {
        let current = Self::CURRENT;
        if self.version != current.version {
            return Err(legacy_error(format!(
                "layout version {} is not known to this build, which writes layout {}",
                self.version, current.version
            )));
        }
        let within = self.release_bytes <= current.release_bytes
            && self.file_record_bytes <= current.file_record_bytes
            && (1..=current.block_slots).contains(&self.block_slots)
            && (1..=current.shard_entries).contains(&self.shard_entries)
            && self.release_bytes > 0
            && self.file_record_bytes > 0;
        if !within {
            return Err(legacy_error(format!(
                "layout {self:?} exceeds the bounds of layout {current:?}"
            )));
        }
        Ok(())
    }
}

/// The own commits of one writer, oldest first, with what the writer shard
/// records about it. The head is the last record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyChain {
    pub kind: LegacyWriterKind,
    pub native: Option<String>,
    pub parent: Option<String>,
    pub parent_version: u64,
    pub head_version: u64,
    pub records: Vec<HistoryRecord>,
}

/// Every object of `legacy/`, planned in memory: the data files in chain
/// order, the id shards, the writer shards and the directory that lists them.
/// Only [`LegacyObjects::plan`] builds one, so the set is always consistent.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyObjects {
    files: Vec<Vec<HistoryRecord>>,
    id_shards: Vec<LegacyIdShard>,
    writer_shards: Vec<LegacyWriterShard>,
    directory: LegacyDirectory,
}

impl LegacyObjects {
    /// Cut `chains` into data files under `layout`, never across a writer and
    /// never across a break of the first-parent link, then shard the ids and
    /// the writers and list everything in the directory.
    pub fn plan(
        layout: &LegacyLayout,
        source_stamp: u32,
        attempt: Ulid,
        chains: &[LegacyChain],
        absent_parents: &BTreeSet<Ulid>,
    ) -> Result<Self> {
        layout.check()?;
        let release = usize::try_from(layout.release_bytes).map_err(legacy_error)?;
        let file_bytes = usize::try_from(layout.file_record_bytes).map_err(legacy_error)?;
        let slots = layout.block_slots as usize;
        let mut files: Vec<Vec<HistoryRecord>> = Vec::new();
        let mut ids = Vec::new();
        let mut writers = Vec::new();
        for chain in chains {
            let first_file = files.len();
            let mut file: Vec<HistoryRecord> = Vec::new();
            let (mut held_fields, mut held_bytes) = (0usize, 0usize);
            let mut head = None;
            for record in &chain.records {
                let record = canonical_record(record)?;
                let commit = &record.commit;
                if commit.native_branch != chain.native {
                    return Err(legacy_error(format!(
                        "commit '{}' names native branch {:?}, its writer {:?}",
                        commit.graph_commit_id, commit.native_branch, chain.native
                    )));
                }
                let id = canonical_ulid(&commit.graph_commit_id)?;
                let fields = commit_bytes(commit);
                let bytes = record_bytes(commit, &record.tables);
                if fields > release || bytes > MAX_RECORD_BYTES {
                    return Err(legacy_error(format!(
                        "commit '{}' records {fields} bytes of commit fields and {bytes} bytes \
                         in all, over the bounds {release} and {MAX_RECORD_BYTES}",
                        commit.graph_commit_id
                    )));
                }
                let cut = file.last().is_some_and(|previous| {
                    held_fields + fields > release
                        || held_bytes + bytes > file_bytes
                        || file.len() >= slots
                        || commit.parent_commit_id.as_deref()
                            != Some(previous.commit.graph_commit_id.as_str())
                });
                if cut {
                    files.push(std::mem::take(&mut file));
                    (held_fields, held_bytes) = (0, 0);
                }
                ids.push(LegacyIdEntry {
                    id,
                    file: fits_u32(files.len(), "data file")?,
                    row: fits_u32(file.len(), "data file row")?,
                });
                held_fields += fields;
                held_bytes += bytes;
                head = Some(id);
                file.push(record);
            }
            let Some(head) = head else {
                return Err(legacy_error(format!(
                    "writer {:?} has no own commit",
                    chain.native
                )));
            };
            files.push(file);
            writers.push(LegacyWriter {
                kind: chain.kind,
                native: chain.native.clone(),
                parent: chain.parent.clone(),
                parent_version: chain.parent_version,
                head_version: chain.head_version,
                head,
                first_file: fits_u32(first_file, "data file")?,
                files: fits_u32(files.len() - first_file, "data file")?,
            });
        }
        if files.len() > LEGACY_MAX_FILES {
            return Err(legacy_error("more data files than eight digits name"));
        }

        ids.sort_by_key(|entry| entry.id);
        if let Some(pair) = ids.windows(2).find(|pair| pair[0].id == pair[1].id) {
            return Err(legacy_error(format!(
                "commit '{}' is written by two chains",
                pair[0].id
            )));
        }
        let id_shards: Vec<LegacyIdShard> = ids
            .chunks(layout.shard_entries as usize)
            .map(|entries| LegacyIdShard {
                entries: entries.to_vec(),
            })
            .collect();

        writers.sort_by_key(|writer| (writer.digest(), writer.kind));
        let mut writer_shards: Vec<LegacyWriterShard> = Vec::new();
        let mut held = 0usize;
        for writer in writers {
            let mut entry = Vec::new();
            writer.encode_into(&mut entry)?;
            match writer_shards.last_mut() {
                Some(shard) if held + entry.len() <= TAIL_BYTES => shard.entries.push(writer),
                _ => {
                    held = SHARD_HEADER_BYTES;
                    writer_shards.push(LegacyWriterShard {
                        entries: vec![writer],
                    });
                }
            }
            held += entry.len();
        }

        let mut directory = LegacyDirectory {
            layout: layout.version,
            source_stamp,
            attempt,
            commits: ids.len() as u64,
            files: Vec::with_capacity(files.len()),
            id_shards: Vec::with_capacity(id_shards.len()),
            writer_shards: Vec::with_capacity(writer_shards.len()),
            absent_parents: absent_parents.iter().copied().collect(),
        };
        for records in &files {
            let (first, last) = (&records[0].commit, &records[records.len() - 1].commit);
            directory.files.push(LegacyFile {
                first_version: first.graph_manifest_version,
                last_version: last.graph_manifest_version,
                rows: fits_u32(records.len(), "data file row")?,
                records_sha256: legacy_records_sha256(records)?,
            });
        }
        for shard in &id_shards {
            directory.id_shards.push(LegacyIdShardRef {
                first: shard.entries[0].id,
                last: shard.entries[shard.entries.len() - 1].id,
                entries: fits_u32(shard.entries.len(), "id entry")?,
                sha256: sha256(&shard.encode()?),
            });
        }
        for shard in &writer_shards {
            directory.writer_shards.push(LegacyWriterShardRef {
                first: shard.entries[0].digest(),
                last: shard.entries[shard.entries.len() - 1].digest(),
                entries: fits_u32(shard.entries.len(), "writer entry")?,
                sha256: sha256(&shard.encode()?),
            });
        }
        directory.encode()?;
        Ok(Self {
            files,
            id_shards,
            writer_shards,
            directory,
        })
    }

    pub fn files(&self) -> &[Vec<HistoryRecord>] {
        &self.files
    }

    pub fn id_shards(&self) -> &[LegacyIdShard] {
        &self.id_shards
    }

    pub fn writer_shards(&self) -> &[LegacyWriterShard] {
        &self.writer_shards
    }

    pub fn directory(&self) -> &LegacyDirectory {
        &self.directory
    }
}

decide_seam! {
    /// After each data file, id shard and writer shard of `legacy/` is
    /// created and before the next object: the directory does not exist yet,
    /// so a retry runs the census again and creates the rest.
    pub static UPGRADE_BETWEEN_LEGACY_FILES = ("upgrade.between_legacy_files", Unreachable, [Fail]);
}

/// Create every object of `objects` under `legacy/`: the data files first,
/// then the id shards, the writer shards and the directory last, so a
/// directory that exists lists objects that exist. An object that exists
/// must hold the same bytes, so a resumed write is a no-op.
#[doc(hidden)]
pub async fn write_legacy(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    objects: &LegacyObjects,
) -> Result<()> {
    let (store, base) = store(root_uri, session).await?;
    for (file, records) in objects.files.iter().enumerate() {
        let key = ExtentKey::LegacyData(fits_u32(file, "data file")?);
        if !write_extent(&store, &base, &key, records.clone(), MAX_OBJECT_BYTES).await? {
            return Err(over_limit(&records[0].commit, MAX_OBJECT_BYTES));
        }
        fail(&UPGRADE_BETWEEN_LEGACY_FILES)?;
    }
    let differs = |path: &Path| {
        legacy_error(format!(
            "the object at {path} exists and holds other bytes; the upgrade that wrote it \
             planned a different legacy set"
        ))
    };
    for (shard, object) in objects.id_shards.iter().enumerate() {
        let path = legacy_shard_path(&base, LEGACY_IDS_DIR, fits_u32(shard, "id shard")?);
        if !put_immutable(&store, &path, object.encode()?.into()).await? {
            return Err(differs(&path));
        }
        fail(&UPGRADE_BETWEEN_LEGACY_FILES)?;
    }
    for (shard, object) in objects.writer_shards.iter().enumerate() {
        let path = legacy_shard_path(&base, LEGACY_WRITERS_DIR, fits_u32(shard, "writer shard")?);
        if !put_immutable(&store, &path, object.encode()?.into()).await? {
            return Err(differs(&path));
        }
        fail(&UPGRADE_BETWEEN_LEGACY_FILES)?;
    }
    let path = legacy_directory_path(&base);
    if !put_immutable(&store, &path, objects.directory.encode()?.into()).await? {
        return Err(differs(&path));
    }
    Ok(())
}

/// The rows of one immutable object: its commits, or its complete records.
#[derive(Debug, Clone)]
enum Rows {
    Lineage(Arc<Vec<GraphLineageRow>>),
    Full(Arc<Vec<HistoryRecord>>),
}

impl Rows {
    fn len(&self) -> usize {
        match self {
            Self::Lineage(commits) => commits.len(),
            Self::Full(records) => records.len(),
        }
    }

    fn commit(&self, row: usize) -> Option<&GraphLineageRow> {
        match self {
            Self::Lineage(commits) => commits.get(row),
            Self::Full(records) => records.get(row).map(|record| &record.commit),
        }
    }

    fn commits(&self) -> impl Iterator<Item = &GraphLineageRow> {
        (0..self.len()).filter_map(|row| self.commit(row))
    }

    /// The size the cache bound counts, in the measure of the release budget.
    fn bytes(&self) -> usize {
        let commits = self.commits().map(crate::state::commit_bytes);
        let tables = match self {
            Self::Lineage(_) => 0,
            Self::Full(records) => records
                .iter()
                .flat_map(|record| &record.tables)
                .map(crate::state::table_bytes)
                .fold(0, usize::saturating_add),
        };
        commits.fold(tables, usize::saturating_add)
    }

    /// The rows narrowed to the commits at `slots` of their block.
    fn at_slots(self, slots: &BTreeSet<u16>) -> Result<Self> {
        let mut requested = Vec::new();
        for (row, commit) in self.commits().enumerate() {
            let id = parse_history_block_id(&commit.graph_commit_id)?;
            if id.is_some_and(|id| slots.contains(&id.slot)) {
                requested.push(row);
            }
        }
        if requested.len() == self.len() {
            return Ok(self);
        }
        let requested = requested.into_iter();
        Ok(match &self {
            Self::Lineage(commits) => Self::Lineage(Arc::new(
                requested.map(|row| commits[row].clone()).collect(),
            )),
            Self::Full(records) => Self::Full(Arc::new(
                requested.map(|row| records[row].clone()).collect(),
            )),
        })
    }

    /// The rows at the positions `rows` of the object, each of which it must hold.
    fn at_rows(self, rows: &BTreeSet<u32>) -> Result<Self> {
        let len = self.len();
        if rows.iter().any(|row| *row as usize >= len) {
            return Err(invalid(
                "the legacy locator names a row beyond the rows of the data file",
            ));
        }
        if rows.len() == len {
            return Ok(self);
        }
        let requested = rows.iter().map(|row| *row as usize);
        Ok(match &self {
            Self::Lineage(commits) => Self::Lineage(Arc::new(
                requested.map(|row| commits[row].clone()).collect(),
            )),
            Self::Full(records) => Self::Full(Arc::new(
                requested.map(|row| records[row].clone()).collect(),
            )),
        })
    }
}

/// What one held object decodes to: the rows of an extent, or an id shard or
/// a writer shard of the legacy locator.
#[derive(Clone)]
enum Held {
    Rows(Rows),
    IdShard(Arc<LegacyIdShard>),
    WriterShard(Arc<LegacyWriterShard>),
}

#[derive(Default)]
struct CachedObjects {
    /// Per object path: what it holds, its size, and when it was last used.
    held: HashMap<String, (Held, usize, u64)>,
    bytes: usize,
    clock: u64,
}

impl CachedObjects {
    /// Keep `held` under `path`, least recently used objects leaving until the
    /// bound holds again.
    fn hold(&mut self, path: &Path, held: Held, bytes: usize) {
        self.clock += 1;
        let clock = self.clock;
        if let Some((_, replaced, _)) = self.held.insert(path.to_string(), (held, bytes, clock)) {
            self.bytes -= replaced;
        }
        self.bytes += bytes;
        while self.bytes > CACHE_BYTES {
            let Some(oldest) = self
                .held
                .iter()
                .min_by_key(|(_, (_, _, used))| *used)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            if let Some((_, evicted, _)) = self.held.remove(&oldest) {
                self.bytes -= evicted;
            }
        }
    }
}

/// What one handle knows of `legacy/locator/directory.oglx`: nothing yet, that
/// the root has none, or the directory itself.
#[derive(Clone, Default)]
enum LegacyProbe {
    #[default]
    Unprobed,
    Absent,
    Present(Arc<LegacyDirectory>),
}

/// The immutable `__history` objects one handle has read, by object path. An
/// object is created once, never replaced and never deleted, so its rows are
/// served again with no request. An absent object and a LIST are never kept,
/// since absence can end, with one exception: whether
/// `legacy/locator/directory.oglx` exists is kept, because the offline storage
/// upgrade creates it before the root can be opened and never again, and a
/// root restore calls [`Self::clear`]. Bounded at 16 MiB, least recently used
/// first out.
#[derive(Clone, Default)]
pub struct ExtentCache {
    objects: Arc<Mutex<CachedObjects>>,
    legacy: Arc<Mutex<LegacyProbe>>,
}

impl ExtentCache {
    /// The held rows of `path`; complete records also serve a lineage read.
    fn get(&self, path: &Path, lineage_only: bool) -> Option<Rows> {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.clock += 1;
        let clock = objects.clock;
        let (held, _, used) = objects.held.get_mut(path.as_ref())?;
        let Held::Rows(rows) = held else {
            return None;
        };
        if !lineage_only && matches!(rows, Rows::Lineage(_)) {
            return None;
        }
        *used = clock;
        Some(rows.clone())
    }

    /// Keep `rows`; complete records replace the lineage of the same object.
    fn insert(&self, path: &Path, rows: &Rows) {
        let bytes = rows.bytes();
        if bytes > CACHE_BYTES {
            return;
        }
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(rows, Rows::Lineage(_))
            && matches!(
                objects.held.get(path.as_ref()),
                Some((Held::Rows(Rows::Full(_)), ..))
            )
        {
            return;
        }
        objects.hold(path, Held::Rows(rows.clone()), bytes);
    }

    /// The held id shard at `path`.
    fn id_shard(&self, path: &Path) -> Option<Arc<LegacyIdShard>> {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.clock += 1;
        let clock = objects.clock;
        let (held, _, used) = objects.held.get_mut(path.as_ref())?;
        let Held::IdShard(shard) = held else {
            return None;
        };
        *used = clock;
        Some(Arc::clone(shard))
    }

    /// Keep the id shard at `path`, whose object is `bytes` long.
    fn insert_id_shard(&self, path: &Path, shard: &Arc<LegacyIdShard>, bytes: usize) {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.hold(path, Held::IdShard(Arc::clone(shard)), bytes);
    }

    /// The held writer shard at `path`.
    fn writer_shard(&self, path: &Path) -> Option<Arc<LegacyWriterShard>> {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.clock += 1;
        let clock = objects.clock;
        let (held, _, used) = objects.held.get_mut(path.as_ref())?;
        let Held::WriterShard(shard) = held else {
            return None;
        };
        *used = clock;
        Some(Arc::clone(shard))
    }

    /// Keep the writer shard at `path`, whose object is `bytes` long.
    fn insert_writer_shard(&self, path: &Path, shard: &Arc<LegacyWriterShard>, bytes: usize) {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.hold(path, Held::WriterShard(Arc::clone(shard)), bytes);
    }

    fn probe(&self) -> LegacyProbe {
        self.legacy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_probe(&self, probe: LegacyProbe) {
        *self.legacy.lock().unwrap_or_else(PoisonError::into_inner) = probe;
    }

    /// The legacy directory this handle has already read, with no request.
    pub(crate) fn known_legacy_directory(&self) -> Option<Arc<LegacyDirectory>> {
        match self.probe() {
            LegacyProbe::Present(directory) => Some(directory),
            LegacyProbe::Unprobed | LegacyProbe::Absent => None,
        }
    }

    /// Forget every held object and whether the root has a legacy directory:
    /// the root was replaced, so a path may now name a different object.
    /// Shared by every clone of this cache.
    pub(crate) fn clear(&self) {
        {
            let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
            objects.held.clear();
            objects.bytes = 0;
        }
        self.set_probe(LegacyProbe::Unprobed);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held
            .len()
    }
}

/// Which rows of an object a read hands back and charges: all of them, those
/// at block slots, or those at the row positions the legacy locator names.
#[derive(Clone, Copy)]
enum Narrow<'a> {
    All,
    Slots(&'a BTreeSet<u16>),
    Rows(&'a BTreeSet<u32>),
}

/// What a read without a handle's cache keeps for its own duration: the
/// directory probe and the id and writer shards it decoded.
#[derive(Default)]
struct LegacyMemo {
    probe: LegacyProbe,
    id_shards: HashMap<u32, Arc<LegacyIdShard>>,
    writer_shards: HashMap<u32, Arc<LegacyWriterShard>>,
}

/// One read of addressed or listed extents: the store, the allowance left,
/// and the cache of the handle that reads.
struct Selection<'a> {
    store: Arc<ObjectStore>,
    base: Path,
    scheduler: Arc<ScanScheduler>,
    lineage_only: bool,
    budget: Option<usize>,
    cache: Option<&'a ExtentCache>,
    memo: LegacyMemo,
}

impl Selection<'_> {
    fn charge(&mut self, bytes: usize, exhausted: &str) -> Result<()> {
        if let Some(left) = &mut self.budget {
            *left = left.checked_sub(bytes).ok_or_else(|| invalid(exhausted))?;
        }
        Ok(())
    }

    fn probe(&self) -> LegacyProbe {
        match self.cache {
            Some(cache) => cache.probe(),
            None => self.memo.probe.clone(),
        }
    }

    fn set_probe(&mut self, probe: LegacyProbe) {
        match self.cache {
            Some(cache) => cache.set_probe(probe),
            None => self.memo.probe = probe,
        }
    }

    /// The bytes of one locator object, whole; `None` when it is absent. The
    /// address and every byte fetched are charged.
    async fn locator_object(&mut self, path: &Path) -> Result<Option<Bytes>> {
        self.charge(KEY_BYTES, "selective lineage address budget exhausted")?;
        let Some(result) = request(&self.store, path, None)
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let fetched = body(result, |size| 0..size).await?;
        self.charge(
            fetched.bytes.len(),
            "selective lineage byte budget exhausted by the legacy locator",
        )?;
        Ok(Some(fetched.bytes))
    }

    /// The legacy directory, read once per handle, or per read without a
    /// handle; `None` when the root has none.
    async fn directory(&mut self) -> Result<Option<Arc<LegacyDirectory>>> {
        let probe = match self.probe() {
            LegacyProbe::Unprobed => {
                match self
                    .locator_object(&legacy_directory_path(&self.base))
                    .await?
                {
                    Some(bytes) => LegacyProbe::Present(Arc::new(LegacyDirectory::decode(&bytes)?)),
                    None => LegacyProbe::Absent,
                }
            }
            known => known,
        };
        self.set_probe(probe.clone());
        Ok(match probe {
            LegacyProbe::Present(directory) => Some(directory),
            LegacyProbe::Unprobed | LegacyProbe::Absent => None,
        })
    }

    /// Id shard `shard` of `directory`, held or read and checked against the
    /// digest the directory lists.
    async fn id_shard(
        &mut self,
        directory: &LegacyDirectory,
        shard: u32,
    ) -> Result<Arc<LegacyIdShard>> {
        let path = legacy_shard_path(&self.base, LEGACY_IDS_DIR, shard);
        let held = match self.cache {
            Some(cache) => cache.id_shard(&path),
            None => self.memo.id_shards.get(&shard).cloned(),
        };
        if let Some(held) = held {
            return Ok(held);
        }
        let listed = directory
            .id_shards
            .get(shard as usize)
            .ok_or_else(|| legacy_error(format!("the directory lists no id shard {shard}")))?;
        let bytes = self.locator_object(&path).await?.ok_or_else(|| {
            legacy_error(format!(
                "the directory lists id shard {shard} and the object is absent"
            ))
        })?;
        if sha256(&bytes) != listed.sha256 {
            return Err(legacy_error(format!(
                "id shard {shard} does not hash to the digest the directory lists"
            )));
        }
        let decoded = Arc::new(LegacyIdShard::decode(&bytes)?);
        match self.cache {
            Some(cache) => cache.insert_id_shard(&path, &decoded, bytes.len()),
            None => {
                self.memo.id_shards.insert(shard, Arc::clone(&decoded));
            }
        }
        Ok(decoded)
    }

    /// Writer shard `shard` of `directory`, held or read and checked against
    /// the digest the directory lists.
    async fn writer_shard(
        &mut self,
        directory: &LegacyDirectory,
        shard: u32,
    ) -> Result<Arc<LegacyWriterShard>> {
        let path = legacy_shard_path(&self.base, LEGACY_WRITERS_DIR, shard);
        let held = match self.cache {
            Some(cache) => cache.writer_shard(&path),
            None => self.memo.writer_shards.get(&shard).cloned(),
        };
        if let Some(held) = held {
            return Ok(held);
        }
        let listed = directory
            .writer_shards
            .get(shard as usize)
            .ok_or_else(|| legacy_error(format!("the directory lists no writer shard {shard}")))?;
        let bytes = self.locator_object(&path).await?.ok_or_else(|| {
            legacy_error(format!(
                "the directory lists writer shard {shard} and the object is absent"
            ))
        })?;
        if sha256(&bytes) != listed.sha256 {
            return Err(legacy_error(format!(
                "writer shard {shard} does not hash to the digest the directory lists"
            )));
        }
        let decoded = Arc::new(LegacyWriterShard::decode(&bytes)?);
        match self.cache {
            Some(cache) => cache.insert_writer_shard(&path, &decoded, bytes.len()),
            None => {
                self.memo.writer_shards.insert(shard, Arc::clone(&decoded));
            }
        }
        Ok(decoded)
    }

    /// The data file and row the locator names for `id`; `None` when it lists
    /// no such commit, which every id that is not a canonical ULID is.
    async fn locate(
        &mut self,
        directory: &LegacyDirectory,
        id: &str,
    ) -> Result<Option<(u32, u32)>> {
        let Ok(id) = canonical_ulid(id) else {
            return Ok(None);
        };
        let Some(shard) = directory.id_shard_of(id) else {
            return Ok(None);
        };
        Ok(self.id_shard(directory, shard).await?.find(id))
    }

    /// The singleton object of `id`, which must hold that commit; `None` when
    /// it is absent.
    async fn singleton(&mut self, id: &str) -> Result<Option<Rows>> {
        let key = ExtentKey::Singleton(singleton_digest(id));
        let Some(rows) = self.read(&key, Narrow::All).await? else {
            return Ok(None);
        };
        if rows
            .commit(0)
            .is_none_or(|commit| commit.graph_commit_id != id)
        {
            return Err(invalid(
                "singleton returned a different requested commit ID",
            ));
        }
        Ok(Some(rows))
    }

    /// The rows of `key`, from the cache or one GET, narrowed as `narrow` says;
    /// `None` when it is absent. Only the rows returned are charged as owned.
    async fn read(&mut self, key: &ExtentKey, narrow: Narrow<'_>) -> Result<Option<Rows>> {
        self.charge(KEY_BYTES, "selective lineage address budget exhausted")?;
        let path = key.path(&self.base);
        let cached = self
            .cache
            .and_then(|cache| cache.get(&path, self.lineage_only));
        let rows = match cached {
            Some(rows) => rows,
            None => {
                let Some(batch) = read_extent(
                    &self.store,
                    &self.scheduler,
                    &self.base,
                    key,
                    self.lineage_only,
                    self.budget.as_mut(),
                )
                .await?
                else {
                    return Ok(None);
                };
                let rows = match self.lineage_only {
                    true => Rows::Lineage(Arc::new(commits_of(&batch)?)),
                    false => Rows::Full(Arc::new(records_of(&batch)?)),
                };
                if let Some(cache) = self.cache {
                    cache.insert(&path, &rows);
                }
                rows
            }
        };
        let rows = match narrow {
            Narrow::All => rows,
            Narrow::Slots(slots) => rows.at_slots(slots)?,
            Narrow::Rows(positions) => rows.at_rows(positions)?,
        };
        self.charge(
            rows.len().saturating_mul(ROW_CHARGE),
            "selective lineage byte budget exhausted",
        )?;
        Ok(Some(rows))
    }

    async fn listed(&mut self, key: &ExtentKey, narrow: Narrow<'_>) -> Result<Rows> {
        self.read(key, narrow)
            .await?
            .ok_or_else(|| invalid("listed immutable extent disappeared"))
    }
}

/// The rows of every extent that can hold one of `ids`, or of every extent: an
/// `hb1` id by its block's one object, else the block LIST; any other id by its
/// singleton until one is absent, then by the legacy locator ([`LegacyProbe`]).
async fn read_selected(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    ids: Option<&[&str]>,
    lineage_only: bool,
    budget: Option<usize>,
    cache: Option<&ExtentCache>,
) -> Result<Vec<Rows>> {
    let (store, base) = store(root_uri, session).await?;
    let mut selection = Selection {
        scheduler: scheduler(&store),
        store,
        base,
        lineage_only,
        budget,
        cache,
        memo: LegacyMemo::default(),
    };
    let mut found = Vec::new();
    let Some(ids) = ids else {
        for key in list_extents(&selection.store, &selection.base, None).await? {
            found.push(selection.listed(&key, Narrow::All).await?);
        }
        return Ok(found);
    };
    if budget.is_some_and(|left| ids.len() > left / KEY_BYTES) {
        return Err(invalid("selective lineage address budget exhausted"));
    }
    let mut blocks: BTreeMap<Ulid, Vec<(u16, &str)>> = BTreeMap::new();
    let mut singletons = BTreeSet::new();
    for id in ids {
        match parse_history_block_id(id)? {
            Some(parsed) => blocks
                .entry(parsed.block)
                .or_default()
                .push((parsed.slot, *id)),
            None => {
                singletons.insert(*id);
            }
        }
    }
    for (block, wanted) in blocks {
        let slots: BTreeSet<u16> = wanted.iter().map(|(slot, _)| *slot).collect();
        let requested = |rows: Rows| match lineage_only {
            true => Ok(rows),
            false => rows.at_slots(&slots),
        };
        let full = ExtentKey::Full { block };
        let direct = selection.read(&full, Narrow::All).await?;
        if let Some(rows) = &direct {
            let opener = rows.commit(0).map(|first| first.graph_commit_id.as_str());
            let opener = opener.map(parse_history_block_id).transpose()?.flatten();
            let opener_slot = opener.map_or(0, |opener| opener.slot);
            let holds = |(slot, id): &(u16, &str)| {
                slot.checked_sub(opener_slot)
                    .and_then(|row| rows.commit(usize::from(row)))
                    .is_some_and(|commit| commit.graph_commit_id == *id)
            };
            if wanted.iter().all(holds) {
                found.extend(direct.map(requested).transpose()?);
                continue;
            }
        }
        let covering = Some((block, &slots));
        let covering = list_extents(&selection.store, &selection.base, covering).await?;
        let copies_overlap = covering.len() > 1;
        let kept = match !lineage_only || copies_overlap {
            true => Narrow::Slots(&slots),
            false => Narrow::All,
        };
        for key in covering {
            if key != full || direct.is_none() {
                found.push(selection.listed(&key, kept).await?);
            }
        }
        found.extend(direct.map(requested).transpose()?);
    }
    let mut singletons = singletons.into_iter();
    let mut missed = None;
    if matches!(selection.probe(), LegacyProbe::Unprobed) {
        for id in singletons.by_ref() {
            match selection.singleton(id).await? {
                Some(rows) => found.push(rows),
                None => {
                    missed = Some(id);
                    break;
                }
            }
        }
    }
    let rest: Vec<&str> = singletons.collect();
    if missed.is_none() && rest.is_empty() {
        return Ok(found);
    }
    let Some(directory) = selection.directory().await? else {
        for id in rest {
            found.extend(selection.singleton(id).await?);
        }
        return Ok(found);
    };
    let mut located: BTreeMap<u32, BTreeMap<u32, &str>> = BTreeMap::new();
    let mut unlisted = Vec::new();
    for id in missed.into_iter().chain(rest) {
        match selection.locate(&directory, id).await? {
            Some((file, row)) => {
                if located.entry(file).or_default().insert(row, id).is_some() {
                    return Err(legacy_error(format!(
                        "the locator places two requested commits at row {row} of data file {file}"
                    )));
                }
            }
            None if missed == Some(id) => {}
            None => unlisted.push(id),
        }
    }
    for (file, rows) in located {
        let positions: BTreeSet<u32> = rows.keys().copied().collect();
        let read = selection
            .read(&ExtentKey::LegacyData(file), Narrow::Rows(&positions))
            .await?
            .ok_or_else(|| {
                legacy_error(format!(
                    "the directory lists data file {file} and the object is absent"
                ))
            })?;
        for (row, id) in rows {
            if !read.commits().any(|commit| commit.graph_commit_id == id) {
                return Err(legacy_error(format!(
                    "the locator places commit '{id}' at row {row} of data file {file}, which \
                     holds another commit"
                )));
            }
        }
        found.push(read);
    }
    for id in unlisted {
        found.extend(selection.singleton(id).await?);
    }
    Ok(found)
}

/// The legacy directory of the root, through `cache` when given, so a handle
/// reads it once; `None` when the root has none, which a handle also keeps.
#[doc(hidden)]
pub async fn legacy_directory(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    cache: Option<&ExtentCache>,
) -> Result<Option<Arc<LegacyDirectory>>> {
    let (store, base) = store(root_uri, session).await?;
    let mut selection = Selection {
        scheduler: scheduler(&store),
        store,
        base,
        lineage_only: true,
        budget: None,
        cache,
        memo: LegacyMemo::default(),
    };
    selection.directory().await
}

/// The keys under `__history`, relative to it and sorted, of every object that
/// is not a schema content archive named by its digest, `limit` of them at
/// most. The offline storage upgrade fences a root only when this is empty.
#[doc(hidden)]
pub async fn foreign_history_objects(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    limit: usize,
) -> Result<Vec<String>> {
    let (store, base) = store(root_uri, session).await?;
    let schemas = base.clone().join(SCHEMAS_DIR);
    let prefix = format!("{base}/");
    let mut stream = store.inner.list(Some(&base));
    let mut foreign = BTreeSet::new();
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::storage(error.into()))?
    {
        let archive = meta
            .location
            .prefix_match(&schemas)
            .is_some_and(|mut parts| {
                let name = parts.next();
                parts.next().is_none()
                    && name.is_some_and(|name| {
                        name.as_ref()
                            .strip_suffix(SCHEMA_EXTENSION)
                            .and_then(|stem| stem.strip_suffix('.'))
                            .is_some_and(valid_schema_digest)
                    })
            });
        if !archive {
            foreign.insert(meta.location.as_ref().replacen(&prefix, "", 1));
            if foreign.len() > limit {
                foreign.pop_last();
            }
        }
    }
    Ok(foreign.into_iter().collect())
}

/// Read every object of `legacy/` again, through no cache, and check it
/// against the directory: the data files are exactly the listed ones and hash
/// to their record digests, every id entry names the row holding its commit,
/// and every writer's head is the last record of its files. Returns the
/// directory, `None` when the root has none.
#[doc(hidden)]
pub async fn verify_legacy(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
) -> Result<Option<Arc<LegacyDirectory>>> {
    let Some(mut reader) = LegacyReader::open(root_uri, session, None).await? else {
        return Ok(None);
    };
    let directory = Arc::clone(&reader.directory);
    let found: BTreeSet<u32> = list_extents(&reader.selection.store, &reader.selection.base, None)
        .await?
        .into_iter()
        .filter_map(|key| match key {
            ExtentKey::LegacyData(file) => Some(file),
            _ => None,
        })
        .collect();
    let listed: BTreeSet<u32> = (0..fits_u32(directory.files.len(), "data file")?).collect();
    if found != listed {
        return Err(legacy_error(format!(
            "legacy/data holds files {found:?}, the directory lists {} files numbered from 0",
            listed.len()
        )));
    }
    let mut files: Vec<Vec<String>> = Vec::with_capacity(directory.files.len());
    let mut rows = 0u64;
    for (file, listed) in (0u32..).zip(&directory.files) {
        let records = reader.file(file).await?;
        if legacy_records_sha256(&records)? != listed.records_sha256 {
            return Err(legacy_error(format!(
                "the records of data file {file} do not hash to the digest the directory lists"
            )));
        }
        rows += records.len() as u64;
        files.push(
            records
                .iter()
                .map(|record| record.commit.graph_commit_id.clone())
                .collect(),
        );
    }
    let held = |file: u32, row: usize| {
        files
            .get(file as usize)
            .and_then(|ids| ids.get(row))
            .map(String::as_str)
    };
    let mut entries = 0u64;
    for shard in 0..fits_u32(directory.id_shards.len(), "id shard")? {
        for entry in &reader.selection.id_shard(&directory, shard).await?.entries {
            if held(entry.file, entry.row as usize) != Some(entry.id.to_string().as_str()) {
                return Err(legacy_error(format!(
                    "id shard {shard} names row {} of data file {} for commit '{}', which that \
                     row does not hold",
                    entry.row, entry.file, entry.id
                )));
            }
            entries += 1;
        }
    }
    if entries != directory.commits || rows != directory.commits {
        return Err(legacy_error(format!(
            "the directory lists {} commits, the id shards {entries} and the data files {rows}",
            directory.commits
        )));
    }
    for shard in 0..fits_u32(directory.writer_shards.len(), "writer shard")? {
        for writer in &reader
            .selection
            .writer_shard(&directory, shard)
            .await?
            .entries
        {
            let last = (writer.first_file + writer.files).saturating_sub(1);
            let head = files
                .get(last as usize)
                .and_then(|ids| ids.last())
                .map(String::as_str);
            if head != Some(writer.head.to_string().as_str()) {
                return Err(legacy_error(format!(
                    "writer shard {shard} names '{}' as the head of a writer whose last data \
                     file {last} ends with another commit",
                    writer.head
                )));
            }
        }
    }
    Ok(Some(directory))
}

/// One reader of the legacy locator of a root that has one, through the cache
/// of a handle when given: the writers it lists and the records of a data file.
pub(crate) struct LegacyReader<'a> {
    selection: Selection<'a>,
    directory: Arc<LegacyDirectory>,
}

impl<'a> LegacyReader<'a> {
    /// The reader of the root's legacy history; `None` when the root has none.
    pub(crate) async fn open(
        root_uri: &str,
        session: &Arc<lance::session::Session>,
        cache: Option<&'a ExtentCache>,
    ) -> Result<Option<Self>> {
        let (store, base) = store(root_uri, session).await?;
        let mut selection = Selection {
            scheduler: scheduler(&store),
            store,
            base,
            lineage_only: false,
            budget: None,
            cache,
            memo: LegacyMemo::default(),
        };
        Ok(selection.directory().await?.map(|directory| Self {
            selection,
            directory,
        }))
    }

    pub(crate) fn directory(&self) -> &Arc<LegacyDirectory> {
        &self.directory
    }

    /// The writers listed under the native name `native` (`None` for main), in
    /// kind order: at most one ref of `__manifest`, then an orphaned writer.
    pub(crate) async fn writers(&mut self, native: Option<&str>) -> Result<Vec<LegacyWriter>> {
        let directory = Arc::clone(&self.directory);
        let mut writers = Vec::new();
        for shard in directory.writer_shards_of(native) {
            let shard = self.selection.writer_shard(&directory, shard).await?;
            writers.extend(
                shard
                    .find(native)
                    .filter(|writer| writer.native.as_deref() == native)
                    .cloned(),
            );
        }
        Ok(writers)
    }

    /// The complete records of data file `file`, in the row count the
    /// directory lists for it.
    pub(crate) async fn file(&mut self, file: u32) -> Result<Arc<Vec<HistoryRecord>>> {
        let listed = self
            .directory
            .files
            .get(file as usize)
            .ok_or_else(|| legacy_error(format!("the directory lists no data file {file}")))?
            .rows;
        let rows = self
            .selection
            .read(&ExtentKey::LegacyData(file), Narrow::All)
            .await?
            .ok_or_else(|| {
                legacy_error(format!(
                    "the directory lists data file {file} and the object is absent"
                ))
            })?;
        match rows {
            Rows::Full(records) if records.len() == listed as usize => Ok(records),
            rows => Err(legacy_error(format!(
                "data file {file} holds {} complete records, the directory lists {listed}",
                rows.len()
            ))),
        }
    }
}

fn insert_copy<T: PartialEq>(known: &mut HashMap<String, T>, id: String, copy: T) -> Result<()> {
    match known.get(&id) {
        Some(first) if first != &copy => Err(copies_differ(&id)),
        Some(_) => Ok(()),
        None => {
            known.insert(id, copy);
            Ok(())
        }
    }
}

fn check_slot(slots: &mut HashMap<(Ulid, u16), String>, id: &str) -> Result<()> {
    if let Some(parsed) = parse_history_block_id(id)?
        && let Some(previous) = slots.insert((parsed.block, parsed.slot), id.to_string())
        && previous != id
    {
        return Err(invalid(
            "overlapping extents disagree on the commit at one slot",
        ));
    }
    Ok(())
}

/// Every settled commit, with duplicate lineage and slot identities checked.
pub async fn read_lineage(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
) -> Result<HashMap<String, GraphLineageRow>> {
    read_lineage_selected(root_uri, session, None, None, None).await
}

/// [`read_lineage`], with every object read through `cache`. The LIST of
/// `__history` is still issued: absence is never cached.
pub async fn read_lineage_in(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    cache: &ExtentCache,
) -> Result<HashMap<String, GraphLineageRow>> {
    read_lineage_selected(root_uri, session, None, None, Some(cache)).await
}

/// Lineage of the blocks addressed by `graph_commit_ids`: the whole block from
/// its one object, else the range extent covering the requested slots, or the
/// commits at those slots when several extents cover them, of which the
/// `COPIES_READ` narrowest per slot are read and compared. Tables remain
/// outside the reads.
pub async fn read_lineage_of(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    graph_commit_ids: &[&str],
) -> Result<HashMap<String, GraphLineageRow>> {
    read_lineage_of_bounded(root_uri, session, graph_commit_ids, DEFAULT_LINEAGE_BYTES).await
}

/// Read addressed lineage within a conservative fetch/decode/owned-row allowance.
/// An extent is fetched only while the allowance covers one suffix read, and
/// every fetched byte is charged, the copies read included.
pub async fn read_lineage_of_bounded(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    graph_commit_ids: &[&str],
    max_bytes: usize,
) -> Result<HashMap<String, GraphLineageRow>> {
    let ids = Some(graph_commit_ids);
    read_lineage_selected(root_uri, session, ids, Some(max_bytes), None).await
}

/// [`read_lineage_of_bounded`] through `cache`: an object the handle has read
/// costs no request and is charged for its rows only.
pub async fn read_lineage_of_bounded_in(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    cache: &ExtentCache,
    graph_commit_ids: &[&str],
    max_bytes: usize,
) -> Result<HashMap<String, GraphLineageRow>> {
    let ids = Some(graph_commit_ids);
    read_lineage_selected(root_uri, session, ids, Some(max_bytes), Some(cache)).await
}

async fn read_lineage_selected(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    ids: Option<&[&str]>,
    budget: Option<usize>,
    cache: Option<&ExtentCache>,
) -> Result<HashMap<String, GraphLineageRow>> {
    let mut found = HashMap::new();
    let mut slots = HashMap::new();
    for rows in read_selected(root_uri, session, ids, true, budget, cache).await? {
        for commit in rows.commits() {
            check_slot(&mut slots, &commit.graph_commit_id)?;
            insert_copy(&mut found, commit.graph_commit_id.clone(), commit.clone())?;
        }
    }
    Ok(found)
}

pub async fn read_commit(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    graph_commit_id: &str,
) -> Result<Option<GraphLineageRow>> {
    Ok(read_lineage_of(root_uri, session, &[graph_commit_id])
        .await?
        .remove(graph_commit_id))
}

/// [`read_commit`] through `cache`.
pub async fn read_commit_in(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    cache: &ExtentCache,
    graph_commit_id: &str,
) -> Result<Option<GraphLineageRow>> {
    let ids = [graph_commit_id];
    let budget = Some(DEFAULT_LINEAGE_BYTES);
    Ok(
        read_lineage_selected(root_uri, session, Some(&ids), budget, Some(cache))
            .await?
            .remove(graph_commit_id),
    )
}

pub async fn read_record(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    graph_commit_id: &str,
) -> Result<Option<HistoryRecord>> {
    Ok(read_records_of(root_uri, session, &[graph_commit_id])
        .await?
        .remove(graph_commit_id))
}

/// [`read_record`] through `cache`.
pub async fn read_record_in(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    cache: &ExtentCache,
    graph_commit_id: &str,
) -> Result<Option<HistoryRecord>> {
    let ids = [graph_commit_id];
    Ok(
        read_records_selected(root_uri, session, Some(&ids), Some(cache))
            .await?
            .remove(graph_commit_id),
    )
}

pub async fn read_records_of(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    graph_commit_ids: &[&str],
) -> Result<HashMap<String, HistoryRecord>> {
    let mut found = read_records_selected(root_uri, session, Some(graph_commit_ids), None).await?;
    let wanted: HashSet<_> = graph_commit_ids.iter().copied().collect();
    found.retain(|id, _| wanted.contains(id.as_str()));
    Ok(found)
}

pub(crate) async fn read_records(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
) -> Result<HashMap<String, HistoryRecord>> {
    read_records_selected(root_uri, session, None, None).await
}

async fn read_records_selected(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    ids: Option<&[&str]>,
    cache: Option<&ExtentCache>,
) -> Result<HashMap<String, HistoryRecord>> {
    let mut found = HashMap::new();
    let mut slots = HashMap::new();
    for rows in read_selected(root_uri, session, ids, false, None, cache).await? {
        let Rows::Full(records) = rows else {
            return Err(invalid("a record read returned lineage only"));
        };
        for record in records.iter() {
            check_slot(&mut slots, &record.commit.graph_commit_id)?;
            insert_copy(
                &mut found,
                record.commit.graph_commit_id.clone(),
                canonical_record(record)?,
            )?;
        }
    }
    Ok(found)
}

/// Decode the shared checked Arrow representation of complete history records.
pub(crate) fn records_of(batch: &RecordBatch) -> Result<Vec<HistoryRecord>> {
    Ok(commits_of(batch)?
        .into_iter()
        .zip(tables_of(batch)?)
        .map(|(commit, tables)| HistoryRecord { commit, tables })
        .collect())
}

fn copies_differ(graph_commit_id: &str) -> OmniError {
    OmniError::manifest_internal(format!(
        "`__history` holds two records of graph commit '{graph_commit_id}' that differ"
    ))
}

#[cfg(test)]
pub(crate) async fn stored_batches(root_uri: &str) -> Result<Vec<RecordBatch>> {
    let (store, base) = store(root_uri, &crate::lance_access::control_session()).await?;
    let mut batches = Vec::new();
    let scheduler = scheduler(&store);
    for key in list_extents(&store, &base, None).await? {
        batches.push(
            read_extent(&store, &scheduler, &base, &key, false, None)
                .await?
                .ok_or_else(|| invalid("missing extent"))?,
        );
    }
    Ok(batches)
}

/// The name of every archive object under `__history`, sorted.
#[cfg(test)]
pub(crate) async fn stored_names(root_uri: &str) -> Result<Vec<String>> {
    let (store, base) = store(root_uri, &crate::lance_access::control_session()).await?;
    let prefix = format!("{base}/");
    Ok(list_extents(&store, &base, None)
        .await?
        .iter()
        .map(|key| key.path(&base).as_ref().replacen(&prefix, "", 1))
        .collect())
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    #[derive(Debug)]
    pub(crate) struct ReadFault {
        pub(crate) block_list: bool,
    }

    impl lance::io::WrappingObjectStore for ReadFault {
        fn wrap(
            &self,
            _: &str,
            original: Arc<dyn object_store::ObjectStore>,
        ) -> Arc<dyn object_store::ObjectStore> {
            Arc::new(FaultStore {
                original,
                block_list: self.block_list,
            })
        }
    }

    #[derive(Debug)]
    struct FaultStore {
        original: Arc<dyn object_store::ObjectStore>,
        block_list: bool,
    }

    fn refused() -> object_store::Error {
        object_store::Error::Generic {
            store: "history test fault",
            source: Box::new(std::io::Error::other("injected history request refusal")),
        }
    }

    impl std::fmt::Display for FaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "history read fault")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FaultStore {
        async fn put_opts(
            &self,
            _: &Path,
            _: PutPayload,
            _: PutOptions,
        ) -> object_store::Result<PutResult> {
            Err(refused())
        }
        async fn put_multipart_opts(
            &self,
            _: &Path,
            _: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(refused())
        }
        async fn get_opts(&self, path: &Path, _: GetOptions) -> object_store::Result<GetResult> {
            if self.block_list {
                return Err(refused());
            }
            Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "listed extent vanished",
                )),
            })
        }
        fn delete_stream(
            &self,
            _: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            futures::stream::once(async { Err(refused()) }).boxed()
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            if self.block_list {
                futures::stream::once(async { Err(refused()) }).boxed()
            } else {
                self.original.list(prefix)
            }
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            if self.block_list {
                return Err(refused());
            }
            self.original.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
            Err(refused())
        }
    }

    fn records() -> Vec<HistoryRecord> {
        crate::tests::history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    }

    fn held(bytes: &[u8]) -> ExtentReader {
        ExtentReader {
            path: Path::from("extent.lance"),
            store: None,
            size: bytes.len(),
            start: 0,
            bytes: Bytes::copy_from_slice(bytes),
            refetched: Arc::default(),
        }
    }

    #[tokio::test]
    async fn extent_is_a_lance_v2_2_file_that_round_trips_nulls_and_table_states() {
        let mut records = records()[..4].to_vec();
        records[0].commit.actor_id = None;
        records[1].commit.native_branch = Some("feature".to_string());
        records[1].tables = Vec::new();
        records[2].tables = crate::tests::history_table_states();
        let bytes = encode(&records, MAX_OBJECT_BYTES).await.unwrap().unwrap();
        let footer = &bytes[bytes.len() - FOOTER_BYTES..];
        assert_eq!(&footer[FOOTER_BYTES - 4..], b"LANC");
        assert_eq!(
            &footer[FOOTER_BYTES - 8..FOOTER_BYTES - 4],
            [2, 0, 2, 0],
            "the footer names file version 2.2"
        );
        let scheduler = scheduler(&Arc::new(ObjectStore::memory()));
        let full = decode(&scheduler, held(&bytes), false).await.unwrap();
        assert_eq!(full.schema().fields().len(), 3);
        assert_eq!(records_of(&full).unwrap(), records);
        let lineage = decode(&scheduler, held(&bytes), true).await.unwrap();
        assert!(lineage.column_by_name(TABLES_COLUMN).is_none());
        assert_eq!(
            commits_of(&lineage).unwrap(),
            records
                .iter()
                .map(|record| record.commit.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn truncated_foreign_and_misaddressed_objects_are_errors() {
        let records = records();
        let bytes = encode(&records[..2], MAX_OBJECT_BYTES)
            .await
            .unwrap()
            .unwrap();
        let scheduler = scheduler(&Arc::new(ObjectStore::memory()));
        for lineage_only in [false, true] {
            for end in [
                0,
                3,
                FOOTER_BYTES - 1,
                FOOTER_BYTES,
                bytes.len() / 2,
                bytes.len() - FOOTER_BYTES,
                bytes.len() - 1,
            ] {
                assert!(
                    decode(&scheduler, held(&bytes[..end]), lineage_only)
                        .await
                        .is_err(),
                    "truncation at {end}"
                );
            }
            for foreign in [vec![7; 4096], b"OGHB0001".repeat(64), bytes[1..].to_vec()] {
                assert!(
                    decode(&scheduler, held(&foreign), lineage_only)
                        .await
                        .is_err()
                );
            }
            let footer_offsets_and_counts = [(0, 8), (16, 8), (24, 4), (28, 4)];
            let derived_from_the_column_count = (8, 8);
            assert!(!footer_offsets_and_counts.contains(&derived_from_the_column_count));
            for (at, width) in footer_offsets_and_counts {
                for value in [0u64, 1, bytes.len() as u64, u64::MAX] {
                    let mut bad = bytes.clone();
                    let at = bytes.len() - FOOTER_BYTES + at;
                    if bad[at..at + width] == value.to_le_bytes()[..width] {
                        continue;
                    }
                    bad[at..at + width].copy_from_slice(&value.to_le_bytes()[..width]);
                    assert!(
                        decode(&scheduler, held(&bad), lineage_only).await.is_err(),
                        "footer field at {at} set to {value}"
                    );
                }
            }
            let mut bad = bytes.clone();
            let at = bytes.len() - FOOTER_BYTES + GLOBAL_BUFFER_COUNT_AT;
            bad[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let error = decode(&scheduler, held(&bad), lineage_only)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("global buffer count"),
                "refused before the Lance reader sizes an allocation from it: {error}"
            );
            assert!(
                decode(&scheduler, held(&bytes), lineage_only).await.is_ok(),
                "the untouched object still decodes"
            );
        }

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        settle(root, &session, &records[..2]).await.unwrap();
        let (store, base) = store(root, &session).await.unwrap();
        let key = list_extents(&store, &base, None).await.unwrap().remove(0);
        store
            .inner
            .put_opts(
                &key.path(&base),
                PutPayload::from(vec![7; 4096]),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let id = records[0].commit.graph_commit_id.as_str();
        assert!(read_record(root, &session, id).await.is_err());
        assert!(read_lineage_of(root, &session, &[id]).await.is_err());
        assert!(
            settle(root, &session, &records[..2]).await.is_err(),
            "a foreign object under an extent key is never taken for the records"
        );
    }

    #[tokio::test]
    async fn overlapping_extents_validate_nonce_lineage_and_complete_records() {
        let records = records();
        let session = crate::lance_access::control_session();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        settle(root, &session, &records[..8]).await.unwrap();
        settle(root, &session, &records[4..]).await.unwrap();
        assert_eq!(read_records(root, &session).await.unwrap().len(), 16);
        settle(root, &session, &records[..8]).await.unwrap();
        assert_eq!(stored_batches(root).await.unwrap().len(), 2);
        let mut different_tables = records[..8].to_vec();
        different_tables[0].tables.clear();
        assert!(
            settle(root, &session, &different_tables)
                .await
                .unwrap_err()
                .to_string()
                .contains("that differ")
        );
        assert_eq!(
            read_record(root, &session, &records[0].commit.graph_commit_id)
                .await
                .unwrap(),
            Some(records[0].clone())
        );

        for conflict in ["nonce", "lineage", "tables"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_str().unwrap();
            settle(root, &session, &records[..8]).await.unwrap();
            let mut overlap = records[4..].to_vec();
            match conflict {
                "nonce" => {
                    let mut id = parse_history_block_id(&overlap[0].commit.graph_commit_id)
                        .unwrap()
                        .unwrap();
                    id.nonce = Ulid::from(999u128);
                    overlap[0].commit.graph_commit_id = id.to_string();
                }
                "lineage" => overlap[0].commit.actor_id = Some("different".to_string()),
                _ => overlap[0].tables.clear(),
            }
            settle(root, &session, &overlap).await.unwrap();
            let id = &records[4].commit.graph_commit_id;
            assert!(read_record(root, &session, id).await.is_err(), "{conflict}");
            if conflict != "tables" {
                assert!(
                    read_lineage_of(root, &session, &[id]).await.is_err(),
                    "{conflict}"
                );
            }
        }
    }

    /// One covering range extent is read whole for lineage; a record read and
    /// overlapping extents keep the requested slots only.
    #[tokio::test]
    async fn range_extents_are_narrowed_to_the_requested_slots() {
        let records = records();
        let session = crate::lance_access::control_session();
        let slot_3 = records[3].commit.graph_commit_id.as_str();
        let slot_5 = records[5].commit.graph_commit_id.as_str();

        let disjoint = tempfile::tempdir().unwrap();
        let root = disjoint.path().to_str().unwrap();
        settle(root, &session, &records[..6]).await.unwrap();
        settle(root, &session, &records[6..]).await.unwrap();
        let canonical = read_records_selected(root, &session, Some(&[slot_3]), None)
            .await
            .unwrap();
        assert_eq!(canonical.len(), 1, "only the requested record is kept");
        assert_eq!(canonical.get(slot_3), Some(&records[3]));
        let lineage = read_lineage_of(root, &session, &[slot_3]).await.unwrap();
        assert_eq!(lineage.len(), 6, "the one covering extent comes whole");

        let overlapping = tempfile::tempdir().unwrap();
        let root = overlapping.path().to_str().unwrap();
        settle(root, &session, &records[..8]).await.unwrap();
        settle(root, &session, &records[4..]).await.unwrap();
        let lineage = read_lineage_of(root, &session, &[slot_5]).await.unwrap();
        assert_eq!(
            lineage.into_values().collect::<Vec<_>>(),
            [records[5].commit.clone()],
            "two covering copies are compared at the requested slot and no other row is owned"
        );
    }

    #[tokio::test]
    async fn opaque_ids_have_bounded_keys_and_listed_extents_cannot_disappear() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let mut record = records().remove(0);
        record.commit.graph_commit_id = "opaque/../".repeat(1000);
        settle(root, &session, std::slice::from_ref(&record))
            .await
            .unwrap();
        assert_eq!(
            read_record(root, &session, &record.commit.graph_commit_id)
                .await
                .unwrap(),
            Some(record.clone())
        );
        let (store, base) = store(root, &session).await.unwrap();
        let key = list_extents(&store, &base, None).await.unwrap().remove(0);
        assert_eq!(key.path(&base).filename().unwrap().len(), 70);
        let mut wrong = record.clone();
        wrong.commit.graph_commit_id = "another".to_string();
        assert!(key.validate(&records_to_batch(&[wrong]).unwrap()).is_err());
        let block = records();
        settle(root, &session, &block).await.unwrap();
        let probes = crate::instrumentation::QueryIoProbes {
            history_wrapper: Some(Arc::new(ReadFault { block_list: false })),
            ..Default::default()
        };
        crate::instrumentation::with_query_io_probes(probes, async {
            for error in [
                read_lineage(root, &session).await.unwrap_err(),
                read_records(root, &session).await.unwrap_err(),
                read_lineage_of(root, &session, &[&block[0].commit.graph_commit_id])
                    .await
                    .unwrap_err(),
                read_record(root, &session, &block[0].commit.graph_commit_id)
                    .await
                    .unwrap_err(),
            ] {
                assert!(
                    error
                        .to_string()
                        .contains("listed immutable extent disappeared"),
                    "{error}"
                );
            }
            assert!(
                read_record(root, &session, "never-archived")
                    .await
                    .unwrap()
                    .is_none()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn selective_lineage_budget_refuses_payload_before_fetching_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let records = records();
        settle(root, &session, &records).await.unwrap();
        let probes = crate::instrumentation::QueryIoProbes {
            history_wrapper: Some(Arc::new(crate::tests::SmallScanProbeMarker)),
            ..Default::default()
        };
        crate::instrumentation::with_query_io_probes(probes.clone(), async {
            let error = read_lineage_of_bounded(
                root,
                &session,
                &[&records[0].commit.graph_commit_id],
                KEY_BYTES + TAIL_BYTES - 1,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("byte budget exhausted"),
                "{error}"
            );
        })
        .await;
        let stores = probes.history_stores.stores();
        let reads: u64 = stores
            .iter()
            .map(|store| store.io_stats_incremental().read_bytes)
            .sum();
        assert_eq!(
            reads, 0,
            "an allowance below one suffix read refuses the extent before any GET"
        );
        assert_eq!(
            read_lineage_of_bounded(
                root,
                &session,
                &[&records[0].commit.graph_commit_id],
                DEFAULT_LINEAGE_BYTES
            )
            .await
            .unwrap()
            .len(),
            16
        );
    }

    #[tokio::test]
    async fn a_record_at_the_single_record_bound_is_archived_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let with_e_tag = |len: usize| {
            let mut record = records().remove(0);
            let crate::state::TableState::Pinned(pin) = &mut record.tables[0].state else {
                unreachable!("the block records pin their table");
            };
            pin.metadata = crate::TableVersionMetadata::from_json_str(&format!(
                r#"{{"manifest_path":"p","manifest_size":null,"e_tag":"{}","naming_scheme":null}}"#,
                "e".repeat(len)
            ))
            .unwrap();
            record
        };
        let empty = with_e_tag(0);
        let rest = MAX_RECORD_BYTES - record_bytes(&empty.commit, &empty.tables);
        let record = with_e_tag(rest);
        assert_eq!(
            record_bytes(&record.commit, &record.tables),
            MAX_RECORD_BYTES
        );
        check_head_record(&record.commit, &record.tables).unwrap();
        let settled = settle(root, &session, std::slice::from_ref(&record)).await;
        assert!(
            settled.is_ok(),
            "an admitted head is archivable: {:.300}",
            settled.unwrap_err().to_string()
        );
        let id = record.commit.graph_commit_id.clone();
        let read = read_record(root, &session, &id).await;
        assert!(
            matches!(&read, Ok(Some(read)) if *read == record),
            "the record reads back whole: {:.300}",
            read.err()
                .map(|error| error.to_string())
                .unwrap_or_default()
        );
        let commit = read_commit(root, &session, &id).await.unwrap();
        assert!(commit.as_ref() == Some(&record.commit));

        let record = with_e_tag(rest + 1);
        let error = check_head_record(&record.commit, &record.tables).unwrap_err();
        assert!(
            error.to_string().contains("bound of one history record"),
            "{error}"
        );
    }

    #[test]
    fn a_head_is_refused_over_the_bound_of_its_commit_fields() {
        let mut record = records().remove(0);
        record.commit.actor_id = None;
        let rest = crate::HISTORY_RELEASE_BYTES - commit_bytes(&record.commit);
        record.commit.actor_id = Some("a".repeat(rest));
        check_head_record(&record.commit, &record.tables).unwrap();

        record.commit.actor_id = Some("a".repeat(rest + 1));
        let error = check_head_record(&record.commit, &record.tables).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("bound of the commit fields of one history record"),
            "{error}"
        );
    }

    /// `count` own commits of `native` with ULIDs from `first`, a first-parent
    /// chain at consecutive versions from `version`, each with every table state.
    fn legacy_records(
        native: Option<&str>,
        first: u128,
        count: usize,
        version: u64,
    ) -> Vec<HistoryRecord> {
        let ids: Vec<String> = (0..count)
            .map(|index| Ulid::from(first + index as u128).to_string())
            .collect();
        ids.iter()
            .enumerate()
            .map(|(index, id)| HistoryRecord {
                commit: GraphLineageRow {
                    graph_commit_id: id.clone(),
                    schema_contract: None,
                    schema_content_hash: None,
                    graph_branch: native.map(|native| native.replace(".fork", "")),
                    native_branch: native.map(str::to_string),
                    graph_manifest_version: version + index as u64,
                    generation: index as u64,
                    parent_commit_id: index.checked_sub(1).map(|parent| ids[parent].clone()),
                    merged_parent_commit_id: None,
                    actor_id: None,
                    created_at: index as i64,
                },
                tables: crate::tests::history_table_states(),
            })
            .collect()
    }

    fn legacy_chain(
        kind: LegacyWriterKind,
        native: Option<&str>,
        parent: Option<&str>,
        records: Vec<HistoryRecord>,
    ) -> LegacyChain {
        LegacyChain {
            kind,
            native: native.map(str::to_string),
            parent: parent.map(str::to_string),
            parent_version: u64::from(parent.is_some()) * 3,
            head_version: records.last().unwrap().commit.graph_manifest_version + 1,
            records,
        }
    }

    /// Main with five commits, a live fork with three, one absent merged parent.
    fn legacy_objects(attempt: u128) -> LegacyObjects {
        LegacyObjects::plan(
            &LegacyLayout::CURRENT,
            13,
            Ulid::from(attempt),
            &[
                legacy_chain(
                    LegacyWriterKind::Main,
                    None,
                    None,
                    legacy_records(None, 1_000, 5, 1),
                ),
                legacy_chain(
                    LegacyWriterKind::Live,
                    Some("feature.fork"),
                    None,
                    legacy_records(Some("feature.fork"), 2_000, 3, 4),
                ),
            ],
            &BTreeSet::from([Ulid::from(7u128), Ulid::from(5u128)]),
        )
        .unwrap()
    }

    fn message(error: OmniError) -> String {
        error.to_string()
    }

    #[test]
    fn legacy_data_keys_name_eight_digit_files_and_nothing_else() {
        let base = Path::from("root/__history");
        let key = ExtentKey::LegacyData(7);
        let path = key.path(&base);
        assert_eq!(path.as_ref(), "root/__history/legacy/data/00000007.lance");
        assert_eq!(ExtentKey::from_path(&base, &path).unwrap(), Some(key));
        assert!(ExtentKey::LegacyData(0).covers(&BTreeSet::new()));
        let parse =
            |relative: &str| ExtentKey::from_path(&base, &Path::from(format!("{base}/{relative}")));
        assert_eq!(
            parse("legacy/data/00000000.lance").unwrap(),
            Some(ExtentKey::LegacyData(0))
        );
        assert_eq!(
            parse(&format!(
                "legacy/data/00000003.lance.tmp.{}",
                "0a".repeat(16)
            ))
            .unwrap(),
            None,
            "a conditional-create temporary of a data file is skipped"
        );
        for refused in [
            "legacy/data/1.lance",
            "legacy/data/000000001.lance",
            "legacy/data/0000000a.lance",
            "legacy/data/00000001.parquet",
            "legacy/x.lance",
            "legacy/locator/directory.oglx",
        ] {
            let error = message(parse(refused).unwrap_err());
            assert!(
                error.contains("invalid history extent"),
                "{refused}: {error}"
            );
        }
    }

    #[test]
    fn legacy_data_files_are_validated_as_one_writer_chain() {
        let validate = |records: &[HistoryRecord]| {
            ExtentKey::LegacyData(0).validate(&records_to_batch(records).unwrap())
        };
        let good = legacy_records(Some("feature.fork"), 10, 4, 2);
        validate(&good).unwrap();

        let mut block_id = good.clone();
        block_id[0].commit.graph_commit_id = records()[0].commit.graph_commit_id.clone();
        block_id[1].commit.parent_commit_id = Some(block_id[0].commit.graph_commit_id.clone());
        assert!(message(validate(&block_id).unwrap_err()).contains("block ID"));

        let mut repeated = good.clone();
        repeated[3].commit.graph_commit_id = repeated[1].commit.graph_commit_id.clone();
        assert!(message(validate(&repeated).unwrap_err()).contains("repeats a commit ID"));

        let mut unlinked = good.clone();
        unlinked[2].commit.parent_commit_id = Some(unlinked[0].commit.graph_commit_id.clone());
        assert!(message(validate(&unlinked).unwrap_err()).contains("first-parent chain"));

        let mut two_writers = good.clone();
        two_writers[3].commit.native_branch = Some("other.fork".to_string());
        assert!(message(validate(&two_writers).unwrap_err()).contains("two writers"));
    }

    #[test]
    fn legacy_records_digest_is_over_the_canonical_records() {
        let records = legacy_records(None, 30, 2, 1);
        let digest = legacy_records_sha256(&records).unwrap();
        let mut reversed = records.clone();
        reversed[0].tables.reverse();
        assert_eq!(legacy_records_sha256(&reversed).unwrap(), digest);
        let mut other_pin = records.clone();
        let TableState::Pinned(pin) = &mut other_pin[1].tables[0].state else {
            panic!("the first table state is pinned");
        };
        pin.row_count += 1;
        assert_ne!(legacy_records_sha256(&other_pin).unwrap(), digest);
        let mut other_commit = records;
        other_commit[0].commit.created_at += 1;
        assert_ne!(legacy_records_sha256(&other_commit).unwrap(), digest);
    }

    #[test]
    fn legacy_locator_codecs_round_trip_and_refuse_other_bytes() {
        let objects = legacy_objects(99);
        let id_shard = &objects.id_shards()[0];
        let writer_shard = &objects.writer_shards()[0];
        let directory = objects.directory();
        assert_eq!(objects.files().len(), 2);
        assert_eq!(id_shard.entries.len(), 8);
        assert_eq!(writer_shard.entries.len(), 2);
        assert_eq!(
            directory.absent_parents,
            [Ulid::from(5u128), Ulid::from(7u128)]
        );

        let id_bytes = id_shard.encode().unwrap();
        assert_eq!(id_bytes.len(), SHARD_HEADER_BYTES + 8 * ID_ENTRY_BYTES);
        assert_eq!(&LegacyIdShard::decode(&id_bytes).unwrap(), id_shard);
        let writer_bytes = writer_shard.encode().unwrap();
        assert_eq!(
            &LegacyWriterShard::decode(&writer_bytes).unwrap(),
            writer_shard
        );
        let directory_bytes = directory.encode().unwrap();
        assert_eq!(
            directory_bytes.len(),
            DIRECTORY_HEADER_BYTES
                + 2 * DIRECTORY_FILE_BYTES
                + DIRECTORY_ID_SHARD_BYTES
                + DIRECTORY_WRITER_SHARD_BYTES
                + 2 * 16
                + SHA256_BYTES
        );
        assert_eq!(
            &LegacyDirectory::decode(&directory_bytes).unwrap(),
            directory
        );

        type Refuses = fn(&[u8]) -> bool;
        let decoders: [(&str, &[u8], Refuses); 3] = [
            ("id shard", &id_bytes, |bytes| {
                LegacyIdShard::decode(bytes).is_err()
            }),
            ("writer shard", &writer_bytes, |bytes| {
                LegacyWriterShard::decode(bytes).is_err()
            }),
            ("directory", &directory_bytes, |bytes| {
                LegacyDirectory::decode(bytes).is_err()
            }),
        ];
        for (object, bytes, refused) in decoders {
            assert!(refused(&bytes[..bytes.len() - 1]), "{object}: truncated");
            assert!(refused(&[bytes, &[0]].concat()), "{object}: trailing byte");
            assert!(refused(&[]), "{object}: empty");
            let magic_and_count = [7, 9];
            let every_byte = [7, 9, bytes.len() / 2, bytes.len() - 1];
            let flipped: &[usize] = match object {
                "directory" => &every_byte,
                _ => &magic_and_count,
            };
            for at in flipped {
                let mut bad = bytes.to_vec();
                bad[*at] ^= 1;
                assert!(refused(&bad), "{object}: byte {at} flipped");
            }
        }
        let mut other_digest = writer_bytes.clone();
        other_digest[SHARD_HEADER_BYTES] ^= 1;
        assert!(
            LegacyWriterShard::decode(&other_digest).is_err(),
            "a writer digest must be the digest of the stored name"
        );

        let mut unsorted = id_shard.clone();
        unsorted.entries.swap(0, 1);
        assert!(message(unsorted.encode().unwrap_err()).contains("strictly ascending"));
        let mut unsorted = writer_shard.clone();
        unsorted.entries.swap(0, 1);
        assert!(message(unsorted.encode().unwrap_err()).contains("strictly ascending"));
        let mut main_named = writer_shard.clone();
        main_named.entries[0].kind = LegacyWriterKind::Main;
        main_named.entries[1].kind = LegacyWriterKind::Main;
        assert!(message(main_named.encode().unwrap_err()).contains("main exactly when"));

        let mut other_layout = directory.clone();
        other_layout.layout = 2;
        let error = message(other_layout.encode().unwrap_err());
        assert!(error.contains("layout version 2 is not known"), "{error}");
        let mut patched = directory_bytes[..directory_bytes.len() - SHA256_BYTES].to_vec();
        patched[8..12].copy_from_slice(&2u32.to_le_bytes());
        let trailer = sha256(&patched);
        patched.extend_from_slice(&trailer);
        let error = message(LegacyDirectory::decode(&patched).unwrap_err());
        assert!(error.contains("layout version 2 is not known"), "{error}");
        let mut miscounted = directory.clone();
        miscounted.commits += 1;
        assert!(message(miscounted.encode().unwrap_err()).contains("disagree"));

        for (row, record) in objects.files()[1].iter().enumerate() {
            let id = Ulid::from_string(&record.commit.graph_commit_id).unwrap();
            assert_eq!(id_shard.find(id), Some((1, row as u32)));
            assert_eq!(directory.id_shard_of(id), Some(0));
        }
        assert_eq!(id_shard.find(Ulid::from(3u128)), None);
        assert_eq!(directory.id_shard_of(Ulid::from(3u128)), None);
        assert_eq!(directory.id_shard_of(Ulid::from(u128::MAX)), None);
        let main: Vec<_> = writer_shard.find(None).collect();
        assert_eq!(main.len(), 1);
        assert_eq!(
            (
                main[0].kind,
                main[0].first_file,
                main[0].files,
                main[0].head
            ),
            (LegacyWriterKind::Main, 0, 1, Ulid::from(1_004u128))
        );
        let feature: Vec<_> = writer_shard.find(Some("feature.fork")).collect();
        assert_eq!(feature.len(), 1);
        assert_eq!(
            (
                feature[0].first_file,
                feature[0].files,
                feature[0].parent_version
            ),
            (1, 1, 0)
        );
        assert_eq!(writer_shard.find(Some("other")).count(), 0);
        assert_eq!(directory.writer_shards_of(None), 0..1);
        assert_eq!(directory.writer_shards_of(Some("feature.fork")), 0..1);
    }

    #[test]
    fn legacy_plan_cuts_id_shards_at_the_entry_bound() {
        assert_eq!(LEGACY_SHARD_ENTRIES, 21_844);
        let plan = |count: usize| {
            LegacyObjects::plan(
                &LegacyLayout::CURRENT,
                13,
                Ulid::from(1u128),
                &[legacy_chain(
                    LegacyWriterKind::Main,
                    None,
                    None,
                    legacy_records(None, 1 << 40, count, 1)
                        .into_iter()
                        .map(|record| HistoryRecord {
                            tables: Vec::new(),
                            ..record
                        })
                        .collect(),
                )],
                &BTreeSet::new(),
            )
            .unwrap()
        };
        let full = plan(LEGACY_SHARD_ENTRIES);
        assert_eq!(full.id_shards().len(), 1);
        let full_bytes = full.id_shards()[0].encode().unwrap().len();
        assert!(
            full_bytes <= TAIL_BYTES && full_bytes + ID_ENTRY_BYTES > TAIL_BYTES,
            "a full shard of {full_bytes} bytes is the most one lineage read holds"
        );
        let over = plan(LEGACY_SHARD_ENTRIES + 1);
        let entries: Vec<_> = over
            .id_shards()
            .iter()
            .map(|shard| shard.entries.len())
            .collect();
        assert_eq!(entries, [LEGACY_SHARD_ENTRIES, 1]);
        let directory = over.directory();
        assert_eq!(directory.commits, LEGACY_SHARD_ENTRIES as u64 + 1);
        assert!(directory.id_shards[0].last < directory.id_shards[1].first);
        assert!(directory.files.len() > 1, "the chain spans several files");
        assert_eq!(
            directory
                .files
                .iter()
                .map(|file| u64::from(file.rows))
                .sum::<u64>(),
            directory.commits
        );
    }

    #[test]
    fn legacy_plan_cuts_data_files_at_each_cap_and_never_across_a_writer() {
        let records = legacy_records(None, 500, 7, 1);
        let fields = commit_bytes(&records[1].commit) as u64;
        let bytes = record_bytes(&records[1].commit, &records[1].tables) as u64;
        assert!(
            commit_bytes(&records[0].commit) < fields as usize,
            "the genesis record has no parent and measures less"
        );
        let layout = |release_bytes: u64, file_record_bytes: u64, block_slots: u32| LegacyLayout {
            release_bytes,
            file_record_bytes,
            block_slots,
            ..LegacyLayout::CURRENT
        };
        let current = LegacyLayout::CURRENT;
        let rows_of = |layout: LegacyLayout, chains: Vec<LegacyChain>| {
            LegacyObjects::plan(&layout, 13, Ulid::from(1u128), &chains, &BTreeSet::new())
                .map(|objects| objects.files().iter().map(Vec::len).collect::<Vec<_>>())
                .map_err(message)
        };
        let main = |records: Vec<HistoryRecord>| {
            vec![legacy_chain(LegacyWriterKind::Main, None, None, records)]
        };
        assert_eq!(
            rows_of(
                layout(3 * fields, current.file_record_bytes, current.block_slots),
                main(records.clone())
            ),
            Ok(vec![3, 3, 1]),
            "cut at the commit-field cap"
        );
        assert_eq!(
            rows_of(
                layout(current.release_bytes, 2 * bytes, current.block_slots),
                main(records.clone())
            ),
            Ok(vec![2, 2, 2, 1]),
            "cut at the record-byte cap"
        );
        assert_eq!(
            rows_of(
                layout(current.release_bytes, current.file_record_bytes, 4),
                main(records.clone())
            ),
            Ok(vec![4, 3]),
            "cut at the row cap"
        );
        let mut broken = records.clone();
        broken[4].commit.parent_commit_id = Some(Ulid::from(1u128).to_string());
        assert_eq!(
            rows_of(current, main(broken)),
            Ok(vec![4, 3]),
            "cut where the first-parent link breaks"
        );
        let mut oversized = records[..3].to_vec();
        let template = crate::tests::history_table_states().remove(0);
        oversized[1].tables = (1..=256u64)
            .map(|table| {
                let mut row = template.clone();
                row.registration.identity = TableIdentity::new(table, 3).unwrap();
                row
            })
            .collect();
        assert_eq!(
            rows_of(
                layout(current.release_bytes, bytes * 3, current.block_slots),
                main(oversized)
            ),
            Ok(vec![1, 1, 1]),
            "a lone oversized record gets its own file"
        );
        let two_writers = vec![
            legacy_chain(LegacyWriterKind::Main, None, None, records[..2].to_vec()),
            legacy_chain(
                LegacyWriterKind::Retired,
                Some("old.fork"),
                None,
                legacy_records(Some("old.fork"), 900, 2, 3),
            ),
        ];
        let objects = LegacyObjects::plan(
            &layout(3 * fields, current.file_record_bytes, current.block_slots),
            13,
            Ulid::from(1u128),
            &two_writers,
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(
            objects.files().iter().map(Vec::len).collect::<Vec<_>>(),
            [2, 2],
            "a file never crosses a writer"
        );
        let files = &objects.directory().files;
        assert_eq!(
            files
                .iter()
                .map(|file| (file.first_version, file.last_version, file.rows))
                .collect::<Vec<_>>(),
            [(1, 2, 2), (3, 4, 2)]
        );
        assert_eq!(
            files[0].records_sha256,
            legacy_records_sha256(&records[..2]).unwrap()
        );
        let writers = &objects.writer_shards()[0].entries;
        let mut placed: Vec<_> = writers
            .iter()
            .map(|writer| (writer.native.clone(), writer.first_file, writer.files))
            .collect();
        placed.sort();
        assert_eq!(placed, [(None, 0, 1), (Some("old.fork".to_string()), 1, 1)]);

        let refused =
            |layout: LegacyLayout, chains: Vec<LegacyChain>| rows_of(layout, chains).unwrap_err();
        let empty = LegacyChain {
            kind: LegacyWriterKind::Live,
            native: Some("idle.fork".to_string()),
            parent: None,
            parent_version: 0,
            head_version: 1,
            records: Vec::new(),
        };
        assert!(refused(current, vec![empty]).contains("has no own commit"));
        assert!(
            refused(
                layout(fields - 1, current.file_record_bytes, current.block_slots),
                main(records.clone())
            )
            .contains("over the bounds")
        );
        assert!(
            refused(
                current,
                vec![
                    legacy_chain(LegacyWriterKind::Main, None, None, records[..2].to_vec()),
                    legacy_chain(LegacyWriterKind::Main, None, None, records[1..3].to_vec()),
                ]
            )
            .contains("written by two chains")
        );
        assert!(
            refused(
                current,
                vec![legacy_chain(
                    LegacyWriterKind::Live,
                    Some("feature.fork"),
                    None,
                    records.clone()
                )]
            )
            .contains("names native branch")
        );
        let mut block_id = records[..1].to_vec();
        block_id[0].commit.graph_commit_id = "hb1.not.a.ulid".to_string();
        assert!(refused(current, main(block_id)).contains("not a canonical ULID"));
        assert!(
            refused(
                LegacyLayout {
                    version: 2,
                    ..current
                },
                main(records.clone())
            )
            .contains("layout version 2 is not known")
        );
        assert!(
            refused(
                LegacyLayout {
                    shard_entries: current.shard_entries + 1,
                    ..current
                },
                main(records)
            )
            .contains("exceeds the bounds")
        );
    }

    #[tokio::test]
    async fn put_immutable_accepts_equal_bytes_and_refuses_different_ones() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let (store, base) = store(root, &session).await.unwrap();
        let path = legacy_directory_path(&base);
        let first = Bytes::from_static(b"first");
        assert!(put_immutable(&store, &path, first.clone()).await.unwrap());
        assert!(put_immutable(&store, &path, first.clone()).await.unwrap());
        assert!(
            !put_immutable(&store, &path, Bytes::from_static(b"other"))
                .await
                .unwrap()
        );
        let held = request(&store, &path, None).await.unwrap().unwrap();
        assert_eq!(body(held, |size| 0..size).await.unwrap().bytes, first);
    }

    #[tokio::test]
    async fn write_legacy_lists_data_files_and_hides_the_locator() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let objects = legacy_objects(99);
        write_legacy(root, &session, &objects).await.unwrap();
        let data_files = [
            "legacy/data/00000000.lance".to_string(),
            "legacy/data/00000001.lance".to_string(),
        ];
        assert_eq!(stored_names(root).await.unwrap(), data_files);

        let (store, base) = store(root, &session).await.unwrap();
        let planted = legacy_locator_path(&base).join(format!(
            "directory.{LOCATOR_EXTENSION}.tmp.{}",
            "0f".repeat(16)
        ));
        store
            .inner
            .put_opts(
                &planted,
                PutPayload::from_static(b"x"),
                PutOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            stored_names(root).await.unwrap(),
            data_files,
            "the locator prefix is skipped, a planted temporary included"
        );
        assert_eq!(
            list_extents(&store, &base, None).await.unwrap(),
            [ExtentKey::LegacyData(0), ExtentKey::LegacyData(1)]
        );

        let expected: Vec<HistoryRecord> = objects.files().concat();
        let records = read_records(root, &session).await.unwrap();
        assert_eq!(records.len(), expected.len());
        for record in &expected {
            assert_eq!(records.get(&record.commit.graph_commit_id), Some(record));
        }
        let lineage = read_lineage(root, &session).await.unwrap();
        assert_eq!(lineage.len(), expected.len());

        let fetch_all = |path: Path| {
            let store = &store;
            async move {
                let result = request(store, &path, None).await.unwrap().unwrap();
                body(result, |size| 0..size).await.unwrap().bytes
            }
        };
        let directory = fetch_all(legacy_directory_path(&base)).await;
        assert_eq!(
            sha256(&directory),
            sha256(&objects.directory().encode().unwrap())
        );
        assert_eq!(
            &LegacyDirectory::decode(&directory).unwrap(),
            objects.directory()
        );
        let id_shard = fetch_all(legacy_shard_path(&base, LEGACY_IDS_DIR, 0)).await;
        assert_eq!(sha256(&id_shard), objects.directory().id_shards[0].sha256);
        assert_eq!(
            &LegacyIdShard::decode(&id_shard).unwrap(),
            &objects.id_shards()[0]
        );
        let writer_shard = fetch_all(legacy_shard_path(&base, LEGACY_WRITERS_DIR, 0)).await;
        assert_eq!(
            sha256(&writer_shard),
            objects.directory().writer_shards[0].sha256
        );
        assert_eq!(
            &LegacyWriterShard::decode(&writer_shard).unwrap(),
            &objects.writer_shards()[0]
        );

        write_legacy(root, &session, &objects)
            .await
            .expect("an equal legacy set is accepted again");
        let error = message(
            write_legacy(root, &session, &legacy_objects(100))
                .await
                .unwrap_err(),
        );
        assert!(
            error.contains("holds other bytes") && error.contains("directory.oglx"),
            "{error}"
        );
        let mut other_tables = legacy_objects(99);
        other_tables.files[1][0].tables.clear();
        let error = message(
            write_legacy(root, &session, &other_tables)
                .await
                .unwrap_err(),
        );
        assert!(error.contains("that differ"), "{error}");
    }

    /// One record under an opaque id, as the genesis of a graph born at stamp
    /// 14 is archived: a singleton, never in a block and never under `legacy/`.
    fn born_at_14() -> HistoryRecord {
        let mut genesis = records().remove(0);
        genesis.commit.graph_commit_id = "born-at-14".to_string();
        genesis.commit.parent_commit_id = None;
        genesis
    }

    #[test]
    fn at_rows_keeps_the_named_rows_and_refuses_one_beyond_the_file() {
        let records = legacy_records(None, 1_000, 5, 1);
        let ids: Vec<&str> = records
            .iter()
            .map(|record| record.commit.graph_commit_id.as_str())
            .collect();
        let full = Rows::Full(Arc::new(records.clone()));
        let one = full.clone().at_rows(&BTreeSet::from([3])).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(
            one.commits()
                .next()
                .map(|commit| commit.graph_commit_id.as_str()),
            Some(ids[3])
        );
        assert_eq!(
            full.clone().at_rows(&(0..5).collect()).unwrap().len(),
            5,
            "every row named hands the object back whole"
        );
        let error = message(full.at_rows(&BTreeSet::from([5])).unwrap_err());
        assert!(
            error.contains("beyond the rows of the data file"),
            "{error}"
        );
        let lineage = Rows::Lineage(Arc::new(
            records.iter().map(|record| record.commit.clone()).collect(),
        ));
        let ends = lineage.at_rows(&BTreeSet::from([0, 4])).unwrap();
        assert_eq!(
            ends.commits()
                .map(|commit| commit.graph_commit_id.as_str())
                .collect::<Vec<_>>(),
            [ids[0], ids[4]]
        );
    }

    /// The GETs of legacy lookups through one handle, a cleared handle, a
    /// handle that only hits singletons, and a read without a handle.
    #[tokio::test]
    async fn legacy_lookup_reads_the_directory_once_per_handle() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let objects = legacy_objects(99);
        write_legacy(root, &session, &objects).await.unwrap();
        let genesis = born_at_14();
        settle(root, &session, std::slice::from_ref(&genesis))
            .await
            .unwrap();
        let main_first = &objects.files()[0][0];
        let fork_last = &objects.files()[1][2];
        let main_id = main_first.commit.graph_commit_id.as_str();
        let fork_id = fork_last.commit.graph_commit_id.as_str();
        let genesis_id = genesis.commit.graph_commit_id.as_str();
        let gets = crate::tests::HistoryGets::default();
        let handle = ExtentCache::default();
        crate::instrumentation::with_query_io_probes(gets.probes(), async {
            assert_eq!(
                read_commit_in(root, &session, &handle, main_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&main_first.commit)
            );
            assert_eq!(
                gets.drain(),
                (4, 1, 0),
                "the absent singleton name, the directory, the id shard and the data file"
            );
            assert_eq!(
                legacy_directory(root, &session, Some(&handle))
                    .await
                    .unwrap()
                    .as_deref(),
                Some(objects.directory())
            );
            assert_eq!(
                read_record_in(root, &session, &handle, fork_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(fork_last)
            );
            assert_eq!(
                gets.drain(),
                (1, 0, 0),
                "the directory and the id shard are held: the other data file alone"
            );
            assert_eq!(
                read_commit_in(root, &session, &handle, main_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&main_first.commit)
            );
            assert_eq!(
                read_commit_in(root, &session, &handle, fork_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&fork_last.commit)
            );
            assert_eq!(
                read_record_in(root, &session, &handle, fork_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(fork_last)
            );
            assert_eq!(
                gets.drain(),
                (0, 0, 0),
                "held data files serve lineage and record reads"
            );
            assert!(
                objects.files()[0].len() > 1,
                "the located row is one of several in its data file"
            );
            let one_row = KEY_BYTES + ROW_CHARGE;
            let located = read_lineage_of_bounded_in(root, &session, &handle, &[main_id], one_row)
                .await
                .expect("a located id is charged its address and its one row");
            assert_eq!(located.get(main_id), Some(&main_first.commit));
            assert_eq!(located.len(), 1);
            let error = message(
                read_lineage_of_bounded_in(root, &session, &handle, &[main_id], one_row - 1)
                    .await
                    .unwrap_err(),
            );
            assert!(
                error.contains("selective lineage byte budget exhausted"),
                "{error}"
            );
            assert_eq!(gets.drain(), (0, 0, 0), "the bounded reads are warm");
            assert_eq!(
                read_commit_in(root, &session, &handle, genesis_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&genesis.commit)
            );
            assert_eq!(
                gets.drain(),
                (1, 0, 0),
                "an id the locator does not list falls back to its singleton"
            );
            assert_eq!(
                read_commit_in(root, &session, &handle, "never-published")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(gets.drain(), (1, 1, 0));
            handle.clear();
            assert_eq!(
                read_commit_in(root, &session, &handle, main_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&main_first.commit)
            );
            assert_eq!(
                gets.drain(),
                (4, 1, 0),
                "a cleared handle probes the directory again"
            );

            let fresh = ExtentCache::default();
            assert_eq!(
                read_commit_in(root, &session, &fresh, genesis_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&genesis.commit)
            );
            assert_eq!(
                gets.drain(),
                (1, 0, 0),
                "a singleton hit never asks for the directory"
            );
            assert_eq!(
                read_commit_in(root, &session, &fresh, "never-published")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                gets.drain(),
                (2, 1, 0),
                "the first singleton miss reads the directory, which lists no such id"
            );
            assert_eq!(
                read_commit_in(root, &session, &fresh, "never-published")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(gets.drain(), (1, 1, 0));

            assert_eq!(
                read_commit(root, &session, main_id).await.unwrap().as_ref(),
                Some(&main_first.commit)
            );
            assert_eq!(
                gets.drain(),
                (4, 1, 0),
                "a read without a handle pays the probe itself"
            );
            let found = read_records_of(root, &session, &[main_id, fork_id, genesis_id])
                .await
                .unwrap();
            assert_eq!(found.get(main_id), Some(main_first));
            assert_eq!(found.get(fork_id), Some(fork_last));
            assert_eq!(found.get(genesis_id), Some(&genesis));
            assert_eq!(
                gets.drain(),
                (6, 1, 0),
                "the first singleton miss, the directory, the id shard once, two data files \
                 and the singleton hit"
            );
        })
        .await;

        let cold = 4 * KEY_BYTES
            + objects.directory().encode().unwrap().len()
            + objects.id_shards()[0].encode().unwrap().len()
            + TAIL_BYTES;
        let located =
            read_lineage_of_bounded_in(root, &session, &ExtentCache::default(), &[main_id], cold)
                .await
                .expect(
                    "a cold lookup is charged four addresses, the directory and the id shard, \
                     and still covers one suffix read of the data file",
                );
        assert_eq!(located.get(main_id), Some(&main_first.commit));
        let error = message(
            read_lineage_of_bounded_in(
                root,
                &session,
                &ExtentCache::default(),
                &[main_id],
                cold - 1,
            )
            .await
            .expect_err("one byte less leaves no suffix read for the data file"),
        );
        assert!(
            error.ends_with("selective lineage byte budget exhausted"),
            "{error}"
        );
    }

    /// Every way the locator can disagree with the objects it names fails the
    /// lookup loudly; none hands back another commit's row.
    #[tokio::test]
    async fn legacy_lookup_refuses_a_locator_that_disagrees_with_its_objects() {
        use object_store::ObjectStoreExt as _;
        let session = crate::lance_access::control_session();
        let objects = legacy_objects(99);
        let ids: Vec<&str> = objects.files()[0]
            .iter()
            .map(|record| record.commit.graph_commit_id.as_str())
            .collect();
        let lookup = |root: &str, wanted: Vec<&str>| {
            let (root, session) = (root.to_string(), Arc::clone(&session));
            let wanted: Vec<String> = wanted.into_iter().map(str::to_string).collect();
            async move {
                let wanted: Vec<&str> = wanted.iter().map(String::as_str).collect();
                let budget = DEFAULT_LINEAGE_BYTES;
                let fresh = ExtentCache::default();
                read_lineage_of_bounded_in(&root, &session, &fresh, &wanted, budget)
                    .await
                    .map_err(message)
            }
        };

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        write_legacy(root, &session, &objects).await.unwrap();
        let (store, base) = store(root, &session).await.unwrap();
        assert_eq!(lookup(root, vec![ids[0]]).await.unwrap().len(), 1);

        let data_file = ExtentKey::LegacyData(0).path(&base);
        store.inner.delete(&data_file).await.unwrap();
        let error = lookup(root, vec![ids[0]]).await.unwrap_err();
        assert!(
            error.contains("the directory lists data file 0 and the object is absent"),
            "{error}"
        );

        let shard_path = legacy_shard_path(&base, LEGACY_IDS_DIR, 0);
        let mut swapped = objects.id_shards()[0].clone();
        let (file, row) = (swapped.entries[0].file, swapped.entries[0].row);
        (swapped.entries[0].file, swapped.entries[0].row) =
            (swapped.entries[1].file, swapped.entries[1].row);
        (swapped.entries[1].file, swapped.entries[1].row) = (file, row);
        assert_ne!(swapped, objects.id_shards()[0]);
        store
            .inner
            .put_opts(
                &shard_path,
                PutPayload::from(swapped.encode().unwrap()),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let error = lookup(root, vec![ids[0]]).await.unwrap_err();
        assert!(
            error.contains("id shard 0 does not hash to the digest the directory lists"),
            "{error}"
        );

        store.inner.delete(&shard_path).await.unwrap();
        let error = lookup(root, vec![ids[0]]).await.unwrap_err();
        assert!(
            error.contains("the directory lists id shard 0 and the object is absent"),
            "{error}"
        );

        let mut misplaced = objects.clone();
        misplaced.id_shards[0].entries[1].row = 0;
        misplaced.id_shards[0].entries[2].row = 3;
        misplaced.directory.id_shards[0].sha256 = sha256(&misplaced.id_shards[0].encode().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        write_legacy(root, &session, &misplaced).await.unwrap();
        assert_eq!(lookup(root, vec![ids[4]]).await.unwrap().len(), 1);
        let error = lookup(root, vec![ids[2]]).await.unwrap_err();
        assert!(
            error.contains(&format!(
                "the locator places commit '{}' at row 3 of data file 0, which holds another \
                 commit",
                ids[2]
            )),
            "{error}"
        );
        let error = lookup(root, vec![ids[0], ids[1]]).await.unwrap_err();
        assert!(
            error.contains("the locator places two requested commits at row 0 of data file 0"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_root_without_legacy_history_pays_one_directory_miss_per_handle() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let genesis = born_at_14();
        settle(root, &session, std::slice::from_ref(&genesis))
            .await
            .unwrap();
        let genesis_id = genesis.commit.graph_commit_id.as_str();
        let gets = crate::tests::HistoryGets::default();
        let handle = ExtentCache::default();
        crate::instrumentation::with_query_io_probes(gets.probes(), async {
            assert_eq!(
                read_commit_in(root, &session, &handle, genesis_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&genesis.commit)
            );
            assert_eq!(
                gets.drain(),
                (1, 0, 0),
                "a graph born at stamp 14 finds its genesis in one GET"
            );
            assert_eq!(
                read_commit_in(root, &session, &handle, "never-published")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                gets.drain(),
                (2, 2, 0),
                "a true not-found adds the directory miss once"
            );
            assert_eq!(
                read_commit_in(root, &session, &handle, "never-published")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                gets.drain(),
                (1, 1, 0),
                "the handle keeps that the root has no directory"
            );
            assert!(
                legacy_directory(root, &session, Some(&handle))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(gets.drain(), (0, 0, 0));
            assert!(
                legacy_directory(root, &session, None)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(gets.drain(), (1, 1, 0));
        })
        .await;
    }

    /// A lineage naming a merged parent the directory lists as absent is
    /// complete without it; an unlisted id, or any id on a root without a
    /// directory, is still a missing parent.
    #[tokio::test]
    async fn lineage_accepts_a_merged_parent_the_directory_lists_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let objects = legacy_objects(99);
        write_legacy(root, &session, &objects).await.unwrap();
        let main_head = objects.files()[0].last().unwrap().commit.clone();
        let head_merging = |merged: u128| GraphLineageRow {
            graph_commit_id: Ulid::from(9_000u128).to_string(),
            graph_manifest_version: main_head.graph_manifest_version + 1,
            generation: main_head.generation + 1,
            parent_commit_id: Some(main_head.graph_commit_id.clone()),
            merged_parent_commit_id: Some(Ulid::from(merged).to_string()),
            ..main_head.clone()
        };
        let graph = |root: &str, head: GraphLineageRow| {
            crate::commit_graph::CommitGraph::from_head(
                root,
                Arc::clone(&session),
                head,
                &[],
                crate::commit_graph::HistoryCache::default(),
            )
        };
        let probes = crate::instrumentation::QueryIoProbes::default();
        let refreshes = Arc::clone(&probes.projection_full_refreshes);
        crate::instrumentation::with_query_io_probes(probes, async {
            let listed = graph(root, head_merging(7));
            let lineage = listed.lineage().await.unwrap();
            assert_eq!(lineage.first_parent_chain().unwrap().len(), 6);
            assert!(lineage.get_commit(&Ulid::from(7u128).to_string()).is_none());
            assert_eq!(refreshes.load(Ordering::Relaxed), 1);
            listed.lineage().await.unwrap();
            assert_eq!(
                refreshes.load(Ordering::Relaxed),
                1,
                "a listed absent parent counts as held: `__history` is not read again"
            );
            let unlisted = Ulid::from(8u128).to_string();
            let refused = graph(root, head_merging(8)).lineage().await.err();
            let error = message(refused.expect("an unlisted merged parent is missing"));
            assert!(
                error.contains("does not hold it") && error.contains(&unlisted),
                "{error}"
            );
        })
        .await;

        let bare = tempfile::tempdir().unwrap();
        let bare = bare.path().to_str().unwrap();
        settle(bare, &session, &objects.files()[0]).await.unwrap();
        let refused = graph(bare, head_merging(7)).lineage().await.err();
        let error = message(refused.expect("a root without a directory lists no absent parent"));
        assert!(error.contains("does not hold it"), "{error}");
        let mut unmerged = head_merging(7);
        unmerged.merged_parent_commit_id = None;
        assert_eq!(
            graph(bare, unmerged)
                .lineage()
                .await
                .unwrap()
                .first_parent_chain()
                .unwrap()
                .len(),
            6
        );
    }
}
