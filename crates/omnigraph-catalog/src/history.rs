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

use crate::error::{OmniError, Result};
use crate::layout::history_uri;
use crate::record::{
    COMMIT_FIELDS, FieldType, PACKED_STRUCT_KEY, RECORD_COLUMN, TABLE_FIELDS, compact_record,
    expand_record, packed_children, packed_record,
};
use crate::row::{CommitColumns, CommitColumnsBuilder, TableColumns, TableColumnsBuilder};
use crate::state::{
    GraphLineageRow, SchemaContractHead, SchemaContractRow, TableRow, commit_bytes, table_bytes,
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
}

fn singleton_digest(id: &str) -> String {
    format!("{:x}", Sha256::digest(id.as_bytes()))
}

impl ExtentKey {
    fn path(&self, base: &Path) -> Path {
        match self {
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
            Self::Full { .. } | Self::Singleton(_) => true,
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
            Self::Singleton(_) => None,
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

/// The rows of one extent file in `__history` column order, without the
/// `tables` column when `lineage_only`.
async fn decode(
    scheduler: &Arc<ScanScheduler>,
    reader: ExtentReader,
    lineage_only: bool,
) -> Result<RecordBatch> {
    let expected = file_schema()?;
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
    let mut stream = store.inner.list(Some(&prefix));
    let mut keys = Vec::new();
    let mut narrowest: BTreeMap<u16, Vec<(u16, ExtentKey)>> = BTreeMap::new();
    let mut temporary = 0;
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::storage(error.into()))?
    {
        if meta.location.prefix_match(&schemas).is_some() {
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
pub(crate) async fn archive_schema(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    contract: &SchemaContractRow,
) -> Result<String> {
    let bytes = schema_object(contract)?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let (store, base) = store(root_uri, session).await?;
    let path = schema_path(&base, &digest);
    if let Err(error) = store.put_if_absent(&path, bytes.clone().into()).await {
        let existing = match request(&store, &path, None).await.map_err(storage_error)? {
            Some(result) => body(result, |size| 0..size).await?,
            None => return Err(OmniError::storage(error.into())),
        };
        if existing.bytes != bytes {
            return Err(unreadable_schema(format!(
                "archived schema content '{digest}' differs from the content it is named for"
            )));
        }
    }
    Ok(digest)
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
}

#[derive(Default)]
struct CachedObjects {
    /// Per object path: its rows, their size, and when they were last used.
    held: HashMap<String, (Rows, usize, u64)>,
    bytes: usize,
    clock: u64,
}

/// The immutable `__history` objects one handle has read, by object path. An
/// object is created once, never replaced and never deleted, so its rows are
/// served again with no request. An absent object and a LIST are never kept:
/// absence can end. Bounded at 16 MiB, least recently used first out.
#[derive(Clone, Default)]
pub struct ExtentCache {
    objects: Arc<Mutex<CachedObjects>>,
}

impl ExtentCache {
    /// The held rows of `path`; complete records also serve a lineage read.
    fn get(&self, path: &Path, lineage_only: bool) -> Option<Rows> {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.clock += 1;
        let clock = objects.clock;
        let (rows, _, used) = objects.held.get_mut(path.as_ref())?;
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
            && matches!(objects.held.get(path.as_ref()), Some((Rows::Full(_), ..)))
        {
            return;
        }
        objects.clock += 1;
        let clock = objects.clock;
        if let Some((_, replaced, _)) = objects
            .held
            .insert(path.to_string(), (rows.clone(), bytes, clock))
        {
            objects.bytes -= replaced;
        }
        objects.bytes += bytes;
        while objects.bytes > CACHE_BYTES {
            let Some(oldest) = objects
                .held
                .iter()
                .min_by_key(|(_, (_, _, used))| *used)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            if let Some((_, evicted, _)) = objects.held.remove(&oldest) {
                objects.bytes -= evicted;
            }
        }
    }

    /// Forget every held object: the root was replaced, so a path may now name
    /// a different object. Shared by every clone of this cache.
    pub(crate) fn clear(&self) {
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects.held.clear();
        objects.bytes = 0;
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

/// One read of addressed or listed extents: the store, the allowance left,
/// and the cache of the handle that reads.
struct Selection<'a> {
    store: Arc<ObjectStore>,
    base: Path,
    scheduler: Arc<ScanScheduler>,
    lineage_only: bool,
    budget: Option<usize>,
    cache: Option<&'a ExtentCache>,
}

impl Selection<'_> {
    /// The rows of `key`, from the cache or one GET, narrowed to `kept_slots` when
    /// given; `None` when it is absent. Only the rows returned are charged as owned.
    async fn read(
        &mut self,
        key: &ExtentKey,
        kept_slots: Option<&BTreeSet<u16>>,
    ) -> Result<Option<Rows>> {
        if let Some(left) = &mut self.budget {
            *left = left
                .checked_sub(KEY_BYTES)
                .ok_or_else(|| invalid("selective lineage address budget exhausted"))?;
        }
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
        let rows = match kept_slots {
            Some(slots) => rows.at_slots(slots)?,
            None => rows,
        };
        if let Some(left) = &mut self.budget {
            *left = left
                .checked_sub(rows.len().saturating_mul(ROW_CHARGE))
                .ok_or_else(|| invalid("selective lineage byte budget exhausted"))?;
        }
        Ok(Some(rows))
    }

    async fn listed(
        &mut self,
        key: &ExtentKey,
        kept_slots: Option<&BTreeSet<u16>>,
    ) -> Result<Rows> {
        self.read(key, kept_slots)
            .await?
            .ok_or_else(|| invalid("listed immutable extent disappeared"))
    }
}

/// The rows of every extent that can hold one of `ids`, or of every extent. An
/// `hb1` id names its block's one object, fetched first; when it is absent or
/// lacks the id, the block LIST gives each slot `COPIES_READ` narrowest extents.
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
    };
    let mut found = Vec::new();
    let Some(ids) = ids else {
        for key in list_extents(&selection.store, &selection.base, None).await? {
            found.push(selection.listed(&key, None).await?);
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
        let direct = selection.read(&full, None).await?;
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
        let kept_slots = (!lineage_only || copies_overlap).then_some(&slots);
        for key in covering {
            if key != full || direct.is_none() {
                found.push(selection.listed(&key, kept_slots).await?);
            }
        }
        found.extend(direct.map(requested).transpose()?);
    }
    for id in singletons {
        let key = ExtentKey::Singleton(singleton_digest(id));
        if let Some(rows) = selection.read(&key, None).await? {
            if rows
                .commit(0)
                .is_none_or(|commit| commit.graph_commit_id != id)
            {
                return Err(invalid(
                    "singleton returned a different requested commit ID",
                ));
            }
            found.push(rows);
        }
    }
    Ok(found)
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

    const FOOTER_BYTES: usize = 40;

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
}
