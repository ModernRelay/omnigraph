//! `load csv`: one CSV object, captured once under the graph's external Blob
//! policy, typed by the `.pg` schema, normalized into the per-type Arrow
//! batches every strict load hands to the load transaction. The reader
//! replaces only the JSON normalization step; admission, staging, validation
//! and the branch-owned `__manifest` publication are the loader's, unchanged.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Float32Array, Float64Array, RecordBatch,
    StringArray, new_null_array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::csv::reader::Format;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::{Catalog, EdgeType, NodeType};
use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
use omnigraph_compiler::types::{PropType, ScalarType};

use super::{
    LoadMode, canonical_key_id, edge_key_columns, generate_id, parse_date32_literal,
    parse_date64_literal,
};
use crate::db::Omnigraph;
use crate::error::{OmniError, Result};
use crate::storage_layer::KEYED_WRITE_MAX_ROWS;
use crate::table_store::{CapturedSource, ExternalSource};

/// The resource a source over the session's `load_source_max_bytes` is
/// refused under, before any payload byte is read.
pub const CSV_SOURCE_BYTES_RESOURCE: &str = "csv_source_bytes";

/// Reading the source object completes within this, or the load is refused
/// with the source named: a slow or stalled source cannot hold a write open.
pub const CSV_SOURCE_READ_DEADLINE: Duration = Duration::from_secs(120);

/// Records per reader batch in overwrite; each batch is one staged-write
/// input.
pub(crate) const CSV_READ_BATCH_ROWS: usize = 8192;

/// The least the source operator reads for the header: the first record
/// must end inside the larger of this and one slice, or the source is
/// refused.
pub(crate) const CSV_HEAD_MIN_BYTES: u64 = 64 * 1024;

/// One `load csv` statement, resolved to the loader's vocabulary.
#[derive(Debug, Clone)]
pub struct CsvLoadRequest {
    pub source: String,
    pub target: LoadTarget,
    pub mode: LoadMode,
    pub delimiter: u8,
    pub header: bool,
    pub columns: Option<Vec<String>>,
}

impl CsvLoadRequest {
    pub fn from_statement(stmt: &LoadStmt) -> Self {
        Self {
            source: stmt.source.clone(),
            target: stmt.target.clone(),
            mode: match stmt.mode {
                LoadStmtMode::Overwrite => LoadMode::Overwrite,
                LoadStmtMode::Append => LoadMode::Append,
                LoadStmtMode::Merge => LoadMode::Merge,
            },
            delimiter: stmt.delimiter,
            header: stmt.header,
            columns: stmt.columns.clone(),
        }
    }

    /// The statement as the grammar reads it back: `LoadStmt::render`'s text.
    pub fn render(&self) -> String {
        LoadStmt {
            source: self.source.clone(),
            target: self.target.clone(),
            mode: match self.mode {
                LoadMode::Overwrite => LoadStmtMode::Overwrite,
                LoadMode::Append => LoadStmtMode::Append,
                LoadMode::Merge => LoadStmtMode::Merge,
            },
            delimiter: self.delimiter,
            header: self.header,
            columns: self.columns.clone(),
        }
        .render()
    }

    /// The columns the statement maps, without reading the source: the
    /// spelled `columns`, else every declared property of the target plus an
    /// edge file's two endpoints. A target absent from `catalog` counts no
    /// property.
    pub fn mapped_column_count(&self, catalog: &Catalog) -> usize {
        if let Some(columns) = &self.columns {
            return columns.len();
        }
        match &self.target {
            LoadTarget::Node(name) => catalog
                .node_types
                .get(name)
                .map_or(0, |node_type| node_type.properties.len()),
            LoadTarget::Edge(name) => catalog
                .edge_types
                .get(name)
                .map_or(0, |edge_type| edge_type.properties.len())
                .saturating_add(2),
        }
    }
}

/// The typed batches a CSV load stages, in source order: their table key,
/// the type name the receipt counts under, whether they are node or edge
/// batches, and the record count across them.
#[derive(Debug)]
pub(crate) struct PreparedCsvBatch {
    pub(crate) type_name: String,
    pub(crate) table_key: String,
    pub(crate) is_edge: bool,
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) rows: usize,
}

/// The strict batch shape the target's delta carries: the type's own Arrow
/// schema, which every normalized batch and the plan's source node declare.
pub(crate) fn target_arrow_schema(catalog: &Catalog, target: &LoadTarget) -> Result<SchemaRef> {
    match target {
        LoadTarget::Node(name) => catalog
            .node_types
            .get(name)
            .map(|node_type| Arc::clone(&node_type.arrow_schema))
            .ok_or_else(|| OmniError::manifest(format!("load csv: unknown node type '{name}'"))),
        LoadTarget::Edge(name) => catalog
            .edge_types
            .get(name)
            .map(|edge_type| Arc::clone(&edge_type.arrow_schema))
            .ok_or_else(|| OmniError::manifest(format!("load csv: unknown edge type '{name}'"))),
    }
}

/// What one CSV column feeds: an endpoint of an edge file or a declared
/// property, with the property's declared type for the typed read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ColumnRole {
    Src,
    Dst,
    Property { name: String, prop: PropType },
}

/// Capture the source object once per statement: authorized by the graph's
/// external Blob policy, bounded by `source_max_bytes` before any payload
/// byte is read, read within `CSV_SOURCE_READ_DEADLINE` as one response
/// bound to the version the probe reported. Every attempt of the statement
/// parses this capture; none fetches again.
pub(super) async fn capture_csv_source(
    bytes: &CsvBytes,
    request: &CsvLoadRequest,
    source_max_bytes: u64,
    source_charge: Arc<crate::engine::LoadCharge>,
) -> Result<CapturedSource> {
    let mut capture = match bytes {
        #[cfg(test)]
        CsvBytes::Memory(bytes) => {
            source_charge.grow(bytes.len() as u64)?;
            CapturedSource {
                bytes: Arc::clone(bytes),
                admission: None,
                size: bytes.len() as u64,
                e_tag: None,
                version: None,
            }
        }
        CsvBytes::Object(source) => {
            let captured = source_read_deadline(
                request,
                source.read_whole(source_max_bytes, CSV_SOURCE_BYTES_RESOURCE, |size| {
                    source_charge.grow(size.saturating_mul(2))
                }),
            )
            .await?;
            source_charge.shrink_to(captured.size);
            captured
        }
    };
    capture.admission = Some(source_charge);
    tracing::debug!(
        source = %request.source,
        bytes = capture.size,
        e_tag = ?capture.e_tag,
        version = ?capture.version,
        "load csv: captured the source"
    );
    Ok(capture)
}

/// The bytes one `load csv` statement reads: the probed external object, read
/// by byte range under its validators, or, for the operator's unit tests,
/// bytes already in memory.
#[derive(Debug, Clone)]
pub(crate) enum CsvBytes {
    Object(ExternalSource),
    #[cfg(test)]
    Memory(Arc<[u8]>),
}

impl CsvBytes {
    pub(crate) fn size(&self) -> u64 {
        match self {
            Self::Object(source) => source.size(),
            #[cfg(test)]
            Self::Memory(bytes) => bytes.len() as u64,
        }
    }

    /// `range` of the source, read within [`CSV_SOURCE_READ_DEADLINE`].
    pub(crate) async fn read(
        &self,
        request: &CsvLoadRequest,
        range: Range<u64>,
    ) -> Result<Arc<[u8]>> {
        match self {
            Self::Object(source) => source_read_deadline(request, source.read_range(range)).await,
            #[cfg(test)]
            Self::Memory(bytes) => {
                let start = usize::try_from(range.start).unwrap_or(usize::MAX);
                let end = usize::try_from(range.end).unwrap_or(usize::MAX);
                bytes.get(start..end).map(Arc::<[u8]>::from).ok_or_else(|| {
                    OmniError::external_blob_source(
                        &request.source,
                        format!("range {range:?} exceeds the source's {} bytes", bytes.len()),
                    )
                })
            }
        }
    }
}

async fn source_read_deadline<T>(
    request: &CsvLoadRequest,
    read: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(CSV_SOURCE_READ_DEADLINE, read)
        .await
        .map_err(|_| {
            OmniError::external_blob_source(
                &request.source,
                format!(
                    "load csv: reading the source exceeded {} seconds",
                    CSV_SOURCE_READ_DEADLINE.as_secs()
                ),
            )
        })?
}

/// The source probed and not read: the policy authorization, the `HEAD` and
/// the `source_max_bytes` refusal, within [`CSV_SOURCE_READ_DEADLINE`]; the
/// plan is built from the size it reports and every byte is read later, by
/// range, under the validators it reports.
pub(crate) async fn probe_csv_source(
    db: &Omnigraph,
    request: &CsvLoadRequest,
    source_max_bytes: u64,
) -> Result<CsvBytes> {
    let source = tokio::time::timeout(
        CSV_SOURCE_READ_DEADLINE,
        db.blob_materializer().external_source(
            &request.source,
            source_max_bytes,
            CSV_SOURCE_BYTES_RESOURCE,
        ),
    )
    .await
    .map_err(|_| {
        OmniError::external_blob_source(
            &request.source,
            format!(
                "load csv: probing the source exceeded {} seconds",
                CSV_SOURCE_READ_DEADLINE.as_secs()
            ),
        )
    })??;
    tracing::debug!(
        source = %request.source,
        bytes = source.size(),
        uri = source.uri(),
        "load csv: probed the source"
    );
    Ok(CsvBytes::Object(source))
}

/// The source's size from its metadata probe alone, for `explain load csv`.
pub(crate) async fn probe_csv_source_size(
    db: &Omnigraph,
    request: &CsvLoadRequest,
    source_max_bytes: u64,
) -> Result<u64> {
    Ok(probe_csv_source(db, request, source_max_bytes)
        .await?
        .size())
}

/// Resolve the target, the mapping and the supported kinds against the
/// transaction's catalog, parse the capture typed, and normalize every
/// reader batch into the strict batch shape. Every refusal names what it
/// refuses; nothing is staged on any error.
pub(crate) fn prepare_csv_batches(
    catalog: &Catalog,
    request: &CsvLoadRequest,
    capture: &CapturedSource,
    admit: &mut dyn FnMut(u64) -> Result<()>,
) -> Result<PreparedCsvBatch> {
    let bytes = capture.bytes.as_ref();
    let resolved = resolve_csv_columns(catalog, request, bytes)?;
    let table_key = resolved.table_key;
    let shape = CsvShape::count(bytes, request);
    admit(shape.reader_bytes(bytes.len()))?;
    let read = parse_records(bytes, request, resolved.read_schema, &table_key)?;
    let rows: usize = read.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return Err(OmniError::manifest(format!(
            "load csv into {table_key}: the source holds no records"
        )));
    }

    let typing = CsvTyping {
        catalog,
        target: &request.target,
        table_key: &table_key,
        properties: resolved.properties,
        arrow_schema: &resolved.arrow_schema,
        roles: &resolved.roles,
        is_edge: resolved.is_edge,
    };
    admit(typing.normalization_bytes(&read)?)?;
    let batches = typing.normalize_batches(&read)?;
    Ok(PreparedCsvBatch {
        type_name: request.target.type_name().to_string(),
        table_key,
        is_edge: resolved.is_edge,
        batches,
        rows,
    })
}

/// The columns of one CSV statement resolved against the catalog: the
/// target's properties and strict schema, each column's role and the read
/// schema the typed reader parses into.
struct ResolvedCsvColumns<'a> {
    table_key: String,
    properties: &'a HashMap<String, PropType>,
    arrow_schema: SchemaRef,
    is_edge: bool,
    roles: Vec<ColumnRole>,
    read_schema: SchemaRef,
}

/// Resolve the target, the mapping and the supported kinds against the
/// catalog from the header alone: every refusal names what it refuses.
fn resolve_csv_columns<'a>(
    catalog: &'a Catalog,
    request: &CsvLoadRequest,
    bytes: &[u8],
) -> Result<ResolvedCsvColumns<'a>> {
    let table_key = request.target.table_key();
    let (properties, arrow_schema, is_edge) = match &request.target {
        LoadTarget::Node(name) => {
            let node_type = catalog.node_types.get(name).ok_or_else(|| {
                OmniError::manifest(format!("load csv: unknown node type '{name}'"))
            })?;
            (
                &node_type.properties,
                Arc::clone(&node_type.arrow_schema),
                false,
            )
        }
        LoadTarget::Edge(name) => {
            let edge_type = catalog.edge_types.get(name).ok_or_else(|| {
                OmniError::manifest(format!("load csv: unknown edge type '{name}'"))
            })?;
            (
                &edge_type.properties,
                Arc::clone(&edge_type.arrow_schema),
                true,
            )
        }
    };
    if let Some(columns) = &request.columns {
        resolve_roles(columns, properties, is_edge, &table_key)?;
    }

    let format = Format::default()
        .with_header(request.header)
        .with_delimiter(request.delimiter);
    let (file_shape, _) = format
        .infer_schema(Cursor::new(bytes), Some(0))
        .map_err(|error| csv_parse_error(&request.source, &table_key, error))?;
    if file_shape.fields().is_empty() {
        return Err(OmniError::manifest(format!(
            "load csv into {table_key}: the source holds no columns"
        )));
    }
    let names: Vec<String> = match &request.columns {
        Some(columns) => columns.clone(),
        None => file_shape
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| header_role_name(index, field.name(), is_edge))
            .collect(),
    };
    if names.len() != file_shape.fields().len() {
        return Err(OmniError::manifest(format!(
            "load csv into {table_key}: `columns` names {} columns, the source holds {}",
            names.len(),
            file_shape.fields().len()
        )));
    }
    let roles = resolve_roles(&names, properties, is_edge, &table_key)?;

    let read_schema = Arc::new(Schema::new(
        roles
            .iter()
            .zip(&names)
            .map(|(role, name)| Field::new(name, read_data_type(role), true))
            .collect::<Vec<_>>(),
    ));
    Ok(ResolvedCsvColumns {
        table_key,
        properties,
        arrow_schema,
        is_edge,
        roles,
        read_schema,
    })
}

/// Where a `csv_core` pass over one buffer stands after its next step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordStep {
    /// A record ended: `end` is the byte after its terminator, the line feed
    /// of a CRLF included.
    Record { end: usize },
    /// The buffer ended inside a record; more bytes decide where it ends.
    NeedMore,
    /// No record remains in the buffer.
    End,
}

/// A `csv_core` pass over one growing buffer from the unquoted state, one
/// record at a time, so a cut never falls inside a quoted field.
struct RecordEnds {
    delimiter: u8,
    reader: csv_core::Reader,
    scanned: usize,
    cr_pending: bool,
}

impl RecordEnds {
    fn new(delimiter: u8) -> Self {
        Self {
            delimiter,
            reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            scanned: 0,
            cr_pending: false,
        }
    }

    /// Advance over `buffer[scanned..]` to the next record end; `at_eof`
    /// closes an unterminated last record.
    fn next(&mut self, buffer: &[u8], at_eof: bool) -> RecordStep {
        let mut output = [0; 4096];
        let mut ends = [0; 128];
        loop {
            if self.cr_pending {
                if self.scanned < buffer.len() {
                    if buffer[self.scanned] == b'\n' {
                        self.scanned += 1;
                    }
                } else if !at_eof {
                    return RecordStep::NeedMore;
                }
                self.cr_pending = false;
                return RecordStep::Record { end: self.scanned };
            }
            let input = &buffer[self.scanned..];
            if input.is_empty() && !at_eof {
                return RecordStep::NeedMore;
            }
            let (result, consumed, _, _) = self.reader.read_record(input, &mut output, &mut ends);
            self.scanned += consumed;
            match result {
                csv_core::ReadRecordResult::Record => {
                    if buffer[..self.scanned].ends_with(b"\r") {
                        self.cr_pending = true;
                        continue;
                    }
                    return RecordStep::Record { end: self.scanned };
                }
                csv_core::ReadRecordResult::InputEmpty if !at_eof || !input.is_empty() => {
                    if at_eof {
                        continue;
                    }
                    return RecordStep::NeedMore;
                }
                csv_core::ReadRecordResult::InputEmpty | csv_core::ReadRecordResult::End => {
                    return RecordStep::End;
                }
                csv_core::ReadRecordResult::OutputFull
                | csv_core::ReadRecordResult::OutputEndsFull => {}
            }
        }
    }

    /// The buffer lost its first `bytes`, which held only complete records.
    fn rebase(&mut self, bytes: usize) {
        self.scanned -= bytes;
    }
}

/// What one slice of a partition's range held: its bytes, cut on record
/// ends, their count, and the time spent waiting for them.
pub(crate) struct ScannedSlice {
    pub(crate) body: Vec<u8>,
    pub(crate) rows: usize,
    pub(crate) read_ns: u64,
}

/// One partition's walk over its byte range of the source: pieces of
/// `slice_bytes` read on demand, records cut on their ends, the partition
/// owning every record that starts before `range_end`. A partition after
/// the first begins at the first record end at or past its range start; the
/// one before it stops at the same end, so the two agree unless the range
/// start fell inside a quoted field, which the breaker detects.
pub(crate) struct RangeScanner {
    bytes: Arc<CsvBytes>,
    size: u64,
    range_start: u64,
    range_end: u64,
    slice_bytes: usize,
    align: bool,
    base: u64,
    buf: Vec<u8>,
    at_eof: bool,
    ends: RecordEnds,
    aligned: bool,
    start: u64,
    end: Option<u64>,
    records: usize,
    last_end: usize,
}

impl RangeScanner {
    /// `range_start..range_end` is the partition's share of the source's
    /// records; `align` is false only for the partition whose range begins
    /// at the records' start.
    pub(crate) fn new(
        bytes: Arc<CsvBytes>,
        delimiter: u8,
        range_start: u64,
        range_end: u64,
        slice_bytes: usize,
        align: bool,
    ) -> Self {
        let size = bytes.size();
        let align = align && range_start > 0;
        let base = if align { range_start - 1 } else { range_start };
        Self {
            bytes,
            size,
            range_start,
            range_end,
            slice_bytes: slice_bytes.max(1),
            align,
            base,
            buf: Vec::new(),
            at_eof: base >= size,
            ends: RecordEnds::new(delimiter),
            aligned: false,
            start: range_start,
            end: None,
            records: 0,
            last_end: 0,
        }
    }

    /// The source offset of the partition's first record, once aligned.
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    /// The source offset after the partition's last record, once it ended.
    pub(crate) fn end(&self) -> Option<u64> {
        self.end
    }

    /// The bytes held for records not yet cut.
    pub(crate) fn buffered(&self) -> usize {
        self.buf.len()
    }

    async fn fetch(
        &mut self,
        request: &CsvLoadRequest,
        admit: &mut (dyn FnMut(usize) -> Result<()> + Send),
        read_ns: &mut u64,
    ) -> Result<()> {
        let from = self.base + self.buf.len() as u64;
        if from >= self.size {
            self.at_eof = true;
            return Ok(());
        }
        let to = from.saturating_add(self.slice_bytes as u64).min(self.size);
        let waited = Instant::now();
        let piece = self.bytes.read(request, from..to).await?;
        *read_ns =
            read_ns.saturating_add(u64::try_from(waited.elapsed().as_nanos()).unwrap_or(u64::MAX));
        admit(piece.len())?;
        self.buf.extend_from_slice(&piece);
        if to >= self.size {
            self.at_eof = true;
        }
        Ok(())
    }

    /// The partition's first record begins at the first line end at or past
    /// the range start, read without regard to quotes; the partition before
    /// stops at the same byte unless it lies inside a quoted field.
    async fn align(
        &mut self,
        request: &CsvLoadRequest,
        admit: &mut (dyn FnMut(usize) -> Result<()> + Send),
        read_ns: &mut u64,
    ) -> Result<()> {
        let start = if self.range_start >= self.size {
            self.size
        } else if !self.align {
            self.range_start
        } else {
            let mut after = 1_usize;
            loop {
                if after >= self.buf.len() {
                    if self.at_eof {
                        break self.size;
                    }
                    self.fetch(request, admit, read_ns).await?;
                    continue;
                }
                match self.buf[after - 1] {
                    b'\n' => break self.base + after as u64,
                    b'\r' if self.buf[after] != b'\n' => break self.base + after as u64,
                    _ => after += 1,
                }
            }
        };
        let skip = usize::try_from(start.saturating_sub(self.base)).unwrap_or(usize::MAX);
        if skip >= self.buf.len() {
            self.buf.clear();
        } else {
            self.buf.drain(..skip);
        }
        self.base = start;
        self.ends = RecordEnds::new(self.ends.delimiter);
        self.start = start;
        self.aligned = true;
        if start >= self.range_end || start >= self.size {
            self.end = Some(start);
        }
        Ok(())
    }

    /// The next slice of the partition's records, or `None` once its range
    /// is exhausted; `admit` is called with every piece of the source read.
    pub(crate) async fn next_slice(
        &mut self,
        request: &CsvLoadRequest,
        admit: &mut (dyn FnMut(usize) -> Result<()> + Send),
    ) -> Result<Option<ScannedSlice>> {
        let mut read_ns = 0;
        if !self.aligned {
            self.align(request, admit, &mut read_ns).await?;
        }
        if self.end.is_some() {
            return Ok(None);
        }
        loop {
            match self.ends.next(&self.buf, self.at_eof) {
                RecordStep::Record { end } => {
                    self.records += 1;
                    self.last_end = end;
                    if self.base + end as u64 >= self.range_end {
                        self.end = Some(self.base + end as u64);
                        break;
                    }
                    if end >= self.slice_bytes {
                        break;
                    }
                }
                RecordStep::NeedMore => self.fetch(request, admit, &mut read_ns).await?,
                RecordStep::End => {
                    self.end = Some(self.size);
                    break;
                }
            }
        }
        if self.records == 0 {
            return Ok(None);
        }
        let mut body = std::mem::take(&mut self.buf);
        self.buf = body.split_off(self.last_end);
        self.base += self.last_end as u64;
        self.ends.rebase(self.last_end);
        let rows = self.records;
        self.records = 0;
        self.last_end = 0;
        Ok(Some(ScannedSlice {
            body,
            rows,
            read_ns,
        }))
    }
}

/// Where whole-source normalization refuses a record, in the order
/// [`CsvTyping::normalize_batches_from`] meets failures: `step` counts the
/// from endpoints, the to endpoints, then each property's conversion and its
/// non-null check, then the ids; `record` is the 1-based source record. The
/// lowest step over every slice, then the lowest record, is the refusal the
/// whole-capture path raises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NormalizeStep {
    pub(crate) step: usize,
    pub(crate) record: usize,
}

impl NormalizeStep {
    /// The step of property `index`'s conversion, or of its non-null check.
    fn property(index: usize, non_null: bool) -> usize {
        2 + 2 * index + usize::from(non_null)
    }
}

/// A record [`CsvTyping::normalize_batches_from`] refused: where it ranks
/// and the refusal, boxed to keep the `Err` variant small.
#[derive(Debug)]
pub(crate) struct NormalizeFailure {
    pub(crate) at: NormalizeStep,
    pub(crate) error: Box<OmniError>,
}

/// Why [`ResolvedCsv::parse_slice`] refused a slice.
#[derive(Debug)]
pub(crate) enum SliceRefusal {
    /// The reader, an admission or the slice itself: the load ends on it,
    /// the lowest refused slice named, as the whole-capture path reads the
    /// source to its end before any record is normalized.
    Parse(Box<OmniError>),
    /// A record failed normalization: the other slices are still normalized
    /// and the earliest [`NormalizeStep`] over all of them is named.
    Normalize(NormalizeFailure),
}

impl SliceRefusal {
    #[cfg(test)]
    pub(crate) fn into_error(self) -> OmniError {
        match self {
            Self::Parse(error) => *error,
            Self::Normalize(failure) => *failure.error,
        }
    }
}

impl From<OmniError> for SliceRefusal {
    fn from(error: OmniError) -> Self {
        Self::Parse(Box::new(error))
    }
}

impl From<NormalizeFailure> for SliceRefusal {
    fn from(failure: NormalizeFailure) -> Self {
        Self::Normalize(failure)
    }
}

/// The source resolved once for the engine's source operator from its first
/// bytes: the columns, the read schema and where its records begin; every
/// refusal the header can raise is raised here, before any range is read.
#[derive(Debug)]
pub(crate) struct ResolvedCsv {
    table_key: String,
    arrow_schema: SchemaRef,
    is_edge: bool,
    roles: Vec<ColumnRole>,
    read_schema: SchemaRef,
    columns: usize,
    records_start: u64,
}

impl ResolvedCsv {
    /// `head` is the source's first bytes and `size` its size: the first
    /// record (the header, when the statement reads one) must end inside
    /// `head`, or the source is refused.
    pub(crate) fn resolve(
        catalog: &Catalog,
        request: &CsvLoadRequest,
        head: &[u8],
        size: u64,
    ) -> Result<Self> {
        let at_eof = head.len() as u64 >= size;
        let first_end = match RecordEnds::new(request.delimiter).next(head, at_eof) {
            RecordStep::Record { end } => end,
            RecordStep::End => head.len(),
            RecordStep::NeedMore => {
                return Err(OmniError::manifest(format!(
                    "load csv into {}: the first record of the source exceeds {} bytes",
                    request.target.table_key(),
                    head.len()
                )));
            }
        };
        let resolved = resolve_csv_columns(catalog, request, &head[..first_end])?;
        Ok(Self {
            table_key: resolved.table_key,
            arrow_schema: resolved.arrow_schema,
            is_edge: resolved.is_edge,
            roles: resolved.roles,
            columns: resolved.read_schema.fields().len(),
            read_schema: resolved.read_schema,
            records_start: if request.header { first_end as u64 } else { 0 },
        })
    }

    /// The source offset of the first record: after the header, when read.
    pub(crate) fn records_start(&self) -> u64 {
        self.records_start
    }

    pub(crate) fn table_key(&self) -> &str {
        &self.table_key
    }

    /// Parse and normalize one slice of `rows` records, admitting the read
    /// and the normalization as the whole-capture path does, every record
    /// numbered from `first_record`.
    pub(crate) fn parse_slice(
        &self,
        catalog: &Catalog,
        request: &CsvLoadRequest,
        body: &[u8],
        rows: usize,
        first_record: usize,
        admit: &mut dyn FnMut(u64) -> Result<()>,
    ) -> std::result::Result<Vec<RecordBatch>, SliceRefusal> {
        let slice = CsvShape {
            rows,
            columns: self.columns,
        };
        admit(slice.reader_bytes(body.len()))?;
        let line_offset = first_record - 1 + usize::from(request.header);
        let parse_error = |error| {
            csv_parse_error(
                &request.source,
                &self.table_key,
                offset_arrow_lines(error, line_offset),
            )
        };
        let reader = ReaderBuilder::new(Arc::clone(&self.read_schema))
            .with_header(false)
            .with_delimiter(request.delimiter)
            .with_batch_size(CSV_READ_BATCH_ROWS.min(slice.rows.max(1)))
            .build(Cursor::new(body))
            .map_err(parse_error)?;
        let read = reader
            .map(|batch| batch.map_err(parse_error))
            .collect::<Result<Vec<RecordBatch>>>()?;
        let read_rows: usize = read.iter().map(RecordBatch::num_rows).sum();
        if read_rows != slice.rows {
            return Err(OmniError::manifest_internal(format!(
                "load csv into {}: a slice counted {} records and read {read_rows}",
                self.table_key, slice.rows
            ))
            .into());
        }
        let properties = match &request.target {
            LoadTarget::Node(name) => &catalog.node_types[name].properties,
            LoadTarget::Edge(name) => &catalog.edge_types[name].properties,
        };
        let typing = CsvTyping {
            catalog,
            target: &request.target,
            table_key: &self.table_key,
            properties,
            arrow_schema: &self.arrow_schema,
            roles: &self.roles,
            is_edge: self.is_edge,
        };
        admit(typing.normalization_bytes(&read)?)?;
        Ok(typing.normalize_batches_from(&read, first_record)?)
    }
}

/// The resolved typing one CSV load applies to every reader batch.
struct CsvTyping<'a> {
    catalog: &'a Catalog,
    target: &'a LoadTarget,
    table_key: &'a str,
    properties: &'a HashMap<String, PropType>,
    arrow_schema: &'a SchemaRef,
    roles: &'a [ColumnRole],
    is_edge: bool,
}

struct CsvShape {
    rows: usize,
    columns: usize,
}

impl CsvShape {
    /// One quote-aware pass over a whole capture: its records after the
    /// header, when read, and the fields of its first record.
    fn count(mut bytes: &[u8], request: &CsvLoadRequest) -> Self {
        let mut reader = csv_core::ReaderBuilder::new()
            .delimiter(request.delimiter)
            .build();
        let mut output = [0; 4096];
        let mut ends = [0; 128];
        let mut records = 0_usize;
        let mut columns = 0_usize;
        let mut fields = 0_usize;
        loop {
            let (result, consumed, _, count) = reader.read_record(bytes, &mut output, &mut ends);
            bytes = &bytes[consumed..];
            fields = fields.saturating_add(count);
            match result {
                csv_core::ReadRecordResult::Record => {
                    if records == 0 {
                        columns = fields;
                    }
                    records = records.saturating_add(1);
                    fields = 0;
                }
                csv_core::ReadRecordResult::End => break,
                _ => {}
            }
        }
        Self {
            rows: records.saturating_sub(usize::from(request.header && records > 0)),
            columns,
        }
    }

    /// Covers Arrow's record offsets, growing text buffer, parsed arrays,
    /// header inference and batch metadata before any of them is allocated.
    fn reader_bytes(&self, source_bytes: usize) -> u64 {
        let rows = self.rows as u64;
        let columns = self.columns as u64;
        (source_bytes as u64)
            .saturating_mul(4)
            .saturating_add(rows.saturating_mul(columns).saturating_mul(32))
            .saturating_add(
                rows.div_ceil(CSV_READ_BATCH_ROWS as u64)
                    .max(1)
                    .saturating_mul(columns)
                    .saturating_mul(2048),
            )
            .saturating_add(8192)
    }
}

struct NormalizedCsvBatch {
    first_record: usize,
    rows: usize,
    endpoints: Vec<ArrayRef>,
    properties: Vec<ArrayRef>,
}

impl CsvTyping<'_> {
    /// Admits typed null arrays, copied text and canonical IDs, including
    /// temporary key strings and the final Arrow copies, before normalization.
    fn normalization_bytes(&self, read: &[RecordBatch]) -> Result<u64> {
        let rows = read
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        let batch_rows = read.iter().map(RecordBatch::num_rows).max().unwrap_or(0) as u64;
        let mut bytes = batch_rows.saturating_mul(128).saturating_add(
            (read.len() as u64)
                .saturating_mul(self.arrow_schema.fields().len() as u64)
                .saturating_mul(2048),
        );
        for field in self.arrow_schema.fields() {
            let width = super::projected_strict_field_bytes(
                field,
                &serde_json::Value::Null,
                self.properties
                    .get(field.name())
                    .is_some_and(|prop| prop.scalar == ScalarType::Blob),
            )?;
            bytes = bytes.saturating_add(rows.saturating_mul(width).saturating_mul(2));
        }
        let keys = match self.target {
            LoadTarget::Node(name) => self.catalog.node_types[name].key.as_ref(),
            LoadTarget::Edge(name) => self.catalog.edge_types[name].key.as_ref(),
        };
        if keys.is_none() {
            bytes = bytes.saturating_add(rows.saturating_mul(64));
        }
        let mut key_scratch = 0_u64;
        for batch in read {
            let mut batch_key_scratch = 0_u64;
            for (role, column) in self.roles.iter().zip(batch.columns()) {
                let role_name = match role {
                    ColumnRole::Src => self.catalog.system_columns.src,
                    ColumnRole::Dst => self.catalog.system_columns.dst,
                    ColumnRole::Property { name, .. } => name.as_str(),
                };
                let keyed = keys.is_some_and(|keys| keys.iter().any(|key| key == role_name));
                if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                    bytes = bytes.saturating_add(
                        (strings.get_buffer_memory_size() as u64).saturating_mul(2),
                    );
                }
                if keyed {
                    let text_key = matches!(role, ColumnRole::Src | ColumnRole::Dst)
                        || matches!(role, ColumnRole::Property { prop, .. } if prop.scalar == ScalarType::String);
                    let key_bytes = if text_key {
                        let strings = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("a text key is read as Utf8");
                        strings.iter().flatten().fold(0_u64, |sum, text| {
                            text.bytes().fold(sum, |sum, byte| {
                                sum.saturating_add(match byte {
                                    0..=31 => 6,
                                    b'"' | b'\\' => 2,
                                    _ => 1,
                                })
                            })
                        })
                    } else {
                        (batch.num_rows() as u64).saturating_mul(384)
                    };
                    let key_bytes =
                        key_bytes.saturating_add((batch.num_rows() as u64).saturating_mul(32));
                    bytes = bytes.saturating_add(key_bytes.saturating_mul(2));
                    batch_key_scratch =
                        batch_key_scratch.saturating_add(key_bytes.saturating_mul(2));
                }
            }
            key_scratch = key_scratch.max(batch_key_scratch);
        }
        Ok(bytes.saturating_add(key_scratch))
    }

    fn normalize_batches(&self, read: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        self.normalize_batches_from(read, 1)
            .map_err(|failure| *failure.error)
    }

    /// [`Self::normalize_batches`] with the records numbered from
    /// `first_record`, a slice's first record in the source; the steps run in
    /// [`NormalizeStep`] order, so the first failure met is the lowest-ranked.
    fn normalize_batches_from(
        &self,
        read: &[RecordBatch],
        first_record: usize,
    ) -> std::result::Result<Vec<RecordBatch>, NormalizeFailure> {
        let table_key = self.table_key;
        let failed = |step: usize, record: usize, error: OmniError| NormalizeFailure {
            at: NormalizeStep { step, record },
            error: Box::new(error),
        };
        let by_role: HashMap<&ColumnRole, usize> = self
            .roles
            .iter()
            .enumerate()
            .map(|(index, role)| (role, index))
            .collect();
        let leading = if self.is_edge { 3 } else { 1 };
        let mut next_record = first_record;
        let mut normalized: Vec<NormalizedCsvBatch> = read
            .iter()
            .map(|batch| {
                let normalized = NormalizedCsvBatch {
                    first_record: next_record,
                    rows: batch.num_rows(),
                    endpoints: Vec::with_capacity(leading - 1),
                    properties: Vec::with_capacity(
                        self.arrow_schema.fields().len().saturating_sub(leading),
                    ),
                };
                next_record += batch.num_rows();
                normalized
            })
            .collect();
        if self.is_edge {
            for (step, (role, label)) in
                [(ColumnRole::Src, FROM_COLUMN), (ColumnRole::Dst, TO_COLUMN)]
                    .into_iter()
                    .enumerate()
            {
                let index = *by_role.get(&role).ok_or_else(|| {
                    failed(
                        step,
                        first_record,
                        OmniError::manifest_internal(format!("{label} column resolved twice")),
                    )
                })?;
                for (read, normalized) in read.iter().zip(&mut normalized) {
                    let strings = utf8_without_empty(read.column(index));
                    if let Some(row) = first_null(&strings) {
                        let record = normalized.first_record + row;
                        return Err(failed(
                            step,
                            record,
                            OmniError::manifest(format!(
                                "load csv into {table_key}: record {record} has an empty `{label}` endpoint"
                            )),
                        ));
                    }
                    normalized.endpoints.push(Arc::new(strings));
                }
            }
        }
        for (property, field) in self.arrow_schema.fields().iter().skip(leading).enumerate() {
            let convert = NormalizeStep::property(property, false);
            let prop = self.properties.get(field.name()).ok_or_else(|| {
                failed(
                    convert,
                    first_record,
                    OmniError::manifest_internal(format!(
                        "property '{}' of {table_key} is missing from its catalog entry",
                        field.name()
                    )),
                )
            })?;
            let role = ColumnRole::Property {
                name: field.name().to_string(),
                prop: prop.clone(),
            };
            let index = by_role.get(&role).copied();
            if index.is_none() && !field.is_nullable() {
                return Err(failed(
                    convert,
                    first_record,
                    OmniError::manifest(format!(
                        "load csv into {table_key}: property '{}' is not nullable and the source has no column for it",
                        field.name()
                    )),
                ));
            }
            for (read, normalized) in read.iter().zip(&mut normalized) {
                let column = match index {
                    Some(index) => convert_property(
                        read.column(index),
                        field.name(),
                        prop,
                        table_key,
                        normalized.first_record,
                    )
                    .map_err(|(row, error)| {
                        failed(convert, normalized.first_record + row, *error)
                    })?,
                    None => new_null_array(field.data_type(), normalized.rows),
                };
                normalized.properties.push(column);
            }
            if !field.is_nullable() {
                for normalized in &normalized {
                    let column = normalized
                        .properties
                        .last()
                        .expect("every batch has the current property");
                    if let Some(row) = first_null(column.as_ref()) {
                        let record = normalized.first_record + row;
                        return Err(failed(
                            NormalizeStep::property(property, true),
                            record,
                            OmniError::manifest(format!(
                                "load csv into {table_key}: record {record} has an empty field for non-nullable property '{}'",
                                field.name()
                            )),
                        ));
                    }
                }
            }
        }
        let ids_step = NormalizeStep::property(self.arrow_schema.fields().len() - leading, false);
        normalized
            .into_iter()
            .map(|normalized| {
                let ids = match self.target {
                    LoadTarget::Node(name) => node_ids(
                        &self.catalog.node_types[name],
                        self.arrow_schema,
                        &normalized.properties,
                        normalized.rows,
                        normalized.first_record,
                    ),
                    LoadTarget::Edge(name) => edge_ids(
                        &self.catalog.edge_types[name],
                        self.arrow_schema,
                        &normalized.endpoints[0],
                        &normalized.endpoints[1],
                        &normalized.properties,
                        self.catalog.system_columns,
                        normalized.rows,
                        normalized.first_record,
                    ),
                }
                .map_err(|(row, error)| failed(ids_step, normalized.first_record + row, *error))?;
                let mut columns: Vec<ArrayRef> =
                    Vec::with_capacity(self.arrow_schema.fields().len());
                columns.push(Arc::new(StringArray::from(ids)));
                columns.extend(normalized.endpoints);
                columns.extend(normalized.properties);
                RecordBatch::try_new(Arc::clone(self.arrow_schema), columns).map_err(|error| {
                    failed(
                        ids_step,
                        normalized.first_record,
                        OmniError::arrow_internal(error),
                    )
                })
            })
            .collect()
    }
}

/// The statement's names for an edge file's endpoint columns, fixed whatever
/// the graph's physical system column spelling is.
const FROM_COLUMN: &str = "from";
const TO_COLUMN: &str = "to";

/// The role name a header column carries: an edge file's first two columns
/// are its endpoints whatever the header calls them (`Person.id|Person.id`
/// in LDBC), every other header name is a property name.
fn header_role_name(index: usize, header: &str, is_edge: bool) -> String {
    match (is_edge, index) {
        (true, 0) => FROM_COLUMN.to_string(),
        (true, 1) => TO_COLUMN.to_string(),
        _ => header.to_string(),
    }
}

/// Every column name to its role, refusing an unknown property, a name used
/// twice, an endpoint on a node file, an edge file missing an endpoint, and
/// a property whose kind CSV cannot encode.
fn resolve_roles(
    names: &[String],
    properties: &HashMap<String, PropType>,
    is_edge: bool,
    table_key: &str,
) -> Result<Vec<ColumnRole>> {
    let mut roles: Vec<ColumnRole> = Vec::with_capacity(names.len());
    for name in names {
        let role = if name == FROM_COLUMN || name == TO_COLUMN {
            if !is_edge {
                return Err(OmniError::manifest(format!(
                    "load csv into {table_key}: column `{name}` names an endpoint, and {table_key} is a node type"
                )));
            }
            if name == FROM_COLUMN {
                ColumnRole::Src
            } else {
                ColumnRole::Dst
            }
        } else {
            let prop = properties.get(name).ok_or_else(|| {
                OmniError::manifest(format!(
                    "load csv into {table_key}: column `{name}` is not a declared property of {table_key}"
                ))
            })?;
            if let Some(kind) = unsupported_kind(prop) {
                return Err(OmniError::manifest(format!(
                    "load csv into {table_key}: property '{name}' is {kind}; CSV defines no encoding for it"
                )));
            }
            ColumnRole::Property {
                name: name.clone(),
                prop: prop.clone(),
            }
        };
        if roles.contains(&role) {
            return Err(OmniError::manifest(format!(
                "load csv into {table_key}: column `{name}` is mapped twice"
            )));
        }
        roles.push(role);
    }
    if is_edge {
        for (role, label) in [(ColumnRole::Src, FROM_COLUMN), (ColumnRole::Dst, TO_COLUMN)] {
            if !roles.contains(&role) {
                return Err(OmniError::manifest(format!(
                    "load csv into {table_key}: an edge file names its `{label}` endpoint column"
                )));
            }
        }
    }
    Ok(roles)
}

/// The kind name of a property CSV cannot carry, `None` for a supported one.
fn unsupported_kind(prop: &PropType) -> Option<&'static str> {
    if prop.list {
        return Some("a list");
    }
    match prop.scalar {
        ScalarType::Vector(_) => Some("a Vector"),
        ScalarType::Blob => Some("a Blob"),
        ScalarType::String
        | ScalarType::Bool
        | ScalarType::I32
        | ScalarType::I64
        | ScalarType::U32
        | ScalarType::U64
        | ScalarType::F32
        | ScalarType::F64
        | ScalarType::Date
        | ScalarType::DateTime => None,
    }
}

/// Numbers typed by the reader, everything else text: `Bool`, `Date` and
/// `DateTime` keep their spelling for the loader's own literal checks, which
/// Arrow's parsers would silently relax (sub-millisecond digits, a Date's time).
fn read_data_type(role: &ColumnRole) -> DataType {
    match role {
        ColumnRole::Src | ColumnRole::Dst => DataType::Utf8,
        ColumnRole::Property { prop, .. } => match prop.scalar {
            ScalarType::I32 => DataType::Int32,
            ScalarType::I64 => DataType::Int64,
            ScalarType::U32 => DataType::UInt32,
            ScalarType::U64 => DataType::UInt64,
            ScalarType::F32 => DataType::Float32,
            ScalarType::F64 => DataType::Float64,
            ScalarType::String
            | ScalarType::Bool
            | ScalarType::Date
            | ScalarType::DateTime
            | ScalarType::Vector(_)
            | ScalarType::Blob => DataType::Utf8,
        },
    }
}

/// The reader's batches of the read schema: `CSV_READ_BATCH_ROWS` records
/// each in overwrite; a keyed mode reads up to its row cap plus one and
/// refuses the record over the cap.
fn parse_records(
    bytes: &[u8],
    request: &CsvLoadRequest,
    schema: SchemaRef,
    table_key: &str,
) -> Result<Vec<RecordBatch>> {
    let parse_error = |error| csv_parse_error(&request.source, table_key, error);
    let row_limit = match request.mode {
        LoadMode::Append | LoadMode::Merge => Some(KEYED_WRITE_MAX_ROWS),
        LoadMode::Overwrite => None,
    };
    let batch_rows = row_limit
        .map_or(CSV_READ_BATCH_ROWS, |limit| limit + 1)
        .min(CsvShape::count(bytes, request).rows.max(1));
    let reader = ReaderBuilder::new(schema)
        .with_header(request.header)
        .with_delimiter(request.delimiter)
        .with_batch_size(batch_rows)
        .build(Cursor::new(bytes))
        .map_err(parse_error)?;
    let mut batches = Vec::new();
    let mut rows = 0_usize;
    for batch in reader {
        let batch = batch.map_err(parse_error)?;
        rows = rows.saturating_add(batch.num_rows());
        if let Some(limit) = row_limit
            && rows > limit
        {
            return Err(OmniError::resource_limit(
                format!("keyed entities for {table_key}"),
                limit as u64,
                rows as u64,
            ));
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn csv_parse_error(source: &str, table_key: &str, error: arrow_schema::ArrowError) -> OmniError {
    OmniError::manifest(format!(
        "load csv into {table_key} from '{source}': {error}"
    ))
}

/// Arrow numbers a parse error's `at line` from its reader's first line; a
/// slice's reader starts `offset` lines into the capture, so the number the
/// whole capture's reader would print is restored.
fn offset_arrow_lines(error: arrow_schema::ArrowError, offset: usize) -> arrow_schema::ArrowError {
    let renumber = |text: String| -> String {
        let Some(at) = text.find(" at line ") else {
            return text;
        };
        let digits = at + " at line ".len();
        let end = text[digits..]
            .find(|c: char| !c.is_ascii_digit())
            .map_or(text.len(), |n| digits + n);
        match text[digits..end].parse::<usize>() {
            Ok(line) => format!(
                "{}{}{}",
                &text[..digits],
                line.saturating_add(offset),
                &text[end..]
            ),
            Err(_) => text,
        }
    };
    match error {
        arrow_schema::ArrowError::ParseError(text) => {
            arrow_schema::ArrowError::ParseError(renumber(text))
        }
        arrow_schema::ArrowError::CsvError(text) => {
            arrow_schema::ArrowError::CsvError(renumber(text))
        }
        other => other,
    }
}

/// A read text column with every empty field as null: the CSV spelling of
/// null, since the format has no other.
fn utf8_without_empty(column: &ArrayRef) -> StringArray {
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a column read as Utf8 is a StringArray");
    strings
        .iter()
        .map(|value| value.filter(|value| !value.is_empty()))
        .collect()
}

/// The 0-based row of the first null, `None` when the column is dense.
fn first_null(column: &dyn Array) -> Option<usize> {
    (0..column.len()).find(|row| column.is_null(*row))
}

/// One read column to its declared Arrow type: numbers as read, text with
/// empty fields as null, `Bool` from exactly `true` or `false`, `Date` and
/// `DateTime` literal-checked cell by cell; a refusal names its 0-based row.
fn convert_property(
    column: &ArrayRef,
    name: &str,
    prop: &PropType,
    table_key: &str,
    first_record: usize,
) -> std::result::Result<ArrayRef, (usize, Box<OmniError>)> {
    let cell_error = |row: usize, message: String| {
        (
            row,
            Box::new(OmniError::manifest(format!(
                "load csv into {table_key}: record {}, property '{name}': {message}",
                first_record + row
            ))),
        )
    };
    Ok(match prop.scalar {
        ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64 => Arc::clone(column),
        ScalarType::F32 => {
            let values = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("a column read as Float32 is a Float32Array");
            if let Some(row) = values
                .iter()
                .position(|value| value.is_some_and(|value| !value.is_finite()))
            {
                return Err(cell_error(row, "expected finite Float32".to_string()));
            }
            Arc::clone(column)
        }
        ScalarType::F64 => {
            let values = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("a column read as Float64 is a Float64Array");
            if let Some(row) = values
                .iter()
                .position(|value| value.is_some_and(|value| !value.is_finite()))
            {
                return Err(cell_error(row, "expected finite Float64".to_string()));
            }
            Arc::clone(column)
        }
        ScalarType::String => Arc::new(utf8_without_empty(column)),
        ScalarType::Bool => {
            let strings = utf8_without_empty(column);
            let mut values = Vec::with_capacity(strings.len());
            for (row, value) in strings.iter().enumerate() {
                values.push(match value {
                    None => None,
                    Some("true") => Some(true),
                    Some("false") => Some(false),
                    Some(other) => {
                        return Err(cell_error(
                            row,
                            format!("'{other}' is not `true` or `false`"),
                        ));
                    }
                });
            }
            Arc::new(arrow_array::BooleanArray::from(values))
        }
        ScalarType::Date => {
            let strings = utf8_without_empty(column);
            let mut values = Vec::with_capacity(strings.len());
            for (row, value) in strings.iter().enumerate() {
                values.push(match value {
                    None => None,
                    Some(text) => Some(
                        parse_date32_literal(text)
                            .map_err(|error| cell_error(row, error.to_string()))?,
                    ),
                });
            }
            Arc::new(Date32Array::from(values))
        }
        ScalarType::DateTime => {
            let strings = utf8_without_empty(column);
            let mut values = Vec::with_capacity(strings.len());
            for (row, value) in strings.iter().enumerate() {
                values.push(match value {
                    None => None,
                    Some(text) => {
                        omnigraph_compiler::check_datetime_literal(text)
                            .map_err(|message| cell_error(row, message))?;
                        Some(
                            parse_date64_literal(text)
                                .map_err(|error| cell_error(row, error.to_string()))?,
                        )
                    }
                });
            }
            Arc::new(Date64Array::from(values))
        }
        ScalarType::Vector(_) | ScalarType::Blob => {
            return Err((
                0,
                Box::new(OmniError::manifest_internal(format!(
                    "property '{name}' of {table_key} passed the kind check with an unsupported kind"
                ))),
            ));
        }
    })
}

/// Node ids as the strict JSONL route derives them: the canonical rendering
/// of the `@key` columns, or a generated id for an unkeyed type.
fn node_ids(
    node_type: &NodeType,
    schema: &SchemaRef,
    property_columns: &[ArrayRef],
    rows: usize,
    first_record: usize,
) -> std::result::Result<Vec<String>, (usize, Box<OmniError>)> {
    let key_columns = node_type
        .key
        .as_ref()
        .map(|properties| {
            properties
                .iter()
                .map(|property| {
                    let schema_index = schema.index_of(property).map_err(|_| {
                        OmniError::manifest_internal(format!(
                            "@key property '{property}' is missing from node {} strict schema",
                            node_type.name
                        ))
                    })?;
                    property_columns
                        .get(schema_index.saturating_sub(1))
                        .cloned()
                        .ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "@key property '{property}' has an invalid strict schema position"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
        .map_err(|error| (0, Box::new(error)))?;
    let id = |row: usize| -> Result<String> {
        match &key_columns {
            Some(key_columns) => canonical_key_id(key_columns, row)?.ok_or_else(|| {
                OmniError::manifest(format!(
                    "load csv into node:{}: record {} is missing a non-null @key value",
                    node_type.name,
                    first_record + row
                ))
            }),
            None => Ok(generate_id()),
        }
    };
    (0..rows)
        .map(|row| id(row).map_err(|error| (row, Box::new(error))))
        .collect()
}

/// Edge ids as the strict JSONL route derives them, through the shared
/// `@key` column resolution over the endpoints and properties.
fn edge_ids(
    edge_type: &EdgeType,
    schema: &SchemaRef,
    src_column: &ArrayRef,
    dst_column: &ArrayRef,
    property_columns: &[ArrayRef],
    system_columns: SystemColumns,
    rows: usize,
    first_record: usize,
) -> std::result::Result<Vec<String>, (usize, Box<OmniError>)> {
    let key_columns = edge_key_columns(
        edge_type,
        schema,
        src_column,
        dst_column,
        property_columns,
        system_columns,
    )
    .map_err(|error| (0, Box::new(error)))?;
    let id = |row: usize| -> Result<String> {
        match &key_columns {
            Some(key_columns) => canonical_key_id(key_columns, row)?.ok_or_else(|| {
                OmniError::manifest(format!(
                    "load csv into edge:{}: record {} is missing a non-null @key value",
                    edge_type.name,
                    first_record + row
                ))
            }),
            None => Ok(generate_id()),
        }
    };
    (0..rows)
        .map(|row| id(row).map_err(|error| (row, Box::new(error))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::fmt::Write;

    fn catalog() -> Catalog {
        build_catalog(&parse_schema("node Person {\n  key: I64 @key\n  score: F64\n}\n").unwrap())
            .unwrap()
    }

    fn request(mode: LoadMode) -> CsvLoadRequest {
        CsvLoadRequest {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode,
            delimiter: b',',
            header: true,
            columns: None,
        }
    }

    fn key_source(records: usize) -> String {
        let mut source = String::from("key\n");
        for row in 0..records {
            writeln!(&mut source, "{row}").unwrap();
        }
        source
    }

    fn key_read_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]))
    }

    fn batch_rows(batches: &[RecordBatch]) -> Vec<usize> {
        batches.iter().map(RecordBatch::num_rows).collect()
    }

    #[test]
    fn overwrite_keeps_every_reader_batch_separate() {
        let source = key_source(CSV_READ_BATCH_ROWS + 1);
        let batches = parse_records(
            source.as_bytes(),
            &request(LoadMode::Overwrite),
            key_read_schema(),
            "node:Person",
        )
        .unwrap();
        assert_eq!(batch_rows(&batches), vec![CSV_READ_BATCH_ROWS, 1]);
    }

    #[test]
    fn keyed_modes_read_the_row_cap_as_one_batch_and_refuse_one_more() {
        for mode in [LoadMode::Append, LoadMode::Merge] {
            let at_cap = key_source(KEYED_WRITE_MAX_ROWS);
            let batches = parse_records(
                at_cap.as_bytes(),
                &request(mode),
                key_read_schema(),
                "node:Person",
            )
            .unwrap();
            assert_eq!(batch_rows(&batches), vec![KEYED_WRITE_MAX_ROWS]);

            let over_cap = key_source(KEYED_WRITE_MAX_ROWS + 1);
            let error = parse_records(
                over_cap.as_bytes(),
                &request(mode),
                key_read_schema(),
                "node:Person",
            )
            .unwrap_err();
            assert!(matches!(
                error,
                OmniError::ResourceLimitExceeded { ref resource, limit, actual }
                    if resource == "keyed entities for node:Person"
                        && limit == KEYED_WRITE_MAX_ROWS as u64
                        && actual == KEYED_WRITE_MAX_ROWS as u64 + 1
            ));
        }
    }

    #[test]
    fn normalized_batches_number_records_across_reader_batches() {
        let catalog = build_catalog(
            &parse_schema("node Person {\n  key: I64 @key\n  score: F64\n}\n").unwrap(),
        )
        .unwrap();
        let mut source = String::from("key,score\n");
        for row in 0..CSV_READ_BATCH_ROWS {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        source.push_str("8192,\n");
        let capture = CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        };
        let error = prepare_csv_batches(
            &catalog,
            &request(LoadMode::Overwrite),
            &capture,
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "load csv into node:Person: record 8193 has an empty field for non-nullable property 'score'",
            "the second reader batch keeps the source numbering"
        );

        source.truncate(source.len() - "8192,\n".len());
        source.push_str("8192,2.5\n");
        let capture = CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        };
        let prepared = prepare_csv_batches(
            &catalog,
            &request(LoadMode::Overwrite),
            &capture,
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(prepared.rows, CSV_READ_BATCH_ROWS + 1);
        assert_eq!(batch_rows(&prepared.batches), vec![CSV_READ_BATCH_ROWS, 1]);
        assert_eq!(prepared.table_key, "node:Person");
        assert!(!prepared.is_edge);
    }

    fn memory(source: &[u8]) -> Arc<CsvBytes> {
        Arc::new(CsvBytes::Memory(Arc::from(source)))
    }

    /// Every slice one partition over `range` cuts at `slice_bytes`, reading
    /// the source in pieces of the same size: `(body, rows)`.
    async fn slices(
        source: &[u8],
        request: &CsvLoadRequest,
        range: Range<u64>,
        slice_bytes: usize,
        align: bool,
    ) -> (RangeScanner, Vec<(Vec<u8>, usize)>) {
        let mut scanner = RangeScanner::new(
            memory(source),
            request.delimiter,
            range.start,
            range.end,
            slice_bytes,
            align,
        );
        let mut cut = Vec::new();
        while let Some(slice) = scanner.next_slice(request, &mut |_| Ok(())).await.unwrap() {
            cut.push((slice.body, slice.rows));
        }
        (scanner, cut)
    }

    /// A slice closes on the first record end at or past the slice size and
    /// the header belongs to none; a case sees one slice under 8 MiB.
    #[tokio::test]
    async fn scanner_cuts_slices_on_record_ends() {
        let mut source = String::from("key,score\n");
        for row in 0..10 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let bytes = source.as_bytes();
        let request = request(LoadMode::Overwrite);
        let resolved =
            ResolvedCsv::resolve(&catalog(), &request, bytes, bytes.len() as u64).unwrap();
        assert_eq!((resolved.records_start(), resolved.columns), (10, 2));
        let (scanner, cut) = slices(bytes, &request, 10..bytes.len() as u64, 8, false).await;
        assert_eq!(
            cut.iter()
                .map(|(body, rows)| (body.len(), *rows))
                .collect::<Vec<_>>(),
            vec![(12, 2); 5]
        );
        assert_eq!(cut[2].0, b"4,1.5\n5,1.5\n");
        assert_eq!(
            (scanner.start(), scanner.end()),
            (10, Some(bytes.len() as u64))
        );

        let (_, whole) = slices(bytes, &request, 10..bytes.len() as u64, usize::MAX, false).await;
        assert_eq!(whole.len(), 1);
        assert_eq!((whole[0].0.len(), whole[0].1), (bytes.len() - 10, 10));
    }

    #[tokio::test]
    async fn scanner_without_a_header_counts_the_first_line_as_a_record() {
        let bytes = b"0,1.5\n1,1.5\n2,1.5\n";
        let request = CsvLoadRequest {
            header: false,
            columns: Some(vec!["key".to_string(), "score".to_string()]),
            ..request(LoadMode::Overwrite)
        };
        let resolved =
            ResolvedCsv::resolve(&catalog(), &request, bytes, bytes.len() as u64).unwrap();
        assert_eq!(resolved.records_start(), 0);
        let (_, cut) = slices(bytes, &request, 0..18, 7, false).await;
        assert_eq!(
            cut.iter()
                .map(|(body, rows)| (body.len(), *rows))
                .collect::<Vec<_>>(),
            [(12, 2), (6, 1)]
        );
    }

    /// `csv_core` reports a CRLF record once it has read the `\r`; the cut
    /// takes the `\n` with it so no slice starts on a bare line feed, even
    /// when the `\n` arrives in the next piece read.
    #[tokio::test]
    async fn scanner_cuts_a_crlf_source_after_the_line_feed() {
        let bytes = b"key,score\r\n0,1.5\r\n1,1.5\r\n2,1.5\r\n";
        let request = request(LoadMode::Overwrite);
        let resolved =
            ResolvedCsv::resolve(&catalog(), &request, bytes, bytes.len() as u64).unwrap();
        assert_eq!(resolved.records_start(), 11);
        for slice_bytes in [1, 7] {
            let (_, cut) =
                slices(bytes, &request, 11..bytes.len() as u64, slice_bytes, false).await;
            assert_eq!(
                cut.iter()
                    .map(|(body, _)| body.as_slice())
                    .collect::<Vec<_>>(),
                [&b"0,1.5\r\n"[..], b"1,1.5\r\n", b"2,1.5\r\n"],
                "slice_bytes {slice_bytes}"
            );
        }
    }

    #[tokio::test]
    async fn scanner_never_cuts_inside_a_quoted_field() {
        let bytes = b"key,name\n1,\"a\nb\"\n2,c\n";
        let request = request(LoadMode::Overwrite);
        let (_, cut) = slices(bytes, &request, 9..bytes.len() as u64, 1, false).await;
        assert_eq!(
            cut.iter()
                .map(|(body, _)| body.as_slice())
                .collect::<Vec<_>>(),
            [&b"1,\"a\nb\"\n"[..], b"2,c\n"]
        );
    }

    /// Neighbouring partitions meet at one record end whatever byte the
    /// range boundary falls on: the record start, inside a record, between
    /// the `\r` and the `\n` of a CRLF.
    #[tokio::test]
    async fn neighbouring_partitions_meet_at_one_record_end() {
        for (source, records_start) in [
            (
                &b"key,score\n0,1.5\n10,1.5\n200,1.5\n3,1.5\n4,1.5\n"[..],
                10_u64,
            ),
            (
                b"key,score\r\n0,1.5\r\n10,1.5\r\n200,1.5\r\n3,1.5\r\n4,1.5\r\n",
                11,
            ),
            (b"key,score\n0,1.5\n10,1.5\n200,1.5\n3,1.5\n4,1.5", 10),
        ] {
            let request = request(LoadMode::Overwrite);
            let size = source.len() as u64;
            for boundary in records_start + 1..size {
                let (before, mut cut) =
                    slices(source, &request, records_start..boundary, 4, false).await;
                let (after, tail) = slices(source, &request, boundary..size, 4, true).await;
                assert_eq!(
                    before.end(),
                    Some(after.start()),
                    "boundary {boundary} of {source:?}"
                );
                cut.extend(tail);
                let rows: usize = cut.iter().map(|(_, rows)| rows).sum();
                assert_eq!(rows, 5, "boundary {boundary} of {source:?}");
                let joined: Vec<u8> = cut.into_iter().flat_map(|(body, _)| body).collect();
                assert_eq!(joined, source[records_start as usize..]);
            }
        }
    }

    /// A range boundary inside a quoted field holding a line break: the
    /// partition before it ends where the record ends, the one after it
    /// begins at the quoted line break, and the two disagree.
    #[tokio::test]
    async fn a_boundary_inside_a_quoted_line_break_makes_the_partitions_disagree() {
        let source = b"key,name\n1,\"a\nb\"\n2,c\n";
        let request = request(LoadMode::Overwrite);
        let boundary = 12;
        assert_eq!(&source[boundary as usize..boundary as usize + 2], b"a\n");
        let (before, _) = slices(source, &request, 9..boundary, 4, false).await;
        let (after, _) = slices(source, &request, boundary..source.len() as u64, 4, true).await;
        assert_eq!(before.end(), Some(17));
        assert_eq!(after.start(), 14);
    }

    /// Arrow numbers a parse error's line from its reader's start; a slice's
    /// refusal carries the source's number, so one slice and many refuse the
    /// same record in the same words.
    #[tokio::test]
    async fn a_slice_parse_error_names_the_source_line() {
        let catalog = catalog();
        let mut source = String::from("key,score\n");
        for row in 0..40 {
            if row == 27 {
                writeln!(&mut source, "{row},x").unwrap();
            } else {
                writeln!(&mut source, "{row},1.5").unwrap();
            }
        }
        let capture = CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        };
        let request = request(LoadMode::Overwrite);
        let whole = prepare_csv_batches(&catalog, &request, &capture, &mut |_| Ok(()))
            .unwrap_err()
            .to_string();
        assert!(whole.contains("at line 28"), "{whole}");

        let bytes = source.as_bytes();
        let resolved = ResolvedCsv::resolve(&catalog, &request, bytes, bytes.len() as u64).unwrap();
        let (_, cut) = slices(bytes, &request, 10..bytes.len() as u64, 64, false).await;
        let mut first_record = 1;
        let (body, rows) = cut
            .iter()
            .find(|(_, rows)| {
                let holds = (first_record..first_record + rows).contains(&28);
                if !holds {
                    first_record += rows;
                }
                holds
            })
            .unwrap();
        assert!(first_record > 1, "record 28 is past the first slice");
        let sliced = resolved
            .parse_slice(&catalog, &request, body, *rows, first_record, &mut |_| {
                Ok(())
            })
            .unwrap_err()
            .into_error()
            .to_string();
        assert_eq!(sliced, whole);
    }

    #[tokio::test]
    async fn a_header_alone_cuts_no_slice() {
        let bytes = b"key,score\n";
        let request = request(LoadMode::Overwrite);
        let resolved =
            ResolvedCsv::resolve(&catalog(), &request, bytes, bytes.len() as u64).unwrap();
        assert_eq!(resolved.records_start(), 10);
        let (scanner, cut) = slices(bytes, &request, 10..10, 8, false).await;
        assert!(cut.is_empty());
        assert_eq!((scanner.start(), scanner.end()), (10, Some(10)));
    }

    /// The header is resolved from the source's first bytes alone; a first
    /// record longer than them is refused with the bytes named.
    #[test]
    fn resolve_refuses_a_first_record_longer_than_the_head() {
        let request = request(LoadMode::Overwrite);
        let error = ResolvedCsv::resolve(&catalog(), &request, b"key,sco", 20)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "load csv into node:Person: the first record of the source exceeds 7 bytes"
        );
        let resolved = ResolvedCsv::resolve(&catalog(), &request, b"key,score\n0,1", 20).unwrap();
        assert_eq!(resolved.records_start(), 10);
    }

    /// Counts through fixed scratch-buffer boundaries and covers Arrow's
    /// per-column buffer floors; GQT cannot inspect these allocations.
    #[test]
    fn reader_admission_covers_wide_quoted_records() {
        let columns = 256;
        let header = (0..columns)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut values = vec!["x".to_string(); columns];
        values[0] = format!("\"{}\n{}\"", "a".repeat(5000), "b".repeat(5000));
        let source = format!("{header}\r\n{}", values.join(","));
        let request = request(LoadMode::Overwrite);
        let shape = CsvShape::count(source.as_bytes(), &request);
        assert_eq!((shape.rows, shape.columns), (1, columns));
        let schema = Arc::new(Schema::new(
            (0..columns)
                .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let batches = parse_records(source.as_bytes(), &request, schema, "node:Person").unwrap();
        let actual = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        assert!(shape.reader_bytes(source.len()) >= actual as u64);
        assert!(actual > 262144);
    }

    /// GQT sees the refusal, but cannot observe whether admission preceded decoding.
    #[test]
    fn admission_precedes_reader_and_omitted_column_allocation() {
        let catalog = build_catalog(
            &parse_schema("node Person {\n key: I64 @key\n embedding: Vector(32768)?\n}\n")
                .unwrap(),
        )
        .unwrap();
        for (source, refuse_at) in [("key\nnot_an_integer\n", 1), ("key\n1\n", 2)] {
            let capture = CapturedSource {
                bytes: Arc::from(source.as_bytes()),
                admission: None,
                size: source.len() as u64,
                e_tag: None,
                version: None,
            };
            let mut calls = 0;
            let error = prepare_csv_batches(
                &catalog,
                &request(LoadMode::Overwrite),
                &capture,
                &mut |bytes| {
                    calls += 1;
                    if calls == refuse_at {
                        if refuse_at == 2 {
                            assert!(bytes > 65536);
                        }
                        Err(OmniError::resource_limit("test admission", 1, bytes))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert_eq!(calls, refuse_at);
            assert!(
                matches!(error, OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "test admission")
            );
        }
    }

    #[test]
    fn render_reads_back_as_the_statement() {
        let request = CsvLoadRequest {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Edge("Knows".to_string()),
            mode: LoadMode::Merge,
            delimiter: b'|',
            header: true,
            columns: Some(vec![
                "from".to_string(),
                "to".to_string(),
                "since".to_string(),
            ]),
        };
        assert_eq!(
            request.render(),
            "load csv \"file:///input.csv\" into edge:Knows mode merge delimiter \"|\" header columns (from, to, since)"
        );
        assert_eq!(
            super::super::CsvLoadRequest::from_statement(&LoadStmt {
                source: "s3://bucket/people.csv".to_string(),
                target: LoadTarget::Node("Person".to_string()),
                mode: LoadStmtMode::Overwrite,
                delimiter: b',',
                header: false,
                columns: None,
            })
            .render(),
            "load csv \"s3://bucket/people.csv\" into node:Person mode overwrite"
        );
    }

    #[test]
    fn mapped_column_count_is_the_spelling_or_the_declared_width() {
        let catalog = build_catalog(
            &parse_schema(
                "node Person {\n  key: I64 @key\n  name: String?\n}\nedge Knows: Person -> Person {\n  since: I64\n}\n",
            )
            .unwrap(),
        )
        .unwrap();
        let spelled = CsvLoadRequest {
            columns: Some(vec!["key".to_string()]),
            ..request(LoadMode::Overwrite)
        };
        assert_eq!(spelled.mapped_column_count(&catalog), 1);
        assert_eq!(
            request(LoadMode::Overwrite).mapped_column_count(&catalog),
            2
        );
        let edge = CsvLoadRequest {
            target: LoadTarget::Edge("Knows".to_string()),
            ..request(LoadMode::Overwrite)
        };
        assert_eq!(edge.mapped_column_count(&catalog), 3);
        let unknown = CsvLoadRequest {
            target: LoadTarget::Node("Ghost".to_string()),
            ..request(LoadMode::Overwrite)
        };
        assert_eq!(unknown.mapped_column_count(&catalog), 0);
    }
}
