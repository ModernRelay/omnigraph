//! `load csv`: one CSV object, captured once under the graph's external Blob
//! policy, typed by the `.pg` schema, normalized into the per-type Arrow
//! batches every strict load hands to the load transaction. The reader
//! replaces only the JSON normalization step; admission, staging, validation
//! and the branch-owned `__manifest` publication are the loader's, unchanged.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

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
use crate::table_store::CapturedSource;

/// The resource a source over the session's `load_source_max_bytes` is
/// refused under, before any payload byte is read.
pub const CSV_SOURCE_BYTES_RESOURCE: &str = "csv_source_bytes";

/// Reading the source object completes within this, or the load is refused
/// with the source named: a slow or stalled source cannot hold a write open.
pub const CSV_SOURCE_READ_DEADLINE: Duration = Duration::from_secs(120);

/// Records per reader batch in overwrite; each batch is one staged-write
/// input.
pub(crate) const CSV_READ_BATCH_ROWS: usize = 8192;

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
    db: &Omnigraph,
    request: &CsvLoadRequest,
    source_max_bytes: u64,
    source_charge: Arc<crate::engine::LoadCharge>,
) -> Result<CapturedSource> {
    let mut capture = tokio::time::timeout(
        CSV_SOURCE_READ_DEADLINE,
        db.blob_materializer().read_external_object(
            &request.source,
            source_max_bytes,
            CSV_SOURCE_BYTES_RESOURCE,
            |size| source_charge.grow(size.saturating_mul(2)),
        ),
    )
    .await
    .map_err(|_| {
        OmniError::external_blob_source(
            &request.source,
            format!(
                "load csv: reading the source exceeded {} seconds",
                CSV_SOURCE_READ_DEADLINE.as_secs()
            ),
        )
    })??;
    source_charge.shrink_to(capture.size);
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

/// The source's size from its metadata probe alone, for `explain load csv`:
/// the policy authorization, `source_max_bytes` refusal and deadline of
/// [`capture_csv_source`], and no payload byte read.
pub(crate) async fn probe_csv_source_size(
    db: &Omnigraph,
    request: &CsvLoadRequest,
    source_max_bytes: u64,
) -> Result<u64> {
    tokio::time::timeout(
        CSV_SOURCE_READ_DEADLINE,
        db.blob_materializer().external_object_size(
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
                "explain load csv: probing the source exceeded {} seconds",
                CSV_SOURCE_READ_DEADLINE.as_secs()
            ),
        )
    })?
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

    let bytes = capture.bytes.as_ref();
    let shape = CsvShape::count(bytes, request);
    admit(shape.reader_bytes(bytes.len()))?;
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
    let read = parse_records(bytes, request, read_schema, &table_key)?;
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
        properties,
        arrow_schema: &arrow_schema,
        roles: &roles,
        is_edge,
    };
    admit(typing.normalization_bytes(&read)?)?;
    let batches = typing.normalize_batches(&read)?;
    Ok(PreparedCsvBatch {
        type_name: request.target.type_name().to_string(),
        table_key,
        is_edge,
        batches,
        rows,
    })
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
            rows: records.saturating_sub(usize::from(request.header)),
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
        let mut bytes = rows.saturating_mul(128).saturating_add(
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
        for batch in read {
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
                    bytes = bytes.saturating_add(
                        key_bytes
                            .saturating_add((batch.num_rows() as u64).saturating_mul(32))
                            .saturating_mul(4),
                    );
                }
            }
        }
        Ok(bytes)
    }

    fn normalize_batches(&self, read: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        let table_key = self.table_key;
        let by_role: HashMap<&ColumnRole, usize> = self
            .roles
            .iter()
            .enumerate()
            .map(|(index, role)| (role, index))
            .collect();
        let leading = if self.is_edge { 3 } else { 1 };
        let mut first_record = 1;
        let mut normalized: Vec<NormalizedCsvBatch> = read
            .iter()
            .map(|batch| {
                let normalized = NormalizedCsvBatch {
                    first_record,
                    rows: batch.num_rows(),
                    endpoints: Vec::with_capacity(leading - 1),
                    properties: Vec::with_capacity(
                        self.arrow_schema.fields().len().saturating_sub(leading),
                    ),
                };
                first_record += batch.num_rows();
                normalized
            })
            .collect();
        if self.is_edge {
            for (role, label) in [(ColumnRole::Src, FROM_COLUMN), (ColumnRole::Dst, TO_COLUMN)] {
                let index = *by_role.get(&role).ok_or_else(|| {
                    OmniError::manifest_internal(format!("{label} column resolved twice"))
                })?;
                for (read, normalized) in read.iter().zip(&mut normalized) {
                    let strings = utf8_without_empty(read.column(index));
                    if let Some(row) = first_null(&strings) {
                        return Err(OmniError::manifest(format!(
                            "load csv into {table_key}: record {} has an empty `{label}` endpoint",
                            normalized.first_record + row
                        )));
                    }
                    normalized.endpoints.push(Arc::new(strings));
                }
            }
        }
        for field in self.arrow_schema.fields().iter().skip(leading) {
            let prop = self.properties.get(field.name()).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "property '{}' of {table_key} is missing from its catalog entry",
                    field.name()
                ))
            })?;
            let role = ColumnRole::Property {
                name: field.name().to_string(),
                prop: prop.clone(),
            };
            let index = by_role.get(&role).copied();
            if index.is_none() && !field.is_nullable() {
                return Err(OmniError::manifest(format!(
                    "load csv into {table_key}: property '{}' is not nullable and the source has no column for it",
                    field.name()
                )));
            }
            for (read, normalized) in read.iter().zip(&mut normalized) {
                let column = match index {
                    Some(index) => convert_property(
                        read.column(index),
                        field.name(),
                        prop,
                        table_key,
                        normalized.first_record,
                    )?,
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
                        return Err(OmniError::manifest(format!(
                            "load csv into {table_key}: record {} has an empty field for non-nullable property '{}'",
                            normalized.first_record + row,
                            field.name()
                        )));
                    }
                }
            }
        }
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
                    )?,
                    LoadTarget::Edge(name) => edge_ids(
                        &self.catalog.edge_types[name],
                        self.arrow_schema,
                        &normalized.endpoints[0],
                        &normalized.endpoints[1],
                        &normalized.properties,
                        self.catalog.system_columns,
                        normalized.rows,
                        normalized.first_record,
                    )?,
                };
                let mut columns: Vec<ArrayRef> =
                    Vec::with_capacity(self.arrow_schema.fields().len());
                columns.push(Arc::new(StringArray::from(ids)));
                columns.extend(normalized.endpoints);
                columns.extend(normalized.properties);
                RecordBatch::try_new(Arc::clone(self.arrow_schema), columns)
                    .map_err(OmniError::arrow_internal)
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
/// `DateTime` literal-checked cell by cell; `first_record` numbers row 0.
fn convert_property(
    column: &ArrayRef,
    name: &str,
    prop: &PropType,
    table_key: &str,
    first_record: usize,
) -> Result<ArrayRef> {
    let cell_error = |row: usize, message: String| {
        OmniError::manifest(format!(
            "load csv into {table_key}: record {}, property '{name}': {message}",
            first_record + row
        ))
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
            return Err(OmniError::manifest_internal(format!(
                "property '{name}' of {table_key} passed the kind check with an unsupported kind"
            )));
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
) -> Result<Vec<String>> {
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
        .transpose()?;
    (0..rows)
        .map(|row| match &key_columns {
            Some(key_columns) => canonical_key_id(key_columns, row)?.ok_or_else(|| {
                OmniError::manifest(format!(
                    "load csv into node:{}: record {} is missing a non-null @key value",
                    node_type.name,
                    first_record + row
                ))
            }),
            None => Ok(generate_id()),
        })
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
) -> Result<Vec<String>> {
    let key_columns = edge_key_columns(
        edge_type,
        schema,
        src_column,
        dst_column,
        property_columns,
        system_columns,
    )?;
    (0..rows)
        .map(|row| match &key_columns {
            Some(key_columns) => canonical_key_id(key_columns, row)?.ok_or_else(|| {
                OmniError::manifest(format!(
                    "load csv into edge:{}: record {} is missing a non-null @key value",
                    edge_type.name,
                    first_record + row
                ))
            }),
            None => Ok(generate_id()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::fmt::Write;

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
