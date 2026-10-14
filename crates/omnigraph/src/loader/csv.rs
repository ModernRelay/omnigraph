//! `load csv`: one CSV object, read under the graph's external Blob policy,
//! typed by the `.pg` schema, normalized into the one per-type Arrow batch
//! every strict load hands to the load transaction. The reader replaces only
//! the JSON normalization step; admission, staging, validation and the
//! branch-owned `__manifest` publication are the loader's, unchanged.

use std::collections::{HashMap, HashSet};
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
    parse_date64_literal, projected_strict_field_bytes,
};
use crate::db::Omnigraph;
use crate::error::{OmniError, Result};
use crate::storage_layer::WriteBudget;

/// The largest CSV object a load statement reads: the served `/load/ndjson`
/// body limit, so a CSV source is bounded by the same number as a JSONL
/// upload. A larger object is refused before any payload byte is read.
pub const CSV_SOURCE_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// The resource a too-large source is refused under.
pub const CSV_SOURCE_BYTES_RESOURCE: &str = "csv_source_bytes";

/// Reading the source object completes within this, or the load is refused
/// with the source named: a slow or stalled source cannot hold a write open.
pub const CSV_SOURCE_READ_DEADLINE: Duration = Duration::from_secs(120);

/// The resource the typed batch is admitted under, in every mode: the name
/// the strict JSONL projection uses, so a CSV load over `write_max_bytes`
/// refuses exactly as the equivalent JSONL load refuses.
const CSV_INPUT_ARROW_BYTES_RESOURCE: &str = "strict_input_arrow_bytes";

/// Records per reader batch; the batches are concatenated into one per load.
const CSV_READ_BATCH_ROWS: usize = 8192;

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
}

/// The one typed batch a CSV load stages: its table key, the type name the
/// receipt counts under, and whether it is a node or an edge batch.
pub(super) struct PreparedCsvBatch {
    pub(super) type_name: String,
    pub(super) table_key: String,
    pub(super) is_edge: bool,
    pub(super) batch: RecordBatch,
}

/// What one CSV column feeds: an endpoint of an edge file or a declared
/// property, with the property's declared type for the typed read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ColumnRole {
    Src,
    Dst,
    Property { name: String, prop: PropType },
}

/// Resolve the target, the mapping and the supported kinds against the
/// transaction's catalog, read the object under the policy, parse it typed,
/// and normalize it into the strict batch shape. Every refusal before the
/// read names what it refuses; nothing is staged on any error.
pub(super) async fn prepare_csv_batch(
    db: &Omnigraph,
    catalog: &Catalog,
    request: &CsvLoadRequest,
    write_budget: WriteBudget,
) -> Result<PreparedCsvBatch> {
    let table_key = request.target.table_key();
    let (properties, blob_properties, arrow_schema, is_edge) = match &request.target {
        LoadTarget::Node(name) => {
            let node_type = catalog.node_types.get(name).ok_or_else(|| {
                OmniError::manifest(format!("load csv: unknown node type '{name}'"))
            })?;
            (
                &node_type.properties,
                &node_type.blob_properties,
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
                &edge_type.blob_properties,
                Arc::clone(&edge_type.arrow_schema),
                true,
            )
        }
    };
    let system_columns = catalog.system_columns;
    if let Some(columns) = &request.columns {
        resolve_roles(columns, properties, is_edge, &table_key)?;
    }

    let bytes = tokio::time::timeout(
        CSV_SOURCE_READ_DEADLINE,
        db.blob_materializer().read_external_object(
            &request.source,
            CSV_SOURCE_MAX_BYTES,
            CSV_SOURCE_BYTES_RESOURCE,
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

    let format = Format::default()
        .with_header(request.header)
        .with_delimiter(request.delimiter);
    let (file_shape, _) = format
        .infer_schema(Cursor::new(bytes.as_ref()), Some(0))
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
    let projections = project_columns(&arrow_schema, blob_properties, &roles, system_columns)?;
    let read = parse_records(
        bytes.as_ref(),
        request,
        Arc::clone(&read_schema),
        &projections,
        write_budget,
        &table_key,
    )?;
    let rows = read.num_rows();
    if rows == 0 {
        return Err(OmniError::manifest(format!(
            "load csv into {table_key}: the source holds no records"
        )));
    }

    let mut by_role: HashMap<&ColumnRole, ArrayRef> = HashMap::with_capacity(roles.len());
    for (index, role) in roles.iter().enumerate() {
        by_role.insert(role, Arc::clone(read.column(index)));
    }
    let endpoint = |role: ColumnRole, label: &str| -> Result<ArrayRef> {
        let column = by_role.get(&role).ok_or_else(|| {
            OmniError::manifest_internal(format!("{label} column resolved twice"))
        })?;
        let strings = utf8_without_empty(column);
        if let Some(record) = first_null(&strings) {
            return Err(OmniError::manifest(format!(
                "load csv into {table_key}: record {record} has an empty `{label}` endpoint"
            )));
        }
        Ok(Arc::new(strings) as ArrayRef)
    };
    let (src_column, dst_column) = if is_edge {
        (
            Some(endpoint(ColumnRole::Src, FROM_COLUMN)?),
            Some(endpoint(ColumnRole::Dst, TO_COLUMN)?),
        )
    } else {
        (None, None)
    };

    let leading = if is_edge { 3 } else { 1 };
    let mut property_columns: Vec<ArrayRef> =
        Vec::with_capacity(arrow_schema.fields().len().saturating_sub(leading));
    for field in arrow_schema.fields().iter().skip(leading) {
        let prop = properties.get(field.name()).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "property '{}' of {table_key} is missing from its catalog entry",
                field.name()
            ))
        })?;
        let role = ColumnRole::Property {
            name: field.name().to_string(),
            prop: prop.clone(),
        };
        let column = match by_role.get(&role) {
            Some(read_column) => convert_property(read_column, field.name(), prop, &table_key)?,
            None => {
                if !field.is_nullable() {
                    return Err(OmniError::manifest(format!(
                        "load csv into {table_key}: property '{}' is not nullable and the source has no column for it",
                        field.name()
                    )));
                }
                new_null_array(field.data_type(), rows)
            }
        };
        if !field.is_nullable()
            && let Some(record) = first_null(column.as_ref())
        {
            return Err(OmniError::manifest(format!(
                "load csv into {table_key}: record {record} has an empty field for non-nullable property '{}'",
                field.name()
            )));
        }
        property_columns.push(column);
    }

    let ids = match &request.target {
        LoadTarget::Node(name) => node_ids(
            &catalog.node_types[name],
            &arrow_schema,
            &property_columns,
            rows,
        )?,
        LoadTarget::Edge(name) => edge_ids(
            &catalog.edge_types[name],
            &arrow_schema,
            src_column
                .as_ref()
                .expect("an edge batch resolved its endpoints"),
            dst_column
                .as_ref()
                .expect("an edge batch resolved its endpoints"),
            &property_columns,
            system_columns,
            rows,
        )?,
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)) as ArrayRef);
    if let (Some(src), Some(dst)) = (src_column, dst_column) {
        columns.push(src);
        columns.push(dst);
    }
    columns.extend(property_columns);
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)
        .map_err(OmniError::arrow_internal)?;
    write_budget.check(
        CSV_INPUT_ARROW_BYTES_RESOURCE,
        crate::table_store::write_batch_bytes(&batch)?.rows,
    )?;
    Ok(PreparedCsvBatch {
        type_name: request.target.type_name().to_string(),
        table_key,
        is_edge,
        batch,
    })
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

#[derive(Debug)]
struct ColumnProjection {
    base_bytes: u64,
    text_column: Option<usize>,
}

fn project_columns(
    schema: &Schema,
    blob_properties: &HashSet<String>,
    roles: &[ColumnRole],
    system_columns: SystemColumns,
) -> Result<Vec<ColumnProjection>> {
    let source_columns: HashMap<&str, usize> = roles
        .iter()
        .enumerate()
        .map(|(index, role)| {
            let name = match role {
                ColumnRole::Src => system_columns.src,
                ColumnRole::Dst => system_columns.dst,
                ColumnRole::Property { name, .. } => name.as_str(),
            };
            (name, index)
        })
        .collect();
    schema
        .fields()
        .iter()
        .map(|field| {
            let base_bytes = projected_strict_field_bytes(
                field,
                &serde_json::Value::Null,
                blob_properties.contains(field.name()),
            )?;
            let text_column = if field.data_type() == &DataType::Utf8 {
                source_columns.get(field.name().as_str()).copied()
            } else {
                None
            };
            Ok(ColumnProjection {
                base_bytes,
                text_column,
            })
        })
        .collect()
}

fn parse_records(
    bytes: &[u8],
    request: &CsvLoadRequest,
    schema: SchemaRef,
    projections: &[ColumnProjection],
    write_budget: WriteBudget,
    table_key: &str,
) -> Result<RecordBatch> {
    let parse_error = |error| csv_parse_error(&request.source, table_key, error);
    let reader = ReaderBuilder::new(Arc::clone(&schema))
        .with_header(request.header)
        .with_delimiter(request.delimiter)
        .with_batch_size(CSV_READ_BATCH_ROWS)
        .build(Cursor::new(bytes))
        .map_err(parse_error)?;
    let mut batches = Vec::new();
    let mut projected = 0_u64;
    for batch in reader {
        let batch = batch.map_err(parse_error)?;
        for row in 0..batch.num_rows() {
            for projection in projections {
                let text_bytes = projection.text_column.map_or(0, |index| {
                    let strings = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("invariant: a projected Utf8 field is read as StringArray");
                    if strings.is_null(row) {
                        0
                    } else {
                        u64::try_from(strings.value(row).len()).unwrap_or(u64::MAX)
                    }
                });
                projected = projected
                    .saturating_add(projection.base_bytes)
                    .saturating_add(text_bytes);
                write_budget.check(CSV_INPUT_ARROW_BYTES_RESOURCE, projected)?;
            }
        }
        batches.push(batch);
    }
    arrow_select::concat::concat_batches(&schema, &batches).map_err(parse_error)
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

/// The 1-based record of the first null, `None` when the column is dense.
fn first_null(column: &dyn Array) -> Option<usize> {
    (0..column.len())
        .find(|row| column.is_null(*row))
        .map(|row| row + 1)
}

/// One read column to its declared Arrow type: numbers as read, text with
/// empty fields as null, `Bool` from exactly `true` or `false`, `Date` and
/// `DateTime` through the loader's literal checks and casts cell by cell.
fn convert_property(
    column: &ArrayRef,
    name: &str,
    prop: &PropType,
    table_key: &str,
) -> Result<ArrayRef> {
    let cell_error = |record: usize, message: String| {
        OmniError::manifest(format!(
            "load csv into {table_key}: record {record}, property '{name}': {message}"
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
                return Err(cell_error(row + 1, "expected finite Float32".to_string()));
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
                return Err(cell_error(row + 1, "expected finite Float64".to_string()));
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
                            row + 1,
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
                            .map_err(|error| cell_error(row + 1, error.to_string()))?,
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
                            .map_err(|message| cell_error(row + 1, message))?;
                        Some(
                            parse_date64_literal(text)
                                .map_err(|error| cell_error(row + 1, error.to_string()))?,
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
                    row + 1
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
                    row + 1
                ))
            }),
            None => Ok(generate_id()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::catalog::schema_ir::{SYSTEM_COLUMNS_META, SYSTEM_COLUMNS_V3};
    use omnigraph_compiler::settings::SessionSettings;
    use std::fmt::Write;

    fn request() -> CsvLoadRequest {
        CsvLoadRequest {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode: LoadMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        }
    }

    fn budget(bytes: u64) -> WriteBudget {
        WriteBudget::from_settings(
            &SessionSettings::default()
                .with("write_max_bytes", &bytes.to_string())
                .unwrap(),
        )
    }

    #[test]
    fn projected_limit_refuses_first_batch_before_parsing_later_records() {
        let target = Schema::new(vec![
            Field::new("__id", DataType::Utf8, false),
            Field::new("key", DataType::Int64, false),
        ]);
        let roles = [ColumnRole::Property {
            name: "key".to_string(),
            prop: PropType::scalar(ScalarType::I64, false),
        }];
        let projections =
            project_columns(&target, &HashSet::new(), &roles, SYSTEM_COLUMNS_V3).unwrap();
        let read_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let mut source = String::from("key\n");
        for row in 0..CSV_READ_BATCH_ROWS {
            writeln!(&mut source, "{row}").unwrap();
        }
        source.push_str("not-an-integer\n");
        let limit = u64::try_from(CSV_READ_BATCH_ROWS).unwrap() * 40 - 1;
        let error = parse_records(
            source.as_bytes(),
            &request(),
            read_schema,
            &projections,
            budget(limit),
            "node:Person",
        )
        .unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { resource, limit: actual_limit, actual }
                if resource == CSV_INPUT_ARROW_BYTES_RESOURCE
                    && actual_limit == limit
                    && actual == limit + 1
        ));
    }

    #[test]
    fn projected_limit_accumulates_across_reader_batches() {
        let target = Schema::new(vec![
            Field::new("__id", DataType::Utf8, false),
            Field::new("key", DataType::Int64, false),
        ]);
        let roles = [ColumnRole::Property {
            name: "key".to_string(),
            prop: PropType::scalar(ScalarType::I64, false),
        }];
        let projections =
            project_columns(&target, &HashSet::new(), &roles, SYSTEM_COLUMNS_V3).unwrap();
        let read_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let mut source = String::from("key\n");
        for row in 0..=CSV_READ_BATCH_ROWS {
            writeln!(&mut source, "{row}").unwrap();
        }
        let limit = u64::try_from(CSV_READ_BATCH_ROWS).unwrap() * 40;
        let error = parse_records(
            source.as_bytes(),
            &request(),
            read_schema,
            &projections,
            budget(limit),
            "node:Person",
        )
        .unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { resource, limit: actual_limit, actual }
                if resource == CSV_INPUT_ARROW_BYTES_RESOURCE
                    && actual_limit == limit
                    && actual == limit + 16
        ));
    }

    #[test]
    fn projection_matches_jsonl_for_reordered_text_dates_and_omitted_properties() {
        let roles = [
            ColumnRole::Property {
                name: "day".to_string(),
                prop: PropType::scalar(ScalarType::Date, false),
            },
            ColumnRole::Property {
                name: "name".to_string(),
                prop: PropType::scalar(ScalarType::String, true),
            },
            ColumnRole::Src,
            ColumnRole::Property {
                name: "key".to_string(),
                prop: PropType::scalar(ScalarType::I64, false),
            },
            ColumnRole::Property {
                name: "when".to_string(),
                prop: PropType::scalar(ScalarType::DateTime, false),
            },
            ColumnRole::Dst,
            ColumnRole::Property {
                name: "flag".to_string(),
                prop: PropType::scalar(ScalarType::Bool, false),
            },
        ];
        let names = ["day", "name", "from", "key", "when", "to", "flag"];
        let read_schema = Arc::new(Schema::new(
            names
                .iter()
                .zip(&roles)
                .map(|(name, role)| Field::new(*name, read_data_type(role), true))
                .collect::<Vec<_>>(),
        ));
        let source = concat!(
            "day,name,from,key,when,to,flag\n",
            "2024-01-01,\"é,x\",a,1,2024-01-01T00:00:00Z,bc,true\n",
            "2024-02-02,,bb,2,2024-02-02T00:00:00Z,c,false\n",
        );
        for system_columns in [SYSTEM_COLUMNS_META, SYSTEM_COLUMNS_V3] {
            for dimension in [4, i32::MAX] {
                let target = Schema::new(vec![
                    Field::new(system_columns.id, DataType::Utf8, false),
                    Field::new(system_columns.src, DataType::Utf8, false),
                    Field::new(system_columns.dst, DataType::Utf8, false),
                    Field::new("key", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("day", DataType::Date32, false),
                    Field::new("when", DataType::Date64, false),
                    Field::new("flag", DataType::Boolean, false),
                    Field::new(
                        "tags",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new(
                        "vector",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            dimension,
                        ),
                        true,
                    ),
                    Field::new("blob", DataType::Struct(Vec::<Field>::new().into()), true),
                ]);
                let blobs = HashSet::from(["blob".to_string()]);
                let projections = project_columns(&target, &blobs, &roles, system_columns).unwrap();
                let objects = [
                    serde_json::json!({
                        system_columns.src: "a", system_columns.dst: "bc", "key": 1,
                        "name": "é,x", "day": "2024-01-01",
                        "when": "2024-01-01T00:00:00Z", "flag": true,
                    }),
                    serde_json::json!({
                        system_columns.src: "bb", system_columns.dst: "c", "key": 2,
                        "name": null, "day": "2024-02-02",
                        "when": "2024-02-02T00:00:00Z", "flag": false,
                    }),
                ];
                let rows = objects
                    .iter()
                    .map(|object| object.as_object().unwrap())
                    .collect::<Vec<_>>();
                let projected = 404 + u64::try_from(dimension).unwrap() * 10;
                let maximum = WriteBudget::default().bytes();
                let limits = if projected > maximum {
                    vec![maximum]
                } else {
                    vec![projected - 1, projected]
                };
                for limit in limits {
                    let csv = parse_records(
                        source.as_bytes(),
                        &request(),
                        Arc::clone(&read_schema),
                        &projections,
                        budget(limit),
                        "edge:Knows",
                    );
                    let jsonl = super::super::preflight_strict_rows_arrow_bytes(
                        &target, &rows, &blobs, limit,
                    );
                    if limit == projected {
                        assert_eq!(csv.unwrap().num_rows(), 2);
                        jsonl.unwrap();
                    } else {
                        let error = csv.unwrap_err();
                        assert_eq!(error.to_string(), jsonl.unwrap_err().to_string());
                        let expected = if projected > maximum {
                            188 + u64::try_from(dimension).unwrap() * 5
                        } else {
                            projected
                        };
                        assert!(matches!(
                            error,
                            OmniError::ResourceLimitExceeded { resource, actual, .. }
                                if resource == CSV_INPUT_ARROW_BYTES_RESOURCE
                                    && actual == expected
                        ));
                    }
                }
            }
        }
    }
}
