//! Stored shape (internal-schema stamp 14). A row is three columns: `object_id`,
//! `object_type`, and `record`, a Lance packed struct holding every other field
//! of [`manifest_schema`] row-major in one column, so a scan reads one column's
//! pages for the record instead of one per field. Lance's packed struct has no
//! per-child null bit (Lance 11 refuses a null value inside a packed struct
//! child), so every child is declared non-null and the last child, `present`,
//! carries one bit per field that is null in the logical row; the child under
//! a set bit holds the filler (`""` or `0`), which a reader checks and refuses
//! anything else. [`compact_to_storage`] and [`expand_from_storage`] are the
//! two manifest boundaries; everything above them keeps the logical [`manifest_schema`].
//! History uses the same field codec for its commit-only packed record.
//! A scan projects `record` whole: Lance 11 cannot project a fixed-width child
//! of a packed struct on its own.
//!
//! A `table` row fills the fields from `location` to `dropped_at`, the one
//! `graph_commit` row and every `settled_commit` row fill the fields from
//! `graph_branch` to `schema_content_hash`, and a `replaced_table` row fills the fields
//! of a `table` row and `replaced_at`; the fields of the other row types are
//! null.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::NullBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Int64Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray, StructArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::buffer::NullBuffer;

use crate::error::{OmniError, Result};

/// The stored column holding every record field of a row as one packed struct.
pub(crate) const RECORD_COLUMN: &str = "record";
/// The bitmask child of `record`: bit `i` set means field `i` of
/// its declared field list is null in the logical row.
pub(crate) const PRESENT_COLUMN: &str = "present";
pub(crate) const PACKED_STRUCT_KEY: &str = "lance-encoding:packed";

/// The two top-level columns holding the schema contract's texts on the
/// `schema_contract` row: the `.pg` source and the IR JSON, byte-exact. Null on
/// every other row; absent from the flat shape and from stamp 12.
pub const SCHEMA_CONTENT_COLUMNS: [&str; 2] = ["schema_source", "schema_ir"];

fn schema_content_field(name: &str) -> Field {
    Field::new(name, DataType::LargeUtf8, true)
}

pub(crate) fn packed_projection(dataset: &lance::Dataset, with_content: bool) -> Vec<String> {
    let mut projection: Vec<String> = ["object_id", "object_type", RECORD_COLUMN]
        .into_iter()
        .map(str::to_string)
        .collect();
    if with_content {
        projection.extend(
            SCHEMA_CONTENT_COLUMNS
                .into_iter()
                .filter(|name| dataset.schema().field(name).is_some())
                .map(str::to_string),
        );
    }
    projection
}

/// The type of a record field; its filler is `""` or `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FieldType {
    Utf8,
    UInt64,
    Int64,
}

impl FieldType {
    pub(crate) fn data_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::UInt64 => DataType::UInt64,
            Self::Int64 => DataType::Int64,
        }
    }
}

/// The fields of a `table` row.
pub(crate) const TABLE_FIELDS: [(&str, FieldType); 10] = [
    ("location", FieldType::Utf8),
    ("metadata", FieldType::Utf8),
    ("table_key", FieldType::Utf8),
    ("stable_table_id", FieldType::UInt64),
    ("table_incarnation_id", FieldType::UInt64),
    ("table_version", FieldType::UInt64),
    ("table_branch", FieldType::Utf8),
    ("row_count", FieldType::UInt64),
    ("manifest_version", FieldType::UInt64),
    ("dropped_at", FieldType::UInt64),
];

/// The fields of a `graph_commit` row; the commit's id is the row's `object_id`.
pub(crate) const COMMIT_FIELDS: [(&str, FieldType); 12] = [
    ("graph_branch", FieldType::Utf8),
    ("native_branch", FieldType::Utf8),
    ("graph_manifest_version", FieldType::UInt64),
    ("generation", FieldType::UInt64),
    ("parent_commit_id", FieldType::Utf8),
    ("merged_parent_commit_id", FieldType::Utf8),
    ("actor_id", FieldType::Utf8),
    ("created_at", FieldType::Int64),
    ("schema_ir_hash", FieldType::Utf8),
    ("schema_identity_version", FieldType::UInt64),
    ("schema_identity_domain", FieldType::Utf8),
    ("schema_content_hash", FieldType::Utf8),
];

/// The fields a `replaced_table` row holds beside [`TABLE_FIELDS`]: the
/// `__manifest` version whose publish replaced the row, and that same version
/// again as `registered_at` when that publish registered the identity.
pub(crate) const REPLACED_FIELDS: [(&str, FieldType); 2] = [
    ("replaced_at", FieldType::UInt64),
    ("registered_at", FieldType::UInt64),
];

const RECORD_FIELD_COUNT: usize = TABLE_FIELDS.len() + COMMIT_FIELDS.len() + REPLACED_FIELDS.len();

/// The logical fields folded into `record`, [`TABLE_FIELDS`], [`COMMIT_FIELDS`]
/// then [`REPLACED_FIELDS`], in bit order and in the order of
/// [`manifest_schema`] after `object_id` and `object_type`.
pub(crate) const RECORD_FIELDS: [(&str, FieldType); RECORD_FIELD_COUNT] = {
    let mut fields = [("", FieldType::Utf8); RECORD_FIELD_COUNT];
    let commits = TABLE_FIELDS.len();
    let replaced = commits + COMMIT_FIELDS.len();
    let mut next = 0;
    while next < commits {
        fields[next] = TABLE_FIELDS[next];
        next += 1;
    }
    while next < replaced {
        fields[next] = COMMIT_FIELDS[next - commits];
        next += 1;
    }
    while next < fields.len() {
        fields[next] = REPLACED_FIELDS[next - replaced];
        next += 1;
    }
    fields
};
const _: () = assert!(RECORD_FIELDS.len() <= u32::BITS as usize);

/// The logical `__manifest` row schema, what every reader and writer above the
/// storage boundary sees. `object_id` keeps Lance's unenforced primary-key
/// marker: Lance treats the marker as fixed once set, so an overwrite must
/// present it too. The stored shape is [`manifest_storage_schema`].
pub fn manifest_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(RECORD_FIELDS.len() + 2);
    fields.extend(key_fields());
    fields.extend(
        RECORD_FIELDS
            .iter()
            .map(|(name, field_type)| Field::new(*name, field_type.data_type(), true)),
    );
    fields.extend(SCHEMA_CONTENT_COLUMNS.map(schema_content_field));
    Arc::new(Schema::new(fields))
}

fn key_fields() -> [Field; 2] {
    let object_id_metadata = HashMap::from([(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    )]);
    [
        Field::new("object_id", DataType::Utf8, false).with_metadata(object_id_metadata),
        Field::new("object_type", DataType::Utf8, false),
    ]
}

/// Non-null physical children in null-bit order, followed by the null bitmask.
pub(crate) fn packed_children(fields: &[(&str, FieldType)]) -> Fields {
    fields
        .iter()
        .map(|(name, field_type)| Field::new(*name, field_type.data_type(), false))
        .chain([Field::new(PRESENT_COLUMN, DataType::UInt32, false)])
        .collect()
}

/// The stored schema: `object_id` (with Lance's unenforced primary-key
/// marker), `object_type`, and the packed `record` struct; the dataset's
/// schema metadata (where the stamp lives) is `metadata`.
pub(crate) fn manifest_storage_schema(metadata: HashMap<String, String>) -> SchemaRef {
    let record = Field::new(
        RECORD_COLUMN,
        DataType::Struct(packed_children(&RECORD_FIELDS)),
        false,
    )
    .with_metadata(HashMap::from([(
        PACKED_STRUCT_KEY.to_string(),
        "true".to_string(),
    )]));
    let [object_id, object_type] = key_fields();
    Arc::new(Schema::new_with_metadata(
        vec![
            object_id,
            object_type,
            record,
            schema_content_field(SCHEMA_CONTENT_COLUMNS[0]),
            schema_content_field(SCHEMA_CONTENT_COLUMNS[1]),
        ],
        metadata,
    ))
}

fn named_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch.column_by_name(name).cloned().ok_or_else(|| {
        OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
    })
}

fn require_type(name: &str, column: &ArrayRef, field_type: FieldType) -> Result<()> {
    if *column.data_type() != field_type.data_type() {
        return Err(OmniError::manifest_internal(format!(
            "record field '{name}' has type {}, expected {}",
            column.data_type(),
            field_type.data_type()
        )));
    }
    Ok(())
}

/// The stored child of a primitive logical column: its value buffer when it has no null, else a
/// rebuilt buffer holding the filler under every null, whose `present` bit is set.
fn compact_primitive<T: ArrowPrimitiveType>(
    column: &ArrayRef,
    present: &mut [u32],
    mask: u32,
) -> ArrayRef {
    let column = column.as_primitive::<T>();
    if column.null_count() == 0 {
        return Arc::new(PrimitiveArray::<T>::new(column.values().clone(), None));
    }
    let values = present.iter_mut().enumerate().map(|(row, bits)| {
        if column.is_null(row) {
            *bits |= mask;
            T::Native::default()
        } else {
            column.value(row)
        }
    });
    Arc::new(PrimitiveArray::<T>::from_iter_values(values))
}

fn compact_string(column: &ArrayRef, present: &mut [u32], mask: u32) -> ArrayRef {
    let column = column.as_string::<i32>();
    if column.null_count() == 0 {
        return Arc::new(StringArray::new(
            column.offsets().clone(),
            column.values().clone(),
            None,
        ));
    }
    let values = present.iter_mut().enumerate().map(|(row, bits)| {
        if column.is_null(row) {
            *bits |= mask;
            ""
        } else {
            column.value(row)
        }
    });
    Arc::new(StringArray::from_iter_values(values))
}

/// Fold a logical [`manifest_schema`] batch into the stored shape: the record
/// fields become the children of one packed struct, a null field is stored as
/// its filler with its `present` bit set. A field with no nulls keeps its
/// buffers; only a field carrying nulls is rebuilt, since the bytes under a
/// null slot are unspecified and the child must hold the filler there.
pub(crate) fn compact_to_storage(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let record = compact_record(
        &RECORD_FIELDS,
        batch.num_rows(),
        RECORD_FIELDS
            .iter()
            .map(|(name, _)| named_column(batch, name)),
    )?;
    RecordBatch::try_new(
        schema.clone(),
        vec![
            named_column(batch, "object_id")?,
            named_column(batch, "object_type")?,
            Arc::new(record),
            named_column(batch, SCHEMA_CONTENT_COLUMNS[0])?,
            named_column(batch, SCHEMA_CONTENT_COLUMNS[1])?,
        ],
    )
    .map_err(OmniError::arrow_internal)
}

/// Pack logical columns in `fields` order; set bits mark nulls with canonical fillers.
pub(crate) fn compact_record(
    fields: &[(&str, FieldType)],
    rows: usize,
    columns: impl IntoIterator<Item = Result<ArrayRef>>,
) -> Result<StructArray> {
    let mut present = vec![0u32; rows];
    let mut children: Vec<ArrayRef> = Vec::with_capacity(fields.len() + 1);
    for (bit, ((name, field_type), column)) in fields.iter().zip(columns).enumerate() {
        let mask = 1u32 << bit;
        let column = column?;
        require_type(name, &column, *field_type)?;
        children.push(match field_type {
            FieldType::Utf8 => compact_string(&column, &mut present, mask),
            FieldType::UInt64 => compact_primitive::<UInt64Type>(&column, &mut present, mask),
            FieldType::Int64 => compact_primitive::<Int64Type>(&column, &mut present, mask),
        });
    }
    children.push(Arc::new(UInt32Array::from(present)));
    StructArray::try_new(packed_children(fields), children, None).map_err(OmniError::arrow_internal)
}

fn expand_primitive<T: ArrowPrimitiveType>(
    child: &ArrayRef,
    present: &UInt32Array,
    mask: u32,
    nulls: Option<NullBuffer>,
    name: &str,
) -> Result<ArrayRef> {
    let values = child.as_primitive::<T>();
    if let Some(row) =
        marked_rows(present, mask).find(|&row| values.value(row) != T::Native::default())
    {
        return Err(filler_mismatch(name, row));
    }
    Ok(Arc::new(PrimitiveArray::<T>::new(
        values.values().clone(),
        nulls,
    )))
}

fn expand_string(
    child: &ArrayRef,
    present: &UInt32Array,
    mask: u32,
    nulls: Option<NullBuffer>,
    name: &str,
) -> Result<ArrayRef> {
    let values = child.as_string::<i32>();
    if let Some(row) = marked_rows(present, mask).find(|&row| !values.value(row).is_empty()) {
        return Err(filler_mismatch(name, row));
    }
    Ok(Arc::new(StringArray::new(
        values.offsets().clone(),
        values.values().clone(),
        nulls,
    )))
}

/// Spread a stored batch back into the logical [`manifest_schema`] columns, restoring each null
/// from its `present` bit. Every child keeps its value buffers: a logical column is the child's
/// buffers under a null bitmap built from `present`, and only the rows marked null are read, to
/// check that the filler is what the child holds there.
pub(crate) fn expand_from_storage(batch: &RecordBatch) -> Result<RecordBatch> {
    let record = named_column(batch, RECORD_COLUMN)?;
    let (record, present) = packed_record(&record, batch.num_rows())?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(RECORD_FIELDS.len() + 2);
    columns.push(named_column(batch, "object_id")?);
    columns.push(named_column(batch, "object_type")?);
    columns.extend(expand_record(&RECORD_FIELDS, record, present)?);
    for name in SCHEMA_CONTENT_COLUMNS {
        columns.push(batch.column_by_name(name).cloned().unwrap_or_else(|| {
            Arc::new(arrow_array::LargeStringArray::new_null(batch.num_rows()))
        }));
    }
    RecordBatch::try_new(manifest_schema(), columns).map_err(OmniError::arrow_internal)
}

/// Read the physical struct and its row-aligned null bitmask before decoding fields.
pub(crate) fn packed_record(
    record: &ArrayRef,
    rows: usize,
) -> Result<(&StructArray, &UInt32Array)> {
    let record = record
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal("manifest record column is not a struct".to_string())
        })?;
    let present = record
        .column_by_name(PRESENT_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "manifest record struct has no UInt32 present bitmask".to_string(),
            )
        })?;
    if present.len() != rows {
        return Err(OmniError::manifest_internal(format!(
            "manifest record present bitmask has {} rows, batch has {rows}",
            present.len()
        )));
    }
    Ok((record, present))
}

/// Restore logical nulls in field order, rejecting values hidden beneath a null bit.
pub(crate) fn expand_record(
    fields: &[(&str, FieldType)],
    record: &StructArray,
    present: &UInt32Array,
) -> Result<Vec<ArrayRef>> {
    let mut columns = Vec::with_capacity(fields.len());
    for (bit, (name, field_type)) in fields.iter().enumerate() {
        let mask = 1u32 << bit;
        let child = record.column_by_name(name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "manifest record struct is missing child '{name}'"
            ))
        })?;
        require_type(name, child, *field_type)?;
        let mut nulls = NullBufferBuilder::new(present.len());
        for bits in present.values() {
            nulls.append(bits & mask == 0);
        }
        let nulls = nulls.finish();
        columns.push(match field_type {
            FieldType::Utf8 => expand_string(child, present, mask, nulls, name)?,
            FieldType::UInt64 => expand_primitive::<UInt64Type>(child, present, mask, nulls, name)?,
            FieldType::Int64 => expand_primitive::<Int64Type>(child, present, mask, nulls, name)?,
        });
    }
    Ok(columns)
}

/// The rows whose `present` bits mark the field at `mask` null.
fn marked_rows(present: &UInt32Array, mask: u32) -> impl Iterator<Item = usize> + '_ {
    present
        .values()
        .iter()
        .enumerate()
        .filter(move |(_, bits)| *bits & mask != 0)
        .map(|(row, _)| row)
}

fn filler_mismatch(name: &str, row: usize) -> OmniError {
    OmniError::manifest_internal(format!(
        "manifest record field '{name}' is marked null but carries a value at row {row}"
    ))
}
