//! Stored shape (internal-schema stamp 12). A row is three columns: `object_id`,
//! `object_type`, and `record`, a Lance packed struct holding every other field
//! of [`manifest_schema`] row-major in one column, so a scan reads one column's
//! pages for the record instead of one per field. Lance's packed struct has no
//! per-child null bit (Lance 11 refuses a null value inside a packed struct
//! child), so every child is declared non-null and the ninth child, `present`,
//! carries one bit per field that is null in the logical row; the child under
//! a set bit holds the filler (`""` or `0`), which a reader checks and refuses
//! anything else. [`compact_to_storage`] and [`expand_from_storage`] are the
//! two boundaries; everything above them keeps the logical [`manifest_schema`].
//! A scan projects `record` whole: Lance 11 cannot project a fixed-width child
//! of a packed struct on its own.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::NullBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, StructArray, UInt8Array, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::error::{OmniError, Result};
use crate::migrations::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION, PACKED_RECORD_STAMP,
};

/// The logical `__manifest` row schema, what every reader and writer above the
/// storage boundary sees. `object_id` keeps Lance's unenforced primary-key
/// marker: every existing `__manifest` carries it and Lance treats the marker
/// as fixed once set, so an overwrite must present it too. The stored shape is
/// [`manifest_storage_schema`] (stamp 12: the fields after `object_type`
/// packed into one `record` struct) or [`flat_manifest_schema`] (stamps 5 to
/// 11). The publish CAS itself is the version commit of `commit::overwrite`.
pub fn manifest_schema() -> SchemaRef {
    let object_id_metadata: HashMap<String, String> =
        [("lance-schema:unenforced-primary-key", "true")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false).with_metadata(object_id_metadata),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("table_key", DataType::Utf8, false),
        Field::new("stable_table_id", DataType::UInt64, true),
        Field::new("table_incarnation_id", DataType::UInt64, true),
        Field::new("table_version", DataType::UInt64, true),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("row_count", DataType::UInt64, true),
    ]))
}

/// The stored shape of stamps 5 to 11: the logical columns as flat columns,
/// plus `base_objects`, a list column no path ever wrote a value into or read
/// (dropped from the logical schema at stamp 12). Readers of a stamp-11
/// manifest project the logical columns out of it; the upgrade command
/// validates a conversion source against it.
pub fn flat_manifest_schema() -> SchemaRef {
    let logical = manifest_schema();
    let mut fields: Vec<Arc<Field>> = logical.fields().iter().cloned().collect();
    fields.insert(
        4,
        Arc::new(Field::new(
            "base_objects",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )),
    );
    Arc::new(Schema::new(fields))
}

/// The stored column holding every record field of a row as one packed struct.
pub(crate) const RECORD_COLUMN: &str = "record";
/// The bitmask child of `record`: bit `i` set means field `i` of
/// [`RECORD_FIELDS`] is null in the logical row.
pub(crate) const PRESENT_COLUMN: &str = "present";
const PACKED_STRUCT_KEY: &str = "lance-encoding:packed";

/// The logical fields folded into `record`, in bit order and in the order of
/// [`manifest_schema`] after `object_id` and `object_type`.
pub(crate) const RECORD_FIELDS: [&str; 8] = [
    "location",
    "metadata",
    "table_key",
    "stable_table_id",
    "table_incarnation_id",
    "table_version",
    "table_branch",
    "row_count",
];
const _: () = assert!(RECORD_FIELDS.len() <= u8::BITS as usize);

/// Which physical shape a `__manifest` version carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoredShape {
    Flat,
    Packed,
}

/// The shape a reader expects at `stamp`: packed from [`PACKED_RECORD_STAMP`], flat below.
pub(crate) fn stored_shape(stamp: Option<u32>) -> StoredShape {
    if stamp.is_some_and(|stamp| stamp >= PACKED_RECORD_STAMP) {
        StoredShape::Packed
    } else {
        StoredShape::Flat
    }
}

/// The shape a publish writes at `stamp`: packed for every served stamp (so a
/// stamp-11 manifest converts on its next publish), flat below the served floor.
pub(crate) fn written_shape(stamp: Option<u32>) -> StoredShape {
    if stamp.is_some_and(|stamp| stamp >= MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION) {
        StoredShape::Packed
    } else {
        StoredShape::Flat
    }
}
/// A flat write lands only below the served floor and every served stamp reads packed from
/// [`PACKED_RECORD_STAMP`] up, so [`stored_shape`] and [`written_shape`] agree only while the
/// packed stamp sits inside the served range.
const _: () = assert!(
    MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION <= PACKED_RECORD_STAMP
        && PACKED_RECORD_STAMP <= INTERNAL_MANIFEST_SCHEMA_VERSION
);

/// The stored schema at stamp 12: `object_id` (with Lance's unenforced
/// primary-key marker), `object_type`, and the packed `record` struct; the
/// dataset's schema metadata (where the stamp lives) is `metadata`. The only
/// error is a `RECORD_FIELDS` name missing from `manifest_schema`, two
/// constants of this file, so the `Result` is never `Err`; it stays for the
/// crate's no-panic rule.
pub(crate) fn manifest_storage_schema(metadata: HashMap<String, String>) -> Result<SchemaRef> {
    let logical = manifest_schema();
    let mut children = Vec::with_capacity(RECORD_FIELDS.len() + 1);
    for name in RECORD_FIELDS {
        let field = logical.field_with_name(name).map_err(|_| {
            OmniError::manifest_internal(format!(
                "record field '{name}' is not in the manifest schema"
            ))
        })?;
        children.push(Field::new(name, field.data_type().clone(), false));
    }
    children.push(Field::new(PRESENT_COLUMN, DataType::UInt8, false));
    let record = Field::new(
        RECORD_COLUMN,
        DataType::Struct(Fields::from(children)),
        false,
    )
    .with_metadata(HashMap::from([(
        PACKED_STRUCT_KEY.to_string(),
        "true".to_string(),
    )]));
    Ok(Arc::new(Schema::new_with_metadata(
        vec![logical.field(0).clone(), logical.field(1).clone(), record],
        metadata,
    )))
}

fn record_fields_of(schema: &SchemaRef) -> Result<Fields> {
    match schema
        .field_with_name(RECORD_COLUMN)
        .map(|field| field.data_type())
    {
        Ok(DataType::Struct(fields)) => Ok(fields.clone()),
        _ => Err(OmniError::manifest_internal(
            "manifest storage schema has no packed record struct".to_string(),
        )),
    }
}

fn named_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch.column_by_name(name).cloned().ok_or_else(|| {
        OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
    })
}

/// Fold a logical [`manifest_schema`] batch into the stored shape: the record
/// fields become the children of one packed struct, a null field is stored as
/// its filler with its `present` bit set. A field with no nulls keeps its
/// buffers; only a field carrying nulls is rebuilt, since the bytes under a
/// null slot are unspecified and the child must hold the filler there.
pub(crate) fn compact_to_storage(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let logical = manifest_schema();
    let mut present = vec![0u8; rows];
    let mut children: Vec<ArrayRef> = Vec::with_capacity(RECORD_FIELDS.len() + 1);
    for (bit, name) in RECORD_FIELDS.iter().enumerate() {
        let mask = 1u8 << bit;
        let column = named_column(batch, name)?;
        if column.null_count() > 0
            && !logical
                .field_with_name(name)
                .is_ok_and(|field| field.is_nullable())
        {
            return Err(OmniError::manifest_internal(format!(
                "record field '{name}' is not nullable but carries a null"
            )));
        }
        let child: ArrayRef = match column.data_type() {
            DataType::Utf8 => {
                let column = column.as_string::<i32>();
                if column.null_count() == 0 {
                    Arc::new(StringArray::new(
                        column.offsets().clone(),
                        column.values().clone(),
                        None,
                    ))
                } else {
                    let mut values = Vec::with_capacity(rows);
                    for (row, bits) in present.iter_mut().enumerate() {
                        if column.is_null(row) {
                            *bits |= mask;
                            values.push("");
                        } else {
                            values.push(column.value(row));
                        }
                    }
                    Arc::new(StringArray::from(values))
                }
            }
            DataType::UInt64 => {
                let column = column.as_primitive::<UInt64Type>();
                if column.null_count() == 0 {
                    Arc::new(UInt64Array::new(column.values().clone(), None))
                } else {
                    let mut values = Vec::with_capacity(rows);
                    for (row, bits) in present.iter_mut().enumerate() {
                        if column.is_null(row) {
                            *bits |= mask;
                            values.push(0);
                        } else {
                            values.push(column.value(row));
                        }
                    }
                    Arc::new(UInt64Array::from(values))
                }
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "record field '{name}' has unsupported type {other}"
                )));
            }
        };
        children.push(child);
    }
    children.push(Arc::new(UInt8Array::from(present)));
    let record = StructArray::try_new(record_fields_of(schema)?, children, None)
        .map_err(OmniError::arrow_internal)?;
    RecordBatch::try_new(
        schema.clone(),
        vec![
            named_column(batch, "object_id")?,
            named_column(batch, "object_type")?,
            Arc::new(record),
        ],
    )
    .map_err(OmniError::arrow_internal)
}

/// Spread a stored batch back into the logical [`manifest_schema`] columns, restoring each null
/// from its `present` bit. Every child keeps its value buffers: a logical column is the child's
/// buffers under a null bitmap built from `present`, and only the rows marked null are read, to
/// check that the filler is what the child holds there. Any other projected column (a Lance
/// row-metadata column) passes through unchanged after the logical columns, so the boundary is
/// total over projections.
pub(crate) fn expand_from_storage(batch: &RecordBatch) -> Result<RecordBatch> {
    let record = named_column(batch, RECORD_COLUMN)?;
    let record = record
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal("manifest record column is not a struct".to_string())
        })?;
    let present = record
        .column_by_name(PRESENT_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "manifest record struct has no UInt8 present bitmask".to_string(),
            )
        })?;
    let rows = batch.num_rows();
    if present.len() != rows {
        return Err(OmniError::manifest_internal(format!(
            "manifest record present bitmask has {} rows, batch has {rows}",
            present.len()
        )));
    }
    let logical = manifest_schema();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(logical.fields().len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical.fields().len() + 1);
    fields.push(Arc::new(logical.field(0).clone()));
    columns.push(named_column(batch, "object_id")?);
    fields.push(Arc::new(logical.field(1).clone()));
    columns.push(named_column(batch, "object_type")?);
    for (bit, name) in RECORD_FIELDS.iter().enumerate() {
        let mask = 1u8 << bit;
        let field = logical.field_with_name(name).map_err(|_| {
            OmniError::manifest_internal(format!(
                "record field '{name}' is not in the manifest schema"
            ))
        })?;
        fields.push(Arc::new(field.clone()));
        let child = record.column_by_name(name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "manifest record struct is missing child '{name}'"
            ))
        })?;
        let mut nulls = NullBufferBuilder::new(rows);
        for bits in present.values() {
            nulls.append(bits & mask == 0);
        }
        let nulls = nulls.finish();
        let column: ArrayRef = match child.data_type() {
            DataType::Utf8 => {
                let values = child.as_string::<i32>();
                if let Some(row) =
                    marked_rows(present, mask).find(|&row| !values.value(row).is_empty())
                {
                    return Err(filler_mismatch(name, row));
                }
                Arc::new(StringArray::new(
                    values.offsets().clone(),
                    values.values().clone(),
                    nulls,
                ))
            }
            DataType::UInt64 => {
                let values = child.as_primitive::<UInt64Type>();
                if let Some(row) = marked_rows(present, mask).find(|&row| values.value(row) != 0) {
                    return Err(filler_mismatch(name, row));
                }
                Arc::new(UInt64Array::new(values.values().clone(), nulls))
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "record field '{name}' has unsupported type {other}"
                )));
            }
        };
        columns.push(column);
    }
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !matches!(
            field.name().as_str(),
            "object_id" | "object_type" | RECORD_COLUMN
        ) {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

/// The rows whose `present` byte marks the field at `mask` null.
fn marked_rows(present: &UInt8Array, mask: u8) -> impl Iterator<Item = usize> + '_ {
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

/// The stamp-11 stored shape of a logical batch: the flat columns plus the
/// all-null `base_objects` list the flat layout carries. Written only for a
/// manifest still stamped below the supported floor, which a storage
/// conversion may hold open; every supported manifest converts to the packed
/// shape on its next publish.
pub(crate) fn flat_to_storage(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) => Ok(column.clone()),
            None if field.name() == "base_objects" => {
                Ok(new_null_array(field.data_type(), batch.num_rows()))
            }
            None => Err(OmniError::manifest_internal(format!(
                "manifest batch is missing column {}",
                field.name()
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(OmniError::arrow_internal)
}
