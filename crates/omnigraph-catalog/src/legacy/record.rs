//! The stored shapes of `__manifest` stamps 5 to 13. Stamps 5 to 11 store the
//! logical columns flat beside `base_objects`, a list column no path ever wrote
//! a value into. Stamps 12 and 13 store `object_id`, `object_type` and
//! `record`, a Lance packed struct holding the eight other fields row-major,
//! whose `present` byte sets bit `i` when field `i` of [`RECORD_FIELDS`] is
//! null and the child holds its filler (`""` or `0`). Stamp 13 adds the two
//! schema content columns beside `record`, null on every row but the
//! `schema_contract` row. [`expand_from_storage`] is the reader's boundary;
//! everything above it sees the logical [`manifest_schema`].

use std::sync::Arc;

use arrow_array::builder::NullBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, StructArray, UInt8Array, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::error::{OmniError, Result};
use crate::record::{PRESENT_COLUMN, RECORD_COLUMN, SCHEMA_CONTENT_COLUMNS};

/// The first stamp stored packed; every stamp below it is stored flat.
pub(crate) const PACKED_RECORD_STAMP: u32 = 12;
/// The first and only stamp whose `__manifest` carries the schema content columns.
pub(crate) const SCHEMA_CONTENT_STAMP: u32 = 13;

/// The logical row schema of stamps 5 to 13: ten fields, then the two content
/// columns. `object_id` keeps Lance's unenforced primary-key marker, which an
/// overwrite must present again.
pub(crate) fn manifest_schema() -> SchemaRef {
    let object_id_metadata = std::collections::HashMap::from([(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    )]);
    let mut fields = vec![
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
    ];
    fields.extend(SCHEMA_CONTENT_COLUMNS.map(schema_content_field));
    Arc::new(Schema::new(fields))
}

fn schema_content_field(name: &str) -> Field {
    Field::new(name, DataType::LargeUtf8, true)
}

fn is_schema_content_column(name: &str) -> bool {
    SCHEMA_CONTENT_COLUMNS.contains(&name)
}

/// The stored schema of stamps 5 to 11: the logical columns less the content
/// columns, with `base_objects` at index 4.
pub(crate) fn flat_manifest_schema() -> SchemaRef {
    let logical = manifest_schema();
    let mut fields: Vec<Arc<Field>> = logical
        .fields()
        .iter()
        .filter(|field| !is_schema_content_column(field.name()))
        .cloned()
        .collect();
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

/// The logical columns a flat-shape scan projects: every stored column but `base_objects`.
pub(crate) fn flat_projection() -> Vec<String> {
    flat_manifest_schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| name != "base_objects")
        .collect()
}

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

/// The shape a version stamped `stamp` is stored in; an absent stamp is flat.
pub(crate) fn stored_shape(stamp: Option<u32>) -> StoredShape {
    if stamp.is_some_and(|stamp| stamp >= PACKED_RECORD_STAMP) {
        StoredShape::Packed
    } else {
        StoredShape::Flat
    }
}

/// The children of `record`: the [`RECORD_FIELDS`] non-null in bit order, then `present`.
pub(crate) fn record_children() -> Result<Fields> {
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
    Ok(Fields::from(children))
}

/// Whether `data_type` is the `record` struct of stamps 12 and 13, child for child.
pub(crate) fn is_record_type(data_type: &DataType) -> Result<bool> {
    let expected = record_children()?;
    Ok(matches!(
        data_type,
        DataType::Struct(children)
            if children.len() == expected.len()
                && children.iter().zip(expected.iter()).all(|(found, expected)| {
                    found.name() == expected.name() && found.data_type() == expected.data_type()
                })
    ))
}

fn named_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch.column_by_name(name).cloned().ok_or_else(|| {
        OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
    })
}

/// The content column `name` of `batch`: its own column, or all null when the
/// scan did not project it or the version does not carry it.
fn stored_content_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    match batch.column_by_name(name) {
        None => Ok(new_null_array(&DataType::LargeUtf8, batch.num_rows())),
        Some(column) if column.data_type() == &DataType::LargeUtf8 => Ok(column.clone()),
        Some(column) => Err(OmniError::manifest_internal(format!(
            "manifest column '{name}' is {}, expected LargeUtf8",
            column.data_type()
        ))),
    }
}

/// Spread a packed stored batch into the logical [`manifest_schema`] columns,
/// restoring each null from its `present` bit and refusing a marked row whose
/// child holds anything but the filler. A column the scan projected beyond
/// those passes through after the logical columns.
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
    for name in SCHEMA_CONTENT_COLUMNS {
        fields.push(Arc::new(schema_content_field(name)));
        columns.push(stored_content_column(batch, name)?);
    }
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !matches!(
            field.name().as_str(),
            "object_id" | "object_type" | RECORD_COLUMN
        ) && !is_schema_content_column(field.name())
        {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

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

/// The packed stored schema under `metadata`, with the content columns when
/// `with_content` (stamp 13) and without them (stamp 12).
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn manifest_storage_schema(
    metadata: std::collections::HashMap<String, String>,
    with_content: bool,
) -> Result<SchemaRef> {
    let logical = manifest_schema();
    let record = Field::new(RECORD_COLUMN, DataType::Struct(record_children()?), false)
        .with_metadata(std::collections::HashMap::from([(
            crate::record::PACKED_STRUCT_KEY.to_string(),
            "true".to_string(),
        )]));
    let mut fields = vec![logical.field(0).clone(), logical.field(1).clone(), record];
    if with_content {
        fields.extend(SCHEMA_CONTENT_COLUMNS.map(schema_content_field));
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

/// Fold a logical batch into the packed `schema`: each null field is stored as
/// its filler with its `present` bit set. A content value is refused when
/// `schema` carries no content column.
#[cfg(any(test, feature = "test-util"))]
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
            DataType::UInt64 => {
                let column = column.as_primitive::<UInt64Type>();
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
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "record field '{name}' has unsupported type {other}"
                )));
            }
        };
        children.push(child);
    }
    children.push(Arc::new(UInt8Array::from(present)));
    let record = StructArray::try_new(record_children()?, children, None)
        .map_err(OmniError::arrow_internal)?;
    let mut columns = vec![
        named_column(batch, "object_id")?,
        named_column(batch, "object_type")?,
        Arc::new(record) as ArrayRef,
    ];
    for name in SCHEMA_CONTENT_COLUMNS {
        let column = stored_content_column(batch, name)?;
        if schema.field_with_name(name).is_ok() {
            columns.push(column);
        } else if column.null_count() < column.len() {
            return Err(OmniError::manifest_internal(format!(
                "manifest column '{name}' carries a value, which a stamp-12 record cannot store"
            )));
        }
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(OmniError::arrow_internal)
}

/// A logical batch in the flat `schema` of stamps 5 to 11, `base_objects` all
/// null. A content value is refused: no flat version ever stored one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn flat_to_storage(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    for name in SCHEMA_CONTENT_COLUMNS {
        if batch
            .column_by_name(name)
            .is_some_and(|column| column.null_count() < column.len())
        {
            return Err(OmniError::manifest_internal(format!(
                "manifest column '{name}' carries a value, which the flat shape cannot store"
            )));
        }
    }
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
