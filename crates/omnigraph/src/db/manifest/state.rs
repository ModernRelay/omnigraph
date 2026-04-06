use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;

use crate::error::{OmniError, Result};

use super::layout::version_object_id;
use super::metadata::TableVersionMetadata;
use super::{OBJECT_TYPE_TABLE, OBJECT_TYPE_TABLE_VERSION};

#[derive(Debug, Clone)]
pub struct SubTableEntry {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
}

#[derive(Debug, Clone)]
pub(super) struct ManifestState {
    pub(super) version: u64,
    pub(super) entries: Vec<SubTableEntry>,
}

pub(super) fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new(
            "base_objects",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("table_key", DataType::Utf8, false),
        Field::new("table_version", DataType::UInt64, true),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("row_count", DataType::UInt64, true),
    ]))
}

pub(super) async fn read_manifest_state(dataset: &Dataset) -> Result<ManifestState> {
    let version = dataset.version().version;
    let entries = read_manifest_entries(dataset).await?;
    let mut latest_versions = HashMap::<String, SubTableEntry>::new();

    for entry in entries {
        match latest_versions.get(&entry.table_key) {
            Some(existing) if existing.table_version >= entry.table_version => {}
            _ => {
                latest_versions.insert(entry.table_key.clone(), entry);
            }
        }
    }

    let mut entries: Vec<SubTableEntry> = latest_versions.into_values().collect();
    entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));

    Ok(ManifestState { version, entries })
}

pub(super) async fn read_manifest_entries(dataset: &Dataset) -> Result<Vec<SubTableEntry>> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut table_locations = HashMap::new();
    let mut version_entries = Vec::new();

    for batch in &batches {
        let object_types = string_column(batch, "object_type")?;
        let locations = string_column(batch, "location")?;
        let metadata = string_column(batch, "metadata")?;
        let table_keys = string_column(batch, "table_key")?;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;
        let row_counts = u64_column(batch, "row_count")?;

        for row in 0..batch.num_rows() {
            let table_key = table_keys.value(row).to_string();
            match object_types.value(row) {
                OBJECT_TYPE_TABLE => {
                    if locations.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table row missing location for {}",
                            table_key
                        )));
                    }
                    table_locations.insert(table_key, locations.value(row).to_string());
                }
                OBJECT_TYPE_TABLE_VERSION => {
                    let table_version = required_u64(versions, row, "table_version")?;
                    let row_count = required_u64(row_counts, row, "row_count")?;
                    if metadata.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table_version row missing metadata for {}",
                            table_key
                        )));
                    }
                    let table_branch = if branches.is_null(row) {
                        None
                    } else {
                        Some(branches.value(row).to_string())
                    };
                    version_entries.push(SubTableEntry {
                        table_key: table_key.clone(),
                        table_path: String::new(),
                        table_version,
                        table_branch,
                        row_count,
                        version_metadata: TableVersionMetadata::from_json_str(metadata.value(row))?,
                    });
                }
                _ => {}
            }
        }
    }

    let mut entries = version_entries
        .into_iter()
        .map(|mut entry| {
            entry.table_path = table_locations
                .get(&entry.table_key)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest missing table row for {}",
                        entry.table_key
                    ))
                })?;
            Ok(entry)
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|a, b| {
        a.table_key
            .cmp(&b.table_key)
            .then(a.table_version.cmp(&b.table_version))
    });

    Ok(entries)
}

pub(super) fn entries_to_batch(
    entries: &[SubTableEntry],
    version_metadata: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let mut object_ids = Vec::with_capacity(entries.len() * 2);
    let mut object_types = Vec::with_capacity(entries.len() * 2);
    let mut locations = Vec::with_capacity(entries.len() * 2);
    let mut metadata = Vec::with_capacity(entries.len() * 2);
    let mut table_keys = Vec::with_capacity(entries.len() * 2);
    let mut table_versions = Vec::with_capacity(entries.len() * 2);
    let mut table_branches = Vec::with_capacity(entries.len() * 2);
    let mut row_counts = Vec::with_capacity(entries.len() * 2);

    for entry in entries {
        object_ids.push(entry.table_key.clone());
        object_types.push(OBJECT_TYPE_TABLE.to_string());
        locations.push(Some(entry.table_path.clone()));
        metadata.push(None);
        table_keys.push(entry.table_key.clone());
        table_versions.push(None);
        table_branches.push(None);
        row_counts.push(None);

        object_ids.push(version_object_id(&entry.table_key, entry.table_version));
        object_types.push(OBJECT_TYPE_TABLE_VERSION.to_string());
        locations.push(None);
        metadata.push(Some(
            version_metadata
                .get(&entry.table_key)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing initial version metadata for {}",
                        entry.table_key
                    ))
                })?,
        ));
        table_keys.push(entry.table_key.clone());
        table_versions.push(Some(entry.table_version));
        table_branches.push(entry.table_branch.clone());
        row_counts.push(Some(entry.row_count));
    }

    manifest_rows_batch(
        object_ids,
        object_types,
        locations,
        metadata,
        table_keys,
        table_versions,
        table_branches,
        row_counts,
    )
}

pub(super) fn manifest_rows_batch(
    object_ids: Vec<String>,
    object_types: Vec<String>,
    locations: Vec<Option<String>>,
    metadata: Vec<Option<String>>,
    table_keys: Vec<String>,
    table_versions: Vec<Option<u64>>,
    table_branches: Vec<Option<String>>,
    row_counts: Vec<Option<u64>>,
) -> Result<RecordBatch> {
    let len = object_ids.len();
    RecordBatch::try_new(
        manifest_schema(),
        vec![
            Arc::new(StringArray::from(object_ids)),
            Arc::new(StringArray::from(object_types)),
            Arc::new(StringArray::from(locations)),
            Arc::new(StringArray::from(metadata)),
            new_null_array(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                len,
            ),
            Arc::new(StringArray::from(table_keys)),
            Arc::new(UInt64Array::from(table_versions)),
            Arc::new(StringArray::from(table_branches)),
            Arc::new(UInt64Array::from(row_counts)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

pub(super) fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not Utf8"))
        })
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not UInt64"))
        })
}

fn required_u64(column: &UInt64Array, row: usize, name: &str) -> Result<u64> {
    if column.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest column '{name}' is null at row {row}"
        )));
    }
    Ok(column.value(row))
}
