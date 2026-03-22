use std::collections::{HashMap, HashSet};
use std::path::Path;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use serde_json::Value as JsonValue;

use crate::catalog::schema_ir::PropDef;
use crate::error::{NanoError, Result};
use crate::json_output::array_value_to_json;
use crate::store::lance_io::read_lance_batches;
use crate::store::metadata::{DatabaseMetadata, DatasetLocator};

pub async fn build_export_rows_at_path(
    db_path: &Path,
    include_internal_fields: bool,
    include_embeddings: bool,
) -> Result<Vec<JsonValue>> {
    let metadata = DatabaseMetadata::open(db_path)?;
    build_export_rows_from_metadata(&metadata, include_internal_fields, include_embeddings).await
}

async fn build_export_rows_from_metadata(
    metadata: &DatabaseMetadata,
    include_internal_fields: bool,
    include_embeddings: bool,
) -> Result<Vec<JsonValue>> {
    let mut rows = Vec::new();
    let mut node_key_tokens: HashMap<String, HashMap<u64, String>> = HashMap::new();

    for node in metadata.schema_ir().node_types() {
        let Some(batch) = read_dataset_batch(metadata.node_dataset_locator(&node.name)).await?
        else {
            continue;
        };
        let id_arr = batch
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "node batch '{}' missing UInt64 id column",
                    node.name
                ))
            })?;
        let key_prop = node
            .properties
            .iter()
            .find(|prop| prop.key)
            .map(|prop| prop.name.as_str());
        let key_col = match key_prop {
            Some(prop_name) => {
                let key_idx =
                    node_property_index(batch.schema().as_ref(), prop_name).ok_or_else(|| {
                        NanoError::Storage(format!(
                            "node batch '{}' missing @key property '{}'",
                            node.name, prop_name
                        ))
                    })?;
                Some((prop_name.to_string(), batch.column(key_idx).clone()))
            }
            None => None,
        };

        let mut key_tokens = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let id = id_arr.value(row_idx);
            if let Some((prop_name, key_array)) = key_col.as_ref() {
                let key_token = export_key_token(key_array, row_idx, prop_name)?;
                key_tokens.insert(id, key_token);
            }

            let data = export_data_map(
                &batch,
                row_idx,
                &[0],
                node.properties.iter().filter_map(|prop| {
                    if include_embeddings || !is_embedding_property(prop) {
                        None
                    } else {
                        Some(prop.name.as_str())
                    }
                }),
            );
            let mut row = serde_json::json!({
                "type": node.name,
                "data": data,
            });
            if include_internal_fields {
                row["id"] = serde_json::Value::Number(id.into());
            }
            rows.push(row);
        }
        if !key_tokens.is_empty() {
            node_key_tokens.insert(node.name.clone(), key_tokens);
        }
    }

    for edge in metadata.schema_ir().edge_types() {
        let Some(batch) = read_dataset_batch(metadata.edge_dataset_locator(&edge.name)).await?
        else {
            continue;
        };
        let id_arr = batch
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "edge batch '{}' missing UInt64 id column",
                    edge.name
                ))
            })?;
        let src_arr = batch
            .column_by_name("src")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "edge batch '{}' missing UInt64 src column",
                    edge.name
                ))
            })?;
        let dst_arr = batch
            .column_by_name("dst")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "edge batch '{}' missing UInt64 dst column",
                    edge.name
                ))
            })?;

        for row_idx in 0..batch.num_rows() {
            let id = id_arr.value(row_idx);
            let src = src_arr.value(row_idx);
            let dst = dst_arr.value(row_idx);
            let from = node_key_tokens
                .get(&edge.src_type_name)
                .and_then(|m| m.get(&src))
                .cloned()
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "cannot export portable edge '{}': source {} node {} is missing an @key token",
                        edge.name, edge.src_type_name, src
                    ))
                })?;
            let to = node_key_tokens
                .get(&edge.dst_type_name)
                .and_then(|m| m.get(&dst))
                .cloned()
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "cannot export portable edge '{}': destination {} node {} is missing an @key token",
                        edge.name, edge.dst_type_name, dst
                    ))
                })?;
            let data = export_data_map(
                &batch,
                row_idx,
                &[0, 1, 2],
                edge.properties.iter().filter_map(|prop| {
                    if include_embeddings || !is_embedding_property(prop) {
                        None
                    } else {
                        Some(prop.name.as_str())
                    }
                }),
            );

            let mut row = serde_json::json!({
                "edge": edge.name,
                "from": from,
                "to": to,
                "data": data,
            });
            if include_internal_fields {
                row["id"] = serde_json::Value::Number(id.into());
                row["src"] = serde_json::Value::Number(src.into());
                row["dst"] = serde_json::Value::Number(dst.into());
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

async fn read_dataset_batch(locator: Option<DatasetLocator>) -> Result<Option<RecordBatch>> {
    let Some(locator) = locator else {
        return Ok(None);
    };
    let batches = read_lance_batches(&locator.dataset_path, locator.dataset_version).await?;
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(Some(batches[0].clone()));
    }
    let schema = batches[0].schema();
    let batch = arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| NanoError::Storage(format!("concat export batch error: {}", e)))?;
    Ok(Some(batch))
}

fn node_property_index(schema: &arrow_schema::Schema, prop_name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(idx, field)| (field.name() == prop_name).then_some(idx))
}

fn export_key_token(array: &ArrayRef, row_idx: usize, prop_name: &str) -> Result<String> {
    match array_value_to_json(array, row_idx) {
        serde_json::Value::Null => Err(NanoError::Storage(format!(
            "@key property {} cannot be null",
            prop_name
        ))),
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        other => Err(NanoError::Storage(format!(
            "unsupported @key export value for {}: {}",
            prop_name, other
        ))),
    }
}

fn export_data_map<I, S>(
    batch: &RecordBatch,
    row_idx: usize,
    excluded_indices: &[usize],
    excluded_names: I,
) -> JsonValue
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let excluded = excluded_indices.iter().copied().collect::<HashSet<_>>();
    let excluded_names = excluded_names
        .into_iter()
        .map(|name| name.as_ref().to_string())
        .collect::<HashSet<_>>();
    let mut data = serde_json::Map::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        if excluded.contains(&col_idx) || excluded_names.contains(field.name()) {
            continue;
        }
        data.insert(
            field.name().clone(),
            array_value_to_json(batch.column(col_idx), row_idx),
        );
    }
    JsonValue::Object(data)
}

fn is_embedding_property(prop: &PropDef) -> bool {
    prop.embed_source.is_some() || prop.scalar_type.starts_with("Vector(")
}
