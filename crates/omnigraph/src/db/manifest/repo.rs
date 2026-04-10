use std::collections::HashMap;

use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};

use super::TABLE_VERSION_MANAGEMENT_KEY;
use super::layout::{manifest_uri, open_manifest_dataset, type_name_hash};
use super::metadata::TableVersionMetadata;
use super::state::{
    ManifestState, SubTableEntry, entries_to_batch, manifest_schema, read_manifest_state,
};

pub(super) async fn init_manifest_repo(
    root_uri: &str,
    catalog: &Catalog,
) -> Result<(Dataset, ManifestState)> {
    let root = root_uri.trim_end_matches('/');
    let (entries, version_metadata) = build_initial_entries(root, catalog).await?;

    let manifest_batch = entries_to_batch(&entries, &version_metadata)?;
    let schema = manifest_schema();
    let reader = RecordBatchIterator::new(vec![Ok(manifest_batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let manifest_path = manifest_uri(root);
    let mut dataset = Dataset::write(reader, &manifest_path, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    dataset
        .update_config([(TABLE_VERSION_MANAGEMENT_KEY, Some("true"))])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let known_state = read_manifest_state(&dataset).await?;
    Ok((dataset, known_state))
}

pub(super) async fn open_manifest_repo(
    root_uri: &str,
    branch: Option<&str>,
) -> Result<(Dataset, ManifestState)> {
    let dataset = open_manifest_dataset(root_uri.trim_end_matches('/'), branch).await?;
    let known_state = read_manifest_state(&dataset).await?;
    Ok((dataset, known_state))
}

pub(super) async fn snapshot_state_at(
    root_uri: &str,
    branch: Option<&str>,
    version: u64,
) -> Result<ManifestState> {
    let dataset = open_manifest_dataset(root_uri.trim_end_matches('/'), branch).await?;
    let dataset = dataset
        .checkout_version(version)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    read_manifest_state(&dataset).await
}

async fn build_initial_entries(
    root_uri: &str,
    catalog: &Catalog,
) -> Result<(Vec<SubTableEntry>, HashMap<String, String>)> {
    let mut entries = Vec::new();
    let mut version_metadata = HashMap::new();

    for (name, node_type) in &catalog.node_types {
        let hash = type_name_hash(name);
        let table_path = format!("nodes/{}", hash);
        let full_path = format!("{}/{}", root_uri, table_path);

        let ds = create_empty_dataset(&full_path, &node_type.arrow_schema).await?;
        let table_key = format!("node:{}", name);
        let metadata = TableVersionMetadata::from_dataset(root_uri, &table_path, &ds)?;

        entries.push(SubTableEntry {
            table_key: table_key.clone(),
            table_path: table_path.clone(),
            table_version: ds.version().version,
            table_branch: None,
            row_count: 0,
            version_metadata: metadata.clone(),
        });
        version_metadata.insert(table_key, metadata.to_json_string()?);
    }

    for (name, edge_type) in &catalog.edge_types {
        let hash = type_name_hash(name);
        let table_path = format!("edges/{}", hash);
        let full_path = format!("{}/{}", root_uri, table_path);

        let ds = create_empty_dataset(&full_path, &edge_type.arrow_schema).await?;
        let table_key = format!("edge:{}", name);
        let metadata = TableVersionMetadata::from_dataset(root_uri, &table_path, &ds)?;

        entries.push(SubTableEntry {
            table_key: table_key.clone(),
            table_path: table_path.clone(),
            table_version: ds.version().version,
            table_branch: None,
            row_count: 0,
            version_metadata: metadata.clone(),
        });
        version_metadata.insert(table_key, metadata.to_json_string()?);
    }

    Ok((entries, version_metadata))
}

async fn create_empty_dataset(uri: &str, schema: &SchemaRef) -> Result<Dataset> {
    let batch = RecordBatch::new_empty(schema.clone());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        allow_external_blob_outside_bases: true,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}
