use std::collections::HashMap;

use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};

use super::TABLE_VERSION_MANAGEMENT_KEY;
use super::layout::{manifest_uri, open_manifest_dataset};
use super::metadata::TableVersionMetadata;
use super::migrations::stamp_current_version;
use super::state::{
    GraphLineageRow, ManifestState, SubTableEntry, entries_to_batch, graph_lineage_row_parts,
    manifest_schema, read_manifest_state,
};
use super::{TableIdentity, table_path_for_identity};

/// The manifest version the init `Dataset::write` produces (Lance datasets start
/// at version one). The genesis graph commit pins this version — a snapshot at
/// it is the empty, freshly-initialized graph. The two config-only commits that
/// follow (`update_config`, `stamp_current_version`) advance the live manifest
/// version but add no table data, so genesis correctly stays pinned at one.
const GENESIS_MANIFEST_VERSION: u64 = 1;

pub(super) async fn init_manifest_graph(
    root_uri: &str,
    catalog: &Catalog,
) -> Result<(Dataset, ManifestState)> {
    let root = root_uri.trim_end_matches('/');
    let (entries, version_metadata) = build_initial_entries(root, catalog).await?;

    // Genesis graph commit: parentless, actorless, minted once and folded into
    // the init write so `__manifest` is the single source of graph lineage from
    // version one (no `_graph_commits.lance` row, no separate publish).
    let genesis = GraphLineageRow {
        graph_commit_id: ulid::Ulid::new().to_string(),
        manifest_branch: None,
        manifest_version: GENESIS_MANIFEST_VERSION,
        parent_commit_id: None,
        merged_parent_commit_id: None,
        actor_id: None,
        created_at: crate::db::now_micros()?,
    };
    let genesis_lineage = graph_lineage_row_parts(&genesis, None)?;

    let manifest_batch = entries_to_batch(&entries, &version_metadata, &genesis_lineage)?;
    let schema = manifest_schema();
    let reader = RecordBatchIterator::new(vec![Ok(manifest_batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        auto_cleanup: None,
        skip_auto_cleanup: true,
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
    stamp_current_version(&mut dataset).await?;

    let known_state = read_manifest_state(&dataset).await?;
    Ok((dataset, known_state))
}

pub(super) async fn open_manifest_graph(
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
) -> Result<(Vec<SubTableEntry>, HashMap<TableIdentity, String>)> {
    let mut entries = Vec::new();
    let mut version_metadata = HashMap::new();
    let accepted_ir = catalog.bound_schema_ir().ok_or_else(|| {
        OmniError::manifest_internal(
            "manifest initialization requires an identity-bound accepted catalog",
        )
    })?;

    for (name, node_type) in &catalog.node_types {
        let node_ir = accepted_ir
            .nodes
            .iter()
            .find(|node| node.name == *name)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "identity-bound catalog is missing node IR for '{name}'"
                ))
            })?;
        let identity =
            TableIdentity::new(node_ir.type_id.get(), node_ir.table_incarnation_id.get())?;
        let table_key = format!("node:{}", name);
        let table_path = table_path_for_identity(&table_key, identity)?;
        let full_path = format!("{}/{}", root_uri, table_path);

        let ds = create_empty_dataset(&full_path, &node_type.arrow_schema).await?;
        let metadata = TableVersionMetadata::from_dataset(root_uri, &table_path, &ds)?;

        entries.push(SubTableEntry {
            identity,
            table_key: table_key.clone(),
            table_path: table_path.clone(),
            table_version: ds.version().version,
            table_branch: None,
            row_count: 0,
            version_metadata: metadata.clone(),
        });
        version_metadata.insert(identity, metadata.to_json_string()?);
    }

    for (name, edge_type) in &catalog.edge_types {
        let edge_ir = accepted_ir
            .edges
            .iter()
            .find(|edge| edge.name == *name)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "identity-bound catalog is missing edge IR for '{name}'"
                ))
            })?;
        let identity =
            TableIdentity::new(edge_ir.type_id.get(), edge_ir.table_incarnation_id.get())?;
        let table_key = format!("edge:{}", name);
        let table_path = table_path_for_identity(&table_key, identity)?;
        let full_path = format!("{}/{}", root_uri, table_path);

        let ds = create_empty_dataset(&full_path, &edge_type.arrow_schema).await?;
        let metadata = TableVersionMetadata::from_dataset(root_uri, &table_path, &ds)?;

        entries.push(SubTableEntry {
            identity,
            table_key: table_key.clone(),
            table_path: table_path.clone(),
            table_version: ds.version().version,
            table_branch: None,
            row_count: 0,
            version_metadata: metadata.clone(),
        });
        version_metadata.insert(identity, metadata.to_json_string()?);
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
        auto_cleanup: None,
        skip_auto_cleanup: true,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}
