use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field, Schema, SchemaRef};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance::datatypes::{LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION};
use lance_file::version::LanceFileVersion;
use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};

use super::TABLE_VERSION_MANAGEMENT_KEY;
use super::layout::{manifest_uri, open_manifest_dataset_with_session};
use super::metadata::TableVersionMetadata;
use super::migrations::stamp_current_version;
use super::state::{
    GraphLineageRow, ManifestState, SubTableEntry, entries_to_batch, graph_lineage_row_parts,
    manifest_schema, read_manifest_state, read_manifest_state_and_lineage,
};
use super::token_store::initialize_stream_token_authority;
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
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, ManifestState, Vec<GraphLineageRow>)> {
    let root = root_uri.trim_end_matches('/');
    let stream_token_authority = initialize_stream_token_authority(root, control_session).await?;
    let (entries, version_metadata) = build_initial_entries(root, catalog, control_session).await?;

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
        stream_fold_attribution: None,
    };
    let genesis_lineage = graph_lineage_row_parts(&genesis, None)?;

    let manifest_batch = entries_to_batch(
        &entries,
        &version_metadata,
        &genesis_lineage,
        &stream_token_authority,
    )?;
    let schema = manifest_schema();
    let reader = RecordBatchIterator::new(vec![Ok(manifest_batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        auto_cleanup: None,
        skip_auto_cleanup: true,
        session: Some(Arc::clone(control_session)),
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

    let (known_state, lineage_rows) = read_manifest_state_and_lineage(&dataset).await?;
    Ok((dataset, known_state, lineage_rows))
}

pub(super) async fn open_manifest_graph(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, ManifestState)> {
    let dataset =
        open_manifest_dataset_with_session(root_uri.trim_end_matches('/'), branch, control_session)
            .await?;
    let known_state = read_manifest_state(&dataset).await?;
    Ok((dataset, known_state))
}

pub(super) async fn open_manifest_graph_with_lineage(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, ManifestState, Vec<GraphLineageRow>)> {
    let dataset =
        open_manifest_dataset_with_session(root_uri.trim_end_matches('/'), branch, control_session)
            .await?;
    let (known_state, lineage_rows) = read_manifest_state_and_lineage(&dataset).await?;
    Ok((dataset, known_state, lineage_rows))
}

pub(super) async fn snapshot_state_at(
    root_uri: &str,
    branch: Option<&str>,
    version: u64,
) -> Result<ManifestState> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(
        root_uri.trim_end_matches('/'),
        branch,
        &control_session,
    )
    .await?;
    let dataset = dataset
        .checkout_version(version)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    read_manifest_state(&dataset).await
}

async fn build_initial_entries(
    root_uri: &str,
    catalog: &Catalog,
    control_session: &Arc<lance::session::Session>,
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

        let ds = create_empty_dataset(&full_path, &node_type.arrow_schema, control_session).await?;
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

        let ds = create_empty_dataset(&full_path, &edge_type.arrow_schema, control_session).await?;
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

async fn create_empty_dataset(
    uri: &str,
    schema: &SchemaRef,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    // Keep initialization self-contained even for manifest-level callers that
    // construct an identity-bound compiler catalog directly in tests. Engine
    // catalogs already carry this metadata, but there must never be a
    // create-then-annotate window: Lance makes the PK metadata immutable after
    // dataset creation and RFC-023 activates it only for new format-v6 graphs.
    let schema = keyed_graph_table_schema(schema)?;
    let batch = RecordBatch::new_empty(schema.clone());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        allow_external_blob_outside_bases: true,
        auto_cleanup: None,
        skip_auto_cleanup: true,
        session: Some(Arc::clone(control_session)),
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}

pub(super) fn keyed_graph_table_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    // Engine catalogs have already crossed `fixup_physical_schemas` and carry
    // the exact internal field. Manifest-level tests and older call sites may
    // still supply the logical schema. Accept those two representations only:
    // a caller-defined lookalike must never be interpreted as trusted metadata.
    let mut has_stream_metadata = false;
    for field in schema
        .fields()
        .iter()
        .filter(|field| field.name() == crate::db::STREAM_METADATA_COLUMN)
    {
        if has_stream_metadata {
            return Err(OmniError::manifest_internal(format!(
                "graph table schema supplies reserved physical field '{}' more than once",
                crate::db::STREAM_METADATA_COLUMN
            )));
        }
        super::stream_token::validate_trusted_stream_metadata_field(field).map_err(|error| {
            OmniError::manifest_internal(format!(
                "graph table schema supplies a non-canonical reserved physical field '{}': {error}",
                crate::db::STREAM_METADATA_COLUMN
            ))
        })?;
        has_stream_metadata = true;
    }
    let mut id_count = 0;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| {
            let mut field = field.as_ref().clone();
            let mut metadata = field.metadata().clone();
            metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY_POSITION);
            if field.name() == "id" {
                id_count += 1;
                metadata.insert(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string());
            } else {
                metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY);
            }
            field.set_metadata(metadata);
            field
        })
        .collect::<Vec<Field>>();

    if id_count != 1 {
        return Err(OmniError::manifest_internal(format!(
            "graph table initialization requires exactly one top-level `id` field; found {id_count}"
        )));
    }
    let id = fields
        .iter()
        .find(|field| field.name() == "id")
        .expect("id_count == 1");
    if id.is_nullable() {
        return Err(OmniError::manifest_internal(
            "graph table initialization requires a non-null `id` field",
        ));
    }

    // Internal schema v9 provisions the exact nullable trusted-attribution
    // envelope on every graph table from creation. Pre-stream/direct rows use
    // a null top-level struct; no caller-supplied lookalike is interpreted.
    if !has_stream_metadata {
        fields.push(super::stream_token::trusted_stream_metadata_field());
    }

    Ok(std::sync::Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata.clone(),
    )))
}
