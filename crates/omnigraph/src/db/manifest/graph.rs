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

use super::layout::{
    manifest_uri, open_manifest_dataset_with_identifier_with_session,
    open_manifest_dataset_with_session,
};
use super::metadata::TableVersionMetadata;
use super::migrations::{INTERNAL_MANIFEST_SCHEMA_VERSION, current_stamp_entry, guard_stamp};
use super::state::{
    GraphLineageRow, ManifestState, SubTableEntry, entries_to_batch, graph_lineage_row_parts,
    manifest_schema, read_manifest_state, read_manifest_state_and_lineage,
};
use super::{TableIdentity, table_path_for_identity};

/// The manifest version the init `Dataset::write` produces (Lance datasets start
/// at version one). The genesis graph commit pins this version — a snapshot at
/// it is the empty, freshly-initialized graph, and since the whole manifest
/// birth is that single Create commit (entries, lineage, and the
/// internal-schema stamp all ride it), genesis IS the live version at init.
const GENESIS_MANIFEST_VERSION: u64 = 1;

/// Exact, attempt-local receipt embedded in a fresh graph's atomic manifest
/// birth.  The random commit id distinguishes this initialization attempt from
/// another valid v1 manifest whose deterministic table identities happen to
/// have the same numeric values.
#[derive(Debug, Clone)]
pub(crate) struct GenesisManifestAttempt {
    lineage: GraphLineageRow,
}

impl GenesisManifestAttempt {
    /// Mint the receipt before invoking the manifest Create operation so the
    /// caller retains enough information to classify a lost acknowledgement.
    pub(crate) fn mint() -> Result<Self> {
        Ok(Self {
            lineage: GraphLineageRow {
                graph_commit_id: ulid::Ulid::new().to_string(),
                manifest_branch: None,
                manifest_version: GENESIS_MANIFEST_VERSION,
                parent_commit_id: None,
                merged_parent_commit_id: None,
                actor_id: None,
                created_at: crate::db::now_micros()?,
            },
        })
    }

    fn lineage(&self) -> &GraphLineageRow {
        &self.lineage
    }
}

/// Internal phase distinction for graph initialization.  Once the manifest
/// Create call has been invoked, any returned error is acknowledgement-unknown:
/// the immutable v1 manifest may already be durable and must be probed before
/// any schema cleanup is considered.
#[derive(Debug)]
pub(crate) enum ManifestInitError {
    BeforeManifestCreate(OmniError),
    ManifestCreateOutcomeUnknown(OmniError),
}

impl ManifestInitError {
    pub(crate) fn into_source(self) -> OmniError {
        match self {
            Self::BeforeManifestCreate(source) | Self::ManifestCreateOutcomeUnknown(source) => {
                source
            }
        }
    }
}

impl From<OmniError> for ManifestInitError {
    fn from(source: OmniError) -> Self {
        Self::BeforeManifestCreate(source)
    }
}

impl From<ManifestInitError> for OmniError {
    fn from(error: ManifestInitError) -> Self {
        error.into_source()
    }
}

/// Assembles the initial entries, genesis lineage, and internal-schema stamp,
/// and lands them all in the one `__manifest` Create commit. A returned error
/// from the Create call is acknowledgement-unknown; reading the state back is
/// `load_initial_manifest_state`, on the confirmed post-commit side.
pub(super) async fn init_manifest_graph(
    root_uri: &str,
    catalog: &Catalog,
    control_session: &Arc<lance::session::Session>,
    attempt: &GenesisManifestAttempt,
) -> std::result::Result<Dataset, ManifestInitError> {
    let root = root_uri.trim_end_matches('/');
    let (entries, version_metadata) = build_initial_entries(root, catalog, control_session).await?;

    // The caller pre-mints this exact parentless, actorless receipt and retains
    // it across the Create call.  Both lineage rows ride the same immutable v1
    // commit as every table entry and the internal-schema stamp.
    let genesis_lineage = graph_lineage_row_parts(attempt.lineage(), None)?;

    let manifest_batch = entries_to_batch(&entries, &version_metadata, &genesis_lineage)?;
    // The internal-schema stamp rides the Create write's schema metadata, so
    // the stamp is atomic with manifest birth: no crash window can leave
    // `__manifest` durable but unstamped. The Create commit is the manifest's
    // entire birth — entries, genesis lineage, and the stamp in one commit,
    // with nothing failable after it. (A `table_version_management` config
    // key is deliberately not written: neither the pinned Lance substrate nor
    // this crate reads it.)
    let (stamp_key, stamp_value) = current_stamp_entry();
    let schema: SchemaRef = Arc::new(
        manifest_schema()
            .as_ref()
            .clone()
            .with_metadata([(stamp_key, stamp_value)].into_iter().collect()),
    );
    let manifest_batch = RecordBatch::try_new(schema.clone(), manifest_batch.columns().to_vec())
        .map_err(|e| {
            OmniError::manifest_internal(format!("attach stamp metadata to init batch: {e}"))
        })?;
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
    let dataset = Dataset::write(reader, &manifest_path, Some(params))
        .await
        .map_err(|e| {
            ManifestInitError::ManifestCreateOutcomeUnknown(OmniError::Lance(e.to_string()))
        })?;
    // Models the object-store shape where the immutable Create landed but its
    // acknowledgement was lost before `Dataset::write` returned success to
    // OmniGraph.  It deliberately sits inside the create half, before the
    // caller can classify the returned Dataset as proof of commitment.
    crate::failpoints::maybe_fail(crate::failpoints::names::INIT_MANIFEST_CREATE_ACK_LOST)
        .map_err(ManifestInitError::ManifestCreateOutcomeUnknown)?;
    Ok(dataset)
}

/// Reopen and authenticate the one immutable genesis manifest created by
/// `attempt`.  This is the only positive classification of an ambiguous Create
/// result; a merely valid v1 manifest from another initializer is not enough.
pub(super) async fn open_exact_genesis_manifest(
    root_uri: &str,
    attempt: &GenesisManifestAttempt,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, ManifestState, Vec<GraphLineageRow>)> {
    crate::failpoints::maybe_fail(crate::failpoints::names::INIT_MANIFEST_CREATE_PROBE)?;
    let (dataset, known_state, lineage_rows, _) =
        open_manifest_graph_with_lineage(root_uri, None, control_session).await?;

    // `read_manifest_state_and_lineage` intentionally folds head rows into a
    // branch-keyed map for normal reads.  Authentication needs the stronger
    // raw-shape guarantee: two duplicate `graph_head:main` rows must not be
    // mistaken for the one head row emitted by this Create attempt.
    let mut head_scanner = dataset.scan();
    head_scanner.filter_expr(
        datafusion::prelude::col("object_type")
            .eq(datafusion::prelude::lit(super::OBJECT_TYPE_GRAPH_HEAD)),
    );
    let graph_head_row_count = head_scanner
        .count_rows()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;

    let stamp = guard_stamp(&dataset)?;
    if stamp != INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(genesis_probe_mismatch(
            root_uri,
            format!(
                "internal-schema stamp is v{stamp}, expected v{INTERNAL_MANIFEST_SCHEMA_VERSION}"
            ),
        ));
    }
    if dataset.version().version != GENESIS_MANIFEST_VERSION
        || known_state.version != GENESIS_MANIFEST_VERSION
    {
        return Err(genesis_probe_mismatch(
            root_uri,
            format!(
                "manifest version is {}, expected {GENESIS_MANIFEST_VERSION}",
                dataset.version().version
            ),
        ));
    }
    if lineage_rows.as_slice() != std::slice::from_ref(attempt.lineage()) {
        return Err(genesis_probe_mismatch(
            root_uri,
            "genesis lineage does not match this initialization attempt",
        ));
    }
    if graph_head_row_count != 1
        || known_state.graph_heads.len() != 1
        || known_state
            .graph_heads
            .get(super::MAIN_BRANCH_HEAD_KEY)
            .map(String::as_str)
            != Some(attempt.lineage().graph_commit_id.as_str())
    {
        return Err(genesis_probe_mismatch(
            root_uri,
            "main graph head does not match this initialization attempt",
        ));
    }

    Ok((dataset, known_state, lineage_rows))
}

fn genesis_probe_mismatch(root_uri: &str, detail: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_conflict(format!(
        "__manifest at '{}' is not the exact genesis created by this initialization attempt: {detail}",
        root_uri.trim_end_matches('/')
    ))
}

/// Reads back the state the `__manifest` Create commit landed. The
/// `init.post_manifest_create` failpoint fires as this function's first
/// statement.
pub(super) async fn load_initial_manifest_state(
    dataset: &Dataset,
) -> Result<(ManifestState, Vec<GraphLineageRow>)> {
    crate::failpoints::maybe_fail(crate::failpoints::names::INIT_POST_MANIFEST_CREATE)?;
    read_manifest_state_and_lineage(dataset).await
}

pub(super) async fn open_manifest_graph(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(
    Dataset,
    ManifestState,
    lance::dataset::refs::BranchIdentifier,
)> {
    let (dataset, branch_identifier) = open_manifest_dataset_with_identifier_with_session(
        root_uri.trim_end_matches('/'),
        branch,
        control_session,
    )
    .await?;
    let known_state = read_manifest_state(&dataset).await?;
    Ok((dataset, known_state, branch_identifier))
}

pub(super) async fn open_manifest_graph_with_lineage(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(
    Dataset,
    ManifestState,
    Vec<GraphLineageRow>,
    lance::dataset::refs::BranchIdentifier,
)> {
    let (dataset, branch_identifier) = open_manifest_dataset_with_identifier_with_session(
        root_uri.trim_end_matches('/'),
        branch,
        control_session,
    )
    .await?;
    let (known_state, lineage_rows) = read_manifest_state_and_lineage(&dataset).await?;
    Ok((dataset, known_state, lineage_rows, branch_identifier))
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
    let dataset = Dataset::write(reader, uri, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    crate::failpoints::maybe_fail(crate::failpoints::names::INIT_TABLE_CREATE_ACK_LOST)?;
    Ok(dataset)
}

fn keyed_graph_table_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    let mut id_count = 0;
    let fields = schema
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

    Ok(std::sync::Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata.clone(),
    )))
}
