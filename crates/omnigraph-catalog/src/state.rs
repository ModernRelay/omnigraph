use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, UInt64Array};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use lance::Dataset;

use crate::error::{OmniError, Result};
use crate::record::{
    SCHEMA_CONTENT_COLUMNS, StoredShape, expand_from_storage, flat_projection, packed_projection,
    stored_shape,
};
/// The row schemas' public path; they are defined in the private `record` module.
pub use crate::record::{flat_manifest_schema, manifest_schema};

use super::layout::{manifest_version_from_object_id, table_object_id, version_object_id};
use super::metadata::TableVersionMetadata;
use super::migrations::read_stamp;
use super::{
    MAIN_BRANCH_HEAD_KEY, OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_GRAPH_HEAD,
    OBJECT_TYPE_SCHEMA_CONTRACT, OBJECT_TYPE_TABLE, OBJECT_TYPE_TABLE_TOMBSTONE,
    OBJECT_TYPE_TABLE_VERSION, SCHEMA_CONTRACT_OBJECT_ID, TableIdentity, TableRegistration,
};

#[derive(Debug, Clone)]
pub struct DatasetEntry {
    pub identity: TableIdentity,
    pub type_key: String,
    pub dataset_path: String,
    pub published_dataset_version: u64,
    pub native_dataset_branch: Option<String>,
    pub entity_count: u64,
    pub version_metadata: TableVersionMetadata,
    /// The `__manifest` version whose publish wrote this registration: the
    /// clock the projection orders registrations by (RFC 0062).
    pub manifest_version: u64,
}

impl DatasetEntry {
    /// Field-for-field equal registration, the Lance manifest metadata included.
    pub fn same_registration(&self, other: &DatasetEntry) -> bool {
        self.identity == other.identity
            && self.type_key == other.type_key
            && self.dataset_path == other.dataset_path
            && self.published_dataset_version == other.published_dataset_version
            && self.native_dataset_branch == other.native_dataset_branch
            && self.entity_count == other.entity_count
            && self.version_metadata == other.version_metadata
            && self.manifest_version == other.manifest_version
    }
}

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub version: u64,
    pub entries: Vec<DatasetEntry>,
    /// Exact materialized `graph_head:<branch>` values from this SAME manifest
    /// version. Keeping the head beside the table snapshot prevents a
    /// manifest-only refresh from leaving coarse write authority split between
    /// a fresh table view and the commit graph's older derived cache.
    pub graph_heads: HashMap<String, String>,
    /// The `schema_contract` row's small fields from this SAME manifest
    /// version; `None` before the row exists (a stamp-12 version read through
    /// time travel).
    pub schema_contract: Option<SchemaContractHead>,
}

/// The small fields of the `schema_contract` row: the JSON payload of its
/// `metadata` column and the part of the contract every manifest-state read
/// folds. The texts stay in the content columns, projected for serving admission
/// and by [`read_schema_contract_row`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaContractHead {
    pub schema_ir_hash: String,
    pub schema_identity_version: u32,
    pub schema_identity_domain: String,
}

/// The whole `schema_contract` row: the `.pg` source and the IR JSON,
/// byte-exact, beside the folded head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContractRow {
    pub source: String,
    pub ir: String,
    pub head: SchemaContractHead,
}

impl SchemaContractRow {
    /// A contract for a catalog-only fixture: the IR text and hash of the
    /// catalog's bound IR, an empty source, identity version 1.
    #[cfg(any(test, feature = "test-util"))]
    pub fn for_test_catalog(catalog: &omnigraph_compiler::catalog::Catalog) -> Result<Self> {
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "a test schema contract requires an identity-bound catalog".to_string(),
            )
        })?;
        Ok(Self {
            source: String::new(),
            ir: omnigraph_compiler::schema_ir_pretty_json(schema_ir)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
            head: SchemaContractHead {
                schema_ir_hash: omnigraph_compiler::schema_ir_hash(schema_ir)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                schema_identity_version: 1,
                schema_identity_domain: schema_ir.schema_identity_domain.as_str().to_string(),
            },
        })
    }
}

#[derive(Debug, Clone)]
struct TableTombstoneEntry {
    identity: TableIdentity,
    table_key: String,
    tombstone_version: u64,
    manifest_version: u64,
}

/// A graph-lineage commit projected out of the `__manifest` `graph_commit`
/// rows (RFC-013 step 4). Field-for-field identical to `commit_graph::GraphCommit`
/// so the commit-graph cache can be sourced from the manifest projection without
/// touching any reader above that boundary. Kept as a separate struct here to
/// keep `state.rs` free of the `commit_graph` module dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLineageRow {
    pub graph_commit_id: String,
    pub graph_branch: Option<String>,
    pub graph_manifest_version: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

/// JSON payload of a `graph_commit` row's `metadata` column. The immutable
/// commit fields that have no dedicated manifest column live here; the mutable
/// graph-facing values (`graph_commit_id`, `graph_branch`,
/// `graph_manifest_version`) reuse the persisted manifest columns `object_id`,
/// `table_branch`, and `table_version`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GraphCommitMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merged_parent_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_id: Option<String>,
    created_at: i64,
}

/// JSON payload of a `graph_head` row's `metadata` column.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GraphHeadMetadata {
    head_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_commit_id: Option<String>,
}

/// The `object_id` for a branch's mutable head pointer row. Main encodes as
/// `graph_head:main`; named branches as `graph_head:<branch>`.
pub fn graph_head_object_id(branch: Option<&str>) -> String {
    format!(
        "{}{}",
        super::GRAPH_HEAD_OBJECT_ID_PREFIX,
        branch.unwrap_or(MAIN_BRANCH_HEAD_KEY)
    )
}

#[derive(Debug)]
struct ManifestScan {
    table_registrations: HashMap<TableIdentity, TableRegistration>,
    version_entries: Vec<DatasetEntry>,
    tombstones: Vec<TableTombstoneEntry>,
    /// Graph-lineage `graph_commit` rows, collected in the SAME pass only when
    /// the caller asked (`collect_lineage`). Empty on the table-state read hot
    /// path so it never pays the O(commits) lineage JSON decode; populated on the
    /// publish path, where `load_publish_state` already needs the parent and would
    /// otherwise scan `__manifest` a second time via `read_graph_lineage`.
    lineage_rows: Vec<GraphLineageRow>,
    /// Exact materialized `graph_head:<branch>` values, collected on every
    /// table-state/publish pass. This is bounded by branch count; unlike
    /// `lineage_rows`, it does not grow with commit history. OCC must distinguish
    /// a present head from an absent one (notably on a fresh named branch).
    graph_heads: HashMap<String, String>,
    /// The `schema_contract` row's head, collected on every pass.
    schema_contract: Option<SchemaContractHead>,
    schema_contract_row: Result<Option<SchemaContractRow>>,
    /// Every scanned row in the logical `manifest_schema()` columns (expanded
    /// from the packed shape when stored so, the content columns included),
    /// kept only on the publish scan: the copy-on-write publish rewrites them.
    live_rows: Vec<RecordBatch>,
}

pub async fn read_manifest_state_with_registration_clocks(
    dataset: &Dataset,
) -> Result<ManifestState> {
    if super::migrations::read_stamp(dataset) != Some(6) {
        return Err(OmniError::manifest_internal(
            "registration-clock conversion requires a v6 source".to_string(),
        ));
    }
    let scan = read_manifest_scan_with_clocks(dataset, false, None, true, false, false).await?;
    manifest_state_from_scan(dataset.version().version, scan)
}

pub async fn read_manifest_state(dataset: &Dataset) -> Result<ManifestState> {
    let version = dataset.version().version;
    // The table-state hot path never needs lineage, so don't pay its JSON decode.
    let scan = read_manifest_scan(dataset, false).await?;
    manifest_state_from_scan(version, scan)
}

/// Read the visible table state and complete graph-lineage projection from one
/// coherent `__manifest` scan.
///
/// `GraphCoordinator` needs both projections. Keeping them in one pass avoids a
/// second dataset open/full scan and guarantees its table snapshot and lineage
/// cache describe the exact same manifest version.
pub async fn read_manifest_state_and_lineage(
    dataset: &Dataset,
) -> Result<(ManifestState, Vec<GraphLineageRow>)> {
    let version = dataset.version().version;
    let ManifestScan {
        table_registrations,
        version_entries,
        tombstones,
        lineage_rows,
        graph_heads,
        schema_contract,
        schema_contract_row: _,
        live_rows: _,
    } = read_manifest_scan(dataset, true).await?;
    let state = assemble_manifest_state(
        version,
        table_registrations,
        version_entries,
        tombstones
            .into_iter()
            .map(|t| (t.identity, t.manifest_version)),
        graph_heads,
        schema_contract,
    )?;
    Ok((state, lineage_rows))
}

fn manifest_state_from_scan(version: u64, scan: ManifestScan) -> Result<ManifestState> {
    assemble_manifest_state(
        version,
        scan.table_registrations,
        scan.version_entries,
        scan.tombstones
            .into_iter()
            .map(|t| (t.identity, t.manifest_version)),
        scan.graph_heads,
        scan.schema_contract,
    )
}

/// Fold accumulators for the manifest projection, retained by a coordinator so
/// a refresh can fold ONLY newly appended rows instead of re-scanning the
/// whole catalog (the projection at head H is derivable from the
/// projection at H-1 plus the appended rows). The fold arms are the exact
/// reductions `assemble_manifest_state` applies — both paths run through this
/// type, so the incremental and full projections CANNOT diverge in
/// dedup/filter/sort semantics. Sizes: `registrations`/`latest_versions`/
/// `tombstone_map` are O(tables), and `graph_heads` is O(branches). Lineage is
/// deliberately not retained here: `GraphCoordinator` already owns that
/// projection, and duplicating it made an otherwise small exception-safety
/// clone O(commit history).
#[derive(Debug, Clone)]
pub struct ProjectionAccumulator {
    registrations: HashMap<TableIdentity, TableRegistration>,
    latest_versions: HashMap<TableIdentity, DatasetEntry>,
    tombstone_map: HashMap<TableIdentity, u64>,
    graph_heads: HashMap<String, String>,
    schema_contract: Option<SchemaContractHead>,
}

impl ProjectionAccumulator {
    fn empty() -> Self {
        Self {
            registrations: HashMap::new(),
            latest_versions: HashMap::new(),
            tombstone_map: HashMap::new(),
            graph_heads: HashMap::new(),
            schema_contract: None,
        }
    }

    /// Fold rows in. THE one copy of the per-row reductions: the full scan,
    /// the fragment-restricted delta fold, and `assemble_manifest_state`
    /// (and through it the publisher's post-publish fold) all run this, so
    /// none of them can drift in dedup/keep semantics.
    fn fold_parts(
        &mut self,
        registrations: HashMap<TableIdentity, TableRegistration>,
        version_entries: Vec<DatasetEntry>,
        tombstones: impl IntoIterator<Item = (TableIdentity, u64)>,
        graph_heads: HashMap<String, String>,
        schema_contract: Option<SchemaContractHead>,
    ) -> Result<()> {
        for (identity, registration) in registrations {
            if let Some(existing) = self.registrations.insert(identity, registration.clone()) {
                if existing != registration {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest has conflicting table rows for identity {identity}"
                    )));
                }
            }
        }
        for entry in version_entries {
            match self.latest_versions.get(&entry.identity) {
                Some(existing) if existing.manifest_version == entry.manifest_version => {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest has two rows for identity {} at manifest version {}",
                        entry.identity, entry.manifest_version
                    )));
                }
                Some(existing) if existing.manifest_version > entry.manifest_version => {}
                _ => {
                    self.latest_versions.insert(entry.identity, entry);
                }
            }
        }
        for (identity, tombstone_clock) in tombstones {
            match self.tombstone_map.get(&identity) {
                Some(existing) if *existing == tombstone_clock => {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest has two rows for identity {identity} at manifest version {tombstone_clock}"
                    )));
                }
                Some(existing) if *existing > tombstone_clock => {}
                _ => {
                    self.tombstone_map.insert(identity, tombstone_clock);
                }
            }
        }
        self.graph_heads.extend(graph_heads);
        if let Some(head) = schema_contract {
            self.schema_contract = Some(head);
        }
        Ok(())
    }

    fn fold_scan(&mut self, scan: ManifestScan) -> Result<()> {
        // A delta tombstone may refer to a registration in an older fragment,
        // so validate against the union of incoming and retained authority.
        // Do this before mutating the accumulator to preserve exception safety.
        for tombstone in &scan.tombstones {
            let registration = scan
                .table_registrations
                .get(&tombstone.identity)
                .or_else(|| self.registrations.get(&tombstone.identity))
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest tombstone for identity {} has no table registration",
                        tombstone.identity
                    ))
                })?;
            if registration.table_key != tombstone.table_key {
                return Err(OmniError::manifest_internal(format!(
                    "manifest tombstone identity {} has diagnostic alias '{}', current binding is '{}'",
                    tombstone.identity, tombstone.table_key, registration.table_key
                )));
            }
        }
        self.fold_parts(
            scan.table_registrations,
            scan.version_entries,
            scan.tombstones
                .into_iter()
                .map(|t| (t.identity, t.manifest_version)),
            scan.graph_heads,
            scan.schema_contract,
        )
    }

    /// Drop one branch's head pointer (its durable row was deleted — the
    /// mutable-row half of a head update, or a branch deletion; a replacement,
    /// when one exists, arrives via the delta fragments' live rows).
    pub(crate) fn remove_head(&mut self, branch_key: &str) {
        self.graph_heads.remove(branch_key);
    }

    /// Drop the schema contract's head (its durable row was deleted, the
    /// mutable-row half of a contract replacement; the replacement arrives via
    /// the delta fragments' live rows).
    pub(crate) fn remove_schema_contract(&mut self) {
        self.schema_contract = None;
    }

    /// Reduce to the visible state at `version`. Pure and repeatable.
    pub(crate) fn finish(&self, version: u64) -> Result<ManifestState> {
        finish_manifest_state(
            version,
            &self.registrations,
            self.latest_versions.values().cloned(),
            &self.tombstone_map,
            self.graph_heads.clone(),
            self.schema_contract.clone(),
        )
    }
}

/// One coherent scan producing the finished state, compact retained fold
/// accumulators, and lineage rows handed off to `CommitGraph`. The plain read
/// paths keep `read_manifest_state`.
pub(crate) async fn read_manifest_projection(
    dataset: &Dataset,
) -> Result<(ManifestState, ProjectionAccumulator, Vec<GraphLineageRow>)> {
    let version = dataset.version().version;
    let scan = read_manifest_scan_fragments(dataset, true, None).await?;
    projection_from_scan(version, scan)
}

pub(crate) async fn read_manifest_projection_with_contract(
    dataset: &Dataset,
) -> Result<(
    ManifestState,
    ProjectionAccumulator,
    Vec<GraphLineageRow>,
    Result<SchemaContractRow>,
)> {
    let version = dataset.version().version;
    let (scan, contract) = read_manifest_scan_with_contract(dataset, true).await?;
    let (state, accumulator, lineage_rows) = projection_from_scan(version, scan)?;
    Ok((state, accumulator, lineage_rows, contract))
}

pub(crate) async fn read_manifest_state_with_contract(
    dataset: &Dataset,
) -> Result<(ManifestState, Result<SchemaContractRow>)> {
    let (scan, contract) = read_manifest_scan_with_contract(dataset, false).await?;
    Ok((
        manifest_state_from_scan(dataset.version().version, scan)?,
        contract,
    ))
}

async fn read_manifest_scan_with_contract(
    dataset: &Dataset,
    collect_lineage: bool,
) -> Result<(ManifestScan, Result<SchemaContractRow>)> {
    let content_shape = require_schema_content(dataset);
    let scan_dataset = if content_shape.is_ok()
        && !dataset
            .schema()
            .metadata
            .contains_key(crate::migrations::UPGRADE_PENDING_KEY)
    {
        crate::instrumentation::manifest_scan_dataset(dataset).await?
    } else {
        dataset.clone()
    };
    let version = dataset.version().version;
    let mut scan = read_manifest_scan_with_clocks(
        &scan_dataset,
        collect_lineage,
        None,
        false,
        false,
        content_shape.is_ok(),
    )
    .await?;
    let captured = std::mem::replace(&mut scan.schema_contract_row, Ok(None));
    let contract = content_shape
        .and(captured)
        .and_then(|row| finish_schema_contract_row(row, scan.schema_contract.as_ref(), version));
    Ok((scan, contract))
}

fn projection_from_scan(
    version: u64,
    mut scan: ManifestScan,
) -> Result<(ManifestState, ProjectionAccumulator, Vec<GraphLineageRow>)> {
    let lineage_rows = std::mem::take(&mut scan.lineage_rows);
    let mut accumulator = ProjectionAccumulator::empty();
    accumulator.fold_scan(scan)?;
    let state = accumulator.finish(version)?;
    Ok((state, accumulator, lineage_rows))
}

/// Fold ONLY the live rows of `fragments` of `dataset` into `accumulator`
/// (the appended-fragments delta of an incremental refresh) and reduce to the
/// state at the dataset's version.
pub(crate) async fn fold_projection_delta(
    dataset: &Dataset,
    fragments: Vec<lance_table::format::Fragment>,
    accumulator: &mut ProjectionAccumulator,
) -> Result<(ManifestState, Vec<GraphLineageRow>)> {
    let version = dataset.version().version;
    let mut scan = read_manifest_scan_fragments(dataset, true, Some(fragments)).await?;
    let lineage_rows = std::mem::take(&mut scan.lineage_rows);
    accumulator.fold_scan(scan)?;
    Ok((accumulator.finish(version)?, lineage_rows))
}

/// `(object_type, object_id)` of the rows at `offsets` inside one fragment,
/// read from a pinned version where those rows are still live. Bounded by the
/// deletion-vector difference the caller measured — a handful of rows, never
/// history-sized.
pub(crate) async fn read_object_identities_at_offsets(
    dataset: &Dataset,
    fragment: lance_table::format::Fragment,
    offsets: &std::collections::HashSet<u32>,
) -> Result<Vec<(String, String)>> {
    if offsets.is_empty() {
        return Ok(Vec::new());
    }

    let fragment_id = u32::try_from(fragment.id).map_err(|_| {
        OmniError::manifest_internal(format!(
            "manifest fragment id {} cannot be represented as a Lance row address",
            fragment.id
        ))
    })?;
    let mut addresses = offsets
        .iter()
        .copied()
        .map(|offset| {
            u64::from(lance_core::utils::address::RowAddress::new_from_parts(
                fragment_id,
                offset,
            ))
        })
        .collect::<Vec<_>>();
    addresses.sort_unstable();

    // A fragment restriction limits which files a scanner visits; it is not a
    // row-level take. Use the physical addresses already supplied by the
    // deletion-vector delta so a compacted, history-sized fragment still reads
    // only the newly deleted head rows from the exact pinned old dataset.
    let dataset = Arc::new(dataset.clone());
    let projection_schema = dataset
        .schema()
        .project_preserve_system_columns(&["object_id", "object_type"])
        .map_err(OmniError::storage)?;
    let projection = lance::dataset::ProjectionRequest::from_schema(projection_schema)
        .into_projection_plan(Arc::clone(&dataset))
        .map_err(OmniError::storage)?;
    let batch = lance::dataset::TakeBuilder::try_new_from_addresses(
        dataset,
        addresses,
        Arc::new(projection),
    )
    .map_err(OmniError::storage)?
    .execute()
    .await
    .map_err(OmniError::storage)?;
    crate::instrumentation::record_projection_identity_rows(batch.num_rows());

    let object_ids = string_column(&batch, "object_id")?;
    let object_types = string_column(&batch, "object_type")?;
    let identities = (0..batch.num_rows())
        .map(|row| {
            (
                object_types.value(row).to_string(),
                object_ids.value(row).to_string(),
            )
        })
        .collect();
    Ok(identities)
}

/// Reduce raw manifest rows to the visible per-table state: keep the
/// registration with the greatest `manifest_version` per immutable table
/// identity, drop any sealed by a tombstone with a greater `manifest_version`,
/// then sort by `table_key` for deterministic output. Shared by the scan path
/// (`read_manifest_state`) and the in-memory post-publish fold in the publisher
/// (RFC-013 PR2 #1b), so the two CANNOT diverge in the dedup/filter/sort — the
/// byte-identity the fold relies on. Tombstones are passed as
/// `(identity, manifest_version)` tuples so callers outside this module need
/// not name the private `TableTombstoneEntry`.
pub(crate) fn assemble_manifest_state(
    version: u64,
    registrations: HashMap<TableIdentity, TableRegistration>,
    version_entries: Vec<DatasetEntry>,
    tombstones: impl IntoIterator<Item = (TableIdentity, u64)>,
    graph_heads: HashMap<String, String>,
    schema_contract: Option<SchemaContractHead>,
) -> Result<ManifestState> {
    assemble_manifest_projection(
        version,
        registrations,
        version_entries,
        tombstones,
        graph_heads,
        schema_contract,
    )
    .map(|(state, _)| state)
}

/// Return the exact compact accumulators alongside the visible state. The
/// publisher already folds these inputs; retaining the result avoids another
/// scan after the graph coordinator has adopted the corresponding lineage.
pub(crate) fn assemble_manifest_projection(
    version: u64,
    registrations: HashMap<TableIdentity, TableRegistration>,
    version_entries: Vec<DatasetEntry>,
    tombstones: impl IntoIterator<Item = (TableIdentity, u64)>,
    graph_heads: HashMap<String, String>,
    schema_contract: Option<SchemaContractHead>,
) -> Result<(ManifestState, ProjectionAccumulator)> {
    let mut accumulator = ProjectionAccumulator::empty();
    accumulator.fold_parts(
        registrations,
        version_entries,
        tombstones,
        graph_heads,
        schema_contract,
    )?;
    Ok((accumulator.finish(version)?, accumulator))
}

/// The shared reduction tail (tombstone filter, alias-uniqueness check, sort)
/// — one implementation under both the full-scan path and the incremental
/// fold, so the two cannot diverge.
fn finish_manifest_state(
    version: u64,
    registrations: &HashMap<TableIdentity, TableRegistration>,
    latest_versions: impl Iterator<Item = DatasetEntry>,
    tombstone_map: &HashMap<TableIdentity, u64>,
    graph_heads: HashMap<String, String>,
    schema_contract: Option<SchemaContractHead>,
) -> Result<ManifestState> {
    let mut entries: Vec<DatasetEntry> = latest_versions
        .filter(|entry| {
            tombstone_map
                .get(&entry.identity)
                .map(|tombstone_clock| *tombstone_clock < entry.manifest_version)
                .unwrap_or(true)
        })
        .map(|mut entry| {
            let registration = registrations.get(&entry.identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "manifest missing table row for identity {} (version row alias '{}')",
                    entry.identity, entry.type_key
                ))
            })?;
            entry.type_key = registration.table_key.clone();
            entry.dataset_path = registration.table_path.clone();
            Ok(entry)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut aliases = HashMap::<String, TableIdentity>::new();
    for entry in &entries {
        if let Some(existing) = aliases.insert(entry.type_key.clone(), entry.identity) {
            if existing != entry.identity {
                return Err(OmniError::manifest_internal(format!(
                    "manifest has two live table identities ({existing} and {}) bound to alias '{}'",
                    entry.identity, entry.type_key
                )));
            }
        }
    }
    entries.sort_by(|a, b| a.type_key.cmp(&b.type_key));
    Ok(ManifestState {
        version,
        entries,
        graph_heads,
        schema_contract,
    })
}

// Preserve historical registrations for cleanup's published-pin proof, including
// versions superseded by later writes. Current snapshots alone are insufficient.
pub(crate) async fn read_manifest_entries(dataset: &Dataset) -> Result<Vec<DatasetEntry>> {
    let scan = read_manifest_scan(dataset, false).await?;
    let registrations = scan.table_registrations;
    scan.version_entries
        .into_iter()
        .map(|mut entry| {
            let registration = registrations.get(&entry.identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "manifest missing table row for identity {}",
                    entry.identity
                ))
            })?;
            entry.type_key = registration.table_key.clone();
            entry.dataset_path = registration.table_path.clone();
            Ok(entry)
        })
        .collect()
}

/// The full table state the publisher needs to build its CAS batch, plus the
/// `graph_commit` lineage rows for parent resolution — all from ONE `__manifest`
/// scan (RFC-013 P2). Replaces the prior four scans on the publish path (three
/// thin accessors + a separate `read_graph_lineage`): `load_publish_state`
/// projects every piece it needs out of this single result.
pub(crate) struct PublishScan {
    pub(crate) table_registrations: HashMap<TableIdentity, TableRegistration>,
    pub(crate) version_entries: Vec<DatasetEntry>,
    /// `(identity, manifest_version)` of each tombstone, mapped to the sealed
    /// Lance data version the tombstone row records.
    pub(crate) tombstones: Vec<((TableIdentity, u64), u64)>,
    pub(crate) lineage_rows: Vec<GraphLineageRow>,
    /// Exact `graph_head:<branch>` rows keyed by the branch suffix (`main` for
    /// main). Absence is meaningful and is preserved by a missing map entry.
    pub(crate) graph_heads: HashMap<String, String>,
    /// The `schema_contract` row's head; absent before the row exists.
    pub(crate) schema_contract: Option<SchemaContractHead>,
    /// The scanned rows themselves, the input of `commit::overwrite`.
    pub(crate) live_rows: Vec<RecordBatch>,
}

pub(crate) async fn read_manifest_table_registrations(
    dataset: &Dataset,
) -> Result<Vec<TableRegistration>> {
    Ok(read_manifest_scan(dataset, false)
        .await?
        .table_registrations
        .into_values()
        .collect())
}

/// One-scan read of everything the publish path needs. `collect_lineage` is
/// always on here (the publisher resolves a parent), so the lineage JSON decode
/// rides the same pass as the table-state assembly instead of a second scan.
pub(crate) async fn read_publish_scan(dataset: &Dataset) -> Result<PublishScan> {
    let scan = read_manifest_scan_with_clocks(dataset, true, None, false, true, false).await?;
    Ok(publish_scan_from_rows(scan))
}

pub(crate) async fn decode_publish_batch(
    dataset: &Dataset,
    batch: RecordBatch,
) -> Result<PublishScan> {
    let batches = futures::stream::iter([expand_from_storage(&batch)]);
    let scan = reduce_manifest_batches(dataset, batches, true, false, false, true, false).await?;
    Ok(publish_scan_from_rows(scan))
}

fn publish_scan_from_rows(scan: ManifestScan) -> PublishScan {
    PublishScan {
        table_registrations: scan.table_registrations,
        version_entries: scan.version_entries,
        tombstones: scan
            .tombstones
            .into_iter()
            .map(|tombstone| {
                (
                    (tombstone.identity, tombstone.manifest_version),
                    tombstone.tombstone_version,
                )
            })
            .collect(),
        lineage_rows: scan.lineage_rows,
        graph_heads: scan.graph_heads,
        schema_contract: scan.schema_contract,
        live_rows: scan.live_rows,
    }
}

/// Decode one `schema_contract` row's head out of its `metadata` column.
/// Shared by the state scan and the content read, so the two cannot drift.
fn decode_schema_contract_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    row: usize,
) -> Result<SchemaContractHead> {
    require_object_id(
        object_ids,
        row,
        SCHEMA_CONTRACT_OBJECT_ID,
        OBJECT_TYPE_SCHEMA_CONTRACT,
    )?;
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(
            "manifest schema_contract row missing metadata".to_string(),
        ));
    }
    serde_json::from_str(metadata.value(row)).map_err(|e| {
        OmniError::manifest_internal(format!("failed to decode schema_contract metadata: {e}"))
    })
}

/// Read the complete contract at the pinned version and verify its folded head.
/// AllEarly prevents Lance 11's heuristic from splitting the packed record
/// into incompatible child projections during the filtered scan.
pub(crate) async fn read_schema_contract_row(
    dataset: &Dataset,
    expected: Option<&SchemaContractHead>,
) -> Result<SchemaContractRow> {
    require_schema_content(dataset)?;
    crate::instrumentation::record_manifest_scan();
    let mut scanner = dataset.scan();
    scanner
        .project(&packed_projection(dataset, true))
        .map_err(OmniError::storage)?;
    scanner.filter_expr(
        datafusion::prelude::col("object_id")
            .eq(datafusion::prelude::lit(SCHEMA_CONTRACT_OBJECT_ID)),
    );
    scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllEarly);
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut found: Option<SchemaContractRow> = None;
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        let batch = expand_from_storage(&batch)?;
        fold_schema_contract_batch(&batch, &mut found)?;
    }
    finish_schema_contract_row(found, expected, dataset.version().version)
}

pub(crate) fn schema_contract_from_batch(
    dataset: &Dataset,
    batch: &RecordBatch,
    expected: Option<&SchemaContractHead>,
) -> Result<SchemaContractRow> {
    require_schema_content(dataset)?;
    let batch = expand_from_storage(batch)?;
    let mut found = None;
    fold_schema_contract_batch(&batch, &mut found)?;
    finish_schema_contract_row(found, expected, dataset.version().version)
}

fn require_schema_content(dataset: &Dataset) -> Result<()> {
    let stamp = read_stamp(dataset);
    if stored_shape(stamp) != StoredShape::Packed
        || SCHEMA_CONTENT_COLUMNS
            .iter()
            .any(|name| dataset.schema().field(name).is_none())
    {
        return Err(OmniError::manifest_internal(format!(
            "__manifest version {} carries no schema_contract row: it was written at internal \
             schema v{}, before the contract lived in the manifest",
            dataset.version().version,
            stamp.map_or("?".to_string(), |stamp| stamp.to_string())
        )));
    }
    for name in SCHEMA_CONTENT_COLUMNS {
        let field = dataset
            .schema()
            .field(name)
            .expect("content field checked above");
        if field.data_type() != arrow_schema::DataType::LargeUtf8 {
            return Err(OmniError::manifest_internal(format!(
                "manifest column '{name}' is {}, expected LargeUtf8",
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn fold_schema_contract_batch(
    batch: &RecordBatch,
    found: &mut Option<SchemaContractRow>,
) -> Result<()> {
    let object_ids = string_column(batch, "object_id")?;
    let object_types = string_column(batch, "object_type")?;
    let metadata = string_column(batch, "metadata")?;
    let sources = large_string_column(batch, SCHEMA_CONTENT_COLUMNS[0])?;
    let irs = large_string_column(batch, SCHEMA_CONTENT_COLUMNS[1])?;
    for row in 0..batch.num_rows() {
        if object_ids.value(row) != SCHEMA_CONTRACT_OBJECT_ID {
            continue;
        }
        if object_types.value(row) != OBJECT_TYPE_SCHEMA_CONTRACT {
            return Err(OmniError::manifest_internal(format!(
                "manifest row '{SCHEMA_CONTRACT_OBJECT_ID}' has object_type '{}'",
                object_types.value(row)
            )));
        }
        let head = decode_schema_contract_row(object_ids, metadata, row)?;
        if sources.is_null(row) || irs.is_null(row) {
            return Err(OmniError::manifest_internal(
                "manifest schema_contract row is missing its source or IR text".to_string(),
            ));
        }
        let contract = SchemaContractRow {
            source: sources.value(row).to_string(),
            ir: irs.value(row).to_string(),
            head,
        };
        if found.replace(contract).is_some() {
            return Err(OmniError::manifest_internal(
                "manifest has two schema_contract rows".to_string(),
            ));
        }
    }
    Ok(())
}

fn finish_schema_contract_row(
    found: Option<SchemaContractRow>,
    expected: Option<&SchemaContractHead>,
    version: u64,
) -> Result<SchemaContractRow> {
    let contract = found.ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "__manifest version {} has no schema_contract row",
            version
        ))
    })?;
    if let Some(expected) = expected
        && *expected != contract.head
    {
        return Err(OmniError::manifest_internal(format!(
            "manifest schema_contract row disagrees with the folded state: the row carries IR \
             hash {} (identity v{} of domain {}), the state {} (identity v{} of domain {})",
            contract.head.schema_ir_hash,
            contract.head.schema_identity_version,
            contract.head.schema_identity_domain,
            expected.schema_ir_hash,
            expected.schema_identity_version,
            expected.schema_identity_domain,
        )));
    }
    Ok(contract)
}

/// Decode one `graph_commit` row (`object_type == OBJECT_TYPE_GRAPH_COMMIT`) into
/// a [`GraphLineageRow`]. The single decode for both lineage readers — the
/// dedicated `read_graph_lineage` scan and the folded `collect_lineage` branch of
/// `read_manifest_scan` — so the two cannot drift. The caller has already matched
/// the object type; `row` indexes into the per-batch columns.
fn decode_graph_commit_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    versions: &UInt64Array,
    branches: &StringArray,
    row: usize,
) -> Result<GraphLineageRow> {
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest graph_commit row missing metadata for {}",
            object_ids.value(row)
        )));
    }
    let commit_meta: GraphCommitMetadata =
        serde_json::from_str(metadata.value(row)).map_err(|e| {
            OmniError::manifest_internal(format!("failed to decode graph_commit metadata: {e}"))
        })?;
    Ok(GraphLineageRow {
        graph_commit_id: object_ids.value(row).to_string(),
        graph_branch: if branches.is_null(row) {
            None
        } else {
            Some(branches.value(row).to_string())
        },
        graph_manifest_version: required_u64(versions, row, "table_version")?,
        parent_commit_id: commit_meta.parent_commit_id,
        merged_parent_commit_id: commit_meta.merged_parent_commit_id,
        actor_id: commit_meta.actor_id,
        created_at: commit_meta.created_at,
    })
}

/// Decode one `graph_head` row into its exact branch-key / commit-id pair.
/// Shared by the dedicated lineage reader and the publisher's folded one-scan
/// path so presence, absence, and malformed-row handling cannot drift.
fn decode_graph_head_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    row: usize,
) -> Result<(String, String)> {
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest graph_head row missing metadata for {}",
            object_ids.value(row)
        )));
    }
    let head_meta: GraphHeadMetadata = serde_json::from_str(metadata.value(row)).map_err(|e| {
        OmniError::manifest_internal(format!("failed to decode graph_head metadata: {e}"))
    })?;
    let branch_key = object_ids
        .value(row)
        .strip_prefix(super::GRAPH_HEAD_OBJECT_ID_PREFIX)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "invalid graph_head object id {}",
                object_ids.value(row)
            ))
        })?
        .to_string();
    Ok((branch_key, head_meta.head_commit_id))
}

fn require_clock_at_or_below(clock: u64, dataset: &Dataset, table_key: &str) -> Result<()> {
    let version = dataset.version().version;
    if clock > version {
        return Err(OmniError::manifest_internal(format!(
            "manifest row for {table_key} carries manifest version {clock} above the scanned \
             dataset version {version}"
        )));
    }
    Ok(())
}

fn registration_clock(
    dataset: &Dataset,
    legacy: bool,
    key_version: u64,
    table_version: u64,
    update_versions: Option<&UInt64Array>,
    row: usize,
    table_key: &str,
) -> Result<u64> {
    if legacy && key_version != table_version {
        return Err(OmniError::manifest_internal(format!(
            "v6 manifest row for {table_key} has key version {key_version}, expected table version {table_version}"
        )));
    }
    let clock = match update_versions {
        Some(versions) => required_u64(versions, row, "_row_last_updated_at_version")?,
        None => key_version,
    };
    if !legacy || update_versions.is_some() {
        require_clock_at_or_below(clock, dataset, table_key)?;
    }
    Ok(clock)
}

async fn read_manifest_scan(dataset: &Dataset, collect_lineage: bool) -> Result<ManifestScan> {
    read_manifest_scan_fragments(dataset, collect_lineage, None).await
}

/// The one manifest row decoder. `fragments = None` scans the whole catalog;
/// `Some(...)` restricts to the given fragments' LIVE rows — the incremental
/// refresh's delta read, which decodes O(appended rows) instead of
/// O(history).
async fn read_manifest_scan_fragments(
    dataset: &Dataset,
    collect_lineage: bool,
    fragments: Option<Vec<lance_table::format::Fragment>>,
) -> Result<ManifestScan> {
    read_manifest_scan_with_clocks(dataset, collect_lineage, fragments, false, false, false).await
}

/// The columns a `__manifest` scan projects: `record` whole when packed, the content
/// columns only when `with_content` (publication or cold admission), the flat
/// logical columns otherwise.
fn manifest_projection(dataset: &Dataset, shape: StoredShape, with_content: bool) -> Vec<String> {
    match shape {
        StoredShape::Packed => packed_projection(dataset, with_content),
        StoredShape::Flat => flat_projection(),
    }
}

fn read_manifest_scan_with_clocks(
    dataset: &Dataset,
    collect_lineage: bool,
    fragments: Option<Vec<lance_table::format::Fragment>>,
    use_row_update_versions: bool,
    retain_live_rows: bool,
    read_contract: bool,
) -> BoxFuture<'_, Result<ManifestScan>> {
    Box::pin(async move {
        let historical;
        let dataset = if let Some(stamp @ (7 | 13)) = super::migrations::read_stamp(dataset)
            && dataset
                .schema()
                .metadata
                .contains_key(crate::migrations::UPGRADE_PENDING_KEY)
        {
            historical =
                Box::pin(crate::migrations::historical_source(dataset.clone(), stamp)).await?;
            &historical
        } else {
            dataset
        };
        let stamp = read_stamp(dataset);
        let shape = stored_shape(stamp);
        crate::instrumentation::record_manifest_scan();
        let mut projection = manifest_projection(dataset, shape, retain_live_rows || read_contract);
        if use_row_update_versions {
            projection.push("_row_last_updated_at_version".to_string());
        }
        let is_delta_scan = fragments.is_some();
        let mut scanner = dataset.scan();
        scanner.project(&projection).map_err(OmniError::storage)?;
        if let Some(fragments) = fragments {
            // Lance semantics this fold depends on: `with_fragments(vec![])`
            // scans ZERO fragments (it does not fall back to all) — an empty
            // delta (a DV-only refresh window) must read nothing.
            scanner.with_fragments(fragments);
        }
        let batches = scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)?
            .map(move |batch| {
                let batch = batch.map_err(OmniError::storage)?;
                match shape {
                    StoredShape::Packed => expand_from_storage(&batch),
                    StoredShape::Flat => Ok(batch),
                }
            });
        reduce_manifest_batches(
            dataset,
            batches,
            collect_lineage,
            use_row_update_versions,
            is_delta_scan,
            retain_live_rows,
            read_contract,
        )
        .await
    })
}

async fn reduce_manifest_batches(
    dataset: &Dataset,
    mut batches: impl Stream<Item = Result<RecordBatch>> + Unpin,
    collect_lineage: bool,
    use_row_update_versions: bool,
    is_delta_scan: bool,
    retain_live_rows: bool,
    read_contract: bool,
) -> Result<ManifestScan> {
    let legacy = read_stamp(dataset) == Some(6);
    let mut table_registrations = HashMap::new();
    let mut version_entries = Vec::new();
    let mut tombstones = Vec::new();
    let mut lineage_rows = Vec::new();
    let mut graph_heads = HashMap::new();
    let mut schema_contract = None;
    let mut schema_contract_row = Ok(None);
    let mut live_rows = Vec::new();

    while let Some(batch) = batches.try_next().await? {
        if read_contract
            && let Ok(found) = &mut schema_contract_row
            && let Err(error) = fold_schema_contract_batch(&batch, found)
        {
            schema_contract_row = Err(error);
        }
        if retain_live_rows {
            live_rows.push(batch.clone());
        }
        // Reduce each batch before polling the next; the owned projection is
        // retained, but Arrow buffers for the complete journal are not.
        let batch = &batch;
        let object_types = string_column(batch, "object_type")?;
        let locations = string_column(batch, "location")?;
        let metadata = string_column(batch, "metadata")?;
        let table_keys = string_column(batch, "table_key")?;
        let stable_table_ids = u64_column(batch, "stable_table_id")?;
        let table_incarnation_ids = u64_column(batch, "table_incarnation_id")?;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;
        let row_counts = u64_column(batch, "row_count")?;
        let update_versions = use_row_update_versions
            .then(|| u64_column(batch, "_row_last_updated_at_version"))
            .transpose()?;
        // `object_id` is needed for the exact graph-head authority even on the
        // table-state path. We still skip every `graph_commit` decode there, so
        // the added work is bounded by the number of branch-head rows rather
        // than growing with commit history.
        let object_ids = string_column(batch, "object_id")?;

        for row in 0..batch.num_rows() {
            let table_key = table_keys.value(row).to_string();
            if object_ids.value(row) == SCHEMA_CONTRACT_OBJECT_ID
                && object_types.value(row) != OBJECT_TYPE_SCHEMA_CONTRACT
            {
                return Err(OmniError::manifest_internal(format!(
                    "manifest row '{SCHEMA_CONTRACT_OBJECT_ID}' has object_type '{}'",
                    object_types.value(row)
                )));
            }
            match object_types.value(row) {
                OBJECT_TYPE_TABLE => {
                    let identity = required_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_TABLE,
                    )?;
                    require_object_id(
                        object_ids,
                        row,
                        &table_object_id(identity),
                        OBJECT_TYPE_TABLE,
                    )?;
                    if locations.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table row missing location for {}",
                            table_key
                        )));
                    }
                    let table_path = locations.value(row).to_string();
                    let canonical_path = super::table_path_for_identity(&table_key, identity)?;
                    if table_path != canonical_path {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table row for identity {identity} has path '{table_path}', \
                             expected '{canonical_path}'"
                        )));
                    }
                    let registration = TableRegistration {
                        identity,
                        table_key,
                        table_path,
                    };
                    if let Some(existing) =
                        table_registrations.insert(identity, registration.clone())
                    {
                        if existing != registration {
                            return Err(OmniError::manifest_internal(format!(
                                "manifest has conflicting table rows for identity {identity}"
                            )));
                        }
                    }
                }
                OBJECT_TYPE_TABLE_VERSION => {
                    let identity = required_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_TABLE_VERSION,
                    )?;
                    let table_version = required_u64(versions, row, "table_version")?;
                    let key_version = manifest_version_from_object_id(
                        object_ids.value(row),
                        identity,
                        OBJECT_TYPE_TABLE_VERSION,
                    )?;
                    let manifest_version = registration_clock(
                        dataset,
                        legacy,
                        key_version,
                        table_version,
                        update_versions,
                        row,
                        &table_key,
                    )?;
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
                    version_entries.push(DatasetEntry {
                        identity,
                        type_key: table_key.clone(),
                        dataset_path: String::new(),
                        published_dataset_version: table_version,
                        native_dataset_branch: table_branch,
                        entity_count: row_count,
                        version_metadata: TableVersionMetadata::from_json_str(metadata.value(row))?,
                        manifest_version,
                    });
                }
                OBJECT_TYPE_TABLE_TOMBSTONE => {
                    let identity = required_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_TABLE_TOMBSTONE,
                    )?;
                    let tombstone_version = required_u64(versions, row, "table_version")?;
                    let key_version = manifest_version_from_object_id(
                        object_ids.value(row),
                        identity,
                        OBJECT_TYPE_TABLE_TOMBSTONE,
                    )?;
                    let manifest_version = registration_clock(
                        dataset,
                        legacy,
                        key_version,
                        tombstone_version,
                        update_versions,
                        row,
                        &table_key,
                    )?;
                    tombstones.push(TableTombstoneEntry {
                        identity,
                        table_key,
                        tombstone_version,
                        manifest_version,
                    });
                }
                // `graph_commit` rows (RFC-013) are decoded ONLY for the publish
                // path, which resolves a parent. The table-state hot path skips
                // them, while still decoding the bounded graph-head authority
                // rows below.
                OBJECT_TYPE_GRAPH_COMMIT => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_GRAPH_COMMIT,
                    )?;
                    if collect_lineage {
                        lineage_rows.push(decode_graph_commit_row(
                            object_ids, metadata, versions, branches, row,
                        )?);
                    }
                }
                OBJECT_TYPE_GRAPH_HEAD => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_GRAPH_HEAD,
                    )?;
                    let (branch_key, head_commit_id) =
                        decode_graph_head_row(object_ids, metadata, row)?;
                    graph_heads.insert(branch_key, head_commit_id);
                }
                OBJECT_TYPE_SCHEMA_CONTRACT => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_SCHEMA_CONTRACT,
                    )?;
                    let head = decode_schema_contract_row(object_ids, metadata, row)?;
                    if schema_contract.replace(head).is_some() {
                        return Err(OmniError::manifest_internal(
                            "manifest has two schema_contract rows".to_string(),
                        ));
                    }
                }
                // Commit rows are skipped on the table-state path; unknown future
                // object types are skipped on every path.
                _ => {}
            }
        }
    }

    version_entries.sort_by(|a, b| {
        a.identity
            .cmp(&b.identity)
            .then(a.manifest_version.cmp(&b.manifest_version))
    });
    // Whole-catalog invariant only: a delta scan's tombstone can reference a
    // registration living in an unread older fragment (a table drop in the
    // refresh window), so the join check would misfire there — the fold's
    // `finish` joins against the ACCUMULATED registration map instead.
    if !is_delta_scan {
        let mut clocks = std::collections::HashSet::new();
        for (identity, clock) in version_entries
            .iter()
            .map(|entry| (entry.identity, entry.manifest_version))
            .chain(
                tombstones
                    .iter()
                    .map(|tombstone| (tombstone.identity, tombstone.manifest_version)),
            )
        {
            if !clocks.insert((identity, clock)) && (!legacy || use_row_update_versions) {
                return Err(OmniError::manifest_internal(format!(
                    "manifest has two rows for identity {identity} at manifest version {clock}"
                )));
            }
        }
        for tombstone in &tombstones {
            let registration = table_registrations
                .get(&tombstone.identity)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest tombstone for identity {} has no table registration",
                        tombstone.identity
                    ))
                })?;
            if registration.table_key != tombstone.table_key {
                return Err(OmniError::manifest_internal(format!(
                    "manifest tombstone identity {} has diagnostic alias '{}', current binding is '{}'",
                    tombstone.identity, tombstone.table_key, registration.table_key
                )));
            }
        }
    }

    Ok(ManifestScan {
        table_registrations,
        version_entries,
        tombstones,
        lineage_rows,
        graph_heads,
        schema_contract,
        schema_contract_row,
        live_rows,
    })
}

/// Project the graph-lineage rows (`graph_commit` + `graph_head`) out of
/// `__manifest` (RFC-013 step 4). Returns every commit and the per-branch head
/// map (keyed by branch name, `"main"` for main). `__manifest` is the single
/// source of graph lineage: the commit-graph cache is sourced from here, and the
/// publisher resolves a new commit's parent from here inside its CAS loop.
///
/// Dedicated scan (separate from `read_manifest_scan`): it decodes ONLY the two
/// lineage object types and builds no table snapshot, so the table-state hot
/// path never pays for lineage JSON and this path never pays for table-entry
/// assembly.
pub async fn read_graph_lineage(
    dataset: &Dataset,
) -> Result<(Vec<GraphLineageRow>, HashMap<String, String>)> {
    crate::instrumentation::record_manifest_scan();
    let shape = stored_shape(read_stamp(dataset));
    let mut scanner = dataset.scan();
    scanner
        .project(&manifest_projection(dataset, shape, false))
        .map_err(OmniError::storage)?;
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;

    let mut graph_commits = Vec::new();
    let mut graph_heads = HashMap::new();

    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        let batch = match shape {
            StoredShape::Packed => expand_from_storage(&batch)?,
            StoredShape::Flat => batch,
        };
        // Reduce each batch before polling the next; the owned projection is
        // retained, but Arrow buffers for the complete journal are not.
        let batch = &batch;
        let object_ids = string_column(batch, "object_id")?;
        let object_types = string_column(batch, "object_type")?;
        let metadata = string_column(batch, "metadata")?;
        let stable_table_ids = u64_column(batch, "stable_table_id")?;
        let table_incarnation_ids = u64_column(batch, "table_incarnation_id")?;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;

        for row in 0..batch.num_rows() {
            match object_types.value(row) {
                OBJECT_TYPE_GRAPH_COMMIT => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_GRAPH_COMMIT,
                    )?;
                    graph_commits.push(decode_graph_commit_row(
                        object_ids, metadata, versions, branches, row,
                    )?);
                }
                OBJECT_TYPE_GRAPH_HEAD => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        OBJECT_TYPE_GRAPH_HEAD,
                    )?;
                    let (branch_key, head_commit_id) =
                        decode_graph_head_row(object_ids, metadata, row)?;
                    graph_heads.insert(branch_key, head_commit_id);
                }
                _ => {}
            }
        }
    }

    Ok((graph_commits, graph_heads))
}

/// The current head of a branch's lineage: the [`GraphLineageRow`] with the
/// greatest `(graph_manifest_version, created_at, graph_commit_id)`. This is the same
/// ordering the commit-graph cache uses to pick its head (`should_replace_head`)
/// — kept in one place so the publisher's per-attempt parent resolution and the
/// cache agree by construction. `None` only for a graph with no commits yet
/// (a parentless genesis).
pub fn head_lineage_row(rows: &[GraphLineageRow]) -> Option<&GraphLineageRow> {
    rows.iter().max_by(|a, b| {
        a.graph_manifest_version
            .cmp(&b.graph_manifest_version)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
    })
}

/// One `__manifest` row materializing a piece of a graph commit's lineage. The
/// publisher maps these onto its `PendingVersionRow`s (folding lineage into the
/// table-version publish batch), and the genesis init path pushes them straight
/// into the init batch.
pub struct GraphLineageRowPart {
    pub object_id: String,
    pub object_type: &'static str,
    pub metadata: String,
    pub table_version: Option<u64>,
    pub table_branch: Option<String>,
}

/// Encode one graph commit into its two `__manifest` rows: the immutable
/// `graph_commit` row plus the mutable `graph_head:<branch>` pointer (a
/// merge-insert on `object_id` updates the head in place). `branch` is `None`
/// for main. The immutable commit fields with no dedicated column live in the
/// `graph_commit` row's `metadata` JSON; the mutable head pointer payload lives
/// in the `graph_head` row's `metadata`.
pub fn graph_lineage_row_parts(
    commit: &GraphLineageRow,
    branch: Option<&str>,
) -> Result<[GraphLineageRowPart; 2]> {
    let commit_metadata = serde_json::to_string(&GraphCommitMetadata {
        parent_commit_id: commit.parent_commit_id.clone(),
        merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
        actor_id: commit.actor_id.clone(),
        created_at: commit.created_at,
    })
    .map_err(|e| {
        OmniError::manifest_internal(format!("failed to encode graph_commit metadata: {e}"))
    })?;
    let head_metadata = serde_json::to_string(&GraphHeadMetadata {
        head_commit_id: commit.graph_commit_id.clone(),
        parent_commit_id: commit.parent_commit_id.clone(),
    })
    .map_err(|e| {
        OmniError::manifest_internal(format!("failed to encode graph_head metadata: {e}"))
    })?;

    Ok([
        // Only the immutable commit row carries the manifest version + branch.
        GraphLineageRowPart {
            object_id: commit.graph_commit_id.clone(),
            object_type: OBJECT_TYPE_GRAPH_COMMIT,
            metadata: commit_metadata,
            table_version: Some(commit.graph_manifest_version),
            table_branch: commit.graph_branch.clone(),
        },
        // The head row reuses `metadata` for its pointer payload.
        GraphLineageRowPart {
            object_id: graph_head_object_id(branch),
            object_type: OBJECT_TYPE_GRAPH_HEAD,
            metadata: head_metadata,
            table_version: None,
            table_branch: None,
        },
    ])
}

/// The `metadata` JSON of a `schema_contract` row carrying `head`.
pub(crate) fn schema_contract_metadata_json(head: &SchemaContractHead) -> Result<String> {
    serde_json::to_string(head).map_err(|e| {
        OmniError::manifest_internal(format!("failed to encode schema_contract metadata: {e}"))
    })
}

pub(crate) fn entries_to_batch(
    entries: &[DatasetEntry],
    version_metadata: &HashMap<TableIdentity, String>,
    genesis_lineage: &[GraphLineageRowPart],
    schema_contract: Option<&SchemaContractRow>,
) -> Result<RecordBatch> {
    let cap = entries.len() * 2 + genesis_lineage.len() + usize::from(schema_contract.is_some());
    let mut object_ids = Vec::with_capacity(cap);
    let mut object_types = Vec::with_capacity(cap);
    let mut locations = Vec::with_capacity(cap);
    let mut metadata = Vec::with_capacity(cap);
    let mut table_keys = Vec::with_capacity(cap);
    let mut table_identities = Vec::with_capacity(cap);
    let mut table_versions = Vec::with_capacity(cap);
    let mut table_branches = Vec::with_capacity(cap);
    let mut row_counts = Vec::with_capacity(cap);
    let mut schema_sources = Vec::with_capacity(cap);
    let mut schema_irs = Vec::with_capacity(cap);

    for entry in entries {
        object_ids.push(table_object_id(entry.identity));
        object_types.push(OBJECT_TYPE_TABLE.to_string());
        locations.push(Some(entry.dataset_path.clone()));
        metadata.push(None);
        table_keys.push(entry.type_key.clone());
        table_identities.push(Some(entry.identity));
        table_versions.push(None);
        table_branches.push(None);
        row_counts.push(None);
        schema_sources.push(None);
        schema_irs.push(None);

        object_ids.push(version_object_id(entry.identity, entry.manifest_version));
        object_types.push(OBJECT_TYPE_TABLE_VERSION.to_string());
        locations.push(None);
        metadata.push(Some(
            version_metadata
                .get(&entry.identity)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing initial version metadata for {}",
                        entry.type_key
                    ))
                })?,
        ));
        table_keys.push(entry.type_key.clone());
        table_identities.push(Some(entry.identity));
        table_versions.push(Some(entry.published_dataset_version));
        table_branches.push(entry.native_dataset_branch.clone());
        row_counts.push(Some(entry.entity_count));
        schema_sources.push(None);
        schema_irs.push(None);
    }

    // Genesis graph-lineage rows ride the init write so a fresh graph carries
    // its `graph_commit` + `graph_head` in `__manifest` from version one (no
    // separate lineage fragment, no second commit). `table_key` is non-nullable
    // but lineage rows have no table identity, so the empty string stands in
    // (never matched by a real key).
    for part in genesis_lineage {
        object_ids.push(part.object_id.clone());
        object_types.push(part.object_type.to_string());
        locations.push(None);
        metadata.push(Some(part.metadata.clone()));
        table_keys.push(String::new());
        table_identities.push(None);
        table_versions.push(part.table_version);
        table_branches.push(part.table_branch.clone());
        row_counts.push(None);
        schema_sources.push(None);
        schema_irs.push(None);
    }

    if let Some(contract) = schema_contract {
        object_ids.push(SCHEMA_CONTRACT_OBJECT_ID.to_string());
        object_types.push(OBJECT_TYPE_SCHEMA_CONTRACT.to_string());
        locations.push(None);
        metadata.push(Some(schema_contract_metadata_json(&contract.head)?));
        table_keys.push(String::new());
        table_identities.push(None);
        table_versions.push(None);
        table_branches.push(None);
        row_counts.push(None);
        schema_sources.push(Some(contract.source.clone()));
        schema_irs.push(Some(contract.ir.clone()));
    }

    manifest_rows_batch(
        object_ids,
        object_types,
        locations,
        metadata,
        table_keys,
        table_identities,
        table_versions,
        table_branches,
        row_counts,
        schema_sources,
        schema_irs,
    )
}

pub(crate) fn manifest_rows_batch(
    object_ids: Vec<String>,
    object_types: Vec<String>,
    locations: Vec<Option<String>>,
    metadata: Vec<Option<String>>,
    table_keys: Vec<String>,
    table_identities: Vec<Option<TableIdentity>>,
    table_versions: Vec<Option<u64>>,
    table_branches: Vec<Option<String>>,
    row_counts: Vec<Option<u64>>,
    schema_sources: Vec<Option<String>>,
    schema_irs: Vec<Option<String>>,
) -> Result<RecordBatch> {
    let len = object_ids.len();
    if table_identities.len() != len {
        return Err(OmniError::manifest_internal(format!(
            "manifest batch has {} object rows but {} table identities",
            len,
            table_identities.len()
        )));
    }
    if schema_sources.len() != len || schema_irs.len() != len {
        return Err(OmniError::manifest_internal(format!(
            "manifest batch has {len} object rows but {} schema sources and {} schema IRs",
            schema_sources.len(),
            schema_irs.len()
        )));
    }
    for (row, (object_type, identity)) in
        object_types.iter().zip(table_identities.iter()).enumerate()
    {
        let carries_content = schema_sources[row].is_some() || schema_irs[row].is_some();
        if carries_content != (object_type == OBJECT_TYPE_SCHEMA_CONTRACT) {
            return Err(OmniError::manifest_internal(format!(
                "manifest {object_type} row at index {row} {} schema contract content",
                if carries_content {
                    "must not carry"
                } else {
                    "is missing its"
                }
            )));
        }
        match object_type.as_str() {
            OBJECT_TYPE_TABLE | OBJECT_TYPE_TABLE_VERSION | OBJECT_TYPE_TABLE_TOMBSTONE => {
                let identity = identity.ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest {object_type} row at index {row} is missing table identity"
                    ))
                })?;
                identity.validate()?;
                let object_id = object_ids.get(row).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest {object_type} row at index {row} is missing object_id"
                    ))
                })?;
                match object_type.as_str() {
                    OBJECT_TYPE_TABLE => {
                        let expected_object_id = table_object_id(identity);
                        if object_id != &expected_object_id {
                            return Err(OmniError::manifest_internal(format!(
                                "manifest {object_type} row at index {row} has object_id {object_id:?}, expected '{expected_object_id}'"
                            )));
                        }
                    }
                    _ => {
                        if table_versions.get(row).copied().flatten().is_none() {
                            return Err(OmniError::manifest_internal(format!(
                                "manifest {object_type} row at index {row} is missing table_version"
                            )));
                        }
                        manifest_version_from_object_id(object_id, identity, object_type)?;
                    }
                }
            }
            OBJECT_TYPE_GRAPH_COMMIT | OBJECT_TYPE_GRAPH_HEAD | OBJECT_TYPE_SCHEMA_CONTRACT
                if identity.is_some() =>
            {
                return Err(OmniError::manifest_internal(format!(
                    "manifest {object_type} row at index {row} must not carry table identity"
                )));
            }
            OBJECT_TYPE_SCHEMA_CONTRACT => {
                if object_ids[row] != SCHEMA_CONTRACT_OBJECT_ID {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest {object_type} row at index {row} has object_id {:?}, expected '{SCHEMA_CONTRACT_OBJECT_ID}'",
                        object_ids[row]
                    )));
                }
                if schema_sources[row].is_none() || schema_irs[row].is_none() {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest {object_type} row at index {row} is missing its source or IR text"
                    )));
                }
            }
            _ => {}
        }
    }
    let stable_table_ids = table_identities
        .iter()
        .map(|identity| identity.map(|identity| identity.stable_table_id))
        .collect::<Vec<_>>();
    let table_incarnation_ids = table_identities
        .iter()
        .map(|identity| identity.map(|identity| identity.table_incarnation_id))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        manifest_schema(),
        vec![
            Arc::new(StringArray::from(object_ids)),
            Arc::new(StringArray::from(object_types)),
            Arc::new(StringArray::from(locations)),
            Arc::new(StringArray::from(metadata)),
            Arc::new(StringArray::from(table_keys)),
            Arc::new(UInt64Array::from(stable_table_ids)),
            Arc::new(UInt64Array::from(table_incarnation_ids)),
            Arc::new(UInt64Array::from(table_versions)),
            Arc::new(StringArray::from(table_branches)),
            Arc::new(UInt64Array::from(row_counts)),
            Arc::new(LargeStringArray::from(schema_sources)),
            Arc::new(LargeStringArray::from(schema_irs)),
        ],
    )
    .map_err(OmniError::arrow_internal)
}

fn large_string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a LargeStringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not LargeUtf8"))
        })
}

pub(crate) fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
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

fn required_table_identity(
    stable_table_ids: &UInt64Array,
    table_incarnation_ids: &UInt64Array,
    row: usize,
    object_type: &str,
) -> Result<TableIdentity> {
    let stable_table_id = required_u64(stable_table_ids, row, "stable_table_id")?;
    let table_incarnation_id = required_u64(table_incarnation_ids, row, "table_incarnation_id")?;
    TableIdentity::new(stable_table_id, table_incarnation_id).map_err(|error| {
        OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} has invalid table identity: {error}"
        ))
    })
}

fn require_null_table_identity(
    stable_table_ids: &UInt64Array,
    table_incarnation_ids: &UInt64Array,
    row: usize,
    object_type: &str,
) -> Result<()> {
    if !stable_table_ids.is_null(row) || !table_incarnation_ids.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} must not carry table identity"
        )));
    }
    Ok(())
}

fn require_object_id(
    object_ids: &StringArray,
    row: usize,
    expected: &str,
    object_type: &str,
) -> Result<()> {
    let actual = object_ids.value(row);
    if actual != expected {
        return Err(OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} has object_id '{actual}', expected '{expected}'"
        )));
    }
    Ok(())
}
