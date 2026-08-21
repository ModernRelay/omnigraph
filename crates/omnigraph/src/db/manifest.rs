use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::branch_control::list_branch_contents;
use crate::error::{OmniError, Result, missing_graph_type_at_snapshot};
use datafusion::logical_expr::Expr;
use lance::Dataset;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::datatypes::{BlobHandling, Schema as LanceSchema};
use lance::index::DatasetIndexExt;
use lance_namespace::models::CreateTableVersionRequest;
use lance_table::format::IndexMetadata;
use omnigraph_compiler::catalog::Catalog;

#[path = "manifest/graph.rs"]
mod graph;
#[path = "manifest/layout.rs"]
mod layout;
#[path = "manifest/metadata.rs"]
mod metadata;
#[path = "manifest/migrations.rs"]
mod migrations;
// Entirely test-only since RFC-013 step 3a: with both reads (Fix 2) and writes
// bypassing the Lance namespace, nothing in production routes through it; the
// `LanceNamespace` impls are retained only to validate the contract in unit tests.
#[cfg(test)]
#[path = "manifest/namespace.rs"]
mod namespace;
#[path = "manifest/publisher.rs"]
mod publisher;
#[path = "manifest/recovery.rs"]
mod recovery;
#[path = "manifest/state.rs"]
mod state;

pub(crate) use graph::{GenesisManifestAttempt, ManifestInitError};
use graph::{
    init_manifest_graph, load_initial_manifest_state, open_exact_genesis_manifest,
    open_manifest_graph, snapshot_state_at,
};
pub(crate) use layout::manifest_uri;
#[cfg(test)]
use layout::open_manifest_dataset;
use layout::{
    branch_ref_error, open_manifest_dataset_with_identifier_with_session,
    open_manifest_dataset_with_session, table_uri_for_path,
};
pub(crate) use metadata::TableVersionMetadata;
#[cfg(test)]
use metadata::{OMNIGRAPH_ROW_COUNT_KEY, table_version_metadata_for_state};
#[cfg(test)]
use namespace::{branch_manifest_namespace, staged_table_namespace};
pub(crate) use publisher::{GraphHeadExpectation, LineageIntent, PublishPrecondition};
use publisher::{GraphNamespacePublisher, ManifestBatchPublisher, PublishOutcome};
#[cfg(test)]
pub(crate) use recovery::MAX_EFFECT_IDENTITY_SCAN_VERSIONS;
pub(crate) use recovery::{
    HealPendingOutcome, MAX_BRANCH_MERGE_DATA_TRANSACTIONS, RecoveryAuthorityToken,
    RecoveryBranchMergeEffect, RecoveryBranchMergeEffectKind, RecoveryLineageIntent,
    RecoveryManifestDelta, RecoveryMode, RecoverySchemaApplyEffect, RecoverySchemaApplyEffectKind,
    RecoverySidecar, RecoverySidecarHandle, RecoveryTableUpdateSlot, SidecarKind, SidecarTablePin,
    SidecarTableRegistration, SidecarTableRename, SidecarTombstone,
    confirm_branch_merge_sidecar_v9, confirm_ensure_indices_sidecar_v9, confirm_occ_sidecar_v9,
    confirm_schema_apply_sidecar_v9, delete_sidecar, ensure_read_only_schema_coherent,
    finalize_effect_free_occ_sidecar, heal_pending_sidecars_roll_forward, list_sidecars,
    new_branch_merge_sidecar_v9, new_ensure_indices_sidecar_v9, new_occ_sidecar_v9,
    new_optimize_sidecar_v9, new_schema_apply_sidecar_v9, recover_manifest_drift,
    schema_apply_serial_queue_key, write_sidecar,
};
pub use state::DatasetEntry;
#[cfg(test)]
use state::string_column;
pub(crate) use state::{GraphLineageRow, read_graph_lineage};
use state::{
    ManifestState, ProjectionAccumulator, fold_projection_delta, read_manifest_projection,
    read_manifest_state, read_object_identities_at_offsets,
};

/// The internal-schema (storage-format) version this binary writes and reads.
/// A graph's on-disk per-branch stamp is read via [`internal_schema_stamp_at`];
/// this const is the binary's CURRENT. Surfaced to operators via `omnigraph
/// snapshot` and `omnigraph --version`.
pub const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = migrations::INTERNAL_MANIFEST_SCHEMA_VERSION;

const OBJECT_TYPE_TABLE: &str = "table";
const OBJECT_TYPE_TABLE_VERSION: &str = "table_version";
const OBJECT_TYPE_TABLE_TOMBSTONE: &str = "table_tombstone";
/// Immutable per-commit graph-lineage row (RFC-013 Phase 7). One row per graph
/// commit; the projected form reconstructs a [`GraphCommit`]. `__manifest` is
/// the single source — written in the same publish CAS as the table-version
/// rows (no `_graph_commits.lance` row).
const OBJECT_TYPE_GRAPH_COMMIT: &str = "graph_commit";
/// Mutable per-branch head pointer for the graph lineage (RFC-013 Phase 7).
/// `object_id` is `graph_head:<branch>` (`graph_head:main` for the main branch).
const OBJECT_TYPE_GRAPH_HEAD: &str = "graph_head";
/// `object_id` prefix of the head rows — one constant for row minting, row
/// decode, and the incremental fold's dead-row classification.
pub(super) const GRAPH_HEAD_OBJECT_ID_PREFIX: &str = "graph_head:";

/// Stable head-key segment for the main branch in `graph_head:<branch>` rows.
/// `table_branch`/`manifest_branch` encode main as null, but `object_id` must be
/// non-null, so the head row needs a literal — matching the `"main"` sentinel
/// already used by `SnapshotId::synthetic` and `open_for_branch`.
pub(crate) const MAIN_BRANCH_HEAD_KEY: &str = "main";

/// The result of a manifest commit that may have folded in a graph commit
/// (RFC-013 Phase 7).
#[derive(Debug, Clone)]
pub(crate) struct CommitOutcome {
    /// The new `__manifest` version after the publish.
    pub version: u64,
    /// The parent the publisher resolved for the recorded commit, or `None` when
    /// no lineage was recorded or the commit is the genesis. Lets the caller
    /// update its in-memory commit cache without re-reading the manifest.
    pub parent_commit_id: Option<String>,
}

/// The on-disk internal-schema stamp of `__manifest` at `branch` (main when
/// `None`), or `None` when no parseable stamp exists (a torn init by an
/// older binary, a genuine pre-stamp store, or corrupt metadata — see
/// `migrations::guard_stamp`, which the open paths use instead). Surfaces
/// the storage version to operators (`omnigraph snapshot`).
pub(crate) async fn internal_schema_stamp_at(
    root_uri: &str,
    branch: Option<&str>,
) -> Result<Option<u32>> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(root_uri, branch, &control_session).await?;
    Ok(migrations::read_stamp(&dataset))
}

/// Refuse to open a graph whose `__manifest` (main) is stamped outside this
/// binary's supported internal-schema range (newer than CURRENT, or older than
/// MIN_SUPPORTED). Both open paths (read-write and read-only) call this before
/// reading any data, so an old binary refuses a newer graph instead of silently
/// misreading it, and this binary refuses a below-floor graph with a
/// rebuild-via-export/import message instead of opening a format it can't read.
///
/// The stamp is gated at the GRAPH level (main only). It is a graph-wide
/// storage-format property — the upgrade path is a whole-graph export/import, so
/// with one binary version every branch is always CURRENT (init stamps main,
/// `create_branch` forks the stamp, the publisher writes rows without
/// re-stamping). A branch stamped out of range while main stays in range is only
/// reachable with concurrent multi-version writers, an unsupported topology
/// (writes are refused per-branch by the publisher; a newer binary advancing
/// main is refused here). See the matching known gap in `docs/dev/invariants.md`.
pub(crate) async fn refuse_if_internal_schema_unsupported(root_uri: &str) -> Result<()> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(root_uri, None, &control_session).await?;
    migrations::guard_stamp(&dataset).map(|_| ())
}

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    root_uri: String,
    version: u64,
    entries: HashMap<String, DatasetEntry>,
    /// Exact materialized `graph_head:<branch>` commit ids from this SAME
    /// pinned manifest version (see `ManifestState::graph_heads`). Carried so
    /// a read can report the commit id of the world it was served from —
    /// resolving the head separately (e.g. via `CommitGraph`) could pair this
    /// snapshot's datasets with a different version's head.
    graph_heads: HashMap<String, String>,
    /// Per-graph read caches (shared `Session` + held-handle cache), injected by
    /// `Omnigraph::resolved_target` for live Branch reads so dataset opens reuse
    /// handles (0 IO on a warm repeat) and one `Session`. `None` for write-prelude
    /// snapshots, time-travel / Snapshot-id reads, and directly-built test
    /// snapshots, which fall back to a plain open.
    read_caches: Option<Arc<crate::runtime_cache::ReadCaches>>,
}

/// Read-only view of one backing dataset pinned by a [`Snapshot`].
///
/// The underlying Lance [`Dataset`] is deliberately private: a snapshot dataset
/// can scan rows and inspect read metadata, but it cannot reach Lance's
/// mutating APIs or advance a dataset HEAD outside OmniGraph's coordinated write
/// path.
#[derive(Debug, Clone)]
pub struct SnapshotDataset {
    dataset: Dataset,
}

/// Read-only scan builder for a [`SnapshotDataset`].
///
/// This forwards scan configuration and execution, but not Lance's raw
/// [`Scanner`] or physical-plan construction. A Lance physical scan plan
/// exposes its embedded [`Dataset`], which would let SDK callers recover a
/// writable handle and bypass graph publication.
pub struct SnapshotScanner {
    scanner: Scanner,
}

impl SnapshotScanner {
    /// Select the output columns.
    pub fn project<T: AsRef<str>>(&mut self, columns: &[T]) -> Result<&mut Self> {
        self.scanner
            .project(columns)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        Ok(self)
    }

    /// Apply a SQL filter expression.
    pub fn filter(&mut self, filter: &str) -> Result<&mut Self> {
        self.scanner
            .filter(filter)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        Ok(self)
    }

    /// Apply a structured DataFusion filter expression.
    pub fn filter_expr(&mut self, filter: Expr) -> &mut Self {
        self.scanner.filter_expr(filter);
        self
    }

    /// Set the requested number of rows returned in one scan batch.
    ///
    /// Lance's byte-based batch target overrides this setting unless
    /// [`Self::strict_batch_size`] is also enabled.
    pub fn batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.scanner.batch_size(batch_size);
        self
    }

    /// Set the approximate in-memory byte target for one scan batch.
    pub fn batch_size_bytes(&mut self, batch_size_bytes: u64) -> &mut Self {
        self.scanner.batch_size_bytes(batch_size_bytes);
        self
    }

    /// Require full output batches to contain exactly the requested row count.
    ///
    /// The final batch may contain fewer rows. This restores a hard output-row
    /// ceiling when [`Self::batch_size_bytes`] is also configured.
    pub fn strict_batch_size(&mut self, strict_batch_size: bool) -> &mut Self {
        self.scanner.strict_batch_size(strict_batch_size);
        self
    }

    /// Apply a row limit and offset.
    pub fn limit(&mut self, limit: Option<i64>, offset: Option<i64>) -> Result<&mut Self> {
        self.scanner
            .limit(limit, offset)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        Ok(self)
    }

    /// Include Lance's stable row-id column in the output.
    pub fn with_row_id(&mut self) -> &mut Self {
        self.scanner.with_row_id();
        self
    }

    /// Choose how blob columns are represented in scan output.
    pub fn blob_handling(&mut self, blob_handling: BlobHandling) -> &mut Self {
        self.scanner.blob_handling(blob_handling);
        self
    }

    /// Execute the configured read without exposing its physical plan.
    pub async fn try_into_stream(&self) -> Result<DatasetRecordBatchStream> {
        self.scanner
            .try_into_stream()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))
    }
}

impl SnapshotDataset {
    fn new(dataset: Dataset) -> Self {
        Self { dataset }
    }

    /// Build a read-only scanner over this pinned dataset version.
    pub fn scan(&self) -> SnapshotScanner {
        SnapshotScanner {
            scanner: self.dataset.scan(),
        }
    }

    /// Count physical rows in this pinned dataset version, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        self.dataset
            .count_rows(filter)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))
    }

    /// Lance schema of this pinned dataset version.
    pub fn schema(&self) -> &LanceSchema {
        self.dataset.schema()
    }

    /// Lance version of this pinned dataset.
    pub fn published_dataset_version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Read-only physical index metadata for this pinned dataset version.
    pub async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>> {
        self.dataset
            .load_indices()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))
    }

    /// Whether `column` has complete usable BTREE coverage.
    pub async fn index_coverage(&self, column: &str) -> Result<crate::IndexCoverage> {
        crate::table_store::TableStore::key_column_index_coverage(&self.dataset, column).await
    }

    /// Whether any user index leaves current fragments uncovered.
    pub async fn has_unindexed_fragments(&self) -> Result<bool> {
        crate::table_store::TableStore::has_unindexed_fragments(&self.dataset).await
    }

    /// Whether this dataset has a user BTREE index on physical `column`.
    pub async fn has_btree_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_btree_index_on(&self.dataset, column).await
    }

    /// Whether this dataset has a user full-text index on physical `column`.
    pub async fn has_fts_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_fts_index_on(&self.dataset, column).await
    }

    /// Whether this dataset has a user vector index on physical `column`.
    pub async fn has_vector_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_vector_index_on(&self.dataset, column).await
    }
}

impl Snapshot {
    /// Exact `graph_head:<branch>` commit id from this snapshot's own pinned
    /// graph-manifest version (`None` = main). Absent on a branch with no commits.
    ///
    /// This exact row is write authority when present. A fresh named branch has
    /// no materialized row yet; callers that need its effective inherited
    /// lineage head must resolve it through `GraphCoordinator`.
    pub fn graph_head(&self, branch: Option<&str>) -> Option<&str> {
        let branch_key = branch.unwrap_or(MAIN_BRANCH_HEAD_KEY);
        self.graph_heads.get(branch_key).map(String::as_str)
    }

    /// Bind the current accepted catalog's aliases onto a historical snapshot
    /// by immutable table identity for query execution.
    ///
    /// Public historical snapshots retain their original aliases. This method
    /// is used only on the operation-local copy behind `run_query_at` and an
    /// explicit snapshot-target query, whose source is typechecked against the
    /// current catalog. A pure type rename can therefore address the same old
    /// table lifetime under its current name. Conversely, a reused name whose
    /// current identity is absent from the historical snapshot is removed from
    /// the execution view, preventing cross-incarnation adoption.
    pub(crate) fn bind_catalog_aliases(&mut self, catalog: &Catalog) -> Result<()> {
        // Freeze the historical identity map before changing any aliases. A
        // renamed-away alias may later be reused by a new live type; resolving
        // that replacement first must not remove the only entry needed to bind
        // the original identity under its current renamed alias.
        let historical_by_identity = self
            .entries
            .values()
            .cloned()
            .map(|entry| (entry.identity, entry))
            .collect::<HashMap<_, _>>();
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "historical query alias binding requires an identity-bound catalog".to_string(),
            )
        })?;
        let table_aliases = schema_ir
            .nodes
            .iter()
            .map(|node| {
                (
                    format!("node:{}", node.name),
                    node.type_id.get(),
                    node.table_incarnation_id.get(),
                )
            })
            .chain(schema_ir.edges.iter().map(|edge| {
                (
                    format!("edge:{}", edge.name),
                    edge.type_id.get(),
                    edge.table_incarnation_id.get(),
                )
            }))
            .collect::<Vec<_>>();

        for (table_key, stable_table_id, incarnation_id) in table_aliases {
            let identity = TableIdentity::new(stable_table_id, incarnation_id)?;

            // An exact alias belonging to another lifetime is actively unsafe:
            // remove it before looking for this catalog type's identity under a
            // historical name.
            self.entries.remove(&table_key);
            if let Some(entry) = historical_by_identity.get(&identity) {
                self.entries.insert(table_key, entry.clone());
            }
        }
        Ok(())
    }

    /// Open a backing dataset at its pinned version by qualified graph type key. With read caches present
    /// (live Branch reads), reuse a held handle through the cache (0 open IO on a
    /// warm repeat) and the shared `Session`; otherwise plain-open (Fix 2).
    pub async fn open_dataset(&self, type_key: &str) -> Result<SnapshotDataset> {
        self.open_lance_dataset(type_key)
            .await
            .map(SnapshotDataset::new)
    }

    /// Open the raw Lance dataset for engine-internal read execution.
    ///
    /// This stays crate-private so downstream SDK callers cannot obtain a
    /// writable `Dataset` from a logical graph snapshot.
    pub(crate) async fn open_lance_dataset(&self, type_key: &str) -> Result<Dataset> {
        let entry = self
            .entries
            .get(type_key)
            .ok_or_else(|| OmniError::manifest(missing_graph_type_at_snapshot(type_key)))?;
        match &self.read_caches {
            Some(caches) => {
                let location = table_uri_for_path(
                    &self.root_uri,
                    &entry.dataset_path,
                    entry.native_dataset_branch.as_deref(),
                );
                caches
                    .handles
                    .get_or_open(
                        &entry.dataset_path,
                        entry.native_dataset_branch.as_deref(),
                        entry.published_dataset_version,
                        entry.version_metadata.e_tag(),
                        &location,
                        Some(&caches.session),
                    )
                    .await
            }
            None => entry.open(&self.root_uri, None).await,
        }
    }

    /// Attach per-graph read caches (shared `Session` + handle cache) so this
    /// snapshot's dataset opens reuse handles and the session. Set by
    /// `Omnigraph::resolved_target` for live Branch reads only.
    pub(crate) fn set_read_caches(&mut self, caches: Arc<crate::runtime_cache::ReadCaches>) {
        self.read_caches = Some(caches);
    }

    /// Graph-manifest version this snapshot was taken from.
    pub fn graph_manifest_version(&self) -> u64 {
        self.version
    }

    /// Look up backing-dataset metadata by qualified graph type key.
    pub fn dataset(&self, type_key: &str) -> Option<&DatasetEntry> {
        self.entries.get(type_key)
    }

    /// Iterate over metadata for every backing dataset in this snapshot.
    pub fn datasets(&self) -> impl Iterator<Item = &DatasetEntry> {
        self.entries.values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestIncarnation {
    pub(crate) version: u64,
    pub(crate) e_tag: Option<String>,
    timestamp_nanos: Option<u128>,
    branch_identifier: lance::dataset::refs::BranchIdentifier,
}

impl ManifestIncarnation {
    pub(crate) fn matches(&self, held: &Self) -> bool {
        if self.version != held.version || self.branch_identifier != held.branch_identifier {
            return false;
        }
        match (&self.e_tag, &held.e_tag) {
            (Some(latest), Some(current)) => latest == current,
            _ => match (self.timestamp_nanos, held.timestamp_nanos) {
                (Some(latest), Some(current)) => latest == current,
                // Some object stores can omit both e_tag and manifest timestamp
                // from the reachable API. In that narrow case the version-number
                // probe is the strongest available identity.
                _ => true,
            },
        }
    }
}

/// Immutable probe handle captured from one exact manifest dataset
/// incarnation.
///
/// It retains only Lance's pinned Dataset handle plus the active branch and
/// incarnation token. It does not carry mutable graph-head state or a lineage
/// projection, so callers cannot mistake it for publish authority.
#[derive(Debug, Clone)]
pub(crate) struct CapturedManifestProbe {
    dataset: Dataset,
    active_branch: Option<String>,
    captured: ManifestIncarnation,
}

impl CapturedManifestProbe {
    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        crate::instrumentation::record_probe();
        probe_dataset_latest_incarnation(&self.dataset, self.active_branch.as_deref()).await
    }

    pub(crate) async fn is_current(&self) -> Result<bool> {
        Ok(self
            .probe_latest_incarnation()
            .await?
            .matches(&self.captured))
    }
}

async fn probe_dataset_latest_incarnation(
    dataset: &Dataset,
    active_branch: Option<&str>,
) -> Result<ManifestIncarnation> {
    if active_branch.is_none() {
        return Ok(ManifestIncarnation {
            version: dataset
                .latest_version_id()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?,
            e_tag: dataset.manifest_location().e_tag.clone(),
            timestamp_nanos: Some(dataset.manifest().timestamp_nanos),
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
        });
    }
    let branch = active_branch.expect("named-branch arm checked above");
    // A named branch's native identifier is its lifetime witness. Pair it with
    // the branch-local latest version instead of loading the manifest body:
    // version changes catch ordinary commits, while the identifier catches a
    // same-source delete/recreate even when version, e-tag, and timestamp all
    // repeat. Read the version first so a recreation between the two probes
    // yields the replacement identifier rather than a false match to the held
    // lifetime.
    let version = dataset
        .latest_version_id()
        .await
        .map_err(|error| branch_ref_error(error, branch))?;
    let branch_identifier = dataset
        .branch_identifier()
        .await
        .map_err(|error| branch_ref_error(error, branch))?;
    Ok(ManifestIncarnation {
        version,
        e_tag: None,
        timestamp_nanos: None,
        branch_identifier,
    })
}

impl DatasetUpdate {
    pub(crate) fn to_create_table_version_request(&self) -> CreateTableVersionRequest {
        self.version_metadata.to_create_table_version_request(
            &self.type_key,
            self.published_dataset_version,
            self.entity_count,
            self.native_dataset_branch.as_deref(),
        )
    }
}

/// Immutable graph-level identity of one physical table lifetime.
///
/// `stable_table_id` survives supported type renames. Dropping and re-adding a
/// type mints a new `table_incarnation_id`, so old version and tombstone rows
/// can never alias the replacement even when it reuses the same display name.
/// Both components are non-zero; zero remains the sentinel for "no table" in
/// external formats and is never persisted on a table-bearing manifest row.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct TableIdentity {
    pub(crate) stable_table_id: u64,
    pub(crate) table_incarnation_id: u64,
}

impl TableIdentity {
    pub(crate) fn new(stable_table_id: u64, table_incarnation_id: u64) -> Result<Self> {
        let identity = Self {
            stable_table_id,
            table_incarnation_id,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn validate(self) -> Result<()> {
        if self.stable_table_id == 0 || self.table_incarnation_id == 0 {
            return Err(OmniError::manifest(format!(
                "table identity components must be non-zero (stable_table_id={}, \
                 table_incarnation_id={})",
                self.stable_table_id, self.table_incarnation_id
            )));
        }
        Ok(())
    }
}

impl std::fmt::Display for TableIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:016x}:{:016x}",
            self.stable_table_id, self.table_incarnation_id
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRegistration {
    pub(crate) identity: TableIdentity,
    pub(crate) table_key: String,
    pub(crate) table_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableTombstone {
    pub(crate) identity: TableIdentity,
    pub(crate) table_key: String,
    pub(crate) tombstone_version: u64,
}

/// Metadata-only rebinding of one live table identity to a new alias.
///
/// `table_path` is the path the caller observed. The publisher requires it to
/// equal the currently registered path and rewrites only the registration row;
/// no `table_version` row is emitted, so the Lance version is preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRename {
    pub(crate) identity: TableIdentity,
    pub(crate) expected_table_key: String,
    pub(crate) table_key: String,
    pub(crate) table_path: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ManifestChange {
    Update(DatasetUpdate),
    RegisterTable(TableRegistration),
    RenameTable(TableRename),
    Tombstone(TableTombstone),
}

/// One table-version authority assertion supplied to a publish attempt.
///
/// The map key is the immutable table identity. `table_key` is retained only
/// as a diagnostic binding and is itself checked, so a caller prepared before a
/// metadata-only rename cannot accidentally publish against the renamed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableVersionExpectation {
    pub(crate) table_key: String,
    pub(crate) table_version: u64,
}

pub(crate) type ExpectedTableVersions = HashMap<TableIdentity, TableVersionExpectation>;

impl DatasetEntry {
    /// Open this dataset at its pinned version directly by location (Fix 2),
    /// without the Lance namespace — which would full-scan `__manifest` twice per
    /// open (`describe_table` + `describe_table_version`). The resolved Snapshot
    /// already holds the path, version, and branch. Branches are Lance native
    /// branches, so `with_branch` resolves `{base}/tree/{branch}` from the base
    /// URI; main uses `with_version`.
    pub(crate) async fn open(
        &self,
        root_uri: &str,
        session: Option<&Arc<lance::session::Session>>,
    ) -> Result<Dataset> {
        // The branch-qualified location is the dataset that physically holds this
        // version: main at `{table_path}`, a branch at
        // `{table_path}/tree/{branch}` (Lance native-branch storage). `with_version`
        // then resolves the version within THAT dataset's `_versions` — a branch
        // version lives under `tree/{branch}/_versions`, not the base. This
        // matches the physical layout the namespace path resolved, without the
        // per-open `__manifest` scan.
        let location = table_uri_for_path(
            root_uri,
            &self.dataset_path,
            self.native_dataset_branch.as_deref(),
        );
        // Route through the one opener (Fix 3). With no session this is exactly
        // the Fix-2 `from_uri(location).with_version`. This is the uncached
        // fallback (a snapshot detached from its graph's read caches); the
        // cached path (`Snapshot::open_lance_dataset` → handle cache) calls the same opener on
        // a miss with the shared session, so both paths count on the per-query
        // `table_wrapper`.
        crate::instrumentation::open_dataset(
            &location,
            crate::instrumentation::VersionResolution::At(self.published_dataset_version),
            session,
            crate::instrumentation::table_wrapper(),
        )
        .await
    }
}

pub(crate) fn table_path_for_identity(table_key: &str, identity: TableIdentity) -> Result<String> {
    if table_key.strip_prefix("node:").is_some() {
        return Ok(format!(
            "nodes/{:016x}-{:016x}",
            identity.stable_table_id, identity.table_incarnation_id
        ));
    }
    if table_key.strip_prefix("edge:").is_some() {
        return Ok(format!(
            "edges/{:016x}-{:016x}",
            identity.stable_table_id, identity.table_incarnation_id
        ));
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

/// An update to apply to the manifest via `commit`.
#[derive(Debug, Clone)]
pub struct DatasetUpdate {
    pub(crate) identity: TableIdentity,
    pub type_key: String,
    pub published_dataset_version: u64,
    pub native_dataset_branch: Option<String>,
    pub entity_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
}

/// The set of deleted row offsets of one fragment (empty when it has no
/// deletion vector).
async fn fragment_deletion_offsets(
    fragment: &lance::dataset::fragment::FileFragment,
) -> Result<std::collections::HashSet<u32>> {
    Ok(fragment
        .get_deletion_vector()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
        .map(|dv| dv.as_ref().iter().collect())
        .unwrap_or_default())
}

/// Coordinates cross-dataset state through the namespace `__manifest` table.
///
/// Table rows register stable metadata such as location. Append-only
/// `table_version` rows are the graph publish boundary and reconstruct the
/// current graph snapshot by selecting the latest visible version row per
/// sub-table.
pub(crate) struct ManifestCoordinator {
    root_uri: String,
    dataset: Dataset,
    known_state: ManifestState,
    active_branch: Option<String>,
    /// Lance-native lifetime captured coherently with `dataset` and
    /// `known_state`. A named ref keeps this value across ordinary commits and
    /// receives a new value after delete/recreate.
    branch_identifier: lance::dataset::refs::BranchIdentifier,
    publisher: Arc<dyn ManifestBatchPublisher>,
    /// Retained fold accumulators of the projection, tagged with the manifest
    /// version they describe; `refresh_with_lineage` folds only the catalog
    /// fragments appended since that version instead of re-scanning
    /// O(history). The tag is the staleness fence: a path that advances
    /// `dataset` without folding must clear this, and a missed clear is
    /// caught by the tag mismatch — stale accumulators degrade to a full
    /// scan, never serve as current. One extra lineage-row copy beside the
    /// commit graph's; the maps are O(tables + branches).
    projection: Option<(u64, ProjectionAccumulator)>,
}

impl ManifestCoordinator {
    fn default_batch_publisher(
        root_uri: &str,
        active_branch: Option<&str>,
        control_session: Arc<lance::session::Session>,
    ) -> Arc<dyn ManifestBatchPublisher> {
        Arc::new(GraphNamespacePublisher::new_with_session(
            root_uri,
            active_branch,
            control_session,
        ))
    }

    fn from_parts(
        root_uri: &str,
        dataset: Dataset,
        known_state: ManifestState,
        active_branch: Option<String>,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
        publisher: Arc<dyn ManifestBatchPublisher>,
    ) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            dataset,
            known_state,
            active_branch,
            branch_identifier,
            publisher,
            projection: None,
        }
    }

    fn from_parts_with_default_publisher(
        root_uri: &str,
        dataset: Dataset,
        known_state: ManifestState,
        active_branch: Option<String>,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
    ) -> Self {
        let publisher =
            Self::default_batch_publisher(root_uri, active_branch.as_deref(), dataset.session());
        Self::from_parts(
            root_uri,
            dataset,
            known_state,
            active_branch,
            branch_identifier,
            publisher,
        )
    }

    fn snapshot_from_state(root_uri: &str, state: ManifestState) -> Snapshot {
        Snapshot {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            version: state.version,
            entries: state
                .entries
                .into_iter()
                .map(|entry| (entry.type_key.clone(), entry))
                .collect(),
            graph_heads: state.graph_heads,
            read_caches: None,
        }
    }

    #[cfg(test)]
    fn with_batch_publisher(mut self, publisher: Arc<dyn ManifestBatchPublisher>) -> Self {
        self.publisher = publisher;
        self
    }

    /// Test-only compatibility helper for manifest fixtures that need only the
    /// coordinator and not the coherently decoded lineage rows.
    #[cfg(test)]
    pub(crate) async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        let (coordinator, _) = Self::init_with_lineage(root_uri, catalog, &control_session).await?;
        Ok(coordinator)
    }

    /// Test-only composition of the two init halves; production init goes
    /// through them separately so the commit point is a caller-visible
    /// boundary (issue #495).
    #[cfg(test)]
    pub(crate) async fn init_with_lineage(
        root_uri: &str,
        catalog: &Catalog,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(Self, Vec<GraphLineageRow>)> {
        let attempt = GenesisManifestAttempt::mint()?;
        let dataset = Self::init_commit(root_uri, catalog, control_session, &attempt).await?;
        Self::finish_init(root_uri, dataset).await
    }

    /// Commit half of manifest init; ends at the `__manifest` Create commit
    /// (assembled in `init_manifest_graph`).
    pub(crate) async fn init_commit(
        root_uri: &str,
        catalog: &Catalog,
        control_session: &Arc<lance::session::Session>,
        attempt: &GenesisManifestAttempt,
    ) -> std::result::Result<Dataset, ManifestInitError> {
        init_manifest_graph(
            root_uri.trim_end_matches('/'),
            catalog,
            control_session,
            attempt,
        )
        .await
    }

    /// Probe an acknowledgement-unknown manifest Create and accept only the
    /// exact immutable genesis receipt minted by this initialization attempt.
    /// A transport/read/mismatch error remains indeterminate to the caller;
    /// this method never turns absence or ambiguity into cleanup authority.
    pub(crate) async fn open_exact_genesis_with_lineage(
        root_uri: &str,
        attempt: &GenesisManifestAttempt,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(Self, Vec<GraphLineageRow>)> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state, lineage_rows) =
            open_exact_genesis_manifest(root, attempt, control_session).await?;
        Ok((
            Self::from_parts_with_default_publisher(
                root,
                dataset,
                known_state,
                None,
                lance::dataset::refs::BranchIdentifier::main(),
            ),
            lineage_rows,
        ))
    }

    /// Post-commit half of manifest init: reads the committed state back and
    /// assembles the coordinator; see `init_post_commit_checks` for the
    /// caller contract.
    pub(crate) async fn finish_init(
        root_uri: &str,
        dataset: Dataset,
    ) -> Result<(Self, Vec<GraphLineageRow>)> {
        let root = root_uri.trim_end_matches('/');
        let (known_state, lineage_rows) = load_initial_manifest_state(&dataset).await?;
        Ok((
            Self::from_parts_with_default_publisher(
                root,
                dataset,
                known_state,
                None,
                lance::dataset::refs::BranchIdentifier::main(),
            ),
            lineage_rows,
        ))
    }

    /// Open an existing graph's manifest.
    pub async fn open(root_uri: &str) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_with_session(root_uri, &control_session).await
    }

    pub(crate) async fn open_with_session(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state, branch_identifier) =
            open_manifest_graph(root, None, control_session).await?;
        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            None,
            branch_identifier,
        ))
    }

    /// Open an existing graph's manifest at a specific branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_at_branch_with_session(root_uri, branch, &control_session).await
    }

    pub(crate) async fn open_at_branch_with_session(
        root_uri: &str,
        branch: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        if branch == "main" {
            return Self::open_with_session(root_uri, control_session).await;
        }

        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state, branch_identifier) =
            open_manifest_graph(root, Some(branch), control_session).await?;
        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            Some(branch.to_string()),
            branch_identifier,
        ))
    }

    pub(crate) async fn open_with_lineage(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(Self, Vec<GraphLineageRow>)> {
        // Boxed wholesale for the same stack-depth reason as
        // `refresh_with_lineage`: this body now builds the projection
        // accumulators and is awaited inside the merge future via
        // coordinator swaps.
        Box::pin(Self::open_with_lineage_inner(
            root_uri,
            branch,
            control_session,
        ))
        .await
    }

    async fn open_with_lineage_inner(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(Self, Vec<GraphLineageRow>)> {
        let root = root_uri.trim_end_matches('/');
        let branch = branch.filter(|branch| *branch != "main");
        // Retain the fold accumulators alongside the state (the incremental merge-authority projection): the
        // scan this open pays anyway becomes the base a later refresh folds
        // deltas into, instead of a sunk cost repeated per refresh.
        let (dataset, branch_identifier) =
            open_manifest_dataset_with_identifier_with_session(root, branch, control_session)
                .await?;
        let (known_state, projection) = read_manifest_projection(&dataset).await?;
        let lineage_rows = projection.lineage_rows().to_vec();
        let projection_version = dataset.version().version;
        let mut coordinator = Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            branch.map(str::to_string),
            branch_identifier,
        );
        coordinator.projection = Some((projection_version, projection));
        Ok((coordinator, lineage_rows))
    }

    pub async fn snapshot_at(
        root_uri: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<Snapshot> {
        let root = root_uri.trim_end_matches('/');
        Ok(Self::snapshot_from_state(
            root,
            snapshot_state_at(root, branch, version).await?,
        ))
    }

    /// Test whether one live graph branch still inherits a table fork from a
    /// branch being deleted, without capturing the candidate branch's native
    /// incarnation.
    ///
    /// This deliberately narrow predicate is valid only while the caller holds
    /// branch-delete's complete control envelope: the schema-control gate, the
    /// delete-target branch gate, and every accepted-catalog table gate for the
    /// target. The schema gate serializes native branch create/delete in the
    /// supported single-writer process, and an ordinary writer can only replace
    /// an inherited `table_branch` with its own branch; it cannot make a live
    /// branch newly inherit the held delete target. A raced write can therefore
    /// make this snapshot conservatively report an old dependency, never hide a
    /// new one.
    ///
    /// General coordinator, OCC, and live-read/feed opens must not use this
    /// path: they need the BranchIdentifier captured with their manifest
    /// projection to fence delete/recreate ABA.
    pub(super) async fn branch_depends_on_delete_target_under_control_gates(
        root_uri: &str,
        candidate_branch: Option<&str>,
        delete_target: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<bool> {
        let root = root_uri.trim_end_matches('/');
        let dataset =
            open_manifest_dataset_with_session(root, candidate_branch, control_session).await?;
        let snapshot = Self::snapshot_from_state(root, read_manifest_state(&dataset).await?);
        Ok(snapshot
            .datasets()
            .any(|entry| entry.native_dataset_branch.as_deref() == Some(delete_target)))
    }

    /// Return a Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        Self::snapshot_from_state(&self.root_uri, self.known_state.clone())
    }

    pub(crate) fn control_session(&self) -> Arc<lance::session::Session> {
        self.dataset.session()
    }

    pub(crate) fn captured_probe(&self) -> CapturedManifestProbe {
        CapturedManifestProbe {
            dataset: self.dataset.clone(),
            active_branch: self.active_branch.clone(),
            captured: self.incarnation(),
        }
    }

    pub(crate) async fn refresh_with_lineage(&mut self) -> Result<Vec<GraphLineageRow>> {
        // Boxed wholesale: this body carries the incremental fold (fragment
        // maps, fold clone, debug verify) plus the full-scan fallback, and it
        // is awaited deep inside the merge future — the engine's known
        // stack-depth hazard. The box keeps that layout out of every caller's
        // generator frame.
        Box::pin(self.refresh_with_lineage_inner()).await
    }

    async fn refresh_with_lineage_inner(&mut self) -> Result<Vec<GraphLineageRow>> {
        // Incremental first (the incremental-projection design): fold only the catalog fragments
        // appended since the held pin. Every unprovable precondition falls
        // back to the full scan below — provably current or full read.
        if let Some(lineage_rows) = self.refresh_incremental().await? {
            return Ok(lineage_rows);
        }
        crate::instrumentation::record_projection_full_refresh();
        let control_session = self.dataset.session();
        let (dataset, branch_identifier) = open_manifest_dataset_with_identifier_with_session(
            &self.root_uri,
            self.active_branch.as_deref(),
            &control_session,
        )
        .await?;
        let (known_state, projection) = read_manifest_projection(&dataset).await?;
        let lineage_rows = projection.lineage_rows().to_vec();
        let projection_version = dataset.version().version;
        self.dataset = dataset;
        self.known_state = known_state;
        self.branch_identifier = branch_identifier;
        self.projection = Some((projection_version, projection));
        Ok(lineage_rows)
    }

    /// Incremental projection refresh. `Ok(None)` = a precondition
    /// was unprovable — the caller does the full scan. `Ok(Some(rows))` = the
    /// coordinator now describes the latest catalog version, having read only
    /// the appended fragments (plus the deletion-vector differences on shared
    /// ones).
    ///
    /// Soundness rests on the catalog's write shape: publishes append new
    /// fragments and, for the mutable `graph_head:<branch>` rows, mark the
    /// superseded row deleted in an existing fragment's deletion vector. So a
    /// provable delta is: every held fragment still present with identical
    /// data files and overlays (compaction rewrites fail this and fall back),
    /// deletion-vector growth explained entirely by `graph_head` rows (any
    /// other deleted row means machinery this fold does not model — full
    /// read), and new fragments whose LIVE rows are the appended state. In
    /// debug builds every successful fold is verified against a full scan of
    /// the same version and any divergence fails loudly, so the entire test
    /// suite oracles this path.
    async fn refresh_incremental(&mut self) -> Result<Option<Vec<GraphLineageRow>>> {
        let Some((projection_version, projection)) = self.projection.as_ref() else {
            return Ok(None);
        };
        // The staleness fence: accumulators describing any version other than
        // the held pin cannot be folded onto (some path advanced the dataset
        // without them) — full scan.
        if *projection_version != self.dataset.version().version {
            return Ok(None);
        }
        let control_session = self.dataset.session();
        let (new_dataset, new_identifier) = open_manifest_dataset_with_identifier_with_session(
            &self.root_uri,
            self.active_branch.as_deref(),
            &control_session,
        )
        .await?;
        // Delete/recreate ABA fence: a same-named branch with a new lifetime
        // must never fold onto the old lifetime's projection.
        if new_identifier != self.branch_identifier {
            tracing::debug!("projection refresh: branch identifier changed; full scan");
            return Ok(None);
        }
        if new_dataset.version().version == self.dataset.version().version {
            crate::instrumentation::record_projection_incremental_refresh();
            return Ok(Some(projection.lineage_rows().to_vec()));
        }

        let old_fragments: std::collections::HashMap<u64, lance::dataset::fragment::FileFragment> =
            self.dataset
                .get_fragments()
                .into_iter()
                .map(|fragment| (fragment.metadata().id, fragment))
                .collect();
        let mut delta_fragments = Vec::new();
        let mut dead_head_rows: Vec<(
            lance_table::format::Fragment,
            std::collections::HashSet<u32>,
        )> = Vec::new();
        for new_fragment in new_dataset.get_fragments() {
            let new_meta = new_fragment.metadata().clone();
            let Some(old_fragment) = old_fragments.get(&new_meta.id) else {
                delta_fragments.push(new_meta);
                continue;
            };
            let old_meta = old_fragment.metadata();
            if old_meta.files != new_meta.files || old_meta.overlays != new_meta.overlays {
                // Rewritten in place (compaction shape) — not an append.
                tracing::debug!("projection refresh: fragment rewritten; full scan");
                return Ok(None);
            }
            if old_meta.deletion_file == new_meta.deletion_file {
                continue;
            }
            let old_dv = fragment_deletion_offsets(old_fragment).await?;
            let new_dv = fragment_deletion_offsets(&new_fragment).await?;
            if old_dv.difference(&new_dv).next().is_some() {
                // A row came back to life; no forward publish does that.
                tracing::debug!("projection refresh: deletion vector shrank; full scan");
                return Ok(None);
            }
            let newly_dead: std::collections::HashSet<u32> =
                new_dv.difference(&old_dv).copied().collect();
            if !newly_dead.is_empty() {
                dead_head_rows.push((old_meta.clone(), newly_dead));
            }
        }
        // Held fragments absent from the new version (cleanup/compaction
        // shapes) are not an append either.
        let new_ids: std::collections::HashSet<u64> = new_dataset
            .get_fragments()
            .iter()
            .map(|fragment| fragment.metadata().id)
            .collect();
        if old_fragments.keys().any(|id| !new_ids.contains(id)) {
            tracing::debug!("projection refresh: held fragment removed; full scan");
            return Ok(None);
        }

        // Exception safety: fold a clone (CPU-only, O(history) rows in
        // memory), install on success.
        let mut folded = projection.clone();
        for (fragment, offsets) in &dead_head_rows {
            let identities =
                read_object_identities_at_offsets(&self.dataset, fragment.clone(), offsets).await?;
            if identities.len() != offsets.len() {
                tracing::debug!("projection refresh: dead rows unaccounted; full scan");
                return Ok(None);
            }
            for (object_type, object_id) in identities {
                if object_type != OBJECT_TYPE_GRAPH_HEAD {
                    // A deleted row this fold does not model (retention,
                    // repair, future machinery): the append assumption is
                    // gone.
                    tracing::debug!(
                        object_type,
                        "projection refresh: non-head row deleted; full scan"
                    );
                    return Ok(None);
                }
                let Some(branch_key) = object_id.strip_prefix(GRAPH_HEAD_OBJECT_ID_PREFIX) else {
                    // A head-typed row without the head prefix is a
                    // malformed-row signal, not a routine fallback shape —
                    // say so before degrading to the full scan (which
                    // re-checks it loudly).
                    tracing::warn!(
                        object_id,
                        "projection refresh: malformed graph_head object id; full scan"
                    );
                    return Ok(None);
                };
                folded.remove_head(branch_key);
            }
        }
        let known_state =
            match fold_projection_delta(&new_dataset, delta_fragments, &mut folded).await {
                Ok(state) => state,
                Err(error) => {
                    // A fold inconsistency means a precondition this gate missed,
                    // not a caller error — degrade to the full scan.
                    tracing::debug!(error = %error, "projection refresh: fold failed; full scan");
                    return Ok(None);
                }
            };

        #[cfg(debug_assertions)]
        {
            let (full_state, full_projection) = read_manifest_projection(&new_dataset).await?;
            let mut folded_lineage = folded.lineage_rows().to_vec();
            let mut full_lineage = full_projection.lineage_rows().to_vec();
            folded_lineage.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
            full_lineage.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
            // Entries are a sorted Vec (Debug-comparable); graph_heads is a
            // HashMap, whose Debug order is nondeterministic — compare it as
            // an ordered map.
            let full_heads: std::collections::BTreeMap<_, _> =
                full_state.graph_heads.iter().collect();
            let folded_heads: std::collections::BTreeMap<_, _> =
                known_state.graph_heads.iter().collect();
            if format!("{:?}", full_state.entries) != format!("{:?}", known_state.entries)
                || full_state.version != known_state.version
                || full_heads != folded_heads
                || folded_lineage != full_lineage
            {
                return Err(OmniError::manifest_internal(format!(
                    "incremental projection diverged from the full scan at manifest version {} \
                     (branch {:?}) — this is a bug in the incremental-projection fold.\nfull state:   {:?}\n\
                     folded state: {:?}\nfull lineage ({}): {:?}\nfolded lineage ({}): {:?}",
                    new_dataset.version().version,
                    self.active_branch,
                    full_state,
                    known_state,
                    full_lineage.len(),
                    full_lineage,
                    folded_lineage.len(),
                    folded_lineage,
                )));
            }
        }

        crate::instrumentation::record_projection_incremental_refresh();
        let lineage_rows = folded.lineage_rows().to_vec();
        let projection_version = new_dataset.version().version;
        self.dataset = new_dataset;
        self.known_state = known_state;
        self.projection = Some((projection_version, folded));
        Ok(Some(lineage_rows))
    }

    /// Refresh one live-read view without ever installing a manifest state
    /// whose inherited lineage projection has not also been refreshed.
    ///
    /// `projection_has_head` reports whether the caller's lineage projection
    /// already contains a commit id. The cheap state-only path is taken ONLY
    /// when the refreshed branch head row exists AND the projection already
    /// knows that exact head (the manifest moved without extending this
    /// branch's chain — e.g. a maintenance pointer publish). A head the
    /// projection lacks means another handle or process committed, so the
    /// lineage is re-read atomically with the state — otherwise the caller
    /// would pair the new head with a stale commit map and every later feed
    /// poll or head resolution on this handle would fail with a
    /// missing-commit error and never self-heal. An absent head row (fresh
    /// named branch) refreshes the inherited lineage, as before; every
    /// fallible read completes before either field is replaced so a transient
    /// lineage failure leaves the previous coordinator coherent.
    pub(crate) async fn refresh_for_live_read(
        &mut self,
        projection_has_head: impl FnOnce(&str) -> bool,
    ) -> Result<Option<Vec<GraphLineageRow>>> {
        let control_session = self.dataset.session();
        let (dataset, branch_identifier) = open_manifest_dataset_with_identifier_with_session(
            &self.root_uri,
            self.active_branch.as_deref(),
            &control_session,
        )
        .await?;
        let known_state = read_manifest_state(&dataset).await?;
        let branch_key = self
            .active_branch
            .as_deref()
            .unwrap_or(MAIN_BRANCH_HEAD_KEY);
        let lineage_rows = match known_state.graph_heads.get(branch_key) {
            Some(head) if projection_has_head(head) => None,
            _ => {
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::READ_REFRESH_POST_STATE_PRE_LINEAGE,
                )?;
                Some(read_graph_lineage(&dataset).await?.0)
            }
        };

        self.dataset = dataset;
        self.known_state = known_state;
        self.branch_identifier = branch_identifier;
        // Same staleness rule as the post-publish fold: this refresh advances
        // `dataset` without folding the projection accumulators, so they must
        // not survive it.
        self.projection = None;
        Ok(lineage_rows)
    }

    /// Commit updated sub-table versions to the manifest.
    ///
    /// Atomically inserts one immutable `table_version` row per updated table.
    /// The merge-insert commit on `__manifest` is the graph-level publish point.
    #[cfg(test)]
    pub(crate) async fn commit(&mut self, updates: &[DatasetUpdate]) -> Result<u64> {
        let changes = updates
            .iter()
            .cloned()
            .map(ManifestChange::Update)
            .collect::<Vec<_>>();
        self.commit_changes(&changes).await
    }

    /// Same as [`commit`], but with caller-supplied per-table expected
    /// versions used for optimistic concurrency control. Each entry asserts
    /// the manifest's current latest non-tombstoned `table_version` for that
    /// table identity is exactly what the caller observed; mismatches surface
    /// as `OmniError::Manifest` with `ManifestConflictDetails::PublishedDatasetVersionMismatch`.
    #[cfg(test)]
    pub(crate) async fn commit_with_expected(
        &mut self,
        updates: &[DatasetUpdate],
        expected_table_versions: &ExpectedTableVersions,
    ) -> Result<u64> {
        let changes = updates
            .iter()
            .cloned()
            .map(ManifestChange::Update)
            .collect::<Vec<_>>();
        self.commit_changes_with_expected(&changes, expected_table_versions)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn commit_changes(&mut self, changes: &[ManifestChange]) -> Result<u64> {
        self.commit_changes_with_expected(changes, &HashMap::new())
            .await
    }

    #[cfg(test)]
    pub(crate) async fn commit_changes_with_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
    ) -> Result<u64> {
        Ok(self
            .commit_changes_with_lineage(changes, expected_table_versions, None)
            .await?
            .version)
    }

    /// Publish `changes` and, when `lineage` is present, record the graph commit
    /// in the SAME merge-insert (RFC-013 Phase 7). `__manifest` is the single
    /// source of graph lineage: the `graph_commit` + `graph_head:<branch>` rows
    /// ride the table-version publish so the whole commit lands at one manifest
    /// version — no separate write, no manifest→commit-graph atomicity gap, no
    /// per-write commit-graph refresh. Returns the new version and the parent the
    /// publisher resolved for the commit (so the caller can update its in-memory
    /// commit cache without a re-read).
    #[cfg(test)]
    pub(crate) async fn commit_changes_with_lineage(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
    ) -> Result<CommitOutcome> {
        self.commit_changes_with_lineage_and_precondition(
            changes,
            expected_table_versions,
            lineage,
            &PublishPrecondition::Any,
        )
        .await
    }

    /// Token-aware graph publication. Exact authority is checked by the
    /// publisher from every CAS attempt's existing one-scan state.
    pub(crate) async fn commit_changes_with_lineage_and_precondition(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<CommitOutcome> {
        if changes.is_empty()
            && expected_table_versions.is_empty()
            && lineage.is_none()
            && matches!(precondition, PublishPrecondition::Any)
        {
            return Ok(CommitOutcome {
                version: self.version(),
                parent_commit_id: None,
            });
        }

        let PublishOutcome {
            dataset,
            parent_commit_id,
            known_state,
        } = self
            .publisher
            .publish_with_precondition(changes, expected_table_versions, lineage, precondition)
            .await?;
        // RFC-013 PR2 #1b: the publisher folded the new visible state in-memory
        // (byte-identical to a re-scan via the shared `assemble_manifest_state`),
        // so adopt it directly instead of an O(fragments) `read_manifest_state`.
        self.dataset = dataset;
        self.known_state = known_state;
        // The projection accumulators do not ride this fold — cleared so a
        // later incremental refresh cannot serve the pre-publish lineage as
        // "unchanged" (the staleness fence on the `projection` field).
        self.projection = None;
        Ok(CommitOutcome {
            version: self.version(),
            parent_commit_id,
        })
    }

    /// Project the graph-lineage rows out of `__manifest` at `branch` without an
    /// open coordinator. Opens the manifest fresh; used by `CommitGraph` to
    /// source its in-memory cache from the manifest projection.
    pub(crate) async fn read_graph_lineage_at(
        root_uri: &str,
        branch: Option<&str>,
    ) -> Result<(Vec<GraphLineageRow>, HashMap<String, String>)> {
        let control_session = crate::lance_access::control_session();
        let dataset =
            open_manifest_dataset_with_session(root_uri, branch, &control_session).await?;
        read_graph_lineage(&dataset).await
    }

    /// Current graph-manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    #[cfg(test)]
    pub(crate) async fn probe_latest_version(&self) -> Result<u64> {
        self.dataset
            .latest_version_id()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))
    }

    /// Lance-native stable identity captured with the active manifest state.
    /// Unlike a manifest version/eTag, this remains stable across ordinary
    /// commits and changes when a named branch is deleted and recreated (ABA
    /// protection). Returning the capture, rather than re-reading the live ref,
    /// prevents callers from pairing old state with a replacement witness.
    pub(crate) async fn branch_identifier(&self) -> Result<lance::dataset::refs::BranchIdentifier> {
        Ok(self.branch_identifier.clone())
    }

    /// Exact materialized `graph_head:<active-branch>` from the same pinned
    /// manifest version as [`Self::snapshot`]. This is write authority, not a
    /// lineage-cache query: a read may refresh only the manifest, so consulting
    /// `CommitGraph` here would combine a fresh table snapshot with a stale head.
    pub(crate) fn exact_graph_head(&self) -> Option<String> {
        let branch_key = self
            .active_branch
            .as_deref()
            .unwrap_or(MAIN_BRANCH_HEAD_KEY);
        self.known_state.graph_heads.get(branch_key).cloned()
    }

    pub(crate) fn incarnation(&self) -> ManifestIncarnation {
        ManifestIncarnation {
            version: self.version(),
            e_tag: self.dataset.manifest_location().e_tag.clone(),
            timestamp_nanos: Some(self.dataset.manifest().timestamp_nanos),
            branch_identifier: self.branch_identifier.clone(),
        }
    }

    /// Latest committed manifest identity. Main cannot be deleted/recreated, so
    /// the cheap version-number probe is sufficient there. Non-main Lance
    /// branches can be deleted and recreated with the same version, e_tag, and
    /// timestamp when both lifetimes fork the same source; the native branch
    /// identifier is therefore part of the freshness result as well.
    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        probe_dataset_latest_incarnation(&self.dataset, self.active_branch.as_deref()).await
    }

    pub(crate) async fn create_branch(&mut self, name: &str) -> Result<()> {
        let mut ds = self.dataset.clone();
        match crate::branch_control::create_branch_recoverably(&mut ds, name, self.version())
            .await?
        {
            crate::branch_control::BranchCreateOutcome::Created(_) => Ok(()),
            crate::branch_control::BranchCreateOutcome::RefAlreadyExists => Err(
                OmniError::manifest_conflict(format!("branch '{}' already exists", name)),
            ),
        }
    }

    async fn open_branch_control_dataset(&self) -> Result<Dataset> {
        let uri = manifest_uri(&self.root_uri);
        crate::instrumentation::open_dataset(
            &uri,
            crate::instrumentation::VersionResolution::Latest,
            None,
            crate::instrumentation::manifest_wrapper(),
        )
        .await
    }

    pub(crate) async fn delete_branch(&mut self, name: &str) -> Result<()> {
        let mut ds = self.open_branch_control_dataset().await?;
        let branches = list_branch_contents(&ds).await?;
        let expected_identifier = branches
            .get(name)
            .ok_or_else(|| OmniError::manifest_not_found(format!("branch '{}' not found", name)))?
            .identifier
            .clone();
        crate::branch_control::delete_branch_recoverably(&mut ds, name, &expected_identifier)
            .await?;
        Ok(())
    }

    /// Delete `name` only if its live Lance BranchContents still has the exact
    /// identifier captured by the caller's post-gate branch view.
    ///
    /// The coordinator may itself be bound to `name`: operation-local native
    /// controls discard that captured coordinator after the authority change,
    /// so refreshing a just-deleted bound ref would be both wasted work and an
    /// error. Deleting a sibling ref does not mutate this coordinator's pinned
    /// manifest version/state either.
    pub(crate) async fn delete_branch_with_expected(
        &mut self,
        name: &str,
        expected_identifier: &lance::dataset::refs::BranchIdentifier,
    ) -> Result<()> {
        let mut ds = self.open_branch_control_dataset().await?;
        crate::branch_control::delete_branch_recoverably(&mut ds, name, expected_identifier).await
    }

    pub async fn list_graph_branches(&self) -> Result<Vec<String>> {
        let branches = list_branch_contents(&self.dataset).await?;
        let mut names: Vec<String> = branches.into_keys().filter(|name| name != "main").collect();
        names.sort();
        let mut all = vec!["main".to_string()];
        all.extend(names);
        Ok(all)
    }

    pub async fn descendant_branches(&self, name: &str) -> Result<Vec<String>> {
        let branches = list_branch_contents(&self.dataset).await?;
        let mut frontier = vec![name.to_string()];
        let mut descendants = Vec::new();
        let mut seen = HashSet::new();

        while let Some(parent) = frontier.pop() {
            let mut children = branches
                .iter()
                .filter_map(|(branch, contents)| {
                    (contents.parent_branch.as_deref() == Some(parent.as_str()))
                        .then_some(branch.clone())
                })
                .collect::<Vec<_>>();
            children.sort();
            for child in children {
                if seen.insert(child.clone()) {
                    frontier.push(child.clone());
                    descendants.push(child);
                }
            }
        }

        Ok(descendants)
    }
}

#[cfg(test)]
#[path = "manifest/tests.rs"]
mod tests;
