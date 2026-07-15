use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{OmniError, Result};
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

use graph::{init_manifest_graph, open_manifest_graph, snapshot_state_at};
pub(crate) use layout::manifest_uri;
use layout::{open_manifest_dataset, table_uri_for_path};
pub(crate) use metadata::TableVersionMetadata;
#[cfg(test)]
use metadata::{OMNIGRAPH_ROW_COUNT_KEY, table_version_metadata_for_state};
#[cfg(test)]
use namespace::{branch_manifest_namespace, staged_table_namespace};
pub(crate) use publisher::{GraphHeadExpectation, LineageIntent, PublishPrecondition};
use publisher::{GraphNamespacePublisher, ManifestBatchPublisher, PublishOutcome};
pub(crate) use recovery::{
    HealPendingOutcome, RecoveryAuthorityToken, RecoveryBranchMergeEffect,
    RecoveryBranchMergeEffectKind, RecoveryLineageIntent, RecoveryManifestDelta, RecoveryMode,
    RecoverySchemaApplyEffect, RecoverySchemaApplyEffectKind, RecoverySidecar,
    RecoverySidecarHandle, RecoveryTableUpdateSlot, SidecarKind, SidecarTablePin,
    SidecarTableRegistration, SidecarTableRename, SidecarTombstone,
    confirm_branch_merge_sidecar_v9, confirm_ensure_indices_sidecar_v9, confirm_occ_sidecar_v9,
    confirm_schema_apply_sidecar_v9, delete_sidecar, ensure_read_only_schema_coherent,
    heal_pending_sidecars_roll_forward, list_sidecars, new_branch_merge_sidecar_v9,
    new_ensure_indices_sidecar_v9, new_occ_sidecar_v9, new_optimize_sidecar_v9,
    new_schema_apply_sidecar_v9, recover_manifest_drift, schema_apply_serial_queue_key,
    write_sidecar,
};
pub use state::SubTableEntry;
#[cfg(test)]
use state::string_column;
pub(crate) use state::{GraphLineageRow, read_graph_lineage};
use state::{ManifestState, read_manifest_state};

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
const TABLE_VERSION_MANAGEMENT_KEY: &str = "table_version_management";

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
/// `None`). Used by the open-path refusal guard and to surface the storage
/// version to operators (`omnigraph snapshot`).
pub(crate) async fn internal_schema_stamp_at(root_uri: &str, branch: Option<&str>) -> Result<u32> {
    let dataset = open_manifest_dataset(root_uri, branch).await?;
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
    let stamp = internal_schema_stamp_at(root_uri, None).await?;
    migrations::refuse_if_stamp_unsupported(stamp)
}

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    root_uri: String,
    version: u64,
    entries: HashMap<String, SubTableEntry>,
    /// Per-graph read caches (shared `Session` + held-handle cache), injected by
    /// `Omnigraph::resolved_target` for live Branch reads so table opens reuse
    /// handles (0 IO on a warm repeat) and one `Session`. `None` for write-prelude
    /// snapshots, time-travel / Snapshot-id reads, and directly-built test
    /// snapshots, which fall back to a plain open.
    read_caches: Option<Arc<crate::runtime_cache::ReadCaches>>,
}

/// Read-only view of one table pinned by a [`Snapshot`].
///
/// The underlying Lance [`Dataset`] is deliberately private: a snapshot table
/// can scan rows and inspect read metadata, but it cannot reach Lance's
/// mutating APIs or advance a table HEAD outside OmniGraph's coordinated write
/// path.
#[derive(Debug, Clone)]
pub struct SnapshotTable {
    dataset: Dataset,
}

/// Read-only scan builder for a [`SnapshotTable`].
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

impl SnapshotTable {
    fn new(dataset: Dataset) -> Self {
        Self { dataset }
    }

    /// Build a read-only scanner over this pinned table version.
    pub fn scan(&self) -> SnapshotScanner {
        SnapshotScanner {
            scanner: self.dataset.scan(),
        }
    }

    /// Count rows in this pinned table version, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        self.dataset
            .count_rows(filter)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))
    }

    /// Lance schema of this pinned table version.
    pub fn schema(&self) -> &LanceSchema {
        self.dataset.schema()
    }

    /// Lance manifest version of this pinned table.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Read-only physical index metadata for this pinned table version.
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

    /// Whether this table has a user BTREE index on `column`.
    pub async fn has_btree_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_btree_index_on(&self.dataset, column).await
    }

    /// Whether this table has a user full-text index on `column`.
    pub async fn has_fts_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_fts_index_on(&self.dataset, column).await
    }

    /// Whether this table has a user vector index on `column`.
    pub async fn has_vector_index(&self, column: &str) -> Result<bool> {
        crate::table_store::TableStore::has_vector_index_on(&self.dataset, column).await
    }
}

impl Snapshot {
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

    /// Open a sub-table dataset at its pinned version. With read caches present
    /// (live Branch reads), reuse a held handle through the cache (0 open IO on a
    /// warm repeat) and the shared `Session`; otherwise plain-open (Fix 2).
    pub async fn open(&self, table_key: &str) -> Result<SnapshotTable> {
        self.open_dataset(table_key).await.map(SnapshotTable::new)
    }

    /// Open the raw Lance dataset for engine-internal read execution.
    ///
    /// This stays crate-private so downstream SDK callers cannot obtain a
    /// writable `Dataset` from a logical graph snapshot.
    pub(crate) async fn open_dataset(&self, table_key: &str) -> Result<Dataset> {
        let entry = self
            .entries
            .get(table_key)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        match &self.read_caches {
            Some(caches) => {
                let location = table_uri_for_path(
                    &self.root_uri,
                    &entry.table_path,
                    entry.table_branch.as_deref(),
                );
                caches
                    .handles
                    .get_or_open(
                        &entry.table_path,
                        entry.table_branch.as_deref(),
                        entry.table_version,
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
    /// snapshot's table opens reuse handles and the session. Set by
    /// `Omnigraph::resolved_target` for live Branch reads only.
    pub(crate) fn set_read_caches(&mut self, caches: Arc<crate::runtime_cache::ReadCaches>) {
        self.read_caches = Some(caches);
    }

    /// Manifest version this snapshot was taken from.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Look up a sub-table entry by key.
    pub fn entry(&self, table_key: &str) -> Option<&SubTableEntry> {
        self.entries.get(table_key)
    }

    pub fn entries(&self) -> impl Iterator<Item = &SubTableEntry> {
        self.entries.values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestIncarnation {
    pub(crate) version: u64,
    pub(crate) e_tag: Option<String>,
    timestamp_nanos: Option<u128>,
}

impl ManifestIncarnation {
    pub(crate) fn matches(&self, held: &Self) -> bool {
        if self.version != held.version {
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

impl SubTableUpdate {
    pub(crate) fn to_create_table_version_request(&self) -> CreateTableVersionRequest {
        self.version_metadata.to_create_table_version_request(
            &self.table_key,
            self.table_version,
            self.row_count,
            self.table_branch.as_deref(),
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
    Update(SubTableUpdate),
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

impl SubTableEntry {
    /// Open this sub-table at its pinned version directly by location (Fix 2),
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
        let location = table_uri_for_path(root_uri, &self.table_path, self.table_branch.as_deref());
        // Route through the one opener (Fix 3). With no session this is exactly
        // the Fix-2 `from_uri(location).with_version`. This is the uncached
        // fallback (a snapshot detached from its graph's read caches); the
        // cached path (`Snapshot::open` → handle cache) calls the same opener on
        // a miss with the shared session, so both paths count on the per-query
        // `table_wrapper`.
        crate::instrumentation::open_dataset(
            &location,
            crate::instrumentation::VersionResolution::At(self.table_version),
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
pub struct SubTableUpdate {
    pub(crate) identity: TableIdentity,
    pub table_key: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
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
    publisher: Arc<dyn ManifestBatchPublisher>,
}

impl ManifestCoordinator {
    fn default_batch_publisher(
        root_uri: &str,
        active_branch: Option<&str>,
    ) -> Arc<dyn ManifestBatchPublisher> {
        Arc::new(GraphNamespacePublisher::new(root_uri, active_branch))
    }

    fn from_parts(
        root_uri: &str,
        dataset: Dataset,
        known_state: ManifestState,
        active_branch: Option<String>,
        publisher: Arc<dyn ManifestBatchPublisher>,
    ) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            dataset,
            known_state,
            active_branch,
            publisher,
        }
    }

    fn from_parts_with_default_publisher(
        root_uri: &str,
        dataset: Dataset,
        known_state: ManifestState,
        active_branch: Option<String>,
    ) -> Self {
        let publisher = Self::default_batch_publisher(root_uri, active_branch.as_deref());
        Self::from_parts(root_uri, dataset, known_state, active_branch, publisher)
    }

    fn snapshot_from_state(root_uri: &str, state: ManifestState) -> Snapshot {
        Snapshot {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            version: state.version,
            entries: state
                .entries
                .into_iter()
                .map(|entry| (entry.table_key.clone(), entry))
                .collect(),
            read_caches: None,
        }
    }

    #[cfg(test)]
    fn with_batch_publisher(mut self, publisher: Arc<dyn ManifestBatchPublisher>) -> Self {
        self.publisher = publisher;
        self
    }

    /// Create a new graph at `root_uri` from a catalog.
    ///
    /// Creates per-type Lance datasets and the namespace `__manifest` table.
    /// The genesis graph commit is folded into the init write, so `__manifest`
    /// is the single source of graph lineage from version one — callers read it
    /// back through the lineage projection rather than via a second write.
    pub(crate) async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = init_manifest_graph(root, catalog).await?;

        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            None,
        ))
    }

    /// Open an existing graph's manifest.
    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = open_manifest_graph(root, None).await?;
        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            None,
        ))
    }

    /// Open an existing graph's manifest at a specific branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        if branch == "main" {
            return Self::open(root_uri).await;
        }

        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = open_manifest_graph(root, Some(branch)).await?;
        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            Some(branch.to_string()),
        ))
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

    /// Return a Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        Self::snapshot_from_state(&self.root_uri, self.known_state.clone())
    }

    /// Re-read manifest from storage to see other writers' commits.
    pub async fn refresh(&mut self) -> Result<()> {
        self.dataset = open_manifest_dataset(&self.root_uri, self.active_branch.as_deref()).await?;
        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(())
    }

    /// Commit updated sub-table versions to the manifest.
    ///
    /// Atomically inserts one immutable `table_version` row per updated table.
    /// The merge-insert commit on `__manifest` is the graph-level publish point.
    #[cfg(test)]
    pub(crate) async fn commit(&mut self, updates: &[SubTableUpdate]) -> Result<u64> {
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
    /// as `OmniError::Manifest` with `ManifestConflictDetails::ExpectedVersionMismatch`.
    #[cfg(test)]
    pub(crate) async fn commit_with_expected(
        &mut self,
        updates: &[SubTableUpdate],
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
        let dataset = open_manifest_dataset(root_uri, branch).await?;
        read_graph_lineage(&dataset).await
    }

    /// Current manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Latest committed manifest version on disk (one object-store op, no row
    /// scan). The freshness probe for warm reuse: compare against `version()`
    /// (the held handle's pinned version) to decide whether to refresh.
    pub async fn probe_latest_version(&self) -> Result<u64> {
        self.dataset
            .latest_version_id()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    /// Lance-native stable identity for the active manifest branch. Unlike a
    /// manifest version/eTag, this remains stable across ordinary commits and
    /// changes when a named branch is deleted and recreated (ABA protection).
    pub(crate) async fn branch_identifier(&self) -> Result<lance::dataset::refs::BranchIdentifier> {
        self.dataset
            .branch_identifier()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
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
        }
    }

    /// Latest committed manifest identity. Main cannot be deleted/recreated, so
    /// the cheap version-number probe is sufficient there. Non-main Lance
    /// branches can be deleted and recreated with the same version number, so
    /// load the latest manifest location and compare its e_tag / timestamp too.
    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        if self.active_branch.is_none() {
            return Ok(ManifestIncarnation {
                version: self.probe_latest_version().await?,
                e_tag: self.dataset.manifest_location().e_tag.clone(),
                timestamp_nanos: Some(self.dataset.manifest().timestamp_nanos),
            });
        }
        let (manifest, location) = self
            .dataset
            .latest_manifest()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(ManifestIncarnation {
            version: manifest.version,
            e_tag: location.e_tag,
            timestamp_nanos: Some(manifest.timestamp_nanos),
        })
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

    pub(crate) async fn delete_branch(&mut self, name: &str) -> Result<()> {
        let uri = manifest_uri(&self.root_uri);
        let mut ds = crate::instrumentation::open_dataset(
            &uri,
            crate::instrumentation::VersionResolution::Latest,
            None,
            crate::instrumentation::manifest_wrapper(),
        )
        .await?;
        let branches = ds
            .list_branches()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let expected_identifier = branches
            .get(name)
            .ok_or_else(|| OmniError::manifest_not_found(format!("branch '{}' not found", name)))?
            .identifier
            .clone();
        crate::branch_control::delete_branch_recoverably(&mut ds, name, &expected_identifier)
            .await?;
        self.dataset = open_manifest_dataset(&self.root_uri, self.active_branch.as_deref()).await?;
        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(())
    }

    pub async fn list_branches(&self) -> Result<Vec<String>> {
        let branches = self
            .dataset
            .list_branches()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let mut names: Vec<String> = branches.into_keys().filter(|name| name != "main").collect();
        names.sort();
        let mut all = vec!["main".to_string()];
        all.extend(names);
        Ok(all)
    }

    pub async fn descendant_branches(&self, name: &str) -> Result<Vec<String>> {
        let branches = self
            .dataset
            .list_branches()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
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
