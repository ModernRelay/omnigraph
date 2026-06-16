use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{OmniError, Result};
use lance::Dataset;
use lance_namespace::models::CreateTableVersionRequest;
use omnigraph_compiler::catalog::Catalog;

#[path = "manifest/graph.rs"]
mod graph;
#[path = "manifest/layout.rs"]
mod layout;
#[path = "manifest/metadata.rs"]
mod metadata;
#[path = "manifest/migrations.rs"]
mod migrations;
#[path = "manifest/namespace.rs"]
mod namespace;
#[path = "manifest/publisher.rs"]
mod publisher;
#[path = "manifest/recovery.rs"]
mod recovery;
#[path = "manifest/state.rs"]
mod state;

use graph::{init_manifest_graph, open_manifest_graph, snapshot_state_at};
use layout::{manifest_uri, open_manifest_dataset, table_uri_for_path, type_name_hash};
pub(crate) use metadata::TableVersionMetadata;
#[cfg(test)]
use metadata::{OMNIGRAPH_ROW_COUNT_KEY, table_version_metadata_for_state};
pub(crate) use namespace::open_table_head_for_write;
#[cfg(test)]
use namespace::{branch_manifest_namespace, staged_table_namespace};
use publisher::{GraphNamespacePublisher, ManifestBatchPublisher};
pub(crate) use recovery::{
    RecoveryMode, RecoverySidecarHandle, SidecarKind, SidecarTablePin, SidecarTableRegistration,
    SidecarTombstone, delete_sidecar, has_schema_apply_sidecar, heal_pending_sidecars_roll_forward,
    list_sidecars, new_sidecar, recover_manifest_drift, schema_apply_serial_queue_key,
    write_sidecar,
};
pub use state::SubTableEntry;
#[cfg(test)]
use state::string_column;
use state::{ManifestState, read_manifest_state};

const OBJECT_TYPE_TABLE: &str = "table";
const OBJECT_TYPE_TABLE_VERSION: &str = "table_version";
const OBJECT_TYPE_TABLE_TOMBSTONE: &str = "table_tombstone";
const TABLE_VERSION_MANAGEMENT_KEY: &str = "table_version_management";

/// Apply pending internal-schema migrations against `__manifest` on the
/// open-for-write path, independent of a publish.
///
/// `Omnigraph::open(ReadWrite)` calls this before the coordinator reads branch
/// state, so branch-observing code (`branch_list`, the schema-apply
/// blocking-branch checks) sees the post-migration graph. In particular the
/// v2→v3 step sweeps legacy `__run__*` staging branches off `__manifest`
/// (MR-770); running it here closes the window where those branches would
/// otherwise block schema apply before the first publish runs the migration.
///
/// Idempotent: a no-op stamp read when the on-disk version already matches.
pub(crate) async fn migrate_on_open(root_uri: &str) -> Result<()> {
    let mut dataset = open_manifest_dataset(root_uri, None).await?;
    migrations::migrate_internal_schema(&mut dataset).await
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

impl Snapshot {
    /// Open a sub-table dataset at its pinned version. With read caches present
    /// (live Branch reads), reuse a held handle through the cache (0 open IO on a
    /// warm repeat) and the shared `Session`; otherwise plain-open (Fix 2).
    pub async fn open(&self, table_key: &str) -> Result<Dataset> {
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
                        &location,
                        Some(&caches.session),
                    )
                    .await
            }
            None => entry.open(&self.root_uri).await,
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

#[derive(Debug, Clone)]
pub(crate) struct TableRegistration {
    pub(crate) table_key: String,
    pub(crate) table_path: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TableTombstone {
    pub(crate) table_key: String,
    pub(crate) tombstone_version: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum ManifestChange {
    Update(SubTableUpdate),
    RegisterTable(TableRegistration),
    Tombstone(TableTombstone),
}

impl SubTableEntry {
    /// Open this sub-table at its pinned version directly by location (Fix 2),
    /// without the Lance namespace — which would full-scan `__manifest` twice per
    /// open (`describe_table` + `describe_table_version`). The resolved Snapshot
    /// already holds the path, version, and branch. Branches are Lance native
    /// branches, so `with_branch` resolves `{base}/tree/{branch}` from the base
    /// URI; main uses `with_version`.
    pub(crate) async fn open(&self, root_uri: &str) -> Result<Dataset> {
        // The branch-qualified location is the dataset that physically holds this
        // version: main at `{table_path}`, a branch at
        // `{table_path}/tree/{branch}` (Lance native-branch storage). `with_version`
        // then resolves the version within THAT dataset's `_versions` — a branch
        // version lives under `tree/{branch}/_versions`, not the base. This
        // matches the physical layout the namespace path resolved, without the
        // per-open `__manifest` scan.
        let location = table_uri_for_path(root_uri, &self.table_path, self.table_branch.as_deref());
        // Route through the instrumented data-table opener (Fix 3). With no
        // session this is exactly the Fix-2 `from_uri(location).with_version`.
        // This is the uncached fallback (a snapshot with no read caches); the
        // cached path (`Snapshot::open` → handle cache) calls the same opener on
        // a miss with the shared session, so both paths count on the per-query
        // `table_wrapper`.
        crate::instrumentation::open_table_dataset(&location, self.table_version, None).await
    }
}

pub(crate) fn table_path_for_table_key(table_key: &str) -> Result<String> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        return Ok(format!("nodes/{}", type_name_hash(type_name)));
    }
    if let Some(type_name) = table_key.strip_prefix("edge:") {
        return Ok(format!("edges/{}", type_name_hash(type_name)));
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

/// An update to apply to the manifest via `commit`.
#[derive(Debug, Clone)]
pub struct SubTableUpdate {
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
pub struct ManifestCoordinator {
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
    pub async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
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
    pub async fn commit(&mut self, updates: &[SubTableUpdate]) -> Result<u64> {
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
    /// `table_key` is exactly what the caller observed; mismatches surface
    /// as `OmniError::Manifest` with `ManifestConflictDetails::ExpectedVersionMismatch`.
    pub async fn commit_with_expected(
        &mut self,
        updates: &[SubTableUpdate],
        expected_table_versions: &HashMap<String, u64>,
    ) -> Result<u64> {
        let changes = updates
            .iter()
            .cloned()
            .map(ManifestChange::Update)
            .collect::<Vec<_>>();
        self.commit_changes_with_expected(&changes, expected_table_versions)
            .await
    }

    pub(crate) async fn commit_changes(&mut self, changes: &[ManifestChange]) -> Result<u64> {
        self.commit_changes_with_expected(changes, &HashMap::new())
            .await
    }

    pub(crate) async fn commit_changes_with_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &HashMap<String, u64>,
    ) -> Result<u64> {
        if changes.is_empty() && expected_table_versions.is_empty() {
            return Ok(self.version());
        }

        self.dataset = self
            .publisher
            .publish(changes, expected_table_versions)
            .await?;

        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(self.version())
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

    pub fn active_branch(&self) -> Option<&str> {
        self.active_branch.as_deref()
    }

    pub async fn create_branch(&mut self, name: &str) -> Result<()> {
        let mut ds = self.dataset.clone();
        ds.create_branch(name, self.version(), None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(())
    }

    pub async fn delete_branch(&mut self, name: &str) -> Result<()> {
        let uri = manifest_uri(&self.root_uri);
        let mut ds = Dataset::open(&uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        ds.delete_branch(name)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
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

    /// Root URI of the graph.
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

#[cfg(test)]
#[path = "manifest/tests.rs"]
mod tests;
