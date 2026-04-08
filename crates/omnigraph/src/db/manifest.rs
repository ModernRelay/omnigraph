use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{OmniError, Result};
use lance::Dataset;
use lance_namespace::models::CreateTableVersionRequest;
use omnigraph_compiler::catalog::Catalog;

#[path = "manifest/layout.rs"]
mod layout;
#[path = "manifest/metadata.rs"]
mod metadata;
#[path = "manifest/namespace.rs"]
mod namespace;
#[path = "manifest/publisher.rs"]
mod publisher;
#[path = "manifest/repo.rs"]
mod repo;
#[path = "manifest/state.rs"]
mod state;

use layout::{manifest_uri, open_manifest_dataset};
pub(crate) use metadata::TableVersionMetadata;
#[cfg(test)]
use metadata::{OMNIGRAPH_ROW_COUNT_KEY, table_version_metadata_for_state};
use namespace::open_table_at_version_from_manifest;
pub(crate) use namespace::open_table_head_for_write;
#[cfg(test)]
use namespace::{branch_manifest_namespace, staged_table_namespace};
use publisher::{GraphNamespacePublisher, ManifestBatchPublisher};
use repo::{init_manifest_repo, open_manifest_repo, snapshot_state_at};
pub use state::SubTableEntry;
#[cfg(test)]
use state::string_column;
use state::{ManifestState, read_manifest_state};

const OBJECT_TYPE_TABLE: &str = "table";
const OBJECT_TYPE_TABLE_VERSION: &str = "table_version";
const TABLE_VERSION_MANAGEMENT_KEY: &str = "table_version_management";

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    root_uri: String,
    version: u64,
    entries: HashMap<String, SubTableEntry>,
}

impl Snapshot {
    /// Open a sub-table dataset at its pinned version.
    pub async fn open(&self, table_key: &str) -> Result<Dataset> {
        let entry = self
            .entries
            .get(table_key)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        entry.open(&self.root_uri).await
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

impl SubTableEntry {
    pub(crate) async fn open(&self, root_uri: &str) -> Result<Dataset> {
        open_table_at_version_from_manifest(
            root_uri,
            &self.table_key,
            self.table_branch.as_deref(),
            self.table_version,
        )
        .await
    }
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
        }
    }

    #[cfg(test)]
    fn with_batch_publisher(mut self, publisher: Arc<dyn ManifestBatchPublisher>) -> Self {
        self.publisher = publisher;
        self
    }

    /// Create a new repo at `root_uri` from a catalog.
    ///
    /// Creates per-type Lance datasets and the namespace `__manifest` table.
    pub async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = init_manifest_repo(root, catalog).await?;

        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            None,
        ))
    }

    /// Open an existing repo's manifest.
    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = open_manifest_repo(root, None).await?;
        Ok(Self::from_parts_with_default_publisher(
            root,
            dataset,
            known_state,
            None,
        ))
    }

    /// Open an existing repo's manifest at a specific branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        if branch == "main" {
            return Self::open(root_uri).await;
        }

        let root = root_uri.trim_end_matches('/');
        let (dataset, known_state) = open_manifest_repo(root, Some(branch)).await?;
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
        if updates.is_empty() {
            return Ok(self.version());
        }

        self.dataset = self.publisher.publish(updates).await?;

        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(self.version())
    }

    /// Current manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
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

    /// Root URI of the repo.
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

#[cfg(test)]
#[path = "manifest/tests.rs"]
mod tests;
