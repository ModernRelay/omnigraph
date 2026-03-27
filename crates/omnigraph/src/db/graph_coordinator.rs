use std::fmt;
use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};
use crate::failpoints;
use crate::storage::{StorageAdapter, join_uri, normalize_root_uri};

use super::commit_graph::{CommitGraph, GraphCommit};
use super::manifest::{ManifestCoordinator, Snapshot, SubTableUpdate};
use super::run_registry::{RunId, RunRecord, RunRegistry, graph_runs_uri, is_internal_run_branch};

const GRAPH_COMMITS_DIR: &str = "_graph_commits.lance";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotId(String);

impl SnapshotId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn synthetic(branch: Option<&str>, version: u64) -> Self {
        match branch {
            Some(branch) => Self(format!("manifest:{}:v{}", branch, version)),
            None => Self(format!("manifest:main:v{}", version)),
        }
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadTarget {
    Branch(String),
    Snapshot(SnapshotId),
}

impl ReadTarget {
    pub fn branch(name: impl Into<String>) -> Self {
        Self::Branch(name.into())
    }

    pub fn snapshot(id: impl Into<SnapshotId>) -> Self {
        Self::Snapshot(id.into())
    }
}

impl From<&str> for ReadTarget {
    fn from(value: &str) -> Self {
        Self::branch(value)
    }
}

impl From<String> for ReadTarget {
    fn from(value: String) -> Self {
        Self::Branch(value)
    }
}

impl From<SnapshotId> for ReadTarget {
    fn from(value: SnapshotId) -> Self {
        Self::Snapshot(value)
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub requested: ReadTarget,
    pub branch: Option<String>,
    pub snapshot_id: SnapshotId,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedSnapshot {
    pub manifest_version: u64,
    pub _snapshot_id: SnapshotId,
}

pub struct GraphCoordinator {
    root_uri: String,
    storage: Arc<dyn StorageAdapter>,
    manifest: ManifestCoordinator,
    commit_graph: Option<CommitGraph>,
    run_registry: Option<RunRegistry>,
    bound_branch: Option<String>,
}

impl GraphCoordinator {
    pub async fn init(
        root_uri: &str,
        catalog: &Catalog,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(root_uri);
        let manifest = ManifestCoordinator::init(&root, catalog).await?;
        let commit_graph = Some(CommitGraph::init(&root, manifest.version()).await?);
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            run_registry: None,
            bound_branch: None,
        })
    }

    pub async fn open(root_uri: &str, storage: Arc<dyn StorageAdapter>) -> Result<Self> {
        let root = normalize_root_uri(root_uri);
        let manifest = ManifestCoordinator::open(&root).await?;
        let commit_graph = if storage.exists(&graph_commits_uri(&root))? {
            Some(CommitGraph::open(&root).await?)
        } else {
            None
        };
        let run_registry = if storage.exists(&graph_runs_uri(&root))? {
            Some(RunRegistry::open(&root).await?)
        } else {
            None
        };
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            run_registry,
            bound_branch: None,
        })
    }

    pub async fn open_branch(
        root_uri: &str,
        branch: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let branch = normalize_branch_name(branch)?;
        let Some(branch_name) = branch else {
            return Self::open(root_uri, storage).await;
        };

        let root = normalize_root_uri(root_uri);
        let manifest = ManifestCoordinator::open_at_branch(&root, &branch_name).await?;
        let commit_graph = if storage.exists(&graph_commits_uri(&root))? {
            Some(CommitGraph::open_at_branch(&root, &branch_name).await?)
        } else {
            None
        };
        let run_registry = if storage.exists(&graph_runs_uri(&root))? {
            Some(RunRegistry::open(&root).await?)
        } else {
            None
        };

        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            run_registry,
            bound_branch: Some(branch_name),
        })
    }

    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }

    pub fn version(&self) -> u64 {
        self.manifest.version()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.manifest.snapshot()
    }

    pub fn current_branch(&self) -> Option<&str> {
        self.bound_branch.as_deref()
    }

    pub async fn refresh(&mut self) -> Result<()> {
        self.manifest.refresh().await?;
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph.refresh().await?;
        }
        if let Some(run_registry) = &mut self.run_registry {
            let root_uri = self.root_uri.clone();
            run_registry.refresh(&root_uri).await?;
        }
        Ok(())
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.manifest.list_branches().await.map(|branches| {
            branches
                .into_iter()
                .filter(|branch| !is_internal_run_branch(branch))
                .collect()
        })
    }

    pub async fn branch_create(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::Manifest("cannot create branch 'main'".to_string()))?;
        self.ensure_commit_graph_initialized().await?;
        self.manifest.create_branch(&branch).await?;
        failpoints::maybe_fail("branch_create.after_manifest_branch_create")?;
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph.create_branch(&branch).await?;
        }
        Ok(())
    }

    pub async fn snapshot_at_version(&self, version: u64) -> Result<Snapshot> {
        ManifestCoordinator::snapshot_at(self.root_uri(), self.current_branch(), version).await
    }

    pub async fn resolve_snapshot_id(&self, branch: &str) -> Result<SnapshotId> {
        let normalized = normalize_branch_name(branch)?;
        let other = match normalized.as_deref() {
            Some(branch) => {
                GraphCoordinator::open_branch(self.root_uri(), branch, Arc::clone(&self.storage))
                    .await?
            }
            None => GraphCoordinator::open(self.root_uri(), Arc::clone(&self.storage)).await?,
        };

        Ok(other
            .head_commit_id()
            .await?
            .unwrap_or_else(|| SnapshotId::synthetic(other.current_branch(), other.version())))
    }

    pub async fn resolve_target(&self, target: &ReadTarget) -> Result<ResolvedTarget> {
        match target {
            ReadTarget::Branch(branch) => {
                let normalized = normalize_branch_name(branch)?;
                let other = match normalized.as_deref() {
                    Some(branch) => {
                        GraphCoordinator::open_branch(
                            self.root_uri(),
                            branch,
                            Arc::clone(&self.storage),
                        )
                        .await?
                    }
                    None => {
                        GraphCoordinator::open(self.root_uri(), Arc::clone(&self.storage)).await?
                    }
                };
                let snapshot_id = other.head_commit_id().await?.unwrap_or_else(|| {
                    SnapshotId::synthetic(other.current_branch(), other.version())
                });
                Ok(ResolvedTarget {
                    requested: target.clone(),
                    branch: other.bound_branch.clone(),
                    snapshot_id,
                    snapshot: other.snapshot(),
                })
            }
            ReadTarget::Snapshot(snapshot_id) => {
                let commit = self.resolve_commit(snapshot_id).await?;
                let snapshot = ManifestCoordinator::snapshot_at(
                    self.root_uri(),
                    commit.manifest_branch.as_deref(),
                    commit.manifest_version,
                )
                .await?;
                Ok(ResolvedTarget {
                    requested: target.clone(),
                    branch: commit.manifest_branch.clone(),
                    snapshot_id: snapshot_id.clone(),
                    snapshot,
                })
            }
        }
    }

    pub async fn resolve_commit(&self, snapshot_id: &SnapshotId) -> Result<GraphCommit> {
        if let Some(commit_graph) = &self.commit_graph {
            let commits = commit_graph.load_commits().await?;
            if let Some(commit) = commits
                .into_iter()
                .find(|commit| commit.graph_commit_id == snapshot_id.as_str())
            {
                return Ok(commit);
            }
        }

        for branch in self.manifest.list_branches().await? {
            let normalized = normalize_branch_name(&branch)?;
            let Some(commit_graph) = self
                .open_commit_graph_for_branch(normalized.as_deref())
                .await?
            else {
                break;
            };
            let commits = commit_graph.load_commits().await?;
            if let Some(commit) = commits
                .into_iter()
                .find(|commit| commit.graph_commit_id == snapshot_id.as_str())
            {
                return Ok(commit);
            }
        }

        Err(OmniError::Manifest(format!(
            "commit '{}' not found",
            snapshot_id
        )))
    }

    pub(crate) async fn head_commit_id(&self) -> Result<Option<SnapshotId>> {
        match &self.commit_graph {
            Some(commit_graph) => commit_graph
                .head_commit_id()
                .await
                .map(|id| id.map(SnapshotId::new)),
            None => Ok(None),
        }
    }

    pub(crate) async fn ensure_commit_graph_initialized(&mut self) -> Result<()> {
        if self.commit_graph.is_some() {
            return Ok(());
        }
        if !self.storage.exists(&graph_commits_uri(self.root_uri()))? {
            let _ = CommitGraph::init(self.root_uri(), self.manifest.version()).await?;
        }
        self.commit_graph = match self.current_branch() {
            Some(branch) => Some(CommitGraph::open_at_branch(self.root_uri(), branch).await?),
            None => Some(CommitGraph::open(self.root_uri()).await?),
        };
        Ok(())
    }

    pub(crate) async fn ensure_run_registry_initialized(&mut self) -> Result<()> {
        if self.run_registry.is_some() {
            return Ok(());
        }
        if !self.storage.exists(&graph_runs_uri(self.root_uri()))? {
            let _ = RunRegistry::init(self.root_uri()).await?;
        }
        self.run_registry = Some(RunRegistry::open(self.root_uri()).await?);
        Ok(())
    }

    pub(crate) async fn commit_updates(
        &mut self,
        updates: &[SubTableUpdate],
    ) -> Result<PublishedSnapshot> {
        let manifest_version = self.commit_manifest_updates(updates).await?;
        let snapshot_id = self.record_graph_commit(manifest_version).await?;
        Ok(PublishedSnapshot {
            manifest_version,
            _snapshot_id: snapshot_id,
        })
    }

    pub(crate) async fn commit_manifest_updates(
        &mut self,
        updates: &[SubTableUpdate],
    ) -> Result<u64> {
        let manifest_version = self.manifest.commit(updates).await?;
        failpoints::maybe_fail("graph_publish.after_manifest_commit")?;
        Ok(manifest_version)
    }

    pub(crate) async fn record_graph_commit(
        &mut self,
        manifest_version: u64,
    ) -> Result<SnapshotId> {
        self.ensure_commit_graph_initialized().await?;
        let current_branch = self.current_branch().map(str::to_string);
        let Some(commit_graph) = &mut self.commit_graph else {
            return Ok(SnapshotId::synthetic(
                current_branch.as_deref(),
                manifest_version,
            ));
        };
        failpoints::maybe_fail("graph_publish.before_commit_append")?;
        let graph_commit_id = commit_graph
            .append_commit(current_branch.as_deref(), manifest_version)
            .await?;
        Ok(SnapshotId::new(graph_commit_id))
    }

    pub(crate) async fn record_merge_commit(
        &mut self,
        manifest_version: u64,
        parent_commit_id: &str,
        merged_parent_commit_id: &str,
    ) -> Result<SnapshotId> {
        self.ensure_commit_graph_initialized().await?;
        let current_branch = self.current_branch().map(str::to_string);
        let commit_graph = self.commit_graph.as_mut().ok_or_else(|| {
            OmniError::Manifest("branch merge requires _graph_commits.lance".to_string())
        })?;
        failpoints::maybe_fail("graph_publish.before_commit_append")?;
        let graph_commit_id = commit_graph
            .append_merge_commit(
                current_branch.as_deref(),
                manifest_version,
                parent_commit_id,
                merged_parent_commit_id,
            )
            .await?;
        Ok(SnapshotId::new(graph_commit_id))
    }

    async fn open_commit_graph_for_branch(
        &self,
        branch: Option<&str>,
    ) -> Result<Option<CommitGraph>> {
        if !self.storage.exists(&graph_commits_uri(self.root_uri()))? {
            return Ok(None);
        }
        let graph = match branch {
            Some(branch) => CommitGraph::open_at_branch(self.root_uri(), branch).await?,
            None => CommitGraph::open(self.root_uri()).await?,
        };
        Ok(Some(graph))
    }

    pub(crate) async fn append_run_record(&mut self, record: &RunRecord) -> Result<()> {
        self.ensure_run_registry_initialized().await?;
        let Some(run_registry) = &mut self.run_registry else {
            return Err(OmniError::Manifest(
                "run registry not initialized".to_string(),
            ));
        };
        run_registry.append_record(record).await
    }

    pub(crate) async fn get_run(&self, run_id: &RunId) -> Result<RunRecord> {
        if !self.storage.exists(&graph_runs_uri(self.root_uri()))? {
            return Err(OmniError::Manifest(format!("run '{}' not found", run_id)));
        }
        let run_registry = RunRegistry::open(self.root_uri()).await?;
        run_registry
            .get_run(run_id)
            .await?
            .ok_or_else(|| OmniError::Manifest(format!("run '{}' not found", run_id)))
    }

    pub(crate) async fn list_runs(&self) -> Result<Vec<RunRecord>> {
        if !self.storage.exists(&graph_runs_uri(self.root_uri()))? {
            return Ok(Vec::new());
        }
        let run_registry = RunRegistry::open(self.root_uri()).await?;
        run_registry.list_runs().await
    }
}

fn graph_commits_uri(root_uri: &str) -> String {
    join_uri(root_uri, GRAPH_COMMITS_DIR)
}

fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
    let branch = branch.trim();
    if branch.is_empty() {
        return Err(OmniError::Manifest(
            "branch name cannot be empty".to_string(),
        ));
    }
    if branch == "main" {
        return Ok(None);
    }
    Ok(Some(branch.to_string()))
}
