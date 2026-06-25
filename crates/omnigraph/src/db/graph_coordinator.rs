use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};
use crate::failpoints;
use crate::storage::{StorageAdapter, join_uri, normalize_root_uri};

use super::commit_graph::{CommitGraph, GraphCommit};
use super::is_internal_system_branch;
use super::manifest::{
    ManifestChange, ManifestCoordinator, ManifestIncarnation, Snapshot, SubTableUpdate,
};

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

    pub(crate) fn synthetic(branch: Option<&str>, version: u64, e_tag: Option<&str>) -> Self {
        let branch = branch.unwrap_or("main");
        match e_tag {
            Some(e_tag) => Self(format!("manifest:{}:v{}:etag:{}", branch, version, e_tag)),
            None => Self(format!("manifest:{}:v{}", branch, version)),
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
    bound_branch: Option<String>,
}

impl GraphCoordinator {
    pub async fn init(
        root_uri: &str,
        catalog: &Catalog,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(root_uri)?;
        // The genesis graph commit is folded into the manifest init write, so
        // `__manifest` is the single source of graph lineage from version one
        // (RFC-013 Phase 7). `CommitGraph::init` then creates the empty
        // branch-ref dataset and seeds its cache from that manifest genesis.
        let manifest = ManifestCoordinator::init(&root, catalog).await?;
        let commit_graph = CommitGraph::init(&root).await?;
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph: Some(commit_graph),
            bound_branch: None,
        })
    }

    pub async fn open(root_uri: &str, storage: Arc<dyn StorageAdapter>) -> Result<Self> {
        let root = normalize_root_uri(root_uri)?;
        let manifest = ManifestCoordinator::open(&root).await?;
        let commit_graph = if storage.exists(&graph_commits_uri(&root)).await? {
            Some(CommitGraph::open(&root).await?)
        } else {
            None
        };
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
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

        let root = normalize_root_uri(root_uri)?;
        let manifest = ManifestCoordinator::open_at_branch(&root, &branch_name).await?;
        let commit_graph = if storage.exists(&graph_commits_uri(&root)).await? {
            Some(CommitGraph::open_at_branch(&root, &branch_name).await?)
        } else {
            None
        };

        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            bound_branch: Some(branch_name),
        })
    }

    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }

    pub fn version(&self) -> u64 {
        self.manifest.version()
    }

    pub(crate) fn manifest_incarnation(&self) -> ManifestIncarnation {
        self.manifest.incarnation()
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
        Ok(())
    }

    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        crate::instrumentation::record_probe();
        self.manifest.probe_latest_incarnation().await
    }

    /// Refresh only the manifest (not the commit graph). The read path uses this
    /// on a stale same-branch probe: a read pins its snapshot by manifest version
    /// and never needs the commit graph, so a full `refresh` (which also scans
    /// the commit graph) would be wasted IO.
    pub async fn refresh_manifest_only(&mut self) -> Result<()> {
        self.manifest.refresh().await
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.manifest.list_branches().await.map(|branches| {
            branches
                .into_iter()
                .filter(|branch| !is_internal_system_branch(branch))
                .collect()
        })
    }

    pub(crate) async fn all_branches(&self) -> Result<Vec<String>> {
        self.manifest.list_branches().await
    }

    pub async fn branch_descendants(&self, name: &str) -> Result<Vec<String>> {
        self.manifest
            .descendant_branches(name)
            .await
            .map(|branches| {
                branches
                    .into_iter()
                    .filter(|branch| !is_internal_system_branch(branch))
                    .collect()
            })
    }

    pub async fn branch_create(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot create branch 'main'".to_string()))?;
        self.ensure_commit_graph_initialized().await?;

        // Manifest authority flip first.
        self.manifest.create_branch(&branch).await?;

        // Derived commit-graph branch. If anything after the authority flip
        // fails, roll back the manifest branch so the branch never half-exists
        // (a manifest branch with no commit-graph branch breaks the next write).
        if let Err(err) = self.create_commit_graph_branch(&branch).await {
            if let Err(rollback_err) = self.manifest.delete_branch(&branch).await {
                tracing::warn!(
                    target: "omnigraph::branch_create",
                    branch = %branch,
                    error = %rollback_err,
                    "rollback of manifest branch failed after commit-graph create failure",
                );
            }
            return Err(err);
        }
        Ok(())
    }

    /// Create the derived commit-graph branch for `branch`, healing a zombie ref
    /// left by an incomplete prior delete. The manifest branch was just created
    /// fresh, so any existing commit-graph branch with this name is provably
    /// orphaned and is force-dropped before recreating.
    async fn create_commit_graph_branch(&mut self, branch: &str) -> Result<()> {
        failpoints::maybe_fail(crate::failpoints::names::BRANCH_CREATE_AFTER_MANIFEST_BRANCH_CREATE)?;
        let Some(commit_graph) = &mut self.commit_graph else {
            return Ok(());
        };
        if commit_graph
            .list_branches()
            .await?
            .iter()
            .any(|existing| existing == branch)
        {
            commit_graph.force_delete_branch(branch).await?;
        }
        commit_graph.create_branch(branch).await
    }

    pub async fn branch_delete(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot delete branch 'main'".to_string()))?;
        if self.current_branch() == Some(branch.as_str()) {
            return Err(OmniError::manifest_conflict(format!(
                "cannot delete currently active branch '{}'",
                branch
            )));
        }

        // Manifest authority flip — the single atomic op that makes the branch
        // cease to exist. Must succeed; everything after is derived state
        // reclaimed best-effort.
        self.manifest.delete_branch(&branch).await?;

        // Commit-graph branch is derived state. Reclaim best-effort with the
        // idempotent force variant: a failure here (or a missing dataset) is
        // reconciled by `cleanup` and must not fail the delete after the
        // authority already flipped.
        if let Err(err) = self.reclaim_commit_graph_branch(&branch).await {
            tracing::warn!(
                target: "omnigraph::branch_delete::cleanup",
                branch = %branch,
                error = %err,
                "best-effort commit-graph branch reclaim failed; cleanup will reconcile",
            );
        }

        Ok(())
    }

    /// Best-effort, idempotent reclaim of the commit-graph branch `branch`.
    /// Tolerates an absent commit-graph dataset (a graph that never committed).
    async fn reclaim_commit_graph_branch(&mut self, branch: &str) -> Result<()> {
        failpoints::maybe_fail(crate::failpoints::names::BRANCH_DELETE_BEFORE_COMMIT_GRAPH_RECLAIM)?;
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph.force_delete_branch(branch).await
        } else if self
            .storage
            .exists(&graph_commits_uri(self.root_uri()))
            .await?
        {
            let mut commit_graph = CommitGraph::open(self.root_uri()).await?;
            commit_graph.force_delete_branch(branch).await
        } else {
            Ok(())
        }
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

        Ok(other.head_commit_id().await?.unwrap_or_else(|| {
            SnapshotId::synthetic(
                other.current_branch(),
                other.version(),
                other.manifest_incarnation().e_tag.as_deref(),
            )
        }))
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
                    SnapshotId::synthetic(
                        other.current_branch(),
                        other.version(),
                        other.manifest_incarnation().e_tag.as_deref(),
                    )
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
            if let Some(commit) = commit_graph.get_commit(snapshot_id.as_str()) {
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
            if let Some(commit) = commit_graph.get_commit(snapshot_id.as_str()) {
                return Ok(commit);
            }
        }

        Err(OmniError::manifest_not_found(format!(
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
        if !self
            .storage
            .exists(&graph_commits_uri(self.root_uri()))
            .await?
        {
            // A graph opened without a commit-graph dataset gets the empty
            // branch-ref dataset created lazily here. Graph lineage lives in
            // `__manifest` (RFC-013 Phase 7) — a graph initialized by current
            // code already carries its genesis there, and the commit graph
            // sources its cache from it. No genesis is written here.
            CommitGraph::init(self.root_uri()).await?;
        }
        self.commit_graph = match self.current_branch() {
            Some(branch) => Some(CommitGraph::open_at_branch(self.root_uri(), branch).await?),
            None => Some(CommitGraph::open(self.root_uri()).await?),
        };
        Ok(())
    }

    pub(crate) async fn commit_updates_with_actor(
        &mut self,
        updates: &[SubTableUpdate],
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        self.commit_updates_with_actor_with_expected(updates, &HashMap::new(), actor_id)
            .await
    }

    /// Commit with publisher-level OCC fence. The `expected_table_versions` map
    /// asserts the manifest's current latest non-tombstoned `table_version` for
    /// each `table_key` matches what the caller observed before writing.
    /// Mismatches surface as `OmniError::Manifest` with
    /// `ManifestConflictDetails::ExpectedVersionMismatch`.
    pub(crate) async fn commit_updates_with_actor_with_expected(
        &mut self,
        updates: &[SubTableUpdate],
        expected_table_versions: &HashMap<String, u64>,
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        let changes = updates_to_changes(updates);
        self.commit_changes_with_actor_with_expected(&changes, expected_table_versions, actor_id)
            .await
    }

    pub(crate) async fn commit_changes_with_actor(
        &mut self,
        changes: &[ManifestChange],
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        self.commit_changes_with_actor_with_expected(changes, &HashMap::new(), actor_id)
            .await
    }

    /// Publish `changes` and record one graph commit in the SAME manifest CAS
    /// (RFC-013 Phase 7). The lineage intent (a freshly minted commit id, the
    /// branch, the actor) rides the publish so the `graph_commit` + `graph_head`
    /// rows land atomically with the table-version rows — one manifest version,
    /// no separate write, no `commit_graph.refresh()` to pick a parent (the
    /// publisher resolves it under the CAS). The in-memory commit cache is then
    /// updated from the intent + the resolved parent without a re-read.
    async fn commit_changes_with_actor_with_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &HashMap<String, u64>,
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        self.ensure_commit_graph_initialized().await?;
        let intent = self.new_lineage_intent(actor_id, None)?;
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND)?;
        let outcome = self
            .manifest
            .commit_changes_with_lineage(changes, expected_table_versions, Some(&intent))
            .await?;
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT)?;
        let snapshot_id = self.apply_lineage_to_cache(intent, &outcome);
        Ok(PublishedSnapshot {
            manifest_version: outcome.version,
            _snapshot_id: snapshot_id,
        })
    }

    /// Publish a branch-merge: `updates` (the merged table versions) plus the
    /// merge commit, in one manifest CAS (RFC-013 Phase 7). The merge commit's
    /// merged-in parent is `merged_parent_commit_id` (the source head, stable);
    /// its first parent is resolved by the publisher as the current target-branch
    /// head — the live head, which is the post-merge correct parent even if the
    /// target advanced since the merge began.
    pub(crate) async fn commit_merge_with_actor(
        &mut self,
        updates: &[SubTableUpdate],
        merged_parent_commit_id: &str,
        actor_id: Option<&str>,
    ) -> Result<SnapshotId> {
        self.ensure_commit_graph_initialized().await?;
        let intent =
            self.new_lineage_intent(actor_id, Some(merged_parent_commit_id.to_string()))?;
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND)?;
        let changes = updates_to_changes(updates);
        let outcome = self
            .manifest
            .commit_changes_with_lineage(&changes, &HashMap::new(), Some(&intent))
            .await?;
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT)?;
        Ok(self.apply_lineage_to_cache(intent, &outcome))
    }

    /// Mint a [`LineageIntent`] for the next commit on the current branch: a
    /// fresh ULID (stable across the publisher's CAS retries) and a timestamp.
    /// The parent is NOT chosen here — the publisher resolves it per attempt
    /// against the manifest it commits against.
    fn new_lineage_intent(
        &self,
        actor_id: Option<&str>,
        merged_parent_commit_id: Option<String>,
    ) -> Result<crate::db::manifest::LineageIntent> {
        Ok(crate::db::manifest::LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            branch: self.current_branch().map(str::to_string),
            actor_id: actor_id.map(str::to_string),
            merged_parent_commit_id,
            created_at: crate::db::now_micros()?,
        })
    }

    /// Insert the just-published commit into the in-memory commit cache from the
    /// intent + the publisher-resolved parent + the new manifest version. No
    /// storage I/O: the durable write already happened in the publish CAS, and
    /// this keeps a same-handle read's `head_commit_id` consistent with the
    /// snapshot it just advanced. Falls back to a synthetic id only when the
    /// commit graph is somehow absent (never on a real write).
    fn apply_lineage_to_cache(
        &mut self,
        intent: crate::db::manifest::LineageIntent,
        outcome: &crate::db::manifest::CommitOutcome,
    ) -> SnapshotId {
        let Some(commit_graph) = &mut self.commit_graph else {
            return SnapshotId::synthetic(
                self.bound_branch.as_deref(),
                outcome.version,
                self.manifest.incarnation().e_tag.as_deref(),
            );
        };
        let commit = GraphCommit {
            graph_commit_id: intent.graph_commit_id.clone(),
            manifest_branch: intent.branch,
            manifest_version: outcome.version,
            parent_commit_id: outcome.parent_commit_id.clone(),
            merged_parent_commit_id: intent.merged_parent_commit_id,
            actor_id: intent.actor_id,
            created_at: intent.created_at,
        };
        commit_graph.insert_committed(commit);
        SnapshotId::new(intent.graph_commit_id)
    }

    async fn open_commit_graph_for_branch(
        &self,
        branch: Option<&str>,
    ) -> Result<Option<CommitGraph>> {
        if !self
            .storage
            .exists(&graph_commits_uri(self.root_uri()))
            .await?
        {
            return Ok(None);
        }
        let graph = match branch {
            Some(branch) => CommitGraph::open_at_branch(self.root_uri(), branch).await?,
            None => CommitGraph::open(self.root_uri()).await?,
        };
        Ok(Some(graph))
    }

    pub(crate) async fn list_commits(&self) -> Result<Vec<GraphCommit>> {
        if let Some(commit_graph) = &self.commit_graph {
            return commit_graph.load_commits().await;
        }
        if !self
            .storage
            .exists(&graph_commits_uri(self.root_uri()))
            .await?
        {
            return Ok(Vec::new());
        }
        let commit_graph = match self.current_branch() {
            Some(branch) => CommitGraph::open_at_branch(self.root_uri(), branch).await?,
            None => CommitGraph::open(self.root_uri()).await?,
        };
        commit_graph.load_commits().await
    }
}

fn graph_commits_uri(root_uri: &str) -> String {
    join_uri(root_uri, GRAPH_COMMITS_DIR)
}

/// Wrap each `SubTableUpdate` as a `ManifestChange::Update` for the publisher.
fn updates_to_changes(updates: &[SubTableUpdate]) -> Vec<ManifestChange> {
    updates
        .iter()
        .cloned()
        .map(ManifestChange::Update)
        .collect()
}

fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
    let branch = branch.trim();
    if branch.is_empty() {
        return Err(OmniError::manifest(
            "branch name cannot be empty".to_string(),
        ));
    }
    if branch == "main" {
        return Ok(None);
    }
    Ok(Some(branch.to_string()))
}
