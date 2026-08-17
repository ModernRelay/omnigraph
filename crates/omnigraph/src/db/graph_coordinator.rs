use std::fmt;
use std::sync::Arc;

use lance::Dataset;

use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};
use crate::failpoints;
use crate::storage::{StorageAdapter, normalize_root_uri};

use super::commit_graph::{CommitGraph, FirstParentEdge, GraphCommit};
use super::is_internal_system_branch;
use super::manifest::{
    CapturedManifestProbe, ExpectedTableVersions, GenesisManifestAttempt, LineageIntent,
    ManifestChange, ManifestCoordinator, ManifestIncarnation, ManifestInitError,
    PublishPrecondition, Snapshot, SubTableUpdate,
};

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
    /// Effective graph-lineage head of this exact snapshot. On a freshly
    /// forked named branch this is the inherited source commit even though the
    /// branch intentionally has no materialized `graph_head:<branch>` row yet.
    pub graph_commit_id: Option<String>,
    pub snapshot: Snapshot,
}

/// Internal lineage classification for an existing two-commit diff request.
/// Arbitrary ranges retain net-current semantics; direct adjacency is derived
/// only from the child's persisted first-parent pointer.
pub(crate) enum ResolvedCommitRange {
    FirstParent(FirstParentEdge),
    Arbitrary { from: GraphCommit, to: GraphCommit },
}

fn classify_commit_range(from: GraphCommit, to: GraphCommit) -> ResolvedCommitRange {
    if to.parent_commit_id.as_deref() == Some(from.graph_commit_id.as_str()) {
        ResolvedCommitRange::FirstParent(FirstParentEdge {
            parent: from,
            child: to,
        })
    } else {
        ResolvedCommitRange::Arbitrary { from, to }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedSnapshot {
    pub manifest_version: u64,
    pub _snapshot_id: SnapshotId,
    pub commit: GraphCommit,
}

pub(crate) struct GraphCoordinator {
    root_uri: String,
    storage: Arc<dyn StorageAdapter>,
    manifest: ManifestCoordinator,
    commit_graph: CommitGraph,
    bound_branch: Option<String>,
}

impl GraphCoordinator {
    /// Commit half of coordinator init: ends at the `__manifest` Create
    /// commit (see `init_commit_phase` for the phase contract).
    pub(crate) async fn init_commit_with_session(
        root_uri: &str,
        catalog: &Catalog,
        control_session: &Arc<lance::session::Session>,
        attempt: &GenesisManifestAttempt,
    ) -> std::result::Result<Dataset, ManifestInitError> {
        let root = normalize_root_uri(root_uri)?;
        // The genesis graph commit is folded into the manifest init write, so
        // `__manifest` is the single source of graph lineage from version one
        // (RFC-013 Phase 7).
        ManifestCoordinator::init_commit(&root, catalog, control_session, attempt).await
    }

    /// Reopen an acknowledgement-unknown manifest Create and construct a
    /// coordinator only when the exact attempt-local genesis receipt is
    /// present.  The caller still owns schema-IR validation before it can
    /// return a graph handle.
    pub(crate) async fn open_exact_genesis_with_storage(
        root_uri: &str,
        attempt: &GenesisManifestAttempt,
        storage: Arc<dyn StorageAdapter>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let root = normalize_root_uri(root_uri)?;
        let (manifest, lineage_rows) =
            ManifestCoordinator::open_exact_genesis_with_lineage(&root, attempt, control_session)
                .await?;
        let commit_graph = CommitGraph::from_manifest_rows(&root, None, lineage_rows);
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            bound_branch: None,
        })
    }

    /// Post-commit half of coordinator init: builds the coordinator's view of
    /// the completed graph; see `init_post_commit_checks` for the caller
    /// contract.
    pub(crate) async fn finish_init_with_storage(
        root_uri: &str,
        dataset: Dataset,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(root_uri)?;
        let (manifest, lineage_rows) = ManifestCoordinator::finish_init(&root, dataset).await?;
        let commit_graph = CommitGraph::from_manifest_rows(&root, None, lineage_rows);
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            commit_graph,
            bound_branch: None,
        })
    }

    pub async fn open(root_uri: &str, storage: Arc<dyn StorageAdapter>) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_with_session(root_uri, storage, &control_session).await
    }

    pub(crate) async fn open_with_session(
        root_uri: &str,
        storage: Arc<dyn StorageAdapter>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let root = normalize_root_uri(root_uri)?;
        let (manifest, lineage_rows) =
            ManifestCoordinator::open_with_lineage(&root, None, control_session).await?;
        let commit_graph = CommitGraph::from_manifest_rows(&root, None, lineage_rows);
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
        let control_session = crate::lance_access::control_session();
        Self::open_branch_with_session(root_uri, branch, storage, &control_session).await
    }

    pub(crate) async fn open_branch_with_session(
        root_uri: &str,
        branch: &str,
        storage: Arc<dyn StorageAdapter>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let branch = normalize_branch_name(branch)?;
        let Some(branch_name) = branch else {
            return Self::open_with_session(root_uri, storage, control_session).await;
        };

        let root = normalize_root_uri(root_uri)?;
        let (manifest, lineage_rows) =
            ManifestCoordinator::open_with_lineage(&root, Some(&branch_name), control_session)
                .await?;
        let commit_graph = CommitGraph::from_manifest_rows(&root, Some(&branch_name), lineage_rows);

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

    pub(crate) fn captured_manifest_probe(&self) -> CapturedManifestProbe {
        self.manifest.captured_probe()
    }

    /// Lance-native identity of the active `__manifest` branch. Stable across
    /// commits; changes when a named branch is deleted and recreated.
    pub(crate) async fn branch_identifier(&self) -> Result<lance::dataset::refs::BranchIdentifier> {
        self.manifest.branch_identifier().await
    }

    /// Exact `graph_head:<active-branch>` pointer, preserving `None` for a
    /// freshly-created named branch even though its inherited commit history has
    /// an inferred head. Sourced from the manifest coordinator's SAME pinned
    /// state as [`Self::snapshot`], not the separately refreshed lineage cache.
    pub(crate) fn exact_graph_head(&self) -> Option<String> {
        self.manifest.exact_graph_head()
    }

    /// Effective lineage head for the manifest snapshot held by this
    /// coordinator. The exact branch-head row is authoritative once the branch
    /// owns a commit. Its absence is first-class only for a fresh fork, where
    /// the commit projection loaded from the same branch manifest supplies the
    /// inherited source head.
    pub(crate) async fn effective_graph_head(&self) -> Result<Option<String>> {
        match self.exact_graph_head() {
            Some(head) => Ok(Some(head)),
            None => self
                .head_commit_id()
                .await
                .map(|head| head.map(|head| head.as_str().to_string())),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.manifest.snapshot()
    }

    pub fn current_branch(&self) -> Option<&str> {
        self.bound_branch.as_deref()
    }

    pub async fn refresh(&mut self) -> Result<()> {
        let lineage_rows = self.manifest.refresh_with_lineage().await?;
        self.commit_graph.replace_from_manifest_rows(lineage_rows);
        Ok(())
    }

    /// Refresh the live read snapshot and, only when its exact branch-head row
    /// is absent, the inherited lineage fallback. `ManifestCoordinator`
    /// completes every required read before installing either new view, so a
    /// failure cannot leave replacement rows paired with stale branch lineage.
    pub(crate) async fn refresh_for_live_read(&mut self) -> Result<()> {
        if let Some(lineage_rows) = self.manifest.refresh_for_live_read().await? {
            self.commit_graph.replace_from_manifest_rows(lineage_rows);
        }
        Ok(())
    }

    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        crate::instrumentation::record_probe();
        self.manifest.probe_latest_incarnation().await
    }

    /// Clone the already-loaded lineage projection. This performs no storage
    /// I/O; branch merge uses it to compute the base from the same coordinator
    /// instances that supplied source/target authority.
    pub(crate) async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        self.commit_graph.load_commits().await
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

    pub(crate) async fn branch_create(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot create branch 'main'".to_string()))?;

        // Manifest BranchContents is the single branch authority. Lance creates
        // it in two physical phases (shallow clone, then BranchContents); the
        // manifest coordinator classifies/reclaims a clone-only zombie before
        // a bounded retry. No graph-lineage branch is created or rolled back.
        self.manifest.create_branch(&branch).await
    }

    pub(crate) async fn branch_delete(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot delete branch 'main'".to_string()))?;
        if self.current_branch() == Some(branch.as_str()) {
            return Err(OmniError::manifest_conflict(format!(
                "cannot delete currently active branch '{}'",
                branch
            )));
        }

        // Removing manifest BranchContents is the logical visibility point.
        // Lance reclaims the branch tree afterward, so an error may still mean
        // logical deletion succeeded; the manifest coordinator reclassifies
        // that outcome from fresh authority. Per-table data forks remain
        // derived state and are reclaimed by the engine afterward.
        self.manifest.delete_branch(&branch).await
    }

    /// Delete the branch represented by an operation-local post-gate capture.
    ///
    /// Unlike [`Self::branch_delete`], this permits the disposable coordinator
    /// itself to be bound to `name`. The exact captured BranchIdentifier fences
    /// delete/recreate ABA; the caller discards this coordinator after the
    /// native authority change.
    pub(crate) async fn branch_delete_captured(
        &mut self,
        name: &str,
        expected_identifier: &lance::dataset::refs::BranchIdentifier,
    ) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot delete branch 'main'".to_string()))?;
        self.manifest
            .delete_branch_with_expected(&branch, expected_identifier)
            .await
    }

    pub async fn snapshot_at_version(&self, version: u64) -> Result<Snapshot> {
        ManifestCoordinator::snapshot_at(self.root_uri(), self.current_branch(), version).await
    }

    pub async fn resolve_snapshot_id(&self, branch: &str) -> Result<SnapshotId> {
        let normalized = normalize_branch_name(branch)?;
        let other = match normalized.as_deref() {
            Some(branch) => {
                GraphCoordinator::open_branch_with_session(
                    self.root_uri(),
                    branch,
                    Arc::clone(&self.storage),
                    &self.manifest.control_session(),
                )
                .await?
            }
            None => {
                GraphCoordinator::open_with_session(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &self.manifest.control_session(),
                )
                .await?
            }
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
                        GraphCoordinator::open_branch_with_session(
                            self.root_uri(),
                            branch,
                            Arc::clone(&self.storage),
                            &self.manifest.control_session(),
                        )
                        .await?
                    }
                    None => {
                        GraphCoordinator::open_with_session(
                            self.root_uri(),
                            Arc::clone(&self.storage),
                            &self.manifest.control_session(),
                        )
                        .await?
                    }
                };
                let graph_commit_id = other.effective_graph_head().await?;
                let snapshot_id = graph_commit_id
                    .as_deref()
                    .map(SnapshotId::new)
                    .unwrap_or_else(|| {
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
                    graph_commit_id,
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
                    graph_commit_id: Some(commit.graph_commit_id),
                    snapshot,
                })
            }
        }
    }

    pub async fn resolve_commit(&self, snapshot_id: &SnapshotId) -> Result<GraphCommit> {
        if let Some(commit) = self.commit_graph.get_commit(snapshot_id.as_str()) {
            return Ok(commit);
        }

        for branch in self.manifest.list_branches().await? {
            let normalized = normalize_branch_name(&branch)?;
            let commit_graph = self
                .open_commit_graph_for_branch(normalized.as_deref())
                .await?;
            if let Some(commit) = commit_graph.get_commit(snapshot_id.as_str()) {
                return Ok(commit);
            }
        }

        Err(OmniError::manifest_not_found(format!(
            "commit '{}' not found",
            snapshot_id
        )))
    }

    /// Resolve both endpoints and classify direct first-parent adjacency from
    /// the child's persisted parent pointer.
    ///
    /// This is deliberately O(1) after the two commits are resolved: it adds
    /// no ancestry index or history walk. Arbitrary ranges retain the existing
    /// net-current diff semantics.
    pub(crate) async fn resolve_commit_range(
        &self,
        from_id: &SnapshotId,
        to_id: &SnapshotId,
    ) -> Result<ResolvedCommitRange> {
        let from = self.resolve_commit(from_id).await?;
        let to = self.resolve_commit(to_id).await?;
        Ok(classify_commit_range(from, to))
    }

    pub(crate) async fn head_commit_id(&self) -> Result<Option<SnapshotId>> {
        self.commit_graph
            .head_commit_id()
            .await
            .map(|id| id.map(SnapshotId::new))
    }

    #[cfg(test)]
    pub(crate) async fn commit_updates_with_actor(
        &mut self,
        updates: &[SubTableUpdate],
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        self.commit_updates_with_actor_with_expected(
            updates,
            &ExpectedTableVersions::new(),
            actor_id,
        )
        .await
    }

    /// Commit with publisher-level OCC fence. The `expected_table_versions` map
    /// asserts the manifest's current latest non-tombstoned `table_version` for
    /// each immutable table identity matches what the caller observed before
    /// writing; the diagnostic alias is checked as part of the expectation.
    /// Mismatches surface as `OmniError::Manifest` with
    /// `ManifestConflictDetails::ExpectedVersionMismatch`.
    pub(crate) async fn commit_updates_with_actor_with_expected(
        &mut self,
        updates: &[SubTableUpdate],
        expected_table_versions: &ExpectedTableVersions,
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        let changes = updates_to_changes(updates);
        self.commit_changes_with_actor_with_expected(&changes, expected_table_versions, actor_id)
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
        expected_table_versions: &ExpectedTableVersions,
        actor_id: Option<&str>,
    ) -> Result<PublishedSnapshot> {
        let intent = self.new_lineage_intent(actor_id, None)?;
        self.commit_changes_with_intent_and_expected(
            changes,
            expected_table_versions,
            intent,
            &PublishPrecondition::Any,
        )
        .await
    }

    /// Publish a pre-minted lineage intent under an explicit authority
    /// precondition. The intent's identity and timestamp remain stable across
    /// publisher retries and can also be persisted by the caller's recovery
    /// protocol before this method is invoked.
    pub(crate) async fn commit_changes_with_intent_and_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        intent: LineageIntent,
        precondition: &PublishPrecondition,
    ) -> Result<PublishedSnapshot> {
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND)?;
        let outcome = self
            .manifest
            .commit_changes_with_lineage_and_precondition(
                changes,
                expected_table_versions,
                Some(&intent),
                precondition,
            )
            .await?;
        failpoints::maybe_fail(crate::failpoints::names::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT)?;
        let commit = self.apply_lineage_to_cache(intent, &outcome);
        Ok(PublishedSnapshot {
            manifest_version: outcome.version,
            _snapshot_id: SnapshotId::new(commit.graph_commit_id.clone()),
            commit,
        })
    }

    /// Mint a [`LineageIntent`] for the next commit on the current branch: a
    /// fresh ULID (stable across the publisher's CAS retries) and a timestamp.
    /// The parent is NOT chosen here — the publisher resolves it per attempt
    /// against the manifest it commits against.
    pub(crate) fn new_lineage_intent(
        &self,
        actor_id: Option<&str>,
        merged_parent_commit_id: Option<String>,
    ) -> Result<LineageIntent> {
        Ok(LineageIntent {
            graph_commit_id: crate::dst_ids::new_ulid().to_string(),
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
    /// snapshot it just advanced.
    fn apply_lineage_to_cache(
        &mut self,
        intent: crate::db::manifest::LineageIntent,
        outcome: &crate::db::manifest::CommitOutcome,
    ) -> GraphCommit {
        let commit = GraphCommit {
            graph_commit_id: intent.graph_commit_id.clone(),
            manifest_branch: intent.branch,
            manifest_version: outcome.version,
            parent_commit_id: outcome.parent_commit_id.clone(),
            merged_parent_commit_id: intent.merged_parent_commit_id,
            actor_id: intent.actor_id,
            created_at: intent.created_at,
        };
        self.commit_graph.insert_committed(commit.clone());
        commit
    }

    async fn open_commit_graph_for_branch(&self, branch: Option<&str>) -> Result<CommitGraph> {
        match branch {
            Some(branch) => CommitGraph::open_at_branch(self.root_uri(), branch).await,
            None => CommitGraph::open(self.root_uri()).await,
        }
    }

    pub(crate) async fn list_commits(&self) -> Result<Vec<GraphCommit>> {
        self.commit_graph.load_commits().await
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(
        id: &str,
        parent_commit_id: Option<&str>,
        merged_parent_commit_id: Option<&str>,
    ) -> GraphCommit {
        GraphCommit {
            graph_commit_id: id.to_string(),
            manifest_branch: None,
            manifest_version: 1,
            parent_commit_id: parent_commit_id.map(str::to_string),
            merged_parent_commit_id: merged_parent_commit_id.map(str::to_string),
            actor_id: None,
            created_at: 0,
        }
    }

    #[test]
    fn commit_range_classification_uses_only_the_child_first_parent_pointer() {
        let root = commit("root", None, None);
        let child = commit("child", Some("root"), None);
        match classify_commit_range(root.clone(), child.clone()) {
            ResolvedCommitRange::FirstParent(edge) => {
                assert_eq!(edge.parent.graph_commit_id, "root");
                assert_eq!(edge.child.graph_commit_id, "child");
            }
            ResolvedCommitRange::Arbitrary { .. } => {
                panic!("a direct child must classify as a first-parent edge")
            }
        }
        assert!(matches!(
            classify_commit_range(child.clone(), root.clone()),
            ResolvedCommitRange::Arbitrary { .. }
        ));
        assert!(matches!(
            classify_commit_range(root, commit("grandchild", Some("child"), None)),
            ResolvedCommitRange::Arbitrary { .. }
        ));

        let left = commit("left", Some("root"), None);
        let right = commit("right", Some("root"), None);
        let merge = commit("merge", Some("left"), Some("right"));
        match classify_commit_range(left, merge.clone()) {
            ResolvedCommitRange::FirstParent(edge) => {
                assert_eq!(edge.parent.graph_commit_id, "left");
                assert_eq!(edge.child.graph_commit_id, "merge");
                assert_eq!(edge.child.merged_parent_commit_id.as_deref(), Some("right"));
            }
            ResolvedCommitRange::Arbitrary { .. } => {
                panic!("a merge must be adjacent only to its persisted first parent")
            }
        }
        assert!(matches!(
            classify_commit_range(right, merge),
            ResolvedCommitRange::Arbitrary { .. }
        ));
    }
}
