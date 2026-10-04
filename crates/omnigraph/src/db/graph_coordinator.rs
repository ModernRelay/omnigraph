use std::fmt;
use std::sync::Arc;

use lance::Dataset;

use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, normalize_root_uri};

use super::commit_graph::{
    CommitGraph, FirstParentEdge, GraphCommit, HistoryCache, Lineage,
    graph_commit_from_manifest_row,
};
use super::manifest::{
    BranchRecords, CapturedManifestProbe, CommitBuffer, DatasetUpdate, ExpectedTableVersions,
    GenesisManifestAttempt, HistoryRecord, HistoryReleaseBytes, LineageIntent, ManifestChange,
    ManifestCoordinator, ManifestIncarnation, ManifestInitError, PublishPrecondition,
    SchemaContractRow,
};
use super::snapshot::Snapshot;
use crate::seams::{decide_seam, fail};

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
    pub graph_manifest_version: u64,
    pub _snapshot_id: SnapshotId,
    pub commit: GraphCommit,
}

pub(crate) struct GraphCoordinator {
    root_uri: String,
    storage: Arc<dyn StorageAdapter>,
    manifest: ManifestCoordinator,
    bound_branch: Option<String>,
}

/// What a merge reads of one captured branch beside its write transaction: the
/// captured head's record, the buffer of the `__manifest` version that holds
/// it, and the commit graph that can load that exact head's lineage if needed.
pub(crate) struct CapturedLineage {
    head: HistoryRecord,
    buffer: CommitBuffer,
    pub graph: CommitGraph,
}

impl CapturedLineage {
    /// The records a merge of the captured branch appends to `__history`: its
    /// buffered commits after `merge_base`, oldest first, then its head.
    pub(crate) fn into_records(self, merge_base: &str) -> BranchRecords {
        BranchRecords {
            buffer: self.buffer,
            head: self.head,
            merge_base: Some(merge_base.to_string()),
        }
    }
}

/// A graph commit with the snapshot of the graph as of it.
pub(crate) struct CommitState {
    pub commit: GraphCommit,
    pub snapshot: Snapshot,
}

decide_seam! {
    pub static GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT = ("graph_publish.after_manifest_commit", AnyWrite, [Fail]);
}

decide_seam! {
    pub static GRAPH_PUBLISH_BEFORE_COMMIT_APPEND = ("graph_publish.before_commit_append", AnyWrite, [Fail]);
}

impl GraphCoordinator {
    /// Commit half of coordinator init: ends at the `__manifest` Create
    /// commit (see `init_commit_phase` for the phase contract).
    pub(crate) async fn init_commit_with_session(
        root_uri: &str,
        catalog: &Catalog,
        contract: &SchemaContractRow,
        control_session: &Arc<lance::session::Session>,
        attempt: &GenesisManifestAttempt,
    ) -> std::result::Result<Dataset, ManifestInitError> {
        let root = normalize_root_uri(root_uri)?;
        // The genesis graph commit is folded into the manifest init write, so
        // `__manifest` is the single source of graph lineage from version one
        // (RFC-013 Phase 7).
        ManifestCoordinator::init_commit(&root, catalog, contract, control_session, attempt).await
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
        let manifest =
            ManifestCoordinator::open_exact_genesis(&root, attempt, control_session).await?;
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
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
        let manifest = ManifestCoordinator::finish_init(&root, dataset).await?;
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            bound_branch: None,
        })
    }

    #[cfg(test)]
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
        let (manifest, _lineage_rows, _contract) =
            ManifestCoordinator::open_with_lineage_and_contract(&root, None, control_session)
                .await?;
        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            bound_branch: None,
        })
    }

    #[cfg(test)]
    pub async fn open_branch(
        root_uri: &str,
        branch: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_branch_with_session(root_uri, branch, storage, &control_session).await
    }

    pub(crate) async fn open_with_contract(
        root_uri: &str,
        storage: Arc<dyn StorageAdapter>,
        prepared: crate::db::manifest::PreparedManifestOpen,
    ) -> Result<(Self, SchemaContractRow)> {
        let root = normalize_root_uri(root_uri)?;
        let (manifest, _lineage_rows, contract) =
            ManifestCoordinator::open_prepared_with_lineage_and_contract(&root, prepared).await?;
        let mut coordinator = Self {
            root_uri: root,
            storage,
            manifest,
            bound_branch: None,
        };
        let contract = coordinator.refresh_contract_capture(contract).await?;
        Ok((coordinator, contract))
    }

    async fn refresh_contract_capture(
        &mut self,
        contract: Result<SchemaContractRow>,
    ) -> Result<SchemaContractRow> {
        let captured = self.snapshot();
        self.refresh().await?;
        self.manifest.validate_serving_format()?;
        if self.snapshot().same_manifest_image(&captured) {
            contract
        } else {
            self.read_schema_contract().await
        }
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
        let (manifest, _lineage_rows, _contract) =
            ManifestCoordinator::open_with_lineage_and_contract(
                &root,
                Some(&branch_name),
                control_session,
            )
            .await?;

        Ok(Self {
            root_uri: root,
            storage,
            manifest,
            bound_branch: Some(branch_name),
        })
    }

    /// The coordinator of `branch` (`None` = main), opened from its
    /// `__manifest` and reading settled commits through this coordinator's
    /// history cache.
    async fn open_sibling(&self, branch: Option<&str>) -> Result<Self> {
        let session = self.manifest.control_session();
        let storage = Arc::clone(&self.storage);
        let sibling = match branch {
            Some(branch) => {
                Self::open_branch_with_session(self.root_uri(), branch, storage, &session).await?
            }
            None => Self::open_with_session(self.root_uri(), storage, &session).await?,
        };
        Ok(sibling.sharing_history(self.history().clone()))
    }

    /// This coordinator reading settled commits through `history`.
    pub(crate) fn sharing_history(mut self, history: HistoryCache) -> Self {
        self.manifest.share_history(history);
        self
    }

    pub(crate) fn history(&self) -> &HistoryCache {
        self.manifest.history()
    }

    /// An operation-local source for native branch controls. Current table
    /// state and the head are copied; the history cache and the Lance session
    /// are shared. Callers must first probe the complete manifest incarnation.
    pub(crate) fn capture_for_branch_control(&self) -> Self {
        Self {
            root_uri: self.root_uri.clone(),
            storage: Arc::clone(&self.storage),
            manifest: self.manifest.capture(),
            bound_branch: self.bound_branch.clone(),
        }
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

    /// Lance-native identity captured with this coordinator's active
    /// `__manifest` state. Stable across commits; changes when a named branch
    /// is deleted and recreated.
    pub(crate) async fn branch_identifier(&self) -> Result<lance::dataset::refs::BranchIdentifier> {
        self.manifest.branch_identifier().await
    }

    /// The exact head of the active branch: the head commit when this branch
    /// incarnation wrote it, `None` on a fork that has not published. From the
    /// same `__manifest` version as [`Self::snapshot`].
    pub(crate) fn exact_graph_head(&self) -> Option<String> {
        self.manifest.exact_graph_head()
    }

    /// The head commit of the `__manifest` version this coordinator holds,
    /// which a fork that has not published inherited from its source.
    pub(crate) fn head_commit(&self) -> GraphCommit {
        graph_commit_from_manifest_row(self.manifest.head().clone())
    }

    /// The id of [`Self::head_commit`].
    pub(crate) async fn effective_graph_head(&self) -> Result<Option<String>> {
        Ok(Some(self.manifest.head().graph_commit_id.clone()))
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::wrap(self.manifest.snapshot())
    }

    /// Read the contract of the same pinned manifest image as [`Self::snapshot`],
    /// using captured content when available and a filtered scan otherwise.
    pub(crate) async fn read_schema_contract(&self) -> Result<SchemaContractRow> {
        self.manifest.read_schema_contract().await
    }

    pub fn current_branch(&self) -> Option<&str> {
        self.bound_branch.as_deref()
    }

    /// Install the latest version of the branch's `__manifest`: the table
    /// state and the head, from one read of that version.
    pub async fn refresh(&mut self) -> Result<()> {
        self.manifest.refresh().await
    }

    pub(crate) async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        crate::instrumentation::record_probe();
        self.manifest.probe_latest_incarnation().await
    }

    /// The lineage of the head this coordinator holds. Every operation that
    /// asks for history reads it here; the read of `__history` behind it is
    /// skipped while the history cache holds the head's parents.
    async fn lineage(&self) -> Result<Lineage> {
        self.manifest.commit_graph().lineage().await
    }

    /// The head this coordinator holds, as a merge captures it.
    pub(crate) async fn captured_lineage(&self) -> Result<CapturedLineage> {
        Ok(CapturedLineage {
            head: self.manifest.head_record().clone(),
            buffer: self.manifest.buffer().clone(),
            graph: self.manifest.commit_graph(),
        })
    }

    /// The commits of the branch, oldest first.
    pub(crate) async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        self.manifest.commit_graph().load_commits().await
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.manifest.list_graph_branches().await
    }

    pub(crate) async fn all_branches(&self) -> Result<Vec<String>> {
        self.manifest.list_graph_branches().await
    }

    /// The native Lance ref this coordinator's branch resolved to; `None` on
    /// main. Every table fork of the branch carries exactly this name.
    pub(crate) fn native_branch(&self) -> Option<&str> {
        self.manifest.native_branch()
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

    /// Delete the branch represented by an operation-local post-gate capture.
    ///
    /// The disposable coordinator may be bound to `name`. Its captured BranchIdentifier fences
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

    pub async fn snapshot_at_graph_manifest_version(
        &self,
        graph_manifest_version: u64,
    ) -> Result<Snapshot> {
        ManifestCoordinator::snapshot_at(
            self.root_uri(),
            self.current_branch(),
            graph_manifest_version,
        )
        .await
        .map(Snapshot::wrap)
    }

    pub async fn resolve_snapshot_id(&self, branch: &str) -> Result<SnapshotId> {
        let normalized = normalize_branch_name(branch)?;
        let opened;
        let coordinator = if normalized.as_deref() == self.current_branch()
            && self
                .probe_latest_incarnation()
                .await?
                .matches(&self.manifest_incarnation())
        {
            self
        } else {
            opened = match normalized.as_deref() {
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
            &opened
        };

        Ok(coordinator
            .effective_graph_head()
            .await?
            .map(SnapshotId::new)
            .unwrap_or_else(|| {
                SnapshotId::synthetic(
                    coordinator.current_branch(),
                    coordinator.version(),
                    coordinator.manifest_incarnation().e_tag.as_deref(),
                )
            }))
    }

    pub async fn resolve_target(&self, target: &ReadTarget) -> Result<ResolvedTarget> {
        match target {
            ReadTarget::Branch(branch) => {
                let normalized = normalize_branch_name(branch)?;
                let other = self.open_sibling(normalized.as_deref()).await?;
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
                let CommitState { commit, snapshot } = self.commit_state(snapshot_id).await?;
                Ok(ResolvedTarget {
                    requested: target.clone(),
                    branch: commit.graph_branch.clone(),
                    snapshot_id: snapshot_id.clone(),
                    graph_commit_id: Some(commit.graph_commit_id),
                    snapshot,
                })
            }
        }
    }

    /// The commit `snapshot_id` names and the graph as of it, from its record:
    /// held by this coordinator, else in `__history`, else in a live branch.
    /// Refused once the branch incarnation that wrote it is not live.
    async fn commit_state(&self, snapshot_id: &SnapshotId) -> Result<CommitState> {
        let session = self.manifest.control_session();
        let id = snapshot_id.as_str();
        if self.exact_graph_head().as_deref() == Some(id) {
            ManifestCoordinator::ensure_incarnation_live(
                self.root_uri(),
                &session,
                self.manifest.head(),
            )
            .await?;
            return Ok(CommitState {
                commit: self.head_commit(),
                snapshot: self.snapshot(),
            });
        }
        let record = match self.manifest.held_record(id) {
            Some(record) => Some(record),
            None => match self
                .history()
                .read_record(self.root_uri(), &session, id)
                .await?
            {
                Some(record) => Some(record),
                None => self.record_in_live_branches(id).await?,
            },
        };
        match record {
            Some(record) => settled_commit_state(self.root_uri(), &session, record).await,
            None => Err(commit_not_found(snapshot_id)),
        }
    }

    /// The record of a commit a read of `__history` did not find: held by the
    /// latest `__manifest` version of a live branch, the bound one first,
    /// else appended to `__history` since, which a second read finds.
    async fn record_in_live_branches(&self, id: &str) -> Result<Option<HistoryRecord>> {
        if let Some(record) = self.manifest.record_in_live_branches(id).await? {
            return Ok(Some(record));
        }
        self.history()
            .read_record(self.root_uri(), &self.manifest.control_session(), id)
            .await
    }

    /// The commit `snapshot_id` names: the head of the active branch or a
    /// commit its `__manifest` buffers, a settled commit of `__history`, or
    /// what [`Self::record_in_live_branches`] finds.
    pub async fn resolve_commit(&self, snapshot_id: &SnapshotId) -> Result<GraphCommit> {
        let id = snapshot_id.as_str();
        if let Some(commit) = self.manifest.held_commit(id) {
            return Ok(graph_commit_from_manifest_row(commit.clone()));
        }
        if let Some(commit) = self.history().get_commit(id) {
            return Ok(commit);
        }
        let session = self.manifest.control_session();
        if let Some(commit) = self
            .history()
            .read_commit(self.root_uri(), &session, id)
            .await?
        {
            return Ok(graph_commit_from_manifest_row(commit));
        }
        match self.record_in_live_branches(id).await? {
            Some(record) => Ok(graph_commit_from_manifest_row(record.commit)),
            None => Err(commit_not_found(snapshot_id)),
        }
    }

    /// The captured head, buffer and history cache only, with no read of
    /// `__history` and no branch fanout on a miss. `commit_id` names a held
    /// commit by its published ID or by its intent nonce.
    pub(crate) fn captured_commit(&self, commit_id: &str) -> Result<Option<GraphCommit>> {
        match self.manifest.held_commit_answering(commit_id)? {
            Some(held) => Ok(Some(graph_commit_from_manifest_row(held.clone()))),
            None => self.history().get_commit_answering(commit_id),
        }
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
        Ok(Some(SnapshotId::new(
            self.manifest.head().graph_commit_id.clone(),
        )))
    }

    /// Capture a change-feed cut by COLD-opening the requested branch. Used
    /// only when the requested branch differs from this handle's warm
    /// coordinator; the common same-branch poll uses [`Self::build_change_feed_cut`]
    /// on the already-warm coordinator (no manifest re-open).
    pub(crate) async fn capture_change_cut(
        &self,
        branch: Option<&str>,
    ) -> Result<crate::changes::feed::ChangeFeedCut> {
        self.open_sibling(branch)
            .await?
            .build_change_feed_cut()
            .await
    }

    /// Build a change-feed cut from THIS coordinator's current state: the
    /// head and its snapshot, the branch identifier captured with them, and
    /// the lineage of that head, so a concurrent commit cannot split the head
    /// from the chain it tops and old lineage cannot be paired with a
    /// replacement witness. The lineage reads `__history` only while the
    /// history cache lacks the head's parents.
    pub(crate) async fn build_change_feed_cut(
        &self,
    ) -> Result<crate::changes::feed::ChangeFeedCut> {
        let lineage = self.lineage().await?;
        let head = lineage.head().graph_commit_id.clone();
        // Main cannot be deleted/recreated, so a fixed witness suffices; a
        // named ref's Lance-native identifier changes on delete/recreate and
        // fences cursor ABA. This is the identifier captured with the head and
        // lineage above, never a fresh ref read that could name a replacement.
        let witness = match self.current_branch() {
            None => crate::changes::token::hashed_identity("branch:main"),
            Some(_) => {
                let identifier = self.branch_identifier().await?;
                let encoded = serde_json::to_string(&identifier).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to encode Lance branch identifier: {error}"
                    ))
                })?;
                crate::changes::token::hashed_identity(&encoded)
            }
        };
        // One walk finds genesis AND builds the forward first-parent child
        // index (chain member → its unique on-chain child), so a poll can walk
        // FORWARD from its cursor bounded by its commit ceiling instead of
        // cloning the whole unread backlog, and on-chain validation is O(1).
        let mut first_parent_children = std::collections::HashMap::new();
        let mut genesis = head.clone();
        loop {
            let commit = lineage.get_commit(&genesis).ok_or_else(|| {
                OmniError::manifest_internal(format!("lineage chain is missing commit '{genesis}'"))
            })?;
            match &commit.parent_commit_id {
                Some(parent) => {
                    first_parent_children.insert(parent.clone(), genesis.clone());
                    genesis = parent.clone();
                }
                None => break,
            }
        }
        Ok(crate::changes::feed::ChangeFeedCut {
            branch: self.bound_branch.clone(),
            head,
            head_record: self.manifest.head_record().clone(),
            head_snapshot: self.snapshot(),
            buffer: self.manifest.buffer().clone(),
            control_session: self.manifest.control_session(),
            witness,
            genesis,
            lineage,
            first_parent_children,
        })
    }

    #[cfg(test)]
    pub(crate) async fn commit_updates_with_actor(
        &mut self,
        updates: &[DatasetUpdate],
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
    /// `ManifestConflictDetails::PublishedDatasetVersionMismatch`.
    pub(crate) async fn commit_updates_with_actor_with_expected(
        &mut self,
        updates: &[DatasetUpdate],
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
    /// precondition. The intent's nonce and timestamp remain stable across
    /// publisher retries.
    pub(crate) async fn commit_changes_with_intent_and_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        intent: LineageIntent,
        precondition: &PublishPrecondition,
    ) -> Result<PublishedSnapshot> {
        fail(&GRAPH_PUBLISH_BEFORE_COMMIT_APPEND)?;
        let outcome = self
            .manifest
            .commit_changes_with_lineage_and_precondition(
                changes,
                expected_table_versions,
                Some(&intent),
                precondition,
            )
            .await?;
        fail(&GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT)?;
        let record = outcome.commit.ok_or_else(|| {
            OmniError::manifest_internal(
                "a publish with a lineage intent returned no graph commit record",
            )
        })?;
        let commit = graph_commit_from_manifest_row(record);
        Ok(PublishedSnapshot {
            graph_manifest_version: outcome.version,
            _snapshot_id: SnapshotId::new(commit.graph_commit_id.clone()),
            commit,
        })
    }

    /// Mint a [`LineageIntent`] for the next commit on the current branch: a
    /// fresh intent nonce (stable across CAS retries) and a timestamp. The returned
    /// publication carries the actual addressed graph commit ID.
    /// The parent is NOT chosen here — the publisher resolves it per attempt
    /// against the manifest it commits against.
    pub(crate) fn new_lineage_intent(
        &self,
        actor_id: Option<&str>,
        merged_parent: Option<BranchRecords>,
    ) -> Result<LineageIntent> {
        Self::new_lineage_intent_for_branch(
            self.current_branch(),
            actor_id,
            merged_parent,
            HistoryReleaseBytes::PRODUCTION,
        )
    }

    /// Mint identity for an explicitly captured branch without reading graph
    /// state. Parentage and branch authority are resolved by the publisher's
    /// precondition; minting an ID and timestamp does not need a coordinator.
    pub(crate) fn new_lineage_intent_for_branch(
        branch: Option<&str>,
        actor_id: Option<&str>,
        merged_parent: Option<BranchRecords>,
        history_release_bytes: HistoryReleaseBytes,
    ) -> Result<LineageIntent> {
        let branch = normalize_branch_name(branch.unwrap_or("main"))?;
        Ok(LineageIntent {
            graph_commit_id: crate::dst_ids::new_ulid().to_string(),
            branch,
            actor_id: actor_id.map(str::to_string),
            merged_parent,
            created_at: crate::db::now_micros()?,
            history_release_bytes,
        })
    }

    pub(crate) async fn list_commits(&self) -> Result<Vec<GraphCommit>> {
        self.load_commits().await
    }
}

/// The state of a settled commit, from the `table` rows of its record.
async fn settled_commit_state(
    root_uri: &str,
    control_session: &Arc<lance::session::Session>,
    record: HistoryRecord,
) -> Result<CommitState> {
    ManifestCoordinator::ensure_incarnation_live(root_uri, control_session, &record.commit).await?;
    let snapshot = Snapshot::wrap(record.snapshot(root_uri)?);
    Ok(CommitState {
        commit: graph_commit_from_manifest_row(record.commit),
        snapshot,
    })
}

fn commit_not_found(snapshot_id: &SnapshotId) -> OmniError {
    OmniError::manifest_not_found(format!("commit '{}' not found", snapshot_id))
}

/// Wrap each `DatasetUpdate` as a `ManifestChange::Update` for the publisher.
fn updates_to_changes(updates: &[DatasetUpdate]) -> Vec<ManifestChange> {
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
    crate::branch_names::ensure_logical_branch_name(branch)?;
    Ok(Some(branch.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cold_open_refresh_uses_content_only_from_the_held_image() {
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        const CAPTURE_ERROR: &str = "captured contract content error";
        for advance in [false, true] {
            for captured_error in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().to_str().unwrap();
                let _owner = crate::db::Omnigraph::init(root, "node Person { name: String }")
                    .await
                    .unwrap();
                let session = crate::lance_access::control_session();
                let (manifest, _lineage, captured) =
                    ManifestCoordinator::open_with_lineage_and_contract(root, None, &session)
                        .await
                        .unwrap();
                let mut expected = captured.unwrap();
                let old_head = expected.head.clone();
                let old_version = manifest.version();
                let mut reader = GraphCoordinator {
                    root_uri: normalize_root_uri(root).unwrap(),
                    storage: crate::storage::storage_for_uri(root).unwrap(),
                    manifest,
                    bound_branch: None,
                };
                let captured = if captured_error {
                    Err(OmniError::manifest_internal(CAPTURE_ERROR))
                } else {
                    Ok(expected.clone())
                };
                let expected_version = if advance {
                    expected.source = format!("\n{}\n", expected.source);
                    expected.ir = format!("\n{}\n", expected.ir);
                    assert_eq!(expected.head, old_head);
                    let mut writer = ManifestCoordinator::open_with_session(root, &session)
                        .await
                        .unwrap();
                    let version = writer
                        .commit_changes(&[ManifestChange::SchemaContract(expected.clone())])
                        .await
                        .unwrap();
                    assert!(version > old_version);
                    version
                } else {
                    old_version
                };
                let probes = crate::instrumentation::QueryIoProbes::default();
                let scans = Arc::clone(&probes.manifest_scan_count);
                let result = crate::instrumentation::with_query_io_probes(
                    probes,
                    reader.refresh_contract_capture(captured),
                )
                .await;
                assert_eq!(reader.version(), expected_version);
                assert_eq!(
                    reader.manifest.snapshot().schema_contract(),
                    Some(&old_head)
                );
                if captured_error && !advance {
                    match result.unwrap_err() {
                        OmniError::Manifest(error) => {
                            assert_eq!(error.kind, crate::error::ManifestErrorKind::Internal);
                            assert_eq!(error.message, CAPTURE_ERROR);
                            assert!(error.details.is_none());
                            assert!(!error.publication_in_doubt);
                        }
                        error => panic!("unexpected capture error: {error:?}"),
                    }
                } else {
                    assert_eq!(result.unwrap(), expected);
                }
                let scans = scans.load(std::sync::atomic::Ordering::Relaxed);
                if advance {
                    assert!(scans > 0, "the replacement must read its pinned content");
                } else {
                    assert_eq!(scans, 0, "unchanged success/error must reuse the capture");
                }
            }
        }
    }

    #[tokio::test]
    async fn prepared_cold_open_refreshes_content_published_after_admission() {
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let _owner = crate::db::Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap();
        let session = crate::lance_access::control_session();
        let prepared = ManifestCoordinator::prepare_open_with_contract(root, &session)
            .await
            .unwrap();
        let mut writer = ManifestCoordinator::open_with_session(root, &session)
            .await
            .unwrap();
        let mut expected = writer.read_schema_contract().await.unwrap();
        let old_head = expected.head.clone();
        expected.source = format!("\n{}\n", expected.source);
        expected.ir = format!("\n{}\n", expected.ir);
        let version = writer
            .commit_changes(&[ManifestChange::SchemaContract(expected.clone())])
            .await
            .unwrap();
        let (reader, contract) = GraphCoordinator::open_with_contract(
            root,
            crate::storage::storage_for_uri(root).unwrap(),
            prepared,
        )
        .await
        .unwrap();
        assert_eq!(reader.version(), version);
        assert_eq!(contract.head, old_head);
        assert_eq!(contract, expected);
    }

    /// A read by commit id of a commit made under an earlier schema takes that
    /// schema's text from `__history/schemas/`, with no open or scan of
    /// `__manifest`, so it survives the pruning of old `__manifest` versions.
    #[tokio::test]
    async fn commit_under_an_earlier_schema_reads_its_contract_from_the_archive() {
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = crate::db::Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap();
        let session = crate::lance_access::control_session();
        let open = || async {
            let storage = crate::storage::storage_for_uri(root).unwrap();
            GraphCoordinator::open_with_session(root, storage, &session)
                .await
                .unwrap()
        };
        let at_genesis = open().await;
        let genesis = SnapshotId::new(at_genesis.exact_graph_head().unwrap());
        let genesis_contract = at_genesis.read_schema_contract().await.unwrap();
        db.apply_schema("node Person {\n    name: String\n    nickname: String?\n}\n")
            .await
            .unwrap();

        let reader = open().await;
        let applied_contract = reader.read_schema_contract().await.unwrap();
        assert_ne!(applied_contract, genesis_contract);
        let resolved = reader
            .resolve_target(&ReadTarget::Snapshot(genesis.clone()))
            .await
            .unwrap();
        let probes = crate::instrumentation::QueryIoProbes::default();
        let opens = Arc::clone(&probes.internal_open_count);
        let scans = Arc::clone(&probes.manifest_scan_count);
        let contract = crate::instrumentation::with_query_io_probes(
            probes,
            resolved.snapshot.read_schema_contract(root),
        )
        .await
        .unwrap();
        assert_eq!(contract, genesis_contract);
        assert_eq!(
            (
                opens.load(std::sync::atomic::Ordering::Relaxed),
                scans.load(std::sync::atomic::Ordering::Relaxed)
            ),
            (0, 0),
            "the contract of an earlier commit is read without `__manifest`"
        );

        let digest = omnigraph_catalog::history::schema_content_hash(&genesis_contract).unwrap();
        std::fs::remove_file(
            dir.path()
                .join(format!("__history/schemas/{digest}.schema")),
        )
        .unwrap();
        let error = resolved
            .snapshot
            .read_schema_contract(root)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is missing"), "{error}");
    }

    /// `captured_commit` names a commit by its intent nonce after the commit
    /// left the buffer for `__history`, which holds it under its `hb1` id.
    #[tokio::test]
    async fn captured_commit_answers_the_nonce_of_a_released_commit() {
        use omnigraph_core::graph_commit_id::{intent_nonce, parse_history_block_id};
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = crate::db::Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap();
        let settings = crate::settings::SessionSettings::default()
            .with("history_release_bytes", "2048")
            .unwrap();
        let lowered = crate::Session::from_defaults(Arc::new(db), settings);
        let slot_of = |id: &str| parse_history_block_id(id).unwrap().unwrap().slot;
        let mut published: Vec<String> = Vec::new();
        for load in 0..64 {
            let line = format!(r#"{{"type": "Person", "data": {{"name": "P{load}"}}}}"#);
            let receipt = lowered
                .load_as_with_receipt("main", None, &line, crate::loader::LoadMode::Append, None)
                .await
                .unwrap();
            published.push(receipt.commit.graph_commit_id);
            if published.len() > 2 && slot_of(&published[published.len() - 2]) == 0 {
                break;
            }
        }
        assert!(
            published.len() > 2 && slot_of(&published[published.len() - 2]) == 0,
            "64 loads under 2048 bytes released no block: {published:?}"
        );
        let released = &published[0];
        let nonce = intent_nonce(released).unwrap();
        let storage = crate::storage::storage_for_uri(root).unwrap();
        let session = crate::lance_access::control_session();
        let coordinator = GraphCoordinator::open_with_session(root, storage, &session)
            .await
            .unwrap();
        assert!(
            coordinator
                .manifest
                .held_commit_answering(&nonce)
                .unwrap()
                .is_none(),
            "the first load's commit left the buffer"
        );
        coordinator.list_commits().await.unwrap();
        let found = coordinator
            .captured_commit(&nonce)
            .unwrap()
            .expect("a released commit answers its intent nonce");
        assert_eq!(&found.graph_commit_id, released);
    }

    fn commit(
        id: &str,
        parent_commit_id: Option<&str>,
        merged_parent_commit_id: Option<&str>,
    ) -> GraphCommit {
        GraphCommit {
            graph_commit_id: id.to_string(),
            graph_branch: None,
            graph_manifest_version: 1,
            generation: 0,
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

    #[tokio::test]
    async fn prepared_cold_open_refuses_a_legacy_stamp_published_after_admission() {
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let _owner = crate::db::Omnigraph::init(root, "node Person { name: String }")
            .await
            .unwrap();
        let session = crate::lance_access::control_session();
        let prepared = ManifestCoordinator::prepare_open_with_contract(root, &session)
            .await
            .unwrap();
        let mut dataset =
            crate::db::manifest::layout::open_manifest_dataset_with_session(root, None, &session)
                .await
                .unwrap();
        let captured_version = dataset.version().version;
        crate::db::manifest::migrations::set_stamp_for_test(&mut dataset, 12)
            .await
            .unwrap();
        assert!(dataset.version().version > captured_version);
        assert_eq!(
            crate::db::manifest::migrations::read_stamp(&dataset),
            Some(12)
        );
        let probes = crate::instrumentation::QueryIoProbes::default();
        let opens = Arc::clone(&probes.internal_open_count);
        let result = crate::instrumentation::with_query_io_probes(
            probes,
            GraphCoordinator::open_with_contract(
                root,
                crate::storage::storage_for_uri(root).unwrap(),
                prepared,
            ),
        )
        .await;
        assert_eq!(opens.load(std::sync::atomic::Ordering::Relaxed), 1);
        let error = result
            .err()
            .expect("final refreshed image must still be served format");
        let OmniError::Manifest(error) = error else {
            panic!("expected typed format refusal: {error:?}");
        };
        assert_eq!(error.kind, crate::error::ManifestErrorKind::BadRequest);
        assert!(
            error.message.contains("internal schema v12"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains(&format!(
                "reads only v{} to v{}",
                crate::db::manifest::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
                crate::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            )),
            "{}",
            error.message
        );
        assert!(error.details.is_none());
        assert!(!error.publication_in_doubt);
    }
}
