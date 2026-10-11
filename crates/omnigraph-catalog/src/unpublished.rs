//! Unpublished graph commits: one write's complete result, readable by its
//! graph commit id and invisible to every branch read, published or dropped
//! later (RFC "Unpublished graph commits").
//!
//! The proposal is a detached version `D` of the target branch's `__manifest`
//! holding the candidate rows: the result `table` rows, a head row whose id is
//! the candidate's graph commit id `C` and whose clock is `M + 1` over the
//! captured linear version `M`, an empty buffer and the captured schema
//! contract. Its transaction carries [`UnpublishedCommitProperty`], the exact
//! recipe a later publication replays through the ordinary publisher. The one
//! addressability authority is the root tag `unpublished_<C>`, created
//! put-if-absent at `(native branch, D)`.

use std::collections::HashMap;
use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::{BranchIdentifier, Ref, TagContents};
use lance::dataset::transaction::Transaction;
pub use omnigraph_core::branch_names::{
    UNPUBLISHED_TAG_PREFIX, is_unpublished_tag, unpublished_tag_commit_id,
};

use crate::commit_graph::{CommitGraph, GraphCommit, HistoryCache};
use crate::error::{OmniError, Result};
use crate::history::HistoryRecord;
use crate::migrations::{read_stamp, require_unpublished_commits};
use crate::publisher::{
    CandidateOutcome, GraphHeadExpectation, LineageIntent, PublishPrecondition,
};
use crate::retention::{ManifestTagInventory, release_merge_input_tags};
use crate::seams::{decide_seam, fail};
use crate::state::{
    BranchRecords, GraphLineageRow, TablePin, TableRow, TableState, read_manifest_rows_projected,
};
use crate::{
    CommitOutcome, DatasetUpdate, ExpectedTableVersions, HistoryReleaseBytes, ManifestChange,
    ManifestCoordinator, NativeRefPin, TableIdentity, TableRename, TableTombstone,
    TableVersionExpectation, open_manifest_dataset_native_with_session,
};

/// The transaction property of a candidate `__manifest` version.
pub const UNPUBLISHED_COMMIT_PROPERTY: &str = "omnigraph.unpublished_graph_commit";

/// The layout version of [`UnpublishedCommitProperty`]; any other is refused.
pub const UNPUBLISHED_PROPERTY_VERSION: u32 = 1;

pub fn unpublished_tag_name(graph_commit_id: &str) -> String {
    format!("{UNPUBLISHED_TAG_PREFIX}{graph_commit_id}")
}

/// One table version the candidate was built against, as the publisher checks it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedTableVersionRecord {
    pub identity: TableIdentity,
    pub table_key: String,
    pub table_version: u64,
    /// `None` is [`NativeRefPin::Unchecked`]; `Some(ref)` is `Exact(ref)`.
    pub native_ref: Option<Option<String>>,
}

impl ExpectedTableVersionRecord {
    fn from_expectation(identity: TableIdentity, expectation: &TableVersionExpectation) -> Self {
        Self {
            identity,
            table_key: expectation.table_key.clone(),
            table_version: expectation.table_version,
            native_ref: match &expectation.native_ref {
                NativeRefPin::Unchecked => None,
                NativeRefPin::Exact(native) => Some(native.clone()),
            },
        }
    }

    fn expectation(&self) -> TableVersionExpectation {
        TableVersionExpectation {
            table_key: self.table_key.clone(),
            table_version: self.table_version,
            native_ref: match &self.native_ref {
                None => NativeRefPin::Unchecked,
                Some(native) => NativeRefPin::Exact(native.clone()),
            },
        }
    }
}

/// The exact lineage inputs a merge candidate holds until it is terminal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnpublishedMergeInputs {
    pub source_branch: String,
    /// The source's native ref at capture, `None` for main.
    pub source_native: Option<String>,
    /// The source `__manifest` version the held records are read from.
    pub source_manifest_version: u64,
    pub source_head_commit_id: String,
    pub merge_base_commit_id: String,
    /// The merge-input tags that protect the source, target and base
    /// versions; the collector expires them with the proposal.
    pub input_tags: Vec<String>,
}

/// The transaction property of a candidate version: the recipe its
/// publication replays and the authority it was captured under.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnpublishedCommitProperty {
    pub version: u32,
    pub graph_commit_id: String,
    pub intent_nonce: String,
    pub graph_branch: Option<String>,
    pub native_branch: Option<String>,
    pub branch_identifier: BranchIdentifier,
    pub parent_commit_id: String,
    /// The captured authority held no exact head: the target had not written
    /// since its creation, so `parent_commit_id` is the head it inherited and
    /// its publication expects an absent head.
    #[serde(default)]
    pub inherited_head: bool,
    pub captured_manifest_version: u64,
    pub actor_id: Option<String>,
    pub created_at: i64,
    pub history_release_bytes: usize,
    pub expected_table_versions: Vec<ExpectedTableVersionRecord>,
    pub merge: Option<UnpublishedMergeInputs>,
}

impl UnpublishedCommitProperty {
    pub(crate) fn new(
        head: &GraphLineageRow,
        intent: &LineageIntent,
        authority: &GraphHeadExpectation,
        captured_manifest_version: u64,
        expected_table_versions: &ExpectedTableVersions,
        merge: Option<UnpublishedMergeInputs>,
    ) -> Result<Self> {
        let parent_commit_id = head.parent_commit_id.clone().ok_or_else(|| {
            OmniError::manifest_internal("a candidate head records no parent commit")
        })?;
        let mut expected: Vec<(TableIdentity, &TableVersionExpectation)> = expected_table_versions
            .iter()
            .map(|(id, e)| (*id, e))
            .collect();
        expected.sort_by_key(|(identity, _)| *identity);
        Ok(Self {
            version: UNPUBLISHED_PROPERTY_VERSION,
            graph_commit_id: head.graph_commit_id.clone(),
            intent_nonce: intent.graph_commit_id.clone(),
            graph_branch: head.graph_branch.clone(),
            native_branch: head.native_branch.clone(),
            branch_identifier: authority.branch_identifier.clone(),
            parent_commit_id,
            inherited_head: authority.head_commit_id.is_none(),
            captured_manifest_version,
            actor_id: head.actor_id.clone(),
            created_at: head.created_at,
            history_release_bytes: intent.history_release_bytes.0,
            expected_table_versions: expected
                .into_iter()
                .map(|(identity, expectation)| {
                    ExpectedTableVersionRecord::from_expectation(identity, expectation)
                })
                .collect(),
            merge,
        })
    }

    pub(crate) fn encode(&self) -> Result<HashMap<String, String>> {
        let value = serde_json::to_string(self).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode the unpublished commit property: {error}"
            ))
        })?;
        Ok(HashMap::from([(
            UNPUBLISHED_COMMIT_PROPERTY.to_string(),
            value,
        )]))
    }

    pub(crate) fn decode(transaction: &Transaction) -> Result<Option<Self>> {
        let Some(value) = transaction
            .transaction_properties
            .as_ref()
            .and_then(|properties| properties.get(UNPUBLISHED_COMMIT_PROPERTY))
        else {
            return Ok(None);
        };
        let property: Self = serde_json::from_str(value).map_err(|error| {
            OmniError::manifest_internal(format!(
                "the unpublished commit property does not decode: {error}"
            ))
        })?;
        if property.version != UNPUBLISHED_PROPERTY_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unpublished commit property version {} is not the {} this binary reads",
                property.version, UNPUBLISHED_PROPERTY_VERSION
            )));
        }
        Ok(Some(property))
    }

    pub fn tag_name(&self) -> String {
        unpublished_tag_name(&self.graph_commit_id)
    }

    pub fn expected_table_versions(&self) -> ExpectedTableVersions {
        self.expected_table_versions
            .iter()
            .map(|record| (record.identity, record.expectation()))
            .collect()
    }

    /// The authority the candidate was captured under: its publication requires it.
    pub fn authority(&self) -> GraphHeadExpectation {
        GraphHeadExpectation::new(
            self.graph_branch.as_deref(),
            self.branch_identifier.clone(),
            (!self.inherited_head).then(|| self.parent_commit_id.clone()),
        )
    }

    /// Refuse a candidate whose rows, tag and property disagree.
    fn validate(
        &self,
        head: &GraphLineageRow,
        tag: &TagContents,
        detached_version: u64,
    ) -> Result<()> {
        let mismatch = |what: &str| {
            OmniError::manifest_internal(format!(
                "unpublished commit '{}': its {what} disagrees with its transaction property",
                self.graph_commit_id
            ))
        };
        if head.graph_commit_id != self.graph_commit_id {
            return Err(mismatch("head commit id"));
        }
        if head.parent_commit_id.as_deref() != Some(self.parent_commit_id.as_str()) {
            return Err(mismatch("parent commit id"));
        }
        if head.graph_manifest_version != self.captured_manifest_version + 1 {
            return Err(mismatch("row clock"));
        }
        if head.graph_branch != self.graph_branch || head.native_branch != self.native_branch {
            return Err(mismatch("branch"));
        }
        if head.actor_id != self.actor_id || head.created_at != self.created_at {
            return Err(mismatch("author"));
        }
        if tag.branch != self.native_branch || tag.version != detached_version {
            return Err(mismatch("tag"));
        }
        if !self.graph_commit_id.starts_with("hb1.")
            || omnigraph_core::graph_commit_id::intent_nonce(&self.graph_commit_id)?
                != self.intent_nonce
        {
            return Err(mismatch("intent nonce"));
        }
        Ok(())
    }
}

/// Why a proposal can no longer be published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnpublishedExpiry {
    /// The target's head provably moved past the captured parent.
    HeadAdvanced { head: String },
    /// The target incarnation is retired or replaced.
    IncarnationRetired,
    /// The target's lineage holds the commit itself: it was published and
    /// its tag outlived the release.
    Published,
}

/// The collector's three-way lifetime verdict on a proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnpublishedLifetime {
    /// Same live incarnation, same head: retained and publishable.
    Live,
    /// Cannot publish; its tag is eligible for removal.
    Expired(UnpublishedExpiry),
    /// Incomplete evidence: retained, every operation fails closed.
    Undecidable(String),
}

/// A proposal as read from its tag, its detached version and its property.
#[derive(Debug, Clone)]
pub struct UnpublishedCommit {
    pub property: UnpublishedCommitProperty,
    /// The candidate head with the result `table` rows.
    pub record: HistoryRecord,
    /// The detached `__manifest` version holding the rows: the physical
    /// coordinate a receipt reports as `graph_manifest_version`.
    pub detached_version: u64,
    pub lifetime: UnpublishedLifetime,
}

impl UnpublishedCommit {
    /// The proposal a reserved `unpublished_` tag of `inventory` names,
    /// judged against the live target.
    pub async fn from_inventory(
        inventory: &ManifestTagInventory,
        root_uri: &str,
        name: &str,
    ) -> Result<Self> {
        let tag = inventory.tags.get(name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "native tag '{name}' is not in the captured inventory"
            ))
        })?;
        ManifestCoordinator::unpublished_from_tag(root_uri, inventory.dataset(), name, tag).await
    }

    pub fn tag_name(&self) -> String {
        self.property.tag_name()
    }

    pub fn graph_commit(&self) -> GraphCommit {
        let mut commit =
            crate::commit_graph::graph_commit_from_manifest_row(self.record.commit.clone());
        commit.graph_manifest_version = self.detached_version;
        commit.published = false;
        commit
    }

    /// The snapshot of the candidate, labelled with its physical coordinate.
    pub fn snapshot(&self, root_uri: &str) -> Result<crate::Snapshot> {
        let mut snapshot = self.record.snapshot_as(
            root_uri,
            self.detached_version,
            self.property.native_branch.as_deref(),
        )?;
        snapshot.graph_branch = self.property.graph_branch.clone();
        Ok(snapshot)
    }

    pub fn is_live(&self) -> bool {
        self.lifetime == UnpublishedLifetime::Live
    }

    /// Refuse a new read of a proposal that is no longer live: an expired
    /// proposal is not found, and an undecidable proposal fails closed.
    pub fn require_readable(&self) -> Result<()> {
        match &self.lifetime {
            UnpublishedLifetime::Live => Ok(()),
            UnpublishedLifetime::Expired(_) => Err(OmniError::manifest_not_found(format!(
                "unpublished commit '{}' not found: it expired and awaits cleanup",
                self.property.graph_commit_id
            ))),
            UnpublishedLifetime::Undecidable(_) => self.require_publishable(),
        }
    }

    /// Refuse a drop the lifetime rule does not admit: a published commit
    /// is never dropped, and an undecidable proposal fails closed.
    pub fn require_droppable(&self) -> Result<()> {
        match &self.lifetime {
            UnpublishedLifetime::Live | UnpublishedLifetime::Expired(_)
                if self.lifetime != UnpublishedLifetime::Expired(UnpublishedExpiry::Published) =>
            {
                Ok(())
            }
            _ => self.require_publishable(),
        }
    }

    /// Refuse an operation the lifetime rule does not admit.
    pub fn require_publishable(&self) -> Result<()> {
        match &self.lifetime {
            UnpublishedLifetime::Live => Ok(()),
            UnpublishedLifetime::Expired(UnpublishedExpiry::Published) => {
                Err(OmniError::manifest_conflict(format!(
                    "commit '{}' is published; its proposal tag awaits cleanup",
                    self.property.graph_commit_id
                )))
            }
            UnpublishedLifetime::Expired(UnpublishedExpiry::HeadAdvanced { head }) => {
                Err(OmniError::precondition_failed(
                    self.property.graph_branch.as_deref().unwrap_or("main"),
                    self.property.parent_commit_id.clone(),
                    Some(head.clone()),
                ))
            }
            UnpublishedLifetime::Expired(UnpublishedExpiry::IncarnationRetired) => {
                Err(OmniError::manifest_not_found(format!(
                    "unpublished commit '{}' targets a retired incarnation of branch '{}'",
                    self.property.graph_commit_id,
                    self.property.graph_branch.as_deref().unwrap_or("main")
                )))
            }
            UnpublishedLifetime::Undecidable(reason) => Err(OmniError::manifest(format!(
                "unpublished commit '{}' cannot be judged against its target: {reason}",
                self.property.graph_commit_id
            ))),
        }
    }
}

/// Delete a reserved proposal tag; absence is the completed outcome.
pub async fn release_unpublished_tag(dataset: &Dataset, name: &str) -> Result<()> {
    if !is_unpublished_tag(name) {
        return Err(OmniError::manifest_internal(
            "refusing to release a native tag that is not a proposal tag",
        ));
    }
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    if let Err(error) = store
        .delete(&lance::dataset::refs::tag_path(&root.path, name))
        .await
        && !error.is_not_found()
    {
        return Err(OmniError::storage(error));
    }
    Ok(())
}

fn pins_equal(a: &TablePin, b: &TablePin) -> bool {
    a.table_version == b.table_version
        && a.table_branch == b.table_branch
        && a.row_count == b.row_count
        && a.metadata == b.metadata
}

/// The changes that turn the rows `current` into the rows `candidate`: what
/// the candidate's publication replays. The row clocks are not compared.
pub(crate) fn candidate_changes(
    current: &[TableRow],
    candidate: &[TableRow],
) -> Result<Vec<ManifestChange>> {
    let current: HashMap<TableIdentity, &TableRow> = current
        .iter()
        .map(|table| (table.registration.identity, table))
        .collect();
    let mut changes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for table in candidate {
        let identity = table.registration.identity;
        seen.insert(identity);
        let before = current.get(&identity).copied();
        match before {
            None => changes.push(ManifestChange::RegisterTable(table.registration.clone())),
            Some(before) if before.registration.table_key != table.registration.table_key => {
                changes.push(ManifestChange::RenameTable(TableRename {
                    identity,
                    expected_table_key: before.registration.table_key.clone(),
                    table_key: table.registration.table_key.clone(),
                    table_path: table.registration.table_path.clone(),
                }));
            }
            Some(_) => {}
        }
        match (&table.state, before.map(|before| &before.state)) {
            (TableState::Pinned(pin), Some(TableState::Pinned(previous)))
                if pins_equal(pin, previous) => {}
            (TableState::Pinned(pin), _) => changes.push(ManifestChange::Update(DatasetUpdate {
                identity,
                type_key: table.registration.table_key.clone(),
                published_dataset_version: pin.table_version,
                native_dataset_branch: pin.table_branch.clone(),
                entity_count: pin.row_count,
                version_metadata: pin.metadata.clone(),
            })),
            (TableState::Dropped { .. }, Some(TableState::Dropped { .. })) => {}
            (TableState::Dropped { sealed_version, .. }, _) => {
                changes.push(ManifestChange::Tombstone(TableTombstone {
                    identity,
                    table_key: table.registration.table_key.clone(),
                    tombstone_version: *sealed_version,
                }));
            }
            (TableState::Registered, _) => {}
        }
    }
    if let Some(missing) = current.keys().find(|identity| !seen.contains(identity)) {
        return Err(OmniError::manifest_internal(format!(
            "unpublished commit rows lack the live table identity {missing}"
        )));
    }
    Ok(changes)
}

/// The lifetime of a proposal captured under `property`, judged against the
/// live target: one branch listing and one scan of the target's head.
async fn judge_lifetime(
    root_uri: &str,
    main: &Dataset,
    property: &UnpublishedCommitProperty,
) -> Result<UnpublishedLifetime> {
    let target = match (&property.graph_branch, &property.native_branch) {
        (None, None) => main.clone(),
        (Some(logical), Some(native)) => {
            match crate::branch_control::resolve_live_native_branch(main, logical).await? {
                Some(live) if &live == native => match main.checkout_branch(native).await {
                    Ok(dataset) => dataset,
                    Err(lance::Error::RefNotFound { .. }) => {
                        return Ok(UnpublishedLifetime::Expired(
                            UnpublishedExpiry::IncarnationRetired,
                        ));
                    }
                    Err(error) => return Err(OmniError::storage(error)),
                },
                _ => {
                    return Ok(UnpublishedLifetime::Expired(
                        UnpublishedExpiry::IncarnationRetired,
                    ));
                }
            }
        }
        _ => {
            return Err(OmniError::manifest_internal(format!(
                "unpublished commit '{}' names a branch without its native ref",
                property.graph_commit_id
            )));
        }
    };
    let rows = read_manifest_rows_projected(&target).await?;
    let head = rows.head.graph_commit_id.clone();
    if head == property.parent_commit_id {
        return Ok(UnpublishedLifetime::Live);
    }
    if head == property.graph_commit_id || rows.buffer.get(&property.graph_commit_id).is_some() {
        return Ok(UnpublishedLifetime::Expired(UnpublishedExpiry::Published));
    }
    if rows.buffer.get(&property.parent_commit_id).is_some() {
        return Ok(UnpublishedLifetime::Expired(
            UnpublishedExpiry::HeadAdvanced { head },
        ));
    }
    let graph = CommitGraph::from_head(
        root_uri,
        target.session(),
        rows.head,
        rows.buffer.commits(),
        HistoryCache::default(),
    );
    match graph.lineage().await {
        Ok(lineage) if lineage.get_commit(&property.graph_commit_id).is_some() => {
            Ok(UnpublishedLifetime::Expired(UnpublishedExpiry::Published))
        }
        Ok(lineage) if lineage.get_commit(&property.parent_commit_id).is_some() => Ok(
            UnpublishedLifetime::Expired(UnpublishedExpiry::HeadAdvanced { head }),
        ),
        Ok(_) => Ok(UnpublishedLifetime::Undecidable(format!(
            "the target head '{head}' does not descend from the captured parent '{}'",
            property.parent_commit_id
        ))),
        Err(error) => Ok(UnpublishedLifetime::Undecidable(format!(
            "the target lineage below '{head}' is unavailable: {error}"
        ))),
    }
}

decide_seam! {
    /// After `commit drop` released the input holds of a merge proposal and
    /// before it deletes the proposal tag: the proposal is still addressable
    /// and its property still names the holds, so a retried drop releases
    /// them again (an absent tag is a completed release) and then deletes.
    pub static DROP_POST_INPUTS_PRE_TAG = ("drop.post_inputs_pre_tag", AnyWrite, [Fail]);
}

impl ManifestCoordinator {
    /// Write `changes` as an unpublished commit on this coordinator's branch
    /// under the exact `authority`. The live head, and this coordinator's
    /// view of it, are unchanged: nothing is installed.
    pub async fn propose_changes(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        intent: &LineageIntent,
        authority: &GraphHeadExpectation,
        merge: Option<UnpublishedMergeInputs>,
    ) -> Result<CandidateOutcome> {
        self.publisher
            .write_candidate(changes, expected_table_versions, intent, authority, merge)
            .await
    }

    /// The proposal `graph_commit_id` names, or `None` when no tag names it.
    /// One tag read, one checkout of the detached version, one scan and one
    /// transaction read, then the lifetime rule against the live target.
    pub async fn unpublished_commit(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        graph_commit_id: &str,
    ) -> Result<Option<UnpublishedCommit>> {
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        let name = unpublished_tag_name(graph_commit_id);
        let tag = match main.tags().get(&name).await {
            Ok(tag) => tag,
            Err(lance::Error::RefNotFound { .. }) => return Ok(None),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(OmniError::storage(error)),
        };
        Self::unpublished_from_tag(root_uri, &main, &name, &tag)
            .await
            .map(Some)
    }

    pub(crate) async fn unpublished_from_tag(
        root_uri: &str,
        main: &Dataset,
        name: &str,
        tag: &TagContents,
    ) -> Result<UnpublishedCommit> {
        let expected_id = unpublished_tag_commit_id(name)?.ok_or_else(|| {
            OmniError::manifest_internal(format!("native tag '{name}' is not a proposal tag"))
        })?;
        let detached = main
            .checkout_version(Ref::Version(tag.branch.clone(), Some(tag.version)))
            .await
            .map_err(OmniError::storage)?;
        let stamp = read_stamp(&detached).ok_or_else(|| {
            OmniError::manifest_internal(format!("the version tag '{name}' names carries no stamp"))
        })?;
        require_unpublished_commits(stamp)?;
        let transaction = detached
            .read_transaction()
            .await
            .map_err(OmniError::storage)?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the version tag '{name}' names records no transaction"
                ))
            })?;
        let property = UnpublishedCommitProperty::decode(&transaction)?.ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "the version tag '{name}' names carries no unpublished commit property"
            ))
        })?;
        if property.graph_commit_id != expected_id {
            return Err(OmniError::manifest_internal(format!(
                "native tag '{name}' names a version whose property records commit '{}'",
                property.graph_commit_id
            )));
        }
        let rows = read_manifest_rows_projected(&detached).await?;
        property.validate(&rows.head, tag, tag.version)?;
        if !rows.buffer.commits().is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "unpublished commit '{expected_id}' buffers commits; a candidate buffers none"
            )));
        }
        let lifetime = judge_lifetime(root_uri, main, &property).await?;
        Ok(UnpublishedCommit {
            property,
            record: HistoryRecord {
                commit: rows.head,
                tables: rows.tables,
            },
            detached_version: tag.version,
            lifetime,
        })
    }

    /// Every live proposal whose target is `branch` (`None` = main), newest
    /// first by `(created_at, graph_commit_id)`. One tag listing, then one
    /// read per proposal tag: the cost grows with the retained proposals.
    pub async fn list_unpublished(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        branch: Option<&str>,
    ) -> Result<Vec<UnpublishedCommit>> {
        let branch = branch.filter(|branch| *branch != "main");
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        let tags = main.tags().list().await.map_err(OmniError::storage)?;
        let mut names: Vec<(String, TagContents)> = tags
            .into_iter()
            .filter(|(name, _)| is_unpublished_tag(name))
            .collect();
        names.sort_by(|a, b| a.0.cmp(&b.0));
        let mut proposals = Vec::new();
        for (name, tag) in &names {
            let proposal = Self::unpublished_from_tag(root_uri, &main, name, tag).await?;
            if proposal.property.graph_branch.as_deref() == branch && proposal.is_live() {
                proposals.push(proposal);
            }
        }
        proposals.sort_by(|a, b| {
            (b.property.created_at, &b.property.graph_commit_id)
                .cmp(&(a.property.created_at, &a.property.graph_commit_id))
        });
        Ok(proposals)
    }

    /// Publish `candidate` on this coordinator's branch, the candidate's
    /// recorded target: the ordinary publish of the changes its rows record,
    /// under the captured authority, required to allocate the candidate's own
    /// id. The result tags are released afterwards as retryable cleanup; a
    /// release failure never undoes the publication.
    pub async fn publish_unpublished(
        &mut self,
        candidate: &UnpublishedCommit,
    ) -> Result<CommitOutcome> {
        let property = &candidate.property;
        if self.active_branch.as_deref() != property.graph_branch.as_deref() {
            return Err(OmniError::manifest_internal(format!(
                "unpublished commit '{}' targets branch '{}' and the coordinator is bound to '{}'",
                property.graph_commit_id,
                property.graph_branch.as_deref().unwrap_or("main"),
                self.active_branch.as_deref().unwrap_or("main")
            )));
        }
        candidate.require_publishable()?;
        if self.head.commit.graph_commit_id != property.parent_commit_id {
            return Err(OmniError::manifest_read_set_changed(
                format!(
                    "graph_head:{}",
                    property.graph_branch.as_deref().unwrap_or("main")
                ),
                Some(property.parent_commit_id.clone()),
                Some(self.head.commit.graph_commit_id.clone()),
            ));
        }
        let changes = candidate_changes(&self.head.tables, &candidate.record.tables)?;
        let merged_parent = match &property.merge {
            None => None,
            Some(merge) => Some(self.held_merge_records(merge).await?),
        };
        let intent = LineageIntent {
            graph_commit_id: property.intent_nonce.clone(),
            branch: property.graph_branch.clone(),
            actor_id: property.actor_id.clone(),
            merged_parent,
            created_at: property.created_at,
            history_release_bytes: HistoryReleaseBytes(property.history_release_bytes),
        };
        let precondition = PublishPrecondition::ExactCandidate {
            authority: property.authority(),
            graph_commit_id: property.graph_commit_id.clone(),
        };
        let outcome = self
            .commit_changes_with_lineage_and_precondition(
                &changes,
                &property.expected_table_versions(),
                Some(&intent),
                &precondition,
            )
            .await?;
        if let Err(error) = self.release_candidate_tags(candidate).await {
            tracing::warn!(
                graph_commit_id = %property.graph_commit_id,
                %error,
                "unpublished commit published; its tags remain until cleanup releases them"
            );
        }
        Ok(outcome)
    }

    /// The source records a merge candidate publishes as its merged parent,
    /// read from the held source version.
    async fn held_merge_records(&self, merge: &UnpublishedMergeInputs) -> Result<BranchRecords> {
        let source = self
            .dataset
            .checkout_version(Ref::Version(
                merge.source_native.clone(),
                Some(merge.source_manifest_version),
            ))
            .await
            .map_err(OmniError::storage)?;
        let rows = read_manifest_rows_projected(&source).await?;
        if rows.head.graph_commit_id != merge.source_head_commit_id {
            return Err(OmniError::manifest_internal(format!(
                "held source version {} of branch '{}' holds head '{}', not the captured '{}'",
                merge.source_manifest_version,
                merge.source_branch,
                rows.head.graph_commit_id,
                merge.source_head_commit_id
            )));
        }
        let mut records = rows.records();
        records.merge_base = Some(merge.merge_base_commit_id.clone());
        Ok(records)
    }

    /// Release the candidate's input holds, then remove its addressability.
    /// Publication authority is the caller's to check first: a published
    /// commit is never dropped. The holds go first: they are nonce-owned and
    /// idempotent, and a failure between the two steps leaves a readable
    /// proposal whose retried drop releases them again, whereas holds left
    /// after the tag is gone have no owner to name them until the target
    /// head moves. The deletion is confirmed by a read.
    pub async fn drop_unpublished(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        candidate: &UnpublishedCommit,
    ) -> Result<()> {
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        if let Some(merge) = &candidate.property.merge {
            release_merge_input_tags(&main, &merge.input_tags).await?;
        }
        fail(&DROP_POST_INPUTS_PRE_TAG)?;
        let name = candidate.tag_name();
        release_unpublished_tag(&main, &name).await?;
        match main.tags().get(&name).await {
            Ok(_) => Err(OmniError::manifest_publish_in_doubt(format!(
                "the tag of unpublished commit '{}' is still listed after its deletion",
                candidate.property.graph_commit_id
            ))),
            Err(lance::Error::RefNotFound { .. }) => Ok(()),
            Err(error) if error.is_not_found() => Ok(()),
            Err(error) => Err(OmniError::manifest_publish_in_doubt(format!(
                "the deletion of the tag of unpublished commit '{}' could not be confirmed: {error}",
                candidate.property.graph_commit_id
            ))),
        }
    }

    async fn release_candidate_tags(&self, candidate: &UnpublishedCommit) -> Result<()> {
        release_unpublished_tag(&self.dataset, &candidate.tag_name()).await?;
        if let Some(merge) = &candidate.property.merge {
            release_merge_input_tags(&self.dataset, &merge.input_tags).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposal_tag_names_carry_a_graph_commit_id() {
        let id = "hb1.01ARZ3NDEKTSV4RRFFQ69G5FAV.3.01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let name = unpublished_tag_name(id);
        assert_eq!(
            unpublished_tag_commit_id(&name).unwrap().as_deref(),
            Some(id)
        );
        assert!(is_unpublished_tag(&name));
        assert_eq!(unpublished_tag_commit_id("user_tag").unwrap(), None);
        assert!(unpublished_tag_commit_id("unpublished_not-a-commit").is_err());
        assert!(!is_unpublished_tag("unpublished_"));
    }
}
