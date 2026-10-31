//! Unattached graph branches: one write's complete snapshot, readable and
//! extendable by its graph commit id, invisible to every branch read and
//! never published (RFC "Unattached graph branches").
//!
//! A snapshot is a detached version `D` of the recorded target's
//! `__manifest` holding its complete rows: the result `table` rows, a head
//! row whose id is the snapshot's graph commit id `C` (a fresh canonical
//! ULID), an empty buffer and the captured schema contract. Its transaction
//! carries [`UnattachedCommitProperty`], the version 2 layout of the
//! candidate property that version 1 proposals share the key of. A chain
//! starts at a root, whose base is the recorded target's live head, and
//! grows by children whose base is an earlier snapshot; every record repeats
//! the root's captured coordinates and is a root exactly when its root id is
//! its own id. The one addressability authority is the candidate tag
//! `unattached_<root>_<C>`, created put-if-absent at `(native branch, D)`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::{BranchIdentifier, Ref, TagContents};
use lance::dataset::transaction::Transaction;
pub use omnigraph_core::branch_names::{
    UNATTACHED_TAG_PREFIX, is_unattached_tag, unattached_tag_name, unattached_tag_parts,
    unattached_tag_root_prefix,
};
use omnigraph_core::graph_commit_id::canonical_ulid;

use crate::commit_graph::{CommitGraph, GraphCommit, GraphCommitKind, HistoryCache};
use crate::error::{OmniError, Result};
use crate::history::HistoryRecord;
use crate::migrations::{read_stamp, require_unattached_branches};
use crate::publisher::{
    GraphHeadExpectation, UnattachedIntent, UnattachedOutcome, UnattachedWriteBase,
};
use crate::retention::{ManifestTagInventory, list_native_tag_names, read_native_tag};
use crate::seams::{decide_seam, fail};
use crate::state::{GraphLineageRow, read_manifest_rows_projected};
use crate::unpublished::{
    UNPUBLISHED_COMMIT_PROPERTY, UnpublishedCommitProperty, resolve_recorded_target,
};
use crate::{ManifestCoordinator, open_manifest_dataset_native_with_session};

/// The layout version of [`UnattachedCommitProperty`]; version 1 is the
/// publishable proposal, any other is refused.
pub const UNATTACHED_PROPERTY_VERSION: u32 = 2;

/// The transaction property of an unattached snapshot's detached version:
/// its place in its chain and the target authority its root captured.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnattachedCommitProperty {
    pub version: u32,
    /// `C`, a canonical ULID minted for this attempt.
    pub graph_commit_id: String,
    /// The first snapshot of the chain; equal to `graph_commit_id` on a root.
    pub root_commit_id: String,
    /// The captured effective head of the recorded target on a root, the
    /// base snapshot's `C` on a child.
    pub parent_commit_id: String,
    /// The root's captured effective head, repeated on every record of the
    /// chain: the lifetime rule compares the live head with it.
    pub captured_head_commit_id: String,
    pub graph_branch: Option<String>,
    pub native_branch: Option<String>,
    pub branch_identifier: BranchIdentifier,
    /// The captured authority held no exact head: the target had not written
    /// since its creation, so `captured_head_commit_id` is the head it
    /// inherited.
    pub inherited_head: bool,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

impl UnattachedCommitProperty {
    /// The transaction properties of the detached version that carries this
    /// record, under the key version 1 proposals share.
    pub fn encode(&self) -> Result<HashMap<String, String>> {
        let value = serde_json::to_string(self).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode the unattached commit property: {error}"
            ))
        })?;
        Ok(HashMap::from([(
            UNPUBLISHED_COMMIT_PROPERTY.to_string(),
            value,
        )]))
    }

    pub fn tag_name(&self) -> String {
        unattached_tag_name(&self.root_commit_id, &self.graph_commit_id)
    }

    /// A record is a root exactly when its root is itself; no discriminator
    /// is stored.
    pub fn is_root(&self) -> bool {
        self.root_commit_id == self.graph_commit_id
    }

    /// The authority the chain's root captured: a child's writer revalidates
    /// it, never the child's own parent id.
    pub fn authority(&self) -> GraphHeadExpectation {
        GraphHeadExpectation::new(
            self.graph_branch.as_deref(),
            self.branch_identifier.clone(),
            (!self.inherited_head).then(|| self.captured_head_commit_id.clone()),
        )
    }

    /// Refuse a snapshot whose rows, tag and property disagree. No row clock
    /// is compared: the head row's version column carries `D`.
    fn validate(
        &self,
        head: &GraphLineageRow,
        tag: &TagContents,
        detached_version: u64,
    ) -> Result<()> {
        let mismatch = |what: &str| {
            OmniError::manifest_internal(format!(
                "unattached branch '{}': its {what} disagrees with its transaction property",
                self.graph_commit_id
            ))
        };
        if self.version != UNATTACHED_PROPERTY_VERSION {
            return Err(mismatch("property version"));
        }
        if canonical_ulid(&self.graph_commit_id).is_none() {
            return Err(mismatch("graph commit id (not a canonical ULID)"));
        }
        if canonical_ulid(&self.root_commit_id).is_none() {
            return Err(mismatch("root commit id (not a canonical ULID)"));
        }
        if self.is_root() && self.parent_commit_id != self.captured_head_commit_id {
            return Err(mismatch("root parent (not the captured head)"));
        }
        if !self.is_root() && canonical_ulid(&self.parent_commit_id).is_none() {
            return Err(mismatch("parent commit id (not a canonical ULID)"));
        }
        if head.graph_commit_id != self.graph_commit_id {
            return Err(mismatch("head commit id"));
        }
        if head.parent_commit_id.as_deref() != Some(self.parent_commit_id.as_str()) {
            return Err(mismatch("parent commit id"));
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
        Ok(())
    }
}

/// The candidate property of a detached `__manifest` version, dispatched by
/// its stored `version`: the publishable proposal or the unattached snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateProperty {
    Unpublished(UnpublishedCommitProperty),
    Unattached(UnattachedCommitProperty),
}

impl CandidateProperty {
    pub(crate) fn decode(transaction: &Transaction) -> Result<Option<Self>> {
        let Some(value) = transaction
            .transaction_properties
            .as_ref()
            .and_then(|properties| properties.get(UNPUBLISHED_COMMIT_PROPERTY))
        else {
            return Ok(None);
        };
        Self::decode_value(value).map(Some)
    }

    pub(crate) fn decode_value(value: &str) -> Result<Self> {
        let peeked: serde_json::Value = serde_json::from_str(value).map_err(|error| {
            OmniError::manifest_internal(format!("the candidate property does not decode: {error}"))
        })?;
        match peeked.get("version").and_then(serde_json::Value::as_u64) {
            Some(1) => UnpublishedCommitProperty::decode_value(value).map(Self::Unpublished),
            Some(2) => serde_json::from_str(value)
                .map(Self::Unattached)
                .map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "the unattached commit property does not decode: {error}"
                    ))
                }),
            other => Err(OmniError::manifest_internal(format!(
                "candidate property version {} is neither the proposal layout 1 nor the \
                 unattached layout {UNATTACHED_PROPERTY_VERSION} this binary reads",
                other.map_or_else(|| "(absent)".to_string(), |version| version.to_string())
            ))),
        }
    }
}

/// Why an unattached snapshot is no longer readable or extendable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnattachedExpiry {
    /// The target's head provably moved past the captured head.
    HeadAdvanced { head: String },
    /// The target incarnation is retired or replaced.
    IncarnationRetired,
}

/// The three-way lifetime verdict on a snapshot, shared by creation, reads,
/// listing, drop and collection; there is no published arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnattachedLifetime {
    /// Same live incarnation, same head: readable and extendable.
    Live,
    /// Unavailable to new reads and writes; its tag is eligible for cleanup.
    Expired(UnattachedExpiry),
    /// Incomplete evidence: retained, every operation fails closed.
    Undecidable(String),
}

/// A snapshot as read from its tag, its detached version and its property.
#[derive(Debug, Clone)]
pub struct UnattachedCommit {
    pub property: UnattachedCommitProperty,
    /// The snapshot head with its complete `table` rows.
    pub record: HistoryRecord,
    /// `D`: the physical coordinate a receipt reports as
    /// `graph_manifest_version`.
    pub detached_version: u64,
    pub lifetime: UnattachedLifetime,
}

impl UnattachedCommit {
    /// The snapshot a reserved `unattached_` tag of `inventory` names,
    /// judged against the live target.
    pub async fn from_inventory(
        inventory: &ManifestTagInventory,
        root_uri: &str,
        name: &str,
    ) -> Result<Self> {
        UnattachedInventoryReader::new(inventory, root_uri)
            .read(name)
            .await
    }

    pub fn tag_name(&self) -> String {
        self.property.tag_name()
    }

    pub fn graph_commit(&self) -> GraphCommit {
        let mut commit =
            crate::commit_graph::graph_commit_from_manifest_row(self.record.commit.clone());
        commit.graph_manifest_version = self.detached_version;
        commit.published = false;
        commit.kind = GraphCommitKind::Unattached;
        commit.root_commit_id = Some(self.property.root_commit_id.clone());
        commit
    }

    /// The snapshot's view, labelled with its physical coordinate.
    /// Its reads go through the store registry of `session`, the reading
    /// handle's control session.
    pub fn snapshot(
        &self,
        root_uri: &str,
        session: &Arc<lance::session::Session>,
    ) -> Result<crate::Snapshot> {
        let mut snapshot = self.record.snapshot_as(
            root_uri,
            self.detached_version,
            self.property.native_branch.as_deref(),
            session,
        )?;
        snapshot.graph_branch = self.property.graph_branch.clone();
        Ok(snapshot)
    }

    pub fn is_live(&self) -> bool {
        self.lifetime == UnattachedLifetime::Live
    }

    pub fn is_root(&self) -> bool {
        self.property.is_root()
    }

    /// Refuse a new read of a snapshot that is no longer live: an expired
    /// snapshot is not found, and an undecidable one fails closed.
    pub fn require_readable(&self) -> Result<()> {
        match &self.lifetime {
            UnattachedLifetime::Live => Ok(()),
            UnattachedLifetime::Expired(expiry) => Err(self.expired(expiry)),
            UnattachedLifetime::Undecidable(reason) => Err(self.undecidable(reason)),
        }
    }

    /// Refuse a child over a snapshot that is no longer live: the same rule
    /// as a read, since a child captures its base's view.
    pub fn require_extendable(&self) -> Result<()> {
        self.require_readable()
    }

    /// Refuse a drop the lifetime rule does not admit: a live or expired
    /// snapshot may go, an undecidable one is retained.
    pub fn require_droppable(&self) -> Result<()> {
        match &self.lifetime {
            UnattachedLifetime::Live | UnattachedLifetime::Expired(_) => Ok(()),
            UnattachedLifetime::Undecidable(reason) => Err(self.undecidable(reason)),
        }
    }

    fn expired(&self, expiry: &UnattachedExpiry) -> OmniError {
        let branch = self.property.graph_branch.as_deref().unwrap_or("main");
        let cause = match expiry {
            UnattachedExpiry::HeadAdvanced { head } => {
                format!("the head of branch '{branch}' advanced to '{head}'")
            }
            UnattachedExpiry::IncarnationRetired => {
                format!("its incarnation of branch '{branch}' is retired")
            }
        };
        OmniError::manifest_not_found(format!(
            "unattached branch '{}' not found: it expired ({cause}) and awaits cleanup",
            self.property.graph_commit_id
        ))
    }

    fn undecidable(&self, reason: &str) -> OmniError {
        OmniError::manifest(format!(
            "unattached branch '{}' cannot be judged against its target: {reason}",
            self.property.graph_commit_id
        ))
    }
}

/// Delete a reserved candidate tag; absence is the completed outcome.
pub async fn release_unattached_tag(dataset: &Dataset, name: &str) -> Result<()> {
    if !is_unattached_tag(name) {
        return Err(OmniError::manifest_internal(
            "refusing to release a native tag that is not a candidate tag",
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

/// Delete a reserved candidate tag and confirm its absence by a read: a
/// tag still listed, or a read that fails, leaves the release in doubt.
pub async fn release_unattached_tag_confirmed(
    dataset: &Dataset,
    name: &str,
    graph_commit_id: &str,
) -> Result<()> {
    release_unattached_tag(dataset, name).await?;
    confirm_tag_released(dataset, name, graph_commit_id).await
}

/// Reads snapshots against one captured tag inventory, sharing ancestry and
/// target checks for the duration of a listing or collection operation.
pub struct UnattachedInventoryReader<'a> {
    inventory: &'a ManifestTagInventory,
    tags: NativeTags<'a>,
    root_uri: &'a str,
    judged: JudgedLifetimes,
}

impl<'a> UnattachedInventoryReader<'a> {
    pub fn new(inventory: &'a ManifestTagInventory, root_uri: &'a str) -> Self {
        Self {
            inventory,
            tags: NativeTags::sealed(inventory),
            root_uri,
            judged: JudgedLifetimes::default(),
        }
    }

    pub async fn read(&mut self, name: &str) -> Result<UnattachedCommit> {
        let tag = self.inventory.tags.get(name).cloned().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "native tag '{name}' is not in the captured inventory"
            ))
        })?;
        let dataset = self.tags.dataset;
        ManifestCoordinator::unattached_from_tag_sharing(
            self.root_uri,
            dataset,
            &mut self.tags,
            name,
            &tag,
            &mut self.judged,
        )
        .await
    }
}

/// Candidate tags by name: a sealed inventory answers from what it captured
/// and reads nothing, an on-demand source reads each selected name once.
struct NativeTags<'a> {
    dataset: &'a Dataset,
    sealed: bool,
    read: BTreeMap<String, Option<TagContents>>,
}

impl<'a> NativeTags<'a> {
    fn on_demand(dataset: &'a Dataset) -> Self {
        Self {
            dataset,
            sealed: false,
            read: BTreeMap::new(),
        }
    }

    fn sealed(inventory: &'a ManifestTagInventory) -> Self {
        Self {
            dataset: inventory.dataset(),
            sealed: true,
            read: inventory
                .tags
                .iter()
                .map(|(name, tag)| (name.clone(), Some(tag.clone())))
                .collect(),
        }
    }

    async fn get(&mut self, name: &str) -> Result<Option<&TagContents>> {
        if !self.read.contains_key(name) {
            let tag = if self.sealed {
                None
            } else {
                read_native_tag(self.dataset, name).await?
            };
            self.read.insert(name.to_string(), tag);
        }
        Ok(self.read.get(name).and_then(Option::as_ref))
    }
}

#[derive(Default)]
struct JudgedLifetimes {
    targets: HashMap<(Option<String>, Option<String>, String), UnattachedLifetime>,
    ancestors: HashMap<String, (UnattachedCommitProperty, UnattachedLifetime)>,
}

impl JudgedLifetimes {
    async fn judge(
        &mut self,
        root_uri: &str,
        main: &Dataset,
        tags: &mut NativeTags<'_>,
        property: &UnattachedCommitProperty,
    ) -> Result<UnattachedLifetime> {
        let mut current = property.clone();
        let mut path = Vec::new();
        let mut visited = HashSet::new();
        let lifetime = loop {
            validate_chain_coordinates(property, &current)?;
            if !visited.insert(current.graph_commit_id.clone()) {
                return Err(OmniError::manifest_internal(format!(
                    "unattached branch '{}' has cyclic ancestry at '{}'",
                    property.graph_commit_id, current.graph_commit_id
                )));
            }
            if let Some((validated, lifetime)) = self.ancestors.get(&current.graph_commit_id) {
                if validated != &current {
                    return Err(OmniError::manifest_internal(format!(
                        "unattached branch '{}' has inconsistent ancestry metadata",
                        current.graph_commit_id
                    )));
                }
                break lifetime.clone();
            }
            path.push(current.clone());
            if current.is_root() {
                break self.judge_target(root_uri, main, &current).await?;
            }
            let parent_tag =
                unattached_tag_name(&current.root_commit_id, &current.parent_commit_id);
            let Some(tag) = tags.get(&parent_tag).await?.cloned() else {
                break UnattachedLifetime::Undecidable(format!(
                    "required ancestor '{}' under root '{}' has no retained candidate tag",
                    current.parent_commit_id, current.root_commit_id
                ));
            };
            if let Some((parent, _)) = self.ancestors.get(&current.parent_commit_id) {
                current = parent.clone();
            } else {
                current = ManifestCoordinator::unattached_record_from_tag(main, &parent_tag, &tag)
                    .await?
                    .0;
            }
        };
        for ancestor in path {
            self.ancestors.insert(
                ancestor.graph_commit_id.clone(),
                (ancestor, lifetime.clone()),
            );
        }
        Ok(lifetime)
    }

    async fn judge_target(
        &mut self,
        root_uri: &str,
        main: &Dataset,
        property: &UnattachedCommitProperty,
    ) -> Result<UnattachedLifetime> {
        let key = (
            property.graph_branch.clone(),
            property.native_branch.clone(),
            property.captured_head_commit_id.clone(),
        );
        if let Some(lifetime) = self.targets.get(&key) {
            return Ok(lifetime.clone());
        }
        let lifetime = judge_unattached_lifetime(root_uri, main, property).await?;
        self.targets.insert(key, lifetime.clone());
        Ok(lifetime)
    }
}

fn validate_chain_coordinates(
    child: &UnattachedCommitProperty,
    ancestor: &UnattachedCommitProperty,
) -> Result<()> {
    if child.root_commit_id != ancestor.root_commit_id
        || child.graph_branch != ancestor.graph_branch
        || child.native_branch != ancestor.native_branch
        || child.branch_identifier != ancestor.branch_identifier
        || child.captured_head_commit_id != ancestor.captured_head_commit_id
        || child.inherited_head != ancestor.inherited_head
    {
        return Err(OmniError::manifest_internal(format!(
            "unattached branch '{}' disagrees with ancestor '{}' about its root or target",
            child.graph_commit_id, ancestor.graph_commit_id
        )));
    }
    Ok(())
}

/// The lifetime of a snapshot captured under `property`, judged against the
/// live target: one branch listing and one scan of the target's head, whose
/// row carries the inherited id on a fresh branch (the same-head case).
async fn judge_unattached_lifetime(
    root_uri: &str,
    main: &Dataset,
    property: &UnattachedCommitProperty,
) -> Result<UnattachedLifetime> {
    let Some(target) = resolve_recorded_target(
        main,
        &property.graph_commit_id,
        property.graph_branch.as_deref(),
        property.native_branch.as_deref(),
    )
    .await?
    else {
        return Ok(UnattachedLifetime::Expired(
            UnattachedExpiry::IncarnationRetired,
        ));
    };
    let rows = read_manifest_rows_projected(&target).await?;
    let head = rows.head.graph_commit_id.clone();
    let captured = property.captured_head_commit_id.as_str();
    if head == captured {
        return Ok(UnattachedLifetime::Live);
    }
    if rows.buffer.get(captured).is_some() {
        return Ok(UnattachedLifetime::Expired(
            UnattachedExpiry::HeadAdvanced { head },
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
        Ok(lineage) if lineage.get_commit(captured).is_some() => Ok(UnattachedLifetime::Expired(
            UnattachedExpiry::HeadAdvanced { head },
        )),
        Ok(_) => Ok(UnattachedLifetime::Undecidable(format!(
            "the target head '{head}' does not descend from the captured head '{captured}'"
        ))),
        Err(error) => Ok(UnattachedLifetime::Undecidable(format!(
            "the target lineage below '{head}' is unavailable: {error}"
        ))),
    }
}

decide_seam! {
    /// After a descendant's candidate tag is confirmed gone and before the
    /// next release toward the requested id: the requested id and its
    /// remaining descendants stay addressable, so a retried drop finds fewer
    /// descendants and finishes upward; nothing is removed out of order.
    pub static UNATTACHED_DROP_POST_DESCENDANT_PRE_PARENT = ("unattached.drop_post_descendant_pre_parent", AnyWrite, [Fail]);
}

impl ManifestCoordinator {
    /// Write `changes` as an unattached snapshot on this coordinator's
    /// branch: a root over the live head, or a child over the snapshot
    /// `base` names, under the exact `authority` the chain's root captured.
    /// The live head, and this coordinator's view of it, are unchanged:
    /// nothing is installed.
    pub async fn propose_unattached(
        &self,
        changes: &[crate::ManifestChange],
        expected_table_versions: &crate::ExpectedTableVersions,
        intent: &UnattachedIntent,
        authority: &GraphHeadExpectation,
        base: &UnattachedWriteBase,
    ) -> Result<UnattachedOutcome> {
        self.publisher
            .write_unattached_candidate(changes, expected_table_versions, intent, authority, base)
            .await
    }

    /// The snapshot `graph_commit_id` names, or `None` when no tag names it.
    /// One listing of tag names (the tag name carries the root, which a
    /// lookup by `C` alone does not know) and one read of the named tag,
    /// then validates the snapshot and its retained ancestry, reading each
    /// ancestor tag, detached version and transaction along the parent path
    /// before judging the live target. No other tag is opened. An id that is
    /// not a canonical ULID names no snapshot and costs no listing.
    pub async fn unattached_commit(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        graph_commit_id: &str,
    ) -> Result<Option<UnattachedCommit>> {
        if canonical_ulid(graph_commit_id).is_none() {
            return Ok(None);
        }
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        let mut named = Vec::new();
        for name in list_native_tag_names(&main).await? {
            if let Some((_, named_id)) = unattached_tag_parts(&name)?
                && named_id == graph_commit_id
            {
                named.push(name);
            }
        }
        match named.as_slice() {
            [] => Ok(None),
            [name] => {
                let mut tags = NativeTags::on_demand(&main);
                let Some(tag) = tags.get(name).await?.cloned() else {
                    return Ok(None);
                };
                Self::unattached_from_tag_sharing(
                    root_uri,
                    &main,
                    &mut tags,
                    name,
                    &tag,
                    &mut JudgedLifetimes::default(),
                )
                .await
                .map(Some)
            }
            many => Err(OmniError::manifest_internal(format!(
                "unattached branch '{graph_commit_id}' is named by {} candidate tags under \
                 different roots ({})",
                many.len(),
                many.join(", ")
            ))),
        }
    }

    async fn unattached_from_tag_sharing(
        root_uri: &str,
        main: &Dataset,
        tags: &mut NativeTags<'_>,
        name: &str,
        tag: &TagContents,
        judged: &mut JudgedLifetimes,
    ) -> Result<UnattachedCommit> {
        let (property, record) = Self::unattached_record_from_tag(main, name, tag).await?;
        let lifetime = judged.judge(root_uri, main, tags, &property).await?;
        Ok(UnattachedCommit {
            property,
            record,
            detached_version: tag.version,
            lifetime,
        })
    }

    async fn unattached_record_from_tag(
        main: &Dataset,
        name: &str,
        tag: &TagContents,
    ) -> Result<(UnattachedCommitProperty, HistoryRecord)> {
        let (expected_root, expected_id) = unattached_tag_parts(name)?.ok_or_else(|| {
            OmniError::manifest_internal(format!("native tag '{name}' is not a candidate tag"))
        })?;
        let detached = main
            .checkout_version(Ref::Version(tag.branch.clone(), Some(tag.version)))
            .await
            .map_err(OmniError::storage)?;
        let stamp = read_stamp(&detached).ok_or_else(|| {
            OmniError::manifest_internal(format!("the version tag '{name}' names carries no stamp"))
        })?;
        require_unattached_branches(stamp)?;
        let transaction = detached
            .read_transaction()
            .await
            .map_err(OmniError::storage)?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the version tag '{name}' names records no transaction"
                ))
            })?;
        let property = match CandidateProperty::decode(&transaction)? {
            Some(CandidateProperty::Unattached(property)) => property,
            Some(CandidateProperty::Unpublished(_)) => {
                return Err(OmniError::manifest_internal(format!(
                    "the version tag '{name}' names carries a publishable proposal, not an \
                     unattached branch"
                )));
            }
            None => {
                return Err(OmniError::manifest_internal(format!(
                    "the version tag '{name}' names carries no candidate property"
                )));
            }
        };
        if property.graph_commit_id != expected_id || property.root_commit_id != expected_root {
            return Err(OmniError::manifest_internal(format!(
                "native tag '{name}' names a version whose property records root '{}' and \
                 commit '{}'",
                property.root_commit_id, property.graph_commit_id
            )));
        }
        let rows = read_manifest_rows_projected(&detached).await?;
        property.validate(&rows.head, tag, tag.version)?;
        if !rows.buffer.commits().is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "unattached branch '{expected_id}' buffers commits; a snapshot buffers none"
            )));
        }
        Ok((
            property,
            HistoryRecord {
                commit: rows.head,
                tables: rows.tables,
            },
        ))
    }

    /// Every live snapshot whose recorded target is `branch` (`None` =
    /// main), newest first by `(created_at, graph_commit_id)`. One listing
    /// of tag names, then one read per candidate tag and one target scan per
    /// distinct chain: the cost grows with the retained snapshots, and no
    /// other tag is opened.
    pub async fn list_unattached(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        branch: Option<&str>,
    ) -> Result<Vec<UnattachedCommit>> {
        let branch = branch.filter(|branch| *branch != "main");
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        let names: Vec<String> = list_native_tag_names(&main)
            .await?
            .into_iter()
            .filter(|name| is_unattached_tag(name))
            .collect();
        let mut tags = NativeTags::on_demand(&main);
        let mut judged = JudgedLifetimes::default();
        let mut snapshots = Vec::new();
        for name in &names {
            let Some(tag) = tags.get(name).await?.cloned() else {
                continue;
            };
            let snapshot = Self::unattached_from_tag_sharing(
                root_uri,
                &main,
                &mut tags,
                name,
                &tag,
                &mut judged,
            )
            .await?;
            if snapshot.property.graph_branch.as_deref() == branch && snapshot.is_live() {
                snapshots.push(snapshot);
            }
        }
        snapshots.sort_by(|a, b| {
            (b.property.created_at, &b.property.graph_commit_id)
                .cmp(&(a.property.created_at, &a.property.graph_commit_id))
        });
        Ok(snapshots)
    }

    /// Every snapshot under `root` that descends from `graph_commit_id`,
    /// deepest first (post-order), so a caller removes each one before its
    /// base. Discovery selects the candidate tags by the root's name prefix
    /// from one listing of tag names and opens no other tag; each one under
    /// the prefix is read, decoded and judged once. A record reached twice
    /// is a cycle, an integrity failure.
    pub async fn unattached_descendants(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        root: &str,
        graph_commit_id: &str,
    ) -> Result<Vec<UnattachedCommit>> {
        let prefix = unattached_tag_root_prefix(root);
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        let names: Vec<String> = list_native_tag_names(&main)
            .await?
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
            .collect();
        let mut tags = NativeTags::on_demand(&main);
        let mut judged = JudgedLifetimes::default();
        let mut records = Vec::with_capacity(names.len());
        for name in &names {
            let Some(tag) = tags.get(name).await?.cloned() else {
                continue;
            };
            records.push(
                Self::unattached_from_tag_sharing(
                    root_uri,
                    &main,
                    &mut tags,
                    name,
                    &tag,
                    &mut judged,
                )
                .await?,
            );
        }
        let mut children: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, record) in records.iter().enumerate() {
            record.require_droppable()?;
            if !record.is_root() {
                children
                    .entry(record.property.parent_commit_id.as_str())
                    .or_default()
                    .push(index);
            }
        }
        let mut visited: HashSet<&str> = HashSet::from([graph_commit_id]);
        let mut order = Vec::new();
        descend(
            graph_commit_id,
            root,
            &records,
            &children,
            &mut visited,
            &mut order,
        )?;
        let mut taken: Vec<Option<UnattachedCommit>> = records.into_iter().map(Some).collect();
        Ok(order
            .into_iter()
            .map(|index| {
                taken[index]
                    .take()
                    .expect("post-order lists each record once")
            })
            .collect())
    }

    /// Remove the addressability of `candidate` after that of `descendants`,
    /// which the caller captured deepest first under the recorded target's
    /// control gate: each tag's deletion is confirmed by a read before the
    /// next release, so a failure leaves the requested id and every not yet
    /// removed descendant addressable and a retried drop finishes the rest.
    /// The lifetime rule is checked on every record before the first
    /// release: an undecidable descendant blocks its ancestor.
    pub async fn drop_unattached(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        candidate: &UnattachedCommit,
        descendants: &[UnattachedCommit],
    ) -> Result<()> {
        candidate.require_droppable()?;
        for descendant in descendants {
            descendant.require_droppable()?;
        }
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        for descendant in descendants {
            release_unattached_tag_confirmed(
                &main,
                &descendant.tag_name(),
                &descendant.property.graph_commit_id,
            )
            .await?;
            fail(&UNATTACHED_DROP_POST_DESCENDANT_PRE_PARENT)?;
        }
        release_unattached_tag_confirmed(
            &main,
            &candidate.tag_name(),
            &candidate.property.graph_commit_id,
        )
        .await
    }
}

fn descend<'a>(
    id: &'a str,
    root: &str,
    records: &'a [UnattachedCommit],
    children: &HashMap<&'a str, Vec<usize>>,
    visited: &mut HashSet<&'a str>,
    order: &mut Vec<usize>,
) -> Result<()> {
    for &child in children.get(id).map(Vec::as_slice).unwrap_or_default() {
        let child_id = records[child].property.graph_commit_id.as_str();
        if !visited.insert(child_id) {
            return Err(OmniError::manifest_internal(format!(
                "unattached branch '{child_id}' under root '{root}' is reached twice: its \
                 ancestry holds a cycle"
            )));
        }
        descend(child_id, root, records, children, visited, order)?;
        order.push(child);
    }
    Ok(())
}

/// The deletion of the candidate tag `name`, which addresses graph commit
/// `graph_commit_id`, confirmed by a read: a tag still listed, or a read that
/// fails, leaves the release in doubt.
async fn confirm_tag_released(main: &Dataset, name: &str, graph_commit_id: &str) -> Result<()> {
    match main.tags().get(name).await {
        Ok(_) => Err(OmniError::manifest_publish_in_doubt(
            graph_commit_id,
            format!("the candidate tag '{name}' is still listed after its deletion"),
        )),
        Err(lance::Error::RefNotFound { .. }) => Ok(()),
        Err(error) if error.is_not_found() => Ok(()),
        Err(error) => Err(OmniError::manifest_publish_in_doubt(
            graph_commit_id,
            format!("the deletion of the candidate tag '{name}' could not be confirmed: {error}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::TableRow;

    const ROOT: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const CHILD: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
    const HEAD: &str = "hb1.01ARZ3NDEKTSV4RRFFQ69G5FAV.3.01ARZ3NDEKTSV4RRFFQ69G5FAW";

    fn root_property() -> UnattachedCommitProperty {
        UnattachedCommitProperty {
            version: UNATTACHED_PROPERTY_VERSION,
            graph_commit_id: ROOT.to_string(),
            root_commit_id: ROOT.to_string(),
            parent_commit_id: HEAD.to_string(),
            captured_head_commit_id: HEAD.to_string(),
            graph_branch: Some("feature".to_string()),
            native_branch: Some("feature.01ARZ3NDEKTSV4RRFFQ69G5FAW".to_string()),
            branch_identifier: BranchIdentifier::main(),
            inherited_head: false,
            actor_id: Some("alice".to_string()),
            created_at: 7,
        }
    }

    fn child_property() -> UnattachedCommitProperty {
        UnattachedCommitProperty {
            graph_commit_id: CHILD.to_string(),
            parent_commit_id: ROOT.to_string(),
            ..root_property()
        }
    }

    fn version_1_value() -> String {
        serde_json::to_string(&UnpublishedCommitProperty {
            version: crate::unpublished::UNPUBLISHED_PROPERTY_VERSION,
            graph_commit_id: HEAD.to_string(),
            intent_nonce: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_string(),
            graph_branch: None,
            native_branch: None,
            branch_identifier: BranchIdentifier::main(),
            parent_commit_id: ROOT.to_string(),
            inherited_head: false,
            captured_manifest_version: 4,
            actor_id: None,
            created_at: 7,
            history_release_bytes: 0,
            expected_table_versions: Vec::new(),
            merge: None,
        })
        .unwrap()
    }

    #[test]
    fn version_2_property_round_trips_and_names_its_tag() {
        for property in [root_property(), child_property()] {
            let encoded = property.encode().unwrap();
            let value = &encoded[UNPUBLISHED_COMMIT_PROPERTY];
            assert_eq!(
                CandidateProperty::decode_value(value).unwrap(),
                CandidateProperty::Unattached(property.clone())
            );
            assert_eq!(
                property.tag_name(),
                unattached_tag_name(ROOT, &property.graph_commit_id)
            );
            let refused = UnpublishedCommitProperty::decode_value(value)
                .unwrap_err()
                .to_string();
            assert!(
                refused.contains("the unpublished commit property does not decode"),
                "a version 1 reader refuses the version 2 layout: {refused}"
            );
        }
        assert!(root_property().is_root());
        assert!(!child_property().is_root());
        assert_eq!(
            root_property().authority().head_commit_id.as_deref(),
            Some(HEAD)
        );
        let inherited = UnattachedCommitProperty {
            inherited_head: true,
            ..root_property()
        };
        assert_eq!(inherited.authority().head_commit_id, None);
    }

    #[test]
    fn candidate_property_dispatches_on_the_stored_version() {
        let version_1 = version_1_value();
        assert!(matches!(
            CandidateProperty::decode_value(&version_1).unwrap(),
            CandidateProperty::Unpublished(property) if property.graph_commit_id == HEAD
        ));

        let version_3 = version_1.replacen("\"version\":1", "\"version\":3", 1);
        assert_ne!(version_3, version_1);
        let refused = CandidateProperty::decode_value(&version_3)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("version 3 is neither"), "{refused}");

        let absent = "{\"graph_commit_id\":\"x\"}";
        let refused = CandidateProperty::decode_value(absent)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("version (absent) is neither"), "{refused}");

        let encoded = root_property().encode().unwrap();
        let value = &encoded[UNPUBLISHED_COMMIT_PROPERTY];
        let with_kind = value.replacen("{", "{\"kind\":\"unattached\",", 1);
        let refused = CandidateProperty::decode_value(&with_kind)
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("unattached commit property does not decode")
                && refused.contains("unknown field `kind`"),
            "no kind discriminator is stored: {refused}"
        );
    }

    fn snapshot_with(lifetime: UnattachedLifetime) -> UnattachedCommit {
        let property = child_property();
        let commit = GraphLineageRow {
            graph_commit_id: property.graph_commit_id.clone(),
            schema_contract: None,
            schema_content_hash: None,
            graph_branch: property.graph_branch.clone(),
            native_branch: property.native_branch.clone(),
            graph_manifest_version: 9,
            generation: 2,
            parent_commit_id: Some(property.parent_commit_id.clone()),
            merged_parent_commit_id: None,
            actor_id: property.actor_id.clone(),
            created_at: property.created_at,
        };
        let tables: Vec<TableRow> = Vec::new();
        UnattachedCommit {
            property,
            record: HistoryRecord { commit, tables },
            detached_version: 42,
            lifetime,
        }
    }

    #[test]
    fn lifetime_rule_admits_reads_extensions_and_drops() {
        let live = snapshot_with(UnattachedLifetime::Live);
        assert!(live.require_readable().is_ok());
        assert!(live.require_extendable().is_ok());
        assert!(live.require_droppable().is_ok());
        let commit = live.graph_commit();
        assert_eq!(commit.kind, GraphCommitKind::Unattached);
        assert!(!commit.published);
        assert_eq!(commit.graph_manifest_version, 42);
        assert_eq!(commit.root_commit_id.as_deref(), Some(ROOT));

        let advanced = snapshot_with(UnattachedLifetime::Expired(
            UnattachedExpiry::HeadAdvanced {
                head: "later".to_string(),
            },
        ));
        let refused = advanced.require_readable().unwrap_err().to_string();
        assert!(
            refused.contains("not found") && refused.contains("advanced to 'later'"),
            "{refused}"
        );
        assert!(advanced.require_extendable().is_err());
        assert!(advanced.require_droppable().is_ok());

        let retired = snapshot_with(UnattachedLifetime::Expired(
            UnattachedExpiry::IncarnationRetired,
        ));
        let refused = retired.require_readable().unwrap_err().to_string();
        assert!(refused.contains("retired"), "{refused}");
        assert!(retired.require_droppable().is_ok());

        let undecidable = snapshot_with(UnattachedLifetime::Undecidable("no lineage".to_string()));
        for refused in [
            undecidable.require_readable(),
            undecidable.require_extendable(),
            undecidable.require_droppable(),
        ] {
            let refused = refused.unwrap_err().to_string();
            assert!(
                refused.contains("cannot be judged") && refused.contains("no lineage"),
                "{refused}"
            );
        }
    }
}
