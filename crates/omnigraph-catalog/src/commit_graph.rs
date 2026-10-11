use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use lance::Dataset;
use omnigraph_core::graph_commit_id::{commit_id_answers, parse_history_block_id};
use ulid::Ulid;

use crate::error::{OmniError, Result};
use crate::history;
use crate::state::{BlockRun, LineageRegistry, MergeEdge};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GraphCommit {
    pub graph_commit_id: String,
    pub graph_branch: Option<String>,
    pub graph_manifest_version: u64,
    /// The greatest generation among the commit's parents plus one; the
    /// genesis commit is generation 0.
    pub generation: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
    /// `false` for an unpublished commit: a proposal readable by its id and
    /// invisible to every branch read until it is published.
    #[serde(default = "published_by_default")]
    pub published: bool,
}

fn published_by_default() -> bool {
    true
}

impl GraphCommit {
    /// The one total order for deterministic lineage listing, head selection,
    /// and any future commit-list keyset cursor. Ancestry and CDC traversal use
    /// persisted first-parent links instead; this key never defines feed order.
    pub fn lineage_key(&self) -> (u64, i64, &str) {
        (
            self.graph_manifest_version,
            self.created_at,
            &self.graph_commit_id,
        )
    }
}

/// One observable graph transition on a branch's first-parent lineage.
///
/// CDC compares exactly these two immutable graph commits. A merge commit is
/// compared with the branch state it landed on (`parent`); its
/// `merged_parent_commit_id` remains provenance and is never traversed as a
/// second feed path.
#[derive(Debug, Clone)]
pub struct FirstParentEdge {
    pub parent: GraphCommit,
    pub child: GraphCommit,
}

type SettledCommits = Arc<HashMap<String, GraphCommit>>;

/// Two captured tails, with room for their parent frontiers on both walks.
/// A larger merge search reads addressed blocks on demand.
const MERGE_BASE_VISIT_BUDGET: usize = 4 * (crate::TAIL_MAX_COMMITS + 1);

/// Visits one lazy merge search may spend, queue and distance bookkeeping included.
const LAZY_MERGE_VISITS: usize = 100_000;
/// Bytes of retained lineage records one lazy merge search may hold; storage
/// bounds each encoded extent separately at 64 MiB.
const LAZY_MERGE_BYTES: usize = 32 * 1024 * 1024;
/// Missing commits one fetch of the lazy merge search may address at once.
const LAZY_MERGE_FETCH_IDS: usize = 8;

/// The commits one reader has read from `__history`, seen leave a
/// `__manifest` buffer, or merged, shared by every [`CommitGraph`] built over
/// it. A published commit never changes, so what the cache holds stays true
/// for any branch of the graph; a head and a buffered commit are never taken
/// from it.
///
/// With the head and the buffer of the reader, the commits it holds are
/// closed under their parents. A read of `__history` is: a commit is appended
/// after its parents or with them, or while the branch it was merged into
/// still buffers them. [`Self::settle`] adds a commit only when the cache
/// holds its parents.
#[derive(Clone, Default)]
pub struct HistoryCache {
    settled: Arc<Mutex<Option<SettledCommits>>>,
    /// The immutable `__history` objects read through this cache. Apart from
    /// `settled`: an object holds the commits of one block, with no claim
    /// that the cache holds their parents.
    objects: history::ExtentCache,
    /// The cold registry fragments of `__manifest` read through this cache,
    /// by data file: written once and named by every later version, where
    /// fragment ids repeat across Lance branches and file names do not.
    cold: Arc<Mutex<HashMap<String, Arc<LineageRegistry>>>>,
}

/// Cold registry files held per handle before the ones a read did not name
/// are dropped: a few branches' worth of logarithmic fragment counts.
const COLD_REGISTRY_FILES: usize = 64;

impl HistoryCache {
    /// The registry rows of the cold fragments of `dataset`'s version,
    /// cached by data-file path while retained in this handle.
    pub(crate) async fn cold_registry(&self, dataset: &Dataset) -> Result<LineageRegistry> {
        let fragments = crate::state::cold_fragments(dataset);
        let mut registry = LineageRegistry::default();
        let mut named = Vec::with_capacity(fragments.len());
        for fragment in &fragments {
            let path = fragment
                .files
                .first()
                .map(|file| file.path.clone())
                .ok_or_else(|| {
                    OmniError::manifest_internal("a `__manifest` fragment names no data file")
                })?;
            let held = self
                .cold
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&path)
                .cloned();
            let rows = match held {
                Some(rows) => rows,
                None => {
                    let rows = Arc::new(
                        crate::state::read_cold_registry(dataset, std::slice::from_ref(fragment))
                            .await?,
                    );
                    self.cold
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(path.clone(), Arc::clone(&rows));
                    rows
                }
            };
            registry.merge(&rows);
            named.push(path);
        }
        let mut cold = self.cold.lock().unwrap_or_else(PoisonError::into_inner);
        if cold.len() > COLD_REGISTRY_FILES {
            cold.retain(|path, _| named.contains(path));
        }
        Ok(registry)
    }

    /// The `__history` objects read through this cache, kept with no
    /// freshness check: an object never changes once created.
    pub fn objects(&self) -> &history::ExtentCache {
        &self.objects
    }

    /// [`history::read_commit`], through the objects this cache holds.
    pub async fn read_commit(
        &self,
        root_uri: &str,
        session: &Arc<lance::session::Session>,
        graph_commit_id: &str,
    ) -> Result<Option<crate::GraphLineageRow>> {
        history::read_commit_in(root_uri, session, &self.objects, graph_commit_id).await
    }

    /// [`history::read_record`], through the objects this cache holds.
    pub async fn read_record(
        &self,
        root_uri: &str,
        session: &Arc<lance::session::Session>,
        graph_commit_id: &str,
    ) -> Result<Option<history::HistoryRecord>> {
        history::read_record_in(root_uri, session, &self.objects, graph_commit_id).await
    }

    fn held(&self) -> Option<SettledCommits> {
        self.settled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn replace(&self, commits: SettledCommits) {
        *self.settled.lock().unwrap_or_else(PoisonError::into_inner) = Some(commits);
    }

    /// A settled commit the cache holds, with no read of `__history`.
    pub fn get_commit(&self, commit_id: &str) -> Option<GraphCommit> {
        self.held()?.get(commit_id).cloned()
    }

    /// A settled commit the cache holds, named by its published ID or by the
    /// intent nonce a publish gave it ([`commit_id_answers`]).
    pub fn get_commit_answering(&self, requested: &str) -> Result<Option<GraphCommit>> {
        let Some(held) = self.held() else {
            return Ok(None);
        };
        if let Some(commit) = held.get(requested) {
            return Ok(Some(commit.clone()));
        }
        for commit in held.values() {
            if commit_id_answers(&commit.graph_commit_id, requested)? {
                return Ok(Some(commit.clone()));
            }
        }
        Ok(None)
    }

    /// Forget every settled commit and every `__history` object: the root was
    /// replaced under this handle, so nothing held describes the new root.
    /// Reaches every clone sharing this cache, unlike a new `HistoryCache`.
    pub fn reset(&self) {
        *self.settled.lock().unwrap_or_else(PoisonError::into_inner) = None;
        self.objects.clear();
        self.cold
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Add published commits, parents before children: the commits that left
    /// a buffer and the commits of a merged branch. A commit whose parents the
    /// cache lacks is left out, and the next lineage that names it reads
    /// `__history`.
    pub fn settle(&self, commits: impl IntoIterator<Item = GraphCommit>) {
        let mut held = self.settled.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(settled) = held.as_mut() else {
            return;
        };
        for commit in commits {
            if holds_parents(settled, &commit) && !settled.contains_key(&commit.graph_commit_id) {
                Arc::make_mut(settled).insert(commit.graph_commit_id.clone(), commit);
            }
        }
    }
}

fn holds_parents(settled: &HashMap<String, GraphCommit>, commit: &GraphCommit) -> bool {
    [&commit.parent_commit_id, &commit.merged_parent_commit_id]
        .into_iter()
        .flatten()
        .all(|parent| settled.contains_key(parent))
}

/// The head commit of one graph branch and the commits its `__manifest`
/// buffers, read from one version of that `__manifest`.
///
/// An operation that asks for history calls [`Self::lineage`], the one reader
/// of `__history` lineage; opening a graph, refreshing it, publishing and
/// reading a branch never do.
#[derive(Clone)]
pub struct CommitGraph {
    root_uri: String,
    session: Arc<lance::session::Session>,
    held: HeldCommits,
    history: HistoryCache,
    /// The registry rows the `__manifest` version's hot fragment holds.
    registry: Arc<LineageRegistry>,
    /// That version, for its cold registry fragments; `None` for a head
    /// read from no `__manifest` version, which the walk serves by records.
    manifest: Option<Dataset>,
}

/// The commits one `__manifest` version holds: its head and, oldest first, the
/// commits it buffers, a first-parent run that ends at the head.
#[derive(Clone)]
struct HeldCommits {
    head: GraphCommit,
    buffered: Arc<[GraphCommit]>,
}

impl HeldCommits {
    fn get(&self, commit_id: &str) -> Option<&GraphCommit> {
        std::iter::once(&self.head)
            .chain(self.buffered.iter().rev())
            .find(|commit| commit.graph_commit_id == commit_id)
    }

    /// The parents the held commits name and do not hold: the first parent of
    /// the oldest held commit and every merged parent.
    fn outside_parents(&self) -> impl Iterator<Item = &str> {
        let oldest = self.buffered.first().unwrap_or(&self.head);
        let merged = self
            .buffered
            .iter()
            .chain(std::iter::once(&self.head))
            .filter_map(|commit| commit.merged_parent_commit_id.as_deref());
        oldest
            .parent_commit_id
            .as_deref()
            .into_iter()
            .chain(merged)
            .filter(move |parent| self.get(parent).is_none())
    }
}

/// A branch head, the commits its `__manifest` buffers, and the commits read
/// from `__history`: what a history operation reads. Cloning it is O(1), and
/// no later publish changes it.
#[derive(Clone)]
pub struct Lineage {
    held: HeldCommits,
    settled: SettledCommits,
}

impl Lineage {
    pub fn head(&self) -> &GraphCommit {
        &self.held.head
    }

    /// The commit `commit_id`, looked up in the head, then the buffer, then
    /// the commits read from `__history`.
    pub fn get_commit(&self, commit_id: &str) -> Option<&GraphCommit> {
        self.held
            .get(commit_id)
            .or_else(|| self.settled.get(commit_id))
    }

    /// The head and every commit its first-parent pointers reach, oldest
    /// first: the commits of the branch.
    pub fn first_parent_chain(&self) -> Result<Vec<GraphCommit>> {
        let mut chain = vec![self.held.head.clone()];
        let mut parent = self.held.head.parent_commit_id.as_deref();
        while let Some(id) = parent {
            let commit = self.get_commit(id).ok_or_else(|| missing_parent(id))?;
            parent = commit.parent_commit_id.as_deref();
            chain.push(commit.clone());
        }
        chain.reverse();
        Ok(chain)
    }
}

fn missing_parent(commit_id: &str) -> OmniError {
    OmniError::manifest_internal(format!(
        "graph commit '{commit_id}' is the parent of a commit of this graph and `__history` does \
         not hold it"
    ))
}

/// One merge-base walk over the two lineages plus imported records; each
/// `unresolved_*` side lists the ids it reaches, held by no lineage, before
/// meeting a commit the other side also holds.
pub struct MergeBaseSearch {
    pub base: Option<GraphCommit>,
    pub unresolved_source: Vec<String>,
    pub unresolved_target: Vec<String>,
}

/// One incremental merge-base walk. Both merge and cleanup feed other
/// captured lineages in the same order and stop at the same resolved frontier.
pub struct MergeBaseResolver<'a> {
    source: &'a Lineage,
    target: &'a Lineage,
    source_head: &'a str,
    target_head: &'a str,
    imported: HashMap<String, GraphCommit>,
    retired_owners: HashMap<String, String>,
}

impl<'a> MergeBaseResolver<'a> {
    pub fn new(
        source: &'a Lineage,
        target: &'a Lineage,
        source_head: &'a str,
        target_head: &'a str,
    ) -> Self {
        Self {
            source,
            target,
            source_head,
            target_head,
            imported: HashMap::new(),
            retired_owners: HashMap::new(),
        }
    }

    fn get(&self, commit_id: &str) -> Option<&GraphCommit> {
        self.source
            .get_commit(commit_id)
            .or_else(|| self.target.get_commit(commit_id))
            .or_else(|| self.imported.get(commit_id))
    }

    pub fn search(&self) -> MergeBaseSearch {
        merge_base_search(&|id| self.get(id), self.source_head, self.target_head)
    }

    pub fn import_retired(&mut self, commits: Vec<GraphCommit>, owner: &str) -> Result<()> {
        for commit in &commits {
            if self.get(&commit.graph_commit_id).is_none() {
                self.retired_owners
                    .insert(commit.graph_commit_id.clone(), owner.to_string());
            }
        }
        self.import(commits)
    }

    /// Retain only providers of imported records reached by the actual ancestry walks.
    pub fn retained_import_owners(&self) -> BTreeSet<String> {
        let get = |id: &str| self.get(id);
        let mut unresolved = BTreeSet::new();
        let mut owners = BTreeSet::new();
        for head in [self.source_head, self.target_head] {
            let reached = ancestor_distances_from(head, &get, &|_| false, &mut unresolved);
            owners.extend(
                reached
                    .keys()
                    .filter_map(|id| self.retired_owners.get(id))
                    .cloned(),
            );
        }
        owners
    }

    pub fn import(&mut self, commits: impl IntoIterator<Item = GraphCommit>) -> Result<()> {
        for commit in commits {
            if self
                .get(&commit.graph_commit_id)
                .is_some_and(|previous| previous != &commit)
            {
                return Err(OmniError::manifest_internal(
                    "one graph commit id has conflicting lineage records",
                ));
            }
            self.imported
                .entry(commit.graph_commit_id.clone())
                .or_insert(commit);
        }
        Ok(())
    }
}

impl MergeBaseSearch {
    pub fn needs_import(&self) -> bool {
        !self.unresolved_source.is_empty() || !self.unresolved_target.is_empty()
    }
}

impl CommitGraph {
    /// The commit graph of the branch whose `__manifest` holds `head` and
    /// buffers `buffered`, oldest first, over the commits `history` has read.
    pub fn from_head(
        root_uri: &str,
        session: Arc<lance::session::Session>,
        head: crate::GraphLineageRow,
        buffered: &[crate::GraphLineageRow],
        history: HistoryCache,
    ) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            session,
            held: HeldCommits {
                head: graph_commit_from_manifest_row(head),
                buffered: buffered
                    .iter()
                    .cloned()
                    .map(graph_commit_from_manifest_row)
                    .collect(),
            },
            history,
            registry: Arc::default(),
            manifest: None,
        }
    }

    /// With the block registry of the `__manifest` version the head was read
    /// from: `registry` is its hot fragment's rows, `dataset` names its cold
    /// fragments, read by a merge and never by an open.
    pub fn with_registry(mut self, registry: Arc<LineageRegistry>, dataset: &Dataset) -> Self {
        self.registry = registry;
        self.manifest = Some(dataset.clone());
        self
    }

    /// The block registry of the head's `__manifest` version, hot and cold
    /// rows; a merge walks it and copies it into the target.
    pub async fn registry(&self) -> Result<LineageRegistry> {
        let mut registry = self.registry.as_ref().clone();
        if let Some(dataset) = &self.manifest {
            registry.merge(&self.history.cold_registry(dataset).await?);
        }
        Ok(registry)
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        Self::open_branch(root_uri, None).await
    }

    /// Fails with the typed branch miss when `branch` is not a live branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        Self::open_branch(root_uri, Some(branch)).await
    }

    async fn open_branch(root_uri: &str, branch: Option<&str>) -> Result<Self> {
        let session = crate::lance_access::control_session();
        let (rows, dataset) =
            crate::ManifestCoordinator::read_rows_at(root_uri, branch, &session).await?;
        Ok(Self::from_head(
            root_uri,
            session,
            rows.head,
            rows.buffer.commits(),
            HistoryCache::default(),
        )
        .with_registry(Arc::new(rows.registry), &dataset))
    }

    pub fn head(&self) -> &GraphCommit {
        &self.held.head
    }

    pub async fn head_commit(&self) -> Result<Option<GraphCommit>> {
        Ok(Some(self.held.head.clone()))
    }

    pub async fn head_commit_id(&self) -> Result<Option<String>> {
        Ok(Some(self.held.head.graph_commit_id.clone()))
    }

    /// Prove a merge base from both captured tails and already cached commits,
    /// without reading storage or adding entries to the history cache. `None`
    /// means the bounded proof is inconclusive, not that no common ancestor exists.
    /// A successful proof does not validate unread settled history; malformed or
    /// missing records outside its frontier can remain undetected.
    pub fn held_merge_base(&self, other: &Self) -> Option<GraphCommit> {
        if self.root_uri != other.root_uri {
            return None;
        }
        let settled = self.history.held();
        let other_settled = other.history.held();
        partial_merge_base_search(
            &|id| {
                self.held
                    .get(id)
                    .or_else(|| other.held.get(id))
                    .or_else(|| settled.as_ref().and_then(|commits| commits.get(id)))
                    .or_else(|| other_settled.as_ref().and_then(|commits| commits.get(id)))
            },
            &self.held.head.graph_commit_id,
            &other.held.head.graph_commit_id,
            MERGE_BASE_VISIT_BUDGET,
        )
    }

    /// Resolve only ancestry that can affect the exact merge-base ordering.
    /// The block registries of both heads' versions let the walk cross
    /// released history without a read; a commit outside them is fetched by
    /// its block. Fetched blocks are never inserted in the parent-closed cache
    /// used by full lineage readers; the objects they came from stay in
    /// [`HistoryCache::objects`], so a repeated search issues no request.
    pub async fn merge_base(&self, other: &Self) -> Result<Option<GraphCommit>> {
        if self.root_uri != other.root_uri {
            return Err(OmniError::manifest_internal(
                "merge heads belong to different roots",
            ));
        }
        if let Some(base) = self.held_merge_base(other) {
            return Ok(Some(base));
        }
        let mut registry = self.registry().await?;
        registry.merge(&other.registry().await?);
        let index = RegistryIndex::new(&registry);
        let settled = self.history.held();
        let other_settled = other.history.held();
        lazy_merge_base_search(
            &|id| {
                self.held
                    .get(id)
                    .or_else(|| other.held.get(id))
                    .or_else(|| settled.as_ref().and_then(|commits| commits.get(id)))
                    .or_else(|| other_settled.as_ref().and_then(|commits| commits.get(id)))
            },
            &self.head().graph_commit_id,
            &other.head().graph_commit_id,
            &index,
            |ids, remaining_bytes| async move {
                let ids: Vec<_> = ids.iter().map(String::as_str).collect();
                let mut found: HashMap<_, _> = history::read_lineage_of_bounded_in(
                    &self.root_uri,
                    &self.session,
                    self.history.objects(),
                    &ids,
                    remaining_bytes,
                )
                .await?
                .into_iter()
                .map(|(id, row)| (id, Some(graph_commit_from_manifest_row(row))))
                .collect();
                let missing: Vec<_> = ids
                    .iter()
                    .copied()
                    .filter(|id| !found.contains_key(*id))
                    .collect();
                for id in self.listed_absent(&missing).await? {
                    found.insert(id.to_string(), None);
                }
                Ok(found)
            },
            |block, slot, remaining_bytes| async move {
                Ok(history::read_commit_at_slot_in(
                    &self.root_uri,
                    &self.session,
                    self.history.objects(),
                    block,
                    slot,
                    remaining_bytes,
                )
                .await?
                .map(graph_commit_from_manifest_row))
            },
            LAZY_MERGE_VISITS,
            LAZY_MERGE_BYTES,
        )
        .await
    }

    /// The lineage of the head. Reads the lineage columns of `__history`
    /// unless the head and the buffer name no parent outside them, or the
    /// cache holds every such parent, which its closure under parents makes
    /// the whole ancestry. A merged parent the legacy directory lists as absent
    /// counts as held: the storage upgrade found no ref holding it. The
    /// directory is requested only for a parent the read of `__history` lacks.
    pub async fn lineage(&self) -> Result<Lineage> {
        let outside: Vec<&str> = self.held.outside_parents().collect();
        let missing = |settled: &SettledCommits| -> Vec<&str> {
            outside
                .iter()
                .copied()
                .filter(|id| !settled.contains_key(*id))
                .collect()
        };
        let held = self.history.held();
        let complete = held.as_ref().is_some_and(|settled| {
            let directory = self.history.objects().known_legacy_directory();
            missing(settled).into_iter().all(|id| {
                directory
                    .as_ref()
                    .is_some_and(|directory| directory.lists_absent(id))
            })
        });
        let settled = match held {
            Some(settled) if complete => settled,
            None if outside.is_empty() => {
                let settled = SettledCommits::default();
                self.history.replace(Arc::clone(&settled));
                settled
            }
            _ => {
                crate::instrumentation::record_projection_full_refresh();
                let settled: SettledCommits = Arc::new(
                    history::read_lineage_in(&self.root_uri, &self.session, self.history.objects())
                        .await?
                        .into_iter()
                        .map(|(id, commit)| (id, graph_commit_from_manifest_row(commit)))
                        .collect(),
                );
                self.history.replace(Arc::clone(&settled));
                settled
            }
        };
        if let Some(parent) = self.unlisted_absent(&missing(&settled)).await? {
            return Err(missing_parent(parent));
        }
        Ok(Lineage {
            held: self.held.clone(),
            settled,
        })
    }

    /// The first of `ids` the legacy directory does not list as an absent
    /// merged parent, every one of them when the root has no directory;
    /// `None` when it lists them all. No ids read nothing.
    async fn unlisted_absent<'a>(&self, ids: &[&'a str]) -> Result<Option<&'a str>> {
        let listed = self.listed_absent(ids).await?;
        Ok(ids.iter().copied().find(|id| !listed.contains(id)))
    }

    /// The `ids` the legacy directory lists as absent merged parents: the
    /// storage upgrade found no ref holding them. No ids read nothing.
    async fn listed_absent<'a>(&self, ids: &[&'a str]) -> Result<Vec<&'a str>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let directory =
            history::legacy_directory(&self.root_uri, &self.session, Some(self.history.objects()))
                .await?;
        Ok(ids
            .iter()
            .copied()
            .filter(|id| {
                directory
                    .as_ref()
                    .is_some_and(|directory| directory.lists_absent(id))
            })
            .collect())
    }

    /// The commits of the branch in [`GraphCommit::lineage_key`] order.
    pub async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        let mut commits = self.lineage().await?.first_parent_chain()?;
        commits.sort_by(|a, b| a.lineage_key().cmp(&b.lineage_key()));
        Ok(commits)
    }
}

pub fn graph_commit_from_manifest_row(row: crate::GraphLineageRow) -> GraphCommit {
    GraphCommit {
        graph_commit_id: row.graph_commit_id,
        graph_branch: row.graph_branch,
        graph_manifest_version: row.graph_manifest_version,
        generation: row.generation,
        parent_commit_id: row.parent_commit_id,
        merged_parent_commit_id: row.merged_parent_commit_id,
        actor_id: row.actor_id,
        created_at: row.created_at,
        published: true,
    }
}

fn merge_search_limit() -> OmniError {
    OmniError::manifest_internal("merge ancestry search exceeds its prototype work or memory limit")
}

fn charge_merge_bytes(used: &mut usize, added: usize, limit: usize) -> Result<()> {
    *used = used.checked_add(added).ok_or_else(merge_search_limit)?;
    if *used > limit {
        return Err(merge_search_limit());
    }
    Ok(())
}

fn commit_bytes(commit: &GraphCommit) -> usize {
    std::mem::size_of::<GraphCommit>()
        + 128
        + commit.graph_commit_id.capacity()
        + [
            &commit.graph_branch,
            &commit.parent_commit_id,
            &commit.merged_parent_commit_id,
            &commit.actor_id,
        ]
        .into_iter()
        .flatten()
        .map(String::capacity)
        .sum::<usize>()
}

/// Which heads reached a commit during a merge-base walk, and whether a
/// lowest common ancestor descends from it (`STALE`).
const FROM_SOURCE: u8 = 1;
const FROM_TARGET: u8 = 1 << 1;
const FROM_BOTH: u8 = FROM_SOURCE | FROM_TARGET;
const STALE: u8 = 1 << 2;

/// What a merge-base walk needs from its driver before it can continue.
enum WalkStep {
    /// The lowest common ancestors, best first by [`Candidate::key`]; the
    /// driver resolves the record of the first and of every candidate tied
    /// with it, and picks the smaller commit id.
    Found(Vec<Candidate>),
    /// Parents no record is held for, each with the greatest generation it can
    /// have, all able to be or hide a lowest common ancestor; the driver
    /// fetches them, declines the proof, or drops them as leaves.
    Unresolved(Vec<(String, u64)>),
    /// Every reachable commit is popped and none was reached by both heads.
    Exhausted,
}

/// A lowest common ancestor the walk popped with both heads' flags.
#[derive(Clone, Debug)]
struct Candidate {
    /// Fewest commits to the two heads summed, then the higher generation.
    key: (u64, Reverse<u64>),
    node: Node,
}

/// A commit as the walk addresses it: by id, or by its slot in a history
/// block when a registry run covers the slot, so that its generation and its
/// parents come from the run and its record is never read.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Node {
    Slot(Ulid, u16),
    Id(String),
}

impl Node {
    fn bytes(&self) -> usize {
        match self {
            Self::Slot(..) => 32,
            Self::Id(id) => id.len(),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Slot(block, slot) => write!(f, "slot {slot} of history block {block}"),
            Self::Id(id) => f.write_str(id),
        }
    }
}

/// A block registry as the walk reads it: the runs of each block and the
/// merge edges of each block by slot ([`LineageRegistry`]).
#[derive(Default)]
pub(crate) struct RegistryIndex {
    runs: HashMap<Ulid, Vec<BlockRun>>,
    merges: HashMap<Ulid, BTreeMap<u16, MergeEdge>>,
}

impl RegistryIndex {
    pub(crate) fn new(registry: &LineageRegistry) -> Self {
        let mut index = Self::default();
        for run in &registry.runs {
            index.runs.entry(run.block).or_default().push(run.clone());
        }
        for edge in &registry.merges {
            index
                .merges
                .entry(edge.block)
                .or_default()
                .insert(edge.slot, edge.clone());
        }
        index
    }

    /// The run covering `slot` of `block` that starts lowest: the runs of one
    /// block overlap after repeated merges and describe one chain.
    fn run(&self, block: Ulid, slot: u16) -> Option<&BlockRun> {
        self.runs
            .get(&block)?
            .iter()
            .filter(|run| run.first_slot <= slot && slot <= run.last_slot)
            .min_by_key(|run| run.first_slot)
    }

    /// The node of `id`: its slot when a run covers it.
    fn node(&self, id: &str) -> Node {
        match parse_history_block_id(id) {
            Ok(Some(parsed)) if self.run(parsed.block, parsed.slot).is_some() => {
                Node::Slot(parsed.block, parsed.slot)
            }
            _ => Node::Id(id.to_string()),
        }
    }

    /// The generation of `slot` in `run`: that of the last anchor at or below
    /// it, a merge edge or the run's first slot, plus the slots after it.
    fn generation(&self, run: &BlockRun, slot: u16) -> u64 {
        let anchor = self
            .merges
            .get(&run.block)
            .and_then(|edges| edges.range(run.first_slot..=slot).next_back())
            .map(|(at, edge)| (*at, edge.generation))
            .filter(|(at, _)| *at > run.first_slot)
            .unwrap_or((run.first_slot, run.first_generation));
        anchor.1.saturating_add(u64::from(slot - anchor.0))
    }

    fn merged_parent(&self, block: Ulid, slot: u16) -> Option<&str> {
        self.merges
            .get(&block)?
            .get(&slot)
            .map(|edge| edge.merged_parent_commit_id.as_str())
    }

    /// The highest slot of `run` below `slot` that carries a merge edge.
    fn merge_below(&self, run: &BlockRun, slot: u16) -> Option<u16> {
        self.merges
            .get(&run.block)?
            .range(run.first_slot..slot)
            .next_back()
            .map(|(at, _)| *at)
    }
}

/// What the walk holds for a queued node: its record, or the generation a
/// run gives its slot.
enum Rec {
    Commit(GraphCommit),
    Run(u64),
}

impl Rec {
    fn generation(&self) -> u64 {
        match self {
            Self::Commit(commit) => commit.generation,
            Self::Run(generation) => *generation,
        }
    }
}

/// The slots `bottom < slot < top` of one block the walk crossed in one
/// step, as the chain of a run, carrying `flags` from `top` at `distance`: a
/// slot among them reached later from another child inherits both.
struct Segment {
    top: u16,
    bottom: u16,
    flags: u8,
    distance: [u64; 2],
}

/// One priority queue over both heads, highest generation first, with per-head
/// reached flags propagated to parents: a node popped with both flags is a
/// lowest common ancestor, its ancestors are stale, the walk ends when none is live.
struct MergeBaseWalk<'r> {
    index: &'r RegistryIndex,
    heap: BinaryHeap<(u64, Reverse<Node>)>,
    /// Queued nodes, removed when popped.
    records: HashMap<Node, Rec>,
    reached: HashMap<Node, u8>,
    /// Fewest commits from the source head and from the target head.
    distances: HashMap<Node, [u64; 2]>,
    /// Ids no record is held for, each bounded by `child.generation - 1`.
    parked: BTreeMap<Node, u64>,
    /// The slots the walk reached, per block: a descent stops at them.
    recorded: HashMap<Ulid, BTreeSet<u16>>,
    segments: HashMap<Ulid, Vec<Segment>>,
    /// The id a slot was reached under, for the record of a slot candidate.
    known_ids: HashMap<(Ulid, u16), String>,
    candidates: Vec<Candidate>,
    live: usize,
    visits: usize,
    visit_limit: usize,
    bytes: usize,
    byte_limit: usize,
}

impl<'r> MergeBaseWalk<'r> {
    fn new(
        source: &GraphCommit,
        target: &GraphCommit,
        index: &'r RegistryIndex,
        visit_limit: usize,
        byte_limit: usize,
    ) -> Result<Self> {
        let mut walk = Self {
            index,
            heap: BinaryHeap::new(),
            records: HashMap::new(),
            reached: HashMap::new(),
            distances: HashMap::new(),
            parked: BTreeMap::new(),
            recorded: HashMap::new(),
            segments: HashMap::new(),
            known_ids: HashMap::new(),
            candidates: Vec::new(),
            live: 0,
            visits: 0,
            visit_limit,
            bytes: 0,
            byte_limit,
        };
        walk.charge(source.graph_commit_id.len() + target.graph_commit_id.len() + 256)?;
        let source_node = walk.node_of(&source.graph_commit_id);
        let target_node = walk.node_of(&target.graph_commit_id);
        walk.distances.insert(source_node.clone(), [0, u64::MAX]);
        walk.distances
            .entry(target_node.clone())
            .and_modify(|distance| distance[1] = 0)
            .or_insert([u64::MAX, 0]);
        let source_rec = walk.rec_of(&source_node, source)?;
        walk.reach(source_node, source_rec, FROM_SOURCE)?;
        let target_rec = walk.rec_of(&target_node, target)?;
        walk.reach(target_node, target_rec, FROM_TARGET)?;
        Ok(walk)
    }

    fn charge(&mut self, added: usize) -> Result<()> {
        charge_merge_bytes(&mut self.bytes, added, self.byte_limit)
    }

    fn remaining_bytes(&self) -> usize {
        self.byte_limit.saturating_sub(self.bytes)
    }

    /// The node of `id`, remembering the id of a slot node.
    fn node_of(&mut self, id: &str) -> Node {
        let node = self.index.node(id);
        if let Node::Slot(block, slot) = &node {
            self.known_ids
                .entry((*block, *slot))
                .or_insert_with(|| id.to_string());
        }
        node
    }

    /// What the walk holds for a head: its record, or the generation the
    /// run covering it gives, which must agree with the record.
    fn rec_of(&self, node: &Node, commit: &GraphCommit) -> Result<Rec> {
        match node {
            Node::Id(_) => Ok(Rec::Commit(commit.clone())),
            Node::Slot(block, slot) => {
                let run = self
                    .index
                    .run(*block, *slot)
                    .expect("a slot node is covered by a run");
                let generation = self.index.generation(run, *slot);
                if generation != commit.generation {
                    return Err(registry_disagrees(node, generation, commit.generation));
                }
                Ok(Rec::Run(generation))
            }
        }
    }

    /// Create the bookkeeping of `node` if absent: a slot inside a crossed
    /// segment inherits the flags and the distance the crossing carried.
    fn touch(&mut self, node: &Node) -> Result<()> {
        if self.reached.contains_key(node) {
            return Ok(());
        }
        self.charge(node.bytes() + 128)?;
        let mut flags = 0u8;
        let mut distance = [u64::MAX; 2];
        if let Node::Slot(block, slot) = node {
            if let Some(segments) = self.segments.get(block) {
                for segment in segments
                    .iter()
                    .filter(|segment| segment.bottom < *slot && *slot < segment.top)
                {
                    flags |= segment.flags;
                    let hops = u64::from(segment.top - *slot);
                    for (near, far) in distance.iter_mut().zip(segment.distance) {
                        *near = (*near).min(far.saturating_add(hops));
                    }
                }
            }
            self.recorded.entry(*block).or_default().insert(*slot);
        }
        self.reached.insert(node.clone(), flags);
        let nearer = self.distances.entry(node.clone()).or_insert([u64::MAX; 2]);
        for side in 0..2 {
            nearer[side] = nearer[side].min(distance[side]);
        }
        Ok(())
    }

    /// Add `flags` to a touched node; a queued node that turns stale stops
    /// counting as live.
    fn flag(&mut self, node: &Node, flags: u8) {
        let known = self
            .reached
            .get_mut(node)
            .expect("a flagged node is touched");
        let was_live = *known & STALE == 0;
        *known |= flags;
        if was_live && flags & STALE != 0 && self.records.contains_key(node) {
            self.live -= 1;
        }
    }

    /// Parked ids a lowest common ancestor is not known to descend from.
    fn live_parked(&self) -> usize {
        self.parked
            .keys()
            .filter(|node| self.reached[*node] & STALE == 0)
            .count()
    }

    /// Flag `node` and queue it once with `rec`.
    fn reach(&mut self, node: Node, rec: Rec, flags: u8) -> Result<()> {
        self.touch(&node)?;
        self.flag(&node, flags);
        if !self.records.contains_key(&node) {
            if let Rec::Commit(commit) = &rec {
                self.charge(commit_bytes(commit))?;
            }
            self.heap.push((rec.generation(), Reverse(node.clone())));
            if self.reached[&node] & STALE == 0 {
                self.live += 1;
            }
            self.records.insert(node, rec);
        }
        Ok(())
    }

    /// Queue a record the driver resolved for a parked id.
    fn admit(&mut self, commit: &GraphCommit) -> Result<()> {
        let node = Node::Id(commit.graph_commit_id.clone());
        let Some(bound) = self.parked.remove(&node) else {
            return Ok(());
        };
        if commit.generation > bound {
            return Err(generation_order_broken(&node));
        }
        self.reach(node, Rec::Commit(commit.clone()), 0)
    }

    /// Drop parked ids the driver has no record for; the walk treats them as leaves.
    fn forget<'b>(&mut self, ids: impl IntoIterator<Item = &'b str>) {
        for id in ids {
            self.parked.remove(&Node::Id(id.to_string()));
        }
    }

    fn finish(&mut self) -> WalkStep {
        if self.candidates.is_empty() {
            return WalkStep::Exhausted;
        }
        let mut candidates = self.candidates.clone();
        candidates.sort_by_key(|candidate| candidate.key);
        WalkStep::Found(candidates)
    }

    /// Reach `node`, a parent `hops` commits below a popped node of
    /// generation `bound + 1` reached at `distance` with `flags`: queue it
    /// from its run or from the record `get` holds, else park it.
    fn step<'a>(
        &mut self,
        node: Node,
        hops: u64,
        bound: u64,
        distance: [u64; 2],
        flags: u8,
        get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    ) -> Result<()> {
        self.touch(&node)?;
        let nearer = self
            .distances
            .get_mut(&node)
            .expect("a touched node has distances");
        for side in 0..2 {
            nearer[side] = nearer[side].min(distance[side].saturating_add(hops));
        }
        self.flag(&node, flags);
        match &node {
            Node::Slot(block, slot) => {
                let run = self
                    .index
                    .run(*block, *slot)
                    .expect("a slot node is covered by a run");
                let generation = self.index.generation(run, *slot);
                if generation > bound {
                    return Err(generation_order_broken(&node));
                }
                self.reach(node, Rec::Run(generation), 0)
            }
            Node::Id(id) => match get(id) {
                Some(record) => {
                    if record.generation > bound {
                        return Err(generation_order_broken(&node));
                    }
                    let record = record.clone();
                    self.reach(node, Rec::Commit(record), 0)
                }
                None => {
                    let parked = self.parked.entry(node).or_insert(bound);
                    *parked = (*parked).max(bound);
                    Ok(())
                }
            },
        }
    }

    /// The highest slot of `run` below `slot` at which the chain has an
    /// event: a merge edge, a slot the walk reached from elsewhere, or the
    /// run's first slot.
    fn event_below(&self, run: &BlockRun, slot: u16) -> u16 {
        let merge = self.index.merge_below(run, slot);
        let recorded = self
            .recorded
            .get(&run.block)
            .and_then(|slots| slots.range(run.first_slot..slot).next_back().copied());
        run.first_slot
            .max(merge.unwrap_or(0))
            .max(recorded.unwrap_or(0))
    }

    fn next<'a>(&mut self, get: &impl Fn(&str) -> Option<&'a GraphCommit>) -> Result<WalkStep> {
        loop {
            if self.live == 0 && self.live_parked() == 0 {
                return Ok(self.finish());
            }
            let top = self.heap.peek().map(|(generation, _)| *generation);
            let parked_bound = self.parked.values().max().copied();
            match (top, parked_bound) {
                (Some(generation), Some(bound)) if bound < generation => {}
                (Some(_), None) => {}
                (_, Some(_)) => {
                    let threshold = top.unwrap_or(0);
                    return Ok(WalkStep::Unresolved(
                        self.parked
                            .iter()
                            .filter(|(_, bound)| **bound >= threshold)
                            .map(|(node, bound)| (node.to_string(), *bound))
                            .collect(),
                    ));
                }
                (None, None) => return Ok(self.finish()),
            }
            if self.visits == self.visit_limit {
                return Err(merge_search_limit());
            }
            self.visits += 1;
            let (generation, Reverse(node)) = self.heap.pop().expect("peeked a queued node");
            let rec = self.records.remove(&node).expect("queued nodes are held");
            let mut flags = self.reached[&node];
            if flags & STALE == 0 {
                self.live -= 1;
                if flags == FROM_BOTH {
                    let [from_source, from_target] = self.distances[&node];
                    self.candidates.push(Candidate {
                        key: (from_source + from_target, Reverse(generation)),
                        node: node.clone(),
                    });
                    flags |= STALE;
                }
            }
            let distance = self.distances.get(&node).copied().unwrap_or([u64::MAX; 2]);
            let Some(bound) = generation.checked_sub(1) else {
                continue;
            };
            match rec {
                Rec::Commit(commit) => {
                    for parent in [&commit.parent_commit_id, &commit.merged_parent_commit_id]
                        .into_iter()
                        .flatten()
                    {
                        let parent = self.node_of(parent);
                        self.step(parent, 1, bound, distance, flags, get)?;
                    }
                }
                Rec::Run(_) => {
                    let Node::Slot(block, slot) = node else {
                        unreachable!("a run record belongs to a slot node");
                    };
                    if let Some(merged) = self.index.merged_parent(block, slot) {
                        let merged = merged.to_string();
                        let parent = self.node_of(&merged);
                        self.step(parent, 1, bound, distance, flags, get)?;
                    }
                    let run = self
                        .index
                        .run(block, slot)
                        .expect("a slot node is covered by a run")
                        .clone();
                    if slot == run.first_slot {
                        if let Some(parent) = &run.parent_commit_id {
                            let parent = self.node_of(parent);
                            self.step(parent, 1, bound, distance, flags, get)?;
                        }
                    } else {
                        let event = self.event_below(&run, slot);
                        if event + 1 < slot {
                            self.charge(64)?;
                            self.segments.entry(block).or_default().push(Segment {
                                top: slot,
                                bottom: event,
                                flags,
                                distance,
                            });
                        }
                        let hops = u64::from(slot - event);
                        self.step(Node::Slot(block, event), hops, bound, distance, flags, get)?;
                    }
                }
            }
        }
    }

    /// The record of `candidate` among held records: by id, or by the id its
    /// slot was reached under.
    fn held_record<'a>(
        &self,
        candidate: &Candidate,
        get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    ) -> Option<GraphCommit> {
        match &candidate.node {
            Node::Id(id) => get(id).cloned(),
            Node::Slot(block, slot) => get(self.known_ids.get(&(*block, *slot))?).cloned(),
        }
    }
}

/// The candidates tied with the best one by commit count and generation.
fn tied(candidates: &[Candidate]) -> &[Candidate] {
    let Some(best) = candidates.first() else {
        return candidates;
    };
    let end = candidates
        .iter()
        .position(|candidate| candidate.key != best.key)
        .unwrap_or(candidates.len());
    &candidates[..end]
}

/// The pick among resolved candidates: the smallest commit id of the tied.
fn pick(resolved: Vec<GraphCommit>) -> Option<GraphCommit> {
    resolved
        .into_iter()
        .min_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id))
}

fn generation_order_broken(node: &Node) -> OmniError {
    OmniError::manifest_internal(format!(
        "graph commit '{node}' has a generation not below its child's"
    ))
}

fn registry_disagrees(node: &Node, run: u64, record: u64) -> OmniError {
    OmniError::manifest_internal(format!(
        "the block registry gives '{node}' generation {run} and its record generation {record}"
    ))
}

/// The lowest common ancestor over the records `get` holds; a parent `get`
/// lacks is a leaf. `None` for no common ancestor or a malformed generation.
fn complete_merge_base<'a>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source_commit_id: &str,
    target_commit_id: &str,
) -> Option<GraphCommit> {
    let index = RegistryIndex::default();
    let (source, target) = (get(source_commit_id)?, get(target_commit_id)?);
    let mut walk = MergeBaseWalk::new(source, target, &index, usize::MAX, usize::MAX).ok()?;
    loop {
        match walk.next(get).ok()? {
            WalkStep::Found(candidates) => {
                let resolved = tied(&candidates)
                    .iter()
                    .map(|candidate| walk.held_record(candidate, get))
                    .collect::<Option<Vec<_>>>()?;
                return pick(resolved);
            }
            WalkStep::Unresolved(ids) => walk.forget(ids.iter().map(|(id, _)| id.as_str())),
            WalkStep::Exhausted => return None,
        }
    }
}

/// Prove the lowest common ancestor from held records alone. `None` when a
/// parent `get` lacks could precede or tie the answer, when the visit budget
/// runs out, or when no common ancestor is held: inconclusive, not absent.
fn partial_merge_base_search<'a>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source_commit_id: &str,
    target_commit_id: &str,
    visit_budget: usize,
) -> Option<GraphCommit> {
    let index = RegistryIndex::default();
    let (source, target) = (get(source_commit_id)?, get(target_commit_id)?);
    let mut walk = MergeBaseWalk::new(source, target, &index, visit_budget, usize::MAX).ok()?;
    match walk.next(get).ok()? {
        WalkStep::Found(candidates) => {
            let resolved = tied(&candidates)
                .iter()
                .map(|candidate| walk.held_record(candidate, get))
                .collect::<Option<Vec<_>>>()?;
            pick(resolved)
        }
        WalkStep::Unresolved(_) | WalkStep::Exhausted => None,
    }
}

/// The lowest common ancestor with parked parents fetched in bounded batches
/// and the registry runs of `index` crossed without a read; a parent `fetch`
/// reports as `None` is a leaf, `fetch_slot` reads a candidate reached by slot.
async fn lazy_merge_base_search<'a, F, Fut, S, SFut>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source: &str,
    target: &str,
    index: &RegistryIndex,
    mut fetch: F,
    mut fetch_slot: S,
    visit_limit: usize,
    byte_limit: usize,
) -> Result<Option<GraphCommit>>
where
    F: FnMut(Vec<String>, usize) -> Fut,
    Fut: Future<Output = Result<HashMap<String, Option<GraphCommit>>>>,
    S: FnMut(Ulid, u16, usize) -> SFut,
    SFut: Future<Output = Result<Option<GraphCommit>>>,
{
    let (Some(source), Some(target)) = (get(source), get(target)) else {
        return Err(OmniError::manifest_internal(
            "captured merge heads are unavailable",
        ));
    };
    let mut walk = MergeBaseWalk::new(source, target, index, visit_limit, byte_limit)?;
    let mut loaded = HashMap::<String, GraphCommit>::new();
    let mut absent = BTreeSet::new();
    loop {
        let step = {
            let held = |id: &str| get(id).or_else(|| loaded.get(id));
            walk.next(&held)?
        };
        let unresolved = match step {
            WalkStep::Found(candidates) => {
                let mut resolved = Vec::new();
                for candidate in tied(&candidates) {
                    let held = |id: &str| get(id).or_else(|| loaded.get(id));
                    let record = if let Some(record) = walk.held_record(candidate, &held) {
                        record
                    } else {
                        let Node::Slot(block, slot) = &candidate.node else {
                            return Err(OmniError::manifest_internal(format!(
                                "merge base candidate '{}' was popped and has no record",
                                candidate.node
                            )));
                        };
                        fetch_slot(*block, *slot, walk.remaining_bytes())
                            .await?
                            .ok_or_else(|| {
                                OmniError::manifest_internal(format!(
                                    "the block registry covers {} and `__history` holds no record there",
                                    candidate.node
                                ))
                            })?
                    };
                    let expected = candidate.key.1.0;
                    if record.generation != expected {
                        return Err(registry_disagrees(
                            &candidate.node,
                            expected,
                            record.generation,
                        ));
                    }
                    walk.charge(commit_bytes(&record))?;
                    resolved.push(record);
                }
                return Ok(pick(resolved));
            }
            WalkStep::Exhausted => return Ok(None),
            WalkStep::Unresolved(ids) => ids,
        };
        let requested: Vec<String> = unresolved
            .iter()
            .map(|(id, _)| id.clone())
            .filter(|id| !absent.contains(id))
            .take(LAZY_MERGE_FETCH_IDS)
            .collect();
        if requested.is_empty() {
            return Err(missing_parent(&unresolved[0].0));
        }
        let fetched = fetch(requested.clone(), walk.remaining_bytes()).await?;
        let mut leaves = BTreeSet::new();
        for (key, commit) in fetched {
            let Some(commit) = commit else {
                walk.charge(key.len() + 128)?;
                leaves.insert(key);
                continue;
            };
            if key != commit.graph_commit_id {
                return Err(OmniError::manifest_internal(
                    "history lookup returned a different commit id",
                ));
            }
            if let Some(previous) = get(&key).or_else(|| loaded.get(&key)) {
                if previous != &commit {
                    return Err(OmniError::manifest_internal(
                        "one graph commit id has conflicting lineage records",
                    ));
                }
                continue;
            }
            walk.admit(&commit)?;
            walk.charge(key.len() + commit_bytes(&commit))?;
            loaded.insert(key, commit);
        }
        walk.forget(leaves.iter().map(String::as_str));
        for id in requested {
            if get(&id).is_none() && !loaded.contains_key(&id) && !leaves.contains(&id) {
                walk.charge(id.len() + 128)?;
                absent.insert(id);
            }
        }
    }
}

fn merge_base_search<'a>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source_commit_id: &str,
    target_commit_id: &str,
) -> MergeBaseSearch {
    if get(source_commit_id).is_none() || get(target_commit_id).is_none() {
        return MergeBaseSearch {
            base: None,
            unresolved_source: Vec::new(),
            unresolved_target: Vec::new(),
        };
    }

    let mut full_walk_unresolved = BTreeSet::new();
    let source_distances =
        ancestor_distances_from(source_commit_id, get, &|_| false, &mut full_walk_unresolved);
    let target_distances =
        ancestor_distances_from(target_commit_id, get, &|_| false, &mut full_walk_unresolved);
    let mut unresolved_source = BTreeSet::new();
    ancestor_distances_from(
        source_commit_id,
        get,
        &|id| target_distances.contains_key(id),
        &mut unresolved_source,
    );
    let mut unresolved_target = BTreeSet::new();
    ancestor_distances_from(
        target_commit_id,
        get,
        &|id| source_distances.contains_key(id),
        &mut unresolved_target,
    );
    MergeBaseSearch {
        base: complete_merge_base(get, source_commit_id, target_commit_id),
        unresolved_source: unresolved_source.into_iter().collect(),
        unresolved_target: unresolved_target.into_iter().collect(),
    }
}

/// Breadth-first ancestor distances from `start_id`; a commit `stop_at` accepts
/// is recorded but not expanded, and ids `get` cannot resolve go to `unresolved`.
fn ancestor_distances_from<'a>(
    start_id: &str,
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    stop_at: &impl Fn(&str) -> bool,
    unresolved: &mut BTreeSet<String>,
) -> HashMap<String, u64> {
    let mut distances = HashMap::new();
    let mut queue = VecDeque::from([(start_id.to_string(), 0u64)]);

    while let Some((id, distance)) = queue.pop_front() {
        if distances
            .get(&id)
            .is_some_and(|existing| *existing <= distance)
        {
            continue;
        }
        distances.insert(id.clone(), distance);

        match get(&id) {
            Some(commit) => {
                if stop_at(&id) {
                    continue;
                }
                if let Some(parent) = &commit.parent_commit_id {
                    queue.push_back((parent.clone(), distance + 1));
                }
                if let Some(parent) = &commit.merged_parent_commit_id {
                    queue.push_back((parent.clone(), distance + 1));
                }
            }
            None => {
                unresolved.insert(id);
            }
        }
    }
    distances
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(
        id: &str,
        version: u64,
        generation: u64,
        parent: Option<&str>,
        merged_parent: Option<&str>,
    ) -> GraphCommit {
        GraphCommit {
            graph_commit_id: id.to_string(),
            graph_branch: None,
            graph_manifest_version: version,
            generation,
            parent_commit_id: parent.map(str::to_string),
            merged_parent_commit_id: merged_parent.map(str::to_string),
            actor_id: None,
            created_at: 0,
            published: true,
        }
    }

    fn search(commits: &[GraphCommit]) -> MergeBaseSearch {
        let by_id: HashMap<_, _> = commits
            .iter()
            .map(|commit| (commit.graph_commit_id.as_str(), commit))
            .collect();
        merge_base_search(&|id| by_id.get(id).copied(), "source", "target")
    }

    fn no_slot(_: Ulid, _: u16, _: usize) -> std::future::Ready<Result<Option<GraphCommit>>> {
        panic!("no run covers a slot of these commits")
    }

    fn bounded_search(commits: &[GraphCommit], visit_budget: usize) -> Option<GraphCommit> {
        let by_id: HashMap<_, _> = commits
            .iter()
            .map(|commit| (commit.graph_commit_id.as_str(), commit))
            .collect();
        partial_merge_base_search(
            &|id| by_id.get(id).copied(),
            "source",
            "target",
            visit_budget,
        )
    }

    #[test]
    fn merge_base_ties_use_the_smallest_commit_id() {
        for (a_version, b_version) in [(3, 5), (5, 5)] {
            let commits = [
                commit("root", 1, 0, None, None),
                commit("a", a_version, 1, Some("root"), None),
                commit("b", b_version, 1, Some("root"), None),
                commit("source", 6, 2, Some("a"), Some("b")),
                commit("target", 7, 2, Some("b"), Some("a")),
            ];
            let found = search(&commits);
            assert!(!found.needs_import());
            assert_eq!(found.base.unwrap().graph_commit_id, "a");
            assert_eq!(
                bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                    .unwrap()
                    .graph_commit_id,
                "a"
            );
        }
    }

    #[test]
    fn merge_base_is_the_newest_common_ancestor_past_a_second_parent_shortcut() {
        let commits = [
            commit("root", 1, 0, None, None),
            commit("older", 2, 1, Some("root"), None),
            commit("middle", 3, 2, Some("older"), None),
            commit("newer", 4, 3, Some("middle"), None),
            commit("source-path", 5, 4, Some("newer"), None),
            commit("target-path", 6, 4, Some("newer"), None),
            commit("source", 7, 5, Some("source-path"), Some("older")),
            commit("target", 8, 5, Some("target-path"), Some("older")),
        ];
        let found = search(&commits);
        assert!(!found.needs_import());
        assert_eq!(
            found.base.unwrap().graph_commit_id,
            "newer",
            "`older` is one hop from both heads through their second parents and still an \
             ancestor of `newer`, which both first-parent paths descend from"
        );
        assert_eq!(
            bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "newer"
        );
    }

    #[test]
    fn merge_base_linear_fork_and_unrelated_shapes() {
        let chain = [
            commit("root", 1, 0, None, None),
            commit("target", 2, 1, Some("root"), None),
            commit("mid", 3, 2, Some("target"), None),
            commit("source", 4, 3, Some("mid"), None),
        ];
        assert_eq!(search(&chain).base.unwrap().graph_commit_id, "target");
        assert_eq!(
            bounded_search(&chain, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "target"
        );
        let fork = [
            commit("root", 1, 0, None, None),
            commit("a", 2, 1, Some("root"), None),
            commit("b", 3, 1, Some("root"), None),
            commit("source", 4, 2, Some("a"), None),
            commit("target", 5, 2, Some("b"), None),
        ];
        assert_eq!(search(&fork).base.unwrap().graph_commit_id, "root");
        assert_eq!(
            bounded_search(&fork, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "root"
        );
        let unrelated = [
            commit("source", 1, 0, None, None),
            commit("target", 2, 0, None, None),
        ];
        assert!(search(&unrelated).base.is_none());
        assert!(bounded_search(&unrelated, MERGE_BASE_VISIT_BUDGET).is_none());
    }

    #[test]
    fn merge_base_criss_cross_picks_the_smallest_id_and_partial_proofs_stay_sound() {
        let commits = [
            commit("root", 1, 0, None, None),
            commit("base", 2, 1, Some("root"), None),
            commit("a", 3, 2, Some("base"), None),
            commit("b", 4, 2, Some("base"), None),
            commit("source-3", 5, 3, Some("b"), None),
            commit("source-2", 6, 4, Some("source-3"), None),
            commit("source-1", 7, 5, Some("source-2"), None),
            commit("target-3", 8, 3, Some("a"), None),
            commit("target-2", 9, 4, Some("target-3"), None),
            commit("target-1", 10, 5, Some("target-2"), None),
            commit("source", 11, 6, Some("a"), Some("source-1")),
            commit("target", 12, 6, Some("b"), Some("target-1")),
        ];
        assert_eq!(
            search(&commits).base.unwrap().graph_commit_id,
            "a",
            "`a` and `b` are both lowest common ancestors; the smaller id wins"
        );

        let partial = search(&commits[2..]);
        assert_eq!(partial.base.as_ref().unwrap().graph_commit_id, "a");
        assert!(!partial.needs_import());
        assert_eq!(
            bounded_search(&commits[2..], MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "a",
            "`a` is popped with both flags before any parent below generation 2 is examined"
        );

        let expected = search(&commits).base.unwrap();
        let mut proved = 0;
        let mut declined = 0;
        for mask in 0..1usize << (commits.len() - 2) {
            let held: Vec<_> = commits
                .iter()
                .enumerate()
                .filter(|(index, _)| *index >= commits.len() - 2 || mask & (1 << index) != 0)
                .map(|(_, commit)| commit.clone())
                .collect();
            match bounded_search(&held, MERGE_BASE_VISIT_BUDGET) {
                Some(base) => {
                    assert_eq!(base, expected, "partial capture mask {mask}");
                    proved += 1;
                }
                None => declined += 1,
            }
        }
        assert!(proved > 0 && declined > 0);
    }

    #[test]
    fn merge_base_partial_proof_declines_on_a_parked_record_or_the_visit_budget() {
        let commits = [
            commit("older", 3, 2, Some("unheld"), None),
            commit("base", 4, 3, Some("older"), None),
            commit("source", 5, 4, Some("base"), None),
            commit("target", 6, 4, Some("base"), None),
        ];
        for held in [&commits[..], &commits[1..]] {
            assert_eq!(
                bounded_search(held, MERGE_BASE_VISIT_BUDGET)
                    .unwrap()
                    .graph_commit_id,
                "base",
                "`base` is popped with both flags before its parent is examined"
            );
        }
        for visit_budget in [0, 1, 2] {
            assert!(
                bounded_search(&commits, visit_budget).is_none(),
                "budget {visit_budget} cannot pop both heads and the base"
            );
        }
        assert!(bounded_search(&commits, 3).is_some());

        let parked = [
            commit("base", 1, 0, None, None),
            commit("target", 2, 1, Some("base"), None),
            commit("source", 4, 3, Some("base"), Some("missing")),
        ];
        assert_eq!(search(&parked).base.unwrap().graph_commit_id, "base");
        assert!(
            bounded_search(&parked, MERGE_BASE_VISIT_BUDGET).is_none(),
            "`missing` can have generation 2 and reach `target`, above the next pop"
        );
    }

    #[test]
    fn merge_base_parked_records_at_the_same_generation_block_a_proof() {
        for (hidden_id, base_id) in [("hidden", "base"), ("a", "b")] {
            let commits = [
                commit(hidden_id, 5, 0, None, None),
                commit(base_id, 4, 0, None, None),
                commit("source", 6, 1, Some(base_id), Some(hidden_id)),
                commit("target", 7, 1, Some(base_id), Some(hidden_id)),
            ];
            assert_eq!(
                search(&commits).base.unwrap().graph_commit_id,
                hidden_id.min(base_id)
            );
            assert_eq!(
                search(&commits[1..]).base.unwrap().graph_commit_id,
                base_id,
                "a parent no record holds is a leaf of the complete search"
            );
            assert!(
                bounded_search(&commits[1..], MERGE_BASE_VISIT_BUDGET).is_none(),
                "the parked record can tie generation 0 and win by id"
            );
        }

        let commits = [
            commit("hidden", 3, 2, Some("target"), None),
            commit("base", 1, 0, None, None),
            commit("target", 2, 1, Some("base"), None),
            commit("source", 4, 3, Some("base"), Some("hidden")),
        ];
        assert_eq!(
            search(&commits).base.unwrap().graph_commit_id,
            "target",
            "the other head is reached through the second parent"
        );
        assert_eq!(search(&commits[1..]).base.unwrap().graph_commit_id, "base");
        assert!(
            bounded_search(&commits[1..], MERGE_BASE_VISIT_BUDGET).is_none(),
            "the parked record is bounded at generation 2, above the next pop"
        );
    }

    #[test]
    fn merge_base_refuses_a_generation_not_below_its_child() {
        let commits = [
            commit("base", 1, 1, None, None),
            commit("source", 2, 1, Some("base"), None),
            commit("target", 3, 2, Some("base"), None),
        ];
        assert!(search(&commits).base.is_none());
        assert!(bounded_search(&commits, MERGE_BASE_VISIT_BUDGET).is_none());
    }

    #[test]
    fn merge_base_criss_cross_prefers_the_ancestor_nearest_both_heads() {
        let commits = [
            commit("root", 1, 0, None, None),
            commit("a1", 2, 1, Some("root"), None),
            commit("a2", 3, 2, Some("a1"), None),
            commit("b1", 4, 1, Some("root"), None),
            commit("left", 5, 3, Some("a2"), None),
            commit("right", 6, 3, Some("a2"), None),
            commit("source", 7, 4, Some("left"), Some("b1")),
            commit("target", 8, 4, Some("right"), Some("b1")),
        ];
        assert_eq!(
            search(&commits).base.unwrap().graph_commit_id,
            "b1",
            "`a2` and `b1` are both lowest common ancestors; `b1`, one commit from each head, \
             beats the higher-generation fork point `a2`, two commits from each"
        );
        assert_eq!(
            bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "b1"
        );
    }

    async fn assert_lazy_matches_complete(commits: &[GraphCommit], block_width: usize) {
        let held = |id: &str| {
            commits
                .iter()
                .find(|commit| commit.graph_commit_id == id && matches!(id, "source" | "target"))
        };
        let mut requested = BTreeSet::new();
        let found = lazy_merge_base_search(
            &held,
            "source",
            "target",
            &RegistryIndex::default(),
            |ids, allowance| {
                assert!(allowance < LAZY_MERGE_BYTES);
                let blocks: BTreeSet<_> = ids
                    .iter()
                    .map(|id| {
                        assert!(requested.insert(id.clone()), "already fetched {id}");
                        commits
                            .iter()
                            .position(|commit| &commit.graph_commit_id == id)
                            .unwrap()
                            / block_width
                    })
                    .collect();
                let mut used = 0;
                let result = commits
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| blocks.contains(&(index / block_width)))
                    .map(|(_, commit)| {
                        charge_merge_bytes(
                            &mut used,
                            commit.graph_commit_id.len() + commit_bytes(commit),
                            allowance,
                        )?;
                        Ok((commit.graph_commit_id.clone(), Some(commit.clone())))
                    })
                    .collect::<Result<HashMap<_, _>>>();
                std::future::ready(result)
            },
            no_slot,
            LAZY_MERGE_VISITS,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(found, search(commits).base, "block width {block_width}");
        let head_is_base = found
            .as_ref()
            .is_some_and(|base| matches!(base.graph_commit_id.as_str(), "source" | "target"));
        assert!(
            head_is_base || !requested.is_empty(),
            "the oracle must exercise fetched ancestry unless a head is the base"
        );
    }

    #[tokio::test]
    async fn lazy_merge_matches_complete_dags_across_block_boundaries() {
        for seed in 1..=32u64 {
            let mut random = seed;
            let mut commits: Vec<GraphCommit> = Vec::new();
            for index in 0..32usize {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let parent = (index > 0).then(|| random as usize % index);
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let merged = (index > 1 && random & 1 == 0).then(|| random as usize % index);
                let generation = parent
                    .into_iter()
                    .chain(merged)
                    .map(|index| commits[index].generation + 1)
                    .max()
                    .unwrap_or(0);
                let id = match index {
                    30 => "source".to_string(),
                    31 => "target".to_string(),
                    _ => format!("node-{index:02}"),
                };
                commits.push(commit(
                    &id,
                    (index / 3 + 1) as u64,
                    generation,
                    parent.map(|index| commits[index].graph_commit_id.as_str()),
                    merged.map(|index| commits[index].graph_commit_id.as_str()),
                ));
            }
            for width in [1, 4, 16] {
                assert_lazy_matches_complete(&commits, width).await;
            }
        }
        for (a_version, b_version) in [(3, 5), (5, 5)] {
            let commits = [
                commit("a", a_version, 0, None, None),
                commit("b", b_version, 0, None, None),
                commit("source", 6, 1, Some("a"), Some("b")),
                commit("target", 7, 1, Some("b"), Some("a")),
            ];
            assert_lazy_matches_complete(&commits, 1).await;
        }
        let shortcut = [
            commit("root", 1, 0, None, None),
            commit("older", 2, 1, Some("root"), None),
            commit("middle", 3, 2, Some("older"), None),
            commit("newer", 4, 3, Some("middle"), None),
            commit("source-path", 5, 4, Some("newer"), None),
            commit("target-path", 6, 4, Some("newer"), None),
            commit("source", 7, 5, Some("source-path"), Some("older")),
            commit("target", 8, 5, Some("target-path"), Some("older")),
        ];
        assert_lazy_matches_complete(&shortcut, 2).await;
    }

    #[tokio::test]
    async fn lazy_merge_missing_frontiers_and_limits_remain_explicit() {
        let held = [
            commit("base", 1, 1, Some("missing"), None),
            commit("source", 2, 2, Some("base"), None),
            commit("target", 3, 2, Some("base"), None),
        ];
        let get = |id: &str| held.iter().find(|commit| commit.graph_commit_id == id);
        let none = RegistryIndex::default();
        let no_fetch =
            |_: Vec<String>,
             _: usize|
             -> std::future::Ready<Result<HashMap<String, Option<GraphCommit>>>> {
                panic!("`base` is popped with both flags before its parent is examined")
            };
        assert_eq!(
            lazy_merge_base_search(
                &get,
                "source",
                "target",
                &none,
                no_fetch,
                no_slot,
                100,
                LAZY_MERGE_BYTES
            )
            .await
            .unwrap()
            .unwrap()
            .graph_commit_id,
            "base"
        );
        for (visits, bytes) in [(0, LAZY_MERGE_BYTES), (1, LAZY_MERGE_BYTES), (100, 0)] {
            let error = lazy_merge_base_search(
                &get, "source", "target", &none, no_fetch, no_slot, visits, bytes,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("work or memory limit"));
        }
        let tied = [
            commit("base", 1, 0, None, None),
            commit("target", 2, 1, Some("base"), None),
            commit("source", 4, 3, Some("base"), Some("missing")),
        ];
        let get = |id: &str| tied.iter().find(|commit| commit.graph_commit_id == id);
        let error = lazy_merge_base_search(
            &get,
            "source",
            "target",
            &none,
            |_, allowance| {
                assert!(allowance < LAZY_MERGE_BYTES);
                std::future::ready(Ok(HashMap::new()))
            },
            no_slot,
            100,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("missing"),
            "the missing node is bounded at generation 2 and could reach target, so a lookup \
             that finds nothing must be an error, not a miss: {error}"
        );
        let listed_absent = lazy_merge_base_search(
            &get,
            "source",
            "target",
            &none,
            |ids, _| std::future::ready(Ok(ids.into_iter().map(|id| (id, None)).collect())),
            no_slot,
            100,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(
            listed_absent.unwrap().graph_commit_id,
            "base",
            "a merged parent the legacy directory lists as absent is a leaf, as `lineage` holds"
        );
        let error = lazy_merge_base_search(
            &get,
            "source",
            "target",
            &none,
            |_, allowance| {
                assert!(
                    allowance < 4096,
                    "search bookkeeping must be subtracted before fetching"
                );
                let mut used = 0;
                std::future::ready(
                    charge_merge_bytes(&mut used, allowance + 1, allowance)
                        .map(|()| HashMap::new()),
                )
            },
            no_slot,
            100,
            4096,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("work or memory limit"));
    }

    /// Random `hb1` DAGs with a registry run over every block or every other
    /// one: the walk over runs finds the base the complete walk finds, never
    /// fetches a covered commit, and reads a record by slot for the base alone.
    #[tokio::test]
    async fn lazy_merge_over_registry_runs_matches_complete_and_skips_covered_blocks() {
        use omnigraph_core::graph_commit_id::HistoryBlockId;
        for seed in 1..=96u64 {
            let mut random = seed;
            let mut next = || {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                random
            };
            let mut commits: Vec<GraphCommit> = Vec::new();
            let mut placed: Vec<(Ulid, u16)> = Vec::new();
            let mut tips: HashMap<usize, (Ulid, u16)> = HashMap::new();
            for index in 0..48usize {
                let parent = (index > 0).then(|| next() as usize % index);
                let merged = (index > 1 && next() % 3 == 0)
                    .then(|| next() as usize % index)
                    .filter(|merged| Some(*merged) != parent);
                let generation = parent
                    .into_iter()
                    .chain(merged)
                    .map(|at| commits[at].generation + 1)
                    .max()
                    .unwrap_or(0);
                let (block, slot) = match parent.and_then(|at| tips.remove(&at)) {
                    Some((block, slot)) => (block, slot + 1),
                    None => (Ulid::from(index as u128 + 1), 0),
                };
                tips.insert(index, (block, slot));
                placed.push((block, slot));
                let id = HistoryBlockId::new(block, slot, Ulid::from(1_000 + index as u128))
                    .unwrap()
                    .to_string();
                let parent_id = parent.map(|at| commits[at].graph_commit_id.clone());
                let merged_id = merged.map(|at| commits[at].graph_commit_id.clone());
                commits.push(commit(
                    &id,
                    (index / 3 + 1) as u64,
                    generation,
                    parent_id.as_deref(),
                    merged_id.as_deref(),
                ));
            }
            let mut by_block: BTreeMap<Ulid, Vec<usize>> = BTreeMap::new();
            for (index, (block, _)) in placed.iter().enumerate() {
                by_block.entry(*block).or_default().push(index);
            }
            for covered_every in [1usize, 2] {
                let mut registry = LineageRegistry::default();
                for (nth, (block, members)) in by_block.iter().enumerate() {
                    if nth % covered_every != 0 {
                        continue;
                    }
                    let (first, last) = (members[0], *members.last().unwrap());
                    registry.runs.push(BlockRun {
                        block: *block,
                        first_slot: 0,
                        last_slot: placed[last].1,
                        first_generation: commits[first].generation,
                        parent_commit_id: commits[first].parent_commit_id.clone(),
                    });
                    for &member in members {
                        if let Some(merged) = &commits[member].merged_parent_commit_id {
                            registry.merges.push(MergeEdge {
                                block: *block,
                                slot: placed[member].1,
                                generation: commits[member].generation,
                                merged_parent_commit_id: merged.clone(),
                            });
                        }
                    }
                }
                let index = RegistryIndex::new(&registry);
                for (source_at, target_at) in [(46, 47), (47, 40), (20, 45)] {
                    let source = commits[source_at].graph_commit_id.clone();
                    let target = commits[target_at].graph_commit_id.clone();
                    let held = |id: &str| {
                        commits.iter().find(|commit| {
                            commit.graph_commit_id == id && (id == source || id == target)
                        })
                    };
                    let by_id =
                        |id: &str| commits.iter().find(|commit| commit.graph_commit_id == id);
                    let expected = complete_merge_base(&by_id, &source, &target);
                    let mut fetched = 0usize;
                    let mut slot_reads = 0usize;
                    let found = lazy_merge_base_search(
                        &held,
                        &source,
                        &target,
                        &index,
                        |ids, _| {
                            for id in &ids {
                                assert_eq!(
                                    index.node(id),
                                    Node::Id(id.clone()),
                                    "fetched a commit a run covers"
                                );
                            }
                            fetched += ids.len();
                            std::future::ready(Ok(ids
                                .into_iter()
                                .map(|id| {
                                    let record = by_id(&id).cloned();
                                    (id, record)
                                })
                                .collect()))
                        },
                        |block, slot, _| {
                            slot_reads += 1;
                            std::future::ready(Ok(commits
                                .iter()
                                .zip(&placed)
                                .find(|(_, at)| **at == (block, slot))
                                .map(|(commit, _)| commit.clone())))
                        },
                        LAZY_MERGE_VISITS,
                        LAZY_MERGE_BYTES,
                    )
                    .await
                    .unwrap();
                    assert_eq!(
                        found, expected,
                        "seed {seed}, every {covered_every}th block covered, heads \
                         {source_at} and {target_at}"
                    );
                    if covered_every == 1 {
                        assert_eq!(fetched, 0, "seed {seed}: every commit is covered");
                        assert!(slot_reads <= 2, "seed {seed}: {slot_reads} slot reads");
                    }
                }
            }
        }
    }

    /// A head covered by a run crosses the run in one step; an edge into the
    /// crossed slots that pops later inherits what the crossing carried, so
    /// the base is the slot the edge names and its record is the one read.
    #[tokio::test]
    async fn lazy_merge_edge_into_a_crossed_run_inherits_the_crossing() {
        use omnigraph_core::graph_commit_id::HistoryBlockId;
        let block = Ulid::from(7u128);
        let id = |slot: u16| {
            HistoryBlockId::new(block, slot, Ulid::from(100 + u128::from(slot)))
                .unwrap()
                .to_string()
        };
        let mut commits: Vec<GraphCommit> = (0..10u16)
            .map(|slot| {
                let parent = (slot > 0).then(|| id(slot - 1));
                commit(&id(slot), 1, u64::from(slot), parent.as_deref(), None)
            })
            .collect();
        commits.push(commit("source", 2, 7, Some(&id(3)), None));
        let registry = LineageRegistry {
            runs: vec![BlockRun {
                block,
                first_slot: 0,
                last_slot: 9,
                first_generation: 0,
                parent_commit_id: None,
            }],
            merges: Vec::new(),
        };
        let index = RegistryIndex::new(&registry);
        let target = id(9);
        let held = |wanted: &str| {
            commits.iter().find(|commit| {
                commit.graph_commit_id == wanted && (wanted == "source" || wanted == target)
            })
        };
        let mut slot_reads = Vec::new();
        let found = lazy_merge_base_search(
            &held,
            "source",
            &target,
            &index,
            |ids: Vec<String>,
             _: usize|
             -> std::future::Ready<Result<HashMap<String, Option<GraphCommit>>>> {
                panic!("every parent is covered by the run: {ids:?}")
            },
            |read_block, slot, _| {
                assert_eq!(read_block, block);
                slot_reads.push(slot);
                let record = commits
                    .iter()
                    .find(|commit| commit.graph_commit_id == id(slot))
                    .cloned();
                std::future::ready(Ok(record))
            },
            LAZY_MERGE_VISITS,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(found.map(|base| base.graph_commit_id), Some(id(3)));
        assert_eq!(slot_reads, [3]);
    }
    /// Private candidate allocations are bounded across fetched and already-held records.
    #[tokio::test]
    async fn lazy_merge_tied_candidates_respect_total_record_budget() {
        let id = |block: u128| {
            omnigraph_core::graph_commit_id::HistoryBlockId::new(
                Ulid::from(block),
                0,
                Ulid::from(block + 100),
            )
            .unwrap()
            .to_string()
        };
        let mut a = commit(&id(1), 2, 1, Some("root"), None);
        let mut b = commit(&id(2), 3, 1, Some("root"), None);
        a.actor_id = Some("a".repeat(6000));
        b.actor_id = Some("b".repeat(6000));
        let middle = commit(
            "middle",
            4,
            2,
            Some(&a.graph_commit_id),
            Some(&b.graph_commit_id),
        );
        let source = commit("source", 5, 3, Some("middle"), None);
        let target = commit(
            "target",
            6,
            2,
            Some(&b.graph_commit_id),
            Some(&a.graph_commit_id),
        );
        let registry = LineageRegistry {
            runs: [1u128, 2]
                .into_iter()
                .map(|block| BlockRun {
                    block: Ulid::from(block),
                    first_slot: 0,
                    last_slot: 0,
                    first_generation: 1,
                    parent_commit_id: Some("root".to_string()),
                })
                .collect(),
            merges: Vec::new(),
        };
        let index = RegistryIndex::new(&registry);
        assert!(commit_bytes(&a) + commit_bytes(&b) > 10_000);

        for hold_candidates in [false, true] {
            for limit in [LAZY_MERGE_BYTES, 10_000] {
                let get = |wanted: &str| {
                    [&source, &target]
                        .into_iter()
                        .chain(hold_candidates.then_some(&a))
                        .chain(hold_candidates.then_some(&b))
                        .find(|record| record.graph_commit_id == wanted)
                };
                assert!(
                    partial_merge_base_search(&get, "source", "target", 100).is_none(),
                    "the missing middle must force the lazy path"
                );
                let result = lazy_merge_base_search(
                    &get,
                    "source",
                    "target",
                    &index,
                    |wanted, allowance| {
                        assert_eq!(wanted, ["middle"]);
                        assert!(commit_bytes(&middle) <= allowance);
                        std::future::ready(Ok(HashMap::from([(
                            middle.graph_commit_id.clone(),
                            Some(middle.clone()),
                        )])))
                    },
                    |block, slot, allowance| {
                        assert!(!hold_candidates);
                        assert_eq!(slot, 0);
                        let record = if block == Ulid::from(1u128) {
                            a.clone()
                        } else {
                            assert_eq!(block, Ulid::from(2u128));
                            b.clone()
                        };
                        let result = if commit_bytes(&record) > allowance {
                            Err(merge_search_limit())
                        } else {
                            Ok(Some(record))
                        };
                        std::future::ready(result)
                    },
                    100,
                    limit,
                )
                .await;
                if limit == 10_000 {
                    let error = result.unwrap_err();
                    assert!(error.to_string().contains("work or memory limit"));
                } else {
                    assert_eq!(result.unwrap().unwrap().graph_commit_id, a.graph_commit_id);
                }
            }
        }
    }

    /// A covered candidate must agree with its held or incidentally fetched record.
    #[tokio::test]
    async fn lazy_merge_checks_registry_against_held_candidate() {
        let block = Ulid::from(7u128);
        let id = |slot: u16| {
            omnigraph_core::graph_commit_id::HistoryBlockId::new(
                block,
                slot,
                Ulid::from(100 + u128::from(slot)),
            )
            .unwrap()
            .to_string()
        };
        let base = commit(&id(0), 2, 1, Some("root"), None);
        let middle = commit(&id(1), 3, 2, Some(&base.graph_commit_id), None);
        let source2 = commit("source2", 4, 3, Some(&middle.graph_commit_id), None);
        let source = commit("source", 5, 4, Some("source2"), None);
        let target = commit("target", 6, 2, Some(&base.graph_commit_id), None);

        for hold_base in [false, true] {
            for recorded_generation in [1, 0] {
                let registry = LineageRegistry {
                    runs: vec![BlockRun {
                        block,
                        first_slot: 0,
                        last_slot: 0,
                        first_generation: recorded_generation,
                        parent_commit_id: Some("root".to_string()),
                    }],
                    merges: Vec::new(),
                };
                let index = RegistryIndex::new(&registry);
                let get = |wanted: &str| {
                    [&source, &source2, &target]
                        .into_iter()
                        .chain(hold_base.then_some(&base))
                        .find(|record| record.graph_commit_id == wanted)
                };
                assert!(partial_merge_base_search(&get, "source", "target", 100).is_none());
                let result = lazy_merge_base_search(
                    &get,
                    "source",
                    "target",
                    &index,
                    |wanted, _| {
                        assert_eq!(wanted, std::slice::from_ref(&middle.graph_commit_id));
                        std::future::ready(Ok(HashMap::from([
                            (middle.graph_commit_id.clone(), Some(middle.clone())),
                            (base.graph_commit_id.clone(), Some(base.clone())),
                        ])))
                    },
                    |_, _, _| -> std::future::Ready<Result<Option<GraphCommit>>> {
                        panic!("the selected base is already held or was returned with middle")
                    },
                    100,
                    LAZY_MERGE_BYTES,
                )
                .await;
                if recorded_generation == 0 {
                    let error = result.unwrap_err();
                    assert!(error.to_string().contains("block registry"));
                    assert!(error.to_string().contains("record generation 1"));
                } else {
                    assert_eq!(
                        result.unwrap().unwrap().graph_commit_id,
                        base.graph_commit_id
                    );
                }
            }
        }
    }

    /// A fetched parked record occupies both the loaded map and the queued walk record.
    #[tokio::test]
    async fn lazy_merge_accounts_for_both_owners_of_an_admitted_record() {
        let base = commit("base", 2, 1, Some("root"), None);
        let mut middle = commit("middle", 3, 2, Some("base"), None);
        middle.actor_id = Some("m".repeat(6000));
        let source = commit("source", 4, 3, Some("middle"), None);
        let target = commit("target", 5, 2, Some("base"), None);
        let index = RegistryIndex::default();
        let get = |wanted: &str| {
            [&source, &target, &base]
                .into_iter()
                .find(|record| record.graph_commit_id == wanted)
        };
        assert!(commit_bytes(&middle) * 2 > 10_000);
        assert!(partial_merge_base_search(&get, "source", "target", 100).is_none());

        for limit in [LAZY_MERGE_BYTES, 10_000] {
            let result = lazy_merge_base_search(
            &get,
            "source",
            "target",
            &index,
            |wanted, allowance| {
                assert_eq!(wanted, ["middle"]);
                assert!(
                    middle.graph_commit_id.len() + commit_bytes(&middle) <= allowance,
                    "one decoded record fits; retaining both owned copies exceeds the small budget"
                );
                std::future::ready(Ok(HashMap::from([(
                    middle.graph_commit_id.clone(),
                    Some(middle.clone()),
                )])))
            },
            no_slot,
            100,
            limit,
        )
        .await;
            if limit == 10_000 {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("work or memory limit"));
            } else {
                assert_eq!(result.unwrap().unwrap().graph_commit_id, "base");
            }
        }
    }
}
