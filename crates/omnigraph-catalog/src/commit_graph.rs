use std::collections::{BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use omnigraph_core::graph_commit_id::commit_id_answers;

use crate::error::{OmniError, Result};
use crate::history;

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
}

impl HistoryCache {
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
        }
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
        let (head, buffer) =
            crate::ManifestCoordinator::read_held_at(root_uri, branch, &session).await?;
        Ok(Self::from_head(
            root_uri,
            session,
            head,
            buffer.commits(),
            HistoryCache::default(),
        ))
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
    /// Fetched blocks are never inserted in the parent-closed cache used by
    /// full lineage readers; the objects they came from stay in
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
            |ids, remaining_bytes| async move {
                let ids: Vec<_> = ids.iter().map(String::as_str).collect();
                Ok(history::read_lineage_of_bounded_in(
                    &self.root_uri,
                    &self.session,
                    self.history.objects(),
                    &ids,
                    remaining_bytes,
                )
                .await?
                .into_iter()
                .map(|(id, row)| (id, graph_commit_from_manifest_row(row)))
                .collect())
            },
            LAZY_MERGE_VISITS,
            LAZY_MERGE_BYTES,
        )
        .await
    }

    /// The lineage of the head. Reads the lineage columns of `__history`
    /// unless the head and the buffer name no parent outside them, or the
    /// cache holds every such parent, which its closure under parents makes
    /// the whole ancestry.
    pub async fn lineage(&self) -> Result<Lineage> {
        let outside: Vec<&str> = self.held.outside_parents().collect();
        let holds = |settled: &SettledCommits| outside.iter().all(|id| settled.contains_key(*id));
        let settled = match self.history.held().filter(holds) {
            Some(settled) => settled,
            None if outside.is_empty() => {
                let settled = SettledCommits::default();
                self.history.replace(Arc::clone(&settled));
                settled
            }
            None => {
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
        if let Some(parent) = outside.iter().find(|id| !settled.contains_key(**id)) {
            return Err(missing_parent(parent));
        }
        Ok(Lineage {
            held: self.held.clone(),
            settled,
        })
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

/// Persistent breadth-first walks: one discovery per side, with bounded
/// batches at the current frontier. Missing records are errors only when
/// their d+1 lower bound can still tie or beat the selected score.
async fn lazy_merge_base_search<'a, F, Fut>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source: &str,
    target: &str,
    mut fetch: F,
    visit_limit: usize,
    byte_limit: usize,
) -> Result<Option<GraphCommit>>
where
    F: FnMut(Vec<String>, usize) -> Fut,
    Fut: Future<Output = Result<HashMap<String, GraphCommit>>>,
{
    if get(source).is_none() || get(target).is_none() {
        return Err(OmniError::manifest_internal(
            "captured merge heads are unavailable",
        ));
    }
    let mut bytes = 0;
    charge_merge_bytes(&mut bytes, source.len() + target.len() + 256, byte_limit)?;
    let mut queues = [
        VecDeque::from([(source.to_string(), 0u64)]),
        VecDeque::from([(target.to_string(), 0u64)]),
    ];
    let mut distances = [HashMap::<String, u64>::new(), HashMap::<String, u64>::new()];
    let mut loaded = HashMap::<String, GraphCommit>::new();
    let mut absent = BTreeSet::new();
    let mut missing: Option<(u64, String)> = None;
    let mut best: Option<(u64, GraphCommit)> = None;
    let mut visits = 0;
    while let Some((side, distance)) = (0..2)
        .filter_map(|side| queues[side].front().map(|(_, d)| (side, *d)))
        .min_by_key(|&(side, d)| (d, side))
    {
        if best.as_ref().is_some_and(|(score, _)| distance > *score) {
            break;
        }
        let (id, _) = queues[side]
            .pop_front()
            .expect("selected nonempty merge frontier");
        if distances[side].contains_key(&id) {
            continue;
        }
        if visits == visit_limit {
            return Err(merge_search_limit());
        }
        visits += 1;
        charge_merge_bytes(&mut bytes, id.len() + 128, byte_limit)?;
        distances[side].insert(id.clone(), distance);

        if get(&id).is_none() && !loaded.contains_key(&id) {
            let nothing_behind_the_missing_node_scores_below_distance_plus_one =
                best.as_ref().is_some_and(|(score, _)| distance >= *score);
            if nothing_behind_the_missing_node_scores_below_distance_plus_one {
                continue;
            }
            if !absent.contains(&id) {
                let mut requested = BTreeSet::from([id.clone()]);
                for queue in &queues {
                    for (queued, _) in queue.iter().take_while(|(_, d)| *d == distance) {
                        if requested.len() < LAZY_MERGE_FETCH_IDS
                            && get(queued).is_none()
                            && !loaded.contains_key(queued)
                            && !absent.contains(queued)
                        {
                            requested.insert(queued.clone());
                        }
                    }
                }
                let fetched = fetch(
                    requested.iter().cloned().collect(),
                    byte_limit.saturating_sub(bytes),
                )
                .await?;
                for (key, commit) in fetched {
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
                    } else {
                        charge_merge_bytes(
                            &mut bytes,
                            key.len() + commit_bytes(&commit),
                            byte_limit,
                        )?;
                        loaded.insert(key, commit);
                    }
                }
                for requested in requested {
                    if get(&requested).is_none() && !loaded.contains_key(&requested) {
                        charge_merge_bytes(&mut bytes, requested.len() + 128, byte_limit)?;
                        absent.insert(requested);
                    }
                }
            }
        }
        let Some(commit) = get(&id).or_else(|| loaded.get(&id)) else {
            if missing.as_ref().is_none_or(|(d, _)| distance < *d) {
                missing = Some((distance, id));
            }
            continue;
        };
        if let Some(other_distance) = distances[1 - side].get(&id) {
            let score = distance + other_distance;
            let key = (
                score,
                u64::MAX - commit.graph_manifest_version,
                commit.graph_commit_id.as_str(),
            );
            if best.as_ref().is_none_or(|(known_score, known)| {
                key < (
                    *known_score,
                    u64::MAX - known.graph_manifest_version,
                    known.graph_commit_id.as_str(),
                )
            }) {
                charge_merge_bytes(&mut bytes, commit_bytes(commit), byte_limit)?;
                best = Some((score, commit.clone()));
            }
        }
        for parent in [&commit.parent_commit_id, &commit.merged_parent_commit_id]
            .into_iter()
            .flatten()
        {
            charge_merge_bytes(&mut bytes, parent.len() + 128, byte_limit)?;
            queues[side].push_back((parent.clone(), distance + 1));
        }
    }
    if let Some((distance, id)) = missing
        && best.as_ref().is_none_or(|(score, _)| distance < *score)
    {
        return Err(missing_parent(&id));
    }
    Ok(best.map(|(_, commit)| commit))
}

/// Search both heads breadth first, retaining every possible score tie.
/// A missing node cannot be either known head; any contender it hides has
/// total distance at least its frontier distance plus one.
fn partial_merge_base_search<'a>(
    get: &impl Fn(&str) -> Option<&'a GraphCommit>,
    source_commit_id: &str,
    target_commit_id: &str,
    visit_budget: usize,
) -> Option<GraphCommit> {
    let mut queues = [
        VecDeque::from([(source_commit_id.to_string(), 0u64)]),
        VecDeque::from([(target_commit_id.to_string(), 0u64)]),
    ];
    let mut distances = [HashMap::new(), HashMap::new()];
    let mut missing: [Option<u64>; 2] = [None, None];
    let mut best: Option<(u64, GraphCommit)> = None;
    let mut visits = 0;

    while let Some((side, distance)) = (0..2)
        .filter_map(|side| queues[side].front().map(|(_, distance)| (side, *distance)))
        .min_by_key(|&(side, distance)| (distance, side))
    {
        if best.as_ref().is_some_and(|(score, _)| distance > *score) {
            break;
        }
        let (id, _) = queues[side]
            .pop_front()
            .expect("selected a nonempty frontier");
        if distances[side].contains_key(&id) {
            continue;
        }
        if visits == visit_budget {
            return None;
        }
        visits += 1;
        distances[side].insert(id.clone(), distance);
        let Some(commit) = get(&id) else {
            missing[side] = Some(missing[side].map_or(distance, |known| known.min(distance)));
            continue;
        };
        if let Some(other_distance) = distances[1 - side].get(&id) {
            let score = distance + other_distance;
            let key = (
                score,
                u64::MAX - commit.graph_manifest_version,
                commit.graph_commit_id.as_str(),
            );
            if best.as_ref().is_none_or(|(known_score, known)| {
                key < (
                    *known_score,
                    u64::MAX - known.graph_manifest_version,
                    known.graph_commit_id.as_str(),
                )
            }) {
                best = Some((score, commit.clone()));
            }
        }
        for parent in [&commit.parent_commit_id, &commit.merged_parent_commit_id]
            .into_iter()
            .flatten()
        {
            queues[side].push_back((parent.clone(), distance + 1));
        }
    }

    let (score, commit) = best?;
    if missing
        .into_iter()
        .flatten()
        .any(|distance| distance < score)
    {
        return None;
    }
    Some(commit)
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
    let base = source_distances
        .iter()
        .filter_map(|(id, source_distance)| {
            target_distances.get(id).and_then(|target_distance| {
                get(id).map(|commit| {
                    (
                        (
                            *source_distance + *target_distance,
                            u64::MAX - commit.graph_manifest_version,
                            commit.graph_commit_id.clone(),
                        ),
                        commit.clone(),
                    )
                })
            })
        })
        .min_by(|(left, _), (right, _)| left.cmp(right))
        .map(|(_, commit)| commit);
    MergeBaseSearch {
        base,
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
        }
    }

    fn search(commits: &[GraphCommit]) -> MergeBaseSearch {
        let by_id: HashMap<_, _> = commits
            .iter()
            .map(|commit| (commit.graph_commit_id.as_str(), commit))
            .collect();
        merge_base_search(&|id| by_id.get(id).copied(), "source", "target")
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
    fn merge_base_distance_ties_use_version_then_commit_id() {
        for (a_version, b_version, expected) in [(3, 5, "b"), (5, 5, "a")] {
            let commits = [
                commit("root", 1, 0, None, None),
                commit("a", a_version, 1, Some("root"), None),
                commit("b", b_version, 1, Some("root"), None),
                commit("source", 6, 2, Some("a"), Some("b")),
                commit("target", 7, 2, Some("b"), Some("a")),
            ];
            let found = search(&commits);
            assert!(!found.needs_import());
            assert_eq!(found.base.unwrap().graph_commit_id, expected);
            assert_eq!(
                bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                    .unwrap()
                    .graph_commit_id,
                expected
            );
        }
    }

    #[test]
    fn merge_base_second_parent_distance_can_beat_newer_generation() {
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
        assert_eq!(found.base.unwrap().graph_commit_id, "older");
        assert_eq!(
            bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "older"
        );
    }

    #[test]
    fn merge_base_missing_frontier_can_hide_better_second_parent_ancestor() {
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
        assert_eq!(search(&commits).base.unwrap().graph_commit_id, "base");

        let partial = search(&commits[2..]);
        assert_eq!(partial.base.as_ref().unwrap().graph_commit_id, "b");
        assert!(
            !partial.needs_import(),
            "the existing import frontier stops at known common commits; it is not a proof \
             that a partial capture selected the same base as complete history"
        );
        assert!(bounded_search(&commits[2..], MERGE_BASE_VISIT_BUDGET).is_none());

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
    fn merge_base_partial_proof_uses_missing_score_bound_and_bounded_visits() {
        let commits = [
            commit("older", 3, 2, Some("unheld"), None),
            commit("base", 4, 3, Some("older"), None),
            commit("source", 5, 4, Some("base"), None),
            commit("target", 6, 4, Some("base"), None),
        ];
        assert_eq!(
            bounded_search(&commits, MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "base",
            "the missing record is at distance 3, beyond the winning score 2"
        );
        assert_eq!(
            bounded_search(&commits[1..], MERGE_BASE_VISIT_BUDGET)
                .unwrap()
                .graph_commit_id,
            "base",
            "a missing record at distance 2 is neither known head; its score and any \
             concealed ancestor's score are at least 3, beyond the winning score 2"
        );
        for visit_budget in [0, 1, 5] {
            assert!(
                bounded_search(&commits, visit_budget).is_none(),
                "budget {visit_budget} cannot finish the proof even after finding a candidate"
            );
        }
    }

    #[test]
    fn merge_base_missing_score_ties_still_require_full_history() {
        for (hidden_id, hidden_version, base_id, base_version) in
            [("hidden", 5, "base", 4), ("a", 4, "b", 4)]
        {
            let commits = [
                commit(hidden_id, hidden_version, 0, None, None),
                commit(base_id, base_version, 0, None, None),
                commit("source", 6, 1, Some(base_id), Some(hidden_id)),
                commit("target", 7, 1, Some(base_id), Some(hidden_id)),
            ];
            assert_eq!(
                search(&commits).base.unwrap().graph_commit_id,
                hidden_id,
                "the missing record ties the held base's total distance and wins the \
                 remaining component of the ordering, version or id"
            );
            assert_eq!(search(&commits[1..]).base.unwrap().graph_commit_id, base_id);
            assert!(
                bounded_search(&commits[1..], MERGE_BASE_VISIT_BUDGET).is_none(),
                "missing distance 1 plus the other side's minimum distance 1 ties score 2"
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
            "the ancestor beyond the missing record is the other known head, at distance \
             zero on its side: one extra edge is the only universally valid improvement \
             to the missing-distance lower bound"
        );
        assert_eq!(search(&commits[1..]).base.unwrap().graph_commit_id, "base");
        assert!(
            bounded_search(&commits[1..], MERGE_BASE_VISIT_BUDGET).is_none(),
            "the concealed route to the opposite head can tie score 2 and win by version"
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
                        Ok((commit.graph_commit_id.clone(), commit.clone()))
                    })
                    .collect::<Result<HashMap<_, _>>>();
                std::future::ready(result)
            },
            LAZY_MERGE_VISITS,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(found, search(commits).base, "block width {block_width}");
        assert!(
            !requested.is_empty(),
            "the oracle must exercise fetched ancestry"
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
        let no_fetch = |_: Vec<String>,
                        _: usize|
         -> std::future::Ready<Result<HashMap<String, GraphCommit>>> {
            panic!("missing distance 2 cannot affect the winning total distance 2")
        };
        assert_eq!(
            lazy_merge_base_search(&get, "source", "target", no_fetch, 100, LAZY_MERGE_BYTES)
                .await
                .unwrap()
                .unwrap()
                .graph_commit_id,
            "base"
        );
        for (visits, bytes) in [(0, LAZY_MERGE_BYTES), (1, LAZY_MERGE_BYTES), (100, 0)] {
            let error = lazy_merge_base_search(&get, "source", "target", no_fetch, visits, bytes)
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
            |_, allowance| {
                assert!(allowance < LAZY_MERGE_BYTES);
                std::future::ready(Ok(HashMap::new()))
            },
            100,
            LAZY_MERGE_BYTES,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("missing"),
            "the missing node could reach target in one edge, tying the held base's score \
             and winning by version, so its lookup must be an error, not a miss: {error}"
        );
        let error = lazy_merge_base_search(
            &get,
            "source",
            "target",
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
            100,
            4096,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("work or memory limit"));
    }
}
