use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct GraphCommit {
    pub graph_commit_id: String,
    pub graph_branch: Option<String>,
    pub graph_manifest_version: u64,
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
pub(crate) struct FirstParentEdge {
    pub(crate) parent: GraphCommit,
    pub(crate) child: GraphCommit,
}

/// A pure projection of the graph lineage that lives in `__manifest`
/// (`graph_commit` + `graph_head` rows, RFC-013 Phase 7). It opens NO Lance
/// dataset (Phase B retired `_graph_commits.lance` / `_graph_commit_actors.lance`):
/// the in-memory cache is built from `ManifestCoordinator::read_graph_lineage_at`,
/// and branch authority lives entirely in `__manifest`. Reads
/// (`head_commit`/`load_commits`/`get_commit`/`merge_base_search`) and writes
/// (`insert_committed`, fed by the coordinator's manifest publish CAS) both work
/// off this projection.
pub struct CommitGraph {
    root_uri: String,
    active_branch: Option<String>,
    commit_by_id: Arc<HashMap<String, GraphCommit>>,
    head_commit: Option<GraphCommit>,
}

/// Immutable view handed from authority capture to merge-base selection.
/// Cloning it is O(1); coordinator updates use copy-on-write, so an authority
/// capture cannot change underneath its merge-base walk.
#[derive(Clone)]
pub(crate) struct CommitGraphSnapshot {
    commit_by_id: Arc<HashMap<String, GraphCommit>>,
}

/// One merge-base walk over the two branch-local maps plus imported records;
/// each `unresolved_*` side lists the ids it reaches, held by no map, before
/// meeting a commit the other side also holds.
pub(crate) struct MergeBaseSearch {
    pub(crate) base: Option<GraphCommit>,
    pub(crate) unresolved_source: Vec<String>,
    pub(crate) unresolved_target: Vec<String>,
}

impl CommitGraph {
    /// Seed the in-memory cache for a fresh graph from the `__manifest` genesis
    /// lineage (folded into the manifest init write — RFC-013 Phase 7). No Lance
    /// dataset is created or opened — the projection sees genesis identically to
    /// [`open`].
    pub async fn init(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (commit_by_id, head_commit) = load_commit_cache_from_manifest(root, None).await?;
        Ok(Self {
            root_uri: root.to_string(),
            active_branch: None,
            commit_by_id: Arc::new(commit_by_id),
            head_commit,
        })
    }

    /// Build the lineage projection from rows decoded alongside one coherent
    /// manifest-state scan.
    pub(crate) fn from_manifest_rows(
        root_uri: &str,
        active_branch: Option<&str>,
        rows: Vec<crate::db::manifest::GraphLineageRow>,
    ) -> Self {
        let (commit_by_id, head_commit) = build_commit_cache(rows);
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            active_branch: active_branch.map(str::to_string),
            commit_by_id: Arc::new(commit_by_id),
            head_commit,
        }
    }

    /// Replace this derived cache from rows captured with the coordinator's
    /// newly-refreshed manifest state.
    pub(crate) fn replace_from_manifest_rows(
        &mut self,
        rows: Vec<crate::db::manifest::GraphLineageRow>,
    ) {
        let (commit_by_id, head_commit) = build_commit_cache(rows);
        self.commit_by_id = Arc::new(commit_by_id);
        self.head_commit = head_commit;
    }

    /// Extend the cache with lineage rows decoded from newly appended
    /// manifest fragments. The head reduction is identical to a full rebuild,
    /// but no historical map or row vector is cloned.
    pub(crate) fn append_manifest_rows(&mut self, rows: Vec<crate::db::manifest::GraphLineageRow>) {
        let commits = Arc::make_mut(&mut self.commit_by_id);
        for row in rows {
            let commit = graph_commit_from_manifest_row(row);
            if should_replace_head(self.head_commit.as_ref(), &commit) {
                self.head_commit = Some(commit.clone());
            }
            commits.insert(commit.graph_commit_id.clone(), commit);
        }
    }

    pub(crate) fn snapshot(&self) -> CommitGraphSnapshot {
        CommitGraphSnapshot {
            commit_by_id: Arc::clone(&self.commit_by_id),
        }
    }

    /// Insert a just-published commit into the in-memory cache (RFC-013 Phase 7).
    /// The durable write already happened in the manifest publish CAS; this only
    /// keeps the cache consistent for same-handle reads, with no storage I/O.
    /// Head selection matches the manifest-sourced load (`should_replace_head`).
    pub fn insert_committed(&mut self, commit: GraphCommit) {
        debug_assert_eq!(
            commit.graph_branch.as_deref(),
            self.active_branch.as_deref(),
            "published lineage must target the commit graph's active branch"
        );
        if should_replace_head(self.head_commit.as_ref(), &commit) {
            self.head_commit = Some(commit.clone());
        }
        Arc::make_mut(&mut self.commit_by_id).insert(commit.graph_commit_id.clone(), commit);
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (commit_by_id, head_commit) = load_commit_cache_for_branch(root, None).await?;
        Ok(Self {
            root_uri: root.to_string(),
            active_branch: None,
            commit_by_id: Arc::new(commit_by_id),
            head_commit,
        })
    }

    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        // `load_commit_cache_for_branch` opens the branch's `__manifest` (the
        // authoritative table), so a truly absent branch fails loudly here.
        let (commit_by_id, head_commit) = load_commit_cache_for_branch(root, Some(branch)).await?;
        Ok(Self {
            root_uri: root.to_string(),
            active_branch: Some(branch.to_string()),
            commit_by_id: Arc::new(commit_by_id),
            head_commit,
        })
    }

    pub async fn refresh(&mut self) -> Result<()> {
        let (commit_by_id, head_commit) =
            load_commit_cache_for_branch(&self.root_uri, self.active_branch.as_deref()).await?;
        self.commit_by_id = Arc::new(commit_by_id);
        self.head_commit = head_commit;
        Ok(())
    }

    pub async fn head_commit(&self) -> Result<Option<GraphCommit>> {
        Ok(self.head_commit.clone())
    }

    pub async fn head_commit_id(&self) -> Result<Option<String>> {
        Ok(self.head_commit().await?.map(|c| c.graph_commit_id))
    }

    pub async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        let mut commits = self.commit_by_id.values().cloned().collect::<Vec<_>>();
        commits.sort_by(|a, b| a.lineage_key().cmp(&b.lineage_key()));
        Ok(commits)
    }

    /// The maximal commit (by [`GraphCommit::lineage_key`]) satisfying `pred`.
    /// Callers wanting "the latest X" use this instead of consuming
    /// `load_commits` positionally, so no caller couples to iteration
    /// direction.
    pub(crate) fn latest_commit_matching(
        &self,
        pred: impl Fn(&GraphCommit) -> bool,
    ) -> Option<GraphCommit> {
        self.commit_by_id
            .values()
            .filter(|commit| pred(commit))
            .max_by(|a, b| a.lineage_key().cmp(&b.lineage_key()))
            .cloned()
    }

    pub fn get_commit(&self, commit_id: &str) -> Option<GraphCommit> {
        self.commit_by_id.get(commit_id).cloned()
    }

    /// The walk behind `Omnigraph::resolve_merge_base`; `imported` holds the
    /// records read from other branches for ids the two branch-local maps lack.
    pub(crate) fn merge_base_search(
        source: &CommitGraphSnapshot,
        target: &CommitGraphSnapshot,
        imported: &HashMap<String, GraphCommit>,
        source_commit_id: &str,
        target_commit_id: &str,
    ) -> MergeBaseSearch {
        merge_base_from_maps(
            &source.commit_by_id,
            &target.commit_by_id,
            imported,
            source_commit_id,
            target_commit_id,
        )
    }
}

/// Build the in-memory commit cache for `branch` from the `__manifest`
/// graph-lineage projection (RFC-013 Phase 7) — the single source of lineage on a
/// v4 graph. Sub-v4 graphs are refused at open (`refuse_if_stamp_unsupported`),
/// so there is no legacy `_graph_commits.lance` fallback.
async fn load_commit_cache_for_branch(
    root_uri: &str,
    branch: Option<&str>,
) -> Result<(HashMap<String, GraphCommit>, Option<GraphCommit>)> {
    load_commit_cache_from_manifest(root_uri, branch).await
}

/// Build the in-memory commit cache from the `__manifest` graph-lineage
/// projection (RFC-013 step 4). The lineage rows carry the actor inline, so no
/// separate actor-table read is needed. Head selection (`should_replace_head`)
/// is the [`GraphCommit::lineage_key`] maximum — the same total order every
/// ordered lineage view derives from.
async fn load_commit_cache_from_manifest(
    root_uri: &str,
    branch: Option<&str>,
) -> Result<(HashMap<String, GraphCommit>, Option<GraphCommit>)> {
    let (rows, _) =
        crate::db::manifest::ManifestCoordinator::read_graph_lineage_at(root_uri, branch).await?;
    Ok(build_commit_cache(rows))
}

fn build_commit_cache(
    rows: Vec<crate::db::manifest::GraphLineageRow>,
) -> (HashMap<String, GraphCommit>, Option<GraphCommit>) {
    let mut commit_by_id = HashMap::with_capacity(rows.len());
    let mut head_commit = None;
    for row in rows {
        let commit = graph_commit_from_manifest_row(row);
        if should_replace_head(head_commit.as_ref(), &commit) {
            head_commit = Some(commit.clone());
        }
        commit_by_id.insert(commit.graph_commit_id.clone(), commit);
    }
    (commit_by_id, head_commit)
}

pub(crate) fn graph_commit_from_manifest_row(
    row: crate::db::manifest::GraphLineageRow,
) -> GraphCommit {
    GraphCommit {
        graph_commit_id: row.graph_commit_id,
        graph_branch: row.graph_branch,
        graph_manifest_version: row.graph_manifest_version,
        parent_commit_id: row.parent_commit_id,
        merged_parent_commit_id: row.merged_parent_commit_id,
        actor_id: row.actor_id,
        created_at: row.created_at,
    }
}

fn merge_base_from_maps(
    source_commits: &HashMap<String, GraphCommit>,
    target_commits: &HashMap<String, GraphCommit>,
    imported: &HashMap<String, GraphCommit>,
    source_commit_id: &str,
    target_commit_id: &str,
) -> MergeBaseSearch {
    let get = |id: &str| {
        source_commits
            .get(id)
            .or_else(|| target_commits.get(id))
            .or_else(|| imported.get(id))
    };
    if get(source_commit_id).is_none() || get(target_commit_id).is_none() {
        return MergeBaseSearch {
            base: None,
            unresolved_source: Vec::new(),
            unresolved_target: Vec::new(),
        };
    }

    let mut full_walk_unresolved = BTreeSet::new();
    let source_distances = ancestor_distances_from(
        source_commit_id,
        &get,
        &|_| false,
        &mut full_walk_unresolved,
    );
    let target_distances = ancestor_distances_from(
        target_commit_id,
        &get,
        &|_| false,
        &mut full_walk_unresolved,
    );
    let mut unresolved_source = BTreeSet::new();
    ancestor_distances_from(
        source_commit_id,
        &get,
        &|id| target_distances.contains_key(id),
        &mut unresolved_source,
    );
    let mut unresolved_target = BTreeSet::new();
    ancestor_distances_from(
        target_commit_id,
        &get,
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
                        ),
                        commit.clone(),
                    )
                })
            })
        })
        .min_by_key(|(score, _)| *score)
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

fn should_replace_head(current: Option<&GraphCommit>, candidate: &GraphCommit) -> bool {
    current.is_none_or(|existing| candidate.lineage_key() > existing.lineage_key())
}
