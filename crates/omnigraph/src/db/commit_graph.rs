use std::collections::{HashMap, VecDeque};

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
/// (`head_commit`/`load_commits`/`get_commit`/`merge_base`) and writes
/// (`insert_committed`, fed by the coordinator's manifest publish CAS) both work
/// off this projection.
pub struct CommitGraph {
    root_uri: String,
    active_branch: Option<String>,
    commit_by_id: HashMap<String, GraphCommit>,
    head_commit: Option<GraphCommit>,
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
            commit_by_id,
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
            commit_by_id,
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
        self.commit_by_id = commit_by_id;
        self.head_commit = head_commit;
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
        self.commit_by_id
            .insert(commit.graph_commit_id.clone(), commit);
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let (commit_by_id, head_commit) = load_commit_cache_for_branch(root, None).await?;
        Ok(Self {
            root_uri: root.to_string(),
            active_branch: None,
            commit_by_id,
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
            commit_by_id,
            head_commit,
        })
    }

    pub async fn refresh(&mut self) -> Result<()> {
        let (commit_by_id, head_commit) =
            load_commit_cache_for_branch(&self.root_uri, self.active_branch.as_deref()).await?;
        self.commit_by_id = commit_by_id;
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

    pub async fn merge_base(
        root_uri: &str,
        source_branch: Option<&str>,
        target_branch: Option<&str>,
    ) -> Result<Option<GraphCommit>> {
        let source = open_for_branch(root_uri, source_branch).await?;
        let target = open_for_branch(root_uri, target_branch).await?;

        let source_head = match source.head_commit().await? {
            Some(commit) => commit,
            None => return Ok(None),
        };
        let target_head = match target.head_commit().await? {
            Some(commit) => commit,
            None => return Ok(None),
        };

        Self::merge_base_from_open_graphs(
            source,
            target,
            &source_head.graph_commit_id,
            &target_head.graph_commit_id,
        )
        .await
    }

    async fn merge_base_from_open_graphs(
        source: Self,
        target: Self,
        source_commit_id: &str,
        target_commit_id: &str,
    ) -> Result<Option<GraphCommit>> {
        Ok(Self::merge_base_from_commits(
            source.load_commits().await?,
            target.load_commits().await?,
            source_commit_id,
            target_commit_id,
        ))
    }

    /// Compute a merge base from two already-captured lineage projections.
    /// The caller is responsible for coupling each projection to the branch
    /// authority snapshot it captured; this function is pure and performs no
    /// storage I/O.
    pub(crate) fn merge_base_from_commits(
        source_commits: Vec<GraphCommit>,
        target_commits: Vec<GraphCommit>,
        source_commit_id: &str,
        target_commit_id: &str,
    ) -> Option<GraphCommit> {
        let mut commits = HashMap::new();
        for commit in source_commits {
            commits.insert(commit.graph_commit_id.clone(), commit);
        }
        for commit in target_commits {
            commits.insert(commit.graph_commit_id.clone(), commit);
        }

        if !commits.contains_key(source_commit_id) || !commits.contains_key(target_commit_id) {
            return None;
        }

        let source_distances = ancestor_distances(source_commit_id, &commits);
        let target_distances = ancestor_distances(target_commit_id, &commits);

        source_distances
            .iter()
            .filter_map(|(id, source_distance)| {
                target_distances.get(id).and_then(|target_distance| {
                    commits.get(id).map(|commit| {
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
            .map(|(_, commit)| commit)
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
        let commit = GraphCommit {
            graph_commit_id: row.graph_commit_id,
            graph_branch: row.graph_branch,
            graph_manifest_version: row.graph_manifest_version,
            parent_commit_id: row.parent_commit_id,
            merged_parent_commit_id: row.merged_parent_commit_id,
            actor_id: row.actor_id,
            created_at: row.created_at,
        };
        if should_replace_head(head_commit.as_ref(), &commit) {
            head_commit = Some(commit.clone());
        }
        commit_by_id.insert(commit.graph_commit_id.clone(), commit);
    }
    (commit_by_id, head_commit)
}

fn should_replace_head(current: Option<&GraphCommit>, candidate: &GraphCommit) -> bool {
    current.is_none_or(|existing| candidate.lineage_key() > existing.lineage_key())
}

fn ancestor_distances(
    start_id: &str,
    commits: &HashMap<String, GraphCommit>,
) -> HashMap<String, u64> {
    let mut distances = HashMap::new();
    let mut queue = VecDeque::from([(start_id.to_string(), 0u64)]);

    while let Some((id, distance)) = queue.pop_front() {
        if let Some(existing) = distances.get(&id) {
            if *existing <= distance {
                continue;
            }
        }
        distances.insert(id.clone(), distance);

        if let Some(commit) = commits.get(&id) {
            if let Some(parent) = &commit.parent_commit_id {
                queue.push_back((parent.clone(), distance + 1));
            }
            if let Some(parent) = &commit.merged_parent_commit_id {
                queue.push_back((parent.clone(), distance + 1));
            }
        }
    }

    distances
}

async fn open_for_branch(root_uri: &str, branch: Option<&str>) -> Result<CommitGraph> {
    match branch {
        Some(branch) if branch != "main" => CommitGraph::open_at_branch(root_uri, branch).await,
        _ => CommitGraph::open(root_uri).await,
    }
}
