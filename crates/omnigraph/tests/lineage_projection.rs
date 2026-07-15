//! RFC-013 Phase 7 acceptance gate: graph lineage lives ONLY in `__manifest`.
//!
//! The `graph_commit` + `graph_head` rows ride the same publish CAS as the
//! table-version rows, so no standalone commit-lineage dataset exists. This
//! gate proves two things over a realistic history (commits on main, a branch,
//! a merge, all with actors):
//!
//! 1. The production commit-graph projection (`CommitGraph::open(...)`, which now
//!    reads `__manifest`) reconstructs the full lineage correctly — commit set,
//!    parents, the merge commit's two parents + merge actor, per-branch heads,
//!    and the inline actors.
//! 2. `_graph_commits.lance` and `_graph_commit_actors.lance` are NEVER CREATED
//!    (Phase B retired them — the commit graph opens no Lance dataset). This is
//!    the load-bearing "single source" assertion.

mod helpers;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{GraphCommit, Omnigraph};

use helpers::*;

/// True when a Lance dataset directory exists under the graph root.
fn table_dir_exists(root: &str, dir: &str) -> bool {
    std::path::Path::new(root.trim_end_matches('/'))
        .join(dir)
        .exists()
}

/// The production commit-graph projection at `branch`, sourced from `__manifest`.
async fn projected_commits(root: &str, branch: Option<&str>) -> Vec<GraphCommit> {
    let graph = match branch {
        Some(branch) => CommitGraph::open_at_branch(root, branch).await.unwrap(),
        None => CommitGraph::open(root).await.unwrap(),
    };
    let mut commits = graph.load_commits().await.unwrap();
    commits.sort_by(|a, b| {
        a.manifest_version
            .cmp(&b.manifest_version)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
    });
    commits
}

async fn head_id(root: &str, branch: Option<&str>) -> String {
    let graph = match branch {
        Some(branch) => CommitGraph::open_at_branch(root, branch).await.unwrap(),
        None => CommitGraph::open(root).await.unwrap(),
    };
    graph.head_commit().await.unwrap().unwrap().graph_commit_id
}

#[tokio::test]
async fn graph_lineage_lives_only_in_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Build a realistic history: several authored commits on main, a branch with
    // its own authored commits, then an authored merge back into main.
    let main = init_and_load(&dir).await;

    main.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Alice")], &[("$age", 30)]),
        Some("act-alice"),
    )
    .await
    .unwrap();
    main.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Bob")], &[("$age", 41)]),
        Some("act-bob"),
    )
    .await
    .unwrap();

    main.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(&uri).await.unwrap();
    feature
        .mutate_as(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Carol")], &[("$age", 27)]),
            Some("act-carol"),
        )
        .await
        .unwrap();
    feature
        .mutate_as(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Dave")], &[("$age", 33)]),
            Some("act-dave"),
        )
        .await
        .unwrap();

    // Advance main once more so the merge is a real (non-fast-forward) merge with
    // two distinct parents.
    main.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Erin")], &[("$age", 38)]),
        Some("act-erin"),
    )
    .await
    .unwrap();

    let outcome = main
        .branch_merge_as("feature", "main", Some("act-merger"))
        .await
        .unwrap();
    // A genuine three-way merge (both sides advanced past the base).
    assert_eq!(
        outcome,
        omnigraph::db::MergeOutcome::Merged,
        "expected a real merge, not fast-forward/up-to-date"
    );

    // ── single source: the commit-graph tables are never created ─────────────
    // RFC-013 Phase 7 folds lineage into `__manifest`; Phase B then retired the
    // two commit-graph datasets entirely (the projection opens no Lance dataset).
    // A full realistic history — commits on main, a branch, a merge, all with
    // actors — must not bring either directory into existence. If a stray
    // dataset write reappears, this turns red.
    assert!(
        !table_dir_exists(&uri, "_graph_commits.lance"),
        "_graph_commits.lance must never be created — lineage lives in __manifest"
    );
    assert!(
        !table_dir_exists(&uri, "_graph_commit_actors.lance"),
        "_graph_commit_actors.lance must never be created — actors live inline in __manifest"
    );

    // ── main lineage projected from `__manifest` ─────────────────────────────
    let main_commits = projected_commits(&uri, None).await;
    // genesis + Alice + Bob + Erin + the merge = 5 on main.
    assert!(
        main_commits.len() >= 5,
        "expected a non-trivial main history, got {} commits",
        main_commits.len()
    );

    // Genesis is the unique parentless commit and carries no actor.
    let genesis: Vec<&GraphCommit> = main_commits
        .iter()
        .filter(|c| c.parent_commit_id.is_none())
        .collect();
    assert_eq!(genesis.len(), 1, "exactly one genesis (parentless) commit");
    assert!(
        genesis[0].actor_id.is_none(),
        "genesis commit carries no actor"
    );

    // Every non-genesis commit's parent resolves to a known commit (a connected
    // lineage — the publisher resolved each parent under the CAS).
    for commit in &main_commits {
        if let Some(parent) = &commit.parent_commit_id {
            assert!(
                main_commits.iter().any(|c| &c.graph_commit_id == parent),
                "parent {parent} of {} must be a known commit",
                commit.graph_commit_id
            );
        }
    }

    // The merge commit carries both parents and the merge actor.
    let merge_commit = main_commits
        .iter()
        .find(|c| c.merged_parent_commit_id.is_some())
        .expect("a merge commit with a merged parent must exist");
    assert_eq!(merge_commit.actor_id.as_deref(), Some("act-merger"));
    assert!(merge_commit.parent_commit_id.is_some());
    // The merge is the head of main.
    assert_eq!(
        head_id(&uri, None).await,
        merge_commit.graph_commit_id,
        "the merge commit is the head of main"
    );

    // ── feature lineage projected from `__manifest` ──────────────────────────
    let feature_commits = projected_commits(&uri, Some("feature")).await;
    // The feature head is Dave's commit (the last authored on the branch).
    let feature_head = head_id(&uri, Some("feature")).await;
    let feature_head_commit = feature_commits
        .iter()
        .find(|c| c.graph_commit_id == feature_head)
        .expect("feature head must be in the feature projection");
    assert_eq!(
        feature_head_commit.actor_id.as_deref(),
        Some("act-dave"),
        "feature head is Dave's authored commit"
    );

    // ── actors surface inline from the manifest metadata ─────────────────────
    // main's authored commits: Alice, Bob, Erin (direct) + the merge (act-merger)
    // = 4. Carol/Dave were authored on the feature branch, not main. Genesis has
    // no actor.
    let authored = main_commits.iter().filter(|c| c.actor_id.is_some()).count();
    assert!(
        authored >= 4,
        "expected the authored commits to surface their actor in the projection, saw {authored}"
    );
}
