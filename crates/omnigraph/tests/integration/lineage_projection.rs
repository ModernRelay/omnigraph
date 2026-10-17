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

use crate::helpers;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{GraphCommit, Omnigraph, ReadTarget};

use crate::helpers::*;

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
    commits.sort_by(|a, b| a.lineage_key().cmp(&b.lineage_key()));
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

    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
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
        outcome.outcome,
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
    let resolved_main_head = main.resolve_snapshot("main").await.unwrap();
    assert_eq!(
        resolved_main_head.as_str(),
        merge_commit.graph_commit_id,
        "the production snapshot resolver and lineage projection must expose the same main head"
    );
    let resolved_main_snapshot = main.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let resolved_main_commit = main.get_commit(resolved_main_head.as_str()).await.unwrap();
    assert_eq!(
        resolved_main_commit.graph_manifest_version,
        resolved_main_snapshot.graph_manifest_version(),
        "without intervening physical-only maintenance, main's resolved snapshot and head \
         lineage must come from one manifest version"
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
    let resolved_feature_head = main.resolve_snapshot("feature").await.unwrap();
    assert_eq!(
        resolved_feature_head.as_str(),
        feature_head,
        "the production snapshot resolver and lineage projection must expose the same feature head"
    );
    let resolved_feature_snapshot = main
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    let resolved_feature_commit = main
        .get_commit(resolved_feature_head.as_str())
        .await
        .unwrap();
    assert_eq!(
        resolved_feature_commit.graph_manifest_version,
        resolved_feature_snapshot.graph_manifest_version(),
        "without intervening physical-only maintenance, feature's resolved snapshot and head \
         lineage must come from one manifest version"
    );

    // ── public listing contract: newest-first, one total order ───────────────
    // `Omnigraph::list_commits` is the single public door (embedded CLI, HTTP
    // server, SDK all funnel through it); its contract is most-recent-first by
    // `GraphCommit::lineage_key`. The internal projection (`load_commits`,
    // asserted above) stays ascending — this block pins the public flip.
    let listed_main = main.list_commits(None).await.unwrap();
    assert_eq!(
        listed_main.first().map(|c| c.graph_commit_id.as_str()),
        Some(merge_commit.graph_commit_id.as_str()),
        "public main listing must start at the branch head (newest first)"
    );
    assert!(
        listed_main
            .last()
            .is_some_and(|c| c.parent_commit_id.is_none()),
        "public main listing must end at the parentless genesis commit"
    );
    assert!(
        listed_main
            .windows(2)
            .all(|pair| pair[0].lineage_key() > pair[1].lineage_key()),
        "public listing must be strictly descending by lineage_key"
    );
    let listed_feature = main.list_commits(Some("feature")).await.unwrap();
    assert_eq!(
        listed_feature.first().map(|c| c.graph_commit_id.as_str()),
        Some(feature_head.as_str()),
        "public feature listing must start at the feature head (newest first)"
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

    // ── paged listing: the same order, one page at a time ────────────────────
    // Pages walk first parents by falling generation; across the fork and the
    // merge they must concatenate to the listing, whatever the page size.
    // The branch-scoped door proves each continuation on the branch's history
    // and answers the same pages.
    for (branch, listed) in [(None, &listed_main), (Some("feature"), &listed_feature)] {
        for limit in [1, 2, listed.len(), listed.len() + 1] {
            for on_branch in [false, true] {
                let (head, paged) = all_pages(&main, branch, limit, on_branch).await;
                assert_eq!(head, listed[0].graph_commit_id, "{branch:?} by {limit}");
                assert_eq!(
                    &paged, listed,
                    "{branch:?} by {limit}, on branch {on_branch}"
                );
            }
        }
    }
    assert!(
        listed_main
            .windows(2)
            .all(|pair| pair[0].generation > pair[1].generation),
        "the listing falls in generation, the page walk's keyset"
    );

    // A continuation answers for the head its first page resolved, after the
    // branch moves; another branch's request refuses it.
    let first = main.list_commits_page(None, None, Some(2)).await.unwrap();
    assert_eq!(first.head_commit_id, merge_commit.graph_commit_id);
    assert_eq!(first.commits, listed_main[..2]);
    let token = first.next_page_token.clone().expect("more pages");
    main.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 52)]),
        Some("act-frank"),
    )
    .await
    .unwrap();
    let mut rest = first.commits.clone();
    let mut next = Some(token.clone());
    while let Some(token) = next {
        let page = main
            .list_commits_page(None, Some(&token), Some(2))
            .await
            .unwrap();
        assert_eq!(page.head_commit_id, merge_commit.graph_commit_id);
        rest.extend(page.commits);
        next = page.next_page_token;
    }
    assert_eq!(rest, listed_main, "the continuation keeps the old head");
    let proven = main
        .list_commits_page_on_branch(None, Some(&token), Some(2))
        .await
        .unwrap();
    assert_eq!(
        proven.commits,
        listed_main[2..4],
        "the proof walks over the commit published since"
    );
    let moved = main.list_commits_page(None, None, Some(1)).await.unwrap();
    assert_ne!(moved.head_commit_id, merge_commit.graph_commit_id);
    assert_eq!(moved.commits[0].actor_id.as_deref(), Some("act-frank"));
    for (branch, token) in [(Some("feature"), token.as_str()), (None, "not-a-token")] {
        let error = main
            .list_commits_page(branch, Some(token), Some(2))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                omnigraph::error::OmniError::ChangeCursorRejected { .. }
            ),
            "{error:?}"
        );
    }
    let error = main
        .list_commits_page(
            None,
            None,
            Some(omnigraph::db::COMMIT_LIST_MAX_PAGE_COMMITS + 1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            omnigraph::error::OmniError::ResourceLimitExceeded { .. }
        ),
        "{error:?}"
    );
    assert!(main.list_commits_page(None, None, Some(0)).await.is_err());

    // A token is checksummed, not signed: re-signed to continue `main` from a
    // commit only `feature`'s first-parent chain holds, the trusting door
    // starts there and the branch-scoped door refuses it.
    let carol = listed_feature
        .iter()
        .find(|commit| commit.actor_id.as_deref() == Some("act-carol"))
        .unwrap();
    assert!(!listed_main.contains(carol), "test setup: off main's chain");
    let token = main
        .list_commits_page(None, None, Some(1))
        .await
        .unwrap()
        .next_page_token
        .unwrap();
    let forged = resign_change_token(&token, |payload| {
        payload["next_commit_id"] = serde_json::json!(carol.graph_commit_id);
        payload["below_generation"] = serde_json::json!(u64::MAX);
    });
    let trusted = main
        .list_commits_page(None, Some(&forged), Some(1))
        .await
        .unwrap();
    assert_eq!(trusted.commits[0].graph_commit_id, carol.graph_commit_id);
    match main
        .list_commits_page_on_branch(None, Some(&forged), Some(1))
        .await
        .unwrap_err()
    {
        omnigraph::error::OmniError::ChangeCursorRejected { reason } => {
            assert_eq!(
                reason,
                "commit list page token names a commit off this branch's history"
            )
        }
        other => panic!("expected ChangeCursorRejected, got {other:?}"),
    }
}

/// One page of `branch`'s commit list, through the branch-scoped door when
/// `on_branch`.
async fn commit_page(
    db: &omnigraph::session::Session,
    branch: Option<&str>,
    token: Option<&str>,
    limit: usize,
    on_branch: bool,
) -> omnigraph::db::CommitListPage {
    match on_branch {
        true => {
            db.list_commits_page_on_branch(branch, token, Some(limit))
                .await
        }
        false => db.list_commits_page(branch, token, Some(limit)).await,
    }
    .unwrap()
}

/// Every page of `branch`'s commit list at `limit` per page, concatenated,
/// and the head the first page named, which every page repeats; through the
/// branch-scoped door when `on_branch`.
async fn all_pages(
    db: &omnigraph::session::Session,
    branch: Option<&str>,
    limit: usize,
    on_branch: bool,
) -> (String, Vec<GraphCommit>) {
    let first = commit_page(db, branch, None, limit, on_branch).await;
    let head = first.head_commit_id.clone();
    let mut commits = first.commits;
    let mut next = first.next_page_token;
    while let Some(token) = next {
        let page = commit_page(db, branch, Some(&token), limit, on_branch).await;
        assert_eq!(page.head_commit_id, head, "every page names the first head");
        assert!(page.commits.len() <= limit && !page.commits.is_empty());
        commits.extend(page.commits);
        next = page.next_page_token;
    }
    (head, commits)
}
