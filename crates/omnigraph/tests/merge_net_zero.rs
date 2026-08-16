//! Merging a branch whose edits net back to the fork point (#473).
//!
//! A branch can carry real commits whose *content* returns to the merge base:
//! insert an edge, then delete it. The branch's manifest state still differs
//! from the target's — its Lance version advanced twice — so the table is not
//! caught by the manifest-equality gate that skips untouched tables, and it
//! reaches adopt classification.
//!
//! Publishing that adopt re-registers the entry `__manifest` already holds,
//! which the publisher's registry guard rejects. The failure is deterministic:
//! a retry recomputes the same publish and fails identically, so the branch is
//! permanently unmergeable.
//!
//! These tests pin the whole contract rather than "the merge stops erroring";
//! each one names the half it owns.

mod helpers;

use helpers::{
    MUTATION_QUERIES, TEST_DATA, TEST_QUERIES, TEST_SCHEMA, count_rows, first_column_sorted,
    init_and_load, mixed_params, mutate_branch, mutate_main, params, query_main, snapshot_main,
};
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};

/// `Diana` has no outgoing `Knows` in the fixture, so adding one edge from her
/// and then deleting every edge from her returns `edge:Knows` to its exact
/// fork-point content while advancing its Lance version twice.
const NET_ZERO_SOURCE: &str = "Diana";

/// Insert one edge from [`NET_ZERO_SOURCE`], then delete it again.
async fn apply_net_zero_edge_cycle(db: &mut Omnigraph, branch: &str) {
    mutate_branch(
        db,
        branch,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", NET_ZERO_SOURCE), ("$to", "Alice")]),
    )
    .await
    .expect("insert the edge that the delete below removes again");

    mutate_branch(
        db,
        branch,
        MUTATION_QUERIES,
        "remove_friendship",
        &params(&[("$from", NET_ZERO_SOURCE)]),
    )
    .await
    .expect("delete the edge just inserted, returning the table to fork content");
}

/// The `friends_of` answer for every fixture person, as one comparable value.
async fn friend_map(db: &mut Omnigraph) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for person in ["Alice", "Bob", "Charlie", "Diana"] {
        let result = query_main(
            db,
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", person)]),
        )
        .await
        .expect("traversal must answer for every fixture person");
        out.push((person.to_string(), first_column_sorted(&result)));
    }
    out
}

/// The merge succeeds, its lineage lands, and it is idempotent.
///
/// The second merge is the load-bearing assertion. A fix that suppresses the
/// no-op publish but never commits the merge lineage would still satisfy the
/// first assertion while leaving the branch unmerged forever, and the third
/// merge would keep reporting `FastForward`.
#[tokio::test]
async fn branch_whose_edits_net_to_zero_merges_and_records_its_lineage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let outcome = main
        .branch_merge("feature", "main")
        .await
        .expect("a branch whose net content change is zero must be mergeable");

    // The target never moved, so this is a fast-forward.
    assert_eq!(outcome, MergeOutcome::FastForward);

    // The lineage landed: `feature`'s head is now an ancestor of `main`.
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
        "the first merge must record its lineage, not silently publish nothing",
    );
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
        "repeating a landed merge stays a typed no-op",
    );
}

/// A no-op merge advances the manifest exactly once and moves no table.
///
/// Re-registering the stored entry would publish a second `table_version` row
/// for a table nothing happened to. This pins that `__manifest` records the
/// merge commit and nothing else.
#[tokio::test]
async fn net_zero_merge_advances_the_manifest_once_and_moves_no_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let before = snapshot_main(&main).await.unwrap();
    let before_version = before.version();
    let before_tables: Vec<(String, u64, u64)> = before
        .entries()
        .map(|entry| {
            (
                entry.table_key.clone(),
                entry.table_version,
                entry.row_count,
            )
        })
        .collect();

    main.branch_merge("feature", "main").await.unwrap();

    let after = snapshot_main(&main).await.unwrap();
    assert_eq!(
        after.version(),
        before_version + 1,
        "a no-op merge publishes its lineage commit and nothing else",
    );
    for (table_key, table_version, row_count) in before_tables {
        let entry = after
            .entry(&table_key)
            .unwrap_or_else(|| panic!("table '{table_key}' must survive the merge"));
        assert_eq!(
            (entry.table_version, entry.row_count),
            (table_version, row_count),
            "table '{table_key}' must not move across a merge that changes nothing",
        );
    }
}

/// Graph content is identical on both sides of the merge.
#[tokio::test]
async fn net_zero_merge_leaves_graph_content_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    let friends_before = friend_map(&mut main).await;
    let knows_before = count_rows(&main, "edge:Knows").await;
    let people_before = count_rows(&main, "node:Person").await;

    main.branch_merge("feature", "main").await.unwrap();

    assert_eq!(friend_map(&mut main).await, friends_before);
    assert_eq!(count_rows(&main, "edge:Knows").await, knows_before);
    assert_eq!(count_rows(&main, "node:Person").await, people_before);
}

/// The non-fast-forward route: the target moved after the fork, so the
/// merge is a true three-way publication that still has nothing to say about
/// the net-zero table.
#[tokio::test]
async fn net_zero_branch_merges_into_a_target_that_moved() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut feature, "feature").await;

    // Move the target after the fork so the merge base is neither head.
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Erin")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        main.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
    );
    assert_eq!(
        count_rows(&main, "node:Person").await,
        5,
        "the target's own row must survive a merge that publishes no table",
    );
}

/// Suppression must not over-reach. One branch carries both a net-zero
/// table and a table with a real delta; the real delta still lands.
#[tokio::test]
async fn merge_publishes_a_real_delta_alongside_a_net_zero_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    // edge:Knows nets to zero; node:Person genuinely gains a row.
    apply_net_zero_edge_cycle(&mut feature, "feature").await;
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    let knows_before = count_rows(&main, "edge:Knows").await;
    let knows_version_before = snapshot_main(&main)
        .await
        .unwrap()
        .entry("edge:Knows")
        .expect("fixture registers edge:Knows")
        .table_version;

    main.branch_merge("feature", "main").await.unwrap();

    let people = query_main(&mut main, TEST_QUERIES, "total_people", &params(&[]))
        .await
        .unwrap();
    assert_eq!(people.num_rows(), 1);
    assert_eq!(
        count_rows(&main, "node:Person").await,
        5,
        "the sibling table's real delta must still publish",
    );
    assert_eq!(
        count_rows(&main, "edge:Knows").await,
        knows_before,
        "the net-zero table's content must be untouched",
    );
    assert_eq!(
        snapshot_main(&main)
            .await
            .unwrap()
            .entry("edge:Knows")
            .expect("edge:Knows must survive the merge")
            .table_version,
        knows_version_before,
        "the net-zero table must not be re-registered at a new version",
    );
}

/// The branch-to-branch route: the target owns the table on its own
/// branch lineage, which is the second arm that can plan a re-registration.
///
/// The fixture is loaded without `ensure_indices`: writing to a branch forked
/// from another branch whose table carries a scalar index fails in the
/// deferred-fork staging path, unrelated to this merge shape. Skipping the
/// index keeps this test on the arm it is meant to cover.
#[tokio::test]
async fn net_zero_branch_merges_into_a_branch_that_owns_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&main, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("target").await.unwrap();

    // Materialize `target`'s own lineage on edge:Knows before forking `source`,
    // so the merge lands on a table the target branch owns.
    let mut target = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut target, "target").await;

    target
        .branch_create_from(ReadTarget::branch("target"), "source")
        .await
        .unwrap();
    let mut source = Omnigraph::open(uri).await.unwrap();
    apply_net_zero_edge_cycle(&mut source, "source").await;

    let outcome = target
        .branch_merge("source", "target")
        .await
        .expect("a branch-owned target must accept a net-zero source");
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        target.branch_merge("source", "target").await.unwrap(),
        MergeOutcome::AlreadyUpToDate,
    );
}
