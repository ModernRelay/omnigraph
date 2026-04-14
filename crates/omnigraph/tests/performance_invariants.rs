mod helpers;

use std::collections::HashMap;

use arrow_array::{Array, StringArray};

use omnigraph::db::{Omnigraph, ReadTarget};

use helpers::*;

// ─── Observability helpers ──────────────────────────────────────────────────

/// Capture a map of table_key → table_version from a snapshot.
async fn table_versions(db: &Omnigraph, branch: &str) -> HashMap<String, u64> {
    let snap = db
        .snapshot_of(ReadTarget::branch(branch))
        .await
        .unwrap();
    snap.entries()
        .map(|e| (e.table_key.clone(), e.table_version))
        .collect()
}

/// Capture a map of table_key → table_version from main.
async fn main_table_versions(db: &Omnigraph) -> HashMap<String, u64> {
    table_versions(db, "main").await
}

// ─── 1. Branch creation does not touch sub-tables ───────────────────────────

/// Creating a branch should only write to the manifest (Lance branch on the
/// manifest dataset). No node or edge sub-table should change version.
#[tokio::test]
async fn branch_create_does_not_touch_sub_tables() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Capture sub-table versions before branch creation
    let versions_before = main_table_versions(&db).await;

    // Create a branch
    db.branch_create("feature-x").await.unwrap();

    // Capture sub-table versions after branch creation
    let versions_after = main_table_versions(&db).await;

    // All sub-table versions must be unchanged
    assert_eq!(
        versions_before.len(),
        versions_after.len(),
        "number of tables changed after branch_create"
    );
    for (key, ver_before) in &versions_before {
        let ver_after = versions_after
            .get(key)
            .unwrap_or_else(|| panic!("table {} disappeared after branch_create", key));
        assert_eq!(
            ver_before, ver_after,
            "table {} version changed from {} to {} after branch_create",
            key, ver_before, ver_after
        );
    }

    // Also verify the new branch sees the same sub-table versions (lazy branching)
    let feature_versions = table_versions(&db, "feature-x").await;
    for (key, ver_main) in &versions_before {
        let ver_feature = feature_versions
            .get(key)
            .unwrap_or_else(|| panic!("table {} missing on branch feature-x", key));
        assert_eq!(
            ver_main, ver_feature,
            "table {} has different version on feature-x ({}) vs main ({})",
            key, ver_feature, ver_main
        );
    }
}

// ─── 2. Repo open does not advance any data table versions ──────────────────

/// Opening an existing repo should not write to any sub-table. This verifies
/// that `Omnigraph::open()` is read-only with respect to node/edge datasets.
#[tokio::test]
async fn repo_open_does_not_advance_data_table_versions() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Initialize and load data
    let db = init_and_load(&dir).await;
    let versions_before = main_table_versions(&db).await;
    let manifest_version_before = db
        .version_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    drop(db);

    // Re-open the repo
    let db = Omnigraph::open(uri).await.unwrap();
    let versions_after = main_table_versions(&db).await;
    let manifest_version_after = db
        .version_of(ReadTarget::branch("main"))
        .await
        .unwrap();

    // No sub-table should have advanced
    for (key, ver_before) in &versions_before {
        let ver_after = versions_after
            .get(key)
            .unwrap_or_else(|| panic!("table {} disappeared after re-open", key));
        assert_eq!(
            ver_before, ver_after,
            "table {} version changed from {} to {} after repo re-open",
            key, ver_before, ver_after
        );
    }

    // Manifest version should also not have advanced
    assert_eq!(
        manifest_version_before, manifest_version_after,
        "manifest version changed from {} to {} after repo re-open",
        manifest_version_before, manifest_version_after
    );
}

// ─── 3. Delete cascade only touches referencing edge types ──────────────────

/// Deleting a Person node should cascade to edge types that reference Person
/// (Knows: Person→Person, WorksAt: Person→Company) but must NOT touch the
/// Company node table. In the test schema, the only node table that should
/// advance is node:Person, and the edge tables that should advance are
/// edge:Knows and edge:WorksAt.
#[tokio::test]
async fn delete_cascade_only_touches_referencing_types() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Capture versions before the delete
    let versions_before = main_table_versions(&db).await;

    // Delete Alice — she has Knows edges (from/to) and a WorksAt edge
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    let versions_after = main_table_versions(&db).await;

    // These tables SHOULD have been touched (version advanced):
    let expected_touched = ["node:Person", "edge:Knows", "edge:WorksAt"];

    // This table MUST NOT be touched:
    let expected_untouched = ["node:Company"];

    for key in &expected_touched {
        let before = versions_before.get(*key).unwrap();
        let after = versions_after.get(*key).unwrap();
        assert!(
            after > before,
            "expected {} to be written (version advance) during Person delete cascade, but version stayed at {}",
            key, before
        );
    }

    for key in &expected_untouched {
        let before = versions_before.get(*key).unwrap();
        let after = versions_after.get(*key).unwrap();
        assert_eq!(
            before, after,
            "table {} should NOT have been touched during Person delete cascade, but version advanced from {} to {}",
            key, before, after
        );
    }
}

// ─── 4. Graph index sees updated data after edge write ──────────────────────

/// After inserting a new edge, traversal queries must reflect the new
/// connectivity. This verifies that the graph index is effectively invalidated
/// (or rebuilt) when the underlying edge data changes.
///
/// We verify this behaviorally: run a traversal, insert an edge, run it again,
/// and confirm the result set changes.
#[tokio::test]
async fn traversal_reflects_edge_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Baseline: friends of Bob = [Diana] (from test data: Bob→Diana via Knows)
    let result_before = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let batch_before = result_before.concat_batches().unwrap();
    let names_col = batch_before
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names_before: Vec<&str> = (0..names_col.len()).map(|i| names_col.value(i)).collect();
    assert_eq!(names_before, vec!["Diana"], "baseline friends of Bob");

    // Insert a new edge: Bob knows Charlie
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Bob"), ("$to", "Charlie")]),
    )
    .await
    .unwrap();

    // After insert: friends of Bob should now include Charlie
    let result_after = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let batch_after = result_after.concat_batches().unwrap();
    let names_col = batch_after
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_after: Vec<&str> = (0..names_col.len()).map(|i| names_col.value(i)).collect();
    names_after.sort();
    assert_eq!(
        names_after,
        vec!["Charlie", "Diana"],
        "traversal should reflect new Bob→Charlie edge"
    );
}

/// Same as above but for edge deletion: removing an edge must be reflected
/// in subsequent traversal queries.
#[tokio::test]
async fn traversal_reflects_edge_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Baseline: friends of Alice = [Bob, Charlie]
    let result_before = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    let batch_before = result_before.concat_batches().unwrap();
    let names_col = batch_before
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_before: Vec<&str> = (0..names_col.len()).map(|i| names_col.value(i)).collect();
    names_before.sort();
    assert_eq!(
        names_before,
        vec!["Bob", "Charlie"],
        "baseline friends of Alice"
    );

    // Delete Alice's Knows edges (remove_friendship deletes by from)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_friendship",
        &params(&[("$from", "Alice")]),
    )
    .await
    .unwrap();

    // After delete: friends of Alice should be empty
    let result_after = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    let batch_after = result_after.concat_batches().unwrap();
    let count = batch_after.num_rows();
    assert_eq!(
        count, 0,
        "traversal should reflect deletion of Alice's Knows edges, got {} rows",
        count
    );
}
