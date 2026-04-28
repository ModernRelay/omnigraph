// Maintenance tests: `optimize` (Lance compact_files) and `cleanup`
// (Lance cleanup_old_versions) at the graph level. Covers no-op edges
// (empty repo, already-optimized repo), the policy-validation contract on
// `cleanup`, and the keep-versions cap that protects head.

mod helpers;

use std::time::Duration;

use omnigraph::db::{CleanupPolicyOptions, Omnigraph};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::{TEST_DATA, TEST_SCHEMA, count_rows, init_and_load};

#[tokio::test]
async fn optimize_on_empty_repo_returns_stats_per_table_with_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let stats = db.optimize().await.unwrap();

    // Schema declares 2 nodes + 2 edges = 4 tables. Compaction should run on
    // each but find nothing to merge.
    assert_eq!(stats.len(), 4);
    for s in &stats {
        assert_eq!(s.fragments_removed, 0, "{} should not remove", s.table_key);
        assert_eq!(s.fragments_added, 0, "{} should not add", s.table_key);
    }
}

#[tokio::test]
async fn optimize_after_load_then_again_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // First pass may compact (load wrote real fragments).
    let _first = db.optimize().await.unwrap();

    // Second pass should be a no-op: already-compacted repo produces no
    // fragments_removed / fragments_added.
    let second = db.optimize().await.unwrap();
    for s in &second {
        assert_eq!(
            s.fragments_removed, 0,
            "{} re-optimize should be no-op",
            s.table_key
        );
        assert_eq!(
            s.fragments_added, 0,
            "{} re-optimize should be no-op",
            s.table_key
        );
        assert!(
            !s.committed,
            "{} re-optimize should not commit a new version",
            s.table_key
        );
    }
}

#[tokio::test]
async fn cleanup_without_any_policy_option_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db
        .cleanup(CleanupPolicyOptions::default())
        .await
        .expect_err("cleanup with no policy options must error");

    let msg = format!("{}", err);
    assert!(
        msg.contains("keep_versions") && msg.contains("older_than"),
        "error should name the two policy fields, got: {msg}"
    );
}

#[tokio::test]
async fn cleanup_keep_one_preserves_head_and_table_remains_readable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    // Most aggressive version-based cleanup short of forcing keep=0. Lance's
    // contract is that head is always preserved regardless, so the table
    // must remain openable and rows must still be visible.
    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, people_before);
}

#[tokio::test]
async fn cleanup_older_than_zero_preserves_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Aggressive policy: every version is "older than zero seconds ago".
    // Lance must still preserve the head manifest, so the table is openable
    // afterwards and a subsequent load still works.
    let _stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: None,
            older_than: Some(Duration::from_secs(0)),
        })
        .await
        .unwrap();

    // Smoke test: after aggressive cleanup, we can still read and write the
    // graph — head wasn't pruned.
    load_jsonl(&mut db, TEST_DATA, LoadMode::Merge).await.unwrap();
}

#[tokio::test]
async fn cleanup_then_optimize_succeed_in_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    db.optimize().await.unwrap();
}
