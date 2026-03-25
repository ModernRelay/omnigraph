mod helpers;

use std::fs;

use arrow_array::{Array, Int32Array};

use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::error::{MergeConflictKind, OmniError};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

const SEARCH_SCHEMA: &str = include_str!("fixtures/search.pg");
const SEARCH_DATA: &str = include_str!("fixtures/search.jsonl");
const SEARCH_QUERIES: &str = include_str!("fixtures/search.gq");
const SEARCH_MUTATIONS: &str = r#"
query set_doc_title($slug: String, $title: String) {
    update Doc set { title: $title } where slug = $slug
}
"#;

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

#[tokio::test]
async fn branch_create_open_list_and_lazy_branching_work() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main", "feature"]);

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    assert_eq!(count_rows(&feature, "node:Person").await, 4);
    assert_eq!(
        feature
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        None
    );

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let snap = feature.snapshot();
    assert_eq!(
        snap.entry("node:Person").unwrap().table_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        snap.entry("edge:Knows").unwrap().table_branch.as_deref(),
        None
    );

    let main = Omnigraph::open(uri).await.unwrap();
    let qr = main
        .snapshot()
        .open("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(qr, 4);
}

#[tokio::test]
async fn branch_merge_updates_main_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(dir.path().to_str().unwrap(), "feature")
        .await
        .unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Diana")]),
        )
        .await
        .unwrap();

    let feature_qr = feature
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let main_before = main
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(main_before.num_rows(), 2);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let merged = main
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(merged.num_rows(), 3);
}

#[tokio::test]
async fn branch_merge_applies_node_insert_to_main() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let outcome = feature.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    let qr = reopened
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_records_single_latest_commit_with_two_parents() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let source_head_before = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    let target_head_before = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let commit_graph = CommitGraph::open(uri).await.unwrap();
    let head = commit_graph.head_commit().await.unwrap().unwrap();
    let commits = commit_graph.load_commits().await.unwrap();
    let latest_manifest_version = commits.iter().map(|c| c.manifest_version).max().unwrap();
    let latest_commits: Vec<_> = commits
        .iter()
        .filter(|commit| commit.manifest_version == latest_manifest_version)
        .collect();

    assert_eq!(latest_commits.len(), 1);
    assert_eq!(head.manifest_version, latest_manifest_version);
    assert_eq!(
        head.parent_commit_id.as_deref(),
        Some(target_head_before.graph_commit_id.as_str())
    );
    assert_eq!(
        head.merged_parent_commit_id.as_deref(),
        Some(source_head_before.graph_commit_id.as_str())
    );
}

#[tokio::test]
async fn already_up_to_date_branch_merge_returns_without_new_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let source_head_before = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    let target_head_before = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source_head_before.manifest_version,
        target_head_before.manifest_version
    );

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::AlreadyUpToDate);

    let commit_graph = CommitGraph::open(uri).await.unwrap();
    let head = commit_graph.head_commit().await.unwrap().unwrap();

    assert_eq!(head.manifest_version, target_head_before.manifest_version);
    assert_eq!(head.graph_commit_id, target_head_before.graph_commit_id);
    assert_eq!(
        head.graph_commit_id,
        source_head_before.graph_commit_id
    );
}

#[tokio::test]
async fn branch_merge_returns_merged_for_non_fast_forward_auto_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    main.run_mutation(
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let bob = main
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Bob")]))
        .await
        .unwrap()
        .concat_batches()
        .unwrap();
    let bob_ages = bob.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = main
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(eve.num_rows(), 1);
}

#[tokio::test]
async fn merged_rewritten_indexed_table_is_searchable_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_search_db(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    main.run_mutation(
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "ml-intro"), ("$title", "Orion ML Intro")]),
    )
    .await
    .unwrap();

    feature
        .run_mutation(
            SEARCH_MUTATIONS,
            "set_doc_title",
            &params(&[("$slug", "dl-basics"), ("$title", "Orion DL Basics")]),
        )
        .await
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let result = main
        .run_query(SEARCH_QUERIES, "text_search", &params(&[("$q", "Orion")]))
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    let slugs = batch.column(0).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
    let values: Vec<&str> = (0..slugs.len()).map(|idx| slugs.value(idx)).collect();
    assert!(values.contains(&"ml-intro"));
    assert!(values.contains(&"dl-basics"));
}

#[tokio::test]
async fn branch_merge_reports_divergent_update_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    main.run_mutation(
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", 32)]),
        )
        .await
        .unwrap();

    let err = feature.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "node:Person"
                    && conflict.row_id.as_deref() == Some("Alice")
                    && conflict.kind == MergeConflictKind::DivergentUpdate
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    let qr = reopened
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    let batch = qr.concat_batches().unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 31);
}

#[tokio::test]
async fn branch_refresh_preserves_branch_and_sees_branch_local_writes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut writer = Omnigraph::open_branch(uri, "feature").await.unwrap();
    let mut reader = Omnigraph::open_branch(uri, "feature").await.unwrap();
    let mut main_reader = Omnigraph::open(uri).await.unwrap();

    writer
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let stale = reader
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(stale.num_rows(), 0);

    reader.refresh().await.unwrap();
    assert_eq!(reader.active_branch(), Some("feature"));
    let refreshed = reader
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(refreshed.num_rows(), 1);

    main_reader.refresh().await.unwrap();
    let main_result = main_reader
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(main_result.num_rows(), 0);
}

#[tokio::test]
async fn branch_created_from_non_main_inherits_branch_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    feature.branch_create("experiment").await.unwrap();

    assert_eq!(
        feature.branch_list().await.unwrap(),
        vec!["main", "experiment", "feature"]
    );

    let mut experiment = Omnigraph::open_branch(uri, "experiment").await.unwrap();
    let qr = experiment
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = reopened_main
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 0);
}

#[tokio::test]
async fn ensure_indices_on_child_branch_forks_inherited_table_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    feature.branch_create("experiment").await.unwrap();

    let mut experiment = Omnigraph::open_branch(uri, "experiment").await.unwrap();
    assert_eq!(
        experiment
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );

    experiment.ensure_indices().await.unwrap();

    let experiment_snap = experiment.snapshot();
    assert_eq!(
        experiment_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("experiment")
    );
    assert_eq!(
        experiment_snap
            .entry("edge:Knows")
            .unwrap()
            .table_branch
            .as_deref(),
        None
    );

    feature.refresh().await.unwrap();
    assert_eq!(
        feature
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );
    assert_eq!(count_rows(&feature, "node:Person").await, 5);
    assert_eq!(count_rows(&experiment, "node:Person").await, 5);
}

#[tokio::test]
async fn branch_edge_only_write_only_branches_edge_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Diana")]),
        )
        .await
        .unwrap();

    let snap = feature.snapshot();
    assert_eq!(
        snap.entry("node:Person").unwrap().table_branch.as_deref(),
        None
    );
    assert_eq!(
        snap.entry("edge:Knows").unwrap().table_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        snap.entry("edge:WorksAt").unwrap().table_branch.as_deref(),
        None
    );

    let feature_qr = feature
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = reopened_main
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 2);
}

#[tokio::test]
async fn branch_merge_into_non_main_target_works() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    feature.branch_create("experiment").await.unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
        )
        .await
        .unwrap();

    let outcome = main.branch_merge("feature", "experiment").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut experiment = Omnigraph::open_branch(uri, "experiment").await.unwrap();
    let bob = experiment
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Bob")]))
        .await
        .unwrap();
    let bob_batch = bob.concat_batches().unwrap();
    let bob_ages = bob_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = experiment
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
        .await
        .unwrap();
    assert_eq!(eve.num_rows(), 1);
    assert_eq!(
        experiment
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("experiment")
    );

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_bob = reopened_main
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Bob")]))
        .await
        .unwrap();
    let main_batch = main_bob.concat_batches().unwrap();
    let main_ages = main_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(main_ages.value(0), 25);
}

#[tokio::test]
async fn branch_merge_reports_divergent_insert_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    main.run_mutation(
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 21)]),
    )
    .await
    .unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let err = feature.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "node:Person"
                    && conflict.row_id.as_deref() == Some("Eve")
                    && conflict.kind == MergeConflictKind::DivergentInsert
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

#[tokio::test]
async fn branch_merge_reports_delete_vs_update_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    main.run_mutation(
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", 32)]),
        )
        .await
        .unwrap();

    let err = feature.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "node:Person"
                    && conflict.row_id.as_deref() == Some("Alice")
                    && conflict.kind == MergeConflictKind::DeleteVsUpdate
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

#[tokio::test]
async fn branch_merge_reports_orphan_edge_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    feature
        .run_mutation(
            MUTATION_QUERIES,
            "remove_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();

    main.run_mutation(
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let err = feature.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "edge:Knows" && conflict.kind == MergeConflictKind::OrphanEdge
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

#[tokio::test]
async fn branch_create_bootstraps_missing_commit_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    drop(db);

    fs::remove_dir_all(dir.path().join("_graph_commits.lance")).unwrap();

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    reopened.branch_create("feature").await.unwrap();

    assert!(dir.path().join("_graph_commits.lance").exists());

    let feature = Omnigraph::open_branch(uri, "feature").await.unwrap();
    assert_eq!(count_rows(&feature, "node:Person").await, 4);
}

#[tokio::test]
async fn branch_api_rejects_reserved_main_and_same_source_target_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db.branch_create("main").await.unwrap_err();
    assert!(err.to_string().contains("cannot create branch 'main'"));

    let err = db.branch_merge("main", "main").await.unwrap_err();
    assert!(err.to_string().contains("distinct source and target"));
}
