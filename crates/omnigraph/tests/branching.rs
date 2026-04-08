mod helpers;

use std::fs;

use arrow_array::{Array, Int32Array, UInt64Array};
use futures::TryStreamExt;
use lance_index::{DatasetIndexExt, is_system_index};

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
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

const UNIQUE_SCHEMA: &str = r#"
node User {
    name: String @key
    email: String?
    @unique(email)
}
"#;

const UNIQUE_DATA: &str = r#"{"type":"User","data":{"name":"Alice","email":"alice@example.com"}}"#;

const UNIQUE_MUTATIONS: &str = r#"
query insert_user($name: String, $email: String) {
    insert User { name: $name, email: $email }
}
"#;

const CARDINALITY_SCHEMA: &str = r#"
node Person {
    name: String @key
}

node Company {
    name: String @key
}

edge WorksAt: Person -> Company @card(0..1)
"#;

const CARDINALITY_DATA: &str = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"type":"Company","data":{"name":"Beta"}}"#;

const CARDINALITY_MUTATIONS: &str = r#"
query add_employment($person: String, $company: String) {
    insert WorksAt { from: $person, to: $company }
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

async fn init_db_from_schema_and_data(
    dir: &tempfile::TempDir,
    schema: &str,
    data: &str,
) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn branch_create_open_list_and_lazy_branching_work() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main", "feature"]);

    let mut feature = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        4
    );
    let initial_feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        initial_feature_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        None
    );

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        snap.entry("node:Person").unwrap().table_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        snap.entry("edge:Knows").unwrap().table_branch.as_deref(),
        None
    );

    let main = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&main, "node:Person").await, 4);
}

#[tokio::test]
async fn explicit_target_query_reads_multiple_branches_from_one_handle() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let main_qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 0);

    let feature_qr = db
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 1);
}

#[tokio::test]
async fn resolved_snapshot_stays_pinned_after_branch_advances() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let snapshot_id = db.resolve_snapshot("main").await.unwrap();
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let pinned = db
        .query(
            ReadTarget::Snapshot(snapshot_id.clone()),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(pinned.num_rows(), 0);

    let head = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(head.num_rows(), 1);
}

#[tokio::test]
async fn explicit_target_load_writes_to_named_branch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"Eve","age":22}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let main_qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 0);

    let feature_qr = db
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_updates_main_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let feature_qr = query_branch(
        &mut feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let main_before = query_main(
        &mut main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(main_before.num_rows(), 2);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let merged = query_main(
        &mut main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = feature.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    let qr = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
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
async fn branch_merge_records_actor_on_latest_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = main
        .branch_merge_as("feature", "main", Some("act-ragnor"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.actor_id.as_deref(), Some("act-ragnor"));
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
    assert_eq!(head.graph_commit_id, source_head_before.graph_commit_id);
}

#[tokio::test]
async fn branch_merge_returns_merged_for_non_fast_forward_auto_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let bob = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap()
    .concat_batches()
    .unwrap();
    let bob_ages = bob.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_allows_identical_updates_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let alice = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .concat_batches()
    .unwrap();
    let ages = alice
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 31);
}

#[tokio::test]
async fn merged_rewritten_indexed_table_is_searchable_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_search_db(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "ml-intro"), ("$title", "Orion ML Intro")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "dl-basics"), ("$title", "Orion DL Basics")]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let result = query_main(
        &mut main,
        SEARCH_QUERIES,
        "text_search",
        &params(&[("$q", "Orion")]),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let values: Vec<&str> = (0..slugs.len()).map(|idx| slugs.value(idx)).collect();
    assert!(values.contains(&"ml-intro"));
    assert!(values.contains(&"dl-basics"));

    let ds = snapshot_main(&main)
        .await
        .unwrap()
        .open("node:Doc")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();
    assert_eq!(
        user_indices.len(),
        4,
        "expected rebuilt id BTree plus key-property and title/body indices after rewritten merge"
    );
}

#[tokio::test]
async fn branch_merge_reports_divergent_update_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
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
    let qr = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
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
async fn explicit_target_reads_see_branch_local_writes_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut writer = Omnigraph::open(uri).await.unwrap();
    let mut reader = Omnigraph::open(uri).await.unwrap();
    let mut main_reader = Omnigraph::open(uri).await.unwrap();

    mutate_branch(
        &mut writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let visible = query_branch(
        &mut reader,
        "feature",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(visible.num_rows(), 1);

    let main_result = query_main(
        &mut main_reader,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    assert_eq!(
        feature.branch_list().await.unwrap(),
        vec!["main", "experiment", "feature"]
    );

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let qr = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let experiment_inherited = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_inherited
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );

    experiment.ensure_indices_on("experiment").await.unwrap();

    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
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

    let feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        feature_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        5
    );
    assert_eq!(
        count_rows_branch(&experiment, "experiment", "node:Person").await,
        5
    );
}

#[tokio::test]
async fn branch_edge_only_write_only_branches_edge_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
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

    let feature_qr = query_branch(
        &mut feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "experiment").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let bob = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let bob_batch = bob.concat_batches().unwrap();
    let bob_ages = bob_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("experiment")
    );

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_bob = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
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

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 21)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
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

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
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

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    mutate_main(
        &mut main,
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
async fn branch_merge_reports_unique_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, UNIQUE_SCHEMA, UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Bob"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Carol"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "node:User"
                    && conflict.kind == MergeConflictKind::UniqueViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

#[tokio::test]
async fn branch_merge_reports_cardinality_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, CARDINALITY_SCHEMA, CARDINALITY_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Beta")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "edge:WorksAt"
                    && conflict.kind == MergeConflictKind::CardinalityViolation
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

    let feature = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        4
    );
}

#[tokio::test]
async fn branch_api_rejects_reserved_main_and_same_source_target_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db.branch_create("main").await.unwrap_err();
    assert!(err.to_string().contains("cannot create branch 'main'"));

    let err = db.branch_delete("main").await.unwrap_err();
    assert!(err.to_string().contains("cannot delete branch 'main'"));

    let err = db.branch_merge("main", "main").await.unwrap_err();
    assert!(err.to_string().contains("distinct source and target"));

    db.branch_create("feature").await.unwrap();
    db.sync_branch("feature").await.unwrap();
    let err = db.branch_delete("feature").await.unwrap_err();
    assert!(err.to_string().contains("currently active branch"));
}

#[tokio::test]
async fn branch_delete_removes_owned_table_branches_and_allows_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    main.branch_delete("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main"]);

    main.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut main,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    assert_eq!(count_rows_branch(&main, "feature", "node:Person").await, 5);
}

#[tokio::test]
async fn branch_delete_rejects_branches_still_referenced_by_descendants() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    let err = main.branch_delete("feature").await.unwrap_err();
    assert!(err.to_string().contains("still depends on it"));
}

// ─── Step 9b: Surgical merge publish tests ──────────────────────────────────

#[tokio::test]
async fn merged_table_preserves_row_version_for_unchanged_rows() {
    // After a non-FF merge, unchanged rows retain their original _row_created_at_version.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // Main updates Bob's age → changes one row
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one row
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // After merge: scan node:Person with _row_created_at_version
    let snap = snapshot_main(&main).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner.project(&["id", "_row_created_at_version"]).unwrap();
    let batches: Vec<_> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // Collect _row_created_at_version for each person
    let mut version_by_id: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let versions = batch
            .column_by_name("_row_created_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..ids.len() {
            version_by_id.insert(ids.value(i).to_string(), versions.value(i));
        }
    }

    // The key assertion: NOT all rows have the same _row_created_at_version.
    // With truncate+append, all rows would be re-stamped to the merge version.
    // With surgical merge_insert, unchanged rows keep their original version.
    let unique_versions: std::collections::HashSet<u64> = version_by_id.values().copied().collect();
    assert!(
        unique_versions.len() > 1,
        "After surgical merge, rows should have different _row_created_at_version values \
         (original rows keep old version, merged-in rows get new version). \
         Got only {:?} for ids {:?}",
        unique_versions,
        version_by_id
    );
}

#[tokio::test]
async fn edge_tables_have_id_btree_after_ensure_indices() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.ensure_indices().await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("edge:Knows").await.unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();

    // Should have BTree on id, src, dst = 3 indices
    let index_names: Vec<_> = user_indices.iter().map(|idx| idx.fields.clone()).collect();
    assert!(
        user_indices.len() >= 3,
        "Edge table should have at least 3 indices (id, src, dst), got {:?}",
        index_names
    );
}

#[tokio::test]
async fn merge_delta_only_bumps_changed_rows() {
    // After a non-FF merge, unchanged rows should NOT have _row_last_updated_at_version
    // bumped. Only rows that were actually modified should get new version stamps.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // Main updates Bob's age → changes one Person row
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one Person row (makes it non-FF)
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // Scan all persons with _row_last_updated_at_version
    let snap = snapshot_main(&main).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner
        .project(&["id", "_row_last_updated_at_version"])
        .unwrap();
    let batches: Vec<_> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // Collect all _row_last_updated_at_version values
    let mut versions: Vec<u64> = Vec::new();
    for batch in &batches {
        let v = batch
            .column_by_name("_row_last_updated_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..v.len() {
            versions.push(v.value(i));
        }
    }

    // Not all rows should have the same version — unchanged rows keep old version
    let unique_versions: std::collections::HashSet<u64> = versions.iter().copied().collect();
    assert!(
        unique_versions.len() > 1,
        "After surgical merge, rows should have different _row_last_updated_at_version values. \
         Unchanged rows should keep old version, changed rows get new version. \
         Got only {:?}",
        unique_versions
    );
}
