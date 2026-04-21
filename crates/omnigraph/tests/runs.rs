mod helpers;

use std::collections::HashMap;

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use futures::TryStreamExt;
use lance::Dataset;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{Omnigraph, ReadTarget, RunStatus};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

#[derive(Debug, Clone)]
struct PersistedRun {
    run_id: String,
    target_branch: String,
    run_branch: String,
    status: String,
    updated_at: i64,
}

async fn latest_runs(uri: &str) -> Vec<PersistedRun> {
    let runs_uri = format!("{}/_graph_runs.lance", uri);
    let ds = Dataset::open(&runs_uri).await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut latest: HashMap<String, PersistedRun> = HashMap::new();
    for batch in batches {
        let run_ids = batch
            .column_by_name("run_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let target_branches = batch
            .column_by_name("target_branch")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let run_branches = batch
            .column_by_name("run_branch")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let statuses = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let updated_ats = batch
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let record = PersistedRun {
                run_id: run_ids.value(row).to_string(),
                target_branch: target_branches.value(row).to_string(),
                run_branch: run_branches.value(row).to_string(),
                status: statuses.value(row).to_string(),
                updated_at: updated_ats.value(row),
            };
            match latest.get(record.run_id.as_str()) {
                Some(existing) if existing.updated_at >= record.updated_at => {}
                _ => {
                    latest.insert(record.run_id.clone(), record);
                }
            }
        }
    }

    let mut records = latest.into_values().collect::<Vec<_>>();
    records.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    records
}

#[tokio::test]
async fn begin_run_creates_hidden_internal_branch_and_isolates_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let base_snapshot = db.resolve_snapshot("main").await.unwrap();

    let run = db.begin_run("main", Some("test-load")).await.unwrap();

    assert!(run.run_branch.starts_with("__run__"));
    assert_eq!(run.target_branch, "main");
    assert_eq!(run.base_snapshot_id, base_snapshot.as_str());
    assert_eq!(run.status, RunStatus::Running);
    assert_eq!(db.branch_list().await.unwrap(), vec!["main"]);

    db.load(
        &run.run_branch,
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

    let run_qr = db
        .query(
            ReadTarget::branch(run.run_branch.as_str()),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(run_qr.num_rows(), 1);
}

#[tokio::test]
async fn publish_run_merges_internal_branch_into_target_and_marks_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let run = db.begin_run("main", Some("publish-test")).await.unwrap();

    db.load(
        &run.run_branch,
        r#"{"type":"Person","data":{"name":"Eve","age":22}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let published_snapshot = db.publish_run(&run.run_id).await.unwrap();
    let record = db.get_run(&run.run_id).await.unwrap();

    assert_eq!(record.status, RunStatus::Published);
    assert_eq!(
        record.published_snapshot_id.as_deref(),
        Some(published_snapshot.as_str())
    );

    let main_qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 1);
}

#[tokio::test]
async fn abort_run_leaves_target_unchanged_and_deletes_run_branch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let run = db.begin_run("main", Some("abort-test")).await.unwrap();

    db.load(
        &run.run_branch,
        r#"{"type":"Person","data":{"name":"Eve","age":22}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let aborted = db.abort_run(&run.run_id).await.unwrap();
    assert_eq!(aborted.status, RunStatus::Aborted);

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

    let err = db
        .query(
            ReadTarget::branch(run.run_branch.as_str()),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, OmniError::Manifest(ref e) if e.kind == ManifestErrorKind::NotFound)
            || matches!(err, OmniError::Lance(_)),
        "run branch should be gone after abort, got: {}",
        err
    );
}

#[tokio::test]
async fn public_branch_apis_reject_internal_run_refs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let run = db.begin_run("main", Some("guard-test")).await.unwrap();

    let merge_err = db.branch_merge(&run.run_branch, "main").await.unwrap_err();
    match merge_err {
        OmniError::Manifest(message) => assert!(message.message.contains("internal run refs")),
        other => panic!("unexpected error: {}", other),
    }

    let create_err = db.branch_create(&run.run_branch).await.unwrap_err();
    match create_err {
        OmniError::Manifest(message) => assert!(message.message.contains("internal run ref")),
        other => panic!("unexpected error: {}", other),
    }

    let delete_err = db.branch_delete(&run.run_branch).await.unwrap_err();
    match delete_err {
        OmniError::Manifest(message) => assert!(message.message.contains("internal run ref")),
        other => panic!("unexpected error: {}", other),
    }

    let fork_err = db
        .branch_create_from(ReadTarget::branch(run.run_branch.as_str()), "child")
        .await
        .unwrap_err();
    match fork_err {
        OmniError::Manifest(message) => assert!(message.message.contains("internal run ref")),
        other => panic!("unexpected error: {}", other),
    }
}

#[tokio::test]
async fn branch_delete_rejects_target_branches_with_active_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    let run = db.begin_run("feature", Some("delete-guard")).await.unwrap();

    let err = db.branch_delete("feature").await.unwrap_err();
    assert!(err.to_string().contains(run.run_id.as_str()));
    assert!(err.to_string().contains("targeting it is running"));
}

#[tokio::test]
async fn public_load_uses_hidden_transactional_run_and_publishes_it() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let result = load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(result.nodes_loaded.len(), 2);
    assert_eq!(result.edges_loaded.len(), 2);
    assert_eq!(db.branch_list().await.unwrap(), vec!["main"]);

    let runs = latest_runs(uri).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].target_branch, "main");
    assert_eq!(runs[0].status, "published");
    assert!(runs[0].run_branch.starts_with("__run__"));

    let qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

#[tokio::test]
async fn public_load_preserves_staged_edge_ids_on_publish() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let run = db.begin_run("main", Some("preserve-ids-load")).await.unwrap();
    db.load(&run.run_branch, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let mut staged_ids = collect_column_strings(
        &read_table_branch(&db, run.run_branch.as_str(), "edge:Knows").await,
        "id",
    );
    staged_ids.sort();

    db.publish_run(&run.run_id).await.unwrap();

    let mut main_ids = collect_column_strings(&read_table(&db, "edge:Knows").await, "id");
    main_ids.sort();
    assert_eq!(main_ids, staged_ids);
}

#[tokio::test]
async fn failed_public_load_marks_run_failed_and_leaves_target_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let bad = r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"edge":"Knows","from":"Alice","to":"Missing"}"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .unwrap_err();
    match err {
        OmniError::Manifest(message) => assert!(message.message.contains("not found in Person")),
        other => panic!("unexpected error: {}", other),
    }

    let runs = latest_runs(uri).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "failed");
    assert!(runs[0].run_branch.starts_with("__run__"));

    let snap = snapshot_main(&db).await.unwrap();
    let person_count = snap
        .open("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(person_count, 0);
}

#[tokio::test]
async fn public_mutation_uses_hidden_transactional_run_and_publishes_it() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    let runs = latest_runs(uri).await;
    assert!(!runs.is_empty());
    let latest = runs.last().unwrap();
    assert_eq!(latest.target_branch, "main");
    assert_eq!(latest.status, "published");
    assert!(latest.run_branch.starts_with("__run__"));

    let qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

#[tokio::test]
async fn public_mutation_preserves_staged_edge_ids_on_publish() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let run = db
        .begin_run("main", Some("preserve-ids-mutation"))
        .await
        .unwrap();
    db.mutate(
        run.run_branch.as_str(),
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let mut staged_ids = collect_column_strings(
        &read_table_branch(&db, run.run_branch.as_str(), "edge:Knows").await,
        "id",
    );
    staged_ids.sort();

    db.publish_run(&run.run_id).await.unwrap();

    let mut main_ids = collect_column_strings(&read_table(&db, "edge:Knows").await, "id");
    main_ids.sort();
    assert_eq!(main_ids, staged_ids);
}

#[tokio::test]
async fn failed_public_mutation_marks_run_failed_and_leaves_target_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Missing")]),
        )
        .await
        .unwrap_err();
    match err {
        OmniError::Manifest(message) => assert!(message.message.contains("not found")),
        other => panic!("unexpected error: {}", other),
    }

    let runs = latest_runs(uri).await;
    assert!(!runs.is_empty());
    let latest = runs.last().unwrap();
    assert_eq!(latest.status, "failed");
    assert!(latest.run_branch.starts_with("__run__"));

    let qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 2);
}

#[tokio::test]
async fn concurrent_conflicting_run_publish_fails_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let run_a = db.begin_run("main", Some("conflict-a")).await.unwrap();
    let run_b = db.begin_run("main", Some("conflict-b")).await.unwrap();

    db.mutate(
        run_a.run_branch.as_str(),
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    db.mutate(
        run_b.run_branch.as_str(),
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 32)]),
    )
    .await
    .unwrap();

    db.publish_run(&run_a.run_id).await.unwrap();
    let publish_b = db.publish_run(&run_b.run_id).await;
    assert!(publish_b.is_err(), "second conflicting publish should fail");
    let err = publish_b.unwrap_err().to_string();
    assert!(
        err.contains("conflict") || err.contains("divergent") || err.contains("Alice"),
        "unexpected conflict error: {}",
        err
    );

    let alice = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    let rows = alice.to_rust_json();
    assert_eq!(alice.num_rows(), 1);
    assert_eq!(rows[0]["p.age"], serde_json::json!(31));

    let run_a_record = db.get_run(&run_a.run_id).await.unwrap();
    assert_eq!(run_a_record.status, RunStatus::Published);
    let run_b_record = db.get_run(&run_b.run_id).await.unwrap();
    assert_eq!(run_b_record.status, RunStatus::Running);
}

#[tokio::test]
async fn public_mutation_records_actor_on_run_and_published_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    db.mutate_as(
        "main",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
        Some("act-andrew"),
    )
    .await
    .unwrap();

    let runs = db.list_runs().await.unwrap();
    let run = runs
        .iter()
        .find(|run| run.operation_hash.as_deref() == Some("mutation:set_age:branch=main"))
        .expect("published mutation run should exist");
    assert_eq!(run.actor_id.as_deref(), Some("act-andrew"));
    assert_eq!(run.status, RunStatus::Published);

    let head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.actor_id.as_deref(), Some("act-andrew"));
}

#[tokio::test]
async fn run_branches_do_not_accumulate_across_repeated_loads() {
    // MR-674: run branches are transactional scaffolding. Every terminal
    // state (Published, Aborted, Failed) deletes the branch. Verifying the
    // invariant end-to-end: after 10 publishes and one abort, only main
    // should remain.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    for i in 0..10 {
        let payload = format!(
            r#"{{"type":"Person","data":{{"name":"p{}","age":{}}}}}"#,
            i, i
        );
        load_jsonl(&mut db, &payload, LoadMode::Append)
            .await
            .unwrap();
    }

    let aborted_run = db.begin_run("main", Some("abort-me")).await.unwrap();
    db.abort_run(&aborted_run.run_id).await.unwrap();

    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
    let all_branches = Omnigraph::open(uri)
        .await
        .unwrap()
        .branch_list()
        .await
        .unwrap();
    assert_eq!(all_branches, vec!["main".to_string()]);
}

#[tokio::test]
async fn failed_load_deletes_run_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let bad = r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"edge":"Knows","from":"Alice","to":"Missing"}"#;
    let _ = load_jsonl(&mut db, bad, LoadMode::Overwrite).await;

    let runs = latest_runs(uri).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "failed");

    let err = db
        .snapshot_of(ReadTarget::branch(runs[0].run_branch.as_str()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OmniError::Manifest(ref e) if e.kind == ManifestErrorKind::NotFound)
            || matches!(err, OmniError::Lance(_)),
        "failed run's branch should be gone, got: {}",
        err
    );
}
