mod helpers;

use omnigraph::db::MergeOutcome;
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

#[tokio::test(flavor = "multi_thread")]
async fn s3_compatible_repo_lifecycle_works() {
    let Some(uri) = s3_test_repo_uri("omnigraph-runtime") else {
        eprintln!("skipping s3 runtime test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    assert!(snapshot.entry("node:Person").is_some());
    assert!(snapshot.entry("edge:Knows").is_some());

    let alice = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(alice[0]["p.name"], "Alice");

    reopened
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "RustFS-Eve")], &[("$age", 29)]),
        )
        .await
        .unwrap();

    let run = reopened
        .begin_run("main", Some("s3-runtime-run"))
        .await
        .unwrap();
    reopened
        .load(
            &run.run_branch,
            r#"{"type":"Person","data":{"name":"RunOnly","age":31}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    reopened.publish_run(&run.run_id).await.unwrap();

    let runs = reopened.list_runs().await.unwrap();
    assert!(
        runs.iter()
            .any(|record| { record.run_id == run.run_id && record.status.as_str() == "published" }),
        "expected published run record in {:?}",
        runs
    );

    let mut reopened_again = Omnigraph::open(&uri).await.unwrap();
    let eve = query_main(
        &mut reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RustFS-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(eve[0]["p.name"], "RustFS-Eve");

    let run_only = query_main(
        &mut reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RunOnly")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(run_only[0]["p.name"], "RunOnly");
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_branch_change_merge_flow_works() {
    let Some(uri) = s3_test_repo_uri("omnigraph-branching") else {
        eprintln!("skipping s3 branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut main = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut main, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(&uri).await.unwrap();
    feature
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Feature-Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let before_merge = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(before_merge.num_rows(), 0);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let after_merge = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(after_merge[0]["p.name"], "Feature-Eve");
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_public_load_uses_hidden_run_and_publishes() {
    let Some(uri) = s3_test_repo_uri("omnigraph-public-load") else {
        eprintln!("skipping s3 public load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Loaded-Over-S3","age":34}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let runs = db.list_runs().await.unwrap();
    assert!(
        runs.iter().any(|record| {
            record.target_branch == "main" && record.status.as_str() == "published"
        }),
        "expected published transactional run in {:?}",
        runs
    );

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let loaded = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Loaded-Over-S3")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(loaded[0]["p.name"], "Loaded-Over-S3");
}

/// Regression test for MR-640: Lance BTree index creation fails on RustFS
/// when a node type's Lance data file exceeds a size threshold. The index
/// builder reads column data back from S3 via ranged GETs, and RustFS breaks
/// the HTTP response body streaming for larger objects.
///
/// Trigger: complex schema (many node/edge types) + 14K rows with wide
/// indexed name fields push the Lance data file past the RustFS threshold.
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_load_builds_indices_without_error() {
    let Some(uri) = s3_test_repo_uri("large-load-indices") else {
        eprintln!("skipping s3 large load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let schema = include_str!("fixtures/life-graph.pg");
    let mut db = Omnigraph::init(&uri, schema).await.unwrap();

    // Generate 14,000 rows with wide name + content fields. The `name` column
    // has @index (inverted index), so wider names = larger index data files.
    let mut lines = Vec::with_capacity(14_000);
    for i in 0..14_000 {
        let padding = "x".repeat(50 + (i % 200));
        let name = format!(
            "sender: [DM with Person {person}] This is message content for artifact number {i}",
            person = i % 200,
        );
        lines.push(format!(
            r#"{{"type":"Artifact","data":{{"slug":"art-{i}","name":"{name}","kind":"message","source":"whatsapp","source_ref":"stanza-{i:08}","content":"[DM with Person {person}] sender: This is message {i}. {padding}","timestamp":"2026-04-15T00:00:00Z","createdAt":"2026-04-15T00:00:00Z","updatedAt":"2026-04-15T00:00:00Z"}}}}"#,
            person = i % 200,
        ));
    }
    let data = lines.join("\n");

    load_jsonl(&mut db, &data, LoadMode::Overwrite).await.unwrap();

    // Verify data survived the round-trip
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 14_000, "expected 14,000 Artifact rows after load");
}
