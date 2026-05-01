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

    // Direct-to-target load: no run lifecycle, single publisher
    // commit lands the row.
    reopened
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"RunOnly","age":31}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();

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

    // Direct-to-target writes: no run state machine, just the
    // published commit lands the row. Verify by reopening and reading.
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
