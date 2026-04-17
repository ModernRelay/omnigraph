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

const LIFE_GRAPH_SCHEMA: &str = include_str!("fixtures/life-graph.pg");

/// Generate `n` Artifact JSONL rows with wide indexed name fields.
/// The name column has @index (inverted index), so wider names = larger
/// index data files, which triggers the RustFS streaming bug.
fn generate_artifact_jsonl(n: usize) -> String {
    let mut lines = Vec::with_capacity(n);
    for i in 0..n {
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
    lines.join("\n")
}

/// Generate `n` Artifact rows with slug offsets (for non-overlapping merge loads).
fn generate_artifact_jsonl_offset(n: usize, offset: usize) -> String {
    let mut lines = Vec::with_capacity(n);
    for i in 0..n {
        let idx = offset + i;
        let padding = "x".repeat(50 + (i % 200));
        let name = format!(
            "sender: [DM with Person {person}] This is message content for artifact number {idx}",
            person = i % 200,
        );
        lines.push(format!(
            r#"{{"type":"Artifact","data":{{"slug":"art-{idx}","name":"{name}","kind":"message","source":"whatsapp","source_ref":"stanza-{idx:08}","content":"[DM with Person {person}] sender: This is message {idx}. {padding}","timestamp":"2026-04-15T00:00:00Z","createdAt":"2026-04-15T00:00:00Z","updatedAt":"2026-04-15T00:00:00Z"}}}}"#,
            person = i % 200,
        ));
    }
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// MR-640 regression tests: S3 index staging
// ---------------------------------------------------------------------------

/// Core regression test: overwrite load of 14K rows on main.
/// Deterministic FAIL before the fix on RustFS.
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_load_builds_indices_without_error() {
    let Some(uri) = s3_test_repo_uri("large-load-indices") else {
        eprintln!("skipping s3 large load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();
    let data = generate_artifact_jsonl(14_000);

    load_jsonl(&mut db, &data, LoadMode::Overwrite).await.unwrap();

    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 14_000, "expected 14,000 Artifact rows after load");
}

/// Tests branch-aware index staging: loads data via db.load() which creates
/// a transactional __run__ branch, writes data there, then publishes to main.
/// The branch URI must include /tree/__run__XXXX for the download to get the
/// right data. This would fail if build_indices_via_local_staging used the
/// base table path instead of ds.uri().
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_load_via_run_builds_indices_on_correct_branch() {
    let Some(uri) = s3_test_repo_uri("large-load-run-branch") else {
        eprintln!("skipping s3 run branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();
    let data = generate_artifact_jsonl(14_000);

    // db.load() wraps in a run: begin_run → load on __run__ → publish to main
    db.load("main", &data, LoadMode::Overwrite).await.unwrap();

    // Verify data published to main
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 14_000, "expected 14,000 Artifact rows via run");

    // Verify a published run exists
    let runs = reopened.list_runs().await.unwrap();
    assert!(
        runs.iter().any(|r| r.status.as_str() == "published"),
        "expected a published run"
    );
}

/// Tests sequential merge loads accumulating data across multiple runs.
/// Each load creates a new __run__ branch, merges into main, and triggers
/// index building on increasingly large datasets. This mirrors the original
/// WhatsApp import pattern that surfaced MR-640.
#[tokio::test(flavor = "multi_thread")]
async fn s3_sequential_merge_loads_accumulate_correctly() {
    let Some(uri) = s3_test_repo_uri("large-load-merge-seq") else {
        eprintln!("skipping s3 merge test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();

    // Load 3 batches of 5K rows each via merge, accumulating to 15K total
    for batch in 0..3 {
        let data = generate_artifact_jsonl_offset(5_000, batch * 5_000);
        db.load("main", &data, LoadMode::Merge).await.unwrap();
    }

    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 15_000, "expected 15,000 Artifact rows after 3 merge loads");

    // Should have 3 published runs
    let runs = reopened.list_runs().await.unwrap();
    let published = runs.iter().filter(|r| r.status.as_str() == "published").count();
    assert_eq!(published, 3, "expected 3 published runs");
}

/// Tests loading onto a named feature branch (not main). The index staging
/// must use the branch-aware dataset URI for the feature branch, not the
/// base table path which would point at main's data.
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_load_on_feature_branch() {
    let Some(uri) = s3_test_repo_uri("large-load-feature-branch") else {
        eprintln!("skipping s3 feature branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();

    // Seed main with a small load so the branch has something to fork from
    let seed = r#"{"type":"Person","data":{"slug":"p-seed","name":"Seed","relation":"other","createdAt":"2026-04-15T00:00:00Z","updatedAt":"2026-04-15T00:00:00Z"}}"#;
    load_jsonl(&mut db, seed, LoadMode::Overwrite).await.unwrap();

    // Create feature branch and load large data onto it
    db.branch_create("feature").await.unwrap();
    let data = generate_artifact_jsonl(14_000);
    db.load("feature", &data, LoadMode::Overwrite).await.unwrap();

    // Verify feature branch has the data
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let feature_snapshot = reopened.snapshot_of("feature").await.unwrap();
    let ds = feature_snapshot.open("node:Artifact").await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 14_000, "expected 14,000 Artifact rows on feature branch");

    // Main should NOT have the artifacts (only the seed person)
    let main_snapshot = reopened.snapshot_of("main").await.unwrap();
    let main_ds = main_snapshot.open("node:Artifact").await.unwrap();
    let main_count = main_ds.count_rows(None).await.unwrap();
    assert_eq!(main_count, 0, "main should have 0 Artifact rows");
}

/// Tests that transactional mutations on S3 repos still work after the
/// index-deferral change. The mutation path (exec/mutation.rs) goes through
/// publish_run and must call ensure_indices_on afterward for S3, same as
/// the loader path. Without this, mutations on S3 would commit data but
/// never build indexes.
#[tokio::test(flavor = "multi_thread")]
async fn s3_mutation_after_load_builds_indices() {
    let Some(uri) = s3_test_repo_uri("mutation-indices") else {
        eprintln!("skipping s3 mutation test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // Mutate via the transactional path (begin_run → mutate → publish_run)
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "S3-Mutation-Test")], &[("$age", 42)]),
    )
    .await
    .unwrap();

    // Verify the mutation is visible after reopen
    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let result = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "S3-Mutation-Test")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(result[0]["p.name"], "S3-Mutation-Test");
    assert_eq!(result[0]["p.age"], 42);

    // Verify a published run was created for the mutation
    let runs = reopened.list_runs().await.unwrap();
    assert!(
        runs.iter().any(|r| r.status.as_str() == "published"),
        "expected a published run for the mutation"
    );
}

/// Tests that mutations work correctly on a non-main branch on S3.
/// This exercises the branch-aware index staging in the mutation path.
#[tokio::test(flavor = "multi_thread")]
async fn s3_mutation_on_feature_branch() {
    let Some(uri) = s3_test_repo_uri("mutation-feature-branch") else {
        eprintln!("skipping s3 mutation branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.branch_create("feature").await.unwrap();

    // Mutate on the feature branch
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Branch-Only")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    // Verify mutation is visible on feature branch
    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let feature_result = query_branch(
        &mut reopened,
        "feature",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Branch-Only")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_result.num_rows(), 1);

    // Verify mutation is NOT visible on main
    let main_result = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Branch-Only")]),
    )
    .await
    .unwrap();
    assert_eq!(main_result.num_rows(), 0, "main should not see branch mutation");
}
