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

// ---------------------------------------------------------------------------
// MR-640: S3 index staging regression tests
// ---------------------------------------------------------------------------

const LIFE_GRAPH_SCHEMA: &str = include_str!("fixtures/life-graph.pg");

/// Generate `n` Artifact JSONL rows starting at `offset`. Wide name fields
/// (~80 chars, indexed) push the Lance data file past the RustFS streaming
/// threshold at ~14K rows.
fn generate_artifacts(n: usize, offset: usize) -> String {
    let mut lines = Vec::with_capacity(n);
    for i in 0..n {
        let idx = offset + i;
        let padding = "x".repeat(50 + (i % 200));
        let person = i % 200;
        let name = format!(
            "sender: [DM with Person {person}] This is message content for artifact number {idx}",
        );
        lines.push(format!(
            r#"{{"type":"Artifact","data":{{"slug":"art-{idx}","name":"{name}","kind":"message","source":"whatsapp","source_ref":"stanza-{idx:08}","content":"[DM with Person {person}] sender: This is message {idx}. {padding}","timestamp":"2026-04-15T00:00:00Z","createdAt":"2026-04-15T00:00:00Z","updatedAt":"2026-04-15T00:00:00Z"}}}}"#,
        ));
    }
    lines.join("\n")
}

/// Helper: assert that a dataset has a BTree index on the given column.
async fn assert_has_btree_index(ds: &lance::Dataset, column: &str) {
    use lance_index::{DatasetIndexExt, is_system_index};
    let field_id = ds
        .schema()
        .field(column)
        .unwrap_or_else(|| panic!("dataset missing column '{column}'"))
        .id;
    let indices = ds.load_indices().await.unwrap();
    let has_index = indices.iter().any(|idx| {
        !is_system_index(idx)
            && idx.fields.len() == 1
            && idx.fields[0] == field_id
            && idx
                .index_details
                .as_ref()
                .map(|d| d.type_url.ends_with("BTreeIndexDetails"))
                .unwrap_or(false)
    });
    assert!(has_index, "expected BTree index on '{column}' but none found");
}

// -- Threshold test: proves the bug exists and the fix works ----------------

/// MR-640 core regression: 14K rows with wide indexed fields on a complex
/// schema. Before the fix, this deterministically fails with:
///   `create BTree index on node:Artifact(id): LanceError(IO): Generic S3
///    error: HTTP error: request or response body error`
/// After the fix, data loads successfully and indexes are present.
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_load_builds_indices_without_error() {
    let Some(uri) = s3_test_repo_uri("large-load-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();
    let data = generate_artifacts(14_000, 0);
    load_jsonl(&mut db, &data, LoadMode::Overwrite).await.unwrap();

    // Verify data
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 14_000);

    // Verify indexes were actually built (not just data queryable)
    assert_has_btree_index(&ds, "id").await;
}

// -- Codepath tests: small data, verify the S3 staging path works ----------

/// Load via db.load() (transactional run path) on S3. Verifies that
/// build_indices_via_local_staging uses the branch-aware URI (ds.uri())
/// for the __run__ branch, not the base table path.
#[tokio::test(flavor = "multi_thread")]
async fn s3_load_via_run_creates_indices() {
    let Some(uri) = s3_test_repo_uri("run-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    db.load("main", TEST_DATA, LoadMode::Overwrite).await.unwrap();

    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Person").await.unwrap();
    assert_has_btree_index(&ds, "id").await;

    let runs = reopened.list_runs().await.unwrap();
    assert!(runs.iter().any(|r| r.status.as_str() == "published"));
}

/// Mutation via the transactional path on S3. Verifies that
/// exec/mutation.rs calls ensure_indices_on after publish_run.
#[tokio::test(flavor = "multi_thread")]
async fn s3_mutation_creates_indices() {
    let Some(uri) = s3_test_repo_uri("mutation-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite).await.unwrap();

    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "S3-Mut")], &[("$age", 42)]),
    )
    .await
    .unwrap();

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let result = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "S3-Mut")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(result[0]["p.name"], "S3-Mut");

    // Verify indexes exist after mutation
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Person").await.unwrap();
    assert_has_btree_index(&ds, "id").await;
}

/// Load on a named feature branch on S3. Verifies branch-aware index
/// staging for non-main branches and branch isolation.
#[tokio::test(flavor = "multi_thread")]
async fn s3_load_on_feature_branch_creates_indices() {
    let Some(uri) = s3_test_repo_uri("branch-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite).await.unwrap();
    db.branch_create("feature").await.unwrap();

    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"BranchPerson","age":25}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let reopened = Omnigraph::open(&uri).await.unwrap();

    // Feature branch has data + indexes
    let feature_snap = reopened.snapshot_of("feature").await.unwrap();
    let feature_ds = feature_snap.open("node:Person").await.unwrap();
    assert_has_btree_index(&feature_ds, "id").await;

    // Main does NOT have the branch-only data
    let mut main_db = Omnigraph::open(&uri).await.unwrap();
    let main_result = query_main(
        &mut main_db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "BranchPerson")]),
    )
    .await
    .unwrap();
    assert_eq!(main_result.num_rows(), 0, "main should not see branch data");
}

/// Mutation on a feature branch on S3. Verifies branch-aware index
/// staging in the mutation path.
#[tokio::test(flavor = "multi_thread")]
async fn s3_mutation_on_feature_branch_creates_indices() {
    let Some(uri) = s3_test_repo_uri("mutation-branch-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite).await.unwrap();
    db.branch_create("feature").await.unwrap();

    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "BranchMut")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snap = reopened.snapshot_of("feature").await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    assert_has_btree_index(&ds, "id").await;

    // Main does NOT see the mutation
    let mut main_db = Omnigraph::open(&uri).await.unwrap();
    let main_result = query_main(
        &mut main_db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "BranchMut")]),
    )
    .await
    .unwrap();
    assert_eq!(main_result.num_rows(), 0);
}

/// Sequential merge loads accumulating data across multiple runs.
/// Mirrors the WhatsApp import pattern that originally surfaced MR-640.
#[tokio::test(flavor = "multi_thread")]
async fn s3_sequential_merge_loads_accumulate_with_indices() {
    let Some(uri) = s3_test_repo_uri("merge-seq-indices") else {
        eprintln!("skipping: OMNIGRAPH_S3_TEST_BUCKET not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, LIFE_GRAPH_SCHEMA).await.unwrap();

    // 3 batches of 5K rows, accumulating to 15K
    for batch in 0..3 {
        let data = generate_artifacts(5_000, batch * 5_000);
        db.load("main", &data, LoadMode::Merge).await.unwrap();
    }

    let reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    let ds = snapshot.open("node:Artifact").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 15_000);
    assert_has_btree_index(&ds, "id").await;

    let runs = reopened.list_runs().await.unwrap();
    let published = runs.iter().filter(|r| r.status.as_str() == "published").count();
    assert_eq!(published, 3);
}
