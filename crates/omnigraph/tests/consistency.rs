mod helpers;

use arrow_array::{Array, Date32Array, Int32Array, StringArray};
use futures::TryStreamExt;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

use helpers::*;

// ─── Snapshot data-level isolation ──────────────────────────────────────────

#[tokio::test]
async fn snapshot_returns_stale_data_after_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Snapshot BEFORE mutation
    let snap_before = snapshot_main(&db).await.unwrap();

    // Insert a new person
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Snapshot AFTER mutation
    let snap_after = snapshot_main(&db).await.unwrap();

    // Old snapshot should still see 4 persons
    let ds_before = snap_before.open("node:Person").await.unwrap();
    assert_eq!(ds_before.count_rows(None).await.unwrap(), 4);

    // New snapshot should see 5 persons
    let ds_after = snap_after.open("node:Person").await.unwrap();
    assert_eq!(ds_after.count_rows(None).await.unwrap(), 5);

    // Verify Eve is NOT in old snapshot's data
    let batches_before: Vec<arrow_array::RecordBatch> = ds_before
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids_before = collect_column_strings(&batches_before, "id");
    assert!(!ids_before.contains(&"Eve".to_string()));

    // Verify Eve IS in new snapshot's data
    let batches_after: Vec<arrow_array::RecordBatch> = ds_after
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids_after = collect_column_strings(&batches_after, "id");
    assert!(ids_after.contains(&"Eve".to_string()));
}

// ─── LoadMode::Merge ────────────────────────────────────────────────────────

#[tokio::test]
async fn load_merge_upserts_existing_and_inserts_new() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load Alice(30) and Bob(25) via Overwrite
    let initial = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}"#;
    load_jsonl(&mut db, initial, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, 2);

    // Merge: Alice updated to age=31, Charlie is new
    let merge_data = r#"{"type": "Person", "data": {"name": "Alice", "age": 31}}
{"type": "Person", "data": {"name": "Charlie", "age": 35}}"#;
    load_jsonl(&mut db, merge_data, LoadMode::Merge)
        .await
        .unwrap();

    // Should have 3 persons total (not 4)
    assert_eq!(count_rows(&db, "node:Person").await, 3);

    // Verify individual values
    let batches = read_table(&db, "node:Person").await;
    let batch = &batches[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    for i in 0..batch.num_rows() {
        match ids.value(i) {
            "Alice" => assert_eq!(ages.value(i), 31, "Alice should be updated to 31"),
            "Bob" => assert_eq!(ages.value(i), 25, "Bob should be unchanged"),
            "Charlie" => assert_eq!(ages.value(i), 35, "Charlie should be inserted"),
            other => panic!("unexpected person: {}", other),
        }
    }
}

// ─── Multi-writer refresh ───────────────────────────────────────────────────

#[tokio::test]
async fn explicit_target_query_sees_other_writer_commits_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();

    // Two independent handles to the same repo
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    // Writer 1 inserts Eve
    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Explicit-target reads resolve the latest branch head and should see Eve
    let qr = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1, "explicit target reads should see Eve");
}

#[tokio::test]
async fn explicit_target_query_rebuilds_graph_index_after_external_edge_write() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    let warm = query_main(
        &mut db2,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(warm.num_rows(), 2);

    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let refreshed = query_main(
        &mut db2,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(
        refreshed.num_rows(),
        3,
        "explicit target reads should rebuild topology after edge change"
    );

    let batch = refreshed.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    assert!(values.contains(&"Bob"));
    assert!(values.contains(&"Diana"));
}

// ─── Null handling ──────────────────────────────────────────────────────────

#[tokio::test]
async fn null_values_in_filter_and_projection() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load data: Alice has age, Bob has null age, Charlie has age
    let data = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob"}}
{"type": "Person", "data": {"name": "Charlie", "age": 35}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Filter: age > 30 should exclude Bob (null) and Alice (30), keep Charlie (35)
    let queries = r#"
query older_than_30() {
    match {
        $p: Person
        $p.age > 30
    }
    return { $p.name, $p.age }
    order { $p.age desc }
}

query all_persons() {
    match { $p: Person }
    return { $p.name, $p.age }
    order { $p.age desc }
}
"#;

    let result = query_main(&mut db, queries, "older_than_30", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Charlie");

    // Projection: Bob's age should be null
    let all = query_main(&mut db, queries, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    let batch = &all.batches()[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    for i in 0..batch.num_rows() {
        if ids.value(i) == "Bob" {
            assert!(ages.is_null(i), "Bob's age should be null");
        }
    }
}

// ─── Graph index after node+edge insert ─────────────────────────────────────

#[tokio::test]
async fn traversal_works_after_node_then_edge_insert() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Warm up the graph index cache by running a traversal
    let _ = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    // Insert a new node (does NOT invalidate graph index)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    // Insert an edge from Frank → Alice (DOES invalidate graph index)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Frank"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    // Traversal should work: Frank → Alice
    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Frank")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
}

// ─── Edge property insert ───────────────────────────────────────────────────

#[tokio::test]
async fn insert_edge_with_property() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Knows has `since: Date?` property
    let queries = r#"
query add_friend_since($from: String, $to: String, $since: Date) {
    insert Knows { from: $from, to: $to, since: $since }
}
"#;
    let mut p = params(&[("$from", "Diana"), ("$to", "Bob")]);
    p.insert("since".to_string(), Literal::Date("2024-06-15".to_string()));

    let result = mutate_main(&mut db, queries, "add_friend_since", &p)
        .await
        .unwrap();
    assert_eq!(result.affected_edges, 1);

    // Verify the edge property was stored
    let batches = read_table(&db, "edge:Knows").await;
    let mut found = false;
    for batch in &batches {
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dsts = batch
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let since = batch
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if srcs.value(i) == "Diana" && dsts.value(i) == "Bob" {
                assert!(!since.is_null(i), "since should not be null");
                found = true;
            }
        }
    }
    assert!(found, "should find Diana→Bob edge");
}

// ─── Update / delete no-match ───────────────────────────────────────────────

#[tokio::test]
async fn update_nonexistent_returns_zero_affected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Nobody")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
}

#[tokio::test]
async fn delete_nonexistent_returns_zero_affected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Nobody")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 0);

    // All 4 persons still intact
    assert_eq!(count_rows(&db, "node:Person").await, 4);
}

// ─── Large batch load ───────────────────────────────────────────────────────

#[tokio::test]
async fn large_batch_load_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let schema = r#"
node Item {
    name: String @key
    value: I32
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Generate 500 items
    let mut lines = Vec::with_capacity(500);
    for i in 0..500 {
        lines.push(format!(
            r#"{{"type": "Item", "data": {{"name": "item_{:04}", "value": {}}}}}"#,
            i, i
        ));
    }
    let data = lines.join("\n");
    load_jsonl(&mut db, &data, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Item").await, 500);

    // Query with filter — value > 490
    let queries = r#"
query high_value() {
    match {
        $i: Item
        $i.value > 490
    }
    return { $i.name, $i.value }
    order { $i.value asc }
}
"#;
    let result = query_main(&mut db, queries, "high_value", &ParamMap::new())
        .await
        .unwrap();

    // Items 491..499 = 9 items
    assert_eq!(result.num_rows(), 9);
    let batch = &result.batches()[0];
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 491);
    assert_eq!(values.value(8), 499);
}

// ─── Regression: mutation on stale handle detects version drift ──────────────

#[tokio::test]
async fn stale_handle_mutation_detects_version_drift() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    // Writer 1 inserts — advances the Person sub-table version
    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Writer 2 (stale) tries to mutate — should detect version drift
    let result = mutate_main(
        &mut db2,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await;
    assert!(result.is_err(), "stale handle should detect version drift");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("version drift"),
        "error should mention version drift: {}",
        err
    );

    // After refresh, the mutation should succeed
    sync_main(&mut db2).await.unwrap();
    let result = mutate_main(
        &mut db2,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await;
    assert!(result.is_ok(), "refreshed handle should succeed");
}
