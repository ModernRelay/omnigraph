mod helpers;

use arrow_array::{Array, Float64Array, Int32Array, Int64Array, StringArray};

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

use helpers::*;

// ─── Count aggregate with GROUP BY ─────────────────────────────────────────

#[tokio::test]
async fn friend_counts_grouped_by_person() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Test data: Alice knows Bob, Alice knows Charlie, Bob knows Diana
    // So: Alice=2 friends, Bob=1 friend (Charlie & Diana have no outgoing knows → dropped)
    let result = query_main(&mut db, TEST_QUERIES, "friend_counts", &ParamMap::new())
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column_by_name("p.name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column_by_name("friends")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Ordered by friends desc: Alice(2), Bob(1)
    assert_eq!(names.value(0), "Alice");
    assert_eq!(counts.value(0), 2);
    assert_eq!(names.value(1), "Bob");
    assert_eq!(counts.value(1), 1);
}

// ─── Global count (no group key) ───────────────────────────────────────────

#[tokio::test]
async fn total_people_global_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(&mut db, TEST_QUERIES, "total_people", &ParamMap::new())
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let total = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 4);
}

// ─── Sum/Avg/Min/Max aggregates ────────────────────────────────────────────

#[tokio::test]
async fn age_stats_per_company() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Test data: Alice(30) worksAt Acme, Bob(25) worksAt Globex
    let result = query_main(&mut db, TEST_QUERIES, "age_stats", &ParamMap::new())
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column_by_name("c.name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Find the Acme row (order not guaranteed without ORDER BY)
    let acme_row = (0..batch.num_rows())
        .find(|&i| names.value(i) == "Acme")
        .expect("Acme should be in results");
    let globex_row = (0..batch.num_rows())
        .find(|&i| names.value(i) == "Globex")
        .expect("Globex should be in results");

    let headcount = batch
        .column_by_name("headcount")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(headcount.value(acme_row), 1);
    assert_eq!(headcount.value(globex_row), 1);

    let avg_age = batch
        .column_by_name("avg_age")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((avg_age.value(acme_row) - 30.0).abs() < 0.01);
    assert!((avg_age.value(globex_row) - 25.0).abs() < 0.01);

    let youngest = batch
        .column_by_name("youngest")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(youngest.value(acme_row), 30);
    assert_eq!(youngest.value(globex_row), 25);

    let total_age = batch
        .column_by_name("total_age")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((total_age.value(acme_row) - 30.0).abs() < 0.01);
    assert!((total_age.value(globex_row) - 25.0).abs() < 0.01);
}

// ─── Aggregate + order + limit ─────────────────────────────────────────────

#[tokio::test]
async fn top_connected_person() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Alice has 2 friends (most connected)
    let result = query_main(&mut db, TEST_QUERIES, "top_connected", &ParamMap::new())
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let names = batch
        .column_by_name("p.name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column_by_name("friends")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(names.value(0), "Alice");
    assert_eq!(counts.value(0), 2);
}

// ─── Inline aggregate query (not from fixture) ────────────────────────────

#[tokio::test]
async fn inline_count_with_no_friends() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Diana has no outgoing Knows edges, so she won't appear in friend_counts.
    // But Alice(2) and Bob(1) do. Verify the count matches.
    let queries = r#"
query fc() {
    match {
        $p: Person
        $p knows $f
    }
    return { $p.name, count($f) as friends }
    order { friends desc }
}
"#;
    let result = query_main(&mut db, queries, "fc", &ParamMap::new())
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    // Only Alice and Bob have outgoing Knows edges
    assert_eq!(batch.num_rows(), 2);
}

#[tokio::test]
async fn inline_global_count_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load only nodes, no edges
    let data = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Company", "data": {"name": "Acme"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Global count — should return 1 row with count=1
    let queries = r#"
query tc() {
    match { $p: Person }
    return { count($p) as total }
}
"#;
    let result = query_main(&mut db, queries, "tc", &ParamMap::new())
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let total = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 1);
}

#[tokio::test]
async fn inline_min_max_string() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let queries = r#"
query name_range() {
    match { $p: Person }
    return {
        min($p.name) as first_name
        max($p.name) as last_name
    }
}
"#;
    let result = query_main(&mut db, queries, "name_range", &ParamMap::new())
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let first = batch
        .column_by_name("first_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last = batch
        .column_by_name("last_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first.value(0), "Alice");
    assert_eq!(last.value(0), "Diana");
}

#[tokio::test]
async fn inline_multi_hop_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Friends of friends count per person
    let queries = r#"
query fof_counts($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $fof
    }
    return { $p.name, count($fof) as fof_count }
}
"#;
    // Alice → Bob, Charlie. Bob → Diana. Charlie → nobody.
    // So Alice's fof = [Diana] → count = 1
    let result = query_main(
        &mut db,
        queries,
        "fof_counts",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let count = batch
        .column_by_name("fof_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 1);
}
