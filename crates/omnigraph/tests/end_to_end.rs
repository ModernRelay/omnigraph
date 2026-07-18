mod helpers;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl, load_jsonl_file};
use omnigraph_compiler::ir::ParamMap;

use helpers::*;

// ─── Init + Load ────────────────────────────────────────────────────────────

#[tokio::test]
async fn init_creates_schema_file_and_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    assert!(dir.path().join("_schema.pg").exists());
    assert!(dir.path().join("__manifest").exists());
    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
}

#[tokio::test]
async fn open_restores_full_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let original = init_and_load(&dir).await;
    let v = version_main(&original).await.unwrap();
    drop(original);

    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened.catalog().node_types.len(), 2);
    assert_eq!(reopened.catalog().edge_types.len(), 2);
    // Version should be what we left it at
    // (manifest was committed during load)
    assert!(version_main(&reopened).await.unwrap() >= v);
}

#[tokio::test]
async fn load_populates_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let snap = snapshot_main(&db).await.unwrap();

    // 4 persons
    let person_ds = snap.open("node:Person").await.unwrap();
    assert_eq!(person_ds.count_rows(None).await.unwrap(), 4);

    // 2 companies
    let company_ds = snap.open("node:Company").await.unwrap();
    assert_eq!(company_ds.count_rows(None).await.unwrap(), 2);

    // 3 Knows edges
    let knows_ds = snap.open("edge:Knows").await.unwrap();
    assert_eq!(knows_ds.count_rows(None).await.unwrap(), 3);

    // 2 WorksAt edges
    let works_at_ds = snap.open("edge:WorksAt").await.unwrap();
    assert_eq!(works_at_ds.count_rows(None).await.unwrap(), 2);
}

// ─── Read consistency ───────────────────────────────────────────────────────

#[tokio::test]
async fn node_ids_are_key_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let batches = read_table(&db, "node:Person").await;
    let mut ids = collect_column_strings(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec!["Alice", "Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn node_properties_are_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

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

    // Find Alice's row and check age
    let alice_idx = (0..ids.len()).find(|&i| ids.value(i) == "Alice").unwrap();
    assert_eq!(ages.value(alice_idx), 30);
}

#[tokio::test]
async fn entity_at_returns_typed_json_values() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Flagged {
    slug: String @key
    active: Bool
    rating: I32?
}
"#;
    let data = r#"{"type":"Flagged","data":{"slug":"alpha","active":true,"rating":42}}"#;

    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let entity = db
        .entity_at_target(ReadTarget::branch("main"), "node:Flagged", "alpha")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entity["id"], serde_json::json!("alpha"));
    assert_eq!(entity["active"], serde_json::json!(true));
    assert_eq!(entity["rating"], serde_json::json!(42));
}

#[tokio::test]
async fn nullable_vectors_round_trip_as_null() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Doc {
    slug: String @key
    embedding: Vector(2)?
}
"#;
    let data = r#"{"type":"Doc","data":{"slug":"a"}}
{"type":"Doc","data":{"slug":"b","embedding":[1.0,2.0]}}"#;

    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let missing = db
        .entity_at_target(ReadTarget::branch("main"), "node:Doc", "a")
        .await
        .unwrap()
        .unwrap();
    let present = db
        .entity_at_target(ReadTarget::branch("main"), "node:Doc", "b")
        .await
        .unwrap()
        .unwrap();

    assert!(missing["embedding"].is_null());
    assert_eq!(present["embedding"], serde_json::json!([1.0, 2.0]));
}

#[tokio::test]
async fn edge_src_dst_reference_node_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let batches = read_table(&db, "edge:Knows").await;
    let batch = &batches[0];
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

    // Collect all (src, dst) pairs
    let mut edges: Vec<(&str, &str)> = (0..batch.num_rows())
        .map(|i| (srcs.value(i), dsts.value(i)))
        .collect();
    edges.sort();

    assert_eq!(
        edges,
        vec![("Alice", "Bob"), ("Alice", "Charlie"), ("Bob", "Diana")]
    );
}

#[tokio::test]
async fn edge_ids_are_unique_strings() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let batches = read_table(&db, "edge:Knows").await;
    let batch = &batches[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let id_values: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
    // All unique
    let mut deduped = id_values.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(id_values.len(), deduped.len());
    // All non-empty
    assert!(id_values.iter().all(|id| !id.is_empty()));
}

// ─── Load modes ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn overwrite_replaces_data() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load full data
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // Overwrite to a small SELF-CONSISTENT image. Overwrite is per-table, so a
    // Person-only overwrite would drop Alice/Bob while the retained Knows/WorksAt
    // edges still reference them — a now-rejected orphan (see
    // `validators::overwrite_node_removal_rejects_retained_orphan_edge`). To
    // replace the graph, overwrite the edge tables too; Company stays retained
    // and Zara->Acme references it.
    let small = r#"{"type": "Person", "data": {"name": "Zara", "age": 40}}
{"edge": "Knows", "from": "Zara", "to": "Zara"}
{"edge": "WorksAt", "from": "Zara", "to": "Acme"}"#;
    load_jsonl(&mut db, small, LoadMode::Overwrite)
        .await
        .unwrap();

    let batches = read_table(&db, "node:Person").await;
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "Zara");
}

#[tokio::test]
async fn append_adds_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let batch1 = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#;
    let batch2 = r#"{"type": "Person", "data": {"name": "Bob", "age": 25}}"#;

    load_jsonl(&mut db, batch1, LoadMode::Overwrite)
        .await
        .unwrap();
    load_jsonl(&mut db, batch2, LoadMode::Append).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 2);
}

// ─── Load from fixture file ─────────────────────────────────────────────────

#[tokio::test]
async fn load_from_file_works() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let fixture_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/test.jsonl");
    load_jsonl_file(&mut db, fixture_path, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 4);
}

// ─── Signals fixture (complex @key schema) ──────────────────────────────────

#[tokio::test]
async fn signals_fixture_loads_correctly() {
    let schema = include_str!("fixtures/signals.pg");
    let data = include_str!("fixtures/signals.jsonl");

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();

    // Verify some types have data
    let company_ds = snap.open("node:Company").await.unwrap();
    assert!(company_ds.count_rows(None).await.unwrap() > 0);

    // Verify node IDs are @key values (slug)
    let batches: Vec<arrow_array::RecordBatch> = company_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = collect_column_strings(&batches, "id");
    // Should contain slug values like "aws", "openai", etc.
    assert!(ids.contains(&"aws".to_string()));
    assert!(ids.contains(&"openai".to_string()));
}

// ─── Query execution ────────────────────────────────────────────────────────

#[tokio::test]
async fn query_get_person_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    assert_eq!(result.num_rows(), 1);
    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");

    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 30);
}

#[tokio::test]
async fn query_get_person_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Nobody")]),
    )
    .await
    .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[tokio::test]
async fn query_adults_filtered_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(&mut db, TEST_QUERIES, "adults", &ParamMap::new())
        .await
        .unwrap();

    // Only Charlie (35) matches age > 30, ordered desc
    assert_eq!(result.num_rows(), 1);
    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Charlie");
}

#[tokio::test]
async fn query_top_by_age_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(&mut db, TEST_QUERIES, "top_by_age", &ParamMap::new())
        .await
        .unwrap();

    // Top 2 by age desc: Charlie (35), Alice (30)
    assert_eq!(result.num_rows(), 2);
    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Charlie");
    assert_eq!(names.value(1), "Alice");

    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 35);
    assert_eq!(ages.value(1), 30);
}

// ─── Graph traversal ─────────────────────────────────────────────────────

#[tokio::test]
async fn query_friends_of() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    // Alice knows Bob and Charlie
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut friend_names: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    friend_names.sort();
    assert_eq!(friend_names, vec!["Bob", "Charlie"]);
}

#[tokio::test]
async fn query_employees_of() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "employees_of",
        &params(&[("$company", "Acme")]),
    )
    .await
    .unwrap();

    // Alice works at Acme (reverse traversal)
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names.value(0), "Alice");
}

#[tokio::test]
async fn query_friends_of_friends() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of_friends",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    // Alice→Bob→Diana (Alice→Charlie→nobody)
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut fof_names: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    fof_names.sort();
    assert_eq!(fof_names, vec!["Diana"]);
}

#[tokio::test]
async fn query_unemployed() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = query_main(&mut db, TEST_QUERIES, "unemployed", &ParamMap::new())
        .await
        .unwrap();

    // Charlie and Diana have no WorksAt edges
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut unemployed: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    unemployed.sort();
    assert_eq!(unemployed, vec!["Charlie", "Diana"]);
}

#[tokio::test]
async fn query_anti_join_all_have_edges() {
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company
"#;
    let data = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Person", "data": {"name": "Bob"}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
{"edge": "WorksAt", "from": "Bob", "to": "Acme"}
"#;
    let queries = r#"
query unemployed() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let result = query_main(&mut db, queries, "unemployed", &ParamMap::new())
        .await
        .unwrap();

    // Everyone has a WorksAt edge → empty result
    assert_eq!(result.num_rows(), 0);
}

// ─── Mutations ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn mutation_insert_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    // Query it back
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let batch = &qr.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Eve");
}

#[tokio::test]
async fn mutation_insert_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Insert Eve
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Add edge Eve → Alice
    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Eve"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 1);

    // Verify traversal
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let batch = qr.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
}

#[tokio::test]
async fn mutation_multi_insert_node_and_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // In one atomic mutation: insert Eve + edge Eve→Alice
    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(&[("$name", "Eve"), ("$friend", "Alice")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 1);

    // Verify traversal: Eve → Alice
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let batch = qr.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
}

#[tokio::test]
async fn mutation_update_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    // Verify the update
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let batch = &qr.batches()[0];
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 31);
}

#[tokio::test]
async fn mutation_delete_node_cascades_edges() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Alice has: 2 outgoing Knows (Alice→Bob, Alice→Charlie) + 1 WorksAt (Alice→Acme) = 3 edges
    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert!(
        result.affected_edges >= 3,
        "expected at least 3 cascaded edges, got {}",
        result.affected_edges
    );

    // Alice should be gone
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 0);

    // Verify no edges reference Alice
    let snap = snapshot_main(&db).await.unwrap();
    for edge_key in &["edge:Knows", "edge:WorksAt"] {
        let ds = snap.open(edge_key).await.unwrap();
        let batches: Vec<arrow_array::RecordBatch> = ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
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
            for i in 0..batch.num_rows() {
                assert_ne!(
                    srcs.value(i),
                    "Alice",
                    "found edge src=Alice in {}",
                    edge_key
                );
                assert_ne!(
                    dsts.value(i),
                    "Alice",
                    "found edge dst=Alice in {}",
                    edge_key
                );
            }
        }
    }
}

#[tokio::test]
async fn mutation_delete_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Delete all Knows edges from Alice (Alice→Bob, Alice→Charlie)
    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_friendship",
        &params(&[("$from", "Alice")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 2);

    // Alice should still exist
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    // But has no friends
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 0);
}

#[tokio::test]
async fn mutation_insert_duplicate_key_upserts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Alice already exists with age=30. Insert again with age=99.
    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);

    // Should still be exactly 1 Alice (upsert, not duplicate)
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    // Age should be updated to 99
    let batch = &qr.batches()[0];
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 99);
}

#[tokio::test]
async fn mutation_update_key_property_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let queries = r#"
query rename_person($old_name: String, $new_name: String) {
    update Person set { name: $new_name } where name = $old_name
}
"#;

    let result = mutate_main(
        &mut db,
        queries,
        "rename_person",
        &params(&[("$old_name", "Alice"), ("$new_name", "Bob")]),
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@key"), "error should mention @key: {}", err);
}

// ─── Blob support ────────────────────────────────────────────────────────────

const BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;

const BLOB_QUERIES: &str = r#"
query all_docs() {
    match { $d: Document }
    return { $d.title, $d.content }
}

query get_doc($title: String) {
    match { $d: Document { title: $title } }
    return { $d.title, $d.content }
}
"#;

const BLOB_MUTATIONS: &str = r#"
query insert_doc($title: String, $content: Blob) {
    insert Document { title: $title, content: $content }
}

query update_doc_content($title: String, $content: Blob) {
    update Document set { content: $content } where title = $title
}
"#;

#[tokio::test]
async fn blob_schema_parses_and_init_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    assert!(
        db.catalog().node_types["Document"]
            .blob_properties
            .contains("content")
    );
    assert_eq!(db.catalog().node_types["Document"].properties.len(), 2);
}

#[tokio::test]
async fn blob_load_base64_inline() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // "Hello World" = "SGVsbG8gV29ybGQ="
    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}
{"type": "Document", "data": {"title": "empty"}}
"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Document").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 2);
}

#[tokio::test]
async fn blob_query_returns_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let result = query_main(
        &mut db,
        BLOB_QUERIES,
        "get_doc",
        &params(&[("$title", "readme")]),
    )
    .await
    .unwrap();

    assert_eq!(result.num_rows(), 1);

    let json = result.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "readme");
    // Blob columns return null in query projections — data is accessed via take_blobs API.
    // (Lance bug: BlobsDescriptions + filter triggers assertion, so blobs are excluded from scan)
    assert!(
        row["d.content"].is_null(),
        "blob column should return null in query projection"
    );
}

#[tokio::test]
async fn blob_null_returns_null_in_query() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "empty"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let result = query_main(
        &mut db,
        BLOB_QUERIES,
        "get_doc",
        &params(&[("$title", "empty")]),
    )
    .await
    .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json = result.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "empty");
    // Nullable blob with no value should return null
    assert!(
        row["d.content"].is_null(),
        "null blob should return null, got: {}",
        row["d.content"]
    );
}

#[tokio::test]
async fn blob_insert_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let result = mutate_main(
        &mut db,
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[("$title", "new-doc"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);

    // Query it back
    let qr = query_main(
        &mut db,
        BLOB_QUERIES,
        "get_doc",
        &params(&[("$title", "new-doc")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let json = qr.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "new-doc");
    // Blob column present but null in query projection (data accessed via take_blobs)
    assert!(
        row.get("d.content").is_some(),
        "content column should be present"
    );
}

#[tokio::test]
async fn blob_update_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // First insert a doc with blob
    mutate_main(
        &mut db,
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[("$title", "updatable"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();

    // Update the blob
    let result = mutate_main(
        &mut db,
        BLOB_MUTATIONS,
        "update_doc_content",
        &params(&[("$title", "updatable"), ("$content", "base64:BAUG")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 1);

    let bytes = read_blob_bytes(&db, "Document", "updatable", "content").await;
    assert_eq!(&bytes[..], &[4, 5, 6]);
}

// ─── Blob read API ───────────────────────────────────────────────────────

#[tokio::test]
async fn blob_read_returns_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // "Hello World" = base64 "SGVsbG8gV29ybGQ="
    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let blob = db
        .read_blob_at("main", "Document", "readme", "content")
        .await
        .unwrap();
    assert_eq!(blob.size, Some(11)); // "Hello World" = 11 bytes
    let omnigraph::db::BlobContent::Streamed(reader) = blob.content else {
        panic!("expected internal blob");
    };
    assert_eq!(reader.size(), 11);

    // Bounded range read (the streaming handler's shape) plus full read.
    let head = reader.read_range(0..5).await.unwrap();
    assert_eq!(&head[..], b"Hello");
    let bytes = reader.read_all().await.unwrap();
    assert_eq!(&bytes[..], b"Hello World");

    // Out-of-bounds range is a typed bad-request validated before storage.
    let oob = reader.read_range(4..12).await;
    assert!(oob.is_err(), "range past end must fail");
}

#[tokio::test]
async fn blob_read_not_found_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Non-existent ID
    let err = db
        .read_blob_at("main", "Document", "nonexistent", "content")
        .await;
    assert!(err.is_err());

    // Non-blob property
    let err = db.read_blob_at("main", "Document", "readme", "title").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn blob_read_after_mutation_insert() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    let stale_reader = Omnigraph::open(uri).await.unwrap();

    // Insert via mutation (base64 for bytes [1, 2, 3])
    mutate_main(
        &mut db,
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[("$title", "inserted"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();

    // The reader was opened before the other handle's commit. The blob read
    // must freshness-probe the requested branch instead of using its held
    // coordinator snapshot.
    let bytes = read_blob_bytes(&stale_reader, "Document", "inserted", "content").await;
    assert_eq!(&bytes[..], &[1, 2, 3]);
}

// ─── Blob low-level: probe BlobHandling::BlobsDescriptions ───────────────

#[tokio::test]
async fn blob_scan_with_descriptions_on_nonempty_dataset() {
    use lance::datatypes::BlobHandling;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Open the dataset directly and try BlobsDescriptions
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Document").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 1);

    // BlobsDescriptions works without filter
    let mut scanner = ds.scan();
    scanner.blob_handling(BlobHandling::BlobsDescriptions);
    let stream = scanner.try_into_stream().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    // Blob descriptor is a struct with kind, position, size, blob_id, blob_uri
    let content_col = batches[0].column_by_name("content").unwrap();
    assert!(
        matches!(content_col.data_type(), arrow_schema::DataType::Struct(_)),
        "blob column should be Struct, got {:?}",
        content_col.data_type()
    );

    // BlobsDescriptions combined with a filter WORKS on the current Lance
    // pin (9.0.0-rc.1). The query executor still excludes blob columns from
    // filtered scans and re-adds them as nulls — that response shape is
    // shipped contract, so returning descriptors from /query is a deliberate
    // future contract change, not a workaround removal. A red here means the
    // upstream projection assertion regressed and the exec/query.rs exclusion
    // is load-bearing again.
    let mut filtered = ds.scan();
    filtered.blob_handling(BlobHandling::BlobsDescriptions);
    filtered.filter("title = 'readme'").unwrap();
    let filtered_rows: usize = filtered
        .try_into_stream()
        .await
        .expect("filtered BlobsDescriptions scan must plan on rc.1")
        .try_collect::<Vec<RecordBatch>>()
        .await
        .expect("filtered BlobsDescriptions scan must execute on rc.1")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(filtered_rows, 1);
}

// ─── Constraint enforcement ──────────────────────────────────────────────────

#[tokio::test]
async fn range_constraint_rejects_out_of_bounds() {
    let schema = r#"
node Person {
    name: String @key
    age: I32?
    @range(age, 0..200)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // age = 300 exceeds max of 200
    let data = r#"{"type": "Person", "data": {"name": "Old", "age": 300}}"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "expected range violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@range violation"), "error: {}", err);
}

#[tokio::test]
async fn range_constraint_allows_within_bounds() {
    let schema = r#"
node Person {
    name: String @key
    age: I32?
    @range(age, 0..200)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 1);
}

#[tokio::test]
async fn range_constraint_float_rejects_out_of_bounds() {
    let schema = r#"
node Measurement {
    name: String @key
    temperature: F64?
    @range(temperature, 0.0..100.0)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Measurement", "data": {"name": "hot", "temperature": 150.5}}"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "expected range violation for float");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@range violation"), "error: {}", err);
}

#[tokio::test]
async fn range_constraint_float_allows_within_bounds() {
    let schema = r#"
node Measurement {
    name: String @key
    temperature: F64?
    @range(temperature, 0.0..100.0)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Measurement", "data": {"name": "warm", "temperature": 37.5}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Measurement").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 1);
}

#[tokio::test]
async fn range_constraint_negative_float_bounds() {
    let schema = r#"
node Measurement {
    name: String @key
    temperature: F64?
    @range(temperature, -40.0..60.0)
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Within bounds — should succeed
    let data = r#"{"type": "Measurement", "data": {"name": "cold", "temperature": -20.0}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Below minimum — should fail
    let data = r#"{"type": "Measurement", "data": {"name": "arctic", "temperature": -50.0}}"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "expected range violation for -50.0");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@range violation"), "error: {}", err);
}

#[tokio::test]
async fn check_constraint_rejects_bad_pattern() {
    let schema = r#"
node Order {
    code: String @key
    @check(code, "^[A-Z]{3}-[0-9]+$")
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Order", "data": {"code": "invalid"}}"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "expected check violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@check violation"), "error: {}", err);
}

#[tokio::test]
async fn check_constraint_allows_matching_pattern() {
    let schema = r#"
node Order {
    code: String @key
    @check(code, "^[A-Z]{3}-[0-9]+$")
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Order", "data": {"code": "ABC-123"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Order").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 1);
}

#[tokio::test]
async fn mutation_insert_rejects_range_violation() {
    let schema = r#"
node Person {
    name: String @key
    age: I32?
    @range(age, 0..200)
}
"#;
    let queries = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let result = mutate_main(&mut db, queries, "insert_person", &{
        let mut p = omnigraph_compiler::ir::ParamMap::new();
        p.insert(
            "name".to_string(),
            omnigraph_compiler::query::ast::Literal::String("Old".to_string()),
        );
        p.insert(
            "age".to_string(),
            omnigraph_compiler::query::ast::Literal::Integer(300),
        );
        p
    })
    .await;
    assert!(result.is_err(), "expected range violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@range violation"), "error: {}", err);
}

#[tokio::test]
async fn mutation_update_rejects_range_violation() {
    let schema = r#"
node Person {
    name: String @key
    age: I32?
    @range(age, 0..200)
}
"#;
    let queries = r#"
query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let result = mutate_main(&mut db, queries, "set_age", &{
        let mut p = omnigraph_compiler::ir::ParamMap::new();
        p.insert(
            "name".to_string(),
            omnigraph_compiler::query::ast::Literal::String("Alice".to_string()),
        );
        p.insert(
            "age".to_string(),
            omnigraph_compiler::query::ast::Literal::Integer(300),
        );
        p
    })
    .await;
    assert!(result.is_err(), "expected range violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@range violation"), "error: {}", err);
}

#[tokio::test]
async fn mutation_insert_rejects_check_violation() {
    let schema = r#"
node Order {
    code: String @key
    @check(code, "^[A-Z]{3}-[0-9]+$")
}
"#;
    let queries = r#"
query insert_order($code: String) {
    insert Order { code: $code }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let result = mutate_main(&mut db, queries, "insert_order", &{
        let mut p = omnigraph_compiler::ir::ParamMap::new();
        p.insert(
            "code".to_string(),
            omnigraph_compiler::query::ast::Literal::String("invalid".to_string()),
        );
        p
    })
    .await;
    assert!(result.is_err(), "expected check violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@check violation"), "error: {}", err);
}

#[tokio::test]
async fn mutation_update_rejects_check_violation() {
    let schema = r#"
node Order {
    code: String @key
    label: String?
    @check(label, "^[A-Z]+$")
}
"#;
    let queries = r#"
query set_label($code: String, $label: String) {
    update Order set { label: $label } where code = $code
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type": "Order", "data": {"code": "ABC-123", "label": "VALID"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let result = mutate_main(&mut db, queries, "set_label", &{
        let mut p = omnigraph_compiler::ir::ParamMap::new();
        p.insert(
            "code".to_string(),
            omnigraph_compiler::query::ast::Literal::String("ABC-123".to_string()),
        );
        p.insert(
            "label".to_string(),
            omnigraph_compiler::query::ast::Literal::String("invalid".to_string()),
        );
        p
    })
    .await;
    assert!(result.is_err(), "expected check violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@check violation"), "error: {}", err);
}

#[tokio::test]
async fn edge_cardinality_max_enforced() {
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company @card(0..1)
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Alice works at two companies — violates @card(0..1)
    let data = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Company", "data": {"name": "Acme"}}
{"type": "Company", "data": {"name": "Globex"}}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
{"edge": "WorksAt", "from": "Alice", "to": "Globex"}
"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "expected cardinality violation");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("@card violation"), "error: {}", err);
}

#[tokio::test]
async fn edge_cardinality_allows_within_bounds() {
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company @card(0..1)
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let data = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("edge:WorksAt").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 1);
}

// ─── Regression: apply_assignments with blob mid-schema ──────────────────────

#[tokio::test]
async fn update_with_blob_mid_schema_does_not_panic() {
    // Blob column in the MIDDLE of schema — not last. This previously caused
    // a column-index mismatch in apply_assignments (batch.column(idx) used
    // schema position but the batch had blob columns excluded from projection).
    let schema = r#"
node Article {
    slug: String @key
    attachment: Blob?
    summary: String?
    rating: I32?
}
"#;
    let mutations = r#"
query insert_article($slug: String, $summary: String, $rating: I32) {
    insert Article { slug: $slug, summary: $summary, rating: $rating }
}
query update_summary($slug: String, $summary: String) {
    update Article set { summary: $summary } where slug = $slug
}
query get_article($slug: String) {
    match { $a: Article { slug: $slug } }
    return { $a.slug, $a.summary, $a.rating }
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    mutate_main(
        &mut db,
        mutations,
        "insert_article",
        &mixed_params(
            &[("$slug", "a1"), ("$summary", "hello")],
            &[("$rating", 42)],
        ),
    )
    .await
    .unwrap();

    // This would panic with the old batch.column(idx) code
    let result = mutate_main(
        &mut db,
        mutations,
        "update_summary",
        &params(&[("$slug", "a1"), ("$summary", "updated")]),
    )
    .await
    .unwrap();
    assert_eq!(result.affected_nodes, 1);

    // Verify the update applied correctly
    let qr = query_main(
        &mut db,
        mutations,
        "get_article",
        &params(&[("$slug", "a1")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

// ─── Regression: blob update null → non-null ─────────────────────────────────

#[tokio::test]
async fn blob_update_null_to_non_null() {
    // Regression: updating a blob column that was previously all-null panicked
    // with assertion `left: 0, right: 1` in lance-table stream.rs because the
    // two-phase blob update sent a blob-only batch to merge_insert on a dataset
    // with zero blob fragments.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // Load a row with blob = null (no blob data in dataset)
    let data = r#"{"type": "Document", "data": {"title": "kid-a"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Update: null → non-null blob. Previously panicked with assertion
    // `left: 0, right: 1` in lance-table stream.rs.
    let result = mutate_main(
        &mut db,
        BLOB_MUTATIONS,
        "update_doc_content",
        &params(&[("$title", "kid-a"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();
    assert_eq!(result.affected_nodes, 1);

    let bytes = read_blob_bytes(&db, "Document", "kid-a", "content").await;
    assert_eq!(&bytes[..], &[1, 2, 3]);
}

#[tokio::test]
async fn read_blob_errors_are_typed_and_quoted_ids_need_no_escaping() {
    use omnigraph::error::{ManifestError, ManifestErrorKind, OmniError};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // A stored key containing a single quote must be findable through the
    // typed exact-id predicate (no SQL-text flattening of the caller's id).
    let data = r#"{"type": "Document", "data": {"title": "O'Brien", "content": "base64:AQID"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();
    let bytes = read_blob_bytes(&db, "Document", "O'Brien", "content").await;
    assert_eq!(&bytes[..], &[1u8, 2, 3]);

    // Missing row → typed NotFound, not a generic bad-request.
    let missing = db.read_blob_at("main", "Document", "absent", "content").await;
    assert!(
        matches!(
            missing,
            Err(OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::NotFound,
                ..
            }))
        ),
        "missing row should be NotFound, got {:?}",
        missing.err()
    );

    // Unknown type → typed NotFound.
    let unknown_type = db.read_blob_at("main", "Nope", "x", "content").await;
    assert!(
        matches!(
            unknown_type,
            Err(OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::NotFound,
                ..
            }))
        ),
        "unknown type should be NotFound, got {:?}",
        unknown_type.err()
    );

    // Non-Blob property → typed BadRequest.
    let not_blob = db.read_blob_at("main", "Document", "O'Brien", "title").await;
    assert!(
        matches!(
            not_blob,
            Err(OmniError::Manifest(ManifestError {
                kind: ManifestErrorKind::BadRequest,
                ..
            }))
        ),
        "non-blob property should be BadRequest, got {:?}",
        not_blob.err()
    );
}

// ─── Regression: blob load with external file URI ────────────────────────────

#[tokio::test]
async fn blob_load_external_file_uri() {
    // Regression: loading blobs with external file:// URIs was rejected with
    // "External blob URI '...' is outside registered external bases" because
    // allow_external_blob_outside_bases was not set on data table write paths.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Create a temp file to reference
    let blob_dir = tempfile::tempdir().unwrap();
    let blob_path = blob_dir.path().join("test.txt");
    std::fs::write(&blob_path, b"Hello from file").unwrap();
    let file_uri = format!("file://{}", blob_path.display());

    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    let data = format!(
        r#"{{"type": "Document", "data": {{"title": "from-file", "content": "{}"}}}}"#,
        file_uri
    );

    // Load with external URI
    load_jsonl(&mut db, &data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Descriptor-first classification: the external reference comes back
    // WITHOUT the engine resolving the external store or touching the file.
    let blob = db
        .read_blob_at("main", "Document", "from-file", "content")
        .await
        .unwrap();
    match blob.content {
        omnigraph::db::BlobContent::ExternalRef { uri } => {
            assert!(
                uri.starts_with("file://"),
                "external blob should keep its absolute URI, got {uri}"
            );
        }
        omnigraph::db::BlobContent::Streamed(_) => {
            panic!("external URI blob must classify as ExternalRef")
        }
    }
    // The descriptor deliberately records no size for an external reference:
    // injecting position/size children into the logical blob input changes
    // the batch schema, which create/overwrite persist as the table's blob
    // shape — and a divergent shape breaks every later keyed write (see
    // keyed_writes_survive_committed_external_blob_reference). The facade
    // maps Lance's BlobRange{0,0} "size unknown" encoding to None.
    assert_eq!(
        blob.size, None,
        "external blob size is unknown by design (no schema-perturbing annotation)"
    );

    // The classification proof: delete the referenced file, and the
    // descriptor read STILL succeeds — nothing server-side resolves the
    // external store on the read path.
    std::fs::remove_file(&blob_path).unwrap();
    let after_delete = db
        .read_blob_at("main", "Document", "from-file", "content")
        .await
        .unwrap();
    assert!(matches!(
        after_delete.content,
        omnigraph::db::BlobContent::ExternalRef { .. }
    ));

    // An UNREADABLE external URI now fails at load time (fail loudly at
    // ingest), instead of deferring the failure to the first read.
    let missing_uri = format!("file://{}", blob_dir.path().join("absent.bin").display());
    let bad = format!(
        r#"{{"type": "Document", "data": {{"title": "bad-ref", "content": "{}"}}}}"#,
        missing_uri
    );
    let err = load_jsonl(&mut db, &bad, LoadMode::Overwrite).await;
    assert!(
        err.is_err(),
        "loading an unreadable external blob URI must fail at ingest"
    );
}

#[tokio::test]
async fn keyed_writes_survive_committed_external_blob_reference() {
    // Regression: a reference-retaining write (create/overwrite) with an
    // external-URI blob cell must not perturb the table's persisted logical
    // blob schema. When ingest injected position/size children into the
    // input struct, the persisted schema diverged from the canonical
    // {data, uri} shape and every later keyed write fell off Lance's v2
    // merge fast path: strict insert (Append load) failed with "did not
    // produce an inserted-row key filter" and upsert (Merge load) failed
    // inside Lance's legacy merger with a blob structural decode error
    // ("more fields in the schema than provided column indices").
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let blob_path = blob_dir.path().join("external.bin");
    std::fs::write(&blob_path, b"external-bytes").unwrap();
    let file_uri = format!("file://{}", blob_path.display());

    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    let seed = format!(
        "{}\n{}",
        r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#,
        format!(r#"{{"type": "Document", "data": {{"title": "ext", "content": "{file_uri}"}}}}"#),
    );
    load_jsonl(&mut db, &seed, LoadMode::Overwrite).await.unwrap();

    // Strict insert onto the table holding a committed external row.
    load_jsonl(
        &mut db,
        r#"{"type": "Document", "data": {"title": "second", "content": "base64:c2Vjb25k"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("strict insert after a committed external blob reference");

    // Upsert rewriting the inline row on the same table.
    load_jsonl(
        &mut db,
        r#"{"type": "Document", "data": {"title": "readme", "content": "base64:R29vZGJ5ZQ=="}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("upsert after a committed external blob reference");

    assert_eq!(
        read_blob_bytes(&db, "Document", "readme", "content").await,
        b"Goodbye",
        "merge must rewrite the inline cell"
    );
    let ext = db
        .read_blob_at("main", "Document", "ext", "content")
        .await
        .unwrap();
    match ext.content {
        omnigraph::db::BlobContent::ExternalRef { uri: stored } => {
            assert_eq!(stored, file_uri, "external reference must survive keyed writes");
        }
        _ => panic!("external row must still classify as ExternalRef"),
    }
}

// ─── Regression: execute_update on edge type ─────────────────────────────────

#[tokio::test]
async fn update_edge_type_returns_error_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // The typechecker should reject this, but even if bypassed,
    // execute_update must not panic with HashMap key-not-found.
    let mutations = r#"
query update_edge($from: String) {
    update Knows set { since: "2025-01-01" } where from = $from
}
"#;
    let result = mutate_main(
        &mut db,
        mutations,
        "update_edge",
        &params(&[("$from", "Alice")]),
    )
    .await;
    assert!(result.is_err(), "should return error, not panic");
}

// ─── Regression: Date/DateTime SQL literal escaping ──────────────────────────

#[tokio::test]
async fn date_literal_with_quote_is_escaped() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // A date-like value with a single-quote must not cause SQL injection.
    // This tests that literal_to_sql escapes Date/DateTime values.
    let queries = r#"
query filter_date($d: String) {
    match { $p: Person { name: $d } }
    return { $p.name }
}
"#;
    // Pass a value with a single-quote — should not error or return all rows
    let result = query_main(
        &mut db,
        queries,
        "filter_date",
        &params(&[("$d", "2025-01-01' OR '1'='1")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 0);
}

// ─── Regression: manifest row_count tracks total, not batch size ─────────────

#[tokio::test]
async fn append_mode_manifest_row_count_is_total() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await; // Overwrite: 4 persons

    let extra = r#"{"type": "Person", "data": {"name": "Eve", "age": 22}}"#;
    load_jsonl(&mut db, extra, LoadMode::Append).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let entry = snap.entry("node:Person").unwrap();
    // Must be total rows (4 + 1 = 5), not just the appended batch size (1)
    assert_eq!(entry.row_count, 5);

    // Verify actual dataset count matches manifest
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap() as u64, entry.row_count);
}

// ─── Regression: cardinality violation must not commit manifest ───────────────

#[tokio::test]
async fn cardinality_violation_does_not_commit_manifest() {
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company @card(0..1)
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Alice works at two companies — violates @card(0..1) (at most 1)
    let data = r#"
{"type": "Person", "data": {"name": "Alice"}}
{"type": "Company", "data": {"name": "Acme"}}
{"type": "Company", "data": {"name": "Beta"}}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
{"edge": "WorksAt", "from": "Alice", "to": "Beta"}
"#;

    let v_before = version_main(&db).await.unwrap();
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "cardinality violation should be rejected");
    assert!(
        result.unwrap_err().to_string().contains("@card violation"),
        "error should mention @card"
    );

    // Manifest must NOT have advanced — invalid data was not committed
    assert_eq!(version_main(&db).await.unwrap(), v_before);
}

// ─── Regression: dangling edge references are rejected ───────────────────────

#[tokio::test]
async fn dangling_edge_dst_rejected_on_load() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let data = r#"
{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "NonExistent"}
"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "dangling edge dst should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "error should mention 'not found': {}",
        err
    );
}

#[tokio::test]
async fn dangling_edge_src_rejected_on_load() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let data = r#"
{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "WorksAt", "from": "Ghost", "to": "Acme"}
"#;
    let result = load_jsonl(&mut db, data, LoadMode::Overwrite).await;
    assert!(result.is_err(), "dangling edge src should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "error should mention 'not found': {}",
        err
    );
}

// ─── Regression: ensure_indices is idempotent ────────────────────────────────

#[tokio::test]
async fn ensure_indices_does_not_error_on_repeated_call() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let version_after_load = version_main(&db).await.unwrap();

    // load commits now enforce required indices; repeated ensure_indices calls
    // should be a no-op at the manifest level.
    db.ensure_indices().await.unwrap();
    let version_after_first = version_main(&db).await.unwrap();
    db.ensure_indices().await.unwrap();
    let version_after_second = version_main(&db).await.unwrap();

    assert_eq!(version_after_first, version_after_load);
    assert_eq!(version_after_second, version_after_load);

    // Data should still be queryable after index operations
    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 4);
}

// ─── DataFusion-Expr filter pushdown (Tier-1 follow-up to the Lance v6 bump) ──

/// Regression for `CompOp::Contains` pushdown via `array_has` in
/// `ir_filter_to_expr`. Before the Expr-pushdown refactor, the
/// `ir_filter_to_sql` family returned `None` for list-contains (the
/// comment said *"Can't pushdown list contains"*) and the predicate was
/// applied post-scan in memory. With `Scanner::filter_expr(Expr)` and
/// DF's `array_has` builtin, the contains predicate now pushes down to
/// Lance — the test confirms results are correct AND the pushdown path
/// is exercised (a regression on the pushdown would land all rows in
/// the scan, then be filtered post-hoc; that still produces the right
/// count so this test pins correctness, while `lance_surface_guards.rs`
/// is the structural pin for the surface itself).
#[tokio::test]
async fn ir_filter_with_list_contains_pushes_down() {
    let schema = r#"
node Doc {
    slug: String @key
    tags: [String]
}
"#;
    let data = r#"{"type":"Doc","data":{"slug":"alpha","tags":["red","blue"]}}
{"type":"Doc","data":{"slug":"bravo","tags":["green"]}}
{"type":"Doc","data":{"slug":"charlie","tags":["red","green"]}}
{"type":"Doc","data":{"slug":"delta","tags":[]}}"#;

    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
        .await
        .unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let queries = r#"
query docs_with_tag($tag: String) {
    match {
        $d: Doc
        $d.tags contains $tag
    }
    return { $d.slug }
}
"#;
    let result = query_main(
        &mut db,
        queries,
        "docs_with_tag",
        &params(&[("$tag", "red")]),
    )
    .await
    .unwrap();

    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut got: Vec<&str> = (0..slugs.len()).map(|i| slugs.value(i)).collect();
    got.sort();
    assert_eq!(
        got,
        vec!["alpha", "charlie"],
        "contains-pushdown should return exactly the rows whose tags list contains 'red'"
    );
}

// ─── Maintenance in the full lifecycle: optimize (compaction) ────────────────

/// `optimize` (Lance compaction) is part of a realistic graph lifecycle: it
/// advances the Lance HEAD and publishes the compacted version to the manifest.
/// The rest of the flow must keep working across that boundary — reads observe
/// the compacted data, strict updates (which check Lance HEAD == manifest
/// version) still commit, inserts still commit, and the state survives a reopen
/// (the open-time recovery sweep finds no leftover drift). Before optimize
/// published its compaction, the manifest lagged the Lance HEAD here and the
/// post-optimize update below failed with "stale view ... refresh and retry".
#[tokio::test]
async fn full_flow_optimize_then_query_update_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = init_and_load(&dir).await;

    // Build several Person fragments so compaction has something to merge.
    for (name, age) in [("Eve", 40), ("Frank", 41), ("Grace", 42)] {
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", age)]),
        )
        .await
        .unwrap();
    }

    let stats = db.optimize().await.unwrap();
    assert!(
        stats.iter().any(|s| s.committed),
        "a multi-fragment table should have compacted in this flow"
    );

    // Reads observe the compacted data.
    let qr = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    // Strict update after optimize commits (previously failed with "stale view"
    // because the manifest lagged the compacted Lance HEAD).
    let upd = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    assert_eq!(upd.affected_nodes, 1);

    // Insert after optimize also commits.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Ivan")], &[("$age", 50)]),
    )
    .await
    .unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, 8); // 4 seed + Eve/Frank/Grace + Ivan

    // State survives a reopen — the recovery sweep runs and finds no drift.
    drop(db);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(count_rows(&reopened, "node:Person").await, 8);
    let alice = reopened
        .entity_at_target(ReadTarget::branch("main"), "node:Person", "Alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        alice["age"],
        serde_json::json!(31),
        "Alice's post-optimize age update must persist across reopen"
    );
}
