mod helpers;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use omnigraph::db::Omnigraph;
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
    assert!(dir.path().join("_manifest.lance").exists());
    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
}

#[tokio::test]
async fn open_restores_full_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let original = init_and_load(&dir).await;
    let v = original.version();
    drop(original);

    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened.catalog().node_types.len(), 2);
    assert_eq!(reopened.catalog().edge_types.len(), 2);
    // Version should be what we left it at
    // (manifest was committed during load)
    assert!(reopened.version() >= v);
}

#[tokio::test]
async fn load_populates_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let snap = db.snapshot();

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
    let mut db = init_and_load(&dir).await;

    let batches = read_table(&db, "node:Person").await;
    let mut ids = collect_column_strings(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec!["Alice", "Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn node_properties_are_correct() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

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
async fn edge_src_dst_reference_node_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

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
    let mut db = init_and_load(&dir).await;

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

    // Overwrite with just one person
    let small = r#"{"type": "Person", "data": {"name": "Zara", "age": 40}}"#;
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
    load_jsonl(&mut db, batch2, LoadMode::Append)
        .await
        .unwrap();

    let snap = db.snapshot();
    let ds = snap.open("node:Person").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 2);
}

// ─── Load from fixture file ─────────────────────────────────────────────────

#[tokio::test]
async fn load_from_file_works() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/test.jsonl"
    );
    load_jsonl_file(&mut db, fixture_path, LoadMode::Overwrite)
        .await
        .unwrap();

    let snap = db.snapshot();
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

    let snap = db.snapshot();

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

    let result = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
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

    let result = db
        .run_query(
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

    let result = db
        .run_query(TEST_QUERIES, "adults", &ParamMap::new())
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

    let result = db
        .run_query(TEST_QUERIES, "top_by_age", &ParamMap::new())
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

    let result = db
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
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

    let result = db
        .run_query(
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

    let result = db
        .run_query(
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

    let result = db
        .run_query(TEST_QUERIES, "unemployed", &ParamMap::new())
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
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let result = db
        .run_query(queries, "unemployed", &ParamMap::new())
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

    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    // Query it back
    let qr = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Eve")]))
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
    db.run_mutation(
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Add edge Eve → Alice
    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Eve"), ("$to", "Alice")]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 1);

    // Verify traversal
    let qr = db
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Eve")]))
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

    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    // Verify the update
    let qr = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
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
    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "remove_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);
    assert!(result.affected_edges >= 3, "expected at least 3 cascaded edges, got {}", result.affected_edges);

    // Alice should be gone
    let qr = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 0);

    // Verify no edges reference Alice
    let snap = db.snapshot();
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
                assert_ne!(srcs.value(i), "Alice", "found edge src=Alice in {}", edge_key);
                assert_ne!(dsts.value(i), "Alice", "found edge dst=Alice in {}", edge_key);
            }
        }
    }
}

#[tokio::test]
async fn mutation_delete_edge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Delete all Knows edges from Alice (Alice→Bob, Alice→Charlie)
    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "remove_friendship",
            &params(&[("$from", "Alice")]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 2);

    // Alice should still exist
    let qr = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);

    // But has no friends
    let qr = db
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 0);
}

#[tokio::test]
async fn mutation_insert_duplicate_key_upserts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Alice already exists with age=30. Insert again with age=99.
    let result = db
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);

    // Should still be exactly 1 Alice (upsert, not duplicate)
    let qr = db
        .run_query(TEST_QUERIES, "get_person", &params(&[("$name", "Alice")]))
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

    let result = db
        .run_mutation(
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

    assert!(db.catalog().node_types["Document"]
        .blob_properties
        .contains("content"));
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
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let snap = db.snapshot();
    let ds = snap.open("node:Document").await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 2);
}

#[tokio::test]
async fn blob_query_returns_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let result = db
        .run_query(BLOB_QUERIES, "get_doc", &params(&[("$title", "readme")]))
        .await
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let json = result.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "readme");
    // Blob columns return null in query projections — data is accessed via take_blobs API.
    // (Lance bug: BlobsDescriptions + filter triggers assertion, so blobs are excluded from scan)
    assert!(row["d.content"].is_null(), "blob column should return null in query projection");
}

#[tokio::test]
async fn blob_null_returns_null_in_query() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "empty"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let result = db
        .run_query(BLOB_QUERIES, "get_doc", &params(&[("$title", "empty")]))
        .await
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json = result.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "empty");
    // Nullable blob with no value should return null
    assert!(row["d.content"].is_null(), "null blob should return null, got: {}", row["d.content"]);
}

#[tokio::test]
async fn blob_insert_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let result = db
        .run_mutation(
            BLOB_MUTATIONS,
            "insert_doc",
            &params(&[("$title", "new-doc"), ("$content", "base64:AQID")]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);

    // Query it back
    let qr = db
        .run_query(BLOB_QUERIES, "get_doc", &params(&[("$title", "new-doc")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let json = qr.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "new-doc");
    // Blob column present but null in query projection (data accessed via take_blobs)
    assert!(row.get("d.content").is_some(), "content column should be present");
}

#[tokio::test]
async fn blob_update_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // First insert a doc with blob
    db.run_mutation(
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[("$title", "updatable"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();

    // Update the blob
    let result = db
        .run_mutation(
            BLOB_MUTATIONS,
            "update_doc_content",
            &params(&[("$title", "updatable"), ("$content", "base64:BAUG")]),
        )
        .await
        .unwrap();

    assert_eq!(result.affected_nodes, 1);

    // Query it back — blob metadata should still be present
    let qr = db
        .run_query(BLOB_QUERIES, "get_doc", &params(&[("$title", "updatable")]))
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let json = qr.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    // Blob column present (data was actually updated via separate merge_insert)
    assert!(row.get("d.content").is_some(), "content column should be present after update");
}

// ─── Blob read API ───────────────────────────────────────────────────────

#[tokio::test]
async fn blob_read_returns_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // "Hello World" = base64 "SGVsbG8gV29ybGQ="
    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    let blob = db.read_blob("Document", "readme", "content").await.unwrap();
    assert_eq!(blob.size(), 11); // "Hello World" = 11 bytes

    let bytes = blob.read().await.unwrap();
    assert_eq!(&bytes[..], b"Hello World");
}

#[tokio::test]
async fn blob_read_not_found_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    let data = r#"{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8="}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    // Non-existent ID
    let err = db.read_blob("Document", "nonexistent", "content").await;
    assert!(err.is_err());

    // Non-blob property
    let err = db.read_blob("Document", "readme", "title").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn blob_read_after_mutation_insert() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // Insert via mutation (base64 for bytes [1, 2, 3])
    db.run_mutation(
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[("$title", "inserted"), ("$content", "base64:AQID")]),
    )
    .await
    .unwrap();

    let blob = db.read_blob("Document", "inserted", "content").await.unwrap();
    let bytes = blob.read().await.unwrap();
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
    load_jsonl(&mut db, data, LoadMode::Overwrite).await.unwrap();

    // Open the dataset directly and try BlobsDescriptions
    let snap = db.snapshot();
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
}
