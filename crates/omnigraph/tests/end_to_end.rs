use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl, load_jsonl_file};
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::ir::ParamMap;

const TEST_SCHEMA: &str = include_str!("fixtures/test.pg");
const TEST_DATA: &str = include_str!("fixtures/test.jsonl");
const TEST_QUERIES: &str = include_str!("fixtures/test.gq");

/// Helper: init a repo and load the test data.
async fn init_and_load(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

/// Helper: read all rows from a sub-table by table_key.
async fn read_table(db: &Omnigraph, table_key: &str) -> Vec<RecordBatch> {
    let snap = db.snapshot();
    let ds = snap.open(table_key).await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

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
    let batch = &batches[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut id_values: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec!["Alice", "Bob", "Charlie", "Diana"]);
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
    let ids = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let id_values: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
    // Should contain slug values like "aws", "openai", etc.
    assert!(id_values.contains(&"aws"));
    assert!(id_values.contains(&"openai"));
}

// ─── Query execution ────────────────────────────────────────────────────────

fn params(pairs: &[(&str, &str)]) -> ParamMap {
    pairs
        .iter()
        .map(|(k, v)| {
            // Strip leading $ if present — the IR stores param names without $
            let key = k.strip_prefix('$').unwrap_or(k);
            (key.to_string(), Literal::String(v.to_string()))
        })
        .collect()
}

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
