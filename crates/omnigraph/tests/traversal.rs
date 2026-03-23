mod helpers;

use arrow_array::{Array, Int32Array, StringArray};

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

use helpers::*;

// ─── Anti-join slow path (predicated negation) ──────────────────────────────

#[tokio::test]
async fn anti_join_predicated_negation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // "People who do NOT work at Acme"
    // Inner pipeline: Expand(worksAt) + Filter(name="Acme") → 2 ops → slow path
    let queries = r#"
query not_at_acme() {
    match {
        $p: Person
        not {
            $p worksAt $c
            $c.name = "Acme"
        }
    }
    return { $p.name }
}
"#;
    // Test data: Alice→Acme, Bob→Globex. Charlie and Diana have no WorksAt.
    // Expected: everyone except Alice = {Bob, Charlie, Diana}
    let result = db
        .run_query(queries, "not_at_acme", &ParamMap::new())
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_vec: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    names_vec.sort();
    assert_eq!(names_vec, vec!["Bob", "Charlie", "Diana"]);
}

// ─── Variable-length hops ───────────────────────────────────────────────────

const CHAIN_SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person
"#;

const CHAIN_DATA: &str = r#"{"type": "Person", "data": {"name": "A"}}
{"type": "Person", "data": {"name": "B"}}
{"type": "Person", "data": {"name": "C"}}
{"type": "Person", "data": {"name": "D"}}
{"edge": "Knows", "from": "A", "to": "B"}
{"edge": "Knows", "from": "B", "to": "C"}
{"edge": "Knows", "from": "C", "to": "D"}
"#;

async fn init_chain(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CHAIN_SCHEMA).await.unwrap();
    load_jsonl(&mut db, CHAIN_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn variable_hops_1_to_3() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_chain(&dir).await;

    let queries = r#"
query reachable($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{1,3} $f
    }
    return { $f.name }
}
"#;
    let result = db
        .run_query(queries, "reachable", &params(&[("$name", "A")]))
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_vec: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    names_vec.sort();
    // A→B (1 hop), A→B→C (2 hops), A→B→C→D (3 hops)
    assert_eq!(names_vec, vec!["B", "C", "D"]);
}

#[tokio::test]
async fn variable_hops_2_to_3() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_chain(&dir).await;

    let queries = r#"
query far_reachable($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{2,3} $f
    }
    return { $f.name }
}
"#;
    let result = db
        .run_query(queries, "far_reachable", &params(&[("$name", "A")]))
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_vec: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    names_vec.sort();
    // Skip 1-hop (B), keep 2-hop (C) and 3-hop (D)
    assert_eq!(names_vec, vec!["C", "D"]);
}

#[tokio::test]
async fn variable_hops_exact_2() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_chain(&dir).await;

    let queries = r#"
query exactly_2($name: String) {
    match {
        $p: Person { name: $name }
        $p knows{2,2} $f
    }
    return { $f.name }
}
"#;
    let result = db
        .run_query(queries, "exactly_2", &params(&[("$name", "A")]))
        .await
        .unwrap();

    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names_vec: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    names_vec.sort();
    // Exactly 2 hops from A: only C (A→B→C)
    assert_eq!(names_vec, vec!["C"]);
}

// ─── Ordering ASC ───────────────────────────────────────────────────────────

#[tokio::test]
async fn ordering_ascending() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let queries = r#"
query by_age_asc() {
    match { $p: Person }
    return { $p.name, $p.age }
    order { $p.age asc }
}
"#;
    let result = db
        .run_query(queries, "by_age_asc", &ParamMap::new())
        .await
        .unwrap();

    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Bob(25), Diana(28), Alice(30), Charlie(35) — ascending by age
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(ages.value(0), 25);
    assert_eq!(ages.value(1), 28);
    assert_eq!(ages.value(2), 30);
    assert_eq!(ages.value(3), 35);

    assert_eq!(names.value(0), "Bob");
    assert_eq!(names.value(3), "Charlie");
}

// ─── Empty graph traversal ──────────────────────────────────────────────────

#[tokio::test]
async fn traversal_no_edges_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load only nodes, no edges
    let data = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Company", "data": {"name": "Acme"}}"#;
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Traversal should return empty, not crash
    let result = db
        .run_query(TEST_QUERIES, "friends_of", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    // Anti-join: everyone is "unemployed" since no WorksAt edges exist
    let result = db
        .run_query(TEST_QUERIES, "unemployed", &ParamMap::new())
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.len(), 2); // Alice and Bob
}

// ─── Error paths ────────────────────────────────────────────────────────────

#[tokio::test]
async fn insert_missing_required_property_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Insert Person with no name — name is @key, so this should fail
    let queries = r#"
query insert_no_name($age: I32) {
    insert Person { age: $age }
}
"#;
    let result = db
        .run_mutation(queries, "insert_no_name", &int_params(&[("$age", 25)]))
        .await;

    assert!(result.is_err(), "insert without @key property should fail");
}
