mod helpers;

use helpers::*;
use omnigraph_compiler::ir::ParamMap;

// ─── Inline queries for point-in-time tests ─────────────────────────────────

const ALL_PERSONS_QUERY: &str = r#"
query all_persons() {
    match {
        $p: Person
    }
    return { $p.name, $p.age }
    order { $p.name asc }
}
"#;

const FRIENDS_QUERY: &str = r#"
query friends_of($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name }
    order { $f.name asc }
}
"#;

// ─── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn run_query_at_returns_historical_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = version_main(&db).await.unwrap();

    // Insert Eve after recording the version
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Historical query at v_before should NOT see Eve
    let historical = db
        .run_query_at(v_before, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();

    // Current query should see Eve
    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();

    assert_eq!(historical.num_rows(), 4, "historical should have 4 persons");
    assert_eq!(current.num_rows(), 5, "current should have 5 persons");

    // Verify Eve is absent from historical results
    let historical_names = collect_column_strings(historical.batches(), "p.name");
    assert!(
        !historical_names.contains(&"Eve".to_string()),
        "Eve should not appear in historical query"
    );

    // Verify Eve is present in current results
    let current_names = collect_column_strings(current.batches(), "p.name");
    assert!(
        current_names.contains(&"Eve".to_string()),
        "Eve should appear in current query"
    );
}

#[tokio::test]
async fn run_query_at_traversal_uses_historical_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Fixture: Alice knows Bob and Charlie
    let v_before = version_main(&db).await.unwrap();

    // Insert Eve and a Knows edge: Eve -> Alice
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Eve"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    // Historical traversal: friends_of Alice at v_before
    // Should see Bob and Charlie (the original edges)
    let historical = db
        .run_query_at(
            v_before,
            FRIENDS_QUERY,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();

    // Current traversal: friends_of Eve should find Alice
    let current = query_main(
        &mut db,
        FRIENDS_QUERY,
        "friends_of",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();

    // Historical: Alice's friends at v_before = Bob, Charlie
    assert_eq!(historical.num_rows(), 2);
    let hist_names = collect_column_strings(historical.batches(), "f.name");
    assert!(hist_names.contains(&"Bob".to_string()));
    assert!(hist_names.contains(&"Charlie".to_string()));

    // Current: Eve's friends = Alice
    assert_eq!(current.num_rows(), 1);
    let cur_names = collect_column_strings(current.batches(), "f.name");
    assert!(cur_names.contains(&"Alice".to_string()));
}

#[tokio::test]
async fn snapshot_at_version_fails_for_nonexistent_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let result = db.snapshot_at_version(99999).await;
    assert!(result.is_err(), "non-existent version should return error");
}

#[tokio::test]
async fn run_query_at_multiple_versions_sees_correct_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // v1: initial state (4 persons)
    let v1 = version_main(&db).await.unwrap();

    // Mutation 1: update Alice's age to 99
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();
    let v2 = version_main(&db).await.unwrap();

    // Mutation 2: insert Frank
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    // Query at v1: Alice has original age, no Frank
    let at_v1 = db
        .run_query_at(v1, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(at_v1.num_rows(), 4, "v1 should have 4 persons");
    let v1_names = collect_column_strings(at_v1.batches(), "p.name");
    assert!(!v1_names.contains(&"Frank".to_string()));

    // Query at v2: Alice has age 99, no Frank
    let at_v2 = db
        .run_query_at(v2, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(at_v2.num_rows(), 4, "v2 should have 4 persons");
    let v2_names = collect_column_strings(at_v2.batches(), "p.name");
    assert!(!v2_names.contains(&"Frank".to_string()));

    // Current: Alice has age 99, Frank present
    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 5, "current should have 5 persons");
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(cur_names.contains(&"Frank".to_string()));
}
