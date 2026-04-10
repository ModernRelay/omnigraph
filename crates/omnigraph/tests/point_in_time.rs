mod helpers;

use arrow_array::{Array, Int32Array};
use helpers::*;
use omnigraph::db::Omnigraph;
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

const UNEMPLOYED_QUERY: &str = r#"
query unemployed() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
    order { $p.name asc }
}
"#;

const FILTERED_QUERY: &str = r#"
query older_than($min_age: I32) {
    match {
        $p: Person
        $p.age > $min_age
    }
    return { $p.name, $p.age }
    order { $p.name asc }
}
"#;

const GET_PERSON_QUERY: &str = r#"
query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.name, $p.age }
}
"#;

// ─── Morphological matrix ───────────────────────────────────────────────────
//
// Dimensions:
//   Query type:   Tabular | Traversal | Negation (AntiJoin) | Filtered | Aggregation
//   Mutation:     Insert  | Update    | Delete node         | Delete edge
//   Branch:       Main    | Named branch
//   Result shape: Empty→non-empty | Non-empty→empty | Count changes | Value changes
//
// Existing coverage (4 tests):
//   Tabular  × Insert  × Main  (returns_historical_data)
//   Traversal × Insert × Main  (traversal_uses_historical_graph_index)
//   Tabular  × Update  × Main  (multiple_versions_sees_correct_state)
//   Error case                  (snapshot_at_version_fails_for_nonexistent_version)
//
// New coverage (9 tests below):
//   Tabular    × Delete node  × Main   → non-empty becomes smaller
//   Traversal  × Delete edge  × Main   → edge disappears from historical
//   Negation   × Insert edge  × Main   → anti-join result shrinks after insert
//   Negation   × Delete edge  × Main   → anti-join result grows after delete
//   Filtered   × Update       × Main   → entity enters/exits filter after age change
//   Multi-hop  × Insert(n+e)  × Main   → friends-of-friends grows after new path
//   Traversal  × Delete node  × Main   → cascade removes edges from traversal
//   Tabular    × Insert       × Branch → branch isolation for point-in-time
//   Tabular    × Multi-step   × Main   → 4-version chain: insert, update, delete

// ─── Original tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn run_query_at_returns_historical_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = version_main(&db).await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let historical = db
        .run_query_at(v_before, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();

    assert_eq!(historical.num_rows(), 4, "historical should have 4 persons");
    assert_eq!(current.num_rows(), 5, "current should have 5 persons");

    let historical_names = collect_column_strings(historical.batches(), "p.name");
    assert!(!historical_names.contains(&"Eve".to_string()));

    let current_names = collect_column_strings(current.batches(), "p.name");
    assert!(current_names.contains(&"Eve".to_string()));
}

#[tokio::test]
async fn run_query_at_traversal_uses_historical_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = version_main(&db).await.unwrap();

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

    let historical = db
        .run_query_at(
            v_before,
            FRIENDS_QUERY,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    let current = query_main(
        &mut db,
        FRIENDS_QUERY,
        "friends_of",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();

    assert_eq!(historical.num_rows(), 2);
    let hist_names = collect_column_strings(historical.batches(), "f.name");
    assert!(hist_names.contains(&"Bob".to_string()));
    assert!(hist_names.contains(&"Charlie".to_string()));

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
    let v1 = version_main(&db).await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();
    let v2 = version_main(&db).await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    let at_v1 = db
        .run_query_at(v1, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(at_v1.num_rows(), 4, "v1 should have 4 persons");
    let v1_names = collect_column_strings(at_v1.batches(), "p.name");
    assert!(!v1_names.contains(&"Frank".to_string()));

    let at_v2 = db
        .run_query_at(v2, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(at_v2.num_rows(), 4, "v2 should have 4 persons");
    let v2_names = collect_column_strings(at_v2.batches(), "p.name");
    assert!(!v2_names.contains(&"Frank".to_string()));

    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 5, "current should have 5 persons");
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(cur_names.contains(&"Frank".to_string()));
}

// ─── Tabular × Delete node ─────────────────────────────────────────────────

#[tokio::test]
async fn tabular_delete_node_invisible_at_historical_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice, Bob, Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Charlie")]),
    )
    .await
    .unwrap();

    // Historical: Charlie still exists
    let historical = db
        .run_query_at(v_before, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    let hist_names = collect_column_strings(historical.batches(), "p.name");
    assert_eq!(historical.num_rows(), 4);
    assert!(hist_names.contains(&"Charlie".to_string()));

    // Current: Charlie is gone
    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert_eq!(current.num_rows(), 3);
    assert!(!cur_names.contains(&"Charlie".to_string()));
}

// ─── Traversal × Delete edge ───────────────────────────────────────────────

#[tokio::test]
async fn traversal_delete_edge_invisible_at_historical_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice knows Bob, Alice knows Charlie
    let v_before = version_main(&db).await.unwrap();

    // Remove all Knows edges FROM Alice
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_friendship",
        &params(&[("$from", "Alice")]),
    )
    .await
    .unwrap();

    // Historical traversal: Alice's friends at v_before = Bob, Charlie
    let historical = db
        .run_query_at(
            v_before,
            FRIENDS_QUERY,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 2);
    let hist_names = collect_column_strings(historical.batches(), "f.name");
    assert!(hist_names.contains(&"Bob".to_string()));
    assert!(hist_names.contains(&"Charlie".to_string()));

    // Current: Alice has no friends (edges deleted)
    let current = query_main(
        &mut db,
        FRIENDS_QUERY,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(
        current.num_rows(),
        0,
        "Alice should have no friends after edge deletion"
    );
}

// ─── Negation (AntiJoin) × Insert ──────────────────────────────────────────

#[tokio::test]
async fn negation_insert_shrinks_antijoin_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice worksAt Acme, Bob worksAt Globex
    // Unemployed: Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    // Give Charlie a job
    mutate_main(
        &mut db,
        r#"
query hire($from: String, $to: String) {
    insert WorksAt { from: $from, to: $to }
}
"#,
        "hire",
        &params(&[("$from", "Charlie"), ("$to", "Acme")]),
    )
    .await
    .unwrap();

    // Historical: Charlie and Diana were unemployed
    let historical = db
        .run_query_at(v_before, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
        .await
        .unwrap();
    let hist_names = collect_column_strings(historical.batches(), "p.name");
    assert_eq!(historical.num_rows(), 2);
    assert!(hist_names.contains(&"Charlie".to_string()));
    assert!(hist_names.contains(&"Diana".to_string()));

    // Current: only Diana is unemployed
    let current = query_main(&mut db, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
        .await
        .unwrap();
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert_eq!(current.num_rows(), 1);
    assert!(cur_names.contains(&"Diana".to_string()));
    assert!(!cur_names.contains(&"Charlie".to_string()));
}

// ─── Negation (AntiJoin) × Delete edge ─────────────────────────────────────

#[tokio::test]
async fn negation_delete_edge_grows_antijoin_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice worksAt Acme, Bob worksAt Globex
    // Unemployed at start: Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    // Fire Alice (delete WorksAt edge)
    mutate_main(
        &mut db,
        r#"
query fire($from: String) {
    delete WorksAt where from = $from
}
"#,
        "fire",
        &params(&[("$from", "Alice")]),
    )
    .await
    .unwrap();

    // Historical: 2 unemployed (Charlie, Diana)
    let historical = db
        .run_query_at(v_before, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 2);
    let hist_names = collect_column_strings(historical.batches(), "p.name");
    assert!(!hist_names.contains(&"Alice".to_string()));

    // Current: 3 unemployed (Alice, Charlie, Diana)
    let current = query_main(&mut db, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 3);
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(cur_names.contains(&"Alice".to_string()));
}

// ─── Filtered × Update (value enters/exits filter) ─────────────────────────

#[tokio::test]
async fn filtered_update_entity_crosses_filter_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice(30), Bob(25), Charlie(35), Diana(28)
    // older_than(30): Charlie(35) only
    let v_before = version_main(&db).await.unwrap();

    // Update Bob's age from 25 to 40 → enters the filter
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    // Historical: only Charlie is older than 30
    let historical = db
        .run_query_at(
            v_before,
            FILTERED_QUERY,
            "older_than",
            &int_params(&[("$min_age", 30)]),
        )
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 1);
    let hist_names = collect_column_strings(historical.batches(), "p.name");
    assert_eq!(hist_names, vec!["Charlie"]);

    // Current: Bob(40) and Charlie(35) are older than 30
    let current = query_main(
        &mut db,
        FILTERED_QUERY,
        "older_than",
        &int_params(&[("$min_age", 30)]),
    )
    .await
    .unwrap();
    assert_eq!(current.num_rows(), 2);
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(cur_names.contains(&"Bob".to_string()));
    assert!(cur_names.contains(&"Charlie".to_string()));
}

// ─── Multi-hop traversal × Insert ──────────────────────────────────────────

#[tokio::test]
async fn multi_hop_traversal_historical_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice→Bob, Alice→Charlie, Bob→Diana
    // friends_of_friends(Alice) = Diana (Alice→Bob→Diana)
    let v_before = version_main(&db).await.unwrap();

    // Insert Eve and edge: Charlie→Eve
    // Now friends_of_friends(Alice) = Diana + Eve (Alice→Charlie→Eve)
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
        &params(&[("$from", "Charlie"), ("$to", "Eve")]),
    )
    .await
    .unwrap();

    let fof_query = r#"
query fof($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $f
    }
    return { $f.name }
    order { $f.name asc }
}
"#;

    // Historical: friends-of-friends of Alice = Diana only
    let historical = db
        .run_query_at(v_before, fof_query, "fof", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 1);
    let hist_names = collect_column_strings(historical.batches(), "f.name");
    assert_eq!(hist_names, vec!["Diana"]);

    // Current: friends-of-friends of Alice = Diana + Eve
    let current = query_main(&mut db, fof_query, "fof", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 2);
    let cur_names = collect_column_strings(current.batches(), "f.name");
    assert!(cur_names.contains(&"Diana".to_string()));
    assert!(cur_names.contains(&"Eve".to_string()));
}

// ─── Traversal × Delete node (cascade removes edges) ───────────────────────

#[tokio::test]
async fn traversal_delete_node_cascade_removes_edges() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Fixture: Alice knows Bob, Alice knows Charlie, Bob knows Diana
    let v_before = version_main(&db).await.unwrap();

    // Delete Bob → cascades to Knows edges involving Bob
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();

    // Historical: Alice's friends = Bob, Charlie
    let historical = db
        .run_query_at(
            v_before,
            FRIENDS_QUERY,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 2);
    let hist_names = collect_column_strings(historical.batches(), "f.name");
    assert!(hist_names.contains(&"Bob".to_string()));
    assert!(hist_names.contains(&"Charlie".to_string()));

    // Current: Alice's friends = Charlie only (Bob was deleted, edge cascaded)
    let current = query_main(
        &mut db,
        FRIENDS_QUERY,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(current.num_rows(), 1);
    let cur_names = collect_column_strings(current.batches(), "f.name");
    assert_eq!(cur_names, vec!["Charlie"]);
}

// ─── Branch isolation for point-in-time ────────────────────────────────────

#[tokio::test]
async fn branch_point_in_time_isolated_from_main() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let v_main_before = version_main(&main).await.unwrap();

    // Insert Eve on main
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Insert Frank on feature branch
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    // Historical main at v_main_before: 4 persons, no Eve, no Frank
    let hist_main = main
        .run_query_at(
            v_main_before,
            ALL_PERSONS_QUERY,
            "all_persons",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(hist_main.num_rows(), 4);
    let hist_names = collect_column_strings(hist_main.batches(), "p.name");
    assert!(!hist_names.contains(&"Eve".to_string()));
    assert!(!hist_names.contains(&"Frank".to_string()));

    // Current main: 5 persons (Eve present, Frank not visible on main)
    let cur_main = query_main(
        &mut main,
        ALL_PERSONS_QUERY,
        "all_persons",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(cur_main.num_rows(), 5);
    let cur_names = collect_column_strings(cur_main.batches(), "p.name");
    assert!(cur_names.contains(&"Eve".to_string()));
    assert!(!cur_names.contains(&"Frank".to_string()));

    // Feature branch: 5 persons (Frank present, Eve not visible on feature)
    let cur_feature = query_branch(
        &mut feature,
        "feature",
        ALL_PERSONS_QUERY,
        "all_persons",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(cur_feature.num_rows(), 5);
    let feat_names = collect_column_strings(cur_feature.batches(), "p.name");
    assert!(feat_names.contains(&"Frank".to_string()));
    assert!(!feat_names.contains(&"Eve".to_string()));
}

// ─── Multi-step version chain: insert → update → delete ────────────────────

#[tokio::test]
async fn four_version_chain_insert_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // v1: baseline (Alice=30, Bob=25, Charlie=35, Diana=28)
    let v1 = version_main(&db).await.unwrap();

    // v2: insert Eve(22)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    let v2 = version_main(&db).await.unwrap();

    // v3: update Eve's age to 50
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Eve")], &[("$age", 50)]),
    )
    .await
    .unwrap();
    let v3 = version_main(&db).await.unwrap();

    // v4: delete Eve
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();

    // v1: no Eve, 4 persons
    let at_v1 = db
        .run_query_at(v1, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(at_v1.num_rows(), 4);
    let v1_names = collect_column_strings(at_v1.batches(), "p.name");
    assert!(!v1_names.contains(&"Eve".to_string()));

    // v2: Eve exists with age 22, 5 persons
    let at_v2 = db
        .run_query_at(
            v2,
            GET_PERSON_QUERY,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(at_v2.num_rows(), 1);
    let v2_batch = at_v2.concat_batches().unwrap();
    let v2_ages = v2_batch
        .column_by_name("p.age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v2_ages.value(0), 22);

    // v3: Eve exists with age 50
    let at_v3 = db
        .run_query_at(
            v3,
            GET_PERSON_QUERY,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(at_v3.num_rows(), 1);
    let v3_batch = at_v3.concat_batches().unwrap();
    let v3_ages = v3_batch
        .column_by_name("p.age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v3_ages.value(0), 50);

    // v4 (current): Eve is gone, back to 4
    let current = query_main(&mut db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 4);
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(!cur_names.contains(&"Eve".to_string()));
}
