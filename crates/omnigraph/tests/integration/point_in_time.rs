use crate::helpers;

use crate::helpers::*;
use arrow_array::{Array, Int32Array};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;

#[tokio::test]
async fn wildcard_reads_refuse_historical_targets_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let version = version_main(&db).await.unwrap();
    let snapshot = snapshot_id(&db, "main").await.unwrap();
    let source = r#"query neighbors() {
        match { $p: Person, $f: Person, $p * $f } where $p.name = "Alice"
        return { $f.name }
    }"#;
    assert_eq!(
        query_main(&db, source, "neighbors", &ParamMap::new())
            .await
            .unwrap()
            .num_rows(),
        2
    );
    let explicit = source.replace("$p * $f", "$p (knows | knows) $f");
    assert_eq!(
        db.run_query_at(version, &explicit, "neighbors", &ParamMap::new())
            .await
            .unwrap()
            .num_rows(),
        2
    );
    let errors = [
        db.run_query_at(version, source, "neighbors", &ParamMap::new())
            .await
            .expect_err("historical wildcard"),
        db.query(
            ReadTarget::Snapshot(snapshot.clone()),
            source,
            "neighbors",
            &ParamMap::new(),
        )
        .await
        .expect_err("snapshot wildcard"),
        db.explain_query(
            ReadTarget::Snapshot(snapshot.clone()),
            source,
            "neighbors",
            &ParamMap::new(),
        )
        .await
        .expect_err("historical explain"),
        db.query_inspected(
            ReadTarget::Snapshot(snapshot),
            source,
            "neighbors",
            &ParamMap::new(),
        )
        .await
        .err()
        .expect("historical inspection"),
    ];
    for error in errors {
        let text = error.to_string();
        assert!(
            text.contains("wildcard") && text.contains("historical"),
            "{text}"
        );
    }
}

#[tokio::test]
async fn historical_explicit_unions_pin_all_members_and_refuse_a_member_absent_at_capture_issue_659()
 {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = version_main(&db).await.unwrap();
    db.apply_schema(&format!("{TEST_SCHEMA}\nedge Likes: Person -> Person\n"))
        .await
        .unwrap();
    db.load_jsonl(
        r#"{"edge":"Likes","from":"Alice","to":"Charlie"}"#,
        LoadMode::Insert,
    )
    .await
    .unwrap();
    let captured = version_main(&db).await.unwrap();
    let selected = r#"query neighbors() {
        match { $p: Person, $p $e:(knows | likes) $f } where $p.name = "Alice"
        return { $e.@type as edge_type, $f.name }
    }"#;
    assert_eq!(
        db.run_query_at(captured, selected, "neighbors", &ParamMap::new())
            .await
            .unwrap()
            .num_rows(),
        3
    );
    db.load_jsonl(
        r#"{"edge":"Likes","from":"Alice","to":"Bob"}"#,
        LoadMode::Insert,
    )
    .await
    .unwrap();
    assert_eq!(
        db.run_query_at(captured, selected, "neighbors", &ParamMap::new())
            .await
            .unwrap()
            .num_rows(),
        3
    );
    assert_eq!(
        query_main(&db, selected, "neighbors", &ParamMap::new())
            .await
            .unwrap()
            .num_rows(),
        4
    );
    use omnigraph_compiler::settings::Traversal;
    for (pattern, routes) in [
        (
            "likes",
            vec![Traversal::Auto, Traversal::Indexed, Traversal::Csr],
        ),
        (
            "$e:likes",
            vec![Traversal::Auto, Traversal::Indexed, Traversal::Csr],
        ),
        ("(knows | likes)", vec![Traversal::Auto, Traversal::Indexed]),
        (
            "$e:(knows | likes)",
            vec![Traversal::Auto, Traversal::Indexed],
        ),
    ] {
        for route in routes {
            let db = with_traversal(&db, route);
            let query = format!(
                "query missing() {{ match {{ $p: Person, $p {pattern} $f }} where $p.name = \"Alice\" return {{ $f.@id }} }}"
            );
            let error = db
                .run_query_at(before, &query, "missing", &ParamMap::new())
                .await
                .expect_err(
                    "a historical member without a captured dataset is refused consistently",
                );
            assert!(
                error.to_string().contains("Likes"),
                "{pattern} / {route:?}: {error}"
            );
            let empty = query.replace("Alice", "absent-source");
            assert_eq!(
                db.run_query_at(before, &empty, "missing", &ParamMap::new())
                    .await
                    .unwrap()
                    .num_rows(),
                0,
                "{pattern} / {route:?}"
            );
        }
    }
}

// ─── Inline queries for point-in-time tests ─────────────────────────────────

const ALL_PERSONS_QUERY: &str = r#"
query all_persons() {
    match { $p: Person }
    return { $p.name, $p.age }
    order { $p.name asc }
}
"#;

const FRIENDS_QUERY: &str = r#"
query friends_of($name: String) {
    match { $p: Person, $p knows $f }
    where $p.name = $name
    return { $f.name }
    order { $f.name asc }
}
"#;

const UNEMPLOYED_QUERY: &str = r#"
query unemployed() {
    match { $p: Person }
    where not exists { $p worksAt $_ }
    return { $p.name }
    order { $p.name asc }
}
"#;

const FILTERED_QUERY: &str = r#"
query older_than($min_age: I32) {
    match { $p: Person }
    where $p.age > $min_age
    return { $p.name, $p.age }
    order { $p.name asc }
}
"#;

const GET_PERSON_QUERY: &str = r#"
query get_person($name: String) {
    match { $p: Person }
    where $p.name = $name
    return { $p.name, $p.age }
}
"#;

const ALL_HUMANS_QUERY: &str = r#"
query all_humans() {
    match { $p: Human }
    return { $p.name }
}
"#;

const ALL_REPLACEMENT_PERSONS_QUERY: &str = r#"
query all_replacement_persons() {
    match { $p: Person }
    return { $p.name }
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
//   Error case                  (snapshot_at_graph_manifest_version_fails_for_nonexistent_version)
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
    let db = init_and_load(&dir).await;
    let v_before = version_main(&db).await.unwrap();

    mutate_main(
        &db,
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
    let current = query_main(&db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
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
async fn historical_query_resolves_rename_by_identity_but_rejects_reincarnation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(
        Omnigraph::init(
            uri,
            "node Person { name: String @key }\nnode Anchor { name: String @key }\n",
        )
        .await
        .unwrap(),
    );
    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Alice"}}"#,
        LoadMode::Replace,
    )
    .await
    .unwrap();
    let before_rename = version_main(&db).await.unwrap();
    let stale_handle = session(Omnigraph::open(uri).await.unwrap());
    let stale_snapshot_handle = session(Omnigraph::open(uri).await.unwrap());
    let snapshot_before_rename = db.resolve_snapshot("main").await.unwrap();

    db.apply_schema(
        "node Human @rename_from(\"Person\") { name: String @key }\n\
         node Anchor { name: String @key }\n",
    )
    .await
    .unwrap();
    let renamed_view = db
        .run_query_at(
            before_rename,
            ALL_HUMANS_QUERY,
            "all_humans",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(renamed_view.batches(), "p.name"),
        vec!["Alice".to_string()]
    );
    let stale_view = stale_handle
        .run_query_at(
            before_rename,
            ALL_HUMANS_QUERY,
            "all_humans",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(stale_view.batches(), "p.name"),
        vec!["Alice"]
    );
    let stale_snapshot_view = stale_snapshot_handle
        .query(
            omnigraph::db::ReadTarget::Snapshot(snapshot_before_rename),
            ALL_HUMANS_QUERY,
            "all_humans",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(stale_snapshot_view.batches(), "p.name"),
        vec!["Alice"]
    );

    db.apply_schema(
        "node Human { name: String @key }\n\
         node Person { name: String @key }\n\
         node Anchor { name: String @key }\n",
    )
    .await
    .unwrap();
    let still_renamed_view = db
        .run_query_at(
            before_rename,
            ALL_HUMANS_QUERY,
            "all_humans",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(still_renamed_view.batches(), "p.name"),
        vec!["Alice".to_string()],
        "reusing the old alias must not erase the historical binding for the renamed identity"
    );
    let error = db
        .run_query_at(
            before_rename,
            ALL_REPLACEMENT_PERSONS_QUERY,
            "all_replacement_persons",
            &ParamMap::new(),
        )
        .await
        .expect_err("a replacement type must not adopt the prior incarnation's rows");
    assert!(
        error
            .to_string()
            .contains("node type 'Person' does not exist at this snapshot"),
        "unexpected cross-incarnation refusal: {error}"
    );
}

/// A read at an earlier commit sees the schema accepted there (RFC GQ 3, A2),
/// through stable identities: a property renamed since reads under its
/// current name, a whole node holds the properties the commit holds, and
/// naming one the commit does not hold (added since, or dropped and added
/// again as a new property) is a type error naming the commit, before
/// anything runs. Both historical doors decide the same shape, the bound plan
/// records it, and replay at the same commit agrees.
/// `issue_1058_a_read_at_a_commit_sees_its_own_schema.gqt` holds the rows.
#[tokio::test]
async fn historical_query_binds_properties_by_identity_issue_1058() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(
        Omnigraph::init(
            uri,
            "node Person {\n    name: String @key\n    note: String?\n    memo: String?\n}\n\
             edge Knows: Person -> Person {\n    since: I32?\n}\n",
        )
        .await
        .unwrap(),
    );
    db.load(
        "main",
        concat!(
            r#"{"type":"Person","data":{"name":"Alice","note":"secret","memo":"kept"}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"Bob"}}"#,
            "\n",
            r#"{"edge":"Knows","from":"Alice","to":"Bob","data":{"since":2020}}"#,
        ),
        LoadMode::Replace,
    )
    .await
    .unwrap();
    let before = version_main(&db).await.unwrap();
    let before_snapshot = db.resolve_snapshot("main").await.unwrap();
    let commit = before_snapshot.as_str().to_string();

    db.apply_schema(
        "node Person {\n    name: String @key\n    note: String?\n    remark: String? @rename_from(\"memo\")\n}\n\
         edge Knows: Person -> Person {\n    since: I32?\n}\n",
    )
    .await
    .unwrap();
    db.apply_schema(
        "node Person {\n    name: String @key\n    remark: String?\n}\n\
         edge Knows: Person -> Person\n",
    )
    .await
    .unwrap();
    db.apply_schema(
        "node Person {\n    name: String @key\n    remark: String?\n    note: String?\n    age: I32?\n}\n\
         edge Knows: Person -> Person {\n    since: I32?\n}\n",
    )
    .await
    .unwrap();

    let note = "query q() { match { $p: Person } where $p.name = \"Alice\" return { $p.note } }";
    let live = query_main(&db, note, "q", &ParamMap::new()).await.unwrap();
    assert!(
        collect_column_strings(live.batches(), "p.note").is_empty(),
        "the re-added property starts empty on the live branch"
    );

    let absent = |owner: &str, property: &str, why: &str| {
        format!("`{owner}` has no property `{property}` at commit `{commit}`: {why}")
    };
    let again = |property: &str| {
        format!(
            "the `{property}` that commit holds was dropped since, and `{property}` was added again as a new property"
        )
    };
    for (source, expected) in [
        (note.to_string(), absent("Person", "note", &again("note"))),
        (
            "query q() { match { $p: Person } where $p.note = \"secret\" return { $p.name } }"
                .to_string(),
            absent("Person", "note", &again("note")),
        ),
        (
            "query q() { match { $p: Person } return { $p.name, $p.age } }".to_string(),
            absent("Person", "age", "it was added after that commit"),
        ),
        (
            "query q() { match { $p: Person, $p $k:knows $f } return { $f.name, $k.since } }"
                .to_string(),
            absent("Knows", "since", &again("since")),
        ),
    ] {
        let at_version = db
            .run_query_at(before, &source, "q", &ParamMap::new())
            .await
            .expect_err("a property lifetime absent at the commit must refuse");
        let at_snapshot = db
            .query(
                ReadTarget::Snapshot(before_snapshot.clone()),
                &source,
                "q",
                &ParamMap::new(),
            )
            .await
            .expect_err("a property lifetime absent at the commit must refuse");
        let explained = db
            .explain_query(
                ReadTarget::Snapshot(before_snapshot.clone()),
                &source,
                "q",
                &ParamMap::new(),
            )
            .await
            .expect_err("the shape is fixed before planning");
        for error in [at_version, at_snapshot, explained] {
            assert!(
                matches!(error, OmniError::Compiler(_))
                    && error.to_string().contains("T6")
                    && error.to_string().contains(&expected),
                "{source}: expected T6 {expected:?}; got {error:?}"
            );
        }
    }

    let renamed = "query q() { match { $p: Person } where $p.remark = \"kept\" return { $p.name, $p.remark } }";
    let whole = "query q() { match { $p: Person } return { $p } order { $p.name asc } }";
    for result in [
        db.run_query_at(before, renamed, "q", &ParamMap::new())
            .await
            .unwrap(),
        db.query(
            ReadTarget::Snapshot(before_snapshot.clone()),
            renamed,
            "q",
            &ParamMap::new(),
        )
        .await
        .unwrap(),
    ] {
        assert_eq!(
            collect_column_strings(result.batches(), "p.remark"),
            vec!["kept".to_string()],
            "a property renamed since reads under its current name through its identity"
        );
    }
    let rows = |result: &omnigraph_compiler::result::QueryResult| {
        let serde_json::Value::Array(rows) = result.to_rust_json().unwrap() else {
            panic!("rows")
        };
        rows
    };
    let expected = serde_json::json!([
        {"p": {"@id": "Alice", "name": "Alice", "remark": "kept"}},
        {"p": {"@id": "Bob", "name": "Bob"}},
    ]);
    let at_version = db
        .run_query_at(before, whole, "q", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(serde_json::Value::Array(rows(&at_version)), expected);

    let inspected = db
        .query_inspected(
            ReadTarget::Snapshot(before_snapshot.clone()),
            whole,
            "q",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(serde_json::Value::Array(rows(&inspected.result)), expected);
    let envelope = omnigraph_planner::ReplayEnvelope::new(
        whole,
        "q",
        inspected.plan.clone(),
        &inspected.evidence,
        inspected.catalog.clone(),
    )
    .to_bytes();
    let replayed = db
        .replay_bound_plan(ReadTarget::Snapshot(before_snapshot.clone()), &envelope)
        .await
        .unwrap();
    assert_eq!(serde_json::Value::Array(rows(&replayed.result)), expected);
    assert_eq!(
        replayed.result.schema(),
        inspected.result.schema(),
        "replay at the same commit answers the shape the bound plan recorded"
    );

    let catalog = db
        .query_catalog(ReadTarget::Snapshot(before_snapshot.clone()))
        .await
        .unwrap();
    let person = &catalog.node_types["Person"];
    let mut held: Vec<&str> = person.properties.keys().map(String::as_str).collect();
    held.sort_unstable();
    assert_eq!(held, ["name", "remark"]);
    assert_eq!(catalog.historical().unwrap().commit(), commit);
    assert!(
        db.query_catalog("main")
            .await
            .unwrap()
            .historical()
            .is_none()
    );
}

#[tokio::test]
async fn run_query_at_traversal_uses_historical_graph_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let v_before = version_main(&db).await.unwrap();

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    mutate_main(
        &db,
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
        &db,
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
async fn historical_exists_never_sees_an_edge_from_a_warm_live_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = version_main(&db).await.unwrap();
    db.apply_schema(&format!("{TEST_SCHEMA}\nedge Likes: Person -> Person\n"))
        .await
        .unwrap();
    db.load_jsonl(
        r#"{"edge":"Likes","from":"Alice","to":"Charlie"}"#,
        LoadMode::Insert,
    )
    .await
    .unwrap();
    db.optimize_as(
        None,
        omnigraph::db::OptimizeRequest::default().with_datasets(["node:Person"]),
    )
    .await
    .unwrap();
    let csr = with_traversal(&db, Traversal::Csr);
    let alice = params(&[("$name", "Alice")]);
    query_main(&csr, FRIENDS_QUERY, "friends_of", &alice)
        .await
        .unwrap();
    let both = r#"query both() {
        match { $p: Person } where exists { $p knows $_ } and exists { $p likes $_ }
        return { $p.name }
    }"#;
    let error = csr
        .run_query_at(before, both, "both", &ParamMap::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Likes"), "{error}");
}

/// A historical traversal takes its graph index from the handle's cache, as a
/// live read does: a second read of the version builds nothing, and the live
/// head, whose edge tables are that version's, reuses the same index. Before
/// that, every `run_query_at` built an index of its own and held it for the
/// query, so concurrent historical reads each held a copy.
#[tokio::test]
async fn run_query_at_reuses_the_cached_adjacency() {
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let version = version_main(&db).await.unwrap();
    // A node-only write: `Knows` at `version` is still the live edge table.
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    let csr = with_traversal(&db, Traversal::Csr);
    let alice = params(&[("$name", "Alice")]);

    let builds = Arc::new(AtomicU64::new(0));
    let probes = || QueryIoProbes {
        graph_build_count: Arc::clone(&builds),
        ..Default::default()
    };
    let historical = with_query_io_probes(
        probes(),
        csr.run_query_at(version, FRIENDS_QUERY, "friends_of", &alice),
    )
    .await
    .unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "the first historical traversal scans the edge table"
    );
    let again = with_query_io_probes(
        probes(),
        csr.run_query_at(version, FRIENDS_QUERY, "friends_of", &alice),
    )
    .await
    .unwrap();
    let live = with_query_io_probes(
        probes(),
        csr.query(
            ReadTarget::branch("main"),
            FRIENDS_QUERY,
            "friends_of",
            &alice,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "a second read of the version and a read of the live head reuse its index"
    );
    let friends = vec!["Bob".to_string(), "Charlie".to_string()];
    for result in [&historical, &again, &live] {
        let mut names = collect_column_strings(result.batches(), "f.name");
        names.sort();
        assert_eq!(names, friends);
    }
}

#[tokio::test]
async fn snapshot_at_graph_manifest_version_fails_for_nonexistent_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let result = db.snapshot_at_graph_manifest_version(99999).await;
    assert!(result.is_err(), "non-existent version should return error");
}

#[tokio::test]
async fn run_query_at_multiple_versions_sees_correct_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let v1 = version_main(&db).await.unwrap();

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();
    let v2 = version_main(&db).await.unwrap();

    mutate_main(
        &db,
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

    let current = query_main(&db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice, Bob, Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    mutate_main(
        &db,
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
    let current = query_main(&db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice knows Bob, Alice knows Charlie
    let v_before = version_main(&db).await.unwrap();

    // Remove all Knows edges FROM Alice
    mutate_main(
        &db,
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
        &db,
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice worksAt Acme, Bob worksAt Globex
    // Unemployed: Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    // Give Charlie a job
    mutate_main(
        &db,
        r#"
query hire($from: String, $to: String) {
    insert WorksAt { @src: $from, @dst: $to }
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
    let current = query_main(&db, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice worksAt Acme, Bob worksAt Globex
    // Unemployed at start: Charlie, Diana
    let v_before = version_main(&db).await.unwrap();

    // Fire Alice (delete WorksAt edge)
    mutate_main(
        &db,
        r#"
query fire($from: String) {
    match { $x: WorksAt } where $x.@src = $from
    delete $x
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
    let current = query_main(&db, UNEMPLOYED_QUERY, "unemployed", &ParamMap::new())
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice(30), Bob(25), Charlie(35), Diana(28)
    // older_than(30): Charlie(35) only
    let v_before = version_main(&db).await.unwrap();

    // Update Bob's age from 25 to 40 → enters the filter
    mutate_main(
        &db,
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
        &db,
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice→Bob, Alice→Charlie, Bob→Diana
    // friends_of_friends(Alice) = Diana (Alice→Bob→Diana)
    let v_before = version_main(&db).await.unwrap();

    // Insert Eve and edge: Charlie→Eve
    // Now friends_of_friends(Alice) = Diana + Eve (Alice→Charlie→Eve)
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Charlie"), ("$to", "Eve")]),
    )
    .await
    .unwrap();

    let fof_query = r#"
query fof($name: String) {
    match { $p: Person, $p knows $mid, $mid knows $f }
    where $p.name = $name
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
    let current = query_main(&db, fof_query, "fof", &params(&[("$name", "Alice")]))
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
    let db = init_and_load(&dir).await;
    // Fixture: Alice knows Bob, Alice knows Charlie, Bob knows Diana
    let v_before = version_main(&db).await.unwrap();

    // Delete Bob → cascades to Knows edges involving Bob
    mutate_main(
        &db,
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
        &db,
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
    let main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let v_main_before = version_main(&main).await.unwrap();

    // Insert Eve on main
    mutate_main(
        &main,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Insert Frank on feature branch
    let feature = session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
    let cur_main = query_main(&main, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(cur_main.num_rows(), 5);
    let cur_names = collect_column_strings(cur_main.batches(), "p.name");
    assert!(cur_names.contains(&"Eve".to_string()));
    assert!(!cur_names.contains(&"Frank".to_string()));

    // Feature branch: 5 persons (Frank present, Eve not visible on feature)
    let cur_feature = query_branch(
        &feature,
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
    let db = init_and_load(&dir).await;

    // v1: baseline (Alice=30, Bob=25, Charlie=35, Diana=28)
    let v1 = version_main(&db).await.unwrap();

    // v2: insert Eve(22)
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    let v2 = version_main(&db).await.unwrap();

    // v3: update Eve's age to 50
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Eve")], &[("$age", 50)]),
    )
    .await
    .unwrap();
    let v3 = version_main(&db).await.unwrap();

    // v4: delete Eve
    mutate_main(
        &db,
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
    let current = query_main(&db, ALL_PERSONS_QUERY, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(current.num_rows(), 4);
    let cur_names = collect_column_strings(current.batches(), "p.name");
    assert!(!cur_names.contains(&"Eve".to_string()));
}

// ─── Retention: reclaimed pinned versions are typed ─────────────────────────

/// Regression (test-first for the typed opener): a manifest-pinned Lance
/// version that cleanup has reclaimed must surface as the typed
/// `HistoricalVersionReclaimed` error, never as a generic storage string a
/// caller would have to parse.
#[tokio::test]
async fn historical_read_of_reclaimed_version_is_typed() {
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let v1 = version_main(&db).await.unwrap();

    // Sanity: the historical entity read works while the version is retained.
    let alice = db.entity_at("node:Person", "Alice", v1).await.unwrap();
    assert!(alice.is_some(), "Alice should exist at v1");

    // Advance the Person table so v1's pin becomes an old Lance version…
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    let stats = db
        .cleanup_as(helpers::collector::keep_one(), None)
        .await
        .unwrap();
    let person = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .expect("Person cleanup row");
    assert!(person.error.is_none(), "{person:?}");
    assert!(
        person.old_versions_removed > 0,
        "precondition: history was reclaimed"
    );

    let err = db.entity_at("node:Person", "Alice", v1).await.unwrap_err();
    assert!(
        matches!(err, OmniError::HistoricalVersionReclaimed { .. }),
        "expected the typed reclaimed-version error, got: {err:?}"
    );
}
