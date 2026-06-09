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

/// Regression: two sequential `LoadMode::Merge` invocations against the
/// same set of keys must both succeed. Pre-fix, the second one failed
/// with `Ambiguous merge inserts are prohibited: multiple source rows
/// match the same target row on (id = "TEST-1")` even though every
/// source batch had one row per key.
///
/// Triggered by Lance's `processed_row_ids: Mutex<HashSet<u64>>`
/// (lance-6.0.1 `src/dataset/write/merge_insert.rs:2099`) double-
/// processing the same source/target match against datasets previously
/// rewritten by merge_insert. Worked around by opting
/// `MergeInsertBuilder` into `SourceDedupeBehavior::FirstSeen` in
/// `crates/omnigraph/src/table_store.rs` — see that file for the full
/// rationale and the safety pin (`loader_rejects_intra_batch_duplicate_keys`).
/// Tracked at MR-957; upstream: lance-format/lance#6877.
#[tokio::test]
async fn load_merge_repeated_against_overlapping_keys_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    required_val: String
    optional_val: String?
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Seed with 50 fully-populated rows (id + required + optional).
    let mut seed = String::new();
    for i in 1..=50 {
        seed.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i}","optional_val":"optional {i}"}}}}
"#,
        ));
    }
    load_jsonl(&mut db, &seed, LoadMode::Overwrite)
        .await
        .unwrap();

    // Partial-schema delta — mirrors the bug report exactly: omits
    // `optional_val`. 25 existing keys + 5 new keys, one row per key.
    let mut delta = String::new();
    for i in (1..=25).chain(51..=55) {
        delta.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i} UPDATED"}}}}
"#,
        ));
    }

    load_jsonl(&mut db, &delta, LoadMode::Merge)
        .await
        .expect("first merge must succeed");
    assert_eq!(count_rows(&db, "node:Thing").await, 55);

    load_jsonl(&mut db, &delta, LoadMode::Merge)
        .await
        .expect("second merge against same keys must succeed");
    assert_eq!(count_rows(&db, "node:Thing").await, 55);
}

/// Safety pin for the `SourceDedupeBehavior::FirstSeen` workaround in
/// `crates/omnigraph/src/table_store.rs`. FirstSeen tells Lance to
/// silently skip a duplicate source row instead of erroring. Our use of
/// it depends on user-provided duplicates being rejected *before* the
/// batch reaches Lance — otherwise FirstSeen could silently drop user
/// data.
///
/// Defense in depth:
/// 1. The loader's `enforce_unique_constraints_intra_batch`
///    (`loader/mod.rs:1442`), invoked unconditionally on any node type
///    with a `@key`, errors on intra-batch duplicate `@key` values at
///    intake — pinned by this test across every `LoadMode`.
/// 2. The `check_batch_unique_by_keys` precondition at the top of
///    `merge_insert_batch` and `stage_merge_insert` is the final
///    fail-fast guard: even if a future caller bypasses the loader path
///    (e.g. branch-merge's `publish_rewritten_merge_table` builds its
///    own source batch directly), a real duplicate id reaches Lance
///    only after surfacing as an `OmniError::Manifest`, never silently
///    via FirstSeen. Pinned by the unit tests in `table_store::tests`.
#[tokio::test]
async fn loader_rejects_intra_batch_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    value: String
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let dupes = r#"{"type":"Thing","data":{"key":"DUP","value":"first"}}
{"type":"Thing","data":{"key":"DUP","value":"second"}}
"#;

    for mode in [LoadMode::Overwrite, LoadMode::Append, LoadMode::Merge] {
        let err = load_jsonl(&mut db, dupes, mode).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@unique violation") && msg.contains("DUP"),
            "load mode {mode:?} must reject intra-batch duplicate @key (got: {msg})"
        );
        assert_eq!(
            count_rows(&db, "node:Thing").await,
            0,
            "load mode {mode:?} must not persist any rows when the batch is rejected"
        );
    }
}

/// Regression for MR-983: a node-level composite `@unique(a, b)` must be
/// enforced as a true composite key, not degraded into independent
/// single-field checks. Pre-fix, `unique_property_names_for_node` flattened
/// every constraint group into one property list, so `@unique(source,
/// external_id)` was enforced as `@unique(source)` *and* `@unique(external_id)`
/// — rejecting rows that were unique on the composite key and naming only the
/// first field in the error.
#[tokio::test]
async fn loader_enforces_composite_unique_as_composite_key() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node ExternalID {
    slug: String @key
    source: String @index
    external_id: String @index
    @unique(source, external_id)
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Same `source`, different `external_id` → unique on the composite key.
    // This is the exact repro from MR-983 and must be accepted.
    let composite_ok = r#"{"type":"ExternalID","data":{"slug":"a","source":"whatsapp","external_id":"+E.164"}}
{"type":"ExternalID","data":{"slug":"b","source":"whatsapp","external_id":"pn:12345"}}
"#;
    load_jsonl(&mut db, composite_ok, LoadMode::Overwrite)
        .await
        .expect("rows unique on the composite (source, external_id) must be accepted");
    assert_eq!(count_rows(&db, "node:ExternalID").await, 2);

    // Both composite columns equal → genuine violation. The error must name
    // the whole composite, not just the first field.
    let composite_dupe = r#"{"type":"ExternalID","data":{"slug":"c","source":"whatsapp","external_id":"dup"}}
{"type":"ExternalID","data":{"slug":"d","source":"whatsapp","external_id":"dup"}}
"#;
    let err = load_jsonl(&mut db, composite_dupe, LoadMode::Overwrite)
        .await
        .unwrap_err();
    let msg = err.to_string();
    // Columns are canonicalized to sorted order in the catalog, so the
    // message reads `(external_id, source)`; assert order-agnostically that
    // both composite columns are named (not just the first, as pre-fix).
    assert!(
        msg.contains("@unique violation")
            && msg.contains("source")
            && msg.contains("external_id"),
        "composite violation must name both columns (got: {msg})"
    );
}

/// Guard: the intake path (load/insert/update) and the branch-merge path must
/// derive the same composite `@unique(a, b)` key, so a pair of rows unique on
/// the tuple is accepted by BOTH. Both paths now key on the tuple itself (no
/// separator), so a value containing any byte — including the `|` that an
/// earlier merge-path join used as its separator — can't forge a collision.
/// `("x|y", "z")` and `("x", "y|z")` are distinct tuples and must survive a
/// load-on-branch then merge without a phantom `UniqueViolation`. This pins the
/// cross-path consistency against any future drift in the shared keying.
#[tokio::test]
async fn composite_unique_key_is_consistent_across_intake_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Item {
    slug: String @key
    a: String @index
    b: String @index
    @unique(a, b)
}
"#;
    let insert_item = r#"
query insert_item($slug: String, $a: String, $b: String) {
    insert Item { slug: $slug, a: $a, b: $b }
}
"#;
    let main = Omnigraph::init(uri, schema).await.unwrap();
    main.branch_create("feature").await.unwrap();

    // Two rows unique on the composite (a, b), where `a`/`b` carry a literal
    // `|`. Distinct under a tuple key; identical (`x|y|z`) under a `|`-join.
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .mutate(
            "feature",
            insert_item,
            "insert_item",
            &params(&[("$slug", "r1"), ("$a", "x|y"), ("$b", "z")]),
        )
        .await
        .expect("intake must accept the first composite-unique row");
    feature
        .mutate(
            "feature",
            insert_item,
            "insert_item",
            &params(&[("$slug", "r2"), ("$a", "x"), ("$b", "y|z")]),
        )
        .await
        .expect("intake must accept the second composite-unique row (distinct on the tuple)");

    // The merge re-validates uniqueness over the adopted source rows. Both
    // rows are unique on (a, b), so this must merge cleanly with no phantom
    // conflict — intake and merge must key the tuple identically.
    let merge_result = feature.branch_merge("feature", "main").await;
    assert!(
        merge_result.is_ok(),
        "rows unique on the composite (a, b) must merge cleanly; \
         intake and merge must key the tuple the same way (got: {:?})",
        merge_result.err()
    );

    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&reopened, "node:Item").await, 2);
}

/// Canary for the upstream Lance gap that the `FirstSeen` workaround
/// in `table_store.rs` masks. The bug class is "Window 2": load →
/// indices built explicitly → merge → merge. Even with the engine
/// fully aligned to the "indexes are derived state" invariant
/// (MR-848), as long as an `id` index has been built between the
/// first and second merge_insert, the Lance internal that triggers
/// the bug remains reachable.
///
/// This test runs the Window-2 sequence under the FirstSeen workaround.
/// It is expected to pass today. If a future Lance upgrade or local
/// change makes it START failing, the workaround has lost effectiveness
/// (upstream Lance changed something, or the FirstSeen setter was
/// dropped from `table_store.rs`). If a future Lance upgrade fixes the
/// bug class, this test continues to pass and the FirstSeen setter can
/// be retired.
///
/// Tracked at MR-957; upstream: lance-format/lance#6877.
#[tokio::test]
async fn load_merge_window_2_documents_upstream_lance_gap() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    required_val: String
    optional_val: String?
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    let mut seed = String::new();
    for i in 1..=50 {
        seed.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i}","optional_val":"optional {i}"}}}}
"#,
        ));
    }
    load_jsonl(&mut db, &seed, LoadMode::Overwrite)
        .await
        .unwrap();

    // Explicit ensure_indices between seed and the merges — the Window
    // 2 trigger. The eager-build behavior (MR-583) means the BTREE on
    // `id` is already present here, but calling explicitly pins the
    // invariant for the post-MR-848 future where the eager build is
    // gone.
    db.ensure_indices().await.unwrap();

    let mut delta = String::new();
    for i in (1..=25).chain(51..=55) {
        delta.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i} UPDATED"}}}}
"#,
        ));
    }

    // Both merges must succeed under the FirstSeen workaround.
    // `processed_row_ids` re-processes the same target row_id under
    // the default `SourceDedupeBehavior::Fail`; FirstSeen tolerates it.
    load_jsonl(&mut db, &delta, LoadMode::Merge)
        .await
        .expect("first merge after ensure_indices must succeed");
    db.ensure_indices().await.unwrap();
    load_jsonl(&mut db, &delta, LoadMode::Merge).await.expect(
        "second merge after ensure_indices must succeed \
             (Window 2 canary: drop the FirstSeen setter in table_store.rs \
             only when this stays green WITHOUT it)",
    );
    assert_eq!(count_rows(&db, "node:Thing").await, 55);
}

#[tokio::test]
async fn cross_type_traversal_deduplicates_duplicate_edges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company
"#;
    let data = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme"}
{"edge":"WorksAt","from":"Alice","to":"Acme"}"#;
    let query = r#"
query company($name: String) {
    match {
        $p: Person { name: $name }
        $p worksAt $c
    }
    return { $c.name }
}
"#;

    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let result = query_main(&mut db, query, "company", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

// ─── Multi-writer refresh ───────────────────────────────────────────────────

#[tokio::test]
async fn explicit_target_query_sees_other_writer_commits_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();

    // Two independent handles to the same graph
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

// ─── Stale handle must refresh-and-retry (no silent rebase) ──────────────

#[tokio::test]
async fn stale_handle_public_mutation_must_refresh_then_retry() {
    // With the Run state machine removed, the engine no longer
    // auto-rebases stale-handle mutations onto the latest target head.
    // The publisher's `expected_table_versions` CAS makes the contract
    // explicit — a stale writer fails loudly with
    // `ExpectedVersionMismatch` and the client decides whether to
    // refresh-and-retry.
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    // Writer 1 inserts Eve — advances the Person sub-table.
    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Writer 2 is now stale. Its first attempt must fail with
    // ExpectedVersionMismatch — no silent rebase.
    let stale_err = mutate_main(
        &mut db2,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .expect_err("stale writer must hit ExpectedVersionMismatch");
    let omnigraph::error::OmniError::Manifest(manifest_err) = stale_err else {
        panic!("expected Manifest error");
    };
    assert!(matches!(
        manifest_err.details,
        Some(omnigraph::error::ManifestConflictDetails::ExpectedVersionMismatch { .. })
    ));

    // Refresh and retry — the canonical client recovery path.
    db2.sync_branch("main").await.unwrap();
    mutate_main(
        &mut db2,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    // Both Writer 1's insert and Writer 2's update are visible.
    let result = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.to_rust_json()[0]["p.age"], serde_json::json!(99));

    let eve = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1, "concurrent insert should be preserved");
}
