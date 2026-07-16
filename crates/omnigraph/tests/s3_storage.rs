mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnigraph::db::MergeOutcome;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes, with_traversal_mode};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

#[tokio::test(flavor = "multi_thread")]
async fn s3_compatible_graph_lifecycle_works() {
    let Some(uri) = s3_test_graph_uri("omnigraph-runtime") else {
        eprintln!("skipping s3 runtime test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    assert!(snapshot.entry("node:Person").is_some());
    assert!(snapshot.entry("edge:Knows").is_some());

    let alice = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(alice[0]["p.name"], "Alice");

    reopened
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "RustFS-Eve")], &[("$age", 29)]),
        )
        .await
        .unwrap();

    // Direct-to-target load: no run lifecycle, single publisher
    // commit lands the row.
    reopened
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"RunOnly","age":31}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();

    let mut reopened_again = Omnigraph::open(&uri).await.unwrap();
    let eve = query_main(
        &mut reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RustFS-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(eve[0]["p.name"], "RustFS-Eve");

    let run_only = query_main(
        &mut reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RunOnly")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(run_only[0]["p.name"], "RunOnly");
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_branch_change_merge_flow_works() {
    let Some(uri) = s3_test_graph_uri("omnigraph-branching") else {
        eprintln!("skipping s3 branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut main = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut main, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(&uri).await.unwrap();
    feature
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Feature-Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let before_merge = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(before_merge.num_rows(), 0);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let after_merge = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(after_merge[0]["p.name"], "Feature-Eve");
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_public_load_uses_hidden_run_and_publishes() {
    let Some(uri) = s3_test_graph_uri("omnigraph-public-load") else {
        eprintln!("skipping s3 public load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Loaded-Over-S3","age":34}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    // Direct-to-target writes: no run state machine, just the
    // published commit lands the row. Verify by reopening and reading.
    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let loaded = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Loaded-Over-S3")]),
    )
    .await
    .unwrap()
    .to_rust_json();
    assert_eq!(loaded[0]["p.name"], "Loaded-Over-S3");
}

/// The conditional-write contract the cluster ledger depends on (RFC-006):
/// versioned read -> If-Match replace -> stale token refused. Pins the
/// S3-compatible backend's behavior (RustFS in CI) — turns red if a backend
/// bump regresses conditional puts.
#[tokio::test(flavor = "multi_thread")]
async fn s3_adapter_conditional_writes_contract() {
    let Some(uri) = s3_test_graph_uri("adapter-cas") else {
        eprintln!("skipping s3 adapter cas test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    use omnigraph::storage::storage_for_uri;
    let adapter = storage_for_uri(&uri).unwrap();
    let object = format!("{uri}/cas-probe.json");

    assert!(adapter.write_text_if_absent(&object, "v1").await.unwrap());
    assert!(!adapter.write_text_if_absent(&object, "v1b").await.unwrap());
    assert_eq!(
        adapter
            .read_text_if_exists(&object)
            .await
            .unwrap()
            .as_deref(),
        Some("v1")
    );
    assert_eq!(
        adapter
            .read_text_if_exists(&format!("{uri}/missing-probe.json"))
            .await
            .unwrap(),
        None
    );

    let (text, version) = adapter.read_text_versioned(&object).await.unwrap();
    assert_eq!(text, "v1");
    let next = adapter
        .write_text_if_match(&object, "v2", &version)
        .await
        .unwrap()
        .expect("fresh etag must win");
    assert!(
        adapter
            .write_text_if_match(&object, "v3", &version)
            .await
            .unwrap()
            .is_none(),
        "stale etag must be refused"
    );
    let again = adapter
        .write_text_if_match(&object, "v3", &next)
        .await
        .unwrap();
    assert!(again.is_some());

    // Prefix delete: recursive + idempotent.
    adapter
        .write_text(&format!("{uri}/tree/a.json"), "a")
        .await
        .unwrap();
    adapter
        .write_text(&format!("{uri}/tree/sub/b.json"), "b")
        .await
        .unwrap();
    adapter.delete_prefix(&format!("{uri}/tree")).await.unwrap();
    assert!(!adapter.exists(&format!("{uri}/tree/a.json")).await.unwrap());
    adapter.delete_prefix(&format!("{uri}/tree")).await.unwrap();
    adapter.delete(&object).await.unwrap();
    assert_eq!(adapter.read_text_if_exists(&object).await.unwrap(), None);
}

/// Schema apply against an S3 graph — the cluster's schema executor will
/// lean on this; previously untested upstream on object storage.
#[tokio::test(flavor = "multi_thread")]
async fn s3_schema_apply_migrates_live_graph() {
    let Some(uri) = s3_test_graph_uri("schema-apply") else {
        eprintln!("skipping s3 schema apply test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let desired = format!("{TEST_SCHEMA}\nnode Note {{\n    title: String @key\n}}\n");
    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.applied, "{result:?}");

    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert!(
        reopened.schema_source().contains("Note"),
        "live S3 schema must carry the migration"
    );
}

/// Graph-index (CSR topology) cross-branch reuse on a real object store, where the
/// cache key's per-table `e_tag` is a genuine non-`None` token (Lance e_tag is
/// `None` on local FS, so the local twin in `warm_read_cost.rs` keys on `None` —
/// this exercises the e_tag-present path production runs). With e_tags present, a
/// fresh lazy-fork branch reuses main's cached index (`graph_build_count == 0`).
/// Forces CSR via the scoped `with_traversal_mode` seam (no env mutation, so no
/// interference with the other tests in this binary).
#[tokio::test(flavor = "multi_thread")]
async fn s3_fresh_branch_traversal_reuses_main_graph_index_with_etags() {
    let Some(uri) = s3_test_graph_uri("graph-index-etag") else {
        eprintln!("skipping s3 graph-index test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut writer = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    // TEST_DATA seeds Alice->Bob and Alice->Charlie Knows edges.
    load_jsonl(&mut writer, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // Separate reader: it never creates the branch, so branch_create below does
    // not invalidate the reader's warm cache.
    let reader = Omnigraph::open(&uri).await.unwrap();

    // Warm main on the CSR path: builds + caches the topology index keyed by the
    // edge table's physical identity incl. its real e_tag.
    let warm = with_traversal_mode(
        "csr",
        reader.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        first_column_sorted(&warm),
        vec!["Bob", "Charlie"],
        "test setup: Alice knows Bob and Charlie"
    );

    // Lazy fork: feature's edge tables are physically main's (same version +
    // e_tag, table_branch = None).
    writer.branch_create("feature").await.unwrap();

    let graph_build = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        graph_build_count: Arc::clone(&graph_build),
        ..Default::default()
    };
    let on_branch = with_traversal_mode(
        "csr",
        with_query_io_probes(
            probes,
            reader.query(
                ReadTarget::branch("feature"),
                TEST_QUERIES,
                "friends_of",
                &params(&[("$name", "Alice")]),
            ),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        first_column_sorted(&on_branch),
        vec!["Bob", "Charlie"],
        "fresh branch sees main's edges (lazy fork) and the reused index is correct"
    );
    assert_eq!(
        graph_build.load(Ordering::Relaxed),
        0,
        "with real e_tags, a fresh lazy-fork branch must reuse main's cached CSR index, not rebuild"
    );
}
