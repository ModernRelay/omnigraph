mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnigraph::db::MergeOutcome;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{
    MergeWriteProbes, QueryIoProbes, with_merge_write_probes, with_query_io_probes,
    with_traversal_mode,
};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};

use helpers::*;

#[tokio::test(flavor = "multi_thread")]
async fn s3_compatible_graph_lifecycle_works() {
    let Some(uri) = s3_test_graph_uri("omnigraph-runtime") else {
        eprintln!("skipping s3 runtime test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let mut reopened = Omnigraph::open(&uri).await.unwrap();
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    assert!(snapshot.dataset("node:Person").is_some());
    assert!(snapshot.dataset("edge:Knows").is_some());

    let alice = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
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
    .to_rust_json()
    .unwrap();
    assert_eq!(eve[0]["p.name"], "RustFS-Eve");

    let run_only = query_main(
        &mut reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RunOnly")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(run_only[0]["p.name"], "RunOnly");
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_branch_change_merge_flow_works() {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use futures::TryStreamExt;
    use omnigraph::instrumentation::{MergePreparationOptions, with_merge_preparation_options};

    let Some(uri) = s3_test_graph_uri("omnigraph-branching") else {
        eprintln!("skipping s3 branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let mut main = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&main, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(&uri).await.unwrap();
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
    .to_rust_json()
    .unwrap();
    assert_eq!(after_merge[0]["p.name"], "Feature-Eve");
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()]
    );

    // Continue the existing backend journey with four genuinely divergent
    // tables. Each width gets independent branches from the same unchanged
    // main state; four inserts per side keep the entire fixture tiny.
    type Rows = BTreeMap<String, Vec<BTreeMap<String, Option<String>>>>;
    #[derive(Debug, PartialEq, Eq)]
    struct BranchState {
        manifest: u64,
        head: Option<String>,
        lineage: Vec<String>,
        // Dataset path, native lifetime, published version, actual native HEAD.
        pins: BTreeMap<String, (String, Option<String>, u64, u64)>,
        rows: Rows,
    }

    async fn branch_state(db: &Omnigraph, branch: &str) -> BranchState {
        let snapshot = snapshot_branch(db, branch).await.unwrap();
        let mut rows = BTreeMap::new();
        let mut pins = BTreeMap::new();
        for entry in snapshot.datasets() {
            let dataset = snapshot.open_dataset(&entry.type_key).await.unwrap();
            let mut stream = dataset.scan().try_into_stream().await.unwrap();
            let mut table_rows = Vec::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                assert!(table_rows.len() + batch.num_rows() <= 8);
                for row_index in 0..batch.num_rows() {
                    let mut row = BTreeMap::new();
                    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                        // Physical row versions are verified through table
                        // pins; retain all logical columns, including IDs and
                        // edge endpoints, with null distinct from empty text.
                        if matches!(
                            field.name().as_str(),
                            "_row_created_at_version" | "_row_last_updated_at_version"
                        ) {
                            continue;
                        }
                        let value = if column.is_null(row_index) {
                            None
                        } else {
                            Some(
                                arrow_cast::display::array_value_to_string(
                                    column.as_ref(),
                                    row_index,
                                )
                                .unwrap(),
                            )
                        };
                        row.insert(field.name().clone(), value);
                    }
                    assert!(row.get("id").and_then(Option::as_deref).is_some());
                    if entry.type_key.starts_with("edge:") {
                        assert!(row.get("src").and_then(Option::as_deref).is_some());
                        assert!(row.get("dst").and_then(Option::as_deref).is_some());
                    }
                    table_rows.push(row);
                }
            }
            table_rows.sort();
            rows.insert(entry.type_key.clone(), table_rows);
            let mut table_uri = format!(
                "{}/{}",
                db.uri().trim_end_matches('/'),
                entry.dataset_path.trim_start_matches('/')
            );
            if let Some(native) = &entry.native_dataset_branch {
                table_uri.push_str("/tree/");
                table_uri.push_str(native);
            }
            pins.insert(
                entry.type_key.clone(),
                (
                    entry.dataset_path.clone(),
                    entry.native_dataset_branch.clone(),
                    entry.published_dataset_version,
                    lance::Dataset::open(&table_uri)
                        .await
                        .unwrap()
                        .version()
                        .version,
                ),
            );
        }
        BranchState {
            manifest: snapshot.graph_manifest_version(),
            head: snapshot
                .graph_head((branch != "main").then_some(branch))
                .map(str::to_string),
            lineage: db
                .list_commits(Some(branch))
                .await
                .unwrap()
                .into_iter()
                .map(|commit| commit.graph_commit_id)
                .collect(),
            pins,
            rows,
        }
    }

    let main_before = branch_state(&reopened, "main").await;
    let feature_before = branch_state(&reopened, "feature").await;
    for width in [1, 4] {
        let source_branch = format!("general-source-{width}");
        let target_branch = format!("general-target-{width}");
        for branch in [&source_branch, &target_branch] {
            reopened
                .branch_create_from(ReadTarget::branch("main"), branch)
                .await
                .unwrap();
        }
        for (branch, side, age) in [
            (&source_branch, "Source", 31),
            (&target_branch, "Target", 32),
        ] {
            let inserts = format!(
                r#"{{"type":"Person","data":{{"name":"General-{side}","age":{age}}}}}
{{"type":"Company","data":{{"name":"General-{side}-Co"}}}}
{{"edge":"Knows","from":"General-{side}","to":"Alice","data":{{"since":"2026-01-01"}}}}
{{"edge":"WorksAt","from":"General-{side}","to":"General-{side}-Co"}}"#
            );
            reopened
                .load(branch, &inserts, LoadMode::Append)
                .await
                .unwrap();
        }
        let source_before = branch_state(&reopened, &source_branch).await;
        let target_before = branch_state(&reopened, &target_branch).await;
        let mut expected = target_before.rows.clone();
        for (key, source_rows) in &source_before.rows {
            let merged_rows = expected.get_mut(key).unwrap();
            for row in source_rows {
                if let Some(existing) = merged_rows.iter().find(|other| other["id"] == row["id"]) {
                    assert_eq!(existing, row, "shared base row changed unexpectedly");
                } else {
                    merged_rows.push(row.clone());
                }
            }
            merged_rows.sort();
        }
        assert_eq!(expected.len(), 4);
        assert_eq!(expected.values().map(Vec::len).sum::<usize>(), 20);

        let probes = MergeWriteProbes::default();
        let started = Instant::now();
        let outcome = with_merge_write_probes(
            probes.clone(),
            with_merge_preparation_options(
                MergePreparationOptions {
                    width,
                    additional_bytes: 128 * 1024 * 1024,
                },
                Box::pin(reopened.branch_merge(&source_branch, &target_branch)),
            ),
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(outcome, MergeOutcome::Merged);
        let preparation = probes.merge_preparation_snapshot();
        assert_eq!(preparation.admitted, 4, "{preparation:?}");
        assert_eq!(preparation.collected, 4, "{preparation:?}");
        assert_eq!(preparation.peak_active, width as u64, "{preparation:?}");
        assert_eq!(
            preparation.peak_uncollected, width as u64,
            "{preparation:?}"
        );
        assert_eq!(preparation.discarded, 0, "{preparation:?}");
        assert_eq!(preparation.budget_fallbacks, 0, "{preparation:?}");
        assert_eq!(
            preparation.active + preparation.ready + preparation.uncollected,
            0
        );
        assert_eq!(preparation.accounted_bytes, 0, "{preparation:?}");
        assert_eq!(preparation.scratch_owners, 0, "{preparation:?}");
        assert_eq!(preparation.scratch_bytes, 0, "{preparation:?}");
        assert!(
            probes.completed_full_walk_classification_calls()
                + probes.completed_lineage_classification_calls()
                >= 4,
            "all four tables must take general reconciliation"
        );
        assert_eq!(probes.proven_insert_history_read_calls(), 0);

        let fresh = Omnigraph::open(&uri).await.unwrap();
        let target_after = branch_state(&fresh, &target_branch).await;
        assert_eq!(target_after.rows, expected);
        assert_eq!(target_after.manifest, target_before.manifest + 1);
        assert_ne!(target_after.head, target_before.head);
        for (key, (path, native, version, head)) in &target_before.pins {
            let after = &target_after.pins[key];
            assert_eq!((&after.0, &after.1), (path, native));
            assert_eq!(after.2, version + 1);
            assert_eq!(after.3, head + 1);
        }
        assert_eq!(branch_state(&fresh, &source_branch).await, source_before);
        assert_eq!(branch_state(&fresh, "main").await, main_before);
        assert_eq!(branch_state(&fresh, "feature").await, feature_before);

        // Actual configured-backend timings are supplemental diagnostics. They
        // neither assert a speedup nor claim wire-request counts or a qualified
        // release benchmark; setup and exact state verification are untimed.
        eprintln!(
            "S3_MERGE_PREPARATION_DIAGNOSTIC {}",
            serde_json::json!({
                "diagnostic_only": true,
                "backend": "configured-s3-compatible",
                "width": width,
                "tables": 4,
                "merged_rows": 20,
                "elapsed_us": elapsed.as_micros(),
                "peak_active": preparation.peak_active,
                "peak_uncollected": preparation.peak_uncollected,
                "peak_accounted_bytes": preparation.peak_accounted_bytes,
                "budget_fallbacks": preparation.budget_fallbacks,
                "phases": probes.merge_timing_snapshot().into_iter().map(|phase| {
                    serde_json::json!({
                        "phase": phase.phase,
                        "total_us": phase.total_us,
                        "interval_count": phase.interval_count,
                    })
                }).collect::<Vec<_>>(),
            })
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_public_load_uses_hidden_run_and_publishes() {
    let Some(uri) = s3_test_graph_uri("omnigraph-public-load") else {
        eprintln!("skipping s3 public load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let schema =
        format!("{TEST_SCHEMA}\nnode Document {{\n    title: String @key\n    content: Blob\n}}\n");
    let graph_uri = format!("{uri}/graph");
    let db = Omnigraph::init(&graph_uri, &schema).await.unwrap();
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
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
    let mut reopened = Omnigraph::open(&graph_uri).await.unwrap();
    let loaded = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Loaded-Over-S3")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(loaded[0]["p.name"], "Loaded-Over-S3");

    // RFC-033's only server-safe source scheme is S3. Exercise it against the
    // configured backend rather than inferring remote behavior from file://:
    // two URI spellings for one object share one metadata probe and one payload
    // read, and keyed load owns the resulting bytes after the source disappears.
    let external_base = format!("{uri}/external/");
    let external_uri = format!("{external_base}shared~source.bin");
    let adapter = omnigraph::storage::storage_for_uri(&uri).unwrap();
    adapter
        .write_text(&external_uri, "RustFS external Blob")
        .await
        .unwrap();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(&external_base, ExternalBlobExecutionScope::ServerSafe).unwrap(),
    ])
    .unwrap();
    let db = db.with_external_blob_policy(policy).unwrap();
    let encoded_alias = external_uri.replace("~source", "%7Esource");
    assert_ne!(encoded_alias, external_uri);
    let rows = format!(
        "{}\n{}",
        serde_json::json!({
            "type": "Document",
            "data": {"title": "remote-a", "content": external_uri},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "remote-b", "content": encoded_alias},
        })
    );
    let probes = MergeWriteProbes::default();
    with_merge_write_probes(probes.clone(), db.load("main", &rows, LoadMode::Append))
        .await
        .unwrap();
    assert_eq!(probes.external_blob_probe_inputs(), 2);
    assert_eq!(
        probes.external_blob_probe_calls(),
        1,
        "normalized-equivalent S3 sources must issue one metadata probe"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        1,
        "one prepared keyed batch must read a normalized S3 source once"
    );

    adapter.delete(&external_uri).await.unwrap();
    let reopened = Omnigraph::open(&graph_uri).await.unwrap();
    for title in ["remote-a", "remote-b"] {
        let blob = read_managed_blob_bytes(
            &reopened,
            ReadTarget::branch("main"),
            node_blob_cell("Document", title, "content"),
        )
        .await;
        assert_eq!(&blob[..], b"RustFS external Blob");
    }
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
    let db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
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

    let writer = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    // TEST_DATA seeds Alice->Bob and Alice->Charlie Knows edges.
    load_jsonl(&writer, TEST_DATA, LoadMode::Overwrite)
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
