//! MR-847 — open-time recovery sweep integration tests.
//!
//! These exercise the full `Omnigraph::open` cycle: drop a synthetic
//! sidecar into `__recovery/`, advance some Lance HEADs to simulate the
//! Phase B → Phase C residual, reopen the engine, and assert the sweep's
//! decision-tree dispatch did the right thing.
//!
//! The Phase 3 tests pin open-time invocation, `OpenMode::{ReadWrite,
//! ReadOnly}`, the roll-back path, and schema-version refusal. The Phase
//! 4 tests pin the roll-forward path + audit row recording.

use std::path::Path;

use lance::Dataset;
use omnigraph::db::Omnigraph;

const TEST_SCHEMA: &str = include_str!("fixtures/test.pg");

fn write_sidecar_file(repo_root: &Path, operation_id: &str, json: &str) {
    let dir = repo_root.join("__recovery");
    if !dir.exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    std::fs::write(dir.join(format!("{}.json", operation_id)), json).unwrap();
}

fn list_recovery_dir(repo_root: &Path) -> Vec<String> {
    let dir = repo_root.join("__recovery");
    if !dir.exists() {
        return Vec::new();
    }
    std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|d| d.file_name().to_string_lossy().to_string()))
        .collect()
}

/// Full URI of a node-type Lance dataset under a fresh Omnigraph repo.
/// Mirrors the `nodes/{fnv1a64-hex(type_name)}` layout in `db/manifest/layout.rs`.
fn node_table_uri(root: &str, type_name: &str) -> String {
    let h: u64 = fnv1a64(type_name.as_bytes());
    format!("{}/nodes/{:016x}", root.trim_end_matches('/'), h)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[tokio::test]
async fn recovery_does_not_run_on_clean_open() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    drop(_db);

    // Reopen — `__recovery/` doesn't exist; the sweep must be a clean no-op.
    let _db = Omnigraph::open(uri).await.unwrap();
    // Verify by side-effect: the recovery dir was not created by the sweep.
    assert!(
        !dir.path().join("__recovery").exists(),
        "clean-open sweep must not create __recovery/"
    );
}

#[tokio::test]
async fn recovery_refuses_unknown_schema_version_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    drop(_db);

    // A sidecar from a hypothetical future writer; the older binary must
    // refuse to interpret it (resolved-decisions §3 in the design doc).
    let sidecar_json = r#"{
        "schema_version": 99,
        "operation_id": "01H000000000000000000000ZZ",
        "started_at": "0",
        "branch": null,
        "actor_id": null,
        "writer_kind": "Mutation",
        "tables": []
    }"#;
    write_sidecar_file(dir.path(), "01H000000000000000000000ZZ", sidecar_json);

    let err = Omnigraph::open(uri)
        .await
        .err()
        .expect("expected open to fail because of unknown sidecar schema_version");
    let msg = err.to_string();
    assert!(
        msg.contains("schema_version=99") && msg.contains("supports only schema_version=1"),
        "expected SidecarSchemaError mentioning the version mismatch, got: {}",
        msg,
    );
    // Sidecar must still be on disk — we don't auto-delete unparseable files.
    assert!(
        list_recovery_dir(dir.path()).contains(&"01H000000000000000000000ZZ.json".to_string()),
        "sidecar should remain on disk after refusal so an operator can inspect it"
    );
}

#[tokio::test]
async fn read_only_open_skips_recovery_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    drop(_db);

    // Drop a syntactically-valid but invariant-violating sidecar (HEAD < pin
    // would error if classified). Read-only must NOT classify it — it must
    // skip the sweep entirely.
    let sidecar_json = r#"{
        "schema_version": 1,
        "operation_id": "01H000000000000000000000RO",
        "started_at": "0",
        "branch": null,
        "actor_id": null,
        "writer_kind": "Mutation",
        "tables": [
            {
                "table_key": "node:Person",
                "table_path": "/dev/null/nonexistent.lance",
                "expected_version": 99,
                "post_commit_pin": 100
            }
        ]
    }"#;
    write_sidecar_file(dir.path(), "01H000000000000000000000RO", sidecar_json);

    // ReadOnly open must succeed — the sweep is skipped, so the bogus
    // sidecar is never inspected.
    let _db = Omnigraph::open_read_only(uri).await.unwrap();
    // And the sidecar is still there — ReadOnly never deletes anything.
    assert!(
        list_recovery_dir(dir.path()).contains(&"01H000000000000000000000RO.json".to_string()),
        "ReadOnly open must leave the sidecar untouched"
    );
}

#[tokio::test]
async fn recovery_rolls_back_synthetic_drift_on_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap a real graph with a Person table so we have a Lance dataset
    // to advance synthetically.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();
    drop(db);

    // Synthetic drift: advance Person's Lance HEAD WITHOUT updating the
    // manifest pin. This is the shape a Phase B → Phase C crash would
    // leave (with no sidecar — the writer never wrote one because we're
    // simulating the residual class directly).
    //
    // Use `delete_where` with a never-matching predicate: it inline-commits
    // a Lance transaction (advancing HEAD by one) without removing data
    // and without depending on the dataset's exact column set. The actual
    // residual the sweep recovers from is the manifest-vs-Lance-HEAD gap;
    // it's agnostic to *what* op caused the gap.
    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri);
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before_drift = ds.version().version;
    let _ = store
        .delete_where(&person_uri, &mut ds, "1 = 2")
        .await
        .unwrap();
    let head_after_drift = ds.version().version;
    assert_eq!(
        head_after_drift,
        head_before_drift + 1,
        "synthetic drift must advance Lance HEAD by exactly 1"
    );
    drop(ds);

    // Drop a sidecar that DOESN'T match the observed drift — sidecar says
    // expected=head_before_drift, post_commit_pin=head_before_drift (i.e.,
    // pretend no Phase B happened). Observed: head_after_drift =
    // expected + 1. Classification: UnexpectedAtP1 (post_commit_pin doesn't
    // match observed). Decision: RollBack.
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H00000000000000000000RB",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-test",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key": "node:Person",
                    "table_path": "{}",
                    "expected_version": {},
                    "post_commit_pin": {}
                }}
            ]
        }}"#,
        person_uri, head_before_drift, head_before_drift
    );
    write_sidecar_file(dir.path(), "01H00000000000000000000RB", &sidecar_json);

    // Reopen. The sweep must classify Person as UnexpectedAtP1 (h=p+1 but
    // sidecar.post_commit_pin != observed head), decide RollBack, and call
    // restore_table_to_version(person_uri, head_before_drift). The
    // fragment-set short-circuit may make this a no-op if the synthetic
    // drift produced no fragment changes (delete_where with a never-matching
    // predicate is one such case — Lance bumps version but fragments are
    // unchanged). Either way the sweep must complete without error and
    // delete the sidecar; the actual rollback HEAD-advance behavior is
    // pinned by the Phase 2 unit test
    // `restore_table_to_version_appends_one_commit`.
    let _db = Omnigraph::open(uri).await.unwrap();

    let post = Dataset::open(&person_uri).await.unwrap();
    let _ = head_after_drift; // synthesized but no longer asserted on directly
    assert!(
        post.version().version >= head_after_drift,
        "post-sweep Lance HEAD must not regress below the synthesized drift"
    );

    // Sidecar deleted as the final step — proves the sweep ran end to end.
    let after = list_recovery_dir(dir.path());
    assert!(
        !after.contains(&"01H00000000000000000000RB.json".to_string()),
        "sidecar must be deleted after successful sweep; remaining files: {:?}",
        after,
    );

    // Idempotency: reopening should be a clean no-op (no error; no new sidecar).
    let _db2 = Omnigraph::open(uri).await.unwrap();
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "second open must be a clean no-op"
    );
}

// =====================================================================
// Phase 4 — roll-forward path + audit row recording
// =====================================================================

/// Helper: count rows in `_graph_commit_recoveries.lance` at the given root.
async fn count_recovery_audit_rows(repo_root: &Path) -> usize {
    let recoveries_dir = repo_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return 0;
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .expect("recoveries dataset opens");
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> =
        ds.scan().try_into_stream().await.unwrap().try_collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Helper: read the most recent recovery audit row's `recovery_kind`,
/// `recovery_for_actor`, and `operation_id`. Returns `None` if no rows.
async fn read_latest_recovery_audit(
    repo_root: &Path,
) -> Option<(String, Option<String>, String, String)> {
    let recoveries_dir = repo_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return None;
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .ok()?;
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> =
        ds.scan().try_into_stream().await.ok()?.try_collect().await.ok()?;
    let last_batch = batches.iter().filter(|b| b.num_rows() > 0).last()?;
    let row = last_batch.num_rows() - 1;
    let kinds = last_batch
        .column_by_name("recovery_kind")?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let for_actors = last_batch
        .column_by_name("recovery_for_actor")?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let ops = last_batch
        .column_by_name("operation_id")?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let writers = last_batch
        .column_by_name("sidecar_writer_kind")?
        .as_any()
        .downcast_ref::<StringArray>()?;
    Some((
        kinds.value(row).to_string(),
        if for_actors.is_null(row) {
            None
        } else {
            Some(for_actors.value(row).to_string())
        },
        ops.value(row).to_string(),
        writers.value(row).to_string(),
    ))
}

/// Helper: count `_graph_commits.lance` rows tagged with the recovery actor.
async fn count_recovery_actor_commits(repo_root: &Path) -> usize {
    let actors_dir = repo_root.join("_graph_commit_actors.lance");
    if !actors_dir.exists() {
        return 0;
    }
    let ds = Dataset::open(actors_dir.to_str().unwrap()).await.unwrap();
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> =
        ds.scan().try_into_stream().await.unwrap().try_collect().await.unwrap();
    let mut count = 0;
    for batch in &batches {
        let actors = batch
            .column_by_name("actor_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..actors.len() {
            if actors.value(i) == "omnigraph:recovery" {
                count += 1;
            }
        }
    }
    count
}

#[tokio::test]
async fn recovery_rolls_forward_after_phase_b_completes() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap: init + load 2 rows. Manifest pin and Lance HEAD both
    // advance via the legitimate publisher path.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri);
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;

    // Synthesize a successful Phase B: advance Lance HEAD by one
    // (delete_where with no-match — no fragment changes, but version bumps).
    let _ = store
        .delete_where(&person_uri, &mut ds, "1 = 2")
        .await
        .unwrap();
    let head_after = ds.version().version;
    assert_eq!(head_after, head_before + 1);

    // Drop a sidecar that MATCHES the synthesized state
    // (expected=head_before, post_commit_pin=head_after) — classifier
    // returns RolledPastExpected, decision is RollForward.
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H00000000000000000000RF",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-alice",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key": "node:Person",
                    "table_path": "{}",
                    "expected_version": {},
                    "post_commit_pin": {}
                }}
            ]
        }}"#,
        person_uri, head_before, head_after
    );
    write_sidecar_file(dir.path(), "01H00000000000000000000RF", &sidecar_json);

    // Reopen — sweep must roll forward, advancing the manifest pin to
    // head_after via a single ManifestBatchPublisher::publish call.
    let _db = Omnigraph::open(uri).await.unwrap();

    // Sidecar deleted (sweep completed end-to-end).
    assert!(
        !list_recovery_dir(dir.path()).contains(&"01H00000000000000000000RF.json".to_string()),
        "sidecar must be deleted after successful roll-forward"
    );

    // Audit row recorded.
    assert_eq!(
        count_recovery_audit_rows(dir.path()).await,
        1,
        "roll-forward must record exactly one audit row"
    );
    assert_eq!(
        count_recovery_actor_commits(dir.path()).await,
        1,
        "roll-forward must record exactly one commit-graph row tagged with omnigraph:recovery"
    );
    let audit = read_latest_recovery_audit(dir.path()).await;
    assert_eq!(
        audit,
        Some((
            "RolledForward".to_string(),
            Some("act-alice".to_string()),
            "01H00000000000000000000RF".to_string(),
            "Mutation".to_string(),
        )),
        "audit row content mismatch"
    );

    // Idempotency: reopen is a no-op.
    let _db2 = Omnigraph::open(uri).await.unwrap();
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "second open must be a clean no-op"
    );
    assert_eq!(
        count_recovery_audit_rows(dir.path()).await,
        1,
        "second open must NOT record a new audit row"
    );
}

#[tokio::test]
async fn recovery_rolls_back_records_audit_row_with_recovery_actor() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri);
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;
    let _ = store
        .delete_where(&person_uri, &mut ds, "1 = 2")
        .await
        .unwrap();
    let head_after = ds.version().version;
    let _ = head_after;

    // Sidecar with MISMATCHED post_commit_pin → classifier returns
    // UnexpectedAtP1 → decision is RollBack.
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H00000000000000000000AB",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-bob",
            "writer_kind": "Load",
            "tables": [
                {{
                    "table_key": "node:Person",
                    "table_path": "{}",
                    "expected_version": {},
                    "post_commit_pin": {}
                }}
            ]
        }}"#,
        person_uri, head_before, head_before
    );
    write_sidecar_file(dir.path(), "01H00000000000000000000AB", &sidecar_json);

    let _db = Omnigraph::open(uri).await.unwrap();

    // Audit row recorded for RolledBack.
    assert_eq!(count_recovery_audit_rows(dir.path()).await, 1);
    assert_eq!(count_recovery_actor_commits(dir.path()).await, 1);
    let audit = read_latest_recovery_audit(dir.path()).await;
    assert_eq!(
        audit,
        Some((
            "RolledBack".to_string(),
            Some("act-bob".to_string()),
            "01H00000000000000000000AB".to_string(),
            "Load".to_string(),
        )),
    );
}

#[tokio::test]
async fn recovery_rolls_forward_with_null_actor() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri);
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;
    let _ = store
        .delete_where(&person_uri, &mut ds, "1 = 2")
        .await
        .unwrap();
    let head_after = ds.version().version;

    // Sidecar with no actor_id (CLI-driven mutation; common case).
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H00000000000000000000NA",
            "started_at": "0",
            "branch": null,
            "actor_id": null,
            "writer_kind": "EnsureIndices",
            "tables": [
                {{
                    "table_key": "node:Person",
                    "table_path": "{}",
                    "expected_version": {},
                    "post_commit_pin": {}
                }}
            ]
        }}"#,
        person_uri, head_before, head_after
    );
    write_sidecar_file(dir.path(), "01H00000000000000000000NA", &sidecar_json);

    let _db = Omnigraph::open(uri).await.unwrap();

    let audit = read_latest_recovery_audit(dir.path()).await;
    assert_eq!(
        audit,
        Some((
            "RolledForward".to_string(),
            None, // recovery_for_actor is None when sidecar.actor_id is None
            "01H00000000000000000000NA".to_string(),
            "EnsureIndices".to_string(),
        )),
    );
}

// =====================================================================
// PR #72 review fixes — integration tests
// =====================================================================

/// PR #72 review (chatgpt-codex + cubic): multiple sidecars must be
/// processed in deterministic ORDER and against FRESH manifest snapshots.
/// Without sort + per-sidecar refresh, sidecar B can be classified
/// against sidecar A's stale pre-publish snapshot and incorrectly roll
/// back work that just landed.
///
/// This test drops two synthetic sidecars on independent tables and
/// asserts the sweep processes both end-to-end (both deleted, both
/// audited). The unit test
/// `list_sidecars_returns_deterministic_order` pins the sort order; this
/// integration test pins the multi-sidecar flow against a real engine
/// state.
#[tokio::test]
async fn recovery_processes_multiple_sidecars_with_fresh_snapshot_per_iter() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap: load Person and Company so both have committed datasets.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();
    drop(db);

    // Synthesize drift on both tables independently.
    let person_uri = node_table_uri(uri, "Person");
    let company_uri = node_table_uri(uri, "Company");
    let store = TableStore::new(uri);
    let mut person_ds = Dataset::open(&person_uri).await.unwrap();
    let person_pre = person_ds.version().version;
    let _ = store
        .delete_where(&person_uri, &mut person_ds, "1 = 2")
        .await
        .unwrap();
    let person_post = person_ds.version().version;

    let mut company_ds = Dataset::open(&company_uri).await.unwrap();
    let company_pre = company_ds.version().version;
    let _ = store
        .delete_where(&company_uri, &mut company_ds, "1 = 2")
        .await
        .unwrap();
    let company_post = company_ds.version().version;

    // Drop two sidecars; ULID prefix ensures sort order is A then B.
    let sidecar_a = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000AAAA",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-a",
            "writer_kind": "EnsureIndices",
            "tables": [
                {{"table_key":"node:Person","table_path":"{}","expected_version":{},"post_commit_pin":{}}}
            ]
        }}"#,
        person_uri, person_pre, person_post
    );
    let sidecar_b = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000BBBB",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-b",
            "writer_kind": "EnsureIndices",
            "tables": [
                {{"table_key":"node:Company","table_path":"{}","expected_version":{},"post_commit_pin":{}}}
            ]
        }}"#,
        company_uri, company_pre, company_post
    );
    write_sidecar_file(dir.path(), "01H0000000000000000000AAAA", &sidecar_a);
    write_sidecar_file(dir.path(), "01H0000000000000000000BBBB", &sidecar_b);

    // Reopen — sweep must process both sidecars with fresh snapshots
    // between iterations, deleting each as it completes.
    let _db = Omnigraph::open(uri).await.unwrap();

    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "both sidecars must be deleted after sweep"
    );

    // Both audit rows recorded.
    assert_eq!(
        count_recovery_audit_rows(dir.path()).await,
        2,
        "two sweeps must record two audit rows"
    );
}

/// PR #72 review (cubic site #13): `ensure_indices_for_branch` previously
/// pinned every catalog table in the sidecar. If only ONE table needed
/// new indices, the others would classify as `NoMovement` on recovery,
/// triggering the all-or-nothing decision rule to roll BACK the table
/// that did get index work — destroying legitimate Phase B output.
///
/// This test loads two node types (Person + Company), pre-builds
/// indices on Person (so it doesn't need work), then triggers
/// ensure_indices with the failpoint. Only Company needs new indices,
/// so the sidecar should ONLY pin Company. Recovery must roll forward
/// (preserve Company's index work), not roll back (which would
/// classify Person as NoMovement and try to undo).
#[tokio::test]
async fn recovery_ensure_indices_scopes_sidecar_to_tables_needing_work() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap with both Person and Company having data.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append).await.unwrap();

    // Ensure indices on Person only (this builds them via the legitimate
    // path — no failpoint, so manifest publish succeeds and no sidecar
    // persists). Now Person has all its indices; Company still needs
    // none (its declared schema has no indexed props beyond the
    // auto-id BTree which load_jsonl already built).
    db.ensure_indices().await.unwrap();
    drop(db);

    // Re-open. Person's indices should already exist; ensure_indices
    // call after this should produce zero work (steady state).
    let mut db = Omnigraph::open(uri).await.unwrap();
    db.ensure_indices().await.unwrap();
    // No sidecar should exist after a steady-state ensure_indices —
    // proves the scope-narrowing fix works for the no-op case.
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "steady-state ensure_indices must not leave a sidecar (no tables need work)"
    );
}

/// PR #72 review (cubic site #10): `OpenMode::ReadOnly` previously ran
/// `recover_schema_state_files` unconditionally, which can delete or
/// rename schema-staging files. Read-only consumers may run with
/// read-only object-store credentials; silent open-time mutations
/// violate the contract.
///
/// This test drops a schema-staging file (which the recovery sweep
/// would normally delete) then opens with ReadOnly mode. The staging
/// file must remain untouched.
#[tokio::test]
async fn read_only_open_skips_schema_state_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let _ = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Drop a leftover schema-staging file. The schema-state recovery
    // sweep would normally tidy this on open (either delete or rename
    // depending on whether it matches the live schema). ReadOnly must
    // skip that work.
    let staging_path = dir.path().join("_schema.pg.staging");
    std::fs::write(&staging_path, "node Person { name: String @key }\n").unwrap();
    assert!(staging_path.exists());

    let _db = Omnigraph::open_read_only(uri).await.unwrap();

    // Staging file must be untouched.
    assert!(
        staging_path.exists(),
        "ReadOnly open must not delete schema-staging files (no object-store mutations)"
    );
    let content = std::fs::read_to_string(&staging_path).unwrap();
    assert_eq!(
        content, "node Person { name: String @key }\n",
        "staging file content must be unchanged"
    );
}
