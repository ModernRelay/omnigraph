//! Open-time recovery sweep integration tests.
//!
//! These exercise the full `Omnigraph::open` cycle: drop a synthetic
//! sidecar into `__recovery/`, advance some Lance HEADs to simulate the
//! Phase B → Phase C residual, reopen the engine, and assert the sweep's
//! decision-tree dispatch did the right thing.
//!
//! Coverage: open-time invocation, `OpenMode::{ReadWrite, ReadOnly}`,
//! roll-back path, schema-version refusal, roll-forward path, and audit
//! row recording.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use omnigraph::db::Omnigraph;

mod helpers;
use helpers::recovery::{RecoveryExpectation, TableExpectation, assert_post_recovery_invariants};

const TEST_SCHEMA: &str = include_str!("fixtures/test.pg");

fn write_sidecar_file(graph_root: &Path, operation_id: &str, json: &str) {
    let dir = graph_root.join("__recovery");
    if !dir.exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    std::fs::write(dir.join(format!("{}.json", operation_id)), json).unwrap();
}

fn list_recovery_dir(graph_root: &Path) -> Vec<String> {
    let dir = graph_root.join("__recovery");
    if !dir.exists() {
        return Vec::new();
    }
    std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|d| d.file_name().to_string_lossy().to_string()))
        .collect()
}

/// Full URI of a node-type Lance dataset under a fresh Omnigraph graph.
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

/// Build a Person RecordBatch matching the post-init Lance schema:
/// `id: Utf8, age: Int32?, name: Utf8`. Used by tests that need to advance
/// Lance HEAD with real fragment changes (not no-op deletes) bypassing
/// `__manifest`.
fn person_batch(rows: &[(&str, &str, Option<i32>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<&str> = rows.iter().map(|(id, _, _)| *id).collect();
    let names: Vec<&str> = rows.iter().map(|(_, name, _)| *name).collect();
    let ages: Vec<Option<i32>> = rows.iter().map(|(_, _, age)| *age).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(ages)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
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

    // A sidecar from a hypothetical future writer (version NEWER than this
    // binary's max); the reader must refuse to interpret it — it cannot guess
    // semantics a newer writer baked in. (Older versions are accepted and
    // interpreted with their original semantics; see `parse_sidecar`.)
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
        .expect("expected open to fail because of a future sidecar schema_version");
    let msg = err.to_string();
    assert!(
        msg.contains("schema_version=99") && msg.contains("newer than the maximum"),
        "expected a future-version refusal, got: {}",
        msg,
    );
    // Sidecar must still be on disk — we don't auto-delete unparseable files.
    assert!(
        list_recovery_dir(dir.path()).contains(&"01H000000000000000000000ZZ.json".to_string()),
        "sidecar should remain on disk after refusal so an operator can inspect it"
    );
}

#[tokio::test]
async fn recovery_refuses_corrupt_sidecar_on_open_and_write() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // A truncated/garbage sidecar — e.g. a crashed writer or a partial
    // local-FS write (S3 PutObject is atomic; local fs::write is not).
    write_sidecar_file(dir.path(), "01H000000000000000000000CC", "{not json");

    // A live handle's write-entry heal must surface the parse failure
    // loudly instead of proceeding over a sidecar it cannot interpret.
    let err = load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
        LoadMode::Merge,
    )
    .await
    .err()
    .expect("expected the write to fail on the corrupt sidecar");
    assert!(
        err.to_string().contains("is not valid JSON"),
        "expected the corrupt-sidecar parse error, got: {}",
        err,
    );

    // A fresh ReadWrite open fails the same way.
    drop(db);
    let err = Omnigraph::open(uri)
        .await
        .err()
        .expect("expected open to fail because of the corrupt sidecar");
    let msg = err.to_string();
    assert!(
        msg.contains("01H000000000000000000000CC") && msg.contains("is not valid JSON"),
        "expected the corrupt-sidecar parse error naming the file, got: {}",
        msg,
    );
    // The file must remain on disk for inspection — never auto-deleted.
    assert!(
        list_recovery_dir(dir.path()).contains(&"01H000000000000000000000CC.json".to_string()),
        "corrupt sidecar should remain on disk after refusal"
    );

    // Read-only open still works — the sweep is skipped entirely.
    let _db = Omnigraph::open_read_only(uri).await.unwrap();
}

/// The commit-time drift guard's advice must be branch-aware: a pending
/// sidecar on ANOTHER branch does not cover this branch's drift. With a
/// deferred feature-branch sidecar on disk and genuinely uncovered drift
/// on main, the main write must still point at `omnigraph repair` — a
/// read-write reopen recovers the sidecar but cannot repair main's
/// uncovered drift.
#[tokio::test]
async fn drift_guard_advice_ignores_other_branch_sidecars() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    // A real feature write forks Person's Lance dataset onto the branch
    // (the heal classifies a feature sidecar against the forked head).
    db.mutate(
        "feature",
        helpers::MUTATION_QUERIES,
        "insert_person",
        &helpers::mixed_params(&[("$name", "eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // A sidecar pinning node:Person ON FEATURE, shaped so the write-entry
    // heal defers it (head < expected_version classifies as an invariant
    // violation; roll-forward-only mode leaves it for the next ReadWrite
    // open) — it persists through the write attempt below.
    let person_uri = node_table_uri(uri, "Person");
    let sidecar_json = format!(
        r#"{{
        "schema_version": 1,
        "operation_id": "01H000000000000000000000XB",
        "started_at": "0",
        "branch": "feature",
        "actor_id": null,
        "writer_kind": "Mutation",
        "tables": [
            {{
                "table_key": "node:Person",
                "table_path": "{person_uri}",
                "expected_version": 999,
                "post_commit_pin": 1000,
                "table_branch": "feature"
            }}
        ]
    }}"#
    );
    write_sidecar_file(dir.path(), "01H000000000000000000000XB", &sidecar_json);

    // Genuinely uncovered drift on MAIN's Person (raw Lance write
    // bypassing the manifest — the `omnigraph repair` class).
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;

    let err = load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
        LoadMode::Merge,
    )
    .await
    .err()
    .expect("uncovered main drift must fail the write");
    assert!(
        err.to_string().contains("run `omnigraph repair`"),
        "a feature-branch sidecar must not flip main's uncovered-drift \
         advice to the reopen path; got: {err}"
    );
}

/// A deferred sidecar pinned to a branch that is subsequently DELETED
/// must not wedge the graph: the branch's tree and forks are reclaimed,
/// so the pinned drift is unreachable and the sidecar is provably moot.
/// Both the write-entry heal and the open-time sweep must classify it
/// as orphaned (audit + discard) instead of failing to open the dead
/// branch on every write and every ReadWrite open — a terminal state,
/// since `repair` refuses while a sidecar is pending.
#[tokio::test]
async fn deleted_branch_sidecar_does_not_wedge_writes_or_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        helpers::MUTATION_QUERIES,
        "insert_person",
        &helpers::mixed_params(&[("$name", "eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // A rollback-eligible (deferred) sidecar pinned to feature — shaped
    // so every roll-forward-only pass leaves it on disk.
    let person_uri = node_table_uri(&uri, "Person");
    let sidecar_json = format!(
        r#"{{
        "schema_version": 1,
        "operation_id": "01H000000000000000000000DB",
        "started_at": "0",
        "branch": "feature",
        "actor_id": null,
        "writer_kind": "Mutation",
        "tables": [
            {{
                "table_key": "node:Person",
                "table_path": "{person_uri}",
                "expected_version": 999,
                "post_commit_pin": 1000,
                "table_branch": "feature"
            }}
        ]
    }}"#
    );
    write_sidecar_file(dir.path(), "01H000000000000000000000DB", &sidecar_json);

    // Branch delete defers the rollback-eligible sidecar and proceeds —
    // the sidecar now references a branch that no longer exists.
    db.branch_delete("feature").await.unwrap();

    // The next write's heal must classify the orphan and discard it,
    // not fail opening the dead branch.
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
        LoadMode::Merge,
    )
    .await
    .expect("a write after deleting a sidecar-pinned branch must succeed");
    assert_eq!(
        list_recovery_dir(dir.path()).len(),
        0,
        "the orphaned sidecar must be discarded (with an audit row), not left to wedge"
    );

    // And a fresh ReadWrite open must succeed too (the sweep shares the
    // same classification).
    drop(db);
    let db = Omnigraph::open(&uri)
        .await
        .expect("ReadWrite open after deleting a sidecar-pinned branch must succeed");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
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

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap a real graph with a Person table so we have a Lance dataset
    // to advance synthetically.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    drop(db);

    // Synthetic drift: advance Person's Lance HEAD WITHOUT updating the
    // manifest pin. This is the shape a Phase B → Phase C crash would
    // leave (with no sidecar — the writer never wrote one because we're
    // simulating the residual class directly).
    //
    // Use `lance_delete_inline` (a test helper that calls Lance directly) with
    // a never-matching predicate: it inline-commits
    // a Lance transaction (advancing HEAD by one) without removing data
    // and without depending on the dataset's exact column set. The actual
    // residual the sweep recovers from is the manifest-vs-Lance-HEAD gap;
    // it's agnostic to *what* op caused the gap.
    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before_drift = ds.version().version;
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;
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
    // drift produced no fragment changes (lance_delete_inline with a never-matching
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

/// Regression: recovery roll-back must PUBLISH the restored version so
/// `manifest == Lance HEAD` afterward (no residual "orphaned drift"). Before the
/// fix, roll-back restored via `Dataset::restore` but left the manifest pin
/// behind HEAD, so a subsequent strict write / schema apply failed its
/// HEAD-vs-manifest precondition ("stale view … refresh and retry") — and a
/// failed schema apply's own roll-back leaked +1 each retry (the original bug's
/// loop). With convergence, one roll-back leaves `manifest == HEAD` and the
/// follow-up succeeds.
#[tokio::test]
async fn recovery_rollback_converges_manifest_so_schema_apply_succeeds() {
    use omnigraph::db::ReadTarget;
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    drop(db);

    // Forge a Phase-B residual: advance Person's Lance HEAD without publishing to
    // the manifest (the manifest pin stays at the load's committed version).
    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let manifest_pin = ds.version().version;
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;
    drop(ds);

    // Roll-back-classified sidecar (post_commit_pin != observed head ⇒
    // UnexpectedAtP1 ⇒ RollBack).
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000CVG",
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
        person_uri, manifest_pin, manifest_pin
    );
    write_sidecar_file(dir.path(), "01H0000000000000000000CVG", &sidecar_json);

    // Reopen runs the sweep: restore Person to manifest_pin, then PUBLISH so the
    // manifest tracks the restored Lance HEAD.
    let db = Omnigraph::open(uri).await.unwrap();

    // Convergence: manifest pin == Lance HEAD. Fails before the fix — the
    // manifest stays at manifest_pin while HEAD advanced past it.
    let snap = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snap.entry("node:Person").unwrap();
    let lance_head = Dataset::open(&person_uri).await.unwrap().version().version;
    assert_eq!(
        entry.table_version, lance_head,
        "roll-back must publish so manifest pin ({}) == Lance HEAD ({})",
        entry.table_version, lance_head,
    );

    // The +1-loop victim: an additive schema apply must now succeed (its
    // HEAD-vs-manifest precondition is satisfied). Before the fix this failed
    // with "stale view … refresh and retry".
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    db.apply_schema(&desired)
        .await
        .expect("schema apply after a converging roll-back must succeed");
}

// =====================================================================
// Phase 4 — roll-forward path + audit row recording
// =====================================================================

/// Helper: count rows in `_graph_commit_recoveries.lance` at the given root.
async fn count_recovery_audit_rows(graph_root: &Path) -> usize {
    let recoveries_dir = graph_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return 0;
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .expect("recoveries dataset opens");
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Helper: read the most recent recovery audit row's `recovery_kind`,
/// `recovery_for_actor`, and `operation_id`. Returns `None` if no rows.
async fn read_latest_recovery_audit(
    graph_root: &Path,
) -> Option<(String, Option<String>, String, String)> {
    let recoveries_dir = graph_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return None;
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap()).await.ok()?;
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .ok()?
        .try_collect()
        .await
        .ok()?;
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

/// Helper: read every recovery audit row's `recovery_kind` value, in
/// storage order (multiple batches concatenated). Used by the
/// multi-sidecar fresh-snapshot test as a diagnostic alongside the
/// post-recovery Lance HEAD assertion.
async fn list_recovery_audit_kinds(graph_root: &Path) -> Vec<String> {
    let recoveries_dir = graph_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return Vec::new();
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .unwrap();
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let batches: Vec<arrow_array::RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let kinds = batch
            .column_by_name("recovery_kind")
            .expect("recovery_kind column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("recovery_kind is Utf8");
        for i in 0..kinds.len() {
            out.push(kinds.value(i).to_string());
        }
    }
    out
}

/// Helper: count graph commits authored by the recovery actor. RFC-013 Phase 7
/// records the recovery commit in `__manifest` (folded into the recovery publish
/// CAS), not `_graph_commits.lance`, so this counts through the production
/// commit-graph projection (`load_commits`), filtering on the inline actor.
async fn count_recovery_actor_commits(graph_root: &Path) -> usize {
    let commits = omnigraph::db::commit_graph::CommitGraph::open(graph_root.to_str().unwrap())
        .await
        .unwrap()
        .load_commits()
        .await
        .unwrap();
    commits
        .iter()
        .filter(|c| c.actor_id.as_deref() == Some("omnigraph:recovery"))
        .count()
}

#[tokio::test]
async fn recovery_rolls_forward_after_phase_b_completes() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap: init + load 2 rows. Manifest pin and Lance HEAD both
    // advance via the legitimate publisher path.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;

    // Synthesize a successful Phase B: advance Lance HEAD by one
    // (lance_delete_inline with no-match — no fragment changes, but version bumps).
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;
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
    let db = Omnigraph::open(uri).await.unwrap();
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        "01H00000000000000000000RF",
        RecoveryExpectation::RolledForward {
            tables: vec![TableExpectation::main("node:Person").expected_lance_head(head_after)],
        },
    )
    .await
    .unwrap();
}

/// A previous recovery's `roll_forward_all` succeeded (manifest pin
/// already advanced past the sidecar's `expected_version`) but
/// `record_audit` or sidecar deletion failed, leaving the sidecar to be
/// re-discovered on a subsequent open. The naive RollBack arm would
/// classify all tables as NoMovement and record a `RolledBack` audit row
/// with empty outcomes — misleading because the actual outcome was a
/// successful roll-forward. Recovery must detect this stale-after-
/// success shape and record `RolledForward` instead.
///
/// Synthesizes the state by:
/// 1. Letting init + load advance the manifest pin AND Lance HEAD
///    legitimately to some version `v`.
/// 2. Writing a sidecar whose `expected_version < v` and
///    `post_commit_pin == v` — exactly the shape left over after a
///    publisher succeeds but audit fails.
///
/// On reopen the classifier sees `lance_head == manifest_pinned == v`
/// → all NoMovement → decide returns RollBack. The new audit-recovery
/// branch must detect `manifest_pinned > expected_version` and record
/// `RolledForward` with `from_version=expected_version`,
/// `to_version=v`.
#[tokio::test]
async fn recovery_records_rolled_forward_for_stale_sidecar_after_successful_roll_forward() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();

    // Capture the current manifest pin and Lance HEAD — these match
    // because the load went through the publisher.
    let person_entry = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .expect("Person entry exists post-load")
        .clone();
    let manifest_pin = person_entry.table_version;
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let head_now = Dataset::open(&person_uri).await.unwrap().version().version;
    assert_eq!(
        head_now, manifest_pin,
        "Lance HEAD must equal manifest pin in steady state"
    );
    // Sidecar shape that simulates "publisher succeeded; audit/delete
    // failed in a previous recovery pass". `expected_version` is less
    // than the current manifest pin (the publish already ran) and
    // `post_commit_pin` matches the current head.
    let stale_expected = manifest_pin - 1;
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H00000000000000000000SF",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-original",
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
        person_uri, stale_expected, manifest_pin
    );
    write_sidecar_file(dir.path(), "01H00000000000000000000SF", &sidecar_json);

    // Reopen — sweep must classify Person as NoMovement (head_now ==
    // manifest_pinned) but recognize stale-after-success because
    // manifest_pinned > stale_expected. Audit-recovery branch records
    // RolledForward and deletes the sidecar.
    let _db = Omnigraph::open(uri).await.unwrap();

    // Sidecar deleted.
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "stale-after-success sidecar must be deleted after audit-recovery"
    );

    // Audit row says RolledForward (not RolledBack).
    let audit = read_latest_recovery_audit(dir.path()).await;
    assert_eq!(
        audit,
        Some((
            "RolledForward".to_string(),
            Some("act-original".to_string()),
            "01H00000000000000000000SF".to_string(),
            "Mutation".to_string(),
        )),
        "stale-after-success sidecar must record RolledForward, not RolledBack"
    );
    // Audit outcomes report from_version=stale_expected, to_version=manifest_pin.
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let recoveries_dir = dir.path().join("_graph_commit_recoveries.lance");
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .unwrap();
    let batches: Vec<arrow_array::RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let last = batches.iter().filter(|b| b.num_rows() > 0).last().unwrap();
    let row = last.num_rows() - 1;
    let outcomes_json = last
        .column_by_name("per_table_outcomes_json")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row);
    let outcomes: serde_json::Value = serde_json::from_str(outcomes_json).unwrap();
    let arr = outcomes.as_array().unwrap();
    assert_eq!(arr.len(), 1, "outcomes must include the Person table");
    let outcome = &arr[0];
    assert_eq!(outcome["table_key"], "node:Person");
    assert_eq!(outcome["from_version"].as_u64().unwrap(), stale_expected);
    assert_eq!(outcome["to_version"].as_u64().unwrap(), manifest_pin);
}

#[tokio::test]
async fn recovery_rolls_back_records_audit_row_with_recovery_actor() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;
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

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let head_before = ds.version().version;
    let _ = helpers::lance_delete_inline(&mut ds, "1 = 2").await;
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
// Multi-sidecar processing — integration tests
// =====================================================================

/// Multiple sidecars must be processed in deterministic ORDER and against
/// FRESH manifest snapshots. Without sort + per-sidecar refresh, sidecar
/// B can be classified against sidecar A's stale pre-publish snapshot
/// and incorrectly roll back work that just landed.
///
/// This test drops two synthetic sidecars on independent tables and
/// asserts the sweep processes both end-to-end (both deleted, both
/// audited). The unit test `list_sidecars_returns_deterministic_order`
/// pins the sort order; this integration test pins the multi-sidecar
/// flow against a real engine state.
#[tokio::test]
async fn recovery_processes_multiple_sidecars_with_fresh_snapshot_per_iter() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap: load Person and Company so both have committed datasets.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    drop(db);

    // Synthesize drift on both tables independently.
    let person_uri = node_table_uri(uri, "Person");
    let company_uri = node_table_uri(uri, "Company");
    let mut person_ds = Dataset::open(&person_uri).await.unwrap();
    let person_pre = person_ds.version().version;
    let _ = helpers::lance_delete_inline(&mut person_ds, "1 = 2").await;
    let person_post = person_ds.version().version;

    let mut company_ds = Dataset::open(&company_uri).await.unwrap();
    let company_pre = company_ds.version().version;
    let _ = helpers::lance_delete_inline(&mut company_ds, "1 = 2").await;
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

/// `ensure_indices_for_branch` must only pin tables that actually need
/// new index work. If it pinned every catalog table and only one needed
/// new indices, the others would classify as `NoMovement` on recovery,
/// triggering the all-or-nothing decision rule to roll BACK the table
/// that did get index work — destroying legitimate Phase B output.
///
/// Steady-state case: when nothing needs indexing, no sidecar should
/// be written. The sibling test `recovery_ensure_indices_handles_empty_tables`
/// covers the more nuanced empty-table case where the existing
/// ensure_indices loop has `if row_count > 0 { build_indices(...) }` —
/// empty tables produce zero commits and would otherwise force
/// NoMovement → rollback.
#[tokio::test]
async fn recovery_ensure_indices_steady_state_no_sidecar() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}
"#;
    load_jsonl(&mut db, test_data, LoadMode::Append)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    drop(db);

    let mut db = Omnigraph::open(uri).await.unwrap();
    db.ensure_indices().await.unwrap();
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "steady-state ensure_indices must not leave a sidecar (no tables need work)"
    );
}

/// Empty tables (zero rows) bypass `build_indices_on_dataset` because
/// `ensure_indices_for_branch` has `if row_count > 0 { build_indices(...) }`.
/// The `needs_index_work_*` helpers must match this — pinning an empty
/// table means recovery classifies it as `NoMovement` (no commits ever
/// ran) and rolls back any sibling table's legitimate index work.
///
/// Integration verification: after a real init + ensure_indices on a
/// graph where every table is empty, the recovery sweep must complete
/// cleanly (no leftover sidecar) AND the next ensure_indices must also
/// leave no sidecar — proving the empty-table-scoping behavior lets
/// steady-state runs incur zero sidecar I/O. The
/// `count_rows == 0 → return false` short-circuit in `needs_index_work_*`
/// is what makes this work.
///
/// A stronger assertion that captured the sidecar mid-flight and verified
/// the persisted JSON omits empty tables would require bypassing
/// `load_jsonl` (which auto-builds indices via
/// `prepare_updates_for_commit`); pinning that with a unit test on the
/// helpers directly would require bootstrapping an engine plus raw Lance
/// writes — left as a follow-up.
#[tokio::test]
async fn recovery_ensure_indices_handles_empty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    // Don't load any data — every table is empty.
    db.ensure_indices().await.unwrap();
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "ensure_indices on an all-empty graph must not leave a sidecar"
    );
    // Reopen + ensure_indices — still steady state, still no sidecar.
    drop(db);
    let mut db = Omnigraph::open(uri).await.unwrap();
    db.ensure_indices().await.unwrap();
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "second ensure_indices on an all-empty graph must also not leave a sidecar"
    );
}

/// Multi-sidecar processing must refresh the manifest snapshot between
/// sidecars: sidecar N's roll-forward writes manifest changes that
/// sidecar N+1 must observe, otherwise N+1 classifies its tables
/// against stale pins and may incorrectly run a Dataset::restore that
/// would not have run under a fresh view.
///
/// Setup:
/// - Sidecar A: kind=EnsureIndices (loose), refers to Person at
///   expected=v1, post=v2.
/// - Sidecar B: kind=EnsureIndices (loose), refers to Person at
///   expected=v2, post=v3.
/// - Lance HEAD for Person sits at v3, and v1, v2, v3 have DIFFERENT
///   fragment-id sets (each version added a real row via append_batch).
///   This means `restore_table_to_version`'s fragment-set short-circuit
///   does NOT fire under the bug path — a real `Dataset::restore`
///   actually runs there, producing HEAD=v4.
///
/// Outcome paths:
///
/// **Stale-snapshot bug** (no per-sidecar refresh):
///   Sidecar A's classifier sees pre-recovery pin=v1, expected=v1
///   matches → RolledPastExpected → RollForward to HEAD=v3. Manifest
///   advances Person v1 → v3. Sidecar B's classifier still sees the
///   STALE pin v1: lance_head=v3, manifest_pinned=v1, expected=v2.
///   Loose-match predicate `expected == manifest_pinned` fails (v2 !=
///   v1); `lance_head == manifest_pinned + 1` fails (v3 != v2) →
///   UnexpectedMultistep → RollBack. Restore Person to expected=v2,
///   creating Lance HEAD v4.
///
/// **Fresh-snapshot fix** (refresh per sidecar):
///   Sidecar A: same as above; manifest pin advances to v3.
///   Sidecar B refresh: classifier now sees pin=v3, lance_head=v3,
///   expected=v2. lance_head == manifest_pinned → NoMovement → RollBack
///   decision but the rollback loop has no eligible tables (only
///   {RolledPastExpected, UnexpectedAtP1, UnexpectedMultistep} are
///   restored), so it's a no-op rollback. Lance HEAD stays at v3.
///
/// **Differentiating assertion**: post-recovery Lance HEAD for Person
/// must be == v3 (no restore happened). The stale-snapshot bug would
/// have advanced HEAD to v4 via Dataset::restore.
///
/// Note: the audit row for sidecar B is "RolledBack" in the fix path
/// because the all-or-nothing decision sees NoMovement. Overlapping-
/// sidecar scenarios where one writer's HEAD-chained work absorbs the
/// other's are rare in practice — per-(table, branch) writer
/// serialization prevents them in steady state — but the recovery
/// sweep handles them safely without forward-progress drift.
#[tokio::test]
async fn recovery_multi_sidecar_requires_fresh_snapshot_for_correctness() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Bootstrap: load Person rows; manifest pin and Lance HEAD == some
    // baseline N.
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    drop(db);

    let person_uri = node_table_uri(uri, "Person");
    let mut ds = Dataset::open(&person_uri).await.unwrap();
    let v1 = ds.version().version;

    // Advance Lance HEAD twice via raw append_batch to mimic two
    // consecutive would-be-publishes that didn't land. Each append adds
    // a new fragment, so v1, v2, v3 have DIFFERENT fragment-id sets —
    // restore_table_to_version's fragment-set short-circuit will not
    // fire when classifier dispatches to rollback (the
    // differentiator we rely on).
    //
    // Bypassing __manifest is what `delete_where` and `append_batch`
    // both do (direct on Lance); using append_batch (instead of no-op
    // deletes) is what makes the fragment-set differ across versions.
    helpers::lance_append_inline(&mut ds, person_batch(&[("bob-id", "bob", Some(25))])).await;
    let v2 = ds.version().version;
    helpers::lance_append_inline(&mut ds, person_batch(&[("carol-id", "carol", Some(40))])).await;
    let v3 = ds.version().version;
    assert_eq!(v2, v1 + 1);
    assert_eq!(v3, v2 + 1);

    // Sidecar A: writer A's intent was pin v1 → v2.
    // Sidecar B: writer B's intent was pin v2 → v3 (depends on A landing).
    // Both EnsureIndices kind so loose-match applies.
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
        person_uri, v1, v2
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
                {{"table_key":"node:Person","table_path":"{}","expected_version":{},"post_commit_pin":{}}}
            ]
        }}"#,
        person_uri, v2, v3
    );
    write_sidecar_file(dir.path(), "01H0000000000000000000AAAA", &sidecar_a);
    write_sidecar_file(dir.path(), "01H0000000000000000000BBBB", &sidecar_b);

    // Reopen — both sidecars must process to completion (sidecar B
    // requires fresh snapshot to see sidecar A's manifest update).
    let _db = Omnigraph::open(uri).await.unwrap();

    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "both sidecars must process to completion (fresh snapshot per iteration)"
    );
    assert_eq!(
        count_recovery_audit_rows(dir.path()).await,
        2,
        "two sidecars → two audit rows"
    );

    // The "sidecars deleted + audit rows present" assertions above are
    // necessary but not sufficient — both pass even when sidecar B rolls
    // back under a stale snapshot (the bug path), because the sidecar is
    // still deleted and an audit row is still written. The differentiating
    // signal is the post-recovery Lance HEAD for Person:
    //   - Fresh-snapshot fix: sidecar B is no-op rollback (NoMovement);
    //     no Dataset::restore runs; HEAD stays at v3.
    //   - Stale-snapshot bug: sidecar B classifies as UnexpectedMultistep;
    //     restore advances HEAD to v4.
    let ds_after = Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        ds_after.version().version,
        v3,
        "Person Lance HEAD must remain v3 (no restore from stale-snapshot rollback); got {} \
         — a higher value indicates sidecar B classified UnexpectedMultistep against the \
         stale pre-recovery pin and ran a restore",
        ds_after.version().version
    );
    // Sanity: the audit kinds are diagnostic — first sidecar rolls forward
    // (RolledPastExpected → RollForward); second is no-op rollback in this
    // overlapping-sidecar scenario.
    let kinds = list_recovery_audit_kinds(dir.path()).await;
    assert_eq!(kinds.len(), 2, "expected 2 audit rows, got {:?}", kinds);
    assert!(
        matches!(kinds[0].as_str(), "RolledForward"),
        "first sidecar must roll forward; got {:?}",
        kinds
    );
}

/// A sidecar from a feature-branch writer must be classified against
/// THAT FEATURE BRANCH's manifest pin and Lance HEAD — not main's.
/// Otherwise:
///   - `snapshot.entry(table_key)` returns main's entry (or None) and
///     `manifest_pinned` is wrong.
///   - `Dataset::open(path)` returns the default ref's HEAD (main),
///     missing the feature branch's actual drift.
/// Either way, the classifier sees NoMovement → RollBack as no-op →
/// sidecar deleted while feature's drift remains. Subsequent feature
/// writers surface ExpectedVersionMismatch.
///
/// Setup:
/// - Load alice on main.
/// - Create `feature` branch.
/// - Mutate feature (insert bob) → feature's manifest pin AND Lance
///   HEAD on the feature branch advance.
/// - Capture feature's post-mutate manifest pin (v_pin) and Lance HEAD
///   (v_head).
/// - Synthesize a sidecar with `branch=Some("feature")`, pin Person at
///   `expected=v_pin, post=v_pin+1`, `table_branch=Some("feature")`.
/// - Drop the engine and append_batch on Person's feature branch to
///   advance HEAD to v_pin+1 (bypass manifest).
///
/// On reopen, recovery must:
///   - Open a per-branch coordinator at `feature` for snapshot
///     classification.
///   - Open Person's Lance dataset at the `feature` ref for HEAD read.
///   - Classify as RolledPastExpected and roll forward.
#[tokio::test]
async fn recovery_classifies_feature_branch_sidecar_against_feature_branch() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        helpers::MUTATION_QUERIES,
        "insert_person",
        &helpers::mixed_params(&[("$name", "bob")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    // Capture feature-branch state.
    let feature_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("feature"))
        .await
        .unwrap();
    let feature_entry = feature_snapshot
        .entry("node:Person")
        .expect("feature snapshot must have Person entry");
    let v_pin = feature_entry.table_version;
    let feature_branch_name = feature_entry.table_branch.clone();
    let main_pin = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .expect("main snapshot must have Person entry")
        .table_version;
    drop(db);

    // Bypass the manifest: append directly to Person's Lance HEAD on the
    // feature branch ref to advance HEAD past v_pin.
    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri, std::sync::Arc::new(lance::session::Session::default()));
    let mut ds = store
        .open_dataset_head(&person_uri, feature_branch_name.as_deref())
        .await
        .unwrap();
    helpers::lance_append_inline(&mut ds, person_batch(&[("carol-id", "carol", Some(40))])).await;
    let v_head = ds.version().version;
    assert_eq!(v_head, v_pin + 1, "append must advance HEAD by 1");

    // Synthesize a sidecar saying the writer's intent was to publish
    // feature's pin v_pin → v_pin+1. (Mutation kind = strict match.)
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000FEAT",
            "started_at": "0",
            "branch": "feature",
            "actor_id": "act-feature",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key":"node:Person",
                    "table_path":"{}",
                    "expected_version":{},
                    "post_commit_pin":{},
                    "table_branch":{}
                }}
            ]
        }}"#,
        person_uri,
        v_pin,
        v_head,
        match &feature_branch_name {
            Some(b) => format!("\"{}\"", b),
            None => "null".to_string(),
        },
    );
    write_sidecar_file(dir.path(), "01H0000000000000000000FEAT", &sidecar_json);

    // Reopen — recovery sweep must process the feature-branch sidecar
    // against feature's snapshot, not main's. With the fix, feature's
    // manifest pin advances v_pin → v_head.
    let db = Omnigraph::open(uri).await.unwrap();
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        "01H0000000000000000000FEAT",
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::branch("node:Person", "feature")
                    .expected_lance_head(v_head)
                    .expected_main_manifest_pin(main_pin),
            ],
        },
    )
    .await
    .unwrap();
}

/// Companion to the roll-forward feature-branch test: branch-axis
/// rollback. Synthesize a feature-branch sidecar that classifies as
/// rollback-eligible (UnexpectedAtP1) and assert the recovery sweep
/// processes the sidecar against the FEATURE branch (not main) and runs
/// the rollback. Without branch-aware recovery, classification would
/// happen against main's snapshot/HEAD — likely NoMovement → no-op
/// rollback that doesn't touch the actually-drifted feature ref.
#[tokio::test]
async fn recovery_rolls_back_feature_branch_sidecar_against_feature_branch() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        helpers::MUTATION_QUERIES,
        "insert_person",
        &helpers::mixed_params(&[("$name", "bob")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    let feature_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("feature"))
        .await
        .unwrap();
    let feature_entry = feature_snapshot
        .entry("node:Person")
        .expect("feature snapshot must have Person entry");
    let v_pin = feature_entry.table_version;
    let feature_branch_name = feature_entry.table_branch.clone();
    let main_pin = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .expect("main snapshot must have Person entry")
        .table_version;
    drop(db);

    // Bypass the manifest: append on the feature ref to advance HEAD past
    // the manifest pin.
    let person_uri = node_table_uri(uri, "Person");
    let store = TableStore::new(uri, std::sync::Arc::new(lance::session::Session::default()));
    let mut ds = store
        .open_dataset_head(&person_uri, feature_branch_name.as_deref())
        .await
        .unwrap();
    helpers::lance_append_inline(&mut ds, person_batch(&[("dave-id", "dave", Some(50))])).await;
    let v_head = ds.version().version;
    assert_eq!(v_head, v_pin + 1);

    // Sidecar with bogus expected (mismatch) AND post_commit_pin == v_head.
    // Strict Mutation classifier sees lance_head == manifest_pinned + 1
    // but expected != manifest_pinned → UnexpectedAtP1 → RollBack.
    let bogus_expected = v_pin.saturating_sub(1);
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000FRB1",
            "started_at": "0",
            "branch": "feature",
            "actor_id": "act-feature-rb",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key":"node:Person",
                    "table_path":"{}",
                    "expected_version":{},
                    "post_commit_pin":{},
                    "table_branch":{}
                }}
            ]
        }}"#,
        person_uri,
        bogus_expected,
        v_head,
        match &feature_branch_name {
            Some(b) => format!("\"{}\"", b),
            None => "null".to_string(),
        },
    );
    write_sidecar_file(dir.path(), "01H0000000000000000000FRB1", &sidecar_json);

    // Reopen with full sweep — RollBack is allowed at open time.
    let db = Omnigraph::open(uri).await.unwrap();
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        "01H0000000000000000000FRB1",
        RecoveryExpectation::RolledBack {
            tables: vec![
                TableExpectation::branch("node:Person", "feature")
                    .expected_main_manifest_pin(main_pin),
            ],
        },
    )
    .await
    .unwrap();

    // Lance HEAD on the feature ref must have advanced (real restore ran).
    let post = store
        .open_dataset_head(&person_uri, feature_branch_name.as_deref())
        .await
        .unwrap();
    assert!(
        post.version().version > v_head,
        "real restore must have appended a commit on feature; v_head={}, post={}",
        v_head,
        post.version().version,
    );

    let db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        2,
        "feature branch must still expose the manifest-pinned rows after rollback"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        1,
        "feature rollback must not move main"
    );
}

/// `OpenMode::ReadOnly` must NOT run `recover_schema_state_files`,
/// which can delete or rename schema-staging files. Read-only consumers
/// may run with read-only object-store credentials, and silent open-time
/// mutations violate the contract.
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
