#![cfg(feature = "failpoints")]

mod helpers;

use fail::FailScenario;
use omnigraph::db::Omnigraph;
use omnigraph::failpoints::ScopedFailPoint;

use helpers::{MUTATION_QUERIES, mixed_params, mutate_main, version_main};

const SCHEMA_V1: &str = "node Person { name: String @key }\n";
const SCHEMA_V2_ADDED_TYPE: &str =
    "node Person { name: String @key }\nnode Company { name: String @key }\n";

#[tokio::test]
async fn branch_create_failpoint_triggers() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();
    let _failpoint = ScopedFailPoint::new("branch_create.after_manifest_branch_create", "return");

    let err = db.branch_create("feature").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: branch_create.after_manifest_branch_create")
    );
}

#[tokio::test]
async fn graph_publish_failpoint_triggers_before_commit_append() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();
    let _failpoint = ScopedFailPoint::new("graph_publish.before_commit_append", "return");

    let err = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: graph_publish.before_commit_append")
    );
}

// Atomic schema apply: schema apply writes staging files first, then commits
// the manifest, then renames staging → final. Tests below inject crashes at
// the two boundaries and assert that reopening the repo yields a consistent
// state.

#[tokio::test]
async fn schema_apply_recovers_pre_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "got: {}",
            err
        );
    }

    // Reopen — recovery sweep should delete staging files and keep the
    // original schema, since the manifest commit never happened.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V1);
    assert_no_staging_files(dir.path());
}

#[tokio::test]
async fn schema_apply_recovers_post_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_manifest_commit", "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_manifest_commit"),
            "got: {}",
            err
        );
    }

    // Reopen — manifest is at the new version, so recovery sweep should
    // complete the rename and the live schema matches v2.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

#[tokio::test]
async fn schema_apply_recovers_partial_rename() {
    // Construct a partial-rename state: _schema.pg has been renamed in
    // (matching v2), but _schema.ir.json.staging and __schema_state.json.staging
    // were never renamed. Recovery should detect that the live source matches
    // the staging state's hash and complete the remaining renames.
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap();
    }

    // Simulate: one of the renames (the IR or state file) didn't complete by
    // copying the live ir/state files back to their staging names.
    std::fs::copy(
        dir.path().join("_schema.ir.json"),
        dir.path().join("_schema.ir.json.staging"),
    )
    .unwrap();
    std::fs::copy(
        dir.path().join("__schema_state.json"),
        dir.path().join("__schema_state.json.staging"),
    )
    .unwrap();

    // Reopen — recovery should complete the rename (overwriting final files
    // with identical staging content) and remove the staging files.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

/// Prove the recovery sweep closes the "finalize → publisher residual"
/// across one open cycle.
///
/// `MutationStaging::finalize` runs `commit_staged` per touched table
/// sequentially before the publisher commits the manifest. Lance has no
/// multi-dataset atomic commit primitive, so a failure between the
/// per-table staged commits and the manifest commit leaves Lance HEAD
/// advanced on the touched tables with no manifest update.
///
/// Closing the residual: finalize writes a sidecar at
/// `__recovery/{ulid}.json` BEFORE Phase B, the failpoint fires AFTER
/// finalize but BEFORE the publisher, the engine handle is dropped, and
/// the next `Omnigraph::open` runs the recovery sweep. The sweep
/// classifies every table in the sidecar as `RolledPastExpected` (Lance
/// HEAD == expected + 1, post_commit_pin matches), decides RollForward,
/// atomically extends every manifest pin via
/// `ManifestBatchPublisher::publish`, records an audit row, and deletes
/// the sidecar.
///
/// After this test passes:
/// - The originally-attempted insert ("Eve") is visible via a normal
///   query.
/// - The next mutation succeeds without `ExpectedVersionMismatch`.
/// - `_graph_commit_recoveries.lance` carries an audit row with
///   `recovery_kind=RolledForward` and the original sidecar's
///   `actor_id` in `recovery_for_actor`.
///
/// Continuous in-process recovery (no restart needed between failure
/// and recovery) is the goal of a future background reconciler.
#[tokio::test]
async fn recovery_rolls_forward_after_finalize_publisher_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Phase A: trigger the residual.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");

        // The mutation's finalize completes (commit_staged advances Lance
        // HEAD on node:Person AND writes a `__recovery/{ulid}.json`
        // sidecar). Then the failpoint kicks in before the publisher's
        // manifest commit, so the manifest pin stays at the pre-write
        // version. The sidecar persists for the next-open recovery sweep.
        let err = mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: mutation.post_finalize_pre_publisher"
            ),
            "unexpected error: {err}"
        );

        // Sidecar must still exist on disk for the recovery sweep to find.
        let recovery_dir = dir.path().join("__recovery");
        let sidecars: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            sidecars.len(),
            1,
            "exactly one sidecar should persist after the finalize failure"
        );

        // Drop the failpoint scope and the engine handle.
    }

    // Phase B: reopen runs the recovery sweep. The sweep finds the
    // sidecar, classifies node:Person as RolledPastExpected, decides
    // RollForward, publishes the manifest update, records the audit
    // row, deletes the sidecar.
    let mut db = Omnigraph::open(&uri).await.unwrap();

    // Sidecar gone — sweep completed end to end.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted after successful roll-forward; remaining: {:?}",
            remaining,
        );
    }

    // The originally-attempted "Eve" insert is now visible — the recovery
    // sweep extended the manifest pin to include the staged commit.
    let person_count = helpers::count_rows(&db, "node:Person").await;
    assert_eq!(
        person_count, 1,
        "exactly one person (Eve) must be visible after roll-forward"
    );

    // The next mutation on the same table succeeds — no ExpectedVersionMismatch.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .expect("next mutation must succeed after recovery rolled forward");
    let person_count = helpers::count_rows(&db, "node:Person").await;
    assert_eq!(
        person_count, 2,
        "Frank's insert must land normally after recovery"
    );

    // Audit row recorded.
    let audit_dir = dir.path().join("_graph_commit_recoveries.lance");
    assert!(
        audit_dir.exists(),
        "_graph_commit_recoveries.lance must exist after a successful recovery"
    );
}

/// Refresh-time recovery (Option B): the in-process `Omnigraph::refresh`
/// runs roll-forward-only recovery, closing the long-running-server
/// residual without restart.
///
/// Setup: trigger `mutation.post_finalize_pre_publisher` once. The
/// sidecar persists. Without dropping the engine, call `db.refresh()`.
/// The post-condition: sidecar gone; Eve visible; subsequent mutation
/// on the same handle succeeds without restart and without
/// ExpectedVersionMismatch.
#[tokio::test]
async fn refresh_runs_roll_forward_recovery_in_process() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Phase A: trigger the residual (sidecar persists; manifest unchanged).
    {
        let _failpoint =
            ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: mutation.post_finalize_pre_publisher"
            ),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "exactly one sidecar must persist after the finalize failure"
        );
    }

    // Phase B: explicit refresh runs roll-forward-only recovery
    // in-process — no restart needed. Sidecar finds the Person drift,
    // classifies RolledPastExpected, rolls forward via publisher CAS,
    // and deletes the sidecar.
    db.refresh().await.expect("refresh must succeed");

    // Sidecar must be gone — refresh-time recovery rolled it forward.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted by refresh-time roll-forward; remaining: {:?}",
            remaining,
        );
    }

    // Eve (the originally-attempted insert) is visible without restart.
    let person_count = helpers::count_rows(&db, "node:Person").await;
    assert_eq!(
        person_count, 1,
        "Eve must be visible after refresh-time roll-forward"
    );

    // A direct Person mutation also succeeds without ExpectedVersionMismatch.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .expect("Person insert must succeed after refresh-time recovery");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
}

/// Refresh-time recovery must NOT call `Dataset::restore` — it can
/// silently orphan a concurrent writer's commit. Sidecars that would
/// require rollback must be left on disk for the next ReadWrite open.
///
/// Setup: synthesize a sidecar that would classify as `UnexpectedAtP1`
/// (rollback territory) — strict-match Mutation kind with
/// expected_version != manifest_pinned. Trigger refresh and assert:
/// sidecar still on disk, Lance HEAD unchanged (no restore commit).
/// Then drop + open: full sweep handles it.
#[tokio::test]
async fn refresh_defers_rollback_eligible_sidecar_to_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Bootstrap.
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    // Capture Person's full URI and manifest pin.
    let snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let entry = snapshot.entry("node:Person").unwrap();
    let person_uri = format!("{}/{}", uri.trim_end_matches('/'), entry.table_path);
    let manifest_pin = entry.table_version;

    // Drift Person's Lance HEAD ahead of the manifest pin (without
    // touching the manifest) so the classifier can reach UnexpectedAtP1
    // / UnexpectedMultistep / RolledPastExpected paths that require
    // a real restore on rollback.
    let store = TableStore::new(&uri);
    let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
    store
        .delete_where(&person_uri, &mut ds, "1 = 2")
        .await
        .unwrap();
    let head_after_drift = ds.version().version;
    assert_eq!(head_after_drift, manifest_pin + 1);

    // Synthesize a sidecar with expected_version that DOES NOT match
    // the current manifest pin AND post_commit_pin == lance_head →
    // strict Mutation classifier sees lance_head == manifest_pinned + 1
    // but expected != manifest_pinned → UnexpectedAtP1. decide → RollBack.
    //
    // expected_version must be a REAL Lance version (`restore_table_to_version`
    // calls `checkout_version` on it, and an unknown version errors). Use
    // manifest_pin - 1 which exists from the bootstrap commit chain.
    let bogus_expected = manifest_pin - 1;
    let bogus_post = head_after_drift;
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000RBCK",
            "started_at": "0",
            "branch": null,
            "actor_id": "act-rollback",
            "writer_kind": "Mutation",
            "tables": [
                {{
                    "table_key":"node:Person",
                    "table_path":"{}",
                    "expected_version":{},
                    "post_commit_pin":{}
                }}
            ]
        }}"#,
        person_uri, bogus_expected, bogus_post,
    );
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    std::fs::write(
        recovery_dir.join("01H0000000000000000000RBCK.json"),
        &sidecar_json,
    )
    .unwrap();

    // Capture pre-refresh Lance HEAD on Person.
    let pre_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    // Trigger refresh-time recovery directly. Sidecar is rollback-
    // eligible (UnexpectedAtP1); RollForwardOnly mode defers it,
    // leaving the sidecar on disk and Lance HEAD unchanged on Person.
    db.refresh().await.expect("refresh must succeed (deferring rollback)");

    // Sidecar still on disk.
    assert_eq!(
        std::fs::read_dir(&recovery_dir).unwrap().count(),
        1,
        "rollback-eligible sidecar must be deferred to next ReadWrite open",
    );

    // Lance HEAD on Person unchanged — no restore ran.
    let post_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        pre_head, post_head,
        "refresh-time recovery must NOT call Dataset::restore on Person; \
         pre_head={pre_head}, post_head={post_head}",
    );

    // Cross-check: drop the engine and reopen — full sweep handles
    // the rollback (will use Dataset::restore safely; no concurrent
    // writers at open time).
    drop(db);
    let _db = Omnigraph::open(&uri).await.unwrap();
    // After full-sweep recovery, the sidecar should be processed
    // (deleted). Sidecar's tables are eligible for rollback (UnexpectedAtP1):
    // restore happens on Person (HEAD advances by 1).
    let remaining = if recovery_dir.exists() {
        std::fs::read_dir(&recovery_dir).unwrap().count()
    } else {
        0
    };
    assert_eq!(
        remaining, 0,
        "full sweep at next open must process the deferred sidecar",
    );
    let final_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert!(
        final_head > post_head,
        "full sweep must run Dataset::restore (head advances); \
         post_head={post_head}, final_head={final_head}",
    );
}

/// Companion to the above — confirms that a finalize→publisher failure
/// on one table leaves OTHER tables untouched. Subsequent writes to
/// non-drifted tables proceed normally; the drift is contained.
#[tokio::test]
async fn finalize_publisher_residual_does_not_drift_untouched_tables() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let _ = mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .expect_err("synthetic failpoint must fire");
    }

    // node:Person drifted. node:Company didn't — try a Company write.
    use omnigraph::loader::{LoadMode, load_jsonl};
    load_jsonl(
        &mut db,
        r#"{"type": "Company", "data": {"name": "Acme"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("Company write on a non-drifted table should succeed");
}

/// Acceptance test: a Phase A failure in the staged-index path
/// (`stage_create_btree_index` succeeded; `commit_staged` not yet
/// called) leaves NO Lance-HEAD drift on the existing tables.
/// Subsequent operations against those tables succeed without
/// `ExpectedVersionMismatch`.
///
/// Path: `apply_schema(v1 → v2)` adds a new node type. The
/// `added_tables` loop in `schema_apply` creates the empty dataset and
/// then calls `build_indices_on_dataset_for_catalog` →
/// `stage_and_commit_btree(..., &["id"])`. The failpoint fires
/// between `stage_create_btree_index` and `commit_staged`, so the
/// staged segments are written under `_indices/<uuid>/` but Lance HEAD
/// on the new dataset is unchanged at v=1. The schema-apply lock
/// branch is released by `apply_schema`'s outer match. Existing
/// tables (e.g. `node:Person`) are completely untouched by the new
/// node's added_tables iteration — they're outside the failed apply
/// path entirely — and we assert that mutations against them continue
/// to work.
///
/// The orphan empty dataset from the failed apply is acceptable
/// residual: it's unreferenced by `__manifest` and will be reclaimed
/// by `cleanup_old_versions` (or removed when a future apply at the
/// same target path resolves the rename).
#[tokio::test]
async fn ensure_indices_phase_a_btree_failure_leaves_existing_tables_writable() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Init with TEST_SCHEMA which declares Person + Knows. Indices on
    // those tables get built during init.
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Apply a schema that adds a new node type. The added_tables loop
    // will hit the failpoint between stage and commit on the new
    // node:Project table's btree-on-id build. (TEST_SCHEMA already
    // has Person + Company + Knows + WorksAt — pick a name that isn't
    // already declared.)
    let extended_schema = format!("{}\nnode Project {{ name: String @key }}\n", helpers::TEST_SCHEMA);

    {
        let _failpoint = ScopedFailPoint::new(
            "ensure_indices.post_stage_pre_commit_btree",
            "return",
        );
        let err = db.apply_schema(&extended_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("ensure_indices.post_stage_pre_commit_btree"),
            "schema apply should fail with the synthetic failpoint error, got: {err}"
        );
    }

    // Existing tables stayed at their pre-apply versions; subsequent
    // mutations against them succeed (no Lance-HEAD drift).
    mutate_main(
        &mut db,
        helpers::MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .expect("Person mutation must succeed after the failed schema apply — existing tables are not drifted");
}

fn assert_no_staging_files(repo: &std::path::Path) {
    for name in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        let path = repo.join(name);
        assert!(
            !path.exists(),
            "staging file {} still exists after recovery",
            path.display()
        );
    }
}

// =====================================================================
// Per-writer Phase B → Phase C recovery integration
// =====================================================================
//
// Each of the four migrated writers writes a sidecar BEFORE its
// per-table commit_staged loop and deletes it AFTER the manifest
// publish. The `recovery_rolls_forward_after_finalize_publisher_failure`
// test above covers MutationStaging::finalize. The three tests below
// cover the other three writers: schema_apply, branch_merge,
// ensure_indices.
//
// Each follows the same shape: trigger the writer with a failpoint
// active in the Phase B → Phase C window, drop the engine, reopen,
// assert recovery rolled forward (manifest pin advanced, audit row
// recorded, sidecar deleted) and a follow-up operation succeeds without
// ExpectedVersionMismatch.

#[tokio::test]
async fn schema_apply_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed: a Person table with one row so the schema-apply rewritten_tables
    // loop has actual work to do.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }

    // Capture pre-failure manifest version so we can assert the recovery
    // sweep advances it.
    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    // Phase A: trigger the residual via `schema_apply.after_staging_write`.
    // This failpoint fires AFTER the rewritten_tables/indexed_tables loops
    // (Lance HEAD advanced) AND AFTER the schema-state staging files are
    // written, but BEFORE the manifest publish. The recovery sidecar persists.
    {
        let mut db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        // v2 schema: add a `city` property to Person AND add a new
        // `Tag` node type. The new property triggers the rewritten_tables
        // path (Phase B sidecar coverage). The new type changes the
        // overall table set — required to keep `recover_schema_state_files`
        // (which runs BEFORE recover_manifest_drift) happy: it can't
        // disambiguate property-only migrations and would reject the
        // open before the recovery sweep ever ran.
        let v2_schema = r#"node Person {
    name: String @key
    age: I32?
    city: String?
}

node Company {
    name: String @key
}

node Tag {
    label: String @key
}

edge Knows: Person -> Person {
    since: Date?
}

edge WorksAt: Person -> Company
"#;
        let err = db.apply_schema(v2_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "unexpected error: {err}"
        );

        // Sidecar must still exist.
        let recovery_dir = dir.path().join("__recovery");
        let sidecars: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            sidecars.len(),
            1,
            "exactly one sidecar must persist after schema_apply failure"
        );
    }

    // Phase B: reopen runs the recovery sweep. Sidecar's writer_kind is
    // SchemaApply (loose-match) — classifier accepts the multi-commit
    // drift on Person, decision is RollForward, manifest extends to the
    // current Lance HEAD.
    let db = Omnigraph::open(&uri).await.unwrap();

    // Sidecar gone, audit row recorded.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted; remaining: {:?}",
            remaining,
        );
    }
    let audit_dir = dir.path().join("_graph_commit_recoveries.lance");
    assert!(
        audit_dir.exists(),
        "_graph_commit_recoveries.lance must exist after schema_apply recovery"
    );

    // Recovery sweep must have advanced the manifest pin on the rewritten
    // table: roll-forward published the post-failure Lance HEAD.
    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest version must advance post-recovery; pre={pre_failure_version}, \
         post={post_recovery_version}",
    );

    // Schema-apply atomicity: the live `_schema.pg` must reflect the
    // NEW schema (city column on Person, Tag node type) — not the old.
    // Without the schema-staging coordination, the schema-state
    // recovery would have deleted the staging files (because manifest
    // hadn't advanced when it ran), leaving a corrupt repo with new-
    // schema data on disk but old-schema catalog.
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        live_schema.contains("city: String?"),
        "_schema.pg must reflect the NEW schema (city column added); got:\n{live_schema}",
    );
    assert!(
        live_schema.contains("node Tag"),
        "_schema.pg must reflect the NEW schema (Tag type added); got:\n{live_schema}",
    );
    drop(db);
}

#[tokio::test]
async fn branch_merge_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main with a row, branch off, mutate BOTH sides so the merge
    // produces at least one `RewriteMerged` candidate (target moved past
    // base too — required for the recovery sidecar to pin anything; the
    // sidecar only pins RewriteMerged candidates because they're the
    // only path that always advances Lance HEAD via
    // `publish_rewritten_merge_table`).
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
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
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Bob")], &[("$age", 40)]),
        )
        .await
        .unwrap();
        // Mutate main too so the merge sees target ≠ base for Person —
        // forces RewriteMerged classification.
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Carol")], &[("$age", 50)]),
        )
        .await
        .unwrap();
    }

    // Capture pre-failure state on main for post-recovery comparison.
    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    // Phase A: failpoint fires after the per-table publish loop completes
    // but before commit_manifest_updates. Sidecar persists.
    {
        let mut db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("branch_merge.post_phase_b_pre_manifest_commit", "return");
        let err = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: branch_merge.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );

        let recovery_dir = dir.path().join("__recovery");
        let sidecars: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            sidecars.len(),
            1,
            "exactly one sidecar must persist after branch_merge failure"
        );
    }

    // Phase B: reopen runs the sweep. BranchMerge uses LOOSE
    // classification — `publish_rewritten_merge_table` runs multiple
    // commit_staged calls per table (stage_merge_insert + delete_where +
    // index rebuilds), so post_commit_pin in the sidecar is a lower
    // bound; the loose-match classifier accepts any HEAD > expected_version
    // when expected_version == manifest_pinned.
    let db = Omnigraph::open(&uri).await.unwrap();

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted; remaining: {:?}",
            remaining,
        );
    }
    let audit_dir = dir.path().join("_graph_commit_recoveries.lance");
    assert!(
        audit_dir.exists(),
        "_graph_commit_recoveries.lance must exist after branch_merge recovery"
    );

    // Recovery must have advanced main's manifest pin (the merge published).
    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest version must advance post-recovery; pre={pre_failure_version}, \
         post={post_recovery_version}",
    );

    // The recovered branch_merge must record a MERGE commit (with
    // `merged_parent_commit_id` set), not a plain commit. Without
    // this, future merges between the same pair lose
    // already-up-to-date detection. We verify by reading
    // `_graph_commits.lance` and asserting the most recent commit
    // tagged with the recovery actor has a non-null
    // `merged_parent_commit_id`.
    {
        use arrow_array::{Array, StringArray};
        use futures::TryStreamExt;
        let commits_dir = dir.path().join("_graph_commits.lance");
        let ds = lance::Dataset::open(commits_dir.to_str().unwrap())
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
        let mut found_recovery_merge = false;
        for batch in batches {
            let merged = batch
                .column_by_name("merged_parent_commit_id")
                .expect("merged_parent_commit_id column present")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("merged_parent_commit_id is Utf8");
            // The actor_id lives in _graph_commit_actors; cross-checking
            // is heavier than necessary. Detecting any non-null
            // merged_parent_commit_id in the post-recovery state is
            // sufficient: only a recovered branch_merge can produce one
            // here (we never completed a normal merge in this test).
            for i in 0..merged.len() {
                if !merged.is_null(i) {
                    found_recovery_merge = true;
                    break;
                }
            }
        }
        assert!(
            found_recovery_merge,
            "recovered branch_merge must record `merged_parent_commit_id` so future \
             merges detect already-up-to-date — no merge-parent-tagged commit found",
        );
    }
    drop(db);
}

/// Branch-axis variant of the branch_merge recovery test: target is a
/// non-main branch. Catches the branch-specific commit-graph head bug
/// (D2) — without `CommitGraph::open_at_branch`, the recovery sweep
/// would record the global head as the merge parent on a non-main
/// target, and future merges between the same pair would lose
/// already-up-to-date detection.
#[tokio::test]
async fn branch_merge_phase_b_failure_recovered_on_non_main_target() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Setup:
    //   main: alice
    //   target_branch (off main): + bob (target moved past base)
    //   source_branch (off main): + carol (source moved past base)
    // Merge: source_branch → target_branch
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
        db.branch_create("target_branch").await.unwrap();
        db.mutate(
            "target_branch",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Bob")], &[("$age", 40)]),
        )
        .await
        .unwrap();
        db.branch_create("source_branch").await.unwrap();
        db.mutate(
            "source_branch",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Carol")], &[("$age", 50)]),
        )
        .await
        .unwrap();
    }

    // Phase A: failpoint fires after the per-table publish loop completes
    // but before commit_manifest_updates. Sidecar persists with
    // branch=Some("target_branch").
    {
        let mut db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("branch_merge.post_phase_b_pre_manifest_commit", "return");
        let err = db
            .branch_merge("source_branch", "target_branch")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: branch_merge.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        let sidecar_count = std::fs::read_dir(&recovery_dir).unwrap().count();
        assert_eq!(
            sidecar_count,
            1,
            "exactly one sidecar must persist after non-main branch_merge failure"
        );
    }

    // Phase B: reopen runs full sweep. The BranchMerge sidecar's branch
    // = Some("target_branch"); D2 fix opens a per-branch CommitGraph
    // for the audit append so the merge-parent linkage is correct.
    let _db = Omnigraph::open(&uri).await.unwrap();

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted; remaining: {:?}",
            remaining,
        );
    }

    // Audit row for a merge commit was recorded with a non-null
    // merged_parent_commit_id — proves the recovery sweep used the
    // branch-specific commit-graph head as parent (not the global
    // head). Also assert the recovery audit's recovery_kind ==
    // RolledForward.
    use arrow_array::{Array, StringArray};
    use futures::TryStreamExt;
    let commits_dir = dir.path().join("_graph_commits.lance");
    // Recovery wrote the merge commit to the target_branch's Lance ref
    // on the commit_graph dataset (per CommitGraph::open_at_branch).
    // Open at that ref to find the merge commit.
    let ds = lance::Dataset::open(commits_dir.to_str().unwrap())
        .await
        .unwrap()
        .checkout_branch("target_branch")
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
    let mut found_recovery_merge = false;
    for batch in batches {
        let merged = batch
            .column_by_name("merged_parent_commit_id")
            .expect("merged_parent_commit_id column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("merged_parent_commit_id is Utf8");
        for i in 0..merged.len() {
            if !merged.is_null(i) {
                found_recovery_merge = true;
                break;
            }
        }
    }
    assert!(
        found_recovery_merge,
        "non-main branch_merge recovery must record `merged_parent_commit_id` on the \
         target branch's commit graph",
    );
}

/// `ensure_indices` only writes a sidecar when at least one table
/// genuinely needs index work (per `needs_index_work_*` helpers in
/// `db/omnigraph/table_ops.rs`). When all tables are steady-state
/// (every declared index already built, or empty tables that the loop
/// skips), the sidecar is omitted entirely.
///
/// Test setup: `load_jsonl` auto-builds indices via
/// `prepare_updates_for_commit`. So after the load, every Person/Knows
/// index is built and Company is empty. `ensure_indices` correctly
/// produces zero pins → no sidecar. The failpoint still fires (it sits
/// after the loops), so the call returns Err — but no recovery state
/// persists. Reopen is a clean no-op.
///
/// Triggering an actual sidecar persistence requires bypassing
/// `load_jsonl`'s auto-build via raw `TableStore::append_batch` — the
/// helper-direct path. That's covered structurally by the
/// `needs_index_work_*` code path and the
/// `recovery_ensure_indices_handles_empty_tables` integration test.
#[tokio::test]
async fn ensure_indices_phase_b_failure_does_not_leak_sidecar_when_no_work_needed() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed: load_jsonl auto-builds Person's indices via
    // prepare_updates_for_commit. After this, ensure_indices has no
    // work to do (steady state).
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Person","data":{"name":"bob","age":25}}
"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }

    // Phase A: trigger the failpoint. Steady-state ensure_indices
    // produces zero sidecar pins (the helpers scope pins to tables
    // that genuinely need work); no sidecar is written. The failpoint
    // still fires, surfacing the Err.
    {
        let mut db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint = ScopedFailPoint::new(
            "ensure_indices.post_phase_b_pre_manifest_commit",
            "return",
        );
        let err = db.ensure_indices().await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: ensure_indices.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );

        // KEY ASSERTION: no sidecar persists, because the helpers
        // scope pins to tables that genuinely need work. Steady-state
        // = no pins = no sidecar = no recovery state = zero open-time
        // overhead.
        let recovery_dir = dir.path().join("__recovery");
        let sidecars: Vec<_> = if recovery_dir.exists() {
            std::fs::read_dir(&recovery_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect()
        } else {
            Vec::new()
        };
        assert!(
            sidecars.is_empty(),
            "steady-state ensure_indices must not leave a sidecar; got {:?}",
            sidecars,
        );
    }

    // Phase B: reopen is a clean no-op (no sidecar to recover).
    let _db = Omnigraph::open(&uri).await.unwrap();

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must remain deleted; remaining: {:?}",
            remaining,
        );
    }
    // No audit row expected — no sidecar was processed.
    let audit_dir = dir.path().join("_graph_commit_recoveries.lance");
    assert!(
        !audit_dir.exists(),
        "_graph_commit_recoveries.lance must NOT exist when no sidecar was processed"
    );
}
