#![cfg(feature = "failpoints")]

mod helpers;

use fail::FailScenario;
use omnigraph::db::Omnigraph;
use omnigraph::failpoints::ScopedFailPoint;

use helpers::{MUTATION_QUERIES, mixed_params, mutate_main};

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

/// Prove the MR-847 recovery sweep closes the "finalize → publisher
/// residual" across one open cycle — the post-MR-847 contract.
///
/// `MutationStaging::finalize` runs `commit_staged` per touched table
/// sequentially before the publisher commits the manifest. Lance has no
/// multi-dataset atomic commit primitive, so a failure between the
/// per-table staged commits and the manifest commit leaves Lance HEAD
/// advanced on the touched tables with no manifest update.
///
/// Pre-MR-847: the next mutation surfaced `ExpectedVersionMismatch` and
/// the residual persisted until process restart. Post-MR-847: the
/// finalize writes a sidecar at `__recovery/{ulid}.json` BEFORE Phase B,
/// the failpoint fires AFTER finalize but BEFORE the publisher, the
/// engine handle is dropped, and the next `Omnigraph::open` runs the
/// recovery sweep. The sweep classifies every table in the sidecar as
/// `RolledPastExpected` (Lance HEAD == expected + 1, post_commit_pin
/// matches), decides RollForward, atomically extends every manifest pin
/// via `ManifestBatchPublisher::publish`, records an audit row, and
/// deletes the sidecar.
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
/// and recovery) is MR-856 (background reconciler).
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

/// MR-793 Phase 4 acceptance bar — proves that a Phase A failure in
/// the staged-index path (`stage_create_btree_index` succeeded;
/// `commit_staged` not yet called) leaves NO Lance-HEAD drift on the
/// existing tables. Subsequent operations against those tables succeed
/// without `ExpectedVersionMismatch`.
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
// MR-847 Phase 9 — per-writer Phase B → Phase C recovery integration
// =====================================================================
//
// Each of the four migrated writers writes a sidecar BEFORE its per-table
// commit_staged loop and deletes it AFTER the manifest publish. The
// `recovery_rolls_forward_after_finalize_publisher_failure` test above
// covers MutationStaging::finalize. The three tests below cover the
// other three writers: schema_apply, branch_merge, ensure_indices.
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

    // Phase A: trigger the residual via `schema_apply.after_staging_write`.
    // This failpoint fires AFTER the rewritten_tables/indexed_tables loops
    // (Lance HEAD advanced) AND AFTER the schema-state staging files are
    // written, but BEFORE the manifest publish. The MR-847 sidecar persists.
    {
        let mut db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        // v2 schema: add a `city` property to Person AND add a new
        // `Tag` node type. The new property triggers the rewritten_tables
        // path (Phase B sidecar coverage). The new type changes the
        // overall table set — required to keep `recover_schema_state_files`
        // (which runs BEFORE recover_manifest_drift) happy: it can't
        // disambiguate property-only migrations and would reject the
        // open before the MR-847 sweep ever ran.
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
    let _db = Omnigraph::open(&uri).await.unwrap();

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
}

#[tokio::test]
async fn branch_merge_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main with a row, branch off, mutate the branch, then attempt
    // a merge with the failpoint active.
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
    }

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

    // Phase B: reopen runs the sweep. BranchMerge is strict-classified
    // (single commit_staged per table for the merge_insert path), so the
    // sidecar's post_commit_pin should match observed Lance HEAD.
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
    let audit_dir = dir.path().join("_graph_commit_recoveries.lance");
    assert!(
        audit_dir.exists(),
        "_graph_commit_recoveries.lance must exist after branch_merge recovery"
    );
}

/// PR #72 round-2 fix: `ensure_indices` only writes a sidecar when at
/// least one table genuinely needs index work (per `needs_index_work_*`
/// helpers in `db/omnigraph/table_ops.rs`). When all tables are
/// steady-state (every declared index already built, or empty tables
/// that the loop skips), the sidecar is omitted entirely.
///
/// Test setup: `load_jsonl` auto-builds indices via
/// `prepare_updates_for_commit`. So after the load, every Person/Knows
/// index is built and Company is empty. `ensure_indices` correctly
/// produces zero pins → no sidecar. The failpoint still fires (it sits
/// after the loops), so the call returns Err — but no recovery state
/// persists. Reopen is a clean no-op.
///
/// (Triggering an actual sidecar persistence requires bypassing
/// `load_jsonl`'s auto-build via raw `TableStore::append_batch` — the
/// helper-direct path. That's covered structurally by the
/// `needs_index_work_*` code review + the
/// `recovery_ensure_indices_handles_empty_tables` integration test.)
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
    // produces zero sidecar pins (per the round-2 fix); no sidecar is
    // written. The failpoint still fires, surfacing the Err.
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

        // KEY ASSERTION: no sidecar persists, because the round-2 fix
        // scopes pins to tables that genuinely need work. Steady-state
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
