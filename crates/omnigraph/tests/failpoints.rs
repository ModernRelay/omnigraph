#![cfg(feature = "failpoints")]

mod helpers;

use fail::FailScenario;
use futures::FutureExt;
use omnigraph::db::Omnigraph;
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::failpoints::ScopedFailPoint;
use omnigraph::loader::LoadMode;
use serial_test::serial;

use helpers::recovery::{
    FollowUpMutation, RecoveryExpectation, TableExpectation, assert_post_recovery_invariants,
    branch_head_commit_id, single_sidecar_operation_id,
};
use helpers::{
    MUTATION_QUERIES, collect_column_strings, mixed_params, mutate_main, read_table, version_main,
};

const SCHEMA_V1: &str = "node Person { name: String @key }\n";
const SCHEMA_V2_ADDED_TYPE: &str =
    "node Person { name: String @key }\nnode Company { name: String @key }\n";

fn node_table_uri(root: &str, type_name: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in type_name.as_bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    format!("{}/nodes/{hash:016x}", root.trim_end_matches('/'))
}

#[tokio::test]
async fn branch_create_failpoint_triggers() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();
    let _failpoint = ScopedFailPoint::new("branch_create.after_manifest_branch_create", "return");

    let err = db.branch_create("feature").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: branch_create.after_manifest_branch_create")
    );
}

// Branch delete flips the manifest authority first, then reclaims the per-table
// forks best-effort. A failure during that reclaim (here, the
// `branch_delete.before_table_cleanup` failpoint, standing in for a transient
// object-store error) must NOT fail the call: the branch is already gone, and
// `cleanup` reconciles the stranded fork. The branch name is reusable after.
#[tokio::test]
async fn branch_delete_partial_failure_converges_via_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(&uri).await.unwrap();
    helpers::mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    let person_uri = node_table_uri(&uri, "Person");
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "precondition: the owned table fork exists before delete"
        );
    }

    // Inject a failure during per-table cleanup, AFTER the manifest authority
    // flip. branch_delete must still succeed (best-effort reclaim).
    {
        let _fp = ScopedFailPoint::new("branch_delete.before_table_cleanup", "return");
        main.branch_delete("feature").await.expect(
            "branch_delete is best-effort after the manifest flip: a cleanup-step \
             failure must not fail the call",
        );
    }

    // Authority flipped: the branch is gone.
    assert_eq!(main.branch_list().await.unwrap(), vec!["main".to_string()]);

    // The eager reclaim failed, so the orphan is stranded until cleanup.
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "failed eager reclaim should leave the orphan for cleanup to reconcile"
        );
    }

    // cleanup converges: the orphan is reclaimed.
    main.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches().await.unwrap().contains_key("feature"),
            "cleanup should reconcile the orphaned fork away"
        );
    }

    // The name is reusable after cleanup reclaims the orphan.
    main.branch_create("feature").await.unwrap();
    let mut feature2 = Omnigraph::open(&uri).await.unwrap();
    helpers::mutate_branch(
        &mut feature2,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();
}

// Reusing a branch name whose delete left an orphaned fork (before `cleanup`
// reconciles it) must SELF-HEAL on the next write — the write reclaims the
// manifest-unreferenced fork and re-forks, rather than wedging with "incomplete
// prior delete; run cleanup". (This test was the inverse before the fork-as-
// idempotent-reconcile fix; its flip is the signal the bug class is closed.)
#[tokio::test]
async fn recreate_over_orphaned_fork_self_heals_without_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(&uri).await.unwrap();
    helpers::mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    // Partial delete: leaves the Person fork orphaned (cleanup not yet run).
    {
        let _fp = ScopedFailPoint::new("branch_delete.before_table_cleanup", "return");
        main.branch_delete("feature").await.unwrap();
    }

    // Recreate the name and write to the previously-forked table WITHOUT a
    // cleanup in between. The write must self-heal the stale orphan fork.
    main.branch_create("feature").await.unwrap();
    let mut feature2 = Omnigraph::open(&uri).await.unwrap();
    helpers::mutate_branch(
        &mut feature2,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .expect("recreate-over-orphan write must self-heal, not require cleanup");

    // The recreated branch forks FRESH from main: the deleted branch's Eve is
    // gone and only the new Frank is added on top of main's seed. A count of
    // main + 2 would mean Eve resurrected from the stale fork (the bug).
    let main_people = helpers::count_rows(&main, "node:Person").await;
    let feature_people = helpers::count_rows_branch(&feature2, "feature", "node:Person").await;
    assert_eq!(
        feature_people,
        main_people + 1,
        "self-healed feature must fork fresh from main (+Frank only); \
         main={main_people}, feature={feature_people} (main+2 ⇒ Eve resurrected)"
    );
}

// The write-path orphan reclaim shares the same fresh-authority classifier as
// cleanup. If that classifier is Indeterminate (transient read on a live
// branch), the write must return a clear retryable authority-read conflict and
// leave the ref in place. It must not squeeze the ambiguity through
// ExpectedVersionMismatch with expected == actual, which lies about the cause.
#[tokio::test]
async fn recreate_over_orphaned_fork_reports_indeterminate_authority_read() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    let person_uri = node_table_uri(&uri, "Person");
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("feature", base, None).await.unwrap();
    }

    let row = r#"{"type":"Person","data":{"name":"Grace","age":37}}"#;
    {
        let _fp = ScopedFailPoint::new("classify.fresh_read", "return");
        let err = db
            .load_as("feature", None, row, LoadMode::Merge, None)
            .await
            .expect_err("indeterminate authority read must fail retryably");

        match &err {
            OmniError::Manifest(manifest) => {
                assert_eq!(manifest.kind, ManifestErrorKind::Conflict);
                assert!(
                    manifest.details.is_none(),
                    "indeterminate authority read is not an expected-version mismatch: {manifest:?}"
                );
            }
            other => panic!("expected manifest conflict, got {other:?}"),
        }
        let message = err.to_string();
        assert!(
            message.contains("could not verify")
                && message.contains("fresh manifest authority was unavailable")
                && message.contains("refresh and retry"),
            "error should name the unavailable authority read, got: {message}"
        );
        assert!(
            !message.contains("expected manifest table version"),
            "indeterminate authority must not be reported as a version mismatch: {message}"
        );

        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "ambiguous orphan status must leave the fork for a later retry"
        );
    }

    db.load_as("feature", None, row, LoadMode::Merge, None)
        .await
        .expect("when fresh authority is available, the orphan is reclaimed and write converges");
}

// cleanup is the guaranteed convergence backstop, so one table's transient
// failure must not abort the whole sweep. Inject a one-shot version-GC failure
// for a single table and assert: cleanup still succeeds, the failure is
// surfaced per-table in the returned stats, and the independent reconcile pass
// still reclaimed an orphan.
#[tokio::test]
async fn cleanup_isolates_single_table_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = helpers::init_and_load(&dir).await;

    // Forge an orphaned fork on the Person table (a reconcile target).
    let person_uri = node_table_uri(&uri, "Person");
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
    }

    // One table's version GC fails once; the sweep must isolate it.
    let _fp = ScopedFailPoint::new("cleanup.table_gc", "1*return");
    let stats = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect("a single table's GC failure must not abort cleanup");

    let errored = stats.iter().filter(|s| s.error.is_some()).count();
    assert_eq!(
        errored, 1,
        "exactly one table's GC failure should be surfaced in stats, got {errored}"
    );
    assert!(
        stats.len() >= 4,
        "every node+edge table should still appear in the stats"
    );

    // The reconcile pass is independent of the GC failure, so the orphan is gone.
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches().await.unwrap().contains_key("ghost"),
            "reconcile should reclaim the orphan despite the GC failure"
        );
    }
}

// Companion to the version-GC isolation test, exercising the OTHER cleanup
// loop: a force-delete failure while reconciling one orphaned fork must be
// isolated (logged, not propagated) so the sweep continues, and a later
// cleanup converges. This is the loop the Devin finding was about.
#[tokio::test]
async fn cleanup_isolates_reconcile_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = helpers::init_and_load(&dir).await;

    // Forge an orphaned fork the reconcile pass will try to reclaim.
    let person_uri = node_table_uri(&uri, "Person");
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
    }

    // Inject a one-shot failure into the reconcile force-delete. The sweep must
    // not abort.
    {
        let _fp = ScopedFailPoint::new("cleanup.reconcile_fork", "1*return");
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect("a reconcile force-delete failure must not abort cleanup");
    }
    // The blocked orphan is still present (the failure was isolated, not retried).
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("ghost"),
            "the orphan whose reclaim was injected-to-fail should remain"
        );
    }
    // A second cleanup with no injected failure converges.
    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches().await.unwrap().contains_key("ghost"),
            "the second cleanup should reconcile the orphan"
        );
    }
}

// The cleanup reconciler must reclaim orphaned commit-graph branches, not just
// per-table forks. A delete whose best-effort commit-graph reclaim fails leaves
// a commit-graph orphan; the next cleanup must drop it.
#[tokio::test]
async fn cleanup_reclaims_orphaned_commit_graph_branch() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = helpers::init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    // Delete, failing the commit-graph reclaim → commit-graph "feature" orphan
    // (manifest branch gone, commit-graph branch left behind).
    {
        let _fp = ScopedFailPoint::new("branch_delete.before_commit_graph_reclaim", "return");
        db.branch_delete("feature").await.unwrap();
    }

    let commits_uri = format!("{}/_graph_commits.lance", uri.trim_end_matches('/'));
    {
        let ds = lance::Dataset::open(&commits_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "precondition: the commit-graph branch should be orphaned after the failed reclaim"
        );
    }

    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();

    {
        let ds = lance::Dataset::open(&commits_uri).await.unwrap();
        assert!(
            !ds.list_branches().await.unwrap().contains_key("feature"),
            "cleanup should reclaim the orphaned commit-graph branch"
        );
    }
}

// `classify_fork_ref` returns `Indeterminate` when the fresh-authority read
// fails on a LIVE branch — and a destructive caller must SKIP, never delete, on
// that ambiguity. Here the reconciler has a genuine origin-2 orphan candidate
// (a manifest-unreferenced Person fork on the live `feature` branch), but the
// `classify.fresh_read` failpoint makes the fresh re-check fail: cleanup must
// leave the ref in place (cannot confirm it is unreferenced), then reclaim it on
// the next run once the read succeeds. This pins the Indeterminate arm and the
// don't-destroy-on-ambiguity rule end-to-end through cleanup.
#[tokio::test]
async fn reconcile_skips_fork_when_fresh_recheck_is_unavailable_then_converges() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    // Forge a manifest-unreferenced Person fork on the live `feature` branch —
    // a genuine orphan the reconciler would normally reclaim.
    let person_uri = node_table_uri(&uri, "Person");
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("feature", base, None).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "precondition: forged orphan fork present"
        );
    }

    // With the fresh re-check failing, the fork's status is Indeterminate (the
    // branch is live but unreadable) → cleanup must SKIP it, not delete.
    {
        let _fp = ScopedFailPoint::new("classify.fresh_read", "return");
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "reconcile must NOT delete a fork whose fresh re-check is inconclusive"
        );
    }

    // Read succeeds now → cleanup confirms the orphan and reclaims it (converges).
    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    {
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            !ds.list_branches().await.unwrap().contains_key("feature"),
            "next cleanup (fresh read available) must reclaim the confirmed orphan"
        );
    }
}

// A branch_delete whose best-effort commit-graph reclaim fails leaves a
// commit-graph "zombie" branch. Recreating that name must heal the zombie and
// succeed (branch_create force-deletes a stale commit-graph ref since the
// manifest branch is created fresh), instead of dying on the leftover ref.
#[tokio::test]
async fn branch_create_recreates_over_commit_graph_zombie() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();

    db.branch_create("feature").await.unwrap();
    {
        // Fail the best-effort commit-graph reclaim → commit-graph "feature"
        // zombie survives the delete (manifest authority still flips).
        let _fp = ScopedFailPoint::new("branch_delete.before_commit_graph_reclaim", "return");
        db.branch_delete("feature").await.unwrap();
    }
    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);

    db.branch_create("feature")
        .await
        .expect("branch_create should heal the zombie commit-graph branch and succeed");
    assert!(
        db.branch_list()
            .await
            .unwrap()
            .contains(&"feature".to_string())
    );
}

// branch_create is authority-then-derived: if the derived commit-graph branch
// cannot be created, the manifest branch (the authority) must be rolled back so
// the branch does not half-exist. The existing failpoint fires right after the
// manifest create, standing in for any post-authority failure.
#[tokio::test]
async fn branch_create_rolls_back_manifest_on_commit_graph_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();

    let err = {
        let _fp = ScopedFailPoint::new("branch_create.after_manifest_branch_create", "return");
        db.branch_create("feature").await.unwrap_err()
    };
    assert!(
        !db.branch_list()
            .await
            .unwrap()
            .contains(&"feature".to_string()),
        "branch_create must roll back the manifest branch when the derived \
         commit-graph branch fails, got error: {err}"
    );
}

// A fork collision must be classified by the manifest authority, not by Lance
// branch versions. When a concurrent first-write legitimately wins the fork
// race, the loser sees a version mismatch — but that is a stale snapshot, not
// an orphan, so it must be a retryable "refresh and retry", never a misleading
// "run cleanup".
//
// Ordering is made deterministic (no sleeps) via a callback at the fork point:
// `compare_exchange` lets only the FIRST arrival (writer A) record readiness and
// block until released; later arrivals (writer B) fall through. The test waits
// on the readiness flag, lets B win and commit the fork, then releases A.
static FORK_A_AT_POINT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static FORK_RELEASE_A: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[tokio::test(flavor = "multi_thread")]
async fn fork_collision_with_live_concurrent_fork_is_retryable() {
    use std::sync::atomic::Ordering::SeqCst;

    let _scenario = FailScenario::setup();
    FORK_A_AT_POINT.store(false, SeqCst);
    FORK_RELEASE_A.store(false, SeqCst);

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    // First arrival (A) records readiness and blocks until released; the rest
    // (B) fall through immediately. Bounded spin so a mistake can't hang forever.
    fail::cfg_callback("fork.before_classify", || {
        if FORK_A_AT_POINT
            .compare_exchange(false, true, SeqCst, SeqCst)
            .is_ok()
        {
            for _ in 0..2000 {
                if FORK_RELEASE_A.load(SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    })
    .unwrap();

    let uri_a = uri.clone();
    let writer_a = tokio::spawn(async move {
        let mut a = Omnigraph::open(&uri_a).await.unwrap();
        helpers::mutate_branch(
            &mut a,
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
    });

    // Wait (bounded) until A is parked at the fork point.
    for _ in 0..600 {
        if FORK_A_AT_POINT.load(SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        FORK_A_AT_POINT.load(SeqCst),
        "writer A never reached the fork point"
    );

    // B wins the fork and commits it.
    let mut b = Omnigraph::open(&uri).await.unwrap();
    helpers::mutate_branch(
        &mut b,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    // Release A; it resumes, re-reads the manifest, and sees the fork is live.
    FORK_RELEASE_A.store(true, SeqCst);
    let err = writer_a
        .await
        .unwrap()
        .expect_err("A's stale-snapshot fork should be a retryable conflict");
    fail::remove("fork.before_classify");

    let msg = err.to_string();
    assert!(
        !msg.contains("cleanup"),
        "a live concurrent fork must not be misclassified as an orphan, got: {msg}"
    );
    assert!(
        msg.contains("refresh and retry") || msg.contains("expected manifest table version"),
        "expected a retryable stale-view error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
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
// the two boundaries and assert that reopening the graph yields a consistent
// state.

#[tokio::test]
async fn schema_apply_pre_commit_crash_rolls_forward_via_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "got: {}",
            err
        );
    }

    // Reopen. With the sidecar protocol, a Phase B → Phase C crash
    // (per-table commit_staged done; manifest publish not yet) is
    // recoverable: the sidecar's `additional_registrations` carries the
    // intent to register `node:Company`, schema-state recovery promotes
    // the staging files, and the manifest-drift sweep publishes the
    // RegisterTable + Update so the manifest catches up to the schema
    // the writer already declared. The orphan-dataset-on-disk-with-no-
    // manifest-entry corruption that pre-this-protocol recoveries left
    // behind is closed.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        db.schema_source().as_str(),
        SCHEMA_V2_ADDED_TYPE,
        "live schema must reflect the rolled-forward apply (Company added)"
    );
    assert_no_staging_files(dir.path());
    // node:Company must be registered in the manifest (queryable);
    // pre-protocol recoveries left it as an orphan dataset on disk.
    let company_rows = helpers::count_rows(&db, "node:Company").await;
    assert_eq!(
        company_rows, 0,
        "node:Company must have a manifest entry post-recovery"
    );
}

#[tokio::test]
async fn schema_apply_recovers_post_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
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
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
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
        let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
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
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
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
    let operation_id;

    // Setup: trigger the residual.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");

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
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
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
        operation_id = single_sidecar_operation_id(dir.path());

        // Drop the failpoint scope and the engine handle.
    }

    // Recovery: reopen runs the recovery sweep. The sweep finds the
    // sidecar, classifies node:Person as RolledPastExpected, decides
    // RollForward, publishes the manifest update, records the audit
    // row, deletes the sidecar.
    let db = Omnigraph::open(&uri).await.unwrap();

    // The originally-attempted "Eve" insert is now visible — the recovery
    // sweep extended the manifest pin to include the staged commit.
    let person_count = helpers::count_rows(&db, "node:Person").await;
    assert_eq!(
        person_count, 1,
        "exactly one person (Eve) must be visible after roll-forward"
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![TableExpectation::main("node:Person").follow_up_mutation(
                FollowUpMutation::new(
                    "main",
                    MUTATION_QUERIES,
                    "insert_person",
                    mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
                ),
            )],
        },
    )
    .await
    .unwrap();

    let db = Omnigraph::open(&uri).await.unwrap();
    let person_count = helpers::count_rows(&db, "node:Person").await;
    assert_eq!(
        person_count, 2,
        "Frank's insert must land normally after recovery"
    );
}

#[tokio::test]
async fn inline_delete_conflict_writes_sidecar_before_rejecting() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;

    let pre_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let pre_person_pin = pre_snapshot.entry("node:Person").unwrap().table_version;
    let person_uri = node_table_uri(&uri, "Person");

    {
        let _pause_delete =
            ScopedFailPoint::new("mutation.delete_node_pre_primary_delete", "pause");
        let delete_params = helpers::params(&[("$name", "Alice")]);
        let delete = db.mutate("main", MUTATION_QUERIES, "remove_person", &delete_params);
        tokio::pin!(delete);

        let mut concurrent_update_succeeded = false;
        for _ in 0..50 {
            if delete.as_mut().now_or_never().is_some() {
                panic!("delete mutation completed before primary-delete failpoint was released");
            }
            let mut concurrent = Omnigraph::open_read_only(&uri).await.unwrap();
            if mutate_main(
                &mut concurrent,
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
            )
            .await
            .is_ok()
            {
                concurrent_update_succeeded = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            concurrent_update_succeeded,
            "concurrent update must land while delete is paused"
        );
        fail::remove("mutation.delete_node_pre_primary_delete");

        let err = delete.await.unwrap_err();
        assert!(
            err.to_string().contains("stale view of 'node:Person'")
                || err.to_string().contains("ExpectedVersionMismatch")
                || err.to_string().contains("expected version mismatch"),
            "unexpected error: {err}",
        );
    }

    let person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert!(
        person_head > pre_person_pin,
        "primary inline delete must have advanced node:Person before rejecting"
    );
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        4,
        "manifest-conflicted delete must not remove net Person rows after recovery"
    );
    assert_eq!(
        helpers::count_rows(&db, "edge:Knows").await,
        3,
        "manifest-conflicted delete must not remove net Knows rows after recovery"
    );
}

#[tokio::test]
async fn recovery_rolls_forward_load_on_feature_branch() {
    use omnigraph::loader::LoadMode;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let main_person_pin;
    let feature_parent_commit_id;

    {
        let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "BeforeLoad")], &[("$age", 40)]),
        )
        .await
        .unwrap();
        main_person_pin = db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .expect("main must have Person")
            .table_version;
        feature_parent_commit_id = branch_head_commit_id(dir.path(), "feature").await.unwrap();

        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = db
            .load(
                "feature",
                r#"{"type":"Person","data":{"name":"FeatureLoad","age":41}}
"#,
                LoadMode::Append,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        2,
        "feature branch load row must be visible after recovery"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        0,
        "feature branch load recovery must not publish the row to main"
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::branch("node:Person", "feature")
                    .expected_main_manifest_pin(main_person_pin)
                    .expected_recovery_parent_commit_id(feature_parent_commit_id)
                    .follow_up_mutation(FollowUpMutation::new(
                        "feature",
                        MUTATION_QUERIES,
                        "insert_person",
                        mixed_params(&[("$name", "AfterLoad")], &[("$age", 42)]),
                    )),
            ],
        },
    )
    .await
    .unwrap();

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        3,
        "follow-up feature mutation must succeed after load recovery"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        0,
        "follow-up feature mutation must not move main"
    );
}

#[tokio::test]
async fn recovery_rolls_forward_load_overwrite() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let parent_commit_id;

    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, helpers::TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
        parent_commit_id = branch_head_commit_id(dir.path(), "main").await.unwrap();

        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = db
            .load(
                "main",
                r#"{"type":"Person","data":{"name":"OverwriteLoad","age":41}}
"#,
                LoadMode::Overwrite,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        1,
        "overwrite row must be visible after recovery rolls the load forward"
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::main("node:Person")
                    .expected_recovery_parent_commit_id(parent_commit_id)
                    .follow_up_mutation(FollowUpMutation::new(
                        "main",
                        MUTATION_QUERIES,
                        "insert_person",
                        mixed_params(&[("$name", "AfterOverwriteLoad")], &[("$age", 42)]),
                    )),
            ],
        },
    )
    .await
    .unwrap();

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        2,
        "follow-up mutation must succeed after overwrite load recovery"
    );
}

#[tokio::test]
async fn recovery_rolls_forward_ensure_indices_on_feature_branch() {
    use lance::index::DatasetIndexExt;
    use omnigraph::loader::{LoadMode, load_jsonl};
    use omnigraph::table_store::TableStore;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let feature_parent_commit_id;
    let main_person_pin;

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
        &mixed_params(&[("$name", "BeforeEnsure")], &[("$age", 42)]),
    )
    .await
    .unwrap();

    main_person_pin = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .expect("main must have Person")
        .table_version;

    // Make the feature branch's Person table genuinely need index work
    // while keeping the manifest internally consistent. The test-only
    // publisher deliberately skips the normal index-rebuild preparation;
    // the failed writer below is still the real `ensure_indices_on`.
    let person_uri = node_table_uri(&uri, "Person");
    let store = TableStore::new(&uri);
    let mut ds = store
        .open_dataset_head(&person_uri, Some("feature"))
        .await
        .unwrap();
    ds.drop_index("id_idx").await.unwrap();
    let dropped_index_head = ds.version().version;
    db.failpoint_publish_table_head_without_index_rebuild_for_test(
        "feature",
        "node:Person",
        Some("feature"),
    )
    .await
    .unwrap();
    let feature_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("feature"))
        .await
        .unwrap();
    assert_eq!(
        feature_snapshot
            .entry("node:Person")
            .expect("feature must have Person")
            .table_version,
        dropped_index_head,
        "test setup must publish the dropped-index table head before ensure_indices runs",
    );
    feature_parent_commit_id = branch_head_commit_id(dir.path(), "feature").await.unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new("ensure_indices.post_phase_b_pre_manifest_commit", "return");
        let err = db.ensure_indices_on("feature").await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: ensure_indices.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }
    drop(db);

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        2,
        "feature should see inherited alice plus recovered branch-local row"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        1,
        "ensure_indices branch recovery must not move main"
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::branch("node:Person", "feature")
                    .expected_main_manifest_pin(main_person_pin)
                    .expected_recovery_parent_commit_id(feature_parent_commit_id)
                    .follow_up_mutation(FollowUpMutation::new(
                        "feature",
                        MUTATION_QUERIES,
                        "insert_person",
                        mixed_params(&[("$name", "AfterEnsure")], &[("$age", 44)]),
                    )),
            ],
        },
    )
    .await
    .unwrap();

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        3,
        "follow-up feature mutation must succeed after ensure_indices recovery"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        1,
        "follow-up feature mutation must not move main"
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

    // Setup: trigger the residual (sidecar persists; manifest unchanged).
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
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
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "exactly one sidecar must persist after the finalize failure"
        );
    }

    // Recovery: explicit refresh runs roll-forward-only recovery
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

/// The long-lived-process contract for `load`: a Phase B → Phase C
/// failure (per-table `commit_staged` advanced Lance HEAD, manifest
/// publish did not land, sidecar persists) must not wedge subsequent
/// loads on the same engine handle. This is the server shape — `POST
/// /ingest` calls `load_as` on a shared handle with no reopen between
/// requests — so the follow-up load must heal the sidecar-covered
/// drift in-process: no restart, no explicit `refresh()`, no
/// `omnigraph repair`.
#[tokio::test]
async fn load_after_finalize_publisher_failure_heals_without_reopen() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Failed multi-table load: Person + Company + WorksAt all run
    // commit_staged (Lance HEAD advances on three tables), then the
    // publisher is wedged before the manifest commit.
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme"}
"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "exactly one sidecar must persist after the finalize failure"
        );
    }

    // Follow-up load on the SAME handle, touching the drifted tables.
    // Must succeed without manual intervention.
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Carol","age":41}}
{"type":"Company","data":{"name":"Globex"}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect(
        "a follow-up load on the same handle must heal sidecar-covered \
         drift in-process instead of demanding repair/restart",
    );

    // Both batches are visible: the first load rolled forward, the
    // second landed normally on top of it.
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 3);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 2);
    assert_eq!(helpers::count_rows(&db, "edge:WorksAt").await, 1);

    // The sidecar was consumed by the in-process roll-forward.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "sidecar must be consumed by the in-process roll-forward"
        );
    }
}

/// Phase A storage-fault contract: a sidecar PUT failure (S3 PutObject /
/// fs write, injected at `recovery.sidecar_write`) must abort the load
/// BEFORE any Lance HEAD advances — no sidecar, no drift, nothing to
/// recover — and the same handle must write normally once the fault
/// clears (a transient storage error never wedges the graph).
#[tokio::test]
async fn sidecar_write_failure_aborts_load_with_no_head_advance() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    let person_uri = node_table_uri(&uri, "Person");
    let pre_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    {
        let _failpoint = ScopedFailPoint::new("recovery.sidecar_write", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Company","data":{"name":"Acme"}}
"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: recovery.sidecar_write"),
            "unexpected error: {err}"
        );
    }

    // Phase A ordering: the sidecar write precedes the first
    // commit_staged, so the failed load left no sidecar and moved no
    // Lance HEAD — manifest and HEAD agree, nothing to recover.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "a Phase A put failure must not leave a sidecar"
        );
    }
    let post_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        pre_head, post_head,
        "a Phase A put failure must abort before any Lance HEAD advance"
    );
    let manifest_pin = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    assert_eq!(manifest_pin, post_head, "no drift after a Phase A abort");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 0);

    // Fault cleared: the same handle writes normally — no wedge, no
    // recovery required.
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Company","data":{"name":"Acme"}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("a transient sidecar put failure must not wedge later writes");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);
}

/// Real-backend coverage of the sidecar lifecycle: the same-handle heal
/// scenario on an S3-compatible store, exercising sidecar put / list /
/// delete through the S3 object-store backend instead of the
/// local filesystem backend. Skips unless `OMNIGRAPH_S3_TEST_BUCKET` is set
/// (same gate as `s3_storage.rs`); CI runs it against RustFS.
#[tokio::test]
async fn s3_load_recovers_after_publisher_failure_without_reopen() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let Some(uri) = helpers::s3_test_graph_uri("failpoints") else {
        eprintln!(
            "skipping s3_load_recovers_after_publisher_failure_without_reopen: \
             OMNIGRAPH_S3_TEST_BUCKET is not set"
        );
        return;
    };

    let _scenario = FailScenario::setup();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Failed load: commit_staged lands on S3, manifest publish does not;
    // the sidecar PUT went through the S3 adapter.
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Company","data":{"name":"Acme"}}
"#,
            LoadMode::Merge,
        )
        .await
        .err()
        .expect("finalize failpoint must fail the load");
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
    }

    // Same-handle follow-up load: the entry heal LISTs __recovery/ on
    // S3, rolls the sidecar forward, DELETEs it, and the write lands.
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("the same-handle heal must converge on an S3-backed graph");

    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);

    // Reopen cross-check: nothing left for the open-time sweep, state
    // converged (the heal consumed the sidecar on S3).
    drop(db);
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
}

/// Storage-fault contract for the recovery AUDIT write (injected at
/// `recovery.record_audit`): a failure after the roll-forward's manifest
/// publish aborts that recovery attempt loudly and keeps the sidecar;
/// re-entry detects the already-published manifest (stale-sidecar path),
/// records exactly one `RolledForward` audit row, and converges — the
/// documented retry tolerance in `record_audit`'s contract, exercised
/// end-to-end through a real injected failure.
#[tokio::test]
async fn record_audit_failure_after_roll_forward_converges_on_next_write() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Pending sidecar with real drift.
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
            LoadMode::Merge,
        )
        .await
        .err()
        .expect("finalize failpoint must fail the load");
    }

    // The next write's heal rolls forward (manifest publish lands) but
    // the audit write fails — the write must fail loudly and the sidecar
    // must survive for the retry.
    {
        let _failpoint = ScopedFailPoint::new("recovery.record_audit", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
            LoadMode::Merge,
        )
        .await
        .err()
        .expect("an audit write failure mid-heal must fail the write");
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: recovery.record_audit"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "the sidecar must survive an audit write failure so the retry can record it"
        );
    }

    // Fault cleared: the next write converges — stale-sidecar audit
    // recovery (manifest already advanced) + the write itself.
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Carol","age":41}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("recovery must converge once the audit fault clears");

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 0);
    }
    // Alice (rolled forward) + Carol (clean). Bob's write failed before
    // staging anything — the heal error aborted his load at entry.
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
    // Exactly one audit row despite two recovery attempts: the first
    // attempt's audit failed before any row landed; the retry recorded
    // the roll-forward once.
    let audit_uri = format!(
        "{}/_graph_commit_recoveries.lance",
        uri.trim_end_matches('/')
    );
    let audit_rows = lance::Dataset::open(&audit_uri)
        .await
        .expect("audit dataset exists after the retried recovery")
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(audit_rows, 1, "exactly one recovery audit row");
}

/// Storage-fault contract for the `__recovery/` LIST (S3 ListObjectsV2,
/// injected at `recovery.sidecar_list`): every consumer fails loudly —
/// the write-entry heal fails the write, the open-time sweep fails the
/// open — rather than silently skipping recovery over a pending sidecar
/// (which would be consumer tolerance of drift). Once the fault clears,
/// open recovers normally.
#[tokio::test]
async fn sidecar_list_failure_fails_write_and_open_loudly_then_clears() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Pending sidecar via the usual finalize → publisher failure.
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 1);
    }

    let _failpoint = ScopedFailPoint::new("recovery.sidecar_list", "return");

    // Write-entry heal: the list failure surfaces as the write's error —
    // no silent skip that would proceed over the pending sidecar.
    let err = load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
        LoadMode::Merge,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: recovery.sidecar_list"),
        "the write-entry heal must surface a list failure loudly; got: {err}"
    );

    // Open-time sweep: a fresh ReadWrite open fails on the same fault.
    drop(db);
    let err = Omnigraph::open(&uri)
        .await
        .err()
        .expect("open must fail while the sidecar list fault is active");
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: recovery.sidecar_list"),
        "the open-time sweep must surface a list failure loudly; got: {err}"
    );

    // Fault cleared: open recovers the pending sidecar normally.
    drop(_failpoint);
    let db = Omnigraph::open(&uri).await.unwrap();
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "open after the fault clears must recover the sidecar"
        );
    }
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
}

/// Phase D storage-fault contract: a sidecar DELETE failure (S3
/// DeleteObject, injected at `recovery.sidecar_delete`) after a
/// successful manifest publish must NOT fail the user's write — the
/// data is durable and visible. The stale sidecar it leaves behind is
/// consumed by the next write's entry heal (attributed `RolledForward`
/// audit row), not by an operator.
#[tokio::test]
async fn sidecar_delete_failure_keeps_write_success_and_next_write_heals() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new("recovery.sidecar_delete", "return");
        // The load itself must succeed: commit_staged + manifest publish
        // landed; only the Phase D cleanup failed (swallowed + logged).
        load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
            LoadMode::Merge,
        )
        .await
        .expect("a Phase D delete failure must not fail a write that already published");
        assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "the swallowed delete leaves a stale sidecar behind"
        );
    }

    // Fault cleared: the next write's entry heal consumes the stale
    // sidecar (manifest pin already caught up — the stale-sidecar
    // roll-forward audit path) and the write lands.
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("a stale sidecar from a failed Phase D delete must not block later writes");

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "the stale sidecar must be consumed by the next write's heal"
        );
    }
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
}

/// Phase A storage-fault contract for branch_merge — the multi-table
/// writer where sidecar-before-commit ordering matters most. A sidecar
/// PUT failure must abort the merge before any target-table HEAD moves;
/// retrying after the fault clears merges cleanly.
#[tokio::test]
async fn sidecar_write_failure_aborts_branch_merge_with_no_head_advance() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    db.branch_create("feature").await.unwrap();
    // Diverge BOTH sides so Person is a RewriteMerged candidate (the
    // merge path that pins a recovery sidecar; an unchanged target would
    // adopt source state without one).
    helpers::mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    helpers::mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Mallory")], &[("$age", 35)]),
    )
    .await
    .unwrap();

    let person_uri = node_table_uri(&uri, "Person");
    let pre_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    {
        let _failpoint = ScopedFailPoint::new("recovery.sidecar_write", "return");
        let err = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: recovery.sidecar_write"),
            "unexpected error: {err}"
        );
    }

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "a Phase A put failure must not leave a sidecar"
        );
    }
    let post_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        pre_head, post_head,
        "a Phase A put failure must abort the merge before any target \
         Lance HEAD advance"
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);

    // Fault cleared: the merge lands cleanly.
    db.branch_merge("feature", "main")
        .await
        .expect("a transient sidecar put failure must not wedge the merge");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 3);
}

/// Same contract as
/// `load_after_finalize_publisher_failure_heals_without_reopen`, for the
/// mutation entry point: after a failed mutation leaves a sidecar, the
/// next mutation on the same handle heals it in-process — no explicit
/// `refresh()` (which `refresh_runs_roll_forward_recovery_in_process`
/// covers), no reopen.
#[tokio::test]
async fn mutation_after_finalize_publisher_failure_heals_without_reopen() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
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
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "exactly one sidecar must persist after the finalize failure"
        );
    }

    // Follow-up mutation on the SAME handle, same table. No refresh, no
    // reopen — the write entry point heals the drift itself.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .expect(
        "a follow-up mutation on the same handle must heal sidecar-covered \
         drift in-process instead of demanding repair/restart",
    );

    // Eve rolled forward, Frank landed normally.
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);

    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "sidecar must be consumed by the in-process roll-forward"
        );
    }
}

/// Same heal contract as the load/mutation variants, for the schema
/// apply entry point: a pending roll-forward-eligible sidecar (here
/// from a failed load) must be healed in-process before the migration
/// runs, so a long-lived handle can evolve the schema without a
/// restart after a Phase B → Phase C failure.
#[tokio::test]
async fn schema_apply_after_finalize_publisher_failure_heals_without_reopen() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Company","data":{"name":"Acme"}}
"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 1);
    }

    // Additive migration on the SAME handle. Must heal the load's
    // sidecar first, then apply normally.
    let desired = format!("{}\nnode Tag {{ name: String @key }}\n", helpers::TEST_SCHEMA);
    db.apply_schema(&desired).await.expect(
        "schema apply on the same handle must heal sidecar-covered \
         drift in-process instead of failing until restart",
    );

    // The failed load rolled forward; the migration landed.
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);
    assert_eq!(helpers::count_rows(&db, "node:Tag").await, 0);

    // No sidecar remains (the load's was consumed by the heal; schema
    // apply deleted its own after publish).
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "no sidecar may remain after heal + successful schema apply"
        );
    }
}

/// Same heal contract for the branch-merge entry point: a pending
/// roll-forward-eligible sidecar on the target branch must be healed
/// (with its recovery audit row) before the merge reads its target
/// snapshot — not silently folded into the merge's publish.
#[tokio::test]
async fn branch_merge_after_finalize_publisher_failure_heals_without_reopen() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    // A feature branch with its own write, to merge back later.
    db.branch_create("feature").await.unwrap();
    helpers::mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Failed load on MAIN: Person drifts ahead of the manifest with a
    // sidecar covering it.
    {
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
        let err = load_jsonl(
            &mut db,
            r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_finalize_pre_publisher"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 1);
    }

    // Merge on the SAME handle. The entry heal must consume the load's
    // sidecar (publishing Bob with a recovery audit row) BEFORE the
    // merge captures its target snapshot.
    db.branch_merge("feature", "main").await.expect(
        "branch merge on the same handle must heal sidecar-covered \
         drift in-process instead of failing or folding it silently",
    );

    // No sidecar remains: the heal consumed the load's sidecar; the
    // merge deleted its own after publish. Without the entry heal the
    // merge's publish makes the drifted commit visible as a side effect
    // (manifest catches up to HEAD) and the stale sidecar lingers
    // until some later sweep — recovery must be attributed, not
    // incidental.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "the load's sidecar must be consumed by the entry heal, not left behind"
        );
    }

    // All three writes are visible on main: Alice (clean load), Bob
    // (rolled forward), Eve (merged).
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 3);
}

/// Discarding an orphaned-branch sidecar must be idempotent across a
/// Phase D delete failure: the audit row + commit land before the
/// sidecar delete, so a delete fault leaves the sidecar on disk with
/// the audit already written — the retry must NOT append a second
/// audit row for the same operation, only finish the delete.
#[tokio::test]
async fn orphaned_branch_discard_is_idempotent_across_delete_failure() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    helpers::mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Deferred-shape sidecar pinned to feature (head < expected ⇒
    // invariant violation ⇒ every roll-forward-only pass defers it).
    let person_uri = node_table_uri(&uri, "Person");
    let sidecar_json = format!(
        r#"{{
        "schema_version": 1,
        "operation_id": "01H000000000000000000000ID",
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
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    std::fs::write(
        recovery_dir.join("01H000000000000000000000ID.json"),
        &sidecar_json,
    )
    .unwrap();

    // Orphan the sidecar.
    db.branch_delete("feature").await.unwrap();

    // First write: the discard path writes its audit row, then the
    // sidecar delete fails (injected). The write fails loudly.
    {
        let _failpoint = ScopedFailPoint::new("recovery.sidecar_delete", "return");
        let err = load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
            LoadMode::Merge,
        )
        .await
        .err()
        .expect("a sidecar-delete fault mid-discard must fail the write");
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: recovery.sidecar_delete"),
            "unexpected error: {err}"
        );
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 1);
    }

    // Retry: must finish the delete WITHOUT a second audit row.
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
        LoadMode::Merge,
    )
    .await
    .expect("the retry must complete the orphan discard and the write");
    assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 0);
    let orphan_rows = helpers::recovery::recovery_audit_kinds(dir.path())
        .await
        .into_iter()
        .filter(|kind| kind == "OrphanedBranchDiscarded")
        .count();
    assert_eq!(
        orphan_rows, 1,
        "exactly one OrphanedBranchDiscarded audit row despite the delete-fault retry"
    );
}

/// When the commit-time drift guard cannot LIST sidecars to classify
/// the drift (transient storage fault on the guard's list, after the
/// entry heal's list succeeded), it must say so and name BOTH recovery
/// paths — not confidently route to `omnigraph repair`, which refuses
/// while a sidecar is pending. Sequenced failpoint: first list (entry
/// heal) passes, second list (the guard) fails.
#[tokio::test]
async fn drift_guard_names_both_paths_when_sidecar_list_fails() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n",
        LoadMode::Append,
    )
    .await
    .unwrap();

    // Rollback-eligible (deferred) sidecar covering main's Person drift —
    // same shape as refresh_defers_rollback_eligible_sidecar_to_next_open.
    let snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let entry = snapshot.entry("node:Person").unwrap();
    let person_uri = format!("{}/{}", uri.trim_end_matches('/'), entry.table_path);
    let manifest_pin = entry.table_version;
    let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
    helpers::lance_delete_inline(&mut ds, "1 = 2").await;
    let head_after_drift = ds.version().version;
    let sidecar_json = format!(
        r#"{{
            "schema_version": 1,
            "operation_id": "01H0000000000000000000LSTF",
            "started_at": "0",
            "branch": null,
            "actor_id": null,
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
        person_uri,
        manifest_pin - 1,
        head_after_drift,
    );
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    std::fs::write(
        recovery_dir.join("01H0000000000000000000LSTF.json"),
        &sidecar_json,
    )
    .unwrap();

    // First list (entry heal) passes and defers the sidecar; second
    // list (the guard's classification) fails.
    let _failpoint = ScopedFailPoint::new("recovery.sidecar_list", "1*off->1*return");
    let err = load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"bob\",\"age\":25}}\n",
        LoadMode::Merge,
    )
    .await
    .err()
    .expect("drift must still fail the write");
    let msg = err.to_string();
    assert!(
        msg.contains("could not classify the drift")
            && msg.contains("omnigraph repair")
            && msg.contains("reopen the graph read-write"),
        "an unclassifiable drift must name BOTH recovery paths, not \
         confidently route to repair; got: {msg}"
    );
}

/// The other half of the orphan-discard fault matrix: the audit append
/// fails AFTER the recovery commit landed. The retry (keyed on the
/// audit row, the operator-facing record) must converge to exactly one
/// audit row and a consumed sidecar. The second recovery commit the
/// retry appends is the documented not-atomic-pair-write tolerance
/// (same class as `record_audit` and the manifest→commit-graph Known
/// Gap): bounded commit-graph noise, never a lost or duplicated audit
/// record under clean failures.
#[tokio::test]
async fn orphaned_branch_discard_converges_across_audit_append_failure() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    helpers::mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Deferred-shape sidecar pinned to feature, then orphaned.
    let person_uri = node_table_uri(&uri, "Person");
    let sidecar_json = format!(
        r#"{{
        "schema_version": 1,
        "operation_id": "01H000000000000000000000AF",
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
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    std::fs::write(
        recovery_dir.join("01H000000000000000000000AF.json"),
        &sidecar_json,
    )
    .unwrap();
    db.branch_delete("feature").await.unwrap();

    // First write: the recovery commit lands, then the audit append
    // fails (injected). The write fails loudly; the sidecar survives so
    // the discard is retried with the audit still owed.
    {
        let _failpoint = ScopedFailPoint::new("recovery.orphan_discard_audit_append", "return");
        let err = load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
            LoadMode::Merge,
        )
        .await
        .err()
        .expect("an audit-append fault mid-discard must fail the write");
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: recovery.orphan_discard_audit_append"),
            "unexpected error: {err}"
        );
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            1,
            "the sidecar must survive an audit-append fault so the discard is retried"
        );
        let orphan_rows = helpers::recovery::recovery_audit_kinds(dir.path())
            .await
            .into_iter()
            .filter(|kind| kind == "OrphanedBranchDiscarded")
            .count();
        assert_eq!(orphan_rows, 0, "no audit row landed before the fault");
    }

    // Retry: converges — sidecar consumed, exactly one audit row.
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":25}}\n",
        LoadMode::Merge,
    )
    .await
    .expect("the retry must complete the orphan discard and the write");
    assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 0);
    let orphan_rows = helpers::recovery::recovery_audit_kinds(dir.path())
        .await
        .into_iter()
        .filter(|kind| kind == "OrphanedBranchDiscarded")
        .count();
    assert_eq!(
        orphan_rows, 1,
        "exactly one OrphanedBranchDiscarded audit row despite the audit-fault retry"
    );
}

/// After the write-entry heal rolls a SchemaApply sidecar forward (a
/// crashed apply on the SAME handle: staging promoted, registrations
/// published), the handle's in-memory catalog must be reloaded — disk
/// and manifest are on the new schema, and validating subsequent
/// writes against the stale catalog rejects rows of types the graph
/// already has.
#[tokio::test]
async fn load_after_schema_apply_phase_b_failure_uses_recovered_catalog() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n",
        LoadMode::Append,
    )
    .await
    .unwrap();

    // v2: a Person property (rewritten_tables work) + a new Tag type
    // (table-set change, keeps the staging disambiguator decisive).
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
    {
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        let err = db.apply_schema(v2_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "unexpected error: {err}"
        );
        let recovery_dir = dir.path().join("__recovery");
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 1);
    }

    // Same handle: a load of the NEW type. The entry heal rolls the
    // apply forward (staging promoted, manifest registers node:Tag) —
    // and the loader must then validate against the RECOVERED catalog,
    // not the stale in-memory one.
    load_jsonl(
        &mut db,
        "{\"type\":\"Tag\",\"data\":{\"label\":\"t1\"}}\n",
        LoadMode::Merge,
    )
    .await
    .expect(
        "after the heal rolls the schema apply forward, the same handle \
         must accept rows of the recovered schema's types",
    );
    assert_eq!(helpers::count_rows(&db, "node:Tag").await, 1);
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 0);
    }
}

/// A concurrent write's entry heal must NOT promote a LIVE schema
/// apply's staging files. The apply pauses just after writing its
/// staging files (sidecar on disk from Phase A, staging on disk,
/// manifest not yet committed); a load on the same handle fires the
/// heal in that window. If the heal's schema-staging reconcile runs
/// unserialized, it promotes the staging files from under the live
/// apply — putting the NEW catalog live against the OLD manifest — and
/// the resumed apply's own renames then fail on the missing sources:
/// an error (and a corrupted catalog) for an otherwise-healthy apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heal_does_not_promote_live_schema_apply_staging() {
    use omnigraph::loader::LoadMode;
    use std::sync::Arc;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let db = Arc::new(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());

    // Pause the apply right after its staging files land (its sidecar is
    // already on disk from Phase A; the manifest commit has not run).
    let failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "pause");

    let apply_db = Arc::clone(&db);
    let desired = format!("{}\nnode Tag {{ name: String @key }}\n", helpers::TEST_SCHEMA);
    let apply = tokio::spawn(async move { apply_db.apply_schema(&desired).await });

    // Wait until the apply is parked in the window: staging on disk.
    let staging_pg = dir.path().join("_schema.pg.staging");
    for _ in 0..500 {
        if staging_pg.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(staging_pg.exists(), "schema apply never reached the paused window");

    // Concurrent load on the same handle: its entry heal runs while the
    // apply is paused. The load itself may fail (schema apply in
    // progress) — what matters is what its heal does to the live apply.
    let load_db = Arc::clone(&db);
    let load = tokio::spawn(async move {
        load_db
            .load_as(
                "main",
                None,
                "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n",
                LoadMode::Merge,
                None,
            )
            .await
    });

    // Give the load's heal time to act inside the window. Broken code
    // completes the load here (its heal promoted the staging files and
    // stole the apply's commit); fixed code leaves the load blocked on
    // the schema-apply serialization key until the apply finishes.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(failpoint);

    let apply_result = apply.await.unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(30), load)
        .await
        .expect("load must complete once the apply releases its guards")
        .unwrap();
    apply_result.expect(
        "a concurrent write's heal must not promote the live schema \
         apply's staging files out from under it",
    );

    // The migration landed and nothing recovery-shaped remains.
    assert_eq!(helpers::count_rows(&db, "node:Tag").await, 0);
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(std::fs::read_dir(&recovery_dir).unwrap().count(), 0);
    }
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
    let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
    helpers::lance_delete_inline(&mut ds, "1 = 2").await;
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
    db.refresh()
        .await
        .expect("refresh must succeed (deferring rollback)");

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

    // A write attempt while the rollback-eligible sidecar is deferred:
    // the write-entry heal defers it again (roll-forward-only), and the
    // commit-time drift guard must name the actual recovery path (a
    // read-write reopen) — NOT `omnigraph repair`, which refuses while
    // a sidecar is pending.
    let err = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Grace")], &[("$age", 50)]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("a pending recovery sidecar requires rollback"),
        "drift guard must point at a read-write reopen for sidecar-covered \
         rollback-eligible drift; got: {err}"
    );

    // Cross-check: drop the engine and reopen — full sweep handles
    // the rollback (will use Dataset::restore safely; no concurrent
    // writers at open time).
    drop(db);
    let db = Omnigraph::open(&uri).await.unwrap();
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
    // Convergence: roll-back published the restored HEAD, so the manifest pin
    // tracks Lance HEAD afterward (no residual drift).
    let entry_version = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    assert_eq!(
        entry_version, final_head,
        "full-sweep roll-back must publish so manifest pin ({entry_version}) == Lance HEAD ({final_head})",
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
        let _failpoint = ScopedFailPoint::new("mutation.post_finalize_pre_publisher", "return");
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

/// Acceptance test: a stage-step failure in the staged-index path
/// (`stage_create_btree_index` succeeded; `commit_staged` not yet called)
/// leaves NO Lance-HEAD drift, so other tables stay writable.
///
/// Under iss-848 schema apply no longer builds indexes inline — the build
/// happens in the reconciler (`ensure_indices`/`optimize`) and at load. So this
/// fires the failpoint where it lives now: an `ensure_indices` build of a BTREE
/// that a prior apply declared (`@index`) but deferred. The failpoint fires
/// between `stage_create_btree_index` and `commit_staged`, so the staged
/// segment is written under `_indices/<uuid>/` but `node:Person`'s Lance HEAD is
/// unchanged. `ensure_indices` fails and its EnsureIndices sidecar pins only
/// Person at NoMovement (a clean no-op on the next open). A write to a
/// different, unpinned table (`node:Company`) is unaffected: mutations/loads run
/// a roll-forward-only heal and proceed — they do not refuse on a pending
/// sidecar the way `optimize`/`repair` do — so the write succeeds with no drift.
#[tokio::test]
async fn ensure_indices_stage_btree_failure_leaves_existing_tables_writable() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Seed a Person row — the load builds Person's id BTREE + name FTS.
    mutate_main(
        &mut db,
        helpers::MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Alice")], &[("$age", 30)]),
    )
    .await
    .expect("seed Person");

    // Add `@index` on `age`: schema apply records the intent but defers the
    // physical build (iss-848), so the BTREE on `age` is unbuilt.
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    db.apply_schema(&indexed_schema)
        .await
        .expect("adding an @index is metadata-only and succeeds");

    {
        // ensure_indices builds the deferred `age` BTREE on Person; the failpoint
        // fires between stage and commit, so Person's Lance HEAD does not move.
        let _failpoint =
            ScopedFailPoint::new("ensure_indices.post_stage_pre_commit_btree", "return");
        let err = db.ensure_indices().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("ensure_indices.post_stage_pre_commit_btree"),
            "ensure_indices should fail with the synthetic failpoint error, got: {err}"
        );
    }

    // A different, unpinned table is untouched by the failed index build.
    use omnigraph::loader::{LoadMode, load_jsonl};
    load_jsonl(
        &mut db,
        r#"{"type": "Company", "data": {"name": "Acme"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("Company write on a table untouched by the failed ensure_indices should succeed");
}

fn assert_no_staging_files(graph: &std::path::Path) {
    for name in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        let path = graph.join(name);
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
async fn schema_apply_without_schema_staging_rolls_back_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;

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

    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.before_staging_write", "return");
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
                .contains("injected failpoint triggered: schema_apply.before_staging_write"),
            "unexpected error: {err}"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }

    let db = Omnigraph::open(&uri).await.unwrap();
    // Roll-back now publishes the restored version, so the manifest version
    // advances — but to the OLD-schema content: the migration never applied
    // (asserted by count_rows + the `_schema.pg` checks below), and the sweep
    // converges (`manifest == Lance HEAD`, asserted by
    // assert_post_recovery_invariants's RolledBack arm).
    assert!(
        version_main(&db).await.unwrap() > pre_failure_version,
        "roll-back publishes the restored (old-schema) version, advancing the manifest; \
         pre={pre_failure_version}",
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        1,
        "old-schema data must remain readable after rollback"
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![TableExpectation::main("node:Person")],
        },
    )
    .await
    .unwrap();

    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        !live_schema.contains("city: String?"),
        "_schema.pg must keep the OLD schema when staging files never existed; got:\n{live_schema}",
    );
    assert!(
        !live_schema.contains("node Tag"),
        "_schema.pg must keep the OLD schema when staging files never existed; got:\n{live_schema}",
    );
}

#[tokio::test]
async fn schema_apply_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;

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

    // Setup: trigger the residual via `schema_apply.after_staging_write`.
    // This failpoint fires AFTER the rewritten_tables/indexed_tables loops
    // (Lance HEAD advanced) AND AFTER the schema-state staging files are
    // written, but BEFORE the manifest publish. The recovery sidecar persists.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
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
        operation_id = single_sidecar_operation_id(dir.path());
    }

    // Recovery: reopen runs the recovery sweep. Sidecar's writer_kind is
    // SchemaApply (loose-match) — classifier accepts the multi-commit
    // drift on Person, decision is RollForward, manifest extends to the
    // current Lance HEAD.
    let db = Omnigraph::open(&uri).await.unwrap();

    // Recovery sweep must have advanced the manifest pin on the rewritten
    // table: roll-forward published the post-failure Lance HEAD.
    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest version must advance post-recovery; pre={pre_failure_version}, \
         post={post_recovery_version}",
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![TableExpectation::main("node:Person")],
        },
    )
    .await
    .unwrap();

    // Schema-apply atomicity: the live `_schema.pg` must reflect the
    // NEW schema (city column on Person, Tag node type) — not the old.
    // Without the schema-staging coordination, the schema-state
    // recovery would have deleted the staging files (because manifest
    // hadn't advanced when it ran), leaving a corrupt graph with new-
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

    // Catalog ↔ manifest agreement: the new `node:Tag` type the schema
    // declares must have a manifest entry the engine can read against.
    // Without registrations / tombstones in the sidecar, recovery's
    // `roll_forward_all` only publishes Updates for rewritten tables;
    // added tables (Tag) end up as orphan datasets on disk with no
    // manifest entry, and the live schema declares a type the manifest
    // doesn't know about.
    let db = Omnigraph::open(&uri).await.unwrap();
    let tag_rows = helpers::count_rows(&db, "node:Tag").await;
    assert_eq!(
        tag_rows, 0,
        "node:Tag must have a manifest entry (with 0 rows) post-recovery; \
         a panic here means recovery failed to register the added table"
    );
}

/// `optimize` Phase B → Phase C residual: `compact_files` advanced the Lance
/// HEAD but the manifest publish hasn't run. The `Optimize` recovery sidecar
/// (loose-match, like SchemaApply/EnsureIndices) must roll the compacted version
/// forward on next open so the manifest tracks the Lance HEAD — and the healed
/// table must then accept a schema apply (the original bug's victim).
#[tokio::test]
#[serial(optimize)]
async fn optimize_phase_b_failure_recovered_on_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;

    // Seed: several separate Person inserts → multiple fragments, so compaction
    // has real work and advances the Lance HEAD.
    {
        let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        for (name, age) in [("alice", 30), ("bob", 31), ("carol", 32), ("dave", 33)] {
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", name)], &[("$age", age)]),
            )
            .await
            .unwrap();
        }
    }

    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    // Failpoint fires AFTER compact_files advanced the Lance HEAD but BEFORE the
    // manifest publish. The Optimize sidecar persists (only node:Person has
    // compactable fragments, so exactly one sidecar is written).
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("optimize.post_phase_b_pre_manifest_commit", "return");
        let err = db.optimize().await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: optimize.post_phase_b_pre_manifest_commit"
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
            "exactly one Optimize sidecar must persist after optimize failure"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }

    // Recovery: reopen runs the sweep. The Optimize sidecar classifies
    // RolledPastExpected (loose-match) → RollForward → manifest extends to the
    // compacted Lance HEAD.
    let db = Omnigraph::open(&uri).await.unwrap();
    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest version must advance post-recovery (compaction rolled forward); \
         pre={pre_failure_version}, post={post_recovery_version}",
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![TableExpectation::main("node:Person")],
        },
    )
    .await
    .unwrap();

    // The healed table accepts an additive schema apply — its HEAD-vs-manifest
    // precondition is satisfied because recovery published the compacted version.
    let db = Omnigraph::open(&uri).await.unwrap();
    let desired = helpers::TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    db.apply_schema(&desired)
        .await
        .expect("schema apply after optimize recovery must succeed");
}

/// Cross-process race (the prod bug): a served write advances the manifest on the
/// same table while a SEPARATE `optimize` process is paused between its compaction
/// and its manifest publish. The in-process write queue does NOT serialize across
/// processes, so optimize's equality-CAS publish (expected = its pre-compaction
/// version) finds the manifest already advanced. optimize must CONVERGE — the
/// concurrent write built on top of the compacted HEAD, so the compaction is
/// already reflected — not fail with "expected X but current Y". RED before the
/// monotonic-publish fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_survives_concurrent_insert_advancing_manifest() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        for (name, age) in [("alice", 30), ("bob", 31), ("carol", 32), ("dave", 33)] {
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", name)], &[("$age", age)]),
            )
            .await
            .unwrap();
        }
    }

    // Pause optimize AFTER compact_files advanced the Lance HEAD but BEFORE publish.
    let failpoint = ScopedFailPoint::new("optimize.post_phase_b_pre_manifest_commit", "pause");

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = Omnigraph::open(&uri_opt).await.unwrap();
        db.optimize().await
    });

    // Wait until optimize reaches the pause (its Optimize sidecar is on disk).
    assert!(
        wait_for_sidecar(dir.path()).await,
        "optimize never reached the paused publish window",
    );

    // Concurrent insert on the SAME table via a SEPARATE handle (= separate
    // in-process write queue = a different process) advances the manifest.
    {
        let db_b = Omnigraph::open(&uri).await.unwrap();
        db_b.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "eve")], &[("$age", 34)]),
        )
        .await
        .unwrap();
    }

    drop(failpoint); // release optimize
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("optimize task hung")
        .unwrap();
    result.expect("optimize must survive a concurrent same-table write (cross-process)");

    // No lost write: 4 seed + eve all present; graph remains re-optimizable.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        5,
        "concurrent insert must not be lost",
    );
    db.optimize()
        .await
        .expect("graph must remain healthy / re-optimizable");
}

/// Cross-process race: a served DELETE commits on the same table while a SEPARATE
/// `optimize` process is parked just before its compaction. Lance rebases the
/// compaction past the delete cleanly (so this surfaces as a manifest-CAS mismatch
/// at publish, not a Lance `Rewrite` conflict — the genuine `Rewrite`-vs-`Rewrite`
/// overlap is the rarer many-fragment/concurrent-compaction case, covered by the
/// shared `is_retryable_lance_conflict` retry the internal-table path already
/// exercises). optimize must converge its publish over the advanced manifest and
/// preserve the delete. RED before the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_survives_concurrent_delete_before_compaction() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        for (name, age) in [("alice", 30), ("bob", 31), ("carol", 32), ("dave", 33)] {
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", name)], &[("$age", age)]),
            )
            .await
            .unwrap();
        }
    }

    // Pause optimize BEFORE its compaction commits.
    let failpoint = ScopedFailPoint::new("optimize.before_compact", "pause");

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = Omnigraph::open(&uri_opt).await.unwrap();
        db.optimize().await
    });

    assert!(
        wait_for_sidecar(dir.path()).await,
        "optimize never reached the pre-compact pause",
    );

    // Concurrent DELETE of an existing row writes a deletion vector onto the
    // fragment optimize is about to compact → optimize's Rewrite overlap-conflicts
    // at the Lance level ("Rewrite … preempted by concurrent Delete/Update").
    {
        let db_b = Omnigraph::open(&uri).await.unwrap();
        db_b.mutate(
            "main",
            MUTATION_QUERIES,
            "remove_person",
            &mixed_params(&[("$name", "alice")], &[]),
        )
        .await
        .unwrap();
    }

    drop(failpoint); // release optimize
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("optimize task hung")
        .unwrap();
    result.expect("optimize must reopen+replan past a concurrent overlapping delete");

    // No lost write: alice's delete persisted (3 rows); graph remains re-optimizable.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        3,
        "the concurrent delete must persist (alice removed)",
    );
    db.optimize()
        .await
        .expect("graph must remain healthy / re-optimizable");
}

/// Poll until `optimize` has written its recovery sidecar (i.e. reached Phase B
/// and is about to / has compacted), signalling it is parked at its failpoint.
async fn wait_for_sidecar(root: &std::path::Path) -> bool {
    let recovery_dir = root.join("__recovery");
    for _ in 0..1000 {
        if recovery_dir.exists()
            && std::fs::read_dir(&recovery_dir)
                .map(|d| d.count() > 0)
                .unwrap_or(false)
        {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
#[serial(branch_merge_phase_b)]
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

    // Setup: failpoint fires after the per-table publish loop completes
    // but before commit_manifest_updates. Sidecar persists.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
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

    // Recovery: reopen runs the sweep. BranchMerge uses LOOSE
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

/// AdoptWithDelta recovery (the gap closure): a fast-forward merge — main has
/// NOT advanced since the branch forked, so the touched table is classified
/// `AdoptWithDelta`, not `RewriteMerged` — that fails after Phase B must still
/// recover on the next open. Before the recovery-pin closure this drifted
/// silently: the adopt path advanced Lance HEAD but was unpinned, so the sweep
/// found no sidecar and the merge was lost.
#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_adopt_with_delta_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main, branch off, mutate ONLY the branch. main stays at base, so the
    // merge is a fast-forward and Person classifies `AdoptWithDelta` (forked
    // source, target == base, non-empty delta) — NOT `RewriteMerged`.
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
        // main intentionally NOT mutated → fast-forward → AdoptWithDelta.
    }

    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    // Fail after the per-table publish loop, before commit_manifest_updates.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("branch_merge.post_phase_b_pre_manifest_commit", "return");
        let err = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: branch_merge.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );

        // The gap closure: an AdoptWithDelta merge must persist a sidecar.
        let recovery_dir = dir.path().join("__recovery");
        let sidecars: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            sidecars.len(),
            1,
            "AdoptWithDelta merge must persist exactly one recovery sidecar (the closed gap)"
        );
    }

    // Reopen → the recovery sweep rolls the AdoptWithDelta merge forward.
    let db = Omnigraph::open(&uri).await.unwrap();
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        let remaining: Vec<_> = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "sidecar must be deleted post-recovery; remaining: {remaining:?}"
        );
    }

    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest must advance post-recovery; pre={pre_failure_version} post={post_recovery_version}"
    );
    let names = collect_column_strings(&read_table(&db, "node:Person").await, "name");
    assert!(
        names.contains(&"Bob".to_string()),
        "recovered AdoptWithDelta merge must include Bob; have {names:?}"
    );
    drop(db);
}

/// Which branch-merge publish path a partial-Phase-B test exercises.
enum MergeScenario {
    /// main stays at base → the touched table is `AdoptWithDelta`
    /// (`publish_adopted_delta`: append → upsert → delete).
    Adopt,
    /// main advances past base → the touched table is `RewriteMerged`
    /// (`publish_rewritten_merge_table`: merge_insert → delete → index).
    Rewrite,
}

async fn sorted_person_names(db: &Omnigraph) -> Vec<String> {
    let mut names = collect_column_strings(&read_table(db, "node:Person").await, "name");
    names.sort();
    names
}

/// THE recovery-atomicity regression gate. A branch merge whose per-table publish
/// is a multi-commit sequence (append → upsert → delete, or merge_insert → delete
/// → index) advances Lance HEAD step by step before the manifest publish. If the
/// process dies *mid*-sequence — after some commits but before the achieved-version
/// intent is recorded — recovery must roll the whole merge **back**, not publish
/// the partial and record the merge as complete.
///
/// The delta is deliberately MIXED — a fresh id (`bob`, append), a modified base id
/// (`carol`, upsert) and a removed base id (`dave`, delete) — so every partial
/// window leaves real work undone. Proof of rollback: after recovery the target is
/// back at its base name-set, and a *re-run* of the merge re-applies the full delta
/// (the partial was not silently recorded as "already merged").
///
/// RED before the fix: the loose `BranchMerge` classification rolls any
/// `lance_head > manifest_pinned` forward, so the partial is published (e.g. `bob`
/// present, `dave` kept) and the merge recorded — the first assert (back at base)
/// fails. GREEN after: `achieved_version == None` → `IncompletePhaseB` → roll back.
async fn assert_partial_merge_rolls_back(scenario: MergeScenario, failpoint: &str) {
    use omnigraph::loader::load_jsonl;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main {alice, carol, dave}; on `feature` add bob (append), bump carol
    // (upsert), remove dave (delete). For Rewrite, also move main past base so the
    // table classifies RewriteMerged instead of a fast-forward AdoptWithDelta.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n\
             {\"type\":\"Person\",\"data\":{\"name\":\"carol\",\"age\":50}}\n\
             {\"type\":\"Person\",\"data\":{\"name\":\"dave\",\"age\":60}}\n",
            LoadMode::Append,
        )
        .await
        .unwrap();
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "bob")], &[("$age", 40)]),
        )
        .await
        .unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "carol")], &[("$age", 55)]),
        )
        .await
        .unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "remove_person",
            &mixed_params(&[("$name", "dave")], &[]),
        )
        .await
        .unwrap();
        if matches!(scenario, MergeScenario::Rewrite) {
            db.mutate(
                "main",
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "alice")], &[("$age", 35)]),
            )
            .await
            .unwrap();
        }
    }

    // Crash mid-Phase-B at the injected window.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _fp = ScopedFailPoint::new(failpoint, "return");
        let err = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(
            err.to_string().contains(failpoint),
            "expected the injected failpoint {failpoint}, got: {err}"
        );
    }

    // Reopen → the open-time sweep must ROLL BACK to base (the merge never reached
    // its commit boundary), and a re-run must then apply the FULL delta.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        assert_eq!(
            sorted_person_names(&db).await,
            vec!["alice", "carol", "dave"],
            "partial Phase B at {failpoint} must roll back to base \
             (no bob, dave kept, carol's upsert reverted); the merge must NOT be recorded",
        );
        db.branch_merge("feature", "main").await.unwrap();
        assert_eq!(
            sorted_person_names(&db).await,
            vec!["alice", "bob", "carol"],
            "re-merge after rollback must re-apply the full delta \
             (bob added, dave removed) — proof the partial was not silently recorded",
        );
    }
}

#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_adopt_partial_after_append_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Adopt,
        "branch_merge.adopt_after_append_pre_upsert",
    )
    .await;
}

#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_adopt_partial_after_upsert_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Adopt,
        "branch_merge.adopt_after_upsert_pre_delete",
    )
    .await;
}

#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_rewrite_partial_after_merge_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Rewrite,
        "branch_merge.rewrite_after_merge_pre_delete",
    )
    .await;
}

#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_rewrite_partial_after_delete_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Rewrite,
        "branch_merge.rewrite_after_delete_pre_index",
    )
    .await;
}

/// Backward-compat: a `BranchMerge` sidecar written by a *pre-confirmation*
/// binary (schema_version 1, no `confirmed_version`) must NOT be misread as a
/// partial Phase B and rolled back. A pre-upgrade crash in the Phase-B→C gap can
/// leave such a sidecar over a *completed* merge; rolling it back would silently
/// discard a finished merge with no operator signal — the regression greptile /
/// Cursor flagged.
///
/// We synthesize the pre-upgrade sidecar realistically: crash after Phase B (a
/// real sidecar + advanced Lance HEAD), then downgrade the on-disk JSON to the
/// v1 shape (`schema_version` = 1, strip every pin's `confirmed_version`) before
/// reopening — exactly what an old binary would have left.
///
/// RED before the versioning fix: a v1 sidecar with no `confirmed_version`
/// classifies `IncompletePhaseB` → rolls back → `bob` is discarded. GREEN after:
/// the version-aware classifier reads v1 as the old loose generation → rolls
/// forward → `bob` preserved.
#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn pre_upgrade_v1_branch_merge_sidecar_rolls_forward_not_back() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // main {alice}; feature adds bob → a fast-forward AdoptWithDelta merge, which
    // writes a recovery sidecar.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n",
            LoadMode::Append,
        )
        .await
        .unwrap();
        db.branch_create("feature").await.unwrap();
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "bob")], &[("$age", 40)]),
        )
        .await
        .unwrap();
    }

    // Crash after Phase B (Lance HEAD advanced, manifest not published) → a real
    // sidecar lands on disk.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _fp = ScopedFailPoint::new("branch_merge.post_phase_b_pre_manifest_commit", "return");
        db.branch_merge("feature", "main").await.unwrap_err();
    }

    // Downgrade the sidecar to the pre-confirmation v1 shape an old binary writes.
    {
        let recovery_dir = std::path::Path::new(&uri).join("__recovery");
        let path = std::fs::read_dir(&recovery_dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "json"))
            .expect("a recovery sidecar must exist after the post-Phase-B crash");
        let mut v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        v["schema_version"] = serde_json::json!(1);
        for table in v["tables"].as_array_mut().unwrap() {
            table.as_object_mut().unwrap().remove("confirmed_version");
        }
        std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap()).unwrap();
    }

    // Reopen → the pre-upgrade completed merge must roll FORWARD (bob kept), not
    // be silently discarded.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        assert_eq!(
            sorted_person_names(&db).await,
            vec!["alice", "bob"],
            "a pre-confirmation (v1) BranchMerge sidecar over a completed merge must roll \
             forward, not be misread as a partial and rolled back",
        );
    }
}

/// Branch-axis variant of the branch_merge recovery test: target is a
/// non-main branch. Catches the branch-specific commit-graph head bug
/// (D2) — without `CommitGraph::open_at_branch`, the recovery sweep
/// would record the global head as the merge parent on a non-main
/// target, and future merges between the same pair would lose
/// already-up-to-date detection.
#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_phase_b_failure_recovered_on_non_main_target() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let target_parent_commit_id;

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

    let main_person_pin = {
        let db = Omnigraph::open(&uri).await.unwrap();
        db.snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .expect("main must have Person")
            .table_version
    };
    target_parent_commit_id = branch_head_commit_id(dir.path(), "target_branch")
        .await
        .unwrap();

    // Setup: failpoint fires after the per-table publish loop completes
    // but before commit_manifest_updates. Sidecar persists with
    // branch=Some("target_branch").
    {
        let db = Omnigraph::open(&uri).await.unwrap();
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
            sidecar_count, 1,
            "exactly one sidecar must persist after non-main branch_merge failure"
        );
        operation_id = single_sidecar_operation_id(dir.path());
    }

    // Recovery: reopen runs full sweep. The BranchMerge sidecar's branch
    // = Some("target_branch"); D2 fix opens a per-branch CommitGraph
    // for the audit append so the merge-parent linkage is correct.
    let db = Omnigraph::open(&uri).await.unwrap();
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::branch("node:Person", "target_branch")
                    .expected_main_manifest_pin(main_person_pin)
                    .expected_recovery_parent_commit_id(target_parent_commit_id),
            ],
        },
    )
    .await
    .unwrap();
}

/// Contract: the BranchMerge sidecar's per-table `table_branch` MUST be
/// the merge target branch (where commits land via
/// `publish_rewritten_merge_table` → `open_for_mutation` → potentially
/// `fork_dataset_from_entry_state`), NOT `entry.table_branch` (where
/// the table currently lives in the target's manifest snapshot).
///
/// `ensure_indices_for_branch` already has this invariant pinned by an
/// explicit comment at `table_ops.rs:115-120`. Without the same fix in
/// `merge.rs`, a future change to candidate selection or the publish
/// path that produces a `RewriteMerged` whose entry.table_branch
/// diverges from active_branch would silently drift Lance HEAD on the
/// target ref while recovery checks the wrong ref and no-ops the
/// rollback.
///
/// This test reads the sidecar JSON directly and asserts every per-pin
/// `table_branch` equals the active (target) branch. Even when the
/// values happen to coincide in practice (the strict candidate logic
/// keeps RewriteMerged tables on active_branch), the contract assertion
/// catches a regression that reverts to `entry.table_branch.clone()`.
#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_sidecar_pins_table_branch_to_active_branch() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

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

    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("branch_merge.post_phase_b_pre_manifest_commit", "return");
        let _ = db
            .branch_merge("source_branch", "target_branch")
            .await
            .expect_err("failpoint must fire");
    }

    let operation_id = single_sidecar_operation_id(dir.path());
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar_json = std::fs::read_to_string(&sidecar_path).unwrap();
    let sidecar: serde_json::Value = serde_json::from_str(&sidecar_json).unwrap();

    let tables = sidecar["tables"]
        .as_array()
        .expect("sidecar tables must be an array");
    assert!(
        !tables.is_empty(),
        "sidecar must pin at least one RewriteMerged table — both branches mutated Person"
    );
    for pin in tables {
        let table_branch = pin
            .get("table_branch")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "sidecar pin must record table_branch as the merge target (active_branch); \
                     got pin {pin:?}"
                )
            });
        assert_eq!(
            table_branch, "target_branch",
            "sidecar pin must record `table_branch` as the merge target branch (where \
             commits actually land via publish_rewritten_merge_table → open_for_mutation), \
             NOT entry.table_branch from the target snapshot. See merge.rs filter_map and \
             the rationale comment at table_ops.rs:115-120. Got pin: {pin:?}"
        );
    }
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

    // Setup: trigger the failpoint. Steady-state ensure_indices
    // produces zero sidecar pins (the helpers scope pins to tables
    // that genuinely need work); no sidecar is written. The failpoint
    // still fires, surfacing the Err.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new("ensure_indices.post_phase_b_pre_manifest_commit", "return");
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

    // Recovery: reopen is a clean no-op (no sidecar to recover).
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

// ─── MR-668 PR 2a: Omnigraph::init cleanup on partial failure ──────────────
//
// `init_with_storage` writes three schema artifacts before invoking
// `GraphCoordinator::init`. Without cleanup, a failure between any of those
// steps left orphan files behind, making the URI unusable for a retry of
// `init` (it would refuse because `_schema.pg` already exists). The tests
// below pin: on failpoint trigger at each of the three phase boundaries,
// the three schema files are removed before the error is returned.
//
// Coverage note: the third boundary (`init.after_coordinator_init`) only
// asserts cleanup of the schema files. Lance per-type directories and
// `__manifest/` are NOT cleaned up — that requires a recursive
// `StorageAdapter::delete_prefix` primitive deferred along with
// `DELETE /graphs/{id}` (MR-668 PR 2b). The orphan Lance directories
// after a coordinator-init-phase failure are documented as a known
// limitation.

#[tokio::test]
async fn init_failpoint_after_schema_pg_written_cleans_up_schema_file() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new("init.after_schema_pg_written", "return");

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: init.after_schema_pg_written"),
        "got: {err}"
    );

    // Only `_schema.pg` was written at this phase boundary, but the
    // cleanup attempts all three — `delete` treats not-found as Ok,
    // so the other two deletes are no-ops.
    assert!(
        !dir.path().join("_schema.pg").exists(),
        "_schema.pg must be cleaned up after init failure"
    );
}

#[tokio::test]
async fn init_failpoint_after_schema_contract_written_cleans_up_all_schema_files() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new("init.after_schema_contract_written", "return");

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: init.after_schema_contract_written"),
        "got: {err}"
    );

    assert!(
        !dir.path().join("_schema.pg").exists(),
        "_schema.pg must be cleaned up"
    );
    assert!(
        !dir.path().join("_schema.ir.json").exists(),
        "_schema.ir.json must be cleaned up"
    );
    assert!(
        !dir.path().join("__schema_state.json").exists(),
        "__schema_state.json must be cleaned up"
    );
}

#[tokio::test]
async fn init_failpoint_after_coordinator_init_cleans_up_schema_files() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new("init.after_coordinator_init", "return");

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: init.after_coordinator_init"),
        "got: {err}"
    );

    // Schema files are cleaned up by `best_effort_cleanup_init_artifacts`.
    assert!(
        !dir.path().join("_schema.pg").exists(),
        "_schema.pg must be cleaned up after late-phase init failure"
    );
    assert!(
        !dir.path().join("_schema.ir.json").exists(),
        "_schema.ir.json must be cleaned up after late-phase init failure"
    );
    assert!(
        !dir.path().join("__schema_state.json").exists(),
        "__schema_state.json must be cleaned up after late-phase init failure"
    );

    // Documented limitation: Lance per-type datasets and `__manifest/`
    // created by `GraphCoordinator::init` are NOT cleaned up — recursive
    // deletion requires the deferred `delete_prefix` primitive. This
    // assertion does NOT check for their absence; it merely documents
    // the boundary by noting we don't validate orphan directories here.
    // When PR 2b lands, this test can be tightened to assert the graph
    // root is fully empty.
}

#[tokio::test]
async fn init_failpoint_returns_original_error_not_cleanup_error() {
    // The cleanup is best-effort. If `storage.delete` fails (e.g. transient
    // network blip on S3), the original init failpoint error must still
    // surface — not be masked by a cleanup failure. This test triggers the
    // failpoint and asserts the returned error references the failpoint,
    // not the cleanup. (The cleanup currently logs via `tracing::warn`;
    // we can't easily fault-inject delete failures without another seam,
    // so this is a smoke test for the precedence contract.)
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new("init.after_schema_pg_written", "return");

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    // Failpoint message wins; no "cleanup" substring expected.
    let msg = err.to_string();
    assert!(
        msg.contains("init.after_schema_pg_written"),
        "init error must surface the failpoint cause, got: {msg}"
    );
}
