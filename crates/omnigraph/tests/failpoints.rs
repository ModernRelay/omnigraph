#![cfg(feature = "failpoints")]

mod helpers;

use std::process::Command;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::Schema;
use fail::FailScenario;
use lance::Dataset;
use lance::dataset::{CommitBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched};
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::failpoints::ScopedFailPoint;
use omnigraph::failpoints::names;
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::{LoadMode, load_jsonl};
use serial_test::serial;

use helpers::recovery::{
    FollowUpMutation, RecoveryExpectation, TableExpectation, assert_post_recovery_invariants,
    branch_head_commit_id, recovery_audit_kinds, single_sidecar_operation_id,
};
use helpers::{
    MUTATION_QUERIES, collect_column_strings, count_rows, mixed_params, mutate_main, params,
    read_table, version_main,
};

const SCHEMA_V1: &str = "node Person { name: String @key }\n";
const SCHEMA_V2_ADDED_TYPE: &str =
    "node Person { name: String @key }\nnode Company { name: String @key }\n";

const RFC023_KEY_SCHEMA: &str = r#"
node Person {
    name: String @key
    score: I32
}
"#;

const RFC023_EXTERNAL_WRITER_ENV: &str = "OMNIGRAPH_RFC023_EXTERNAL_WRITER";
const RFC023_EXTERNAL_URI_ENV: &str = "OMNIGRAPH_RFC023_EXTERNAL_URI";
const RFC023_EXTERNAL_MODE_ENV: &str = "OMNIGRAPH_RFC023_EXTERNAL_MODE";
const RFC023_EXTERNAL_PAYLOAD_ENV: &str = "OMNIGRAPH_RFC023_EXTERNAL_PAYLOAD";

const OCC_UNIQUE_SCHEMA: &str = r#"
node User {
    name: String @key
    email: String?
    @unique(email)
}
"#;

const OCC_UNIQUE_MUTATIONS: &str = r#"
query insert_user($name: String, $email: String) {
    insert User { name: $name, email: $email }
}
"#;

const OCC_DISJOINT_MUTATIONS: &str = r#"
query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}

query insert_company($name: String) {
    insert Company { name: $name }
}
"#;

async fn node_table_uri(db: &Omnigraph, type_name: &str) -> String {
    let table_key = format!("node:{type_name}");
    let snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let table_path = &snapshot
        .entry(&table_key)
        .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
        .table_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

/// Run one independent-process writer through the public load entry point.
///
/// The parent failpoint test pauses after its final authority check but before
/// it writes a sidecar. A second invocation of this integration-test binary is
/// therefore the smallest faithful stand-in for a foreign process: it does not
/// share the root-scoped in-process gates, and it completes both the Lance
/// effect and manifest publish before the stale parent resumes.
fn run_rfc023_external_writer(
    uri: String,
    mode: LoadMode,
    payload: String,
) -> std::result::Result<(), String> {
    let mode = match mode {
        LoadMode::Append => "append",
        LoadMode::Merge => "merge",
        LoadMode::Overwrite => "overwrite",
    };
    let output = Command::new(std::env::current_exe().map_err(|error| error.to_string())?)
        .arg("--exact")
        .arg("rfc023_external_writer_process")
        .arg("--ignored")
        .arg("--nocapture")
        .env(RFC023_EXTERNAL_WRITER_ENV, "1")
        .env(RFC023_EXTERNAL_URI_ENV, uri)
        .env(RFC023_EXTERNAL_MODE_ENV, mode)
        .env(RFC023_EXTERNAL_PAYLOAD_ENV, payload)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "external writer failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    ))
}

/// Subprocess-only half of the RFC-023 foreign-writer tests below.
///
/// It is ignored during ordinary test enumeration. The parent starts it by
/// exact name and supplies all state through environment variables. Keep this
/// helper non-`serial`: a child must not contend on serial_test's interprocess
/// lock with the parent that is deliberately waiting for it.
#[test]
#[ignore = "subprocess helper; exercised by RFC-023 failpoint tests"]
fn rfc023_external_writer_process() {
    if std::env::var_os(RFC023_EXTERNAL_WRITER_ENV).is_none() {
        return;
    }
    let uri = std::env::var(RFC023_EXTERNAL_URI_ENV).expect("external writer URI");
    let payload = std::env::var(RFC023_EXTERNAL_PAYLOAD_ENV).expect("external writer payload");
    let mode = match std::env::var(RFC023_EXTERNAL_MODE_ENV)
        .expect("external writer mode")
        .as_str()
    {
        "append" => LoadMode::Append,
        "merge" => LoadMode::Merge,
        "overwrite" => LoadMode::Overwrite,
        other => panic!("unknown external writer mode '{other}'"),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let db = Omnigraph::open(&uri).await.unwrap();
            db.load("main", &payload, mode).await.unwrap();
        });
}

/// Commit a one-row filtered exact-id upsert directly through Lance.
///
/// This is intentionally a physical-only test injector. It is used only after
/// an enrolled multi-table writer has committed table 1 and parked before
/// table 2, where publishing a competing graph commit would be both unnecessary
/// and semantically wrong for the recovery assertion. The source schema is
/// restricted to the two-column `name @key` fixture used by that test.
async fn commit_raw_fenced_name_row(table_uri: &str, id: &str) {
    let base = Arc::new(Dataset::open(table_uri).await.unwrap());
    let schema = Arc::new(Schema::from(base.schema()));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"],
        "raw conflict injector is intentionally limited to the name-only fixture"
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(StringArray::from(vec![id])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut builder = MergeInsertBuilder::try_new(base.clone(), vec!["id".to_string()]).unwrap();
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .use_index(false)
        .conflict_retries(0);
    let staged = builder
        .try_build()
        .unwrap()
        .execute_uncommitted(reader)
        .await
        .unwrap();
    CommitBuilder::new(base)
        .with_max_retries(0)
        .execute(staged.transaction)
        .await
        .unwrap();
}

fn collect_i32_column(batches: &[RecordBatch], column: &str) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("missing column '{column}'"))
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("column '{column}' is not Int32"));
            (0..values.len()).map(|row| values.value(row))
        })
        .collect()
}

fn node_table_identity_json(db: &Omnigraph, type_name: &str) -> serde_json::Value {
    let catalog = db.catalog();
    let stable_table_id = catalog
        .type_id(type_name)
        .unwrap_or_else(|| panic!("bound catalog has no stable id for {type_name}"))
        .get();
    let table_incarnation_id = catalog
        .table_incarnation_id(type_name)
        .unwrap_or_else(|| panic!("bound catalog has no incarnation id for {type_name}"))
        .get();
    serde_json::json!({
        "stable_table_id": stable_table_id,
        "table_incarnation_id": table_incarnation_id,
    })
}

fn pending_schema_apply_node_table_uri(root: &str, type_name: &str) -> String {
    let operation_id = single_sidecar_operation_id(std::path::Path::new(root));
    let sidecar_path = std::path::Path::new(root)
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
    let table_key = format!("node:{type_name}");
    let registration = sidecar["protocol_v7"]["intended_delta"]["registrations"]
        .as_array()
        .expect("SchemaApply sidecar must carry its intended registrations")
        .iter()
        .find(|registration| registration["table_key"] == table_key)
        .unwrap_or_else(|| panic!("SchemaApply sidecar has no registration for {table_key}"));
    let table_path = registration["table_path"]
        .as_str()
        .expect("SchemaApply registration must carry its identity-derived table path");
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

// Lance can durably complete a native ref mutation while the caller observes
// an error (for example, a lost object-store acknowledgement). BranchContents
// is the logical authority in both directions: matching create metadata means
// success, and an absent ref means delete succeeded even if tree cleanup or the
// acknowledgement failed. Neither control emits graph lineage or a main-table
// version.
#[tokio::test]
#[serial]
async fn native_branch_controls_reclassify_lost_acknowledgements() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::init_and_load(&dir).await;
    let before_version = version_main(&db).await.unwrap();
    let before_commits = db.list_commits(Some("main")).await.unwrap().len();

    {
        let _fp = ScopedFailPoint::new(names::BRANCH_CREATE_POST_NATIVE, "return");
        db.branch_create("feature")
            .await
            .expect("matching BranchContents must classify a lost create acknowledgement");
    }
    assert!(
        db.branch_list()
            .await
            .unwrap()
            .iter()
            .any(|branch| branch == "feature")
    );

    {
        let _fp = ScopedFailPoint::new(names::BRANCH_DELETE_POST_NATIVE, "return");
        db.branch_delete("feature")
            .await
            .expect("absent BranchContents must classify a lost delete acknowledgement");
    }
    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
    assert_eq!(version_main(&db).await.unwrap(), before_version);
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        before_commits,
        "native branch controls must not manufacture graph lineage"
    );
}

// Branch delete flips the manifest authority first, then reclaims the per-table
// forks best-effort. A failure during that reclaim (here, the
// `branch_delete.before_table_cleanup` failpoint, standing in for a transient
// object-store error) must NOT fail the call: the branch is already gone, and
// `cleanup` reconciles the stranded fork. The branch name is reusable after.
#[tokio::test]
#[serial]
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

    let person_uri = node_table_uri(&main, "Person").await;
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
        let _fp = ScopedFailPoint::new(names::BRANCH_DELETE_BEFORE_TABLE_CLEANUP, "return");
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
#[serial]
async fn recreate_over_orphaned_fork_self_heals_without_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;

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
        let _fp = ScopedFailPoint::new(names::BRANCH_DELETE_BEFORE_TABLE_CLEANUP, "return");
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
// cleanup. If that classifier is Indeterminate after v3 recovery has been
// armed, the write must retain recovery ownership and return RecoveryRequired;
// deleting the sidecar would lose authority over a target ref that may already
// have been created. A full read-write reopen resolves the armed no-HEAD-effect
// attempt before the write can be retried.
#[tokio::test]
#[serial]
async fn recreate_over_orphaned_fork_reports_indeterminate_authority_read() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("feature", base, None).await.unwrap();
    }

    let row = r#"{"type":"Person","data":{"name":"Grace","age":37}}"#;
    {
        let _fp = ScopedFailPoint::new(names::CLASSIFY_FRESH_READ, "return");
        let err = db
            .load_as("feature", None, row, LoadMode::Merge, None)
            .await
            .expect_err("indeterminate authority read must fail retryably");

        assert!(
            matches!(err, OmniError::RecoveryRequired { .. }),
            "an ambiguous post-arm fork error must retain recovery ownership: {err:?}"
        );
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

    drop(db);
    let db = Omnigraph::open(&uri)
        .await
        .expect("read-write reopen must resolve the retained armed sidecar");
    db.load_as("feature", None, row, LoadMode::Merge, None)
        .await
        .expect("after recovery, fresh authority reclaims the orphan and the write converges");
}

// cleanup is the guaranteed convergence backstop, so one table's transient
// failure must not abort the whole sweep. Inject a one-shot version-GC failure
// for a single table and assert: cleanup still succeeds, the failure is
// surfaced per-table in the returned stats, and the independent reconcile pass
// still reclaimed an orphan.
#[tokio::test]
#[serial]
async fn cleanup_isolates_single_table_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = helpers::init_and_load(&dir).await;

    // Forge an orphaned fork on the Person table (a reconcile target).
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
    }

    // One table's version GC fails once; the sweep must isolate it.
    let _fp = ScopedFailPoint::new(names::CLEANUP_TABLE_GC, "1*return");
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
#[serial]
async fn cleanup_isolates_reconcile_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = helpers::init_and_load(&dir).await;

    // Forge an orphaned fork the reconcile pass will try to reclaim.
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
    }

    // Inject a one-shot failure into the reconcile force-delete. The sweep must
    // not abort.
    {
        let _fp = ScopedFailPoint::new(names::CLEANUP_RECONCILE_FORK, "1*return");
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

// `classify_fork_ref` returns `Indeterminate` when the fresh-authority read
// fails on a LIVE branch — and a destructive caller must SKIP, never delete, on
// that ambiguity. Here the reconciler has a genuine origin-2 orphan candidate
// (a manifest-unreferenced Person fork on the live `feature` branch), but the
// `classify.fresh_read` failpoint makes the fresh re-check fail: cleanup must
// leave the ref in place (cannot confirm it is unreferenced), then reclaim it on
// the next run once the read succeeds. This pins the Indeterminate arm and the
// don't-destroy-on-ambiguity rule end-to-end through cleanup.
#[tokio::test]
#[serial]
async fn reconcile_skips_fork_when_fresh_recheck_is_unavailable_then_converges() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    // Forge a manifest-unreferenced Person fork on the live `feature` branch —
    // a genuine orphan the reconciler would normally reclaim.
    let person_uri = node_table_uri(&db, "Person").await;
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
        let _fp = ScopedFailPoint::new(names::CLASSIFY_FRESH_READ, "return");
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

// A fork collision must be classified by the manifest authority, not by Lance
// branch versions. When a concurrent first-write legitimately wins the fork
// race, the loser sees a changed read set — but that is a safe pre-effect
// retry for Insert. RFC-022 discards and reprepares it automatically, never
// misclassifying the live fork as an orphan that needs cleanup.
//
// Ordering is made deterministic (no fixed sleeps) via the shared rendezvous:
// it parks the first arrival (writer A) at the fork point until released; later
// arrivals (writer B) fall through. The test waits on the reached condition,
// lets B win and commit the fork, then releases A.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn fork_collision_with_live_concurrent_fork_reprepares() {
    let _scenario = FailScenario::setup();

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let rv = helpers::failpoint::Rendezvous::park_first(names::FORK_BEFORE_CLASSIFY);

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

    // Wait until A is parked at the fork point.
    rv.wait_until_reached().await;

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

    // Release A; it resumes, sees that B changed branch authority, discards its
    // stale attempt, and reprepares Eve against the now-live feature fork.
    rv.release();
    writer_a
        .await
        .unwrap()
        .expect("A's retryable insert must reprepare after B wins the fork");

    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        6,
        "feature must preserve four inherited rows plus both concurrent inserts"
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        4,
        "feature fork retries must not change main"
    );
}

/// RFC-022 first-touch forks are physical effects and must be protected before
/// `create_branch` runs. Writer A parks after its target ref exists but before
/// any data commit. A separately-opened writer B shares the root-scoped gates:
/// it waits rather than reclaiming/deleting A's live ref merely because the
/// graph manifest has not published it yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cross_handle_reclaim_never_deletes_live_intent_owned_fork() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();
    drop(main);

    // Open both handles before A creates a pending sidecar. Opening B afterward
    // would deliberately invoke the quiesced full recovery sweep instead of the
    // live-writer roll-forward-only path this race exercises.
    let db_a = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_FORK_PRE_COMMIT);

    let writer_a_db = std::sync::Arc::clone(&db_a);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
            )
            .await
    });
    rendezvous.wait_until_reached().await;

    let person_uri = node_table_uri(&db_b, "Person").await;
    let fork_before = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("feature")
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();

    let writer_b_db = std::sync::Arc::clone(&db_b);
    let mut writer_b = tokio::spawn(async move {
        writer_b_db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
            )
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut writer_b)
            .await
            .is_err(),
        "the second handle must wait on A's root-scoped effect gates"
    );

    let fork_after = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("feature")
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    assert_eq!(
        fork_after, fork_before,
        "B must not delete and recreate A's intent-owned Lance ref"
    );

    rendezvous.release();
    writer_a
        .await
        .unwrap()
        .expect("A must commit normally after the competing reclaim is rejected");
    writer_b
        .await
        .unwrap()
        .expect("B must reprepare after A publishes and update without recreating the ref");
    drop(db_a);

    let mut db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        5,
        "feature must contain the four inherited rows plus A's insert"
    );
    let alice = helpers::query_branch(
        &mut db,
        "feature",
        helpers::TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(alice.to_rust_json()[0]["p.age"], serde_json::json!(99));
}

#[tokio::test]
#[serial]
async fn armed_first_touch_recovery_accepts_missing_target_ref() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_SIDECAR_PRE_FORK, "return");
        let err = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
            )
            .await
            .expect_err("failure after arming must leave recovery intent");
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
    }
    let operation_id = single_sidecar_operation_id(dir.path());
    let person_uri = node_table_uri(&db, "Person").await;
    assert!(
        !lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "precondition: crash happened before the target ref was created"
    );

    // Materialize the narrower Lance crash window underneath the already-
    // durable writer intent: shallow clone present, BranchContents absent.
    // Full recovery must not equate "not listed" with "no physical fork".
    let mut person = lance::Dataset::open(&person_uri).await.unwrap();
    let person_version = person.version().version;
    person
        .create_branch("feature", person_version, None)
        .await
        .unwrap();
    std::fs::remove_file(
        std::path::Path::new(&person_uri)
            .join("_refs")
            .join("branches")
            .join("feature.json"),
    )
    .unwrap();
    assert!(
        std::path::Path::new(&person_uri)
            .join("tree")
            .join("feature")
            .exists(),
        "precondition: clone-only target tree exists"
    );
    drop(person);

    // Stage A is a synchronous, branch-aware barrier. The Armed sidecar has no
    // table effect for a HEAD-drift check to discover, but it is still ownership:
    // another mutation on the same branch must name it and stop before preparing
    // a competing first-touch effect.
    let mutation_err = db
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Follower")], &[("$age", 23)]),
        )
        .await
        .expect_err("same-branch mutation must stop at the unresolved Armed intent");
    match mutation_err {
        OmniError::RecoveryRequired {
            operation_id: actual,
            ..
        } => assert_eq!(actual, operation_id),
        other => panic!("expected RecoveryRequired for the Armed intent, got {other}"),
    }

    // An explicit load base is read authority too. Reject before the implicit
    // child branch is created, even though the load target itself has no sidecar.
    let load_err = db
        .load_as(
            "child",
            Some("feature"),
            r#"{"type":"Person","data":{"name":"LoadFollower","age":24}}"#,
            LoadMode::Merge,
            None,
        )
        .await
        .expect_err("load must stop on an unresolved intent for its explicit base");
    match load_err {
        OmniError::RecoveryRequired {
            operation_id: actual,
            ..
        } => assert_eq!(actual, operation_id),
        other => panic!("expected RecoveryRequired for the load base, got {other}"),
    }
    assert!(
        !db.branch_list()
            .await
            .unwrap()
            .iter()
            .any(|branch| branch == "child"),
        "barrier rejection must precede the load's implicit branch creation"
    );

    // The feature intent does not overlap main. Branch filtering must avoid
    // turning one branch's rollback-eligible residue into a graph-wide outage.
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "MainFollower")], &[("$age", 25)]),
    )
    .await
    .expect("an unresolved feature intent must not block a main mutation");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, main_rows + 1);
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![operation_id.clone()],
        "blocked attempts must neither replace nor delete the owning sidecar"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows,
        "abandoned intent must leave the feature table inherited"
    );
    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "full recovery must remove the empty armed intent"
    );
    assert!(
        !std::path::Path::new(&person_uri)
            .join("tree")
            .join("feature")
            .exists(),
        "full recovery must reclaim an unlisted clone-only table fork"
    );
}

#[tokio::test]
#[serial]
async fn armed_first_touch_recovery_defers_legacy_path_overlap_until_leaf_delete() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;

    // Materialize the leaf first so its graph snapshot owns a matching
    // per-table Lance branch. New OmniGraph versions reject the inverse live
    // namespace, but old stores could contain it.
    db.branch_create("feature/child").await.unwrap();
    db.mutate(
        "feature/child",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Leaf")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Forge only the legacy graph-level ancestor admission. It inherits main,
    // so an interrupted first write will own an unpublished table fork.
    let mut manifest = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap();
    let manifest_version = manifest.version().version;
    manifest
        .create_branch("feature", manifest_version, None)
        .await
        .unwrap();
    drop(manifest);

    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_SIDECAR_PRE_FORK, "return");
        let error = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Ancestor")], &[("$age", 23)]),
            )
            .await
            .expect_err("failure after arming must leave the ancestor recovery intent");
        assert!(matches!(error, OmniError::RecoveryRequired { .. }));
    }
    let operation_id = single_sidecar_operation_id(dir.path());

    // Reproduce Lance's narrower clone-only crash window under the already
    // durable intent. The live leaf shares the ancestor's physical path, so a
    // force-delete of the ancestor cannot safely complete yet.
    let person_uri = node_table_uri(&db, "Person").await;
    let mut person = lance::Dataset::open(&person_uri).await.unwrap();
    let person_version = person.version().version;
    person
        .create_branch("feature", person_version, None)
        .await
        .unwrap();
    std::fs::remove_file(
        std::path::Path::new(&person_uri)
            .join("_refs")
            .join("branches")
            .join("feature.json"),
    )
    .unwrap();
    assert!(
        person
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature/child"),
        "precondition: live leaf table branch exists"
    );
    assert!(
        !person
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "precondition: ancestor is clone-only"
    );
    drop(person);
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("legacy path overlap must defer cleanup instead of wedging read-write open");
    assert!(
        dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "deferred cleanup must retain its ownership sidecar"
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows,
        "the empty ancestor intent must remain unpublished"
    );

    recovered
        .branch_delete("feature/child")
        .await
        .expect("open handle must permit the documented leaf-first remediation");
    drop(recovered);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("the next Full sweep must finish ancestor cleanup");
    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "recovery must retire the intent after the path child is gone"
    );
    assert!(
        !std::path::Path::new(&person_uri)
            .join("tree")
            .join("feature")
            .exists(),
        "the clone-only ancestor tree must be reclaimed after leaf deletion"
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows
    );
}

#[tokio::test]
#[serial]
async fn partial_first_touch_recovery_fails_closed_on_legacy_path_overlap() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_person_rows = helpers::count_rows(&db, "node:Person").await;
    let main_edge_rows = helpers::count_rows(&db, "edge:Knows").await;

    let main_snapshot = db.snapshot_of("main").await.unwrap();
    let table_pins = ["node:Person", "edge:Knows"]
        .into_iter()
        .map(|table_key| {
            let entry = main_snapshot.entry(table_key).unwrap();
            (
                table_key.to_string(),
                format!("{uri}/{}", entry.table_path),
                entry.table_version,
            )
        })
        .collect::<Vec<_>>();

    db.branch_create("feature").await.unwrap();
    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_TABLE_COMMIT, "return");
        let error = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person_and_friend",
                &mixed_params(
                    &[("$name", "Ancestor"), ("$friend", "Alice")],
                    &[("$age", 23)],
                ),
            )
            .await
            .expect_err("the first exact table effect must leave a partial v3 intent");
        match error {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("expected RecoveryRequired, got {other}"),
        }
    };
    assert_eq!(single_sidecar_operation_id(dir.path()), operation_id);

    // All deferred first-touch refs are created before the table-commit loop.
    // Staging order is intentionally non-semantic, so either table may own the
    // one durable effect while its sibling remains at the exact fork point.
    let mut head_deltas = Vec::new();
    let mut heads_before_open = Vec::new();
    for (table_key, table_uri, expected_version) in &table_pins {
        let mut root = lance::Dataset::open(table_uri).await.unwrap();
        let branches = root.list_branches().await.unwrap();
        assert!(
            branches.contains_key("feature"),
            "precondition: {table_key} has its armed first-touch ref"
        );
        let head = root
            .checkout_branch("feature")
            .await
            .unwrap()
            .version()
            .version;
        heads_before_open.push((table_key.clone(), head));
        head_deltas.push(head.checked_sub(*expected_version).unwrap());

        // Forge the legacy namespace only after the partial effect exists. New
        // OmniGraph versions reject this path-prefix overlap at branch create,
        // but old stores can contain it. Clone from the exact feature HEAD so
        // the child itself introduces no extra ancestor movement.
        root.create_branch("feature/child", ("feature", head), None)
            .await
            .unwrap();
    }
    head_deltas.sort_unstable();
    assert_eq!(
        head_deltas,
        vec![0, 1],
        "exactly one table effect must be durable while its sibling remains an untouched fork"
    );

    let mut manifest = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap();
    let feature_manifest_version = manifest
        .checkout_branch("feature")
        .await
        .unwrap()
        .version()
        .version;
    manifest
        .create_branch("feature/child", ("feature", feature_manifest_version), None)
        .await
        .unwrap();
    drop(manifest);

    let open_error = match Omnigraph::open(&uri).await {
        Ok(_) => panic!(
            "Full recovery must not return a writable handle while owned effects remain unrolled"
        ),
        Err(error) => error,
    };
    assert!(
        open_error.to_string().contains("owns physical effects")
            && open_error.to_string().contains("feature/child"),
        "failure must explain the safe leaf-first remediation boundary: {open_error}"
    );
    assert!(
        dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "failed Full recovery must retain exact ownership"
    );
    let manifest_after_failed_open = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap()
        .checkout_branch("feature")
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        manifest_after_failed_open, feature_manifest_version,
        "failed rollback preflight must not publish the manifest"
    );
    for ((table_key, table_uri, _), (_, before)) in table_pins.iter().zip(heads_before_open.iter())
    {
        let root = lance::Dataset::open(table_uri).await.unwrap();
        let after = root
            .checkout_branch("feature")
            .await
            .unwrap()
            .version()
            .version;
        assert_eq!(
            after, *before,
            "failed preflight must not restore or publish {table_key}"
        );
    }

    // A handle that predates the interrupted attempt can remove the graph leaf
    // under the normal branch-control gates. This fixture forged its table refs
    // outside the manifest, so finish the documented offline leaf cleanup at
    // Lance level. Once the physical overlap is gone, the next quiesced Full
    // sweep can compensate the owned effect atomically.
    db.branch_delete("feature/child").await.unwrap();
    for (_, table_uri, _) in &table_pins {
        lance::Dataset::open(table_uri)
            .await
            .unwrap()
            .force_delete_branch("feature/child")
            .await
            .unwrap();
    }
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("rollback must converge after leaf-first remediation");
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_person_rows
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "edge:Knows").await,
        main_edge_rows
    );
}

#[tokio::test]
#[serial]
async fn load_without_explicit_base_does_not_add_main_to_recovery_scope() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();

    // Leave an Armed main intent with an exact physical effect. RollForwardOnly
    // must defer it, giving the load barrier a real unresolved main operation to
    // filter rather than a synthetic test-only record.
    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_CONFIRM, "return");
        let err = db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "InterruptedMain")], &[("$age", 26)]),
            )
            .await
            .expect_err("confirmation failure must leave an Armed main intent");
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
    }
    let operation_id = single_sidecar_operation_id(dir.path());

    // `base=None` means no explicit base dependency; it must not be represented
    // as a phantom main branch in the recovery scope. The feature snapshot was
    // cut before the interrupted main effect and can proceed independently.
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"FeatureLoad","age":27}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("a feature load without --from must ignore unresolved main recovery");
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        main_rows + 1
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![operation_id],
        "the unrelated main intent must remain for Full recovery"
    );

    drop(db);
    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must still compensate the interrupted main effect");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        main_rows
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows + 1
    );
}

#[tokio::test]
#[serial]
async fn armed_first_touch_recovery_reclaims_exact_no_effect_fork() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_FORK_PRE_COMMIT, "return");
        let err = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
            )
            .await
            .expect_err("failure after the fork must require recovery");
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
    }
    let operation_id = single_sidecar_operation_id(dir.path());
    let person_uri = node_table_uri(&db, "Person").await;
    assert!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "precondition: intent-owned target ref exists without a committed effect"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows,
        "recovery must leave the feature table inherited"
    );
    assert!(
        !lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "full recovery must reclaim the exact unpublished no-effect ref"
    );
    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "full recovery must delete the abandoned sidecar"
    );
}

/// A live writer keeps its schema/branch/table guards through Phase D. A
/// refresh from a separately-opened handle must share those root-scoped gates,
/// wait, and then skip the sidecar after A publishes/deletes it. Recovering the
/// confirmed effect early would make A's own publisher lose authority.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cross_handle_refresh_waits_for_live_confirmed_writer() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(helpers::init_and_load(&dir).await);
    let db_a = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER);
    let writer_db = std::sync::Arc::clone(&db_a);
    let writer = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
            )
            .await
    });
    rendezvous.wait_until_reached().await;

    let refresher_db = std::sync::Arc::clone(&db_b);
    let mut refresher = tokio::spawn(async move { refresher_db.refresh().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut refresher)
            .await
            .is_err(),
        "refresh must wait for the live writer's shared table gate"
    );

    rendezvous.release();
    writer
        .await
        .unwrap()
        .expect("A must publish normally; refresh must not steal its sidecar");
    refresher
        .await
        .unwrap()
        .expect("refresh must skip A's now-deleted sidecar");
    assert_eq!(helpers::count_rows(&db_b, "node:Person").await, 5);
    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "the successful writer must leave no recovery sidecar"
    );
}

/// A ReadWrite open is itself a recovery actor. It must join the same
/// root-scoped gates as an already-open handle before running Full recovery;
/// otherwise it can classify an Armed pre-fork sidecar as dead and delete it
/// while the live writer is still about to create the target ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn read_write_open_waits_for_live_armed_prefork_writer() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db_a = std::sync::Arc::new(helpers::init_and_load(&dir).await);
    db_a.branch_create("feature").await.unwrap();

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_SIDECAR_PRE_FORK);
    let writer_db = std::sync::Arc::clone(&db_a);
    let writer = tokio::spawn(async move {
        writer_db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
            )
            .await
    });
    rendezvous.wait_until_reached().await;

    let opener_uri = uri.clone();
    let mut opener = tokio::spawn(async move { Omnigraph::open(&opener_uri).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut opener)
            .await
            .is_err(),
        "ReadWrite open must wait on the live writer's root-scoped schema gate"
    );

    rendezvous.release();
    writer
        .await
        .unwrap()
        .expect("A must finish; Full recovery must not delete its live intent");
    let opened = opener
        .await
        .unwrap()
        .expect("the waiting open must succeed after A releases its gates");
    assert_eq!(
        helpers::count_rows_branch(&opened, "feature", "node:Person").await,
        5
    );
    let recovery_dir = dir.path().join("__recovery");
    assert!(!recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none());
}

/// Two dead Armed attempts may name the same first-touch ref: A created the
/// exact no-effect fork and failed, then B armed and failed before its fork.
/// Full recovery is quiesced by the shared gates: while another claim exists a
/// no-effect sidecar discards only itself, and the last survivor performs the
/// exact-ref cleanup. Mutual foreign-claim rejection would wedge every future
/// ReadWrite open forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn full_recovery_converges_multiple_no_effect_claims_for_one_fork() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();

    // B passes the synchronous recovery barrier before A's sidecar exists,
    // then parks before gates/effects. This models a foreign process or an
    // already-prepared attempt; a newly-started in-process write is blocked by
    // Stage A once A leaves its unresolved intent.
    let before_effect =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let writer_b_db = std::sync::Arc::new(db);
    let writer_b_handle = std::sync::Arc::clone(&writer_b_db);
    let writer_b = tokio::spawn(async move {
        writer_b_handle
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "B")], &[("$age", 21)]),
            )
            .await
    });
    before_effect.wait_until_reached().await;

    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_FORK_PRE_COMMIT, "return");
        let err = writer_b_db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "A")], &[("$age", 20)]),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
    }
    let first_operation = single_sidecar_operation_id(dir.path());
    let first_sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{first_operation}.json"));
    let first_sidecar_body = std::fs::read_to_string(&first_sidecar_path).unwrap();
    // Model a foreign/already-prepared B that did not observe A at the current
    // binary's second Stage-A check; restore A afterward to exercise recovery's
    // compatibility path for the preexisting two-claim state.
    std::fs::remove_file(&first_sidecar_path).unwrap();
    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_SIDECAR_PRE_FORK, "return");
        before_effect.release();
        let err = writer_b.await.unwrap().unwrap_err();
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
    }
    std::fs::write(&first_sidecar_path, first_sidecar_body).unwrap();
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        2,
        "precondition: both no-effect claims are durable"
    );
    let person_uri = node_table_uri(&writer_b_db, "Person").await;
    assert!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "precondition: A's exact no-effect target ref exists"
    );
    drop(writer_b_db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("ordered no-effect claim recovery must converge");
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    assert!(
        !lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "the last no-effect claim must reclaim the exact unpublished ref"
    );
}

/// A no-effect Armed intent must not delete a ref while a competing confirmed
/// intent claims it. A starts first and fails before fork; a synthesized
/// foreign/older writer B does not observe A, creates/commits/confirms that same
/// ref, and fails before publish. Full recovery discards A without touching B's
/// ref, then rolls B's exact effect forward.
#[tokio::test]
#[serial]
async fn full_recovery_discards_no_effect_claim_before_confirmed_competitor() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = std::sync::Arc::new(helpers::init_and_load(&dir).await);
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("feature").await.unwrap();

    // Produce a real, valid Armed pre-fork sidecar for A.
    let armed_sidecar_path;
    let armed_sidecar_body;
    {
        let _failpoint = ScopedFailPoint::new(names::MUTATION_POST_SIDECAR_PRE_FORK, "return");
        let err = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Abandoned")], &[("$age", 40)]),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
        let armed_operation = single_sidecar_operation_id(dir.path());
        armed_sidecar_path = dir
            .path()
            .join("__recovery")
            .join(format!("{armed_operation}.json"));
        armed_sidecar_body = std::fs::read_to_string(&armed_sidecar_path).unwrap();
    }

    // Current binaries close the Stage-A TOCTOU in commit_all and would reject
    // B while A is visible. Temporarily hide A to synthesize the compatible
    // preexisting/foreign-process state an older writer can leave, then restore
    // the exact sidecar bytes after B's confirmed effect is durable.
    std::fs::remove_file(&armed_sidecar_path).unwrap();
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        let err = db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Winner")], &[("$age", 41)]),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("mutation.post_finalize_pre_publisher"),
            "unexpected confirmed-writer failure: {err}"
        );
    }
    std::fs::write(&armed_sidecar_path, armed_sidecar_body).unwrap();
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        2,
        "precondition: Armed A and EffectsConfirmed B are both pending"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("A must discard without deleting B's confirmed ref");
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        main_rows + 1
    );
    let people = helpers::read_table_branch(&recovered, "feature", "node:Person").await;
    let names = collect_column_strings(&people, "name");
    assert!(names.iter().any(|name| name == "Winner"));
    assert!(!names.iter().any(|name| name == "Abandoned"));
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn graph_publish_failpoint_triggers_before_commit_append() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();
    let _failpoint = ScopedFailPoint::new(names::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND, "return");

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

/// RFC-023's key fence is allowed to return an ordinary conflict or reprepare
/// only after exact recovery classification proves that this attempt moved no
/// table HEAD and retires its Armed sidecar.
///
/// The independent writer wins after A's final authority check but before A
/// writes its sidecar. For strict Append, A must return typed `KeyConflict`
/// without retrying. For Merge/upsert, the same effect-free substrate conflict
/// becomes `ReadSetChanged` internally and the public load contract must replay
/// the whole operation against the winner. Both outcomes leave no recovery
/// intent; a generic `RecoveryRequired` would be unnecessarily sticky, while a
/// same-transaction rebase would weaken the prepared plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn rfc023_effect_free_conflict_is_typed_or_fully_reprepared() {
    let _scenario = FailScenario::setup();

    for (case, mode, expected_attempts, expected_score) in [
        ("strict", LoadMode::Append, 1_u64, 1_i32),
        ("upsert", LoadMode::Merge, 2_u64, 2_i32),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap().to_string();
        let db = Arc::new(Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap());
        let probes = MergeWriteProbes::default();

        let rendezvous = helpers::failpoint::Rendezvous::park_first(names::FORK_BEFORE_CLASSIFY);
        let writer_db = Arc::clone(&db);
        let writer_probes = probes.clone();
        let writer = tokio::spawn(async move {
            with_merge_write_probes(
                writer_probes,
                writer_db.load(
                    "main",
                    r#"{"type":"Person","data":{"name":"racer","score":2}}"#,
                    mode,
                ),
            )
            .await
        });

        rendezvous.wait_until_reached().await;
        let external_uri = uri.clone();
        let external = tokio::task::spawn_blocking(move || {
            run_rfc023_external_writer(
                external_uri,
                mode,
                r#"{"type":"Person","data":{"name":"racer","score":1}}"#.to_string(),
            )
        })
        .await
        .unwrap();
        // Always release A before asserting the subprocess result so a useful
        // child failure cannot strand the parked writer until its timeout.
        rendezvous.release();
        external.unwrap_or_else(|error| panic!("{case}: {error}"));

        let outcome = writer.await.unwrap();
        if mode == LoadMode::Append {
            let err = outcome.expect_err("strict insert must reject the foreign key winner");
            assert!(
                matches!(
                    err,
                    OmniError::KeyConflict {
                        ref table_key,
                        key: Some(ref key),
                    } if table_key == "node:Person" && key == "racer"
                ),
                "strict conflict must remain typed and report the freshly visible exact key: {err:?}"
            );
            // The strict error exits before the normal outer refresh. Refresh
            // explicitly so the result assertion uses the foreign writer's
            // manifest-published snapshot, not this handle's old warm view.
            db.refresh().await.unwrap();
        } else {
            outcome.expect("upsert must fully reprepare and publish after the winner");
        }

        assert_eq!(
            probes.stage_merge_insert_calls(),
            expected_attempts,
            "{case}: strict must not retry; upsert must stage a fresh second attempt"
        );
        assert!(
            helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
            "{case}: an exact effect-free conflict must retire its Armed sidecar"
        );
        assert_eq!(count_rows(&db, "node:Person").await, 1);
        assert_eq!(
            collect_i32_column(&read_table(&db, "node:Person").await, "score"),
            vec![expected_score],
            "{case}: final data must identify whether the foreign winner or A's fresh replay won"
        );

        drop(rendezvous);
    }
}

/// Lance's retryable conflict class is wider than an exact-key collision. A
/// disjoint raw Append committed after strict staging still makes the prepared
/// filtered transaction stale, but fresh manifest authority proves that none
/// of the strict source ids exists. The public load must therefore reprepare;
/// it must never fabricate `KeyConflict` from the substrate error alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn rfc023_disjoint_retryable_strict_conflict_reprepares_without_key_conflict() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap());
    // Open the publisher before manufacturing physical drift. A normal open
    // after the raw append would correctly refuse the uncovered HEAD.
    let mut publisher = Omnigraph::open(&uri).await.unwrap();
    let person_uri = node_table_uri(&db, "Person").await;
    let probes = MergeWriteProbes::default();

    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::FORK_BEFORE_CLASSIFY);
    let writer_db = Arc::clone(&db);
    let writer_probes = probes.clone();
    let writer = tokio::spawn(async move {
        with_merge_write_probes(
            writer_probes,
            writer_db.load(
                "main",
                r#"{"type":"Person","data":{"name":"strict-a","score":2}}"#,
                LoadMode::Append,
            ),
        )
        .await
    });

    rendezvous.wait_until_reached().await;
    let mut raw_person = Dataset::open(&person_uri).await.unwrap();
    let schema = Arc::new(Schema::from(raw_person.schema()));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "name", "score"],
        "raw disjoint-conflict injector is schema-specific"
    );
    let foreign = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["foreign-disjoint"])),
            Arc::new(StringArray::from(vec!["foreign-disjoint"])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();
    helpers::lance_append_inline(&mut raw_person, foreign).await;
    publisher
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
        .await
        .unwrap();
    rendezvous.release();

    let outcome = writer.await.unwrap();
    outcome.expect(
        "a disjoint retryable substrate conflict must reprepare instead of becoming KeyConflict",
    );
    assert_eq!(
        probes.stage_merge_insert_calls(),
        2,
        "the stale strict attempt must be abandoned and staged again from fresh authority"
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());

    let observer = Omnigraph::open(&uri).await.unwrap();
    let mut names = collect_column_strings(&read_table(&observer, "node:Person").await, "name");
    names.sort();
    assert_eq!(names, ["foreign-disjoint", "strict-a"]);
}

/// Once table 1 of a multi-table operation has committed, a key-fence conflict
/// on table N is no longer effect-free. The writer must preserve the sidecar
/// and return `RecoveryRequired`; converting that conflict into a normal
/// read-set retry would execute the logical operation twice around an owned
/// durable effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn rfc023_table_n_conflict_after_table_1_keeps_recovery_ownership() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Omnigraph::init(&uri, SCHEMA_V2_ADDED_TYPE).await.unwrap());

    let before = helpers::snapshot_main(&db).await.unwrap();
    let table_fixtures = [
        (
            "node:Person",
            "Person",
            "person-race",
            node_table_uri(&db, "Person").await,
        ),
        (
            "node:Company",
            "Company",
            "company-race",
            node_table_uri(&db, "Company").await,
        ),
    ];

    let probes = MergeWriteProbes::default();
    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_TABLE_COMMIT);
    let writer_db = Arc::clone(&db);
    let writer_probes = probes.clone();
    let writer = tokio::spawn(async move {
        with_merge_write_probes(
            writer_probes,
            writer_db.load(
                "main",
                r#"{"type":"Person","data":{"name":"person-race"}}
{"type":"Company","data":{"name":"company-race"}}"#,
                LoadMode::Merge,
            ),
        )
        .await
    });

    rendezvous.wait_until_reached().await;
    let operation_id = single_sidecar_operation_id(dir.path());

    let mut committed_by_a = Vec::new();
    let mut not_yet_committed = Vec::new();
    for fixture in &table_fixtures {
        let expected = before.entry(fixture.0).unwrap().table_version;
        let head = Dataset::open(&fixture.3)
            .await
            .unwrap()
            .latest_version_id()
            .await
            .unwrap();
        match head.checked_sub(expected) {
            Some(1) => committed_by_a.push(fixture),
            Some(0) => not_yet_committed.push(fixture),
            other => panic!(
                "{}: expected parked physical HEAD at base or base+1, got delta {other:?}",
                fixture.0
            ),
        }
    }
    assert_eq!(
        (committed_by_a.len(), not_yet_committed.len()),
        (1, 1),
        "the failpoint must park immediately after exactly one table effect"
    );

    let loser = not_yet_committed[0];
    commit_raw_fenced_name_row(&loser.3, loser.2).await;
    rendezvous.release();

    let err = writer
        .await
        .unwrap()
        .expect_err("table-N key conflict after table 1 must require recovery");
    assert!(
        matches!(
            err,
            OmniError::RecoveryRequired {
                operation_id: ref actual,
                ..
            } if actual == &operation_id
        ),
        "post-effect conflict must retain and identify its recovery operation: {err:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![operation_id],
        "post-effect conflict must not delete or replace the owning sidecar"
    );
    assert_eq!(
        probes.stage_merge_insert_calls(),
        2,
        "the complete two-table batch must stage once; RecoveryRequired must not replay it"
    );

    let after = helpers::snapshot_main(&db).await.unwrap();
    for fixture in &table_fixtures {
        let expected = before.entry(fixture.0).unwrap().table_version;
        assert_eq!(
            after.entry(fixture.0).unwrap().table_version,
            expected,
            "the failed operation must publish neither table"
        );
        let head = Dataset::open(&fixture.3)
            .await
            .unwrap()
            .latest_version_id()
            .await
            .unwrap();
        assert_eq!(
            head,
            expected + 1,
            "{} must retain one physical effect for exact recovery classification",
            fixture.0
        );
    }
}

/// RFC-022 coarse OCC must protect the *validated plan*, not only the table
/// version handed to Lance. Writer A validates that an email is free and parks
/// after staging but before the branch effect gate. Writer B then commits the
/// same `@unique` email under a different key. Releasing A must discard the
/// stale attempt and rerun validation against B's commit; merely refreshing
/// A's expected table version would publish an invalid duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn mutation_revalidates_unique_after_pre_effect_authority_change() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = std::sync::Arc::new(Omnigraph::init(uri, OCC_UNIQUE_SCHEMA).await.unwrap());

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let writer_a_db = std::sync::Arc::clone(&db);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .mutate(
                "main",
                OCC_UNIQUE_MUTATIONS,
                "insert_user",
                &params(&[("$name", "stale-plan"), ("$email", "winner@example.com")]),
            )
            .await
    });

    rendezvous.wait_until_reached().await;
    db.mutate(
        "main",
        OCC_UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "winner"), ("$email", "winner@example.com")]),
    )
    .await
    .expect("the second writer commits while the first attempt is parked pre-effect");
    let winner_manifest_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:User")
        .unwrap()
        .table_version;
    let user_uri = node_table_uri(&db, "User").await;
    let winner_lance_head = lance::Dataset::open(&user_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("the stale attempt must be replanned and fail @unique validation");
    assert!(
        err.to_string().contains("@unique violation on User.email"),
        "expected fresh @unique validation, got: {err}"
    );

    let final_manifest_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:User")
        .unwrap()
        .table_version;
    let final_lance_head = lance::Dataset::open(&user_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    assert_eq!(
        (final_manifest_pin, final_lance_head),
        (winner_manifest_pin, winner_lance_head),
        "the rejected stale attempt must not move either manifest pin or Lance HEAD"
    );

    assert_eq!(count_rows(&db, "node:User").await, 1);
    let users = read_table(&db, "node:User").await;
    assert_eq!(collect_column_strings(&users, "name"), vec!["winner"]);
}

/// The coarse token is branch-wide: a commit to a table that the prepared
/// mutation does not write can still invalidate schema/cardinality/RI inputs.
/// A strict update therefore reports `ReadSetChanged` before moving its Person
/// table HEAD when a disjoint Company commit wins during preparation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn strict_mutation_rejects_disjoint_head_change_before_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(helpers::init_and_load(&dir).await);

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let writer_a_db = std::sync::Arc::clone(&db);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .mutate(
                "main",
                OCC_DISJOINT_MUTATIONS,
                "set_age",
                &helpers::mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
            )
            .await
    });

    rendezvous.wait_until_reached().await;
    db.mutate(
        "main",
        OCC_DISJOINT_MUTATIONS,
        "insert_company",
        &params(&[("$name", "ConcurrentCo")]),
    )
    .await
    .expect("the disjoint Company insert commits while Person update is parked");

    let winner_person_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let person_uri = node_table_uri(&db, "Person").await;
    let winner_person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("strict stale read set must fail rather than auto-reprepare");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected a typed manifest conflict");
    };
    assert!(matches!(
        manifest_err.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "graph_head:main"
    ));

    let final_person_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let final_person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    assert_eq!(
        (final_person_pin, final_person_head),
        (winner_person_pin, winner_person_head),
        "strict rejection must happen before any Person table effect"
    );
}

/// The load adapter shares the same prepared-write boundary as mutations.
/// Append is retryable, but a retry means rebuilding and revalidating the
/// whole attempt; it must never mean rebasing an already-validated batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn append_load_revalidates_unique_after_pre_effect_authority_change() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = std::sync::Arc::new(Omnigraph::init(uri, OCC_UNIQUE_SCHEMA).await.unwrap());

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let writer_a_db = std::sync::Arc::clone(&db);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .load(
                "main",
                r#"{"type":"User","data":{"name":"stale-plan","email":"winner@example.com"}}"#,
                LoadMode::Append,
            )
            .await
    });

    rendezvous.wait_until_reached().await;
    db.load(
        "main",
        r#"{"type":"User","data":{"name":"winner","email":"winner@example.com"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("the second load commits while the first attempt is parked pre-effect");
    let winner_manifest_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:User")
        .unwrap()
        .table_version;
    let user_uri = node_table_uri(&db, "User").await;
    let winner_lance_head = lance::Dataset::open(&user_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("the stale append must be rebuilt and fail @unique validation");
    assert!(
        err.to_string().contains("@unique violation on User.email"),
        "expected fresh @unique validation, got: {err}"
    );

    let final_manifest_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:User")
        .unwrap()
        .table_version;
    let final_lance_head = lance::Dataset::open(&user_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    assert_eq!(
        (final_manifest_pin, final_lance_head),
        (winner_manifest_pin, winner_lance_head),
        "the rejected stale load must not move either manifest pin or Lance HEAD"
    );

    assert_eq!(count_rows(&db, "node:User").await, 1);
    let users = read_table(&db, "node:User").await;
    assert_eq!(collect_column_strings(&users, "name"), vec!["winner"]);
}

/// Overwrite is strict because its replacement image was computed from the
/// captured branch state. Even a disjoint graph commit invalidates that coarse
/// read token, and rejection must happen before the overwritten table moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn overwrite_load_rejects_disjoint_head_change_before_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(helpers::init_and_load(&dir).await);

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let writer_a_db = std::sync::Arc::clone(&db);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .load(
                "main",
                r#"{"type":"Person","data":{"name":"Alice","age":31}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Person","data":{"name":"Charlie","age":35}}
{"type":"Person","data":{"name":"Diana","age":28}}"#,
                LoadMode::Overwrite,
            )
            .await
    });

    rendezvous.wait_until_reached().await;
    db.mutate(
        "main",
        OCC_DISJOINT_MUTATIONS,
        "insert_company",
        &params(&[("$name", "ConcurrentCo")]),
    )
    .await
    .expect("the disjoint Company insert commits while overwrite is parked");

    let winner_person_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let person_uri = node_table_uri(&db, "Person").await;
    let winner_person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("strict overwrite must reject the changed read set");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected a typed manifest conflict");
    };
    assert!(matches!(
        manifest_err.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "graph_head:main"
    ));

    let final_person_pin = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let final_person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .latest_version_id()
        .await
        .unwrap();
    assert_eq!(
        (final_person_pin, final_person_head),
        (winner_person_pin, winner_person_head),
        "overwrite rejection must happen before any Person table effect"
    );
}

/// Stage A's entry-point heal and Stage C's effect gate are separated by
/// reclaimable preparation. B can therefore observe a clean graph, stage, and
/// pause while A leaves an EffectsConfirmed sidecar behind. Once B acquires
/// the shared gates it must attribute the barrier to A's exact operation — not
/// commit a second effect and not collapse the condition into generic manifest
/// drift.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn follower_after_initial_heal_reports_exact_pending_recovery_operation() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(helpers::init_and_load(&dir).await);
    let db_a = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let person_uri = node_table_uri(&db_a, "Person").await;
    let initial_manifest_pin = helpers::snapshot_main(&db_a)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;

    // B passes its initial (empty) recovery heal, stages from the old pin, and
    // pauses before acquiring the effect gates.
    let before_effect =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let follower_db = std::sync::Arc::clone(&db_b);
    let follower = tokio::spawn(async move {
        follower_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Follower")], &[("$age", 44)]),
            )
            .await
    });
    before_effect.wait_until_reached().await;

    // A passes through the same pre-effect point (only the rendezvous's first
    // arrival parks), commits its exact Lance transaction, confirms the v3
    // sidecar, then fails before manifest visibility.
    {
        let _post_effect_failure =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        let err = db_a
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Interrupted")], &[("$age", 55)]),
            )
            .await
            .expect_err("A must stop after confirming its physical effect");
        assert!(
            err.to_string()
                .contains("mutation.post_finalize_pre_publisher"),
            "unexpected A failure: {err}"
        );

        let operation_id = single_sidecar_operation_id(dir.path());
        let sidecar: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(
                dir.path()
                    .join("__recovery")
                    .join(format!("{operation_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            sidecar["protocol_v3"]["effect_phase"], "EffectsConfirmed",
            "A must leave the exact confirmed-effect state this race targets"
        );

        let a_effect_head = lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .latest_version_id()
            .await
            .unwrap();
        assert_eq!(
            a_effect_head,
            initial_manifest_pin + 1,
            "A must advance exactly one physical table version"
        );
        assert_eq!(
            helpers::snapshot_main(&db_a)
                .await
                .unwrap()
                .entry("node:Person")
                .unwrap()
                .table_version,
            initial_manifest_pin,
            "A's failed publisher must leave the manifest pin unchanged"
        );

        before_effect.release();
        let follower_err = follower
            .await
            .unwrap()
            .expect_err("B must stop at A's recovery barrier before effects");
        match follower_err {
            OmniError::RecoveryRequired {
                operation_id: actual,
                ..
            } => assert_eq!(
                actual, operation_id,
                "B must name the deterministic sidecar that owns the barrier"
            ),
            other => panic!("expected exact RecoveryRequired attribution, got: {other}"),
        }

        let after_b_head = lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .latest_version_id()
            .await
            .unwrap();
        assert_eq!(
            after_b_head, a_effect_head,
            "B must not advance a second physical effect"
        );
        assert_eq!(
            helpers::snapshot_main(&db_b)
                .await
                .unwrap()
                .entry("node:Person")
                .unwrap()
                .table_version,
            initial_manifest_pin,
            "B must not publish around A's pending recovery"
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()),
            vec![operation_id.clone()],
            "the attributed sidecar must remain available for recovery"
        );
    }

    drop(before_effect);
    drop(db_a);
    drop(db_b);
    let recovered = Omnigraph::open(&uri)
        .await
        .expect("A's confirmed sidecar must remain recoverable");
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let people = read_table(&recovered, "node:Person").await;
    let names = collect_column_strings(&people, "name");
    assert!(names.iter().any(|name| name == "Interrupted"));
    assert!(
        !names.iter().any(|name| name == "Follower"),
        "B's staged-but-uncommitted row must never become visible"
    );
}

/// Separately-opened handles share the root-scoped branch gate. Once A's exact
/// Lance effect is durable, a disjoint B write on the same graph branch must
/// wait through A's manifest publish instead of moving graph_head and forcing
/// A into post-effect recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cross_handle_branch_gate_serializes_post_effect_publish() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    drop(db);

    let db_a = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());

    // B prepares first but pauses before effects. A then commits its Person
    // table effect and pauses before visibility. Releasing B makes it contend
    // for the shared branch gate, which A still holds through Phase D.
    let before_effect =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let after_effect =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER);

    let writer_b_db = std::sync::Arc::clone(&db_b);
    let mut writer_b = tokio::spawn(async move {
        writer_b_db
            .mutate(
                "main",
                OCC_DISJOINT_MUTATIONS,
                "insert_company",
                &params(&[("$name", "VisibleCo")]),
            )
            .await
    });
    before_effect.wait_until_reached().await;

    let writer_a_db = std::sync::Arc::clone(&db_a);
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "Doomed")], &[("$age", 55)]),
            )
            .await
    });
    after_effect.wait_until_reached().await;

    before_effect.release();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut writer_b)
            .await
            .is_err(),
        "B must wait while A holds the shared branch gate after its table effect"
    );
    after_effect.release();

    writer_a
        .await
        .unwrap()
        .expect("A must publish normally while B waits");
    writer_b
        .await
        .unwrap()
        .expect("B must reprepare against A's published authority and then commit");

    assert_eq!(
        count_rows(&db_a, "node:Person").await,
        5,
        "A's Person insert must remain visible"
    );
    assert_eq!(
        count_rows(&db_b, "node:Company").await,
        3,
        "B's disjoint Company insert must publish after A"
    );
    assert!(
        !std::path::Path::new(&uri).join("__recovery").exists()
            || std::fs::read_dir(std::path::Path::new(&uri).join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "both successful writers must delete their recovery intents"
    );
}

// Atomic schema apply: schema apply writes staging files first, then commits
// the manifest, then renames staging → final. Tests below inject crashes at
// the two boundaries and assert that reopening the graph yields a consistent
// state.

#[tokio::test]
#[serial]
async fn schema_apply_pre_commit_crash_rolls_forward_via_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_STAGING_WRITE, "return");
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
#[serial]
async fn schema_apply_recovers_partial_schema_promotion_after_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT, "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_manifest_commit"),
            "got: {}",
            err
        );
    }

    // ReadOnly must remain non-mutating, but it also must not combine the
    // already-published v7 manifest delta with the old live schema contract.
    // It fails closed until a read-write open performs promotion/recovery.
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{}.json", single_sidecar_operation_id(dir.path())));

    // Even though read-only open historically ignores corrupt recovery files,
    // it must fail closed when schema-staging artifacts mean that the corrupt
    // file could be the only proof of a committed-but-unpromoted SchemaApply.
    // Keep the valid body so the rest of this test can exercise recovery.
    let valid_sidecar = std::fs::read_to_string(&sidecar_path).unwrap();
    std::fs::write(&sidecar_path, "{not json").unwrap();
    let corrupt_read_only_error = match Omnigraph::open_read_only(&uri).await {
        Ok(_) => panic!("read-only open must refuse corrupt intent plus schema staging"),
        Err(error) => error,
    };
    assert!(matches!(
        corrupt_read_only_error,
        OmniError::RecoveryRequired { .. }
    ));
    assert!(
        corrupt_read_only_error
            .to_string()
            .contains("schema-staging artifacts alongside unparseable recovery sidecar")
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap(),
        SCHEMA_V1,
        "the corrupt-sidecar guard must remain non-mutating"
    );
    std::fs::write(&sidecar_path, valid_sidecar).unwrap();

    let read_only_error = match Omnigraph::open_read_only(&uri).await {
        Ok(_) => panic!("read-only open must refuse a committed-but-unpromoted SchemaApply"),
        Err(error) => error,
    };
    assert!(matches!(
        read_only_error,
        OmniError::RecoveryRequired { .. }
    ));
    assert!(sidecar_path.exists());
    assert!(dir.path().join("_schema.pg.staging").exists());
    assert!(dir.path().join("_schema.ir.json.staging").exists());
    assert!(dir.path().join("__schema_state.json.staging").exists());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap(),
        SCHEMA_V1,
        "the read-only coherence guard must not promote schema files"
    );

    // Simulate a crash partway through promotion: source reached its final
    // name, while the exact IR/state contract remains staged. Recovery must
    // validate the mixed state as one target identity and finish it, rather
    // than rejecting the already-visible fixed manifest outcome.
    std::fs::rename(
        dir.path().join("_schema.pg.staging"),
        dir.path().join("_schema.pg"),
    )
    .unwrap();
    assert!(!dir.path().join("_schema.pg.staging").exists());
    assert!(dir.path().join("_schema.ir.json.staging").exists());
    assert!(dir.path().join("__schema_state.json.staging").exists());

    // Reopen — the fixed manifest outcome is visible, so recovery completes
    // the remaining promotion and the live schema matches v2.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

/// The applying handle's coordinator observes the fixed manifest commit before
/// schema files and the catalog ArcSwap are promoted. Query capture joins the
/// schema gate so it cannot pair that new snapshot with the old catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn schema_apply_live_query_waits_for_coherent_schema_publication() {
    const SCHEMA_V2_WITH_EDGE: &str = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company
"#;
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = std::sync::Arc::new(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());
    let stale_reader = Omnigraph::open(&uri).await.unwrap();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT);

    let apply_db = std::sync::Arc::clone(&db);
    let apply_task = tokio::spawn(async move { apply_db.apply_schema(SCHEMA_V2_WITH_EDGE).await });
    rendezvous.wait_until_reached().await;

    let query_db = std::sync::Arc::clone(&db);
    let mut query_task = tokio::spawn(async move {
        query_db
            .query(
                omnigraph::db::ReadTarget::branch("main"),
                "query people() { match { $p: Person } return { $p.name } }",
                "people",
                &helpers::params(&[]),
            )
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut query_task)
            .await
            .is_err(),
        "query must remain queued while manifest and catalog publication are split"
    );

    rendezvous.release();
    apply_task
        .await
        .unwrap()
        .expect("SchemaApply should finish after publication is released");
    let result = query_task
        .await
        .unwrap()
        .expect("queued query should capture the fully promoted schema view");
    assert_eq!(result.num_rows(), 0);
    assert!(
        db.snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Company")
            .is_some()
    );
    let companies = stale_reader
        .query(
            omnigraph::db::ReadTarget::branch("main"),
            "query companies() { match { $c: Company } return { $c.name } }",
            "companies",
            &helpers::params(&[]),
        )
        .await
        .expect("a pre-apply handle must rebuild its operation-local read catalog");
    assert_eq!(companies.num_rows(), 0);
    assert_eq!(
        stale_reader
            .export_jsonl("main", &["Company".to_string()], &[])
            .await
            .expect("export must use the same operation-local accepted catalog"),
        ""
    );
    assert!(
        stale_reader
            .graph_index()
            .await
            .expect("whole-graph index must enumerate edges from the accepted catalog")
            .csr("WorksAt")
            .is_some(),
        "a pre-apply handle must include the newly accepted edge type"
    );
}

#[tokio::test]
#[serial]
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
#[serial]
async fn recovery_rolls_forward_after_finalize_publisher_failure() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;

    // Setup: trigger the residual.
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");

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
        RecoveryExpectation::RolledForwardOriginalLineage {
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

/// Regression for iss-schema-apply-reopen-recovery-race: the open-time
/// recovery sweep's roll-forward must CONVERGE (not fatally error the open)
/// when a concurrent writer advances the manifest past the sidecar's pin
/// during the classify→publish window.
///
/// Two concurrent `Omnigraph::open` sweeps race the same pending sidecar.
/// One is parked at `recovery.before_roll_forward_publish` (after it has
/// classified `RolledPastExpected`, before its publish CAS); the other falls
/// through, rolls the sidecar forward (manifest v → v+1), and deletes it. The
/// parked sweep then loses its publish CAS at the now-stale `expected = v`.
/// The manifest already reached the sidecar's goal, so this is convergence,
/// not a logical conflict — the open must succeed, not panic with
/// `ExpectedVersionMismatch`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn open_sweep_roll_forward_converges_when_manifest_advances_concurrently() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Setup: leave one pending sidecar (node:Person at Lance v+1, manifest v).
    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap_err();
    }
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        1,
        "exactly one pending sidecar must persist for the sweep to roll forward"
    );

    // Park the FIRST sweep to reach the publish window; later arrivals fall
    // through. wait_until_reached gates the second open so it is guaranteed
    // to be the one that converges the sidecar.
    let rv =
        helpers::failpoint::Rendezvous::park_first(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH);

    let uri_parked = uri.clone();
    let parked_open = tokio::spawn(async move { Omnigraph::open(&uri_parked).await });
    rv.wait_until_reached().await;

    // A concurrent open rolls the sidecar forward (manifest v → v+1) and
    // deletes it, advancing the manifest past the parked sweep's pin.
    let converging_open = Omnigraph::open(&uri)
        .await
        .expect("the second open's sweep should roll the sidecar forward and succeed");
    assert_eq!(
        helpers::count_rows(&converging_open, "node:Person").await,
        1,
        "the converging open must publish the rolled-forward Person row"
    );

    // Release the parked sweep: its publish CAS finds the manifest already at
    // the goal. It must converge, not fail the open.
    rv.release();
    parked_open
        .await
        .expect("the parked open task must not panic")
        .expect(
            "the open-time sweep must converge when the manifest already reached \
             the sidecar's goal, not fail the open with ExpectedVersionMismatch",
        );

    // The sidecar is gone and the graph is readable and consistent.
    let recovery_dir = dir.path().join("__recovery");
    if recovery_dir.exists() {
        assert_eq!(
            std::fs::read_dir(&recovery_dir).unwrap().count(),
            0,
            "the sidecar must be gone after both sweeps converge"
        );
    }
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);

    // Exactly one RolledForward audit row for this recovery event: the loser's
    // convergence path must NOT append a duplicate once the winner already
    // recorded the audit and deleted the sidecar (append-idempotent per
    // operation_id). Two rows here would be the duplicate-audit regression.
    let kinds = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(
        kinds.len(),
        1,
        "exactly one recovery audit row expected after concurrent convergence, got {kinds:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn inline_delete_conflict_writes_sidecar_before_rejecting() {
    use std::sync::Arc;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(helpers::init_and_load(&dir).await);

    let pre_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let pre_person_pin = pre_snapshot.entry("node:Person").unwrap().table_version;
    let person_uri = node_table_uri(&db, "Person").await;

    {
        // Park the delete at the primary-delete point. The concurrent update
        // then lands deterministically before the delete resumes, so the
        // delete's manifest CAS is guaranteed stale — no retry loop, no sleep.
        let rv = helpers::failpoint::Rendezvous::park_first(
            names::MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE,
        );

        let del_db = Arc::clone(&db);
        let delete = tokio::spawn(async move {
            let delete_params = helpers::params(&[("$name", "Alice")]);
            del_db
                .mutate("main", MUTATION_QUERIES, "remove_person", &delete_params)
                .await
        });

        rv.wait_until_reached().await;

        // Concurrent update lands while the delete is parked.
        let mut concurrent = Omnigraph::open_read_only(&uri).await.unwrap();
        mutate_main(
            &mut concurrent,
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
        )
        .await
        .expect("concurrent update must land while delete is paused");

        rv.release();

        let err = delete.await.unwrap().unwrap_err();
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
#[serial]
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

        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
        RecoveryExpectation::RolledForwardOriginalLineage {
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
#[serial]
async fn recovery_rolls_forward_load_overwrite() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let parent_commit_id;

    {
        let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
        // Seed Persons only (no edges): the fault-injected step below overwrites
        // node:Person down to a single row, and a per-table overwrite that drops
        // Persons referenced by seeded Knows/WorksAt edges is now rejected as an
        // orphan during validation — before it ever reaches the post-finalize
        // failpoint this test drives. Keeping the seed edge-free makes the
        // overwrite a clean single-table roll-forward, which is what's under test.
        load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":30}}\n\
             {\"type\":\"Person\",\"data\":{\"name\":\"Bob\",\"age\":31}}\n",
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        parent_commit_id = branch_head_commit_id(dir.path(), "main").await.unwrap();

        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
        RecoveryExpectation::RolledForwardOriginalLineage {
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
#[serial]
async fn recovery_rolls_forward_ensure_indices_on_feature_branch() {
    use lance::index::DatasetIndexExt;
    use omnigraph::loader::{LoadMode, load_jsonl};

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

    // RFC-022 writes no longer build declared indexes inline. Materialize the
    // feature index once so the setup below can deliberately drop it and test
    // ensure_indices recovery rather than first materialization.
    db.ensure_indices_on("feature").await.unwrap();

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
    let person_uri = node_table_uri(&db, "Person").await;
    let mut ds = helpers::open_dataset_head(&person_uri, Some("feature")).await;
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
        let _failpoint = ScopedFailPoint::new(
            names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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
        RecoveryExpectation::RolledForwardOriginalLineage {
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

    let mut db = Omnigraph::open(&uri).await.unwrap();
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

    // Repeat the same confirmed residual on the feature branch, but keep this
    // handle alive. The entry barrier must finish the roll-forward-eligible v8
    // intent before the retry captures another base or plans another index.
    let mut ds = helpers::open_dataset_head(&person_uri, Some("feature")).await;
    ds.drop_index("id_idx").await.unwrap();
    db.failpoint_publish_table_head_without_index_rebuild_for_test(
        "feature",
        "node:Person",
        Some("feature"),
    )
    .await
    .unwrap();
    let same_handle_parent_commit_id = branch_head_commit_id(dir.path(), "feature").await.unwrap();

    let same_handle_operation_id;
    {
        let _failpoint = ScopedFailPoint::new(
            names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        let err = db.ensure_indices_on("feature").await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: ensure_indices.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );
        same_handle_operation_id = single_sidecar_operation_id(dir.path());
    }

    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{same_handle_operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(
        sidecar["protocol_v8"]["effect_phase"], "EffectsConfirmed",
        "the same-handle retry fixture requires a roll-forward-eligible v8 intent",
    );

    let pending = db
        .ensure_indices_on("feature")
        .await
        .expect("same-handle retry must heal the confirmed index intent before repreparing");
    assert!(
        pending.is_empty(),
        "the healed feature index should leave no deferred index work"
    );
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "the entry barrier must finalize the prior confirmed intent before continuing",
    );
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &same_handle_operation_id,
        RecoveryExpectation::RolledForwardOriginalLineage {
            tables: vec![
                TableExpectation::branch("node:Person", "feature")
                    .expected_main_manifest_pin(main_person_pin)
                    .expected_recovery_parent_commit_id(same_handle_parent_commit_id),
            ],
        },
    )
    .await
    .unwrap();
}

/// Even when every planned CreateIndex transaction landed exactly, a v8
/// sidecar that is still Armed is rollback-only. Recovery must not infer the
/// intended manifest delta from complete-looking physical state.
#[tokio::test]
#[serial]
async fn ensure_indices_complete_armed_effects_roll_back() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    db.apply_schema(&indexed_schema).await.unwrap();

    let operation_id;
    {
        let _failpoint =
            ScopedFailPoint::new(names::ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM, "return");
        let err = db
            .ensure_indices()
            .await
            .expect_err("failpoint must stop after exact effects but before confirmation");
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
        operation_id = single_sidecar_operation_id(dir.path());
    }

    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(
        sidecar["protocol_v8"]["effect_phase"], "Armed",
        "the complete-effect rollback fixture requires Armed v8 ownership",
    );
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Armed exact index effects must roll back on Full recovery");
    drop(recovered);
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![TableExpectation::main("node:Person")],
        },
    )
    .await
    .unwrap();

    let recovered = Omnigraph::open(&uri).await.unwrap();
    recovered
        .ensure_indices()
        .await
        .expect("clean retry must rebuild the compensated index batch");
}

/// A partial Armed v8 intent can leave one planned table still missing its
/// index. A retry must reject that recovery requirement at entry, before it
/// stages the remaining table's immutable artifacts.
#[tokio::test]
#[serial]
async fn ensure_indices_entry_barrier_refuses_partial_armed_before_staging() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let operation_id;
    {
        let _failpoint = ScopedFailPoint::new(names::ENSURE_INDICES_POST_TABLE_EFFECT, "return");
        let err = db
            .ensure_indices()
            .await
            .expect_err("failpoint must stop after the first of two table effects");
        assert!(matches!(err, OmniError::RecoveryRequired { .. }));
        operation_id = single_sidecar_operation_id(dir.path());
    }

    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(
        sidecar["protocol_v8"]["effect_phase"], "Armed",
        "a partial table-effect failure must remain rollback-only",
    );
    assert_eq!(
        sidecar["tables"].as_array().map(Vec::len),
        Some(2),
        "the fixture must pin two planned table effects so one remains to stage",
    );

    let snapshot = helpers::snapshot_main(&db).await.unwrap();
    let mut effected_tables = Vec::new();
    for (table_key, type_name) in [("node:Person", "Person"), ("node:Company", "Company")] {
        let manifest_pin = snapshot.entry(table_key).unwrap().table_version;
        let table_uri = node_table_uri(&db, type_name).await;
        let lance_head = helpers::open_dataset_head(&table_uri, None)
            .await
            .version()
            .version;
        if lance_head > manifest_pin {
            effected_tables.push(table_key);
        }
    }
    assert_eq!(
        effected_tables.len(),
        1,
        "the first-table failpoint must leave exactly one owned index effect",
    );

    let retry_error = {
        let _failpoint =
            ScopedFailPoint::new(names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE, "return");
        db.ensure_indices()
            .await
            .expect_err("rollback-only ownership must refuse before remaining index staging")
    };
    match retry_error {
        OmniError::RecoveryRequired {
            operation_id: actual,
            ..
        } => assert_eq!(actual, operation_id),
        other => panic!("expected exact pre-staging RecoveryRequired attribution, got {other}"),
    }
    assert_eq!(
        single_sidecar_operation_id(dir.path()),
        operation_id,
        "the refused retry must leave only the original Armed intent",
    );
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must compensate the exact partial index effect");
    drop(recovered);
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![TableExpectation::main(effected_tables[0])],
        },
    )
    .await
    .unwrap();

    let recovered = Omnigraph::open(&uri).await.unwrap();
    recovered
        .ensure_indices()
        .await
        .expect("clean retry must build both compensated and never-committed indexes");
}

/// A foreign process can publish a disjoint table after v8 confirmation but
/// before the exact graph-head CAS. Full recovery must compensate only the
/// unpublished index effect and descend from the winning graph commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ensure_indices_post_effect_disjoint_winner_is_preserved() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    db.apply_schema(&indexed_schema).await.unwrap();
    drop(db);

    let index_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut winner_db = Omnigraph::open(&uri).await.unwrap();
    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );
    let index_handle = std::sync::Arc::clone(&index_db);
    let index_task = tokio::spawn(async move { index_handle.ensure_indices().await });
    rendezvous.wait_until_reached().await;

    let company_uri = node_table_uri(&winner_db, "Company").await;
    let mut raw_company = lance::Dataset::open(&company_uri).await.unwrap();
    helpers::lance_delete_inline(&mut raw_company, "1 = 2").await;
    let winner_company_version = raw_company.version().version;
    winner_db
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Company", None)
        .await
        .unwrap();
    let winner_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    rendezvous.release();

    let operation_id = match index_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("post-effect authority loss must require recovery: {other}"),
    };
    drop(index_db);
    drop(winner_db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must compensate around the disjoint winner");
    drop(recovered);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![
                TableExpectation::main("node:Person")
                    .expected_recovery_parent_commit_id(winner_head),
            ],
        },
    )
    .await
    .unwrap();
    let recovered = Omnigraph::open(&uri).await.unwrap();
    let main = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        main.entry("node:Company").unwrap().table_version,
        winner_company_version,
        "index compensation must preserve the disjoint winner"
    );
}

/// A same-table foreign commit can bury the exact CreateIndex transaction.
/// Recovery must retain the sidecar and leave the winning manifest/HEAD alone;
/// restoring through that winner would be destructive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ensure_indices_post_effect_same_table_winner_fails_closed() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    db.apply_schema(&indexed_schema).await.unwrap();
    drop(db);

    let index_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut winner_db = Omnigraph::open(&uri).await.unwrap();
    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );
    let index_handle = std::sync::Arc::clone(&index_db);
    let index_task = tokio::spawn(async move { index_handle.ensure_indices().await });
    rendezvous.wait_until_reached().await;

    let person_uri = node_table_uri(&winner_db, "Person").await;
    let mut raw_person = lance::Dataset::open(&person_uri).await.unwrap();
    helpers::lance_delete_inline(&mut raw_person, "1 = 2").await;
    let winner_lance_head = raw_person.version().version;
    winner_db
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
        .await
        .unwrap();
    let winner_manifest_version = winner_db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    rendezvous.release();

    let operation_id = match index_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("same-table authority loss must require recovery: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    drop(index_db);

    let error = match Omnigraph::open(&uri).await {
        Ok(_) => panic!("Full recovery must refuse to restore through a buried index effect"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("buried")
            || error.to_string().contains("foreign")
            || error.to_string().contains("unverifiable"),
        "unexpected fail-closed error: {error}"
    );
    assert!(sidecar_path.exists());
    assert_eq!(
        winner_db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        winner_manifest_version
    );
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        winner_lance_head
    );
}

/// EnsureIndices first-touch ordering is sidecar-before-ref. A crash in that
/// window is a valid no-effect state: Full recovery must accept the missing
/// target ref, retire the intent, and leave the child inheriting its parent
/// until a clean retry performs real index work.
#[tokio::test]
#[serial]
async fn ensure_indices_first_touch_crash_before_ref_recovers_cleanly() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
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
        &mixed_params(&[("$name", "feature-row")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    db.branch_create_from(omnigraph::db::ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    // Pre-arm ownership fence: an unregistered target ref is foreign/orphaned
    // state, never something a new loose sidecar may claim.
    let person_uri = node_table_uri(&db, "Person").await;
    let mut person = lance::Dataset::open(&person_uri).await.unwrap();
    let feature_head = person
        .checkout_branch("feature")
        .await
        .unwrap()
        .version()
        .version;
    person
        .create_branch("experiment", ("feature", feature_head), None)
        .await
        .unwrap();
    let orphan_error = db
        .ensure_indices_on("experiment")
        .await
        .expect_err("pre-existing target ref must be refused before recovery is armed");
    assert!(
        orphan_error
            .to_string()
            .contains("refusing to claim unowned physical state"),
        "unexpected orphan-ref refusal: {orphan_error}"
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    person.delete_branch("experiment").await.unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new(names::ENSURE_INDICES_POST_SIDECAR_PRE_FORK, "return");
        db.ensure_indices_on("experiment")
            .await
            .expect_err("failpoint must fire after sidecar and before target ref creation");
    }
    let operation_id = single_sidecar_operation_id(dir.path());
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must accept the missing first-touch target ref");
    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists()
    );
    let inherited = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("experiment"))
        .await
        .unwrap();
    assert_eq!(
        inherited
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );

    recovered.ensure_indices_on("experiment").await.unwrap();
    let owned = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("experiment"))
        .await
        .unwrap();
    assert_eq!(
        owned.entry("node:Person").unwrap().table_branch.as_deref(),
        Some("experiment")
    );
}

/// Mixed first-touch recovery must clean only untouched refs. A table whose
/// derived index HEAD moved is not a no-effect fork merely because the legacy
/// EnsureIndices payload lacks transaction identities; Full recovery restores
/// it through the normal loose-table rollback while accepting a missing sibling
/// ref.
#[tokio::test]
#[serial]
async fn ensure_indices_mixed_first_touch_rollback_does_not_delete_moved_ref() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    db.branch_create("feature").await.unwrap();
    db.load(
        "feature",
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n\
         {\"type\":\"Company\",\"data\":{\"name\":\"acme\"}}",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create_from(omnigraph::db::ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    {
        let _failpoint = ScopedFailPoint::new(names::ENSURE_INDICES_POST_TABLE_EFFECT, "return");
        db.ensure_indices_on("experiment")
            .await
            .expect_err("failpoint must stop after the first first-touch table effect");
    }
    let operation_id = single_sidecar_operation_id(dir.path());
    drop(db);

    {
        let _failpoint =
            ScopedFailPoint::new(names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT, "return");
        let first_recovery = Omnigraph::open(&uri).await;
        assert!(
            first_recovery.is_err(),
            "first recovery must stop after publishing its fixed rollback"
        );
    }
    assert!(
        dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "interrupted rollback must retain its outcome-bearing sidecar"
    );

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("mixed first-touch rollback retry must finalize its fixed outcome");
    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists()
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "experiment", "node:Person").await,
        1
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "experiment", "node:Company").await,
        1
    );
    drop(recovered);
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledBack"],
        "an interrupted index rollback must never be reclassified as RolledForward"
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
#[serial]
async fn refresh_runs_roll_forward_recovery_in_process() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Setup: trigger the residual (sidecar persists; manifest unchanged).
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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

/// Destructive version GC must not erase the transaction/version history that
/// schema-v3 recovery uses to prove exact effect ownership. A pending sidecar
/// is therefore a graph-wide cleanup barrier, checked before orphan reclaim or
/// any per-table fault-isolated GC begins.
#[tokio::test]
#[serial]
async fn cleanup_refuses_pending_v3_sidecar_before_version_gc() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        mutate_main(
            &mut db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "pending-gc")], &[("$age", 22)]),
        )
        .await
        .expect_err("failpoint must leave a confirmed v3 recovery intent");
    }

    let person_uri = node_table_uri(&db, "Person").await;
    let before = lance::Dataset::open(&person_uri).await.unwrap();
    let before_head = before.version().version;
    let before_versions = before.versions().await.unwrap().len();
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        1,
        "test precondition: one v3 sidecar is pending"
    );

    let err = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect_err("cleanup must not outrun recovery");
    assert!(
        err.to_string()
            .contains("cleanup requires a clean recovery state"),
        "unexpected cleanup error: {err}"
    );

    let after = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(after.version().version, before_head);
    assert_eq!(after.versions().await.unwrap().len(), before_versions);
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        1,
        "refused cleanup must leave recovery ownership intact"
    );

    drop(db);
    Omnigraph::open(&uri)
        .await
        .expect("read-write reopen resolves the retained sidecar");
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
#[serial]
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
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
#[serial]
async fn sidecar_write_failure_aborts_load_with_no_head_advance() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let pre_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
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
#[serial]
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
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
#[serial]
async fn record_audit_failure_after_roll_forward_converges_on_next_write() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Pending sidecar with real drift.
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_RECORD_AUDIT, "return");
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
#[serial]
async fn sidecar_list_failure_fails_write_and_open_loudly_then_clears() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Pending sidecar via the usual finalize → publisher failure.
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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

    let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");

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
#[serial]
async fn sidecar_delete_failure_keeps_write_success_and_next_write_heals() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
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
#[serial]
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

    let person_uri = node_table_uri(&db, "Person").await;
    let pre_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
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
#[serial]
async fn mutation_after_finalize_publisher_failure_heals_without_reopen() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
#[serial]
async fn schema_apply_after_finalize_publisher_failure_heals_without_reopen() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
    let desired = format!(
        "{}\nnode Tag {{ name: String @key }}\n",
        helpers::TEST_SCHEMA
    );
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
#[serial]
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
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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
#[serial]
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
    let person_uri = node_table_uri(&db, "Person").await;
    let person_identity = node_table_identity_json(&db, "Person");
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
                "identity": {person_identity},
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

    // Orphan the synthetic sidecar with a raw substrate ref delete. The public
    // branch-control protocol now runs the synchronous recovery barrier and
    // correctly refuses to delete a branch named by pending recovery intent;
    // this test needs the crash/external-state shape that recovery must handle.
    let manifest_uri = format!("{uri}/__manifest");
    let mut manifest = lance::Dataset::open(&manifest_uri).await.unwrap();
    manifest.delete_branch("feature").await.unwrap();

    // First write: the discard path writes its audit row, then the
    // sidecar delete fails (injected). The write fails loudly.
    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
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

/// The closed mutation/load Stage-A barrier owns pending covered drift before
/// the later HEAD guard has to guess between recovery and repair. It reports the
/// exact operation id and recovery action instead of routing an enrolled write
/// toward `repair`.
#[tokio::test]
#[serial]
async fn stage_a_barrier_reports_exact_operation_before_drift_guard() {
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
    let person_identity = node_table_identity_json(&db, "Person");
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
                    "identity":{person_identity},
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
        matches!(
            err,
            OmniError::RecoveryRequired { ref operation_id, .. }
                if operation_id == "01H0000000000000000000LSTF"
        ) && msg.contains("reopen the graph read-write")
            && !msg.contains("omnigraph repair"),
        "the Stage-A barrier must attribute the pending recovery exactly; got: {msg}"
    );
}

/// The other half of the orphan-discard fault matrix: the audit append
/// fails AFTER the recovery commit landed. The retry (keyed on the
/// audit row, the operator-facing record) must converge to exactly one
/// audit row and a consumed sidecar. Lineage and the recovery audit remain two
/// durable writes; a legacy sidecar has no fixed recovery-commit id, so a retry
/// after the lineage CAS but before the audit append may add bounded lineage
/// noise. The operator-facing audit record still lands exactly once.
#[tokio::test]
#[serial]
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
    let person_uri = node_table_uri(&db, "Person").await;
    let person_identity = node_table_identity_json(&db, "Person");
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
                "identity": {person_identity},
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
    // Manufacture the dead-branch recovery shape below the public branch
    // control protocol, which now refuses a ref delete while this sidecar is
    // pending.
    let manifest_uri = format!("{uri}/__manifest");
    let mut manifest = lance::Dataset::open(&manifest_uri).await.unwrap();
    manifest.delete_branch("feature").await.unwrap();

    // First write: the recovery commit lands, then the audit append
    // fails (injected). The write fails loudly; the sidecar survives so
    // the discard is retried with the audit still owed.
    {
        let _failpoint =
            ScopedFailPoint::new(names::RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND, "return");
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
#[serial]
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
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_STAGING_WRITE, "return");
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
#[serial]
async fn heal_does_not_promote_live_schema_apply_staging() {
    use omnigraph::loader::LoadMode;
    use std::sync::Arc;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let db = Arc::new(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());

    // Park the apply right after its staging files land (its sidecar is
    // already on disk from Phase A; the manifest commit has not run).
    let rv = helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);

    let apply_db = Arc::clone(&db);
    let desired = format!(
        "{}\nnode Tag {{ name: String @key }}\n",
        helpers::TEST_SCHEMA
    );
    let apply = tokio::spawn(async move { apply_db.apply_schema(&desired).await });

    // Wait until the apply is parked in the window (staging files written).
    rv.wait_until_reached().await;
    let staging_pg = dir.path().join("_schema.pg.staging");
    assert!(
        staging_pg.exists(),
        "schema apply never reached the paused window"
    );

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
    rv.release();

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
#[serial]
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
    let person_identity = node_table_identity_json(&db, "Person");
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
                    "identity":{person_identity},
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

    // A write attempt while the rollback-eligible sidecar is deferred stops at
    // the synchronous Stage-A barrier, before base capture/effects, and names
    // the exact operation that requires a read-write reopen.
    let err = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Grace")], &[("$age", 50)]),
    )
    .await
    .unwrap_err();
    match err {
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => {
            assert_eq!(operation_id, "01H0000000000000000000RBCK");
            assert!(
                reason.contains("reopen the graph read-write"),
                "barrier must point at the safe recovery path; got: {reason}"
            );
        }
        other => panic!("expected typed RecoveryRequired barrier, got: {other}"),
    }

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
#[serial]
async fn finalize_publisher_residual_does_not_drift_untouched_tables() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();

    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
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

/// Expensive index artifact construction happens before the RFC-022 gates and
/// before the v8 recovery intent is armed. While A is parked after staging its
/// immutable files, B can publish a disjoint graph write. A then loses final
/// authority revalidation, abandons its uncommitted artifacts, and leaves no
/// table movement or recovery sidecar behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ensure_indices_stage_btree_failure_leaves_existing_tables_writable() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();

    // Seed a Person row. The enrolled mutation publishes only its logical data
    // effect; physical index construction remains reconciler-owned.
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
    let person_uri = node_table_uri(&db, "Person").await;
    let person_pin_before = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let person_head_before = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    let db = std::sync::Arc::new(db);

    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
    );
    let writer_a_db = std::sync::Arc::clone(&db);
    let writer_a = tokio::spawn(async move { writer_a_db.ensure_indices().await });
    rendezvous.wait_until_reached().await;

    db.load(
        "main",
        r#"{"type":"Company","data":{"name":"Acme"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("disjoint writer must publish while index artifacts are staged pre-gate");
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("stale index plan must fail final authority revalidation");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected a typed read-set change, got {err}");
    };
    assert!(matches!(
        manifest_err.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "graph_head:main"
    ));
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "pre-gate stale preparation must not arm recovery"
    );
    let person_pin_after = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let person_head_after = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(person_pin_after, person_pin_before);
    assert_eq!(person_head_after, person_head_before);
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

fn schema_with_person_city() -> String {
    helpers::TEST_SCHEMA.replace("    age: I32?\n}", "    age: I32?\n    city: String?\n}")
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
#[serial]
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
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
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

/// A metadata-only SchemaApply still changes the accepted schema contract, so
/// it needs a durable intent even though it has no table pins. A crash before
/// schema staging must roll that empty-table intent back unambiguously.
#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_before_staging_rolls_back_on_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
        let err = db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.before_staging_write"),
            "unexpected error: {err}"
        );
        single_sidecar_operation_id(dir.path())
    };
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("empty-table SchemaApply intent should roll back cleanly");
    drop(recovered);
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack { tables: vec![] },
    )
    .await
    .unwrap();
    assert_no_staging_files(dir.path());
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        !live_schema.contains("age: I32? @index"),
        "a pre-staging crash must retain the old accepted schema"
    );
}

/// A recovery-owned rollback commit also advances the manifest. The durable
/// SchemaApply Phase-C marker—not numeric version movement—must therefore keep
/// a pre-staging crash classified as RolledBack when recovery itself is retried
/// after publishing rollback but before recording its audit.
#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_rollback_retry_never_flips_forward() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");

    {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
        db.apply_schema(&indexed_schema).await.unwrap_err();
    }
    let operation_id = single_sidecar_operation_id(dir.path());
    drop(db);

    {
        let _failpoint =
            ScopedFailPoint::new(names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT, "return");
        let first_recovery = Omnigraph::open(&uri).await;
        assert!(
            first_recovery.is_err(),
            "first recovery must stop after its rollback publish"
        );
    }
    assert!(
        dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "failed audit phase must retain the original sidecar"
    );

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("the next Full recovery must finish the rollback");
    drop(recovered);
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledBack"],
        "rollback retry must never be reclassified or additionally audited as RolledForward"
    );
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(!live_schema.contains("age: I32? @index"));
}

/// The same retry discriminator is required when SchemaApply moved a table
/// before staging. After recovery restores and publishes the table, numeric
/// pins look like a stale roll-forward; the old live schema hash proves the
/// outcome is still RolledBack.
#[tokio::test]
#[serial]
async fn pinned_schema_apply_rollback_retry_never_flips_forward() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let desired = helpers::TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
        db.apply_schema(&desired).await.unwrap_err();
    }
    drop(db);

    {
        let _failpoint =
            ScopedFailPoint::new(names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT, "return");
        let first_recovery = Omnigraph::open(&uri).await;
        assert!(first_recovery.is_err());
    }

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("pinned SchemaApply rollback retry must converge");
    assert_eq!(helpers::count_rows(&recovered, "node:Person").await, 4);
    drop(recovered);
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledBack"],
        "restored table pins must not be mistaken for stale roll-forward"
    );
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(!live_schema.contains("nickname: String?"));
}

/// The other half of the zero-table protocol: once schema staging exists, the
/// same metadata-only intent must roll forward and accept the new contract.
#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_after_staging_rolls_forward_on_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_STAGING_WRITE, "return");
        let err = db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "unexpected error: {err}"
        );
        single_sidecar_operation_id(dir.path())
    };
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("empty-table SchemaApply intent should roll forward cleanly");
    drop(recovered);
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForwardOriginalLineage { tables: vec![] },
    )
    .await
    .unwrap();
    assert_no_staging_files(dir.path());
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        live_schema.contains("age: I32? @index"),
        "an after-staging crash must promote the new accepted schema"
    );
}

/// Schema-file recovery runs before sidecar processing. If it promotes staging
/// and the sweep then crashes, the next pass sees marker=false and no staging;
/// the live target hash must carry the decision forward into the normal
/// manifest roll-forward path.
#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_recovers_after_promotion_prepass_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");

    {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_STAGING_WRITE, "return");
        db.apply_schema(&indexed_schema).await.unwrap_err();
    }
    drop(db);

    {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_POST_LIST_PRE_GATES, "return");
        let interrupted = Omnigraph::open(&uri).await;
        assert!(
            interrupted.is_err(),
            "first recovery must stop after schema promotion and before sidecar processing"
        );
    }

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("live target identity must let the next pass finish roll-forward");
    drop(recovered);
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(live_schema.contains("age: I32? @index"));
}

/// Phase D is outside SchemaApply's logical commit boundary too. With no table
/// pins, recovery must use the durable Phase-C marker plus target schema hash to
/// recognize that the metadata-only commit and promotion are already visible;
/// it must not mislabel the completed apply as RolledBack or wedge the next
/// write waiting for a full reopen.
#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_delete_failure_heals_on_next_write() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
        db.apply_schema(&indexed_schema)
            .await
            .expect("Phase-D delete failure must not fail an already-visible schema apply");
        single_sidecar_operation_id(dir.path())
    };

    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "after-schema-heal")], &[("$age", 36)]),
    )
    .await
    .expect("the next write must heal an already-visible metadata-only apply in process");

    assert!(
        !dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json"))
            .exists(),
        "the next write's entry heal must retire the stale SchemaApply sidecar"
    );
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"],
        "the completed SchemaApply must have exactly one forward audit and no rollback audit"
    );
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        live_schema.contains("age: I32? @index"),
        "healing a stale Phase-D sidecar must retain the accepted schema"
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
}

/// A v9 AddType target is a first-touch effect with an exact version-one
/// transaction identity. Pre-staging rollback owns that dataset, reclaims it,
/// and leaves the target path reusable by a clean retry.
#[tokio::test]
#[serial]
async fn schema_apply_recovery_reclaims_owned_add_type_target_and_retry_succeeds() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();

    {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
        db.apply_schema(SCHEMA_V2_ADDED_TYPE)
            .await
            .expect_err("the pre-staging failpoint must leave the AddType intent pending");
    }
    let company_uri = pending_schema_apply_node_table_uri(&uri, "Company");
    drop(db);
    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery should roll back the pre-staging AddType intent");
    assert!(
        recovered
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Company")
            .is_none(),
        "rollback must not register the orphan target"
    );
    assert!(
        !std::path::Path::new(&company_uri).exists(),
        "exact v9 rollback must reclaim the first-touch dataset it owns"
    );
    assert!(
        !dir.path().join("__recovery").exists()
            || std::fs::read_dir(dir.path().join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "recovery must consume the failed attempt before retry"
    );

    recovered
        .apply_schema(SCHEMA_V2_ADDED_TYPE)
        .await
        .expect("retry must recreate the reclaimed AddType target and publish normally");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Company").await,
        0,
        "the retried AddType must be registered and queryable"
    );
}

/// A strict first-touch SchemaApply create can lose to an independent creator
/// after its v9 sidecar is durable. The foreign dataset is not graph-visible
/// and is not ours to adopt or delete: Full recovery records the fixed rollback
/// outcome, retires only this intent, and preserves the winner byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn schema_apply_first_touch_foreign_winner_is_preserved_not_adopted() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = std::sync::Arc::new(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT);
    let apply_db = std::sync::Arc::clone(&db);
    let apply_task = tokio::spawn(async move { apply_db.apply_schema(SCHEMA_V2_ADDED_TYPE).await });
    rendezvous.wait_until_reached().await;

    let company_uri = pending_schema_apply_node_table_uri(&uri, "Company");
    let foreign_schema =
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "foreign_owner",
            arrow_schema::DataType::Utf8,
            false,
        )]));
    let foreign_batch = arrow_array::RecordBatch::try_new(
        foreign_schema,
        vec![std::sync::Arc::new(arrow_array::StringArray::from(vec![
            "winner",
        ]))],
    )
    .unwrap();
    let reader = arrow_array::RecordBatchIterator::new(
        vec![Ok(foreign_batch.clone())],
        foreign_batch.schema(),
    );
    let foreign = lance::Dataset::write(
        reader,
        &company_uri,
        Some(lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            allow_external_blob_outside_bases: true,
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let foreign_identity = foreign
        .read_transaction()
        .await
        .unwrap()
        .expect("version-one foreign create must retain its transaction")
        .uuid
        .clone();
    assert_eq!(foreign.version().version, 1);
    assert_eq!(foreign.count_rows(None).await.unwrap(), 1);
    drop(foreign);
    rendezvous.release();

    let operation_id = match apply_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("post-arm first-touch loss must require recovery: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    assert!(sidecar_path.exists());
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must preserve a foreign first-touch winner");
    assert_eq!(recovered.schema_source().as_str(), SCHEMA_V1);
    assert!(
        recovered
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Company")
            .is_none(),
        "recovery must never adopt the foreign dataset into the graph manifest"
    );
    assert!(!sidecar_path.exists());
    assert_eq!(recovery_audit_kinds(dir.path()).await, vec!["RolledBack"]);

    let preserved = lance::Dataset::open(&company_uri).await.unwrap();
    assert_eq!(preserved.version().version, 1);
    assert_eq!(preserved.count_rows(None).await.unwrap(), 1);
    assert_eq!(
        preserved
            .read_transaction()
            .await
            .unwrap()
            .as_ref()
            .unwrap()
            .uuid,
        foreign_identity,
        "recovery must leave the foreign transaction identity untouched"
    );
}

#[tokio::test]
#[serial]
async fn schema_apply_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;
    let fixed_commit_id;
    const ACTOR: &str = "schema-v9-recovery-actor";

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
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_AFTER_STAGING_WRITE, "return");
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
        let err = db
            .apply_schema_as(
                v2_schema,
                omnigraph::db::SchemaApplyOptions::default(),
                Some(ACTOR),
            )
            .await
            .unwrap_err();
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
        let sidecar_path = recovery_dir.join(format!("{operation_id}.json"));
        let sidecar: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
        assert_eq!(sidecar["schema_version"], 9);
        assert_eq!(sidecar["actor_id"], ACTOR);
        assert_eq!(
            sidecar["protocol_v7"]["effect_phase"], "EffectsConfirmed",
            "post-staging failure must leave an exact roll-forward intent"
        );
        fixed_commit_id = sidecar["protocol_v7"]["lineage"]["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_string();
    }

    // Recovery: reopen proves the exact confirmed Person overwrite and Tag
    // first-touch transaction, then publishes the sidecar's fixed lineage and
    // complete manifest delta.
    let db = Omnigraph::open(&uri).await.unwrap();

    // Recovery sweep must have advanced the manifest pin on the rewritten
    // table: roll-forward published the post-failure Lance HEAD.
    let post_recovery_version = version_main(&db).await.unwrap();
    assert!(
        post_recovery_version > pre_failure_version,
        "manifest version must advance post-recovery; pre={pre_failure_version}, \
        post={post_recovery_version}",
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        fixed_commit_id,
        "recovery must publish the pre-minted SchemaApply commit"
    );
    let recovered_commit = db.get_commit(&fixed_commit_id).await.unwrap();
    assert_eq!(recovered_commit.actor_id.as_deref(), Some(ACTOR));
    drop(db);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForwardOriginalLineage {
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

/// A rename+rewrite pin uses the desired alias (`node:Human`) for its forward
/// output, but an Armed crash must compensate back into the accepted manifest
/// binding (`node:Person`). Publishing the restore under the target alias would
/// fail OCC after the physical Restore and wedge every subsequent open.
#[tokio::test]
#[serial]
async fn schema_apply_rename_rewrite_partial_effect_restores_source_alias() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let people_before = helpers::count_rows(&db, "node:Person").await;
    let desired = r#"
node Human @rename_from("Person") {
    full_name: String @key @rename_from("name")
    age: I32?
}

node Company {
    name: String @key
}

edge Knows: Human -> Human {
    since: Date?
}

edge WorksAt: Human -> Company
"#;

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_POST_TABLE_COMMIT, "return");
        let error = db
            .apply_schema(desired)
            .await
            .expect_err("rename+rewrite must stop after its exact table effect");
        assert!(
            error.to_string().contains("schema_apply.post_table_commit"),
            "unexpected partial rename error: {error}"
        );
        single_sidecar_operation_id(dir.path())
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(sidecar["protocol_v7"]["effect_phase"], "Armed");
    let rename = &sidecar["protocol_v7"]["intended_delta"]["renames"][0];
    assert_eq!(rename["expected_table_key"], "node:Person");
    assert_eq!(rename["table_key"], "node:Human");
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must publish the restored source alias");
    let snapshot = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(snapshot.entry("node:Person").is_some());
    assert!(snapshot.entry("node:Human").is_none());
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
    assert!(!recovered.schema_source().contains("node Human"));
    drop(recovered);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![TableExpectation::main("node:Person")],
        },
    )
    .await
    .unwrap();
}

/// A multi-table apply can crash after only its first exact transaction. The
/// Armed sidecar must prove and compensate that proper subset without touching
/// a planned table whose HEAD never moved.
#[tokio::test]
#[serial]
async fn schema_apply_partial_table_effect_rolls_back_exactly() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let people_before = helpers::count_rows(&db, "node:Person").await;
    let companies_before = helpers::count_rows(&db, "node:Company").await;
    let desired = schema_with_person_city().replace(
        "    name: String @key\n}\n\nedge Knows",
        "    name: String @key\n    domain: String?\n}\n\nedge Knows",
    );

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_POST_TABLE_COMMIT, "return");
        let error = db
            .apply_schema(&desired)
            .await
            .expect_err("the first exact table commit must be interrupted");
        assert!(
            error.to_string().contains("schema_apply.post_table_commit"),
            "unexpected partial-effect error: {error}"
        );
        single_sidecar_operation_id(dir.path())
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(sidecar["protocol_v7"]["effect_phase"], "Armed");
    assert_eq!(
        sidecar["protocol_v7"]["effects"].as_array().unwrap().len(),
        2
    );
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must compensate the exact partial table set");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
    assert_eq!(
        helpers::count_rows(&recovered, "node:Company").await,
        companies_before
    );
    assert!(!recovered.schema_source().contains("city: String?"));
    assert!(!recovered.schema_source().contains("domain: String?"));
    assert!(!sidecar_path.exists());
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledBack"],
        "partial compensation must have one durable rollback outcome"
    );

    recovered
        .apply_schema(&desired)
        .await
        .expect("the complete migration must succeed after exact compensation");
    assert!(recovered.schema_source().contains("city: String?"));
    assert!(recovered.schema_source().contains("domain: String?"));
}

/// SchemaApply is prepared against one exact main graph head. A foreign
/// process may publish a disjoint table after the schema table effect but
/// before the exact manifest CAS. The apply must retain recovery ownership;
/// Full recovery compensates only its unpublished overwrite and descends from
/// the winning graph commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn schema_apply_post_effect_disjoint_winner_is_preserved() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let people_before = helpers::count_rows(&db, "node:Person").await;
    drop(db);

    let schema_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut winner_db = Omnigraph::open(&uri).await.unwrap();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let desired = schema_with_person_city();
    let apply_handle = std::sync::Arc::clone(&schema_db);
    let apply_task = tokio::spawn(async move { apply_handle.apply_schema(&desired).await });
    rendezvous.wait_until_reached().await;

    // Model a writer in another process: advance Company below the normal
    // process-local gates, then publish that exact physical HEAD to main.
    let company_uri = node_table_uri(&winner_db, "Company").await;
    let mut raw_company = lance::Dataset::open(&company_uri).await.unwrap();
    helpers::lance_delete_inline(&mut raw_company, "1 = 2").await;
    let winner_company_version = raw_company.version().version;
    winner_db
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Company", None)
        .await
        .unwrap();
    let winner_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    rendezvous.release();

    let operation_id = match apply_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("post-effect authority loss must require recovery: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    assert!(sidecar_path.exists());
    drop(schema_db);
    drop(winner_db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must compensate around a disjoint winner");
    assert!(!sidecar_path.exists());
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before,
        "rollback must restore the pre-apply Person image"
    );
    assert!(
        !recovered.schema_source().contains("city: String?"),
        "the stale schema contract must not be promoted"
    );
    let main = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        main.entry("node:Company").unwrap().table_version,
        winner_company_version,
        "compensation must preserve the disjoint winner's table pin"
    );
    let rollback_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    let rollback_commit = recovered.get_commit(&rollback_head).await.unwrap();
    assert_eq!(
        rollback_commit.parent_commit_id.as_deref(),
        Some(winner_head.as_str()),
        "rollback lineage must descend from the winning main commit"
    );
}

/// If a foreign main commit buries SchemaApply's exact Person overwrite, a
/// destructive Restore could erase the winner. Recovery must leave both the
/// winning manifest/HEAD and the durable sidecar untouched for operator review.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn schema_apply_post_effect_same_table_winner_fails_closed() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    drop(db);

    let schema_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut winner_db = Omnigraph::open(&uri).await.unwrap();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let desired = schema_with_person_city();
    let apply_handle = std::sync::Arc::clone(&schema_db);
    let apply_task = tokio::spawn(async move { apply_handle.apply_schema(&desired).await });
    rendezvous.wait_until_reached().await;

    let person_uri = node_table_uri(&winner_db, "Person").await;
    let mut raw_person = lance::Dataset::open(&person_uri).await.unwrap();
    helpers::lance_delete_inline(&mut raw_person, "1 = 2").await;
    let winner_lance_head = raw_person.version().version;
    winner_db
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
        .await
        .unwrap();
    let winner_manifest_version = winner_db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    rendezvous.release();

    let operation_id = match apply_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("same-table authority loss must require recovery: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    assert!(sidecar_path.exists());
    drop(schema_db);

    let error = match Omnigraph::open(&uri).await {
        Ok(_) => panic!("Full recovery must refuse to restore through a same-table winner"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("foreign")
            || error.to_string().contains("unverifiable")
            || error.to_string().contains("interleaved"),
        "unexpected fail-closed error: {error}"
    );
    assert!(
        sidecar_path.exists(),
        "fail-closed intent must remain durable"
    );
    assert_eq!(
        winner_db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        winner_manifest_version,
        "failed recovery must not move the winning manifest pin"
    );
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        winner_lance_head,
        "failed recovery must not Restore through the winning Lance HEAD"
    );
}

/// `optimize` Phase B → Phase C residual: `compact_files` advanced the Lance
/// HEAD but the manifest publish hasn't run. The `Optimize` recovery sidecar
/// (loose-match, like SchemaApply/EnsureIndices) must roll the compacted version
/// forward on next open so the manifest tracks the Lance HEAD — and the healed
/// table must then accept a schema apply (the original bug's victim).
async fn seed_two_productive_optimize_tables(uri: &str) {
    let db = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();
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
    for name in ["acme", "beta", "cygnus", "delta"] {
        db.mutate(
            "main",
            OCC_DISJOINT_MUTATIONS,
            "insert_company",
            &params(&[("$name", name)]),
        )
        .await
        .unwrap();
    }
}

fn assert_optimize_v9_sidecar_tables(
    graph_root: &std::path::Path,
    operation_id: &str,
    expected: &[&str],
) {
    let body = std::fs::read_to_string(
        graph_root
            .join("__recovery")
            .join(format!("{operation_id}.json")),
    )
    .unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["schema_version"], 9);
    assert_eq!(json["writer_kind"], "Optimize");
    let mut actual = json["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| table["table_key"].as_str().unwrap())
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected, "Optimize sidecar pinned the wrong tables");
}

#[tokio::test]
#[serial(optimize)]
async fn optimize_phase_b_failure_recovered_on_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let operation_id;

    // Both node tables have multiple fragments, so one Optimize arms one
    // multi-pin sidecar and completes both physical effects before the seam.
    seed_two_productive_optimize_tables(&uri).await;

    let pre_failure_version = {
        let db = Omnigraph::open(&uri).await.unwrap();
        version_main(&db).await.unwrap()
    };

    // Failpoint fires AFTER compact_files advanced the Lance HEAD but BEFORE the
    // graph-wide manifest publish. Exactly one multi-table sidecar persists.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new(names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT, "return");
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
        assert_optimize_v9_sidecar_tables(
            dir.path(),
            &operation_id,
            &["node:Company", "node:Person"],
        );
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
            tables: vec![
                TableExpectation::main("node:Company"),
                TableExpectation::main("node:Person"),
            ],
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

/// Lost acknowledgement after the one graph-wide manifest CAS: both table
/// pointers and Optimize lineage are already visible, but the shared v2
/// sidecar remains. Recovery must recognize a stale successful intent, preserve
/// both pointers, record RolledForward, and remove the sidecar.
#[tokio::test]
#[serial(optimize)]
async fn optimize_post_manifest_failure_finalizes_multi_table_v2_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_two_productive_optimize_tables(&uri).await;

    let operation_id;
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint =
            ScopedFailPoint::new(names::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT, "1*return");
        let error = db.optimize().await.unwrap_err();
        assert!(
            matches!(error, OmniError::RecoveryRequired { .. }),
            "lost graph-publish acknowledgement must require recovery, got {error}"
        );
        operation_id = single_sidecar_operation_id(dir.path());
        assert_optimize_v9_sidecar_tables(
            dir.path(),
            &operation_id,
            &["node:Company", "node:Person"],
        );
    }

    // Opening read-write finalizes the already-visible outcome; it must not
    // compensate either content-preserving table effect.
    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(helpers::count_rows(&recovered, "node:Person").await, 4);
    assert_eq!(helpers::count_rows(&recovered, "node:Company").await, 4);
    drop(recovered);
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![
                TableExpectation::main("node:Company"),
                TableExpectation::main("node:Person"),
            ],
        },
    )
    .await
    .unwrap();
}

/// Pending-only index intent is status, not a physical effect. An untrainable
/// vector table beside a productive sibling must be reported but excluded from
/// the shared Optimize sidecar; otherwise its NoMovement classification would
/// spuriously roll back the sibling after a crash.
#[tokio::test]
#[serial(optimize)]
async fn optimize_excludes_pending_only_vector_table_from_v2_sidecar() {
    const SCHEMA: &str = r#"
node Work {
    name: String @key
}

node Embedding {
    name: String @key
    vector: Vector(2)? @index
}
"#;
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Work","data":{"name":"w0"}}
{"type":"Embedding","data":{"name":"e0","vector":null}}
"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let pending = db.ensure_indices().await.unwrap();
    assert!(
        pending
            .iter()
            .any(|index| { index.table_key == "node:Embedding" && index.column == "vector" }),
        "fixture must leave the null vector index pending"
    );
    // Fixed productive tail on Work only; Embedding stays a one-fragment table
    // whose buildable indexes are current and whose vector remains pending-only.
    for name in ["w1", "w2", "w3", "w4"] {
        load_jsonl(
            &mut db,
            &format!(r#"{{"type":"Work","data":{{"name":"{name}"}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }

    let _failpoint =
        ScopedFailPoint::new(names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT, "1*return");
    let error = db.optimize().await.unwrap_err();
    assert!(matches!(error, OmniError::RecoveryRequired { .. }));
    let operation_id = single_sidecar_operation_id(dir.path());
    assert_optimize_v9_sidecar_tables(dir.path(), &operation_id, &["node:Work"]);
    drop(db);

    drop(Omnigraph::open(&uri).await.unwrap());
    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledForward {
            tables: vec![TableExpectation::main("node:Work")],
        },
    )
    .await
    .unwrap();

    let db = Omnigraph::open(&uri).await.unwrap();
    let stats = db.optimize().await.unwrap();
    let embedding = stats
        .iter()
        .find(|stat| stat.table_key == "node:Embedding")
        .expect("Embedding stat present");
    assert!(!embedding.committed, "pending-only table must stay a no-op");
    assert!(
        embedding
            .pending_indexes
            .iter()
            .any(|index| index.column == "vector"),
        "pending-only vector status must remain visible"
    );
}

/// One graph-wide v2 sidecar makes partial physical completion an
/// all-or-nothing recovery unit. One table stops before its first effect while
/// its sibling finishes; no table pointer becomes graph-visible, and Full open
/// compensates the moved sibling before publishing one rollback commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_multi_table_partial_effect_rolls_back_under_one_v2_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_two_productive_optimize_tables(&uri).await;

    let db = Omnigraph::open(&uri).await.unwrap();
    let snapshot_before = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let person_pin = snapshot_before.entry("node:Person").unwrap().table_version;
    let company_pin = snapshot_before.entry("node:Company").unwrap().table_version;
    let commits_before = db.list_commits(Some("main")).await.unwrap().len();
    let person_rows = helpers::count_rows(&db, "node:Person").await;
    let company_rows = helpers::count_rows(&db, "node:Company").await;
    let person_uri = node_table_uri(&db, "Person").await;
    let company_uri = node_table_uri(&db, "Company").await;

    // The first table task to hit the seam returns; the other task proceeds.
    // Collection waits for both, leaving one completed physical effect under
    // the one shared sidecar and no graph publish.
    let _failpoint = ScopedFailPoint::new(names::OPTIMIZE_BEFORE_COMPACT, "1*return");
    let error = db.optimize().await.unwrap_err();
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "post-arm partial Optimize must require recovery, got {error}"
    );
    let operation_id = single_sidecar_operation_id(dir.path());
    assert_optimize_v9_sidecar_tables(dir.path(), &operation_id, &["node:Company", "node:Person"]);

    let snapshot_after_failure = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        snapshot_after_failure
            .entry("node:Person")
            .unwrap()
            .table_version,
        person_pin,
        "partial Optimize must not publish Person"
    );
    assert_eq!(
        snapshot_after_failure
            .entry("node:Company")
            .unwrap()
            .table_version,
        company_pin,
        "partial Optimize must not publish Company"
    );
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        commits_before,
        "partial Optimize must not publish graph lineage"
    );

    let person_head = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    let company_head = lance::Dataset::open(&company_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        [person_head > person_pin, company_head > company_pin]
            .into_iter()
            .filter(|moved| *moved)
            .count(),
        1,
        "fixture must leave exactly one sibling physical effect before recovery"
    );
    drop(db);

    // Read-write open owns the quiescent v2 intent and compensates the moved
    // table; the no-movement sibling remains at its original pin.
    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        person_rows
    );
    assert_eq!(
        helpers::count_rows(&recovered, "node:Company").await,
        company_rows
    );
    assert_eq!(
        recovered.list_commits(Some("main")).await.unwrap().len(),
        commits_before + 1,
        "recovery must publish exactly one compensation lineage commit"
    );
    drop(recovered);

    assert_post_recovery_invariants(
        dir.path(),
        &operation_id,
        RecoveryExpectation::RolledBack {
            tables: vec![
                TableExpectation::main("node:Company"),
                TableExpectation::main("node:Person"),
            ],
        },
    )
    .await
    .unwrap();
}

async fn seed_optimize_late_sidecar_race(dir: &tempfile::TempDir) {
    let mut seed = helpers::init_and_load(dir).await;
    // Leave real compaction work behind so a missing barrier advances Person
    // instead of accidentally passing because Optimize was a no-op.
    helpers::commit_many(&mut seed, 4).await;
}

/// Optimize's entry recovery probe is only a fast path. A graph-global
/// SchemaApply can arm its durable sidecar after that probe while Optimize is
/// waiting for the main-branch gate, then fail before staging any schema/table
/// effect. Once Optimize owns that gate it must relist recovery state and return
/// the older operation's exact RecoveryRequired id before opening HEAD, writing
/// an Optimize sidecar, or moving either the table or manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[serial(optimize)]
async fn optimize_rechecks_late_schema_apply_sidecar_after_main_gate() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    seed_optimize_late_sidecar_race(&dir).await;

    // Both handles must exist before the late sidecar: a new read-write open
    // would recover it instead of exercising the long-lived-handle race.
    let optimize_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let schema_db = Omnigraph::open(&uri).await.unwrap();
    let person_uri = node_table_uri(optimize_db.as_ref(), "Person").await;
    let manifest_uri = format!("{uri}/__manifest");
    let manifest_before = version_main(optimize_db.as_ref()).await.unwrap();
    let physical_manifest_before = lance::Dataset::open(&manifest_uri)
        .await
        .unwrap()
        .version()
        .version;
    let graph_head_before = branch_head_commit_id(dir.path(), "main").await.unwrap();
    let person_head_before = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE,
    );
    let optimize_task_db = std::sync::Arc::clone(&optimize_db);
    let optimize = tokio::spawn(async move { optimize_task_db.optimize().await });
    rendezvous.wait_until_reached().await;

    // Adding @index changes only accepted schema metadata. The failpoint fires
    // after SchemaApply's zero-table v5 sidecar is durable, but before schema
    // staging or any physical table effect.
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    {
        let _failpoint = ScopedFailPoint::new(names::SCHEMA_APPLY_BEFORE_STAGING_WRITE, "return");
        let error = schema_db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected failpoint triggered: schema_apply.before_staging_write"),
            "unexpected SchemaApply failure: {error}",
        );
    }
    let schema_operation_id = single_sidecar_operation_id(dir.path());

    rendezvous.release();
    let optimize_error = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("Optimize task hung after releasing the recovery-check rendezvous")
        .unwrap()
        .expect_err("Optimize must refuse the late graph-global recovery intent");
    match optimize_error {
        OmniError::RecoveryRequired { operation_id, .. } => {
            assert_eq!(operation_id, schema_operation_id)
        }
        other => panic!("expected exact RecoveryRequired attribution, got {other}"),
    }
    drop(rendezvous);

    assert_eq!(
        single_sidecar_operation_id(dir.path()),
        schema_operation_id,
        "Optimize must not add its own sidecar beside the older SchemaApply intent",
    );
    assert_eq!(
        version_main(optimize_db.as_ref()).await.unwrap(),
        manifest_before,
        "late-barrier refusal must happen before any data or internal manifest effect",
    );
    assert_eq!(
        lance::Dataset::open(&manifest_uri)
            .await
            .unwrap()
            .version()
            .version,
        physical_manifest_before,
        "late-barrier refusal must not compact the physical __manifest afterward",
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        graph_head_before,
        "late-barrier refusal must not manufacture graph lineage",
    );
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        person_head_before,
        "Optimize must not move the target table HEAD around the older sidecar",
    );

    drop(optimize_db);
    drop(schema_db);

    // Full recovery owns the abandoned SchemaApply intent. Once it rolls the
    // metadata-only attempt back, the same Optimize work is safe and succeeds.
    let recovered = Omnigraph::open(&uri)
        .await
        .expect("read-write open must recover the abandoned SchemaApply intent");
    assert_post_recovery_invariants(
        dir.path(),
        &schema_operation_id,
        RecoveryExpectation::RolledBack { tables: vec![] },
    )
    .await
    .unwrap();
    recovered
        .optimize()
        .await
        .expect("Optimize must succeed after the older recovery intent is resolved");
}

/// A main-branch recovery intent overlaps Optimize even when it touched a
/// different table: both operations publish through the shared
/// `graph_head:main`. This pins the branch-wide half of the final barrier. A
/// confirmed Company mutation fails before visibility while Optimize is parked;
/// Optimize must not compact Person or advance main around that fixed authority.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[serial(optimize)]
async fn optimize_rechecks_late_disjoint_main_sidecar_after_main_gate() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_optimize_late_sidecar_race(&dir).await;

    let optimize_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let writer_db = Omnigraph::open(&uri).await.unwrap();
    let person_uri = node_table_uri(optimize_db.as_ref(), "Person").await;
    let manifest_uri = format!("{uri}/__manifest");

    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        names::OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE,
    );
    let optimize_task_db = std::sync::Arc::clone(&optimize_db);
    let optimize = tokio::spawn(async move { optimize_task_db.optimize().await });
    rendezvous.wait_until_reached().await;

    // The interrupted writer affects Company, while the productive Optimize
    // work this test protects is Person compaction. Its v3 sidecar is confirmed
    // and durable; only the main graph-head authority overlaps.
    {
        let _failpoint =
            ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        let error = writer_db
            .mutate(
                "main",
                OCC_DISJOINT_MUTATIONS,
                "insert_company",
                &params(&[("$name", "InterruptedCo")]),
            )
            .await
            .expect_err("Company mutation must stop after confirming its physical effect");
        assert!(
            error
                .to_string()
                .contains("mutation.post_finalize_pre_publisher"),
            "unexpected mutation failure: {error}",
        );
    }
    let writer_operation_id = single_sidecar_operation_id(dir.path());

    // Baseline after the older writer's owned Company effect. Nothing below may
    // add Optimize movement before recovery resolves that intent.
    let manifest_before_optimize = version_main(optimize_db.as_ref()).await.unwrap();
    let physical_manifest_before_optimize = lance::Dataset::open(&manifest_uri)
        .await
        .unwrap()
        .version()
        .version;
    let graph_head_before_optimize = branch_head_commit_id(dir.path(), "main").await.unwrap();
    let person_head_before_optimize = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    rendezvous.release();
    let optimize_error = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("Optimize task hung after releasing the recovery-check rendezvous")
        .unwrap()
        .expect_err("Optimize must refuse a late main-branch recovery intent");
    match optimize_error {
        OmniError::RecoveryRequired { operation_id, .. } => {
            assert_eq!(operation_id, writer_operation_id)
        }
        other => panic!("expected exact RecoveryRequired attribution, got {other}"),
    }
    drop(rendezvous);

    assert_eq!(
        single_sidecar_operation_id(dir.path()),
        writer_operation_id,
        "Optimize must not add a sidecar beside the disjoint main writer",
    );
    assert_eq!(
        version_main(optimize_db.as_ref()).await.unwrap(),
        manifest_before_optimize,
        "Optimize must not publish a main-table pointer around the older intent",
    );
    assert_eq!(
        lance::Dataset::open(&manifest_uri)
            .await
            .unwrap()
            .version()
            .version,
        physical_manifest_before_optimize,
        "Optimize must not compact the physical __manifest after refusing",
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        graph_head_before_optimize,
        "Optimize must not invalidate the older intent's fixed graph-head authority",
    );
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        person_head_before_optimize,
        "disjoint Company recovery must still block Person compaction",
    );

    drop(optimize_db);
    drop(writer_db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("read-write open must recover the confirmed Company mutation");
    assert_post_recovery_invariants(
        dir.path(),
        &writer_operation_id,
        RecoveryExpectation::RolledForwardOriginalLineage {
            tables: vec![TableExpectation::main("node:Company")],
        },
    )
    .await
    .unwrap();
    recovered
        .optimize()
        .await
        .expect("Optimize must succeed after the main recovery intent is resolved");
}

/// Optimize retains main's branch gate after its final recovery relist and
/// through its effects. A Company insert started while productive Person
/// compaction is paused must therefore wait despite sharing no table gate, then
/// commit after Optimize releases the branch-wide legacy-adapter envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_holds_main_gate_through_disjoint_table_effects() {
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

    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());

    // Pause productive Person optimize after the under-main-gate recovery
    // relist and after its Person sidecar is durable.
    let failpoint = ScopedFailPoint::new(names::OPTIMIZE_BEFORE_COMPACT, "pause");

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = Omnigraph::open(&uri_opt).await.unwrap();
        db.optimize().await
    });

    // Wait until optimize reaches the pause (its Optimize sidecar is on disk).
    assert!(
        wait_for_sidecar(dir.path()).await,
        "optimize never reached the pre-compact pause",
    );

    let writer_db = std::sync::Arc::clone(&db_b);
    let writer = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                OCC_DISJOINT_MUTATIONS,
                "insert_company",
                &params(&[("$name", "QueuedCo")]),
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "table-disjoint main writer must wait for Optimize's branch-gate lifetime"
    );

    drop(failpoint); // release optimize
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("optimize task hung")
        .unwrap();
    result.expect("optimize must finish before the queued disjoint main writer");
    writer
        .await
        .expect("writer task panicked")
        .expect("queued Company insert must resume after Optimize");

    // No lost work on either table; graph remains re-optimizable.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        4,
        "Person compaction must preserve every seed row",
    );
    assert_eq!(
        helpers::count_rows(&db, "node:Company").await,
        1,
        "queued table-disjoint Company insert must persist",
    );
    db.optimize()
        .await
        .expect("graph must remain healthy / re-optimizable");
}

/// Same as the insert cell, for a strict delete: the second handle waits for
/// optimize, then replans from the manifest-visible post-optimize state and
/// preserves the deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_serializes_concurrent_delete_across_handles() {
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

    let db_b = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());

    // Pause optimize BEFORE its compaction commits.
    let failpoint = ScopedFailPoint::new(names::OPTIMIZE_BEFORE_COMPACT, "pause");

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = Omnigraph::open(&uri_opt).await.unwrap();
        db.optimize().await
    });

    assert!(
        wait_for_sidecar(dir.path()).await,
        "optimize never reached the pre-compact pause",
    );

    let writer_db = std::sync::Arc::clone(&db_b);
    let writer = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "remove_person",
                &mixed_params(&[("$name", "alice")], &[]),
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "same-root delete must wait for optimize's guarded sidecar lifetime"
    );

    drop(failpoint); // release optimize
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("optimize task hung")
        .unwrap();
    result.expect("optimize must finish before the queued delete");
    writer
        .await
        .expect("writer task panicked")
        .expect("queued delete must resume after optimize");

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

/// Regression: the outer compaction retry loop must NOT misclassify optimize's OWN
/// committed Phase-B work as external drift. Attempt 1 compacts (HEAD → V+1); if a
/// LATER Phase-B op (reindex) then hits a retryable conflict, the reopened attempt
/// sees Lance HEAD ahead of the manifest — from OUR compaction, not an external
/// writer. The drift guard must skip it (we hold the sidecar) and converge, not
/// delete the sidecar and return `skipped_for_drift` (which would strand uncovered
/// drift). Reproduced by injecting one retryable reindex conflict after the compact.
#[tokio::test]
#[serial(optimize)]
async fn optimize_retry_does_not_misclassify_own_head_drift() {
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

    // Inject exactly one retryable reindex conflict: attempt 1 compacts (HEAD+1) then
    // "conflicts" on reindex → retry; attempt 2 reopens with HEAD ahead of the manifest
    // from our own compaction — the misclassification trigger.
    let _failpoint = ScopedFailPoint::new(names::OPTIMIZE_INJECT_REINDEX_CONFLICT, "1*return");

    let db = Omnigraph::open(&uri).await.unwrap();
    let stats = db
        .optimize()
        .await
        .expect("optimize must converge, not misclassify its own HEAD drift");
    let person = stats
        .iter()
        .find(|s| s.table_key == "node:Person")
        .expect("node:Person stat present");
    assert!(
        person.skipped.is_none(),
        "node:Person must converge, not skipped_for_drift: {:?}",
        person.skipped,
    );

    // No uncovered drift stranded: a follow-up optimize is clean and all rows read.
    let stats2 = db.optimize().await.unwrap();
    let person2 = stats2
        .iter()
        .find(|s| s.table_key == "node:Person")
        .unwrap();
    assert!(
        person2.skipped.is_none(),
        "follow-up optimize must be clean (no stranded drift): {:?}",
        person2.skipped,
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 4);
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
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_phase_b_failure_recovered_on_next_open() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main with a row, branch off, mutate BOTH sides so the merge
    // produces a `RewriteMerged` candidate (target moved past base too). The
    // v4 sidecar records its exact planned data transactions and any contiguous
    // derived-index tail before publishing the complete logical delta.
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
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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

    // Recovery: reopen proves the v4 sidecar's exact planned transaction chain,
    // accepts only its contiguous derived CreateIndex suffix, and publishes the
    // confirmed final version plus fixed lineage and complete manifest delta.
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
    // `merged_parent_commit_id` set), not a plain commit. Without this, future
    // merges between the same pair lose already-up-to-date detection. RFC-013
    // Phase 7 records the recovery commit in `__manifest` (folded into the
    // recovery publish CAS), so we read it through the commit-graph projection
    // (`CommitGraph::load_commits`) and assert some commit carries a non-null
    // `merged_parent_commit_id`. Only a recovered branch_merge can produce one
    // here (we never completed a normal merge in this test).
    {
        let commits = omnigraph::db::commit_graph::CommitGraph::open(dir.path().to_str().unwrap())
            .await
            .unwrap()
            .load_commits()
            .await
            .unwrap();
        let found_recovery_merge = commits.iter().any(|c| c.merged_parent_commit_id.is_some());
        assert!(
            found_recovery_merge,
            "recovered branch_merge must record `merged_parent_commit_id` so future \
             merges detect already-up-to-date — no merge-parent-tagged commit found",
        );
    }
    drop(db);
}

/// The v4 recovery delta is wider than its physical pin set. A mixed merge can
/// rewrite one table while another table needs only a manifest pointer switch;
/// recovery must publish both under the original merge lineage.
#[tokio::test]
#[serial(branch_merge_phase_b)]
async fn branch_merge_recovery_replays_pointer_slots_with_fixed_lineage() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("target").await.unwrap();

    // Main is the source. Advance Person and Company after target forked;
    // target independently advances Person. Person therefore needs a physical
    // three-way rewrite, while Company is a source-on-main pointer adoption.
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-main-person")], &[("$age", 51)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        OCC_DISJOINT_MUTATIONS,
        "insert_company",
        &params(&[("$name", "source-main-company")]),
    )
    .await
    .unwrap();
    db.mutate(
        "target",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "target-person")], &[("$age", 52)]),
    )
    .await
    .unwrap();
    let source_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    let target_parent = branch_head_commit_id(dir.path(), "target").await.unwrap();

    let error = {
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        db.branch_merge_as("main", "target", Some("merge-author"))
            .await
            .unwrap_err()
    };
    let operation_id = match error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("confirmed mixed merge must retain recovery ownership: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    let fixed_commit_id = sidecar["protocol_v4"]["lineage"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    let delta_keys = sidecar["protocol_v4"]["intended_delta"]["table_updates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|slot| slot["table_key"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    let effect_keys = sidecar["protocol_v4"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|effect| effect["table_key"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        delta_keys,
        std::collections::HashSet::from(["node:Person", "node:Company"])
    );
    assert_eq!(
        effect_keys,
        std::collections::HashSet::from(["node:Person"]),
        "pointer-only Company must be in the logical delta but not physical pins"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert!(!sidecar_path.exists());
    let people = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Person").await,
        "name",
    );
    assert!(people.iter().any(|name| name == "source-main-person"));
    assert!(people.iter().any(|name| name == "target-person"));
    let companies = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Company").await,
        "name",
    );
    assert!(companies.iter().any(|name| name == "source-main-company"));

    let recovered_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    assert_eq!(recovered_head, fixed_commit_id);
    let commit = recovered.get_commit(&recovered_head).await.unwrap();
    assert_eq!(
        commit.parent_commit_id.as_deref(),
        Some(target_parent.as_str())
    );
    assert_eq!(
        commit.merged_parent_commit_id.as_deref(),
        Some(source_head.as_str())
    );
    assert_eq!(commit.actor_id.as_deref(), Some("merge-author"));
}

/// Phase D is outside the logical commit boundary. If deleting a confirmed v4
/// sidecar fails after the exact merge commit is already visible, the merge
/// still succeeds. The next open must recognize that fixed outcome, append only
/// its recovery audit, and retire the stale artifact without publishing a
/// second graph commit or changing any lineage field.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_sidecar_delete_failure_finalizes_visible_fixed_lineage_once() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _) = setup_diverged_merge_branches(&dir).await;
    let db = Omnigraph::open(&uri).await.unwrap();
    let source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    let target_parent = branch_head_commit_id(dir.path(), "target").await.unwrap();

    let outcome = {
        let _failpoint = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
        db.branch_merge_as("source", "target", Some("phase-d-actor"))
            .await
            .expect("Phase-D sidecar deletion failure must not fail a visible merge")
    };
    assert_eq!(outcome, omnigraph::db::MergeOutcome::Merged);

    let operation_id = single_sidecar_operation_id(dir.path());
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    let fixed_commit_id = sidecar["protocol_v4"]["lineage"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();
    let fixed_created_at = sidecar["protocol_v4"]["lineage"]["created_at"]
        .as_i64()
        .unwrap();
    assert_eq!(
        branch_head_commit_id(dir.path(), "target").await.unwrap(),
        fixed_commit_id,
        "the writer must already have published the sidecar's fixed commit"
    );
    let commits_before_reopen = db.list_commits(Some("target")).await.unwrap().len();
    assert!(
        recovery_audit_kinds(dir.path()).await.is_empty(),
        "the writer does not append a recovery audit before stale-sidecar finalization"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert!(!sidecar_path.exists());
    let recovered_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    assert_eq!(recovered_head, fixed_commit_id);
    let recovered_commit = recovered.get_commit(&recovered_head).await.unwrap();
    assert_eq!(
        recovered_commit.parent_commit_id.as_deref(),
        Some(target_parent.as_str())
    );
    assert_eq!(
        recovered_commit.merged_parent_commit_id.as_deref(),
        Some(source_head.as_str())
    );
    assert_eq!(recovered_commit.actor_id.as_deref(), Some("phase-d-actor"));
    assert_eq!(recovered_commit.created_at, fixed_created_at);
    assert_eq!(
        recovered.list_commits(Some("target")).await.unwrap().len(),
        commits_before_reopen,
        "stale-sidecar finalization must not publish a second graph commit"
    );
    assert_eq!(
        recovery_audit_kinds(dir.path())
            .await
            .into_iter()
            .filter(|kind| kind == "RolledForward")
            .count(),
        1,
        "the visible fixed outcome must have exactly one recovery audit row"
    );

    drop(recovered);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        reopened.list_commits(Some("target")).await.unwrap().len(),
        commits_before_reopen,
        "a second reopen must remain a lineage no-op"
    );
    assert_eq!(
        recovery_audit_kinds(dir.path())
            .await
            .into_iter()
            .filter(|kind| kind == "RolledForward")
            .count(),
        1,
        "a second reopen must not duplicate the audit row"
    );
}

/// A merge plan is classified against one exact target graph head. Advancing
/// that head after every table effect is confirmed but before the manifest CAS
/// must not silently re-parent the stale merge. This uses the queue-bypassing
/// test seam to model a writer in another process: ordinary `Omnigraph`
/// handles share the root-scoped gates and therefore cannot exercise the
/// persistent-authority boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(branch_merge_phase_b)]
async fn branch_merge_post_effect_target_advance_requires_recovery_and_preserves_winner() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        db.mutate(
            "target",
            OCC_DISJOINT_MUTATIONS,
            "insert_company",
            &params(&[("$name", "target-winner-company")]),
        )
        .await
        .unwrap();
    }
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut target_winner = Omnigraph::open(&uri).await.unwrap();
    let source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();

    let merge_rv = helpers::failpoint::Rendezvous::park_first(
        names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );
    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    // Publish a logically redundant Company pin directly on the target. The
    // table is disjoint from the merge's Person effect, but the graph lineage
    // still advances, invalidating the merge's coarse target authority token.
    // The test-only seam deliberately bypasses the process-local queues.
    let company_uri = node_table_uri(&target_winner, "Company").await;
    let mut raw_company = lance::Dataset::open(&company_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    helpers::lance_delete_inline(&mut raw_company, "1 = 2").await;
    target_winner
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "target",
            "node:Company",
            Some("target"),
        )
        .await
        .unwrap();
    let winner_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    merge_rv.release();

    let error = merge_task
        .await
        .unwrap()
        .expect_err("a post-effect target advance must reject the stale merge publish");
    let operation_id = match error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("expected RecoveryRequired after merge effects, got {other}"),
    };

    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(
        sidecar["protocol_v4"]["lineage"]["merged_parent_commit_id"],
        source_head.as_str(),
        "recovery must retain the captured source commit as the merge parent"
    );

    drop(merge_db);
    drop(target_winner);

    // Full recovery sees that target authority changed. It must preserve the
    // winning graph commit and compensate only the unpublished merge effects.
    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert!(
        !sidecar_path.exists(),
        "successful compensation must retire the merge recovery intent"
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1,
        "recovery must restore the pre-merge target image"
    );
    let target_names = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Person").await,
        "name",
    );
    assert!(target_names.iter().any(|name| name == "old-target-only"));
    assert!(!target_names.iter().any(|name| name == "source-only"));

    let recovered_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    let recovered_commit = recovered.get_commit(&recovered_head).await.unwrap();
    assert_eq!(
        recovered_commit.parent_commit_id.as_deref(),
        Some(winner_head.as_str()),
        "rollback lineage must descend from, never replace, the target winner"
    );
}

/// If a foreign writer advances and publishes the SAME target table after the
/// merge's confirmed effect, that effect is buried under foreign state. Full
/// recovery must fail closed instead of restoring through the winner or
/// adopting the newer numeric HEAD as this merge's output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(branch_merge_phase_b)]
async fn branch_merge_post_effect_same_table_advance_fails_closed() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut target_winner = Omnigraph::open(&uri).await.unwrap();
    let merge_rv = helpers::failpoint::Rendezvous::park_first(
        names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    let person_uri = node_table_uri(&target_winner, "Person").await;
    let mut raw_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    helpers::lance_delete_inline(&mut raw_target, "1 = 2").await;
    let winner_lance_head = raw_target.version().version;
    target_winner
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "target",
            "node:Person",
            Some("target"),
        )
        .await
        .unwrap();
    let winner_manifest_version = helpers::version_branch(&target_winner, "target")
        .await
        .unwrap();
    merge_rv.release();

    let operation_id = match merge_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("same-table post-effect contention must require recovery: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    assert!(sidecar_path.exists());
    drop(merge_db);

    let error = match Omnigraph::open(&uri).await {
        Ok(_) => panic!("Full recovery must refuse to restore through a same-table winner"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("manifest pin changed")
            || error.to_string().contains("foreign or unverifiable"),
        "unexpected fail-closed error: {error}"
    );
    assert!(
        sidecar_path.exists(),
        "operator-owned intent must remain durable"
    );
    assert_eq!(
        helpers::version_branch(&target_winner, "target")
            .await
            .unwrap(),
        winner_manifest_version,
        "failed recovery must not move the winning target manifest"
    );
    let lance_after_failed_recovery = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        lance_after_failed_recovery, winner_lance_head,
        "fail-closed recovery must not restore through the winning Lance HEAD"
    );
}

/// A v4 rollback is itself a recoverable multi-step write. If open restores an
/// owned merge effect and then crashes before publishing that compensated HEAD,
/// the next open must recognize the restore transaction, reuse it, and finish
/// the fixed rollback once. Re-restoring forever would monotonically advance
/// Lance HEAD while every read-write open remained wedged.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_rollback_restarts_after_restore_before_publish() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;

    // Give the target a disjoint owned table. After the merge reaches its
    // confirmed Phase B, a no-op commit on this table advances target graph
    // authority without burying the merge-owned Person HEAD, making safe
    // compensation both necessary and possible.
    let db = Omnigraph::open(&uri).await.unwrap();
    db.mutate(
        "target",
        OCC_DISJOINT_MUTATIONS,
        "insert_company",
        &params(&[("$name", "rollback-winner-company")]),
    )
    .await
    .unwrap();
    let mut target_winner = Omnigraph::open(&uri).await.unwrap();

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        match db.branch_merge("source", "target").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("confirmed merge must retain recovery ownership: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    assert!(sidecar_path.exists());

    let company_uri = node_table_uri(&target_winner, "Company").await;
    let mut raw_company = lance::Dataset::open(&company_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    helpers::lance_delete_inline(&mut raw_company, "1 = 2").await;
    target_winner
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "target",
            "node:Company",
            Some("target"),
        )
        .await
        .unwrap();
    let winner_head = branch_head_commit_id(dir.path(), "target").await.unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let person_before_restore = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .version()
        .version;
    drop(db);
    drop(target_winner);

    {
        let _failpoint =
            ScopedFailPoint::new(names::RECOVERY_POST_TABLE_RESTORE_PRE_PUBLISH, "return");
        let error = match Omnigraph::open(&uri).await {
            Ok(_) => panic!("recovery must stop after the injected table restore"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("recovery.post_table_restore_pre_publish"),
            "unexpected interrupted-rollback error: {error}"
        );
    }
    assert!(
        sidecar_path.exists(),
        "an interrupted rollback must retain its recovery intent"
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "target").await.unwrap(),
        winner_head,
        "the rollback manifest publish must not occur before the failpoint"
    );
    assert!(
        recovery_audit_kinds(dir.path()).await.is_empty(),
        "an interrupted rollback must not claim a completed audit outcome"
    );
    let person_after_interrupted_restore = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .version()
        .version;
    assert!(
        person_after_interrupted_restore > person_before_restore,
        "the fixture must durably restore Person before interrupting the manifest publish"
    );

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("the next open must recognize and finish the interrupted compensation");
    assert!(!sidecar_path.exists());
    let person_after_recovery = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(
        person_after_recovery, person_after_interrupted_restore,
        "restartable rollback must reuse the owned restore instead of restoring again"
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1
    );
    let target_names = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Person").await,
        "name",
    );
    assert!(target_names.iter().any(|name| name == "old-target-only"));
    assert!(!target_names.iter().any(|name| name == "source-only"));
    let rollback_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    let rollback_commit = recovered.get_commit(&rollback_head).await.unwrap();
    assert_eq!(
        rollback_commit.parent_commit_id.as_deref(),
        Some(winner_head.as_str())
    );
    assert_eq!(
        recovery_audit_kinds(dir.path())
            .await
            .into_iter()
            .filter(|kind| kind == "RolledBack")
            .count(),
        1
    );

    drop(recovered);
    let _reopened = Omnigraph::open(&uri).await.unwrap();
    let person_after_second_open = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(person_after_second_open, person_after_recovery);
    assert_eq!(
        recovery_audit_kinds(dir.path())
            .await
            .into_iter()
            .filter(|kind| kind == "RolledBack")
            .count(),
        1,
        "a second open must not repeat rollback or its audit"
    );
}

/// A pure first-touch/ref-only merge can reach EffectsConfirmed without any
/// data HEAD movement. Recovery must validate the minted ref identity and roll
/// the exact pointer delta forward, not discard it as an empty intent.
#[tokio::test]
#[serial(branch_merge_first_touch)]
async fn branch_merge_confirmed_ref_only_effect_rolls_forward() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "confirmed-ref-row")], &[("$age", 38)]),
    )
    .await
    .unwrap();

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        match db.branch_merge("source", "target").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("confirmed ref-only merge must retain recovery ownership: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["protocol_v4"]["effect_phase"], "EffectsConfirmed");
    assert!(!sidecar["protocol_v4"]["effects"][0]["kind"]["confirmed_branch_identifier"].is_null());
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert!(!sidecar_path.exists());
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1
    );
    let names = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Person").await,
        "name",
    );
    assert!(names.iter().any(|name| name == "confirmed-ref-row"));
}

/// RewriteMerged can append reconciler-owned CreateIndex commits after its
/// pre-minted logical data transactions. An Armed crash in that derived tail is
/// still rollback-only, but recovery must prove the exact planned transaction
/// prefix and accept only the contiguous CreateIndex suffix rather than treating
/// its larger numeric HEAD as foreign movement.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_armed_index_tail_rolls_back_after_exact_transaction_prefix() {
    use lance::index::DatasetIndexExt;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let mut db = Omnigraph::open(&uri).await.unwrap();
    let person_uri = node_table_uri(&db, "Person").await;

    // Make the rewrite rebuild `id_idx` after its logical data transaction.
    // Publish the dropped-index HEAD first so the merge's expected version and
    // planned transaction chain are anchored to a fully consistent target.
    let mut target_person = helpers::open_dataset_head(&person_uri, Some("target")).await;
    target_person.drop_index("id_idx").await.unwrap();
    let dropped_index_head = target_person.version().version;
    db.failpoint_publish_table_head_without_index_rebuild_for_test(
        "target",
        "node:Person",
        Some("target"),
    )
    .await
    .unwrap();
    assert_eq!(
        db.snapshot_of(omnigraph::db::ReadTarget::branch("target"))
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        dropped_index_head
    );

    let operation_id = {
        let _failpoint =
            ScopedFailPoint::new(names::BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM, "return");
        match db.branch_merge("source", "target").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("Armed derived-tail failure must retain recovery ownership: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["protocol_v4"]["effect_phase"], "Armed");
    let person_pin = sidecar["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pin| pin["table_key"] == "node:Person")
        .unwrap();
    let expected_version = person_pin["expected_version"].as_u64().unwrap();
    let person_effect = sidecar["protocol_v4"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|effect| effect["table_key"] == "node:Person")
        .unwrap();
    let planned_transaction_count = person_effect["kind"]["planned_transactions"]
        .as_array()
        .unwrap()
        .len();
    assert!(
        planned_transaction_count > 0,
        "fixture must plan at least one logical Person transaction"
    );
    let raw_head_with_index_tail = helpers::open_dataset_head(&person_uri, Some("target"))
        .await
        .version()
        .version;
    assert!(
        raw_head_with_index_tail > expected_version + planned_transaction_count as u64,
        "raw HEAD {raw_head_with_index_tail} must include at least one CreateIndex tail after \
         expected {expected_version} + {planned_transaction_count} planned transactions"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Full recovery must roll an exact Armed transaction+index tail back");
    assert!(!sidecar_path.exists());
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1
    );
    let names = collect_column_strings(
        &helpers::read_table_branch(&recovered, "target", "node:Person").await,
        "name",
    );
    assert!(names.iter().any(|name| name == "old-target-only"));
    assert!(!names.iter().any(|name| name == "source-only"));
}

/// Phase-B confirmation is an ownership proof, not a numeric HEAD stamp. A
/// foreign logical Append can land after the merge's exact data transaction and
/// before its retrying CreateIndex tail; Lance may validly rebase that index
/// build over the Append, but the merge must not confirm or recover the foreign
/// row as its own output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_confirmation_rejects_foreign_append_before_index_tail() {
    use futures::TryStreamExt;
    use lance::index::DatasetIndexExt;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _) = setup_diverged_merge_branches(&dir).await;
    let mut db = Omnigraph::open(&uri).await.unwrap();

    // Prepare one schema-exact row on an unrelated branch. Its RecordBatch can
    // then be appended directly to target Lance HEAD without invoking any
    // target manifest publisher or process-local write gate.
    db.branch_create("foreign-seed").await.unwrap();
    db.mutate(
        "foreign-seed",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "foreign-unpublished")], &[("$age", 61)]),
    )
    .await
    .unwrap();
    let foreign_batch = helpers::read_table_branch(&db, "foreign-seed", "node:Person")
        .await
        .into_iter()
        .find_map(|batch| {
            let names = batch
                .column_by_name("name")?
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()?;
            (0..batch.num_rows())
                .find(|row| names.value(*row) == "foreign-unpublished")
                .map(|row| batch.slice(row, 1))
        })
        .expect("foreign seed row must be readable as one append batch");

    // Force RewriteMerged to execute a CreateIndex tail after its exact data
    // transaction, while keeping the target manifest consistent before merge.
    let person_uri = node_table_uri(&db, "Person").await;
    let mut target_person = helpers::open_dataset_head(&person_uri, Some("target")).await;
    target_person.drop_index("id_idx").await.unwrap();
    let dropped_index_head = target_person.version().version;
    db.failpoint_publish_table_head_without_index_rebuild_for_test(
        "target",
        "node:Person",
        Some("target"),
    )
    .await
    .unwrap();
    let target_head_before_merge = branch_head_commit_id(dir.path(), "target").await.unwrap();

    let merge_db = std::sync::Arc::new(db);
    let merge_rv = helpers::failpoint::Rendezvous::park_first(
        names::BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX,
    );
    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    // The merge's logical data transaction is now at HEAD, but its index build
    // has not started. Append a real logical row without publishing target
    // manifest authority, then let CreateIndex rebase over that foreign commit.
    let mut raw_target = helpers::open_dataset_head(&person_uri, Some("target")).await;
    helpers::lance_append_inline(&mut raw_target, foreign_batch).await;
    let foreign_append_head = raw_target.version().version;
    merge_rv.release();

    let operation_id = match merge_task.await.unwrap().unwrap_err() {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("foreign confirmation tail must retain recovery ownership: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(
        sidecar["protocol_v4"]["effect_phase"], "Armed",
        "confirmation must reject before persisting EffectsConfirmed"
    );
    let raw_head_after_index_rebase = helpers::open_dataset_head(&person_uri, Some("target"))
        .await
        .version()
        .version;
    assert!(
        raw_head_after_index_rebase > foreign_append_head,
        "fixture must place at least one rebased CreateIndex commit after the foreign Append"
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "target").await.unwrap(),
        target_head_before_merge,
        "rejected confirmation must not advance target graph lineage"
    );
    drop(merge_db);

    let open_error = match Omnigraph::open(&uri).await {
        Ok(_) => panic!("Full recovery must fail closed on a non-CreateIndex foreign tail"),
        Err(error) => error,
    };
    assert!(
        open_error.to_string().contains("foreign")
            || open_error.to_string().contains("unverifiable")
            || open_error.to_string().contains("CreateIndex"),
        "unexpected fail-closed error: {open_error}"
    );
    assert!(sidecar_path.exists());
    assert_eq!(
        branch_head_commit_id(dir.path(), "target").await.unwrap(),
        target_head_before_merge,
        "failed recovery must not publish or re-parent the merge"
    );
    let read_only = Omnigraph::open_read_only(&uri).await.unwrap();
    assert_eq!(
        read_only
            .snapshot_of(omnigraph::db::ReadTarget::branch("target"))
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        dropped_index_head,
        "target manifest must remain at its pre-merge Person pin"
    );
    let raw_after_failed_recovery = helpers::open_dataset_head(&person_uri, Some("target")).await;
    assert_eq!(
        raw_after_failed_recovery.version().version,
        raw_head_after_index_rebase,
        "fail-closed recovery must not restore through the foreign Append"
    );
    let raw_batches: Vec<arrow_array::RecordBatch> = raw_after_failed_recovery
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        collect_column_strings(&raw_batches, "name")
            .iter()
            .any(|name| name == "foreign-unpublished"),
        "the unpublished foreign row must survive fail-closed confirmation and recovery"
    );
}

/// AdoptWithDelta recovery (the gap closure): a fast-forward merge — main has
/// NOT advanced since the branch forked, so the touched table is classified
/// `AdoptWithDelta`, not `RewriteMerged` — that fails after Phase B must still
/// recover on the next open. Before the recovery-pin closure this drifted
/// silently: the adopt path advanced Lance HEAD but was unpinned, so the sweep
/// found no sidecar and the merge was lost.
#[tokio::test]
#[serial]
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
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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

/// Build an `AdoptWithDelta` merge whose insert delta is one row larger than
/// the keyed adapter's 8,192-row chunk. The source uses two ordinary capped
/// loads so this fixture does not bypass the public write limit it is testing.
async fn setup_branch_merge_multichunk_adopt(dir: &tempfile::TempDir) -> (String, String, u64) {
    const CHUNK_ROWS: usize = 8192;

    let uri = dir.path().to_str().unwrap().to_string();
    let mut db = Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"base","score":0}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();

    let mut first_chunk = String::with_capacity(CHUNK_ROWS * 70);
    for row in 0..CHUNK_ROWS {
        first_chunk.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"merge-row-{row}\",\"score\":1}}}}\n"
        ));
    }
    db.load("feature", &first_chunk, LoadMode::Append)
        .await
        .unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"merge-row-8192","score":1}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        CHUNK_ROWS + 2
    );

    let snapshot = helpers::snapshot_main(&db).await.unwrap();
    let expected_version = snapshot.entry("node:Person").unwrap().table_version;
    let person_uri = node_table_uri(&db, "Person").await;
    drop(db);
    (uri, person_uri, expected_version)
}

/// A crash between pure-insert chunks leaves only a proper prefix of the exact
/// transaction chain on raw Lance HEAD. Armed recovery must restore that
/// prefix before a retry can publish the complete insertion atomically.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_multichunk_insert_armed_prefix_rolls_back() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, person_uri, expected_version) = setup_branch_merge_multichunk_adopt(&dir).await;
    let db = Omnigraph::open(&uri).await.unwrap();

    let operation_id = {
        let _failpoint =
            ScopedFailPoint::new(names::BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS, "return");
        match db.branch_merge("feature", "main").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("between-insert-chunk failure must retain recovery: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(sidecar["protocol_v4"]["effect_phase"], "Armed");
    let person_effect = sidecar["protocol_v4"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|effect| effect["table_key"] == "node:Person")
        .unwrap();
    assert_eq!(
        person_effect["kind"]["planned_transactions"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "8,193 inserted rows must arm two exact transaction identities"
    );
    assert_eq!(
        Dataset::open(&person_uri).await.unwrap().version().version,
        expected_version + 1,
        "only the first insert chunk may be durable at this failpoint"
    );
    assert_eq!(
        helpers::snapshot_main(&db)
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        expected_version
    );
    assert_eq!(count_rows(&db, "node:Person").await, 1);
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Armed partial insert chain must roll back on open");
    assert!(!sidecar_path.exists());
    assert_eq!(count_rows(&recovered, "node:Person").await, 1);
    recovered
        .branch_merge("feature", "main")
        .await
        .expect("the complete multi-chunk insert remains retryable after rollback");
    assert_eq!(count_rows(&recovered, "node:Person").await, 8194);
    let names = collect_column_strings(&read_table(&recovered, "node:Person").await, "name");
    assert!(names.iter().any(|name| name == "merge-row-8192"));
}

/// Once both exact pure-insert transactions are durably confirmed, a
/// pre-manifest crash rolls the one fixed manifest/lineage outcome forward
/// atomically.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_multichunk_effects_confirmed_rolls_forward() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, person_uri, expected_version) = setup_branch_merge_multichunk_adopt(&dir).await;
    let db = Omnigraph::open(&uri).await.unwrap();

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        match db.branch_merge("feature", "main").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("confirmed multi-chunk failure must retain recovery: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(sidecar["protocol_v4"]["effect_phase"], "EffectsConfirmed");
    let person_effect = sidecar["protocol_v4"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|effect| effect["table_key"] == "node:Person")
        .unwrap();
    assert_eq!(
        person_effect["kind"]["planned_transactions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        person_effect["kind"]["confirmed_version"].as_u64(),
        Some(expected_version + 2)
    );
    assert_eq!(
        Dataset::open(&person_uri).await.unwrap().version().version,
        expected_version + 2
    );
    assert_eq!(
        helpers::snapshot_main(&db)
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        expected_version,
        "confirmed table effects stay invisible until the manifest commit"
    );
    assert_eq!(count_rows(&db, "node:Person").await, 1);
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("EffectsConfirmed multi-chunk merge must roll forward on open");
    assert!(!sidecar_path.exists());
    assert_eq!(count_rows(&recovered, "node:Person").await, 8194);
    let names = collect_column_strings(&read_table(&recovered, "node:Person").await, "name");
    assert!(names.iter().any(|name| name == "base"));
    assert!(names.iter().any(|name| name == "merge-row-0"));
    assert!(names.iter().any(|name| name == "merge-row-8192"));
}

/// Build an `AdoptWithDelta` merge whose source removes 8,193 rows. The delete
/// set therefore becomes two row-bounded filters and two exact recovery-owned
/// transactions; the one retained row proves a partial prefix never leaks.
async fn setup_branch_merge_multichunk_delete(dir: &tempfile::TempDir) -> (String, String, u64) {
    const CHUNK_ROWS: usize = 8192;
    const DELETE_QUERY: &str = r#"
query remove_scored() {
    delete Person where score = 1
}
"#;

    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap();
    let mut first_chunk = String::with_capacity(CHUNK_ROWS * 70);
    for row in 0..CHUNK_ROWS {
        first_chunk.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"delete-row-{row}\",\"score\":1}}}}\n"
        ));
    }
    db.load("main", &first_chunk, LoadMode::Append)
        .await
        .unwrap();
    db.load(
        "main",
        "{\"type\":\"Person\",\"data\":{\"name\":\"delete-row-8192\",\"score\":1}}\n\
         {\"type\":\"Person\",\"data\":{\"name\":\"keep\",\"score\":0}}",
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    db.mutate("feature", DELETE_QUERY, "remove_scored", &params(&[]))
        .await
        .unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        1
    );

    let snapshot = helpers::snapshot_main(&db).await.unwrap();
    let expected_version = snapshot.entry("node:Person").unwrap().table_version;
    let person_uri = node_table_uri(&db, "Person").await;
    drop(db);
    (uri, person_uri, expected_version)
}

/// A crash between delete chunks leaves only a proper prefix of the exact
/// transaction chain on raw Lance HEAD. Armed recovery must restore that
/// prefix before a retry can publish the complete deletion atomically.
#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_multichunk_delete_armed_prefix_rolls_back() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, person_uri, expected_version) = setup_branch_merge_multichunk_delete(&dir).await;
    let db = Omnigraph::open(&uri).await.unwrap();

    let operation_id = {
        let _failpoint = ScopedFailPoint::new(names::BRANCH_MERGE_BETWEEN_DELETE_CHUNKS, "return");
        match db.branch_merge("feature", "main").await.unwrap_err() {
            OmniError::RecoveryRequired { operation_id, .. } => operation_id,
            other => panic!("between-delete-chunk failure must retain recovery: {other}"),
        }
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(sidecar["protocol_v4"]["effect_phase"], "Armed");
    let person_effect = sidecar["protocol_v4"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|effect| effect["table_key"] == "node:Person")
        .unwrap();
    assert_eq!(
        person_effect["kind"]["planned_transactions"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "8,193 deleted ids must arm two exact transaction identities"
    );
    assert_eq!(
        Dataset::open(&person_uri).await.unwrap().version().version,
        expected_version + 1,
        "only the first delete chunk may be durable at this failpoint"
    );
    assert_eq!(
        helpers::snapshot_main(&db)
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version,
        expected_version
    );
    assert_eq!(count_rows(&db, "node:Person").await, 8194);
    drop(db);

    let recovered = Omnigraph::open(&uri)
        .await
        .expect("Armed partial delete chain must roll back on open");
    assert!(!sidecar_path.exists());
    assert_eq!(count_rows(&recovered, "node:Person").await, 8194);
    recovered
        .branch_merge("feature", "main")
        .await
        .expect("the complete multi-chunk delete remains retryable after rollback");
    assert_eq!(count_rows(&recovered, "node:Person").await, 1);
    assert_eq!(
        collect_column_strings(&read_table(&recovered, "node:Person").await, "name"),
        ["keep"]
    );
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
/// process dies *mid*-sequence — after some planned commits but while the v4
/// sidecar remains Armed — recovery must roll the whole merge **back**, not
/// publish the partial and record the merge as complete.
///
/// The delta is deliberately MIXED — a fresh id (`bob`, append), a modified base id
/// (`carol`, upsert) and a removed base id (`dave`, delete) — so every partial
/// window leaves real work undone. Proof of rollback: after recovery the target is
/// back at its base name-set, and a *re-run* of the merge re-applies the full delta
/// (the partial was not silently recorded as "already merged").
///
/// RED before the fix: the legacy loose `BranchMerge` classifier rolled any
/// `lance_head > manifest_pinned` forward, so the partial was published (e.g.
/// `bob` present, `dave` kept) and the merge recorded. GREEN after: an Armed v4
/// sidecar can prove only a proper prefix of its planned transaction chain, so
/// recovery compensates it rather than treating numeric movement as completion.
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
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_adopt_partial_after_append_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Adopt,
        "branch_merge.adopt_after_append_pre_upsert",
    )
    .await;
}

#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_adopt_partial_after_upsert_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Adopt,
        "branch_merge.adopt_after_upsert_pre_delete",
    )
    .await;
}

#[tokio::test]
#[serial]
#[serial(branch_merge_phase_b)]
async fn branch_merge_rewrite_partial_after_merge_rolls_back() {
    assert_partial_merge_rolls_back(
        MergeScenario::Rewrite,
        "branch_merge.rewrite_after_merge_pre_delete",
    )
    .await;
}

#[tokio::test]
#[serial]
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
#[serial]
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
        let _fp = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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
        v["merge_source_commit_id"] =
            v["protocol_v4"]["lineage"]["merged_parent_commit_id"].clone();
        v["schema_version"] = serde_json::json!(1);
        v.as_object_mut().unwrap().remove("protocol_v4");
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
#[serial]
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
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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
        RecoveryExpectation::RolledForwardOriginalLineage {
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
#[serial]
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
        let _failpoint = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
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
/// Test setup: RFC-022 leaves index materialization to the reconciler, so the
/// first `ensure_indices` after load builds Person's declared index. A second
/// call is then the steady-state no-work case: zero pins → no sidecar. The
/// failpoint still fires (it sits after the loops), so the call returns Err —
/// but no recovery state persists. Reopen is a clean no-op.
///
/// Actual EnsureIndices sidecar persistence and staged-index failure are covered
/// by `ensure_indices_stage_btree_failure_leaves_existing_tables_writable`; this
/// test deliberately reaches the second, no-work reconciliation pass.
#[tokio::test]
#[serial]
async fn ensure_indices_phase_b_failure_does_not_leak_sidecar_when_no_work_needed() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed, then reconcile the index declaration once. RFC-022 writes publish
    // only their exact data effect; index construction is derived work.
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
        db.ensure_indices().await.unwrap();
    }

    // Setup: trigger the failpoint. Steady-state ensure_indices
    // produces zero sidecar pins (the helpers scope pins to tables
    // that genuinely need work); no sidecar is written. The failpoint
    // still fires, surfacing the Err.
    {
        let db = Omnigraph::open(&uri).await.unwrap();
        let _failpoint = ScopedFailPoint::new(
            names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
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
#[serial]
async fn init_failpoint_after_schema_pg_written_cleans_up_schema_file() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new(names::INIT_AFTER_SCHEMA_PG_WRITTEN, "return");

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
#[serial]
async fn init_failpoint_after_schema_contract_written_cleans_up_all_schema_files() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new(names::INIT_AFTER_SCHEMA_CONTRACT_WRITTEN, "return");

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
#[serial]
async fn init_failpoint_after_coordinator_init_cleans_up_schema_files() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = ScopedFailPoint::new(names::INIT_AFTER_COORDINATOR_INIT, "return");

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
#[serial]
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
    let _failpoint = ScopedFailPoint::new(names::INIT_AFTER_SCHEMA_PG_WRITTEN, "return");

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

// The publisher's outer retry must re-run `load_publish_state` on a RETRYABLE error,
// not propagate it fatally. A bounded internal loop can surface a `RowLevelCasContention`
// on exhaustion EXPECTING this re-run (a clean second scan, by which point a concurrent
// winner has finished). Before the fix, `load_publish_state().await?` short-circuited the
// outer loop — only `merge_rows` conflicts hit the retry — so the typed contention aborted
// the publish. Inject a ONE-SHOT retryable contention into `load_publish_state`: the write
// must still commit, because the publisher retries and the cleared second attempt wins.
#[tokio::test]
#[serial]
async fn publisher_retries_retryable_load_publish_state_error() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::init_and_load(&dir).await;

    // `1*return`: fail only the FIRST `load_publish_state` of the next publish, so the
    // retry's second call is clean. Set after `init_and_load` so its publishes are
    // unaffected.
    let _fp = ScopedFailPoint::new(names::PUBLISH_LOAD_STATE_RETRYABLE_CONTENTION, "1*return");
    let row = r#"{"type":"Person","data":{"name":"Grace","age":37}}"#;
    db.load_as("main", None, row, LoadMode::Merge, None)
        .await
        .expect("publisher must retry the one-shot retryable load_publish_state error and commit");
}

/// `create_branch` can succeed before reopening the new ref fails. That error is
/// post-effect even on the first deferred table: the v3 sidecar must remain so
/// Full recovery can reclaim the exact untouched ref.
#[tokio::test]
#[serial]
async fn first_touch_post_create_open_error_keeps_recovery_ownership() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    let error = {
        let _fp = ScopedFailPoint::new(names::FORK_POST_CREATE_PRE_OPEN, "return");
        db.mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "post-create")], &[("$age", 22)]),
        )
        .await
        .expect_err("post-create reopen failure must fail into recovery")
    };
    assert!(matches!(error, OmniError::RecoveryRequired { .. }));
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        1,
        "ambiguous post-create failure must retain its ownership sidecar"
    );
    let person_uri = node_table_uri(&db, "Person").await;
    assert!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "test seam fires only after the target ref is durable"
    );

    drop(db);
    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&recovered, "feature", "node:Person").await,
        4,
        "failed first touch must not publish its row"
    );
    assert!(
        !lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "Full recovery must reclaim the sidecar-owned untouched ref"
    );
}

/// A branch delete's first recovery probe is not its authority boundary. A data
/// writer can arm an intent in the prepare-to-gate gap; after delete acquires
/// schema/branch/all-table gates the owner is no longer live, so removing the
/// native ref safely makes that branch-local intent unreachable. The next heal
/// must audit/discard it instead of wedging the graph.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_delete_orphans_sidecar_armed_after_initial_barrier() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let initial = helpers::init_and_load(&dir).await;
    initial.branch_create("feature").await.unwrap();
    drop(initial);
    let delete_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let writer_db = Omnigraph::open(&uri).await.unwrap();

    let branch_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_CONTROL_POST_RECOVERY_BARRIER);
    let delete_handle = std::sync::Arc::clone(&delete_db);
    let delete_task = tokio::spawn(async move { delete_handle.branch_delete("feature").await });
    branch_rv.wait_until_reached().await;

    {
        let _fp = ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        writer_db
            .mutate(
                "feature",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "late-sidecar")], &[("$age", 23)]),
            )
            .await
            .expect_err("writer must leave a confirmed sidecar in the branch-control gap");
    }
    branch_rv.release();
    delete_task.await.unwrap().unwrap();
    assert!(
        !delete_db
            .branch_list()
            .await
            .unwrap()
            .iter()
            .any(|branch| branch == "feature"),
        "branch delete must remove the authority that made the sidecar reachable"
    );
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        1,
        "delete leaves the orphaned intent for the audited recovery path"
    );
    writer_db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "after-delete")], &[("$age", 24)]),
        )
        .await
        .expect("the next write must discard the dead-branch sidecar and proceed");
    assert_eq!(
        std::fs::read_dir(dir.path().join("__recovery"))
            .unwrap()
            .count(),
        0,
        "orphan-discard recovery must retire the late sidecar"
    );
}

async fn setup_diverged_merge_branches(dir: &tempfile::TempDir) -> (String, usize) {
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-only")], &[("$age", 34)]),
    )
    .await
    .unwrap();
    db.mutate(
        "target",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "old-target-only")], &[("$age", 35)]),
    )
    .await
    .unwrap();
    drop(db);
    (uri, main_rows)
}

/// A branch merge captures source/target authority before it builds a plan.
/// Once captured, native target delete+recreate must not reuse the same name
/// underneath that plan: both operations join the root-shared schema -> branch
/// gate order, so the control operation linearizes after the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_fences_target_delete_recreate_aba() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;

    // Open both handles before the merge takes the schema gate. Open itself
    // captures one coherent schema contract under that gate.
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let control_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());

    // A recreated Lance ref can reuse the same branch name and numeric
    // version; BranchIdentifier is the incarnation component that prevents
    // that pair from masquerading as the authority captured by the merge.
    let person_uri = node_table_uri(merge_db.as_ref(), "Person").await;
    let old_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    let old_target_version = old_target.version().version;
    let old_target_identifier = old_target.branch_identifier().await.unwrap();

    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);
    let control_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_CONTROL_POST_RECOVERY_BARRIER);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    let control_handle = std::sync::Arc::clone(&control_db);
    let mut control_task = tokio::spawn(async move {
        control_handle.branch_delete("target").await?;
        control_handle.branch_create("target").await?;
        control_handle
            .mutate(
                "target",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "replacement-only")], &[("$age", 36)]),
            )
            .await?;
        Ok::<(), OmniError>(())
    });
    control_rv.wait_until_reached().await;
    control_rv.release();

    // The control task is known to be immediately before its gate acquisition.
    // It must remain blocked while merge holds the target-incarnation gate.
    let control_blocked =
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut control_task)
            .await
            .is_err();
    let target_unchanged_while_parked = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap()
        == old_target_identifier;
    // Always release before assertions so a failed oracle cannot strand the
    // parked callback thread for its 30-second safety bound.
    merge_rv.release();
    assert!(
        control_blocked,
        "target delete+recreate crossed a merge authority window (branch-name ABA)"
    );
    assert!(
        target_unchanged_while_parked,
        "the target ref incarnation changed while merge authority was parked"
    );

    let outcome = merge_task.await.unwrap().unwrap();
    assert_eq!(outcome, omnigraph::db::MergeOutcome::Merged);
    control_task.await.unwrap().unwrap();

    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&reopened, "source", "node:Person").await,
        main_rows + 1,
        "source branch retains the source-only row"
    );
    assert_eq!(
        helpers::count_rows_branch(&reopened, "target", "node:Person").await,
        main_rows + 1,
        "the recreated target must contain only main plus its replacement row"
    );
    let target_names = helpers::collect_column_strings(
        &helpers::read_table_branch(&reopened, "target", "node:Person").await,
        "name",
    );
    assert!(
        target_names.iter().any(|name| name == "replacement-only")
            && !target_names.iter().any(|name| name == "source-only")
            && !target_names.iter().any(|name| name == "old-target-only"),
        "recreated target leaked state from the deleted target incarnation: {target_names:?}"
    );
    let new_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    assert_ne!(
        new_target.branch_identifier().await.unwrap(),
        old_target_identifier,
        "delete+recreate must mint a new target incarnation"
    );
    assert_eq!(
        new_target.version().version,
        old_target_version,
        "the regression fixture must exercise same-name/same-version ABA"
    );
}

/// `sync_branch` replaces a handle's active coordinator. It must join the same
/// schema authority gate held by branch merge, otherwise it can overwrite the
/// temporary target coordinator inside merge's swap -> publish -> restore
/// window and redirect physical effects or the merge commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_fences_concurrent_sync_on_same_handle() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let db = Omnigraph::open(&uri).await.unwrap();
    db.branch_create("other").await.unwrap();
    let db = std::sync::Arc::new(db);
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    let sync_handle = std::sync::Arc::clone(&db);
    let mut sync_task = tokio::spawn(async move { sync_handle.sync_branch("other").await });
    let sync_blocked = tokio::time::timeout(std::time::Duration::from_millis(250), &mut sync_task)
        .await
        .is_err();
    merge_rv.release();
    assert!(
        sync_blocked,
        "sync replaced the active coordinator inside merge's authority window"
    );

    assert_eq!(
        merge_task.await.unwrap().unwrap(),
        omnigraph::db::MergeOutcome::Merged
    );
    sync_task.await.unwrap().unwrap();
    assert_eq!(
        helpers::count_rows_branch(&db, "target", "node:Person").await,
        main_rows + 2,
        "merge must publish both divergent rows to target before sync takes effect"
    );
}

/// The post-table-gate merge check must read storage, not the swapped
/// coordinator's cached snapshot. Advance only the target manifest while merge
/// is parked after authority capture; the table version itself stays unchanged,
/// so only a fresh manifest-incarnation comparison catches the stale plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_rejects_fresh_target_manifest_change_before_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut target_writer = Omnigraph::open(&uri).await.unwrap();
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    let before = helpers::version_branch(&merge_db, "target").await.unwrap();
    // Advance the target's physical HEAD without changing row content, then
    // publish that new pin through the legacy test seam. The merge handle's
    // cached target snapshot remains at `before`.
    let person_uri = node_table_uri(&target_writer, "Person").await;
    let mut raw_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    helpers::lance_delete_inline(&mut raw_target, "1 = 2").await;
    let publish_result = target_writer
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "target",
            "node:Person",
            Some("target"),
        )
        .await;
    let after_result = helpers::version_branch(&target_writer, "target").await;
    // Release before asserting fixture setup so an unexpected setup error does
    // not strand the parked callback thread.
    merge_rv.release();
    publish_result.unwrap();
    let after = after_result.unwrap();
    assert!(after > before, "fixture must advance the target manifest");

    let error = merge_task
        .await
        .unwrap()
        .expect_err("merge must discard a plan prepared from the old target manifest");
    let OmniError::Manifest(manifest_error) = error else {
        panic!("expected a typed read-set conflict");
    };
    assert!(matches!(
        manifest_error.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "branch_merge_target:target"
    ));
    assert!(
        !dir.path().join("__recovery").exists()
            || std::fs::read_dir(dir.path().join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "pre-effect revalidation must fail before merge arms recovery"
    );
}

/// The final merge fence covers physical state as well as manifest authority.
/// Drift injected after authority capture but before the final table gates must
/// be rejected before BranchMerge writes a recovery sidecar that could falsely
/// claim the pre-existing Lance commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_rejects_late_uncovered_target_drift_before_sidecar() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("source").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-only")], &[("$age", 34)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "target-only")], &[("$age", 35)]),
    )
    .await
    .unwrap();
    let manifest_before = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let db = std::sync::Arc::new(db);
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);
    let merge_handle = std::sync::Arc::clone(&db);
    let merge_task = tokio::spawn(async move { merge_handle.branch_merge("source", "main").await });
    merge_rv.wait_until_reached().await;

    let person_uri = node_table_uri(db.as_ref(), "Person").await;
    let mut raw_main = lance::Dataset::open(&person_uri).await.unwrap();
    helpers::lance_delete_inline(&mut raw_main, "1 = 2").await;
    let raw_head = raw_main.version().version;
    assert!(
        raw_head > manifest_before,
        "fixture must create uncovered drift"
    );
    merge_rv.release();

    let err = merge_task
        .await
        .unwrap()
        .expect_err("merge must reject physical drift that predates its recovery intent");
    assert!(
        err.to_string().contains("omnigraph repair"),
        "error should direct the operator to repair; got: {err}"
    );
    assert!(
        !dir.path().join("__recovery").exists()
            || std::fs::read_dir(dir.path().join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "the refusal must happen before BranchMerge arms recovery"
    );
    let manifest_after = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .table_version;
    let head_after = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(manifest_after, manifest_before);
    assert_eq!(head_after, raw_head);
}

/// Source is a captured immutable input, not part of the target publisher's
/// atomic read set. A later commit on the same source incarnation must not make
/// the merge substitute the newer source head (or reject an otherwise-valid
/// captured snapshot).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_source_advance_keeps_captured_source_parent() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let mut source_writer = Omnigraph::open(&uri).await.unwrap();
    let captured_source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    // Model a foreign source writer without changing row content: advance the
    // source table HEAD with a no-op delete, then publish it through the
    // queue-bypassing seam. The source branch incarnation remains unchanged.
    let person_uri = node_table_uri(&source_writer, "Person").await;
    let mut raw_source = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("source")
        .await
        .unwrap();
    helpers::lance_delete_inline(&mut raw_source, "1 = 2").await;
    source_writer
        .failpoint_publish_table_head_without_index_rebuild_for_test(
            "source",
            "node:Person",
            Some("source"),
        )
        .await
        .unwrap();
    let advanced_source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    assert_ne!(advanced_source_head, captured_source_head);
    merge_rv.release();

    assert_eq!(
        merge_task.await.unwrap().unwrap(),
        omnigraph::db::MergeOutcome::Merged
    );
    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&reopened, "target", "node:Person").await,
        main_rows + 2
    );
    let target_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    let merge_commit = reopened.get_commit(&target_head).await.unwrap();
    assert_eq!(
        merge_commit.merged_parent_commit_id.as_deref(),
        Some(captured_source_head.as_str()),
        "merge lineage must name the source commit captured before planning"
    );
    assert_ne!(
        merge_commit.merged_parent_commit_id.as_deref(),
        Some(advanced_source_head.as_str()),
        "a later source head must never be substituted at publish time"
    );
}

/// The proven pure-insert route pins the native source-table incarnation, not
/// only its graph branch name and numeric version. A raw Lance caller can
/// delete and recreate that table ref while merge is between proof and its
/// final table gates; even a numerically newer replacement must be rejected
/// before recovery is armed or target HEAD moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_pure_insert_rejects_source_table_ref_aba_before_arm() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-only")], &[("$age", 34)]),
    )
    .await
    .unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let old_source = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("source")
        .await
        .unwrap();
    let old_source_version = old_source.version().version;
    let old_source_identifier = old_source.branch_identifier().await.unwrap();
    let merge_db = std::sync::Arc::new(db);
    let mut source_writer = Omnigraph::open(&uri).await.unwrap();
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_CANDIDATE_VALIDATION);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task = tokio::spawn(async move { merge_handle.branch_merge("source", "main").await });
    merge_rv.wait_until_reached().await;

    // Recreate the table ref from main and advance it with empty transactions.
    // Publish only the replacement source pin through the test seam; the
    // graph-level source branch itself remains the same incarnation.
    let replacement_result = async {
        let mut root = lance::Dataset::open(&person_uri)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let main_version = root.version().version;
        root.force_delete_branch("source")
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if let Err(error) = std::fs::remove_dir_all(
            std::path::Path::new(&person_uri)
                .join("tree")
                .join("source"),
        ) && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
        root.create_branch("source", main_version, None)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let mut replacement = root
            .checkout_branch("source")
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        // The manifest publisher refuses to register the same table version a
        // second time for one logical table identity, so advance the recreated
        // ref beyond the captured version. The final native identifier check,
        // not numeric monotonicity, must still reject it.
        helpers::lance_delete_inline(&mut replacement, "1 = 2").await;
        helpers::lance_delete_inline(&mut replacement, "1 = 2").await;
        source_writer
            .failpoint_publish_table_head_without_index_rebuild_for_test(
                "source",
                "node:Person",
                Some("source"),
            )
            .await?;
        Ok::<_, OmniError>((
            replacement.version().version,
            replacement
                .branch_identifier()
                .await
                .map_err(|error| OmniError::Lance(error.to_string()))?,
        ))
    }
    .await;
    // Always release the parked merge before checking fixture assertions.
    merge_rv.release();
    let (replacement_version, replacement_identifier) = replacement_result.unwrap();
    assert!(
        replacement_version > old_source_version,
        "fixture must publish a numerically newer replacement source ref"
    );
    assert_ne!(
        replacement_identifier, old_source_identifier,
        "delete/recreate must mint a distinct native source-table incarnation"
    );

    let error = merge_task
        .await
        .unwrap()
        .expect_err("merge must reject the replacement source-table ref");
    let OmniError::Manifest(manifest_error) = error else {
        panic!("expected a typed read-set conflict");
    };
    assert!(matches!(
        manifest_error.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "branch_merge_source_table_head:node:Person"
    ));
    assert_eq!(
        helpers::count_rows_branch(&merge_db, "main", "node:Person").await,
        main_rows,
        "pre-arm source ABA must not move the target"
    );
    assert!(
        !dir.path().join("__recovery").exists()
            || std::fs::read_dir(dir.path().join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "source ABA must fail before recovery is armed"
    );
}

/// The no-target-probe pure-insert route is sound only while the live target
/// table ref is the exact native incarnation on which the source absence proof
/// was founded. Replace an already-owned target ref outside OmniGraph's queues
/// at the same numeric version and with the same logical rows; the final
/// under-gate BranchIdentifier check must reject that ABA before recovery is
/// armed or the graph-visible target moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_pure_insert_rejects_target_table_ref_aba_before_arm() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;

    // Materialize an owned target table ref without changing its logical row
    // image, then fork source from that exact graph commit. The one source-only
    // all-new upsert is therefore a certificate-proven descendant of the owned
    // target ref, not of main's inherited table ref.
    db.branch_create("target").await.unwrap();
    db.mutate(
        "target",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 30)]),
    )
    .await
    .unwrap();
    db.branch_create_from(omnigraph::db::ReadTarget::branch("target"), "source")
        .await
        .unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-only")], &[("$age", 34)]),
    )
    .await
    .unwrap();

    let target_snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("target"))
        .await
        .unwrap();
    let target_entry = target_snapshot.entry("node:Person").unwrap();
    assert_eq!(
        target_entry.table_branch.as_deref(),
        Some("target"),
        "fixture requires an already-owned target table ref"
    );
    assert_eq!(target_entry.row_count, main_rows as u64);
    let expected_target_version = target_entry.table_version;
    let target_head_before = branch_head_commit_id(dir.path(), "target").await.unwrap();
    let source_head_before = branch_head_commit_id(dir.path(), "source").await.unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let old_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    assert_eq!(old_target.version().version, expected_target_version);
    let old_target_identifier = old_target.branch_identifier().await.unwrap();

    let merge_db = std::sync::Arc::new(db);
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_CANDIDATE_VALIDATION);
    let probes = MergeWriteProbes::default();
    let task_probes = probes.clone();
    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task = tokio::spawn(async move {
        with_merge_write_probes(task_probes, merge_handle.branch_merge("source", "target")).await
    });
    merge_rv.wait_until_reached().await;

    // Lance's public branch API correctly refuses to delete a target ref that
    // the source identifier still references. Simulate the adversarial
    // lower-level ABA by replacing only the authoritative BranchContents
    // identifier. The target tree, path, numeric version, and logical rows all
    // remain unchanged, so only incarnation-aware validation can catch it.
    let target_ref_path = std::path::Path::new(&person_uri)
        .join("_refs")
        .join("branches")
        .join("target.json");
    let replacement_result = (|| {
        let bytes = std::fs::read(&target_ref_path)?;
        let mut contents: lance::dataset::refs::BranchContents = serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if contents.parent_branch.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "target ABA fixture expected a main-parented target table ref",
            ));
        }
        contents.identifier = lance::dataset::refs::BranchIdentifier::new(
            &lance::dataset::refs::BranchIdentifier::main(),
            contents.parent_version,
        );
        let bytes = serde_json::to_vec_pretty(&contents)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(&target_ref_path, bytes)?;
        Ok::<_, std::io::Error>(contents.identifier)
    })();
    // Always release before checking fixture assertions so a failed raw-Lance
    // setup cannot strand the parked callback thread.
    merge_rv.release();
    let replacement_identifier = replacement_result.unwrap();
    assert_ne!(
        replacement_identifier, old_target_identifier,
        "raw ref replacement must mint a distinct native target-table incarnation"
    );
    let replacement_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    assert_eq!(
        replacement_target.version().version,
        expected_target_version,
        "fixture must preserve the captured target numeric version"
    );
    assert_eq!(
        replacement_target.branch_identifier().await.unwrap(),
        replacement_identifier,
        "fixture must expose the replacement native target-table incarnation"
    );
    assert!(
        probes.proven_insert_raw_batch_calls() > 0,
        "fixture must reach the proven source-interval route before parking"
    );
    assert_eq!(
        probes.ordered_cursor_scan_calls(),
        0,
        "fixture must not fall back to the general ordered diff"
    );

    let error = merge_task
        .await
        .unwrap()
        .expect_err("merge must reject the replacement target-table ref");
    let OmniError::Manifest(manifest_error) = error else {
        panic!("expected a typed read-set conflict");
    };
    assert!(matches!(
        manifest_error.details,
        Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
            ref member,
            ..
        }) if member == "branch_merge_target_table_incarnation:node:Person"
    ));

    assert_eq!(
        branch_head_commit_id(dir.path(), "target").await.unwrap(),
        target_head_before,
        "pre-arm target ABA must not publish merge lineage"
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "source").await.unwrap(),
        source_head_before,
        "rejected merge must not move its captured source"
    );
    assert_eq!(
        helpers::count_rows_branch(&merge_db, "target", "node:Person").await,
        main_rows,
        "rejected merge must leave the target graph row image unchanged"
    );
    assert_eq!(
        helpers::count_rows_branch(&merge_db, "source", "node:Person").await,
        main_rows + 1,
        "rejected merge must leave the source graph row image unchanged"
    );
    let target_names = helpers::collect_column_strings(
        &helpers::read_table_branch(&merge_db, "target", "node:Person").await,
        "name",
    );
    assert!(
        !target_names.iter().any(|name| name == "source-only"),
        "source-only row leaked into rejected target merge: {target_names:?}"
    );
    let final_target = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch("target")
        .await
        .unwrap();
    assert_eq!(final_target.version().version, expected_target_version);
    assert_eq!(
        final_target.branch_identifier().await.unwrap(),
        replacement_identifier,
        "rejected merge must not move the raw replacement target ref"
    );
    assert!(
        !dir.path().join("__recovery").exists()
            || std::fs::read_dir(dir.path().join("__recovery"))
                .unwrap()
                .next()
                .is_none(),
        "target ABA must fail before recovery is armed"
    );
}

async fn assert_branch_merge_first_touch_ref_is_recovered(
    failpoint: &str,
    ref_exists_before_recovery: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-first-touch")], &[("$age", 37)]),
    )
    .await
    .unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    assert!(
        !person.list_branches().await.unwrap().contains_key("target"),
        "fixture requires the target table ref to be lazy"
    );

    let error = {
        let _failpoint = ScopedFailPoint::new(failpoint, "return");
        db.branch_merge("source", "target").await.unwrap_err()
    };
    let operation_id = match error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("first-touch failure must retain recovery ownership: {other}"),
    };
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 9);
    assert_eq!(
        sidecar["protocol_v4"]["effects"][0]["kind"]["kind"],
        "RefOnlyFork"
    );

    let person = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        person.list_branches().await.unwrap().contains_key("target"),
        ref_exists_before_recovery,
        "fixture must stop at the intended sidecar/ref boundary"
    );
    drop(db);

    let recovered = Omnigraph::open(&uri).await.unwrap();
    assert!(!sidecar_path.exists());
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    assert!(
        !person.list_branches().await.unwrap().contains_key("target"),
        "Full recovery must reclaim an unpublished first-touch target ref"
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows,
        "failed first-touch merge must leave target inheriting its old image"
    );

    assert_eq!(
        recovered.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::FastForward
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1
    );
}

#[tokio::test]
#[serial(branch_merge_first_touch)]
async fn branch_merge_sidecar_precedes_first_touch_target_ref() {
    let _scenario = FailScenario::setup();
    assert_branch_merge_first_touch_ref_is_recovered(
        names::BRANCH_MERGE_POST_SIDECAR_PRE_FORK,
        false,
    )
    .await;
}

#[tokio::test]
#[serial(branch_merge_first_touch)]
async fn branch_merge_recovers_ambiguous_first_touch_ref_creation() {
    let _scenario = FailScenario::setup();
    assert_branch_merge_first_touch_ref_is_recovered(names::FORK_POST_CREATE_PRE_OPEN, true).await;
}

/// A legacy writer can arm a relevant sidecar after merge's initial recovery
/// barrier. Merge acquires the complete source/target table envelope and lists
/// again before Phase A, so the late intent blocks it even when no table HEAD
/// moved and a version-only check would pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_rechecks_late_sidecar_after_table_gates() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(Omnigraph::open(&uri).await.unwrap());
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    const OPERATION_ID: &str = "01H000000000000000000LATE";
    let recovery_dir = dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    std::fs::write(
        recovery_dir.join(format!("{OPERATION_ID}.json")),
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "operation_id": OPERATION_ID,
            "started_at": "0",
            "branch": "target",
            "actor_id": null,
            "writer_kind": "EnsureIndices",
            "tables": []
        }))
        .unwrap(),
    )
    .unwrap();

    merge_rv.release();
    let error = merge_task
        .await
        .unwrap()
        .expect_err("late recovery ownership must block merge before effects");
    assert!(matches!(
        error,
        OmniError::RecoveryRequired {
            ref operation_id,
            ..
        } if operation_id == OPERATION_ID
    ));
    assert_eq!(
        helpers::count_rows_branch(&merge_db, "target", "node:Person").await,
        main_rows + 1,
        "blocked merge must leave the target image unchanged"
    );
}

/// Cleanup's fast sidecar probe is only an optimization. A writer can fail
/// after that probe; the schema/branch/table GC envelope and final recheck must
/// refuse before Lance can delete recovery history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cleanup_rechecks_sidecars_under_gc_gates() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    drop(helpers::init_and_load(&dir).await);

    let cleanup_rv =
        helpers::failpoint::Rendezvous::park_first(names::CLEANUP_POST_RECOVERY_CHECK_PRE_GATES);
    let cleanup_uri = uri.clone();
    let cleanup_task = tokio::spawn(async move {
        let mut db = Omnigraph::open(&cleanup_uri).await.unwrap();
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
    });
    cleanup_rv.wait_until_reached().await;

    let writer = Omnigraph::open(&uri).await.unwrap();
    {
        let _fp = ScopedFailPoint::new(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, "return");
        writer
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "gc-race")], &[("$age", 24)]),
            )
            .await
            .expect_err("writer must leave a sidecar after cleanup's fast probe");
    }
    let person_uri = node_table_uri(&writer, "Person").await;
    let before_versions = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .versions()
        .await
        .unwrap()
        .len();
    cleanup_rv.release();
    let error = cleanup_task
        .await
        .unwrap()
        .expect_err("authoritative under-gate check must refuse cleanup");
    assert!(error.to_string().contains("after acquiring its GC gates"));
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .versions()
            .await
            .unwrap()
            .len(),
        before_versions,
        "refused cleanup must not delete any recovery version history"
    );
}

/// Full recovery must classify the sidecar body it re-reads after discovery,
/// not the stale parsed copy from its directory listing. Branch merge now holds
/// the schema gate for its complete authority window, so a second open cannot
/// discover its live sidecar underneath it. Instead, leave a confirmed crash
/// residual, park recovery after discovery, change that body to the valid
/// unconfirmed crash shape, and require the fresh body to drive rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn full_recovery_rereads_sidecar_body_after_discovery() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "merge-feature")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "merge-main")], &[("$age", 32)]),
    )
    .await
    .unwrap();
    drop(db);

    {
        let _publish_failure = ScopedFailPoint::new(
            names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        Omnigraph::open(&uri)
            .await
            .unwrap()
            .branch_merge("feature", "main")
            .await
            .expect_err("merge must leave a confirmed pre-publish sidecar");
    }

    let recovery_rv =
        helpers::failpoint::Rendezvous::park_first(names::RECOVERY_POST_LIST_PRE_GATES);
    let recovery_uri = uri.clone();
    let recovery_task = tokio::spawn(async move { Omnigraph::open(&recovery_uri).await });
    recovery_rv.wait_until_reached().await;

    let operation_id = single_sidecar_operation_id(dir.path());
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{operation_id}.json"));
    let mut sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sidecar_path).unwrap()).unwrap();
    let tables = sidecar["tables"]
        .as_array_mut()
        .expect("branch-merge sidecar tables must be an array");
    assert!(
        tables
            .iter()
            .any(|table| !table["confirmed_version"].is_null()),
        "fixture must begin with a confirmed BranchMerge residual"
    );
    for table in tables {
        table["confirmed_version"] = serde_json::Value::Null;
    }
    let protocol = sidecar["protocol_v4"]
        .as_object_mut()
        .expect("branch-merge sidecar must carry protocol_v4");
    protocol.insert(
        "effect_phase".to_string(),
        serde_json::Value::String("Armed".to_string()),
    );
    for effect in protocol["effects"]
        .as_array_mut()
        .expect("branch-merge effects must be an array")
    {
        effect["kind"]["confirmed_version"] = serde_json::Value::Null;
        effect["kind"]["confirmed_branch_identifier"] = serde_json::Value::Null;
    }
    for slot in protocol["intended_delta"]["table_updates"]
        .as_array_mut()
        .expect("branch-merge delta slots must be an array")
    {
        slot["confirmed"] = serde_json::Value::Null;
    }
    std::fs::write(
        &sidecar_path,
        serde_json::to_string_pretty(&sidecar).unwrap(),
    )
    .unwrap();

    recovery_rv.release();
    let recovered = recovery_task
        .await
        .unwrap()
        .expect("full recovery must consume the freshly re-read body");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        5,
        "fresh unconfirmed sidecar must roll the interrupted merge back; stale discovery would expose six rows"
    );
}
