#![cfg(feature = "failpoints")]
#![recursion_limit = "512"]

mod helpers;

use std::process::Command;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use lance::Dataset;
use omnigraph::db::{Omnigraph, ReadTarget, StagingVerdict};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use omnigraph::seams::FailScenario;
use omnigraph::seams::catalog;
use serial_test::serial;

use helpers::collector::{
    detached_versions, insert_person, keep_one, main_plan, merge_three_chunk_chain, staged_since,
};
use helpers::recovery::{branch_head_commit_id, sidecar_operation_ids};
use helpers::{
    MUTATION_QUERIES, Session, TEST_QUERIES, collect_column_strings, count_rows,
    forge_linear_head_from_pin, init_and_load, is_detached_version, mixed_params, mutate_main,
    node_blob_cell, params, pinned_version, read_managed_blob_bytes, read_table, version_main,
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
const RFC023_EXTERNAL_ACTION_ENV: &str = "OMNIGRAPH_RFC023_EXTERNAL_ACTION";

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
        .dataset(&table_key)
        .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
        .dataset_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

/// A graph with rows and no built indexes: the index writer has work on
/// every table. `init_and_load` builds the indexes; this does not.
async fn graph_with_unbuilt_indexes(dir: &tempfile::TempDir) -> Session {
    use omnigraph::loader::LoadMode;
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());
    db.load_jsonl(helpers::TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
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
    run_rfc023_external_writer_action(uri, mode, payload, "load")
}

fn run_rfc023_external_writer_action(
    uri: String,
    mode: LoadMode,
    payload: String,
    action: &'static str,
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
        .env(RFC023_EXTERNAL_ACTION_ENV, action)
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
            let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
            let action =
                std::env::var(RFC023_EXTERNAL_ACTION_ENV).unwrap_or_else(|_| "load".to_string());
            match action.as_str() {
                "load" => {
                    db.load("main", &payload, mode).await.unwrap();
                }
                "collector_late_live" | "collector_late_retired" => {
                    db.branch_create("collector-late").await.unwrap();
                    db.load("collector-late", &payload, mode).await.unwrap();
                    if action == "collector_late_retired" {
                        db.branch_merge("collector-late", "main").await.unwrap();
                        db.branch_delete("collector-late").await.unwrap();
                    }
                }
                "collector_late_stranded" => {
                    db.branch_create("collector-late").await.unwrap();
                    let _scenario = FailScenario::setup();
                    {
                        let _fp = catalog::MUTATION_POST_TABLE_COMMIT.fail_once_at(1);
                        let outcome = db
                            .mutate(
                                "collector-late",
                                MUTATION_QUERIES,
                                "insert_person",
                                &mixed_params(&[("$name", "collector-stranded")], &[("$age", 40)]),
                            )
                            .await;
                        assert!(outcome.is_err(), "the staged write must not publish");
                    }
                    db.branch_delete("collector-late").await.unwrap();
                }
                "collector_late_base_carrier" => {
                    db.branch_create("carrier").await.unwrap();
                    db.load("carrier", &payload, LoadMode::Merge).await.unwrap();
                    db.branch_merge("carrier", "source").await.unwrap();
                    db.branch_merge("carrier", "target").await.unwrap();
                    db.branch_delete("carrier").await.unwrap();
                }
                "collector_advance_merge_source" => {
                    db.load("source", &payload, mode).await.unwrap();
                    let rows = db.cleanup(keep_one()).await.unwrap();
                    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
                }
                "collector_mixed" => {
                    db.load("main", &payload, mode).await.unwrap();
                    db.branch_merge("main", "collector-existing").await.unwrap();
                    db.load(
                        "collector-existing",
                        r#"{"type":"Person","data":{"name":"collector-follow-up","age":41}}"#,
                        LoadMode::Append,
                    )
                    .await
                    .unwrap();
                }
                other => panic!("unknown external writer action '{other}'"),
            }
        });
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

/// The one node dataset directory the live manifest does not register: the
/// leftover of an abandoned add-type create (RFC 0067).
async fn unregistered_node_table_uri(db: &Omnigraph) -> String {
    let registered: std::collections::BTreeSet<String> = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .datasets()
        .map(|entry| entry.dataset_path.trim_start_matches('/').to_string())
        .collect();
    let root = db.uri().trim_end_matches('/').to_string();
    let mut orphans: Vec<String> = std::fs::read_dir(format!("{root}/nodes"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| !registered.contains(&format!("nodes/{name}")))
        .collect();
    orphans.sort();
    assert_eq!(
        orphans.len(),
        1,
        "expected exactly one unregistered node dataset, got {orphans:?}"
    );
    format!("{root}/nodes/{}", orphans[0])
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
        let _fp = catalog::BRANCH_CREATE_POST_NATIVE.fire_always();
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
        let _fp = catalog::BRANCH_DELETE_POST_NATIVE.fire_always();
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

/// Branch delete leaves the deleted branch's detached staging for `cleanup`:
/// a failed collection keeps it and the retry reclaims it.
#[tokio::test]
#[serial]
async fn branch_delete_cleanup_failure_converges_on_retry() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let person_uri = node_table_uri(&main, "Person").await;
    let before = detached_versions(&person_uri).await;
    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    let former_staging = staged_since(&person_uri, &before).await;
    assert!(
        helpers::snapshot_branch(&main, "feature")
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .is_none(),
        "the branch write keeps main's registration: it never forks"
    );
    main.branch_delete("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main".to_string()]);
    assert!(
        detached_versions(&person_uri)
            .await
            .contains(&former_staging),
        "delete leaves the branch's staging for cleanup"
    );
    {
        let _fp = catalog::CLEANUP_TABLE_GC.fire_always();
        main.cleanup(keep_one()).await.unwrap();
    }
    assert!(
        detached_versions(&person_uri)
            .await
            .contains(&former_staging),
        "failed cleanup preserves the deleted branch's staging"
    );
    main.cleanup(keep_one()).await.unwrap();
    assert!(
        !detached_versions(&person_uri)
            .await
            .contains(&former_staging),
        "cleanup retry reclaims the deleted branch's staging"
    );

    main.branch_create("feature").await.unwrap();
    let feature2 = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature2,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();
}

/// A recreated branch writes while the deleted incarnation's staging awaits
/// `cleanup`; the collector then reclaims the old staging and keeps the pin.
#[tokio::test]
#[serial]
async fn recreate_over_a_deleted_branch_writes_without_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let person_uri = node_table_uri(&main, "Person").await;
    let before = detached_versions(&person_uri).await;
    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    let first_staging = staged_since(&person_uri, &before).await;
    main.branch_delete("feature").await.unwrap();
    assert!(
        detached_versions(&person_uri)
            .await
            .contains(&first_staging)
    );
    main.branch_create("feature").await.unwrap();
    let before_second = detached_versions(&person_uri).await;
    let feature2 = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature2,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .expect("a new branch writes while the former staging awaits cleanup");
    assert_eq!(
        helpers::count_rows_branch(&feature2, "feature", "node:Person").await,
        helpers::count_rows(&main, "node:Person").await + 1,
    );
    let second_staging = staged_since(&person_uri, &before_second).await;
    assert_ne!(first_staging, second_staging);
    assert_eq!(
        pinned_version(&feature2, "feature", "node:Person").await,
        second_staging,
        "the recreated branch pins its own staging"
    );
    assert!(
        helpers::snapshot_branch(&feature2, "feature")
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .is_none(),
        "the recreated branch keeps main's registration: it never forks"
    );
    let remaining = detached_versions(&person_uri).await;
    assert!(
        remaining.contains(&first_staging),
        "writes do not collect the former staging"
    );
    assert!(remaining.contains(&second_staging));
    main.cleanup(keep_one()).await.unwrap();
    let remaining = detached_versions(&person_uri).await;
    assert!(!remaining.contains(&first_staging));
    assert!(
        remaining.contains(&second_staging),
        "cleanup preserves the live pin"
    );
    drop(feature2);
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&reopened, "feature", "node:Person").await,
        5
    );
}

/// Branch delete acknowledges without collecting: the deleted branch's
/// staging survives delete and reopen, and only `cleanup` reclaims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn branch_delete_acknowledges_with_staging_awaiting_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let person_uri = node_table_uri(&main, "Person").await;
    let before = detached_versions(&person_uri).await;
    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    let former_staging = staged_since(&person_uri, &before).await;
    {
        let _fp = catalog::CLEANUP_RECONCILE_FORK.panic_at();
        main.branch_delete("feature").await.unwrap();
        assert_eq!(main.branch_list().await.unwrap(), vec!["main".to_string()]);
    }
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string()]
    );
    assert!(
        detached_versions(&person_uri)
            .await
            .contains(&former_staging),
        "delete and reopen defer collection of the branch's staging"
    );
    main.cleanup(keep_one()).await.unwrap();
    assert!(
        !detached_versions(&person_uri)
            .await
            .contains(&former_staging)
    );
}

/// Branch recreate completes without waiting for `cleanup` while the deleted
/// incarnation's staging awaits collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial]
async fn branch_recreate_completes_while_old_staging_awaits_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let main = helpers::init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let person_uri = node_table_uri(&main, "Person").await;
    let before = detached_versions(&person_uri).await;
    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    drop(feature);

    let first_staging = staged_since(&person_uri, &before).await;
    let racer = helpers::session(Omnigraph::open(&uri).await.unwrap());
    main.branch_delete("feature").await.unwrap();
    let racer = {
        let _fp = catalog::CLEANUP_RECONCILE_FORK.panic_at();
        let create =
            tokio::spawn(async move { racer.branch_create("feature").await.map(|()| racer) });
        tokio::time::timeout(std::time::Duration::from_secs(30), create)
            .await
            .expect("recreate must complete without waiting for table-fork cleanup")
            .expect("recreate task must not panic")
            .unwrap()
    };
    assert!(
        detached_versions(&person_uri)
            .await
            .contains(&first_staging)
    );
    let before_second = detached_versions(&person_uri).await;
    helpers::mutate_branch(
        &racer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();
    let second_staging = staged_since(&person_uri, &before_second).await;
    assert_ne!(first_staging, second_staging);
    assert_eq!(
        pinned_version(&racer, "feature", "node:Person").await,
        second_staging
    );
    let remaining = detached_versions(&person_uri).await;
    assert!(remaining.contains(&first_staging));
    assert!(remaining.contains(&second_staging));
    main.cleanup(keep_one()).await.unwrap();
    let remaining = detached_versions(&person_uri).await;
    assert!(!remaining.contains(&first_staging));
    assert!(remaining.contains(&second_staging));
    assert_eq!(
        helpers::count_rows_branch(&racer, "feature", "node:Person").await,
        5
    );
}

/// A branch write never forks, so it never consults the cleanup classifier:
/// it lands beside a forged orphan ref that only `cleanup` reclaims.
#[tokio::test]
#[serial]
async fn branch_write_ignores_unavailable_cleanup_classifier() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    let person_uri = node_table_uri(&db, "Person").await;
    let feature_native = helpers::graph_native_ref(&uri, "feature").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch(&feature_native, base, None).await.unwrap();
    }

    let orphan_identifier = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .checkout_branch(&feature_native)
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    let row = r#"{"type":"Person","data":{"name":"Grace","age":37}}"#;
    {
        let _fp = catalog::CLASSIFY_FRESH_READ.fire_always();
        db.load_as("feature", None, row, LoadMode::Merge, None)
            .await
            .expect("fresh writes do not require cleanup classification");
    }
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let published = helpers::snapshot_branch(&db, "feature").await.unwrap();
    assert!(
        published
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .is_none(),
        "the branch write keeps main's registration: it never forks"
    );
    assert!(is_detached_version(
        pinned_version(&db, "feature", "node:Person").await
    ));
    let root_dataset = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        root_dataset
            .checkout_branch(&feature_native)
            .await
            .unwrap()
            .branch_identifier()
            .await
            .unwrap(),
        orphan_identifier
    );
    assert_eq!(
        helpers::count_rows_branch(&db, "feature", "node:Person").await,
        5
    );
    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    let branches = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .list_branches()
        .await
        .unwrap();
    assert!(
        !branches.contains_key(&feature_native),
        "cleanup reclaims the forged orphan ref"
    );
    drop(db);
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&reopened, "feature", "node:Person").await,
        5
    );
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
    let db = helpers::init_and_load(&dir).await;

    // Forge an orphaned fork on the Person table (a reconcile target).
    let person_uri = node_table_uri(&db, "Person").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("ghost", base, None).await.unwrap();
    }

    // One table's version GC fails once; the sweep must isolate it.
    let _fp = catalog::CLEANUP_TABLE_GC.fire_once_at(1);
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
            !ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "ghost")),
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
    let db = helpers::init_and_load(&dir).await;

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
        let _fp = catalog::CLEANUP_RECONCILE_FORK.fire_once_at(1);
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
            ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "ghost")),
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
            !ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "ghost")),
            "the second cleanup should reconcile the orphan"
        );
    }
}

/// A `classify.fresh_read` failure on a live branch leaves a
/// manifest-unreferenced fork in place, since cleanup never deletes on an
/// unconfirmed read; the next cleanup reclaims it.
#[tokio::test]
#[serial]
async fn reconcile_skips_fork_when_fresh_recheck_is_unavailable_then_converges() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    // Forge a manifest-unreferenced Person fork on the live `feature` branch —
    // a genuine orphan the reconciler would normally reclaim.
    let person_uri = node_table_uri(&db, "Person").await;
    let feature_native = helpers::graph_native_ref(dir.path().to_str().unwrap(), "feature").await;
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch(&feature_native, base, None).await.unwrap();
        assert!(
            ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "feature")),
            "precondition: forged orphan fork present"
        );
    }

    // With the fresh re-check failing, the fork's status is Indeterminate (the
    // branch is live but unreadable) → cleanup must SKIP it, not delete.
    {
        let _fp = catalog::CLASSIFY_FRESH_READ.fire_always();
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
        let ds = lance::Dataset::open(&person_uri).await.unwrap();
        assert!(
            ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "feature")),
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
            !ds.list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "feature")),
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

    let rv = helpers::failpoint::Rendezvous::park_first(&catalog::FORK_BEFORE_CLASSIFY);

    let uri_a = uri.clone();
    let writer_a = tokio::spawn(async move {
        let a = helpers::session(Omnigraph::open(&uri_a).await.unwrap());
        helpers::mutate_branch(
            &a,
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
    let b = helpers::session(Omnigraph::open(&uri).await.unwrap());
    helpers::mutate_branch(
        &b,
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

    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
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

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn graph_publish_failpoint_triggers_before_commit_append() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::session(
        Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
            .await
            .unwrap(),
    );
    let _failpoint = catalog::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND.fire_always();

    let err = mutate_main(
        &db,
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
        let db = Arc::new(helpers::session(
            Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap(),
        ));
        let probes = MergeWriteProbes::default();

        let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::FORK_BEFORE_CLASSIFY);
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
                        ref type_key,
                        entity_id: Some(ref entity_id),
                    } if type_key == "node:Person" && entity_id == "racer"
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

        if mode == LoadMode::Append {
            assert_eq!(probes.stage_merge_insert_calls(), 0);
            assert_eq!(
                probes.stage_fenced_insert_calls(),
                expected_attempts,
                "{case}: strict must stage exactly one join-free fenced attempt"
            );
        } else {
            assert_eq!(
                probes.stage_merge_insert_calls(),
                expected_attempts,
                "{case}: upsert must stage a fresh second attempt"
            );
            assert_eq!(probes.stage_fenced_insert_calls(), 0);
        }
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
    let db = Arc::new(helpers::session(
        Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap(),
    ));
    // Open the publisher before manufacturing physical drift. A normal open
    // after the raw append would correctly refuse the uncovered HEAD.
    let publisher = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let person_uri = node_table_uri(&db, "Person").await;
    let probes = MergeWriteProbes::default();

    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::FORK_BEFORE_CLASSIFY);
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
        ["__id", "name", "score"],
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
        probes.stage_fenced_insert_calls(),
        2,
        "the stale strict attempt must be abandoned and staged again from fresh authority"
    );
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());

    let observer = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let mut names = collect_column_strings(&read_table(&observer, "node:Person").await, "name");
    names.sort();
    assert_eq!(names, ["foreign-disjoint", "strict-a"]);
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
    let db = std::sync::Arc::new(helpers::session(
        Omnigraph::init(uri, OCC_UNIQUE_SCHEMA).await.unwrap(),
    ));

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
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
        .dataset("node:User")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:User")
        .unwrap()
        .published_dataset_version;
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
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
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

/// A caller graph-head precondition is terminal even when the race occurs
/// after the initial capture. Update and delete are intentionally not eligible
/// for internal reprepare, so the authoritative under-gate check must map both
/// shapes to `PreconditionFailed` rather than leaking the engine's internal
/// `ReadSetChanged` classification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn conditional_update_and_delete_races_return_precondition_failed_before_effects() {
    let _scenario = FailScenario::setup();

    for query_name in ["set_age", "remove_person"] {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(helpers::init_and_load(&dir).await);
        let expected = branch_head_commit_id(dir.path(), "main").await.unwrap();
        let mutation_params = if query_name == "set_age" {
            helpers::mixed_params(&[("$name", "Alice")], &[("$age", 99)])
        } else {
            helpers::mixed_params(&[("$name", "Alice")], &[])
        };

        let rendezvous = helpers::failpoint::Rendezvous::park_first(
            &catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE,
        );
        let writer_a_db = std::sync::Arc::clone(&db);
        let expected_for_writer = expected.clone();
        let writer_a = tokio::spawn(async move {
            writer_a_db
                .mutate_as_with_expected_head(
                    "main",
                    MUTATION_QUERIES,
                    query_name,
                    &mutation_params,
                    None,
                    Some(&expected_for_writer),
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
        .expect("the disjoint winner commits while the conditional mutation is parked");
        let winner_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
        let winner_person_pin = helpers::snapshot_main(&db)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version;
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
            .expect_err("the raced caller precondition must fail");
        match err {
            OmniError::PreconditionFailed {
                branch,
                expected: actual_expected,
                actual,
            } => {
                assert_eq!(branch, "main");
                assert_eq!(actual_expected, expected);
                assert_eq!(actual.as_deref(), Some(winner_head.as_str()));
            }
            other => {
                panic!("conditional {query_name} race must be PreconditionFailed, got: {other}")
            }
        }

        let final_person_pin = helpers::snapshot_main(&db)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version;
        let final_person_head = lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .latest_version_id()
            .await
            .unwrap();
        assert_eq!(
            (final_person_pin, final_person_head),
            (winner_person_pin, winner_person_head),
            "conditional {query_name} must fail before any Person effect"
        );
        assert_eq!(
            count_rows(&db, "node:Person").await,
            4,
            "conditional {query_name} must preserve the fixture rows"
        );
    }

    // A zero-match update has no staged table transaction and therefore uses
    // the no-effect branch-gated linearization path instead of `commit_all`.
    // It must not acknowledge a stale caller token merely because there is no
    // payload effect to publish.
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(helpers::init_and_load(&dir).await);
    let expected = branch_head_commit_id(dir.path(), "main").await.unwrap();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_NO_EFFECT_PRE_GATE);
    let writer_a_db = std::sync::Arc::clone(&db);
    let expected_for_writer = expected.clone();
    let writer_a = tokio::spawn(async move {
        writer_a_db
            .mutate_as_with_expected_head(
                "main",
                MUTATION_QUERIES,
                "set_age",
                &helpers::mixed_params(&[("$name", "Missing")], &[("$age", 99)]),
                None,
                Some(&expected_for_writer),
            )
            .await
    });

    rendezvous.wait_until_reached().await;
    db.mutate(
        "main",
        OCC_DISJOINT_MUTATIONS,
        "insert_company",
        &params(&[("$name", "NoOpRaceWinner")]),
    )
    .await
    .expect("the winner commits while the conditional no-op is parked");
    let winner_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    rendezvous.release();

    let err = writer_a
        .await
        .unwrap()
        .expect_err("a raced conditional no-op must fail its caller precondition");
    match err {
        OmniError::PreconditionFailed {
            branch,
            expected: actual_expected,
            actual,
        } => {
            assert_eq!(branch, "main");
            assert_eq!(actual_expected, expected);
            assert_eq!(actual.as_deref(), Some(winner_head.as_str()));
        }
        other => panic!("conditional no-op race must be PreconditionFailed, got: {other}"),
    }
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        winner_head,
        "the losing no-op must not publish a second graph commit"
    );
}

/// A fresh named branch needs its inherited lineage fallback because it has no
/// exact branch-head row. If that second read fails after opening the
/// replacement manifest, the warm handle must keep its previous manifest and
/// lineage paired so the next request retries the coherent refresh instead of
/// serving replacement rows with the deleted branch's private head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn live_read_refresh_failure_keeps_manifest_and_lineage_coherent() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let writer = helpers::init_and_load(&dir).await;
    let reader = helpers::session(Omnigraph::open(uri).await.unwrap());

    writer.branch_create("feature").await.unwrap();
    helpers::mutate_branch(
        &writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "OldFeature")], &[("$age", 21)]),
    )
    .await
    .unwrap();
    reader.sync_branch("feature").await.unwrap();
    let (_, old_head) = reader
        .query_with_head(
            omnigraph::db::ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "OldFeature")]),
        )
        .await
        .unwrap();
    let old_head = old_head.expect("old feature owns a private head");

    writer.branch_delete("feature").await.unwrap();
    helpers::mutate_main(
        &writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "ReplacementMain")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    let replacement_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    writer.branch_create("feature").await.unwrap();

    {
        let _failpoint = catalog::READ_REFRESH_POST_STATE_PRE_LINEAGE.fire_always();
        reader
            .query_with_head(
                omnigraph::db::ReadTarget::branch("feature"),
                TEST_QUERIES,
                "get_person",
                &params(&[("$name", "ReplacementMain")]),
            )
            .await
            .expect_err("the injected lineage-refresh failure must surface");
    }

    let (rows, served_head) = reader
        .query_with_head(
            omnigraph::db::ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "ReplacementMain")]),
        )
        .await
        .expect("the next read must retry one coherent refresh");
    assert_eq!(rows.num_rows(), 1);
    assert_eq!(served_head.as_deref(), Some(replacement_head.as_str()));
    assert_ne!(served_head.as_deref(), Some(old_head.as_str()));
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
    let db = std::sync::Arc::new(helpers::session(
        Omnigraph::init(uri, OCC_UNIQUE_SCHEMA).await.unwrap(),
    ));

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
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
        .dataset("node:User")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:User")
        .unwrap()
        .published_dataset_version;
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
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
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

    let db_a = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let db_b = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));

    // B prepares first but pauses before effects. A then commits its Person
    // table effect and pauses before visibility. Releasing B makes it contend
    // for the shared branch gate, which A still holds through Phase D.
    let before_effect =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let after_effect =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_FINALIZE_PRE_PUBLISHER);

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
async fn schema_apply_pre_commit_crash_discards_staging_on_reopen() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = helpers::session(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE.fire_always();
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "got: {}",
            err
        );
        assert!(
            !matches!(err, OmniError::RecoveryRequired { .. }),
            "a failure before publication needs no recovery: {err}"
        );
    }
    assert!(
        dir.path().join("__schema_state.json.staging").exists(),
        "the staged contract outlives the writer"
    );
    assert_no_recovery_sidecars(dir.path());

    // RFC 0067: the staged contract names a graph commit that never landed,
    // so the next read-write open discards it; the created Company dataset
    // is unregistered garbage at the path the retry creates at.
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(db.schema_source().as_str(), SCHEMA_V1);
    assert_no_staging_files(dir.path());
    assert!(
        db.snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .dataset("node:Company")
            .is_none(),
        "nothing registers an unpublished apply's table"
    );
    let company_uri = unregistered_node_table_uri(&db).await;
    assert!(
        std::path::Path::new(&company_uri).exists(),
        "the abandoned create stays as unregistered garbage until the retry reclaims it"
    );

    db.apply_schema(SCHEMA_V2_ADDED_TYPE)
        .await
        .expect("the retry reclaims the leftover and publishes");
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 0);
    assert_eq!(node_table_uri(&db, "Company").await, company_uri);
    assert_no_staging_files(dir.path());
    assert_no_recovery_sidecars(dir.path());
}

#[tokio::test]
#[serial]
async fn schema_apply_recovers_partial_schema_promotion_after_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = helpers::session(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT.fire_always();
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            matches!(err, OmniError::RecoveryRequired { .. }),
            "a failure after publication reports the pending contract installation: {err}"
        );
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_manifest_commit"),
            "got: {}",
            err
        );
    }
    assert_no_recovery_sidecars(dir.path());

    // ReadOnly must remain non-mutating, but it also must not combine the
    // already-published manifest delta with the old live schema contract.
    // It fails closed until a read-write open performs the promotion.
    let read_only_error = match Omnigraph::open_read_only(&uri).await {
        Ok(_) => panic!("read-only open must refuse a committed-but-unpromoted SchemaApply"),
        Err(error) => error,
    };
    assert!(
        matches!(read_only_error, OmniError::RecoveryRequired { .. }),
        "{read_only_error}"
    );
    assert!(
        read_only_error
            .to_string()
            .contains("schema contract promotion is pending"),
        "{read_only_error}"
    );
    assert!(dir.path().join("_schema.pg.staging").exists());
    assert!(dir.path().join("_schema.ir.json.staging").exists());
    assert!(dir.path().join("__schema_state.json.staging").exists());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap(),
        SCHEMA_V1,
        "the read-only coherence guard must not promote schema files"
    );

    // Simulate a crash partway through promotion: source reached its final
    // name, while the IR/state contract remains staged. Recovery must
    // validate the mixed state as one target identity and finish it.
    std::fs::rename(
        dir.path().join("_schema.pg.staging"),
        dir.path().join("_schema.pg"),
    )
    .unwrap();
    assert!(!dir.path().join("_schema.pg.staging").exists());
    assert!(dir.path().join("_schema.ir.json.staging").exists());
    assert!(dir.path().join("__schema_state.json.staging").exists());

    // Reopen: the publishing commit is in lineage, so recovery completes the
    // remaining promotion and the live schema matches v2.
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 0);
    db.apply_schema(SCHEMA_V2_ADDED_TYPE)
        .await
        .expect("the reclaimed sentinel admits the next apply, a no-op here");
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
    let db = std::sync::Arc::new(helpers::session(
        Omnigraph::init(&uri, SCHEMA_V1).await.unwrap(),
    ));
    let stale_reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT);

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
            .dataset("node:Company")
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
            .export_jsonl("main", &["Company".to_string()])
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
        let db = helpers::session(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());
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
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(db.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

/// Azure implements schema-contract promotion as GET -> completed PUT ->
/// DELETE. Model a crash after the destination PUT by leaving the identical
/// live and staging objects together, then prove the ordinary open-time
/// recovery completes the remaining source deletions.
#[tokio::test]
#[serial]
async fn azure_schema_apply_recovers_source_and_destination_after_partial_rename() {
    let Ok(container) = std::env::var("OMNIGRAPH_AZURE_TEST_CONTAINER") else {
        eprintln!(
            "skipping Azure schema rename recovery: OMNIGRAPH_AZURE_TEST_CONTAINER is not set"
        );
        return;
    };
    let _scenario = FailScenario::setup();
    let uri = format!(
        "az://{container}/engine-failpoints/schema-rename-{}",
        ulid::Ulid::new()
    );
    let storage = omnigraph_storage::storage_for_uri(&uri).unwrap();

    {
        let db = helpers::session(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());
        db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap();
    }

    for name in ["_schema.ir.json", "__schema_state.json"] {
        let live = format!("{uri}/{name}");
        let staging = format!("{live}.staging");
        let body = storage.read_text(&live).await.unwrap();
        storage.write_text(&staging, &body).await.unwrap();
        assert!(storage.exists(&live).await.unwrap());
        assert!(storage.exists(&staging).await.unwrap());
    }

    let reopened = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("Azure open must complete the interrupted schema-contract rename"),
    );
    assert_eq!(reopened.schema_source().as_str(), SCHEMA_V2_ADDED_TYPE);
    for name in ["_schema.ir.json", "__schema_state.json"] {
        assert!(storage.exists(&format!("{uri}/{name}")).await.unwrap());
        assert!(
            !storage
                .exists(&format!("{uri}/{name}.staging"))
                .await
                .unwrap()
        );
    }
    drop(reopened);
    storage.delete_prefix(&uri).await.unwrap();
}

#[tokio::test]
#[serial]
async fn schema_apply_retries_over_its_own_unpublished_staging_without_reopen() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_rows = helpers::count_rows(&db, "node:Person").await;
    let desired = format!(
        "{}\nnode Tag {{ name: String @key }}\n",
        helpers::TEST_SCHEMA
    );
    {
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE.fire_always();
        let err = db.apply_schema(&desired).await.unwrap_err();
        assert!(
            !matches!(err, OmniError::RecoveryRequired { .. }),
            "a failure before publication is a plain error: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert!(dir.path().join("__schema_state.json.staging").exists());

    db.apply_schema(&desired)
        .await
        .expect("the retry restages over its own unpublished staging in-process");

    assert_eq!(helpers::count_rows(&db, "node:Person").await, person_rows);
    assert_eq!(helpers::count_rows(&db, "node:Tag").await, 0);
    assert_no_staging_files(dir.path());
    assert_no_recovery_sidecars(dir.path());
}

#[tokio::test]
#[serial]
async fn load_after_schema_apply_pre_publish_failure_keeps_the_accepted_catalog() {
    use omnigraph::loader::LoadMode;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
    db.load_jsonl(
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n",
        LoadMode::Append,
    )
    .await
    .unwrap();

    let v2_schema = format!(
        "{}\nnode Tag {{ label: String @key }}\n",
        schema_with_person_city()
    );
    {
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE.fire_always();
        let err = db.apply_schema(&v2_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "unexpected error: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());

    // Same handle: the entry heal leaves an unpublished staging alone and the
    // accepted catalog stays authoritative, so the new type is unknown.
    db.load_jsonl(
        "{\"type\":\"Tag\",\"data\":{\"label\":\"t1\"}}\n",
        LoadMode::Merge,
    )
    .await
    .expect_err("an unpublished apply's type must not be loadable");
    db.load_jsonl(
        "{\"type\":\"Person\",\"data\":{\"name\":\"bob\",\"age\":31}}\n",
        LoadMode::Merge,
    )
    .await
    .expect("the accepted schema keeps accepting rows");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
    drop(db);

    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_no_staging_files(dir.path());
    assert!(!reopened.schema_source().contains("node Tag"));
    assert_eq!(helpers::count_rows(&reopened, "node:Person").await, 2);
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

    let db = Arc::new(helpers::session(
        Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap(),
    ));

    // Park the apply right after its staging files land (its sidecar is
    // already on disk from Phase A; the manifest commit has not run).
    let rv = helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE);

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

/// Companion to the above — confirms that a finalize→publisher failure
/// on one table leaves OTHER tables untouched. Subsequent writes to
/// non-drifted tables proceed normally; the drift is contained.
#[tokio::test]
#[serial]
async fn finalize_publisher_residual_does_not_drift_untouched_tables() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = helpers::session(
        Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
            .await
            .unwrap(),
    );

    {
        let _failpoint = catalog::MUTATION_POST_FINALIZE_PRE_PUBLISHER.fire_always();
        let _ = mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .expect_err("synthetic failpoint must fire");
    }

    // node:Person drifted. node:Company didn't — try a Company write.
    use omnigraph::loader::LoadMode;
    db.load_jsonl(
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
    let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());

    // Seed a Person row. The enrolled mutation publishes only its logical data
    // effect; physical index construction remains reconciler-owned.
    mutate_main(
        &db,
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let person_head_before = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    let db = std::sync::Arc::new(db);

    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        &catalog::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
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
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let person_head_after = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;
    assert_eq!(person_pin_after, person_pin_before);
    assert_eq!(person_head_after, person_head_before);
}

fn assert_no_recovery_sidecars(root: &std::path::Path) {
    let recovery_dir = root.join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(&recovery_dir).unwrap().next().is_none(),
        "no recovery sidecar may exist"
    );
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

#[tokio::test]
#[serial]
async fn schema_apply_pre_staging_failure_leaves_no_residue() {
    use omnigraph::loader::LoadMode;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
        db.load_jsonl(
            r#"{"type":"Person","data":{"name":"alice","age":30}}
"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }

    let (pre_failure_version, person_before, person_head_before) = {
        let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
        let person_before = helpers::snapshot_main(&db)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let (head, _) = person_head_and_published(&db, "main").await;
        (version_main(&db).await.unwrap(), person_before, head)
    };
    let v2_schema = format!(
        "{}\nnode Tag {{ label: String @key }}\n",
        schema_with_person_city()
    );
    {
        let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
        let _failpoint = catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE.fire_always();
        let err = db.apply_schema(&v2_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.before_staging_write"),
            "unexpected error: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert_no_staging_files(dir.path());

    // The Person rewrite is a detached version and the Tag create is an
    // unregistered dataset: nothing moved the manifest or any linear HEAD,
    // so reopening has nothing to roll back.
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        version_main(&db).await.unwrap(),
        pre_failure_version,
        "an unpublished apply publishes nothing, not even a rollback"
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
    let snapshot = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let person_entry = snapshot.dataset("node:Person").unwrap();
    assert!(
        person_entry.same_registration(&person_before),
        "an unpublished apply leaves the Person registration as it was"
    );
    let person_uri = node_table_uri(&db, "Person").await;
    let person_head = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        person_head.version().version,
        person_head_before,
        "a detached rewrite never moves the linear HEAD"
    );
    assert!(snapshot.dataset("node:Tag").is_none());
    let live_schema = std::fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(!live_schema.contains("city: String?"), "{live_schema}");
    assert!(!live_schema.contains("node Tag"), "{live_schema}");

    db.apply_schema(&v2_schema)
        .await
        .expect("the retry rewrites from the pin and reclaims the Tag leftover");
    assert!(db.schema_source().contains("city: String?"));
    assert_eq!(helpers::count_rows(&db, "node:Tag").await, 0);
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
}

#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_before_staging_leaves_no_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    {
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
        let _failpoint = catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE.fire_always();
        let err = db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.before_staging_write"),
            "unexpected error: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert_no_staging_files(dir.path());

    let recovered = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("an index-only apply that failed before staging left nothing"),
    );
    assert!(!recovered.schema_source().contains("age: I32? @index"));
    recovered
        .apply_schema(&indexed_schema)
        .await
        .expect("the retry applies the index-only change");
    assert!(recovered.schema_source().contains("age: I32? @index"));
    assert_no_staging_files(dir.path());
}

#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_after_staging_discards_on_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    {
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE.fire_always();
        let err = db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "unexpected error: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert!(dir.path().join("__schema_state.json.staging").exists());

    // Metadata-only applies have no table effect: the staged contract is
    // their only durable state, and its recorded commit never landed.
    let recovered = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("an unpublished index-only staging is discarded"),
    );
    assert_no_staging_files(dir.path());
    assert!(!recovered.schema_source().contains("age: I32? @index"));
    recovered
        .apply_schema(&indexed_schema)
        .await
        .expect("the retry applies the index-only change");
    assert!(recovered.schema_source().contains("age: I32? @index"));
}

#[tokio::test]
#[serial]
async fn metadata_only_schema_apply_post_publish_failure_heals_on_next_write() {
    use omnigraph::loader::LoadMode;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let indexed_schema = helpers::TEST_SCHEMA.replace("age: I32?", "age: I32? @index");
    let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
    {
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT.fire_always();
        let err = db.apply_schema(&indexed_schema).await.unwrap_err();
        assert!(
            matches!(err, OmniError::RecoveryRequired { .. }),
            "a failure after publication reports the pending contract installation: {err}"
        );
    }
    assert!(dir.path().join("__schema_state.json.staging").exists());
    assert_no_recovery_sidecars(dir.path());

    // The next write's entry heal finds the recorded commit in lineage,
    // installs the contract and releases the dead apply's sentinel.
    db.load_jsonl(
        "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n",
        LoadMode::Append,
    )
    .await
    .expect("the next write heals an already-published apply in process");
    assert_no_staging_files(dir.path());
    assert!(
        db.schema_source().contains("age: I32? @index"),
        "the healed handle serves the published schema"
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
    db.apply_schema(&indexed_schema)
        .await
        .expect("the released sentinel admits the next apply, a no-op here");
}

#[tokio::test]
#[serial]
async fn schema_apply_retry_reclaims_an_abandoned_add_type_dataset() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(Omnigraph::init(&uri, SCHEMA_V1).await.unwrap());

    {
        let _failpoint = catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE.fire_always();
        db.apply_schema(SCHEMA_V2_ADDED_TYPE)
            .await
            .expect_err("the pre-staging failpoint must stop the apply after the create");
    }
    let company_uri = unregistered_node_table_uri(&db).await;
    assert!(
        std::path::Path::new(&company_uri).exists(),
        "the version-one create is durable before the failure"
    );
    drop(db);
    let recovered = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("an unregistered dataset is not recovery state"),
    );
    assert!(
        recovered
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap()
            .dataset("node:Company")
            .is_none(),
        "nothing registers the orphan target"
    );
    assert!(
        std::path::Path::new(&company_uri).exists(),
        "the open leaves the unregistered leftover for the retry"
    );
    assert_no_recovery_sidecars(dir.path());

    recovered
        .apply_schema(SCHEMA_V2_ADDED_TYPE)
        .await
        .expect("the retry reclaims the leftover under the sentinel and publishes");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Company").await,
        0,
        "the retried AddType must be registered and queryable"
    );
    assert_eq!(node_table_uri(&recovered, "Company").await, company_uri);
    let company = lance::Dataset::open(&company_uri).await.unwrap();
    assert_eq!(
        company.version().version,
        1,
        "the reclaimed path holds the retry's own version-one create"
    );
}

#[tokio::test]
#[serial]
async fn schema_apply_rename_rewrite_partial_effect_leaves_no_residue() {
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

    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_TABLE_COMMIT.fire_always();
        let error = db
            .apply_schema(desired)
            .await
            .expect_err("rename+rewrite must stop after its detached table effect");
        assert!(
            error.to_string().contains("schema_apply.post_table_commit"),
            "unexpected partial rename error: {error}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert_no_staging_files(dir.path());
    drop(db);

    // The rewrite is a detached version behind the source alias's pin; the
    // rename was never published. Reopening finds the graph untouched.
    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let snapshot = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(snapshot.dataset("node:Person").is_some());
    assert!(snapshot.dataset("node:Human").is_none());
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
    assert!(!recovered.schema_source().contains("node Human"));

    recovered
        .apply_schema(desired)
        .await
        .expect("the retry publishes the rename and rewrite");
    assert_eq!(
        helpers::count_rows(&recovered, "node:Human").await,
        people_before
    );
    assert!(recovered.schema_source().contains("node Human"));
}

#[tokio::test]
#[serial]
async fn schema_apply_partial_table_effect_leaves_no_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let people_before = helpers::count_rows(&db, "node:Person").await;
    let companies_before = helpers::count_rows(&db, "node:Company").await;
    let snapshot_before = helpers::snapshot_main(&db).await.unwrap();
    let mut heads_before = std::collections::BTreeMap::new();
    for type_name in ["Person", "Company"] {
        let head = lance::Dataset::open(&node_table_uri(&db, type_name).await)
            .await
            .unwrap()
            .version()
            .version;
        heads_before.insert(type_name, head);
    }
    let desired = schema_with_person_city().replace(
        "    name: String @key\n}\n\nedge Knows",
        "    name: String @key\n    domain: String?\n}\n\nedge Knows",
    );

    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_TABLE_COMMIT.fire_always();
        let error = db
            .apply_schema(&desired)
            .await
            .expect_err("the first detached table commit must be interrupted");
        assert!(
            error.to_string().contains("schema_apply.post_table_commit"),
            "unexpected partial-effect error: {error}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert_no_staging_files(dir.path());
    drop(db);

    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let snapshot = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    for type_name in ["Person", "Company"] {
        let table_key = format!("node:{type_name}");
        let entry = snapshot.dataset(&table_key).unwrap();
        assert!(
            entry.same_registration(snapshot_before.dataset(&table_key).unwrap()),
            "{type_name}: an unpublished apply leaves the registration as it was"
        );
        let head = lance::Dataset::open(&node_table_uri(&recovered, type_name).await)
            .await
            .unwrap();
        assert_eq!(
            head.version().version,
            heads_before[type_name],
            "{type_name}: a detached rewrite never moves the linear HEAD"
        );
    }
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

    recovered
        .apply_schema(&desired)
        .await
        .expect("the complete migration must succeed after an abandoned attempt");
    assert!(recovered.schema_source().contains("city: String?"));
    assert!(recovered.schema_source().contains("domain: String?"));
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
}

/// A concurrent publication on main between the staged effects and the
/// manifest commit: the apply loses its graph-head CAS, which is a plain
/// refusal before publication. Its detached rewrite and staged contract are
/// garbage, the winner is untouched, and the retry plans from the winner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn schema_apply_loses_the_manifest_cas_to_a_concurrent_publication_without_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let people_before = helpers::count_rows(&db, "node:Person").await;
    drop(db);

    let schema_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let winner_db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let desired = schema_with_person_city();
    let apply_handle = std::sync::Arc::clone(&schema_db);
    let apply_task = tokio::spawn(async move { apply_handle.apply_schema(&desired).await });
    rendezvous.wait_until_reached().await;

    let person_uri = node_table_uri(&winner_db, "Person").await;
    let published_before = helpers::snapshot_main(&winner_db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let winner_lance_head =
        forge_linear_head_from_pin(&winner_db, "main", "node:Person", published_before).await;
    winner_db
        .failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Person", None)
        .await
        .unwrap();
    let winner_pin = winner_db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let winner_head = branch_head_commit_id(dir.path(), "main").await.unwrap();
    rendezvous.release();

    let error = apply_task.await.unwrap().unwrap_err();
    assert!(
        !matches!(error, OmniError::RecoveryRequired { .. }),
        "losing the CAS before publication needs no recovery: {error}"
    );
    assert_no_recovery_sidecars(dir.path());
    drop(schema_db);
    drop(winner_db);

    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_no_staging_files(dir.path());
    assert!(
        !recovered.schema_source().contains("city: String?"),
        "the unpublished staging must not be promoted"
    );
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
    let main = recovered
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        main.dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        winner_pin,
        "the winner's pin is preserved"
    );
    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        winner_lance_head,
        "the abandoned detached rewrite never moved the linear HEAD"
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        winner_head,
        "nothing publishes on behalf of the lost apply"
    );

    recovered
        .apply_schema(&schema_with_person_city())
        .await
        .expect("the retry plans from the winner's authority");
    assert!(recovered.schema_source().contains("city: String?"));
    assert_eq!(
        helpers::count_rows(&recovered, "node:Person").await,
        people_before
    );
}

/// `optimize` Phase B → Phase C residual: `compact_files` advanced the Lance
/// HEAD but the manifest publish hasn't run. The `Optimize` recovery sidecar
/// (loose-match, like SchemaApply/EnsureIndices) must roll the compacted version
/// forward on next open so the manifest tracks the Lance HEAD — and the healed
/// table must then accept a schema apply (the original bug's victim).
async fn seed_two_productive_optimize_tables(uri: &str) {
    let db = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());
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

/// RFC 0067: a failure before Optimize publishes leaves no residue. The
/// compaction rewrites are detached versions behind the pins, the manifest
/// and every linear HEAD stay where they were, and the retry re-plans.
#[tokio::test]
#[serial(optimize)]
async fn optimize_pre_publish_failure_leaves_no_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_two_productive_optimize_tables(&uri).await;
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let manifest_before = version_main(&db).await.unwrap();
    let person_uri = node_table_uri(&db, "Person").await;
    let company_uri = node_table_uri(&db, "Company").await;
    let person_head = helpers::open_dataset_head_exact(&person_uri, None)
        .await
        .version()
        .version;
    let company_head = helpers::open_dataset_head_exact(&company_uri, None)
        .await
        .version()
        .version;
    {
        let _failpoint = catalog::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT.fire_always();
        let err = db.optimize().await.unwrap_err();
        assert!(
            err.to_string().contains(
                "injected failpoint triggered: optimize.post_phase_b_pre_manifest_commit"
            ),
            "unexpected error: {err}"
        );
        assert!(
            !matches!(err, OmniError::RecoveryRequired { .. }),
            "a failure before publication needs no recovery: {err}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    assert_eq!(version_main(&db).await.unwrap(), manifest_before);
    for (uri, head_before) in [(&person_uri, person_head), (&company_uri, company_head)] {
        let head = lance::Dataset::open(uri).await.unwrap().version().version;
        assert_eq!(
            head, head_before,
            "a detached rewrite never moves the linear HEAD"
        );
    }
    drop(db);
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(version_main(&db).await.unwrap(), manifest_before);
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 4);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 4);
    let stats = db
        .optimize()
        .await
        .expect("the retry re-plans from the pins");
    assert!(
        stats.iter().any(|stat| stat.committed),
        "the retry compacts what the failed attempt staged again"
    );
    assert_person_pin_detached(&db, "main", person_head).await;
    let desired = helpers::TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    db.apply_schema(&desired)
        .await
        .expect("schema apply after optimize must succeed");
}

/// RFC 0067: a lost acknowledgement of Optimize's manifest commit reports
/// an error, but the detached pins are published: reads serve the compacted
/// rows through them and nothing is left to compact.
#[tokio::test]
#[serial(optimize)]
async fn optimize_lost_publish_acknowledgement_leaves_published_detached_pins() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_two_productive_optimize_tables(&uri).await;
    let before = {
        let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
        let before = person_head_and_published(&db, "main").await;
        let _failpoint = catalog::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT.fire_once_at(1);
        db.optimize()
            .await
            .expect_err("a lost publish acknowledgement surfaces as an error");
        before
    };
    assert_no_recovery_sidecars(dir.path());
    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(helpers::count_rows(&recovered, "node:Person").await, 4);
    assert_eq!(helpers::count_rows(&recovered, "node:Company").await, 4);
    let (_, published) = person_head_and_published(&recovered, "main").await;
    assert!(
        published > before.1,
        "the unacknowledged publication is durable: before {}, published {published}",
        before.1
    );
    assert_person_pin_detached(&recovered, "main", before.0).await;
    let stats = recovered.optimize().await.unwrap();
    assert!(
        stats
            .iter()
            .all(|stat| stat.type_key == "__manifest" || !stat.committed),
        "nothing is left to compact after the published compaction: {stats:?}"
    );
}

/// Pending index work publishes no pin; productive siblings remain committed
/// through an interrupted optimize.
#[tokio::test]
#[serial(optimize)]
async fn optimize_publishes_no_pin_for_a_table_with_only_pending_index_work() {
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
    let db = helpers::session(Omnigraph::init(&uri, SCHEMA).await.unwrap());
    db.load_jsonl(
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
            .any(|index| { index.type_key == "node:Embedding" && index.property == "vector" }),
        "fixture must leave the null vector index pending"
    );
    db.load_jsonl(
        r#"{"type":"Embedding","data":{"name":"e1","vector":null}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.optimize().await.unwrap();
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let embedding = snapshot.open_dataset("node:Embedding").await.unwrap();
    assert!(embedding.has_unindexed_fragments().await.unwrap());
    assert_eq!(
        embedding.index_coverage("__id").await.unwrap(),
        omnigraph::IndexCoverage::Indexed,
        "only the excluded FTS tail and untrainable vector may remain"
    );
    // Fixed productive tail on Work only; Embedding's buildable indexes are
    // current, while its vector and FTS tail both remain deferred-only.
    for name in ["w1", "w2", "w3", "w4"] {
        db.load_jsonl(
            &format!(r#"{{"type":"Work","data":{{"name":"{name}"}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }

    // RFC 0067: a table with only pending index work publishes no pin; a
    // pre-publish failure leaves nothing, and the retry reports the pending
    // work again.
    let embedding_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Embedding")
        .unwrap()
        .clone();
    let embedding_uri = node_table_uri(&db, "Embedding").await;
    let embedding_head_before = helpers::open_dataset_head_exact(&embedding_uri, None)
        .await
        .version()
        .version;
    {
        let _failpoint = catalog::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT.fire_once_at(1);
        let error = db.optimize().await.unwrap_err();
        assert!(
            !matches!(error, OmniError::RecoveryRequired { .. }),
            "{error}"
        );
    }
    assert_no_recovery_sidecars(dir.path());
    let embedding_entry = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Embedding")
        .unwrap()
        .clone();
    assert!(
        embedding_entry.same_registration(&embedding_before),
        "a pending-only table never publishes a pin"
    );
    assert_eq!(
        helpers::open_dataset_head_exact(&embedding_uri, None)
            .await
            .version()
            .version,
        embedding_head_before,
        "nothing moves the pending-only table's linear HEAD"
    );
    drop(db);
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let stats = db.optimize().await.unwrap();
    let work = stats
        .iter()
        .find(|stat| stat.type_key == "node:Work")
        .expect("Work stat present");
    assert!(work.committed, "the retry compacts Work");
    let embedding = stats
        .iter()
        .find(|stat| stat.type_key == "node:Embedding")
        .expect("Embedding stat present");
    assert!(!embedding.committed, "pending-only table must stay a no-op");
    assert!(
        embedding
            .pending_indexes
            .iter()
            .any(|index| index.property == "vector"),
        "pending-only vector status must remain visible"
    );
    assert!(
        embedding.pending_indexes.iter().any(|index| {
            index.property == "name" && index.reason.contains("rebuild-full-text-indexes")
        }),
        "the deferred FTS tail must name its explicit rebuild remedy"
    );
}

/// RFC 0067: a failure after one table's detached rewrite and before the
/// batch publishes leaves no residue: no pin moves, no lineage is added,
/// no linear HEAD moves, and the retry compacts every table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_partial_table_effect_leaves_no_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    seed_two_productive_optimize_tables(&uri).await;
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let snapshot_before = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let person_pin = snapshot_before
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let company_pin = snapshot_before
        .dataset("node:Company")
        .unwrap()
        .published_dataset_version;
    let commits_before = db.list_commits(Some("main")).await.unwrap().len();
    let person_uri = node_table_uri(&db, "Person").await;
    let company_uri = node_table_uri(&db, "Company").await;
    let person_head = helpers::open_dataset_head_exact(&person_uri, None)
        .await
        .version()
        .version;
    let company_head = helpers::open_dataset_head_exact(&company_uri, None)
        .await
        .version()
        .version;
    let _failpoint = catalog::OPTIMIZE_POST_TABLE_EFFECT.fire_once_at(1);
    let error = db.optimize().await.unwrap_err();
    assert!(
        !matches!(error, OmniError::RecoveryRequired { .. }),
        "a partial batch before publication needs no recovery: {error}"
    );
    assert_no_recovery_sidecars(dir.path());
    let snapshot_after = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        snapshot_after
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_pin
    );
    assert_eq!(
        snapshot_after
            .dataset("node:Company")
            .unwrap()
            .published_dataset_version,
        company_pin
    );
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        commits_before,
        "a partial Optimize publishes no lineage"
    );
    for (uri, head_before) in [(&person_uri, person_head), (&company_uri, company_head)] {
        let head = lance::Dataset::open(uri).await.unwrap().version().version;
        assert_eq!(
            head, head_before,
            "a detached rewrite never moves the linear HEAD"
        );
    }
    drop(db);
    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(helpers::count_rows(&recovered, "node:Person").await, 4);
    assert_eq!(helpers::count_rows(&recovered, "node:Company").await, 4);
    assert_eq!(
        recovered.list_commits(Some("main")).await.unwrap().len(),
        commits_before
    );
    let stats = recovered.optimize().await.unwrap();
    assert_eq!(
        stats
            .iter()
            .filter(|stat| stat.type_key != "__manifest" && stat.committed)
            .count(),
        2,
        "the retry compacts both tables: {stats:?}"
    );
}

/// Optimize captures a complete authority token before entering any writer
/// gate, then revalidates it after schema -> main -> table (`optimize.rs`, the
/// comment above `open_write_txn`). That revalidation is the only thing that
/// stops a maintenance run from planning against a graph that moved while it
/// waited — v6 had no such check, it used a bare fresh snapshot.
///
/// The token carried in `admission_txn` is the authority proof and the source
/// of the recovery sidecar's `RecoveryAuthorityToken`. Removing it would
/// silently downgrade optimize/cleanup/repair from
/// reject-on-authority-drift to read-whatever-is-fresh.
async fn seed_optimize_race_graph(dir: &tempfile::TempDir) {
    let seed = helpers::init_and_load(dir).await;
    // Leave real compaction work behind so a missing barrier advances Person
    // instead of accidentally passing because Optimize was a no-op.
    helpers::commit_many(&seed, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[serial(optimize)]
async fn optimize_refuses_when_graph_authority_moves_before_its_gates() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    seed_optimize_race_graph(&dir).await;

    let optimize_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let writer_db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let person_uri = node_table_uri(optimize_db.as_ref(), "Person").await;
    let graph_head_before = branch_head_commit_id(dir.path(), "main").await.unwrap();

    // Park Optimize with its authority token captured but no gate held.
    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        &catalog::OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES,
    );
    let optimize_task_db = std::sync::Arc::clone(&optimize_db);
    let optimize = tokio::spawn(async move { optimize_task_db.optimize().await });
    rendezvous.wait_until_reached().await;

    // Advance the graph head underneath it with an ordinary committed write, so
    // Optimize's captured token is now stale in `graph_head`.
    mutate_main(
        &writer_db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "authority-mover")], &[("$age", 41)]),
    )
    .await
    .expect("the concurrent write must commit while Optimize waits");
    let graph_head_after = branch_head_commit_id(dir.path(), "main").await.unwrap();
    assert_ne!(
        graph_head_before, graph_head_after,
        "the fixture must actually move the graph head, or this test is vacuous",
    );
    // Baseline taken AFTER the concurrent write: that write legitimately moves
    // Person. What must not move it again is Optimize.
    let person_head_after_write = lance::Dataset::open(&person_uri)
        .await
        .unwrap()
        .version()
        .version;

    rendezvous.release();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("Optimize task hung after releasing the authority-capture rendezvous")
        .unwrap();
    drop(rendezvous);

    let error = outcome.expect_err(
        "Optimize must refuse a token whose graph head moved before it acquired its gates",
    );
    let rendered = error.to_string();
    assert!(
        rendered.contains("read set")
            || rendered.contains("graph_head")
            || rendered.contains("changed"),
        "expected an authority-drift refusal, got: {rendered}",
    );

    assert_eq!(
        lance::Dataset::open(&person_uri)
            .await
            .unwrap()
            .version()
            .version,
        person_head_after_write,
        "the refusal must land before any physical maintenance effect",
    );
    let recovery_dir = dir.path().join("__recovery");
    let sidecars: Vec<_> = std::fs::read_dir(&recovery_dir)
        .map(|entries| entries.filter_map(|entry| entry.ok()).collect())
        .unwrap_or_default();
    assert!(
        sidecars.is_empty(),
        "a pre-effect authority refusal must leave no Optimize sidecar: {sidecars:?}",
    );
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
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
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

    let db_b = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));

    // Park optimize before its first detached rewrite.
    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::OPTIMIZE_BEFORE_COMPACT);

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = helpers::session(Omnigraph::open(&uri_opt).await.unwrap());
        db.optimize().await
    });
    rendezvous.wait_until_reached().await;

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

    rendezvous.release();
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
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
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

/// Same as the insert cell, for a strict delete. The second handle waits for
/// optimize; Optimize's publication then moved the graph head the delete was
/// prepared against, so the strict read-modify-write reports the typed
/// read-set conflict instead of a silent rebase (RFC 0067: Optimize is an
/// ordinary publication, not a recovery barrier), and the caller's retry
/// lands on the compacted pins and preserves the deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(optimize)]
async fn optimize_serializes_concurrent_delete_across_handles() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
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

    let db_b = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));

    // Park optimize before its first detached rewrite.
    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::OPTIMIZE_BEFORE_COMPACT);

    let uri_opt = uri.clone();
    let optimize = tokio::spawn(async move {
        let db = helpers::session(Omnigraph::open(&uri_opt).await.unwrap());
        db.optimize().await
    });
    rendezvous.wait_until_reached().await;

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
        "same-root delete must wait for optimize's branch-gate lifetime"
    );

    rendezvous.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), optimize)
        .await
        .expect("optimize task hung")
        .unwrap();
    result.expect("optimize must finish before the queued delete");
    let conflict = writer
        .await
        .expect("writer task panicked")
        .expect_err("a strict delete prepared before Optimize published reports the conflict");
    assert!(
        matches!(
            &conflict,
            OmniError::Manifest(omnigraph::error::ManifestError {
                details: Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged {
                    member,
                    ..
                }),
                ..
            }) if member == "graph_head:main"
        ),
        "the queued delete must see Optimize's publication as a read-set change: {conflict}"
    );
    db_b.mutate(
        "main",
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "alice")], &[]),
    )
    .await
    .expect("the retried delete lands on the compacted pins");

    // No lost write: alice's delete persisted (3 rows); graph remains re-optimizable.
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows(&db, "node:Person").await,
        3,
        "the concurrent delete must persist (alice removed)",
    );
    db.optimize()
        .await
        .expect("graph must remain healthy / re-optimizable");
}

#[tokio::test]
#[serial(branch_merge_first_touch)]
async fn branch_merge_pointer_keeps_an_unregistered_target_ref() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "orphan-window-row")], &[("$age", 38)]),
    )
    .await
    .unwrap();
    let person_uri = node_table_uri(&db, "Person").await;
    let target_native = helpers::graph_native_ref(&uri, "target").await;
    let mut person = lance::Dataset::open(&person_uri).await.unwrap();
    let orphan_version = person.version().version;
    // forbidden-api-allow: test synthesizes an unregistered target ref from an older main version.
    person
        .create_branch(&target_native, orphan_version, None)
        .await
        .unwrap();
    let orphan_identifier = person
        .checkout_branch(&target_native)
        .await
        .unwrap()
        .branch_identifier()
        .await
        .unwrap();
    drop(person);

    let source_before = helpers::snapshot_branch(&db, "source").await.unwrap();
    let source_entry = source_before.dataset("node:Person").unwrap();
    let target_before = helpers::snapshot_branch(&db, "target").await.unwrap();
    assert!(
        source_entry.native_dataset_branch.is_none()
            && target_before
                .dataset("node:Person")
                .unwrap()
                .native_dataset_branch
                .is_none(),
        "branch writes never fork: both keep main's registration"
    );
    let source_pin = pinned_version(&db, "source", "node:Person").await;
    assert_ne!(
        source_pin,
        pinned_version(&db, "target", "node:Person").await,
        "the source's write is its own detached pin"
    );
    assert_eq!(
        db.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::FastForward
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    drop(db);
    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let target_after = helpers::snapshot_branch(&recovered, "target")
        .await
        .unwrap();
    let target_entry = target_after.dataset("node:Person").unwrap();
    assert_eq!(
        target_entry.native_dataset_branch,
        source_entry.native_dataset_branch
    );
    assert_eq!(
        target_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    assert_eq!(
        pinned_version(&recovered, "target", "node:Person").await,
        source_pin,
        "the target takes the source's detached pin"
    );
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        person
            .checkout_branch(&target_native)
            .await
            .unwrap()
            .branch_identifier()
            .await
            .unwrap(),
        orphan_identifier
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        helpers::count_rows_branch(&recovered, "source", "node:Person").await
    );
}

#[tokio::test]
#[serial(branch_merge_first_touch)]
async fn branch_merge_pointer_failure_retries_without_sidecar() {
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

    let person_uri = node_table_uri(&db, "Person").await;
    let target_native = helpers::graph_native_ref(&uri, "target").await;
    let mut person = lance::Dataset::open(&person_uri).await.unwrap();
    let orphan_version = person.version().version;
    // forbidden-api-allow: test synthesizes an unregistered target ref from an older main version.
    person
        .create_branch(&target_native, orphan_version, None)
        .await
        .unwrap();
    let orphan = person.checkout_branch(&target_native).await.unwrap();
    let orphan_identifier = orphan.branch_identifier().await.unwrap();
    drop(orphan);
    drop(person);
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());

    let source_before = helpers::snapshot_branch(&db, "source").await.unwrap();
    let source_entry = source_before.dataset("node:Person").unwrap();
    let target_before = helpers::snapshot_branch(&db, "target").await.unwrap();
    {
        let _failpoint = catalog::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT.fire_always();
        let error = db.branch_merge("source", "target").await.unwrap_err();
        assert!(
            !matches!(error, OmniError::RecoveryRequired { .. }),
            "a pointer-only failure has no table effect to recover"
        );
    }
    let still_target = helpers::snapshot_branch(&db, "target").await.unwrap();
    assert_eq!(
        still_target
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch,
        target_before
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
    );
    assert_eq!(
        still_target
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        target_before
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        db.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::FastForward
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    drop(db);
    let recovered = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let target_after = helpers::snapshot_branch(&recovered, "target")
        .await
        .unwrap();
    let target_entry = target_after.dataset("node:Person").unwrap();
    assert_eq!(
        target_entry.native_dataset_branch,
        source_entry.native_dataset_branch
    );
    assert_eq!(
        target_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    assert_eq!(
        person
            .checkout_branch(&target_native)
            .await
            .unwrap()
            .branch_identifier()
            .await
            .unwrap(),
        orphan_identifier
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        helpers::count_rows_branch(&recovered, "source", "node:Person").await
    );
    assert_eq!(
        helpers::count_rows_branch(&recovered, "target", "node:Person").await,
        main_rows + 1
    );
}

/// Build an `AdoptWithDelta` merge whose insert delta is one row larger than
/// the keyed adapter's 8,192-row chunk. The source uses two ordinary capped
/// loads so this fixture does not bypass the public write limit it is testing.
async fn setup_branch_merge_multichunk_adopt(dir: &tempfile::TempDir) -> (String, String, u64) {
    const CHUNK_ROWS: usize = 8192;

    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap());
    db.load_jsonl(
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
    let expected_version = snapshot
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let person_uri = node_table_uri(&db, "Person").await;
    drop(db);
    (uri, person_uri, expected_version)
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
    let db = helpers::session(Omnigraph::init(&uri, RFC023_KEY_SCHEMA).await.unwrap());
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
    let expected_version = snapshot
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let person_uri = node_table_uri(&db, "Person").await;
    drop(db);
    (uri, person_uri, expected_version)
}

/// Which branch-merge publish path a partial-Phase-B test exercises.
enum MergeScenario {
    /// main stays at base → the touched table is `AdoptWithDelta`
    /// (`publish_adopted_delta`: append → upsert → delete).
    Adopt,
    /// main advances past base → the touched table is `RewriteMerged`
    /// (`publish_rewritten_merge_table`: merge_insert → delete).
    Rewrite,
}

async fn sorted_person_names(db: &Omnigraph) -> Vec<String> {
    let mut names = collect_column_strings(&read_table(db, "node:Person").await, "name");
    names.sort();
    names
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
/// Staged-index failure before the gates is covered by
/// `ensure_indices_stage_btree_failure_leaves_existing_tables_writable`; this
/// test deliberately reaches the second, no-work reconciliation pass.
#[tokio::test]
#[serial]
async fn ensure_indices_phase_b_failure_does_not_leak_sidecar_when_no_work_needed() {
    use omnigraph::loader::LoadMode;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed, then reconcile the index declaration once. RFC-022 writes publish
    // only their exact data effect; index construction is derived work.
    {
        let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
        db.load_jsonl(
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
        let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
        let _failpoint = catalog::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT.fire_always();
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
    let _db = helpers::session(Omnigraph::open(&uri).await.unwrap());

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
// below pin: on failpoint trigger at the two pre-commit phase boundaries,
// the three schema files are removed before the error is returned.
//
// The third boundary (`init.after_coordinator_init`) sits past the graph's
// commit point, where the cleanup must not run (issue #495 — deleting the
// schema files there left a graph that could neither open nor re-init).
// Its test asserts the graph survives an error at that window.
//
// Coverage note: orphan Lance directories after a failure DURING
// `GraphCoordinator::init` are a known limitation — see the coverage-gap
// comment in `init_with_storage`.

#[tokio::test]
#[serial]
async fn init_failpoint_after_schema_pg_written_cleans_up_schema_file() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = catalog::INIT_AFTER_SCHEMA_PG_WRITTEN.fire_always();

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
    assert!(
        !dir.path().join("__init_claim.json").exists(),
        "pre-create failure must release the init claim after cleanup"
    );
}

#[tokio::test]
#[serial]
async fn init_failpoint_after_schema_contract_written_cleans_up_all_schema_files() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = catalog::INIT_AFTER_SCHEMA_CONTRACT_WRITTEN.fire_always();

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
    assert!(
        !dir.path().join("__init_claim.json").exists(),
        "pre-create failure must release the init claim after cleanup"
    );
}

#[tokio::test]
#[serial]
async fn init_failpoint_after_coordinator_init_leaves_completed_store_intact() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let _failpoint = catalog::INIT_AFTER_COORDINATOR_INIT.fire_always();

    let err = match Omnigraph::init(&uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    let OmniError::InitializationCommitted {
        uri: error_uri,
        source,
    } = err
    else {
        panic!("post-commit validation failure must remain typed");
    };
    assert_eq!(error_uri, uri);
    let msg = source.to_string();
    assert!(
        msg.contains("injected failpoint triggered: init.after_coordinator_init"),
        "init error must surface the original cause, got: {msg}"
    );

    // The graph was durably complete before the failpoint fired; the schema
    // files must survive the failed init.
    for schema_file in ["_schema.pg", "_schema.ir.json", "__schema_state.json"] {
        assert!(
            dir.path().join(schema_file).exists(),
            "{schema_file} must survive a post-commit-point init failure"
        );
    }

    // And the graph is not merely present but fully usable: it opens,
    // accepts a write, and serves a read.
    let db = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("graph must open cleanly after a post-commit-point init failure"),
    );
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "post-commit-survivor")], &[("$age", 1)]),
    )
    .await
    .expect("graph must accept writes after a post-commit-point init failure");
    assert_eq!(count_rows(&db, "node:Person").await, 1);
}

// Error-return twin of `init_crash_after_manifest_create_leaves_openable_store`:
// an error injected just past the commit point must not trigger cleanup.
#[tokio::test]
#[serial]
async fn init_failpoint_post_manifest_create_leaves_completed_graph_intact() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let _failpoint = catalog::INIT_POST_MANIFEST_CREATE.fire_always();

    let err = match Omnigraph::init(&uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    let OmniError::InitializationCommitted {
        uri: error_uri,
        source,
    } = err
    else {
        panic!("post-commit read-back failure must remain typed");
    };
    assert_eq!(error_uri, uri);
    let msg = source.to_string();
    assert!(
        msg.contains("injected failpoint triggered: init.post_manifest_create"),
        "init error must surface the original cause, got: {msg}"
    );

    for schema_file in ["_schema.pg", "_schema.ir.json", "__schema_state.json"] {
        assert!(
            dir.path().join(schema_file).exists(),
            "{schema_file} must survive a post-commit-point init failure"
        );
    }

    let db = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("graph must open cleanly after a post-commit-point init failure"),
    );
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "post-commit-survivor")], &[("$age", 1)]),
    )
    .await
    .expect("graph must accept writes after a post-commit-point init failure");
    assert_eq!(count_rows(&db, "node:Person").await, 1);
}

#[tokio::test]
#[serial]
async fn init_manifest_create_lost_ack_recovers_exact_genesis() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let _lost_ack = catalog::INIT_MANIFEST_CREATE_POST_NATIVE.fire_always();

    let db = helpers::session(
        Omnigraph::init(&uri, helpers::TEST_SCHEMA)
            .await
            .expect("exact genesis probe must recover a lost Create acknowledgement"),
    );
    drop(db);
    for artifact in ["_schema.pg", "_schema.ir.json", "__schema_state.json"] {
        assert!(
            dir.path().join(artifact).exists(),
            "lost-ack recovery must preserve {artifact}"
        );
    }
    assert!(
        !dir.path().join("__init_claim.json").exists(),
        "exactly recovered initialization must release its transient claim"
    );
    let db = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("an exactly recovered genesis must reopen through the ordinary path"),
    );
    let commits = db.list_commits(None).await.expect("list genesis commit");
    assert_eq!(commits.len(), 1);
    assert!(commits[0].parent_commit_id.is_none());
    assert!(commits[0].actor_id.is_none());

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "lost-ack-survivor")], &[("$age", 1)]),
    )
    .await
    .expect("recovered graph must accept writes");
    assert_eq!(count_rows(&db, "node:Person").await, 1);
}

#[tokio::test]
#[serial]
async fn init_table_create_lost_ack_preserves_claim_and_schema() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let _lost_ack = catalog::INIT_TABLE_CREATE_POST_NATIVE.fire_always();

    let err = match Omnigraph::init(&uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("a table Create acknowledgement failure must not return success"),
        Err(err) => err,
    };
    let OmniError::InitializationIndeterminate {
        uri: error_uri,
        source,
        ..
    } = err
    else {
        panic!("a physical table Create outcome must remain indeterminate");
    };
    assert_eq!(error_uri, uri);
    assert!(source.to_string().contains("init.table_create_post_native"));
    for artifact in [
        "_schema.pg",
        "_schema.ir.json",
        "__schema_state.json",
        "__init_claim.json",
    ] {
        assert!(
            dir.path().join(artifact).exists(),
            "physical-init ambiguity must preserve {artifact}"
        );
    }
    assert!(
        dir.path().join("nodes").exists() || dir.path().join("edges").exists(),
        "the injected error must follow a real durable table Create"
    );
    assert!(
        !dir.path().join("__manifest").exists(),
        "the table-Create test must fail before graph manifest creation"
    );
}

#[tokio::test]
#[serial]
async fn init_manifest_create_unknown_and_probe_failure_preserves_claim_and_schema() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let lost_ack = catalog::INIT_MANIFEST_CREATE_POST_NATIVE.fire_always();
    let probe_failure = catalog::INIT_MANIFEST_CREATE_PROBE.fire_always();

    let err = match Omnigraph::init(&uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("an unavailable exact probe must leave the outcome indeterminate"),
        Err(err) => err,
    };
    let OmniError::InitializationIndeterminate {
        uri: error_uri,
        source,
        probe,
    } = err
    else {
        panic!("unknown Create plus failed probe must remain typed");
    };
    assert_eq!(error_uri, uri);
    assert!(
        source
            .to_string()
            .contains("init.manifest_create_post_native")
    );
    assert!(probe.to_string().contains("init.manifest_create_probe"));
    for artifact in [
        "_schema.pg",
        "_schema.ir.json",
        "__schema_state.json",
        "__init_claim.json",
    ] {
        assert!(
            dir.path().join(artifact).exists(),
            "indeterminate initialization must preserve {artifact}"
        );
    }

    drop(probe_failure);
    drop(lost_ack);
    let _db = Omnigraph::open(&uri)
        .await
        .expect("clearing the observation fault must reveal the committed graph");
    assert!(
        dir.path().join("__init_claim.json").exists(),
        "ordinary open must not speculate that an indeterminate init claim is stale"
    );
}

// The floor under the schema-files-gone damage state: however a graph loses
// its schema files while keeping its data and `__manifest`, `open` must
// diagnose the state by name.
#[tokio::test]
#[serial]
async fn open_missing_schema_pg_reports_schema_files_missing() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    Omnigraph::init(&uri, helpers::TEST_SCHEMA)
        .await
        .expect("init must succeed");
    std::fs::remove_file(dir.path().join("_schema.pg")).unwrap();

    let err = match Omnigraph::open(&uri).await {
        Ok(_) => panic!("open must fail without _schema.pg"),
        Err(e) => e,
    };
    let OmniError::Manifest(manifest) = &err else {
        panic!("missing schema source must remain a typed manifest error");
    };
    assert_eq!(manifest.kind, ManifestErrorKind::NotFound);
    let msg = err.to_string();
    assert!(
        msg.contains("missing its schema files") && msg.contains("_schema.pg"),
        "open must name the missing schema files, got: {msg}"
    );

    let read_only_err = match Omnigraph::open_read_only(&uri).await {
        Ok(_) => panic!("read-only open must also fail without _schema.pg"),
        Err(err) => err,
    };
    let OmniError::Manifest(manifest) = read_only_err else {
        panic!("read-only missing schema source must remain a typed manifest error");
    };
    assert_eq!(manifest.kind, ManifestErrorKind::NotFound);

    std::fs::remove_file(dir.path().join("_schema.ir.json")).unwrap();
    std::fs::remove_file(dir.path().join("__schema_state.json")).unwrap();
    let reinit = match Omnigraph::init(&uri, "node Replacement { key: String @key }\n").await {
        Ok(_) => panic!("strict init must not rebind a readable manifest with missing schema"),
        Err(err) => err,
    };
    assert!(matches!(reinit, OmniError::AlreadyInitialized { .. }));
    for artifact in ["_schema.pg", "_schema.ir.json", "__schema_state.json"] {
        assert!(
            !dir.path().join(artifact).exists(),
            "manifest preflight must not write replacement {artifact}"
        );
    }
    assert!(
        !dir.path().join("__init_claim.json").exists(),
        "manifest preflight must refuse before acquiring an init claim"
    );
}

// The torn-init regression: the `__manifest` Create commit is the manifest's
// entire birth (entries, genesis lineage, and the internal-schema stamp ride
// the one commit), so a crash immediately after it — previously the
// create-to-stamp gap, which left a durable-but-unstamped manifest that every
// later open misdiagnosed as "created by omnigraph 0.3.1 or earlier" and no
// re-init could recover — must now leave a store that opens cleanly and
// serves reads and writes. The crash is a panic (not an error return) so
// init's best-effort cleanup does not run, modeling a process death.
#[tokio::test]
#[serial]
async fn init_crash_after_manifest_create_leaves_openable_store() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let crash = catalog::INIT_POST_MANIFEST_CREATE.panic_at();
    let cloned = uri.clone();
    let died = tokio::spawn(async move {
        Omnigraph::init(&cloned, helpers::TEST_SCHEMA)
            .await
            .map(|_| ())
    })
    .await;
    drop(crash);
    assert!(
        died.expect_err("init must die at the injected crash point")
            .is_panic(),
        "the injected failpoint action is a panic"
    );

    // The Create commit carried the stamp, so the store is fully born: it
    // opens without the ancient-version misdiagnosis and serves a write and
    // a read.
    let db = helpers::session(
        Omnigraph::open(&uri)
            .await
            .expect("store must open cleanly after a crash in the post-create window"),
    );
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "torn-init-survivor")], &[("$age", 1)]),
    )
    .await
    .expect("store must accept writes after the crash");
    assert_eq!(count_rows(&db, "node:Person").await, 1);
}

#[tokio::test]
#[serial]
async fn init_failpoint_returns_original_error_not_cleanup_error() {
    // A delete failure is outcome-unknown: retain the claim so a delayed
    // delete cannot race a later initializer, but still return the original
    // pre-physical init error rather than masking it with cleanup failure.
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = catalog::INIT_AFTER_SCHEMA_PG_WRITTEN.fire_always();
    let _delete_failure = catalog::INIT_SCHEMA_CLEANUP_DELETE.fire_always();

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the configured failpoint"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("init.after_schema_pg_written"),
        "init error must surface the failpoint cause, got: {msg}"
    );
    assert!(
        dir.path().join("_schema.pg").exists(),
        "the injected delete failure must leave the owned schema artifact"
    );
    assert!(
        dir.path().join("__init_claim.json").exists(),
        "an indeterminate schema delete must retain the init claim"
    );
}

// Local roots probe create-if-absent on init and on read-write open (issue
// #453: no hard_link(2) breaks every write). The failpoint stands in for a
// refusing filesystem; real refusals are classified by storage-crate tests.

#[tokio::test]
#[serial]
async fn init_create_if_absent_probe_failure_leaves_empty_root() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _failpoint = catalog::LOCAL_CREATE_IF_ABSENT_PROBE.fire_always();

    let err = match Omnigraph::init(uri, helpers::TEST_SCHEMA).await {
        Ok(_) => panic!("expected Omnigraph::init to fail at the create-if-absent probe"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: storage.local_create_if_absent_probe"),
        "got: {err}"
    );
    // The probe precedes the `_schema.pg` claim and every Lance commit, so a
    // capability failure leaves the root with no artifacts at all.
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "capability-probe failure must leave the graph root empty"
    );
}

#[tokio::test]
#[serial]
async fn read_write_open_create_if_absent_probe_failure_aborts_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _ = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());

    let _failpoint = catalog::LOCAL_CREATE_IF_ABSENT_PROBE.fire_always();
    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("expected read-write open to fail at the create-if-absent probe"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: storage.local_create_if_absent_probe"),
        "got: {err}"
    );
}

/// Reads never need hard links, so a read-only open must stay usable on a
/// filesystem that refuses them (export from a store copied onto FAT/exFAT).
#[tokio::test]
#[serial]
async fn read_only_open_skips_create_if_absent_probe() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _ = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());

    let _failpoint = catalog::LOCAL_CREATE_IF_ABSENT_PROBE.fire_always();
    let _db = Omnigraph::open_read_only(uri)
        .await
        .expect("read-only open must not run the create-if-absent probe");
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
    let _fp = catalog::PUBLISH_LOAD_STATE.fire_once_at(1);
    let row = r#"{"type":"Person","data":{"name":"Grace","age":37}}"#;
    db.load_as("main", None, row, LoadMode::Merge, None)
        .await
        .expect("publisher must retry the one-shot retryable load_publish_state error and commit");
}

/// A live named-branch Blob read captured before a same-path/same-version
/// delete-recreate reads its own detached pin: it returns the captured bytes,
/// never the replacement's (each incarnation pins a distinct detached version).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn blob_live_branch_read_keeps_the_captured_pin_across_delete_recreate_aba() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let schema = r#"
node Document {
    title: String @key
    content: Blob
}
"#;
    let setup = helpers::session(Omnigraph::init(&uri, schema).await.unwrap());
    setup
        .load_jsonl(
            r#"{"type":"Document","data":{"title":"aba","content":"base64:QmFzZQ=="}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    setup.branch_create("feature").await.unwrap();
    setup
        .load(
            "feature",
            r#"{"type":"Document","data":{"title":"aba","content":"base64:T2xk"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(setup);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let control = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let old_entry = control
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    let old_pin = pinned_version(&control, "feature", "node:Document").await;
    let old_head = branch_head_commit_id(dir.path(), "feature").await.unwrap();
    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::BLOB_READ_POST_CAPTURE);
    let cell = node_blob_cell("Document", "aba", "content");
    let read_cell = cell.clone();
    let read_task = tokio::spawn(async move {
        reader
            .read_blob_at(ReadTarget::branch("feature"), read_cell)
            .await
    });
    rendezvous.wait_until_reached().await;

    // Keep the parked reader releasable even if one control-plane operation
    // fails: collect the replacement result first, then release before any
    // assertion or unwrap.
    let replacement = async {
        control.branch_delete("feature").await?;
        control.branch_create("feature").await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"aba","content":"base64:TmV3"}}"#,
                LoadMode::Merge,
            )
            .await?;
        control.snapshot_of(ReadTarget::branch("feature")).await
    }
    .await;
    rendezvous.release();

    let new_snapshot = replacement.expect("delete/recreate replacement must complete");
    let new_entry = new_snapshot.dataset("node:Document").unwrap();
    assert_eq!(new_entry.dataset_path, old_entry.dataset_path);
    assert!(
        new_entry.native_dataset_branch.is_none() && old_entry.native_dataset_branch.is_none(),
        "both incarnations keep main's registration: branch writes never fork"
    );
    assert_ne!(
        pinned_version(&control, "feature", "node:Document").await,
        old_pin,
        "the replacement pins its own detached version"
    );
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "the regression must exercise same-path/same-version branch ABA"
    );

    let captured = read_task
        .await
        .unwrap()
        .expect("the captured detached pin stays readable across the branch ABA");
    assert_eq!(
        captured.resolved_target.graph_commit_id.as_deref(),
        Some(old_head.as_str()),
        "the read resolves the captured incarnation's commit"
    );
    let omnigraph::BlobContent::Managed { reader, .. } = captured.content else {
        panic!("expected managed Blob content");
    };
    assert_eq!(
        reader.read_range(0..reader.len()).await.unwrap().as_ref(),
        b"Old",
        "the stale live-branch capture returns its captured bytes, never the replacement bytes"
    );
    assert_eq!(
        read_managed_blob_bytes(&control, ReadTarget::branch("feature"), cell).await,
        b"New",
        "the replacement branch must contain different readable bytes"
    );
}

/// A change-feed poll captures its cut, then reopens each commit's per-branch
/// manifest snapshot lock-free. Deleting and recreating the polled branch in
/// that window reuses the physical path and numeric manifest version, so the
/// in-poll incarnation re-prove must reject the stale cut instead of emitting
/// the replacement branch's rows under the captured commit's label.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn change_feed_poll_refuses_delete_recreate_aba_after_cut_capture() {
    use omnigraph::changes::{
        ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart,
    };

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let schema = r#"
node Document {
    title: String @key
    content: String
}
"#;
    let setup = helpers::session(Omnigraph::init(&uri, schema).await.unwrap());
    setup
        .load_jsonl(
            r#"{"type":"Document","data":{"title":"aba","content":"base"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    setup.branch_create("feature").await.unwrap();
    setup
        .load(
            "feature",
            r#"{"type":"Document","data":{"title":"aba","content":"old"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(setup);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let control = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let old_entry = control
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();

    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::CHANGE_FEED_POST_CAPTURE);
    // Poll from the beginning so the feature-authored commit is reopened; its
    // snapshot is what the ABA retargets.
    let request = ChangeFeedRequest {
        branch: Some("feature".to_string()),
        position: ChangeFeedPosition::Start(ChangeFeedStart::Beginning),
        scope: ChangeFeedScope::default(),
        max_changes: None,
        max_bytes: None,
        max_commits: None,
    };
    let poll_task = tokio::spawn(async move { reader.poll_change_feed(request).await });
    rendezvous.wait_until_reached().await;

    // Keep the parked poll releasable even if a control-plane op fails.
    let replacement = async {
        control.branch_delete("feature").await?;
        control.branch_create("feature").await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"aba","content":"new"}}"#,
                LoadMode::Merge,
            )
            .await?;
        control.snapshot_of(ReadTarget::branch("feature")).await
    }
    .await;
    rendezvous.release();

    let new_snapshot = replacement.expect("delete/recreate replacement must complete");
    let new_entry = new_snapshot.dataset("node:Document").unwrap();
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "the regression must exercise same-version branch ABA"
    );

    let error = poll_task
        .await
        .unwrap()
        .expect_err("the stale cut must never emit the replacement branch's rows");
    assert!(
        matches!(
            error,
            OmniError::Manifest(ref manifest)
                if manifest.kind == ManifestErrorKind::BadRequest
                    && manifest
                        .message
                        .contains("has no persisted native-branch incarnation witness")
        ),
        "in-poll branch ABA must fail with the incarnation refusal, got {error:?}"
    );
}

/// The feed re-proves each commit's manifest head (`commit_snapshot`), but then
/// opens the per-table datasets SEPARATELY by (branch path, numeric version). A
/// branch delete/recreate AFTER the head proof but before the table open would
/// retarget the physical open to the replacement branch's rows. Two witnesses
/// close that second window: `open_at_entry_verified`'s per-table e_tag
/// comparison (defense-in-depth; exercised here — on local FS the synthetic
/// e_tag changes on recreation, so this arm fires first) and the logical
/// post-open `reprove_named_branch_heads` (exercised by the e_tag-less twin
/// below). The `CHANGE_FEED_POST_CAPTURE` test above covers the first window
/// (pre-`commit_snapshot`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn change_feed_poll_refuses_delete_recreate_aba_before_table_open() {
    use omnigraph::changes::{
        ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart,
    };

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let schema = r#"
node Document {
    title: String @key
    content: String
}
"#;
    let setup = helpers::session(Omnigraph::init(&uri, schema).await.unwrap());
    // Isolate a SINGLE commit (the feature-authored one) so its manifest head is
    // proven BEFORE the failpoint and only the per-table open can catch the ABA;
    // start after the base commit so a later commit's `commit_snapshot` (the
    // first-window guard) is not the one that catches it.
    let base = setup
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"aba","content":"base"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    let base_commit_id = base.commit.graph_commit_id.clone();
    setup.branch_create("feature").await.unwrap();
    setup
        .load(
            "feature",
            r#"{"type":"Document","data":{"title":"aba","content":"old"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(setup);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let control = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let old_entry = control
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::CHANGE_FEED_PRE_TABLE_OPEN);
    let request = ChangeFeedRequest {
        branch: Some("feature".to_string()),
        position: ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit(base_commit_id)),
        scope: ChangeFeedScope::default(),
        max_changes: None,
        max_bytes: None,
        max_commits: None,
    };
    let poll_task = tokio::spawn(async move { reader.poll_change_feed(request).await });
    rendezvous.wait_until_reached().await;

    let replacement = async {
        control.branch_delete("feature").await?;
        control.branch_create("feature").await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"aba","content":"new"}}"#,
                LoadMode::Merge,
            )
            .await?;
        control.snapshot_of(ReadTarget::branch("feature")).await
    }
    .await;
    rendezvous.release();

    let new_snapshot = replacement.expect("delete/recreate replacement must complete");
    let new_entry = new_snapshot.dataset("node:Document").unwrap();
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "the regression must exercise same-version branch ABA"
    );

    let error = poll_task
        .await
        .unwrap()
        .expect_err("the retargeted table open must not emit the replacement branch's rows");
    assert!(
        matches!(
            error,
            OmniError::Manifest(ref manifest)
                if manifest.kind == ManifestErrorKind::BadRequest
                    && manifest
                        .message
                        .contains("has no persisted native-branch incarnation witness")
        ),
        "in-poll table-open ABA must fail with the incarnation refusal, got {error:?}"
    );
}

/// The second ABA window, on a store that persists no table e_tags: with the
/// `CHANGE_FEED_ETAG_WITNESS` comparison skipped, only the logical post-open
/// witness (`reprove_named_branch_heads`) catches the delete/recreate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn change_feed_poll_refuses_table_open_aba_without_etag_witness() {
    use omnigraph::changes::{
        ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart,
    };

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let schema = r#"
node Document {
    title: String @key
    content: String
}
"#;
    let setup = helpers::session(Omnigraph::init(&uri, schema).await.unwrap());
    let base = setup
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"aba","content":"base"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    let base_commit_id = base.commit.graph_commit_id.clone();
    setup.branch_create("feature").await.unwrap();
    setup
        .load(
            "feature",
            r#"{"type":"Document","data":{"title":"aba","content":"old"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(setup);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let control = helpers::session(Omnigraph::open(&uri).await.unwrap());

    // Simulate an e_tag-less store for the whole poll: the per-table e_tag
    // comparison is skipped, exactly as on a store whose persisted version
    // metadata carries no e_tag.
    let _skip_etag = catalog::CHANGE_FEED_ETAG_WITNESS.fire_always();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::CHANGE_FEED_PRE_TABLE_OPEN);
    let request = ChangeFeedRequest {
        branch: Some("feature".to_string()),
        position: ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit(base_commit_id)),
        scope: ChangeFeedScope::default(),
        max_changes: None,
        max_bytes: None,
        max_commits: None,
    };
    let poll_task = tokio::spawn(async move { reader.poll_change_feed(request).await });
    rendezvous.wait_until_reached().await;

    let replacement = async {
        control.branch_delete("feature").await?;
        control.branch_create("feature").await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"aba","content":"new"}}"#,
                LoadMode::Merge,
            )
            .await
    }
    .await;
    rendezvous.release();
    replacement.expect("delete/recreate replacement must complete");

    let error = poll_task
        .await
        .unwrap()
        .expect_err("without an e_tag witness the logical post-open re-prove must still refuse");
    assert!(
        matches!(
            error,
            OmniError::Manifest(ref manifest)
                if manifest.kind == ManifestErrorKind::BadRequest
                    && manifest
                        .message
                        .contains("has no persisted native-branch incarnation witness")
        ),
        "the logical post-open witness must produce the incarnation refusal, got {error:?}"
    );
}

/// The THIRD ABA window: after the final post-open logical head witness, no
/// step of the poll may read the branch's numeric-path history live. Version
/// manifests sit at replaceable numeric paths (unlike UUID-named data and
/// transaction files), so a future `(begin, end]` history walk placed after
/// `CHANGE_FEED_POST_HEAD_WITNESS` could classify replacement history. The
/// current adjacent classifier reads the transaction referenced by the pinned
/// child manifest and stores its complete plan before this witness; this cell
/// locks that placement against a future widening. When replacement history at
/// the same versions is row-set-preserving while the ORIGINAL commit carried a
/// delete, a live post-witness classifier would silently omit that delete. The
/// poll may instead fail loudly (reader survival across branch recreation is
/// not promised), but any page it returns must carry the original delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn change_feed_poll_classifies_intervals_before_the_head_witness() {
    use omnigraph::changes::{
        ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart, ChangeOpKind,
    };

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let schema = r#"
node Document {
    title: String @key
    content: String
}
"#;
    let setup = helpers::session(Omnigraph::init(&uri, schema).await.unwrap());
    setup
        .load(
            "main",
            r#"{"type":"Document","data":{"title":"base","content":"base"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    setup.branch_create("feature").await.unwrap();
    // One load -> one fragment holding both rows. The follow-up delete then
    // only adds a deletion vector, so across the delete commit's interval the
    // retained manifests' fragment sets are identical — a wrongly-pruned
    // enumeration derives an EMPTY candidate set and emits nothing.
    let insert = setup
        .load_with_receipt(
            "feature",
            concat!(
                r#"{"type":"Document","data":{"title":"keep","content":"kept"}}"#,
                "\n",
                r#"{"type":"Document","data":{"title":"victim","content":"doomed"}}"#,
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let insert_commit_id = insert.commit.graph_commit_id.clone();
    setup
        .mutate(
            "feature",
            r#"
query remove_victim() {
    delete Document where title = "victim"
}
"#,
            "remove_victim",
            &mixed_params(&[], &[]),
        )
        .await
        .unwrap();
    drop(setup);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let control = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let old_entry = control
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(&catalog::CHANGE_FEED_POST_HEAD_WITNESS);
    let request = ChangeFeedRequest {
        branch: Some("feature".to_string()),
        position: ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit(insert_commit_id)),
        scope: ChangeFeedScope::default(),
        max_changes: None,
        max_bytes: None,
        max_commits: None,
    };
    let poll_task = tokio::spawn(async move { reader.poll_change_feed(request).await });
    rendezvous.wait_until_reached().await;

    // Replace the branch with history that is provably row-set-preserving at
    // the same table versions: two marker-carrying keyed loads, landing the
    // replacement's numeric version manifests exactly where the original
    // insert + delete commits left theirs.
    let replacement = async {
        control.branch_delete("feature").await?;
        control.branch_create("feature").await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"repl-one","content":"one"}}"#,
                LoadMode::Merge,
            )
            .await?;
        control
            .load(
                "feature",
                r#"{"type":"Document","data":{"title":"repl-two","content":"two"}}"#,
                LoadMode::Merge,
            )
            .await?;
        control.snapshot_of(ReadTarget::branch("feature")).await
    }
    .await;
    rendezvous.release();

    let new_snapshot = replacement.expect("delete/recreate replacement must complete");
    let new_entry = new_snapshot.dataset("node:Document").unwrap();
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "the regression must exercise same-version branch ABA"
    );

    if let Ok(page) = poll_task.await.unwrap() {
        let carries_original_delete = page
            .blocks
            .iter()
            .flat_map(|block| block.changes.iter())
            .any(|change| change.op == ChangeOpKind::Delete && change.id.contains("victim"));
        assert!(
            carries_original_delete,
            "a page returned across the in-poll delete/recreate must still carry the \
             original commit's delete; omitting it silently is the classification ABA \
             this cell pins: {page:?}"
        );
    }
    // A loud refusal is acceptable: reader survival across branch recreation
    // is not promised — only never-silent retargeting.
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
    let merge_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let control_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));

    let old_entry = helpers::snapshot_branch(merge_db.as_ref(), "target")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert!(
        old_entry.native_dataset_branch.is_none(),
        "the target's write keeps main's registration: branch writes never fork"
    );
    let old_target_ref = helpers::graph_native_ref(&uri, "target").await;

    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);
    let control_rv = helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_CONTROL_PRE_GATES);

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
    let target_unchanged_while_parked =
        helpers::graph_native_ref(&uri, "target").await == old_target_ref;
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

    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
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
    assert_ne!(
        helpers::graph_native_ref(&uri, "target").await,
        old_target_ref,
        "delete+recreate must mint a new target incarnation"
    );
    let new_entry = helpers::snapshot_branch(&reopened, "target")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(
        (&new_entry.dataset_path, new_entry.published_dataset_version),
        (&old_entry.dataset_path, old_entry.published_dataset_version),
        "the regression fixture must exercise same-path/same-version ABA"
    );
}

/// `sync_branch` replaces a handle's active coordinator. It must join the same
/// schema authority gate held by branch merge, so the handle's binding remains
/// stable throughout the captured operation and its publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_fences_concurrent_sync_on_same_handle() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    db.branch_create("other").await.unwrap();
    let db = std::sync::Arc::new(db);
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

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

/// The post-table-gate merge check must read storage, not a warm
/// coordinator's cached snapshot. Advance the target branch while merge is
/// parked after authority capture; the legacy publish seam moves both the
/// table pin and graph lineage, so fresh authority comparison catches the
/// stale plan before any merge effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_rejects_fresh_target_manifest_change_before_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let target_writer = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let target_published = helpers::snapshot_branch(&target_writer, "target")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    let before = helpers::version_branch(&merge_db, "target").await.unwrap();
    forge_linear_head_from_pin(&target_writer, "target", "node:Person", target_published).await;
    let publish_result = target_writer
        .failpoint_publish_table_head_without_index_rebuild_for_test("target", "node:Person", None)
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
        }) if member == "graph_head:target"
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
    let merge_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let source_writer = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let source_published = helpers::snapshot_branch(&source_writer, "source")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .published_dataset_version;
    let captured_source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    let merge_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);

    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;

    forge_linear_head_from_pin(&source_writer, "source", "node:Person", source_published).await;
    source_writer
        .failpoint_publish_table_head_without_index_rebuild_for_test("source", "node:Person", None)
        .await
        .unwrap();
    let advanced_source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    assert_ne!(advanced_source_head, captured_source_head);
    merge_rv.release();

    assert_eq!(
        merge_task.await.unwrap().unwrap(),
        omnigraph::db::MergeOutcome::Merged
    );
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
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

/// A captured merge input may be superseded while another process collects.
/// An acknowledged pointer switch must still name a readable detached pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_captured_source_survives_concurrent_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(
        Omnigraph::init(
            &uri,
            "node Person {\n    name: String @key\n    age: I32\n}",
        )
        .await
        .unwrap(),
    );
    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"p","age":0}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    for age in [1, 2] {
        db.load(
            "source",
            &format!(r#"{{"type":"Person","data":{{"name":"p","age":{age}}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }
    let captured = pinned_version(&db, "source", "node:Person").await;
    let person_uri = node_table_uri(&db, "Person").await;
    let captured_path =
        std::path::Path::new(&person_uri).join(format!("_versions/d{captured}.manifest"));
    assert!(captured_path.is_file());
    let captured_source_head = branch_head_commit_id(dir.path(), "source").await.unwrap();
    let merge_db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let rendezvous = helpers::failpoint::Rendezvous::park_first(
        &catalog::BRANCH_MERGE_POST_CANDIDATE_VALIDATION,
    );
    let _no_table_effect = catalog::BRANCH_MERGE_POST_TABLE_EFFECT.fire_always();
    let merge = tokio::spawn(async move { merge_db.branch_merge("source", "target").await });
    rendezvous.wait_until_reached().await;
    let child_uri = uri.clone();
    let child = tokio::task::spawn_blocking(move || {
        run_rfc023_external_writer_action(
            child_uri,
            LoadMode::Merge,
            r#"{"type":"Person","data":{"name":"p","age":3}}"#.to_string(),
            "collector_advance_merge_source",
        )
    })
    .await;
    let captured_survived = captured_path.is_file();
    rendezvous.release();
    let merged = merge.await.unwrap();
    child.unwrap().unwrap();
    eprintln!(
        "captured pin {captured}: present after cleanup={captured_survived}; merge={merged:?}"
    );
    assert_eq!(merged.unwrap(), omnigraph::db::MergeOutcome::FastForward);

    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let published = pinned_version(&reader, "target", "node:Person").await;
    assert_eq!(
        published, captured,
        "the merge is a captured-source pointer switch"
    );
    assert_ne!(
        pinned_version(&reader, "source", "node:Person").await,
        captured
    );
    let target_head = branch_head_commit_id(dir.path(), "target").await.unwrap();
    assert_eq!(
        reader
            .get_commit(&target_head)
            .await
            .unwrap()
            .merged_parent_commit_id
            .as_deref(),
        Some(captured_source_head.as_str()),
    );
    let snapshot = helpers::snapshot_branch(&reader, "target").await.unwrap();
    let person = snapshot.open_dataset("node:Person").await.expect(
        "an acknowledged pointer switch must not publish a detached pin reclaimed during merge",
    );
    assert_eq!(
        person
            .count_rows(Some("age = 2".to_string()))
            .await
            .unwrap(),
        1
    );
}

/// A delete parked at its primary-delete point loses the manifest CAS to a
/// concurrent update on the same table: its detached effect is never
/// published, the linear history never moves, and no sidecar exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn inline_delete_conflict_rejects_without_effect() {
    use std::sync::Arc;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(init_and_load(&dir).await);
    let person_uri = node_table_uri(&db, "Person").await;
    let (head_before, pin_before) = person_head_and_published(&db, "main").await;
    {
        let rv = helpers::failpoint::Rendezvous::park_first(
            &catalog::MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE,
        );
        let del_db = Arc::clone(&db);
        let delete = tokio::spawn(async move {
            let delete_params = helpers::params(&[("$name", "Alice")]);
            del_db
                .mutate("main", MUTATION_QUERIES, "remove_person", &delete_params)
                .await
        });
        rv.wait_until_reached().await;
        let concurrent = helpers::session(Omnigraph::open(&uri).await.unwrap());
        mutate_main(
            &concurrent,
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
        )
        .await
        .expect("concurrent update must land while the delete is parked");
        rv.release();
        let err = delete.await.unwrap().unwrap_err();
        assert!(
            matches!(
                &err,
                OmniError::Manifest(manifest) if manifest.kind == ManifestErrorKind::Conflict
            ),
            "unexpected error: {err}"
        );
    }
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        4,
        "a conflicted delete removes nothing"
    );
    let (_, pin) = person_head_and_published(&fresh, "main").await;
    assert_eq!(pin, pin_before + 1, "only the update published a pin");
    assert_person_pin_detached(&fresh, "main", head_before).await;
    assert_eq!(
        helpers::open_dataset_head(&person_uri, None)
            .await
            .version()
            .version,
        head_before,
        "no writer moved the linear history"
    );
}

/// Issue #554 under RFC 0067: a write that dies after its detached effect
/// and before publication leaves no sidecar and no drift; the next writes
/// proceed, the failed row never appears, and every pin stays detached.
#[tokio::test]
#[serial]
async fn interrupted_write_leaves_main_writable_without_recovery_issue_554() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = count_rows(&db, "node:Person").await;
    let head_before = helpers::open_dataset_head(&person_uri, None)
        .await
        .version()
        .version;
    {
        let _failpoint = catalog::MUTATION_POST_TABLE_COMMIT.fire_always();
        let err = mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: mutation.post_table_commit"),
            "unexpected error: {err}"
        );
    }
    assert!(
        sidecar_operation_ids(dir.path()).is_empty(),
        "a detached effect arms no recovery sidecar"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before,
        "the failed write is invisible"
    );
    assert_eq!(
        helpers::open_dataset_head(&person_uri, None)
            .await
            .version()
            .version,
        head_before,
        "a detached effect never moves the linear head"
    );

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 23)]),
    )
    .await
    .unwrap();
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    mutate_main(
        &fresh,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Grace")], &[("$age", 24)]),
    )
    .await
    .unwrap();
    let names = collect_column_strings(&read_table(&fresh, "node:Person").await, "name");
    assert!(!names.iter().any(|name| name == "Eve"), "{names:?}");
    assert!(
        names.iter().any(|name| name == "Frank") && names.iter().any(|name| name == "Grace"),
        "{names:?}"
    );
    assert_eq!(count_rows(&fresh, "node:Person").await, before + 2);
    assert_person_pin_detached(&fresh, "main", head_before).await;
    assert!(sidecar_operation_ids(dir.path()).is_empty());
}

/// Linear HEAD and published version of a branch's Person table.
async fn person_head_and_published(db: &Omnigraph, branch: &str) -> (u64, u64) {
    let snapshot = if branch == "main" {
        helpers::snapshot_main(db).await.unwrap()
    } else {
        helpers::snapshot_branch(db, branch).await.unwrap()
    };
    let entry = snapshot.dataset("node:Person").unwrap();
    let uri = node_table_uri(db, "Person").await;
    let head = helpers::open_dataset_head_exact(&uri, entry.native_dataset_branch.as_deref()).await;
    (head.version().version, entry.published_dataset_version)
}

/// The branch's Person pin is a detached version and the linear HEAD is
/// still `head_before`: no writer moves it under the detached-only switch.
async fn assert_person_pin_detached(db: &Omnigraph, branch: &str, head_before: u64) {
    let (head, _) = person_head_and_published(db, branch).await;
    assert_eq!(
        head, head_before,
        "{branch}: no writer moves the linear HEAD"
    );
    let pinned = pinned_version(db, branch, "node:Person").await;
    assert!(
        is_detached_version(pinned),
        "{branch}: the Person pin {pinned} is a detached version"
    );
}

/// Publish one Person insert on `branch` and prove its pin is detached.
async fn write_detached_person_pin(db: &Session, branch: &str, name: &str) {
    let (head_before, _) = person_head_and_published(db, branch).await;
    db.mutate(
        branch,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", name)], &[("$age", 30)]),
    )
    .await
    .unwrap();
    assert_person_pin_detached(db, branch, head_before).await;
}

/// A failure between the detached index commit and publication leaves the
/// graph unchanged: no sidecar, no linear movement, no pin; the next pass
/// rebuilds and publishes a detached pin.
#[tokio::test]
#[serial]
async fn ensure_indices_failure_before_publish_leaves_no_residue() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = graph_with_unbuilt_indexes(&dir).await;
    let before = person_head_and_published(&db, "main").await;
    let version = helpers::version_main(&db).await.unwrap();
    {
        let _failpoint = catalog::ENSURE_INDICES_POST_TABLE_EFFECT.fire_always();
        let err = db.ensure_indices().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: ensure_indices.post_table_effect"),
            "unexpected error: {err}"
        );
    }
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(person_head_and_published(&db, "main").await, before);
    assert_eq!(helpers::version_main(&db).await.unwrap(), version);

    db.ensure_indices().await.unwrap();
    assert_person_pin_detached(&db, "main", before.0).await;
    assert_eq!(
        person_head_and_published(&db, "main").await.1,
        before.1 + 1,
        "the retry publishes one index pin"
    );
}

const SEARCH_SCHEMA: &str = include_str!("fixtures/search.pg");
const SEARCH_DATA: &str = include_str!("fixtures/search.jsonl");
const SEARCH_QUERIES: &str = include_str!("fixtures/search.gq");

/// HEAD and published version of the Doc table on `branch`.
async fn doc_head_and_published(db: &Omnigraph, branch: &str) -> (u64, u64) {
    let snapshot = if branch == "main" {
        helpers::snapshot_main(db).await.unwrap()
    } else {
        helpers::snapshot_branch(db, branch).await.unwrap()
    };
    let entry = snapshot.dataset("node:Doc").unwrap();
    let uri = node_table_uri(db, "Doc").await;
    let head = helpers::open_dataset_head_exact(&uri, entry.native_dataset_branch.as_deref()).await;
    (head.version().version, entry.published_dataset_version)
}

async fn search_titles(db: &Session, branch: &str, term: &str) -> Vec<String> {
    let result = if branch == "main" {
        helpers::query_main(db, SEARCH_QUERIES, "text_search", &params(&[("$q", term)]))
            .await
            .unwrap()
    } else {
        helpers::query_branch(
            db,
            branch,
            SEARCH_QUERIES,
            "text_search",
            &params(&[("$q", term)]),
        )
        .await
        .unwrap()
    };
    let mut titles = helpers::first_column_sorted(&result);
    titles.sort();
    titles
}

/// A full-text rebuild on a branch stages on the inherited Doc table: the
/// branch keeps main's registration shape with its own detached pin, and
/// main is untouched.
#[tokio::test]
#[serial]
async fn full_text_rebuild_on_a_branch_stages_on_the_inherited_table() {
    use omnigraph::loader::LoadMode;
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(Omnigraph::init(&uri, SEARCH_SCHEMA).await.unwrap());
    db.load_jsonl(SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db.branch_create("rebuild").await.unwrap();
    let main_before = doc_head_and_published(&db, "main").await;
    let main_entry = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Doc")
        .unwrap()
        .clone();
    let main_pin = pinned_version(&db, "main", "node:Doc").await;
    let branch_version = helpers::version_branch(&db, "rebuild").await.unwrap();

    db.rebuild_full_text_indices_on("rebuild")
        .await
        .expect("a branch rebuild stages on the inherited table and never forks");
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    let rebuilt = helpers::snapshot_branch(&db, "rebuild").await.unwrap();
    let rebuilt_entry = rebuilt.dataset("node:Doc").unwrap();
    assert_eq!(rebuilt_entry.dataset_path, main_entry.dataset_path);
    assert_eq!(
        rebuilt_entry.native_dataset_branch, main_entry.native_dataset_branch,
        "the branch keeps the inherited registration: the rebuild never forks"
    );
    assert!(main_entry.native_dataset_branch.is_none());
    assert!(helpers::version_branch(&db, "rebuild").await.unwrap() > branch_version);
    let rebuilt_pin = pinned_version(&db, "rebuild", "node:Doc").await;
    assert!(is_detached_version(rebuilt_pin), "{rebuilt_pin}");
    assert_ne!(
        rebuilt_pin, main_pin,
        "the rebuild publishes its own detached pin"
    );
    assert_eq!(
        doc_head_and_published(&db, "rebuild").await.0,
        main_before.0,
        "the rebuild never moves the linear HEAD"
    );
    assert_eq!(doc_head_and_published(&db, "main").await, main_before);
    assert_eq!(pinned_version(&db, "main", "node:Doc").await, main_pin);
    assert!(!search_titles(&db, "rebuild", "Machine").await.is_empty());
}

/// The next write extends a published detached pin after a lost acknowledgement.
async fn assert_write_detached_pin_survives_next_write(uri: &str) {
    let db = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());
    let head_before = person_head_and_published(&db, "main").await.0;
    {
        let _failpoint = catalog::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT.fire_once_at(1);
        let error = db
            .load_jsonl(
                r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Company","data":{"name":"Acme"}}
"#,
                LoadMode::Merge,
            )
            .await
            .expect_err("publication landed before its acknowledgement was lost");
        assert!(
            error
                .to_string()
                .contains("graph_publish.after_manifest_commit"),
            "the publication boundary must be reached: {error}"
        );
    }
    assert_person_pin_detached(&db, "main", head_before).await;
    let first_pin = pinned_version(&db, "main", "node:Person").await;
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 1);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);

    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Bob","age":25}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("the next write extends the detached pin");
    assert_person_pin_detached(&db, "main", head_before).await;
    assert_ne!(pinned_version(&db, "main", "node:Person").await, first_pin);
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);

    drop(db);
    let db = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_person_pin_detached(&db, "main", head_before).await;
    assert_eq!(helpers::count_rows(&db, "node:Person").await, 2);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 1);
}

/// Compaction and a later write keep their detached pins across a lost acknowledgement.
async fn assert_optimize_detached_pin_survives_next_write(uri: &str) {
    let db = helpers::session(Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap());
    db.load_jsonl(helpers::TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    for (name, age) in [("opt-a", 41), ("opt-b", 42), ("opt-c", 43)] {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", age)]),
        )
        .await
        .unwrap();
    }
    let rows = helpers::count_rows(&db, "node:Person").await;
    let head_before = person_head_and_published(&db, "main").await.0;
    let before_pin = pinned_version(&db, "main", "node:Person").await;
    {
        let _failpoint = catalog::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT.fire_once_at(1);
        let error = db
            .optimize()
            .await
            .expect_err("compaction published before its acknowledgement was lost");
        assert!(
            error
                .to_string()
                .contains("graph_publish.after_manifest_commit"),
            "the publication boundary must be reached: {error}"
        );
    }
    assert_person_pin_detached(&db, "main", head_before).await;
    let compacted_pin = pinned_version(&db, "main", "node:Person").await;
    assert_ne!(compacted_pin, before_pin, "Person must actually compact");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows);

    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"AfterOptimize","age":25}}
"#,
        LoadMode::Merge,
    )
    .await
    .expect("the next write extends the compacted detached pin");
    assert_person_pin_detached(&db, "main", head_before).await;
    assert_ne!(
        pinned_version(&db, "main", "node:Person").await,
        compacted_pin
    );
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows + 1);

    drop(db);
    let db = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows + 1);
    assert_person_pin_detached(&db, "main", head_before).await;
}

/// Exercise both shared backend contracts locally even without configured cloud fixtures.
#[tokio::test]
#[serial]
async fn local_detached_backend_contracts_survive_lost_ack_and_next_write() {
    let _scenario = FailScenario::setup();
    let write = tempfile::tempdir().unwrap();
    assert_write_detached_pin_survives_next_write(write.path().to_str().unwrap()).await;
    let optimize = tempfile::tempdir().unwrap();
    assert_optimize_detached_pin_survives_next_write(optimize.path().to_str().unwrap()).await;
}

/// Detached write retention on RustFS/S3, when its existing test bucket is configured.
#[tokio::test]
#[serial]
async fn s3_write_detached_pin_survives_next_write() {
    let Some(uri) = helpers::s3_test_graph_uri("failpoints") else {
        eprintln!(
            "skipping s3_write_detached_pin_survives_next_write: OMNIGRAPH_S3_TEST_BUCKET is not set"
        );
        return;
    };
    let _scenario = FailScenario::setup();
    assert_write_detached_pin_survives_next_write(&uri).await;
}

/// Detached compaction retention on RustFS/S3, when its test bucket is configured.
#[tokio::test]
#[serial]
async fn s3_optimize_detached_pin_survives_next_write() {
    let Some(uri) = helpers::s3_test_graph_uri("failpoints") else {
        eprintln!(
            "skipping s3_optimize_detached_pin_survives_next_write: OMNIGRAPH_S3_TEST_BUCKET is not set"
        );
        return;
    };
    let _scenario = FailScenario::setup();
    assert_optimize_detached_pin_survives_next_write(&uri).await;
}

/// A fresh graph root under the configured Azurite container.
fn azure_test_graph_uri(name: &str) -> Option<String> {
    let container = std::env::var("OMNIGRAPH_AZURE_TEST_CONTAINER").ok()?;
    Some(format!(
        "az://{container}/engine-failpoints/{name}-{}",
        ulid::Ulid::new()
    ))
}

/// Delete the fixture's graph prefix from the configured Azurite container.
async fn delete_azure_graph(uri: &str) {
    omnigraph_storage::storage_for_uri(uri)
        .unwrap()
        .delete_prefix(uri)
        .await
        .unwrap();
}

/// Detached write retention on configured Azure/Azurite, followed by fixture cleanup.
#[tokio::test]
#[serial]
async fn azure_write_detached_pin_survives_next_write() {
    let Some(uri) = azure_test_graph_uri("write-detached-pin") else {
        eprintln!("skipping Azure write detached pin: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
        return;
    };
    let _scenario = FailScenario::setup();
    assert_write_detached_pin_survives_next_write(&uri).await;
    delete_azure_graph(&uri).await;
}

/// Detached compaction retention on configured Azure/Azurite, followed by fixture cleanup.
#[tokio::test]
#[serial]
async fn azure_optimize_detached_pin_survives_next_write() {
    let Some(uri) = azure_test_graph_uri("optimize-detached-pin") else {
        eprintln!(
            "skipping Azure optimize detached pin: OMNIGRAPH_AZURE_TEST_CONTAINER is not set"
        );
        return;
    };
    let _scenario = FailScenario::setup();
    assert_optimize_detached_pin_survives_next_write(&uri).await;
    delete_azure_graph(&uri).await;
}
/// Person HEAD and published version on `branch`, with the linear HEAD read
/// from the ref the entry names.
async fn person_versions(db: &Omnigraph, branch: &str) -> (u64, u64) {
    person_head_and_published(db, branch).await
}

/// RFC 0067: a merge failure anywhere between its first detached chunk and
/// its publication leaves the target exactly as it was: no sidecar, no
/// linear movement, no pin; the chain is reclaimable garbage and the retry
/// publishes the complete delta. `scenario` selects the fast-forward adopt
/// path or the three-way rewrite path; `seam` the window.
async fn assert_partial_merge_leaves_no_residue(
    scenario: MergeScenario,
    seam: &'static omnigraph::seams::DecideSeam,
) {
    let carol_ages = |batches: &[RecordBatch]| {
        batches
            .iter()
            .flat_map(|batch| {
                let names = batch
                    .column_by_name("name")
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
                (0..batch.num_rows())
                    .filter(|&row| names.value(row) == "carol")
                    .map(|row| ages.value(row))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed main {alice, carol, dave}; on `feature` add bob, bump carol, remove
    // dave. For Rewrite, also move main past base so the table classifies
    // RewriteMerged instead of a fast-forward AdoptWithDelta.
    let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
    db.load_jsonl(
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
    let before = person_versions(&db, "main").await;
    assert!(
        is_detached_version(pinned_version(&db, "main", "node:Person").await),
        "fixture main's pin is a detached version"
    );

    {
        let _fp = seam.fire_always();
        let err = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(
            err.to_string().contains(seam.name()),
            "expected the injected failpoint {}, got: {err}",
            seam.name()
        );
        assert!(
            !matches!(err, OmniError::RecoveryRequired { .. }),
            "a failed detached merge owns no recovery: {err}"
        );
    }
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        person_versions(&db, "main").await,
        before,
        "the chain moved neither HEAD nor the pin at {}",
        seam.name()
    );
    assert_eq!(
        sorted_person_names(&db).await,
        vec!["alice", "carol", "dave"],
        "main is unchanged after a failure at {}",
        seam.name()
    );
    assert_eq!(carol_ages(&read_table(&db, "node:Person").await), [50]);

    db.branch_merge("feature", "main").await.unwrap();
    assert_eq!(
        sorted_person_names(&db).await,
        vec!["alice", "bob", "carol"],
        "the retry after {} applies the full delta",
        seam.name()
    );
    assert_eq!(carol_ages(&read_table(&db, "node:Person").await), [55]);
    let after = person_versions(&db, "main").await;
    assert_person_pin_detached(&db, "main", before.0).await;
    assert!(after.1 > before.1);
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        sorted_person_names(&fresh).await,
        vec!["alice", "bob", "carol"]
    );
}

/// A merge onto an unadvanced main is a pointer switch: `seam`, a window of
/// the materializing adopt chain, is never reached, nothing is staged, and
/// main takes the source's pin with its `dataset_path` unchanged.
async fn assert_merge_onto_main_is_a_pointer_switch(seam: &'static omnigraph::seams::DecideSeam) {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(Omnigraph::init(&uri, helpers::TEST_SCHEMA).await.unwrap());
    db.load_jsonl(
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
    let source_entry = helpers::snapshot_branch(&db, "feature")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let source_pin = pinned_version(&db, "feature", "node:Person").await;
    let main_entry = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let before = person_versions(&db, "main").await;

    let probes = MergeWriteProbes::default();
    {
        let _fp = seam.fire_always();
        assert_eq!(
            with_merge_write_probes(probes.clone(), db.branch_merge("feature", "main"))
                .await
                .unwrap(),
            omnigraph::db::MergeOutcome::FastForward,
            "a merge onto main never reaches {}",
            seam.name()
        );
    }
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(probes.stage_append_calls(), 0);
    assert_eq!(probes.stage_merge_insert_calls(), 0);
    assert_eq!(probes.stage_fenced_insert_calls(), 0);
    assert_eq!(probes.stage_known_present_update_calls(), 0);
    let merged_entry = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(merged_entry.dataset_path, main_entry.dataset_path);
    assert_eq!(
        merged_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    assert_eq!(
        pinned_version(&db, "main", "node:Person").await,
        source_pin,
        "main takes the source's detached pin"
    );
    assert_person_pin_detached(&db, "main", before.0).await;
    assert_eq!(
        sorted_person_names(&db).await,
        vec!["alice", "bob", "carol"]
    );
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        sorted_person_names(&fresh).await,
        vec!["alice", "bob", "carol"]
    );
}

#[tokio::test]
#[serial]
async fn branch_merge_onto_main_never_reaches_adopt_after_append() {
    assert_merge_onto_main_is_a_pointer_switch(
        &catalog::BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_onto_main_never_reaches_adopt_after_upsert() {
    assert_merge_onto_main_is_a_pointer_switch(
        &catalog::BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_rewrite_partial_after_insert_leaves_no_residue() {
    assert_partial_merge_leaves_no_residue(
        MergeScenario::Rewrite,
        &catalog::BRANCH_MERGE_REWRITE_AFTER_INSERT_PRE_UPDATE,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_rewrite_partial_after_merge_leaves_no_residue() {
    assert_partial_merge_leaves_no_residue(
        MergeScenario::Rewrite,
        &catalog::BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_rewrite_partial_after_delete_leaves_no_residue() {
    assert_partial_merge_leaves_no_residue(
        MergeScenario::Rewrite,
        &catalog::BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_failure_after_table_effects_leaves_no_residue() {
    assert_partial_merge_leaves_no_residue(
        MergeScenario::Rewrite,
        &catalog::BRANCH_MERGE_POST_TABLE_EFFECT,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_failure_before_publish_leaves_no_residue() {
    assert_partial_merge_leaves_no_residue(
        MergeScenario::Adopt,
        &catalog::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    )
    .await;
}

/// A multi-chunk merge onto an unadvanced main is a pointer switch: the
/// between-chunk `seam` is never reached and main takes the source's pin
/// without moving the linear HEAD.
async fn assert_multichunk_merge_onto_main_is_a_pointer_switch(
    uri: &str,
    person_uri: &str,
    seam: &'static omnigraph::seams::DecideSeam,
    rows_before: usize,
    rows_after: usize,
) {
    let dir = std::path::Path::new(uri);
    let db = helpers::session(Omnigraph::open(uri).await.unwrap());
    let source_entry = helpers::snapshot_branch(&db, "feature")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let source_pin = pinned_version(&db, "feature", "node:Person").await;
    let main_path = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .dataset_path
        .clone();
    let head_before = helpers::open_dataset_head_exact(person_uri, None)
        .await
        .version()
        .version;
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows_before);
    {
        let _fp = seam.fire_always();
        assert_eq!(
            db.branch_merge("feature", "main").await.unwrap(),
            omnigraph::db::MergeOutcome::FastForward,
            "a merge onto main never reaches {}",
            seam.name()
        );
    }
    assert!(sidecar_operation_ids(dir).is_empty());
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows_after);
    let merged_entry = helpers::snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(merged_entry.dataset_path, main_path);
    assert_eq!(
        merged_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    assert_eq!(
        pinned_version(&db, "main", "node:Person").await,
        source_pin,
        "main takes the source's detached pin"
    );
    assert_person_pin_detached(&db, "main", head_before).await;
}

#[tokio::test]
#[serial]
async fn branch_merge_multichunk_insert_onto_main_never_reaches_the_chunk_seam() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, person_uri, _) = setup_branch_merge_multichunk_adopt(&dir).await;
    assert_multichunk_merge_onto_main_is_a_pointer_switch(
        &uri,
        &person_uri,
        &catalog::BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS,
        1,
        8192 + 2,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn branch_merge_multichunk_delete_onto_main_never_reaches_the_chunk_seam() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, person_uri, _) = setup_branch_merge_multichunk_delete(&dir).await;
    assert_multichunk_merge_onto_main_is_a_pointer_switch(
        &uri,
        &person_uri,
        &catalog::BRANCH_MERGE_BETWEEN_DELETE_CHUNKS,
        8192 + 2,
        1,
    )
    .await;
}

/// A target that advances after the merge's detached effects makes the merge
/// lose its manifest CAS: the winner's state stays, the chain is garbage, no
/// sidecar exists, and a retry merges over the winner. The winner publishes
/// through the test-only publisher that bypasses the process-local queues
/// the parked merge holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_loses_the_manifest_cas_after_detached_effects() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let winner = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let before = person_versions(&winner, "target").await;

    let merge_rv = helpers::failpoint::Rendezvous::park_first(
        &catalog::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );
    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;
    let winner_head = forge_linear_head_from_pin(&winner, "target", "node:Person", before.1).await;
    winner
        .failpoint_publish_table_head_without_index_rebuild_for_test("target", "node:Person", None)
        .await
        .unwrap();
    merge_rv.release();

    let error = merge_task
        .await
        .unwrap()
        .expect_err("a target advance after the detached effects loses the merge's CAS");
    assert!(
        matches!(&error, OmniError::Manifest(manifest) if manifest.kind == ManifestErrorKind::Conflict),
        "{error}"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        helpers::count_rows_branch(&winner, "target", "node:Person").await,
        main_rows + 1,
        "only the fixture's target row is visible"
    );
    let after = person_versions(&winner, "target").await;
    assert_eq!(
        after,
        (winner_head, winner_head),
        "the winner's linear pin stands"
    );

    merge_db.branch_merge("source", "target").await.unwrap();
    assert_eq!(
        helpers::count_rows_branch(&winner, "target", "node:Person").await,
        main_rows + 2
    );
    assert_person_pin_detached(&winner, "target", winner_head).await;
}

/// A foreign linear commit that lands on the target's table after the
/// merge's detached effects does not block the merge: the merge publishes
/// one detached pin, and no later write moves the linear history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn branch_merge_foreign_linear_commit_after_effects_does_not_block_the_merge() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, main_rows) = setup_diverged_merge_branches(&dir).await;
    let merge_db = std::sync::Arc::new(helpers::session(Omnigraph::open(&uri).await.unwrap()));
    let observer = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let before = person_versions(&observer, "target").await;
    let person_uri = node_table_uri(&observer, "Person").await;

    let merge_rv = helpers::failpoint::Rendezvous::park_first(
        &catalog::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    );
    let merge_handle = std::sync::Arc::clone(&merge_db);
    let merge_task =
        tokio::spawn(async move { merge_handle.branch_merge("source", "target").await });
    merge_rv.wait_until_reached().await;
    let mut raw = helpers::open_dataset_head_exact(&person_uri, None).await;
    helpers::lance_delete_inline(&mut raw, "1 = 2").await;
    let foreign = raw.version().version;
    assert_eq!(
        foreign,
        before.0 + 1,
        "the foreign commit is linear history"
    );
    merge_rv.release();

    merge_task
        .await
        .unwrap()
        .expect("the merge publishes over a foreign linear commit");
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    let after = person_versions(&observer, "target").await;
    assert_eq!(after.1, before.1 + 1, "the merge publishes one pin");
    assert_person_pin_detached(&observer, "target", foreign).await;
    assert_eq!(
        helpers::count_rows_branch(&observer, "target", "node:Person").await,
        main_rows + 2,
        "reads serve the merged rows through the pin"
    );

    observer
        .mutate(
            "target",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "behind-the-block")], &[("$age", 45)]),
        )
        .await
        .unwrap();
    assert_eq!(
        helpers::count_rows_branch(&observer, "target", "node:Person").await,
        main_rows + 3
    );
    let later = person_versions(&observer, "target").await;
    assert_eq!(later.0, after.0, "the later write moves no linear history");
    assert_eq!(later.1, after.1 + 1, "the later write publishes one pin");
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&fresh, "target", "node:Person").await,
        main_rows + 3
    );
}

/// A three-way merge into a named target stages on the inherited table: the
/// target keeps the inherited registration shape under its own detached pin.
#[tokio::test]
#[serial]
async fn branch_merge_into_a_named_target_stages_on_the_inherited_table() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.branch_create("donor").await.unwrap();
    db.mutate(
        "donor",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "target-inherited")], &[("$age", 38)]),
    )
    .await
    .unwrap();
    db.branch_create_from(ReadTarget::branch("donor"), "target")
        .await
        .unwrap();
    let inherited = helpers::snapshot_branch(&db, "target").await.unwrap();
    let inherited_entry = inherited.dataset("node:Person").unwrap().clone();
    db.mutate(
        "source",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "source-first-touch")], &[("$age", 37)]),
    )
    .await
    .unwrap();

    let inherited_pin = pinned_version(&db, "target", "node:Person").await;
    let (head_before, _) = person_versions(&db, "target").await;
    assert_eq!(
        db.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::Merged,
        "a merge into a named target whose target advanced is a three-way merge"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    let merged = helpers::snapshot_branch(&db, "target").await.unwrap();
    let merged_entry = merged.dataset("node:Person").unwrap();
    assert!(inherited_entry.native_dataset_branch.is_none());
    assert_eq!(
        merged_entry.native_dataset_branch, inherited_entry.native_dataset_branch,
        "the target keeps the inherited registration: the merge never forks"
    );
    assert_eq!(merged_entry.dataset_path, inherited_entry.dataset_path);
    assert!(merged_entry.published_dataset_version > inherited_entry.published_dataset_version);
    assert_ne!(
        pinned_version(&db, "target", "node:Person").await,
        inherited_pin,
        "the merge publishes its own detached pin"
    );
    assert_person_pin_detached(&db, "target", head_before).await;
    assert_eq!(
        helpers::count_rows_branch(&db, "target", "node:Person").await,
        main_rows + 2
    );
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&fresh, "target", "node:Person").await,
        main_rows + 2
    );
}

/// A fast-forward pointer merge adopts the source's entry as it is: the same
/// detached pin is then registered on both branches, reads on the target
/// serve it, and the next writer of the target writes behind it.
#[tokio::test]
#[serial]
async fn branch_merge_pointer_adoption_carries_the_source_pin() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::init_and_load(&dir).await;
    let main_rows = helpers::count_rows(&db, "node:Person").await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    write_detached_person_pin(&db, "source", "pointer-source").await;
    let source_entry = helpers::snapshot_branch(&db, "source")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let source_pin = pinned_version(&db, "source", "node:Person").await;

    assert_eq!(
        db.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::FastForward
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    let target_entry = helpers::snapshot_branch(&db, "target")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(
        target_entry.native_dataset_branch,
        source_entry.native_dataset_branch
    );
    assert_eq!(
        target_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    assert_eq!(
        helpers::count_rows_branch(&db, "target", "node:Person").await,
        main_rows + 1,
        "the target serves the source's pin"
    );
    assert_eq!(
        pinned_version(&db, "target", "node:Person").await,
        source_pin,
        "the adopted pin is the source's detached version"
    );

    db.mutate(
        "target",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "target-writer")], &[("$age", 21)]),
    )
    .await
    .unwrap();
    let fresh = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&fresh, "target", "node:Person").await,
        main_rows + 2
    );
    assert_eq!(
        helpers::count_rows_branch(&fresh, "source", "node:Person").await,
        main_rows + 1
    );
}

// Review-only repros for the RFC 0067 stack. Reuse the failure-window fixtures.

/// RFC 0067: a handle whose snapshot predates another handle's write holds
/// a stale read set: its write reprepares from the current manifest instead
/// of being refused with a repair conflict.
#[tokio::test]
#[serial]
async fn rfc_0067_stale_handle_write_after_another_write_reprepares() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let first = init_and_load(&dir).await;
    let second = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let stale = helpers::snapshot_main(&second).await.unwrap();
    write_detached_person_pin(&first, "main", "first-writer").await;
    let (_, published) = person_head_and_published(&first, "main").await;
    assert_eq!(
        published,
        stale
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version
            + 1,
        "the first write published one pin past the stale handle's registration"
    );
    mutate_main(
        &second,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "second-writer")], &[("$age", 31)]),
    )
    .await
    .expect("a stale handle reprepares instead of refusing with drift");
    let names = collect_column_strings(&read_table(&second, "node:Person").await, "name");
    assert!(names.contains(&"first-writer".to_string()), "{names:?}");
    assert!(names.contains(&"second-writer".to_string()), "{names:?}");
}

#[tokio::test]
#[serial]
async fn rfc_0067_detached_pin_diff_keeps_the_acknowledged_insert() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = helpers::snapshot_id(&db, "main").await.unwrap();
    write_detached_person_pin(&db, "main", "detached-diff").await;
    assert!(
        collect_column_strings(&read_table(&db, "node:Person").await, "name")
            .contains(&"detached-diff".to_string())
    );
    let after = helpers::snapshot_id(&db, "main").await.unwrap();
    let changes = db
        .diff_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &omnigraph::changes::ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        changes.stats.inserts, 1,
        "acknowledged insert absent from public diff: {changes:?}"
    );
}

#[tokio::test]
#[serial]
async fn rfc_0067_foreign_linear_commit_diff_keeps_the_acknowledged_insert() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    write_detached_person_pin(&db, "main", "blocked-diff-first").await;
    let person_uri = node_table_uri(&db, "Person").await;
    let mut raw = helpers::open_dataset_head_exact(&person_uri, None).await;
    helpers::lance_delete_inline(&mut raw, "1 = 2").await;
    let before = helpers::snapshot_id(&db, "main").await.unwrap();
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "blocked-diff-second")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    let after = helpers::snapshot_id(&db, "main").await.unwrap();
    assert!(
        collect_column_strings(&read_table(&db, "node:Person").await, "name")
            .contains(&"blocked-diff-second".to_string())
    );
    let changes = db
        .diff_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &omnigraph::changes::ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        changes.stats.inserts, 1,
        "acknowledged chain insert absent from public diff: {changes:?}"
    );
}

#[tokio::test]
#[serial]
async fn rfc_0067_cleanup_reaps_aged_surplus_detached_manifests() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    for name in ["reap-one", "reap-two", "reap-three"] {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", 31)]),
        )
        .await
        .unwrap();
    }
    let person_uri = node_table_uri(&db, "Person").await;
    let raw = helpers::open_dataset_head_exact(&person_uri, None).await;
    let before = raw.list_detached_manifests().await.unwrap();
    assert!(before.len() >= 3);
    for entry in std::fs::read_dir(std::path::Path::new(&person_uri).join("_versions")).unwrap() {
        let path = entry.unwrap().path();
        if path.file_name().unwrap().to_string_lossy().starts_with('d') {
            std::fs::File::open(path)
                .unwrap()
                .set_times(
                    std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                )
                .unwrap();
        }
    }
    for _ in 0..2 {
        db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: Some(std::time::Duration::ZERO),
        })
        .await
        .unwrap();
    }
    let pinned = pinned_version(&db, "main", "node:Person").await;
    let remaining = raw
        .list_detached_manifests()
        .await
        .unwrap()
        .into_iter()
        .map(|manifest| manifest.version)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        remaining,
        std::collections::BTreeSet::from([pinned]),
        "two cleanups reap every aged surplus detached manifest and keep main's pin; before={}",
        before.len()
    );
}

/// Unpublished staging retains its manifest and data regardless of age: cleanup
/// has no durable evidence that the writer has stopped.
#[tokio::test]
#[serial]
async fn rfc_0067_zero_age_cleanup_keeps_a_fresh_unpublished_detached_manifest() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let versions_dir = std::path::Path::new(&person_uri).join("_versions");
    let detached_files = || -> std::collections::BTreeSet<String> {
        std::fs::read_dir(&versions_dir)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                (name.starts_with('d') && name.ends_with(".manifest")).then_some(name)
            })
            .collect()
    };

    let pre = detached_files();
    // Strand an unpublished detached manifest: the insert stages its detached
    // Person commit, then dies at the post-table-commit window before it can
    // publish a pin — exactly a parked writer's residue.
    let stranded = {
        let _fp = catalog::MUTATION_POST_TABLE_COMMIT.fail_once_at(1);
        let outcome = mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "parked")], &[("$age", 41)]),
        )
        .await;
        assert!(
            outcome.is_err(),
            "the seam must fail the write before publish"
        );
        let post = detached_files();
        &post - &pre
    };
    assert!(
        !stranded.is_empty(),
        "the interrupted write must strand at least one detached manifest"
    );

    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(std::time::Duration::ZERO),
    })
    .await
    .unwrap();
    let after = detached_files();
    assert!(
        stranded.is_subset(&after),
        "zero-age cleanup reaped a fresh unpublished detached manifest; stranded={stranded:?} after={after:?}"
    );

    // Aging cannot establish that a writer has stopped.
    for name in &stranded {
        std::fs::File::open(versions_dir.join(name))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
            .unwrap();
    }
    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(std::time::Duration::ZERO),
    })
    .await
    .unwrap();
    let aged = detached_files();
    assert!(
        stranded.is_subset(&aged),
        "an aged unpublished detached manifest must be retained; stranded={stranded:?} still={aged:?}"
    );
}

#[test]
#[ignore = "environment: OMNIGRAPH_RFC0067_CLEANUP_URI; subprocess helper"]
fn rfc_0067_external_cleanup_process() {
    let Ok(uri) = std::env::var("OMNIGRAPH_RFC0067_CLEANUP_URI") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let db = Omnigraph::open(&uri).await.unwrap();
            db.cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: Some(std::time::Duration::ZERO),
            })
            .await
            .unwrap();
        });
}

#[tokio::test]
#[serial]
async fn rfc_0067_cleanup_keeps_a_live_aged_detached_write() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let graph_uri = dir.path().to_str().unwrap().to_string();
    let versions_dir = std::path::Path::new(&person_uri).join("_versions");
    let before: std::collections::BTreeSet<_> = std::fs::read_dir(&versions_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    let data_dir = std::path::Path::new(&person_uri).join("data");
    let old_data: std::collections::BTreeSet<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    let _observe = catalog::MUTATION_POST_TABLE_COMMIT.observe(move || {
        // Age only this writer's staging, beyond Lance's unverified-file grace.
        let mut aged_data = 0;
        for file in std::fs::read_dir(&data_dir).unwrap() {
            let file = file.unwrap();
            if !old_data.contains(&file.file_name()) && file.path().is_file() {
                aged_data += 1;
                std::fs::File::open(file.path())
                    .unwrap()
                    .set_times(
                        std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                    )
                    .unwrap();
            }
        }
        assert!(aged_data > 0, "must exercise aged referenced data too");
        let mut staged = 0;
        for file in std::fs::read_dir(&versions_dir).unwrap() {
            let file = file.unwrap();
            if !before.contains(&file.file_name())
                && file.file_name().to_string_lossy().starts_with('d')
            {
                staged += 1;
                std::fs::File::open(file.path())
                    .unwrap()
                    .set_times(
                        std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
                    )
                    .unwrap();
            }
        }
        assert!(
            staged > 0,
            "the live write must have staged a new detached manifest"
        );
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "rfc_0067_external_cleanup_process",
                "--ignored",
                "--nocapture",
            ])
            .env("OMNIGRAPH_RFC0067_CLEANUP_URI", &graph_uri)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cleanup child: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
    let outcome = {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "aged-in-flight")], &[("$age", 42)]),
        )
        .await
    };
    outcome.expect("the graph write was acknowledged");
    let reopened = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let opened = snapshot.open_dataset("node:Person").await;
    assert!(
        opened.is_ok(),
        "acknowledged table is unreadable after cleanup reaped live staging: {opened:?}"
    );
    let names = collect_column_strings(&read_table(&reopened, "node:Person").await, "name");
    assert_eq!(
        names
            .iter()
            .filter(|name| name.as_str() == "aged-in-flight")
            .count(),
        1
    );
}

/// RFC 0067 ambiguity read-back: when the `__manifest` merge-insert commits
/// durably but its acknowledgement is lost, the write is already graph-visible,
/// so the publisher must recognize it as success rather than returning an
/// opaque failure (which a non-idempotent retry would double-apply). The
/// `publish.post_merge_pre_ack` seam models the lost ack of a durable commit;
/// the insert must succeed and its row must be visible and durable.
#[tokio::test]
#[serial]
async fn rfc_0067_lost_ack_of_a_durable_publish_reads_back_as_success() {
    assert_lost_ack_reads_back_as_success(false).await;
}

#[tokio::test]
#[serial]
async fn rfc_0067_lost_ack_after_another_publish_reads_back_as_success() {
    assert_lost_ack_reads_back_as_success(true).await;
}

struct AdvanceBeforeLostAck {
    uri: String,
    fired: std::sync::atomic::AtomicBool,
}
impl omnigraph::seams::Behavior for AdvanceBeforeLostAck {}
impl omnigraph::seams::Decide for AdvanceBeforeLostAck {
    fn decide(&self, _: &'static str) -> omnigraph::seams::Decision {
        if self.fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return omnigraph::seams::Decision::Pass;
        }
        run_rfc023_external_writer(
            self.uri.clone(),
            LoadMode::Merge,
            r#"{"type":"Company","data":{"name":"later-writer"}}"#.to_string(),
        )
        .unwrap();
        omnigraph::seams::Decision::Fire(omnigraph::seams::Effect::Fail)
    }
}

async fn assert_lost_ack_reads_back_as_success(advance_head: bool) {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let outcome = {
        let _guard = if advance_head {
            catalog::PUBLISH_POST_MERGE_PRE_ACK.install(Arc::new(AdvanceBeforeLostAck {
                uri: dir.path().to_str().unwrap().to_string(),
                fired: std::sync::atomic::AtomicBool::new(false),
            }))
        } else {
            catalog::PUBLISH_POST_MERGE_PRE_ACK.fail_once_at(1)
        };
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "ack-overtaken")], &[("$age", 42)]),
        )
        .await
    };
    let reopened = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let names = collect_column_strings(&read_table(&reopened, "node:Person").await, "name");
    assert!(
        names.contains(&"ack-overtaken".to_string()),
        "first commit must be durable"
    );
    let companies = collect_column_strings(&read_table(&reopened, "node:Company").await, "name");
    assert_eq!(
        companies.contains(&"later-writer".to_string()),
        advance_head,
        "external-writer schedule must be exercised"
    );
    assert!(
        outcome.is_ok(),
        "durable write was misreported after another writer advanced HEAD: {outcome:?}"
    );
    assert_eq!(
        names
            .iter()
            .filter(|name| name.as_str() == "ack-overtaken")
            .count(),
        1
    );
    assert_eq!(count_rows(&db, "node:Person").await, names.len());
    assert_eq!(count_rows(&db, "node:Company").await, companies.len());
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "same-handle-next")], &[("$age", 43)]),
    )
    .await
    .unwrap();
    let fresh = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, names.len() + 1);
    assert_eq!(count_rows(&fresh, "node:Company").await, companies.len());
}

#[tokio::test]
#[serial]
async fn rfc_0067_unavailable_ack_readback_is_indeterminate_without_replay() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = count_rows(&db, "node:Person").await;
    let outcome = {
        let _ack = catalog::PUBLISH_POST_MERGE_PRE_ACK.fail_once_at(1);
        let _read = catalog::PUBLISH_READ_BACK.fail_once_at(1);
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "indeterminate")], &[("$age", 42)]),
        )
        .await
    };
    let error = outcome.unwrap_err().to_string();
    assert!(error.contains("outcome is in doubt"), "{error}");
    let fresh = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before + 1,
        "no automatic replay"
    );
    assert_eq!(count_rows(&db, "node:Person").await, before + 1);
}

/// GENERALIZED LIVENESS, persistent pre-publish faults: the same live handle
/// keeps writing once faults stop, without reopening. A persistent seam
/// failure before the effect gates fails every write with nothing durable;
/// dropping the guard models the fault source stopping, and the very next
/// write on the SAME handle must succeed. This replaces the old
/// RecoveryRequired-specific check: the wedge class it watched is gone, but
/// the liveness contract it enforced is general.
#[tokio::test]
#[serial]
async fn live_handle_keeps_writing_after_persistent_prepublish_faults_stop() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = count_rows(&db, "node:Person").await;

    {
        let _fault = catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE.fire_always();
        for attempt in 0..2 {
            let outcome = mutate_main(
                &db,
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(
                    &[("$name", format!("dead{attempt}").as_str())],
                    &[("$age", 20)],
                ),
            )
            .await;
            assert!(
                outcome.is_err(),
                "attempt {attempt} must fail under the fault"
            );
        }
    }
    assert_eq!(
        count_rows(&db, "node:Person").await,
        before,
        "pre-publish failures leave nothing visible"
    );

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "revive")], &[("$age", 21)]),
    )
    .await
    .expect("the same handle must write again once faults stop, without reopening");
    assert_eq!(count_rows(&db, "node:Person").await, before + 1);

    let fresh = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before + 1,
        "a fresh handle agrees with the never-reopened one"
    );
}

/// Persistent lost acknowledgements: every write's manifest commit lands but
/// the caller sees an error; once the faults stop the SAME handle writes
/// again, and every acknowledged-then-denied row is present exactly once.
#[tokio::test]
#[serial]
async fn live_handle_keeps_writing_after_persistent_ack_loss_stops() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before = count_rows(&db, "node:Person").await;

    {
        let _fault = catalog::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT.fire_always();
        for attempt in 0..2 {
            let outcome = mutate_main(
                &db,
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(
                    &[("$name", format!("denied{attempt}").as_str())],
                    &[("$age", 30)],
                ),
            )
            .await;
            assert!(
                outcome.is_err(),
                "attempt {attempt} must report the lost acknowledgement"
            );
        }
    }

    mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "revive_ack")], &[("$age", 31)]),
    )
    .await
    .expect("the same handle must write again once faults stop, without reopening");

    // Both denied writes were durably published (an acknowledged write is
    // never lost) and nothing double-applied.
    let names = collect_column_strings(&read_table(&db, "node:Person").await, "name");
    for expected in ["denied0", "denied1", "revive_ack"] {
        assert_eq!(
            names.iter().filter(|name| *name == expected).count(),
            1,
            "{expected} must be present exactly once"
        );
    }
    assert_eq!(count_rows(&db, "node:Person").await, before + 3);
    let fresh = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    assert_eq!(
        count_rows(&fresh, "node:Person").await,
        before + 3,
        "a fresh handle agrees with the never-reopened one"
    );
}

/// GENERALIZED LIVENESS, the write-family sweep: for every write-path seam
/// family the failure-window matrix does not already cover with its
/// same-handle actor, fail the family's driver once at the seam on a live
/// handle, then prove the SAME handle's next ordinary write succeeds —
/// without reopening. The driver's own outcome is recorded, not asserted
/// (some seams absorb, some drivers may not reach an armed seam); the
/// liveness insert is the contract.
#[tokio::test]
#[serial]
async fn live_handle_writes_after_every_write_family_seam_failure() {
    #[derive(Clone, Copy, Debug)]
    enum Driver {
        Insert,
        DeletePerson,
        NoEffectUpdate,
        BranchCreate,
        BranchDelete,
        BranchMutate,
        LoadFork,
        LoadTwoTables,
        Merge,
        SchemaApply,
        EnsureIndices,
        Optimize,
        Cleanup,
        Refresh,
    }
    // Matrix-covered families (mutation/load/merge/schema/optimize/indices
    // windows of the ten writers) are deliberately absent; these are the
    // remaining write-path seams. init.*/open.* have no live handle by
    // definition; blob/change-feed read seams cannot arm write state.
    let cells: &[(&str, Driver)] = &[
        ("branch_control.pre_gates", Driver::BranchCreate),
        ("branch_create.post_native", Driver::BranchCreate),
        ("branch_delete.post_table_gates", Driver::BranchDelete),
        ("branch_delete.post_native", Driver::BranchDelete),
        ("branch_delete.post_archive", Driver::BranchDelete),
        ("load.post_branch_create_pre_stage", Driver::LoadFork),
        ("load.between_table_stages", Driver::LoadTwoTables),
        ("fork.before_classify", Driver::BranchMutate),
        ("classify.fresh_read", Driver::BranchMutate),
        (
            "mutation.delete_node_pre_primary_delete",
            Driver::DeletePerson,
        ),
        ("mutation.post_no_effect_pre_gate", Driver::NoEffectUpdate),
        ("graph_publish.before_commit_append", Driver::Insert),
        ("graph_publish.after_manifest_commit", Driver::Insert),
        ("publish.post_merge_pre_ack", Driver::Insert),
        ("read.refresh_post_state_pre_lineage", Driver::Refresh),
        ("optimize.before_compact", Driver::Optimize),
        (
            "optimize.post_authority_capture_pre_gates",
            Driver::Optimize,
        ),
        ("cleanup.pre_gates", Driver::Cleanup),
        ("cleanup.resolve_branch_snapshot", Driver::Cleanup),
        ("cleanup.reconcile_fork", Driver::Cleanup),
        ("cleanup.table_gc", Driver::Cleanup),
        (
            catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE.name(),
            Driver::Merge,
        ),
        (
            catalog::BRANCH_MERGE_POST_CANDIDATE_VALIDATION.name(),
            Driver::Merge,
        ),
        ("schema_apply.post_lock_pre_effect", Driver::SchemaApply),
        ("schema_apply.before_staging_write", Driver::SchemaApply),
        ("schema_apply.after_manifest_commit", Driver::SchemaApply),
        (
            "ensure_indices.post_stage_pre_commit_btree",
            Driver::EnsureIndices,
        ),
    ];

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // Cumulative schema state: apply rows only ever add optional properties
    // (and the one @index the ensure row needs), so every apply is additive
    // over the last.
    let mut schema_props: Vec<String> = Vec::new();
    let mut age_indexed = false;
    let render_schema = |props: &[String], indexed: bool| {
        let age = if indexed {
            "age: I32? @index"
        } else {
            "age: I32?"
        };
        let mut schema = helpers::TEST_SCHEMA.replace("age: I32?", age);
        if !props.is_empty() {
            schema = schema.replace(
                "    age: I32?",
                &format!("    age: I32?\n    {}", props.join("\n    ")),
            );
        }
        schema
    };

    for (index, (seam, driver)) in cells.iter().enumerate() {
        // Schema rows need the engine's mono-branch restriction satisfied:
        // earlier rows deliberately leave branches behind (including a failed
        // load's surviving fork), so clear every non-main branch first, on
        // the same live handle, before arming.
        if matches!(driver, Driver::SchemaApply | Driver::EnsureIndices) {
            for branch in db.branch_list().await.expect("sweep branch listing") {
                if branch != "main" {
                    db.branch_delete(&branch)
                        .await
                        .unwrap_or_else(|error| panic!("sweep delete of {branch}: {error}"));
                }
            }
        }
        let armed = catalog::decide(seam)
            .unwrap_or_else(|| panic!("sweep names unknown seam {seam}"))
            .fail_once_at(1);
        let outcome: std::result::Result<(), String> = match driver {
            Driver::Insert => mutate_main(
                &db,
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", format!("drv{index}").as_str())], &[("$age", 40)]),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
            Driver::DeletePerson => mutate_main(
                &db,
                MUTATION_QUERIES,
                "remove_person",
                &params(&[("$name", "live_0")]),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
            Driver::NoEffectUpdate => mutate_main(
                &db,
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "nobody-here")], &[("$age", 1)]),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
            Driver::BranchCreate => db
                .branch_create(&format!("lv_b{index}"))
                .await
                .map_err(|error| error.to_string()),
            Driver::BranchDelete => {
                let name = format!("lv_d{index}");
                db.branch_create(&name).await.expect("delete-row setup");
                db.branch_delete(&name)
                    .await
                    .map_err(|error| error.to_string())
            }
            Driver::BranchMutate => {
                let name = format!("lv_m{index}");
                db.branch_create(&name).await.expect("mutate-row setup");
                db.mutate(
                    &name,
                    MUTATION_QUERIES,
                    "insert_person",
                    &mixed_params(
                        &[("$name", format!("br{index}").as_str())],
                        &[("$age", 41)],
                    ),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
            }
            Driver::LoadFork => db
                .load_as(
                    &format!("lv_l{index}"),
                    Some("main"),
                    &format!(
                        "{{\"type\": \"Person\", \"data\": {{\"name\": \"lf{index}\", \"age\": 42}}}}"
                    ),
                    LoadMode::Merge,
                    None,
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Driver::LoadTwoTables => db
                .load_jsonl(
                    &format!(
                        "{{\"type\": \"Person\", \"data\": {{\"name\": \"lt{index}\", \"age\": 43}}}}\n\
                         {{\"type\": \"Company\", \"data\": {{\"name\": \"lc{index}\"}}}}"
                    ),
                    LoadMode::Merge,
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Driver::Merge => {
                let name = format!("lv_s{index}");
                db.branch_create(&name).await.expect("merge-row setup");
                db.mutate(
                    &name,
                    MUTATION_QUERIES,
                    "insert_person",
                    &mixed_params(
                        &[("$name", format!("mg{index}").as_str())],
                        &[("$age", 44)],
                    ),
                )
                .await
                .expect("merge-row source write");
                Box::pin(db.branch_merge(&name, "main"))
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
            Driver::SchemaApply => {
                schema_props.push(format!("liveprop{index}: I64?"));
                db.apply_schema(&render_schema(&schema_props, age_indexed))
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
            Driver::EnsureIndices => {
                age_indexed = true;
                db.apply_schema(&render_schema(&schema_props, age_indexed))
                    .await
                    .expect("ensure-row schema setup");
                db.ensure_indices()
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
            Driver::Optimize => db
                .optimize()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Driver::Cleanup => Box::pin(db.cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: None,
            }))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
            Driver::Refresh => db.refresh().await.map_err(|error| error.to_string()),
        };
        drop(armed);
        println!(
            "liveness sweep [{seam} driver={driver:?}]: {}",
            match &outcome {
                Ok(()) => "driver absorbed or seam unreached".to_string(),
                Err(error) => format!("driver failed: {}", error.lines().next().unwrap_or("")),
            }
        );

        // The contract: whatever the fault did, the same live handle's next
        // ordinary write succeeds without reopening.
        let live = format!("live_{index}");
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", live.as_str())], &[("$age", 45)]),
        )
        .await
        .unwrap_or_else(|error| panic!("liveness violated after {seam} ({driver:?}): {error}"));
        let names = collect_column_strings(&read_table(&db, "node:Person").await, "name");
        assert!(
            names.contains(&live),
            "liveness write after {seam} must be visible on the same handle"
        );
    }

    let fresh = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let mut same: Vec<String> =
        collect_column_strings(&read_table(&db, "node:Person").await, "name");
    let mut other: Vec<String> =
        collect_column_strings(&read_table(&fresh, "node:Person").await, "name");
    same.sort();
    other.sort();
    assert_eq!(
        same, other,
        "fresh-handle view diverged from the live handle"
    );
}

#[tokio::test]
#[serial]
async fn rfc_0067_merge_diff_keeps_every_chunk() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (uri, _, _) = setup_branch_merge_multichunk_adopt(&dir).await;
    let db = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let before = helpers::snapshot_id(&db, "main").await.unwrap();
    let before_rows = count_rows(&db, "node:Person").await;
    {
        db.branch_merge("feature", "main").await.unwrap();
    }
    let after = helpers::snapshot_id(&db, "main").await.unwrap();
    let after_rows = count_rows(&db, "node:Person").await;
    let changes = db
        .diff_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &omnigraph::changes::ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        changes.stats.inserts as usize,
        after_rows - before_rows,
        "diff omitted acknowledged merge chunks: {} inserts for {} added rows",
        changes.stats.inserts,
        after_rows - before_rows
    );
    db.branch_delete("feature").await.unwrap();
    db.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: Some(std::time::Duration::ZERO),
    })
    .await
    .unwrap();
    let person_uri = node_table_uri(&db, "Person").await;
    let pinned = pinned_version(&db, "main", "node:Person").await;
    assert_eq!(
        detached_versions(&person_uri).await,
        std::collections::BTreeSet::from([pinned]),
        "once the source branch is gone every chain intermediate is reclaimed; main's pin stays"
    );
    let fresh = Omnigraph::open_read_only(&uri).await.unwrap();
    assert_eq!(count_rows(&fresh, "node:Person").await, after_rows);
}

#[tokio::test]
#[serial]
async fn rfc_0067_cleanup_window_keeps_version_gc_on_a_written_table() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    for name in ["window-one", "window-two", "window-three"] {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", 31)]),
        )
        .await
        .unwrap();
    }
    let person_uri = node_table_uri(&db, "Person").await;
    let raw = helpers::open_dataset_head_exact(&person_uri, None).await;
    let detached_before = raw.list_detached_manifests().await.unwrap().len();
    assert!(
        detached_before >= 3,
        "every write leaves its detached version"
    );

    for pass in 0..2 {
        let stats = db
            .cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: Some(std::time::Duration::from_secs(7 * 24 * 60 * 60)),
            })
            .await
            .unwrap();
        let person = stats
            .iter()
            .find(|row| row.type_key == "node:Person")
            .unwrap();
        assert!(
            person.error.is_none(),
            "pass {pass}: proven copies inside the age window must not defer version GC: {person:?}"
        );
    }

    let detached_after = raw.list_detached_manifests().await.unwrap().len();
    assert_eq!(
        detached_after, detached_before,
        "young proven copies are kept, not reaped"
    );
    let fresh = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let names = collect_column_strings(&read_table(&fresh, "node:Person").await, "name");
    for name in ["window-one", "window-two", "window-three"] {
        assert!(
            names.contains(&name.to_string()),
            "{name} must survive cleanup"
        );
    }
}

/// Detached-only collector, fixture 2 of its `blocked_on` 2: an unpublished
/// staging left by the mutation's pre-publication window is live while the
/// branch's head stands and dead once it moves.
#[tokio::test]
#[serial]
async fn collector_keeps_live_staging_and_reclaims_it_once_the_head_moves() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = detached_versions(&person_uri).await;
    {
        let _fp = catalog::MUTATION_POST_TABLE_COMMIT.fail_once_at(1);
        let outcome = mutate_main(
            &db,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "parked")], &[("$age", 30)]),
        )
        .await;
        assert!(outcome.is_err(), "the seam fails the write before publish");
    }
    let stranded = &detached_versions(&person_uri).await - &before;
    assert!(
        !stranded.is_empty(),
        "the interrupted write strands its staging"
    );

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    for version in &stranded {
        assert!(!plan.published_set.contains(version));
        let staging = plan
            .unpublished
            .iter()
            .find(|manifest| manifest.version == *version)
            .unwrap_or_else(|| panic!("{version} is unpublished staging: {:?}", plan.unpublished));
        assert_eq!(
            staging.verdict,
            StagingVerdict::Live,
            "its recorded head is the branch's head"
        );
        assert!(staging.bytes > 0);
    }
    let stats = db.cleanup(keep_one()).await.unwrap();
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert_eq!(
        row.unpublished_manifests as usize,
        stranded.len(),
        "{row:?}"
    );
    assert!(row.unpublished_bytes > 0, "{row:?}");

    insert_person(&db, "main", "after").await;
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    for version in &stranded {
        let staging = plan
            .unpublished
            .iter()
            .find(|manifest| manifest.version == *version)
            .unwrap();
        assert!(
            matches!(staging.verdict, StagingVerdict::Dead(_)),
            "the head moved past the recorded one: {:?}",
            staging.verdict
        );
    }
    let stats = db.cleanup(keep_one()).await.unwrap();
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert!(row.error.is_none(), "{row:?}");
    assert!(
        row.manifests_removed >= stranded.len() as u64,
        "the dead staging is reclaimed with the pruned pins: {row:?}"
    );
    let remaining = detached_versions(&person_uri).await;
    for version in &stranded {
        assert!(
            !remaining.contains(version),
            "the dead staging {version} is gone: {remaining:?}"
        );
    }
}

/// A publication between the graph cut and final validation refuses cleanup;
/// a fresh run retains the published pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn collector_refuses_a_publication_during_capture_and_retries_with_new_roots() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let person_uri = node_table_uri(&db, "Person").await;
    let before = detached_versions(&person_uri).await;

    let (guard, hold) = catalog::CLEANUP_COLLECTOR_POST_SNAPSHOT.hold();
    let planner = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
    let plan = tokio::spawn(async move { planner.cleanup_plan(keep_one()).await });
    let reached = hold.clone();
    tokio::task::spawn_blocking(move || reached.wait_until_reached())
        .await
        .unwrap();
    insert_person(&db, "main", "late").await;
    let late = staged_since(&person_uri, &before).await;
    hold.release();
    let error = plan.await.unwrap().unwrap_err();
    drop(guard);
    assert!(!hold.timed_out(), "the hold was released, not timed out");
    assert!(matches!(error, OmniError::Manifest(_)), "{error:?}");
    assert!(
        error.to_string().contains("changed during capture"),
        "{error}"
    );
    assert!(
        detached_versions(&person_uri).await.contains(&late),
        "a rejected capture must retain the late publication"
    );

    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(
        plan.roots.contains(&late),
        "the next run pins it: {:?}",
        plan.roots
    );
    assert!(
        plan.unpublished
            .iter()
            .all(|manifest| manifest.version != late)
    );
}

/// Changed publication cuts refuse sweeping; a late retired branch's failed
/// staging is reclaimable when no live publication changed the original cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn collector_keeps_pins_published_by_a_late_branch_in_another_process() {
    for action in [
        "collector_late_live",
        "collector_late_retired",
        "collector_late_stranded",
    ] {
        let _scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let db = init_and_load(&dir).await;
        let person_uri = node_table_uri(&db, "Person").await;
        let before = detached_versions(&person_uri).await;
        let data_files = || {
            std::fs::read_dir(std::path::Path::new(&person_uri).join("data"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let data_before = data_files();
        let expected_rows = count_rows(&db, "node:Person").await + 1;
        let cleaner = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
        let (guard, hold) = catalog::CLEANUP_COLLECTOR_POST_SNAPSHOT.hold();
        let cleanup = tokio::spawn(async move { cleaner.cleanup(keep_one()).await });
        let reached = hold.clone();
        tokio::task::spawn_blocking(move || reached.wait_until_reached())
            .await
            .unwrap();
        let uri = db.uri().to_string();
        let child = tokio::task::spawn_blocking(move || {
            run_rfc023_external_writer_action(
                uri,
                LoadMode::Append,
                r#"{"type":"Person","data":{"name":"collector-late","age":40}}"#.to_string(),
                action,
            )
        })
        .await
        .unwrap();
        let added = &detached_versions(&person_uri).await - &before;
        let added_data = &data_files() - &data_before;
        hold.release();
        let result = cleanup.await.unwrap();
        drop(guard);
        child.unwrap();
        assert!(
            !hold.timed_out(),
            "the child finished before releasing cleanup"
        );
        let reader = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
        if action == "collector_late_stranded" {
            let rows = result.unwrap();
            assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
            assert_eq!(added.len(), 1, "one failed write must leave staging");
            assert!(
                added.is_disjoint(&detached_versions(&person_uri).await),
                "post-inventory revocation can reclaim staging when the graph cut is unchanged"
            );
            assert!(!added_data.is_empty());
            for path in added_data {
                assert!(
                    !path.exists(),
                    "the revoked staging data must be reclaimed: {}",
                    path.display()
                );
            }
            let retry = reader.cleanup(keep_one()).await.unwrap();
            assert!(retry.iter().all(|row| row.error.is_none()), "{retry:?}");
            assert_eq!(count_rows(&reader, "node:Person").await, expected_rows - 1);
            continue;
        }
        let error = result.expect_err("a changed graph cut must refuse cleanup before sweeping");
        assert!(
            matches!(error, OmniError::Manifest(ref details) if details.kind == ManifestErrorKind::Conflict),
            "{error}"
        );
        assert!(added.is_subset(&detached_versions(&person_uri).await));
        assert!(added_data.iter().all(|path| path.exists()));
        let branch = if action == "collector_late_live" {
            "collector-late"
        } else {
            "main"
        };
        let pin = pinned_version(&reader, branch, "node:Person").await;
        assert!(
            detached_versions(&person_uri).await.contains(&pin),
            "{action}: cleanup deleted the current published pin {pin}"
        );
        assert_eq!(
            helpers::count_rows_branch(&reader, branch, "node:Person").await,
            expected_rows,
            "{action}: a fresh reader sees the acknowledged row"
        );
    }
}

/// Revalidation refuses a cut whose later branch imports a publication that
/// the earlier branch capture could not observe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn collector_refuses_mixed_branch_captures_before_deleting() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("collector-existing").await.unwrap();
    let person_uri = node_table_uri(&db, "Person").await;
    let expected_rows = count_rows(&db, "node:Person").await + 1;
    let cleaner = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
    let (guard, hold) = catalog::CLEANUP_COLLECTOR_MID_BRANCH_READ.hold();
    let cleanup = tokio::spawn(async move { cleaner.cleanup(keep_one()).await });
    let reached = hold.clone();
    tokio::task::spawn_blocking(move || reached.wait_until_reached())
        .await
        .unwrap();
    let uri = db.uri().to_string();
    let child = tokio::task::spawn_blocking(move || {
        run_rfc023_external_writer_action(
            uri,
            LoadMode::Append,
            r#"{"type":"Person","data":{"name":"collector-main","age":40}}"#.to_string(),
            "collector_mixed",
        )
    })
    .await
    .unwrap();
    hold.release();
    let result = cleanup.await.unwrap();
    drop(guard);
    child.unwrap();
    assert!(
        !hold.timed_out(),
        "the child finished before releasing cleanup"
    );
    let error = result.expect_err("a mixed capture must fail before sweeping");
    assert!(error.to_string().contains("collector branch"), "{error}");
    let reader = helpers::session(Omnigraph::open(db.uri()).await.unwrap());
    let pin = pinned_version(&reader, "main", "node:Person").await;
    assert!(detached_versions(&person_uri).await.contains(&pin));
    assert_eq!(count_rows(&reader, "node:Person").await, expected_rows);
    let retry = reader.cleanup(keep_one()).await.unwrap();
    assert!(retry.iter().all(|row| row.error.is_none()), "{retry:?}");
    assert!(detached_versions(&person_uri).await.contains(&pin));
}

/// Detached-only collector, fixture 7: a pass interrupted inside its sweep
/// reports the window and counts what it deleted, and the retry sweeps the
/// rest; report-only, the interrupted plan counts nothing and the retry replans.
#[tokio::test]
#[serial]
async fn collector_pass_interrupted_mid_sweep_counts_nothing_and_retries_whole() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (db, person_uri, chain) = merge_three_chunk_chain(&dir).await;

    let stats = {
        let _fp = catalog::CLEANUP_SWEEP_BETWEEN_MANIFESTS.fail_once_at(1);
        db.cleanup(keep_one()).await.unwrap()
    };
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert!(
        row.error
            .as_deref()
            .is_some_and(|error| error.contains("cleanup.sweep_between_manifests")),
        "the interrupted pass reports the window: {row:?}"
    );
    assert_eq!(
        row.manifests_removed, 1,
        "the pass stopped after its first manifest: {row:?}"
    );
    let stats = db.cleanup(keep_one()).await.unwrap();
    let row = stats
        .iter()
        .find(|row| row.type_key == "node:Person")
        .unwrap();
    assert!(row.error.is_none(), "{row:?}");
    assert_eq!(
        row.manifests_removed, 2,
        "the retry sweeps what the first pass left of the two links and main's pruned base pin: {row:?}"
    );
    let remaining = detached_versions(&person_uri).await;
    for link in &chain[..2] {
        assert!(!remaining.contains(link), "{link} is gone: {remaining:?}");
    }
    assert!(remaining.contains(&chain[2]), "the pinned tip stays");
    let report = db.cleanup_plan(keep_one()).await.unwrap();
    let plan = main_plan(&report, "node:Person");
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert!(plan.would_remove().is_empty(), "{:?}", plan.would_remove());
    assert_eq!(plan.roots, std::collections::BTreeSet::from([chain[2]]));
}

/// A failed publish acknowledgement and unavailable readback leave publication
/// indeterminate. Its input protection survives the returned error until cleanup
/// observes the target publication and can safely expire the old witness.
#[tokio::test]
#[serial]
async fn branch_merge_in_doubt_keeps_input_tags_until_cleanup() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    db.load(
        "source",
        r#"{"type":"Person","data":{"name":"merge-indeterminate","age":42}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let source_pin = pinned_version(&db, "source", "node:Person").await;
    let source_rows = helpers::count_rows_branch(&db, "source", "node:Person").await;
    let native = helpers::open_dataset_head_exact(&format!("{uri}/__manifest"), None).await;
    let outcome = {
        let _ack = catalog::PUBLISH_POST_MERGE_PRE_ACK.fail_once_at(1);
        let _read = catalog::PUBLISH_READ_BACK.fail_once_at(1);
        db.branch_merge("source", "target").await
    };
    let error = outcome.unwrap_err();
    assert!(
        matches!(&error, OmniError::Manifest(manifest) if manifest.kind == ManifestErrorKind::Internal),
        "unknown publication remains an internal outcome: {error:?}"
    );
    assert!(error.to_string().contains("outcome is in doubt"), "{error}");
    let protected = native.tags().list().await.unwrap();
    assert_eq!(
        protected
            .keys()
            .filter(|name| name.starts_with("__omnigraph_merge_input_v1_"))
            .count(),
        2,
        "the acknowledged source and target guards must survive an in-doubt fast-forward"
    );

    let rows = db.cleanup(keep_one()).await.unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert!(
        !native
            .tags()
            .list()
            .await
            .unwrap()
            .keys()
            .any(|name| name.starts_with("__omnigraph_merge_input_v1_")),
        "cleanup observes the landed publication and releases its expired input guards"
    );
    let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_eq!(
        pinned_version(&reopened, "target", "node:Person").await,
        source_pin
    );
    assert_eq!(
        helpers::count_rows_branch(&reopened, "target", "node:Person").await,
        source_rows,
        "the landed merge remains readable without replay"
    );
}

/// Cancellation preserves accepted inputs until the target's exact publication
/// witness changes. A later cleanup then releases the abandoned protection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn abandoned_merge_input_tags_expire_after_target_advances() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(
        Omnigraph::init(
            &uri,
            "node Person {\n    name: String @key\n    age: I32\n}",
        )
        .await
        .unwrap(),
    );
    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"p","age":0}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    for age in [1, 2] {
        db.load(
            "source",
            &format!(r#"{{"type":"Person","data":{{"name":"p","age":{age}}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }
    let captured = pinned_version(&db, "source", "node:Person").await;
    let person_uri = node_table_uri(&db, "Person").await;
    let captured_path =
        std::path::Path::new(&person_uri).join(format!("_versions/d{captured}.manifest"));
    let merger = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let parked =
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_MERGE_POST_AUTHORITY_CAPTURE);
    let merge = tokio::spawn(async move { merger.branch_merge("source", "target").await });
    parked.wait_until_reached().await;
    let native = helpers::open_dataset_head_exact(&format!("{uri}/__manifest"), None).await;
    let tags = native.tags().list().await.unwrap();
    assert!(
        tags.keys()
            .any(|name| name.starts_with("__omnigraph_merge_input_v1_")),
        "accepted merge must have durable input protection"
    );
    merge.abort();
    parked.release();
    assert!(merge.await.unwrap_err().is_cancelled());
    db.load(
        "source",
        r#"{"type":"Person","data":{"name":"p","age":3}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let rows = db.cleanup(keep_one()).await.unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert!(
        captured_path.is_file(),
        "unchanged target witness retains cancelled merge inputs"
    );
    db.load(
        "target",
        r#"{"type":"Person","data":{"name":"q","age":4}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let rows = db.cleanup(keep_one()).await.unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert!(
        !captured_path.exists(),
        "revoked target witness releases the old source pin"
    );
    assert!(
        !native
            .tags()
            .list()
            .await
            .unwrap()
            .keys()
            .any(|name| name.starts_with("__omnigraph_merge_input_v1_")),
        "cleanup removes only the expired merge protection tags"
    );
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        helpers::count_rows_branch(&reopened, "target", "node:Person").await,
        2
    );
}

/// Manifest trees first observed after a collector's root cut belong to the
/// next pass, including retired providers of newly published merged parents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cleanup_keeps_a_late_retired_merge_base_provider() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = helpers::session(
        Omnigraph::init(
            &uri,
            "node Person {\n    name: String @key\n    age: I32\n}",
        )
        .await
        .unwrap(),
    );
    for name in ["p", "q"] {
        db.load(
            "main",
            &format!(r#"{{"type":"Person","data":{{"name":"{name}","age":0}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }
    db.branch_create("source").await.unwrap();
    db.branch_create("target").await.unwrap();
    let cleaner = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let parked = helpers::failpoint::Rendezvous::park_first(&catalog::CLEANUP_TABLE_GC);
    let cleanup = tokio::spawn(async move { cleaner.cleanup(keep_one()).await });
    parked.wait_until_reached().await;
    let child_uri = uri.clone();
    let child = tokio::task::spawn_blocking(move || {
        run_rfc023_external_writer_action(
            child_uri,
            LoadMode::Merge,
            r#"{"type":"Person","data":{"name":"p","age":1}}"#.to_string(),
            "collector_late_base_carrier",
        )
    })
    .await;
    parked.release();
    let rows = cleanup.await.unwrap().unwrap();
    child.unwrap().unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    for (branch, name) in [("source", "p"), ("target", "q")] {
        reopened
            .load(
                branch,
                &format!(r#"{{"type":"Person","data":{{"name":"{name}","age":2}}}}"#),
                LoadMode::Merge,
            )
            .await
            .unwrap();
    }
    let merger = helpers::session(Omnigraph::open(&uri).await.unwrap());
    assert_eq!(
        merger.branch_merge("source", "target").await.unwrap(),
        omnigraph::db::MergeOutcome::Merged
    );
    let snapshot = helpers::snapshot_branch(&merger, "target").await.unwrap();
    let person = snapshot.open_dataset("node:Person").await.unwrap();
    assert_eq!(
        person
            .count_rows(Some("age = 2".to_string()))
            .await
            .unwrap(),
        2
    );
}
