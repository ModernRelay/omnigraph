//! RFC 0040 Rollout step 3: the explicit system-column upgrade of a
//! legacy-vintage graph, its preflight refusals, and its roll-forward-only
//! recovery at every effect boundary.
#![cfg(feature = "failpoints")]

mod helpers;

use std::fs;

use helpers::recovery::{recovery_audit_kinds, sidecar_operation_ids};
use helpers::*;
use omnigraph::db::{
    Omnigraph, ReadTarget, SnapshotDataset, SnapshotId, SystemColumnUpgradeOptions,
    SystemColumnUpgradeOutcome,
};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::seams::{DecideSeam, FailScenario, catalog};
use omnigraph_compiler::ir::ParamMap;

const LEGACY_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
    @unique(src, dst)
}
"#;

const LEGACY_DATA: &str = r#"{"type":"Person","data":{"id":"Alice","name":"Alice","age":30}}
{"type":"Person","data":{"id":"Bob","name":"Bob","age":25}}
{"type":"Company","data":{"id":"company-1","name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"company-1","data":{"id":"works-alice","title":"engineer"}}
{"edge":"WorksAt","id":"works-bob","from":"Bob","to":"company-1","data":{}}"#;

/// The promoted source plus a user property only the upgraded namespace
/// admits (RFC 0040): a legacy accept refuses `Person.id`.
const UPGRADED_SCHEMA_WITH_ID_PROPERTY: &str = r#"
node Person {
    name: String @key
    age: I32?
    id: String?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
    @unique(@src, @dst)
}
"#;

const COMPANY_QUERY: &str =
    "query company_identity() { match { $c: Company } return { $c.@id, $c.name } }";
const OLD_PEOPLE_QUERY: &str =
    "query old_people() { match { $p: Person } return { $p.@id, $p.name } order { $p.@id asc } }";
const OLD_COWORKERS_QUERY: &str = "query old_coworkers() { match { $p: Person\n $p worksat $c } return { $p.@id, $c.name } order { $p.@id asc } }";

async fn legacy_graph_with_data(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init_with_legacy_system_columns_for_tests(uri, LEGACY_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&db, LEGACY_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

fn schema_ir(dir: &tempfile::TempDir) -> serde_json::Value {
    serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap()).unwrap()
}

/// The lock is a native `__manifest` branch: its ref file is
/// `__schema_apply_lock__.<ULID>.json` under the dataset's branch refs, and
/// a deleted branch keeps that file with a retirement marker inside.
fn schema_apply_lock_present(dir: &tempfile::TempDir) -> bool {
    let refs = dir.path().join("__manifest").join("_refs").join("branches");
    fs::read_dir(&refs)
        .map(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("__schema_apply_lock__")
                    && !fs::read_to_string(entry.path())
                        .unwrap()
                        .contains("omnigraph.retired_manifest_branch")
            })
        })
        .unwrap_or_else(|error| panic!("{}: {error}", refs.display()))
}

async fn assert_history_readable(
    db: &Omnigraph,
    version_before: u64,
    snapshot_before: &SnapshotId,
) {
    let historical = db
        .run_query_at(
            version_before,
            OLD_PEOPLE_QUERY,
            "old_people",
            &ParamMap::new(),
        )
        .await
        .expect("a GQ query at the pre-upgrade version plans against the image's own spellings");
    assert_eq!(
        collect_column_strings(historical.batches(), "p.@id"),
        ["Alice", "Bob"]
    );
    let at_snapshot = db
        .query(
            ReadTarget::snapshot(snapshot_before.clone()),
            OLD_COWORKERS_QUERY,
            "old_coworkers",
            &ParamMap::new(),
        )
        .await
        .expect("a GQ query at the pre-upgrade SnapshotId plans against the image's own spellings");
    assert_eq!(
        collect_column_strings(at_snapshot.batches(), "p.@id"),
        ["Alice", "Bob"]
    );
    assert_eq!(
        collect_column_strings(at_snapshot.batches(), "c.name"),
        ["Acme", "Acme"]
    );
}

fn primary_key_of(ds: &SnapshotDataset) -> Vec<String> {
    ds.schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

async fn assert_upgraded(db: &mut Omnigraph, dir: &tempfile::TempDir, expected_export: &str) {
    assert_eq!(
        db.internal_schema_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        10
    );
    let ir = schema_ir(dir);
    assert_eq!(ir["ir_version"].as_u64(), Some(5));
    assert!(
        ir["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|feature| feature == "system-columns"),
        "the promoted IR must record the current vintage: {ir}"
    );
    let source = fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    assert!(
        source.contains("@unique(@src, @dst)"),
        "the promoted source must spell its endpoints as meta-fields: {source}"
    );
    assert!(!source.contains("@unique(src, dst)"));
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    for staging in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        assert!(
            !dir.path().join(staging).exists(),
            "{staging} must be promoted"
        );
    }

    let snap = snapshot_main(db).await.unwrap();
    for table_key in ["node:Person", "node:Company", "edge:WorksAt"] {
        let dataset = snap.open_dataset(table_key).await.unwrap();
        assert_eq!(primary_key_of(&dataset), ["__id"], "{table_key}");
        assert!(dataset.schema().field("id").is_none(), "{table_key}");
        let id_field = dataset.schema().field("__id").unwrap().id;
        let indices = dataset.load_indices().await.unwrap();
        assert!(
            indices.iter().any(|index| index.fields.contains(&id_field)),
            "{table_key} keeps its primary-key index across the rename by field id"
        );
    }
    let works_at = snap.open_dataset("edge:WorksAt").await.unwrap();
    assert!(works_at.schema().field("__src").is_some());
    assert!(works_at.schema().field("__dst").is_some());
    assert!(works_at.schema().field("src").is_none());
    assert!(works_at.schema().field("dst").is_none());

    assert_eq!(count_rows(db, "node:Person").await, 2);
    assert_eq!(count_rows(db, "node:Company").await, 1);
    assert_eq!(count_rows(db, "edge:WorksAt").await, 2);
    let result = query_main(db, COMPANY_QUERY, "company_identity", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(result.batches(), "c.@id"),
        ["company-1"]
    );
    let entity = db
        .entity_at_target(
            omnigraph::db::ReadTarget::branch("main"),
            "edge:WorksAt",
            "works-alice",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entity["@src"], "Alice");
    assert_eq!(entity["@dst"], "company-1");
    assert_eq!(db.export_jsonl("main", &[]).await.unwrap(), expected_export);

    load_jsonl(
        db,
        r#"{"type":"Person","id":"Carol","data":{"name":"Carol","age":41}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    assert_eq!(
        count_rows(db, "node:Person").await,
        3,
        "the graph keeps writing under the new spellings"
    );
    load_jsonl(
        db,
        r#"{"edge":"WorksAt","id":"works-carol","from":"Carol","to":"company-1","data":{}}"#,
        LoadMode::Append,
    )
    .await
    .expect("an edge insert satisfies the respelled @unique(@src, @dst)");
    load_jsonl(
        db,
        r#"{"edge":"WorksAt","id":"works-carol-again","from":"Carol","to":"company-1","data":{}}"#,
        LoadMode::Append,
    )
    .await
    .expect_err("the respelled @unique(@src, @dst) still refuses a duplicate endpoint pair");
    assert_eq!(count_rows(db, "edge:WorksAt").await, 3);
    db.ensure_indices()
        .await
        .expect("index reconciliation after the rename is a no-op by field id");
}

#[tokio::test]
async fn system_column_upgrade_respells_a_legacy_graph_in_place() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let version_before = version_main(&db).await.unwrap();
    let snapshot_before = db.resolve_snapshot("main").await.unwrap();

    let check = db
        .upgrade_system_columns(SystemColumnUpgradeOptions { check: true })
        .await
        .unwrap();
    assert_eq!(check.outcome, SystemColumnUpgradeOutcome::CheckPassed);
    assert!(check.success());
    assert_eq!((check.stamp_before, check.stamp_after), (10, 10));
    assert_eq!(
        check.tables,
        ["edge:WorksAt", "node:Company", "node:Person"]
            .map(str::to_string)
            .to_vec()
    );
    assert_eq!(
        db.internal_schema_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        10,
        "check mode writes nothing"
    );
    assert_eq!(schema_ir(&dir)["ir_version"].as_u64(), Some(2));

    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_eq!((report.stamp_before, report.stamp_after), (10, 10));
    assert!(report.findings.is_empty());
    assert!(report.graph_manifest_version.is_some());
    assert_upgraded(&mut db, &dir, &export_before).await;
    assert!(recovery_audit_kinds(dir.path()).await.is_empty());

    let entity = db
        .entity_at("node:Company", "company-1", version_before)
        .await
        .unwrap()
        .expect("a historical read at the pre-upgrade version still decodes the legacy image");
    assert_eq!(entity["@id"], "company-1");
    assert_eq!(entity["name"], "Acme");
    assert_history_readable(&db, version_before, &snapshot_before).await;

    let again = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(again.outcome, SystemColumnUpgradeOutcome::AlreadyCurrent);
    assert_eq!((again.stamp_before, again.stamp_after), (10, 10));
    drop(db);

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&reopened, "node:Person").await, 3);
    let result = query_main(
        &mut reopened,
        COMPANY_QUERY,
        "company_identity",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(collect_column_strings(result.batches(), "c.name"), ["Acme"]);
    let read_only = Omnigraph::open_read_only(uri).await.unwrap();
    assert_eq!(
        read_only
            .internal_schema_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        10
    );
}

#[tokio::test]
async fn system_column_upgrade_keeps_history_readable_after_a_user_id_property() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = legacy_graph_with_data(&dir).await;
    let version_before = version_main(&db).await.unwrap();
    let snapshot_before = db.resolve_snapshot("main").await.unwrap();
    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);

    db.apply_schema(UPGRADED_SCHEMA_WITH_ID_PROPERTY)
        .await
        .expect("the upgraded namespace admits a user property named id");
    let live = query_main(
        &mut db,
        "query ids() { match { $p: Person } return { $p.@id, $p.id } order { $p.@id asc } }",
        "ids",
        &ParamMap::new(),
    )
    .await
    .expect("the live catalog carries both @id and the user property id");
    assert_eq!(
        collect_column_strings(live.batches(), "p.@id"),
        ["Alice", "Bob"]
    );

    assert_history_readable(&db, version_before, &snapshot_before).await;
    let error = db
        .run_query_at(
            version_before,
            "query old_ids() { match { $p: Person } return { $p.id } }",
            "old_ids",
            &ParamMap::new(),
        )
        .await
        .expect_err("the pre-upgrade image has no column for the later user property");
    assert!(error.to_string().contains("id"), "{error}");
}

#[tokio::test]
async fn system_column_upgrade_history_survives_unrelated_reclaimed_tables() {
    use lance::Dataset;
    use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
    use omnigraph::error::OmniError;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let version_before = version_main(&db).await.unwrap();
    let snapshot_before = db.resolve_snapshot("main").await.unwrap();
    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);

    let snap = snapshot_main(&db).await.unwrap();
    let first = snap.datasets().next().unwrap().type_key.clone();
    let first_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        snap.dataset(&first)
            .unwrap()
            .dataset_path
            .trim_start_matches('/')
    );
    let dataset = Dataset::open(&first_uri).await.unwrap();
    let removed = cleanup_old_versions(
        &dataset,
        CleanupPolicy {
            before_version: Some(dataset.version().version),
            delete_unverified: true,
            error_if_tagged_old_versions: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        removed.old_versions > 0,
        "precondition: {first} history was reclaimed"
    );

    let (source, name, column, expected, reclaimed_id) = if first == "node:Person" {
        (
            COMPANY_QUERY,
            "company_identity",
            "c.@id",
            vec!["company-1"],
            "Alice",
        )
    } else if first == "node:Company" {
        (
            OLD_PEOPLE_QUERY,
            "old_people",
            "p.@id",
            vec!["Alice", "Bob"],
            "company-1",
        )
    } else {
        (
            OLD_PEOPLE_QUERY,
            "old_people",
            "p.@id",
            vec!["Alice", "Bob"],
            "works-alice",
        )
    };
    let historical = db
        .run_query_at(version_before, source, name, &ParamMap::new())
        .await
        .expect("a pinned read that never touches the reclaimed table still plans");
    assert_eq!(
        collect_column_strings(historical.batches(), column),
        expected
    );
    let at_snapshot = db
        .query(
            ReadTarget::snapshot(snapshot_before),
            source,
            name,
            &ParamMap::new(),
        )
        .await
        .expect("the SnapshotId read never touches the reclaimed table either");
    assert_eq!(
        collect_column_strings(at_snapshot.batches(), column),
        expected
    );
    let error = db
        .entity_at(&first, reclaimed_id, version_before)
        .await
        .expect_err("the reclaimed table's own history stays a typed refusal");
    assert!(
        matches!(error, OmniError::HistoricalVersionReclaimed { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn system_column_upgrade_refuses_before_any_effect() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    db.branch_create("feature").await.unwrap();

    for check in [true, false] {
        let report = db
            .upgrade_system_columns(SystemColumnUpgradeOptions { check })
            .await
            .unwrap();
        assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Refused);
        assert!(!report.success());
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].code, "system_columns_preflight");
        assert!(
            report.findings[0].message.contains("feature"),
            "{}",
            report.findings[0].message
        );
    }
    assert_eq!(
        db.internal_schema_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        10
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(schema_ir(&dir)["ir_version"].as_u64(), Some(2));

    let reserved_dir = tempfile::tempdir().unwrap();
    let reserved_uri = reserved_dir.path().to_str().unwrap();
    let reserved = Omnigraph::init_with_legacy_system_columns_for_tests(
        reserved_uri,
        "node Person {\n    name: String @key\n    _legacy_note: String?\n}\n",
    )
    .await
    .unwrap();
    let report = reserved
        .upgrade_system_columns(SystemColumnUpgradeOptions { check: true })
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Refused);
    assert_eq!(report.findings.len(), 1);
    assert!(
        report.findings[0].message.contains("Person._legacy_note"),
        "{}",
        report.findings[0].message
    );
}

async fn crash_then_roll_forward(seam: &'static DecideSeam) {
    let failpoint = seam.name();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();

    let error = {
        let _failpoint = seam.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade")
    };
    assert!(
        error.to_string().contains(failpoint),
        "unexpected error at {failpoint}: {error}"
    );
    let operation_ids = sidecar_operation_ids(dir.path());
    assert_eq!(
        operation_ids.len(),
        1,
        "exactly one intent survives {failpoint}"
    );
    let sidecar: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            dir.path()
                .join("__recovery")
                .join(format!("{}.json", operation_ids[0])),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar["writer_kind"], "SchemaApply");
    assert_eq!(
        sidecar["protocol_v7"]["system_column_upgrade"]["from_stamp"],
        10
    );
    assert_eq!(
        sidecar["protocol_v7"]["system_column_upgrade"]["to_stamp"],
        10
    );
    assert!(
        sidecar["protocol_v7"]["effects"]
            .as_array()
            .unwrap()
            .iter()
            .all(|effect| effect["kind"]["kind"] == "SystemColumnRename"),
        "{sidecar}"
    );
    drop(db);

    let read_only = Omnigraph::open_read_only(uri)
        .await
        .err()
        .expect("a read-only open must not serve an unfinished upgrade");
    assert!(
        read_only.to_string().contains("system-column upgrade")
            || read_only.to_string().contains("read-write"),
        "{read_only}"
    );

    let mut recovered = Omnigraph::open(uri)
        .await
        .expect("the read-write open rolls the upgrade forward");
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    assert_upgraded(&mut recovered, &dir, &export_before).await;
    let again = recovered
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(again.outcome, SystemColumnUpgradeOutcome::AlreadyCurrent);
}

#[tokio::test]
async fn system_column_upgrade_retries_on_the_same_handle_after_a_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade after arming");
    }
    let retried = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default()),
    )
    .await
    .expect("the same-handle retry must not deadlock on the schema gate")
    .unwrap();
    assert_eq!(retried.outcome, SystemColumnUpgradeOutcome::AlreadyCurrent);
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    assert_upgraded(&mut db, &dir, &export_before).await;
}

#[tokio::test]
async fn system_column_upgrade_survives_an_interrupted_recovery() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_TABLE_COMMIT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade after the first rename");
    }
    drop(db);
    {
        let _failpoint = catalog::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH.fire_always();
        Omnigraph::open(uri)
            .await
            .err()
            .expect("the first recovery stops after confirming the intent, before publishing");
    }
    assert_eq!(sidecar_operation_ids(dir.path()).len(), 1);
    let mut recovered = Omnigraph::open(uri)
        .await
        .expect("the second read-write open publishes the confirmed intent");
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    assert_upgraded(&mut recovered, &dir, &export_before).await;
}

#[tokio::test]
async fn system_column_upgrade_recovery_reclaims_a_dead_writers_lock() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let crashed = {
        let _failpoint = catalog::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT.panic_at();
        tokio::spawn(async move {
            db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
                .await
                .map(|report| report.outcome)
        })
        .await
    };
    assert!(
        crashed
            .expect_err("the writer dies after arming, releasing nothing")
            .is_panic()
    );
    assert!(
        schema_apply_lock_present(&dir),
        "a dead writer leaves its lock behind"
    );
    assert_eq!(sidecar_operation_ids(dir.path()).len(), 1);

    let mut recovered = Omnigraph::open(uri)
        .await
        .expect("the read-write open rolls the upgrade forward");
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    assert!(
        !schema_apply_lock_present(&dir),
        "recovery reclaims the dead writer's lock"
    );
    assert_upgraded(&mut recovered, &dir, &export_before).await;
    recovered
        .apply_schema(UPGRADED_SCHEMA_WITH_ID_PROPERTY)
        .await
        .expect("schema apply is live again after recovery");
    recovered
        .branch_create("after-upgrade")
        .await
        .expect("branch control is live again after recovery");
}

#[tokio::test]
async fn system_column_upgrade_recovery_survives_a_crash_after_the_lock_reclaim() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_TABLE_COMMIT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade after the first rename");
    }
    drop(db);
    {
        let _failpoint = catalog::SYSTEM_COLUMN_UPGRADE_AFTER_LOCK_RECLAIM.fire_always();
        Omnigraph::open(uri).await.err().expect(
            "the first recovery stops after reclaiming the lock, before retiring the intent",
        );
    }
    assert_eq!(
        sidecar_operation_ids(dir.path()).len(),
        1,
        "the intent outlives the lock"
    );
    assert!(!schema_apply_lock_present(&dir));
    let mut recovered = Omnigraph::open(uri)
        .await
        .expect("the second read-write open retires the intent");
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        recovery_audit_kinds(dir.path()).await,
        vec!["RolledForward"]
    );
    assert_upgraded(&mut recovered, &dir, &export_before).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_before_the_stamp_advance() {
    crash_then_roll_forward(&catalog::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_after_the_stamp_advance() {
    crash_then_roll_forward(&catalog::SYSTEM_COLUMN_UPGRADE_AFTER_STAMP_ADVANCE).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_after_the_first_rename() {
    crash_then_roll_forward(&catalog::SCHEMA_APPLY_POST_TABLE_COMMIT).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_after_the_stamp_before_staging() {
    crash_then_roll_forward(&catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_after_confirmation() {
    crash_then_roll_forward(&catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE).await;
}

#[tokio::test]
async fn system_column_upgrade_rolls_forward_after_the_manifest_commit() {
    crash_then_roll_forward(&catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT).await;
}
