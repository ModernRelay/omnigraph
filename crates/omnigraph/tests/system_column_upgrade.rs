//! RFC 0040 Rollout step 3: the explicit system-column upgrade of a
//! legacy-vintage graph, its preflight refusals, and its crash windows. Since
//! RFC 0067 the upgrade arms no recovery sidecar: a failure before its one
//! manifest commit leaves the graph unchanged, and one after it is finished
//! by the next read-write open or the next write on the same handle.
#![cfg(feature = "failpoints")]

mod helpers;

use std::fs;

use helpers::recovery::sidecar_operation_ids;
use helpers::*;
use omnigraph::Session;
use omnigraph::db::{
    Omnigraph, ReadTarget, SnapshotDataset, SnapshotId, SystemColumnUpgradeOptions,
    SystemColumnUpgradeOutcome,
};
use omnigraph::loader::LoadMode;
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

async fn legacy_graph_with_data(dir: &tempfile::TempDir) -> Session {
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(
        Omnigraph::init_with_legacy_system_columns_for_tests(uri, LEGACY_SCHEMA)
            .await
            .unwrap(),
    );
    db.load_jsonl(LEGACY_DATA, LoadMode::Overwrite)
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

async fn assert_history_readable(db: &Session, version_before: u64, snapshot_before: &SnapshotId) {
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

async fn assert_upgraded(db: &Session, dir: &tempfile::TempDir, expected_export: &str) {
    assert_eq!(
        db.internal_schema_version_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap(),
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
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

    db.load_jsonl(
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
    db.load_jsonl(
        r#"{"edge":"WorksAt","id":"works-carol","from":"Carol","to":"company-1","data":{}}"#,
        LoadMode::Append,
    )
    .await
    .expect("an edge insert satisfies the respelled @unique(@src, @dst)");
    db.load_jsonl(
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
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let version_before = version_main(&db).await.unwrap();
    let snapshot_before = db.resolve_snapshot("main").await.unwrap();

    let check = db
        .upgrade_system_columns(SystemColumnUpgradeOptions { check: true })
        .await
        .unwrap();
    assert_eq!(check.outcome, SystemColumnUpgradeOutcome::CheckPassed);
    assert!(check.success());
    assert_eq!(
        (check.stamp_before, check.stamp_after),
        (
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
        )
    );
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
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
        "check mode writes nothing"
    );
    assert_eq!(schema_ir(&dir)["ir_version"].as_u64(), Some(2));

    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_eq!(
        (report.stamp_before, report.stamp_after),
        (
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
        )
    );
    assert!(report.findings.is_empty());
    assert!(report.graph_manifest_version.is_some());
    assert_upgraded(&db, &dir, &export_before).await;

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
    assert_eq!(
        (again.stamp_before, again.stamp_after),
        (
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
        )
    );
    drop(db);

    let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_eq!(count_rows(&reopened, "node:Person").await, 3);
    let result = query_main(
        &reopened,
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
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
    );
}

#[tokio::test]
async fn system_column_upgrade_keeps_history_readable_after_a_user_id_property() {
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

    db.apply_schema(UPGRADED_SCHEMA_WITH_ID_PROPERTY)
        .await
        .expect("the upgraded namespace admits a user property named id");
    let live = query_main(
        &db,
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
    use omnigraph::db::CleanupPolicyOptions;
    use omnigraph::error::OmniError;

    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let version_before = version_main(&db).await.unwrap();
    let snapshot_before = db.resolve_snapshot("main").await.unwrap();
    db.load_jsonl(
        r#"{"type":"Company","data":{"id":"company-2","name":"Beta"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let company_moved = version_main(&db).await.unwrap();
    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);

    let first = "node:Company";
    let keep = CleanupPolicyOptions {
        keep_versions: Some(
            u32::try_from(version_main(&db).await.unwrap() - company_moved + 1).unwrap(),
        ),
        older_than: None,
    };
    let plan = db.cleanup_plan(keep.clone()).await.unwrap();
    let main = helpers::collector::retained_on(&plan, None);
    assert!(
        main.retained.contains(&company_moved) && main.would_prune.contains(&version_before),
        "the run keeps the Company write's `__manifest` version and drops the pre-write one: {main:?}"
    );
    let stats = db.cleanup(keep).await.unwrap();
    let row = stats.iter().find(|row| row.type_key == first).unwrap();
    assert!(row.error.is_none(), "{row:?}");
    assert!(
        row.old_versions_removed > 0,
        "precondition: {first} history was reclaimed"
    );

    let (source, name, column, expected, reclaimed_id) = (
        OLD_PEOPLE_QUERY,
        "old_people",
        "p.@id",
        vec!["Alice", "Bob"],
        "company-1",
    );
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
        .entity_at(first, reclaimed_id, version_before)
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
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION
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

/// Person HEAD and published version on main.
async fn person_head_and_pin(db: &Omnigraph, dir: &tempfile::TempDir) -> (u64, u64) {
    let snapshot = snapshot_main(db).await.unwrap();
    let entry = snapshot.dataset("node:Person").unwrap();
    let uri = format!(
        "{}/{}",
        dir.path().to_str().unwrap().trim_end_matches('/'),
        entry.dataset_path.trim_start_matches('/')
    );
    let head = open_dataset_head_exact(&uri, None).await;
    (head.version().version, entry.published_dataset_version)
}

fn assert_no_staging(dir: &tempfile::TempDir) {
    for staging in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        assert!(
            !dir.path().join(staging).exists(),
            "{staging} must not outlive the open"
        );
    }
}

/// A failure before the upgrade's one manifest commit leaves the graph
/// exactly as it was: the plain error, no sidecar, the sentinel released, the
/// legacy contract still served (a read-only open included), every linear
/// HEAD at its pin, any staged contract discarded by the next read-write
/// open, and the retry completes.
async fn crash_before_publication_leaves_no_residue(seam: &'static DecideSeam) {
    let failpoint = seam.name();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let (head_before, pin_before) = person_head_and_pin(&db, &dir).await;

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
    assert!(
        !matches!(error, omnigraph::error::OmniError::RecoveryRequired { .. }),
        "nothing was published at {failpoint}, so nothing needs recovery: {error}"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    assert!(
        !schema_apply_lock_present(&dir),
        "a failed upgrade releases its sentinel"
    );
    assert_eq!(
        person_head_and_pin(&db, &dir).await,
        (head_before, pin_before),
        "a detached rename never moves the linear HEAD or the pin"
    );
    drop(db);

    let read_only = Omnigraph::open_read_only(uri)
        .await
        .expect("an unpublished upgrade is invisible to a read-only open");
    assert_eq!(
        read_only.export_jsonl("main", &[]).await.unwrap(),
        export_before
    );
    drop(read_only);

    let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_no_staging(&dir);
    assert_eq!(
        reopened.export_jsonl("main", &[]).await.unwrap(),
        export_before
    );
    let report = reopened
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_upgraded(&reopened, &dir, &export_before).await;
}

#[tokio::test]
async fn system_column_upgrade_pre_effect_failure_leaves_no_residue() {
    crash_before_publication_leaves_no_residue(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT).await;
}

#[tokio::test]
async fn system_column_upgrade_failure_after_the_first_rename_leaves_no_residue() {
    crash_before_publication_leaves_no_residue(&catalog::SCHEMA_APPLY_POST_TABLE_COMMIT).await;
}

#[tokio::test]
async fn system_column_upgrade_failure_before_staging_leaves_no_residue() {
    crash_before_publication_leaves_no_residue(&catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE).await;
}

#[tokio::test]
async fn system_column_upgrade_failure_after_staging_leaves_no_residue() {
    crash_before_publication_leaves_no_residue(&catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE).await;
}

/// A failure after the manifest commit reports the published commit. The
/// manifest already names the renamed tables, so a read-only open refuses
/// the uninstalled contract and the next read-write open installs it.
#[tokio::test]
async fn system_column_upgrade_post_commit_failure_is_finished_by_the_next_open() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let error = {
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade after its commit")
    };
    assert!(
        matches!(error, omnigraph::error::OmniError::RecoveryRequired { .. }),
        "a published upgrade whose contract is not installed names its commit: {error}"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
    drop(db);

    let read_only = Omnigraph::open_read_only(uri)
        .await
        .err()
        .expect("a read-only open must not serve a published upgrade under the old contract");
    assert!(read_only.to_string().contains("read-write"), "{read_only}");

    let recovered = helpers::session(
        Omnigraph::open(uri)
            .await
            .expect("the read-write open installs the published contract"),
    );
    assert_no_staging(&dir);
    assert_upgraded(&recovered, &dir, &export_before).await;
    let again = recovered
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(again.outcome, SystemColumnUpgradeOutcome::AlreadyCurrent);
}

/// The same handle finishes its own published upgrade at its next write
/// entry, without a reopen.
#[tokio::test]
async fn system_column_upgrade_post_commit_failure_heals_on_the_same_handle() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    {
        let _failpoint = catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade after its commit");
    }
    let retried = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default()),
    )
    .await
    .expect("the same-handle retry must not deadlock on the schema gate")
    .unwrap();
    assert_eq!(retried.outcome, SystemColumnUpgradeOutcome::AlreadyCurrent);
    assert_no_staging(&dir);
    assert_upgraded(&db, &dir, &export_before).await;
}

/// RFC 0067: the upgrade arms no recovery sidecar. Its only control-object
/// writes are the three staged contract files and the three live ones, and
/// its only deletes retire the staging.
#[tokio::test]
async fn system_column_upgrade_writes_no_control_object() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;

    // Failpoints are process-global: take the scenario so a sibling test's
    // armed seam cannot fire inside this run.
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    drop(legacy_graph_with_data(&dir).await);
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
    let db = helpers::session(Omnigraph::open_with_storage(uri, adapter).await.unwrap());

    let before_write_text = counts.write_text();
    let before_delete = counts.delete();
    let report = db
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_eq!(
        counts.write_text() - before_write_text,
        6,
        "the upgrade writes the staged and live contract files and no sidecar"
    );
    assert_eq!(
        counts.delete() - before_delete,
        3,
        "the upgrade deletes only its three staging files"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());
}

#[tokio::test]
async fn system_column_upgrade_retries_on_the_same_handle_after_a_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    {
        let _failpoint = catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT.fire_always();
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
            .await
            .expect_err("the failpoint must stop the upgrade before its first effect");
    }
    let retried = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        db.upgrade_system_columns(SystemColumnUpgradeOptions::default()),
    )
    .await
    .expect("the same-handle retry must not deadlock on the schema gate")
    .unwrap();
    assert_eq!(retried.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_upgraded(&db, &dir, &export_before).await;
}

/// A writer that dies holding the sentinel leaves it behind with nothing
/// published; the next read-write open reclaims it and the upgrade runs.
#[tokio::test]
async fn system_column_upgrade_open_reclaims_a_dead_writers_lock() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = legacy_graph_with_data(&dir).await;
    let export_before = db.export_jsonl("main", &[]).await.unwrap();
    let crashed = {
        let _failpoint = catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT.panic_at();
        tokio::spawn(async move {
            db.upgrade_system_columns(SystemColumnUpgradeOptions::default())
                .await
                .map(|report| report.outcome)
        })
        .await
    };
    assert!(
        crashed
            .expect_err("the writer dies under its sentinel, releasing nothing")
            .is_panic()
    );
    assert!(
        schema_apply_lock_present(&dir),
        "a dead writer leaves its lock behind"
    );
    assert!(sidecar_operation_ids(dir.path()).is_empty());

    let recovered = helpers::session(
        Omnigraph::open(uri)
            .await
            .expect("the read-write open reclaims the stale sentinel"),
    );
    assert!(
        !schema_apply_lock_present(&dir),
        "the open reclaims the dead writer's lock"
    );
    assert_eq!(
        recovered.export_jsonl("main", &[]).await.unwrap(),
        export_before,
        "nothing was published, so the graph is still the legacy one"
    );
    let report = recovered
        .upgrade_system_columns(SystemColumnUpgradeOptions::default())
        .await
        .unwrap();
    assert_eq!(report.outcome, SystemColumnUpgradeOutcome::Completed);
    assert_upgraded(&recovered, &dir, &export_before).await;
    recovered
        .apply_schema(UPGRADED_SCHEMA_WITH_ID_PROPERTY)
        .await
        .expect("schema apply is live again");
    recovered
        .branch_create("after-upgrade")
        .await
        .expect("branch control is live again");
}
