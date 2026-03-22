use std::collections::HashSet;
use std::fs;
use std::path::Path;

use lance::Dataset;
use lance_index::DatasetIndexExt;
use tempfile::TempDir;

use nanograph::schema::parser::parse_schema;
use nanograph::schema_ir::SchemaIR;
use nanograph::store::database::Database;
use nanograph::store::export::build_export_rows_at_path;
use nanograph::store::manifest::GraphManifest;
use nanograph::store::migration::{
    MigrationStatus, MigrationStep, SchemaCompatibility, analyze_schema_diff,
    execute_schema_migration,
};
use nanograph::store::scalar_index_name;
use nanograph::store::txlog::read_tx_catalog_entries;

fn base_schema() -> &'static str {
    r#"
node User {
    name: String @key
    age: I32?
}
edge Knows: User -> User
"#
}

fn rename_schema() -> &'static str {
    r#"
node Account @rename_from("User") {
    full_name: String @key @rename_from("name")
    age: I32?
}
edge ConnectedTo: Account -> Account @rename_from("Knows")
"#
}

fn schema_with_new_non_nullable_prop() -> &'static str {
    r#"
node User {
    name: String
    age: I32?
    email: String
}
edge Knows: User -> User
"#
}

fn schema_with_drop_prop() -> &'static str {
    r#"
node User {
    name: String
}
edge Knows: User -> User
"#
}

fn schema_with_name_as_i32() -> &'static str {
    r#"
node User {
    name: I32?
    age: I32?
}
edge Knows: User -> User
"#
}

fn schema_with_age_non_nullable() -> &'static str {
    r#"
node User {
    name: String
    age: I32
}
edge Knows: User -> User
"#
}

fn rebind_base_schema() -> &'static str {
    r#"
node User {
    name: String @key
}
node Company {
    name: String
}
edge Rel: User -> User
"#
}

fn rebind_target_schema() -> &'static str {
    r#"
node User {
    name: String
}
node Company {
    name: String
}
edge Rel: User -> Company
"#
}

fn rebind_data() -> &'static str {
    r#"
{"type":"User","data":{"name":"Alice"}}
{"type":"User","data":{"name":"Bob"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"rel","from":"Alice","to":"Bob"}
"#
}

fn schema_with_agent_metadata() -> &'static str {
    r#"
node User @description("Tracked person") @instruction("Query by slug") {
    name: String @key @description("Stable slug")
    age: I32?
}
edge Knows: User -> User @description("Social tie")
"#
}

fn schema_with_enum_addition() -> &'static str {
    r#"
node Ticket {
    slug: String @key
    status: enum(open, closed, blocked)
}
"#
}

fn schema_with_enum_removal() -> &'static str {
    r#"
node Ticket {
    slug: String @key
    status: enum(open, closed)
}
"#
}

fn schema_with_key_change() -> &'static str {
    r#"
node User {
    name: String
    email: String? @key
    age: I32?
}
edge Knows: User -> User
"#
}

fn drop_type_base_schema() -> &'static str {
    r#"
node User {
    name: String @key
}
node Company {
    name: String @key
}
edge WorksAt: User -> Company
"#
}

fn drop_type_target_schema() -> &'static str {
    r#"
node User {
    name: String
}
"#
}

fn drop_type_data() -> &'static str {
    r#"
{"type":"User","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"worksAt","from":"Alice","to":"Acme"}
"#
}

fn account_with_nullable_prop_schema() -> &'static str {
    r#"
node Account {
    full_name: String
    age: I32?
    nickname: String?
}
edge ConnectedTo: Account -> Account
"#
}

fn schema_with_index_annotation() -> &'static str {
    r#"
node User {
    name: String @index
    age: I32?
}
edge Knows: User -> User
"#
}

fn schema_with_key_auto_index() -> &'static str {
    r#"
node User {
    name: String @key
    age: I32?
}
edge Knows: User -> User
"#
}

fn sample_data() -> &'static str {
    r#"
{"type":"User","data":{"name":"Alice","age":30}}
{"type":"User","data":{"name":"Bob","age":null}}
{"edge":"knows","from":"Alice","to":"Bob"}
"#
}

fn edge_order_bug_base_schema() -> &'static str {
    r#"
node Person {
    slug: String @key
    name: String
    email: String @unique
    age: I32?
    role: enum(admin, member, guest)
    tags: [String]?
    joinedAt: Date
}

node Project {
    slug: String @key
    name: String
    description: String?
    isActive: Bool
    budget: F64?
    createdAt: DateTime
}

edge WorksOn: Person -> Project {
    role: enum(lead, contributor, reviewer)
    startedAt: Date
}
"#
}

fn edge_order_bug_target_schema() -> &'static str {
    r#"
node Person {
    slug: String @key
    name: String
    displayName: String?
    email: String @unique
    age: I32?
    role: enum(admin, member, guest)
    tags: [String]?
    joinedAt: Date
}

node Project {
    slug: String @key
    name: String
    description: String?
    isActive: Bool
    budget: F64?
    createdAt: DateTime
}

edge WorksOn: Person -> Project {
    role: enum(lead, contributor, reviewer)
    startedAt: Date
}
"#
}

fn edge_order_bug_data() -> &'static str {
    r#"
{"type":"Person","data":{"slug":"alice","name":"Alice","email":"alice@example.com","age":30,"role":"admin","tags":["rust"],"joinedAt":"2024-01-10"}}
{"type":"Project","data":{"slug":"apollo","name":"Apollo","description":"Ship it","isActive":true,"budget":12.5,"createdAt":"2024-01-10T12:00:00Z"}}
{"edge":"worksOn","from":"alice","to":"apollo","data":{"role":"lead","startedAt":"2024-02-01"}}
"#
}

async fn init_db_with_data(schema: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("db");

    Database::init(&db_path, schema).await.expect("init db");
    let db = Database::open(&db_path).await.expect("open db");
    db.load(sample_data()).await.expect("load data");

    (dir, db_path)
}

async fn init_db_with_custom_data(schema: &str, data: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("db");

    Database::init(&db_path, schema).await.expect("init db");
    let db = Database::open(&db_path).await.expect("open db");
    db.load(data).await.expect("load data");

    (dir, db_path)
}

fn write_schema(db_path: &Path, schema: &str) {
    fs::write(db_path.join("schema.pg"), schema).expect("write schema.pg");
}

fn sidecar_paths(db_path: &Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let parent = db_path.parent().expect("db parent");
    let name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .expect("db name");
    (
        parent.join(format!("{}.migration.journal.json", name)),
        parent.join(format!("{}.migration.backup", name)),
        parent.join(format!("{}.migration.staging.test", name)),
    )
}

fn has_rename_type_step(
    plan_steps: &[nanograph::store::migration::PlannedStep],
    kind: &str,
    from: &str,
    to: &str,
) -> bool {
    plan_steps.iter().any(|s| {
        matches!(
            &s.step,
            MigrationStep::RenameType {
                type_kind,
                old_name,
                new_name,
                ..
            } if type_kind == kind && old_name == from && new_name == to
        )
    })
}

#[tokio::test]
async fn migration_dry_run_reports_rename_steps() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, rename_schema());

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");

    assert_eq!(exec.status, MigrationStatus::Applied);
    assert!(
        has_rename_type_step(&exec.plan.steps, "node", "User", "Account"),
        "expected node rename step"
    );
    assert!(
        has_rename_type_step(&exec.plan.steps, "edge", "Knows", "ConnectedTo"),
        "expected edge rename step"
    );
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                &s.step,
                MigrationStep::RenameProperty {
                    type_name,
                    old_name,
                    new_name,
                    ..
                } if type_name == "Account" && old_name == "name" && new_name == "full_name"
            )
        }),
        "expected property rename step"
    );
    assert!(
        exec.plan.blocked.is_empty(),
        "unexpected blocked reasons: {:?}",
        exec.plan.blocked
    );
}

#[tokio::test]
async fn migration_apply_rename_preserves_data() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, rename_schema());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let db = Database::open(&db_path).await.expect("re-open migrated db");
    assert!(db.schema_ir.node_type_id("Account").is_some());
    assert!(db.schema_ir.node_type_id("User").is_none());
    assert!(db.schema_ir.edge_type_id("ConnectedTo").is_some());
    assert!(db.schema_ir.edge_type_id("Knows").is_none());
    let rows = build_export_rows_at_path(&db_path, false, true)
        .await
        .expect("export rows");
    let mut values = rows
        .iter()
        .filter(|row| row.get("type").and_then(serde_json::Value::as_str) == Some("Account"))
        .filter_map(|row| row["data"]["full_name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec!["Alice".to_string(), "Bob".to_string()]);

    let edge_count = rows
        .iter()
        .filter(|row| row.get("edge").and_then(serde_json::Value::as_str) == Some("ConnectedTo"))
        .count();
    assert_eq!(edge_count, 1);
}

#[tokio::test]
async fn migration_apply_add_property_preserves_edge_property_order() {
    let (_dir, db_path) =
        init_db_with_custom_data(edge_order_bug_base_schema(), edge_order_bug_data()).await;
    write_schema(&db_path, edge_order_bug_target_schema());

    let exec = execute_schema_migration(&db_path, None, false, true)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let rows = build_export_rows_at_path(&db_path, false, true)
        .await
        .expect("export rows");
    let edge = rows
        .iter()
        .find(|row| row.get("edge").and_then(serde_json::Value::as_str) == Some("WorksOn"))
        .expect("worksOn row");
    assert_eq!(edge["data"]["role"].as_str(), Some("lead"));
    assert_eq!(edge["data"]["startedAt"].as_str(), Some("2024-02-01"));
}

#[tokio::test]
async fn migration_blocks_non_nullable_property_without_backfill() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_new_non_nullable_prop());

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");

    assert_eq!(exec.status, MigrationStatus::Blocked);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::AddProperty {
                    ref type_name,
                    ref prop_name,
                    nullable,
                    ..
                } if type_name == "User" && prop_name == "email" && !nullable
            )
        }),
        "expected blocked AddProperty step for User.email"
    );
    assert!(
        exec.plan.blocked.iter().any(|r| r.contains("non-nullable")),
        "expected blocked reason mentioning non-nullable property"
    );
}

#[tokio::test]
async fn migration_requires_confirmation_for_drop_property() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_drop_prop());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("plan migration");
    assert_eq!(exec.status, MigrationStatus::NeedsConfirmation);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::DropProperty {
                    ref type_name,
                    ref prop_name,
                    ..
                } if type_name == "User" && prop_name == "age"
            )
        }),
        "expected DropProperty step for User.age"
    );

    let applied = execute_schema_migration(&db_path, None, false, true)
        .await
        .expect("apply with auto-approve");
    assert_eq!(applied.status, MigrationStatus::Applied);

    let db = Database::open(&db_path).await.expect("re-open migrated db");
    let user = db
        .schema_ir
        .node_types()
        .find(|node| node.name == "User")
        .expect("user node type");
    let dataset_path = db_path.join("nodes").join(SchemaIR::dir_name(user.type_id));
    let dataset = Dataset::open(dataset_path.to_string_lossy().as_ref())
        .await
        .expect("open user dataset");
    assert!(
        !dataset
            .schema()
            .fields
            .iter()
            .any(|field| field.name == "age")
    );
}

#[tokio::test]
async fn migration_blocks_invalid_type_cast() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_name_as_i32());

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");

    assert_eq!(exec.status, MigrationStatus::NeedsConfirmation);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::AlterPropertyType {
                    ref type_name,
                    ref prop_name,
                    ..
                } if type_name == "User" && prop_name == "name"
            )
        }),
        "expected AlterPropertyType step for User.name"
    );
    assert!(exec.plan.blocked.is_empty());
}

#[tokio::test]
async fn migration_blocks_nullable_to_non_nullable_when_nulls_exist() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_age_non_nullable());

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");

    assert_eq!(exec.status, MigrationStatus::Blocked);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::AlterPropertyNullability {
                    ref type_name,
                    ref prop_name,
                    nullable,
                    ..
                } if type_name == "User" && prop_name == "age" && !nullable
            )
        }),
        "expected AlterPropertyNullability step for User.age"
    );
    assert!(
        exec.plan
            .blocked
            .iter()
            .any(|r| r.contains("non-nullable") && r.contains("null value")),
        "expected blocked reason for existing nulls"
    );
}

#[tokio::test]
async fn migration_blocks_edge_endpoint_rebind_with_invalid_rows() {
    let (_dir, db_path) = init_db_with_custom_data(rebind_base_schema(), rebind_data()).await;
    write_schema(&db_path, rebind_target_schema());

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");

    assert_eq!(exec.status, MigrationStatus::Blocked);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::RebindEdgeEndpoints { ref edge_name, .. } if edge_name == "Rel"
            )
        }),
        "expected RebindEdgeEndpoints step for Rel"
    );
    assert!(
        exec.plan
            .blocked
            .iter()
            .any(|r| r.contains("endpoint rebind") && r.contains("invalidate")),
        "expected blocked reason for invalid edge endpoint rebind"
    );
}

#[tokio::test]
async fn migration_requires_confirmation_for_drop_node_and_edge_type() {
    let (_dir, db_path) = init_db_with_custom_data(drop_type_base_schema(), drop_type_data()).await;
    write_schema(&db_path, drop_type_target_schema());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("plan migration");

    assert_eq!(exec.status, MigrationStatus::NeedsConfirmation);
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::DropNodeType { ref name, .. } if name == "Company"
            )
        }),
        "expected DropNodeType step for Company"
    );
    assert!(
        exec.plan.steps.iter().any(|s| {
            matches!(
                s.step,
                MigrationStep::DropEdgeType { ref name, .. } if name == "WorksAt"
            )
        }),
        "expected DropEdgeType step for WorksAt"
    );
}

#[tokio::test]
async fn migration_sequential_runs_increment_schema_identity_version() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;

    write_schema(&db_path, rename_schema());
    let first = execute_schema_migration(&db_path, None, false, true)
        .await
        .expect("first migration");
    assert_eq!(first.status, MigrationStatus::Applied);

    write_schema(&db_path, account_with_nullable_prop_schema());
    let second = execute_schema_migration(&db_path, None, false, true)
        .await
        .expect("second migration");
    assert_eq!(second.status, MigrationStatus::Applied);

    let manifest = GraphManifest::read(&db_path).expect("read manifest");
    assert_eq!(manifest.schema_identity_version, 3);
}

#[tokio::test]
async fn migration_appends_tx_catalog_row() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, rename_schema());

    let exec = execute_schema_migration(&db_path, None, false, true)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let manifest = GraphManifest::read(&db_path).expect("read manifest");
    let tx_rows = read_tx_catalog_entries(&db_path).expect("read tx catalog");
    let last = tx_rows.last().expect("at least one tx row");

    assert_eq!(last.db_version, manifest.db_version);
    assert_eq!(last.tx_id, manifest.last_tx_id);
    assert_eq!(last.op_summary, "schema_migration");
}

#[tokio::test]
async fn migration_preserves_index_annotations_in_schema_ir() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_index_annotation());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let db = Database::open(&db_path).await.expect("re-open migrated db");
    let user = db
        .schema_ir
        .node_types()
        .find(|n| n.name == "User")
        .unwrap();
    let name_prop = user.properties.iter().find(|p| p.name == "name").unwrap();
    let age_prop = user.properties.iter().find(|p| p.name == "age").unwrap();

    assert!(name_prop.index);
    assert!(!name_prop.key);
    assert!(!age_prop.index);
}

#[tokio::test]
async fn migration_auto_indexes_key_properties_in_schema_ir() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_key_auto_index());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let db = Database::open(&db_path).await.expect("re-open migrated db");
    let user = db
        .schema_ir
        .node_types()
        .find(|n| n.name == "User")
        .unwrap();
    let name_prop = user.properties.iter().find(|p| p.name == "name").unwrap();

    assert!(name_prop.key);
    assert!(name_prop.index);
}

#[tokio::test]
async fn migration_builds_scalar_indexes_for_indexed_properties() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    write_schema(&db_path, schema_with_index_annotation());

    let exec = execute_schema_migration(&db_path, None, false, false)
        .await
        .expect("apply migration");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let db = Database::open(&db_path).await.expect("re-open migrated db");
    let user = db
        .schema_ir
        .node_types()
        .find(|n| n.name == "User")
        .expect("user node");
    let dataset_path = db_path.join("nodes").join(SchemaIR::dir_name(user.type_id));
    let dataset = Dataset::open(dataset_path.to_string_lossy().as_ref())
        .await
        .expect("open user dataset");
    let index_names: HashSet<String> = dataset
        .load_indices()
        .await
        .expect("load indices")
        .iter()
        .map(|idx| idx.name.clone())
        .collect();

    let expected_name_idx = scalar_index_name(user.type_id, "name");
    assert!(
        index_names.contains(&expected_name_idx),
        "expected migration-created scalar index {}",
        expected_name_idx
    );
}

#[tokio::test]
async fn migration_bootstraps_identity_counters_from_legacy_manifest() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    let old_db = Database::open(&db_path).await.expect("open db");
    let max_type_id = old_db
        .schema_ir
        .types
        .iter()
        .map(|t| match t {
            nanograph::schema_ir::TypeDef::Node(n) => n.type_id,
            nanograph::schema_ir::TypeDef::Edge(e) => e.type_id,
        })
        .max()
        .expect("at least one type");
    drop(old_db);

    let mut manifest = GraphManifest::read(&db_path).expect("read manifest");
    manifest.next_type_id = 0;
    manifest.next_prop_id = 0;
    manifest.schema_identity_version = 0;
    manifest
        .write_atomic(&db_path)
        .expect("write legacy manifest");

    write_schema(
        &db_path,
        r#"
node User {
    name: String
    age: I32?
}
node Team {
    name: String
}
edge Knows: User -> User
"#,
    );

    let exec = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect("dry run");
    assert_eq!(exec.status, MigrationStatus::Applied);

    let add_team = exec.plan.steps.iter().find_map(|s| match s.step {
        MigrationStep::AddNodeType { ref name, type_id } if name == "Team" => Some(type_id),
        _ => None,
    });
    assert_eq!(add_team, Some(max_type_id.saturating_add(1).max(1)));
}

#[tokio::test]
async fn open_cleans_committed_journal_and_backup_sidecars() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    let (journal_path, backup_path, staging_path) = sidecar_paths(&db_path);

    fs::create_dir_all(&backup_path).expect("create backup dir");
    fs::create_dir_all(&staging_path).expect("create staging dir");
    fs::write(backup_path.join("marker.txt"), "old").expect("write backup marker");
    fs::write(staging_path.join("marker.txt"), "staged").expect("write staging marker");

    let journal = serde_json::json!({
        "version": 1,
        "state": "COMMITTED",
        "db_path": db_path.display().to_string(),
        "backup_path": backup_path.display().to_string(),
        "staging_path": staging_path.display().to_string(),
        "old_schema_hash": "old",
        "new_schema_hash": "new",
        "created_at_unix": 0
    });
    fs::write(
        &journal_path,
        serde_json::to_string_pretty(&journal).expect("serialize journal"),
    )
    .expect("write journal");

    let _db = Database::open(&db_path).await.expect("open db");
    assert!(!journal_path.exists(), "journal should be removed");
    assert!(!backup_path.exists(), "backup should be removed");
    assert!(!staging_path.exists(), "staging should be removed");
}

#[tokio::test]
async fn open_cleans_prepared_journal_when_db_is_intact() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    let (journal_path, backup_path, staging_path) = sidecar_paths(&db_path);

    fs::create_dir_all(&staging_path).expect("create staging dir");
    fs::write(staging_path.join("marker.txt"), "staged").expect("write staging marker");

    let journal = serde_json::json!({
        "version": 1,
        "state": "PREPARED",
        "db_path": db_path.display().to_string(),
        "backup_path": backup_path.display().to_string(),
        "staging_path": staging_path.display().to_string(),
        "old_schema_hash": "old",
        "new_schema_hash": "new",
        "created_at_unix": 0
    });
    fs::write(
        &journal_path,
        serde_json::to_string_pretty(&journal).expect("serialize journal"),
    )
    .expect("write journal");

    let _db = Database::open(&db_path).await.expect("open db");
    assert!(!journal_path.exists(), "journal should be removed");
    assert!(!staging_path.exists(), "staging should be removed");
}

#[tokio::test]
async fn open_cleans_aborted_journal_when_backup_is_already_restored() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    let (journal_path, backup_path, staging_path) = sidecar_paths(&db_path);

    fs::create_dir_all(&staging_path).expect("create staging dir");
    fs::write(staging_path.join("marker.txt"), "staged").expect("write staging marker");

    let journal = serde_json::json!({
        "version": 1,
        "state": "ABORTED",
        "db_path": db_path.display().to_string(),
        "backup_path": backup_path.display().to_string(),
        "staging_path": staging_path.display().to_string(),
        "old_schema_hash": "old",
        "new_schema_hash": "new",
        "created_at_unix": 0
    });
    fs::write(
        &journal_path,
        serde_json::to_string_pretty(&journal).expect("serialize journal"),
    )
    .expect("write journal");

    let _db = Database::open(&db_path).await.expect("open db");
    assert!(!journal_path.exists(), "journal should be removed");
    assert!(!staging_path.exists(), "staging should be removed");
}

#[tokio::test]
async fn migration_refuses_orphan_backup_sidecar() {
    let (_dir, db_path) = init_db_with_data(base_schema()).await;
    let (_journal_path, backup_path, _staging_path) = sidecar_paths(&db_path);
    fs::create_dir_all(&backup_path).expect("create backup dir");

    let err = execute_schema_migration(&db_path, None, true, false)
        .await
        .expect_err("migration should fail");
    assert!(
        err.to_string().contains("orphaned migration backup"),
        "unexpected error: {}",
        err
    );
}

#[test]
fn schema_diff_reports_additive_agent_metadata_and_enum_growth() {
    let old_schema = parse_schema(base_schema()).unwrap();
    let new_schema = parse_schema(schema_with_agent_metadata()).unwrap();
    let report = analyze_schema_diff(&old_schema, &new_schema).unwrap();

    assert_eq!(
        report.compatibility,
        SchemaCompatibility::Additive,
        "unexpected report: {:?}",
        report
    );
    assert!(report.steps.iter().any(|step| matches!(
        step.step,
        MigrationStep::AlterMetadata { ref annotation, .. } if annotation == "description"
    )));

    let enum_report = analyze_schema_diff(
        &parse_schema(
            r#"
node Ticket {
    slug: String @key
    status: enum(open, closed)
}
"#,
        )
        .unwrap(),
        &parse_schema(schema_with_enum_addition()).unwrap(),
    )
    .unwrap();
    assert!(
        enum_report
            .steps
            .iter()
            .any(|step| matches!(step.step, MigrationStep::AlterPropertyEnumValues { .. }))
    );
    assert_eq!(enum_report.compatibility, SchemaCompatibility::Additive);
}

#[test]
fn schema_diff_flags_breaking_key_and_enum_removal_changes() {
    let key_report = analyze_schema_diff(
        &parse_schema(base_schema()).unwrap(),
        &parse_schema(schema_with_key_change()).unwrap(),
    )
    .unwrap();
    assert_eq!(key_report.compatibility, SchemaCompatibility::Breaking);
    assert!(key_report.has_breaking);
    assert!(
        key_report
            .steps
            .iter()
            .any(|step| matches!(step.step, MigrationStep::AlterPropertyKey { .. }))
    );

    let enum_report = analyze_schema_diff(
        &parse_schema(schema_with_enum_addition()).unwrap(),
        &parse_schema(schema_with_enum_removal()).unwrap(),
    )
    .unwrap();
    assert_eq!(enum_report.compatibility, SchemaCompatibility::Breaking);
    assert!(
        enum_report
            .steps
            .iter()
            .any(|step| matches!(step.step, MigrationStep::AlterPropertyEnumValues { .. }))
    );
}

#[test]
fn schema_diff_uses_rename_from_to_reduce_false_positive_renames() {
    let report = analyze_schema_diff(
        &parse_schema(base_schema()).unwrap(),
        &parse_schema(rename_schema()).unwrap(),
    )
    .unwrap();
    assert_eq!(report.compatibility, SchemaCompatibility::Additive);
    assert!(report.steps.iter().any(|step| matches!(
        step.step,
        MigrationStep::RenameType { ref old_name, ref new_name, .. }
            if old_name == "User" && new_name == "Account"
    )));
    assert!(report.steps.iter().any(|step| matches!(
        step.step,
        MigrationStep::RenameProperty { ref old_name, ref new_name, .. }
            if old_name == "name" && new_name == "full_name"
    )));
}
