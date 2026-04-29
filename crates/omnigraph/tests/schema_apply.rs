mod helpers;

use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::{SchemaMigrationStep, SchemaTypeKind};

use helpers::*;

#[tokio::test]
async fn plan_schema_reports_supported_additive_change() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported);
    assert!(plan.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::AddProperty {
            type_kind: SchemaTypeKind::Node,
            type_name,
            property_name,
            ..
        } if type_name == "Person" && property_name == "nickname"
    )));
}

#[tokio::test]
async fn plan_schema_rejects_when_schema_contract_has_drifted() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let drifted = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    fs::write(dir.path().join("_schema.pg"), drifted).unwrap();

    let err = db.plan_schema(TEST_SCHEMA).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("current _schema.pg no longer matches the accepted compiled schema")
    );
}

#[tokio::test]
async fn apply_schema_noop_returns_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let result = db.apply_schema(TEST_SCHEMA).await.unwrap();
    assert!(result.supported);
    assert!(!result.applied);
    assert!(result.steps.is_empty());
}

#[tokio::test]
async fn apply_schema_rejects_when_non_main_branch_exists() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    db.branch_create("feature").await.unwrap();

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("schema apply requires a repo with only main")
    );
}

#[tokio::test]
async fn apply_schema_unsupported_plan_does_not_advance_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let desired = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(err.to_string().contains("changing property type"));
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

// ─── Destructive / safety-tier rejections ────────────────────────────────────
//
// Schema migration v1 only accepts additive change: add type, add nullable
// property, add index, rename. Every other shape returns an
// `UnsupportedChange` step that surfaces as an error from `apply_schema`,
// without advancing the manifest. These tests pin that contract for the
// destructive shapes (drop type, drop property, narrow type, add required,
// remove constraint) so a regression in the planner can't silently allow them.

#[tokio::test]
async fn apply_schema_rejects_dropping_a_property_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop `age` from Person. v1 doesn't support property removal even when
    // the column is nullable — it would silently destroy data.
    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(
        err.to_string().contains("removing property"),
        "expected 'removing property' in error, got: {err}"
    );

    // Manifest didn't advance and existing rows are untouched.
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
    assert_eq!(count_rows(&db, "node:Person").await, people_before);
}

#[tokio::test]
async fn apply_schema_rejects_dropping_a_node_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop the `Company` node type and its outgoing edge that references it.
    let desired = r#"
node Person {
    name: String @key
    age: I32?
}

edge Knows: Person -> Person {
    since: Date?
}
"#;
    let err = db.apply_schema(desired).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("removing node type") || msg.contains("removing edge type"),
        "expected drop-type error, got: {msg}"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

#[tokio::test]
async fn apply_schema_rejects_dropping_an_edge_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop only the `WorksAt` edge.
    let desired = TEST_SCHEMA.replace("\nedge WorksAt: Person -> Company", "");
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(
        err.to_string().contains("removing edge type"),
        "expected 'removing edge type' error, got: {err}"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

#[tokio::test]
async fn apply_schema_rejects_adding_a_required_property_without_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Add `email: String` (required, non-nullable, no @rename_from). Existing
    // rows have no value to fill in, so this is unsupported in v1.
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    email: String\n}",
    );
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(
        err.to_string().contains("adding required property"),
        "expected 'adding required property' error, got: {err}"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

#[tokio::test]
async fn plan_schema_for_property_type_narrowing_is_not_supported() {
    // Symmetric companion to `apply_schema_unsupported_plan_does_not_advance_manifest`,
    // which exercises widening (I32 -> I64). Narrowing (I64 -> I32) is also
    // unsupported in v1, and should be flagged at plan time so callers can
    // route to a manual-migration path before invoking apply.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let initial = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let mut db = Omnigraph::init(uri, &initial).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let plan = db.plan_schema(TEST_SCHEMA).await.unwrap();
    assert!(!plan.supported, "narrowing I64 -> I32 must not be supported");
    assert!(plan.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::UnsupportedChange { reason, .. }
            if reason.contains("changing property type")
    )));
}

#[tokio::test]
async fn apply_schema_renames_node_type_via_rename_from_and_preserves_rows() {
    // Covers the stable-type-id contract: renaming a type preserves the
    // underlying Lance dataset (by stable id), so existing rows survive the
    // rename and become queryable under the new table key. This is the
    // "supported" half of the destructive-vs-supported boundary that the
    // rejections above cover.
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    // Rename Person -> Human (and the keying property name -> full_name).
    // Edges that referenced Person must update to Human in the same migration.
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

    let result = db.apply_schema(desired).await.unwrap();
    assert!(result.supported && result.applied);

    // Type rename is emitted as a RenameType step.
    assert!(
        result.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Node,
                from,
                to,
            } if from == "Person" && to == "Human"
        )),
        "expected RenameType Person -> Human in {:?}",
        result.steps
    );
    // Property rename rides along under the new type name.
    assert!(
        result.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::RenameProperty {
                type_kind: SchemaTypeKind::Node,
                type_name,
                from,
                to,
            } if type_name == "Human" && from == "name" && to == "full_name"
        )),
        "expected RenameProperty name -> full_name on Human in {:?}",
        result.steps
    );

    // Rows survive: table key now resolves under the new type name and the
    // old key is gone.
    assert_eq!(count_rows(&db, "node:Human").await, people_before);
    assert!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .is_none(),
        "old node:Person table key should be unmapped after rename"
    );
}
