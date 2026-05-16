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

// ─── Destructive / safety-tier behavior ──────────────────────────────────────
//
// Schema migration v1 accepts:
// - Additive change: add type, add nullable property, add index, rename.
// - DropProperty { Soft } via the schema-lint v1 chassis (commit #3 of MR-694)
//   — the dropped column is removed from the current manifest version but
//   remains reachable via Lance time travel at the prior version, until
//   `omnigraph cleanup` runs. Hard mode (immediate data cleanup) lands in
//   commit #5 gated by `--allow-data-loss`.
//
// Every other destructive shape (drop type, narrow type, add required without
// backfill, remove constraint) still returns an `UnsupportedChange` step that
// surfaces as an error from `apply_schema`. These tests pin the current
// contract so a regression in the planner can't silently change behavior.

#[tokio::test]
async fn apply_schema_drops_a_nullable_property_softly_preserves_prior_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop `age` from Person. v1 + chassis commit #3 emit
    // `DropProperty { Soft }`; the rewrite path projects to the
    // target schema (no `age`), commits via stage_overwrite. Row
    // counts are unchanged — only the column is dropped from the
    // current schema view.
    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");

    // Confirm the plan emits DropProperty { Soft } (not UnsupportedChange).
    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported, "drop-property plan must be supported");
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty {
                type_kind: SchemaTypeKind::Node,
                type_name,
                property_name,
                mode: omnigraph_compiler::DropMode::Soft,
                ..
            } if type_name == "Person" && property_name == "age"
        )),
        "expected DropProperty {{ type=Person, property=age, mode=Soft }} in plan; got {plan:?}",
    );

    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.supported);
    assert!(result.applied);

    // Manifest advanced; row count unchanged.
    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    assert!(
        after_version > before_version,
        "manifest version should advance after soft drop; before={before_version}, after={after_version}",
    );
    assert_eq!(count_rows(&db, "node:Person").await, people_before);

    // (a) Current snapshot: `age` is gone from the dataset schema.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let current_ds = current_snapshot.open("node:Person").await.unwrap();
    let current_fields = current_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !current_fields.iter().any(|f| f == "age"),
        "current Person dataset schema must not include 'age' after soft drop; got fields {current_fields:?}",
    );

    // (b) Time travel: at the pre-drop manifest version, the prior
    // Person dataset version still has `age`. Soft drop is reversible
    // via Lance's version graph until `omnigraph cleanup` runs.
    let pre_drop_snapshot = db.snapshot_at_version(before_version).await.unwrap();
    let pre_drop_ds = pre_drop_snapshot.open("node:Person").await.unwrap();
    let pre_drop_fields = pre_drop_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        pre_drop_fields.iter().any(|f| f == "age"),
        "pre-drop Person dataset schema must still include 'age' (time-travel reversibility); got fields {pre_drop_fields:?}",
    );

    // (c) Reopen consistency: close the engine, reopen, verify the
    // drop is preserved (column still absent from current schema).
    let uri = dir.path().to_str().unwrap().to_string();
    drop(db);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let reopened_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let reopened_ds = reopened_snapshot.open("node:Person").await.unwrap();
    let reopened_fields = reopened_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !reopened_fields.iter().any(|f| f == "age"),
        "after reopen, Person dataset schema must still lack 'age'; got fields {reopened_fields:?}",
    );
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
        msg.contains("OG-DS-102") || msg.contains("OG-DS-103"),
        "expected schema-lint code OG-DS-102 or OG-DS-103 in error, got: {msg}"
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
    let msg = err.to_string();
    assert!(
        msg.contains("OG-DS-103"),
        "expected schema-lint code OG-DS-103 in error, got: {msg}"
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
    let msg = err.to_string();
    assert!(
        msg.contains("OG-MF-103"),
        "expected schema-lint code OG-MF-103 in error, got: {msg}"
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
        SchemaMigrationStep::UnsupportedChange { code, .. }
            if code.as_deref() == Some("OG-MF-106")
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
