mod helpers;

use std::fs;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_compiler::{build_schema_ir, schema_ir_pretty_json};
use omnigraph_compiler::schema::parser::parse_schema;

use helpers::*;

#[tokio::test]
async fn init_creates_repo() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    assert!(dir.path().join("_schema.pg").exists());
    assert!(dir.path().join("_schema.ir.json").exists());
    assert!(dir.path().join("__schema_state.json").exists());

    let snap = snapshot_main(&db).await.unwrap();
    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("node:Company").is_some());
    assert!(snap.entry("edge:Knows").is_some());
    assert!(snap.entry("edge:WorksAt").is_some());

    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
    assert_eq!(
        db.catalog().node_types["Person"].key_property(),
        Some("name")
    );
}

#[tokio::test]
async fn open_reads_existing_repo() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
    let snap = snapshot_main(&db).await.unwrap();
    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("edge:Knows").is_some());
}

#[tokio::test]
async fn open_bootstraps_legacy_schema_state_for_main_only_repo() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    fs::remove_file(dir.path().join("_schema.ir.json")).unwrap();
    fs::remove_file(dir.path().join("__schema_state.json")).unwrap();

    let db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(db.catalog().node_types.len(), 2);
    assert!(dir.path().join("_schema.ir.json").exists());
    assert!(dir.path().join("__schema_state.json").exists());
}

#[tokio::test]
async fn open_rejects_legacy_repo_with_public_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    db.branch_create("feature").await.unwrap();

    fs::remove_file(dir.path().join("_schema.ir.json")).unwrap();
    fs::remove_file(dir.path().join("__schema_state.json")).unwrap();

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("expected legacy repo with public branch to fail schema bootstrap"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("public branches block schema evolution entirely")
    );
}

#[tokio::test]
async fn long_lived_handle_rejects_schema_source_drift() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let drifted = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    fs::write(dir.path().join("_schema.pg"), drifted).unwrap();

    let err = match db.snapshot_of(ReadTarget::branch("main")).await {
        Ok(_) => panic!("expected schema source drift to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("current _schema.pg no longer matches the accepted compiled schema")
    );
}

#[tokio::test]
async fn long_lived_handle_rejects_schema_ir_drift() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    fs::write(dir.path().join("_schema.ir.json"), "{not valid json").unwrap();

    let err = match db.snapshot_of(ReadTarget::branch("main")).await {
        Ok(_) => panic!("expected schema IR drift to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("accepted compiled schema contract in _schema.ir.json is invalid")
    );
}

#[tokio::test]
async fn long_lived_handle_rejects_ir_and_source_updates_without_state_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let drifted = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let drifted_ast = parse_schema(&drifted).unwrap();
    let drifted_ir = build_schema_ir(&drifted_ast).unwrap();
    let drifted_ir_json = schema_ir_pretty_json(&drifted_ir).unwrap();
    fs::write(dir.path().join("_schema.pg"), drifted).unwrap();
    fs::write(dir.path().join("_schema.ir.json"), drifted_ir_json).unwrap();

    let err = match db.snapshot_of(ReadTarget::branch("main")).await {
        Ok(_) => panic!("expected schema state mismatch to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("accepted compiled schema does not match the recorded schema state")
    );
}

#[tokio::test]
async fn comment_only_schema_edit_keeps_schema_state_valid() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let commented = format!("// comment-only drift\n{}", TEST_SCHEMA);
    fs::write(dir.path().join("_schema.pg"), commented).unwrap();

    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(snapshot.entry("node:Person").is_some());
}

#[tokio::test]
async fn open_nonexistent_fails() {
    let result = Omnigraph::open("/tmp/nonexistent_omnigraph_test_xyz").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn snapshot_version_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let snap1 = snapshot_main(&db).await.unwrap();
    let v1 = snap1.version();

    omnigraph::loader::load_jsonl(
        &mut db,
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
        omnigraph::loader::LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let snap2 = snapshot_main(&db).await.unwrap();
    assert!(snap2.version() > v1);

    assert_eq!(snap1.version(), v1);
}
