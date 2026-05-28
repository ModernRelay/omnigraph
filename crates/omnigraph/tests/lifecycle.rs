mod helpers;

use std::fs;

use omnigraph::db::{InitOptions, Omnigraph, ReadTarget};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{build_schema_ir, schema_ir_pretty_json};

use helpers::*;

#[tokio::test]
async fn init_creates_graph() {
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
async fn open_reads_existing_graph() {
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
async fn open_bootstraps_legacy_schema_state_for_main_only_graph() {
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
async fn open_rejects_legacy_graph_with_public_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    db.branch_create("feature").await.unwrap();

    fs::remove_file(dir.path().join("_schema.ir.json")).unwrap();
    fs::remove_file(dir.path().join("__schema_state.json")).unwrap();

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("expected legacy graph with public branch to fail schema bootstrap"),
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

/// Regression for the `Omnigraph::init` re-init footgun (MR-668
/// follow-up): a second `init` against a URI that already holds a
/// graph must NOT modify or destroy the existing graph's schema
/// artifacts. Today's behavior is destructive either way — the
/// `write_text(_schema.pg, ...)` call at the top of
/// `init_storage_phase` overwrites the existing file before any
/// preflight, and `best_effort_cleanup_init_artifacts` will later
/// delete all three files if the inner `GraphCoordinator::init`
/// fails. Both outcomes corrupt an existing graph.
///
/// After the fix: strict-mode `init` (no `force` flag) errors out
/// before touching any file, and the original schema artifacts
/// match their pre-attempt contents byte-for-byte.
#[tokio::test]
async fn init_on_existing_graph_uri_does_not_destroy_existing_schema() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Establish the first graph and snapshot its three schema files.
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let original_schema_pg = fs::read_to_string(dir.path().join("_schema.pg")).unwrap();
    let original_schema_ir = fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap();
    let original_schema_state = fs::read_to_string(dir.path().join("__schema_state.json")).unwrap();

    // Attempt a re-init with a deliberately different schema so any
    // overwrite would be observable in the file contents.
    let different_schema = "node Other { id: String @key }\n";
    let result = Omnigraph::init(uri, different_schema).await;

    // The new init must report the conflict, not silently mutate.
    assert!(
        result.is_err(),
        "init against an existing graph URI must error, not silently overwrite"
    );

    // The three schema files must remain present and byte-identical to
    // their pre-attempt contents.
    assert!(
        dir.path().join("_schema.pg").exists(),
        "_schema.pg must not be deleted by a failed re-init"
    );
    assert!(
        dir.path().join("_schema.ir.json").exists(),
        "_schema.ir.json must not be deleted by a failed re-init"
    );
    assert!(
        dir.path().join("__schema_state.json").exists(),
        "__schema_state.json must not be deleted by a failed re-init"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("_schema.pg")).unwrap(),
        original_schema_pg,
        "_schema.pg contents must be preserved when re-init is rejected"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap(),
        original_schema_ir,
        "_schema.ir.json contents must be preserved when re-init is rejected"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("__schema_state.json")).unwrap(),
        original_schema_state,
        "__schema_state.json contents must be preserved when re-init is rejected"
    );
}

/// Happy-path sibling to the strict re-init regression above:
/// `InitOptions { force: true }` must skip the schema-file preflight
/// when the operator deliberately wants to recover from orphan
/// schema artifacts (e.g. files left behind by a failed prior init).
///
/// Documented semantics per `InitOptions::force`: skips the preflight
/// only. Force does NOT purge existing Lance datasets or `__manifest/`
/// — that needs `StorageAdapter::delete_prefix`, which is tracked
/// separately. The realistic recovery scenario is "schema files
/// exist but Lance state doesn't," which this test reproduces.
///
/// Without this test, a future refactor could invert the `if !force`
/// branch and silently break the operator-facing escape hatch.
#[tokio::test]
async fn init_with_force_recovers_from_orphan_schema_files() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Simulate orphan schema files: write `_schema.pg` to disk
    // without running a full init. The preflight will see it and
    // bail in strict mode.
    fs::write(dir.path().join("_schema.pg"), TEST_SCHEMA).unwrap();

    // Strict mode refuses because `_schema.pg` exists.
    let strict_err = match Omnigraph::init(uri, TEST_SCHEMA).await {
        Ok(_) => panic!("strict init must refuse when orphan _schema.pg exists"),
        Err(e) => e,
    };
    assert!(
        strict_err.to_string().contains("already initialized"),
        "strict init must surface AlreadyInitialized (sanity check); got: {strict_err}"
    );

    // Force init succeeds: it skips the preflight, overwrites the
    // orphan file, and proceeds to initialize Lance state (which
    // didn't exist, so `GraphCoordinator::init` is unblocked).
    let db = Omnigraph::init_with_options(uri, TEST_SCHEMA, InitOptions { force: true })
        .await
        .expect("force init must succeed when only orphan schema files block strict init");

    // Confirm the catalog is populated as expected — proves the
    // graph is functional after force-recovery, not just that the
    // call returned Ok.
    assert!(
        db.catalog().node_types.contains_key("Person"),
        "force-recovered graph must have the new catalog installed"
    );
    assert!(
        dir.path().join("__schema_state.json").exists(),
        "force-recovered graph must have full schema state written"
    );
}
