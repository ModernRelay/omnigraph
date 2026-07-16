mod helpers;

use std::fs;

use omnigraph::db::{InitOptions, Omnigraph, ReadTarget};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    SchemaIR, SchemaIdentityDomain, compile_schema_shape, resolve_schema_ir, schema_ir_hash,
    schema_ir_pretty_json, schema_shape_hash, schema_shape_hash_from_ir,
};

use helpers::*;

fn compile_shape(source: &str) -> omnigraph_compiler::SchemaShape {
    compile_schema_shape(&parse_schema(source).unwrap()).unwrap()
}

fn schema_state_json(ir: &SchemaIR) -> serde_json::Value {
    serde_json::json!({
        "format_version": 2,
        "schema_shape_hash": schema_shape_hash_from_ir(ir).unwrap(),
        "schema_ir_hash": schema_ir_hash(ir).unwrap(),
        "schema_identity_version": 2,
        "schema_identity_domain": ir.schema_identity_domain.as_str(),
    })
}

fn persist_schema_contract(root: &std::path::Path, ir: &SchemaIR) {
    fs::write(
        root.join("_schema.ir.json"),
        schema_ir_pretty_json(ir).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("__schema_state.json"),
        serde_json::to_string_pretty(&schema_state_json(ir)).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn init_creates_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    assert!(dir.path().join("_schema.pg").exists());
    assert!(dir.path().join("_schema.ir.json").exists());
    assert!(dir.path().join("__schema_state.json").exists());

    let ir: SchemaIR =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("__schema_state.json")).unwrap())
            .unwrap();
    assert_eq!(ir.ir_version, 2);
    assert!(ir.next_identity_id > 1);
    assert!(SchemaIdentityDomain::parse(ir.schema_identity_domain.as_str()).is_ok());
    assert_eq!(state["format_version"].as_u64(), Some(2));
    assert_eq!(state["schema_identity_version"].as_u64(), Some(2));
    assert_eq!(
        state["schema_ir_hash"].as_str(),
        Some(schema_ir_hash(&ir).unwrap().as_str())
    );
    assert_eq!(
        state["schema_shape_hash"].as_str(),
        Some(
            schema_shape_hash(&compile_shape(TEST_SCHEMA))
                .unwrap()
                .as_str()
        )
    );
    assert_eq!(
        state["schema_identity_domain"].as_str(),
        Some(ir.schema_identity_domain.as_str())
    );
    assert_eq!(
        db.catalog()
            .bound_schema_ir()
            .unwrap()
            .schema_identity_domain
            .as_str(),
        ir.schema_identity_domain.as_str()
    );

    let snap = snapshot_main(&db).await.unwrap();
    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("node:Company").is_some());
    assert!(snap.entry("edge:Knows").is_some());
    assert!(snap.entry("edge:WorksAt").is_some());
    for table_key in ["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        let dataset = snap.open(table_key).await.unwrap();
        let primary_key = dataset
            .schema()
            .unenforced_primary_key()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            primary_key,
            ["id"],
            "fresh graph table {table_key} must be created with exactly `id` as its Lance unenforced primary key"
        );
    }

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

    let created = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let person_type_id = created.catalog().type_id("Person").unwrap();
    let person_name_id = created.catalog().property_id("Person", "name").unwrap();
    let identity_domain = created
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .schema_identity_domain
        .as_str()
        .to_string();
    drop(created);

    let db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
    let snap = snapshot_main(&db).await.unwrap();
    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("edge:Knows").is_some());
    assert_eq!(db.catalog().type_id("Person"), Some(person_type_id));
    assert_eq!(
        db.catalog().property_id("Person", "name"),
        Some(person_name_id)
    );
    assert_eq!(
        db.catalog()
            .bound_schema_ir()
            .unwrap()
            .schema_identity_domain
            .as_str(),
        identity_domain.as_str()
    );
}

#[tokio::test]
async fn open_refuses_missing_identity_contract_without_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    fs::remove_file(dir.path().join("_schema.ir.json")).unwrap();
    fs::remove_file(dir.path().join("__schema_state.json")).unwrap();

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("open must not reconstruct stable IDs from mutable source names"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("automatic bootstrap is not supported")
    );
    assert!(!dir.path().join("_schema.ir.json").exists());
    assert!(!dir.path().join("__schema_state.json").exists());
}

#[tokio::test]
async fn open_refuses_partial_identity_contract() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    fs::remove_file(dir.path().join("__schema_state.json")).unwrap();

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("open must reject a partial identity contract"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("schema contract is incomplete"));
    assert!(dir.path().join("_schema.ir.json").exists());
    assert!(!dir.path().join("__schema_state.json").exists());
}

#[tokio::test]
async fn open_refuses_pre_identity_schema_state_format() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let state_path = dir.path().join("__schema_state.json");
    let mut state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap()).unwrap();
    state["format_version"] = serde_json::json!(1);
    fs::write(&state_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("schema-state v1 must not be served as identity-capable state"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("schema state format 1 is unsupported")
    );
}

#[tokio::test]
async fn open_rejects_same_alias_with_foreign_table_identity() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let accepted: SchemaIR =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    let company_only = r#"
node Company {
    name: String @key
}
"#;
    let after_drop = resolve_schema_ir(&accepted, &compile_shape(company_only))
        .unwrap()
        .schema_ir;
    let replacement = resolve_schema_ir(&after_drop, &compile_shape(TEST_SCHEMA))
        .unwrap()
        .schema_ir;
    assert_ne!(
        replacement
            .nodes
            .iter()
            .find(|node| node.name == "Person")
            .unwrap()
            .type_id,
        accepted
            .nodes
            .iter()
            .find(|node| node.name == "Person")
            .unwrap()
            .type_id
    );
    persist_schema_contract(dir.path(), &replacement);

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("open must reject a same-name table with a foreign stable identity"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("accepted schema/manifest identity mismatch")
    );
    assert!(err.to_string().contains("has identity"));
    assert!(err.to_string().contains("accepted SchemaIR requires"));
}

#[tokio::test]
async fn open_rejects_live_manifest_tables_absent_from_schema_ir() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let accepted: SchemaIR =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    let company_only = r#"
node Company {
    name: String @key
}
"#;
    let replacement = resolve_schema_ir(&accepted, &compile_shape(company_only))
        .unwrap()
        .schema_ir;
    fs::write(dir.path().join("_schema.pg"), company_only).unwrap();
    persist_schema_contract(dir.path(), &replacement);

    let err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("open must reject manifest tables omitted by accepted SchemaIR"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("contains live table"));
    assert!(err.to_string().contains("absent from accepted SchemaIR"));
}

#[tokio::test]
async fn refresh_rejects_schema_ir_tables_missing_from_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let accepted = db.catalog().bound_schema_ir().unwrap().clone();
    let with_temporary_source =
        format!("{TEST_SCHEMA}\nnode Temporary {{\n    key: String @key\n}}\n");
    let replacement = resolve_schema_ir(&accepted, &compile_shape(&with_temporary_source))
        .unwrap()
        .schema_ir;
    fs::write(dir.path().join("_schema.pg"), with_temporary_source).unwrap();
    persist_schema_contract(dir.path(), &replacement);

    let err = db
        .refresh()
        .await
        .expect_err("refresh must reject an IR table with no manifest registration");
    assert!(err.to_string().contains("node:Temporary"));
    assert!(err.to_string().contains("is missing from manifest"));
}

#[tokio::test]
async fn write_capture_rejects_schema_ir_tables_missing_from_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let accepted = db.catalog().bound_schema_ir().unwrap().clone();
    let with_temporary_source =
        format!("{TEST_SCHEMA}\nnode Temporary {{\n    key: String @key\n}}\n");
    let replacement = resolve_schema_ir(&accepted, &compile_shape(&with_temporary_source))
        .unwrap()
        .schema_ir;
    fs::write(dir.path().join("_schema.pg"), with_temporary_source).unwrap();
    persist_schema_contract(dir.path(), &replacement);

    let err = omnigraph::loader::load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"blocked"}}"#,
        omnigraph::loader::LoadMode::Merge,
    )
    .await
    .expect_err("write preparation must reject schema/manifest identity drift");
    assert!(err.to_string().contains("node:Temporary"));
    assert!(err.to_string().contains("is missing from manifest"));
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
    let accepted_ir: SchemaIR =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    let drifted_ir = resolve_schema_ir(&accepted_ir, &compile_shape(&drifted))
        .unwrap()
        .schema_ir;
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
async fn long_lived_handle_rejects_schema_state_domain_drift() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let state_path = dir.path().join("__schema_state.json");
    let mut state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap()).unwrap();
    state["schema_identity_domain"] =
        serde_json::Value::String(SchemaIdentityDomain::new().as_str().to_string());
    fs::write(&state_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();

    let err = match db.snapshot_of(ReadTarget::branch("main")).await {
        Ok(_) => panic!("expected schema identity-domain drift to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("identity domain does not match the recorded schema state")
    );
}

#[tokio::test]
async fn refresh_detects_identity_aba_when_source_bytes_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let accepted = db.catalog().bound_schema_ir().unwrap().clone();
    // Add and then drop a temporary type. The final source shape and every
    // surviving table identity return to their original values, but the
    // monotonic allocator proves the accepted identity history changed.
    let with_temporary_source =
        format!("{TEST_SCHEMA}\nnode Temporary {{\n    key: String @key\n}}\n");
    let with_temporary = resolve_schema_ir(&accepted, &compile_shape(&with_temporary_source))
        .unwrap()
        .schema_ir;
    let replacement = resolve_schema_ir(&with_temporary, &compile_shape(TEST_SCHEMA))
        .unwrap()
        .schema_ir;
    assert_eq!(
        replacement.schema_identity_domain,
        accepted.schema_identity_domain
    );
    assert!(replacement.next_identity_id > accepted.next_identity_id);
    assert_ne!(
        schema_ir_hash(&replacement).unwrap(),
        schema_ir_hash(&accepted).unwrap()
    );

    fs::write(
        dir.path().join("_schema.ir.json"),
        schema_ir_pretty_json(&replacement).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.path().join("__schema_state.json"),
        serde_json::to_string_pretty(&schema_state_json(&replacement)).unwrap(),
    )
    .unwrap();

    // `_schema.pg` is deliberately byte-identical. A source-byte fast path
    // would preserve the stale identity allocator and bound catalog here.
    db.refresh().await.unwrap();
    let refreshed = db.catalog();
    let refreshed_ir = refreshed.bound_schema_ir().unwrap();
    assert_eq!(refreshed_ir.next_identity_id, replacement.next_identity_id);
    assert_eq!(
        schema_ir_hash(refreshed_ir).unwrap(),
        schema_ir_hash(&replacement).unwrap()
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

#[tokio::test]
async fn force_init_refuses_existing_manifest_and_preserves_identity_contract() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let schema_path = dir.path().join("_schema.pg");
    let ir_path = dir.path().join("_schema.ir.json");
    let state_path = dir.path().join("__schema_state.json");
    let before = [
        fs::read_to_string(&schema_path).unwrap(),
        fs::read_to_string(&ir_path).unwrap(),
        fs::read_to_string(&state_path).unwrap(),
    ];

    let err = match Omnigraph::init_with_options(
        uri,
        "node Replacement { key: String @key }\n",
        InitOptions { force: true },
    )
    .await
    {
        Ok(_) => panic!("force init must not rebind an existing manifest to a new identity domain"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("force init refuses graph root"));
    assert_eq!(fs::read_to_string(schema_path).unwrap(), before[0]);
    assert_eq!(fs::read_to_string(ir_path).unwrap(), before[1]);
    assert_eq!(fs::read_to_string(state_path).unwrap(), before[2]);
}

/// Happy-path sibling to the strict re-init regression above:
/// `InitOptions { force: true }` may replace orphan schema artifacts when the
/// operator deliberately recovers from a failed prior init.
///
/// Force does not purge Lance state and refuses any existing `__manifest`.
/// The supported recovery scenario is therefore "schema files exist but Lance
/// state doesn't," which this test reproduces.
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

    // Force init succeeds after proving no manifest exists, overwrites the
    // orphan file, and proceeds to initialize Lance state.
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

/// E2e for the schema-level `.pg` surface: `@description` (node / edge /
/// property) and `@instruction` (node / edge only) parse, validate, and
/// persist verbatim into the on-disk `_schema.ir.json` through `Omnigraph::init`
/// — the contract that surfaces them in catalog metadata for tooling.
#[tokio::test]
async fn schema_annotations_persist_into_ir_json_on_init() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let schema = r#"
node Task @description("Tracked work item") @instruction("Prefer querying by slug") {
    slug: String @key @description("Stable external identifier")
}

edge DependsOn: Task -> Task @description("Hard dependency") @instruction("Use only for blockers")
"#;

    Omnigraph::init(uri, schema).await.unwrap();

    let ir_json = fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap();
    let ir: serde_json::Value = serde_json::from_str(&ir_json).unwrap();

    // Helper: collect the {name -> value} map of annotations that carry a
    // string value. Value-less annotations (e.g. `@key`, which also desugars
    // to a constraint) are skipped — they aren't what this test asserts.
    let anns = |v: &serde_json::Value| -> std::collections::BTreeMap<String, String> {
        v["annotations"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|a| {
                Some((
                    a["name"].as_str()?.to_string(),
                    a["value"].as_str()?.to_string(),
                ))
            })
            .collect()
    };

    let node = ir["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "Task")
        .unwrap();
    let node_anns = anns(node);
    assert_eq!(
        node_anns.get("description").map(String::as_str),
        Some("Tracked work item")
    );
    assert_eq!(
        node_anns.get("instruction").map(String::as_str),
        Some("Prefer querying by slug"),
        "node @instruction persists into _schema.ir.json"
    );

    let prop = node["properties"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "slug")
        .unwrap();
    assert_eq!(
        anns(prop).get("description").map(String::as_str),
        Some("Stable external identifier"),
        "property @description persists into _schema.ir.json"
    );

    let edge = ir["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "DependsOn")
        .unwrap();
    let edge_anns = anns(edge);
    assert_eq!(
        edge_anns.get("description").map(String::as_str),
        Some("Hard dependency")
    );
    assert_eq!(
        edge_anns.get("instruction").map(String::as_str),
        Some("Use only for blockers")
    );
}

/// `@instruction` is rejected on a property at compile time, so init aborts
/// before any graph state is written (mirrors the parser-level rejection from
/// the full engine boundary).
#[tokio::test]
async fn init_rejects_instruction_on_property() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let schema = r#"
node Task {
    slug: String @key @instruction("bad")
}
"#;

    // `Omnigraph` is not `Debug`, so match rather than `unwrap_err`.
    let err = match Omnigraph::init(uri, schema).await {
        Ok(_) => panic!("property-level @instruction must abort init"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("@instruction is only supported on node and edge types"),
        "property-level @instruction must abort init: {err}"
    );
    assert!(
        !dir.path().join("_schema.ir.json").exists(),
        "rejected init must not persist a schema IR"
    );
}
