mod helpers;

use std::fs;

use omnigraph::db::{
    GraphCreateReconciliation, InitOptions, Omnigraph, PreparedGraphCreate, ReadTarget,
};
use omnigraph_compiler::schema::parser::{parse_persisted_schema_contract, parse_schema};
use omnigraph_compiler::{
    SchemaIR, SchemaIdentityDomain, compile_schema_shape, resolve_schema_ir, schema_ir_hash,
    schema_ir_pretty_json,
};

use helpers::*;

fn compile_shape(source: &str) -> omnigraph_compiler::SchemaShape {
    compile_schema_shape(&parse_schema(source).unwrap()).unwrap()
}

fn compile_persisted_shape(source: &str) -> omnigraph_compiler::SchemaShape {
    compile_schema_shape(&parse_persisted_schema_contract(source).unwrap()).unwrap()
}

/// Replace the `schema_contract` row of main's `__manifest` with `source` and
/// `ir` in one publish, as an apply would, without touching any table: the
/// way a test plants a contract the engine did not write.
async fn publish_schema_contract_row(uri: &str, source: &str, ir: &SchemaIR) -> u64 {
    publish_schema_contract_text(uri, source, &schema_ir_pretty_json(ir).unwrap(), ir).await
}

async fn publish_schema_contract_text(
    uri: &str,
    source: &str,
    ir_text: &str,
    ir: &SchemaIR,
) -> u64 {
    let mut manifest = omnigraph_catalog::ManifestCoordinator::open(uri)
        .await
        .unwrap();
    manifest
        .commit_changes(&[omnigraph_catalog::ManifestChange::SchemaContract(
            omnigraph_catalog::SchemaContractRow {
                source: source.to_string(),
                ir: ir_text.to_string(),
                head: omnigraph_catalog::SchemaContractHead {
                    schema_ir_hash: schema_ir_hash(ir).unwrap(),
                    schema_identity_version: 2,
                    schema_identity_domain: ir.schema_identity_domain.as_str().to_string(),
                },
            },
        )])
        .await
        .unwrap()
}

async fn read_contract(uri: &str) -> omnigraph_catalog::SchemaContractRow {
    omnigraph_catalog::ManifestCoordinator::open(uri)
        .await
        .unwrap()
        .read_schema_contract()
        .await
        .unwrap()
}

#[tokio::test]
async fn warm_publisher_preserves_foreign_contract_text_at_the_same_identity() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    let before = read_contract(uri).await;
    let ir: SchemaIR = serde_json::from_str(&before.ir).unwrap();
    let source = format!("\n{}\n", before.source);
    let ir_text = format!("\n{}\n", serde_json::to_string(&ir).unwrap());
    let old_head = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_head(None)
        .map(str::to_owned);
    publish_schema_contract_text(uri, &source, &ir_text, &ir).await;
    let replacement = read_contract(uri).await;
    assert_eq!(replacement.head, before.head);
    assert_ne!(replacement.source, before.source);
    assert_ne!(replacement.ir, before.ir);
    assert_eq!(
        omnigraph_catalog::ManifestCoordinator::open(uri)
            .await
            .unwrap()
            .exact_graph_head(),
        old_head
    );
    let (resolved, io) = helpers::cost::measure(db.resolve_snapshot("main")).await;
    let resolved = resolved.unwrap();
    assert_eq!(Some(resolved.as_str()), old_head.as_deref());
    assert_eq!(io.version_probes, 1);
    assert_eq!(io.internal_open_count, 1);
    assert_eq!(io.manifest_scan_count, 1);
    mutate_main(
        &db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 77)]),
    )
    .await
    .unwrap();
    assert_eq!(read_contract(uri).await, replacement);
    db.refresh().await.unwrap();
    assert_eq!(db.schema_source().as_str(), source);
    let fresh = helpers::session(Omnigraph::open_read_only(uri).await.unwrap());
    for (name, age) in [("Alice", 31), ("Bob", 77)] {
        let result = fresh
            .query(
                ReadTarget::branch("main"),
                TEST_QUERIES,
                "get_person",
                &params(&[("$name", name)]),
            )
            .await
            .unwrap();
        let batch = result.concat_batches().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .value(0),
            age
        );
    }
}

#[tokio::test]
async fn schema_contract_integrity_refuses_mismatched_identity() {
    for corruption in ["source", "ir", "hash", "domain", "version"] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let held = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        held.branch_create("merge_source").await.unwrap();
        held.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let refreshing = Omnigraph::open(uri).await.unwrap();
        let writing = Omnigraph::open(uri).await.unwrap();
        let merging = helpers::session(Omnigraph::open(uri).await.unwrap());
        let mut manifest = omnigraph_catalog::ManifestCoordinator::open(uri)
            .await
            .unwrap();
        let mut row = manifest.read_schema_contract().await.unwrap();
        if corruption == "source" {
            row.source = "node Different { age: String }".to_string();
        } else if corruption == "ir" {
            row.ir = "not valid JSON".to_string();
        } else if corruption == "domain" {
            row.head.schema_identity_domain = SchemaIdentityDomain::from_ulid(ulid::Ulid::new())
                .as_str()
                .to_string();
        } else if corruption == "version" {
            row.head.schema_identity_version += 1;
        } else {
            row.head.schema_ir_hash = "invalid-hash".to_string();
        }
        manifest
            .commit_changes(&[omnigraph_catalog::ManifestChange::SchemaContract(row)])
            .await
            .unwrap();
        let before = manifest.version();
        let expected = match corruption {
            "source" => "source no longer matches",
            "ir" => "schema contract in the schema_contract row is invalid",
            "domain" => "identity domain",
            "version" => "identity version",
            _ => "schema_ir_hash",
        };
        let refresh_error = refreshing.refresh().await.unwrap_err();
        assert!(
            refresh_error.to_string().contains(expected),
            "{refresh_error}"
        );
        let warm_error = held
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap_err();
        assert!(warm_error.to_string().contains(expected), "{warm_error}");
        let write_error = writing.branch_create("must_refuse").await.unwrap_err();
        assert!(write_error.to_string().contains(expected), "{write_error}");
        let merge_error = merging
            .branch_merge("merge_source", "main")
            .await
            .unwrap_err();
        assert!(merge_error.to_string().contains(expected), "{merge_error}");
        for read_only in [true, false] {
            let result = if read_only {
                Omnigraph::open_read_only(uri).await
            } else {
                Omnigraph::open(uri).await
            };
            let error = result.err().expect("corrupt contract must refuse open");
            assert!(error.to_string().contains(expected), "{error}");
        }
        assert_eq!(
            omnigraph_catalog::ManifestCoordinator::open(uri)
                .await
                .unwrap()
                .version(),
            before
        );
    }
}

// Prepared birth is a storage-authority protocol, not query behavior: exercise
// serialization, exact identity and absence of writes through the public engine.
#[tokio::test]
async fn prepared_graph_create_reconciles_only_its_own_exact_genesis() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let prepared = Omnigraph::prepare_graph_create(uri, TEST_SCHEMA)
        .await
        .unwrap();
    let other = Omnigraph::prepare_graph_create(uri, TEST_SCHEMA)
        .await
        .unwrap();
    assert_ne!(prepared.desired_contract(), other.desired_contract());
    assert_ne!(prepared.graph_commit_id(), other.graph_commit_id());
    assert_eq!(
        fs::read_dir(dir.path()).unwrap().count(),
        0,
        "preparation has no storage effects"
    );
    assert_eq!(
        Omnigraph::reconcile_prepared_graph_create(&prepared)
            .await
            .unwrap(),
        GraphCreateReconciliation::Absent
    );
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    let encoded = serde_json::to_vec(&prepared).unwrap();
    let prepared: PreparedGraphCreate = serde_json::from_slice(&encoded).unwrap();
    prepared.validate().unwrap();
    let db = Omnigraph::apply_prepared_graph_create(&prepared)
        .await
        .unwrap();
    assert_eq!(&db.schema_contract_digest(), prepared.desired_contract());
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(snapshot.graph_manifest_version(), 1);
    assert_eq!(snapshot.graph_head(None), Some(prepared.graph_commit_id()));
    assert_eq!(
        Omnigraph::reconcile_prepared_graph_create(&prepared)
            .await
            .unwrap(),
        GraphCreateReconciliation::Created {
            graph_manifest_version: 1,
            contract: prepared.desired_contract().clone(),
        }
    );
    assert_eq!(
        Omnigraph::reconcile_prepared_graph_create(&other)
            .await
            .unwrap(),
        GraphCreateReconciliation::Unknown
    );
    assert!(
        Omnigraph::apply_prepared_graph_create(&other)
            .await
            .is_err()
    );
    assert!(
        Omnigraph::apply_prepared_graph_create(&prepared)
            .await
            .is_err(),
        "reconciliation is not replay"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        1
    );
}

#[tokio::test]
async fn prepared_graph_create_rejects_mutated_serialized_input_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let prepared = Omnigraph::prepare_graph_create(dir.path().to_str().unwrap(), TEST_SCHEMA)
        .await
        .unwrap();
    for (pointer, value) in [
        ("/version", serde_json::json!(2)),
        (
            "/source",
            serde_json::json!("node Different { key: String @key }"),
        ),
        ("/contract/schema_ir_hash", serde_json::json!("wrong")),
        (
            "/contract/schema_identity_domain",
            serde_json::json!("not-a-domain"),
        ),
        (
            "/genesis/lineage/graph_manifest_version",
            serde_json::json!(2),
        ),
        (
            "/genesis/lineage/parent_commit_id",
            serde_json::json!("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        ),
        (
            "/genesis/lineage/graph_commit_id",
            serde_json::json!("invalid"),
        ),
    ] {
        let mut encoded = serde_json::to_value(&prepared).unwrap();
        *encoded.pointer_mut(pointer).unwrap() = value;
        let corrupted: PreparedGraphCreate = serde_json::from_value(encoded).unwrap();
        assert!(corrupted.validate().is_err(), "{pointer}");
        assert!(
            Omnigraph::apply_prepared_graph_create(&corrupted)
                .await
                .is_err(),
            "{pointer}"
        );
        assert!(
            Omnigraph::reconcile_prepared_graph_create(&corrupted)
                .await
                .is_err(),
            "{pointer}"
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0, "{pointer}");
    }
}

#[tokio::test]
async fn init_creates_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Keep the original public struct-literal API as well as the v2 format.
    let db = Omnigraph::init_with_options(uri, TEST_SCHEMA, InitOptions { force: false })
        .await
        .unwrap();

    for name in ["_schema.pg", "_schema.ir.json", "__schema_state.json"] {
        assert!(
            !dir.path().join(name).exists(),
            "init must not write {name}"
        );
    }
    let contract_row = read_contract(uri).await;
    let ir: SchemaIR = serde_json::from_str(&contract_row.ir).unwrap();
    assert_eq!(ir.ir_version, 5);
    assert!(ir.features.contains("system-columns"));
    let persisted: serde_json::Value = serde_json::from_str(&contract_row.ir).unwrap();
    assert!(persisted.get("actor_provenance").is_none());
    assert_eq!(db.schema_source().as_str(), TEST_SCHEMA);
    assert!(ir.next_identity_id > 1);
    assert!(SchemaIdentityDomain::parse(ir.schema_identity_domain.as_str()).is_ok());
    assert_eq!(contract_row.head.schema_identity_version, 2);
    assert_eq!(
        contract_row.head.schema_ir_hash,
        schema_ir_hash(&ir).unwrap()
    );
    assert_eq!(
        contract_row.head.schema_identity_domain,
        ir.schema_identity_domain.as_str()
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
    assert_eq!(
        db.internal_schema_version_of(ReadTarget::branch("main"))
            .await
            .unwrap(),
        14,
        "fresh graphs are stamped v14: current state in `__manifest`, settled commits in `__history`"
    );
    assert_eq!(
        omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
        14,
        "the writer's stamp constant is the released format number"
    );
    assert_eq!(contract_row.source, TEST_SCHEMA);
    assert!(snap.dataset("node:Person").is_some());
    assert!(snap.dataset("node:Company").is_some());
    assert!(snap.dataset("edge:Knows").is_some());
    assert!(snap.dataset("edge:WorksAt").is_some());
    assert_eq!(snap.datasets().count(), 4);
    for table_key in ["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        let dataset = snap.open_dataset(table_key).await.unwrap();
        let primary_key = dataset
            .schema()
            .unenforced_primary_key()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            primary_key,
            ["__id"],
            "fresh graph table {table_key} must be created with exactly `__id` as its Lance unenforced primary key"
        );
        assert!(
            dataset.schema().field("__omnigraph_stream_v1$").is_none(),
            "fresh v6 table {table_key} must not carry abandoned stream metadata"
        );
        assert_stable_property_markers(&db, table_key).await;
    }

    assert!(
        !dir.path().join("_stream_tokens.lance").exists(),
        "fresh v6 roots must not create the abandoned token-authority dataset"
    );
    let mut pending = vec![dir.path().to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert_ne!(
                entry.file_name().to_string_lossy(),
                "_mem_wal",
                "fresh v6 roots must not create MemWAL storage"
            );
            if path.is_dir() {
                pending.push(path);
            }
        }
    }

    assert_eq!(db.catalog().node_types.len(), 2);
    assert_eq!(db.catalog().edge_types.len(), 2);
    assert_eq!(
        db.catalog().node_types["Person"].key_property(),
        Some("name")
    );
}

#[tokio::test]
async fn open_accepts_historical_body_unique_blob_but_init_rejects_it() {
    const BASE_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;
    const HISTORICAL_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    @unique(content)
}
"#;

    let rejected_dir = tempfile::tempdir().unwrap();
    let rejected_uri = rejected_dir.path().to_str().unwrap();
    let error = match Omnigraph::init(rejected_uri, HISTORICAL_SCHEMA).await {
        Ok(_) => panic!("new init must reject body-level @unique(Blob)"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("@unique is not supported on blob property Document.content")
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BASE_SCHEMA).await.unwrap();
    let accepted = db.catalog().bound_schema_ir().unwrap().clone();
    drop(db);

    let historical_ir = resolve_schema_ir(&accepted, &compile_persisted_shape(HISTORICAL_SCHEMA))
        .unwrap()
        .schema_ir;
    publish_schema_contract_row(uri, HISTORICAL_SCHEMA, &historical_ir).await;

    let reopened = Omnigraph::open(uri)
        .await
        .expect("historically admitted v6 schema must remain openable");
    assert_eq!(reopened.schema_source().as_str(), HISTORICAL_SCHEMA);
    assert!(
        reopened
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .dataset("node:Document")
            .is_some()
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
    assert!(snap.dataset("node:Person").is_some());
    assert!(snap.dataset("edge:Knows").is_some());
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
async fn open_refuses_v3_schema_without_changing_files() {
    fn files(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        let mut result = std::collections::BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    result.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(path).unwrap(),
                    );
                }
            }
        }
        result
    }

    // The unsupported version is the boundary, including disabled bindings.
    // Do not require this binary to understand the removed binding shape.
    for (enabled, duplicate_features) in [(true, false), (false, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let accepted = Omnigraph::init(uri, TEST_SCHEMA)
            .await
            .unwrap()
            .catalog()
            .bound_schema_ir()
            .unwrap()
            .clone();
        let mut ir: serde_json::Value =
            serde_json::from_str(&schema_ir_pretty_json(&accepted).unwrap()).unwrap();
        ir["ir_version"] = serde_json::json!(3);
        ir["actor_provenance"] = serde_json::json!({ "enabled": enabled });
        let mut ir_text = serde_json::to_string_pretty(&ir).unwrap();
        if duplicate_features {
            ir_text.insert_str(1, "\"features\": [],");
        }
        publish_schema_contract_text(uri, TEST_SCHEMA, &ir_text, &accepted).await;
        let before = files(dir.path());

        for read_only in [true, false] {
            let opened = if read_only {
                Omnigraph::open_read_only(uri).await
            } else {
                Omnigraph::open(uri).await
            };
            let error = match opened {
                Ok(_) => panic!("unsupported v3 schema must refuse in either open mode"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("unsupported ir_version 3"),
                "{error}"
            );
            assert_eq!(
                files(dir.path()),
                before,
                "refusal must preserve every durable file"
            );
        }
    }
}

#[tokio::test]
async fn open_rejects_same_alias_with_foreign_table_identity() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let accepted = Omnigraph::init(uri, TEST_SCHEMA)
        .await
        .unwrap()
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .clone();
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
    publish_schema_contract_row(uri, TEST_SCHEMA, &replacement).await;

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
    let accepted = Omnigraph::init(uri, TEST_SCHEMA)
        .await
        .unwrap()
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .clone();
    let company_only = r#"
node Company {
    name: String @key
}
"#;
    let replacement = resolve_schema_ir(&accepted, &compile_shape(company_only))
        .unwrap()
        .schema_ir;
    publish_schema_contract_row(uri, company_only, &replacement).await;

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
    publish_schema_contract_row(uri, &with_temporary_source, &replacement).await;

    let err = db
        .refresh()
        .await
        .expect_err("refresh must reject an IR table with no manifest registration");
    assert!(err.to_string().contains("node:Temporary"));
    assert!(err.to_string().contains("is missing from manifest"));
}

#[tokio::test]
async fn write_preparation_manifest_read_failures_carry_before_effect_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, TEST_SCHEMA).await.unwrap());
    db.branch_create("refusal-probe").await.unwrap();
    let before = db.list_commits(None).await.unwrap();
    let branches_before = db.branch_list().await.unwrap();
    let manifest_path = dir.path().join("__manifest");
    let unavailable_path = dir.path().join("manifest-held");
    fs::rename(&manifest_path, &unavailable_path).unwrap();
    let mutation_error = mutate_main(
        &db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "blocked")], &[("$age", 12)]),
    )
    .await
    .unwrap_err();
    let deletion_error = db.branch_delete("refusal-probe").await.unwrap_err();
    let creation_error = db.branch_create("new-probe").await.unwrap_err();
    let creation_from_error = db
        .branch_create_from("main", "from-probe")
        .await
        .unwrap_err();
    let merge_error = db.branch_merge("refusal-probe", "main").await.unwrap_err();
    let data = r#"{"type":"Person","data":{"name":"blocked","age":12}}"#;
    let load_error = db
        .load("main", data, omnigraph::loader::LoadMode::Append)
        .await
        .unwrap_err();
    let graph_batch_error = db
        .load_graph_batch("main", data, omnigraph::loader::LoadMode::Append)
        .await
        .unwrap_err();
    let fork_load_error = db
        .load_as(
            "load-probe",
            Some("main"),
            data,
            omnigraph::loader::LoadMode::Append,
            None,
        )
        .await
        .unwrap_err();
    let schema_error = db.apply_schema(TEST_SCHEMA).await.unwrap_err();
    fs::rename(&unavailable_path, &manifest_path).unwrap();
    for (door, error) in [
        ("mutation", mutation_error),
        ("deletion", deletion_error),
        ("creation", creation_error),
        ("creation-from", creation_from_error),
        ("merge", merge_error),
        ("load", load_error),
        ("graph-batch", graph_batch_error),
        ("fork-load", fork_load_error),
        ("schema", schema_error),
    ] {
        assert_eq!(
            error.completion_evidence(),
            Some(omnigraph::error::CompletionEvidence::BeforeEffect),
            "{door}: a failed manifest authority read before effects must carry owning evidence: {error}"
        );
        assert_eq!(
            error.storage_failure().map(|failure| failure.kind),
            Some(omnigraph::error::StorageFailureKind::NotFound)
        );
    }
    assert_eq!(db.list_commits(None).await.unwrap(), before);
    assert_eq!(db.branch_list().await.unwrap(), branches_before);
    db.branch_delete("refusal-probe").await.unwrap();
}

#[tokio::test]
async fn write_capture_rejects_schema_ir_tables_missing_from_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, TEST_SCHEMA).await.unwrap());
    let accepted = db.catalog().bound_schema_ir().unwrap().clone();
    let with_temporary_source =
        format!("{TEST_SCHEMA}\nnode Temporary {{\n    key: String @key\n}}\n");
    let replacement = resolve_schema_ir(&accepted, &compile_shape(&with_temporary_source))
        .unwrap()
        .schema_ir;
    publish_schema_contract_row(uri, &with_temporary_source, &replacement).await;

    let err = db
        .load_jsonl(
            r#"{"type":"Person","data":{"name":"blocked"}}"#,
            omnigraph::loader::LoadMode::Merge,
        )
        .await
        .expect_err("write preparation must reject schema/manifest identity drift");
    assert!(err.to_string().contains("node:Temporary"));
    assert!(err.to_string().contains("is missing from manifest"));
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

    publish_schema_contract_row(uri, TEST_SCHEMA, &replacement).await;
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
async fn open_nonexistent_fails() {
    let result = Omnigraph::open("/tmp/nonexistent_omnigraph_test_xyz").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn snapshot_version_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let db = helpers::session(Omnigraph::init(uri, TEST_SCHEMA).await.unwrap());

    let snap1 = snapshot_main(&db).await.unwrap();
    let v1 = snap1.graph_manifest_version();

    db.load_jsonl(
        r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
        omnigraph::loader::LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let snap2 = snapshot_main(&db).await.unwrap();
    assert!(snap2.graph_manifest_version() > v1);

    assert_eq!(snap1.graph_manifest_version(), v1);
}

#[tokio::test]
async fn init_on_existing_graph_uri_does_not_destroy_existing_schema() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let before = read_contract(uri).await;
    assert!(
        Omnigraph::init(uri, "node Other { id: String @key }\n")
            .await
            .is_err()
    );
    assert_eq!(read_contract(uri).await, before);
}

#[tokio::test]
async fn force_init_refuses_existing_manifest_and_preserves_identity_contract() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let before = read_contract(uri).await;

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
    assert_eq!(read_contract(uri).await, before);
}

#[tokio::test]
async fn init_ignores_orphan_schema_files() {
    for force in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        fs::write(dir.path().join("_schema.pg"), "orphan source").unwrap();
        let db = Omnigraph::init_with_options(uri, TEST_SCHEMA, InitOptions { force })
            .await
            .unwrap();
        assert!(db.catalog().node_types.contains_key("Person"));
        assert_eq!(read_contract(uri).await.source, TEST_SCHEMA);
        assert_eq!(
            fs::read_to_string(dir.path().join("_schema.pg")).unwrap(),
            "orphan source"
        );
    }
}

/// Schema annotations persist in the contract row and catalog metadata.
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

    let ir_json = read_contract(uri).await.ir;
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

/// The local backend implements create-if-absent with `hard_link(2)`, which
/// some filesystems refuse (Android app storage, FAT/exFAT — issue #453).
/// Read-write binds probe the capability at the graph root before any claim,
/// migration, or Lance commit can fail mid-flight on such a filesystem.
mod local_create_if_absent_probe {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use omnigraph::db::Omnigraph;
    use omnigraph::error::{OmniError, Result};
    use omnigraph::storage::{ListDirBounds, StorageAdapter, storage_for_uri};

    use super::helpers;

    const PROBE_FILENAME_PREFIX: &str = "__create_if_absent_probe";
    const SENTINEL: &str = "capability probe refused by test adapter";

    /// Local-adapter decorator that refuses the create-if-absent capability
    /// probe, simulating a filesystem without hard-link support.
    #[derive(Debug)]
    struct ProbeRefusingAdapter {
        inner: Arc<dyn StorageAdapter>,
        collide_once: bool,
        probe_attempts: AtomicUsize,
        deleted_probe: AtomicBool,
    }

    impl ProbeRefusingAdapter {
        fn wrap(inner: Arc<dyn StorageAdapter>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                collide_once: false,
                probe_attempts: AtomicUsize::new(0),
                deleted_probe: AtomicBool::new(false),
            })
        }

        fn wrap_with_collision(inner: Arc<dyn StorageAdapter>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                collide_once: true,
                probe_attempts: AtomicUsize::new(0),
                deleted_probe: AtomicBool::new(false),
            })
        }

        fn is_probe(uri: &str) -> bool {
            uri.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with(PROBE_FILENAME_PREFIX))
        }
    }

    #[async_trait]
    impl StorageAdapter for ProbeRefusingAdapter {
        async fn read_text(&self, uri: &str) -> Result<String> {
            self.inner.read_text(uri).await
        }

        async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>> {
            self.inner.read_text_if_exists(uri).await
        }

        async fn read_text_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> Result<Option<String>> {
            self.inner.read_text_if_exists_bounded(uri, max_bytes).await
        }

        async fn read_bytes_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> Result<Option<Vec<u8>>> {
            self.inner
                .read_bytes_if_exists_bounded(uri, max_bytes)
                .await
        }

        async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
            self.inner.write_text(uri, contents).await
        }

        async fn write_bytes(&self, uri: &str, contents: &[u8]) -> Result<()> {
            self.inner.write_bytes(uri, contents).await
        }

        async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
            if Self::is_probe(uri) {
                let attempt = self.probe_attempts.fetch_add(1, Ordering::Relaxed);
                if self.collide_once && attempt == 0 {
                    return Ok(false);
                }
                return Err(OmniError::manifest_internal(SENTINEL));
            }
            self.inner.write_text_if_absent(uri, contents).await
        }

        async fn exists(&self, uri: &str) -> Result<bool> {
            self.inner.exists(uri).await
        }

        async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
            self.inner.rename_text(from_uri, to_uri).await
        }

        async fn delete(&self, uri: &str) -> Result<()> {
            if Self::is_probe(uri) {
                self.deleted_probe.store(true, Ordering::Relaxed);
            }
            self.inner.delete(uri).await
        }

        async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
            self.inner.list_dir(dir_uri).await
        }

        async fn list_dir_bounded(
            &self,
            dir_uri: &str,
            matching_suffix: &str,
            bounds: ListDirBounds,
        ) -> Result<Vec<String>> {
            self.inner
                .list_dir_bounded(dir_uri, matching_suffix, bounds)
                .await
        }

        async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
            self.inner.read_text_versioned(uri).await
        }

        async fn write_text_if_match(
            &self,
            uri: &str,
            contents: &str,
            expected_version: &str,
        ) -> Result<Option<String>> {
            self.inner
                .write_text_if_match(uri, contents, expected_version)
                .await
        }

        async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
            self.inner.delete_prefix(prefix_uri).await
        }
    }

    /// A read-write open on a local root runs the create-if-absent probe before any
    /// coordinator work, and a probe failure aborts the open with that error.
    #[tokio::test]
    async fn read_write_open_fails_fast_when_create_if_absent_probe_fails() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let _ = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();

        let adapter = ProbeRefusingAdapter::wrap(storage_for_uri(uri).unwrap());
        let err = match Omnigraph::open_with_storage(uri, adapter).await {
            Ok(_) => panic!("read-write open must fail when the create-if-absent probe fails"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains(SENTINEL),
            "open must surface the probe failure, got: {err}"
        );
    }

    /// An already-existing candidate belongs to a prior or foreign writer. It
    /// proves nothing about this bind's hard-link capability and must neither
    /// be accepted nor deleted; the bind retries with a fresh owned name.
    #[tokio::test]
    async fn read_write_open_retries_without_deleting_a_colliding_probe() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let _ = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();

        let adapter = ProbeRefusingAdapter::wrap_with_collision(storage_for_uri(uri).unwrap());
        let err = match Omnigraph::open_with_storage(uri, adapter.clone()).await {
            Ok(_) => panic!("a colliding probe must not bypass the capability check"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains(SENTINEL),
            "the fresh retry must surface its capability failure, got: {err}"
        );
        assert_eq!(
            adapter.probe_attempts.load(Ordering::Relaxed),
            2,
            "the bind must retry once with a fresh candidate"
        );
        assert!(
            !adapter.deleted_probe.load(Ordering::Relaxed),
            "the bind must not delete a probe candidate it did not create"
        );
    }

    /// The probe object is removed before the open returns; it never persists
    /// as residue in the graph root.
    #[tokio::test]
    async fn read_write_open_leaves_no_probe_residue() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let _ = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();

        let db = Omnigraph::open(uri).await.unwrap();
        drop(db);
        assert!(
            std::fs::read_dir(dir.path()).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(PROBE_FILENAME_PREFIX)
            }),
            "capability probe must clean up after itself"
        );
    }
}
