//! `omnigraph-catalog` tests that drive engine types, moved here by the crate split.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    SchemaIdentityDomain, build_catalog_from_ir, compile_schema_shape, initialize_schema_ir,
};
use omnigraph_core::metadata::table_version_metadata_for_state;

#[cfg(feature = "failpoints")]
use crate::db::Omnigraph;
use crate::db::manifest::layout::open_manifest_dataset;
use crate::db::manifest::namespace::branch_manifest_namespace;
use crate::db::manifest::publisher::{GraphNamespacePublisher, ManifestBatchPublisher};
use crate::db::manifest::*;
use crate::error::Result;
#[cfg(feature = "failpoints")]
use crate::seams::catalog;

#[tokio::test]
async fn detached_branch_pins_without_etags_isolate_handles_writes_and_topology() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let graph = Arc::new(
        crate::db::Omnigraph::init(
            root,
            r#"
node Person { name: String @key }
edge Knows: Person -> Person {}
"#,
        )
        .await
        .unwrap(),
    );
    let session = crate::Session::from_defaults(
        Arc::clone(&graph),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
    session
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"a"}}
{"type":"Person","data":{"name":"b"}}
{"type":"Person","data":{"name":"c"}}"#,
            crate::loader::LoadMode::Merge,
        )
        .await
        .unwrap();
    for branch in ["left", "right"] {
        graph.branch_create(branch).await.unwrap();
    }
    session
        .load_with_receipt(
            "left",
            r#"{"type":"Person","data":{"name":"d"}}
{"edge":"Knows","from":"a","to":"b"}"#,
            crate::loader::LoadMode::Merge,
        )
        .await
        .unwrap();
    session
        .load_with_receipt(
            "right",
            r#"{"type":"Person","data":{"name":"e"}}
{"type":"Person","data":{"name":"f"}}
{"edge":"Knows","from":"b","to":"c"}"#,
            crate::loader::LoadMode::Merge,
        )
        .await
        .unwrap();
    drop(session);
    drop(graph);
    let reader = crate::db::Omnigraph::open(root).await.unwrap();
    let (mut left, catalog) = reader
        .capture_read_view(crate::db::ReadTarget::branch("left"))
        .await
        .unwrap();
    let (mut right, _) = reader
        .capture_read_view(crate::db::ReadTarget::branch("right"))
        .await
        .unwrap();
    for resolved in [&mut left, &mut right] {
        for entry in resolved.snapshot.raw_mut().entries.values_mut() {
            let mut metadata = serde_json::to_value(&entry.version_metadata).unwrap();
            metadata["e_tag"] = serde_json::Value::Null;
            entry.version_metadata = serde_json::from_value(metadata).unwrap();
        }
    }
    for table in ["node:Person", "edge:Knows"] {
        let a = left.snapshot.dataset(table).unwrap();
        let b = right.snapshot.dataset(table).unwrap();
        assert_eq!(a.published_dataset_version, b.published_dataset_version);
        assert_eq!(a.dataset_path, b.dataset_path);
        assert_eq!(a.native_dataset_branch, b.native_dataset_branch);
        assert_ne!(
            a.version_metadata.staged_version(),
            b.version_metadata.staged_version()
        );
    }
    let left_people = left
        .snapshot
        .open_lance_dataset("node:Person")
        .await
        .unwrap();
    let right_people = right
        .snapshot
        .open_lance_dataset("node:Person")
        .await
        .unwrap();
    assert_eq!(left_people.count_rows(None).await.unwrap(), 4);
    assert_eq!(right_people.count_rows(None).await.unwrap(), 5);
    assert_ne!(
        left_people.version().version,
        right_people.version().version
    );

    let scope = HashMap::from([(
        "Knows".to_string(),
        ("Person".to_string(), "Person".to_string()),
    )]);
    let left_index = reader
        .graph_index_for_resolved(&left, &scope, catalog.system_columns)
        .await
        .unwrap();
    let right_index = reader
        .graph_index_for_resolved(&right, &scope, catalog.system_columns)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&left_index, &right_index));
    assert_ne!(
        left_index.type_index("Person").unwrap().ids(),
        right_index.type_index("Person").unwrap().ids()
    );
    crate::graph_index::persist::save(
        &left.snapshot,
        reader.storage_adapter(),
        &scope,
        &left_index,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        crate::graph_index::persist::load(&left.snapshot, &scope, Some(reader.storage_adapter()))
            .await
            .is_some()
    );
    assert!(
        crate::graph_index::persist::load(&right.snapshot, &scope, Some(reader.storage_adapter()))
            .await
            .is_none(),
        "a cold artifact must reject another detached pin at the same counter"
    );

    let entry = right.snapshot.dataset("node:Person").unwrap();
    let full_path = format!("{root}/{}", entry.dataset_path);
    let base = crate::db::omnigraph::promotion::open_pinned_for_write(&reader, &full_path, entry)
        .await
        .unwrap();
    assert_eq!(base.version(), right_people.version().version);
    let transaction = right_people.read_transaction().await.unwrap().unwrap();
    let witness = crate::table_store::StagingWitness::from_transaction(&transaction).unwrap();
    let staged = reader
        .storage()
        .stage_delete(
            &base,
            datafusion::prelude::ident("name").eq(datafusion::prelude::lit("e")),
        )
        .await
        .unwrap()
        .expect("the right branch owns e");
    let (written, identity) = reader
        .storage()
        .commit_staged_detached(base, staged, &witness)
        .await
        .unwrap();
    assert_eq!(identity.read_version, right_people.version().version);
    assert_eq!(
        reader.storage().count_rows(&written, None).await.unwrap(),
        4
    );
    assert_eq!(left_people.count_rows(None).await.unwrap(), 4);
    assert_eq!(right_people.count_rows(None).await.unwrap(), 5);
}

fn test_schema_source() -> &'static str {
    r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company {
    title: String?
}
"#
}

fn build_test_catalog() -> Catalog {
    let schema = parse_schema(test_schema_source()).unwrap();
    let shape = compile_schema_shape(&schema).unwrap();
    let domain = SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let schema_ir = initialize_schema_ir(domain, &shape).unwrap().schema_ir;
    build_catalog_from_ir(&schema_ir).unwrap()
}

fn entity_batch(
    schema: Arc<Schema>,
    id: impl Into<String>,
    name: impl Into<String>,
    age: Option<i32>,
) -> RecordBatch {
    let id = id.into();
    let name = name.into();
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Arc<dyn arrow_array::Array> {
            match field.name().as_str() {
                "id" | "__id" => Arc::new(StringArray::from(vec![id.clone()])),
                "name" => Arc::new(StringArray::from(vec![name.clone()])),
                "age" => Arc::new(Int32Array::from(vec![age])),
                _ => arrow_array::new_null_array(field.data_type(), 1),
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn open_refuses_a_stamp_below_the_served_floor_before_any_effect() {
    let _scenario = crate::seams::FailScenario::setup();
    for legacy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let schema = "node Person { name: String }\n";
        let db = if legacy {
            Omnigraph::init_with_legacy_system_columns_for_tests(uri, schema)
                .await
                .unwrap()
        } else {
            Omnigraph::init(uri, schema).await.unwrap()
        };
        drop(db);
        let mut manifest = open_manifest_dataset(uri, None).await.unwrap();
        crate::db::manifest::migrations::set_stamp_for_test(&mut manifest, 9)
            .await
            .unwrap();
        let before_version = manifest.version().version;
        let reached_effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let test_thread = std::thread::current().id();
        let _probes = [
            &catalog::LOCAL_CREATE_IF_ABSENT_PROBE,
            &catalog::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
        ]
        .map(|seam| {
            let reached_effects = Arc::clone(&reached_effects);
            seam.observe(move || {
                if std::thread::current().id() == test_thread {
                    reached_effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            })
        });
        for mode in [
            crate::db::OpenMode::ReadOnly,
            crate::db::OpenMode::ReadWrite,
        ] {
            let result = match mode {
                crate::db::OpenMode::ReadOnly => Omnigraph::open_read_only(uri).await,
                crate::db::OpenMode::ReadWrite => Omnigraph::open(uri).await,
            };
            let error = result.err().expect("a v9 stamp is below the served floor");
            assert!(
                error.to_string().contains("reads only v11 to v11"),
                "{error}"
            );
            assert!(error.to_string().contains("omnigraph upgrade"), "{error}");
            assert_eq!(
                reached_effects.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "a refused stamp must refuse before the local write probe or recovery"
            );
            assert_eq!(
                open_manifest_dataset(uri, None)
                    .await
                    .unwrap()
                    .version()
                    .version,
                before_version
            );
        }
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn open_refuses_unknown_schema_features_before_recovery() {
    let _scenario = crate::seams::FailScenario::setup();
    for staged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        drop(
            Omnigraph::init(uri, "node Person { name: String }\n")
                .await
                .unwrap(),
        );
        let live_path = dir.path().join(crate::db::schema_state::SCHEMA_IR_FILENAME);
        let mut ir: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&live_path).unwrap()).unwrap();
        ir["features"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::String("time-travel".into()));
        let target = if staged {
            dir.path()
                .join(crate::db::schema_state::SCHEMA_IR_STAGING_FILENAME)
        } else {
            live_path
        };
        let tampered = serde_json::to_string(&ir).unwrap();
        std::fs::write(&target, &tampered).unwrap();
        let before_version = open_manifest_dataset(uri, None)
            .await
            .unwrap()
            .version()
            .version;
        let reached_effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let test_thread = std::thread::current().id();
        let _probes = [
            &catalog::LOCAL_CREATE_IF_ABSENT_PROBE,
            &catalog::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
        ]
        .map(|seam| {
            let reached_effects = Arc::clone(&reached_effects);
            seam.observe(move || {
                if std::thread::current().id() == test_thread {
                    reached_effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            })
        });
        for mode in [
            crate::db::OpenMode::ReadOnly,
            crate::db::OpenMode::ReadWrite,
        ] {
            let result = match mode {
                crate::db::OpenMode::ReadOnly => Omnigraph::open_read_only(uri).await,
                crate::db::OpenMode::ReadWrite => Omnigraph::open(uri).await,
            };
            let error = result
                .err()
                .expect("an unknown feature name must refuse open");
            assert!(
                error.to_string().contains("unknown to this build"),
                "staged {staged}: {error}"
            );
            assert_eq!(
                reached_effects.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "unknown feature names must refuse before the local write probe or recovery"
            );
            assert_eq!(
                std::fs::read_to_string(&target).unwrap(),
                tampered,
                "staged {staged}: the refused artifact must be left in place"
            );
            assert_eq!(
                open_manifest_dataset(uri, None)
                    .await
                    .unwrap()
                    .version()
                    .version,
                before_version
            );
        }
    }
}

#[tokio::test]
async fn test_drop_and_same_name_readd_uses_new_identity_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let before_version = mc.version();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();

    let table_key = "node:Person".to_string();
    let identity = TableIdentity::new(10_000, 1).unwrap();
    let table_path = table_path_for_identity(&table_key, identity).unwrap();
    let dataset_uri = format!("{}/{}", uri, table_path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let ds = crate::table_store::TableStore::create_empty_dataset(&dataset_uri, &schema)
        .await
        .unwrap();
    let state =
        crate::table_store::TableStore::new(uri, Arc::new(lance::session::Session::default()))
            .table_state(&dataset_uri, &ds)
            .await
            .unwrap();

    mc.commit_changes(&[
        ManifestChange::RegisterTable(TableRegistration {
            identity,
            table_key: table_key.clone(),
            table_path: table_path.clone(),
        }),
        ManifestChange::Update(DatasetUpdate {
            identity,
            type_key: table_key.clone(),
            published_dataset_version: state.version,
            native_dataset_branch: None,
            entity_count: state.row_count,
            version_metadata: state.version_metadata,
        }),
        ManifestChange::Tombstone(TableTombstone {
            identity: person_entry.identity,
            table_key: "node:Person".to_string(),
            tombstone_version: person_entry.published_dataset_version + 1,
        }),
    ])
    .await
    .unwrap();

    let head = mc.snapshot();
    let replacement = head.dataset("node:Person").unwrap();
    assert_eq!(replacement.identity, identity);
    assert_ne!(replacement.identity, person_entry.identity);
    assert_ne!(replacement.dataset_path, person_entry.dataset_path);
    assert_eq!(replacement.published_dataset_version, 1);

    let historical = ManifestCoordinator::snapshot_at(uri, None, before_version)
        .await
        .unwrap();
    let historical_person = historical.dataset("node:Person").unwrap();
    assert_eq!(historical_person.identity, person_entry.identity);
    assert_eq!(historical_person.dataset_path, person_entry.dataset_path);
}

#[tokio::test]
async fn snapshot_dataset_proves_index_inventory_from_raw_manifest_section() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("{}/indexed.lance", dir.path().display());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
    )
    .unwrap();
    let dataset = crate::table_store::TableStore::write_dataset(&uri, batch)
        .await
        .unwrap();
    assert!(
        !omnigraph_catalog::SnapshotDataset::new(dataset.clone()).has_raw_index_section(),
        "a dataset created without indexes must have no raw index section"
    );

    let store = crate::table_store::TableStore::new(
        dir.path().to_str().unwrap(),
        Arc::new(lance::session::Session::default()),
    );
    let staged = store
        .stage_create_indices(
            &dataset,
            &[crate::storage_layer::IndexBuildSpec::BTree {
                column: "id".to_string(),
                name: None,
            }],
        )
        .await
        .unwrap();
    let indexed = store
        .commit_staged(Arc::new(dataset), staged)
        .await
        .unwrap();
    assert!(
        omnigraph_catalog::SnapshotDataset::new(indexed).has_raw_index_section(),
        "the raw manifest witness must observe a committed index section"
    );
}

/// Regression (PR #307 review): the warm post-publish fold must pick a same
/// Lance-version registration with a new `table_branch` by its clock, exactly
/// as a fresh reopen does; the buggy fold kept the first equal-version row.
#[tokio::test]
async fn test_post_publish_fold_reflects_owner_branch_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut main_mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    main_mc.create_branch("feature").await.unwrap();

    // Fork Person onto `feature` at version Vf (owner = feature).
    let snap = main_mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.published_dataset_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_ds.append(reader, None).await.unwrap();
    let feature_version = feature_ds.version().version;
    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        Some("feature"),
        feature_version,
    )
    .await
    .unwrap();
    branch_manifest_namespace(uri, Some("feature"))
        .create_table_version(feature_metadata.to_create_table_version_request(
            "node:Person",
            feature_version,
            1,
            Some("feature"),
        ))
        .await
        .unwrap();

    // Create `experiment` from feature and fork Person at the SAME version Vf.
    let mut feature_mc = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    feature_mc.create_branch("experiment").await.unwrap();
    feature_ds
        .create_branch("experiment", feature_version, None)
        .await
        .unwrap();
    let experiment_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        Some("experiment"),
        feature_version,
    )
    .await
    .unwrap();

    // Publish through the warm graph coordinator so both its projection and
    // lineage adopt the same successful attempt.
    let mut experiment_mc = crate::db::graph_coordinator::GraphCoordinator::open_branch(
        uri,
        "experiment",
        Arc::new(crate::storage::ObjectStorageAdapter::local()),
    )
    .await
    .unwrap();
    // Pre-publish: experiment inherits feature's ownership of Person@Vf.
    assert_eq!(
        experiment_mc
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        Some("feature"),
    );
    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        Some("experiment"),
        experiment_mc.branch_identifier().await.unwrap(),
        experiment_mc.exact_graph_head(),
    ));
    let intent = experiment_mc.new_lineage_intent(None, None).unwrap();
    experiment_mc
        .commit_changes_with_intent_and_expected(
            &[ManifestChange::Update(DatasetUpdate {
                identity: person_entry.identity,
                type_key: "node:Person".to_string(),
                published_dataset_version: feature_version,
                native_dataset_branch: Some("experiment".to_string()),
                entity_count: 1,
                version_metadata: experiment_metadata,
            })],
            &HashMap::new(),
            intent,
            &precondition,
        )
        .await
        .unwrap();

    // Warm side: the folded known_state the commit adopted.
    let folded_branch = experiment_mc
        .snapshot()
        .dataset("node:Person")
        .unwrap()
        .native_dataset_branch
        .clone();
    // Oracle: a fresh reopen rebuilds known_state via `read_manifest_state`.
    let reopened = ManifestCoordinator::open_at_branch(uri, "experiment")
        .await
        .unwrap();
    let scanned_branch = reopened
        .snapshot()
        .dataset("node:Person")
        .unwrap()
        .native_dataset_branch
        .clone();

    assert_eq!(
        scanned_branch.as_deref(),
        Some("experiment"),
        "fresh reopen should reflect the owner-branch handoff",
    );
    assert_eq!(
        folded_branch, scanned_branch,
        "warm coordinator's folded known_state diverged from a fresh re-scan after an \
         owner-branch handoff (folded {folded_branch:?} vs scanned {scanned_branch:?})",
    );
    let probes = crate::instrumentation::QueryIoProbes::default();
    crate::instrumentation::with_query_io_probes(probes.clone(), experiment_mc.refresh())
        .await
        .unwrap();
    assert_eq!(
        probes
            .manifest_scan_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "same-version ownership handoff must retain its exact projection"
    );
}

#[tokio::test]
async fn future_stamp_is_refused_in_both_open_modes() {
    use crate::db::{Omnigraph, OpenMode};
    use crate::storage::storage_for_uri;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // A full graph (schema artifacts present) so `Omnigraph::open*` gets past its
    // schema read to the stamp check.
    Omnigraph::init(uri, "node Person { name: String }\n")
        .await
        .unwrap();

    // Stamp past this binary's known version.
    {
        let mut ds = open_manifest_dataset(uri, None).await.unwrap();
        ds.update_schema_metadata([(
            "omnigraph:internal_schema_version".to_string(),
            Some((INTERNAL_MANIFEST_SCHEMA_VERSION + 1).to_string()),
        )])
        .await
        .unwrap();
    }

    let storage = storage_for_uri(uri).unwrap();
    for mode in [OpenMode::ReadWrite, OpenMode::ReadOnly] {
        // `Omnigraph` is not `Debug`, so match instead of `expect_err`.
        let err = match Omnigraph::open_with_storage_and_mode(uri, Arc::clone(&storage), mode).await
        {
            Ok(_) => panic!("{mode:?}: a future-stamped graph must be refused"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("upgrade omnigraph"),
            "{mode:?}: expected an upgrade-omnigraph refusal, got: {err}",
        );
    }
}

#[tokio::test]
async fn sub_current_graph_is_refused_on_open_with_rebuild_hint() {
    use crate::db::Omnigraph;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // A full v5 graph (schema artifacts present) so the open path gets past its
    // schema read to the stamp check.
    Omnigraph::init(uri, "node Person { name: String }\n")
        .await
        .unwrap();

    {
        let mut ds = open_manifest_dataset(uri, None).await.unwrap();
        crate::db::manifest::migrations::set_stamp_for_test(&mut ds, 4)
            .await
            .unwrap();
    }

    // Read-write open is refused with the rebuild hint.
    let rw_err = match Omnigraph::open(uri).await {
        Ok(_) => panic!("read-write open of a sub-CURRENT graph must be refused"),
        Err(err) => err,
    };
    assert!(
        rw_err.to_string().contains("export"),
        "read-write refusal must point at `omnigraph export`, got: {rw_err}",
    );

    // Read-only open is refused identically.
    let ro_err = match Omnigraph::open_read_only(uri).await {
        Ok(_) => panic!("read-only open of a sub-CURRENT graph must be refused"),
        Err(err) => err,
    };
    assert!(
        ro_err.to_string().contains("export"),
        "read-only refusal must point at `omnigraph export`, got: {ro_err}",
    );
}

// The full operator upgrade narrative in one flow: load data → export → a graph from
// an older release (simulated by rewinding the stamp below CURRENT) is refused with
// the export/import nudge → rebuild via fresh `init` + `load` → the data is present
// and the rebuilt graph opens. The refusal is **stamp-only** (read before any data),
// so a stamp-rewound graph is a faithful stand-in for a real older-release graph
// without needing a second binary — the on-disk layout is never reached. Data
// fidelity for vector / blob columns is covered by the export round-trip tests in
// `tests/export.rs`; this test composes the refusal with the rebuild so the operator
// path proven in the docs (`docs/user/operations/upgrade.md`) is exercised end to end.
#[tokio::test]
async fn sub_current_graph_is_refused_then_rebuilt_via_export_import() {
    use crate::db::Omnigraph;
    use crate::loader::LoadMode;

    let schema = "node Person {\n    name: String @key\n    age: I32?\n}\n";
    let seed = "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n\
                {\"type\":\"Person\",\"data\":{\"name\":\"bob\",\"age\":41}}\n";

    // The operator's existing graph; export it with the (here, current) binary
    // before upgrading.
    let dir_old = tempfile::tempdir().unwrap();
    let uri_old = dir_old.path().to_str().unwrap();
    let db_old = crate::Session::from_defaults(
        std::sync::Arc::new(Omnigraph::init(uri_old, schema).await.unwrap()),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
    db_old.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
    let exported = db_old.export_jsonl("main", &[]).await.unwrap();
    assert!(
        exported.contains("alice") && exported.contains("bob"),
        "export must carry the loaded rows",
    );
    drop(db_old);

    // Make it look like a graph from an older release: rewind the stamp below CURRENT.
    {
        let mut ds = open_manifest_dataset(uri_old, None).await.unwrap();
        crate::db::manifest::migrations::set_stamp_for_test(&mut ds, 4)
            .await
            .unwrap();
    }
    let err = match Omnigraph::open(uri_old).await {
        Ok(_) => panic!("a sub-CURRENT graph must be refused on open"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("export"),
        "the refusal must nudge the operator to `omnigraph export`, got: {err}",
    );
    assert!(
        msg.contains("0.8.x"),
        "the refusal must name the release that wrote this stamp (v4 → 0.8.x) so the \
         operator knows which binary to use, got: {err}",
    );

    // Rebuild with this binary: fresh init + load the export.
    let dir_new = tempfile::tempdir().unwrap();
    let uri_new = dir_new.path().to_str().unwrap();
    let db_new = crate::Session::from_defaults(
        std::sync::Arc::new(Omnigraph::init(uri_new, schema).await.unwrap()),
        omnigraph_compiler::settings::SessionSettings::default(),
    );
    db_new
        .load_jsonl(&exported, LoadMode::Overwrite)
        .await
        .unwrap();

    // The rebuilt graph preserves the data and is at CURRENT (opens without refusal).
    let rebuilt = db_new.export_jsonl("main", &[]).await.unwrap();
    assert!(
        rebuilt.contains("alice") && rebuilt.contains("bob"),
        "the rebuilt graph must preserve every node",
    );
    assert_eq!(
        rebuilt.lines().count(),
        exported.lines().count(),
        "export → init → load round-trips every row",
    );
    Omnigraph::open(uri_new)
        .await
        .expect("the rebuilt graph is at CURRENT and opens");
}

/// A microsecond UNIX timestamp for a `LineageIntent`, matching the genesis /
/// commit-graph `created_at` unit.
fn lineage_now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// The incremental fold and a clean full reopen must produce the same state
/// and lineage. This is an explicit correctness oracle, kept out of the
/// production refresh path so debug cost tests measure the same I/O shape as a
/// release build.
#[tokio::test]
async fn projection_refresh_matches_clean_full_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let _mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();

    async fn publish_empty_commit(publisher: &GraphNamespacePublisher) -> Result<()> {
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            branch: None,
            actor_id: None,
            merged_parent_commit_id: None,
            created_at: lineage_now_micros(),
        };
        publisher
            .publish(&[], &HashMap::new(), Some(&intent))
            .await
            .map(|_| ())
    }
    let publisher = GraphNamespacePublisher::new(uri, None);
    let control_session = crate::lance_access::control_session();
    let (mut reader, _) = ManifestCoordinator::open_with_lineage(uri, None, &control_session)
        .await
        .unwrap();
    let old_head = reader.known_state.graph_heads[MAIN_BRANCH_HEAD_KEY].clone();

    for _ in 0..8 {
        publish_empty_commit(&publisher).await.unwrap();
    }
    let LineageRefresh::Replace(mut folded_lineage) = reader.refresh_with_lineage().await.unwrap()
    else {
        panic!("a copy-on-write publish replaces the fragment set, so refresh must replace");
    };
    assert_ne!(
        reader.known_state.graph_heads[MAIN_BRANCH_HEAD_KEY], old_head,
        "the oracle must exercise mutable graph-head replacement, not only appends"
    );

    let (fresh, mut full_lineage) =
        ManifestCoordinator::open_with_lineage(uri, None, &control_session)
            .await
            .unwrap();
    assert_eq!(reader.known_state.version, fresh.known_state.version);
    assert_eq!(
        format!("{:?}", reader.known_state.entries),
        format!("{:?}", fresh.known_state.entries)
    );
    assert_eq!(
        reader.known_state.graph_heads,
        fresh.known_state.graph_heads
    );
    folded_lineage.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
    full_lineage.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
    assert_eq!(folded_lineage, full_lineage);

    // A local publish already has the exact next state. Once the graph cache
    // has adopted its lineage, the next refresh must not rebuild that state.
    let mut writer = crate::db::graph_coordinator::GraphCoordinator::open(
        uri,
        Arc::new(crate::storage::ObjectStorageAdapter::local()),
    )
    .await
    .unwrap();
    writer.commit_updates_with_actor(&[], None).await.unwrap();
    let probes = crate::instrumentation::QueryIoProbes::default();
    crate::instrumentation::with_query_io_probes(probes.clone(), writer.refresh())
        .await
        .unwrap();
    assert_eq!(
        probes
            .manifest_scan_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a successful local publish must preserve the coherent projection"
    );

    // If another writer advanced the base, preserving only our own lineage
    // would hide its commit. The full refresh must still include both writers.
    publish_empty_commit(&publisher).await.unwrap();
    writer.commit_updates_with_actor(&[], None).await.unwrap();
    writer.refresh().await.unwrap();
    let fresh = crate::db::graph_coordinator::GraphCoordinator::open(
        uri,
        Arc::new(crate::storage::ObjectStorageAdapter::local()),
    )
    .await
    .unwrap();
    let mut actual = writer.load_commits().await.unwrap();
    let mut expected = fresh.load_commits().await.unwrap();
    actual.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
    expected.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));

    // Registration replacement and tombstone suppression must also survive
    // the handoff; an append-only accumulator would retain the old alias.
    let person = writer.snapshot().dataset("node:Person").unwrap().clone();
    for change in [
        ManifestChange::RenameTable(TableRename {
            identity: person.identity,
            expected_table_key: person.type_key.clone(),
            table_key: "node:Human".to_string(),
            table_path: person.dataset_path.clone(),
        }),
        ManifestChange::Tombstone(TableTombstone {
            identity: person.identity,
            table_key: "node:Human".to_string(),
            tombstone_version: person.published_dataset_version,
        }),
    ] {
        let intent = writer.new_lineage_intent(None, None).unwrap();
        writer
            .commit_changes_with_intent_and_expected(
                &[change],
                &HashMap::new(),
                intent,
                &PublishPrecondition::Any,
            )
            .await
            .unwrap();
        let probes = crate::instrumentation::QueryIoProbes::default();
        crate::instrumentation::with_query_io_probes(probes.clone(), writer.refresh())
            .await
            .unwrap();
        assert_eq!(
            probes
                .manifest_scan_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "local metadata replacements must preserve the exact projection"
        );
        let fresh = ManifestCoordinator::open(uri).await.unwrap();
        let actual_snapshot = writer.snapshot();
        let expected_snapshot = fresh.snapshot();
        let mut actual = actual_snapshot.datasets().collect::<Vec<_>>();
        let mut expected = expected_snapshot.datasets().collect::<Vec<_>>();
        actual.sort_by(|a, b| a.type_key.cmp(&b.type_key));
        expected.sort_by(|a, b| a.type_key.cmp(&b.type_key));
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"),);
    }

    // Publication alone cannot claim that a separate lineage cache adopted the
    // commit. This is also the state left by a post-manifest failure.
    reader.refresh_with_lineage().await.unwrap();
    let unacknowledged = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: None,
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
    };
    reader
        .commit_changes_with_lineage(&[], &HashMap::new(), Some(&unacknowledged))
        .await
        .unwrap();
    let LineageRefresh::Replace(rows) = reader.refresh_with_lineage().await.unwrap() else {
        panic!("an unacknowledged lineage handoff must reconstruct the complete history");
    };
    assert!(
        rows.iter()
            .any(|row| row.graph_commit_id == unacknowledged.graph_commit_id)
    );
}

mod migrations_tests {
    use crate::db::manifest::migrations::*;

    /// An admitted root opens at the stamp its conversion names and at no
    /// other; the admission ends with its guard, and a foreign root is never
    /// admitted.
    #[tokio::test]
    async fn conversion_admission_admits_one_root_at_one_stamp_while_held() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        drop(
            crate::db::Omnigraph::init(root, "node Person { name: String }")
                .await
                .unwrap(),
        );
        let control_session = crate::lance_access::control_session();
        let mut manifest = crate::db::manifest::layout::open_manifest_dataset_with_session(
            root,
            None,
            &control_session,
        )
        .await
        .unwrap();
        set_stamp(&mut manifest, 10).await.unwrap();
        let refused = guard_stamp(&manifest).unwrap_err().to_string();
        assert!(refused.contains("reads only v11 to v11"), "{refused}");
        {
            let _admission = admit_conversion_source(root, 10);
            assert_eq!(guard_stamp(&manifest).unwrap(), 10);
            let _other = admit_conversion_source("/nowhere/else", 9);
            assert!(guard_stamp(&manifest).is_ok());
        }
        assert!(
            guard_stamp(&manifest).is_err(),
            "the admission ends with its guard"
        );
        let _wrong_stamp = admit_conversion_source(root, 9);
        assert!(
            guard_stamp(&manifest).is_err(),
            "an admission names one stamp and admits no other"
        );
    }
}
