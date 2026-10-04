use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    DescribeTableRequest, DescribeTableVersionRequest, ListTableVersionsRequest,
};
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tokio::sync::Mutex;

use super::commit_graph::GraphCommit;
use super::publisher::{
    GraphHeadExpectation, LineageIntent, ManifestBatchPublisher, PublishOutcome,
    PublishPrecondition, is_retryable_publish_conflict, map_lance_publish_error,
};
use super::state::{ManifestRows, TABLE_ROW_BYTES, read_manifest_rows, read_manifest_state};
use super::*;
use crate::error::{ManifestConflictDetails, ManifestError, StorageFailureKind};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    SchemaIdentityDomain, build_catalog_from_ir, compile_schema_shape, initialize_schema_ir,
};

#[test]
fn publisher_retry_vocabulary_is_the_version_cas() {
    for lost in [
        lance::Error::commit_conflict_source(
            3,
            Box::new(std::io::Error::other("next version already written")),
        ),
        lance::Error::too_much_write_contention("contended"),
        lance::Error::retryable_commit_conflict_source(
            3,
            Box::new(std::io::Error::other("stale transaction")),
        ),
    ] {
        let lost = map_lance_publish_error(lost);
        assert!(is_retryable_publish_conflict(&lost));
        assert!(matches!(
            lost,
            OmniError::Manifest(ManifestError {
                details: Some(ManifestConflictDetails::RowLevelCasContention),
                ..
            })
        ));
    }

    let generic = map_lance_publish_error(lance::Error::io_source(Box::new(
        std::io::Error::other("disk gone"),
    )));
    assert!(!is_retryable_publish_conflict(&generic));
}

/// A foreign append (an old binary's writer) takes the next `__manifest`
/// version; Lance would rebase an overwrite over it, so only zero retries make
/// the stale overwrite lose the version CAS instead of dropping the append.
#[tokio::test]
async fn stale_overwrite_loses_the_version_cas_without_replacing_the_winner() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let base = open_manifest_dataset(uri, None).await.unwrap();
    let rows = read_manifest_rows(&base).await.unwrap();
    let stored: Vec<RecordBatch> = base
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let append = WriteParams {
        mode: WriteMode::Append,
        skip_auto_cleanup: true,
        ..Default::default()
    };
    let winner = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&append)
        .execute(vec![relabelled_manifest_row(&stored, "cas_probe:winner")])
        .await
        .unwrap();
    assert_eq!(winner.version().version, base.version().version + 1);

    let lost = super::commit::overwrite(base, &rows)
        .await
        .expect_err("a stale overwrite must not land over the winner");
    assert!(is_retryable_publish_conflict(&lost), "{lost}");

    let head = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(head.version().version, winner.version().version);
    let ids = manifest_object_ids(&head).await;
    assert!(ids.contains("cas_probe:winner"), "{ids:?}");
    assert_eq!(ids.len(), rows.tables.len() + 3, "{ids:?}");
}

/// One stored `__manifest` row under a new `object_id`.
fn relabelled_manifest_row(stored: &[RecordBatch], object_id: &str) -> RecordBatch {
    let row = stored[0].slice(0, 1);
    let schema = row.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(row.columns())
        .map(|(field, column)| {
            if field.name() == "object_id" {
                Arc::new(StringArray::from(vec![object_id])) as arrow_array::ArrayRef
            } else {
                column.clone()
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

async fn manifest_object_ids(dataset: &Dataset) -> HashSet<String> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            super::state::string_column(batch, "object_id")
                .unwrap()
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
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

fn build_same_name_node_edge_catalog() -> Catalog {
    let schema = parse_schema(
        r#"
node Link {
    name: String @key
}
edge Link: Link -> Link
"#,
    )
    .unwrap();
    let shape = compile_schema_shape(&schema).unwrap();
    let domain = SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
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

#[test]
fn table_identity_rejects_zero_and_drives_paths_and_object_ids() {
    assert!(TableIdentity::new(0, 1).is_err());
    assert!(TableIdentity::new(1, 0).is_err());

    let identity = TableIdentity::new(0x2a, 0x7).unwrap();
    assert_eq!(
        table_path_for_identity("node:Person", identity).unwrap(),
        "nodes/000000000000002a-0000000000000007"
    );
    assert_eq!(
        table_path_for_identity("edge:Knows", identity).unwrap(),
        "edges/000000000000002a-0000000000000007"
    );
    assert_eq!(
        super::layout::table_object_id(identity),
        "table:000000000000002a:0000000000000007"
    );
}

#[test]
fn azure_table_locations_and_manifest_paths_use_remote_object_layout() {
    let root = "az://omnigraph/clusters/company%20brain";
    let table_path = "nodes/000000000000002a-0000000000000007";

    assert_eq!(
        table_uri_for_path(root, table_path, None).unwrap(),
        format!("{root}/{table_path}")
    );
    assert_eq!(
        table_uri_for_path(root, table_path, Some("review/one")).unwrap(),
        format!("{root}/{table_path}/tree/review/one")
    );
    assert_eq!(
        object_store_path_from_uri(&format!(
            "{root}/{table_path}/_versions/00000000000000000001.manifest"
        ))
        .unwrap(),
        format!("clusters/company brain/{table_path}/_versions/00000000000000000001.manifest")
    );
    assert!(table_uri_for_path("https://example.test/graph", table_path, None).is_err());
}

#[tokio::test]
async fn historical_alias_binding_keeps_same_name_node_and_edge_identities_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_same_name_node_edge_catalog();
    let mut snapshot = ManifestCoordinator::init(uri, &catalog)
        .await
        .unwrap()
        .snapshot();
    let node_identity = snapshot.dataset("node:Link").unwrap().identity;
    let edge_identity = snapshot.dataset("edge:Link").unwrap().identity;
    assert_ne!(node_identity, edge_identity);

    snapshot.bind_catalog_aliases(&catalog).unwrap();

    assert_eq!(
        snapshot.dataset("node:Link").unwrap().identity,
        node_identity
    );
    assert_eq!(
        snapshot.dataset("edge:Link").unwrap().identity,
        edge_identity
    );
}

#[tokio::test]
async fn test_init_creates_manifest_and_sub_tables() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();

    assert!(snap.dataset("node:Person").is_some());
    assert!(snap.dataset("node:Company").is_some());
    assert!(snap.dataset("edge:Knows").is_some());
    assert!(snap.dataset("edge:WorksAt").is_some());

    for key in &["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        let entry = snap.dataset(key).unwrap();
        assert_eq!(entry.published_dataset_version, 1);
        assert_eq!(entry.entity_count, 0);
        assert!(entry.native_dataset_branch.is_none());
    }
}

#[tokio::test]
async fn exact_genesis_probe_rejects_another_initialization_attempt() {
    for system_columns in [SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let current = build_test_catalog();
        let mut schema_ir = current.bound_schema_ir().unwrap().clone();
        if system_columns == SYSTEM_COLUMNS_LEGACY {
            schema_ir = omnigraph_compiler::into_legacy_vintage(schema_ir);
        }
        let catalog = build_catalog_from_ir(&schema_ir).unwrap();
        assert_eq!(catalog.system_columns, system_columns);
        let control_session = crate::lance_access::control_session();
        let committed_attempt = GenesisManifestAttempt::mint(catalog.system_columns).unwrap();

        ManifestCoordinator::init_commit(
            uri,
            &catalog,
            &SchemaContractRow::for_test_catalog(&catalog).unwrap(),
            &control_session,
            &committed_attempt,
        )
        .await
        .unwrap();
        ManifestCoordinator::open_exact_genesis(uri, &committed_attempt, &control_session)
            .await
            .expect("the creating attempt must authenticate its own immutable genesis");

        let foreign_attempt = GenesisManifestAttempt::mint(catalog.system_columns).unwrap();
        let error =
            match ManifestCoordinator::open_exact_genesis(uri, &foreign_attempt, &control_session)
                .await
            {
                Ok(_) => {
                    panic!("a valid v1 manifest from another initializer must not authenticate")
                }
                Err(error) => error,
            };
        assert!(
            error
                .to_string()
                .contains("genesis lineage does not match this initialization attempt"),
            "unexpected probe error: {error:?}"
        );

        let other_vintage = if system_columns == SYSTEM_COLUMNS_LEGACY {
            SYSTEM_COLUMNS_V3
        } else {
            SYSTEM_COLUMNS_LEGACY
        };
        let foreign_vintage_attempt = GenesisManifestAttempt::mint(other_vintage).unwrap();
        let error = match ManifestCoordinator::open_exact_genesis(
            uri,
            &foreign_vintage_attempt,
            &control_session,
        )
        .await
        {
            Ok(_) => panic!("a manifest stamped for another vintage must not authenticate"),
            Err(error) => error,
        };
        // Since v10 both vintages share one stamp, so the attempt's own
        // lineage, not the stamp, is what separates a foreign initializer.
        assert!(
            error
                .to_string()
                .contains("genesis lineage does not match this initialization attempt"),
            "unexpected probe error: {error:?}"
        );
    }
}

/// A prepared-create token written by a binary without the lineage fields of
/// this format decodes at the genesis generation and still validates.
#[test]
fn an_upstream_shaped_genesis_lineage_decodes_at_the_genesis_generation() {
    let upstream = serde_json::json!({
        "graph_commit_id": "01BX5ZZKBKACTAV9WEVGEMMVRZ",
        "graph_branch": null,
        "graph_manifest_version": 1,
        "parent_commit_id": null,
        "merged_parent_commit_id": null,
        "actor_id": null,
        "created_at": 1,
    });
    let lineage: GraphLineageRow = serde_json::from_value(upstream.clone()).unwrap();
    assert_eq!(lineage.generation, 0);
    assert_eq!(lineage.native_branch, None);
    assert_eq!(lineage.schema_content_hash, None);
    let attempt: GenesisManifestAttempt = serde_json::from_value(serde_json::json!({
        "lineage": upstream,
        "stamp": stamp_for_system_columns(SYSTEM_COLUMNS_V3).unwrap(),
    }))
    .unwrap();
    attempt.validate_for(SYSTEM_COLUMNS_V3).unwrap();
}

#[test]
fn prepared_genesis_attempt_refuses_a_later_generation_or_a_native_branch() {
    let minted =
        serde_json::to_value(GenesisManifestAttempt::mint(SYSTEM_COLUMNS_V3).unwrap()).unwrap();
    for (field, value) in [
        ("generation", serde_json::json!(7)),
        (
            "native_branch",
            serde_json::json!("feature.01BX5ZZKBKACTAV9WEVGEMMVRZ"),
        ),
    ] {
        let mut token = minted.clone();
        token["lineage"][field] = value;
        let attempt: GenesisManifestAttempt = serde_json::from_value(token).unwrap();
        let error = attempt.validate_for(SYSTEM_COLUMNS_V3).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid prepared genesis attempt"),
            "{field}: {error}"
        );
    }
    let attempt: GenesisManifestAttempt = serde_json::from_value(minted).unwrap();
    attempt.validate_for(SYSTEM_COLUMNS_V3).unwrap();
}

#[tokio::test]
async fn test_open_reads_existing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    ManifestCoordinator::init(uri, &catalog).await.unwrap();

    let mc = ManifestCoordinator::open(uri).await.unwrap();
    let snap = mc.snapshot();
    assert!(snap.dataset("node:Person").is_some());
    assert!(snap.dataset("edge:Knows").is_some());
}

#[tokio::test]
async fn test_commit_advances_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let v1 = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let new_version = mc
        .commit(&[DatasetUpdate {
            identity: person_entry.identity,
            type_key: "node:Person".to_string(),
            published_dataset_version: person_version,
            native_dataset_branch: None,
            entity_count: 1,
            version_metadata: table_version_metadata_for_state(
                uri,
                &person_entry.dataset_path,
                None,
                person_version,
            )
            .await
            .unwrap(),
        }])
        .await
        .unwrap();

    assert!(new_version > v1);

    let snap = mc.snapshot();
    let person = snap.dataset("node:Person").unwrap();
    assert_eq!(person.published_dataset_version, person_version);
    assert_eq!(person.entity_count, 1);

    let company = snap.dataset("node:Company").unwrap();
    assert_eq!(company.published_dataset_version, 1);
    assert_eq!(company.entity_count, 0);
}

/// RFC 0062: a dropped identity is never re-registered. A later registration
/// would win the clock-ordered fold and resurrect the table, so the publisher
/// refuses it before the merge-insert.
#[tokio::test]
async fn test_publish_refuses_registration_of_a_tombstoned_identity() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    mc.commit_changes(&[ManifestChange::Tombstone(TableTombstone {
        identity: person_entry.identity,
        table_key: "node:Person".to_string(),
        tombstone_version: person_entry.published_dataset_version + 1,
    })])
    .await
    .unwrap();
    let dropped_version = mc.version();
    assert!(mc.snapshot().dataset("node:Person").is_none());

    let sealed_version = person_entry.published_dataset_version + 1;
    let rows = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    let dropped = rows
        .tables
        .iter()
        .find(|table| table.registration.identity == person_entry.identity)
        .expect("a dropped table keeps its row");
    assert_eq!(dropped.registration.table_key, "node:Person");
    assert_eq!(
        dropped.state,
        TableState::Dropped {
            dropped_at: dropped_version,
            sealed_version,
        }
    );
    let pre_drop_pin = HashMap::from([(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: person_entry.published_dataset_version,
            native_ref: NativeRefPin::Exact(None),
        },
    )]);
    let err = mc
        .commit_changes_with_expected(&[], &pre_drop_pin)
        .await
        .expect_err("a pin taken before the drop must be refused");
    assert!(
        matches!(
            &err,
            OmniError::Manifest(ManifestError {
                details: Some(ManifestConflictDetails::PublishedDatasetVersionMismatch {
                    actual_published_dataset_version,
                    ..
                }),
                ..
            }) if *actual_published_dataset_version == sealed_version
        ),
        "the refusal must report the sealed version: {err:?}"
    );

    let err = mc
        .commit_changes(&[ManifestChange::Update(DatasetUpdate {
            identity: person_entry.identity,
            type_key: "node:Person".to_string(),
            published_dataset_version: person_entry.published_dataset_version,
            native_dataset_branch: None,
            entity_count: person_entry.entity_count,
            version_metadata: person_entry.version_metadata.clone(),
        })])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("is tombstoned"),
        "unexpected: {err}"
    );

    let err = mc
        .commit_changes(&[ManifestChange::Tombstone(TableTombstone {
            identity: person_entry.identity,
            table_key: "node:Person".to_string(),
            tombstone_version: person_entry.published_dataset_version + 1,
        })])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("already tombstoned"),
        "unexpected: {err}"
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), dropped_version);
    assert!(reopened.snapshot().dataset("node:Person").is_none());
}

#[tokio::test]
async fn metadata_only_rename_preserves_identity_path_and_table_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let before_manifest_version = mc.version();
    let before = mc.snapshot().dataset("node:Person").unwrap().clone();

    // A rename into another live identity's alias is rejected before the
    // manifest advances.
    let collision = mc
        .commit_changes(&[ManifestChange::RenameTable(TableRename {
            identity: before.identity,
            expected_table_key: "node:Person".to_string(),
            table_key: "node:Company".to_string(),
            table_path: before.dataset_path.clone(),
        })])
        .await
        .expect_err("two live table identities cannot share an alias");
    assert!(collision.to_string().contains("two live table identities"));
    assert_eq!(
        mc.probe_latest_version().await.unwrap(),
        before_manifest_version
    );

    mc.commit_changes(&[ManifestChange::RenameTable(TableRename {
        identity: before.identity,
        expected_table_key: "node:Person".to_string(),
        table_key: "node:Human".to_string(),
        table_path: before.dataset_path.clone(),
    })])
    .await
    .unwrap();

    let head = mc.snapshot();
    assert!(head.dataset("node:Person").is_none());
    let renamed = head.dataset("node:Human").unwrap();
    assert_eq!(renamed.identity, before.identity);
    assert_eq!(renamed.dataset_path, before.dataset_path);
    assert_eq!(
        renamed.published_dataset_version,
        before.published_dataset_version
    );
    assert_eq!(renamed.entity_count, before.entity_count);

    let stale_binding = HashMap::from([(
        before.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: before.published_dataset_version,
            native_ref: NativeRefPin::Unchecked,
        },
    )]);
    let stale_error = mc
        .commit_changes_with_expected(&[], &stale_binding)
        .await
        .expect_err("an identity expectation with a stale alias must fail");
    assert!(
        matches!(
            &stale_error,
            OmniError::Manifest(manifest)
                if matches!(
                    manifest.details.as_ref(),
                    Some(crate::error::ManifestConflictDetails::ReadSetChanged { .. })
                )
        ),
        "expected a typed stale-binding error, got {stale_error:?}"
    );

    let historical = ManifestCoordinator::snapshot_at(uri, None, before_manifest_version)
        .await
        .unwrap();
    assert!(historical.dataset("node:Human").is_none());
    let historical_person = historical.dataset("node:Person").unwrap();
    assert_eq!(historical_person.identity, before.identity);
    assert_eq!(historical_person.dataset_path, before.dataset_path);
    assert_eq!(
        historical_person.published_dataset_version,
        before.published_dataset_version
    );
}

#[tokio::test]
async fn test_snapshot_open_sub_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_ds = snap.open_dataset("node:Person").await.unwrap();

    assert_eq!(person_ds.schema().fields.len(), 3);
    assert_eq!(person_ds.count_rows(None).await.unwrap(), 0);
}

#[tokio::test]
async fn snapshot_scanner_row_and_byte_limits_are_composed() {
    const ROWS: usize = 10_000;
    const BATCH_ROWS: usize = 8_192;

    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let expected_ids = (0..ROWS)
        .map(|row| format!("row-{row:05}"))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(&expected_ids))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new([Ok(batch)], Arc::clone(&schema));
    let dataset = Dataset::write(reader, dir.path().to_str().unwrap(), None)
        .await
        .unwrap();
    let table = SnapshotDataset::new(dataset);
    let assert_contents = |batches: &[RecordBatch]| {
        let actual_ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_ids.len(), ROWS);
        assert_eq!(
            actual_ids,
            expected_ids
                .iter()
                .map(|id| Some(id.as_str()))
                .collect::<Vec<_>>()
        );
    };

    // A large byte target must retain the row ceiling; a small one must
    // constrain the batches further. Neither setting may lose or alter rows.
    for (byte_target, byte_limited) in [(32 * 1024 * 1024, false), (4 * 1024, true)] {
        let mut scanner = table.scan();
        scanner.batch_size(BATCH_ROWS);
        scanner.batch_size_bytes(byte_target);
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_contents(&batches);
        assert!(
            batches
                .iter()
                .all(|batch| (1..=BATCH_ROWS).contains(&batch.num_rows()))
        );
        let largest_batch = batches.iter().map(RecordBatch::num_rows).max().unwrap();
        if byte_limited {
            assert!(largest_batch < BATCH_ROWS);
        } else {
            assert_eq!(largest_batch, BATCH_ROWS);
        }
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            // Count this fixture's logical bytes, not retained Arrow buffers.
            let logical_bytes = std::mem::size_of_val(ids.value_offsets())
                + (ids.value_offsets().last().unwrap() - ids.value_offsets()[0]) as usize;
            assert!(logical_bytes <= byte_target as usize);
        }
    }

    // Strict batching still guarantees exact row counts when used alone.
    let mut scanner = table.scan();
    scanner.batch_size(BATCH_ROWS);
    scanner.strict_batch_size(true);

    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_contents(&batches);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![BATCH_ROWS, ROWS - BATCH_ROWS]
    );

    scanner.batch_size_bytes(32 * 1024 * 1024);
    let error = scanner
        .try_into_stream()
        .await
        .err()
        .expect("strict row batching and a byte target must be rejected");
    assert_eq!(
        error.storage_failure().map(|failure| failure.kind),
        Some(StorageFailureKind::Configuration)
    );
    assert!(
        error
            .to_string()
            .contains("strict_batch_size=true cannot be combined with batch_size_bytes=")
    );
}

#[tokio::test]
async fn test_version_is_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    assert_eq!(mc.version(), snap.graph_manifest_version());
}

#[tokio::test]
async fn test_list_graph_branches_only_returns_main_once() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let branches = mc.list_graph_branches().await.unwrap();
    assert_eq!(
        branches
            .iter()
            .filter(|branch| branch.as_str() == "main")
            .count(),
        1
    );
}

#[tokio::test]
async fn test_branch_namespace_lists_and_describes_versions() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();

    mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
        .await
        .expect(
            "a graph commit settles the genesis commit with its pins, so the pin the namespace \
             write replaces stays listed",
        );
    let namespace = branch_manifest_namespace(uri, None);
    let request =
        version_metadata.to_create_table_version_request("node:Person", person_version, 1, None);
    namespace.create_table_version(request).await.unwrap();
    mc.refresh().await.unwrap();

    let versions = namespace
        .list_table_versions(ListTableVersionsRequest {
            id: Some(vec!["node:Person".to_string()]),
            descending: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(versions.versions.len(), 2);
    assert_eq!(versions.versions[0].version as u64, person_version);
    assert_eq!(versions.versions[1].version, 1);

    let described = namespace
        .describe_table_version(DescribeTableVersionRequest {
            id: Some(vec!["node:Person".to_string()]),
            version: Some(person_version as i64),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(described.version.version as u64, person_version);
    assert_eq!(
        mc.snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_version
    );
    assert_eq!(
        mc.snapshot().dataset("node:Person").unwrap().entity_count,
        1
    );
}

#[tokio::test]
async fn test_directory_namespace_direct_publish_cannot_replace_native_omnigraph_write_path() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let graph_manifest_version = mc.version();

    let namespace = DirectoryNamespaceBuilder::new(uri)
        .manifest_enabled(true)
        .dir_listing_enabled(false)
        .table_version_tracking_enabled(true)
        .inline_optimization_enabled(false)
        .build()
        .await
        .unwrap();

    // Manifest v5 keys rows by immutable table identity, not the mutable
    // diagnostic alias understood by DirectoryNamespace. Native per-table
    // namespace APIs therefore cannot address OmniGraph tables by alias, much
    // less replace the graph-wide publisher.
    let list_error = namespace
        .list_table_versions(ListTableVersionsRequest {
            id: Some(vec!["node:Person".to_string()]),
            descending: Some(true),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let cannot_address = |error: &str| error.contains("FieldNotFound");
    assert!(
        cannot_address(&format!("{list_error:?}")),
        "the directory namespace reads `__manifest` by its own catalog columns; `location` \
         lives inside the packed `record` struct, so it fails at FieldNotFound: {list_error:?}"
    );

    let describe_error = namespace
        .describe_table_version(DescribeTableVersionRequest {
            id: Some(vec!["node:Person".to_string()]),
            version: Some(person_version as i64),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        cannot_address(&format!("{describe_error:?}")),
        "{describe_error:?}"
    );

    // omnigraph's manifest stays authoritative: refresh ignores the direct
    // `person_ds.append` above (it was never manifest-published), so the row
    // count stays 0 and the version is unchanged.
    mc.refresh().await.unwrap();
    assert_eq!(mc.version(), graph_manifest_version);
    assert_eq!(
        mc.snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        mc.snapshot().dataset("node:Person").unwrap().entity_count,
        0
    );
}

#[tokio::test]
async fn test_snapshot_at_reads_branch_pinned_historical_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let main_manifest_version = mc.version();
    mc.create_branch("feature").await.unwrap();

    let snap = mc.snapshot();
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

    let namespace = branch_manifest_namespace(uri, Some("feature"));
    let request = feature_metadata.to_create_table_version_request(
        "node:Person",
        feature_version,
        1,
        Some("feature"),
    );
    namespace.create_table_version(request).await.unwrap();

    let feature_mc = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    let feature_snapshot =
        ManifestCoordinator::snapshot_at(uri, Some("feature"), feature_mc.version())
            .await
            .unwrap();
    let feature_entry = feature_snapshot.dataset("node:Person").unwrap();
    assert_eq!(feature_entry.published_dataset_version, feature_version);
    assert_eq!(
        feature_entry.native_dataset_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        feature_snapshot
            .open_dataset("node:Person")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        1
    );

    let main_snapshot = ManifestCoordinator::snapshot_at(uri, None, main_manifest_version)
        .await
        .unwrap();
    let main_entry = main_snapshot.dataset("node:Person").unwrap();
    assert_eq!(
        main_entry.published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(main_entry.native_dataset_branch, None);
    assert_eq!(
        main_snapshot
            .open_dataset("node:Person")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_branch_manifest_namespace_uses_entry_owner_branch_for_latest_table_reads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    mc.create_branch("feature").await.unwrap();

    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.published_dataset_version, None)
        .await
        .unwrap();
    let mut feature_person_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_person_ds.append(reader, None).await.unwrap();
    let feature_person_version = feature_person_ds.version().version;
    let feature_person_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        Some("feature"),
        feature_person_version,
    )
    .await
    .unwrap();

    branch_manifest_namespace(uri, Some("feature"))
        .create_table_version(feature_person_metadata.to_create_table_version_request(
            "node:Person",
            feature_person_version,
            1,
            Some("feature"),
        ))
        .await
        .unwrap();

    let feature_namespace = branch_manifest_namespace(uri, Some("feature"));

    let inherited_company = feature_namespace
        .describe_table(DescribeTableRequest {
            id: Some(vec!["node:Company".to_string()]),
            with_table_uri: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    let inherited_company_uri = inherited_company.table_uri.as_deref().unwrap();
    assert!(
        !inherited_company_uri.contains("/tree/feature"),
        "inherited table should resolve to its owning branch, got {inherited_company_uri}"
    );

    let branch_owned_person = feature_namespace
        .describe_table(DescribeTableRequest {
            id: Some(vec!["node:Person".to_string()]),
            with_table_uri: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    let branch_owned_person_uri = branch_owned_person.table_uri.as_deref().unwrap();
    assert!(
        branch_owned_person_uri.contains("/tree/feature"),
        "branch-owned table should resolve to feature branch, got {branch_owned_person_uri}"
    );

    // Lance 9 validates that the resolved manifest belongs to the requested
    // branch (builder.rs "open of branch X resolved a manifest belonging to
    // branch Y"), so `with_branch("feature")` on a main-owned inherited table
    // is now a LOUD error — the substrate enforcing exactly the invariant the
    // entry-owner resolution exists for. Pin both halves: the mismatched open
    // errors, and the owner-branch open (no with_branch — how production
    // opens entries, by owner-resolved location) reads the inherited table.
    let mismatched = DatasetBuilder::from_namespace(
        Arc::clone(&feature_namespace),
        vec!["node:Company".to_string()],
    )
    .await
    .unwrap()
    .with_branch("feature", None)
    .load()
    .await;
    let err = format!(
        "{:?}",
        mismatched.expect_err("branch-mismatched open must fail on v9")
    );
    assert!(
        err.contains("belonging to branch"),
        "expected the v9 branch-consistency error, got: {err}"
    );
    let inherited_company_ds = DatasetBuilder::from_namespace(
        Arc::clone(&feature_namespace),
        vec!["node:Company".to_string()],
    )
    .await
    .unwrap()
    .load()
    .await
    .unwrap();
    assert_eq!(inherited_company_ds.count_rows(None).await.unwrap(), 0);

    let branch_owned_person_ds = DatasetBuilder::from_namespace(
        Arc::clone(&feature_namespace),
        vec!["node:Person".to_string()],
    )
    .await
    .unwrap()
    .with_branch("feature", None)
    .load()
    .await
    .unwrap();
    assert_eq!(branch_owned_person_ds.count_rows(None).await.unwrap(), 1);
    assert_eq!(
        company_entry.native_dataset_branch, None,
        "sanity check: company table stays inherited on feature"
    );
}

#[tokio::test]
async fn test_refresh_observes_external_publish_without_mutating_existing_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut reader = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let frozen_snapshot = reader.snapshot();
    let person_entry = frozen_snapshot.dataset("node:Person").unwrap().clone();
    let manifest_version = reader.version();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader_batch = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader_batch, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();

    branch_manifest_namespace(uri, None)
        .create_table_version(version_metadata.to_create_table_version_request(
            "node:Person",
            person_version,
            1,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reader.version(), manifest_version);
    assert_eq!(
        frozen_snapshot
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        frozen_snapshot
            .open_dataset("node:Person")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        0
    );

    reader.refresh().await.unwrap();
    assert!(reader.version() > manifest_version);
    assert_eq!(
        reader
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_version
    );
    assert_eq!(
        reader
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .entity_count,
        1
    );
}

#[tokio::test]
async fn test_batch_create_table_versions_is_atomic_on_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let manifest_version = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let person_version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();
    let company_version_metadata = table_version_metadata_for_state(
        uri,
        &company_entry.dataset_path,
        None,
        company_entry.published_dataset_version,
    )
    .await
    .unwrap();

    let person_request = person_version_metadata.to_create_table_version_request(
        "node:Person",
        person_version,
        1,
        None,
    );

    let company_request = company_version_metadata.to_create_table_version_request(
        "node:Company",
        company_entry.published_dataset_version,
        company_entry.entity_count,
        None,
    );
    let duplicate_company_request = company_version_metadata.to_create_table_version_request(
        "node:Company",
        company_entry.published_dataset_version,
        company_entry.entity_count + 1,
        None,
    );

    let err = GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[person_request, company_request, duplicate_company_request])
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("is claimed twice in one publish request"),
        "unexpected refusal: {err}"
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .entity_count,
        0
    );
}

#[tokio::test]
async fn test_batch_create_table_versions_rejects_duplicate_requests_without_advancing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let manifest_version = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();
    let request =
        version_metadata.to_create_table_version_request("node:Person", person_version, 1, None);

    let err = GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[request.clone(), request])
        .await
        .unwrap_err();
    // The within-request duplicate and the registry collision are distinct
    // refusals and say so: a caller that batched the same version twice is a
    // different bug from a caller racing a stored registration.
    assert!(
        err.to_string()
            .contains("is claimed twice in one publish request"),
        "unexpected refusal: {err}"
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .entity_count,
        0
    );
}

/// Re-registering the row already stored is not a collision.
///
/// The registry guard exists to catch a DIFFERENT row landing on an occupied
/// `(identity, version)`. An identical re-registration reaches the same folded
/// state whether it is applied or skipped, so refusing it turned a benign
/// publish into a permanent failure (#473). The engine no longer emits one,
/// but the guard should not be the thing that makes it fatal.
#[tokio::test]
async fn test_batch_create_table_versions_allows_identical_reregistration() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();

    let version_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        None,
        person_entry.published_dataset_version,
    )
    .await
    .unwrap();
    let request = version_metadata.to_create_table_version_request(
        "node:Person",
        person_entry.published_dataset_version,
        person_entry.entity_count,
        None,
    );

    GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[request])
        .await
        .expect("re-registering the stored row must not be a conflict");

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    let entry = reopened.snapshot().dataset("node:Person").unwrap().clone();
    assert_eq!(
        entry.published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        entry.native_dataset_branch,
        person_entry.native_dataset_branch
    );
    assert_eq!(entry.entity_count, person_entry.entity_count);
}

/// RFC 0062: a row that differs at an occupied Lance version is an ordinary
/// later registration. It replaces the table's pin under its own manifest
/// version.
#[tokio::test]
async fn test_batch_create_table_versions_later_clock_wins_at_same_lance_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();

    let version_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        None,
        person_entry.published_dataset_version,
    )
    .await
    .unwrap();
    // Same identity, same version, same branch — but a different row count.
    let request = version_metadata.to_create_table_version_request(
        "node:Person",
        person_entry.published_dataset_version,
        person_entry.entity_count + 1,
        None,
    );

    GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[request])
        .await
        .expect("a differing row at an occupied Lance version is a later registration");

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    let entry = reopened.snapshot().dataset("node:Person").unwrap().clone();
    assert_eq!(entry.entity_count, person_entry.entity_count + 1);
    assert_eq!(
        entry.published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(entry.manifest_version, reopened.version());
    assert!(entry.manifest_version > person_entry.manifest_version);
    let ds = open_manifest_dataset(uri, None).await.unwrap();
    let rows = read_manifest_rows(&ds).await.unwrap();
    assert_eq!(
        rows.tables.len(),
        reopened.known_state.entries.len(),
        "a later registration replaces the pin in the table's one row"
    );
    assert_eq!(
        rows.buffer.replaced(),
        [ReplacedTable {
            replaced_at: reopened.version(),
            before: ReplacedRow::Table(Box::new(
                rows_written_with(uri, &rows.head)
                    .await
                    .tables
                    .into_iter()
                    .find(|table| table.registration.identity == person_entry.identity)
                    .unwrap()
            )),
        }],
        "the row the publish read stays for the head, which the publish did not replace"
    );
    assert_eq!(
        ds.count_rows(None).await.unwrap(),
        rows.tables.len() + 3,
        "one row per table, the head commit and the replaced row"
    );
}

/// RFC 0062: a registration with a LOWER Lance version number on another
/// native ref wins the fold when it was published later; the pre-v7 fold
/// picked the greater number and never showed it.
#[tokio::test]
async fn test_later_registration_with_lower_lance_version_wins_the_fold() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.published_dataset_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    for name in ["Alice", "Bob"] {
        let batch = entity_batch(
            Arc::clone(&person_schema),
            format!("person-{name}"),
            name,
            Some(30),
        );
        let reader = RecordBatchIterator::new(vec![Ok(batch)], Arc::clone(&person_schema));
        feature_ds.append(reader, None).await.unwrap();
    }
    let feature_version = feature_ds.version().version;
    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        Some("feature"),
        feature_version,
    )
    .await
    .unwrap();
    GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[feature_metadata.to_create_table_version_request(
            "node:Person",
            feature_version,
            2,
            Some("feature"),
        )])
        .await
        .unwrap();

    let batch = entity_batch(
        Arc::clone(&person_schema),
        "person-carol",
        "Carol",
        Some(30),
    );
    let reader = RecordBatchIterator::new(vec![Ok(batch)], Arc::clone(&person_schema));
    person_ds.append(reader, None).await.unwrap();
    let main_version = person_ds.version().version;
    assert!(main_version < feature_version);
    let main_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, main_version)
            .await
            .unwrap();
    let mut warm = ManifestCoordinator::open(uri).await.unwrap();
    warm.commit(&[DatasetUpdate {
        identity: person_entry.identity,
        type_key: "node:Person".to_string(),
        published_dataset_version: main_version,
        native_dataset_branch: None,
        entity_count: 1,
        version_metadata: main_metadata,
    }])
    .await
    .unwrap();
    let folded = warm.snapshot().dataset("node:Person").unwrap().clone();

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    let entry = reopened.snapshot().dataset("node:Person").unwrap().clone();
    assert_eq!(entry.published_dataset_version, main_version);
    assert_eq!(entry.native_dataset_branch, None);
    assert_eq!(entry.entity_count, 1);
    assert_eq!(entry.manifest_version, reopened.version());
    assert_eq!(
        folded.published_dataset_version,
        entry.published_dataset_version
    );
    assert_eq!(folded.native_dataset_branch, entry.native_dataset_branch);
    assert_eq!(folded.entity_count, entry.entity_count);
    assert_eq!(folded.manifest_version, entry.manifest_version);
}

/// RFC 0062: one publish lands at one manifest version, which is the row
/// key, so a second registration of one identity in the same batch is refused
/// before the merge-insert, whatever its Lance version number.
#[tokio::test]
async fn test_publish_refuses_two_registrations_of_one_identity_in_one_batch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let manifest_version = mc.version();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let mut versions = Vec::new();
    for name in ["Alice", "Bob"] {
        let batch = entity_batch(
            Arc::clone(&person_schema),
            format!("person-{name}"),
            name,
            Some(30),
        );
        let reader = RecordBatchIterator::new(vec![Ok(batch)], Arc::clone(&person_schema));
        person_ds.append(reader, None).await.unwrap();
        versions.push(person_ds.version().version);
    }
    let mut requests = Vec::new();
    for (rows, version) in versions.iter().enumerate() {
        let metadata =
            table_version_metadata_for_state(uri, &person_entry.dataset_path, None, *version)
                .await
                .unwrap();
        requests.push(metadata.to_create_table_version_request(
            "node:Person",
            *version,
            rows as u64 + 1,
            None,
        ));
    }

    let err = GraphNamespacePublisher::new(uri, None)
        .publish_requests(&requests)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("is claimed twice in one publish request"),
        "unexpected refusal: {err}"
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
}

#[tokio::test]
async fn test_batch_create_table_versions_allows_owner_branch_handoff_at_same_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut main_mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    main_mc.create_branch("feature").await.unwrap();

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

    GraphNamespacePublisher::new(uri, Some("experiment"))
        .publish_requests(&[experiment_metadata.to_create_table_version_request(
            "node:Person",
            feature_version,
            1,
            Some("experiment"),
        )])
        .await
        .unwrap();

    let experiment_mc = ManifestCoordinator::open_at_branch(uri, "experiment")
        .await
        .unwrap();
    let experiment_snapshot = experiment_mc.snapshot();
    let experiment_entry = experiment_snapshot.dataset("node:Person").unwrap();
    assert_eq!(experiment_entry.published_dataset_version, feature_version);
    assert_eq!(
        experiment_entry.native_dataset_branch.as_deref(),
        Some("experiment")
    );
}

#[tokio::test]
async fn test_staged_namespace_lists_native_table_versions_before_publish() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let namespace = staged_table_namespace(uri, "node:Person", &person_entry.dataset_path, None);
    let listed = namespace
        .list_table_versions(ListTableVersionsRequest {
            id: Some(vec!["node:Person".to_string()]),
            descending: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let listed_versions: Vec<u64> = listed
        .versions
        .into_iter()
        .map(|version| version.version as u64)
        .collect();
    assert_eq!(listed_versions, vec![1, person_version]);

    let described = namespace
        .describe_table_version(DescribeTableVersionRequest {
            id: Some(vec!["node:Person".to_string()]),
            version: Some(person_version as i64),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(described.version.version as u64, person_version);
}

#[derive(Clone)]
struct RecordingPublisher {
    inner: Arc<GraphNamespacePublisher>,
    requests: Arc<Mutex<Vec<CreateTableVersionRequest>>>,
}

impl RecordingPublisher {
    fn new(root_uri: &str, branch: Option<&str>) -> Self {
        Self {
            inner: Arc::new(GraphNamespacePublisher::new(root_uri, branch)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn recorded_requests(&self) -> Vec<CreateTableVersionRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ManifestBatchPublisher for RecordingPublisher {
    async fn publish_with_precondition(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome> {
        let requests: Vec<CreateTableVersionRequest> = changes
            .iter()
            .filter_map(|change| match change {
                ManifestChange::Update(update) => Some(update.to_create_table_version_request()),
                ManifestChange::RegisterTable(_)
                | ManifestChange::RenameTable(_)
                | ManifestChange::SchemaContract(_)
                | ManifestChange::Tombstone(_) => None,
            })
            .collect();
        self.requests.lock().await.extend_from_slice(&requests);
        self.inner
            .publish_with_precondition(changes, expected_table_versions, lineage, precondition)
            .await
    }
}

struct FailingPublisher;

#[async_trait]
impl ManifestBatchPublisher for FailingPublisher {
    async fn publish_with_precondition(
        &self,
        _changes: &[ManifestChange],
        _expected_table_versions: &ExpectedTableVersions,
        _lineage: Option<&LineageIntent>,
        _precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome> {
        Err(OmniError::manifest(
            "injected batch publisher failure".to_string(),
        ))
    }
}

#[tokio::test]
async fn test_commit_routes_through_injected_batch_publisher() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();

    let recording = RecordingPublisher::new(uri, None);
    mc = mc.with_batch_publisher(Arc::new(recording.clone()));

    mc.commit(&[DatasetUpdate {
        identity: person_entry.identity,
        type_key: "node:Person".to_string(),
        published_dataset_version: person_version,
        native_dataset_branch: None,
        entity_count: 1,
        version_metadata: version_metadata.clone(),
    }])
    .await
    .unwrap();

    let recorded = recording.recorded_requests().await;
    assert_eq!(recorded.len(), 1);
    let request = &recorded[0];
    assert_eq!(
        request.id.as_ref().unwrap(),
        &vec!["node:Person".to_string()]
    );
    assert_eq!(request.version as u64, person_version);
    assert_eq!(request.manifest_path, version_metadata.manifest_path());
    assert_eq!(
        request.manifest_size,
        version_metadata.manifest_size().map(|size| size as i64)
    );
    assert_eq!(request.e_tag.as_deref(), version_metadata.e_tag());
    assert_eq!(
        request.naming_scheme.as_deref(),
        version_metadata.naming_scheme()
    );
    assert_eq!(
        request
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(OMNIGRAPH_ROW_COUNT_KEY))
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        mc.snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_version
    );
}

#[tokio::test]
async fn test_commit_failure_from_injected_batch_publisher_preserves_visible_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let manifest_version = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = entity_batch(Arc::clone(&person_schema), "person-1", "Alice", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, person_version)
            .await
            .unwrap();

    mc = mc.with_batch_publisher(Arc::new(FailingPublisher));
    let err = mc
        .commit(&[DatasetUpdate {
            identity: person_entry.identity,
            type_key: "node:Person".to_string(),
            published_dataset_version: person_version,
            native_dataset_branch: None,
            entity_count: 1,
            version_metadata,
        }])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("injected batch publisher failure"));
    assert_eq!(mc.version(), manifest_version);
    assert_eq!(
        mc.snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
    assert_eq!(
        mc.snapshot().dataset("node:Person").unwrap().entity_count,
        0
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        person_entry.published_dataset_version
    );
}

/// Drive Person to a fresh on-disk dataset version `v` (returns the new
/// version number) and produce a `DatasetUpdate` ready to publish.
async fn append_person_and_make_update(
    uri: &str,
    person_entry: &DatasetEntry,
    name: &str,
) -> DatasetUpdate {
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let row = entity_batch(
        Arc::clone(&person_schema),
        format!("person-{name}"),
        name,
        Some(30),
    );
    let reader = RecordBatchIterator::new(vec![Ok(row)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let new_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, new_version)
            .await
            .unwrap();
    DatasetUpdate {
        identity: person_entry.identity,
        type_key: "node:Person".to_string(),
        published_dataset_version: new_version,
        native_dataset_branch: None,
        entity_count: 1,
        version_metadata,
    }
}

#[tokio::test]
async fn test_commit_with_expected_accepts_matching_versions() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    let update = append_person_and_make_update(uri, &person_entry, "Alice").await;
    let mut expected = HashMap::new();
    // After init, every table is at table_version=1 — assert that.
    expected.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );
    expected.insert(
        company_entry.identity,
        TableVersionExpectation {
            table_key: "node:Company".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );

    mc.commit_with_expected(std::slice::from_ref(&update), &expected)
        .await
        .expect("matching expected versions should publish cleanly");

    let after = mc.snapshot();
    assert_eq!(
        after
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        update.published_dataset_version
    );
}

#[tokio::test]
async fn test_commit_with_expected_rejects_stale_with_typed_details() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();

    // Writer A advances Person.
    let update_a = append_person_and_make_update(uri, &person_entry, "Alice").await;
    let advanced_version = update_a.published_dataset_version;
    mc.commit(&[update_a]).await.unwrap();

    // Writer B then tries to commit, asserting Person is still at v=1.
    let update_b = append_person_and_make_update(uri, &person_entry, "Bob").await;
    let mut stale_expected = HashMap::new();
    stale_expected.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );

    let err = mc
        .commit_with_expected(&[update_b], &stale_expected)
        .await
        .expect_err("stale expected_table_versions should reject");

    match err {
        OmniError::Manifest(m) => match m.details {
            Some(ManifestConflictDetails::PublishedDatasetVersionMismatch {
                type_key,
                expected_published_dataset_version,
                actual_published_dataset_version,
            }) => {
                assert_eq!(type_key, "node:Person");
                assert_eq!(expected_published_dataset_version, 1);
                assert_eq!(actual_published_dataset_version, advanced_version);
            }
            other => panic!(
                "expected PublishedDatasetVersionMismatch details, got {:?}",
                other
            ),
        },
        other => panic!("expected OmniError::Manifest, got {:?}", other),
    }
}

/// RFC 0062: two registrations of one identity can carry equal Lance version
/// numbers on different native refs, so a pin naming one ref must be refused
/// when the winner at that number sits on another.
#[tokio::test]
async fn test_commit_with_expected_rejects_same_number_on_another_native_ref() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.dataset_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.published_dataset_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    let batch = entity_batch(
        Arc::clone(&person_schema),
        "person-alice",
        "Alice",
        Some(30),
    );
    let reader = RecordBatchIterator::new(vec![Ok(batch)], Arc::clone(&person_schema));
    feature_ds.append(reader, None).await.unwrap();
    let feature_version = feature_ds.version().version;

    let batch = entity_batch(Arc::clone(&person_schema), "person-bob", "Bob", Some(30));
    let reader = RecordBatchIterator::new(vec![Ok(batch)], Arc::clone(&person_schema));
    person_ds.append(reader, None).await.unwrap();
    let main_version = person_ds.version().version;
    assert_eq!(
        feature_version, main_version,
        "the test needs equal Lance numbers on the two refs"
    );

    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.dataset_path,
        Some("feature"),
        feature_version,
    )
    .await
    .unwrap();
    GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[feature_metadata.to_create_table_version_request(
            "node:Person",
            feature_version,
            1,
            Some("feature"),
        )])
        .await
        .unwrap();
    let main_metadata =
        table_version_metadata_for_state(uri, &person_entry.dataset_path, None, main_version)
            .await
            .unwrap();
    GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[main_metadata.to_create_table_version_request(
            "node:Person",
            main_version,
            1,
            None,
        )])
        .await
        .unwrap();

    let mut mc = ManifestCoordinator::open(uri).await.unwrap();
    let winner = mc.snapshot().dataset("node:Person").unwrap().clone();
    assert_eq!(winner.published_dataset_version, main_version);
    assert_eq!(winner.native_dataset_branch, None);
    let manifest_version = mc.version();

    let update = append_person_and_make_update(uri, &person_entry, "Carol").await;
    let mut feature_pin = HashMap::new();
    feature_pin.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: feature_version,
            native_ref: NativeRefPin::Exact(Some("feature".to_string())),
        },
    );
    let err = mc
        .commit_with_expected(std::slice::from_ref(&update), &feature_pin)
        .await
        .expect_err("a pin on the feature ref must not be satisfied by the root-ref winner");
    assert!(
        err.to_string().contains("native_ref:"),
        "unexpected refusal: {err}"
    );
    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        main_version
    );

    let mut root_pin = HashMap::new();
    root_pin.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: main_version,
            native_ref: NativeRefPin::Exact(None),
        },
    );
    mc.commit_with_expected(std::slice::from_ref(&update), &root_pin)
        .await
        .expect("the same pin naming the root lineage matches the winner");
    assert_eq!(
        mc.snapshot()
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        update.published_dataset_version
    );
}

#[tokio::test]
async fn test_commit_with_expected_catches_drift_on_untouched_table() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    // Writer A advances Company.
    let mut company_ds = Dataset::open(&format!("{}/{}", uri, company_entry.dataset_path))
        .await
        .unwrap();
    let company_schema = Arc::new(company_ds.schema().into());
    let row = entity_batch(Arc::clone(&company_schema), "company-1", "Acme", None);
    let reader = RecordBatchIterator::new(vec![Ok(row)], company_schema);
    company_ds.append(reader, None).await.unwrap();
    let company_version = company_ds.version().version;
    let company_metadata =
        table_version_metadata_for_state(uri, &company_entry.dataset_path, None, company_version)
            .await
            .unwrap();
    mc.commit(&[DatasetUpdate {
        identity: company_entry.identity,
        type_key: "node:Company".to_string(),
        published_dataset_version: company_version,
        native_dataset_branch: None,
        entity_count: 1,
        version_metadata: company_metadata,
    }])
    .await
    .unwrap();

    // Writer B writes Person but asserts Company is still at v=1.
    let update_person = append_person_and_make_update(uri, &person_entry, "Bob").await;
    let mut expected = HashMap::new();
    expected.insert(
        company_entry.identity,
        TableVersionExpectation {
            table_key: "node:Company".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );

    let err = mc
        .commit_with_expected(&[update_person], &expected)
        .await
        .expect_err("drift on an untouched expected table should reject");

    let OmniError::Manifest(m) = err else {
        panic!("expected OmniError::Manifest");
    };
    match m.details {
        Some(ManifestConflictDetails::PublishedDatasetVersionMismatch {
            ref type_key,
            expected_published_dataset_version,
            actual_published_dataset_version,
        }) => {
            assert_eq!(type_key, "node:Company");
            assert_eq!(expected_published_dataset_version, 1);
            assert_eq!(actual_published_dataset_version, company_version);
        }
        other => panic!("expected PublishedDatasetVersionMismatch, got {:?}", other),
    }
}

#[tokio::test]
async fn test_commit_with_expected_unknown_table_reports_actual_zero() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();

    let mut expected = HashMap::new();
    expected.insert(
        TableIdentity::new(99_999, 1).unwrap(),
        TableVersionExpectation {
            table_key: "node:DoesNotExist".to_string(),
            table_version: 7,
            native_ref: NativeRefPin::Unchecked,
        },
    );
    let err = mc
        .commit_with_expected(&[], &expected)
        .await
        .expect_err("unknown expected table should reject");

    let OmniError::Manifest(m) = err else {
        panic!("expected OmniError::Manifest");
    };
    match m.details {
        Some(ManifestConflictDetails::PublishedDatasetVersionMismatch {
            type_key,
            expected_published_dataset_version,
            actual_published_dataset_version,
        }) => {
            assert_eq!(type_key, "node:DoesNotExist");
            assert_eq!(expected_published_dataset_version, 7);
            assert_eq!(actual_published_dataset_version, 0);
        }
        other => panic!("expected PublishedDatasetVersionMismatch, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_publish_with_overlapping_expected_versions_one_succeeds() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();

    // Advance the Person dataset once so we have a real on-disk version 2 that
    // both publishers can target. Both attempt to land the *same*
    // `version:node:Person@v=2` row in `__manifest`, which is the row-level
    // CAS conflict the publisher must detect: load_publish_state at the same
    // baseline → pre-check passes for both → only one merge_insert can land
    // the unique `object_id`.
    let update = append_person_and_make_update(uri, &person_entry, "Alice").await;

    let mut expected = HashMap::new();
    expected.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );

    let publisher_a = GraphNamespacePublisher::new(uri, None);
    let publisher_b = GraphNamespacePublisher::new(uri, None);
    let changes_a = vec![ManifestChange::Update(update.clone())];
    let changes_b = vec![ManifestChange::Update(update)];
    let expected_a = expected.clone();
    let expected_b = expected;

    let (res_a, res_b) = tokio::join!(
        async { publisher_a.publish(&changes_a, &expected_a, None).await },
        async { publisher_b.publish(&changes_b, &expected_b, None).await }
    );

    let (succeeded, err) = match (res_a, res_b) {
        (Ok(_), Err(e)) => (1, e),
        (Err(e), Ok(_)) => (1, e),
        (Ok(_), Ok(_)) => panic!("both writers committed -- OCC failed"),
        (Err(a), Err(b)) => panic!("both writers failed: {:?} / {:?}", a, b),
    };
    assert_eq!(succeeded, 1, "exactly one writer must succeed");

    let OmniError::Manifest(m) = err else {
        panic!("expected OmniError::Manifest, got {:?}", err);
    };
    // The losing writer surfaces either PublishedDatasetVersionMismatch (its retry's
    // pre-check observed the winner's advance) or a plain Conflict (Lance
    // row-level CAS rejected, retry exhausted before the pre-check fired).
    // Both are acceptable typed conflict signals; what matters is that the
    // failure is not silent.
    use crate::error::ManifestErrorKind;
    assert!(
        matches!(m.kind, ManifestErrorKind::Conflict),
        "expected Conflict-kind manifest error, got {:?}: {}",
        m.kind,
        m.message,
    );
    if let Some(ManifestConflictDetails::PublishedDatasetVersionMismatch {
        ref type_key,
        expected_published_dataset_version,
        ..
    }) = m.details
    {
        assert_eq!(type_key, "node:Person");
        assert_eq!(expected_published_dataset_version, 1);
    }

    // Manifest must reflect exactly one new commit on Person at the requested
    // version (no duplicate version rows).
    let mc = ManifestCoordinator::open(uri).await.unwrap();
    let entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    assert!(
        entry.published_dataset_version > 1,
        "Person should have advanced past v=1"
    );
}

#[tokio::test]
async fn test_init_stamps_internal_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    ManifestCoordinator::init(uri, &catalog).await.unwrap();

    let ds = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(
        ds.version().version,
        1,
        "fresh __manifest HEAD must be its Create commit; init must not append config or stamp commits",
    );
    assert_eq!(
        super::migrations::read_stamp(&ds),
        Some(super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION),
        "init should stamp the manifest at the current internal schema version",
    );

    // The stamp must ride the Create commit itself — checking out manifest
    // version one (the genesis Create write) must already show it. This is the
    // torn-init guarantee: there is no on-disk moment where `__manifest`
    // exists unstamped.
    let genesis = ds.checkout_version(1).await.unwrap();
    assert_eq!(
        super::migrations::read_stamp(&genesis),
        Some(super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION),
        "the stamp must land in the same Lance commit that creates __manifest",
    );
}

// The absent-stamp arm of the open guard. An unstamped manifest that carries
// the modern (v5+) identity columns cannot be a genuine pre-stamp v1 store,
// but the remaining metadata cannot distinguish an init interrupted under a
// pre-atomic-stamp binary from later metadata damage. The guard must name both
// possibilities, fail closed, and make delete-and-re-init conditional on the
// operator independently knowing that initialization never completed.
#[tokio::test]
async fn unstamped_modern_manifest_is_refused_as_interrupted_init_or_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    ManifestCoordinator::init(uri, &catalog).await.unwrap();

    let mut ds = open_manifest_dataset(uri, None).await.unwrap();
    super::migrations::remove_stamp_for_test(&mut ds)
        .await
        .unwrap();

    let ds = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(super::migrations::read_stamp(&ds), None);
    let err = super::migrations::guard_stamp(&ds)
        .expect_err("an unstamped manifest must be refused")
        .to_string();
    assert!(
        err.contains("interrupted `omnigraph init`")
            && err.contains("damaged or externally modified metadata"),
        "the refusal must name both possible causes: {err}"
    );
    assert!(
        err.contains("cannot safely distinguish those cases")
            && err.contains("If you know initialization never completed")
            && err.contains("Otherwise preserve the root"),
        "the refusal must fail closed and make deletion conditional: {err}"
    );
    assert!(
        !err.contains("holds no committed data") && !err.contains("0.3.1"),
        "the refusal must neither assume an empty graph nor misdiagnose it as ancient: {err}"
    );
}

// The unreadable-stamp arm: a stamp key that is present but not a version
// number must be refused naming the raw value — never classified as absent,
// because an explicitly corrupt value should not flow even into the modern
// absent-stamp arm's conditional delete advice.
#[tokio::test]
async fn unreadable_stamp_is_refused_without_delete_advice() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    ManifestCoordinator::init(uri, &catalog).await.unwrap();

    let mut ds = open_manifest_dataset(uri, None).await.unwrap();
    super::migrations::set_raw_stamp_for_test(&mut ds, "not-a-number")
        .await
        .unwrap();

    let ds = open_manifest_dataset(uri, None).await.unwrap();
    let err = super::migrations::guard_stamp(&ds)
        .expect_err("an unreadable stamp must be refused")
        .to_string();
    assert!(
        err.contains("not-a-number"),
        "the refusal must name the raw value: {err}"
    );
    assert!(
        !err.contains("Delete the graph root") && !err.contains("0.3.1"),
        "corrupt metadata must not trigger delete advice or the ancient misdiagnosis: {err}"
    );
}

// The internal-schema stamp is gated at the graph (main) level. That is sufficient
// for supported inputs precisely because a branch cannot diverge from main's stamp
// under single-binary operation: a fresh graph stamps main at CURRENT, `create_branch`
// forks main's `__manifest` (carrying its schema metadata, stamp included), and the
// publisher writes rows without re-stamping. So every branch is always at main's
// stamp. (A divergent branch stamp needs concurrent *multi-version* writers — an
// unsupported topology, recorded as a known gap in docs/dev/invariants.md.) This is
// the "if mixed branch stamps are impossible for supported inputs, prove it" test.
#[tokio::test]
async fn branch_inherits_main_internal_schema_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    mc.create_branch("feature").await.unwrap();

    let main_ds = open_manifest_dataset(uri, None).await.unwrap();
    let feature_ds = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    assert_eq!(
        super::migrations::read_stamp(&main_ds),
        Some(super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION),
        "fresh graph stamps main at CURRENT",
    );
    assert_eq!(
        super::migrations::read_stamp(&feature_ds),
        super::migrations::read_stamp(&main_ds),
        "create_branch forks main's stamp — a branch never diverges under single-binary operation",
    );
}

#[tokio::test]
async fn test_publish_rejects_manifest_stamped_at_future_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();

    // Stamp the manifest at a version higher than this binary knows about.
    let future = super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION + 99;
    {
        let mut ds = open_manifest_dataset(uri, None).await.unwrap();
        ds.update_schema_metadata([(
            "omnigraph:internal_schema_version".to_string(),
            Some(future.to_string()),
        )])
        .await
        .unwrap();
    }

    let mut expected = HashMap::new();
    expected.insert(
        person_entry.identity,
        TableVersionExpectation {
            table_key: "node:Person".to_string(),
            table_version: 1,
            native_ref: NativeRefPin::Unchecked,
        },
    );
    let err = GraphNamespacePublisher::new(uri, None)
        .publish(&[], &expected, None)
        .await
        .expect_err("future-stamped manifest should reject open-for-write");
    let msg = err.to_string();
    assert!(
        msg.contains("upgrade omnigraph") && msg.contains(&future.to_string()),
        "expected forward-version refusal, got: {}",
        msg,
    );
}

#[test]
fn manifest_column_helpers_return_error_for_bad_schema() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "table_key",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![1_u64]))],
    )
    .unwrap();

    let err = string_column(&batch, "table_key").unwrap_err();
    assert!(err.to_string().contains("table_key"));
}

// ── RFC-013 Phase 7 / step 5: the `graph_head` concurrency gate ──────────────
//
// Two (or N) writers committing DISJOINT tables on the same branch still share
// one mutable `graph_head:main` row (one `object_id`, `WhenMatched::UpdateAll`).
// Their table-version rows never collide (distinct `object_id`s), so the *only*
// row-level CAS contention is on `graph_head:main`. Two contracts coexist:
// legacy `Any` publishers retry, re-parent, and eventually form one linear DAG;
// RFC-022 `ExactGraphHead` publishers instead reject the stale prepared write
// after the first winner changes authority. Both rely on the same shared row to
// prevent a fork; the tests below pin both behaviors explicitly.

/// A microsecond UNIX timestamp for a `LineageIntent`, matching the genesis /
/// commit-graph `created_at` unit.
fn lineage_now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Race two lineage-only publishes against the same exact named-branch head.
/// Returns after proving exactly one committed and the loser surfaced a typed
/// read-set change. `establish_head=false` exercises the load-bearing absent-row
/// case on a fresh branch; `true` exercises ordinary head advancement.
async fn assert_exact_named_head_race(establish_head: bool) {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    mc.create_branch("feature").await.unwrap();

    let publisher = GraphNamespacePublisher::new(uri, Some("feature"));
    let empty = HashMap::new();
    let expected_head = if establish_head {
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            branch: Some("feature".to_string()),
            actor_id: None,
            merged_parent: None,
            created_at: lineage_now_micros(),
            history_release_bytes: HistoryReleaseBytes::PRODUCTION,
        };
        let outcome = publisher
            .publish(&[], &empty, Some(&intent))
            .await
            .expect("establish named-branch graph head");
        Some(outcome.head.graph_commit_id)
    } else {
        None
    };

    let branch_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let state = read_manifest_state(&branch_manifest).await.unwrap();
    assert_eq!(
        state.graph_heads,
        expected_head
            .iter()
            .map(|head| ("feature".to_string(), head.clone()))
            .collect(),
        "the head inherited from main must not count as the exact head of `feature`"
    );
    let branch_identifier = branch_manifest.branch_identifier().await.unwrap();

    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        Some("feature"),
        branch_identifier,
        expected_head.clone(),
    ));
    let intent_a = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: Some("feature".to_string()),
        actor_id: Some("act-a".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: Some("feature".to_string()),
        actor_id: Some("act-b".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let publisher_a = GraphNamespacePublisher::new(uri, Some("feature"));
    let publisher_b = GraphNamespacePublisher::new(uri, Some("feature"));
    let precondition_a = precondition.clone();
    let precondition_b = precondition;

    let (result_a, result_b) = tokio::join!(
        async {
            publisher_a
                .publish_with_precondition(&[], &empty, Some(&intent_a), &precondition_a)
                .await
        },
        async {
            publisher_b
                .publish_with_precondition(&[], &empty, Some(&intent_b), &precondition_b)
                .await
        }
    );

    let (winner_id, loser_error) = match (result_a, result_b) {
        (Ok(outcome), Err(err)) => {
            assert_intent_nonce(&outcome.head.graph_commit_id, &intent_a);
            (outcome.head.graph_commit_id, err)
        }
        (Err(err), Ok(outcome)) => {
            assert_intent_nonce(&outcome.head.graph_commit_id, &intent_b);
            (outcome.head.graph_commit_id, err)
        }
        (Ok(_), Ok(_)) => panic!("exact-head race silently re-parented both writers"),
        (Err(a), Err(b)) => panic!("both exact-head writers failed: {a:?} / {b:?}"),
    };

    let OmniError::Manifest(error) = loser_error else {
        panic!("expected typed manifest conflict, got {loser_error:?}");
    };
    assert_eq!(
        error.details,
        Some(ManifestConflictDetails::ReadSetChanged {
            member: "graph_head:feature".to_string(),
            expected: expected_head,
            actual: Some(winner_id.clone()),
        })
    );

    let branch_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let (commits, heads) = read_graph_lineage(uri, &branch_manifest).await.unwrap();
    assert_eq!(heads.get("feature"), Some(&winner_id));
    assert_eq!(
        commits
            .iter()
            .filter(|commit| {
                [&intent_a, &intent_b].into_iter().any(|intent| {
                    omnigraph_core::graph_commit_id::parse_history_block_id(&commit.graph_commit_id)
                        .unwrap()
                        .is_some_and(|id| id.nonce.to_string() == intent.graph_commit_id)
                })
            })
            .count(),
        1,
        "the rejected intent must not leave a graph commit"
    );
    let settled = settled_commits(uri).await;
    assert!(
        settled.keys().all(|id| {
            omnigraph_core::graph_commit_id::parse_history_block_id(id)
                .unwrap()
                .is_none_or(|id| {
                    id.nonce.to_string() != intent_a.graph_commit_id
                        && id.nonce.to_string() != intent_b.graph_commit_id
                })
        }),
        "`__history` holds the commits both attempts read, never the one either attempted: \
         {settled:?}"
    );
}

/// The records of the commits of the branch `dataset` is checked out on, oldest
/// first, and the exact head of that branch. The commits are the ones the
/// lineage of the head holds.
async fn read_graph_lineage(
    uri: &str,
    dataset: &Dataset,
) -> Result<(Vec<GraphLineageRow>, HashMap<String, String>)> {
    let (state, ManifestRows { head, buffer, .. }) =
        super::state::read_manifest_state_and_rows(dataset).await?;
    let graph = CommitGraph::from_head(
        uri,
        dataset.session(),
        head.clone(),
        buffer.commits(),
        HistoryCache::default(),
    );
    let chain: Vec<GraphCommit> = graph.lineage().await?.first_parent_chain()?;
    let mut settled = super::history::read_lineage(uri, &dataset.session()).await?;
    let rows: Vec<GraphLineageRow> = chain
        .iter()
        .map(|commit| {
            std::iter::once(&head)
                .chain(buffer.commits())
                .find(|held| held.graph_commit_id == commit.graph_commit_id)
                .cloned()
                .or_else(|| settled.remove(&commit.graph_commit_id))
                .expect("a commit of the chain is the head, buffered or in `__history`")
        })
        .collect();
    assert_eq!(
        rows.iter()
            .cloned()
            .map(super::commit_graph::graph_commit_from_manifest_row)
            .collect::<Vec<_>>(),
        chain
    );
    Ok((rows, state.graph_heads))
}

/// Every commit `__history` holds, by id.
async fn settled_commits(uri: &str) -> HashMap<String, GraphLineageRow> {
    super::history::read_lineage(uri, &crate::lance_access::control_session())
        .await
        .unwrap()
}

/// Number of distinct immutable history records.
async fn history_row_count(uri: &str) -> usize {
    super::history::read_records(uri, &crate::lance_access::control_session())
        .await
        .unwrap()
        .len()
}

/// A lineage intent on `branch` under a fresh `fixture-` commit id, a singleton
/// identity; the placement and retry tests supply production intent nonces.
fn lineage_intent(branch: Option<&str>, merged_parent: Option<BranchRecords>) -> LineageIntent {
    LineageIntent {
        graph_commit_id: format!("fixture-{}", ulid::Ulid::new()),
        branch: branch.map(str::to_string),
        actor_id: None,
        merged_parent,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    }
}

/// A [`lineage_intent`] whose commit takes about 4 KiB of the release budget,
/// so that about `HISTORY_RELEASE_BYTES / 4096` of them fill a buffer.
fn fat_intent(branch: Option<&str>, merged_parent: Option<BranchRecords>) -> LineageIntent {
    LineageIntent {
        actor_id: Some("a".repeat(4000)),
        ..lineage_intent(branch, merged_parent)
    }
}

/// A [`fat_intent`] with a production nonce, which the publish places in a
/// history block.
fn fat_block_intent(branch: Option<&str>) -> LineageIntent {
    LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        ..fat_intent(branch, None)
    }
}

/// Publish lineage-only commits until the next publish of `coordinator`
/// releases its buffer, and return their ids.
async fn fill_buffer(coordinator: &mut ManifestCoordinator, branch: Option<&str>) -> Vec<String> {
    let mut published = Vec::new();
    while !coordinator
        .buffer()
        .is_full(coordinator.head(), &coordinator.head_record().tables)
        .unwrap()
    {
        let intent = fat_intent(branch, None);
        coordinator
            .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        published.push(intent.graph_commit_id);
    }
    published
}

/// The rows of the latest `__manifest` version of main.
async fn main_rows(uri: &str) -> ManifestRows {
    read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap()
}

fn assert_intent_nonce(commit_id: &str, intent: &LineageIntent) {
    let id = omnigraph_core::graph_commit_id::parse_history_block_id(commit_id)
        .unwrap()
        .expect("canonical publication intents allocate block commit ids");
    assert_eq!(id.nonce.to_string(), intent.graph_commit_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_head_publish_rejects_reparent_on_fresh_and_established_named_branch() {
    assert_exact_named_head_race(false).await;
    assert_exact_named_head_race(true).await;
}

#[tokio::test]
async fn exact_publish_rejects_named_branch_delete_recreate_aba() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    mc.create_branch("feature").await.unwrap();

    let old_branch = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let old_identifier = old_branch.branch_identifier().await.unwrap();
    assert!(
        read_manifest_state(&old_branch)
            .await
            .unwrap()
            .graph_heads
            .is_empty(),
        "fresh named branch starts without an exact head"
    );

    // Recreate the same name at the same logical fork point. Numeric manifest
    // version and exact graph-head absence can repeat; only Lance's native
    // branch identifier distinguishes the incarnation.
    mc.delete_branch("feature").await.unwrap();
    assert!(
        matches!(
            probe_dataset_latest_incarnation(&old_branch, Some("feature")).await,
            Err(OmniError::BranchNotFound { .. })
        ),
        "a cached native handle must reject retired live authority"
    );
    let archived = crate::branch_control::archived_manifest_branch(
        &old_branch,
        old_branch.manifest().branch.as_deref().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(archived.identifier, old_identifier);
    assert!(matches!(
        old_branch.branch_identifier().await,
        Err(lance::Error::RefNotFound { .. })
    ));
    mc.create_branch("feature").await.unwrap();
    assert_ne!(
        probe_dataset_latest_incarnation(&old_branch, Some("feature"))
            .await
            .unwrap()
            .branch_identifier,
        old_identifier,
        "the freshness probe must resolve the replacement incarnation"
    );
    let recreated = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let recreated_identifier = recreated.branch_identifier().await.unwrap();
    assert_ne!(old_identifier, recreated_identifier);

    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        Some("feature"),
        old_identifier,
        None,
    ));
    let err = GraphNamespacePublisher::new(uri, Some("feature"))
        .publish_with_precondition(&[], &HashMap::new(), None, &precondition)
        .await
        .expect_err("recreated branch must reject the old incarnation token");
    let OmniError::Manifest(error) = err else {
        panic!("expected typed manifest conflict, got {err:?}");
    };
    match error.details {
        Some(ManifestConflictDetails::ReadSetChanged {
            member,
            expected: Some(expected),
            actual: Some(actual),
        }) => {
            assert_eq!(member, "branch_identifier:feature");
            assert_ne!(expected, actual);
        }
        other => panic!("expected branch-identifier ReadSetChanged, got {other:?}"),
    }
}

/// Append one row to a two-column NODE table (`id`, `name`) and return the
/// resulting `DatasetUpdate` at the new on-disk version. Generalizes
/// `append_person_and_make_update` to any node table whose schema is `(id:
/// String, name: String[, ...])`; the extra `Person.age` column is filled null
/// when present so the same helper drives both `node:Person` and `node:Company`.
async fn append_node_row_and_make_update(
    uri: &str,
    entry: &DatasetEntry,
    id: &str,
    name: &str,
) -> DatasetUpdate {
    let mut ds = Dataset::open(&format!("{}/{}", uri, entry.dataset_path))
        .await
        .unwrap();
    let schema = Arc::new(ds.schema().into());
    let arrow_schema: &Schema = &schema;
    let columns: Vec<Arc<dyn arrow_array::Array>> = arrow_schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "id" | "__id" => {
                Arc::new(StringArray::from(vec![id.to_string()])) as Arc<dyn arrow_array::Array>
            }
            "name" => Arc::new(StringArray::from(vec![name.to_string()])),
            _ => arrow_array::new_null_array(field.data_type(), 1),
        })
        .collect();
    let row = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(row)], schema);
    ds.append(reader, None).await.unwrap();
    let new_version = ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &entry.dataset_path, None, new_version)
            .await
            .unwrap();
    DatasetUpdate {
        identity: entry.identity,
        type_key: entry.type_key.clone(),
        published_dataset_version: new_version,
        native_dataset_branch: None,
        entity_count: 1,
        version_metadata,
    }
}

/// Read the `graph_commit` lineage rows from `__manifest` at main and assert
/// they form a single LINEAR chain of `expected_total` commits (one genesis +
/// the rest), with no fork. Returns the head commit id.
///
/// "Linear, not a fork" is proven structurally: (1) exactly one parentless
/// genesis; (2) no two commits share a `parent_commit_id` (a fork would have two
/// children off one parent); (3) every commit except the unique head is the
/// parent of exactly one other commit — so the parent pointers form one path
/// that visits all commits. (1)+(2)+(3) over a connected set is a single chain.
async fn assert_linear_chain(uri: &str, expected_total: usize) -> String {
    let ds = open_manifest_dataset(uri, None).await.unwrap();
    let (rows, _heads) = read_graph_lineage(uri, &ds).await.unwrap();
    assert_eq!(
        rows.len(),
        expected_total,
        "expected {expected_total} graph commits (genesis + the concurrent commits), got {}",
        rows.len(),
    );
    assert_eq!(
        rows.iter().map(|row| row.generation).collect::<Vec<_>>(),
        (0..expected_total as u64).collect::<Vec<_>>(),
        "a linear chain counts its generations from the genesis commit's 0"
    );
    let settled = settled_commits(uri).await;
    let (head, ancestors) = rows.split_last().expect("a lineage holds its genesis");
    assert!(!settled.contains_key(&head.graph_commit_id));
    let held = read_manifest_rows(&ds).await.unwrap();
    assert_eq!(
        settled
            .values()
            .chain(held.buffer.commits())
            .collect::<HashSet<_>>(),
        ancestors.iter().collect::<HashSet<_>>(),
        "every commit behind the head is buffered or in `__history`, and neither holds a commit \
         that lost its CAS"
    );
    assert_eq!(
        ds.count_rows(None).await.unwrap(),
        held.tables.len() + 2 + held.buffer.commits().len() + held.buffer.replaced().len(),
        "a branch's `__manifest` holds one row per table, its head commit and its buffer"
    );

    // (1) exactly one genesis.
    let genesis: Vec<&GraphLineageRow> = rows
        .iter()
        .filter(|r| r.parent_commit_id.is_none())
        .collect();
    assert_eq!(
        genesis.len(),
        1,
        "exactly one parentless genesis commit in a linear chain, got {}",
        genesis.len(),
    );

    // (2) no two commits parent off the same commit (no fork).
    let mut parents: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.parent_commit_id.as_deref())
        .collect();
    let parent_count = parents.len();
    parents.sort_unstable();
    parents.dedup();
    assert_eq!(
        parents.len(),
        parent_count,
        "two commits share a parent_commit_id — the DAG forked instead of forming a linear chain",
    );

    // (3) the head (the `should_replace_head` winner) plus the parent set covers
    // every commit exactly once: each non-head commit is some commit's parent.
    let ids: std::collections::HashSet<&str> =
        rows.iter().map(|r| r.graph_commit_id.as_str()).collect();
    let parent_set: std::collections::HashSet<&str> = parents.iter().copied().collect();
    // The head is the only commit that is not a parent of anything.
    let non_parents: Vec<&str> = ids
        .iter()
        .copied()
        .filter(|id| !parent_set.contains(id))
        .collect();
    assert_eq!(
        non_parents,
        vec![head.graph_commit_id.as_str()],
        "the only commit that is no one's parent must be the head — a fork or break leaves others",
    );
    // Every parent points at a real commit (connectedness).
    for parent in &parent_set {
        assert!(
            ids.contains(parent),
            "parent {parent} must be a known commit in the chain",
        );
    }

    head.graph_commit_id.clone()
}

/// Test A (deterministic, the must-have): two writers, two DISJOINT table
/// updates, two distinct `LineageIntent`s, `tokio::join!`. BOTH commit (the loser
/// retries on the `graph_head:main` CAS conflict and re-parents off the winner),
/// and the on-disk graph_commit DAG is a single linear chain genesis → c → c'.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_disjoint_writes_share_head_and_form_linear_chain() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    // Two DISJOINT table-version rows (`node:Person@v=2`, `node:Company@v=2`):
    // distinct `object_id`s, so neither hits the table-version CAS. The ONLY
    // shared row both writers merge is `graph_head:main`.
    let update_a = append_node_row_and_make_update(uri, &person_entry, "p1", "Alice").await;
    let update_b = append_node_row_and_make_update(uri, &company_entry, "c1", "Acme").await;

    let publisher_a = GraphNamespacePublisher::new(uri, None);
    let publisher_b = GraphNamespacePublisher::new(uri, None);
    let changes_a = vec![ManifestChange::Update(update_a)];
    let changes_b = vec![ManifestChange::Update(update_b)];
    // Each writer mints its own stable commit id; the parent re-resolves per
    // attempt inside the publisher.
    let intent_a = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-a".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-b".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    // Empty expected-versions: the two writers are disjoint, so neither asserts a
    // version on the other's table; contention is purely the shared head row.
    let empty = HashMap::new();
    let (res_a, res_b) = tokio::join!(
        async {
            publisher_a
                .publish(&changes_a, &empty, Some(&intent_a))
                .await
        },
        async {
            publisher_b
                .publish(&changes_b, &empty, Some(&intent_b))
                .await
        }
    );

    // BOTH commit: disjoint tables → the head-row CAS loser retries within
    // PUBLISHER_RETRY_BUDGET, re-resolves its parent off the winner, and lands.
    let outcome_a = res_a.expect("writer A must commit");
    let outcome_b = res_b.expect("writer B must commit");
    assert_intent_nonce(&outcome_a.head.graph_commit_id, &intent_a);
    assert_intent_nonce(&outcome_b.head.graph_commit_id, &intent_b);

    // End-state assertion (the on-disk DAG is fixed once both committed): a single
    // linear chain genesis → first → second, no fork. The two minted ids both
    // appear; their parents form a chain (one off genesis, the other off the
    // first), so no two commits share a parent.
    let head = assert_linear_chain(uri, 3).await;
    assert!(
        head == outcome_a.head.graph_commit_id || head == outcome_b.head.graph_commit_id,
        "the head must be one of the two concurrent commits",
    );
    // Both committed table writes are visible (Person and Company advanced).
    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    let after = reopened.snapshot();
    assert_eq!(
        after
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        2
    );
    assert_eq!(
        after
            .dataset("node:Company")
            .unwrap()
            .published_dataset_version,
        2
    );
}

/// Test C (S3 variant, bucket-gated): the same two-disjoint-writers +
/// `LineageIntent` race as Test A, but on a real object store so the one-winner
/// behaviour exercises the genuine conditional-put CAS on `__manifest` rather
/// than the local content-token emulation. Skips with a log when
/// `OMNIGRAPH_S3_TEST_BUCKET` is unset (the `tests/s3_storage.rs` gate); the
/// rustfs CI job sets it. Asserts the same end-state: both commit, single linear
/// chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_disjoint_writes_form_linear_chain_on_s3() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!(
            "SKIP concurrent_disjoint_writes_form_linear_chain_on_s3: \
             OMNIGRAPH_S3_TEST_BUCKET unset — the S3 lineage-CAS gate needs an object store"
        );
        return;
    };
    let uri = format!(
        "s3://{bucket}/lineage-concurrency/{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    );

    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(&uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    let update_a = append_node_row_and_make_update(&uri, &person_entry, "p1", "Alice").await;
    let update_b = append_node_row_and_make_update(&uri, &company_entry, "c1", "Acme").await;

    let publisher_a = GraphNamespacePublisher::new(&uri, None);
    let publisher_b = GraphNamespacePublisher::new(&uri, None);
    let changes_a = vec![ManifestChange::Update(update_a)];
    let changes_b = vec![ManifestChange::Update(update_b)];
    let intent_a = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-a".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-b".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let empty = HashMap::new();
    let (res_a, res_b) = tokio::join!(
        async {
            publisher_a
                .publish(&changes_a, &empty, Some(&intent_a))
                .await
        },
        async {
            publisher_b
                .publish(&changes_b, &empty, Some(&intent_b))
                .await
        }
    );
    let outcome_a = res_a.expect("writer A must commit on S3");
    let outcome_b = res_b.expect("writer B must commit on S3");

    let head = assert_linear_chain(&uri, 3).await;
    assert!(
        head == outcome_a.head.graph_commit_id || head == outcome_b.head.graph_commit_id,
        "the head must be one of the two concurrent commits",
    );
}

/// Test B (bounded-retry convergence, scaled): N=8 same-branch writers, each
/// touching a DISJOINT table-version row + its own `LineageIntent`, each wrapped
/// in an APP-LEVEL retry loop. `PUBLISHER_RETRY_BUDGET=5` means the later writers
/// can exhaust the internal budget under contention, so the app loop re-submits
/// on a typed `Conflict` / row-level-CAS-contention error. All 8 eventually
/// commit and the final DAG is a single linear chain of 8 (+genesis), no fork,
/// no lost commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn n_concurrent_disjoint_writers_converge_to_one_linear_chain() {
    use crate::error::ManifestConflictDetails;
    use crate::error::ManifestErrorKind;

    const N: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.dataset("node:Person").unwrap().clone();
    let company_entry = snap.dataset("node:Company").unwrap().clone();

    // Synthesize N=8 DISJOINT table-version updates by sequentially advancing the
    // two node tables four versions each (Person@v2..v5, Company@v2..v5). Each
    // update is a distinct `object_id`, so the writers never collide on a
    // table-version row — only on the shared `graph_head:main`. Built serially
    // here (before the concurrent phase) so the on-disk versions exist.
    let mut updates: Vec<DatasetUpdate> = Vec::with_capacity(N);
    for i in 0..(N / 2) {
        updates.push(
            append_node_row_and_make_update(uri, &person_entry, &format!("p{i}"), &format!("P{i}"))
                .await,
        );
        updates.push(
            append_node_row_and_make_update(
                uri,
                &company_entry,
                &format!("c{i}"),
                &format!("C{i}"),
            )
            .await,
        );
    }
    assert_eq!(updates.len(), N);

    // Each writer: its own publisher + its own commit id + an app-level retry loop
    // re-submitting on a typed Conflict (the publisher's internal budget can be
    // exhausted by the later contenders, so convergence relies on the app retry).
    let uri_owned = uri.to_string();
    let mut handles = Vec::with_capacity(N);
    for update in updates {
        let uri = uri_owned.clone();
        handles.push(tokio::spawn(async move {
            let commit_id = ulid::Ulid::new().to_string();
            let changes = vec![ManifestChange::Update(update)];
            let empty = HashMap::new();
            // Bounded app-level retry: re-submit on a Conflict-kind manifest error
            // (the only retryable outcome here is losing the shared-head CAS).
            for _attempt in 0..64 {
                let intent = LineageIntent {
                    graph_commit_id: commit_id.clone(),
                    branch: None,
                    actor_id: None,
                    merged_parent: None,
                    created_at: lineage_now_micros(),
                    history_release_bytes: HistoryReleaseBytes::PRODUCTION,
                };
                let publisher = GraphNamespacePublisher::new(&uri, None);
                match publisher.publish(&changes, &empty, Some(&intent)).await {
                    Ok(outcome) => {
                        let physical = omnigraph_core::graph_commit_id::parse_history_block_id(
                            &outcome.head.graph_commit_id,
                        )
                        .unwrap()
                        .unwrap();
                        assert_eq!(physical.nonce.to_string(), commit_id);
                        return outcome.head.graph_commit_id;
                    }
                    Err(OmniError::Manifest(m))
                        if matches!(m.kind, ManifestErrorKind::Conflict)
                            && matches!(
                                m.details,
                                Some(ManifestConflictDetails::RowLevelCasContention)
                            ) =>
                    {
                        // lost the shared-head CAS after exhausting the internal
                        // budget — re-resolve parent + re-submit.
                        continue;
                    }
                    Err(other) => panic!("non-retryable publish error: {other:?}"),
                }
            }
            panic!("writer for commit {commit_id} did not converge within the app-retry budget");
        }));
    }

    let mut committed_ids = Vec::with_capacity(N);
    for handle in handles {
        committed_ids.push(handle.await.unwrap());
    }
    // All 8 distinct writer ids committed (no lost commit, no duplicate id).
    committed_ids.sort();
    committed_ids.dedup();
    assert_eq!(
        committed_ids.len(),
        N,
        "every writer must commit exactly once"
    );

    // The final DAG is a single linear chain of genesis + 8 = 9, no fork.
    assert_linear_chain(uri, N + 1).await;
}

/// The `present` bits marking exactly `null_fields` null, by their bit in `RECORD_FIELDS`.
fn null_bits(null_fields: &[&str]) -> u32 {
    null_fields
        .iter()
        .map(|field| {
            let bit = super::record::RECORD_FIELDS
                .iter()
                .position(|(name, _)| name == field)
                .unwrap();
            1u32 << bit
        })
        .fold(0, |mask, bit| mask | bit)
}

fn probe_commit(id: &str, parent: Option<&str>) -> GraphLineageRow {
    GraphLineageRow {
        graph_commit_id: id.to_string(),
        schema_contract: None,
        schema_content_hash: None,
        graph_branch: None,
        native_branch: None,
        graph_manifest_version: 4,
        generation: 2,
        parent_commit_id: parent.map(str::to_string),
        merged_parent_commit_id: None,
        actor_id: Some(String::new()),
        created_at: -1,
    }
}

fn probe_table(stable_table_id: u64, state: TableState) -> TableRow {
    let identity = TableIdentity::new(stable_table_id, 3).unwrap();
    let table_key = format!("node:Probe{stable_table_id}");
    TableRow {
        registration: TableRegistration {
            identity,
            table_path: table_path_for_identity(&table_key, identity).unwrap(),
            table_key,
        },
        state,
    }
}

fn probe_pin(table_branch: Option<&str>) -> TableState {
    TableState::Pinned(TablePin {
        table_version: 9,
        table_branch: table_branch.map(str::to_string),
        row_count: 0,
        metadata: TableVersionMetadata::from_json_str(
            r#"{"manifest_path":"p","manifest_size":null,"e_tag":null,"naming_scheme":null}"#,
        )
        .unwrap(),
        manifest_version: 4,
    })
}

/// A logical batch with every record field in each of its states: a value,
/// null, and its filler as a value (the empty string, zero), so the packed
/// shape must keep the filler and null apart through the `present` bits.
fn record_states_batch() -> RecordBatch {
    let rows = ManifestRows {
        schema_contract_head: None,
        schema_contract: None,
        tables: vec![probe_table(
            7,
            TableState::Dropped {
                dropped_at: 5,
                sealed_version: 9,
            },
        )],
        head: probe_commit("01ARZ3NDEKTSV4RRFFQ69G5FAV", Some("p")),
        buffer: CommitBuffer {
            commits: Vec::new(),
            replaced: vec![ReplacedTable {
                replaced_at: 4,
                before: ReplacedRow::Unregistered(
                    probe_table(7, TableState::Registered).registration,
                ),
            }],
        },
    };
    let schema = super::state::manifest_schema();
    let mut fillers: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["x"])),
        Arc::new(StringArray::from(vec!["probe"])),
    ];
    fillers.extend(schema.fields().iter().skip(2).map(|field| {
        let filler: arrow_array::ArrayRef = match field.data_type() {
            DataType::Utf8 => Arc::new(StringArray::from(vec![""])),
            DataType::LargeUtf8 => Arc::new(arrow_array::LargeStringArray::from(vec![""])),
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![0])),
            DataType::Int64 => Arc::new(arrow_array::Int64Array::from(vec![0])),
            other => panic!("record field {} has type {other}", field.name()),
        };
        filler
    }));
    let fillers = RecordBatch::try_new(schema.clone(), fillers).unwrap();
    datafusion::arrow::compute::concat_batches(&schema, [&rows.to_batch().unwrap(), &fillers])
        .unwrap()
}

#[test]
fn packed_record_round_trips_nulls_through_present_bits() {
    let logical = record_states_batch();
    let schema = super::record::manifest_storage_schema(HashMap::new());
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "object_id",
            "object_type",
            "record",
            "schema_source",
            "schema_ir"
        ]
    );
    let stored = super::record::compact_to_storage(&logical, &schema).unwrap();
    let record = stored
        .column_by_name("record")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(
        record.column_names(),
        [
            "location",
            "metadata",
            "table_key",
            "stable_table_id",
            "table_incarnation_id",
            "table_version",
            "table_branch",
            "row_count",
            "manifest_version",
            "dropped_at",
            "graph_branch",
            "native_branch",
            "graph_manifest_version",
            "generation",
            "parent_commit_id",
            "merged_parent_commit_id",
            "actor_id",
            "created_at",
            "schema_ir_hash",
            "schema_identity_version",
            "schema_identity_domain",
            "schema_content_hash",
            "replaced_at",
            "registered_at",
            "present",
        ]
    );
    assert!(record.columns().iter().all(|child| child.null_count() == 0));
    let present = record
        .column_by_name("present")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    assert_eq!(
        present.values().as_ref(),
        &[
            null_bits(&[
                "schema_ir_hash",
                "schema_identity_version",
                "schema_identity_domain",
                "schema_content_hash",
                "metadata",
                "table_branch",
                "row_count",
                "manifest_version",
                "graph_branch",
                "native_branch",
                "graph_manifest_version",
                "generation",
                "parent_commit_id",
                "merged_parent_commit_id",
                "actor_id",
                "created_at",
                "replaced_at",
                "registered_at",
            ]),
            null_bits(&[
                "schema_ir_hash",
                "schema_identity_version",
                "schema_identity_domain",
                "schema_content_hash",
                "location",
                "metadata",
                "table_key",
                "stable_table_id",
                "table_incarnation_id",
                "table_version",
                "table_branch",
                "row_count",
                "manifest_version",
                "dropped_at",
                "graph_branch",
                "native_branch",
                "merged_parent_commit_id",
                "replaced_at",
                "registered_at",
            ]),
            null_bits(&[
                "schema_ir_hash",
                "schema_identity_version",
                "schema_identity_domain",
                "schema_content_hash",
                "location",
                "metadata",
                "table_key",
                "table_version",
                "table_branch",
                "row_count",
                "manifest_version",
                "dropped_at",
                "graph_branch",
                "native_branch",
                "graph_manifest_version",
                "generation",
                "parent_commit_id",
                "merged_parent_commit_id",
                "actor_id",
                "created_at",
            ]),
            null_bits(&[]),
        ]
    );

    let expanded = super::record::expand_from_storage(&stored).unwrap();
    assert_eq!(expanded, logical);
}

/// `stored` with its `present` column replaced by `present`, the rows a
/// writer that set a null bit over a value would leave behind.
fn with_present_bits(stored: &RecordBatch, present: arrow_array::ArrayRef) -> RecordBatch {
    let record = stored
        .column_by_name("record")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let mut children = record.columns().to_vec();
    let present_index = record
        .column_names()
        .iter()
        .position(|name| *name == super::record::PRESENT_COLUMN)
        .unwrap();
    children[present_index] = present;
    let tampered = arrow_array::StructArray::new(record.fields().clone(), children, None);
    let mut columns = stored.columns().to_vec();
    columns[stored.schema().index_of("record").unwrap()] = Arc::new(tampered);
    RecordBatch::try_new(stored.schema(), columns).unwrap()
}

#[test]
fn packed_record_refuses_a_null_bit_beside_a_value() {
    let logical = record_states_batch();
    let schema = super::record::manifest_storage_schema(HashMap::new());
    let stored = super::record::compact_to_storage(&logical, &schema).unwrap();

    for (field, row) in [
        ("location", 0),
        ("stable_table_id", 0),
        ("parent_commit_id", 1),
        ("generation", 1),
        ("created_at", 1),
        ("replaced_at", 2),
        ("registered_at", 2),
    ] {
        let mut present = vec![0u32; 4];
        present[row] = null_bits(&[field]);
        let tampered =
            with_present_bits(&stored, Arc::new(arrow_array::UInt32Array::from(present)));
        let error = super::record::expand_from_storage(&tampered).unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "'{field}' is marked null but carries a value at row {row}"
            )),
            "{error}"
        );
    }
}

/// Open, read and publish serve the stamp this binary writes and refuse the
/// stamps before it on the stamp alone, naming no in-place route.
#[tokio::test]
async fn stamps_11_and_12_are_refused_by_open_read_and_publish() {
    for stamp in [11u32, 12] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        ManifestCoordinator::init(uri, &build_test_catalog())
            .await
            .unwrap();
        let mut dataset = open_manifest_dataset(uri, None).await.unwrap();
        let rows_at_birth = read_manifest_rows(&dataset).await.unwrap();
        super::migrations::set_stamp_for_test(&mut dataset, stamp)
            .await
            .unwrap();
        let dataset = open_manifest_dataset(uri, None).await.unwrap();
        assert_eq!(super::migrations::read_stamp(&dataset), Some(stamp));

        let refused = |what: &str, error: OmniError| {
            let message = error.to_string();
            assert!(
                message.contains(&format!("internal schema v{stamp}"))
                    && message.contains("reads only v14 to v14")
                    && !message.contains("omnigraph upgrade"),
                "{what} of a stamp {stamp} manifest: {message}"
            );
        };
        refused(
            "open",
            ManifestCoordinator::open(uri)
                .await
                .err()
                .expect("open must refuse"),
        );
        refused(
            "stamp read",
            read_supported_internal_schema_version(uri)
                .await
                .unwrap_err(),
        );
        refused(
            "state read",
            read_manifest_state(&dataset).await.unwrap_err(),
        );
        refused(
            "lineage read",
            read_graph_lineage(uri, &dataset).await.unwrap_err(),
        );
        refused(
            "point-in-time read",
            ManifestCoordinator::snapshot_at(uri, None, dataset.version().version)
                .await
                .unwrap_err(),
        );
        refused(
            "publish",
            GraphNamespacePublisher::new(uri, None)
                .publish(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
                .await
                .unwrap_err(),
        );
        refused(
            "overwrite",
            super::commit::overwrite(dataset.clone(), &rows_at_birth)
                .await
                .unwrap_err(),
        );
        assert_eq!(
            dataset.latest_version_id().await.unwrap(),
            dataset.version().version,
            "a refused publish must not move `__manifest`"
        );
        assert_eq!(history_row_count(uri).await, 0);
    }
}

#[tokio::test]
async fn publish_keeps_one_row_per_table_and_buffers_the_head_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let ManifestRows {
        tables: mut tables_before,
        head: mut previous,
        buffer,
        ..
    } = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(buffer, CommitBuffer::default());
    assert_eq!(
        (previous.generation, previous.parent_commit_id.as_deref()),
        (0, None)
    );
    let mut buffered = Vec::new();

    for round in 1..=4u64 {
        let person = mc.snapshot().dataset("node:Person").unwrap().clone();
        let update = append_person_and_make_update(uri, &person, &format!("p{round}")).await;
        let intent = lineage_intent(None, None);
        let outcome = mc
            .commit_changes_with_lineage(
                &[ManifestChange::Update(update.clone())],
                &HashMap::new(),
                Some(&intent),
            )
            .await
            .unwrap();

        let dataset = open_manifest_dataset(uri, None).await.unwrap();
        let rows = read_manifest_rows(&dataset).await.unwrap();
        assert_eq!(rows.tables.len(), tables_before.len());
        assert_eq!(
            dataset.count_rows(None).await.unwrap(),
            rows.tables.len() + 2 + 2 * round as usize,
            "one row per table, the head, and per buffered commit its record and the row of \
             `node:Person` the publish after it replaced"
        );
        assert_eq!(outcome.commit.as_ref(), Some(&rows.head));
        assert_eq!(
            rows.head,
            GraphLineageRow {
                graph_commit_id: intent.graph_commit_id.clone(),
                schema_contract: previous.schema_contract.clone(),
                schema_content_hash: previous.schema_content_hash.clone(),
                graph_branch: None,
                native_branch: None,
                graph_manifest_version: dataset.version().version,
                generation: round,
                parent_commit_id: Some(previous.graph_commit_id.clone()),
                merged_parent_commit_id: None,
                actor_id: None,
                created_at: intent.created_at,
            }
        );
        let person_row = rows
            .tables
            .iter()
            .find(|table| table.registration.identity == person.identity)
            .unwrap();
        assert!(
            matches!(
                &person_row.state,
                TableState::Pinned(pin)
                    if pin.table_version == update.published_dataset_version
                        && pin.manifest_version == dataset.version().version
            ),
            "{person_row:?}"
        );

        buffered.push(HistoryRecord {
            commit: previous,
            tables: tables_before,
        });
        assert_eq!(
            rows.buffer.records(&rows.tables),
            buffered,
            "the buffer holds every head a publish replaced, oldest first, each with the \
             `table` rows of the version that wrote it"
        );
        assert_eq!(mc.branch_records(), rows.records());
        assert_eq!(
            mc.held_record(&buffered[0].commit.graph_commit_id).as_ref(),
            Some(&buffered[0])
        );
        assert!(
            matches!(
                Dataset::open(&history_uri(uri)).await,
                Err(lance::Error::DatasetNotFound { .. })
            ),
            "a publish into a buffer with room appends nothing, so `__history` is not born"
        );
        tables_before = rows.tables;
        previous = rows.head;
    }
}

/// In every `__manifest` version the `table` rows are the state as of the
/// `graph_commit` row, except after a publish with no lineage intent, which
/// only tests issue: the head stays, the rows move, `__history` is not written.
#[tokio::test]
async fn publish_without_lineage_moves_the_table_rows_under_the_head_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let before = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    let person = mc.snapshot().dataset("node:Person").unwrap().clone();
    let update = append_person_and_make_update(uri, &person, "Alice").await;

    let outcome = mc
        .commit_changes_with_lineage(&[ManifestChange::Update(update)], &HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(outcome.commit, None);
    assert_eq!(outcome.parent_commit_id, None);

    let dataset = open_manifest_dataset(uri, None).await.unwrap();
    let after = read_manifest_rows(&dataset).await.unwrap();
    assert_eq!(after.head, before.head);
    assert_ne!(after.tables, before.tables);
    assert!(after.head.graph_manifest_version < dataset.version().version);
    assert_eq!(mc.exact_graph_head(), Some(before.head.graph_commit_id));
    assert_eq!(history_row_count(uri).await, 0);
}

#[tokio::test]
async fn fresh_fork_publishes_on_top_of_the_head_it_inherits() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let genesis = mc.exact_graph_head().unwrap();
    let on_main = lineage_intent(None, None);
    mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&on_main))
        .await
        .unwrap();
    mc.create_branch("feature").await.unwrap();

    let fork = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    assert_eq!(fork.exact_graph_head(), None);
    assert!(fork.snapshot().graph_heads.is_empty());
    let fork_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let native = fork_manifest.manifest().branch.clone();
    assert!(native.is_some());
    let inherited = read_manifest_rows(&fork_manifest).await.unwrap().head;
    assert_eq!(inherited.graph_commit_id, on_main.graph_commit_id);
    assert_eq!(inherited.native_branch, None);
    let (lineage, heads) = read_graph_lineage(uri, &fork_manifest).await.unwrap();
    assert!(heads.is_empty());
    assert_eq!(
        lineage
            .iter()
            .map(|commit| commit.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        [genesis.as_str(), on_main.graph_commit_id.as_str()]
    );
    let main_rows = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        read_manifest_rows(&fork_manifest).await.unwrap().records(),
        main_rows.records(),
        "a fork inherits the head, the buffer and the `replaced_table` rows of the version it \
         forks"
    );
    assert_eq!(
        history_row_count(uri).await,
        0,
        "creating a branch appends nothing"
    );

    let on_fork = lineage_intent(Some("feature"), None);
    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        Some("feature"),
        fork_manifest.branch_identifier().await.unwrap(),
        None,
    ));
    let outcome = GraphNamespacePublisher::new(uri, Some("feature"))
        .publish_with_precondition(&[], &HashMap::new(), Some(&on_fork), &precondition)
        .await
        .unwrap();
    assert_eq!(
        outcome.parent_commit_id.as_deref(),
        Some(on_main.graph_commit_id.as_str())
    );
    assert_eq!(outcome.head.native_branch, native);
    assert_eq!(outcome.head.graph_branch.as_deref(), Some("feature"));
    assert_eq!(outcome.head.generation, 2);
    assert_eq!(
        outcome.known_state.graph_heads,
        HashMap::from([("feature".to_string(), on_fork.graph_commit_id.clone())])
    );
    assert_eq!(
        outcome
            .buffer
            .commits()
            .iter()
            .map(|commit| commit.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        [genesis.as_str(), on_main.graph_commit_id.as_str()],
        "the fork's first publish buffers the head it inherited behind the buffer it inherited"
    );
    assert_eq!(
        outcome.buffer.get(&inherited.graph_commit_id),
        Some(&inherited)
    );
    assert_eq!(history_row_count(uri).await, 0);

    let fork = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    assert_eq!(fork.exact_graph_head(), Some(on_fork.graph_commit_id));
    let main = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(main.exact_graph_head(), Some(on_main.graph_commit_id));
}

/// A branch created again under the name of a deleted one holds the head of its
/// source: the head the deleted incarnation wrote is an ancestor, never the
/// exact head of the new incarnation.
#[tokio::test]
async fn recreated_branch_has_no_exact_head_and_inherits_the_head_of_its_source() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let genesis = mc.exact_graph_head().unwrap();
    let publish = |branch: &'static str| async move {
        let intent = lineage_intent(Some(branch), None);
        let outcome = GraphNamespacePublisher::new(uri, Some(branch))
            .publish(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        outcome.head
    };

    mc.create_branch("b").await.unwrap();
    let on_b = publish("b").await;
    ManifestCoordinator::open_at_branch(uri, "b")
        .await
        .unwrap()
        .create_branch("c")
        .await
        .unwrap();
    let on_c = publish("c").await;
    assert_eq!(
        on_c.parent_commit_id.as_deref(),
        Some(on_b.graph_commit_id.as_str())
    );
    mc.delete_branch("b").await.unwrap();
    ManifestCoordinator::open_at_branch(uri, "c")
        .await
        .unwrap()
        .create_branch("b")
        .await
        .unwrap();

    let recreated = ManifestCoordinator::open_at_branch(uri, "b").await.unwrap();
    assert_eq!(recreated.exact_graph_head(), None);
    let manifest = open_manifest_dataset(uri, Some("b")).await.unwrap();
    assert_ne!(manifest.manifest().branch, on_b.native_branch);
    let (lineage, heads) = read_graph_lineage(uri, &manifest).await.unwrap();
    assert!(heads.is_empty());
    assert_eq!(
        lineage
            .iter()
            .map(|commit| commit.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        [
            genesis.as_str(),
            on_b.graph_commit_id.as_str(),
            on_c.graph_commit_id.as_str()
        ]
    );

    let on_recreated = publish("b").await;
    assert_eq!(
        on_recreated.parent_commit_id.as_deref(),
        Some(on_c.graph_commit_id.as_str())
    );
    assert_eq!(on_recreated.native_branch, manifest.manifest().branch);
    assert_eq!(on_recreated.generation, 3);
}

#[tokio::test]
async fn generation_is_the_greatest_parent_generation_plus_one() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    mc.create_branch("feature").await.unwrap();
    let publish = |branch: Option<&'static str>, merged_parent: Option<BranchRecords>| async move {
        GraphNamespacePublisher::new(uri, branch)
            .publish(
                &[],
                &HashMap::new(),
                Some(&lineage_intent(branch, merged_parent)),
            )
            .await
            .unwrap()
            .head
    };
    let merged = |commit: &GraphLineageRow| {
        Some(BranchRecords::from(HistoryRecord {
            commit: commit.clone(),
            tables: Vec::new(),
        }))
    };

    let mut on_feature = Vec::new();
    for _ in 0..3 {
        on_feature.push(publish(Some("feature"), None).await);
    }
    assert_eq!(
        on_feature
            .iter()
            .map(|commit| commit.generation)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let on_main = publish(None, None).await;
    assert_eq!(on_main.generation, 1);

    let merge = publish(None, merged(&on_feature[2])).await;
    assert_eq!(merge.generation, 4);
    assert_eq!(
        merge.parent_commit_id.as_deref(),
        Some(on_main.graph_commit_id.as_str())
    );
    assert_eq!(
        merge.merged_parent_commit_id.as_deref(),
        Some(on_feature[2].graph_commit_id.as_str())
    );

    let merge_of_an_older_commit = publish(None, merged(&on_feature[0])).await;
    assert_eq!(merge_of_an_older_commit.generation, 5);
}

/// The ids of the commits of `reader`'s branch, oldest first, and how many
/// times reading them read `__history`.
async fn lineage_and_history_reads(reader: &ManifestCoordinator) -> (Vec<String>, u64) {
    let probes = crate::instrumentation::QueryIoProbes::default();
    let history_reads = Arc::clone(&probes.projection_full_refreshes);
    let lineage = crate::instrumentation::with_query_io_probes(probes, async {
        reader.commit_graph().lineage().await.unwrap()
    })
    .await;
    let ids = lineage
        .first_parent_chain()
        .unwrap()
        .into_iter()
        .map(|commit| commit.graph_commit_id)
        .collect();
    (
        ids,
        history_reads.load(std::sync::atomic::Ordering::Relaxed),
    )
}

#[tokio::test]
async fn lineage_reads_history_only_for_a_parent_neither_the_buffer_nor_the_cache_holds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut writer = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut follower = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    let mut chain = vec![writer.head().graph_commit_id.clone()];
    assert_eq!(
        lineage_and_history_reads(&follower).await,
        (chain.clone(), 0),
        "the genesis commit names no parent"
    );

    while !writer
        .buffer()
        .is_full(writer.head(), &writer.head_record().tables)
        .unwrap()
    {
        let intent = fat_intent(None, None);
        writer
            .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        chain.push(intent.graph_commit_id);
        assert_eq!(
            lineage_and_history_reads(&writer).await,
            (chain.clone(), 0),
            "the buffer holds every ancestor of the head"
        );
    }
    follower.refresh().await.unwrap();
    assert_eq!(
        lineage_and_history_reads(&follower).await,
        (chain.clone(), 0)
    );
    assert_eq!(history_row_count(uri).await, 0);

    let releasing = lineage_intent(None, None);
    writer
        .commit_changes_with_lineage(&[], &HashMap::new(), Some(&releasing))
        .await
        .unwrap();
    chain.push(releasing.graph_commit_id);
    assert_eq!(writer.buffer().commits().len(), 1);
    assert_eq!(
        lineage_and_history_reads(&writer).await,
        (chain.clone(), 0),
        "the commits a publish releases enter the cache of the publishing coordinator"
    );
    follower.refresh().await.unwrap();
    assert_eq!(
        lineage_and_history_reads(&follower).await,
        (chain.clone(), 0),
        "the commits a refresh sees leave the buffer enter the cache"
    );

    let late = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    assert_eq!(
        lineage_and_history_reads(&late).await,
        (chain.clone(), 1),
        "a cache that has read nothing reads `__history` for the parent of the oldest \
         buffered commit"
    );
    assert_eq!(lineage_and_history_reads(&late).await, (chain.clone(), 0));

    for _ in 0..2 {
        chain.extend(fill_buffer(&mut writer, None).await);
        chain.push(publish_on(&mut writer, None, None).await.graph_commit_id);
    }
    follower.refresh().await.unwrap();
    assert_eq!(
        lineage_and_history_reads(&follower).await,
        (chain.clone(), 1),
        "a refresh across more than one release misses the commits between them, and the \
         next lineage reads the whole of `__history` for them"
    );
    assert_eq!(lineage_and_history_reads(&follower).await, (chain, 0));
}

/// A root born at the current stamp has no legacy directory, and a follower
/// whose cache misses a released parent refreshes without asking for one.
#[tokio::test]
async fn stale_follower_lineage_on_a_root_born_current_asks_for_no_legacy_directory() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut writer = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut follower = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    let mut chain = vec![writer.head().graph_commit_id.clone()];
    assert_eq!(
        lineage_and_history_reads(&follower).await,
        (chain.clone(), 0)
    );
    for _ in 0..2 {
        chain.extend(fill_buffer(&mut writer, None).await);
        chain.push(publish_on(&mut writer, None, None).await.graph_commit_id);
    }
    follower.refresh().await.unwrap();

    let sent = HistoryGets::default();
    let read = || async {
        let probes = sent.probes();
        let lineage = crate::instrumentation::with_query_io_probes(probes.clone(), async {
            follower.commit_graph().lineage().await.unwrap()
        })
        .await;
        let ids: Vec<String> = lineage
            .first_parent_chain()
            .unwrap()
            .into_iter()
            .map(|commit| commit.graph_commit_id)
            .collect();
        assert_eq!(ids, chain);
        let refreshes = probes
            .projection_full_refreshes
            .load(std::sync::atomic::Ordering::Relaxed);
        let (gets, absent, heads) = sent.drain();
        (refreshes, gets > 0, absent, heads)
    };
    assert_eq!(
        read().await,
        (1, true, 0, 0),
        "the refresh reads `__history` and no GET is answered not found"
    );
    assert_eq!(read().await, (0, false, 0, 0), "the cache then answers");
}

/// A merge publish appends the records the merged branch wrote, so the second
/// parent of the merge commit is in `__history` while the merged branch
/// publishes nothing more, and its ancestors are there or in the target's buffer.
#[tokio::test]
async fn merge_publish_appends_the_records_of_an_idle_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    main.create_branch("feature").await.unwrap();
    let mut feature = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    feature
        .commit_changes_with_lineage(
            &[],
            &HashMap::new(),
            Some(&lineage_intent(Some("feature"), None)),
        )
        .await
        .unwrap();
    let on_main = lineage_intent(None, None);
    main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&on_main))
        .await
        .unwrap();

    let feature_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let source_records = feature.branch_records();
    assert_eq!(
        source_records,
        read_manifest_rows(&feature_manifest)
            .await
            .unwrap()
            .records(),
        "a coordinator holds the rows of the version its publish wrote"
    );
    let merged = source_records.head.clone();
    assert!(!merged.tables.is_empty());
    assert_eq!(source_records.buffer.commits().len(), 1);
    assert_eq!(history_row_count(uri).await, 0);

    let merge = lineage_intent(None, Some(source_records.clone()));
    main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&merge))
        .await
        .unwrap();
    assert_eq!(
        main.head().merged_parent_commit_id.as_deref(),
        Some(merged.commit.graph_commit_id.as_str())
    );
    assert_eq!(
        history_records(uri).await,
        std::slice::from_ref(&merged),
        "the target buffers the genesis commit the source inherited"
    );
    assert_eq!(
        super::history::read_record(uri, &session, &merged.commit.graph_commit_id)
            .await
            .unwrap(),
        Some(merged.clone())
    );
    assert_eq!(
        main.buffer()
            .commits()
            .iter()
            .map(|commit| commit.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        [
            source_records.buffer.commits()[0].graph_commit_id.as_str(),
            on_main.graph_commit_id.as_str()
        ],
        "a merge into a buffer with room releases nothing of the target"
    );

    let late = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    let target = late.commit_graph().lineage().await.unwrap();
    let source = feature.commit_graph().lineage().await.unwrap();
    let search = super::commit_graph::MergeBaseResolver::new(
        &source,
        &target,
        &merged.commit.graph_commit_id,
        &merge.graph_commit_id,
    )
    .search();
    assert!(!search.needs_import());
    assert_eq!(
        search.base.map(|base| base.graph_commit_id),
        Some(merged.commit.graph_commit_id.clone()),
        "the merged head is the base of the idle branch and the branch it was merged into"
    );

    main.delete_branch("feature").await.unwrap();
    assert_eq!(
        ManifestCoordinator::open_with_session(uri, &session)
            .await
            .unwrap()
            .commit_graph()
            .lineage()
            .await
            .unwrap()
            .get_commit(&merged.commit.graph_commit_id)
            .map(|commit| commit.graph_commit_id.clone()),
        Some(merged.commit.graph_commit_id.clone()),
        "a reader of the target needs the target's `__manifest` and `__history` only"
    );
}

/// A catalog of [`WIDE_TABLES`] node tables whose names carry a 512-byte suffix.
fn build_wide_catalog() -> Catalog {
    let source: String = (0..WIDE_TABLES)
        .map(|table| format!("node N{table}{} {{ x: I64 }}\n", "x".repeat(512)))
        .collect();
    let shape = compile_schema_shape(&parse_schema(&source).unwrap()).unwrap();
    let domain = SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let schema_ir = initialize_schema_ir(domain, &shape).unwrap().schema_ir;
    build_catalog_from_ir(&schema_ir).unwrap()
}

/// Capturing a branch of cheap commits over a wide unchanged catalog for a
/// merge rebuilds no `table` rows; the merge rebuilds each appended record once.
#[tokio::test]
async fn branch_records_of_a_wide_unchanged_catalog_rebuild_no_table_rows() {
    use super::state::TABLE_ROWS_REBUILT;
    const COMMITS: usize = 64;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = ManifestCoordinator::init(uri, &build_wide_catalog())
        .await
        .unwrap();
    main.create_branch("feature").await.unwrap();
    let mut feature = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    for _ in 0..COMMITS {
        feature
            .commit_changes_with_lineage(
                &[],
                &HashMap::new(),
                Some(&lineage_intent(Some("feature"), None)),
            )
            .await
            .unwrap();
    }
    assert_eq!(feature.buffer().commits().len(), COMMITS);
    assert_eq!(feature.head_record().tables.len(), WIDE_TABLES);

    TABLE_ROWS_REBUILT.set(0);
    let records = feature.branch_records();
    assert!(
        TABLE_ROWS_REBUILT.get() <= WIDE_TABLES,
        "the capture of {COMMITS} buffered commits over {WIDE_TABLES} unchanged tables rebuilt \
         {} table rows; it holds one catalog",
        TABLE_ROWS_REBUILT.get()
    );

    let appended: Vec<HistoryRecord> = records.oldest_first().skip(1).collect();
    assert_eq!(appended.len(), COMMITS);
    TABLE_ROWS_REBUILT.set(0);
    let merge = lineage_intent(None, Some(records));
    main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&merge))
        .await
        .unwrap();
    assert_eq!(
        TABLE_ROWS_REBUILT.get(),
        COMMITS * WIDE_TABLES,
        "the merge builds each record it appends once, when its extent is encoded"
    );
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(appended),
        "main keeps the genesis commit the branch inherited"
    );
}

fn sorted_history_records(mut records: Vec<HistoryRecord>) -> Vec<HistoryRecord> {
    records.sort_by(|a, b| {
        (a.commit.generation, &a.commit.graph_commit_id)
            .cmp(&(b.commit.generation, &b.commit.graph_commit_id))
    });
    records
}

/// Complete records, ordered by generation and ID only for deterministic assertions.
async fn history_records(uri: &str) -> Vec<HistoryRecord> {
    sorted_history_records(
        super::history::read_records(uri, &crate::lance_access::control_session())
            .await
            .unwrap()
            .into_values()
            .collect(),
    )
}

async fn history_extent_count(uri: &str) -> usize {
    super::history::stored_batches(uri).await.unwrap().len()
}

/// Deleting a branch appends the commits it wrote, buffered ones and head,
/// before its ref is retired. A branch that has published nothing appends
/// nothing, and a second copy of a record reads as the first.
#[tokio::test]
async fn branch_delete_appends_the_commits_the_branch_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();

    main.create_branch("unwritten").await.unwrap();
    main.delete_branch("unwritten").await.unwrap();
    assert_eq!(history_row_count(uri).await, 0);

    main.create_branch("feature").await.unwrap();
    let mut feature = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    for _ in 0..2 {
        let person = feature.snapshot().dataset("node:Person").unwrap().clone();
        let update = append_person_and_make_update(uri, &person, "Alice").await;
        feature
            .commit_changes_with_lineage(
                &[ManifestChange::Update(update)],
                &HashMap::new(),
                Some(&lineage_intent(Some("feature"), None)),
            )
            .await
            .unwrap();
    }
    let feature_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let records = feature.branch_records();
    let head = records.head.clone();
    let buffered = records.buffer.records(&head.tables);
    assert_eq!(buffered.len(), 2);
    assert_ne!(buffered[1].tables, head.tables);
    let control = main.open_branch_control_dataset().await.unwrap();
    let native = feature_manifest.manifest().branch.clone().unwrap();
    main.settle_head_of(&control, &native, "feature")
        .await
        .unwrap();
    assert_eq!(buffered[0].commit, main.head().clone());
    let appended: Vec<HistoryRecord> = records.oldest_first().skip(1).collect();
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(appended.clone()),
        "main keeps the genesis commit the branch inherited"
    );
    assert_eq!(history_extent_count(uri).await, appended.len());

    main.delete_branch("feature").await.unwrap();
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(appended.clone()),
        "the delete after an interrupted one acknowledges the same immutable copies"
    );
    for record in &appended {
        assert_eq!(
            super::history::read_record(uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(record)
        );
    }
    assert!(matches!(
        ManifestCoordinator::open_at_branch(uri, "feature").await,
        Err(OmniError::BranchNotFound { .. })
    ));
    assert_eq!(
        super::history::read_record(uri, &session, &head.commit.graph_commit_id)
            .await
            .unwrap(),
        Some(head.clone())
    );
    assert_eq!(
        super::history::read_commit(uri, &session, &head.commit.graph_commit_id)
            .await
            .unwrap(),
        Some(head.commit.clone())
    );

    let snapshot = head.snapshot(uri).unwrap();
    let at_head = ManifestCoordinator::snapshot_from_state(
        uri,
        read_manifest_state(&feature_manifest).await.unwrap(),
    );
    assert_eq!(snapshot.version, at_head.version);
    assert_eq!(snapshot.graph_heads, at_head.graph_heads);
    assert_eq!(snapshot.graph_branch(), Some("feature"));
    assert_eq!(snapshot.entries.len(), at_head.entries.len());
    for (table_key, entry) in &at_head.entries {
        assert!(
            snapshot.entries[table_key].same_registration(entry),
            "{table_key}"
        );
    }

    let error = ManifestCoordinator::ensure_incarnation_live(uri, &session, &head.commit)
        .await
        .unwrap_err();
    assert!(matches!(error, OmniError::BranchNotFound { .. }), "{error}");
    main.create_branch("feature").await.unwrap();
    let error = ManifestCoordinator::ensure_incarnation_live(uri, &session, &head.commit)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no persisted native-branch incarnation witness"),
        "{error}"
    );
    ManifestCoordinator::ensure_incarnation_live(uri, &session, main.head())
        .await
        .unwrap();
}

/// Sixteen records of `block`; the commit at slot 0 opened it, so its nonce is
/// the block token.
pub(super) fn history_block_records(block: &str) -> Vec<HistoryRecord> {
    history_block_run(block, 16)
}

/// The records of the first `slots` slots of `block`, the opener at slot 0.
pub(super) fn history_block_run(block: &str, slots: usize) -> Vec<HistoryRecord> {
    let ids: Vec<_> = (0..slots)
        .map(|slot| match slot {
            0 => format!("hb1.{block}.0.{block}"),
            _ => format!("hb1.{block}.{slot}.{:026}", slot + 1),
        })
        .collect();
    ids.iter()
        .enumerate()
        .map(|(slot, id)| HistoryRecord {
            commit: GraphLineageRow {
                graph_manifest_version: slot as u64 + 1,
                generation: slot as u64,
                ..probe_commit(id, slot.checked_sub(1).map(|parent| ids[parent].as_str()))
            },
            tables: vec![probe_table(1, probe_pin(None))],
        })
        .collect()
}

/// One table row in each state a history record stores, in identity order.
pub(super) fn history_table_states() -> Vec<TableRow> {
    vec![
        probe_table(1, probe_pin(None)),
        probe_table(2, probe_pin(Some("feature.01ARZ3NDEKTSV4RRFFQ69G5FAV"))),
        probe_table(3, TableState::Registered),
        probe_table(
            4,
            TableState::Dropped {
                dropped_at: 3,
                sealed_version: 8,
            },
        ),
    ]
}

/// The hex noise of a [`bulky_table`]: SHA-256 digests of 64 hex characters each.
const BULKY_NOISE_BYTES: usize = 16 * 64;

/// Tables per record that take the `tables` column of `records` records over
/// `bytes` once encoded: hex noise compresses by less than three.
fn bulky_tables_over(records: usize, bytes: usize) -> u64 {
    (3 * bytes).div_ceil(records * BULKY_NOISE_BYTES) as u64
}

/// A pinned table whose metadata carries [`BULKY_NOISE_BYTES`] of hex noise.
fn bulky_table(record: usize, table: u64) -> TableRow {
    use sha2::{Digest, Sha256};
    let noise: String = (0..BULKY_NOISE_BYTES / 64)
        .map(|part| format!("{:x}", Sha256::digest(format!("{record}.{table}.{part}"))))
        .collect();
    let TableState::Pinned(mut pin) = probe_pin(None) else {
        unreachable!("probe_pin returns a pinned state");
    };
    pin.metadata = TableVersionMetadata::from_json_str(&format!(
        r#"{{"manifest_path":"p","manifest_size":null,"e_tag":"{noise}","naming_scheme":null}}"#
    ))
    .unwrap();
    probe_table(table + 1, TableState::Pinned(pin))
}

fn history_requests(probes: &crate::instrumentation::QueryIoProbes) -> (usize, usize, usize, u64) {
    let mut seen = (0, 0, 0, 0);
    for store in probes.history_stores.stores() {
        let stats = store.io_stats_incremental();
        seen.3 += stats.read_bytes;
        for request in stats.requests {
            seen.0 += usize::from(request.method.starts_with("get"));
            seen.1 += usize::from(request.method.starts_with("list"));
            seen.2 += usize::from(request.method.starts_with("head"));
        }
    }
    seen
}

/// The block `records` fills, as a writer that saw the commit after it knows it.
fn closed_block(records: &[HistoryRecord]) -> super::history::ClosedBlocks {
    let last = omnigraph_core::graph_commit_id::parse_history_block_id(
        &records.last().unwrap().commit.graph_commit_id,
    )
    .unwrap()
    .unwrap();
    HashMap::from([(last.block, last.slot)])
}

/// The lineage columns of an extent sit in the suffix a lineage read fetches,
/// so the read of a commit in a complete block is one GET of the name its id
/// gives, with no LIST, whatever the size of the `tables` column.
#[tokio::test]
async fn history_lineage_read_is_one_suffix_get_without_the_tables_column() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let mut records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let tables = bulky_tables_over(records.len(), 2 * super::history::TAIL_BYTES);
    for (index, record) in records.iter_mut().enumerate() {
        record.tables = (0..tables).map(|table| bulky_table(index, table)).collect();
    }
    super::history::settle_closed(&uri, &session, &records, &closed_block(&records))
        .await
        .unwrap();
    assert_eq!(
        super::history::stored_names(&uri).await.unwrap(),
        ["blocks/01ARZ3NDEKTSV4RRFFQ69G5FAV/all.lance"]
    );
    let wanted = &records[5];

    let probes = fresh_publish_probes();
    let lineage = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_lineage_of(&uri, &session, &[&wanted.commit.graph_commit_id]),
    )
    .await
    .unwrap();
    assert_eq!(lineage.len(), 16);
    for record in &records {
        assert_eq!(
            lineage.get(&record.commit.graph_commit_id),
            Some(&record.commit)
        );
    }
    let (gets, lists, heads, lineage_bytes) = history_requests(&probes);
    assert_eq!(
        (gets, lists, heads),
        (1, 0, 0),
        "a lineage read of a complete block is one suffix GET and no LIST"
    );
    assert!(
        lineage_bytes <= super::history::TAIL_BYTES as u64,
        "a lineage read fetched {lineage_bytes} bytes"
    );

    let probes = fresh_publish_probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_record(&uri, &session, &wanted.commit.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found.as_ref(), Some(wanted));
    let (gets, lists, heads, record_bytes) = history_requests(&probes);
    assert_eq!(
        (gets, lists, heads),
        (1, 0, 0),
        "a record read of a complete block is one whole-object GET and no LIST"
    );
    assert!(
        record_bytes >= 2 * super::history::TAIL_BYTES as u64,
        "the tables column makes the object {record_bytes} bytes, two suffixes or more"
    );
}

/// A run archived under its slot range has no object under the block's one
/// name: the reader finds it by the block LIST, and stops listing once a
/// writer that knows the block complete has created that name.
#[tokio::test]
async fn absent_block_name_falls_back_to_the_block_listing() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    super::history::settle(&uri, &session, &records[..6])
        .await
        .unwrap();
    super::history::settle(&uri, &session, &records[6..])
        .await
        .unwrap();
    assert_eq!(
        super::history::stored_names(&uri).await.unwrap(),
        [
            "blocks/01ARZ3NDEKTSV4RRFFQ69G5FAV/0-5.lance",
            "blocks/01ARZ3NDEKTSV4RRFFQ69G5FAV/6-15.lance"
        ]
    );
    let sent = HistoryGets::default();
    let read = |held: std::ops::Range<usize>| {
        let (sent, uri, session, records) = (&sent, &uri, &session, &records);
        async move {
            let probes = sent.probes();
            let lineage = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                super::history::read_lineage_of(
                    uri,
                    session,
                    &[&records[3].commit.graph_commit_id],
                ),
            )
            .await
            .unwrap();
            assert_eq!(lineage.len(), held.len());
            for record in &records[held] {
                assert_eq!(
                    lineage.get(&record.commit.graph_commit_id),
                    Some(&record.commit)
                );
            }
            let (found, lists, _, _) = history_requests(&probes);
            let (gets, absent, heads) = sent.drain();
            assert_eq!(gets, found + absent, "every GET sent is found or absent");
            (gets, absent, lists, heads)
        }
    };
    assert_eq!(
        read(0..6).await,
        (2, 1, 1, 0),
        "the GET of the absent name, the block LIST, and the GET of the one extent covering the slot"
    );

    super::history::settle_closed(&uri, &session, &records, &closed_block(&records))
        .await
        .unwrap();
    assert_eq!(super::history::stored_names(&uri).await.unwrap().len(), 3);
    assert_eq!(
        read(0..16).await,
        (1, 0, 0, 0),
        "the block's one name answers alone"
    );
    for record in &records {
        assert_eq!(
            super::history::read_record(&uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(record)
        );
    }
}

/// A record read of a block archived as disjoint range extents fetches, decodes and keeps only the extents that cover a requested slot.
#[tokio::test]
async fn one_record_read_fetches_only_the_range_extent_that_holds_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    super::history::settle(&uri, &session, &records[..6])
        .await
        .unwrap();
    super::history::settle(&uri, &session, &records[6..])
        .await
        .unwrap();
    assert_eq!(
        stored_ranges(&uri, "01ARZ3NDEKTSV4RRFFQ69G5FAV").await,
        [(0, 5), (6, 15)]
    );
    let wanted = &records[3];

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_record(&uri, &session, &wanted.commit.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found.as_ref(), Some(wanted));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((2, 1, 0), 1),
        "the absent whole-block name, the block LIST, and the extent `0-5` alone"
    );

    let cache = super::commit_graph::HistoryCache::default();
    assert_eq!(
        cache
            .read_record(&uri, &session, &wanted.commit.graph_commit_id)
            .await
            .unwrap()
            .as_ref(),
        Some(wanted)
    );
    assert_eq!(
        cache.objects().len(),
        1,
        "the extent `6-15` was never decoded"
    );

    let both = [
        wanted.commit.graph_commit_id.as_str(),
        records[9].commit.graph_commit_id.as_str(),
    ];
    let found = super::history::read_records_of(&uri, &session, &both)
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
    assert_eq!(found.get(both[0]), Some(wanted));
    assert_eq!(found.get(both[1]), Some(&records[9]));
}

/// The block LIST a reader falls back to holds the block's one name beside the
/// range extents of the same block: an id the block does not hold reads as
/// absent, and an id it holds is found, with and without a handle's cache.
#[tokio::test]
async fn block_listing_takes_the_whole_block_object_beside_its_range_extents() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let block = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let records = history_block_records(block);
    super::history::settle(&uri, &session, &records[..6])
        .await
        .unwrap();
    super::history::settle(&uri, &session, &records[6..])
        .await
        .unwrap();
    super::history::settle_closed(&uri, &session, &records, &closed_block(&records))
        .await
        .unwrap();
    assert_eq!(
        super::history::stored_names(&uri).await.unwrap(),
        [
            format!("blocks/{block}/all.lance"),
            format!("blocks/{block}/0-5.lance"),
            format!("blocks/{block}/6-15.lance"),
        ]
    );

    let never_published = format!("hb1.{block}.20.{:026}", 99);
    let held = &records[9];
    let cache = super::commit_graph::HistoryCache::default();
    for _ in 0..2 {
        assert_eq!(
            super::history::read_commit(&uri, &session, &never_published)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            super::history::read_record(&uri, &session, &never_published)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            cache
                .read_commit(&uri, &session, &never_published)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            cache
                .read_record(&uri, &session, &never_published)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            cache
                .read_commit(&uri, &session, &held.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(&held.commit)
        );
        assert_eq!(
            super::history::read_record(&uri, &session, &held.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(held)
        );
    }
}

/// A run is written under the block's one name only when it starts the block.
/// A run whose first commit has its parent in the same block continues it,
/// whatever its id says, and goes under its slot range.
#[tokio::test]
async fn run_whose_parent_lies_in_its_block_is_not_named_the_whole_block() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let block = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let mut records = history_block_records(block);
    let claims_the_block = format!("hb1.{block}.4.{block}");
    records[4].commit.graph_commit_id = claims_the_block.clone();
    records[5].commit.parent_commit_id = Some(claims_the_block);
    let closed = closed_block(&records);

    super::history::settle_closed(&uri, &session, &records[4..], &closed)
        .await
        .unwrap();
    assert_eq!(
        super::history::stored_names(&uri).await.unwrap(),
        [format!("blocks/{block}/4-15.lance")],
        "slot 4 carries the block token as its nonce, as the commit that opens a block \
         does, and continues the block: its parent is slot 3"
    );
    assert_eq!(
        super::history::read_commit(&uri, &session, &records[2].commit.graph_commit_id)
            .await
            .unwrap(),
        None,
        "the slots before the run are not archived"
    );
    assert_eq!(
        super::history::read_record(&uri, &session, &records[7].commit.graph_commit_id)
            .await
            .unwrap()
            .as_ref(),
        Some(&records[7])
    );

    super::history::settle_closed(&uri, &session, &records, &closed)
        .await
        .unwrap();
    assert_eq!(
        super::history::stored_names(&uri).await.unwrap(),
        [
            format!("blocks/{block}/all.lance"),
            format!("blocks/{block}/4-15.lance"),
        ],
        "the run from slot 0, whose parent lies outside the block, takes the one name"
    );
    for record in &records {
        assert_eq!(
            super::history::read_record(&uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(record)
        );
    }
}

/// A store that refuses a suffix range (Azure) serves a lineage read from the
/// head of the object: one GET for an extent no larger than the suffix, two for
/// a larger one, no HEAD; a store that takes a suffix range serves both in one GET.
#[tokio::test]
async fn lineage_read_needs_no_size_request_on_a_store_without_suffix_ranges() {
    let session = crate::lance_access::control_session();
    for small in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", dir.path().display());
        let mut records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let tables = if small {
            1
        } else {
            bulky_tables_over(records.len(), super::history::TAIL_BYTES)
        };
        for (index, record) in records.iter_mut().enumerate() {
            record.tables = (0..tables).map(|table| bulky_table(index, table)).collect();
        }
        super::history::settle_closed(&uri, &session, &records, &closed_block(&records))
            .await
            .unwrap();
        let wanted = &records[5].commit.graph_commit_id;
        for refuse_suffix in [false, true] {
            let sent = HistoryGets {
                refuse_suffix,
                ..Default::default()
            };
            let probes = sent.probes();
            let lineage = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                super::history::read_lineage_of(&uri, &session, &[wanted]),
            )
            .await
            .unwrap();
            assert_eq!(lineage.len(), records.len());
            for record in &records {
                assert_eq!(
                    lineage.get(&record.commit.graph_commit_id),
                    Some(&record.commit)
                );
            }
            let (_, lists, _, bytes) = history_requests(&probes);
            let two_gets = refuse_suffix && !small;
            assert_eq!(
                (sent.drain(), lists),
                ((1 + usize::from(two_gets), 0, 0), 0),
                "{tables} tables per commit, suffix refused: {refuse_suffix}"
            );
            let suffix = super::history::TAIL_BYTES as u64;
            assert_eq!(
                small,
                bytes < suffix,
                "the lineage read fetched {bytes} bytes"
            );
            assert!(bytes <= suffix * (1 + u64::from(two_gets)));

            let found = crate::instrumentation::with_query_io_probes(
                sent.probes(),
                super::history::read_record(&uri, &session, wanted),
            )
            .await
            .unwrap();
            assert_eq!(found.as_ref(), Some(&records[5]));
            assert_eq!(sent.drain(), (1, 0, 0), "a record read is one whole GET");
        }
    }
}

/// An immutable archive object a handle has read is served again with no
/// request and no freshness check. An absent name and a LIST are asked again.
#[tokio::test]
async fn archive_object_read_once_is_served_again_with_no_request() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let whole = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    super::history::settle_closed(&uri, &session, &whole, &closed_block(&whole))
        .await
        .unwrap();
    let ranged = history_block_records("01BX5ZZKBKACTAV9WEVGEMMVRZ");
    super::history::settle(&uri, &session, &ranged)
        .await
        .unwrap();
    let single = HistoryRecord {
        commit: probe_commit("opaque", None),
        tables: vec![probe_table(1, probe_pin(None))],
    };
    super::history::settle(&uri, &session, std::slice::from_ref(&single))
        .await
        .unwrap();

    let cache = super::commit_graph::HistoryCache::default();
    let sent = HistoryGets::default();
    let commit = |record: &HistoryRecord| {
        let (cache, uri, session) = (cache.clone(), uri.clone(), session.clone());
        let record = record.clone();
        let probes = sent.probes();
        async move {
            let found = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                cache.read_commit(&uri, &session, &record.commit.graph_commit_id),
            )
            .await
            .unwrap();
            assert_eq!(found, Some(record.commit));
            history_requests(&probes)
        }
    };
    let full = |record: &HistoryRecord| {
        let (cache, uri, session) = (cache.clone(), uri.clone(), session.clone());
        let record = record.clone();
        async move {
            let probes = fresh_publish_probes();
            let found = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                cache.read_record(&uri, &session, &record.commit.graph_commit_id),
            )
            .await
            .unwrap();
            assert_eq!(found, Some(record));
            history_requests(&probes)
        }
    };

    let (gets, lists, heads, bytes) = commit(&whole[3]).await;
    assert_eq!((gets, lists, heads), (1, 0, 0));
    assert!(bytes > 0);
    for record in [&whole[3], &whole[12]] {
        assert_eq!(
            commit(record).await,
            (0, 0, 0, 0),
            "every commit of an object the handle read costs no request"
        );
    }
    let (gets, lists, heads, _) = full(&whole[3]).await;
    assert_eq!(
        (gets, lists, heads),
        (1, 0, 0),
        "the lineage a handle holds has no tables: a record read fetches the object once"
    );
    assert_eq!(full(&whole[9]).await, (0, 0, 0, 0));
    assert_eq!(
        commit(&whole[9]).await,
        (0, 0, 0, 0),
        "complete records serve a lineage read"
    );

    let (gets, lists, heads, _) = full(&single).await;
    assert_eq!((gets, lists, heads), (1, 0, 0));
    assert_eq!(full(&single).await, (0, 0, 0, 0));
    assert_eq!(commit(&single).await, (0, 0, 0, 0));

    sent.drain();
    let (gets, lists, heads, _) = commit(&ranged[2]).await;
    assert_eq!((gets, lists, heads), (1, 1, 0));
    assert_eq!(
        sent.drain(),
        (2, 1, 0),
        "the GET of the block's absent name precedes the LIST"
    );
    assert_eq!(
        commit(&ranged[2]).await,
        (0, 1, 0, 0),
        "absence is never cached: the LIST is asked again, the listed object is not fetched"
    );
    assert_eq!(
        sent.drain(),
        (1, 1, 0),
        "the absent name is asked again with it"
    );
    assert_eq!(cache.objects().len(), 3);

    let unread = super::commit_graph::HistoryCache::default();
    assert_eq!(unread.objects().len(), 0);
    assert_eq!(
        super::history::read_commit(&uri, &session, &whole[3].commit.graph_commit_id)
            .await
            .unwrap(),
        Some(whole[3].commit.clone())
    );
    assert_eq!(
        unread.objects().len(),
        0,
        "a read outside a handle keeps nothing"
    );
}

struct HistoryIo {
    puts: usize,
    gets: usize,
    lists: usize,
    heads: usize,
}

fn history_io(probes: &crate::instrumentation::QueryIoProbes) -> HistoryIo {
    let mut seen = HistoryIo {
        puts: 0,
        gets: 0,
        lists: 0,
        heads: 0,
    };
    for store in probes.history_stores.stores() {
        for request in store.io_stats_incremental().requests {
            seen.puts += usize::from(request.method.starts_with("put"));
            seen.gets += usize::from(request.method.starts_with("get"));
            seen.lists += usize::from(request.method.starts_with("list"));
            seen.heads += usize::from(request.method.starts_with("head"));
        }
    }
    seen
}

/// One lineage-only publish of `coordinator` with a production nonce, the
/// commit it wrote, and the requests it made to `__history`.
async fn publish_block_commit(
    coordinator: &mut ManifestCoordinator,
    intent: LineageIntent,
) -> (GraphLineageRow, HistoryIo) {
    let probes = fresh_publish_probes();
    crate::instrumentation::with_query_io_probes(
        probes.clone(),
        coordinator.commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent)),
    )
    .await
    .unwrap();
    (coordinator.head().clone(), history_io(&probes))
}

fn block_slot(commit: &GraphLineageRow) -> (ulid::Ulid, u16) {
    let id = omnigraph_core::graph_commit_id::parse_history_block_id(&commit.graph_commit_id)
        .unwrap()
        .unwrap();
    (id.block, id.slot)
}

/// The release is decided by the bytes the buffer holds: small commits pass
/// the old sixteen-commit bound unreleased, and large ones release before it.
#[tokio::test]
async fn release_fires_on_the_byte_budget_not_on_a_commit_count() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut small = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    for published in 1..=40u16 {
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            ..lineage_intent(None, None)
        };
        let (commit, io) = publish_block_commit(&mut small, intent).await;
        assert_eq!(block_slot(&commit).1, published);
        assert_eq!(io.puts, 0, "publish {published} released the buffer");
    }
    assert_eq!(small.buffer().commits().len(), 40);
    assert!(
        small
            .buffer()
            .buffered_bytes(small.head(), &small.head_record().tables)
            < HISTORY_RELEASE_BYTES
    );
    assert_eq!(history_extent_count(uri).await, 0);

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut large = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let fat = HISTORY_RELEASE_BYTES / 8;
    let fill = HISTORY_RELEASE_BYTES.div_ceil(fat);
    let mut published = 0;
    loop {
        published += 1;
        assert!(
            published <= fill + 2,
            "{fill} commits of {fat} bytes reach the {HISTORY_RELEASE_BYTES}-byte budget, the \
             next publish opens a block and the one after it releases"
        );
        let below = large
            .buffer()
            .buffered_bytes(large.head(), &large.head_record().tables)
            < HISTORY_RELEASE_BYTES;
        let was_full = large
            .buffer()
            .is_full(large.head(), &large.head_record().tables)
            .unwrap();
        let intent = LineageIntent {
            actor_id: Some("a".repeat(fat)),
            ..fat_block_intent(None)
        };
        let (commit, io) = publish_block_commit(&mut large, intent).await;
        if was_full {
            assert!(io.puts > 0);
            assert_eq!(block_slot(&commit).1, 1);
            break;
        }
        assert_eq!(io.puts, 0);
        assert_eq!(
            block_slot(&commit).1 == 0,
            !below,
            "the commit published on a buffer at the budget opens a block, and no other does"
        );
        assert_eq!(
            large
                .buffer()
                .is_full(large.head(), &large.head_record().tables)
                .unwrap(),
            !below
        );
    }
    assert_eq!(history_row_count(uri).await, published - 1);
    assert_eq!(large.buffer().commits().len(), 1);
}

/// In the steady state of one branch a release is one PUT of every slot of
/// one block under the block's one name, with no other request to `__history`.
#[tokio::test]
async fn steady_release_is_one_put_of_every_slot_of_one_block() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    while !main
        .buffer()
        .is_full(main.head(), &main.head_record().tables)
        .unwrap()
    {
        publish_block_commit(&mut main, fat_block_intent(None)).await;
    }
    let first = main.buffer().commits().to_vec();
    assert_eq!(
        block_slot(&first[1]).1,
        1,
        "the first release of a lineage holds the genesis singleton and a block starting at slot 1"
    );
    let (_, io) = publish_block_commit(&mut main, fat_block_intent(None)).await;
    assert_eq!((io.puts, io.gets, io.lists, io.heads), (2, 0, 0, 0));
    let mut names = super::history::stored_names(uri).await.unwrap();
    assert_eq!(names.len(), 2);
    assert_eq!(
        names[0],
        format!("blocks/{}/all.lance", block_slot(&first[1]).0),
        "the block that starts at slot 1 is whole under its one name too"
    );
    let mut archived = first.len();

    for _ in 0..3 {
        while !main
            .buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
        {
            let (_, io) = publish_block_commit(&mut main, fat_block_intent(None)).await;
            assert_eq!((io.puts, io.gets, io.lists, io.heads), (0, 0, 0, 0));
        }
        let block = main.buffer().commits().to_vec();
        let (name, _) = block_slot(&block[0]);
        for (slot, commit) in block.iter().enumerate() {
            assert_eq!(block_slot(commit), (name, slot as u16));
        }
        let (next, slot) = block_slot(main.head());
        assert!(next != name && slot == 0, "the head opened the next block");

        let (_, io) = publish_block_commit(&mut main, fat_block_intent(None)).await;
        assert_eq!(
            (io.puts, io.gets, io.lists, io.heads),
            (1, 0, 0, 0),
            "a steady release is one conditional create"
        );
        let after = super::history::stored_names(uri).await.unwrap();
        assert_eq!(
            after
                .iter()
                .filter(|name| !names.contains(name))
                .collect::<Vec<_>>(),
            [&format!("blocks/{name}/all.lance")]
        );
        names = after;
        archived += block.len();
        assert_eq!(history_row_count(uri).await, archived);

        let probes = fresh_publish_probes();
        let session = crate::lance_access::control_session();
        let lineage = crate::instrumentation::with_query_io_probes(
            probes.clone(),
            super::history::read_lineage_of(uri, &session, &[&block[1].graph_commit_id]),
        )
        .await
        .unwrap();
        assert_eq!(lineage.len(), block.len());
        assert!(
            block
                .iter()
                .all(|commit| lineage.get(&commit.graph_commit_id) == Some(commit))
        );
        let (gets, lists, heads, _) = history_requests(&probes);
        assert_eq!(
            (gets, lists, heads),
            (1, 0, 0),
            "the lineage of a block released on the byte budget fits one suffix GET"
        );
    }
}

/// A merge-base search that walked archived blocks keeps them in the cache of
/// its handle: the same search again issues no request and finds the same base.
#[tokio::test]
async fn repeated_merge_base_search_makes_no_request() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    while !main
        .buffer()
        .is_full(main.head(), &main.head_record().tables)
        .unwrap()
    {
        publish_block_commit(&mut main, fat_block_intent(None)).await;
    }
    for _ in 0..3 {
        publish_block_commit(&mut main, fat_block_intent(None)).await;
    }
    let fork_point = main.head().graph_commit_id.clone();
    main.create_branch("feature").await.unwrap();
    let mut feature = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    publish_block_commit(&mut feature, fat_block_intent(Some("feature"))).await;
    for _ in 0..3 {
        while !main
            .buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
        {
            publish_block_commit(&mut main, fat_block_intent(None)).await;
        }
        publish_block_commit(&mut main, fat_block_intent(None)).await;
    }
    assert!(
        main.buffer().get(&fork_point).is_none(),
        "the fork point left the buffer of main"
    );

    let late = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    let target = late.commit_graph();
    let source = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .commit_graph();
    assert!(target.held_merge_base(&source).is_none());
    let search = || async {
        let probes = fresh_publish_probes();
        let base = crate::instrumentation::with_query_io_probes(
            probes.clone(),
            target.merge_base(&source),
        )
        .await
        .unwrap();
        assert_eq!(
            base.map(|base| base.graph_commit_id).as_deref(),
            Some(fork_point.as_str())
        );
        history_requests(&probes)
    };
    let (gets, lists, heads, bytes) = search().await;
    assert!(
        (4..=6).contains(&gets) && lists == 0 && heads == 0 && bytes > 0,
        "a cold search is one GET per object it walks (the three blocks main released since \
         the fork, the fork point's block, and what the source side reaches before the base \
         is proven: main's first block and the genesis commit): {gets} GET, {lists} LIST, \
         {heads} HEAD"
    );
    assert_eq!(search().await, (0, 0, 0, 0));
    assert_eq!(late.history().objects().len(), gets);
}

#[tokio::test]
async fn history_settles_one_aligned_block_with_one_put_and_no_list() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let probes = fresh_publish_probes();
    let appended = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::settle(&uri, &session, &records),
    )
    .await
    .unwrap();
    assert!(records.iter().all(|record| appended.holds(&record.commit)));
    let requests: Vec<_> = probes
        .history_stores
        .stores()
        .into_iter()
        .flat_map(|store| store.io_stats_incremental().requests)
        .collect();
    assert_eq!(
        (
            requests
                .iter()
                .filter(|request| request.method.starts_with("put"))
                .count(),
            requests
                .iter()
                .filter(|request| request.method.starts_with("list"))
                .count(),
        ),
        (1, 0),
        "one uncontended 16-commit block must archive with one PUT and no directory lookup: {requests:?}"
    );
    for record in &records {
        assert_eq!(
            super::history::read_record(&uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(record)
        );
    }
}

#[tokio::test]
async fn history_block_lookup_does_not_read_unrelated_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    super::history::settle(&uri, &session, &records)
        .await
        .unwrap();
    let wanted = &records[7];
    let mut before = None;
    for round in 0..2 {
        if round == 1 {
            for block in 1..=16 {
                let unrelated = history_block_records(&format!("{block:026}"));
                super::history::settle(&uri, &session, &unrelated)
                    .await
                    .unwrap();
            }
        }
        let probes = fresh_publish_probes();
        let found = crate::instrumentation::with_query_io_probes(
            probes.clone(),
            super::history::read_record(&uri, &session, &wanted.commit.graph_commit_id),
        )
        .await
        .unwrap();
        assert_eq!(found.as_ref(), Some(wanted));
        let mut read_iops = 0;
        let mut read_bytes = 0;
        let mut read_paths = Vec::new();
        for store in probes.history_stores.stores() {
            let stats = store.io_stats_incremental();
            read_iops += stats.read_iops;
            read_bytes += stats.read_bytes;
            read_paths.extend(
                stats
                    .requests
                    .into_iter()
                    .filter(|request| request.method.starts_with("get"))
                    .map(|request| request.path.to_string()),
            );
        }
        read_paths.sort();
        assert!(read_iops > 0 && read_bytes > 0 && !read_paths.is_empty());
        let observed = (read_iops, read_bytes, read_paths);
        match &before {
            Some(before) => assert_eq!(
                &observed, before,
                "adding unrelated history blocks must not change a commit lookup's requests, bytes, or fetched objects"
            ),
            None => before = Some(observed),
        }
    }
}

#[tokio::test]
async fn history_reads_absent_as_empty_and_takes_any_copy_of_a_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    assert!(settled_commits(uri).await.is_empty());
    assert!(
        super::history::read_record(uri, &session, "a")
            .await
            .unwrap()
            .is_none()
    );

    let first = HistoryRecord {
        commit: GraphLineageRow {
            schema_contract: Some(replacement_contract().head),
            schema_content_hash: Some(
                super::history::schema_content_hash(&replacement_contract()).unwrap(),
            ),
            ..probe_commit("a", None)
        },
        tables: vec![
            probe_table(1, probe_pin(None)),
            probe_table(2, probe_pin(Some("feature.01ARZ3NDEKTSV4RRFFQ69G5FAV"))),
            probe_table(3, TableState::Registered),
            probe_table(
                4,
                TableState::Dropped {
                    dropped_at: 3,
                    sealed_version: 8,
                },
            ),
        ],
    };
    let second = HistoryRecord {
        commit: GraphLineageRow {
            actor_id: None,
            ..probe_commit("b", Some("a"))
        },
        tables: Vec::new(),
    };
    super::history::settle(uri, &session, std::slice::from_ref(&first))
        .await
        .unwrap();
    super::history::settle(uri, &session, &[second.clone(), first.clone()])
        .await
        .unwrap();
    assert_eq!(history_row_count(uri).await, 2);

    assert_eq!(
        settled_commits(uri).await,
        HashMap::from([
            ("a".to_string(), first.commit.clone()),
            ("b".to_string(), second.commit.clone())
        ])
    );
    for record in [&first, &second] {
        assert_eq!(
            super::history::read_record(uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(record)
        );
        assert_eq!(
            super::history::read_commit(uri, &session, &record.commit.graph_commit_id)
                .await
                .unwrap()
                .as_ref(),
            Some(&record.commit)
        );
    }
    assert_eq!(
        super::history::read_records_of(uri, &session, &["a", "b", "missing"])
            .await
            .unwrap(),
        HashMap::from([
            ("a".to_string(), first.clone()),
            ("b".to_string(), second.clone()),
        ])
    );
    let stored = super::history::stored_batches(uri).await.unwrap();
    assert_eq!(stored.len(), 2, "duplicate settlement is idempotent");
    let schema = stored[0].schema();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["graph_commit_id", "record", "tables"]
    );
    let record_field = schema.field_with_name("record").unwrap();
    assert!(!record_field.is_nullable());
    let arrow_schema::DataType::Struct(children) = record_field.data_type() else {
        panic!("history must preserve the packed record representation");
    };
    assert!(children.iter().all(|field| !field.is_nullable()));
    let actor_bit = super::record::COMMIT_FIELDS
        .iter()
        .position(|(name, _)| *name == "actor_id")
        .unwrap();
    let mut actor_null_bits = HashMap::new();
    for batch in &stored {
        let record = batch
            .column_by_name("record")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        assert_eq!(record.num_columns(), super::record::COMMIT_FIELDS.len() + 1);
        assert!(record.columns().iter().all(|child| child.null_count() == 0));
        let actors = record
            .column_by_name("actor_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(actors.iter().all(|actor| actor == Some("")));
        let present = record
            .column_by_name("present")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .unwrap();
        let ids = batch
            .column_by_name("graph_commit_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (id, bits) in ids.iter().zip(present.values()) {
            actor_null_bits.insert(id.unwrap().to_string(), bits & (1 << actor_bit) != 0);
        }
    }
    assert_eq!(
        actor_null_bits,
        HashMap::from([("a".to_string(), false), ("b".to_string(), true)])
    );

    let mut forged = first.clone();
    forged.commit.actor_id = Some("someone else".to_string());
    let error = super::history::settle(uri, &session, &[forged])
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("two records of graph commit 'a' that differ"),
        "{error}"
    );
    assert_eq!(
        super::history::read_record(uri, &session, "a")
            .await
            .unwrap(),
        Some(first)
    );
}

/// A null marker must not hide a non-filler value in any history field type.
#[tokio::test]
async fn history_refuses_null_bits_over_non_fillers() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let record = HistoryRecord {
        commit: probe_commit("a", Some("parent")),
        tables: Vec::new(),
    };
    super::history::settle(uri, &session, &[record])
        .await
        .unwrap();
    let stored = super::history::stored_batches(uri).await.unwrap();
    assert_eq!(stored.len(), 1);
    let stored = &stored[0];
    let packed = stored
        .column_by_name("record")
        .expect("history stores commit fields in one packed record")
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let original = packed
        .column_by_name("present")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    for field in ["parent_commit_id", "generation", "created_at"] {
        let bit = super::record::COMMIT_FIELDS
            .iter()
            .position(|(name, _)| *name == field)
            .unwrap();
        let mut present = original.values().to_vec();
        present[0] |= 1 << bit;
        let tampered = with_present_bits(stored, Arc::new(arrow_array::UInt32Array::from(present)));
        let error = super::history::records_of(&tampered).unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "'{field}' is marked null but carries a value at row 0"
            )),
            "{error}"
        );
    }
}

/// Concurrent first archives preserve both independent immutable objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_first_appends_both_land() {
    for _ in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let session = crate::lance_access::control_session();
        let records = ["a", "b"].map(|id| HistoryRecord {
            commit: probe_commit(id, None),
            tables: vec![probe_table(1, probe_pin(None))],
        });
        let (first, second) = tokio::join!(
            super::history::settle(uri, &session, &records[..1]),
            super::history::settle(uri, &session, &records[1..]),
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(history_row_count(uri).await, 2);
        assert_eq!(settled_commits(uri).await.len(), 2);
    }
}

/// The rows of the `__manifest` version that wrote `commit`.
async fn rows_written_with(uri: &str, commit: &GraphLineageRow) -> ManifestRows {
    let main = open_manifest_dataset(uri, None).await.unwrap();
    let version = lance::dataset::refs::Ref::Version(
        commit.native_branch.clone(),
        Some(commit.graph_manifest_version),
    );
    let rows = read_manifest_rows(&main.checkout_version(version).await.unwrap())
        .await
        .unwrap();
    assert_eq!(&rows.head, commit);
    rows
}

/// Assert that the state `rows` gives each commit it buffers is, row for row,
/// the `table` rows of the version that wrote the commit, and that `rows` keeps
/// no `replaced_table` row the oldest buffered commit does not need.
async fn assert_buffered_states(uri: &str, rows: &ManifestRows) {
    for record in rows.buffer.records(&rows.tables) {
        assert_eq!(
            record.tables,
            rows_written_with(uri, &record.commit).await.tables,
            "state of buffered commit {:?}",
            record.commit
        );
    }
    let oldest = rows.buffer.commits().first().unwrap_or(&rows.head);
    assert_eq!(
        rows.buffer
            .replaced()
            .iter()
            .filter(|replaced| replaced.replaced_at <= oldest.graph_manifest_version)
            .collect::<Vec<_>>(),
        Vec::<&ReplacedTable>::new(),
        "rows replaced at or before version {} restore no buffered commit",
        oldest.graph_manifest_version
    );
}

#[tokio::test]
async fn state_of_a_buffered_commit_is_the_table_rows_of_the_version_that_wrote_it() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let company = mc.snapshot().dataset("node:Company").unwrap().clone();
    let mut company_key = company.type_key.clone();
    let mut registered = Vec::new();
    let mut written = Vec::new();

    let mut round = 0;
    let mut since_release = 0;
    let publishes_after_the_first_release = 5;
    while since_release <= publishes_after_the_first_release {
        round += 1;
        if since_release > 0
            || mc
                .buffer()
                .is_full(mc.head(), &mc.head_record().tables)
                .unwrap()
        {
            since_release += 1;
        }
        let change = match round % 5 {
            1 => {
                let person = mc.snapshot().dataset("node:Person").unwrap().clone();
                let name = format!("p{round}");
                let update = append_person_and_make_update(uri, &person, &name).await;
                Some(ManifestChange::Update(update))
            }
            2 => {
                let identity = TableIdentity::new(9_000 + round, 1).unwrap();
                let table_key = format!("node:Extra{round}");
                let registration = TableRegistration {
                    identity,
                    table_path: table_path_for_identity(&table_key, identity).unwrap(),
                    table_key,
                };
                registered.push(registration.clone());
                Some(ManifestChange::RegisterTable(registration))
            }
            3 => {
                let expected_table_key = company_key.clone();
                company_key = format!("node:Company{round}");
                Some(ManifestChange::RenameTable(TableRename {
                    identity: company.identity,
                    expected_table_key,
                    table_key: company_key.clone(),
                    table_path: company.dataset_path.clone(),
                }))
            }
            4 => {
                let dropped: TableRegistration = registered.pop().unwrap();
                Some(ManifestChange::Tombstone(TableTombstone {
                    identity: dropped.identity,
                    table_key: dropped.table_key,
                    tombstone_version: 1,
                }))
            }
            _ => None,
        };
        let changes: Vec<ManifestChange> = change.into_iter().collect();
        mc.commit_changes_with_lineage(&changes, &HashMap::new(), Some(&fat_intent(None, None)))
            .await
            .unwrap();

        let rows = main_rows(uri).await;
        assert!(!rows.buffer.commits().is_empty());
        assert_buffered_states(uri, &rows).await;
        assert_eq!(mc.branch_records(), rows.records());
        written.push(HistoryRecord {
            commit: rows.head.clone(),
            tables: rows.tables,
        });
    }

    let released = history_records(uri).await;
    assert!(
        released.len() > 5,
        "the one release wrote the genesis commit and every round before it"
    );
    assert_eq!(released.len() + 6, round as usize);
    assert_eq!(
        released[1..],
        written[..released.len() - 1],
        "a released commit carries the state it had while it was buffered"
    );
    assert_eq!(
        super::history::read_record(uri, &session, &released[0].commit.graph_commit_id)
            .await
            .unwrap()
            .map(|genesis| genesis.commit.parent_commit_id),
        Some(None)
    );
}

/// A fork holds what the version it forks holds, and its first publish
/// buffers the head it inherited with the state that head was written with.
#[tokio::test]
async fn fork_inherits_the_buffer_and_the_replaced_rows_of_the_version_it_forks() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    for round in 0..3 {
        let person = main.snapshot().dataset("node:Person").unwrap().clone();
        let update = append_person_and_make_update(uri, &person, &format!("m{round}")).await;
        main.commit_changes_with_lineage(
            &[ManifestChange::Update(update)],
            &HashMap::new(),
            Some(&lineage_intent(None, None)),
        )
        .await
        .unwrap();
    }
    main.create_branch("feature").await.unwrap();
    let mut fork = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    assert_eq!(fork.branch_records(), main.branch_records());
    assert_eq!(fork.buffer(), main.buffer());
    assert_eq!(fork.buffer().replaced().len(), 3);

    let company = fork.snapshot().dataset("node:Company").unwrap().clone();
    let update = append_node_row_and_make_update(uri, &company, "c1", "Acme").await;
    fork.commit_changes_with_lineage(
        &[ManifestChange::Update(update)],
        &HashMap::new(),
        Some(&lineage_intent(Some("feature"), None)),
    )
    .await
    .unwrap();
    let rows = read_manifest_rows(&open_manifest_dataset(uri, Some("feature")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        rows.buffer.commits(),
        [main.buffer().commits(), std::slice::from_ref(main.head())].concat()
    );
    assert_buffered_states(uri, &rows).await;
    assert_eq!(history_row_count(uri).await, 0);
}

/// Production nonces assign slots from the captured buffer phase. A fork
/// archives an inherited prefix before its parent archives the full block;
/// both copies must resolve to the same complete historical table snapshots.
#[tokio::test]
async fn block_placement_preserves_fork_and_parent_records_through_two_releases() {
    use omnigraph_core::graph_commit_id::parse_history_block_id;

    async fn publish(
        uri: &str,
        coordinator: &mut ManifestCoordinator,
        branch: Option<&str>,
        name: &str,
    ) -> HistoryRecord {
        let previous = coordinator.head_record().clone();
        let buffer = coordinator.buffer();
        let expected_slot = if buffer.is_full(&previous.commit, &previous.tables).unwrap() {
            1
        } else if buffer.closes_with(&previous.commit, &previous.tables) {
            0
        } else {
            buffer.commits().len() as u16 + 1
        };
        let person = coordinator
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let update = append_person_and_make_update(uri, &person, name).await;
        let intent = fat_block_intent(branch);
        let outcome = coordinator
            .commit_changes_with_lineage(
                &[ManifestChange::Update(update.clone())],
                &HashMap::new(),
                Some(&intent),
            )
            .await
            .unwrap();
        let record = coordinator.head_record().clone();
        assert_eq!(outcome.commit.as_ref(), Some(&record.commit));
        assert_eq!(
            record.commit.parent_commit_id.as_deref(),
            Some(previous.commit.graph_commit_id.as_str())
        );
        assert_intent_nonce(&record.commit.graph_commit_id, &intent);
        let id = parse_history_block_id(&record.commit.graph_commit_id)
            .unwrap()
            .unwrap();
        assert_eq!(id.slot, expected_slot);
        if let Some(parent) = parse_history_block_id(&previous.commit.graph_commit_id).unwrap() {
            let extends_block = previous.commit.native_branch == record.commit.native_branch
                && parent.slot + 1 == id.slot;
            assert_eq!(id.block == parent.block, extends_block);
        }
        let table = record
            .tables
            .iter()
            .find(|row| row.registration.identity == person.identity)
            .unwrap();
        assert!(matches!(&table.state, TableState::Pinned(pin)
            if pin.table_version == update.published_dataset_version));
        record
    }

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut expected = vec![main.head_record().clone()];
    let mut round = 0;
    while !main
        .buffer()
        .is_full(main.head(), &main.head_record().tables)
        .unwrap()
    {
        round += 1;
        expected.push(publish(uri, &mut main, None, &format!("main-{round}")).await);
    }
    for _ in 0..5 {
        round += 1;
        expected.push(publish(uri, &mut main, None, &format!("main-{round}")).await);
    }
    let fork_point = main.head_record().clone();
    let inherited = parse_history_block_id(&fork_point.commit.graph_commit_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        inherited.slot, 5,
        "past the singleton genesis release the fork point is slot 5 of a block that began at \
         slot 0, so the fork releases A[0..5] before main releases all of A"
    );
    let block = format!("blocks/{}/", inherited.block);
    main.create_branch("feature").await.unwrap();
    let mut fork = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    assert_eq!(fork.branch_records(), main.branch_records());
    for release in 0..2 {
        while !fork
            .buffer()
            .is_full(fork.head(), &fork.head_record().tables)
            .unwrap()
        {
            round += 1;
            expected.push(publish(uri, &mut fork, Some("feature"), &format!("fork-{round}")).await);
        }
        round += 1;
        expected.push(publish(uri, &mut fork, Some("feature"), &format!("fork-{round}")).await);
        if release == 0 {
            let names = super::history::stored_names(uri).await.unwrap();
            assert!(
                names.contains(&format!("{block}0-5.lance"))
                    && !names.contains(&format!("{block}all.lance")),
                "a fork archives the prefix it inherited under its slot range: {names:?}"
            );
        }
    }
    let session = crate::lance_access::control_session();
    assert_eq!(
        super::history::read_record(uri, &session, &fork_point.commit.graph_commit_id)
            .await
            .unwrap(),
        Some(fork_point.clone()),
        "the fork's first release archived its inherited prefix"
    );
    for _ in 0..2 {
        while !main
            .buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
        {
            round += 1;
            expected.push(publish(uri, &mut main, None, &format!("main-{round}")).await);
        }
        round += 1;
        expected.push(publish(uri, &mut main, None, &format!("main-{round}")).await);
    }
    let names = super::history::stored_names(uri).await.unwrap();
    assert!(
        names.contains(&format!("{block}0-5.lance"))
            && names.contains(&format!("{block}all.lance")),
        "the branch that wrote the block archives it whole under its one name: {names:?}"
    );
    assert_eq!(
        super::history::read_record(uri, &session, &fork_point.commit.graph_commit_id)
            .await
            .unwrap(),
        Some(fork_point.clone()),
        "both copies of the inherited prefix hold the same records"
    );
    for branch in [&main, &fork] {
        let records = branch.branch_records();
        let records: Vec<_> = records.oldest_first().collect();
        super::history::settle(uri, &session, &records)
            .await
            .unwrap();
    }
    for record in &mut expected {
        record
            .tables
            .sort_by_key(|table| table.registration.identity);
    }
    let ids: Vec<_> = expected
        .iter()
        .map(|record| record.commit.graph_commit_id.as_str())
        .collect();
    let found = super::history::read_records_of(uri, &session, &ids)
        .await
        .unwrap();
    assert_eq!(
        found.len(),
        expected.len(),
        "after both branches settle their acknowledged tails every captured record, each \
         branch's last head included, is resolvable through storage"
    );
    for record in &expected {
        assert_eq!(found.get(&record.commit.graph_commit_id), Some(record));
    }

    main.delete_branch("feature").await.unwrap();
    let cache = super::commit_graph::HistoryCache::default();
    for record in &expected {
        let id = &record.commit.graph_commit_id;
        assert_eq!(
            cache.read_record(uri, &session, id).await.unwrap().as_ref(),
            Some(record),
            "the delete archives what the fork still buffered; {id} stays resolvable by id \
             through its direct name or the block listing"
        );
        assert_eq!(
            super::history::read_commit(uri, &session, id)
                .await
                .unwrap()
                .as_ref(),
            Some(&record.commit)
        );
    }
}

#[tokio::test]
async fn publication_rejects_history_block_ids_as_intent_nonces() {
    use omnigraph_core::graph_commit_id::HistoryBlockId;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let coordinator = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let original = coordinator.head_record().clone();
    let version = coordinator.version();
    let valid = HistoryBlockId::new(ulid::Ulid::new(), 3, ulid::Ulid::new())
        .unwrap()
        .to_string();
    let publisher = GraphNamespacePublisher::new(uri, None);
    for nonce in [valid, "hb1".to_string(), "hb1.malformed".to_string()] {
        let mut intent = lineage_intent(None, None);
        intent.graph_commit_id = nonce;
        let error = publisher
            .publish(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("intent must contain a nonce"),
            "{error}"
        );
        let reopened = ManifestCoordinator::open(uri).await.unwrap();
        assert_eq!(reopened.version(), version);
        assert_eq!(reopened.head_record(), &original);
        assert_eq!(history_row_count(uri).await, 0);
    }
}

/// One lineage-only publish on main and how many times it opened `__manifest`
/// or `__history`.
async fn publish_and_count_opens(uri: &str, intent: &LineageIntent) -> u64 {
    let probes = crate::instrumentation::QueryIoProbes::default();
    let opens = Arc::clone(&probes.internal_open_count);
    crate::instrumentation::with_query_io_probes(probes, async {
        GraphNamespacePublisher::new(uri, None)
            .publish(&[], &HashMap::new(), Some(intent))
            .await
            .unwrap()
    })
    .await;
    opens.load(std::sync::atomic::Ordering::Relaxed)
}

#[tokio::test]
async fn publish_appends_to_history_only_when_it_finds_the_buffer_full() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let genesis = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap()
        .head_record()
        .clone();
    let mut published = vec![genesis.commit.graph_commit_id.clone()];

    let mut opens_with_room = HashSet::new();
    let mut full = main_rows(uri).await;
    while !full.buffer.is_full(&full.head, &full.tables).unwrap() {
        let intent = fat_intent(None, None);
        opens_with_room.insert(publish_and_count_opens(uri, &intent).await);
        published.push(intent.graph_commit_id);
        assert_eq!(history_extent_count(uri).await, 0);
        full = main_rows(uri).await;
    }
    let first = full.buffer.commits().len();
    assert_eq!(first + 1, published.len());
    assert_eq!(
        opens_with_room.len(),
        1,
        "every publish into a buffer with room opens the same datasets: {opens_with_room:?}"
    );
    let opens_with_room = opens_with_room.into_iter().next().unwrap();

    let releasing = fat_intent(None, None);
    assert_eq!(
        publish_and_count_opens(uri, &releasing).await,
        opens_with_room,
        "settlement does not open another Lance dataset"
    );
    assert_eq!(
        history_extent_count(uri).await,
        first,
        "explicit singleton fixture IDs each have one immutable object"
    );
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(full.buffer.records(&full.tables))
    );
    assert_eq!(
        full.buffer
            .commits()
            .iter()
            .map(|commit| commit.graph_commit_id.clone())
            .collect::<Vec<_>>(),
        published[..first]
    );
    let mut released = main_rows(uri).await;
    assert_eq!(released.head.graph_commit_id, releasing.graph_commit_id);
    assert_eq!(released.buffer.commits(), [full.head]);

    let mut publishes = first + 1;
    while !released
        .buffer
        .is_full(&released.head, &released.tables)
        .unwrap()
    {
        let intent = fat_intent(None, None);
        assert_eq!(publish_and_count_opens(uri, &intent).await, opens_with_room);
        publishes += 1;
        released = main_rows(uri).await;
    }
    let second = released.buffer.commits().len();
    assert!(second > 1);
    assert_eq!(history_extent_count(uri).await, first);
    publish_and_count_opens(uri, &fat_intent(None, None)).await;
    assert_eq!(history_extent_count(uri).await, first + second);
    assert_eq!(history_row_count(uri).await, first + second);
    assert_linear_chain(uri, publishes + 2).await;
}

/// A merge that finds the target's buffer full appends the target's buffered
/// commits and the records the source wrote in one Append, each commit once.
#[tokio::test]
async fn merge_into_a_full_buffer_appends_both_lineages_in_one_append() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    main.create_branch("feature").await.unwrap();
    let mut feature = ManifestCoordinator::open_at_branch(uri, "feature")
        .await
        .unwrap();
    for round in 0..2 {
        let company = feature.snapshot().dataset("node:Company").unwrap().clone();
        let update =
            append_node_row_and_make_update(uri, &company, &format!("c{round}"), "Acme").await;
        feature
            .commit_changes_with_lineage(
                &[ManifestChange::Update(update)],
                &HashMap::new(),
                Some(&lineage_intent(Some("feature"), None)),
            )
            .await
            .unwrap();
    }
    let filled = fill_buffer(&mut main, None).await.len();
    assert!(
        main.buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
    );
    assert_eq!(main.buffer().commits().len(), filled);
    assert_eq!(history_extent_count(uri).await, 0);
    let target = main.branch_records();
    let source = feature.branch_records();
    let target_buffered = target.buffer.records(&target.head.tables);
    let source_buffered = source.buffer.records(&source.head.tables);
    assert_eq!(source_buffered.len(), 2);

    let merge = lineage_intent(None, Some(source.clone()));
    main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&merge))
        .await
        .unwrap();
    assert_eq!(history_extent_count(uri).await, filled + 2);
    assert_eq!(source_buffered[0], target_buffered[0]);
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(
            [
                target_buffered,
                source_buffered[1..].to_vec(),
                vec![source.head.clone()]
            ]
            .concat()
        ),
        "the target buffer and complete source-owned records are each archived once"
    );
    assert_eq!(main.buffer().commits(), [target.head.commit]);
    assert_eq!(
        main.head().merged_parent_commit_id.as_deref(),
        Some(source.head.commit.graph_commit_id.as_str())
    );
    let chain = lineage_and_history_reads(&main).await;
    assert_eq!(chain.0.len(), filled + 2);
}

/// One lineage-only publish of `coordinator` on `branch`, and the commit it wrote.
async fn publish_on(
    coordinator: &mut ManifestCoordinator,
    branch: Option<&str>,
    merged_parent: Option<BranchRecords>,
) -> GraphLineageRow {
    let intent = lineage_intent(branch, merged_parent);
    coordinator
        .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
        .await
        .unwrap();
    assert_eq!(coordinator.head().graph_commit_id, intent.graph_commit_id);
    coordinator.head().clone()
}

/// The IDs of all distinct archived commits.
async fn history_ids(uri: &str) -> HashSet<String> {
    history_records(uri)
        .await
        .into_iter()
        .map(|record| record.commit.graph_commit_id)
        .collect()
}

/// Each merge of a still-open source block archives only the source commits no earlier merge archived, so a cold lookup of its newest and oldest commit stays in budget.
#[tokio::test]
async fn repeated_merges_of_an_open_source_block_append_each_commit_once_and_stay_readable() {
    const MERGES: u16 = 450;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    main.create_branch("f").await.unwrap();
    let mut source = ManifestCoordinator::open_at_branch(uri, "f").await.unwrap();
    let mut first = None;
    for _ in 0..MERGES {
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            ..lineage_intent(Some("f"), None)
        };
        source
            .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        first.get_or_insert_with(|| source.head().clone());
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            ..lineage_intent(None, Some(source.branch_records()))
        };
        main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
    }
    let first = first.unwrap();
    let newest = source.head().clone();
    let (block, newest_slot) = block_slot(&newest);
    assert_eq!((block, 1), block_slot(&first));
    assert_eq!(newest_slot, MERGES);
    assert!(
        !source
            .buffer()
            .is_full(&newest, &source.head_record().tables)
            .unwrap(),
        "the source block is still open"
    );

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_commit(uri, &session, &newest.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found, Some(newest));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((2, 1, 0), 1),
        "the absent whole-block name, the block LIST, and the one extent that covers the slot"
    );
    assert_eq!(
        super::history::read_commit(uri, &session, &first.graph_commit_id)
            .await
            .unwrap(),
        Some(first),
        "the oldest source commit is covered by one extent, not by every later merge"
    );
    assert_eq!(
        stored_ranges(uri, &block.to_string()).await,
        (1..=MERGES).map(|slot| (slot, slot)).collect::<Vec<_>>(),
        "merge i archives source slot i alone"
    );
}

/// A merge appends only the source commits after its merge base, so a target that released its buffer between two merges of an open source block archives no prefix of the block again.
#[tokio::test]
async fn merges_separated_by_a_target_release_append_each_source_commit_once() {
    const MERGES: u16 = 4;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    main.create_branch("f").await.unwrap();
    let mut source = ManifestCoordinator::open_at_branch(uri, "f").await.unwrap();
    let mut merge_base = main.head().graph_commit_id.clone();
    let mut first = None;
    for _ in 0..MERGES {
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            ..lineage_intent(Some("f"), None)
        };
        source
            .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        first.get_or_insert_with(|| source.head().clone());
        let merged = BranchRecords {
            merge_base: Some(merge_base),
            ..source.branch_records()
        };
        merge_base = merged.head.commit.graph_commit_id.clone();
        main.commit_changes_with_lineage(
            &[],
            &HashMap::new(),
            Some(&lineage_intent(None, Some(merged))),
        )
        .await
        .unwrap();
        let merge_commit = main.head().graph_commit_id.clone();
        while main.held_commit(&merge_commit).is_some() {
            main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&fat_intent(None, None)))
                .await
                .unwrap();
        }
    }
    let first = first.unwrap();
    let source_block = format!("blocks/{}/", block_slot(&first).0);
    let mut source_extents = super::history::stored_names(uri).await.unwrap();
    source_extents.retain(|name| name.starts_with(&source_block));
    source_extents.sort();
    assert_eq!(
        source_extents,
        (1..=MERGES)
            .map(|slot| format!("{source_block}{slot}-{slot}.lance"))
            .collect::<Vec<_>>(),
        "main held no merge commit at any merge after the first, and merge i still archives slot i alone"
    );

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_commit(uri, &session, &first.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found, Some(first));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((2, 1, 0), 1),
        "the oldest source commit is covered by one extent"
    );
}

/// The block `01ARZ3NDEKTSV4RRFFQ69G5FAV` under 1,104 range extents that all
/// cover slot 48: every `start-end` with `start` in 0..=47 and `end` in 48..=70,
/// the run `(k+1)-j` a target forked at `k` appends when it merges at `j`.
async fn settle_ranges_over_slot_48(
    uri: &str,
    session: &Arc<lance::session::Session>,
) -> Vec<HistoryRecord> {
    let records = history_block_run("01ARZ3NDEKTSV4RRFFQ69G5FAV", 96);
    for start in 0..=47 {
        for end in 48..=70 {
            super::history::settle(uri, session, &records[start..=end])
                .await
                .unwrap();
        }
    }
    assert_eq!(
        stored_ranges(uri, "01ARZ3NDEKTSV4RRFFQ69G5FAV").await.len(),
        48 * 23
    );
    records
}

/// A slot covered by more range extents than the old listing cap is read from the three narrowest of them, cold and through a handle's cache, for its commit and for its record.
#[tokio::test]
async fn a_slot_covered_by_over_a_thousand_equal_ranges_is_read_from_three_of_them() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = settle_ranges_over_slot_48(&uri, &session).await;
    let wanted = &records[48];
    let id = wanted.commit.graph_commit_id.as_str();

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_commit(&uri, &session, id),
    )
    .await
    .unwrap();
    assert_eq!(found.as_ref(), Some(&wanted.commit));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((4, 1, 0), 1),
        "the absent whole-block name, the block LIST, and the copies `47-48`, `46-48`, `47-49`"
    );

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_record(&uri, &session, id),
    )
    .await
    .unwrap();
    assert_eq!(found.as_ref(), Some(wanted));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((4, 1, 0), 1),
        "a record read fetches the same three copies whole"
    );

    let cache = super::commit_graph::HistoryCache::default();
    assert_eq!(
        cache
            .read_commit(&uri, &session, id)
            .await
            .unwrap()
            .as_ref(),
        Some(&wanted.commit)
    );
    assert_eq!(
        cache.objects().len(),
        3,
        "the handle keeps the three copies it read and no other"
    );
}

/// Each target branch forked from main merges main's still-open block from its own fork slot, so the block's directory gains one range per target that all end at main's head; a cold read of that head fetches three of them.
#[tokio::test]
async fn several_targets_merging_one_open_source_block_keep_its_head_readable_in_bounded_gets() {
    const TARGETS: u16 = 8;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let block_intent = || LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        ..lineage_intent(None, None)
    };
    main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&block_intent()))
        .await
        .unwrap();
    let mut fork_bases = Vec::new();
    for target in 0..TARGETS {
        main.create_branch(&format!("t{target}")).await.unwrap();
        fork_bases.push(main.head().graph_commit_id.clone());
        main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&block_intent()))
            .await
            .unwrap();
    }
    let head = main.head().clone();
    let (block, head_slot) = block_slot(&head);
    assert_eq!(head_slot, TARGETS + 1);
    assert!(
        !main
            .buffer()
            .is_full(&head, &main.head_record().tables)
            .unwrap(),
        "main's block is still open"
    );
    for (target, fork_base) in fork_bases.into_iter().enumerate() {
        let name = format!("t{target}");
        let mut target = ManifestCoordinator::open_at_branch(uri, &name)
            .await
            .unwrap();
        let merged = BranchRecords {
            merge_base: Some(fork_base),
            ..main.branch_records()
        };
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            ..lineage_intent(Some(&name), Some(merged))
        };
        target
            .commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
    }
    assert_eq!(
        stored_ranges(uri, &block.to_string()).await,
        (2..=head_slot)
            .map(|start| (start, head_slot))
            .collect::<Vec<_>>(),
        "the target forked at slot k appends the run (k+1)-{head_slot}"
    );

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_commit(uri, &session, &head.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found.as_ref(), Some(&head));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!(
        (sent.drain(), lists),
        ((4, 1, 0), 1),
        "the absent whole-block name, the block LIST, and the three narrowest of the {TARGETS} covering ranges"
    );

    let sent = HistoryGets::default();
    let probes = sent.probes();
    let found = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_record(uri, &session, &head.graph_commit_id),
    )
    .await
    .unwrap();
    assert_eq!(found.map(|record| record.commit), Some(head));
    let (_, lists, _, _) = history_requests(&probes);
    assert_eq!((sent.drain(), lists), ((4, 1, 0), 1));
}

/// A copy that differs from the others at a requested slot is refused when it is among the extents read, however many equal copies cover the slot beside it.
#[tokio::test]
async fn a_conflicting_copy_among_the_ranges_read_is_still_refused() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = settle_ranges_over_slot_48(&uri, &session).await;
    let mut conflicting = records[48..=48].to_vec();
    conflicting[0].commit.actor_id = Some("different".to_string());
    super::history::settle(&uri, &session, &conflicting)
        .await
        .unwrap();
    let id = records[48].commit.graph_commit_id.as_str();

    let error = super::history::read_commit(&uri, &session, id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("that differ"), "{error}");
    let error = super::history::read_record(&uri, &session, id)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("that differ"), "{error}");
}

/// The addressed read compares only the `COPIES_READ` narrowest covering range
/// extents: a copy that differs in the widest one is answered past by the
/// addressed read and refused by the whole-archive read.
#[tokio::test]
async fn a_conflicting_copy_outside_the_narrowest_three_is_refused_only_by_the_whole_archive_read()
{
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let records = settle_ranges_over_slot_48(&uri, &session).await;
    let mut widest = records[0..=95].to_vec();
    widest[48].commit.actor_id = Some("different".to_string());
    super::history::settle(&uri, &session, &widest)
        .await
        .unwrap();
    let id = records[48].commit.graph_commit_id.as_str();

    let commit = super::history::read_commit(&uri, &session, id)
        .await
        .unwrap()
        .expect("the three narrowest covering copies agree, so the addressed read answers");
    assert_eq!(
        commit.actor_id, records[48].commit.actor_id,
        "the addressed read answers from the narrowest copies, not from the widest one"
    );
    let error = super::history::read_lineage(&uri, &session)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("that differ"), "{error}");
}

/// A merge appends no commit the target wrote or holds, and a delete appends
/// the commits the deleted branch wrote: a branch per task costs one row of
/// `__history` per commit of the task at its merge and one at its delete.
#[tokio::test]
async fn merge_and_delete_append_only_the_commits_no_other_lineage_keeps() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut main = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    for _ in 0..8 {
        publish_on(&mut main, None, None).await;
    }

    main.create_branch("task").await.unwrap();
    let mut task = ManifestCoordinator::open_at_branch(uri, "task")
        .await
        .unwrap();
    let on_task = publish_on(&mut task, Some("task"), None).await;
    assert_eq!(task.buffer().commits().len(), 9);
    publish_on(&mut main, None, Some(task.branch_records())).await;
    assert_eq!(
        history_ids(uri).await,
        HashSet::from([on_task.graph_commit_id.clone()]),
        "main buffers every commit the task inherited"
    );
    main.delete_branch("task").await.unwrap();
    assert_eq!(
        history_ids(uri).await,
        HashSet::from([on_task.graph_commit_id.clone()]),
        "the deleted branch wrote one commit"
    );

    main.create_branch("slow").await.unwrap();
    let mut slow = ManifestCoordinator::open_at_branch(uri, "slow")
        .await
        .unwrap();
    let on_slow = publish_on(&mut slow, Some("slow"), None).await;
    let inherited: Vec<String> = slow
        .buffer()
        .commits()
        .iter()
        .map(|commit| commit.graph_commit_id.clone())
        .collect();
    while main.buffer().get(&inherited[0]).is_some() {
        let intent = fat_intent(None, None);
        main.commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
    }
    publish_on(&mut main, None, None).await;
    assert!(
        !main
            .buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
    );
    let settled = settled_commits(uri).await;
    assert!(inherited.iter().all(|id| settled.contains_key(id)));
    let before = history_ids(uri).await;
    publish_on(&mut main, None, Some(slow.branch_records())).await;
    assert_eq!(
        history_ids(uri)
            .await
            .difference(&before)
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from([on_slow.graph_commit_id.clone()]),
        "main appended every commit the branch inherited from it"
    );

    main.create_branch("outer").await.unwrap();
    let mut outer = ManifestCoordinator::open_at_branch(uri, "outer")
        .await
        .unwrap();
    let on_outer = publish_on(&mut outer, Some("outer"), None).await;
    outer.create_branch("inner").await.unwrap();
    let mut inner = ManifestCoordinator::open_at_branch(uri, "inner")
        .await
        .unwrap();
    let on_inner = publish_on(&mut inner, Some("inner"), None).await;
    let before = history_ids(uri).await;
    assert!(
        !main
            .buffer()
            .is_full(main.head(), &main.head_record().tables)
            .unwrap()
    );
    publish_on(&mut main, None, Some(inner.branch_records())).await;
    assert_eq!(
        history_ids(uri)
            .await
            .difference(&before)
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from([
            on_outer.graph_commit_id.clone(),
            on_inner.graph_commit_id.clone()
        ]),
        "a commit of a third branch is in no `__manifest` a reader of main opens"
    );
    let before_delete = history_ids(uri).await;
    main.delete_branch("inner").await.unwrap();
    main.delete_branch("outer").await.unwrap();
    assert_eq!(
        history_ids(uri).await,
        before_delete,
        "each delete acknowledges its already archived records"
    );

    main.create_branch("left").await.unwrap();
    main.create_branch("right").await.unwrap();
    let mut left = ManifestCoordinator::open_at_branch(uri, "left")
        .await
        .unwrap();
    let mut right = ManifestCoordinator::open_at_branch(uri, "right")
        .await
        .unwrap();
    let on_right = publish_on(&mut right, Some("right"), None).await;
    assert!(
        !left
            .buffer()
            .is_full(left.head(), &left.head_record().tables)
            .unwrap()
    );
    assert_eq!(
        right.buffer().get(&left.head().graph_commit_id),
        Some(left.head())
    );
    let before = history_ids(uri).await;
    publish_on(&mut left, Some("left"), Some(right.branch_records())).await;
    assert_eq!(
        history_ids(uri)
            .await
            .difference(&before)
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from([on_right.graph_commit_id.clone()]),
        "the target holds the commits of main both branches inherited"
    );
    assert_lineage_holds_every_ancestor(&left.commit_graph().lineage().await.unwrap());

    let late = ManifestCoordinator::open_with_session(uri, &session)
        .await
        .unwrap();
    let lineage = late.commit_graph().lineage().await.unwrap();
    for commit in [&on_task, &on_slow, &on_outer, &on_inner] {
        assert_eq!(
            lineage
                .get_commit(&commit.graph_commit_id)
                .map(|found| found.graph_commit_id.as_str()),
            Some(commit.graph_commit_id.as_str())
        );
    }
    assert_lineage_holds_every_ancestor(&lineage);
}

/// Every commit the head of `lineage` reaches through either parent is one
/// the lineage holds.
fn assert_lineage_holds_every_ancestor(lineage: &super::commit_graph::Lineage) {
    let mut reached = vec![lineage.head().graph_commit_id.clone()];
    let mut seen = HashSet::new();
    while let Some(id) = reached.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        let commit = lineage
            .get_commit(&id)
            .unwrap_or_else(|| panic!("no `__manifest` row and no `__history` row holds '{id}'"));
        reached.extend(commit.parent_commit_id.clone());
        reached.extend(commit.merged_parent_commit_id.clone());
    }
}

fn buffered_run(length: usize) -> (CommitBuffer, GraphLineageRow) {
    let mut commits: Vec<GraphLineageRow> = Vec::new();
    for version in 1..=length as u64 + 1 {
        let parent = commits.last().map(|parent| parent.graph_commit_id.clone());
        commits.push(GraphLineageRow {
            graph_manifest_version: version,
            generation: version - 1,
            ..probe_commit(&format!("c{version}"), parent.as_deref())
        });
    }
    let head = commits.pop().unwrap();
    (
        CommitBuffer {
            commits,
            replaced: Vec::new(),
        },
        head,
    )
}

#[tokio::test]
async fn commit_leaves_the_buffer_only_after_the_append_that_holds_it() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let (full, replaced_head) = buffered_run(TAIL_MAX_COMMITS);
    let next_head = GraphLineageRow {
        graph_manifest_version: replaced_head.graph_manifest_version + 1,
        ..probe_commit("next", Some(&replaced_head.graph_commit_id))
    };
    let after = |appended| {
        full.after_publish(
            Some(&replaced_head),
            Vec::new(),
            &next_head,
            appended,
            &[],
            HistoryReleaseBytes::PRODUCTION,
        )
    };

    let error = after(None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("'c1' would leave the buffer of `__manifest`"),
        "{error}"
    );
    let all_but_the_newest =
        super::history::settle(uri, &session, &full.records(&[])[..TAIL_MAX_COMMITS - 1])
            .await
            .unwrap();
    let error = after(Some(&all_but_the_newest)).unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "'c{TAIL_MAX_COMMITS}' would leave the buffer of `__manifest`"
        )),
        "{error}"
    );
    let all = super::history::settle(uri, &session, &full.records(&[]))
        .await
        .unwrap();
    assert_eq!(
        after(Some(&all)).unwrap().commits(),
        std::slice::from_ref(&replaced_head)
    );

    let (with_room, replaced_head) = buffered_run(15);
    assert!(!with_room.is_full(&replaced_head, &[]).unwrap());
    assert_eq!(
        with_room
            .after_publish(
                Some(&replaced_head),
                Vec::new(),
                &next_head,
                None,
                &[],
                HistoryReleaseBytes::PRODUCTION,
            )
            .unwrap()
            .commits()
            .len(),
        16
    );
}

/// A failing history store proves these buffered operations never access history.
#[tokio::test]
async fn buffer_with_room_is_published_and_read_with_no_request_to_history() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let session = crate::lance_access::control_session();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut probes = fresh_publish_probes();
    probes.history_wrapper = Some(Arc::new(super::history::test_support::ReadFault {
        block_list: true,
    }));
    crate::instrumentation::with_query_io_probes(probes, async {
        super::history::read_lineage(uri, &session)
            .await
            .expect_err("a read of the blocked `__history` fails");

        let mut follower = ManifestCoordinator::open_with_session(uri, &session)
            .await
            .unwrap();
        let mut published = 0;
        while !mc
            .buffer()
            .is_full(mc.head(), &mc.head_record().tables)
            .unwrap()
        {
            published += 1;
            let person = mc.snapshot().dataset("node:Person").unwrap().clone();
            let update = append_person_and_make_update(uri, &person, "Alice").await;
            mc.commit_changes_with_lineage(
                &[ManifestChange::Update(update)],
                &HashMap::new(),
                Some(&fat_intent(None, None)),
            )
            .await
            .unwrap();
            follower.refresh().await.unwrap();
            let chain = follower
                .commit_graph()
                .lineage()
                .await
                .unwrap()
                .first_parent_chain()
                .unwrap();
            assert_eq!(chain.len(), published + 1);
            let manifest = open_manifest_dataset(uri, None).await.unwrap();
            let person_pins = super::state::read_manifest_entries(uri, &manifest)
                .await
                .unwrap()
                .into_iter()
                .filter(|entry| entry.type_key == "node:Person")
                .count();
            assert_eq!(person_pins, published + 1);
            assert_eq!(
                follower
                    .held_record(&chain[0].graph_commit_id)
                    .map(|genesis| genesis.tables),
                Some(
                    rows_written_with(uri, &follower.buffer().commits()[0])
                        .await
                        .tables
                )
            );
        }
        mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
            .await
            .expect_err("the publish that finds the buffer full appends to `__history`");
    })
    .await;
}

/// A publish whose Append fails leaves `__manifest` as it read it: the
/// buffered commits stay in the buffer and the next publish appends them.
#[tokio::test]
async fn publish_whose_append_fails_keeps_the_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    fill_buffer(&mut mc, None).await;
    let full = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    let version = mc.version();
    let blocked = dir.path().join("__history/singletons");
    std::fs::write(&blocked, b"a file where the records of these commits go").unwrap();

    let refused = lineage_intent(None, None);
    mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&refused))
        .await
        .expect_err("a publish cannot drop buffered commits it could not append");
    assert_eq!(mc.probe_latest_version().await.unwrap(), version);
    let kept = read_manifest_rows(&open_manifest_dataset(uri, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(kept.records(), full.records());

    std::fs::remove_file(&blocked).unwrap();
    mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
        .await
        .unwrap();
    assert_eq!(
        history_records(uri).await,
        sorted_history_records(full.buffer.records(&full.tables))
    );
    assert_eq!(mc.buffer().commits(), [full.head]);
}

/// A `replaced_table` row stores what differs from the identity's `table` row
/// (no `location`, `table_key` across a rename, the pin's metadata as a delta;
/// whole metadata when the current row holds no pin) and reads back whole.
#[test]
fn replaced_table_rows_store_the_delta_against_the_current_row() {
    let pin = |table_version: u64, metadata: &str| {
        TableState::Pinned(TablePin {
            table_version,
            table_branch: None,
            row_count: 0,
            metadata: TableVersionMetadata::from_json_str(metadata).unwrap(),
            manifest_version: table_version,
        })
    };
    let current_metadata = r#"{"manifest_path":"t/_versions/9.manifest","manifest_size":90,"e_tag":"e9","naming_scheme":"V2","transaction_uuid":"u9"}"#;
    let before_metadata = r#"{"manifest_path":"t/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V2"}"#;
    let mut renamed = probe_table(7, pin(7, before_metadata));
    renamed.registration.table_key = "node:Earlier".to_string();
    let rows = ManifestRows {
        schema_contract_head: None,
        schema_contract: None,
        tables: vec![
            probe_table(7, pin(9, current_metadata)),
            probe_table(
                8,
                TableState::Dropped {
                    dropped_at: 9,
                    sealed_version: 8,
                },
            ),
        ],
        head: probe_commit("c9", Some("c8")),
        buffer: CommitBuffer {
            commits: vec![probe_commit("c8", None)],
            replaced: vec![
                ReplacedTable {
                    replaced_at: 9,
                    before: ReplacedRow::Table(Box::new(probe_table(7, pin(8, before_metadata)))),
                },
                ReplacedTable {
                    replaced_at: 8,
                    before: ReplacedRow::Table(Box::new(renamed)),
                },
                ReplacedTable {
                    replaced_at: 9,
                    before: ReplacedRow::Table(Box::new(probe_table(8, pin(8, before_metadata)))),
                },
                ReplacedTable {
                    replaced_at: 7,
                    before: ReplacedRow::Unregistered(
                        probe_table(8, TableState::Registered).registration,
                    ),
                },
            ],
        },
    };
    let logical = rows.to_batch().unwrap();
    let column = |name: &str| {
        logical
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.map(str::to_string))
            .collect::<Vec<_>>()
    };
    let replaced_rows = 4..8;
    assert_eq!(
        column("object_type")[replaced_rows.clone()],
        vec![Some("replaced_table".to_string()); 4]
    );
    assert_eq!(
        column("location")[replaced_rows.clone()],
        [None, None, None, None]
    );
    assert_eq!(
        column("table_key")[replaced_rows.clone()],
        [None, Some("node:Earlier".to_string()), None, None]
    );
    let json = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
    let delta = json(
        r#"{"e_tag":"e8","manifest_file":"8.manifest","manifest_size":80,"transaction_uuid":null}"#,
    );
    assert_eq!(
        column("metadata")[replaced_rows]
            .iter()
            .map(|text| text.as_deref().map(json))
            .collect::<Vec<_>>(),
        [
            Some(delta.clone()),
            Some(delta),
            Some(json(before_metadata)),
            None,
        ]
    );

    let schema = super::record::manifest_storage_schema(HashMap::new());
    let stored = super::record::compact_to_storage(&logical, &schema).unwrap();
    let decoded = super::state::rows_of_batch(&stored).unwrap();
    let mut expected = rows.buffer.replaced.clone();
    expected.sort_by_key(|row| (row.replaced_at, row.identity()));
    assert_eq!(decoded.buffer.replaced, expected);
    assert_eq!(decoded.tables, rows.tables);
}

/// The bytes `to_batch` wrote at `row`: every string value and every non-null fixed-width cell.
fn stored_row_bytes(batch: &RecordBatch, row: usize) -> usize {
    use arrow_array::{Array, LargeStringArray};
    batch
        .columns()
        .iter()
        .filter(|column| column.is_valid(row))
        .map(|column| {
            let cell = column.as_any();
            if let Some(strings) = cell.downcast_ref::<StringArray>() {
                strings.value(row).len()
            } else if let Some(strings) = cell.downcast_ref::<LargeStringArray>() {
                strings.value(row).len()
            } else {
                column
                    .data_type()
                    .primitive_width()
                    .unwrap_or_else(|| panic!("no stored size for a {} cell", column.data_type()))
            }
        })
        .sum()
}

/// One `replaced_table` row over `current`: the rows, its share of `buffered_bytes`, its stored bytes.
fn replaced_row_measure_and_stored(
    current: Vec<TableRow>,
    before: ReplacedRow,
) -> (ManifestRows, usize, usize) {
    let rows = ManifestRows {
        schema_contract_head: None,
        schema_contract: None,
        tables: current,
        head: probe_commit("c9", Some("c8")),
        buffer: CommitBuffer {
            commits: vec![],
            replaced: vec![ReplacedTable {
                replaced_at: 9,
                before,
            }],
        },
    };
    let stored = stored_row_bytes(&rows.to_batch().unwrap(), rows.tables.len() + 1);
    let measure = rows.buffer.buffered_bytes(&rows.head, &rows.tables)
        - CommitBuffer::default().buffered_bytes(&rows.head, &rows.tables);
    (rows, measure, stored)
}

fn measured_pin(table_version: u64, metadata: &str) -> TableState {
    TableState::Pinned(TablePin {
        table_version,
        table_branch: None,
        row_count: 0,
        metadata: TableVersionMetadata::from_json_str(metadata).unwrap(),
        manifest_version: table_version,
    })
}

/// The release measure of a `replaced_table` row is at least the row `to_batch` writes when it keeps the old key across a rename.
#[test]
fn replaced_row_measure_covers_the_key_stored_across_a_rename() {
    let metadata = r#"{"manifest_path":"t/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V2"}"#;
    let mut renamed = probe_table(7, measured_pin(8, metadata));
    renamed.registration.table_key = format!("node:{}", "x".repeat(HISTORY_RELEASE_BYTES));
    let (rows, measure, stored) = replaced_row_measure_and_stored(
        vec![probe_table(7, measured_pin(8, metadata))],
        ReplacedRow::Table(Box::new(renamed)),
    );
    assert!(
        stored > HISTORY_RELEASE_BYTES,
        "the row keeps the old key: {stored} bytes stored"
    );
    assert!(
        measure >= stored,
        "measured {measure} bytes for a row of {stored}"
    );
    assert!(rows.buffer.closes_with(&rows.head, &rows.tables));
}

/// The release measure of a `replaced_table` row is at least the row `to_batch` writes when it holds the metadata delta between two pins.
#[test]
fn replaced_row_measure_covers_the_metadata_delta_between_pins() {
    let before = format!(
        r#"{{"manifest_path":"t/_versions/8.manifest","manifest_size":80,"e_tag":"{}","naming_scheme":"V2"}}"#,
        "e".repeat(4096)
    );
    let current = r#"{"manifest_path":"t/_versions/9.manifest","manifest_size":90,"e_tag":"e9","naming_scheme":"V2","table_fork_owner":"owner","staged_version":9,"transaction_uuid":"u9","last_linear_version":1}"#;
    let (_, measure, stored) = replaced_row_measure_and_stored(
        vec![probe_table(7, measured_pin(9, current))],
        ReplacedRow::Table(Box::new(probe_table(7, measured_pin(8, &before)))),
    );
    assert!(
        stored > 4096,
        "the row keeps the differing member: {stored} bytes stored"
    );
    assert!(
        measure >= stored,
        "measured {measure} bytes for a row of {stored}"
    );
}

/// The release measure of a `replaced_table` row is at least the row `to_batch` writes when it holds whole metadata: the current row is dropped, or absent.
#[test]
fn replaced_row_measure_covers_whole_metadata_under_an_unpinned_current_row() {
    let before = format!(
        r#"{{"manifest_path":"{}/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V2"}}"#,
        "t".repeat(4096)
    );
    let dropped = probe_table(
        7,
        TableState::Dropped {
            dropped_at: 9,
            sealed_version: 8,
        },
    );
    for current in [vec![dropped], vec![]] {
        let (rows, measure, stored) = replaced_row_measure_and_stored(
            current,
            ReplacedRow::Table(Box::new(probe_table(7, measured_pin(8, &before)))),
        );
        assert!(
            stored > 4096,
            "the row keeps whole metadata over {} current rows: {stored} bytes stored",
            rows.tables.len()
        );
        assert!(
            measure >= stored,
            "measured {measure} bytes for a row of {stored} over {} current rows",
            rows.tables.len()
        );
    }
}

/// The release measure of the ordinary `replaced_table` row, a pin replaced under a pin of the same key, is the fixed allowance plus the delta the row stores: no key, no whole metadata.
#[test]
fn replaced_row_measure_tracks_the_delta_of_a_pin_replaced_under_a_pin() {
    let dir = "t".repeat(4096);
    let before = format!(
        r#"{{"manifest_path":"{dir}/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V2"}}"#
    );
    let current = format!(
        r#"{{"manifest_path":"{dir}/_versions/9.manifest","manifest_size":90,"e_tag":"e9","naming_scheme":"V2"}}"#
    );
    let key = format!("node:{}", "x".repeat(4096));
    let mut before_row = probe_table(7, measured_pin(8, &before));
    before_row.registration.table_key = key.clone();
    let mut current_row = probe_table(7, measured_pin(9, &current));
    current_row.registration.table_key = key;
    let delta = super::row::metadata_delta(
        &TableVersionMetadata::from_json_str(&before).unwrap(),
        &TableVersionMetadata::from_json_str(&current).unwrap(),
    )
    .unwrap();
    assert!(delta.len() < 128, "the row stores a small delta: {delta}");
    let (_, measure, stored) = replaced_row_measure_and_stored(
        vec![current_row],
        ReplacedRow::Table(Box::new(before_row)),
    );
    assert!(
        measure >= stored,
        "measured {measure} bytes for a row of {stored}"
    );
    assert!(
        measure <= stored + TABLE_ROW_BYTES,
        "measured {measure} bytes for a row of {stored}: the key or whole metadata is charged"
    );
    assert_eq!(
        measure,
        TABLE_ROW_BYTES + delta.len() - r#""manifest_size":80,"#.len()
            + super::row::MANIFEST_SIZE_MEASURE_BYTES,
        "the measure charges `manifest_size` at its fixed width: {delta}"
    );
}

/// The release measure does not follow `manifest_size`: Lance sizes its manifest by a wall-clock varint, so two current pins that differ only in whether the member repeats give one measure although the stored delta elides it in one of them.
#[test]
fn replaced_row_measure_does_not_follow_a_repeated_manifest_size() {
    let before = r#"{"manifest_path":"t/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V2"}"#;
    let repeated = r#"{"manifest_path":"t/_versions/9.manifest","manifest_size":80,"e_tag":"e9","naming_scheme":"V2"}"#;
    let moved = r#"{"manifest_path":"t/_versions/9.manifest","manifest_size":81,"e_tag":"e9","naming_scheme":"V2"}"#;
    let delta_against = |current: &str| {
        super::row::metadata_delta(
            &TableVersionMetadata::from_json_str(before).unwrap(),
            &TableVersionMetadata::from_json_str(current).unwrap(),
        )
        .unwrap()
    };
    assert_ne!(
        delta_against(repeated).len(),
        delta_against(moved).len(),
        "the stored delta elides a repeated `manifest_size`"
    );
    let measure_over = |current: &str| {
        let (_, measure, _) = replaced_row_measure_and_stored(
            vec![probe_table(7, measured_pin(9, current))],
            ReplacedRow::Table(Box::new(probe_table(7, measured_pin(8, before)))),
        );
        measure
    };
    assert_eq!(
        measure_over(repeated),
        measure_over(moved),
        "a repeated `manifest_size` must not move the release"
    );
}

/// The release measure covers the widest metadata delta: every base member differs, the directory differs, and the current pin holds every optional member, so the delta carries a `null` per optional member.
#[test]
fn replaced_row_measure_covers_the_widest_metadata_delta() {
    let before = r#"{"manifest_path":"a/_versions/8.manifest","manifest_size":80,"e_tag":"e8","naming_scheme":"V1"}"#;
    let current = r#"{"manifest_path":"b/_versions/9.manifest","manifest_size":90,"e_tag":"e9","naming_scheme":"V2","table_fork_owner":"owner","staged_version":9,"transaction_uuid":"u9","last_linear_version":1}"#;
    let delta = super::row::metadata_delta(
        &TableVersionMetadata::from_json_str(before).unwrap(),
        &TableVersionMetadata::from_json_str(current).unwrap(),
    )
    .unwrap();
    let members: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&delta).unwrap();
    assert_eq!(
        members.len(),
        8,
        "every member of `TableVersionMetadata` is in the delta: {delta}"
    );
    let (_, measure, stored) = replaced_row_measure_and_stored(
        vec![probe_table(7, measured_pin(9, current))],
        ReplacedRow::Table(Box::new(probe_table(7, measured_pin(8, before)))),
    );
    let widest_replaced_at_digits = u64::MAX.to_string().len() - 1;
    assert!(
        measure >= stored + widest_replaced_at_digits,
        "measured {measure} bytes for a row of {stored} whose object id can grow by \
         {widest_replaced_at_digits}"
    );
    assert!(
        measure <= stored + TABLE_ROW_BYTES,
        "measured {measure} bytes for a row of {stored}"
    );
    assert_eq!(
        measure,
        TABLE_ROW_BYTES + delta.len() - r#""manifest_size":80,"#.len()
            + super::row::MANIFEST_SIZE_MEASURE_BYTES,
        "the measure charges `manifest_size` at its fixed width: {delta}"
    );
}

/// The release measure of a `ReplacedRow::Unregistered` row follows the key the row stores: whole under another or no current row, none under a current row of the same key.
#[test]
fn replaced_row_measure_follows_the_key_of_an_unregistered_row() {
    let metadata = r#"{"manifest_path":"t/_versions/9.manifest","manifest_size":90,"e_tag":"e9","naming_scheme":"V2"}"#;
    let mut registration = probe_table(7, TableState::Registered).registration;
    registration.table_key = format!("node:{}", "x".repeat(4096));
    let renamed = probe_table(7, measured_pin(9, metadata));
    let mut same_key = probe_table(7, measured_pin(9, metadata));
    same_key.registration.table_key = registration.table_key.clone();
    for (current, stores_the_key) in [
        (vec![renamed], true),
        (vec![same_key], false),
        (vec![], true),
    ] {
        let (rows, measure, stored) = replaced_row_measure_and_stored(
            current,
            ReplacedRow::Unregistered(registration.clone()),
        );
        assert!(
            if stores_the_key {
                stored > 4096
            } else {
                stored < TABLE_ROW_BYTES
            },
            "{stored} bytes stored over {} current rows, key stored: {stores_the_key}",
            rows.tables.len()
        );
        assert!(
            measure >= stored,
            "measured {measure} bytes for a row of {stored} over {} current rows",
            rows.tables.len()
        );
        assert!(
            measure <= stored + TABLE_ROW_BYTES,
            "measured {measure} bytes for a row of {stored} over {} current rows",
            rows.tables.len()
        );
    }
}

/// A `settled_commit` row implies its parent (the previous buffered commit) and
/// inherits its contract (identity version 0) from the next newer commit; the
/// genesis commit and a commit whose contract differs store theirs whole.
#[test]
fn settled_commit_rows_imply_their_parent_and_inherit_their_contract() {
    let contract = |version: u32| {
        Some(SchemaContractHead {
            schema_ir_hash: format!("sha256:{version}"),
            schema_identity_version: version,
            schema_identity_domain: "d".to_string(),
        })
    };
    let commit = |id: &str, parent: Option<&str>, version: Option<u32>| {
        let mut commit = probe_commit(id, parent);
        commit.schema_contract = version.and_then(contract);
        commit.schema_content_hash = version.map(|version| format!("content{version}"));
        commit
    };
    let rows = ManifestRows {
        schema_contract_head: None,
        schema_contract: None,
        tables: Vec::new(),
        head: commit("c4", Some("c3"), Some(2)),
        buffer: CommitBuffer {
            commits: vec![
                commit("c1", None, None),
                commit("c2", Some("c1"), Some(1)),
                commit("c3", Some("c2"), Some(1)),
            ],
            replaced: Vec::new(),
        },
    };
    let logical = rows.to_batch().unwrap();
    let strings = |name: &str| {
        logical
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.map(str::to_string))
            .collect::<Vec<_>>()
    };
    let versions: Vec<Option<u64>> = logical
        .column_by_name("schema_identity_version")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(
        strings("object_id"),
        ["c4", "c1", "c2", "c3"].map(|id| Some(id.to_string()))
    );
    assert_eq!(
        strings("parent_commit_id"),
        [Some("c3".to_string()), None, None, None]
    );
    assert_eq!(versions, [Some(2), None, Some(0), Some(1)]);
    assert_eq!(
        strings("schema_content_hash"),
        [
            Some("content2".to_string()),
            None,
            None,
            Some("content1".to_string())
        ]
    );

    let schema = super::record::manifest_storage_schema(HashMap::new());
    let stored = super::record::compact_to_storage(&logical, &schema).unwrap();
    let decoded = super::state::rows_of_batch(&stored).unwrap();
    assert_eq!(decoded.buffer.commits, rows.buffer.commits);
    assert_eq!(decoded.head, rows.head);
}

/// The decoder refuses a buffer that is not the first-parent run ending at
/// the head, and one above the bound.
#[test]
fn manifest_rows_refuse_a_buffer_that_is_not_a_run_behind_the_head() {
    let decode = |buffer: CommitBuffer, head: GraphLineageRow| {
        let rows = ManifestRows {
            schema_contract_head: None,
            schema_contract: None,
            tables: Vec::new(),
            head,
            buffer,
        };
        let schema = super::record::manifest_storage_schema(HashMap::new());
        let stored = super::record::compact_to_storage(&rows.to_batch().unwrap(), &schema).unwrap();
        super::state::rows_of_batch(&stored)
    };

    let (run, head) = buffered_run(TAIL_MAX_COMMITS);
    let decoded = decode(run.clone(), head.clone()).unwrap();
    assert_eq!((decoded.buffer, decoded.head), (run.clone(), head.clone()));

    let mut gap = run.clone();
    gap.commits.remove(3);
    let error = decode(gap, head.clone()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("buffers graph commit 'c3' and the next commit it holds, 'c5'"),
        "{error}"
    );

    let mut headless = run.clone();
    headless.commits.pop();
    let error = decode(headless, head).unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "'c{}' and the next commit it holds, 'c{}'",
            TAIL_MAX_COMMITS - 1,
            TAIL_MAX_COMMITS + 1
        )),
        "{error}"
    );

    let (above, head) = buffered_run(TAIL_MAX_COMMITS + 1);
    let error = decode(above, head).unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("above the bound of {TAIL_MAX_COMMITS}")),
        "{error}"
    );
}

fn replacement_contract() -> SchemaContractRow {
    SchemaContractRow {
        source: "node Person {\n    name: String\n    nickname: String?\n}\n".to_string(),
        ir: "{\n  \"ir_version\": 5,\n  \"nodes\": []\n}\n".to_string(),
        head: SchemaContractHead {
            schema_ir_hash: "sha256:replacement".to_string(),
            schema_identity_version: 2,
            schema_identity_domain: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
        },
    }
}

async fn schema_contract_row_count(dataset: &Dataset) -> usize {
    let mut scanner = dataset.scan();
    scanner.filter_expr(
        datafusion::prelude::col("object_id")
            .eq(datafusion::prelude::lit(SCHEMA_CONTRACT_OBJECT_ID)),
    );
    scanner.count_rows().await.unwrap() as usize
}

/// Genesis writes the one `schema_contract` row in the Create commit: the head is folded into
/// the state and the snapshot, the texts come back byte-exact through both reads.
#[tokio::test]
async fn genesis_writes_the_schema_contract_row_and_reads_it_back() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let expected = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();

    assert_eq!(
        mc.known_state.schema_contract.as_ref(),
        Some(&expected.head)
    );
    assert_eq!(mc.snapshot().schema_contract(), Some(&expected.head));
    assert_eq!(mc.read_schema_contract().await.unwrap(), expected);
    assert_eq!(
        ManifestCoordinator::read_schema_contract_at(uri, None, 1)
            .await
            .unwrap(),
        expected
    );
    let ds = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(schema_contract_row_count(&ds).await, 1);
    let genesis_id = mc.exact_graph_head().unwrap();
    let evidence = read_schema_publication_at(uri, 1, &genesis_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.contract, expected);
    assert_eq!(evidence.commit.graph_commit_id, genesis_id);
    assert_eq!(evidence.commit.graph_manifest_version, 1);
    assert_eq!(
        evidence.branch_identifier,
        lance::dataset::refs::BranchIdentifier::main()
    );
    let encoded = serde_json::to_string(&evidence.commit).unwrap();
    assert_eq!(
        serde_json::from_str::<crate::commit_graph::GraphCommit>(&encoded).unwrap(),
        evidence.commit,
    );
    for (version, id) in [(1, ulid::Ulid::new().to_string()), (2, genesis_id)] {
        assert!(
            read_schema_publication_at(uri, version, &id)
                .await
                .unwrap()
                .is_none()
        );
    }
    let batch = &read_manifest_rows(&ds).await.unwrap().to_batch().unwrap();
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        super::state::manifest_schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        "the publish scan carries every logical column, the content columns included"
    );
}

/// `ManifestChange::SchemaContract` replaces the live row in the same commit as its table rows;
/// a publish without one carries the row forward; the pre-replacement version still answers
/// with the old contract; two replacements in one batch are refused.
#[tokio::test]
async fn publish_replaces_the_schema_contract_row_and_carries_it_forward() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let genesis_contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let genesis_version = mc.version();
    let person_entry = mc.snapshot().dataset("node:Person").unwrap().clone();
    let person_update = append_person_and_make_update(uri, &person_entry, "Ann").await;

    let replacement = replacement_contract();
    let intent = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("schema-author".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let parent = mc.exact_graph_head();
    let replaced_at = mc
        .commit_changes_with_lineage_and_precondition(
            &[
                ManifestChange::Update(person_update),
                ManifestChange::SchemaContract(replacement.clone()),
            ],
            &ExpectedTableVersions::new(),
            Some(&intent),
            &PublishPrecondition::Any,
        )
        .await
        .unwrap()
        .version;
    assert_eq!(
        mc.known_state.schema_contract.as_ref(),
        Some(&replacement.head),
        "the publish fold reflects the replaced row without a re-scan"
    );
    assert_eq!(mc.read_schema_contract().await.unwrap(), replacement);
    assert_eq!(
        mc.snapshot().dataset("node:Person").unwrap().entity_count,
        1,
        "the table row and the contract land in one commit"
    );

    let carried = append_person_and_make_update(uri, &person_entry, "Bob").await;
    let carried_at = mc
        .commit_changes(&[ManifestChange::Update(carried)])
        .await
        .unwrap();
    assert!(carried_at > replaced_at);
    assert_eq!(
        mc.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );
    assert_eq!(mc.read_schema_contract().await.unwrap(), replacement);
    let ds = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(schema_contract_row_count(&ds).await, 1);
    let evidence = read_schema_publication_at(uri, replaced_at, &intent.graph_commit_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.contract, replacement);
    assert_eq!(evidence.commit.parent_commit_id, parent);
    assert_eq!(evidence.commit.actor_id, intent.actor_id);
    assert_eq!(evidence.commit.created_at, intent.created_at);
    assert_eq!(evidence.commit.graph_manifest_version, replaced_at);
    assert!(
        read_schema_publication_at(uri, carried_at, &intent.graph_commit_id)
            .await
            .unwrap()
            .is_none(),
        "a carried commit and matching contract are not evidence at the commit's own version",
    );

    assert_eq!(
        ManifestCoordinator::read_schema_contract_at(uri, None, genesis_version)
            .await
            .unwrap(),
        genesis_contract,
        "time travel below the replacement reads the contract of that version"
    );

    let old_record = mc
        .buffer
        .records(&mc.head.tables)
        .into_iter()
        .find(|record| record.commit.graph_manifest_version == genesis_version)
        .unwrap();
    let historical = old_record.snapshot(uri).unwrap();
    assert_eq!(historical.schema_contract(), Some(&genesis_contract.head));
    assert_eq!(
        ManifestCoordinator::read_schema_contract_for_snapshot(uri, &historical)
            .await
            .unwrap(),
        genesis_contract
    );

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(
        reopened.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );

    let twice = mc
        .commit_changes(&[
            ManifestChange::SchemaContract(replacement.clone()),
            ManifestChange::SchemaContract(genesis_contract.clone()),
        ])
        .await
        .expect_err("two contract replacements in one batch must be refused")
        .to_string();
    assert!(twice.contains("replaced twice"), "{twice}");
    assert_eq!(mc.version(), carried_at, "a refused batch advances nothing");

    let published = ds.checkout_version(replaced_at).await.unwrap();
    published
        .object_store(None)
        .await
        .unwrap()
        .delete(&published.manifest_location().path)
        .await
        .unwrap();
    assert!(
        read_schema_publication_at(uri, replaced_at, &intent.graph_commit_id)
            .await
            .unwrap()
            .is_none(),
        "pruned exact evidence stays unknown despite a later retained lineage row",
    );
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PublishIo {
    metadata_gets: usize,
    lists: usize,
    reads: u64,
    writes: u64,
}

fn fresh_publish_probes() -> crate::instrumentation::QueryIoProbes {
    crate::instrumentation::QueryIoProbes {
        manifest_wrapper: Some(Arc::new(SmallScanProbeMarker)),
        history_wrapper: Some(Arc::new(SmallScanProbeMarker)),
        ..Default::default()
    }
}

fn drain_publish_io(stores: &crate::instrumentation::ProbedStores) -> PublishIo {
    let mut total = PublishIo::default();
    for store in stores.stores() {
        let stats = store.io_stats_incremental();
        total.reads += stats.read_iops;
        total.writes += stats.write_iops;
        for request in stats.requests {
            total.metadata_gets += usize::from(
                request.method.starts_with("get") && request.path.as_ref().ends_with(".manifest"),
            );
            total.lists += usize::from(request.method.starts_with("list"));
        }
    }
    total
}

#[tokio::test]
async fn acknowledged_publisher_reuses_metadata_in_each_fresh_probe() {
    for file_url in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = if file_url {
            format!("file://{}", dir.path().display())
        } else {
            dir.path().to_str().unwrap().to_string()
        };
        ManifestCoordinator::init(&uri, &build_test_catalog())
            .await
            .unwrap();
        let publisher = GraphNamespacePublisher::new(&uri, None);
        publisher
            .publish(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
            .await
            .unwrap();
        let mut previous = Vec::new();
        for round in 0..3 {
            let probes = fresh_publish_probes();
            let intent = lineage_intent(None, None);
            let outcome = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                publisher.publish(&[], &HashMap::new(), Some(&intent)),
            )
            .await
            .unwrap();
            assert_eq!(outcome.head.graph_commit_id, intent.graph_commit_id);
            for old in &previous {
                assert_eq!(
                    drain_publish_io(old),
                    PublishIo::default(),
                    "round {round}: a previous query probe received this publish's IO"
                );
            }
            let io = drain_publish_io(&probes.manifest_stores);
            assert!(io.writes >= 3, "file_url={file_url}, round={round}: {io:?}");
            assert_eq!(
                io.metadata_gets, 0,
                "file_url={file_url}, round={round}: an acknowledged image needs no metadata GET: {io:?}"
            );
            previous.push(probes.manifest_stores);
        }
    }
}

#[tokio::test]
async fn history_settlement_uses_current_probes_and_preserves_foreign_archives() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let coordinator = ManifestCoordinator::init(&uri, &build_test_catalog())
        .await
        .unwrap();
    let publisher = GraphNamespacePublisher::new(&uri, None);
    let mut published = vec![coordinator.head().graph_commit_id.clone()];
    let mut old_history_stores = Vec::new();
    let mut released = 0;
    let mut round = 0;
    while old_history_stores.len() < 2 {
        round += 1;
        let rows = main_rows(&uri).await;
        let releases = rows.buffer.is_full(&rows.head, &rows.tables).unwrap();
        if releases {
            released += rows.buffer.commits().len();
        }
        let intent = fat_intent(None, None);
        let probes = fresh_publish_probes();
        crate::instrumentation::with_query_io_probes(
            probes.clone(),
            publisher.publish(&[], &HashMap::new(), Some(&intent)),
        )
        .await
        .unwrap();
        published.push(intent.graph_commit_id);
        for old in &old_history_stores {
            assert_eq!(drain_publish_io(old), PublishIo::default());
        }
        let io = drain_publish_io(&probes.history_stores);
        if releases && old_history_stores.is_empty() {
            assert!(io.writes >= 3, "first settlement must be counted: {io:?}");
            old_history_stores.push(probes.history_stores);
        } else if releases {
            assert!(io.writes >= 3, "{io:?}");
            assert_eq!(
                io.metadata_gets, 0,
                "settlement needs no history metadata image: {io:?}"
            );
            old_history_stores.push(probes.history_stores);
        } else {
            assert_eq!(io, PublishIo::default(), "round {round} does not settle");
        }
    }

    let foreign = HistoryRecord {
        commit: GraphLineageRow {
            schema_contract: coordinator.head().schema_contract.clone(),
            schema_content_hash: coordinator.head().schema_content_hash.clone(),
            ..probe_commit("foreign-history-append", None)
        },
        tables: Vec::new(),
    };
    super::history::settle(
        &uri,
        &crate::lance_access::control_session(),
        std::slice::from_ref(&foreign),
    )
    .await
    .unwrap();
    loop {
        let rows = main_rows(&uri).await;
        let releases = rows.buffer.is_full(&rows.head, &rows.tables).unwrap();
        let intent = fat_intent(None, None);
        let probes = fresh_publish_probes();
        crate::instrumentation::with_query_io_probes(
            probes.clone(),
            publisher.publish(&[], &HashMap::new(), Some(&intent)),
        )
        .await
        .unwrap();
        let io = drain_publish_io(&probes.history_stores);
        published.push(intent.graph_commit_id);
        if releases {
            released += rows.buffer.commits().len();
            assert!(
                io.metadata_gets == 0 && io.lists == 0 && io.writes >= 1,
                "a foreign archive does not require reopening shared history metadata: {io:?}"
            );
            break;
        }
        assert_eq!(io, PublishIo::default());
    }
    for old in &old_history_stores {
        assert_eq!(drain_publish_io(old), PublishIo::default());
    }
    let settled = settled_commits(&uri).await;
    assert_eq!(settled.len(), released + 1);
    assert_eq!(
        settled.get(&foreign.commit.graph_commit_id),
        Some(&foreign.commit)
    );
    assert!(
        published[..released]
            .iter()
            .all(|id| settled.contains_key(id))
    );
}

#[tokio::test]
async fn acknowledged_publisher_rejects_equal_version_reincarnation() {
    for cached_session in [false, true] {
        let session = if cached_session {
            Arc::new(lance::session::Session::default())
        } else {
            crate::lance_access::control_session()
        };
        assert_publisher_rejects_equal_version_reincarnation(session, cached_session).await;
    }
}

async fn assert_publisher_rejects_equal_version_reincarnation(
    session: Arc<lance::session::Session>,
    cached_session: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("graph");
    let uri = root.to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let publisher = GraphNamespacePublisher::new_with_session(uri, None, session.clone());
    let old = publisher
        .publish(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
        .await
        .unwrap();
    let primed = super::layout::open_manifest_dataset_with_session(uri, None, &session)
        .await
        .unwrap();
    assert_eq!(primed.version().version, old.dataset.version().version);
    assert!(Arc::ptr_eq(&primed.session(), &session));
    if cached_session {
        assert!(session.metadata_cache_stats().await.num_entries > 0);
    }
    let expected = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        None,
        lance::dataset::refs::BranchIdentifier::main(),
        Some(old.head.graph_commit_id.clone()),
    ));
    std::fs::remove_dir_all(&root).unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let contract = replacement_contract();
    let replacement = GraphNamespacePublisher::new(uri, None)
        .publish(
            &[ManifestChange::SchemaContract(contract.clone())],
            &HashMap::new(),
            Some(&lineage_intent(None, None)),
        )
        .await
        .unwrap();
    assert_eq!(
        old.dataset.version().version,
        replacement.dataset.version().version
    );
    assert_ne!(
        old.dataset.manifest_location().e_tag,
        replacement.dataset.manifest_location().e_tag
    );
    let error = publisher
        .publish_with_precondition(
            &[],
            &HashMap::new(),
            Some(&lineage_intent(None, None)),
            &expected,
        )
        .await
        .expect_err("a cached image must not authorize publication in a recreated root");
    assert!(matches!(
        error,
        OmniError::Manifest(ManifestError {
            details: Some(ManifestConflictDetails::ReadSetChanged { .. }),
            ..
        })
    ));
    let current = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(
        current.version().version,
        replacement.dataset.version().version
    );
    assert_eq!(
        read_manifest_rows(&current).await.unwrap().head,
        replacement.head
    );
    let next = publisher
        .publish(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
        .await
        .unwrap();
    assert_eq!(
        next.parent_commit_id.as_deref(),
        Some(replacement.head.graph_commit_id.as_str())
    );
    assert_eq!(
        next.known_state.schema_contract.as_ref(),
        Some(&contract.head)
    );
    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.read_schema_contract().await.unwrap(), contract);
}

#[tokio::test]
async fn publisher_reuses_only_its_exact_bounded_published_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let coordinator = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let publisher = GraphNamespacePublisher::new(uri, None);
    let mut contract = coordinator.read_schema_contract().await.unwrap();
    for (round, expected_scans) in [1, 0, 1, 0, 1, 1].into_iter().enumerate() {
        if round == 2 {
            contract.source = format!("\n{}\n", contract.source);
            contract.ir = format!("\n{}\n", contract.ir);
            let foreign = GraphNamespacePublisher::new(uri, None);
            foreign
                .publish(
                    &[ManifestChange::SchemaContract(contract.clone())],
                    &HashMap::new(),
                    None,
                )
                .await
                .unwrap();
        }
        if round == 4 {
            contract.source = format!(
                "{}{}",
                " ".repeat(super::publisher::PUBLISHED_ROWS_CACHE_BYTES + 1024),
                contract.source
            );
            publisher
                .publish(
                    &[ManifestChange::SchemaContract(contract.clone())],
                    &HashMap::new(),
                    None,
                )
                .await
                .unwrap();
        }
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            branch: None,
            actor_id: None,
            merged_parent: None,
            created_at: 0,
            history_release_bytes: HistoryReleaseBytes::PRODUCTION,
        };
        let probes = fresh_publish_probes();
        let scans = probes.manifest_scan_count.clone();
        let stores = probes.manifest_stores.clone();
        let outcome = crate::instrumentation::with_query_io_probes(
            probes,
            publisher.publish(&[], &HashMap::new(), Some(&intent)),
        )
        .await
        .unwrap();
        assert_eq!(
            scans.load(std::sync::atomic::Ordering::Relaxed),
            expected_scans,
            "round {round}: cold, warm, foreign replacement, warm, oversized, oversized"
        );
        let io = drain_publish_io(&stores);
        assert_eq!(
            io.metadata_gets == 0,
            expected_scans == 0,
            "round {round}: only an exact bounded acknowledged image skips metadata reads: {io:?}"
        );
        let rows = read_manifest_rows(&outcome.dataset).await.unwrap();
        assert_eq!(rows.buffer.commits().len(), round + 1);
        assert_eq!(rows.head, outcome.head);
        assert_intent_nonce(&rows.head.graph_commit_id, &intent);
        let reopened = ManifestCoordinator::open(uri).await.unwrap();
        assert_eq!(reopened.read_schema_contract().await.unwrap(), contract);
        assert_eq!(schema_contract_row_count(&outcome.dataset).await, 1);
    }
}

/// The content read checks the row against the head the caller's state
/// folded from the same version; a disagreement is an error, never a silent
/// pick of one side.
#[tokio::test]
async fn schema_contract_read_refuses_a_row_that_disagrees_with_the_fold() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    mc.known_state.schema_contract = Some(replacement_contract().head);
    let error = mc
        .read_schema_contract()
        .await
        .expect_err("a folded head that differs from the row must be refused")
        .to_string();
    assert!(error.contains("disagrees with the folded state"), "{error}");
}

/// A live-read refresh after another handle's schema apply observes the
/// replaced contract, and the refresh's projection folds it.
#[tokio::test]
async fn refresh_observes_a_schema_contract_replaced_by_another_handle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let genesis_contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let mut reader = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(
        reader.known_state.schema_contract.as_ref(),
        Some(&genesis_contract.head)
    );

    let mut writer = ManifestCoordinator::open(uri).await.unwrap();
    let replacement = replacement_contract();
    writer
        .commit_changes(&[ManifestChange::SchemaContract(replacement.clone())])
        .await
        .unwrap();

    reader.refresh().await.unwrap();
    assert_eq!(
        reader.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );
    assert_eq!(reader.read_schema_contract().await.unwrap(), replacement);
    let captured = reader.snapshot();
    reader.refresh().await.unwrap();
    assert!(reader.snapshot().same_manifest_image(&captured));
    assert_eq!(
        reader.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );

    // A retained main handle must not equate a manifest version with a root
    // lifetime: main's native branch identity is fixed. This is the refresh
    // boundary prepared schema no-op admission and reconciliation rely on.
    reader.refresh().await.unwrap();
    let replacement_dir = tempfile::tempdir().unwrap();
    let replacement_catalog = build_same_name_node_edge_catalog();
    let replacement_contract = SchemaContractRow::for_test_catalog(&replacement_catalog).unwrap();
    let mut other = ManifestCoordinator::init(
        replacement_dir.path().to_str().unwrap(),
        &replacement_catalog,
    )
    .await
    .unwrap();
    while other.version() < reader.version() {
        other
            .commit_changes(&[ManifestChange::SchemaContract(replacement_contract.clone())])
            .await
            .unwrap();
    }
    assert_eq!(other.version(), reader.version());
    assert_ne!(reader.exact_graph_head(), other.exact_graph_head());
    std::fs::remove_dir_all(dir.path()).unwrap();
    std::fs::rename(replacement_dir.path(), dir.path()).unwrap();
    reader.refresh().await.unwrap();
    assert_eq!(
        reader.read_schema_contract().await.unwrap(),
        replacement_contract
    );
    assert_eq!(reader.exact_graph_head(), other.exact_graph_head());
}

#[tokio::test]
async fn cold_contract_capture_scans_once_and_refuses_reserved_id_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut writer = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let mut expected = writer.read_schema_contract().await.unwrap();
    expected.source = format!("\n{}\n", expected.source);
    expected.ir = format!("\n{}\n", expected.ir);
    writer
        .commit_changes(&[ManifestChange::SchemaContract(expected.clone())])
        .await
        .unwrap();
    let session = crate::lance_access::control_session();
    let probes = crate::instrumentation::QueryIoProbes::default();
    let scans = probes.manifest_scan_count.clone();
    let (reader, lineage, contract) = crate::instrumentation::with_query_io_probes(
        probes,
        ManifestCoordinator::open_with_lineage_and_contract(uri, None, &session),
    )
    .await
    .unwrap();
    assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(contract.unwrap(), expected);
    assert_eq!(reader.snapshot().schema_contract(), Some(&expected.head));
    assert_eq!(reader.version(), writer.version());
    let dataset = open_manifest_dataset(uri, None).await.unwrap();
    assert!(lineage.is_empty(), "cold admission leaves lineage lazy");
    assert_eq!(
        reader.head().graph_commit_id,
        read_manifest_rows(&dataset)
            .await
            .unwrap()
            .head
            .graph_commit_id
    );

    let rows = vec![
        read_manifest_rows(&dataset)
            .await
            .unwrap()
            .to_batch()
            .unwrap(),
    ];
    let alias = relabelled_manifest_row(&rows, SCHEMA_CONTRACT_OBJECT_ID);
    let mut columns = alias.columns().to_vec();
    columns[alias.schema().index_of("object_type").unwrap()] =
        Arc::new(StringArray::from(vec!["unknown_extension"]));
    let alias = RecordBatch::try_new(alias.schema(), columns).unwrap();
    let schema = super::record::manifest_storage_schema(dataset.schema().metadata.clone());
    let stored = super::record::compact_to_storage(&alias, &schema).unwrap();
    InsertBuilder::new(Arc::new(dataset))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            skip_auto_cleanup: true,
            ..Default::default()
        })
        .execute(vec![stored])
        .await
        .unwrap();
    let error = ManifestCoordinator::open_with_lineage_and_contract(uri, None, &session)
        .await
        .err()
        .expect("reserved schema-contract id must prevent catalog construction");
    let OmniError::Manifest(error) = error else {
        panic!("expected a manifest integrity error, got: {error}");
    };
    assert_eq!(error.kind, crate::error::ManifestErrorKind::Internal);
    assert_eq!(
        error.message,
        "manifest row 'schema_contract' has object_type 'unknown_extension'"
    );
    let error = read_schema_publication_at(
        uri,
        writer.version() + 1,
        &writer.exact_graph_head().unwrap(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("manifest row 'schema_contract' has object_type 'unknown_extension'"),
        "{error}"
    );
}

#[tokio::test]
async fn prepared_contract_capture_retries_only_replaced_unreadable_data() {
    for replace in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let writer = ManifestCoordinator::init(root, &build_test_catalog())
            .await
            .unwrap();
        let expected = writer.read_schema_contract().await.unwrap();
        let session = crate::lance_access::control_session();
        let prepared = ManifestCoordinator::prepare_open_with_contract(root, &session)
            .await
            .unwrap();
        let dataset = open_manifest_dataset(root, None).await.unwrap();
        let old_version = dataset.version().version;
        let files = dataset
            .get_fragments()
            .iter()
            .flat_map(|fragment| {
                fragment
                    .metadata()
                    .files
                    .iter()
                    .map(|file| file.path.clone())
            })
            .collect::<Vec<_>>();
        if replace {
            let rows = read_manifest_rows(&dataset)
                .await
                .unwrap()
                .to_batch()
                .unwrap();
            let schema = super::record::manifest_storage_schema(dataset.schema().metadata.clone());
            let stored = vec![super::record::compact_to_storage(&rows, &schema).unwrap()];
            let replacement = InsertBuilder::new(Arc::new(dataset))
                .with_params(&WriteParams {
                    mode: WriteMode::Overwrite,
                    skip_auto_cleanup: true,
                    ..Default::default()
                })
                .execute(stored)
                .await
                .unwrap();
            assert!(replacement.version().version > old_version);
            assert!(replacement.get_fragments().iter().all(|fragment| {
                fragment
                    .metadata()
                    .files
                    .iter()
                    .all(|file| !files.contains(&file.path))
            }));
        }
        for file in files {
            std::fs::remove_file(dir.path().join("__manifest/data").join(file)).unwrap();
        }
        let probes = crate::instrumentation::QueryIoProbes::default();
        let opens = probes.internal_open_count.clone();
        let scans = probes.manifest_scan_count.clone();
        let result = crate::instrumentation::with_query_io_probes(
            probes,
            ManifestCoordinator::open_prepared_with_lineage_and_contract(root, prepared),
        )
        .await;
        assert_eq!(opens.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(
            scans.load(std::sync::atomic::Ordering::Relaxed),
            if replace { 2 } else { 1 }
        );
        if replace {
            let (reader, _, contract) = result.unwrap();
            assert!(reader.version() > old_version);
            assert_eq!(contract.unwrap(), expected);
        } else {
            assert!(
                result.is_err(),
                "unchanged unreadable data must remain an error"
            );
        }
    }
}

#[tokio::test]
async fn prepared_contract_capture_rejects_another_root_before_scanning() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    ManifestCoordinator::init(root, &build_test_catalog())
        .await
        .unwrap();
    let prepared = ManifestCoordinator::prepare_open_with_contract(
        root,
        &crate::lance_access::control_session(),
    )
    .await
    .unwrap();
    let probes = crate::instrumentation::QueryIoProbes::default();
    let scans = probes.manifest_scan_count.clone();
    let result = crate::instrumentation::with_query_io_probes(
        probes,
        ManifestCoordinator::open_prepared_with_lineage_and_contract("/another-root", prepared),
    )
    .await;
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("different graph root")
    );
    assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 0);
}

/// Routine state scans leave contract content unread; cold admission projects it once.
#[tokio::test]
async fn state_scan_projection_leaves_the_content_columns_unread() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let ds = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(
        super::record::packed_projection(&ds, false),
        ["object_id", "object_type", "record"]
    );
    assert_eq!(
        super::record::packed_projection(&ds, true),
        [
            "object_id",
            "object_type",
            "record",
            "schema_source",
            "schema_ir"
        ]
    );
    let state = super::state::read_manifest_state(&ds).await.unwrap();
    assert!(state.schema_contract.is_some());
    let projected = super::state::read_manifest_rows_projected(&ds)
        .await
        .unwrap();
    assert!(projected.schema_contract.is_none());
    assert_eq!(projected.schema_contract_head, state.schema_contract);
    assert!(
        projected.to_batch().is_err(),
        "projected rows cannot overwrite the complete contract"
    );
}

#[derive(Debug)]
pub(super) struct SmallScanProbeMarker;

impl lance::io::WrappingObjectStore for SmallScanProbeMarker {
    fn wrap(
        &self,
        _: &str,
        original: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        original
    }
}

/// Counts the requests `__history` is sent by verb, "not found" GETs included
/// (Lance's tracker records only successful GETs); with `refuse_suffix` a
/// suffix range is refused before it is sent, as `object_store`'s Azure client does.
#[derive(Debug, Default, Clone)]
pub(super) struct HistoryGets {
    refuse_suffix: bool,
    sent: Arc<std::sync::atomic::AtomicUsize>,
    absent: Arc<std::sync::atomic::AtomicUsize>,
    heads: Arc<std::sync::atomic::AtomicUsize>,
}

impl HistoryGets {
    /// Probes whose `__history` requests pass through this counter.
    pub(super) fn probes(&self) -> crate::instrumentation::QueryIoProbes {
        crate::instrumentation::QueryIoProbes {
            manifest_wrapper: Some(Arc::new(SmallScanProbeMarker)),
            history_wrapper: Some(Arc::new(self.clone())),
            ..Default::default()
        }
    }

    /// The `(GET, GET answered "not found", HEAD)` requests since the last call.
    pub(super) fn drain(&self) -> (usize, usize, usize) {
        let take = |count: &std::sync::atomic::AtomicUsize| {
            count.swap(0, std::sync::atomic::Ordering::Relaxed)
        };
        (take(&self.sent), take(&self.absent), take(&self.heads))
    }
}

impl lance::io::WrappingObjectStore for HistoryGets {
    fn wrap(
        &self,
        _: &str,
        original: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(HistoryGetStore {
            original,
            gets: self.clone(),
        })
    }
}

#[derive(Debug)]
struct HistoryGetStore {
    original: Arc<dyn object_store::ObjectStore>,
    gets: HistoryGets,
}

impl std::fmt::Display for HistoryGetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "history request counter over {}", self.original)
    }
}

#[async_trait]
impl object_store::ObjectStore for HistoryGetStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.original.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.original.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.gets.refuse_suffix
            && matches!(options.range, Some(object_store::GetRange::Suffix(_)))
        {
            return Err(object_store::Error::NotSupported {
                source: "this store does not support suffix range requests".into(),
            });
        }
        let sent = match options.head {
            true => &self.gets.heads,
            false => &self.gets.sent,
        };
        sent.fetch_add(1, Relaxed);
        let result = self.original.get_opts(location, options).await;
        if matches!(result, Err(object_store::Error::NotFound { .. })) {
            self.gets.absent.fetch_add(1, Relaxed);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.original.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.original.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.original.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.original.copy_opts(from, to, options).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SmallScanIo {
    requests: u64,
    bytes: u64,
    writes: u64,
}

fn drain_small_scan_io(stores: &crate::instrumentation::ProbedStores) -> SmallScanIo {
    let mut total = SmallScanIo {
        requests: 0,
        bytes: 0,
        writes: 0,
    };
    for store in stores.stores() {
        let stats = store.io_stats_incremental();
        total.requests += stats.read_iops;
        total.bytes += stats.read_bytes;
        total.writes += stats.write_iops;
    }
    total
}

/// At least `bytes` of comment lines of random hex, which compress by less than three.
fn schema_comment_noise(bytes: usize) -> String {
    let mut random = 0xa076_1d64_78bd_642fu64;
    let mut text = String::new();
    while text.len() < bytes {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        use std::fmt::Write;
        writeln!(&mut text, "// {random:016x}").unwrap();
    }
    text
}

/// The inclusive byte budget of a manifest scan, as the store the scan of an
/// eligible dataset rebinds reports it: the scan's block size is the budget.
async fn manifest_scan_budget() -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("__manifest");
    let dataset = small_read_fixture(uri.to_str().unwrap(), 1).await;
    let original = dataset.object_store(None).await.unwrap();
    let scan = crate::instrumentation::manifest_scan_dataset(&dataset)
        .await
        .unwrap();
    let budget = scan.object_store(None).await.unwrap().block_size();
    assert!(
        budget > original.block_size(),
        "a one-fragment fixture of four rows is within the scan budget"
    );
    budget as u64
}

/// A manifest whose schema contract is under the scan budget, or over it when
/// `large`, with the contract, the encoded bytes of its data files, and the budget.
async fn small_scan_catalog_fixture(root: &str, large: bool) -> (SchemaContractRow, u64, u64) {
    let budget = manifest_scan_budget().await;
    let catalog = build_test_catalog();
    let mut contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    contract.source = format!("{}\n", test_schema_source());
    let noise = if large { 3 * budget as usize } else { 5_120 };
    contract.source.push_str(&schema_comment_noise(noise));
    let attempt = GenesisManifestAttempt::mint(catalog.system_columns).unwrap();
    let dataset = ManifestCoordinator::init_commit(
        root,
        &catalog,
        &contract,
        &crate::lance_access::control_session(),
        &attempt,
    )
    .await
    .unwrap();
    assert!(
        dataset
            .manifest()
            .fragments
            .iter()
            .all(|fragment| fragment.deletion_file.is_none() && fragment.overlays.is_empty())
    );
    let total = dataset
        .manifest()
        .fragments
        .iter()
        .flat_map(|f| &f.files)
        .map(|file| file.file_size_bytes.get().unwrap().get())
        .sum::<u64>();
    if large {
        assert!(
            total > budget,
            "fixture did not cross fallback boundary: {total}"
        );
    } else {
        assert!(
            (4097..=budget).contains(&total),
            "fixture missed small-read window: {total}"
        );
    }
    (contract, total, budget)
}

async fn measured_catalog_scan(
    root: &str,
    expected: &SchemaContractRow,
    optimized: bool,
) -> (SmallScanIo, Vec<usize>) {
    use std::sync::atomic::Ordering;
    let probes = crate::instrumentation::QueryIoProbes {
        manifest_wrapper: Some(Arc::new(SmallScanProbeMarker)),
        ..Default::default()
    };
    let stores = probes.manifest_stores.clone();
    let opens = probes.internal_open_count.clone();
    let scans = probes.manifest_scan_count.clone();
    crate::instrumentation::with_query_io_probes(probes, async {
        let session = crate::lance_access::control_session();
        let dataset = super::layout::open_manifest_dataset_with_session(root, None, &session)
            .await
            .unwrap();
        assert_eq!(dataset.object_store(None).await.unwrap().block_size(), 4096);
        let original = dataset.object_store(None).await.unwrap();
        let _ = drain_small_scan_io(&stores);
        opens.store(0, Ordering::Relaxed);
        scans.store(0, Ordering::Relaxed);

        let rows = if optimized {
            let (rows, contract) = super::state::read_manifest_rows_with_contract(&dataset)
                .await
                .unwrap();
            assert_eq!(contract.unwrap(), *expected);
            rows
        } else {
            read_manifest_rows(&dataset).await.unwrap()
        };
        assert_eq!(rows.schema_contract.as_ref(), Some(expected));
        assert_eq!(rows.schema_contract_head.as_ref(), Some(&expected.head));
        assert!(!rows.head.graph_commit_id.is_empty());
        assert_eq!(
            opens.load(Ordering::Relaxed),
            0,
            "scan helper must not reopen __manifest"
        );
        assert_eq!(scans.load(Ordering::Relaxed), 1);
        assert!(Arc::ptr_eq(
            &original,
            &dataset.object_store(None).await.unwrap()
        ));
        assert_eq!(
            original.block_size(),
            4096,
            "the held control dataset keeps its policy"
        );
        let io = drain_small_scan_io(&stores);
        let mut blocks = stores
            .stores()
            .iter()
            .map(|store| store.block_size())
            .collect::<Vec<_>>();
        blocks.sort_unstable();
        blocks.dedup();
        (io, blocks)
    })
    .await
}

#[tokio::test]
async fn cold_catalog_small_scan_reduces_real_reads_and_large_scan_is_unchanged() {
    for large in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let (expected, encoded_bytes, budget) = small_scan_catalog_fixture(root, large).await;
        let (baseline, baseline_blocks) = measured_catalog_scan(root, &expected, false).await;
        let (actual, blocks) = measured_catalog_scan(root, &expected, true).await;
        let (again, again_blocks) = measured_catalog_scan(root, &expected, true).await;
        assert_eq!(baseline_blocks, [4096]);
        assert_eq!(baseline.writes, 0);
        assert_eq!(actual.writes, 0);
        assert!(
            actual.requests > 0,
            "rebound store IO must remain in probe totals"
        );
        assert_eq!(
            actual, again,
            "a new cold scan must read again, with no persistent byte reuse"
        );
        assert_eq!(blocks, again_blocks);
        if large {
            assert_eq!(
                blocks,
                [4096],
                "large file must retain the original tail/gap policy"
            );
            assert_eq!(
                actual, baseline,
                "large fallback must preserve physical requests AND bytes"
            );
        } else {
            assert_eq!(
                blocks,
                [4096, budget as usize],
                "both original and rebound stores must be accounted"
            );
            assert!(
                actual.requests < baseline.requests,
                "small physical reads did not fall: original={baseline:?}, optimized={actual:?}"
            );
            assert_eq!(
                actual.bytes, encoded_bytes,
                "eligible encoded data should be fetched once, without overlapping ranges"
            );
        }
    }
}

#[tokio::test]
async fn cold_catalog_small_scan_failure_does_not_poison_a_fresh_retry() {
    use object_store::ObjectStoreExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let (expected, _, _) = small_scan_catalog_fixture(root, false).await;
    let probes = crate::instrumentation::QueryIoProbes {
        manifest_wrapper: Some(Arc::new(SmallScanProbeMarker)),
        ..Default::default()
    };
    let stores = probes.manifest_stores.clone();
    crate::instrumentation::with_query_io_probes(probes, async {
        let dataset = super::layout::open_manifest_dataset_with_session(
            root,
            None,
            &crate::lance_access::control_session(),
        )
        .await
        .unwrap();
        let files = dataset
            .manifest()
            .fragments
            .iter()
            .flat_map(|f| &f.files)
            .collect::<Vec<_>>();
        assert_eq!(
            files.len(),
            1,
            "fault fixture should have exactly one physical data file"
        );
        let file_path = object_store::path::Path::from_filesystem_path(
            dir.path()
                .join("__manifest")
                .join("data")
                .join(&files[0].path),
        )
        .unwrap();
        let store = dataset.object_store(None).await.unwrap();
        let saved = store
            .inner
            .get(&file_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .inner
            .put(&file_path, saved.slice(..saved.len() - 1).into())
            .await
            .unwrap();
        let _ = drain_small_scan_io(&stores);
        let failure = super::state::read_manifest_rows_with_contract(&dataset).await;
        assert!(failure.is_err(), "captured-size mismatch must fail closed");
        let failed_io = drain_small_scan_io(&stores);
        assert!(
            failed_io.requests > 0,
            "failure must reach the tracked physical backend"
        );
        store.inner.put(&file_path, saved.into()).await.unwrap();
        let _ = drain_small_scan_io(&stores);
        let (_, contract) = super::state::read_manifest_rows_with_contract(&dataset)
            .await
            .unwrap();
        assert_eq!(contract.unwrap(), expected);
        let retry_io = drain_small_scan_io(&stores);
        assert!(
            retry_io.requests > 0,
            "the retry must issue a fresh physical read"
        );
        assert_eq!(retry_io.writes, 0);
        assert_eq!(dataset.object_store(None).await.unwrap().block_size(), 4096);
    })
    .await;
}

async fn small_read_fixture(uri: &str, fragments: usize) -> Dataset {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator};
    use lance::dataset::{WriteMode, WriteParams};
    for fragment in 0..fragments {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri,
            Some(WriteParams {
                mode: if fragment == 0 {
                    WriteMode::Create
                } else {
                    WriteMode::Append
                },
                skip_auto_cleanup: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    }
    lance::dataset::builder::DatasetBuilder::from_uri(uri)
        .with_store_params(Default::default())
        .with_session(Arc::new(lance::session::Session::new(
            0,
            0,
            Arc::new(lance::io::ObjectStoreRegistry::default()),
        )))
        .load()
        .await
        .unwrap()
}

#[tokio::test]
async fn manifest_scan_read_budget_is_inclusive_and_covers_all_files() {
    let budget = manifest_scan_budget().await;
    let half = budget / 2;
    for (sizes, eligible) in [
        (vec![budget], true),
        (vec![budget + 1], false),
        (vec![half, budget - half], true),
        (vec![half, budget - half + 1], false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("__manifest");
        let dataset = small_read_fixture(uri.to_str().unwrap(), sizes.len()).await;
        let files = dataset
            .manifest()
            .fragments
            .iter()
            .flat_map(|fragment| &fragment.files)
            .collect::<Vec<_>>();
        assert_eq!(
            files.len(),
            sizes.len(),
            "fixture must have distinct physical files"
        );
        for (file, size) in files.iter().zip(&sizes) {
            file.file_size_bytes
                .set(std::num::NonZeroU64::new(*size).unwrap());
        }
        let original = dataset.object_store(None).await.unwrap();
        assert_eq!(original.block_size(), 4096);
        let _ = original.io_stats_incremental();
        let scan = crate::instrumentation::manifest_scan_dataset(&dataset)
            .await
            .unwrap();
        let scan_store = scan.object_store(None).await.unwrap();
        assert_eq!(
            Arc::ptr_eq(&original, &scan_store),
            !eligible,
            "sizes={sizes:?}"
        );
        assert_eq!(
            scan_store.block_size() as u64,
            if eligible { budget } else { 4096 }
        );
        assert_eq!(dataset.object_store(None).await.unwrap().block_size(), 4096);
        assert_eq!(scan.version().version, dataset.version().version);
        assert_eq!(original.io_stats_incremental().read_iops, 0);
        if eligible {
            assert_eq!(
                scan_store.io_stats_incremental().read_iops,
                0,
                "store rebinding must not reopen a dataset or read a file"
            );
        }
    }
}

#[tokio::test]
async fn manifest_scan_preserves_existing_large_block_and_real_deletion_layout() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("__manifest");
    let uri = uri.to_str().unwrap();
    let mut dataset = small_read_fixture(uri, 1).await;
    let budget = manifest_scan_budget().await as usize;
    for block_size in [budget, 2 * budget] {
        let configured = lance::dataset::builder::DatasetBuilder::from_uri(uri)
            .with_store_params(lance::io::ObjectStoreParams {
                block_size: Some(block_size),
                ..Default::default()
            })
            .with_session(crate::lance_access::control_session())
            .load()
            .await
            .unwrap();
        let before = configured.object_store(None).await.unwrap();
        let after = crate::instrumentation::manifest_scan_dataset(&configured)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &before,
            &after.object_store(None).await.unwrap()
        ));
        assert_eq!(
            after.object_store(None).await.unwrap().block_size(),
            block_size
        );
    }

    dataset.delete("value = 1").await.unwrap();
    assert!(
        dataset
            .manifest()
            .fragments
            .iter()
            .any(|f| f.deletion_file.is_some())
    );
    let before = dataset.object_store(None).await.unwrap();
    let scan = crate::instrumentation::manifest_scan_dataset(&dataset)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &before,
        &scan.object_store(None).await.unwrap()
    ));
    assert_eq!(scan.scan().try_into_batch().await.unwrap().num_rows(), 3);
}

#[tokio::test]
async fn manifest_scan_keeps_raw_memory_binding_readable() {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator};
    for scheme in ["memory://", "memory:/", "MeMoRy://"] {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![31, 47])) as ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &format!("{scheme}raw-memory-{}", ulid::Ulid::new()),
            Some(lance::dataset::WriteParams {
                store_params: Some(Default::default()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let before = dataset.object_store(None).await.unwrap();
        let scan = crate::instrumentation::manifest_scan_dataset(&dataset)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &before,
            &scan.object_store(None).await.unwrap()
        ));
        let actual = scan.scan().try_into_batch().await.unwrap();
        assert_eq!(actual.num_rows(), 2);
        let values = actual
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[31, 47]);
    }
}

/// The inclusive slot range of every range extent of `block`, in slot order.
async fn stored_ranges(uri: &str, block: &str) -> Vec<(u16, u16)> {
    let prefix = format!("blocks/{block}/");
    let mut ranges: Vec<(u16, u16)> = super::history::stored_names(uri)
        .await
        .unwrap()
        .iter()
        .map(|name| {
            let range = name.strip_prefix(&prefix).unwrap();
            let range = range.strip_suffix(".lance").unwrap();
            let (start, end) = range
                .split_once('-')
                .unwrap_or_else(|| panic!("'{name}' is not a range extent"));
            (start.parse().unwrap(), end.parse().unwrap())
        })
        .collect();
    ranges.sort();
    ranges
}

/// A run whose one object would exceed the object cap is archived as several
/// range extents, each under the cap, and never under the whole-block name,
/// which a reader then misses and falls back from to the block listing.
#[tokio::test]
async fn run_over_the_object_cap_is_archived_as_range_extents_under_the_cap() {
    const BLOCK: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const CAP: usize = 64 * 1024;
    let session = crate::lance_access::control_session();
    let mut records = history_block_records(BLOCK);
    for (index, record) in records.iter_mut().enumerate() {
        record.tables = (0..8).map(|table| bulky_table(index, table)).collect();
    }
    let closed = closed_block(&records);

    let unsplit = tempfile::tempdir().unwrap();
    let unsplit_uri = format!("file://{}", unsplit.path().display());
    super::history::settle_closed(&unsplit_uri, &session, &records, &closed)
        .await
        .unwrap();
    let whole = unsplit
        .path()
        .join(format!("__history/blocks/{BLOCK}/all.lance"));
    let whole = std::fs::metadata(whole).unwrap().len() as usize;
    assert!(
        whole > CAP,
        "the fixture run is one object of {whole} bytes, above the test cap"
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    for round in 0..2 {
        super::history::settle_within(&uri, &session, &records, &closed, CAP)
            .await
            .unwrap();
        let ranges = stored_ranges(&uri, BLOCK).await;
        assert!(ranges.len() > 1, "round {round}: {ranges:?}");
        assert_eq!(ranges[0].0, 0, "round {round}: {ranges:?}");
        assert_eq!(ranges[ranges.len() - 1].1, 15, "round {round}: {ranges:?}");
        for pair in ranges.windows(2) {
            assert_eq!(
                pair[0].1 + 1,
                pair[1].0,
                "round {round}: the parts tile the run with no gap and no overlap: {ranges:?}"
            );
        }
        for (start, end) in &ranges {
            let path = dir
                .path()
                .join(format!("__history/blocks/{BLOCK}/{start}-{end}.lance"));
            let size = std::fs::metadata(path).unwrap().len() as usize;
            assert!(size <= CAP, "extent {start}-{end} is {size} bytes");
        }
    }

    let ids: Vec<&str> = records
        .iter()
        .map(|record| record.commit.graph_commit_id.as_str())
        .collect();
    let found = super::history::read_records_of(&uri, &session, &ids)
        .await
        .unwrap();
    assert_eq!(found.len(), records.len());
    for record in &records {
        assert_eq!(found.get(&record.commit.graph_commit_id), Some(record));
    }
    let sent = HistoryGets::default();
    let probes = sent.probes();
    let lineage = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        super::history::read_lineage_of(&uri, &session, &ids[9..10]),
    )
    .await
    .unwrap();
    assert_eq!(lineage.get(ids[9]), Some(&records[9].commit));
    assert!(
        lineage.len() < records.len(),
        "only the part covering slot 9 is read, not the {} commits of the block",
        records.len()
    );
    let (_, lists, _, _) = history_requests(&probes);
    let (_, absent, heads) = sent.drain();
    assert_eq!(
        (absent, lists, heads),
        (1, 1, 0),
        "the absent whole-block name, then the block listing finds the part"
    );

    let alone = tempfile::tempdir().unwrap();
    let alone_uri = format!("file://{}", alone.path().display());
    let error = super::history::settle_within(
        &alone_uri,
        &session,
        &records[..1],
        &Default::default(),
        1024,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("alone exceeds the 1024-byte"),
        "{error}"
    );
    assert!(
        super::history::stored_names(&alone_uri)
            .await
            .unwrap()
            .is_empty(),
        "a record no object can take writes nothing"
    );
}

const WIDE_TABLES: usize = 128;

/// [`WIDE_TABLES`] pinned tables whose keys carry a 512-byte suffix, in identity order.
fn wide_tables() -> Vec<TableRow> {
    (1..=WIDE_TABLES as u64)
        .map(|table| {
            let identity = TableIdentity::new(table, 3).unwrap();
            let table_key = format!("node:Wide{table}{}", "x".repeat(512));
            TableRow {
                registration: TableRegistration {
                    identity,
                    table_path: table_path_for_identity(&table_key, identity).unwrap(),
                    table_key,
                },
                state: probe_pin(None),
            }
        })
        .collect()
}

/// The range extents of `block` under `dir`: they tile slots 0..=15, each is at
/// most `cap` bytes, and together they read back as `records`.
async fn wide_run_ranges(
    dir: &tempfile::TempDir,
    block: &str,
    cap: usize,
    records: &[HistoryRecord],
) -> Vec<(u16, u16)> {
    let uri = format!("file://{}", dir.path().display());
    let ranges = stored_ranges(&uri, block).await;
    assert_eq!(ranges[0].0, 0, "{ranges:?}");
    assert_eq!(ranges[ranges.len() - 1].1, 15, "{ranges:?}");
    for pair in ranges.windows(2) {
        assert_eq!(pair[0].1 + 1, pair[1].0, "no gap, no overlap: {ranges:?}");
    }
    for (start, end) in &ranges {
        let path = dir
            .path()
            .join(format!("__history/blocks/{block}/{start}-{end}.lance"));
        let size = std::fs::metadata(path).unwrap().len() as usize;
        assert!(size <= cap, "extent {start}-{end} is {size} bytes");
    }
    let ids: Vec<&str> = records
        .iter()
        .map(|record| record.commit.graph_commit_id.as_str())
        .collect();
    let session = crate::lance_access::control_session();
    let found = super::history::read_records_of(&uri, &session, &ids)
        .await
        .unwrap();
    assert_eq!(found.len(), records.len());
    for record in records {
        assert_eq!(found.get(&record.commit.graph_commit_id), Some(record));
    }
    ranges
}

/// A buffered run of cheap commits over a wide unchanged catalog, above the
/// object cap as one object, is archived holding the `table` rows of one
/// extent at a time, whether it arrives as records, a release or a merge.
#[tokio::test]
async fn a_wide_run_over_the_object_cap_is_rebuilt_one_extent_at_a_time() {
    use super::history::{EXTENT_ROWS_HELD, RecordSource};
    use super::state::{HeldRecord, TABLE_ROWS_REBUILT};
    const BLOCK: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let session = crate::lance_access::control_session();
    let current = wide_tables();
    let run = history_block_records(BLOCK);
    let closed = closed_block(&run);
    let buffer = CommitBuffer {
        commits: run.into_iter().map(|record| record.commit).collect(),
        replaced: Vec::new(),
    };
    let records = buffer.records(&current);
    let one_record = current.iter().map(super::state::table_bytes).sum::<usize>()
        + super::state::commit_bytes(&buffer.commits[0]);
    let cap = 7 * one_record;
    let two_records = 2 * WIDE_TABLES;

    let owned = tempfile::tempdir().unwrap();
    let owned_uri = format!("file://{}", owned.path().display());
    EXTENT_ROWS_HELD.set(0);
    super::history::settle_within(
        &owned_uri,
        &session,
        &buffer.records(&current),
        &closed,
        cap,
    )
    .await
    .unwrap();
    assert!(
        EXTENT_ROWS_HELD.get() <= two_records,
        "one extent attempt held {} table rows of a run of {} records of {WIDE_TABLES} tables; \
         a halved part under half the cap is two records",
        EXTENT_ROWS_HELD.get(),
        records.len()
    );
    let owned_ranges = wide_run_ranges(&owned, BLOCK, cap, &records).await;
    assert!(owned_ranges.len() >= 8, "{owned_ranges:?}");

    let released: Vec<HeldRecord<'_>> = buffer
        .commits
        .iter()
        .map(|commit| HeldRecord {
            commit,
            buffer: &buffer,
            current: &current,
        })
        .collect();
    assert_eq!(
        released[0].record_bytes(),
        RecordSource::record_bytes(&released[0].record()),
        "a held record measures as the record it builds, so a run splits at the same slots"
    );
    let release = tempfile::tempdir().unwrap();
    let release_uri = format!("file://{}", release.path().display());
    EXTENT_ROWS_HELD.set(0);
    TABLE_ROWS_REBUILT.set(0);
    super::history::settle_within(&release_uri, &session, &released, &closed, cap)
        .await
        .unwrap();
    assert!(
        EXTENT_ROWS_HELD.get() <= two_records,
        "one extent attempt of the released run held {} table rows",
        EXTENT_ROWS_HELD.get()
    );
    assert_eq!(
        TABLE_ROWS_REBUILT.get(),
        records.len() * WIDE_TABLES,
        "measuring a held record rebuilds nothing; each record is built once, for its extent"
    );
    assert_eq!(
        wide_run_ranges(&release, BLOCK, cap, &records).await,
        owned_ranges,
        "a released run is written under the names the same records are"
    );

    let mut commits = buffer.commits.clone();
    let head = commits.pop().unwrap();
    let source = BranchRecords {
        buffer: CommitBuffer {
            commits,
            replaced: Vec::new(),
        },
        head: HistoryRecord {
            commit: head,
            tables: current.clone(),
        },
        merge_base: None,
    };
    let merged: Vec<HeldRecord<'_>> = source.held().collect();
    let merge = tempfile::tempdir().unwrap();
    let merge_uri = format!("file://{}", merge.path().display());
    EXTENT_ROWS_HELD.set(0);
    super::history::settle_within(&merge_uri, &session, &merged, &closed, cap)
        .await
        .unwrap();
    assert!(
        EXTENT_ROWS_HELD.get() <= two_records,
        "one extent attempt of the merged run held {} table rows",
        EXTENT_ROWS_HELD.get()
    );
    assert_eq!(
        wide_run_ranges(&merge, BLOCK, cap, &records).await,
        owned_ranges
    );
}

/// The publisher chooses the records of a release and of a merge over a wide catalog as held records: it rebuilds no `table` row before an extent is encoded.
#[test]
fn records_to_append_of_a_wide_run_rebuild_no_table_rows() {
    use super::publisher::{GraphNamespacePublisher, HeldRows};
    use super::state::TABLE_ROWS_REBUILT;
    let current = wide_tables();
    let run = history_block_records("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let buffer = CommitBuffer {
        commits: run.iter().map(|record| record.commit.clone()).collect(),
        replaced: Vec::new(),
    };
    let opener_of_the_next_block = history_block_records("01BX5ZZKBKACTAV9WEVGEMMVRZ")
        .remove(0)
        .commit;
    assert!(buffer.is_full(&opener_of_the_next_block, &current).unwrap());

    TABLE_ROWS_REBUILT.set(0);
    let release = lineage_intent(None, None);
    let read = HeldRows {
        tables: &current,
        head: &opener_of_the_next_block,
        buffer: &buffer,
    };
    let released =
        GraphNamespacePublisher::chosen_records_to_append(&read, None, &release).unwrap();
    assert_eq!(
        (released.len(), TABLE_ROWS_REBUILT.get()),
        (run.len(), 0),
        "a release of {} commits over {WIDE_TABLES} tables is chosen with no table row rebuilt",
        run.len()
    );

    let mut commits = buffer.commits.clone();
    let head = commits.pop().unwrap();
    let source = BranchRecords {
        buffer: CommitBuffer {
            commits,
            replaced: Vec::new(),
        },
        head: HistoryRecord {
            commit: head,
            tables: current.clone(),
        },
        merge_base: None,
    };
    let merge = lineage_intent(Some("target"), Some(source));
    let read = HeldRows {
        tables: &current,
        head: &opener_of_the_next_block,
        buffer: &CommitBuffer::default(),
    };
    let merged =
        GraphNamespacePublisher::chosen_records_to_append(&read, Some("target"), &merge).unwrap();
    assert_eq!(
        (merged.len(), TABLE_ROWS_REBUILT.get()),
        (run.len(), 0),
        "the records of a merged branch are chosen with no table row rebuilt"
    );
}

/// A head whose commit fields are over their bound is refused by the publish
/// that would acknowledge it, before any write: `__manifest` keeps its
/// version, the buffer never holds the commit, and the branch publishes on.
#[tokio::test]
async fn publish_refuses_a_head_over_the_commit_field_bound_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let mut mc = ManifestCoordinator::init(&uri, &build_test_catalog())
        .await
        .unwrap();
    let genesis = mc.head().clone();
    let version = mc.version();

    let oversized = LineageIntent {
        actor_id: Some("a".repeat(HISTORY_RELEASE_BYTES)),
        ..lineage_intent(None, None)
    };
    let probes = fresh_publish_probes();
    let error = crate::instrumentation::with_query_io_probes(
        probes.clone(),
        mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&oversized)),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("commit fields of one history record; the commit is not published"),
        "{error}"
    );
    assert!(
        matches!(&error, OmniError::Manifest(_)),
        "a publish error: {error:?}"
    );
    assert_eq!(
        (
            drain_publish_io(&probes.manifest_stores).writes,
            drain_publish_io(&probes.history_stores).writes
        ),
        (0, 0),
        "the refusal precedes every write"
    );
    let rows = main_rows(&uri).await;
    assert_eq!(
        open_manifest_dataset(&uri, None)
            .await
            .unwrap()
            .version()
            .version,
        version
    );
    assert_eq!(rows.head, genesis);
    assert!(rows.buffer.commits().is_empty());

    let under_the_bound = LineageIntent {
        actor_id: Some("a".repeat(HISTORY_RELEASE_BYTES / 2)),
        ..lineage_intent(None, None)
    };
    let large = under_the_bound.graph_commit_id.clone();
    let filling = LineageIntent {
        actor_id: under_the_bound.actor_id.clone(),
        ..lineage_intent(None, None)
    };
    for intent in [
        under_the_bound,
        filling,
        lineage_intent(None, None),
        lineage_intent(None, None),
    ] {
        mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
            .await
            .unwrap();
        assert_eq!(mc.head().graph_commit_id, intent.graph_commit_id);
    }
    let archived =
        super::history::read_record(&uri, &crate::lance_access::control_session(), &large)
            .await
            .unwrap()
            .expect("a head under the bound is archived when it leaves the buffer");
    assert_eq!(
        archived.commit.actor_id.map(|actor| actor.len()),
        Some(HISTORY_RELEASE_BYTES / 2)
    );
}

fn schema_object_path(dir: &std::path::Path, digest: &str) -> std::path::PathBuf {
    dir.join(format!("__history/schemas/{digest}.schema"))
}

/// The schema content of a commit is one object under `__history/schemas/`,
/// named by the SHA-256 of its bytes; a source-only change takes a new name;
/// a commit record's snapshot reads its contract there, never from `__manifest`.
#[tokio::test]
async fn schema_content_is_archived_by_digest_and_serves_commit_snapshots() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let catalog = build_test_catalog();
    let genesis_contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    let mut mc = ManifestCoordinator::init(&uri, &catalog).await.unwrap();
    let genesis = mc.head().clone();

    let digest = super::history::schema_content_hash(&genesis_contract).unwrap();
    assert_eq!(
        genesis.schema_content_hash.as_deref(),
        Some(digest.as_str())
    );
    let object = std::fs::read(schema_object_path(dir.path(), &digest)).unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(&object)), digest);
    let head_json = serde_json::to_vec(&genesis_contract.head).unwrap();
    let parts = [
        head_json.as_slice(),
        genesis_contract.source.as_bytes(),
        genesis_contract.ir.as_bytes(),
    ];
    let mut expected_object = b"OGSC0001".to_vec();
    for part in parts {
        expected_object.extend_from_slice(&(part.len() as u64).to_le_bytes());
    }
    for part in parts {
        expected_object.extend_from_slice(part);
    }
    assert_eq!(object, expected_object);
    assert_eq!(
        super::history::read_schema(&uri, &session, &digest, &genesis_contract.head)
            .await
            .unwrap(),
        genesis_contract
    );

    let mut source_only = genesis_contract.clone();
    source_only
        .source
        .push_str("// a comment the IR does not carry\n");
    assert_eq!(source_only.head, genesis_contract.head);
    let source_only_digest = super::history::schema_content_hash(&source_only).unwrap();
    assert_ne!(source_only_digest, digest);
    mc.commit_changes_with_lineage(
        &[ManifestChange::SchemaContract(source_only.clone())],
        &HashMap::new(),
        Some(&lineage_intent(None, None)),
    )
    .await
    .unwrap();
    assert_eq!(
        mc.head().schema_content_hash.as_deref(),
        Some(source_only_digest.as_str())
    );
    assert!(schema_object_path(dir.path(), &source_only_digest).exists());

    let probes = fresh_publish_probes();
    crate::instrumentation::with_query_io_probes(
        probes.clone(),
        mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&lineage_intent(None, None))),
    )
    .await
    .unwrap();
    assert_eq!(
        mc.head().schema_content_hash.as_deref(),
        Some(source_only_digest.as_str()),
        "a publish that leaves the contract alone carries the name forward"
    );
    assert_eq!(
        drain_publish_io(&probes.history_stores),
        PublishIo::default(),
        "and sends `__history` nothing"
    );
    assert_eq!(
        std::fs::read_dir(dir.path().join("__history/schemas"))
            .unwrap()
            .count(),
        2
    );

    let read_genesis_contract = |record: HistoryRecord| {
        let uri = uri.clone();
        async move {
            let snapshot = record.snapshot(&uri).unwrap();
            let sent = HistoryGets::default();
            let probes = sent.probes();
            let opens = probes.internal_open_count.clone();
            let contract = crate::instrumentation::with_query_io_probes(
                probes.clone(),
                ManifestCoordinator::read_schema_contract_for_snapshot(&uri, &snapshot),
            )
            .await
            .unwrap();
            let manifest = drain_publish_io(&probes.manifest_stores);
            let (gets, absent, heads) = sent.drain();
            assert_eq!(
                (
                    opens.load(std::sync::atomic::Ordering::Relaxed),
                    manifest.reads,
                    gets,
                    absent,
                    heads
                ),
                (0, 0, 1, 0, 0),
                "one GET of the archived content and no request to `__manifest`"
            );
            contract
        }
    };
    let buffered = mc.held_record(&genesis.graph_commit_id).unwrap();
    assert_eq!(read_genesis_contract(buffered).await, genesis_contract);

    fill_buffer(&mut mc, None).await;
    mc.commit_changes_with_lineage(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
        .await
        .unwrap();
    assert!(mc.held_record(&genesis.graph_commit_id).is_none());
    let archived = super::history::read_record(&uri, &session, &genesis.graph_commit_id)
        .await
        .unwrap()
        .expect("the release archived the genesis commit");
    assert_eq!(archived.commit, genesis);
    assert_eq!(read_genesis_contract(archived).await, genesis_contract);
    let latest = mc.head_record().snapshot(&uri).unwrap();
    assert_eq!(
        ManifestCoordinator::read_schema_contract_for_snapshot(&uri, &latest)
            .await
            .unwrap(),
        source_only
    );
}

/// Archived schema content that is missing, altered, of another identity or
/// unnamed fails closed, and a second create under a name must hold the same
/// bytes. No failure falls back to a `__manifest` version.
#[tokio::test]
async fn missing_or_inconsistent_archived_schema_content_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let session = crate::lance_access::control_session();
    let catalog = build_test_catalog();
    let contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    let mc = ManifestCoordinator::init(&uri, &catalog).await.unwrap();
    let digest = mc.head().schema_content_hash.clone().unwrap();
    let path = schema_object_path(dir.path(), &digest);
    let intact = std::fs::read(&path).unwrap();
    let snapshot = mc.head_record().snapshot(&uri).unwrap();
    let read = || async {
        let direct = super::history::read_schema(&uri, &session, &digest, &contract.head).await;
        let through = ManifestCoordinator::read_schema_contract_for_snapshot(&uri, &snapshot).await;
        assert_eq!(
            direct.as_ref().map_err(ToString::to_string),
            through.as_ref().map_err(ToString::to_string),
            "a commit snapshot reads the archive and nothing else"
        );
        direct
    };
    let refused = |result: Result<SchemaContractRow>, expected: &str| {
        let error = result.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    };
    assert_eq!(read().await.unwrap(), contract);

    assert_eq!(
        super::history::archive_schema(&uri, &session, &contract)
            .await
            .unwrap(),
        digest,
        "a duplicate create of the same content agrees"
    );
    assert_eq!(std::fs::read(&path).unwrap(), intact);

    let other_identity = SchemaContractHead {
        schema_identity_version: contract.head.schema_identity_version + 1,
        ..contract.head.clone()
    };
    refused(
        super::history::read_schema(&uri, &session, &digest, &other_identity).await,
        "carries IR hash",
    );
    refused(
        super::history::read_schema(&uri, &session, "../../__manifest", &contract.head).await,
        "not lowercase SHA-256 hex",
    );

    let mut altered = intact.clone();
    *altered.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &altered).unwrap();
    refused(read().await, "does not hash to its name");
    refused(
        super::history::archive_schema(&uri, &session, &contract)
            .await
            .map(|_| contract.clone()),
        "differs from the content it is named for",
    );

    let mut other = contract.clone();
    other.source.push_str("// other content\n");
    let other_digest = super::history::archive_schema(&uri, &session, &other)
        .await
        .unwrap();
    std::fs::copy(schema_object_path(dir.path(), &other_digest), &path).unwrap();
    refused(read().await, "does not hash to its name");

    std::fs::remove_file(&path).unwrap();
    refused(read().await, "is missing");

    let mut unnamed = mc.head_record().clone();
    unnamed.commit.schema_content_hash = None;
    refused(
        ManifestCoordinator::read_schema_contract_for_snapshot(
            &uri,
            &unnamed.snapshot(&uri).unwrap(),
        )
        .await,
        "recorded no schema content",
    );
    let rows = ManifestRows {
        schema_contract_head: None,
        schema_contract: None,
        tables: Vec::new(),
        head: unnamed.commit,
        buffer: CommitBuffer::default(),
    };
    let schema = super::record::manifest_storage_schema(HashMap::new());
    let stored = super::record::compact_to_storage(&rows.to_batch().unwrap(), &schema).unwrap();
    let error = super::state::rows_of_batch(&stored)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("without the name of its archived schema content"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_version_publish_races_for_one_candidate_without_rebase() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let coordinator = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let base = coordinator.version();
    let head = coordinator.exact_graph_head();
    let precondition = PublishPrecondition::ExactGraphVersion {
        authority: GraphHeadExpectation::new(
            None,
            lance::dataset::refs::BranchIdentifier::main(),
            head.clone(),
        ),
        version: base,
    };
    let original = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("original".to_string()),
        merged_parent: None,
        created_at: lineage_now_micros(),
        history_release_bytes: HistoryReleaseBytes::PRODUCTION,
    };
    let fence = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        actor_id: Some("recovery".to_string()),
        ..original.clone()
    };
    let left = GraphNamespacePublisher::new(uri, None);
    let right = GraphNamespacePublisher::new(uri, None);
    let expected = HashMap::new();
    let (a, b) = tokio::join!(
        left.publish_with_precondition(&[], &expected, Some(&original), &precondition),
        right.publish_with_precondition(&[], &expected, Some(&fence), &precondition),
    );
    let (winner, loser) = match (a, b) {
        (Ok(_), Err(error)) => (&original, error),
        (Err(error), Ok(_)) => (&fence, error),
        other => panic!("exactly one numeric candidate must win: {other:?}"),
    };
    assert!(matches!(loser, OmniError::Manifest(ManifestError {
        details: Some(ManifestConflictDetails::ReadSetChanged { ref member, .. }), ..
    }) if member == "prepared_schema_manifest_version"));
    let candidate = read_schema_publication_candidate_at(
        uri,
        base + 1,
        Some(&original.graph_commit_id),
        Some(&fence.graph_commit_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_intent_nonce(&candidate.head.graph_commit_id, winner);
    assert_eq!(candidate.head.parent_commit_id, head);
    assert_ne!(candidate.original.is_some(), candidate.settlement.is_some());
    assert_eq!(
        open_manifest_dataset(uri, None)
            .await
            .unwrap()
            .version()
            .version,
        base + 1
    );
}

fn legacy_metadata() -> TableVersionMetadata {
    TableVersionMetadata::from_json_str(
        r#"{"manifest_path":"p","manifest_size":null,"e_tag":null,"naming_scheme":null}"#,
    )
    .unwrap()
}

fn legacy_table(stable_table_id: u64, table_key: &str) -> TableRegistration {
    let identity = TableIdentity::new(stable_table_id, 1).unwrap();
    TableRegistration {
        identity,
        table_path: table_path_for_identity(table_key, identity).unwrap(),
        table_key: table_key.to_string(),
    }
}

fn legacy_pin(table: &TableRegistration, table_version: u64) -> super::legacy::write::LegacyPin {
    super::legacy::write::LegacyPin {
        identity: table.identity,
        table_version,
        table_branch: None,
        row_count: table_version * 10,
        metadata: legacy_metadata(),
    }
}

fn legacy_read_pin(table_version: u64, manifest_version: u64) -> TablePin {
    TablePin {
        table_version,
        table_branch: None,
        row_count: table_version * 10,
        metadata: legacy_metadata(),
        manifest_version,
    }
}

fn legacy_commit(id: &str, created_at: i64) -> super::legacy::write::LegacyCommitIntent {
    super::legacy::write::LegacyCommitIntent {
        graph_commit_id: id.to_string(),
        merged_parent_commit_id: None,
        actor_id: Some("actor".to_string()),
        created_at,
    }
}

fn legacy_read_commit(
    id: &str,
    branch: Option<&str>,
    version: u64,
    parent: Option<&str>,
) -> super::legacy::LegacyCommit {
    super::legacy::LegacyCommit {
        graph_commit_id: id.to_string(),
        graph_branch: branch.map(str::to_string),
        graph_manifest_version: version,
        parent_commit_id: parent.map(str::to_string),
        merged_parent_commit_id: None,
        actor_id: Some("actor".to_string()),
        created_at: i64::try_from(version).unwrap(),
    }
}

fn legacy_contract(source: &str) -> SchemaContractRow {
    SchemaContractRow {
        source: source.to_string(),
        ir: r#"{"ir":1}"#.to_string(),
        head: SchemaContractHead {
            schema_ir_hash: "ir-hash".to_string(),
            schema_identity_version: 1,
            schema_identity_domain: "domain".to_string(),
        },
    }
}

/// Main of a stamp-13 history: genesis `G` at version 1 registers and pins
/// `node:Person` and `node:Company` under the contract, `C2` at version 2 pins
/// Person again and renames Company to `node:Firm`.
async fn legacy_main(
    root: &str,
) -> (
    super::legacy::write::Stamp13History,
    [TableRegistration; 3],
    SchemaContractRow,
) {
    legacy_main_with_ids(root, ["G", "C2"]).await
}

/// [`legacy_main`] with the ids of its two commits given.
async fn legacy_main_with_ids(
    root: &str,
    [genesis, second]: [&str; 2],
) -> (
    super::legacy::write::Stamp13History,
    [TableRegistration; 3],
    SchemaContractRow,
) {
    use super::legacy::write::{LegacyPublish, Stamp13History};
    let person = legacy_table(1, "node:Person");
    let company = legacy_table(2, "node:Company");
    let firm = TableRegistration {
        table_key: "node:Firm".to_string(),
        ..company.clone()
    };
    let contract = legacy_contract("node Person {}");
    let mut history = Stamp13History::create(
        root,
        LegacyPublish {
            tables: vec![person.clone(), company.clone()],
            contract: Some(contract.clone()),
            pins: vec![legacy_pin(&person, 1), legacy_pin(&company, 1)],
            commit: Some(legacy_commit(genesis, 1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let version = history
        .publish(
            None,
            LegacyPublish {
                tables: vec![firm.clone()],
                pins: vec![legacy_pin(&person, 2)],
                commit: Some(legacy_commit(second, 2)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(version, 2);
    (history, [person, company, firm], contract)
}

async fn manifest_scans<T>(future: impl std::future::Future<Output = Result<T>>) -> (T, u64) {
    let probes = crate::instrumentation::QueryIoProbes::default();
    let scans = probes.manifest_scan_count.clone();
    let value = crate::instrumentation::with_query_io_probes(probes, future)
        .await
        .unwrap();
    (value, scans.load(std::sync::atomic::Ordering::Relaxed))
}

/// A stamp-13 history written as kept-then-pending overwrites reads back
/// through `Stamp13Source`: every commit, head, pin and drop of a head, and
/// each version's tables and contract, its IR read only when it changed.
#[tokio::test]
async fn legacy_stamp_13_history_round_trips_through_the_source_reader() {
    use super::legacy::write::LegacyPublish;
    use super::legacy::{HeadScan, LegacyManifestSource, SourceRole, Stamp13Source, VersionSchema};
    use std::collections::BTreeMap;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let (mut history, [person, company, firm], contract) = legacy_main(root).await;
    let feature = crate::branch_names::native_branch_name(
        "feature",
        &crate::branch_names::mint_incarnation(),
    );
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    let version = history
        .publish(
            Some(&feature),
            LegacyPublish {
                drops: vec![(firm.identity, 1)],
                commit: Some(legacy_commit("C3", 3)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(version, 3);

    let source = Stamp13Source;
    let main = history.head(None).unwrap().clone();
    let branch = history.head(Some(&feature)).unwrap().clone();
    assert_eq!(source.admits(&main, SourceRole::LiveHead).unwrap(), 13);
    assert_eq!(source.admits(&branch, SourceRole::LiveHead).unwrap(), 13);
    let pins = BTreeMap::from([
        ((person.identity, 1), legacy_read_pin(1, 1)),
        ((company.identity, 1), legacy_read_pin(1, 1)),
        ((person.identity, 2), legacy_read_pin(2, 2)),
    ]);
    let main_scan = HeadScan {
        commits: vec![
            legacy_read_commit("G", None, 1, None),
            legacy_read_commit("C2", None, 2, Some("G")),
        ],
        heads: HashMap::from([("main".to_string(), "C2".to_string())]),
        pins: pins.clone(),
        tombstones: BTreeMap::new(),
        contract: Some(contract.clone()),
        rows: 9,
    };
    assert_eq!(source.scan_head(&main).await.unwrap(), main_scan);
    assert_eq!(
        source.scan_head(&branch).await.unwrap(),
        HeadScan {
            commits: vec![
                legacy_read_commit("G", None, 1, None),
                legacy_read_commit("C2", None, 2, Some("G")),
                legacy_read_commit("C3", Some("feature"), 3, Some("C2")),
            ],
            heads: HashMap::from([
                ("main".to_string(), "C2".to_string()),
                ("feature".to_string(), "C3".to_string()),
            ]),
            pins,
            tombstones: BTreeMap::from([((firm.identity, 3), 1)]),
            contract: Some(contract.clone()),
            rows: 12,
        }
    );

    let (at_1, scans) =
        manifest_scans(source.version_schema(&main.checkout_version(1).await.unwrap(), None)).await;
    assert_eq!(
        (at_1.clone(), scans),
        (
            VersionSchema {
                tables: vec![person.clone(), company],
                contract: Some(contract.clone()),
            },
            2
        )
    );
    let (at_2, scans) = manifest_scans(
        source.version_schema(&main.checkout_version(2).await.unwrap(), Some(&at_1)),
    )
    .await;
    assert_eq!(
        (at_2.clone(), scans),
        (
            VersionSchema {
                tables: vec![person.clone(), firm.clone()],
                contract: Some(contract.clone()),
            },
            1
        )
    );

    let edited = legacy_contract("node Person {}\n");
    let version = history
        .publish(
            None,
            LegacyPublish {
                contract: Some(edited.clone()),
                commit: Some(legacy_commit("C4", 4)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (at_3, scans) = manifest_scans(
        source.version_schema(
            &history
                .head(None)
                .unwrap()
                .checkout_version(version)
                .await
                .unwrap(),
            Some(&at_2),
        ),
    )
    .await;
    assert_eq!(
        (at_3, scans),
        (
            VersionSchema {
                tables: vec![person, firm],
                contract: Some(edited),
            },
            2
        )
    );
}

/// Versions rewritten at stamps 12, 8 and 6 read in their own shape: the
/// packed record without content and the flat columns hold the head and the
/// tables, and no contract; a live head admits only 13, a retired head 8 to 13.
#[tokio::test]
async fn legacy_versions_below_13_read_in_their_own_shape() {
    use super::legacy::write::LegacyPublish;
    use super::legacy::{HeadScan, LegacyManifestSource, SourceRole, Stamp13Source, VersionSchema};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let (mut history, [person, _, firm], _) = legacy_main(root).await;
    let source = Stamp13Source;
    let at_13 = source.scan_head(history.head(None).unwrap()).await.unwrap();
    let without_contract = HeadScan {
        contract: None,
        rows: at_13.rows - 1,
        ..at_13.clone()
    };
    let tables = VersionSchema {
        tables: vec![person, firm],
        contract: None,
    };

    assert_eq!(history.restamp_for_test(None, Some(12)).await.unwrap(), 3);
    let at_12 = history.head(None).unwrap().clone();
    let error = source.admits(&at_12, SourceRole::LiveHead).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 3 is v12: the head of a live branch must be stamped v13"),
        "{error}"
    );
    assert_eq!(source.admits(&at_12, SourceRole::RetiredHead).unwrap(), 12);
    assert_eq!(source.scan_head(&at_12).await.unwrap(), without_contract);
    assert_eq!(source.version_schema(&at_12, None).await.unwrap(), tables);

    assert_eq!(history.restamp_for_test(None, Some(8)).await.unwrap(), 4);
    let at_8 = history.head(None).unwrap().clone();
    assert_eq!(source.admits(&at_8, SourceRole::RetiredHead).unwrap(), 8);
    assert_eq!(source.scan_head(&at_8).await.unwrap(), without_contract);

    assert_eq!(history.restamp_for_test(None, Some(6)).await.unwrap(), 5);
    let at_6 = history.head(None).unwrap().clone();
    let error = source.admits(&at_6, SourceRole::RetiredHead).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 5 is v6: the head of a retired branch must be stamped v8 to v13"),
        "{error}"
    );
    assert_eq!(source.admits(&at_6, SourceRole::Version).unwrap(), 6);
    let (at_6_tables, scans) = manifest_scans(source.version_schema(&at_6, None)).await;
    assert_eq!((at_6_tables, scans), (tables, 1));

    let version = history
        .publish(
            None,
            LegacyPublish {
                commit: Some(legacy_commit("C3", 6)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(version, 6);
    let at_13 = history.head(None).unwrap().clone();
    assert_eq!(source.admits(&at_13, SourceRole::LiveHead).unwrap(), 13);
    assert_eq!(
        source.scan_head(&at_13).await.unwrap().heads,
        HashMap::from([("main".to_string(), "C3".to_string())])
    );
}

/// A pending conversion's key refuses the read in every role.
#[tokio::test]
async fn legacy_source_refuses_a_pending_conversion_in_every_role() {
    use super::legacy::{LegacyManifestSource, SourceRole, Stamp13Source};
    let dir = tempfile::tempdir().unwrap();
    let (history, _, _) = legacy_main(dir.path().to_str().unwrap()).await;
    let mut main = history.head(None).unwrap().clone();
    main.update_schema_metadata([(super::migrations::UPGRADE_PENDING_KEY, "{}")])
        .await
        .unwrap();
    let pending = "version 3 carries a pending storage conversion";
    let source = Stamp13Source;
    for role in [
        SourceRole::LiveHead,
        SourceRole::RetiredHead,
        SourceRole::Version,
    ] {
        let error = source.admits(&main, role).unwrap_err();
        assert!(error.to_string().contains(pending), "{role:?}: {error}");
    }
    let error = source.scan_head(&main).await.unwrap_err();
    assert!(error.to_string().contains(pending), "{error}");
    let error = source.version_schema(&main, None).await.unwrap_err();
    assert!(error.to_string().contains(pending), "{error}");
}

/// The stamp alone admits nothing: stamp-14 rows restamped 13 are refused by
/// their shape, an unstamped version only at version 1 or 2, a stamp below 6
/// in every role.
#[tokio::test]
async fn legacy_source_admits_a_version_by_its_stamp_and_its_stored_shape() {
    use super::legacy::{LegacyManifestSource, SourceRole, Stamp13Source, VersionSchema};
    let source = Stamp13Source;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("current");
    let uri = uri.to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut current = open_manifest_dataset(uri, None).await.unwrap();
    super::migrations::set_stamp_for_test(&mut current, 13)
        .await
        .unwrap();
    for role in [SourceRole::LiveHead, SourceRole::Version] {
        let error = source.admits(&current, role).unwrap_err();
        assert!(
            error.to_string().contains(
                "version 2 is stamped v13 but its rows are not stored as a v13 version stores \
                 them: expected the packed 8-field record and both schema content columns"
            ),
            "{role:?}: {error}"
        );
    }

    let root = dir.path().join("legacy");
    let (mut history, [person, company, _], _) = legacy_main(root.to_str().unwrap()).await;
    let at_1 = history
        .head(None)
        .unwrap()
        .checkout_version(1)
        .await
        .unwrap();
    assert_eq!(source.admits(&at_1, SourceRole::Version).unwrap(), 13);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut genesis = super::legacy::write::Stamp13History::create(
        root,
        super::legacy::write::LegacyPublish {
            tables: vec![person.clone(), company.clone()],
            pins: vec![legacy_pin(&person, 1), legacy_pin(&company, 1)],
            commit: Some(legacy_commit("G", 1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(genesis.restamp_for_test(None, None).await.unwrap(), 2);
    let unstamped = genesis.head(None).unwrap().clone();
    assert_eq!(source.admits(&unstamped, SourceRole::Version).unwrap(), 1);
    assert_eq!(
        source.version_schema(&unstamped, None).await.unwrap(),
        VersionSchema {
            tables: vec![person, company],
            contract: None,
        }
    );
    let error = source.admits(&unstamped, SourceRole::LiveHead).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 2 is unstamped: the head of a live branch must be stamped v13"),
        "{error}"
    );
    assert_eq!(genesis.restamp_for_test(None, None).await.unwrap(), 3);
    let error = source
        .admits(genesis.head(None).unwrap(), SourceRole::Version)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 3 is unstamped: a retained version must be stamped v6 to v13"),
        "{error}"
    );

    assert_eq!(history.restamp_for_test(None, Some(5)).await.unwrap(), 3);
    for role in [
        SourceRole::LiveHead,
        SourceRole::RetiredHead,
        SourceRole::Version,
    ] {
        let error = source
            .admits(history.head(None).unwrap(), role)
            .unwrap_err();
        assert!(
            error.to_string().contains("version 3 is v5"),
            "{role:?}: {error}"
        );
    }
}

fn legacy_id(number: u128) -> String {
    ulid::Ulid::from(number).to_string()
}

fn legacy_native(logical: &str) -> String {
    crate::branch_names::native_branch_name(logical, &crate::branch_names::mint_incarnation())
}

fn legacy_pinned(table: &TableRegistration, table_version: u64, clock: u64) -> TableRow {
    TableRow {
        registration: table.clone(),
        state: TableState::Pinned(legacy_read_pin(table_version, clock)),
    }
}

/// The census input of every ref of `history` at its head, less the refs
/// named in `gone`, as an inventory that cannot see them would pin it.
async fn legacy_census_input(
    history: &super::legacy::write::Stamp13History,
    gone: &[&str],
) -> super::legacy::CensusInput {
    use super::legacy::{CensusInput, CensusRef};
    let main = history.head(None).unwrap();
    let pinned =
        |native: String, version: u64, contents: lance::dataset::refs::BranchContents| CensusRef {
            native: Some(native),
            version,
            parent: contents.parent_branch,
            parent_version: contents.parent_version,
        };
    let mut live = vec![CensusRef {
        native: None,
        version: main.version().version,
        parent: None,
        parent_version: 0,
    }];
    for (native, contents) in crate::branch_control::list_live_manifest_branch_contents(main)
        .await
        .unwrap()
    {
        if !gone.contains(&native.as_str()) {
            let version = history.head(Some(&native)).unwrap().version().version;
            live.push(pinned(native, version, contents));
        }
    }
    let mut retired = Vec::new();
    for (native, contents) in super::retention::retired_manifest_branches(main)
        .await
        .unwrap()
    {
        if !gone.contains(&native.as_str()) {
            let head = main
                .checkout_version(lance::dataset::refs::Ref::Version(
                    Some(native.clone()),
                    None,
                ))
                .await
                .unwrap();
            retired.push(pinned(native, head.version().version, contents));
        }
    }
    CensusInput {
        attempt: ulid::Ulid::from(7u128),
        live,
        retired,
    }
}

async fn legacy_census(
    history: &super::legacy::write::Stamp13History,
    gone: &[&str],
) -> std::result::Result<super::legacy::LegacyCensus, super::legacy::CensusError> {
    let input = legacy_census_input(history, gone).await;
    super::legacy::census(history.root(), &input, &super::legacy::Stamp13Source).await
}

fn legacy_finding<T: std::fmt::Debug>(
    result: std::result::Result<T, super::legacy::CensusError>,
) -> (super::legacy::FindingCode, String) {
    match result {
        Err(super::legacy::CensusError::Finding(finding)) => (finding.code, finding.message),
        other => panic!("expected a finding, got {other:?}"),
    }
}

fn legacy_record_ids(chain: &super::history::LegacyChain) -> Vec<&str> {
    chain
        .records
        .iter()
        .map(|record| record.commit.graph_commit_id.as_str())
        .collect()
}

/// Every commit lands under the ref that wrote it with the tables of its
/// version, the plan is a pure function of the census, and the materialized
/// objects and the conversion rows read back as the census derived them.
#[tokio::test]
async fn legacy_census_derives_writers_tables_and_heads_and_plans_deterministically() {
    use super::history::{LegacyLayout, LegacyWriterKind};
    use super::legacy::write::LegacyPublish;
    use super::legacy::{FindingCode, LegacyPlan};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2, f3, c3, c4, dangling] = [1, 2, 3, 4, 5, 99].map(legacy_id);
    let (mut history, [person, company, firm], contract) =
        legacy_main_with_ids(root, [&g, &c2]).await;

    let first_feature = legacy_native("feature");
    assert_eq!(history.fork(None, &first_feature).await.unwrap(), 2);
    history
        .publish(
            Some(&first_feature),
            LegacyPublish {
                pins: vec![legacy_pin(&person, 3)],
                commit: Some(legacy_commit(&f3, 3)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let reused = legacy_table(3, "node:Company");
    let edited = legacy_contract("node Person {}\n");
    history
        .publish(
            None,
            LegacyPublish {
                tables: vec![reused.clone()],
                contract: Some(edited.clone()),
                pins: vec![legacy_pin(&reused, 1)],
                commit: Some(legacy_commit(&c3, 3)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let fresh = legacy_native("fresh");
    assert_eq!(history.fork(None, &fresh).await.unwrap(), 3);
    history
        .publish(
            None,
            LegacyPublish {
                commit: Some(super::legacy::write::LegacyCommitIntent {
                    merged_parent_commit_id: Some(dangling.clone()),
                    ..legacy_commit(&c4, 4)
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let child = legacy_native("child");
    assert_eq!(history.fork(Some(&first_feature), &child).await.unwrap(), 3);
    history.retire(&first_feature).await.unwrap();
    let second_feature = legacy_native("feature");
    assert_eq!(
        history.fork(Some(&child), &second_feature).await.unwrap(),
        3
    );

    let census = legacy_census(&history, &[]).await.unwrap();
    assert_eq!(census.source_stamp, 13);
    assert_eq!(
        census
            .chains
            .iter()
            .map(|chain| (
                chain.kind,
                chain.native.as_deref(),
                legacy_record_ids(chain)
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                LegacyWriterKind::Main,
                None,
                vec![g.as_str(), c2.as_str(), c3.as_str(), c4.as_str()]
            ),
            (
                LegacyWriterKind::Retired,
                Some(first_feature.as_str()),
                vec![f3.as_str()]
            ),
        ]
    );
    let retired = &census.chains[1];
    assert_eq!(
        (
            retired.parent.as_deref(),
            retired.parent_version,
            retired.head_version
        ),
        (None, 2, 3)
    );
    let main_records = &census.chains[0].records;
    assert_eq!(
        main_records
            .iter()
            .chain(&retired.records)
            .map(|record| (
                record.commit.generation,
                record.commit.native_branch.as_deref(),
                record.commit.graph_branch.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            (0, None, None),
            (1, None, None),
            (2, None, None),
            (3, None, None),
            (2, Some(first_feature.as_str()), Some("feature")),
        ]
    );
    assert_eq!(
        main_records[0].tables,
        vec![legacy_pinned(&person, 1, 1), legacy_pinned(&company, 1, 1)]
    );
    assert_eq!(
        main_records[1].tables,
        vec![legacy_pinned(&person, 2, 2), legacy_pinned(&firm, 1, 1)]
    );
    let after_reuse = vec![
        legacy_pinned(&person, 2, 2),
        legacy_pinned(&firm, 1, 1),
        legacy_pinned(&reused, 1, 3),
    ];
    assert_eq!(main_records[2].tables, after_reuse);
    assert_eq!(main_records[3].tables, after_reuse);
    assert_eq!(
        retired.records[0].tables,
        vec![legacy_pinned(&person, 3, 3), legacy_pinned(&firm, 1, 1)]
    );

    let [first_hash, edited_hash] =
        [&contract, &edited].map(|row| super::history::schema_content_hash(row).unwrap());
    assert_ne!(first_hash, edited_hash);
    let mut archived = vec![(&first_hash, &contract), (&edited_hash, &edited)];
    archived.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(
        census.contracts.iter().collect::<Vec<_>>(),
        archived.into_iter().map(|(_, row)| row).collect::<Vec<_>>()
    );
    assert_eq!(
        main_records
            .iter()
            .chain(&retired.records)
            .map(|record| record.commit.schema_content_hash.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec![
            first_hash.as_str(),
            first_hash.as_str(),
            edited_hash.as_str(),
            edited_hash.as_str(),
            first_hash.as_str()
        ]
    );
    assert_eq!(
        main_records[3].commit.merged_parent_commit_id,
        Some(dangling.clone())
    );
    assert_eq!(
        census.absent_parents.iter().collect::<Vec<_>>(),
        vec![&ulid::Ulid::from(99u128)]
    );

    let head_of = |native: Option<&str>| {
        let head = census
            .heads
            .iter()
            .find(|head| head.native.as_deref() == native)
            .unwrap();
        (head.version, &head.record)
    };
    assert_eq!(census.heads.len(), 4);
    assert_eq!(census.heads[0].native, None);
    assert_eq!(head_of(None), (4, &main_records[3]));
    assert_eq!(head_of(Some(&fresh)), (3, &main_records[2]));
    assert_eq!(head_of(Some(&child)), (3, &retired.records[0]));
    assert_eq!(head_of(Some(&second_feature)), (3, &retired.records[0]));
    assert_eq!(
        (
            census.counts.live_refs,
            census.counts.retired_refs,
            census.counts.orphan_writers,
            census.counts.legacy_commits,
            census.counts.bookkeeping_versions,
            census.counts.absent_parents,
            census.counts.schema_contents,
        ),
        (4, 1, 0, 5, 0, 1, 2)
    );

    let layout = LegacyLayout::CURRENT;
    let plan = super::legacy::plan(&census, &layout).unwrap();
    let again = legacy_census(&history, &[]).await.unwrap();
    assert_eq!(again, census);
    assert_eq!(super::legacy::plan(&again, &layout).unwrap(), plan);
    assert_eq!(
        (
            plan.commits,
            plan.data_files,
            plan.id_shards,
            plan.writer_shards
        ),
        (5, 2, 1, 1)
    );
    let json = serde_json::to_string(&plan).unwrap();
    assert_eq!(serde_json::from_str::<LegacyPlan>(&json).unwrap(), plan);
    let one_per_file = LegacyLayout {
        block_slots: 1,
        ..layout
    };
    let cut = super::legacy::plan(&census, &one_per_file).unwrap();
    assert_eq!(cut.data_files, 5);
    assert_ne!(cut.directory_sha256, plan.directory_sha256);

    let session = crate::lance_access::control_session();
    let (code, message) = legacy_finding(
        super::legacy::materialize(
            root,
            &session,
            &census,
            &LegacyPlan {
                layout,
                ..cut.clone()
            },
        )
        .await,
    );
    assert_eq!(code, FindingCode::PlanChanged, "{message}");
    assert!(
        super::history::legacy_directory(root, &session, None)
            .await
            .unwrap()
            .is_none(),
        "a plan that differs writes nothing"
    );
    for _ in 0..2 {
        super::legacy::materialize(root, &session, &census, &plan)
            .await
            .unwrap();
    }
    let directory = super::history::legacy_directory(root, &session, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(directory.encode().unwrap())
        ),
        plan.directory_sha256
    );
    assert_eq!(directory.absent_parents, vec![ulid::Ulid::from(99u128)]);
    assert_eq!(
        history_records(root).await,
        sorted_history_records(
            census
                .chains
                .iter()
                .flat_map(|chain| chain.records.clone())
                .collect()
        )
    );

    let metadata = history.head(None).unwrap().schema().metadata.clone();
    for (native, own_head) in [(None, true), (Some(child.as_str()), false)] {
        let (version, record) = head_of(native);
        let stale = super::legacy::conversion_batch(root, &session, record, metadata.clone())
            .await
            .unwrap_err();
        assert!(
            stale
                .to_string()
                .contains("conversion rows are written under stamp 14"),
            "{stale}"
        );
        let mut converted = metadata.clone();
        converted.insert(
            super::migrations::INTERNAL_SCHEMA_VERSION_KEY.to_string(),
            "14".to_string(),
        );
        let (schema, batch) = super::legacy::conversion_batch(root, &session, record, converted)
            .await
            .unwrap();
        assert_eq!(batch.schema(), schema);
        let rows = super::state::rows_of_batch(&batch).unwrap();
        assert_eq!(
            (&rows.head, &rows.tables, rows.buffer.commits().len()),
            (&record.commit, &record.tables, 0)
        );
        let expected = if own_head { &edited } else { &contract };
        assert_eq!(rows.schema_contract.as_ref(), Some(expected));
        let state = rows.state(version, native).unwrap();
        assert_eq!(
            state.graph_heads,
            if own_head {
                HashMap::from([("main".to_string(), c4.clone())])
            } else {
                HashMap::new()
            }
        );
    }
}

/// The source `Stamp13Source` reads, less one commit row of every head.
struct LegacySourceWithout(String);

#[async_trait]
impl super::legacy::LegacyManifestSource for LegacySourceWithout {
    fn admits(&self, dataset: &Dataset, role: super::legacy::SourceRole) -> Result<u32> {
        super::legacy::Stamp13Source.admits(dataset, role)
    }

    async fn scan_head(&self, dataset: &Dataset) -> Result<super::legacy::HeadScan> {
        let mut scan = super::legacy::Stamp13Source.scan_head(dataset).await?;
        scan.commits
            .retain(|commit| commit.graph_commit_id != self.0);
        Ok(scan)
    }

    async fn version_schema(
        &self,
        dataset: &Dataset,
        previous: Option<&super::legacy::VersionSchema>,
    ) -> Result<super::legacy::VersionSchema> {
        super::legacy::Stamp13Source
            .version_schema(dataset, previous)
            .await
    }
}

/// A commit only inherited, whose writer no ref is any more, is adopted once
/// under an orphaned writer named by its logical branch. A first parent no
/// ref holds, and an id that is not a ULID, refuse the census.
#[tokio::test]
async fn legacy_census_adopts_orphans_and_refuses_a_first_parent_gap() {
    use super::history::LegacyWriterKind;
    use super::legacy::FindingCode;
    use super::legacy::write::LegacyPublish;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2, b3, k4] = [1, 2, 3, 4].map(legacy_id);
    let (mut history, [person, _, firm], _) = legacy_main_with_ids(root, [&g, &c2]).await;
    let gone = legacy_native("gone");
    history.fork(None, &gone).await.unwrap();
    history
        .publish(
            Some(&gone),
            LegacyPublish {
                pins: vec![legacy_pin(&person, 3)],
                commit: Some(legacy_commit(&b3, 3)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let keeper = legacy_native("keeper");
    assert_eq!(history.fork(Some(&gone), &keeper).await.unwrap(), 3);
    history
        .publish(
            Some(&keeper),
            LegacyPublish {
                commit: Some(legacy_commit(&k4, 4)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let census = legacy_census(&history, &[&gone]).await.unwrap();
    assert_eq!(
        census
            .chains
            .iter()
            .map(|chain| (
                chain.kind,
                chain.native.as_deref(),
                legacy_record_ids(chain)
            ))
            .collect::<Vec<_>>(),
        vec![
            (LegacyWriterKind::Main, None, vec![g.as_str(), c2.as_str()]),
            (
                LegacyWriterKind::Live,
                Some(keeper.as_str()),
                vec![k4.as_str()]
            ),
            (LegacyWriterKind::Orphaned, Some("gone"), vec![b3.as_str()]),
        ]
    );
    let orphan = &census.chains[2].records[0];
    assert_eq!(
        (
            orphan.commit.native_branch.as_deref(),
            orphan.commit.graph_branch.as_deref(),
            orphan.commit.generation,
            orphan.commit.parent_commit_id.as_deref(),
        ),
        (Some("gone"), Some("gone"), 2, Some(c2.as_str()))
    );
    let pinned_on_gone = vec![legacy_pinned(&person, 3, 3), legacy_pinned(&firm, 1, 1)];
    assert_eq!(orphan.tables, pinned_on_gone);
    let kept = &census.chains[1].records[0];
    assert_eq!(
        (
            kept.commit.generation,
            kept.commit.parent_commit_id.as_deref()
        ),
        (3, Some(b3.as_str()))
    );
    assert_eq!(kept.tables, pinned_on_gone);
    assert_eq!(census.counts.orphan_writers, 1);
    let plan = super::legacy::plan(&census, &super::history::LegacyLayout::CURRENT).unwrap();
    let session = crate::lance_access::control_session();
    super::legacy::materialize(root, &session, &census, &plan)
        .await
        .unwrap();
    let cache = super::history::ExtentCache::default();
    for cache in [None, Some(&cache)] {
        assert_eq!(
            super::legacy::record_at(root, &session, cache, Some("gone"), 3, false)
                .await
                .unwrap(),
            super::legacy::LegacyAt::Exact(orphan.clone())
        );
        let below = super::legacy::record_at(root, &session, cache, Some("gone"), 2, false)
            .await
            .unwrap_err();
        assert!(
            matches!(below, OmniError::Manifest(ref manifest)
                if manifest.kind == crate::error::ManifestErrorKind::NotFound),
            "{below:?}"
        );
        assert!(
            below.to_string().contains(
                "version 2 of the deleted branch 'gone' is below the first graph commit the \
                 legacy history kept of it, and the ref it forked from is unknown"
            ),
            "{below}"
        );
    }

    let input = legacy_census_input(&history, &[&gone]).await;
    let (code, message) =
        legacy_finding(super::legacy::census(root, &input, &LegacySourceWithout(b3.clone())).await);
    assert_eq!(code, FindingCode::LineageIncomplete, "{message}");
    assert_eq!(
        message,
        format!("1 first parents are held by no ref: '{b3}' (first parent of '{k4}')")
    );

    let dir = tempfile::tempdir().unwrap();
    let (named, _, _) = legacy_main(dir.path().to_str().unwrap()).await;
    let (code, message) = legacy_finding(legacy_census(&named, &[]).await);
    assert_eq!(code, FindingCode::UnsupportedSource);
    assert!(message.contains("is not a canonical ULID"), "{message}");
}

/// Versions without a commit, of older stamps too, are verified against their
/// nearest commit; a commit of a version without a contract row is recorded
/// under its ref's head contract; an uncommitted pin or registration refuses.
#[tokio::test]
async fn legacy_census_verifies_versions_without_a_commit() {
    use super::legacy::FindingCode;
    use super::legacy::write::{LegacyPublish, Stamp13History};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c4, c7] = [1, 2, 3].map(legacy_id);
    let person = legacy_table(1, "node:Person");
    let born = legacy_contract("node Person {}");
    let mut history = Stamp13History::create(
        root,
        LegacyPublish {
            tables: vec![person.clone()],
            contract: Some(born.clone()),
            pins: vec![legacy_pin(&person, 1)],
            commit: Some(legacy_commit(&g, 1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(history.restamp_for_test(None, None).await.unwrap(), 2);
    assert_eq!(history.restamp_for_test(None, Some(6)).await.unwrap(), 3);
    let commit = |id: &str, created_at| LegacyPublish {
        commit: Some(legacy_commit(id, created_at)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, commit(&c4, 4)).await.unwrap(), 4);
    assert_eq!(history.restamp_for_test(None, Some(12)).await.unwrap(), 5);
    let converted = legacy_contract("node Person { name: String }");
    let contract_only = LegacyPublish {
        contract: Some(converted.clone()),
        ..Default::default()
    };
    assert_eq!(history.publish(None, contract_only).await.unwrap(), 6);
    assert_eq!(history.publish(None, commit(&c7, 7)).await.unwrap(), 7);

    let census = legacy_census(&history, &[]).await.unwrap();
    let records = &census.chains[0].records;
    assert_eq!(legacy_record_ids(&census.chains[0]), [&g, &c4, &c7]);
    let [born_hash, converted_hash] =
        [&born, &converted].map(|row| super::history::schema_content_hash(row).unwrap());
    assert_eq!(
        records
            .iter()
            .map(|record| record.commit.schema_content_hash.clone().unwrap())
            .collect::<Vec<_>>(),
        vec![born_hash, converted_hash.clone(), converted_hash]
    );
    for record in records {
        assert_eq!(record.tables, vec![legacy_pinned(&person, 1, 1)]);
    }
    assert_eq!(
        (
            census.counts.bookkeeping_versions,
            census.counts.pre_genesis_versions,
            census.counts.census_reads,
            census.counts.schema_contents,
        ),
        (4, 0, 8, 2)
    );
    assert_eq!(census.heads[0].record, records[2]);

    let pin_only = LegacyPublish {
        pins: vec![legacy_pin(&person, 2)],
        ..Default::default()
    };
    assert_eq!(history.publish(None, pin_only).await.unwrap(), 8);
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::UncommittedChange);
    assert_eq!(
        message,
        format!(
            "version 8 of main holds no graph commit, yet a table pin or drop changed since \
             graph commit '{c7}' at version 7"
        )
    );

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let (mut history, _, _) = legacy_main_with_ids(root, [&g, &c4]).await;
    let branch = legacy_native("feature");
    history.fork(None, &branch).await.unwrap();
    let registration = LegacyPublish {
        tables: vec![legacy_table(9, "node:Late")],
        ..Default::default()
    };
    assert_eq!(
        history.publish(Some(&branch), registration).await.unwrap(),
        3
    );
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::UncommittedChange);
    assert_eq!(
        message,
        format!(
            "version 3 of ref '{branch}' holds no graph commit, yet the table membership or an \
             alias changed since graph commit '{c4}' at version 2"
        )
    );
}

/// A census over the cell bound (before any version read), a directory over
/// one read block, a head contract its head commit does not carry, and a
/// record over the commit-field bound each refuse with their own code.
#[tokio::test]
async fn legacy_census_refuses_over_its_bounds_and_on_a_head_that_differs_from_its_record() {
    use super::legacy::FindingCode;
    use super::legacy::write::{LegacyCommitIntent, LegacyPublish};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2, c3] = [1, 2, 3].map(legacy_id);
    let (mut history, _, _) = legacy_main_with_ids(root, [&g, &c2]).await;

    let input = legacy_census_input(&history, &[]).await;
    let probes = crate::instrumentation::QueryIoProbes::default();
    let scans = probes.manifest_scan_count.clone();
    let bounded = crate::instrumentation::with_query_io_probes(
        probes,
        super::legacy::census_within(
            root,
            &input,
            &super::legacy::Stamp13Source,
            15,
            super::legacy::MAX_CENSUS_SNAPSHOT_BYTES,
        ),
    )
    .await;
    let (code, message) = legacy_finding(bounded);
    assert_eq!(code, FindingCode::CensusOverBound);
    assert_eq!(
        message,
        "reading every retained version would scan 16 cells of the object_type column, above \
         the bound of 15; no version was read"
    );
    assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 1);
    let census = super::legacy::census_within(
        root,
        &input,
        &super::legacy::Stamp13Source,
        16,
        super::legacy::MAX_CENSUS_SNAPSHOT_BYTES,
    )
    .await
    .unwrap();
    assert_eq!(census.counts.census_cells, 16);

    let mut wide = census.clone();
    wide.absent_parents = (0..40_000u128).map(ulid::Ulid::from).collect();
    let (code, message) = legacy_finding(super::legacy::plan(
        &wide,
        &super::history::LegacyLayout::CURRENT,
    ));
    assert_eq!(code, FindingCode::DirectoryOverBound);
    assert!(
        message.contains("bytes of the `__history` legacy directory, above the bound of 524288"),
        "{message}"
    );

    let replaced = LegacyPublish {
        contract: Some(legacy_contract("node Person { age: I32 }")),
        ..Default::default()
    };
    assert_eq!(history.publish(None, replaced).await.unwrap(), 3);
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::HeadRecordMismatch);
    assert_eq!(
        message,
        format!(
            "main at its head version 3 differs from the record of its head graph commit \
             '{c2}' in the schema contract"
        )
    );

    let long = LegacyPublish {
        commit: Some(LegacyCommitIntent {
            actor_id: Some("a".repeat(crate::HISTORY_RELEASE_BYTES)),
            ..legacy_commit(&c3, 4)
        }),
        ..Default::default()
    };
    assert_eq!(history.publish(None, long).await.unwrap(), 4);
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::RecordOverBound);
    assert!(
        message.contains(&format!("graph commit '{c3}' records"))
            && message.contains("at most 262144 bytes of commit fields"),
        "{message}"
    );
}

/// [`super::legacy::Stamp13Source`] with a head scan that counts no `table`
/// row of [`legacy_main`], as a head that lost its registrations would.
struct HeadWithoutTables;

#[async_trait::async_trait]
impl super::legacy::LegacyManifestSource for HeadWithoutTables {
    fn admits(&self, dataset: &lance::Dataset, role: super::legacy::SourceRole) -> Result<u32> {
        super::legacy::Stamp13Source.admits(dataset, role)
    }

    async fn scan_head(&self, dataset: &lance::Dataset) -> Result<super::legacy::HeadScan> {
        let mut scan = super::legacy::Stamp13Source.scan_head(dataset).await?;
        scan.rows -= 2;
        Ok(scan)
    }

    async fn version_schema(
        &self,
        dataset: &lance::Dataset,
        previous: Option<&super::legacy::VersionSchema>,
    ) -> Result<super::legacy::VersionSchema> {
        super::legacy::Stamp13Source
            .version_schema(dataset, previous)
            .await
    }
}

/// Two commits and the head of main keep two table rows each, its first
/// version two registrations. One byte under the six rows refuses from the head
/// scan alone; a head scan that counts no table refuses at the record over it.
#[tokio::test]
async fn legacy_census_refuses_table_snapshots_over_the_bound_before_any_version_read() {
    use super::legacy::{FindingCode, MAX_CENSUS_CELLS, Stamp13Source, census_within};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2] = [1, 2].map(legacy_id);
    let (history, _, _) = legacy_main_with_ids(root, [&g, &c2]).await;
    let input = legacy_census_input(&history, &[]).await;
    let rows = 6 * size_of::<TableRow>() as u64;
    let bytes = rows + 2 * size_of::<TableRegistration>() as u64;
    let bound = bytes - 1;
    const REBUILD: &str = "rebuild the graph with the build that wrote it";

    let probes = crate::instrumentation::QueryIoProbes::default();
    let scans = probes.manifest_scan_count.clone();
    let refused = crate::instrumentation::with_query_io_probes(
        probes,
        census_within(root, &input, &Stamp13Source, MAX_CENSUS_CELLS, rows - 1),
    )
    .await;
    let (code, message) = legacy_finding(refused);
    assert_eq!(code, FindingCode::CensusOverBound);
    assert_eq!(
        message,
        format!(
            "no version was read: the records of 2 graph commits and the heads of 1 refs would \
             retain 6 table rows, {rows} bytes of table snapshots, above the bound of {}; \
             {REBUILD}",
            rows - 1
        )
    );
    assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 1);

    let census = census_within(root, &input, &Stamp13Source, MAX_CENSUS_CELLS, bytes)
        .await
        .unwrap();
    assert_eq!(census.counts.legacy_commits, 2);
    assert_eq!(
        census.chains[0]
            .records
            .iter()
            .map(|record| record.tables.len())
            .sum::<usize>()
            + census.heads[0].record.tables.len(),
        6
    );

    let late = census_within(root, &input, &HeadWithoutTables, MAX_CENSUS_CELLS, bound).await;
    let (code, message) = legacy_finding(late);
    assert_eq!(code, FindingCode::CensusOverBound);
    assert_eq!(
        message,
        format!(
            "the records and heads read so far retain 6 table rows and 2 table registrations, \
             {bytes} bytes of table snapshots, above the bound of {bound}; {REBUILD}"
        )
    );
}

/// Every version of a fork below its first own commit keeps its registrations
/// until the records exist: two such versions of `feature` are charged with
/// its first version and its head, and one byte under their sum refuses.
#[tokio::test]
async fn legacy_census_charges_the_registrations_of_versions_without_a_commit() {
    use super::legacy::write::LegacyPublish;
    use super::legacy::{FindingCode, MAX_CENSUS_CELLS, Stamp13Source, census_within};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2] = [1, 2].map(legacy_id);
    let (mut history, _, contract) = legacy_main_with_ids(root, [&g, &c2]).await;
    let feature = legacy_native("feature");
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    for version in [3, 4] {
        let contract_only = LegacyPublish {
            contract: Some(contract.clone()),
            ..Default::default()
        };
        assert_eq!(
            history
                .publish(Some(&feature), contract_only)
                .await
                .unwrap(),
            version
        );
    }
    let input = legacy_census_input(&history, &[]).await;
    let bytes = 8 * size_of::<TableRow>() as u64 + 8 * size_of::<TableRegistration>() as u64;

    let census = census_within(root, &input, &Stamp13Source, MAX_CENSUS_CELLS, bytes)
        .await
        .unwrap();
    assert_eq!(
        (
            census.counts.legacy_commits,
            census.counts.bookkeeping_versions
        ),
        (2, 2)
    );

    let refused = census_within(root, &input, &Stamp13Source, MAX_CENSUS_CELLS, bytes - 1).await;
    let (code, message) = legacy_finding(refused);
    assert_eq!(code, FindingCode::CensusOverBound);
    assert_eq!(
        message,
        format!(
            "the records and heads read so far retain 8 table rows and 8 table registrations, \
             {bytes} bytes of table snapshots, above the bound of {}; rebuild the graph with the \
             build that wrote it",
            bytes - 1
        )
    );
}

fn legacy_route_intent(protocol: u32, source_format: u32, target_format: u32) -> String {
    format!(
        r#"{{"protocol":{protocol},"attempt":"{}","source_format":{source_format},"target_format":{target_format},"graph_identity":"domain","branches":[{{"native":null,"version":2}}]}}"#,
        legacy_id(9)
    )
}

/// Main as a root born before stamp 13 and upgraded 12 to 13: genesis `g` at
/// version 1 without a contract row, a stamp-12 version 2, the fence at 3,
/// `conversion` under the pending key at 4 and the activation at 5.
async fn legacy_upgraded_main(
    root: &str,
    g: &str,
    person: &TableRegistration,
    conversion: super::legacy::write::LegacyPublish,
) -> super::legacy::write::Stamp13History {
    use super::legacy::write::{LegacyPublish, Stamp13History};
    let mut history = Stamp13History::create(
        root,
        LegacyPublish {
            tables: vec![person.clone()],
            pins: vec![legacy_pin(person, 1)],
            commit: Some(legacy_commit(g, 1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(history.restamp_for_test(None, Some(12)).await.unwrap(), 2);
    let intent = legacy_route_intent(5, 12, 13);
    assert_eq!(history.fence_for_test(&intent, 13).await.unwrap(), 3);
    assert_eq!(history.publish(None, conversion).await.unwrap(), 4);
    assert_eq!(history.activate_for_test().await.unwrap(), 5);
    history
}

/// The fence and the conversion an earlier route left on main carry its
/// pending key. The fence reads in its source's shape, the conversion in its
/// target's, and both are verified against their nearest commit.
#[tokio::test]
async fn legacy_census_reads_through_the_fence_and_conversion_of_an_earlier_route() {
    use super::legacy::write::LegacyPublish;
    use super::legacy::{
        FindingCode, LegacyManifestSource, SourceRole, Stamp13Source, VersionSchema,
    };
    let source = Stamp13Source;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c6] = [1, 2].map(legacy_id);
    let person = legacy_table(1, "node:Person");
    let converted = legacy_contract("node Person { name: String }");
    let contract_only = LegacyPublish {
        contract: Some(converted.clone()),
        ..Default::default()
    };
    let mut history = legacy_upgraded_main(root, &g, &person, contract_only.clone()).await;
    let commit = LegacyPublish {
        commit: Some(legacy_commit(&c6, 6)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, commit).await.unwrap(), 6);

    let main = history.head(None).unwrap();
    let fence = main.checkout_version(3).await.unwrap();
    let conversion = main.checkout_version(4).await.unwrap();
    for keyed in [&fence, &conversion] {
        assert!(
            keyed
                .schema()
                .metadata
                .contains_key(super::migrations::UPGRADE_PENDING_KEY)
        );
        for role in [SourceRole::LiveHead, SourceRole::RetiredHead] {
            let error = source.admits(keyed, role).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("carries a pending storage conversion"),
                "{role:?}: {error}"
            );
        }
    }
    assert_eq!(source.admits(&fence, SourceRole::Version).unwrap(), 12);
    assert_eq!(source.admits(&conversion, SourceRole::Version).unwrap(), 13);
    assert_eq!(
        source.version_schema(&fence, None).await.unwrap(),
        VersionSchema {
            tables: vec![person.clone()],
            contract: None,
        }
    );
    assert_eq!(
        source.version_schema(&conversion, None).await.unwrap(),
        VersionSchema {
            tables: vec![person.clone()],
            contract: Some(converted.clone()),
        }
    );

    let census = legacy_census(&history, &[]).await.unwrap();
    let records = &census.chains[0].records;
    assert_eq!(legacy_record_ids(&census.chains[0]), [&g, &c6]);
    let converted_hash = super::history::schema_content_hash(&converted).unwrap();
    for record in records {
        assert_eq!(record.tables, vec![legacy_pinned(&person, 1, 1)]);
        assert_eq!(
            record.commit.schema_content_hash.as_ref(),
            Some(&converted_hash)
        );
    }
    assert_eq!(
        (
            census.counts.bookkeeping_versions,
            census.counts.pre_genesis_versions,
            census.counts.census_reads,
            census.counts.schema_contents,
        ),
        (4, 0, 7, 1)
    );
    assert_eq!(census.heads[0].record, records[1]);

    let dir = tempfile::tempdir().unwrap();
    let pinning = LegacyPublish {
        pins: vec![legacy_pin(&person, 2)],
        ..contract_only
    };
    let history = legacy_upgraded_main(dir.path().to_str().unwrap(), &g, &person, pinning).await;
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::UncommittedChange);
    assert_eq!(
        message,
        format!(
            "version 4 of main holds no graph commit, yet a table pin or drop changed since \
             graph commit '{g}' at version 1"
        )
    );

    let dir = tempfile::tempdir().unwrap();
    let (mut history, _, _) = legacy_main(dir.path().to_str().unwrap()).await;
    for (intent, target, refusal) in [
        (
            legacy_route_intent(6, 13, 14),
            13,
            "version 3 carries a pending storage conversion (omnigraph:storage_upgrade_pending) \
             that is not the fence or the conversion of a completed earlier upgrade: its intent \
             names protocol 6 from v13 to v14, which no released upgrade ran",
        ),
        (
            legacy_route_intent(5, 11, 13),
            12,
            "version 4 carries a pending storage conversion (omnigraph:storage_upgrade_pending) \
             that is not the fence or the conversion of a completed earlier upgrade: its intent \
             targets v13 and the version is stamped Some(12)",
        ),
        (
            legacy_route_intent(2, 7, 8),
            8,
            "version 5 carries a pending storage conversion (omnigraph:storage_upgrade_pending) \
             that is not the fence or the conversion of a completed earlier upgrade: its rows \
             are stored neither as v8 nor as v7 stores them",
        ),
    ] {
        history.fence_for_test(&intent, target).await.unwrap();
        let error = source
            .admits(history.head(None).unwrap(), SourceRole::Version)
            .unwrap_err();
        assert!(error.to_string().contains(refusal), "{error}");
    }
    let (code, message) = legacy_finding(legacy_census(&history, &[]).await);
    assert_eq!(code, FindingCode::UnsupportedSource);
    assert!(
        message.contains("version 5 carries a pending storage conversion"),
        "{message}"
    );
}

/// A fork without a commit of its own inherits a head written before its
/// source held a contract row. That head is recorded under the first contract
/// the source stored after it, which the fork holds, not under a later one.
#[tokio::test]
async fn legacy_census_records_a_contractless_head_under_the_contract_its_forks_hold() {
    use super::legacy::write::{LegacyPublish, Stamp13History};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c3, c5] = [1, 2, 3].map(legacy_id);
    let person = legacy_table(1, "node:Person");
    let born = legacy_contract("node Person {}");
    let converted = legacy_contract("node Person { name: String }");
    let changed = legacy_contract("node Person { name: String, age: I32 }");
    let mut history = Stamp13History::create(
        root,
        LegacyPublish {
            tables: vec![person.clone()],
            contract: Some(born.clone()),
            pins: vec![legacy_pin(&person, 1)],
            commit: Some(legacy_commit(&g, 1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(history.restamp_for_test(None, Some(12)).await.unwrap(), 2);
    let publish =
        |contract: Option<&SchemaContractRow>, commit: Option<(&str, i64)>| LegacyPublish {
            contract: contract.cloned(),
            commit: commit.map(|(id, created_at)| legacy_commit(id, created_at)),
            ..Default::default()
        };
    assert_eq!(
        history
            .publish(None, publish(None, Some((c3.as_str(), 3))))
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        history
            .publish(None, publish(Some(&converted), None))
            .await
            .unwrap(),
        4
    );
    let fork = legacy_native("fork");
    assert_eq!(history.fork(None, &fork).await.unwrap(), 4);
    assert_eq!(
        history
            .publish(None, publish(Some(&changed), Some((c5.as_str(), 5))))
            .await
            .unwrap(),
        5
    );

    let census = legacy_census(&history, &[]).await.unwrap();
    let records = &census.chains[0].records;
    assert_eq!(legacy_record_ids(&census.chains[0]), [&g, &c3, &c5]);
    assert_eq!(
        records
            .iter()
            .map(|record| record.commit.schema_content_hash.clone().unwrap())
            .collect::<Vec<_>>(),
        [&born, &converted, &changed].map(|row| super::history::schema_content_hash(row).unwrap())
    );
    let forked = &census.heads[1];
    assert_eq!(
        (forked.native.as_deref(), forked.version, &forked.record),
        (Some(fork.as_str()), 4, &records[1])
    );

    let session = crate::lance_access::control_session();
    let plan = super::legacy::plan(&census, &super::history::LegacyLayout::CURRENT).unwrap();
    super::legacy::materialize(root, &session, &census, &plan)
        .await
        .unwrap();
    let mut metadata = history.head(Some(&fork)).unwrap().schema().metadata.clone();
    metadata.insert(
        super::migrations::INTERNAL_SCHEMA_VERSION_KEY.to_string(),
        "14".to_string(),
    );
    let (_, batch) = super::legacy::conversion_batch(root, &session, &forked.record, metadata)
        .await
        .unwrap();
    assert_eq!(
        super::state::rows_of_batch(&batch).unwrap().schema_contract,
        Some(converted)
    );
}

/// The source `Stamp13Source` reads, with every head scan rewritten.
struct LegacySourceRewriting<F>(F);

#[async_trait]
impl<F> super::legacy::LegacyManifestSource for LegacySourceRewriting<F>
where
    F: Fn(&mut super::legacy::HeadScan) + Send + Sync,
{
    fn admits(&self, dataset: &Dataset, role: super::legacy::SourceRole) -> Result<u32> {
        super::legacy::Stamp13Source.admits(dataset, role)
    }

    async fn scan_head(&self, dataset: &Dataset) -> Result<super::legacy::HeadScan> {
        let mut scan = super::legacy::Stamp13Source.scan_head(dataset).await?;
        (self.0)(&mut scan);
        Ok(scan)
    }

    async fn version_schema(
        &self,
        dataset: &Dataset,
        previous: Option<&super::legacy::VersionSchema>,
    ) -> Result<super::legacy::VersionSchema> {
        super::legacy::Stamp13Source
            .version_schema(dataset, previous)
            .await
    }
}

async fn legacy_census_rewriting(
    history: &super::legacy::write::Stamp13History,
    rewrite: impl Fn(&mut super::legacy::LegacyCommit) + Send + Sync,
) -> (super::legacy::FindingCode, String) {
    let input = legacy_census_input(history, &[]).await;
    let source = LegacySourceRewriting(|scan: &mut super::legacy::HeadScan| {
        scan.commits.iter_mut().for_each(&rewrite)
    });
    legacy_finding(super::legacy::census(history.root(), &input, &source).await)
}

/// A merge takes the greatest generation of its two parents plus one. Commit
/// rows that contradict their refs are refused as corrupt, naming the commit:
/// a foreign branch, a shared version, two owners, a cycle, a stale head row.
#[tokio::test]
async fn legacy_census_walks_a_merge_and_refuses_a_corrupt_lineage() {
    use super::legacy::FindingCode;
    use super::legacy::write::{LegacyCommitIntent, LegacyPublish};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2, f3, f4, c3, c4] = [1, 2, 3, 4, 5, 6].map(legacy_id);
    let (mut history, _, _) = legacy_main_with_ids(root, [&g, &c2]).await;
    let feature = legacy_native("feature");
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    let commit = |id: &str, created_at: i64, merged: Option<&str>| LegacyPublish {
        commit: Some(LegacyCommitIntent {
            merged_parent_commit_id: merged.map(str::to_string),
            ..legacy_commit(id, created_at)
        }),
        ..Default::default()
    };
    for (native, publish, version) in [
        (Some(feature.as_str()), commit(&f3, 3, None), 3),
        (Some(feature.as_str()), commit(&f4, 4, None), 4),
        (None, commit(&c3, 3, None), 3),
        (None, commit(&c4, 4, Some(&f4)), 4),
    ] {
        assert_eq!(history.publish(native, publish).await.unwrap(), version);
    }

    let census = legacy_census(&history, &[]).await.unwrap();
    assert_eq!(
        census
            .chains
            .iter()
            .map(|chain| chain
                .records
                .iter()
                .map(|record| (
                    record.commit.graph_commit_id.as_str(),
                    record.commit.generation
                ))
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![
                (g.as_str(), 0),
                (c2.as_str(), 1),
                (c3.as_str(), 2),
                (c4.as_str(), 4)
            ],
            vec![(f3.as_str(), 2), (f4.as_str(), 3)],
        ]
    );
    assert!(census.absent_parents.is_empty());

    let corrupt = |(code, message): (FindingCode, String)| {
        assert_eq!(code, FindingCode::LineageCorrupt, "{message}");
        message
    };
    assert_eq!(
        corrupt(
            legacy_census_rewriting(&history, |commit| {
                if commit.graph_commit_id == c2 {
                    commit.graph_branch = Some("other".to_string());
                }
            })
            .await
        ),
        format!(
            "graph commit '{c2}' names branch Some(\"other\") and version 2; main wrote every \
             commit above its fork point 0 as branch None, up to its head 4"
        )
    );
    assert_eq!(
        corrupt(
            legacy_census_rewriting(&history, |commit| {
                if commit.graph_commit_id == c2 {
                    commit.graph_manifest_version = 1;
                }
            })
            .await
        ),
        format!("version 1 of main holds two graph commits, '{g}' and '{c2}'")
    );
    assert_eq!(
        corrupt(
            legacy_census_rewriting(&history, |commit| {
                if commit.graph_commit_id == f3 {
                    commit.graph_commit_id = c2.clone();
                }
            })
            .await
        ),
        format!("graph commit '{c2}' is an own commit of two refs")
    );
    assert_eq!(
        corrupt(
            legacy_census_rewriting(&history, |commit| {
                if commit.graph_commit_id == g {
                    commit.parent_commit_id = Some(c2.clone());
                }
            })
            .await
        ),
        format!("graph commit '{g}' is its own ancestor")
    );
    assert_eq!(
        corrupt(
            legacy_census_rewriting(&history, |commit| {
                if commit.graph_commit_id == c2 {
                    commit.parent_commit_id = Some(c2.clone());
                }
            })
            .await
        ),
        format!("graph commit '{c2}' is its own ancestor")
    );

    let input = legacy_census_input(&history, &[]).await;
    let stale_head = LegacySourceRewriting(|scan: &mut super::legacy::HeadScan| {
        scan.heads
            .insert(super::MAIN_BRANCH_HEAD_KEY.to_string(), c3.clone());
    });
    assert_eq!(
        corrupt(legacy_finding(
            super::legacy::census(root, &input, &stale_head).await
        )),
        format!(
            "the graph_head row of main names its own commit '{c3}', its lineage ends at '{c4}'"
        )
    );
}

/// A ref whose head is its own commit names exactly that commit in its
/// graph_head row; a fork that wrote no commit holds no row and is converted
/// under the head it inherits.
#[tokio::test]
async fn legacy_census_refuses_an_own_head_its_graph_head_row_does_not_name() {
    use super::legacy::write::LegacyPublish;
    use super::legacy::{FindingCode, HeadScan, LegacyManifestSource, Stamp13Source};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2, f3] = [1, 2, 3].map(legacy_id);
    let (mut history, _, _) = legacy_main_with_ids(root, [&g, &c2]).await;
    let [feature, fresh] = ["feature", "fresh"].map(legacy_native);
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    let on_feature = LegacyPublish {
        commit: Some(legacy_commit(&f3, 3)),
        ..Default::default()
    };
    assert_eq!(
        history.publish(Some(&feature), on_feature).await.unwrap(),
        3
    );
    assert_eq!(history.fork(Some(&feature), &fresh).await.unwrap(), 3);

    let inherited = Stamp13Source
        .scan_head(history.head(Some(&fresh)).unwrap())
        .await
        .unwrap();
    assert!(!inherited.heads.contains_key("fresh"), "{inherited:?}");
    let census = legacy_census(&history, &[]).await.unwrap();
    let forked = census
        .heads
        .iter()
        .find(|head| head.native.as_deref() == Some(fresh.as_str()))
        .unwrap();
    assert_eq!(
        (
            forked.record.commit.graph_commit_id.as_str(),
            forked.record.commit.native_branch.as_deref()
        ),
        (f3.as_str(), Some(feature.as_str()))
    );

    let input = legacy_census_input(&history, &[]).await;
    for (key, pointed, refused) in [
        (
            super::MAIN_BRANCH_HEAD_KEY,
            None,
            format!(
                "main holds no graph_head row for 'main', its lineage ends at its own commit \
                 '{c2}'"
            ),
        ),
        (
            super::MAIN_BRANCH_HEAD_KEY,
            Some(&f3),
            format!(
                "the graph_head row of main names '{f3}', its lineage ends at its own commit \
                 '{c2}'"
            ),
        ),
        (
            "feature",
            None,
            format!(
                "ref '{feature}' holds no graph_head row for 'feature', its lineage ends at its \
                 own commit '{f3}'"
            ),
        ),
        (
            "feature",
            Some(&c2),
            format!(
                "the graph_head row of ref '{feature}' names '{c2}', its lineage ends at its \
                 own commit '{f3}'"
            ),
        ),
    ] {
        let source = LegacySourceRewriting(|scan: &mut HeadScan| match pointed {
            Some(pointed) => drop(scan.heads.insert(key.to_string(), pointed.clone())),
            None => drop(scan.heads.remove(key)),
        });
        let (code, message) = legacy_finding(super::legacy::census(root, &input, &source).await);
        assert_eq!(code, FindingCode::LineageCorrupt, "{message}");
        assert_eq!(message, refused);
    }
}

/// A version below main's first commit that holds no table is counted as
/// pre-genesis; one that holds a pin refuses as an uncommitted change.
#[tokio::test]
async fn legacy_census_counts_an_empty_version_below_the_genesis_commit() {
    use super::legacy::FindingCode;
    use super::legacy::write::{LegacyPublish, Stamp13History};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let [g, c2] = [1, 2].map(legacy_id);
    let person = legacy_table(1, "node:Person");
    let contract_only = LegacyPublish {
        contract: Some(legacy_contract("node Person {}")),
        ..Default::default()
    };
    let mut history = Stamp13History::create(root, contract_only).await.unwrap();
    let genesis = LegacyPublish {
        tables: vec![person.clone()],
        pins: vec![legacy_pin(&person, 1)],
        commit: Some(legacy_commit(&g, 2)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, genesis).await.unwrap(), 2);
    let census = legacy_census(&history, &[]).await.unwrap();
    assert_eq!(legacy_record_ids(&census.chains[0]), [&g]);
    assert_eq!(
        census.chains[0].records[0].tables,
        vec![legacy_pinned(&person, 1, 2)]
    );
    assert_eq!(
        (
            census.counts.pre_genesis_versions,
            census.counts.bookkeeping_versions
        ),
        (1, 1)
    );

    let dir = tempfile::tempdir().unwrap();
    let (history, _, _) = legacy_main_with_ids(dir.path().to_str().unwrap(), [&g, &c2]).await;
    let input = legacy_census_input(&history, &[]).await;
    let without_genesis = LegacySourceRewriting(|scan: &mut super::legacy::HeadScan| {
        scan.commits.retain(|commit| commit.graph_commit_id != g);
        scan.commits
            .iter_mut()
            .for_each(|commit| commit.parent_commit_id = None);
    });
    let (code, message) =
        legacy_finding(super::legacy::census(history.root(), &input, &without_genesis).await);
    assert_eq!(code, FindingCode::UncommittedChange);
    assert_eq!(
        message,
        "version 1 of main holds no graph commit, yet a table pin or drop changed since the \
         creation of the root, before its genesis commit"
    );
}

/// A stamp-13 root with its legacy history written, as the upgrade leaves it
/// before it fences main.
struct LegacyServed {
    history: super::legacy::write::Stamp13History,
    census: super::legacy::LegacyCensus,
    contract: SchemaContractRow,
    feature: String,
    fresh: String,
    gone: String,
    idle: String,
    ids: [String; 5],
}

impl LegacyServed {
    fn record(&self, id: &str) -> HistoryRecord {
        self.census
            .chains
            .iter()
            .flat_map(|chain| &chain.records)
            .find(|record| record.commit.graph_commit_id == id)
            .unwrap()
            .clone()
    }
}

/// Main: `G`@1, `C2`@2, no commit@3, `C4`@4. `feature` forks at 2: no commit@3,
/// `F4`@4; commit-less `fresh` forks from it at 4. Retired: `gone` (fork at 2,
/// `X3`@3) and commit-less `idle`. `ids` are `[G, C2, C4, F4, X3]`.
async fn legacy_served(root: &str) -> LegacyServed {
    use super::legacy::write::LegacyPublish;
    let ids = [1, 2, 3, 4, 5].map(legacy_id);
    let [g, c2, c4, f4, x3] = &ids;
    let (mut history, [person, _, _], contract) = legacy_main_with_ids(root, [g, c2]).await;
    let no_commit = LegacyPublish {
        contract: Some(contract.clone()),
        ..Default::default()
    };
    let committing = |id: &str, table_version: u64| LegacyPublish {
        pins: vec![legacy_pin(&person, table_version)],
        commit: Some(legacy_commit(id, 4)),
        ..Default::default()
    };
    let [feature, fresh, gone, idle] = ["feature", "fresh", "gone", "idle"].map(legacy_native);
    assert_eq!(history.fork(None, &gone).await.unwrap(), 2);
    assert_eq!(
        history
            .publish(Some(&gone), committing(x3, 5))
            .await
            .unwrap(),
        3
    );
    assert_eq!(history.fork(None, &feature).await.unwrap(), 2);
    for native in [Some(feature.as_str()), None] {
        assert_eq!(history.publish(native, no_commit.clone()).await.unwrap(), 3);
    }
    assert_eq!(
        history
            .publish(Some(&feature), committing(f4, 6))
            .await
            .unwrap(),
        4
    );
    assert_eq!(history.publish(None, committing(c4, 7)).await.unwrap(), 4);
    assert_eq!(history.fork(Some(&feature), &fresh).await.unwrap(), 4);
    assert_eq!(history.fork(None, &idle).await.unwrap(), 4);
    history.retire(&idle).await.unwrap();
    history.retire(&gone).await.unwrap();

    let census = legacy_census(&history, &[]).await.unwrap();
    let plan = super::legacy::plan(&census, &super::history::LegacyLayout::CURRENT).unwrap();
    assert_eq!((plan.commits, plan.writer_shards), (5, 1));
    let session = crate::lance_access::control_session();
    super::legacy::materialize(root, &session, &census, &plan)
        .await
        .unwrap();
    LegacyServed {
        history,
        census,
        contract,
        feature,
        fresh,
        gone,
        idle,
        ids,
    }
}

/// A version of a ref resolves to its own commit, to the greatest commit at
/// or below it, or through the ref it forked from; main's fence and conversion
/// versions resolve only when the version carries the pending key.
#[tokio::test]
async fn legacy_record_at_resolves_a_version_by_writer_parent_walk_and_fence() {
    use super::history::ExtentCache;
    use super::legacy::write::{LegacyPublish, Stamp13History};
    use super::legacy::{LegacyAt, record_at};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let served = legacy_served(root).await;
    let [g, c2, c4, f4, x3] = served.ids.each_ref().map(|id| served.record(id));
    let session = crate::lance_access::control_session();
    let cache = ExtentCache::default();
    let (feature, fresh, gone, idle) = (
        served.feature.as_str(),
        served.fresh.as_str(),
        served.gone.as_str(),
        served.idle.as_str(),
    );
    for (native, version, expected) in [
        (None, 1, LegacyAt::Exact(g)),
        (None, 2, LegacyAt::Exact(c2.clone())),
        (None, 3, LegacyAt::Nearest(c2.clone())),
        (None, 4, LegacyAt::Exact(c4.clone())),
        (Some(feature), 4, LegacyAt::Exact(f4.clone())),
        (Some(feature), 3, LegacyAt::Nearest(c2.clone())),
        (Some(feature), 2, LegacyAt::Nearest(c2)),
        (Some(fresh), 4, LegacyAt::Nearest(f4)),
        (Some(gone), 3, LegacyAt::Exact(x3)),
        (Some(idle), 4, LegacyAt::Nearest(c4.clone())),
    ] {
        for cache in [None, Some(&cache)] {
            assert_eq!(
                record_at(root, &session, cache, native, version, false)
                    .await
                    .unwrap(),
                expected,
                "{native:?} at {version}"
            );
        }
    }

    for version in [5, 6] {
        assert_eq!(
            record_at(root, &session, Some(&cache), None, version, true)
                .await
                .unwrap(),
            LegacyAt::Nearest(c4.clone())
        );
    }
    for (native, version, fenced, refusal) in [
        (
            None,
            5,
            false,
            "version 5 of main is above version 4, the last one the legacy history records for \
             it, and is not a fence or a conversion of the storage upgrade"
                .to_string(),
        ),
        (
            None,
            7,
            true,
            "version 7 of main is above version 4".to_string(),
        ),
        (
            Some(feature),
            5,
            true,
            format!("version 5 of ref '{feature}' is above version 4"),
        ),
        (
            Some("absent"),
            3,
            false,
            "'absent' is neither a ref of __manifest nor a writer of the legacy history; version \
             3 of it cannot be read"
                .to_string(),
        ),
    ] {
        let error = record_at(root, &session, Some(&cache), native, version, fenced)
            .await
            .unwrap_err();
        assert!(
            matches!(error, OmniError::Manifest(ref manifest)
                if manifest.kind == crate::error::ManifestErrorKind::NotFound),
            "{error:?}"
        );
        assert!(error.to_string().contains(&refusal), "{error}");
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let person = legacy_table(1, "node:Person");
    let contract_only = LegacyPublish {
        contract: Some(legacy_contract("node Person {}")),
        ..Default::default()
    };
    let mut history = Stamp13History::create(root, contract_only).await.unwrap();
    let genesis = LegacyPublish {
        tables: vec![person.clone()],
        pins: vec![legacy_pin(&person, 1)],
        commit: Some(legacy_commit(&legacy_id(1), 2)),
        ..Default::default()
    };
    assert_eq!(history.publish(None, genesis).await.unwrap(), 2);
    let missing = record_at(root, &session, None, None, 1, false)
        .await
        .unwrap_err();
    assert!(
        missing.to_string().contains(
            "version 1 of main is stored in an earlier format and the root holds no legacy \
             history under __history/legacy/"
        ),
        "{missing}"
    );
    let census = legacy_census(&history, &[]).await.unwrap();
    let plan = super::legacy::plan(&census, &super::history::LegacyLayout::CURRENT).unwrap();
    super::legacy::materialize(root, &session, &census, &plan)
        .await
        .unwrap();
    let below = record_at(root, &session, None, None, 1, false)
        .await
        .unwrap();
    assert_eq!(below, LegacyAt::PreGenesis);
    assert!(
        below
            .served(None, 1)
            .unwrap_err()
            .to_string()
            .contains("version 1 of main precedes the genesis commit")
    );
    let error = ManifestCoordinator::snapshot_at(root, None, 1)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("version 1 of main precedes the genesis commit"),
        "{error}"
    );

    let collected = ManifestCoordinator::collector_branch_under_control_gates(root, None, &session)
        .await
        .unwrap();
    let cache = super::commit_graph::HistoryCache::default();
    assert!(
        collected.snapshot_at(1, &cache).await.unwrap().is_none(),
        "a version below the genesis commit holds no table for cleanup to protect"
    );
    let genesis = collected.snapshot_at(2, &cache).await.unwrap().unwrap();
    assert_eq!(
        (genesis.version, genesis.graph_head(None)),
        (2, Some(legacy_id(1).as_str()))
    );
    assert_eq!(
        genesis
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version,
        1
    );
}

/// After the upgrade, a version written before it is served from the record
/// of its commit or of the nearest one, labelled with the version asked for;
/// the conversion under the pending key is read only by the fenced reader.
#[tokio::test]
async fn legacy_versions_of_an_upgraded_root_are_served_from_their_records() {
    use super::commit_graph::{HistoryCache, graph_commit_from_manifest_row};
    use super::migrations::UPGRADE_PENDING_KEY;
    use super::retention::ManifestTagInventory;
    use lance::dataset::refs::Ref;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut served = legacy_served(root).await;
    let [_, c2, c4, f4, x3] = served.ids.each_ref().map(|id| served.record(id));
    let session = crate::lance_access::control_session();

    let intent = legacy_route_intent(6, 13, 14);
    assert_eq!(served.history.fence_for_test(&intent, 14).await.unwrap(), 5);
    let fence = served.history.head(None).unwrap().clone();
    let metadata = fence.schema().metadata.clone();
    let (_, rows) = super::legacy::conversion_batch(root, &session, &c4, metadata.clone())
        .await
        .unwrap();
    let conversion = super::commit::commit_overwrite(fence, vec![rows])
        .await
        .unwrap();
    assert_eq!(conversion.version().version, 6);
    assert!(
        conversion
            .schema()
            .metadata
            .contains_key(UPGRADE_PENDING_KEY)
    );
    let refused = read_manifest_state(&conversion).await.unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("storage upgrade recovery required"),
        "{refused}"
    );
    let (state, contract) = super::state::read_converted_state(&conversion)
        .await
        .unwrap();
    assert_eq!(contract, served.contract);
    assert_eq!(state.version, 6);
    assert_eq!(
        state.graph_heads,
        HashMap::from([("main".to_string(), c4.commit.graph_commit_id.clone())])
    );
    let mut keys: Vec<&str> = state
        .entries
        .iter()
        .map(|entry| entry.type_key.as_str())
        .collect();
    keys.sort();
    assert_eq!(keys, ["node:Firm", "node:Person"]);

    let mut active = conversion.clone();
    let kept: Vec<(String, String)> = metadata
        .into_iter()
        .filter(|(key, _)| key != UPGRADE_PENDING_KEY)
        .collect();
    active.update_schema_metadata(kept).replace().await.unwrap();
    assert_eq!(active.version().version, 7);
    let (activated, _) = super::state::read_converted_state(&active).await.unwrap();
    let ordinary = read_manifest_state(&active).await.unwrap();
    assert_eq!(
        (activated.version, &activated.graph_heads),
        (7, &ordinary.graph_heads)
    );

    let feature = served.feature.as_str();
    let pinned_person = |snapshot: &Snapshot| {
        snapshot
            .dataset("node:Person")
            .unwrap()
            .published_dataset_version
    };
    for (branch, version, head, person_version, record) in [
        (None, 2, Some(&c2), 2, &c2),
        (None, 3, Some(&c2), 2, &c2),
        (None, 5, Some(&c4), 7, &c4),
        (None, 6, Some(&c4), 7, &c4),
        (Some("feature"), 3, None, 2, &c2),
        (Some("feature"), 4, Some(&f4), 6, &f4),
    ] {
        let snapshot = ManifestCoordinator::snapshot_at(root, branch, version)
            .await
            .unwrap();
        let context = format!("{branch:?} at {version}");
        assert_eq!(snapshot.version, version, "{context}");
        assert_eq!(
            snapshot.graph_head(branch),
            head.map(|head| head.commit.graph_commit_id.as_str()),
            "{context}"
        );
        assert_eq!(pinned_person(&snapshot), person_version, "{context}");
        assert_eq!(snapshot.graph_branch(), branch, "{context}");
        assert_eq!(snapshot.native_branch(), branch.map(|_| feature));
        assert_eq!(
            snapshot.schema_contract(),
            record.commit.schema_contract.as_ref(),
            "{context}"
        );
        assert_eq!(
            ManifestCoordinator::read_schema_contract_for_snapshot(root, &snapshot)
                .await
                .unwrap(),
            served.contract,
            "{context}"
        );
    }
    let current = ManifestCoordinator::snapshot_at(root, None, 7)
        .await
        .unwrap();
    assert_eq!(
        (current.version, current.graph_head(None)),
        (7, Some(c4.commit.graph_commit_id.as_str()))
    );

    let history = HistoryCache::default();
    for (record, version) in [(&c2, 2), (&f4, 4)] {
        let commit = graph_commit_from_manifest_row(record.commit.clone());
        let pinned = ManifestCoordinator::pinned_graph_commit(root, &commit)
            .await
            .unwrap();
        assert_eq!(pinned.dataset.version().version, version);
        assert_eq!(
            pinned.dataset.manifest().branch,
            record.commit.native_branch
        );
        assert_eq!(pinned.snapshot.version, version);
        let graph = pinned.commit_graph(root, &history).await.unwrap();
        assert_eq!(graph.head(), &commit);
    }
    let unheld = GraphCommit {
        graph_manifest_version: 3,
        ..graph_commit_from_manifest_row(c2.commit.clone())
    };
    let error = match ManifestCoordinator::pinned_graph_commit(root, &unheld).await {
        Ok(_) => panic!("version 3 of main holds no commit"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("has no matching retained native manifest at version 3"),
        "{error}"
    );

    let retired = ManifestCoordinator::retired_commit_graphs(root, &history)
        .await
        .unwrap();
    assert_eq!(
        retired
            .iter()
            .map(|(native, graph)| (native.as_str(), graph.head().graph_commit_id.as_str()))
            .collect::<Vec<_>>(),
        vec![(served.gone.as_str(), x3.commit.graph_commit_id.as_str())]
    );

    active
        .tags()
        .create("kept", Ref::Version(None, Some(3)))
        .await
        .unwrap();
    let tags = ManifestTagInventory::capture(&active).await.unwrap();
    let tagged = tags.snapshot(&tags.tags["kept"], root).await.unwrap();
    assert_eq!(
        (tagged.snapshot.version, pinned_person(&tagged.snapshot)),
        (3, 2)
    );
    assert_eq!(
        tagged
            .commit_graph(root, &history)
            .await
            .unwrap()
            .head()
            .graph_commit_id,
        c2.commit.graph_commit_id
    );
}

/// Open, read and publish refuse stamp 13 on the stamp alone and never convert
/// it: the refusal names `omnigraph upgrade` for a standalone root and the
/// export with a stamp-13 build for a cluster-managed graph.
#[tokio::test]
async fn stamps_13_is_refused_by_open_read_and_publish_naming_the_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut dataset = open_manifest_dataset(uri, None).await.unwrap();
    let rows_at_birth = read_manifest_rows(&dataset).await.unwrap();
    super::migrations::set_stamp_for_test(&mut dataset, 13)
        .await
        .unwrap();
    let dataset = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(super::migrations::read_stamp(&dataset), Some(13));
    let expected = super::migrations::refuse_if_stamp_unsupported(13)
        .unwrap_err()
        .to_string();
    for clause in [
        "internal schema v13",
        "reads only v14 to v14",
        "For a standalone root, stop all readers, writers and maintenance",
        "run `omnigraph upgrade <graph> --check` and then `omnigraph upgrade <graph>`",
        "A cluster-managed graph has no in-place route yet: with a main development build from \
         2026-10-01 to 2026-10-04 (schema contract in manifest) run `omnigraph export <graph>",
    ] {
        assert!(expected.contains(clause), "{clause}: {expected}");
    }

    let refused = |what: &str, error: OmniError| {
        assert_eq!(error.to_string(), expected, "{what} of a stamp 13 manifest");
    };
    refused(
        "open",
        ManifestCoordinator::open(uri)
            .await
            .err()
            .expect("open must refuse"),
    );
    refused(
        "stamp read",
        read_supported_internal_schema_version(uri)
            .await
            .unwrap_err(),
    );
    refused(
        "state read",
        read_manifest_state(&dataset).await.unwrap_err(),
    );
    refused(
        "lineage read",
        read_graph_lineage(uri, &dataset).await.unwrap_err(),
    );
    refused(
        "publish",
        GraphNamespacePublisher::new(uri, None)
            .publish(&[], &HashMap::new(), Some(&lineage_intent(None, None)))
            .await
            .unwrap_err(),
    );
    refused(
        "overwrite",
        super::commit::overwrite(dataset.clone(), &rows_at_birth)
            .await
            .unwrap_err(),
    );
    assert_eq!(
        dataset.latest_version_id().await.unwrap(),
        dataset.version().version,
        "a refused open, read or publish must not move `__manifest`"
    );
    assert_eq!(history_row_count(uri).await, 0);
}

fn migrations_intent(version: u64) -> super::migrations::UpgradeIntent {
    use super::migrations::{SourceBranch, UpgradeIntent, UpgradeSchemaContract};
    let contract = SchemaContractRow {
        head: SchemaContractHead {
            schema_ir_hash: format!("sha256:{}", "a".repeat(64)),
            ..legacy_contract("").head
        },
        ..legacy_contract("node Person { name: String }")
    };
    UpgradeIntent {
        protocol: 6,
        attempt: legacy_id(9),
        source_format: 13,
        target_format: 14,
        graph_identity: "domain".to_string(),
        branches: vec![
            SourceBranch {
                native: Some("feature".to_string()),
                identity: lance::dataset::refs::BranchIdentifier::main(),
                version: version + 4,
                parent_version: version,
            },
            SourceBranch {
                native: None,
                identity: lance::dataset::refs::BranchIdentifier::main(),
                version,
                parent_version: 0,
            },
        ],
        schema_contract: Some(UpgradeSchemaContract::from_row(&contract)),
        legacy: super::legacy::LegacyPlan {
            layout: super::history::LegacyLayout::CURRENT,
            commits: 3,
            data_files: 1,
            id_shards: 1,
            writer_shards: 1,
            directory_sha256: "b".repeat(64),
        },
    }
}

async fn migrations_root(dir: &tempfile::TempDir) -> Dataset {
    let uri = dir.path().to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    open_manifest_dataset(uri, None).await.unwrap()
}

async fn migrations_set(dataset: &mut Dataset, key: &str, value: &str) -> u64 {
    dataset
        .update_schema_metadata([(key.to_string(), value.to_string())])
        .await
        .unwrap();
    dataset.version().version
}

/// The intent round-trips through the pending key byte for byte, and
/// `intent_from` owns only protocol 6 from 13 to 14 with a contract, a plan
/// under a known layout and one main pinned last.
#[tokio::test]
async fn migrations_intent_round_trips_and_refuses_what_this_build_cannot_own() {
    use super::migrations::{
        MAX_BRANCHES, MAX_INTENT_BYTES, SourceBranch, UPGRADE_PENDING_KEY, UpgradeIntent,
        UpgradeSchemaContract, guard_stamp, intent_from, recovery_guidance,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut dataset = migrations_root(&dir).await;
    assert_eq!(intent_from(&dataset).unwrap(), None);

    let intent = migrations_intent(1);
    let json = serde_json::to_string(&intent).unwrap();
    migrations_set(&mut dataset, UPGRADE_PENDING_KEY, &json).await;
    let read = intent_from(&dataset).unwrap().expect("the key is set");
    assert_eq!(read, intent);
    assert_eq!(serde_json::to_string(&read).unwrap(), json);
    let guidance = recovery_guidance(&dataset);
    assert_eq!(
        guidance,
        format!(
            "storage upgrade recovery required: this graph carries the pending storage \
             conversion of attempt {} to format v14. Keep the graph offline: stop all readers, \
             writers and maintenance, retain the backup taken before the attempt, and rerun \
             `omnigraph upgrade <graph>` without `--check` with this executable; the upgrade \
             resumes the existing attempt.",
            intent.attempt
        )
    );
    assert_eq!(
        guard_stamp(&dataset).unwrap_err().to_string(),
        OmniError::manifest(guidance).to_string()
    );

    let contract = intent.schema_contract.clone().unwrap();
    let row = SchemaContractRow {
        head: contract.identity.clone(),
        ..legacy_contract("node Person { name: String }")
    };
    contract.validate_row(&row).unwrap();
    let edited = SchemaContractRow {
        source: "node Person { name: String? }".to_string(),
        ..row
    };
    assert!(
        contract
            .validate_row(&edited)
            .unwrap_err()
            .to_string()
            .contains("upgrade schema contract identity or exact text changed")
    );

    let ambiguous = "unsupported or ambiguous storage upgrade intent";
    let edit = |edit: &dyn Fn(&mut UpgradeIntent)| {
        let mut edited = intent.clone();
        edit(&mut edited);
        serde_json::to_string(&edited).unwrap()
    };
    let field = |name: &str, value: Option<serde_json::Value>| {
        let mut object = serde_json::to_value(&intent).unwrap();
        let fields = object.as_object_mut().unwrap();
        match value {
            Some(value) => fields.insert(name.to_string(), value),
            None => fields.remove(name),
        };
        object.to_string()
    };
    let cases: Vec<(&str, String, &str)> = vec![
        ("an empty object", "{}".to_string(), "missing field"),
        (
            "an earlier route",
            legacy_route_intent(5, 12, 13),
            "unrecognized upgrade ownership",
        ),
        (
            "no legacy plan",
            field("legacy", None),
            "missing field `legacy`",
        ),
        (
            "an unknown field",
            field("digest", Some(serde_json::json!("x"))),
            "unknown field `digest`",
        ),
        (
            "no contract",
            edit(&|intent| intent.schema_contract = None),
            ambiguous,
        ),
        ("protocol 5", edit(&|intent| intent.protocol = 5), ambiguous),
        (
            "source 12",
            edit(&|intent| intent.source_format = 12),
            ambiguous,
        ),
        (
            "target 15",
            edit(&|intent| intent.target_format = 15),
            ambiguous,
        ),
        (
            "a non-ULID attempt",
            edit(&|intent| intent.attempt = "attempt".into()),
            ambiguous,
        ),
        (
            "no graph identity",
            edit(&|intent| intent.graph_identity.clear()),
            ambiguous,
        ),
        (
            "a contract of another graph",
            edit(&|intent| intent.graph_identity = "other".into()),
            ambiguous,
        ),
        (
            "a contract text digest that is no sha256",
            edit(&|intent| {
                intent.schema_contract = Some(UpgradeSchemaContract {
                    ir_sha256: "c".repeat(63),
                    ..contract.clone()
                })
            }),
            ambiguous,
        ),
        (
            "a directory digest that is no sha256",
            edit(&|intent| intent.legacy.directory_sha256 = "z".repeat(64)),
            ambiguous,
        ),
        (
            "no branch",
            edit(&|intent| intent.branches.clear()),
            ambiguous,
        ),
        (
            "main not last",
            edit(&|intent| intent.branches.reverse()),
            ambiguous,
        ),
        (
            "a ref pinned twice",
            edit(&|intent| {
                let twice = intent.branches[0].clone();
                intent.branches.insert(0, twice)
            }),
            ambiguous,
        ),
        (
            "a zero source version",
            edit(&|intent| intent.branches[1].version = 0),
            ambiguous,
        ),
        (
            "a named ref without a fork version",
            edit(&|intent| intent.branches[0].parent_version = 0),
            ambiguous,
        ),
        (
            "a named ref forked above its head",
            edit(&|intent| intent.branches[0].parent_version = intent.branches[0].version + 1),
            ambiguous,
        ),
        (
            "main with a fork version",
            edit(&|intent| intent.branches[1].parent_version = 1),
            ambiguous,
        ),
        (
            "more live refs than the bound",
            edit(&|intent| {
                let named = intent.branches[0].clone();
                let more: Vec<SourceBranch> = (0..MAX_BRANCHES)
                    .map(|number| SourceBranch {
                        native: Some(format!("b{number}")),
                        ..named.clone()
                    })
                    .collect();
                intent.branches.splice(0..0, more);
            }),
            ambiguous,
        ),
        (
            "a layout version of another build",
            edit(&|intent| intent.legacy.layout.version = 2),
            "storage upgrade intent names legacy layout version 2, which this build does not \
             know (it writes layout version 1); finish the upgrade with the executable that \
             fenced the graph",
        ),
        (
            "an intent over the metadata budget",
            " ".repeat(MAX_INTENT_BYTES + 1),
            "storage upgrade intent exceeds the metadata budget",
        ),
    ];
    for (what, json, refusal) in cases {
        migrations_set(&mut dataset, UPGRADE_PENDING_KEY, &json).await;
        let error = intent_from(&dataset).unwrap_err().to_string();
        assert!(error.contains(refusal), "{what}: {error}");
        let guidance = recovery_guidance(&dataset);
        assert!(
            guidance.starts_with(
                "storage upgrade recovery required: this graph carries a pending storage \
                 conversion whose ownership this executable cannot establish ("
            ) && guidance.contains(refusal)
                && guidance.contains(
                    "run `omnigraph upgrade <graph> --check` for read-only diagnostics, and \
                     finish the conversion with the omnigraph executable that started it. Never \
                     delete the marker."
                ),
            "{what}: {guidance}"
        );
    }
}

/// A receipt is a pure function of the intent and the ref's pin, and
/// `branch_completed` accepts it only at the versions the protocol adds: one
/// on a named ref, two on fenced main, three once main is activated.
#[tokio::test]
async fn migrations_receipts_complete_a_ref_only_at_the_protocol_versions() {
    use super::migrations::{
        INTERNAL_SCHEMA_VERSION_KEY, SourceBranch, UPGRADE_PENDING_KEY, UPGRADE_RECEIPT_KEY,
        branch_completed, receipt,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut dataset = migrations_root(&dir).await;
    let stale = format!(
        r#"{{"protocol":5,"attempt":"{}","source":{{"native":null,"identity":{},"version":1}}}}"#,
        legacy_id(8),
        serde_json::to_string(&lance::dataset::refs::BranchIdentifier::main()).unwrap()
    );
    let intent = migrations_intent(dataset.version().version + 1);
    let [named, main] = [&intent.branches[0], &intent.branches[1]];
    let completed = |dataset: &Dataset, source: &SourceBranch| {
        branch_completed(dataset, source, &intent).map_err(|error| error.to_string())
    };
    let refused = |dataset: &Dataset, source: &SourceBranch, refusal: &str| {
        let error = completed(dataset, source).unwrap_err();
        assert!(error.contains(refusal), "{error}");
    };
    assert_eq!(receipt(main, &intent), receipt(main, &intent));
    assert_ne!(receipt(main, &intent), receipt(named, &intent));
    let own = serde_json::to_string(&receipt(main, &intent)).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&own).unwrap(),
        serde_json::json!({
            "protocol": 6,
            "attempt": intent.attempt,
            "source": serde_json::to_value(main).unwrap(),
        }),
        "a receipt carries the protocol, the attempt and the source pin, and no digest"
    );
    assert_eq!(completed(&dataset, main), Ok(false));

    assert_eq!(
        migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, &stale).await,
        main.version
    );
    assert_eq!(
        completed(&dataset, main),
        Ok(false),
        "the receipt of an earlier route on the unconverted source head"
    );
    let json = serde_json::to_string(&intent).unwrap();
    assert_eq!(
        migrations_set(&mut dataset, UPGRADE_PENDING_KEY, &json).await,
        main.version + 1
    );
    assert_eq!(
        completed(&dataset, main),
        Ok(false),
        "the fence keeps the earlier receipt"
    );
    let moved = SourceBranch {
        version: main.version - 1,
        ..main.clone()
    };
    refused(&dataset, &moved, "foreign upgrade receipt");
    assert_eq!(
        migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, &own).await,
        main.version + 2
    );
    assert_eq!(completed(&dataset, main), Ok(true));
    refused(&dataset, named, "foreign upgrade receipt");

    let kept: Vec<(String, String)> = dataset
        .schema()
        .metadata
        .iter()
        .filter(|(key, _)| key.as_str() != UPGRADE_PENDING_KEY)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    dataset
        .update_schema_metadata(kept)
        .replace()
        .await
        .unwrap();
    assert_eq!(dataset.version().version, main.version + 3);
    assert_eq!(completed(&dataset, main), Ok(true));
    migrations_set(&mut dataset, "omnigraph:other", "x").await;
    refused(
        &dataset,
        main,
        "upgraded branch moved while conversion was incomplete",
    );
    migrations_set(&mut dataset, INTERNAL_SCHEMA_VERSION_KEY, "13").await;
    refused(&dataset, main, "upgrade receipt has an incompatible format");

    let dir = tempfile::tempdir().unwrap();
    let mut dataset = migrations_root(&dir).await;
    let named = SourceBranch {
        version: dataset.version().version,
        ..named.clone()
    };
    let own = serde_json::to_string(&receipt(&named, &intent)).unwrap();
    assert_eq!(
        migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, &own).await,
        named.version + 1
    );
    assert_eq!(completed(&dataset, &named), Ok(true));
    let foreign = own.replace(&intent.attempt, &legacy_id(8));
    migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, &foreign).await;
    refused(&dataset, &named, "foreign upgrade receipt");
    migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, "{}").await;
    refused(&dataset, &named, "invalid upgrade receipt");

    let dir = tempfile::tempdir().unwrap();
    let mut dataset = migrations_root(&dir).await;
    let rival = SourceBranch {
        version: migrations_set(&mut dataset, UPGRADE_RECEIPT_KEY, &foreign).await,
        ..named.clone()
    };
    assert_eq!(
        (dataset.version().version, intent.protocol),
        (rival.version, 6),
        "another attempt of this same protocol left its receipt on the unconverted source head"
    );
    refused(&dataset, &rival, "foreign upgrade receipt");
    let rival_main = SourceBranch {
        version: rival.version,
        ..main.clone()
    };
    assert_eq!(
        migrations_set(&mut dataset, UPGRADE_PENDING_KEY, &json).await,
        rival_main.version + 1
    );
    refused(&dataset, &rival_main, "foreign upgrade receipt");
}
