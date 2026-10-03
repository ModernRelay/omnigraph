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

use super::publisher::{
    GraphHeadExpectation, LineageIntent, ManifestBatchPublisher, PublishOutcome,
    PublishPrecondition, is_retryable_publish_conflict, map_lance_publish_error,
};
use super::state::read_publish_scan;
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
    let live_rows = read_publish_scan(&base).await.unwrap().live_rows;

    let append = WriteParams {
        mode: WriteMode::Append,
        skip_auto_cleanup: true,
        ..Default::default()
    };
    let stored_schema =
        super::record::manifest_storage_schema(base.schema().metadata.clone()).unwrap();
    let winner_row = super::record::compact_to_storage(
        &relabelled_manifest_row(&live_rows, "cas_probe:winner"),
        &stored_schema,
    )
    .unwrap();
    let winner = InsertBuilder::new(Arc::new(base.clone()))
        .with_params(&append)
        .execute(vec![winner_row])
        .await
        .unwrap();
    assert_eq!(winner.version().version, base.version().version + 1);

    let lost = super::commit::overwrite(
        base,
        relabelled_manifest_row(&live_rows, "cas_probe:loser"),
        live_rows,
    )
    .await
    .expect_err("a stale overwrite must not land over the winner");
    assert!(is_retryable_publish_conflict(&lost), "{lost}");

    let head = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(head.version().version, winner.version().version);
    let ids = manifest_object_ids(&head).await;
    assert!(ids.contains("cas_probe:winner"), "{ids:?}");
    assert!(!ids.contains("cas_probe:loser"), "{ids:?}");
}

/// One live `__manifest` row under a new `object_id`, so each published image
/// carries one row that names it.
fn relabelled_manifest_row(live_rows: &[RecordBatch], object_id: &str) -> RecordBatch {
    let row = live_rows[0].slice(0, 1);
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
    assert_eq!(
        super::layout::version_object_id(identity, 3),
        "table_version:000000000000002a:0000000000000007:00000000000000000003"
    );
    assert_eq!(
        super::layout::tombstone_object_id(identity, 4),
        "table_tombstone:000000000000002a:0000000000000007:00000000000000000004"
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
        let contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();

        ManifestCoordinator::init_commit(
            uri,
            &catalog,
            &contract,
            &control_session,
            &committed_attempt,
        )
        .await
        .unwrap();
        ManifestCoordinator::open_exact_genesis_with_lineage(
            uri,
            &committed_attempt,
            &control_session,
        )
        .await
        .expect("the creating attempt must authenticate its own immutable genesis");

        let foreign_attempt = GenesisManifestAttempt::mint(catalog.system_columns).unwrap();
        let error = match ManifestCoordinator::open_exact_genesis_with_lineage(
            uri,
            &foreign_attempt,
            &control_session,
        )
        .await
        {
            Ok(_) => panic!("a valid v1 manifest from another initializer must not authenticate"),
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
        let error = match ManifestCoordinator::open_exact_genesis_with_lineage(
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

    let namespace = branch_manifest_namespace(uri, None);
    let request =
        version_metadata.to_create_table_version_request("node:Person", person_version, 1, None);
    namespace.create_table_version(request).await.unwrap();
    let _ = mc.refresh_for_live_read(|_| true).await.unwrap();

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
        "the directory namespace reads `__manifest` by its own catalog columns; since stamp 12 \
         `location` lives inside the packed `record` struct, so it fails at FieldNotFound one \
         step before the TableNotFound a flat manifest gave: {list_error:?}"
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
    let _ = mc.refresh_for_live_read(|_| true).await.unwrap();
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

    let _ = reader.refresh_for_live_read(|_| true).await.unwrap();
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
/// later registration. Its greater manifest version wins the fold and the
/// earlier row survives beside it.
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
    let rows = super::state::read_publish_scan(&ds)
        .await
        .unwrap()
        .version_entries;
    assert_eq!(
        rows.iter()
            .filter(|row| row.type_key == "node:Person")
            .count(),
        2,
        "the earlier registration must survive beside the later one: {rows:?}"
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
                | ManifestChange::Tombstone(_)
                | ManifestChange::SchemaContract(_) => None,
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
            merged_parent_commit_id: None,
            created_at: lineage_now_micros(),
        };
        publisher
            .publish(&[], &empty, Some(&intent))
            .await
            .expect("establish named-branch graph head");
        Some(intent.graph_commit_id)
    } else {
        None
    };

    // The folded publish scan must preserve exact absence. In particular, the
    // inferred lineage head inherited from main must not masquerade as a
    // materialized `graph_head:feature` row.
    let branch_manifest = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let scan = read_publish_scan(&branch_manifest).await.unwrap();
    assert_eq!(scan.graph_heads.get("feature").cloned(), expected_head);
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
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: Some("feature".to_string()),
        actor_id: Some("act-b".to_string()),
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
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
        (Ok(_), Err(err)) => (intent_a.graph_commit_id.clone(), err),
        (Err(err), Ok(_)) => (intent_b.graph_commit_id.clone(), err),
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
    let (commits, heads) = read_graph_lineage(&branch_manifest).await.unwrap();
    assert_eq!(heads.get("feature"), Some(&winner_id));
    assert_eq!(
        commits
            .iter()
            .filter(|commit| {
                commit.graph_commit_id == intent_a.graph_commit_id
                    || commit.graph_commit_id == intent_b.graph_commit_id
            })
            .count(),
        1,
        "the rejected intent must not leave an immutable graph_commit row"
    );
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

    let publisher = GraphNamespacePublisher::new(uri, Some("feature"));
    let contract = mc.read_schema_contract().await.unwrap();
    publisher
        .publish(
            &[ManifestChange::SchemaContract(contract.clone())],
            &HashMap::new(),
            None,
        )
        .await
        .unwrap();
    mc.commit_changes(&[ManifestChange::SchemaContract(contract)])
        .await
        .unwrap();
    let old_branch = open_manifest_dataset(uri, Some("feature")).await.unwrap();
    let old_identifier = old_branch.branch_identifier().await.unwrap();
    assert!(
        !read_publish_scan(&old_branch)
            .await
            .unwrap()
            .graph_heads
            .contains_key("feature"),
        "fresh named branch starts without its own graph_head row"
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
    assert_eq!(old_branch.version().version, recreated.version().version);

    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        Some("feature"),
        old_identifier,
        None,
    ));
    let err = publisher
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
    let (rows, _heads) = read_graph_lineage(&ds).await.unwrap();
    assert_eq!(
        rows.len(),
        expected_total,
        "expected {expected_total} graph_commit rows (genesis + the concurrent commits), got {}",
        rows.len(),
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
    let head = super::state::head_lineage_row(&rows).expect("a non-empty lineage has a head");
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
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-b".to_string()),
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
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
    res_a.expect("writer A must commit");
    res_b.expect("writer B must commit");

    // End-state assertion (the on-disk DAG is fixed once both committed): a single
    // linear chain genesis → first → second, no fork. The two minted ids both
    // appear; their parents form a chain (one off genesis, the other off the
    // first), so no two commits share a parent.
    let head = assert_linear_chain(uri, 3).await;
    assert!(
        head == intent_a.graph_commit_id || head == intent_b.graph_commit_id,
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
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
    };
    let intent_b = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: None,
        actor_id: Some("act-b".to_string()),
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
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
    res_a.expect("writer A must commit on S3");
    res_b.expect("writer B must commit on S3");

    let head = assert_linear_chain(&uri, 3).await;
    assert!(
        head == intent_a.graph_commit_id || head == intent_b.graph_commit_id,
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
                    merged_parent_commit_id: None,
                    created_at: lineage_now_micros(),
                };
                let publisher = GraphNamespacePublisher::new(&uri, None);
                match publisher.publish(&changes, &empty, Some(&intent)).await {
                    Ok(_) => return commit_id,
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

async fn legacy_manifest_fixture(
    uri: &str,
    source: &DatasetEntry,
    table_version: u64,
    key_version: u64,
    mode: lance::dataset::WriteMode,
) -> Dataset {
    let mut entry = source.clone();
    entry.published_dataset_version = table_version;
    entry.manifest_version = key_version;
    let metadata = HashMap::from([(
        entry.identity,
        entry.version_metadata.to_json_string().unwrap(),
    )]);
    let batch = super::state::entries_to_batch(&[entry], &metadata, &[], None).unwrap();
    let batch = if matches!(mode, lance::dataset::WriteMode::Append) {
        batch.slice(1, 1)
    } else {
        batch
    };
    let schema = Arc::new(
        batch
            .schema()
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([(
                "omnigraph:internal_schema_version".to_string(),
                "6".to_string(),
            )])),
    );
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap();
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(lance::dataset::WriteParams {
            mode,
            enable_stable_row_ids: true,
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn legacy_manifest_decoder_preserves_data_version_order() {
    let dir = tempfile::tempdir().unwrap();
    let mc = ManifestCoordinator::init(
        dir.path().join("graph").to_str().unwrap(),
        &build_test_catalog(),
    )
    .await
    .unwrap();
    let source = mc.known_state.entries[0].clone();
    let fixture = dir.path().join("legacy");
    let uri = fixture.to_str().unwrap();
    legacy_manifest_fixture(uri, &source, 40, 40, lance::dataset::WriteMode::Create).await;
    let dataset =
        legacy_manifest_fixture(uri, &source, 20, 20, lance::dataset::WriteMode::Append).await;
    let legacy = super::state::read_manifest_state(&dataset).await.unwrap();
    assert_eq!(legacy.entries[0].published_dataset_version, 40);
    let by_update = super::state::read_manifest_state_with_registration_clocks(&dataset)
        .await
        .unwrap();
    assert_eq!(by_update.entries[0].published_dataset_version, 20);
    assert_eq!(by_update.entries[0].manifest_version, 2);
    let historical = dataset.checkout_version(1).await.unwrap();
    assert_eq!(
        super::state::read_manifest_state(&historical)
            .await
            .unwrap()
            .entries[0]
            .published_dataset_version,
        40
    );
    assert!(super::migrations::guard_stamp(&dataset).is_err());
}

#[tokio::test]
async fn legacy_manifest_decoder_refuses_key_pointer_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let mc = ManifestCoordinator::init(
        dir.path().join("graph").to_str().unwrap(),
        &build_test_catalog(),
    )
    .await
    .unwrap();
    let source = mc.known_state.entries[0].clone();
    let dataset = legacy_manifest_fixture(
        dir.path().join("legacy").to_str().unwrap(),
        &source,
        40,
        41,
        lance::dataset::WriteMode::Create,
    )
    .await;
    let error = super::state::read_manifest_state(&dataset)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("expected table version 40"));
    let error = super::state::read_manifest_state_with_registration_clocks(&dataset)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("expected table version 40"));
}

#[tokio::test]
async fn legacy_manifest_decoder_preserves_equal_version_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mc = ManifestCoordinator::init(
        dir.path().join("graph").to_str().unwrap(),
        &build_test_catalog(),
    )
    .await
    .unwrap();
    let mut source = mc.known_state.entries[0].clone();
    source.published_dataset_version = 40;
    source.manifest_version = 40;
    let fixture = dir.path().join("legacy");
    let uri = fixture.to_str().unwrap();
    let original =
        legacy_manifest_fixture(uri, &source, 40, 40, lance::dataset::WriteMode::Create).await;
    let metadata = HashMap::from([(
        source.identity,
        source.version_metadata.to_json_string().unwrap(),
    )]);
    let batch = super::state::entries_to_batch(std::slice::from_ref(&source), &metadata, &[], None)
        .unwrap()
        .slice(1, 1);
    let mut columns = batch.columns().to_vec();
    columns[0] = Arc::new(StringArray::from(vec![super::layout::tombstone_object_id(
        source.identity,
        40,
    )]));
    columns[1] = Arc::new(StringArray::from(vec![OBJECT_TYPE_TABLE_TOMBSTONE]));
    let schema = Arc::new(
        batch
            .schema()
            .as_ref()
            .clone()
            .with_metadata(original.schema().metadata.clone()),
    );
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert!(
        super::state::read_manifest_state(&dataset)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        super::state::read_manifest_state_with_registration_clocks(&dataset)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    dataset
        .update_schema_metadata([("omnigraph:internal_schema_version", "7")])
        .await
        .unwrap();
    let error = super::state::read_manifest_state(&dataset)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("above the scanned dataset version")
    );
}

/// The `present` byte marking exactly `null_fields` null, by their bit in `RECORD_FIELDS`.
fn null_bits(null_fields: &[&str]) -> u8 {
    null_fields
        .iter()
        .map(|field| {
            let bit = super::record::RECORD_FIELDS
                .iter()
                .position(|name| name == field)
                .unwrap();
            1u8 << bit
        })
        .fold(0, |mask, bit| mask | bit)
}

/// A logical batch with every record field in each of its states: a value,
/// null, and (for strings) empty, so the packed shape must keep empty and
/// null apart through the `present` bits.
fn record_states_batch() -> RecordBatch {
    let identity = TableIdentity {
        stable_table_id: 7,
        table_incarnation_id: 3,
    };
    super::state::manifest_rows_batch(
        vec![
            super::layout::table_object_id(identity),
            "graph_head:main".into(),
            "x".into(),
        ],
        vec!["table".into(), "graph_head".into(), "probe".into()],
        vec![Some("tables/7.3/".into()), None, Some(String::new())],
        vec![None, Some("{}".into()), Some(String::new())],
        vec!["node:Person".into(), String::new(), "k".into()],
        vec![Some(identity), None, Some(identity)],
        vec![None, Some(9), Some(0)],
        vec![None, Some("main".into()), Some(String::new())],
        vec![None, None, Some(0)],
        vec![None, None, None],
        vec![None, None, None],
    )
    .unwrap()
}

#[test]
fn packed_record_round_trips_nulls_through_present_bits() {
    let logical = record_states_batch();
    let schema = super::record::manifest_storage_schema(HashMap::new()).unwrap();
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
    assert!(
        stored
            .column_by_name("schema_source")
            .is_some_and(|column| column.null_count() == 3)
            && stored
                .column_by_name("schema_ir")
                .is_some_and(|column| column.null_count() == 3),
        "rows without a contract store null content"
    );
    let record = stored
        .column_by_name("record")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(record.num_columns(), 9);
    assert!(record.columns().iter().all(|child| child.null_count() == 0));
    let present = record
        .column_by_name("present")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::UInt8Array>()
        .unwrap();
    assert_eq!(
        present.values().as_ref(),
        &[
            null_bits(&["metadata", "table_version", "table_branch", "row_count"]),
            null_bits(&[
                "location",
                "stable_table_id",
                "table_incarnation_id",
                "row_count"
            ]),
            null_bits(&[]),
        ]
    );

    let expanded = super::record::expand_from_storage(&stored).unwrap();
    assert_eq!(expanded, logical);

    let without_content = stored.project(&[0, 1, 2]).unwrap();
    assert_eq!(
        super::record::expand_from_storage(&without_content).unwrap(),
        logical,
        "a scan that never projected the content columns expands them null"
    );

    let flat = super::state::flat_manifest_schema();
    assert_eq!(flat.fields().len(), 11);
    assert_eq!(flat.field(4).name(), "base_objects");
    assert!(
        flat.field_with_name("schema_source").is_err()
            && flat.field_with_name("schema_ir").is_err(),
        "the flat shape never carried the content columns"
    );
}

/// `stored` with its `present` column replaced by `present`, the rows a
/// writer that set a null bit over a value would leave behind.
fn with_present_bits(stored: &RecordBatch, present: Vec<u8>) -> RecordBatch {
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
    children[present_index] = Arc::new(arrow_array::UInt8Array::from(present));
    let tampered = arrow_array::StructArray::new(record.fields().clone(), children, None);
    let columns = stored
        .schema()
        .fields()
        .iter()
        .zip(stored.columns())
        .map(|(field, column)| -> arrow_array::ArrayRef {
            if field.name() == "record" {
                Arc::new(tampered.clone())
            } else {
                column.clone()
            }
        })
        .collect();
    RecordBatch::try_new(stored.schema(), columns).unwrap()
}

#[test]
fn packed_record_refuses_a_null_bit_beside_a_value() {
    let logical = record_states_batch();
    let schema = super::record::manifest_storage_schema(HashMap::new()).unwrap();
    let stored = super::record::compact_to_storage(&logical, &schema).unwrap();

    let string_tampered = with_present_bits(&stored, vec![null_bits(&["location"]), 0, 0]);
    let error = super::record::expand_from_storage(&string_tampered).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("'location' is marked null but carries a value at row 0"),
        "{error}"
    );

    let u64_tampered = with_present_bits(&stored, vec![null_bits(&["stable_table_id"]), 0, 0]);
    let error = super::record::expand_from_storage(&u64_tampered).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("'stable_table_id' is marked null but carries a value at row 0"),
        "{error}"
    );
}

/// A flat stamp-11 manifest reads as it is; a publish over it rewrites it packed at stamp 13
/// with the same table state (no `schema_contract` row: the storage upgrade route adds that),
/// and the pre-conversion version keeps its flat shape for time travel. A snapshot captured
/// before the conversion keeps reporting stamp 11 after it, from its attached manifest
/// dataset and from the fallback that opens the captured version.
#[tokio::test]
async fn stamp_11_manifest_converts_on_its_next_publish() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut born = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(super::migrations::read_stamp(&born), Some(13));
    let at_birth = logical_view(&born).await;

    super::migrations::restamp_flat_for_test(&mut born, 11)
        .await
        .unwrap();
    assert_eq!(super::migrations::read_stamp(&born), Some(11));
    let flat = open_manifest_dataset(uri, None).await.unwrap();
    assert_eq!(super::migrations::read_stamp(&flat), Some(11));
    assert!(flat.schema().field("location").is_some());
    assert!(flat.schema().field("base_objects").is_some());
    let flat_version = flat.version().version;
    assert_eq!(logical_view(&flat).await, at_birth);
    assert!(
        super::state::read_manifest_state(&flat)
            .await
            .unwrap()
            .schema_contract
            .is_none(),
        "a flat manifest has no schema_contract row"
    );

    let held = ManifestCoordinator::snapshot_at(uri, None, flat_version)
        .await
        .unwrap();

    let live_rows = read_publish_scan(&flat).await.unwrap().live_rows;
    let empty_pending = live_rows[0].slice(0, 0);
    let (converted, _) = super::commit::overwrite(flat, empty_pending, live_rows)
        .await
        .unwrap();
    assert_eq!(super::migrations::read_stamp(&converted), Some(13));
    assert!(
        converted.manifest().uses_stable_row_ids(),
        "the conversion overwrite must keep the stable row ids genesis enables"
    );
    let names: Vec<_> = converted
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
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
    let fragments = converted.get_fragments();
    let file = &fragments[0].metadata().files[0];
    assert_eq!(
        file.column_indices.len(),
        5,
        "the packed record is one physical column beside `object_id`, `object_type` and the two content columns: {:?}",
        file.column_indices
    );
    assert_eq!(logical_view(&converted).await, at_birth);

    let historical = converted.checkout_version(flat_version).await.unwrap();
    assert_eq!(super::migrations::read_stamp(&historical), Some(11));
    assert!(historical.schema().field("base_objects").is_some());
    assert_eq!(logical_view(&historical).await, at_birth);

    let stamp_of = |snapshot: Snapshot| async move {
        ManifestCoordinator::internal_schema_stamp_for_snapshot(uri, &snapshot).await
    };
    assert!(held.manifest_dataset.is_some());
    assert_eq!(stamp_of(held.clone()).await.unwrap(), Some(11));
    let mut detached = held.clone();
    detached.manifest_dataset = None;
    assert_eq!(
        stamp_of(detached.clone()).await.unwrap(),
        Some(11),
        "the fallback opens the captured version, not the converted head"
    );
    let current = ManifestCoordinator::snapshot_at(uri, None, converted.version().version)
        .await
        .unwrap();
    assert_eq!(stamp_of(current).await.unwrap(), Some(13));
    let elsewhere = tempfile::tempdir().unwrap();
    let error = ManifestCoordinator::internal_schema_stamp_for_snapshot(
        elsewhere.path().to_str().unwrap(),
        &held,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("another root"), "{error}");
    let mut unprovenanced = detached;
    unprovenanced.graph_branch = Some("feature".to_string());
    unprovenanced.native_branch = None;
    let error = stamp_of(unprovenanced).await.unwrap_err();
    assert!(
        error.to_string().contains("lacks native branch provenance"),
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
    let batch = &read_publish_scan(&ds).await.unwrap().live_rows[0];
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
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
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
            merged_parent_commit_id: None,
            created_at: 0,
        };
        let probes = crate::instrumentation::QueryIoProbes::default();
        let scans = probes.manifest_scan_count.clone();
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
        let scan = read_publish_scan(&outcome.dataset).await.unwrap();
        assert_eq!(scan.lineage_rows.len(), round + 2);
        assert_eq!(scan.graph_heads.get("main"), Some(&intent.graph_commit_id));
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
    let control_session = crate::lance_access::control_session();
    let (mut reader, _) = ManifestCoordinator::open_with_lineage(uri, None, &control_session)
        .await
        .unwrap();
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

    reader.refresh_with_lineage().await.unwrap();
    assert_eq!(
        reader.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );
    assert_eq!(reader.read_schema_contract().await.unwrap(), replacement);
    let refreshed = reader.refresh_for_live_read(|_| true).await.unwrap();
    assert!(refreshed.is_none());
    assert_eq!(
        reader.known_state.schema_contract.as_ref(),
        Some(&replacement.head)
    );

    // A retained main handle must not equate a manifest version with a root
    // lifetime: main's native branch identity is fixed. This is the refresh
    // boundary prepared schema no-op admission and reconciliation rely on.
    reader.refresh_with_lineage().await.unwrap();
    assert!(reader.projection.is_some());
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
    reader.refresh_with_lineage().await.unwrap();
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
    assert_eq!(lineage, read_graph_lineage(&dataset).await.unwrap().0);

    let rows = read_publish_scan(&dataset).await.unwrap().live_rows;
    let alias = relabelled_manifest_row(&rows, SCHEMA_CONTRACT_OBJECT_ID);
    let mut columns = alias.columns().to_vec();
    columns[alias.schema().index_of("object_type").unwrap()] =
        Arc::new(StringArray::from(vec!["unknown_extension"]));
    let alias = RecordBatch::try_new(alias.schema(), columns).unwrap();
    let schema = super::record::manifest_storage_schema(dataset.schema().metadata.clone()).unwrap();
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
        error.to_string().contains("duplicate selected rows"),
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
            let rows = read_publish_scan(&dataset).await.unwrap().live_rows;
            let schema =
                super::record::manifest_storage_schema(dataset.schema().metadata.clone()).unwrap();
            let stored = rows
                .iter()
                .map(|row| super::record::compact_to_storage(row, &schema).unwrap())
                .collect::<Vec<_>>();
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
}

/// What a reader projects out of a `__manifest` version through either stored shape: the
/// `ManifestState` entries (as `Debug` text: `DatasetEntry` has no `PartialEq`) and heads, and
/// the lineage scan's commits and heads; not the dataset version, which every rewrite changes.
async fn logical_view(
    dataset: &lance::Dataset,
) -> (
    String,
    HashMap<String, String>,
    Vec<GraphLineageRow>,
    HashMap<String, String>,
) {
    let state = super::state::read_manifest_state(dataset).await.unwrap();
    let (commits, heads) = read_graph_lineage(dataset).await.unwrap();
    (
        format!("{:?}", state.entries),
        state.graph_heads,
        commits,
        heads,
    )
}

/// A retired same-name candidate can lack the wanted version. Continue to the
/// exact owner, but never accept a different commit or a missing owning manifest.
#[tokio::test]
async fn pinned_graph_commit_skips_absent_wrong_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut mc = ManifestCoordinator::init(uri, &build_test_catalog())
        .await
        .unwrap();
    let mut main = open_manifest_dataset(uri, None).await.unwrap();
    let base_version = main.version().version;
    let old_native = "b1.00000000000000000000000001";
    let live_native = "b1.00000000000000000000000002";
    let old = crate::lance_clone::create_branch(&mut main, old_native, base_version)
        .await
        .unwrap();
    mc.delete_branch("b1").await.unwrap();
    drop(old);
    crate::lance_clone::create_branch(&mut main, live_native, base_version)
        .await
        .unwrap();
    let mut live = ManifestCoordinator::open_at_branch(uri, "b1")
        .await
        .unwrap();
    let intent = LineageIntent {
        graph_commit_id: "00000000000000000000000003".into(),
        branch: Some("b1".into()),
        actor_id: None,
        merged_parent_commit_id: None,
        created_at: 1,
    };
    live.commit_changes_with_lineage(&[], &HashMap::new(), Some(&intent))
        .await
        .unwrap();
    let graph = crate::commit_graph::CommitGraph::open_at_branch(uri, "b1")
        .await
        .unwrap();
    let commit = graph.get_commit(&intent.graph_commit_id).unwrap();
    assert!(commit.graph_manifest_version > base_version);
    let absent_candidate = main
        .checkout_version(lance::dataset::refs::Ref::Version(
            Some(old_native.to_string()),
            Some(commit.graph_manifest_version),
        ))
        .await
        .expect_err("the retired candidate never published the requested version");
    assert!(
        matches!(&absent_candidate, lance::Error::DatasetNotFound { .. }),
        "{absent_candidate:?}"
    );
    let pinned = ManifestCoordinator::pinned_graph_commit(uri, &commit)
        .await
        .expect("the retired candidate lacks this version; the live owner has it");
    assert_eq!(
        pinned.dataset.manifest().branch.as_deref(),
        Some(live_native)
    );
    assert_eq!(
        pinned.dataset.version().version,
        commit.graph_manifest_version
    );

    let wrong = crate::commit_graph::GraphCommit {
        graph_commit_id: "00000000000000000000000004".into(),
        ..commit.clone()
    };
    let error = ManifestCoordinator::pinned_graph_commit(uri, &wrong)
        .await
        .err()
        .expect("a same-version manifest must match the full requested graph commit");
    assert!(
        matches!(
            &error,
            OmniError::Manifest(ManifestError {
                kind: crate::error::ManifestErrorKind::NotFound,
                ..
            })
        ),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("no matching retained native manifest")
    );

    let store = pinned.dataset.object_store(None).await.unwrap();
    store
        .delete(&pinned.dataset.manifest_location().path)
        .await
        .unwrap();
    let error = ManifestCoordinator::pinned_graph_commit(uri, &commit)
        .await
        .err()
        .expect("the true retained manifest is missing; no candidate may substitute");
    assert!(
        matches!(
            &error,
            OmniError::Manifest(ManifestError {
                kind: crate::error::ManifestErrorKind::NotFound,
                ..
            })
        ),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("no matching retained native manifest")
    );
}

#[derive(Debug)]
struct SmallScanProbeMarker;

impl lance::io::WrappingObjectStore for SmallScanProbeMarker {
    fn wrap(
        &self,
        _: &str,
        original: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        original
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

fn schema_comment_noise(lines: usize) -> String {
    let mut random = 0xa076_1d64_78bd_642fu64;
    let mut text = String::new();
    for _ in 0..lines {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        use std::fmt::Write;
        writeln!(&mut text, "// {random:016x}").unwrap();
    }
    text
}

async fn small_scan_catalog_fixture(root: &str, large: bool) -> (SchemaContractRow, u64) {
    let catalog = build_test_catalog();
    let mut contract = SchemaContractRow::for_test_catalog(&catalog).unwrap();
    contract.source = format!("{}\n", test_schema_source());
    contract
        .source
        .push_str(&schema_comment_noise(if large { 32_768 } else { 256 }));
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
            total > 65_536,
            "fixture did not cross fallback boundary: {total}"
        );
    } else {
        assert!(
            (4097..=65_536).contains(&total),
            "fixture missed small-read window: {total}"
        );
    }
    (contract, total)
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

        if optimized {
            let (state, _, lineage, row) =
                super::state::read_manifest_projection_with_contract(&dataset)
                    .await
                    .unwrap();
            assert_eq!(row.unwrap(), *expected);
            assert_eq!(state.schema_contract.as_ref(), Some(&expected.head));
            assert_eq!(state.version, dataset.version().version);
            assert!(!lineage.is_empty());
        } else {
            let scan = super::state::read_publish_scan(&dataset).await.unwrap();
            assert_eq!(scan.schema_contract.as_ref(), Some(&expected.head));
            assert!(!scan.lineage_rows.is_empty());
            let mut rows = 0;
            for batch in scan.live_rows {
                use arrow_array::Array;
                let ids = batch
                    .column_by_name("object_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let sources = batch
                    .column_by_name("schema_source")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                let irs = batch
                    .column_by_name("schema_ir")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    if ids.value(row) == SCHEMA_CONTRACT_OBJECT_ID {
                        assert!(!sources.is_null(row) && !irs.is_null(row));
                        assert_eq!(sources.value(row), expected.source);
                        assert_eq!(irs.value(row), expected.ir);
                        rows += 1;
                    }
                }
            }
            assert_eq!(rows, 1);
        }
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
        let (expected, encoded_bytes) = small_scan_catalog_fixture(root, large).await;
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
                [4096, 65_536],
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
    let (expected, _) = small_scan_catalog_fixture(root, false).await;
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
        let failure = super::state::read_manifest_projection_with_contract(&dataset).await;
        assert!(failure.is_err(), "captured-size mismatch must fail closed");
        let failed_io = drain_small_scan_io(&stores);
        assert!(
            failed_io.requests > 0,
            "failure must reach the tracked physical backend"
        );
        store.inner.put(&file_path, saved.into()).await.unwrap();
        let _ = drain_small_scan_io(&stores);
        let (_, _, _, contract) = super::state::read_manifest_projection_with_contract(&dataset)
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
    for (sizes, eligible) in [
        (vec![65_536u64], true),
        (vec![65_537u64], false),
        (vec![32_768u64, 32_768], true),
        (vec![32_768u64, 32_769], false),
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
            scan_store.block_size(),
            if eligible { 65_536 } else { 4096 }
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
    for block_size in [65_536usize, 131_072] {
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
        merged_parent_commit_id: None,
        created_at: lineage_now_micros(),
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
    assert_eq!(candidate.head.graph_commit_id, winner.graph_commit_id);
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
