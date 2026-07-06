use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use lance::dataset::builder::DatasetBuilder;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    DescribeTableRequest, DescribeTableVersionRequest, ListTableVersionsRequest,
};
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tokio::sync::Mutex;

use super::publisher::{LineageIntent, ManifestBatchPublisher, PublishOutcome};
use super::*;
use omnigraph_compiler::catalog::build_catalog;
use omnigraph_compiler::schema::parser::parse_schema;

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
    build_catalog(&schema).unwrap()
}

#[tokio::test]
async fn test_init_creates_manifest_and_sub_tables() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();

    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("node:Company").is_some());
    assert!(snap.entry("edge:Knows").is_some());
    assert!(snap.entry("edge:WorksAt").is_some());

    for key in &["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        let entry = snap.entry(key).unwrap();
        assert_eq!(entry.table_version, 1);
        assert_eq!(entry.row_count, 0);
        assert!(entry.table_branch.is_none());
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
    assert!(snap.entry("node:Person").is_some());
    assert!(snap.entry("edge:Knows").is_some());
}

#[tokio::test]
async fn test_commit_advances_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let v1 = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let new_version = mc
        .commit(&[SubTableUpdate {
            table_key: "node:Person".to_string(),
            table_version: person_version,
            table_branch: None,
            row_count: 1,
            version_metadata: table_version_metadata_for_state(
                uri,
                &person_entry.table_path,
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
    let person = snap.entry("node:Person").unwrap();
    assert_eq!(person.table_version, person_version);
    assert_eq!(person.row_count, 1);

    let company = snap.entry("node:Company").unwrap();
    assert_eq!(company.table_version, 1);
    assert_eq!(company.row_count, 0);
}

#[tokio::test]
async fn test_commit_changes_can_register_new_table_and_tombstone_old_one() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let before_version = mc.version();
    let person_entry = mc.snapshot().entry("node:Person").unwrap().clone();

    let table_key = "node:Human".to_string();
    let table_path = table_path_for_table_key(&table_key).unwrap();
    let dataset_uri = format!("{}/{}", uri, table_path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let ds = crate::table_store::TableStore::create_empty_dataset(&dataset_uri, &schema)
        .await
        .unwrap();
    let state = crate::table_store::TableStore::new(uri, Arc::new(lance::session::Session::default()))
        .table_state(&dataset_uri, &ds)
        .await
        .unwrap();

    mc.commit_changes(&[
        ManifestChange::RegisterTable(TableRegistration {
            table_key: table_key.clone(),
            table_path: table_path.clone(),
        }),
        ManifestChange::Update(SubTableUpdate {
            table_key: table_key.clone(),
            table_version: state.version,
            table_branch: None,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
        }),
        ManifestChange::Tombstone(TableTombstone {
            table_key: "node:Person".to_string(),
            tombstone_version: person_entry.table_version + 1,
        }),
    ])
    .await
    .unwrap();

    let head = mc.snapshot();
    assert!(head.entry("node:Human").is_some());
    assert!(head.entry("node:Person").is_none());

    let historical = ManifestCoordinator::snapshot_at(uri, None, before_version)
        .await
        .unwrap();
    assert!(historical.entry("node:Person").is_some());
    assert!(historical.entry("node:Human").is_none());
}

#[tokio::test]
async fn test_snapshot_open_sub_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_ds = snap.open("node:Person").await.unwrap();

    assert_eq!(person_ds.schema().fields.len(), 3);
    assert_eq!(person_ds.count_rows(None).await.unwrap(), 0);
}

#[tokio::test]
async fn test_version_is_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    assert_eq!(mc.version(), snap.version());
}

#[tokio::test]
async fn test_list_branches_only_returns_main_once() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let branches = mc.list_branches().await.unwrap();
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();

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
        mc.snapshot().entry("node:Person").unwrap().table_version,
        person_version
    );
    assert_eq!(mc.snapshot().entry("node:Person").unwrap().row_count, 1);
}

#[tokio::test]
async fn test_directory_namespace_direct_publish_cannot_replace_native_omnigraph_write_path() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();

    let namespace = DirectoryNamespaceBuilder::new(uri)
        .manifest_enabled(true)
        .dir_listing_enabled(false)
        .table_version_tracking_enabled(true)
        .inline_optimization_enabled(false)
        .build()
        .await
        .unwrap();

    // Lance 9.0.0-beta.15 realignment: `table_version_storage_enabled` was
    // removed upstream (the `__manifest` version-storage experiment was
    // retired, PR #7222) and dir.rs no longer eagerly rewrites PK annotations —
    // yet the guard's thesis holds unchanged: with dir-listing disabled the
    // native namespace still reports omnigraph's manifest-tracked tables as
    // TableNotFound and cannot publish over the manifest. The v7 mechanism
    // notes below are retained as history.
    //
    // Lance 7: the native `DirectoryNamespace` no longer recognizes omnigraph's
    // manifest-tracked tables, so list / describe / create_table_version all
    // return `TableNotFound`. The mechanism is *contingent on omnigraph's legacy
    // boolean PK key*, not an unconditional v7 property: v7's namespace eagerly
    // rewrites any `__manifest` whose `object_id` lacks the new
    // `lance-schema:unenforced-primary-key:position` key, omnigraph declares the
    // PK with the legacy boolean key, and v7 forbids changing a PK once set — so
    // `ensure_manifest_table_up_to_date` errors, the namespace silently falls
    // back to directory listing (disabled here), and `check_table_status` reports
    // the table absent. omnigraph keeps the boolean key deliberately: Lance
    // honors it permanently (it maps to PK position 0) and one uniform on-disk
    // format beats a new-vs-old split, since existing graphs can't be re-keyed to
    // the position key under that same immutability rule. The decoupling is
    // therefore an accepted, production-irrelevant tradeoff (omnigraph never uses
    // the native namespace — its publisher writes `__manifest` via merge_insert
    // and its reads go through its own `LanceNamespace` impls), and it only
    // strengthens this guard's thesis: native tooling cannot enumerate, inspect,
    // or publish over omnigraph's tables, let alone replace the write path.
    let assert_table_not_found = |what: &str, dbg: String| {
        assert!(
            dbg.contains("TableNotFound") && dbg.contains("node:Person"),
            "{what}: expected TableNotFound for node:Person, got: {dbg}"
        );
    };
    assert_table_not_found(
        "list_table_versions",
        format!(
            "{:?}",
            namespace
                .list_table_versions(ListTableVersionsRequest {
                    id: Some(vec!["node:Person".to_string()]),
                    descending: Some(true),
                    ..Default::default()
                })
                .await
                .unwrap_err()
        ),
    );
    assert_table_not_found(
        "describe_table_version",
        format!(
            "{:?}",
            namespace
                .describe_table_version(DescribeTableVersionRequest {
                    id: Some(vec!["node:Person".to_string()]),
                    version: Some(person_version as i64),
                    ..Default::default()
                })
                .await
                .unwrap_err()
        ),
    );
    assert_table_not_found(
        "create_table_version",
        format!(
            "{:?}",
            namespace
                .create_table_version(version_metadata.to_create_table_version_request(
                    "node:Person",
                    person_version,
                    1,
                    None,
                ))
                .await
                .unwrap_err()
        ),
    );

    // omnigraph's manifest stays authoritative: refresh ignores the direct
    // `person_ds.append` above (it was never manifest-published), so the row
    // count stays 0 and the version is unchanged.
    mc.refresh().await.unwrap();
    assert_eq!(
        mc.snapshot().entry("node:Person").unwrap().table_version,
        person_entry.table_version
    );
    assert_eq!(mc.snapshot().entry("node:Person").unwrap().row_count, 0);
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.table_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_ds.append(reader, None).await.unwrap();
    let feature_version = feature_ds.version().version;
    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.table_path,
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
    let feature_entry = feature_snapshot.entry("node:Person").unwrap();
    assert_eq!(feature_entry.table_version, feature_version);
    assert_eq!(feature_entry.table_branch.as_deref(), Some("feature"));
    assert_eq!(
        feature_snapshot
            .open("node:Person")
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
    let main_entry = main_snapshot.entry("node:Person").unwrap();
    assert_eq!(main_entry.table_version, person_entry.table_version);
    assert_eq!(main_entry.table_branch, None);
    assert_eq!(
        main_snapshot
            .open("node:Person")
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.table_version, None)
        .await
        .unwrap();
    let mut feature_person_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_person_ds.append(reader, None).await.unwrap();
    let feature_person_version = feature_person_ds.version().version;
    let feature_person_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.table_path,
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
    let err = format!("{:?}", mismatched.expect_err("branch-mismatched open must fail on v9"));
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
        company_entry.table_branch, None,
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
    let person_entry = frozen_snapshot.entry("node:Person").unwrap().clone();
    let manifest_version = reader.version();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader_batch = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader_batch, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
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
        frozen_snapshot.entry("node:Person").unwrap().table_version,
        person_entry.table_version
    );
    assert_eq!(
        frozen_snapshot
            .open("node:Person")
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
            .entry("node:Person")
            .unwrap()
            .table_version,
        person_version
    );
    assert_eq!(reader.snapshot().entry("node:Person").unwrap().row_count, 1);
}

#[tokio::test]
async fn test_batch_create_table_versions_is_atomic_on_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let manifest_version = mc.version();
    let snap = mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let person_version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();
    let company_version_metadata = table_version_metadata_for_state(
        uri,
        &company_entry.table_path,
        None,
        company_entry.table_version,
    )
    .await
    .unwrap();

    let person_request = person_version_metadata.to_create_table_version_request(
        "node:Person",
        person_version,
        1,
        None,
    );

    let conflicting_company_request = company_version_metadata.to_create_table_version_request(
        "node:Company",
        company_entry.table_version,
        0,
        None,
    );

    let err = GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[person_request, conflicting_company_request])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"));

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_version,
        person_entry.table_version
    );
    assert_eq!(
        reopened.snapshot().entry("node:Person").unwrap().row_count,
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
    let person_entry = snap.entry("node:Person").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();
    let request =
        version_metadata.to_create_table_version_request("node:Person", person_version, 1, None);

    let err = GraphNamespacePublisher::new(uri, None)
        .publish_requests(&[request.clone(), request])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"));

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_version,
        person_entry.table_version
    );
    assert_eq!(
        reopened.snapshot().entry("node:Person").unwrap().row_count,
        0
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.table_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_ds.append(reader, None).await.unwrap();
    let feature_version = feature_ds.version().version;
    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.table_path,
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
        &person_entry.table_path,
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
    let experiment_entry = experiment_snapshot.entry("node:Person").unwrap();
    assert_eq!(experiment_entry.table_version, feature_version);
    assert_eq!(experiment_entry.table_branch.as_deref(), Some("experiment"));
}

/// Regression (PR #307 review — Cursor Bugbot High + Codex P2): the post-publish
/// fold (`#1b`) must reflect an owner-branch handoff. A handoff UPDATEs a
/// `table_version` row IN PLACE at the SAME Lance version with a new
/// `table_branch` — merge-insert `UpdateAll` on the deterministic
/// `version_object_id(table_key, version)`, so `__manifest` ends with one row
/// carrying the new branch. The buggy fold appended the pending row after
/// `existing_versions`, and `assemble_manifest_state` keeps the FIRST entry at
/// equal `table_version`, so the WARM coordinator retained the stale
/// `table_branch` ("feature") while a fresh `read_manifest_state` reopen reflected
/// the handoff ("experiment"). Unlike the namespace-publisher handoff test above,
/// this commits through the coordinator's `commit` path to exercise the fold, then
/// reads the warm `snapshot()` WITHOUT reopening.
#[tokio::test]
async fn test_post_publish_fold_reflects_owner_branch_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mut main_mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    main_mc.create_branch("feature").await.unwrap();

    // Fork Person onto `feature` at version Vf (owner = feature).
    let snap = main_mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    person_ds
        .create_branch("feature", person_entry.table_version, None)
        .await
        .unwrap();
    let mut feature_ds = person_ds.checkout_branch("feature").await.unwrap();
    let person_schema = Arc::new(feature_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    feature_ds.append(reader, None).await.unwrap();
    let feature_version = feature_ds.version().version;
    let feature_metadata = table_version_metadata_for_state(
        uri,
        &person_entry.table_path,
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
        &person_entry.table_path,
        Some("experiment"),
        feature_version,
    )
    .await
    .unwrap();

    // Publish the handoff through a WARM coordinator's `commit` (exercises the
    // post-publish fold), NOT GraphNamespacePublisher (which reopens fresh).
    let mut experiment_mc = ManifestCoordinator::open_at_branch(uri, "experiment")
        .await
        .unwrap();
    // Pre-publish: experiment inherits feature's ownership of Person@Vf.
    assert_eq!(
        experiment_mc
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature"),
    );
    experiment_mc
        .commit(&[SubTableUpdate {
            table_key: "node:Person".to_string(),
            table_version: feature_version,
            table_branch: Some("experiment".to_string()),
            row_count: 1,
            version_metadata: experiment_metadata,
        }])
        .await
        .unwrap();

    // Warm side: the folded known_state the commit adopted.
    let folded_branch = experiment_mc
        .snapshot()
        .entry("node:Person")
        .unwrap()
        .table_branch
        .clone();
    // Oracle: a fresh reopen rebuilds known_state via `read_manifest_state`.
    let reopened = ManifestCoordinator::open_at_branch(uri, "experiment")
        .await
        .unwrap();
    let scanned_branch = reopened
        .snapshot()
        .entry("node:Person")
        .unwrap()
        .table_branch
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
}

#[tokio::test]
async fn test_staged_namespace_lists_native_table_versions_before_publish() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();

    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap().clone();

    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;

    let namespace = staged_table_namespace(uri, "node:Person", &person_entry.table_path, None);
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
    async fn publish(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &HashMap<String, u64>,
        lineage: Option<&LineageIntent>,
    ) -> Result<PublishOutcome> {
        let requests: Vec<CreateTableVersionRequest> = changes
            .iter()
            .filter_map(|change| match change {
                ManifestChange::Update(update) => Some(update.to_create_table_version_request()),
                ManifestChange::RegisterTable(_) | ManifestChange::Tombstone(_) => None,
            })
            .collect();
        self.requests.lock().await.extend_from_slice(&requests);
        self.inner
            .publish(changes, expected_table_versions, lineage)
            .await
    }
}

struct FailingPublisher;

#[async_trait]
impl ManifestBatchPublisher for FailingPublisher {
    async fn publish(
        &self,
        _changes: &[ManifestChange],
        _expected_table_versions: &HashMap<String, u64>,
        _lineage: Option<&LineageIntent>,
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();

    let recording = RecordingPublisher::new(uri, None);
    mc = mc.with_batch_publisher(Arc::new(recording.clone()));

    mc.commit(&[SubTableUpdate {
        table_key: "node:Person".to_string(),
        table_version: person_version,
        table_branch: None,
        row_count: 1,
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
        mc.snapshot().entry("node:Person").unwrap().table_version,
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let person_batch = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec!["person-1"])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(person_batch)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let person_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, person_version)
            .await
            .unwrap();

    mc = mc.with_batch_publisher(Arc::new(FailingPublisher));
    let err = mc
        .commit(&[SubTableUpdate {
            table_key: "node:Person".to_string(),
            table_version: person_version,
            table_branch: None,
            row_count: 1,
            version_metadata,
        }])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("injected batch publisher failure"));
    assert_eq!(mc.version(), manifest_version);
    assert_eq!(
        mc.snapshot().entry("node:Person").unwrap().table_version,
        person_entry.table_version
    );
    assert_eq!(mc.snapshot().entry("node:Person").unwrap().row_count, 0);

    let reopened = ManifestCoordinator::open(uri).await.unwrap();
    assert_eq!(reopened.version(), manifest_version);
    assert_eq!(
        reopened
            .snapshot()
            .entry("node:Person")
            .unwrap()
            .table_version,
        person_entry.table_version
    );
}

/// Drive Person to a fresh on-disk dataset version `v` (returns the new
/// version number) and produce a `SubTableUpdate` ready to publish.
async fn append_person_and_make_update(
    uri: &str,
    person_entry: &SubTableEntry,
    name: &str,
) -> SubTableUpdate {
    let mut person_ds = Dataset::open(&format!("{}/{}", uri, person_entry.table_path))
        .await
        .unwrap();
    let person_schema = Arc::new(person_ds.schema().into());
    let row = RecordBatch::try_new(
        Arc::clone(&person_schema),
        vec![
            Arc::new(StringArray::from(vec![format!("person-{name}")])),
            Arc::new(StringArray::from(vec![Some(name.to_string())])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(row)], person_schema);
    person_ds.append(reader, None).await.unwrap();
    let new_version = person_ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &person_entry.table_path, None, new_version)
            .await
            .unwrap();
    SubTableUpdate {
        table_key: "node:Person".to_string(),
        table_version: new_version,
        table_branch: None,
        row_count: 1,
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
    let person_entry = snap.entry("node:Person").unwrap().clone();

    let update = append_person_and_make_update(uri, &person_entry, "Alice").await;
    let mut expected = HashMap::new();
    // After init, every table is at table_version=1 — assert that.
    expected.insert("node:Person".to_string(), 1);
    expected.insert("node:Company".to_string(), 1);

    mc.commit_with_expected(&[update.clone()], &expected)
        .await
        .expect("matching expected versions should publish cleanly");

    let after = mc.snapshot();
    assert_eq!(
        after.entry("node:Person").unwrap().table_version,
        update.table_version
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
    let person_entry = snap.entry("node:Person").unwrap().clone();

    // Writer A advances Person.
    let update_a = append_person_and_make_update(uri, &person_entry, "Alice").await;
    let advanced_version = update_a.table_version;
    mc.commit(&[update_a]).await.unwrap();

    // Writer B then tries to commit, asserting Person is still at v=1.
    let update_b = append_person_and_make_update(uri, &person_entry, "Bob").await;
    let mut stale_expected = HashMap::new();
    stale_expected.insert("node:Person".to_string(), 1);

    let err = mc
        .commit_with_expected(&[update_b], &stale_expected)
        .await
        .expect_err("stale expected_table_versions should reject");

    match err {
        OmniError::Manifest(m) => match m.details {
            Some(ManifestConflictDetails::ExpectedVersionMismatch {
                table_key,
                expected,
                actual,
            }) => {
                assert_eq!(table_key, "node:Person");
                assert_eq!(expected, 1);
                assert_eq!(actual, advanced_version);
            }
            other => panic!("expected ExpectedVersionMismatch details, got {:?}", other),
        },
        other => panic!("expected OmniError::Manifest, got {:?}", other),
    }
}

#[tokio::test]
async fn test_commit_with_expected_catches_drift_on_untouched_table() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let snap = mc.snapshot();
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

    // Writer A advances Company.
    let mut company_ds = Dataset::open(&format!("{}/{}", uri, company_entry.table_path))
        .await
        .unwrap();
    let company_schema = Arc::new(company_ds.schema().into());
    let row = RecordBatch::try_new(
        Arc::clone(&company_schema),
        vec![
            Arc::new(StringArray::from(vec!["company-1"])),
            Arc::new(StringArray::from(vec!["Acme"])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(row)], company_schema);
    company_ds.append(reader, None).await.unwrap();
    let company_version = company_ds.version().version;
    let company_metadata =
        table_version_metadata_for_state(uri, &company_entry.table_path, None, company_version)
            .await
            .unwrap();
    mc.commit(&[SubTableUpdate {
        table_key: "node:Company".to_string(),
        table_version: company_version,
        table_branch: None,
        row_count: 1,
        version_metadata: company_metadata,
    }])
    .await
    .unwrap();

    // Writer B writes Person but asserts Company is still at v=1.
    let update_person = append_person_and_make_update(uri, &person_entry, "Bob").await;
    let mut expected = HashMap::new();
    expected.insert("node:Company".to_string(), 1);

    let err = mc
        .commit_with_expected(&[update_person], &expected)
        .await
        .expect_err("drift on an untouched expected table should reject");

    let OmniError::Manifest(m) = err else {
        panic!("expected OmniError::Manifest");
    };
    match m.details {
        Some(ManifestConflictDetails::ExpectedVersionMismatch {
            ref table_key,
            expected,
            actual,
        }) => {
            assert_eq!(table_key, "node:Company");
            assert_eq!(expected, 1);
            assert_eq!(actual, company_version);
        }
        other => panic!("expected ExpectedVersionMismatch, got {:?}", other),
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
    expected.insert("node:DoesNotExist".to_string(), 7);
    let err = mc
        .commit_with_expected(&[], &expected)
        .await
        .expect_err("unknown expected table should reject");

    let OmniError::Manifest(m) = err else {
        panic!("expected OmniError::Manifest");
    };
    match m.details {
        Some(ManifestConflictDetails::ExpectedVersionMismatch {
            table_key,
            expected,
            actual,
        }) => {
            assert_eq!(table_key, "node:DoesNotExist");
            assert_eq!(expected, 7);
            assert_eq!(actual, 0);
        }
        other => panic!("expected ExpectedVersionMismatch, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_publish_with_overlapping_expected_versions_one_succeeds() {
    use crate::error::ManifestConflictDetails;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let catalog = build_test_catalog();
    let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
    let person_entry = mc.snapshot().entry("node:Person").unwrap().clone();

    // Advance the Person dataset once so we have a real on-disk version 2 that
    // both publishers can target. Both attempt to land the *same*
    // `version:node:Person@v=2` row in `__manifest`, which is the row-level
    // CAS conflict the publisher must detect: load_publish_state at the same
    // baseline → pre-check passes for both → only one merge_insert can land
    // the unique `object_id`.
    let update = append_person_and_make_update(uri, &person_entry, "Alice").await;

    let mut expected = HashMap::new();
    expected.insert("node:Person".to_string(), 1);

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
    // The losing writer surfaces either ExpectedVersionMismatch (its retry's
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
    if let Some(ManifestConflictDetails::ExpectedVersionMismatch {
        ref table_key,
        expected,
        ..
    }) = m.details
    {
        assert_eq!(table_key, "node:Person");
        assert_eq!(expected, 1);
    }

    // Manifest must reflect exactly one new commit on Person at the requested
    // version (no duplicate version rows).
    let mc = ManifestCoordinator::open(uri).await.unwrap();
    let entry = mc.snapshot().entry("node:Person").unwrap().clone();
    assert!(
        entry.table_version > 1,
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
        super::migrations::read_stamp(&ds),
        super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION,
        "init should stamp the manifest at the current internal schema version",
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
        super::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION,
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
    ManifestCoordinator::init(uri, &catalog).await.unwrap();

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
    expected.insert("node:Person".to_string(), 1);
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
            Some("5".to_string()),
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

// A graph stamped below CURRENT (the strand floor: `MIN_SUPPORTED == CURRENT`,
// so anything older than v4) is refused on open in BOTH modes, with the
// rebuild-via-export/import hint — there is no in-place migration. This is the
// floor twin of `future_stamp_is_refused_in_both_open_modes` (the ceiling). The
// open path (`Omnigraph::open` read-write and `Omnigraph::open_read_only`) routes
// the stamp through `refuse_if_stamp_unsupported`, whose sub-MIN branch points
// the operator at `omnigraph export`.
#[tokio::test]
async fn sub_current_graph_is_refused_on_open_with_rebuild_hint() {
    use crate::db::Omnigraph;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    // A full v4 graph (schema artifacts present) so the open path gets past its
    // schema read to the stamp check.
    Omnigraph::init(uri, "node Person { name: String }\n")
        .await
        .unwrap();

    // Rewind main's stamp to v3 — a graph this binary's single served version
    // (v4) cannot open, since `MIN_SUPPORTED == CURRENT == 4`.
    {
        let mut ds = open_manifest_dataset(uri, None).await.unwrap();
        super::migrations::set_stamp_for_test(&mut ds, 3).await.unwrap();
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
    use crate::loader::{LoadMode, load_jsonl};

    let schema = "node Person {\n    name: String @key\n    age: I32?\n}\n";
    let seed = "{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n\
                {\"type\":\"Person\",\"data\":{\"name\":\"bob\",\"age\":41}}\n";

    // The operator's existing graph; export it with the (here, current) binary
    // before upgrading.
    let dir_old = tempfile::tempdir().unwrap();
    let uri_old = dir_old.path().to_str().unwrap();
    let mut db_old = Omnigraph::init(uri_old, schema).await.unwrap();
    load_jsonl(&mut db_old, seed, LoadMode::Overwrite)
        .await
        .unwrap();
    let exported = db_old.export_jsonl("main", &[], &[]).await.unwrap();
    assert!(
        exported.contains("alice") && exported.contains("bob"),
        "export must carry the loaded rows",
    );
    drop(db_old);

    // Make it look like a graph from an older release: rewind the stamp below CURRENT.
    {
        let mut ds = open_manifest_dataset(uri_old, None).await.unwrap();
        super::migrations::set_stamp_for_test(&mut ds, 3).await.unwrap();
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
        msg.contains("0.7.2"),
        "the refusal must name the release that wrote this stamp (v3 → 0.6.2 to 0.7.2) so the \
         operator knows which binary to use, got: {err}",
    );

    // Rebuild with this binary: fresh init + load the export.
    let dir_new = tempfile::tempdir().unwrap();
    let uri_new = dir_new.path().to_str().unwrap();
    let mut db_new = Omnigraph::init(uri_new, schema).await.unwrap();
    load_jsonl(&mut db_new, &exported, LoadMode::Overwrite)
        .await
        .unwrap();

    // The rebuilt graph preserves the data and is at CURRENT (opens without refusal).
    let rebuilt = db_new.export_jsonl("main", &[], &[]).await.unwrap();
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

// ── RFC-013 Phase 7 / step 5: the `graph_head` concurrency gate ──────────────
//
// Two (or N) writers committing DISJOINT tables on the same branch still share
// one mutable `graph_head:main` row (one `object_id`, `WhenMatched::UpdateAll`).
// Their table-version rows never collide (distinct `object_id`s), so the *only*
// row-level CAS contention is on `graph_head:main`. The contract under test:
// exactly one writer wins each CAS round; the loser retries, re-resolves its
// parent off the freshly-advanced head (inside the publisher's retry loop), and
// re-commits — so every writer commits and the resulting graph_commit DAG is a
// single LINEAR chain (no fork), not a tree. This is the cross-process
// disjoint-table fork closed by the shared head row (invariants.md §7.1).

/// A microsecond UNIX timestamp for a `LineageIntent`, matching the genesis /
/// commit-graph `created_at` unit.
fn lineage_now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Append one row to a two-column NODE table (`id`, `name`) and return the
/// resulting `SubTableUpdate` at the new on-disk version. Generalizes
/// `append_person_and_make_update` to any node table whose schema is `(id:
/// String, name: String[, ...])`; the extra `Person.age` column is filled null
/// when present so the same helper drives both `node:Person` and `node:Company`.
async fn append_node_row_and_make_update(
    uri: &str,
    entry: &SubTableEntry,
    id: &str,
    name: &str,
) -> SubTableUpdate {
    let mut ds = Dataset::open(&format!("{}/{}", uri, entry.table_path))
        .await
        .unwrap();
    let schema = Arc::new(ds.schema().into());
    let arrow_schema: &Schema = &schema;
    // Columns 0/1 are (id, name); a third column (Person.age) is filled null.
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(StringArray::from(vec![id.to_string()])),
        Arc::new(StringArray::from(vec![name.to_string()])),
    ];
    for field in arrow_schema.fields().iter().skip(2) {
        columns.push(arrow_array::new_null_array(field.data_type(), 1));
    }
    let row = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(row)], schema);
    ds.append(reader, None).await.unwrap();
    let new_version = ds.version().version;
    let version_metadata =
        table_version_metadata_for_state(uri, &entry.table_path, None, new_version)
            .await
            .unwrap();
    SubTableUpdate {
        table_key: entry.table_key.clone(),
        table_version: new_version,
        table_branch: None,
        row_count: 1,
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
    let genesis: Vec<&GraphLineageRow> =
        rows.iter().filter(|r| r.parent_commit_id.is_none()).collect();
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

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
        async { publisher_a.publish(&changes_a, &empty, Some(&intent_a)).await },
        async { publisher_b.publish(&changes_b, &empty, Some(&intent_b)).await }
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
    assert_eq!(after.entry("node:Person").unwrap().table_version, 2);
    assert_eq!(after.entry("node:Company").unwrap().table_version, 2);
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

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
        async { publisher_a.publish(&changes_a, &empty, Some(&intent_a)).await },
        async { publisher_b.publish(&changes_b, &empty, Some(&intent_b)).await }
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
    let person_entry = snap.entry("node:Person").unwrap().clone();
    let company_entry = snap.entry("node:Company").unwrap().clone();

    // Synthesize N=8 DISJOINT table-version updates by sequentially advancing the
    // two node tables four versions each (Person@v2..v5, Company@v2..v5). Each
    // update is a distinct `object_id`, so the writers never collide on a
    // table-version row — only on the shared `graph_head:main`. Built serially
    // here (before the concurrent phase) so the on-disk versions exist.
    let mut updates: Vec<SubTableUpdate> = Vec::with_capacity(N);
    for i in 0..(N / 2) {
        updates.push(
            append_node_row_and_make_update(uri, &person_entry, &format!("p{i}"), &format!("P{i}"))
                .await,
        );
        updates.push(
            append_node_row_and_make_update(uri, &company_entry, &format!("c{i}"), &format!("C{i}"))
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
    assert_eq!(committed_ids.len(), N, "every writer must commit exactly once");

    // The final DAG is a single linear chain of genesis + 8 = 9, no fork.
    assert_linear_chain(uri, N + 1).await;
}
