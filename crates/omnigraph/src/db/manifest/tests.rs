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

use super::publisher::ManifestBatchPublisher;
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
        .table_version_storage_enabled(true)
        .inline_optimization_enabled(false)
        .build()
        .await
        .unwrap();

    let versions = namespace
        .list_table_versions(ListTableVersionsRequest {
            id: Some(vec!["node:Person".to_string()]),
            descending: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        versions.versions[0].version as u64,
        person_entry.table_version
    );

    let err = namespace
        .describe_table_version(DescribeTableVersionRequest {
            id: Some(vec!["node:Person".to_string()]),
            version: Some(person_version as i64),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"));

    let err = namespace
        .create_table_version(version_metadata.to_create_table_version_request(
            "node:Person",
            person_version,
            1,
            None,
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"));

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

    let inherited_company_ds = DatasetBuilder::from_namespace(
        Arc::clone(&feature_namespace),
        vec!["node:Company".to_string()],
    )
    .await
    .unwrap()
    .with_branch("feature", None)
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
    async fn publish(&self, updates: &[SubTableUpdate]) -> Result<Dataset> {
        let requests: Vec<CreateTableVersionRequest> = updates
            .iter()
            .map(SubTableUpdate::to_create_table_version_request)
            .collect();
        self.requests.lock().await.extend_from_slice(&requests);
        self.inner.publish_requests(&requests).await
    }
}

struct FailingPublisher;

#[async_trait]
impl ManifestBatchPublisher for FailingPublisher {
    async fn publish(&self, _updates: &[SubTableUpdate]) -> Result<Dataset> {
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
