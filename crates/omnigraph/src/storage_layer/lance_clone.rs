//! Index-origin preservation at Lance's public shallow-clone commit boundary.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::io::ObjectStore;
use lance_core::{Error, Result};
use lance_table::format::pb::fragment_reuse_index_details::{Content, InlineContent};
use lance_table::format::pb::{FragmentReuseIndexDetails, transaction};
use lance_table::format::{BasePath, IndexMetadata, Manifest, Transaction};
use lance_table::io::commit::{
    CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter,
};
use lance_table::io::manifest::read_manifest_indexes;
use object_store::ObjectStore as RawObjectStore;
use object_store::path::Path;
use prost::Message;

tokio::task_local! {
    static CLONE_CONTEXT: CloneContext;
}

struct CloneContext {
    operation: transaction::Clone,
    target: Path,
    source_indices: Vec<IndexMetadata>,
    inline_fragment_reuse_details: HashMap<usize, Vec<u8>>,
    expected_bases: HashMap<u32, BasePath>,
    new_base_id: u32,
}

impl CloneContext {
    async fn capture(source: &Dataset, branch: &str, source_version: u64) -> Result<Self> {
        lance::dataset::refs::check_valid_branch(branch)?;
        if source.version().version != source_version {
            return Err(Error::invalid_input(format!(
                "clone source version changed: expected {source_version}, got {}",
                source.version().version,
            )));
        }
        let source_indices = if source.manifest().index_section.is_some() {
            let store = source.object_store(None).await?;
            read_manifest_indexes(&store, source.manifest_location(), source.manifest()).await?
        } else {
            Vec::new()
        };
        let mut inline_fragment_reuse_details = HashMap::new();
        for (ordinal, index) in source_indices.iter().enumerate() {
            if let Some(details) = normalize_fragment_reuse(source, index).await? {
                inline_fragment_reuse_details.insert(ordinal, details);
            }
        }
        let new_base_id = match source.manifest().base_paths.keys().max() {
            Some(id) => id
                .checked_add(1)
                .ok_or_else(|| Error::invalid_input("clone base ID overflow"))?,
            None => 0,
        };
        let operation = transaction::Clone {
            is_shallow: true,
            ref_name: source.manifest().branch.clone(),
            ref_version: source_version,
            ref_path: source.uri().to_string(),
            branch_name: Some(branch.to_string()),
        };
        let mut expected_bases = source.manifest().base_paths.clone();
        expected_bases.insert(
            new_base_id,
            BasePath::new(
                new_base_id,
                operation.ref_path.clone(),
                operation.ref_name.clone(),
                true,
            ),
        );
        Ok(Self {
            operation,
            target: source.branch_location().find_branch(Some(branch))?.path,
            source_indices,
            inline_fragment_reuse_details,
            expected_bases,
            new_base_id,
        })
    }

    fn correct(
        &self,
        manifest: &Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        transaction: &Transaction,
    ) -> Result<Option<Vec<IndexMetadata>>> {
        let transaction = transaction.as_pb();
        if base_path != &self.target
            || transaction.read_version != self.operation.ref_version
            || transaction.operation.as_ref()
                != Some(&transaction::Operation::Clone(self.operation.clone()))
            || manifest.branch != self.operation.branch_name
            || manifest.base_paths != self.expected_bases
        {
            return Err(Error::invalid_input(
                "shallow clone does not match its captured source and target",
            ));
        }
        let Some(mut indices) = indices else {
            return if self.source_indices.is_empty() {
                Ok(None)
            } else {
                Err(Error::invalid_input(
                    "shallow clone lost its captured index metadata",
                ))
            };
        };
        if indices.len() != self.source_indices.len() {
            return Err(Error::invalid_input(
                "shallow clone index inventory changed",
            ));
        }
        for (ordinal, (index, source)) in indices.iter_mut().zip(&self.source_indices).enumerate() {
            let expected_base = source.base_id.or(Some(self.new_base_id));
            if index.base_id != Some(self.new_base_id) && index.base_id != expected_base {
                return Err(Error::invalid_input(format!(
                    "shallow clone index {} has an unexpected base ID",
                    index.uuid
                )));
            }
            let mut comparable = index.clone();
            comparable.base_id = source.base_id;
            if comparable != *source {
                return Err(Error::invalid_input(format!(
                    "shallow clone changed metadata of index {} beyond its base ID",
                    index.uuid
                )));
            }
            index.base_id = expected_base;
            if let Some(inline) = self.inline_fragment_reuse_details.get(&ordinal) {
                let details = index
                    .index_details
                    .as_mut()
                    .ok_or_else(|| Error::index("captured fragment-reuse details disappeared"))?;
                Arc::make_mut(details).value = inline.clone();
            }
        }
        Ok(Some(indices))
    }
}

async fn normalize_fragment_reuse(
    source: &Dataset,
    index: &IndexMetadata,
) -> Result<Option<Vec<u8>>> {
    let Some(details) = index
        .index_details
        .as_ref()
        .filter(|details| details.type_url.ends_with("FragmentReuseIndexDetails"))
    else {
        return Ok(None);
    };
    let details = details.to_msg::<FragmentReuseIndexDetails>()?;
    match details.content {
        Some(Content::Inline(_)) => Ok(None),
        Some(Content::External(external)) => {
            let directory = match index.base_id {
                None => source.indices_dir(),
                Some(base_id) => {
                    let base = source.manifest().base_paths.get(&base_id).ok_or_else(|| {
                        Error::index(format!(
                            "fragment-reuse index {} refers to missing base {base_id}",
                            index.uuid
                        ))
                    })?;
                    let path = base.extract_path(source.session().store_registry())?;
                    if base.is_dataset_root {
                        path.join("_indices")
                    } else {
                        path
                    }
                }
            };
            let path = directory.join(index.uuid.to_string()).join(external.path);
            let store = source.object_store(index.base_id).await?;
            let reader = store.open(&path).await?;
            let range = fragment_reuse_range(external.offset, external.size, reader.size().await?)?;
            let bytes = reader.get_range(range).await?;
            let inline = InlineContent::decode(bytes)?;
            lance_table::system_index::frag_reuse::FragReuseIndexDetails::try_from(inline.clone())?;
            Ok(Some(
                FragmentReuseIndexDetails {
                    content: Some(Content::Inline(inline)),
                }
                .encode_to_vec(),
            ))
        }
        None => Err(Error::index("fragment-reuse index details have no content")),
    }
}

fn fragment_reuse_range(
    offset: u64,
    size: u64,
    file_size: usize,
) -> Result<std::ops::Range<usize>> {
    let end = offset
        .checked_add(size)
        .ok_or_else(|| Error::index("fragment-reuse detail range overflows"))?;
    let start = usize::try_from(offset)
        .map_err(|_| Error::index("fragment-reuse detail offset is not addressable"))?;
    let end = usize::try_from(end)
        .map_err(|_| Error::index("fragment-reuse detail end is not addressable"))?;
    if end > file_size {
        return Err(Error::index(format!(
            "fragment-reuse detail range ends at {end}, beyond file length {file_size}"
        )));
    }
    Ok(start..end)
}

#[derive(Debug)]
struct IndexOriginCommitHandler {
    inner: Arc<dyn CommitHandler>,
}

pub(crate) fn wrap_commit_handler(inner: Arc<dyn CommitHandler>) -> Arc<dyn CommitHandler> {
    Arc::new(IndexOriginCommitHandler { inner })
}

pub(crate) async fn configured_commit_handler(
    uri: &str,
    params: &Option<lance::io::ObjectStoreParams>,
    existing: Option<Arc<dyn CommitHandler>>,
) -> Result<Arc<dyn CommitHandler>> {
    let handler = match existing {
        Some(handler) => handler,
        None => lance_table::io::commit::commit_handler_from_url(uri, params).await?,
    };
    Ok(wrap_commit_handler(handler))
}

pub(crate) async fn write_params(
    uri: &str,
    mut params: lance::dataset::WriteParams,
) -> Result<lance::dataset::WriteParams> {
    params.commit_handler = Some(
        configured_commit_handler(uri, &params.store_params, params.commit_handler.take()).await?,
    );
    Ok(params)
}

pub(crate) async fn create_branch(
    source: &mut Dataset,
    branch: &str,
    source_version: u64,
) -> Result<Dataset> {
    let context = CloneContext::capture(source, branch, source_version).await?;
    CLONE_CONTEXT
        .scope(context, source.create_branch(branch, source_version, None))
        .await
}

#[async_trait]
impl CommitHandler for IndexOriginCommitHandler {
    fn is_version_not_found_definitive(&self) -> bool {
        self.inner.is_version_not_found_definitive()
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        self.inner.propagate_commit_error_after_success()
    }

    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<ManifestLocation> {
        self.inner
            .resolve_latest_location(base_path, object_store)
            .await
    }

    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn RawObjectStore,
    ) -> Result<ManifestLocation> {
        self.inner
            .resolve_version_location(base_path, version, object_store)
            .await
    }

    async fn version_exists(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn RawObjectStore,
        naming_scheme: ManifestNamingScheme,
    ) -> Result<bool> {
        self.inner
            .version_exists(base_path, version, object_store, naming_scheme)
            .await
    }

    fn list_detached_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_detached_manifest_locations(base_path, object_store)
    }

    fn list_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        sorted_descending: bool,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_manifest_locations(base_path, object_store, sorted_descending)
    }

    fn list_manifest_locations_since<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        since_version: u64,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_manifest_locations_since(base_path, object_store, since_version)
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let indices = match transaction.as_ref() {
            Some(transaction) if matches!(transaction.as_pb().operation.as_ref(), Some(transaction::Operation::Clone(operation)) if operation.is_shallow) => {
                CLONE_CONTEXT
                    .try_with(|context| context.correct(manifest, indices, base_path, transaction))
                    .map_err(|_| {
                        Error::invalid_input("shallow clone has no scoped source-index context")
                    })??
            }
            _ => {
                if CLONE_CONTEXT
                    .try_with(|context| context.target == *base_path)
                    .unwrap_or(false)
                {
                    return Err(Error::invalid_input(
                        "scoped shallow clone has no matching inline transaction",
                    )
                    .into());
                }
                indices
            }
        };
        self.inner
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
            )
            .await
    }

    async fn delete(&self, base_path: &Path) -> Result<()> {
        self.inner.delete(base_path).await
    }
}

#[cfg(test)]
mod tests {
    #[derive(Debug)]
    struct ConfiguredBackend;

    #[async_trait]
    impl CommitHandler for ConfiguredBackend {
        fn is_version_not_found_definitive(&self) -> bool {
            true
        }
        fn propagate_commit_error_after_success(&self) -> bool {
            false
        }

        async fn resolve_version_location(
            &self,
            _base_path: &Path,
            _version: u64,
            _object_store: &dyn RawObjectStore,
        ) -> Result<ManifestLocation> {
            Err(Error::io("configured resolver"))
        }

        async fn commit(
            &self,
            _manifest: &mut Manifest,
            _indices: Option<Vec<IndexMetadata>>,
            _base_path: &Path,
            _object_store: &ObjectStore,
            _writer: ManifestWriter,
            _scheme: ManifestNamingScheme,
            _transaction: Option<Transaction>,
        ) -> std::result::Result<ManifestLocation, CommitError> {
            Err(Error::io("configured commit").into())
        }
    }

    #[tokio::test]
    async fn creation_preserves_configured_handler_session_and_failure_semantics() {
        let session = Arc::new(Session::default());
        let params = write_params(
            "custom-commit-provider://dataset",
            WriteParams {
                commit_handler: Some(Arc::new(ConfiguredBackend)),
                session: Some(session.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(params.session.as_ref().unwrap(), &session));
        let handler = params.commit_handler.unwrap();
        assert!(handler.is_version_not_found_definitive());
        assert!(!handler.propagate_commit_error_after_success());
        let backend = object_store::memory::InMemory::new();
        let error = handler
            .resolve_version_location(&Path::from("dataset"), 1, &backend)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configured resolver"));
        let dir = tempfile::tempdir().unwrap();
        let mut source = fresh_dataset(dir.path().join("init.lance").to_str().unwrap()).await;
        let error = handler
            .commit(
                Arc::make_mut(&mut source.manifest),
                None,
                &Path::from("dataset"),
                &ObjectStore::from_uri("memory://delegation")
                    .await
                    .unwrap()
                    .0,
                lance_table::io::commit::write_manifest_file_to_path,
                ManifestNamingScheme::V1,
                None,
            )
            .await
            .unwrap_err();
        assert!(Error::from(error).to_string().contains("configured commit"));
    }

    #[tokio::test]
    async fn newly_created_dataset_can_fork_twice_without_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("fresh.lance");
        let uri = uri.to_str().unwrap();
        let mut source = fresh_dataset(uri).await;
        source
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        let version = source.version().version;
        let mut first = create_branch(&mut source, "first", version).await.unwrap();
        let version = first.version().version;
        let second = create_branch(&mut first, "second", version).await.unwrap();
        let index = second.load_indices().await.unwrap()[0].clone();
        assert_eq!(
            second.manifest().base_paths[&index.base_id.unwrap()].path,
            source.uri()
        );
        let mut scan = second.scan();
        scan.filter("value = 1").unwrap();
        assert!(
            scan.explain_plan(true)
                .await
                .unwrap()
                .contains("ScalarIndexQuery")
        );
        assert_eq!(scan.try_into_batch().await.unwrap().num_rows(), 1);
    }
    use super::*;
    use arrow_array::{FixedSizeListArray, Float32Array};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance::dataset::builder::DatasetBuilder;
    use lance::dataset::{WriteMode, WriteParams};
    use lance::index::DatasetIndexExt;
    use lance::index::vector::VectorIndexParams;
    use lance::session::Session;
    use lance_file::version::LanceFileVersion;
    use lance_index::IndexType;
    use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams};
    use lance_linalg::distance::MetricType;
    async fn fresh_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let params = write_params(uri, params).await.unwrap();
        Dataset::write(reader, uri, Some(params)).await.unwrap()
    }

    async fn append_guard_row(dataset: &mut Dataset, id: &str, value: i32) {
        let schema = Arc::new(Schema::from(dataset.schema()));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        dataset
            .append(
                reader,
                Some(WriteParams {
                    mode: WriteMode::Append,
                    enable_stable_row_ids: true,
                    data_storage_version: Some(LanceFileVersion::V2_2),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn second_and_third_generation_clone_preserves_inherited_and_local_index_bases() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("guard24.lance");
        let uri = uri.to_str().unwrap();
        let mut ds = fresh_dataset(uri).await;
        ds.create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        let mut ds = engine_open(uri).await;
        let root_indices = ds.load_indices().await.unwrap();
        assert_eq!(root_indices.len(), 1);
        let inherited_uuid = root_indices[0].uuid;
        assert_eq!(root_indices[0].base_id, None);

        let version = ds.version().version;
        let mut feature = create_branch(&mut ds, "feature", version).await.unwrap();
        append_guard_row(&mut feature, "carol", 3).await;
        feature
            .create_index_builder(&["id"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        let feature_indices = feature.load_indices().await.unwrap();
        assert_eq!(feature_indices.len(), 2);
        let inherited = feature_indices
            .iter()
            .find(|index| index.uuid == inherited_uuid)
            .unwrap();
        let inherited_base = inherited
            .base_id
            .expect("first clone redirects its inherited index");
        let local = feature_indices
            .iter()
            .find(|index| index.uuid != inherited_uuid)
            .unwrap();
        assert_eq!(
            local.base_id, None,
            "newly built index belongs to feature's own tree"
        );
        let local_uuid = local.uuid;
        let feature_version = feature.version().version;
        let mut experiment = create_branch(&mut feature, "experiment", feature_version)
            .await
            .unwrap();

        async fn indexed_rows(dataset: &Dataset, filter: &str) -> Vec<(String, i32)> {
            let mut scanner = dataset.scan();
            scanner.filter(filter).unwrap();
            let plan = scanner.explain_plan(true).await.unwrap();
            assert!(
                plan.contains("ScalarIndexQuery"),
                "regression must open the index: {plan}"
            );
            let batches: Vec<RecordBatch> = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                rows.extend(
                    (0..batch.num_rows())
                        .map(|row| (ids.value(row).to_string(), values.value(row))),
                );
            }
            rows.sort_unstable();
            rows
        }

        let reopened = DatasetBuilder::from_uri(uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap()
            .checkout_branch("experiment")
            .await
            .unwrap();
        for dataset in [&experiment, &reopened] {
            let indices = dataset.load_indices().await.unwrap();
            assert_eq!(indices.len(), 2);
            let inherited = indices
                .iter()
                .find(|index| index.uuid == inherited_uuid)
                .unwrap();
            assert_eq!(inherited.base_id, Some(inherited_base));
            assert_eq!(
                dataset.manifest().base_paths[&inherited_base].path,
                feature.manifest().base_paths[&inherited_base].path,
            );
            let local = indices
                .iter()
                .find(|index| index.uuid == local_uuid)
                .unwrap();
            let local_base = local
                .base_id
                .expect("second clone redirects feature's local index");
            assert_ne!(local_base, inherited_base);
            let local_path = &dataset.manifest().base_paths[&local_base];
            assert!(local_path.is_dataset_root);
            assert_eq!(local_path.path, feature.uri());
            assert_eq!(
                indexed_rows(dataset, "value = 1").await,
                vec![("alice".to_string(), 1)]
            );
            assert_eq!(
                indexed_rows(dataset, "id = 'carol'").await,
                vec![("carol".to_string(), 3)]
            );
        }
        let experiment_indices = experiment.load_indices().await.unwrap();
        let version = experiment.version().version;
        let third = create_branch(&mut experiment, "third", version)
            .await
            .unwrap();
        let third_reopened = DatasetBuilder::from_uri(uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap()
            .checkout_branch("third")
            .await
            .unwrap();
        for dataset in [&third, &third_reopened] {
            let indices = dataset.load_indices().await.unwrap();
            assert_eq!(indices.len(), experiment_indices.len());
            for source_index in experiment_indices.iter() {
                let inherited = indices
                    .iter()
                    .find(|index| index.uuid == source_index.uuid)
                    .unwrap();
                assert!(source_index.base_id.is_some());
                assert_eq!(inherited.base_id, source_index.base_id);
                let base = source_index.base_id.unwrap();
                assert_eq!(
                    dataset.manifest().base_paths[&base],
                    experiment.manifest().base_paths[&base]
                );
            }
            assert_eq!(
                indexed_rows(dataset, "value = 1").await,
                vec![("alice".to_string(), 1)]
            );
            assert_eq!(
                indexed_rows(dataset, "id = 'carol'").await,
                vec![("carol".to_string(), 3)]
            );
        }
        append_guard_row(&mut experiment, "dave", 4).await;
        let after_append = DatasetBuilder::from_uri(uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap()
            .checkout_branch("experiment")
            .await
            .unwrap();
        assert_eq!(
            indexed_rows(&after_append, "id = 'dave'").await,
            vec![("dave".to_string(), 4)]
        );
    }

    async fn engine_open(uri: &str) -> Dataset {
        crate::instrumentation::open_dataset(
            uri,
            crate::instrumentation::VersionResolution::Latest,
            Some(&Arc::new(Session::default())),
            None,
        )
        .await
        .unwrap()
    }

    async fn indexed_source(uri: &str) -> Dataset {
        let mut dataset = fresh_dataset(uri).await;
        dataset
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        engine_open(uri).await
    }

    #[tokio::test]
    async fn clone_context_is_required_and_bound_before_manifest_publication() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("scoped.lance");
        let uri = uri.to_str().unwrap();
        let mut source = indexed_source(uri).await;
        let version = source.version().version;
        let missing = source
            .create_branch("missing", version, None)
            .await
            .unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("no scoped source-index context"),
            "{missing}"
        );
        let context = CloneContext::capture(&source, "expected", version)
            .await
            .unwrap();
        let mismatched = CLONE_CONTEXT
            .scope(context, source.create_branch("wrong", version, None))
            .await
            .unwrap_err();
        assert!(
            mismatched
                .to_string()
                .contains("does not match its captured source and target"),
            "{mismatched}"
        );
        assert!(source.list_branches().await.unwrap().is_empty());
        assert_eq!(source.version().version, version);
        for branch in ["missing", "wrong"] {
            assert!(source.checkout_branch(branch).await.is_err());
        }
        assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
        let good = create_branch(&mut source, "good", version).await.unwrap();
        assert_eq!(good.count_rows(None).await.unwrap(), 2);
        assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
    }

    #[tokio::test]
    async fn concurrent_clone_contexts_keep_each_source_index_origin() {
        async fn branch_twice(uri: String) -> Dataset {
            let mut source = indexed_source(&uri).await;
            let source_index = source.load_indices().await.unwrap()[0].uuid;
            let version = source.version().version;
            let mut first = create_branch(&mut source, "first", version).await.unwrap();
            tokio::task::yield_now().await;
            let version = first.version().version;
            let second = create_branch(&mut first, "second", version).await.unwrap();
            let index = second.load_indices().await.unwrap()[0].clone();
            assert_eq!(index.uuid, source_index);
            assert_eq!(
                second.manifest().base_paths[&index.base_id.unwrap()].path,
                source.uri()
            );
            assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
            second
        }
        let dir = tempfile::tempdir().unwrap();
        let left_uri = dir.path().join("left.lance").to_str().unwrap().to_string();
        let right_uri = dir.path().join("right.lance").to_str().unwrap().to_string();
        let (left, right) = tokio::join!(
            tokio::spawn(branch_twice(left_uri)),
            tokio::spawn(branch_twice(right_uri))
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(
            left.load_indices().await.unwrap()[0].uuid,
            right.load_indices().await.unwrap()[0].uuid
        );
    }

    #[tokio::test]
    async fn source_capture_reads_only_index_metadata_and_append_cost_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let empty_uri = dir.path().join("empty.lance");
        fresh_dataset(empty_uri.to_str().unwrap()).await;
        let empty = engine_open(empty_uri.to_str().unwrap()).await;
        let store = empty.object_store(None).await.unwrap();
        store.io_stats_incremental();
        CloneContext::capture(&empty, "child", empty.version().version)
            .await
            .unwrap();
        let empty_capture = store.io_stats_incremental();
        assert_eq!(empty_capture.read_iops, 0, "{empty_capture:?}");
        assert_eq!(empty_capture.write_iops, 0, "{empty_capture:?}");

        let indexed_uri = dir.path().join("indexed.lance");
        let indexed = indexed_source(indexed_uri.to_str().unwrap()).await;
        let store = indexed.object_store(None).await.unwrap();
        store.io_stats_incremental();
        CloneContext::capture(&indexed, "child", indexed.version().version)
            .await
            .unwrap();
        let indexed_capture = store.io_stats_incremental();
        assert_eq!(
            indexed_capture.read_iops, 1,
            "small indexed manifest capture: {indexed_capture:?}"
        );
        assert_eq!(indexed_capture.write_iops, 0, "{indexed_capture:?}");

        let raw_uri = dir.path().join("raw.lance");
        let engine_uri = dir.path().join("eng.lance");
        fresh_dataset(raw_uri.to_str().unwrap()).await;
        fresh_dataset(engine_uri.to_str().unwrap()).await;
        let mut raw = DatasetBuilder::from_uri(raw_uri.to_str().unwrap())
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();
        let mut engine = engine_open(engine_uri.to_str().unwrap()).await;
        let raw_store = raw.object_store(None).await.unwrap();
        let engine_store = engine.object_store(None).await.unwrap();
        raw_store.io_stats_incremental();
        engine_store.io_stats_incremental();
        append_guard_row(&mut raw, "carol", 3).await;
        let raw_cost = raw_store.io_stats_incremental();
        append_guard_row(&mut engine, "carol", 3).await;
        let engine_cost = engine_store.io_stats_incremental();
        assert_eq!(
            (engine_cost.read_iops, engine_cost.write_iops),
            (raw_cost.read_iops, raw_cost.write_iops),
            "raw={raw_cost:?}, engine={engine_cost:?}"
        );
        for source in [&mut raw, &mut engine] {
            source
                .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
                .replace(true)
                .await
                .unwrap();
        }
        for branch in ["first", "second"] {
            let raw_store = raw.object_store(None).await.unwrap();
            let engine_store = engine.object_store(None).await.unwrap();
            raw_store.io_stats_incremental();
            engine_store.io_stats_incremental();
            let version = raw.version().version;
            raw = raw.create_branch(branch, version, None).await.unwrap();
            let raw_clone_cost = raw_store.io_stats_incremental();
            let version = engine.version().version;
            engine = create_branch(&mut engine, branch, version).await.unwrap();
            let engine_clone_cost = engine_store.io_stats_incremental();
            assert!(raw_clone_cost.write_iops > 0, "{raw_clone_cost:?}");
            assert_eq!(
                engine_clone_cost.write_iops, raw_clone_cost.write_iops,
                "{branch}: raw={raw_clone_cost:?}, engine={engine_clone_cost:?}"
            );
        }
        assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
    }

    async fn external_fragment_reuse_source(
        uri: &str,
        payload: Vec<u8>,
        offset: u64,
        size: u64,
    ) -> Dataset {
        let mut source = fresh_dataset(uri).await;
        source
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        let mut metadata = source.load_indices().await.unwrap()[0].clone();
        metadata.uuid = "27a30b70-7fb8-4ee3-a731-35a1bcff9260".parse().unwrap();
        metadata.name = lance_table::system_index::frag_reuse::FRAG_REUSE_INDEX_NAME.to_string();
        metadata.fields.clear();
        metadata.covering_fields.clear();
        metadata.dataset_version = source.version().version;
        metadata.index_version = 0;
        metadata.base_id = None;
        metadata.files = None;
        let external = lance_table::format::pb::ExternalFile {
            path: "details.binpb".to_string(),
            offset,
            size,
        };
        let proto = FragmentReuseIndexDetails {
            content: Some(Content::External(external)),
        };
        let details = Arc::make_mut(metadata.index_details.as_mut().unwrap());
        details.type_url = "type.googleapis.com/lance.table.FragmentReuseIndexDetails".to_string();
        details.value = proto.encode_to_vec();
        let path = source
            .indices_dir()
            .join(metadata.uuid.to_string())
            .join("details.binpb");
        source
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put_opts(&path, payload.into(), object_store::PutOptions::default())
            .await
            .unwrap();
        Dataset::commit(
            uri,
            lance::dataset::transaction::Operation::CreateIndex {
                new_indices: vec![metadata],
                removed_indices: vec![],
            },
            Some(source.version().version),
            source.store_params().cloned(),
            None,
            source.session().clone(),
            false,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn external_fragment_reuse_is_inlined_from_local_and_inherited_origins() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("external-fri.lance");
        let uri = uri.to_str().unwrap();
        let inline = InlineContent {
            versions: vec![
                lance_table::format::pb::fragment_reuse_index_details::Version {
                    dataset_version: 1,
                    groups: vec![],
                },
            ],
        };
        let payload = inline.encode_to_vec();
        let payload_size = u64::try_from(payload.len()).unwrap();
        let mut raw = external_fragment_reuse_source(uri, payload, 0, payload_size).await;
        let version = raw.version().version;
        let stock_first = raw
            .create_branch("stock_first", version, None)
            .await
            .unwrap();
        let mut local = engine_open(uri).await;
        let mut inherited = engine_open(stock_first.uri()).await;
        for (source, branch) in [
            (&mut local, "local_normalized"),
            (&mut inherited, "inherited_normalized"),
        ] {
            let source_indices = read_manifest_indexes(
                &source.object_store(None).await.unwrap(),
                source.manifest_location(),
                source.manifest(),
            )
            .await
            .unwrap();
            let source_fri = source_indices
                .iter()
                .find(|index| {
                    index.name == lance_table::system_index::frag_reuse::FRAG_REUSE_INDEX_NAME
                })
                .unwrap();
            assert!(matches!(
                source_fri
                    .index_details
                    .as_ref()
                    .unwrap()
                    .to_msg::<FragmentReuseIndexDetails>()
                    .unwrap()
                    .content,
                Some(Content::External(_))
            ));
            let version = source.version().version;
            let child = create_branch(source, branch, version).await.unwrap();
            let cold = DatasetBuilder::from_uri(uri)
                .with_session(Arc::new(Session::default()))
                .load()
                .await
                .unwrap()
                .checkout_branch(branch)
                .await
                .unwrap();
            for dataset in [&child, &cold] {
                let store = dataset.object_store(None).await.unwrap();
                let indices =
                    read_manifest_indexes(&store, dataset.manifest_location(), dataset.manifest())
                        .await
                        .unwrap();
                let fri = indices
                    .iter()
                    .find(|index| index.uuid == source_fri.uuid)
                    .unwrap();
                let proto = fri
                    .index_details
                    .as_ref()
                    .unwrap()
                    .to_msg::<FragmentReuseIndexDetails>()
                    .unwrap();
                assert_eq!(proto.content, Some(Content::Inline(inline.clone())));
                let base = fri.base_id.unwrap();
                assert_eq!(dataset.manifest().base_paths[&base].path, raw.uri());
                let loaded = lance::index::frag_reuse::load_frag_reuse_index_details(dataset, fri)
                    .await
                    .unwrap();
                assert_eq!(loaded.versions.len(), 1);
                assert_eq!(loaded.versions[0].dataset_version, 1);
                let mut scanner = dataset.scan();
                scanner.filter("value = 1").unwrap();
                assert!(
                    scanner
                        .explain_plan(true)
                        .await
                        .unwrap()
                        .contains("ScalarIndexQuery")
                );
                assert_eq!(scanner.try_into_batch().await.unwrap().num_rows(), 1);
            }
            let original = read_manifest_indexes(
                &source.object_store(None).await.unwrap(),
                source.manifest_location(),
                source.manifest(),
            )
            .await
            .unwrap();
            assert_eq!(
                original
                    .iter()
                    .find(|index| index.uuid == source_fri.uuid)
                    .unwrap(),
                source_fri
            );
        }
    }

    #[tokio::test]
    async fn malformed_external_fragment_reuse_refuses_before_native_create() {
        for (label, payload, offset, size) in [
            ("malformed", vec![0xff], 0, 1),
            ("overflow", vec![0], u64::MAX, 1),
            ("truncated", vec![0], 0, 2),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().join(format!("{label}.lance"));
            let uri = uri.to_str().unwrap();
            external_fragment_reuse_source(uri, payload, offset, size).await;
            let mut source = engine_open(uri).await;
            let version = source.version().version;
            assert!(
                create_branch(&mut source, "refused", version)
                    .await
                    .is_err()
            );
            assert!(source.list_branches().await.unwrap().is_empty());
            let target = source
                .branch_location()
                .find_branch(Some("refused"))
                .unwrap();
            assert!(!std::path::Path::new(&target.uri).exists());
            assert_eq!(source.version().version, version);
        }
    }

    #[test]
    fn fragment_reuse_ranges_validate_file_bounds_before_reading() {
        assert_eq!(fragment_reuse_range(2, 3, 5).unwrap(), 2..5);
        assert_eq!(fragment_reuse_range(5, 0, 5).unwrap(), 5..5);
        assert!(fragment_reuse_range(2, 4, 5).is_err());
        assert!(fragment_reuse_range(u64::MAX, 1, usize::MAX).is_err());
    }

    #[tokio::test]
    async fn inherited_full_text_and_vector_indexes_survive_two_forks_and_cold_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("search.lance");
        let uri = uri.to_str().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(
                (0..256)
                    .flat_map(|row| [row as f32, 0.0])
                    .collect::<Vec<_>>(),
            )),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..256).map(|row| format!("row-{row}")).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..256)
                        .map(|row| if row == 0 { "needle" } else { "other" })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let mut source = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                enable_stable_row_ids: true,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        source
            .create_index_builder(
                &["text"],
                IndexType::Inverted,
                &InvertedIndexParams::default(),
            )
            .replace(true)
            .await
            .unwrap();
        source
            .create_index_builder(
                &["vector"],
                IndexType::Vector,
                &VectorIndexParams::ivf_flat(1, MetricType::L2),
            )
            .replace(true)
            .await
            .unwrap();
        let original = source.load_indices().await.unwrap();
        let mut source = engine_open(uri).await;
        let version = source.version().version;
        let mut first = create_branch(&mut source, "first", version).await.unwrap();
        let version = first.version().version;
        let second = create_branch(&mut first, "second", version).await.unwrap();
        let cold = DatasetBuilder::from_uri(uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap()
            .checkout_branch("second")
            .await
            .unwrap();
        for dataset in [&second, &cold] {
            let indices = dataset.load_indices().await.unwrap();
            assert_eq!(indices.len(), original.len());
            for index in indices.iter() {
                assert!(original.iter().any(|original| original.uuid == index.uuid));
                assert_eq!(
                    dataset.manifest().base_paths[&index.base_id.unwrap()].path,
                    source.uri()
                );
            }
            let mut text = dataset.scan();
            text.project(&["id"]).unwrap();
            text.full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();
            let text_plan = text.explain_plan(true).await.unwrap();
            assert!(
                text_plan.contains("MatchQuery:") && !text_plan.contains("FlatMatchQuery"),
                "{text_plan}"
            );
            let result = text.try_into_batch().await.unwrap();
            assert_eq!(result.num_rows(), 1);
            assert_eq!(
                result
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "row-0"
            );
            let mut vector = dataset.scan();
            vector.project(&["id"]).unwrap();
            vector
                .nearest("vector", &Float32Array::from(vec![0.0, 0.0]), 1)
                .unwrap();
            let vector_plan = vector.explain_plan(true).await.unwrap();
            assert!(vector_plan.contains("ANNIvf"), "{vector_plan}");
            let result = vector.try_into_batch().await.unwrap();
            assert_eq!(result.num_rows(), 1);
            assert_eq!(
                result
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "row-0"
            );
        }
    }
}
