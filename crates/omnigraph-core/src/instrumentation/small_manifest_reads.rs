use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance_table::format::{RowDatasetVersionMeta, RowIdMeta};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::error::{OmniError, Result};

const MAX_SCAN_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// A scan-local store for small manifest data files; larger or indirect layouts retain their reader.
pub async fn manifest_scan_dataset(dataset: &Dataset) -> Result<Dataset> {
    let Some(files) = small_files(dataset) else {
        return Ok(dataset.clone());
    };
    let original = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    if original.block_size() >= MAX_SCAN_FILE_BYTES as usize {
        return Ok(dataset.clone());
    }
    let Some(mut params) = dataset.store_params().cloned() else {
        return Ok(dataset.clone());
    };
    params.block_size = Some(MAX_SCAN_FILE_BYTES as usize);
    let (store, _) = lance::io::ObjectStore::from_uri_and_params(
        dataset.session().store_registry(),
        dataset.uri(),
        &params,
    )
    .await
    .map_err(OmniError::storage)?;
    if let Some(wrapper) = &params.object_store_wrapper {
        super::record_probed_store(wrapper, Arc::clone(&store));
    }
    let data_path = dataset.branch_location().path.clone().join("data");
    let sizes = files
        .into_iter()
        .map(|(path, size)| (data_path.clone().join(path), size))
        .collect();
    let mut bounded = store.as_ref().clone();
    bounded.inner = Arc::new(BoundedManifestStore {
        inner: Arc::clone(&store.inner),
        sizes,
    });
    Ok(dataset.with_object_store(Arc::new(bounded), Some(params)))
}

fn small_files(dataset: &Dataset) -> Option<HashMap<String, u64>> {
    if url::Url::parse(dataset.uri()).is_ok_and(|uri| uri.scheme() == "memory")
        || !dataset.manifest().base_paths.is_empty()
    {
        return None;
    }
    let mut files = HashMap::new();
    let mut total = 0u64;
    for fragment in dataset.manifest().fragments.iter() {
        if !fragment.overlays.is_empty()
            || fragment.deletion_file.is_some()
            || matches!(fragment.row_id_meta, Some(RowIdMeta::External(_)))
            || matches!(
                fragment.last_updated_at_version_meta,
                Some(RowDatasetVersionMeta::External(_))
            )
            || matches!(
                fragment.created_at_version_meta,
                Some(RowDatasetVersionMeta::External(_))
            )
        {
            return None;
        }
        for file in &fragment.files {
            if file.base_id.is_some() || files.contains_key(&file.path) {
                return None;
            }
            let size = file.file_size_bytes.get()?.get();
            total = total.checked_add(size)?;
            if total > MAX_SCAN_FILE_BYTES {
                return None;
            }
            files.insert(file.path.clone(), size);
        }
    }
    (!files.is_empty()).then_some(files)
}

#[derive(Debug)]
struct BoundedManifestStore {
    inner: Arc<dyn ObjectStore>,
    sizes: HashMap<Path, u64>,
}

impl fmt::Display for BoundedManifestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bounded manifest reads")
    }
}

fn invalid_data(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "bounded manifest reads",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    }
}

#[async_trait]
impl ObjectStore for BoundedManifestStore {
    async fn get_opts(
        &self,
        path: &Path,
        mut options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(&size) = self.sizes.get(path) else {
            return self.inner.get_opts(path, options).await;
        };
        if options.range.is_some() || options.head {
            return self.inner.get_opts(path, options).await;
        }
        options.range = Some(GetRange::Bounded(0..size));
        let result = self.inner.get_opts(path, options).await?;
        if result.meta.size != size || result.range != (0..size) {
            return Err(invalid_data(
                "manifest data size differs from its captured metadata",
            ));
        }
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let stream = futures::stream::try_unfold(
            (result.into_stream(), size),
            |(mut stream, remaining)| async move {
                match stream.try_next().await? {
                    Some(chunk) if chunk.len() as u64 > remaining => Err(invalid_data(
                        "manifest data response exceeds its bounded read",
                    )),
                    Some(chunk) => {
                        let remaining = remaining - chunk.len() as u64;
                        Ok(Some((chunk, (stream, remaining))))
                    }
                    None if remaining != 0 => Err(invalid_data(
                        "manifest data response is shorter than its captured size",
                    )),
                    None => Ok(None),
                }
            },
        );
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            meta,
            range: 0..size,
            attributes,
        })
    }

    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(invalid_data("manifest scan store is read-only"))
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(invalid_data("manifest scan store is read-only"))
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        futures::stream::once(async { Err(invalid_data("manifest scan store is read-only")) })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(invalid_data("manifest scan store is read-only"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn bounded_manifest_reads_validate_object_size_and_keep_other_paths_readable() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("main/data/file.lance");
        let metadata = Path::from("main/_versions/1.manifest");
        inner
            .put(&path, Bytes::from(vec![7; 5542]).into())
            .await
            .unwrap();
        inner
            .put(&metadata, Bytes::from_static(b"metadata").into())
            .await
            .unwrap();
        for declared in [5541, 5542, 5543] {
            let store = BoundedManifestStore {
                inner: inner.clone(),
                sizes: HashMap::from([(path.clone(), declared)]),
            };
            let result = store.get(&path).await;
            if declared == 5542 {
                assert_eq!(
                    result.unwrap().bytes().await.unwrap(),
                    Bytes::from(vec![7; 5542])
                );
            } else {
                assert!(result.is_err(), "declared={declared}");
            }
            assert_eq!(
                store.get(&metadata).await.unwrap().bytes().await.unwrap(),
                "metadata"
            );
            assert!(
                store
                    .put(&path, Bytes::from_static(b"rewrite").into())
                    .await
                    .is_err()
            );
        }
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy, Debug)]
    enum BodyCase {
        Exact,
        Short,
        Overflow,
        Fault,
    }

    #[derive(Debug)]
    struct BodyStore {
        inner: Arc<dyn ObjectStore>,
        case: BodyCase,
        requests: Arc<AtomicUsize>,
        polls: Arc<AtomicUsize>,
    }

    impl fmt::Display for BodyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "manifest body test store")
        }
    }

    #[async_trait]
    impl ObjectStore for BodyStore {
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            assert_eq!(options.range, Some(GetRange::Bounded(0..6)));
            let mut result = self.inner.get_opts(path, options).await?;
            let mut chunks = vec![Ok(Bytes::from_static(b"ab"))];
            match self.case {
                BodyCase::Exact => chunks.push(Ok(Bytes::from_static(b"cdef"))),
                BodyCase::Short => {}
                BodyCase::Overflow => chunks.push(Ok(Bytes::from_static(b"cdefg"))),
                BodyCase::Fault => chunks.push(Err(object_store::Error::Generic {
                    store: "manifest body fault",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected manifest body fault",
                    )),
                })),
            }
            let polls = Arc::clone(&self.polls);
            result.payload = GetResultPayload::Stream(
                futures::stream::iter(chunks)
                    .inspect(move |_| {
                        polls.fetch_add(1, Ordering::SeqCst);
                    })
                    .boxed(),
            );
            Ok(result)
        }

        async fn put_opts(
            &self,
            _: &Path,
            _: PutPayload,
            _: PutOptions,
        ) -> object_store::Result<PutResult> {
            Err(invalid_data("unexpected write through body test store"))
        }

        async fn put_multipart_opts(
            &self,
            _: &Path,
            _: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(invalid_data("unexpected write through body test store"))
        }

        fn delete_stream(
            &self,
            _: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            futures::stream::once(async {
                Err(invalid_data("unexpected delete through body test store"))
            })
            .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
            Err(invalid_data("unexpected copy through body test store"))
        }
    }

    #[tokio::test]
    async fn bounded_manifest_body_stays_lazy_and_preserves_stream_failures() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("main/data/file.lance");
        inner
            .put(&path, Bytes::from_static(b"abcdef").into())
            .await
            .unwrap();
        for case in [
            BodyCase::Exact,
            BodyCase::Short,
            BodyCase::Overflow,
            BodyCase::Fault,
        ] {
            let requests = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let store = BoundedManifestStore {
                inner: Arc::new(BodyStore {
                    inner: inner.clone(),
                    case,
                    requests: Arc::clone(&requests),
                    polls: Arc::clone(&polls),
                }),
                sizes: HashMap::from([(path.clone(), 6)]),
            };
            let result = store.get(&path).await.unwrap();
            assert_eq!(requests.load(Ordering::SeqCst), 1, "case={case:?}");
            assert_eq!(
                polls.load(Ordering::SeqCst),
                0,
                "get_opts consumed body: {case:?}"
            );
            assert_eq!(result.meta.size, 6);
            assert_eq!(result.range, 0..6);
            let bytes = result.bytes().await;
            match case {
                BodyCase::Exact => assert_eq!(bytes.unwrap(), Bytes::from_static(b"abcdef")),
                BodyCase::Short | BodyCase::Overflow => {
                    let object_store::Error::Generic { store, source } = bytes.unwrap_err() else {
                        panic!(
                            "bounded body mismatch must retain its invalid-data diagnosis: {case:?}"
                        );
                    };
                    assert_eq!(store, "bounded manifest reads");
                    assert_eq!(
                        source.downcast_ref::<std::io::Error>().unwrap().kind(),
                        std::io::ErrorKind::InvalidData,
                    );
                }
                BodyCase::Fault => {
                    let object_store::Error::Generic { store, source } = bytes.unwrap_err() else {
                        panic!("body fault must propagate as the original object-store error");
                    };
                    assert_eq!(store, "manifest body fault");
                    let source = source.downcast_ref::<std::io::Error>().unwrap();
                    assert_eq!(source.kind(), std::io::ErrorKind::ConnectionReset);
                    assert_eq!(source.to_string(), "injected manifest body fault");
                }
            }
            let expected_polls = if matches!(case, BodyCase::Short) {
                1
            } else {
                2
            };
            assert_eq!(
                polls.load(Ordering::SeqCst),
                expected_polls,
                "case={case:?}"
            );
            assert_eq!(requests.load(Ordering::SeqCst), 1, "case={case:?}");
        }
    }
}
