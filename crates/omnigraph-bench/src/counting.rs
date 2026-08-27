//! Logical object-store call counting for one measured operation.
//!
//! These counters observe calls at Lance's [`WrappingObjectStore`] seam. One
//! logical call may fan out into retries, pagination, or multipart requests
//! below this wrapper, so these values are deliberately never described as
//! physical requests, round trips, or cloud cost.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as StoreResult,
    UploadPart,
};
use serde::{Deserialize, Serialize};

/// Logical store calls since the preceding [`LogicalCallCounter::take`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalCallCounts {
    pub get: u64,
    pub put: u64,
    pub put_part: u64,
    pub head: u64,
    pub list: u64,
    pub delete: u64,
    pub copy: u64,
    pub rename: u64,
    pub multipart_complete: u64,
    pub multipart_abort: u64,
}

impl LogicalCallCounts {
    /// Whether the observed interval issued any operation that may mutate the
    /// wrapped object store.
    pub fn has_mutations(self) -> bool {
        self.put != 0
            || self.put_part != 0
            || self.delete != 0
            || self.copy != 0
            || self.rename != 0
            || self.multipart_complete != 0
            || self.multipart_abort != 0
    }
}

#[derive(Debug, Default)]
struct Tally {
    get: AtomicU64,
    put: AtomicU64,
    put_part: AtomicU64,
    head: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
    rename: AtomicU64,
    multipart_complete: AtomicU64,
    multipart_abort: AtomicU64,
}

/// Cloneable logical-call counter installed through `QueryIoProbes`.
#[derive(Debug, Default, Clone)]
pub struct LogicalCallCounter(Arc<Tally>);

impl LogicalCallCounter {
    /// Atomically read and reset all classes.
    pub fn take(&self) -> LogicalCallCounts {
        LogicalCallCounts {
            get: self.0.get.swap(0, Ordering::Relaxed),
            put: self.0.put.swap(0, Ordering::Relaxed),
            put_part: self.0.put_part.swap(0, Ordering::Relaxed),
            head: self.0.head.swap(0, Ordering::Relaxed),
            list: self.0.list.swap(0, Ordering::Relaxed),
            delete: self.0.delete.swap(0, Ordering::Relaxed),
            copy: self.0.copy.swap(0, Ordering::Relaxed),
            rename: self.0.rename.swap(0, Ordering::Relaxed),
            multipart_complete: self.0.multipart_complete.swap(0, Ordering::Relaxed),
            multipart_abort: self.0.multipart_abort.swap(0, Ordering::Relaxed),
        }
    }
}

impl WrappingObjectStore for LogicalCallCounter {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CountingStore {
            target,
            counter: self.clone(),
        })
    }
}

#[derive(Debug)]
struct CountingStore {
    target: Arc<dyn ObjectStore>,
    counter: LogicalCallCounter,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CountingStore({})", self.target)
    }
}

#[derive(Debug)]
struct CountingMultipart {
    inner: Box<dyn MultipartUpload>,
    counter: LogicalCallCounter,
}

#[async_trait]
impl MultipartUpload for CountingMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counter.0.put_part.fetch_add(1, Ordering::Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        self.counter
            .0
            .multipart_complete
            .fetch_add(1, Ordering::Relaxed);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> StoreResult<()> {
        self.counter
            .0
            .multipart_abort
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort().await
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.counter.0.put.fetch_add(1, Ordering::Relaxed);
        self.target.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        // RFC 0031/0039 classify initiation only at the physical-attempt
        // layer. The logical vocabulary starts with uploaded parts and records
        // completion/abort, so wrapping the upload itself is intentionally not
        // a logical count.
        let inner = self.target.put_multipart_opts(location, options).await?;
        Ok(Box::new(CountingMultipart {
            inner,
            counter: self.counter.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        if options.head {
            self.counter.0.head.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counter.0.get.fetch_add(1, Ordering::Relaxed);
        }
        self.target.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.counter.0.get.fetch_add(1, Ordering::Relaxed);
        self.target.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        // This is one logical delete-stream call, not a physical object count.
        self.counter.0.delete.fetch_add(1, Ordering::Relaxed);
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.counter.0.copy.fetch_add(1, Ordering::Relaxed);
        self.target.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        // Preserve the backend's atomic rename instead of inheriting the
        // trait's copy-plus-delete fallback merely for instrumentation.
        self.counter.0.rename.fetch_add(1, Ordering::Relaxed);
        self.target.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[derive(Debug)]
    struct RangeSpyStore {
        target: Arc<dyn ObjectStore>,
        get_opts: AtomicU64,
        get_ranges: AtomicU64,
    }

    impl fmt::Display for RangeSpyStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "RangeSpyStore({})", self.target)
        }
    }

    #[async_trait]
    impl ObjectStore for RangeSpyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.target.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            self.get_opts.fetch_add(1, Ordering::Relaxed);
            self.target.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> StoreResult<Vec<Bytes>> {
            self.get_ranges.fetch_add(1, Ordering::Relaxed);
            self.target.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.target.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.target.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.target.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn take_resets_every_tally() {
        let counter = LogicalCallCounter::default();
        counter.0.get.fetch_add(3, Ordering::Relaxed);
        counter.0.put.fetch_add(1, Ordering::Relaxed);
        counter.0.put_part.fetch_add(2, Ordering::Relaxed);

        assert_eq!(
            counter.take(),
            LogicalCallCounts {
                get: 3,
                put: 1,
                put_part: 2,
                ..Default::default()
            }
        );
        assert_eq!(counter.take(), LogicalCallCounts::default());
    }

    #[test]
    fn zero_classes_remain_explicit_in_json() {
        let value = serde_json::to_value(LogicalCallCounts::default()).unwrap();
        for class in [
            "get",
            "put",
            "put_part",
            "head",
            "list",
            "delete",
            "copy",
            "rename",
            "multipart_complete",
            "multipart_abort",
        ] {
            assert_eq!(value[class], 0, "missing zero-valued class {class}");
        }
    }

    #[tokio::test]
    async fn get_ranges_remains_one_delegated_logical_call() {
        let location = Path::from("ranges");
        let target = Arc::new(InMemory::new());
        target
            .put(&location, PutPayload::from_static(b"abcdef"))
            .await
            .unwrap();
        let spy = Arc::new(RangeSpyStore {
            target,
            get_opts: AtomicU64::new(0),
            get_ranges: AtomicU64::new(0),
        });
        let counter = LogicalCallCounter::default();
        let store = counter.wrap("test", spy.clone());

        let result = store.get_ranges(&location, &[0..2, 4..6]).await.unwrap();

        assert_eq!(
            result,
            vec![Bytes::from_static(b"ab"), Bytes::from_static(b"ef")]
        );
        assert_eq!(spy.get_ranges.load(Ordering::Relaxed), 1);
        assert_eq!(spy.get_opts.load(Ordering::Relaxed), 0);
        assert_eq!(
            counter.take(),
            LogicalCallCounts {
                get: 1,
                ..Default::default()
            }
        );
    }
}
