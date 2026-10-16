//! Statement-scoped memo of identical immutable-object reads: validation
//! re-fetches the same byte ranges across one statement's scans; the memo
//! serves the repeat from the statement's admitted reuse allowance.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};

/// Charged per entry beside its payload bytes, covering key and metadata.
const ENTRY_OVERHEAD_BYTES: u64 = 128;

/// Path components whose objects are immutable once written (uuid-named data,
/// index and deletion files); only reads under them are memoized.
const IMMUTABLE_COMPONENTS: [&str; 3] = ["/data/", "/_indices/", "/_deletions/"];

#[derive(Clone, PartialEq, Eq, Hash)]
enum KeyRange {
    Whole,
    Bounded(u64, u64),
    Offset(u64),
    Suffix(u64),
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct MemoKey {
    store_prefix: String,
    path: String,
    range: KeyRange,
}

#[derive(Clone)]
struct MemoEntry {
    meta: ObjectMeta,
    range: Range<u64>,
    attributes: Attributes,
    bytes: Bytes,
}

impl MemoEntry {
    fn into_result(self) -> GetResult {
        GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(self.bytes) }).boxed(),
            ),
            meta: self.meta,
            range: self.range,
            attributes: self.attributes,
        }
    }
}

struct MemoState {
    entries: HashMap<MemoKey, MemoEntry>,
    remaining_bytes: u64,
}

/// One statement's read memo: a byte-budgeted map of completed immutable-path
/// reads. When the budget cannot admit an entry the read passes through
/// unretained, so a hot early range never blocks the statement.
pub struct RangeReadMemo {
    state: Mutex<MemoState>,
    _lease: Arc<dyn fmt::Debug + Send + Sync>,
}

impl RangeReadMemo {
    pub fn new(budget_bytes: u64) -> Arc<Self> {
        Self::with_lease(budget_bytes, Arc::new(()))
    }

    /// Keep the caller's admission alive as long as any wrapped store can
    /// retain memoized bytes. The lease need not expose its implementation.
    pub fn with_lease(budget_bytes: u64, lease: Arc<dyn fmt::Debug + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(MemoState {
                entries: HashMap::new(),
                remaining_bytes: budget_bytes,
            }),
            _lease: lease,
        })
    }

    fn lookup(&self, key: &MemoKey) -> Option<MemoEntry> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.get(key).cloned()
    }

    fn reserve(&self, cost: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cost > state.remaining_bytes {
            return false;
        }
        state.remaining_bytes -= cost;
        true
    }

    fn release(&self, cost: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.remaining_bytes = state.remaining_bytes.saturating_add(cost);
    }

    /// Insert under an already-made reservation; a concurrent double insert
    /// keeps one entry and refunds the duplicate's cost.
    fn insert(&self, key: MemoKey, entry: MemoEntry, cost: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.entries.insert(key, entry).is_some() {
            state.remaining_bytes = state.remaining_bytes.saturating_add(cost);
        }
    }
}

impl fmt::Debug for RangeReadMemo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeReadMemo").finish_non_exhaustive()
    }
}

/// Installs a [`RangeReadMemo`] over every table store a statement opens; the
/// wrapper's identity keys Lance's store cache, so one statement's stores are
/// never served to another.
#[derive(Debug)]
pub struct RangeMemoWrapper {
    memo: Weak<RangeReadMemo>,
}

impl RangeMemoWrapper {
    pub fn new(memo: &Arc<RangeReadMemo>) -> Arc<Self> {
        Arc::new(Self {
            memo: Arc::downgrade(memo),
        })
    }
}

impl WrappingObjectStore for RangeMemoWrapper {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let Some(memo) = self.memo.upgrade() else {
            return original;
        };
        Arc::new(RangeMemoStore {
            memo,
            store_prefix: store_prefix.to_string(),
            inner: original,
        })
    }
}

/// Stacks two store wrappers, `outer` over `inner`, into one
/// [`WrappingObjectStore`] slot.
#[derive(Debug)]
pub struct StackedWrapper {
    outer: Arc<dyn WrappingObjectStore>,
    inner: Arc<dyn WrappingObjectStore>,
}

impl StackedWrapper {
    pub fn new(
        outer: Arc<dyn WrappingObjectStore>,
        inner: Arc<dyn WrappingObjectStore>,
    ) -> Arc<Self> {
        Arc::new(Self { outer, inner })
    }
}

impl WrappingObjectStore for StackedWrapper {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.outer
            .wrap(store_prefix, self.inner.wrap(store_prefix, original))
    }
}

struct RangeMemoStore {
    memo: Arc<RangeReadMemo>,
    store_prefix: String,
    inner: Arc<dyn ObjectStore>,
}

impl RangeMemoStore {
    /// `None` when the request or path is not memoizable: conditional,
    /// versioned or `head` requests, and any path outside the immutable
    /// components, always pass through.
    fn memo_key(&self, location: &Path, options: &GetOptions) -> Option<MemoKey> {
        if options.head
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return None;
        }
        let path = location.as_ref();
        if !IMMUTABLE_COMPONENTS
            .iter()
            .any(|component| path.contains(component))
        {
            return None;
        }
        let range = match &options.range {
            None => KeyRange::Whole,
            Some(GetRange::Bounded(bounded)) => KeyRange::Bounded(bounded.start, bounded.end),
            Some(GetRange::Offset(offset)) => KeyRange::Offset(*offset),
            Some(GetRange::Suffix(suffix)) => KeyRange::Suffix(*suffix),
        };
        Some(MemoKey {
            store_prefix: self.store_prefix.clone(),
            path: path.to_string(),
            range,
        })
    }
}

impl fmt::Debug for RangeMemoStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeMemoStore").finish_non_exhaustive()
    }
}

impl fmt::Display for RangeMemoStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RangeMemo({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RangeMemoStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(key) = self.memo_key(location, &options) else {
            return self.inner.get_opts(location, options).await;
        };
        if let Some(entry) = self.memo.lookup(&key) {
            return Ok(entry.into_result());
        }
        let result = self.inner.get_opts(location, options).await?;
        let cost = result
            .range
            .end
            .saturating_sub(result.range.start)
            .saturating_add(ENTRY_OVERHEAD_BYTES);
        if !self.memo.reserve(cost) {
            return Ok(result);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        match result.bytes().await {
            Ok(bytes) => {
                let entry = MemoEntry {
                    meta,
                    range,
                    attributes,
                    bytes,
                };
                self.memo.insert(key, entry.clone(), cost);
                Ok(entry.into_result())
            }
            Err(error) => {
                self.memo.release(cost);
                Err(error)
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        gets: AtomicU64,
    }

    impl CountingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                gets: AtomicU64::new(0),
            })
        }
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Counting({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    const DATA_PATH: &str = "tables/x/data/aaaa.lance";

    async fn seed(counting: &Arc<CountingStore>, path: &str) {
        counting
            .inner
            .put(&Path::from(path), PutPayload::from_static(&[7u8; 256]))
            .await
            .expect("seed object");
    }

    fn wrapped(counting: &Arc<CountingStore>, budget: u64) -> Arc<dyn ObjectStore> {
        let memo = RangeReadMemo::new(budget);
        RangeMemoWrapper::new(&memo).wrap("test", Arc::clone(counting) as Arc<dyn ObjectStore>)
    }

    fn bounded(start: u64, end: u64) -> GetOptions {
        GetOptions {
            range: Some(GetRange::Bounded(start..end)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn identical_immutable_reads_reach_the_store_once() {
        let counting = CountingStore::new();
        seed(&counting, DATA_PATH).await;
        let store = wrapped(&counting, 1024 * 1024);
        let path = Path::from(DATA_PATH);
        let first = store.get_opts(&path, bounded(16, 48)).await.unwrap();
        let first_bytes = first.bytes().await.unwrap();
        let second = store.get_opts(&path, bounded(16, 48)).await.unwrap();
        assert_eq!(first_bytes, second.bytes().await.unwrap());
        assert_eq!(counting.gets.load(Ordering::Relaxed), 1);
        let whole = store.get(&path).await.unwrap();
        whole.bytes().await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(counting.gets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn conditional_head_and_mutable_paths_pass_through() {
        let counting = CountingStore::new();
        seed(&counting, DATA_PATH).await;
        seed(&counting, "tables/x/_versions/1.manifest").await;
        let store = wrapped(&counting, 1024 * 1024);
        let manifest = Path::from("tables/x/_versions/1.manifest");
        store.get_opts(&manifest, bounded(0, 8)).await.unwrap();
        store.get_opts(&manifest, bounded(0, 8)).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::Relaxed), 2);
        let data = Path::from(DATA_PATH);
        store.head(&data).await.unwrap();
        store.head(&data).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn an_exhausted_budget_skips_caching() {
        let counting = CountingStore::new();
        seed(&counting, DATA_PATH).await;
        let store = wrapped(&counting, 8);
        let path = Path::from(DATA_PATH);
        store
            .get_opts(&path, bounded(0, 64))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .get_opts(&path, bounded(0, 64))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(counting.gets.load(Ordering::Relaxed), 2);
    }
    /// Observes registry owners and lease destruction, which GQT cannot inspect.
    #[tokio::test]
    async fn registry_keys_do_not_retain_memo_and_live_stores_keep_the_lease() {
        use lance::io::{ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreRegistry};
        use std::sync::Weak;

        #[derive(Debug)]
        struct SweepLease {
            registry: Weak<ObjectStoreRegistry>,
        }
        impl Drop for SweepLease {
            fn drop(&mut self) {
                if let Some(registry) = self.registry.upgrade() {
                    drop(registry.active_stores());
                }
            }
        }

        let registry = Arc::new(ObjectStoreRegistry::default());
        let original_provider = registry.get_provider("memory").unwrap();
        let lease = Arc::new(SweepLease {
            registry: Arc::downgrade(&registry),
        });
        let weak_lease = Arc::downgrade(&lease);
        let memo = RangeReadMemo::with_lease(4096, lease.clone());
        let weak_memo = Arc::downgrade(&memo);
        let wrapper = RangeMemoWrapper::new(&memo);
        let weak_wrapper = Arc::downgrade(&wrapper);
        let params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..Default::default()
        };
        let (store, _) = LanceObjectStore::from_uri_and_params(
            registry.clone(),
            "memory://memo-lifetime/table",
            &params,
        )
        .await
        .unwrap();
        let data = Path::from("table/data/probe.lance");
        store
            .inner
            .put(&data, PutPayload::from_static(&[7_u8; 256]))
            .await
            .unwrap();
        assert_eq!(
            store
                .inner
                .get(&data)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .len(),
            256
        );
        assert_eq!(memo.state.lock().unwrap().entries.len(), 1);

        drop(params);
        drop(wrapper);
        drop(memo);
        drop(lease);
        assert!(weak_memo.upgrade().is_some());
        assert!(weak_lease.upgrade().is_some());
        assert_eq!(registry.stats().active_stores, 1);

        drop(store);
        assert!(weak_memo.upgrade().is_none());
        assert!(weak_lease.upgrade().is_none());
        assert!(weak_wrapper.upgrade().is_none());
        assert_eq!(registry.stats().active_stores, 0);
        assert!(Arc::ptr_eq(
            &original_provider,
            &registry.get_provider("memory").unwrap()
        ));
    }
}
