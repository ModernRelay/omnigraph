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
use object_store::list::PaginatedListStore;
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
const IMMUTABLE_COMPONENTS: [&str; 3] = ["data", "_indices", "_deletions"];

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

struct LeasedBytes {
    bytes: Bytes,
    _lease: Arc<dyn fmt::Debug + Send + Sync>,
}

impl AsRef<[u8]> for LeasedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
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
            extensions: Default::default(),
        }
    }
}

struct MemoRoot {
    store_prefix: String,
    path: Path,
    dataset_root: bool,
}

struct MemoState {
    roots: Vec<MemoRoot>,
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
                roots: Vec::new(),
                entries: HashMap::new(),
                remaining_bytes: budget_bytes,
            }),
            _lease: lease,
        })
    }

    fn register_root(&self, store_prefix: &str, path: &Path, dataset_root: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.roots.iter().any(|root| {
            root.store_prefix == store_prefix
                && root.path == *path
                && root.dataset_root == dataset_root
        }) {
            return;
        }
        let cost = std::mem::size_of::<MemoRoot>()
            .saturating_add(1)
            .saturating_mul(4)
            .saturating_add(store_prefix.len().saturating_mul(2))
            .saturating_add(path.as_ref().len().saturating_mul(2))
            .saturating_add(64) as u64;
        if cost > state.remaining_bytes {
            return;
        }
        state.remaining_bytes -= cost;
        state.roots.push(MemoRoot {
            store_prefix: store_prefix.to_owned(),
            path: path.clone(),
            dataset_root,
        });
    }

    fn immutable_path(&self, store_prefix: &str, path: &Path) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.roots.iter().any(|root| {
            root.store_prefix == store_prefix
                && path.prefix_match(&root.path).is_some_and(|mut relative| {
                    let Some(first) = relative.next() else {
                        return false;
                    };
                    !root.dataset_root
                        || (IMMUTABLE_COMPONENTS.contains(&first.as_ref())
                            && relative.next().is_some())
                })
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
    pub async fn register_dataset(
        &self,
        dataset: &lance::Dataset,
        session: &Arc<lance::session::Session>,
    ) {
        let Some(memo) = self.memo.upgrade() else {
            return;
        };
        let registry = session.store_registry();
        let prefix = |uri: &str| {
            registry
                .calculate_object_store_prefix(uri, dataset.initial_storage_options())
                .ok()
        };
        if let Some(store_prefix) = prefix(dataset.uri()) {
            memo.register_root(&store_prefix, dataset.refs.base(), true);
        }
        for base in dataset.manifest().base_paths.values() {
            let Ok(path) = base.extract_path(Arc::clone(&registry)) else {
                continue;
            };
            if let Some(store_prefix) = prefix(&base.path) {
                memo.register_root(&store_prefix, &path, base.is_dataset_root);
            }
        }
    }

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

    fn wrap_paginated(
        &self,
        _store_prefix: &str,
        original: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        // The memo intercepts GETs only; it never changes listing visibility.
        Some(original)
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

    fn wrap_paginated(
        &self,
        store_prefix: &str,
        original: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        self.outer.wrap_paginated(
            store_prefix,
            self.inner.wrap_paginated(store_prefix, original)?,
        )
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
            || !options.extensions.is_empty()
        {
            return None;
        }
        let path = location.as_ref();
        if !self.memo.immutable_path(&self.store_prefix, location) {
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
        // Opaque response context has neither a bounded retained size nor a
        // reusable meaning. Preserve it on the original response, uncached.
        if !result.extensions.is_empty() {
            return Ok(result);
        }
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
                    bytes: Bytes::from_owner(LeasedBytes {
                        bytes,
                        _lease: Arc::clone(&self.memo._lease),
                    }),
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
        response_extensions: bool,
    }

    impl CountingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                gets: AtomicU64::new(0),
                response_extensions: false,
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
            let request = self.gets.fetch_add(1, Ordering::Relaxed) + 1;
            let mut result = self.inner.get_opts(location, options).await?;
            if self.response_extensions {
                result.extensions.insert(request);
            }
            Ok(result)
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
        memo.register_root("test", &Path::from("tables/x"), true);
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
    async fn opaque_request_and_response_context_bypass_the_memo() {
        for response_extensions in [false, true] {
            let counting = Arc::new(CountingStore {
                inner: InMemory::new(),
                gets: AtomicU64::new(0),
                response_extensions,
            });
            seed(&counting, DATA_PATH).await;
            let memo = RangeReadMemo::new(1024 * 1024);
            memo.register_root("test", &Path::from("tables/x"), true);
            let available = memo.state.lock().unwrap().remaining_bytes;
            let store = RangeMemoWrapper::new(&memo)
                .wrap("test", Arc::clone(&counting) as Arc<dyn ObjectStore>);
            for request in 1_u64..=2 {
                let mut options = bounded(16, 48);
                if !response_extensions {
                    options.extensions.insert(request);
                }
                let result = store
                    .get_opts(&Path::from(DATA_PATH), options)
                    .await
                    .unwrap();
                assert_eq!(
                    result.extensions.get::<u64>().copied(),
                    response_extensions.then_some(request)
                );
                assert_eq!(result.bytes().await.unwrap().as_ref(), &[7; 32]);
            }
            assert_eq!(counting.gets.load(Ordering::Relaxed), 2);
            let state = memo.state.lock().unwrap();
            assert!(state.entries.is_empty());
            assert_eq!(state.remaining_bytes, available);
        }
    }

    #[derive(Debug)]
    struct PaginationWrapper {
        name: &'static str,
        allows_pushdown: bool,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl WrappingObjectStore for PaginationWrapper {
        fn wrap(&self, _: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            original
        }

        fn wrap_paginated(
            &self,
            store_prefix: &str,
            original: Arc<dyn PaginatedListStore>,
        ) -> Option<Arc<dyn PaginatedListStore>> {
            assert_eq!(store_prefix, "test");
            self.calls.lock().unwrap().push(self.name);
            self.allows_pushdown.then_some(original)
        }
    }

    #[async_trait]
    impl PaginatedListStore for PaginationWrapper {
        async fn list_paginated(
            &self,
            _: Option<&str>,
            _: object_store::list::PaginatedListOptions,
        ) -> object_store::Result<object_store::list::PaginatedListResult> {
            panic!("wrapping must not issue a listing")
        }
    }

    #[test]
    fn pagination_preserves_the_lister_and_each_wrappers_refusal() {
        for inner_allows in [false, true] {
            for outer_allows in [false, true] {
                let calls = Arc::new(Mutex::new(Vec::new()));
                let wrapper = |name, allows_pushdown| {
                    Arc::new(PaginationWrapper {
                        name,
                        allows_pushdown,
                        calls: Arc::clone(&calls),
                    })
                };
                let original: Arc<dyn PaginatedListStore> = wrapper("original", true);
                let memo = RangeReadMemo::new(0);
                let passed = RangeMemoWrapper::new(&memo)
                    .wrap_paginated("test", Arc::clone(&original))
                    .unwrap();
                assert!(Arc::ptr_eq(&original, &passed));
                let stacked = StackedWrapper::new(
                    wrapper("outer", outer_allows),
                    wrapper("inner", inner_allows),
                );
                let result = stacked.wrap_paginated("test", Arc::clone(&original));
                assert_eq!(result.is_some(), inner_allows && outer_allows);
                if let Some(result) = result {
                    assert!(Arc::ptr_eq(&original, &result));
                }
                assert_eq!(
                    *calls.lock().unwrap(),
                    if inner_allows {
                        vec!["inner", "outer"]
                    } else {
                        vec!["inner"]
                    }
                );
            }
        }
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
    async fn mutable_paths_below_data_named_ancestors_are_not_memoized() {
        let counting = CountingStore::new();
        let memo = RangeReadMemo::new(1024 * 1024);
        let store = RangeMemoWrapper::new(&memo)
            .wrap("test", Arc::clone(&counting) as Arc<dyn ObjectStore>);
        for (root, control) in [
            ("data/graph/tables/x", "_versions/1.manifest"),
            ("srv/data/graph/tables/x", "_transactions/txn.txn"),
            ("srv/_indices/graph/tables/x", "_latest.manifest"),
            ("srv/_deletions/graph/tables/x", "_refs/main"),
        ] {
            memo.register_root("test", &Path::from(root), true);
            let path = format!("{root}/{control}");
            seed(&counting, &path).await;
            let path = Path::from(path);
            let first = store
                .get_opts(&path, bounded(0, 8))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            counting
                .inner
                .put(&path, PutPayload::from_static(&[9_u8; 256]))
                .await
                .unwrap();
            let second = store
                .get_opts(&path, bounded(0, 8))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_ne!(first, second, "mutable object {path} must be reread");
            let data = format!("{root}/data/77777777-7777-7777-7777-777777777777.lance");
            seed(&counting, &data).await;
            let data = Path::from(data);
            for _ in 0..2 {
                store
                    .get_opts(&data, bounded(0, 8))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
            }
        }
        assert_eq!(counting.gets.load(Ordering::Relaxed), 12);
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
    /// Pinned native branches can read both their own fragments and fragments
    /// inherited from main and an earlier native branch. Resolve real Lance
    /// paths, then count reads independently of Lance's own metadata caches.
    #[tokio::test]
    async fn native_and_inherited_branch_data_are_memoized() {
        use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use lance::dataset::{WriteMode, WriteParams};
        use lance::session::Session;
        use lance_file::version::LanceFileVersion;

        fn row(value: i32) -> impl arrow_array::RecordBatchReader + Send + 'static {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![format!("branch-{value}")])),
                    Arc::new(Int32Array::from(vec![value])),
                ],
            )
            .unwrap();
            RecordBatchIterator::new(vec![Ok(batch)], schema)
        }

        fn params(mode: WriteMode) -> WriteParams {
            WriteParams {
                mode,
                enable_stable_row_ids: true,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("data/graph/table.lance");
        let mut main = crate::lance_clone::fresh_dataset(uri.to_str().unwrap()).await;
        let main_data = main.data_dir();
        let main_version = main.version().version;
        let mut parent =
            crate::lance_clone::create_branch(&mut main, "parent/data/topic", main_version)
                .await
                .unwrap();
        parent
            .append(row(2), Some(params(WriteMode::Append)))
            .await
            .unwrap();
        let parent_data = parent.data_dir();
        let parent_version = parent.version().version;
        let mut child =
            crate::lance_clone::create_branch(&mut parent, "child/_indices/topic", parent_version)
                .await
                .unwrap();
        child
            .append(row(3), Some(params(WriteMode::Append)))
            .await
            .unwrap();
        let child_data = child.data_dir();

        let access = crate::lance_access::LanceAccessContext::with_io_scope(Some(
            omnigraph_storage::StorageIoScope::new(),
        ));
        let registry = access.data_session().store_registry();
        let session = Arc::new(Session::new(1024 * 1024, 1024 * 1024, registry.clone()));
        let pinned = crate::instrumentation::open_dataset(
            child.uri(),
            crate::instrumentation::VersionResolution::At(child.version().version),
            Some(&session),
            None,
        )
        .await
        .unwrap();
        let memo = RangeReadMemo::new(1024 * 1024);
        let wrapper = RangeMemoWrapper::new(&memo);
        wrapper.register_dataset(&pinned, &session).await;

        let counting = CountingStore::new();
        let mut seen_roots = std::collections::HashSet::new();
        let mut read_count = 0;
        for fragment in pinned.manifest().fragments.iter() {
            for file in &fragment.files {
                let data_root = match file.base_id {
                    Some(base_id) => {
                        let base = &pinned.manifest().base_paths[&base_id];
                        assert!(base.is_dataset_root);
                        base.extract_path(registry.clone()).unwrap().join("data")
                    }
                    None => pinned.data_dir(),
                };
                seen_roots.insert(data_root.clone());
                let path = data_root.join(file.path.as_str());
                let base_uri = file.base_id.map_or_else(
                    || pinned.uri(),
                    |base_id| pinned.manifest().base_paths[&base_id].path.as_str(),
                );
                let prefix = registry
                    .calculate_object_store_prefix(base_uri, pinned.initial_storage_options())
                    .unwrap();
                let store = wrapper.wrap(&prefix, counting.clone() as Arc<dyn ObjectStore>);
                seed(&counting, path.as_ref()).await;
                for _ in 0..2 {
                    assert_eq!(
                        store
                            .get_opts(&path, bounded(0, 8))
                            .await
                            .unwrap()
                            .bytes()
                            .await
                            .unwrap()
                            .as_ref(),
                        &[7_u8; 8],
                    );
                }
                read_count += 1;
                assert_eq!(counting.gets.load(Ordering::Relaxed), read_count);
            }
        }
        assert_eq!(
            seen_roots,
            [main_data, parent_data, child_data].into_iter().collect()
        );
        assert_eq!(
            read_count, 3,
            "fixture must exercise three independently written files"
        );

        let prefix = registry
            .calculate_object_store_prefix(pinned.uri(), pinned.initial_storage_options())
            .unwrap();
        let store = wrapper.wrap(&prefix, counting.clone() as Arc<dyn ObjectStore>);
        let manifest = pinned
            .refs
            .base()
            .clone()
            .join("_versions")
            .join("1.manifest");
        seed(&counting, manifest.as_ref()).await;
        let first = store
            .get_opts(&manifest, bounded(0, 8))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        counting
            .inner
            .put(&manifest, PutPayload::from_static(&[9_u8; 256]))
            .await
            .unwrap();
        let second = store
            .get_opts(&manifest, bounded(0, 8))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(counting.gets.load(Ordering::Relaxed), read_count + 2);
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
        let root = lance::io::ObjectStore::extract_path_from_uri(
            Arc::clone(&registry),
            "memory://memo-lifetime/table",
        )
        .unwrap();
        let prefix = registry
            .calculate_object_store_prefix("memory://memo-lifetime/table", None)
            .unwrap();
        memo.register_root(&prefix, &root, true);
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
        let data = root.join("data").join("probe.lance");
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
        let bytes = store.inner.get(&data).await.unwrap().bytes().await.unwrap();

        drop(params);
        drop(wrapper);
        drop(memo);
        drop(lease);
        assert!(weak_memo.upgrade().is_some());
        assert!(weak_lease.upgrade().is_some());
        assert_eq!(registry.stats().active_stores, 1);

        drop(store);
        assert!(weak_memo.upgrade().is_none());
        assert!(weak_lease.upgrade().is_some());
        assert_eq!(bytes.len(), 256);
        drop(bytes);
        assert!(weak_lease.upgrade().is_none());
        assert!(weak_wrapper.upgrade().is_none());
        assert_eq!(registry.stats().active_stores, 0);
        assert!(Arc::ptr_eq(
            &original_provider,
            &registry.get_provider("memory").unwrap()
        ));
    }
}
