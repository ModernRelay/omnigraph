use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use lance::Dataset;
use lance::io::WrappingObjectStore;
use lance::session::Session;
use lance_core::deepsize::DeepSizeOf;
use lance_io::object_store::ChainedWrappingObjectStore;
use tokio::sync::Mutex;

use crate::error::Result;

#[derive(Debug)]
pub struct LruMap<K, V>
where
    K: Clone + Eq + Hash,
{
    entries: HashMap<K, V>,
    lru: VecDeque<K>,
    cap: usize,
}

#[allow(clippy::len_without_is_empty)]
impl<K, V> LruMap<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn new(cap: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            cap,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.entries.contains_key(key) {
            self.touch(key.clone());
            self.entries.get(key)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key.clone(), value);
        self.touch(key);
        while self.entries.len() > self.cap {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
        self.lru.clear();
    }

    /// Remove the least recently used entry.
    pub fn pop_oldest(&mut self) -> Option<(K, V)> {
        while let Some(oldest) = self.lru.pop_front() {
            if let Some(value) = self.entries.remove(&oldest) {
                return Some((oldest, value));
            }
        }
        None
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn keys_by_recency(&self) -> Vec<K> {
        self.lru.iter().cloned().collect()
    }

    pub fn touch(&mut self, key: K) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }

    /// Move a held entry to the eviction front; a later `get` promotes it
    /// back. A key that is not held changes nothing.
    pub fn demote(&mut self, key: &K) {
        if !self.entries.contains_key(key) {
            return;
        }
        self.lru.retain(|existing| existing != key);
        self.lru.push_front(key.clone());
    }
}

/// Most held `Dataset` handles.
const TABLE_HANDLE_CACHE_CAP: usize = 64;

/// Most decoded manifest bytes the held handles keep alive, weighed as Lance
/// weighs a manifest (`DeepSizeOf`): a manifest lists every fragment, so a
/// count cap alone held 64 fragment lists of one hot table (issue 859).
const TABLE_HANDLE_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableHandleKey {
    pub table_path: String,
    pub table_branch: Option<String>,
    pub version: u64,
    /// The detached pin `version` resolves to; `version` alone is a
    /// per-lineage counter two branches share.
    pub staged_version: Option<u64>,
    pub e_tag: Option<String>,
}

impl TableHandleKey {
    pub fn new(
        table_path: &str,
        table_branch: Option<&str>,
        version: u64,
        staged_version: Option<u64>,
        e_tag: Option<&str>,
    ) -> Self {
        Self {
            table_path: table_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
            staged_version,
            e_tag: e_tag.map(str::to_string),
        }
    }
}

/// Held open-`Dataset` handles keyed by `(table_path, branch, version, pin,
/// e_tag)` — the version-keyed analogue of LanceDB's `DatasetConsistencyWrapper`
/// (`rust/lancedb/src/table/dataset.rs`). A warm read reuses a held handle with
/// zero open IO (a cheap `Dataset` clone); a miss opens once at the location with
/// the shared `Session`. Version, pin and e_tag are in the key, so a write (or a
/// delete/recreate that reuses a version number on object stores with e_tags) is
/// simply a new key. A same-branch manifest refresh clears this cache as the
/// fallback for e_tag-less table locations. Live branch reads and the pinned
/// base a writer stages on both open through it.
///
/// Retention is bounded twice: by entry count and by the decoded manifest
/// bytes the entries keep alive (`TABLE_HANDLE_CACHE_BYTES`). A writer that
/// publishes a new pin demotes the pin it staged on (`demote`): it stays held
/// while the budget has room, for a reader or a fork still at that version,
/// and is the first out under pressure, so a steadily written table's history
/// never displaces a live pin.
#[derive(Default)]
pub struct TableHandleCache {
    inner: Mutex<TableHandleCacheInner>,
}

struct TableHandleCacheInner {
    entries: LruMap<TableHandleKey, HeldHandle>,
    byte_budget: usize,
}

struct HeldHandle {
    dataset: Dataset,
    /// `DeepSizeOf` of the dataset's manifest, measured once on admission.
    manifest_bytes: usize,
}

/// What the held handles occupy: the count and the decoded manifest bytes the
/// byte budget weighs.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HandleCacheOccupancy {
    pub handles: usize,
    pub fragments: usize,
    pub manifest_bytes: usize,
}

impl TableHandleCache {
    /// Drop all held handles. Correctness never requires this (version-in-key);
    /// it is memory hygiene, called from the same hooks that clear the graph
    /// index cache (branch switch / refresh).
    pub async fn invalidate_all(&self) {
        let mut inner = self.inner.lock().await;
        inner.entries.invalidate_all();
    }

    /// Return the dataset for `(dataset_path, branch, version, e_tag)`, reusing a
    /// held handle (0 open IO) or opening it once at `location` with the shared
    /// `session` on a miss. A `statement_wrapper` stacks over the per-query
    /// instrumentation wrapper, so instrumentation sees only its misses.
    pub async fn get_or_open(
        &self,
        dataset_path: &str,
        table_branch: Option<&str>,
        version: u64,
        e_tag: Option<&str>,
        staged_version: Option<u64>,
        transaction_uuid: Option<&str>,
        last_linear_version: Option<u64>,
        location: &str,
        session: Option<&Arc<Session>>,
        statement_wrapper: Option<Arc<crate::read_memo::RangeMemoWrapper>>,
    ) -> Result<Dataset> {
        let key = TableHandleKey::new(dataset_path, table_branch, version, staged_version, e_tag);
        {
            let mut inner = self.inner.lock().await;
            if let Some(held) = inner.entries.get(&key) {
                return Ok(held.dataset.clone());
            }
        }
        let memo = statement_wrapper.clone();
        let wrapper = match (statement_wrapper, crate::instrumentation::table_wrapper()) {
            (Some(statement), Some(probe)) => {
                // Native chains apply inner wrappers first; memo hits stay outside the probe.
                Some(
                    Arc::new(ChainedWrappingObjectStore::new(vec![probe, statement]))
                        as Arc<dyn WrappingObjectStore>,
                )
            }
            (Some(statement), None) => Some(statement as Arc<dyn WrappingObjectStore>),
            (None, probe) => probe,
        };
        // Miss: open without holding the lock (the open is async IO). A concurrent
        // double-miss opens twice and one wins the insert — correct (the dataset
        // at a version is immutable) and rare.
        let ds = crate::instrumentation::open_pinned_dataset(
            location,
            version,
            staged_version,
            transaction_uuid,
            last_linear_version,
            session,
            wrapper,
        )
        .await?;
        if let (Some(memo), Some(session)) = (memo, session) {
            memo.register_dataset(&ds, session).await;
        }
        // O(fragments), like the decode this miss already paid.
        let manifest_bytes = ds.manifest().deep_size_of();
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.entries.get(&key) {
            return Ok(existing.dataset.clone());
        }
        inner.insert(
            key,
            HeldHandle {
                dataset: ds.clone(),
                manifest_bytes,
            },
        );
        Ok(ds)
    }

    /// A held handle for this pin, without opening on a miss (RFC 0067: a
    /// writer that just published a pin holds the handle it needs).
    pub async fn get(
        &self,
        dataset_path: &str,
        table_branch: Option<&str>,
        version: u64,
        staged_version: Option<u64>,
        e_tag: Option<&str>,
    ) -> Option<Dataset> {
        let key = TableHandleKey::new(dataset_path, table_branch, version, staged_version, e_tag);
        let mut inner = self.inner.lock().await;
        inner.entries.get(&key).map(|held| held.dataset.clone())
    }

    /// Move exactly this pin, if held, to the eviction front: a writer calls
    /// it for the pin it staged on once its publication names a newer one.
    /// A reader or fork that opens the pin again promotes it back.
    pub async fn demote(&self, key: &TableHandleKey) {
        let mut inner = self.inner.lock().await;
        inner.entries.demote(key);
    }

    /// The held handles' count, fragment entries and weighed manifest bytes.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn occupancy(&self) -> HandleCacheOccupancy {
        let inner = self.inner.lock().await;
        inner
            .entries
            .values()
            .fold(HandleCacheOccupancy::default(), |mut total, held| {
                total.handles += 1;
                total.fragments += held.dataset.manifest().fragments.len();
                total.manifest_bytes += held.manifest_bytes;
                total
            })
    }

    #[cfg(test)]
    fn with_limits(cap: usize, byte_budget: usize) -> Self {
        Self {
            inner: Mutex::new(TableHandleCacheInner {
                entries: LruMap::new(cap),
                byte_budget,
            }),
        }
    }

    /// The keys held, least recently used first.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn held_keys(&self) -> Vec<TableHandleKey> {
        let inner = self.inner.lock().await;
        inner.entries.keys_by_recency()
    }

    /// Hold `dataset` for the pin a writer just committed, under the key the
    /// published registration will carry, so the next open of that pin on
    /// this handle costs no request (RFC 0067: the writer holds the handle it
    /// needs; the pin is immutable, so a lost publish leaves a harmless entry).
    pub async fn insert(
        &self,
        dataset_path: &str,
        table_branch: Option<&str>,
        version: u64,
        staged_version: Option<u64>,
        e_tag: Option<&str>,
        dataset: Dataset,
    ) {
        let key = TableHandleKey::new(dataset_path, table_branch, version, staged_version, e_tag);
        let manifest_bytes = dataset.manifest().deep_size_of();
        let mut inner = self.inner.lock().await;
        inner.insert(
            key,
            HeldHandle {
                dataset,
                manifest_bytes,
            },
        );
    }
}

impl TableHandleCacheInner {
    /// Admit `key`, then evict from the front (demoted pins, then the least
    /// recently used) until the held manifests fit the byte budget. The
    /// newest entry always stays.
    fn insert(&mut self, key: TableHandleKey, value: HeldHandle) {
        self.entries.insert(key, value);
        while self.entries.len() > 1
            && self
                .entries
                .values()
                .map(|held| held.manifest_bytes)
                .sum::<usize>()
                > self.byte_budget
        {
            if self.entries.pop_oldest().is_none() {
                break;
            }
        }
    }
}

impl Default for TableHandleCacheInner {
    fn default() -> Self {
        Self {
            entries: LruMap::new(TABLE_HANDLE_CACHE_CAP),
            byte_budget: TABLE_HANDLE_CACHE_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};

    use super::*;

    /// One Lance table with `versions` versions; version `v` lists `v`
    /// fragments, the shape a steadily appended graph table has.
    async fn appended_table(uri: &str, versions: u64) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        for v in 1..=versions {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![v as i32]))],
            )
            .unwrap();
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let mode = if v == 1 {
                WriteMode::Create
            } else {
                WriteMode::Append
            };
            let params = WriteParams {
                mode,
                ..Default::default()
            };
            // forbidden-api-allow: test-only raw Lance fixture with many versions for the handle cache
            Dataset::write(reader, uri, Some(params)).await.unwrap();
        }
    }

    async fn open(cache: &TableHandleCache, uri: &str, version: u64) -> Dataset {
        cache
            .get_or_open("t", None, version, None, None, None, None, uri, None, None)
            .await
            .unwrap()
    }

    /// Issue 859: every commit of a hot table is a new key, and each held
    /// version decodes its own fragment list, so a count cap alone held 64
    /// copies of it. The byte budget keeps the held manifests within bounds
    /// whatever the commit count, and the newest version stays held.
    #[tokio::test]
    async fn superseded_versions_fit_the_manifest_byte_budget_issue_859() {
        const VERSIONS: u64 = 80;
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t.lance");
        let uri = uri.to_str().unwrap();
        appended_table(uri, VERSIONS).await;

        let newest_bytes = open(&TableHandleCache::default(), uri, VERSIONS)
            .await
            .manifest()
            .deep_size_of();
        let budget = 4 * newest_bytes;
        let cache = TableHandleCache::with_limits(TABLE_HANDLE_CACHE_CAP, budget);
        for version in 1..=VERSIONS {
            open(&cache, uri, version).await;
            let occupancy = cache.occupancy().await;
            assert!(
                occupancy.manifest_bytes <= budget,
                "after version {version}: {occupancy:?} exceeds the {budget}-byte budget"
            );
        }
        let held = cache.held_keys().await;
        assert_eq!(
            held.last().map(|key| key.version),
            Some(VERSIONS),
            "the newest version stays held"
        );
        assert!(
            held.len() < TABLE_HANDLE_CACHE_CAP,
            "the byte budget, not the count cap, bounded {} held versions",
            held.len()
        );
    }

    /// A manifest larger than the whole budget is still admitted alone, so
    /// the newest open is always warm.
    #[tokio::test]
    async fn newest_handle_is_admitted_over_the_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t.lance");
        let uri = uri.to_str().unwrap();
        appended_table(uri, 3).await;

        let cache = TableHandleCache::with_limits(TABLE_HANDLE_CACHE_CAP, 1);
        open(&cache, uri, 2).await;
        open(&cache, uri, 3).await;
        let held = cache.held_keys().await;
        assert_eq!(held.iter().map(|key| key.version).collect::<Vec<_>>(), [3]);
    }

    /// `demote` moves exactly the named pin to the eviction front, keeps it
    /// held, and ignores a pin that is not held.
    #[tokio::test]
    async fn demote_moves_only_the_named_pin_to_the_eviction_front() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t.lance");
        let uri = uri.to_str().unwrap();
        appended_table(uri, 3).await;

        let cache = TableHandleCache::default();
        for version in 1..=3 {
            open(&cache, uri, version).await;
        }
        cache
            .demote(&TableHandleKey::new("t", None, 3, None, None))
            .await;
        cache
            .demote(&TableHandleKey::new("t", Some("other"), 2, None, None))
            .await;
        let held = cache.held_keys().await;
        assert_eq!(
            held.iter().map(|key| key.version).collect::<Vec<_>>(),
            [3, 1, 2]
        );
        assert_eq!(cache.occupancy().await.handles, 3);
    }

    /// Under budget pressure a demoted pin goes before every live one, even
    /// the most recently opened; a hit on a demoted pin promotes it back, so
    /// the oldest live pin goes instead.
    #[tokio::test]
    async fn budget_pressure_drops_demoted_pins_before_live_ones() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t.lance");
        let uri = uri.to_str().unwrap();
        appended_table(uri, 4).await;

        let mut all_four = 0;
        for version in 1..=4 {
            all_four += open(&TableHandleCache::default(), uri, version)
                .await
                .manifest()
                .deep_size_of();
        }
        let budget = all_four - 1;

        let cache = TableHandleCache::with_limits(TABLE_HANDLE_CACHE_CAP, budget);
        for version in 1..=3 {
            open(&cache, uri, version).await;
        }
        cache
            .demote(&TableHandleKey::new("t", None, 3, None, None))
            .await;
        open(&cache, uri, 4).await;
        let held = cache.held_keys().await;
        assert_eq!(
            held.iter().map(|key| key.version).collect::<Vec<_>>(),
            [1, 2, 4],
            "the demoted newest pin went first"
        );

        let cache = TableHandleCache::with_limits(TABLE_HANDLE_CACHE_CAP, budget);
        for version in 1..=3 {
            open(&cache, uri, version).await;
        }
        cache
            .demote(&TableHandleKey::new("t", None, 2, None, None))
            .await;
        assert!(
            cache.get("t", None, 2, None, None).await.is_some(),
            "a hit on a demoted pin"
        );
        open(&cache, uri, 4).await;
        let held = cache.held_keys().await;
        assert_eq!(
            held.iter().map(|key| key.version).collect::<Vec<_>>(),
            [3, 2, 4],
            "the promoted pin stayed; the oldest live pin went"
        );
    }
}
