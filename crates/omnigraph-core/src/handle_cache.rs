use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use lance::Dataset;
use lance::session::Session;
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

    #[cfg(any(test, feature = "test-util"))]
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn touch(&mut self, key: K) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }
}

/// Max held `Dataset` handles. A handle holds only Arcs (object store + manifest),
/// never table data, so this is cheap; it bounds how many `(table, branch,
/// version, e_tag)` cells stay warm. One graph's live table set across a couple
/// of branches at the current version fits comfortably, with headroom for the
/// recently-superseded versions left by writes until they age out.
const TABLE_HANDLE_CACHE_CAP: usize = 64;

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

/// Held open-`Dataset` handles keyed by `(table_path, branch, version, pin,
/// e_tag)` — the version-keyed analogue of LanceDB's `DatasetConsistencyWrapper`
/// (`rust/lancedb/src/table/dataset.rs`). A warm read reuses a held handle with
/// zero open IO (a cheap `Dataset` clone); a miss opens once at the location with
/// the shared `Session`. Version, pin and e_tag are in the key, so a write (or a
/// delete/recreate that reuses a version number on object stores with e_tags) is
/// simply a new key. A same-branch manifest refresh clears this cache as the
/// fallback for e_tag-less table locations. Reads open through it, a writer
/// opens the pin it stages on through it, and a writer holds the version it
/// committed once its publication succeeds.
#[derive(Default)]
pub struct TableHandleCache {
    inner: Mutex<TableHandleCacheInner>,
}

struct TableHandleCacheInner {
    entries: LruMap<TableHandleKey, Dataset>,
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
    /// `session` on a miss.
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
    ) -> Result<Dataset> {
        let key = TableHandleKey {
            table_path: dataset_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
            staged_version,
            e_tag: e_tag.map(str::to_string),
        };
        {
            let mut inner = self.inner.lock().await;
            if let Some(ds) = inner.entries.get(&key).cloned() {
                return Ok(ds);
            }
        }
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
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.entries.get(&key).cloned() {
            return Ok(existing);
        }
        inner.insert(key, ds.clone());
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
        let key = TableHandleKey {
            table_path: dataset_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
            staged_version,
            e_tag: e_tag.map(str::to_string),
        };
        let mut inner = self.inner.lock().await;
        inner.entries.get(&key).cloned()
    }

    /// Hold the handle a writer committed, under the pin its publication
    /// registered, so the next write or read of that pin opens nothing. Call
    /// it only after the publication succeeded: the key names an immutable
    /// detached version, so the handle can never go stale.
    pub async fn hold_published(
        &self,
        dataset_path: &str,
        table_branch: Option<&str>,
        version: u64,
        staged_version: Option<u64>,
        e_tag: Option<&str>,
        dataset: Dataset,
    ) {
        let key = TableHandleKey {
            table_path: dataset_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
            staged_version,
            e_tag: e_tag.map(str::to_string),
        };
        let mut inner = self.inner.lock().await;
        inner.insert(key, dataset);
    }
}

impl TableHandleCacheInner {
    fn insert(&mut self, key: TableHandleKey, value: Dataset) {
        self.entries.insert(key, value);
    }
}

impl Default for TableHandleCacheInner {
    fn default() -> Self {
        Self {
            entries: LruMap::new(TABLE_HANDLE_CACHE_CAP),
        }
    }
}
