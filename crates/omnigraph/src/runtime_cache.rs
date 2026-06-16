use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use lance::Dataset;
use lance::session::Session;
use omnigraph_compiler::catalog::Catalog;
use tokio::sync::Mutex;

use crate::db::ResolvedTarget;
use crate::error::Result;
use crate::graph_index::GraphIndex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GraphIndexCacheKey {
    snapshot_id: String,
    edge_tables: Vec<GraphIndexTableState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GraphIndexTableState {
    table_key: String,
    table_version: u64,
    table_branch: Option<String>,
}

#[derive(Debug, Default)]
pub struct RuntimeCache {
    graph_indices: Mutex<GraphIndexCache>,
}

#[derive(Debug, Default)]
struct GraphIndexCache {
    entries: HashMap<GraphIndexCacheKey, Arc<GraphIndex>>,
    lru: VecDeque<GraphIndexCacheKey>,
}

impl RuntimeCache {
    pub async fn invalidate_all(&self) {
        let mut cache = self.graph_indices.lock().await;
        cache.entries.clear();
        cache.lru.clear();
    }

    pub async fn graph_index(
        &self,
        resolved: &ResolvedTarget,
        catalog: &Catalog,
    ) -> Result<Arc<GraphIndex>> {
        let key = graph_index_cache_key(resolved, catalog);
        {
            let mut cache = self.graph_indices.lock().await;
            if let Some(index) = cache.entries.get(&key).cloned() {
                cache.touch(key.clone());
                return Ok(index);
            }
        }

        let edge_types = catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
            .collect();

        let index = Arc::new(GraphIndex::build(&resolved.snapshot, &edge_types).await?);
        let mut cache = self.graph_indices.lock().await;
        if let Some(existing) = cache.entries.get(&key).cloned() {
            cache.touch(key);
            return Ok(existing);
        }
        cache.insert(key, Arc::clone(&index));
        Ok(index)
    }
}

impl GraphIndexCache {
    fn insert(&mut self, key: GraphIndexCacheKey, value: Arc<GraphIndex>) {
        self.entries.insert(key.clone(), value);
        self.touch(key);
        while self.entries.len() > 8 {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest).is_some() {
                break;
            }
        }
    }

    fn touch(&mut self, key: GraphIndexCacheKey) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }
}

fn graph_index_cache_key(resolved: &ResolvedTarget, catalog: &Catalog) -> GraphIndexCacheKey {
    let mut edge_tables: Vec<GraphIndexTableState> = catalog
        .edge_types
        .keys()
        .filter_map(|edge_name| {
            let table_key = format!("edge:{}", edge_name);
            resolved
                .snapshot
                .entry(&table_key)
                .map(|entry| GraphIndexTableState {
                    table_key,
                    table_version: entry.table_version,
                    table_branch: entry.table_branch.clone(),
                })
        })
        .collect();
    edge_tables.sort_by(|a, b| a.table_key.cmp(&b.table_key));

    GraphIndexCacheKey {
        snapshot_id: resolved.snapshot_id.as_str().to_string(),
        edge_tables,
    }
}

/// Max held `Dataset` handles. A handle holds only Arcs (object store + manifest),
/// never table data, so this is cheap; it bounds how many `(table, branch,
/// version)` cells stay warm. One graph's live table set across a couple of
/// branches at the current version fits comfortably, with headroom for the
/// recently-superseded versions left by writes until they age out.
const TABLE_HANDLE_CACHE_CAP: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableHandleKey {
    table_path: String,
    table_branch: Option<String>,
    version: u64,
}

/// Held open-`Dataset` handles keyed by `(table_path, branch, version)` — the
/// version-keyed analogue of LanceDB's `DatasetConsistencyWrapper`
/// (`rust/lancedb/src/table/dataset.rs`). A warm read reuses a held handle with
/// zero open IO (a cheap `Dataset` clone); a miss opens once at the location with
/// the shared `Session`. Version is in the key, so a write (which advances the
/// table version) is simply a new key: the old handle ages out via LRU, no
/// explicit invalidation needed for correctness. Only read-path Data opens use
/// this — writes open HEAD directly and never receive a pinned handle.
#[derive(Default)]
pub struct TableHandleCache {
    inner: Mutex<TableHandleCacheInner>,
}

#[derive(Default)]
struct TableHandleCacheInner {
    entries: HashMap<TableHandleKey, Dataset>,
    lru: VecDeque<TableHandleKey>,
}

impl TableHandleCache {
    /// Drop all held handles. Correctness never requires this (version-in-key);
    /// it is memory hygiene, called from the same hooks that clear the graph
    /// index cache (branch switch / refresh).
    pub async fn invalidate_all(&self) {
        let mut inner = self.inner.lock().await;
        inner.entries.clear();
        inner.lru.clear();
    }

    /// Return the dataset for `(table_path, branch, version)`, reusing a held
    /// handle (0 open IO) or opening it once at `location` with the shared
    /// `session` on a miss.
    pub async fn get_or_open(
        &self,
        table_path: &str,
        table_branch: Option<&str>,
        version: u64,
        location: &str,
        session: Option<&Arc<Session>>,
    ) -> Result<Dataset> {
        let key = TableHandleKey {
            table_path: table_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
        };
        {
            let mut inner = self.inner.lock().await;
            if let Some(ds) = inner.entries.get(&key).cloned() {
                inner.touch(key);
                return Ok(ds);
            }
        }
        // Miss: open without holding the lock (the open is async IO). A concurrent
        // double-miss opens twice and one wins the insert — correct (the dataset
        // at a version is immutable) and rare.
        let ds = crate::instrumentation::open_table_dataset(location, version, session).await?;
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.entries.get(&key).cloned() {
            inner.touch(key);
            return Ok(existing);
        }
        inner.insert(key, ds.clone());
        Ok(ds)
    }
}

impl TableHandleCacheInner {
    fn insert(&mut self, key: TableHandleKey, value: Dataset) {
        self.entries.insert(key.clone(), value);
        self.touch(key);
        while self.entries.len() > TABLE_HANDLE_CACHE_CAP {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest).is_some() {
                break;
            }
        }
    }

    fn touch(&mut self, key: TableHandleKey) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }
}

/// Per-graph read caches handed to a resolved `Snapshot` so its table opens reuse
/// one shared `Session` (LanceDB's one-session-per-connection pattern) and the
/// held-handle cache. Manual `Debug` because `lance::session::Session` is not
/// `Debug`; this lets `Snapshot` keep its `#[derive(Debug)]`.
pub struct ReadCaches {
    pub session: Arc<Session>,
    pub handles: Arc<TableHandleCache>,
}

impl std::fmt::Debug for ReadCaches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCaches").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn key(id: usize) -> GraphIndexCacheKey {
        GraphIndexCacheKey {
            snapshot_id: format!("snap-{id}"),
            edge_tables: Vec::new(),
        }
    }

    fn empty_index() -> Arc<GraphIndex> {
        Arc::new(GraphIndex::empty_for_test())
    }

    #[test]
    fn graph_index_cache_evicts_oldest_entry() {
        let mut cache = GraphIndexCache::default();
        for idx in 0..9 {
            cache.insert(key(idx), empty_index());
        }

        assert_eq!(cache.entries.len(), 8);
        assert!(!cache.entries.contains_key(&key(0)));
        assert!(cache.entries.contains_key(&key(8)));
    }

    #[test]
    fn graph_index_cache_touch_keeps_recent_entry() {
        let mut cache = GraphIndexCache::default();
        for idx in 0..8 {
            cache.insert(key(idx), empty_index());
        }

        cache.touch(key(0));
        cache.insert(key(8), empty_index());

        assert!(cache.entries.contains_key(&key(0)));
        assert!(!cache.entries.contains_key(&key(1)));
    }
}
