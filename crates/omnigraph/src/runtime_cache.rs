use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

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
