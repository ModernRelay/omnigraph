use std::collections::HashMap;
use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;

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
    graph_indices: HashMap<GraphIndexCacheKey, Arc<GraphIndex>>,
}

impl RuntimeCache {
    pub fn invalidate_all(&mut self) {
        self.graph_indices.clear();
    }

    pub async fn graph_index(
        &mut self,
        resolved: &ResolvedTarget,
        catalog: &Catalog,
    ) -> Result<Arc<GraphIndex>> {
        let key = graph_index_cache_key(resolved, catalog);
        if let Some(index) = self.graph_indices.get(&key) {
            return Ok(Arc::clone(index));
        }

        let edge_types = catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
            .collect();

        let index = Arc::new(GraphIndex::build(&resolved.snapshot, &edge_types).await?);
        self.graph_indices.insert(key, Arc::clone(&index));
        Ok(index)
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
