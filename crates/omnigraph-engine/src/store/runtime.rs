use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use ahash::AHashMap;
use arrow_array::{RecordBatch, UInt64Array};
use tokio::sync::Mutex;

use crate::catalog::Catalog;
use crate::error::{NanoError, Result};

use super::csr::CsrIndex;
use super::lance_io::{read_lance_batches, read_lance_projected_batches};
use super::metadata::{DatabaseMetadata, DatasetLocator};

#[derive(Debug, Clone)]
pub(crate) struct NodeLookup {
    pub(crate) batch: RecordBatch,
    pub(crate) id_to_row: AHashMap<u64, usize>,
}

#[derive(Debug, Default)]
pub(crate) struct NodeBatchCache {
    inner: Mutex<AHashMap<(String, u64), Option<RecordBatch>>>,
}

impl NodeBatchCache {
    pub(crate) async fn get_or_load(
        &self,
        type_name: &str,
        locator: &DatasetLocator,
    ) -> Result<Option<RecordBatch>> {
        let key = (type_name.to_string(), locator.dataset_version);
        let mut guard = self.inner.lock().await;
        if let Some(batch) = guard.get(&key) {
            return Ok(batch.clone());
        }

        let batches = read_lance_batches(&locator.dataset_path, locator.dataset_version).await?;
        let batch = if batches.is_empty() {
            None
        } else if batches.len() == 1 {
            Some(batches[0].clone())
        } else {
            let schema = batches[0].schema();
            Some(
                arrow_select::concat::concat_batches(&schema, &batches)
                    .map_err(|e| NanoError::Storage(format!("concat error: {}", e)))?,
            )
        };
        guard.insert(key, batch.clone());
        Ok(batch)
    }
}

#[derive(Debug, Default)]
pub(crate) struct NodeLookupCache {
    inner: Mutex<AHashMap<(String, u64), Option<Arc<NodeLookup>>>>,
}

impl NodeLookupCache {
    pub(crate) async fn get_or_build(
        &self,
        type_name: &str,
        locator: &DatasetLocator,
        batch_cache: &NodeBatchCache,
    ) -> Result<Option<Arc<NodeLookup>>> {
        let key = (type_name.to_string(), locator.dataset_version);
        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.get(&key) {
            return Ok(entry.clone());
        }

        let Some(batch) = batch_cache.get_or_load(type_name, locator).await? else {
            guard.insert(key, None);
            return Ok(None);
        };
        let id_array = batch
            .column_by_name("id")
            .ok_or_else(|| {
                NanoError::Storage(format!("node dataset {} missing id column", type_name))
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "node dataset {} id column is not UInt64",
                    type_name
                ))
            })?;
        let mut id_to_row = AHashMap::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            id_to_row.insert(id_array.value(row), row);
        }
        let lookup = Arc::new(NodeLookup { batch, id_to_row });
        guard.insert(key, Some(lookup.clone()));
        Ok(Some(lookup))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EdgeIndexPair {
    pub(crate) csr: Arc<CsrIndex>,
    pub(crate) csc: Arc<CsrIndex>,
}

#[derive(Debug, Default)]
pub(crate) struct EdgeIndexCache {
    inner: Mutex<AHashMap<(String, u64), Arc<EdgeIndexPair>>>,
}

impl EdgeIndexCache {
    pub(crate) async fn get_or_build(
        &self,
        edge_type: &str,
        dataset_path: &Path,
        dataset_version: u64,
        max_node_id: u64,
    ) -> Result<Arc<EdgeIndexPair>> {
        let key = (edge_type.to_string(), dataset_version);
        let mut guard = self.inner.lock().await;
        if let Some(pair) = guard.get(&key) {
            return Ok(pair.clone());
        }

        let batches =
            read_lance_projected_batches(dataset_path, dataset_version, &["id", "src", "dst"])
                .await?;
        let mut out_edges = Vec::new();
        let mut in_edges = Vec::new();
        for batch in batches {
            let id_arr = batch
                .column_by_name("id")
                .ok_or_else(|| NanoError::Storage("edge batch missing id column".to_string()))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge id column is not UInt64".to_string()))?;
            let src_arr = batch
                .column_by_name("src")
                .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
            let dst_arr = batch
                .column_by_name("dst")
                .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;

            for row in 0..batch.num_rows() {
                let edge_id = id_arr.value(row);
                let src = src_arr.value(row);
                let dst = dst_arr.value(row);
                out_edges.push((src, dst, edge_id));
                in_edges.push((dst, src, edge_id));
            }
        }

        let pair = Arc::new(EdgeIndexPair {
            csr: Arc::new(CsrIndex::build(max_node_id as usize, &mut out_edges)),
            csc: Arc::new(CsrIndex::build(max_node_id as usize, &mut in_edges)),
        });
        guard.insert(key, pair.clone());
        Ok(pair)
    }
}

#[derive(Debug)]
pub(crate) struct DatabaseRuntime {
    catalog: Catalog,
    node_locators: HashMap<String, DatasetLocator>,
    edge_locators: HashMap<String, DatasetLocator>,
    next_node_id: u64,
    next_edge_id: u64,
    edge_index_cache: Arc<EdgeIndexCache>,
    node_batch_cache: Arc<NodeBatchCache>,
    node_lookup_cache: Arc<NodeLookupCache>,
}

impl DatabaseRuntime {
    pub(crate) fn empty(catalog: Catalog) -> Self {
        Self {
            catalog,
            node_locators: HashMap::new(),
            edge_locators: HashMap::new(),
            next_node_id: 0,
            next_edge_id: 0,
            edge_index_cache: Arc::new(EdgeIndexCache::default()),
            node_batch_cache: Arc::new(NodeBatchCache::default()),
            node_lookup_cache: Arc::new(NodeLookupCache::default()),
        }
    }

    pub(crate) fn from_metadata(metadata: &DatabaseMetadata) -> Self {
        let mut runtime = Self::empty(metadata.catalog().clone());
        runtime.next_node_id = metadata.manifest().next_node_id;
        runtime.next_edge_id = metadata.manifest().next_edge_id;
        for entry in &metadata.manifest().datasets {
            let locator = DatasetLocator {
                dataset_path: metadata.path().join(&entry.dataset_path),
                dataset_version: entry.dataset_version,
                row_count: entry.row_count,
            };
            match entry.kind.as_str() {
                "node" => {
                    runtime
                        .node_locators
                        .insert(entry.type_name.clone(), locator);
                }
                "edge" => {
                    runtime
                        .edge_locators
                        .insert(entry.type_name.clone(), locator);
                }
                _ => {}
            }
        }
        runtime
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) fn next_node_id(&self) -> u64 {
        self.next_node_id
    }

    pub(crate) fn node_dataset_locator(&self, type_name: &str) -> Option<&DatasetLocator> {
        self.node_locators.get(type_name)
    }

    pub(crate) fn edge_dataset_locator(&self, type_name: &str) -> Option<&DatasetLocator> {
        self.edge_locators.get(type_name)
    }

    pub(crate) fn node_dataset_path(&self, type_name: &str) -> Option<&Path> {
        self.node_dataset_locator(type_name)
            .map(|locator| locator.dataset_path.as_path())
    }

    pub(crate) fn node_dataset_version(&self, type_name: &str) -> Option<u64> {
        self.node_dataset_locator(type_name)
            .map(|locator| locator.dataset_version)
    }

    pub(crate) fn edge_dataset_path(&self, type_name: &str) -> Option<&Path> {
        self.edge_dataset_locator(type_name)
            .map(|locator| locator.dataset_path.as_path())
    }

    pub(crate) fn edge_dataset_version(&self, type_name: &str) -> Option<u64> {
        self.edge_dataset_locator(type_name)
            .map(|locator| locator.dataset_version)
    }

    pub(crate) fn node_dataset_count(&self) -> usize {
        self.node_locators.len()
    }

    pub(crate) fn edge_dataset_count(&self) -> usize {
        self.edge_locators.len()
    }

    pub(crate) async fn load_node_lookup(
        &self,
        type_name: &str,
    ) -> Result<Option<Arc<NodeLookup>>> {
        let Some(locator) = self.node_dataset_locator(type_name) else {
            return Ok(None);
        };
        self.node_lookup_cache
            .get_or_build(type_name, locator, &self.node_batch_cache)
            .await
    }

    pub(crate) fn edge_index_cache(&self) -> Arc<EdgeIndexCache> {
        self.edge_index_cache.clone()
    }
}
