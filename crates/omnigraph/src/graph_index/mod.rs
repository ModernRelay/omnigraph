use std::collections::HashMap;

use arrow_array::StringArray;
use futures::TryStreamExt;

use crate::db::Snapshot;
use crate::error::{OmniError, Result};

/// Dense u32 mapping for a single node type: String ID ↔ dense index.
#[derive(Debug, Clone)]
pub struct TypeIndex {
    id_to_dense: HashMap<String, u32>,
    dense_to_id: Vec<String>,
}

impl TypeIndex {
    fn new() -> Self {
        Self {
            id_to_dense: HashMap::new(),
            dense_to_id: Vec::new(),
        }
    }

    /// Get or insert a string ID, returning its dense index.
    fn get_or_insert(&mut self, id: &str) -> u32 {
        if let Some(&idx) = self.id_to_dense.get(id) {
            return idx;
        }
        let idx = self.dense_to_id.len() as u32;
        self.dense_to_id.push(id.to_string());
        self.id_to_dense.insert(id.to_string(), idx);
        idx
    }

    pub fn to_dense(&self, id: &str) -> Option<u32> {
        self.id_to_dense.get(id).copied()
    }

    pub fn to_id(&self, dense: u32) -> Option<&str> {
        self.dense_to_id.get(dense as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.dense_to_id.len()
    }
}

/// CSR (Compressed Sparse Row) adjacency index.
#[derive(Debug, Clone)]
pub struct CsrIndex {
    /// offsets[i] .. offsets[i+1] gives the neighbor range for node i.
    offsets: Vec<u32>,
    /// Dense indices of destination nodes.
    targets: Vec<u32>,
}

impl CsrIndex {
    fn build(num_nodes: usize, edges: &[(u32, u32)]) -> Self {
        // Count outgoing edges per source
        let mut counts = vec![0u32; num_nodes];
        for &(src, _) in edges {
            counts[src as usize] += 1;
        }

        // Build offset array (prefix sum)
        let mut offsets = Vec::with_capacity(num_nodes + 1);
        offsets.push(0);
        for &c in &counts {
            offsets.push(offsets.last().unwrap() + c);
        }

        // Fill targets
        let mut targets = vec![0u32; edges.len()];
        let mut cursors = vec![0u32; num_nodes];
        for &(src, dst) in edges {
            let s = src as usize;
            let pos = offsets[s] + cursors[s];
            targets[pos as usize] = dst;
            cursors[s] += 1;
        }

        Self { offsets, targets }
    }

    /// Return the dense indices of neighbors for a given dense node index.
    pub fn neighbors(&self, node: u32) -> &[u32] {
        let start = self.offsets[node as usize] as usize;
        let end = self.offsets[node as usize + 1] as usize;
        &self.targets[start..end]
    }

    /// Check if a node has any outgoing edges. O(1), no allocation.
    pub fn has_neighbors(&self, node: u32) -> bool {
        let n = node as usize;
        self.offsets[n + 1] > self.offsets[n]
    }
}

/// Topology-only graph index. No node data cached — just adjacency.
#[derive(Debug, Clone)]
pub struct GraphIndex {
    /// Dense index per node type (built from edge src/dst columns).
    type_indices: HashMap<String, TypeIndex>,
    /// Outgoing adjacency per edge type.
    csr: HashMap<String, CsrIndex>,
    /// Incoming adjacency per edge type.
    csc: HashMap<String, CsrIndex>,
}

impl GraphIndex {
    /// Build a graph index by scanning edge sub-tables from a snapshot.
    pub async fn build(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>, // edge_name → (from_type, to_type)
    ) -> Result<Self> {
        let mut type_indices: HashMap<String, TypeIndex> = HashMap::new();
        let mut csr = HashMap::new();
        let mut csc = HashMap::new();

        // Phase 1: Scan all edges, build TypeIndices and collect edge pairs
        let mut edge_pairs: HashMap<String, Vec<(u32, u32)>> = HashMap::new();

        for (edge_name, (from_type, to_type)) in edge_types {
            let table_key = format!("edge:{}", edge_name);
            if snapshot.entry(&table_key).is_none() {
                continue;
            }

            let ds = snapshot.open(&table_key).await?;

            let batches: Vec<arrow_array::RecordBatch> = ds
                .scan()
                .project(&["src", "dst"])
                .map_err(|e| OmniError::Lance(e.to_string()))?
                .try_into_stream()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
                .try_collect()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            type_indices
                .entry(from_type.clone())
                .or_insert_with(TypeIndex::new);
            type_indices
                .entry(to_type.clone())
                .or_insert_with(TypeIndex::new);

            let mut edges: Vec<(u32, u32)> = Vec::new();
            for batch in &batches {
                let srcs = batch
                    .column_by_name("src")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let dsts = batch
                    .column_by_name("dst")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let src_dense = type_indices
                        .get_mut(from_type)
                        .unwrap()
                        .get_or_insert(srcs.value(i));
                    let dst_dense = type_indices
                        .get_mut(to_type)
                        .unwrap()
                        .get_or_insert(dsts.value(i));
                    edges.push((src_dense, dst_dense));
                }
            }
            edge_pairs.insert(edge_name.clone(), edges);
        }

        // Phase 2: Build CSR/CSC using final TypeIndex sizes
        for (edge_name, (from_type, to_type)) in edge_types {
            let Some(edges) = edge_pairs.get(edge_name) else {
                continue;
            };

            let src_count = type_indices[from_type].len();
            let dst_count = type_indices[to_type].len();

            csr.insert(
                edge_name.clone(),
                CsrIndex::build(src_count, edges),
            );

            let reversed: Vec<(u32, u32)> = edges.iter().map(|&(s, d)| (d, s)).collect();
            csc.insert(
                edge_name.clone(),
                CsrIndex::build(dst_count, &reversed),
            );
        }

        Ok(Self {
            type_indices,
            csr,
            csc,
        })
    }

    pub fn type_index(&self, type_name: &str) -> Option<&TypeIndex> {
        self.type_indices.get(type_name)
    }

    pub fn csr(&self, edge_type: &str) -> Option<&CsrIndex> {
        self.csr.get(edge_type)
    }

    pub fn csc(&self, edge_type: &str) -> Option<&CsrIndex> {
        self.csc.get(edge_type)
    }
}
