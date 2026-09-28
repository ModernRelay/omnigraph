use std::collections::HashMap;

use arrow_array::StringArray;
use futures::TryStreamExt;
use omnigraph_catalog::Snapshot;
use omnigraph_compiler::SystemColumns;
use omnigraph_core::error::{OmniError, Result};

/// Dense u32 mapping for a single node type: String ID ↔ dense index.
#[derive(Debug, Clone)]
pub struct TypeIndex {
    id_to_dense: HashMap<String, u32>,
    dense_to_id: Vec<String>,
}

impl TypeIndex {
    pub(crate) fn new() -> Self {
        Self {
            id_to_dense: HashMap::new(),
            dense_to_id: Vec::new(),
        }
    }

    /// Get or insert a string ID, returning its dense index.
    pub(crate) fn get_or_insert(&mut self, id: &str) -> u32 {
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

    #[allow(
        clippy::len_without_is_empty,
        reason = "the dense id space size, read as a CSR row width"
    )]
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
    pub(crate) fn build(num_nodes: usize, edges: &[(u32, u32)]) -> Self {
        let mut counts = vec![0u32; num_nodes];
        for &(src, _) in edges {
            counts[src as usize] += 1;
        }

        let mut offsets = Vec::with_capacity(num_nodes + 1);
        offsets.push(0);
        for &c in &counts {
            offsets.push(offsets.last().unwrap() + c);
        }

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

    /// The dense id space this adjacency was built over. `has_neighbors`
    /// indexes `offsets[node + 1]` unchecked, so callers walking a
    /// `TypeIndex`'s dense space must verify it matches this width first.
    pub fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
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
        edge_types: &HashMap<String, (String, String)>,
        system_columns: SystemColumns,
    ) -> Result<Self> {
        crate::instrumentation::record_graph_build(edge_types.len());
        let mut type_indices: HashMap<String, TypeIndex> = HashMap::new();
        let mut csr = HashMap::new();
        let mut csc = HashMap::new();

        let mut edge_pairs: HashMap<String, Vec<(u32, u32)>> = HashMap::new();

        let mut __dst_e1: Vec<_> = edge_types.iter().collect();
        __dst_e1.sort_by(|a, b| a.0.cmp(b.0));
        for (edge_name, (from_type, to_type)) in __dst_e1 {
            let table_key = format!("edge:{}", edge_name);
            if snapshot.dataset(&table_key).is_none() {
                continue;
            }

            let ds = snapshot.open_lance_dataset(&table_key).await?;

            let batches: Vec<arrow_array::RecordBatch> = ds
                .scan()
                .project(&[system_columns.src, system_columns.dst])
                .map_err(OmniError::storage)?
                .try_into_stream()
                .await
                .map_err(OmniError::storage)?
                .try_collect()
                .await
                .map_err(OmniError::storage)?;

            type_indices
                .entry(from_type.clone())
                .or_insert_with(TypeIndex::new);
            type_indices
                .entry(to_type.clone())
                .or_insert_with(TypeIndex::new);

            let mut edges: Vec<(u32, u32)> = Vec::new();
            for batch in &batches {
                let srcs = string_column(batch, system_columns.src)?;
                let dsts = string_column(batch, system_columns.dst)?;

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

        let mut __dst_e2: Vec<_> = edge_types.iter().collect();
        __dst_e2.sort_by(|a, b| a.0.cmp(b.0));
        for (edge_name, (from_type, to_type)) in __dst_e2 {
            let Some(edges) = edge_pairs.get(edge_name) else {
                continue;
            };

            let src_count = type_indices[from_type].len();
            let dst_count = type_indices[to_type].len();

            csr.insert(edge_name.clone(), CsrIndex::build(src_count, edges));

            let reversed: Vec<(u32, u32)> = edges.iter().map(|&(s, d)| (d, s)).collect();
            csc.insert(edge_name.clone(), CsrIndex::build(dst_count, &reversed));
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

fn string_column<'a>(batch: &'a arrow_array::RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("graph index batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("graph index column '{name}' is not Utf8"))
        })
}
