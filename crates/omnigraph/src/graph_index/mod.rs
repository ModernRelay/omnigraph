pub(crate) mod persist;

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

    // The size of the dense id space, consumed as a CSR row width; emptiness is
    // not a meaningful question for it.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.dense_to_id.len()
    }

    /// The dense-ordered id dictionary (index `i` holds the id of dense `i`).
    pub(crate) fn ids(&self) -> &[String] {
        &self.dense_to_id
    }

    /// Rebuild from a dense-ordered dictionary (persisted-artifact load).
    /// Duplicate ids would silently alias two dense slots, so they are refused.
    pub(crate) fn from_ids(ids: Vec<String>) -> Result<Self> {
        let mut id_to_dense = HashMap::with_capacity(ids.len());
        for (dense, id) in ids.iter().enumerate() {
            if id_to_dense.insert(id.clone(), dense as u32).is_some() {
                return Err(OmniError::manifest(format!(
                    "graph index dictionary holds duplicate id '{id}'"
                )));
            }
        }
        Ok(Self {
            id_to_dense,
            dense_to_id: ids,
        })
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

    pub(crate) fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    pub(crate) fn targets(&self) -> &[u32] {
        &self.targets
    }

    /// Rebuild from persisted arrays, enforcing the structural invariants
    /// `neighbors` relies on: a non-empty, zero-based, monotone offsets array
    /// whose final entry covers exactly the targets array.
    pub(crate) fn from_parts(offsets: Vec<u32>, targets: Vec<u32>) -> Result<Self> {
        let valid = offsets.first() == Some(&0)
            && offsets.windows(2).all(|w| w[0] <= w[1])
            && offsets.last().copied() == Some(targets.len() as u32)
            && targets.len() <= u32::MAX as usize;
        if !valid {
            return Err(OmniError::manifest(
                "graph index adjacency arrays are structurally invalid".to_string(),
            ));
        }
        Ok(Self { offsets, targets })
    }

    /// The dense id space this adjacency was built over. `has_neighbors`
    /// indexes `offsets[node + 1]` unchecked, so callers walking a
    /// `TypeIndex`'s dense space must verify it matches this width first.
    /// The subtraction cannot underflow: `build` always pushes the leading
    /// 0 offset, so `offsets` holds at least one entry.
    pub fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }
}

/// Borrowed views of a `GraphIndex`'s three maps (dictionaries, outgoing,
/// incoming), for artifact serialization.
pub(crate) type GraphIndexParts<'a> = (
    &'a HashMap<String, TypeIndex>,
    &'a HashMap<String, CsrIndex>,
    &'a HashMap<String, CsrIndex>,
);

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
        // Counted here — not at the cache-miss site — so the probe counts
        // actual edge-table scan builds, never persisted-artifact loads.
        crate::instrumentation::record_graph_build(edge_types.len());
        // INVARIANT (A1 graph-index cache key): the topology is a pure function of
        // the edge tables' `src`/`dst` columns and nothing else. `RuntimeCache`
        // keys `GraphIndexCacheKey` on each edge table's physical identity
        // `(table_key, version, table_branch, e_tag)` so a lazy-fork branch reuses
        // main's built index. If you read node tables, schema, or other state here,
        // add it to that key or the cache will serve a stale index.
        let mut type_indices: HashMap<String, TypeIndex> = HashMap::new();
        let mut csr = HashMap::new();
        let mut csc = HashMap::new();

        // Phase 1: Scan all edges, build TypeIndices and collect edge pairs
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
                .project(&["src", "dst"])
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
                let srcs = string_column(batch, "src")?;
                let dsts = string_column(batch, "dst")?;

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

    /// Load the persisted adjacency artifact when one matches this snapshot's
    /// physical identity; fall back to the in-memory scan build. The load path
    /// is fail-open — a rejected artifact logs and builds, never errors.
    /// `adapter` is the owning db's storage adapter when one exists (so
    /// instrumented/injected adapters observe the artifact GET); `None` lets
    /// the loader derive one from the store root.
    pub async fn load_or_build(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
        adapter: Option<&dyn crate::storage::StorageAdapter>,
    ) -> Result<Self> {
        if let Some((index, _)) = persist::load(snapshot, edge_types, adapter).await {
            return Ok(index);
        }
        Self::build(snapshot, edge_types).await
    }

    /// Load the persisted artifact fresh for `edge_types`, returning the full
    /// decoded index together with its identity stamps — the shelf's key in
    /// `RuntimeCache` (one decode Arc-shared across every scope it can
    /// serve). `None` on any miss, exactly like the loader inside
    /// [`Self::load_or_build`].
    pub(crate) async fn load_persisted(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
        adapter: Option<&dyn crate::storage::StorageAdapter>,
    ) -> Option<(Self, Vec<persist::TableStamp>)> {
        persist::load(snapshot, edge_types, adapter).await
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

    /// Internal views for artifact serialization.
    pub(crate) fn parts(&self) -> GraphIndexParts<'_> {
        (&self.type_indices, &self.csr, &self.csc)
    }

    /// Rebuild from artifact parts. Structural validation of each adjacency
    /// happened in `CsrIndex::from_parts`; here the cross-map invariant is
    /// enforced: every edge type carries BOTH orientations, since traversal
    /// dispatch assumes csr and csc exist together.
    pub(crate) fn from_parts(
        type_indices: HashMap<String, TypeIndex>,
        csr: HashMap<String, CsrIndex>,
        csc: HashMap<String, CsrIndex>,
    ) -> Result<Self> {
        for edge in csr.keys() {
            if !csc.contains_key(edge) {
                return Err(OmniError::manifest(format!(
                    "graph index artifact misses the csc orientation for edge '{edge}'"
                )));
            }
        }
        for edge in csc.keys() {
            if !csr.contains_key(edge) {
                return Err(OmniError::manifest(format!(
                    "graph index artifact misses the csr orientation for edge '{edge}'"
                )));
            }
        }
        Ok(Self {
            type_indices,
            csr,
            csc,
        })
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            type_indices: HashMap::new(),
            csr: HashMap::new(),
            csc: HashMap::new(),
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn type_index_round_trip() {
        let mut idx = TypeIndex::new();
        let a = idx.get_or_insert("Alice");
        let b = idx.get_or_insert("Bob");
        let c = idx.get_or_insert("Charlie");

        assert_eq!(idx.to_dense("Alice"), Some(a));
        assert_eq!(idx.to_dense("Bob"), Some(b));
        assert_eq!(idx.to_dense("Charlie"), Some(c));

        assert_eq!(idx.to_id(a), Some("Alice"));
        assert_eq!(idx.to_id(b), Some("Bob"));
        assert_eq!(idx.to_id(c), Some("Charlie"));
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn type_index_idempotent_insert() {
        let mut idx = TypeIndex::new();
        let a1 = idx.get_or_insert("Alice");
        let a2 = idx.get_or_insert("Alice");
        assert_eq!(a1, a2);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn type_index_unknown_returns_none() {
        let idx = TypeIndex::new();
        assert_eq!(idx.to_dense("unknown"), None);
        assert_eq!(idx.to_id(999), None);
    }

    #[test]
    fn csr_neighbors_correct() {
        // Graph: 0→1, 0→2, 1→2
        let edges = vec![(0, 1), (0, 2), (1, 2)];
        let csr = CsrIndex::build(3, &edges);

        let mut n0: Vec<u32> = csr.neighbors(0).to_vec();
        n0.sort();
        assert_eq!(n0, vec![1, 2]);

        assert_eq!(csr.neighbors(1), &[2]);
        assert_eq!(csr.neighbors(2), &[] as &[u32]);
    }

    #[test]
    fn csr_empty_graph() {
        let csr = CsrIndex::build(3, &[]);
        assert_eq!(csr.neighbors(0), &[] as &[u32]);
        assert_eq!(csr.neighbors(1), &[] as &[u32]);
        assert_eq!(csr.neighbors(2), &[] as &[u32]);
        assert!(!csr.has_neighbors(0));
    }

    #[test]
    fn csr_has_neighbors() {
        // 0→1, 1→2
        let csr = CsrIndex::build(3, &[(0, 1), (1, 2)]);
        assert!(csr.has_neighbors(0));
        assert!(csr.has_neighbors(1));
        assert!(!csr.has_neighbors(2));
    }

    #[test]
    fn string_column_returns_error_for_bad_schema() {
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "src",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![1_u64]))],
        )
        .unwrap();

        let err = string_column(&batch, "src").unwrap_err();
        assert!(err.to_string().contains("src"));
    }
}
