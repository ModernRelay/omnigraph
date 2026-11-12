pub(crate) mod persist;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::LargeStringBuilder;
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray, UInt32Array};
use arrow_schema::DataType;
use futures::TryStreamExt;
use hashbrown::HashTable;
use omnigraph_compiler::SystemColumns;

use crate::db::Snapshot;
use crate::error::{OmniError, Result};

/// The id hash (over an id's UTF-8 bytes), keyed at random per dictionary:
/// node ids come from writers, and a public fixed key would let ids that
/// share a bucket make building and lookups quadratic. The table is never
/// persisted, and each dictionary keeps the state it was built with.
fn id_hash_state() -> ahash::RandomState {
    ahash::RandomState::new()
}

/// The dense u32 mapping of one node type in a built or loaded graph index:
/// the ids in dense order as one Arrow `LargeUtf8` array (index `i` holds the id
/// of dense `i`, in first-seen order), and a table of dense ids that hashes
/// and compares through that array. No id is held twice and none is a
/// separate allocation; destination columns are gathered from `ids` with
/// Arrow's `take`. Immutable: built by [`TypeIndexBuilder`].
#[derive(Debug, Clone)]
pub struct TypeIndex {
    ids: LargeStringArray,
    dense_of: HashTable<u32>,
    state: ahash::RandomState,
}

impl TypeIndex {
    pub fn to_dense(&self, id: &str) -> Option<u32> {
        let hash = self.state.hash_one(id.as_bytes());
        self.dense_of
            .find(hash, |&dense| self.ids.value(dense as usize) == id)
            .copied()
    }

    pub fn to_id(&self, dense: u32) -> Option<&str> {
        let dense = dense as usize;
        (dense < self.ids.len()).then(|| self.ids.value(dense))
    }

    // The size of the dense id space, consumed as a CSR row width; emptiness is
    // not a meaningful question for it.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// The dense-ordered id dictionary (index `i` holds the id of dense `i`).
    pub(crate) fn ids(&self) -> &LargeStringArray {
        &self.ids
    }

    /// The index of `ids` in dense order; a duplicate is refused.
    #[cfg(test)]
    pub(crate) fn from_ids(ids: Vec<String>) -> Result<Self> {
        let mut builder = TypeIndexBuilder::new();
        for id in &ids {
            builder.push_unique(id)?;
        }
        Ok(builder.finish())
    }
}

/// Builds a [`TypeIndex`]: appends each new id to one growing `LargeUtf8`
/// buffer and its dense id to the table, in first-seen order. The buffer's
/// i64 offsets hold any dictionary a graph can have; only a dense id past
/// `u32` is refused.
#[derive(Debug)]
pub(crate) struct TypeIndexBuilder {
    ids: LargeStringBuilder,
    len: usize,
    dense_of: HashTable<u32>,
    state: ahash::RandomState,
}

impl TypeIndexBuilder {
    pub(crate) fn new() -> Self {
        Self::with_capacity(0, 0)
    }

    /// Room for `ids` ids of `bytes` bytes in total.
    pub(crate) fn with_capacity(ids: usize, bytes: usize) -> Self {
        Self {
            ids: LargeStringBuilder::with_capacity(ids, bytes),
            len: 0,
            dense_of: HashTable::with_capacity(ids),
            state: id_hash_state(),
        }
    }

    fn find(&self, hash: u64, id: &str) -> Option<u32> {
        let offsets = self.ids.offsets_slice();
        let values = self.ids.values_slice();
        self.dense_of
            .find(hash, |&dense| {
                let dense = dense as usize;
                &values[offsets[dense] as usize..offsets[dense + 1] as usize] == id.as_bytes()
            })
            .copied()
    }

    fn push(&mut self, hash: u64, id: &str) -> Result<u32> {
        // Dense ids are u32, as the CSR rows they index are.
        let dense = u32::try_from(self.len).map_err(|_| {
            OmniError::manifest_internal(format!(
                "graph index dictionary outgrew u32 dense ids ({} ids)",
                self.len
            ))
        })?;
        self.ids.append_value(id);
        self.len += 1;
        let (state, offsets, values) = (
            &self.state,
            self.ids.offsets_slice(),
            self.ids.values_slice(),
        );
        self.dense_of.insert_unique(hash, dense, |&entry| {
            let entry = entry as usize;
            state.hash_one(&values[offsets[entry] as usize..offsets[entry + 1] as usize])
        });
        Ok(dense)
    }

    /// The dense id of `id`, appended when it is new.
    pub(crate) fn get_or_insert(&mut self, id: &str) -> Result<u32> {
        let hash = self.state.hash_one(id.as_bytes());
        match self.find(hash, id) {
            Some(dense) => Ok(dense),
            None => self.push(hash, id),
        }
    }

    /// Append `id` as the next dense id (persisted-artifact load). A
    /// duplicate would silently alias two dense slots, so it is refused.
    pub(crate) fn push_unique(&mut self, id: &str) -> Result<u32> {
        let hash = self.state.hash_one(id.as_bytes());
        if self.find(hash, id).is_some() {
            return Err(OmniError::manifest(format!(
                "graph index dictionary holds duplicate id '{id}'"
            )));
        }
        self.push(hash, id)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The UTF-8 bytes of every id appended so far.
    pub(crate) fn value_bytes(&self) -> usize {
        self.ids.values_slice().len()
    }

    pub(crate) fn finish(mut self) -> TypeIndex {
        TypeIndex {
            ids: self.ids.finish(),
            dense_of: self.dense_of,
            state: self.state,
        }
    }
}

/// A traversal's own growing String ID ↔ dense index mapping, for a walk
/// that starts on the indexed path and interns ids as its scans find them.
#[derive(Debug, Clone, Default)]
pub struct IdInterner {
    id_to_dense: HashMap<String, u32, ahash::RandomState>,
    dense_to_id: Vec<String>,
}

impl IdInterner {
    pub(crate) fn new() -> Self {
        Self::default()
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

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.dense_to_id.len()
    }

    /// The dense-ordered id dictionary (index `i` holds the id of dense `i`).
    pub(crate) fn ids(&self) -> &[String] {
        &self.dense_to_id
    }
}

/// The dictionary a walk's dense destination ids resolve through: a graph
/// index's [`TypeIndex`] or the walk's own [`IdInterner`].
#[derive(Clone, Copy)]
pub(crate) enum DenseIds<'a> {
    Index(&'a LargeStringArray),
    Interned(&'a [String]),
}

impl DenseIds<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Index(ids) => ids.len(),
            Self::Interned(ids) => ids.len(),
        }
    }

    /// The UTF-8 length of the id of `dense`, `None` outside the dictionary.
    pub(crate) fn byte_len(&self, dense: u32) -> Option<usize> {
        let dense = dense as usize;
        match self {
            Self::Index(ids) => (dense < ids.len()).then(|| {
                let offsets = ids.value_offsets();
                (offsets[dense + 1] - offsets[dense]) as usize
            }),
            Self::Interned(ids) => ids.get(dense).map(String::len),
        }
    }

    /// The `Utf8` column of the ids of `dense`, every one inside the
    /// dictionary (the caller checked); a graph index's ids are gathered
    /// with Arrow's `take`, then narrowed to `Utf8`: a chunk is bounded by
    /// the batch bytes, so its offsets fit i32, and the cast rewrites only
    /// the offsets and keeps the gathered value buffer.
    pub(crate) fn column(&self, dense: &UInt32Array) -> Result<ArrayRef> {
        let failed = |error: arrow_schema::ArrowError| {
            OmniError::manifest_internal(format!("gathering destination ids failed: {error}"))
        };
        match self {
            Self::Index(ids) => {
                let gathered = arrow_select::take::take(*ids, dense, None).map_err(failed)?;
                arrow_cast::cast(&gathered, &DataType::Utf8).map_err(failed)
            }
            Self::Interned(ids) => Ok(Arc::new(StringArray::from_iter_values(
                dense.values().iter().map(|&d| ids[d as usize].as_str()),
            ))),
        }
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
        system_columns: SystemColumns,
    ) -> Result<Self> {
        Self::build_counted(snapshot, edge_types, system_columns, None)
            .await?
            .ok_or_else(|| OmniError::manifest_internal("an uncounted graph index build stopped"))
    }

    /// Build as [`Self::build`] does, reading each edge table one scan batch
    /// at a time. With `count`, each batch's growth of the persisted artifact
    /// is added to it, and once the count passes its limit the build drops
    /// what it holds and returns `None`, before it keeps another batch and
    /// before the adjacency arrays are allocated.
    pub(crate) async fn build_counted(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
        system_columns: SystemColumns,
        mut count: Option<&mut persist::ArtifactCount>,
    ) -> Result<Option<Self>> {
        // Counted here — not at the cache-miss site — so the probe counts
        // actual edge-table scan builds, never persisted-artifact loads.
        crate::instrumentation::record_graph_build(edge_types.len());
        // INVARIANT (A1 graph-index cache key): the topology is a pure function of
        // the edge tables' `src`/`dst` columns and nothing else. `RuntimeCache`
        // keys `GraphIndexCacheKey` on each edge table's physical identity
        // `(table_key, version, table_branch, e_tag)` so a lazy-fork branch reuses
        // main's built index. If you read node tables, schema, or other state here,
        // add it to that key or the cache will serve a stale index.
        let mut builders: HashMap<String, TypeIndexBuilder> = HashMap::new();
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

            let mut batches = ds
                .scan()
                .project(&[system_columns.src, system_columns.dst])
                .map_err(OmniError::storage)?
                .try_into_stream()
                .await
                .map_err(OmniError::storage)?;

            builders
                .entry(from_type.clone())
                .or_insert_with(TypeIndexBuilder::new);
            builders
                .entry(to_type.clone())
                .or_insert_with(TypeIndexBuilder::new);

            let mut edges: Vec<(u32, u32)> = Vec::new();
            while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
                let appended = |builders: &HashMap<String, TypeIndexBuilder>, name: &str| {
                    let builder = &builders[name];
                    (builder.len(), builder.value_bytes())
                };
                let from_before = appended(&builders, from_type);
                let to_before = appended(&builders, to_type);
                let srcs = string_column(&batch, system_columns.src)?;
                let dsts = string_column(&batch, system_columns.dst)?;

                for i in 0..batch.num_rows() {
                    let src_dense = builders
                        .get_mut(from_type)
                        .unwrap()
                        .get_or_insert(srcs.value(i))?;
                    let dst_dense = builders
                        .get_mut(to_type)
                        .unwrap()
                        .get_or_insert(dsts.value(i))?;
                    edges.push((src_dense, dst_dense));
                }
                if let Some(count) = count.as_deref_mut() {
                    let (ids, bytes) = appended(&builders, from_type);
                    count.add_ids(from_type, ids - from_before.0, bytes - from_before.1);
                    // A type on both ends of the edge has one dictionary.
                    if to_type != from_type {
                        let (ids, bytes) = appended(&builders, to_type);
                        count.add_ids(to_type, ids - to_before.0, bytes - to_before.1);
                    }
                    count.add_edges(batch.num_rows() as u64);
                    if count.passed() {
                        return Ok(None);
                    }
                }
            }
            edge_pairs.insert(edge_name.clone(), edges);
        }

        // Phase 2: Build CSR/CSC using final TypeIndex sizes
        let type_indices: HashMap<String, TypeIndex> = builders
            .into_iter()
            .map(|(name, builder)| (name, builder.finish()))
            .collect();
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

        Ok(Some(Self {
            type_indices,
            csr,
            csc,
        }))
    }

    /// Load the persisted adjacency artifact when one matches this snapshot's
    /// physical identity; fall back to the in-memory scan build. The load path
    /// is fail-open — a rejected artifact logs and builds, never errors.
    /// `adapter` is the owning db's storage adapter, so instrumented, injected
    /// and bound adapters observe the artifact GET.
    pub async fn load_or_build(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
        adapter: &dyn crate::storage::StorageAdapter,
        system_columns: SystemColumns,
    ) -> Result<Self> {
        if let Some((index, _)) = persist::load(snapshot, edge_types, adapter).await {
            return Ok(index);
        }
        Self::build(snapshot, edge_types, system_columns).await
    }

    /// Load the persisted artifact fresh for `edge_types`, returning the full
    /// decoded index together with its identity stamps — the shelf's key in
    /// `RuntimeCache` (one decode Arc-shared across every scope it can
    /// serve). `None` on any miss, exactly like the loader inside
    /// [`Self::load_or_build`].
    pub(crate) async fn load_persisted(
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
        adapter: &dyn crate::storage::StorageAdapter,
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
        let mut builder = TypeIndexBuilder::new();
        let a = builder.get_or_insert("Alice").unwrap();
        let b = builder.get_or_insert("Bob").unwrap();
        let c = builder.get_or_insert("Charlie").unwrap();
        assert_eq!((a, b, c), (0, 1, 2), "dense ids follow first-seen order");
        let idx = builder.finish();

        assert_eq!(idx.to_dense("Alice"), Some(a));
        assert_eq!(idx.to_dense("Bob"), Some(b));
        assert_eq!(idx.to_dense("Charlie"), Some(c));

        assert_eq!(idx.to_id(a), Some("Alice"));
        assert_eq!(idx.to_id(b), Some("Bob"));
        assert_eq!(idx.to_id(c), Some("Charlie"));
        assert_eq!(idx.len(), 3);
        assert_eq!(
            idx.ids(),
            &LargeStringArray::from(vec!["Alice", "Bob", "Charlie"])
        );
    }

    #[test]
    fn type_index_idempotent_insert() {
        let mut builder = TypeIndexBuilder::new();
        let a1 = builder.get_or_insert("Alice").unwrap();
        let a2 = builder.get_or_insert("Alice").unwrap();
        assert_eq!(a1, a2);
        assert_eq!(builder.finish().len(), 1);
    }

    #[test]
    fn type_index_unknown_returns_none() {
        let idx = TypeIndexBuilder::new().finish();
        assert_eq!(idx.to_dense("unknown"), None);
        assert_eq!(idx.to_id(999), None);
    }

    #[test]
    fn type_index_finds_every_id_across_table_growth() {
        let mut builder = TypeIndexBuilder::new();
        for i in 0..10_000u32 {
            assert_eq!(builder.get_or_insert(&format!("node-{i}")).unwrap(), i);
        }
        for i in (0..10_000u32).rev() {
            assert_eq!(builder.get_or_insert(&format!("node-{i}")).unwrap(), i);
        }
        let idx = builder.finish();
        for i in 0..10_000u32 {
            assert_eq!(idx.to_dense(&format!("node-{i}")), Some(i));
            assert_eq!(idx.to_id(i), Some(format!("node-{i}").as_str()));
        }
        assert_eq!(idx.to_dense("node-10000"), None);
        assert_eq!(idx.to_dense(""), None);
    }

    #[test]
    fn type_index_push_unique_refuses_a_duplicate() {
        let mut builder = TypeIndexBuilder::new();
        builder.push_unique("Alice").unwrap();
        builder.push_unique("").unwrap();
        let err = builder.push_unique("Alice").unwrap_err();
        assert!(err.to_string().contains("duplicate id 'Alice'"), "{err}");
    }

    #[test]
    fn dense_ids_columns_match_for_both_dictionaries() {
        let mut builder = TypeIndexBuilder::new();
        let mut interner = IdInterner::new();
        for id in ["a", "", "ccc", "dd"] {
            builder.get_or_insert(id).unwrap();
            interner.get_or_insert(id);
        }
        let index = builder.finish();
        let dense = UInt32Array::from(vec![3u32, 0, 0, 1, 2]);
        let expected: ArrayRef = Arc::new(StringArray::from(vec!["dd", "a", "a", "", "ccc"]));
        for ids in [
            DenseIds::Index(index.ids()),
            DenseIds::Interned(interner.ids()),
        ] {
            assert_eq!(ids.len(), 4);
            assert_eq!(ids.byte_len(2), Some(3));
            assert_eq!(ids.byte_len(1), Some(0));
            assert_eq!(ids.byte_len(4), None);
            assert_eq!(&ids.column(&dense).unwrap(), &expected);
        }
    }

    #[test]
    fn id_interner_round_trip() {
        let mut idx = IdInterner::new();
        let a = idx.get_or_insert("Alice");
        let b = idx.get_or_insert("Bob");
        assert_eq!(idx.get_or_insert("Alice"), a);
        assert_eq!(idx.to_dense("Bob"), Some(b));
        assert_eq!(idx.to_id(a), Some("Alice"));
        assert_eq!(idx.to_dense("unknown"), None);
        assert_eq!(idx.len(), 2);
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
                "__src",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![1_u64]))],
        )
        .unwrap();

        let err = string_column(&batch, "__src").unwrap_err();
        assert!(err.to_string().contains("__src"));
    }
}
