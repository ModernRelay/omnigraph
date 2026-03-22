/// Compressed Sparse Row index for adjacency lookups.
#[derive(Debug, Clone)]
pub struct CsrIndex {
    /// offsets[i] = start position in neighbors for node i
    /// offsets[num_nodes] = total number of edges
    pub offsets: Vec<u64>,
    /// neighbor node IDs, sorted by source
    pub neighbors: Vec<u64>,
}

impl CsrIndex {
    /// Build a CSR index from a set of edges.
    /// `edges` is a slice of (src_id, dst_id, edge_id) tuples.
    /// `num_nodes` is the total number of nodes (determines offsets array size).
    pub fn build(num_nodes: usize, edges: &mut [(u64, u64, u64)]) -> Self {
        // Sort by source node
        edges.sort_by_key(|e| e.0);

        let mut offsets = vec![0u64; num_nodes + 1];
        let mut neighbors = Vec::with_capacity(edges.len());
        for &(src, dst, eid) in edges.iter() {
            if (src as usize) < num_nodes {
                offsets[src as usize + 1] += 1;
            }
            neighbors.push(dst);
            let _ = eid;
        }

        // Prefix sum to get offsets
        for i in 1..=num_nodes {
            offsets[i] += offsets[i - 1];
        }

        CsrIndex { offsets, neighbors }
    }

    /// Get the neighbor node IDs for a given source node.
    pub fn neighbors(&self, src: u64) -> &[u64] {
        let idx = src as usize;
        if idx >= self.offsets.len() - 1 {
            return &[] as &[u64];
        }
        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        &self.neighbors[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_build_and_lookup() {
        // 4 nodes: 0, 1, 2, 3
        // edges: 0->1 (e0), 0->2 (e1), 1->3 (e2), 2->3 (e3)
        let mut edges = vec![(0, 1, 0), (0, 2, 1), (1, 3, 2), (2, 3, 3)];
        let csr = CsrIndex::build(4, &mut edges);

        assert_eq!(csr.neighbors(0), &[1, 2]);
        assert_eq!(csr.neighbors(1), &[3]);
        assert_eq!(csr.neighbors(2), &[3]);
        assert_eq!(csr.neighbors(3), &[] as &[u64]);
    }

    #[test]
    fn test_csr_empty() {
        let mut edges: Vec<(u64, u64, u64)> = vec![];
        let csr = CsrIndex::build(3, &mut edges);
        assert_eq!(csr.neighbors(0), &[] as &[u64]);
        assert_eq!(csr.neighbors(1), &[] as &[u64]);
        assert_eq!(csr.neighbors(2), &[] as &[u64]);
    }

    #[test]
    fn test_csr_out_of_range() {
        let mut edges = vec![(0, 1, 0)];
        let csr = CsrIndex::build(2, &mut edges);
        assert_eq!(csr.neighbors(99), &[] as &[u64]);
    }
}
