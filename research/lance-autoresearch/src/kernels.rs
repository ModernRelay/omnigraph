// SPDX-License-Identifier: Apache-2.0
//
// AGENT'S PLAYGROUND. This is the file you (the agent) modify.
//
// Algorithmically modeled on the L2 path in lance-linalg's distance / pq modules
// (Lance 4.x, Apache-2.0; see https://github.com/lance-format/lance). It is *not*
// a verbatim vendored copy — pulling in lance-linalg's private helpers as deps
// would couple this harness to crate internals and slow rebuilds. The baseline is
// intentionally a clean scalar implementation of the same algorithm Lance uses:
// build an asymmetric distance LUT, then probe every PQ-encoded vector via 16
// table lookups + an accumulator. Beating the baseline (and porting wins back
// upstream) is the point of this repo.
//
// PUBLIC API CONTRACT (must remain stable so `bin/run_experiment.rs` keeps building):
//   - DistanceTable type alias
//   - compute_distance_table_l2(query, codebook) -> DistanceTable
//   - probe_pq_l2_top_k(table, codes, num_vectors, &mut TopKHeap)
//   - TopKHeap::new() / push / into_sorted
//
// You may add private helpers, switch internal data layouts (e.g. transpose the
// codebook for vectorized table-build, pack the LUT for `pshufb`), drop down to
// `std::arch` intrinsics behind cfg gates, mark functions `#[inline]`, etc.
// You may NOT change `DIM` / `NUM_SUB_VECTORS` / `NUM_CENTROIDS` / `TOP_K`
// (those are pinned by the fixture geometry in `lib.rs`).

use crate::{NUM_CENTROIDS, NUM_SUB_VECTORS, SUB_VECTOR_DIM, TOP_K};

/// Precomputed asymmetric L2 distance table.
///
/// Indexed as `table[sub_vector_idx][centroid_idx]`. Each entry is the squared
/// L2 distance from the query's `m`-th sub-vector to the `k`-th centroid of the
/// `m`-th sub-quantizer.
pub type DistanceTable = [[f32; NUM_CENTROIDS]; NUM_SUB_VECTORS];

/// Build the asymmetric distance table for one query against the codebook.
///
/// `codebook` layout: contiguous `[NUM_SUB_VECTORS][NUM_CENTROIDS][SUB_VECTOR_DIM]`.
#[allow(clippy::needless_range_loop)]
pub fn compute_distance_table_l2(query: &[f32], codebook: &[f32]) -> DistanceTable {
    debug_assert_eq!(query.len(), NUM_SUB_VECTORS * SUB_VECTOR_DIM);
    debug_assert_eq!(
        codebook.len(),
        NUM_SUB_VECTORS * NUM_CENTROIDS * SUB_VECTOR_DIM
    );

    let mut table = [[0.0f32; NUM_CENTROIDS]; NUM_SUB_VECTORS];
    for m in 0..NUM_SUB_VECTORS {
        let q_sub = &query[m * SUB_VECTOR_DIM..(m + 1) * SUB_VECTOR_DIM];
        let cb_offset = m * NUM_CENTROIDS * SUB_VECTOR_DIM;
        for k in 0..NUM_CENTROIDS {
            let base = cb_offset + k * SUB_VECTOR_DIM;
            let mut acc = 0.0f32;
            for d in 0..SUB_VECTOR_DIM {
                let diff = q_sub[d] - codebook[base + d];
                acc += diff * diff;
            }
            table[m][k] = acc;
        }
    }
    table
}

/// Probe every PQ-encoded vector and accumulate the top-K minimum distances.
///
/// `codes` layout: `[num_vectors][NUM_SUB_VECTORS]` packed; one byte per sub-quantizer.
pub fn probe_pq_l2_top_k(
    table: &DistanceTable,
    codes: &[u8],
    num_vectors: usize,
    out: &mut TopKHeap,
) {
    debug_assert_eq!(codes.len(), num_vectors * NUM_SUB_VECTORS);

    for i in 0..num_vectors {
        let off = i * NUM_SUB_VECTORS;
        let mut acc = 0.0f32;
        for m in 0..NUM_SUB_VECTORS {
            let k = codes[off + m] as usize;
            acc += table[m][k];
        }
        out.push(i as u32, acc);
    }
}

/// Fixed-capacity max-heap that keeps the K *smallest*-distance entries seen.
///
/// Root is the largest of the K kept distances, so deciding whether to admit a
/// new entry is one comparison.
pub struct TopKHeap {
    entries: [(u32, f32); TOP_K],
    len: usize,
}

impl Default for TopKHeap {
    fn default() -> Self {
        Self::new()
    }
}

impl TopKHeap {
    pub fn new() -> Self {
        Self {
            entries: [(u32::MAX, f32::INFINITY); TOP_K],
            len: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, id: u32, dist: f32) {
        if self.len < TOP_K {
            self.entries[self.len] = (id, dist);
            self.len += 1;
            if self.len == TOP_K {
                self.heapify();
            }
            return;
        }
        if dist < self.entries[0].1 {
            self.entries[0] = (id, dist);
            self.sift_down(0);
        }
    }

    fn heapify(&mut self) {
        for i in (0..TOP_K / 2).rev() {
            self.sift_down(i);
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let l = 2 * i + 1;
            let r = 2 * i + 2;
            let mut largest = i;
            if l < self.len && self.entries[l].1 > self.entries[largest].1 {
                largest = l;
            }
            if r < self.len && self.entries[r].1 > self.entries[largest].1 {
                largest = r;
            }
            if largest == i {
                return;
            }
            self.entries.swap(i, largest);
            i = largest;
        }
    }

    pub fn into_sorted(self) -> Vec<(u32, f32)> {
        let mut v: Vec<_> = self.entries[..self.len].to_vec();
        v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        v
    }
}
