// SPDX-License-Identifier: MIT OR Apache-2.0
//
// AGENT'S PLAYGROUND. This is the file you (the agent) modify.
//
// Algorithmically modeled on the L2 path in lance-linalg's distance / pq modules
// (Lance 4.x, Apache-2.0; https://github.com/lance-format/lance). It is *not* a
// verbatim vendored copy — pulling in lance-linalg's private helpers as deps
// would couple this harness to crate internals and slow rebuilds. The baseline
// is intentionally a clean scalar implementation of the same algorithm Lance
// uses: pre-process the codebook into a kernel context, build an asymmetric
// distance LUT per query, then probe every PQ-encoded vector via
// `num_sub_vectors` table lookups + an accumulator.
//
// PUBLIC API CONTRACT (must remain stable so the bench keeps building):
//   - `pub struct PqKernel`
//   - `PqKernel::new(shape: PqShape, codebook: &[f32]) -> Self`
//   - `PqKernel::distance_table(&self, query: &[f32], out: &mut [f32])`
//   - `PqKernel::probe_top_k(&self, table: &[f32], codes: &[u8], num_vectors: usize, k: usize) -> Vec<(u32, f32)>`
//
// What you CAN do:
//   - Pre-process the codebook in `new` (transpose, pack into u8 LUT, cache c·c
//     for the FMA trick, etc.). Build cost is paid once per dataset, amortized
//     across queries — the bench measures per-query, not per-(build + query).
//   - Reorder loops, switch internal data layouts, drop down to `std::arch`
//     intrinsics under `#[cfg(target_arch = ...)]` gates (always keep a
//     portable scalar fallback so this compiles everywhere).
//   - Use `unsafe` if needed; document the invariants.
//   - Add private helpers freely.
//
// What you CANNOT do:
//   - Change the public API above.
//   - Modify lib.rs / reference.rs / inputs.rs / run_experiment.rs / benches/.
//   - Lose accuracy — the correctness phase asserts max_abs_err ≤ 1e-4 against
//     the scalar reference on every input. Lossy techniques (u8-LUT
//     quantization, etc.) will fail the gate; if you want to explore them,
//     surface a separate kernel and propose a "lossy track" extension to the
//     human.

use crate::PqShape;

/// Kernel context. Pre-computed state derived from the codebook lives here.
pub struct PqKernel {
    shape: PqShape,
    /// Codebook stored in `[m][k][d]` layout.
    codebook: Vec<f32>,
}

impl PqKernel {
    /// Build a kernel context from a codebook. Pre-processing time is amortized
    /// across all queries against this kernel.
    ///
    /// `codebook` layout: `[num_sub_vectors][num_centroids][sub_vector_dim]` flat.
    pub fn new(shape: PqShape, codebook: &[f32]) -> Self {
        debug_assert_eq!(codebook.len(), shape.codebook_len());
        Self {
            shape,
            codebook: codebook.to_vec(),
        }
    }

    /// Write the asymmetric L2 distance table for one query into `out`.
    ///
    /// `out` layout: `[num_sub_vectors][num_centroids]` flat
    /// (`out[m * num_centroids + k]`). Caller pre-allocates `out` with length
    /// `shape.distance_table_len()`; the bench reuses one buffer across all
    /// queries so allocator cost stays out of the per-query timing.
    #[allow(clippy::needless_range_loop)]
    pub fn distance_table(&self, query: &[f32], out: &mut [f32]) {
        let s = &self.shape;
        let svd = s.sub_vector_dim();
        debug_assert_eq!(query.len(), s.dim);
        debug_assert_eq!(out.len(), s.distance_table_len());

        for m in 0..s.num_sub_vectors {
            let q_sub = &query[m * svd..(m + 1) * svd];
            let cb_off = m * s.num_centroids * svd;
            let tbl_off = m * s.num_centroids;
            for k in 0..s.num_centroids {
                let base = cb_off + k * svd;
                let mut acc = 0.0f32;
                for d in 0..svd {
                    let diff = q_sub[d] - self.codebook[base + d];
                    acc += diff * diff;
                }
                out[tbl_off + k] = acc;
            }
        }
    }

    /// Probe `num_vectors` PQ-encoded vectors and return top-K by ascending
    /// distance.
    ///
    /// `codes` layout: `[num_vectors][num_sub_vectors]` flat.
    pub fn probe_top_k(
        &self,
        table: &[f32],
        codes: &[u8],
        num_vectors: usize,
        k: usize,
    ) -> Vec<(u32, f32)> {
        let s = &self.shape;
        debug_assert_eq!(table.len(), s.distance_table_len());
        debug_assert_eq!(codes.len(), num_vectors * s.num_sub_vectors);

        let mut heap = TopKHeap::with_capacity(k);
        for i in 0..num_vectors {
            let off = i * s.num_sub_vectors;
            let mut acc = 0.0f32;
            for m in 0..s.num_sub_vectors {
                let c = codes[off + m] as usize;
                acc += table[m * s.num_centroids + c];
            }
            heap.push(i as u32, acc);
        }
        heap.into_sorted()
    }
}

/// Variable-capacity max-heap that keeps the K *smallest*-distance entries.
/// Root is the largest of the K kept distances, so admission is one comparison.
struct TopKHeap {
    cap: usize,
    entries: Vec<(u32, f32)>,
}

impl TopKHeap {
    fn with_capacity(k: usize) -> Self {
        Self {
            cap: k,
            entries: Vec::with_capacity(k),
        }
    }

    #[inline]
    fn push(&mut self, id: u32, dist: f32) {
        if self.entries.len() < self.cap {
            self.entries.push((id, dist));
            if self.entries.len() == self.cap {
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
        for i in (0..self.entries.len() / 2).rev() {
            self.sift_down(i);
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let len = self.entries.len();
        loop {
            let l = 2 * i + 1;
            let r = 2 * i + 2;
            let mut largest = i;
            if l < len && self.entries[l].1 > self.entries[largest].1 {
                largest = l;
            }
            if r < len && self.entries[r].1 > self.entries[largest].1 {
                largest = r;
            }
            if largest == i {
                return;
            }
            self.entries.swap(i, largest);
            i = largest;
        }
    }

    fn into_sorted(mut self) -> Vec<(u32, f32)> {
        self.entries
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        self.entries
    }
}
