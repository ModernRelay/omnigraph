//! Lance autoresearch harness — public API for the bench binary, benchmarks, and tests.
//!
//! Contract (Karpathy-style three files):
//!
//! - `kernels` — the AGENT'S PLAYGROUND. Modify freely.
//! - `reference` — IMMUTABLE. Scalar reference kernel. Defines the math.
//! - `inputs` — IMMUTABLE. Diverse test-data + workload generators,
//!   deterministic per fixed seed, varied across the input battery.
//!
//! The optimization target is dataset-independent: the agent's kernel must match
//! the scalar reference within `MAX_ABS_ERR` on every input the bench generates,
//! and minimize geomean ns/query across multiple PQ shapes and data
//! distributions. There is no fixed dataset; an "improvement" by construction
//! generalizes across distributions and shapes.

pub mod inputs;
pub mod kernels;
pub mod reference;

/// Geometry of a PQ index: vector dimension, number of sub-quantizers, centroids
/// per sub-quantizer. We pin nbits=8 (256 centroids) — the dominant Lance code
/// path. `dim` must be divisible by `num_sub_vectors`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PqShape {
    pub dim: usize,
    pub num_sub_vectors: usize,
    pub num_centroids: usize,
}

impl PqShape {
    pub const fn new(dim: usize, num_sub_vectors: usize, num_centroids: usize) -> Self {
        Self {
            dim,
            num_sub_vectors,
            num_centroids,
        }
    }
    pub const fn sub_vector_dim(&self) -> usize {
        self.dim / self.num_sub_vectors
    }
    pub const fn distance_table_len(&self) -> usize {
        self.num_sub_vectors * self.num_centroids
    }
    pub const fn codebook_len(&self) -> usize {
        self.num_sub_vectors * self.num_centroids * self.sub_vector_dim()
    }
}

/// Tolerance for the agent kernel's distance values vs. the scalar reference.
/// Loose enough to permit legal SIMD-accumulator reordering; tight enough to
/// catch real arithmetic bugs.
pub const MAX_ABS_ERR: f32 = 1e-4;

/// Tolerance for top-K *distances* (id sets are compared with tie-tolerance —
/// see `reference::topk_consistent`).
pub const TOPK_DIST_TOL: f32 = 1e-4;
