//! Autoresearch target: Lance PQ L2 distance kernel optimization.
//!
//! Karpathy-style three-file contract:
//!
//! - `kernels` — the AGENT'S PLAYGROUND. Modify freely.
//! - `reference` — IMMUTABLE. Scalar reference kernel. Defines the math.
//! - `inputs` — IMMUTABLE. Diverse test-data + workload generators,
//!   deterministic per fixed seed, varied across the input battery.
//!
//! The optimization target is dataset-independent: the agent's kernel must
//! match the scalar reference within `harness_common::MAX_ABS_ERR` on every
//! input the bench generates, and minimize geomean ns/query across multiple
//! PQ shapes and data distributions. There is no fixed dataset.
//!
//! Shared utilities (deterministic PRNG, geomean, peak RSS, tolerance
//! constants, time budget) come from the `harness-common` workspace crate.
//! See `../HARNESS.md` for the harness conventions every target follows.

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
