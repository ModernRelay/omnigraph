//! Lance autoresearch harness — public API for the bench binary, benchmarks, and tests.
//!
//! Layout mirrors Karpathy's nanochat-research / autoresearch three-file contract:
//!
//! - `kernels`   — the AGENT'S PLAYGROUND. May be rewritten freely.
//! - `reference` — IMMUTABLE. Exact brute-force baseline used to certify recall.
//! - `fixture`   — IMMUTABLE. Dataset + frozen codebook loader.
//!
//! Constants are global because the agent shouldn't have to thread sizes through
//! its kernel — they pin the optimization target (SIFT1M-shaped: 128-d f32,
//! 16 sub-vectors × 256 centroids × 8-d, top-10).

pub mod fixture;
pub mod kernels;
pub mod reference;

pub const DIM: usize = 128;
pub const NUM_SUB_VECTORS: usize = 16;
pub const NUM_CENTROIDS: usize = 256;
pub const SUB_VECTOR_DIM: usize = DIM / NUM_SUB_VECTORS;
pub const TOP_K: usize = 10;
