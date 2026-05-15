//! Default tolerance constants for bit-exact correctness oracles.
//!
//! These suit float-arithmetic kernels (PQ distance, BM25 scoring, vector
//! normalization) where SIMD-accumulator reordering is legal but real bugs
//! shift values by orders of magnitude. Targets that operate on integer or
//! byte-exact data (bitpack decode, dictionary decode, FSST decode) should
//! assert strict bitwise equality and not use these constants.

/// Maximum permitted absolute element error between agent kernel output and
/// scalar reference output, for float kernels.
pub const MAX_ABS_ERR: f32 = 1e-4;

/// Maximum permitted distance error when comparing top-K results between
/// agent kernel and scalar reference, for float kernels.
pub const TOPK_DIST_TOL: f32 = 1e-4;
