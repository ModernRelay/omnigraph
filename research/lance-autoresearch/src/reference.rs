//! IMMUTABLE. Brute-force exact L2 top-K. Used at fixture-build time to compute
//! synthetic-dataset ground truth (against which the agent's PQ-approximate
//! kernel is then scored for recall). For SIFT1M fixtures we use the published
//! ground-truth file instead and never call this at bench-time.

use crate::DIM;

/// Brute-force exact top-K by squared L2. Returns `(id, distance)` ascending.
///
/// Quadratic in `num_vectors`; only used by the fixture builder, not the hot path.
pub fn brute_force_top_k_l2(
    query: &[f32],
    base: &[f32],
    num_vectors: usize,
    k: usize,
) -> Vec<(u32, f32)> {
    assert_eq!(query.len(), DIM);
    assert_eq!(base.len(), num_vectors * DIM);

    let mut dists: Vec<(u32, f32)> = (0..num_vectors)
        .map(|i| {
            let v = &base[i * DIM..(i + 1) * DIM];
            let mut acc = 0.0f32;
            for d in 0..DIM {
                let diff = query[d] - v[d];
                acc += diff * diff;
            }
            (i as u32, acc)
        })
        .collect();

    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    dists.truncate(k);
    dists
}
