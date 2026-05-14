//! IMMUTABLE. Scalar reference kernel — defines the math the agent must match.
//!
//! Same public API as `kernels::PqKernel`, intentionally — it's the bit-for-bit
//! oracle the bench compares against. The implementation here is deliberately
//! plain: no SIMD, no preprocessing, no cleverness. If `kernels::PqKernel`
//! produces a distance value off by more than `MAX_ABS_ERR` from this, the
//! correctness phase fails and the agent's edit is rejected.

use crate::PqShape;

pub struct ScalarReference {
    shape: PqShape,
    codebook: Vec<f32>,
}

impl ScalarReference {
    pub fn new(shape: PqShape, codebook: &[f32]) -> Self {
        assert_eq!(codebook.len(), shape.codebook_len());
        Self {
            shape,
            codebook: codebook.to_vec(),
        }
    }

    pub fn shape(&self) -> &PqShape {
        &self.shape
    }

    #[allow(clippy::needless_range_loop)]
    pub fn distance_table(&self, query: &[f32]) -> Vec<f32> {
        let s = &self.shape;
        let svd = s.sub_vector_dim();
        assert_eq!(query.len(), s.dim);

        let mut table = vec![0.0f32; s.distance_table_len()];
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
                table[tbl_off + k] = acc;
            }
        }
        table
    }

    pub fn probe_top_k(
        &self,
        table: &[f32],
        codes: &[u8],
        num_vectors: usize,
        k: usize,
    ) -> Vec<(u32, f32)> {
        let s = &self.shape;
        assert_eq!(table.len(), s.distance_table_len());
        assert_eq!(codes.len(), num_vectors * s.num_sub_vectors);

        let mut all: Vec<(u32, f32)> = (0..num_vectors)
            .map(|i| {
                let off = i * s.num_sub_vectors;
                let mut acc = 0.0f32;
                for m in 0..s.num_sub_vectors {
                    let c = codes[off + m] as usize;
                    acc += table[m * s.num_centroids + c];
                }
                (i as u32, acc)
            })
            .collect();
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(k);
        all
    }
}

/// Compare two distance tables and report the worst absolute element error.
pub fn max_abs_err(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).abs())
        .fold(0.0f32, f32::max)
}

/// Check two top-K results are equivalent up to:
///   - shared distance tolerance `dist_tol`
///   - distance-tied id substitution (if two candidates have equal distances,
///     either order is acceptable)
///
/// Returns `Ok(())` on match, or `Err(diagnostic_string)` describing the first
/// disagreement found.
pub fn topk_consistent(
    agent: &[(u32, f32)],
    reference: &[(u32, f32)],
    dist_tol: f32,
) -> Result<(), String> {
    if agent.len() != reference.len() {
        return Err(format!(
            "topk length mismatch: agent={} reference={}",
            agent.len(),
            reference.len()
        ));
    }
    for (i, ((a_id, a_d), (r_id, r_d))) in agent.iter().zip(reference).enumerate() {
        if (a_d - r_d).abs() > dist_tol {
            return Err(format!(
                "topk[{i}] distance mismatch: agent=({a_id}, {a_d}) reference=({r_id}, {r_d}) | err={}",
                (a_d - r_d).abs()
            ));
        }
        if a_id != r_id {
            // Different id at same rank is acceptable iff this distance is tied
            // with a neighbor in either result — we accept any permutation
            // within a tied-distance band.
            let agent_neighbor_match = agent.iter().any(|(id, d)| id == r_id && (d - r_d).abs() <= dist_tol);
            let ref_neighbor_match = reference.iter().any(|(id, d)| id == a_id && (d - a_d).abs() <= dist_tol);
            if !agent_neighbor_match || !ref_neighbor_match {
                return Err(format!(
                    "topk[{i}] id mismatch with no tie-break excuse: agent=({a_id}, {a_d}) reference=({r_id}, {r_d})"
                ));
            }
        }
    }
    Ok(())
}
