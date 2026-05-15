//! IMMUTABLE. Diverse test-data + workload generators.
//!
//! Two surfaces. `correctness_battery(seed)` yields several
//! `(shape, query, codebook, codes)` tuples spanning Gaussian, uniform, sparse,
//! large-dynamic-range, and mostly-zero distributions. Used by the correctness
//! phase to pin the agent's kernel against the scalar reference. Same seed
//! produces the same battery, so a regression surfaces in one trial.
//!
//! `speed_workloads(seed)` yields larger `(shape, queries, codebook, codes)`
//! workloads spanning multiple PQ shapes × data distributions. Used by the
//! speed phase. Fixed seed produces reproducible timings across trials.
//!
//! There is no fixed dataset (no SIFT1M, no codebook fixture). Everything is
//! generated deterministically per call. The PQ codebook for each workload is
//! "trained" with a fast pseudo-k-means (random-init + 2 Lloyd iterations) so
//! the codebook is shape-appropriate, not random.

use crate::PqShape;
use harness_common::SplitMix64;

/// PQ shapes the bench evaluates. The agent's kernel must produce correct
/// output and competitive speed on every one.
pub const SHAPES: &[PqShape] = &[
    PqShape::new(128, 16, 256),  // SIFT-like; sub_vec_dim = 8
    PqShape::new(256, 16, 256),  // larger dim, sub_vec_dim = 16
    PqShape::new(768, 96, 256),  // BERT-base-like; sub_vec_dim = 8, large codebook
];

/// Number of base vectors used in each speed workload. Kept moderate so a full
/// trial finishes in seconds, not minutes — the benchmark is per-query speed,
/// not throughput, so absolute scale doesn't change the comparison.
pub const SPEED_NUM_BASE: usize = 20_000;

/// Number of queries timed per (shape, distribution) speed workload.
pub const SPEED_NUM_QUERIES: usize = 32;

/// Training-set size for codebook k-means. Decoupled from base size so a
/// (768, 96) shape doesn't pay 96 × 256 × 50k inner-loop ops just to build
/// fixtures. K-means converges fine on a few thousand training samples.
const CODEBOOK_TRAIN_SIZE: usize = 4096;

pub const SPEED_TOP_K: usize = 10;

#[derive(Clone, Copy, Debug)]
pub enum DataDistribution {
    /// Gaussian-mixture clusters with low intra-cluster noise. SIFT-like.
    Clustered,
    /// Uniform on [-1, 1]. No structure for PQ to exploit.
    Uniform,
    /// 90% of values are exactly zero, the rest are Gaussian. Stresses
    /// branch-on-data optimizations.
    Sparse,
}

pub const DISTRIBUTIONS: &[DataDistribution] = &[
    DataDistribution::Clustered,
    DataDistribution::Uniform,
    DataDistribution::Sparse,
];

pub struct CorrectnessCase {
    pub label: &'static str,
    pub shape: PqShape,
    pub query: Vec<f32>,
    pub codebook: Vec<f32>,
    pub codes: Vec<u8>,
    pub num_vectors: usize,
    pub k: usize,
}

pub struct SpeedWorkload {
    pub shape: PqShape,
    pub distribution: DataDistribution,
    pub queries: Vec<f32>,    // [num_queries × dim] flat
    pub codebook: Vec<f32>,
    pub codes: Vec<u8>,
    pub num_queries: usize,
    pub num_vectors: usize,
    pub k: usize,
}

pub fn correctness_battery(seed: u64) -> Vec<CorrectnessCase> {
    let mut out = Vec::new();
    let kinds: &[(InputKind, &'static str)] = &[
        (InputKind::Gaussian, "gaussian"),
        (InputKind::Uniform, "uniform"),
        (InputKind::Sparse, "sparse"),
        (InputKind::LargeDynamicRange, "large_dynamic_range"),
        (InputKind::MostlyZero, "mostly_zero"),
    ];
    for &shape in SHAPES {
        for (kind, label) in kinds {
            let mut rng = SplitMix64::new(seed ^ shape_hash(shape) ^ kind_hash(*kind));
            let num_vectors = 4096;
            let codebook = build_codebook(shape, *kind, CODEBOOK_TRAIN_SIZE, &mut rng);
            let base = gen_vectors(num_vectors, shape.dim, *kind, &mut rng);
            let codes = encode(shape, num_vectors, &base, &codebook);
            let query = gen_vectors(1, shape.dim, *kind, &mut rng);
            out.push(CorrectnessCase {
                label,
                shape,
                query,
                codebook,
                codes,
                num_vectors,
                k: 10,
            });
        }
    }
    out
}

pub fn speed_workloads(seed: u64) -> Vec<SpeedWorkload> {
    let mut out = Vec::new();
    for &shape in SHAPES {
        for &dist in DISTRIBUTIONS {
            let mut rng = SplitMix64::new(seed ^ shape_hash(shape) ^ dist_hash(dist));
            let kind = match dist {
                DataDistribution::Clustered => InputKind::Clustered,
                DataDistribution::Uniform => InputKind::Uniform,
                DataDistribution::Sparse => InputKind::Sparse,
            };
            let codebook = build_codebook(shape, kind, CODEBOOK_TRAIN_SIZE, &mut rng);
            let base = gen_vectors(SPEED_NUM_BASE, shape.dim, kind, &mut rng);
            let codes = encode(shape, SPEED_NUM_BASE, &base, &codebook);
            let queries = gen_vectors(SPEED_NUM_QUERIES, shape.dim, kind, &mut rng);
            out.push(SpeedWorkload {
                shape,
                distribution: dist,
                queries,
                codebook,
                codes,
                num_queries: SPEED_NUM_QUERIES,
                num_vectors: SPEED_NUM_BASE,
                k: SPEED_TOP_K,
            });
        }
    }
    out
}

#[derive(Clone, Copy, Debug)]
enum InputKind {
    Gaussian,
    Uniform,
    Sparse,
    LargeDynamicRange,
    MostlyZero,
    Clustered,
}

fn gen_vectors(n: usize, dim: usize, kind: InputKind, rng: &mut SplitMix64) -> Vec<f32> {
    let total = n * dim;
    let mut out = Vec::with_capacity(total);
    match kind {
        InputKind::Gaussian => {
            for _ in 0..total {
                out.push(rng.next_normal());
            }
        }
        InputKind::Uniform => {
            for _ in 0..total {
                out.push(rng.next_f32() * 2.0 - 1.0);
            }
        }
        InputKind::Sparse => {
            for _ in 0..total {
                let v = if rng.next_f32() < 0.10 { rng.next_normal() } else { 0.0 };
                out.push(v);
            }
        }
        InputKind::LargeDynamicRange => {
            for _ in 0..total {
                // Mix three orders of magnitude.
                let exp = match (rng.next_u64() % 3) as u32 {
                    0 => -3,
                    1 => 0,
                    _ => 3,
                };
                out.push(rng.next_normal() * (10.0f32).powi(exp));
            }
        }
        InputKind::MostlyZero => {
            for _ in 0..total {
                let v = if rng.next_f32() < 0.01 { rng.next_normal() } else { 0.0 };
                out.push(v);
            }
        }
        InputKind::Clustered => {
            // 32 cluster centers, tight noise, evenly-distributed assignments.
            let num_clusters = 32usize;
            let centers: Vec<f32> = (0..num_clusters * dim).map(|_| rng.next_normal()).collect();
            for i in 0..n {
                let ci = i % num_clusters;
                let center = &centers[ci * dim..(ci + 1) * dim];
                for &c in center {
                    out.push(c + 0.10 * rng.next_normal());
                }
            }
        }
    }
    out
}

/// Build a PQ codebook by random-init plus two Lloyd refinements per subspace.
/// Cheap; not optimal; good enough that codes are non-degenerate so probe-time
/// access patterns look realistic.
fn build_codebook(shape: PqShape, kind: InputKind, num_train: usize, rng: &mut SplitMix64) -> Vec<f32> {
    let svd = shape.sub_vector_dim();
    let train = gen_vectors(num_train, shape.dim, kind, rng);
    let mut codebook = vec![0.0f32; shape.codebook_len()];
    let k = shape.num_centroids.min(num_train);

    for m in 0..shape.num_sub_vectors {
        // Init centroids from random training samples.
        for ki in 0..k {
            let src = (rng.next_u64() as usize) % num_train;
            let src_off = src * shape.dim + m * svd;
            let dst_off = m * shape.num_centroids * svd + ki * svd;
            codebook[dst_off..dst_off + svd]
                .copy_from_slice(&train[src_off..src_off + svd]);
        }

        let mut assignments = vec![0u8; num_train];
        for _iter in 0..2 {
            for i in 0..num_train {
                let sub = &train[i * shape.dim + m * svd..i * shape.dim + (m + 1) * svd];
                let mut best_k = 0u8;
                let mut best_d = f32::INFINITY;
                for ki in 0..k {
                    let c_off = m * shape.num_centroids * svd + ki * svd;
                    let mut acc = 0.0f32;
                    for d in 0..svd {
                        let diff = sub[d] - codebook[c_off + d];
                        acc += diff * diff;
                    }
                    if acc < best_d {
                        best_d = acc;
                        best_k = ki as u8;
                    }
                }
                assignments[i] = best_k;
            }
            let mut sums = vec![0.0f32; k * svd];
            let mut counts = vec![0u32; k];
            for i in 0..num_train {
                let ki = assignments[i] as usize;
                let sub = &train[i * shape.dim + m * svd..i * shape.dim + (m + 1) * svd];
                for d in 0..svd {
                    sums[ki * svd + d] += sub[d];
                }
                counts[ki] += 1;
            }
            for ki in 0..k {
                let c_off = m * shape.num_centroids * svd + ki * svd;
                if counts[ki] == 0 {
                    let src = (rng.next_u64() as usize) % num_train;
                    let src_off = src * shape.dim + m * svd;
                    codebook[c_off..c_off + svd]
                        .copy_from_slice(&train[src_off..src_off + svd]);
                } else {
                    let inv = 1.0 / counts[ki] as f32;
                    for d in 0..svd {
                        codebook[c_off + d] = sums[ki * svd + d] * inv;
                    }
                }
            }
        }
    }
    codebook
}

fn encode(shape: PqShape, n: usize, base: &[f32], codebook: &[f32]) -> Vec<u8> {
    let svd = shape.sub_vector_dim();
    let mut out = vec![0u8; n * shape.num_sub_vectors];
    for i in 0..n {
        for m in 0..shape.num_sub_vectors {
            let sub = &base[i * shape.dim + m * svd..i * shape.dim + (m + 1) * svd];
            let mut best_k = 0u8;
            let mut best_d = f32::INFINITY;
            for ki in 0..shape.num_centroids {
                let c_off = m * shape.num_centroids * svd + ki * svd;
                let mut acc = 0.0f32;
                for d in 0..svd {
                    let diff = sub[d] - codebook[c_off + d];
                    acc += diff * diff;
                }
                if acc < best_d {
                    best_d = acc;
                    best_k = ki as u8;
                }
            }
            out[i * shape.num_sub_vectors + m] = best_k;
        }
    }
    out
}

fn shape_hash(s: PqShape) -> u64 {
    (s.dim as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (s.num_sub_vectors as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9)
        ^ (s.num_centroids as u64).wrapping_mul(0x94D0_49BB_1331_11EB)
}

fn kind_hash(k: InputKind) -> u64 {
    let tag: u64 = match k {
        InputKind::Gaussian => 0x11,
        InputKind::Uniform => 0x22,
        InputKind::Sparse => 0x33,
        InputKind::LargeDynamicRange => 0x44,
        InputKind::MostlyZero => 0x55,
        InputKind::Clustered => 0x66,
    };
    tag.wrapping_mul(0xDEAD_BEEF_CAFE_F00D)
}

fn dist_hash(d: DataDistribution) -> u64 {
    let tag: u64 = match d {
        DataDistribution::Clustered => 0x101,
        DataDistribution::Uniform => 0x202,
        DataDistribution::Sparse => 0x303,
    };
    tag.wrapping_mul(0xFEED_FACE_BABE_CAFE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correctness_battery_is_deterministic() {
        let a = correctness_battery(0xABCD);
        let b = correctness_battery(0xABCD);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.shape, y.shape);
            assert_eq!(x.codes, y.codes);
            assert_eq!(x.query, y.query);
        }
    }

    #[test]
    fn speed_workloads_match_shapes() {
        let w = speed_workloads(0x1234);
        assert_eq!(w.len(), SHAPES.len() * DISTRIBUTIONS.len());
        for wl in w {
            assert_eq!(wl.codebook.len(), wl.shape.codebook_len());
            assert_eq!(wl.codes.len(), wl.num_vectors * wl.shape.num_sub_vectors);
            assert_eq!(wl.queries.len(), wl.num_queries * wl.shape.dim);
        }
    }
}
