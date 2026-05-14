//! IMMUTABLE. Fixture loader.
//!
//! The bench runs against one of:
//!   - SIFT1M (preferred; 128-d, 1M base, 10k queries, published ground truth)
//!     loaded from `~/.cache/lance-autoresearch/{sift_base,sift_query,sift_groundtruth}.fvecs|.ivecs`
//!     plus pre-trained frozen artifacts `pq_codebook.bin` and `pq_codes.bin`.
//!   - A synthetic fallback (1024 base / 64 queries, deterministic seed) so the
//!     harness is smoke-testable without any external download.
//!
//! Run `scripts/prepare_fixtures.sh` once to populate the SIFT1M fixtures.

use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

use crate::reference::brute_force_top_k_l2;
use crate::{DIM, NUM_CENTROIDS, NUM_SUB_VECTORS, SUB_VECTOR_DIM};

pub const SYNTHETIC_NUM_BASE: usize = 1024;
pub const SYNTHETIC_NUM_QUERY: usize = 64;
pub const SYNTHETIC_TOP_K_TRUTH: usize = 32;
const KMEANS_ITERS: usize = 12;

pub enum FixtureSource {
    Sift1M,
    Synthetic { seed: u64 },
}

pub struct Fixture {
    pub base_vectors: Vec<f32>,
    pub query_vectors: Vec<f32>,
    pub codebook: Vec<f32>,
    pub codes: Vec<u8>,
    pub groundtruth: Vec<u32>,
    pub num_base: usize,
    pub num_query: usize,
    pub top_k_truth: usize,
    pub source: FixtureSource,
}

impl Fixture {
    /// Try SIFT1M first; fall back to a deterministic synthetic dataset.
    pub fn load_or_synthesize() -> Result<Self> {
        let dir = cache_dir();
        if dir.join("sift_base.fvecs").exists()
            && dir.join("sift_query.fvecs").exists()
            && dir.join("sift_groundtruth.ivecs").exists()
            && dir.join("pq_codebook.bin").exists()
            && dir.join("pq_codes.bin").exists()
        {
            Self::load_sift1m(&dir)
        } else {
            Self::synthesize(SYNTHETIC_NUM_BASE, SYNTHETIC_NUM_QUERY, 0xC0FFEE_C0FFEE)
        }
    }

    pub fn source_str(&self) -> &'static str {
        match self.source {
            FixtureSource::Sift1M => "sift1m",
            FixtureSource::Synthetic { .. } => "synthetic",
        }
    }

    fn load_sift1m(dir: &Path) -> Result<Self> {
        let base_vectors = read_fvecs(&dir.join("sift_base.fvecs"))?;
        let query_vectors = read_fvecs(&dir.join("sift_query.fvecs"))?;
        let (groundtruth, top_k_truth) = read_ivecs(&dir.join("sift_groundtruth.ivecs"))?;
        let codebook = read_f32_bin(&dir.join("pq_codebook.bin"))?;
        let codes = read_u8_bin(&dir.join("pq_codes.bin"))?;

        let num_base = base_vectors.len() / DIM;
        let num_query = query_vectors.len() / DIM;
        if codebook.len() != NUM_SUB_VECTORS * NUM_CENTROIDS * SUB_VECTOR_DIM {
            return Err(anyhow!(
                "codebook size mismatch: got {}, expected {}",
                codebook.len(),
                NUM_SUB_VECTORS * NUM_CENTROIDS * SUB_VECTOR_DIM
            ));
        }
        if codes.len() != num_base * NUM_SUB_VECTORS {
            return Err(anyhow!(
                "codes size mismatch: got {}, expected {}",
                codes.len(),
                num_base * NUM_SUB_VECTORS
            ));
        }

        Ok(Self {
            base_vectors,
            query_vectors,
            codebook,
            codes,
            groundtruth,
            num_base,
            num_query,
            top_k_truth,
            source: FixtureSource::Sift1M,
        })
    }

    fn synthesize(num_base: usize, num_query: usize, seed: u64) -> Result<Self> {
        let mut rng = SplitMix64::new(seed);
        // Cluster the base set so PQ has structure to compress and queries have
        // meaningful nearest neighbors. With i.i.d. Gaussian noise the asymptotic
        // recall of PQ is near-chance; with cluster-shaped data PQ tracks the
        // true top-K closely, which is what we want when smoke-testing kernels.
        let base_vectors = gen_clustered(num_base, DIM, 32, 0.15, &mut rng);
        // Queries are perturbed base points so they have a true near-neighbor.
        let query_vectors = gen_query_near_base(&base_vectors, num_base, num_query, &mut rng);

        let codebook = train_codebook(&base_vectors, num_base, &mut rng);
        let codes = encode(&base_vectors, num_base, &codebook);

        let mut groundtruth = Vec::with_capacity(num_query * SYNTHETIC_TOP_K_TRUTH);
        for qi in 0..num_query {
            let q = &query_vectors[qi * DIM..(qi + 1) * DIM];
            let top = brute_force_top_k_l2(q, &base_vectors, num_base, SYNTHETIC_TOP_K_TRUTH);
            groundtruth.extend(top.iter().map(|(id, _)| *id));
        }

        Ok(Self {
            base_vectors,
            query_vectors,
            codebook,
            codes,
            groundtruth,
            num_base,
            num_query,
            top_k_truth: SYNTHETIC_TOP_K_TRUTH,
            source: FixtureSource::Synthetic { seed },
        })
    }
}

pub fn cache_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    home.join(".cache").join("lance-autoresearch")
}

fn read_fvecs(path: &Path) -> Result<Vec<f32>> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::with_capacity(bytes.len() / 4);
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 > bytes.len() {
            return Err(anyhow!("truncated fvecs header at offset {i}"));
        }
        let dim = u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]) as usize;
        if dim != DIM {
            return Err(anyhow!("fvecs dim {dim} != expected {DIM}"));
        }
        i += 4;
        let row_bytes = dim * 4;
        if i + row_bytes > bytes.len() {
            return Err(anyhow!("truncated fvecs row at offset {i}"));
        }
        for d in 0..dim {
            let off = i + d * 4;
            out.push(f32::from_le_bytes([
                bytes[off],
                bytes[off + 1],
                bytes[off + 2],
                bytes[off + 3],
            ]));
        }
        i += row_bytes;
    }
    Ok(out)
}

fn read_ivecs(path: &Path) -> Result<(Vec<u32>, usize)> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    let mut top_k: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 > bytes.len() {
            return Err(anyhow!("truncated ivecs header"));
        }
        let dim = u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]) as usize;
        i += 4;
        if let Some(k) = top_k {
            if k != dim {
                return Err(anyhow!("ivecs rows have varying widths {k} vs {dim}"));
            }
        } else {
            top_k = Some(dim);
        }
        let row_bytes = dim * 4;
        if i + row_bytes > bytes.len() {
            return Err(anyhow!("truncated ivecs row"));
        }
        for d in 0..dim {
            let off = i + d * 4;
            out.push(u32::from_le_bytes([
                bytes[off],
                bytes[off + 1],
                bytes[off + 2],
                bytes[off + 3],
            ]));
        }
        i += row_bytes;
    }
    Ok((out, top_k.unwrap_or(0)))
}

fn read_f32_bin(path: &Path) -> Result<Vec<f32>> {
    let f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut bytes = Vec::new();
    r.read_to_end(&mut bytes)?;
    if bytes.len() % 4 != 0 {
        return Err(anyhow!("f32 binary file not a multiple of 4 bytes"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .collect())
}

fn read_u8_bin(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("reading {}", path.display()))
}

/// xorshift-ish deterministic PRNG (SplitMix64). Vendored small enough to avoid
/// a `rand` dep — the fixture must be reproducible bit-for-bit.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f32(&mut self) -> f32 {
        let bits = (self.next_u64() >> 40) as u32;
        bits as f32 / ((1u32 << 24) as f32)
    }
    /// Box-Muller standard normal.
    fn next_normal(&mut self) -> f32 {
        let mut u1 = self.next_f32();
        if u1 < 1e-7 {
            u1 = 1e-7;
        }
        let u2 = self.next_f32();
        (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
    }
}

fn gen_vectors(n: usize, d: usize, rng: &mut SplitMix64) -> Vec<f32> {
    let mut out = Vec::with_capacity(n * d);
    for _ in 0..n * d {
        out.push(rng.next_normal());
    }
    out
}

/// Generate `n` vectors of dim `d` as a Gaussian mixture: `num_clusters` random
/// centers, then `n/num_clusters` points per center perturbed by N(0, noise).
fn gen_clustered(n: usize, d: usize, num_clusters: usize, noise: f32, rng: &mut SplitMix64) -> Vec<f32> {
    let centers = gen_vectors(num_clusters, d, rng);
    let mut out = Vec::with_capacity(n * d);
    for i in 0..n {
        let ci = i % num_clusters;
        let center = &centers[ci * d..(ci + 1) * d];
        for &c in center {
            out.push(c + noise * rng.next_normal());
        }
    }
    out
}

/// Generate query vectors by picking `n_query` random base points and perturbing
/// them. Guarantees each query has true near neighbors in the base set.
fn gen_query_near_base(
    base: &[f32],
    num_base: usize,
    n_query: usize,
    rng: &mut SplitMix64,
) -> Vec<f32> {
    let mut out = Vec::with_capacity(n_query * DIM);
    for _ in 0..n_query {
        let src = (rng.next_u64() as usize) % num_base;
        let src_off = src * DIM;
        for d in 0..DIM {
            out.push(base[src_off + d] + 0.05 * rng.next_normal());
        }
    }
    out
}

/// Train a product-quantization codebook by per-subspace k-means.
fn train_codebook(base: &[f32], num_base: usize, rng: &mut SplitMix64) -> Vec<f32> {
    let mut codebook = vec![0.0f32; NUM_SUB_VECTORS * NUM_CENTROIDS * SUB_VECTOR_DIM];

    let k = NUM_CENTROIDS.min(num_base);
    if k == 0 {
        return codebook;
    }

    for m in 0..NUM_SUB_VECTORS {
        for ki in 0..k {
            let src = (rng.next_u64() as usize) % num_base;
            let src_off = src * DIM + m * SUB_VECTOR_DIM;
            let dst_off = m * NUM_CENTROIDS * SUB_VECTOR_DIM + ki * SUB_VECTOR_DIM;
            codebook[dst_off..dst_off + SUB_VECTOR_DIM]
                .copy_from_slice(&base[src_off..src_off + SUB_VECTOR_DIM]);
        }

        let mut assignments = vec![0u8; num_base];
        for _iter in 0..KMEANS_ITERS {
            for i in 0..num_base {
                let sub = &base[i * DIM + m * SUB_VECTOR_DIM..i * DIM + (m + 1) * SUB_VECTOR_DIM];
                let mut best_k = 0u8;
                let mut best_d = f32::INFINITY;
                for ki in 0..k {
                    let c_off = m * NUM_CENTROIDS * SUB_VECTOR_DIM + ki * SUB_VECTOR_DIM;
                    let mut acc = 0.0f32;
                    for d in 0..SUB_VECTOR_DIM {
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

            let mut sums = vec![0.0f32; k * SUB_VECTOR_DIM];
            let mut counts = vec![0u32; k];
            for i in 0..num_base {
                let ki = assignments[i] as usize;
                let sub = &base[i * DIM + m * SUB_VECTOR_DIM..i * DIM + (m + 1) * SUB_VECTOR_DIM];
                for d in 0..SUB_VECTOR_DIM {
                    sums[ki * SUB_VECTOR_DIM + d] += sub[d];
                }
                counts[ki] += 1;
            }
            for ki in 0..k {
                let c_off = m * NUM_CENTROIDS * SUB_VECTOR_DIM + ki * SUB_VECTOR_DIM;
                if counts[ki] == 0 {
                    let src = (rng.next_u64() as usize) % num_base;
                    let src_off = src * DIM + m * SUB_VECTOR_DIM;
                    codebook[c_off..c_off + SUB_VECTOR_DIM]
                        .copy_from_slice(&base[src_off..src_off + SUB_VECTOR_DIM]);
                } else {
                    let inv = 1.0 / counts[ki] as f32;
                    for d in 0..SUB_VECTOR_DIM {
                        codebook[c_off + d] = sums[ki * SUB_VECTOR_DIM + d] * inv;
                    }
                }
            }
        }
    }

    codebook
}

fn encode(base: &[f32], num_base: usize, codebook: &[f32]) -> Vec<u8> {
    let mut out = vec![0u8; num_base * NUM_SUB_VECTORS];
    for i in 0..num_base {
        for m in 0..NUM_SUB_VECTORS {
            let sub = &base[i * DIM + m * SUB_VECTOR_DIM..i * DIM + (m + 1) * SUB_VECTOR_DIM];
            let mut best_k = 0u8;
            let mut best_d = f32::INFINITY;
            for ki in 0..NUM_CENTROIDS {
                let c_off = m * NUM_CENTROIDS * SUB_VECTOR_DIM + ki * SUB_VECTOR_DIM;
                let mut acc = 0.0f32;
                for d in 0..SUB_VECTOR_DIM {
                    let diff = sub[d] - codebook[c_off + d];
                    acc += diff * diff;
                }
                if acc < best_d {
                    best_d = acc;
                    best_k = ki as u8;
                }
            }
            out[i * NUM_SUB_VECTORS + m] = best_k;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    //! Fixture-builder tests. The default smoke test exercises the synthetic path
    //! end-to-end. `build_fixtures` is `#[ignore]` — it runs only when invoked
    //! explicitly by `scripts/prepare_fixtures.sh` and writes the frozen SIFT1M
    //! PQ artifacts to `~/.cache/lance-autoresearch/`.
    use super::*;
    use std::io::Write;

    #[test]
    fn synthetic_fixture_is_self_consistent() {
        let fix = Fixture::synthesize(256, 8, 0xDEADBEEF).unwrap();
        assert_eq!(fix.base_vectors.len(), 256 * DIM);
        assert_eq!(fix.codebook.len(), NUM_SUB_VECTORS * NUM_CENTROIDS * SUB_VECTOR_DIM);
        assert_eq!(fix.codes.len(), 256 * NUM_SUB_VECTORS);
        assert_eq!(fix.groundtruth.len(), 8 * SYNTHETIC_TOP_K_TRUTH);
        for &id in &fix.groundtruth {
            assert!((id as usize) < 256);
        }
    }

    #[test]
    #[ignore]
    fn build_fixtures() {
        if std::env::var("LANCE_AUTORESEARCH_BUILD_FIXTURES").is_err() {
            eprintln!("skipping: set LANCE_AUTORESEARCH_BUILD_FIXTURES=1 to run");
            return;
        }
        let dir = cache_dir();
        let base = read_fvecs(&dir.join("sift_base.fvecs")).expect("read sift_base");
        let num_base = base.len() / DIM;
        eprintln!("[build_fixtures] training PQ codebook on {num_base} vectors...");

        let mut rng = SplitMix64::new(0x0005_1F74_F1AC);
        let codebook = train_codebook(&base, num_base, &mut rng);
        let codes = encode(&base, num_base, &codebook);

        let codebook_bytes: Vec<u8> = codebook
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        std::fs::File::create(dir.join("pq_codebook.bin"))
            .unwrap()
            .write_all(&codebook_bytes)
            .unwrap();
        std::fs::File::create(dir.join("pq_codes.bin"))
            .unwrap()
            .write_all(&codes)
            .unwrap();
        eprintln!("[build_fixtures] wrote {} centroids × {} bytes codebook, {} bytes codes",
            NUM_SUB_VECTORS * NUM_CENTROIDS, SUB_VECTOR_DIM * 4, codes.len());
    }
}
