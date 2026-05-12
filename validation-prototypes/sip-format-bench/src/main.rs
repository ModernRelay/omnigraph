//! MR-925 Experiment 1.4 — roaring bitmap variant for u64 row IDs (SIP wire format).
//!
//! Validates MR-737 §5.6 (semi-join side-information / SIP filter wire format)
//! and §5.8 / Open Q4 (does roaring win at our representative payload shapes,
//! or do we want a hand-rolled sorted-Vec<u64> + varint encoding?).
//!
//! Encodings compared:
//!   - SortedVec u64 raw little-endian (control / floor — no compression).
//!   - SortedVec u64 + varint over deltas (cheap compression).
//!   - RoaringTreemap (the roaring crate's u64 wrapper over BTreeMap<u32, RoaringBitmap>).
//!
//! Workload cells (representative of Lance row IDs):
//!   - n_elements: 1K, 10K, 100K, 1M.
//!   - distribution: random uniform across u64, clustered by fragment
//!     (fragment_id in upper 32 bits, dense local row in lower 32 bits).
//!   - shape: dense (90% of fragment space covered) vs sparse (1% covered).

use std::time::Instant;

use anyhow::Result;
use rand::prelude::*;
use rand::rngs::StdRng;
use roaring::RoaringTreemap;

#[derive(Clone, Copy, Debug)]
enum Distribution {
    UniformRandom,
    DenseClustered,  // 90% of N_FRAGS fragments densely populated, each fragment ~90% full
    SparseClustered, // 90% of N_FRAGS fragments sparsely populated, each fragment ~1% full
}

#[derive(Clone)]
struct Cell {
    name: &'static str,
    n_elements: usize,
    distribution: Distribution,
}

fn cells() -> Vec<Cell> {
    let sizes = [1_000usize, 10_000, 100_000, 1_000_000];
    let distributions = [
        ("uniform", Distribution::UniformRandom),
        ("dense", Distribution::DenseClustered),
        ("sparse", Distribution::SparseClustered),
    ];
    let mut out = vec![];
    for n in sizes {
        for (dname, d) in distributions {
            out.push(Cell {
                name: Box::leak(format!("{dname}_n={}", n).into_boxed_str()),
                n_elements: n,
                distribution: d,
            });
        }
    }
    out
}

fn gen_ids(cell: &Cell, rng: &mut StdRng) -> Vec<u64> {
    let n = cell.n_elements;
    let mut ids: Vec<u64> = match cell.distribution {
        Distribution::UniformRandom => (0..n).map(|_| rng.r#gen::<u64>()).collect(),
        Distribution::DenseClustered => {
            // Cluster into ~16 fragments, each fragment_id stable, local row indices dense.
            let n_frags = 16u64;
            let mut out = Vec::with_capacity(n);
            let mut frag_count = vec![0u64; n_frags as usize];
            for _ in 0..n {
                let f = rng.gen_range(0..n_frags) as usize;
                let local = frag_count[f];
                frag_count[f] += 1;
                let frag_id = f as u64;
                out.push((frag_id << 32) | local);
            }
            out
        }
        Distribution::SparseClustered => {
            // 16 fragments but each fragment has a very wide local-row range (1M),
            // populated with N/16 sparse rows.
            let n_frags = 16u64;
            let local_range = 1_000_000u64;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let f = rng.gen_range(0..n_frags);
                let local = rng.gen_range(0..local_range);
                out.push((f << 32) | local);
            }
            out
        }
    };
    ids.sort_unstable();
    ids.dedup();
    ids
}

// ---------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------

fn enc_raw_le(ids: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ids.len() * 8);
    for v in ids {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn dec_raw_le(buf: &[u8]) -> Vec<u64> {
    let mut out = Vec::with_capacity(buf.len() / 8);
    for chunk in buf.chunks_exact(8) {
        out.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    out
}

fn write_varint_u64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn read_varint_u64(buf: &[u8], cursor: &mut usize) -> u64 {
    let mut shift = 0u32;
    let mut out = 0u64;
    loop {
        let b = buf[*cursor];
        *cursor += 1;
        out |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return out;
        }
        shift += 7;
    }
}

fn enc_varint_deltas(ids: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ids.len() * 2);
    write_varint_u64(&mut out, ids.len() as u64);
    let mut prev = 0u64;
    for &v in ids {
        let delta = v - prev;
        write_varint_u64(&mut out, delta);
        prev = v;
    }
    out
}

fn dec_varint_deltas(buf: &[u8]) -> Vec<u64> {
    let mut cursor = 0;
    let n = read_varint_u64(buf, &mut cursor) as usize;
    let mut out = Vec::with_capacity(n);
    let mut prev = 0u64;
    for _ in 0..n {
        let delta = read_varint_u64(buf, &mut cursor);
        let v = prev + delta;
        out.push(v);
        prev = v;
    }
    out
}

fn enc_roaring(ids: &[u64]) -> Vec<u8> {
    let mut rb = RoaringTreemap::new();
    rb.extend(ids.iter().copied());
    let mut out = Vec::with_capacity(rb.serialized_size());
    rb.serialize_into(&mut out).unwrap();
    out
}

fn dec_roaring(buf: &[u8]) -> RoaringTreemap {
    RoaringTreemap::deserialize_from(buf).unwrap()
}

// ---------------------------------------------------------------------------
// Bench harness
// ---------------------------------------------------------------------------

fn time_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e3
}

#[derive(Default, Debug)]
struct Result1 {
    enc_ms: f64,
    dec_ms: f64,
    contains_1k_ms: f64,
    intersect_ms: f64,
    bytes: usize,
}

fn bench_raw(ids: &[u64], probe_targets: &[u64], other: &[u64]) -> Result1 {
    let t = Instant::now();
    let buf = enc_raw_le(ids);
    let enc_ms = time_ms(t);

    let t = Instant::now();
    let _ = dec_raw_le(&buf);
    let dec_ms = time_ms(t);

    let t = Instant::now();
    let mut hits = 0u64;
    for &p in probe_targets {
        if ids.binary_search(&p).is_ok() {
            hits += 1;
        }
    }
    let contains_1k_ms = time_ms(t);
    std::hint::black_box(hits);

    let t = Instant::now();
    let n: usize = intersect_sorted(ids, other);
    let intersect_ms = time_ms(t);
    std::hint::black_box(n);

    Result1 {
        enc_ms,
        dec_ms,
        contains_1k_ms,
        intersect_ms,
        bytes: buf.len(),
    }
}

fn bench_varint(ids: &[u64], probe_targets: &[u64], other: &[u64]) -> Result1 {
    let t = Instant::now();
    let buf = enc_varint_deltas(ids);
    let enc_ms = time_ms(t);

    let t = Instant::now();
    let decoded = dec_varint_deltas(&buf);
    let dec_ms = time_ms(t);
    debug_assert_eq!(decoded, ids);

    // contains requires a sorted Vec — use the decoded result, which is the
    // shape callers would consume.
    let t = Instant::now();
    let mut hits = 0u64;
    for &p in probe_targets {
        if decoded.binary_search(&p).is_ok() {
            hits += 1;
        }
    }
    let contains_1k_ms = time_ms(t);
    std::hint::black_box(hits);

    let t = Instant::now();
    let n: usize = intersect_sorted(&decoded, other);
    let intersect_ms = time_ms(t);
    std::hint::black_box(n);

    Result1 {
        enc_ms,
        dec_ms,
        contains_1k_ms,
        intersect_ms,
        bytes: buf.len(),
    }
}

fn bench_roaring(ids: &[u64], probe_targets: &[u64], other: &[u64]) -> Result1 {
    let t = Instant::now();
    let buf = enc_roaring(ids);
    let enc_ms = time_ms(t);

    let t = Instant::now();
    let rb = dec_roaring(&buf);
    let dec_ms = time_ms(t);

    let t = Instant::now();
    let mut hits = 0u64;
    for &p in probe_targets {
        if rb.contains(p) {
            hits += 1;
        }
    }
    let contains_1k_ms = time_ms(t);
    std::hint::black_box(hits);

    let t = Instant::now();
    let mut other_rb = RoaringTreemap::new();
    other_rb.extend(other.iter().copied());
    let intersection = rb & other_rb;
    let intersect_ms = time_ms(t);
    std::hint::black_box(intersection.len());

    Result1 {
        enc_ms,
        dec_ms,
        contains_1k_ms,
        intersect_ms,
        bytes: buf.len(),
    }
}

fn intersect_sorted(a: &[u64], b: &[u64]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut count = 0;
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] > b[j] {
            j += 1;
        } else {
            count += 1;
            i += 1;
            j += 1;
        }
    }
    count
}

fn main() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(0xC0FFEEFEEDFACE);

    println!(
        "{:<28} {:>8} {:>9} {:>9} {:>10} {:>10} {:>11}",
        "cell × encoding", "bytes", "enc_ms", "dec_ms", "cnt_1k_ms", "isect_ms", "bits/elem"
    );
    println!("{:-<92}", "");

    for cell in cells() {
        let ids = gen_ids(&cell, &mut rng);
        let other = gen_ids(&cell, &mut rng);

        // Probe targets: 1000 random samples from the input + 1000 misses.
        let mut probes: Vec<u64> = ids.choose_multiple(&mut rng, 1000).copied().collect();
        for _ in 0..1000 {
            probes.push(rng.r#gen::<u64>());
        }

        for (label, r) in [
            ("raw-LE", bench_raw(&ids, &probes, &other)),
            ("varint-delta", bench_varint(&ids, &probes, &other)),
            ("roaring", bench_roaring(&ids, &probes, &other)),
        ] {
            let bits_per_elem = (r.bytes * 8) as f64 / ids.len() as f64;
            println!(
                "{:<28} {:>8} {:>9.3} {:>9.3} {:>10.3} {:>10.3} {:>11.2}",
                format!("{} × {}", cell.name, label),
                r.bytes,
                r.enc_ms,
                r.dec_ms,
                r.contains_1k_ms,
                r.intersect_ms,
                bits_per_elem,
            );
        }
        println!();
    }
    Ok(())
}
