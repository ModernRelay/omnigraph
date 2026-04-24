//! Benchmark expand/traversal path and microbenchmark the per-row dedup overhead.
//!
//! Run: `cargo run --release --example bench_expand`
//!
//! Produces end-to-end query latency for 1/2/3-hop Match queries against a
//! synthetic Person/Knows graph, plus a microbench comparing
//! `HashSet<String>` vs `HashSet<u32>` dedup inside the BFS inner loop.

use std::collections::HashSet;
use std::time::Instant;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
edge Knows: Person -> Person
"#;

const QUERIES: &str = r#"
query hop1() {
    match {
        $a: Person
        $b: Person
        $a knows $b
    }
    return { $a.name, $b.name }
}

query hop2() {
    match {
        $a: Person
        $b: Person
        $c: Person
        $a knows $b
        $b knows $c
    }
    return { $a.name, $c.name }
}

query hop3() {
    match {
        $a: Person
        $b: Person
        $c: Person
        $d: Person
        $a knows $b
        $b knows $c
        $c knows $d
    }
    return { $a.name, $d.name }
}
"#;

/// Generate a synthetic graph: `n` persons, each with `avg_degree` outgoing Knows
/// edges to random other persons (seeded, deterministic).
fn generate_jsonl(n: usize, avg_degree: usize, seed: u64) -> String {
    let mut s = String::with_capacity(n * 80);
    for i in 0..n {
        s.push_str(&format!(
            r#"{{"type":"Person","data":{{"name":"p{}","age":{}}}}}"#,
            i,
            (i % 80) as i32 + 18
        ));
        s.push('\n');
    }
    // Simple xorshift-ish PRNG to avoid adding a dep.
    let mut state: u64 = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let next = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    };
    for i in 0..n {
        for _ in 0..avg_degree {
            let j = (next(&mut state) as usize) % n;
            if j == i {
                continue;
            }
            s.push_str(&format!(
                r#"{{"edge":"Knows","from":"p{}","to":"p{}"}}"#,
                i, j
            ));
            s.push('\n');
        }
    }
    s
}

async fn time_query(
    db: &Omnigraph,
    name: &str,
    runs: usize,
) -> (std::time::Duration, std::time::Duration, usize) {
    let params = ParamMap::new();
    // Warm up once (builds graph index lazily, caches lance datasets).
    let warmup = db
        .query(ReadTarget::branch("main"), QUERIES, name, &params)
        .await
        .expect("warmup query");
    let row_count: usize = warmup.num_rows();

    let mut total = std::time::Duration::ZERO;
    let mut min = std::time::Duration::MAX;
    for _ in 0..runs {
        let t = Instant::now();
        let r = db
            .query(ReadTarget::branch("main"), QUERIES, name, &params)
            .await
            .expect("query");
        let d = t.elapsed();
        std::hint::black_box(r);
        total += d;
        if d < min {
            min = d;
        }
    }
    (total / runs as u32, min, row_count)
}

fn microbench_dedup() {
    // Simulate the per-source BFS dedup inner loop.
    // Inputs: 10k "sources" each expanding to 512 neighbors (3-hop-ish footprint).
    const SOURCES: usize = 10_000;
    const NEIGHBORS_PER_SRC: usize = 512;
    const UNIVERSE: usize = 10_000;

    // Pre-generate dense ids and corresponding strings.
    let mut dense_streams: Vec<Vec<u32>> = Vec::with_capacity(SOURCES);
    let mut string_streams: Vec<Vec<String>> = Vec::with_capacity(SOURCES);
    let mut state: u64 = 0xdeadbeef;
    for _ in 0..SOURCES {
        let mut ds = Vec::with_capacity(NEIGHBORS_PER_SRC);
        for _ in 0..NEIGHBORS_PER_SRC {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ds.push((state as usize % UNIVERSE) as u32);
        }
        let ss: Vec<String> = ds.iter().map(|d| format!("p{}", d)).collect();
        dense_streams.push(ds);
        string_streams.push(ss);
    }

    // Variant A: HashSet<String> (current behavior)
    let t = Instant::now();
    let mut emitted_a: usize = 0;
    for ss in &string_streams {
        let mut seen: HashSet<String> = HashSet::new();
        for s in ss {
            if seen.insert(s.clone()) {
                emitted_a += 1;
            }
        }
    }
    let dur_string = t.elapsed();

    // Variant B: HashSet<u32> (default hasher)
    let t = Instant::now();
    let mut emitted_b: usize = 0;
    for ds in &dense_streams {
        let mut seen: HashSet<u32> = HashSet::new();
        for &d in ds {
            if seen.insert(d) {
                emitted_b += 1;
            }
        }
    }
    let dur_u32 = t.elapsed();

    // Variant C: Vec<bool> bitmap
    let t = Instant::now();
    let mut emitted_d: usize = 0;
    let mut bitmap = vec![false; UNIVERSE];
    for ds in &dense_streams {
        // Reset only the bits we touched
        let mut touched: Vec<u32> = Vec::with_capacity(NEIGHBORS_PER_SRC);
        for &d in ds {
            let idx = d as usize;
            if !bitmap[idx] {
                bitmap[idx] = true;
                touched.push(d);
                emitted_d += 1;
            }
        }
        for &d in &touched {
            bitmap[d as usize] = false;
        }
    }
    let dur_bitmap = t.elapsed();

    println!("\n── Microbench: per-source dedup inner loop ──");
    println!(
        "  {} sources × {} neighbors (universe {})",
        SOURCES, NEIGHBORS_PER_SRC, UNIVERSE
    );
    println!(
        "  HashSet<String>   {:>9.2?}  emitted={}   → {:>6.1} ns/op",
        dur_string,
        emitted_a,
        dur_string.as_nanos() as f64 / (SOURCES * NEIGHBORS_PER_SRC) as f64
    );
    println!(
        "  HashSet<u32>      {:>9.2?}  emitted={}   → {:>6.1} ns/op  (speedup {:.1}×)",
        dur_u32,
        emitted_b,
        dur_u32.as_nanos() as f64 / (SOURCES * NEIGHBORS_PER_SRC) as f64,
        dur_string.as_secs_f64() / dur_u32.as_secs_f64()
    );
    println!(
        "  Vec<bool> bitmap  {:>9.2?}  emitted={}   → {:>6.1} ns/op  (speedup {:.1}×)",
        dur_bitmap,
        emitted_d,
        dur_bitmap.as_nanos() as f64 / (SOURCES * NEIGHBORS_PER_SRC) as f64,
        dur_string.as_secs_f64() / dur_bitmap.as_secs_f64()
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("── End-to-end query latency ──");

    for &(n, avg_deg, label) in &[
        (1_000usize, 4usize, "small (1k nodes, avg_deg=4)"),
        (10_000, 8, "medium (10k nodes, avg_deg=8)"),
        (30_000, 8, "large (30k nodes, avg_deg=8)"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let t = Instant::now();
        let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
        let init_elapsed = t.elapsed();

        let jsonl = generate_jsonl(n, avg_deg, 42);
        let t = Instant::now();
        load_jsonl(&mut db, &jsonl, LoadMode::Overwrite).await.unwrap();
        let load_elapsed = t.elapsed();

        println!(
            "\n[{}]  init={:.2?}  load={:.2?}  jsonl_bytes={}",
            label,
            init_elapsed,
            load_elapsed,
            jsonl.len()
        );

        for q in &["hop1", "hop2", "hop3"] {
            let runs = if *q == "hop3" && n >= 10_000 { 2 } else { 3 };
            let (mean, min, rows) = time_query(&db, q, runs).await;
            println!(
                "  {:<6}  mean={:>9.2?}  min={:>9.2?}  rows={:>7}  runs={}",
                q, mean, min, rows, runs
            );
        }
    }

    microbench_dedup();
}
