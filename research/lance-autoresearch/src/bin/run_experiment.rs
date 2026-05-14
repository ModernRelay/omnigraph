//! IMMUTABLE entry point — the single command the agent invokes per trial.
//!
//! Run with:  `cargo run --release --bin run_experiment > run.log 2>&1`
//!
//! Loads (or synthesizes) the fixture, calls the kernels in `src/kernels.rs`,
//! and prints a fixed-format result block the agent can grep:
//!
//!     ---
//!     source:               sift1m | synthetic
//!     num_base:             1000000
//!     num_queries:          1000
//!     recall_at_10:         0.9421
//!     geomean_ns_per_query: 184273
//!     peak_mem_mb:          42.1
//!     total_seconds:        21.7
//!
//! Exit codes:
//!   0  — ran to completion, recall above floor, within time budget.
//!   2  — recall below floor (kernel is broken).
//!   3  — total wall-clock exceeded budget.
//!   1  — any other error.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::Result;

use lance_autoresearch::fixture::Fixture;
use lance_autoresearch::kernels::{TopKHeap, compute_distance_table_l2, probe_pq_l2_top_k};
use lance_autoresearch::{DIM, TOP_K};

const MAX_QUERIES_BENCHED: usize = 1000;
const TIME_BUDGET_SECS: u64 = 600;
const RECALL_FLOOR: f32 = 0.50;

fn main() {
    match real_main() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(1);
        }
    }
}

fn real_main() -> Result<()> {
    let start = Instant::now();
    let fix = Fixture::load_or_synthesize()?;

    let n_q = MAX_QUERIES_BENCHED.min(fix.num_query);
    let mut hits = 0usize;
    let mut total_relevant = 0usize;
    let mut per_query_ns: Vec<u64> = Vec::with_capacity(n_q);

    for qi in 0..n_q {
        let q = &fix.query_vectors[qi * DIM..(qi + 1) * DIM];

        let t0 = Instant::now();
        let table = compute_distance_table_l2(q, &fix.codebook);
        let mut heap = TopKHeap::new();
        probe_pq_l2_top_k(&table, &fix.codes, fix.num_base, &mut heap);
        per_query_ns.push(t0.elapsed().as_nanos() as u64);

        let candidates: Vec<u32> = heap.into_sorted().into_iter().map(|(id, _)| id).collect();
        let truth_slice =
            &fix.groundtruth[qi * fix.top_k_truth..qi * fix.top_k_truth + TOP_K.min(fix.top_k_truth)];
        let truth_set: HashSet<u32> = truth_slice.iter().copied().collect();
        for c in &candidates {
            if truth_set.contains(c) {
                hits += 1;
            }
        }
        total_relevant += TOP_K;
    }

    let recall = hits as f32 / total_relevant as f32;
    let geomean_ns = geomean(&per_query_ns);
    let elapsed = start.elapsed();
    let mem_mb = peak_rss_mb();

    println!("---");
    println!("source:               {}", fix.source_str());
    println!("num_base:             {}", fix.num_base);
    println!("num_queries:          {n_q}");
    println!("recall_at_10:         {recall:.4}");
    println!("geomean_ns_per_query: {geomean_ns}");
    println!("peak_mem_mb:          {mem_mb:.1}");
    println!("total_seconds:        {:.2}", elapsed.as_secs_f64());

    if recall < RECALL_FLOOR {
        eprintln!("FAIL: recall@10 {recall:.4} below floor {RECALL_FLOOR:.4}");
        std::process::exit(2);
    }
    if elapsed.as_secs() > TIME_BUDGET_SECS {
        eprintln!(
            "FAIL: total wall-clock {}s exceeds budget {}s",
            elapsed.as_secs(),
            TIME_BUDGET_SECS
        );
        std::process::exit(3);
    }

    Ok(())
}

fn geomean(xs: &[u64]) -> u64 {
    if xs.is_empty() {
        return 0;
    }
    let mut sum_ln = 0.0f64;
    for &x in xs {
        sum_ln += (x.max(1) as f64).ln();
    }
    (sum_ln / xs.len() as f64).exp() as u64
}

#[cfg(target_os = "linux")]
fn peak_rss_mb() -> f64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
        return 0.0;
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmPeak:") {
            let kb: f64 = rest
                .split_whitespace()
                .next()
                .and_then(|t| t.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_mb() -> f64 {
    0.0
}
