//! IMMUTABLE entry point — the single command the agent invokes per trial.
//!
//! Run with:  `cargo run --release --bin run_experiment > run.log 2>&1`
//!
//! Two phases:
//!
//!   PHASE 1 — CORRECTNESS.  For every (shape × input distribution) in the
//!   correctness battery, build the agent kernel and the scalar reference,
//!   compare distance tables (max abs err must be ≤ MAX_ABS_ERR) and top-K
//!   results (must be tie-tolerant equivalent within TOPK_DIST_TOL). Any
//!   single failure → exit 2 ("correctness").
//!
//!   PHASE 2 — SPEED.  For every (shape × data distribution) speed workload,
//!   build the agent kernel once, then time `distance_table + probe_top_k`
//!   for each query. Report per-(shape × distribution) geomean ns, plus
//!   global geomean / worst / best across all timed queries.
//!
//! Output (fixed format the agent can grep):
//!
//!     ---
//!     correctness:           pass | fail
//!     shapes_tested:         (128,16,256) (256,16,256) (768,96,256)
//!     distributions_tested:  clustered uniform sparse
//!     geomean_ns_per_query:  18234
//!     worst_ns_per_query:    24515 (768x96, sparse)
//!     best_ns_per_query:     12876 (128x16, clustered)
//!     peak_mem_mb:           28.4
//!     total_seconds:         12.3
//!
//! Exit codes:
//!   0  — both phases passed within time budget.
//!   2  — correctness failure (agent kernel disagrees with reference).
//!   3  — total wall-clock exceeded budget.
//!   1  — any other error.

use std::time::Instant;

use harness_common::{MAX_ABS_ERR, TIME_BUDGET_SECS, TOPK_DIST_TOL, geomean, peak_rss_mb};
use pq_l2::inputs::{
    DISTRIBUTIONS, DataDistribution, SHAPES, SpeedWorkload, correctness_battery, speed_workloads,
};
use pq_l2::kernels::PqKernel;
use pq_l2::reference::{ScalarReference, max_abs_err, topk_consistent};
use pq_l2::PqShape;

// Any constants; the only requirement is that they're pinned across trials so
// the inputs and the timings are reproducible.
const CORRECTNESS_SEED: u64 = 0xC0FF_EEC0_DEBE_EFFE;
const SPEED_SEED: u64 = 0x5EED_F1AC_BABE_FACE;

fn main() {
    let start = Instant::now();

    if let Err(e) = run_correctness() {
        eprintln!("---");
        eprintln!("correctness:           fail");
        eprintln!("first_failure:         {e}");
        eprintln!("total_seconds:         {:.2}", start.elapsed().as_secs_f64());
        std::process::exit(2);
    }
    println!("correctness:           pass");

    let workloads = speed_workloads(SPEED_SEED);
    let report = run_speed(&workloads);

    let elapsed = start.elapsed();
    let mem_mb = peak_rss_mb();

    println!("---");
    println!("correctness:           pass");
    println!(
        "shapes_tested:         {}",
        SHAPES
            .iter()
            .map(format_shape)
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!(
        "distributions_tested:  {}",
        DISTRIBUTIONS
            .iter()
            .map(format_dist)
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!("geomean_ns_per_query:  {}", report.geomean_ns);
    println!(
        "worst_ns_per_query:    {} ({}, {})",
        report.worst_ns,
        format_shape(&report.worst_shape),
        format_dist(&report.worst_dist)
    );
    println!(
        "best_ns_per_query:     {} ({}, {})",
        report.best_ns,
        format_shape(&report.best_shape),
        format_dist(&report.best_dist)
    );
    println!("per_combo_geomean_ns:");
    for combo in &report.per_combo {
        println!(
            "  {} {:<10} -> {} ns",
            format_shape(&combo.shape),
            format_dist(&combo.dist),
            combo.geomean_ns
        );
    }
    println!("peak_mem_mb:           {mem_mb:.1}");
    println!("total_seconds:         {:.2}", elapsed.as_secs_f64());

    if elapsed.as_secs() > TIME_BUDGET_SECS {
        eprintln!(
            "FAIL: total wall-clock {}s exceeds budget {}s",
            elapsed.as_secs(),
            TIME_BUDGET_SECS
        );
        std::process::exit(3);
    }
}

fn run_correctness() -> Result<(), String> {
    let cases = correctness_battery(CORRECTNESS_SEED);
    for case in &cases {
        let agent = PqKernel::new(case.shape, &case.codebook);
        let reference = ScalarReference::new(case.shape, &case.codebook);

        let agent_table = agent.distance_table(&case.query);
        let ref_table = reference.distance_table(&case.query);
        let table_err = max_abs_err(&agent_table, &ref_table);
        if table_err > MAX_ABS_ERR {
            return Err(format!(
                "case={}/{} distance_table max_abs_err={table_err} > {MAX_ABS_ERR}",
                format_shape(&case.shape),
                case.label
            ));
        }

        let agent_topk = agent.probe_top_k(&agent_table, &case.codes, case.num_vectors, case.k);
        let ref_topk = reference.probe_top_k(&ref_table, &case.codes, case.num_vectors, case.k);
        if let Err(diag) = topk_consistent(&agent_topk, &ref_topk, TOPK_DIST_TOL) {
            return Err(format!(
                "case={}/{} top_k disagreement: {diag}",
                format_shape(&case.shape),
                case.label
            ));
        }
    }
    Ok(())
}

struct ComboReport {
    shape: PqShape,
    dist: DataDistribution,
    geomean_ns: u64,
}

struct SpeedReport {
    geomean_ns: u64,
    worst_ns: u64,
    worst_shape: PqShape,
    worst_dist: DataDistribution,
    best_ns: u64,
    best_shape: PqShape,
    best_dist: DataDistribution,
    per_combo: Vec<ComboReport>,
}

fn run_speed(workloads: &[SpeedWorkload]) -> SpeedReport {
    let mut all_timings: Vec<u64> = Vec::new();
    let mut per_combo: Vec<ComboReport> = Vec::new();

    let mut worst = (0u64, SHAPES[0], DISTRIBUTIONS[0]);
    let mut best = (u64::MAX, SHAPES[0], DISTRIBUTIONS[0]);

    for wl in workloads {
        let kernel = PqKernel::new(wl.shape, &wl.codebook);
        let mut combo_timings: Vec<u64> = Vec::with_capacity(wl.num_queries);
        for qi in 0..wl.num_queries {
            let q = &wl.queries[qi * wl.shape.dim..(qi + 1) * wl.shape.dim];
            let t0 = Instant::now();
            let table = kernel.distance_table(q);
            let _hits = kernel.probe_top_k(&table, &wl.codes, wl.num_vectors, wl.k);
            combo_timings.push(t0.elapsed().as_nanos() as u64);
        }
        let combo_geo = geomean(&combo_timings);
        per_combo.push(ComboReport {
            shape: wl.shape,
            dist: wl.distribution,
            geomean_ns: combo_geo,
        });
        if combo_geo > worst.0 {
            worst = (combo_geo, wl.shape, wl.distribution);
        }
        if combo_geo < best.0 {
            best = (combo_geo, wl.shape, wl.distribution);
        }
        all_timings.extend(combo_timings);
    }

    SpeedReport {
        geomean_ns: geomean(&all_timings),
        worst_ns: worst.0,
        worst_shape: worst.1,
        worst_dist: worst.2,
        best_ns: best.0,
        best_shape: best.1,
        best_dist: best.2,
        per_combo,
    }
}

fn format_shape(s: &PqShape) -> String {
    format!("({},{},{})", s.dim, s.num_sub_vectors, s.num_centroids)
}

fn format_dist(d: &DataDistribution) -> String {
    match d {
        DataDistribution::Clustered => "clustered",
        DataDistribution::Uniform => "uniform",
        DataDistribution::Sparse => "sparse",
    }
    .to_string()
}
