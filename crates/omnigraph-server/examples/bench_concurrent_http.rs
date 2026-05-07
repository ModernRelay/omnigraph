//! Server-level concurrent HTTP benchmark for MR-686 (PR 0 baseline).
//!
//! Drives concurrent `/change` requests against an in-process Omnigraph HTTP
//! server. Measures the global `Arc<RwLock<Omnigraph>>` lock penalty on
//! current `main` so PR 1 + PR 2 can be evaluated against a real baseline.
//!
//! Per the MR-686 plan: this is the load-bearing bench. `Omnigraph::mutate_as`
//! is `&mut self`, so an engine-level concurrent bench either serializes on the
//! borrow checker (measures nothing) or drives multiple handles (measures Lance
//! contention, not the server bottleneck). Driving the HTTP server is the only
//! way to measure the actual `RwLock<Omnigraph>` contention this work removes.
//!
//! Usage:
//! ```sh
//! cargo run --release -p omnigraph-server --example bench_concurrent_http -- \
//!     --tables 16 --actors 16 --ops-per-actor 1000 --mode disjoint \
//!     --output bench-results/baseline-main/cross-table.json
//! ```
//!
//! Modes:
//! - `disjoint`: each actor writes to a distinct node type (cross-table fanout)
//! - `same-key`: all actors write to the same node type (hot-key contention)
//! - `mixed`: each actor writes to a different table per op (round-robin)

use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use clap::{Parser, ValueEnum};
use omnigraph::db::Omnigraph;
use omnigraph_server::api::ChangeRequest;
use omnigraph_server::{AppState, build_app};
use serde::Serialize;
use tower::ServiceExt;

#[derive(Parser, Debug)]
#[command(about = "Concurrent HTTP bench for MR-686")]
struct Args {
    /// Number of distinct node types in the schema.
    #[arg(long, default_value_t = 16)]
    tables: usize,
    /// Number of concurrent actors driving requests.
    #[arg(long, default_value_t = 16)]
    actors: usize,
    /// Operations per actor.
    #[arg(long, default_value_t = 100)]
    ops_per_actor: usize,
    /// Workload mode.
    #[arg(long, value_enum, default_value_t = Mode::Disjoint)]
    mode: Mode,
    /// Output file for the JSON results. Stdout always gets a copy.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Optional label to record alongside results (e.g. "baseline-main").
    #[arg(long, default_value = "")]
    label: String,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    Disjoint,
    SameKey,
    Mixed,
}

#[derive(Serialize, Debug)]
struct BenchResults {
    label: String,
    mode: Mode,
    tables: usize,
    actors: usize,
    ops_per_actor: usize,
    total_ops: usize,
    error_count: usize,
    wall_time_ms: u64,
    throughput_ops_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
    notes: &'static str,
}

fn build_schema(num_tables: usize) -> String {
    let mut schema = String::new();
    for i in 0..num_tables {
        schema.push_str(&format!(
            "node Item{i} {{\n    name: String @key\n    value: I32?\n}}\n\n"
        ));
    }
    schema
}

fn build_query_source(table_idx: usize) -> String {
    format!(
        "query insert_item($name: String, $value: I32) {{\n    insert Item{table_idx} {{ name: $name, value: $value }}\n}}"
    )
}

fn pick_table(actor_idx: usize, op_idx: usize, mode: Mode, num_tables: usize) -> usize {
    match mode {
        Mode::Disjoint => actor_idx % num_tables,
        Mode::SameKey => 0,
        Mode::Mixed => (actor_idx.wrapping_mul(7919) ^ op_idx) % num_tables,
    }
}

async fn drive_actor(
    app: Router,
    actor_idx: usize,
    ops: usize,
    mode: Mode,
    num_tables: usize,
) -> (Vec<Duration>, usize) {
    let mut latencies = Vec::with_capacity(ops);
    let mut errors = 0usize;
    for op_idx in 0..ops {
        let table_idx = pick_table(actor_idx, op_idx, mode, num_tables);
        let request_body = ChangeRequest {
            query_source: build_query_source(table_idx),
            query_name: Some("insert_item".to_string()),
            params: Some(serde_json::json!({
                "name": format!("a{actor_idx}_o{op_idx}"),
                "value": op_idx as i32,
            })),
            branch: None,
        };
        let body = serde_json::to_vec(&request_body).unwrap();
        let req = Request::builder()
            .method(Method::POST)
            .uri("/change")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let start = Instant::now();
        let response = match app.clone().oneshot(req).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("actor {actor_idx} op {op_idx} transport error: {e:?}");
                errors += 1;
                continue;
            }
        };
        let elapsed = start.elapsed();
        let status = response.status();
        if status != StatusCode::OK {
            errors += 1;
            // Drain body for logging on the first few failures.
            if errors <= 3 {
                let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap_or_default();
                eprintln!(
                    "actor {actor_idx} op {op_idx} status {status} body {}",
                    String::from_utf8_lossy(&body)
                );
            }
        }
        latencies.push(elapsed);
    }
    (latencies, errors)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.tables == 0 || args.actors == 0 || args.ops_per_actor == 0 {
        eprintln!("--tables, --actors, --ops-per-actor must all be > 0");
        std::process::exit(2);
    }

    let temp = tempfile::tempdir().expect("tempdir");
    let repo = temp.path().join("bench.omni");
    let schema = build_schema(args.tables);
    Omnigraph::init(repo.to_str().unwrap(), &schema)
        .await
        .expect("init repo");

    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .expect("open AppState");
    let app = build_app(state);

    eprintln!(
        "running mode={:?} tables={} actors={} ops_per_actor={}",
        args.mode, args.tables, args.actors, args.ops_per_actor
    );

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.actors);
    for actor_idx in 0..args.actors {
        let app = app.clone();
        let mode = args.mode;
        let ops = args.ops_per_actor;
        let num_tables = args.tables;
        handles.push(tokio::spawn(async move {
            drive_actor(app, actor_idx, ops, mode, num_tables).await
        }));
    }

    let mut all_latencies: Vec<Duration> = Vec::with_capacity(args.actors * args.ops_per_actor);
    let mut total_errors = 0usize;
    for h in handles {
        let (lats, errs) = h.await.expect("actor task panicked");
        all_latencies.extend(lats);
        total_errors += errs;
    }
    let wall = start.elapsed();

    all_latencies.sort();
    let n = all_latencies.len();
    let pct = |p: f64| -> f64 {
        if n == 0 {
            return 0.0;
        }
        let idx = ((n as f64 - 1.0) * p).round() as usize;
        all_latencies[idx].as_secs_f64() * 1000.0
    };
    let max_ms = all_latencies
        .last()
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    let throughput = if wall.as_secs_f64() > 0.0 {
        n as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    let results = BenchResults {
        label: args.label.clone(),
        mode: args.mode,
        tables: args.tables,
        actors: args.actors,
        ops_per_actor: args.ops_per_actor,
        total_ops: n,
        error_count: total_errors,
        wall_time_ms: wall.as_millis() as u64,
        throughput_ops_per_sec: throughput,
        p50_ms: pct(0.50),
        p95_ms: pct(0.95),
        p99_ms: pct(0.99),
        p999_ms: pct(0.999),
        max_ms,
        notes: "MR-686 PR 0 baseline. Drives /change via Tower oneshot.",
    };

    let json = serde_json::to_string_pretty(&results).unwrap();
    println!("{json}");

    if let Some(path) = args.output.as_ref() {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).expect("mkdir output parent");
        }
        std::fs::write(path, &json).expect("write output");
        eprintln!("wrote {}", path.display());
    }

    if total_errors > 0 {
        eprintln!("WARN: {total_errors} requests failed");
        std::process::exit(1);
    }
}
