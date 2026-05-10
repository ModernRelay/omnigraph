//! Actor-isolation benchmark for MR-686's `WorkloadController`.
//!
//! The handoff calls this out as the empirical proof of MR-686's central
//! design promise: per-actor admission control isolates noisy actors so a
//! heavy `/ingest` user does not starve light `/change` traffic. The
//! per-`(table, branch)` queue pins the same-key serialization story; this
//! bench pins actor isolation under load.
//!
//! Setup:
//! - One "heavy" actor flooding `/ingest` with multi-row NDJSON bodies.
//! - N "light" actors each running short bursts of `/change` inserts.
//! - Each actor authenticates with its own bearer token so the
//!   `WorkloadController` accounts them as distinct identities.
//!
//! Output: heavy-actor throughput / 429s, light-actor p50 / p95 / p99
//! latency. Acceptance heuristic on local FS: light-actor p99 < 2 s
//! while the heavy actor saturates its own per-actor cap.
//!
//! Usage:
//! ```sh
//! cargo run --release -p omnigraph-server --example bench_actor_isolation -- \
//!     --light-actors 4 --light-ops-per-actor 50 \
//!     --heavy-batches 200 --heavy-rows-per-batch 200 \
//!     --inflight-cap 1 \
//!     --output bench-results/after-pr2-phase2/actor-isolation.json
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use clap::Parser;
use omnigraph::db::Omnigraph;
use omnigraph_server::api::{ChangeRequest, IngestRequest};
use omnigraph_server::workload::WorkloadController;
use omnigraph_server::{AppState, build_app};
use serde::Serialize;
use tower::ServiceExt;

const SCHEMA: &str = "node Person {\n    name: String @key\n    age: I32?\n}\n";

const HEAVY_TOKEN: &str = "heavy-actor-token";
const HEAVY_ACTOR: &str = "act-heavy";

#[derive(Parser, Debug)]
#[command(about = "Actor-isolation HTTP bench for MR-686 WorkloadController")]
struct Args {
    /// Number of light actors driving /change traffic concurrently with the
    /// heavy /ingest flood. Each gets its own bearer token.
    #[arg(long, default_value_t = 4)]
    light_actors: usize,
    /// Number of /change ops per light actor.
    #[arg(long, default_value_t = 50)]
    light_ops_per_actor: usize,
    /// Number of /ingest batches the heavy actor sends.
    #[arg(long, default_value_t = 200)]
    heavy_batches: usize,
    /// NDJSON rows per heavy /ingest batch.
    #[arg(long, default_value_t = 200)]
    heavy_rows_per_batch: usize,
    /// Concurrent in-flight /ingest tasks the heavy actor maintains. With
    /// `inflight_cap` smaller than this, the heavy actor exercises its own
    /// admission cap (and the bench reports `heavy_too_many_requests > 0`),
    /// proving the gate fires without affecting light actors. Default 4
    /// against cap=1 → expect ~3/4 batches rejected.
    #[arg(long, default_value_t = 4)]
    heavy_concurrency: usize,
    /// Per-actor in-flight cap for the run. Passed directly into the
    /// `WorkloadController` constructor (no env-var fiddling). Lower
    /// values surface admission rejections faster.
    #[arg(long, default_value_t = 1)]
    inflight_cap: u32,
    /// Per-actor byte budget (bytes). Default 1 GiB so byte budget
    /// doesn't bottleneck the count gate during normal bench runs.
    #[arg(long, default_value_t = 1_073_741_824)]
    byte_cap: u64,
    /// Output file for the JSON results. Stdout always gets a copy.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Optional label to record alongside results.
    #[arg(long, default_value = "")]
    label: String,
}

#[derive(Serialize, Debug)]
struct BenchResults {
    label: String,
    inflight_cap: u32,
    light_actors: usize,
    light_ops_per_actor: usize,
    heavy_batches: usize,
    heavy_rows_per_batch: usize,
    wall_time_ms: u64,
    heavy_ok: usize,
    heavy_too_many_requests: usize,
    heavy_other_errors: usize,
    heavy_throughput_attempts_per_sec: f64,
    light_ok: usize,
    light_too_many_requests: usize,
    light_other_errors: usize,
    light_p50_ms: f64,
    light_p95_ms: f64,
    light_p99_ms: f64,
    light_p999_ms: f64,
    light_max_ms: f64,
    notes: &'static str,
}

fn build_heavy_body(batch_idx: usize, rows: usize) -> String {
    let mut data = String::new();
    for r in 0..rows {
        data.push_str(&format!(
            "{{\"type\":\"Person\",\"data\":{{\"name\":\"heavy-b{}-r{}\",\"age\":{}}}}}\n",
            batch_idx,
            r,
            r % 100,
        ));
    }
    serde_json::to_string(&IngestRequest {
        data,
        branch: Some("main".to_string()),
        from: Some("main".to_string()),
        mode: Some(omnigraph::loader::LoadMode::Merge),
    })
    .unwrap()
}

async fn send_heavy_batch(app: Router, batch_idx: usize, rows: usize) -> StatusCode {
    let body = build_heavy_body(batch_idx, rows);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/ingest")
        .header("authorization", format!("Bearer {HEAVY_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    match app.oneshot(req).await {
        Ok(r) => r.status(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Drive `batches` /ingest calls from the heavy actor with up to
/// `concurrency` in flight at a time. With `concurrency > inflight_cap`,
/// the heavy actor's own admission permits are exhausted at peak, and
/// some batches return 429. Returns (ok, 429, other) counts.
async fn drive_heavy_actor(
    app: Router,
    batches: usize,
    rows_per_batch: usize,
    concurrency: usize,
) -> (usize, usize, usize) {
    use tokio::sync::Semaphore;

    // Asserted at startup in `main()`; check again here for defense in
    // depth so a future caller can't pass 0 silently.
    assert!(concurrency > 0, "drive_heavy_actor concurrency must be > 0");
    let limiter = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(batches);
    for b in 0..batches {
        let app = app.clone();
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            // Bound concurrency to `concurrency`; this is the bench's
            // own pacing, not the server's admission control. The
            // server's `WorkloadController` is what we're trying to
            // exercise — and it has its own cap (potentially smaller).
            let _permit = limiter.acquire_owned().await.unwrap();
            send_heavy_batch(app, b, rows_per_batch).await
        }));
    }

    let mut ok = 0usize;
    let mut too_many = 0usize;
    let mut other = 0usize;
    for h in handles {
        match h.await.unwrap_or(StatusCode::INTERNAL_SERVER_ERROR) {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS => too_many += 1,
            _ => other += 1,
        }
    }
    (ok, too_many, other)
}

use std::sync::Arc;

async fn drive_light_actor(
    app: Router,
    token: String,
    actor_idx: usize,
    ops: usize,
) -> (Vec<Duration>, usize, usize, usize) {
    let mut latencies = Vec::with_capacity(ops);
    let mut ok = 0usize;
    let mut too_many = 0usize;
    let mut other = 0usize;
    for op_idx in 0..ops {
        let request_body = ChangeRequest {
            query_source: "query insert_person($name: String, $age: I32) {\n    insert Person { name: $name, age: $age }\n}".to_string(),
            query_name: Some("insert_person".to_string()),
            params: Some(serde_json::json!({
                "name": format!("light-{actor_idx}-{op_idx}"),
                "age": op_idx as i32,
            })),
            branch: Some("main".to_string()),
        };
        let body = serde_json::to_vec(&request_body).unwrap();
        let req = Request::builder()
            .method(Method::POST)
            .uri("/change")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let start = Instant::now();
        let response = match app.clone().oneshot(req).await {
            Ok(r) => r,
            Err(_) => {
                other += 1;
                continue;
            }
        };
        let elapsed = start.elapsed();
        match response.status() {
            StatusCode::OK => {
                ok += 1;
                latencies.push(elapsed);
            }
            StatusCode::TOO_MANY_REQUESTS => {
                too_many += 1;
                // Drain to free the body resource.
                let _ = to_bytes(response.into_body(), 16 * 1024).await;
            }
            _ => {
                other += 1;
                let _ = to_bytes(response.into_body(), 16 * 1024).await;
            }
        }
    }
    (latencies, ok, too_many, other)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.light_actors == 0 || args.light_ops_per_actor == 0 || args.heavy_batches == 0 {
        eprintln!("--light-actors, --light-ops-per-actor, --heavy-batches must all be > 0");
        std::process::exit(2);
    }
    if args.heavy_concurrency == 0 {
        eprintln!(
            "--heavy-concurrency must be > 0 (zero would prevent the heavy actor from \
             ever firing a batch; if you want to disable heavy traffic, set --heavy-batches=0)"
        );
        std::process::exit(2);
    }

    let temp = tempfile::tempdir().expect("tempdir");
    let repo = temp.path().join("bench.omni");
    Omnigraph::init(repo.to_str().unwrap(), SCHEMA)
        .await
        .expect("init repo");

    // Build bearer tokens: one for the heavy actor + one per light actor.
    let mut tokens: Vec<(String, String)> =
        vec![(HEAVY_ACTOR.to_string(), HEAVY_TOKEN.to_string())];
    for i in 0..args.light_actors {
        tokens.push((format!("act-light-{i}"), format!("light-token-{i}")));
    }
    let db = Omnigraph::open(repo.to_str().unwrap())
        .await
        .expect("open repo");
    // Construct a custom WorkloadController with the requested caps and
    // pass it through `AppState::new_with_workload`. Avoids the
    // `unsafe { std::env::set_var(...) }` antipattern that violates
    // `setenv`'s thread-safety precondition once the multi-thread tokio
    // runtime is up.
    let workload = WorkloadController::new(args.inflight_cap, args.byte_cap);
    let state = AppState::new_with_workload(
        repo.to_string_lossy().to_string(),
        db,
        tokens,
        workload,
    );
    let app = build_app(state);

    eprintln!(
        "running heavy={}x{} (concurrency={}) light={}x{} cap={}",
        args.heavy_batches,
        args.heavy_rows_per_batch,
        args.heavy_concurrency,
        args.light_actors,
        args.light_ops_per_actor,
        args.inflight_cap,
    );

    let start = Instant::now();
    let heavy_app = app.clone();
    let heavy_concurrency = args.heavy_concurrency;
    let heavy_handle = tokio::spawn(async move {
        drive_heavy_actor(
            heavy_app,
            args.heavy_batches,
            args.heavy_rows_per_batch,
            heavy_concurrency,
        )
        .await
    });

    let mut light_handles = Vec::with_capacity(args.light_actors);
    for actor_idx in 0..args.light_actors {
        let app = app.clone();
        let token = format!("light-token-{actor_idx}");
        let ops = args.light_ops_per_actor;
        light_handles.push(tokio::spawn(async move {
            drive_light_actor(app, token, actor_idx, ops).await
        }));
    }

    let (heavy_ok, heavy_too_many, heavy_other) = heavy_handle.await.expect("heavy task panicked");
    let mut light_latencies: Vec<Duration> =
        Vec::with_capacity(args.light_actors * args.light_ops_per_actor);
    let mut light_ok = 0usize;
    let mut light_too_many = 0usize;
    let mut light_other = 0usize;
    for h in light_handles {
        let (lats, ok, too_many, other) = h.await.expect("light task panicked");
        light_latencies.extend(lats);
        light_ok += ok;
        light_too_many += too_many;
        light_other += other;
    }
    let wall = start.elapsed();

    light_latencies.sort();
    let n = light_latencies.len();
    let pct = |p: f64| -> f64 {
        if n == 0 {
            return 0.0;
        }
        let idx = ((n as f64 - 1.0) * p).round() as usize;
        light_latencies[idx].as_secs_f64() * 1000.0
    };
    let max_ms = light_latencies
        .last()
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    let heavy_throughput = if wall.as_secs_f64() > 0.0 {
        args.heavy_batches as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    let results = BenchResults {
        label: args.label.clone(),
        inflight_cap: args.inflight_cap,
        light_actors: args.light_actors,
        light_ops_per_actor: args.light_ops_per_actor,
        heavy_batches: args.heavy_batches,
        heavy_rows_per_batch: args.heavy_rows_per_batch,
        wall_time_ms: wall.as_millis() as u64,
        heavy_ok,
        heavy_too_many_requests: heavy_too_many,
        heavy_other_errors: heavy_other,
        heavy_throughput_attempts_per_sec: heavy_throughput,
        light_ok,
        light_too_many_requests: light_too_many,
        light_other_errors: light_other,
        light_p50_ms: pct(0.50),
        light_p95_ms: pct(0.95),
        light_p99_ms: pct(0.99),
        light_p999_ms: pct(0.999),
        light_max_ms: max_ms,
        notes: "MR-686 actor-isolation bench. Heavy /ingest + light /change concurrent.",
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
}
