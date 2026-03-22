// Legacy manual performance harness.
// Prefer the Criterion benchmark in `crates/nanograph/benches/query_lookup.rs`.

use std::time::{Duration, Instant};

use nanograph::query::parser::parse_query;
use nanograph::store::database::Database;
use nanograph::{ParamMap, RunResult};
use tempfile::TempDir;

fn schema_source(indexed: bool) -> &'static str {
    if indexed {
        r#"
node Person {
    name: String
    email: String @index
}
"#
    } else {
        r#"
node Person {
    name: String
    email: String
}
"#
    }
}

fn query_source() -> &'static str {
    r#"
query q($email: String) {
    match {
        $p: Person
        $p.email = $email
    }
    return { $p.name }
}
"#
}

fn build_data_source(rows: usize) -> String {
    let mut data = String::with_capacity(rows.saturating_mul(90));
    for i in 0..rows {
        let name = format!("user_{:06}", i);
        let email = format!("{}@example.com", name);
        data.push_str(&format!(
            r#"{{"type":"Person","data":{{"name":"{}","email":"{}"}}}}"#,
            name, email
        ));
        data.push('\n');
    }
    data
}

async fn average_query_latency(
    indexed: bool,
    rows: usize,
    target_email: &str,
    iterations: usize,
) -> Duration {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, schema_source(indexed))
        .await
        .expect("init db");
    let data_source = build_data_source(rows);
    db.load(&data_source).await.expect("load data");
    drop(db);

    let reopened = Database::open(&db_path).await.expect("open db");
    let qf = parse_query(query_source()).expect("parse query");
    let query = &qf.queries[0];
    let mut params = ParamMap::new();
    params.insert(
        "email".to_string(),
        nanograph::query::ast::Literal::String(target_email.to_string()),
    );

    // Warm up scanner, caches, and JIT-like one-time paths.
    for _ in 0..3 {
        let RunResult::Query(result) = reopened
            .run_query(query, &params)
            .await
            .expect("warmup query")
        else {
            panic!("expected query result");
        };
        let batches = result.into_batches();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    let start = Instant::now();
    for _ in 0..iterations {
        let RunResult::Query(result) = reopened
            .run_query(query, &params)
            .await
            .expect("benchmark query")
        else {
            panic!("expected query result");
        };
        let batches = result.into_batches();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
    let total = start.elapsed();
    Duration::from_secs_f64(total.as_secs_f64() / iterations as f64)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "performance harness; run manually with --ignored --nocapture"]
async fn benchmark_indexed_lookup_vs_full_scan_baseline() {
    let rows = std::env::var("NANOGRAPH_PERF_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(75_000);
    let iterations = std::env::var("NANOGRAPH_PERF_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
    let target_idx = rows.saturating_sub(1).min(rows / 2);
    let target_email = format!("user_{:06}@example.com", target_idx);

    let no_index_avg = average_query_latency(false, rows, &target_email, iterations).await;
    let indexed_avg = average_query_latency(true, rows, &target_email, iterations).await;

    let speedup = if indexed_avg.is_zero() {
        f64::INFINITY
    } else {
        no_index_avg.as_secs_f64() / indexed_avg.as_secs_f64()
    };

    println!(
        "index_perf rows={} iters={} no_index_avg_ms={:.3} indexed_avg_ms={:.3} speedup={:.2}x",
        rows,
        iterations,
        no_index_avg.as_secs_f64() * 1000.0,
        indexed_avg.as_secs_f64() * 1000.0,
        speedup
    );

    // Optional guardrail for local regression checks.
    if std::env::var("NANOGRAPH_PERF_ENFORCE").as_deref() == Ok("1") {
        let min_speedup = std::env::var("NANOGRAPH_PERF_MIN_SPEEDUP")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.05);
        assert!(
            speedup >= min_speedup,
            "indexed lookup speedup {:.2}x below minimum {:.2}x (rows={}, iters={})",
            speedup,
            min_speedup,
            rows,
            iterations
        );
    }
}
