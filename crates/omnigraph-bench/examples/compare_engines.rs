//! Diagnostic warm-query comparison; JSON timings are evidence, never assertions.
#![recursion_limit = "512"]

use std::collections::HashMap;
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::result::QueryResult;
use serde_json::json;

const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I64
    team: String
    bio: String
    embedding: Vector(32)
}
edge Knows: Person -> Person
"#;

const QUERIES: &str = r#"
query scan() { match { $p: Person } return { $p.name } }
query wide_scan() { match { $p: Person } return { $p } }
query filter() { match { $p: Person $p.age >= 90 } return { $p.name, $p.age } }
query lookup() { match { $p: Person { name: "p000500" } } return { $p.name, $p.age } }
query count_all() { match { $p: Person } return { count($p) as n } }
query grouped() {
    match { $p: Person }
    return { $p.team, sum($p.age) as total, avg($p.age) as mean, count($p) as n }
}
query top_people() {
    match { $p: Person }
    return { $p.name, $p.age }
    order { $p.age desc, $p.name asc }
    limit 20
}
query friends() { match { $p: Person $p knows $q } return { $p.name, $q.name } }
query filtered_friends() {
    match { $p: Person $p.age >= 90 $p knows $q }
    return { $p.name, $q.name }
}
query no_friends() { match { $p: Person not { $p knows $q } } return { $p.name } }
"#;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 10_000)]
    rows: usize,
    #[arg(long, default_value_t = 20)]
    iterations: usize,
    #[arg(long, default_value_t = 5)]
    warmups: usize,
}

fn canonical(result: &QueryResult, ordered: bool) -> Result<Vec<String>, Box<dyn Error>> {
    let mut rows: Vec<String> = result
        .to_rust_json()?
        .as_array()
        .ok_or("query result is not an array")?
        .iter()
        .map(serde_json::Value::to_string)
        .collect();
    if !ordered {
        rows.sort();
    }
    Ok(rows)
}

async fn query(session: &Session, name: &str) -> Result<QueryResult, Box<dyn Error>> {
    Ok(tokio::time::timeout(
        Duration::from_secs(60),
        session.query(ReadTarget::branch("main"), QUERIES, name, &HashMap::new()),
    )
    .await??)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    if cfg!(debug_assertions) {
        return Err("use an optimized release build for this diagnostic".into());
    }
    if env!("OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT") != "false" {
        return Err(
            "rebuild with RUSTFLAGS=; encoded Rust flags are not a qualified diagnostic build"
                .into(),
        );
    }
    let args = Args::parse();
    if args.rows < 1_000 || args.rows > 100_000 || args.rows % 100 != 0 {
        return Err("rows must be a multiple of 100 in 1000..=100000".into());
    }
    if args.iterations < 6 || args.iterations % 2 != 0 || args.warmups < 2 {
        return Err("use an even number of at least six measured pairs and two warmups".into());
    }
    let directory = tempfile::tempdir()?;
    let setup = Session::from_defaults(
        Arc::new(Omnigraph::init(directory.path().to_str().ok_or("non-UTF8 path")?, SCHEMA).await?),
        SessionSettings::default(),
    );
    let bio = "synthetic person biography ".repeat(40);
    for start in (0..args.rows).step_by(1_000) {
        let mut lines = Vec::new();
        for i in start..(start + 1_000).min(args.rows) {
            lines.push(
                json!({"type": "Person", "data": {
                    "name": format!("p{i:06}"), "age": i % 100, "team": format!("g{}", i % 10),
                    "bio": bio, "embedding": vec![(i % 97) as f32 / 97.0; 32]
                }})
                .to_string(),
            );
        }
        setup
            .load_jsonl(&lines.join("\n"), LoadMode::Append)
            .await?;
    }
    for start in (0..args.rows).step_by(2_000) {
        let lines: Vec<String> = (start..(start + 2_000).min(args.rows))
            .step_by(2)
            .map(|i| {
                json!({"edge": "Knows", "from": format!("p{i:06}"), "to": format!("p{:06}", i + 1)})
                    .to_string()
            })
            .collect();
        setup
            .load_jsonl(&lines.join("\n"), LoadMode::Append)
            .await?;
    }
    setup.ensure_indices().await?;
    let sessions = [
        Session::from_defaults(
            Arc::clone(setup.db()),
            setup.settings().clone().with("engine", "v1")?,
        ),
        Session::from_defaults(
            Arc::clone(setup.db()),
            setup.settings().clone().with("engine", "v2")?,
        ),
    ];
    let cases = [
        ("scan", args.rows),
        ("wide_scan", args.rows),
        ("filter", args.rows / 10),
        ("lookup", 1),
        ("count_all", 1),
        ("grouped", 10),
        ("top_people", 20),
        ("friends", args.rows / 2),
        ("filtered_friends", args.rows / 20),
        ("no_friends", args.rows / 2),
    ];
    let mut expected = Vec::new();
    for (name, rows) in &cases {
        let result = query(&sessions[0], name).await?;
        assert_eq!(result.num_rows(), *rows, "non-vacuous {name}");
        if *name == "count_all" {
            assert_eq!(result.to_rust_json()?, json!([{"n": args.rows}]));
        }
        let other = query(&sessions[1], name).await?;
        let fields = |result: &QueryResult| {
            result
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(fields(&result), fields(&other), "result types: {name}");
        let reference = canonical(&result, *name == "top_people")?;
        assert_eq!(
            reference,
            canonical(&other, *name == "top_people")?,
            "result parity: {name}"
        );
        expected.push(reference);
    }
    println!(
        "{}",
        json!({"kind": "fixture", "rows": args.rows,
        "head": env!("OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT"),
        "worktree_dirty": env!("OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY"),
        "rustc": env!("OMNIGRAPH_BENCH_RUSTC_VERSION"), "edges": args.rows / 2,
        "bio_bytes": bio.len(), "vector_dimensions": 32, "iterations": args.iterations,
        "warmups": args.warmups, "tokio_workers": 4, "cache": "shared-handle warm; OS page cache uncontrolled",
        "boundary": "Session::query through materialized QueryResult; excludes JSON conversion and verification",
        "claim_eligible": false, "durable_record": false})
    );
    for round in 0..args.warmups + args.iterations {
        for offset in 0..cases.len() {
            let index = (round + offset) % cases.len();
            let (name, _) = cases[index];
            for engine in [(round + index) % 2, 1 - (round + index) % 2] {
                let start = Instant::now();
                let result = query(&sessions[engine], name).await?;
                let elapsed = start.elapsed();
                black_box(&result);
                assert_eq!(
                    canonical(&result, name == "top_people")?,
                    expected[index],
                    "sample parity: {name}"
                );
                if round >= args.warmups {
                    println!(
                        "{}",
                        json!({"kind": "sample", "rows": args.rows, "query": name,
                        "engine": if engine == 0 { "v1" } else { "v2" }, "pair": round - args.warmups,
                        "elapsed_ns": elapsed.as_nanos(), "result_rows": result.num_rows()})
                    );
                }
            }
        }
    }
    Ok(())
}
