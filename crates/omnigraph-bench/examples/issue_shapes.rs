//! Diagnostic v1/v2 comparison of the four issue shapes; every measurement is a
//! fresh child process whose peak RSS is evidence, never an assertion.
#![recursion_limit = "512"]

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::result::QueryResult;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I64
    team: String
    body: String @index
    bio: String
    embedding: Vector(1024)
}
edge Knows: Person -> Person
"#;

const QUERIES: &str = r#"
query count_bare() { match { $p: Person } return { count($p) as n } }
query destination_projection() { match { $p: Person $p knows $q } return { $q.name } }
query grouped_fanout() {
    match { $p: Person $p knows $q }
    return { $p.team, count($q) as n }
}
query destination_search() {
    match { $p: Person $p knows $q search($q.body, "needle") }
    return { $q.name }
}
"#;

const SHAPES: [(&str, &str); 4] = [
    ("704", "count_bare"),
    ("703", "destination_projection"),
    ("723", "grouped_fanout"),
    ("750", "destination_search"),
];
const ENGINES: [&str; 2] = ["v1", "v2"];
const HUBS: usize = 200;
const FANOUT: usize = 500;
const TEAMS: usize = 10;
const NEEDLE_EVERY: usize = 10;
const VECTOR_DIMENSIONS: usize = 1024;
const TOKIO_WORKERS: usize = 4;
const RAW_FILE: &str = "issue_shapes_raw.jsonl";
const SUMMARY_FILE: &str = "issue_shapes_summary.json";

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 20_000)]
    rows: usize,
    #[arg(long, default_value_t = 4)]
    repeats: usize,
    #[arg(long, default_value_t = 11)]
    iterations: usize,
    #[arg(long, default_value_t = 3)]
    warmups: usize,
    /// Empty store directory for a newly generated fixture.
    #[arg(long)]
    store: Option<PathBuf>,
    /// Directory receiving the raw JSONL and the summary JSON.
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    child: bool,
    #[arg(long)]
    engine: Option<String>,
    #[arg(long)]
    shape: Option<String>,
    #[arg(long, default_value_t = 0)]
    repeat: usize,
}

#[derive(Serialize, Deserialize, Clone)]
struct Expected {
    rows: usize,
    edges: usize,
    groups: usize,
    per_group: usize,
    destination_search: usize,
}

impl Expected {
    fn from_fixture(rows: usize) -> Self {
        let targets = (0..HUBS).flat_map(|hub| (0..FANOUT).map(move |j| (hub * FANOUT + j) % rows));
        Expected {
            rows,
            edges: HUBS * FANOUT,
            groups: TEAMS,
            per_group: HUBS / TEAMS * FANOUT,
            destination_search: targets.filter(|i| i % NEEDLE_EVERY == 0).count(),
        }
    }

    fn rows_for(&self, shape: &str) -> Result<usize, Box<dyn Error>> {
        Ok(match shape {
            "count_bare" => 1,
            "destination_projection" => self.edges,
            "grouped_fanout" => self.groups,
            "destination_search" => self.destination_search,
            other => return Err(format!("unknown shape {other}").into()),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct ChildRecord {
    kind: String,
    engine: String,
    shape: String,
    repeat: usize,
    rows_returned: usize,
    answer: String,
    mismatch: Option<String>,
    median_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    samples: Vec<f64>,
    rss_after_open_bytes: u64,
    peak_rss_bytes: u64,
}

async fn query(session: &Session, name: &str) -> Result<QueryResult, Box<dyn Error>> {
    Ok(tokio::time::timeout(
        Duration::from_secs(60),
        session.query(ReadTarget::branch("main"), QUERIES, name, &HashMap::new()),
    )
    .await??)
}

#[cfg(unix)]
fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let peak = u64::try_from(usage.ru_maxrss)?;
    if cfg!(target_os = "macos") {
        Ok(peak)
    } else {
        Ok(peak.saturating_mul(1024))
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    Err("peak RSS needs getrusage".into())
}

#[cfg(target_os = "macos")]
fn sysctl_bytes(name: &str) -> Option<Vec<u8>> {
    let name = std::ffi::CString::new(name).ok()?;
    let mut length: libc::size_t = 0;
    let null = std::ptr::null_mut();
    if unsafe { libc::sysctlbyname(name.as_ptr(), null, &mut length, null, 0) } != 0 {
        return None;
    }
    let mut buffer = vec![0u8; length];
    let data = buffer.as_mut_ptr().cast::<libc::c_void>();
    if unsafe { libc::sysctlbyname(name.as_ptr(), data, &mut length, null, 0) } != 0 {
        return None;
    }
    buffer.truncate(length);
    Some(buffer)
}

#[cfg(not(target_os = "macos"))]
fn sysctl_bytes(_name: &str) -> Option<Vec<u8>> {
    None
}

fn machine() -> Value {
    let brand = sysctl_bytes("machdep.cpu.brand_string").map(|bytes| {
        String::from_utf8_lossy(&bytes)
            .trim_end_matches('\0')
            .to_string()
    });
    let memsize = sysctl_bytes("hw.memsize").and_then(|bytes| {
        let raw: [u8; 8] = bytes.try_into().ok()?;
        Some(u64::from_ne_bytes(raw))
    });
    json!({
        "cpu": brand,
        "logical_cpus": std::thread::available_parallelism().map(usize::from).ok(),
        "memory_bytes": memsize,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
    })
}

fn sha256_of(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn provenance() -> Result<Value, Box<dyn Error>> {
    Ok(json!({
        "binary_sha256": sha256_of(&std::env::current_exe()?)?,
        "instrument_sha256": format!("{:x}", Sha256::digest(include_str!("issue_shapes.rs").as_bytes())),
        "head": env!("OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT"),
        "worktree_dirty": env!("OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY"),
        "rustc": env!("OMNIGRAPH_BENCH_RUSTC_VERSION"),
        "encoded_rustflags_present": env!("OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT"),
        "machine": machine(),
        "tokio_workers": TOKIO_WORKERS,
        "profile": "release",
        "debug_assertions": cfg!(debug_assertions),
    }))
}

fn canonical(result: &QueryResult) -> Result<Vec<String>, Box<dyn Error>> {
    let mut rows: Vec<_> = result
        .to_rust_json()?
        .as_array()
        .ok_or("query result is not an array")?
        .iter()
        .map(Value::to_string)
        .collect();
    rows.sort();
    Ok(rows)
}

fn expected_rows(shape: &str, expected: &Expected) -> Result<Vec<String>, Box<dyn Error>> {
    let mut rows = match shape {
        "count_bare" => vec![json!({"n": expected.rows}).to_string()],
        "grouped_fanout" => (0..TEAMS)
            .map(|team| json!({"p.team": format!("g{team}"), "n": expected.per_group}).to_string())
            .collect(),
        "destination_projection" | "destination_search" => (0..HUBS)
            .flat_map(|hub| (0..FANOUT).map(move |j| (hub * FANOUT + j) % expected.rows))
            .filter(|target| shape == "destination_projection" || target % NEEDLE_EVERY == 0)
            .map(|target| json!({"q.name": format!("p{target:06}")}).to_string())
            .collect(),
        other => return Err(format!("unknown shape {other}").into()),
    };
    rows.sort();
    Ok(rows)
}

fn check(
    shape: &str,
    result: &QueryResult,
    expected: &Expected,
) -> Result<Option<String>, Box<dyn Error>> {
    let rows = canonical(result)?;
    let want = expected_rows(shape, expected)?;
    Ok((rows != want).then(|| {
        format!(
            "{shape}: complete result differs from fixture oracle ({} rows, expected {})",
            rows.len(),
            want.len()
        )
    }))
}

async fn verify_parity(
    store: &Path,
    expected: &Expected,
) -> Result<(HashSet<String>, Vec<Value>), Box<dyn Error>> {
    let db = Arc::new(Omnigraph::open(store.to_str().ok_or("non-UTF8 path")?).await?);
    let mut excluded = HashSet::new();
    let mut evidence = Vec::new();
    let fields = |result: &QueryResult| {
        result
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect::<Vec<_>>()
    };
    for (_, shape) in SHAPES {
        let mut results = Vec::new();
        for engine in ENGINES {
            let session = Session::from_defaults(
                Arc::clone(&db),
                SessionSettings::default().with("engine", engine)?,
            );
            let result = query(&session, shape).await?;
            let mismatch = check(shape, &result, expected)?;
            if let Some(problem) = &mismatch {
                if engine != "v1" || shape != "destination_search" {
                    return Err(format!("{engine} preflight failed: {problem}").into());
                }
                excluded.insert(format!("{engine}:{shape}"));
            }
            let explain = if engine == "v2" {
                Some(
                    session
                        .explain_query("main", QUERIES, shape, &HashMap::new())
                        .await?,
                )
            } else {
                None
            };
            evidence.push(
                json!({"engine": engine, "shape": shape, "mismatch": mismatch,
                "schema": format!("{:?}", result.schema()), "explain": explain}),
            );
            results.push(result);
        }
        if !excluded.contains(&format!("v1:{shape}"))
            && (fields(&results[0]) != fields(&results[1])
                || canonical(&results[0])? != canonical(&results[1])?)
        {
            return Err(format!("result parity failed before timing: {shape}").into());
        }
    }
    Ok((excluded, evidence))
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let rank = (fraction * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

async fn child(args: &Args) -> Result<(), Box<dyn Error>> {
    let store = args.store.as_deref().ok_or("--child needs --store")?;
    let engine = args.engine.as_deref().ok_or("--child needs --engine")?;
    let shape = args.shape.as_deref().ok_or("--child needs --shape")?;
    let expected: Expected =
        serde_json::from_str(&fs::read_to_string(store.join("expected.json"))?)?;
    let db = Arc::new(Omnigraph::open(store.to_str().ok_or("non-UTF8 path")?).await?);
    let session = Session::from_defaults(db, SessionSettings::default().with("engine", engine)?);
    let rss_after_open_bytes = peak_rss_bytes()?;
    let mut samples = Vec::with_capacity(args.iterations);
    let mut rows_returned = 0;
    for round in 0..args.warmups + args.iterations {
        let start = Instant::now();
        let result = query(&session, shape).await?;
        let elapsed = start.elapsed();
        black_box(&result);
        if let Some(problem) = check(shape, &result, &expected)? {
            return Err(format!("{engine} failed its row check: {problem}").into());
        }
        rows_returned = result.num_rows();
        if round >= args.warmups {
            samples.push(elapsed.as_secs_f64() * 1e3);
        }
    }
    let peak_rss_bytes = peak_rss_bytes()?;
    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    let record = ChildRecord {
        kind: "child".into(),
        engine: engine.into(),
        shape: shape.into(),
        repeat: args.repeat,
        rows_returned,
        answer: "ok".into(),
        mismatch: None,
        median_ms: percentile(&sorted, 0.5),
        p95_ms: percentile(&sorted, 0.95),
        min_ms: sorted[0],
        samples,
        rss_after_open_bytes,
        peak_rss_bytes,
    };
    println!("{}", serde_json::to_string(&record)?);
    Ok(())
}

async fn build_fixture(store: &Path, rows: usize) -> Result<Expected, Box<dyn Error>> {
    let setup = Session::from_defaults(
        Arc::new(Omnigraph::init(store.to_str().ok_or("non-UTF8 path")?, SCHEMA).await?),
        SessionSettings::default(),
    );
    let bio = "synthetic person biography ".repeat(40);
    let filler = "plain filler words about a person and a team ".repeat(4);
    for start in (0..rows).step_by(500) {
        let mut lines = Vec::new();
        for i in start..(start + 500).min(rows) {
            let body = if i % NEEDLE_EVERY == 0 {
                format!("{filler} needle")
            } else {
                filler.clone()
            };
            lines.push(
                json!({"type": "Person", "data": {
                    "name": format!("p{i:06}"), "age": i % 100, "team": format!("g{}", i % TEAMS),
                    "body": body, "bio": bio,
                    "embedding": vec![(i % 97) as f32 / 97.0; VECTOR_DIMENSIONS]
                }})
                .to_string(),
            );
        }
        setup
            .load_jsonl(&lines.join("\n"), LoadMode::Append)
            .await?;
    }
    for hub in 0..HUBS {
        let lines: Vec<String> = (0..FANOUT)
            .map(|j| {
                let target = (hub * FANOUT + j) % rows;
                json!({"edge": "Knows", "from": format!("p{hub:06}"), "to": format!("p{target:06}")})
                    .to_string()
            })
            .collect();
        setup
            .load_jsonl(&lines.join("\n"), LoadMode::Append)
            .await?;
    }
    setup.ensure_indices().await?;
    let expected = Expected::from_fixture(rows);
    fs::write(
        store.join("expected.json"),
        serde_json::to_string_pretty(&expected)?,
    )?;
    Ok(expected)
}

fn run_child(
    store: &Path,
    args: &Args,
    engine: &str,
    shape: &str,
    repeat: usize,
) -> Result<ChildRecord, Box<dyn Error>> {
    let output = Command::new(std::env::current_exe()?)
        .arg("--child")
        .arg("--out")
        .arg(&args.out)
        .arg("--store")
        .arg(store)
        .args(["--engine", engine, "--shape", shape])
        .args(["--repeat", &repeat.to_string()])
        .args(["--warmups", &args.warmups.to_string()])
        .args(["--iterations", &args.iterations.to_string()])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "child {engine} {shape} repeat {repeat} exited {}:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let line = stdout.lines().last().ok_or("child printed nothing")?;
    Ok(serde_json::from_str(line)?)
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    percentile(values, 0.5)
}

fn summarize(
    records: &[ChildRecord],
    expected: &Expected,
    args: &Args,
    metadata: Value,
    parity: Vec<Value>,
    excluded: &HashSet<String>,
) -> Result<Value, Box<dyn Error>> {
    let mib = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    let mut results = Vec::new();
    for (issue, shape) in SHAPES {
        for engine in ENGINES {
            let runs: Vec<&ChildRecord> = records
                .iter()
                .filter(|r| r.shape == shape && r.engine == engine)
                .collect();
            if excluded.contains(&format!("{engine}:{shape}")) {
                results.push(json!({"issue": issue, "shape": shape, "engine": engine,
                    "answer": "wrong", "timing": "excluded: preflight differs from fixture oracle"}));
                continue;
            }
            if runs.is_empty() {
                return Err(format!("no records for {shape} {engine}").into());
            }
            let mut medians: Vec<f64> = runs.iter().map(|r| r.median_ms).collect();
            let mut pooled: Vec<f64> = runs
                .iter()
                .flat_map(|r| r.samples.iter().copied())
                .collect();
            pooled.sort_by(f64::total_cmp);
            let peak = runs.iter().map(|r| r.peak_rss_bytes).max().unwrap_or(0);
            let after_open = runs
                .iter()
                .map(|r| r.rss_after_open_bytes)
                .max()
                .unwrap_or(0);
            let delta = runs
                .iter()
                .map(|r| r.peak_rss_bytes.saturating_sub(r.rss_after_open_bytes))
                .max()
                .unwrap_or(0);
            let answers: Vec<&str> = runs.iter().map(|r| r.answer.as_str()).collect();
            results.push(json!({
                "issue": issue, "shape": shape, "engine": engine,
                "rows_returned": runs[0].rows_returned,
                "expected_rows": expected.rows_for(shape)?,
                "answer": if answers.iter().all(|a| *a == "ok") { "ok" } else { "wrong" },
                "mismatch": runs.iter().find_map(|r| r.mismatch.clone()),
                "median_ms": median(&mut medians),
                "repeat_medians_ms": runs.iter().map(|r| r.median_ms).collect::<Vec<_>>(),
                "p95_ms_pooled": percentile(&pooled, 0.95),
                "min_ms": pooled[0],
                "peak_rss_mib": mib(peak),
                "rss_after_open_mib": mib(after_open),
                "delta_rss_over_open_mib": mib(delta),
                "repeat_peak_rss_mib": runs.iter().map(|r| mib(r.peak_rss_bytes)).collect::<Vec<_>>(),
                "repeat_delta_rss_mib": runs
                    .iter()
                    .map(|r| mib(r.peak_rss_bytes.saturating_sub(r.rss_after_open_bytes)))
                    .collect::<Vec<_>>(),
            }));
        }
    }
    Ok(json!({
        "metadata": metadata,
        "preflight": parity,
        "fixture": {
            "rows": expected.rows, "edges": expected.edges, "hubs": HUBS, "fanout": FANOUT,
            "teams": TEAMS, "needle_every": NEEDLE_EVERY, "vector_dimensions": VECTOR_DIMENSIONS,
            "expected": expected, "repeats": args.repeats, "warmups": args.warmups,
            "iterations": args.iterations,
            "process": "one fresh child per (engine, shape, repeat); engine order alternates per repeat",
            "memory": "ru_maxrss of the child: a process peak, an upper bound including open, warm-up and JSON checks",
            "timing": "Session::query through materialized QueryResult; row checks outside the timer",
            "claim_eligible": false, "durable_record": false,
        },
        "results": results,
    }))
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
    if args.iterations < 5 || args.warmups < 2 {
        return Err("use at least five iterations and two warmups".into());
    }
    if args.child {
        return child(&args).await;
    }
    if args.repeats < 2 || args.repeats % 2 != 0 {
        return Err("repeats must be even and at least two to balance engine order".into());
    }
    let metadata = provenance()?;
    fs::create_dir_all(&args.out)?;
    let mut raw = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.out.join(RAW_FILE))?;
    let mut summary_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.out.join(SUMMARY_FILE))?;
    if args.rows < 1_000 || args.rows > 50_000 || args.rows % 100 != 0 {
        return Err("rows must be a multiple of 100 in 1000..=50000".into());
    }
    let temporary = tempfile::tempdir()?;
    let store = args
        .store
        .clone()
        .unwrap_or_else(|| temporary.path().to_path_buf());
    fs::create_dir_all(&store)?;
    if fs::read_dir(&store)?.next().is_some() {
        return Err("--store must be empty; fixture reuse cannot prove payload identity".into());
    }
    let started = Instant::now();
    let expected = build_fixture(&store, args.rows).await?;
    eprintln!(
        "fixture built in {:.1}s at {}",
        started.elapsed().as_secs_f64(),
        store.display()
    );
    let (excluded, parity) = verify_parity(&store, &expected).await?;
    let mut records = Vec::new();
    for repeat in 0..args.repeats {
        for (_, shape) in SHAPES {
            let order = if repeat % 2 == 0 { [0, 1] } else { [1, 0] };
            for engine in order.map(|index| ENGINES[index]) {
                if excluded.contains(&format!("{engine}:{shape}")) {
                    continue;
                }
                let started = Instant::now();
                let record = run_child(&store, &args, engine, shape, repeat)?;
                eprintln!(
                    "repeat {repeat} {shape:<24} {engine} median {:.3} ms peak {:.1} MiB answer {} ({:.1}s)",
                    record.median_ms,
                    record.peak_rss_bytes as f64 / (1024.0 * 1024.0),
                    record.answer,
                    started.elapsed().as_secs_f64()
                );
                writeln!(raw, "{}", serde_json::to_string(&record)?)?;
                records.push(record);
            }
        }
    }
    let summary = summarize(&records, &expected, &args, metadata, parity, &excluded)?;
    writeln!(summary_file, "{}", serde_json::to_string_pretty(&summary)?)?;
    println!("{}", serde_json::to_string(&summary["results"])?);
    Ok(())
}
