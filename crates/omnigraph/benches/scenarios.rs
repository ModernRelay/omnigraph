//! Scenario benchmark harness — a decision instrument, not a CI gate.
//!
//! Each scenario is ONE cold, stateful, multi-second macro-run (a branch
//! merge, a filtered vector search) executed in a fresh subprocess and
//! instrumented for wall-clock, peak RSS, and scenario-specific metrics.
//! Results are JSON lines on stdout; there are no assertions and this target
//! is never part of `cargo test --workspace` or any CI gate. Criterion is
//! deliberately not used: statistics over many warm in-process iterations is
//! the wrong model for these workloads (cold-vs-warm is the whole game, the
//! primary metric is memory, and an OOM under a cap is a *data point* that
//! needs crash isolation, not a bench failure).
//!
//! Run:
//!   cargo bench -p omnigraph-engine --bench scenarios -- \
//!     --scenario merge-all-changed --rows 20000 --dims 256
//!   cargo bench -p omnigraph-engine --bench scenarios -- \
//!     --scenario nearest-prefilter --rows 100000 --dims 64 --selectivity 0.05
//!
//! Mechanism: the parent re-invokes `current_exe()` with `--child` per run,
//! reaps it with `libc::wait4`, and reads `rusage.ru_maxrss` — the kernel's
//! exact per-child peak RSS, no sampling. `--memory-cap-mb` applies
//! `setrlimit(RLIMIT_AS)` in the child (reliable on Linux; macOS often
//! ignores RLIMIT_AS — the cap variant is primarily a Linux tool, while
//! peak-RSS reporting works everywhere).

// The harness is Unix-only (wait4/rusage/setrlimit); a Windows host gets an
// inert stub so `cargo bench`/`cargo build --benches` still compile there.
#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

#[path = "../tests/helpers/mod.rs"]
#[cfg(unix)]
mod helpers;

use std::fmt::Write as _;
use std::io::Read as _;
use std::time::Instant;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[derive(Debug, Clone)]
struct Args {
    scenario: String,
    rows: usize,
    dims: usize,
    seed: u64,
    runs: usize,
    /// Selectivity for nearest-prefilter: fraction of rows matching the filter.
    selectivity: f64,
    /// ANN k (the query's `limit`) for nearest-prefilter.
    k: usize,
    memory_cap_mb: Option<u64>,
    /// Results-log override; see `results_path`.
    out: Option<String>,
    /// Run the identical workload but SKIP the measured operation; the
    /// peak-RSS delta between a normal run and a baseline run isolates the
    /// measured op's own memory contribution (ru_maxrss spans the whole
    /// child, seeding included).
    baseline: bool,
    child: bool,
}

#[cfg(unix)]
impl Args {
    fn parse() -> Self {
        let mut args = Args {
            scenario: String::new(),
            rows: 20_000,
            dims: 256,
            seed: 42,
            runs: 1,
            selectivity: 0.05,
            k: 10,
            memory_cap_mb: None,
            out: None,
            baseline: false,
            child: false,
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut take = |name: &str| {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {name}"))
            };
            match arg.as_str() {
                "--scenario" => args.scenario = take("--scenario"),
                "--rows" => args.rows = take("--rows").parse().expect("--rows"),
                "--dims" => args.dims = take("--dims").parse().expect("--dims"),
                "--seed" => args.seed = take("--seed").parse().expect("--seed"),
                "--runs" => args.runs = take("--runs").parse().expect("--runs"),
                "--selectivity" => {
                    args.selectivity = take("--selectivity").parse().expect("--selectivity")
                }
                "--k" => args.k = take("--k").parse().expect("--k"),
                "--out" => args.out = Some(take("--out")),
                "--memory-cap-mb" => {
                    args.memory_cap_mb = Some(take("--memory-cap-mb").parse().expect("cap"))
                }
                "--baseline" => args.baseline = true,
                "--child" => args.child = true,
                // `cargo bench` appends `--bench`; tolerate any unknown flag so
                // the harness composes with cargo's own argument plumbing.
                _ => {}
            }
        }
        args
    }

    fn to_child_argv(&self) -> Vec<String> {
        let mut v = vec![
            "--scenario".into(),
            self.scenario.clone(),
            "--rows".into(),
            self.rows.to_string(),
            "--dims".into(),
            self.dims.to_string(),
            "--seed".into(),
            self.seed.to_string(),
            "--selectivity".into(),
            self.selectivity.to_string(),
            "--k".into(),
            self.k.to_string(),
            "--child".into(),
        ];
        if self.baseline {
            v.push("--baseline".into());
        }
        if let Some(cap) = self.memory_cap_mb {
            v.push("--memory-cap-mb".into());
            v.push(cap.to_string());
        }
        v
    }
}

// ---------------------------------------------------------------------------
// Parent: spawn child, reap with wait4, merge rusage into the JSON record
// ---------------------------------------------------------------------------

#[cfg(not(unix))]
fn main() {
    eprintln!("the scenario harness requires a Unix platform (wait4/rusage/setrlimit)");
}

#[cfg(unix)]
fn main() {
    let args = Args::parse();
    if args.scenario.is_empty() {
        eprintln!(
            "usage: --scenario <merge-all-changed|nearest-prefilter> [--rows N] [--dims D] \
             [--seed S] [--runs K] [--selectivity F] [--k K] [--memory-cap-mb M]"
        );
        // `cargo bench` with no args must exit 0 so the target stays inert in
        // any blanket `cargo bench` invocation.
        return;
    }
    if args.child {
        run_child(&args);
        return;
    }
    for run in 0..args.runs {
        let record = run_once(&args, run);
        println!("{record}");
        append_result(&args, &record);
    }
}

#[cfg(unix)]
/// Where run records accumulate: `--out <path>`, else `OMNIGRAPH_BENCH_RESULTS`,
/// else `benches/results.jsonl` next to this crate (gitignored — results are
/// host-specific; each record is self-describing via `host` + `params` +
/// `git_sha`). Append-only JSON lines, the harness's system of record.
fn results_path(args: &Args) -> std::path::PathBuf {
    if let Some(ref out) = args.out {
        return out.into();
    }
    if let Ok(env_path) = std::env::var("OMNIGRAPH_BENCH_RESULTS") {
        if !env_path.trim().is_empty() {
            return env_path.trim().into();
        }
    }
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/results.jsonl")
}

fn append_result(args: &Args, record: &serde_json::Value) {
    let path = results_path(args);
    let appended = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| {
            use std::io::Write as _;
            writeln!(f, "{record}")
        });
    if let Err(e) = appended {
        eprintln!("WARNING: could not append to {}: {e}", path.display());
    }
}

fn git_sha() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn run_once(args: &Args, run: usize) -> serde_json::Value {
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(args.to_child_argv())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn child");
    let pid = child.id() as i32;

    // Read stdout to EOF BEFORE reaping — the pipe closes when the child
    // exits, and reading first avoids any pipe-full deadlock.
    let mut child_stdout = String::new();
    child
        .stdout
        .take()
        .expect("child stdout piped")
        .read_to_string(&mut child_stdout)
        .expect("read child stdout");

    let (exit_status, peak_rss_bytes) = wait4_rusage(pid);

    // The child prints exactly one JSON metrics line on success; on a crash
    // (e.g. OOM under --memory-cap-mb) stdout may be empty — record that as
    // the result rather than failing the harness.
    let scenario_metrics: serde_json::Value = child_stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str(l).ok())
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "scenario": args.scenario,
        "run": run,
        "ts": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        "git_sha": git_sha(),
        "params": {
            "rows": args.rows,
            "dims": args.dims,
            "seed": args.seed,
            "selectivity": args.selectivity,
            "k": args.k,
            "memory_cap_mb": args.memory_cap_mb,
            "baseline": args.baseline,
        },
        "exit_status": exit_status,
        "peak_rss_bytes": peak_rss_bytes,
        "metrics": scenario_metrics,
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        },
    })
}

#[cfg(unix)]
/// Reap `pid` with `wait4` and return (exit code or -signal, peak RSS bytes).
/// `ru_maxrss` is bytes on macOS and KiB on Linux.
fn wait4_rusage(pid: i32) -> (i64, u64) {
    let mut status: libc::c_int = 0;
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    // Retry on EINTR: a delivered signal (SIGTERM from the shell, SIGCHLD
    // from an unrelated child) interrupts the blocking wait with -1.
    let reaped = loop {
        let r = unsafe { libc::wait4(pid, &mut status, 0, &mut rusage) };
        if r != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break r;
        }
    };
    assert_eq!(reaped, pid, "wait4 reaped unexpected pid");
    let exit: i64 = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status) as i64
    } else if libc::WIFSIGNALED(status) {
        -(libc::WTERMSIG(status) as i64)
    } else {
        i64::MIN
    };
    #[cfg(target_os = "macos")]
    let peak = rusage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak = (rusage.ru_maxrss as u64) * 1024;
    (exit, peak)
}

// ---------------------------------------------------------------------------
// Child: apply the cap, build a runtime, run the scenario, print metrics JSON
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn run_child(args: &Args) {
    if let Some(cap_mb) = args.memory_cap_mb {
        let cap = libc::rlimit {
            rlim_cur: cap_mb * 1024 * 1024,
            rlim_max: cap_mb * 1024 * 1024,
        };
        // RLIMIT_AS is enforced on Linux; macOS frequently ignores it. Applied
        // best-effort everywhere so the same command line works on both — but
        // a FAILED setrlimit must be loud (stderr is inherited): the parent's
        // JSON records the requested cap, and silently running uncapped would
        // misrepresent the test conditions.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &cap) } != 0 {
            eprintln!(
                "WARNING: setrlimit(RLIMIT_AS, {cap_mb} MiB) failed ({}); child runs UNCAPPED",
                std::io::Error::last_os_error()
            );
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let metrics = runtime.block_on(async {
        match args.scenario.as_str() {
            "merge-all-changed" => merge_all_changed(args).await,
            "nearest-prefilter" => nearest_prefilter(args).await,
            other => panic!("unknown scenario '{other}'"),
        }
    });
    println!("{metrics}");
}

// ---------------------------------------------------------------------------
// Deterministic vectors (the tests/search.rs mock_embedding pattern, local
// copy — those fns are private to that test binary)
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn fnv1a64(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(unix)]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[cfg(unix)]
/// Unit-norm D-dim vector seeded by (seed, slug). `pole` biases the first
/// component: +1.0 clusters vectors near e1, -1.0 near -e1, 0.0 uniform-ish —
/// the lever the prefilter scenario uses to place matching rows far from the
/// query point.
fn seeded_vector(seed: u64, slug: &str, dims: usize, pole: f32) -> Vec<f32> {
    let mut state = seed ^ fnv1a64(slug);
    if state == 0 {
        state = 0x9e3779b97f4a7c15;
    }
    let mut v: Vec<f32> = (0..dims)
        .map(|_| ((xorshift64(&mut state) >> 11) as f32 / (1u64 << 53) as f32) * 2.0 - 1.0)
        .collect();
    if pole != 0.0 {
        // Dominate the direction with the pole while keeping per-row jitter.
        v[0] = pole * 10.0;
    }
    let norm = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt() as f32;
    if norm > f32::EPSILON {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

#[cfg(unix)]
fn push_vector_json(out: &mut String, v: &[f32]) {
    out.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{x:.8}");
    }
    out.push(']');
}

// ---------------------------------------------------------------------------
// Scenario: merge-all-changed
// ---------------------------------------------------------------------------

#[cfg(unix)]
/// The merge-memory scenario: an embedding-bearing table where a branch
/// changed EVERY row's vector (the re-embed-the-corpus workflow), merged back
/// into main. Measures the changed-delta materialization cost of
/// `branch_merge` (exec/merge.rs concat + hash-join path — the part the
/// fast-forward streaming fix does not cover).
async fn merge_all_changed(args: &Args) -> serde_json::Value {
    const BATCH_ROWS: usize = 500;
    let schema = format!(
        "node Doc {{\n    slug: String @key\n    embedding: Vector({})\n}}\n",
        args.dims
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, &schema).await.expect("init");

    // Seed N rows on main in batches (merge-written fragments, matching the
    // embed workflow's write shape). JSONL strings are per-batch transients.
    let seed_start = Instant::now();
    load_vector_rows(&db, "main", args, BATCH_ROWS, args.seed, 0.0).await;
    let seed_ms = seed_start.elapsed().as_millis() as u64;

    db.branch_create("bench").await.expect("branch_create");

    // Diverge main with one non-conflicting insert so the merge takes the
    // three-way path (publish_rewritten_merge_table) rather than the
    // fast-forward adopt; the measured cost is the changed-delta concat +
    // hash join that path performs.
    {
        let mut jsonl = String::new();
        let slug = "doc-main-diverge";
        let _ = write!(jsonl, r#"{{"type":"Doc","data":{{"slug":"{slug}","embedding":"#);
        push_vector_json(&mut jsonl, &seeded_vector(args.seed ^ 0x5eed, slug, args.dims, 0.0));
        jsonl.push_str("}}
");
        db.load("main", &jsonl, LoadMode::Merge).await.expect("diverge main");
    }

    // Rewrite every row's vector on the branch (same keys, new seed).
    let branch_start = Instant::now();
    load_vector_rows(&db, "bench", args, BATCH_ROWS, args.seed ^ 0xdead_beef, 0.0).await;
    let branch_load_ms = branch_start.elapsed().as_millis() as u64;

    if args.baseline {
        // Identical workload minus the measured op — see Args::baseline.
        return serde_json::json!({
            "seed_ms": seed_ms,
            "branch_load_ms": branch_load_ms,
            "baseline": true,
        });
    }

    // The measured window: the merge alone.
    let merge_start = Instant::now();
    let outcome = db.branch_merge("bench", "main").await.expect("branch_merge");
    let merge_ms = merge_start.elapsed().as_millis() as u64;

    serde_json::json!({
        "seed_ms": seed_ms,
        "branch_load_ms": branch_load_ms,
        "merge_ms": merge_ms,
        "merge_outcome": format!("{outcome:?}"),
        "raw_delta_bytes": (args.rows * args.dims * 4) as u64,
    })
}

#[cfg(unix)]
async fn load_vector_rows(
    db: &Omnigraph,
    branch: &str,
    args: &Args,
    batch_rows: usize,
    seed: u64,
    pole: f32,
) {
    let mut row = 0;
    while row < args.rows {
        let end = (row + batch_rows).min(args.rows);
        let mut jsonl = String::with_capacity(batch_rows * (args.dims * 12 + 64));
        for i in row..end {
            let slug = format!("doc-{i:08}");
            let _ = write!(jsonl, r#"{{"type":"Doc","data":{{"slug":"{slug}","embedding":"#);
            push_vector_json(&mut jsonl, &seeded_vector(seed, &slug, args.dims, pole));
            jsonl.push_str("}}\n");
        }
        db.load(branch, &jsonl, LoadMode::Merge).await.expect("load batch");
        row = end;
    }
}

// ---------------------------------------------------------------------------
// Scenario: nearest-prefilter
// ---------------------------------------------------------------------------

#[cfg(unix)]
/// The filtered-ANN scenario: `selectivity` fraction of rows match
/// `status = "hit"` but sit FAR from the query vector, while all non-matching
/// rows cluster AROUND it — so a post-filtered ANN top-k (the current Lance
/// default; no `prefilter(true)` on the scanner) returns ~0 of the k requested
/// rows even though `rows * selectivity` matches exist. `rows_returned` is the
/// headline metric pre-fix; the same scenario becomes the prefilter latency
/// comparison once the fix lands.
async fn nearest_prefilter(args: &Args) -> serde_json::Value {
    const BATCH_ROWS: usize = 1000;
    const QUERY_ITERS: usize = 20;
    let schema = format!(
        "node Doc {{\n    slug: String @key\n    status: String @index\n    embedding: Vector({}) @index\n}}\n",
        args.dims
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, &schema).await.expect("init");

    // Every ~1/selectivity-th row is a far-from-query "hit"; the rest cluster
    // near the query point (+e1 pole).
    let stride = (1.0 / args.selectivity).round().max(1.0) as usize;
    let seed_start = Instant::now();
    let mut row = 0;
    let mut hit_rows = 0usize;
    while row < args.rows {
        let end = (row + BATCH_ROWS).min(args.rows);
        let mut jsonl = String::with_capacity(BATCH_ROWS * (args.dims * 12 + 96));
        for i in row..end {
            let slug = format!("doc-{i:08}");
            let hit = i % stride == 0;
            if hit {
                hit_rows += 1;
            }
            let (status, pole) = if hit { ("hit", -1.0) } else { ("miss", 1.0) };
            let _ = write!(
                jsonl,
                r#"{{"type":"Doc","data":{{"slug":"{slug}","status":"{status}","embedding":"#
            );
            push_vector_json(&mut jsonl, &seeded_vector(args.seed, &slug, args.dims, pole));
            jsonl.push_str("}}\n");
        }
        db.load("main", &jsonl, LoadMode::Merge).await.expect("load batch");
        row = end;
    }
    let seed_ms = seed_start.elapsed().as_millis() as u64;

    // Fold coverage / materialize any deferred index work.
    let optimize_start = Instant::now();
    db.optimize().await.expect("optimize");
    let optimize_ms = optimize_start.elapsed().as_millis() as u64;

    if args.baseline {
        // Identical workload minus the measured query loop — see
        // Args::baseline (the peak-RSS delta isolates the queries' cost).
        return serde_json::json!({
            "seed_ms": seed_ms,
            "optimize_ms": optimize_ms,
            "hit_rows": hit_rows,
            "baseline": true,
        });
    }

    // Query vector = +e1 (the "miss" cluster's pole): the global ANN top-k is
    // dominated by non-matching rows by construction.
    let mut query_vec = vec![0.0f32; args.dims];
    query_vec[0] = 1.0;
    let query_src = format!(
        "query filtered_nearest($q: Vector({dims})) {{\n    match {{ $d: Doc {{ status: \"hit\" }} }}\n    return {{ $d.slug }}\n    order {{ nearest($d.embedding, $q) }}\n    limit {k}\n}}\n",
        dims = args.dims,
        k = args.k
    );
    let params = helpers::vector_param("q", &query_vec);

    let mut rows_returned = 0usize;
    let mut total_ms = 0u64;
    for i in 0..QUERY_ITERS {
        let q_start = Instant::now();
        let result = db
            .query(ReadTarget::branch("main"), &query_src, "filtered_nearest", &params)
            .await
            .expect("filtered nearest query");
        total_ms += q_start.elapsed().as_millis() as u64;
        let n: usize = result.batches().iter().map(|b| b.num_rows()).sum();
        if i == 0 {
            rows_returned = n;
        }
        std::hint::black_box(n);
    }

    serde_json::json!({
        "seed_ms": seed_ms,
        "optimize_ms": optimize_ms,
        "hit_rows": hit_rows,
        "k": args.k,
        "rows_returned": rows_returned,
        "recall_vs_k": rows_returned as f64 / args.k as f64,
        "query_iters": QUERY_ITERS,
        "mean_query_ms": total_ms as f64 / QUERY_ITERS as f64,
    })
}
