//! Scenario benchmark harness — a decision instrument, not a CI gate.
//!
//! Each scenario is ONE cold, stateful, multi-second macro-run (a branch
//! merge, a filtered vector search, a fenced keyed write) executed in a fresh subprocess and
//! instrumented for wall-clock, peak RSS, and scenario-specific metrics. The
//! RFC-023 all-new adopt scenario is stricter: setup, measured operation, and
//! verification run in three separate subprocesses over one persisted fixture,
//! so setup memory cannot mask the operation's high-water mark.
//! Results are JSON lines on stdout. Scenario-local structural assertions keep
//! a run on its claimed route, but there are no timing/RSS acceptance
//! assertions and this target is never part of `cargo test --workspace` or any
//! CI gate. Criterion is
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
//!   cargo bench -p omnigraph-engine --bench scenarios -- \
//!     --scenario ann-probe-budget --rows 10000 --dims 32 --selectivity 0.01 \
//!     --k 50 --ann-partitions 100 --ann-probes 20
//!   cargo bench -p omnigraph-engine --bench scenarios -- \
//!     --scenario fenced-small-upsert --rows 100000 --dims 256
//!   cargo bench -p omnigraph-engine --bench scenarios -- \
//!     --scenario fenced-adopt-all-new --rows 100000 --dims 256
//!
//! Mechanism: the parent re-invokes `current_exe()` with `--child` per run (or
//! per phase for RFC-023 adopt), reaps it with `libc::wait4`, and reads
//! `rusage.ru_maxrss` — the kernel's exact per-child peak RSS, no sampling.
//! `--memory-cap-mb` is accepted only where enforcement can be verified
//! (currently Linux `RLIMIT_AS`). For phased adopt it applies only to the fresh
//! measured-operation child. An unsupported or failed cap exits that child
//! before opening the fixture and is persisted in the parent record, so an
//! uncapped run cannot masquerade as a capped result. Peak-RSS reporting works
//! on every supported Unix host.

// The harness is Unix-only (wait4/rusage/setrlimit); a Windows host gets an
// inert stub so `cargo bench`/`cargo build --benches` still compile there.
#![cfg_attr(not(unix), allow(dead_code, unused_imports))]
#![recursion_limit = "512"]

#[path = "../tests/helpers/mod.rs"]
#[cfg(unix)]
mod helpers;

#[path = "scenarios/rfc023.rs"]
#[cfg(unix)]
mod rfc023_scenarios;

#[path = "scenarios/rfc023_limits.rs"]
#[cfg(unix)]
mod rfc023_limits;

#[path = "scenarios/child_protocol.rs"]
#[cfg(unix)]
mod child_protocol;

use std::fmt::Write as _;
use std::io::{Read as _, Write as _};
use std::time::Instant;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use sha2::{Digest as _, Sha256};

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
    /// Selectivity for nearest-prefilter (fraction of rows matching the
    /// filter) and for rrf-gate (fraction of ranked rows carrying an edge —
    /// the gate's eligibility ratio).
    selectivity: f64,
    /// ANN k (the query's `limit`) for nearest-prefilter; the rrf `limit`
    /// for rrf-gate.
    k: usize,
    /// IVF partition count for the Lance ANN probe-budget reproduction.
    ann_partitions: usize,
    /// Minimum/maximum probe count used by the bounded comparator.
    ann_probes: usize,
    /// Per-row text payload for rrf-gate: 200 KiB reproduces the issue-#563
    /// overflow-scale corpus, ~2 KiB the wide variant, and a tiny value turns
    /// the same scenario into the in-list build + BTREE probe microbench.
    text_bytes: usize,
    /// How many already-committed rows the source branch MODIFIES, for
    /// `general-merge-updates`. This is the branch delta; `--rows` is the
    /// target size. Holding this small while `--rows` grows is the whole
    /// point of that scenario: it separates delta cost from target cost.
    delta_rows: usize,
    /// `general-merge-updates` source shape: "update" rewrites committed rows,
    /// "insert" adds brand-new rows. Both run against a target that advanced
    /// after the fork, which is what distinguishes this from the adopt
    /// scenario's untouched target.
    source_mode: String,
    memory_cap_mb: Option<u64>,
    /// Results-log override; see `results_path`.
    out: Option<String>,
    /// Select the scenario's comparator. Existing scenarios skip the measured
    /// operation. RFC-023's small-upsert comparator keeps Lance's default
    /// indexed merge route. Each all-new-adopt trial builds the same OmniGraph
    /// init/load/branch fixture in a separate setup child; its comparator
    /// substitutes only the fresh operation child's merge with a clearly
    /// labeled direct Lance streaming Append and is not production-path
    /// evidence. See each scenario's `metrics.routing` and
    /// `metrics.measurement_boundary`.
    baseline: bool,
    child: bool,
    /// Internal phase plumbing for the RFC-023 all-new-adopt scenario. Parent
    /// invocations never set these fields directly.
    phase: Option<String>,
    fixture_root: Option<String>,
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
            ann_partitions: 100,
            ann_probes: 20,
            text_bytes: 2048,
            delta_rows: 50,
            source_mode: "update".to_string(),
            memory_cap_mb: None,
            out: None,
            baseline: false,
            child: false,
            phase: None,
            fixture_root: None,
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
                "--ann-partitions" => {
                    args.ann_partitions =
                        take("--ann-partitions").parse().expect("--ann-partitions")
                }
                "--ann-probes" => {
                    args.ann_probes = take("--ann-probes").parse().expect("--ann-probes")
                }
                "--text-bytes" => {
                    args.text_bytes = take("--text-bytes").parse().expect("--text-bytes")
                }
                "--delta-rows" => {
                    args.delta_rows = take("--delta-rows").parse().expect("--delta-rows")
                }
                "--source-mode" => args.source_mode = take("--source-mode"),
                "--out" => args.out = Some(take("--out")),
                "--memory-cap-mb" => {
                    args.memory_cap_mb = Some(take("--memory-cap-mb").parse().expect("cap"))
                }
                "--baseline" => args.baseline = true,
                "--child" => args.child = true,
                "--phase" => args.phase = Some(take("--phase")),
                "--fixture-root" => args.fixture_root = Some(take("--fixture-root")),
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
            "--ann-partitions".into(),
            self.ann_partitions.to_string(),
            "--ann-probes".into(),
            self.ann_probes.to_string(),
            "--text-bytes".into(),
            self.text_bytes.to_string(),
            "--delta-rows".into(),
            self.delta_rows.to_string(),
            "--source-mode".into(),
            self.source_mode.clone(),
            "--child".into(),
        ];
        if self.baseline {
            v.push("--baseline".into());
        }
        if let Some(cap) = self.memory_cap_mb {
            v.push("--memory-cap-mb".into());
            v.push(cap.to_string());
        }
        if let Some(phase) = &self.phase {
            v.push("--phase".into());
            v.push(phase.clone());
        }
        if let Some(root) = &self.fixture_root {
            v.push("--fixture-root".into());
            v.push(root.clone());
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
            "usage: --scenario <merge-all-changed|nearest-prefilter|ann-probe-budget|fenced-small-upsert|\
             fenced-adopt-all-new|general-merge-updates|rrf-gate> [--rows N] [--dims D] \
             [--seed S] [--runs K] [--selectivity F] [--k K] [--ann-partitions N] \
             [--ann-probes N] [--text-bytes B] [--delta-rows N] \
             [--source-mode update|insert] [--memory-cap-mb M]"
        );
        // `cargo bench` with no args must exit 0 so the target stays inert in
        // any blanket `cargo bench` invocation.
        return;
    }
    if let Err(error) = rfc023_scenarios::validate_args(&args) {
        eprintln!("invalid RFC-023 benchmark shape: {error}");
        std::process::exit(2);
    }
    if args.child {
        run_child(&args);
        return;
    }
    let mut aggregate_exit_status = 0_i64;
    for run in 0..args.runs {
        let record = if matches!(
            args.scenario.as_str(),
            "fenced-adopt-all-new" | "general-merge-updates"
        ) {
            run_phased_adopt_once(&args, run)
        } else {
            run_once(&args, run)
        };
        let run_exit_status = record
            .get("exit_status")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(CHILD_PROTOCOL_EXIT_STATUS);
        if aggregate_exit_status == 0 && run_exit_status != 0 {
            aggregate_exit_status = if (1..=255).contains(&run_exit_status) {
                run_exit_status
            } else {
                1
            };
        }
        println!("{record}");
        append_result(&args, &record);
    }
    if aggregate_exit_status != 0 {
        std::process::exit(aggregate_exit_status as i32);
    }
}

#[cfg(unix)]
/// Where run records accumulate: `--out <path>`, else `OMNIGRAPH_BENCH_RESULTS`,
/// else `benches/results.jsonl` next to this crate (gitignored — results are
/// host-specific; each record is self-describing via `host` + `params` + the
/// Git state and exact benchmark-binary digest). Append-only JSON lines, the
/// harness's system of record.
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
fn git_tree_sha() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "HEAD^{tree}"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[cfg(unix)]
fn git_worktree_dirty() -> Option<bool> {
    let out = std::process::Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    out.status.success().then_some(!out.stdout.is_empty())
}

#[cfg(unix)]
fn benchmark_binary_sha256() -> Option<String> {
    let mut binary = std::fs::File::open(std::env::current_exe().ok()?).ok()?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = binary.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

#[cfg(unix)]
struct ChildRun {
    /// Effective phase status. A malformed child protocol overrides an
    /// otherwise-successful process with `EX_SOFTWARE` so evidence can never
    /// be accepted after silently losing a child record.
    exit_status: i64,
    process_exit_status: i64,
    peak_rss_bytes: u64,
    wall_ms: u64,
    records: Vec<serde_json::Value>,
    protocol_error: Option<String>,
}

#[cfg(unix)]
const CHILD_PROTOCOL_EXIT_STATUS: i64 = 70;

#[cfg(unix)]
fn run_child_process(args: &Args) -> ChildRun {
    let exe = std::env::current_exe().expect("current_exe");
    let wall_start = Instant::now();
    // Reaped by `wait4_rusage(pid)` below, which is used instead of
    // `Child::wait` because only `wait4` reports the child's peak RSS.
    #[allow(clippy::zombie_processes)]
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

    let (process_exit_status, peak_rss_bytes) = wait4_rusage(pid);
    let wall_ms = wall_start.elapsed().as_millis() as u64;

    // The child flushes the cap status before allocating the runtime or any
    // scenario data. Preserve that evidence even when it later crashes (for
    // example, an OOM under a verified cap).
    let parsed = child_protocol::parse_child_records(&child_stdout, process_exit_status);
    let records = parsed.records;
    let protocol_error = parsed.protocol_error;
    let exit_status = if protocol_error.is_some() {
        CHILD_PROTOCOL_EXIT_STATUS
    } else {
        process_exit_status
    };
    ChildRun {
        exit_status,
        process_exit_status,
        peak_rss_bytes,
        wall_ms,
        records,
        protocol_error,
    }
}

#[cfg(unix)]
fn child_record_field(run: &ChildRun, field: &str) -> Option<serde_json::Value> {
    run.records
        .iter()
        .find_map(|record| record.get(field))
        .cloned()
}

#[cfg(unix)]
fn memory_cap_record(args: &Args, run: Option<&ChildRun>) -> serde_json::Value {
    run.and_then(|run| child_record_field(run, "memory_cap_status"))
        .unwrap_or_else(|| {
            serde_json::json!({
                "requested_mb": args.memory_cap_mb,
                "status": "missing_child_status",
                "cap_applied": false,
                "effective_bytes": null,
                "hard_limit_bytes": null,
                "error": "child exited before reporting memory-cap status",
            })
        })
}

#[cfg(unix)]
fn run_once(args: &Args, run: usize) -> serde_json::Value {
    let child = run_child_process(args);
    let memory_cap = memory_cap_record(args, Some(&child));
    let scenario_metrics =
        child_record_field(&child, "scenario_metrics").unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "scenario": args.scenario,
        "run": run,
        "ts": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        "git_sha": git_sha(),
        "git_tree_sha": git_tree_sha(),
        "git_worktree_dirty": git_worktree_dirty(),
        "benchmark_binary_sha256": benchmark_binary_sha256(),
        "params": {
            "rows": args.rows,
            "dims": args.dims,
            "seed": args.seed,
            "selectivity": args.selectivity,
            "k": args.k,
            "ann_partitions": args.ann_partitions,
            "ann_probes": args.ann_probes,
            "text_bytes": args.text_bytes,
            "memory_cap_mb": args.memory_cap_mb,
            "baseline": args.baseline,
        },
        "exit_status": child.exit_status,
        "child_process_exit_status": child.process_exit_status,
        "child_protocol_error": child.protocol_error,
        "wall_ms": child.wall_ms,
        "peak_rss_bytes": child.peak_rss_bytes,
        "memory_cap": memory_cap,
        "metrics": scenario_metrics,
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        },
    })
}

#[cfg(unix)]
fn phased_child_args(args: &Args, phase: &str, fixture_root: &str, apply_cap: bool) -> Args {
    let mut child_args = args.clone();
    child_args.child = true;
    child_args.phase = Some(phase.to_string());
    child_args.fixture_root = Some(fixture_root.to_string());
    if !apply_cap {
        child_args.memory_cap_mb = None;
    }
    child_args
}

#[cfg(unix)]
fn phase_summary(run: Option<&ChildRun>) -> serde_json::Value {
    match run {
        Some(run) => serde_json::json!({
            "status": if run.exit_status == 0 {
                "completed"
            } else if run.exit_status == 78 {
                "refused"
            } else {
                "failed"
            },
            "exit_status": run.exit_status,
            "process_exit_status": run.process_exit_status,
            "protocol_error": run.protocol_error,
            "wall_ms": run.wall_ms,
            "peak_rss_bytes": run.peak_rss_bytes,
        }),
        None => serde_json::json!({
            "status": "skipped",
            "exit_status": null,
            "process_exit_status": null,
            "protocol_error": null,
            "wall_ms": null,
            "peak_rss_bytes": null,
        }),
    }
}

#[cfg(unix)]
fn extend_metrics(target: &mut serde_json::Map<String, serde_json::Value>, run: Option<&ChildRun>) {
    let Some(value) = run.and_then(|run| child_record_field(run, "scenario_metrics")) else {
        return;
    };
    if let Some(object) = value.as_object() {
        target.extend(object.clone());
    }
}

#[cfg(unix)]
fn run_phased_adopt_once(args: &Args, run: usize) -> serde_json::Value {
    let controller_start = Instant::now();
    let fixture = tempfile::tempdir().expect("create persisted RFC-023 benchmark fixture root");
    let fixture_root = fixture
        .path()
        .to_str()
        .expect("UTF-8 RFC-023 benchmark fixture root");

    let setup_args = phased_child_args(args, "setup", fixture_root, false);
    let setup = run_child_process(&setup_args);

    let operation = (setup.exit_status == 0).then(|| {
        let operation_args = phased_child_args(args, "operation", fixture_root, true);
        run_child_process(&operation_args)
    });
    let verify = operation
        .as_ref()
        .filter(|operation| operation.exit_status == 0)
        .map(|_| {
            let verify_args = phased_child_args(args, "verify", fixture_root, false);
            run_child_process(&verify_args)
        });

    let exit_status = if setup.exit_status != 0 {
        setup.exit_status
    } else if let Some(operation) = &operation {
        if operation.exit_status != 0 {
            operation.exit_status
        } else {
            verify.as_ref().map_or(0, |verify| verify.exit_status)
        }
    } else {
        i64::MIN
    };
    let controller_wall_ms = controller_start.elapsed().as_millis() as u64;
    let controller_peak_rss_bytes = current_process_peak_rss_bytes();
    let operation_peak_rss_bytes = operation.as_ref().map_or(0, |run| run.peak_rss_bytes);
    let verify_peak_rss_bytes = verify.as_ref().map(|run| run.peak_rss_bytes);

    let mut metrics = serde_json::Map::new();
    extend_metrics(&mut metrics, Some(&setup));
    extend_metrics(&mut metrics, operation.as_ref());
    extend_metrics(&mut metrics, verify.as_ref());
    metrics.insert(
        "setup_peak_rss_bytes".into(),
        serde_json::json!(setup.peak_rss_bytes),
    );
    metrics.insert(
        "controller_peak_rss_bytes".into(),
        serde_json::json!(controller_peak_rss_bytes),
    );
    metrics.insert(
        "operation_peak_rss_bytes".into(),
        serde_json::json!(operation.as_ref().map(|run| run.peak_rss_bytes)),
    );
    metrics.insert(
        "verify_peak_rss_bytes".into(),
        serde_json::json!(verify_peak_rss_bytes),
    );

    let memory_cap = memory_cap_record(args, operation.as_ref());
    serde_json::json!({
        "scenario": args.scenario,
        "run": run,
        "ts": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        "git_sha": git_sha(),
        "git_tree_sha": git_tree_sha(),
        "git_worktree_dirty": git_worktree_dirty(),
        "benchmark_binary_sha256": benchmark_binary_sha256(),
        "params": {
            "rows": args.rows,
            "dims": args.dims,
            "seed": args.seed,
            "selectivity": args.selectivity,
            "k": args.k,
            "delta_rows": args.delta_rows,
            "source_mode": args.source_mode,
            "memory_cap_mb": args.memory_cap_mb,
            "baseline": args.baseline,
        },
        "exit_status": exit_status,
        "wall_ms": controller_wall_ms,
        // Compatibility field: for phased adopt this is deliberately ONLY
        // the measured-operation child's whole-process wait4 peak.
        "peak_rss_bytes": operation_peak_rss_bytes,
        "setup_peak_rss_bytes": setup.peak_rss_bytes,
        "controller_peak_rss_bytes": controller_peak_rss_bytes,
        "operation_peak_rss_bytes": operation.as_ref().map(|run| run.peak_rss_bytes),
        "verify_peak_rss_bytes": verify_peak_rss_bytes,
        "phases": {
            "setup": phase_summary(Some(&setup)),
            "controller": {
                "status": if exit_status == 0 {
                    "completed"
                } else if exit_status == 78 {
                    "refused"
                } else {
                    "failed"
                },
                "exit_status": exit_status,
                "wall_ms": controller_wall_ms,
                "peak_rss_bytes": controller_peak_rss_bytes,
            },
            "operation": phase_summary(operation.as_ref()),
            "verify": phase_summary(verify.as_ref()),
        },
        "memory_cap": memory_cap,
        "metrics": serde_json::Value::Object(metrics),
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
    (exit, normalized_peak_rss_bytes(&rusage))
}

#[cfg(unix)]
fn normalized_peak_rss_bytes(rusage: &libc::rusage) -> u64 {
    #[cfg(target_os = "macos")]
    let peak = rusage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak = (rusage.ru_maxrss as u64) * 1024;
    peak
}

/// Return this process's current high-water RSS. RFC-023 records it immediately
/// before and after the operation; the parent still owns the authoritative
/// whole-operation-child `wait4` peak.
#[cfg(unix)]
fn current_process_peak_rss_bytes() -> u64 {
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };
    assert_eq!(
        result,
        0,
        "getrusage(RUSAGE_SELF) failed: {}",
        std::io::Error::last_os_error()
    );
    normalized_peak_rss_bytes(&rusage)
}

// ---------------------------------------------------------------------------
// Child: apply the cap, build a runtime, run the scenario, print metrics JSON
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn run_child(args: &Args) {
    let memory_cap_status = apply_memory_cap(args.memory_cap_mb);
    emit_child_record(serde_json::json!({
        "memory_cap_status": memory_cap_status,
    }));
    if args.memory_cap_mb.is_some()
        && !memory_cap_status
            .get("cap_applied")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        eprintln!("requested memory cap was not verifiably applied; refusing to run the scenario");
        std::process::exit(78);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let metrics = runtime.block_on(async {
        match (args.scenario.as_str(), args.phase.as_deref()) {
            ("fenced-adopt-all-new", Some("setup")) => {
                rfc023_scenarios::fenced_adopt_setup(args).await
            }
            ("fenced-adopt-all-new", Some("operation")) => {
                rfc023_scenarios::fenced_adopt_operation(args).await
            }
            ("fenced-adopt-all-new", Some("verify")) => {
                rfc023_scenarios::fenced_adopt_verify(args).await
            }
            ("general-merge-updates", Some("setup")) => {
                rfc023_scenarios::general_merge_setup(args).await
            }
            ("general-merge-updates", Some("operation")) => {
                rfc023_scenarios::general_merge_operation(args).await
            }
            ("general-merge-updates", Some("verify")) => {
                rfc023_scenarios::general_merge_verify(args).await
            }
            ("merge-all-changed", None) => merge_all_changed(args).await,
            ("nearest-prefilter", None) => nearest_prefilter(args).await,
            ("ann-probe-budget", None) => ann_probe_budget(args).await,
            ("rrf-gate", None) => rrf_gate(args).await,
            ("fenced-small-upsert", None) => rfc023_scenarios::fenced_small_upsert(args).await,
            (other, phase) => panic!("unknown scenario/phase '{other}/{phase:?}'"),
        }
    });
    emit_child_record(serde_json::json!({ "scenario_metrics": metrics }));
}

#[cfg(unix)]
fn emit_child_record(record: serde_json::Value) {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{record}").expect("write child benchmark record");
    stdout.flush().expect("flush child benchmark record");
}

#[cfg(unix)]
fn apply_memory_cap(cap_mb: Option<u64>) -> serde_json::Value {
    let Some(cap_mb) = cap_mb else {
        return serde_json::json!({
            "requested_mb": null,
            "status": "not_requested",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": null,
        });
    };
    apply_requested_memory_cap(cap_mb)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn apply_requested_memory_cap(cap_mb: u64) -> serde_json::Value {
    serde_json::json!({
        "requested_mb": cap_mb,
        "status": "unsupported_platform",
        "cap_applied": false,
        "effective_bytes": null,
        "hard_limit_bytes": null,
        "error": format!(
            "verified RLIMIT_AS enforcement is not supported on {}",
            std::env::consts::OS
        ),
    })
}

#[cfg(target_os = "linux")]
fn apply_requested_memory_cap(cap_mb: u64) -> serde_json::Value {
    let Some(requested_bytes) = cap_mb.checked_mul(1024 * 1024) else {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "invalid_requested_cap",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": "requested memory cap overflows bytes",
        });
    };
    // The *64 rlimit API takes u64 limits on every libc; legacy `setrlimit`'s
    // `rlim_t` is u32 on 32-bit glibc, which would need a fallible narrowing.
    let requested = libc::rlimit64 {
        rlim_cur: requested_bytes,
        rlim_max: requested_bytes,
    };
    if unsafe { libc::setrlimit64(libc::RLIMIT_AS, &requested) } != 0 {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "setrlimit_failed",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": std::io::Error::last_os_error().to_string(),
        });
    }

    let mut observed: libc::rlimit64 = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit64(libc::RLIMIT_AS, &mut observed) } != 0 {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "getrlimit_failed",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": std::io::Error::last_os_error().to_string(),
        });
    }
    let applied = observed.rlim_cur == requested_bytes && observed.rlim_max == requested_bytes;
    serde_json::json!({
        "requested_mb": cap_mb,
        "status": if applied { "applied" } else { "verification_mismatch" },
        "cap_applied": applied,
        "effective_bytes": observed.rlim_cur,
        "hard_limit_bytes": observed.rlim_max,
        "error": if applied {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(
                "observed RLIMIT_AS did not equal the requested soft and hard limits".to_string()
            )
        },
    })
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
    let norm = v
        .iter()
        .map(|x| (*x as f64) * (*x as f64))
        .sum::<f64>()
        .sqrt() as f32;
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
        let _ = write!(
            jsonl,
            r#"{{"type":"Doc","data":{{"slug":"{slug}","embedding":"#
        );
        push_vector_json(
            &mut jsonl,
            &seeded_vector(args.seed ^ 0x5eed, slug, args.dims, 0.0),
        );
        jsonl.push_str(
            "}}
",
        );
        db.load("main", &jsonl, LoadMode::Merge)
            .await
            .expect("diverge main");
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
    let outcome = db
        .branch_merge("bench", "main")
        .await
        .expect("branch_merge");
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
            let _ = write!(
                jsonl,
                r#"{{"type":"Doc","data":{{"slug":"{slug}","embedding":"#
            );
            push_vector_json(&mut jsonl, &seeded_vector(seed, &slug, args.dims, pole));
            jsonl.push_str("}}\n");
        }
        db.load(branch, &jsonl, LoadMode::Merge)
            .await
            .expect("load batch");
        row = end;
    }
}

// ---------------------------------------------------------------------------
// Scenario: ann-probe-budget
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[derive(Debug, serde::Serialize)]
struct AnnScanMeasurement {
    rows_returned: usize,
    elapsed_us: u64,
    partitions_searched: usize,
    partitions_ranked: usize,
    index_parts_loaded: usize,
    index_bytes_read: usize,
    index_iops: usize,
    storage_requests: usize,
}

#[cfg(unix)]
async fn measure_ann_scan(
    dataset: &lance::Dataset,
    query: &arrow_array::Float32Array,
    k: usize,
    filtered: bool,
    minimum_nprobes: usize,
    maximum_nprobes: Option<usize>,
) -> AnnScanMeasurement {
    use std::sync::{Arc, Mutex};

    use lance::dataset::scanner::ExecutionSummaryCounts;
    use lance_datafusion::utils::{PARTITIONS_RANKED_METRIC, PARTITIONS_SEARCHED_METRIC};

    let collected = Arc::new(Mutex::new(None::<ExecutionSummaryCounts>));
    let callback_target = collected.clone();
    let mut scanner = dataset.scan();
    scanner.nearest("vector", query, k).expect("nearest");
    scanner.minimum_nprobes(minimum_nprobes);
    if let Some(maximum) = maximum_nprobes {
        scanner.maximum_nprobes(maximum);
    }
    if filtered {
        scanner.prefilter(true);
        scanner.filter("eligible = true").expect("eligible filter");
    }
    scanner
        .project(&["id".to_string()])
        .expect("project id")
        .target_parallelism(1)
        .scan_stats_callback(Arc::new(move |summary| {
            *callback_target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(summary.clone());
        }));

    let started = Instant::now();
    let batch = scanner.try_into_batch().await.expect("ANN scan");
    let elapsed_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
    let summary = collected
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .expect("Lance scan statistics callback");

    AnnScanMeasurement {
        rows_returned: batch.num_rows(),
        elapsed_us,
        partitions_searched: summary
            .all_counts
            .get(PARTITIONS_SEARCHED_METRIC)
            .copied()
            .unwrap_or_default(),
        partitions_ranked: summary
            .all_counts
            .get(PARTITIONS_RANKED_METRIC)
            .copied()
            .unwrap_or_default(),
        index_parts_loaded: summary.parts_loaded,
        index_bytes_read: summary.bytes_read,
        index_iops: summary.iops,
        storage_requests: summary.requests,
    }
}

#[cfg(unix)]
async fn measure_fresh_ann_scan(
    uri: &str,
    query: &arrow_array::Float32Array,
    k: usize,
    filtered: bool,
    minimum_nprobes: usize,
    maximum_nprobes: Option<usize>,
) -> AnnScanMeasurement {
    let dataset = lance::Dataset::open(uri).await.expect("reopen ANN fixture");
    measure_ann_scan(
        &dataset,
        query,
        k,
        filtered,
        minimum_nprobes,
        maximum_nprobes,
    )
    .await
}

#[cfg(unix)]
/// A current-Lance structural reproduction for issue #567 and PR #591. The
/// fixture gives every IVF partition the same row count and the same number of
/// filter-eligible rows. Precomputed centroids remove k-means randomness, so
/// the reported partition counts distinguish Lance's adaptive search from a
/// hard maximum and from the PR's whole-query retry.
async fn ann_probe_budget(args: &Args) -> serde_json::Value {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use lance::Dataset;
    use lance::dataset::{WriteMode, WriteParams};
    use lance::index::DatasetIndexExt;
    use lance::index::vector::VectorIndexParams;
    use lance_file::version::LanceFileVersion;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;
    use lance_index::vector::ivf::IvfBuildParams;
    use lance_linalg::distance::MetricType;

    const BATCH_ROWS: usize = 8_192;
    const VECTOR_INDEX_NAME: &str = "vector_idx";

    assert!(args.dims >= 3, "ann-probe-budget requires --dims >= 3");
    assert!(
        args.selectivity > 0.0 && args.selectivity <= 1.0,
        "ann-probe-budget requires 0 < --selectivity <= 1"
    );
    assert!(args.ann_partitions > 1, "--ann-partitions must exceed 1");
    assert!(args.ann_probes > 0, "--ann-probes must be positive");
    assert!(
        args.ann_probes < args.ann_partitions,
        "--ann-probes must be lower than --ann-partitions"
    );
    assert_eq!(
        args.rows % args.ann_partitions,
        0,
        "--rows must be divisible by --ann-partitions"
    );

    let rows_per_partition = args.rows / args.ann_partitions;
    let eligibility_stride = (1.0 / args.selectivity).round().max(1.0) as usize;
    let eligible_per_partition = rows_per_partition.div_ceil(eligibility_stride);
    let eligible_rows = eligible_per_partition * args.ann_partitions;
    assert!(
        args.k <= rows_per_partition,
        "choose --k <= rows per partition so unfiltered Lance-default search can stop after one probe"
    );
    assert!(
        args.k <= eligible_rows,
        "the fixture must contain at least k eligible rows"
    );
    assert!(
        args.ann_probes * eligible_per_partition < args.k,
        "choose k/selectivity so the hard probe cap deterministically under-fills"
    );

    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("eligible", DataType::Boolean, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(item.clone(), args.dims as i32),
            false,
        ),
    ]));

    let dir = tempfile::tempdir().expect("ANN fixture tempdir");
    let uri = dir.path().join("ann-probe-budget.lance");
    let uri = uri.to_str().expect("UTF-8 fixture path");

    let starts = (0..args.rows).step_by(BATCH_ROWS).collect::<Vec<_>>();
    let batch_schema = schema.clone();
    let batch_item = item.clone();
    let rows = args.rows;
    let dims = args.dims;
    let partitions = args.ann_partitions;
    let seed = args.seed;
    let batches = starts.into_iter().map(move |start| {
        let end = (start + BATCH_ROWS).min(rows);
        let mut vector_values = vec![0.0_f32; (end - start) * dims];
        let mut eligible = Vec::with_capacity(end - start);
        for (offset, row) in (start..end).enumerate() {
            let partition = row % partitions;
            let round = row / partitions;
            let angle = std::f32::consts::TAU * partition as f32 / partitions as f32;
            let vector = &mut vector_values[offset * dims..(offset + 1) * dims];
            vector[0] = angle.cos();
            vector[1] = angle.sin();
            let mut state = seed ^ (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            for value in &mut vector[2..] {
                *value =
                    ((xorshift64(&mut state) >> 40) as f32 / (1_u32 << 24) as f32 - 0.5) * 0.0001;
            }
            eligible.push(round.is_multiple_of(eligibility_stride));
        }
        let vectors = FixedSizeListArray::new(
            batch_item.clone(),
            dims as i32,
            Arc::new(Float32Array::from(vector_values)),
            None,
        );
        RecordBatch::try_new(
            batch_schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(start as u64..end as u64)),
                Arc::new(BooleanArray::from(eligible)),
                Arc::new(vectors),
            ],
        )
    });
    let reader = RecordBatchIterator::new(batches, schema);

    let setup_started = Instant::now();
    let mut dataset = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await
    .expect("write ANN fixture");

    dataset
        .create_index_builder(
            &["eligible"],
            IndexType::BTree,
            &ScalarIndexParams::default(),
        )
        .name("eligible_idx".to_string())
        .replace(true)
        .await
        .expect("build eligible BTREE");

    let mut centroid_values = vec![0.0_f32; args.ann_partitions * args.dims];
    for partition in 0..args.ann_partitions {
        let angle = std::f32::consts::TAU * partition as f32 / args.ann_partitions as f32;
        let centroid = &mut centroid_values[partition * args.dims..(partition + 1) * args.dims];
        centroid[0] = angle.cos();
        centroid[1] = angle.sin();
    }
    let centroids = FixedSizeListArray::new(
        item,
        args.dims as i32,
        Arc::new(Float32Array::from(centroid_values)),
        None,
    );
    let ivf = IvfBuildParams::try_with_centroids(args.ann_partitions, Arc::new(centroids))
        .expect("valid fixed centroids");
    let vector_params = VectorIndexParams::with_ivf_flat_params(MetricType::L2, ivf);
    dataset
        .create_index_builder(&["vector"], IndexType::Vector, &vector_params)
        .name(VECTOR_INDEX_NAME.to_string())
        .replace(true)
        .await
        .expect("build IVF_FLAT index");
    let setup_ms = setup_started.elapsed().as_millis() as u64;

    let index_stats: serde_json::Value = serde_json::from_str(
        &dataset
            .index_statistics(VECTOR_INDEX_NAME)
            .await
            .expect("vector index statistics"),
    )
    .expect("vector index statistics JSON");
    let actual_partitions = index_stats["indices"][0]["num_partitions"]
        .as_u64()
        .expect("IVF statistics expose num_partitions") as usize;
    assert_eq!(actual_partitions, args.ann_partitions);

    if args.baseline {
        return serde_json::json!({
            "baseline": true,
            "setup_ms": setup_ms,
            "actual_partitions": actual_partitions,
            "eligible_rows": eligible_rows,
        });
    }

    // Two query shapes expose both sides of Lance's heuristic. A query equal
    // to centroid 0 has nearest-centroid distance zero, so the adaptive
    // minimum remains one. The orthogonal query is equidistant from every
    // centroid, so Lance 11's k>=11 distance threshold promotes the minimum
    // to every partition unless a maximum clamps it.
    let mut aligned_values = vec![0.0_f32; args.dims];
    aligned_values[0] = 1.0;
    let aligned_query = Float32Array::from(aligned_values);
    let mut equidistant_values = vec![0.0_f32; args.dims];
    equidistant_values[2] = 1.0;
    let equidistant_query = Float32Array::from(equidistant_values);

    let aligned_adaptive =
        measure_fresh_ann_scan(uri, &aligned_query, args.k, false, 1, None).await;
    let aligned_maximum_only =
        measure_fresh_ann_scan(uri, &aligned_query, args.k, false, 1, Some(args.ann_probes)).await;
    let aligned_fixed = measure_fresh_ann_scan(
        uri,
        &aligned_query,
        args.k,
        false,
        args.ann_probes,
        Some(args.ann_probes),
    )
    .await;
    let equidistant_adaptive =
        measure_fresh_ann_scan(uri, &equidistant_query, args.k, false, 1, None).await;
    let equidistant_maximum_only = measure_fresh_ann_scan(
        uri,
        &equidistant_query,
        args.k,
        false,
        1,
        Some(args.ann_probes),
    )
    .await;

    let unfiltered_large_k = args.ann_probes * rows_per_partition + 1;
    let unfiltered_large_k_adaptive =
        measure_fresh_ann_scan(uri, &aligned_query, unfiltered_large_k, false, 1, None).await;
    let unfiltered_large_k_bounded = measure_fresh_ann_scan(
        uri,
        &aligned_query,
        unfiltered_large_k,
        false,
        1,
        Some(args.ann_probes),
    )
    .await;
    let filtered_adaptive =
        measure_fresh_ann_scan(uri, &aligned_query, args.k, true, 1, None).await;
    let filtered_bounded =
        measure_fresh_ann_scan(uri, &aligned_query, args.k, true, 1, Some(args.ann_probes)).await;

    let retry_dataset = Dataset::open(uri).await.expect("reopen retry fixture");
    let retry_first = measure_ann_scan(
        &retry_dataset,
        &aligned_query,
        args.k,
        true,
        1,
        Some(args.ann_probes),
    )
    .await;
    let retry_second = if retry_first.rows_returned < args.k {
        Some(measure_ann_scan(&retry_dataset, &aligned_query, args.k, true, 1, None).await)
    } else {
        None
    };
    let retry_totals = serde_json::json!({
        "elapsed_us": retry_first.elapsed_us
            + retry_second.as_ref().map_or(0, |m| m.elapsed_us),
        "partitions_searched": retry_first.partitions_searched
            + retry_second.as_ref().map_or(0, |m| m.partitions_searched),
        "index_parts_loaded": retry_first.index_parts_loaded
            + retry_second.as_ref().map_or(0, |m| m.index_parts_loaded),
        "index_bytes_read": retry_first.index_bytes_read
            + retry_second.as_ref().map_or(0, |m| m.index_bytes_read),
        "index_iops": retry_first.index_iops
            + retry_second.as_ref().map_or(0, |m| m.index_iops),
        "storage_requests": retry_first.storage_requests
            + retry_second.as_ref().map_or(0, |m| m.storage_requests),
        "final_rows_returned": retry_second
            .as_ref()
            .map_or(retry_first.rows_returned, |m| m.rows_returned),
    });

    assert_eq!(aligned_adaptive.rows_returned, args.k);
    assert_eq!(aligned_maximum_only.rows_returned, args.k);
    assert_eq!(
        aligned_adaptive.partitions_searched, 1,
        "a centroid-aligned query must expose Lance's one-partition fast path"
    );
    assert_eq!(
        aligned_maximum_only.partitions_searched, 1,
        "a maximum-only guard must preserve the one-partition fast path"
    );
    assert_eq!(
        aligned_fixed.partitions_searched, args.ann_probes,
        "setting both minimum and maximum must force needless easy-query work"
    );

    assert_eq!(equidistant_adaptive.rows_returned, args.k);
    assert_eq!(equidistant_maximum_only.rows_returned, args.k);
    assert_eq!(
        equidistant_adaptive.partitions_searched, actual_partitions,
        "the off-centroid query must reproduce Lance's all-partition expansion"
    );
    assert_eq!(
        equidistant_maximum_only.partitions_searched, args.ann_probes,
        "the maximum-only guard must clamp the all-partition expansion"
    );
    assert!(
        equidistant_maximum_only.index_parts_loaded < equidistant_adaptive.index_parts_loaded,
        "the maximum-only guard must reduce payload partition reads"
    );

    assert_eq!(
        unfiltered_large_k_adaptive.rows_returned,
        unfiltered_large_k
    );
    assert!(
        unfiltered_large_k_bounded.rows_returned < unfiltered_large_k,
        "the hard probe cap must expose unfiltered large-k underfill"
    );
    assert_eq!(
        unfiltered_large_k_bounded.partitions_searched, args.ann_probes,
        "the hard probe cap must stop unfiltered large-k search at the requested ceiling"
    );

    assert_eq!(filtered_adaptive.rows_returned, args.k);
    assert_eq!(
        filtered_bounded.partitions_searched, args.ann_probes,
        "maximum_nprobes must impose the requested filtered probe ceiling"
    );
    assert!(
        filtered_bounded.rows_returned < args.k,
        "the deterministic selective fixture must expose hard-cap underfill"
    );
    let retry_second = retry_second.expect("hard-cap underfill must trigger the PR retry");
    assert_eq!(retry_second.rows_returned, args.k);
    assert!(
        retry_first.partitions_searched + retry_second.partitions_searched
            > filtered_adaptive.partitions_searched,
        "capped-then-retry must repeat IVF work compared with one adaptive scan"
    );

    serde_json::json!({
        "scope": "synthetic local structural reproduction; timing is diagnostic, not the issue's S3 production measurement",
        "lance_version": "11.0.0",
        "index_type": "IVF_FLAT",
        "setup_ms": setup_ms,
        "fixture": {
            "rows": args.rows,
            "dims": args.dims,
            "partitions": actual_partitions,
            "rows_per_partition": rows_per_partition,
            "requested_selectivity": args.selectivity,
            "actual_selectivity": eligible_rows as f64 / args.rows as f64,
            "eligible_rows": eligible_rows,
            "eligible_per_partition": eligible_per_partition,
            "k": args.k,
            "maximum_nprobes": args.ann_probes,
        },
        "modes": {
            "centroid_aligned_lance_default": aligned_adaptive,
            "centroid_aligned_maximum_only": aligned_maximum_only,
            "centroid_aligned_fixed_minimum_and_maximum": aligned_fixed,
            "equidistant_lance_default": equidistant_adaptive,
            "equidistant_maximum_only": equidistant_maximum_only,
            "unfiltered_large_k": unfiltered_large_k,
            "unfiltered_large_k_lance_default_min1_no_max": unfiltered_large_k_adaptive,
            "unfiltered_large_k_maximum_only": unfiltered_large_k_bounded,
            "filtered_lance_default_min1_no_max": filtered_adaptive,
            "filtered_maximum_only_first_pass": filtered_bounded,
            "filtered_maximum_only_then_retry": {
                "first_pass": retry_first,
                "retry": retry_second,
                "totals": retry_totals,
            },
        },
    })
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
            push_vector_json(
                &mut jsonl,
                &seeded_vector(args.seed, &slug, args.dims, pole),
            );
            jsonl.push_str("}}\n");
        }
        db.load("main", &jsonl, LoadMode::Merge)
            .await
            .expect("load batch");
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
            .query(
                ReadTarget::branch("main"),
                &query_src,
                "filtered_nearest",
                &params,
            )
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

// ---------------------------------------------------------------------------
// Scenario: rrf-gate
// ---------------------------------------------------------------------------

#[cfg(unix)]
/// One cell of the rrf prefilter-gate matrix: a bm25+bm25 `rrf()` read
/// joined through an edge traversal, where `--selectivity` of the ranked rows
/// carry edges (fanout 2 — the issue-#563 repro shape; `--text-bytes 204800`
/// reproduces its overflow-scale corpus, ~2048 the wide variant, and a tiny
/// value turns the cell into the in-list BTREE-probe microbench).
///
/// `--baseline` forces the postfilter plan (v0.9 rrf semantics: uncapped
/// corpus-wide arms); the default forces the prefilter plan, so the
/// wall-clock crossover stays measurable ABOVE the natural threshold too.
/// Query iteration 0 runs against a cold `RuntimeCache` (the gate's
/// forced-CSR-build case); later iterations are warm. Per-iteration
/// object-store reads come from one persistent `IOTracker` pair installed
/// before the graph opens (the `cost_harness` pattern), the gate's verdict
/// from the `rrf_gate_verdicts` probe, and the in-list `Expr` build over the
/// eligible ids is timed separately (the predicate-build half). A query
/// error — e.g. the #563 Offset overflow under the postfilter plan at
/// 200 KiB text — is recorded as that iteration's `error`, not a crash: an
/// overflow under the baseline is a data point.
async fn rrf_gate(args: &Args) -> serde_json::Value {
    use lance::io::WrappingObjectStore;
    use lance_io::utils::tracking_store::IOTracker;
    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes, with_rrf_plan};

    const ARTIFACTS: usize = 750;
    const FANOUT: usize = 2;
    const QUERY_ITERS: usize = 3;
    const EDGE_BATCH_ROWS: usize = 4_000;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(
            std::sync::Arc::new(table_tracker.clone()) as std::sync::Arc<dyn WrappingObjectStore>
        ),
        manifest_wrapper: Some(std::sync::Arc::new(manifest_tracker.clone())
            as std::sync::Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    let verdicts = std::sync::Arc::clone(&probes.rrf_gate_verdicts);
    let args = args.clone();
    let table = table_tracker.clone();
    let manifest = manifest_tracker.clone();

    // The whole cell runs under ONE probe scope so every dataset handle —
    // including cached ones reused by warm iterations — carries the trackers.
    with_query_io_probes(
        probes,
        Box::pin(async move {
            let schema = "node Chunk {\n    slug: String @key\n    text: String @index\n}\n\n\
                          node Artifact {\n    slug: String @key\n}\n\n\
                          edge ChunkOfArtifact: Chunk -> Artifact {\n    label: String\n}\n";
            let dir = tempfile::tempdir().expect("tempdir");
            let uri = dir.path().to_str().unwrap();
            let db = Omnigraph::init(uri, schema).await.expect("init");

            // Both query terms in every row (rank ties are irrelevant here —
            // this is a cost instrument, not the equality oracle), padded to
            // `--text-bytes` with a tiny vocabulary of long tokens so the
            // stored column carries full byte weight while the FTS term
            // dictionary stays small.
            let mut text = String::with_capacity(args.text_bytes + 64);
            text.push_str("needle563 sharp563 ");
            let filler_unit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
                               cccccccccccccccccccccccccccccccc dddddddddddddddddddddddddddddddd ";
            while text.len() < args.text_bytes {
                text.push_str(filler_unit);
            }

            let seed_start = Instant::now();
            let mut head = String::new();
            for a in 0..ARTIFACTS {
                let _ = writeln!(head, r#"{{"type":"Artifact","data":{{"slug":"art-{a:04}"}}}}"#);
            }
            db.load("main", &head, LoadMode::Merge).await.expect("load artifacts");

            let chunk_batch_rows = (24 * 1024 * 1024 / args.text_bytes.max(1)).clamp(1, 4_000);
            let mut chunk_batch = String::new();
            for c in 0..args.rows {
                let _ = writeln!(
                    chunk_batch,
                    r#"{{"type":"Chunk","data":{{"slug":"chunk-{c:06}","text":"{text}"}}}}"#
                );
                if (c + 1) % chunk_batch_rows == 0 || c + 1 == args.rows {
                    db.load("main", &chunk_batch, LoadMode::Merge)
                        .await
                        .expect("load chunks");
                    chunk_batch.clear();
                }
            }

            // Every stride-th chunk is eligible (carries FANOUT edges).
            let stride = (1.0 / args.selectivity).round().max(1.0) as usize;
            let mut eligible_slugs: Vec<String> = Vec::new();
            let mut edges = String::new();
            let mut edge_rows = 0usize;
            for c in (0..args.rows).step_by(stride) {
                eligible_slugs.push(format!("chunk-{c:06}"));
                for f in 0..FANOUT {
                    let a = (c * FANOUT + f) % ARTIFACTS;
                    let _ = writeln!(
                        edges,
                        r#"{{"edge":"ChunkOfArtifact","from":"chunk-{c:06}","to":"art-{a:04}","data":{{"id":"e-{c:06}-{f}","label":"of"}}}}"#
                    );
                    edge_rows += 1;
                    if edge_rows == EDGE_BATCH_ROWS {
                        db.load("main", &edges, LoadMode::Merge).await.expect("load edges");
                        edges.clear();
                        edge_rows = 0;
                    }
                }
            }
            if edge_rows > 0 {
                db.load("main", &edges, LoadMode::Merge).await.expect("load edges");
            }
            let seed_ms = seed_start.elapsed().as_millis() as u64;

            // FTS + BTREE indices built AFTER the last write: full fragment
            // coverage, so the gate's coverage fence admits the prefilter plan.
            let index_start = Instant::now();
            db.ensure_indices().await.expect("ensure_indices");
            let index_ms = index_start.elapsed().as_millis() as u64;

            // Query on a FRESH handle: the seeding handle's dataset cache
            // would otherwise serve the scans through unwrapped stores (the
            // IO trackers attach at open) and mask the cold-cache cost the
            // matrix wants — iteration 0 must pay real data opens and the
            // gate's cold CSR build.
            drop(db);
            let db = Omnigraph::open(uri).await.expect("reopen");

            // Constructing one `lit()` per eligible id plus the `IN`-list
            // `Expr` — the per-id predicate-BUILD cost, which prior
            // indexed-eval measurements never covered.
            let inlist_build_start = Instant::now();
            let id_list: Vec<datafusion::prelude::Expr> = eligible_slugs
                .iter()
                .map(|slug| datafusion::prelude::lit(slug.clone()))
                .collect();
            let in_list_expr = datafusion::prelude::col("id").in_list(id_list, false);
            let inlist_build_ms = inlist_build_start.elapsed().as_micros() as f64 / 1000.0;
            std::hint::black_box(&in_list_expr);

            let query_src = format!(
                "query recall_rrf($q1: String, $q2: String) {{\n    match {{\n        $c: Chunk\n        $c chunkOfArtifact $a\n    }}\n    return {{ $c.slug, $a.slug }}\n    order {{ rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }}\n    limit {}\n}}\n",
                args.k
            );
            let query_params = helpers::params(&[("$q1", "needle563"), ("$q2", "sharp563")]);
            let plan_mode: &'static str = if args.baseline {
                "force_postfilter"
            } else {
                "force_prefilter"
            };

            // Drop the seed/index reads so per-iteration stats start clean.
            let _ = table.incremental_stats();
            let _ = manifest.incremental_stats();

            let mut iterations: Vec<serde_json::Value> = Vec::new();
            for iter in 0..QUERY_ITERS {
                let started = Instant::now();
                let outcome = with_rrf_plan(
                    plan_mode,
                    db.query(
                        ReadTarget::branch("main"),
                        &query_src,
                        "recall_rrf",
                        &query_params,
                    ),
                )
                .await;
                let wall_ms = started.elapsed().as_millis() as u64;
                let table_stats = table.incremental_stats();
                let manifest_stats = manifest.incremental_stats();
                let verdict = verdicts
                    .lock()
                    .ok()
                    .and_then(|v| v.last().cloned())
                    .map(|v| {
                        serde_json::json!({
                            "plan": format!("{:?}", v.plan),
                            "fallback": v.fallback.map(|f| format!("{f:?}")),
                            "forced": v.forced,
                            "eligible": v.eligible,
                            "corpus": v.corpus,
                        })
                    })
                    .unwrap_or(serde_json::Value::Null);
                match outcome {
                    Ok(result) => {
                        let rows: usize = result.batches().iter().map(|b| b.num_rows()).sum();
                        iterations.push(serde_json::json!({
                            "iter": iter,
                            "wall_ms": wall_ms,
                            "rows_returned": rows,
                            "table_read_iops": table_stats.read_iops,
                            "table_read_bytes": table_stats.read_bytes,
                            "manifest_read_iops": manifest_stats.read_iops,
                            "verdict": verdict,
                            "error": serde_json::Value::Null,
                        }));
                    }
                    Err(error) => {
                        iterations.push(serde_json::json!({
                            "iter": iter,
                            "wall_ms": wall_ms,
                            "rows_returned": serde_json::Value::Null,
                            "table_read_iops": table_stats.read_iops,
                            "table_read_bytes": table_stats.read_bytes,
                            "manifest_read_iops": manifest_stats.read_iops,
                            "verdict": verdict,
                            "error": error.to_string(),
                        }));
                        // Later iterations would fail identically; the error
                        // itself is the cell's result.
                        break;
                    }
                }
            }

            let warm: Vec<u64> = iterations
                .iter()
                .skip(1)
                .filter(|it| it.get("error") == Some(&serde_json::Value::Null))
                .filter_map(|it| it.get("wall_ms").and_then(serde_json::Value::as_u64))
                .collect();
            serde_json::json!({
                "plan_mode": plan_mode,
                "seed_ms": seed_ms,
                "index_ms": index_ms,
                "corpus_rows": args.rows,
                "text_bytes": args.text_bytes,
                "eligible_rows": eligible_slugs.len(),
                "fanout": FANOUT,
                "inlist_build_ms": inlist_build_ms,
                "cold_wall_ms": iterations.first().and_then(|it| it.get("wall_ms").cloned()),
                "warm_mean_wall_ms": if warm.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(warm.iter().sum::<u64>() as f64 / warm.len() as f64)
                },
                "iterations": iterations,
            })
        }),
    )
    .await
}
