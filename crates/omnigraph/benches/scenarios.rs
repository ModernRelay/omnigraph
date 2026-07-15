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
    /// Selectivity for nearest-prefilter: fraction of rows matching the filter.
    selectivity: f64,
    /// ANN k (the query's `limit`) for nearest-prefilter.
    k: usize,
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
            "usage: --scenario <merge-all-changed|nearest-prefilter|fenced-small-upsert|\
             fenced-adopt-all-new> [--rows N] [--dims D] \
             [--seed S] [--runs K] [--selectivity F] [--k K] [--memory-cap-mb M]"
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
        let record = if args.scenario == "fenced-adopt-all-new" {
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
    out.status.success().then(|| !out.stdout.is_empty())
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
            ("merge-all-changed", None) => merge_all_changed(args).await,
            ("nearest-prefilter", None) => nearest_prefilter(args).await,
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
    let Ok(requested_limit) = libc::rlim_t::try_from(requested_bytes) else {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "invalid_requested_cap",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": "requested memory cap does not fit rlim_t",
        });
    };
    let requested = libc::rlimit {
        rlim_cur: requested_limit,
        rlim_max: requested_limit,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_AS, &requested) } != 0 {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "setrlimit_failed",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": std::io::Error::last_os_error().to_string(),
        });
    }

    let mut observed: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut observed) } != 0 {
        return serde_json::json!({
            "requested_mb": cap_mb,
            "status": "getrlimit_failed",
            "cap_applied": false,
            "effective_bytes": null,
            "hard_limit_bytes": null,
            "error": std::io::Error::last_os_error().to_string(),
        });
    }
    let applied = observed.rlim_cur == requested_limit && observed.rlim_max == requested_limit;
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
