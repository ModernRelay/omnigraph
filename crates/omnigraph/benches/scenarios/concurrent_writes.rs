//! Concurrent-writes throughput diagnostic (RFC 0067, "What to measure
//! before promising numbers"): sustained commits/sec per branch under N
//! closed-loop writers, with the manifest-shape and backend axes the RFC
//! names — compact vs fragmented (`--history-commits`, `--manifest-layout`)
//! and local-fs vs an S3-compatible store (`--target-uri s3://…`).
//!
//! The driver is CLOSED-LOOP: each writer task issues its next insert only
//! after the previous acknowledgement. Per RFC 0039 Rule 1 that shape is
//! subject to coordinated omission, so nothing here is claim-grade: the
//! record self-labels `driver: "closed-loop"`, `claim_grade: false`, and
//! latencies are named `service_time_*` (time to serve one acknowledged
//! write), never arrival latency. Claim-grade throughput/latency needs the
//! future open-loop scheduled-arrival benchmark kind.
//!
//! Workload: insert-only `Chunk` rows with disjoint keys per worker
//! (`cw-w{worker}-{seq}`), the shape that exercises the write path's real
//! serialization — the process-global write queue and the exclusive schema
//! gate every writer crosses in `commit_all` — without manufacturing key
//! conflicts. `Omnigraph::mutate` replays a typed read-set/authority
//! conflict (`ReadSetChanged`: the graph head moved under a concurrent
//! writer) itself, up to `MAX_PRE_EFFECT_REPREPARES` times for an
//! insert-only mutation; the `mutation_reprepares` probe counts those
//! replays (`reprepares_per_commit`). A `ReadSetChanged` reaches the driver
//! only when that loop is exhausted; the driver then retries the op like a
//! real client and counts the exhaustion (`authority_conflicts`). Both
//! kinds of retry ride inside the op's service time. Any other worker
//! error, any `KeyConflict`, or a verification mismatch invalidates the run.
//!
//! Concurrency shape: N tokio tasks over clones of ONE `Session` (one
//! `Arc<Omnigraph>`) — the production server shape (one engine handle per
//! graph, a session per request, `&self` writes).
//!
//! Op counting rides the ungated instrumentation surface: one
//! `QueryIoProbes` value (Lance manifest/table planes, `IOTracker`
//! wrappers) installed on the graph open and on EVERY worker task — the
//! probes are a tokio task-local and do not cross `tokio::spawn` — plus a
//! `CountingStorageAdapter` for the engine control plane. The Lance totals
//! are read from each probed `ObjectStore`'s own tracker (`ProbedStores`),
//! because on `file://` Lance's direct local reader and writer never reach
//! a wrapper. Counters are logical calls, not physical requests. Verification
//! runs on a fresh, uncounted handle after every counter and clock has been
//! read.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use lance::io::WrappingObjectStore;
use lance_io::utils::tracking_store::IOTracker;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{
    CountingStorageAdapter, ProbedStores, QueryIoProbes, StorageReadCounts,
    enabled_engine_cargo_features, with_query_io_probes,
};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph::storage::storage_for_uri;

use super::{Args, current_process_peak_rss_bytes, helpers, rfc023_limits, rfc023_scenarios};

/// Phase flag values shared between the timeline task and the writers.
const PHASE_WARMUP: u8 = 0;
const PHASE_MEASURED: u8 = 1;
const PHASE_STOP: u8 = 2;

/// Slack past `warmup + duration` before the child self-terminates: covers
/// fixture build, joins and verification, so a wedged remote store cannot
/// hang the parent's `wait4` forever.
const WATCHDOG_SLACK: Duration = Duration::from_secs(120);

pub(super) fn is_scenario(name: &str) -> bool {
    name == "concurrent-writes"
}

pub(super) fn validate_args(args: &Args) -> Result<(), String> {
    if args.writers == 0 {
        return Err("--writers must be greater than zero".into());
    }
    if args.duration_secs == 0 {
        return Err("--duration-secs must be greater than zero".into());
    }
    if args.write_branches == 0 {
        return Err("--write-branches must be greater than zero".into());
    }
    if args.baseline {
        return Err("concurrent-writes has no baseline arm".into());
    }
    if args.cache_state == "warm" {
        return Err(
            "concurrent-writes runs cold only; warm preparation is a branch-control axis".into(),
        );
    }
    if args.phase.is_some() || args.fixture_root.is_some() {
        return Err("--phase/--fixture-root are internal to phased adopt children".into());
    }
    if let Some(uri) = &args.target_uri
        && !uri.starts_with("s3://")
    {
        return Err(format!(
            "--target-uri must be an s3:// URI (or unset for a local tempdir), got '{uri}'"
        ));
    }
    // Seed feasibility under the same chunked-load plan the fixtures use.
    rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)?;
    Ok(())
}

/// The insert grammar; the vector parameter type carries the fixture's
/// dimension, so the grammar is built per run.
fn insert_source(dims: usize) -> String {
    format!(
        "query put($slug: String, $v: Vector({dims})) {{ insert Chunk {{ slug: $slug, embedding: $v }} }}"
    )
}

/// Exact nearest-rank percentile over a sorted sample set — the same
/// formula `omnigraph-bench` uses (`record.rs::nearest_rank`), so numbers
/// stay comparable across the two harnesses.
fn nearest_rank(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// `cfg!(tokio_unstable)` in one place: the workspace injects the cfg via
/// `.cargo/config.toml` rustflags unless `RUSTFLAGS=` replaces it, and the
/// record must say which build it measured. The cfg is not declared in this
/// crate's lint table, hence the local allow.
#[allow(unexpected_cfgs)]
fn tokio_unstable_cfg() -> bool {
    cfg!(tokio_unstable)
}

#[derive(Default)]
struct LancePlaneTotals {
    read_iops: u64,
    read_bytes: u64,
    write_iops: u64,
    write_bytes: u64,
}

impl LancePlaneTotals {
    /// Totals come from each probed store's own tracker, which also sees
    /// Lance's direct local reader and writer; the wrapper tracker misses
    /// those on `file://` and is drained only to bound its request log.
    fn drain_from(&mut self, wrapper: &IOTracker, stores: &ProbedStores) {
        let _ = wrapper.incremental_stats();
        for store in stores.stores() {
            let stats = store.io_stats_incremental();
            self.read_iops += stats.read_iops;
            self.read_bytes += stats.read_bytes;
            self.write_iops += stats.write_iops;
            self.write_bytes += stats.written_bytes;
        }
    }
}

/// A point-in-time copy of the control-plane counters, so the measured
/// window is a delta rather than a since-open total.
#[derive(Default, Clone)]
struct ControlSnapshot {
    read_text: u64,
    read_text_if_exists: u64,
    read_bytes_if_exists: u64,
    exists: u64,
    read_text_versioned: u64,
    list_dir: u64,
    mutation_calls: u64,
    write_text: u64,
    write_bytes: u64,
    delete: u64,
}

impl ControlSnapshot {
    fn capture(counts: &StorageReadCounts) -> Self {
        Self {
            read_text: counts.read_text.load(Ordering::Relaxed),
            read_text_if_exists: counts.read_text_if_exists.load(Ordering::Relaxed),
            read_bytes_if_exists: counts.read_bytes_if_exists.load(Ordering::Relaxed),
            exists: counts.exists.load(Ordering::Relaxed),
            read_text_versioned: counts.read_text_versioned.load(Ordering::Relaxed),
            list_dir: counts.list_dir.load(Ordering::Relaxed),
            mutation_calls: counts.mutation_calls.load(Ordering::Relaxed),
            write_text: counts.write_text.load(Ordering::Relaxed),
            write_bytes: counts.write_bytes.load(Ordering::Relaxed),
            delete: counts.delete.load(Ordering::Relaxed),
        }
    }

    fn delta_json(&self, later: &Self) -> serde_json::Value {
        serde_json::json!({
            "read_text": later.read_text - self.read_text,
            "read_text_if_exists": later.read_text_if_exists - self.read_text_if_exists,
            "read_bytes_if_exists": later.read_bytes_if_exists - self.read_bytes_if_exists,
            "exists": later.exists - self.exists,
            "read_text_versioned": later.read_text_versioned - self.read_text_versioned,
            "list_dir": later.list_dir - self.list_dir,
            "mutation_calls": later.mutation_calls - self.mutation_calls,
            "write_text": later.write_text - self.write_text,
            "write_bytes": later.write_bytes - self.write_bytes,
            "delete": later.delete - self.delete,
        })
    }
}

/// A typed read-set/authority conflict that outlived the engine's own
/// reprepare loop. Under concurrent same-branch writers this is an expected
/// outcome, not an invalid run; the driver retries it and the record counts it.
fn is_authority_conflict(error: &omnigraph::error::OmniError) -> bool {
    matches!(
        error,
        omnigraph::error::OmniError::Manifest(manifest)
            if matches!(
                manifest.details,
                Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged { .. })
            )
    )
}

/// Why a worker stopped, classified once from the typed `OmniError`.
enum WorkerFailure {
    KeyConflict(String),
    Other(String),
}

impl WorkerFailure {
    fn classify(error: &omnigraph::error::OmniError) -> Self {
        match error {
            omnigraph::error::OmniError::KeyConflict { .. } => Self::KeyConflict(error.to_string()),
            _ => Self::Other(error.to_string()),
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::KeyConflict(message) | Self::Other(message) => message,
        }
    }
}

enum VerificationFailure {
    RowCount(String),
    Readback(String),
}

impl VerificationFailure {
    fn message(&self) -> &str {
        match self {
            Self::RowCount(message) | Self::Readback(message) => message,
        }
    }
}

struct WorkerOutcome {
    worker: usize,
    branch: String,
    acked_ops: u64,
    warmup_ops: u64,
    /// Exhaustions of the engine's reprepare loop the driver retried (see
    /// [`is_authority_conflict`]).
    authority_conflicts: u64,
    /// `(start_offset_us_from_run_start, service_time_us)` for ops whose
    /// start fell inside the measured window.
    samples: Vec<(u64, u64)>,
    failure: Option<WorkerFailure>,
}

/// What the measured probes scope hands back to the (unprobed) tail of the
/// run: everything needed for the record and for verification.
struct MeasuredRun {
    root_uri: String,
    branches: Vec<String>,
    setup: serde_json::Value,
    warmup_elapsed_us: u64,
    /// The measured window: flip to `PHASE_MEASURED` until `PHASE_STOP`.
    window_us: u64,
    /// `PHASE_STOP` until the last worker joined: ops in flight at the stop.
    drain_us: u64,
    /// `mutation_reprepares` taken after the measured flip.
    reprepares: u64,
    outcomes: Vec<WorkerOutcome>,
    lance_manifest: LancePlaneTotals,
    lance_table: LancePlaneTotals,
    control_delta: Option<serde_json::Value>,
    probe_counters: serde_json::Value,
    pre_measure_peak_rss: u64,
}

pub(super) async fn run(args: &Args) -> serde_json::Value {
    // ---- Target root -----------------------------------------------------
    // Local: a tempdir owned for the child's lifetime. S3: a unique prefix
    // under --target-uri, probed for reachability BEFORE any fixture work so
    // a bad endpoint or credential set is a refusal (78), not a mid-run
    // panic.
    let mut local_dir: Option<tempfile::TempDir> = None;
    let (root_uri, target_backend) = match &args.target_uri {
        None => {
            let dir = tempfile::tempdir().expect("tempdir");
            let uri = dir.path().to_str().expect("utf8 tempdir").to_string();
            local_dir = Some(dir);
            (uri, "local-fs")
        }
        Some(base) => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let run_root = format!("{}/cw-{nanos}", base.trim_end_matches('/'));
            let probe_uri = format!("{run_root}/__cw_probe");
            let reachable = async {
                let storage = storage_for_uri(&run_root).map_err(|e| e.to_string())?;
                storage
                    .write_text(&probe_uri, "concurrent-writes probe")
                    .await
                    .map_err(|e| e.to_string())?;
                storage.delete(&probe_uri).await.map_err(|e| e.to_string())
            }
            .await;
            if let Err(error) = reachable {
                eprintln!(
                    "refusing --target-uri '{base}': the store is not usable from this \
                     environment ({error}); set the AWS_* variables the deployment guide \
                     documents (endpoint, credentials, path style)"
                );
                std::process::exit(78);
            }
            (run_root, "s3")
        }
    };

    // ---- Watchdog --------------------------------------------------------
    let watchdog_budget = Duration::from_secs(args.warmup_secs + args.duration_secs)
        .saturating_add(WATCHDOG_SLACK)
        .saturating_mul(2);
    let watchdog = tokio::spawn(async move {
        tokio::time::sleep(watchdog_budget).await;
        eprintln!(
            "concurrent-writes watchdog: run exceeded {}s; terminating",
            watchdog_budget.as_secs()
        );
        std::process::exit(75);
    });

    // ---- Fixture (outside every timer) ----------------------------------
    let setup_started = Instant::now();
    let schema = rfc023_scenarios::graph_schema(args.dims);
    let db = Session::from_defaults(
        Arc::new(
            Omnigraph::init(&root_uri, &schema)
                .await
                .expect("initialize concurrent-writes fixture"),
        ),
        SessionSettings::default(),
    );
    let patterns = rfc023_scenarios::vector_json_patterns(args.dims, args.seed);
    let plan =
        rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows).expect("validated shape");
    rfc023_scenarios::load_graph_rows(
        &db,
        "main",
        "base",
        args.rows,
        plan.batch_rows,
        &patterns,
        LoadMode::Append,
    )
    .await;
    let aging = rfc023_scenarios::age_fixture(&db, args).await;
    let branches: Vec<String> = if args.write_branches == 1 {
        vec!["main".to_string()]
    } else {
        let mut forks = Vec::with_capacity(args.write_branches as usize);
        for fork in 0..args.write_branches {
            let name = format!("cw-{fork}");
            db.branch_create(&name).await.expect("create writer branch");
            forks.push(name);
        }
        forks
    };
    let layout = super::fixture_controls::prepare_layout(&root_uri, args).await;
    drop(db);
    let setup = serde_json::json!({
        "setup_wall_us": setup_started.elapsed().as_micros() as u64,
        "seeded_rows": args.rows,
        "aging": aging,
        "manifest_layout": layout,
    });

    // ---- Measured region under one probes value --------------------------
    // The pre-generated worker vectors: parsed once, shared by Arc.
    let vectors: Arc<Vec<Vec<f32>>> = Arc::new(
        (0..patterns.len())
            .map(|pattern| {
                serde_json::from_str::<Vec<f32>>(&format!("[{}]", patterns[pattern]))
                    .expect("pattern vector json")
            })
            .collect(),
    );
    let insert_src = Arc::new(insert_source(args.dims));

    let manifest_tracker = IOTracker::default();
    let table_tracker = IOTracker::default();
    let shared_probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest_tracker.clone()) as Arc<dyn WrappingObjectStore>),
        table_wrapper: Some(Arc::new(table_tracker.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };

    let measured = run_measured(
        args,
        root_uri.clone(),
        branches.clone(),
        setup,
        vectors,
        insert_src,
        manifest_tracker,
        table_tracker,
        shared_probes,
    )
    .await;

    // ---- Judge + summarize (no probes, counters already read) ------------
    let record = judge_and_summarize(args, target_backend, &measured).await;

    // ---- Teardown --------------------------------------------------------
    watchdog.abort();
    if target_backend == "s3" && !args.keep_fixture {
        if let Ok(storage) = storage_for_uri(&measured.root_uri) {
            if let Err(error) = storage.delete_prefix(&measured.root_uri).await {
                eprintln!(
                    "concurrent-writes teardown: could not delete '{}': {error}",
                    measured.root_uri
                );
            }
        }
    }
    drop(local_dir);
    record
}

#[allow(clippy::too_many_arguments)]
async fn run_measured(
    args: &Args,
    root_uri: String,
    branches: Vec<String>,
    setup: serde_json::Value,
    vectors: Arc<Vec<Vec<f32>>>,
    insert_src: Arc<String>,
    manifest_tracker: IOTracker,
    table_tracker: IOTracker,
    shared_probes: QueryIoProbes,
) -> MeasuredRun {
    let no_probes = args.no_probes;
    let writers = args.writers;
    let warmup = Duration::from_secs(args.warmup_secs);
    let duration = Duration::from_secs(args.duration_secs);
    let probes_for_main = shared_probes.clone();
    let scope_probes = shared_probes.clone();

    let body = async move {
        // The counting reopen: control plane through CountingStorageAdapter,
        // Lance planes through the probes this future runs under, so the
        // coordinator's `__manifest` handle is wrapped from birth.
        let (engine, control_counts): (Omnigraph, Option<Arc<StorageReadCounts>>) = if no_probes {
            (
                Omnigraph::open(&root_uri).await.expect("open fixture"),
                None,
            )
        } else {
            let storage = storage_for_uri(&root_uri).expect("storage for fixture root");
            let (counting, counts) = CountingStorageAdapter::new(storage);
            (
                Omnigraph::open_with_storage(&root_uri, counting)
                    .await
                    .expect("open fixture under counting adapter"),
                Some(counts),
            )
        };
        let session = Session::from_defaults(Arc::new(engine), SessionSettings::default());
        let pre_measure_peak_rss = current_process_peak_rss_bytes();

        let phase = Arc::new(AtomicU8::new(PHASE_WARMUP));
        let run_start = Instant::now();
        let mut handles = Vec::with_capacity(writers);
        for worker in 0..writers {
            let session = session.clone();
            let phase = Arc::clone(&phase);
            let vectors = Arc::clone(&vectors);
            let insert_src = Arc::clone(&insert_src);
            let branch = branches[worker % branches.len()].clone();
            let worker_probes = shared_probes.clone();
            let worker_body = async move {
                let mut samples: Vec<(u64, u64)> = Vec::with_capacity(4096);
                let mut acked_ops = 0u64;
                let mut warmup_ops = 0u64;
                let mut authority_conflicts = 0u64;
                let mut failure = None;
                'ops: loop {
                    let current_phase = phase.load(Ordering::Acquire);
                    if current_phase == PHASE_STOP {
                        break;
                    }
                    let slug = format!("cw-w{worker:02}-{acked_ops:010}");
                    let mut params = helpers::params(&[("$slug", slug.as_str())]);
                    params.extend(helpers::vector_param(
                        "$v",
                        &vectors[(acked_ops as usize) % vectors.len()],
                    ));
                    let start_offset = run_start.elapsed().as_micros() as u64;
                    let op_started = Instant::now();
                    loop {
                        match session.mutate(&branch, &insert_src, "put", &params).await {
                            Ok(_) => {
                                let service_time = op_started.elapsed().as_micros() as u64;
                                acked_ops += 1;
                                if current_phase == PHASE_MEASURED {
                                    samples.push((start_offset, service_time));
                                } else {
                                    warmup_ops += 1;
                                }
                                break;
                            }
                            // The engine already replayed this op up to
                            // `MAX_PRE_EFFECT_REPREPARES` times; this arm
                            // sees only the exhaustion of that loop. No
                            // graph-visible effect precedes the refusal (the
                            // staged files are reclaimable orphans), so
                            // re-issuing the same slug is safe — a duplicate
                            // would surface as a KeyConflict and invalidate
                            // the run.
                            Err(op_error) if is_authority_conflict(&op_error) => {
                                authority_conflicts += 1;
                                if phase.load(Ordering::Acquire) == PHASE_STOP {
                                    // The run is over; abandon the unacked op
                                    // rather than spinning past the stop.
                                    break 'ops;
                                }
                            }
                            Err(op_error) => {
                                failure = Some(WorkerFailure::classify(&op_error));
                                break 'ops;
                            }
                        }
                    }
                }
                WorkerOutcome {
                    worker,
                    branch,
                    acked_ops,
                    warmup_ops,
                    authority_conflicts,
                    samples,
                    failure,
                }
            };
            handles.push(if no_probes {
                tokio::spawn(worker_body)
            } else {
                // The probes are a task_local; a spawned task starts outside
                // the parent's scope, so every worker re-enters it with the
                // SAME shared trackers.
                tokio::spawn(with_query_io_probes(worker_probes, Box::pin(worker_body)))
            });
        }

        // Timeline: warmup, then the measured window with per-second tracker
        // drains (get-and-reset; an unbounded run must not let the trackers'
        // request logs grow), then stop.
        let mut lance_manifest = LancePlaneTotals::default();
        let mut lance_table = LancePlaneTotals::default();
        tokio::time::sleep(warmup).await;
        // Discard warmup-window IO so the measured totals start clean; the
        // tails of warmup-started ops still land in the measured totals and
        // the record says so.
        LancePlaneTotals::default().drain_from(&manifest_tracker, &probes_for_main.manifest_stores);
        LancePlaneTotals::default().drain_from(&table_tracker, &probes_for_main.table_stores);
        let warmup_elapsed_us = run_start.elapsed().as_micros() as u64;
        let control_before = control_counts.as_deref().map(ControlSnapshot::capture);
        let reprepares_before = probes_for_main.mutation_reprepares.load(Ordering::Relaxed);
        phase.store(PHASE_MEASURED, Ordering::Release);
        let measured_started = Instant::now();
        while measured_started.elapsed() < duration {
            let remaining = duration.saturating_sub(measured_started.elapsed());
            tokio::time::sleep(remaining.min(Duration::from_secs(1))).await;
            lance_manifest.drain_from(&manifest_tracker, &probes_for_main.manifest_stores);
            lance_table.drain_from(&table_tracker, &probes_for_main.table_stores);
        }
        phase.store(PHASE_STOP, Ordering::Release);
        let window_us = measured_started.elapsed().as_micros() as u64;
        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            outcomes.push(handle.await.expect("worker task join"));
        }
        let drain_us = (measured_started.elapsed().as_micros() as u64).saturating_sub(window_us);
        let reprepares = probes_for_main
            .mutation_reprepares
            .load(Ordering::Relaxed)
            .saturating_sub(reprepares_before);
        lance_manifest.drain_from(&manifest_tracker, &probes_for_main.manifest_stores);
        lance_table.drain_from(&table_tracker, &probes_for_main.table_stores);
        let control_delta = match (&control_before, control_counts.as_deref()) {
            (Some(before), Some(counts)) => {
                Some(before.delta_json(&ControlSnapshot::capture(counts)))
            }
            _ => None,
        };
        let probe_counters = serde_json::json!({
            "data_open_count": probes_for_main.data_open_count.load(Ordering::Relaxed),
            "internal_open_count": probes_for_main.internal_open_count.load(Ordering::Relaxed),
            "manifest_scan_count": probes_for_main.manifest_scan_count.load(Ordering::Relaxed),
            "mutation_reprepares_since_open": probes_for_main.mutation_reprepares.load(Ordering::Relaxed),
        });
        MeasuredRun {
            root_uri,
            branches,
            setup,
            warmup_elapsed_us,
            window_us,
            drain_us,
            reprepares,
            outcomes,
            lance_manifest,
            lance_table,
            control_delta,
            probe_counters,
            pre_measure_peak_rss,
        }
    };

    if no_probes {
        body.await
    } else {
        with_query_io_probes(scope_probes, Box::pin(body)).await
    }
}

async fn judge_and_summarize(
    args: &Args,
    target_backend: &str,
    measured: &MeasuredRun,
) -> serde_json::Value {
    // ---- Aggregate the workers ------------------------------------------
    let mut errors: Vec<String> = Vec::new();
    let mut key_conflicts = 0u64;
    let mut authority_conflicts = 0u64;
    let mut warmup_ops = 0u64;
    let mut ops_committed = 0u64;
    let mut per_worker = Vec::with_capacity(measured.outcomes.len());
    let mut per_branch_acked: std::collections::BTreeMap<&str, u64> =
        std::collections::BTreeMap::new();
    let mut service_times: Vec<u64> = Vec::new();
    let mut commits_by_second = vec![0u64; args.duration_secs as usize];
    let mut completed_in_drain = 0u64;
    for outcome in &measured.outcomes {
        if let Some(failure) = &outcome.failure {
            if matches!(failure, WorkerFailure::KeyConflict(_)) {
                key_conflicts += 1;
            }
            errors.push(format!("worker {}: {}", outcome.worker, failure.message()));
        }
        for (start_offset, service) in &outcome.samples {
            let completed_us = (start_offset + service).saturating_sub(measured.warmup_elapsed_us);
            match commits_by_second.get_mut((completed_us / 1_000_000) as usize) {
                Some(second) if completed_us < measured.window_us => *second += 1,
                _ => completed_in_drain += 1,
            }
        }
        warmup_ops += outcome.warmup_ops;
        authority_conflicts += outcome.authority_conflicts;
        ops_committed += outcome.samples.len() as u64;
        *per_branch_acked.entry(outcome.branch.as_str()).or_default() += outcome.acked_ops;
        per_worker.push(serde_json::json!({
            "worker": outcome.worker,
            "branch": outcome.branch,
            "acked_ops": outcome.acked_ops,
            "measured_ops": outcome.samples.len(),
            "authority_conflicts": outcome.authority_conflicts,
        }));
        service_times.extend(outcome.samples.iter().map(|(_, service)| *service));
    }
    service_times.sort_unstable();
    // Samples are ops that STARTED inside the window, so the window (not
    // window plus drain) is their denominator.
    let window_secs = (measured.window_us as f64 / 1_000_000.0).max(f64::EPSILON);
    let commits_per_sec = ops_committed as f64 / window_secs;
    let mean = if service_times.is_empty() {
        0
    } else {
        service_times.iter().sum::<u64>() / service_times.len() as u64
    };

    // ---- Verification on a fresh, uncounted handle -----------------------
    // Every acknowledged write must be durable and visible: per-branch row
    // counts are exact, and a sample of acknowledged slugs reads back by
    // key. Aging restores the logical base fixture, so the expectation is
    // seeded rows plus this run's acknowledgements.
    let mut verification_failures: Vec<VerificationFailure> = Vec::new();
    let mut sampled_readback = 0usize;
    let verify = Session::from_defaults(
        Arc::new(
            Omnigraph::open(&measured.root_uri)
                .await
                .expect("open fixture for verification"),
        ),
        SessionSettings::default(),
    );
    let readback_src =
        "query get($slug: String) { match { $c: Chunk { slug: $slug } } return { $c.slug } }";
    for branch in &measured.branches {
        let acked: u64 = measured
            .outcomes
            .iter()
            .filter(|outcome| &outcome.branch == branch)
            .map(|outcome| outcome.acked_ops)
            .sum();
        let expected = args.rows + acked as usize;
        let actual = helpers::count_rows_branch(&verify, branch, "node:Chunk").await;
        if actual != expected {
            verification_failures.push(VerificationFailure::RowCount(format!(
                "branch {branch}: expected {expected} Chunk rows (seeded {} + acked {acked}), found {actual}",
                args.rows
            )));
        }
        // The last acknowledged slug of up to 8 workers per branch reads
        // back by key: the freshest write is the one a lost-durability bug
        // would most plausibly drop.
        for outcome in measured
            .outcomes
            .iter()
            .filter(|outcome| &outcome.branch == branch && outcome.acked_ops > 0)
            .take(8)
        {
            let sample_seq = outcome.acked_ops - 1;
            let slug = format!("cw-w{:02}-{sample_seq:010}", outcome.worker);
            let params = helpers::params(&[("$slug", slug.as_str())]);
            match verify
                .query(ReadTarget::branch(branch), readback_src, "get", &params)
                .await
            {
                Ok(result) if result.num_rows() == 1 => {}
                Ok(result) => verification_failures.push(VerificationFailure::Readback(format!(
                    "branch {branch}: acknowledged slug '{slug}' read back {} rows, expected 1",
                    result.num_rows()
                ))),
                Err(error) => verification_failures.push(VerificationFailure::Readback(format!(
                    "branch {branch}: readback of acknowledged slug '{slug}' failed: {error}"
                ))),
            }
            sampled_readback += 1;
        }
    }
    drop(verify);

    let per_branch: Vec<serde_json::Value> = measured
        .branches
        .iter()
        .map(|branch| {
            serde_json::json!({
                "branch": branch,
                "acked_ops": per_branch_acked.get(branch.as_str()).copied().unwrap_or(0),
                "workers": measured
                    .outcomes
                    .iter()
                    .filter(|outcome| &outcome.branch == branch)
                    .count(),
            })
        })
        .collect();

    let passed = errors.is_empty() && verification_failures.is_empty();
    let rows_exact = !verification_failures
        .iter()
        .any(|failure| matches!(failure, VerificationFailure::RowCount(_)));
    let verification_messages: Vec<&str> = verification_failures
        .iter()
        .map(VerificationFailure::message)
        .collect();
    let io_coverage = "counts are read from each probed Lance ObjectStore's own io_tracker, which \
                       also sees the direct local reader and writer a WrappingObjectStore misses \
                       on local-fs; one put per file written, one read per range get";
    let per_op = |total: u64| -> f64 {
        if ops_committed == 0 {
            0.0
        } else {
            total as f64 / ops_committed as f64
        }
    };
    let record = serde_json::json!({
        "routing": "production-session-mutate",
        "production_path": true,
        "driver": "closed-loop",
        "claim_grade": false,
        "claim_note": "closed-loop driver; subject to coordinated omission (RFC 0039 Rule 1); \
                       claim-grade throughput/latency requires the future open-loop declarative kind",
        "target_backend": target_backend,
        "target_root": measured.root_uri,
        "branch_mode": if measured.branches.len() == 1 { "main" } else { "forks" },
        "setup": measured.setup,
        "warmup_ops": warmup_ops,
        "warmup_elapsed_us": measured.warmup_elapsed_us,
        "ops_committed": ops_committed,
        "window_us": measured.window_us,
        "drain_us": measured.drain_us,
        "commits_per_sec": commits_per_sec,
        "commits_by_second": commits_by_second,
        "completed_in_drain": completed_in_drain,
        "service_time_us": {
            "min": service_times.first().copied().unwrap_or(0),
            "mean": mean,
            "p50": nearest_rank(&service_times, 0.50),
            "p95": nearest_rank(&service_times, 0.95),
            "p99": nearest_rank(&service_times, 0.99),
            "max": service_times.last().copied().unwrap_or(0),
            "samples": service_times.len(),
        },
        "per_branch": per_branch,
        "per_worker": per_worker,
        "errors": errors,
        "key_conflicts": key_conflicts,
        "authority_conflicts": authority_conflicts,
        "reprepares": if args.no_probes { serde_json::json!(null) } else { serde_json::json!(measured.reprepares) },
        "retry_note": "`Omnigraph::mutate` replays a typed read-set/authority conflict \
                       (`ReadSetChanged`, e.g. graph_head moved under a concurrent \
                       writer) itself, up to MAX_PRE_EFFECT_REPREPARES times for an \
                       insert-only mutation; `reprepares` counts those replays inside \
                       the measured window. `authority_conflicts` counts only the \
                       exhaustions of that loop, which the driver retries like a real \
                       client. Both ride inside the op's service time",
        "io": if args.no_probes { serde_json::json!(null) } else { serde_json::json!({
            "manifest": {
                "read_iops": measured.lance_manifest.read_iops,
                "read_bytes": measured.lance_manifest.read_bytes,
                "write_iops": measured.lance_manifest.write_iops,
                "write_bytes": measured.lance_manifest.write_bytes,
            },
            "table": {
                "read_iops": measured.lance_table.read_iops,
                "read_bytes": measured.lance_table.read_bytes,
                "write_iops": measured.lance_table.write_iops,
                "write_bytes": measured.lance_table.write_bytes,
            },
            "probes": measured.probe_counters,
            "control_plane": measured.control_delta,
            "per_op": {
                "manifest_reads_per_commit": per_op(measured.lance_manifest.read_iops),
                "manifest_writes_per_commit": per_op(measured.lance_manifest.write_iops),
                "table_reads_per_commit": per_op(measured.lance_table.read_iops),
                "table_writes_per_commit": per_op(measured.lance_table.write_iops),
                "reprepares_per_commit": per_op(measured.reprepares),
            },
            "counter_semantics": "logical calls recorded by the Lance ObjectStore tracker (one per \
                                  range get, one per file put), not physical requests and not \
                                  round trips",
            "io_coverage": io_coverage,
            "probe_coverage": "task-local probes installed on the counting open and every \
                               worker task; wrappers attach at dataset open, so IO on wrapped \
                               handles is counted regardless of thread; measured totals start \
                               at the window flip and include the tails of warmup-started ops",
        })},
        "probes_installed": !args.no_probes,
        "attestation": {
            "enabled_engine_cargo_features": enabled_engine_cargo_features(),
            "lance_mem_pool_size_env": std::env::var("LANCE_MEM_POOL_SIZE").ok(),
            "rustflags_env": std::env::var("RUSTFLAGS").ok(),
            "tokio_unstable_cfg": tokio_unstable_cfg(),
            "tokio_worker_threads": tokio::runtime::Handle::current().metrics().num_workers(),
        },
        "operation_pre_peak_rss_bytes": measured.pre_measure_peak_rss,
        "operation_post_peak_rss_bytes": current_process_peak_rss_bytes(),
        "rss_boundary": "single child; the parent's wait4 peak includes fixture seeding",
        "measurement_boundary": "timer wraps each Session::mutate acknowledgement inside the \
                                 measured window; fixture build, aging, layout preparation, \
                                 verification and teardown are outside it",
        "verification": {
            "rows_exact": rows_exact,
            "sampled_readback": sampled_readback,
            "failures": verification_messages,
            "passed": passed,
        },
    });
    assert!(
        passed,
        "concurrent-writes run invalid: errors={errors:?} verification={verification_messages:?}"
    );
    record
}
