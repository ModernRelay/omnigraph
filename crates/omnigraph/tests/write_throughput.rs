//! Concurrent-write throughput instrument (research, not a CI gate).
//!
//! N closed-loop writers over ONE `Omnigraph` handle (the server shape: one
//! engine per graph, a session per request), each inserting one small row per
//! mutation with disjoint keys, on the same branch (`WT_MODE=same`) or one
//! branch per writer (`WT_MODE=separate`). Writer `i` writes node type
//! `C{i % WT_TYPES}`, so `WT_TYPES=1` puts every writer on one table and a
//! larger value spreads them over disjoint tables.
//!
//! Every object-store request the engine makes through Lance is counted by
//! method and plane (`__manifest`, `__history`, data tables) with a counting
//! `WrappingObjectStore` installed through `QueryIoProbes` on the graph open
//! and on every writer task. On `s3://` every request reaches the wrapper; on
//! `file://` Lance's direct local reader/writer bypass it, so local counts are
//! a lower bound and the S3 counts are the request costs.
//!
//! The driver is closed-loop (each writer issues its next write after the
//! previous acknowledgement), so results are diagnostic, not claim-grade
//! (RFC 0039 Rule 1). Reported per cell: commits/s inside the measured
//! window, service-time percentiles of operations acknowledged inside it, the
//! longest service time of any operation including those that finished in the
//! drain (starvation shows there), per-writer counts, engine re-prepares,
//! driver-level retries, and requests per commit. After the drain a fresh
//! handle verifies the exact row count of every (branch, table).
//!
//! Configuration (environment):
//! - `WT_BACKEND`   `file` (default) or `s3` (needs `OMNIGRAPH_S3_TEST_BUCKET`
//!   and the `AWS_*` endpoint/credential variables)
//! - `WT_WRITERS`   comma list, default `1,4,16,64`
//! - `WT_MODE`      comma list of `same` / `separate`, default `same`
//! - `WT_TYPES`     tables written, default 1 (the schema always declares 8)
//! - `WT_WARMUP_SECS`, `WT_SECS`  default 3 and 15
//! - `WT_HISTORY`   sequential commits on main before the run, default 32
//! - `WT_REPS`      repetitions per cell, default 1
//! - `WT_LABEL`     build/variant label written into every record
//! - `WT_OUT`       JSONL output path (appended), default stdout only
//! - `WT_ROOT`      parent directory for `file` graphs, default the OS temp dir

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OSResult,
};

use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, phase_timers, with_query_io_probes};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

const TYPES_DECLARED: usize = 8;

fn schema() -> String {
    (0..TYPES_DECLARED)
        .map(|t| format!("node C{t} {{\n    slug: String @key\n    n: I64?\n}}\n"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn queries() -> String {
    (0..TYPES_DECLARED)
        .map(|t| format!("query put{t}($slug: String, $n: I64) {{ insert C{t} {{ slug: $slug, n: $n }} }}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn seed() -> String {
    (0..TYPES_DECLARED)
        .map(|t| format!("{{\"type\": \"C{t}\", \"data\": {{\"slug\": \"seed-{t}\", \"n\": 0}}}}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn put_params(slug: &str, n: i64) -> ParamMap {
    let mut map = ParamMap::new();
    map.insert("slug".to_string(), Literal::String(slug.to_string()));
    map.insert("n".to_string(), Literal::Integer(i128::from(n)));
    map
}

// ---------------------------------------------------------------------------
// Request counting
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MethodCounts {
    get: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    put_create: AtomicU64,
    put_update: AtomicU64,
    multipart: AtomicU64,
    list: AtomicU64,
    list_paginated: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
}

impl MethodCounts {
    fn snapshot(&self) -> BTreeMap<&'static str, u64> {
        let mut m = BTreeMap::new();
        m.insert("get", self.get.load(Ordering::Relaxed));
        m.insert("head", self.head.load(Ordering::Relaxed));
        m.insert("put", self.put.load(Ordering::Relaxed));
        m.insert("put_create", self.put_create.load(Ordering::Relaxed));
        m.insert("put_update", self.put_update.load(Ordering::Relaxed));
        m.insert("multipart", self.multipart.load(Ordering::Relaxed));
        m.insert("list", self.list.load(Ordering::Relaxed));
        m.insert("list_paginated", self.list_paginated.load(Ordering::Relaxed));
        m.insert("delete", self.delete.load(Ordering::Relaxed));
        m.insert("copy", self.copy.load(Ordering::Relaxed));
        m
    }
}

#[derive(Debug, Clone, Default)]
struct Counter(Arc<MethodCounts>);

impl WrappingObjectStore for Counter {
    fn wrap(&self, _prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CountingStore {
            target,
            counts: self.0.clone(),
        })
    }

    fn wrap_paginated(
        &self,
        _prefix: &str,
        original: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        Some(Arc::new(CountingLister {
            target: original,
            counts: self.0.clone(),
        }))
    }
}

struct CountingLister {
    target: Arc<dyn PaginatedListStore>,
    counts: Arc<MethodCounts>,
}

#[async_trait]
impl PaginatedListStore for CountingLister {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> OSResult<PaginatedListResult> {
        self.counts.list_paginated.fetch_add(1, Ordering::Relaxed);
        self.target.list_paginated(prefix, opts).await
    }
}

#[derive(Debug)]
struct CountingStore {
    target: Arc<dyn ObjectStore>,
    counts: Arc<MethodCounts>,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.target)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        let counter = match opts.mode {
            PutMode::Overwrite => &self.counts.put,
            PutMode::Create => &self.counts.put_create,
            PutMode::Update(_) => &self.counts.put_update,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.target.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.counts.multipart.fetch_add(1, Ordering::Relaxed);
        self.target.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        if options.head {
            self.counts.head.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts.get.fetch_add(1, Ordering::Relaxed);
        }
        self.target.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.counts.delete.fetch_add(1, Ordering::Relaxed);
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.counts.copy.fetch_add(1, Ordering::Relaxed);
        self.target.copy_opts(from, to, options).await
    }
}

#[derive(Clone)]
struct Planes {
    manifest: Counter,
    history: Counter,
    table: Counter,
}

impl Planes {
    fn new() -> Self {
        Self {
            manifest: Counter::default(),
            history: Counter::default(),
            table: Counter::default(),
        }
    }

    fn probes(&self) -> QueryIoProbes {
        QueryIoProbes {
            manifest_wrapper: Some(Arc::new(self.manifest.clone())),
            history_wrapper: Some(Arc::new(self.history.clone())),
            table_wrapper: Some(Arc::new(self.table.clone())),
            ..Default::default()
        }
    }

    fn snapshot(&self) -> PlaneSnapshot {
        PlaneSnapshot {
            manifest: self.manifest.0.snapshot(),
            history: self.history.0.snapshot(),
            table: self.table.0.snapshot(),
        }
    }
}

#[derive(Clone)]
struct PlaneSnapshot {
    manifest: BTreeMap<&'static str, u64>,
    history: BTreeMap<&'static str, u64>,
    table: BTreeMap<&'static str, u64>,
}

impl PlaneSnapshot {
    fn delta_per_commit(&self, later: &Self, commits: u64) -> serde_json::Value {
        let plane = |a: &BTreeMap<&'static str, u64>, b: &BTreeMap<&'static str, u64>| {
            let mut out = serde_json::Map::new();
            let mut total = 0u64;
            for (k, v) in b {
                let d = v - a.get(k).copied().unwrap_or(0);
                total += d;
                if d > 0 {
                    out.insert(
                        (*k).to_string(),
                        serde_json::json!(round2(d as f64 / commits.max(1) as f64)),
                    );
                }
            }
            out.insert(
                "total".to_string(),
                serde_json::json!(round2(total as f64 / commits.max(1) as f64)),
            );
            (serde_json::Value::Object(out), total)
        };
        let (m, mt) = plane(&self.manifest, &later.manifest);
        let (h, ht) = plane(&self.history, &later.history);
        let (t, tt) = plane(&self.table, &later.table);
        serde_json::json!({
            "manifest": m,
            "history": h,
            "table": t,
            "total": round2((mt + ht + tt) as f64 / commits.max(1) as f64),
        })
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

const PHASE_WARMUP: u8 = 0;
const PHASE_MEASURED: u8 = 1;
const PHASE_STOP: u8 = 2;

#[derive(Clone)]
struct Config {
    backend: String,
    writers: usize,
    mode: String,
    types: usize,
    warmup: Duration,
    window: Duration,
    history: usize,
    label: String,
    rep: usize,
}

struct Op {
    phase_at_end: u8,
    latency: Duration,
}

struct WriterResult {
    ops: Vec<Op>,
    acknowledged_total: u64,
    driver_retries: u64,
    errors: Vec<String>,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn graph_uri(cfg: &Config) -> (String, Option<tempfile::TempDir>) {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    match cfg.backend.as_str() {
        "s3" => {
            let bucket = std::env::var("OMNIGRAPH_S3_TEST_BUCKET")
                .expect("WT_BACKEND=s3 needs OMNIGRAPH_S3_TEST_BUCKET");
            (
                format!(
                    "s3://{bucket}/wt/{}/w{}-{}-t{}-r{}-{unique}",
                    cfg.label, cfg.writers, cfg.mode, cfg.types, cfg.rep
                ),
                None,
            )
        }
        _ => {
            let root = env_or("WT_ROOT", &std::env::temp_dir().to_string_lossy());
            std::fs::create_dir_all(&root).unwrap();
            let dir = tempfile::Builder::new()
                .prefix("wt-")
                .tempdir_in(root)
                .unwrap();
            (dir.path().to_string_lossy().into_owned(), Some(dir))
        }
    }
}

fn branch_of(cfg: &Config, writer: usize) -> String {
    match cfg.mode.as_str() {
        "separate" => format!("w{writer}"),
        _ => "main".to_string(),
    }
}

fn nearest_rank(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

async fn run_cell(cfg: Config) -> serde_json::Value {
    let (uri, _guard) = graph_uri(&cfg);
    let source = queries();

    // Fixture: tables, branches and history, built on a handle that is dropped
    // before the measured open so every measured handle carries the probes.
    {
        let db = Session::from_defaults(
            Arc::new(Omnigraph::init(&uri, &schema()).await.expect("init")),
            SessionSettings::default(),
        );
        db.load_jsonl(&seed(), LoadMode::Overwrite)
            .await
            .expect("seed");
        for i in 0..cfg.history {
            db.mutate(
                "main",
                &source,
                "put0",
                &put_params(&format!("hist-{i}"), i as i64),
            )
            .await
            .expect("history commit");
        }
        if cfg.mode == "separate" {
            for w in 0..cfg.writers {
                db.branch_create(&format!("w{w}")).await.expect("branch");
            }
        }
    }

    let planes = Planes::new();
    let probes = planes.probes();
    let db = with_query_io_probes(probes.clone(), async {
        Session::from_defaults(
            Arc::new(Omnigraph::open(&uri).await.expect("open")),
            SessionSettings::default(),
        )
    })
    .await;

    let reprepares = probes.mutation_reprepares.clone();
    let carried = probes.writes_carried_over_row_preserving.clone();
    let phase = Arc::new(AtomicU8::new(PHASE_WARMUP));
    let failed = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(cfg.writers);
    for w in 0..cfg.writers {
        let db = db.clone();
        let phase = phase.clone();
        let failed = failed.clone();
        let probes = probes.clone();
        let source = source.clone();
        let branch = branch_of(&cfg, w);
        let query = format!("put{}", w % cfg.types);
        handles.push(tokio::spawn(with_query_io_probes(probes, async move {
            let mut result = WriterResult {
                ops: Vec::new(),
                acknowledged_total: 0,
                driver_retries: 0,
                errors: Vec::new(),
            };
            let mut seq: u64 = 0;
            while phase.load(Ordering::Acquire) != PHASE_STOP && !failed.load(Ordering::Relaxed)
            {
                let slug = format!("w{w}-{seq}");
                let started = Instant::now();
                loop {
                    match db
                        .mutate(&branch, &source, &query, &put_params(&slug, seq as i64))
                        .await
                    {
                        Ok(_) => break,
                        Err(e) if e.is_read_set_changed() => {
                            // The engine's bounded re-prepare loop gave up; a
                            // real client resubmits.
                            result.driver_retries += 1;
                        }
                        Err(e) => {
                            result.errors.push(format!("{e}"));
                            failed.store(true, Ordering::Relaxed);
                            return result;
                        }
                    }
                }
                result.ops.push(Op {
                    phase_at_end: phase.load(Ordering::Acquire),
                    latency: started.elapsed(),
                });
                result.acknowledged_total += 1;
                seq += 1;
            }
            result
        })));
    }

    tokio::time::sleep(cfg.warmup).await;
    let phases_before = phase_timers::snapshot();
    let events_before = phase_timers::events();
    let before = planes.snapshot();
    let reprepares_before = reprepares.load(Ordering::Relaxed);
    let carried_before = carried.load(Ordering::Relaxed);
    phase.store(PHASE_MEASURED, Ordering::Release);
    let window_started = Instant::now();
    tokio::time::sleep(cfg.window).await;
    phase.store(PHASE_STOP, Ordering::Release);
    let window = window_started.elapsed();
    let after = planes.snapshot();
    let phases_after = phase_timers::snapshot();
    let events_after = phase_timers::events();
    let reprepares_in_window = reprepares.load(Ordering::Relaxed) - reprepares_before;
    let carried_in_window = carried.load(Ordering::Relaxed) - carried_before;

    let drain_started = Instant::now();
    let mut writers = Vec::with_capacity(cfg.writers);
    for h in handles {
        writers.push(h.await.expect("writer task panicked"));
    }
    let drain = drain_started.elapsed();

    let errors: Vec<String> = writers.iter().flat_map(|w| w.errors.clone()).collect();
    let mut latencies: Vec<u64> = Vec::new();
    let mut per_writer: Vec<u64> = Vec::new();
    let mut max_any_us: u64 = 0;
    for w in &writers {
        let mut count = 0u64;
        for op in &w.ops {
            let us = op.latency.as_micros() as u64;
            max_any_us = max_any_us.max(us);
            if op.phase_at_end == PHASE_MEASURED {
                latencies.push(us);
                count += 1;
            }
        }
        per_writer.push(count);
    }
    latencies.sort_unstable();
    let commits = latencies.len() as u64;
    let driver_retries: u64 = writers.iter().map(|w| w.driver_retries).sum();

    // Verification on a fresh, uncounted handle: exact rows per (branch, table).
    let verified = if errors.is_empty() {
        let check = Omnigraph::open(&uri).await.expect("verify open");
        let mut expected: BTreeMap<(String, usize), u64> = BTreeMap::new();
        for (w, result) in writers.iter().enumerate() {
            *expected
                .entry((branch_of(&cfg, w), w % cfg.types))
                .or_insert(0) += result.acknowledged_total;
        }
        let mut ok = true;
        for ((branch, t), acked) in &expected {
            let base = 1 + if t == &0 { cfg.history as u64 } else { 0 };
            let snap = check
                .snapshot_of(ReadTarget::branch(branch.as_str()))
                .await
                .expect("verify snapshot");
            let ds = snap
                .open_dataset(&format!("node:C{t}"))
                .await
                .expect("verify dataset");
            let rows = ds.count_rows(None).await.expect("count") as u64;
            if rows != base + acked {
                eprintln!(
                    "VERIFY MISMATCH branch={branch} table=C{t}: rows={rows} expected={}",
                    base + acked
                );
                ok = false;
            }
        }
        ok
    } else {
        false
    };

    let mut phases = serde_json::Map::new();
    for ((name, c0, t0), (_, c1, t1)) in phases_before.iter().zip(phases_after.iter()) {
        let n = c1 - c0;
        let us = t1 - t0;
        if n == 0 {
            continue;
        }
        phases.insert(
            (*name).to_string(),
            serde_json::json!({
                "per_commit": round2(n as f64 / commits.max(1) as f64),
                "ms_each": round2(us as f64 / n as f64 / 1000.0),
                "ms_per_commit": round2(us as f64 / commits.max(1) as f64 / 1000.0),
            }),
        );
    }
    let mut events = serde_json::Map::new();
    for ((name, a), (_, b)) in events_before.iter().zip(events_after.iter()) {
        if b > a {
            events.insert((*name).to_string(), serde_json::json!(b - a));
        }
    }
    let min_w = per_writer.iter().copied().min().unwrap_or(0);
    let max_w = per_writer.iter().copied().max().unwrap_or(0);
    let record = serde_json::json!({
        "label": cfg.label,
        "backend": cfg.backend,
        "writers": cfg.writers,
        "mode": cfg.mode,
        "types": cfg.types,
        "rep": cfg.rep,
        "history": cfg.history,
        "window_s": round2(window.as_secs_f64()),
        "drain_s": round2(drain.as_secs_f64()),
        "commits": commits,
        "commits_per_s": round2(commits as f64 / window.as_secs_f64()),
        "p50_ms": round2(nearest_rank(&latencies, 0.50) as f64 / 1000.0),
        "p90_ms": round2(nearest_rank(&latencies, 0.90) as f64 / 1000.0),
        "p99_ms": round2(nearest_rank(&latencies, 0.99) as f64 / 1000.0),
        "max_ms": round2(latencies.last().copied().unwrap_or(0) as f64 / 1000.0),
        "max_any_incl_drain_ms": round2(max_any_us as f64 / 1000.0),
        "per_writer_min": min_w,
        "per_writer_max": max_w,
        "per_writer": per_writer,
        "reprepares_per_commit": round2(reprepares_in_window as f64 / commits.max(1) as f64),
        "carried_per_commit": round2(carried_in_window as f64 / commits.max(1) as f64),
        "driver_retries": driver_retries,
        "requests_per_commit": before.delta_per_commit(&after, commits),
        "phases": serde_json::Value::Object(phases),
        "events": serde_json::Value::Object(events),
        "verified": verified,
        "errors": errors.iter().take(3).collect::<Vec<_>>(),
        "driver": "closed-loop",
        "claim_grade": false,
    });
    drop(db);
    if cfg.backend == "s3" {
        // Leave the prefix; RustFS data lives on a scratch disk.
    }
    record
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "instrument: concurrent write throughput sweep (research); run with --ignored and WT_* env"]
async fn write_throughput_sweep() {
    let backend = env_or("WT_BACKEND", "file");
    let writers: Vec<usize> = env_or("WT_WRITERS", "1,4,16,64")
        .split(',')
        .map(|s| s.trim().parse().expect("WT_WRITERS"))
        .collect();
    let modes: Vec<String> = env_or("WT_MODE", "same")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let types: usize = env_or("WT_TYPES", "1").parse().expect("WT_TYPES");
    assert!((1..=TYPES_DECLARED).contains(&types));
    let warmup = Duration::from_secs_f64(env_or("WT_WARMUP_SECS", "3").parse().unwrap());
    let window = Duration::from_secs_f64(env_or("WT_SECS", "15").parse().unwrap());
    let history: usize = env_or("WT_HISTORY", "32").parse().unwrap();
    let reps: usize = env_or("WT_REPS", "1").parse().unwrap();
    let label = env_or("WT_LABEL", "unlabeled");
    let out = std::env::var("WT_OUT").ok();

    for rep in 0..reps {
        for mode in &modes {
            for &w in &writers {
                let cfg = Config {
                    backend: backend.clone(),
                    writers: w,
                    mode: mode.clone(),
                    types,
                    warmup,
                    window,
                    history,
                    label: label.clone(),
                    rep,
                };
                let record = run_cell(cfg).await;
                let line = serde_json::to_string(&record).unwrap();
                println!("WT {line}");
                if let Some(path) = &out {
                    use std::io::Write;
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .unwrap();
                    writeln!(f, "{line}").unwrap();
                }
            }
        }
    }
}
