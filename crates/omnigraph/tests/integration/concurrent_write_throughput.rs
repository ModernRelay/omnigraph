//! Concurrent write throughput instrument (research, `research/write-throughput`).
//!
//! N closed-loop writers over ONE `Arc<Omnigraph>` (the server shape: one
//! engine handle per graph, a session per request) insert small rows with
//! disjoint keys, either all on `main` (`same`) or each on its own branch
//! (`separate`). Each cell builds a fresh graph, warms up, then measures a
//! fixed window:
//!
//! - acknowledged writes per second and distinct graph commits per second
//!   (they differ only when several writes share one publication);
//! - service-time percentiles of the writes acknowledged in the window, and
//!   the longest service time of any write, drain included, so a starved
//!   writer is visible;
//! - per-writer acknowledged counts;
//! - object-store requests per acknowledged write, by method, counted by a
//!   wrapper on every Lance store the writers open (complete on S3; on
//!   `file://` Lance's direct local reader/writer bypass wrappers, so the
//!   store trackers' totals are reported beside them);
//! - the engine's internal re-prepares and the driver's own retries.
//!
//! Closed-loop driving finds the ceiling and is subject to coordinated
//! omission (RFC 0039 Rule 1): these are decision instruments, not claims.
//! Verification (exact row counts per branch, sampled read-back) runs on a
//! fresh handle after the clocks stop; a mismatch fails the run.
//!
//! Configuration is read from the environment so the same binary measures
//! every variant:
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `OG_CW_TARGET` | `local` | `local` (tempdir) or `s3://bucket/prefix` |
//! | `OG_CW_WRITERS` | `1,4,16,64` | writer counts to sweep |
//! | `OG_CW_MODES` | `same,separate` | branch layouts to sweep |
//! | `OG_CW_WARMUP_SECS` | `3` | warm-up before the window |
//! | `OG_CW_DURATION_SECS` | `20` | measured window |
//! | `OG_CW_RUNS` | `1` | repetitions per cell |
//! | `OG_CW_SEED_ROWS` | `64` | rows loaded before the run |
//! | `OG_CW_TOKIO_WORKERS` | `8` | runtime worker threads |
//! | `OG_CW_LABEL` | `unlabeled` | variant name written to every record |
//! | `OG_CW_OUT` | unset | append one JSON line per cell here |
//! | `OG_CW_PROBES` | `1` | `0` disables request counting |
//! | `OG_CW_CLAIM` | `0` | `1` claims `main`'s writer epoch (same-branch cells) |
//!
//! ```bash
//! OG_CW_WRITERS=1,16 OG_CW_MODES=same cargo test -p omnigraph-engine \
//!   --test integration concurrent_write_throughput -- --ignored --nocapture
//! ```
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OSResult,
};

use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{ProbedStores, QueryIoProbes, with_query_io_probes};
use omnigraph::settings::SessionSettings;

use super::helpers::mixed_params;

const SCHEMA: &str = "node Item {\n    slug: String @key\n    n: I64\n}\n";
const QUERIES: &str = "query put($slug: String, $n: I64) {\n    insert Item { slug: $slug, n: $n }\n}\n";
const TABLE: &str = "node:Item";

const PHASE_WARMUP: u8 = 0;
const PHASE_MEASURED: u8 = 1;
const PHASE_STOP: u8 = 2;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_num(name: &str, default: u64) -> u64 {
    env_or(name, &default.to_string())
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a number"))
}

#[derive(Clone, Debug)]
struct Config {
    target: String,
    writers: Vec<usize>,
    modes: Vec<String>,
    warmup: Duration,
    duration: Duration,
    runs: u64,
    seed_rows: u64,
    label: String,
    out: Option<String>,
    probes: bool,
    tokio_workers: usize,
}

impl Config {
    fn from_env() -> Self {
        let writers = env_or("OG_CW_WRITERS", "1,4,16,64")
            .split(',')
            .map(|w| w.trim().parse().expect("OG_CW_WRITERS is a comma list"))
            .collect();
        let modes = env_or("OG_CW_MODES", "same,separate")
            .split(',')
            .map(|m| m.trim().to_string())
            .collect::<Vec<_>>();
        for mode in &modes {
            assert!(
                mode == "same" || mode == "separate",
                "OG_CW_MODES takes same and separate, got {mode}"
            );
        }
        Self {
            target: env_or("OG_CW_TARGET", "local"),
            writers,
            modes,
            warmup: Duration::from_secs(env_num("OG_CW_WARMUP_SECS", 3)),
            duration: Duration::from_secs(env_num("OG_CW_DURATION_SECS", 20)),
            runs: env_num("OG_CW_RUNS", 1),
            seed_rows: env_num("OG_CW_SEED_ROWS", 64),
            label: env_or("OG_CW_LABEL", "unlabeled"),
            out: std::env::var("OG_CW_OUT").ok(),
            probes: env_or("OG_CW_PROBES", "1") != "0",
            tokio_workers: env_num("OG_CW_TOKIO_WORKERS", 8) as usize,
        }
    }
}

// ── Request counting ─────────────────────────────────────────────────────

#[derive(Default, Debug)]
struct Counts {
    get: AtomicU64,
    get_range: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    put_create: AtomicU64,
    put_update: AtomicU64,
    put_multipart: AtomicU64,
    list: AtomicU64,
    list_delimiter: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
}

#[derive(Default, Debug, Clone, Copy)]
struct CountSnapshot {
    get: u64,
    get_range: u64,
    head: u64,
    put: u64,
    put_create: u64,
    put_update: u64,
    put_multipart: u64,
    list: u64,
    list_delimiter: u64,
    delete: u64,
    copy: u64,
}

impl Counts {
    fn snapshot(&self) -> CountSnapshot {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        CountSnapshot {
            get: l(&self.get),
            get_range: l(&self.get_range),
            head: l(&self.head),
            put: l(&self.put),
            put_create: l(&self.put_create),
            put_update: l(&self.put_update),
            put_multipart: l(&self.put_multipart),
            list: l(&self.list),
            list_delimiter: l(&self.list_delimiter),
            delete: l(&self.delete),
            copy: l(&self.copy),
        }
    }
}

impl CountSnapshot {
    fn delta(&self, later: &Self) -> Self {
        Self {
            get: later.get - self.get,
            get_range: later.get_range - self.get_range,
            head: later.head - self.head,
            put: later.put - self.put,
            put_create: later.put_create - self.put_create,
            put_update: later.put_update - self.put_update,
            put_multipart: later.put_multipart - self.put_multipart,
            list: later.list - self.list,
            list_delimiter: later.list_delimiter - self.list_delimiter,
            delete: later.delete - self.delete,
            copy: later.copy - self.copy,
        }
    }

    fn total(&self) -> u64 {
        self.get
            + self.get_range
            + self.head
            + self.put
            + self.put_create
            + self.put_update
            + self.put_multipart
            + self.list
            + self.list_delimiter
            + self.delete
            + self.copy
    }

    fn json(&self, per: f64) -> serde_json::Value {
        let p = |v: u64| if per > 0.0 { v as f64 / per } else { 0.0 };
        serde_json::json!({
            "get": p(self.get), "get_range": p(self.get_range), "head": p(self.head),
            "put": p(self.put), "put_create": p(self.put_create), "put_update": p(self.put_update),
            "put_multipart": p(self.put_multipart), "list": p(self.list),
            "list_delimiter": p(self.list_delimiter), "delete": p(self.delete),
            "copy": p(self.copy), "total": p(self.total()),
        })
    }
}

/// One traced request: start, end, plane, method, path.
type TraceEntry = (Instant, Instant, &'static str, &'static str, String);

/// Every request's timing when `OG_CW_TRACE=1`, for the cost model.
static TRACE: std::sync::LazyLock<Option<Mutex<Vec<TraceEntry>>>> =
    std::sync::LazyLock::new(|| (env_or("OG_CW_TRACE", "0") == "1").then(|| Mutex::new(Vec::new())));

static BT_PATTERN: std::sync::LazyLock<Option<String>> =
    std::sync::LazyLock::new(|| std::env::var("OG_CW_BT_PATTERN").ok());
static BT_PRINTED: AtomicU64 = AtomicU64::new(0);

fn trace(start: Instant, plane: &'static str, method: &'static str, path: &Path) {
    if let Some(pattern) = BT_PATTERN.as_ref()
        && method.starts_with("get")
        && path.as_ref().contains(pattern.as_str())
        && { let n = BT_PRINTED.fetch_add(1, Ordering::Relaxed); n >= env_num("OG_CW_BT_SKIP", 0) && n < env_num("OG_CW_BT_SKIP", 0) + 2 }
    {
        let bt = std::backtrace::Backtrace::force_capture().to_string();
        let frames: Vec<&str> = bt
            .lines()
            .filter(|line| line.contains("omnigraph") || line.contains("lance"))
            .filter(|line| !line.contains("concurrent_write_throughput"))
            .take(40)
            .collect();
        eprintln!("[bt] {method} {path}\n{}", frames.join("\n"));
    }
    if let Some(log) = TRACE.as_ref() {
        log.lock()
            .unwrap()
            .push((start, Instant::now(), plane, method, path.to_string()));
    }
}

/// A `WrappingObjectStore` counting every request by method. One per plane.
#[derive(Clone, Default, Debug)]
struct RequestCounter(Arc<Counts>, &'static str);

impl WrappingObjectStore for RequestCounter {
    fn wrap_paginated(
        &self,
        _: &str,
        _original: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        // Keep listings on the wrapped path so they are counted.
        None
    }

    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CountingStore {
            target,
            counts: Arc::clone(&self.0),
            plane: self.1,
        })
    }
}

#[derive(Debug)]
struct CountingStore {
    target: Arc<dyn ObjectStore>,
    counts: Arc<Counts>,
    plane: &'static str,
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
        let method = match opts.mode {
            PutMode::Overwrite => {
                self.counts.put.fetch_add(1, Ordering::Relaxed);
                "put"
            }
            PutMode::Create => {
                self.counts.put_create.fetch_add(1, Ordering::Relaxed);
                "put_if_absent"
            }
            PutMode::Update(_) => {
                self.counts.put_update.fetch_add(1, Ordering::Relaxed);
                "put_if_match"
            }
        };
        let start = Instant::now();
        let result = self.target.put_opts(location, payload, opts).await;
        trace(start, self.plane, method, location);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.counts.put_multipart.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.target.put_multipart_opts(location, opts).await;
        trace(start, self.plane, "put_multipart", location);
        result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let method = if options.head {
            self.counts.head.fetch_add(1, Ordering::Relaxed);
            "head"
        } else if options.range.is_some() {
            self.counts.get_range.fetch_add(1, Ordering::Relaxed);
            "get_range"
        } else {
            self.counts.get.fetch_add(1, Ordering::Relaxed);
            "get"
        };
        let start = Instant::now();
        let result = self.target.get_opts(location, options).await;
        trace(start, self.plane, method, location);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        let counts = Arc::clone(&self.counts);
        self.target.delete_stream(
            locations
                .inspect(move |_| {
                    counts.delete.fetch_add(1, Ordering::Relaxed);
                })
                .boxed(),
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        trace(Instant::now(), self.plane, "list", &prefix.cloned().unwrap_or_default());
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counts.list.fetch_add(1, Ordering::Relaxed);
        trace(Instant::now(), self.plane, "list_offset", &prefix.cloned().unwrap_or_default());
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.counts.list_delimiter.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.target.list_with_delimiter(prefix).await;
        trace(start, self.plane, "list_delim", &prefix.cloned().unwrap_or_default());
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.counts.copy.fetch_add(1, Ordering::Relaxed);
        self.target.copy_opts(from, to, options).await
    }
}

struct Planes {
    manifest: RequestCounter,
    history: RequestCounter,
    table: RequestCounter,
    probes: QueryIoProbes,
    store_io: Mutex<StoreIo>,
}

impl Planes {
    fn new(enabled: bool) -> Self {
        let manifest = RequestCounter(Arc::default(), "manifest");
        let history = RequestCounter(Arc::default(), "history");
        let table = RequestCounter(Arc::default(), "table");
        let probes = if enabled {
            QueryIoProbes {
                manifest_wrapper: Some(Arc::new(manifest.clone())),
                history_wrapper: Some(Arc::new(history.clone())),
                table_wrapper: Some(Arc::new(table.clone())),
                ..Default::default()
            }
        } else {
            QueryIoProbes::default()
        };
        Self {
            manifest,
            history,
            table,
            probes,
            store_io: Mutex::new(StoreIo::default()),
        }
    }

    fn drain(&self) {
        drain_store_io(
            &[
                &self.probes.manifest_stores,
                &self.probes.history_stores,
                &self.probes.table_stores,
            ],
            &self.store_io,
        );
    }

    fn snapshot(&self) -> PlaneSnapshot {
        PlaneSnapshot {
            manifest: self.manifest.0.snapshot(),
            history: self.history.0.snapshot(),
            table: self.table.0.snapshot(),
            reprepares: self.probes.mutation_reprepares.load(Ordering::Relaxed),
            carried: self
                .probes
                .writes_carried_over_row_preserving
                .load(Ordering::Relaxed),
            store_io: {
                self.drain();
                *self.store_io.lock().unwrap()
            },
        }
    }
}

#[derive(Clone, Copy, Default)]
struct StoreIo {
    read_iops: u64,
    write_iops: u64,
    read_bytes: u64,
    write_bytes: u64,
}

/// Drain every distinct probed Lance store's tracker into `into`. Lance's
/// test-util tracker keeps a log of every request; draining bounds it.
fn drain_store_io(planes: &[&ProbedStores], into: &Mutex<StoreIo>) {
    let mut seen = Vec::<Arc<lance::io::ObjectStore>>::new();
    let mut io = into.lock().unwrap();
    for plane in planes {
        for store in plane.stores() {
            if seen.iter().any(|known| Arc::ptr_eq(known, &store)) {
                continue;
            }
            let stats = store.io_stats_incremental();
            io.read_iops += stats.read_iops;
            io.write_iops += stats.write_iops;
            io.read_bytes += stats.read_bytes;
            io.write_bytes += stats.written_bytes;
            seen.push(store);
        }
    }
}

#[derive(Clone, Copy)]
struct PlaneSnapshot {
    manifest: CountSnapshot,
    history: CountSnapshot,
    table: CountSnapshot,
    reprepares: u64,
    carried: u64,
    store_io: StoreIo,
}

// ── The run ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct WriterStats {
    /// (completion phase, service time in µs, commit id) per acknowledged write.
    acks: Vec<(u8, u64, Option<String>)>,
    /// (start, end) of every acknowledged write, for the trace.
    spans: Vec<(Instant, Instant)>,
    driver_retries: u64,
    longest_us: u64,
    acknowledged_keys: Vec<String>,
}

fn nearest_rank(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn graph_uri(cfg: &Config, cell: &str, dir: &tempfile::TempDir) -> String {
    if cfg.target == "local" {
        return dir.path().join("graph").to_string_lossy().into_owned();
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}/{}/{}-{}",
        cfg.target.trim_end_matches('/'),
        cfg.label,
        cell,
        nanos
    )
}

async fn build_fixture(cfg: &Config, uri: &str, writers: usize, mode: &str) -> Arc<Omnigraph> {
    let db = Arc::new(Omnigraph::init(uri, SCHEMA).await.expect("init graph"));
    let session = Session::from_defaults(Arc::clone(&db), SessionSettings::default());
    // Seed rows in one load so the table starts with one fragment.
    let mut jsonl = String::new();
    for i in 0..cfg.seed_rows {
        jsonl.push_str(&format!(
            "{{\"type\":\"Item\",\"data\":{{\"slug\":\"seed-{i}\",\"n\":{i}}}}}\n"
        ));
    }
    if !jsonl.is_empty() {
        session
            .load_jsonl(&jsonl, omnigraph::loader::LoadMode::Overwrite)
            .await
            .expect("seed load");
    }
    session.ensure_indices_as(None).await.expect("ensure indices");
    if mode == "separate" {
        for w in 0..writers {
            db.branch_create(&format!("w{w}")).await.expect("branch create");
        }
    }
    // The production server shape: the one writer process claims the writer
    // epoch of the branch it writes, which lets its handle read and write its
    // bound branch with no freshness probe (`claim_branch_writer`).
    if env_or("OG_CW_CLAIM", "0") == "1" && mode == "same" {
        db.claim_branch_writer("main").await.expect("claim writer epoch");
    }
    db
}

fn branch_of(mode: &str, writer: usize) -> String {
    if mode == "same" {
        "main".to_string()
    } else {
        format!("w{writer}")
    }
}

async fn run_cell(cfg: &Config, writers: usize, mode: &str, run: u64) -> serde_json::Value {
    let cell = format!("{mode}-w{writers}-r{run}");
    let dir = tempfile::tempdir().unwrap();
    let uri = graph_uri(cfg, &cell, &dir);
    let planes = Arc::new(Planes::new(cfg.probes));
    let db = with_query_io_probes(
        planes.probes.clone(),
        build_fixture(cfg, &uri, writers, mode),
    )
    .await;

    let phase = Arc::new(AtomicU8::new(PHASE_WARMUP));
    let mut handles = Vec::with_capacity(writers);
    for w in 0..writers {
        let session = Session::from_defaults(Arc::clone(&db), SessionSettings::default());
        let phase = Arc::clone(&phase);
        let branch = branch_of(mode, w);
        let probes = planes.probes.clone();
        handles.push(tokio::spawn(with_query_io_probes(probes, async move {
            let mut stats = WriterStats::default();
            let mut seq: u64 = 0;
            loop {
                if phase.load(Ordering::Acquire) == PHASE_STOP {
                    break;
                }
                let slug = format!("cw-w{w}-{seq}");
                let params = mixed_params(&[("$slug", slug.as_str())], &[("$n", seq as i64)]);
                let started = Instant::now();
                loop {
                    match session.mutate_with_receipt(&branch, QUERIES, "put", &params).await {
                        Ok(receipt) => {
                            let us = started.elapsed().as_micros() as u64;
                            stats.longest_us = stats.longest_us.max(us);
                            stats.acks.push((
                                phase.load(Ordering::Acquire),
                                us,
                                receipt.commit.map(|c| c.graph_commit_id),
                            ));
                            stats.acknowledged_keys.push(slug.clone());
                            stats.spans.push((started, Instant::now()));
                            break;
                        }
                        Err(error) if error.is_read_set_changed() => {
                            // The engine's bounded re-prepare loop was
                            // exhausted; a real client resubmits.
                            stats.driver_retries += 1;
                        }
                        Err(error) => panic!("writer {w} failed: {error}"),
                    }
                }
                seq += 1;
            }
            stats
        })));
    }

    let drainer = {
        let planes = Arc::clone(&planes);
        let phase = Arc::clone(&phase);
        tokio::spawn(async move {
            while phase.load(Ordering::Acquire) != PHASE_STOP {
                planes.drain();
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };
    tokio::time::sleep(cfg.warmup).await;
    let known_before = omnigraph::instrumentation::KNOWN_TAIL_PROBES.load(Ordering::Relaxed);
    let before = planes.snapshot();
    let window_start = Instant::now();
    phase.store(PHASE_MEASURED, Ordering::Release);
    tokio::time::sleep(cfg.duration).await;
    phase.store(PHASE_STOP, Ordering::Release);
    let window = window_start.elapsed();
    let after = planes.snapshot();
    let known_after = omnigraph::instrumentation::KNOWN_TAIL_PROBES.load(Ordering::Relaxed);

    let drain_start = Instant::now();
    let mut per_writer = Vec::with_capacity(writers);
    for handle in handles {
        per_writer.push(handle.await.expect("writer task"));
    }
    let drain = drain_start.elapsed();
    drainer.await.expect("drainer task");

    if let Some(log) = TRACE.as_ref() {
        let log = log.lock().unwrap();
        for (start, end) in per_writer[0].spans.iter().rev().take(2).rev() {
            eprintln!(
                "[trace] write of {:.1} ms (writer 0; other writers' requests interleave when writers > 1)",
                (*end - *start).as_secs_f64() * 1000.0
            );
            for (s0, s1, plane, method, path) in log.iter() {
                if *s0 >= *start && *s0 <= *end {
                    eprintln!(
                        "[trace] {:>8.1} {:>8.1} {:>6.1}  {:<8} {:<14} {}",
                        (*s0 - *start).as_secs_f64() * 1000.0,
                        (*s1 - *start).as_secs_f64() * 1000.0,
                        (*s1 - *s0).as_secs_f64() * 1000.0,
                        plane,
                        method,
                        path
                    );
                }
            }
        }
    }

    // Window metrics: writes acknowledged while the phase was MEASURED.
    let mut latencies = Vec::new();
    let mut commits = HashSet::new();
    let mut per_writer_counts = Vec::with_capacity(writers);
    let mut driver_retries = 0;
    let mut longest_us = 0;
    for stats in &per_writer {
        let mut mine = 0u64;
        for (p, us, commit) in &stats.acks {
            if *p == PHASE_MEASURED {
                latencies.push(*us);
                mine += 1;
                if let Some(commit) = commit {
                    commits.insert(commit.clone());
                }
            }
        }
        per_writer_counts.push(mine);
        driver_retries += stats.driver_retries;
        longest_us = longest_us.max(stats.longest_us);
    }
    latencies.sort_unstable();
    let acked = latencies.len() as u64;
    let secs = window.as_secs_f64();

    // Verification on a fresh handle: every acknowledged insert is visible
    // exactly once on its branch, and sampled keys read back.
    let verify = Omnigraph::open(&uri).await.expect("verify open");
    let mut per_branch: BTreeMap<String, u64> = BTreeMap::new();
    for (w, stats) in per_writer.iter().enumerate() {
        *per_branch.entry(branch_of(mode, w)).or_default() += stats.acknowledged_keys.len() as u64;
    }
    for (branch, inserted) in &per_branch {
        let snapshot = verify
            .snapshot_of(omnigraph::db::ReadTarget::branch(branch))
            .await
            .expect("verify snapshot");
        let rows = snapshot
            .open_dataset(TABLE)
            .await
            .expect("verify open table")
            .count_rows(None)
            .await
            .expect("verify count") as u64;
        // `main` holds only the seed in `separate` mode.
        let expected = cfg.seed_rows + inserted;
        assert_eq!(
            rows, expected,
            "branch {branch}: {rows} rows, expected seed {} + {inserted} acknowledged",
            cfg.seed_rows
        );
    }

    let manifest = before.manifest.delta(&after.manifest);
    let history = before.history.delta(&after.history);
    let table = before.table.delta(&after.table);
    let per = acked as f64;
    let target_name = if cfg.target == "local" { "local" } else { "s3" };
    let max_over_min = {
        let max = per_writer_counts.iter().copied().max().unwrap_or(0);
        let min = per_writer_counts.iter().copied().min().unwrap_or(0);
        if min == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(max as f64 / min as f64)
        }
    };
    let git = option_env!("OG_CW_GIT").unwrap_or("unknown");
    let record = serde_json::json!({
        "label": cfg.label,
        "target": target_name,
        "mode": mode,
        "writers": writers,
        "run": run,
        "window_secs": secs,
        "warmup_secs": cfg.warmup.as_secs_f64(),
        "seed_rows": cfg.seed_rows,
        "tokio_workers": cfg.tokio_workers,
        "acked_writes": acked,
        "writes_per_sec": acked as f64 / secs,
        "distinct_commits": commits.len(),
        "commits_per_sec": commits.len() as f64 / secs,
        "service_us": {
            "p50": nearest_rank(&latencies, 0.50),
            "p90": nearest_rank(&latencies, 0.90),
            "p99": nearest_rank(&latencies, 0.99),
            "max_in_window": latencies.last().copied().unwrap_or(0),
            "max_including_drain": longest_us,
        },
        "per_writer": per_writer_counts,
        "per_writer_max_over_min": max_over_min,
        "engine_reprepares_per_write": (after.reprepares - before.reprepares) as f64 / per.max(1.0),
        "carried_per_write": (after.carried - before.carried) as f64 / per.max(1.0),
        "known_tail_probes_per_write": (known_after - known_before) as f64 / per.max(1.0),
        "driver_retries": driver_retries,
        "drain_secs": drain.as_secs_f64(),
        "requests_per_write": {
            "manifest": manifest.json(per),
            "history": history.json(per),
            "table": table.json(per),
            "total": (manifest.total() + history.total() + table.total()) as f64 / per.max(1.0),
        },
        "store_io_per_write": {
            "read_iops": (after.store_io.read_iops - before.store_io.read_iops) as f64 / per.max(1.0),
            "write_iops": (after.store_io.write_iops - before.store_io.write_iops) as f64 / per.max(1.0),
        },
        "git": git,
        "claim": env_or("OG_CW_CLAIM", "0"),
        "group_commit": env_or("OMNIGRAPH_GROUP_COMMIT", "0"),
        "pipeline": env_or("OMNIGRAPH_GROUP_COMMIT_PIPELINE", "0"),
    });
    eprintln!(
        "[cw] {label} {target} {mode:>8} w={writers:<3} run={run}: {wps:8.2} writes/s {cps:8.2} commits/s p50 {p50:>7.1} ms p99 {p99:>8.1} ms max+drain {mx:>8.1} ms req/write {req:6.1} reprep/write {rp:5.2} per-writer {pw:?}",
        label = cfg.label,
        target = if cfg.target == "local" { "local" } else { "s3" },
        wps = acked as f64 / secs,
        cps = commits.len() as f64 / secs,
        p50 = nearest_rank(&latencies, 0.50) as f64 / 1000.0,
        p99 = nearest_rank(&latencies, 0.99) as f64 / 1000.0,
        mx = longest_us as f64 / 1000.0,
        req = (manifest.total() + history.total() + table.total()) as f64 / per.max(1.0),
        rp = (after.reprepares - before.reprepares) as f64 / per.max(1.0),
        pw = per_writer_counts,
    );
    drop(verify);
    drop(db);
    record
}

#[test]
#[ignore = "instrument: concurrent write throughput sweep; configured by OG_CW_* (see module docs)"]
fn concurrent_write_throughput() {
    let cfg = Config::from_env();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.tokio_workers)
        .enable_all()
        .build()
        .unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    runtime.block_on(async {
        for mode in &cfg.modes {
            for &writers in &cfg.writers {
                for run in 0..cfg.runs {
                    let record = run_cell(&cfg, writers, mode, run).await;
                    if let Some(out) = &cfg.out {
                        use std::io::Write;
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(out)
                            .expect("open OG_CW_OUT");
                        writeln!(file, "{record}").unwrap();
                    }
                    records.lock().unwrap().push(record);
                }
            }
        }
    });
}
