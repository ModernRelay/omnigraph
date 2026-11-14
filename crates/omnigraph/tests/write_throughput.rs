//! instrument: concurrent write throughput (WAL research, not a CI gate).
//!
//! N closed-loop writers over clones of one `Session` (the production server
//! shape: one engine handle per graph, one session per request), each issuing
//! small single-node inserts with disjoint keys, either all on `main`
//! (`same`) or each on its own branch (`branches`). Reports commits/s,
//! p50/p99 service time, object-store requests per commit by class, and
//! retries (engine reprepares and client-visible conflicts).
//!
//! Backends:
//! - `file`: a local temp directory, no injected latency. Lance's direct local
//!   reader and writer bypass the seam on `file://`, so request counts there
//!   cover manifest and listing traffic only.
//! - `mem`: Lance's `shared-memory` provider behind the engine's Lance-realm
//!   object-store seam, with a fixed injected latency per request
//!   (`OG_WT_LATENCY_MS`). Every Lance request crosses the seam, so counts are
//!   complete and the latency models a remote object store. The commit
//!   handler for `shared-memory` is Lance's `ConditionalPutCommitHandler`, the
//!   one S3 uses.
//!
//! The driver is closed-loop (coordinated omission applies): numbers are
//! service times under saturation, a research instrument only.
//!
//! Run (one compiled binary, every cell driven by environment):
//!
//! ```text
//! OG_WT_BACKEND=mem OG_WT_LATENCY_MS=20 OG_WT_WRITERS=1,4,16,64 OG_WT_MODE=same \
//!   cargo test -p omnigraph-engine --features dst --test write_throughput \
//!   -- --ignored --nocapture
//! ```
#![cfg(feature = "dst")]

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use arrow_array::Array as _;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use omnigraph::Session;
use omnigraph::db::{InitOptions, Omnigraph};
use omnigraph::instrumentation::{CountingStorageAdapter, QueryIoProbes, with_query_io_probes};
use omnigraph::object_store_seam::{DecorateObjectStore, OBJECT_STORE};
use omnigraph::seams::Behavior;
use omnigraph::settings::SessionSettings;
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

const SCHEMA: &str = r#"
node Item {
    slug: String @key
    value: I64?
}

node Tag {
    name: String @key
}

edge Tagged: Item -> Tag
"#;

const QUERIES: &str = r#"
query put($slug: String, $value: I64) {
    insert Item { slug: $slug, value: $value }
}

query put_tagged($slug: String, $value: I64, $tag: String) {
    insert Item { slug: $slug, value: $value }
    insert Tagged { from: $slug, to: $tag }
}
"#;

// ── The latency + counting object store ────────────────────────────────────

#[derive(Default)]
struct Counters {
    /// (operation, object class) -> calls.
    calls: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    total: AtomicU64,
    trace: Mutex<Option<Vec<TraceEvent>>>,
}

#[derive(Clone, Debug)]
struct TraceEvent {
    start_us: u64,
    end_us: u64,
    op: &'static str,
    path: String,
}

impl Counters {
    fn record(&self, op: &'static str, path: &Path) {
        let class = classify(path.as_ref());
        *self
            .calls
            .lock()
            .unwrap()
            .entry((op, class))
            .or_default() += 1;
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> BTreeMap<(&'static str, &'static str), u64> {
        self.calls.lock().unwrap().clone()
    }

    fn trace_start(&self) {
        *self.trace.lock().unwrap() = Some(Vec::new());
    }

    fn trace_take(&self) -> Vec<TraceEvent> {
        self.trace.lock().unwrap().take().unwrap_or_default()
    }

    fn tracing(&self) -> bool {
        self.trace.lock().unwrap().is_some()
    }

    fn push_trace(&self, event: TraceEvent) {
        if let Some(trace) = self.trace.lock().unwrap().as_mut() {
            trace.push(event);
        }
    }
}

/// Object class from the key: which dataset (`__manifest`, `__history`, or a
/// node/edge table) and which part of it.
fn classify(path: &str) -> &'static str {
    if path.contains("__wal") {
        return "wal";
    }
    let dataset = if path.contains("__manifest") {
        "manifest"
    } else if path.contains("__history") {
        "history"
    } else {
        "table"
    };
    let part = if path.contains("/_versions/") || path.ends_with("_versions") {
        "versions"
    } else if path.contains("/_transactions/") {
        "txn"
    } else if path.contains("/_refs/") {
        "refs"
    } else if path.contains("/_indices/") {
        "indices"
    } else if path.contains("/data/") {
        "data"
    } else if path.contains("/_deletions/") {
        "deletions"
    } else {
        "other"
    };
    match (dataset, part) {
        ("manifest", "versions") => "manifest/versions",
        ("manifest", "txn") => "manifest/txn",
        ("manifest", "refs") => "manifest/refs",
        ("manifest", "data") => "manifest/data",
        ("manifest", "deletions") => "manifest/deletions",
        ("manifest", "indices") => "manifest/indices",
        ("manifest", _) => "manifest/other",
        ("history", _) => "history",
        ("table", "versions") => "table/versions",
        ("table", "txn") => "table/txn",
        ("table", "refs") => "table/refs",
        ("table", "data") => "table/data",
        ("table", "deletions") => "table/deletions",
        ("table", "indices") => "table/indices",
        _ => "table/other",
    }
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn now_us() -> u64 {
    epoch().elapsed().as_micros() as u64
}

struct LatencyDecorator {
    latency: Duration,
    counters: Arc<Counters>,
}

impl Behavior for LatencyDecorator {}

impl DecorateObjectStore for LatencyDecorator {
    fn wrap(&self, base: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(LatencyStore {
            inner: base,
            latency: self.latency,
            counters: Arc::clone(&self.counters),
        })
    }
}

#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    counters: Arc<Counters>,
}

impl fmt::Debug for Counters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Counters")
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Latency({})", self.inner)
    }
}

impl LatencyStore {
    async fn charge(&self, op: &'static str, path: &Path) -> u64 {
        self.counters.record(op, path);
        let start = now_us();
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        start
    }

    fn finish(&self, start: u64, op: &'static str, path: &Path) {
        if self.counters.tracing() {
            self.counters.push_trace(TraceEvent {
                start_us: start,
                end_us: now_us(),
                op,
                path: path.to_string(),
            });
        }
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let op = match opts.mode {
            PutMode::Overwrite => "put",
            PutMode::Create => "put_if_absent",
            PutMode::Update(_) => "put_if_match",
        };
        let start = self.charge(op, location).await;
        let result = self.inner.put_opts(location, payload, opts).await;
        self.finish(start, op, location);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let start = self.charge("multipart", location).await;
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.finish(start, "multipart", location);
        result
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let op = if options.head {
            "head"
        } else if options.range.is_some() {
            "get_range"
        } else {
            "get"
        };
        if op == "get_range"
            && location.as_ref().contains("/nodes/")
            && location.as_ref().contains("_versions/d")
            && std::env::var("OG_WT_BACKTRACE").is_ok()
            && self.counters.tracing()
        {
            eprintln!("--- get_range {location}\n{}", std::backtrace::Backtrace::force_capture());
        }
        let start = self.charge(op, location).await;
        let result = self.inner.get_opts(location, options).await;
        self.finish(start, op, location);
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let start = self.charge("get_ranges", location).await;
        let result = self.inner.get_ranges(location, ranges).await;
        self.finish(start, "get_ranges", location);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let counters = Arc::clone(&self.counters);
        let latency = self.latency;
        let inner = Arc::clone(&self.inner);
        let counted = locations
            .then(move |location| {
                let counters = Arc::clone(&counters);
                async move {
                    if let Ok(path) = &location {
                        counters.record("delete", path);
                        if !latency.is_zero() {
                            tokio::time::sleep(latency).await;
                        }
                    }
                    location
                }
            })
            .boxed();
        inner.delete_stream(counted)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let key = prefix.cloned().unwrap_or_default();
        self.counters.record("list", &key);
        let inner = self.inner.list(prefix);
        let latency = self.latency;
        futures::stream::once(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            inner
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let key = prefix.cloned().unwrap_or_default();
        self.counters.record("list_offset", &key);
        let inner = self.inner.list_with_offset(prefix, offset);
        let latency = self.latency;
        futures::stream::once(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let key = prefix.cloned().unwrap_or_default();
        let start = self.charge("list_delim", &key).await;
        let result = self.inner.list_with_delimiter(prefix).await;
        self.finish(start, "list_delim", &key);
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let start = self.charge("copy", to).await;
        let result = self.inner.copy_opts(from, to, options).await;
        self.finish(start, "copy", to);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let start = self.charge("rename", to).await;
        let result = self.inner.rename_opts(from, to, options).await;
        self.finish(start, "rename", to);
        result
    }
}

// ── Configuration ─────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    backend: String,
    latency_ms: u64,
    writers: Vec<usize>,
    modes: Vec<String>,
    duration: Duration,
    warmup: Duration,
    shape: String,
    out: Option<String>,
    trace: bool,
    label: String,
    claim: bool,
    indexed: bool,
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Self {
        Self {
            backend: env("OG_WT_BACKEND", "mem"),
            latency_ms: env("OG_WT_LATENCY_MS", "20").parse().unwrap(),
            writers: env("OG_WT_WRITERS", "1,4,16,64")
                .split(',')
                .map(|w| w.trim().parse().unwrap())
                .collect(),
            modes: env("OG_WT_MODE", "same")
                .split(',')
                .map(|m| m.trim().to_string())
                .collect(),
            duration: Duration::from_secs_f64(env("OG_WT_DURATION_SECS", "10").parse().unwrap()),
            warmup: Duration::from_secs_f64(env("OG_WT_WARMUP_SECS", "2").parse().unwrap()),
            shape: env("OG_WT_SHAPE", "node"),
            out: std::env::var("OG_WT_OUT").ok(),
            trace: env("OG_WT_TRACE", "0") == "1",
            label: env("OG_WT_LABEL", "baseline"),
            claim: env("OG_WT_CLAIM", "0") == "1",
            indexed: env("OG_WT_INDEXED", "0") == "1",
        }
    }
}

// ── The run ───────────────────────────────────────────────────────────────

static DECORATOR: OnceLock<(
    Arc<Counters>,
    omnigraph::seams::Installed<dyn DecorateObjectStore, omnigraph::seams::Global<dyn DecorateObjectStore>>,
)> = OnceLock::new();

fn counters(latency: Duration) -> Arc<Counters> {
    let (counters, _) = DECORATOR.get_or_init(|| {
        let counters = Arc::new(Counters::default());
        let installed = OBJECT_STORE.install(Arc::new(LatencyDecorator {
            latency,
            counters: Arc::clone(&counters),
        }));
        (counters, installed)
    });
    Arc::clone(counters)
}

fn params(slug: &str, value: i64, tag: Option<&str>) -> ParamMap {
    let mut map = ParamMap::new();
    map.insert("slug".to_string(), Literal::String(slug.to_string()));
    map.insert("value".to_string(), Literal::Integer(i128::from(value)));
    if let Some(tag) = tag {
        map.insert("tag".to_string(), Literal::String(tag.to_string()));
    }
    map
}

struct CellResult {
    commits: u64,
    elapsed: Duration,
    latencies_us: Vec<u64>,
    client_conflicts: u64,
    other_errors: Vec<String>,
    reprepares: u64,
    requests: BTreeMap<(&'static str, &'static str), u64>,
    control_calls: u64,
    verified: bool,
    phases: [u64; 7],
    fold_lag: (f64, u64),
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

async fn open_graph(
    config: &Config,
    cell: &str,
) -> (
    Session,
    Option<tempfile::TempDir>,
    Arc<omnigraph::instrumentation::StorageReadCounts>,
    String,
    Arc<dyn StorageAdapter>,
) {
    match config.backend.as_str() {
        "file" => {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap().to_string();
            let inner: Arc<dyn StorageAdapter> = omnigraph::storage::storage_for_uri(&uri).unwrap();
            let (adapter, counts) = CountingStorageAdapter::new(inner);
            let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
                .await
                .unwrap();
            (
                Session::from_defaults(Arc::new(db), SessionSettings::default()),
                Some(dir),
                counts,
                uri,
                adapter,
            )
        }
        "mem" => {
            let uri = format!(
                "shared-memory://wt-{}-{}/graph",
                std::process::id(),
                cell
            );
            let inner: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
            let (adapter, counts) = CountingStorageAdapter::new(inner);
            let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
                .await
                .unwrap();
            (
                Session::from_defaults(Arc::new(db), SessionSettings::default()),
                None,
                counts,
                uri,
                adapter,
            )
        }
        other => panic!("unknown backend {other}"),
    }
}

async fn run_cell(config: &Config, mode: &str, writers: usize) -> CellResult {
    let counters = counters(Duration::from_millis(config.latency_ms));
    let cell = format!("{mode}-{writers}-{}", now_us());
    let (session, _dir, control, uri, adapter) = open_graph(config, &cell).await;
    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..writers).map(|_| AtomicU64::new(0)).collect());

    // Branches for the separate-branch mode, and the tag the edge shape needs.
    let branches: Vec<String> = (0..writers)
        .map(|i| {
            if mode == "branches" {
                format!("b{i}")
            } else {
                "main".to_string()
            }
        })
        .collect();
    if config.shape == "edge" {
        session
            .mutate(
                "main",
                "query t($name: String) { insert Tag { name: $name } }",
                "t",
                &{
                    let mut m = ParamMap::new();
                    m.insert("name".into(), Literal::String("t0".into()));
                    m
                },
            )
            .await
            .unwrap();
    }
    // An indexed table (`OG_WT_INDEXED=1`): one seed row, then the id BTREE
    // `ensure_indices` builds for every non-empty node table, as a production
    // graph has after its first index build.
    if config.indexed {
        session
            .mutate("main", QUERIES, "put", &params("seed-0", 0, None))
            .await
            .unwrap();
        session.ensure_indices_as(None).await.unwrap();
    }
    if mode == "branches" {
        for branch in &branches {
            session.db().branch_create(branch).await.unwrap();
        }
    }
    // Hold the writer epoch of every branch written, so the
    // process's own view is authoritative and its head probes are skipped.
    if config.claim {
        let mut claimed = std::collections::BTreeSet::new();
        for branch in &branches {
            if claimed.insert(branch.clone()) {
                session.db().claim_branch_writer(branch).await.unwrap();
            }
        }
    }

    let phase = Arc::new(AtomicU64::new(0)); // 0 warmup, 1 measured, 2 stop
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for (w, branch) in branches.iter().cloned().enumerate() {
        let session = session.clone();
        let acked = Arc::clone(&acked);
        let phase = Arc::clone(&phase);
        let stop = Arc::clone(&stop);
        let shape = config.shape.clone();
        handles.push(tokio::spawn(async move {
            let probes = QueryIoProbes::default();
            let reprepares = Arc::clone(&probes.mutation_reprepares);
            let mut latencies = Vec::new();
            let mut commits = 0u64;
            let mut conflicts = 0u64;
            let mut errors = Vec::new();
            let mut measured_reprepares_start = None;
            let mut seq = 0u64;
            with_query_io_probes(probes, async {
                while !stop.load(Ordering::Relaxed) {
                    let measuring = phase.load(Ordering::Relaxed) == 1;
                    if measuring && measured_reprepares_start.is_none() {
                        measured_reprepares_start = Some(reprepares.load(Ordering::Relaxed));
                    }
                    let start = Instant::now();
                    loop {
                        seq += 1;
                        let slug = format!("w{w}-{seq}");
                        let (name, p) = if shape == "edge" {
                            ("put_tagged", params(&slug, seq as i64, Some("t0")))
                        } else {
                            ("put", params(&slug, seq as i64, None))
                        };
                        match session.mutate(&branch, QUERIES, name, &p).await {
                            Ok(_) => {
                                acked[w].fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(e) => {
                                let debug = format!("{e:?}");
                                if debug.contains("ReadSetChanged") {
                                    conflicts += u64::from(measuring);
                                    continue;
                                }
                                errors.push(format!("{e}"));
                                break;
                            }
                        }
                    }
                    if phase.load(Ordering::Relaxed) == 1 && measuring {
                        latencies.push(start.elapsed().as_micros() as u64);
                        commits += 1;
                    }
                }
            })
            .await;
            let reprepares_end = reprepares.load(Ordering::Relaxed);
            (
                commits,
                latencies,
                conflicts,
                errors,
                reprepares_end - measured_reprepares_start.unwrap_or(reprepares_end),
            )
        }));
    }

    tokio::time::sleep(config.warmup).await;
    let _ = omnigraph::db::group_commit::stats::take();
    let _ = omnigraph_catalog::wal::lag::take();
    let before = counters.snapshot();
    let control_before = control.mutation_calls.load(Ordering::Relaxed)
        + control.read_text.load(Ordering::Relaxed)
        + control.read_text_if_exists.load(Ordering::Relaxed)
        + control.read_bytes_if_exists.load(Ordering::Relaxed)
        + control.exists.load(Ordering::Relaxed)
        + control.list_dir.load(Ordering::Relaxed);
    let started = Instant::now();
    phase.store(1, Ordering::SeqCst);
    tokio::time::sleep(config.duration).await;
    phase.store(2, Ordering::SeqCst);
    let elapsed = started.elapsed();
    let after = counters.snapshot();
    let phases = omnigraph::db::group_commit::stats::take();
    let fold_lag = omnigraph_catalog::wal::lag::take();
    let control_after = control.mutation_calls.load(Ordering::Relaxed)
        + control.read_text.load(Ordering::Relaxed)
        + control.read_text_if_exists.load(Ordering::Relaxed)
        + control.read_bytes_if_exists.load(Ordering::Relaxed)
        + control.exists.load(Ordering::Relaxed)
        + control.list_dir.load(Ordering::Relaxed);
    stop.store(true, Ordering::SeqCst);

    let mut result = CellResult {
        commits: 0,
        elapsed,
        latencies_us: Vec::new(),
        client_conflicts: 0,
        other_errors: Vec::new(),
        reprepares: 0,
        requests: BTreeMap::new(),
        control_calls: control_after - control_before,
        verified: false,
        phases,
        fold_lag,
    };
    for handle in handles {
        let (commits, latencies, conflicts, errors, reprepares) = handle.await.unwrap();
        result.commits += commits;
        result.latencies_us.extend(latencies);
        result.client_conflicts += conflicts;
        result.other_errors.extend(errors);
        result.reprepares += reprepares;
    }
    for (key, value) in after {
        let delta = value - before.get(&key).copied().unwrap_or(0);
        if delta > 0 {
            result.requests.insert(key, delta);
        }
    }

    // Optional single-commit trace on a quiet graph: the request chain of one
    // write, for the cost model.
    if config.trace {
        let branch = branches[0].clone();
        counters.trace_start();
        let t0 = now_us();
        session
            .mutate(&branch, QUERIES, "put", &params("trace-1", 1, None))
            .await
            .unwrap();
        let t1 = now_us();
        let trace = counters.trace_take();
        eprintln!("--- trace of one commit ({} requests, {} us) ---", trace.len(), t1 - t0);
        for event in trace {
            eprintln!(
                "{:>8} {:>8} {:>14} {}",
                event.start_us - t0,
                event.end_us - t0,
                event.op,
                event.path
            );
        }
    }
    // Verification: every acknowledged insert is present exactly once, read
    // by a fresh handle over durable `__manifest` state after every log is
    // folded (the handle opens with no in-process log).
    omnigraph_catalog::wal::forget_all().await.unwrap();
    let fresh = Omnigraph::open_with_storage(&uri, Arc::clone(&adapter)).await.unwrap();
    let mut expected: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for (w, branch) in branches.iter().enumerate() {
        *expected.entry(branch.clone()).or_default() += acked[w].load(Ordering::Relaxed);
    }
    if config.trace {
        *expected.entry(branches[0].clone()).or_default() += 1;
    }
    if config.indexed {
        for want in expected.values_mut() {
            *want += 1;
        }
    }
    let mut verified = true;
    for (branch, want) in &expected {
        let snapshot = fresh
            .snapshot_of(omnigraph::db::ReadTarget::branch(branch.as_str()))
            .await
            .unwrap();
        let dataset = snapshot.open_dataset("node:Item").await.unwrap();
        let got = dataset.count_rows(None).await.unwrap() as u64;
        if got != *want {
            verified = false;
            eprintln!("VERIFY FAILED on {branch}: {got} rows, {want} acknowledged inserts");
        }
    }
    result.verified = verified;
    result
}

fn report(config: &Config, mode: &str, writers: usize, mut result: CellResult) -> serde_json::Value {
    result.latencies_us.sort_unstable();
    let commits = result.commits.max(1);
    let total_requests: u64 = result.requests.values().sum();
    let mut by_class: BTreeMap<String, f64> = BTreeMap::new();
    let mut by_op: BTreeMap<String, f64> = BTreeMap::new();
    for ((op, class), count) in &result.requests {
        *by_class.entry((*class).to_string()).or_default() += *count as f64 / commits as f64;
        *by_op.entry((*op).to_string()).or_default() += *count as f64 / commits as f64;
    }
    let value = serde_json::json!({
        "label": config.label,
        "claim": config.claim,
        "indexed": config.indexed,
        "backend": config.backend,
        "latency_ms": config.latency_ms,
        "mode": mode,
        "shape": config.shape,
        "writers": writers,
        "duration_s": result.elapsed.as_secs_f64(),
        "commits": result.commits,
        "commits_per_s": result.commits as f64 / result.elapsed.as_secs_f64(),
        "p50_ms": percentile(&result.latencies_us, 0.50) as f64 / 1000.0,
        "p99_ms": percentile(&result.latencies_us, 0.99) as f64 / 1000.0,
        "max_ms": result.latencies_us.last().copied().unwrap_or(0) as f64 / 1000.0,
        "requests_per_commit": total_requests as f64 / commits as f64,
        "requests_by_class": by_class,
        "requests_by_op": by_op,
        "reprepares_per_commit": result.reprepares as f64 / commits as f64,
        "client_conflicts": result.client_conflicts,
        "control_plane_calls": result.control_calls,
        "errors": result.other_errors.len(),
        "verified": result.verified,
        "unfolded_records_mean": result.fold_lag.0,
        "unfolded_records_max": result.fold_lag.1,
        "batches": result.phases[0],
        "mean_batch": result.phases[1] as f64 / result.phases[0].max(1) as f64,
        "batch_ms": {
            "gates": result.phases[2] as f64 / 1000.0 / result.phases[0].max(1) as f64,
            "capture": result.phases[3] as f64 / 1000.0 / result.phases[0].max(1) as f64,
            "tables": result.phases[4] as f64 / 1000.0 / result.phases[0].max(1) as f64,
            "publish": result.phases[5] as f64 / 1000.0 / result.phases[0].max(1) as f64,
            "queue_per_entry": result.phases[6] as f64 / 1000.0 / result.phases[1].max(1) as f64,
        },
        "group_commit": std::env::var("OMNIGRAPH_GROUP_COMMIT").unwrap_or_default(),
        "wal": std::env::var("OMNIGRAPH_WAL").unwrap_or_default(),
        "first_error": result.other_errors.first(),
    });
    if result.phases[0] > 0 {
        eprintln!("    batches {} mean size {:.1} per-batch ms {}", value["batches"], value["mean_batch"].as_f64().unwrap(), value["batch_ms"]);
    }
    eprintln!(
        "[{}] {} {}ms {} w={:>2}: {:>7.1} commits/s  p50 {:>8.1} ms  p99 {:>8.1} ms  req/commit {:>6.1}  reprep/commit {:.2}  conflicts {}  errors {}  verified {}",
        config.label,
        config.backend,
        config.latency_ms,
        mode,
        writers,
        value["commits_per_s"].as_f64().unwrap(),
        value["p50_ms"].as_f64().unwrap(),
        value["p99_ms"].as_f64().unwrap(),
        value["requests_per_commit"].as_f64().unwrap(),
        value["reprepares_per_commit"].as_f64().unwrap(),
        result.client_conflicts,
        result.other_errors.len(),
        result.verified,
    );
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "instrument: concurrent write throughput sweep (WAL research); run with --ignored"]
async fn concurrent_write_throughput() {
    let config = Config::from_env();
    eprintln!("config: {config:?}");
    let mut lines = Vec::new();
    for mode in &config.modes {
        for &writers in &config.writers {
            let result = run_cell(&config, mode, writers).await;
            lines.push(report(&config, mode, writers, result));
        }
    }
    if let Some(out) = &config.out {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(out)
            .unwrap();
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
    }
}

/// The log's correctness evidence (run with `OMNIGRAPH_WAL=1`): acknowledged
/// writes whose records were never folded survive a crash, a reader in a new
/// process sees them by reading the log tail, and the next owner's claim folds
/// them before it publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "instrument: WAL crash recovery and cold reads (WAL research); run with OMNIGRAPH_WAL=1 --ignored"]
async fn wal_recovery_and_cold_reads() {
    assert!(
        omnigraph_catalog::wal::enabled(),
        "run with OMNIGRAPH_WAL=1"
    );
    let _counters = counters(Duration::ZERO);
    let uri = format!("shared-memory://wt-recovery-{}/graph", std::process::id());
    let adapter: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
    let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
        .await
        .unwrap();
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    session.db().claim_branch_writer("main").await.unwrap();

    async fn count(adapter: &Arc<dyn StorageAdapter>, uri: &str) -> u64 {
        let fresh = Omnigraph::open_with_storage(uri, Arc::clone(adapter)).await.unwrap();
        let snapshot = fresh
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        snapshot
            .open_dataset("node:Item")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap() as u64
    }

    for i in 0..5 {
        session
            .mutate("main", QUERIES, "put", &params(&format!("folded-{i}"), i, None))
            .await
            .unwrap();
    }
    omnigraph_catalog::wal::quiesce_all().await.unwrap();

    // Records only: the fold is paused, then the process "crashes".
    omnigraph_catalog::wal::FOLD_PAUSED.store(true, Ordering::SeqCst);
    for i in 0..7 {
        session
            .mutate("main", QUERIES, "put", &params(&format!("unfolded-{i}"), i, None))
            .await
            .unwrap();
    }
    omnigraph_catalog::wal::crash_forget_all();
    drop(session);
    omnigraph_catalog::wal::FOLD_PAUSED.store(false, Ordering::SeqCst);

    // A reader in a new process: 5 folded + 7 acknowledged in the log tail.
    let cold = count(&adapter, &uri).await;
    eprintln!("cold reader sees {cold} rows (expect 12)");
    assert_eq!(cold, 12, "a cold reader must see every acknowledged write");

    // The next owner claims: the claim folds the 7 records, then publishes.
    let db = Omnigraph::open_with_storage(&uri, Arc::clone(&adapter)).await.unwrap();
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    session.db().claim_branch_writer("main").await.unwrap();
    session
        .mutate("main", QUERIES, "put", &params("after-recovery", 99, None))
        .await
        .unwrap();
    omnigraph_catalog::wal::forget_all().await.unwrap();
    drop(session);
    let recovered = count(&adapter, &uri).await;
    eprintln!("after recovery and one more write: {recovered} rows (expect 13)");
    assert_eq!(recovered, 13);
}

/// Group commit under a mixed workload on one branch (run with the prototype
/// switches): upserts whose keys collide (a second insert of a key is an
/// update) beside updates of existing keys. Every acknowledged key is present
/// exactly once afterwards, and nothing fails but bounded conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "instrument: group commit under colliding upserts and updates (WAL research)"]
async fn group_commit_mixed_workload_is_consistent() {
    let _counters = counters(Duration::from_millis(
        env("OG_WT_LATENCY_MS", "5").parse().unwrap(),
    ));
    let uri = format!("shared-memory://wt-mixed-{}/graph", std::process::id());
    let adapter: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
    let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
        .await
        .unwrap();
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    session.db().claim_branch_writer("main").await.unwrap();
    const UPDATE: &str = "query set_value($slug: String, $value: I64) { update Item set { value: $value } where slug = $slug }";
    let keys: u64 = 120;
    let stop = Arc::new(AtomicBool::new(false));
    let acked: Arc<Mutex<std::collections::BTreeSet<String>>> = Arc::default();
    let errors: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut handles = Vec::new();
    for w in 0..24u64 {
        let session = session.clone();
        let stop = Arc::clone(&stop);
        let acked = Arc::clone(&acked);
        let errors = Arc::clone(&errors);
        handles.push(tokio::spawn(async move {
            let mut seed = w.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut ops = 0u64;
            while !stop.load(Ordering::Relaxed) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let slug = format!("k{}", seed % keys);
                let result = if w < 20 {
                    session
                        .mutate("main", QUERIES, "put", &params(&slug, ops as i64, None))
                        .await
                        .map(|_| true)
                } else {
                    let mut p = ParamMap::new();
                    p.insert("slug".into(), Literal::String(slug.clone()));
                    p.insert("value".into(), Literal::Integer(-(ops as i128)));
                    session.mutate("main", UPDATE, "set_value", &p).await.map(|_| false)
                };
                match result {
                    Ok(true) => {
                        acked.lock().unwrap().insert(slug);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        let text = format!("{error:?}");
                        if !text.contains("ReadSetChanged") {
                            errors.lock().unwrap().push(format!("{error}"));
                        }
                    }
                }
                ops += 1;
            }
            ops
        }));
    }
    tokio::time::sleep(Duration::from_secs(
        env("OG_WT_DURATION_SECS", "6").parse().unwrap(),
    ))
    .await;
    stop.store(true, Ordering::SeqCst);
    let mut ops = 0;
    for handle in handles {
        ops += handle.await.unwrap();
    }
    omnigraph_catalog::wal::forget_all().await.unwrap();
    let fresh = Omnigraph::open_with_storage(&uri, Arc::clone(&adapter)).await.unwrap();
    let snapshot = fresh
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let dataset = snapshot.open_dataset("node:Item").await.unwrap();
    let rows = dataset.count_rows(None).await.unwrap();
    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut seen = std::collections::BTreeMap::<String, usize>::new();
    use futures::TryStreamExt as _;
    while let Some(batch) = stream.try_next().await.unwrap() {
        let column = batch
            .column_by_name("slug")
            .expect("slug column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("slug is a string column")
            .clone();
        for i in 0..column.len() {
            *seen.entry(column.value(i).to_string()).or_default() += 1;
        }
    }
    let acked = acked.lock().unwrap().clone();
    let errors = errors.lock().unwrap().clone();
    eprintln!(
        "mixed: {ops} operations, {} distinct acknowledged keys, {rows} rows, {} distinct slugs, {} errors",
        acked.len(),
        seen.len(),
        errors.len()
    );
    assert!(errors.is_empty(), "unexpected errors: {:?}", &errors[..errors.len().min(5)]);
    assert!(seen.values().all(|count| *count == 1), "a key is present twice");
    assert_eq!(rows, seen.len());
    assert!(
        acked.iter().all(|slug| seen.contains_key(slug)),
        "an acknowledged key is missing"
    );
    assert_eq!(seen.len(), acked.len(), "a key is present that no acknowledged insert wrote");
}

/// What reading the log tail costs a reader in another process: a cold open
/// plus one row count on `main`, with `k` acknowledged records unfolded (run
/// with `OMNIGRAPH_WAL=1`; `OG_WT_LATENCY_MS` per request).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "instrument: cold read cost over an unfolded log tail (WAL research)"]
async fn wal_cold_read_cost() {
    let latency = Duration::from_millis(env("OG_WT_LATENCY_MS", "20").parse().unwrap());
    let counters = counters(latency);
    for k in [0u64, 1, 8, 32, 63] {
        let uri = format!("shared-memory://wt-coldread-{}-{k}/graph", std::process::id());
        let adapter: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        if omnigraph_catalog::wal::enabled() {
            session.db().claim_branch_writer("main").await.unwrap();
        }
        session
            .mutate("main", QUERIES, "put", &params("first", 0, None))
            .await
            .unwrap();
        omnigraph_catalog::wal::quiesce_all().await.unwrap();
        omnigraph_catalog::wal::FOLD_PAUSED.store(true, Ordering::SeqCst);
        for i in 0..k {
            session
                .mutate("main", QUERIES, "put", &params(&format!("r{i}"), i as i64, None))
                .await
                .unwrap();
        }
        let before = counters.snapshot();
        let started = Instant::now();
        let fresh = {
            // A reader with no log of the branch in its process.
            let tails_hidden = omnigraph_catalog::wal::hide_tails();
            let fresh = Omnigraph::open_with_storage(&uri, Arc::clone(&adapter)).await.unwrap();
            let snapshot = fresh
                .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .unwrap();
            let rows = snapshot
                .open_dataset("node:Item")
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap() as u64;
            drop(tails_hidden);
            rows
        };
        let elapsed = started.elapsed();
        let after = counters.snapshot();
        let requests: u64 = after
            .iter()
            .map(|(key, value)| value - before.get(key).copied().unwrap_or(0))
            .sum();
        let wal_requests: u64 = after
            .iter()
            .filter(|((_, class), _)| *class == "wal")
            .map(|(key, value)| value - before.get(key).copied().unwrap_or(0))
            .sum();
        eprintln!(
            "cold read, {k} unfolded records: {fresh} rows (expect {}), {requests} requests ({wal_requests} on the log), {:.1} ms",
            k + 1,
            elapsed.as_secs_f64() * 1000.0
        );
        assert_eq!(fresh, k + 1);
        omnigraph_catalog::wal::FOLD_PAUSED.store(false, Ordering::SeqCst);
        omnigraph_catalog::wal::forget_all().await.unwrap();
    }
}

/// Fencing through the log (run with `OMNIGRAPH_WAL=1`): a claim is the
/// record at the next version, so after another process claims, this
/// process's next append meets the claim, reads its higher writer epoch and is
/// refused `WriterFenced`; nothing it tried lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "instrument: WAL fencing by a claim record (WAL research); run with OMNIGRAPH_WAL=1"]
async fn wal_claim_record_fences_the_previous_owner() {
    assert!(omnigraph_catalog::wal::enabled(), "run with OMNIGRAPH_WAL=1");
    let _counters = counters(Duration::ZERO);
    let uri = format!("shared-memory://wt-fence-{}/graph", std::process::id());
    let adapter: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
    let db = Omnigraph::init_with_storage(&uri, SCHEMA, Arc::clone(&adapter), InitOptions::default())
        .await
        .unwrap();
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    let epoch = session.db().claim_branch_writer("main").await.unwrap();
    for i in 0..3 {
        session
            .mutate("main", QUERIES, "put", &params(&format!("before-{i}"), i, None))
            .await
            .unwrap();
    }
    let foreign = omnigraph_catalog::wal::forge_foreign_claim_for_tests(&uri, None)
        .await
        .unwrap();
    assert_eq!(foreign, epoch + 1);
    let refused = session
        .mutate("main", QUERIES, "put", &params("after-takeover", 9, None))
        .await
        .expect_err("a write after another process's claim must be refused");
    let text = format!("{refused:?}");
    eprintln!("refused: {refused}");
    assert!(text.contains("WriterFenced"), "expected WriterFenced, got {text}");
    omnigraph_catalog::wal::forget_all().await.unwrap();
    drop(session);
    let fresh = Omnigraph::open_with_storage(&uri, Arc::clone(&adapter)).await.unwrap();
    let snapshot = fresh
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let rows = snapshot
        .open_dataset("node:Item")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    eprintln!("rows after the takeover: {rows} (expect 3)");
    assert_eq!(rows, 3);
}
