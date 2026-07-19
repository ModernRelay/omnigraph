//! Shared cost-budget test harness (RFC-013) — the single place the IO-counting
//! plumbing lives, so `warm_read_cost.rs`, `write_cost.rs`, and the S3 variant
//! assert in one vocabulary instead of duplicating `probes()` + raw `IOTracker`
//! reads. Three clean abstractions: structured counts, a `measure` primitive, a
//! named flat-assertion, plus store-agnostic backend fixtures.
//!
//! The data-table wrapper is a **path-classifying** counter (`PrefixCounter`), not a
//! plain `IOTracker`: it splits each read into the **opener** term (latest-version
//! resolution — reads of `_versions/`/`.manifest` objects) vs the **scan** term
//! (data-fragment reads, `data/`/`*.lance`). That lets a cost test isolate the
//! opener (RFC-013 step 3a's target, O(1) after the bypass) from the merge-insert/RI
//! scan (O(fragment-count), compaction's domain) even though both ride the same
//! `Dataset` — without controlling the fixture (no compaction needed). `__manifest`
//! keeps the plain `IOTracker` (no sub-prefixes worth splitting).
#![allow(dead_code)]

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::io::WrappingObjectStore;
use lance::session::Session;
use lance_io::object_store::ObjectStoreParams;
use lance_io::utils::tracking_store::IOTracker;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OSResult,
};

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{
    MergeWriteProbes, QueryIoProbes, with_merge_write_probes, with_query_io_probes,
};
use omnigraph::loader::{LoadMode, load_jsonl};

use super::{MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, init_and_load, mixed_params};

/// Open a Lance dataset with its object-store tracker installed before the
/// first manifest load. Cost fixtures must use this seam for cold-open evidence;
/// wrapping an already-open handle misses latest-manifest resolution entirely.
pub async fn open_tracked_lance_dataset(
    uri: &str,
    session: Arc<Session>,
    tracker: &IOTracker,
) -> lance::Result<Dataset> {
    DatasetBuilder::from_uri(uri)
        .with_session(session)
        .with_store_params(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(tracker.clone())),
            ..Default::default()
        })
        .load()
        .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttemptOutcome {
    Pending,
    Success,
    NotFound,
    Error,
    StreamStarted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreAttempt {
    pub method: &'static str,
    pub path: Path,
    pub outcome: AttemptOutcome,
}

/// Records object-store read attempts, including failed/NotFound HEADs.
///
/// Lance's `IOTracker` intentionally records only successful `get_opts`
/// results. Recovery classifiers need the attempted exact-key shape too,
/// because an expected NotFound is part of their proof. This wrapper records
/// before forwarding and then annotates the outcome.
#[derive(Debug, Default, Clone)]
pub struct AttemptTracker(Arc<Mutex<Vec<ObjectStoreAttempt>>>);

impl AttemptTracker {
    fn begin(&self, method: &'static str, path: Path) -> usize {
        let mut attempts = self.0.lock().unwrap();
        let index = attempts.len();
        attempts.push(ObjectStoreAttempt {
            method,
            path,
            outcome: AttemptOutcome::Pending,
        });
        index
    }

    fn finish<T>(&self, index: usize, result: &OSResult<T>) {
        let outcome = match result {
            Ok(_) => AttemptOutcome::Success,
            Err(object_store::Error::NotFound { .. }) => AttemptOutcome::NotFound,
            Err(_) => AttemptOutcome::Error,
        };
        self.0.lock().unwrap()[index].outcome = outcome;
    }

    fn record_stream(&self, method: &'static str, path: Path) {
        self.0.lock().unwrap().push(ObjectStoreAttempt {
            method,
            path,
            outcome: AttemptOutcome::StreamStarted,
        });
    }

    pub fn incremental_attempts(&self) -> Vec<ObjectStoreAttempt> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl WrappingObjectStore for AttemptTracker {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(AttemptTrackingStore {
            target,
            tracker: self.clone(),
        })
    }
}

#[derive(Debug)]
struct AttemptTrackingStore {
    target: Arc<dyn ObjectStore>,
    tracker: AttemptTracker,
}

impl fmt::Display for AttemptTrackingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AttemptTrackingStore({})", self.target)
    }
}

#[async_trait]
impl ObjectStore for AttemptTrackingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.target.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.target.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let method = if options.head { "head" } else { "get_opts" };
        let index = self.tracker.begin(method, location.clone());
        let result = self.target.get_opts(location, options).await;
        self.tracker.finish(index, &result);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.tracker
            .record_stream("list", prefix.cloned().unwrap_or_default());
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.tracker
            .record_stream("list_with_offset", prefix.cloned().unwrap_or_default());
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        let index = self
            .tracker
            .begin("list_with_delimiter", prefix.cloned().unwrap_or_default());
        let result = self.target.list_with_delimiter(prefix).await;
        self.tracker.finish(index, &result);
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.target.copy_opts(from, to, options).await
    }
}

pub async fn open_attempt_tracked_lance_dataset_at_version(
    uri: &str,
    version: u64,
    session: Arc<Session>,
    tracker: &AttemptTracker,
) -> lance::Result<Dataset> {
    DatasetBuilder::from_uri(uri)
        .with_version(version)
        .with_session(session)
        .with_store_params(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(tracker.clone())),
            ..Default::default()
        })
        .load()
        .await
}

/// Object-store op counts for one measured operation, by table class — the
/// vocabulary cost tests assert in (vs raw `IOTracker::stats().read_iops`).
#[derive(Debug, Clone, Copy, Default)]
pub struct IoCounts {
    /// Per-table DATA opens (node/edge tables). The dominant write-path term.
    pub data_reads: u64,
    pub data_writes: u64,
    /// DATA-table reads attributed to latest-version resolution (`_versions/`,
    /// `.manifest`). This is the **opener** term step 3a flattened — isolated from
    /// the scan, so it can be gated directly without compacting the fixture.
    pub data_opener_reads: u64,
    /// DATA-table reads attributed to data fragments (`data/`, `*.lance`) — the
    /// merge-insert/RI **scan**, which grows with fragment count (compaction's
    /// domain, not the opener).
    pub data_scan_reads: u64,
    /// `__manifest` registry scans (publish state; includes the graph-lineage rows
    /// folded into `__manifest` by RFC-013 Phase 7).
    pub manifest_reads: u64,
    /// Version-probe invocations (the cheap freshness check).
    pub version_probes: u64,
    /// DATA-table open CALL count through the two instrumented chokepoints — an
    /// exact open-invocation count (not the opener-read term), classified by URI so
    /// internal/system-table opens are excluded. Step-3b target:
    /// `data_open_count <= |touched_tables|` for a write.
    pub data_open_count: u64,
    /// Internal/system-table (`__manifest`) open CALL count — the complement of
    /// `data_open_count` (publisher CAS).
    pub internal_open_count: u64,
    /// Full `__manifest` row-scan invocation count. Unlike object-store reads,
    /// this distinguishes one coherent state+lineage scan from two projections
    /// over the same already-open handle.
    pub manifest_scan_count: u64,
}

impl IoCounts {
    pub fn total_reads(&self) -> u64 {
        self.data_reads + self.manifest_reads
    }
}

/// Which staged-write primitives an operation invoked (from `MergeWriteProbes`).
#[derive(Debug, Clone, Copy, Default)]
pub struct StagedCounts {
    pub stage_append: u64,
    pub stage_merge_insert: u64,
    pub stage_fenced_insert: u64,
    pub stage_vector_index: u64,
    pub scan_staged_combined: u64,
}

// ── Path-classifying data-table read counter ──

/// How a data-table object read is attributed.
enum ReadClass {
    /// Latest-version resolution: `_versions/`, `.manifest`, `_latest`.
    Opener,
    /// Data fragments: `data/`, `*.lance`.
    Scan,
    /// Anything else (indices, deletion files, …) — counted in the total only.
    Other,
}

/// Classify a Lance object path by its role in a write open. Lance's on-object
/// layout is identical on local FS and S3, so this split is backend-independent.
fn classify(path: &Path) -> ReadClass {
    let p = path.as_ref();
    if p.contains("_versions") || p.ends_with(".manifest") || p.contains("_latest") {
        ReadClass::Opener
    } else if p.contains("/data/") || p.starts_with("data/") || p.ends_with(".lance") {
        ReadClass::Scan
    } else {
        ReadClass::Other
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PrefixCounts {
    reads: u64,
    writes: u64,
    opener_reads: u64,
    scan_reads: u64,
}

/// A `WrappingObjectStore` that counts reads/writes and attributes each read to the
/// opener vs scan term by object-key prefix. Shares its tally via `Arc<Mutex<…>>` so
/// the wrapped store (handed to Lance) and the test read the same counters.
#[derive(Debug, Default, Clone)]
struct PrefixCounter(Arc<Mutex<PrefixCounts>>);

impl PrefixCounter {
    fn record_read(&self, location: &Path) {
        let mut c = self.0.lock().unwrap();
        c.reads += 1;
        match classify(location) {
            ReadClass::Opener => c.opener_reads += 1,
            ReadClass::Scan => c.scan_reads += 1,
            ReadClass::Other => {}
        }
    }

    fn record_write(&self) {
        self.0.lock().unwrap().writes += 1;
    }

    fn snapshot(&self) -> PrefixCounts {
        *self.0.lock().unwrap()
    }
}

impl WrappingObjectStore for PrefixCounter {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(PrefixCountingStore {
            target,
            counter: self.clone(),
        })
    }
}

/// The wrapped `ObjectStore` that records each call into a [`PrefixCounter`].
/// Implements only the required core `ObjectStore` methods (object_store 0.13: the
/// convenience surface — `get`/`put`/`head`/`get_range`/… — lives on
/// `ObjectStoreExt` and is provided by a blanket impl that routes through `get_opts`
/// / `put_opts`, so every read/write is still counted here). Per-read path
/// classification is the only addition over a plain pass-through.
#[derive(Debug)]
struct PrefixCountingStore {
    target: Arc<dyn ObjectStore>,
    counter: PrefixCounter,
}

impl fmt::Display for PrefixCountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrefixCountingStore({})", self.target)
    }
}

#[async_trait]
impl ObjectStore for PrefixCountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.counter.record_write();
        self.target.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.counter.record_write();
        self.target.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        self.counter.record_read(location);
        self.target.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counter
            .record_read(&prefix.cloned().unwrap_or_default());
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counter
            .record_read(&prefix.cloned().unwrap_or_default());
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.counter
            .record_read(&prefix.cloned().unwrap_or_default());
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.counter.record_write();
        self.target.copy_opts(from, to, options).await
    }
}

// ── Ground-truth `__manifest` meter (lance's per-request tracking idiom) ──
//
// Lance counts IO on a warm/cached dataset by attaching one `IOTracker` to the open
// handle (`Dataset::with_object_store_wrappers`, shared session) and reading
// `incremental_stats()` per request (`rust/lance/src/dataset/tests/dataset_io.rs`).
// We do the same for `__manifest`: `cost_harness` installs ONE persistent tracker for
// a whole test body, so the graph opens UNDER it and every coordinator handle — the
// init handle and each post-publish/refresh reassignment (`db/manifest.rs` keeps
// `self.dataset = …`) — carries the same tracker. `manifest_reads` is then ground
// truth (warm probe + cold scans), handle-age-irrelevant, instead of only the reads
// on handles a single measured op happened to open. Data/commit-graph/probe/open
// counters stay fresh per op (their warm-handle exposure is out of scope here).

/// Persistent per-test meter: owns the ground-truth `__manifest` tracker reused
/// across every `measure` in a `cost_harness` body.
#[derive(Clone, Default)]
pub struct GraphIoMeter {
    manifest: IOTracker,
    /// The most recent measured op's `__manifest` request log (method + path),
    /// stashed for `assert_io_eq!`-style failure diagnostics. Populated in
    /// ground-truth mode only (the standalone fallback has no ambient meter).
    last_manifest_log: Arc<Mutex<Vec<String>>>,
}

tokio::task_local! {
    static COST_METER: GraphIoMeter;
}

/// Run `body` with a persistent ground-truth `__manifest` tracker installed for its
/// whole lifetime. The graph MUST be opened inside `body` (e.g. via `local_graph`)
/// so its coordinator's `__manifest` handle is wrapped from birth. `measure` calls
/// inside reuse that tracker, so `manifest_reads` counts every `__manifest` read
/// regardless of which handle performed it (the warm probe included). Outside
/// `cost_harness`, `measure` falls back to a fresh per-op tracker — today's
/// fresh-open-only behavior, used by `write_cost_s3.rs`.
pub async fn cost_harness<F: Future>(body: F) -> F::Output {
    let meter = GraphIoMeter::default();
    let probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(meter.manifest.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    // Box the body so the (large) per-test future lives on the heap. Wrapping a whole
    // test body in another async layer otherwise overflows the test thread's stack —
    // these cost tests already raise `recursion_limit` for the same reason.
    COST_METER
        .scope(meter, with_query_io_probes(probes, Box::pin(body)))
        .await
}

/// Run a body with persistent raw Lance trackers for the data-table and graph-
/// manifest stores. This is the shared wiring seam for instruments that need
/// request-path attribution beyond [`IoCounts`] (for example MemWAL WAL,
/// generation-data, and PK-sidecar terms). The caller owns tracker resets and
/// classification; probe construction remains centralized here.
pub async fn with_raw_io_trackers<F: Future>(
    table: &IOTracker,
    manifest: &IOTracker,
    body: F,
) -> F::Output {
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(table.clone()) as Arc<dyn WrappingObjectStore>),
        manifest_wrapper: Some(Arc::new(manifest.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    with_query_io_probes(probes, body).await
}

/// The tracker handles backing one measurement; read once into [`IoCounts`]. Data,
/// probe, and open counters are fresh per op; the `__manifest` tracker is the ambient
/// ground-truth one when inside `cost_harness`, else fresh.
struct OpProbes {
    manifest: IOTracker,
    table: PrefixCounter,
    probe_count: Arc<AtomicU64>,
    data_open_count: Arc<AtomicU64>,
    internal_open_count: Arc<AtomicU64>,
    manifest_scan_count: Arc<AtomicU64>,
}

impl OpProbes {
    fn install() -> (QueryIoProbes, Self) {
        // Reuse the ambient ground-truth `__manifest` tracker so reads on the warm
        // coordinator handle (the freshness probe) land in it; fall back to a fresh
        // tracker when standalone. Reset it (get-and-reset) so this op's delta
        // excludes reads from init / `commit_many` between measures.
        let manifest = COST_METER
            .try_with(|m| m.manifest.clone())
            .unwrap_or_default();
        let _ = manifest.incremental_stats();
        let h = OpProbes {
            manifest,
            table: PrefixCounter::default(),
            probe_count: Arc::new(AtomicU64::new(0)),
            data_open_count: Arc::new(AtomicU64::new(0)),
            internal_open_count: Arc::new(AtomicU64::new(0)),
            manifest_scan_count: Arc::new(AtomicU64::new(0)),
        };
        let probes = QueryIoProbes {
            manifest_wrapper: Some(Arc::new(h.manifest.clone()) as Arc<dyn WrappingObjectStore>),
            table_wrapper: Some(Arc::new(h.table.clone()) as Arc<dyn WrappingObjectStore>),
            probe_count: Arc::clone(&h.probe_count),
            data_open_count: Arc::clone(&h.data_open_count),
            internal_open_count: Arc::clone(&h.internal_open_count),
            manifest_scan_count: Arc::clone(&h.manifest_scan_count),
            // graph_build_count / graph_edges_built unused by this harness.
            ..Default::default()
        };
        (probes, h)
    }

    fn counts(&self) -> IoCounts {
        let t = self.table.snapshot();
        // `incremental_stats()` (get-and-reset) yields this op's reads: in
        // ground-truth mode the tracker spans the whole test and was reset in
        // `install`; standalone it is fresh so the delta is the whole count.
        let manifest = self.manifest.incremental_stats();
        // Stash the manifest read log (method + path) on the ambient meter for
        // `assert_io_eq!`-style failure diagnostics; no-op when standalone.
        let _ = COST_METER.try_with(|meter| {
            *meter.last_manifest_log.lock().unwrap() = manifest
                .requests
                .iter()
                .map(|r| format!("{} {}", r.method, r.path))
                .collect();
        });
        IoCounts {
            data_reads: t.reads,
            data_writes: t.writes,
            data_opener_reads: t.opener_reads,
            data_scan_reads: t.scan_reads,
            manifest_reads: manifest.read_iops,
            version_probes: self.probe_count.load(Ordering::Relaxed),
            data_open_count: self.data_open_count.load(Ordering::Relaxed),
            internal_open_count: self.internal_open_count.load(Ordering::Relaxed),
            manifest_scan_count: self.manifest_scan_count.load(Ordering::Relaxed),
        }
    }
}

/// The most recent measured op's `__manifest` reads (`method path`) for failure
/// diagnostics — the `assert_io_eq!` read-log, scoped to `__manifest`. Empty
/// outside `cost_harness` (the standalone fallback records no ambient log).
pub fn last_manifest_reads() -> Vec<String> {
    COST_METER
        .try_with(|m| m.last_manifest_log.lock().unwrap().clone())
        .unwrap_or_default()
}

/// Run `op` under object-store IO counting; return its output + the counts.
/// The only place the `QueryIoProbes` task-local + tracker wiring lives.
pub async fn measure<F: Future>(op: F) -> (F::Output, IoCounts) {
    let (probes, handles) = OpProbes::install();
    let out = with_query_io_probes(probes, op).await;
    (out, handles.counts())
}

/// Like [`measure`], but also capture which staged-write primitives ran
/// (composes the two task-locals cleanly).
pub async fn measure_with_staged<F: Future>(op: F) -> (F::Output, IoCounts, StagedCounts) {
    let (probes, handles) = OpProbes::install();
    let merge = MergeWriteProbes::default();
    let out = with_merge_write_probes(merge.clone(), with_query_io_probes(probes, op)).await;
    let staged = StagedCounts {
        stage_append: merge.stage_append_calls(),
        stage_merge_insert: merge.stage_merge_insert_calls(),
        stage_fenced_insert: merge.stage_fenced_insert_calls(),
        stage_vector_index: merge.stage_vector_index_calls(),
        scan_staged_combined: merge.scan_staged_combined_calls(),
    };
    (out, handles.counts(), staged)
}

/// Assert a per-depth metric is flat: the deepest sample must not exceed the
/// shallowest by more than `slack`. `select` picks the field; `what` names it in
/// the failure message. The shape every depth-swept cost gate uses.
pub fn assert_flat(
    curve: &[(u64, IoCounts)],
    select: impl Fn(&IoCounts) -> u64,
    slack: u64,
    what: &str,
) {
    assert!(curve.len() >= 2, "assert_flat needs >= 2 depth points");
    let (d_lo, lo) = (curve[0].0, select(&curve[0].1));
    let (d_hi, hi) = (curve[curve.len() - 1].0, select(&curve[curve.len() - 1].1));
    assert!(
        hi <= lo + slack,
        "{what} grew with history: depth {d_lo} = {lo} -> depth {d_hi} = {hi} (slack {slack})"
    );
}

/// Assert a per-depth metric *does* grow with history by at least `min_delta` — the
/// dual of [`assert_flat`], used to prove a term is genuinely history-dependent (so a
/// flat sibling term isn't flat merely because nothing was measured).
pub fn assert_grows(
    curve: &[(u64, IoCounts)],
    select: impl Fn(&IoCounts) -> u64,
    min_delta: u64,
    what: &str,
) {
    assert!(curve.len() >= 2, "assert_grows needs >= 2 depth points");
    let (d_lo, lo) = (curve[0].0, select(&curve[0].1));
    let (d_hi, hi) = (curve[curve.len() - 1].0, select(&curve[curve.len() - 1].1));
    assert!(
        hi >= lo + min_delta,
        "{what} did not grow as expected: depth {d_lo} = {lo} -> depth {d_hi} = {hi} (min delta {min_delta})"
    );
}

/// Measure one committing `insert_person` to `main` — the canonical write the cost
/// gates sweep over commit-history depth. Shared by `write_cost.rs` and
/// `write_cost_s3.rs` so the measured write is defined once.
pub async fn measure_insert(db: &mut Omnigraph, tag: &str) -> IoCounts {
    let (res, io) = measure(db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", tag)], &[("$age", 30)]),
    ))
    .await;
    res.unwrap();
    io
}

/// Like [`measure_insert`] but carries an actor — the authenticated (server/CLI)
/// write path. The actor is written inline with the graph-lineage rows in
/// `__manifest` (RFC-013 Phase 7), so its scan is part of `IoCounts::manifest_reads`.
pub async fn measure_insert_as(db: &mut Omnigraph, tag: &str, actor: &str) -> IoCounts {
    let (res, io) = measure(db.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", tag)], &[("$age", 30)]),
        Some(actor),
    ))
    .await;
    res.unwrap();
    io
}

// ── Backend fixtures — one knob, store-agnostic body ──

/// Local tempdir graph (default; deterministic, every-PR).
pub async fn local_graph(dir: &tempfile::TempDir) -> Omnigraph {
    init_and_load(dir).await
}

/// Emulated-S3 graph, bucket-gated. Returns `None` **only** when
/// `OMNIGRAPH_S3_TEST_BUCKET` is unset, so the caller logs + skips — the
/// `tests/s3_storage.rs` graceful-skip pattern. Once the bucket *is* configured
/// (the rustfs CI job), any `init`/seed failure is a real failure and panics
/// rather than silently skipping — otherwise a down/misconfigured store would let
/// a bucket-gated gate pass vacuously. `name` disambiguates the prefix.
pub async fn s3_graph(name: &str) -> Option<Omnigraph> {
    let bucket = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    let uri = format!("s3://{bucket}/cost-tests/{name}-{}", std::process::id());
    let mut db = Omnigraph::init(&uri, TEST_SCHEMA)
        .await
        .expect("OMNIGRAPH_S3_TEST_BUCKET is set but S3 graph init failed");
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .expect("OMNIGRAPH_S3_TEST_BUCKET is set but S3 seed load failed");
    Some(db)
}
