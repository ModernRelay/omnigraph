//! `--measure`: every object-store request a DST case makes, in both of the
//! engine's realms, attributed to the step that made it.
//!
//! The Lance realm: the worker installs one decorator on
//! `omnigraph::object_store_seam::OBJECT_STORE`, the seam the DST fault
//! decorator uses and the GQT worker otherwise leaves empty, so every store
//! the engine's registry builds is wrapped: `__manifest` and table traffic,
//! spawned tasks and cached handles all pass through one ledger. The control
//! realm: init claims, capability probes, manifest-root preflights and
//! graph-index artifacts go through the engine's `StorageAdapter`; legacy
//! schema and recovery paths also have control classes. The live schema
//! contract is inline in `__manifest`, in the Lance realm. The adapter's
//! DST store is a second in-memory
//! object store the registry never builds; the worker wraps the adapter it
//! hands the engine ([`wrap_adapter`], the `control` module) and logs each
//! call as the requests that adapter makes for it, under `control_<kind>`
//! classes. The ledger keeps
//! every request of the run, each tagged with the label current when it was
//! made (`setup`, `step` N, `runner` N); the runner only moves the label, and
//! the report groups the ledger by it, so no request can fall outside a row.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use omnigraph::object_store_seam::DecorateObjectStore;
use omnigraph::seams::Behavior;

mod control;

pub(crate) use control::wrap_adapter;

/// One store request as the ledger keeps it. `path` is the object name with
/// uuids redacted, so the two runs of one seed log the same text. `tick` is the
/// virtual tick the request started at, counted from its group's first request
/// in the report, so the log is a timeline: requests with one tick ran together.
/// `range` is the byte range a `get` asked for, so two reads of one object are
/// the same read only when they ask for the same bytes.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct Request {
    pub verb: &'static str,
    pub class: String,
    pub dataset: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    pub bytes: u64,
    pub tick: u64,
    /// Microseconds of virtual time the request took under the model.
    pub cost_us: u64,
    /// The phase the request started in: the name of the last decision seam
    /// the engine crossed in the step, `start` before the first.
    pub phase: &'static str,
    #[serde(skip)]
    pub start_us: u64,
    /// The object name before redaction: two data files whose uuids differ
    /// are two objects, and only the raw name tells them apart.
    #[serde(skip)]
    pub raw_path: String,
    #[serde(skip)]
    pub label: Label,
}

/// What the runner was doing when a request was made: `setup` before the
/// first step, `step` N while step N ran, `runner` N once it finished (the
/// runner's own checks until the next step begins). `kind` is the step's
/// kind (`mutate`, `query`, `restart`, …), `runner` for the gaps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Label {
    pub slot: &'static str,
    pub step: u64,
    pub line: Option<u64>,
    pub kind: &'static str,
}

tokio::task_local! {
    /// The concurrent-block session whose future is being polled: the
    /// ledger labels its requests with it, the block's gate attributes them
    /// to it, and evidence rows carry its label.
    pub(crate) static SESSION: SessionCtx;
}

pub(crate) struct SessionCtx {
    pub index: usize,
    pub label: Label,
    /// The last decision seam the engine crossed in this session (the
    /// measure phase), per session rather than per process.
    pub phase: Cell<&'static str>,
    /// Set by the block once it is aborted: every later request of the
    /// session is the drain, filed under [`AFTER_ABORT_PHASE`].
    pub draining: Arc<AtomicBool>,
}

impl SessionCtx {
    pub(crate) fn new(index: usize, label: Label, draining: Arc<AtomicBool>) -> Self {
        Self {
            index,
            label,
            phase: Cell::new(START_PHASE),
            draining,
        }
    }
}

/// The measure phase of a request made while a block's sessions drain.
pub(crate) const AFTER_ABORT_PHASE: &str = "after_abort";

/// What one request costs on the paused DST clock: a base latency plus the
/// bytes moved at a bandwidth. Requests issued together overlap (a synchronous
/// in-memory store never lets them), so a group's elapsed virtual time is its
/// critical path under the model, never a per-device queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Model {
    pub name: &'static str,
    base_us: u64,
    /// Bytes per microsecond; `None` moves bytes for free.
    bytes_per_us: Option<u64>,
}

/// `unit`: one tick per request, the model every count is reported under.
/// `s3-like`: the 17 ms per request slope measured on the 0136 branch-age
/// chart and Durner et al.'s 50 MiB/s per request, until a calibration run
/// on the real store replaces them. `local`: a local file system.
pub(crate) const MODELS: [Model; 3] = [
    Model {
        name: "unit",
        base_us: 1_000,
        bytes_per_us: None,
    },
    Model {
        name: "s3-like",
        base_us: 17_000,
        bytes_per_us: Some(52),
    },
    Model {
        name: "local",
        base_us: 100,
        bytes_per_us: Some(1_074),
    },
];

impl Model {
    pub(crate) fn named(name: &str) -> Option<Model> {
        MODELS.into_iter().find(|model| model.name == name)
    }

    fn cost_us(&self, bytes: u64) -> u64 {
        self.base_us + self.transfer_us(bytes)
    }

    /// The time the bytes take on the wire, charged when they are known: before
    /// the call for a write or a bounded read, after it for a whole-object read.
    fn transfer_us(&self, bytes: u64) -> u64 {
        self.bytes_per_us.map_or(0, |per_us| bytes / per_us)
    }
}

/// Virtual microseconds as ticks, a started tick counting whole.
fn ticks(us: u64) -> u64 {
    us.div_ceil(TICK.as_micros() as u64)
}

/// The length of the union of the spans `(start, end)`: the time at least
/// one of them was in flight.
fn union_us(spans: &[(u64, u64)]) -> u64 {
    let mut spans = spans.to_vec();
    spans.sort_unstable();
    let mut total = 0;
    let mut current: Option<(u64, u64)> = None;
    for (start, end) in spans {
        match current {
            Some((_, ref mut open)) if start <= *open => *open = (*open).max(end),
            _ => {
                if let Some((s, e)) = current {
                    total += e - s;
                }
                current = Some((start, end));
            }
        }
    }
    if let Some((s, e)) = current {
        total += e - s;
    }
    total
}

/// The tick: one millisecond of virtual time, the `unit` model's request cost.
pub(crate) const TICK: Duration = Duration::from_millis(1);

/// S3 list prices, microdollars per request: PUT, COPY, POST and LIST at
/// $0.005 per thousand, GET and HEAD at $0.0004 per thousand, DELETE free. A
/// refused request is billed like its verb.
fn usd_micro(verb: &str) -> f64 {
    match verb.trim_end_matches("_failed") {
        "put" | "put_part" | "put_multipart" | "put_complete" | "copy" | "list" => 5.0,
        "get" | "head" => 0.4,
        _ => 0.0,
    }
}

/// Keys per list page on S3 and the stores that copy it.
const LIST_PAGE: usize = 1_000;

/// The phase every step starts in, before the engine crosses a seam.
pub(crate) const START_PHASE: &str = "start";

#[derive(Debug)]
struct Measure {
    model: Model,
    ledger: Mutex<Vec<Request>>,
    label: Mutex<Label>,
    /// Every label the runner set, in order: the report has a row per label
    /// whether or not a request was made under it.
    labels: Mutex<Vec<Label>>,
    phase: Mutex<&'static str>,
    epoch: OnceLock<tokio::time::Instant>,
    details: Mutex<Vec<serde_json::Value>>,
}

const SETUP: Label = Label {
    slot: "setup",
    step: 0,
    line: None,
    kind: "setup",
};

static MEASURE: OnceLock<Arc<Measure>> = OnceLock::new();

/// The measuring decorator for the rest of this process under `model`, for
/// the worker to install on the object-store seam (inside the concurrent
/// block's gate, `concurrent::install`). The first call's model wins.
pub(crate) fn prepare(model: Model) -> Arc<dyn DecorateObjectStore> {
    let measure = Arc::clone(MEASURE.get_or_init(|| {
        Arc::new(Measure {
            model,
            ledger: Mutex::new(Vec::new()),
            label: Mutex::new(SETUP),
            labels: Mutex::new(vec![SETUP]),
            phase: Mutex::new(START_PHASE),
            epoch: OnceLock::new(),
            details: Mutex::new(Vec::new()),
        })
    }));
    measure.epoch.get_or_init(tokio::time::Instant::now);
    Arc::new(MeasureDecorator { measure })
}

/// A block session's label: its label as the slot (leaked for the process,
/// like every slot name), the block's step and the session's kind,
/// registered so the report has a row per session with zero requests.
pub(crate) fn session_label(
    session: &str,
    step: u64,
    line: Option<u64>,
    kind: &'static str,
) -> Label {
    let label = Label {
        slot: Box::leak(session.to_string().into_boxed_str()),
        step,
        line,
        kind,
    };
    if let Some(measure) = MEASURE.get() {
        let mut labels = measure.labels.lock().unwrap();
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    label
}

/// The model the process measures under, when it measures.
pub(crate) fn model() -> Option<Model> {
    MEASURE.get().map(|measure| measure.model)
}

/// Move the label every later request is tagged with, and start a new
/// phase. A no-op when measuring is off.
pub(crate) fn set_label(slot: &'static str, step: u64, line: Option<u64>, kind: &'static str) {
    if let Some(measure) = MEASURE.get() {
        let label = Label {
            slot,
            step,
            line,
            kind,
        };
        *measure.label.lock().unwrap() = label;
        let mut labels = measure.labels.lock().unwrap();
        if !labels.contains(&label) {
            labels.push(label);
        }
        *measure.phase.lock().unwrap() = START_PHASE;
    }
}

/// The engine crossed the decision seam `name`: every later request of the
/// step is in that phase. Inside a concurrent block's session the phase is
/// the session's own. A no-op when measuring is off.
pub(crate) fn cross(name: &'static str) {
    if SESSION.try_with(|session| session.phase.set(name)).is_ok() {
        return;
    }
    if let Some(measure) = MEASURE.get() {
        *measure.phase.lock().unwrap() = name;
    }
}

/// One group of the ledger: the requests made under one label.
pub(crate) struct Group {
    pub label: Label,
    pub io: StepIo,
}

/// The whole run's ledger grouped by label, one group per label the runner
/// set in the order it set them (a step that made no request is a group of
/// zero requests, so zero and missing stay distinct); empty when measuring
/// is off. Takes the ledger and the labels.
pub(crate) fn finish() -> Vec<Group> {
    let Some(measure) = MEASURE.get() else {
        return Vec::new();
    };
    let requests = std::mem::take(&mut *measure.ledger.lock().unwrap());
    let mut order = std::mem::take(&mut *measure.labels.lock().unwrap());
    let mut groups: BTreeMap<Label, Vec<Request>> = BTreeMap::new();
    for request in requests {
        if !order.contains(&request.label) {
            order.push(request.label);
        }
        groups.entry(request.label).or_default().push(request);
    }
    order
        .into_iter()
        .map(|label| Group {
            label,
            io: StepIo::from(groups.remove(&label).unwrap_or_default()),
        })
        .collect()
}

pub(crate) fn push_detail(value: serde_json::Value) {
    if let Some(measure) = MEASURE.get() {
        measure.details.lock().unwrap().push(value);
    }
}

pub(crate) fn take_details() -> Vec<serde_json::Value> {
    MEASURE.get().map_or_else(Vec::new, |measure| {
        std::mem::take(&mut *measure.details.lock().unwrap())
    })
}

/// One phase of a step in the work-span model: the requests made since the
/// engine crossed the named decision seam (`start` before the first
/// crossing), with `makespan` the ticks from its first request's start to
/// its last request's end, `span` the critical path (the phase's non-table
/// requests in series plus the longest single table's time, the tables
/// being independent within a phase), `requests` the work. Every time is
/// the model's: a request's span runs from its start to its end, so the
/// columns share one unit under every model. Phases come from the engine's
/// own seams, in the order it crossed them; nothing is inferred from the
/// log's shape.
#[derive(Debug, serde::Serialize)]
pub(crate) struct PhaseIo {
    pub phase: &'static str,
    pub makespan: u64,
    pub span: u64,
    pub requests: u64,
    pub tables: u64,
}

/// A request into a table's realm. The realm, not the dataset, decides: a
/// branch's `__manifest` lineage lives under `__manifest/tree/<branch>/` and
/// its dataset is the branch name.
fn is_table(request: &Request) -> bool {
    request.class.starts_with("table")
}

fn is_publish(request: &Request) -> bool {
    request.class == "manifest_meta" && request.verb == "put"
}

/// Split a group's log by the phase each request started in, phases in the
/// order of their first request; a group that crossed no seam is one phase,
/// `start`, and reports none.
fn phases(log: &[Request]) -> Vec<PhaseIo> {
    if log.iter().all(|request| request.phase == START_PHASE) {
        return Vec::new();
    }
    let mut order: Vec<&'static str> = Vec::new();
    let mut bounds: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut requests: BTreeMap<&str, u64> = BTreeMap::new();
    let mut serial: BTreeMap<&str, Vec<(u64, u64)>> = BTreeMap::new();
    let mut per_table: BTreeMap<(&str, &str), Vec<(u64, u64)>> = BTreeMap::new();
    for request in log {
        if !order.contains(&request.phase) {
            order.push(request.phase);
        }
        let span = (request.start_us, request.start_us + request.cost_us);
        let bound = bounds.entry(request.phase).or_insert(span);
        *bound = (bound.0.min(span.0), bound.1.max(span.1));
        *requests.entry(request.phase).or_default() += 1;
        if is_table(request) {
            per_table
                .entry((request.phase, request.dataset.as_str()))
                .or_default()
                .push(span);
        } else {
            serial.entry(request.phase).or_default().push(span);
        }
    }
    order
        .into_iter()
        .map(|phase| {
            let tables: Vec<u64> = per_table
                .iter()
                .filter(|((p, _), _)| *p == phase)
                .map(|(_, spans)| union_us(spans))
                .collect();
            let longest_table = tables.iter().copied().max().unwrap_or(0);
            let serial_us = serial.get(phase).map_or(0, |spans| union_us(spans));
            let (first, last) = bounds[phase];
            PhaseIo {
                phase,
                makespan: ticks(last - first),
                span: ticks(serial_us + longest_table),
                requests: requests[phase],
                tables: tables.len() as u64,
            }
        })
        .collect()
}

pub(crate) struct StepIo {
    /// The work: requests, one tick each under the unit model.
    requests: u64,
    /// Ticks from the first request's start to the last request's end: how
    /// long the schedule took under the model.
    makespan: u64,
    /// Reads of an object and range the group had already read: the cache
    /// misses a reader pays for the same bytes twice.
    repeat_reads: u64,
    /// Requests after the group's last `__manifest` version put, the publish
    /// CAS: the crash window, where a crash leaves a published operation
    /// unfinished. `None` when the group published nothing.
    after_publish: Option<u64>,
    /// Virtual microseconds from the first request's start to the last
    /// request's end under the model.
    simulated_us: u64,
    /// The group's requests at S3 list prices, in microdollars.
    usd_micro: f64,
    bytes_read: u64,
    bytes_written: u64,
    by_class: BTreeMap<String, u64>,
    phases: Vec<PhaseIo>,
    log: Vec<Request>,
}

impl From<Vec<Request>> for StepIo {
    fn from(mut log: Vec<Request>) -> Self {
        let first_tick = log.iter().map(|r| r.tick).min().unwrap_or(0);
        let first_us = log.iter().map(|r| r.start_us).min().unwrap_or(0);
        for request in &mut log {
            request.tick -= first_tick;
            request.start_us -= first_us;
        }
        let mut by_class = BTreeMap::new();
        let (mut bytes_read, mut bytes_written) = (0, 0);
        let mut seen_reads = BTreeSet::new();
        let mut repeat_reads = 0;
        let mut usd = 0.0;
        let mut end_us = 0;
        for request in &log {
            *by_class
                .entry(format!("{}.{}", request.class, request.verb))
                .or_insert(0) += 1;
            match request.verb {
                "get" | "head" | "list" | "get_failed" | "head_failed" | "list_failed" => {
                    bytes_read += request.bytes;
                }
                _ => bytes_written += request.bytes,
            }
            if matches!(request.verb, "get" | "head")
                && !seen_reads.insert((request.raw_path.clone(), request.range.clone()))
            {
                repeat_reads += 1;
            }
            usd += usd_micro(request.verb);
            end_us = end_us.max(request.start_us + request.cost_us);
        }
        let after_publish = log
            .iter()
            .rposition(is_publish)
            .map(|publish| (log.len() - publish - 1) as u64);
        Self {
            requests: log.len() as u64,
            makespan: ticks(end_us),
            repeat_reads,
            after_publish,
            simulated_us: end_us,
            usd_micro: usd,
            bytes_read,
            bytes_written,
            by_class,
            phases: phases(&log),
            log,
        }
    }
}

impl StepIo {
    /// The counts: a function of code and seed, compared across the two runs
    /// of one seed like every other evidence row. `span` is the critical path
    /// under the phase model (phases in sequence, tables independent within
    /// one): the sum of the phases' spans, absent when the group is not a
    /// mutation.
    pub(crate) fn counts(&self) -> serde_json::Value {
        let span =
            (!self.phases.is_empty()).then(|| self.phases.iter().map(|p| p.span).sum::<u64>());
        serde_json::json!({
            "requests": self.requests,
            "repeat_reads": self.repeat_reads,
            "makespan": self.makespan,
            "span": span,
            "after_publish": self.after_publish,
            "by_class": self.by_class,
            "phases": self.phases,
        })
    }

    /// Bytes, the model's time and price, and the request log. Lance stamps
    /// manifests with wall-clock time, so sizes may differ between two runs;
    /// these rows stay out of the replay comparison.
    pub(crate) fn detail(&self) -> serde_json::Value {
        serde_json::json!({
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "simulated_us": self.simulated_us,
            "usd_micro": self.usd_micro,
            "log": self.log,
        })
    }
}

/// `<realm>_<kind>`: the realm is the dataset (`manifest` for `__manifest`,
/// `recovery` for the sidecar root, `table` otherwise), the kind the Lance
/// directory the object lives in.
fn classify(path: &str) -> String {
    let mut realm = "table";
    let mut kind = "other";
    for segment in path.split('/') {
        match segment {
            "__manifest" => realm = "manifest",
            "__recovery" => realm = "recovery",
            "_versions" => kind = "meta",
            "_transactions" => kind = "txn",
            "data" => kind = "data",
            "_indices" => kind = "index",
            "_deletions" => kind = "deletion",
            "_refs" => kind = "ref",
            _ => {}
        }
    }
    if kind == "other" && path.ends_with(".manifest") {
        kind = "meta";
    }
    format!("{realm}_{kind}")
}

/// The control realm: the objects the engine reaches through its
/// `StorageAdapter`, a store of its own under DST.
const CONTROL_REALM: &str = "control";

/// The name the engine gives the transient object it writes and deletes to
/// prove a local root supports create-if-absent (one per read-write bind).
const PROBE_PREFIX: &str = "__create_if_absent_probe_";

/// `control_<kind>`, the kind being the object's role read off its name
/// (the control realm has no Lance directories); the roles are pinned by
/// `control_objects_are_classed_by_their_role` and listed in the README.
fn control_class(key: &str) -> String {
    let name = key.rfind('/').map_or(key, |slash| &key[slash + 1..]);
    let live_name = name.strip_suffix(".staging").unwrap_or(name);
    let under = |dir: &str| key.split('/').any(|segment| segment == dir);
    let kind = if matches!(
        live_name,
        "_schema.pg" | "_schema.ir.json" | "__schema_state.json"
    ) {
        "schema"
    } else if under("__recovery") {
        "recovery"
    } else if under("__graph_index") {
        "graph_index"
    } else if under("__manifest") {
        "manifest"
    } else if name == "__init_claim.json" {
        "claim"
    } else if name.starts_with(PROBE_PREFIX) {
        "probe"
    } else {
        "other"
    };
    format!("{CONTROL_REALM}_{kind}")
}

/// The object key the adapter's store sees for a URI: the scheme dropped,
/// the way the in-memory adapter keys `shared-memory://<root>/<file>`.
fn control_key(uri: &str) -> &str {
    uri.split_once("://")
        .map_or(uri, |(_, key)| key)
        .trim_start_matches('/')
}

/// What a request was made on, as the ledger files it: a Lance object by its
/// path, a control object by its role, its real name realm-prefixed so the
/// repeat-read key never meets the same key of the other store.
struct Object {
    class: String,
    dataset: String,
    path: String,
    raw_path: String,
}

impl Object {
    fn lance(path: &str) -> Self {
        Self {
            class: classify(path),
            dataset: dataset(path),
            path: redact(path),
            raw_path: path.to_string(),
        }
    }

    fn control(uri: &str) -> Self {
        let key = control_key(uri);
        Self {
            class: control_class(key),
            dataset: CONTROL_REALM.to_string(),
            path: redact(key),
            raw_path: format!("{CONTROL_REALM}:{key}"),
        }
    }
}

/// The dataset directory the object belongs to: the segment before the first
/// Lance directory (`_versions`, `data`, …), else the object's own directory
/// (`_latest.manifest` at a dataset root, a file under `__recovery`).
fn dataset(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    let lance_dir = segments.iter().position(|segment| {
        matches!(
            *segment,
            "_versions" | "_transactions" | "data" | "_indices" | "_deletions" | "_refs"
        )
    });
    let index = match lance_dir {
        Some(0) | None => segments.len().saturating_sub(2),
        Some(index) => index - 1,
    };
    segments.get(index).copied().unwrap_or("").to_string()
}

/// Replace uuid segments (`data/<uuid>.lance`, `_transactions/<n>-<uuid>.txn`)
/// and the create-if-absent probe's ulid so the log reads the same on every
/// run of a seed.
fn redact(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with(PROBE_PREFIX) {
                return format!("{PROBE_PREFIX}<ulid>");
            }
            let (stem, ext) = match segment.rfind('.') {
                Some(dot) => (&segment[..dot], &segment[dot..]),
                None => (segment, ""),
            };
            let (prefix, tail) = match stem.split_once('-') {
                Some((left, right)) if left.chars().all(|c| c.is_ascii_digit()) => {
                    (Some(left), right)
                }
                _ => (None, stem),
            };
            let hex = tail.chars().filter(|c| c.is_ascii_hexdigit()).count();
            if hex >= 32 && tail.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
                match prefix {
                    Some(prefix) => format!("{prefix}-<uuid>{ext}"),
                    None => format!("<uuid>{ext}"),
                }
            } else {
                segment.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// A request the store refused (a `head` on an absent ref, a conditional put
/// that lost) is a round trip all the same; it is logged under its own verb.
fn failed(verb: &'static str) -> &'static str {
    match verb {
        "get" => "get_failed",
        "head" => "head_failed",
        "put" => "put_failed",
        "put_part" => "put_part_failed",
        "put_multipart" => "put_multipart_failed",
        "put_complete" => "put_complete_failed",
        "put_abort" => "put_abort_failed",
        "copy" => "copy_failed",
        "delete" => "delete_failed",
        "list" => "list_failed",
        _ => "failed",
    }
}

/// The verb a result is logged under.
fn verb_of<T, E>(verb: &'static str, result: &Result<T, E>) -> &'static str {
    if result.is_err() { failed(verb) } else { verb }
}

/// The range a `get` asks for, as the ledger spells it; a whole-object read
/// has none.
fn range_text(range: Option<&GetRange>) -> Option<String> {
    range.map(|range| match range {
        GetRange::Bounded(range) => format!("{}-{}", range.start, range.end),
        GetRange::Offset(offset) => format!("{offset}-"),
        GetRange::Suffix(suffix) => format!("-{suffix}"),
    })
}

/// The bytes a `get` will move when the range says so before the call
/// (`None` for a whole-object or offset read, whose size the result tells).
fn range_bytes(range: Option<&GetRange>) -> Option<u64> {
    match range {
        Some(GetRange::Bounded(range)) => Some(range.end.saturating_sub(range.start)),
        Some(GetRange::Suffix(suffix)) => Some(*suffix),
        Some(GetRange::Offset(_)) | None => None,
    }
}

impl Measure {
    /// Append a Lance-realm request; its index names it for a later relabel.
    fn note(
        &self,
        started: &Started,
        verb: &'static str,
        path: &str,
        bytes: u64,
        range: Option<String>,
    ) -> usize {
        self.note_object(started, verb, Object::lance(path), bytes, range)
    }

    fn note_object(
        &self,
        started: &Started,
        verb: &'static str,
        object: Object,
        bytes: u64,
        range: Option<String>,
    ) -> usize {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.push(Request {
            verb,
            class: object.class,
            dataset: object.dataset,
            path: object.path,
            range,
            bytes,
            tick: started.start_us / (TICK.as_micros() as u64),
            cost_us: started.cost_us,
            phase: started.phase,
            start_us: started.start_us,
            raw_path: object.raw_path,
            label: started.label,
        });
        ledger.len() - 1
    }

    /// A listing's first page is noted when the stream is asked for; the
    /// page's outcome arrives with its first item, and a refusal relabels it.
    fn relabel(&self, index: usize, verb: &'static str) {
        if let Some(request) = self.ledger.lock().unwrap().get_mut(index) {
            request.verb = verb;
        }
    }
}

struct MeasureDecorator {
    measure: Arc<Measure>,
}

impl Behavior for MeasureDecorator {}

impl DecorateObjectStore for MeasureDecorator {
    fn wrap(&self, base: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(MeasureStore {
            inner: base,
            measure: Arc::clone(&self.measure),
        })
    }
}

/// When a request began on the virtual clock, what it cost under the model,
/// and the label current at that moment.
struct Started {
    start_us: u64,
    cost_us: u64,
    label: Label,
    phase: &'static str,
}

impl Started {
    /// Reads the clock, the label and the phase (a block session's own while
    /// it is polled, `after_abort` once its block is aborted), then sleeps the
    /// request's cost on the paused clock: the bytes are known before the call.
    async fn enter(measure: &Arc<Measure>, bytes: u64) -> Self {
        let epoch = *measure.epoch.get_or_init(tokio::time::Instant::now);
        let start_us = tokio::time::Instant::now()
            .saturating_duration_since(epoch)
            .as_micros() as u64;
        let (label, phase) = SESSION
            .try_with(|session| {
                let phase = if session.draining.load(std::sync::atomic::Ordering::Relaxed) {
                    AFTER_ABORT_PHASE
                } else {
                    session.phase.get()
                };
                (session.label, phase)
            })
            .unwrap_or_else(|_| {
                (
                    *measure.label.lock().unwrap(),
                    *measure.phase.lock().unwrap(),
                )
            });
        let cost_us = measure.model.cost_us(bytes);
        tokio::time::sleep(Duration::from_micros(cost_us)).await;
        Started {
            start_us,
            cost_us,
            label,
            phase,
        }
    }

    /// Bytes known only once the call returned (a whole-object read): their
    /// time on the wire, charged after it.
    async fn charge(&mut self, model: &Model, bytes: u64) {
        let transfer = model.transfer_us(bytes);
        if transfer > 0 {
            tokio::time::sleep(Duration::from_micros(transfer)).await;
            self.cost_us += transfer;
        }
    }
}

#[derive(Debug)]
struct MeasureStore {
    inner: Arc<dyn ObjectStore>,
    measure: Arc<Measure>,
}

impl fmt::Display for MeasureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GqtMeasured({})", self.inner)
    }
}

/// A multipart upload counted the way S3 bills it: the create call, one
/// request per part carrying its bytes, and the complete or abort call.
#[derive(Debug)]
struct MeasuredUpload {
    inner: Box<dyn MultipartUpload>,
    measure: Arc<Measure>,
    path: String,
}

#[async_trait]
impl MultipartUpload for MeasuredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let measure = Arc::clone(&self.measure);
        let path = self.path.clone();
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let started = Started::enter(&measure, bytes).await;
            let result = part.await;
            measure.note(&started, verb_of("put_part", &result), &path, bytes, None);
            result
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let started = Started::enter(&self.measure, 0).await;
        let result = self.inner.complete().await;
        self.measure.note(
            &started,
            verb_of("put_complete", &result),
            &self.path,
            0,
            None,
        );
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let started = Started::enter(&self.measure, 0).await;
        let result = self.inner.abort().await;
        self.measure
            .note(&started, verb_of("put_abort", &result), &self.path, 0, None);
        result
    }
}

#[async_trait]
impl ObjectStore for MeasureStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes = payload.content_length() as u64;
        let started = Started::enter(&self.measure, bytes).await;
        let result = self.inner.put_opts(location, payload, opts).await;
        self.measure.note(
            &started,
            verb_of("put", &result),
            location.as_ref(),
            bytes,
            None,
        );
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let started = Started::enter(&self.measure, 0).await;
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.measure.note(
            &started,
            verb_of("put_multipart", &result),
            location.as_ref(),
            0,
            None,
        );
        Ok(Box::new(MeasuredUpload {
            inner: result?,
            measure: Arc::clone(&self.measure),
            path: location.as_ref().to_string(),
        }))
    }

    /// A read's bytes are charged when they are known: a bounded or suffix
    /// range before the call, a whole-object or offset read after it, once
    /// the result says how much came back.
    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let verb = if options.head { "head" } else { "get" };
        let range = range_text(options.range.as_ref());
        let expected = if verb == "get" {
            range_bytes(options.range.as_ref())
        } else {
            Some(0)
        };
        let mut started = Started::enter(&self.measure, expected.unwrap_or(0)).await;
        let result = self.inner.get_opts(location, options).await;
        let bytes = match &result {
            Ok(result) if verb == "get" => result.range.end.saturating_sub(result.range.start),
            _ => 0,
        };
        if expected.is_none() {
            started.charge(&self.measure.model, bytes).await;
        }
        self.measure.note(
            &started,
            verb_of(verb, &result),
            location.as_ref(),
            bytes,
            range,
        );
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<OsPath>>,
    ) -> BoxStream<'static, object_store::Result<OsPath>> {
        let inner = Arc::clone(&self.inner);
        let measure = Arc::clone(&self.measure);
        locations
            .then(move |path| {
                let inner = Arc::clone(&inner);
                let measure = Arc::clone(&measure);
                async move {
                    let path = path?;
                    let started = Started::enter(&measure, 0).await;
                    let result = inner.delete(&path).await;
                    measure.note(&started, verb_of("delete", &result), path.as_ref(), 0, None);
                    result?;
                    Ok(path)
                }
            })
            .boxed()
    }

    /// One request per page of [`LIST_PAGE`] keys: the first page is asked
    /// for before the stream yields, every further page as the stream
    /// crosses into it.
    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let measure = Arc::clone(&self.measure);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let path = prefix
                .as_ref()
                .map(|p| p.as_ref())
                .unwrap_or_default()
                .to_string();
            let started = Started::enter(&measure, 0).await;
            let first_page = measure.note(&started, "list", &path, 0, None);
            let pages = Arc::clone(&measure);
            inner
                .list(prefix.as_ref())
                .enumerate()
                .then(move |(index, item)| {
                    let measure = Arc::clone(&pages);
                    let path = path.clone();
                    async move {
                        if index == 0 && item.is_err() {
                            measure.relabel(first_page, failed("list"));
                        }
                        if index > 0 && index % LIST_PAGE == 0 {
                            let started = Started::enter(&measure, 0).await;
                            measure.note(&started, "list", &path, 0, None);
                        }
                        item
                    }
                })
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        let path = prefix.map(|p| p.as_ref()).unwrap_or_default();
        let started = Started::enter(&self.measure, 0).await;
        let result = self.inner.list_with_delimiter(prefix).await;
        self.measure
            .note(&started, verb_of("list", &result), path, 0, None);
        let result = result?;
        let keys = result.objects.len() + result.common_prefixes.len();
        for _ in 1..keys.div_ceil(LIST_PAGE) {
            let started = Started::enter(&self.measure, 0).await;
            self.measure.note(&started, "list", path, 0, None);
        }
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let started = Started::enter(&self.measure, 0).await;
        let result = self.inner.copy_opts(from, to, options).await;
        self.measure
            .note(&started, verb_of("copy", &result), from.as_ref(), 0, None);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{Label, Model, TICK, classify, dataset, redact};

    fn req(verb: &'static str, class: &str, dataset: &str, tick: u64) -> super::Request {
        req_in(verb, class, dataset, tick, super::START_PHASE)
    }

    fn req_in(
        verb: &'static str,
        class: &str,
        dataset: &str,
        tick: u64,
        phase: &'static str,
    ) -> super::Request {
        super::Request {
            verb,
            class: class.to_string(),
            dataset: dataset.to_string(),
            path: String::new(),
            range: None,
            bytes: 0,
            tick,
            cost_us: 1_000,
            phase,
            start_us: tick * 1_000,
            raw_path: format!("{dataset}/{class}/{tick}"),
            label: Label {
                slot: "step",
                step: 1,
                line: None,
                kind: "mutate",
            },
        }
    }

    fn read(path: &str, range: Option<&str>, tick: u64) -> super::Request {
        super::Request {
            verb: "get",
            class: classify(path),
            dataset: dataset(path),
            path: redact(path),
            range: range.map(str::to_string),
            bytes: 0,
            tick,
            cost_us: 1_000,
            phase: super::START_PHASE,
            start_us: tick * 1_000,
            raw_path: path.to_string(),
            label: Label {
                slot: "step",
                step: 1,
                line: None,
                kind: "query",
            },
        }
    }

    #[test]
    fn classes_follow_the_dataset_and_directory() {
        assert_eq!(
            classify("gqt-dst/case/__manifest/_versions/3.manifest"),
            "manifest_meta"
        );
        assert_eq!(
            classify("gqt-dst/case/__manifest/data/abc.lance"),
            "manifest_data"
        );
        assert_eq!(
            classify("gqt-dst/case/node_Chunk/_transactions/1-x.txn"),
            "table_txn"
        );
        assert_eq!(
            classify("gqt-dst/case/node_Chunk/_refs/branches/b0.json"),
            "table_ref"
        );
        assert_eq!(
            classify("gqt-dst/case/node_Chunk/_latest.manifest"),
            "table_meta"
        );
        assert_eq!(classify("gqt-dst/case/__recovery/x.json"), "recovery_other");
    }

    #[test]
    fn datasets_are_the_directory_above_the_lance_directory() {
        assert_eq!(
            dataset("gqt-dst/case/__manifest/_versions/3.manifest"),
            "__manifest"
        );
        assert_eq!(
            dataset("gqt-dst/case/node_Chunk/data/abc.lance"),
            "node_Chunk"
        );
        assert_eq!(
            dataset("gqt-dst/case/node_Chunk/_refs/branches/b0.json"),
            "node_Chunk"
        );
        assert_eq!(
            dataset("gqt-dst/case/node_Chunk/_latest.manifest"),
            "node_Chunk"
        );
        assert_eq!(dataset("gqt-dst/case/__recovery/x.json"), "__recovery");
        assert_eq!(dataset("gqt-dst/case/node_Chunk/_versions"), "node_Chunk");
    }

    #[test]
    fn phases_follow_the_seams_the_engine_crossed() {
        let staged = "mutation.post_stage_pre_effect_gate";
        let committing = "fork.before_classify";
        let publishing = "mutation.post_finalize_pre_publisher";
        let head_then_opens_together_then_commits_in_series_then_the_cas = vec![
            req("list", "manifest_ref", "__manifest", 0),
            req("get", "table_meta", "a", 1),
            req("get", "table_meta", "b", 1),
            req_in("put", "table_data", "a", 2, staged),
            req_in("put", "table_data", "b", 2, staged),
            req_in("put", "table_meta", "a", 3, committing),
            req_in("put", "table_meta", "b", 4, committing),
            req_in("get", "manifest_data", "__manifest", 5, publishing),
            req_in("put", "manifest_meta", "__manifest", 6, publishing),
        ];
        let log = head_then_opens_together_then_commits_in_series_then_the_cas;
        let phases = super::phases(&log);
        let names: Vec<&str> = phases.iter().map(|p| p.phase).collect();
        assert_eq!(names, ["start", staged, committing, publishing]);
        let get = |name: &str| phases.iter().find(|p| p.phase == name).unwrap();
        assert_eq!((get("start").makespan, get("start").requests), (2, 3));
        assert_eq!(
            (get(staged).makespan, get(staged).span, get(staged).tables),
            (1, 1, 2)
        );
        assert_eq!((get(committing).makespan, get(committing).span), (2, 1));
        assert_eq!((get(publishing).makespan, get(publishing).requests), (2, 2));
        assert!(super::phases(&log[..3]).is_empty());
        let io = super::StepIo::from(log);
        let serial_head_then_longest_table = 1 + 1;
        assert_eq!(
            io.counts()["span"],
            serial_head_then_longest_table + 1 + 1 + 2
        );
        assert_eq!(io.after_publish, Some(0));
    }

    #[test]
    fn schedule_columns_are_the_models_elapsed_time() {
        let mut two_in_series_under_s3_like = vec![
            req_in(
                "get",
                "manifest_meta",
                "__manifest",
                0,
                "publish.load_state",
            ),
            req_in(
                "put",
                "manifest_meta",
                "__manifest",
                17,
                "publish.load_state",
            ),
        ];
        for request in &mut two_in_series_under_s3_like {
            request.cost_us = 17_000;
        }
        let io = super::StepIo::from(two_in_series_under_s3_like);
        assert_eq!((io.requests, io.makespan, io.simulated_us), (2, 34, 34_000));
        assert_eq!(io.counts()["span"], 34);
        assert_eq!(io.phases[0].makespan, 34);
        let two_together = vec![
            req("get", "table_meta", "a", 0),
            req("get", "table_meta", "b", 0),
        ];
        let io = super::StepIo::from(two_together);
        assert_eq!((io.makespan, io.simulated_us), (1, 1_000));
        assert_eq!(super::union_us(&[(0, 5), (3, 8), (20, 21)]), 9);
        assert_eq!(super::ticks(0), 0);
        assert_eq!(super::ticks(1), 1);
        assert_eq!(super::ticks(1_000), 1);
        assert_eq!(super::ticks(1_001), 2);
    }

    #[test]
    fn a_step_without_requests_is_a_group_of_zero() {
        let io = super::StepIo::from(Vec::new());
        assert_eq!((io.requests, io.makespan, io.repeat_reads), (0, 0, 0));
        assert_eq!(io.after_publish, None);
        assert!(io.phases.is_empty());
        assert_eq!(io.counts()["requests"], 0);
        assert!(io.counts()["span"].is_null());
    }

    #[test]
    fn a_refused_request_is_logged_under_its_failed_verb() {
        let refused: Result<(), ()> = Err(());
        let served: Result<(), ()> = Ok(());
        for verb in [
            "get",
            "head",
            "put",
            "put_part",
            "put_multipart",
            "put_complete",
            "put_abort",
            "copy",
            "delete",
            "list",
        ] {
            assert_eq!(super::verb_of(verb, &served), verb);
            assert_eq!(super::verb_of(verb, &refused), format!("{verb}_failed"));
        }
    }

    #[test]
    fn a_group_is_measured_from_its_own_first_tick() {
        let io = super::StepIo::from(vec![
            req("list", "manifest_ref", "__manifest", 40),
            req("get", "table_meta", "a", 41),
            req("get", "table_meta", "b", 41),
            req("put", "table_data", "a", 45),
        ]);
        assert_eq!((io.requests, io.makespan), (4, 6));
        assert_eq!(io.log[0].tick, 0);
        assert_eq!(io.log[3].tick, 5);
        assert_eq!(io.simulated_us, 6_000);
    }

    #[test]
    fn a_repeat_read_is_the_same_object_and_range_again() {
        let same_object_same_range_then_other_range_then_other_uuid = vec![
            read("t/__manifest/_versions/3.manifest", None, 0),
            read("t/__manifest/_versions/3.manifest", None, 1),
            read(
                "t/__manifest/data/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.lance",
                Some("0-4096"),
                2,
            ),
            read(
                "t/__manifest/data/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.lance",
                Some("4096-8192"),
                3,
            ),
            read(
                "t/__manifest/data/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.lance",
                Some("0-4096"),
                4,
            ),
            read(
                "t/__manifest/data/ffffffff-4e5f-6071-8293-a4b5c6d7e8f9.lance",
                Some("0-4096"),
                5,
            ),
        ];
        let io = super::StepIo::from(same_object_same_range_then_other_range_then_other_uuid);
        assert_eq!(io.repeat_reads, 2);
        assert_eq!(io.counts()["repeat_reads"], 2);
    }

    #[test]
    fn prices_follow_the_verb_and_the_model_the_bytes() {
        assert_eq!(super::usd_micro("put"), 5.0);
        assert_eq!(super::usd_micro("get_failed"), 0.4);
        assert_eq!(super::usd_micro("delete"), 0.0);
        let unit = Model::named("unit").unwrap();
        let s3 = Model::named("s3-like").unwrap();
        assert_eq!(unit.cost_us(1 << 20), 1_000);
        assert_eq!(s3.cost_us(0), 17_000);
        assert_eq!(s3.cost_us(52 * 1_000), 18_000);
        assert!(Model::named("fast").is_none());
    }

    pub(super) fn measuring(model: Model) -> std::sync::Arc<super::Measure> {
        std::sync::Arc::new(super::Measure {
            model,
            ledger: std::sync::Mutex::new(Vec::new()),
            label: std::sync::Mutex::new(Label {
                slot: "step",
                step: 1,
                line: None,
                kind: "query",
            }),
            labels: std::sync::Mutex::new(Vec::new()),
            phase: std::sync::Mutex::new(super::START_PHASE),
            epoch: std::sync::OnceLock::new(),
            details: std::sync::Mutex::new(Vec::new()),
        })
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_whole_object_read_is_charged_its_bytes() {
        use object_store::path::Path;
        use object_store::{ObjectStore, ObjectStoreExt as _};
        let base: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        base.put(&Path::from("o"), vec![0u8; 3_487].into())
            .await
            .unwrap();
        let measure = measuring(Model::named("s3-like").unwrap());
        let store = super::MeasureStore {
            inner: base,
            measure: std::sync::Arc::clone(&measure),
        };
        let whole = store.get(&Path::from("o")).await.unwrap();
        assert_eq!(whole.bytes().await.unwrap().len(), 3_487);
        let bounded = store.get_range(&Path::from("o"), 0..3_487).await.unwrap();
        assert_eq!(bounded.len(), 3_487);
        assert!(store.get(&Path::from("absent")).await.is_err());
        let ledger = measure.ledger.lock().unwrap();
        let cost: Vec<(&str, u64, u64)> = ledger
            .iter()
            .map(|r| (r.verb, r.bytes, r.cost_us))
            .collect();
        let bytes_on_the_wire = 3_487 / 52;
        assert_eq!(
            cost,
            [
                ("get", 3_487, 17_000 + bytes_on_the_wire),
                ("get", 3_487, 17_000 + bytes_on_the_wire),
                ("get_failed", 0, 17_000),
            ]
        );
        let clock_moved = ledger[1].start_us - ledger[0].start_us;
        let timer_granularity = TICK.as_micros() as u64;
        assert!(
            clock_moved >= ledger[0].cost_us
                && clock_moved <= ledger[0].cost_us + timer_granularity,
            "the second read starts once the first is charged, to the timer's millisecond: {clock_moved}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_listing_costs_one_request_per_page() {
        use futures::StreamExt as _;
        use object_store::path::Path;
        use object_store::{ObjectStore, ObjectStoreExt as _};
        let base: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        for i in 0..(super::LIST_PAGE + 1) {
            base.put(&Path::from(format!("d/{i}")), vec![0u8; 1].into())
                .await
                .unwrap();
        }
        let measure = measuring(Model::named("unit").unwrap());
        let store = super::MeasureStore {
            inner: base,
            measure: std::sync::Arc::clone(&measure),
        };
        let keys = store.list(Some(&Path::from("d"))).count().await;
        assert_eq!(keys, super::LIST_PAGE + 1);
        let lists = measure
            .ledger
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.verb == "list")
            .count();
        assert_eq!(lists, 2);
        let with_delimiter = store
            .list_with_delimiter(Some(&Path::from("d")))
            .await
            .unwrap();
        assert_eq!(with_delimiter.objects.len(), super::LIST_PAGE + 1);
        let lists = measure
            .ledger
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.verb == "list")
            .count();
        assert_eq!(lists, 4);
    }

    #[test]
    fn uuids_are_redacted_and_versions_kept() {
        assert_eq!(
            redact("t/data/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.lance"),
            "t/data/<uuid>.lance"
        );
        assert_eq!(
            redact("t/_transactions/12-0a1b2c3d4e5f60718293a4b5c6d7e8f9.txn"),
            "t/_transactions/12-<uuid>.txn"
        );
        assert_eq!(redact("t/_versions/12.manifest"), "t/_versions/12.manifest");
    }

    /// A class is report output a case cannot assert.
    #[test]
    fn control_objects_are_classed_by_their_role() {
        use super::{Object, control_class};
        let classes: Vec<String> = [
            "gqt-dst/case/_schema.pg",
            "gqt-dst/case/_schema.ir.json.staging",
            "gqt-dst/case/__schema_state.json",
            "gqt-dst/case/__recovery/stale.json",
            "gqt-dst/case/__graph_index/csr-current.bin",
            "gqt-dst/case/__manifest",
            "gqt-dst/case/__init_claim.json",
            "gqt-dst/case/__create_if_absent_probe_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "gqt-dst/case/nodes/Person",
        ]
        .map(control_class)
        .to_vec();
        assert_eq!(
            classes,
            [
                "control_schema",
                "control_schema",
                "control_schema",
                "control_recovery",
                "control_graph_index",
                "control_manifest",
                "control_claim",
                "control_probe",
                "control_other",
            ]
        );
        let probe = Object::control(
            "shared-memory://gqt-dst/case/__create_if_absent_probe_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        );
        assert_eq!(probe.path, "gqt-dst/case/__create_if_absent_probe_<ulid>");
        assert_eq!(
            probe.raw_path,
            "control:gqt-dst/case/__create_if_absent_probe_01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
        assert_eq!(probe.dataset, "control");
        assert_eq!(
            Object::control("shared-memory://gqt-dst/case/_schema.pg").raw_path,
            "control:gqt-dst/case/_schema.pg"
        );
    }
}
