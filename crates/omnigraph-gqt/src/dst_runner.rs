use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use omnigraph::error::OmniError;
use omnigraph_compiler::QueryResult;
use omnigraph_compiler::settings::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runner_config::{Environment, Execution};
use crate::{CaseOutcome, ServerTarget, admit_served, parse_case, stem_of};

pub(crate) mod seams;
mod settings;

#[cfg(tokio_unstable)]
use seams::{DECORATION, DecideGuard};
pub(crate) use seams::{admit_seam, arm_seams, finish_seams, refuse_two_store_actors};

const WORKER_INPUT: &str = "OMNIGRAPH_GQT_WORKER_INPUT";
const WORKER_REPORT: &str = "OMNIGRAPH_GQT_WORKER_REPORT";
const LIMIT: usize = 256 * 1024 * 1024;

tokio::task_local! {
    static OBSERVATIONS: RefCell<Observations>;
}

#[derive(Default)]
struct SessionObservations {
    values: Vec<String>,
    evidence: Vec<serde_json::Value>,
}

#[derive(Default)]
struct Observations {
    values: Vec<String>,
    sessions: BTreeMap<usize, SessionObservations>,
    bytes: usize,
    entries: usize,
    overflow: bool,
    operation: Option<serde_json::Value>,
    evidence: Vec<serde_json::Value>,
    lifecycle: Option<[std::sync::Arc<std::sync::atomic::AtomicU64>; 2]>,
}

/// Whether this task runs under the DST runner, whose replay evidence and
/// seam crossings a second, unrecorded query would perturb.
pub(crate) fn active() -> bool {
    #[cfg(tokio_unstable)]
    {
        DECORATION.try_with(|_| ()).is_ok()
    }
    #[cfg(not(tokio_unstable))]
    {
        false
    }
}

pub(crate) fn observe(value: impl FnOnce() -> String) {
    if overflowed() {
        return;
    }
    let value = value();
    crate::trace::emit(&crate::trace::Row::Observation { text: &value });
}

/// Whether this task's report stopped keeping rows at its limit; a host
/// hook skips building a value the fold would drop.
fn overflowed() -> bool {
    OBSERVATIONS
        .try_with(|events| events.borrow().overflow)
        .unwrap_or(true)
}

/// The report's sink of the trace stream: every row the worker emits reaches
/// it, and it keeps the operation, observation and evidence rows in the
/// shape the terminal report has always carried, within `LIMIT`.
pub(crate) fn fold(row: &crate::trace::Row<'_>) {
    use crate::trace::Row;
    OBSERVATIONS
        .try_with(|events| {
            let mut events = events.borrow_mut();
            match row {
                Row::Operation => {
                    events.operation = crate::trace::current_step().map(|step| step.report_value());
                }
                Row::Observation { text } => {
                    if events.overflow {
                        return;
                    }
                    let value = (*text).to_string();
                    events.bytes += value.len();
                    if events.bytes > LIMIT || events.entries >= 100_000 {
                        events.overflow = true;
                    } else {
                        events.entries += 1;
                        if let Ok(index) =
                            crate::measure::SESSION.try_with(|session| session.index)
                        {
                            events.sessions.entry(index).or_default().values.push(value);
                        } else {
                            events.values.push(value);
                        }
                    }
                }
                Row::Evidence {
                    record,
                    value,
                    session,
                } => {
                    if events.overflow {
                        return;
                    }
                    let mut event = serde_json::json!({"kind": record, "operation": events.operation, "value": value});
                    if let Some(session) = session {
                        event["session"] = serde_json::json!(session);
                    }
                    events.bytes += event.to_string().len();
                    if events.bytes > LIMIT || events.entries >= 100_000 {
                        events.overflow = true;
                    } else {
                        events.entries += 1;
                        if let Ok(index) =
                            crate::measure::SESSION.try_with(|session| session.index)
                        {
                            events
                                .sessions
                                .entry(index)
                                .or_default()
                                .evidence
                                .push(event);
                        } else {
                            events.evidence.push(event);
                        }
                    }
                }
                _ => {}
            }
        })
        .unwrap_or_default();
}

/// Append each session's ordered evidence in declaration order, independent of completion timing.
pub(crate) fn finish_concurrent_observations() {
    OBSERVATIONS
        .try_with(|events| {
            let mut events = events.borrow_mut();
            for (_, session) in std::mem::take(&mut events.sessions) {
                events.values.extend(session.values);
                events.evidence.extend(session.evidence);
            }
        })
        .unwrap_or_default();
}

pub(crate) fn lifetime_counts() -> Option<[u64; 2]> {
    OBSERVATIONS
        .try_with(|events| {
            events.borrow().lifecycle.as_ref().map(|counts| {
                counts
                    .each_ref()
                    .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            })
        })
        .ok()
        .flatten()
}

#[cfg(tokio_unstable)]
fn lifecycle_probe() -> (
    [std::sync::Arc<std::sync::atomic::AtomicU64>; 2],
    Vec<DecideGuard>,
) {
    let counts = [
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    ];
    let guards = [
        &omnigraph::seams::catalog::INIT_AFTER_COORDINATOR_INIT,
        &omnigraph::seams::catalog::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
    ]
    .into_iter()
    .zip(counts.iter())
    .map(|(seam, count)| {
        let count = count.clone();
        let name = seam.name();
        seam.observe(move || {
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::trace::crossing(name, "pass", None);
        })
    })
    .collect();
    (counts, guards)
}

/// The phase observers of a measured or traced run: one pass-through decider
/// per catalog seam, so the ledger learns which seam the engine crossed last
/// and the trace sees every crossing.
#[cfg(tokio_unstable)]
static PHASE_OBSERVERS: std::sync::Mutex<Vec<(&'static str, DecideGuard)>> =
    std::sync::Mutex::new(Vec::new());

/// Install a phase observer on every empty decision seam; a no-op when the
/// process neither measures nor records a trace.
#[cfg(tokio_unstable)]
pub(crate) fn rearm_phase_observers() {
    if crate::measure::model().is_none() && !crate::trace::active() {
        return;
    }
    let mut observers = PHASE_OBSERVERS.lock().unwrap();
    for entry in omnigraph::seams::catalog::ALL.iter() {
        let Some(seam) = entry.as_decide() else {
            continue;
        };
        let name = entry.name();
        if observers.iter().any(|(held, _)| *held == name) || seam.with(|_| ()).is_some() {
            continue;
        }
        observers.push((
            name,
            seam.observe(move || {
                crate::measure::cross(name);
                crate::trace::crossing(name, "pass", None);
            }),
        ));
    }
}

/// Give the seam `name` back to a `--- seam` directive for its step.
#[cfg(tokio_unstable)]
pub(crate) fn release_phase_observer(name: &str) {
    PHASE_OBSERVERS
        .lock()
        .unwrap()
        .retain(|(held, _)| *held != name);
}

#[cfg(tokio_unstable)]
fn clear_phase_observers() {
    PHASE_OBSERVERS.lock().unwrap().clear();
}

/// Tag every request from here on as step `ordinal`'s, a step of `kind`.
pub(crate) fn measure_step_begin(ordinal: u64, line: Option<u64>, kind: &'static str) {
    crate::measure::set_label("step", ordinal, line, kind);
}

/// Tag every request from here on as the runner's own, made after step
/// `ordinal` (its checks, the next step's setup) until the next step begins.
pub(crate) fn measure_step_end(ordinal: u64) {
    crate::measure::set_label("runner", ordinal, None, "runner");
}

/// The tick-derived counts, measurement-only: a detached commit's overlap moved
/// by one tick between two runs of one seed (2026-09-24, in the seed load and
/// in a step). The request counts are the replay-compared evidence.
const SCHEDULE_COUNTS: [&str; 3] = ["makespan", "span", "phases"];

/// At the end of the run, one `io` evidence row per label group (the counts)
/// and one measurement per group (bytes, the request log); the groups cover
/// every request either store saw, the Lance realm's and the control realm's.
pub(crate) fn measure_finish() {
    for group in crate::measure::finish() {
        let label = group.label;
        let mut counts = group.io.counts();
        counts["slot"] = serde_json::json!(label.slot);
        counts["step"] = serde_json::json!(label.step);
        counts["line"] = serde_json::json!(label.line);
        counts["kind"] = serde_json::json!(label.kind);
        let mut detail = group.io.detail();
        for key in SCHEDULE_COUNTS {
            if let Some(value) = counts.as_object_mut().and_then(|c| c.remove(key)) {
                detail[key] = value;
            }
        }
        record("io", counts);
        crate::measure::push_detail(
            serde_json::json!({"slot": label.slot, "step": label.step, "value": detail}),
        );
    }
}

/// What `--measure` runs under and compares against.
#[derive(Clone, Debug)]
pub struct MeasureOptions {
    /// One of [`crate::measure::MODELS`] by name.
    pub model: String,
    /// A baseline TSV to compare each measured case against, anywhere on
    /// disk; the repository commits none. `None` measures without a delta.
    pub baseline: Option<PathBuf>,
    /// Rewrite the measured cases' rows in the baseline instead of comparing.
    pub write_baseline: bool,
}

pub(crate) fn begin_operation(value: serde_json::Value) {
    crate::trace::operation(&value);
}

pub(crate) fn record(kind: &str, value: serde_json::Value) {
    if overflowed() {
        return;
    }
    crate::trace::emit(&crate::trace::Row::Evidence {
        record: kind,
        value: &value,
        session: crate::measure::SESSION.try_with(|s| s.label.slot).ok(),
    });
}

pub(crate) fn observe_result(result: &QueryResult, ordered: bool) {
    let schema = result.schema().fields().iter().map(|field| serde_json::json!({"name": field.name(), "type": field.data_type().to_string(), "nullable": field.is_nullable(), "metadata": field.metadata()})).collect::<Vec<_>>();
    let rows = result
        .to_rust_json()
        .map(|rows| {
            if let serde_json::Value::Array(mut rows) = rows {
                if !ordered {
                    rows.sort_by_key(crate::canonical_json);
                }
                serde_json::Value::Array(rows)
            } else {
                rows
            }
        })
        .map_err(|error| error.to_string());
    record(
        "query_result",
        serde_json::json!({"schema": schema, "rows": rows, "ordered": ordered}),
    );
    observe(|| format!("actual schema: {:?}", result.schema()));
}

pub(crate) fn observe_query<E: std::fmt::Display>(result: &Result<QueryResult, E>, ordered: bool) {
    match result {
        Ok(result) => observe_result(result, ordered),
        Err(error) => record(
            "query_error",
            serde_json::json!({"message": error.to_string()}),
        ),
    }
}

/// Record the typed shape of a step's error, the evidence a known-failure
/// marker is matched against: a `RecoveryRequired` with its reason, or a
/// `Manifest` error with its kind.
pub(crate) fn observe_fault(error: &OmniError) {
    match error {
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => record(
            "typed_error",
            serde_json::json!({"error": "RecoveryRequired", "reason": reason, "operation_id": operation_id, "message": error.to_string()}),
        ),
        OmniError::Manifest(manifest) => record(
            "typed_error",
            serde_json::json!({"error": "Manifest", "kind": format!("{:?}", manifest.kind), "reason": manifest.message, "message": error.to_string()}),
        ),
        _ => {}
    }
}

/// `file:line` of a seam's declaration or firing, as a report prints it.
fn location(at: &std::panic::Location<'_>) -> String {
    omnigraph_dst::store_places::file_line(at.file(), at.line())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    case_path: PathBuf,
    stem: String,
    #[serde(with = "shared_text")]
    text: std::sync::Arc<str>,
    case_digest: String,
    plan_digest: String,
    executable_digest: String,
    source_revision: String,
    source_digest: String,
    environment: Environment,
    seed: Option<u64>,
    effective_settings: settings::EffectiveSettings,
    #[serde(default)]
    engine: Engine,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    store: Option<String>,
    /// `--server`: the case runs against this server instead of an engine
    /// the worker opens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server: Option<ServerTarget>,
    bless: bool,
    /// `--measure`: the DST worker records every store request per step.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    measure: bool,
    /// The latency model of a `--measure` run, by name; empty when not measuring.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    model: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReport {
    code: String,
    phase: String,
    input_digest: String,
    result: Result<(), String>,
    observations: Vec<String>,
    evidence: Vec<serde_json::Value>,
    /// `--measure` bytes and request logs per step; outside the replay
    /// comparison (`comparable`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    measurements: Vec<serde_json::Value>,
}

/// The report minus its measurements: Lance stamps manifests with wall-clock
/// time, so byte counts may differ between two runs of one seed while every
/// request count stays equal.
fn comparable(report: &WorkerReport) -> Result<Vec<u8>, String> {
    if report.measurements.is_empty() {
        return json(report);
    }
    json(&WorkerReport {
        code: report.code.clone(),
        phase: report.phase.clone(),
        input_digest: report.input_digest.clone(),
        result: report.result.clone(),
        observations: report.observations.clone(),
        evidence: report.evidence.clone(),
        measurements: Vec::new(),
    })
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    environment: Environment,
    seed: Option<u64>,
    replay: usize,
    input: Input,
    outcome: Result<WorkerReport, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Planned {
    environment: Environment,
    seed: Option<u64>,
    replay: usize,
    selected: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotRun {
    execution: Option<Planned>,
    reason: NotRunReason,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum NotRunReason {
    Unselected,
    CoverageUnavailable { error: String },
    PreflightFailed { error: String },
    Suppressed { trigger: Planned, error: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Summary {
    invocation_id: String,
    replay_of: Option<String>,
    planned: Vec<Planned>,
    scope: String,
    code: String,
    case_path: Option<PathBuf>,
    source_report: Option<PathBuf>,
    case_digest: Option<String>,
    executable_digest: Option<String>,
    declared: Option<Vec<Environment>>,
    attempts: Vec<Attempt>,
    not_run: Vec<NotRun>,
    result: Result<(), String>,
}

fn new_summary(case_path: Option<PathBuf>) -> Summary {
    Summary {
        invocation_id: invocation_id(),
        replay_of: None,
        planned: vec![],
        scope: "unavailable".into(),
        code: "pending".into(),
        case_path,
        source_report: None,
        case_digest: None,
        executable_digest: None,
        declared: None,
        attempts: vec![],
        not_run: vec![],
        result: Ok(()),
    }
}

fn invocation_id() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{}-{time}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

fn refuse_ambient() -> Result<(), String> {
    let settings_variables = omnigraph_compiler::settings::DEFINITIONS
        .iter()
        .map(|spec| spec.env);
    for name in [
        "FAILPOINTS",
        "DST_ENTROPY_SEED",
        "RAYON_NUM_THREADS",
        "LANCE_CPU_THREADS",
        "LANCE_DETERMINISTIC_BACKOFF",
        crate::CASE_TIMEOUT_ENV,
    ]
    .into_iter()
    .chain(settings_variables)
    .chain(crate::RETIRED_SETTING_ENVIRONMENT)
    {
        if std::env::var_os(name).is_some() {
            return Err(format!(
                "invalid_case: ambient {name} conflicts with file-owned execution; unset it"
            ));
        }
    }
    Ok(())
}

fn error_code(error: &str) -> &'static str {
    for code in [
        "invalid_case",
        "unsupported_environment",
        "environment_changed",
        "seam_unobserved",
        "fault_cleanup_failed",
        "replay_mismatch",
        "worker_failed",
        "report_failed",
        "timeout",
    ] {
        if error.starts_with(&format!("{code}:")) {
            return code;
        }
    }
    "assertion_failed"
}

fn result_code(result: &Result<(), String>) -> &'static str {
    match result {
        Ok(()) => "passed",
        Err(error) => error_code(error),
    }
}

fn summary_code(summary: &Summary) -> &str {
    let code = result_code(&summary.result);
    if code != "assertion_failed" {
        return code;
    }
    for attempt in &summary.attempts {
        match &attempt.outcome {
            Ok(report) if report.result.is_err() => return &report.code,
            Err(error) => return error_code(error),
            _ => (),
        }
    }
    code
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn executable_digest(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| format!("environment_changed: open executable: {e}"))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file
            .read(&mut buffer)
            .map_err(|e| format!("environment_changed: hash executable: {e}"))?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    if !std::fs::metadata(path)
        .map_err(|e| format!("report_failed: inspect {}: {e}", path.display()))?
        .is_file()
    {
        return Err("report_failed: input must be a regular file".into());
    }
    let file = std::fs::File::open(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut bytes = Vec::new();
    file.take((LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    if bytes.len() > LIMIT {
        return Err(format!(
            "report_failed: {} exceeds {LIMIT} byte limit",
            path.display()
        ));
    }
    Ok(bytes)
}

mod shared_text {
    pub fn serialize<S: serde::Serializer>(
        text: &std::sync::Arc<str>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(text)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<std::sync::Arc<str>, D::Error> {
        <String as serde::Deserialize>::deserialize(deserializer).map(Into::into)
    }
}

fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > LIMIT.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other(
                    "serialized evidence exceeds the byte limit",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded(Vec::new());
    serde_json::to_writer(&mut output, value).map_err(|e| format!("report_failed: encode: {e}"))?;
    Ok(output.0)
}

/// Execute the complete fast-tier corpus case with explicit admission.
pub fn run_corpus_case(path: &Path, executable: &Path, bless: bool) -> CaseOutcome {
    run_with_selection(
        path,
        executable,
        bless,
        Selection {
            target: None,
            storage: None,
            store: None,
            server: None,
            seed: None,
            fast_tier: true,
            trace: false,
            measure: None,
            artifacts: None,
        },
    )
}

/// Select only declared environment/seed values; omission executes the complete case.
/// `measure` records every store request per step under the DST environments;
/// `artifacts` holds reports, measurement TSVs and optional diagnostic traces;
/// `None` uses the build tree's `target/gqt-artifacts/`.
/// `trace` records each selected DST attempt outside replay comparisons.
/// `server` runs the declared `omnigraph-server` environments against that
/// server and selects nothing else.
pub fn run_selected(
    path: &Path,
    executable: &Path,
    bless: bool,
    target: Option<&str>,
    storage: Option<&str>,
    seed: Option<u64>,
    measure: Option<MeasureOptions>,
    artifacts: Option<PathBuf>,
    store: Option<&str>,
    server: Option<&ServerTarget>,
    trace: bool,
) -> CaseOutcome {
    run_with_selection(
        path,
        executable,
        bless,
        Selection {
            target,
            storage,
            store,
            server,
            seed,
            fast_tier: false,
            trace,
            measure,
            artifacts,
        },
    )
}

struct Selection<'a> {
    target: Option<&'a str>,
    storage: Option<&'a str>,
    store: Option<&'a str>,
    server: Option<&'a ServerTarget>,
    seed: Option<u64>,
    fast_tier: bool,
    trace: bool,
    measure: Option<MeasureOptions>,
    artifacts: Option<PathBuf>,
}

/// The directory the report and the measure TSV are written to: the one the
/// caller named, else `target/gqt-artifacts/` of the build tree.
fn artifacts_root(custom: Option<&Path>) -> PathBuf {
    custom.map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gqt-artifacts"),
        Path::to_path_buf,
    )
}

fn run_with_selection(
    path: &Path,
    executable: &Path,
    bless: bool,
    selection: Selection<'_>,
) -> CaseOutcome {
    let started = Instant::now();
    let mut summary = new_summary(Some(path.to_path_buf()));
    summary.result = run_invocation(path, executable, bless, &selection, started, &mut summary);
    (summary.scope, summary.not_run) = coverage(&summary);
    if let Some(options) = &selection.measure {
        if let Err(error) = report_measurements(&summary, options, selection.artifacts.as_deref()) {
            summary.result = Err(match summary.result {
                Ok(()) => error,
                Err(original) => format!("{original}\n{error}"),
            });
        }
    }
    summary.code = summary_code(&summary).into();
    let result = match save_summary(&summary, selection.artifacts.as_deref()) {
        Ok(()) => summary.result,
        Err(error) => Err(format!("{error}; original result: {:?}", summary.result)),
    };
    CaseOutcome {
        stem: stem_of(path),
        elapsed: started.elapsed(),
        result,
    }
}

/// Render rows as an ASCII table; a cell that parses as a number is right-aligned.
fn ascii_table(header: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = (0..header.len())
        .map(|i| {
            rows.iter()
                .map(|r| r.get(i).map_or(0, String::len))
                .chain(std::iter::once(header[i].len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let rule = widths
        .iter()
        .map(|w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let rule = format!("+{rule}+");
    let line = |cells: &[String]| {
        let cells = cells
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if c.parse::<f64>().is_ok() {
                    format!(" {c:>w$} ", w = widths[i])
                } else {
                    format!(" {c:<w$} ", w = widths[i])
                }
            })
            .collect::<Vec<_>>()
            .join("|");
        format!("|{cells}|")
    };
    let header: Vec<String> = header.iter().map(|h| h.to_string()).collect();
    let mut out = vec![rule.clone(), line(&header), rule.clone()];
    out.extend(rows.iter().map(|r| line(r)));
    out.push(rule);
    out.join("\n")
}

fn parallelism(requests: u64, makespan: u64, span: u64) -> String {
    let ratio = |ticks: u64| requests as f64 / ticks.max(1) as f64;
    format!("{:.1} / {:.1}", ratio(makespan), ratio(span))
}

/// One label group of one first run (replay 0), as the table, the TSV and
/// the baseline read it: the compared counts merged with the schedule detail.
struct MeasureRow {
    environment: String,
    seed: String,
    slot: String,
    step: u64,
    line: String,
    kind: String,
    counts: serde_json::Value,
    detail: serde_json::Value,
}

impl MeasureRow {
    fn n(&self, key: &str) -> Option<u64> {
        self.counts[key].as_u64()
    }

    fn position(&self) -> String {
        match self.slot.as_str() {
            "setup" => "setup, before step 1".to_string(),
            "runner" => format!("runner, after step {}", self.step),
            "step" => format!("step {} (line {}, {})", self.step, self.line, self.kind),
            session => format!(
                "step {} (line {}, {session}: {})",
                self.step, self.line, self.kind
            ),
        }
    }

    /// A row whose change matters at any size: a mutation, a merge, a load,
    /// a restart. A read step's cost is secondary and reported past a threshold.
    fn primary(&self) -> bool {
        !matches!(
            self.kind.as_str(),
            "query" | "show" | "list" | "runner" | "setup"
        )
    }

    fn key(&self) -> (String, String, String, u64) {
        (
            self.environment.clone(),
            self.seed.clone(),
            self.slot.clone(),
            self.step,
        )
    }
}

fn measure_rows(summary: &Summary) -> Vec<MeasureRow> {
    let mut rows = Vec::new();
    for attempt in &summary.attempts {
        let Ok(report) = &attempt.outcome else {
            continue;
        };
        if attempt.replay != 0 {
            continue;
        }
        let seed = attempt.seed.map_or("-".to_string(), |s| s.to_string());
        for row in &report.evidence {
            if row["kind"] != "io" {
                continue;
            }
            let value = &row["value"];
            let slot = value["slot"].as_str().unwrap_or("step").to_string();
            let step = value["step"].as_u64().unwrap_or(0);
            let mut counts = value.clone();
            let mut detail = serde_json::Value::Null;
            if let Some(measured) = report
                .measurements
                .iter()
                .find(|m| m["slot"] == slot.as_str() && m["step"] == step)
            {
                detail = measured["value"].clone();
                for key in SCHEDULE_COUNTS {
                    if !detail[key].is_null() {
                        counts[key] = detail[key].clone();
                    }
                }
            }
            rows.push(MeasureRow {
                environment: attempt.environment.to_string(),
                seed: seed.clone(),
                slot,
                step,
                line: value["line"].as_u64().map_or("-".into(), |l| l.to_string()),
                kind: value["kind"].as_str().unwrap_or("").to_string(),
                counts,
                detail,
            });
        }
    }
    rows
}

/// The case's name in the baseline: its path under the corpus, whichever
/// spelling named it (`cases/v2/x.gqt` and its absolute form are one case);
/// a case outside the corpus is its absolute path.
fn baseline_case_key(path: &Path) -> String {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let root = std::fs::canonicalize(crate::corpus_root()).unwrap_or_else(|_| crate::corpus_root());
    canonical.strip_prefix(&root).map_or_else(
        |_| canonical.to_string_lossy().into_owned(),
        |rel| rel.to_string_lossy().into_owned(),
    )
}

fn baseline_case_name(summary: &Summary) -> String {
    summary
        .case_path
        .as_deref()
        .map_or_else(String::new, baseline_case_key)
}

const BASELINE_HEADER: &str =
    "case\tenvironment\tseed\tslot\tstep\tline\tkind\trequests\trepeat_reads\tafter_publish";

fn baseline_line(case: &str, row: &MeasureRow) -> String {
    format!(
        "{case}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        row.environment,
        row.seed,
        row.slot,
        row.step,
        row.line,
        row.kind,
        row.n("requests").unwrap_or(0),
        row.n("repeat_reads").unwrap_or(0),
        row.n("after_publish")
            .map_or("-".to_string(), |v| v.to_string())
    )
}

/// A committed baseline row: the key columns and the three counts.
struct BaselineRow {
    key: (String, String, String, u64),
    line: String,
    kind: String,
    requests: u64,
    repeat_reads: u64,
    after_publish: Option<u64>,
}

fn read_baseline(path: &Path, case: &str) -> Result<Vec<BaselineRow>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("baseline {}: cannot read: {e}", path.display()))?;
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate().skip(1) {
        let cells: Vec<&str> = line.split('\t').collect();
        if cells.len() != 10 {
            return Err(format!(
                "baseline {}: line {} has {} columns, the header names 10",
                path.display(),
                index + 1,
                cells.len()
            ));
        }
        if cells[0] != case {
            continue;
        }
        let count = |cell: &str, name: &str| {
            cell.parse::<u64>().map_err(|e| {
                format!(
                    "baseline {}: line {}: {name} `{cell}`: {e}",
                    path.display(),
                    index + 1
                )
            })
        };
        rows.push(BaselineRow {
            key: (
                cells[1].to_string(),
                cells[2].to_string(),
                cells[3].to_string(),
                count(cells[4], "step")?,
            ),
            line: cells[5].to_string(),
            kind: cells[6].to_string(),
            requests: count(cells[7], "requests")?,
            repeat_reads: count(cells[8], "repeat_reads")?,
            after_publish: if cells[9] == "-" {
                None
            } else {
                Some(count(cells[9], "after_publish")?)
            },
        });
    }
    Ok(rows)
}

/// The (environment, seed) scopes the run measured: the only rows of the
/// case a baseline write replaces or a delta can call gone.
fn measured_scopes(rows: &[MeasureRow]) -> std::collections::BTreeSet<(String, String)> {
    rows.iter()
        .map(|row| (row.environment.clone(), row.seed.clone()))
        .collect()
}

/// Replace the rows of the case's measured scopes with the run's and keep
/// every other row (other cases, the scopes a `--target`/`--seed` selection
/// left out); sorted by case, environment, seed, slot and step.
fn write_baseline(path: &Path, case: &str, rows: &[MeasureRow]) -> Result<(), String> {
    let scopes = measured_scopes(rows);
    let replaced = |line: &str| {
        let cells: Vec<&str> = line.split('\t').collect();
        cells.first() == Some(&case)
            && cells.len() > 2
            && scopes.contains(&(cells[1].to_string(), cells[2].to_string()))
    };
    let mut kept: Vec<String> = match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .skip(1)
            .filter(|line| !replaced(line))
            .map(str::to_string)
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(format!("baseline {}: cannot read: {error}", path.display())),
    };
    kept.extend(rows.iter().map(|row| baseline_line(case, row)));
    kept.sort_by_key(|line| {
        let cells: Vec<&str> = line.split('\t').collect();
        let cell = |i: usize| cells.get(i).copied().unwrap_or("").to_string();
        let step = cells
            .get(4)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let (rank, step) = match cells.get(3).copied() {
            Some("setup") => (0, 0),
            Some("runner") => (2, step),
            _ => (1, step),
        };
        (cell(0), cell(1), cell(2), step, rank)
    });
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("baseline {}: cannot create: {e}", path.display()))?;
    }
    let mut text = String::from(BASELINE_HEADER);
    for line in &kept {
        text.push('\n');
        text.push_str(line);
    }
    text.push('\n');
    std::fs::write(path, text)
        .map_err(|e| format!("baseline {}: cannot write: {e}", path.display()))
}

/// The change a row reports against its baseline row, or `None` when the
/// row is unchanged or, for a secondary row, changed within the relevance
/// threshold (five percent of the baseline, two requests at least).
fn delta_of(row: &MeasureRow, base: &BaselineRow) -> Option<String> {
    let requests = row.n("requests").unwrap_or(0);
    let repeat_reads = row.n("repeat_reads").unwrap_or(0);
    let after_publish = row.n("after_publish");
    let same = requests == base.requests
        && repeat_reads == base.repeat_reads
        && after_publish == base.after_publish;
    if same {
        return None;
    }
    if !row.primary() {
        let threshold = (base.requests / 20).max(2);
        let moved = requests.abs_diff(base.requests);
        if moved <= threshold && after_publish == base.after_publish {
            return None;
        }
    }
    let signed = |now: u64, then: u64| format!("{then} -> {now} ({:+})", now as i64 - then as i64);
    let window = |v: Option<u64>| v.map_or("-".to_string(), |v| v.to_string());
    Some(format!(
        "requests {}, repeat_reads {}, after_publish {} -> {}",
        signed(requests, base.requests),
        signed(repeat_reads, base.repeat_reads),
        window(base.after_publish),
        window(after_publish),
    ))
}

/// Print the run against the named baseline: changed rows past the
/// relevance filter (a primary row at any change), rows the baseline lacks,
/// rows the run lacks. Report only; never fails the case.
fn compare_baseline(path: &Path, case: &str, rows: &[MeasureRow]) -> Result<(), String> {
    if !path.exists() {
        println!(
            "GQT cost delta: no baseline at {}; write one with --write-baseline",
            path.display()
        );
        return Ok(());
    }
    let baseline = read_baseline(path, case)?;
    if baseline.is_empty() {
        println!(
            "GQT cost delta: {} holds no rows for {case}; add them with --write-baseline",
            path.display()
        );
        return Ok(());
    }
    let mut changed = Vec::new();
    let mut primary = 0;
    let mut new_rows = Vec::new();
    for row in rows {
        match baseline.iter().find(|base| base.key == row.key()) {
            Some(base) => {
                if let Some(delta) = delta_of(row, base) {
                    primary += usize::from(row.primary());
                    changed.push(format!(
                        "  {} [{} seed={}]: {delta}",
                        row.position(),
                        row.environment,
                        row.seed
                    ));
                }
            }
            None => new_rows.push(format!(
                "  {} [{} seed={}]: new",
                row.position(),
                row.environment,
                row.seed
            )),
        }
    }
    let scopes = measured_scopes(rows);
    let gone: Vec<String> = baseline
        .iter()
        .filter(|base| scopes.contains(&(base.key.0.clone(), base.key.1.clone())))
        .filter(|base| !rows.iter().any(|row| row.key() == base.key))
        .map(|base| {
            format!(
                "  {} step {} (line {}, {}) [{} seed={}]: gone",
                base.key.2, base.key.3, base.line, base.kind, base.key.0, base.key.1
            )
        })
        .collect();
    println!(
        "GQT cost delta vs {} ({case}): {} changed ({primary} primary), {} new, {} gone",
        path.display(),
        changed.len(),
        new_rows.len(),
        gone.len()
    );
    for line in changed.iter().chain(&new_rows).chain(&gone) {
        println!("{line}");
    }
    Ok(())
}

/// Print every first run (replay 0) as one ASCII table, write the long-form
/// TSV under `target/gqt-artifacts/cost/`, then write or compare the baseline.
fn report_measurements(
    summary: &Summary,
    options: &MeasureOptions,
    artifacts: Option<&Path>,
) -> Result<(), String> {
    let rows = measure_rows(summary);
    let mut tsv = vec!["environment\tseed\tslot\tstep\tline\tkind\tmetric\tvalue".to_string()];
    println!(
        "GQT measure: store requests (DST environments, first run of each seed), model {}",
        options.model
    );
    println!(
        "step = one executing section of the case (a mutate, query, control or restart), numbered in file order; setup = the store, schema and seed before step 1; runner, after N = from step N's end to the next step's start (the runner's own checks)"
    );
    println!(
        "requests = the work; repeats = reads of an object and range already read in the step; makespan = ticks the schedule took (one tick = 1 ms of virtual time); span = its critical path with the tables of each phase side by side; waiting = makespan - span; parallelism = requests per tick, achieved (makespan) / available (span); sim ms = virtual time under the model; u$ = microdollars at S3 list prices"
    );
    let cells = |requests: u64, makespan: Option<u64>, span: Option<u64>, tables: Option<u64>| {
        let waiting = match (makespan, span) {
            (Some(makespan), Some(span)) => {
                let waiting = makespan.saturating_sub(span);
                format!("{waiting} ({}%)", waiting * 100 / makespan.max(1))
            }
            _ => String::new(),
        };
        let parallelism = match (makespan, span) {
            (Some(makespan), Some(span)) => parallelism(requests, makespan, span),
            _ => String::new(),
        };
        vec![
            requests.to_string(),
            makespan.map_or(String::new(), |v| v.to_string()),
            span.map_or(String::new(), |v| v.to_string()),
            waiting,
            tables.map_or(String::new(), |v| v.to_string()),
            parallelism,
        ]
    };
    let mut current: Option<(String, String)> = None;
    let mut table_rows: Vec<Vec<String>> = Vec::new();
    let flush = |current: &Option<(String, String)>, table_rows: &mut Vec<Vec<String>>| {
        if let Some((environment, seed)) = current
            && !table_rows.is_empty()
        {
            println!("\n{environment} seed={seed}");
            println!(
                "{}",
                ascii_table(
                    &[
                        "step",
                        "phase",
                        "requests",
                        "repeats",
                        "makespan",
                        "span",
                        "waiting",
                        "tables",
                        "parallelism",
                        "after the CAS",
                        "sim ms",
                        "u$",
                    ],
                    table_rows
                )
            );
        }
        table_rows.clear();
    };
    for row in &rows {
        let group = (row.environment.clone(), row.seed.clone());
        if current.as_ref() != Some(&group) {
            flush(&current, &mut table_rows);
            current = Some(group);
        }
        let mut step_row = vec![row.position(), "all".to_string()];
        let (requests, makespan, span) = (
            row.n("requests").unwrap_or(0),
            row.n("makespan"),
            row.n("span"),
        );
        let mut all = cells(requests, makespan, span, None);
        all.insert(1, row.n("repeat_reads").unwrap_or(0).to_string());
        step_row.extend(all);
        step_row.push(
            row.n("after_publish")
                .map_or(String::new(), |v| v.to_string()),
        );
        step_row.push(
            row.detail["simulated_us"]
                .as_u64()
                .map_or(String::new(), |us| format!("{:.1}", us as f64 / 1_000.0)),
        );
        step_row.push(
            row.detail["usd_micro"]
                .as_f64()
                .map_or(String::new(), |usd| format!("{usd:.1}")),
        );
        table_rows.push(step_row);
        let mut metrics = vec![("requests".to_string(), row.counts["requests"].clone())];
        for key in ["repeat_reads", "makespan", "span", "after_publish"] {
            if !row.counts[key].is_null() {
                metrics.push((key.to_string(), row.counts[key].clone()));
            }
        }
        for key in ["simulated_us", "usd_micro", "bytes_read", "bytes_written"] {
            if !row.detail[key].is_null() {
                metrics.push((key.to_string(), row.detail[key].clone()));
            }
        }
        if let Some(classes) = row.counts["by_class"].as_object() {
            metrics.extend(classes.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        for io in row.counts["phases"].as_array().into_iter().flatten() {
            let phase = io["phase"].as_str().unwrap_or("?");
            for field in ["makespan", "span", "requests", "tables"] {
                metrics.push((format!("phase.{phase}.{field}"), io[field].clone()));
            }
            let p = |field: &str| io[field].as_u64().unwrap_or(0);
            let mut phase_row = vec![String::new(), phase.to_string()];
            let mut phase_cells = cells(
                p("requests"),
                Some(p("makespan")),
                Some(p("span")),
                Some(p("tables")),
            );
            phase_cells.insert(1, String::new());
            phase_row.extend(phase_cells);
            phase_row.extend([String::new(), String::new(), String::new()]);
            table_rows.push(phase_row);
        }
        for (metric, v) in &metrics {
            tsv.push(format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{metric}\t{v}",
                row.environment, row.seed, row.slot, row.step, row.line, row.kind
            ));
        }
    }
    flush(&current, &mut table_rows);
    let root = artifacts_root(artifacts).join("cost");
    let path = root.join(format!("{}.tsv", summary.invocation_id));
    match std::fs::create_dir_all(&root).and_then(|()| std::fs::write(&path, tsv.join("\n") + "\n"))
    {
        Ok(()) => println!("GQT measure TSV: {}", path.display()),
        Err(error) => println!("GQT measure TSV not written: {error}"),
    }
    if rows.is_empty() {
        return Ok(());
    }
    let Some(baseline) = &options.baseline else {
        return Ok(());
    };
    let case = baseline_case_name(summary);
    if options.write_baseline {
        if let Err(error) = &summary.result {
            return Err(format!(
                "GQT cost baseline: {} not written for {case}: the run failed ({})",
                baseline.display(),
                error.lines().next().unwrap_or("")
            ));
        }
        write_baseline(baseline, &case, &rows)?;
        println!(
            "GQT cost baseline: {} rows for {case} written to {}",
            rows.len(),
            baseline.display()
        );
        Ok(())
    } else {
        compare_baseline(baseline, &case, &rows)
    }
}

/// The worker input as the report keeps it: the bearer token travels to the
/// worker and no further, so a shared report never carries a credential.
fn persisted(mut input: Input) -> Input {
    if let Some(server) = &mut input.server {
        server.token = None;
    }
    input
}

/// An attempt whose state lives outside the report: an external `--store`
/// or a `--server` graph, neither frozen by the report, so neither replays.
fn external_attempt(attempt: &Attempt) -> bool {
    attempt.input.store.is_some() || attempt.input.server.is_some()
}

fn save_summary(summary: &Summary, artifacts: Option<&Path>) -> Result<(), String> {
    let root = artifacts_root(artifacts);
    std::fs::create_dir_all(&root).map_err(|e| {
        format!(
            "report_failed: create artifact directory: {e}; original: {:?}",
            summary.result
        )
    })?;
    let mut file = tempfile::Builder::new()
        .prefix("invocation-")
        .suffix(".json")
        .tempfile_in(&root)
        .map_err(|e| format!("report_failed: create summary: {e}"))?;
    use std::io::Write;
    file.write_all(&json(summary)?)
        .map_err(|e| format!("report_failed: write summary: {e}"))?;
    let (_, path) = file
        .keep()
        .map_err(|e| format!("report_failed: retain summary: {e}"))?;
    println!("GQT report: {}", path.display());
    if summary.attempts.iter().any(external_attempt) {
        println!("GQT replay unavailable: external store or server contents are not frozen");
    } else {
        println!("GQT replay: omnigraph-gqt --replay '{}'", path.display());
    }
    Ok(())
}

fn coverage(summary: &Summary) -> (String, Vec<NotRun>) {
    let terminal_error = || {
        summary
            .result
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(|| "report_failed: execution has no terminal result".into())
    };
    if summary.planned.is_empty() {
        return (
            "unavailable".into(),
            vec![NotRun {
                execution: None,
                reason: NotRunReason::CoverageUnavailable {
                    error: terminal_error(),
                },
            }],
        );
    }
    let scope = if summary.planned.iter().any(|p| !p.selected) {
        "partial"
    } else {
        "full"
    };
    let not_run = summary
        .planned
        .iter()
        .filter_map(|planned| {
            let reason = if !planned.selected {
                NotRunReason::Unselected
            } else if summary.attempts.iter().any(|attempt| {
                attempt.environment == planned.environment
                    && attempt.seed == planned.seed
                    && attempt.replay == planned.replay
            }) {
                return None;
            } else if let Some((attempt, error)) = summary.attempts.iter().find_map(|attempt| {
                attempt
                    .outcome
                    .as_ref()
                    .err()
                    .filter(|error| {
                        attempt.environment == planned.environment
                            || error.starts_with("fault_cleanup_failed:")
                            || error.starts_with("timeout:")
                            || error
                                .starts_with("report_failed: invocation evidence budget exhausted")
                    })
                    .map(|error| (attempt, error))
            }) {
                NotRunReason::Suppressed {
                    trigger: Planned {
                        environment: attempt.environment.clone(),
                        seed: attempt.seed,
                        replay: attempt.replay,
                        selected: true,
                    },
                    error: error.clone(),
                }
            } else {
                NotRunReason::PreflightFailed {
                    error: terminal_error(),
                }
            };
            Some(NotRun {
                execution: Some(planned.clone()),
                reason,
            })
        })
        .collect();
    (scope.into(), not_run)
}

/// Retain a terminal report for command-line refusals before execution begins.
pub fn report_cli_refusal(
    case_path: Option<PathBuf>,
    source_report: Option<PathBuf>,
    error: String,
) -> String {
    let mut summary = new_summary(case_path);
    summary.source_report = source_report;
    summary.result = Err(error.clone());
    summary.code = summary_code(&summary).into();
    (summary.scope, summary.not_run) = coverage(&summary);
    match save_summary(&summary, None) {
        Ok(()) => error,
        Err(report_error) => format!("{report_error}; original result: {error}"),
    }
}

fn run_invocation(
    path: &Path,
    executable: &Path,
    bless: bool,
    selection: &Selection<'_>,
    started: Instant,
    summary: &mut Summary,
) -> Result<(), String> {
    refuse_ambient()?;
    let engine = crate::engine_from_env()?;
    let selected = selection.target;
    let selected_storage = selection.storage;
    let selected_seed = selection.seed;
    let text =
        String::from_utf8(read_bounded(path)?).map_err(|e| format!("invalid_case: UTF-8: {e}"))?;
    let text: std::sync::Arc<str> = text.into();
    summary.case_digest = Some(digest(text.as_bytes()));
    let case = parse_case(&stem_of(path), &text).map_err(|error| {
        if error.starts_with("invalid_case:") {
            error
        } else {
            format!("invalid_case: {error}")
        }
    })?;
    summary.declared = Some(case.runner.environments.clone());
    for env in &case.runner.environments {
        if matches!(env.execution, Execution::ServerDst { .. }) {
            env.admit(case.needs_dst())?;
        }
    }
    let served = selection.server.is_some();
    if served {
        if selection.store.is_some() {
            return Err("invalid_case: --server and --store are mutually exclusive".into());
        }
        if bless {
            return Err(
                "invalid_case: bless requires direct engine execution; a served run cannot rewrite the case".into(),
            );
        }
        if selected.is_some_and(|target| target != "omnigraph-server") {
            return Err(
                "invalid_case: --server runs only omnigraph-server environments; --target selects another".into(),
            );
        }
    }
    let selects =
        |env: &Environment| env.matches(selected, selected_storage) && env.is_served() == served;
    for env in &case.runner.environments {
        for seed in env.seeds() {
            for replay in 0..if seed.is_some() { 2 } else { 1 } {
                summary.planned.push(Planned {
                    environment: env.clone(),
                    seed,
                    replay,
                    selected: selects(env) && selected_seed.is_none_or(|s| seed == Some(s)),
                });
            }
        }
    }
    summary.scope = if summary.planned.iter().any(|p| !p.selected) {
        "partial"
    } else {
        "full"
    }
    .into();
    let plan_digest = digest(&json(&summary.planned)?);
    if selection.fast_tier && case.runner.timeout_ms > 10_000 {
        return Err("invalid_case: the required corpus admits timeout_ms at most 10000; use a standalone heavy reproduction".into());
    }
    if selected_seed.is_some() && selected.is_none() && selected_storage.is_none() {
        return Err("invalid_case: --seed requires --target or --storage".into());
    }
    let selected_envs = case
        .runner
        .environments
        .iter()
        .filter(|env| {
            selects(env) && selected_seed.is_none_or(|seed| env.seeds().contains(&Some(seed)))
        })
        .collect::<Vec<_>>();
    if selected_envs.is_empty() {
        return Err(if served {
            "invalid_case: --server requires a declared omnigraph-server environment".into()
        } else if selected == Some("omnigraph-server")
            && case.runner.environments.iter().any(Environment::is_served)
        {
            "invalid_case: --target omnigraph-server requires --server <URL> --graph <ID>".into()
        } else {
            "invalid_case: environment selector matches no declared environment".into()
        });
    }
    if served {
        admit_served(&case)?;
        if selected_envs.len() > 1 {
            return Err(format!(
                "invalid_case: --server selects {} omnigraph-server environments against one graph, whose state no run resets; pass --storage to select one",
                selected_envs.len()
            ));
        }
    } else {
        case.admit_store(selection.store)?;
    }
    if selection.measure.is_some()
        && !selected_envs.iter().any(|env| {
            matches!(
                env.execution,
                Execution::Dst { .. } | Execution::ServerDst { .. }
            )
        })
    {
        return Err("invalid_case: --measure requires a selected DST environment".into());
    }
    if selection.trace
        && !selected_envs.iter().any(|env| {
            matches!(
                env.execution,
                Execution::Dst { .. } | Execution::ServerDst { .. }
            )
        })
    {
        return Err("invalid_case: --trace requires a selected DST environment".into());
    }
    for env in &selected_envs {
        if served {
            env.admit_served()?;
        } else {
            env.admit_store(case.needs_dst(), selection.store)?;
        }
    }
    for (ordinal, seams) in &case.seams {
        let step = case
            .items
            .iter()
            .flat_map(|item| match item {
                crate::Item::Step(step) => std::slice::from_ref(step),
                crate::Item::Loop { steps, .. } => steps.as_slice(),
            })
            .find(|step| step.ordinal() == *ordinal);
        let admitted = seams
            .iter()
            .map(|seam| admit_seam(seam, step))
            .collect::<Result<Vec<_>, _>>()?;
        refuse_two_store_actors(seams, &admitted)?;
    }
    let in_process_declared = case
        .runner
        .environments
        .iter()
        .filter(|env| !env.is_served())
        .count();
    if bless
        && (in_process_declared != 1
            || !matches!(selected_envs[0].execution, Execution::Engine { .. }))
    {
        return Err("invalid_case: bless requires exactly one direct engine environment".into());
    }
    let build = executable_digest(executable)?;
    summary.executable_digest = Some(build.clone());
    let budget = Duration::from_millis(case.runner.timeout_ms);
    let remaining = || {
        budget
            .checked_sub(started.elapsed())
            .filter(|left| !left.is_zero())
            .ok_or_else(|| format!("timeout: case exceeded wall-time budget {budget:?}"))
    };
    let mut failures = Vec::new();
    let mut isolation_lost = false;
    let mut deadline_expired = false;
    let mut retained_bytes = 0usize;
    let mut evidence_exhausted = false;
    for env in selected_envs {
        let mut worker_failed = false;
        for seed in env.seeds() {
            if selected_seed.is_some() && seed != selected_seed {
                continue;
            }
            let repetitions = if seed.is_some() { 2 } else { 1 };
            let mut reports = Vec::new();
            for replay in 0..repetitions {
                if worker_failed || isolation_lost || evidence_exhausted || deadline_expired {
                    continue;
                }
                if let Err(error) = remaining() {
                    failures.push(error);
                    return Err(failures.join("\n"));
                }
                let input = Input {
                    case_path: path.to_path_buf(),
                    stem: stem_of(path),
                    text: text.clone(),
                    case_digest: digest(text.as_bytes()),
                    plan_digest: plan_digest.clone(),
                    executable_digest: build.clone(),
                    source_revision: env!("GQT_SOURCE_REVISION").into(),
                    source_digest: env!("GQT_SOURCE_DIGEST").into(),
                    environment: env.clone(),
                    seed,
                    effective_settings: settings::EffectiveSettings::for_seed(seed),
                    engine,
                    store: selection.store.map(str::to_owned),
                    server: selection.server.cloned(),
                    bless,
                    measure: selection.measure.is_some(),
                    model: selection
                        .measure
                        .as_ref()
                        .map_or_else(String::new, |options| options.model.clone()),
                };
                let trace = match seed {
                    Some(seed) if selection.trace => {
                        let created = serde_json::to_value(env)
                            .map_err(|e| format!("report_failed: encode environment: {e}"))
                            .and_then(|environment| {
                                crate::trace::create(
                                    &artifacts_root(selection.artifacts.as_deref()),
                                    &crate::trace::Start {
                                        format: crate::trace::FORMAT,
                                        version: crate::trace::VERSION,
                                        invocation_id: &summary.invocation_id,
                                        case_path: &input.case_path,
                                        case_digest: &input.case_digest,
                                        source_digest: &input.source_digest,
                                        executable_digest: &input.executable_digest,
                                        environment,
                                        seed,
                                        replay,
                                    },
                                )
                            });
                        match created {
                            Ok(path) => Some(path),
                            Err(error) => {
                                failures.push(error);
                                return Err(failures.join("\n"));
                            }
                        }
                    }
                    _ => None,
                };
                let mut outcome = remaining()
                    .and_then(|left| run_child(&input, executable, left, trace.as_deref()));
                if let Ok(report) = &outcome {
                    let size = json(report)?.len();
                    retained_bytes = retained_bytes.saturating_add(size);
                    if retained_bytes > LIMIT / 2 {
                        evidence_exhausted = true;
                        outcome = Err("report_failed: invocation evidence budget exhausted; remaining attempts not run".into());
                    }
                }
                match &outcome {
                    Ok(report) => {
                        if let Err(error) = &report.result {
                            failures.push(format!(
                                "{error}; environment={} seed={seed:?} replay={replay}",
                                env
                            ));
                        }
                        reports.push(comparable(report)?);
                    }
                    Err(error) => {
                        failures.push(error.clone());
                        isolation_lost |= error.starts_with("fault_cleanup_failed:");
                        deadline_expired |= error.starts_with("timeout:");
                        worker_failed = true;
                    }
                }
                summary.attempts.push(Attempt {
                    environment: env.clone(),
                    seed,
                    replay,
                    input: persisted(input),
                    outcome,
                    trace,
                });
            }
            if reports.len() == 2 {
                if reports[0] != reports[1] {
                    failures.push(format!(
                        "replay_mismatch: {} seed={seed:?}; both reports retained",
                        env
                    ));
                } else {
                    println!("DST environment={} seed={seed:?} replay matched", env);
                }
            }
        }
    }
    if let Err(error) = remaining() {
        failures.push(error);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn store_environment_variable(store: &str, key: &str) -> bool {
    if store.starts_with("s3://") {
        return key.starts_with("AWS_");
    }
    store.starts_with("az://")
        && (key.starts_with("AZURE_")
            || matches!(
                key,
                "AZURITE_BLOB_STORAGE_URL"
                    | "IDENTITY_ENDPOINT"
                    | "IDENTITY_HEADER"
                    | "MSI_ENDPOINT"
                    | "AWS_ALLOW_HTTP"
                    | "OBJECT_STORE_CLIENT_MAX_RETRIES"
                    | "OBJECT_STORE_CLIENT_RETRY_TIMEOUT"
            ))
}

fn run_child(
    input: &Input,
    executable: &Path,
    budget: Duration,
    trace: Option<&Path>,
) -> Result<WorkerReport, String> {
    input.effective_settings.verify_expected(input.seed)?;
    let started = Instant::now();
    let dir =
        tempfile::tempdir().map_err(|e| format!("worker_failed: create output directory: {e}"))?;
    let input_path = dir.path().join("input.json");
    let report_path = dir.path().join("report.json");
    let bytes = json(input)?;
    std::fs::write(&input_path, &bytes).map_err(|e| format!("report_failed: write input: {e}"))?;
    let mut cmd = Command::new(executable);
    cmd.arg(&input.case_path)
        .env_clear()
        .env(WORKER_INPUT, &input_path)
        .env("TMPDIR", dir.path())
        .env(WORKER_REPORT, &report_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(path) = trace {
        cmd.env(crate::trace::WORKER_PATH, path);
    }
    input.effective_settings.configure(&mut cmd);
    if let Some(store) = &input.store {
        cmd.envs(std::env::vars_os().filter(|(key, _)| {
            key.to_str()
                .is_some_and(|key| store_environment_variable(store, key))
        }));
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("worker_failed: spawn: {e}"))?;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Err(format!(
                        "worker_failed: {} seed={:?} exited {status}",
                        input.environment, input.seed
                    ));
                }
                break;
            }
            Ok(None) if started.elapsed() < budget => std::thread::sleep(
                budget
                    .saturating_sub(started.elapsed())
                    .min(Duration::from_millis(10)),
            ),
            other => {
                let containment =
                    contain_child(&mut child, Instant::now() + Duration::from_millis(250));
                if let Err(error) = containment {
                    let retained = dir.keep();
                    return Err(format!(
                        "fault_cleanup_failed: {error}; original poll={other:?}; wall-time budget={budget:?}; retained context={retained:?}"
                    ));
                }
                return Err(format!(
                    "timeout: worker exceeded wall-time budget {budget:?}; original poll={other:?}; contained=true"
                ));
            }
        }
    }
    let report: WorkerReport = serde_json::from_slice(&read_bounded(&report_path)?)
        .map_err(|e| format!("report_failed: decode worker report: {e}"))?;
    if report.input_digest != digest(&bytes) {
        return Err("environment_changed: worker input digest differs".into());
    }
    Ok(report)
}

fn contain_child(child: &mut std::process::Child, deadline: Instant) -> Result<(), String> {
    let killed = child.kill();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(1)),
            result => {
                return Err(format!(
                    "worker pid={} containment unavailable; kill={killed:?}; reap={result:?}",
                    child.id()
                ));
            }
        }
    }
}

/// Execute only the internal request, never rereading the original case file.
pub fn run_worker_if_requested(_path: &Path) -> Result<bool, String> {
    match (
        std::env::var_os(WORKER_INPUT),
        std::env::var_os(WORKER_REPORT),
    ) {
        (None, None) => Ok(false),
        (Some(input), Some(output)) => {
            let bytes = read_bounded(Path::new(&input))?;
            let input: Input = serde_json::from_slice(&bytes)
                .map_err(|e| format!("invalid_case: worker input: {e}"))?;
            let input_digest = digest(&bytes);
            let recording = std::env::var_os(crate::trace::WORKER_PATH)
                .map(|path| crate::trace::Recording::start(Path::new(&path)))
                .transpose();
            let result = match &recording {
                Ok(_) => worker_report(&input, input_digest.clone()),
                Err(error) => Err(error.clone()),
            };
            let mut report = match result {
                Ok(report) => report,
                Err(error) => WorkerReport {
                    code: error_code(&error).into(),
                    phase: "preflight".into(),
                    input_digest,
                    result: Err(error),
                    observations: vec![],
                    evidence: vec![],
                    measurements: vec![],
                },
            };
            if let Ok(Some(recording)) = recording {
                if let Err(error) = recording.finish(&report.code, &report.phase) {
                    report.result = Err(match report.result {
                        Ok(()) => error,
                        Err(original) => format!("{original}\n{error}"),
                    });
                    report.code = "report_failed".into();
                    report.phase = "teardown".into();
                }
            }
            std::fs::write(output, json(&report)?)
                .map_err(|e| format!("report_failed: write worker report: {e}"))?;
            Ok(true)
        }
        _ => Err("invalid_case: incomplete internal worker request".into()),
    }
}

fn worker_report(input: &Input, input_digest: String) -> Result<WorkerReport, String> {
    input.effective_settings.verify_expected(input.seed)?;
    input.effective_settings.verify_process()?;
    if input.source_revision != env!("GQT_SOURCE_REVISION")
        || input.source_digest != env!("GQT_SOURCE_DIGEST")
    {
        return Err("environment_changed: worker source identity differs".into());
    }
    if digest(input.text.as_bytes()) != input.case_digest {
        return Err("environment_changed: worker case digest differs".into());
    }
    let executable = std::env::current_exe()
        .map_err(|e| format!("environment_changed: locate executable: {e}"))?;
    if executable_digest(&executable)? != input.executable_digest {
        return Err("environment_changed: worker executable digest differs".into());
    }
    let case = parse_case(&input.stem, &input.text)?;
    if !case.runner.environments.contains(&input.environment)
        || !input.environment.seeds().contains(&input.seed)
    {
        return Err("environment_changed: worker selection is not declared".into());
    }
    if let Some(server) = &input.server {
        if input.store.is_some() || input.seed.is_some() || input.bless {
            return Err(
                "environment_changed: a served worker takes no store, seed or bless".into(),
            );
        }
        input.environment.admit_served()?;
        admit_served(&case)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("worker_failed: runtime: {e}"))?;
        return runtime.block_on(capture(
            input_digest,
            crate::execute_case_on_server(&case, server),
        ));
    }
    case.admit_store(input.store.as_deref())?;
    input
        .environment
        .admit_store(case.needs_dst(), input.store.as_deref())?;
    match input.seed {
        None => {
            let settings::TokioRuntime::MultiThread {
                worker_threads,
                thread_stack_bytes,
            } = input.effective_settings.tokio
            else {
                return Err(
                    "environment_changed: direct engine requires its multi-thread runtime settings"
                        .into(),
                );
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .thread_stack_size(thread_stack_bytes)
                .build()
                .map_err(|e| format!("worker_failed: runtime: {e}"))?;
            runtime.block_on(capture(
                input_digest,
                crate::execute_case_on_engine(
                    &case,
                    &input.case_path,
                    input.bless,
                    input.engine,
                    input.store.as_deref(),
                ),
            ))
        }
        Some(seed) => dst_report(input, &case, seed, input_digest),
    }
}

/// One capture at a time per process: the lifecycle probe installs two
/// process-wide seams, and two in-process tests would collide on them.
static CAPTURE_ONE_AT_A_TIME: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn capture(
    input_digest: String,
    future: impl std::future::Future<Output = Result<(), String>>,
) -> Result<WorkerReport, String> {
    use futures::FutureExt;
    let _one_at_a_time = CAPTURE_ONE_AT_A_TIME.lock().await;
    let mut initial = Observations::default();
    #[cfg(tokio_unstable)]
    let _guards = {
        let (counts, guards) = lifecycle_probe();
        initial.lifecycle = Some(counts);
        guards
    };
    #[cfg(not(tokio_unstable))]
    {
        initial.lifecycle = None;
    }
    #[cfg(tokio_unstable)]
    rearm_phase_observers();
    OBSERVATIONS
        .scope(RefCell::new(initial), async {
            let result = std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    Err(format!(
                        "worker_failed: case panicked: {}",
                        crate::panic_message(panic.as_ref())
                    ))
                });
            #[cfg(tokio_unstable)]
            clear_phase_observers();
            finish_concurrent_observations();
            measure_finish();
            let observations = OBSERVATIONS.with(|events| events.take());
            let result = if observations.overflow {
                Err(format!(
                    "report_failed: observation limit exceeded; original={result:?}"
                ))
            } else {
                result
            };
            Ok(WorkerReport {
                code: result_code(&result).into(),
                phase: if observations.operation.is_some() {
                    "execution"
                } else {
                    "setup"
                }
                .into(),
                input_digest,
                result,
                observations: observations.values,
                evidence: observations.evidence,
                measurements: crate::measure::take_details(),
            })
        })
        .await
}

#[cfg(tokio_unstable)]
#[derive(Debug)]
struct GqtScenario<'a> {
    input: &'a Input,
    case: &'a crate::Case,
    input_digest: String,
}

#[cfg(tokio_unstable)]
impl omnigraph_dst::UniverseScenario<omnigraph_dst::memory::MemoryStorage> for GqtScenario<'_> {
    type Output = Result<WorkerReport, String>;

    /// The engine talks to the store through the `STORAGE` seam on every run,
    /// faulted or not: the decoration is a property of the target, and a
    /// store action only hands it one rule for one step.
    async fn run(
        &self,
        resources: &mut omnigraph_dst::memory::MemoryStorage,
        _workload_seed: u64,
    ) -> Self::Output {
        use omnigraph::storage::StorageAdapter;
        let measure = self.input.measure.then(|| {
            let model = crate::measure::Model::named(&self.input.model)
                .unwrap_or_else(|| crate::measure::Model::named("unit").expect("the unit model"));
            crate::measure::prepare(model)
        });
        crate::concurrent::install(measure);
        let base: std::sync::Arc<dyn StorageAdapter> =
            crate::measure::wrap_adapter(resources.adapter.clone(), crate::concurrent::touch);
        let decoration =
            omnigraph_dst::harness::FailingStorage::quiet(base, resources.root.clone());
        omnigraph::storage::STORAGE.clear();
        let _installed = omnigraph::storage::STORAGE.install(std::sync::Arc::new(
            omnigraph_dst::harness::FailingStorageDecorator(decoration.clone()),
        ));
        let storage: std::sync::Arc<dyn StorageAdapter> = decoration.clone();
        DECORATION
            .scope(
                decoration,
                capture(
                    self.input_digest.clone(),
                    crate::execute_case_with_storage(
                        self.case,
                        &self.input.case_path,
                        &resources.root,
                        storage,
                        self.input.engine,
                    ),
                ),
            )
            .await
    }
}

#[cfg(tokio_unstable)]
fn dst_report(
    input: &Input,
    case: &crate::Case,
    seed: u64,
    input_digest: String,
) -> Result<WorkerReport, String> {
    for (key, expected) in omnigraph_dst::env_knobs::QUIESCE_ENV {
        let actual = std::env::var(key).ok();
        if actual.as_deref() != Some(expected) {
            return Err(format!(
                "DST requires {key}={expected} at process start, got {actual:?}"
            ));
        }
    }
    let _seams = omnigraph::seams::FailScenario::setup();
    let environment = omnigraph_dst::memory::MemoryEnvironment::new(
        "shared-memory://gqt-dst/case",
        seed,
        omnigraph_dst::UniverseProcess::Isolated,
    );
    let scenario = GqtScenario {
        input,
        case,
        input_digest: input_digest.clone(),
    };
    let run = omnigraph_dst::run_universe(&environment, &scenario);
    let result = run
        .result
        .map_err(|panic| {
            format!(
                "DST universe panicked: {}",
                crate::panic_message(panic.as_ref())
            )
        })
        .and_then(|result| result)
        .and_then(|result| result);
    let mut report = match result {
        Ok(report) => report,
        Err(error) => WorkerReport {
            code: "worker_failed".into(),
            phase: match run.phase {
                omnigraph_dst::UniversePhase::Runtime => "runtime",
                omnigraph_dst::UniversePhase::Setup => "setup",
                omnigraph_dst::UniversePhase::Scenario => "execution",
            }
            .into(),
            input_digest,
            result: Err(format!("worker_failed: {error}")),
            observations: Vec::new(),
            evidence: Vec::new(),
            measurements: Vec::new(),
        },
    };
    let cleanup = run
        .cleanup
        .map_err(|panic| format!("cleanup panicked: {}", crate::panic_message(panic.as_ref())))
        .and_then(|result| result);
    if let Err(error) = cleanup {
        report.result = Err(format!(
            "fault_cleanup_failed: {error}; original={:?}",
            report.result
        ));
        report.code = "fault_cleanup_failed".into();
        report.phase = "teardown".into();
    }
    Ok(report)
}

#[cfg(not(tokio_unstable))]
fn dst_report(
    _input: &Input,
    _case: &crate::Case,
    _seed: u64,
    _digest: String,
) -> Result<WorkerReport, String> {
    Err("unsupported_environment: DST runner is unavailable".into())
}

/// Replay retained inputs against the identical executable, preserving failed assertions.
pub fn replay_report(path: &Path, executable: &Path) -> Result<(), String> {
    let decoded = read_bounded(path).and_then(|bytes| {
        serde_json::from_slice::<Summary>(&bytes)
            .map_err(|e| format!("report_failed: decode replay: {e}"))
    });
    let prior = match decoded {
        Ok(summary) => summary,
        Err(error) => {
            let mut summary = new_summary(None);
            summary.source_report = Some(path.to_path_buf());
            summary.code = "report_failed".into();
            summary.result = Err(error);
            (summary.scope, summary.not_run) = coverage(&summary);
            save_summary(&summary, None)?;
            return summary.result;
        }
    };
    let mut summary = new_summary(prior.case_path.clone());
    summary.source_report = Some(path.to_path_buf());
    summary.replay_of = Some(prior.invocation_id.clone());
    summary.planned = prior.planned.clone();
    summary.case_digest = prior.case_digest.clone();
    summary.executable_digest = prior.executable_digest.clone();
    summary.declared = prior.declared.clone();
    summary.result = replay_attempts(&prior, executable, &mut summary.attempts);
    (summary.scope, summary.not_run) = coverage(&summary);
    summary.code = summary_code(&summary).into();
    match save_summary(&summary, None) {
        Ok(()) => summary.result,
        Err(error) => Err(format!("{error}; original result: {:?}", summary.result)),
    }
}

fn replay_attempts(
    summary: &Summary,
    executable: &Path,
    executed: &mut Vec<Attempt>,
) -> Result<(), String> {
    refuse_ambient()?;
    crate::engine_from_env()?;
    if summary.attempts.iter().any(external_attempt) {
        return Err("invalid_case: external-store invocations cannot replay, nor served ones; the report does not freeze store or server contents".into());
    }
    for attempt in &summary.attempts {
        attempt
            .input
            .effective_settings
            .verify_expected(attempt.input.seed)?;
    }
    if summary.code != summary_code(summary) {
        return Err("report_failed: inconsistent invocation status".into());
    }
    if summary.attempts.is_empty() {
        return Err("report_failed: no replayable executions".into());
    }
    let selected = summary
        .planned
        .iter()
        .filter(|p| p.selected)
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let actual = summary
        .attempts
        .iter()
        .map(|a| Planned {
            environment: a.environment.clone(),
            seed: a.seed,
            replay: a.replay,
            selected: true,
        })
        .collect::<std::collections::BTreeSet<_>>();
    if selected != actual
        || actual.len() != summary.attempts.len()
        || summary
            .planned
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != summary.planned.len()
    {
        return Err("report_failed: incomplete or duplicate execution inventory".into());
    }
    let (scope, not_run) = coverage(summary);
    if summary.scope != scope || summary.not_run != not_run {
        return Err("report_failed: inconsistent execution coverage".into());
    }
    let plan_digest = digest(&json(&summary.planned)?);
    let started = Instant::now();
    let build = executable_digest(executable)?;
    let mut failures = Vec::new();
    let mut budget = None;
    for attempt in &summary.attempts {
        if attempt.input.plan_digest != plan_digest {
            return Err("environment_changed: execution plan digest differs".into());
        }
        if Some(&attempt.input.executable_digest) != summary.executable_digest.as_ref()
            || Some(&attempt.input.case_digest) != summary.case_digest.as_ref()
            || Some(&attempt.input.case_path) != summary.case_path.as_ref()
            || attempt.environment != attempt.input.environment
            || attempt.seed != attempt.input.seed
            || attempt.replay > usize::from(attempt.seed.is_some())
        {
            return Err("report_failed: inconsistent execution attribution".into());
        }
        if attempt.input.executable_digest != build {
            return Err("environment_changed: replay executable differs".into());
        }
        if attempt.input.bless {
            return Err("invalid_case: blessing invocations cannot replay".into());
        }
        let prior = attempt
            .outcome
            .as_ref()
            .map_err(|e| format!("report_failed: incomplete prior execution: {e}"))?;
        let case = parse_case(&attempt.input.stem, &attempt.input.text)?;
        if summary.declared.as_ref() != Some(&case.runner.environments) {
            return Err("report_failed: declared environments differ from frozen input".into());
        }
        if prior.input_digest != digest(&json(&attempt.input)?) {
            return Err("environment_changed: prior input digest differs".into());
        }

        budget = Some(Duration::from_millis(case.runner.timeout_ms));
        let remaining = Duration::from_millis(case.runner.timeout_ms)
            .checked_sub(started.elapsed())
            .filter(|left| !left.is_zero())
            .ok_or("timeout: replay exceeded wall-time budget")?;
        let outcome = run_child(&attempt.input, executable, remaining, None);
        let report = match outcome {
            Ok(report) => report,
            Err(error) => {
                failures.push(error.clone());
                executed.push(Attempt {
                    environment: attempt.environment.clone(),
                    seed: attempt.seed,
                    replay: attempt.replay,
                    input: attempt.input.clone(),
                    outcome: Err(error),
                    trace: None,
                });
                return Err(failures.join("\n"));
            }
        };
        if comparable(&report)? != comparable(prior)? {
            failures.push(format!(
                "replay_mismatch: {} seed={:?}",
                attempt.environment, attempt.seed
            ));
        }
        if let Err(error) = &report.result {
            failures.push(error.clone());
        }
        executed.push(Attempt {
            environment: attempt.environment.clone(),
            seed: attempt.seed,
            replay: attempt.replay,
            input: attempt.input.clone(),
            outcome: Ok(report),
            trace: None,
        });
    }
    if budget.is_some_and(|budget| started.elapsed() >= budget) {
        failures.push("timeout: replay exceeded wall-time budget".into());
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

#[cfg(test)]
mod action_tests {
    use omnigraph::seams::Effect;

    use super::seams::admitted_effect;
    use crate::runner_config::SeamAction;

    #[cfg(unix)]
    #[test]
    fn timeout_retains_trace_after_worker_cleanup() {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("worker");
        std::fs::write(&executable, "#!/bin/sh\nwhile :; do :; done\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let case_path = dir.path().join("case.gqt");
        let trace =
            crate::trace::create(dir.path(), &crate::trace::Start::test(&case_path)).unwrap();
        let input = Input {
            case_path,
            stem: "timeout".into(),
            text: "".into(),
            case_digest: String::new(),
            plan_digest: String::new(),
            executable_digest: String::new(),
            source_revision: String::new(),
            source_digest: String::new(),
            environment: Environment {
                execution: Execution::Dst {
                    storage: crate::runner_config::Storage::InMemoryObjectStore,
                    seeds: vec![0],
                },
            },
            seed: Some(0),
            effective_settings: settings::EffectiveSettings::for_seed(Some(0)),
            engine: Engine::V2,
            store: None,
            server: None,
            bless: false,
            measure: false,
            model: String::new(),
        };
        let error = run_child(&input, &executable, Duration::ZERO, Some(&trace)).unwrap_err();
        assert!(error.starts_with("timeout:"), "{error}");
        assert!(error.contains("contained=true"), "{error}");
        let text = std::fs::read_to_string(trace).unwrap();
        let records: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "start");
    }

    #[test]
    fn external_worker_environment_is_scoped_to_its_backend() {
        for (uri, accepted) in [
            ("file:///tmp/graph", vec![]),
            (
                "s3://bucket/graph",
                vec![
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "AWS_ENDPOINT_URL_S3",
                    "AWS_ALLOW_HTTP",
                    "AWS_S3_FORCE_PATH_STYLE",
                ],
            ),
            (
                "az://container/graph",
                vec![
                    "AZURE_STORAGE_ACCOUNT_NAME",
                    "AZURE_STORAGE_ACCOUNT_KEY",
                    "AZURE_STORAGE_USE_EMULATOR",
                    "AZURITE_BLOB_STORAGE_URL",
                    "IDENTITY_ENDPOINT",
                    "IDENTITY_HEADER",
                    "MSI_ENDPOINT",
                    "AWS_ALLOW_HTTP",
                    "OBJECT_STORE_CLIENT_MAX_RETRIES",
                    "OBJECT_STORE_CLIENT_RETRY_TIMEOUT",
                ],
            ),
        ] {
            for key in &accepted {
                assert!(super::store_environment_variable(uri, key), "{uri}: {key}");
            }
            for key in [
                "FAILPOINTS",
                "RAYON_NUM_THREADS",
                "LANCE_CPU_THREADS",
                "OMNIGRAPH_ENGINE",
                "DST_ENTROPY_SEED",
                "HOME",
                "PATH",
                "GQT_WORKER_INPUT",
            ] {
                assert!(!super::store_environment_variable(uri, key), "{uri}: {key}");
            }
        }
        assert!(!super::store_environment_variable(
            "s3://bucket/graph",
            "AZURE_STORAGE_ACCOUNT_KEY"
        ));
        assert!(!super::store_environment_variable(
            "az://container/graph",
            "AWS_SECRET_ACCESS_KEY"
        ));
    }
    #[test]
    fn fail_and_contention_are_selectable_regardless_of_declaration_order() {
        for effects in [
            [Effect::Fail, Effect::Contention],
            [Effect::Contention, Effect::Fail],
        ] {
            assert_eq!(
                admitted_effect(SeamAction::Fail, &effects),
                Some(Effect::Fail)
            );
            assert_eq!(
                admitted_effect(SeamAction::Contention, &effects),
                Some(Effect::Contention)
            );
        }
        assert_eq!(
            admitted_effect(SeamAction::Fail, &[Effect::Contention]),
            Some(Effect::Contention),
            "existing fail directives on contention-only seams stay compatible"
        );
        assert_eq!(
            admitted_effect(SeamAction::Contention, &[Effect::Fail, Effect::Skip]),
            None,
            "explicit contention cannot fall back to another effect"
        );
    }

    fn concurrent_session(index: usize, step: u64) -> crate::measure::SessionCtx {
        crate::measure::SessionCtx::new(
            index,
            crate::measure::Label {
                slot: ["w2", "r1"][index],
                step,
                line: Some(18),
                kind: "query",
            },
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
    }

    #[tokio::test]
    async fn concurrent_capture_retains_partial_session_evidence_after_panic() {
        let report = super::capture(
            "same-input".into(),
            crate::measure::SESSION.scope(concurrent_session(1, 1), async {
                super::begin_operation(serde_json::json!({"ordinal": 1}));
                super::observe(|| "partial read".into());
                super::record("query_result", serde_json::json!({"rows": ["alice"]}));
                panic!("session panicked before block completion");
            }),
        )
        .await
        .unwrap();
        assert_eq!(report.code, "worker_failed");
        assert_eq!(report.observations, ["partial read"]);
        assert_eq!(report.evidence.len(), 1);
        assert_eq!(report.evidence[0]["session"], "r1");
        assert_eq!(report.evidence[0]["operation"]["ordinal"], 1);
        assert_eq!(
            report.evidence[0]["value"]["rows"],
            serde_json::json!(["alice"])
        );
    }

    #[tokio::test]
    async fn concurrent_capture_shares_report_limits_across_sessions() {
        let entries = super::capture("same-input".into(), async {
            crate::measure::SESSION
                .scope(concurrent_session(0, 1), async {
                    for _ in 0..50_000 {
                        super::observe(|| "observation".into());
                    }
                })
                .await;
            super::finish_concurrent_observations();
            crate::measure::SESSION
                .scope(concurrent_session(1, 1), async {
                    for _ in 0..50_001 {
                        super::record("result", serde_json::Value::Null);
                    }
                })
                .await;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(entries.code, "report_failed");
        assert_eq!(entries.observations.len(), 50_000);
        assert_eq!(entries.evidence.len(), 50_000);

        let bytes = super::capture("same-input".into(), async {
            for index in [0, 1] {
                crate::measure::SESSION
                    .scope(concurrent_session(index, 1), async {
                        super::observe(|| "x".repeat(super::LIMIT / 2));
                    })
                    .await;
            }
            super::record("result", serde_json::Value::Null);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(bytes.code, "report_failed");
        assert_eq!(bytes.observations.len(), 2);
        assert!(bytes.evidence.is_empty());
    }

    #[tokio::test]
    async fn concurrent_replay_preserves_session_results_across_interleavings() {
        async fn report(order: &[usize], row: &str) -> super::WorkerReport {
            super::capture("same-input".into(), async {
                for ordinal in [1, 2] {
                    super::begin_operation(serde_json::json!({"ordinal": ordinal}));
                    super::observe(|| format!("before block {ordinal}"));
                    for &event in order {
                        let index = event / 2;
                        let ctx = concurrent_session(index, ordinal);
                        crate::measure::SESSION
                            .scope(ctx, async {
                                super::observe(|| format!("session {index} event {}", event % 2));
                                super::record(
                                    "query_result",
                                    serde_json::json!({"event": event % 2, "rows": [{"name": row}]}),
                                );
                            })
                            .await;
                    }
                    crate::record_concurrent_outcome(
                        ordinal as usize,
                        &[serde_json::json!({"label": "w2", "outcome": "ok"}),
                          serde_json::json!({"label": "r1", "outcome": "ok"})],
                        &crate::concurrent::Outcome {
                            stuck_at: None,
                            failure: None,
                            unattributed: 0,
                            log: Vec::new(),
                            wall_ms: 0,
                        },
                    );
                    super::observe(|| format!("after block {ordinal}"));
                }
                Ok(())
            })
            .await
            .unwrap()
        }

        let expected = super::comparable(&report(&[0, 2, 1, 3], "alice").await).unwrap();
        let interleaved = super::comparable(&report(&[2, 0, 3, 1], "alice").await).unwrap();
        assert!(
            expected == interleaved,
            "session completion order must not change replay evidence"
        );
        for (order, row) in [
            (&[1, 2, 0, 3][..], "alice"),
            (&[0, 2, 1][..], "alice"),
            (&[0, 2, 1, 3][..], "bob"),
        ] {
            assert_ne!(
                expected,
                super::comparable(&report(order, row).await).unwrap(),
                "per-session order, completeness and rows remain replay evidence"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_replay_compares_outcomes_without_background_measurements() {
        async fn report(
            sessions: &[serde_json::Value],
            block: &crate::concurrent::Outcome,
        ) -> super::WorkerReport {
            super::capture("same-input".into(), async {
                crate::record_concurrent_outcome(3, sessions, block);
                Ok(())
            })
            .await
            .unwrap()
        }

        let mut sessions = vec![serde_json::json!({
            "label": "r1", "outcome": "ok", "message": null, "script": null,
        })];
        let mut block = crate::concurrent::Outcome {
            stuck_at: None,
            failure: None,
            unattributed: 2,
            log: Vec::new(),
            wall_ms: 5,
        };
        let expected = super::comparable(&report(&sessions, &block).await).unwrap();
        block.unattributed = 3;
        block.wall_ms = 9;
        block.log.push(serde_json::json!({"wall_ms": 7}));
        let measured = super::comparable(&report(&sessions, &block).await).unwrap();
        assert!(
            expected == measured,
            "background I/O and timing measurements must not change replay equality"
        );

        block.stuck_at = Some(0);
        assert_ne!(
            expected,
            super::comparable(&report(&sessions, &block).await).unwrap()
        );
        block.stuck_at = None;
        block.failure = Some("session starved".into());
        assert_ne!(
            expected,
            super::comparable(&report(&sessions, &block).await).unwrap()
        );
        block.failure = None;
        sessions[0]["outcome"] = "failed".into();
        sessions[0]["message"] = "query failed".into();
        assert_ne!(
            expected,
            super::comparable(&report(&sessions, &block).await).unwrap()
        );
    }

    fn measured(environment: &str, seed: &str, step: u64, requests: u64) -> super::MeasureRow {
        super::MeasureRow {
            environment: environment.into(),
            seed: seed.into(),
            slot: "step".into(),
            step,
            line: "7".into(),
            kind: "mutate".into(),
            counts: serde_json::json!({"requests": requests, "repeat_reads": 0, "after_publish": 0}),
            detail: serde_json::Value::Null,
        }
    }

    #[test]
    fn a_baseline_write_replaces_only_the_scopes_the_run_measured() {
        let dir = std::env::temp_dir().join(format!("gqt-baseline-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("baseline.tsv");
        let other_case_and_both_seeds_of_this_one = [
            super::write_baseline(&path, "other.gqt", &[measured("dst", "0", 1, 5)]),
            super::write_baseline(
                &path,
                "this.gqt",
                &[measured("dst", "0", 1, 10), measured("dst", "42", 1, 11)],
            ),
        ];
        assert!(
            other_case_and_both_seeds_of_this_one
                .iter()
                .all(Result::is_ok)
        );
        super::write_baseline(&path, "this.gqt", &[measured("dst", "0", 1, 12)]).unwrap();
        let rows = super::read_baseline(&path, "this.gqt").unwrap();
        let kept: Vec<(String, u64)> = rows
            .iter()
            .map(|row| (row.key.1.clone(), row.requests))
            .collect();
        assert_eq!(kept, [("0".to_string(), 12), ("42".to_string(), 11)]);
        assert_eq!(super::read_baseline(&path, "other.gqt").unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_case_has_one_baseline_name_whichever_way_it_was_spelled() {
        let nested = "v2/planner/input_ann_nprobes.gqt";
        let relative = std::path::Path::new("cases").join(nested);
        assert!(relative.is_file(), "cargo test runs in the crate root");
        let absolute = crate::corpus_root().join(nested);
        assert_eq!(super::baseline_case_key(&relative), nested);
        assert_eq!(super::baseline_case_key(&absolute), nested);
        let outside = std::env::temp_dir().join("elsewhere.gqt");
        assert!(super::baseline_case_key(&outside).ends_with("elsewhere.gqt"));
        assert!(std::path::Path::new(&super::baseline_case_key(&outside)).is_absolute());
    }
}
