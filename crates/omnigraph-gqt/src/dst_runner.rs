use std::cell::RefCell;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use omnigraph::error::OmniError;
use omnigraph_compiler::QueryResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runner_config::{Environment, Execution, Fault};
use crate::{CaseOutcome, parse_case, stem_of};

mod known_failure;
mod settings;

pub(crate) fn validate_known_failure(case: &crate::Case) -> Result<(), String> {
    known_failure::validate(case)
}

const WORKER_INPUT: &str = "OMNIGRAPH_GQT_WORKER_INPUT";
const WORKER_REPORT: &str = "OMNIGRAPH_GQT_WORKER_REPORT";
const LIMIT: usize = 16 * 1024 * 1024;

tokio::task_local! {
    static OBSERVATIONS: RefCell<Observations>;
}

#[derive(Default)]
struct Observations {
    values: Vec<String>,
    bytes: usize,
    overflow: bool,
    fault_hits: Vec<String>,
    operation: Option<serde_json::Value>,
    evidence: Vec<serde_json::Value>,
    lifecycle: Option<[std::sync::Arc<std::sync::atomic::AtomicU64>; 2]>,
}

pub(crate) fn observe(value: impl FnOnce() -> String) {
    OBSERVATIONS
        .try_with(|events| {
            let mut events = events.borrow_mut();
            if events.overflow {
                return;
            }
            let value = value();
            events.bytes += value.len();
            if events.bytes > LIMIT || events.values.len() + events.evidence.len() >= 100_000 {
                events.overflow = true;
            } else {
                events.values.push(value);
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
    Vec<omnigraph::failpoints::ScopedFailPoint>,
) {
    let counts = [
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    ];
    let guards = [
        omnigraph::failpoints::names::INIT_AFTER_SCHEMA_CONTRACT_WRITTEN,
        omnigraph::failpoints::names::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
    ]
    .into_iter()
    .zip(counts.iter())
    .map(|(name, count)| {
        let count = count.clone();
        omnigraph::failpoints::ScopedFailPoint::with_callback(name, move || {
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
    })
    .collect();
    (counts, guards)
}

pub(crate) fn begin_operation(value: serde_json::Value) {
    OBSERVATIONS
        .try_with(|events| events.borrow_mut().operation = Some(value))
        .unwrap_or_default();
}

pub(crate) fn record(kind: &str, value: serde_json::Value) {
    OBSERVATIONS
        .try_with(|events| {
            let mut events = events.borrow_mut();
            if events.overflow {
                return;
            }
            let event =
                serde_json::json!({"kind": kind, "operation": events.operation, "value": value});
            events.bytes += event.to_string().len();
            if events.bytes > LIMIT || events.values.len() + events.evidence.len() >= 100_000 {
                events.overflow = true;
            } else {
                events.evidence.push(event);
            }
        })
        .unwrap_or_default();
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

pub(crate) fn observe_fault(error: &OmniError) {
    if let OmniError::RecoveryRequired {
        operation_id,
        reason,
    } = error
    {
        record(
            "typed_error",
            serde_json::json!({"error": "RecoveryRequired", "reason": reason, "operation_id": operation_id, "message": error.to_string()}),
        );
    }
    let message = match error {
        OmniError::Manifest(error) => &error.message,
        OmniError::RecoveryRequired { reason, .. } => reason,
        _ => return,
    };
    if let Some(name) = message.strip_prefix("injected failpoint triggered: ") {
        OBSERVATIONS
            .try_with(|events| events.borrow_mut().fault_hits.push(name.to_string()))
            .unwrap_or_default();
        record("fault_delivered", serde_json::json!({"hook": name}));
        observe(|| format!("fault delivered: {name}"));
    }
}

fn supported_fault(fault: &Fault) -> bool {
    [
        "branch_merge.post_authority_capture",
        "branch_merge.post_sidecar_pre_fork",
        "branch_merge.post_effects_pre_confirm",
        "branch_merge.post_phase_b_pre_manifest_commit",
        "mutation.post_sidecar_pre_fork",
    ]
    .contains(&fault.at.as_str())
}

#[cfg(tokio_unstable)]
pub(crate) fn arm_fault(
    fault: Option<&Fault>,
) -> Result<Option<omnigraph::failpoints::ScopedFailPoint>, String> {
    OBSERVATIONS
        .try_with(|events| events.borrow_mut().fault_hits.clear())
        .unwrap_or_default();
    fault
        .map(|fault| {
            if !supported_fault(fault) {
                return Err(format!(
                    "unsupported_environment: unsupported DST failpoint: {}",
                    fault.at
                ));
            }
            let action = if fault.occurrence == 1 {
                "1*return".into()
            } else {
                format!("{}*off->1*return", fault.occurrence - 1)
            };
            Ok(omnigraph::failpoints::ScopedFailPoint::new(
                &fault.at, &action,
            ))
        })
        .transpose()
}

#[cfg(not(tokio_unstable))]
pub(crate) fn arm_fault(fault: Option<&Fault>) -> Result<Option<()>, String> {
    if fault.is_some() {
        Err("unsupported_environment: DST runner is unavailable".into())
    } else {
        Ok(None)
    }
}

pub(crate) fn finish_fault(fault: Option<&Fault>) -> Result<(), String> {
    let Some(fault) = fault else {
        return Ok(());
    };
    let hits = OBSERVATIONS
        .try_with(|events| events.borrow().fault_hits.clone())
        .unwrap_or_default();
    if hits != [fault.at.clone()] {
        Err(format!(
            "fault_unobserved: configured faults were not observed exactly once by the selected operation: {}; observed: {hits:?}",
            fault.at
        ))
    } else {
        Ok(())
    }
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
    bless: bool,
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    environment: Environment,
    seed: Option<u64>,
    replay: usize,
    known_failure: bool,
    input: Input,
    outcome: Result<WorkerReport, String>,
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
    for name in [
        "FAILPOINTS",
        "DST_ENTROPY_SEED",
        "RAYON_NUM_THREADS",
        "LANCE_CPU_THREADS",
        "LANCE_DETERMINISTIC_BACKOFF",
        crate::CASE_TIMEOUT_ENV,
        "OMNIGRAPH_TRAVERSAL_MODE",
    ] {
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
        "fault_unobserved",
        "fault_cleanup_failed",
        "replay_mismatch",
        "worker_failed",
        "report_failed",
        "timeout",
        "unexpected_pass",
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
    if summary.result.is_ok() && summary.attempts.iter().any(|attempt| attempt.known_failure) {
        return "known_failure";
    }
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
            seed: None,
            fast_tier: true,
        },
    )
}

/// Select only declared environment/seed values; omission executes the complete case.
pub fn run_selected(
    path: &Path,
    executable: &Path,
    bless: bool,
    target: Option<&str>,
    storage: Option<&str>,
    seed: Option<u64>,
) -> CaseOutcome {
    run_with_selection(
        path,
        executable,
        bless,
        Selection {
            target,
            storage,
            seed,
            fast_tier: false,
        },
    )
}

struct Selection<'a> {
    target: Option<&'a str>,
    storage: Option<&'a str>,
    seed: Option<u64>,
    fast_tier: bool,
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
    summary.code = summary_code(&summary).into();
    let result = match save_summary(&summary) {
        Ok(()) => summary.result,
        Err(error) => Err(format!("{error}; original result: {:?}", summary.result)),
    };
    CaseOutcome {
        stem: stem_of(path),
        elapsed: started.elapsed(),
        result,
    }
}

fn save_summary(summary: &Summary) -> Result<(), String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gqt-artifacts");
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
    println!("GQT replay: omnigraph-gqt --replay '{}'", path.display());
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
    match save_summary(&summary) {
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
    let selected = selection.target;
    let selected_storage = selection.storage;
    let selected_seed = selection.seed;
    let text =
        String::from_utf8(read_bounded(path)?).map_err(|e| format!("invalid_case: UTF-8: {e}"))?;
    let text: std::sync::Arc<str> = text.into();
    summary.case_digest = Some(digest(text.as_bytes()));
    let case =
        parse_case(&stem_of(path), &text).map_err(|error| format!("invalid_case: {error}"))?;
    summary.declared = Some(case.runner.environments.clone());
    for env in &case.runner.environments {
        for seed in env.seeds() {
            for replay in 0..if seed.is_some() { 2 } else { 1 } {
                summary.planned.push(Planned {
                    environment: env.clone(),
                    seed,
                    replay,
                    selected: env.matches(selected, selected_storage)
                        && selected_seed.is_none_or(|s| seed == Some(s)),
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
            env.matches(selected, selected_storage)
                && selected_seed.is_none_or(|seed| env.seeds().contains(&Some(seed)))
        })
        .collect::<Vec<_>>();
    if selected_envs.is_empty() {
        return Err("invalid_case: environment selector matches no declared environment".into());
    }
    for env in &selected_envs {
        env.admit(!case.faults.is_empty())?;
    }
    for (ordinal, fault) in &case.faults {
        if !supported_fault(fault) {
            return Err(format!(
                "unsupported_environment: unsupported DST failpoint: {}",
                fault.at
            ));
        }
        let step = case
            .items
            .iter()
            .flat_map(|item| match item {
                crate::Item::Step(step) => std::slice::from_ref(step),
                crate::Item::Loop { steps, .. } => steps.as_slice(),
            })
            .find(|step| step.ordinal() == *ordinal);
        let compatible = match step {
            Some(crate::Step::Mutate(_)) => fault.at.starts_with("mutation."),
            Some(crate::Step::Control(crate::ControlStep {
                write: crate::ControlWrite::Merge { .. },
                ..
            })) => fault.at.starts_with("branch_merge."),
            _ => false,
        };
        if !compatible {
            return Err(format!(
                "unsupported_environment: fault {} is incompatible with operation {ordinal}",
                fault.at
            ));
        }
    }
    if bless && case.known_failure.is_some() {
        return Err("invalid_case: bless is refused for known_failure cases".into());
    }
    if bless
        && (case.runner.environments.len() != 1
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
                let left = match remaining() {
                    Ok(left) => left,
                    Err(error) => {
                        failures.push(error);
                        return Err(failures.join("\n"));
                    }
                };
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
                    bless,
                };
                let mut outcome = run_child(&input, executable, left);
                if let Ok(report) = &outcome {
                    let size = json(report)?.len();
                    retained_bytes = retained_bytes.saturating_add(size);
                    if retained_bytes > LIMIT / 2 {
                        evidence_exhausted = true;
                        outcome = Err("report_failed: invocation evidence budget exhausted; remaining attempts not run".into());
                    }
                }
                let mut known_failure = false;
                match &outcome {
                    Ok(report) => {
                        match known_failure::classify(&case, report) {
                            Ok(accepted) => {
                                known_failure = accepted;
                                if accepted {
                                    println!(
                                        "KNOWN_FAILURE environment={} seed={seed:?} replay={replay}: known recovery failure",
                                        env
                                    );
                                }
                            }
                            Err(error) => {
                                failures.push(format!(
                                    "{error}; environment={} seed={seed:?} replay={replay}",
                                    env
                                ));
                            }
                        }
                        reports.push(json(report)?);
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
                    known_failure,
                    input,
                    outcome,
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

fn run_child(input: &Input, executable: &Path, budget: Duration) -> Result<WorkerReport, String> {
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
    input.effective_settings.configure(&mut cmd);
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
            let report = match worker_report(&input, input_digest.clone()) {
                Ok(report) => report,
                Err(error) => WorkerReport {
                    code: error_code(&error).into(),
                    phase: "preflight".into(),
                    input_digest,
                    result: Err(error),
                    observations: vec![],
                    evidence: vec![],
                },
            };
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
    input.environment.admit(!case.faults.is_empty())?;
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
                crate::execute_case(&case, &input.case_path, input.bless),
            ))
        }
        Some(seed) => dst_report(input, &case, seed, input_digest),
    }
}

async fn capture(
    input_digest: String,
    future: impl std::future::Future<Output = Result<(), String>>,
) -> Result<WorkerReport, String> {
    use futures::FutureExt;
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

    async fn run(
        &self,
        resources: &mut omnigraph_dst::memory::MemoryStorage,
        _workload_seed: u64,
    ) -> Self::Output {
        capture(
            self.input_digest.clone(),
            crate::execute_case_with_storage(
                self.case,
                &self.input.case_path,
                &resources.root,
                resources.adapter.clone(),
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
    let _failpoints = omnigraph::failpoints::FailScenario::setup();
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
            save_summary(&summary)?;
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
    match save_summary(&summary) {
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
        known_failure::verify_status(&case, prior, attempt.known_failure)?;
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
        let outcome = run_child(&attempt.input, executable, remaining);
        let report = match outcome {
            Ok(report) => report,
            Err(error) => {
                failures.push(error.clone());
                executed.push(Attempt {
                    environment: attempt.environment.clone(),
                    seed: attempt.seed,
                    replay: attempt.replay,
                    known_failure: false,
                    input: attempt.input.clone(),
                    outcome: Err(error),
                });
                return Err(failures.join("\n"));
            }
        };
        if &report != prior {
            failures.push(format!(
                "replay_mismatch: {} seed={:?}",
                attempt.environment, attempt.seed
            ));
        }
        let known_failure = match known_failure::classify(&case, &report) {
            Ok(known_failure) => known_failure,
            Err(error) => {
                failures.push(error);
                false
            }
        };
        executed.push(Attempt {
            environment: attempt.environment.clone(),
            seed: attempt.seed,
            replay: attempt.replay,
            known_failure,
            input: attempt.input.clone(),
            outcome: Ok(report),
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
