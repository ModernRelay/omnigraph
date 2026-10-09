#![recursion_limit = "512"]

use futures::FutureExt as _;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::storage::StorageAdapter;
use omnigraph_compiler::settings::{DEFINITIONS, Engine};
use omnigraph_compiler::{ParamMap, QueryResult};
use omnigraph_gqt_core::concurrent::ConcurrentStep;
pub use omnigraph_gqt_core::runner_config;
use omnigraph_gqt_core::runner_config::{Execution, SeamDirective};
use omnigraph_gqt_core::{
    Case, ControlStep, ControlWrite, ExecutionHost, Item, QueryStep, Step, StepFail,
    canonical_json, case_session, parse_case, run_session, seed_case,
};
use serde_json::Value;
use std::ffi::OsString;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod concurrent;
mod discovery;
mod dst_runner;
mod measure;
mod trace;
pub use discovery::list_cases;
pub use dst_runner::{
    MeasureOptions, replay_report, report_cli_refusal, run_corpus_case, run_selected,
    run_worker_if_requested,
};

pub const CASE_TIMEOUT_ENV: &str = "OMNIGRAPH_GQ_CASE_TIMEOUT_SECS";
pub const DEFAULT_CASE_TIMEOUT_SECS: u64 = 10;
pub const BLESS_ENV: &str = "OMNIGRAPH_GQ_BLESS";
pub const ENGINE_ENV: &str = "OMNIGRAPH_GQ_ENGINE";
pub(crate) const RETIRED_SETTING_ENVIRONMENT: [&str; 1] = ["OMNIGRAPH_TRAVERSAL_MODE"];

struct GqtHost;

impl ExecutionHost for GqtHost {
    type StepGuard = Vec<dst_runner::seams::ArmedSeam>;

    fn admit_case(&self, _case: &Case) -> Result<(), String> {
        Ok(())
    }
    fn arm_seams(&self, seams: &[SeamDirective], step: &Step) -> Result<Self::StepGuard, String> {
        dst_runner::arm_seams(seams, step)
    }
    fn finish_seams(&self, guard: Self::StepGuard) -> Result<(), String> {
        dst_runner::finish_seams(guard)
    }
    fn observe(&self, value: impl FnOnce() -> String) {
        dst_runner::observe(value);
    }
    fn record(&self, kind: &str, value: impl FnOnce() -> Value) {
        dst_runner::record(kind, value());
    }
    fn begin_operation(&self, value: impl FnOnce() -> Value) {
        dst_runner::begin_operation(value());
    }
    fn observe_query<E: std::fmt::Display>(&self, result: &Result<QueryResult, E>, ordered: bool) {
        dst_runner::observe_query(result, ordered);
    }
    fn observe_fault(&self, error: &omnigraph::error::OmniError) {
        dst_runner::observe_fault(error);
    }
    fn lifetime_counts(&self) -> Option<[u64; 2]> {
        dst_runner::lifetime_counts()
    }
    fn active(&self) -> bool {
        dst_runner::active()
    }
    fn observe_snapshot(&self) -> bool {
        dst_runner::active()
    }
    fn measure_step_begin(&self, ordinal: u64, line: Option<u64>, kind: &'static str) {
        dst_runner::measure_step_begin(ordinal, line, kind);
    }
    fn measure_step_end(&self, ordinal: u64) {
        dst_runner::measure_step_end(ordinal);
    }
    fn reference_query<'a>(
        &'a self,
        session: &'a Session,
        step: &'a QueryStep,
        params: &'a ParamMap,
    ) -> futures::future::BoxFuture<'a, Result<QueryResult, String>> {
        async move {
            session
                .clone()
                .with_read_executor(Arc::new(omnigraph_reference_engine::ReferenceEngine))
                .query(
                    ReadTarget::branch(&step.branch),
                    &step.source,
                    &step.name,
                    params,
                )
                .await
                .map_err(|error| error.to_string())
        }
        .boxed()
    }
    fn concurrent_step<'a>(
        &'a self,
        session: &'a Session,
        case: &'a Case,
        step: &'a ConcurrentStep,
    ) -> futures::future::BoxFuture<'a, Result<(), StepFail>> {
        run_concurrent_step(session, case, step).boxed()
    }
}

#[derive(Debug)]
pub struct CaseOutcome {
    pub stem: String,
    pub elapsed: Duration,
    pub result: Result<(), String>,
}

pub fn measure_model_names() -> Vec<&'static str> {
    measure::MODELS.iter().map(|model| model.name).collect()
}

async fn open_case_store(
    case: &Case,
    engine: Engine,
) -> Result<(Session, String, tempfile::TempDir), String> {
    let fixture = case
        .fixture
        .as_ref()
        .ok_or("invalid_case: a case without schema and seed requires --store <URI>")?;
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
    let uri = dir
        .path()
        .to_str()
        .ok_or_else(|| "temp path is not utf-8".to_string())?
        .to_string();
    let db = Omnigraph::init(&uri, &fixture.schema)
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    let session = case_session(db, case, engine)?;
    seed_case(&session, &fixture.seed, case.needs_indices).await?;
    Ok((session, uri, dir))
}

fn execute_case<'a>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    match engine_from_env() {
        Ok(engine) => execute_case_on_engine(case, path, bless, engine, None),
        Err(error) => futures::future::ready(Err(error)).boxed(),
    }
}

fn execute_case_on_engine<'a>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
    engine: Engine,
    store: Option<&'a str>,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_case_inner(case, path, bless, engine, store).boxed()
}

async fn execute_case_inner(
    case: &Case,
    path: &Path,
    bless: bool,
    engine: Engine,
    store: Option<&str>,
) -> Result<(), String> {
    case.admit_store(store)?;
    if let Some(uri) = store {
        let db = Omnigraph::open(uri)
            .await
            .map_err(|e| format!("open failed: {e}"))?;
        let session = case_session(db, case, engine)?;
        return omnigraph_gqt_core::execute_steps(case, path, bless, session, uri, None, &GqtHost)
            .await
            .map(|_| ());
    }
    let (session, uri, _dir) = open_case_store(case, engine).await?;
    omnigraph_gqt_core::execute_steps(case, path, bless, session, &uri, None, &GqtHost)
        .await
        .map(|_| ())
}

#[cfg(tokio_unstable)]
async fn execute_case_with_storage(
    case: &Case,
    path: &Path,
    uri: &str,
    storage: Arc<dyn StorageAdapter>,
    engine: Engine,
) -> Result<(), String> {
    let fixture = case
        .fixture
        .as_ref()
        .ok_or("invalid_case: DST requires schema and seed")?;
    let db =
        Omnigraph::init_with_storage(uri, &fixture.schema, storage.clone(), Default::default())
            .await
            .map_err(|e| format!("init failed: {e}"))?;
    let session = case_session(db, case, engine)?;
    seed_case(&session, &fixture.seed, case.needs_indices).await?;
    omnigraph_gqt_core::execute_steps(case, path, false, session, uri, Some(storage), &GqtHost)
        .await
        .map(|_| ())
}

async fn run_concurrent_step(
    session: &Session,
    case: &Case,
    step: &ConcurrentStep,
) -> Result<(), StepFail> {
    let label = format!("step {} (concurrent)", step.ordinal);
    let fail = |message: String| StepFail::new(label.clone(), message);
    if !dst_runner::active() {
        return Err(fail(
            "a concurrent block runs under the DST runner only; its environment is omnigraph-engine-dst".into(),
        ));
    }
    let line = case.source_lines.get(&step.ordinal).map(|l| *l as u64);
    let run = concurrent::Run::begin(step, concurrent::starve_budget(case.runner.timeout_ms));
    let sessions = step.sessions.iter().enumerate().map(|(index, op)| {
        let ctx = measure::SessionCtx::new(
            index,
            measure::session_label(&op.label, step.ordinal as u64, line, op.kind.name()),
            run.draining_flag(),
        );
        let run = Arc::clone(&run);
        measure::SESSION.scope(ctx, async move {
            let outcome = match run.start_session(index).await {
                Ok(()) => run_session(&GqtHost, session, op).await,
                Err(aborted) => Err(aborted),
            };
            let script = run.finish_session(index).await;
            (outcome, script)
        })
    });
    let results = tokio::select! {
        biased;
        results = futures::future::join_all(sessions) => results,
        () = run.drive_clock() => unreachable!("the clock driver never returns"),
    };
    let block = run.end();
    let sessions_evidence: Vec<Value> = step
        .sessions
        .iter()
        .zip(&results)
        .map(|(op, (outcome, script))| {
            serde_json::json!({
                "label": op.label,
                "outcome": if outcome.is_ok() { "ok" } else { "failed" },
                "message": outcome.as_ref().err(),
                "script": script.as_ref().err(),
            })
        })
        .collect();
    record_concurrent_outcome(step.ordinal, &sessions_evidence, &block);
    if let Some(failure) = block.failure {
        return Err(fail(failure));
    }
    for (op, (outcome, _)) in step.sessions.iter().zip(&results) {
        if let Err(message) = outcome {
            return Err(fail(format!("session `{}`: {message}", op.label)));
        }
    }
    Ok(())
}

fn record_concurrent_outcome(ordinal: usize, sessions: &[Value], block: &concurrent::Outcome) {
    dst_runner::finish_concurrent_observations();
    dst_runner::record(
        "concurrent_block",
        serde_json::json!({
            "sessions": sessions,
            "stuck_at": block.stuck_at.map(|at| at + 1),
            "failure": block.failure,
        }),
    );
    measure::push_detail(serde_json::json!({
        "slot": "concurrent",
        "step": ordinal,
        "value": {"grants": block.log, "wall_ms": block.wall_ms, "unattributed": block.unattributed},
    }));
}

pub async fn run_case(path: PathBuf, bless: bool) -> Result<(), String> {
    let stem = stem_of(&path);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("cannot read case file: {e}"))?;
    let case = parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    case.admit_store(None)?;
    if case.runner.environments.len() != 1
        || !matches!(
            case.runner.environments[0].execution,
            Execution::Engine { .. }
        )
    {
        return Err("DST cases require the file dispatcher; the async normal runner cannot execute mode: dst".into());
    }
    case.runner.environments[0].admit(case.needs_dst())?;
    execute_case(&case, &path, bless).await
}

pub fn corpus_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cases")
}

pub fn bless_from_env() -> Result<bool, String> {
    match std::env::var(BLESS_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(v)) => Err(format!(
            "invalid_case: {BLESS_ENV} requires UTF-8, got {v:?}"
        )),
        Ok(v) if v == "1" => Ok(true),
        Ok(v) if v == "0" || v.is_empty() => Ok(false),
        Ok(v) => Err(format!(
            "invalid_case: {BLESS_ENV} takes 1 (or 0/empty/unset), got `{v}`"
        )),
    }
}

pub fn engine_from_env() -> Result<Engine, String> {
    match std::env::var(ENGINE_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(Engine::V2),
        Err(std::env::VarError::NotUnicode(v)) => Err(format!(
            "invalid_case: {ENGINE_ENV} requires UTF-8, got {v:?}"
        )),
        Ok(v) if v.is_empty() => Ok(Engine::V2),
        Ok(v) => Engine::from_spelling(&v).ok_or_else(|| {
            format!("invalid_case: {ENGINE_ENV} takes v2 (or empty/unset), got `{v}`")
        }),
    }
}

pub fn case_budget_from_env() -> Duration {
    Duration::from_secs(env_positive(CASE_TIMEOUT_ENV).unwrap_or(DEFAULT_CASE_TIMEOUT_SECS))
}

pub fn stem_of(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("<non-utf8>")
        .to_string()
}

pub fn env_positive(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    match value.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => panic!("{name} takes a positive integer, got `{value}`"),
    }
}

pub fn settings_override_refusal(
    lookup: impl Fn(&'static str) -> Option<OsString>,
) -> Option<String> {
    DEFINITIONS
        .iter()
        .find_map(|spec| {
            let value = lookup(spec.env)?;
            Some(format!(
                "{}={} is set; logic tests run under the case's own settings, unset it (a case \
                 that must run one value writes `set {} = <value>;` in a `--- mutate` step)",
                spec.env,
                value.to_string_lossy(),
                spec.name
            ))
        })
        .or_else(|| {
            RETIRED_SETTING_ENVIRONMENT.into_iter().find_map(|name| {
                let value = lookup(name)?;
                Some(format!(
                    "{name}={} is set; it names no setting any more and decides nothing, unset it",
                    value.to_string_lossy()
                ))
            })
        })
}

pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

pub async fn run_bounded<F>(stem: &str, budget: Duration, case: F) -> CaseOutcome
where
    F: Future<Output = Result<(), String>>,
{
    let started = Instant::now();
    let case = AssertUnwindSafe(case).catch_unwind();
    let result = match tokio::time::timeout(budget, case).await {
        Ok(Ok(result)) => result,
        Ok(Err(payload)) => Err(format!(
            "case panicked: {}",
            panic_message(payload.as_ref())
        )),
        Err(_) => Err(format!(
            "case exceeded its budget of {:.2}s ({CASE_TIMEOUT_ENV} overrides the default of \
             {DEFAULT_CASE_TIMEOUT_SECS}s; libtest's --test-threads sets how many cases run \
             concurrently; a case over budget belongs in a `heavy-repro:` `#[ignore]`d test, \
             not the corpus)",
            budget.as_secs_f64()
        )),
    };
    CaseOutcome {
        stem: stem.to_string(),
        elapsed: started.elapsed(),
        result,
    }
}

pub async fn run_case_bounded(path: PathBuf, budget: Duration, bless: bool) -> CaseOutcome {
    let stem = stem_of(&path);
    run_bounded(&stem, budget, run_case(path, bless)).await
}

#[cfg(test)]
mod tests;
