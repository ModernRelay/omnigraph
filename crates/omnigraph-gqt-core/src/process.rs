//! Execution through the product's `omnigraph` binary: every step is one
//! child process over a graph shallow clone (`--target <clone> --base <base>`),
//! judged from the `--json` document the command prints the way the served
//! path judges a route's answer. The engine under test is the binary, never
//! this crate's; the harness times each child from spawn to exit and reads
//! the `--stats` document it writes.

use futures::FutureExt as _;
use omnigraph::db::MergeOutcome;
use omnigraph::loader::LoadMode;
use omnigraph_api_types::{BranchMergeOutcome, BranchOutcomeOutput, ChangeOutput, ReadOutput};
use omnigraph_compiler::query::ast::show_statement_name;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use crate::served::{check_read_expect, control_statement, wire_params};
use crate::{
    Bindings, BranchReadStep, Case, ControlStep, ControlWrite, ExecutionHost, Item, ListStep,
    LoadStep, MergeExpect, MutateExpect, MutateStep, Observer, QueryExpect, QueryStep, ShowStep,
    Step, StepFail, WriteExpect, bound_snapshot, bound_source, check_error_expect, check_rows_json,
    expectation_evidence, merge_outcome_word, operation, record_binding, step_kind, step_label,
    substitute_bound,
};

/// The binary and the clone a `--store` run addresses: the product's
/// `omnigraph`, the clone root it opens as `--target`, the frozen base it
/// reads through as `--base`, the directory each step's `--stats` document and
/// load batch land in, and the whole environment the children run with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessTarget {
    pub executable: PathBuf,
    pub target: String,
    pub base: String,
    pub scratch: PathBuf,
    pub environment: Vec<(String, String)>,
}

/// The record key under which a step's `--stats` document reaches the host.
pub const PROCESS_STATS_RECORD: &str = "process_stats";

/// Refuses, before any child runs, what the process route cannot execute.
/// The route reads a built base through a clone, so a case that carries its
/// own schema and seed stays in-process; each step is a fresh process, so a
/// `--- restart` is admitted anywhere but as the measured step, where it would
/// measure nothing; seams, pins, `--- csv` sections and settings steps have
/// no command-line form.
pub fn admit_process_workload(case: &Case, measured: usize) -> Result<(), String> {
    let mut refused = Vec::new();
    if case.fixture.is_some() {
        refused.push("a `--- schema` or seed (the route reads a built base)".to_string());
    }
    if !case.csv_sections.is_empty() {
        refused.push("`--- csv` fixtures".to_string());
    }
    if !case.seams.is_empty() {
        refused.push("seam directives".to_string());
    }
    if case.traversal.is_some() {
        refused.push("the `# traversal:` pin".to_string());
    }
    if case.csv_slice_bytes.is_some() {
        refused.push("the `# csv_slice_bytes:` pin".to_string());
    }
    if case.overwrite_chunk_rows.is_some() {
        refused.push("the `# overwrite_chunk_rows:` pin".to_string());
    }
    if case.writer_held {
        refused.push("the `# writer:` pin".to_string());
    }
    for step in case.items.iter().flat_map(|item| match item {
        Item::Step(step) => std::slice::from_ref(step),
        Item::Loop { steps, .. } => steps.as_slice(),
    }) {
        let ordinal = step.ordinal();
        match step {
            Step::Restart { .. } if ordinal == measured => {
                refused.push(format!(
                    "step {ordinal} (a measured restart: every step is its own process)"
                ));
            }
            Step::Restart { .. } => {}
            Step::Settings(_) | Step::Concurrent(_) => {
                refused.push(format!("step {ordinal} ({})", step_kind(step)));
            }
            Step::Query(query) => {
                if query.plan.is_some() {
                    refused.push(format!("step {ordinal} (expect plan)"));
                }
                if query.same_as_v1 {
                    refused.push(format!("step {ordinal} (expect same as v1)"));
                }
            }
            Step::Control(control) => {
                if !control.prefix.is_empty() {
                    refused.push(format!(
                        "step {ordinal} ({} with a settings prefix)",
                        control.name
                    ));
                }
                if let ControlWrite::Load { stmt, .. } = &control.write
                    && !stmt.source.contains("://")
                {
                    refused.push(format!(
                        "step {ordinal} (load csv source without an absolute URI)"
                    ));
                }
                if let ControlWrite::Load { plan: Some(_), .. } = &control.write {
                    refused.push(format!("step {ordinal} (expect plan on a load)"));
                }
            }
            Step::Show(show) if !show.prefix.is_empty() => {
                refused.push(format!("step {ordinal} (show with a settings prefix)"));
            }
            Step::Mutate(_)
            | Step::Load(_)
            | Step::List(_)
            | Step::Show(_)
            | Step::BranchRead(_) => {}
        }
    }
    if refused.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unsupported_environment: the omnigraph binary has no command for {}",
            refused.join(", ")
        ))
    }
}

/// Executes a measured workload admitted by [`admit_process_workload`]: one
/// child per step, a restart off the measured step as a no-op (the next step
/// is a fresh process either way), every step reported like the others.
pub fn execute_measured_process<'a, H: ExecutionHost, O: Observer>(
    case: &'a Case,
    target: &'a ProcessTarget,
    host: &'a H,
    observer: &'a O,
    measured: usize,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_process_inner(case, target, host, observer, measured).boxed()
}

async fn execute_process_inner<H: ExecutionHost, O: Observer>(
    case: &Case,
    target: &ProcessTarget,
    host: &H,
    observer: &O,
    measured: usize,
) -> Result<(), String> {
    host.admit_case(case)?;
    admit_process_workload(case, measured)?;
    let client = ProcessClient::new(target, Duration::from_millis(case.runner.timeout_ms));
    observer.observe(|| {
        format!(
            "lifetime: one `{}` process per step over clone {} of base {}",
            target.executable.display(),
            target.target,
            target.base
        )
    });
    let mut bindings = Bindings::new();
    for item in &case.items {
        let (values, var, steps): (Vec<Option<&str>>, Option<&str>, Vec<&Step>) = match item {
            Item::Step(step) => (vec![None], None, vec![step]),
            Item::Loop { var, values, steps } => (
                values.iter().map(|v| Some(v.as_str())).collect(),
                Some(var.as_str()),
                steps.iter().collect(),
            ),
        };
        for value in values {
            let binding = var.zip(value);
            for step in &steps {
                let ordinal = step.ordinal();
                observer.begin_operation(|| {
                    serde_json::json!({"ordinal": ordinal, "source_line": case.source_lines.get(&ordinal), "loop_binding": binding, "generation": 0})
                });
                observer.observe(|| {
                    format!(
                        "operation: ordinal={ordinal} line={:?} binding={binding:?} process expected={step:?}",
                        case.source_lines.get(&ordinal)
                    )
                });
                observer.record("expectation", || expectation_evidence(step));
                observer.measure_step_begin(
                    ordinal as u64,
                    case.source_lines.get(&ordinal).map(|line| *line as u64),
                    step_kind(step),
                );
                let outcome = match step {
                    Step::Query(q) => query(observer, &client, q, binding, &bindings).await,
                    Step::Mutate(m) => mutate(observer, &client, m, binding, &bindings)
                        .await
                        .and_then(|commit| {
                            record_binding(
                                &mut bindings,
                                m.bind.as_deref(),
                                commit,
                                &step_label(ordinal, "mutate", binding),
                            )
                        }),
                    Step::Load(l) => load(observer, &client, l, binding).await,
                    Step::Control(c) => control(observer, &client, c, binding, &bindings)
                        .await
                        .and_then(|commit| {
                            record_binding(
                                &mut bindings,
                                c.bind.as_deref(),
                                commit,
                                &step_label(ordinal, c.name, binding),
                            )
                        }),
                    Step::List(l) => list(observer, &client, l, binding).await,
                    Step::Show(s) => show(observer, &client, s, binding).await,
                    Step::BranchRead(r) => {
                        branch_read(observer, &client, r, binding, &bindings).await
                    }
                    Step::Restart { .. } => {
                        observer.observe(|| {
                            "restart: the next step is a fresh process; no door opened".to_string()
                        });
                        Ok(())
                    }
                    Step::Settings(_) | Step::Concurrent(_) => Err(StepFail::new(
                        step_label(ordinal, step_kind(step), binding),
                        "unsupported_environment: refused by process admission".into(),
                    )),
                };
                observer.measure_step_end(ordinal as u64);
                observer.record("assertion", || match &outcome {
                    Ok(()) => serde_json::json!({"status": "passed"}),
                    Err(error) => {
                        serde_json::json!({"status": "failed", "code": "assertion_failed", "message": error.message})
                    }
                });
                observer.observe(|| {
                    format!(
                        "operation result: {:?}",
                        outcome.as_ref().map_err(|f| (&f.label, &f.message))
                    )
                });
                if let Err(fail) = outcome {
                    return Err(format!("{}: {}", fail.label, fail.message));
                }
            }
        }
    }
    Ok(())
}

/// The most bytes of one child's standard output the runner reads.
const OUTPUT_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Bytes of an undecodable output a diagnostic repeats.
const ECHO_LIMIT_BYTES: usize = 4 * 1024;

/// The commands one process run spawns: every one with the same target, base
/// and environment, each with a `--stats` document of its own.
struct ProcessClient<'a> {
    target: &'a ProcessTarget,
    timeout: Duration,
}

/// The `error` line of the document a failed command prints under `--json`:
/// what an `error:` expect holds against. The typed detail beside it (`code`,
/// `disposition`) is the caller's business, as on the wire.
#[derive(serde::Deserialize)]
struct ErrorText {
    error: String,
}

impl<'a> ProcessClient<'a> {
    fn new(target: &'a ProcessTarget, timeout: Duration) -> Self {
        Self { target, timeout }
    }

    fn stats_path(&self, ordinal: usize, suffix: &str) -> PathBuf {
        self.target
            .scratch
            .join(format!("step-{ordinal:04}{suffix}.stats.json"))
    }

    /// Runs one command; the outer error is the step not reaching a verdict
    /// (no spawn, timeout, no document), the inner the command's `error` text;
    /// the `--stats` document lands under [`PROCESS_STATS_RECORD`] on exit.
    async fn run<Res: DeserializeOwned>(
        &self,
        observer: &impl Observer,
        ordinal: usize,
        suffix: &str,
        args: &[String],
    ) -> Result<Result<Res, String>, String> {
        let stats = self.stats_path(ordinal, suffix);
        match std::fs::remove_file(&stats) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "could not clear the previous --stats document {}: {e}",
                    stats.display()
                ));
            }
        }
        let mut command = tokio::process::Command::new(&self.target.executable);
        command
            .args(args)
            .arg("--target")
            .arg(&self.target.target)
            .arg("--base")
            .arg(&self.target.base)
            .arg("--stats")
            .arg(&stats)
            .arg("--json")
            .arg("--yes")
            .env_clear()
            .envs(self.target.environment.iter().map(|(k, v)| (k, v)))
            .current_dir(&self.target.scratch)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = tokio::time::timeout(self.timeout, command.output())
            .await
            .map_err(|_| {
                format!(
                    "`{}` did not exit within {} ms",
                    self.target.executable.display(),
                    self.timeout.as_millis()
                )
            })?
            .map_err(|e| format!("could not run `{}`: {e}", self.target.executable.display()))?;
        if output.stdout.len() > OUTPUT_LIMIT_BYTES {
            return Err(format!(
                "`{}` printed {} bytes, over the {OUTPUT_LIMIT_BYTES}-byte output bound",
                self.target.executable.display(),
                output.stdout.len()
            ));
        }
        match std::fs::read(&stats) {
            Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(document) => observer.record(
                    PROCESS_STATS_RECORD,
                    || serde_json::json!({"ordinal": ordinal, "stats": document}),
                ),
                Err(e) => {
                    return Err(format!(
                        "`{}` wrote a --stats document this runner cannot decode: {e}",
                        stats.display()
                    ));
                }
            },
            Err(e) => {
                return Err(format!(
                    "`{}` left no --stats document at {}: {e}; stderr: {}",
                    self.target.executable.display(),
                    stats.display(),
                    echo(&output.stderr)
                ));
            }
        }
        if output.status.success() {
            return serde_json::from_slice(&output.stdout).map(Ok).map_err(|e| {
                format!(
                    "`{}` exited 0 with a document this runner cannot decode: {e}: {}",
                    self.target.executable.display(),
                    echo(&output.stdout)
                )
            });
        }
        let error: ErrorText = serde_json::from_slice(&output.stdout).map_err(|e| {
            format!(
                "`{}` exited {} without an error document: {e}: stdout {}; stderr {}",
                self.target.executable.display(),
                output.status,
                echo(&output.stdout),
                echo(&output.stderr)
            )
        })?;
        Ok(Err(error.error))
    }

    /// `query`: the read's rows, materialized before the verdict so the
    /// operation timer includes decoding, as the served path's does.
    async fn read(
        &self,
        observer: &impl Observer,
        ordinal: usize,
        suffix: &str,
        args: &[String],
    ) -> Result<Result<Vec<Value>, String>, String> {
        match self
            .run::<ReadOutput>(observer, ordinal, suffix, args)
            .await?
        {
            Ok(output) => rows_of(&output).map(Ok),
            Err(error) => Ok(Err(error)),
        }
    }

    /// A read statement (`branch list`, `show …`, a branch control read) as
    /// `query -e <text>`, sent bare: no name, params, branch or snapshot.
    async fn statement(
        &self,
        observer: &impl Observer,
        ordinal: usize,
        text: &str,
    ) -> Result<Result<Vec<Value>, String>, String> {
        self.read(
            observer,
            ordinal,
            "",
            &["query".into(), "-e".into(), text.into()],
        )
        .await
    }

    /// Every generated batch through `load --data <file>`; one child per batch.
    async fn load_generated(
        &self,
        observer: &impl Observer,
        ordinal: usize,
        generated: &crate::Generated,
        branch: &str,
        mode: LoadMode,
    ) -> Result<(), String> {
        for (index, batch) in generated.batch_texts().enumerate() {
            let (table, range, text) = batch?;
            let data = self
                .target
                .scratch
                .join(format!("step-{ordinal:04}-batch-{index:04}.jsonl"));
            std::fs::write(&data, text).map_err(|e| format!("writing {}: {e}", data.display()))?;
            let args = [
                "load".to_string(),
                "--data".into(),
                data.to_string_lossy().into_owned(),
                "--mode".into(),
                load_mode_word(mode).into(),
                "--branch".into(),
                branch.to_string(),
            ];
            self.run::<Value>(observer, ordinal, &format!("-batch-{index:04}"), &args)
                .await?
                .map_err(|error| {
                    format!(
                        "generated load {table} rows {}..{} on {branch}: {error}",
                        range.start, range.end
                    )
                })?;
        }
        Ok(())
    }
}

fn load_mode_word(mode: LoadMode) -> &'static str {
    match mode {
        LoadMode::Overwrite => "overwrite",
        LoadMode::Append => "append",
        LoadMode::Merge => "merge",
    }
}

/// Undecodable output as diagnostic text, cut to its bound.
fn echo(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    if text.len() <= ECHO_LIMIT_BYTES {
        return text.into_owned();
    }
    let mut end = ECHO_LIMIT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes, truncated)", &text[..end], body.len())
}

fn rows_of(output: &ReadOutput) -> Result<Vec<Value>, String> {
    match serde_json::from_str::<Value>(output.rows.get()) {
        Ok(Value::Array(rows)) => Ok(rows),
        Ok(_) => Err("the command returned a non-array row set".into()),
        Err(e) => Err(format!("the command's rows are not JSON: {e}")),
    }
}

fn params_argument(params: Option<Value>) -> Result<Vec<String>, String> {
    match params {
        Some(value) => Ok(vec![
            "--params".into(),
            serde_json::to_string(&value).map_err(|e| e.to_string())?,
        ]),
        None => Ok(Vec::new()),
    }
}

async fn query(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &QueryStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "query", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let params = match wire_params(step.params_raw.as_ref(), binding, bindings) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                QueryExpect::Error { needle } if e.contains(needle) => {
                    observer.record("parameter_error", || serde_json::json!({"message": e}));
                    Ok(())
                }
                QueryExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                QueryExpect::Rows { .. } | QueryExpect::Succeeds => Err(fail(e)),
            };
        }
    };
    let snapshot = bound_snapshot(step, bindings).map_err(&fail)?;
    let mut args = vec![
        "query".to_string(),
        "-e".into(),
        step.source.clone(),
        step.name.clone(),
    ];
    match snapshot {
        Some(snapshot) => args.extend(["--snapshot".into(), snapshot]),
        None => args.extend(["--branch".into(), step.branch.clone()]),
    }
    args.extend(params_argument(params).map_err(&fail)?);
    let outcome = operation(
        observer,
        step.ordinal,
        client.read(observer, step.ordinal, "", &args),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    check_read_expect(
        observer,
        &label,
        &step.expect,
        outcome,
        "query failed",
        "the query succeeded",
        binding,
    )
}

async fn mutate(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &MutateStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<Option<String>, StepFail> {
    let label = step_label(step.ordinal, "mutate", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let source = bound_source(step, bindings).map_err(&fail)?;
    let params = match wire_params(step.params_raw.as_ref(), binding, bindings) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                MutateExpect::Error { needle } if e.contains(needle) => {
                    observer.record("parameter_error", || serde_json::json!({"message": e}));
                    Ok(None)
                }
                MutateExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                MutateExpect::Ok | MutateExpect::Affected { .. } => Err(fail(e)),
            };
        }
    };
    let mut args = vec![
        "mutate".to_string(),
        "-e".into(),
        source.into_owned(),
        step.name.clone(),
    ];
    if step.branch_spelled {
        args.extend(["--branch".into(), step.branch.clone()]);
    }
    args.extend(params_argument(params).map_err(&fail)?);
    let outcome = operation(
        observer,
        step.ordinal,
        client.run::<ChangeOutput>(observer, step.ordinal, "", &args),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    observer.record("mutation_result", || match &outcome {
        Ok(result) => {
            serde_json::json!({"nodes": result.affected_nodes, "edges": result.affected_edges})
        }
        Err(error) => serde_json::json!({"error": error}),
    });
    if let Ok(ChangeOutput {
        commit: Some(commit),
        ..
    }) = &outcome
    {
        observer.record("commit", || crate::commit_evidence(commit.published));
    }
    observer.observe(|| match &outcome {
        Ok(result) => format!(
            "actual affected: nodes={} edges={}",
            result.affected_nodes, result.affected_edges
        ),
        Err(error) => format!("actual mutation error: {error}"),
    });
    let commit_id =
        |output: &ChangeOutput| output.commit.as_ref().map(|c| c.graph_commit_id.clone());
    match &step.expect {
        MutateExpect::Ok => outcome
            .map(|output| commit_id(&output))
            .map_err(|e| fail(format!("mutation failed: {e}"))),
        MutateExpect::Affected { nodes, edges } => {
            let result = outcome.map_err(|e| fail(format!("mutation failed: {e}")))?;
            if result.affected_nodes == *nodes && result.affected_edges == *edges {
                Ok(commit_id(&result))
            } else {
                Err(fail(format!(
                    "affected counts mismatch: expected nodes={nodes} edges={edges}, got nodes={} edges={}",
                    result.affected_nodes, result.affected_edges
                )))
            }
        }
        MutateExpect::Error { needle } => {
            check_error_expect(observer, needle, outcome, "the mutation succeeded")
                .map(|()| None)
                .map_err(fail)
        }
    }
}

async fn load(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &LoadStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "load", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        observer,
        step.ordinal,
        client.load_generated(
            observer,
            step.ordinal,
            &step.generated,
            &step.branch,
            step.mode,
        ),
    )
    .await
    .map_err(&fail)?;
    match &step.expect {
        WriteExpect::Ok => outcome.map_err(fail),
        WriteExpect::Error { needle } => {
            check_error_expect(observer, needle, outcome, "load succeeded").map_err(fail)
        }
    }
}

fn check_write_expect(
    observer: &impl Observer,
    name: &str,
    expect: &WriteExpect,
    outcome: Result<ChangeOutput, String>,
) -> Result<(), String> {
    match expect {
        WriteExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| format!("`{name}` failed: {e}")),
        WriteExpect::Error { needle } => {
            check_error_expect(observer, needle, outcome, &format!("`{name}` succeeded"))
        }
    }
}

fn merge_outcome(wire: BranchMergeOutcome) -> MergeOutcome {
    match wire {
        BranchMergeOutcome::AlreadyUpToDate => MergeOutcome::AlreadyUpToDate,
        BranchMergeOutcome::FastForward => MergeOutcome::FastForward,
        BranchMergeOutcome::Merged => MergeOutcome::Merged,
    }
}

async fn control(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &ControlStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<Option<String>, StepFail> {
    let name = step.name;
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let mut args = vec![
        "mutate".to_string(),
        "-e".into(),
        control_statement(&step.write, bindings).map_err(&fail)?,
    ];
    if let ControlWrite::Load {
        branch: Some(branch),
        ..
    } = &step.write
    {
        args.extend(["--branch".into(), branch.clone()]);
    }
    let outcome = operation(
        observer,
        step.ordinal,
        client.run::<ChangeOutput>(observer, step.ordinal, "", &args),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    observer.observe(|| format!("actual control: {:?}", outcome.as_ref().map(|o| &o.outcome)));
    let commit = outcome.as_ref().ok().and_then(|output| {
        output.commit.as_ref().map(|c| {
            observer.record("commit", || crate::commit_evidence(c.published));
            c.graph_commit_id.clone()
        })
    });
    match &step.write {
        ControlWrite::Create { expect, .. }
        | ControlWrite::Delete { expect, .. }
        | ControlWrite::Publish { expect, .. }
        | ControlWrite::Drop { expect, .. } => {
            if let (WriteExpect::Ok, Ok(output)) = (expect, &outcome) {
                let answered = matches!(
                    (&step.write, &output.outcome),
                    (
                        ControlWrite::Create { .. },
                        Some(BranchOutcomeOutput::Created { .. })
                    ) | (
                        ControlWrite::Delete { .. },
                        Some(BranchOutcomeOutput::Deleted { .. })
                    ) | (
                        ControlWrite::Publish { .. },
                        Some(BranchOutcomeOutput::Published)
                    ) | (
                        ControlWrite::Drop { .. },
                        Some(BranchOutcomeOutput::Dropped { .. })
                    )
                );
                if !answered {
                    return Err(fail(format!(
                        "`{name}` answered without a `{name}` outcome: {:?}",
                        output.outcome
                    )));
                }
            }
            check_write_expect(observer, name, expect, outcome)
                .map(|()| commit)
                .map_err(fail)
        }
        ControlWrite::Load { expect, .. } => {
            if let (WriteExpect::Ok, Ok(output)) = (expect, &outcome)
                && (output.outcome.is_some() || output.commit.is_none())
            {
                return Err(fail(format!(
                    "`{name}` answered as a branch statement, not as a load: {:?}",
                    output.outcome
                )));
            }
            check_write_expect(observer, name, expect, outcome)
                .map(|()| commit)
                .map_err(fail)
        }
        ControlWrite::Merge {
            expect: MergeExpect::Write(expect),
            ..
        } => check_write_expect(observer, name, expect, outcome)
            .map(|()| commit)
            .map_err(fail),
        ControlWrite::Merge {
            expect: MergeExpect::Outcome(want),
            ..
        } => {
            let output = outcome.map_err(|e| fail(format!("`{name}` failed: {e}")))?;
            match output.outcome {
                Some(BranchOutcomeOutput::Merged { merge, .. })
                    if merge_outcome(merge) == *want =>
                {
                    Ok(commit)
                }
                Some(BranchOutcomeOutput::Merged { merge, .. }) => Err(fail(format!(
                    "merge outcome mismatch: expected `{}`, got `{}`",
                    merge_outcome_word(*want),
                    merge_outcome_word(merge_outcome(merge))
                ))),
                other => Err(fail(format!(
                    "`{name}` answered without a merge outcome: {other:?}"
                ))),
            }
        }
    }
}

async fn list(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &ListStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "branch list", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        observer,
        step.ordinal,
        client.statement(observer, step.ordinal, "branch list"),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    check_read_expect(
        observer,
        &label,
        &step.expect,
        outcome,
        "`branch list` failed",
        "`branch list` succeeded",
        binding,
    )
}

async fn show(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &ShowStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = show_statement_name(step.id);
    let label = step_label(step.ordinal, &name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        observer,
        step.ordinal,
        client.statement(observer, step.ordinal, &format!("{name};")),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    check_read_expect(
        observer,
        &label,
        &step.expect,
        outcome,
        &format!("`{name}` failed"),
        &format!("`{name}` succeeded"),
        binding,
    )
}

async fn branch_read(
    observer: &impl Observer,
    client: &ProcessClient<'_>,
    step: &BranchReadStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<(), StepFail> {
    let name = step.read.statement_name();
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let text = substitute_bound(&step.source, bindings);
    let outcome = operation(
        observer,
        step.ordinal,
        client.statement(observer, step.ordinal, &text),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    match &step.expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
            ..
        } => {
            let actual = outcome.map_err(|e| fail(format!("`{name}` failed: {e}")))?;
            check_rows_json(
                observer,
                &label,
                &actual,
                *ordered,
                &substitute_bound(body_raw, bindings),
                *span,
                binding,
            )
        }
        QueryExpect::Error { needle } => {
            check_error_expect(observer, needle, outcome, &format!("`{name}` succeeded"))
                .map_err(fail)
        }
        QueryExpect::Succeeds => outcome
            .map(drop)
            .map_err(|e| fail(format!("`{name}` failed: {e}"))),
    }
}
