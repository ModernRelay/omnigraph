//! Execution against a running `omnigraph-server`: every step kind travels
//! to the route that already serves it, as the statement text the server
//! parses for any client, and is judged from the `omnigraph-api-types`
//! answer the way the in-process path judges the engine's. Nothing the
//! server has no door for is emulated; `admit_served` refuses it by name.

use futures::FutureExt as _;
use omnigraph::db::MergeOutcome;
use omnigraph::loader::LoadMode;
use omnigraph_api_types::{
    BranchMergeOutcome, BranchOutcomeOutput, ChangeOutput, ChangeRequest, HTTP_API_CONTRACT,
    HTTP_API_CONTRACT_HEADER, IngestRequest, QueryRequest, ReadOutput, SchemaOutput,
};
use omnigraph_compiler::query::ast::{Param, show_statement_name};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

use crate::{
    Case, ControlStep, ControlWrite, ExecutionHost, Fixture, Item, ListStep, LoadStep, MAIN_BRANCH,
    MergeExpect, MutateExpect, MutateStep, QueryExpect, QueryStep, Seed, ShowStep, Step, StepFail,
    WriteExpect, build_params, check_error_expect, check_rows_json, expectation_evidence,
    merge_outcome_word, operation, step_kind, step_label, substitute,
};

/// The server a `--server` run addresses: its base URL, the graph id under
/// `/graphs/{id}`, and the bearer token of a token-protected deployment.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerTarget {
    pub url: String,
    pub graph: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// `Debug` names the target and whether a token is set, never the token.
impl std::fmt::Debug for ServerTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTarget")
            .field("url", &self.url)
            .field("graph", &self.graph)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Refuses, before any request, every section the server has no door for,
/// naming each so the author sees what keeps the case in-process only.
pub fn admit_served(case: &Case) -> Result<(), String> {
    let mut refused = Vec::new();
    if !case.seams.is_empty() {
        let ordinals = case
            .seams
            .keys()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        refused.push(format!("seam directives at step {ordinals}"));
    }
    if case.traversal.is_some() {
        refused.push("the `# traversal:` pin".to_string());
    }
    if case.needs_indices {
        refused.push("queries that need indices (search, fuzzy, nearest, rrf)".to_string());
    }
    for step in case.items.iter().flat_map(|item| match item {
        Item::Step(step) => std::slice::from_ref(step),
        Item::Loop { steps, .. } => steps.as_slice(),
    }) {
        let ordinal = step.ordinal();
        match step {
            Step::Restart { .. } | Step::Settings(_) | Step::Concurrent(_) => {
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
            Step::Control(control) if !control.prefix.is_empty() => {
                refused.push(format!(
                    "step {ordinal} ({} with a settings prefix)",
                    control.name
                ));
            }
            Step::Show(show) if !show.prefix.is_empty() => {
                refused.push(format!("step {ordinal} (show with a settings prefix)"));
            }
            Step::Mutate(_) | Step::Load(_) | Step::Control(_) | Step::List(_) | Step::Show(_) => {}
        }
    }
    if refused.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unsupported_environment: omnigraph-server has no door for {}",
            refused.join(", ")
        ))
    }
}

/// Executes the case against the server: the schema check and the seed
/// when the case carries a fixture, then every step through its route.
pub fn execute_steps_served<'a, H: ExecutionHost>(
    case: &'a Case,
    target: &'a ServerTarget,
    host: &'a H,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_served_inner(case, target, host).boxed()
}

async fn execute_served_inner<H: ExecutionHost>(
    case: &Case,
    target: &ServerTarget,
    host: &H,
) -> Result<(), String> {
    host.admit_case(case)?;
    admit_served(case)?;
    let client = ServerClient::new(target, Duration::from_millis(case.runner.timeout_ms))?;
    host.observe(|| format!("lifetime: served by {} graph {}", target.url, target.graph));
    if let Some(fixture) = &case.fixture {
        check_schema(&client, &fixture.schema).await?;
        seed(&client, fixture).await?;
    }
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
                host.begin_operation(|| {
                    serde_json::json!({"ordinal": ordinal, "source_line": case.source_lines.get(&ordinal), "loop_binding": binding, "generation": 0})
                });
                host.observe(|| {
                    format!(
                        "operation: ordinal={ordinal} line={:?} binding={binding:?} served expected={step:?}",
                        case.source_lines.get(&ordinal)
                    )
                });
                host.record("expectation", || expectation_evidence(step));
                host.measure_step_begin(
                    ordinal as u64,
                    case.source_lines.get(&ordinal).map(|line| *line as u64),
                    step_kind(step),
                );
                let outcome = match step {
                    Step::Query(q) => query(host, &client, q, binding).await,
                    Step::Mutate(m) => mutate(host, &client, m, binding).await,
                    Step::Load(l) => load(host, &client, l, binding).await,
                    Step::Control(c) => control(host, &client, c, binding).await,
                    Step::List(l) => list(host, &client, l, binding).await,
                    Step::Show(s) => show(host, &client, s, binding).await,
                    Step::Settings(_) | Step::Restart { .. } | Step::Concurrent(_) => {
                        Err(StepFail::new(
                            step_label(ordinal, step_kind(step), binding),
                            "unsupported_environment: refused by served admission".into(),
                        ))
                    }
                };
                host.measure_step_end(ordinal as u64);
                host.record("assertion", || match &outcome {
                    Ok(()) => serde_json::json!({"status": "passed"}),
                    Err(error) => {
                        serde_json::json!({"status": "failed", "code": "assertion_failed", "message": error.message})
                    }
                });
                host.observe(|| {
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

/// The requests one served run sends: every one under `/graphs/{id}`, with
/// the contract header and, when given, the bearer token.
struct ServerClient {
    graph_base: String,
    token: Option<String>,
    http: reqwest::Client,
}

/// The server's error body as the step judges it: the `error` line of an
/// `ErrorOutput`; every typed detail beside it is the client's business.
#[derive(Deserialize)]
struct ErrorText {
    error: String,
}

impl ServerClient {
    /// `timeout` bounds every request at the case budget, so a stalled
    /// server fails the step instead of outliving the run.
    fn new(target: &ServerTarget, timeout: Duration) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(timeout)
            .build()
            .map_err(|e| format!("server client: {e}"))?;
        Ok(Self {
            graph_base: format!(
                "{}/graphs/{}",
                target.url.trim_end_matches('/'),
                target.graph
            ),
            token: target.token.clone(),
            http,
        })
    }

    /// Sends one request. The outer error is the step not reaching a verdict
    /// (transport, an undecodable body); the inner is the server's `error`
    /// text, what an `error:` expect holds against.
    async fn send<Res: DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<Result<Res, String>, String> {
        let builder = builder.header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT);
        let builder = match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        };
        let response = builder
            .send()
            .await
            .map_err(|e| format!("server request failed: {e}"))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| format!("server response failed: {e}"))?;
        if status.is_success() {
            return serde_json::from_slice(&body).map(Ok).map_err(|e| {
                format!(
                    "server answered {status} with a body this runner cannot decode: {e}: {}",
                    String::from_utf8_lossy(&body)
                )
            });
        }
        let error: ErrorText = serde_json::from_slice(&body).map_err(|e| {
            format!(
                "server answered {status} without an error body: {e}: {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        Ok(Err(error.error))
    }

    async fn post<Req: Serialize, Res: DeserializeOwned>(
        &self,
        route: &str,
        body: &Req,
    ) -> Result<Result<Res, String>, String> {
        self.send(
            self.http
                .post(format!("{}{route}", self.graph_base))
                .json(body),
        )
        .await
    }

    async fn get<Res: DeserializeOwned>(&self, route: &str) -> Result<Result<Res, String>, String> {
        self.send(self.http.get(format!("{}{route}", self.graph_base)))
            .await
    }

    async fn load(&self, branch: &str, mode: LoadMode, data: String) -> Result<(), String> {
        let request = IngestRequest {
            branch: Some(branch.to_string()),
            from: None,
            mode: Some(mode),
            data,
        };
        self.post::<_, Value>("/load", &request).await?.map(|_| ())
    }

    /// Every generated batch through `/load`, with the message
    /// `Generated::load_observed` gives a refused batch.
    async fn load_generated(
        &self,
        generated: &crate::Generated,
        branch: &str,
        mode: LoadMode,
    ) -> Result<(), String> {
        for batch in generated.batch_texts() {
            let (table, range, text) = batch?;
            self.load(branch, mode, text).await.map_err(|error| {
                format!(
                    "generated load {table} rows {}..{} on {branch}: {error}",
                    range.start, range.end
                )
            })?;
        }
        Ok(())
    }
}

/// The served graph must already carry the case's schema: a cluster-backed
/// graph refuses a remote schema apply, so the check is the whole contract.
async fn check_schema(client: &ServerClient, schema: &str) -> Result<(), String> {
    let output: SchemaOutput = client
        .get("/schema")
        .await?
        .map_err(|e| format!("unsupported_environment: GET /schema failed: {e}"))?;
    if normalized_schema(&output.schema_source) != normalized_schema(schema) {
        return Err(format!(
            "unsupported_environment: the server's graph schema differs from the case's `--- schema`; served:\n{}\ncase:\n{}",
            output.schema_source, schema
        ));
    }
    Ok(())
}

fn normalized_schema(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The fixture's seed through `/load`, as `Seed::load` sends it to a
/// session: the inline rows overwrite on `main`, generated batches append.
async fn seed(client: &ServerClient, fixture: &Fixture) -> Result<(), String> {
    match &fixture.seed {
        Seed::Inline(text) if text.trim().is_empty() => Ok(()),
        Seed::Inline(text) => client
            .load(MAIN_BRANCH, LoadMode::Overwrite, text.clone())
            .await
            .map_err(|error| format!("seed load failed: {error}")),
        Seed::Generated(generated) => {
            client
                .load_generated(generated, MAIN_BRANCH, LoadMode::Append)
                .await
        }
    }
}

/// The `--- params` body as it goes on the wire: validated against the
/// declaration exactly as the in-process path does, then sent as JSON.
fn wire_params(
    params_raw: Option<&String>,
    ast_params: &[Param],
    binding: Option<(&str, &str)>,
) -> Result<Option<Value>, String> {
    build_params(params_raw, ast_params, binding)?;
    params_raw
        .map(|raw| {
            serde_json::from_str::<Value>(&substitute(raw, binding))
                .map_err(|e| format!("params are not valid JSON: {e}"))
        })
        .transpose()
}

fn rows_of(output: &ReadOutput) -> Result<Vec<Value>, String> {
    match serde_json::from_str::<Value>(output.rows.get()) {
        Ok(Value::Array(rows)) => Ok(rows),
        Ok(_) => Err("server returned a non-array row set".into()),
        Err(e) => Err(format!("server rows are not JSON: {e}")),
    }
}

/// Holds a read expect against the rows a route answered; the shape
/// section is the executor's Arrow schema against the compiler's, which
/// no wire answer carries, so it is not judged here.
fn check_read_expect(
    host: &impl ExecutionHost,
    label: &str,
    expect: &QueryExpect,
    outcome: Result<ReadOutput, String>,
    failed: &str,
    succeeded: &str,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let fail = |message: String| StepFail::new(label.to_string(), message);
    match expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
            shape,
        } => {
            let output = outcome.map_err(|e| fail(format!("{failed}: {e}")))?;
            let actual = rows_of(&output).map_err(&fail)?;
            host.observe(|| {
                format!(
                    "shape: {} line(s) not judged under a server target",
                    shape.lines.len()
                )
            });
            check_rows_json(host, label, &actual, *ordered, body_raw, *span, binding)
        }
        QueryExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, succeeded).map_err(fail)
        }
    }
}

async fn query(
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &QueryStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "query", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let params = match wire_params(step.params_raw.as_ref(), &step.decl.params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                QueryExpect::Error { needle } if e.contains(needle) => {
                    host.record("parameter_error", || serde_json::json!({"message": e}));
                    Ok(())
                }
                QueryExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                QueryExpect::Rows { .. } => Err(fail(e)),
            };
        }
    };
    let request = QueryRequest {
        query: step.source.clone(),
        name: Some(step.name.clone()),
        params,
        branch: Some(step.branch.clone()),
        snapshot: None,
        settings: None,
    };
    let outcome = operation(
        host,
        step.ordinal,
        client.post::<_, ReadOutput>("/query", &request),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    check_read_expect(
        host,
        &label,
        &step.expect,
        outcome,
        "query failed",
        "the query succeeded",
        binding,
    )
}

async fn mutate(
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &MutateStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "mutate", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let params = match wire_params(step.params_raw.as_ref(), &step.ast_params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                MutateExpect::Error { needle } if e.contains(needle) => {
                    host.record("parameter_error", || serde_json::json!({"message": e}));
                    Ok(())
                }
                MutateExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                MutateExpect::Ok | MutateExpect::Affected { .. } => Err(fail(e)),
            };
        }
    };
    let request = ChangeRequest {
        query: step.source.clone(),
        name: Some(step.name.clone()),
        params,
        branch: Some(step.branch.clone()),
        settings: None,
    };
    let outcome = operation(
        host,
        step.ordinal,
        client.post::<_, ChangeOutput>("/mutate", &request),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    host.record("mutation_result", || match &outcome {
        Ok(result) => {
            serde_json::json!({"nodes": result.affected_nodes, "edges": result.affected_edges})
        }
        Err(error) => serde_json::json!({"error": error}),
    });
    host.observe(|| match &outcome {
        Ok(result) => format!(
            "actual affected: nodes={} edges={}",
            result.affected_nodes, result.affected_edges
        ),
        Err(error) => format!("actual mutation error: {error}"),
    });
    match &step.expect {
        MutateExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| fail(format!("mutation failed: {e}"))),
        MutateExpect::Affected { nodes, edges } => {
            let result = outcome.map_err(|e| fail(format!("mutation failed: {e}")))?;
            if result.affected_nodes == *nodes && result.affected_edges == *edges {
                Ok(())
            } else {
                Err(fail(format!(
                    "affected counts mismatch: expected nodes={nodes} edges={edges}, got nodes={} edges={}",
                    result.affected_nodes, result.affected_edges
                )))
            }
        }
        MutateExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, "the mutation succeeded").map_err(fail)
        }
    }
}

async fn load(
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &LoadStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "load", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        host,
        step.ordinal,
        client.load_generated(&step.generated, &step.branch, step.mode),
    )
    .await
    .map_err(&fail)?;
    match &step.expect {
        WriteExpect::Ok => outcome.map_err(fail),
        WriteExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, "load succeeded").map_err(fail)
        }
    }
}

fn check_write_expect(
    host: &impl ExecutionHost,
    name: &str,
    expect: &WriteExpect,
    outcome: Result<ChangeOutput, String>,
) -> Result<(), String> {
    match expect {
        WriteExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| format!("`{name}` failed: {e}")),
        WriteExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, &format!("`{name}` succeeded"))
        }
    }
}

/// The merge outcome the engine would have answered, from the word on the wire.
fn merge_outcome(wire: BranchMergeOutcome) -> MergeOutcome {
    match wire {
        BranchMergeOutcome::AlreadyUpToDate => MergeOutcome::AlreadyUpToDate,
        BranchMergeOutcome::FastForward => MergeOutcome::FastForward,
        BranchMergeOutcome::Merged => MergeOutcome::Merged,
    }
}

/// A branch name as the grammar's `branch_name` spells it: bare when it is
/// an `ident`, a string literal otherwise.
fn branch_name(name: &str) -> String {
    let mut chars = name.chars();
    let ident = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ident {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// The control write as the corpus spells it, so the served path exercises
/// the statement parser a user's text meets at `/mutate`.
fn control_statement(write: &ControlWrite) -> String {
    match write {
        ControlWrite::Create {
            name,
            from: Some(parent),
            ..
        } => format!(
            "branch create {} from {}",
            branch_name(name),
            branch_name(parent)
        ),
        ControlWrite::Create { name, .. } => format!("branch create {}", branch_name(name)),
        ControlWrite::Delete { name, .. } => format!("branch delete {}", branch_name(name)),
        ControlWrite::Merge {
            source,
            into: Some(target),
            ..
        } => format!(
            "branch merge {} into {}",
            branch_name(source),
            branch_name(target)
        ),
        ControlWrite::Merge { source, .. } => format!("branch merge {}", branch_name(source)),
    }
}

async fn control(
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &ControlStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = step.name;
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let request = ChangeRequest {
        query: control_statement(&step.write),
        name: None,
        params: None,
        branch: None,
        settings: None,
    };
    let outcome = operation(
        host,
        step.ordinal,
        client.post::<_, ChangeOutput>("/mutate", &request),
    )
    .await
    .map_err(&fail)?
    .map_err(&fail)?;
    host.observe(|| format!("actual control: {:?}", outcome.as_ref().map(|o| &o.outcome)));
    match &step.write {
        ControlWrite::Create { expect, .. } | ControlWrite::Delete { expect, .. } => {
            if let (WriteExpect::Ok, Ok(output)) = (expect, &outcome) {
                let answered = matches!(
                    (&step.write, &output.outcome),
                    (
                        ControlWrite::Create { .. },
                        Some(BranchOutcomeOutput::Created { .. })
                    ) | (
                        ControlWrite::Delete { .. },
                        Some(BranchOutcomeOutput::Deleted { .. })
                    )
                );
                if !answered {
                    return Err(fail(format!(
                        "`{name}` answered without a `{name}` outcome: {:?}",
                        output.outcome
                    )));
                }
            }
            check_write_expect(host, name, expect, outcome).map_err(fail)
        }
        ControlWrite::Merge {
            expect: MergeExpect::Write(expect),
            ..
        } => check_write_expect(host, name, expect, outcome).map_err(fail),
        ControlWrite::Merge {
            expect: MergeExpect::Outcome(want),
            ..
        } => {
            let output = outcome.map_err(|e| fail(format!("`{name}` failed: {e}")))?;
            match output.outcome {
                Some(BranchOutcomeOutput::Merged { merge, .. })
                    if merge_outcome(merge) == *want =>
                {
                    Ok(())
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
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &ListStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "branch list", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(host, step.ordinal, statement(client, "branch list"))
        .await
        .map_err(&fail)?
        .map_err(&fail)?;
    check_read_expect(
        host,
        &label,
        &step.expect,
        outcome,
        "`branch list` failed",
        "`branch list` succeeded",
        binding,
    )
}

async fn show(
    host: &impl ExecutionHost,
    client: &ServerClient,
    step: &ShowStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = show_statement_name(step.id);
    let label = step_label(step.ordinal, &name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(host, step.ordinal, statement(client, &format!("{name};")))
        .await
        .map_err(&fail)?
        .map_err(&fail)?;
    check_read_expect(
        host,
        &label,
        &step.expect,
        outcome,
        &format!("`{name}` failed"),
        &format!("`{name}` succeeded"),
        binding,
    )
}

/// A read statement (`branch list`, `show …`) at the `/query` door, sent
/// bare: no name, params, branch or snapshot, as the route documents.
async fn statement(
    client: &ServerClient,
    text: &str,
) -> Result<Result<ReadOutput, String>, String> {
    let request = QueryRequest {
        query: text.to_string(),
        name: None,
        params: None,
        branch: None,
        snapshot: None,
        settings: None,
    };
    client.post("/query", &request).await
}
