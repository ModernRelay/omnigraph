//! Execution against a running `omnigraph-server`: every step kind travels
//! to the route that already serves it, as the statement text the server
//! parses for any client, and is judged from the `omnigraph-api-types`
//! answer the way the in-process path judges the engine's. Nothing the
//! server has no door for is emulated; `admit_served` refuses it by name.

use futures::FutureExt as _;
use omnigraph::db::MergeOutcome;
use omnigraph::loader::LoadMode;
use omnigraph_api_types::{
    BranchMergeOutcome, BranchOutcomeOutput, ChangeOutput, ChangeRequest, EntityKind,
    HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER, IngestRequest, QueryRequest, ReadOutput,
    SchemaOutput, SnapshotOutput,
};
use omnigraph_compiler::query::file::{BranchCreateFrom, show_statement_name};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

use crate::concurrent::MAX_HOST_RETRIES;
use crate::{
    Bindings, BranchReadStep, Case, ControlStep, ControlWrite, ExecutionHost, Fixture, Item,
    ListStep, LoadStep, MAIN_BRANCH, MergeExpect, MutateExpect, MutateStep, Observer, ParamsInput,
    PlainHost, QueryExpect, QueryStep, ReadDoor, Seed, SessionExpect, SessionKind, SessionOp,
    SessionReport, ShowStep, Step, StepFail, WriteExpect, bound_snapshot, bound_source,
    check_error_expect, check_rows_json, expectation_evidence, merge_outcome_word, operation,
    record_binding, resolve_operand, step_kind, step_label, substitute_all, substitute_bound,
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
    served_refusals(case, None)
}

/// [`admit_served`] for a measured workload: a `--- restart` after the
/// measured step at `measured` is admitted as the long-running lifecycle's
/// no-op, since the verification it precedes reads the same server; one
/// before it stays refused, as it would claim a reopened engine. A
/// `--- concurrent measured` block is admitted as the measured step itself,
/// which the host runs through [`ExecutionHost::served_block`].
pub fn admit_served_workload(case: &Case, measured: usize) -> Result<(), String> {
    served_refusals(case, Some(measured))
}

fn no_op_restart(step: &Step, measured: Option<usize>) -> bool {
    matches!(step, Step::Restart { .. }) && measured.is_some_and(|m| step.ordinal() > m)
}

fn served_refusals(case: &Case, measured: Option<usize>) -> Result<(), String> {
    let mut refused = Vec::new();
    if !case.csv_sections.is_empty() {
        refused.push("`--- csv` fixtures (engine execution only)".to_string());
    }
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
    if case.csv_slice_bytes.is_some() {
        refused.push("the `# csv_slice_bytes:` pin".to_string());
    }
    if case.overwrite_chunk_rows.is_some() {
        refused.push("the `# overwrite_chunk_rows:` pin".to_string());
    }
    if case.writer_held {
        refused.push("the `# writer:` pin".to_string());
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
            Step::Restart { .. } if no_op_restart(step, measured) => {}
            Step::Concurrent(block) if block.measured && measured == Some(ordinal) => {}
            Step::Restart { .. }
            | Step::Settings(_)
            | Step::Concurrent(_)
            | Step::ApplySchema(_) => {
                refused.push(format!("step {ordinal} ({})", step_kind(step)));
            }
            Step::Query(query) => {
                if query.plan.is_some() {
                    refused.push(format!("step {ordinal} (expect plan)"));
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
            "unsupported_environment: omnigraph-server has no door for {}",
            refused.join(", ")
        ))
    }
}

/// Executes the case against the server: the schema check and the seed
/// when the case carries a fixture, then every step through its route.
pub fn execute_steps_served<'a, H: ExecutionHost, O: Observer>(
    case: &'a Case,
    target: &'a ServerTarget,
    host: &'a H,
    observer: &'a O,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_served_inner(case, target, host, observer, None).boxed()
}

/// [`execute_steps_served`] for a measured workload, admitted by
/// [`admit_served_workload`]: a restart after the measured step runs as
/// the lifecycle's no-op and is reported like every other step.
pub fn execute_measured_served<'a, H: ExecutionHost, O: Observer>(
    case: &'a Case,
    target: &'a ServerTarget,
    host: &'a H,
    observer: &'a O,
    measured: usize,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_served_inner(case, target, host, observer, Some(measured)).boxed()
}

async fn execute_served_inner<H: ExecutionHost, O: Observer>(
    case: &Case,
    target: &ServerTarget,
    host: &H,
    observer: &O,
    measured: Option<usize>,
) -> Result<(), String> {
    host.admit_case(case)?;
    served_refusals(case, measured)?;
    let client = ServerClient::new(target, Duration::from_millis(case.runner.timeout_ms))?;
    observer.observe(|| format!("lifetime: served by {} graph {}", target.url, target.graph));
    if let Some(fixture) = &case.fixture {
        check_schema(&client, &fixture.schema).await?;
        seed(&client, fixture).await?;
    }
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
                        "operation: ordinal={ordinal} line={:?} binding={binding:?} served expected={step:?}",
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
                    Step::Restart { .. } if no_op_restart(step, measured) => {
                        observer.observe(|| {
                            "restart: the server's lifecycle is long-running; no door opened"
                                .to_string()
                        });
                        Ok(())
                    }
                    Step::Concurrent(block) if block.measured && measured == Some(ordinal) => {
                        host.served_block(target, case, block).await
                    }
                    Step::Settings(_)
                    | Step::Restart { .. }
                    | Step::Concurrent(_)
                    | Step::ApplySchema(_) => Err(StepFail::new(
                        step_label(ordinal, step_kind(step), binding),
                        "unsupported_environment: refused by served admission".into(),
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

/// The most bytes one answer may carry before the runner stops reading it:
/// a bounded read, so a large row set or a runaway server fails the step
/// instead of the process.
const RESPONSE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes of an undecodable response body a diagnostic repeats.
const ECHO_LIMIT_BYTES: usize = 4 * 1024;

/// The requests one served run sends: every one under `/graphs/{id}`, with
/// the contract header and, when given, the bearer token.
struct ServerClient {
    graph_base: String,
    token: Option<String>,
    http: reqwest::Client,
}

/// The server's error body: its `error` line, a read-set conflict it names
/// and what its `disposition` says the request did. Lenient, as the server
/// may be another commit: a field it lacks is absent, one it adds ignored.
#[derive(Deserialize)]
struct ErrorDetail {
    error: String,
    #[serde(default)]
    read_set_conflict: Option<Value>,
    #[serde(default)]
    disposition: Option<Value>,
}

/// A request the server answered with an error status.
struct Refusal {
    status: reqwest::StatusCode,
    error: String,
    /// HTTP 409 naming a read-set conflict, with no disposition or one of
    /// `effects: none`: the request applied nothing and may be run again.
    lost_read_set: bool,
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
        Ok(self
            .send_typed(builder)
            .await?
            .map_err(|refusal| refusal.error))
    }

    /// [`send`](Self::send) with the refusal's status and detail kept.
    async fn send_typed<Res: DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<Result<Res, Refusal>, String> {
        let builder = builder.header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT);
        let builder = match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        };
        let mut response = builder
            .send()
            .await
            .map_err(|e| format!("server request failed: {e}"))?;
        let status = response.status();
        if let Some(length) = response.content_length()
            && length > RESPONSE_LIMIT_BYTES
        {
            return Err(format!(
                "server answered {status} with {length} bytes, over the {RESPONSE_LIMIT_BYTES}-byte response bound"
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| format!("server response failed: {e}"))?
        {
            if body.len() + chunk.len() > RESPONSE_LIMIT_BYTES as usize {
                return Err(format!(
                    "server answered {status} with more than {RESPONSE_LIMIT_BYTES} bytes, over the response bound"
                ));
            }
            body.extend_from_slice(&chunk);
        }
        if status.is_success() {
            return serde_json::from_slice(&body).map(Ok).map_err(|e| {
                format!(
                    "server answered {status} with a body this runner cannot decode: {e}: {}",
                    self.echo(&body)
                )
            });
        }
        let detail: ErrorDetail = serde_json::from_slice(&body).map_err(|e| {
            format!(
                "server answered {status} without an error body: {e}: {}",
                self.echo(&body)
            )
        })?;
        let applied_nothing = detail
            .disposition
            .as_ref()
            .is_none_or(|disposition| disposition["effects"] == "none");
        Ok(Err(Refusal {
            status,
            error: detail.error,
            lost_read_set: status == reqwest::StatusCode::CONFLICT
                && detail.read_set_conflict.is_some()
                && applied_nothing,
        }))
    }

    /// An undecodable body as diagnostic text: the bearer token's raw and
    /// JSON-escaped spellings redacted first, then the text cut to its bound.
    fn echo(&self, body: &[u8]) -> String {
        let mut text = String::from_utf8_lossy(body).into_owned();
        if let Some(token) = self.token.as_deref().filter(|t| !t.is_empty()) {
            let json = serde_json::to_string(token).expect("string encoding is infallible");
            for spelling in [&json[1..json.len() - 1], token] {
                text = text.replace(spelling, "<redacted>");
            }
        }
        if text.len() <= ECHO_LIMIT_BYTES {
            return text;
        }
        let mut end = ECHO_LIMIT_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}… ({} bytes, truncated)", &text[..end], body.len())
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

    async fn post_typed<Req: Serialize, Res: DeserializeOwned>(
        &self,
        route: &str,
        body: &Req,
    ) -> Result<Result<Res, Refusal>, String> {
        self.send_typed(
            self.http
                .post(format!("{}{route}", self.graph_base))
                .json(body),
        )
        .await
    }

    /// Resolve after row materialization so the operation timer includes decoding.
    async fn read(&self, request: &QueryRequest) -> Result<Result<Vec<Value>, String>, String> {
        match self.post::<_, ReadOutput>("/query", request).await? {
            Ok(output) => rows_of(&output).map(Ok),
            Err(error) => Ok(Err(error)),
        }
    }

    async fn get<Res: DeserializeOwned>(&self, route: &str) -> Result<Result<Res, String>, String> {
        self.send(self.http.get(format!("{}{route}", self.graph_base)))
            .await
    }

    /// The branch's snapshot: every table with the rows its manifest counts.
    async fn snapshot(&self, branch: &str) -> Result<Result<SnapshotOutput, String>, String> {
        self.send(
            self.http
                .get(format!("{}/snapshot", self.graph_base))
                .query(&[("branch", branch)]),
        )
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

/// The fixture as `seed_case` applies it to a session: the seed through
/// `/load` (inline rows overwrite `main`, generated batches append), then
/// each declared branch as a `branch create` at `/mutate` plus its recipe.
async fn seed(client: &ServerClient, fixture: &Fixture) -> Result<(), String> {
    match &fixture.seed {
        Seed::Inline(text) if text.trim().is_empty() => Ok(()),
        Seed::Inline(text) => client
            .load(MAIN_BRANCH, LoadMode::Overwrite, text.clone())
            .await
            .map_err(|error| format!("seed load failed: {error}")),
        Seed::Generated(generated) => {
            client
                .load_generated(generated, MAIN_BRANCH, LoadMode::Insert)
                .await
        }
    }?;
    let Some(branches) = &fixture.branches else {
        return Ok(());
    };
    for name in branches.names() {
        let request = ChangeRequest {
            query: format!(
                "branch create {} from {}",
                branch_name(&name),
                branch_name(branches.from())
            ),
            name: None,
            params: None,
            branch: None,
            settings: None,
        };
        client
            .post::<_, ChangeOutput>("/mutate", &request)
            .await?
            .map_err(|error| {
                format!(
                    "branches fixture: branch create {name} from {}: {error}",
                    branches.from()
                )
            })?;
        if let Some(generated) = branches.recipe(&name)? {
            client
                .load_generated(&generated, &name, LoadMode::Insert)
                .await
                .map_err(|error| format!("branches fixture: load {name}: {error}"))?;
        }
    }
    Ok(())
}

/// Whether the server at `url` answers `GET /readyz` with success and no
/// graph still loading; a server that cannot be reached yet is not ready.
pub async fn server_ready(url: &str, timeout: Duration) -> Result<bool, String> {
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(timeout)
        .build()
        .map_err(|e| format!("server client: {e}"))?;
    let request = http.get(format!("{}/readyz", url.trim_end_matches('/')));
    let Ok(response) = request.send().await else {
        return Ok(false);
    };
    if !response.status().is_success() {
        return Ok(false);
    }
    Ok(response
        .json::<Value>()
        .await
        .is_ok_and(|body| body["loading_graph_count"] == 0))
}

/// The commit the server at `url` says it was built from, at the
/// unauthenticated `GET /healthz`; `None` for a build that names none. The
/// answer is read leniently, as the server may be another commit than this
/// harness: only its `source_version` is looked at.
pub async fn server_source_commit(url: &str, timeout: Duration) -> Result<Option<String>, String> {
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(timeout)
        .build()
        .map_err(|e| format!("server client: {e}"))?;
    let health: Value = http
        .get(format!("{}/healthz", url.trim_end_matches('/')))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| format!("GET /healthz failed: {e}"))?
        .json()
        .await
        .map_err(|e| format!("GET /healthz answered a body this runner cannot decode: {e}"))?;
    Ok(health["source_version"].as_str().map(str::to_string))
}

/// A table of a branch and the rows a provisioned graph holds in it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SeededTable {
    pub branch: String,
    pub table: String,
    pub rows: u64,
}

/// Makes an empty served graph the dataset's own, for a host that starts its
/// server per run: `dataset` executed as [`execute_steps_served`] executes a
/// case (the schema the graph already carries held against the fixture's, the
/// seed, the declared branches, then every step with its expectation), then
/// every node table of `tables` counted through `/query` and every edge table
/// of `edge_tables` read from its branch's snapshot, each held against its
/// rows. A dataset without a fixture is refused: it would provision nothing.
pub async fn provision_served(
    target: &ServerTarget,
    dataset: &Case,
    tables: &[SeededTable],
    edge_tables: &[SeededTable],
) -> Result<(), String> {
    if dataset.fixture.is_none() {
        return Err("a dataset provisions a graph from its `--- schema` and seed".into());
    }
    execute_served_inner(dataset, target, &PlainHost, &(), None).await?;
    let client = ServerClient::new(target, Duration::from_millis(dataset.runner.timeout_ms))?;
    for seeded in tables {
        let SeededTable {
            branch,
            table,
            rows,
        } = seeded;
        let request = QueryRequest {
            query: format!(
                "query seeded_rows() {{ match {{ $r: {table} }} return {{ count($r) as n }} }}"
            ),
            name: Some("seeded_rows".into()),
            params: None,
            branch: Some(branch.clone()),
            snapshot: None,
            settings: None,
        };
        let answer = client
            .read(&request)
            .await?
            .map_err(|error| format!("count of {table} on {branch} failed: {error}"))?;
        let counted = match answer.as_slice() {
            [] => Some(0),
            [row] => row.get("n").and_then(Value::as_u64),
            _ => None,
        };
        if counted != Some(*rows) {
            return Err(format!(
                "the seeded graph holds {} rows of {table} on {branch} and the fixture holds {rows}",
                counted.map_or_else(|| format!("{answer:?}"), |count| count.to_string())
            ));
        }
    }
    let mut snapshots = std::collections::BTreeMap::new();
    for seeded in edge_tables {
        let SeededTable {
            branch,
            table,
            rows,
        } = seeded;
        if !snapshots.contains_key(branch) {
            let snapshot = client
                .snapshot(branch)
                .await?
                .map_err(|error| format!("snapshot of {branch} failed: {error}"))?;
            snapshots.insert(branch, snapshot);
        }
        let counted = snapshots[branch]
            .datasets
            .iter()
            .find(|dataset| dataset.entity_kind == EntityKind::Edge && dataset.type_name == *table)
            .map_or(0, |dataset| dataset.entity_count);
        if counted != *rows {
            return Err(format!(
                "the seeded graph holds {counted} rows of edge {table} on {branch} and the fixture holds {rows}"
            ));
        }
    }
    Ok(())
}

/// One session of a block against a server: a client of its own, so every
/// worker holds its own connections and its own actor's token.
pub struct ServedSession {
    client: ServerClient,
}

impl ServedSession {
    /// `timeout` bounds every request, as the step client's does.
    pub fn open(target: &ServerTarget, timeout: Duration) -> Result<Self, String> {
        ServerClient::new(target, timeout).map(|client| Self { client })
    }

    /// One run of the session's statement; `seq` is the ordinal of this run on
    /// the worker. An HTTP 409 that names a read-set conflict and applied
    /// nothing is run again, up to [`MAX_HOST_RETRIES`] times, each counted,
    /// and the session's expect holds against the final answer. A conflict
    /// whose disposition says an earlier publication of the request applied
    /// is that final answer: sent again, the publication would apply twice.
    /// The server's own reprepares behind an acknowledged write are not on
    /// the wire, so the count is the host's runs alone. A request refused at
    /// a workload limit (HTTP 429) fails the run: waiting a limit out would
    /// put the wait inside a service time.
    pub async fn run(&self, op: &SessionOp, seq: u64) -> Result<SessionReport, String> {
        let what = op.kind.name();
        let mut host_retries = 0u32;
        loop {
            let outcome = self.attempt(op, seq).await?;
            if let Err(refusal) = &outcome {
                if refusal.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(format!(
                        "the server refused the {what} at a workload limit (HTTP 429), which a block never waits out: {}",
                        refusal.error
                    ));
                }
                if refusal.lost_read_set && host_retries < MAX_HOST_RETRIES {
                    host_retries += 1;
                    continue;
                }
            }
            let report = SessionReport {
                conflicts: host_retries,
            };
            let outcome = outcome.map_err(|refusal| refusal.error);
            return match &op.expect {
                SessionExpect::Ok => outcome
                    .map(|()| report)
                    .map_err(|e| format!("{what} failed: {e}")),
                SessionExpect::Error { needle } => {
                    check_error_expect(&(), needle, outcome, &format!("the {what} succeeded"))
                        .map(|()| report)
                }
            };
        }
    }

    /// One attempt: `Err` is the request not reaching a verdict or the
    /// harness's own refusal, `Ok` the server's answer.
    async fn attempt(&self, op: &SessionOp, seq: u64) -> Result<Result<(), Refusal>, String> {
        let change = |query: String, name: Option<String>, branch: Option<String>| ChangeRequest {
            query,
            name,
            params: None,
            branch,
            settings: None,
        };
        match &op.kind {
            SessionKind::Query => {
                let request = QueryRequest {
                    query: op.source.clone(),
                    name: Some(op.name.clone()),
                    params: None,
                    branch: Some(op.branch.clone()),
                    snapshot: None,
                    settings: None,
                };
                match self
                    .client
                    .post_typed::<_, ReadOutput>("/query", &request)
                    .await?
                {
                    Ok(output) => rows_of(&output).map(|_| Ok(())),
                    Err(refusal) => Ok(Err(refusal)),
                }
            }
            SessionKind::Mutation => {
                let request = change(
                    op.source.clone(),
                    Some(op.name.clone()),
                    Some(op.branch.clone()),
                );
                Ok(self
                    .client
                    .post_typed::<_, ChangeOutput>("/mutate", &request)
                    .await?
                    .map(|_| ()))
            }
            SessionKind::Load(recipe) => {
                let request = IngestRequest {
                    branch: Some(op.branch.clone()),
                    from: None,
                    mode: Some(recipe.mode),
                    data: recipe.generated.dealt_row(op.dealt_index(seq))?,
                };
                Ok(self
                    .client
                    .post_typed::<_, Value>("/load", &request)
                    .await?
                    .map(|_| ()))
            }
            SessionKind::Merge(merge) => {
                let statement =
                    merge_statement(&merge.source, merge.into.as_deref(), merge.as_commit);
                let request = change(statement, None, None);
                Ok(self
                    .client
                    .post_typed::<_, ChangeOutput>("/mutate", &request)
                    .await?
                    .map(|_| ()))
            }
        }
    }
}

impl ReadDoor for ServedSession {
    fn rows<'a>(
        &'a self,
        branch: &'a str,
        source: &'a str,
        name: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Vec<Value>, String>> {
        async move {
            let request = QueryRequest {
                query: source.to_string(),
                name: Some(name.to_string()),
                params: None,
                branch: Some(branch.to_string()),
                snapshot: None,
                settings: None,
            };
            self.client.read(&request).await?
        }
        .boxed()
    }
}

/// The `--- params` as they go on the wire: the JSON the case spells,
/// bindings substituted, or a recipe's generated values; the server holds them
/// against the declaration, so a parameter error is the server's answer, never
/// a local verdict. A generated value travels in the request body, under the
/// server's request limits.
pub(crate) fn wire_params(
    params: Option<&ParamsInput>,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<Option<Value>, String> {
    params
        .map(|params| match params {
            ParamsInput::Literal(raw) => {
                serde_json::from_str::<Value>(&substitute_all(raw, binding, bindings))
                    .map_err(|e| format!("params are not valid JSON: {e}"))
            }
            ParamsInput::Generated(generated) => generated.generate(),
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
pub(crate) fn check_read_expect(
    observer: &impl Observer,
    label: &str,
    expect: &QueryExpect,
    outcome: Result<Vec<Value>, String>,
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
            let actual = outcome.map_err(|e| fail(format!("{failed}: {e}")))?;
            observer.observe(|| {
                format!(
                    "shape: {} line(s) not judged under a server target",
                    shape.lines.len()
                )
            });
            check_rows_json(observer, label, &actual, *ordered, body_raw, *span, binding)
        }
        QueryExpect::Error { needle } => {
            check_error_expect(observer, needle, outcome, succeeded).map_err(fail)
        }
    }
}

async fn query(
    observer: &impl Observer,
    client: &ServerClient,
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
                QueryExpect::Rows { .. } => Err(fail(e)),
            };
        }
    };
    let snapshot = bound_snapshot(step, bindings).map_err(&fail)?;
    let request = QueryRequest {
        query: step.source.clone(),
        name: Some(step.name.clone()),
        params,
        branch: snapshot.is_none().then(|| step.branch.clone()),
        snapshot,
        settings: None,
    };
    let outcome = operation(observer, step.ordinal, client.read(&request))
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
    client: &ServerClient,
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
    let request = ChangeRequest {
        query: source.into_owned(),
        name: Some(step.name.clone()),
        params,
        branch: step.branch_spelled.then(|| step.branch.clone()),
        settings: None,
    };
    let outcome = operation(
        observer,
        step.ordinal,
        client.post::<_, ChangeOutput>("/mutate", &request),
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
    client: &ServerClient,
    step: &LoadStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "load", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        observer,
        step.ordinal,
        client.load_generated(&step.generated, &step.branch, step.mode),
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
        string_literal(name)
    }
}

/// `text` as the grammar's `string_lit`.
fn string_literal(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A `branch merge` as the corpus spells it.
fn merge_statement(source: &str, into: Option<&str>, as_commit: bool) -> String {
    let mut statement = format!("branch merge {}", branch_name(source));
    if let Some(target) = into {
        statement.push_str(&format!(" into {}", branch_name(target)));
    }
    if as_commit {
        statement.push_str(" as commit");
    }
    statement
}

/// The control write as the corpus spells it, a bound commit id resolved, so
/// the served path exercises the statement parser a user's text meets at
/// `/mutate`.
pub(crate) fn control_statement(
    write: &ControlWrite,
    bindings: &Bindings,
) -> Result<String, String> {
    Ok(match write {
        ControlWrite::Create {
            name,
            from: Some(BranchCreateFrom::Branch(parent)),
            ..
        } => format!(
            "branch create {} from {}",
            branch_name(name),
            branch_name(parent)
        ),
        ControlWrite::Create {
            name,
            from: Some(BranchCreateFrom::Commit(commit)),
            ..
        } => format!(
            "branch create {} from commit {}",
            branch_name(name),
            string_literal(&resolve_operand(commit, bindings)?)
        ),
        ControlWrite::Create { name, .. } => format!("branch create {}", branch_name(name)),
        ControlWrite::Delete { name, .. } => format!("branch delete {}", branch_name(name)),
        ControlWrite::Merge {
            source,
            into,
            as_commit,
            ..
        } => merge_statement(source, into.as_deref(), *as_commit),
        ControlWrite::Publish {
            graph_commit_id, ..
        } => format!(
            "commit publish {}",
            string_literal(&resolve_operand(graph_commit_id, bindings)?)
        ),
        ControlWrite::Drop {
            graph_commit_id, ..
        } => format!(
            "commit drop {}",
            string_literal(&resolve_operand(graph_commit_id, bindings)?)
        ),
        ControlWrite::Load { stmt, .. } => {
            if !stmt.source.contains("://") {
                return Err(format!(
                    "`load csv \"{}\"` names a `--- csv` section, which only the engine environment materializes; a served load names an absolute URI the server may read",
                    stmt.source
                ));
            }
            stmt.render()
        }
    })
}

async fn control(
    observer: &impl Observer,
    client: &ServerClient,
    step: &ControlStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<Option<String>, StepFail> {
    let name = step.name;
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let request = ChangeRequest {
        query: control_statement(&step.write, bindings).map_err(&fail)?,
        name: None,
        params: None,
        branch: match &step.write {
            ControlWrite::Load { branch, .. } => branch.clone(),
            _ => None,
        },
        settings: None,
    };
    let outcome = operation(
        observer,
        step.ordinal,
        client.post::<_, ChangeOutput>("/mutate", &request),
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
    client: &ServerClient,
    step: &ListStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "branch list", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(observer, step.ordinal, statement(client, "branch list"))
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
    client: &ServerClient,
    step: &ShowStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = show_statement_name(step.id);
    let label = step_label(step.ordinal, &name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = operation(
        observer,
        step.ordinal,
        statement(client, &format!("{name};")),
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

/// A branch control read at the `/query` door: the statement sent with its
/// bound names replaced, its rows held against the expect with the same
/// names replaced in the expected rows.
async fn branch_read(
    observer: &impl Observer,
    client: &ServerClient,
    step: &BranchReadStep,
    binding: Option<(&str, &str)>,
    bindings: &Bindings,
) -> Result<(), StepFail> {
    let name = step.read.statement_name();
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let text = substitute_bound(&step.source, bindings);
    let outcome = operation(observer, step.ordinal, statement(client, &text))
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
    }
}

/// A read statement (`branch list`, `show …`) at the `/query` door, sent
/// bare: no name, params, branch or snapshot, as the route documents.
async fn statement(
    client: &ServerClient,
    text: &str,
) -> Result<Result<Vec<Value>, String>, String> {
    let request = QueryRequest {
        query: text.to_string(),
        name: None,
        params: None,
        branch: None,
        snapshot: None,
        settings: None,
    };
    client.read(&request).await
}
