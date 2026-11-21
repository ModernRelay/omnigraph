//! The measured block against a server the run starts per repetition
//! (`reset: rebuild`): the worker's run over HTTP against a stand-in server,
//! the route's planning, options and record, and the server's start and stop.
use super::*;
use crate::case::{NetworkPosition, ResetMode};
use crate::gqt_evidence::{OWNED_SERVER_GRAPH, OwnedServerInput};
use crate::gqt_protocol::{PreparationProofV2, RepetitionInputV2};
use crate::gqt_runner::{
    RunOptions, ServerProduct, execute_owned_server_rep_signaled, validate_run_options,
};
use omnigraph_gqt_core::{SeededTable, ServerTarget};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SERVED_WORKLOAD: &str = r#"# issue: none
# notes: Two writers append dealt rows beside one reader, against a server or embedded.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
  - target: omnigraph-server
    storage: local-filesystem

--- bench
warmup_seconds: 0
duration_seconds: 2

--- concurrent measured
writer x2: load generate: v1 seed: 42 mode: append
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
reader: query seeded_row() { match { $c: Chunk { slug: "seed-07" } } return { $c.n } }
--- expect
writer: ok
reader: ok

--- query branch: main
query seeded_row() { match { $c: Chunk { slug: "seed-07" } } return { $c.slug, $c.n } }
--- expect unordered
{"c.slug":"seed-07","c.n":7}
--- expect shape
c.slug: String
c.n: I64
"#;

/// The schema of the sixteen-row fixture, as a server reports it.
const SCHEMA: &str = "node Chunk { slug: String @key n: I64 }\n";

/// What the stand-in sleeps before it answers, so a closed loop against it
/// stays inside the rows a recipe deals.
const SERVICE_TIME: Duration = Duration::from_millis(1);

/// The place of the steps' token in a repetition's tokens; the block's
/// sessions follow it in declaration order.
const STEPS: usize = 0;

fn served() -> GqtEnvironment {
    crate::catalog::served_environment(apfs_backend(), NetworkPosition::SameHost)
}

/// The `writers` scenario over `workload` as a server point under `reset`.
fn server_point(workload: &str, reset: Option<ResetMode>) -> Result<PlannedGqt, String> {
    let directory = tempfile::tempdir().unwrap();
    catalog_of(directory.path(), workload)
        .plan_with("writers", &served(), reset)
        .map_err(|diagnostics| format!("{diagnostics:?}"))
}

fn rebuild_point() -> PlannedGqt {
    server_point(SERVED_WORKLOAD, Some(ResetMode::Rebuild)).unwrap()
}

/// One token for the steps and one per session of the measured block.
fn tokens(sessions: usize) -> Vec<String> {
    (0..=sessions)
        .map(|place| format!("owned-server-test-token-{place:02}"))
        .collect()
}

/// `plan` bound, and the input a parent would send its worker for a server
/// at `url` that accepts `tokens`.
fn input(plan: &PlannedGqt, url: &str, tokens: &[String]) -> (BoundGqt, OwnedServerInput) {
    let bound = plan
        .bind(
            &"c".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
        )
        .unwrap();
    let input = OwnedServerInput {
        target: ServerTarget {
            url: url.into(),
            graph: OWNED_SERVER_GRAPH.into(),
            token: Some(tokens[STEPS].clone()),
        },
        session_tokens: tokens[1..].to_vec(),
        server_sha256: "d".repeat(64),
        tables: vec![SeededTable {
            branch: "main".into(),
            table: "Chunk".into(),
            rows: 16,
        }],
        edge_tables: Vec::new(),
    };
    RepetitionInputV2::OwnedServer {
        input: Box::new(input.clone()),
    }
    .validate(&bound)
    .unwrap();
    (bound, input)
}

fn proof(input: &OwnedServerInput) -> PreparationProofV2 {
    PreparationProofV2::OwnedServer {
        server_sha256: input.server_sha256.clone(),
    }
}

struct Request {
    method: String,
    path: String,
    bearer: Option<String>,
    body: Value,
}

/// A load the stand-in answers with `status`, `error` and the further fields
/// of `detail` once `token` has had `after` loads accepted.
struct Refusal {
    token: usize,
    after: u64,
    status: &'static str,
    error: String,
    detail: Value,
}

/// What a stand-in server holds: the tokens it accepts, the rows it was
/// loaded per type, what each token asked, and what a test scripts.
#[derive(Default)]
struct Graph {
    tokens: Vec<String>,
    rows: BTreeMap<String, Vec<Value>>,
    loads: BTreeMap<usize, u64>,
    queries: BTreeMap<usize, u64>,
    /// What each token sent to `/mutate`; the stand-in accepts every one and
    /// applies none.
    changes: BTreeMap<usize, Vec<Value>>,
    unauthorized: u64,
    /// Loads of a token still to answer 409 with a read-set conflict.
    conflicts: BTreeMap<usize, u32>,
    /// What a scripted conflict's `disposition` says the request did; a
    /// conflict carries no disposition while this is unset.
    conflict_effects: Option<&'static str>,
    refusal: Option<Refusal>,
    /// The loads the scripted refusal answered.
    refused: u64,
    /// The graphs `/readyz` reports as still loading.
    loading: u64,
    /// Whether `/readyz` answers 503, as a server does while it drains.
    unready: bool,
    /// The commit `/healthz` names as the server's build, when it names one.
    source_version: Option<String>,
    /// The rows a snapshot counts per edge type.
    edges: BTreeMap<String, u64>,
}

impl Graph {
    fn answer(&mut self, request: &Request) -> (&'static str, Value) {
        let path = request.path.split('?').next().unwrap_or_default();
        match (request.method.as_str(), path) {
            ("GET", "/readyz") => {
                let status = if self.unready {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                return (status, json!({"loading_graph_count": self.loading}));
            }
            ("GET", "/healthz") => {
                let mut health = json!({"status": "ok", "version": "0.0.0"});
                if let Some(commit) = &self.source_version {
                    health["source_version"] = json!(commit);
                }
                return ("200 OK", health);
            }
            _ => {}
        }
        let known = |bearer: &String| self.tokens.iter().position(|token| token == bearer);
        let Some(who) = request.bearer.as_ref().and_then(known) else {
            self.unauthorized += 1;
            return ("401 Unauthorized", json!({"error": "unknown bearer token"}));
        };
        match (request.method.as_str(), path) {
            ("GET", "/graphs/bench/schema") => ("200 OK", json!({"schema_source": SCHEMA})),
            ("GET", "/graphs/bench/snapshot") => {
                let dataset = |kind: &str, name: &String, rows: u64| {
                    json!({
                        "entity_kind": kind,
                        "type_name": name,
                        "dataset_path": format!("{kind}s/{name}"),
                        "published_dataset_version": 1,
                        "native_dataset_branch": null,
                        "entity_count": rows,
                    })
                };
                let nodes = self.rows.iter();
                let datasets: Vec<Value> = nodes
                    .map(|(name, rows)| dataset("node", name, rows.len() as u64))
                    .chain(
                        self.edges
                            .iter()
                            .map(|(name, rows)| dataset("edge", name, *rows)),
                    )
                    .collect();
                (
                    "200 OK",
                    json!({
                        "graph_branch": "main",
                        "graph_manifest_version": 1,
                        "internal_schema_version": 1,
                        "datasets": datasets,
                    }),
                )
            }
            ("POST", "/graphs/bench/load") => self.load(who, &request.body),
            ("POST", "/graphs/bench/query") => {
                *self.queries.entry(who).or_default() += 1;
                match select(
                    &self.rows,
                    request.body["query"].as_str().unwrap_or_default(),
                ) {
                    Ok(rows) => (
                        "200 OK",
                        json!({
                            "query_name": request.body["name"],
                            "target": {"branch": request.body["branch"], "snapshot": null},
                            "row_count": rows.len(),
                            "rows": rows,
                        }),
                    ),
                    Err(error) => ("400 Bad Request", json!({"error": error})),
                }
            }
            ("POST", "/graphs/bench/mutate") => {
                let sent = &request.body;
                self.changes.entry(who).or_default().push(sent.clone());
                let (name, nodes) = match sent["name"].as_str() {
                    Some(name) => (name, 1),
                    None => ("branch merge", 0),
                };
                (
                    "200 OK",
                    json!({
                        "branch": "main",
                        "query_name": name,
                        "affected_nodes": nodes,
                        "affected_edges": 0,
                        "actor_id": null,
                        "commit": null,
                    }),
                )
            }
            (method, path) => (
                "404 Not Found",
                json!({"error": format!("the stand-in has no route {method} {path}")}),
            ),
        }
    }

    fn load(&mut self, who: usize, body: &Value) -> (&'static str, Value) {
        if let Some(left) = self.conflicts.get_mut(&who).filter(|left| **left > 0) {
            *left -= 1;
            let mut conflict = json!({
                "error": "the write's read set changed before its effects",
                "read_set_conflict": {"table": "node:Chunk"},
            });
            if let Some(effects) = self.conflict_effects {
                conflict["disposition"] = json!({"effects": effects, "action": "refresh"});
            }
            return ("409 Conflict", conflict);
        }
        let accepted = self.loads.entry(who).or_default();
        if let Some(refusal) = &self.refusal
            && refusal.token == who
            && *accepted >= refusal.after
        {
            self.refused += 1;
            let mut answer = refusal.detail.as_object().cloned().unwrap_or_default();
            answer.insert("error".into(), refusal.error.clone().into());
            return (refusal.status, answer.into());
        }
        *accepted += 1;
        for line in body["data"].as_str().unwrap_or_default().lines() {
            let Ok(row) = serde_json::from_str::<Value>(line) else {
                return ("400 Bad Request", json!({"error": "a row is not JSON"}));
            };
            let table = row["type"].as_str().unwrap_or_default().to_string();
            self.rows
                .entry(table)
                .or_default()
                .push(row["data"].clone());
        }
        ("200 OK", json!({"loaded": true}))
    }
}

/// The rows of `match { $v: Table [{ column: literal }] } return { ... }`,
/// the one shape the provisioning count, the readback and these workloads
/// ask: `count($v) as n`, or `$v.column` projections named `v.column`.
fn select(rows: &BTreeMap<String, Vec<Value>>, source: &str) -> Result<Vec<Value>, String> {
    let shape = || format!("the stand-in does not answer `{source}`");
    let (_, rest) = source.split_once("match {").ok_or_else(shape)?;
    let (pattern, returned) = rest.split_once("return {").ok_or_else(shape)?;
    let pattern = pattern.trim().strip_suffix('}').ok_or_else(shape)?;
    let (variable, node) = pattern.split_once(':').ok_or_else(shape)?;
    let variable = variable.trim().trim_start_matches('$');
    let (table, filter) = match node.split_once('{') {
        Some((table, filter)) => (table, Some(filter.trim().trim_end_matches('}'))),
        None => (node, None),
    };
    let filter = filter
        .map(|filter| {
            let (column, literal) = filter.split_once(':').ok_or_else(shape)?;
            let literal: Value = serde_json::from_str(literal.trim()).map_err(|_| shape())?;
            Ok::<_, String>((column.trim().to_string(), literal))
        })
        .transpose()?;
    let matched: Vec<&Value> = rows
        .get(table.trim())
        .into_iter()
        .flatten()
        .filter(|row| {
            filter
                .as_ref()
                .is_none_or(|(column, literal)| &row[column] == literal)
        })
        .collect();
    let returned = returned.trim_end_matches(|c: char| c == '}' || c.is_whitespace());
    if returned.trim().starts_with("count(") {
        return Ok(vec![json!({"n": matched.len()})]);
    }
    let reference = format!("${variable}.");
    let columns = returned
        .split(',')
        .map(|projected| projected.trim().strip_prefix(&reference).ok_or_else(shape))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matched
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| (format!("{variable}.{column}"), row[*column].clone()))
                .collect::<serde_json::Map<_, _>>()
                .into()
        })
        .collect())
}

/// The stand-in listening on a loopback port: one thread per connection, each
/// answering requests until its client closes.
struct GraphServer {
    url: String,
    graph: Arc<Mutex<Graph>>,
    stop: Arc<AtomicBool>,
    accept: Option<std::thread::JoinHandle<()>>,
}

impl GraphServer {
    fn start(tokens: &[String]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let graph = Arc::new(Mutex::new(Graph {
            tokens: tokens.to_vec(),
            ..Graph::default()
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (served, stopped) = (Arc::clone(&graph), Arc::clone(&stop));
        let accept = std::thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let graph = Arc::clone(&served);
                        std::thread::spawn(move || serve(stream, &graph));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("the stand-in's listener failed: {error}"),
                }
            }
        });
        Self {
            url,
            graph,
            stop,
            accept: Some(accept),
        }
    }

    fn graph(&self) -> std::sync::MutexGuard<'_, Graph> {
        self.graph.lock().unwrap()
    }

    /// The address a server prints after its listen prefix.
    fn address(&self) -> &str {
        self.url.trim_start_matches("http://")
    }
}

impl Drop for GraphServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            let joined = accept.join();
            if !std::thread::panicking() {
                joined.expect("the stand-in's listener panicked");
            }
        }
    }
}

fn serve(stream: TcpStream, graph: &Mutex<Graph>) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let Ok(mut answers) = stream.try_clone() else {
        return;
    };
    let mut requests = BufReader::new(stream);
    while let Some(request) = read_request(&mut requests) {
        std::thread::sleep(SERVICE_TIME);
        let (status, body) = graph.lock().unwrap().answer(&request);
        let body = serde_json::to_vec(&body).unwrap();
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        if answers.write_all(head.as_bytes()).is_err() || answers.write_all(&body).is_err() {
            return;
        }
    }
}

/// The next request of a kept-alive connection; `None` once the client is
/// done with it.
fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next()?.to_owned(), parts.next()?.to_owned());
    let (mut length, mut bearer) = (0usize, None);
    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            length = value.parse().ok()?;
        } else if name.eq_ignore_ascii_case("authorization") {
            bearer = value.strip_prefix("Bearer ").map(str::to_owned);
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).ok()?
    };
    Some(Request {
        method,
        path,
        bearer,
        body,
    })
}

type Repetition = (
    GraphServer,
    BoundGqt,
    OwnedServerInput,
    RunnerResult<GqtRepObservation>,
    Signals,
);

/// One repetition of `plan`, a block of `sessions` sessions, against a fresh
/// stand-in, scripted by `script` before the worker's run starts.
async fn repetition_of(
    plan: &PlannedGqt,
    sessions: usize,
    script: impl FnOnce(&mut Graph),
) -> Repetition {
    let tokens = tokens(sessions);
    let server = GraphServer::start(&tokens);
    script(&mut server.graph());
    let (bound, input) = input(plan, &server.url, &tokens);
    let mut signals = Signals::default();
    let outcome = execute_owned_server_rep_signaled(1, &bound, &input, &mut signals).await;
    (server, bound, input, outcome, signals)
}

/// [`repetition_of`] the rebuild point of the two-writer, one-reader block.
async fn repetition(script: impl FnOnce(&mut Graph)) -> Repetition {
    repetition_of(&rebuild_point(), 3, script).await
}

/// A mutation that leaves the seeded row it names as the fixture wrote it.
const TOUCH: &str = r#"query touch() { update Chunk set { n: 7 } where slug = "seed-07" }"#;

const FOLD: &str = "branch merge side into main";

/// [`SERVED_WORKLOAD`] with a session of each statement kind it lacks: the
/// mutation [`TOUCH`] and the merge [`FOLD`], after its writers and reader.
fn every_kind_workload() -> String {
    SERVED_WORKLOAD.replace(
        "--- expect\nwriter: ok\nreader: ok\n",
        &format!(
            "toucher: {TOUCH}\nfolder: {FOLD}\n--- expect\nwriter: ok\nreader: ok\ntoucher: ok\nfolder: ok\n"
        ),
    )
}

/// What a sample of the two-writer, one-reader block shows on every server:
/// both labels over the declared window and every acknowledged row read back.
/// Returns the rows the writers were acknowledged for.
fn assert_block(sample: &GqtRepObservation) -> u64 {
    assert_eq!(sample.outcome, "expectations-passed");
    let sample = serde_json::to_value(sample).unwrap();
    let block = &sample["concurrent"];
    assert_eq!(block["driver"], "closed-loop");
    assert_eq!(block["claim_grade"], false);
    assert_eq!(number(block, "measured_seconds"), 2);
    assert_eq!(warmup_acked(block), 0, "the run was given no warmup");
    let window_us = number(block, "window_us");
    assert!(
        window_us >= 2_000_000,
        "the window is the declared duration"
    );
    let labels = block["labels"].as_array().unwrap();
    assert_eq!(labels.len(), 2, "one row per label, in declaration order");
    let written = assert_label(&labels[0], "writer", "load", 2, window_us);
    assert_label(&labels[1], "reader", "query", 1, window_us);
    assert_eq!(
        number(block, "readback.rows"),
        written,
        "every acknowledged dealt row is read back"
    );
    assert!(number(block, "readback.compared") > 0);
    let receipts = sample["steps"].as_array().unwrap();
    assert_eq!(receipts[0]["ordinal"], 1);
    assert_eq!(receipts[0]["kind"], "concurrent");
    written
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_repetition_provisions_drives_and_reads_back_over_http() {
    let (server, bound, input, outcome, signals) = repetition(|_| {}).await;
    let sample = outcome.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(signals.events, vec!["ready", "settled"]);
    assert_eq!(signals.elapsed, Some(sample.elapsed_us));
    let written = assert_block(&sample);
    assert_eq!(
        sample.server_executable_sha256.as_ref(),
        Some(&input.server_sha256)
    );
    assert!(
        sample.peak_rss_bytes.is_none() && sample.client_peak_rss_bytes.is_none(),
        "the parent reaps both processes; the worker reports neither peak"
    );
    assert!(sample.logical_store_calls.is_none() && sample.control_store_calls.is_none());
    let text = serde_json::to_string(&sample).unwrap();
    for token in input.tokens() {
        assert!(!text.contains(token), "a sample retains no token");
    }
    let graph = server.graph();
    assert_eq!(graph.unauthorized, 0, "every request carried a known token");
    assert_eq!(
        graph.loads.keys().copied().collect::<Vec<_>>(),
        [STEPS, 1, 2],
        "the steps' actor seeds and each writer loads as its own actor"
    );
    assert_eq!(graph.loads[&STEPS], 1, "the fixture is one batch");
    assert_eq!(graph.loads[&1] + graph.loads[&2], written);
    assert_eq!(graph.rows["Chunk"].len() as u64, 16 + written);
    assert!(
        graph.queries[&3] > 0 && !graph.queries.contains_key(&1) && !graph.queries.contains_key(&2),
        "the reader queries as its own actor and no writer reads: {:?}",
        graph.queries
    );
    assert!(
        graph.queries[&STEPS] >= 3,
        "the count, the readback and the step after the block are the steps' actor's"
    );
    drop(graph);

    let proof = proof(&input);
    validate_sample(&sample, &bound, 1, &proof, sample.elapsed_us, false).unwrap();
    let mut reaped = sample.clone();
    (reaped.peak_rss_bytes, reaped.client_peak_rss_bytes) = (Some(1 << 20), Some(1 << 20));
    validate_sample(&reaped, &bound, 1, &proof, reaped.elapsed_us, true).unwrap();
    type Change = fn(&mut GqtRepObservation);
    let refused: &[(&str, Change)] = &[
        ("another server binary", |s| {
            s.server_executable_sha256 = Some("e".repeat(64))
        }),
        ("no server binary", |s| s.server_executable_sha256 = None),
        ("a deployment receipt", |s| {
            s.server_receipt_sha256 = Some("e".repeat(64))
        }),
        ("a base", |s| s.base_manifest_sha256 = Some("e".repeat(64))),
        ("a peak the worker cannot know", |s| {
            s.peak_rss_bytes = Some(1)
        }),
    ];
    for (name, change) in refused {
        let mut changed = sample.clone();
        change(&mut changed);
        assert!(
            validate_sample(&changed, &bound, 1, &proof, changed.elapsed_us, false).is_err(),
            "{name}"
        );
    }
    let provisioned = PreparationProofV2::Served {
        server_receipt_sha256: "e".repeat(64),
    };
    assert!(
        validate_sample(&sample, &bound, 1, &provisioned, sample.elapsed_us, false).is_err(),
        "a provisioned server's proof does not pair with a rebuild point"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_statement_kind_of_a_block_runs_over_http_as_its_own_actor() {
    let plan = server_point(&every_kind_workload(), Some(ResetMode::Rebuild)).unwrap();
    let (server, _, _, outcome, signals) = repetition_of(&plan, 5, |_| {}).await;
    let sample = outcome.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(signals.events, vec!["ready", "settled"]);
    assert_eq!(sample.outcome, "expectations-passed");
    let sample = serde_json::to_value(&sample).unwrap();
    let block = &sample["concurrent"];
    let window_us = number(block, "window_us");
    let labels = block["labels"].as_array().unwrap();
    assert_eq!(labels.len(), 4, "one row per label, in declaration order");
    let written = assert_label(&labels[0], "writer", "load", 2, window_us);
    assert_label(&labels[1], "reader", "query", 1, window_us);
    let touched = assert_label(&labels[2], "toucher", "mutate", 1, window_us);
    let folded = assert_label(&labels[3], "folder", "branch merge", 1, window_us);
    assert_eq!(
        number(block, "readback.rows"),
        written,
        "the readback is of the dealt rows alone"
    );
    let graph = server.graph();
    assert_eq!(graph.unauthorized, 0, "every request carried a known token");
    assert_eq!(
        graph.changes.keys().copied().collect::<Vec<_>>(),
        [4, 5],
        "the mutation and the merge reach `/mutate`, each as its session's own actor"
    );
    let (touches, folds) = (&graph.changes[&4], &graph.changes[&5]);
    assert_eq!(
        (touches.len() as u64, folds.len() as u64),
        (touched, folded),
        "one request per acknowledged run"
    );
    for sent in touches {
        assert_eq!(
            (&sent["query"], &sent["name"], &sent["branch"]),
            (&json!(TOUCH), &json!("touch"), &json!("main")),
            "a mutation is sent as its declaration, by name, on the session's branch"
        );
    }
    for sent in folds {
        assert_eq!(sent["query"], FOLD);
        assert!(
            sent["name"].is_null() && sent["branch"].is_null(),
            "a branch statement is sent with no name and no branch: {sent}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_set_conflict_over_http_is_run_again_and_counted() {
    let (server, _, _, outcome, _) = repetition(|graph| {
        graph.conflicts.insert(1, 3);
    })
    .await;
    let sample = outcome.unwrap_or_else(|error| panic!("{error}"));
    let written = assert_block(&sample);
    let sample = serde_json::to_value(&sample).unwrap();
    let labels = sample["concurrent"]["labels"].as_array().unwrap();
    assert_eq!(
        number(&labels[0], "authority_conflicts"),
        3,
        "each run the server answered with a read-set conflict is counted once"
    );
    assert_eq!(number(&labels[1], "authority_conflicts"), 0);
    let graph = server.graph();
    assert_eq!(
        graph.conflicts[&1], 0,
        "every scripted conflict was answered"
    );
    assert_eq!(
        graph.loads[&1] + graph.loads[&2],
        written,
        "a run answered with a conflict loaded nothing and was acknowledged once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_set_conflict_is_run_again_no_more_than_the_hosts_bound() {
    use omnigraph_gqt_core::concurrent::MAX_HOST_RETRIES;
    let (server, _, _, outcome, signals) = repetition(|graph| {
        graph.conflicts.insert(1, MAX_HOST_RETRIES + 8);
        graph.conflict_effects = Some("none");
    })
    .await;
    let error = outcome.unwrap_err();
    assert!(
        error.message.contains("load failed")
            && error.message.contains("the write's read set changed"),
        "{error}"
    );
    assert_eq!(signals.events, vec!["ready"]);
    assert_eq!(
        server.graph().conflicts[&1],
        7,
        "the first run and each re-run of the bound were answered, and no run after them was sent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_after_an_earlier_publication_is_not_run_again() {
    let (server, _, _, outcome, _) = repetition(|graph| {
        graph.conflicts.insert(1, 3);
        graph.conflict_effects = Some("partial");
    })
    .await;
    let error = outcome.unwrap_err();
    assert!(
        error.message.contains("load failed")
            && error.message.contains("the write's read set changed"),
        "{error}"
    );
    assert_eq!(
        server.graph().conflicts[&1],
        2,
        "a request the server applied in part is sent once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_409_that_names_a_read_set_conflict_is_run_again() {
    let refusals = [
        (
            "500 Internal Server Error",
            json!({"read_set_conflict": {"table": "node:Chunk"}}),
        ),
        ("409 Conflict", Value::Null),
    ];
    for (status, detail) in refusals {
        let (server, _, _, outcome, _) = repetition(move |graph| {
            graph.refusal = Some(Refusal {
                token: 1,
                after: 2,
                status,
                error: "the branch moved".into(),
                detail,
            });
        })
        .await;
        let error = outcome.unwrap_err();
        assert!(
            error.message.contains("load failed") && error.message.contains("the branch moved"),
            "{status}: {error}"
        );
        assert_eq!(
            server.graph().refused,
            1,
            "{status}: the refused run is not sent again"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workload_limit_over_http_fails_the_block_unsettled() {
    let (server, _, _, outcome, signals) = repetition(|graph| {
        graph.refusal = Some(Refusal {
            token: 2,
            after: 5,
            status: "429 Too Many Requests",
            error: "the graph's admission is full".into(),
            detail: Value::Null,
        });
    })
    .await;
    let error = outcome.unwrap_err();
    assert_eq!(error.code, "gqt_measure_failed", "{error}");
    assert!(
        error.message.contains("HTTP 429")
            && error.message.contains("a block never waits out")
            && error.message.contains("the graph's admission is full"),
        "{error}"
    );
    assert!(
        error.context.gqt_settled_sample.is_none(),
        "a block that never settled leaves no sample"
    );
    assert_eq!(signals.events, vec!["ready"]);
    let graph = server.graph();
    assert_eq!(
        graph.loads[&2], 5,
        "every load before the limit was accepted"
    );
    assert_eq!(graph.refused, 1, "the refused run is not sent again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_that_repeats_a_token_reaches_the_parent_redacted() {
    let reflected = tokens(3).swap_remove(1);
    let (_server, _, input, outcome, _) = repetition(|graph| {
        graph.refusal = Some(Refusal {
            token: 1,
            after: 2,
            status: "400 Bad Request",
            error: format!("rejected the request of {reflected}"),
            detail: Value::Null,
        });
    })
    .await;
    let error = outcome.unwrap_err();
    assert!(
        error.message.contains("load failed") && error.message.contains("<redacted>"),
        "{error}"
    );
    for token in input.tokens() {
        assert!(!error.to_string().contains(token), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisioning_refuses_a_graph_whose_rows_differ_from_the_base() {
    let tokens = tokens(3);
    let server = GraphServer::start(&tokens);
    let (bound, mut input) = input(&rebuild_point(), &server.url, &tokens);
    input.tables[0].rows = 17;
    let mut signals = Signals::default();
    let error = execute_owned_server_rep_signaled(1, &bound, &input, &mut signals)
        .await
        .unwrap_err();
    assert!(
        error
            .message
            .contains("the seeded graph holds 16 rows of Chunk on main and the fixture holds 17"),
        "{error}"
    );
    assert!(
        signals.events.is_empty(),
        "the workload never starts on a graph that is not the fixture's"
    );
    assert!(
        !server.graph().loads.contains_key(&1),
        "no session of the block ran"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisioning_counts_the_edge_tables_of_the_base_too() {
    /// A repetition whose base holds five `Knows` edges on `main`, against a
    /// stand-in whose snapshot counts `held`.
    async fn against(held: u64) -> (RunnerResult<GqtRepObservation>, Signals) {
        let tokens = tokens(3);
        let server = GraphServer::start(&tokens);
        server.graph().edges.insert("Knows".into(), held);
        let (bound, mut input) = input(&rebuild_point(), &server.url, &tokens);
        input.edge_tables = vec![SeededTable {
            branch: "main".into(),
            table: "Knows".into(),
            rows: 5,
        }];
        let mut signals = Signals::default();
        let outcome = execute_owned_server_rep_signaled(1, &bound, &input, &mut signals).await;
        (outcome, signals)
    }
    let (outcome, signals) = against(4).await;
    let error = outcome.unwrap_err();
    assert!(
        error.message.contains(
            "the seeded graph holds 4 rows of edge Knows on main and the fixture holds 5"
        ),
        "{error}"
    );
    assert!(
        signals.events.is_empty(),
        "the workload never starts on a graph that is not the fixture's"
    );
    let (outcome, signals) = against(5).await;
    let sample = outcome.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(sample.outcome, "expectations-passed");
    assert_eq!(signals.events, vec!["ready", "settled"]);
}

#[tokio::test]
async fn a_host_that_does_not_measure_refuses_a_measured_block_against_a_server() {
    let case = omnigraph_gqt_core::parse_case("served_block", SERVED_WORKLOAD).unwrap();
    let target = ServerTarget {
        url: "http://127.0.0.1:9".into(),
        graph: OWNED_SERVER_GRAPH.into(),
        token: None,
    };
    let refusal = omnigraph_gqt_core::execute_measured_served(
        &case,
        &target,
        &omnigraph_gqt_core::PlainHost,
        1,
    )
    .await
    .unwrap_err();
    assert!(
        refusal.contains(
            "unsupported_environment: a measured block against a server requires a measuring host"
        ),
        "{refusal}"
    );
}

#[test]
fn a_block_plans_against_a_server_only_under_rebuild_and_within_its_session_bound() {
    let plan = rebuild_point();
    assert_eq!(plan.definition.protocol.reset, ResetMode::Rebuild);
    assert_eq!(
        plan.definition.protocol.attribution,
        crate::case::Attribution::Off
    );
    assert_eq!(
        plan.cache_condition.process,
        crate::case::ProcessLifecycle::FreshPerRepetition,
        "the server is the repetition's own"
    );
    assert!(plan.block.is_some(), "the block's run is planned");
    for reset in [None, Some(ResetMode::Clone)] {
        let refusal = server_point(SERVED_WORKLOAD, reset).unwrap_err();
        assert!(
            refusal.contains("a server started per repetition")
                && refusal.contains("reset: rebuild"),
            "{reset:?}: a refused server point names the level that runs a block: {refusal}"
        );
    }
    let crowded = SERVED_WORKLOAD.replace("writer x2", "writer x64");
    let refusal = server_point(&crowded, Some(ResetMode::Rebuild)).unwrap_err();
    assert!(
        refusal.contains("runs at most 64 sessions") && refusal.contains("declares 65"),
        "{refusal}"
    );
    let full = SERVED_WORKLOAD.replace("writer x2", "writer x63");
    let admitted = server_point(&full, Some(ResetMode::Rebuild)).unwrap();
    let sessions = admitted
        .queries
        .parse()
        .unwrap()
        .measured_block()
        .map(|block| block.sessions.len());
    assert_eq!(sessions, Some(64), "the bound itself is admitted");
    let embedded = first_point(SERVED_WORKLOAD);
    assert_ne!(
        embedded.planned_sha256, plan.planned_sha256,
        "the embedded point and the server point of one workload are two points"
    );
}

#[test]
fn a_rebuild_point_runs_with_a_local_store_and_a_server_binary_and_nothing_else() {
    let plan = rebuild_point();
    let accepted = RunOptions {
        store: Some("/tmp/bench-store".into()),
        server_product: Some(ServerProduct::Beside),
        ..RunOptions::default()
    };
    validate_run_options(&plan, &accepted).unwrap();
    type Change = fn(&mut RunOptions);
    let refused: &[(&str, Change, &str)] = &[
        (
            "no store",
            |o| o.store = None,
            "--store with --server-product",
        ),
        (
            "no server binary",
            |o| o.server_product = None,
            "requires --server-product",
        ),
        (
            "an s3 store",
            |o| o.store = Some("s3://bucket/prefix".into()),
            "an s3:// store and --backend do not apply",
        ),
        (
            "a local route",
            |o| o.local_route = LocalRouteKind::ReadThrough,
            "is reached over HTTP",
        ),
    ];
    for (name, change, needle) in refused {
        let mut options = accepted.clone();
        change(&mut options);
        let refusal = validate_run_options(&plan, &options).unwrap_err().message;
        assert!(refusal.contains(needle), "{name}: {refusal}");
    }
    let embedded = RunOptions {
        server_product: Some(ServerProduct::Beside),
        ..RunOptions::default()
    };
    let refusal = validate_run_options(&first_point(SERVED_WORKLOAD), &embedded)
        .unwrap_err()
        .message;
    assert!(refusal.contains("this point runs embedded"), "{refusal}");
}

/// The record a run of the rebuild point publishes around `sample`: the
/// product's build with its server binary, the base it bound, and the
/// server's peak on the sample, on the legacy fixture's machine and backend.
async fn owned_server_record(
    bound: BoundGqt,
    input: &OwnedServerInput,
    mut sample: GqtRepObservation,
) -> crate::gqt_record::ConcurrentRunRecordV1 {
    use crate::gqt_record::{
        BaseEvidenceV1, ConcurrentRunRecordV1, GqtMeasurementsV1, GqtRunIdentityV1, GqtRunRecordV1,
        GqtSutIdentityV1, ProductSutIdentityV1, ProductSutKind, layer_presence,
    };
    use crate::record::{
        AcquisitionStatusV1, AcquisitionV1, EvidenceStrengthV1, WallClockSummaryV1,
    };
    let plan = bound.plan.clone();
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let (summary, registered_source_identity) = crate::dataset_worker::build_dataset(
        &plan.base_build_plan().unwrap(),
        directory.path().join("base").to_str().unwrap(),
        &scratch,
        None,
    )
    .await
    .unwrap();
    let bound = plan
        .bind(&summary.logical_content_sha256, &summary.algorithm)
        .unwrap();
    (sample.peak_rss_bytes, sample.client_peak_rss_bytes) = (Some(1 << 20), Some(1 << 20));
    let legacy = crate::record::tests::valid_record_fixture();
    let mut invocation = legacy.invocation;
    invocation.invocation_id.replace_range(25..26, "F");
    let elapsed = sample.elapsed_us;
    let observed = sample.concurrent.clone().unwrap();
    ConcurrentRunRecordV1 {
        format_version: 1,
        driver: observed.driver,
        claim_grade: false,
        warmup_seconds: 0,
        measured_seconds: 2,
        sweep: observed.sweep.clone(),
        tokio_unstable: observed.tokio_unstable,
        record: GqtRunRecordV1 {
            format_version: 1,
            invocation,
            run: GqtRunIdentityV1 {
                point_identity_version: 1,
                point_id: bound.point_id,
                point_name: bound.point_name,
                case_id: plan.definition.id.clone(),
                case_digest: plan.case_digest.clone(),
                run_spec: bound.identity,
            },
            sut: GqtSutIdentityV1::Product(Box::new(ProductSutIdentityV1 {
                kind: ProductSutKind::ProductBinary,
                package_version: "0.14.0".into(),
                http_contract: "0.14".into(),
                internal_schema_writes: 1,
                executable_sha256: "e".repeat(64),
                source_commit: None,
                requested_commit: None,
                server_executable_sha256: Some(input.server_sha256.clone()),
                harness: legacy.sut,
            })),
            machine: Some(legacy.machine),
            backend: Some(legacy.backend),
            fixture: None,
            dataset_cache_hit: Some(false),
            base: Some(BaseEvidenceV1 {
                format_version: 1,
                key: "a".repeat(64),
                recipe_sha256: plan.recipe_sha256.clone(),
                engine_digest: crate::dataset_cache::GQT_ENGINE_DIGEST.into(),
                needs_indices: plan.needs_indices,
                storage_format: crate::store::StorageFormatV1::current(),
                summary,
                registered_source_identity,
                object_count: 1,
                object_bytes: 1,
                manifest_sha256: "b".repeat(64),
                built_at_unix_ms: 1,
            }),
            acquisition: AcquisitionV1 {
                status: AcquisitionStatusV1::Complete,
                requested_repetitions: 1,
                observed_repetitions: 1,
                terminal: None,
            },
            measurements: GqtMeasurementsV1 {
                wall_clock: WallClockSummaryV1 {
                    min_us: elapsed,
                    p50_us: elapsed,
                    max_us: elapsed,
                    p95_us: None,
                    p95_supported: false,
                    evidence: EvidenceStrengthV1::Directional,
                },
                raw_samples: vec![sample],
                layer_presence: layer_presence(Target::Server),
                claim_policy: legacy.measurements.claim_policy,
            },
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owned_server_record_round_trips_projects_and_refuses_what_it_does_not_show() {
    use crate::gqt_record::{
        AnyRunRecordV1, ConcurrentRunRecordV1, GqtSutIdentityV1, canonical_bytes, parse,
        validate_concurrent,
    };
    let (_server, bound, input, outcome, _) = repetition(|_| {}).await;
    let sample = outcome.unwrap_or_else(|error| panic!("{error}"));
    let record = owned_server_record(bound, &input, sample).await;
    validate_concurrent(&record).unwrap();
    let bytes = canonical_bytes(&record).unwrap();
    let AnyRunRecordV1::Concurrent(parsed) = parse(&bytes).unwrap() else {
        panic!("an owned-server record parses as a concurrent record");
    };
    assert_eq!(*parsed, record);
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains(&format!(
        "\"server_executable_sha256\":\"{}\"",
        input.server_sha256
    )));
    type Change = fn(&mut ConcurrentRunRecordV1);
    let refused: &[(&str, Change, &str)] = &[
        (
            "a sample without its server's peak memory",
            |r| r.record.measurements.raw_samples[0].peak_rss_bytes = None,
            "a sample without its server's peak memory",
        ),
        (
            "a product that names no server binary",
            |r| {
                let GqtSutIdentityV1::Product(sut) = &mut r.record.sut else {
                    panic!("a product record");
                };
                sut.server_executable_sha256 = None;
            },
            "names a server binary exactly when its target is the server",
        ),
        (
            "a server binary the samples did not run against",
            |r| {
                let GqtSutIdentityV1::Product(sut) = &mut r.record.sut else {
                    panic!("a product record");
                };
                sut.server_executable_sha256 = Some("f".repeat(64));
            },
            "invalid GQT sample",
        ),
        (
            "no base",
            |r| r.record.base = None,
            "target and evidence tuple disagree",
        ),
    ];
    for (name, change, needle) in refused {
        let mut changed = record.clone();
        change(&mut changed);
        let refusal = validate_concurrent(&changed).unwrap_err().message;
        assert!(refusal.contains(needle), "{name}: {refusal}");
    }

    let directory = tempfile::tempdir().unwrap();
    let (archive, projection) = (
        directory.path().join("archive"),
        directory.path().join("projection"),
    );
    crate::archive::preflight_archive_publication(&archive).unwrap();
    crate::archive::publish_record(&archive, &record).unwrap();
    crate::projection::rebuild_projection(&archive, &projection)
        .await
        .unwrap();
    let point = &record.record.run.point_id;
    let runs = crate::projection::list_runs_for_point_page(&projection, point, 10, None)
        .await
        .unwrap();
    let [row] = runs.rows.as_slice() else {
        panic!("one run of the point: {:?}", runs.rows);
    };
    assert_eq!(row["sut_evidence"], "product-binary");
    assert!(row["fixture_manifest_sha256"].is_string());
    for column in [
        "lance_data_plane_logical_calls_p50",
        "control_plane_logical_calls_p50",
    ] {
        assert!(
            row[column].is_null(),
            "{column}: a server reports no store calls"
        );
    }
}

#[cfg(unix)]
mod process {
    use super::*;
    use crate::gqt_server_process::{
        LISTEN_ADDR_PREFIX, ServerDeadlines, ServerProcess, apply_cluster, write_cluster_config,
    };
    use crate::supervisor::tests::worker_script;

    /// A stand-in for the server binary: it records how it was started, prints
    /// the address in `STAND_IN_ADDRESS` as a server prints its own, and
    /// leaves on SIGTERM with the status `on_term` spells.
    fn listening(on_term: &str) -> String {
        format!(
            "trap {on_term} TERM\nprintf '%s\\n' \"$@\" > \"$0.args\"\nprintf '%s' \"$OMNIGRAPH_SERVER_BEARER_TOKENS_JSON\" > \"$0.tokens\"\nprintf '%s' \"${{HOME-unset}}\" > \"$0.home\"\nprintf '%s' \"${{TMPDIR-unset}}\" > \"$0.tmpdir\"\nprintf '{LISTEN_ADDR_PREFIX}%s\\n' \"$STAND_IN_ADDRESS\"\nwhile :; do /bin/sleep 0.05; done\n"
        )
    }

    fn sidecar(script: &Path, suffix: &str) -> String {
        let mut path = script.as_os_str().to_owned();
        path.push(format!(".{suffix}"));
        std::fs::read_to_string(path).unwrap()
    }

    type Started = (
        std::sync::MutexGuard<'static, ()>,
        tempfile::TempDir,
        PathBuf,
        RunnerResult<ServerProcess>,
    );

    /// Starts the stand-in `body` as a server over `root`, answered for at
    /// `ready`'s address, inside `deadline`.
    fn start(body: &str, ready: &GraphServer, deadline: Duration) -> Started {
        start_beside(None, body, ready, deadline)
    }

    /// [`start`] beside a product binary that names the commit `product`.
    fn start_beside(
        product: Option<&str>,
        body: &str,
        ready: &GraphServer,
        deadline: Duration,
    ) -> Started {
        let (guard, directory, script) = worker_script(body);
        let (root, logs) = (directory.path().join("root"), directory.path().join("logs"));
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&logs).unwrap();
        let environment = [("STAND_IN_ADDRESS".to_string(), ready.address().to_string())];
        let tokens = [("act-bench-steps".to_string(), "started-token".to_string())];
        let started = ServerProcess::start(
            &script,
            product,
            &root,
            &tokens,
            &environment,
            &logs,
            deadline,
        );
        (guard, directory, script, started)
    }

    #[test]
    fn a_server_is_started_with_its_tokens_alone_and_reaped_for_its_peak() {
        let ready = GraphServer::start(&[]);
        let (_guard, directory, script, started) =
            start(&listening("'exit 0'"), &ready, Duration::from_secs(20));
        let server = started.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(server.url(), ready.url);
        let root = directory.path().join("root");
        assert_eq!(
            sidecar(&script, "args"),
            format!(
                "--cluster\nfile://{}\n--bind\n127.0.0.1:0\n",
                root.display()
            ),
            "the applied root as a storage URI, on a port the system picks"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&sidecar(&script, "tokens")).unwrap(),
            json!({"act-bench-steps": "started-token"})
        );
        assert_eq!(
            sidecar(&script, "home"),
            "unset",
            "the server inherits nothing of the bench's own environment"
        );
        assert_eq!(
            sidecar(&script, "tmpdir"),
            directory.path().join("logs/tmp").display().to_string(),
            "its temporary files stay under the directory it was started with"
        );
        let stopped = server.stop(Duration::from_secs(20)).unwrap();
        assert!(stopped.peak_rss_bytes > 0);
    }

    #[test]
    fn a_server_that_never_becomes_ready_fails_the_start_with_its_stderr() {
        let ready = GraphServer::start(&[]);
        let failed = |body: &str, deadline: Duration| {
            let (_guard, _directory, _, started) = start(body, &ready, deadline);
            let error = started.err().expect("the stand-in never becomes ready");
            assert_eq!(error.code, "server_start_failed", "{error}");
            error.message
        };
        let exited = failed(
            "echo 'the cluster has no graph' >&2\nexit 3\n",
            Duration::from_secs(20),
        );
        assert!(
            exited.contains("before it was ready") && exited.contains("the cluster has no graph"),
            "{exited}"
        );
        let silent = failed(
            "trap 'exit 0' TERM\nwhile :; do /bin/sleep 0.05; done\n",
            Duration::from_millis(300),
        );
        assert!(
            silent.contains("did not report its bound address"),
            "{silent}"
        );
        ready.graph().loading = 1;
        let loading = never_ready(failed);
        assert!(
            loading.contains(&format!("was not ready at {}/readyz", ready.url)),
            "{loading}"
        );
        {
            let mut graph = ready.graph();
            (graph.loading, graph.unready) = (0, true);
        }
        let draining = never_ready(failed);
        assert!(
            draining.contains(&format!("was not ready at {}/readyz", ready.url)),
            "{draining}"
        );
    }

    /// The message of a start whose stand-in reports its address and is never
    /// ready. The deadline has to outlast the stand-in's own start, so a start
    /// that never saw the address runs again with twice the time.
    fn never_ready(failed: impl Fn(&str, Duration) -> String) -> String {
        let mut deadline = Duration::from_secs(2);
        loop {
            let message = failed(&listening("'exit 0'"), deadline);
            let unseen = message.contains("did not report its bound address");
            if !unseen || deadline >= Duration::from_secs(16) {
                return message;
            }
            deadline *= 2;
        }
    }

    #[test]
    fn a_server_built_from_another_commit_than_the_product_is_refused() {
        let ready = GraphServer::start(&[]);
        let (product, other) = ("a".repeat(40), "b".repeat(40));
        let cases = [
            (Some(&other), Some(product.as_str()), false),
            (Some(&product), Some(product.as_str()), true),
            (None, Some(product.as_str()), true),
            (Some(&other), None, true),
        ];
        for (served, named, admitted) in cases {
            ready.graph().source_version = served.cloned();
            let (_guard, _directory, _, started) = start_beside(
                named,
                &listening("'exit 0'"),
                &ready,
                Duration::from_secs(20),
            );
            match (started, admitted) {
                (Ok(server), true) => {
                    server.stop(Duration::from_secs(20)).unwrap();
                }
                (Err(error), false) => {
                    assert_eq!(error.code, "server_commit_mismatch", "{error}");
                    assert!(
                        error.message.contains(other.as_str())
                            && error.message.contains(product.as_str()),
                        "{error}"
                    );
                }
                (started, _) => panic!(
                    "a server that names {served:?} beside a product that names {named:?}: {:?}",
                    started.map(|_| "started")
                ),
            }
        }
    }

    #[test]
    fn a_failed_start_carries_what_the_server_wrote_to_both_streams() {
        let ready = GraphServer::start(&[]);
        let (_guard, _directory, _, started) = start(
            "echo 'the storage root is unreachable'\necho 'no graph to serve' >&2\nexit 3\n",
            &ready,
            Duration::from_secs(20),
        );
        let error = started
            .err()
            .expect("the stand-in exits before it is ready");
        assert!(
            error
                .message
                .contains("stdout: the storage root is unreachable")
                && error.message.contains("stderr: no graph to serve"),
            "{error}"
        );
    }

    #[test]
    fn a_server_whose_output_is_not_utf8_still_reports_its_address() {
        let ready = GraphServer::start(&[]);
        let body = format!("printf '\\377\\n'\n{}", listening("'exit 0'"));
        let (_guard, directory, _, started) = start(&body, &ready, Duration::from_secs(20));
        let server = started.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(server.url(), ready.url);
        server.stop(Duration::from_secs(20)).unwrap();
        let logged = std::fs::read(directory.path().join("logs/server.stdout.log")).unwrap();
        assert!(
            logged.starts_with(b"\xff\n"),
            "the log keeps the bytes the server wrote"
        );
    }

    #[test]
    fn a_server_that_does_not_leave_cleanly_on_sigterm_fails_the_stop() {
        let ready = GraphServer::start(&[]);
        let stopped = |on_term: &str, deadline: Duration| {
            let (_guard, _directory, _, started) =
                start(&listening(on_term), &ready, Duration::from_secs(20));
            let server = started.unwrap_or_else(|error| panic!("{error}"));
            let error = server.stop(deadline).unwrap_err();
            assert_eq!(error.code, "server_stop_failed", "{error}");
            error.message
        };
        let ignored = stopped("''", Duration::from_millis(300));
        assert!(
            ignored.contains("did not exit within") && ignored.contains("was killed"),
            "{ignored}"
        );
        let unclean = stopped("'exit 7'", Duration::from_secs(20));
        assert!(unclean.contains("after SIGTERM"), "{unclean}");
    }

    /// `omnigraph` and `omnigraph-server` as this build left them, in the
    /// profile directory the test binary runs under.
    fn built_products() -> (PathBuf, PathBuf) {
        let test = std::env::current_exe().unwrap();
        let profile = test.parent().and_then(Path::parent).unwrap();
        let (product, server) = (profile.join("omnigraph"), profile.join("omnigraph-server"));
        assert!(
            product.is_file(),
            "build the omnigraph binary before this test: {}",
            product.display()
        );
        assert!(
            server.is_file(),
            "build the omnigraph-server binary before this test: {}",
            server.display()
        );
        (product, server)
    }

    /// One repetition of `plan` against the product itself: the cluster the
    /// bench writes applied by `omnigraph`, `omnigraph-server` started over it
    /// with one actor per session, the worker's run, and the stop.
    async fn against_the_product(
        plan: &PlannedGqt,
        sessions: usize,
        branches: Vec<SeededTable>,
        edges: Vec<SeededTable>,
    ) -> GqtRepObservation {
        let (product, server) = built_products();
        let DatasetRecipe::Gqt { source } = &plan.dataset else {
            panic!("an authored dataset");
        };
        let schema = source.parse().unwrap().fixture.unwrap().schema;
        let directory = tempfile::tempdir().unwrap();
        let [config, root, logs, home] =
            ["config", "root", "logs", "home"].map(|name| directory.path().join(name));
        for created in [&config, &root, &logs, &home] {
            std::fs::create_dir(created).unwrap();
        }
        let actors: Vec<String> = std::iter::once("steps".to_string())
            .chain((0..sessions).map(|session| format!("s{session:02}")))
            .map(|name| format!("act-bench-{name}"))
            .collect();
        let tokens = tokens(sessions);
        write_cluster_config(&config, OWNED_SERVER_GRAPH, &schema, &actors).unwrap();
        let mut environment = crate::gqt_runner::product_environment();
        environment.push(("OMNIGRAPH_HOME".to_string(), home.display().to_string()));
        let deadlines = ServerDeadlines::default();
        apply_cluster(
            &product,
            &config,
            &root,
            &actors[STEPS],
            &environment,
            deadlines.apply,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let actor_tokens: Vec<(String, String)> =
            actors.iter().cloned().zip(tokens.iter().cloned()).collect();
        let built = crate::gqt_runner::inspect_product(&product, None, &environment)
            .unwrap_or_else(|error| panic!("{error}"));
        let process = ServerProcess::start(
            &server,
            built.source_commit.as_deref(),
            &root,
            &actor_tokens,
            &environment,
            &logs,
            deadlines.start,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            omnigraph_gqt_core::server_source_commit(process.url(), Duration::from_secs(5))
                .await
                .unwrap(),
            built.source_commit,
            "the server and the product of one build name one commit"
        );
        let url = crate::gqt_served::canonical_endpoint(process.url()).unwrap();
        let (bound, mut input) = input(plan, &url, &tokens);
        input.tables.extend(branches);
        input.edge_tables = edges;
        input.server_sha256 = crate::gqt_protocol::digest_worker_executable(&server).unwrap();
        let mut signals = Signals::default();
        let outcome = execute_owned_server_rep_signaled(1, &bound, &input, &mut signals).await;
        let server_logs = process.logs().clone();
        let stopped = process.stop(deadlines.stop);
        let sample =
            outcome.unwrap_or_else(|error| panic!("{error}; server {}", server_logs.tails()));
        let stopped = stopped.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(signals.events, vec!["ready", "settled"]);
        assert_eq!(
            sample.server_executable_sha256.as_ref(),
            Some(&input.server_sha256)
        );
        assert!(
            stopped.peak_rss_bytes > 1 << 20,
            "a server's peak is megabytes"
        );
        let text = serde_json::to_string(&sample).unwrap();
        for token in input.tokens() {
            assert!(!text.contains(token), "a sample retains no token");
        }
        sample
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "nightly: a rebuild repetition against the built omnigraph and omnigraph-server binaries"]
    async fn a_rebuild_repetition_runs_against_the_builds_own_server() {
        let sample = against_the_product(&rebuild_point(), 3, Vec::new(), Vec::new()).await;
        assert_block(&sample);
    }

    /// Three `Next` edges for the sixteen-row fixture, two of them over one
    /// pair of rows, so a count of edges is told from a count of pairs.
    const LINKS: &str = r#"  - kind: edge
    name: Next
    rows: 3
    commits: 1
    batch_rows: 3
    id: {kind: key, prefix: e, width: 1}
    from: {kind: key, prefix: seed-, width: 2, modulo: 2}
    to: {kind: literal, value: seed-02}
    columns: {}
"#;

    /// One branch for the sixteen-row fixture: `side0`, cut from `main`, with
    /// a row of its own.
    const SIDE_BRANCH: &str = r#"
--- branches generate: v1 seed: 0 count: 1 prefix: side from: main
tables:
  - kind: node
    name: Chunk
    rows: 1
    commits: 1
    columns:
      slug: {kind: key, prefix: ${branch}-, width: 3}
      n: {kind: ordinal, start: 100, step: 1}
"#;

    const EVERY_KIND: &str = r#"# issue: none
# notes: Two writers and a reader beside a mutation and a merge, then the rows those two leave on main.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
  - target: omnigraph-server
    storage: local-filesystem

--- bench
warmup_seconds: 0
duration_seconds: 2

--- concurrent measured
writer x2: load generate: v1 seed: 42 mode: append
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
reader: query seeded_row() { match { $c: Chunk { slug: "seed-07" } } return { $c.n } }
toucher: query touch() { update Chunk set { n: 300 } where slug = "seed-03" }
folder: branch merge side0 into main
--- expect
writer: ok
reader: ok
toucher: ok
folder: ok

--- query branch: main
query touched() { match { $c: Chunk { slug: "seed-03" } } return { $c.slug, $c.n } }
--- expect unordered
{"c.slug":"seed-03","c.n":300}
--- expect shape
c.slug: String
c.n: I64

--- query branch: main
query folded() { match { $c: Chunk { slug: "side0-000" } } return { $c.slug, $c.n } }
--- expect unordered
{"c.slug":"side0-000","c.n":100}
--- expect shape
c.slug: String
c.n: I64
"#;

    /// A block of every statement kind against the product: the server answers
    /// the mutation and the merge beside two writers, and the steps after the
    /// block read from `main` the row each of the two left there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "nightly: a block of every statement kind against the built omnigraph and omnigraph-server binaries"]
    async fn every_statement_kind_runs_against_the_builds_own_server() {
        let directory = tempfile::tempdir().unwrap();
        let chunks = "node Chunk { slug: String @key n: I64 }\n";
        let fixture = format!("{FIXTURE}{LINKS}{SIDE_BRANCH}")
            .replace(chunks, &format!("{chunks}edge Next: Chunk -> Chunk\n"));
        let plan = catalog_over(directory.path(), &fixture, EVERY_KIND)
            .plan_with("writers", &served(), Some(ResetMode::Rebuild))
            .unwrap_or_else(|diagnostics| panic!("{diagnostics:?}"));
        let seeded = |branch: &str, table: &str, rows| SeededTable {
            branch: branch.into(),
            table: table.into(),
            rows,
        };
        let side = vec![seeded("side0", "Chunk", 17)];
        let links = vec![seeded("main", "Next", 3), seeded("side0", "Next", 3)];
        let sample = against_the_product(&plan, 5, side, links).await;
        assert_eq!(sample.outcome, "expectations-passed");
        let sample = serde_json::to_value(&sample).unwrap();
        let block = &sample["concurrent"];
        let window_us = number(block, "window_us");
        let labels = block["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 4, "one row per label, in declaration order");
        let written = assert_label(&labels[0], "writer", "load", 2, window_us);
        assert_label(&labels[1], "reader", "query", 1, window_us);
        assert_label(&labels[2], "toucher", "mutate", 1, window_us);
        assert_label(&labels[3], "folder", "branch merge", 1, window_us);
        assert_eq!(
            number(block, "readback.rows"),
            written,
            "every acknowledged dealt row is read back"
        );
    }
}
