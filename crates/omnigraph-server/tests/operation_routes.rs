//! Recorded-operation routes: submit, look up and cancel an optimize run
//! through the serving owner, and the operation record that answers them.
//!
//! A separate binary: the journey re-executes it to run a production server
//! that holds the cluster's admission, which the record requires.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use omnigraph_server::{AppState, OperationSettings, build_app};
use serde_json::{Value, json};

mod support;
use support::*;

const ROOT: &str = "OMNIGRAPH_OPERATIONS_TEST_ROOT";
/// When set, the served child parks the first optimize run at the seam
/// `HOLD_SEAM` names until this file exists.
const HOLD_RELEASE: &str = "OMNIGRAPH_OPERATIONS_TEST_HOLD_RELEASE";
const HOLD_SEAM: &str = "OMNIGRAPH_OPERATIONS_TEST_HOLD_SEAM";
/// After the run's `running` write, before any table work.
const BEFORE_TABLE_WORK: &str = "optimize.post_authority_capture_pre_gates";
/// After the run's publication, before the engine's final cancellation sample.
const AFTER_PUBLICATION: &str = "optimize.configuration_clear";
const OPERATION_ID: &str = "01JC0000000000000000000001";

const POLICY: &str = r#"
version: 1
groups:
  operators: [operator]
  readers: [reader]
rules:
  - id: maintain
    allow:
      actors: { group: operators }
      actions: [maintain, read]
      branch_scope: any
  - id: read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#;

fn post(uri: &str, token: &str, body: Value) -> Request<Body> {
    Request::post(uri)
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

async fn single_graph_state(settings: Option<OperationSettings>) -> (tempfile::TempDir, AppState) {
    let temp = init_loaded_graph().await;
    let policy = temp.path().join("policy.yaml");
    fs::write(&policy, POLICY).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph_path(temp.path()).to_string_lossy().to_string(),
        vec![
            ("operator".into(), "op-secret".into()),
            ("reader".into(), "read-secret".into()),
        ],
        Some(&policy),
    )
    .await
    .unwrap();
    let state = match settings {
        Some(settings) => state.with_operation_settings(settings),
        None => state,
    };
    (temp, state)
}

/// Off by default; an actor without `maintain` is refused first; a server
/// without cluster admission has no record to keep, and refuses before the
/// ceiling is checked.
#[tokio::test]
async fn routes_refuse_without_a_grant_an_operator_switch_or_a_cluster() {
    let route = "/graphs/default/operations/optimize";
    let body = json!({ "operation_id": OPERATION_ID });

    let (_dir, state) = single_graph_state(None).await;
    let app = build_app(state);
    let (status, error) = json_response(&app, post(route, "read-secret", body.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let (status, error) = json_response(&app, post(route, "op-secret", body.clone())).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{error}");
    assert_eq!(error["code"], "service_unavailable", "{error}");

    let enabled = OperationSettings {
        enabled: true,
        ..OperationSettings::default()
    };
    let (_dir, state) = single_graph_state(Some(enabled)).await;
    let app = build_app(state);
    let (status, error) = json_response(
        &app,
        post(route, "op-secret", json!({ "operation_id": "not-a-ulid" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    // The ceiling is admission's last check (RFC §3.2): without a cluster the
    // record refuses first; the journey below covers the 400.
    let (status, error) = json_response(&app, post(route, "op-secret", body)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert!(
        error["error"].as_str().unwrap().contains("v2 cluster"),
        "{error}"
    );
}

struct ContainedChild(Child);

impl Drop for ContainedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn applied_cluster() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("people.pg"),
        "node Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    fs::write(temp.path().join("policy.yaml"), POLICY).unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\npolicies:\n  access:\n    file: ./policy.yaml\n    applies_to: [knowledge]\n",
    )
    .unwrap();
    temp
}

/// Start the served child; `hold` parks the first optimize run at the named
/// seam until the release file exists.
fn spawn_server(root: &Path, hold: Option<(&str, &Path)>) -> (ContainedChild, String) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    if let Some((seam, release)) = hold {
        command.env(HOLD_SEAM, seam).env(HOLD_RELEASE, release);
    }
    let mut child = ContainedChild(
        command
            .args(["--exact", "served_child", "--ignored", "--nocapture"])
            .env(ROOT, root)
            .env("OMNIGRAPH_OPERATION_ROUTES", "true")
            .env(
                "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON",
                r#"{"operator":"op-secret","reader":"read-secret"}"#,
            )
            .env_remove("OMNIGRAPH_SERVER_BEARER_TOKEN")
            .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE")
            .env_remove("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (listen_tx, listen_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(address) = line.strip_prefix(omnigraph_server::LISTEN_ADDR_PREFIX) {
                let _ = listen_tx.send(address.to_string());
            }
        }
    });
    let address = listen_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the served child starts its listener");
    (child, address)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "instrument: subprocess helper for recorded_optimize_keeps_a_record_a_caller_reads_by_id"]
async fn served_child() {
    let Some(root) = std::env::var_os(ROOT).map(PathBuf::from) else {
        return;
    };
    // A parked run: the first optimize stops at the named seam until the
    // parent creates the release file.
    let _scenario = omnigraph::seams::FailScenario::setup();
    let _hold = std::env::var_os(HOLD_RELEASE).map(|release| {
        let release = PathBuf::from(release);
        let seam = std::env::var(HOLD_SEAM).unwrap();
        let (installed, hold) = omnigraph::seams::catalog::decide(&seam)
            .unwrap_or_else(|| panic!("no decision seam {seam}"))
            .hold();
        std::thread::spawn(move || {
            while !release.exists() {
                std::thread::sleep(Duration::from_millis(20));
            }
            hold.release();
        });
        installed
    });
    let mut config = cluster_settings(&root).await.unwrap();
    config.bind = "127.0.0.1:0".into();
    omnigraph_server::serve(config).await.unwrap();
}

struct Client {
    http: reqwest::Client,
    base: String,
}

impl Client {
    async fn send(&self, request: reqwest::RequestBuilder) -> (u16, Value) {
        let response = request
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn submit(&self, token: &str, body: Value) -> (u16, Value) {
        self.send(
            self.http
                .post(format!(
                    "{}/graphs/knowledge/operations/optimize",
                    self.base
                ))
                .bearer_auth(token)
                .json(&body),
        )
        .await
    }

    async fn lookup(&self, id: &str) -> (u16, Value) {
        self.send(
            self.http
                .get(format!("{}/graphs/knowledge/operations/{id}", self.base))
                .bearer_auth("op-secret"),
        )
        .await
    }

    async fn cancel(&self, id: &str) -> (u16, Value) {
        self.send(
            self.http
                .post(format!(
                    "{}/graphs/knowledge/operations/{id}/cancel",
                    self.base
                ))
                .bearer_auth("op-secret"),
        )
        .await
    }

    async fn wait_ready(&self, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(response) = self.http.get(format!("{}/readyz", self.base)).send().await
                && response.status().is_success()
            {
                return;
            }
            assert!(child.try_wait().unwrap().is_none(), "server exited");
            assert!(Instant::now() < deadline, "server never became ready");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The journey: a served run is accepted, runs under its owner and ends
/// `succeeded` with its commit; the same request answers the same record,
/// another request under the id is refused, an unknown id is `unknown`, and
/// the record on storage names the cluster's ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_optimize_keeps_a_record_a_caller_reads_by_id() {
    let temp = applied_cluster();
    apply_cluster_fixture(temp.path()).await;
    {
        // Three loads leave three small fragments for the run to compact.
        let db = session(
            omnigraph::db::Omnigraph::open(
                temp.path().join("graphs/knowledge.omni").to_str().unwrap(),
            )
            .await
            .unwrap(),
        );
        for name in ["Ada", "Grace", "Edsger"] {
            db.load_jsonl(
                &json!({"type": "Person", "data": {"name": name}}).to_string(),
                omnigraph::loader::LoadMode::Merge,
            )
            .await
            .unwrap();
        }
    }
    let (mut child, address) = spawn_server(temp.path(), None);
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;

    let body = json!({ "operation_id": OPERATION_ID, "max_gate_hold_ms": 30_000 });
    let (status, refused) = client.submit("read-secret", body.clone()).await;
    assert_eq!(status, 403, "{refused}");
    let (status, refused) = client
        .submit(
            "op-secret",
            json!({ "operation_id": OPERATION_ID, "max_gate_hold_ms": 3_600_000 }),
        )
        .await;
    assert_eq!(status, 400, "a gate hold over the ceiling: {refused}");
    assert_eq!(refused["code"], "bad_request", "{refused}");

    let (status, accepted) = client.submit("op-secret", body.clone()).await;
    assert!(matches!(status, 200 | 202), "{status}: {accepted}");
    assert_eq!(accepted["lookup"], "found", "{accepted}");

    let deadline = Instant::now() + Duration::from_secs(60);
    let terminal = loop {
        let (status, found) = client.lookup(OPERATION_ID).await;
        assert_eq!(status, 200, "{found}");
        let state = found["operation"]["state"].as_str().unwrap().to_string();
        if matches!(
            state.as_str(),
            "succeeded" | "failed" | "cancelled" | "interrupted" | "unsettled"
        ) {
            break found;
        }
        assert!(Instant::now() < deadline, "the run never finished: {found}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let operation = &terminal["operation"];
    assert_eq!(operation["state"], "succeeded", "{terminal}");
    assert_eq!(terminal["in_progress"], false, "{terminal}");
    assert!(operation["published_commit"].is_string(), "{terminal}");
    // RFC §2.2: the terminal record keeps the intent nonce the `publishing`
    // write stored, and the published commit answers it.
    let nonce = operation["nonce"]
        .as_str()
        .expect("terminal record keeps the nonce");
    assert!(
        operation["published_commit"]
            .as_str()
            .unwrap()
            .ends_with(&format!(".{nonce}")),
        "{terminal}"
    );
    assert!(
        operation["outcome"]["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table["type_key"] == "node:Person" && table["outcome"] == "optimized"),
        "{terminal}"
    );
    assert_eq!(operation["principal"], "operator", "{terminal}");

    let (status, repeated) = client.submit("op-secret", body.clone()).await;
    assert_eq!(status, 200, "{repeated}");
    assert_eq!(
        repeated["operation"], *operation,
        "a repeat answers the record"
    );
    let (status, refused) = client
        .submit(
            "op-secret",
            json!({ "operation_id": OPERATION_ID, "datasets": ["node:Person"] }),
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .starts_with("[operation_request_mismatch]"),
        "{refused}"
    );

    let (status, unknown) = client.lookup("01JC0000000000000000000009").await;
    assert_eq!(status, 200, "{unknown}");
    assert_eq!(unknown["lookup"], "unknown", "{unknown}");
    let (status, unchanged) = client.cancel(OPERATION_ID).await;
    assert_eq!(status, 200, "{unchanged}");
    assert_eq!(unchanged["operation"], *operation);

    let record: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/operations.json")).unwrap())
            .unwrap();
    let ledger: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    assert_eq!(record["ledger_id"], ledger["ledger_id"]);
    assert_eq!(record["terminal"][0]["operation_id"], OPERATION_ID);
    assert!(record.get("active").is_none(), "{record}");
    drop(child);
}

/// With the run parked after its `running` write: a repeat of the same request
/// answers the running record (202, in progress) instead of 429, and another
/// id is refused 429 naming the active one. Then the record is made unreadable
/// while the run finishes, so its final write fails; the known ending is kept
/// and applied by the next lookup once the record reads again, so the run is
/// `succeeded`, never `interrupted`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_known_ending_survives_a_failed_final_write_and_a_repeat_answers_the_running_record() {
    let temp = applied_cluster();
    apply_cluster_fixture(temp.path()).await;
    let release = temp.path().join("release-the-run");
    let (mut child, address) = spawn_server(temp.path(), Some((BEFORE_TABLE_WORK, &release)));
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;
    let record_path = temp.path().join("__cluster/operations.json");

    let body = json!({ "operation_id": OPERATION_ID });
    let (status, accepted) = client.submit("op-secret", body.clone()).await;
    assert_eq!(status, 202, "{accepted}");
    // The run reaches the seam after its `running` write.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, found) = client.lookup(OPERATION_ID).await;
        if found["operation"]["state"] == "running" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the run never reached running: {found}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let (status, repeated) = client.submit("op-secret", body.clone()).await;
    assert_eq!(
        status, 202,
        "a repeat answers the running record: {repeated}"
    );
    assert_eq!(repeated["operation"]["state"], "running", "{repeated}");
    assert_eq!(repeated["in_progress"], true, "{repeated}");
    let (status, refused) = client
        .submit(
            "op-secret",
            json!({ "operation_id": "01JC0000000000000000000002" }),
        )
        .await;
    assert_eq!(status, 429, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains(&format!("operation {OPERATION_ID} is active")),
        "the refusal names the active id: {refused}"
    );

    // The final write meets an unreadable record, so it cannot land.
    let intact = fs::read(&record_path).unwrap();
    fs::write(&record_path, b"not a record").unwrap();
    fs::write(&release, b"").unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    fs::write(&record_path, &intact).unwrap();

    let (status, found) = client.lookup(OPERATION_ID).await;
    assert_eq!(status, 200, "{found}");
    assert_eq!(
        found["operation"]["state"], "succeeded",
        "the kept ending is applied once the record reads again: {found}"
    );
    assert!(
        found["operation"]["outcome"]["datasets"]
            .as_array()
            .is_some_and(|tables| tables
                .iter()
                .any(|table| table["type_key"] == "node:Person")),
        "the ending carries its table outcomes: {found}"
    );
    assert_eq!(found["in_progress"], false, "{found}");
    drop(child);
}

/// How a test breaks the reads of one control object.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum ReadFault {
    /// A symbolic link to itself: every open fails with ELOOP, an I/O error
    /// with no typed kind, so the storage crate classifies it `Unknown`.
    SelfLink,
    /// Bytes that are not UTF-8: every read fails the same way (`Permanent`).
    NotText,
}

/// Break the reads of one control object. Returns the restore.
#[cfg(unix)]
fn break_reads(path: &Path, fault: ReadFault) -> impl FnOnce() + use<> {
    let aside = path.with_extension("json.intact");
    let existed = path.exists();
    if existed {
        fs::rename(path, &aside).unwrap();
    }
    match fault {
        ReadFault::SelfLink => std::os::unix::fs::symlink(path.file_name().unwrap(), path).unwrap(),
        ReadFault::NotText => fs::write(path, [0xff, 0xfe, 0xfd]).unwrap(),
    }
    let path = path.to_path_buf();
    move || {
        fs::remove_file(&path).unwrap();
        if existed {
            fs::rename(&aside, &path).unwrap();
        }
    }
}

/// A failed storage read of the cluster state file or of the operation record
/// comes before any record write, so submit, lookup and cancel answer it with
/// `effects: none`, never the 409 a request change needs. The storage crate's
/// class of the failure decides the rest: a failure with no typed evidence
/// (here ELOOP) may pass, so it is 503 `action: retry`; bytes that are not
/// text fail every read the same way, so they are 500, which does not tell
/// the caller to send the same request again. The same submission is accepted
/// once the store reads again.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_control_read_answers_by_its_storage_class_issue_1356() {
    let temp = applied_cluster();
    apply_cluster_fixture(temp.path()).await;
    let (mut child, address) = spawn_server(temp.path(), None);
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;
    let body = json!({ "operation_id": OPERATION_ID });

    for (fault, status, code, action) in [
        (ReadFault::SelfLink, 503, "service_unavailable", "retry"),
        (ReadFault::NotText, 500, "internal", "refresh"),
    ] {
        for object in ["__cluster/state.json", "__cluster/operations.json"] {
            let restore = break_reads(&temp.path().join(object), fault);
            for (route, (answered, refused)) in [
                ("submit", client.submit("op-secret", body.clone()).await),
                (
                    "submit again",
                    client.submit("op-secret", body.clone()).await,
                ),
                ("lookup", client.lookup(OPERATION_ID).await),
                ("cancel", client.cancel(OPERATION_ID).await),
            ] {
                let case = format!("{route} with {object} {fault:?}: {refused}");
                assert_eq!(answered, status, "{case}");
                assert_eq!(refused["code"], code, "{case}");
                assert_eq!(refused["disposition"]["effects"], "none", "{case}");
                assert_eq!(refused["disposition"]["action"], action, "{case}");
            }
            restore();
        }
    }

    let (status, unknown) = client.lookup(OPERATION_ID).await;
    assert_eq!(status, 200, "{unknown}");
    assert_eq!(
        unknown["lookup"], "unknown",
        "nothing was accepted: {unknown}"
    );
    let (status, accepted) = client.submit("op-secret", body).await;
    assert!(matches!(status, 200 | 202), "{status}: {accepted}");
    assert_eq!(accepted["lookup"], "found", "{accepted}");
    drop(child);
}

/// Wait until the run's record leaves the active states and the run is no
/// longer in progress; return the record.
async fn wait_terminal(client: &Client, id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, found) = client.lookup(id).await;
        assert_eq!(status, 200, "{found}");
        if found["in_progress"] == false
            && matches!(
                found["operation"]["state"].as_str(),
                Some("succeeded" | "failed" | "cancelled" | "interrupted" | "unsettled")
            )
        {
            return found["operation"].clone();
        }
        assert!(Instant::now() < deadline, "the run never finished: {found}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether a terminal record shows the operator's cancellation: the run
/// stopped, or it finished and says the cancellation came late (RFC §1.9).
fn shows_the_cancellation(terminal: &Value) -> bool {
    terminal["state"] == "cancelled"
        || (terminal["state"] == "succeeded"
            && terminal["reason"].as_str().is_some_and(|reason| {
                reason.starts_with("a cancellation arrived")
                    && reason.ends_with(": cancelled by operator")
            }))
}

/// A cancel answered 202 is never lost: the run it reached ends `cancelled`,
/// or `succeeded` with a reason saying the cancellation came late. The
/// engine samples its cancellation once more after its last boundary; a
/// cancel after that sample, while the record still shows the run active,
/// must reach the terminal record too, so the run decides its ending under
/// the record gate the route answers under. A run on a graph with nothing to
/// compact takes a few milliseconds, so the trials sweep the cancel across
/// it in quarter-millisecond steps; on the defect most 202 answers were lost.
// contract: E4
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_answered_202_reaches_the_terminal_record_issue_1554() {
    const TRIALS: u32 = 90;
    let temp = applied_cluster();
    apply_cluster_fixture(temp.path()).await;
    let (mut child, address) = spawn_server(temp.path(), None);
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;

    let mut answered_202 = 0;
    let mut lost = Vec::new();
    for trial in 0..TRIALS {
        // Each id has its own time, so an evicted one never expires the next.
        let id = format!("01JC000{trial:03}0000000000001554");
        let (status, accepted) = client
            .submit("op-secret", json!({ "operation_id": id }))
            .await;
        assert!(matches!(status, 200 | 202), "{status}: {accepted}");
        tokio::time::sleep(Duration::from_micros(u64::from(trial % 30) * 250)).await;
        let (cancel_status, cancelled) = client.cancel(&id).await;
        let terminal = wait_terminal(&client, &id).await;
        match cancel_status {
            202 => {
                answered_202 += 1;
                if !shows_the_cancellation(&terminal) {
                    lost.push(terminal);
                }
            }
            // Too late for the route: it answered the terminal record.
            200 => assert_eq!(cancelled["operation"], terminal, "{cancelled}"),
            _ => panic!("{cancel_status}: {cancelled}"),
        }
    }
    assert!(
        answered_202 > 0,
        "no cancel reached a run before its end, so the trials prove nothing"
    );
    assert!(
        lost.is_empty(),
        "{} of {answered_202} cancels answered 202 are missing from the terminal record: {lost:#?}",
        lost.len()
    );
    drop(child);
}

/// A cancel that reaches a live run whose record is `publishing` is answered
/// 202 with the record unchanged, and the run finishes its publication and
/// records that the cancellation came after it started.
// contract: E4
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_during_the_publication_is_noted_on_the_succeeded_record_issue_1554() {
    let temp = applied_cluster();
    apply_cluster_fixture(temp.path()).await;
    {
        // Two loads leave two small fragments, so the run publishes.
        let db = session(
            omnigraph::db::Omnigraph::open(
                temp.path().join("graphs/knowledge.omni").to_str().unwrap(),
            )
            .await
            .unwrap(),
        );
        for name in ["Ada", "Grace"] {
            db.load_jsonl(
                &json!({"type": "Person", "data": {"name": name}}).to_string(),
                omnigraph::loader::LoadMode::Merge,
            )
            .await
            .unwrap();
        }
    }
    let release = temp.path().join("release-the-run");
    let (mut child, address) = spawn_server(temp.path(), Some((AFTER_PUBLICATION, &release)));
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;

    let (status, accepted) = client
        .submit("op-secret", json!({ "operation_id": OPERATION_ID }))
        .await;
    assert_eq!(status, 202, "{accepted}");
    // The run is parked after its publication until the release file exists.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, found) = client.lookup(OPERATION_ID).await;
        if found["operation"]["state"] == "publishing" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the run never reached its publication: {found}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (status, answered) = client.cancel(OPERATION_ID).await;
    assert_eq!(status, 202, "{answered}");
    assert_eq!(answered["operation"]["state"], "publishing", "{answered}");
    assert_eq!(answered["in_progress"], true, "{answered}");

    fs::write(&release, b"").unwrap();
    let terminal = wait_terminal(&client, OPERATION_ID).await;
    assert_eq!(terminal["state"], "succeeded", "{terminal}");
    assert!(terminal["published_commit"].is_string(), "{terminal}");
    assert_eq!(
        terminal["reason"],
        "a cancellation arrived after the publication started: cancelled by operator",
        "{terminal}"
    );
    drop(child);
}
