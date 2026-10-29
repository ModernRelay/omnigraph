//! Served maintenance routes: submit, look up and cancel a served optimize
//! run, and the operation record that answers them.
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
use omnigraph_server::{AppState, MaintenanceSettings, build_app};
use serde_json::{Value, json};

mod support;
use support::*;

const ROOT: &str = "OMNIGRAPH_MAINTENANCE_TEST_ROOT";
const MAINTENANCE_ID: &str = "01JC0000000000000000000001";

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

async fn single_graph_state(
    settings: Option<MaintenanceSettings>,
) -> (tempfile::TempDir, AppState) {
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
        Some(settings) => state.with_maintenance_settings(settings),
        None => state,
    };
    (temp, state)
}

/// Off by default; an actor without `maintain` is refused first; a server
/// without cluster admission has no record to keep.
#[tokio::test]
async fn routes_refuse_without_a_grant_an_operator_switch_or_a_cluster() {
    let route = "/graphs/default/maintenance/optimize";
    let body = json!({ "maintenance_id": MAINTENANCE_ID });

    let (_dir, state) = single_graph_state(None).await;
    let app = build_app(state);
    let (status, error) = json_response(&app, post(route, "read-secret", body.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let (status, error) = json_response(&app, post(route, "op-secret", body.clone())).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{error}");
    assert_eq!(error["code"], "service_unavailable", "{error}");

    let enabled = MaintenanceSettings {
        enabled: true,
        ..MaintenanceSettings::default()
    };
    let (_dir, state) = single_graph_state(Some(enabled)).await;
    let app = build_app(state);
    let (status, error) = json_response(
        &app,
        post(
            route,
            "op-secret",
            json!({ "maintenance_id": "not-a-ulid" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let (status, error) = json_response(
        &app,
        post(
            route,
            "op-secret",
            json!({ "maintenance_id": MAINTENANCE_ID, "max_gate_hold_ms": 3_600_000 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{error}");
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

fn spawn_server(root: &Path) -> (ContainedChild, String) {
    let mut child = ContainedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "served_child", "--ignored", "--nocapture"])
            .env(ROOT, root)
            .env("OMNIGRAPH_MAINTENANCE_ROUTES", "true")
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
#[ignore = "instrument: subprocess helper for served_optimize_keeps_a_record_a_caller_reads_by_id"]
async fn served_child() {
    let Some(root) = std::env::var_os(ROOT).map(PathBuf::from) else {
        return;
    };
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
                    "{}/graphs/knowledge/maintenance/optimize",
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
                .get(format!("{}/graphs/knowledge/maintenance/{id}", self.base))
                .bearer_auth("op-secret"),
        )
        .await
    }

    async fn cancel(&self, id: &str) -> (u16, Value) {
        self.send(
            self.http
                .post(format!(
                    "{}/graphs/knowledge/maintenance/{id}/cancel",
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
async fn served_optimize_keeps_a_record_a_caller_reads_by_id() {
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
    let (mut child, address) = spawn_server(temp.path());
    let client = Client {
        http: reqwest::Client::new(),
        base: format!("http://{address}"),
    };
    client.wait_ready(&mut child.0).await;

    let body = json!({ "maintenance_id": MAINTENANCE_ID, "max_gate_hold_ms": 30_000 });
    let (status, refused) = client.submit("read-secret", body.clone()).await;
    assert_eq!(status, 403, "{refused}");

    let (status, accepted) = client.submit("op-secret", body.clone()).await;
    assert!(matches!(status, 200 | 202), "{status}: {accepted}");
    assert_eq!(accepted["lookup"], "found", "{accepted}");

    let deadline = Instant::now() + Duration::from_secs(60);
    let terminal = loop {
        let (status, found) = client.lookup(MAINTENANCE_ID).await;
        assert_eq!(status, 200, "{found}");
        let state = found["operation"]["state"].as_str().unwrap().to_string();
        if matches!(
            state.as_str(),
            "succeeded" | "failed" | "cancelled" | "interrupted"
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
    assert!(
        operation["tables"]
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
            json!({ "maintenance_id": MAINTENANCE_ID, "tables": ["node:Person"] }),
        )
        .await;
    assert_eq!(status, 409, "{refused}");

    let (status, unknown) = client.lookup("01JC0000000000000000000009").await;
    assert_eq!(status, 200, "{unknown}");
    assert_eq!(unknown["lookup"], "unknown", "{unknown}");
    let (status, unchanged) = client.cancel(MAINTENANCE_ID).await;
    assert_eq!(status, 200, "{unchanged}");
    assert_eq!(unchanged["operation"], *operation);

    let record: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/maintenance.json")).unwrap())
            .unwrap();
    let ledger: Value =
        serde_json::from_slice(&fs::read(temp.path().join("__cluster/state.json")).unwrap())
            .unwrap();
    assert_eq!(record["ledger_id"], ledger["ledger_id"]);
    assert_eq!(record["terminal"][0]["maintenance_id"], MAINTENANCE_ID);
    assert!(record.get("active").is_none(), "{record}");
    drop(child);
}
