use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest, ReadRequest,
};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use serial_test::serial;
use tower::ServiceExt;

const MUTATION_QUERIES: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#;

const POLICY_YAML: &str = r#"
version: 1
groups:
  team: [act-andrew, act-bruno, act-ragnor]
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: admins-export
    allow:
      actors: { group: admins }
      actions: [export]
      branch_scope: any
  - id: team-write-unprotected
    allow:
      actors: { group: team }
      actions: [change]
      branch_scope: unprotected
  - id: admins-merge
    allow:
      actors: { group: admins }
      actions: [branch_merge]
      target_branch_scope: protected
  - id: admins-publish
    allow:
      actors: { group: admins }
      actions: [run_publish]
      target_branch_scope: protected
"#;

const POLICY_PROTECTED_READ_YAML: &str = r#"
version: 1
groups:
  team: [act-bruno]
protected_branches: [main]
rules:
  - id: protected-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: protected
"#;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph/tests/fixtures")
        .join(name)
}

async fn init_loaded_repo() -> tempfile::TempDir {
    init_repo_with_schema_and_data(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &fs::read_to_string(fixture("test.jsonl")).unwrap(),
    )
    .await
}

async fn init_repo_with_schema_and_data(schema: &str, data: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo_path(temp.path());
    fs::create_dir_all(&repo).unwrap();
    Omnigraph::init(repo.to_str().unwrap(), schema)
        .await
        .unwrap();
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    temp
}

fn repo_path(root: &Path) -> PathBuf {
    root.join("server.omni")
}

async fn manifest_dataset_version(repo: &Path) -> u64 {
    Omnigraph::open(repo.to_string_lossy().as_ref())
        .await
        .unwrap()
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version()
}

fn s3_test_repo_uri(suite: &str) -> Option<String> {
    let bucket = env::var("OMNIGRAPH_S3_TEST_BUCKET").ok()?;
    let prefix = env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some(format!("s3://{}/{}/{}/{}", bucket, prefix, suite, unique))
}

async fn app_for_loaded_repo() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    (temp, build_app(state))
}

async fn app_for_loaded_repo_with_auth(token: &str) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_token(
        repo.to_string_lossy().to_string(),
        db,
        Some(token.to_string()),
    );
    (temp, build_app(state))
}

async fn app_for_loaded_repo_with_auth_tokens(
    tokens: &[(&str, &str)],
) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_tokens(
        repo.to_string_lossy().to_string(),
        db,
        tokens
            .iter()
            .map(|(actor, token)| ((*actor).to_string(), (*token).to_string()))
            .collect(),
    );
    (temp, build_app(state))
}

async fn app_for_loaded_repo_with_auth_tokens_and_policy(
    tokens: &[(&str, &str)],
    policy: &str,
) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, policy).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        tokens
            .iter()
            .map(|(actor, token)| ((*actor).to_string(), (*token).to_string()))
            .collect(),
        Some(&policy_path),
    )
    .await
    .unwrap();
    (temp, build_app(state))
}

async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, env::var(name).ok()))
            .collect::<Vec<_>>();
        for (name, value) in vars {
            unsafe {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..) {
            unsafe {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }
}

fn format_vector(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| format!("{:.8}", value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn normalize_vector(mut values: Vec<f32>) -> Vec<f32> {
    let norm = values
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 14695981039346656037u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211u64);
    }
    hash
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

fn mock_embedding(input: &str, dim: usize) -> Vec<f32> {
    let mut seed = fnv1a64(input.as_bytes());
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        seed = xorshift64(seed);
        let ratio = (seed as f64 / u64::MAX as f64) as f32;
        out.push((ratio * 2.0) - 1.0);
    }
    normalize_vector(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn healthz_succeeds_after_startup() {
    let (_temp, app) = app_for_loaded_repo().await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/healthz")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_require_bearer_token() {
    let (_temp, app) = app_for_loaded_repo_with_auth("demo-token").await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/runs")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Unauthorized)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_accept_valid_bearer_token_while_healthz_stays_open() {
    let (_temp, app) = app_for_loaded_repo_with_auth("demo-token").await;

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/runs")
            .method(Method::GET)
            .header("authorization", "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["runs"].is_array());
}

#[tokio::test(flavor = "multi_thread")]
async fn export_route_returns_jsonl_for_branch_snapshot() {
    let token = "demo-token";
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"Eve","age":29}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let expected = db
        .export_jsonl("feature", &["Person".to_string()], &[])
        .await
        .unwrap();
    drop(db);

    let state = AppState::new_with_bearer_token(
        repo.to_string_lossy().to_string(),
        Omnigraph::open(repo.to_str().unwrap()).await.unwrap(),
        Some(token.to_string()),
    );
    let app = build_app(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/export")
                .method(Method::POST)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", token))
                .body(Body::from(
                    serde_json::to_vec(&ExportRequest {
                        branch: Some("feature".to_string()),
                        type_names: vec!["Person".to_string()],
                        table_keys: Vec::new(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-ndjson; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(text, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_accept_any_configured_team_bearer_token() {
    let (_temp, app) =
        app_for_loaded_repo_with_auth_tokens(&[("team-01", "token-one"), ("team-02", "token-two")])
            .await;

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/runs")
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["runs"].is_array());
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_allows_read_but_distinguishes_401_from_403() {
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token"), ("act-ragnor", "admin-token")],
        POLICY_YAML,
    )
    .await;

    let (missing_status, missing_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let missing_error: ErrorOutput = serde_json::from_value(missing_body).unwrap();
    assert_eq!(missing_status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_error.code,
        Some(omnigraph_server::api::ErrorCode::Unauthorized)
    );

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(snapshot_status, StatusCode::OK);
    assert_eq!(snapshot_body["branch"], "main");

    let export_request = ExportRequest {
        branch: Some("main".to_string()),
        type_names: Vec::new(),
        table_keys: Vec::new(),
    };
    let (forbidden_status, forbidden_body) = json_response(
        &app,
        Request::builder()
            .uri("/export")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&export_request).unwrap()))
            .unwrap(),
    )
    .await;
    let forbidden_error: ErrorOutput = serde_json::from_value(forbidden_body).unwrap();
    assert_eq!(forbidden_status, StatusCode::FORBIDDEN);
    assert_eq!(
        forbidden_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/export")
                .method(Method::POST)
                .header("authorization", "Bearer admin-token")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&export_request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_uses_resolved_branch_for_snapshot_reads() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let snapshot_id = {
        let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
        db.resolve_snapshot("main").await.unwrap().to_string()
    };
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_PROTECTED_READ_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let read = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: None,
        snapshot: Some(snapshot_id),
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["target"]["branch"], Value::Null);
    assert_eq!(body["target"]["snapshot"].as_str(), read.snapshot.as_deref());
    assert_eq!(body["row_count"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_route_returns_manifest_dataset_version() {
    let (temp, app) = app_for_loaded_repo().await;
    let repo = repo_path(temp.path());
    let expected_manifest_version = manifest_dataset_version(&repo).await;

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(snapshot_status, StatusCode::OK);
    assert_eq!(snapshot_body["branch"], "main");
    assert_eq!(
        snapshot_body["manifest_version"].as_u64().unwrap(),
        expected_manifest_version
    );
    assert!(snapshot_body["tables"].is_array());
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_blocks_change_on_protected_main_but_allows_unprotected_branch() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let main_change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
    };
    let (main_status, main_body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&main_change).unwrap()))
            .unwrap(),
    )
    .await;
    let main_error: ErrorOutput = serde_json::from_value(main_body).unwrap();
    assert_eq!(main_status, StatusCode::FORBIDDEN);
    assert_eq!(
        main_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let feature_change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("feature".to_string()),
    };
    let (feature_status, feature_body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&feature_change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(feature_status, StatusCode::OK);
    assert_eq!(feature_body["branch"], "feature");
    assert_eq!(feature_body["affected_nodes"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_blocks_non_admin_merge_to_main_and_allows_admin() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![
            ("act-bruno".to_string(), "team-token".to_string()),
            ("act-ragnor".to_string(), "admin-token".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
    };
    let (deny_status, deny_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    let deny_error: ErrorOutput = serde_json::from_value(deny_body).unwrap();
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(
        deny_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let (allow_status, allow_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("authorization", "Bearer admin-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);
    assert_eq!(allow_body["actor_id"], "act-ragnor");
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_blocks_non_admin_run_publish_to_main() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let run_id = {
        let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
        db.begin_run("main", Some("policy-publish"))
            .await
            .unwrap()
            .run_id
            .as_str()
            .to_string()
    };

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![
            ("act-bruno".to_string(), "team-token".to_string()),
            ("act-ragnor".to_string(), "admin-token".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let (deny_status, deny_body) = json_response(
        &app,
        Request::builder()
            .uri(format!("/runs/{run_id}/publish"))
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let deny_error: ErrorOutput = serde_json::from_value(deny_body).unwrap();
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(
        deny_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    let (allow_status, allow_body) = json_response(
        &app,
        Request::builder()
            .uri(format!("/runs/{run_id}/publish"))
            .method(Method::POST)
            .header("authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);
    assert_eq!(allow_body["target_branch"], "main");
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_change_stamps_actor_on_runs_and_commits() {
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens(&[("act-andrew", "token-one")]).await;

    let change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);
    assert_eq!(change_body["actor_id"], "act-andrew");

    let (runs_status, runs_body) = json_response(
        &app,
        Request::builder()
            .uri("/runs")
            .method(Method::GET)
            .header("authorization", "Bearer token-one")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(runs_status, StatusCode::OK);
    let run = runs_body["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|run| run["operation_hash"] == "mutation:insert_person:branch=main")
        .expect("mutation run should be present");
    assert_eq!(run["actor_id"], "act-andrew");
    assert_eq!(run["status"], "published");

    let (commits_status, commits_body) = json_response(
        &app,
        Request::builder()
            .uri("/commits?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-one")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(commits_status, StatusCode::OK);
    let head = commits_body["commits"]
        .as_array()
        .unwrap()
        .last()
        .expect("head commit should exist");
    assert_eq!(head["actor_id"], "act-andrew");
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_branch_merge_stamps_merge_actor_on_head_commit() {
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens(&[
        ("act-andrew", "token-one"),
        ("act-ragnor", "token-two"),
    ])
    .await;

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/branches")
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(create_status, StatusCode::OK);

    let change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Zoe", "age": 33 })),
        branch: Some("feature".to_string()),
    };
    let (change_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("authorization", "Bearer token-two")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["actor_id"], "act-ragnor");

    let (commit_status, commit_body) = json_response(
        &app,
        Request::builder()
            .uri("/commits?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(commit_status, StatusCode::OK);
    let head = commit_body["commits"]
        .as_array()
        .unwrap()
        .last()
        .expect("head commit should exist");
    assert_eq!(head["actor_id"], "act-ragnor");
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_merge_conflict_response_includes_structured_conflicts() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "set_age",
        &omnigraph_compiler::json_params_to_param_map(
            Some(&json!({"name": "Alice", "age": 31 })),
            &omnigraph_compiler::find_named_query(MUTATION_QUERIES, "set_age")
                .unwrap()
                .params,
            omnigraph_compiler::JsonParamMode::Standard,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &omnigraph_compiler::json_params_to_param_map(
            Some(&json!({"name": "Alice", "age": 32 })),
            &omnigraph_compiler::find_named_query(MUTATION_QUERIES, "set_age")
                .unwrap()
                .params,
            omnigraph_compiler::JsonParamMode::Standard,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    drop(db);

    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);
    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;

    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::Conflict));
    assert!(error.error.contains("merge conflict"));
    assert!(error.merge_conflicts.iter().any(|conflict| {
        conflict.table_key == "node:Person"
            && conflict.row_id.as_deref() == Some("Alice")
            && conflict.kind == omnigraph_server::api::MergeConflictKindOutput::DivergentUpdate
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_read_after_change_sees_updated_state_from_same_app() {
    let (_temp, app) = app_for_loaded_repo().await;

    let change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);
    assert_eq!(change_body["affected_nodes"], 1);

    let read = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Mina" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(read_body["row_count"], 1);
    assert_eq!(read_body["rows"][0]["p.name"], "Mina");
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_branch_list_create_merge_flow_works() {
    let (_temp, app) = app_for_loaded_repo().await;

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_body["branches"], json!(["main"]));

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, create_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(create_status, StatusCode::OK);
    assert_eq!(create_body["from"], "main");
    assert_eq!(create_body["name"], "feature");

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_body["branches"], json!(["feature", "main"]));

    let change = ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Zoe", "age": 33 })),
        branch: Some("feature".to_string()),
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);
    assert_eq!(change_body["branch"], "feature");
    assert_eq!(change_body["affected_nodes"], 1);

    let read_main_before = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Zoe" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read_main_before).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(read_body["row_count"], 0);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["source"], "feature");
    assert_eq!(merge_body["target"], "main");
    assert_eq!(merge_body["outcome"], "fast_forward");

    let read_main_after = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Zoe" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read_main_after).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(read_body["row_count"], 1);
    assert_eq!(read_body["rows"][0]["p.name"], "Zoe");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_opens_s3_repo_directly_and_serves_snapshot_and_read() {
    let Some(uri) = s3_test_repo_uri("server") else {
        eprintln!("skipping s3 server test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    Omnigraph::init(&uri, &fs::read_to_string(fixture("test.pg")).unwrap())
        .await
        .unwrap();
    let mut db = Omnigraph::open(&uri).await.unwrap();
    load_jsonl(
        &mut db,
        &fs::read_to_string(fixture("test.jsonl")).unwrap(),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let app = build_app(
        AppState::open_with_bearer_token(uri.clone(), Some("s3-token".to_string()))
            .await
            .unwrap(),
    );

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot")
            .method(Method::GET)
            .header("authorization", "Bearer s3-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(snapshot_status, StatusCode::OK);
    assert!(snapshot_body["tables"].is_array());

    let read = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("authorization", "Bearer s3-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(read_body["row_count"], 1);
    assert_eq!(read_body["rows"][0]["p.name"], "Alice");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn remote_read_embeds_string_nearest_queries_with_mock_runtime() {
    const EMBED_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    title: String @index
    embedding: Vector(4) @index
}
"#;
    const EMBED_QUERY: &str = r#"
query vector_search_string($q: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#;

    let alpha = mock_embedding("alpha", 4);
    let beta = mock_embedding("beta", 4);
    let gamma = mock_embedding("gamma", 4);
    let data = format!(
        concat!(
            r#"{{"type":"Doc","data":{{"slug":"alpha-doc","title":"alpha guide","embedding":[{}]}}}}"#,
            "\n",
            r#"{{"type":"Doc","data":{{"slug":"beta-doc","title":"beta guide","embedding":[{}]}}}}"#,
            "\n",
            r#"{{"type":"Doc","data":{{"slug":"gamma-doc","title":"gamma handbook","embedding":[{}]}}}}"#
        ),
        format_vector(&alpha),
        format_vector(&beta),
        format_vector(&gamma),
    );

    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("GEMINI_API_KEY", None),
    ]);
    let temp = init_repo_with_schema_and_data(EMBED_SCHEMA, &data).await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    let read = ReadRequest {
        query_source: EMBED_QUERY.to_string(),
        query_name: Some("vector_search_string".to_string()),
        params: Some(json!({ "q": "alpha" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/read")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["row_count"], 3);
    assert_eq!(body["rows"][0]["d.slug"], "alpha-doc");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_run_returns_not_found() {
    let (_temp, app) = app_for_loaded_repo().await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/runs/missing-run")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::NotFound));
    assert!(error.error.contains("run 'missing-run' not found"));
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_conflict_returns_conflict_status() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();

    let run_a = db
        .begin_run("main", Some("server-conflict-a"))
        .await
        .unwrap();
    let run_b = db
        .begin_run("main", Some("server-conflict-b"))
        .await
        .unwrap();
    db.mutate(
        &run_a.run_branch,
        MUTATION_QUERIES,
        "set_age",
        &omnigraph_compiler::json_params_to_param_map(
            Some(&json!({"name": "Alice", "age": 31 })),
            &omnigraph_compiler::find_named_query(MUTATION_QUERIES, "set_age")
                .unwrap()
                .params,
            omnigraph_compiler::JsonParamMode::Standard,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    db.mutate(
        &run_b.run_branch,
        MUTATION_QUERIES,
        "set_age",
        &omnigraph_compiler::json_params_to_param_map(
            Some(&json!({"name": "Alice", "age": 32 })),
            &omnigraph_compiler::find_named_query(MUTATION_QUERIES, "set_age")
                .unwrap()
                .params,
            omnigraph_compiler::JsonParamMode::Standard,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    db.publish_run(&run_a.run_id).await.unwrap();
    drop(db);

    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(format!("/runs/{}/publish", run_b.run_id.as_str()))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(b"{}" as &[u8]))
            .unwrap(),
    )
    .await;

    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::Conflict));
    assert!(error.merge_conflicts.iter().any(|conflict| {
        conflict.table_key == "node:Person"
            && conflict.row_id.as_deref() == Some("Alice")
            && conflict.kind == omnigraph_server::api::MergeConflictKindOutput::DivergentUpdate
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_request_body_returns_payload_too_large() {
    let (_temp, app) = app_for_loaded_repo().await;
    let oversized = "x".repeat(1_100_000);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/read")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(oversized))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
