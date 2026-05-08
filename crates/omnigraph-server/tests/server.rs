use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use lance_index::traits::DatasetIndexExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest,
    IngestRequest, ReadRequest, SchemaApplyRequest, SchemaOutput,
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
      actions: [branch_delete, branch_merge]
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

const INGEST_CREATE_ONLY_POLICY_YAML: &str = r#"
version: 1
groups:
  team: [act-bruno]
protected_branches: [main]
rules:
  - id: team-branch-create
    allow:
      actors: { group: team }
      actions: [branch_create]
      target_branch_scope: unprotected
"#;

const SCHEMA_APPLY_POLICY_YAML: &str = r#"
version: 1
groups:
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: admins-schema-apply
    allow:
      actors: { group: admins }
      actions: [schema_apply]
      target_branch_scope: protected
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

async fn init_repo_with_schema(schema: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo_path(temp.path());
    fs::create_dir_all(&repo).unwrap();
    Omnigraph::init(repo.to_str().unwrap(), schema)
        .await
        .unwrap();
    temp
}

fn repo_path(root: &Path) -> PathBuf {
    root.join("server.omni")
}

fn drifted_test_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?")
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

async fn app_for_repo_with_auth_tokens_and_policy(
    schema: &str,
    tokens: &[(&str, &str)],
    policy: &str,
) -> (tempfile::TempDir, Router) {
    let temp = init_repo_with_schema(schema).await;
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

fn additive_schema_with_nickname() -> String {
    fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    )
}

fn renamed_person_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("node Person {\n", "node Human @rename_from(\"Person\") {\n")
        .replace("edge Knows: Person -> Person", "edge Knows: Human -> Human")
        .replace(
            "edge WorksAt: Person -> Company",
            "edge WorksAt: Human -> Company",
        )
}

fn renamed_age_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "years: I32? @rename_from(\"age\")")
}

fn indexed_name_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("name: String @key", "name: String @key @index")
}

fn unsupported_schema_change() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?")
}

async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

#[tokio::test]
async fn schema_apply_route_updates_repo_for_authorized_admin() {
    let (temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let schema = additive_schema_with_nickname();

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: schema,
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let repo = repo_path(temp.path());
    let reopened = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    assert!(
        reopened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[tokio::test]
async fn schema_apply_route_requires_schema_apply_policy_permission() {
    let (_temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Forbidden).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_requires_bearer_token_when_policy_enabled() {
    let (_temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Unauthorized).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_can_rename_type() {
    let (temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: renamed_person_schema(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let repo = repo_path(temp.path());
    let reopened = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(snapshot.entry("node:Human").is_some());
    assert!(snapshot.entry("node:Person").is_none());
}

#[tokio::test]
async fn schema_apply_route_can_rename_property() {
    let (temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: renamed_age_schema(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let repo = repo_path(temp.path());
    let reopened = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let person = &reopened.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[tokio::test]
async fn schema_apply_route_can_add_index() {
    let (temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let repo = repo_path(temp.path());
    let before_index_count = {
        let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open("node:Person").await.unwrap();
        dataset.load_indices().await.unwrap().len()
    };

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: indexed_name_schema(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let reopened = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let dataset = snapshot.open("node:Person").await.unwrap();
    let after_index_count = dataset.load_indices().await.unwrap().len();
    assert!(after_index_count > before_index_count);
}

#[tokio::test]
async fn schema_apply_route_rejects_unsupported_plan() {
    let (_temp, app) = app_for_repo_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: unsupported_schema_change(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::BadRequest).unwrap()
    );
}

#[tokio::test]
async fn schema_apply_route_rejects_when_non_main_branch_exists() {
    let temp = init_repo_with_schema(&fs::read_to_string(fixture("test.pg")).unwrap()).await;
    let repo = repo_path(temp.path());
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create("feature").await.unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, SCHEMA_APPLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![("act-ragnor".to_string(), "admin-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: additive_schema_with_nickname(),
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        payload["code"],
        serde_json::to_value(omnigraph_server::api::ErrorCode::Conflict).unwrap()
    );
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
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    match option_env!("OMNIGRAPH_SOURCE_VERSION") {
        Some(source_version) => assert_eq!(body["source_version"], source_version),
        None => assert!(body.get("source_version").is_none()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_drift_returns_conflict_for_snapshot_read_and_change() {
    let (temp, app) = app_for_loaded_repo().await;
    let repo = repo_path(temp.path());
    fs::write(repo.join("_schema.pg"), drifted_test_schema()).unwrap();

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let snapshot_error: ErrorOutput = serde_json::from_value(snapshot_body).unwrap();
    assert_eq!(snapshot_status, StatusCode::CONFLICT);
    assert_eq!(
        snapshot_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        snapshot_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );

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
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&read).unwrap()))
            .unwrap(),
    )
    .await;
    let read_error: ErrorOutput = serde_json::from_value(read_body).unwrap();
    assert_eq!(read_status, StatusCode::CONFLICT);
    assert_eq!(
        read_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        read_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );

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
    let change_error: ErrorOutput = serde_json::from_value(change_body).unwrap();
    assert_eq!(change_status, StatusCode::CONFLICT);
    assert_eq!(
        change_error.code,
        Some(omnigraph_server::api::ErrorCode::Conflict)
    );
    assert!(
        change_error
            .error
            .contains("schema evolution is locked down in phase 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_require_bearer_token() {
    let (_temp, app) = app_for_loaded_repo_with_auth("demo-token").await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/branches")
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
            .uri("/branches")
            .method(Method::GET)
            .header("authorization", "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["branches"].is_array());
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
            .uri("/branches")
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["branches"].is_array());
}

/// Verifies the hashed-token lookup correctly resolves each bearer to its
/// associated actor, and that the resolved actor — not the handler-supplied
/// default — is what the policy engine sees. Two tokens for two distinct
/// actors; policy grants read to actor-A only. Swapping tokens must swap
/// the policy outcome.
#[tokio::test(flavor = "multi_thread")]
async fn bearer_token_resolves_to_correct_actor_for_policy_decisions() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(
        &policy_path,
        r#"
version: 1
groups:
  readers: [act-a]
  writers: [act-b]
protected_branches: [main]
rules:
  - id: readers-only
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#,
    )
    .unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![
            ("act-a".to_string(), "token-a".to_string()),
            ("act-b".to_string(), "token-b".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    // act-a is authenticated AND authorized.
    let (ok_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ok_status, StatusCode::OK);

    // act-b is authenticated but policy rejects — proves the resolved actor
    // (not some default) was the policy subject.
    let (denied_status, denied_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let denied_error: ErrorOutput = serde_json::from_value(denied_body).unwrap();
    assert_eq!(denied_status, StatusCode::FORBIDDEN);
    assert_eq!(
        denied_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );

    // Unknown token: 401, never reaches the policy engine.
    let (bad_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer wrong-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(bad_status, StatusCode::UNAUTHORIZED);
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
    assert_eq!(
        body["target"]["snapshot"].as_str(),
        read.snapshot.as_deref()
    );
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
async fn schema_route_returns_current_source() {
    let (_temp, app) = app_for_loaded_repo().await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/schema")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let output: SchemaOutput = serde_json::from_value(body).unwrap();
    assert!(output.schema_source.contains("node Person"));
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_requires_bearer_token_when_auth_configured() {
    let (_temp, app) = app_for_loaded_repo_with_auth("demo-token").await;

    let (missing_status, missing_body) = json_response(
        &app,
        Request::builder()
            .uri("/schema")
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

    let (ok_status, ok_body) = json_response(
        &app,
        Request::builder()
            .uri("/schema")
            .method(Method::GET)
            .header("authorization", "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ok_status, StatusCode::OK);
    let output: SchemaOutput = serde_json::from_value(ok_body).unwrap();
    assert!(!output.schema_source.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_denied_when_actor_lacks_read_permission() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    // Policy grants branch_create only — no read action for act-bruno.
    fs::write(&policy_path, INGEST_CREATE_ONLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        repo.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/schema")
            .method(Method::GET)
            .header("authorization", "Bearer team-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );
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
async fn authenticated_change_stamps_actor_on_commits() {
    // With the Run state machine removed, actor_id is recorded
    // directly on the commit graph (no intermediate run record).
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
async fn ingest_creates_branch_returns_metadata_and_stamps_actor() {
    let (temp, app) = app_for_loaded_repo_with_auth_tokens(&[("act-andrew", "token-one")]).await;
    let repo = repo_path(temp.path());
    let ingest = IngestRequest {
        branch: Some("feature-ingest".to_string()),
        from: Some("main".to_string()),
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}
{"type":"Person","data":{"name":"Bob","age":26}}"#
            .to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/ingest")
            .method(Method::POST)
            .header("authorization", "Bearer token-one")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["branch"], "feature-ingest");
    assert_eq!(body["base_branch"], "main");
    assert_eq!(body["branch_created"], true);
    assert_eq!(body["mode"], "merge");
    assert_eq!(body["actor_id"], "act-andrew");
    assert_eq!(body["tables"][0]["table_key"], "node:Person");
    assert_eq!(body["tables"][0]["rows_loaded"], 2);

    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let snapshot = db
        .snapshot_of(ReadTarget::branch("feature-ingest"))
        .await
        .unwrap();
    let person_ds = snapshot.open("node:Person").await.unwrap();
    assert_eq!(person_ds.count_rows(None).await.unwrap(), 5);
    let head = db
        .list_commits(Some("feature-ingest"))
        .await
        .unwrap()
        .into_iter()
        .last()
        .unwrap();
    assert_eq!(head.actor_id.as_deref(), Some("act-andrew"));
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_existing_branch_skips_branch_create_policy_check() {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    {
        let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
        db.branch_create_from(ReadTarget::branch("main"), "feature")
            .await
            .unwrap();
    }
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
    let ingest = IngestRequest {
        branch: Some("feature".to_string()),
        from: Some("other-base".to_string()),
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#.to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/ingest")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["branch"], "feature");
    assert_eq!(body["branch_created"], false);
    assert_eq!(body["base_branch"], "other-base");
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_denies_missing_branch_without_branch_create_permission() {
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token")],
        POLICY_YAML,
    )
    .await;
    let ingest = IngestRequest {
        branch: Some("feature".to_string()),
        from: Some("main".to_string()),
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#.to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/ingest")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_denies_when_actor_lacks_change_permission() {
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token")],
        INGEST_CREATE_ONLY_POLICY_YAML,
    )
    .await;
    let ingest = IngestRequest {
        branch: Some("feature".to_string()),
        from: Some("main".to_string()),
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#.to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/ingest")
            .method(Method::POST)
            .header("authorization", "Bearer team-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_rejects_payloads_over_32_mib() {
    let (_temp, app) = app_for_loaded_repo().await;
    let oversize = IngestRequest {
        branch: Some("feature".to_string()),
        from: Some("main".to_string()),
        mode: Some(LoadMode::Merge),
        data: "x".repeat(33 * 1024 * 1024),
    };

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingest")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&oversize).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
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
async fn remote_branch_delete_flow_works() {
    let (_temp, app) = app_for_loaded_repo().await;

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, _) = json_response(
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

    let (delete_status, delete_body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/feature")
            .method(Method::DELETE)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(delete_body["name"], "feature");

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
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_delete_denies_without_policy_permission() {
    let (temp, app) = app_for_loaded_repo_with_auth_tokens_and_policy(
        &[("act-andrew", "token-admin"), ("act-bruno", "token-team")],
        POLICY_YAML,
    )
    .await;
    let repo = repo_path(temp.path());

    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/branches/feature")
            .method(Method::DELETE)
            .header("authorization", "Bearer token-team")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("policy denied action 'branch_delete'")
    );
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
async fn change_conflict_returns_manifest_conflict_409() {
    // A write that races with another writer surfaces as HTTP 409 with
    // a structured `manifest_conflict` body — `table_key`, `expected`,
    // and `actual` — so clients can detect-and-retry without parsing
    // the message.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());

    // Build the server first so its handle pins the pre-mutation manifest
    // version. Then advance the manifest from outside the server. The
    // server's next /change call will capture stale `expected_versions`
    // (from its still-pinned snapshot) and the publisher's CAS rejects.
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    {
        let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
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
    }

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&ChangeRequest {
                    query_source: MUTATION_QUERIES.to_string(),
                    query_name: Some("set_age".to_string()),
                    params: Some(json!({ "name": "Alice", "age": 33 })),
                    branch: Some("main".to_string()),
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::Conflict));
    let conflict = error
        .manifest_conflict
        .expect("publisher CAS rejection must populate manifest_conflict body");
    assert_eq!(conflict.table_key, "node:Person");
    assert!(
        conflict.actual > conflict.expected,
        "actual ({}) should be ahead of expected ({})",
        conflict.actual,
        conflict.expected,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_concurrent_inserts_same_key_serialize_without_409() {
    // PR 2 Phase 2 (MR-686): pin the design fix for the same-key
    // concurrency hazard. Pre-fix, in-process concurrent inserts on
    // the same `(table, branch)` rejected with 409 manifest_conflict
    // because `ensure_expected_version` fired before the per-table
    // queue was acquired and saw Lance HEAD already advanced by a
    // peer writer. Post-fix, Insert/Merge skip the strict pre-stage
    // check (see `MutationOpKind::strict_pre_stage_version_check`);
    // the queue serializes commit_staged; Lance's natural rebase
    // handles the in-flight stage; the publisher's CAS on a fresh
    // per-branch snapshot under the queue catches genuine cross-
    // process drift.
    //
    // This test spawns N concurrent /change inserts on a single
    // node type and asserts: every request returns 200 (no 409),
    // and the final row count equals the seed count + N (every
    // staged batch actually committed).
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    // test.jsonl seeds 4 Persons (Alice, Bob, Charlie, Diana).
    const SEED_PERSON_ROWS: u64 = 4;
    const N: usize = 12;

    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: MUTATION_QUERIES.to_string(),
                query_name: Some("insert_person".to_string()),
                params: Some(json!({ "name": format!("racer-{i}"), "age": i as i32 })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let req = Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(req).await.unwrap();
            response.status()
        }));
    }

    let mut statuses = Vec::with_capacity(N);
    for h in handles {
        statuses.push(h.await.unwrap());
    }

    let bad: Vec<_> = statuses
        .iter()
        .enumerate()
        .filter(|(_, s)| **s != StatusCode::OK)
        .collect();
    assert!(
        bad.is_empty(),
        "expected every concurrent insert to return 200, got non-200 for: {:?}",
        bad
    );

    // Verify the inserts actually landed. The status check above only proves
    // the publisher CAS didn't reject; the row count proves none of the
    // concurrent commits silently overwrote a peer.
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
    let person_rows = snapshot_body["tables"]
        .as_array()
        .and_then(|tables| {
            tables
                .iter()
                .find(|t| t["table_key"].as_str() == Some("node:Person"))
        })
        .and_then(|t| t["row_count"].as_u64())
        .expect("snapshot must include node:Person row_count");
    assert_eq!(
        person_rows,
        SEED_PERSON_ROWS + N as u64,
        "expected {} seeded + {} concurrent inserts = {} Person rows; got {}",
        SEED_PERSON_ROWS,
        N,
        SEED_PERSON_ROWS + N as u64,
        person_rows,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_concurrent_updates_same_key_serialize_via_publisher_cas() {
    // Pin Update RYW semantics under in-process concurrency on the same
    // `(table, branch)`. With per-table queue serialization and op-kind-aware
    // drift detection at commit time, exactly one of N concurrent UPDATEs
    // on the same row commits; the rest are rejected as 409 manifest_conflict.
    //
    // Pre-fix bug class: in `MutationStaging::commit_all`, after queue
    // acquisition, the staged Lance transaction is handed straight to
    // `commit_staged`. For a writer whose staged dataset is at V0 but
    // Lance HEAD has advanced to V1 (because the queue's prior winner
    // already published), Lance's transaction conflict resolver fires
    // `RetryableCommitConflict` on Update vs Update on the same row.
    // That error gets wrapped as `OmniError::Lance(<string>)` and the
    // API surfaces it as **500 internal**, not 409. Users see "internal
    // server error" instead of a retryable conflict, breaking the
    // documented 409 contract for in-process drift.
    //
    // Post-fix invariant: `commit_all` does an op-kind-aware drift check
    // before each `commit_staged`. For tables whose tracked op_kind has
    // `strict_pre_stage_version_check() == true` (Update / Delete /
    // SchemaRewrite), if the staged dataset's version doesn't match the
    // fresh manifest pin, return `OmniError::manifest_expected_version_mismatch`
    // → 409 ExpectedVersionMismatch. The N-1 losers see a clean 409
    // before Lance's commit_staged ever runs.
    //
    // Why correct-by-design: closing the class "Lance internal conflict
    // surfaces as 500 instead of 409" rather than mapping the specific
    // Lance error variant. The drift check fires at the right architectural
    // layer (engine boundary, under the queue) and respects the existing
    // `MutationOpKind` policy.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    // Spawn N=8 concurrent UPDATEs on Alice (from test.jsonl, age=30 at V0)
    // writing distinct ages.
    const N: usize = 8;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let app = app.clone();
        let target_age = 100 + i as i32;
        handles.push(tokio::spawn(async move {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: MUTATION_QUERIES.to_string(),
                query_name: Some("set_age".to_string()),
                params: Some(json!({ "name": "Alice", "age": target_age })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let req = Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(req).await.unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, body.to_vec())
        }));
    }

    let mut results = Vec::with_capacity(N);
    for h in handles {
        results.push(h.await.unwrap());
    }
    let statuses: Vec<StatusCode> = results.iter().map(|(s, _)| *s).collect();

    let ok_count = statuses
        .iter()
        .filter(|s| **s == StatusCode::OK)
        .count();
    let conflict_count = statuses
        .iter()
        .filter(|s| **s == StatusCode::CONFLICT)
        .count();
    let other: Vec<_> = statuses
        .iter()
        .enumerate()
        .filter(|(_, s)| **s != StatusCode::OK && **s != StatusCode::CONFLICT)
        .collect();

    let other_bodies: Vec<(usize, StatusCode, String)> = other
        .iter()
        .map(|(i, s)| {
            let body_str = String::from_utf8_lossy(&results[*i].1).to_string();
            (*i, **s, body_str)
        })
        .collect();
    assert!(
        other.is_empty(),
        "expected only 200 or 409 statuses, got non-200/409 entries: {:?}",
        other_bodies
    );
    assert_eq!(
        ok_count + conflict_count,
        N,
        "all responses must be 200 or 409 to satisfy the RYW invariant; statuses: {:?}",
        statuses
    );
    assert_eq!(
        ok_count, 1,
        "expected exactly one update to commit and N-1 to receive 409 manifest_conflict \
         (op-kind-aware drift check rejects stale-V0 staged datasets at commit_all entry). \
         Got {} OK + {} 409 + {} other. \
         Pre-fix symptom: 1 OK + (N-1) x 500 because Lance's RetryableCommitConflict for \
         Update vs Update on the same row bubbles up as `OmniError::Lance(<string>)` and \
         the API maps it to 500 internal, not 409. Statuses: {:?}",
        ok_count,
        conflict_count,
        statuses.len() - ok_count - conflict_count,
        statuses,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_branch_create_from_distinct_parents_does_not_corrupt_coordinator() {
    // Pin the swap-restore atomicity invariant in `branch_create_from`. The
    // pre-fix implementation used three separate `coordinator.write().await`
    // acquisitions: swap → operate → restore. Under `&self` concurrency, two
    // calls `branch_create_from(alpha, gamma)` and `branch_create_from(beta,
    // delta)` could interleave such that A's "operate" step sees B's swapped
    // coordinator (beta), forking gamma off beta's HEAD instead of alpha's
    // HEAD, and the restore step left coordinator pointing at the wrong
    // branch for subsequent operations.
    //
    // Pre-fix symptom (race-dependent, sometimes manifests): gamma's row
    // count matches beta's HEAD instead of alpha's, OR delta's row count
    // matches alpha's instead of beta's.
    //
    // Post-fix invariant (correct by design, AGENTS.md rule 9): hold one
    // `coordinator.write().await` guard across the entire swap-operate-
    // restore sequence so the three steps are atomic relative to other
    // `branch_create_from` callers.
    //
    // Setup: main has 4 Persons (test.jsonl). Create alpha forked from main
    // and add a 5th Person to alpha (alpha: 5 Persons). Beta forks from main
    // and stays untouched (beta: 4 Persons). Then concurrently fork gamma
    // from alpha and delta from beta. Verify each fork inherits its
    // declared parent's row count.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    // Helper: POST /branches { from, name } and assert 200.
    async fn create_branch(app: &Router, from: &str, name: &str) {
        let body = serde_json::to_vec(&BranchCreateRequest {
            from: Some(from.to_string()),
            name: name.to_string(),
        })
        .unwrap();
        let req = Request::builder()
            .uri("/branches")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "branch_create {} -> {} failed",
            from,
            name,
        );
    }

    // Helper: POST /change to add a new Person on a branch.
    async fn insert_person(app: &Router, branch: &str, name: &str, age: i32) {
        let body = serde_json::to_vec(&ChangeRequest {
            query_source: MUTATION_QUERIES.to_string(),
            query_name: Some("insert_person".to_string()),
            params: Some(json!({ "name": name, "age": age })),
            branch: Some(branch.to_string()),
        })
        .unwrap();
        let req = Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "insert_person on {} failed",
            branch,
        );
    }

    // Helper: GET /snapshot?branch=<branch> and return Person row count.
    async fn person_row_count(app: &Router, branch: &str) -> u64 {
        let uri = format!("/snapshot?branch={}", branch);
        let req = Request::builder()
            .uri(uri)
            .method(Method::GET)
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "snapshot {} failed", branch);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let tables = value["tables"].as_array().unwrap();
        let person_table = tables
            .iter()
            .find(|t| t["table_key"].as_str() == Some("node:Person"))
            .unwrap_or_else(|| panic!("snapshot of {} missing node:Person", branch));
        person_table["row_count"].as_u64().unwrap()
    }

    // Setup. Main: 4 Persons (Alice, Bob, Charlie, Diana from test.jsonl).
    create_branch(&app, "main", "alpha").await;
    insert_person(&app, "alpha", "Eve", 22).await;
    create_branch(&app, "main", "beta").await;

    let alpha_count = person_row_count(&app, "alpha").await;
    let beta_count = person_row_count(&app, "beta").await;
    assert_eq!(alpha_count, 5, "alpha should have 5 Persons after Eve insert");
    assert_eq!(beta_count, 4, "beta should have 4 Persons (untouched main fork)");

    // Concurrent forks: many gamma_i from alpha, many delta_i from beta.
    // M=8 fork pairs to amplify race-catching odds; the race is inherently
    // timing-dependent so a single pair would flake on cold runs.
    const M: usize = 8;
    let mut handles = Vec::with_capacity(M * 2);
    for i in 0..M {
        let app_a = app.clone();
        let gamma_name = format!("gamma-{i}");
        handles.push(tokio::spawn(async move {
            create_branch(&app_a, "alpha", &gamma_name).await;
            gamma_name
        }));
        let app_b = app.clone();
        let delta_name = format!("delta-{i}");
        handles.push(tokio::spawn(async move {
            create_branch(&app_b, "beta", &delta_name).await;
            delta_name
        }));
    }

    let mut created = Vec::with_capacity(M * 2);
    for h in handles {
        created.push(h.await.unwrap());
    }
    assert_eq!(created.len(), M * 2);

    // Assertion: every fork inherits its declared parent's row count.
    // Pre-fix: under the race, some gamma_i may report 4 (beta's count) or
    // some delta_i may report 5 (alpha's count) because the operate step
    // ran against the wrong swapped coordinator.
    let mut mismatches: Vec<(String, u64, u64)> = Vec::new();
    for i in 0..M {
        let gamma = format!("gamma-{i}");
        let count = person_row_count(&app, &gamma).await;
        if count != alpha_count {
            mismatches.push((gamma, count, alpha_count));
        }
        let delta = format!("delta-{i}");
        let count = person_row_count(&app, &delta).await;
        if count != beta_count {
            mismatches.push((delta, count, beta_count));
        }
    }
    assert!(
        mismatches.is_empty(),
        "branches forked off the wrong parent under the swap-restore race; \
         (branch, observed_count, expected_count): {:?}",
        mismatches,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_change_during_branch_merge_preserves_writes() {
    // Future-proof against MR-895 work that may move or remove the
    // per-(table, branch) writer queue acquisition inside `branch_merge`
    // (`crates/omnigraph/src/exec/merge.rs:1224`). Today the queue
    // linearizes a concurrent /change on main against branch_merge
    // feature → main on the same touched tables; both succeed and B's
    // row is preserved post-merge.
    //
    // Codex flagged a P1 in PR #75 review claiming the merge could
    // silently overwrite concurrent target writes because the
    // source-rewrite path opens with `MutationOpKind::Merge` (skipping
    // the strict pre-stage check). Validation by subagent showed the
    // queue at merge.rs:1224 is held across both Phase B (per-table
    // commit_staged) and Phase C (manifest publish), so there's no
    // interleave window. The Merge op_kind only affects same-process
    // pre-stage drift detection, not cross-write linearization.
    //
    // This test is the regression pin that catches a future change
    // which drops the queue acquisition and admits the silent overwrite.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    // test.jsonl: 4 Persons on main.
    const SEED_PERSONS: u64 = 4;

    // Create feature branch + insert one Person on feature.
    let create_body = serde_json::to_vec(&BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    })
    .unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/branches")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let feature_insert = serde_json::to_vec(&ChangeRequest {
        query_source: MUTATION_QUERIES.to_string(),
        query_name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Eve", "age": 22 })),
        branch: Some("feature".to_string()),
    })
    .unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(feature_insert))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Concurrent: insert on main + merge feature → main. The queue
    // linearizes them on the (node:Person, main) key; both succeed.
    let app_change = app.clone();
    let change_handle = tokio::spawn(async move {
        let body = serde_json::to_vec(&ChangeRequest {
            query_source: MUTATION_QUERIES.to_string(),
            query_name: Some("insert_person".to_string()),
            params: Some(json!({ "name": "Frank", "age": 33 })),
            branch: Some("main".to_string()),
        })
        .unwrap();
        let req = Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        app_change.oneshot(req).await.unwrap().status()
    });

    let app_merge = app.clone();
    let merge_handle = tokio::spawn(async move {
        let body = serde_json::to_vec(&BranchMergeRequest {
            source: "feature".to_string(),
            target: Some("main".to_string()),
        })
        .unwrap();
        let req = Request::builder()
            .uri("/branches/merge")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        app_merge.oneshot(req).await.unwrap().status()
    });

    let change_status = change_handle.await.unwrap();
    let merge_status = merge_handle.await.unwrap();
    assert_eq!(change_status, StatusCode::OK, "concurrent /change failed");
    assert_eq!(merge_status, StatusCode::OK, "concurrent /branches/merge failed");

    // Post-condition: main has SEED + Eve (from feature) + Frank (inserted).
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let person_rows = body["tables"]
        .as_array()
        .and_then(|tables| {
            tables
                .iter()
                .find(|t| t["table_key"].as_str() == Some("node:Person"))
        })
        .and_then(|t| t["row_count"].as_u64())
        .expect("snapshot must include node:Person row_count");
    assert_eq!(
        person_rows,
        SEED_PERSONS + 2, // +1 from feature merge (Eve), +1 from concurrent /change (Frank)
        "post-merge main must include both the merge result (Eve) and the \
         concurrent insert (Frank); pre-fix race would lose one of them",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_branch_merges_distinct_targets_do_not_swap_into_each_other() {
    // Pin the `branch_merge_impl` swap-restore atomicity invariant.
    // Round 2 of the PR review left this race deferred: `merge.rs:1085-1100`
    // uses three separate `coordinator.write().await` acquisitions
    // (swap → operate → restore) — the same shape that
    // `branch_create_from_impl` shed in round 1.
    //
    // Pre-fix race: two concurrent `branch_merge` calls A and B with
    // distinct targets target_a, target_b. A's swap captures the
    // currently-bound coord as previous_A and replaces self.coordinator
    // with target_a. Before A's operate runs, B's swap captures
    // (now) target_a as previous_B and replaces self.coordinator with
    // target_b. A's `branch_merge_on_current_target` then runs against
    // target_b's coord — A merges its source INTO target_b, not target_a.
    //
    // Post-fix invariant: branch_merge_impl serializes via
    // `merge_exclusive: Arc<tokio::sync::Mutex<()>>` held across the
    // entire swap-operate-restore window. Other writers (the per-table
    // queue, /change, /ingest) are unaffected — only concurrent merges
    // serialize.
    //
    // Setup: main + 2 source branches (feature_a with Eve, feature_b with
    // Frank) + 2 untouched targets (target_a, target_b). Concurrent
    // merges feature_a→target_a and feature_b→target_b. Aligned at a
    // tokio::sync::Barrier so both swaps land close in time.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    async fn do_create_branch(app: &Router, from: &str, name: &str) {
        let body = serde_json::to_vec(&BranchCreateRequest {
            from: Some(from.to_string()),
            name: name.to_string(),
        })
        .unwrap();
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/branches")
                    .method(Method::POST)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "create {} from {} failed", name, from);
    }

    async fn do_insert_person(app: &Router, branch: &str, name: &str, age: i32) {
        let body = serde_json::to_vec(&ChangeRequest {
            query_source: MUTATION_QUERIES.to_string(),
            query_name: Some("insert_person".to_string()),
            params: Some(json!({ "name": name, "age": age })),
            branch: Some(branch.to_string()),
        })
        .unwrap();
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/change")
                    .method(Method::POST)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "insert {} on {} failed", name, branch);
    }

    async fn person_count(app: &Router, branch: &str) -> u64 {
        let uri = format!("/snapshot?branch={}", branch);
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "snapshot {} failed", branch);
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        v["tables"]
            .as_array()
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|t| t["table_key"].as_str() == Some("node:Person"))
            })
            .and_then(|t| t["row_count"].as_u64())
            .unwrap_or_else(|| panic!("snapshot {} missing node:Person", branch))
    }

    // test.jsonl seeds 4 Persons.
    const SEED: u64 = 4;

    // Set up 4 child branches: 2 sources (each with one new Person), 2
    // targets (forked from main, untouched).
    do_create_branch(&app, "main", "feature-a").await;
    do_insert_person(&app, "feature-a", "Eve", 22).await;
    do_create_branch(&app, "main", "feature-b").await;
    do_insert_person(&app, "feature-b", "Frank", 33).await;
    do_create_branch(&app, "main", "target-a").await;
    do_create_branch(&app, "main", "target-b").await;

    assert_eq!(person_count(&app, "feature-a").await, SEED + 1);
    assert_eq!(person_count(&app, "feature-b").await, SEED + 1);
    assert_eq!(person_count(&app, "target-a").await, SEED);
    assert_eq!(person_count(&app, "target-b").await, SEED);

    // Concurrent merges aligned at a barrier so both reach the
    // swap_coordinator_for_branch call close in time. Repeat M=4 times to
    // boost the probability of catching the race. Recreate the
    // target/source branches each iteration to keep the post-condition
    // checks tight.
    const M: usize = 4;
    for iter in 0..M {
        let target_a = format!("target-a-iter{iter}");
        let target_b = format!("target-b-iter{iter}");
        let source_a = format!("feature-a-iter{iter}");
        let source_b = format!("feature-b-iter{iter}");

        do_create_branch(&app, "main", &source_a).await;
        do_insert_person(&app, &source_a, &format!("Eve-{iter}"), 22).await;
        do_create_branch(&app, "main", &source_b).await;
        do_insert_person(&app, &source_b, &format!("Frank-{iter}"), 33).await;
        do_create_branch(&app, "main", &target_a).await;
        do_create_branch(&app, "main", &target_b).await;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let app_a = app.clone();
        let app_b = app.clone();
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);
        let target_a_owned = target_a.clone();
        let target_b_owned = target_b.clone();
        let source_a_owned = source_a.clone();
        let source_b_owned = source_b.clone();

        let h_a = tokio::spawn(async move {
            barrier_a.wait().await;
            let body = serde_json::to_vec(&BranchMergeRequest {
                source: source_a_owned,
                target: Some(target_a_owned),
            })
            .unwrap();
            app_a
                .oneshot(
                    Request::builder()
                        .uri("/branches/merge")
                        .method(Method::POST)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        });
        let h_b = tokio::spawn(async move {
            barrier_b.wait().await;
            let body = serde_json::to_vec(&BranchMergeRequest {
                source: source_b_owned,
                target: Some(target_b_owned),
            })
            .unwrap();
            app_b
                .oneshot(
                    Request::builder()
                        .uri("/branches/merge")
                        .method(Method::POST)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        });

        assert_eq!(h_a.await.unwrap(), StatusCode::OK, "iter {iter} merge A");
        assert_eq!(h_b.await.unwrap(), StatusCode::OK, "iter {iter} merge B");

        // Post-condition: each target has exactly its declared source's
        // contribution. Pre-fix race: A's merge runs against target_b's
        // swapped coord, so feature-a's row lands in target-b instead of
        // target-a. Observable as target-a == 4 (unchanged) and
        // target-b == 6 (both Eve and Frank).
        let count_a = person_count(&app, &target_a).await;
        let count_b = person_count(&app, &target_b).await;
        assert_eq!(
            count_a,
            SEED + 1,
            "iter {iter}: target-a must reflect feature-a's merge \
             (Eve only); pre-fix race would leave it at SEED ({}) and \
             pile both feature-a's and feature-b's rows into target-b. \
             Got target-a={count_a}, target-b={count_b}",
            SEED,
        );
        assert_eq!(
            count_b,
            SEED + 1,
            "iter {iter}: target-b must reflect feature-b's merge \
             (Frank only); pre-fix race would leave it at SEED+2 if A's \
             merge ran against target-b's swapped coord. \
             Got target-a={count_a}, target-b={count_b}",

        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_disjoint_table_concurrency_succeeds_at_http_level() {
    // HTTP-level pin for MR-686's disjoint-table promise: concurrent /change
    // requests touching different node types must coexist without admission
    // rejection or publisher-CAS conflict. The bench harness measures
    // throughput; this test is the regression sentinel that catches a
    // future change which accidentally re-introduces graph-wide
    // serialization on the disjoint path.
    //
    // Setup: test.jsonl seeds 4 Persons + 2 Companies. Spawn N=4 concurrent
    // /change inserts on `node:Person` and N=4 concurrent inserts on
    // `node:Company`. All 8 must return 200, and the post-test row counts
    // must reflect every insert.
    const PERSON_QUERY: &str = r#"
query insert_p($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#;
    const COMPANY_QUERY: &str = r#"
query insert_c($name: String) {
    insert Company { name: $name }
}
"#;
    const SEED_PERSONS: u64 = 4;
    const SEED_COMPANIES: u64 = 2;
    const PER_TYPE: usize = 4;

    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    let mut handles = Vec::with_capacity(PER_TYPE * 2);
    for i in 0..PER_TYPE {
        let app_p = app.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: PERSON_QUERY.to_string(),
                query_name: Some("insert_p".to_string()),
                params: Some(json!({ "name": format!("p-{i}"), "age": i as i32 })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let req = Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            app_p.oneshot(req).await.unwrap().status()
        }));
        let app_c = app.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: COMPANY_QUERY.to_string(),
                query_name: Some("insert_c".to_string()),
                params: Some(json!({ "name": format!("c-{i}") })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let req = Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            app_c.oneshot(req).await.unwrap().status()
        }));
    }

    let mut statuses = Vec::with_capacity(PER_TYPE * 2);
    for h in handles {
        statuses.push(h.await.unwrap());
    }

    let bad: Vec<_> = statuses
        .iter()
        .enumerate()
        .filter(|(_, s)| **s != StatusCode::OK)
        .collect();
    assert!(
        bad.is_empty(),
        "expected every disjoint /change insert to return 200, got non-200 for: {:?}",
        bad,
    );

    // Verify both tables landed every insert.
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let lookup_count = |table_key: &str| -> u64 {
        body["tables"]
            .as_array()
            .and_then(|tables| tables.iter().find(|t| t["table_key"].as_str() == Some(table_key)))
            .and_then(|t| t["row_count"].as_u64())
            .unwrap_or_else(|| panic!("snapshot missing {}", table_key))
    };
    assert_eq!(
        lookup_count("node:Person"),
        SEED_PERSONS + PER_TYPE as u64,
        "Person row count after concurrent inserts",
    );
    assert_eq!(
        lookup_count("node:Company"),
        SEED_COMPANIES + PER_TYPE as u64,
        "Company row count after concurrent inserts",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ingest_per_actor_admission_cap_returns_429() {
    // Pin the admission gate on `/ingest`. With per-actor in-flight cap of 1
    // and 8 concurrent requests from the same actor, at least one request
    // must be rejected with HTTP 429 and `code: too_many_requests`.
    //
    // Pre-fix bug class: the admission pattern at `server_change`
    // (`crates/omnigraph-server/src/lib.rs:932`) was the only handler
    // that called `WorkloadController::try_admit`. A heavy actor sending
    // bulk-ingest traffic would exhaust shared engine capacity (Lance I/O
    // threads, manifest churn) without ever hitting an admission cap.
    // Pinned at the HTTP boundary so future refactors that drop the
    // try_admit call from a mutating handler turn this red.
    //
    // Post-fix invariant: `/ingest`, `/branches/create`, `/branches/delete`,
    // `/branches/merge`, and `/schema/apply` all gate on
    // `state.workload.try_admit(&actor_arc, est_bytes)` after Cedar
    // authorization and before the engine call. Cap exhaustion surfaces as
    // 429 with `code: too_many_requests`.
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX", Some("1")),
        ("OMNIGRAPH_PER_ACTOR_BYTES_MAX", Some("1000000000")),
    ]);
    let (_temp, app) = app_for_loaded_repo_with_auth_tokens(&[("act-flooder", "flooder-token")]).await;

    // Eight concurrent ingests, all from act-flooder. Only one fits in a
    // cap=1 in-flight semaphore; the others must 429.
    const N: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(N));
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let app = app.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            // Align the 8 tasks at the barrier so they all attempt
            // try_admit close in time.
            barrier.wait().await;

            let body = serde_json::to_vec(&IngestRequest {
                data: format!(
                    "{{\"type\":\"Person\",\"data\":{{\"name\":\"flooder-{i}\",\"age\":{i}}}}}\n"
                ),
                branch: Some("main".to_string()),
                from: Some("main".to_string()),
                mode: Some(omnigraph::loader::LoadMode::Merge),
            })
            .unwrap();
            let req = Request::builder()
                .uri("/ingest")
                .method(Method::POST)
                .header("authorization", "Bearer flooder-token")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(req).await.unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, headers, body.to_vec())
        }));
    }

    let mut results = Vec::with_capacity(N);
    for h in handles {
        results.push(h.await.unwrap());
    }
    let statuses: Vec<StatusCode> = results.iter().map(|(s, _, _)| *s).collect();

    let too_many: Vec<usize> = statuses
        .iter()
        .enumerate()
        .filter(|(_, s)| **s == StatusCode::TOO_MANY_REQUESTS)
        .map(|(i, _)| i)
        .collect();
    assert!(
        !too_many.is_empty(),
        "expected at least one /ingest under cap=1 to return 429; got statuses: {:?}",
        statuses,
    );

    // Validate the structured error body for each 429 (body must carry
    // the `too_many_requests` code so clients can distinguish it from
    // generic conflicts).
    for i in &too_many {
        let body_value: Value = serde_json::from_slice(&results[*i].2).unwrap();
        let error: ErrorOutput = serde_json::from_value(body_value).unwrap();
        assert_eq!(
            error.code,
            Some(omnigraph_server::api::ErrorCode::TooManyRequests),
            "429 body must carry code=too_many_requests; idx {} got {:?}",
            i,
            error.code,
        );
    }

    // Validate the `Retry-After` header is set on every 429. Pinned by
    // the same test so a future refactor that drops the header from
    // `IntoResponse for ApiError` turns this red. The constant
    // matches `crates/omnigraph-server/src/lib.rs::ApiError::into_response`.
    for i in &too_many {
        let retry_after = results[*i]
            .1
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        assert!(
            retry_after.is_some(),
            "429 response must include a Retry-After header; idx {} headers were: {:?}",
            i,
            results[*i].1,
        );
    }
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
