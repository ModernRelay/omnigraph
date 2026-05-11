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

// ─────────────────────────────────────────────────────────────────────────
// Branch-ops morphological matrix
//
// Table-driven test covering all interesting (op_a, op_b, target_overlap)
// concurrent-pair cells with the C1-C6 invariants asserted uniformly:
//
//   C1 — both complete (no deadlock, no hang)
//   C2 — status: both 200, or exactly one clean conflict (409/429), no 500
//   C3 — per-target row count
//   C4 — per-target row identity (present + absent named persons)
//   C5 — engine state remains coherent (subsequent /snapshot is consistent)
//   C6 — post-op /change on main succeeds (engine state isn't poisoned)
//
// Cell list (a-k) below. Each cell uses a fresh tempdir + AppState so a
// failure in one doesn't leak into the next. Within a cell, ops align at
// a tokio::sync::Barrier so both reach the engine close in time, and the
// pair is wrapped in tokio::time::timeout(15s) so a deadlock surfaces
// as a clean panic.
//
// Replaces the three narrow concurrent_branch_* tests below; their
// scenarios are folded into cells f, h, i (branch_create_from race),
// cell a (merge race with C4 identity assertions), and cell d
// (concurrent change-during-merge).
// ─────────────────────────────────────────────────────────────────────────

mod matrix {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Barrier;

    #[derive(Debug)]
    pub(super) struct OpStatus {
        pub status: StatusCode,
        pub body: Vec<u8>,
    }

    pub(super) struct Harness {
        pub _temp: tempfile::TempDir,
        pub app: Router,
    }

    impl Harness {
        pub async fn new() -> Self {
            let temp = init_loaded_repo().await;
            let repo = repo_path(temp.path());
            // Build the WorkloadController explicitly with defaults rather
            // than letting `AppState::open` call
            // `WorkloadController::from_env()`. The admission-gate test
            // (`ingest_per_actor_admission_cap_returns_429`) sets
            // OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX=1 inside an EnvGuard while
            // it runs. Process-wide env vars are visible to
            // concurrently-running tests; if a matrix cell reads env at
            // AppState construction time during that window it picks up
            // cap=1 and the second concurrent merge in cell b surfaces
            // 429 instead of the expected 200. Constructing the
            // controller here with explicit defaults makes cells
            // independent of any env mutation other tests perform.
            let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
            let workload =
                omnigraph_server::workload::WorkloadController::with_defaults();
            let state = AppState::new_with_workload(
                repo.to_string_lossy().to_string(),
                db,
                Vec::new(),
                workload,
            );
            let app = build_app(state);
            Self {
                _temp: temp,
                app,
            }
        }

        pub async fn create_branch(&self, from: &str, name: &str) {
            let body = serde_json::to_vec(&BranchCreateRequest {
                from: Some(from.to_string()),
                name: name.to_string(),
            })
            .unwrap();
            let r = self
                .app
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
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "setup create_branch {} from {} failed",
                name,
                from
            );
        }

        pub async fn insert_person(&self, branch: &str, name: &str, age: i32) {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: MUTATION_QUERIES.to_string(),
                query_name: Some("insert_person".to_string()),
                params: Some(json!({ "name": name, "age": age })),
                branch: Some(branch.to_string()),
            })
            .unwrap();
            let r = self
                .app
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
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "setup insert {} on {} failed",
                name,
                branch
            );
        }

        /// Run two ops concurrently with barrier alignment + 15s deadlock
        /// timeout. Returns `(op_a, op_b)`. Panics on timeout.
        pub async fn run_pair(
            &self,
            op_a: impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus>,
            op_b: impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus>,
        ) -> (OpStatus, OpStatus) {
            let barrier = Arc::new(Barrier::new(2));
            let h_a = op_a(self.app.clone(), Arc::clone(&barrier));
            let h_b = op_b(self.app.clone(), Arc::clone(&barrier));
            let result = tokio::time::timeout(Duration::from_secs(15), async {
                let a = h_a.await.unwrap();
                let b = h_b.await.unwrap();
                (a, b)
            })
            .await;
            result.expect("concurrent op pair deadlocked (>15s)")
        }

        pub async fn person_count(&self, branch: &str) -> u64 {
            let r = self
                .app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/snapshot?branch={}", branch))
                        .method(Method::GET)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "snapshot {} failed",
                branch
            );
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

        /// True iff the named Person exists on `branch`. Uses the
        /// `get_person` query from `test.gq` for identity rather than
        /// just count.
        pub async fn person_exists(&self, branch: &str, name: &str) -> bool {
            let body = serde_json::to_vec(&ReadRequest {
                query_source: include_str!(
                    "../../omnigraph/tests/fixtures/test.gq"
                )
                .to_string(),
                query_name: Some("get_person".to_string()),
                params: Some(json!({ "name": name })),
                branch: Some(branch.to_string()),
                snapshot: None,
            })
            .unwrap();
            let r = self
                .app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/read")
                        .method(Method::POST)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "person_exists query for {} on {} failed",
                name,
                branch
            );
            let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap();
            v["row_count"].as_u64().unwrap_or(0) > 0
        }

        /// Asserts each name in `present` exists on `branch` and each in
        /// `absent` does not. Identity-grade check that catches symmetric
        /// swap races a row-count assertion would miss.
        pub async fn assert_persons(
            &self,
            branch: &str,
            cell: &str,
            present: &[&str],
            absent: &[&str],
        ) {
            for name in present {
                assert!(
                    self.person_exists(branch, name).await,
                    "[{}] expected {} to be present on {}",
                    cell,
                    name,
                    branch
                );
            }
            for name in absent {
                assert!(
                    !self.person_exists(branch, name).await,
                    "[{}] expected {} to be absent from {}",
                    cell,
                    name,
                    branch
                );
            }
        }

        /// C6: insert a uniquely-named sentinel on main and verify it
        /// landed. Catches engine-state poisoning where a cell's
        /// concurrent ops left the engine half-broken — subsequent
        /// /change either deadlocks or returns a non-200.
        pub async fn assert_post_op_sentinel(&self, cell: &str, sentinel: &str) {
            let body = serde_json::to_vec(&ChangeRequest {
                query_source: MUTATION_QUERIES.to_string(),
                query_name: Some("insert_person".to_string()),
                params: Some(json!({ "name": sentinel, "age": 99 })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let r = self
                .app
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
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "[{}] post-op sentinel /change on main failed (engine poisoned?)",
                cell
            );
            assert!(
                self.person_exists("main", sentinel).await,
                "[{}] sentinel {} did not land on main",
                cell,
                sentinel
            );
        }
    }

    // Helpers that build the closures for `run_pair`. Each takes a
    // Router + Barrier and returns a JoinHandle yielding the status/body.

    pub(super) fn op_merge(
        source: String,
        target: String,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let body = serde_json::to_vec(&BranchMergeRequest {
                    source,
                    target: Some(target),
                })
                .unwrap();
                let response = app
                    .oneshot(
                    Request::builder()
                        .uri("/branches/merge")
                        .method(Method::POST)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                OpStatus {
                    status,
                    body: body.to_vec(),
                }
            })
        }
    }

    pub(super) fn op_change_insert(
        branch: String,
        name: String,
        age: i32,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let body = serde_json::to_vec(&ChangeRequest {
                    query_source: MUTATION_QUERIES.to_string(),
                    query_name: Some("insert_person".to_string()),
                    params: Some(json!({ "name": name, "age": age })),
                    branch: Some(branch),
                })
                .unwrap();
                let response = app
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
                let status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                OpStatus {
                    status,
                    body: body.to_vec(),
                }
            })
        }
    }

    pub(super) fn op_branch_create(
        from: String,
        name: String,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let body = serde_json::to_vec(&BranchCreateRequest {
                    from: Some(from),
                    name,
                })
                .unwrap();
                let response = app
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
                let status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                OpStatus {
                    status,
                    body: body.to_vec(),
                }
            })
        }
    }

    pub(super) fn op_branch_delete(
        name: String,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let response = app
                    .oneshot(
                    Request::builder()
                        .uri(format!("/branches/{}", name))
                        .method(Method::DELETE)
                        .body(Body::empty())
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                OpStatus {
                    status,
                    body: body.to_vec(),
                }
            })
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_branch_ops_morphological_matrix() {
    // Cell a: Merge × Merge, distinct targets.
    // Pre-fix on b09a097/22d76db: branch_merge_impl's swap-restore race
    // landed feature_a's content in target_b instead of target_a (and
    // vice versa — symmetric swap). Identity asserts catch both
    // asymmetric and symmetric variants.
    {
        let cell = "a:merge×merge:distinct-targets";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "feature-a-cella").await;
        h.insert_person("feature-a-cella", "EveA-cella", 22).await;
        h.create_branch("main", "feature-b-cella").await;
        h.insert_person("feature-b-cella", "FrankB-cella", 33).await;
        h.create_branch("main", "target-a-cella").await;
        h.create_branch("main", "target-b-cella").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge(
                    "feature-a-cella".to_string(),
                    "target-a-cella".to_string(),
                ),
                matrix::op_merge(
                    "feature-b-cella".to_string(),
                    "target-b-cella".to_string(),
                ),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge a", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge b", cell);
        h.assert_persons("target-a-cella", cell, &["EveA-cella"], &["FrankB-cella"])
            .await;
        h.assert_persons("target-b-cella", cell, &["FrankB-cella"], &["EveA-cella"])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cella").await;
    }

    // Cell b: Merge × Merge, same target / distinct sources.
    // Both want to land in main. merge_exclusive serializes; both should
    // succeed and main should contain BOTH sources' contributions.
    {
        let cell = "b:merge×merge:same-target-distinct-sources";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-x-cellb").await;
        h.insert_person("src-x-cellb", "Xavier-cellb", 41).await;
        h.create_branch("main", "src-y-cellb").await;
        h.insert_person("src-y-cellb", "Yvonne-cellb", 42).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-x-cellb".to_string(), "main".to_string()),
                matrix::op_merge("src-y-cellb".to_string(), "main".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge x", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge y", cell);
        h.assert_persons("main", cell, &["Xavier-cellb", "Yvonne-cellb"], &[])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellb").await;
    }

    // Cell c: Merge × Merge, same source / distinct targets (fanout).
    // One source merged into two targets simultaneously. merge_exclusive
    // serializes; both targets should reflect the source's content.
    {
        let cell = "c:merge×merge:same-source-distinct-targets";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-shared-cellc").await;
        h.insert_person("src-shared-cellc", "Sharon-cellc", 50).await;
        h.create_branch("main", "tgt-1-cellc").await;
        h.create_branch("main", "tgt-2-cellc").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge(
                    "src-shared-cellc".to_string(),
                    "tgt-1-cellc".to_string(),
                ),
                matrix::op_merge(
                    "src-shared-cellc".to_string(),
                    "tgt-2-cellc".to_string(),
                ),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge into tgt-1", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge into tgt-2", cell);
        h.assert_persons("tgt-1-cellc", cell, &["Sharon-cellc"], &[])
            .await;
        h.assert_persons("tgt-2-cellc", cell, &["Sharon-cellc"], &[])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellc").await;
    }

    // Cell d: Merge × Change, both touching main. C2 permits both
    // succeed, or exactly one clean 409 if the merge detects target
    // movement after planning but before acquiring the queue.
    {
        let cell = "d:merge×change:into-target";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "feature-celld").await;
        h.insert_person("feature-celld", "EveD-celld", 22).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("feature-celld".to_string(), "main".to_string()),
                matrix::op_change_insert("main".to_string(), "FrankD-celld".to_string(), 33),
            )
            .await;
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        assert!(
            sa.status == StatusCode::OK || sa.status == StatusCode::CONFLICT,
            "[{}] merge must be 200 or clean 409, got {}",
            cell,
            sa.status
        );
        if sa.status == StatusCode::OK {
            h.assert_persons("main", cell, &["EveD-celld", "FrankD-celld"], &[])
                .await;
        } else {
            let error: ErrorOutput = serde_json::from_slice(&sa.body).unwrap();
            let conflict = error
                .manifest_conflict
                .expect("merge 409 must include manifest_conflict");
            assert_eq!(conflict.table_key, "node:Person", "[{}] conflict table", cell);
            h.assert_persons("main", cell, &["FrankD-celld"], &["EveD-celld"])
                .await;
        }
        h.assert_post_op_sentinel(cell, "sentinel-celld").await;
    }

    // Cell e: Merge × BranchCreateFrom-target. Concurrent fork off the
    // merge target while the merge runs. Both should succeed; the new
    // branch should have a coherent view (either pre- or post-merge,
    // both valid). After both, target = main has the merged content.
    {
        let cell = "e:merge×branch_create_from:target";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-celle").await;
        h.insert_person("src-celle", "Eve-celle", 22).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-celle".to_string(), "main".to_string()),
                matrix::op_branch_create("main".to_string(), "fork-celle".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] branch_create_from", cell);
        // Main definitely has Eve.
        h.assert_persons("main", cell, &["Eve-celle"], &[]).await;
        // fork-celle was forked off main at SOME version; main's current
        // count is 5 (4 seeded + Eve). fork-celle has either 4 (pre-merge
        // snapshot) or 5 (post-merge snapshot); both are valid timings.
        let fork_count = h.person_count("fork-celle").await;
        assert!(
            fork_count == 4 || fork_count == 5,
            "[{}] fork-celle row count must be pre- or post-merge view (4 or 5), got {}",
            cell,
            fork_count
        );
        h.assert_post_op_sentinel(cell, "sentinel-celle").await;
    }

    // Cell f: BranchCreateFrom × BranchCreateFrom, distinct parents.
    // Pre-fix on f925ad1: swap-restore race in branch_create_from_impl
    // forked the new branch off the wrong parent. Identity asserts pin
    // that fork-from-A inherits A's content, fork-from-B inherits B's.
    {
        let cell = "f:branch_create_from×branch_create_from:distinct-parents";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "alpha-cellf").await;
        h.insert_person("alpha-cellf", "Eve-cellf", 22).await;
        h.create_branch("main", "beta-cellf").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create(
                    "alpha-cellf".to_string(),
                    "gamma-cellf".to_string(),
                ),
                matrix::op_branch_create(
                    "beta-cellf".to_string(),
                    "delta-cellf".to_string(),
                ),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] gamma create", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delta create", cell);
        // gamma forks off alpha → must contain Eve.
        h.assert_persons("gamma-cellf", cell, &["Eve-cellf"], &[]).await;
        // delta forks off beta → must NOT contain Eve.
        h.assert_persons("delta-cellf", cell, &[], &["Eve-cellf"]).await;
        h.assert_post_op_sentinel(cell, "sentinel-cellf").await;
    }

    // Cell g: BranchCreateFrom × BranchDelete, unrelated branches.
    // Disjoint branches; both should complete cleanly without
    // interference.
    {
        let cell = "g:branch_create_from×branch_delete:unrelated";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed-cellg").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create("main".to_string(), "newborn-cellg".to_string()),
                matrix::op_branch_delete("doomed-cellg".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] create newborn", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delete doomed", cell);
        // newborn-cellg exists with main's content.
        h.assert_persons("newborn-cellg", cell, &["Alice"], &[]).await;
        h.assert_post_op_sentinel(cell, "sentinel-cellg").await;
    }

    // Cell h: BranchDelete × BranchDelete, distinct branches. Both call
    // refresh() internally; verify no deadlock and both deletes land.
    {
        let cell = "h:branch_delete×branch_delete:distinct";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed1-cellh").await;
        h.create_branch("main", "doomed2-cellh").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_delete("doomed1-cellh".to_string()),
                matrix::op_branch_delete("doomed2-cellh".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] delete 1", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delete 2", cell);
        // Verify both gone via /branches list (snapshot would still work
        // for a deleted branch via parent fallback in some paths, so we
        // use the explicit list).
        let r = h
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/branches")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let list_body: Value = serde_json::from_slice(&body).unwrap();
        let branches: Vec<&str> = list_body["branches"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            !branches.contains(&"doomed1-cellh"),
            "[{}] doomed1 still in branch list: {:?}",
            cell,
            branches
        );
        assert!(
            !branches.contains(&"doomed2-cellh"),
            "[{}] doomed2 still in branch list: {:?}",
            cell,
            branches
        );
        h.assert_post_op_sentinel(cell, "sentinel-cellh").await;
    }

    // Cell i: BranchDelete × Change, on a different branch. Delete one
    // branch while a /change runs on main. Both should succeed.
    {
        let cell = "i:branch_delete×change:distinct-branch";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed-celli").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_delete("doomed-celli".to_string()),
                matrix::op_change_insert("main".to_string(), "Pat-celli".to_string(), 44),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] delete", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        h.assert_persons("main", cell, &["Pat-celli"], &[]).await;
        h.assert_post_op_sentinel(cell, "sentinel-celli").await;
    }

    // Cell j: BranchCreateFrom × Change, both on main. The fork timing
    // determines whether the new branch sees the change (pre or post).
    // Both valid. Main must contain the inserted row.
    {
        let cell = "j:branch_create_from×change:on-source";
        let h = matrix::Harness::new().await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create("main".to_string(), "twin-cellj".to_string()),
                matrix::op_change_insert("main".to_string(), "Quincy-cellj".to_string(), 55),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] branch_create", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        h.assert_persons("main", cell, &["Quincy-cellj"], &[]).await;
        // twin-cellj has either pre-change view (no Quincy) or
        // post-change view (with Quincy); either is valid.
        let twin_has_quincy = h.person_exists("twin-cellj", "Quincy-cellj").await;
        let _ = twin_has_quincy; // either valid timing — just ensure no panic
        h.assert_post_op_sentinel(cell, "sentinel-cellj").await;
    }

    // Cell k: reopen consistency. Run a representative concurrent pair,
    // drop the engine, reopen on a separate handle, verify state matches.
    {
        let cell = "k:reopen-after-pair";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-cellk").await;
        h.insert_person("src-cellk", "Rita-cellk", 36).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-cellk".to_string(), "main".to_string()),
                matrix::op_change_insert("main".to_string(), "Steve-cellk".to_string(), 37),
            )
            .await;
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        assert!(
            sa.status == StatusCode::OK || sa.status == StatusCode::CONFLICT,
            "[{}] merge must be 200 or clean 409, got {}",
            cell,
            sa.status
        );
        if sa.status == StatusCode::OK {
            h.assert_persons("main", cell, &["Rita-cellk", "Steve-cellk"], &[])
                .await;
        } else {
            let error: ErrorOutput = serde_json::from_slice(&sa.body).unwrap();
            let conflict = error
                .manifest_conflict
                .expect("merge 409 must include manifest_conflict");
            assert_eq!(conflict.table_key, "node:Person", "[{}] conflict table", cell);
            h.assert_persons("main", cell, &["Steve-cellk"], &["Rita-cellk"])
                .await;
        }

        // Reopen via a fresh AppState on the same repo.
        let repo_uri = format!("{}/server.omni", h._temp.path().display());
        let reopened = AppState::open(repo_uri.clone()).await.unwrap();
        let app2 = build_app(reopened);
        // Sanity: the same identity check via the new app must see
        // Rita and Steve.
        let r = app2
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/snapshot?branch=main")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "[{}] reopen snapshot", cell);
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let person_rows = v["tables"]
            .as_array()
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|t| t["table_key"].as_str() == Some("node:Person"))
            })
            .and_then(|t| t["row_count"].as_u64())
            .expect("reopen snapshot must include node:Person row_count");
        let expected_rows = if sa.status == StatusCode::OK { 6 } else { 5 };
        assert_eq!(
            person_rows, expected_rows,
            "[{}] reopened main should include seed (4) + committed concurrent writes",
            cell,
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
    //
    // Construct the WorkloadController directly with cap=1 instead of
    // mutating `OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX` via EnvGuard. Process-wide
    // env vars are visible to concurrently-running tests; the previous
    // `EnvGuard + #[serial]` pair leaked the override into any other test
    // that called `AppState::open` during the guard's window
    // (matrix CI failure on commit 99b0941). Using the explicit
    // `AppState::new_with_workload` constructor closes that bug class —
    // this test no longer mutates global state and no longer needs
    // `#[serial]`.
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let workload = omnigraph_server::workload::WorkloadController::new(
        1,             // per-actor in-flight cap (the fixture under test)
        1_000_000_000, // per-actor byte budget — large so it never bottlenecks
    );
    let state = AppState::new_with_workload(
        repo.to_string_lossy().to_string(),
        db,
        vec![("act-flooder".to_string(), "flooder-token".to_string())],
        workload,
    );
    let app = build_app(state);
    let _temp = temp;

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
