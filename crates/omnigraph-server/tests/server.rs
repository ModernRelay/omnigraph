use std::env;
use std::fs;
use std::path::{Path, PathBuf};

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
    // MR-771: with the Run state machine removed, actor_id is recorded
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
    // MR-771: a write that races with another writer surfaces as HTTP 409
    // with a structured `manifest_conflict` body — `table_key`, `expected`,
    // and `actual` — so clients can detect-and-retry without parsing the
    // message. (Replaces the old run-publish merge-conflict shape.)
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
