use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::api::{ChangeRequest, ErrorOutput, ReadRequest};
use omnigraph_server::{AppState, build_app};
use serial_test::serial;
use serde_json::{Value, json};
use tower::ServiceExt;

const MUTATION_QUERIES: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
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
    Omnigraph::init(repo.to_str().unwrap(), schema).await.unwrap();
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    temp
}

fn repo_path(root: &Path) -> PathBuf {
    root.join("server.omni")
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
