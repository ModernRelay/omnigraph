//! Shared helpers for the server integration suites (moved verbatim
//! from the monolithic tests/server.rs in the modularization).
#![allow(dead_code)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::header::AUTHORIZATION;
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_policy::{PolicyChecker, PolicyEngine};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ReadRequest,
};
use omnigraph_server::queries::{QueryRegistry, RegistrySpec};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use tower::ServiceExt;


pub const MUTATION_QUERIES: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#;

pub const POLICY_YAML: &str = r#"
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

pub const POLICY_PROTECTED_READ_YAML: &str = r#"
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

pub const INGEST_CREATE_ONLY_POLICY_YAML: &str = r#"
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

pub const SCHEMA_APPLY_POLICY_YAML: &str = r#"
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

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph/tests/fixtures")
        .join(name)
}

pub async fn init_loaded_graph() -> tempfile::TempDir {
    init_graph_with_schema_and_data(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &fs::read_to_string(fixture("test.jsonl")).unwrap(),
    )
    .await
}

pub async fn init_graph_with_schema_and_data(schema: &str, data: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    fs::create_dir_all(&graph).unwrap();
    Omnigraph::init(graph.to_str().unwrap(), schema)
        .await
        .unwrap();
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    temp
}

pub async fn init_graph_with_schema(schema: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    fs::create_dir_all(&graph).unwrap();
    Omnigraph::init(graph.to_str().unwrap(), schema)
        .await
        .unwrap();
    temp
}

pub fn graph_path(root: &Path) -> PathBuf {
    root.join("server.omni")
}

pub fn stored_query_registry(specs: &[(&str, &str, bool)]) -> QueryRegistry {
    // MCP `expose` now lives in the `.gq` source `@mcp(...)` annotation. The
    // `(name, source, expose)` tuple stays for ergonomics: when `expose` is
    // false, inject `@mcp(expose: false)` between the param-list `)` and the
    // body `{` (the first `{` in a query source is always the body open), so
    // the real parse path is exercised.
    QueryRegistry::from_specs(
        specs
            .iter()
            .map(|(name, source, expose)| {
                let source = if *expose {
                    source.to_string()
                } else {
                    match source.find('{') {
                        Some(i) => format!("{}@mcp(expose: false) {}", &source[..i], &source[i..]),
                        None => source.to_string(),
                    }
                };
                RegistrySpec { name: name.to_string(), source }
            })
            .collect(),
    )
    .expect("specs parse and key==symbol")
}

pub async fn app_with_stored_queries(
    specs: &[(&str, &str, bool)],
    tokens: &[(&str, &str)],
    policy: &str,
) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, policy).unwrap();
    let registry = stored_query_registry(specs);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        tokens
            .iter()
            .map(|(actor, token)| ((*actor).to_string(), (*token).to_string()))
            .collect(),
        Some(&policy_path),
        registry,
    )
    .await
    .unwrap();
    (temp, build_app(state))
}

pub const INVOKE_POLICY_YAML: &str = r#"
version: 1
groups:
  invokers: ["act-invoke"]
  full: ["act-full"]
  readers: ["act-noinvoke"]
  invoke_only: ["act-invokeonly"]
protected_branches: [main]
rules:
  # invoke_query is graph-scoped — its own rules, no branch_scope.
  - id: invokers-can-invoke
    allow:
      actors: { group: invokers }
      actions: [invoke_query]
  - id: full-can-invoke
    allow:
      actors: { group: full }
      actions: [invoke_query]
  - id: invoke-only-can-invoke
    allow:
      actors: { group: invoke_only }
      actions: [invoke_query]
  # read / change are branch-scoped.
  - id: invokers-can-read
    allow:
      actors: { group: invokers }
      actions: [read]
      branch_scope: any
  - id: full-can-read-change
    allow:
      actors: { group: full }
      actions: [read, change]
      branch_scope: any
  - id: readers-can-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#;

pub const STORED_QUERY_SCHEMA_APPLY_POLICY_YAML: &str = r#"
version: 1
groups:
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: admins-can-invoke
    allow:
      actors: { group: admins }
      actions: [invoke_query]
  - id: admins-can-read
    allow:
      actors: { group: admins }
      actions: [read]
      branch_scope: any
  - id: admins-can-schema-apply
    allow:
      actors: { group: admins }
      actions: [schema_apply]
      target_branch_scope: protected
"#;

pub const FIND_PERSON_GQ: &str =
    "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }";

/// RFC-011 cluster-only: the single-graph convenience apps built by the
/// `app_for_loaded_graph*` helpers serve the graph under the reserved id
/// `default`. This prefixes a flat per-graph path (e.g. `/snapshot`) with
/// the cluster route prefix so tests address `/graphs/default/snapshot`.
pub fn g(path: &str) -> String {
    format!("/graphs/default{path}")
}

pub fn invoke_request(name: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .uri(g(&format!("/queries/{name}")))
        .method(Method::POST)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub fn invoke_request_bytes(
    name: &str,
    token: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(g(&format!("/queries/{name}")))
        .method(Method::POST)
        .header("authorization", format!("Bearer {token}"));
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder.body(body.into()).unwrap()
}

pub fn get_request(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .method(Method::GET)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

pub fn drifted_test_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?")
}

pub async fn manifest_dataset_version(graph: &Path) -> u64 {
    Omnigraph::open(graph.to_string_lossy().as_ref())
        .await
        .unwrap()
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version()
}

pub fn s3_test_graph_uri(suite: &str) -> Option<String> {
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

pub async fn app_for_loaded_graph() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    (temp, build_app(state))
}

pub fn permit_all_policy_yaml(actors: &[&str]) -> String {
    let members = actors
        .iter()
        .map(|a| format!("\"{a}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
version: 1
groups:
  permitted: [{members}]
protected_branches: [main]
rules:
  - id: permit-data
    allow:
      actors: {{ group: permitted }}
      actions: [read, change, export]
      branch_scope: any
  - id: permit-protected-target-actions
    allow:
      actors: {{ group: permitted }}
      actions: [schema_apply, branch_create, branch_delete, branch_merge]
      target_branch_scope: any
  - id: permit-invoke
    allow:
      actors: {{ group: permitted }}
      actions: [invoke_query]
"#
    )
}

pub async fn app_for_loaded_graph_with_auth(token: &str) -> (tempfile::TempDir, Router) {
    // `AppState::new_with_bearer_token(token)` maps the token to actor "default";
    // permit-all policy needs to include that actor.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["default"])).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("default".to_string(), token.to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    (temp, build_app(state))
}

pub async fn app_for_loaded_graph_with_auth_tokens(
    tokens: &[(&str, &str)],
) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    let actors: Vec<&str> = tokens.iter().map(|(actor, _)| *actor).collect();
    fs::write(&policy_path, permit_all_policy_yaml(&actors)).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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

pub async fn app_for_loaded_graph_with_auth_tokens_and_policy(
    tokens: &[(&str, &str)],
    policy: &str,
) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, policy).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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

pub async fn app_for_graph_with_auth_tokens_and_policy(
    schema: &str,
    tokens: &[(&str, &str)],
    policy: &str,
) -> (tempfile::TempDir, Router) {
    let temp = init_graph_with_schema(schema).await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, policy).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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

pub async fn app_for_graph_with_auth_tokens_only(
    schema: &str,
    tokens: &[(&str, &str)],
) -> (tempfile::TempDir, Router) {
    let temp = init_graph_with_schema(schema).await;
    let graph = graph_path(temp.path());
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        tokens
            .iter()
            .map(|(actor, token)| ((*actor).to_string(), (*token).to_string()))
            .collect(),
        None,
    )
    .await
    .unwrap();
    (temp, build_app(state))
}

pub fn additive_schema_with_nickname() -> String {
    fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    )
}

pub fn schema_without_age() -> String {
    // Drop the nullable `age` column from the test schema. Used by the
    // HTTP soft/hard drop tests below.
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?\n", "")
}

pub fn schema_without_company() -> String {
    // Drop the `Company` node type and the edge referencing it. Used
    // by the HTTP DropType test below. Hand-crafted (no template
    // string replace) because the fixture interleaves the type and
    // its edge.
    r#"node Person {
    name: String @key
    age: I32?
}

edge Knows: Person -> Person {
    since: Date?
}
"#
    .to_string()
}

pub fn renamed_person_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("node Person {\n", "node Human @rename_from(\"Person\") {\n")
        .replace("edge Knows: Person -> Person", "edge Knows: Human -> Human")
        .replace(
            "edge WorksAt: Person -> Company",
            "edge WorksAt: Human -> Company",
        )
}

pub fn renamed_age_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "years: I32? @rename_from(\"age\")")
}

pub fn indexed_name_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("name: String @key", "name: String @key @index")
}

pub fn unsupported_schema_change() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?")
}

pub async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&body).unwrap();
    (status, value)
}

pub struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    pub fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
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

pub fn format_vector(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| format!("{:.8}", value))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn normalize_vector(mut values: Vec<f32>) -> Vec<f32> {
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

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 14695981039346656037u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211u64);
    }
    hash
}

pub fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

pub fn mock_embedding(input: &str, dim: usize) -> Vec<f32> {
    let mut seed = fnv1a64(input.as_bytes());
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        seed = xorshift64(seed);
        let ratio = (seed as f64 / u64::MAX as f64) as f32;
        out.push((ratio * 2.0) - 1.0);
    }
    normalize_vector(out)
}

pub mod matrix {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Barrier;

    #[derive(Debug)]
    pub struct OpStatus {
        pub status: StatusCode,
        pub body: Vec<u8>,
    }

    pub struct Harness {
        pub _temp: tempfile::TempDir,
        pub app: Router,
    }

    impl Harness {
        pub async fn new() -> Self {
            let temp = init_loaded_graph().await;
            let graph = graph_path(temp.path());
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
            let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
            let workload = omnigraph_server::workload::WorkloadController::with_defaults();
            let state = AppState::new_with_workload(
                graph.to_string_lossy().to_string(),
                db,
                Vec::new(),
                workload,
            );
            let app = build_app(state);
            Self { _temp: temp, app }
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
                        .uri(g("/branches"))
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
                query: MUTATION_QUERIES.to_string(),
                name: Some("insert_person".to_string()),
                params: Some(json!({ "name": name, "age": age })),
                branch: Some(branch.to_string()),
            })
            .unwrap();
            let r = self
                .app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(g("/change"))
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
                        .uri(g(&format!("/snapshot?branch={}", branch)))
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

        /// True iff the named Person exists on `branch`. Uses the
        /// `get_person` query from `test.gq` for identity rather than
        /// just count.
        pub async fn person_exists(&self, branch: &str, name: &str) -> bool {
            let body = serde_json::to_vec(&ReadRequest {
                query_source: include_str!("../../../omnigraph/tests/fixtures/test.gq").to_string(),
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
                        .uri(g("/read"))
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
                query: MUTATION_QUERIES.to_string(),
                name: Some("insert_person".to_string()),
                params: Some(json!({ "name": sentinel, "age": 99 })),
                branch: Some("main".to_string()),
            })
            .unwrap();
            let r = self
                .app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(g("/change"))
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

    pub fn op_merge(
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
                            .uri(g("/branches/merge"))
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

    pub fn op_change_insert(
        branch: String,
        name: String,
        age: i32,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let body = serde_json::to_vec(&ChangeRequest {
                    query: MUTATION_QUERIES.to_string(),
                    name: Some("insert_person".to_string()),
                    params: Some(json!({ "name": name, "age": age })),
                    branch: Some(branch),
                })
                .unwrap();
                let response = app
                    .oneshot(
                        Request::builder()
                            .uri(g("/change"))
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

    pub fn op_branch_create(
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
                            .uri(g("/branches"))
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

    pub fn op_branch_delete(
        name: String,
    ) -> impl FnOnce(Router, Arc<Barrier>) -> tokio::task::JoinHandle<OpStatus> {
        move |app: Router, barrier: Arc<Barrier>| {
            tokio::spawn(async move {
                barrier.wait().await;
                let response = app
                    .oneshot(
                        Request::builder()
                            .uri(g(&format!("/branches/{}", name)))
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

pub const PARITY_POLICY_YAML: &str = r#"
version: 1
groups:
  team: [act-bruno]
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: admins-change-anywhere
    allow:
      actors: { group: admins }
      actions: [change]
      branch_scope: any
  - id: admins-merge-to-protected
    allow:
      actors: { group: admins }
      actions: [branch_merge]
      target_branch_scope: protected
"#;

#[derive(Clone, Copy, Debug)]
pub enum ParityDecision {
    Allow,
    Deny,
}

pub async fn build_parity_graph() -> (tempfile::TempDir, PathBuf, PathBuf) {
    // Build a graph with `main` loaded and a `feature` branch ready for
    // merge. Returns the graph path and a written policy.yaml path.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.branch_create_from(ReadTarget::branch("main"), "feature")
            .await
            .unwrap();
        db.load_as(
            "feature",
            None,
            r#"{"type":"Person","data":{"name":"ParityEve","age":29}}"#,
            LoadMode::Append,
            None,
        )
        .await
        .unwrap();
    }
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, PARITY_POLICY_YAML).unwrap();
    (temp, graph, policy_path)
}

pub async fn sdk_change_decision(graph: &Path, policy_path: &Path, actor: &str) -> ParityDecision {
    let policy = PolicyEngine::load_graph(policy_path, graph.to_string_lossy().as_ref()).unwrap();
    let db = Omnigraph::open(graph.to_str().unwrap())
        .await
        .unwrap()
        .with_policy(Arc::new(policy) as Arc<dyn PolicyChecker>);
    let mut params: omnigraph_compiler::ParamMap = Default::default();
    // Parameter keys are bare names (no `$` prefix); the runtime resolves
    // `$name` references in the query body to `params["name"]`.
    params.insert(
        "name".to_string(),
        omnigraph_compiler::Literal::String("ParityCharlie".to_string()),
    );
    params.insert("age".to_string(), omnigraph_compiler::Literal::Integer(30));
    let result = db
        .mutate_as(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &params,
            Some(actor),
        )
        .await;
    match result {
        Ok(_) => ParityDecision::Allow,
        Err(OmniError::Policy(_)) => ParityDecision::Deny,
        Err(other) => panic!("unexpected SDK error for change: {other:?}"),
    }
}

pub async fn http_change_decision(
    graph: &Path,
    policy_path: &PathBuf,
    actor: &str,
    token: &str,
) -> ParityDecision {
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![(actor.to_string(), token.to_string())],
        Some(policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);
    let req = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "ParityCharlie", "age": 30 })),
        branch: Some("main".to_string()),
    };
    let (status, _body) = json_response(
        &app,
        Request::builder()
            .uri(g("/change"))
            .method(Method::POST)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&req).unwrap()))
            .unwrap(),
    )
    .await;
    match status {
        StatusCode::OK => ParityDecision::Allow,
        StatusCode::FORBIDDEN => ParityDecision::Deny,
        other => panic!("unexpected HTTP status for change: {other}"),
    }
}

pub async fn sdk_merge_decision(graph: &Path, policy_path: &Path, actor: &str) -> ParityDecision {
    let policy = PolicyEngine::load_graph(policy_path, graph.to_string_lossy().as_ref()).unwrap();
    let db = Omnigraph::open(graph.to_str().unwrap())
        .await
        .unwrap()
        .with_policy(Arc::new(policy) as Arc<dyn PolicyChecker>);
    let result = db.branch_merge_as("feature", "main", Some(actor)).await;
    match result {
        Ok(_) => ParityDecision::Allow,
        Err(OmniError::Policy(_)) => ParityDecision::Deny,
        Err(other) => panic!("unexpected SDK error for branch_merge: {other:?}"),
    }
}

pub async fn http_merge_decision(
    graph: &Path,
    policy_path: &PathBuf,
    actor: &str,
    token: &str,
) -> ParityDecision {
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![(actor.to_string(), token.to_string())],
        Some(policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);
    let req = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
    };
    let (status, _body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&req).unwrap()))
            .unwrap(),
    )
    .await;
    match status {
        StatusCode::OK => ParityDecision::Allow,
        StatusCode::FORBIDDEN => ParityDecision::Deny,
        other => panic!("unexpected HTTP status for branch_merge: {other}"),
    }
}

pub async fn converged_cluster_dir(policies_yaml: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("people.pg"),
        "\nnode Person {\n  name: String @key\n}\n",
    )
    .unwrap();
    fs::write(
        temp.path().join("people.gq"),
        "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
    )
    .unwrap();
    fs::write(
        temp.path().join("cluster.yaml"),
        format!(
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
{policies_yaml}"#
        ),
    )
    .unwrap();
    let import = omnigraph_cluster::import_config_dir(temp.path()).await;
    assert!(import.ok, "{:?}", import.diagnostics);
    let apply = omnigraph_cluster::apply_config_dir(temp.path()).await;
    assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);
    temp
}

pub async fn cluster_settings(dir: &Path) -> color_eyre::eyre::Result<omnigraph_server::ServerConfig> {
    omnigraph_server::load_server_settings(Some(&dir.to_path_buf()), None, true).await
}
