use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::header::AUTHORIZATION;
use axum::http::{Method, Request, StatusCode};
use lance::index::DatasetIndexExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_policy::{PolicyChecker, PolicyEngine};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest,
    IngestRequest, QueryRequest, ReadRequest, SchemaApplyRequest, SchemaOutput,
};
use omnigraph_server::queries::{QueryRegistry, RegistrySpec};
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

async fn init_loaded_graph() -> tempfile::TempDir {
    init_graph_with_schema_and_data(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &fs::read_to_string(fixture("test.jsonl")).unwrap(),
    )
    .await
}

async fn init_graph_with_schema_and_data(schema: &str, data: &str) -> tempfile::TempDir {
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

async fn init_graph_with_schema(schema: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    fs::create_dir_all(&graph).unwrap();
    Omnigraph::init(graph.to_str().unwrap(), schema)
        .await
        .unwrap();
    temp
}

fn graph_path(root: &Path) -> PathBuf {
    root.join("server.omni")
}

fn stored_query_registry(specs: &[(&str, &str, bool)]) -> QueryRegistry {
    QueryRegistry::from_specs(
        specs
            .iter()
            .map(|(name, source, expose)| RegistrySpec {
                name: name.to_string(),
                source: source.to_string(),
                expose: *expose,
                tool_name: None,
            })
            .collect(),
    )
    .expect("specs parse and key==symbol")
}

#[tokio::test]
async fn server_boots_with_a_valid_stored_query_registry() {
    // A stored query that type-checks against the fixture schema
    // (`Person { name, age }`) must let the server boot.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "find_person",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
        false,
    )]);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    assert!(state.is_ok(), "valid registry should boot: {:?}", state.err());
}

#[tokio::test]
async fn server_refuses_boot_on_type_broken_stored_query() {
    // A stored query referencing a type not in the schema (`Widget`)
    // must abort boot, naming the offending query.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "ghost",
        "query ghost() { match { $w: Widget } return { $w.name } }",
        false,
    )]);
    let result = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    // `AppState` is not `Debug`, so match rather than `expect_err`.
    let err = match result {
        Ok(_) => panic!("type-broken stored query must refuse boot"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("ghost"), "error should name the broken query: {msg}");
    assert!(
        msg.contains("schema check"),
        "error should mention the schema check: {msg}"
    );
}

/// Build a single-mode app with a stored-query registry plus a bearer→actor
/// pairing and a policy, so invoke tests exercise the `invoke_query`
/// boundary gate and the inner read/change gates together.
async fn app_with_stored_queries(
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

/// - `act-invoke`: invoke_query + read (stored reads, not mutations)
/// - `act-full`:   invoke_query + read + change (stored mutations)
/// - `act-noinvoke`: read only, no invoke_query (boundary-denied)
/// - `act-invokeonly`: invoke_query only, no read (clears the boundary, inner read denies)
const INVOKE_POLICY_YAML: &str = r#"
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

const STORED_QUERY_SCHEMA_APPLY_POLICY_YAML: &str = r#"
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

const FIND_PERSON_GQ: &str =
    "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }";

fn invoke_request(name: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .uri(format!("/queries/{name}"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn invoke_request_bytes(
    name: &str,
    token: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(format!("/queries/{name}"))
        .method(Method::POST)
        .header("authorization", format!("Bearer {token}"));
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder.body(body.into()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_returns_rows() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request("find_person", "t-invoke", json!({ "params": { "name": "Alice" } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "find_person");
    assert_eq!(body["row_count"], 1, "Alice is in the fixture; body: {body}");
    assert!(body["rows"].is_array(), "read envelope shape; body: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_accepts_absent_or_empty_body() {
    let no_param_query = "query list_people() { match { $p: Person } return { $p.name } }";
    let (_temp, app) = app_with_stored_queries(
        &[("list_people", no_param_query, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    let (status, body) = json_response(
        &app,
        invoke_request_bytes("list_people", "t-invoke", Body::empty(), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "list_people");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::empty(),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{}"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid stored-query invocation body"),
        "malformed JSON should be rejected as bad request; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_mutation_double_gates_on_change() {
    let specs: &[(&str, &str, bool)] = &[(
        "add_person",
        "query add_person($name: String) { insert Person { name: $name } }",
        false,
    )];
    let (_temp, app) = app_with_stored_queries(
        specs,
        &[("act-invoke", "t-invoke"), ("act-full", "t-full")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Has invoke_query but NOT change → the inner change gate denies (403).
    let (status, body) = json_response(
        &app,
        invoke_request("add_person", "t-invoke", json!({ "params": { "name": "Eve" } })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "invoke_query without change must 403; body: {body}"
    );

    // Has invoke_query + change → applied.
    let (status, body) = json_response(
        &app,
        invoke_request("add_person", "t-full", json!({ "params": { "name": "Eve" } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_query_bad_param_is_400() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    // `name` is declared String; pass a number.
    let (status, body) = json_response(
        &app,
        invoke_request("find_person", "t-invoke", json!({ "params": { "name": 123 } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("name"),
        "400 should name the offending param; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_unknown_query_and_denied_actor_return_identical_404() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke"), ("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Authorized actor, unknown query name → 404.
    let (unknown_status, unknown_body) =
        json_response(&app, invoke_request("does_not_exist", "t-invoke", json!({}))).await;
    // Denied actor (no invoke_query), real query name → 404.
    let (denied_status, denied_body) = json_response(
        &app,
        invoke_request("find_person", "t-noinvoke", json!({ "params": { "name": "Alice" } })),
    )
    .await;

    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(denied_status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown_body, denied_body,
        "deny must be byte-identical to a missing query (no catalog probing)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_query_holder_without_read_sees_403_not_404() {
    // The 404-hiding is for callers WITHOUT invoke_query. An actor that
    // HOLDS invoke_query but lacks `read` clears the boundary gate, then the
    // inner read gate denies → 403 for an EXISTING read query, vs 404 for an
    // unknown one. Existence is visible to grant-holders by design (the
    // documented double-gate); this pins that actual contract.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invokeonly", "t-invokeonly")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (exists_status, _) = json_response(
        &app,
        invoke_request("find_person", "t-invokeonly", json!({ "params": { "name": "Alice" } })),
    )
    .await;
    let (absent_status, _) =
        json_response(&app, invoke_request("does_not_exist", "t-invokeonly", json!({}))).await;
    assert_eq!(
        exists_status,
        StatusCode::FORBIDDEN,
        "an existing read query the holder can't read → inner-gate 403"
    );
    assert_eq!(absent_status, StatusCode::NOT_FOUND, "unknown query still 404s");
}

fn get_request(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .method(Method::GET)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_returns_only_exposed_with_typed_params() {
    let (_temp, app) = app_with_stored_queries(
        &[
            ("find_person", FIND_PERSON_GQ, true),
            (
                "add_person",
                "query add_person($name: String) { insert Person { name: $name } }",
                true,
            ),
            ("hidden", "query hidden() { match { $p: Person } return { $p.name } }", false),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request("/queries", "t-invoke")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let entries = body["queries"].as_array().unwrap();
    let names: Vec<&str> = entries.iter().map(|q| q["name"].as_str().unwrap()).collect();
    assert!(
        names.contains(&"find_person") && names.contains(&"add_person"),
        "exposed queries listed: {names:?}"
    );
    assert!(!names.contains(&"hidden"), "non-exposed query hidden from the catalog: {names:?}");

    let fp = entries.iter().find(|q| q["name"] == "find_person").unwrap();
    assert_eq!(fp["mutation"], false);
    assert_eq!(fp["tool_name"], "find_person");
    assert_eq!(fp["params"][0]["name"], "name");
    assert_eq!(fp["params"][0]["kind"], "string");
    let ap = entries.iter().find(|q| q["name"] == "add_person").unwrap();
    assert_eq!(ap["mutation"], true, "stored insert → mutation");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_read_gated_so_a_non_invoker_can_list() {
    // The catalog is read-gated (not invoke_query-gated), so a reader who
    // lacks invoke_query still enumerates the exposed queries — the
    // documented probe-oracle gap until per-query Cedar filtering lands.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request("/queries", "t-noinvoke")).await;
    assert_eq!(status, StatusCode::OK, "read-gated catalog; body: {body}");
    let names: Vec<&str> = body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"find_person"),
        "a reader lists the catalog despite lacking invoke_query: {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_empty_when_no_registry() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let (status, body) = json_response(&app, get_request("/queries", "demo-token")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body["queries"].as_array().unwrap().is_empty(),
        "no stored-query registry → empty catalog"
    );
}

fn drifted_test_schema() -> String {
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("age: I32?", "age: I64?")
}

async fn manifest_dataset_version(graph: &Path) -> u64 {
    Omnigraph::open(graph.to_string_lossy().as_ref())
        .await
        .unwrap()
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version()
}

fn s3_test_graph_uri(suite: &str) -> Option<String> {
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

async fn app_for_loaded_graph() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    (temp, build_app(state))
}

/// Build a permit-all policy YAML that grants every action used by the
/// HTTP-layer tests to the listed actor names. MR-723 default-deny
/// closed the "tokens but no policy" loophole; helpers that used to
/// represent "auth without policy" now install this permit-all policy
/// so test cases retain their pre-MR-723 semantics ("auth required,
/// every action permitted") without conflicting with the new state
/// matrix. Tests that specifically need the State-2 deny path use
/// `app_for_graph_with_auth_tokens_only` instead.
fn permit_all_policy_yaml(actors: &[&str]) -> String {
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
"#
    )
}

async fn app_for_loaded_graph_with_auth(token: &str) -> (tempfile::TempDir, Router) {
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

async fn app_for_loaded_graph_with_auth_tokens(
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

async fn app_for_loaded_graph_with_auth_tokens_and_policy(
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

async fn app_for_graph_with_auth_tokens_and_policy(
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

/// MR-723 default-deny mode: bearer tokens configured, no policy file.
/// Exercises ServerRuntimeState::DefaultDeny — authenticated requests
/// for Read succeed, every other action is rejected with 403 from
/// `authorize_request`'s state-2 branch.
async fn app_for_graph_with_auth_tokens_only(
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

fn additive_schema_with_nickname() -> String {
    fs::read_to_string(fixture("test.pg")).unwrap().replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    )
}

fn schema_without_age() -> String {
    // Drop the nullable `age` column from the test schema. Used by the
    // HTTP soft/hard drop tests below.
    fs::read_to_string(fixture("test.pg"))
        .unwrap()
        .replace("    age: I32?\n", "")
}

fn schema_without_company() -> String {
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

/// Build a stateless MCP JSON-RPC POST. rmcp's `handle_post` requires the
/// `Accept` header to list both JSON and SSE and the content type to be JSON;
/// a `Host` is needed so DNS-rebinding validation can run.
fn mcp_post(body: Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn mcp_initialize_body() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "smoke", "version": "0.0.0" }
        }
    })
}

#[tokio::test]
async fn mcp_initialize_advertises_tools_capability() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(&app, mcp_post(mcp_initialize_body())).await;
    assert_eq!(status, StatusCode::OK, "initialize should 200");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert!(
        body["result"]["capabilities"]["tools"].is_object(),
        "advertises the tools capability: {body}"
    );
    // Resources are NOT advertised until the resources phase implements
    // `list_resources`/`read_resource`; advertising a capability whose
    // `resources/read` 404s would be a dishonest contract.
    assert!(
        body["result"]["capabilities"]["resources"].is_null(),
        "does not advertise resources until implemented: {body}"
    );
    assert_eq!(body["result"]["serverInfo"]["name"], "omnigraph-server");
}

#[tokio::test]
async fn mcp_requires_bearer_when_auth_enabled() {
    // The §5.8 auth-decoupling invariant: /mcp sits behind the same
    // `require_bearer_auth` middleware as the REST routes, so a missing bearer
    // is rejected at the HTTP boundary (401) BEFORE rmcp runs, and a valid
    // bearer reaches the handler (200). `route_layer` + `route_service` is the
    // exact Axum interaction that could silently regress.
    let (_temp, app) = app_for_loaded_graph_with_auth("mcp-token").await;

    let no_bearer = app
        .clone()
        .oneshot(mcp_post(mcp_initialize_body()))
        .await
        .unwrap();
    assert_eq!(
        no_bearer.status(),
        StatusCode::UNAUTHORIZED,
        "no bearer must 401 at the middleware, not reach rmcp"
    );

    let mut with_bearer = mcp_post(mcp_initialize_body());
    with_bearer
        .headers_mut()
        .insert(AUTHORIZATION, "Bearer mcp-token".parse().unwrap());
    let (status, body) = json_response(&app, with_bearer).await;
    assert_eq!(status, StatusCode::OK, "valid bearer reaches the handler");
    assert_eq!(body["result"]["serverInfo"]["name"], "omnigraph-server");
}

#[tokio::test]
async fn mcp_get_returns_405_not_404() {
    // The MCP endpoint must route GET (the spec requires both POST + GET) and
    // return 405 when SSE is not offered — rmcp's stateless mode gives this
    // for free. A 404 would mean the endpoint isn't reachable.
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .header("host", "localhost")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn mcp_tools_list_includes_builtins() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        mcp_post(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let tools = body["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    for expected in [
        "health",
        "snapshot",
        "schema_get",
        "branches_list",
        "commits_list",
        "query",
        "mutate",
    ] {
        assert!(
            names.contains(&expected),
            "tools/list missing `{expected}`: {names:?}"
        );
    }
}

#[tokio::test]
async fn mcp_tools_call_snapshot_reads_through_extension_passthrough() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        mcp_post(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": { "name": "snapshot", "arguments": {} }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The handler resolved the GraphHandle from the request extensions and ran
    // do_snapshot — this is the RFC-003 §5.8 actor/handle passthrough proof.
    assert_ne!(
        body["result"]["isError"],
        json!(true),
        "snapshot tool errored: {body}"
    );
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .expect("text content block");
    let snapshot: Value = serde_json::from_str(text).expect("snapshot json payload");
    assert_eq!(snapshot["branch"], "main");
    assert!(
        snapshot["manifest_version"].is_number(),
        "snapshot carries a manifest_version: {snapshot}"
    );
}

#[tokio::test]
async fn mcp_tools_call_unknown_tool_is_jsonrpc_error() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        mcp_post(json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": { "name": "does_not_exist", "arguments": {} }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "JSON-RPC errors ride a 200");
    assert!(
        body["error"].is_object(),
        "unknown tool is a JSON-RPC error, not a result: {body}"
    );
}

/// A read-only actor (`act-reader`) and an admin (`act-admin`), so `tools/list`
/// filtering by Cedar action is observable.
const MCP_FILTER_POLICY_YAML: &str = r#"
version: 1
groups:
  readers: ["act-reader"]
  admins: ["act-admin"]
protected_branches: [main]
rules:
  - id: read-only
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
  - id: admin-data
    allow:
      actors: { group: admins }
      actions: [read, change, export]
      branch_scope: any
  - id: admin-targets
    allow:
      actors: { group: admins }
      actions: [schema_apply, branch_create, branch_delete, branch_merge]
      target_branch_scope: any
"#;

fn mcp_post_auth(body: Value, token: &str) -> Request<Body> {
    let mut request = mcp_post(body);
    request
        .headers_mut()
        .insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
    request
}

async fn mcp_tool_names(app: &Router, token: &str) -> Vec<String> {
    let (status, body) = json_response(
        app,
        mcp_post_auth(
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
            token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect()
}

#[tokio::test]
async fn mcp_tools_list_cedar_filters_by_policy() {
    let (_temp, app) = app_with_stored_queries(
        &[],
        &[("act-reader", "reader-tok"), ("act-admin", "admin-tok")],
        MCP_FILTER_POLICY_YAML,
    )
    .await;

    let reader = mcp_tool_names(&app, "reader-tok").await;
    assert!(reader.contains(&"snapshot".to_string()), "{reader:?}");
    assert!(reader.contains(&"query".to_string()), "{reader:?}");
    for hidden in ["mutate", "ingest", "branches_create", "branches_delete", "schema_apply"] {
        assert!(
            !reader.contains(&hidden.to_string()),
            "read-only actor must NOT see `{hidden}`: {reader:?}"
        );
    }

    let admin = mcp_tool_names(&app, "admin-tok").await;
    for visible in ["mutate", "ingest", "branches_create", "schema_apply"] {
        assert!(
            admin.contains(&visible.to_string()),
            "admin must see `{visible}`: {admin:?}"
        );
    }
}

#[tokio::test]
async fn mcp_tools_call_mutate_writes() {
    let (_temp, app) = app_for_loaded_graph_with_auth("admin-token").await;
    let (status, body) = json_response(
        &app,
        mcp_post_auth(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "mutate",
                    "arguments": {
                        "source": "query ins($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
                        "params": { "name": "Zelda", "age": 40 }
                    }
                }
            }),
            "admin-token",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(body["result"]["isError"], json!(true), "mutate failed: {body}");
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .expect("change envelope text");
    let change: Value = serde_json::from_str(text).expect("change json");
    assert!(
        change["affected_nodes"].as_u64().unwrap_or(0) >= 1,
        "mutate wrote a node through MCP: {change}"
    );
}

#[tokio::test]
async fn mcp_tools_call_denied_is_masked_as_unknown() {
    let (_temp, app) =
        app_with_stored_queries(&[], &[("act-reader", "reader-tok")], MCP_FILTER_POLICY_YAML).await;
    let (status, denied) = json_response(
        &app,
        mcp_post_auth(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "mutate", "arguments": { "source": "x" } }
            }),
            "reader-tok",
        ),
    )
    .await;
    let (_s, unknown) = json_response(
        &app,
        mcp_post_auth(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "does_not_exist", "arguments": {} }
            }),
            "reader-tok",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Deny ≡ unknown: a denied existing tool is reported with the SAME error
    // code and the SAME `unknown tool: <name>` template as a truly-unknown tool
    // (the echoed name is the one the caller already supplied), so an
    // unauthorized caller cannot tell "denied" from "does not exist".
    assert_eq!(denied["error"]["code"], json!(-32602));
    assert_eq!(denied["error"]["code"], unknown["error"]["code"]);
    assert_eq!(denied["error"]["message"], json!("unknown tool: mutate"));
    assert_eq!(
        unknown["error"]["message"],
        json!("unknown tool: does_not_exist")
    );
}

#[tokio::test]
async fn mcp_tools_call_business_error_is_iserror() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        mcp_post(json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "query", "arguments": { "source": "this is not valid gq" } }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A bad query is a tool-execution error the model can self-correct on, NOT a
    // JSON-RPC protocol error (the 2025-11-25 SEP-1303 split).
    assert_eq!(
        body["result"]["isError"],
        json!(true),
        "malformed GQ → isError result: {body}"
    );
    assert!(body["error"].is_null(), "not a JSON-RPC error: {body}");
}

#[tokio::test]
async fn mcp_tool_annotations_match_read_write() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (_s, body) = json_response(
        &app,
        mcp_post(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })),
    )
    .await;
    let tools = body["result"]["tools"].as_array().expect("tools array");
    let find = |name: &str| {
        tools
            .iter()
            .find(|t| t["name"] == json!(name))
            .unwrap_or_else(|| panic!("tool `{name}` listed"))
            .clone()
    };
    assert_eq!(find("snapshot")["annotations"]["readOnlyHint"], json!(true));
    let schema_apply = find("schema_apply");
    assert_eq!(schema_apply["annotations"]["readOnlyHint"], json!(false));
    assert_eq!(
        schema_apply["annotations"]["destructiveHint"],
        json!(true),
        "schema_apply is destructive: {schema_apply}"
    );
}

#[tokio::test]
async fn schema_apply_route_updates_graph_for_authorized_admin() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        reopened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_rejects_stored_query_breakage_before_publish() {
    let (temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-ragnor", "admin-token")],
        STORED_QUERY_SCHEMA_APPLY_POLICY_YAML,
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
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {payload}");
    let message = payload["error"].as_str().unwrap_or_default();
    assert!(
        message.contains("find_person") && message.contains("schema check"),
        "registry breakage should name the stored query; body: {payload}"
    );

    let reopened = Omnigraph::open(graph_path(temp.path()).to_str().unwrap())
        .await
        .unwrap();
    let person = &reopened.catalog().node_types["Person"];
    assert!(person.properties.contains_key("age"));
    assert!(!person.properties.contains_key("years"));

    let (invoke_status, invoke_body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "admin-token",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(invoke_status, StatusCode::OK, "body: {invoke_body}");
    assert_eq!(invoke_body["row_count"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_noop_keeps_valid_stored_query_registry() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-ragnor", "admin-token")],
        STORED_QUERY_SCHEMA_APPLY_POLICY_YAML,
    )
    .await;

    let request = Request::builder()
        .method(Method::POST)
        .uri("/schema/apply")
        .header("content-type", "application/json")
        .header("authorization", "Bearer admin-token")
        .body(Body::from(
            serde_json::to_vec(&SchemaApplyRequest {
                schema_source: fs::read_to_string(fixture("test.pg")).unwrap(),
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::OK, "body: {payload}");
    assert_eq!(payload["applied"], false);
}

#[tokio::test]
async fn schema_apply_route_requires_schema_apply_policy_permission() {
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
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
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
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
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(snapshot.entry("node:Human").is_some());
    assert!(snapshot.entry("node:Person").is_none());
}

#[tokio::test]
async fn schema_apply_route_can_rename_property() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let graph = graph_path(temp.path());
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let person = &reopened.catalog().node_types["Person"];
    assert!(person.properties.contains_key("years"));
    assert!(!person.properties.contains_key("age"));
}

#[tokio::test]
async fn schema_apply_route_can_add_index() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());
    let before_index_count = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
                ..Default::default()
            })
            .unwrap(),
        ))
        .unwrap();
    let (status, payload) = json_response(&app, request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
    let (_temp, app) = app_for_graph_with_auth_tokens_and_policy(
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
                ..Default::default()
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
    let temp = init_graph_with_schema(&fs::read_to_string(fixture("test.pg")).unwrap()).await;
    let graph = graph_path(temp.path());
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create("feature").await.unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, SCHEMA_APPLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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
                ..Default::default()
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
    let (_temp, app) = app_for_loaded_graph().await;
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
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    fs::write(graph.join("_schema.pg"), drifted_test_schema()).unwrap();

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
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
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
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;

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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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

    // MR-723: tokens-without-policy is now default-deny. Install a
    // permit-all policy alongside the bearer token so /export
    // (action=Export) passes Cedar evaluation. The test is exercising
    // export semantics, not policy — the policy is just enough to clear
    // the State 3 path.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["default"])).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("default".to_string(), token.to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
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
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[
        ("team-01", "token-one"),
        ("team-02", "token-two"),
    ])
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
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
        graph.to_string_lossy().to_string(),
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

/// Regression test for MR-731: actor identity comes from the matched
/// bearer token, never from a client-supplied request header. A future
/// "convenience" PR that lets clients override `actor_id` to spoof
/// another identity must break this test. The principle is named in
/// `docs/dev/invariants.md` Hard Invariant 11 and at the actor-resolution
/// site in `omnigraph-server/src/lib.rs::authorize_request`.
///
/// Two assertions in one fixture:
/// 1. Spoof-up: bearer for a *denied* actor + X-Actor-Id naming an
///    *allowed* actor — policy still denies (proves the spoof header
///    doesn't promote the request).
/// 2. Spoof-down: bearer for an *allowed* actor + X-Actor-Id naming a
///    *denied* actor — policy still allows (proves the server-resolved
///    identity wins; the spoof can't trick the request into a denial
///    either, which would otherwise be a confusing UX trap).
///
/// Cross-reference: MR-777 covers boundary cases like actor-id
/// *collision* (two distinct tokens minting the same actor_id) and
/// malformed bearer header parsing. See `auth_boundary_case_coverage`
/// suite when it lands; the two tests together pin the full bearer-token
/// → actor identity contract.
#[tokio::test(flavor = "multi_thread")]
async fn actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    // Same readers/writers split as
    // `bearer_token_resolves_to_correct_actor_for_policy_decisions` —
    // `act-a` can read main, `act-b` cannot. The asymmetry is what
    // makes the spoof-up/spoof-down distinction observable.
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
        graph.to_string_lossy().to_string(),
        vec![
            ("act-a".to_string(), "token-a".to_string()),
            ("act-b".to_string(), "token-b".to_string()),
        ],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    // (1) Spoof-up: bearer for act-b (denied) + X-Actor-Id: act-a (allowed).
    // If the server were trusting the header, this would succeed as
    // act-a. The contract is: the bearer wins. Expect 403 because
    // act-b can't read.
    let (spoof_up_status, spoof_up_body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .header("x-actor-id", "act-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let spoof_up_error: ErrorOutput = serde_json::from_value(spoof_up_body).unwrap();
    assert_eq!(
        spoof_up_status,
        StatusCode::FORBIDDEN,
        "X-Actor-Id must not promote a denied bearer to an allowed actor",
    );
    assert_eq!(
        spoof_up_error.code,
        Some(omnigraph_server::api::ErrorCode::Forbidden),
    );

    // (2) Spoof-down: bearer for act-a (allowed) + X-Actor-Id: act-b (denied).
    // If the server were trusting the header, this would fail as act-b.
    // The contract is: the bearer wins. Expect 200 because act-a can read.
    let (spoof_down_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-a")
            .header("x-actor-id", "act-b")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        spoof_down_status,
        StatusCode::OK,
        "X-Actor-Id must not demote an allowed bearer to a denied actor",
    );

    // (3) Empty-string spoof attempt: an X-Actor-Id of "" must not
    // leak through as the policy subject. Same expectation as (1):
    // bearer for act-b is denied regardless of what the header tries.
    let (empty_spoof_status, _) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot?branch=main")
            .method(Method::GET)
            .header("authorization", "Bearer token-b")
            .header("x-actor-id", "")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        empty_spoof_status,
        StatusCode::FORBIDDEN,
        "empty X-Actor-Id must not clear the resolved actor",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_allows_read_but_distinguishes_401_from_403() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let snapshot_id = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.resolve_snapshot("main").await.unwrap().to_string()
    };
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_PROTECTED_READ_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let expected_manifest_version = manifest_dataset_version(&graph).await;

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
    let (_temp, app) = app_for_loaded_graph().await;
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
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;

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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    // Policy grants branch_create only — no read action for act-bruno.
    fs::write(&policy_path, INGEST_CREATE_ONLY_POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("act-bruno".to_string(), "team-token".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap();
    let app = build_app(state);

    let main_change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
        graph.to_string_lossy().to_string(),
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
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[("act-andrew", "token-one")]).await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
    let (temp, app) = app_for_loaded_graph_with_auth_tokens(&[("act-andrew", "token-one")]).await;
    let graph = graph_path(temp.path());
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

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    {
        let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.branch_create_from(ReadTarget::branch("main"), "feature")
            .await
            .unwrap();
    }
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
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
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
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
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
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
    let (_temp, app) = app_for_loaded_graph().await;
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
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[
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
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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

    let state = AppState::open(graph.to_string_lossy().to_string())
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
    let (_temp, app) = app_for_loaded_graph().await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
async fn query_endpoint_runs_inline_read() {
    let (_temp, app) = app_for_loaded_graph().await;

    let query = QueryRequest {
        query: fs::read_to_string(fixture("test.gq")).unwrap(),
        name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/query")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&query).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["query_name"], "get_person");
    assert_eq!(body["row_count"], 1);
    assert_eq!(body["rows"][0]["p.name"], "Alice");
}

#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_rejects_mutation_with_400() {
    let (_temp, app) = app_for_loaded_graph().await;

    let query = QueryRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Should", "age": 1 })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/query")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&query).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("contains mutations") && err.contains("POST /mutate"),
        "expected mutation-rejection message pointing at canonical /mutate, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mutate_endpoint_runs_inline_mutation() {
    // Canonical mutation endpoint. Pairs with `/query` on the read side.
    // Same wire shape as `/change`, no deprecation signal.
    let (_temp, app) = app_for_loaded_graph().await;

    let request = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": { "name": "Mutie", "age": 30 },
        "branch": "main",
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/mutate")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // Canonical route is NOT deprecated; no Deprecation header expected.
    assert!(
        response.headers().get("deprecation").is_none(),
        "POST /mutate must not advertise itself as deprecated"
    );
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["affected_nodes"], 1);
    assert_eq!(body["query_name"], "insert_person");
    assert_eq!(body["branch"], "main");
}

#[tokio::test(flavor = "multi_thread")]
async fn change_endpoint_emits_deprecation_headers() {
    // `/change` is kept indefinitely for back-compat but flagged at runtime
    // per RFC 9745 (`Deprecation: true`) + RFC 8288 (`Link: </mutate>;
    // rel="successor-version"`). The OpenAPI side is covered by
    // `openapi_change_is_deprecated` in tests/openapi.rs.
    let (_temp, app) = app_for_loaded_graph().await;

    let request = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": { "name": "Legacyer", "age": 33 },
        "branch": "main",
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/change")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("deprecation")
            .and_then(|v| v.to_str().ok()),
        Some("true"),
        "POST /change must advertise `Deprecation: true` (RFC 9745)"
    );
    assert_eq!(
        response.headers().get("link").and_then(|v| v.to_str().ok()),
        Some("</mutate>; rel=\"successor-version\""),
        "POST /change must point at /mutate via `Link` rel=successor-version (RFC 8288)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_endpoint_emits_deprecation_headers() {
    // `/read` is kept indefinitely for byte-stable back-compat but flagged
    // at runtime per RFC 9745 + RFC 8288. Successor is `/query`.
    let (_temp, app) = app_for_loaded_graph().await;

    let request = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/read")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("deprecation")
            .and_then(|v| v.to_str().ok()),
        Some("true"),
        "POST /read must advertise `Deprecation: true` (RFC 9745)"
    );
    assert_eq!(
        response.headers().get("link").and_then(|v| v.to_str().ok()),
        Some("</query>; rel=\"successor-version\""),
        "POST /read must point at /query via `Link` rel=successor-version (RFC 8288)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_does_not_emit_deprecation_headers() {
    // Sanity check the inverse: the canonical `/query` endpoint must not
    // carry deprecation signaling, so SDK codegens don't propagate a
    // bogus `@deprecated` marker.
    let (_temp, app) = app_for_loaded_graph().await;

    let request = QueryRequest {
        query: fs::read_to_string(fixture("test.gq")).unwrap(),
        name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/query")
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("deprecation").is_none(),
        "POST /query is canonical and must not advertise itself as deprecated"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn change_endpoint_accepts_legacy_field_names() {
    // The canonical wire field names on /change are `query` and `name`, but
    // serde aliases keep the legacy `query_source`/`query_name` payload
    // shape working for clients that haven't migrated yet. Pin both shapes.
    let (_temp, app) = app_for_loaded_graph().await;

    let legacy_body = json!({
        "query_source": MUTATION_QUERIES,
        "query_name": "insert_person",
        "params": { "name": "Legacy", "age": 21 },
        "branch": "main",
    });
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&legacy_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_nodes"], 1);

    let canonical_body = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": { "name": "Canonical", "age": 22 },
        "branch": "main",
    });
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&canonical_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_nodes"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_branch_list_create_merge_flow_works() {
    let (_temp, app) = app_for_loaded_graph().await;

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
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
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
    let (_temp, app) = app_for_loaded_graph().await;

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
    let (temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-andrew", "token-admin"), ("act-bruno", "token-team")],
        POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
async fn server_opens_s3_graph_directly_and_serves_snapshot_and_read() {
    let Some(uri) = s3_test_graph_uri("server") else {
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
    let temp = init_graph_with_schema_and_data(EMBED_SCHEMA, &data).await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());

    // Build the server first so its handle pins the pre-mutation manifest
    // version. Then advance the manifest from outside the server. The
    // server's next /change call will capture stale `expected_versions`
    // (from its still-pinned snapshot) and the publisher's CAS rejects.
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    {
        let mut db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
                    query: MUTATION_QUERIES.to_string(),
                    name: Some("set_age".to_string()),
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
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
                query: MUTATION_QUERIES.to_string(),
                name: Some("insert_person".to_string()),
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
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
                query: MUTATION_QUERIES.to_string(),
                name: Some("set_age".to_string()),
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

    let ok_count = statuses.iter().filter(|s| **s == StatusCode::OK).count();
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
        ok_count,
        1,
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
                query_source: include_str!("../../omnigraph/tests/fixtures/test.gq").to_string(),
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
                    query: MUTATION_QUERIES.to_string(),
                    name: Some("insert_person".to_string()),
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
                matrix::op_merge("feature-a-cella".to_string(), "target-a-cella".to_string()),
                matrix::op_merge("feature-b-cella".to_string(), "target-b-cella".to_string()),
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
        h.insert_person("src-shared-cellc", "Sharon-cellc", 50)
            .await;
        h.create_branch("main", "tgt-1-cellc").await;
        h.create_branch("main", "tgt-2-cellc").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-shared-cellc".to_string(), "tgt-1-cellc".to_string()),
                matrix::op_merge("src-shared-cellc".to_string(), "tgt-2-cellc".to_string()),
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
            assert_eq!(
                conflict.table_key, "node:Person",
                "[{}] conflict table",
                cell
            );
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
                matrix::op_branch_create("alpha-cellf".to_string(), "gamma-cellf".to_string()),
                matrix::op_branch_create("beta-cellf".to_string(), "delta-cellf".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] gamma create", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delta create", cell);
        // gamma forks off alpha → must contain Eve.
        h.assert_persons("gamma-cellf", cell, &["Eve-cellf"], &[])
            .await;
        // delta forks off beta → must NOT contain Eve.
        h.assert_persons("delta-cellf", cell, &[], &["Eve-cellf"])
            .await;
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
        h.assert_persons("newborn-cellg", cell, &["Alice"], &[])
            .await;
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
            assert_eq!(
                conflict.table_key, "node:Person",
                "[{}] conflict table",
                cell
            );
            h.assert_persons("main", cell, &["Steve-cellk"], &["Rita-cellk"])
                .await;
        }

        // Reopen via a fresh AppState on the same graph.
        let graph_uri = format!("{}/server.omni", h._temp.path().display());
        let reopened = AppState::open(graph_uri.clone()).await.unwrap();
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

    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    let mut handles = Vec::with_capacity(PER_TYPE * 2);
    for i in 0..PER_TYPE {
        let app_p = app.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::to_vec(&ChangeRequest {
                query: PERSON_QUERY.to_string(),
                name: Some("insert_p".to_string()),
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
                query: COMPANY_QUERY.to_string(),
                name: Some("insert_c".to_string()),
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
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|t| t["table_key"].as_str() == Some(table_key))
            })
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
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let workload = omnigraph_server::workload::WorkloadController::new(
        1,             // per-actor in-flight cap (the fixture under test)
        1_000_000_000, // per-actor byte budget — large so it never bottlenecks
    );
    // MR-723: install a permit-all policy alongside the bearer token so
    // /ingest (action=Change) passes Cedar evaluation. The test is
    // exercising the admission cap, not policy — the policy is just
    // enough to clear the State 3 path so the test reaches workload.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["act-flooder"])).unwrap();
    let policy_engine =
        omnigraph_server::PolicyEngine::load_graph(&policy_path, graph.to_string_lossy().as_ref())
            .unwrap();
    let state = AppState::new_single(
        graph.to_string_lossy().to_string(),
        db,
        vec![("act-flooder".to_string(), "flooder-token".to_string())],
        Some(policy_engine),
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

/// Regression for B2 (MR-668): when an `AppState` is built with a
/// per-graph policy and a custom workload, the engine inside the
/// routing's `GraphHandle` MUST have the same policy applied via
/// `Omnigraph::with_policy`. Pre-fix, `new_with_workload(...).with_policy_engine(p)`
/// installed the policy only on the HTTP-layer `handle.policy`; the
/// underlying `Arc<Omnigraph>` was reused without `with_policy`, so any
/// caller reaching through `state.routing()` could bypass Cedar.
///
/// This test reaches the engine the same way an embedded SDK consumer
/// or future routing code path would, and asserts the policy still
/// fires. The deny path is "act-blocked has a valid bearer but isn't in
/// the policy's allowed group" — i.e., authenticated-but-unauthorised.
#[tokio::test(flavor = "multi_thread")]
async fn engine_layer_policy_fires_via_direct_arc_omnigraph_from_new_single() {
    use omnigraph_server::GraphRouting;
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();

    // Permit `act-allowed` for change actions; `act-blocked` is not in
    // any allowed group — every change request from them must deny.
    let policy_path = temp.path().join("policy.yaml");
    fs::write(&policy_path, permit_all_policy_yaml(&["act-allowed"])).unwrap();
    let policy_engine =
        omnigraph_server::PolicyEngine::load_graph(&policy_path, graph.to_string_lossy().as_ref())
            .unwrap();

    let workload = omnigraph_server::workload::WorkloadController::new(100, 1_000_000_000);
    let state = AppState::new_single(
        graph.to_string_lossy().to_string(),
        db,
        vec![("act-blocked".to_string(), "block-token".to_string())],
        Some(policy_engine),
        workload,
    );

    // Reach into the routing and pull the engine the same way an
    // embedded consumer holding `Arc<Omnigraph>` would. If `new_single`
    // failed to apply `with_policy` to the engine, this `mutate_as`
    // would succeed — the HTTP-layer is bypassed entirely.
    let handle = match state.routing() {
        GraphRouting::Single { handle } => Arc::clone(handle),
        GraphRouting::Multi { .. } => panic!("expected single-mode routing"),
    };
    let engine = Arc::clone(&handle.engine);

    let mut params: omnigraph_compiler::ParamMap = Default::default();
    params.insert(
        "name".to_string(),
        omnigraph_compiler::Literal::String("EngineLayerBlocked".to_string()),
    );
    params.insert("age".to_string(), omnigraph_compiler::Literal::Integer(30));
    let result = engine
        .mutate_as(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &params,
            Some("act-blocked"),
        )
        .await;
    match result {
        Err(OmniError::Policy(_)) => { /* expected — engine-layer gate fired */ }
        Ok(_) => panic!(
            "engine-layer policy did NOT fire — act-blocked successfully ran mutate_as via \
             the engine pulled from the registry handle. AppState::new_single failed to apply \
             with_policy to the underlying Omnigraph engine. This is the B2 footgun the \
             with_policy_engine deletion was supposed to close."
        ),
        Err(other) => panic!("expected OmniError::Policy, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_request_body_returns_payload_too_large() {
    let (_temp, app) = app_for_loaded_graph().await;
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

// ─── MR-723 default-deny mode (State 2: tokens without policy) ──────────
//
// `authorize_request` returns 403 for every action except `Read` when a
// PolicyEngine is not installed but bearer tokens are configured. Pinned
// by the three tests below — Read allowed, Change/SchemaApply denied —
// to prevent regressing back to the pre-MR-723 "tokens configured but
// no policy = fully open" trap.

#[tokio::test(flavor = "multi_thread")]
async fn default_deny_mode_allows_read_for_authenticated_actor() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-andrew", "demo-token")],
    )
    .await;

    let (status, _body) = json_response(
        &app,
        Request::builder()
            .uri("/snapshot")
            .method(Method::GET)
            .header(AUTHORIZATION, "Bearer demo-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_deny_mode_rejects_change_with_forbidden() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-andrew", "demo-token")],
    )
    .await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "DefaultDeny", "age": 1 })),
        branch: Some("main".to_string()),
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/change")
            .method(Method::POST)
            .header(AUTHORIZATION, "Bearer demo-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert!(
        error.error.contains("default-deny"),
        "expected default-deny in error message, got: {}",
        error.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn default_deny_mode_rejects_schema_apply_with_forbidden() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-andrew", "demo-token")],
    )
    .await;

    let req = SchemaApplyRequest {
        schema_source: additive_schema_with_nickname(),
        ..Default::default()
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/schema/apply")
            .method(Method::POST)
            .header(AUTHORIZATION, "Bearer demo-token")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&req).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert!(
        error.error.contains("default-deny"),
        "expected default-deny in error message, got: {}",
        error.error
    );
}

// ─── SDK ↔ HTTP decision parity (MR-722 PR A) ─────────────────────────────
//
// Engine and HTTP both consult Cedar via `PolicyChecker::check()`; by
// construction they cannot disagree on a decision. These tests pin that
// property explicitly so a future refactor that introduces a separate
// auth path (or copy-pastes Cedar evaluation logic) turns red.
//
// Four cases cover the per-action scope shapes:
// * Change on a protected branch via `mutate_as` / POST /change
// * Change with an actor that has no permit
// * BranchMerge to a protected target via `branch_merge_as` / POST /branches/merge
// * BranchMerge with an actor that has no permit

const PARITY_POLICY_YAML: &str = r#"
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
enum ParityDecision {
    Allow,
    Deny,
}

async fn build_parity_graph() -> (tempfile::TempDir, PathBuf, PathBuf) {
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

async fn sdk_change_decision(graph: &Path, policy_path: &Path, actor: &str) -> ParityDecision {
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

async fn http_change_decision(
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
            .uri("/change")
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

async fn sdk_merge_decision(graph: &Path, policy_path: &Path, actor: &str) -> ParityDecision {
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

async fn http_merge_decision(
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
            .uri("/branches/merge")
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

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_change_admin_on_main_allowed() {
    // (act-ragnor, change, main) — admins-change-anywhere rule applies.
    // Both SDK and HTTP must allow. Each path uses its own fresh graph
    // because allow→side-effects.
    let (_t1, graph1, policy1) = build_parity_graph().await;
    let sdk = sdk_change_decision(&graph1, &policy1, "act-ragnor").await;
    let (_t2, graph2, policy2) = build_parity_graph().await;
    let http = http_change_decision(&graph2, &policy2, "act-ragnor", "ragnor-token").await;
    assert!(
        matches!(sdk, ParityDecision::Allow) && matches!(http, ParityDecision::Allow),
        "SDK={sdk:?} HTTP={http:?} — should both Allow",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_change_team_on_main_denied() {
    // (act-bruno, change, main) — no rule grants bruno change on
    // protected. Both SDK and HTTP must deny. Same graph is reusable
    // because deny→no side-effects.
    let (_temp, graph, policy) = build_parity_graph().await;
    let sdk = sdk_change_decision(&graph, &policy, "act-bruno").await;
    let http = http_change_decision(&graph, &policy, "act-bruno", "bruno-token").await;
    assert!(
        matches!(sdk, ParityDecision::Deny) && matches!(http, ParityDecision::Deny),
        "SDK={sdk:?} HTTP={http:?} — should both Deny",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_branch_merge_admin_allowed() {
    // (act-ragnor, branch_merge, feature→main) — admins-merge-to-protected
    // rule applies. Both Allow. Each path uses its own fresh graph —
    // a successful merge consumes the feature branch's commit on main.
    let (_t1, graph1, policy1) = build_parity_graph().await;
    let sdk = sdk_merge_decision(&graph1, &policy1, "act-ragnor").await;
    let (_t2, graph2, policy2) = build_parity_graph().await;
    let http = http_merge_decision(&graph2, &policy2, "act-ragnor", "ragnor-token").await;
    assert!(
        matches!(sdk, ParityDecision::Allow) && matches!(http, ParityDecision::Allow),
        "SDK={sdk:?} HTTP={http:?} — should both Allow",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_decision_parity_branch_merge_team_denied() {
    // (act-bruno, branch_merge, feature→main) — no rule grants bruno
    // branch_merge. Both Deny.
    let (_temp, graph, policy) = build_parity_graph().await;
    let sdk = sdk_merge_decision(&graph, &policy, "act-bruno").await;
    let http = http_merge_decision(&graph, &policy, "act-bruno", "bruno-token").await;
    assert!(
        matches!(sdk, ParityDecision::Deny) && matches!(http, ParityDecision::Deny),
        "SDK={sdk:?} HTTP={http:?} — should both Deny",
    );
}

// ─── MR-694 PR B: HTTP soft + hard drop semantics + data preservation ────
//
// SDK-level drop semantics are pinned in `crates/omnigraph/tests/schema_apply.rs`.
// These HTTP-side tests mirror the assertions through POST /schema/apply
// and exercise the new `allow_data_loss` field (closes the gap where
// the schema-lint chassis v1.2 shipped Hard mode on the CLI but the
// HTTP request struct had no equivalent field).

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_soft_drops_property_via_http() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    // Load a row that has the column we're about to drop.
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            r#"{"type":"Person","data":{"name":"PreDrop","age":42}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }
    let pre_version = manifest_dataset_version(&graph).await;

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/schema/apply")
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Catalog reflects the drop: `age` is gone from the live schema.
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types["Person"]
            .properties
            .contains_key("age"),
        "catalog should not contain `age` after drop"
    );

    // Soft drop preserves the prior version — `age` is still readable
    // via time travel to the pre-drop manifest version. Mirrors the
    // SDK-side assertion in `apply_schema_drops_a_nullable_property_softly_preserves_prior_version`.
    let pre_drop_snapshot = reopened.snapshot_at_version(pre_version).await.unwrap();
    let pre_drop_ds = pre_drop_snapshot.open("node:Person").await.unwrap();
    let pre_drop_fields = pre_drop_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        pre_drop_fields.iter().any(|f| f == "age"),
        "soft drop should leave the pre-drop dataset's `age` column \
         time-travel-reachable; got fields {pre_drop_fields:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_soft_drops_node_type_via_http() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/schema/apply")
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_company(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types.contains_key("Company"),
        "catalog should not contain `Company` after drop"
    );
    assert!(
        !reopened.catalog().edge_types.contains_key("WorksAt"),
        "catalog should not contain `WorksAt` after cascade"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_hard_drops_property_with_allow_data_loss() {
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            r#"{"type":"Person","data":{"name":"PreDropHard","age":50}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();
    }

    // Apply with allow_data_loss=true → Hard mode promotion.
    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/schema/apply")
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    allow_data_loss: true,
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Catalog reflects the drop.
    let reopened = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !reopened.catalog().node_types["Person"]
            .properties
            .contains_key("age"),
        "catalog should not contain `age` after Hard drop"
    );
    // Plan steps should show DropMode::Hard for property drops.
    let steps = payload["steps"].as_array().expect("steps array");
    let drop_step = steps
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include drop_property step");
    let mode = &drop_step["mode"];
    assert_eq!(
        mode, "hard",
        "expected hard mode under allow_data_loss=true"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_keeps_drops_soft_without_flag() {
    // Symmetric to the Hard test: same schema change, but no
    // allow_data_loss flag → drops stay Soft (prior column data
    // remains time-travel-reachable). Pins the default semantics
    // against accidental Hard promotion.
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/schema/apply")
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: schema_without_age(),
                    allow_data_loss: false,
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    let steps = payload["steps"].as_array().expect("steps array");
    let drop_step = steps
        .iter()
        .find(|s| s["kind"] == "drop_property")
        .expect("plan should include drop_property step");
    let mode = &drop_step["mode"];
    assert_eq!(mode, "soft", "expected soft mode without allow_data_loss");
    let _ = graph;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_additive_property_preserves_existing_rows() {
    // SDK suite covers rename and drop data preservation. Additive
    // AddProperty wasn't pinned with a row-count check anywhere.
    // Load N rows, apply schema adding nullable property, verify
    // every row is still readable and the new column is null.
    let (temp, app) = app_for_graph_with_auth_tokens_and_policy(
        &fs::read_to_string(fixture("test.pg")).unwrap(),
        &[("act-ragnor", "admin-token")],
        SCHEMA_APPLY_POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    // Standard fixture data: 4 Persons + 1 Company. Load it.
    let pre_count = {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.load(
            "main",
            &fs::read_to_string(fixture("test.jsonl")).unwrap(),
            LoadMode::Append,
        )
        .await
        .unwrap();
        let snap = db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        snap.entry("node:Person").expect("Person").row_count
    };
    assert!(pre_count > 0, "fixture should have loaded Person rows");

    let (status, payload) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/schema/apply")
            .header("content-type", "application/json")
            .header("authorization", "Bearer admin-token")
            .body(Body::from(
                serde_json::to_vec(&SchemaApplyRequest {
                    schema_source: additive_schema_with_nickname(),
                    ..Default::default()
                })
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["applied"], true);

    // Row count preserved.
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snap = db
        .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    let post_count = snap.entry("node:Person").expect("Person").row_count;
    assert_eq!(
        post_count, pre_count,
        "AddProperty should preserve row count",
    );
}

// ─── MR-668: multi-graph startup ──────────────────────────────────────────

mod multi_graph_startup {
    use super::*;
    use omnigraph::storage::normalize_root_uri;
    use omnigraph_server::{
        GraphHandle, GraphId, GraphKey, GraphRegistry, InsertError, ServerConfig, ServerConfigMode,
        load_server_settings,
    };
    use std::sync::Arc;

    async fn build_multi_mode_app(graph_ids: &[&str]) -> (Vec<tempfile::TempDir>, Router) {
        let mut dirs = Vec::with_capacity(graph_ids.len());
        let mut handles = Vec::with_capacity(graph_ids.len());
        for id in graph_ids {
            let dir = tempfile::tempdir().unwrap();
            let graph_uri = dir.path().join(id).to_str().unwrap().to_string();
            let schema = fs::read_to_string(fixture("test.pg")).unwrap();
            let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
            handles.push(Arc::new(GraphHandle {
                key: GraphKey::cluster(GraphId::try_from(*id).unwrap()),
                uri: graph_uri,
                engine: Arc::new(engine),
                policy: None,
                queries: None,
            }));
            dirs.push(dir);
        }
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let state = AppState::new_multi(handles, Vec::new(), None, workload, None).unwrap();
        let app = build_app(state);
        (dirs, app)
    }

    /// Cluster route `/graphs/{graph_id}/snapshot` resolves to the right
    /// engine. Two graphs side by side; assert each responds to its own
    /// id and does NOT respond to the other's URL.
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_routes_dispatch_per_graph_handle() {
        let (_dirs, app) = build_multi_mode_app(&["alpha", "beta"]).await;
        for id in ["alpha", "beta"] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri(format!("/graphs/{id}/snapshot?branch=main"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "graph '{id}' must respond OK on its cluster snapshot route"
            );
        }
    }

    /// Unknown graph id under the cluster prefix yields 404 (not 500,
    /// not 410 — `Gone` is reserved for the future DELETE flow).
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_route_for_unknown_graph_returns_404() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs/nonexistent/snapshot?branch=main")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Coverage net for cluster-route regressions across every
    /// protected handler — not just the few that have inner path
    /// params. Bug-1 surfaced because only `/snapshot` was being
    /// exercised in cluster mode, leaving the other six protected
    /// routes implicitly untested. This sweep hits each one and
    /// asserts the response shows the handler was reached: no 404
    /// (router didn't match), no 500 with "Wrong number of path
    /// arguments" (path extractor broke), no 500 with "missing
    /// extension" (routing middleware didn't inject the handle).
    ///
    /// Status codes are negative assertions because each handler's
    /// happy-path inputs differ — what matters is "the request
    /// reached the handler," not "the handler returned 200." The
    /// individual handlers' logic is already tested in single mode.
    #[tokio::test(flavor = "multi_thread")]
    async fn all_protected_cluster_routes_resolve_to_their_handler() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;

        // (method, path, body) — one minimal request per protected
        // cluster route. Bodies are valid enough that the router and
        // extractors succeed; whether the engine ultimately returns
        // 200 or 4xx is per-handler and not what this test pins.
        let cases: &[(Method, &str, Option<&str>)] = &[
            (Method::GET, "/graphs/alpha/snapshot?branch=main", None),
            (Method::GET, "/graphs/alpha/schema", None),
            (Method::GET, "/graphs/alpha/branches", None),
            (Method::GET, "/graphs/alpha/commits", None),
            (
                Method::POST,
                "/graphs/alpha/read",
                Some(r#"{"query_source":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/change",
                Some(r#"{"query_source":"query q() { return {} }"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/export",
                Some(r#"{"branch":"main"}"#),
            ),
            (
                Method::POST,
                "/graphs/alpha/schema/apply",
                Some(r#"{"schema_source":"","allow_data_loss":false}"#),
            ),
            (Method::POST, "/graphs/alpha/ingest", Some(r#"{"data":""}"#)),
            (
                Method::POST,
                "/graphs/alpha/branches/merge",
                Some(r#"{"source":"main","target":"main"}"#),
            ),
        ];

        for (method, path, body) in cases {
            let req_body = body
                .map(|s| Body::from(s.to_string()))
                .unwrap_or_else(Body::empty);
            let req = Request::builder()
                .method(method.clone())
                .uri(*path)
                .header("content-type", "application/json")
                .body(req_body)
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let body_str = String::from_utf8_lossy(&bytes);

            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{} {} — router didn't match (cluster-route mounting regression). Body: {}",
                method,
                path,
                body_str,
            );
            assert!(
                !(status == StatusCode::INTERNAL_SERVER_ERROR
                    && body_str.contains("Wrong number of path arguments")),
                "{} {} — path extractor broke (Bug-1 class regression). Body: {}",
                method,
                path,
                body_str,
            );
            assert!(
                !(status == StatusCode::INTERNAL_SERVER_ERROR
                    && body_str.to_lowercase().contains("missing extension")),
                "{} {} — routing middleware didn't inject GraphHandle. Body: {}",
                method,
                path,
                body_str,
            );
        }
    }

    /// Regression for the bot-surfaced path-extractor bug: cluster
    /// routes whose inner path also captures a parameter
    /// (`/graphs/{graph_id}/branches/{branch}`,
    /// `/graphs/{graph_id}/commits/{commit_id}`) must extract the
    /// inner param cleanly. Axum 0.8 propagates the outer `{graph_id}`
    /// capture into nested handlers, so a `Path<String>` extractor
    /// would see two values and fail with "Wrong number of path
    /// arguments. Expected 1 but got 2." Today both DELETE branch and
    /// GET commit-by-id break in multi-mode because their handlers
    /// use bare `Path<String>` — this test pins the fix.
    ///
    /// The broader `all_protected_cluster_routes_resolve_to_their_handler`
    /// test sweeps the full route surface; this one stays narrowly
    /// targeted at the inner-path-param shape because that's the
    /// specific regression class.
    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_routes_with_inner_path_params_deserialize_correctly() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;

        // Create a branch we can then delete — DELETE /graphs/alpha/branches/feature
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/graphs/alpha/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"feature"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            create_resp.status(),
            StatusCode::OK,
            "branch create on the cluster route must succeed before delete can be tested"
        );

        // DELETE /graphs/{graph_id}/branches/{branch} — exercises a handler
        // whose only Path extractor (`branch`) is inside a nested route
        // that also captures `graph_id`. The handler must pick `branch`
        // by name, not by position.
        let delete_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/graphs/alpha/branches/feature")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let delete_status = delete_resp.status();
        let delete_body = to_bytes(delete_resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            delete_status,
            StatusCode::OK,
            "DELETE /graphs/{{id}}/branches/{{branch}} must extract `branch` cleanly. \
             Body: {}",
            String::from_utf8_lossy(&delete_body),
        );

        // GET /graphs/{graph_id}/commits/{commit_id} — same shape: the
        // handler's only Path extractor is the inner `commit_id`, which
        // must deserialize by name even though `graph_id` is also in scope.
        // We don't know a real commit_id, but the failure mode under test
        // is path extraction, not commit lookup — a 404 from the engine
        // is fine; a 500 with "Wrong number of path arguments" is the bug.
        let commit_resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs/alpha/commits/0000000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let commit_status = commit_resp.status();
        let commit_body = to_bytes(commit_resp.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&commit_body);
        assert!(
            commit_status != StatusCode::INTERNAL_SERVER_ERROR
                || !body_str.contains("Wrong number of path arguments"),
            "GET /graphs/{{id}}/commits/{{commit_id}} must extract `commit_id` cleanly. \
             Got: {} | {}",
            commit_status,
            body_str,
        );
    }

    /// Flat routes 404 in multi mode — the router only mounts under
    /// `/graphs/{graph_id}/...` so `/snapshot` doesn't resolve.
    #[tokio::test(flavor = "multi_thread")]
    async fn flat_routes_404_in_multi_mode() {
        let (_dirs, app) = build_multi_mode_app(&["alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/snapshot?branch=main")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// `GraphId` validation runs at startup — a reserved name in
    /// `omnigraph.yaml` produces a clear error rather than getting
    /// rejected per-request.
    #[test]
    fn load_server_settings_rejects_reserved_graph_id() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  policies:
    uri: /tmp/g1.omni
"#,
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, false).unwrap_err();
        assert!(
            err.to_string().contains("invalid graph id 'policies'"),
            "expected reserved-name rejection, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_rejects_duplicate_normalized_graph_uris() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("same").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Arc::new(Omnigraph::init(&graph_uri, &schema).await.unwrap());

        let alpha = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: graph_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let beta = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: format!("file://{graph_uri}/"),
            engine,
            policy: None,
            queries: None,
        });

        match GraphRegistry::from_handles(vec![alpha, beta]) {
            Err(InsertError::DuplicateUri(uri)) => {
                assert!(
                    normalize_root_uri(&uri).is_ok(),
                    "duplicate URI should still be parseable, got {uri}"
                );
            }
            Err(err) => panic!("expected DuplicateUri for normalized aliases, got {err:?}"),
            Ok(_) => panic!("expected DuplicateUri for normalized aliases, got Ok"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_stores_canonical_graph_uri() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("canonical").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: format!("file://{graph_uri}/"),
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });

        let registry = GraphRegistry::from_handles(vec![handle]).unwrap();
        let listed = registry.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].uri, graph_uri);
    }

    // ── Four-rule mode inference matrix ───────────────────────────────

    /// Rule 1: CLI positional URI → Single.
    #[test]
    fn mode_inference_cli_uri_is_single() {
        let settings = load_server_settings(
            None,
            Some("/tmp/cli.omni".to_string()),
            None,
            None,
            true, // allow unauth so we get past the runtime-state check
        )
        .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/cli.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 1), got Multi"),
        }
    }

    /// Rule 2: --target picks one graph from `graphs:` map → Single.
    #[test]
    fn mode_inference_cli_target_is_single() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings =
            load_server_settings(Some(&config_path), None, Some("alpha".into()), None, true)
                .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/alpha.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 2), got Multi"),
        }
    }

    /// Rule 3: `server.graph` set → Single (target picked from config).
    #[test]
    fn mode_inference_server_graph_is_single() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
server:
  graph: beta
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/beta.omni"),
            ServerConfigMode::Multi { .. } => panic!("expected Single (rule 3), got Multi"),
        }
    }

    /// Rule 4: `--config` + non-empty `graphs:` + no single-mode selector → Multi.
    #[test]
    fn mode_inference_config_plus_graphs_is_multi() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                let ids: Vec<&str> = graphs.iter().map(|g| g.graph_id.as_str()).collect();
                // BTreeMap iteration order is alphabetical.
                assert_eq!(ids, vec!["alpha", "beta"]);
            }
            ServerConfigMode::Single { .. } => panic!("expected Multi (rule 4), got Single"),
        }
    }

    #[test]
    fn mode_inference_multi_rejects_top_level_policy_file() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
policy:
  file: ./policy.yaml
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level") && msg.contains("policy.file") && msg.contains("not honored"),
            "expected top-level-not-honored guidance, got: {msg}"
        );
        assert!(
            msg.contains("graphs.<graph_id>"),
            "expected per-graph migration guidance, got: {msg}"
        );
        assert!(
            msg.contains("server.policy.file"),
            "expected server policy migration guidance, got: {msg}"
        );
    }

    #[test]
    fn mode_inference_multi_rejects_top_level_queries() {
        // Symmetric to the policy guard: a top-level `queries:` block in
        // multi-graph mode is not honored (each graph uses its own), so it
        // is a loud error rather than a silent no-op.
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "queries:\n  q:\n    file: ./q.gq\ngraphs:\n  alpha:\n    uri: /tmp/alpha.omni\n",
        )
        .unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("queries") && msg.contains("not honored"),
            "top-level queries must be rejected in multi-graph mode: {msg}"
        );
    }

    #[test]
    fn single_mode_named_graph_rejects_top_level_blocks() {
        // Serving a graph by name (`--target`/`server.graph`) uses its
        // per-graph block; a populated top-level block would be silently
        // shadowed, so boot refuses and names the per-graph location.
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "policy:\n  file: ./top.yaml\ngraphs:\n  prod:\n    uri: /tmp/prod.omni\n",
        )
        .unwrap();
        let err =
            load_server_settings(Some(&config_path), None, Some("prod".to_string()), None, true)
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("prod") && msg.contains("policy.file") && msg.contains("graphs.prod"),
            "named single-mode + top-level policy must refuse, naming the graph: {msg}"
        );
    }

    #[test]
    fn single_mode_named_graph_uses_per_graph_policy_and_queries() {
        // The identity rule: `--target prod` attaches `graphs.prod`'s own
        // policy + queries, not the top-level ones (which are absent here).
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("prod.gq"),
            "query pq() { match { $u: User } return { $u.name } }",
        )
        .unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            "graphs:\n  prod:\n    uri: /tmp/prod.omni\n    policy:\n      file: ./prod-policy.yaml\n    \
             queries:\n      pq:\n        file: ./prod.gq\n",
        )
        .unwrap();
        let settings =
            load_server_settings(Some(&config_path), None, Some("prod".to_string()), None, true)
                .unwrap();
        match settings.mode {
            ServerConfigMode::Single {
                graph_id,
                policy_file,
                queries,
                ..
            } => {
                assert_eq!(graph_id, "prod", "named single-mode keeps graph identity");
                assert!(
                    policy_file
                        .as_ref()
                        .is_some_and(|p| p.ends_with("prod-policy.yaml")),
                    "per-graph policy attached: {policy_file:?}"
                );
                assert!(queries.lookup("pq").is_some(), "per-graph query attached");
            }
            other => panic!("expected Single mode, got {other:?}"),
        }
    }

    #[test]
    fn mode_inference_normalizes_multi_graph_uris() {
        let temp = tempfile::tempdir().unwrap();
        let graph = temp.path().join("alpha.omni");
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
graphs:
  alpha:
    uri: file://{}/
"#,
                graph.display()
            ),
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                assert_eq!(graphs[0].uri, graph.to_string_lossy());
            }
            ServerConfigMode::Single { .. } => panic!("expected Multi"),
        }
    }

    /// Rule 5: nothing → error with migration hint.
    #[test]
    fn mode_inference_no_inputs_errors_with_migration_hint() {
        let err = load_server_settings(None, None, None, None, true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no graph to serve"),
            "expected migration-hint error, got: {msg}"
        );
    }

    /// Rule 4 sub-case: `--config` with empty `graphs:` map and no
    /// single-mode selector → rule 5 fires (no graph to serve).
    #[test]
    fn mode_inference_empty_graphs_map_errors() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(&config_path, "server:\n  bind: 127.0.0.1:8080\n").unwrap();
        let err = load_server_settings(Some(&config_path), None, None, None, true).unwrap_err();
        assert!(err.to_string().contains("no graph to serve"));
    }

    /// `--config` + `<URI>` together: URI wins → Single (the CLI URI
    /// takes precedence over the config's graphs map).
    #[test]
    fn mode_inference_cli_uri_overrides_graphs_map() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(
            Some(&config_path),
            Some("/tmp/cli-override.omni".to_string()),
            None,
            None,
            true,
        )
        .unwrap();
        match settings.mode {
            ServerConfigMode::Single { uri, .. } => {
                assert_eq!(
                    uri, "/tmp/cli-override.omni",
                    "CLI URI must win over graphs: map"
                );
            }
            ServerConfigMode::Multi { .. } => {
                panic!("expected Single (CLI URI wins), got Multi")
            }
        }
    }

    /// Per-graph `policy.file` is resolved relative to the config base_dir.
    #[test]
    fn per_graph_policy_file_is_resolved_relative_to_base_dir() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  alpha:
    uri: /tmp/alpha.omni
    policy:
      file: ./policies/alpha.yaml
  beta:
    uri: /tmp/beta.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        let graphs = match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => graphs,
            _ => panic!("expected Multi"),
        };
        // graphs is BTreeMap-iter order (alphabetical).
        let alpha = &graphs[0];
        let beta = &graphs[1];
        assert_eq!(alpha.graph_id, "alpha");
        assert_eq!(
            alpha.policy_file.as_ref().unwrap(),
            &temp.path().join("policies/alpha.yaml")
        );
        assert_eq!(beta.graph_id, "beta");
        assert!(beta.policy_file.is_none());
    }

    /// `server.policy.file` resolves alongside the graphs map.
    #[test]
    fn server_policy_file_is_resolved_relative_to_base_dir() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
server:
  policy:
    file: ./server-policy.yaml
graphs:
  alpha:
    uri: /tmp/alpha.omni
"#,
        )
        .unwrap();
        let settings = load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        match settings.mode {
            ServerConfigMode::Multi {
                server_policy_file, ..
            } => {
                assert_eq!(
                    server_policy_file.unwrap(),
                    temp.path().join("server-policy.yaml")
                );
            }
            _ => panic!("expected Multi"),
        }
    }

    /// `GET /graphs` must NOT leak the registry in Open mode without
    /// an explicit server policy. Operators who pass `--unauthenticated`
    /// opted into trusting the network for graph DATA, not for leaking
    /// server topology (graph IDs + URIs, which may contain S3 bucket
    /// paths or internal hostnames). Cedar gating the management
    /// surface is the documented contract for `server_graphs_list`
    /// ("don't leak the registry until the operator explicitly
    /// authorizes it"); enforcing that contract in every runtime
    /// state — not just `PolicyEnabled` — is the correct-by-design
    /// closure of the open-mode hole the bot-review pass surfaced.
    ///
    /// Today (pre-fix) this returns 200 because `authorize_request`'s
    /// no-policy fallback only denies when `actor.is_some()`, so Open
    /// mode (`actor: None`) falls through to `Ok(())`. The fix in the
    /// next commit tightens the fallback so server-scoped actions
    /// always require explicit policy.
    ///
    /// Sort-order coverage previously lived here; it has moved to
    /// `get_graphs_with_server_policy_authorizes_per_cedar` where
    /// the response body is now non-empty and operator-authorized.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_denied_in_open_mode_without_server_policy() {
        let (_dirs, app) = build_multi_mode_app(&["beta", "alpha"]).await;
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "GET /graphs must require an explicit server policy in every \
             runtime state; Open-mode bypass would leak server topology. \
             Body: {body_str}",
        );
    }

    /// `GET /graphs` returns 405 in single mode (resource exists in the
    /// API surface, just not operational without a `graphs:` map).
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_returns_405_in_single_mode() {
        let temp = init_loaded_graph().await;
        let graph = graph_path(temp.path());
        let state = AppState::open(graph.to_string_lossy().to_string())
            .await
            .unwrap();
        let app = build_app(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// `GET /graphs` requires bearer auth when tokens are configured.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_requires_bearer_auth_when_configured() {
        use omnigraph_server::{GraphHandle, GraphId, GraphKey};
        // Build a multi-mode app with bearer tokens configured.
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().join("alpha").to_str().unwrap().to_string();
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: graph_uri,
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });
        let tokens = vec![("act-andrew".to_string(), "secret-token".to_string())];
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let state = AppState::new_multi(vec![handle], tokens, None, workload, None).unwrap();
        let app = build_app(state);

        // No Authorization header → 401.
        let resp_no_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_no_auth.status(), StatusCode::UNAUTHORIZED);

        // With auth but no server policy → 403 (default-deny, since
        // GraphList is not Read).
        let resp_authed = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_authed.status(), StatusCode::FORBIDDEN);
    }

    /// `GET /graphs` with a server policy that allows `graph_list` → 200
    /// and returns the registry sorted alphabetically by `graph_id`.
    /// `GET /graphs` with a server policy that does NOT allow
    /// `graph_list` (viewer group) → 403.
    ///
    /// This test owns the alphabetical-sort coverage that previously
    /// lived in `get_graphs_lists_registered_graphs_in_multi_mode`.
    /// That test now asserts denial in Open mode (server-scoped actions
    /// require explicit policy in every runtime state), so the positive
    /// body-shape assertions need a home where the response is
    /// operator-authorized — here.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_graphs_with_server_policy_authorizes_per_cedar() {
        use omnigraph_policy::PolicyEngine;
        use omnigraph_server::{GraphHandle, GraphId, GraphKey};

        let dir = tempfile::tempdir().unwrap();

        // Two graphs deliberately registered in non-alphabetical order
        // so the test would fail if the handler relied on insertion
        // order instead of server-side sorting.
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        let mut handles = Vec::new();
        for id in ["beta", "alpha"] {
            let graph_uri = dir.path().join(id).to_str().unwrap().to_string();
            let engine = Omnigraph::init(&graph_uri, &schema).await.unwrap();
            handles.push(Arc::new(GraphHandle {
                key: GraphKey::cluster(GraphId::try_from(id).unwrap()),
                uri: graph_uri,
                engine: Arc::new(engine),
                policy: None,
                queries: None,
            }));
        }

        // Server policy: admins can graph_list, viewers cannot.
        let policy_path = dir.path().join("server-policy.yaml");
        fs::write(
            &policy_path,
            r#"
version: 1
groups:
  admins: [act-andrew]
  viewers: [act-bruno]
rules:
  - id: admins-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
        )
        .unwrap();
        let server_policy = PolicyEngine::load_server(&policy_path).unwrap();

        let tokens = vec![
            ("act-andrew".to_string(), "andrew-token".to_string()),
            ("act-bruno".to_string(), "bruno-token".to_string()),
        ];
        let workload = omnigraph_server::workload::WorkloadController::from_env();
        let state =
            AppState::new_multi(handles, tokens, Some(server_policy), workload, None).unwrap();
        let app = build_app(state);

        // Admin → 200, body returns both graphs alphabetically sorted.
        let resp_admin = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer andrew-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_admin.status(),
            StatusCode::OK,
            "admin must be allowed graph_list"
        );
        let body = to_bytes(resp_admin.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let graphs = json["graphs"].as_array().unwrap();
        assert_eq!(graphs.len(), 2, "response must list both registered graphs");
        assert_eq!(
            graphs[0]["graph_id"].as_str().unwrap(),
            "alpha",
            "server must sort graphs alphabetically by graph_id (insertion order was 'beta', 'alpha')"
        );
        assert_eq!(graphs[1]["graph_id"].as_str().unwrap(), "beta");

        // Viewer → 403
        let resp_viewer = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/graphs")
                    .header("authorization", "Bearer bruno-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_viewer.status(),
            StatusCode::FORBIDDEN,
            "viewer must be denied graph_list (Cedar gate)"
        );
    }

    /// Loads an `omnigraph.yaml` with two graphs and verifies multi-mode
    /// inference plus graph entry resolution. Cluster-route dispatch is
    /// covered by the route tests above.
    #[tokio::test(flavor = "multi_thread")]
    async fn server_settings_load_multi_graph_config_entries() {
        let cfg_dir = tempfile::tempdir().unwrap();
        // Real graph storage dirs (the URIs in the config must point to
        // a graph init-able location).
        let alpha_dir = cfg_dir.path().join("alpha.omni");
        let beta_dir = cfg_dir.path().join("beta.omni");
        let schema = fs::read_to_string(fixture("test.pg")).unwrap();
        Omnigraph::init(alpha_dir.to_str().unwrap(), &schema)
            .await
            .unwrap();
        Omnigraph::init(beta_dir.to_str().unwrap(), &schema)
            .await
            .unwrap();

        let config_path = cfg_dir.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
graphs:
  alpha:
    uri: {alpha}
  beta:
    uri: {beta}
"#,
                alpha = alpha_dir.display(),
                beta = beta_dir.display(),
            ),
        )
        .unwrap();

        let settings: ServerConfig =
            load_server_settings(Some(&config_path), None, None, None, true).unwrap();
        assert!(matches!(settings.mode, ServerConfigMode::Multi { .. }));

        match settings.mode {
            ServerConfigMode::Multi { graphs, .. } => {
                assert_eq!(graphs.len(), 2);
                let ids: Vec<&str> = graphs.iter().map(|g| g.graph_id.as_str()).collect();
                assert_eq!(ids, vec!["alpha", "beta"]);
            }
            _ => unreachable!(),
        }
    }
}
