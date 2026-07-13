//! Bearer auth, actor resolution, Cedar policy decisions, admission.
//! Moved verbatim from tests/server.rs in the modularization.

use std::env;
use std::fs;
use std::sync::Arc;

use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::LoadMode;
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest, ReadRequest, SchemaApplyRequest,
};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use tower::ServiceExt;


mod support;
use support::*;

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
    assert_eq!(
        body["internal_schema_version"].as_u64().unwrap(),
        u64::from(omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
    );
    match option_env!("OMNIGRAPH_SOURCE_VERSION") {
        Some(source_version) => assert_eq!(body["source_version"], source_version),
        None => assert!(body.get("source_version").is_none()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_routes_require_bearer_token() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches"))
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
            .uri(g("/branches"))
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
async fn protected_routes_accept_any_configured_team_bearer_token() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens(&[
        ("team-01", "token-one"),
        ("team-02", "token-two"),
    ])
    .await;

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches"))
            .method(Method::GET)
            .header("authorization", "Bearer token-two")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["branches"].is_array());
}

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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
            .method(Method::GET)
            .header("authorization", "Bearer wrong-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(bad_status, StatusCode::UNAUTHORIZED);
}

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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/export"))
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
                .uri(g("/export"))
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
            .uri(g("/read"))
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
async fn policy_blocks_change_on_protected_main_but_allows_unprotected_branch() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
            .uri(g("/change"))
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
            .uri(g("/change"))
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
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
        delete_branch: false,
    };
    let (deny_status, deny_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches/merge"))
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
            .uri(g("/branches/merge"))
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
            .uri(g("/change"))
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
            .uri(g("/commits?branch=main"))
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
            .uri(g("/branches"))
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
            .uri(g("/change"))
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
        delete_branch: false,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches/merge"))
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
            .uri(g("/commits?branch=main"))
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
async fn branch_merge_delete_branch_policy_denial_is_non_fatal() {
    // act-ragnor (admins) may merge into the protected target, but the
    // composed source deletion targets the UNPROTECTED branch `feature`,
    // which `admins-merge` (target_branch_scope: protected) does not allow.
    // The deletion runs under its own `branch_delete` decision, so the merge
    // must succeed while the denial is reported non-fatally — composition
    // never smuggles a delete through a merge permission.
    let (temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-ragnor", "token-admin")],
        POLICY_YAML,
    )
    .await;
    let graph = graph_path(temp.path());

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: true,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("authorization", "Bearer token-admin")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["outcome"], "already_up_to_date");
    assert_eq!(merge_body["branch_deleted"], false);
    assert!(
        merge_body["branch_delete_error"]
            .as_str()
            .unwrap()
            .contains("policy denied action 'branch_delete'")
    );

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let branches = db.branch_list().await.unwrap();
    assert!(branches.iter().any(|branch| branch == "feature"));
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_layer_policy_fires_via_direct_arc_omnigraph_from_new_single() {
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
    // RFC-011 cluster-only: the single-graph convenience constructor
    // registers the graph under the reserved id `default`.
    let key = omnigraph_server::GraphKey::cluster(
        omnigraph_server::GraphId::try_from("default").unwrap(),
    );
    let handle = match state.routing().registry.get(&key) {
        omnigraph_server::RegistryLookup::Ready(handle) => handle,
        omnigraph_server::RegistryLookup::Gone => panic!("default graph must be registered"),
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
                .uri(g("/read"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(oversized))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

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
            .uri(g("/snapshot"))
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
            .uri(g("/change"))
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
            .uri(g("/schema/apply"))
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
