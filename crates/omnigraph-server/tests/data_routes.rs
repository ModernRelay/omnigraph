//! Data-plane routes: read/query/change/ingest/branches/snapshot/export.
//! Moved verbatim from tests/server.rs in the modularization.

use std::fs;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorOutput, ExportRequest,
    IngestRequest, QueryRequest, ReadRequest,
};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use serial_test::serial;
use tower::ServiceExt;


mod support;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn export_route_returns_jsonl_for_branch_snapshot() {
    let token = "demo-token";
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
                .uri(g("/export"))
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
async fn snapshot_route_returns_manifest_dataset_version() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let expected_manifest_version = manifest_dataset_version(&graph).await;

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/snapshot?branch=main"))
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
            .uri(g("/ingest"))
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
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
            .uri(g("/ingest"))
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
async fn ingest_without_from_returns_404_for_missing_branch_and_creates_nothing() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let ingest = IngestRequest {
        branch: Some("feature-typo".to_string()),
        from: None,
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#.to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/ingest"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::NotFound));

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert!(
        !db.branch_list()
            .await
            .unwrap()
            .contains(&"feature-typo".to_string()),
        "a 404'd ingest must not create the branch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_without_from_loads_into_existing_branch() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    {
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        db.branch_create_from(ReadTarget::branch("main"), "feature")
            .await
            .unwrap();
    }
    let ingest = IngestRequest {
        branch: Some("feature".to_string()),
        from: None,
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Zoe","age":33}}"#.to_string(),
    };

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/ingest"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["branch"], "feature");
    assert_eq!(body["branch_created"], false);
    assert_eq!(body["base_branch"], serde_json::Value::Null);
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
            .uri(g("/ingest"))
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
            .uri(g("/ingest"))
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
                .uri(g("/ingest"))
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
async fn branch_merge_conflict_response_includes_structured_conflicts() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
            .uri(g("/branches/merge"))
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
            .uri(g("/change"))
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
            .uri(g("/read"))
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
            .uri(g("/query"))
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
            .uri(g("/query"))
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
                .uri(g("/mutate"))
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
    // per RFC 9745 (`Deprecation: true`) + RFC 8288 (`Link: <mutate>;
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
                .uri(g("/change"))
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
        Some("<mutate>; rel=\"successor-version\""),
        "POST /change must point at /mutate via `Link` rel=successor-version (RFC 8288)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn load_endpoint_loads_into_existing_branch() {
    // Canonical bulk-load endpoint (RFC-009 Phase 5). Same wire shape as
    // /ingest, no deprecation signal.
    let (_temp, app) = app_for_loaded_graph().await;
    let request = IngestRequest {
        branch: Some("main".to_string()),
        from: None,
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Loaded","age":7}}"#.to_string(),
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load"))
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
        "POST /load must not advertise itself as deprecated"
    );
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["branch"], "main");
    assert_eq!(body["tables"][0]["table_key"], "node:Person");
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_endpoint_emits_deprecation_headers() {
    // `/ingest` is the deprecated alias of `/load` (RFC-009 Phase 5): flagged
    // at runtime per RFC 9745 (`Deprecation: true`) + RFC 8288 (`Link: <load>;
    // rel="successor-version"`). The OpenAPI side is covered by
    // `openapi_ingest_is_deprecated` in tests/openapi.rs.
    let (_temp, app) = app_for_loaded_graph().await;
    let request = IngestRequest {
        branch: Some("main".to_string()),
        from: None,
        mode: Some(LoadMode::Merge),
        data: r#"{"type":"Person","data":{"name":"Legacyer","age":33}}"#.to_string(),
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/ingest"))
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
        "POST /ingest must advertise `Deprecation: true` (RFC 9745)"
    );
    assert_eq!(
        response.headers().get("link").and_then(|v| v.to_str().ok()),
        Some("<load>; rel=\"successor-version\""),
        "POST /ingest must point at /load via `Link` rel=successor-version (RFC 8288)"
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
                .uri(g("/read"))
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
        Some("<query>; rel=\"successor-version\""),
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
                .uri(g("/query"))
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
            .uri(g("/change"))
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
            .uri(g("/change"))
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
            .uri(g("/branches"))
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
            .uri(g("/branches"))
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
            .uri(g("/branches"))
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
            .uri(g("/change"))
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
            .uri(g("/read"))
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
            .uri(g("/branches/merge"))
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
            .uri(g("/read"))
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
            .uri(g("/branches"))
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
            .uri(g("/branches/feature"))
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
            .uri(g("/branches"))
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

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches/feature"))
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
            .uri(g("/read"))
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
        let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
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
            .uri(g("/change"))
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
                .uri(g("/change"))
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
            .uri(g("/snapshot?branch=main"))
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
                .uri(g("/change"))
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
                .uri(g("/change"))
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
                .uri(g("/change"))
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
            .uri(g("/snapshot?branch=main"))
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
                .uri(g("/ingest"))
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
