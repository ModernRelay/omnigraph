//! Accepted schema reads and refusal of the removed schema-apply route.

use std::fs;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_server::api::{
    ErrorOutput, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER, SchemaOutput,
};
use omnigraph_server::{AppState, build_app};
use serde_json::json;
use tower::ServiceExt;

mod support;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn schema_apply_route_is_absent_without_graph_effects() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open_read_only(graph.to_str().unwrap())
        .await
        .unwrap();
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let source = db.schema_source().to_string();
    let response = app
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .method(Method::POST)
                .uri(g("/schema/apply"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "schema_source": additive_schema_with_nickname()
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let reopened = Omnigraph::open_read_only(graph.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(reopened.schema_source().as_str(), source);
    assert_eq!(
        reopened
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        before.graph_manifest_version(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_route_returns_current_source() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/schema"))
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/schema"))
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/schema"))
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/schema"))
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
