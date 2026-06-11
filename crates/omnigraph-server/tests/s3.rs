//! S3-backed single-graph serving (gated on OMNIGRAPH_S3_TEST_BUCKET).
//! Moved verbatim from tests/server.rs in the modularization.

use std::fs;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::api::ReadRequest;
use omnigraph_server::{AppState, build_app};
use serde_json::json;


mod support;
use support::*;

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
