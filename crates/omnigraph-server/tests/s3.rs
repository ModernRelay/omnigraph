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

/// Config-free cluster serving (RFC-006): boot `--cluster s3://bucket/prefix`
/// with NO local files at all — the ledger and catalog on the bucket are the
/// whole deployment artifact. The fixture cluster is applied from a temp
/// config dir, which is then dropped before the server boots from the URI.
#[tokio::test(flavor = "multi_thread")]
async fn server_boots_cluster_from_bare_storage_uri_and_serves_query() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("skipping s3 cluster-serving test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = format!("s3://{bucket}/cluster-serve/{unique}");

    // Apply a one-graph cluster onto the bucket, seed it, then DROP the
    // config dir — the boot below must need nothing local.
    {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("people.pg"),
            "node Person {\n  name: String @key\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("people.gq"),
            "query find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("cluster.yaml"),
            format!(
                "version: 1\nstorage: {root}\ngraphs:\n  knowledge:\n    schema: people.pg\n    queries:\n      find_person:\n        file: people.gq\n"
            ),
        )
        .unwrap();
        let import = omnigraph_cluster::import_config_dir(dir.path()).await;
        assert!(import.ok, "{:?}", import.diagnostics);
        let apply = omnigraph_cluster::apply_config_dir(dir.path()).await;
        assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);

        let graph_uri = format!("{root}/graphs/knowledge.omni");
        let mut db = Omnigraph::open(&graph_uri).await.unwrap();
        load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n",
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    let settings = omnigraph_server::load_server_settings(
        None,
        Some(&std::path::PathBuf::from(&root)),
        None,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    let omnigraph_server::ServerConfigMode::Multi {
        graphs,
        config_path,
        server_policy,
    } = settings.mode
    else {
        panic!("cluster boot must select multi-graph routing");
    };
    let state = omnigraph_server::open_multi_graph_state(
        graphs,
        Vec::new(),
        server_policy.as_ref(),
        config_path,
    )
    .await
    .unwrap();
    let app = build_app(state);

    let response = tower::ServiceExt::oneshot(
        app,
        Request::builder()
            .method(Method::POST)
            .uri("/graphs/knowledge/queries/find_person")
            .header("content-type", "application/json")
            .body(Body::from(json!({"params": {"name": "Ada"}}).to_string()))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["rows"][0]["p.name"], "Ada", "{value}");
}
