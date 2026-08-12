//! S3-backed single-graph serving (gated on OMNIGRAPH_S3_TEST_BUCKET).
//! Moved verbatim from tests/server.rs in the modularization.

use std::fs;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::{BlobCell, BlobContent, EntityKind};
use omnigraph_server::api::{IngestRequest, ReadRequest};
use omnigraph_server::{AppState, build_app};
use serde_json::json;

mod support;
use support::*;

async fn read_managed_blob_bytes(
    db: &Omnigraph,
    type_name: &str,
    id: &str,
    property: &str,
) -> Vec<u8> {
    let read = db
        .read_blob_at(
            ReadTarget::branch("main"),
            BlobCell {
                entity: EntityKind::Node,
                type_name: type_name.to_string(),
                id: id.to_string(),
                property: property.to_string(),
            },
        )
        .await
        .expect("read managed Blob");
    let BlobContent::Managed { reader, .. } = read.content else {
        panic!("expected managed Blob content, got external reference");
    };

    reader
        .read_range(0..reader.len())
        .await
        .expect("small S3 test Blob fits one bounded range")
        .to_vec()
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
    let db = Omnigraph::open(&uri).await.unwrap();
    load_jsonl(
        &db,
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
            .uri(g("/snapshot"))
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
            .uri(g("/read"))
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
    let external_blob_base = format!("s3://{bucket}/cluster-serve/{unique}-external");
    let external_blob_uri = format!("{external_blob_base}/available~asset.bin");
    let external_blob_input_uri = external_blob_uri.replace("~asset", "%7Easset");
    let external_blob_payload = "server-safe external Blob payload";

    omnigraph::storage::storage_for_uri(&external_blob_uri)
        .unwrap()
        .write_text(&external_blob_uri, external_blob_payload)
        .await
        .unwrap();

    // Apply a one-graph cluster onto the bucket, seed it, then DROP the
    // config dir — the boot below must need nothing local.
    {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("people.pg"),
            "node Person {\n  name: String @key\n  avatar: Blob?\n}\n",
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
                "version: 1\nstorage: {root}\ngraphs:\n  knowledge:\n    schema: people.pg\n    external_blobs:\n      allow:\n        - base: {external_blob_base}\n          scope: server_safe\n    queries:\n      find_person:\n        file: people.gq\n"
            ),
        )
        .unwrap();
        let import = omnigraph_cluster::import_config_dir(dir.path()).await;
        assert!(import.ok, "{:?}", import.diagnostics);
        let apply = omnigraph_cluster::apply_config_dir(dir.path()).await;
        assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);

        let graph_uri = format!("{root}/graphs/knowledge.omni");
        let db = Omnigraph::open(&graph_uri).await.unwrap();
        load_jsonl(
            &db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n",
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    let settings = omnigraph_server::load_server_settings(
        Some(&std::path::PathBuf::from(&root)),
        None,
        true,
        false,
    )
    .await
    .unwrap();
    let omnigraph_server::ServerConfigMode::Multi {
        graphs,
        config_path,
        server_policy,
    } = settings.mode;
    let state = omnigraph_server::open_multi_graph_state(
        graphs,
        Vec::new(),
        server_policy.as_ref(),
        config_path,
        false,
    )
    .await
    .unwrap();
    let app = build_app(state);

    // The applied server-safe policy must reach the live graph handle. An
    // encoded spelling of an allowed S3 source is normalized, read through the
    // server's shared object-store registry, and copied into managed Blob
    // storage by keyed Append.
    let load = IngestRequest {
        branch: None,
        from: None,
        mode: Some(LoadMode::Append),
        data: json!({
            "type": "Person",
            "data": {
                "name": "Grace",
                "avatar": external_blob_input_uri,
            },
        })
        .to_string(),
    };
    let (load_status, load_body) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/graphs/knowledge/load")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&load).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(load_status, StatusCode::OK, "{load_body}");
    assert_eq!(load_body["tables"][0]["rows_loaded"], 1, "{load_body}");

    let graph_uri = format!("{root}/graphs/knowledge.omni");
    let reopened = Omnigraph::open(&graph_uri).await.unwrap();
    let copied = read_managed_blob_bytes(&reopened, "Person", "Grace", "avatar").await;
    assert_eq!(copied, external_blob_payload.as_bytes());

    // A missing object beneath that same admitted base is a dependency
    // failure, not a policy refusal or generic server error. The response
    // carries the normalized, credential-free URI without extending the
    // closed ErrorCode enum.
    let missing_input_uri = format!("{external_blob_base}/missing%7Easset.bin");
    let normalized_missing_uri = format!("{external_blob_base}/missing~asset.bin");
    let missing = IngestRequest {
        branch: None,
        from: None,
        mode: Some(LoadMode::Append),
        data: json!({
            "type": "Person",
            "data": {
                "name": "Missing",
                "avatar": missing_input_uri,
            },
        })
        .to_string(),
    };
    let (missing_status, missing_body) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/graphs/knowledge/load")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&missing).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(
        missing_status,
        StatusCode::FAILED_DEPENDENCY,
        "missing allowed source must be HTTP 424, not a generic 500: {missing_body}"
    );
    assert!(
        missing_body.get("code").is_none(),
        "the closed ErrorCode enum must remain absent: {missing_body}"
    );
    assert_eq!(
        missing_body["external_blob_source"]["uri"], normalized_missing_uri,
        "{missing_body}"
    );
    assert!(
        missing_body["external_blob_source"]["reason"]
            .as_str()
            .is_some_and(|reason| !reason.trim().is_empty()),
        "source diagnosis must be present: {missing_body}"
    );

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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["rows"][0]["p.name"], "Ada", "{value}");
}
