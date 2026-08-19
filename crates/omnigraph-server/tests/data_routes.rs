//! Data-plane routes: read/query/change/ingest/branches/snapshot/export.
//! Moved verbatim from tests/server.rs in the modularization.

use std::convert::Infallible;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::http::{HeaderValue, Method, Request, StatusCode};
use futures::TryStreamExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::{
    BLOB_READ_RANGE_MAX_BYTES, ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy,
};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorCode, ErrorOutput, ExportRequest,
    GraphBatchLoadOutput, IngestRequest, QueryRequest, ReadRequest,
};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use serial_test::serial;
use tower::ServiceExt;

mod support;
use support::*;

const BLOB_HTTP_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}

edge Attachment: Document -> Document {
    payload: Blob?
}
"#;

const BLOB_HTTP_DATA: &str = r#"{"type":"Document","data":{"title":"readme","content":"base64:SGVsbG8gV29ybGQ="}}
{"type":"Document","data":{"title":"empty","content":"base64:"}}
{"type":"Document","data":{"title":"null"}}
{"type":"Document","data":{"title":"peer"}}
{"edge":"Attachment","from":"readme","to":"peer","data":{"id":"attachment-1","payload":"base64:RWRnZQ=="}}"#;

async fn app_for_blob_http_data(data: &str) -> (tempfile::TempDir, axum::Router) {
    let temp = init_graph_with_schema_and_data(BLOB_HTTP_SCHEMA, data).await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    (temp, build_app(state))
}

fn blob_uri(entity: &str, type_name: &str, id: &str, property: &str, target: &str) -> String {
    g(&format!(
        "/blob?entity={entity}&type={type_name}&id={id}&property={property}{target}"
    ))
}

fn repeated_zero_blob_input(length: usize) -> String {
    let full_triples = length / 3;
    let tail = match length % 3 {
        0 => "",
        1 => "AA==",
        2 => "AAA=",
        _ => unreachable!(),
    };
    format!("base64:{}{tail}", "AAAA".repeat(full_triples))
}

async fn assert_receipt_commit_matches_get(app: &axum::Router, output: &Value) {
    let receipt = output
        .get("commit")
        .filter(|commit| !commit.is_null())
        .expect("successful effectful mutation must return a commit receipt");
    let commit_id = receipt["graph_commit_id"]
        .as_str()
        .expect("commit receipt must carry graph_commit_id")
        .to_string();
    let (status, shown) = json_response(
        app,
        Request::builder()
            .uri(g(&format!("/commits/{commit_id}")))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        &shown, receipt,
        "receipt must be the exact published commit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_get_head_ranges_and_conditionals_follow_http_contract() {
    let (_temp, app) = app_for_blob_http_data(BLOB_HTTP_DATA).await;
    let uri = blob_uri("node", "Document", "readme", "content", "");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(response.headers().get("content-length").unwrap(), "11");
    assert_eq!(response.headers().get("accept-ranges").unwrap(), "bytes");
    let etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    let snapshot_id = response
        .headers()
        .get("omnigraph-snapshot-id")
        .expect("managed response carries its exact resolved snapshot")
        .to_str()
        .unwrap()
        .to_string();
    assert!(!snapshot_id.is_empty());
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"Hello World"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::GET)
                .header("if-match", "\"stale\"")
                .header("if-none-match", format!("W/{etag}"))
                .header("range", "bytes=0-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    let output: ErrorOutput =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(output.code, Some(ErrorCode::Conflict));

    for (range, expected_range, expected) in [
        ("bytes=1-4", "bytes 1-4/11", &b"ello"[..]),
        ("bytes=6-", "bytes 6-10/11", &b"World"[..]),
        ("bytes=-5", "bytes 6-10/11", &b"World"[..]),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .method(Method::GET)
                    .header("range", range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT, "{range}");
        assert_eq!(
            response.headers().get("content-range").unwrap(),
            expected_range
        );
        assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
        assert_eq!(
            response.headers().get("omnigraph-snapshot-id").unwrap(),
            snapshot_id.as_str()
        );
        assert_eq!(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
            expected,
            "{range}"
        );
    }

    // V1 deliberately ignores multipart ranges and returns the full
    // representation instead of silently inventing multipart framing.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::GET)
                .header("range", "bytes=0-1,6-10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("content-range").is_none());
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"Hello World"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::GET)
                .header("if-none-match", format!("\"other\", W/{etag}"))
                .header("range", "bytes=0-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response.headers().get("content-length").unwrap(), "11");
    assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let weak_etag = format!("W/{etag}");
    for (if_range, expected_status, expected) in [
        (etag.as_str(), StatusCode::PARTIAL_CONTENT, &b"Hello"[..]),
        (weak_etag.as_str(), StatusCode::OK, &b"Hello World"[..]),
        ("\"different\"", StatusCode::OK, &b"Hello World"[..]),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .method(Method::GET)
                    .header("range", "bytes=0-4")
                    .header("if-range", if_range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status, "If-Range: {if_range}");
        assert_eq!(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
            expected,
            "If-Range: {if_range}"
        );
    }

    // HEAD is an explicit metadata path: it ignores Range and If-Range, but
    // still honors If-None-Match. In particular, an unsatisfiable range cannot
    // turn HEAD into 416 and no response carries payload bytes.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::HEAD)
                .header("range", "bytes=99-")
                .header("if-range", &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-length").unwrap(), "11");
    assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    assert!(response.headers().get("content-range").is_none());
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::HEAD)
                .header("if-none-match", "*")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response.headers().get("content-length").unwrap(), "11");
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::HEAD)
                .header("if-match", "W/\"stale\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_get_preserves_empty_null_edge_and_target_semantics() {
    let (temp, app) = app_for_blob_http_data(BLOB_HTTP_DATA).await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snapshot_id = db.resolve_snapshot("main").await.unwrap().to_string();
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    drop(db);

    let empty_uri = blob_uri("node", "Document", "empty", "content", "");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&empty_uri)
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-length").unwrap(), "0");
    assert!(response.headers().get("etag").is_some());
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    // A byte range cannot select any representation bytes from a valid empty
    // Blob. This is 416, not the engine's valid half-open descriptor range
    // 0..0 (which HTTP's inclusive Range syntax cannot express).
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&empty_uri)
                .method(Method::GET)
                .header("range", "bytes=0-0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers().get("content-range").unwrap(),
        "bytes */0"
    );
    assert_eq!(response.headers().get("accept-ranges").unwrap(), "bytes");
    assert!(response.headers().get("etag").is_some());
    assert!(response.headers().get("omnigraph-snapshot-id").is_some());
    let error: ErrorOutput =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        error.code,
        Some(omnigraph_server::api::ErrorCode::BadRequest)
    );
    let range = error
        .blob_range
        .expect("HTTP 416 carries the normalized half-open range");
    assert_eq!((range.start, range.end, range.length), (0, 1, 0));

    for (id, expected) in [
        ("null", StatusCode::NOT_FOUND),
        ("missing", StatusCode::NOT_FOUND),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(blob_uri("node", "Document", id, "content", ""))
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "id={id}");
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(blob_uri("node", "Document", "readme", "title", ""))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(blob_uri(
                    "edge",
                    "Attachment",
                    "attachment-1",
                    "payload",
                    "",
                ))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"Edge"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(blob_uri(
                    "node",
                    "Document",
                    "readme",
                    "content",
                    &format!("&snapshot={snapshot_id}"),
                ))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("omnigraph-snapshot-id").unwrap(),
        snapshot_id.as_str()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(blob_uri(
                    "node",
                    "Document",
                    "readme",
                    "content",
                    "&branch=feature",
                ))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("omnigraph-snapshot-id").is_some());
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"Hello World"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(blob_uri(
                    "node",
                    "Document",
                    "readme",
                    "content",
                    &format!("&branch=main&snapshot={snapshot_id}"),
                ))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let malformed_selectors = [
        (
            "missing property",
            g("/blob?entity=node&type=Document&id=readme"),
        ),
        (
            "invalid entity kind",
            g("/blob?entity=dataset&type=Document&id=readme&property=content"),
        ),
    ];
    for method in [Method::GET, Method::HEAD] {
        for (case, uri) in &malformed_selectors {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .method(method.clone())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{method} {case}"
            );
            assert_eq!(
                response.headers().get("content-type").unwrap(),
                "application/json",
                "{method} {case}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            if method == Method::HEAD {
                assert!(body.is_empty(), "HEAD {case}");
                continue;
            }
            let output: ErrorOutput = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                output.code,
                Some(omnigraph_server::api::ErrorCode::BadRequest),
                "{case}"
            );
            assert!(
                output
                    .error
                    .starts_with("invalid Blob selector query parameters:"),
                "{case}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_external_get_and_head_redirect_without_target_io() {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    fs::create_dir_all(&graph).unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("external.bin");
    fs::write(&external_path, b"must not be read by the Blob route").unwrap();
    let external_uri = format!("file://{}", external_path.display());
    let canonical_external_uri = format!(
        "file://{}",
        fs::canonicalize(&external_path).unwrap().display()
    );
    let external_base = format!("file://{}/", external_dir.path().display());
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(external_base, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let db = Omnigraph::init(graph.to_str().unwrap(), BLOB_HTTP_SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy)
        .unwrap();
    load_jsonl(
        &db,
        &serde_json::json!({
            "type": "Document",
            "data": {"title": "external", "content": external_uri},
        })
        .to_string(),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    fs::remove_file(&external_path).unwrap();

    let app = build_app(AppState::new(graph.to_string_lossy().to_string(), db));
    let uri = blob_uri("node", "Document", "external", "content", "");
    for method in [Method::GET, Method::HEAD] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .method(method.clone())
                    .header("range", "bytes=1-2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND, "{method}");
        assert_eq!(
            response.headers().get("location").unwrap(),
            canonical_external_uri.as_str()
        );
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        assert!(response.headers().get("omnigraph-snapshot-id").is_some());
        assert!(response.headers().get("etag").is_none());
        if let Some(content_length) = response.headers().get("content-length") {
            assert_eq!(
                content_length, "0",
                "a redirect may frame its empty response body but never assert the external object's length"
            );
        }
        assert!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_get_streams_large_managed_values_in_bounded_chunks() {
    let payload_len = usize::try_from(BLOB_READ_RANGE_MAX_BYTES + 1).unwrap();
    let data = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "large",
            "content": repeated_zero_blob_input(payload_len),
        },
    })
    .to_string();
    let (_temp, app) = app_for_blob_http_data(&data).await;
    let uri = blob_uri("node", "Document", "large", "content", "");
    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .method(Method::HEAD)
                .header("range", "bytes=0-0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers().get("content-length").unwrap(),
        payload_len.to_string().as_str()
    );
    assert!(
        to_bytes(head.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-length").unwrap(),
        payload_len.to_string().as_str()
    );

    let mut body = response.into_body().into_data_stream();
    let mut chunks = 0_u64;
    let mut bytes = 0_usize;
    while let Some(chunk) = body.try_next().await.unwrap() {
        chunks += 1;
        bytes += chunk.len();
        assert!(
            chunk.len() <= usize::try_from(BLOB_READ_RANGE_MAX_BYTES).unwrap(),
            "one HTTP payload chunk exceeded the engine's 4 MiB read bound"
        );
        assert!(chunk.iter().all(|byte| *byte == 0));
    }
    assert_eq!(bytes, payload_len);
    assert!(
        chunks >= 2,
        "the fixture must cross at least one chunk boundary"
    );
}

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
        .export_jsonl("feature", &["Person".to_string()])
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

fn export_request(type_names: Vec<String>) -> Request<Body> {
    Request::builder()
        .uri(g("/export"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&ExportRequest {
                branch: Some("main".to_string()),
                type_names,
            })
            .unwrap(),
        ))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn export_invalid_filter_refuses_before_success_headers() {
    let (_temp, app) = app_for_loaded_graph().await;
    let response = app
        .oneshot(export_request(vec!["Missing".to_string()]))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorOutput = serde_json::from_slice(&body).unwrap();
    assert!(error.error.contains("unknown export type 'Missing'"));
}

#[tokio::test(flavor = "multi_thread")]
async fn export_json_rejections_preserve_typed_statuses_before_streaming() {
    let (_temp, app) = app_for_loaded_graph().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/export"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"branch":"main","type_names":[],"table_keys":["node:Person"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // The JSON extractor rejects the retired field before the handler can
    // capture a cut or emit streaming success headers, and the route projects
    // that rejection into its documented error contract.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_ne!(
        response.headers().get("content-type").unwrap(),
        "application/x-ndjson; charset=utf-8"
    );
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorOutput = serde_json::from_slice(&body).unwrap();
    assert!(error.error.contains("unknown field `table_keys`"));

    let wrong_content_type = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/export"))
                .method(Method::POST)
                .header("content-type", "text/plain")
                .body(Body::from(r#"{"branch":"main","type_names":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        wrong_content_type.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        wrong_content_type.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = to_bytes(wrong_content_type.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: ErrorOutput = serde_json::from_slice(&body).unwrap();
    assert!(error.error.contains("Content-Type"));

    // The router's ordinary JSON-body ceiling is 1 MiB. Whitespace remains a
    // valid JSON prefix, so this proves the byte cap wins before syntax parsing.
    let oversized = app
        .oneshot(
            Request::builder()
                .uri(g("/export"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(vec![b' '; 2 * 1024 * 1024]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        oversized.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = to_bytes(oversized.into_body(), usize::MAX).await.unwrap();
    let error: ErrorOutput = serde_json::from_slice(&body).unwrap();
    assert!(error.error.contains("length limit"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_export_refuses_a_second_cut_and_disconnect_releases_it() {
    let (_temp, app) = app_for_loaded_graph().await;

    // Keep the first response body completely unpolled. Its bounded channel
    // may fill, but the queued terminal frame or in-flight producer must keep
    // ownership of the sole immutable root cut.
    let first = app
        .clone()
        .oneshot(export_request(Vec::new()))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .clone()
        .oneshot(export_request(Vec::new()))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
    let error: ErrorOutput = serde_json::from_slice(&second_body).unwrap();
    let limit = error.resource_limit.expect("typed root-cut ceiling");
    assert_eq!(limit.resource, "stream_export_slots");
    assert_eq!((limit.limit, limit.actual), (1, 2));

    // Dropping the body is the HTTP disconnect analogue. The producer's
    // cancellation path must release the cut and the body's byte reservation
    // without waiting for another output write.
    drop(first);
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = app
                .clone()
                .oneshot(export_request(Vec::new()))
                .await
                .unwrap();
            if response.status() == StatusCode::OK {
                break response;
            }
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            drop(response);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect must promptly release served-export ownership");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(!body.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_route_returns_graph_and_published_dataset_versions() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let expected_graph_manifest_version = manifest_dataset_version(&graph).await;

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
    assert_eq!(snapshot_body["graph_branch"], "main");
    assert_eq!(
        snapshot_body["graph_manifest_version"].as_u64().unwrap(),
        expected_graph_manifest_version
    );
    assert_eq!(
        snapshot_body["internal_schema_version"].as_u64().unwrap(),
        u64::from(omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION)
    );
    let datasets = snapshot_body["datasets"]
        .as_array()
        .expect("datasets array");
    let person = datasets
        .iter()
        .find(|dataset| dataset["type_name"] == "Person")
        .expect("Person dataset");
    assert_eq!(person["entity_kind"], "node");
    assert!(person["dataset_path"].is_string());
    assert!(person["published_dataset_version"].is_u64());
    assert!(person["native_dataset_branch"].is_null());
    assert_eq!(person["entity_count"], 4);
    for retired in [
        "table_key",
        "table_path",
        "table_version",
        "table_branch",
        "row_count",
    ] {
        assert!(person.get(retired).is_none(), "retired field {retired}");
    }
    let knows = datasets
        .iter()
        .find(|dataset| dataset["type_name"] == "Knows")
        .expect("Knows dataset");
    assert_eq!(knows["entity_kind"], "edge");
    assert_eq!(knows["entity_count"], 3);
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
    assert_eq!(body["nodes"][0]["name"], "Person");
    assert_eq!(body["nodes"][0]["entities_loaded"], 2);
    assert_eq!(body["edges"], json!([]));
    assert_eq!(body["total_entities"], 2);
    let receipt_commit_id = body["commit"]["graph_commit_id"]
        .as_str()
        .expect("effectful ingest must return a commit receipt")
        .to_string();

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let snapshot = db
        .snapshot_of(ReadTarget::branch("feature-ingest"))
        .await
        .unwrap();
    let person_ds = snapshot.open_dataset("node:Person").await.unwrap();
    assert_eq!(person_ds.count_rows(None).await.unwrap(), 5);
    let head = db
        .list_commits(Some("feature-ingest"))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(head.graph_commit_id, receipt_commit_id);
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
        delete_branch: false,
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
        conflict.entity_kind == omnigraph_server::api::EntityKindOutput::Node
            && conflict.type_name == "Person"
            && conflict.entity_id.as_deref() == Some("Alice")
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
    assert_receipt_commit_matches_get(&app, &body).await;

    let (status, no_op) = json_response(
        &app,
        Request::builder()
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "query": MUTATION_QUERIES,
                    "name": "set_age",
                    "params": { "name": "Missing", "age": 99 },
                    "branch": "main",
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(no_op["affected_nodes"], 0);
    assert!(no_op["commit"].is_null());
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
    assert_eq!(body["nodes"][0]["name"], "Person");
    assert_eq!(body["nodes"][0]["entities_loaded"], 1);
    assert_eq!(body["total_entities"], 1);
    body["commit"]["graph_commit_id"]
        .as_str()
        .expect("effectful JSON load must return a commit receipt");
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_graph_batch_load_publishes_mixed_declarations_in_one_commit() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let commits_before = Omnigraph::open(graph.to_str().unwrap())
        .await
        .unwrap()
        .list_commits(Some("main"))
        .await
        .unwrap()
        .len();
    let batch = concat!(
        r#"{"type":"Person","data":{"name":"Raw Ada","age":31}}"#,
        "\n",
        r#"{"type":"Company","data":{"name":"Raw Labs"}}"#,
        "\n",
        r#"{"edge":"WorksAt","from":"Raw Ada","to":"Raw Labs","data":{}}"#,
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load/ndjson?branch=main&mode=append"))
                .method(Method::POST)
                .header("content-type", "application/x-ndjson")
                .body(Body::from(batch))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(
        !text.contains("table_key"),
        "graph-batch responses must not expose physical table identity: {text}"
    );
    let output: GraphBatchLoadOutput = serde_json::from_slice(&body).unwrap();
    assert_eq!(output.branch, "main");
    assert_eq!(output.total_entities, 3);
    let receipt = output
        .commit
        .as_ref()
        .expect("effectful NDJSON load must return a commit receipt");
    assert!(
        receipt.graph_branch.is_none(),
        "main is represented by the absence of graph branch metadata"
    );
    assert_eq!(
        output
            .nodes
            .iter()
            .map(|entry| (entry.name.as_str(), entry.entities_loaded))
            .collect::<Vec<_>>(),
        [("Company", 1), ("Person", 1)]
    );
    assert_eq!(
        output
            .edges
            .iter()
            .map(|entry| (entry.name.as_str(), entry.entities_loaded))
            .collect::<Vec<_>>(),
        [("WorksAt", 1)]
    );

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        commits_before + 1,
        "one mixed graph batch must append exactly one graph commit"
    );
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        snapshot
            .open_dataset("node:Person")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        snapshot
            .open_dataset("node:Company")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        snapshot
            .open_dataset("edge:WorksAt")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_raw_graph_batch_has_no_effect() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let commits_before = db.list_commits(Some("main")).await.unwrap().len();
    let rows_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .open_dataset("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    drop(db);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load/ndjson?branch=main&mode=append"))
                .method(Method::POST)
                .header("content-type", "application/x-ndjson")
                .body(Body::from(concat!(
                    r#"{"type":"Person","data":{"name":"Must Not Land","age":9}}"#,
                    "\nnot-json"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        commits_before
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .open_dataset("node:Person")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        rows_before
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_graph_batch_requires_ndjson_and_enforces_body_cap() {
    let (_temp, app) = app_for_loaded_graph().await;
    let wrong_type = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load/ndjson?branch=main"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_type.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let oversized = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load/ndjson?branch=main"))
                .method(Method::POST)
                .header("content-type", "application/x-ndjson")
                .header("content-length", (32_u64 * 1024 * 1024 + 1).to_string())
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_graph_batch_policy_refusal_does_not_poll_body() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token")],
        INGEST_CREATE_ONLY_POLICY_YAML,
    )
    .await;
    let polled = Arc::new(AtomicBool::new(false));
    let body_polled = Arc::clone(&polled);
    let body = Body::from_stream(futures::stream::once(async move {
        body_polled.store(true, Ordering::SeqCst);
        Ok::<Bytes, Infallible>(Bytes::from_static(
            br#"{"type":"Person","data":{"name":"Denied","age":1}}"#,
        ))
    }));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/load/ndjson?branch=main"))
                .method(Method::POST)
                .header("authorization", "Bearer team-token")
                .header("content-type", "application/x-ndjson")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        !polled.load(Ordering::SeqCst),
        "Cedar refusal must happen before the NDJSON body is polled"
    );
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
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body_bytes.as_ref(),
        br#"{"query_name":"get_person","target":{"branch":"main","snapshot":null},"row_count":1,"columns":["p.name","p.age"],"rows":[{"p.name":"Alice","p.age":30}]}"#,
        "POST /read's legacy response bytes are an indefinite compatibility contract"
    );
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        body.get("graph_commit_id").is_none(),
        "POST /read has an indefinite byte-stable body contract and must not gain the canonical route's graph_commit_id: {body}"
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
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        body["graph_commit_id"].as_str().is_some(),
        "POST /query must expose the pinned graph-commit token used by conditional writes: {body}"
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
        delete_branch: false,
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
async fn branch_merge_delete_branch_deletes_source_after_merge() {
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
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);

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
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["outcome"], "fast_forward");
    assert_eq!(merge_body["branch_deleted"], true);
    assert!(merge_body["branch_delete_error"].is_null());

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
async fn branch_merge_delete_branch_refusal_is_non_fatal() {
    let (_temp, app) = app_for_loaded_graph().await;

    for (from, name) in [("main", "feature"), ("feature", "feature-child")] {
        let create = BranchCreateRequest {
            from: Some(from.to_string()),
            name: name.to_string(),
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
    }

    // No writes on `feature`, so the merge is `already_up_to_date` — the
    // deletion must still be attempted (the "already merged, clean me up"
    // case) and its refusal (a dependent descendant branch) must be reported
    // without failing the request.
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
            .contains("feature-child")
    );

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
    assert_eq!(
        list_body["branches"],
        json!(["feature", "feature-child", "main"])
    );
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
async fn change_long_lived_handle_refreshes_before_preparing_write() {
    // A handle that merely predates another committed write is not stale
    // authority: open_write_txn probes the manifest incarnation and prepares
    // from the fresh head. ReadSetChanged is reserved for movement *during* an
    // already-prepared attempt (covered by the concurrent test below).
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());

    // Build the server first, then advance the graph through another handle.
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

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["affected_nodes"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_concurrent_inserts_same_key_serialize_without_409() {
    // RFC-022 preservation guard: concurrent retryable inserts still all
    // succeed, but not by rebasing an already-validated Lance transaction.
    // The coarse branch gate serializes effects; a waiter whose authority
    // token changed discards its complete attempt and reprepares from the
    // winner's committed branch state.
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
    let person_rows = snapshot_body["datasets"]
        .as_array()
        .and_then(|datasets| {
            datasets.iter().find(|dataset| {
                dataset["entity_kind"].as_str() == Some("node")
                    && dataset["type_name"].as_str() == Some("Person")
            })
        })
        .and_then(|dataset| dataset["entity_count"].as_u64())
        .expect("snapshot must include Person entity_count");
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
async fn change_concurrent_updates_same_key_return_typed_pre_effect_conflicts() {
    // Strict read-modify-write attempts are never automatically reprepared.
    // Exactly one concurrent UPDATE commits; once it changes branch authority,
    // every waiter reports a typed 409 before any of its Lance effects begin.
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
        "expected exactly one update to commit and N-1 to receive typed 409 conflicts \
         before effects. Got {} OK + {} 409 + {} other. Statuses: {:?}",
        ok_count,
        conflict_count,
        statuses.len() - ok_count - conflict_count,
        statuses,
    );

    for (status, bytes) in &results {
        if *status != StatusCode::CONFLICT {
            continue;
        }
        let error: ErrorOutput = serde_json::from_slice(bytes).unwrap();
        assert_eq!(error.code, Some(omnigraph_server::api::ErrorCode::Conflict));
        let conflict = error
            .read_set_conflict
            .expect("strict OCC loser must include structured read-set authority");
        assert_eq!(conflict.member, "graph_head:main");
        assert_ne!(conflict.actual, conflict.expected);
        assert!(error.published_dataset_version_conflict.is_none());
        assert!(error.recovery_required.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_disjoint_table_concurrency_succeeds_under_branch_occ_gate() {
    // RFC-022 intentionally serializes effect publication per branch because
    // graph-head authority protects validation dependencies across tables.
    // Disjoint retryable inserts must nevertheless all succeed through bounded
    // full-attempt repreparation, without admission rejection or a user-visible
    // publisher conflict.
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
    let lookup_count = |type_name: &str| -> u64 {
        body["datasets"]
            .as_array()
            .and_then(|datasets| {
                datasets.iter().find(|dataset| {
                    dataset["entity_kind"].as_str() == Some("node")
                        && dataset["type_name"].as_str() == Some(type_name)
                })
            })
            .and_then(|dataset| dataset["entity_count"].as_u64())
            .unwrap_or_else(|| panic!("snapshot missing node type {type_name}"))
    };
    assert_eq!(
        lookup_count("Person"),
        SEED_PERSONS + PER_TYPE as u64,
        "Person row count after concurrent inserts",
    );
    assert_eq!(
        lookup_count("Company"),
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

#[tokio::test(flavor = "multi_thread")]
async fn mutate_graph_commit_precondition_issue_365() {
    // GitHub #365: `Omnigraph-If-Graph-Commit: <commit_id>` makes `mutate` a
    // single-round-trip compare-and-swap. A caller that read the branch at
    // head X must be rejected atomically (412, structured
    // `precondition_failure`, zero effect) once the head has advanced past
    // X; a precondition naming the current head passes.
    fn mutate_request(body: &Value, expected_commit: Option<&str>) -> Request<Body> {
        let path = if expected_commit.is_some() {
            "/mutate/if-graph-commit"
        } else {
            "/mutate"
        };
        let mut builder = Request::builder()
            .uri(g(path))
            .method(Method::POST)
            .header("content-type", "application/json");
        if let Some(commit_id) = expected_commit {
            builder = builder.header("omnigraph-if-graph-commit", commit_id);
        }
        builder
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }
    async fn alice_age(app: &axum::Router) -> Value {
        let (status, out) = json_response(
            app,
            Request::builder()
                .uri(g("/query"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "query": FIND_PERSON_GQ,
                        "params": { "name": "Alice" },
                        "branch": "main",
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        out["rows"][0]["p.age"].clone()
    }
    async fn head_commit_id(app: &axum::Router) -> String {
        let (status, out) = json_response(
            app,
            Request::builder()
                .uri(g("/commits?branch=main"))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        out["commits"]
            .as_array()
            .expect("commit list")
            .iter()
            .max_by_key(|commit| commit["graph_manifest_version"].as_u64().unwrap())
            .expect("loaded graph has at least one commit")["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    let (_temp, app) = app_for_loaded_graph().await;
    let stale_head = head_commit_id(&app).await;

    let conditional_body = json!({
        "query": MUTATION_QUERIES,
        "name": "set_age",
        "params": { "name": "Alice", "age": 77 },
        "branch": "main",
    });
    let (status, _) = json_response(
        &app,
        Request::builder()
            .uri(g("/mutate/if-graph-commit"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the conditional capability route must require its header"
    );
    for invalid in ["W/\"weak\"", "\"quoted\"", "one,two"] {
        let (status, _) = json_response(
            &app,
            Request::builder()
                .uri(g("/mutate/if-graph-commit"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .header("omnigraph-if-graph-commit", invalid)
                .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
                .unwrap(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "entity-tag/list syntax must be refused: {invalid}"
        );
    }
    let mut duplicate = Request::builder()
        .uri(g("/mutate/if-graph-commit"))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
        .unwrap();
    duplicate.headers_mut().append(
        "omnigraph-if-graph-commit",
        HeaderValue::from_static("first"),
    );
    duplicate.headers_mut().append(
        "omnigraph-if-graph-commit",
        HeaderValue::from_static("second"),
    );
    let (status, _) = json_response(&app, duplicate).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "duplicate graph-head preconditions must be refused"
    );
    let (status, _) = json_response(
        &app,
        Request::builder()
            .uri(g("/mutate"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .header("omnigraph-if-graph-commit", &stale_head)
            .body(Body::from(serde_json::to_vec(&conditional_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "the ordinary mutation route must refuse an unsafe optional CAS header"
    );
    assert_eq!(alice_age(&app).await, 30, "both refusals are pre-effect");

    // Writer A claims first (plain mutate) — the head advances past the
    // commit both writers read.
    let (status, body) = json_response(
        &app,
        mutate_request(
            &json!({
                "query": MUTATION_QUERIES,
                "name": "set_age",
                "params": { "name": "Alice", "age": 31 },
                "branch": "main",
            }),
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "writer A's plain mutate: {body}");

    // Writer B lost the race: its precondition names the now-stale head, so
    // the store must reject before any effect.
    let (status, body) = json_response(
        &app,
        mutate_request(
            &json!({
                "query": MUTATION_QUERIES,
                "name": "set_age",
                "params": { "name": "Alice", "age": 52 },
                "branch": "main",
            }),
            Some(&stale_head),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "stale graph-commit precondition must be rejected with 412, got {status}: {body}"
    );
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    // code stays None: closed wire contract (`recovery_required` precedent).
    assert_eq!(error.code, None);
    let failure = error
        .precondition_failure
        .expect("412 body must carry structured precondition_failure details");
    assert_eq!(failure.expected, stale_head);
    let current_head = head_commit_id(&app).await;
    assert_eq!(failure.actual.as_deref(), Some(current_head.as_str()));
    assert!(error.read_set_conflict.is_none());

    // The rejected write had no effect: writer A's claim survives.
    assert_eq!(alice_age(&app).await, 31);

    // The read response itself carries the graph commit id of the snapshot
    // the rows came from, so the caller needs no separate id fetch.
    let (status, read_body) = json_response(
        &app,
        Request::builder()
            .uri(g("/query"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "query": FIND_PERSON_GQ,
                    "params": { "name": "Alice" },
                    "branch": "main",
                }))
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let welded_id = read_body["graph_commit_id"]
        .as_str()
        .expect("read response must carry the snapshot's graph_commit_id")
        .to_string();
    assert_eq!(
        welded_id, current_head,
        "the read's id must equal the branch head it was served from"
    );

    // A precondition naming the CURRENT head passes — the CAS succeeds in a
    // single round trip, using the id the read itself supplied.
    let (status, body) = json_response(
        &app,
        mutate_request(
            &json!({
                "query": MUTATION_QUERIES,
                "name": "set_age",
                "params": { "name": "Alice", "age": 33 },
                "branch": "main",
            }),
            Some(&welded_id),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "graph-commit precondition naming the current head must pass: {body}"
    );
    assert_receipt_commit_matches_get(&app, &body).await;
    assert_eq!(alice_age(&app).await, 33);

    // A newly forked branch has no branch-owned graph-head row yet. Its read
    // response must nevertheless expose main's inherited effective head — the
    // same value the engine compares for the branch's conditional first write.
    let inherited_head = head_commit_id(&app).await;
    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "fresh-cas".to_string(),
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri(g("/branches"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create fresh CAS branch: {body}");

    let (status, fresh_read) = json_response(
        &app,
        Request::builder()
            .uri(g("/query"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "query": FIND_PERSON_GQ,
                    "params": { "name": "Alice" },
                    "branch": "fresh-cas",
                }))
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "read fresh CAS branch: {fresh_read}"
    );
    assert_eq!(fresh_read["graph_commit_id"], json!(inherited_head));

    let (status, body) = json_response(
        &app,
        mutate_request(
            &json!({
                "query": MUTATION_QUERIES,
                "name": "set_age",
                "params": { "name": "Alice", "age": 35 },
                "branch": "fresh-cas",
            }),
            Some(&inherited_head),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fresh branch must accept its read token on the first write: {body}"
    );
}

// ─── Commit entity changes route ────────────────────────────────────────────

async fn load_commit(app: &axum::Router, ndjson: &str) -> String {
    let request = IngestRequest {
        branch: Some("main".to_string()),
        from: None,
        mode: Some(LoadMode::Merge),
        data: ndjson.to_string(),
    };
    let (status, body) = json_response(
        app,
        Request::builder()
            .uri(g("/load"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&request).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    body["commit"]["graph_commit_id"]
        .as_str()
        .expect("an effectful load returns its commit")
        .to_string()
}

async fn get_json(app: &axum::Router, uri: String) -> (StatusCode, Value) {
    json_response(
        app,
        Request::builder()
            .uri(uri)
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_changes_pages_are_ordered_with_cause_once() {
    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(
        &app,
        concat!(
            r#"{"type":"Person","data":{"name":"Loaded C","age":7}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"Loaded A","age":5}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"Loaded B","age":6}}"#,
        ),
    )
    .await;

    let (status, first) = get_json(&app, g(&format!("/commits/{commit_id}/changes?limit=2"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["cause"]["graph_commit_id"], commit_id.as_str());
    assert_eq!(first["cause"]["authored_branch"], "main");
    assert_eq!(first["changes"][0]["id"], "Loaded A");
    assert_eq!(first["changes"][0]["op"], "insert");
    assert_eq!(first["changes"][0]["kind"], "node");
    assert_eq!(first["changes"][0]["type"]["name"], "Person");
    assert!(
        first["changes"][0]["type"]["id"].is_string(),
        "opaque graph type identity rides every change"
    );
    assert_eq!(first["changes"][1]["id"], "Loaded B");
    let token = first["next_page_token"]
        .as_str()
        .expect("a truncated block continues by page token");

    let (status, second) = get_json(
        &app,
        g(&format!(
            "/commits/{commit_id}/changes?limit=2&page_token={token}"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["changes"][0]["id"], "Loaded C");
    assert!(second["next_page_token"].is_null());
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_changes_images_follow_op_shape() {
    let (_temp, app) = app_for_loaded_graph().await;
    load_commit(&app, r#"{"type":"Person","data":{"name":"Shape","age":1}}"#).await;
    let update_commit =
        load_commit(&app, r#"{"type":"Person","data":{"name":"Shape","age":2}}"#).await;

    let (status, page) = get_json(&app, g(&format!("/commits/{update_commit}/changes"))).await;
    assert_eq!(status, StatusCode::OK);
    let change = &page["changes"][0];
    assert_eq!(change["op"], "update");
    assert_eq!(change["before"]["properties"]["age"], 1);
    assert_eq!(change["after"]["properties"]["age"], 2);
    // Edge images carry endpoints inside each image.
    let edge_commit = load_commit(
        &app,
        concat!(
            r#"{"type":"Person","data":{"name":"Shape2","age":1}}"#,
            "\n",
            r#"{"edge":"Knows","from":"Shape","to":"Shape2"}"#,
        ),
    )
    .await;
    let (status, page) = get_json(&app, g(&format!("/commits/{edge_commit}/changes"))).await;
    assert_eq!(status, StatusCode::OK);
    let edge = page["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["kind"] == "edge")
        .expect("the edge insert surfaces");
    assert_eq!(edge["op"], "insert");
    assert_eq!(edge["after"]["endpoints"]["from"], "Shape");
    assert_eq!(edge["after"]["endpoints"]["to"], "Shape2");
    assert!(edge["before"].is_null());
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_changes_filters_are_repeatable_and_strict() {
    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(
        &app,
        concat!(
            r#"{"type":"Person","data":{"name":"F1","age":1}}"#,
            "\n",
            r#"{"edge":"Knows","from":"F1","to":"Alice"}"#,
        ),
    )
    .await;

    let (status, nodes_only) =
        get_json(&app, g(&format!("/commits/{commit_id}/changes?kind=node"))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        nodes_only["changes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|change| change["kind"] == "node")
    );

    let (status, ops) = get_json(
        &app,
        g(&format!(
            "/commits/{commit_id}/changes?op=insert&op=update&type=Person"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!ops["changes"].as_array().unwrap().is_empty());

    // Unknown values and unknown parameters are strict 400s: a caller byte
    // limit or physical vocabulary can never silently ride this surface.
    for query in ["kind=table", "op=upsert", "max_bytes=1", "table_key=x"] {
        let (status, _) = get_json(&app, g(&format!("/commits/{commit_id}/changes?{query}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query: {query}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_changes_limit_bounds_and_token_rejections() {
    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(&app, r#"{"type":"Person","data":{"name":"Bound","age":1}}"#).await;

    let (status, _) = get_json(&app, g(&format!("/commits/{commit_id}/changes?limit=0"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get_json(&app, g(&format!("/commits/{commit_id}/changes?limit=8193"))).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    let (status, rejected) = get_json(
        &app,
        g(&format!(
            "/commits/{commit_id}/changes?page_token=not-a-token"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        rejected["error"]
            .as_str()
            .unwrap_or_default()
            .starts_with("change cursor rejected"),
        "a malformed token is a typed 400, never a retention gap: {rejected}"
    );

    let (status, _) = get_json(&app, g("/commits/not-a-commit/changes")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_changes_parentless_commit_is_typed_409() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, commits) = get_json(&app, g("/commits")).await;
    assert_eq!(status, StatusCode::OK);
    let genesis = commits["commits"]
        .as_array()
        .unwrap()
        .last()
        .expect("history has a genesis")["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, refusal) = get_json(&app, g(&format!("/commits/{genesis}/changes"))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        refusal["change_diff_refusal"]["reason"], "parentless_commit",
        "{refusal}"
    );
    assert_eq!(refusal["change_diff_refusal"]["graph_commit_id"], genesis);
}

// ─── Change feed route ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn change_routes_report_a_missing_branch_without_storage_detail() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, feed) = get_json(&app, g("/changes?branch=missing&start=now")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(feed["error"], "branch 'missing' not found");
    assert!(!feed.to_string().contains("_refs"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/changes/baseline"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"branch":"missing"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let baseline: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(baseline["error"], "branch 'missing' not found");
    assert!(!baseline.to_string().contains("_refs"));
}

#[tokio::test(flavor = "multi_thread")]
async fn change_feed_poll_advances_cursor_only_after_complete_commits() {
    let (_temp, app) = app_for_loaded_graph().await;

    // `start=now` captures the head: no replay, a caught-up durable cursor.
    let (status, now) = get_json(&app, g("/changes?start=now")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(now["blocks"].as_array().unwrap().is_empty());
    assert_eq!(now["caught_up"], true);
    let c0 = now["cursor"].as_str().expect("terminal page cursor");

    let commit_id = load_commit(
        &app,
        concat!(
            r#"{"type":"Person","data":{"name":"Feed A","age":1}}"#,
            "\n",
            r#"{"type":"Person","data":{"name":"Feed B","age":2}}"#,
        ),
    )
    .await;

    // A mid-block page carries only a page token — no cursor to checkpoint.
    let (status, partial) = get_json(&app, g(&format!("/changes?cursor={c0}&limit=1"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        partial["blocks"][0]["cause"]["graph_commit_id"],
        commit_id.as_str()
    );
    assert_eq!(partial["blocks"][0]["changes"][0]["id"], "Feed A");
    assert!(partial["cursor"].is_null(), "no durable cursor mid-block");
    let token = partial["next_page_token"].as_str().expect("page token");

    let (status, resumed) = get_json(&app, g(&format!("/changes?page_token={token}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resumed["blocks"][0]["changes"][0]["id"], "Feed B");
    let c1 = resumed["cursor"].as_str().expect("boundary cursor");

    let (status, caught_up) = get_json(&app, g(&format!("/changes?cursor={c1}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(caught_up["blocks"].as_array().unwrap().is_empty());
    assert_eq!(caught_up["caught_up"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn change_feed_start_beginning_replays_history() {
    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(
        &app,
        r#"{"type":"Person","data":{"name":"Replayed","age":1}}"#,
    )
    .await;

    let (status, page) = get_json(&app, g("/changes?start=beginning")).await;
    assert_eq!(status, StatusCode::OK);
    let blocks = page["blocks"].as_array().unwrap();
    assert!(!blocks.is_empty());
    assert_eq!(
        blocks.last().unwrap()["cause"]["graph_commit_id"],
        commit_id.as_str(),
        "oldest first: the newest commit is the last block"
    );
    assert!(page["cursor"].is_string());
    assert_eq!(page["caught_up"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn change_feed_start_and_cursor_are_exclusive_and_validated() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (_, now) = get_json(&app, g("/changes?start=now")).await;
    let cursor = now["cursor"].as_str().unwrap();

    for query in [
        format!("cursor={cursor}&start=beginning"),
        format!("cursor={cursor}&page_token={cursor}"),
        "start=later".to_string(),
        "start=after:".to_string(),
        "start=after:no-such-commit".to_string(),
    ] {
        let (status, _) = get_json(&app, g(&format!("/changes?{query}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query: {query}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn change_feed_scope_mismatch_cursor_is_stable_400() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (_, scoped) = get_json(&app, g("/changes?start=now&op=insert")).await;
    let cursor = scoped["cursor"].as_str().unwrap();

    let (status, rejected) = get_json(&app, g(&format!("/changes?cursor={cursor}"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        rejected["error"]
            .as_str()
            .unwrap_or_default()
            .starts_with("change cursor rejected"),
        "{rejected}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn change_baseline_streams_snapshot_then_terminal_cursor() {
    let (_temp, app) = app_for_loaded_graph().await;
    load_commit(&app, r#"{"type":"Person","data":{"name":"Base","age":1}}"#).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/changes/baseline"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"branch": "main"})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/x-ndjson; charset=utf-8")
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let lines: Vec<&str> = text.lines().filter(|line| !line.is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "snapshot records plus the terminal record"
    );

    // Every line but the last is a snapshot record; the FINAL line is the
    // handshake — an interrupted stream would simply lack it.
    let (terminal, records) = lines.split_last().unwrap();
    for record in records {
        let value: Value = serde_json::from_str(record).unwrap();
        assert!(
            value.get("baseline").is_none(),
            "the handshake appears exactly once, at the end: {record}"
        );
    }
    let terminal: Value = serde_json::from_str(terminal).unwrap();
    let snapshot_commit = terminal["baseline"]["snapshot_commit_id"]
        .as_str()
        .expect("terminal record names the captured commit");
    let resume_cursor = terminal["baseline"]["resume_cursor"]
        .as_str()
        .expect("terminal record carries the resume cursor");
    assert!(
        text.contains("Base"),
        "the snapshot carries the loaded entity"
    );

    // A commit landing after the handshake is the first block the resumed
    // feed yields.
    let post_commit = load_commit(
        &app,
        r#"{"type":"Person","data":{"name":"PostBase","age":2}}"#,
    )
    .await;
    assert_ne!(post_commit, snapshot_commit);
    let (status, resumed) = get_json(&app, g(&format!("/changes?cursor={resume_cursor}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resumed["blocks"][0]["cause"]["graph_commit_id"],
        post_commit.as_str()
    );
    assert_eq!(resumed["caught_up"], true);
}

/// The wire vocabulary gate: no change-surface response may carry physical
/// storage vocabulary. This walks every JSON key of real commit-diff, feed,
/// and baseline-terminal responses and rejects the forbidden set outright.
#[tokio::test(flavor = "multi_thread")]
async fn change_responses_carry_no_storage_vocabulary() {
    const FORBIDDEN_KEYS: &[&str] = &[
        "table_key",
        "stable_table_id",
        "table_incarnation_id",
        "incarnation",
        "manifest_version",
        "table_version",
        "table_branch",
        "table_path",
        "row_addr",
        "_rowid",
        "fragment",
        "part",
        "commit_complete",
        "change_index",
        "max_bytes",
    ];

    fn assert_clean(value: &Value, context: &str) {
        match value {
            Value::Object(map) => {
                for (key, nested) in map {
                    assert!(
                        !FORBIDDEN_KEYS.contains(&key.as_str()),
                        "forbidden wire key '{key}' in {context}: {value}"
                    );
                    assert_clean(nested, context);
                }
            }
            Value::Array(items) => {
                for item in items {
                    assert_clean(item, context);
                }
            }
            _ => {}
        }
    }

    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(
        &app,
        concat!(
            r#"{"type":"Person","data":{"name":"Vocab","age":1}}"#,
            "\n",
            r#"{"edge":"Knows","from":"Vocab","to":"Alice"}"#,
        ),
    )
    .await;

    let (status, page) = get_json(&app, g(&format!("/commits/{commit_id}/changes"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_clean(&page, "commit changes page");

    let (status, feed) = get_json(&app, g("/changes?start=beginning")).await;
    assert_eq!(status, StatusCode::OK);
    assert_clean(&feed, "change feed page");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(g("/changes/baseline"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"branch":"main"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let terminal: Value =
        serde_json::from_str(text.lines().rfind(|line| !line.is_empty()).unwrap()).unwrap();
    assert_clean(&terminal, "baseline terminal record");
}
