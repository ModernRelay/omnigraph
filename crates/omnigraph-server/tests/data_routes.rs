//! Data-plane routes: read/query/change/ingest/branches/snapshot/export.
//! Moved verbatim from tests/server.rs in the modularization.

use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use std::convert::Infallible;
use std::fmt::Write;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::http::{HeaderValue, Method, Request, StatusCode};
use futures::TryStreamExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph::{
    BLOB_READ_RANGE_MAX_BYTES, ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy,
    Session,
};
use omnigraph_server::api::{
    BranchCreateRequest, BranchMergeRequest, ChangeRequest, ErrorCode, ErrorOutput, ExportRequest,
    GraphBatchLoadOutput, IngestRequest, QueryRequest, ReadRequest,
};
use omnigraph_server::{AppState, ProcessDefaults, build_app};
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
{"edge":"Attachment","id":"attachment-1","from":"readme","to":"peer","data":{"payload":"base64:RWRnZQ=="}}"#;

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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(&uri)
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), BLOB_HTTP_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap(),
    );
    Session::from_defaults(Arc::clone(&db), SessionSettings::default())
        .load_jsonl(
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
    let db = Arc::try_unwrap(db)
        .unwrap_or_else(|_| panic!("the loading session is the only other holder and is gone"));

    let app = build_app(AppState::new(graph.to_string_lossy().to_string(), db));
    let uri = blob_uri("node", "Document", "external", "content", "");
    for method in [Method::GET, Method::HEAD] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    for door in ["/export", "/changes/baseline"] {
        let temp = init_loaded_graph().await;
        let state = AppState::open(graph_path(temp.path()).to_string_lossy().to_string())
            .await
            .unwrap();
        let operations = state.operation_runtime().clone();
        let view = state.routing().registry.list().pop().unwrap();
        let app = build_app(state.clone());
        let request = || match door {
            "/export" => export_request(Vec::new()),
            _ => json_post(door, &json!({"branch": "main"})),
        };

        // Keep the first response body completely unpolled. Its bounded channel
        // may fill, but the queued terminal frame or in-flight producer must keep
        // ownership of the sole immutable root cut.
        let first = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let error: ErrorOutput = serde_json::from_slice(&second_body).unwrap();
        let limit = error.resource_limit.expect("typed root-cut ceiling");
        assert_eq!(limit.resource, "stream_export_slots");
        assert_eq!((limit.limit, limit.actual), (1, 2));
        drop(second_body);

        // Dropping the body is the HTTP disconnect analogue. The producer's
        // cancellation path must release the cut and the body's byte reservation
        // without waiting for another output write.
        drop(first);
        let response = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let response = app.clone().oneshot(request()).await.unwrap();
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
        drop(body);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), operations.wait_logical_owners())
                .await
                .unwrap()
        );

        // A graph transition must retain the same body/producer/transport-byte
        // owners without closing the process or replacing the engine.
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        let first_chunk = stream.try_next().await.unwrap().expect("snapshot record");
        let transition = state
            .prepare_same_view(
                &view.key,
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .unwrap()
            .close()
            .unwrap();
        {
            let wait = transition.wait_requests();
            tokio::pin!(wait);
            assert!(
                futures::poll!(&mut wait).is_pending(),
                "{door}: unread body owns the graph"
            );
            drop(stream);
            assert!(
                futures::poll!(&mut wait).is_pending(),
                "{door}: yielded bytes own the graph"
            );
            drop(first_chunk);
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap();
        }
        let next_epoch = transition.resume_same_view().unwrap();
        assert_ne!(next_epoch, view.epoch());
        let resumed = state.routing().registry.list().pop().unwrap();
        assert!(Arc::ptr_eq(resumed.handle(), view.handle()));
        assert!(!operations.snapshot().closed);

        // Exercise the actual streaming handlers through the process-close
        // boundary. Both response bodies and already-yielded transport chunks
        // must retain their observer after their request handler has returned.
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        let first_chunk = stream.try_next().await.unwrap().expect("snapshot record");
        assert!(!first_chunk.is_empty());
        operations.close();
        let logical_wait = operations.wait_logical_owners();
        tokio::pin!(logical_wait);
        assert!(
            futures::poll!(&mut logical_wait).is_pending(),
            "{door}: an unread response still owns work"
        );
        let (status, ready) = get_json(&app, "/readyz".to_string()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ready["status"], "draining");
        assert_eq!(ready["ready"], false);
        assert_eq!(
            get_json(&app, "/healthz".to_string()).await.0,
            StatusCode::OK
        );
        // Management still reaches its existing authorization boundary. This
        // fixture has no management policy, so 403 remains correct while closed.
        assert_eq!(
            get_json(&app, "/graphs".to_string()).await.0,
            StatusCode::FORBIDDEN
        );
        let (status, refused) = get_json(&app, g("/snapshot")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused["code"], "service_unavailable");
        drop(stream);
        assert!(
            futures::poll!(&mut logical_wait).is_pending(),
            "{door}: yielded transport bytes still own an observer"
        );
        drop(first_chunk);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), logical_wait)
                .await
                .unwrap()
        );
        assert_eq!(operations.snapshot().active_reads, 0);
        assert!(operations.snapshot().closed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_route_returns_graph_and_published_dataset_versions() {
    let (temp, app) = app_for_loaded_graph().await;
    let graph = graph_path(temp.path());
    let expected_graph_manifest_version = manifest_dataset_version(&graph).await;

    let (snapshot_status, snapshot_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

/// A loaded graph whose `main` and `feature` branches set Alice's age to
/// different values, so merging `feature` into `main` conflicts.
async fn divergent_alice_graph() -> tempfile::TempDir {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
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
    temp
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_merge_conflict_response_includes_structured_conflicts() {
    let temp = divergent_alice_graph().await;
    let graph = graph_path(temp.path());

    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);
    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: false,
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

fn json_post(path: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri(g(path))
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

/// Hold only this request thread at the existing engine seam. Other server
/// tests may merge concurrently without consuming or waiting on this hold.
struct MergeReturnHold {
    thread: std::thread::ThreadId,
    hold: Arc<omnigraph::seams::Hold>,
}

impl omnigraph::seams::Behavior for MergeReturnHold {
    fn uninstalling(&self) {
        self.hold.release();
    }
}

impl omnigraph::seams::Decide for MergeReturnHold {
    fn decide(&self, name: &'static str) -> omnigraph::seams::Decision {
        if std::thread::current().id() == self.thread {
            omnigraph::seams::Decide::decide(self.hold.as_ref(), name)
        } else {
            omnigraph::seams::Decision::Pass
        }
    }
}

/// Cancel and join the HTTP waiter while its owned write is still pending on
/// the engine's schema gate. The separate schema/control holder leaves main's
/// HEAD unchanged, so eventual graph contents independently prove continuation.
/// This is router cancellation evidence, not an HTTP/2 reset claim.
#[tokio::test(flavor = "multi_thread")]
async fn disconnected_writes_keep_admission_until_the_original_operation_finishes() {
    use omnigraph::seams::catalog::{
        BRANCH_CREATE_POST_INVENTORY_PRE_NATIVE, SCHEMA_RELOAD_BEFORE_CONTRACT_READ,
    };
    const ATOMIC: &str = r#"query atomic() {
        insert Person { name: "Owned", age: 41 }
        insert Company { name: "OwnedCo" }
        insert WorksAt { from: "Owned", to: "OwnedCo" }
    }"#;
    const BATCH: &str = concat!(
        "{\"type\":\"Person\",\"data\":{\"name\":\"Owned\",\"age\":41}}\n",
        "{\"type\":\"Company\",\"data\":{\"name\":\"OwnedCo\"}}\n",
        "{\"edge\":\"WorksAt\",\"from\":\"Owned\",\"to\":\"OwnedCo\"}\n"
    );
    for door in ["/mutate", "/load", "/load/ndjson", "/branches/merge"] {
        let temp = init_loaded_graph().await;
        let graph = graph_path(temp.path());
        let db = Arc::new(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
        if door == "/branches/merge" {
            db.branch_create("feature").await.unwrap();
            Session::from_defaults(Arc::clone(&db), SessionSettings::default())
                .mutate("feature", ATOMIC, "atomic", &Default::default())
                .await
                .unwrap();
        }
        let workload = omnigraph_server::workload::WorkloadController::new(1, 1_000_000);
        let state = AppState::new_multi(
            vec![Arc::new(omnigraph_server::registry::GraphHandle {
                key: omnigraph_server::identity::GraphKey::cluster(
                    omnigraph_server::graph_id::GraphId::try_from("default").unwrap(),
                ),
                uri: graph.to_string_lossy().to_string(),
                engine: Arc::clone(&db),
                policy: None,
                queries: None,
            })],
            Vec::new(),
            None,
            workload.clone(),
            None,
        )
        .unwrap();
        let operations = state.operation_runtime().clone();
        let view = state.routing().registry.list().pop().unwrap();
        let app = build_app(state.clone());
        let harness = matrix::Harness {
            _temp: temp,
            app: app.clone(),
        };
        let before = db.list_commits(None).await.unwrap();
        let request = match door {
            "/mutate" => json_post(door, &json!({"query": ATOMIC})),
            "/load" => json_post(door, &json!({"data": BATCH, "mode": "append"})),
            "/load/ndjson" => Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .method(Method::POST)
                .uri(g(door))
                .header("content-type", "application/x-ndjson")
                .body(Body::from(BATCH))
                .unwrap(),
            _ => json_post(door, &json!({"source": "feature", "delete_branch": true})),
        };
        let holder_db = Arc::clone(&db);
        let (start, started) = std::sync::mpsc::channel();
        let holder_thread = std::thread::spawn(move || {
            started.recv().unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    if door == "/branches/merge" {
                        // Schema apply requires main-only graphs. Creating an
                        // unrelated native branch also holds the exclusive
                        // schema gate, without publishing a main graph commit.
                        holder_db.branch_create("gate-holder").await.unwrap();
                    } else {
                        holder_db.refresh().await.unwrap();
                    }
                });
        });
        let hold = Arc::new(omnigraph::seams::Hold::default());
        let seam = if door == "/branches/merge" {
            &BRANCH_CREATE_POST_INVENTORY_PRE_NATIVE
        } else {
            &SCHEMA_RELOAD_BEFORE_CONTRACT_READ
        };
        let guard = seam.install(Arc::new(MergeReturnHold {
            thread: holder_thread.thread().id(),
            hold: Arc::clone(&hold),
        }));
        start.send(()).unwrap();
        hold.wait_until_reached();
        let waiter = tokio::spawn(app.clone().oneshot(request));
        tokio::time::timeout(Duration::from_secs(10), async {
            while operations.snapshot().active_writes == 0 {
                assert!(
                    !waiter.is_finished(),
                    "{door} must reach owned admission before waiting on the schema gate"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(operations.snapshot().active_writes, 1, "{door}");
        waiter.abort();
        let cancelled = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            cancelled.is_cancelled(),
            "the waiter must stop before releasing the engine gate"
        );
        assert!(
            !holder_thread.is_finished(),
            "the original operation is still blocked"
        );
        let (status, refused) = json_response(&app, json_post("/mutate", &json!({"query": MUTATION_QUERIES, "name": "insert_person", "params": {"name": "Refused", "age": 1}}))).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{door}: {refused}");
        assert_eq!(
            operations.snapshot().active_writes,
            1,
            "disconnect cannot release the write permit"
        );
        assert_eq!(workload.snapshot().operation_count, 1);
        assert_eq!(workload.snapshot().ingress_count, 1);
        assert!(
            workload.snapshot().ingress_bytes > 0,
            "disconnect cannot release the retained input budget"
        );
        let transition = state
            .prepare_same_view(
                &view.key,
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .unwrap()
            .close()
            .unwrap();
        {
            let wait = transition.wait_requests();
            tokio::pin!(wait);
            assert!(
                futures::poll!(&mut wait).is_pending(),
                "{door}: the detached write must still own its graph root"
            );
        }
        let (status, _) = json_response(&app, get_request(&g("/snapshot"), "")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        hold.release();
        holder_thread.join().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), operations.wait_logical_owners())
                .await
                .unwrap()
        );
        assert!(!hold.timed_out());
        drop(guard);
        assert_eq!(operations.snapshot().active_writes, 0);
        assert_eq!(operations.snapshot().uncertain_writes, 0);
        assert_eq!(workload.snapshot().operation_count, 0);
        assert_eq!(workload.snapshot().ingress_count, 0);
        assert_eq!(workload.snapshot().ingress_bytes, 0);
        tokio::time::timeout(Duration::from_secs(10), transition.wait_requests())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(transition.resume_same_view().unwrap(), view.epoch());
        assert!(!operations.snapshot().closed);
        let after = db.list_commits(None).await.unwrap();
        assert_eq!(
            after.len(),
            before.len() + 1,
            "one original publication: {door}"
        );
        assert_eq!(
            after[0].parent_commit_id,
            Some(before[0].graph_commit_id.clone())
        );
        assert_eq!(harness.person_count("main").await, 5, "{door}");
        let (status, rows) = json_response(&app, json_post("/query", &json!({"query": "query q() { match { $p: Person { name: \"Owned\" } $p worksAt $c } return { $p.name, $c.name } }"}))).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(
            rows["rows"],
            json!([{ "p.name": "Owned", "c.name": "OwnedCo" }]),
            "all participants must publish together"
        );
        if door == "/branches/merge" {
            assert!(
                !db.branch_list()
                    .await
                    .unwrap()
                    .contains(&"feature".to_string()),
                "optional deletion belongs to the surviving owner"
            );
        }
        let (status, sentinel) = json_response(&app, json_post("/mutate", &json!({"query": MUTATION_QUERIES, "name": "insert_person", "params": {"name": "Sentinel", "age": 2}}))).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "same process must resume: {sentinel}"
        );
        assert_eq!(harness.person_count("main").await, 6);
    }
}

struct ThreadEngineFault {
    thread: std::thread::ThreadId,
    reached: Arc<AtomicBool>,
}

impl omnigraph::seams::Behavior for ThreadEngineFault {}

impl omnigraph::seams::Decide for ThreadEngineFault {
    fn decide(&self, name: &'static str) -> omnigraph::seams::Decision {
        if std::thread::current().id() != self.thread {
            return omnigraph::seams::Decision::Pass;
        }
        self.reached.store(true, Ordering::SeqCst);
        panic!("contained engine panic at {name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_panics_close_admission_without_fabricating_rollback() {
    use omnigraph::seams::catalog::{
        GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT, GRAPH_PUBLISH_BEFORE_COMMIT_APPEND,
    };
    for (seam, published) in [
        (&GRAPH_PUBLISH_BEFORE_COMMIT_APPEND, false),
        (&GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT, true),
    ] {
        let temp = init_loaded_graph().await;
        let graph = graph_path(temp.path());
        let state = AppState::open(graph.to_string_lossy().to_string())
            .await
            .unwrap();
        let operations = state.operation_runtime().clone();
        let view = state.routing().registry.list().pop().unwrap();
        // Preparing cannot close healthy admission; later uncertainty must
        // invalidate this reserved attempt before it can close or resume.
        let prepared = state
            .prepare_same_view(
                &view.key,
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .unwrap();
        let app = build_app(state.clone());
        let history = || {
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/commits"))
                .body(Body::empty())
                .unwrap()
        };
        let (_, before) = json_response(&app, history()).await;
        let request_app = app.clone();
        let (start, started) = std::sync::mpsc::channel();
        let request_thread = std::thread::spawn(move || {
            started.recv().unwrap();
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(json_response(&request_app, json_post("/mutate", &json!({"query": MUTATION_QUERIES, "name": "insert_person", "params": {"name": "Uncertain", "age": 12}}))))
        });
        let reached = Arc::new(AtomicBool::new(false));
        let guard = seam.install(Arc::new(ThreadEngineFault {
            thread: request_thread.thread().id(),
            reached: Arc::clone(&reached),
        }));
        start.send(()).unwrap();
        let (status, output) = request_thread.join().unwrap();
        drop(guard);
        assert!(reached.load(Ordering::SeqCst), "required fault must fire");
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{output}");
        assert!(operations.snapshot().closed);
        assert_eq!(operations.snapshot().uncertain_writes, 1);
        assert!(!operations.wait_logical_owners().await);
        assert!(
            prepared.close().is_err(),
            "uncertainty must defeat an earlier prepared transition"
        );
        assert!(
            state
                .prepare_same_view(
                    &view.key,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .is_err(),
            "uncertainty must refuse a new transition"
        );
        let (status, refusal) = json_response(&app, json_post("/mutate", &json!({"query": MUTATION_QUERIES, "name": "insert_person", "params": {"name": "MustNotRun", "age": 1}}))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{refusal}");
        // The closed serving epoch cannot provide the oracle. Use a
        // read-only handle after the request runtime has been contained.
        let db = Omnigraph::open_read_only(graph.to_str().unwrap())
            .await
            .unwrap();
        let commits = db.list_commits(None).await.unwrap();
        assert_eq!(
            commits.len(),
            before["commits"].as_array().unwrap().len() + usize::from(published)
        );
        assert_eq!(
            commits[0].graph_commit_id == before["commits"][0]["graph_commit_id"].as_str().unwrap(),
            !published,
            "publication is independent of result delivery"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial(branch_merge_pre_return)]
async fn branch_merge_receipts_keep_own_publication_when_a_later_writer_finishes_first() {
    use omnigraph::seams::catalog::BRANCH_MERGE_PRE_RETURN;

    for door in ["/mutate", "/branches/merge"] {
        for outcome in ["fast_forward", "merged", "already_up_to_date"] {
            let (_temp, app) = app_for_loaded_graph().await;
            let (status, body) = json_response(
                &app,
                json_post("/mutate", &json!({"query": "branch create feature"})),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            for (branch, name, write) in [
                ("feature", "Source", outcome != "already_up_to_date"),
                ("main", "Target", outcome == "merged"),
            ] {
                if write {
                    let (status, body) = json_response(
                        &app,
                        json_post(
                            "/mutate",
                            &json!({
                                "query": MUTATION_QUERIES,
                                "name": "insert_person",
                                "params": {"name": name, "age": 31},
                                "branch": branch
                            }),
                        ),
                    )
                    .await;
                    assert_eq!(status, StatusCode::OK, "{body}");
                }
            }
            let commits_request = |branch| {
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .uri(g(&format!("/commits?branch={branch}")))
                    .body(Body::empty())
                    .unwrap()
            };
            let (_, before) = json_response(&app, commits_request("main")).await;
            let (_, source) = json_response(&app, commits_request("feature")).await;
            let before_count = before["commits"].as_array().unwrap().len();
            let merge_body = if door == "/mutate" {
                json!({"query": "branch merge feature into main"})
            } else {
                json!({"source": "feature", "target": "main"})
            };
            let request = json_post(door, &merge_body);
            let request_app = app.clone();
            let (start, started) = std::sync::mpsc::channel();
            let request_thread = std::thread::spawn(move || {
                started.recv().unwrap();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(json_response(&request_app, request))
            });
            let hold = Arc::new(omnigraph::seams::Hold::default());
            let guard = BRANCH_MERGE_PRE_RETURN.install(Arc::new(MergeReturnHold {
                thread: request_thread.thread().id(),
                hold: hold.clone(),
            }));
            start.send(()).unwrap();
            hold.wait_until_reached();
            // A has returned from the merge implementation and released its
            // write gates, but has not returned to the HTTP response builder.
            let (_, published) = json_response(&app, commits_request("main")).await;
            assert_eq!(
                published["commits"].as_array().unwrap().len(),
                before_count + usize::from(outcome != "already_up_to_date"),
                "{door} {outcome}"
            );
            let (status, later) = json_response(
                &app,
                json_post(
                    "/mutate",
                    &json!({
                        "query": MUTATION_QUERIES,
                        "name": "insert_person",
                        "params": {"name": "Later", "age": 32},
                        "branch": "main"
                    }),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{later}");
            assert_eq!(
                later["commit"]["parent_commit_id"],
                published["commits"][0]["graph_commit_id"]
            );
            hold.release();
            let (status, receipt) = request_thread.join().unwrap();
            assert!(!hold.timed_out(), "the test must release the reached hold");
            drop(guard);
            assert_eq!(status, StatusCode::OK, "{receipt}");
            let returned_outcome = if door == "/mutate" {
                &receipt["outcome"]["merge"]
            } else {
                &receipt["outcome"]
            };
            assert_eq!(returned_outcome, outcome, "{door}: {receipt}");
            assert!(receipt.get("commit").is_some(), "{door}: {receipt}");
            if outcome == "already_up_to_date" {
                assert_eq!(receipt["commit"], Value::Null);
                assert_eq!(published["commits"][0], before["commits"][0]);
            } else {
                assert_eq!(
                    receipt["commit"], published["commits"][0],
                    "{door}: A must return its own publication even after B has finished"
                );
                assert_ne!(receipt["commit"], later["commit"]);
                assert_eq!(receipt["commit"]["graph_branch"], Value::Null);
                assert_eq!(
                    receipt["commit"]["parent_commit_id"],
                    before["commits"][0]["graph_commit_id"]
                );
                assert_eq!(
                    receipt["commit"]["merged_parent_commit_id"],
                    source["commits"][0]["graph_commit_id"]
                );
                assert_receipt_commit_matches_get(&app, &receipt).await;
            }
            let (_, after) = json_response(&app, commits_request("main")).await;
            assert_eq!(after["commits"][0], later["commit"]);
            assert_eq!(
                after["commits"].as_array().unwrap().len(),
                before_count + 1 + usize::from(outcome != "already_up_to_date"),
                "neither result construction nor delivery may replay the merge"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_statements_dispatch_to_their_route_bodies() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "branch create feature", "name": null, "params": null, "branch": null}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "branch": "feature",
            "query_name": "branch create",
            "affected_nodes": 0,
            "affected_edges": 0,
            "actor_id": null,
            "commit": null,
            "outcome": {"kind": "created", "from": "main", "name": "feature"}
        })
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": "branch list", "name": null, "params": null, "branch": null, "snapshot": null}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "query_name": "branch list",
            "target": {"branch": null, "snapshot": null},
            "row_count": 2,
            "columns": ["name"],
            "rows": [{"name": "feature"}, {"name": "main"}]
        })
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "branch create \"review/x\" from feature"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["outcome"],
        json!({"kind": "created", "from": "feature", "name": "review/x"})
    );
    let (_, body) =
        json_response(&app, json_post("/query", &json!({"query": "branch list"}))).await;
    assert_eq!(
        body["rows"],
        json!([{"name": "feature"}, {"name": "main"}, {"name": "review/x"}])
    );
    let alice_on = |branch: &str| {
        json_post(
            "/query",
            &json!({"query": FIND_PERSON_GQ, "params": {"name": "Alice"}, "branch": branch}),
        )
    };
    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "branch merge feature into main"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "main");
    assert_eq!(body["query_name"], "branch merge");
    assert_eq!(body["commit"], Value::Null);
    assert_eq!(
        body["outcome"],
        json!({"kind": "merged", "source": "feature", "target": "main", "merge": "already_up_to_date"})
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({
                "query": MUTATION_QUERIES,
                "name": "insert_person",
                "params": {"name": "Fay", "age": 40},
                "branch": "feature"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("outcome").is_none(), "{body}");
    let (status, body) = json_response(
        &app,
        json_post("/mutate", &json!({"query": "branch merge feature"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "main");
    assert_eq!(body["affected_nodes"], 0);
    assert_eq!(body["affected_edges"], 0);
    assert_eq!(
        body["outcome"],
        json!({"kind": "merged", "source": "feature", "target": "main", "merge": "fast_forward"})
    );
    assert!(
        body["commit"]["graph_commit_id"].is_string(),
        "a fast-forward publishes its own target commit: {body}"
    );
    assert_receipt_commit_matches_get(&app, &body).await;
    let (_, commits) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits?branch=main"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(commits["commits"][0], body["commit"]);

    for (branch, name) in [("main", "Mo"), ("review/x", "Ro")] {
        let (status, body) = json_response(
            &app,
            json_post(
                "/mutate",
                &json!({
                    "query": MUTATION_QUERIES,
                    "name": "insert_person",
                    "params": {"name": name, "age": 50},
                    "branch": branch
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "branch merge \"review/x\" into main"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["outcome"],
        json!({"kind": "merged", "source": "review/x", "target": "main", "merge": "merged"})
    );
    assert!(
        body["commit"]["merged_parent_commit_id"].is_string(),
        "{body}"
    );
    assert_receipt_commit_matches_get(&app, &body).await;

    let (status, body) = json_response(
        &app,
        json_post("/mutate", &json!({"query": "branch delete \"review/x\""})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "branch": "review/x",
            "query_name": "branch delete",
            "affected_nodes": 0,
            "affected_edges": 0,
            "actor_id": null,
            "commit": null,
            "outcome": {"kind": "deleted", "name": "review/x"}
        })
    );
    let (_, body) =
        json_response(&app, json_post("/query", &json!({"query": "branch list"}))).await;
    assert_eq!(body["rows"], json!([{"name": "feature"}, {"name": "main"}]));
    let (status, body) = json_response(&app, alice_on("review/x")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "branch 'review/x' not found");
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_statement_refusals_name_the_door_and_the_envelope() {
    let (_temp, app) = app_for_loaded_graph().await;
    let target_refusal = "a branch statement names its branches itself; drop the request target";
    let name_refusal = "a branch statement takes no name and no parameters";
    let deprecated_refusal =
        "branch statements are not served on deprecated routes; use POST /mutate or POST /query";
    let cases = [
        (
            "/query",
            json!({"query": "branch create b0"}),
            "statement 'branch create' is a control write; use POST /mutate",
        ),
        (
            "/query",
            json!({"query": "branch delete b0"}),
            "statement 'branch delete' is a control write; use POST /mutate",
        ),
        (
            "/query",
            json!({"query": "branch merge b0 into main"}),
            "statement 'branch merge' is a control write; use POST /mutate",
        ),
        (
            "/query",
            json!({"query": "branch merge b0", "branch": "main"}),
            "statement 'branch merge' is a control write; use POST /mutate",
        ),
        (
            "/mutate",
            json!({"query": "branch list"}),
            "statement 'branch list' is a read; use POST /query",
        ),
        (
            "/mutate",
            json!({"query": "branch list", "branch": "main"}),
            "statement 'branch list' is a read; use POST /query",
        ),
        (
            "/query",
            json!({"query": "branch list", "branch": "main"}),
            target_refusal,
        ),
        (
            "/query",
            json!({"query": "branch list", "snapshot": "0123456789abcdef"}),
            target_refusal,
        ),
        (
            "/query",
            json!({"query": "branch list", "name": "branch list"}),
            name_refusal,
        ),
        (
            "/query",
            json!({"query": "branch list", "params": {}}),
            name_refusal,
        ),
        (
            "/mutate",
            json!({"query": "branch create b0", "branch": "main"}),
            target_refusal,
        ),
        (
            "/mutate",
            json!({"query": "branch merge b0 into main", "branch": "other"}),
            target_refusal,
        ),
        (
            "/mutate",
            json!({"query": "branch create b0", "name": "x"}),
            name_refusal,
        ),
        (
            "/mutate",
            json!({"query": "branch delete b0", "params": {"a": 1}}),
            name_refusal,
        ),
        (
            "/mutate",
            json!({"query": "branch create b0", "branch": "main", "name": "x"}),
            target_refusal,
        ),
        (
            "/mutate",
            json!({"query": EXPLAIN_ADULTS}),
            "statement 'explain' is a read; use POST /query",
        ),
        (
            "/change",
            json!({"query": EXPLAIN_ADULTS}),
            "the explain statement is not served on deprecated routes; use POST /query",
        ),
        (
            "/read",
            json!({"query_source": EXPLAIN_ADULTS}),
            "the explain statement is not served on deprecated routes; use POST /query",
        ),
        (
            "/read",
            json!({"query_source": "branch list"}),
            deprecated_refusal,
        ),
        (
            "/read",
            json!({"query_source": "branch create b0"}),
            deprecated_refusal,
        ),
        (
            "/read",
            json!({"query_source": "branch delete b0"}),
            deprecated_refusal,
        ),
        (
            "/change",
            json!({"query": "branch create b0"}),
            deprecated_refusal,
        ),
        (
            "/change",
            json!({"query": "branch delete b0"}),
            deprecated_refusal,
        ),
        (
            "/change",
            json!({"query": "branch list"}),
            deprecated_refusal,
        ),
        (
            "/change",
            json!({"query": "branch merge b0", "branch": "main"}),
            deprecated_refusal,
        ),
    ];
    for (path, request, expected) in cases {
        let (status, body) = json_response(&app, json_post(path, &request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {request}: {body}");
        assert_eq!(body["error"], expected, "{path} {request}");
    }

    let conditional = [
        (
            json!({"query": "branch create b0"}),
            "a branch statement takes no commit precondition",
        ),
        (
            json!({"query": "branch create b0", "name": "x"}),
            name_refusal,
        ),
    ];
    for (request, expected) in conditional {
        let (status, body) = json_response(
            &app,
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/mutate/if-graph-commit"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .header("omnigraph-if-graph-commit", "0123456789abcdef")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request}: {body}");
        assert_eq!(body["error"], expected, "{request}");
    }

    let (route_status, route_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/nope"))
            .method(Method::DELETE)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let (status, body) = json_response(
        &app,
        json_post("/mutate", &json!({"query": "branch delete nope"})),
    )
    .await;
    assert_eq!(route_status, StatusCode::NOT_FOUND, "{route_body}");
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body, route_body);

    let (status, body) =
        json_response(&app, json_post("/query", &json!({"query": "branch list"}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"],
        json!([{"name": "main"}]),
        "a refused statement must create nothing"
    );

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "b0".to_string(),
    };
    let post_create = || {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap()
    };
    let (status, body) = json_response(&app, post_create()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (route_status, route_body) = json_response(&app, post_create()).await;
    let (status, body) = json_response(
        &app,
        json_post("/mutate", &json!({"query": "branch create b0"})),
    )
    .await;
    assert_eq!(route_status, StatusCode::CONFLICT, "{route_body}");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body, route_body);
}

#[tokio::test(flavor = "multi_thread")]
async fn parse_error_precedes_policy_denial_on_every_door() {
    let (_temp, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-bruno", "team-token"), ("act-nobody", "nobody-token")],
        POLICY_YAML,
    )
    .await;
    let send = |path: &str, token: &str, body: Value, expected_head: bool| {
        let mut builder = Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g(path))
            .method(Method::POST)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json");
        if expected_head {
            builder = builder.header("omnigraph-if-graph-commit", "0123456789abcdef");
        }
        builder
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    };
    let read = json!({"query": FIND_PERSON_GQ, "params": {"name": "Alice"}});
    let legacy_read = json!({"query_source": FIND_PERSON_GQ, "params": {"name": "Alice"}});
    let write = json!({
        "query": MUTATION_QUERIES,
        "name": "insert_person",
        "params": {"name": "Nia", "age": 20},
        "branch": "main"
    });
    let denied = [
        ("/query", "nobody-token", read.clone(), false),
        ("/read", "nobody-token", legacy_read, false),
        ("/mutate", "team-token", write.clone(), false),
        ("/change", "team-token", write.clone(), false),
        ("/mutate/if-graph-commit", "team-token", write, true),
    ];
    for (path, token, body, expected_head) in denied {
        let (status, out) = json_response(&app, send(path, token, body, expected_head)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {out}");

        let mut unparseable = json!({"query": "not gq at all", "branch": "main"});
        if path == "/read" {
            unparseable = json!({"query_source": "not gq at all", "branch": "main"});
        }
        let (status, out) =
            json_response(&app, send(path, token, unparseable, expected_head)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {out}");
        let error: ErrorOutput = serde_json::from_value(out).unwrap();
        assert_ne!(error.code, Some(ErrorCode::Forbidden), "{path}");
        let diagnostic = error
            .diagnostic
            .expect("a refused query carries its diagnostic");
        assert_eq!(diagnostic.code, "Q001", "{path}");
        assert!(diagnostic.position.is_some(), "{path}");

        // The measured shape, a declaration without its parameter list, is
        // refused at the name's end with the one fix at every door.
        let field = if path == "/read" {
            "query_source"
        } else {
            "query"
        };
        let missing = json!({field: "query name { match { $p: Person } return { $p.name } }", "branch": "main"});
        let (status, out) = json_response(&app, send(path, token, missing, expected_head)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {out}");
        let error: ErrorOutput = serde_json::from_value(out).unwrap();
        let diagnostic = error
            .diagnostic
            .expect("a refused query carries its diagnostic");
        assert_eq!(diagnostic.code, "Q002", "{path}");
        assert_eq!(diagnostic.fix.as_deref(), Some("query name()"), "{path}");
        let suggestion = diagnostic.suggestion.unwrap();
        assert_eq!(
            suggestion.applicability,
            omnigraph_api_types::ApplicabilityOutput::MachineApplicable
        );
        assert_eq!(
            suggestion.edits,
            vec![omnigraph_api_types::TextEditOutput {
                start: 10,
                end: 10,
                replacement: "()".to_string(),
            }],
            "{path}"
        );
        assert_eq!(
            diagnostic.position.map(|at| (at.line, at.column)),
            Some((1, 11)),
            "{path}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_merge_statement_conflict_matches_the_route_409() {
    let temp = divergent_alice_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: false,
        settings: None,
    };
    let (route_status, route_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(route_status, StatusCode::CONFLICT, "{route_body}");

    let (statement_status, statement_body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "branch merge feature into main"}),
        ),
    )
    .await;
    assert_eq!(statement_status, StatusCode::CONFLICT, "{statement_body}");
    assert_eq!(statement_body, route_body);
    let error: ErrorOutput = serde_json::from_value(statement_body).unwrap();
    assert_eq!(error.code, Some(ErrorCode::Conflict));
    assert!(
        error.error.starts_with("merge conflicts: "),
        "{}",
        error.error
    );
    assert!(!error.merge_conflicts.is_empty());
}

const PROCESS_SETTING_NEEDLE: &str = "is a process setting";
const UNKNOWN_VALUE_NEEDLE: &str = "expected one of off, on, verify";
const UNKNOWN_SETTING_NEEDLE: &str = "unknown setting `turbo`";
const SET_PARAMETER_SHAPE: &str = "query parameter 'set' takes <name>=<value>, got 'merge_lineage'";
const NO_STATEMENT_NEEDLE: &str = "carries no statement";
const SHOW_AT_WRITE_DOOR: &str = "statement 'show merge_lineage' is a read; use POST /query";
const STATEMENT_AT_DEPRECATED_ROUTE: &str =
    "branch statements are not served on deprecated routes; use POST /mutate or POST /query";
const SETTINGS_AT_DEPRECATED_ROUTE: &str = "the deprecated /read and /change routes take no settings, neither a settings field nor a set or reset prefix; use POST /query or POST /mutate";
const SHOW_COLUMNS: [&str; 5] = ["name", "value", "default", "source", "scope"];

/// Every settings refusal is `ApiError::bad_request`: the message, the code,
/// and nothing else in the body.
fn assert_settings_refusal(status: StatusCode, body: &Value, error: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body, &json!({"error": error, "code": "bad_request"}));
}

/// A refusal whose sentence the settings definition owns: the 400 and the
/// claim, never the whole message, which the definition's own tests pin.
fn assert_settings_refusal_needle(status: StatusCode, body: &Value, needle: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "bad_request", "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains(needle), "{needle} not in {error}");
}

/// A `settings` field naming a `process` setting is serde's unknown-field
/// refusal, projected into the same 400 body shape.
async fn assert_unknown_settings_field(app: &axum::Router, request: Request<Body>) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let text = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{status}: {text}");
    let body: Value =
        serde_json::from_str(&text).unwrap_or_else(|_| panic!("not a JSON error body: {text}"));
    assert_eq!(body["code"], "bad_request", "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains("unknown field"), "{error}");
    assert_eq!(body.as_object().unwrap().len(), 2, "{body}");
}

fn show_row(name: &str, value: &str, default: &str, source: &str, scope: &str) -> Value {
    json!({"name": name, "value": value, "default": default, "source": source, "scope": scope})
}

/// The `show` answer for `rows`: `query_name` `show`, no target, the five
/// string columns, no graph commit.
fn show_output(rows: &[Value]) -> Value {
    json!({
        "query_name": "show",
        "target": {"branch": null, "snapshot": null},
        "row_count": rows.len(),
        "columns": SHOW_COLUMNS,
        "rows": rows,
    })
}

/// The `merge_lineage` value the definition's defaults carry: `verify` in
/// a debug build, the row's `on` otherwise.
fn default_merge_lineage() -> &'static str {
    if cfg!(debug_assertions) {
        "verify"
    } else {
        "on"
    }
}

/// The head of `main`: the commit with the largest graph manifest version.
async fn main_head_commit_id(app: &axum::Router) -> String {
    let (status, out) = get_json(app, g("/commits?branch=main")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
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

/// `branch create <name>` then one insert on it, so a merge into `main`
/// fast-forwards.
async fn branch_one_commit_ahead(app: &axum::Router, name: &str, person: &str) {
    let (status, body) = json_response(
        app,
        json_post(
            "/mutate",
            &json!({"query": format!("branch create {name}")}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = json_response(
        app,
        json_post(
            "/mutate",
            &json!({
                "query": MUTATION_QUERIES,
                "name": "insert_person",
                "params": {"name": person, "age": 30},
                "branch": name
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_process_setting_in_request_text_is_refused_at_both_doors() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({
                "query": format!("set stage_write_concurrency = 8;\n{FIND_PERSON_GQ}"),
                "params": {"name": "Alice"}
            }),
        ),
    )
    .await;
    assert_settings_refusal_needle(status, &body, PROCESS_SETTING_NEEDLE);

    let (status, body) = json_response(
        &app,
        json_post("/query", &json!({"query": "reset rrf_plan;\nshow all;"})),
    )
    .await;
    assert_settings_refusal_needle(status, &body, PROCESS_SETTING_NEEDLE);

    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({
                "query": format!("set stage_write_concurrency = 8;\n{MUTATION_QUERIES}"),
                "name": "insert_person",
                "params": {"name": "Pat", "age": 1}
            }),
        ),
    )
    .await;
    assert_settings_refusal_needle(status, &body, PROCESS_SETTING_NEEDLE);
}

/// A `process` name in the `settings` field is refused, never dropped: the
/// conditional mutation route, where the refusal must also leave the branch
/// head where it was.
#[tokio::test(flavor = "multi_thread")]
async fn settings_field_with_a_process_name_is_an_unknown_field() {
    let (_temp, app) = app_for_loaded_graph().await;

    let head = main_head_commit_id(&app).await;
    assert_unknown_settings_field(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate/if-graph-commit"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .header("omnigraph-if-graph-commit", &head)
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "query": MUTATION_QUERIES,
                    "name": "set_age",
                    "params": {"name": "Alice", "age": 42},
                    "branch": "main",
                    "settings": {"stage_write_concurrency": 8}
                }))
                .unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(
        main_head_commit_id(&app).await,
        head,
        "a refused request has no effect"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_field_round_trips_through_show_and_text_overrides_it() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": "show merge_lineage;", "settings": {"merge_lineage": "verify"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[show_row(
            "merge_lineage",
            "verify",
            "on",
            "request",
            "request"
        )])
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": "show merge_lineage;", "settings": {"merge_lineage": "off"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[show_row("merge_lineage", "off", "on", "request", "request")])
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({
                "query": "set merge_lineage = off;\nshow merge_lineage;",
                "settings": {"merge_lineage": "verify"}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[show_row("merge_lineage", "off", "on", "file", "request")])
    );
}

/// `reset` in the text drops the field's value with it: the row returns to
/// the process baseline, not to what the request asked for.
#[tokio::test(flavor = "multi_thread")]
async fn settings_reset_in_text_returns_to_the_baseline_not_the_request_value() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({
                "query": "reset merge_lineage;\nshow merge_lineage;",
                "settings": {"merge_lineage": "off"}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[show_row(
            "merge_lineage",
            default_merge_lineage(),
            "on",
            "default",
            "request"
        )])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_field_carries_ann_nprobes_into_the_session() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": "show ann_nprobes;", "settings": {"ann_nprobes": 1}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[show_row("ann_nprobes", "1", "20", "request", "request")])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_show_all_lists_the_definition_in_order() {
    let (_temp, app) = app_for_loaded_graph().await;
    let (status, body) =
        json_response(&app, json_post("/query", &json!({"query": "show all;"}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        show_output(&[
            show_row("engine", "v2", "v2", "default", "request"),
            show_row("rrf_plan", "auto", "auto", "default", "process"),
            show_row(
                "merge_lineage",
                default_merge_lineage(),
                "on",
                "default",
                "request"
            ),
            show_row("ann_nprobes", "20", "20", "default", "request"),
            show_row("stage_write_concurrency", "8", "8", "default", "process"),
            show_row(
                "traversal_work_limit",
                "1000000",
                "1000000",
                "default",
                "request"
            ),
        ])
    );
}

/// The field is accepted and the conditional mutation lands; that the mode
/// it names selects a classifier is the mode oracle's claim, pinned by
/// `crates/omnigraph/tests/merge_cost.rs::merge_lineage_setting_selects_the_completed_classifier`.
#[tokio::test(flavor = "multi_thread")]
async fn settings_field_is_accepted_at_the_conditional_mutation_route() {
    fn conditional(body: &Value, expected_commit: &str) -> Request<Body> {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/mutate/if-graph-commit"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .header("omnigraph-if-graph-commit", expected_commit)
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }
    let (_temp, app) = app_for_loaded_graph().await;

    let head = main_head_commit_id(&app).await;
    let (status, body) = json_response(
        &app,
        conditional(
            &json!({
                "query": MUTATION_QUERIES,
                "name": "set_age",
                "params": {"name": "Alice", "age": 41},
                "branch": "main",
                "settings": {"merge_lineage": "off"}
            }),
            &head,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["affected_nodes"], 1, "{body}");
    assert_ne!(
        main_head_commit_id(&app).await,
        head,
        "the conditional mutation landed a commit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_show_is_refused_at_the_write_door_and_the_deprecated_read_door() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post("/mutate", &json!({"query": "show merge_lineage;"})),
    )
    .await;
    assert_settings_refusal(status, &body, SHOW_AT_WRITE_DOOR);

    let (status, body) = json_response(
        &app,
        json_post("/read", &json!({"query_source": "show merge_lineage;"})),
    )
    .await;
    assert_settings_refusal(status, &body, STATEMENT_AT_DEPRECATED_ROUTE);
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_field_is_refused_at_the_deprecated_change_route() {
    let (_temp, app) = app_for_loaded_graph().await;
    let mutation = "mutation add_person($name: String) {\n    insert Person { name: $name }\n}";

    let (status, body) = json_response(
        &app,
        json_post(
            "/change",
            &json!({
                "query_source": mutation,
                "params": {"name": "Zed"},
                "settings": {"merge_lineage": "off"}
            }),
        ),
    )
    .await;
    assert_settings_refusal(status, &body, SETTINGS_AT_DEPRECATED_ROUTE);

    assert_unknown_settings_field(
        &app,
        json_post(
            "/change",
            &json!({
                "query_source": mutation,
                "params": {"name": "Zed"},
                "settings": {"stage_write_concurrency": 64}
            }),
        ),
    )
    .await;
}

/// Both carriers meet the same refusal at both deprecated routes, which
/// serve their legacy bodies under the process defaults alone.
#[tokio::test(flavor = "multi_thread")]
async fn settings_field_and_prefix_are_refused_at_the_deprecated_routes() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/read",
            &json!({
                "query_source": FIND_PERSON_GQ,
                "params": {"name": "Alice"},
                "settings": {"merge_lineage": "off"}
            }),
        ),
    )
    .await;
    assert_settings_refusal(status, &body, SETTINGS_AT_DEPRECATED_ROUTE);

    let (status, body) = json_response(
        &app,
        json_post(
            "/read",
            &json!({
                "query_source": format!("set merge_lineage = off;\n{FIND_PERSON_GQ}"),
                "params": {"name": "Alice"}
            }),
        ),
    )
    .await;
    assert_settings_refusal(status, &body, SETTINGS_AT_DEPRECATED_ROUTE);

    let (status, body) = json_response(
        &app,
        json_post(
            "/change",
            &json!({
                "query": format!("set merge_lineage = off;\n{MUTATION_QUERIES}"),
                "name": "insert_person",
                "params": {"name": "Dep", "age": 2}
            }),
        ),
    )
    .await;
    assert_settings_refusal(status, &body, SETTINGS_AT_DEPRECATED_ROUTE);
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_a_file_of_only_set_lines_is_refused_at_both_doors() {
    let (_temp, app) = app_for_loaded_graph().await;
    for door in ["/query", "/mutate"] {
        let (status, body) = json_response(
            &app,
            json_post(door, &json!({"query": "set merge_lineage = off;"})),
        )
        .await;
        assert_settings_refusal_needle(status, &body, NO_STATEMENT_NEEDLE);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_set_parameter_on_the_change_routes_follows_the_definition() {
    let (_temp, app) = app_for_loaded_graph().await;
    let commit_id = load_commit(&app, r#"{"type":"Person","data":{"name":"S1","age":1}}"#).await;

    let cases: [(&str, Option<&str>); 6] = [
        ("set=merge_lineage=off", None),
        ("set=engine=v2", None),
        ("set=merge_lineage=both", Some(UNKNOWN_VALUE_NEEDLE)),
        (
            "set=stage_write_concurrency=8",
            Some(PROCESS_SETTING_NEEDLE),
        ),
        ("set=turbo=v2", Some(UNKNOWN_SETTING_NEEDLE)),
        ("set=merge_lineage", Some(SET_PARAMETER_SHAPE)),
    ];
    for (query, refusal) in cases {
        let uri = format!("/changes?start=now&{query}");
        let (status, body) = get_json(&app, g(&uri)).await;
        match refusal {
            None => assert_eq!(status, StatusCode::OK, "{uri}: {body}"),
            Some(needle) => assert_settings_refusal_needle(status, &body, needle),
        }
    }

    let uri = format!("/commits/{commit_id}/changes?set=merge_lineage=off");
    let (status, body) = get_json(&app, g(&uri)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the commit-diff route takes the same parameter: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn settings_reset_all_at_the_http_door_returns_to_the_process_defaults() {
    let (settings, sources) = omnigraph::settings::from_env_with(|variable| {
        (variable == "OMNIGRAPH_ANN_NPROBES").then(|| "5".to_string())
    })
    .unwrap();
    let (_temp, app) =
        app_for_loaded_graph_with_process_defaults(ProcessDefaults { settings, sources }).await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": "set engine = v2;\nset merge_lineage = off;\nreset all;\nshow all;"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0],
        show_row("engine", "v2", "v2", "default", "request"),
        "{body}"
    );
    assert_eq!(
        body["rows"][2],
        show_row(
            "merge_lineage",
            default_merge_lineage(),
            "on",
            "default",
            "request"
        ),
        "{body}"
    );
    assert_eq!(
        body["rows"][3],
        show_row("ann_nprobes", "5", "20", "env", "request"),
        "reset all returns to the process value, not the definition's: {body}"
    );
}

/// Both carriers are accepted at `branch merge` and the merge runs; that the
/// mode they name selects a classifier is the mode oracle's claim, pinned by
/// `crates/omnigraph/tests/merge_cost.rs::merge_lineage_setting_selects_the_completed_classifier`.
#[tokio::test(flavor = "multi_thread")]
async fn settings_field_and_prefix_are_accepted_at_branch_merge() {
    let (_temp, app) = app_for_loaded_graph().await;

    branch_one_commit_ahead(&app, "feature", "Fay").await;
    let (status, body) = json_response(
        &app,
        json_post(
            "/branches/merge",
            &json!({"source": "feature", "target": "main", "settings": {"merge_lineage": "off"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "fast_forward", "{body}");

    branch_one_commit_ahead(&app, "feature2", "Gus").await;
    let (status, body) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "set merge_lineage = off;\nbranch merge feature2 into main;"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "main");
    assert_eq!(body["query_name"], "branch merge");
    assert_eq!(
        body["outcome"],
        json!({"kind": "merged", "source": "feature2", "target": "main", "merge": "fast_forward"})
    );
    assert!(
        body["commit"]["graph_commit_id"].is_string(),
        "a fast-forward moves main to the commit authored on feature2: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_read_after_change_sees_updated_state_from_same_app() {
    let (_temp, app) = app_for_loaded_graph().await;

    let change = ChangeRequest {
        query: MUTATION_QUERIES.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({ "name": "Mina", "age": 28 })),
        branch: Some("main".to_string()),
        settings: None,
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

const EXPLAIN_ADULTS: &str = "explain query adults() {\n    match {\n        $p: Person\n        $p.age > 30\n    }\n    return { $p.name }\n}\n";

/// One tree of an `explain` answer, rebuilt from its pre-order rows: a node
/// is its `detail` fields plus `node`, its children the rows one level deeper
/// that follow it.
fn explain_tree(body: &Value, tree: &str) -> Value {
    fn build(rows: &[&Value], index: &mut usize, depth: i64) -> Value {
        let row = rows[*index];
        *index += 1;
        let mut node: serde_json::Map<String, Value> =
            serde_json::from_str(row["detail"].as_str().unwrap())
                .unwrap_or_else(|err| panic!("{err}: {row}"));
        node.insert("node".to_string(), row["node"].clone());
        let mut inputs = Vec::new();
        while *index < rows.len() && rows[*index]["depth"] == depth + 1 {
            inputs.push(build(rows, index, depth + 1));
        }
        node.insert("inputs".to_string(), Value::Array(inputs));
        Value::Object(node)
    }
    let rows: Vec<&Value> = body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {body}"))
        .iter()
        .filter(|row| row["tree"] == tree)
        .collect();
    assert!(!rows.is_empty(), "no `{tree}` rows: {body}");
    assert_eq!(rows[0]["depth"], 0, "{body}");
    let mut index = 0;
    let root = build(&rows, &mut index, 0);
    assert_eq!(
        index,
        rows.len(),
        "every `{tree}` row hangs off the root: {body}"
    );
    root
}

/// The `plan` rows of an `explain` answer whose `node` is `name`, as details.
fn explain_plan_rows<'a>(body: &'a Value, name: &str) -> Vec<&'a str> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["tree"] == "plan" && row["node"] == name)
        .map(|row| {
            assert!(row.get("depth").is_none(), "{row}");
            row["detail"].as_str().unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_answers_an_explain_statement_as_rows() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": EXPLAIN_ADULTS, "branch": "main"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["query_name"], "adults");
    assert_eq!(body["columns"], json!(["tree", "depth", "node", "detail"]));
    assert_eq!(body["rows"][0]["tree"], "logical", "{body}");
    let logical = explain_tree(&body, "logical");
    assert!(logical["node"].is_string(), "{logical}");
    assert_eq!(
        logical["inputs"][0]["table"], "node:Person",
        "the logical tree scans Person: {logical}"
    );
    assert_eq!(
        logical["inputs"][0]["filter"]["reads"],
        json!([{"binding": "p", "property": "age"}]),
        "the age filter is pushed into the scan: {logical}"
    );
    assert_eq!(
        logical["inputs"][0]["filter"]["text"], "$p.age > 30",
        "the filter prints as GQ: {logical}"
    );
    assert!(
        logical["inputs"][0].get("side").is_none()
            && logical["inputs"][0].get("fragments").is_none(),
        "a query scan carries no diff-side or substrate fields: {logical}"
    );
    let physical = explain_tree(&body, "physical");
    assert!(physical["node"].is_string(), "{physical}");
    assert!(
        !explain_plan_rows(&body, "pass").is_empty(),
        "one plan row per fired pass: {body}"
    );
    assert_eq!(explain_plan_rows(&body, "route"), vec!["engine"], "{body}");
    assert!(
        explain_plan_rows(&body, "logical_hash")[0].len() == 16,
        "{body}"
    );

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": EXPLAIN_ADULTS, "name": "adults", "snapshot": "0123456789abcdef"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": EXPLAIN_INSERT_PERSON, "name": "insert_person"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"], "`explain` applies to a read query; 'insert_person' contains mutations",
        "{body}"
    );
}

const EXPLAIN_INSERT_PERSON: &str = "explain query insert_person($name: String, $age: I32) {\n    insert Person { name: $name, age: $age }\n}\n";

const EXPLAIN_OLDER_PAIRS: &str = "explain query older_pairs() {\n    match {\n        $a: Person\n        $b: Person\n        $a.age > $b.age\n    }\n    return { $a.name, $b.name }\n}\n";

/// The `explain` answer lists the lowered DataFusion tree after the physical
/// tree: a filter over two bindings, which no scan can take, runs in the
/// `CrossJoinExec` of the two `ScanExec` rows.
#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_explain_lists_the_lowered_datafusion_tree() {
    let (_temp, app) = app_for_loaded_graph().await;

    let (status, body) = json_response(
        &app,
        json_post(
            "/query",
            &json!({"query": EXPLAIN_OLDER_PAIRS, "branch": "main"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().unwrap();
    let mut trees: Vec<&str> = Vec::new();
    for row in rows {
        let tree = row["tree"].as_str().unwrap();
        if trees.last() != Some(&tree) {
            trees.push(tree);
        }
    }
    assert_eq!(
        trees,
        vec!["logical", "physical", "datafusion", "plan"],
        "the three trees then the plan rows: {body}"
    );
    let datafusion: Vec<&Value> = rows
        .iter()
        .filter(|row| row["tree"] == "datafusion")
        .collect();
    assert_eq!(datafusion[0]["depth"], 0, "{body}");
    let join = datafusion
        .iter()
        .find(|row| row["node"] == "CrossJoinExec")
        .unwrap_or_else(|| panic!("a filter over two bindings runs in the CrossJoinExec: {body}"));
    let predicate = join["detail"].as_str().unwrap();
    assert!(
        predicate.contains("a.age") && predicate.contains("b.age"),
        "the predicate reads both bindings: {predicate}"
    );
    assert_eq!(
        datafusion
            .iter()
            .filter(|row| row["node"] == "ScanExec")
            .count(),
        2,
        "one ScanExec per binding: {body}"
    );
    assert!(
        !datafusion.iter().any(|row| row["node"] == "FilterExec"),
        "the join holds the filter, so no FilterExec row: {body}"
    );
    assert!(
        explain_plan_rows(&body, "datafusion").is_empty(),
        "the tree is available, so no `datafusion` plan row: {body}"
    );
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
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

/// An empty source parses to zero declarations; the refusal names that
/// count, not "multiple queries", and names no CLI flag.
#[tokio::test(flavor = "multi_thread")]
async fn empty_source_is_refused_as_no_query_on_both_doors() {
    let (_temp, app) = app_for_loaded_graph().await;

    for path in ["/query", "/mutate"] {
        let request = json!({ "query": "" });
        let (status, body) = json_response(
            &app,
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g(path))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(
            body["error"], "query file contains no query",
            "{path}: {body}"
        );
    }
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
async fn loads_report_unsupported_embedding_generation_without_changing_vectors() {
    const SCHEMA: &str = r#"
node Doc {
    slug: String @key
    body: String
    embedding: Vector(2)? @embed(body)
}
node RequiredDoc {
    slug: String @key
    body: String
    embedding: Vector(2) @embed(body)
}
node Plain { slug: String @key }
"#;
    for path in ["/load", "/load/ndjson", "/ingest"] {
        let temp = init_graph_with_schema(SCHEMA).await;
        let graph = graph_path(temp.path());
        let config = omnigraph::embedding::EmbeddingConfig::from_parts(
            Some("mock"),
            None,
            Some("diagnostics-test".to_string()),
            String::new(),
        )
        .unwrap();
        let db = Omnigraph::open(graph.to_str().unwrap())
            .await
            .unwrap()
            .with_embedding_config(Arc::new(config));
        let app = build_app(AppState::new(graph.to_string_lossy().to_string(), db));
        let request = |data: &str| {
            if path == "/load/ndjson" {
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .uri(g("/load/ndjson?branch=main&mode=merge"))
                    .method(Method::POST)
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from(data.to_owned()))
                    .unwrap()
            } else {
                json_post(
                    path,
                    &json!({"branch": "main", "mode": "merge", "data": data}),
                )
            }
        };
        let (status, body) = json_response(
            &app,
            request(concat!(
                r#"{"type":"Doc","data":{"slug":"omitted","body":"missing vector"}}"#,
                "\n",
                r#"{"type":"Doc","data":{"slug":"supplied","body":"keep vector","embedding":[0.25,0.75]}}"#,
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(body["embedding_generation"], "unsupported", "{path}");

        let (status, body) = json_response(
            &app,
            json_post("/query", &json!({"query": "query docs() { match { $d: Doc } return { $d.slug, $d.embedding } order { $d.slug asc } }"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(
            body["rows"],
            json!([
                {"d.slug": "omitted"},
                {"d.slug": "supplied", "d.embedding": [0.25, 0.75]},
            ]),
            "{path}"
        );
        // JSON query projection omits null cells. Independently verify the
        // durable Arrow column so an omitted output key is not our null oracle.
        let persisted = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
        let snapshot = persisted
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap();
        let batches: Vec<_> = snapshot
            .open_dataset("node:Doc")
            .await
            .unwrap()
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.column_by_name("embedding").unwrap().null_count())
                .sum::<usize>(),
            1
        );

        // This is a capability diagnostic, even when every vector is supplied.
        let (status, body) = json_response(
            &app,
            request(r#"{"type":"Doc","data":{"slug":"all-supplied","body":"also keep","embedding":[1,0]}}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(body["embedding_generation"], "unsupported", "{path}");

        let (status, body) = json_response(
            &app,
            request(r#"{"type":"RequiredDoc","data":{"slug":"missing","body":"requires vector"}}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        let (status, body) =
            json_response(&app, request(r#"{"type":"Plain","data":{"slug":"plain"}}"#)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(
            body.get("embedding_generation"),
            Some(&Value::Null),
            "{path}"
        );
    }
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    let (_temp, app) = app_for_loaded_graph().await;

    let request = ReadRequest {
        query_source: fs::read_to_string(fixture("test.gq")).unwrap(),
        query_name: Some("get_person".to_string()),
        params: Some(json!({ "name": "Alice" })),
        branch: Some("main".to_string()),
        snapshot: None,
        settings: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        "POST /read's envelope is an indefinite compatibility contract (this fixture has no cell whose spelling RFC 0051 changes)"
    );
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        body.get("graph_commit_id").is_none(),
        "POST /read has an indefinite byte-stable envelope and must not gain the canonical route's graph_commit_id: {body}"
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
        settings: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
async fn query_rows_omit_null_cells() {
    let (_temp, app) = app_for_loaded_graph().await;

    let request = QueryRequest {
        query: "query knows_since() {\n    match {\n        $a: Person\n        $a $k:knows $b\n    }\n    return { $a.name, $k.since }\n}\n"
            .to_string(),
        name: Some("knows_since".to_string()),
        params: None,
        branch: Some("main".to_string()),
        snapshot: None,
        settings: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/query"))
                .method(Method::POST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = std::str::from_utf8(&body_bytes).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["columns"], json!(["a.name", "k.since"]), "{body}");
    assert_eq!(body["row_count"], 3, "{body}");
    assert!(
        text.contains(r#""rows":[{"a.name":""#) && !text.contains("k.since\":"),
        "a null cell's key is omitted from its row, and rows travel as the writer's bytes: {text}"
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    let (temp, app) = app_for_loaded_graph().await;

    // Native name validation is a refusal before any branch effect. Both
    // creation doors must leave admission open for the valid flow below.
    for from in [Some("main"), None] {
        let (status, _) = json_response(
            &app,
            json_post("/branches", &json!({"from": from, "name": "bad?name"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, body) = json_response(
            &app,
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(g("/branches"))
                .method(Method::GET)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "name refusal must keep admission open"
        );
        assert_eq!(body["branches"], json!(["main"]));
    }

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

    // Target namespace inventory reads the ancestor ref before it can clone
    // or reclaim anything. A failed read must leave server admission open.
    let refs = graph_path(temp.path()).join("__manifest/_refs/branches");
    let feature_ref = fs::read_dir(refs)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("feature.")
        })
        .unwrap();
    let original = fs::read(&feature_ref).unwrap();
    fs::write(&feature_ref, b"{").unwrap();
    let (status, _) = json_response(
        &app,
        json_post(
            "/branches",
            &json!({"from": "main", "name": "feature/child"}),
        ),
    )
    .await;
    fs::write(&feature_ref, original).unwrap();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (change_status, change_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (read_status, read_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
async fn branch_merge_delete_branch_retires_parent_with_live_child() {
    let (_temp, app) = app_for_loaded_graph().await;

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (change_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/change"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&change).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(change_status, StatusCode::OK);

    let create_child = BranchCreateRequest {
        from: Some("feature".to_string()),
        name: "feature-child".to_string(),
    };
    let (create_child_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create_child).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(create_child_status, StatusCode::OK);

    let merge = BranchMergeRequest {
        source: "feature".to_string(),
        target: Some("main".to_string()),
        delete_branch: true,
        settings: None,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches/merge"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&merge).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(merge_status, StatusCode::OK);
    assert_eq!(merge_body["outcome"], "fast_forward");
    assert_receipt_commit_matches_get(&app, &merge_body).await;
    assert_eq!(merge_body["branch_deleted"], true);
    assert!(merge_body.get("branch_delete_error_details").is_none());
    assert!(merge_body.get("branch_delete_error").is_none());

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_body["branches"], json!(["feature-child", "main"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_merge_delete_branch_refusal_is_non_fatal() {
    let (_temp, app) = app_for_loaded_graph().await;

    let create = BranchCreateRequest {
        from: Some("main".to_string()),
        name: "feature".to_string(),
    };
    let (create_status, _) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&create).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(create_status, StatusCode::OK);

    let merge = BranchMergeRequest {
        source: "main".to_string(),
        target: Some("feature".to_string()),
        delete_branch: true,
        settings: None,
    };
    let (merge_status, merge_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    assert!(merge_body.get("branch_delete_error").is_none());
    assert_eq!(merge_body["commit"], Value::Null);
    assert_eq!(
        merge_body["branch_delete_error_details"]["code"],
        "bad_request"
    );
    assert!(
        merge_body["branch_delete_error_details"]["error"]
            .as_str()
            .unwrap()
            .contains("cannot delete branch 'main'")
    );

    let (list_status, list_body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/branches"))
            .method(Method::GET)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_body["branches"], json!(["feature", "main"]));
}

#[tokio::test(flavor = "multi_thread")]
#[serial(branch_merge_pre_return)]
async fn pre_effect_delete_failure_preserves_receipt_and_write_capacity_under_slow_reads() {
    use omnigraph::seams::catalog::BRANCH_MERGE_PRE_RETURN;
    for compound in [false, true] {
        let temp = init_loaded_graph().await;
        let graph = graph_path(temp.path());
        let workload = omnigraph_server::workload::WorkloadController::with_limits(
            omnigraph_server::workload::WorkloadLimits {
                ingress_inflight_max: 1,
                ..Default::default()
            },
        );
        let state = AppState::new_with_workload(
            graph.to_string_lossy().to_string(),
            Omnigraph::open(graph.to_str().unwrap()).await.unwrap(),
            Vec::new(),
            workload.clone(),
        );
        let operations = state.operation_runtime().clone();
        let app = build_app(state);
        let (status, result) = json_response(
            &app,
            json_post("/mutate", &json!({"query": "branch create feature"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let (status, result) = json_response(
            &app,
            json_post(
                "/mutate",
                &json!({
                    "query": MUTATION_QUERIES, "name": "insert_person",
                    "params": {"name": "Source", "age": 31}, "branch": "feature"
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let held_read = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .uri(g("/snapshot"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(held_read.status(), StatusCode::OK);
        assert_eq!(operations.snapshot().active_reads, 1);
        let mut branch_refs = fs::read_dir(graph.join("__manifest/_refs/branches"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect::<Vec<_>>();
        assert_eq!(branch_refs.len(), 1, "the fixture has one named branch");
        let branch_ref = branch_refs.pop().unwrap();
        let saved = fs::read(&branch_ref).unwrap();
        let (status, output) = if compound {
            let request_app = app.clone();
            let (start, started) = std::sync::mpsc::channel();
            let request = std::thread::spawn(move || {
                started.recv().unwrap();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(json_response(
                        &request_app,
                        json_post(
                            "/branches/merge",
                            &json!({"source": "feature", "delete_branch": true}),
                        ),
                    ))
            });
            let thread = request.thread().id();
            let source = branch_ref.clone();
            let guard = BRANCH_MERGE_PRE_RETURN.observe(move || {
                if std::thread::current().id() == thread {
                    fs::write(&source, b"{").unwrap();
                }
            });
            start.send(()).unwrap();
            let result = request.join().unwrap();
            drop(guard);
            result
        } else {
            fs::write(&branch_ref, b"{").unwrap();
            json_response(
                &app,
                Request::builder()
                    .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                    .uri(g("/branches/feature"))
                    .method(Method::DELETE)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        };
        fs::write(&branch_ref, &saved).unwrap();
        if compound {
            assert_eq!(status, StatusCode::OK, "{output}");
            assert_eq!(output["outcome"], "fast_forward");
            assert_eq!(output["branch_deleted"], false);
            assert_eq!(output["branch_delete_error_details"]["code"], "internal");
            assert!(
                !output["commit"]["graph_commit_id"]
                    .as_str()
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{output}");
        }
        assert!(
            !operations.snapshot().closed,
            "a proven manifest read failure has no deletion effects"
        );
        assert_eq!(workload.snapshot().ingress_count, 0);
        let (status, sentinel) = json_response(
            &app,
            json_post(
                "/mutate",
                &json!({
                    "query": MUTATION_QUERIES, "name": "insert_person",
                    "params": {"name": "Sentinel", "age": 32}
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{sentinel}");
        drop(held_read);
        assert!(operations.wait_logical_owners().await);
        if compound {
            assert_receipt_commit_matches_get(&app, &output).await;
        }
    }
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        settings: None,
    };
    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        let db = session(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/change"))
            .method(Method::POST)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&ChangeRequest {
                    query: MUTATION_QUERIES.to_string(),
                    name: Some("set_age".to_string()),
                    params: Some(json!({ "name": "Alice", "age": 33 })),
                    branch: Some("main".to_string()),
                    settings: None,
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
                settings: None,
            })
            .unwrap();
            let req = Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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

/// The wire body fits the 1 MiB route limit; one reused parameter expands to over
/// 32 MiB of retained Arrow across two individually legal tables, so the engine's
/// operation bound makes the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_mutation_memory_refusal_keeps_graph_and_admission_usable() {
    let temp = init_graph_with_schema_and_data(
        "node First { name: String @key payload: String }\n\
         node Second { name: String @key payload: String }",
        "",
    )
    .await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let operations = state.operation_runtime().clone();
    let app = build_app(state);
    let history = || {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits"))
            .body(Body::empty())
            .unwrap()
    };
    let (_, before) = json_response(&app, history()).await;

    let mut query = String::from("query wide($payload: String) {\n");
    for table in ["First", "Second"] {
        for row in 0..32 {
            writeln!(
                query,
                "insert {table} {{ name: \"row{row}\", payload: $payload }}"
            )
            .unwrap();
        }
    }
    query.push('}');
    let request = json!({"query": query, "params": {"payload": "x".repeat(600_000)}});
    assert!(serde_json::to_vec(&request).unwrap().len() < 1024 * 1024);
    let (status, output) = json_response(&app, json_post("/mutate", &request)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{output}");
    let error: ErrorOutput = serde_json::from_value(output).unwrap();
    let refusal = error.resource_limit.expect("engine resource refusal");
    assert_eq!(refusal.resource, "retained keyed batch bytes per operation");
    assert_eq!(refusal.limit, 32 * 1024 * 1024);
    assert!(refusal.actual > refusal.limit);
    assert_eq!(operations.snapshot().uncertain_writes, 0);
    assert!(!operations.snapshot().closed);
    let (_, after) = json_response(&app, history()).await;
    assert_eq!(after, before, "refusal must publish no graph commit");

    for table in ["First", "Second"] {
        let (status, rows) = json_response(
            &app,
            json_post(
                "/query",
                &json!({"query": format!(
                    "query rows() {{ match {{ $n: {table} }} return {{ $n.name }} }}"
                )}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows["row_count"], 0);
    }
    let (status, output) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query":
                "query small() { insert First { name: \"ok\", payload: \"ok\" } }"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_receipt_commit_matches_get(&app, &output).await;
}

/// Two 16 MiB ids are seeded through the engine (no route carries them); a `/mutate`
/// delete and a `/load` overwrite must each refuse the removed-id sum as a certain 413.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_id_memory_refusals_keep_graph_and_admission_usable() {
    let wide = "x".repeat(16 * 1024 * 1024);
    let temp = init_graph_with_schema_and_data(
        "node First { name: String @key tag: String? }\n\
         node Second { name: String @key tag: String? }",
        &format!(
            "{}\n{}",
            json!({"type": "First", "data": {"name": wide}}),
            json!({"type": "Second", "data": {"name": wide}}),
        ),
    )
    .await;
    drop(wide);
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let operations = state.operation_runtime().clone();
    let app = build_app(state);
    let history = || {
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri(g("/commits"))
            .body(Body::empty())
            .unwrap()
    };
    let (_, before) = json_response(&app, history()).await;

    for (door, request) in [
        (
            "/mutate",
            json!({"query": "query clear() {\n\
                delete First where name != \"\"\n\
                delete Second where name != \"\"\n\
            }"}),
        ),
        (
            "/load",
            json!({
                "mode": "overwrite",
                "data": "{\"type\":\"First\",\"data\":{\"name\":\"small\"}}\n\
                         {\"type\":\"Second\",\"data\":{\"name\":\"small\"}}",
            }),
        ),
    ] {
        let (status, output) = json_response(&app, json_post(door, &request)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{door}: {output}");
        let error: ErrorOutput = serde_json::from_value(output).unwrap();
        let refusal = error.resource_limit.expect("engine resource refusal");
        assert_eq!(
            refusal.resource, "retained removed-id bytes per operation",
            "{door}"
        );
        assert_eq!(refusal.limit, 32 * 1024 * 1024, "{door}");
        assert!(refusal.actual > refusal.limit, "{door}");
        assert_eq!(operations.snapshot().uncertain_writes, 0, "{door}");
        assert!(!operations.snapshot().closed, "{door}");
        let (_, after) = json_response(&app, history()).await;
        assert_eq!(after, before, "{door} refusal must publish no graph commit");
    }

    for table in ["First", "Second"] {
        let (status, rows) = json_response(
            &app,
            json_post(
                "/query",
                &json!({"query": format!(
                    "query rows() {{ match {{ $n: {table} }} return {{ $n.tag }} }}"
                )}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows["row_count"], 1);
    }
    let (status, output) = json_response(
        &app,
        json_post(
            "/mutate",
            &json!({"query": "query small() { insert First { name: \"ok\" } }"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{output}");
    assert_receipt_commit_matches_get(&app, &output).await;
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

    // Spawn N=8 concurrent UPDATE mutations on Alice (from test.jsonl, age=30 at V0)
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
                settings: None,
            })
            .unwrap();
            let req = Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        assert!(
            matches!(
                conflict.member.as_str(),
                "graph_head:main" | "published_dataset_version:node:Person"
            ),
            "a strict loser is refused by the branch head, or by the table pin when it \
             revalidates between the winner's detached table commit and its publish; got {}",
            conflict.member
        );
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
                settings: None,
            })
            .unwrap();
            let req = Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                settings: None,
            })
            .unwrap();
            let req = Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
    let schema = r#"
node Document {
    title: String @key
    body: String?
    content: Blob?
}
"#;
    // A real changed row exceeds the HTTP feed's default packing target;
    // both images and managed-Blob base64 must remain complete. The engine's
    // issue_705 tests separately own the wider-than-sort-cap scan regression.
    let before_body = "x".repeat(4 * 1024 * 1024 + 1);
    let after_body = "y".repeat(before_body.len());
    let before_blob = repeated_zero_blob_input(64 * 1024 + 1);
    let after_blob = repeated_zero_blob_input(64 * 1024 + 2);
    let row = |title: &str, body: &str, content: Option<&str>| {
        json!({"type": "Document", "data": {
            "title": title, "body": body, "content": content,
        }})
        .to_string()
    };
    let temp = init_graph_with_schema_and_data(
        schema,
        &[
            row("A-small", "before", None),
            row("B-wide", &before_body, Some(&before_blob)),
            row("C-tail", "before", None),
        ]
        .join("\n"),
    )
    .await;
    let state = AppState::open(graph_path(temp.path()).to_string_lossy().to_string())
        .await
        .unwrap();
    let view = state.routing().registry.list().pop().unwrap();
    let app = build_app(state.clone());

    // `start=now` captures the head: no replay, a caught-up durable cursor.
    let (status, now) = get_json(&app, g("/changes?start=now")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(now["blocks"].as_array().unwrap().is_empty());
    assert_eq!(now["caught_up"], true);
    let c0 = now["cursor"].as_str().expect("terminal page cursor");

    let commit_id = load_commit(
        &app,
        &[
            row("A-small", "after", None),
            row("B-wide", &after_body, Some(&after_blob)),
            row("C-tail", "after", None),
        ]
        .join("\n"),
    )
    .await;

    // Bytes, not the row limit, split this commit. An initial small change
    // leaves positive space but cannot grant the wide change a second page's
    // solo-overflow allowance. No mid-block cursor is safe to checkpoint.
    let (status, partial) = get_json(&app, g(&format!("/changes?cursor={c0}&limit=100"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(partial["blocks"].as_array().unwrap().len(), 1);
    assert_eq!(partial["blocks"][0]["changes"].as_array().unwrap().len(), 1);
    assert_eq!(
        partial["blocks"][0]["cause"]["graph_commit_id"],
        commit_id.as_str()
    );
    assert_eq!(partial["blocks"][0]["changes"][0]["id"], "A-small");
    assert_eq!(partial["blocks"][0]["changes"][0]["op"], "update");
    assert!(partial["cursor"].is_null(), "no durable cursor mid-block");
    let token = partial["next_page_token"].as_str().expect("page token");
    assert!(token.len() <= 4 * 1024);

    // A later commit cannot enter this page token's captured cut. It must be
    // reached by the next durable-cursor poll after the original block ends.
    let sentinel_commit = load_commit(&app, &row("D-sentinel", "later", None)).await;

    let wide_uri = g(&format!("/changes?page_token={token}"));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                .uri(&wide_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let transition = state
        .prepare_same_view(
            &view.key,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .close()
        .unwrap();
    {
        let wait = transition.wait_requests();
        tokio::pin!(wait);
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "unpolled wide response owns the graph"
        );
        let mut stream = response.into_body().into_data_stream();
        let chunk = stream.try_next().await.unwrap().expect("wide feed page");
        assert!(!chunk.is_empty());
        let retained = chunk.slice(..1);
        drop(chunk);
        drop(stream);
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "a yielded byte slice still owns the abandoned response"
        );
        drop(retained);
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap()
            .unwrap();
    }
    assert_ne!(transition.resume_same_view().unwrap(), view.epoch());
    let resumed_view = state.routing().registry.list().pop().unwrap();
    assert!(Arc::ptr_eq(resumed_view.handle(), view.handle()));

    // Abandoning delivery does not advance a checkpoint. Replaying its exact
    // token returns the complete wide change again, then continuation visits
    // every authored change exactly once in the completed checkpoint walk.
    let (status, resumed) = get_json(&app, wide_uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resumed["blocks"].as_array().unwrap().len(), 1);
    assert_eq!(resumed["blocks"][0]["changes"].as_array().unwrap().len(), 1);
    assert_eq!(resumed["blocks"][0]["cause"]["graph_commit_id"], commit_id);
    let wide = &resumed["blocks"][0]["changes"][0];
    assert_eq!(wide["id"], "B-wide");
    assert_eq!(wide["op"], "update");
    assert_eq!(wide["before"]["properties"]["body"], before_body);
    assert_eq!(wide["after"]["properties"]["body"], after_body);
    assert_eq!(wide["before"]["properties"]["content"], before_blob);
    assert_eq!(wide["after"]["properties"]["content"], after_blob);
    assert!(
        serde_json::to_vec(wide).unwrap().len()
            > omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES as usize
    );
    assert!(resumed["cursor"].is_null());
    let token = resumed["next_page_token"]
        .as_str()
        .expect("tail page token");
    assert!(token.len() <= 4 * 1024);

    let (status, tail) = get_json(&app, g(&format!("/changes?page_token={token}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tail["blocks"].as_array().unwrap().len(), 1);
    assert_eq!(tail["blocks"][0]["changes"].as_array().unwrap().len(), 1);
    assert_eq!(tail["blocks"][0]["cause"]["graph_commit_id"], commit_id);
    assert_eq!(tail["blocks"][0]["changes"][0]["id"], "C-tail");
    assert_eq!(tail["blocks"][0]["changes"][0]["op"], "update");
    assert!(tail["next_page_token"].is_null());
    assert_eq!(
        tail["caught_up"], false,
        "later commit remains outside the cut"
    );
    let c1 = tail["cursor"].as_str().expect("boundary cursor");

    let (status, sentinel) = get_json(&app, g(&format!("/changes?cursor={c1}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sentinel["blocks"].as_array().unwrap().len(), 1);
    assert_eq!(
        sentinel["blocks"][0]["changes"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        sentinel["blocks"][0]["cause"]["graph_commit_id"],
        sentinel_commit
    );
    assert_eq!(sentinel["blocks"][0]["changes"][0]["id"], "D-sentinel");
    assert_eq!(sentinel["blocks"][0]["changes"][0]["op"], "insert");
    assert!(sentinel["next_page_token"].is_null());
    assert_eq!(sentinel["caught_up"], true);
    let c2 = sentinel["cursor"]
        .as_str()
        .expect("sentinel boundary cursor");

    let (status, caught_up) = get_json(&app, g(&format!("/changes?cursor={c2}"))).await;
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
                .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
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
