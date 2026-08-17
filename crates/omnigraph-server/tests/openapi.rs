use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::{AppState, build_app, served_openapi};
use serde_json::Value;
use tower::ServiceExt;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph/tests/fixtures")
        .join(name)
}

fn graph_path(root: &Path) -> PathBuf {
    root.join("openapi_test.omni")
}

async fn init_loaded_graph() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let graph = graph_path(temp.path());
    fs::create_dir_all(&graph).unwrap();
    let schema = fs::read_to_string(fixture("test.pg")).unwrap();
    let data = fs::read_to_string(fixture("test.jsonl")).unwrap();
    Omnigraph::init(graph.to_str().unwrap(), &schema)
        .await
        .unwrap();
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    load_jsonl(&db, &data, LoadMode::Overwrite).await.unwrap();
    temp
}

async fn app_for_loaded_graph() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);
    (temp, app)
}

async fn app_for_loaded_graph_with_auth(token: &str) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_token(
        graph.to_string_lossy().to_string(),
        db,
        Some(token.to_string()),
    );
    let app = build_app(state);
    (temp, app)
}

async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

fn openapi_doc() -> utoipa::openapi::OpenApi {
    // RFC-011 cluster-only: the canonical committed spec is the SERVED
    // shape — protected routes nested under `/graphs/{graph_id}/…`,
    // `/healthz` and `/graphs` flat. This matches what the server serves.
    served_openapi()
}

fn openapi_json() -> Value {
    serde_json::to_value(openapi_doc()).unwrap()
}

fn assert_optional_commit_field(doc: &Value, schema_name: &str) {
    let schema = &doc["components"]["schemas"][schema_name];
    let properties = schema["properties"].as_object().unwrap();
    let commit = properties
        .get("commit")
        .unwrap_or_else(|| panic!("{schema_name} must expose a commit receipt"));
    let required = schema["required"].as_array().unwrap();
    assert!(
        required
            .iter()
            .all(|field| field.as_str() != Some("commit")),
        "{schema_name}.commit must remain optional for successful no-op mutations"
    );
    let commit_ref = commit["$ref"].as_str().or_else(|| {
        commit["oneOf"]
            .as_array()
            .and_then(|schemas| schemas.iter().find_map(|schema| schema["$ref"].as_str()))
    });
    assert_eq!(commit_ref, Some("#/components/schemas/CommitOutput"));
}

// ---------------------------------------------------------------------------
// Endpoint integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn openapi_endpoint_returns_200_with_valid_json() {
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (status, json) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.is_object(), "response must be a JSON object");
}

#[tokio::test]
async fn openapi_endpoint_returns_openapi_31_version() {
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let version = json["openapi"].as_str().unwrap();
    assert!(
        version.starts_with("3.1"),
        "expected OpenAPI 3.1.x, got {version}"
    );
}

#[tokio::test]
async fn openapi_endpoint_does_not_require_auth() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_token(
        graph.to_string_lossy().to_string(),
        db,
        Some("secret-token".to_string()),
    );
    let app = build_app(state);

    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (status, _) = json_response(&app, request).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/openapi.json should not require auth"
    );
}

// ---------------------------------------------------------------------------
// Info and metadata tests
// ---------------------------------------------------------------------------

#[test]
fn openapi_info_contains_title_and_description() {
    let doc = openapi_json();
    let info = &doc["info"];
    assert_eq!(info["title"].as_str().unwrap(), "Omnigraph API");
    assert!(info["description"].as_str().unwrap().contains("Omnigraph"));
}

#[test]
fn openapi_info_contains_version() {
    let doc = openapi_json();
    let version = doc["info"]["version"].as_str().unwrap();
    assert!(!version.is_empty(), "version must not be empty");
}

// ---------------------------------------------------------------------------
// Path coverage tests
// ---------------------------------------------------------------------------

// The canonical served spec keeps `/healthz` and `/graphs` flat; every
// protected route nests under `/graphs/{graph_id}/…`.
const EXPECTED_PATHS: &[&str] = &[
    "/healthz",
    "/graphs",
    "/graphs/{graph_id}/snapshot",
    "/graphs/{graph_id}/blob",
    "/graphs/{graph_id}/read",
    "/graphs/{graph_id}/query",
    "/graphs/{graph_id}/export",
    "/graphs/{graph_id}/change",
    "/graphs/{graph_id}/mutate",
    "/graphs/{graph_id}/mutate/if-graph-commit",
    "/graphs/{graph_id}/queries",
    "/graphs/{graph_id}/queries/{name}",
    "/graphs/{graph_id}/queries/{name}/if-graph-commit",
    "/graphs/{graph_id}/schema",
    "/graphs/{graph_id}/schema/apply",
    "/graphs/{graph_id}/load",
    "/graphs/{graph_id}/load/ndjson",
    "/graphs/{graph_id}/ingest",
    "/graphs/{graph_id}/branches",
    "/graphs/{graph_id}/branches/{branch}",
    "/graphs/{graph_id}/branches/merge",
    "/graphs/{graph_id}/commits",
    "/graphs/{graph_id}/commits/{commit_id}",
    "/graphs/{graph_id}/commits/{commit_id}/changes",
    "/graphs/{graph_id}/changes",
    "/graphs/{graph_id}/changes/baseline",
];

#[test]
fn openapi_contains_all_expected_paths() {
    let doc = openapi_json();
    let paths = doc["paths"].as_object().expect("paths must be an object");
    let path_keys: HashSet<&str> = paths.keys().map(|k| k.as_str()).collect();

    for expected in EXPECTED_PATHS {
        assert!(
            path_keys.contains(expected),
            "missing path: {expected}. Found: {path_keys:?}"
        );
    }
}

#[test]
fn openapi_has_no_unexpected_paths() {
    let doc = openapi_json();
    let paths = doc["paths"].as_object().expect("paths must be an object");
    let expected: HashSet<&str> = EXPECTED_PATHS.iter().copied().collect();

    for path in paths.keys() {
        assert!(
            expected.contains(path.as_str()),
            "unexpected path in OpenAPI spec: {path}"
        );
    }
}

#[test]
fn openapi_commit_changes_is_get_with_page_token_params() {
    let doc = openapi_json();
    let op = &doc["paths"]["/graphs/{graph_id}/commits/{commit_id}/changes"]["get"];
    assert!(op.is_object(), "the commit changes route is a GET");
    let params: Vec<&str> = op["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|param| param["name"].as_str().unwrap())
        .collect();
    for expected in ["commit_id", "page_token", "limit", "kind", "type", "op"] {
        assert!(
            params.contains(&expected),
            "missing param {expected}: {params:?}"
        );
    }
    // Caller byte limits and feed-only continuations never ride the finite
    // commit diff.
    for forbidden in ["max_bytes", "cursor", "start"] {
        assert!(
            !params.contains(&forbidden),
            "param {forbidden} must not exist on the commit diff"
        );
    }
    let responses = op["responses"].as_object().unwrap();
    // No 403: a forbidden commit is indistinguishable from an unknown one (404),
    // so the finite commit diff cannot be a commit-existence oracle.
    for code in ["200", "400", "401", "404", "409", "410", "413"] {
        assert!(responses.contains_key(code), "missing response {code}");
    }
    assert!(
        !responses.contains_key("403"),
        "the commit diff must not advertise 403 (existence oracle)"
    );
}

#[test]
fn openapi_change_feed_is_get_with_cursor_and_start() {
    let doc = openapi_json();
    let op = &doc["paths"]["/graphs/{graph_id}/changes"]["get"];
    assert!(op.is_object(), "the change feed route is a GET");
    let params: Vec<&str> = op["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|param| param["name"].as_str().unwrap())
        .collect();
    for expected in [
        "branch",
        "cursor",
        "start",
        "page_token",
        "limit",
        "kind",
        "type",
        "op",
    ] {
        assert!(
            params.contains(&expected),
            "missing param {expected}: {params:?}"
        );
    }
    assert!(
        !params.contains(&"max_bytes"),
        "caller byte limits never ride the feed"
    );
    let responses = op["responses"].as_object().unwrap();
    for code in ["200", "400", "401", "403", "404", "409", "410", "413"] {
        assert!(responses.contains_key(code), "missing response {code}");
    }
}

#[test]
fn openapi_change_baseline_is_streaming_post() {
    let doc = openapi_json();
    let op = &doc["paths"]["/graphs/{graph_id}/changes/baseline"]["post"];
    assert!(op.is_object(), "the baseline handshake is a POST");
    let ok = &op["responses"]["200"];
    assert!(
        ok["content"]["application/x-ndjson"].is_object(),
        "the baseline streams NDJSON: {ok}"
    );
    for code in ["400", "401", "403", "404", "413", "503"] {
        assert!(
            op["responses"].as_object().unwrap().contains_key(code),
            "missing response {code}"
        );
    }
}

/// Spec-side vocabulary gate: no change-surface schema may declare a physical
/// storage property, and no change route may accept a caller byte limit.
#[test]
fn openapi_change_schemas_reject_storage_vocabulary() {
    const FORBIDDEN_PROPERTIES: &[&str] = &[
        "table_key",
        "stable_table_id",
        "table_incarnation_id",
        "manifest_version",
        "table_version",
        "table_branch",
        "table_path",
        "row_addr",
        "part",
        "commit_complete",
        "change_index",
        "max_bytes",
    ];

    fn assert_schema_clean(name: &str, value: &serde_json::Value) {
        if let Some(properties) = value.get("properties").and_then(|v| v.as_object()) {
            for (property, nested) in properties {
                assert!(
                    !FORBIDDEN_PROPERTIES.contains(&property.as_str()),
                    "schema {name} declares forbidden property '{property}'"
                );
                assert_schema_clean(name, nested);
            }
        }
        for key in ["items", "additionalProperties"] {
            if let Some(nested) = value.get(key) {
                assert_schema_clean(name, nested);
            }
        }
    }

    let doc = openapi_json();
    let schemas = doc["components"]["schemas"].as_object().unwrap();
    let mut change_schemas = 0;
    for (name, schema) in schemas {
        if name.starts_with("Change")
            || name.starts_with("CommitChanges")
            || name.starts_with("EntityChange")
        {
            change_schemas += 1;
            assert_schema_clean(name, schema);
        }
    }
    assert!(change_schemas >= 10, "the change schemas are registered");

    for path in [
        "/graphs/{graph_id}/commits/{commit_id}/changes",
        "/graphs/{graph_id}/changes",
    ] {
        let params = doc["paths"][path]["get"]["parameters"].as_array().unwrap();
        assert!(
            params
                .iter()
                .all(|param| param["name"].as_str() != Some("max_bytes")),
            "{path} must not accept a caller byte limit"
        );
    }
}

/// Reachability form of the vocabulary gate: walk every schema component
/// TRANSITIVELY referenced by the change operations (responses, request
/// bodies, and parameters — error envelopes included) and require every
/// reachable schema to be free of physical storage properties. The name-prefix
/// gate above cannot see a generic component (e.g. a shared error envelope)
/// pulled in by reference; this walk closes that hole — the change routes now
/// reference the graph-vocabulary `ChangeErrorOutput` projection instead of
/// the generic error envelope whose conflict details carry storage keys.
/// Also pins that the baseline's NDJSON success response declares a schema
/// (the terminal `ChangeBaselineRecord` handshake) so generated clients can
/// discover the cursor contract.
#[test]
fn openapi_change_operations_reach_only_graph_vocabulary_schemas() {
    const FORBIDDEN_PROPERTIES: &[&str] = &[
        "table_key",
        "stable_table_id",
        "table_incarnation_id",
        "manifest_version",
        "table_version",
        "table_branch",
        "table_path",
        "row_addr",
    ];

    fn collect_refs(value: &serde_json::Value, refs: &mut std::collections::BTreeSet<String>) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(reference) = map.get("$ref").and_then(|v| v.as_str())
                    && let Some(name) = reference.rsplit('/').next()
                {
                    refs.insert(name.to_string());
                }
                for nested in map.values() {
                    collect_refs(nested, refs);
                }
            }
            serde_json::Value::Array(items) => {
                for nested in items {
                    collect_refs(nested, refs);
                }
            }
            _ => {}
        }
    }

    fn assert_properties_clean(name: &str, value: &serde_json::Value) {
        if let Some(properties) = value.get("properties").and_then(|v| v.as_object()) {
            for (property, nested) in properties {
                assert!(
                    !FORBIDDEN_PROPERTIES.contains(&property.as_str()),
                    "schema '{name}', reachable from a change operation, declares \
                     forbidden storage property '{property}'"
                );
                assert_properties_clean(name, nested);
            }
        }
        for key in ["items", "additionalProperties", "allOf", "oneOf", "anyOf"] {
            if let Some(nested) = value.get(key) {
                assert_properties_clean(name, nested);
            }
        }
    }

    let doc = openapi_json();
    let schemas = doc["components"]["schemas"].as_object().unwrap();
    let change_paths: Vec<(&String, &serde_json::Value)> = doc["paths"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.contains("/changes"))
        .collect();
    assert_eq!(
        change_paths.len(),
        3,
        "the three change routes are registered: {change_paths:?}"
    );

    let mut frontier = std::collections::BTreeSet::new();
    for (_, operations) in &change_paths {
        collect_refs(operations, &mut frontier);
    }
    let mut reachable = std::collections::BTreeSet::new();
    while let Some(name) = frontier.pop_first() {
        if !reachable.insert(name.clone()) {
            continue;
        }
        let Some(schema) = schemas.get(&name) else {
            continue;
        };
        let mut nested = std::collections::BTreeSet::new();
        collect_refs(schema, &mut nested);
        for reference in nested {
            if !reachable.contains(&reference) {
                frontier.insert(reference);
            }
        }
    }
    assert!(
        reachable.contains("ChangeErrorOutput"),
        "change routes reference the graph-vocabulary error projection: {reachable:?}"
    );
    assert!(
        !reachable.contains("ErrorOutput"),
        "the generic error envelope (whose conflict details carry storage keys) \
         must not be reachable from a change operation: {reachable:?}"
    );
    for name in &reachable {
        if let Some(schema) = schemas.get(name) {
            assert_properties_clean(name, schema);
        }
    }

    let baseline_content = &doc["paths"]["/graphs/{graph_id}/changes/baseline"]["post"]["responses"]
        ["200"]["content"]["application/x-ndjson"];
    assert!(
        baseline_content.get("schema").is_some(),
        "the baseline NDJSON success response declares the terminal-record schema"
    );
}

// ---------------------------------------------------------------------------
// HTTP method tests
// ---------------------------------------------------------------------------

#[test]
fn openapi_healthz_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/healthz"]["get"].is_object());
}

#[test]
fn openapi_read_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/read"]["post"].is_object());
}

#[test]
fn openapi_blob_supports_get_and_explicit_head() {
    let doc = openapi_json();
    let path = &doc["paths"]["/graphs/{graph_id}/blob"];
    assert!(path["get"].is_object());
    assert!(path["head"].is_object());

    for method in ["get", "head"] {
        let parameters = path[method]["parameters"].as_array().unwrap();
        for required in ["entity", "type", "id", "property"] {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter["name"] == required)
                .unwrap_or_else(|| panic!("{method} /blob is missing `{required}`"));
            assert_eq!(parameter["in"], "query");
            assert_eq!(parameter["required"], true);
        }
        for optional in ["branch", "snapshot"] {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter["name"] == optional)
                .unwrap_or_else(|| panic!("{method} /blob is missing `{optional}`"));
            assert_eq!(parameter["in"], "query");
            assert_ne!(parameter["required"], true);
        }
        let entity = parameters
            .iter()
            .find(|parameter| parameter["name"] == "entity")
            .unwrap();
        assert_eq!(
            entity["schema"]["$ref"],
            "#/components/schemas/BlobEntityKind"
        );
        assert_eq!(
            doc["components"]["schemas"]["BlobEntityKind"]["enum"],
            serde_json::json!(["node", "edge"])
        );
    }

    let head_parameters = path["head"]["parameters"].as_array().unwrap();
    for ignored_header in ["Range", "If-Range"] {
        let parameter = head_parameters
            .iter()
            .find(|parameter| parameter["name"] == ignored_header)
            .unwrap_or_else(|| panic!("HEAD /blob is missing `{ignored_header}`"));
        assert_eq!(parameter["in"], "header");
        assert!(
            parameter["description"]
                .as_str()
                .is_some_and(|description| description.to_ascii_lowercase().contains("ignored")),
            "HEAD /blob must state that {ignored_header} is ignored"
        );
    }
    for method in ["get", "head"] {
        let parameters = path[method]["parameters"].as_array().unwrap();
        let if_match = parameters
            .iter()
            .find(|parameter| parameter["name"] == "If-Match")
            .unwrap_or_else(|| panic!("{method} /blob is missing `If-Match`"));
        assert_eq!(if_match["in"], "header");
        assert!(
            if_match["description"]
                .as_str()
                .is_some_and(|description| description.contains("Strong")),
            "{method} /blob must document strong If-Match comparison"
        );
    }
}

#[test]
fn openapi_blob_documents_binary_redirect_conditional_and_range_contracts() {
    let doc = openapi_json();
    let path = &doc["paths"]["/graphs/{graph_id}/blob"];
    let get = &path["get"];
    let head = &path["head"];

    for status in ["200", "206"] {
        let schema = &get["responses"][status]["content"]["application/octet-stream"]["schema"];
        assert_eq!(
            schema["type"], "string",
            "GET /blob {status} must describe a byte string"
        );
        assert_eq!(
            schema["format"], "binary",
            "GET /blob {status} must describe binary transfer, not a JSON integer array"
        );
    }
    for status in [
        "302", "304", "400", "401", "403", "404", "412", "416", "500",
    ] {
        assert!(
            get["responses"][status].is_object(),
            "GET /blob must document {status}"
        );
    }
    for status in [
        "200", "302", "304", "400", "401", "403", "404", "412", "500",
    ] {
        assert!(
            head["responses"][status].is_object(),
            "HEAD /blob must document {status}"
        );
    }
    assert!(head["responses"].get("206").is_none());
    assert!(head["responses"].get("416").is_none());
    for status in ["400", "401", "403", "404", "412", "500"] {
        assert!(
            head["responses"][status].get("content").is_none(),
            "HEAD /blob {status} must not promise a JSON body that Axum strips"
        );
    }

    for (status, headers) in [
        (
            "200",
            &[
                "Accept-Ranges",
                "Content-Length",
                "ETag",
                "Omnigraph-Snapshot-Id",
            ][..],
        ),
        (
            "206",
            &[
                "Accept-Ranges",
                "Content-Length",
                "Content-Range",
                "ETag",
                "Omnigraph-Snapshot-Id",
            ][..],
        ),
        (
            "302",
            &["Location", "Cache-Control", "Omnigraph-Snapshot-Id"][..],
        ),
        (
            "304",
            &["Content-Length", "ETag", "Omnigraph-Snapshot-Id"][..],
        ),
        ("412", &["ETag", "Omnigraph-Snapshot-Id"][..]),
        ("416", &["Content-Range"][..]),
    ] {
        for header in headers {
            assert!(
                get["responses"][status]["headers"][header].is_object(),
                "GET /blob {status} must document {header}"
            );
        }
    }

    for status in ["400", "401", "403", "404", "412", "416", "500"] {
        assert_eq!(
            get["responses"][status]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorOutput",
            "GET /blob {status} must use ErrorOutput"
        );
    }
}

#[test]
fn openapi_export_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/export"]["post"].is_object());
}

#[test]
fn export_documents_pre_header_failures() {
    let doc = openapi_json();
    let responses = &doc["paths"]["/graphs/{graph_id}/export"]["post"]["responses"];
    for status in ["400", "401", "403", "404", "409", "413", "503"] {
        assert!(
            responses[status].is_object(),
            "export must document {status}"
        );
        assert_eq!(
            responses[status]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorOutput",
            "export {status} must use ErrorOutput"
        );
    }
}

#[test]
fn openapi_change_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/change"]["post"].is_object());
}

#[test]
fn openapi_mutate_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/mutate"]["post"].is_object());
}

#[test]
fn openapi_conditional_mutation_routes_are_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/mutate/if-graph-commit"]["post"].is_object());
    assert!(doc["paths"]["/graphs/{graph_id}/queries/{name}/if-graph-commit"]["post"].is_object());
    for path in [
        "/graphs/{graph_id}/mutate/if-graph-commit",
        "/graphs/{graph_id}/queries/{name}/if-graph-commit",
    ] {
        let parameters = doc["paths"][path]["post"]["parameters"].as_array().unwrap();
        let header = parameters
            .iter()
            .find(|parameter| parameter["name"] == "Omnigraph-If-Graph-Commit")
            .unwrap_or_else(|| panic!("{path} must declare its capability header"));
        assert_eq!(header["in"], "header");
        assert_eq!(header["required"], true);
    }
    for path in [
        "/graphs/{graph_id}/mutate",
        "/graphs/{graph_id}/queries/{name}",
    ] {
        let parameters = doc["paths"][path]["post"]["parameters"].as_array().unwrap();
        assert!(
            parameters
                .iter()
                .all(|parameter| parameter["name"] != "Omnigraph-If-Graph-Commit"),
            "{path} must not advertise an unsafe optional CAS header"
        );
    }
}

// Deprecation flagging — `/read` and `/change` are kept indefinitely for
// back-compat but are flagged so OpenAPI codegens (typescript-fetch,
// openapi-generator, oapi-codegen, etc.) emit @deprecated on the generated
// SDK methods. The canonical successors `/query` and `/mutate` are not
// flagged. See `deprecation_headers` in `omnigraph-server/src/lib.rs` for
// the matching runtime signal (RFC 9745 + RFC 8288 headers).
#[test]
fn openapi_read_is_deprecated() {
    let doc = openapi_json();
    assert_eq!(
        doc["paths"]["/graphs/{graph_id}/read"]["post"]["deprecated"],
        serde_json::Value::Bool(true),
        "/read must be flagged deprecated in OpenAPI; use /query instead"
    );
}

#[test]
fn openapi_change_is_deprecated() {
    let doc = openapi_json();
    assert_eq!(
        doc["paths"]["/graphs/{graph_id}/change"]["post"]["deprecated"],
        serde_json::Value::Bool(true),
        "/change must be flagged deprecated in OpenAPI; use /mutate instead"
    );
}

#[test]
fn openapi_query_is_not_deprecated() {
    let doc = openapi_json();
    let deprecated = doc["paths"]["/graphs/{graph_id}/query"]["post"]
        .get("deprecated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    assert!(
        !deprecated,
        "/query is the canonical read endpoint and must not be deprecated"
    );
}

#[test]
fn openapi_mutate_is_not_deprecated() {
    let doc = openapi_json();
    let deprecated = doc["paths"]["/graphs/{graph_id}/mutate"]["post"]
        .get("deprecated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    assert!(
        !deprecated,
        "/mutate is the canonical mutation endpoint and must not be deprecated"
    );
}

#[test]
fn openapi_ingest_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/ingest"]["post"].is_object());
}

#[test]
fn openapi_load_is_not_deprecated() {
    // RFC-009 Phase 5: /load is the canonical bulk-load endpoint.
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/load"]["post"].is_object());
    let deprecated = doc["paths"]["/graphs/{graph_id}/load"]["post"]
        .get("deprecated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    assert!(
        !deprecated,
        "/load is the canonical load endpoint and must not be deprecated"
    );
}

#[test]
fn openapi_raw_graph_batch_has_ndjson_body_and_logical_result() {
    let doc = openapi_json();
    let operation = &doc["paths"]["/graphs/{graph_id}/load/ndjson"]["post"];
    assert!(operation.is_object());
    assert!(operation["requestBody"]["content"]["application/x-ndjson"].is_object());
    assert_eq!(
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/GraphBatchLoadOutput"
    );
    let parameters = operation["parameters"].as_array().unwrap();
    for name in ["branch", "from", "mode"] {
        assert!(
            parameters.iter().any(|parameter| parameter["name"] == name),
            "raw graph-batch endpoint must document query parameter {name}"
        );
    }

    let props = doc["components"]["schemas"]["GraphBatchLoadOutput"]["properties"]
        .as_object()
        .unwrap();
    for field in ["branch", "nodes", "edges", "total_rows"] {
        assert!(props.contains_key(field));
    }
    assert_optional_commit_field(&doc, "GraphBatchLoadOutput");
    assert!(!props.contains_key("tables"));
    assert!(!props.contains_key("table_key"));
}

#[test]
fn openapi_ingest_is_deprecated() {
    // RFC-009 Phase 5: /ingest is now the deprecated alias of /load.
    let doc = openapi_json();
    assert_eq!(
        doc["paths"]["/graphs/{graph_id}/ingest"]["post"]["deprecated"],
        serde_json::Value::Bool(true),
        "/ingest must be flagged deprecated now that /load is canonical"
    );
}

#[test]
fn openapi_branches_supports_get_and_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/branches"]["get"].is_object());
    assert!(doc["paths"]["/graphs/{graph_id}/branches"]["post"].is_object());
}

#[test]
fn openapi_branch_delete_is_delete() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/branches/{branch}"]["delete"].is_object());
}

#[test]
fn openapi_branch_merge_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/branches/merge"]["post"].is_object());
}

#[test]
fn openapi_commits_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/commits"]["get"].is_object());
}

#[test]
fn openapi_commit_show_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/commits/{commit_id}"]["get"].is_object());
}

// ---------------------------------------------------------------------------
// Schema coverage tests
// ---------------------------------------------------------------------------

const EXPECTED_SCHEMAS: &[&str] = &[
    "BranchCreateOutput",
    "BranchCreateRequest",
    "BranchDeleteOutput",
    "BranchListOutput",
    "BranchMergeOutcome",
    "BranchMergeOutput",
    "BranchMergeRequest",
    "BlobEntityKind",
    "ChangeOutput",
    "ChangeRequest",
    "QueryRequest",
    "CommitListOutput",
    "CommitOutput",
    "ErrorCode",
    "ErrorOutput",
    "ChangeFeedGapOutput",
    "ChangeEntityKind",
    "ChangeOpOutput",
    "ChangeTypeOutput",
    "ChangeEndpointsOutput",
    "ChangeImageOutput",
    "EntityChangeOutput",
    "ChangeCauseOutput",
    "CommitChangesOutput",
    "ChangeBlockOutput",
    "ChangeFeedOutput",
    "ChangeBaselineRequest",
    "ChangeBaselineOutput",
    "ChangeBaselineRecord",
    "ChangeDiffRefusalOutput",
    "ChangeDiffRefusalReason",
    "BlobRangeOutput",
    "ExternalBlobSourceOutput",
    "ExportRequest",
    "HealthOutput",
    "IngestOutput",
    "IngestRequest",
    "IngestTableOutput",
    "KeyConflictOutput",
    "LoadMode",
    "MergeConflictKindOutput",
    "MergeConflictOutput",
    "ReadOutput",
    "ReadRequest",
    "ReadSetConflictOutput",
    "ReadTargetOutput",
    "PreconditionFailureOutput",
    "RecoveryRequiredOutput",
    "ResourceLimitOutput",
    "ManifestConflictOutput",
    "SchemaApplyOutput",
    "SchemaApplyRequest",
    "SnapshotOutput",
    "SnapshotTableOutput",
];

#[test]
fn openapi_contains_all_expected_schemas() {
    let doc = openapi_json();
    let schemas = doc["components"]["schemas"]
        .as_object()
        .expect("schemas must be an object");
    let schema_keys: HashSet<&str> = schemas.keys().map(|k| k.as_str()).collect();

    for expected in EXPECTED_SCHEMAS {
        assert!(
            schema_keys.contains(expected),
            "missing schema: {expected}. Found: {schema_keys:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Schema field validation tests
// ---------------------------------------------------------------------------

#[test]
fn health_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["HealthOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("status"));
    assert!(props.contains_key("version"));
    assert!(props.contains_key("internal_schema_version"));
    assert!(props.contains_key("source_version"));
}

#[test]
fn read_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ReadRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query_source"));
    assert!(props.contains_key("query_name"));
    assert!(props.contains_key("params"));
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("snapshot"));
}

#[test]
fn read_request_query_source_is_required() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ReadRequest"];
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(required.contains(&"query_source"));
}

#[test]
fn read_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ReadOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query_name"));
    assert!(props.contains_key("target"));
    assert!(props.contains_key("row_count"));
    assert!(props.contains_key("rows"));
}

#[test]
fn change_request_schema_has_expected_fields() {
    // Canonical field names on the wire are now `query` and `name`. The
    // schema descriptions document `query_source` and `query_name` as
    // legacy deserialization aliases for backward compatibility.
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ChangeRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query"));
    assert!(props.contains_key("name"));
    assert!(props.contains_key("params"));
    assert!(props.contains_key("branch"));
    let query_desc = schema["properties"]["query"]["description"]
        .as_str()
        .unwrap_or_default();
    assert!(
        query_desc.contains("query_source"),
        "expected `query` description to mention the legacy `query_source` alias, got: {query_desc}"
    );
}

#[test]
fn query_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["QueryRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query"));
    assert!(props.contains_key("name"));
    assert!(props.contains_key("params"));
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("snapshot"));
}

#[test]
fn query_request_query_is_required() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["QueryRequest"];
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(required.contains(&"query"));
}

#[test]
fn openapi_query_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/graphs/{graph_id}/query"]["post"].is_object());
}

#[test]
fn query_endpoint_documents_mutation_400() {
    let doc = openapi_json();
    let four_hundred = &doc["paths"]["/graphs/{graph_id}/query"]["post"]["responses"]["400"];
    let description = four_hundred["description"].as_str().unwrap_or_default();
    assert!(
        description.contains("mutations") || description.contains("POST /mutate"),
        "expected /query 400 response to mention mutation rejection, got: {description}"
    );
}

#[test]
fn change_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ChangeOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("query_name"));
    assert!(props.contains_key("affected_nodes"));
    assert!(props.contains_key("affected_edges"));
    assert_optional_commit_field(&doc, "ChangeOutput");
}

#[test]
fn ingest_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["IngestRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("from"));
    assert!(props.contains_key("mode"));
    assert!(props.contains_key("data"));
}

#[test]
fn ingest_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["IngestOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("uri"));
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("base_branch"));
    assert!(props.contains_key("branch_created"));
    assert!(props.contains_key("mode"));
    assert!(props.contains_key("tables"));
    assert_optional_commit_field(&doc, "IngestOutput");
}

#[test]
fn export_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ExportRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("type_names"));
    assert!(props.contains_key("table_keys"));
}

#[test]
fn branch_create_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["BranchCreateRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("from"));
    assert!(props.contains_key("name"));
}

#[test]
fn branch_create_request_name_is_required() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["BranchCreateRequest"];
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(required.contains(&"name"));
}

#[test]
fn branch_merge_request_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["BranchMergeRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("source"));
    assert!(props.contains_key("target"));
}

#[test]
fn error_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ErrorOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("error"));
    assert!(props.contains_key("code"));
    assert!(props.contains_key("merge_conflicts"));
    assert!(props.contains_key("manifest_conflict"));
    assert!(props.contains_key("read_set_conflict"));
    assert!(props.contains_key("key_conflict"));
    assert!(props.contains_key("resource_limit"));
    assert!(props.contains_key("blob_range"));
    assert!(props.contains_key("external_blob_source"));
    assert!(props.contains_key("recovery_required"));
    assert!(props.contains_key("precondition_failure"));
}

#[test]
fn manifest_conflict_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ManifestConflictOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("table_key"));
    assert!(props.contains_key("expected"));
    assert!(props.contains_key("actual"));
}

#[test]
fn read_set_conflict_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ReadSetConflictOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("member"));
    assert!(props.contains_key("expected"));
    assert!(props.contains_key("actual"));
}

#[test]
fn key_conflict_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["KeyConflictOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("table_key"));
    assert!(props.contains_key("key"));
}

#[test]
fn resource_limit_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ResourceLimitOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("resource"));
    assert!(props.contains_key("limit"));
    assert!(props.contains_key("actual"));
}

#[test]
fn recovery_required_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["RecoveryRequiredOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("operation_id"));
}

#[test]
fn commit_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["CommitOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("graph_commit_id"));
    assert!(props.contains_key("manifest_version"));
    assert!(props.contains_key("parent_commit_id"));
    assert!(props.contains_key("actor_id"));
    assert!(props.contains_key("created_at"));
}

#[test]
fn snapshot_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["SnapshotOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("branch"));
    assert!(props.contains_key("manifest_version"));
    assert!(props.contains_key("internal_schema_version"));
    assert!(props.contains_key("tables"));
}

#[test]
fn snapshot_table_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["SnapshotTableOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("table_key"));
    assert!(props.contains_key("table_path"));
    assert!(props.contains_key("table_version"));
    assert!(props.contains_key("row_count"));
}

// ---------------------------------------------------------------------------
// Enum schema tests
// ---------------------------------------------------------------------------

#[test]
fn load_mode_schema_has_three_variants() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["LoadMode"];
    let variants = schema["enum"].as_array().unwrap();
    assert_eq!(variants.len(), 3);
    let values: HashSet<&str> = variants.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(values.contains("overwrite"));
    assert!(values.contains("append"));
    assert!(values.contains("merge"));
}

#[test]
fn branch_merge_outcome_schema_has_three_variants() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["BranchMergeOutcome"];
    let variants = schema["enum"].as_array().unwrap();
    assert_eq!(variants.len(), 3);
    let values: HashSet<&str> = variants.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(values.contains("already_up_to_date"));
    assert!(values.contains("fast_forward"));
    assert!(values.contains("merged"));
}

#[test]
fn error_code_schema_has_expected_variants() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ErrorCode"];
    let variants = schema["enum"].as_array().unwrap();
    let values: HashSet<&str> = variants.iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(
        values,
        HashSet::from([
            "unauthorized",
            "forbidden",
            "bad_request",
            "not_found",
            "method_not_allowed",
            "conflict",
            "too_many_requests",
            "internal",
        ]),
        "ErrorCode is a rolling wire contract: new meanings belong in optional \
         structured fields, not new closed-enum values",
    );
}

#[test]
fn external_blob_source_error_is_structured_and_declared_on_write_routes() {
    let doc = openapi_json();
    let detail = &doc["components"]["schemas"]["ExternalBlobSourceOutput"];
    let required: HashSet<&str> = detail["required"]
        .as_array()
        .expect("external Blob source details must declare required fields")
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(required, HashSet::from(["uri", "reason"]));
    let output_field =
        &doc["components"]["schemas"]["ErrorOutput"]["properties"]["external_blob_source"];
    let output_ref = output_field["oneOf"]
        .as_array()
        .and_then(|schemas| schemas.iter().find_map(|schema| schema["$ref"].as_str()))
        .expect("external_blob_source must reference its structured details");
    assert_eq!(output_ref, "#/components/schemas/ExternalBlobSourceOutput");

    for path in [
        "/graphs/{graph_id}/change",
        "/graphs/{graph_id}/mutate",
        "/graphs/{graph_id}/mutate/if-graph-commit",
        "/graphs/{graph_id}/queries/{name}",
        "/graphs/{graph_id}/queries/{name}/if-graph-commit",
        "/graphs/{graph_id}/load",
        "/graphs/{graph_id}/load/ndjson",
        "/graphs/{graph_id}/ingest",
        "/graphs/{graph_id}/branches/merge",
    ] {
        assert_eq!(
            doc["paths"][path]["post"]["responses"]["424"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/ErrorOutput",
            "{path} must advertise the external Blob source failure contract",
        );
    }
}

#[test]
fn blob_range_error_is_structured_and_declared_on_get() {
    let doc = openapi_json();
    let detail = &doc["components"]["schemas"]["BlobRangeOutput"];
    let required: HashSet<&str> = detail["required"]
        .as_array()
        .expect("Blob range details must declare required fields")
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(required, HashSet::from(["start", "end", "length"]));

    let output_field = &doc["components"]["schemas"]["ErrorOutput"]["properties"]["blob_range"];
    let output_ref = output_field["oneOf"]
        .as_array()
        .and_then(|schemas| schemas.iter().find_map(|schema| schema["$ref"].as_str()))
        .expect("blob_range must reference its structured details");
    assert_eq!(output_ref, "#/components/schemas/BlobRangeOutput");
    assert_eq!(
        doc["paths"]["/graphs/{graph_id}/blob"]["get"]["responses"]["416"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ErrorOutput"
    );

    // The Blob detail is additive beside the graph-commit write-precondition
    // detail introduced by #470; neither slice may erase the other's schema.
    let precondition =
        &doc["components"]["schemas"]["ErrorOutput"]["properties"]["precondition_failure"];
    let precondition_ref = precondition["oneOf"]
        .as_array()
        .and_then(|schemas| schemas.iter().find_map(|schema| schema["$ref"].as_str()))
        .expect("precondition_failure must reference its structured details");
    assert_eq!(
        precondition_ref,
        "#/components/schemas/PreconditionFailureOutput"
    );
}

#[test]
fn merge_conflict_kind_output_schema_has_expected_variants() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["MergeConflictKindOutput"];
    let variants = schema["enum"].as_array().unwrap();
    let values: HashSet<&str> = variants.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(values.contains("divergent_insert"));
    assert!(values.contains("divergent_update"));
    assert!(values.contains("delete_vs_update"));
    assert!(values.contains("orphan_edge"));
    assert!(values.contains("unique_violation"));
    assert!(values.contains("cardinality_violation"));
    assert!(values.contains("value_constraint_violation"));
}

// ---------------------------------------------------------------------------
// Security scheme tests
// ---------------------------------------------------------------------------

#[test]
fn openapi_defines_bearer_token_security_scheme() {
    let doc = openapi_json();
    let scheme = &doc["components"]["securitySchemes"]["bearer_token"];
    assert_eq!(scheme["type"].as_str().unwrap(), "http");
    assert_eq!(scheme["scheme"].as_str().unwrap(), "bearer");
}

#[test]
fn protected_endpoints_reference_bearer_token_security() {
    let doc = openapi_json();
    let protected_paths = [
        ("/graphs/{graph_id}/read", "post"),
        ("/graphs/{graph_id}/blob", "get"),
        ("/graphs/{graph_id}/blob", "head"),
        ("/graphs/{graph_id}/change", "post"),
        ("/graphs/{graph_id}/schema/apply", "post"),
        ("/graphs/{graph_id}/queries", "get"),
        ("/graphs/{graph_id}/queries/{name}", "post"),
        ("/graphs/{graph_id}/mutate/if-graph-commit", "post"),
        ("/graphs/{graph_id}/queries/{name}/if-graph-commit", "post"),
        ("/graphs/{graph_id}/load", "post"),
        ("/graphs/{graph_id}/load/ndjson", "post"),
        ("/graphs/{graph_id}/ingest", "post"),
        ("/graphs/{graph_id}/export", "post"),
        ("/graphs/{graph_id}/snapshot", "get"),
        ("/graphs/{graph_id}/branches", "get"),
        ("/graphs/{graph_id}/branches", "post"),
        ("/graphs/{graph_id}/branches/{branch}", "delete"),
        ("/graphs/{graph_id}/branches/merge", "post"),
        ("/graphs/{graph_id}/commits", "get"),
        ("/graphs/{graph_id}/commits/{commit_id}", "get"),
        ("/graphs/{graph_id}/commits/{commit_id}/changes", "get"),
        ("/graphs/{graph_id}/changes", "get"),
        ("/graphs/{graph_id}/changes/baseline", "post"),
    ];

    for (path, method) in protected_paths {
        let operation = &doc["paths"][path][method];
        let security = operation["security"]
            .as_array()
            .unwrap_or_else(|| panic!("no security on {method} {path}"));
        let has_bearer = security
            .iter()
            .any(|s| s.as_object().unwrap().contains_key("bearer_token"));
        assert!(has_bearer, "{method} {path} missing bearer_token security");
    }
}

#[test]
fn healthz_does_not_require_security() {
    let doc = openapi_json();
    let healthz = &doc["paths"]["/healthz"]["get"];
    assert!(
        healthz.get("security").is_none() || healthz["security"].is_null(),
        "/healthz should not have security requirements"
    );
}

// ---------------------------------------------------------------------------
// Path parameter tests
// ---------------------------------------------------------------------------

#[test]
fn branch_delete_has_branch_path_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/graphs/{graph_id}/branches/{branch}"]["delete"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params
        .iter()
        .any(|p| p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("path"));
    assert!(
        has_branch,
        "DELETE /branches/{{branch}} must have 'branch' path parameter"
    );
}

#[test]
fn commit_show_has_commit_id_path_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/graphs/{graph_id}/commits/{commit_id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_commit_id = params
        .iter()
        .any(|p| p["name"].as_str() == Some("commit_id") && p["in"].as_str() == Some("path"));
    assert!(
        has_commit_id,
        "GET /commits/{{commit_id}} must have 'commit_id' path parameter"
    );
}

#[test]
fn snapshot_has_branch_query_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/graphs/{graph_id}/snapshot"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params
        .iter()
        .any(|p| p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("query"));
    assert!(
        has_branch,
        "GET /snapshot must have 'branch' query parameter"
    );
}

#[test]
fn commits_has_branch_query_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/graphs/{graph_id}/commits"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params
        .iter()
        .any(|p| p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("query"));
    assert!(
        has_branch,
        "GET /commits must have 'branch' query parameter"
    );
}

// ---------------------------------------------------------------------------
// Tag tests
// ---------------------------------------------------------------------------

#[test]
fn openapi_operations_have_tags() {
    let doc = openapi_json();
    let paths = doc["paths"].as_object().unwrap();

    for (path, methods) in paths {
        let methods = methods.as_object().unwrap();
        for (method, operation) in methods {
            let tags = operation["tags"].as_array();
            assert!(
                tags.is_some_and(|t| !t.is_empty()),
                "{method} {path} should have at least one tag"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Response schema reference tests
// ---------------------------------------------------------------------------

#[test]
fn read_endpoint_200_references_legacy_read_output_schema() {
    let doc = openapi_json();
    let content = &doc["paths"]["/graphs/{graph_id}/read"]["post"]["responses"]["200"]["content"];
    let schema = &content["application/json"]["schema"];
    let ref_path = schema["$ref"].as_str().unwrap();
    assert!(
        ref_path.contains("LegacyReadOutput"),
        "POST /read 200 should reference LegacyReadOutput, got {ref_path}"
    );
}

#[test]
fn legacy_read_output_schema_cannot_carry_graph_commit_id() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["LegacyReadOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query_name"));
    assert!(props.contains_key("target"));
    assert!(props.contains_key("row_count"));
    assert!(props.contains_key("rows"));
    assert!(!props.contains_key("graph_commit_id"));
}

#[test]
fn change_endpoint_200_references_change_output_schema() {
    let doc = openapi_json();
    let content = &doc["paths"]["/graphs/{graph_id}/change"]["post"]["responses"]["200"]["content"];
    let schema = &content["application/json"]["schema"];
    let ref_path = schema["$ref"].as_str().unwrap();
    assert!(
        ref_path.contains("ChangeOutput"),
        "POST /change 200 should reference ChangeOutput, got {ref_path}"
    );
}

#[test]
fn healthz_200_references_health_output_schema() {
    let doc = openapi_json();
    let content = &doc["paths"]["/healthz"]["get"]["responses"]["200"]["content"];
    let schema = &content["application/json"]["schema"];
    let ref_path = schema["$ref"].as_str().unwrap();
    assert!(
        ref_path.contains("HealthOutput"),
        "GET /healthz 200 should reference HealthOutput, got {ref_path}"
    );
}

#[test]
fn error_responses_reference_error_output_schema() {
    let doc = openapi_json();
    let paths_with_errors = [
        ("/graphs/{graph_id}/read", "post", "400"),
        ("/graphs/{graph_id}/read", "post", "401"),
        ("/graphs/{graph_id}/change", "post", "400"),
        ("/graphs/{graph_id}/change", "post", "409"),
        ("/graphs/{graph_id}/branches", "post", "409"),
    ];

    for (path, method, status) in paths_with_errors {
        let content = &doc["paths"][path][method]["responses"][status]["content"];
        let schema = &content["application/json"]["schema"];
        let ref_path = schema["$ref"].as_str().unwrap();
        assert!(
            ref_path.contains("ErrorOutput"),
            "{method} {path} {status} should reference ErrorOutput, got {ref_path}"
        );
    }
}

#[test]
fn recovery_barrier_write_endpoints_document_recovery_required() {
    let doc = openapi_json();
    for (path, method) in [
        ("/graphs/{graph_id}/change", "post"),
        ("/graphs/{graph_id}/mutate", "post"),
        ("/graphs/{graph_id}/mutate/if-graph-commit", "post"),
        ("/graphs/{graph_id}/queries/{name}", "post"),
        ("/graphs/{graph_id}/queries/{name}/if-graph-commit", "post"),
        ("/graphs/{graph_id}/load", "post"),
        ("/graphs/{graph_id}/load/ndjson", "post"),
        ("/graphs/{graph_id}/ingest", "post"),
        ("/graphs/{graph_id}/branches", "post"),
        ("/graphs/{graph_id}/branches/{branch}", "delete"),
        ("/graphs/{graph_id}/branches/merge", "post"),
    ] {
        let response = &doc["paths"][path][method]["responses"]["503"];
        assert!(
            response.is_object(),
            "{method} {path} must document the recovery-required 503 outcome"
        );
        assert_eq!(
            response["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorOutput",
            "{method} {path} 503 must use ErrorOutput"
        );
    }
}

#[test]
fn bounded_keyed_write_endpoints_document_resource_limit() {
    let doc = openapi_json();
    for (path, method) in [
        ("/graphs/{graph_id}/change", "post"),
        ("/graphs/{graph_id}/mutate", "post"),
        ("/graphs/{graph_id}/mutate/if-graph-commit", "post"),
        ("/graphs/{graph_id}/queries/{name}", "post"),
        ("/graphs/{graph_id}/queries/{name}/if-graph-commit", "post"),
        ("/graphs/{graph_id}/load", "post"),
        ("/graphs/{graph_id}/load/ndjson", "post"),
        ("/graphs/{graph_id}/ingest", "post"),
        ("/graphs/{graph_id}/branches/merge", "post"),
    ] {
        let response = &doc["paths"][path][method]["responses"]["413"];
        assert!(
            response.is_object(),
            "{method} {path} must document the keyed-write resource-limit 413 outcome"
        );
        assert_eq!(
            response["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorOutput",
            "{method} {path} 413 must use ErrorOutput"
        );
    }
}

// ---------------------------------------------------------------------------
// Request body reference tests
// ---------------------------------------------------------------------------

#[test]
fn post_endpoints_have_request_body() {
    let doc = openapi_json();
    let post_paths = [
        ("/graphs/{graph_id}/read", "ReadRequest"),
        ("/graphs/{graph_id}/change", "ChangeRequest"),
        ("/graphs/{graph_id}/schema/apply", "SchemaApplyRequest"),
        ("/graphs/{graph_id}/ingest", "IngestRequest"),
        ("/graphs/{graph_id}/export", "ExportRequest"),
        ("/graphs/{graph_id}/branches", "BranchCreateRequest"),
        ("/graphs/{graph_id}/branches/merge", "BranchMergeRequest"),
    ];

    for (path, expected_schema) in post_paths {
        let request_body = &doc["paths"][path]["post"]["requestBody"];
        assert!(
            request_body.is_object(),
            "POST {path} should have a requestBody"
        );
        let schema = &request_body["content"]["application/json"]["schema"];
        let ref_path = schema["$ref"].as_str().unwrap();
        assert!(
            ref_path.contains(expected_schema),
            "POST {path} requestBody should reference {expected_schema}, got {ref_path}"
        );
    }
}

#[test]
fn invoke_stored_query_request_body_is_optional() {
    let doc = openapi_json();
    let request_body = &doc["paths"]["/graphs/{graph_id}/queries/{name}"]["post"]["requestBody"];
    assert!(
        request_body.is_object(),
        "POST /queries/{{name}} should document its optional request body"
    );
    assert!(
        !request_body["required"].as_bool().unwrap_or(false),
        "stored-query invocation body should be optional"
    );
    let schema = &request_body["content"]["application/json"]["schema"];
    let ref_path = schema["$ref"]
        .as_str()
        .or_else(|| {
            schema["oneOf"]
                .as_array()
                .and_then(|schemas| schemas.iter().find_map(|schema| schema["$ref"].as_str()))
        })
        .unwrap();
    assert!(
        ref_path.contains("InvokeStoredQueryRequest"),
        "POST /queries/{{name}} requestBody should reference InvokeStoredQueryRequest, got {ref_path}"
    );
}

// ---------------------------------------------------------------------------
// Serialization round-trip test
// ---------------------------------------------------------------------------

#[test]
fn openapi_spec_round_trips_through_json() {
    let doc = openapi_doc();
    let json_string = serde_json::to_string_pretty(&doc).unwrap();
    let parsed: Value = serde_json::from_str(&json_string).unwrap();
    assert!(parsed["openapi"].is_string());
    assert!(parsed["paths"].is_object());
    assert!(parsed["components"]["schemas"].is_object());
}

// ---------------------------------------------------------------------------
// Open-mode vs auth-mode: served spec reflects runtime config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_mode_spec_has_no_security_schemes() {
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let schemes = &json["components"]["securitySchemes"];
    assert!(
        schemes.is_null() || schemes.as_object().is_some_and(|m| m.is_empty()),
        "open-mode spec should have no security schemes"
    );
}

#[tokio::test]
async fn open_mode_spec_has_no_operation_security() {
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();
    for (path, methods) in paths {
        for (method, operation) in methods.as_object().unwrap() {
            let security = &operation["security"];
            assert!(
                security.is_null(),
                "open-mode: {method} {path} should have no security requirement"
            );
        }
    }
}

#[tokio::test]
async fn auth_mode_spec_includes_bearer_token_security_scheme() {
    let (_temp, app) = app_for_loaded_graph_with_auth("secret").await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let scheme = &json["components"]["securitySchemes"]["bearer_token"];
    assert_eq!(scheme["type"].as_str().unwrap(), "http");
    assert_eq!(scheme["scheme"].as_str().unwrap(), "bearer");
}

#[tokio::test]
async fn auth_mode_spec_has_security_on_protected_operations() {
    let (_temp, app) = app_for_loaded_graph_with_auth("secret").await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    // RFC-011 cluster-only: the served spec always nests protected
    // routes under `/graphs/{graph_id}/...`.
    let protected_paths = [
        ("/graphs/{graph_id}/read", "post"),
        ("/graphs/{graph_id}/blob", "get"),
        ("/graphs/{graph_id}/blob", "head"),
        ("/graphs/{graph_id}/change", "post"),
        ("/graphs/{graph_id}/snapshot", "get"),
        ("/graphs/{graph_id}/branches", "get"),
        ("/graphs/{graph_id}/commits", "get"),
    ];
    for (path, method) in protected_paths {
        let security = &json["paths"][path][method]["security"];
        let arr = security
            .as_array()
            .unwrap_or_else(|| panic!("auth-mode: {method} {path} missing security"));
        let has_bearer = arr
            .iter()
            .any(|s| s.as_object().unwrap().contains_key("bearer_token"));
        assert!(
            has_bearer,
            "auth-mode: {method} {path} should require bearer_token"
        );
    }
}

#[tokio::test]
async fn auth_mode_healthz_still_has_no_security() {
    let (_temp, app) = app_for_loaded_graph_with_auth("secret").await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let healthz = &json["paths"]["/healthz"]["get"];
    assert!(
        healthz.get("security").is_none() || healthz["security"].is_null(),
        "auth-mode: /healthz should still have no security"
    );
}

#[test]
fn openapi_spec_is_up_to_date() {
    let spec_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../openapi.json");

    let generated = serde_json::to_string_pretty(&openapi_doc()).unwrap() + "\n";

    if !env::var("OMNIGRAPH_UPDATE_OPENAPI")
        .unwrap_or_default()
        .is_empty()
    {
        fs::write(&spec_path, &generated).unwrap();
        return;
    }

    let committed = fs::read_to_string(&spec_path).unwrap_or_else(|_| {
        panic!(
            "openapi.json not found at {}. Run: OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date",
            spec_path.display()
        )
    });

    assert_eq!(
        committed, generated,
        "openapi.json is out of date. Run: OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date"
    );
}

// ---------------------------------------------------------------------------
// MR-668 — multi-mode OpenAPI cluster filter
// ---------------------------------------------------------------------------
//
// In multi-graph mode, `/openapi.json` reports cluster routes
// (`/graphs/{graph_id}/...`) instead of the legacy flat routes. The
// only flat path that survives is `/healthz`. Operation IDs gain a
// `cluster_` prefix so SDK generators have stable, unique ids.
//
// These tests exercise the request-time `server_openapi` handler via
// `oneshot`, not the static `ApiDoc::openapi()` — the rewrite happens
// only on the served document.

const EXPECTED_CLUSTER_PATHS: &[&str] = &[
    "/graphs/{graph_id}/snapshot",
    "/graphs/{graph_id}/blob",
    "/graphs/{graph_id}/read",
    "/graphs/{graph_id}/export",
    "/graphs/{graph_id}/change",
    "/graphs/{graph_id}/mutate/if-graph-commit",
    "/graphs/{graph_id}/schema",
    "/graphs/{graph_id}/queries/{name}/if-graph-commit",
    "/graphs/{graph_id}/schema/apply",
    "/graphs/{graph_id}/load",
    "/graphs/{graph_id}/load/ndjson",
    "/graphs/{graph_id}/ingest",
    "/graphs/{graph_id}/branches",
    "/graphs/{graph_id}/branches/{branch}",
    "/graphs/{graph_id}/branches/merge",
    "/graphs/{graph_id}/commits",
    "/graphs/{graph_id}/commits/{commit_id}",
    "/graphs/{graph_id}/commits/{commit_id}/changes",
    "/graphs/{graph_id}/changes",
    "/graphs/{graph_id}/changes/baseline",
];

async fn app_for_multi_mode(graph_ids: &[&str]) -> (Vec<tempfile::TempDir>, Router) {
    use std::sync::Arc;

    use omnigraph_server::{GraphHandle, GraphId, GraphKey};

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

#[tokio::test]
async fn multi_mode_openapi_lists_cluster_paths() {
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (status, json) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let paths = json["paths"].as_object().expect("paths must be an object");
    let path_keys: HashSet<&str> = paths.keys().map(|k| k.as_str()).collect();
    for expected in EXPECTED_CLUSTER_PATHS {
        assert!(
            path_keys.contains(expected),
            "missing cluster path in multi-mode spec: {expected}. \
             Found: {path_keys:?}"
        );
    }
}

#[tokio::test]
async fn multi_mode_openapi_drops_flat_protected_paths() {
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();
    // None of the legacy flat protected paths should appear in multi mode.
    let flat_protected = [
        "/snapshot",
        "/blob",
        "/read",
        "/export",
        "/change",
        "/schema",
        "/schema/apply",
        "/load",
        "/load/ndjson",
        "/ingest",
        "/branches",
        "/branches/{branch}",
        "/branches/merge",
        "/commits",
        "/commits/{commit_id}",
    ];
    for flat in flat_protected {
        assert!(
            !paths.contains_key(flat),
            "flat path {flat} must not appear in multi-mode spec; \
             cluster routes are the only protected surface"
        );
    }
}

#[tokio::test]
async fn multi_mode_openapi_keeps_management_paths_flat() {
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();
    for flat in ["/healthz", "/graphs"] {
        assert!(
            paths.contains_key(flat),
            "{flat} must remain flat in multi mode"
        );
        let nested = format!("/graphs/{{graph_id}}{flat}");
        assert!(
            !paths.contains_key(&nested),
            "{flat} must NOT be cluster-prefixed to {nested}"
        );
    }
}

#[tokio::test]
async fn multi_mode_openapi_prefixes_operation_ids_with_cluster() {
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    // Every cluster path operation must have a `cluster_` operation_id.
    // Flat-mounted paths (healthz, management /graphs) keep their
    // original operation_ids — they're not per-graph.
    let paths = json["paths"].as_object().unwrap();
    let mut checked = 0;
    for (path, item) in paths {
        if path == "/healthz" || path == "/graphs" {
            continue;
        }
        for method in ["get", "head", "post", "put", "delete", "patch"] {
            if let Some(op) = item.get(method).filter(|v| v.is_object()) {
                if let Some(id) = op["operationId"].as_str() {
                    assert!(
                        id.starts_with("cluster_"),
                        "operation_id at {path}.{method} must start with `cluster_`, got `{id}`"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(
        checked >= EXPECTED_CLUSTER_PATHS.len(),
        "expected at least {} cluster operation_ids; checked {checked}",
        EXPECTED_CLUSTER_PATHS.len()
    );
}

#[tokio::test]
async fn multi_mode_openapi_declares_graph_id_path_parameter() {
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();

    for expected_path in EXPECTED_CLUSTER_PATHS {
        let item = paths
            .get(*expected_path)
            .unwrap_or_else(|| panic!("missing cluster path {expected_path}"));
        for method in ["get", "head", "post", "put", "delete", "patch"] {
            let Some(operation) = item.get(method).filter(|value| value.is_object()) else {
                continue;
            };
            let parameters = operation["parameters"]
                .as_array()
                .unwrap_or_else(|| panic!("{expected_path}.{method} missing parameters"));
            let graph_id = parameters
                .iter()
                .find(|param| param["name"] == "graph_id" && param["in"] == "path")
                .unwrap_or_else(|| {
                    panic!("{expected_path}.{method} missing graph_id path parameter")
                });
            assert_eq!(
                graph_id["required"].as_bool(),
                Some(true),
                "{expected_path}.{method} graph_id parameter must be required"
            );
            assert_eq!(
                graph_id["schema"]["type"].as_str(),
                Some("string"),
                "{expected_path}.{method} graph_id parameter must be string typed"
            );
        }
    }

    for flat in ["/healthz", "/graphs"] {
        let item = paths.get(flat).unwrap();
        for method in ["get", "head", "post", "put", "delete", "patch"] {
            if let Some(operation) = item.get(method).filter(|value| value.is_object()) {
                let has_graph_id = operation["parameters"]
                    .as_array()
                    .map(|params| {
                        params
                            .iter()
                            .any(|param| param["name"] == "graph_id" && param["in"] == "path")
                    })
                    .unwrap_or(false);
                assert!(
                    !has_graph_id,
                    "{flat}.{method} must not declare graph_id; it remains flat"
                );
            }
        }
    }
}

#[tokio::test]
async fn multi_mode_operation_ids_are_unique() {
    // Sanity check: the cluster_ prefix prevents collision with flat ids
    // (which don't appear in multi mode, but the contract is "unique
    // across the spec"). Verify every operation_id in the multi-mode
    // spec is unique.
    let (_dirs, app) = app_for_multi_mode(&["alpha"]).await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();
    let mut seen_ids: HashSet<String> = HashSet::new();
    for (_, item) in paths {
        for method in ["get", "head", "post", "put", "delete", "patch"] {
            if let Some(op) = item.get(method).filter(|v| v.is_object()) {
                if let Some(id) = op["operationId"].as_str() {
                    assert!(
                        seen_ids.insert(id.to_string()),
                        "duplicate operation_id `{id}` in multi-mode spec"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn served_spec_always_nests_under_cluster_prefix() {
    // RFC-011 cluster-only: even a one-graph convenience app serves the
    // nested cluster surface and never the flat protected routes.
    let (_temp, app) = app_for_loaded_graph().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let paths = json["paths"].as_object().unwrap();
    let path_keys: HashSet<&str> = paths.keys().map(|k| k.as_str()).collect();
    for cluster in EXPECTED_CLUSTER_PATHS {
        assert!(
            path_keys.contains(cluster),
            "served spec must emit cluster path: {cluster}. Found: {path_keys:?}"
        );
    }
    // The flat protected routes must NOT appear — only the nested
    // cluster surface plus the always-flat `/healthz` and `/graphs`.
    let flat_protected = [
        "/snapshot",
        "/blob",
        "/read",
        "/query",
        "/export",
        "/change",
        "/mutate",
        "/mutate/if-graph-commit",
        "/queries",
        "/queries/{name}",
        "/queries/{name}/if-graph-commit",
        "/schema",
        "/schema/apply",
        "/load",
        "/load/ndjson",
        "/ingest",
        "/branches",
        "/branches/{branch}",
        "/branches/merge",
        "/commits",
        "/commits/{commit_id}",
    ];
    for flat in flat_protected {
        assert!(
            !path_keys.contains(flat),
            "served spec must NOT emit flat protected path: {flat}"
        );
    }
}
