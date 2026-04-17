use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_server::{ApiDoc, AppState, build_app};
use serde_json::Value;
use tower::ServiceExt;
use utoipa::OpenApi;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../omnigraph/tests/fixtures")
        .join(name)
}

fn repo_path(root: &Path) -> PathBuf {
    root.join("openapi_test.omni")
}

async fn init_loaded_repo() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo_path(temp.path());
    fs::create_dir_all(&repo).unwrap();
    let schema = fs::read_to_string(fixture("test.pg")).unwrap();
    let data = fs::read_to_string(fixture("test.jsonl")).unwrap();
    Omnigraph::init(repo.to_str().unwrap(), &schema)
        .await
        .unwrap();
    let mut db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    load_jsonl(&mut db, &data, LoadMode::Overwrite)
        .await
        .unwrap();
    temp
}

async fn app_for_loaded_repo() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let state = AppState::open(repo.to_string_lossy().to_string())
        .await
        .unwrap();
    let app = build_app(state);
    (temp, app)
}

async fn app_for_loaded_repo_with_auth(token: &str) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_token(
        repo.to_string_lossy().to_string(),
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
    ApiDoc::openapi()
}

fn openapi_json() -> Value {
    serde_json::to_value(openapi_doc()).unwrap()
}

// ---------------------------------------------------------------------------
// Endpoint integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn openapi_endpoint_returns_200_with_valid_json() {
    let (_temp, app) = app_for_loaded_repo().await;
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
    let (_temp, app) = app_for_loaded_repo().await;
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
    let temp = init_loaded_repo().await;
    let repo = repo_path(temp.path());
    let db = Omnigraph::open(repo.to_str().unwrap()).await.unwrap();
    let state = AppState::new_with_bearer_token(
        repo.to_string_lossy().to_string(),
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
    assert_eq!(status, StatusCode::OK, "/openapi.json should not require auth");
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

const EXPECTED_PATHS: &[&str] = &[
    "/healthz",
    "/snapshot",
    "/read",
    "/export",
    "/change",
    "/schema/apply",
    "/ingest",
    "/branches",
    "/branches/{branch}",
    "/branches/merge",
    "/runs",
    "/runs/{run_id}",
    "/runs/{run_id}/publish",
    "/runs/{run_id}/abort",
    "/commits",
    "/commits/{commit_id}",
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
    assert!(doc["paths"]["/read"]["post"].is_object());
}

#[test]
fn openapi_export_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/export"]["post"].is_object());
}

#[test]
fn openapi_change_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/change"]["post"].is_object());
}

#[test]
fn openapi_ingest_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/ingest"]["post"].is_object());
}

#[test]
fn openapi_branches_supports_get_and_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/branches"]["get"].is_object());
    assert!(doc["paths"]["/branches"]["post"].is_object());
}

#[test]
fn openapi_branch_delete_is_delete() {
    let doc = openapi_json();
    assert!(doc["paths"]["/branches/{branch}"]["delete"].is_object());
}

#[test]
fn openapi_branch_merge_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/branches/merge"]["post"].is_object());
}

#[test]
fn openapi_runs_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/runs"]["get"].is_object());
}

#[test]
fn openapi_run_show_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/runs/{run_id}"]["get"].is_object());
}

#[test]
fn openapi_run_publish_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/runs/{run_id}/publish"]["post"].is_object());
}

#[test]
fn openapi_run_abort_is_post() {
    let doc = openapi_json();
    assert!(doc["paths"]["/runs/{run_id}/abort"]["post"].is_object());
}

#[test]
fn openapi_commits_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/commits"]["get"].is_object());
}

#[test]
fn openapi_commit_show_is_get() {
    let doc = openapi_json();
    assert!(doc["paths"]["/commits/{commit_id}"]["get"].is_object());
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
    "ChangeOutput",
    "ChangeRequest",
    "CommitListOutput",
    "CommitOutput",
    "ErrorCode",
    "ErrorOutput",
    "ExportRequest",
    "HealthOutput",
    "IngestOutput",
    "IngestRequest",
    "IngestTableOutput",
    "LoadMode",
    "MergeConflictKindOutput",
    "MergeConflictOutput",
    "ReadOutput",
    "ReadRequest",
    "ReadTargetOutput",
    "SchemaApplyOutput",
    "SchemaApplyRequest",
    "RunListOutput",
    "RunOutput",
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
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["ChangeRequest"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("query_source"));
    assert!(props.contains_key("query_name"));
    assert!(props.contains_key("params"));
    assert!(props.contains_key("branch"));
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
}

#[test]
fn run_output_schema_has_expected_fields() {
    let doc = openapi_json();
    let schema = &doc["components"]["schemas"]["RunOutput"];
    let props = schema["properties"].as_object().unwrap();
    assert!(props.contains_key("run_id"));
    assert!(props.contains_key("target_branch"));
    assert!(props.contains_key("run_branch"));
    assert!(props.contains_key("status"));
    assert!(props.contains_key("created_at"));
    assert!(props.contains_key("updated_at"));
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
    assert!(values.contains("unauthorized"));
    assert!(values.contains("forbidden"));
    assert!(values.contains("bad_request"));
    assert!(values.contains("not_found"));
    assert!(values.contains("conflict"));
    assert!(values.contains("internal"));
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
        ("/read", "post"),
        ("/change", "post"),
        ("/schema/apply", "post"),
        ("/ingest", "post"),
        ("/export", "post"),
        ("/snapshot", "get"),
        ("/branches", "get"),
        ("/branches", "post"),
        ("/branches/{branch}", "delete"),
        ("/branches/merge", "post"),
        ("/runs", "get"),
        ("/runs/{run_id}", "get"),
        ("/runs/{run_id}/publish", "post"),
        ("/runs/{run_id}/abort", "post"),
        ("/commits", "get"),
        ("/commits/{commit_id}", "get"),
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
    let params = doc["paths"]["/branches/{branch}"]["delete"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params.iter().any(|p| {
        p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("path")
    });
    assert!(has_branch, "DELETE /branches/{{branch}} must have 'branch' path parameter");
}

#[test]
fn run_show_has_run_id_path_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/runs/{run_id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_run_id = params.iter().any(|p| {
        p["name"].as_str() == Some("run_id") && p["in"].as_str() == Some("path")
    });
    assert!(has_run_id, "GET /runs/{{run_id}} must have 'run_id' path parameter");
}

#[test]
fn commit_show_has_commit_id_path_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/commits/{commit_id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_commit_id = params.iter().any(|p| {
        p["name"].as_str() == Some("commit_id") && p["in"].as_str() == Some("path")
    });
    assert!(has_commit_id, "GET /commits/{{commit_id}} must have 'commit_id' path parameter");
}

#[test]
fn snapshot_has_branch_query_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/snapshot"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params.iter().any(|p| {
        p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("query")
    });
    assert!(has_branch, "GET /snapshot must have 'branch' query parameter");
}

#[test]
fn commits_has_branch_query_parameter() {
    let doc = openapi_json();
    let params = doc["paths"]["/commits"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let has_branch = params.iter().any(|p| {
        p["name"].as_str() == Some("branch") && p["in"].as_str() == Some("query")
    });
    assert!(has_branch, "GET /commits must have 'branch' query parameter");
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
fn read_endpoint_200_references_read_output_schema() {
    let doc = openapi_json();
    let content = &doc["paths"]["/read"]["post"]["responses"]["200"]["content"];
    let schema = &content["application/json"]["schema"];
    let ref_path = schema["$ref"].as_str().unwrap();
    assert!(
        ref_path.contains("ReadOutput"),
        "POST /read 200 should reference ReadOutput, got {ref_path}"
    );
}

#[test]
fn change_endpoint_200_references_change_output_schema() {
    let doc = openapi_json();
    let content = &doc["paths"]["/change"]["post"]["responses"]["200"]["content"];
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
        ("/read", "post", "400"),
        ("/read", "post", "401"),
        ("/change", "post", "400"),
        ("/change", "post", "409"),
        ("/branches", "post", "409"),
    ];

    for (path, method, status) in paths_with_errors {
        let content =
            &doc["paths"][path][method]["responses"][status]["content"];
        let schema = &content["application/json"]["schema"];
        let ref_path = schema["$ref"].as_str().unwrap();
        assert!(
            ref_path.contains("ErrorOutput"),
            "{method} {path} {status} should reference ErrorOutput, got {ref_path}"
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
        ("/read", "ReadRequest"),
        ("/change", "ChangeRequest"),
        ("/schema/apply", "SchemaApplyRequest"),
        ("/ingest", "IngestRequest"),
        ("/export", "ExportRequest"),
        ("/branches", "BranchCreateRequest"),
        ("/branches/merge", "BranchMergeRequest"),
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
    let (_temp, app) = app_for_loaded_repo().await;
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
    let (_temp, app) = app_for_loaded_repo().await;
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
    let (_temp, app) = app_for_loaded_repo_with_auth("secret").await;
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
    let (_temp, app) = app_for_loaded_repo_with_auth("secret").await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, json) = json_response(&app, request).await;
    let protected_paths = [
        ("/read", "post"),
        ("/change", "post"),
        ("/snapshot", "get"),
        ("/branches", "get"),
        ("/runs", "get"),
        ("/commits", "get"),
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
async fn auth_mode_spec_matches_static_generation() {
    let (_temp, app) = app_for_loaded_repo_with_auth("secret").await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let (_, served) = json_response(&app, request).await;
    let static_doc = openapi_json();
    assert_eq!(
        served, static_doc,
        "auth-mode served spec must match static generation"
    );
}

#[tokio::test]
async fn auth_mode_healthz_still_has_no_security() {
    let (_temp, app) = app_for_loaded_repo_with_auth("secret").await;
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
    let spec_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../openapi.json");

    let generated = serde_json::to_string_pretty(&openapi_doc()).unwrap() + "\n";

    if env::var("OMNIGRAPH_UPDATE_OPENAPI").is_ok() {
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
