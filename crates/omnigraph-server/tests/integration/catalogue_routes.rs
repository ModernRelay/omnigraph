//! The engine catalogue scope over HTTP: `/graphs/og_catalog` serves read
//! queries and the schema listing with no graph, branch or snapshot, under
//! the credential rules of a stored graph with the `graph.og_catalog`
//! policy, and refuses every other route. Rust and not `.gqt`: the served
//! scope opens no store, which no case runs without.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use omnigraph_server::api::{
    ErrorOutput, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER, QueryRequest, ReadOutput,
    SchemaOutput,
};
use omnigraph_server::{AppState, build_app};
use serde_json::Value;

use crate::support::*;

const SCHEMA: &str = "node Person { name: String @key }\n";
const OPERATORS: &str =
    "query operators() { match { $o: @Operator } return { $o.name, $o.inputs } }";

fn catalogue_query(token: Option<&str>, request: &QueryRequest) -> Request<Body> {
    let mut builder = Request::builder()
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .uri("/graphs/og_catalog/query")
        .method(Method::POST)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(request).unwrap()))
        .unwrap()
}

fn operators_request() -> QueryRequest {
    QueryRequest {
        query: OPERATORS.to_string(),
        name: None,
        params: None,
        branch: None,
        snapshot: None,
        settings: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn catalogue_scope_serves_reads_and_refuses_everything_else() {
    let (_temp, app) = app_for_graph_with_auth_tokens_only(SCHEMA, &[("act-a", "token-a")]).await;

    let (status, body) =
        json_response(&app, catalogue_query(Some("token-a"), &operators_request())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let output: ReadOutput = serde_json::from_value(body).unwrap();
    assert!(output.row_count > 0, "{}", output.row_count);
    assert!(output.target.branch.is_none() && output.target.snapshot.is_none());
    assert!(output.graph_commit_id.is_none());
    let rows: Vec<Value> = serde_json::from_str(output.rows.get()).unwrap();
    assert!(rows.iter().any(|row| row["o.name"] == "Scan"));

    for reset in ["reset query_memory_bytes;", "reset all;"] {
        let mut request = operators_request();
        request.settings =
            Some(serde_json::from_value(serde_json::json!({"query_memory_bytes": 1})).unwrap());
        request.query = format!("{reset} {OPERATORS}");
        let (status, body) = json_response(&app, catalogue_query(Some("token-a"), &request)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let reset_output: ReadOutput = serde_json::from_value(body).unwrap();
        assert_eq!(reset_output.row_count, output.row_count);
    }

    let (status, body) =
        json_response(&app, get_request("/graphs/og_catalog/schema", "token-a")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let schema: SchemaOutput = serde_json::from_value(body).unwrap();
    assert!(schema.schema_source.is_empty());
    assert!(
        schema
            .catalogue_source
            .as_deref()
            .is_some_and(|text| text.contains("node @Operator"))
    );

    let mut with_branch = operators_request();
    with_branch.branch = Some("main".to_string());
    let (status, body) = json_response(&app, catalogue_query(Some("token-a"), &with_branch)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    for prefix in ["", "explain "] {
        let mut request = operators_request();
        request.query = format!(
            "{prefix}load csv \"file:///unavailable.csv\" into node:Person mode overwrite header"
        );
        let (status, body) = json_response(&app, catalogue_query(Some("token-a"), &request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let error: ErrorOutput = serde_json::from_value(body).unwrap();
        assert!(
            error.error.contains("serves read query declarations"),
            "{}",
            error.error
        );
    }

    let mut mutation = operators_request();
    mutation.query = "query add() { insert @Operator { name: \"X\" } }".to_string();
    let (status, body) = json_response(&app, catalogue_query(Some("token-a"), &mutation)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = json_response(
        &app,
        Request::builder()
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .uri("/graphs/og_catalog/mutate")
            .method(Method::POST)
            .header("authorization", "Bearer token-a")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    let error: ErrorOutput = serde_json::from_value(body).unwrap();
    assert!(error.error.contains("og_catalog"), "{}", error.error);

    let (status, _) = json_response(&app, catalogue_query(None, &operators_request())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalogue_policy_permits_and_denies_by_actor() {
    let temp = init_graph_with_schema(SCHEMA).await;
    let graph = graph_path(temp.path());
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![
            ("act-a".to_string(), "token-a".to_string()),
            ("act-b".to_string(), "token-b".to_string()),
        ],
        None,
    )
    .await
    .unwrap();
    state.set_catalogue_policy(Some(
        omnigraph_server::PolicyEngine::load_graph_from_source(
            &permit_all_policy_yaml(&["act-a"]),
            "og_catalog",
        )
        .unwrap(),
    ));
    let app = build_app(state);

    let (status, body) =
        json_response(&app, catalogue_query(Some("token-a"), &operators_request())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) =
        json_response(&app, catalogue_query(Some("token-b"), &operators_request())).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, _) = json_response(&app, catalogue_query(None, &operators_request())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
