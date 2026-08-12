//! Stored-query registry boot, /queries listing, and invocation routes.
//! Moved verbatim from tests/server.rs in the modularization.

use axum::body::Body;
use axum::http::StatusCode;
use omnigraph_server::AppState;
use serde_json::json;

mod support;
use support::*;

#[tokio::test]
async fn server_boots_with_a_valid_stored_query_registry() {
    // A stored query that type-checks against the fixture schema
    // (`Person { name, age }`) must let the server boot.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "find_person",
        "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
        false,
    )]);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    assert!(
        state.is_ok(),
        "valid registry should boot: {:?}",
        state.err()
    );
}

#[tokio::test]
async fn server_refuses_boot_on_type_broken_stored_query() {
    // A stored query referencing a type not in the schema (`Widget`)
    // must abort boot, naming the offending query.
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let registry = stored_query_registry(&[(
        "ghost",
        "query ghost() { match { $w: Widget } return { $w.name } }",
        false,
    )]);
    let result = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        None,
        registry,
    )
    .await;
    // `AppState` is not `Debug`, so match rather than `expect_err`.
    let err = match result {
        Ok(_) => panic!("type-broken stored query must refuse boot"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("ghost"),
        "error should name the broken query: {msg}"
    );
    assert!(
        msg.contains("schema check"),
        "error should mention the schema check: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_returns_rows() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "find_person");
    assert_eq!(
        body["row_count"], 1,
        "Alice is in the fixture; body: {body}"
    );
    assert!(body["rows"].is_array(), "read envelope shape; body: {body}");

    // The graph-head precondition is mutation-only. A stored read must reject
    // it instead of silently ignoring a caller's concurrency requirement.
    let request = axum::http::Request::builder()
        .uri(g("/queries/find_person"))
        .method(axum::http::Method::POST)
        .header("content-type", "application/json")
        .header("authorization", "Bearer t-invoke")
        .header("omnigraph-if-graph-commit", "unused-on-reads")
        .body(Body::from(
            serde_json::to_vec(&json!({ "params": { "name": "Alice" } })).unwrap(),
        ))
        .unwrap();
    let (status, body) = json_response(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("applies only to stored mutations"),
        "mutation-only header must not be ignored by a stored read; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_with_mismatched_expected_kind_is_rejected() {
    // RFC-011 D3: the CLI verb asserts the stored query's kind via
    // `expect_mutation`. Invoking a read with `expect_mutation: true`
    // (i.e. `omnigraph mutate <a-read>`) is a 400 naming the right verb.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "expect_mutation": true, "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("'find_person' is a read — use omnigraph query find_person"),
        "expected a kind-mismatch error; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_with_matching_expected_kind_runs() {
    // The matching assertion (`omnigraph query <a-read>`) passes through.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "expect_mutation": false, "params": { "name": "Alice" } }),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "matching kind should run; body: {body}"
    );
    assert_eq!(body["query_name"], "find_person");
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_read_accepts_absent_or_empty_body() {
    let no_param_query = "query list_people() { match { $p: Person } return { $p.name } }";
    let (_temp, app) = app_with_stored_queries(
        &[("list_people", no_param_query, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    let (status, body) = json_response(
        &app,
        invoke_request_bytes("list_people", "t-invoke", Body::empty(), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["query_name"], "list_people");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::empty(),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{}"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let (status, body) = json_response(
        &app,
        invoke_request_bytes(
            "list_people",
            "t-invoke",
            Body::from("{"),
            Some("application/json"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid stored-query invocation body"),
        "malformed JSON should be rejected as bad request; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_mutation_double_gates_on_change() {
    let specs: &[(&str, &str, bool)] = &[(
        "add_person",
        "query add_person($name: String) { insert Person { name: $name } }",
        false,
    )];
    let (_temp, app) = app_with_stored_queries(
        specs,
        &[("act-invoke", "t-invoke"), ("act-full", "t-full")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Has invoke_query but NOT change → the inner change gate denies (403).
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-invoke",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "invoke_query without change must 403; body: {body}"
    );

    // Has invoke_query + change → applied.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-full",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_query_bad_param_is_400() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    // `name` is declared String; pass a number.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invoke",
            json!({ "params": { "name": 123 } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("name"),
        "400 should name the offending param; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_unknown_query_and_denied_actor_return_identical_404() {
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invoke", "t-invoke"), ("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;

    // Authorized actor, unknown query name → 404.
    let (unknown_status, unknown_body) = json_response(
        &app,
        invoke_request("does_not_exist", "t-invoke", json!({})),
    )
    .await;
    // Denied actor (no invoke_query), real query name → 404.
    let (denied_status, denied_body) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-noinvoke",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;

    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(denied_status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown_body, denied_body,
        "deny must be byte-identical to a missing query (no catalog probing)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invoke_query_holder_without_read_sees_403_not_404() {
    // The 404-hiding is for callers WITHOUT invoke_query. An actor that
    // HOLDS invoke_query but lacks `read` clears the boundary gate, then the
    // inner read gate denies → 403 for an EXISTING read query, vs 404 for an
    // unknown one. Existence is visible to grant-holders by design (the
    // documented double-gate); this pins that actual contract.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, false)],
        &[("act-invokeonly", "t-invokeonly")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (exists_status, _) = json_response(
        &app,
        invoke_request(
            "find_person",
            "t-invokeonly",
            json!({ "params": { "name": "Alice" } }),
        ),
    )
    .await;
    let (absent_status, _) = json_response(
        &app,
        invoke_request("does_not_exist", "t-invokeonly", json!({})),
    )
    .await;
    assert_eq!(
        exists_status,
        StatusCode::FORBIDDEN,
        "an existing read query the holder can't read → inner-gate 403"
    );
    assert_eq!(
        absent_status,
        StatusCode::NOT_FOUND,
        "unknown query still 404s"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_returns_only_exposed_with_typed_params() {
    let (_temp, app) = app_with_stored_queries(
        &[
            ("find_person", FIND_PERSON_GQ, true),
            (
                "add_person",
                "query add_person($name: String) { insert Person { name: $name } }",
                true,
            ),
            (
                "hidden",
                "query hidden() { match { $p: Person } return { $p.name } }",
                false,
            ),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-invoke")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let entries = body["queries"].as_array().unwrap();
    let names: Vec<&str> = entries
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"find_person") && names.contains(&"add_person"),
        "exposed queries listed: {names:?}"
    );
    assert!(
        !names.contains(&"hidden"),
        "non-exposed query hidden from the catalog: {names:?}"
    );

    let fp = entries.iter().find(|q| q["name"] == "find_person").unwrap();
    assert_eq!(fp["mutation"], false);
    assert_eq!(fp["tool_name"], "find_person");
    assert_eq!(fp["params"][0]["name"], "name");
    assert_eq!(fp["params"][0]["kind"], "string");
    let ap = entries.iter().find(|q| q["name"] == "add_person").unwrap();
    assert_eq!(ap["mutation"], true, "stored insert → mutation");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_read_gated_so_a_non_invoker_can_list() {
    // The catalog is read-gated (not invoke_query-gated), so a reader who
    // lacks invoke_query still enumerates the exposed queries — the
    // documented probe-oracle gap until per-query Cedar filtering lands.
    let (_temp, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-noinvoke", "t-noinvoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-noinvoke")).await;
    assert_eq!(status, StatusCode::OK, "read-gated catalog; body: {body}");
    let names: Vec<&str> = body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"find_person"),
        "a reader lists the catalog despite lacking invoke_query: {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_surfaces_query_description_and_instruction() {
    // E2e for the query-level `.gq` surface: `@description`/`@instruction` on
    // a stored query declaration are carried through to clients via the typed
    // `QueryCatalogEntry` fields over `GET /queries`. A query without them
    // omits both fields (serde `skip_serializing_if = "Option::is_none"`).
    let described = "query described($name: String) \
        @description(\"Find a person by exact name.\") \
        @instruction(\"Use for exact lookups; prefer search for fuzzy matches.\") \
        { match { $p: Person { name: $name } } return { $p.age } }";
    let (_temp, app) = app_with_stored_queries(
        &[
            ("described", described, true),
            (
                "bare",
                "query bare() { match { $p: Person } return { $p.name } }",
                true,
            ),
        ],
        &[("act-invoke", "t-invoke")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "t-invoke")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let entries = body["queries"].as_array().unwrap();

    let described = entries.iter().find(|q| q["name"] == "described").unwrap();
    assert_eq!(
        described["description"], "Find a person by exact name.",
        "query @description surfaces over GET /queries: {described}"
    );
    assert_eq!(
        described["instruction"], "Use for exact lookups; prefer search for fuzzy matches.",
        "query @instruction surfaces over GET /queries: {described}"
    );

    let bare = entries.iter().find(|q| q["name"] == "bare").unwrap();
    assert!(
        bare.get("description").is_none() && bare.get("instruction").is_none(),
        "a query without the annotations omits both fields: {bare}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_queries_is_empty_when_no_registry() {
    let (_temp, app) = app_for_loaded_graph_with_auth("demo-token").await;
    let (status, body) = json_response(&app, get_request(&g("/queries"), "demo-token")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body["queries"].as_array().unwrap().is_empty(),
        "no stored-query registry → empty catalog"
    );
}

/// GitHub #365: a stored mutation invoked by name honors the same
/// `Omnigraph-If-Graph-Commit` branch-head precondition as `POST /mutate` —
/// this is the CLI's `mutate <name>` path in served deployments, so without it
/// the flag would silently not apply to stored mutations.
#[tokio::test(flavor = "multi_thread")]
async fn invoke_stored_mutation_graph_commit_precondition_issue_365() {
    async fn head_commit_id(app: &axum::Router) -> String {
        let (status, out) =
            json_response(app, get_request(&g("/commits?branch=main"), "t-full")).await;
        assert_eq!(status, StatusCode::OK, "body: {out}");
        out["commits"]
            .as_array()
            .expect("commit list")
            .iter()
            .max_by_key(|commit| commit["manifest_version"].as_u64().unwrap())
            .expect("loaded graph has at least one commit")["graph_commit_id"]
            .as_str()
            .unwrap()
            .to_string()
    }
    fn invoke_with_graph_commit_precondition(
        name: &str,
        body: serde_json::Value,
        expected_commit: &str,
    ) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri(g(&format!("/queries/{name}")))
            .method(axum::http::Method::POST)
            .header("content-type", "application/json")
            .header("authorization", "Bearer t-full")
            .header("omnigraph-if-graph-commit", expected_commit)
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    let specs: &[(&str, &str, bool)] = &[(
        "add_person",
        "query add_person($name: String) { insert Person { name: $name } }",
        false,
    )];
    let (_temp, app) =
        app_with_stored_queries(specs, &[("act-full", "t-full")], INVOKE_POLICY_YAML).await;
    let stale_head = head_commit_id(&app).await;

    // A plain invoke advances the head past the commit the caller read.
    let (status, body) = json_response(
        &app,
        invoke_request(
            "add_person",
            "t-full",
            json!({ "params": { "name": "Eve" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Stale precondition: 412 with structured details, no effect.
    let (status, body) = json_response(
        &app,
        invoke_with_graph_commit_precondition(
            "add_person",
            json!({ "params": { "name": "Zed" } }),
            &stale_head,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "stale graph-commit precondition on a stored mutation must 412; body: {body}"
    );
    assert_eq!(body["precondition_failure"]["expected"], json!(stale_head));

    // Current head passes and commits.
    let current_head = head_commit_id(&app).await;
    let (status, body) = json_response(
        &app,
        invoke_with_graph_commit_precondition(
            "add_person",
            json!({ "params": { "name": "Zed" } }),
            &current_head,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["affected_nodes"], 1, "body: {body}");
}
