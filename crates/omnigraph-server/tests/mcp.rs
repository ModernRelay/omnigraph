//! Black-box tests for the MCP surface (`POST /graphs/{id}/mcp`), driven over
//! `build_app` with in-process tower `oneshot`. Phase 2 covers the read tools,
//! resources, protocol conformance, Cedar-filtered listing, and the server-side
//! Origin fail-closed wiring. (Crate-level transport conformance — 405, the
//! rmcp surface guard — lives in `omnigraph-mcp/tests/standalone.rs`.)

mod support;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use omnigraph_server::queries::{QueryRegistry, RegistrySpec};
use omnigraph_server::{AppState, build_app};
use serde_json::{Value, json};
use support::{
    FIND_PERSON_GQ, INVOKE_POLICY_YAML, app_for_loaded_graph_with_auth_tokens,
    app_for_loaded_graph_with_auth_tokens_and_policy, app_with_stored_queries, g, graph_path,
    init_loaded_graph, json_response,
};

/// Build a JSON-RPC POST to `/graphs/default/mcp`. Sets the `Accept` (both
/// JSON + SSE, as rmcp requires) and `Host` (loopback policy allows it) headers,
/// and an optional bearer token.
fn mcp_request(token: Option<&str>, body: Value) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(g("/mcp"))
        .method(Method::POST)
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn rpc(id: i64, method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

#[tokio::test]
async fn initialize_advertises_tools_and_resources() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                1,
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "0" }
                }),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["result"]["serverInfo"]["name"], "omnigraph");
    assert!(v["result"]["capabilities"]["tools"].is_object());
    assert!(v["result"]["capabilities"]["resources"].is_object());
}

fn tool_names(list_result: &Value) -> Vec<String> {
    list_result["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn tools_list_returns_builtins_with_no_cursor() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) =
        json_response(&app, mcp_request(Some("tok"), rpc(2, "tools/list", json!({})))).await;
    assert_eq!(status, StatusCode::OK);
    let names = tool_names(&v);
    for expected in [
        "graph_health",
        "graph_query",
        "graph_snapshot",
        "schema_get",
        "branch_list",
        "commit_list",
        "commit_get",
    ] {
        assert!(names.contains(&expected.to_string()), "missing tool {expected} in {names:?}");
    }
    // Non-paginated by contract.
    assert!(v["result"]["nextCursor"].is_null());
}

#[tokio::test]
async fn graph_health_returns_ok() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(Some("tok"), rpc(3, "tools/call", json!({ "name": "graph_health", "arguments": {} }))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(v["result"]["isError"], json!(true));
    let text = v["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("\"status\":\"ok\""), "health payload: {text}");
}

#[tokio::test]
async fn graph_query_runs_a_read() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                4,
                "tools/call",
                json!({
                    "name": "graph_query",
                    "arguments": { "query": "query all() { match { $p: Person } return { $p.name } }" }
                }),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(v["result"]["isError"], json!(true), "unexpected isError: {v}");
    // ReadOutput carries a row_count; the text mirror is the serialized DTO.
    let text = v["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("row_count"), "read output: {text}");
}

#[tokio::test]
async fn malformed_query_is_iserror_not_protocol_error() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                5,
                "tools/call",
                json!({ "name": "graph_query", "arguments": { "query": "this is not gq" } }),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A bad query is a semantic (4xx) failure → isError tool result, not a
    // JSON-RPC protocol error (SEP-1303).
    assert_eq!(v["result"]["isError"], json!(true), "expected isError, got {v}");
    assert!(v["error"].is_null());
}

#[tokio::test]
async fn unknown_tool_is_invalid_params() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(Some("tok"), rpc(6, "tools/call", json!({ "name": "no_such_tool", "arguments": {} }))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["error"]["code"], json!(-32602));
}

const READER_ONLY_POLICY: &str = r#"
version: 1
groups:
  readers: [act-reader]
protected_branches: [main]
rules:
  - id: readers-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#;

#[tokio::test]
async fn cedar_filters_listing_and_gates_calls() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-reader", "tok-r"), ("act-none", "tok-n")],
        READER_ONLY_POLICY,
    )
    .await;

    // The reader sees the Read-gated tools.
    let (_s, reader) =
        json_response(&app, mcp_request(Some("tok-r"), rpc(1, "tools/list", json!({})))).await;
    let reader_names = tool_names(&reader);
    assert!(reader_names.contains(&"graph_query".to_string()));
    assert!(reader_names.contains(&"schema_get".to_string()));

    // act-none has no rules → Read denied → only the ungated graph_health shows.
    let (_s, none) =
        json_response(&app, mcp_request(Some("tok-n"), rpc(2, "tools/list", json!({})))).await;
    let none_names = tool_names(&none);
    assert_eq!(none_names, vec!["graph_health".to_string()], "denied actor saw {none_names:?}");

    // And a denied call surfaces isError (the read gate inside the delegate).
    let (status, v) = json_response(
        &app,
        mcp_request(Some("tok-n"), rpc(3, "tools/call", json!({ "name": "schema_get", "arguments": {} }))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["result"]["isError"], json!(true), "expected denied schema_get to isError: {v}");
}

#[tokio::test]
async fn resource_read_returns_schema() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(1, "resources/read", json!({ "uri": "omnigraph://schema" })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = v["result"]["contents"][0]["text"].as_str().unwrap();
    assert!(text.contains("node Person"), "schema resource: {text}");
}

/// Server-side wiring of the fail-closed Origin policy: a non-loopback bind
/// yields `DenyBrowsers`, so a present `Origin` is `403` while an absent one
/// passes. (The policy logic itself is unit-tested in omnigraph-mcp.)
async fn app_with_public_bind() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let state = AppState::open(graph.to_string_lossy().to_string())
        .await
        .unwrap()
        .with_mcp_host_inputs("203.0.113.1:8080".parse().unwrap(), Vec::new(), Vec::new());
    (temp, build_app(state))
}

#[tokio::test]
async fn public_bind_rejects_present_origin() {
    let (_t, app) = app_with_public_bind().await;
    let init = rpc(
        1,
        "initialize",
        json!({ "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": { "name": "t", "version": "0" } }),
    );

    // Present, forged Origin → 403 (origin_guard).
    let mut with_origin = mcp_request(None, init.clone());
    with_origin
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    // A non-loopback bind also disables Host-allowlisting (allowed_hosts None),
    // so the Host header is irrelevant here.
    let resp = {
        use tower::ServiceExt;
        app.clone().oneshot(with_origin).await.unwrap()
    };
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Absent Origin → request proceeds (200).
    let (status, _v) = json_response(&app, mcp_request(None, init)).await;
    assert_eq!(status, StatusCode::OK);
}

// ===== Phase 3: write tools, stored queries, structured output =====

#[tokio::test]
async fn graph_query_emits_structured_content() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (_s, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                1,
                "tools/call",
                json!({
                    "name": "graph_query",
                    "arguments": { "query": "query all() { match { $p: Person } return { $p.name } }" }
                }),
            ),
        ),
    )
    .await;
    // Structured output: structuredContent present (never null) + text mirror.
    assert!(v["result"]["structuredContent"].is_object(), "no structuredContent: {v}");
    assert!(v["result"]["structuredContent"]["row_count"].is_number());
    assert!(v["result"]["content"][0]["text"].is_string());
}

#[tokio::test]
async fn graph_mutate_writes_end_to_end() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                1,
                "tools/call",
                json!({
                    "name": "graph_mutate",
                    "arguments": {
                        "query": "query ins($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
                        "params": { "name": "McpWrite", "age": 41 },
                        "branch": "main"
                    }
                }),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(v["result"]["isError"], json!(true), "mutate failed: {v}");
    assert!(
        v["result"]["structuredContent"]["affected_nodes"].as_u64().unwrap_or(0) >= 1,
        "expected an inserted node: {v}"
    );
}

#[tokio::test]
async fn graph_load_missing_branch_then_fork() {
    let (_t, app) = app_for_loaded_graph_with_auth_tokens(&[("act", "tok")]).await;
    let line = r#"{"type":"Person","data":{"name":"McpLoaded","age":7}}"#;

    // Missing branch + no `from` → 404 → isError (never an implicit fork).
    let (_s, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(1, "tools/call", json!({ "name": "graph_load", "arguments": { "data": line, "branch": "nope" } })),
        ),
    )
    .await;
    assert_eq!(v["result"]["isError"], json!(true), "expected 404 isError: {v}");

    // With `from` → forks the branch and loads.
    let (_s, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                2,
                "tools/call",
                json!({ "name": "graph_load", "arguments": { "data": line, "branch": "feature", "from": "main" } }),
            ),
        ),
    )
    .await;
    assert_ne!(v["result"]["isError"], json!(true), "fork-and-load failed: {v}");
}

#[tokio::test]
async fn stored_query_projects_as_a_tool_and_runs() {
    // 1 exposed query → per_query mode → it appears as its own tool.
    let (_t, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-invoke", "tok")],
        INVOKE_POLICY_YAML,
    )
    .await;

    let (_s, list) =
        json_response(&app, mcp_request(Some("tok"), rpc(1, "tools/list", json!({})))).await;
    assert!(
        tool_names(&list).contains(&"find_person".to_string()),
        "stored query not projected: {:?}",
        tool_names(&list)
    );

    // And it runs (params nested under `params`).
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(
                2,
                "tools/call",
                json!({ "name": "find_person", "arguments": { "params": { "name": "Nobody" } } }),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(v["result"]["isError"], json!(true), "stored query failed: {v}");
    assert!(v["result"]["structuredContent"]["row_count"].is_number());
}

#[tokio::test]
async fn stored_query_invoke_denied_masks_as_unknown_tool() {
    // act-noinvoke has `read` but not `invoke_query` → the outer gate denies and
    // the stored tool masks byte-identically to an unknown tool.
    let (_t, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-noinvoke", "tok")],
        INVOKE_POLICY_YAML,
    )
    .await;
    let (_s, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(1, "tools/call", json!({ "name": "find_person", "arguments": { "params": {} } })),
        ),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602));
    assert_eq!(
        v["error"]["message"].as_str().unwrap(),
        "unknown tool: find_person",
        "denied stored query must mask as unknown"
    );
}

#[tokio::test]
async fn large_catalog_uses_meta_projection() {
    // At/above the auto threshold (24 exposed queries) the projection collapses
    // to the discovery + execute meta pair instead of N typed tools.
    let sources: Vec<(String, String)> = (0..25)
        .map(|i| {
            let name = format!("q{i}");
            let src = format!("query {name}() {{ match {{ $p: Person }} return {{ $p.name }} }}");
            (name, src)
        })
        .collect();
    let specs: Vec<(&str, &str, bool)> = sources
        .iter()
        .map(|(n, s)| (n.as_str(), s.as_str(), true))
        .collect();
    let (_t, app) =
        app_with_stored_queries(&specs, &[("act-invoke", "tok")], INVOKE_POLICY_YAML).await;

    let (_s, list) =
        json_response(&app, mcp_request(Some("tok"), rpc(1, "tools/list", json!({})))).await;
    let names = tool_names(&list);
    assert!(names.contains(&"stored_query_list".to_string()), "{names:?}");
    assert!(names.contains(&"stored_query_run".to_string()), "{names:?}");
    assert!(!names.contains(&"q5".to_string()), "meta mode must not list per-query tools: {names:?}");

    // stored_query_run executes one by name.
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(2, "tools/call", json!({ "name": "stored_query_run", "arguments": { "name": "q5" } })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(v["result"]["isError"], json!(true), "stored_query_run failed: {v}");
}

const PROTECTED_MAIN_WRITE_BRANCHES_POLICY: &str = r#"
version: 1
groups:
  writers: [act-writer]
  readers: [act-reader]
protected_branches: [main]
rules:
  - id: writers-read
    allow:
      actors: { group: writers }
      actions: [read]
      branch_scope: any
  - id: writers-change-unprotected
    allow:
      actors: { group: writers }
      actions: [change]
      branch_scope: unprotected
  - id: readers-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
"#;

#[tokio::test]
async fn write_tool_listed_when_only_unprotected_writes_allowed() {
    // The canonical workflow policy: protected `main`, writable feature branches.
    // `graph_mutate`/`graph_load` must be advertised to an actor who can change
    // unprotected branches — the per-call gate is authoritative and would allow
    // graph_mutate(branch="feature"). Listing probes the action capability on
    // *any* branch, not a fabricated `main` (which is protected → denied). A
    // read-only actor must still NOT see the write tools.
    let (_t, app) = app_for_loaded_graph_with_auth_tokens_and_policy(
        &[("act-writer", "tok-w"), ("act-reader", "tok-r")],
        PROTECTED_MAIN_WRITE_BRANCHES_POLICY,
    )
    .await;

    let (_s, w) =
        json_response(&app, mcp_request(Some("tok-w"), rpc(1, "tools/list", json!({})))).await;
    let w_names = tool_names(&w);
    assert!(
        w_names.contains(&"graph_mutate".to_string()),
        "graph_mutate hidden from an unprotected-branch writer (under-show): {w_names:?}"
    );
    assert!(w_names.contains(&"graph_load".to_string()), "graph_load hidden: {w_names:?}");

    let (_s, r) =
        json_response(&app, mcp_request(Some("tok-r"), rpc(2, "tools/list", json!({})))).await;
    let r_names = tool_names(&r);
    assert!(
        !r_names.contains(&"graph_mutate".to_string()),
        "graph_mutate shown to a read-only actor (over-show regression): {r_names:?}"
    );
    assert!(
        r_names.contains(&"graph_query".to_string()),
        "reader should still see read tools: {r_names:?}"
    );
}

#[tokio::test]
async fn per_query_mode_does_not_expose_meta_tools() {
    // Below the auto threshold the projection is per-query, so the discovery +
    // execute meta pair was never advertised. It must not be callable either —
    // `call_tool` resolves a stored tool through the same projection `tools/list`
    // renders, so list and call cannot diverge.
    let (_t, app) = app_with_stored_queries(
        &[("find_person", FIND_PERSON_GQ, true)],
        &[("act-invoke", "tok")],
        INVOKE_POLICY_YAML,
    )
    .await;

    for tool in ["stored_query_run", "stored_query_list"] {
        let (status, v) = json_response(
            &app,
            mcp_request(
                Some("tok"),
                rpc(1, "tools/call", json!({ "name": tool, "arguments": { "name": "find_person" } })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["error"]["code"], json!(-32602), "{tool} must be unknown in per_query mode: {v}");
        assert_eq!(
            v["error"]["message"].as_str().unwrap(),
            format!("unknown tool: {tool}"),
            "{tool} must mask as unknown when the projection didn't advertise it"
        );
    }
}

#[tokio::test]
async fn stored_query_run_cannot_reach_unexposed_query() {
    // Meta projection (24 exposed) plus one unexposed `hidden`. `stored_query_run`
    // must not resolve the unexposed query even to a caller that knows its name —
    // the agent surface honors `expose`, like every other stored-query path.
    // (`expose:false` stays HTTP/service-callable; this is the MCP boundary only.)
    let exposed: Vec<(String, String)> = (0..24)
        .map(|i| {
            let name = format!("q{i}");
            let src = format!("query {name}() {{ match {{ $p: Person }} return {{ $p.name }} }}");
            (name, src)
        })
        .collect();
    let hidden_src = "query hidden() { match { $p: Person } return { $p.name } }";
    let mut specs: Vec<(&str, &str, bool)> =
        exposed.iter().map(|(n, s)| (n.as_str(), s.as_str(), true)).collect();
    specs.push(("hidden", hidden_src, false));

    let (_t, app) =
        app_with_stored_queries(&specs, &[("act-invoke", "tok")], INVOKE_POLICY_YAML).await;

    // Confirm the meta projection is in force (so stored_query_run exists), and
    // that the unexposed query is not discoverable via stored_query_list.
    let (_s, list) =
        json_response(&app, mcp_request(Some("tok"), rpc(1, "tools/list", json!({})))).await;
    assert!(tool_names(&list).contains(&"stored_query_run".to_string()), "{:?}", tool_names(&list));
    let (_s, listed) = json_response(
        &app,
        mcp_request(Some("tok"), rpc(2, "tools/call", json!({ "name": "stored_query_list", "arguments": {} }))),
    )
    .await;
    let catalog = listed["result"]["structuredContent"]["queries"].as_array().unwrap();
    assert!(
        catalog.iter().all(|q| q["name"] != json!("hidden")),
        "unexposed query leaked into stored_query_list: {listed}"
    );

    // Running the unexposed query by name → not found (isError), never executed.
    let (status, v) = json_response(
        &app,
        mcp_request(
            Some("tok"),
            rpc(3, "tools/call", json!({ "name": "stored_query_run", "arguments": { "name": "hidden" } })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["result"]["isError"], json!(true), "unexposed query must not run via stored_query_run: {v}");
}

#[test]
fn stored_query_shadowing_a_builtin_is_a_load_error() {
    // A stored query whose tool name collides with a built-in must fail loudly
    // at registry load, never be silently un-served.
    let result = QueryRegistry::from_specs(vec![RegistrySpec {
        name: "graph_query".to_string(),
        source: "query graph_query() { match { $p: Person } return { $p.name } }".to_string(),
        expose: true,
        tool_name: None,
    }]);
    let errors = result.expect_err("expected a collision error");
    assert!(
        errors.iter().any(|e| e.message.contains("reserved by a built-in")),
        "expected built-in reservation error, got {errors:?}"
    );
}
