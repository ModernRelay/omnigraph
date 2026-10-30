//! MCP is another authenticated read transport over the native policy handlers.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Method, Request, StatusCode};
use futures::StreamExt as _;
use omnigraph_server::AppState;
use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use crate::support::oidc::{RESOURCE, token, trust};
use crate::support::*;

async fn fixture_app() -> (tempfile::TempDir, Router, AppState) {
    fixture_app_with(omnigraph_server::McpSettings::default()).await
}

async fn fixture_app_with(
    settings: omnigraph_server::McpSettings,
) -> (tempfile::TempDir, Router, AppState) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy = temp.path().join("policy.yaml");
    std::fs::write(
        &policy,
        r#"version: 1
groups:
  readers: ["principal:stable_alice"]
  read_only: ["principal:stable_reader"]
  change_only: ["principal:stable_changer"]
rules:
  - id: alice
    allow:
      actors: {group: readers}
      actions: [read, change]
      branch_scope: any
  - id: read_only
    allow:
      actors: {group: read_only}
      actions: [read]
      branch_scope: any
  - id: change_only
    allow:
      actors: {group: change_only}
      actions: [change]
      branch_scope: any
  - id: invoke
    allow:
      actors: {group: readers}
      actions: [invoke_query]
"#,
    )
    .unwrap();
    let queries = stored_query_registry(&[
        (
            "people",
            "query people() { match { $p: Person } return { $p.name } }",
            true,
        ),
        (
            "person",
            "query person($name: String) { match { $p: Person { name: $name } } return { $p.age } }",
            true,
        ),
        (
            "add",
            "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            true,
        ),
    ]);
    let state = AppState::open_single_with_queries(
        graph.to_string_lossy().to_string(),
        vec![],
        Some(&policy),
        queries,
    )
    .await
    .unwrap();
    let handle = state.routing().registry.list().pop().unwrap();
    let mut entries = vec![omnigraph_server::registry::GraphEntry::ready(Arc::clone(
        handle.handle(),
    ))];
    entries.push(omnigraph_server::GraphEntry::Loading(Arc::new(
        omnigraph_server::LoadingGraph {
            key: omnigraph_server::GraphKey::cluster(
                omnigraph_server::GraphId::try_from("loading").unwrap(),
            ),
            uri: temp.path().join("loading").to_string_lossy().into_owned(),
            policy: handle.policy.clone(),
        },
    )));
    for (id, failure) in [
        (
            "blocked",
            omnigraph_server::api::GraphStartupFailure::OpenFailed,
        ),
        (
            "invalid-policy",
            omnigraph_server::api::GraphStartupFailure::InvalidPolicy,
        ),
        (
            "invalid-config",
            omnigraph_server::api::GraphStartupFailure::InvalidConfiguration,
        ),
    ] {
        entries.push(omnigraph_server::registry::GraphEntry::Blocked(Arc::new(
            omnigraph_server::registry::BlockedGraph {
                key: omnigraph_server::GraphKey::cluster(
                    omnigraph_server::GraphId::try_from(id).unwrap(),
                ),
                uri: temp.path().join(id).to_string_lossy().into_owned(),
                policy: if id == "blocked" {
                    handle.policy.clone()
                } else {
                    None
                },
                failure,
            },
        )));
    }
    let state = AppState::new_multi_entries(
        entries,
        vec![],
        None,
        omnigraph_server::workload::WorkloadController::with_defaults(),
        None,
    )
    .unwrap()
    .with_oidc_identity_trust(trust(temp.path()))
    .with_mcp_settings(settings);
    (temp, omnigraph_server::build_app(state.clone()), state)
}

fn rpc(token: Option<&str>, method: &str, params: Value) -> Request<Body> {
    let mut request = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-11-25");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    request
        .body(Body::from(
            json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
        ))
        .unwrap()
}

async fn call(app: &Router, token: &str, tool: &str, arguments: Value) -> Value {
    let response = app
        .clone()
        .oneshot(rpc(
            Some(token),
            "tools/call",
            json!({"name":tool,"arguments":arguments}),
        ))
        .await
        .unwrap();
    let status = response.status();
    assert!(!response.headers().contains_key("mcp-session-id"));
    assert!(!response.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn oidc_mcp_reuses_discovery_cedar_and_stored_read_handlers_without_mutations() {
    let (temp, app, state) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let bob = token("bob", RESOURCE);
    let response=app.clone().oneshot(rpc(Some(&alice),"initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("mcp-session-id"));
    assert!(!response.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    // `mcp_tool_list_describes_every_tool` owns the listing.
    let key = omnigraph_server::GraphKey::cluster(
        omnigraph_server::GraphId::try_from("default").unwrap(),
    );
    let transition = state
        .prepare_same_view(
            &key,
            tokio::time::Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap()
        .close()
        .unwrap();
    let discovered = call(&app, &bob, "graphs", json!({})).await;
    assert_eq!(
        discovered["result"]["structuredContent"],
        json!({"graphs":[{"graph_id":"blocked","display_name":"blocked"},{"graph_id":"default","display_name":"default"},{"graph_id":"invalid-config","display_name":"invalid-config"},{"graph_id":"invalid-policy","display_name":"invalid-policy"},{"graph_id":"loading","display_name":"loading"}]})
    );
    for (credential, graph, expected) in [
        (&alice, "blocked", 503),
        (&bob, "blocked", 404),
        (&alice, "loading", 503),
        (&bob, "loading", 404),
        (&alice, "default", 503),
        (&bob, "default", 404),
        (&alice, "invalid-policy", 404),
        (&alice, "invalid-config", 404),
        (&alice, "unknown", 404),
    ] {
        let result = call(&app, credential, "queries", json!({"graph":graph})).await;
        assert_eq!(result["result"]["isError"], true);
        assert_eq!(
            result["result"]["structuredContent"]["status"], expected,
            "{result}"
        );
        let (status, native) = json_response(
            &app,
            get_request(&format!("/graphs/{graph}/snapshot"), credential),
        )
        .await;
        assert_eq!(status.as_u16(), expected);
        if expected == 503 {
            assert_eq!(native["code"], "graph_unavailable");
            assert_eq!(result["result"]["structuredContent"]["error"], native);
        }
    }
    transition.wait_requests().await.unwrap();
    transition.resume_same_view().unwrap();
    // The whole catalog, stored mutations included, as the route lists it.
    let catalog = call(&app, &alice, "queries", json!({"graph":"default"})).await;
    let queries = catalog["result"]["structuredContent"]["queries"]
        .as_array()
        .unwrap();
    assert_eq!(queries.len(), 3, "{catalog}");
    assert_eq!(
        queries.iter().filter(|q| q["mutation"] == true).count(),
        1,
        "{catalog}"
    );
    assert_eq!(
        catalog["result"]["structuredContent"],
        http_get(&app, "/graphs/default/queries", &alice).await
    );
    let denied = call(&app, &bob, "queries", json!({"graph":"default"})).await;
    assert_eq!(denied["result"]["isError"], true);
    assert_eq!(denied["result"]["structuredContent"]["status"], 403);
    let read = call(
        &app,
        &alice,
        "query",
        json!({"graph":"default","name":"people"}),
    )
    .await;
    assert_eq!(read["result"]["isError"], false, "{read}");
    let (_, native) = json_response(
        &app,
        invoke_request("people", &alice, json!({"expect_mutation":false})),
    )
    .await;
    assert!(native["rows"].is_array());
    assert_eq!(read["result"]["structuredContent"], native);
    let selected = call(
        &app,
        &alice,
        "query",
        json!({"graph":"default","name":"person","params":{"name":"Alice"},"branch":"main"}),
    )
    .await;
    assert_eq!(selected["result"]["isError"], false, "{selected}");
    assert_eq!(selected["result"]["structuredContent"]["row_count"], 1);
    let (_, native_selected) = json_response(
        &app,
        invoke_request(
            "person",
            &alice,
            json!({"params":{"name":"Alice"},"branch":"main","expect_mutation":false}),
        ),
    )
    .await;
    assert_eq!(selected["result"]["structuredContent"], native_selected);
    let denied = call(
        &app,
        &bob,
        "query",
        json!({"graph":"default","name":"people"}),
    )
    .await;
    assert_eq!(denied["result"]["structuredContent"]["status"], 404);
    let before = manifest_dataset_version(&graph_path(temp.path())).await;
    for arguments in [
        json!({"graph":"default","name":"add","params":{"name":"MCP must not write","age":7}}),
        json!({"graph":"default","name":"people","actor":"principal:stable_alice"}),
        json!({"graph":"default","name":"people","expect_mutation":true}),
        json!({"graph":"default","name":"people","query":"insert Person { name: 'x', age: 1 }"}),
    ] {
        let rejected = call(&app, &alice, "query", arguments).await;
        assert!(
            rejected.get("error").is_some() || rejected["result"]["isError"] == true,
            "{rejected}"
        );
    }
    assert_eq!(
        manifest_dataset_version(&graph_path(temp.path())).await,
        before
    );
}

#[tokio::test]
async fn mcp_authenticates_every_request_and_enforces_resource_and_http_bounds() {
    let (_temp, app, _state) = fixture_app().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&polls);
    let (parts, _) = rpc(None, "tools/list", json!({})).into_parts();
    let untrusted_body = Body::from_stream(futures::stream::poll_fn(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(None::<Result<Bytes, std::io::Error>>)
    }));
    let response = app
        .clone()
        .oneshot(Request::from_parts(parts, untrusted_body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "authentication must precede collection"
    );
    assert!(!response.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    assert_eq!(
        response.headers()["www-authenticate"],
        "Bearer resource_metadata=\"https://data.example/.well-known/oauth-protected-resource/clusters/A/incarnations/one\""
    );
    let response = app
        .clone()
        .oneshot(
            Request::get("/.well-known/oauth-protected-resource")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    let metadata: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(metadata["resource"], RESOURCE);
    assert!(!metadata.to_string().contains("stable_alice"));
    // The challenge's URL itself answers the same document, so a client
    // probing the path-suffixed location first needs no proxy rewrite.
    let advertised = "/.well-known/oauth-protected-resource/clusters/A/incarnations/one";
    let response = app
        .clone()
        .oneshot(Request::get(advertised).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(!response.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    let at_path: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(at_path, metadata);
    for other in [
        "/.well-known/oauth-protected-resource/clusters/A",
        "/.well-known/oauth-protected-resource/clusters/A/incarnations/one/",
        "/.well-known/oauth-protected-resource/clusters/A/incarnations/two",
        "/.well-known/oauth-protected-resource/clusters/A/incarnations/one/extra",
        "/.well-known/oauth-protected-resource/mcp",
        "/.well-known/oauth-protected-resource/clusters/%41/incarnations/one",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(other).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{other}");
    }
    let response = app
        .clone()
        .oneshot(Request::get("/no-such-route").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers()[HTTP_API_CONTRACT_HEADER],
        HTTP_API_CONTRACT
    );

    let alice = token("alice", RESOURCE);
    // A valid token for a subject the admission file does not name is refused
    // without a sign-in challenge: a new token for that subject cannot help.
    let response = app
        .clone()
        .oneshot(rpc(
            Some(&token("unknown", RESOURCE)),
            "tools/list",
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(!response.headers().contains_key("www-authenticate"));
    for wrong in [
        token("alice", "https://data.example/clusters/B/incarnations/one"),
        "ogc_retired".into(),
    ] {
        let response = app
            .clone()
            .oneshot(rpc(Some(&wrong), "tools/list", json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // RFC 6750: a presented, refused bearer is `invalid_token`.
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer error=\"invalid_token\", resource_metadata=\"https://data.example/.well-known/oauth-protected-resource/clusters/A/incarnations/one\""
        );
    }
    // Another scheme is not a presented bearer credential.
    let mut basic = rpc(None, "tools/list", json!({}));
    basic
        .headers_mut()
        .insert("authorization", "Basic YWxpY2U6eA==".parse().unwrap());
    let response = app.clone().oneshot(basic).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()["www-authenticate"],
        "Bearer resource_metadata=\"https://data.example/.well-known/oauth-protected-resource/clusters/A/incarnations/one\""
    );
    let mut forged = rpc(None, "tools/list", json!({}));
    forged
        .extensions_mut()
        .insert(omnigraph_server::ResolvedActor::cluster_static(Arc::from(
            "principal:stable_alice",
        )));
    assert_eq!(
        app.clone().oneshot(forged).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut hostile = rpc(Some(&alice), "tools/list", json!({}));
    hostile
        .headers_mut()
        .insert("host", "attacker.example".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(hostile).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let mut cross_origin = rpc(Some(&alice), "tools/list", json!({}));
    cross_origin
        .headers_mut()
        .insert("origin", "https://attacker.example".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(cross_origin).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let oversized = rpc(
        Some(&alice),
        "tools/call",
        json!({"name":"query","arguments":{"graph":"default","name":"people","params":{"large":"x".repeat(64*1024)}}}),
    );
    assert_eq!(
        app.clone().oneshot(oversized).await.unwrap().status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // A prior successful initialize never permits a later unauthenticated call.
    assert_eq!(
        app.clone()
            .oneshot(rpc(Some(&alice), "tools/list", json!({})))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.oneshot(rpc(None, "tools/list", json!({})))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn mcp_shares_response_lifetimes_closed_admission_and_the_status_lane() {
    let (_temp, app, state) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let runtime = state.operation_runtime();
    let response = app
        .clone()
        .oneshot(rpc(
            Some(&alice),
            "tools/call",
            json!({
                "name":"query", "arguments":{"graph":"default","name":"people"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(runtime.snapshot().active_reads, 1);
    // Even after the HTTP body is gone, its yielded bytes retain the observer.
    let mut body = response.into_body().into_data_stream();
    let bytes = body.next().await.unwrap().unwrap();
    assert!(!bytes.is_empty());
    drop(body);
    assert_eq!(runtime.snapshot().active_reads, 1);
    let held = (1..omnigraph_server::operations::DEFAULT_READ_OBSERVERS)
        .map(|_| runtime.try_observe().unwrap())
        .collect::<Vec<_>>();
    let refused = app
        .clone()
        .oneshot(rpc(Some(&alice), "tools/list", json!({})))
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(!refused.headers().contains_key(HTTP_API_CONTRACT_HEADER));
    let (status, ready) =
        json_response(&app, Request::get("/readyz").body(Body::empty()).unwrap()).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "saturated data admission must leave loading readiness observable"
    );
    assert_eq!(ready["ready"], false);
    assert_eq!(ready["status"], "loading");
    assert_eq!(ready["ready_graph_count"], 1);
    assert_eq!(ready["loading_graph_count"], 1);
    drop(held);
    let view = state.routing().registry.list().pop().unwrap();
    let transition = state
        .prepare_same_view(
            &view.key,
            tokio::time::Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap()
        .close()
        .unwrap();
    {
        let wait = transition.wait_requests();
        tokio::pin!(wait);
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "MCP yielded bytes must retain the selected graph after body drop"
        );
    }
    drop(bytes);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            runtime.wait_logical_owners()
        )
        .await
        .unwrap()
    );
    assert_eq!(runtime.snapshot().active_reads, 0);
    transition.wait_requests().await.unwrap();
    assert_ne!(transition.resume_same_view().unwrap(), view.epoch());
    runtime.close();
    let response = app
        .clone()
        .oneshot(rpc(Some(&alice), "tools/list", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(runtime.snapshot().active_reads, 0);
    let (status, ready) =
        json_response(&app, Request::get("/readyz").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(ready["ready"], false);
    assert_eq!(ready["status"], "draining");
    assert_eq!(
        app.oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn mcp_needs_a_credential_source_and_discovery_needs_an_oidc_resource() {
    // An unauthenticated server has no actor for MCP's policy checks.
    let (_temp, app) = app_for_loaded_graph().await;
    for uri in ["/mcp", "/.well-known/oauth-protected-resource"] {
        let request = if uri == "/mcp" {
            rpc(None, "tools/list", json!({}))
        } else {
            Request::get(uri).body(Body::empty()).unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
}

async fn tool_list(app: &Router, token: &str) -> Vec<Value> {
    let (status, list) = json_response(app, rpc(Some(token), "tools/list", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    list["result"]["tools"].as_array().unwrap().clone()
}

async fn instructions(app: &Router, token: &str) -> String {
    let (status, body) = json_response(
        app,
        rpc(
            Some(token),
            "initialize",
            json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["result"]["instructions"].as_str().unwrap().to_owned()
}

/// One line per tool: name, required arguments, read-only hint, whether an
/// output schema is declared. Every tool is listed; policy alone decides what
/// a call may do.
#[tokio::test]
async fn mcp_tool_list_describes_every_tool() {
    let alice = token("alice", RESOURCE);
    let expected = vec![
        "health [] read_only output",
        "graphs [] read_only output",
        "schema [graph] read_only output",
        "queries [graph] read_only output",
        "query [graph,name] read_only",
        "read [graph,query] read_only",
        "snapshot [graph] read_only",
        "access [graph] read_only output",
        "branches_list [graph] read_only output",
        "branches_diff [graph,source] read_only",
        "branches_merge_base [graph,source] read_only",
        "commits_list [graph] read_only",
        "commits_get [graph,commit_id] read_only",
        "commits_changes [graph,commit_id] read_only",
        "diff [graph,from,to] read_only",
        "changes_poll [graph] read_only",
        "blobs_stat [graph,entity,type,id,property] read_only",
        "mutate [graph]",
        "load [graph,mode,data]",
        "blobs_clear [graph,entity,type,id,property]",
        "branches_create [graph,name]",
        "branches_delete [graph,name]",
        "branches_merge [graph,source]",
        "commits_publish [graph,commit_id]",
        "commits_drop [graph,commit_id]",
    ];
    let (_temp, app, _state) = fixture_app().await;
    let tools = tool_list(&app, &alice).await;
    let advertised: std::collections::BTreeSet<String> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect();
    let lines: Vec<String> = tools
        .iter()
        .map(|tool| {
            let required: Vec<&str> = tool["inputSchema"]["required"]
                .as_array()
                .map(|names| names.iter().map(|n| n.as_str().unwrap()).collect())
                .unwrap_or_default();
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert!(tool["title"].as_str().is_some_and(|t| !t.is_empty()));
            assert!(tool["description"].as_str().unwrap().len() > 20);
            assert_eq!(tool["annotations"]["openWorldHint"], false);
            let mut line = format!(
                "{} [{}]{}",
                tool["name"].as_str().unwrap(),
                required.join(","),
                if tool["annotations"]["readOnlyHint"] == true {
                    " read_only"
                } else {
                    ""
                }
            );
            if let Some(output) = tool.get("outputSchema") {
                assert_eq!(output["type"], "object");
                assert_eq!(output["anyOf"].as_array().unwrap().len(), 2);
                if tool["name"] == "access" {
                    let success = output["anyOf"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|schema| schema["properties"].get("graph_id").is_some())
                        .expect("access advertises its successful result");
                    assert!(
                        success["required"]
                            .as_array()
                            .unwrap()
                            .contains(&json!("actor_id"))
                    );
                    assert!(success["properties"].get("actor_id").is_some());
                    assert!(success["properties"].get("actor").is_none());
                }
                line.push_str(" output");
            }
            line
        })
        .collect();
    assert_eq!(lines, expected);
    let text = instructions(&app, &alice).await;
    assert!(text.contains("Call `schema`"), "{text}");
    assert!(text.contains("`read`"), "{text}");
    assert!(text.contains("Writes:"), "{text}");
    // Compare the real tools/list response with the shared inventory,
    // without adding an engine dependency to the MCP crate.
    let inventoried = omnigraph_server::api::operations::HTTP_OPERATIONS
        .iter()
        .flat_map(|operation| operation.mcp.names.iter().map(|name| (*name).to_owned()))
        .collect();
    assert_eq!(advertised, inventoried);
}

#[tokio::test]
async fn mcp_schema_and_adhoc_read_reuse_their_routes_and_policy() {
    let (temp, app, _state) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let bob = token("bob", RESOURCE);

    // `schema` answers exactly the route's body.
    let schema = call(&app, &alice, "schema", json!({"graph":"default"})).await;
    assert_eq!(schema["result"]["isError"], false, "{schema}");
    let (status, native) = json_response(&app, get_request("/graphs/default/schema", &alice)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(schema["result"]["structuredContent"], native);
    assert!(
        native["schema_source"]
            .as_str()
            .unwrap()
            .contains("node Person")
    );
    // `queries` carries each stored read's typed parameters.
    let catalog = call(&app, &alice, "queries", json!({"graph":"default"})).await;
    let person = catalog["result"]["structuredContent"]["queries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|query| query["name"] == "person")
        .unwrap()
        .clone();
    assert_eq!(
        person["params"],
        json!([{"name":"name","kind":"string","nullable":false}])
    );

    // `read` answers exactly `POST /query`'s body, including branch reads.
    let source = "query adults($min: I32) { match { $p: Person $p.age >= $min } return { $p.name } order { $p.name asc } }";
    for arguments in [
        json!({"graph":"default","query":source,"params":{"min":30}}),
        json!({"graph":"default","query":source,"params":{"min":30},"branch":"main"}),
        json!({"graph":"default","query":"branch list"}),
    ] {
        let read = call(&app, &alice, "read", arguments.clone()).await;
        assert_eq!(read["result"]["isError"], false, "{read}");
        let mut body = arguments.clone();
        body.as_object_mut().unwrap().remove("graph");
        let request = Request::post("/graphs/default/query")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {alice}"))
            .body(Body::from(body.to_string()))
            .unwrap();
        let (status, native) = json_response(&app, request).await;
        assert_eq!(status, StatusCode::OK, "{native}");
        assert_eq!(read["result"]["structuredContent"], native);
    }

    // A denied actor gets the route's refusal in the shared envelope.
    for (tool, arguments) in [
        ("schema", json!({"graph":"default"})),
        (
            "read",
            json!({"graph":"default","query":source,"params":{"min":1}}),
        ),
    ] {
        let denied = call(&app, &bob, tool, arguments).await;
        assert_eq!(denied["result"]["isError"], true, "{denied}");
        assert_eq!(
            denied["result"]["structuredContent"]["status"], 403,
            "{denied}"
        );
        assert_eq!(
            denied["result"]["structuredContent"]["error"]["code"],
            "forbidden"
        );
    }

    let explain_load =
        "explain load csv \"s3://imports/people.csv\" into node:Person mode overwrite header";
    for (subject, expected) in [
        ("read_only", StatusCode::FORBIDDEN),
        ("change_only", StatusCode::BAD_REQUEST),
    ] {
        let bearer = token(subject, RESOURCE);
        let arguments = json!({"graph":"default","query":explain_load});
        let refused = call(&app, &bearer, "read", arguments).await;
        assert_eq!(refused["result"]["isError"], true, "{refused}");
        assert_eq!(
            refused["result"]["structuredContent"]["status"],
            expected.as_u16(),
            "{refused}"
        );
        let request = Request::post("/graphs/default/query")
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {bearer}"))
            .body(Body::from(json!({"query":explain_load}).to_string()))
            .unwrap();
        let (status, native) = json_response(&app, request).await;
        assert_eq!(status, expected, "{native}");
        assert_eq!(refused["result"]["structuredContent"]["error"], native);
        let message = native["error"].as_str().unwrap();
        if subject == "read_only" {
            assert!(message.contains("action 'change'"), "{native}");
        } else {
            assert!(
                message.contains("ingress is denied for this graph"),
                "{native}"
            );
        }
    }

    // Writes never pass through `read`, and leave the graph unchanged.
    let before = manifest_dataset_version(&graph_path(temp.path())).await;
    for query in [
        "query add() { insert Person { name: \"Mallory\", age: 1 } }",
        "query bump() { update Person set { age: 99 } where name = \"Alice\" }",
        "query drop() { delete Person where name = \"Alice\" }",
        "branch create feature from main",
    ] {
        let refused = call(
            &app,
            &alice,
            "read",
            json!({"graph":"default","query":query}),
        )
        .await;
        assert_eq!(refused["result"]["isError"], true, "{query}: {refused}");
        assert_eq!(
            refused["result"]["structuredContent"]["status"], 400,
            "{query}: {refused}"
        );
    }
    let unknown = call(
        &app,
        &alice,
        "read",
        json!({"graph":"default","query":"branch list","settings":{}}),
    )
    .await;
    assert_eq!(unknown["result"]["isError"], true, "{unknown}");
    assert_eq!(
        manifest_dataset_version(&graph_path(temp.path())).await,
        before
    );

    // The request bound applies to the inline source as to any MCP request.
    let oversized = rpc(
        Some(&alice),
        "tools/call",
        json!({"name":"read","arguments":{"graph":"default","query":format!("// {}\nbranch list", "x".repeat(64 * 1024))}}),
    );
    assert_eq!(
        app.clone().oneshot(oversized).await.unwrap().status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

/// A server with static bearer tokens only: actor `default` may do anything.
async fn static_token_app(settings: omnigraph_server::McpSettings) -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy_path = temp.path().join("policy.yaml");
    std::fs::write(&policy_path, permit_all_policy_yaml(&["default"])).unwrap();
    let state = AppState::open_with_bearer_tokens_and_policy(
        graph.to_string_lossy().to_string(),
        vec![("default".to_string(), "secret".to_string())],
        Some(&policy_path),
    )
    .await
    .unwrap()
    .with_mcp_settings(settings);
    (temp, omnigraph_server::build_app(state))
}

#[tokio::test]
async fn static_token_server_serves_mcp_without_oauth_discovery() {
    let mut settings = omnigraph_server::McpSettings::default();
    settings.allowed_hosts = vec!["graphs.internal:8080".into()];
    let (_temp, app) = static_token_app(settings).await;

    let response = app
        .clone()
        .oneshot(rpc(Some("secret"), "tools/list", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for (credential, challenge) in [
        (None, "Bearer"),
        (Some("wrong"), "Bearer error=\"invalid_token\""),
    ] {
        let response = app
            .clone()
            .oneshot(rpc(credential, "tools/list", json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // No OIDC resource: nothing to discover, so the challenge names no
        // metadata, as on every other route.
        assert_eq!(response.headers()["www-authenticate"], challenge);
    }
    assert_eq!(
        app.clone()
            .oneshot(
                Request::get("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    for (host, expected) in [
        ("graphs.internal:8080", StatusCode::OK),
        ("127.0.0.1", StatusCode::OK),
        ("graphs.internal", StatusCode::FORBIDDEN),
        ("attacker.example", StatusCode::FORBIDDEN),
    ] {
        let mut request = rpc(Some("secret"), "tools/list", json!({}));
        request.headers_mut().insert("host", host.parse().unwrap());
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            expected,
            "{host}"
        );
    }
    // No resource origin exists to allow, so any browser Origin is refused.
    for origin in ["https://attacker.example", "http://localhost", "null"] {
        let mut request = rpc(Some("secret"), "tools/list", json!({}));
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN,
            "{origin}"
        );
    }
    // A static token is not an identity, so `graphs` asks `GET /graphs`,
    // whose `graph_list` action only a server-scoped policy grants.
    let graphs = call(&app, "secret", "graphs", json!({})).await;
    assert_eq!(
        graphs["result"]["structuredContent"]["status"], 403,
        "{graphs}"
    );
    let (status, native) = json_response(&app, get_request("/graphs", "secret")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(graphs["result"]["structuredContent"]["error"], native);
    let schema = call(&app, "secret", "schema", json!({"graph":"default"})).await;
    assert_eq!(schema["result"]["isError"], false, "{schema}");
}

async fn http_get(app: &Router, uri: &str, token: &str) -> Value {
    let (status, body) = json_response(app, get_request(uri, token)).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    body
}

#[tokio::test]
async fn mcp_read_parity_tools_equal_their_routes() {
    let (_temp, app) = static_token_app(omnigraph_server::McpSettings::default()).await;
    // A commit to inspect, through the MCP write path.
    let written = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","query":"query add($name: String) { insert Person { name: $name, age: 41 } }","params":{"name":"Dana"}}),
    )
    .await;
    assert_eq!(written["result"]["isError"], false, "{written}");
    let commit = written["result"]["structuredContent"]["commit"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // A branch with two changes of its own, and a proposal awaiting
    // publication.
    let insert = "query add($name: String) { insert Person { name: $name, age: 9 } }";
    for (tool, arguments) in [
        (
            "branches_create",
            json!({"graph":"default","name":"feature"}),
        ),
        (
            "mutate",
            json!({"graph":"default","branch":"feature","query":insert,"params":{"name":"Fern"}}),
        ),
        (
            "mutate",
            json!({"graph":"default","branch":"feature","query":insert,"params":{"name":"Gus"}}),
        ),
        (
            "mutate",
            json!({"graph":"default","query":"query propose() as commit { insert Person { name: \"Pia\", age: 33 } }"}),
        ),
    ] {
        let result = call(&app, "secret", tool, arguments).await;
        assert_eq!(result["result"]["isError"], false, "{tool}: {result}");
    }

    let health = call(&app, "secret", "health", json!({})).await;
    let (_, native_health) =
        json_response(&app, Request::get("/healthz").body(Body::empty()).unwrap()).await;
    assert_eq!(health["result"]["structuredContent"], native_health);

    for (tool, arguments, uri) in [
        (
            "snapshot",
            json!({"graph":"default"}),
            "/graphs/default/snapshot".to_string(),
        ),
        (
            "snapshot",
            json!({"graph":"default","branch":"main"}),
            "/graphs/default/snapshot?branch=main".to_string(),
        ),
        (
            "branches_list",
            json!({"graph":"default"}),
            "/graphs/default/branches".to_string(),
        ),
        (
            "branches_list",
            json!({"graph":"default","detailed":true}),
            "/graphs/default/branches?detailed=true".to_string(),
        ),
        (
            "access",
            json!({"graph":"default"}),
            "/graphs/default/access".to_string(),
        ),
        (
            "access",
            json!({"graph":"default","branch":"main","target_branch":"feature"}),
            "/graphs/default/access?branch=main&target_branch=feature".to_string(),
        ),
        (
            "commits_list",
            json!({"graph":"default"}),
            "/graphs/default/commits".to_string(),
        ),
        (
            "commits_list",
            json!({"graph":"default","branch":"main","limit":1}),
            "/graphs/default/commits?branch=main&limit=1".to_string(),
        ),
        (
            "commits_get",
            json!({"graph":"default","commit_id":commit}),
            format!("/graphs/default/commits/{commit}"),
        ),
        (
            "commits_changes",
            json!({"graph":"default","commit_id":commit,"kind":["node"],"op":["insert"],"limit":10}),
            format!("/graphs/default/commits/{commit}/changes?kind=node&op=insert&limit=10"),
        ),
        (
            "changes_poll",
            json!({"graph":"default","start":"beginning","type":["Person"]}),
            "/graphs/default/changes?start=beginning&type=Person".to_string(),
        ),
        (
            "commits_list",
            json!({"graph":"default","listing":"unpublished"}),
            "/graphs/default/commits?listing=unpublished".to_string(),
        ),
        (
            "commits_list",
            json!({"graph":"default","branch":"feature","listing":"history"}),
            "/graphs/default/commits?branch=feature&listing=history".to_string(),
        ),
        (
            "diff",
            json!({"graph":"default","from":"main","to":"feature"}),
            "/graphs/default/diff?from=main&to=feature".to_string(),
        ),
        (
            "diff",
            json!({"graph":"default","from":"feature","to":commit,"kind":["node"],"op":["insert","delete"],"limit":1}),
            format!(
                "/graphs/default/diff?from=feature&to={commit}&kind=node&op=insert&op=delete&limit=1"
            ),
        ),
        (
            "branches_diff",
            json!({"graph":"default","source":"feature"}),
            "/graphs/default/branches/diff?source=feature".to_string(),
        ),
        (
            "branches_diff",
            json!({"graph":"default","source":"feature","target":"main","type":["Person"],"limit":1}),
            "/graphs/default/branches/diff?source=feature&target=main&type=Person&limit=1"
                .to_string(),
        ),
        (
            "branches_merge_base",
            json!({"graph":"default","source":"feature"}),
            "/graphs/default/branches/merge-base?source=feature".to_string(),
        ),
        (
            "branches_merge_base",
            json!({"graph":"default","source":"main","target":"feature"}),
            "/graphs/default/branches/merge-base?source=main&target=feature".to_string(),
        ),
    ] {
        let result = call(&app, "secret", tool, arguments).await;
        assert_eq!(result["result"]["isError"], false, "{tool}: {result}");
        if tool == "access" {
            let access = &result["result"]["structuredContent"];
            assert_eq!(access["actor_id"], "default", "{access}");
            assert!(access.get("actor").is_none(), "{access}");
        }
        assert_eq!(
            result["result"]["structuredContent"],
            http_get(&app, &uri, "secret").await,
            "{tool}"
        );
    }
    // The proposal is listed as unpublished, so the listing above was not
    // trivially empty.
    let unpublished = call(
        &app,
        "secret",
        "commits_list",
        json!({"graph":"default","listing":"unpublished"}),
    )
    .await;
    assert_eq!(
        unpublished["result"]["structuredContent"]["commits"][0]["published"], false,
        "{unpublished}"
    );
    // A page continues as the route's continuation does: the branch's two
    // changes take two pages of one.
    for (tool, sides) in [
        ("branches_diff", [("source", "feature"), ("target", "main")]),
        ("diff", [("from", "main"), ("to", "feature")]),
    ] {
        let mut arguments = json!({"graph":"default","limit":1});
        for (name, value) in sides {
            arguments[name] = json!(value);
        }
        let first = call(&app, "secret", tool, arguments.clone()).await;
        let token = first["result"]["structuredContent"]["next_page_token"]
            .as_str()
            .unwrap_or_else(|| panic!("{tool}: {first}"));
        arguments["page_token"] = json!(token);
        let next = call(&app, "secret", tool, arguments).await;
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        for (name, value) in sides {
            query.append_pair(name, value);
        }
        let query = query
            .append_pair("limit", "1")
            .append_pair("page_token", token)
            .finish();
        let route = if tool == "diff" {
            "diff"
        } else {
            "branches/diff"
        };
        assert_eq!(
            next["result"]["structuredContent"],
            http_get(&app, &format!("/graphs/default/{route}?{query}"), "secret").await,
            "{tool}"
        );
        assert_eq!(
            next["result"]["structuredContent"]["changes"]
                .as_array()
                .map(Vec::len),
            Some(1),
            "{tool}: {next}"
        );
    }

    // The route's own refusals pass through unchanged: status and body.
    for (tool, arguments, uri, status) in [
        (
            "changes_poll",
            json!({"graph":"default","start":"now","cursor":"x"}),
            "/graphs/default/changes?start=now&cursor=x".to_string(),
            400,
        ),
        (
            "commits_list",
            json!({"graph":"default","listing":"unpublished","limit":1}),
            "/graphs/default/commits?listing=unpublished&limit=1".to_string(),
            400,
        ),
        (
            "diff",
            json!({"graph":"default","from":"main","to":"missing"}),
            "/graphs/default/diff?from=main&to=missing".to_string(),
            404,
        ),
        (
            "branches_diff",
            json!({"graph":"default","source":"missing"}),
            "/graphs/default/branches/diff?source=missing".to_string(),
            404,
        ),
        (
            "branches_merge_base",
            json!({"graph":"default","source":"feature","target":"missing"}),
            "/graphs/default/branches/merge-base?source=feature&target=missing".to_string(),
            404,
        ),
    ] {
        let refused = call(&app, "secret", tool, arguments).await;
        assert_eq!(refused["result"]["isError"], true, "{tool}: {refused}");
        let (native_status, native) = json_response(&app, get_request(&uri, "secret")).await;
        assert_eq!(native_status.as_u16(), status, "{uri}: {native}");
        assert_eq!(
            refused["result"]["structuredContent"],
            json!({"status": status, "error": native}),
            "{tool}"
        );
    }
    let invalid = call(
        &app,
        "secret",
        "commits_list",
        json!({"graph":"default","listing":"everything"}),
    )
    .await;
    assert_eq!(
        invalid["result"]["structuredContent"]["status"], 400,
        "{invalid}"
    );
}

#[tokio::test]
async fn mcp_resources_read_graphs_schema_and_branches_under_policy() {
    let (_temp, app, _state) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let bob = token("bob", RESOURCE);
    let (status, listed) =
        json_response(&app, rpc(Some(&alice), "resources/list", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let uris: Vec<&str> = listed["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    assert_eq!(
        uris,
        vec![
            "omnigraph://graphs",
            "omnigraph://best-practices/index",
            "omnigraph://best-practices/queries",
            "omnigraph://best-practices/data",
            "omnigraph://best-practices/schema",
            "omnigraph://best-practices/search",
            "omnigraph://best-practices/changes",
        ]
    );
    // The references are the agent skill's own files, byte for byte.
    let skill =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills/omnigraph/references");
    for topic in ["queries", "data", "schema", "search", "changes"] {
        let uri = format!("omnigraph://best-practices/{topic}");
        let (_, body) =
            json_response(&app, rpc(Some(&bob), "resources/read", json!({"uri":uri}))).await;
        assert_eq!(body["result"]["contents"][0]["mimeType"], "text/markdown");
        assert_eq!(
            body["result"]["contents"][0]["text"],
            std::fs::read_to_string(skill.join(format!("{topic}.md"))).unwrap(),
            "{topic}"
        );
    }
    let (_, index) = json_response(
        &app,
        rpc(
            Some(&bob),
            "resources/read",
            json!({"uri":"omnigraph://best-practices/index"}),
        ),
    )
    .await;
    assert!(
        index["result"]["contents"][0]["text"]
            .as_str()
            .unwrap()
            .contains("omnigraph://best-practices/search")
    );
    let (_, templates) = json_response(
        &app,
        rpc(Some(&alice), "resources/templates/list", json!({})),
    )
    .await;
    let templates: Vec<&str> = templates["result"]["resourceTemplates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uriTemplate"].as_str().unwrap())
        .collect();
    assert_eq!(
        templates,
        vec![
            "omnigraph://graphs/{graph}/schema",
            "omnigraph://graphs/{graph}/branches"
        ]
    );

    let read = |token: String, uri: &'static str| {
        let app = app.clone();
        async move {
            json_response(
                &app,
                rpc(Some(&token), "resources/read", json!({"uri":uri})),
            )
            .await
            .1
        }
    };
    let schema = read(alice.clone(), "omnigraph://graphs/default/schema").await;
    let contents = &schema["result"]["contents"][0];
    assert_eq!(contents["mimeType"], "text/plain", "{schema}");
    let native = http_get(&app, "/graphs/default/schema", &alice).await;
    assert_eq!(contents["text"], native["schema_source"]);
    let branches = read(alice.clone(), "omnigraph://graphs/default/branches").await;
    let text = branches["result"]["contents"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(text).unwrap(),
        http_get(&app, "/graphs/default/branches", &alice).await
    );
    let graphs = read(bob.clone(), "omnigraph://graphs").await;
    let text = graphs["result"]["contents"][0]["text"].as_str().unwrap();
    assert!(serde_json::from_str::<Value>(text).unwrap()["graphs"].is_array());

    // Policy decides, and an unknown resource is not found.
    let denied = read(bob, "omnigraph://graphs/default/schema").await;
    assert_eq!(denied["error"]["data"]["status"], 403, "{denied}");
    for uri in [
        "omnigraph://graphs/unknown/schema",
        "omnigraph://graphs/default/secrets",
        "omnigraph://schema",
    ] {
        let missing = json_response(
            &app,
            rpc(Some(&alice), "resources/read", json!({"uri":uri})),
        )
        .await
        .1;
        assert_eq!(missing["error"]["code"], -32002, "{uri}: {missing}");
    }
}

#[tokio::test]
async fn mcp_write_tools_are_conditional_and_policy_gated() {
    let (temp, app) = static_token_app(omnigraph_server::McpSettings::default()).await;
    let tools = tool_list(&app, "secret").await;
    for (name, destructive, idempotent) in [
        ("mutate", true, false),
        ("load", true, false),
        ("blobs_clear", true, true),
        ("branches_create", false, false),
        ("branches_delete", true, true),
        ("branches_merge", true, false),
        ("commits_publish", true, true),
        ("commits_drop", true, true),
    ] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], false, "{name}");
        assert_eq!(
            tool["annotations"]["destructiveHint"], destructive,
            "{name}"
        );
        assert_eq!(tool["annotations"]["idempotentHint"], idempotent, "{name}");
        assert!(
            tool["description"]
                .as_str()
                .unwrap()
                .contains("outcome_unknown")
        );
    }
    assert!(
        instructions(&app, "secret")
            .await
            .contains("if_graph_commit")
    );

    // A conditional write against a stale head is refused before effects.
    let read = call(
        &app,
        "secret",
        "read",
        json!({"graph":"default","query":"query n() { match { $p: Person } return { $p.name } }"}),
    )
    .await;
    let head = read["result"]["structuredContent"]["graph_commit_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let insert = "query add($name: String) { insert Person { name: $name, age: 7 } }";
    let first = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","query":insert,"params":{"name":"Eve"},"if_graph_commit":head}),
    )
    .await;
    assert_eq!(first["result"]["isError"], false, "{first}");
    let after_first = manifest_dataset_version(&graph_path(temp.path())).await;
    let stale = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","query":insert,"params":{"name":"Frank"},"if_graph_commit":head}),
    )
    .await;
    assert_eq!(stale["result"]["isError"], true, "{stale}");
    assert_eq!(
        stale["result"]["structuredContent"]["status"], 412,
        "{stale}"
    );
    // The refusal is the route's own error body: its code and a
    // disposition that says nothing was written.
    let refusal = &stale["result"]["structuredContent"];
    assert_eq!(refusal["error"]["code"], "precondition_failed", "{stale}");
    assert_eq!(
        refusal["error"]["disposition"],
        json!({"effects": "none", "action": "refresh"}),
        "{stale}"
    );
    assert_eq!(
        refusal.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["error", "status"],
        "{stale}"
    );
    assert_eq!(
        manifest_dataset_version(&graph_path(temp.path())).await,
        after_first
    );

    // Load, then a branch round trip: create, write, merge, delete.
    let loaded = call(
        &app,
        "secret",
        "load",
        json!({"graph":"default","mode":"merge","data":"{\"type\":\"Person\",\"data\":{\"name\":\"Gina\",\"age\":50}}"}),
    )
    .await;
    assert_eq!(loaded["result"]["isError"], false, "{loaded}");
    for (tool, arguments) in [
        (
            "branches_create",
            json!({"graph":"default","name":"feature"}),
        ),
        (
            "mutate",
            json!({"graph":"default","query":insert,"params":{"name":"Hal"},"branch":"feature"}),
        ),
        (
            "branches_merge",
            json!({"graph":"default","source":"feature"}),
        ),
        (
            "branches_delete",
            json!({"graph":"default","name":"feature"}),
        ),
    ] {
        let result = call(&app, "secret", tool, arguments).await;
        assert_eq!(result["result"]["isError"], false, "{tool}: {result}");
    }
    let names = call(
        &app,
        "secret",
        "read",
        json!({"graph":"default","query":"query names() { match { $p: Person } return { $p.name } order { $p.name asc } }"}),
    )
    .await;
    let rows = names["result"]["structuredContent"]["rows"].to_string();
    for name in ["Eve", "Gina", "Hal"] {
        assert!(rows.contains(name), "{name}: {rows}");
    }
    assert!(!rows.contains("Frank"));
    let branches = call(&app, "secret", "branches_list", json!({"graph":"default"})).await;
    assert_eq!(
        branches["result"]["structuredContent"]["branches"],
        json!(["main"])
    );
    let missing = call(
        &app,
        "secret",
        "branches_delete",
        json!({"graph":"default","name":"feature"}),
    )
    .await;
    assert_eq!(missing["result"]["structuredContent"]["status"], 404);

    // A load that forks its branch publishes the fork before its rows, so a
    // typed refusal of the rows follows effects. The tool's task boundary
    // must not lose that: the body says so, and the branch is there.
    let refused = call(
        &app,
        "secret",
        "load",
        json!({"graph":"default","branch":"forked","from":"main","mode":"append","data":"{\"type\":\"Person\",\"data\":{\"name\":\"Alice\",\"age\":31}}"}),
    )
    .await;
    assert_eq!(refused["result"]["isError"], true, "{refused}");
    let refusal = &refused["result"]["structuredContent"];
    assert_eq!(refusal["status"], 409, "{refused}");
    assert!(refusal["error"]["key_conflict"].is_object(), "{refused}");
    assert_eq!(
        refusal["error"]["disposition"],
        json!({"effects": "partial", "action": "refresh"}),
        "{refused}"
    );
    let branches = call(&app, "secret", "branches_list", json!({"graph":"default"})).await;
    assert_eq!(
        branches["result"]["structuredContent"]["branches"],
        json!(["forked", "main"])
    );

    // Each write keeps its route's policy action: `change` without
    // `branch_create` refuses branch creation.
    let (_temp, app, _state) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let bob = token("bob", RESOURCE);
    let created = call(
        &app,
        &alice,
        "branches_create",
        json!({"graph":"default","name":"x"}),
    )
    .await;
    assert_eq!(
        created["result"]["structuredContent"]["status"], 403,
        "{created}"
    );
    let denied = call(
        &app,
        &bob,
        "mutate",
        json!({"graph":"default","query":insert,"params":{"name":"Ivy"}}),
    )
    .await;
    assert_eq!(
        denied["result"]["structuredContent"]["status"], 403,
        "{denied}"
    );
    let allowed = call(
        &app,
        &alice,
        "mutate",
        json!({"graph":"default","query":insert,"params":{"name":"Ivy"}}),
    )
    .await;
    assert_eq!(allowed["result"]["isError"], false, "{allowed}");
}

const DOCUMENT_SCHEMA: &str = "
node Document {
    title: String @key
    content: Blob?
}
";

const DOCUMENTS: &str = r#"{"type":"Document","data":{"title":"readme","content":"base64:SGVsbG8gV29ybGQ="}}
{"type":"Document","data":{"title":"notes","content":"base64:Tm90ZXM="}}
{"type":"Document","data":{"title":"empty"}}"#;

/// `default` may do everything the tools below need, `reader` may read and
/// invoke stored queries but change nothing, `stranger` may do nothing.
const OPERATIONS_POLICY: &str = r#"version: 1
groups:
  writers: ["default"]
  readers: ["reader"]
rules:
  - id: writers-data
    allow:
      actors: {group: writers}
      actions: [read, change, export]
      branch_scope: any
  - id: writers-targets
    allow:
      actors: {group: writers}
      actions: [branch_create, branch_delete, branch_merge, commit_publish, commit_drop]
      target_branch_scope: any
  - id: writers-invoke
    allow:
      actors: {group: writers}
      actions: [invoke_query]
  - id: readers-data
    allow:
      actors: {group: readers}
      actions: [read]
      branch_scope: any
  - id: readers-invoke
    allow:
      actors: {group: readers}
      actions: [invoke_query]
"#;

/// The standard graph plus Blob documents, a stored read and a stored
/// mutation, behind static tokens and [`OPERATIONS_POLICY`].
async fn operations_app(settings: omnigraph_server::McpSettings) -> (tempfile::TempDir, Router) {
    let schema = format!(
        "{}{DOCUMENT_SCHEMA}",
        std::fs::read_to_string(fixture("test.pg")).unwrap()
    );
    let data = format!(
        "{}\n{DOCUMENTS}",
        std::fs::read_to_string(fixture("test.jsonl"))
            .unwrap()
            .trim_end()
    );
    let temp = init_graph_with_schema_and_data(&schema, &data).await;
    let policy = temp.path().join("policy.yaml");
    std::fs::write(&policy, OPERATIONS_POLICY).unwrap();
    let queries = stored_query_registry(&[
        (
            "people",
            "query people() { match { $p: Person } return { $p.name } }",
            true,
        ),
        (
            "add",
            "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            true,
        ),
    ]);
    let tokens = [
        ("default", "secret"),
        ("reader", "reader-secret"),
        ("stranger", "stranger-secret"),
    ];
    let state = AppState::open_single_with_queries(
        graph_path(temp.path()).to_string_lossy().to_string(),
        tokens
            .iter()
            .map(|(actor, token)| ((*actor).to_string(), (*token).to_string()))
            .collect(),
        Some(&policy),
        queries,
    )
    .await
    .unwrap()
    .with_mcp_settings(settings);
    (temp, omnigraph_server::build_app(state))
}

/// One authenticated HTTP request, with extra headers and an optional JSON
/// body, as the route's own caller sends it.
fn http_request(
    method: Method,
    uri: &str,
    token: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
        .header("authorization", format!("Bearer {token}"));
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

fn blob_uri(id: &str) -> String {
    format!("/graphs/default/blob?entity=node&type=Document&id={id}&property=content")
}

fn blob_cell(id: &str) -> Value {
    json!({"graph":"default","entity":"node","type":"Document","id":id,"property":"content"})
}

async fn main_head(app: &Router) -> String {
    http_get(app, "/graphs/default/commits?limit=1", "secret").await["head_commit_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// A refused tool call carries the route's status and error body unchanged.
fn assert_refusal(result: &Value, status: StatusCode, native: &Value) {
    assert_eq!(result["result"]["isError"], true, "{result}");
    assert_eq!(
        result["result"]["structuredContent"],
        json!({"status": status.as_u16(), "error": native}),
    );
}

/// Blob stat and clear, commit publish and drop, and stored mutations equal
/// their routes for the same request: the result, the refusal body, the
/// precondition and the policy decision.
#[tokio::test]
async fn mcp_blob_commit_control_and_stored_mutation_tools_equal_their_routes() {
    let (temp, app) = operations_app(omnigraph_server::McpSettings::default()).await;
    let graph = graph_path(temp.path());

    // `blobs_stat` is the HEAD answer's headers as `blob stat` JSON.
    let stat = call(&app, "secret", "blobs_stat", blob_cell("readme")).await;
    let head = app
        .clone()
        .oneshot(http_request(
            Method::HEAD,
            &blob_uri("readme"),
            "secret",
            &[],
            None,
        ))
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let header = |name: &str| head.headers()[name].to_str().unwrap().to_owned();
    assert_eq!(header("content-length"), "11");
    assert_eq!(
        stat["result"]["structuredContent"],
        json!({
            "selector": {"entity":"node","type":"Document","id":"readme","property":"content"},
            "kind": "managed",
            "size": 11,
            "etag": header("etag"),
            "target": {"resolved_snapshot": header("omnigraph-snapshot-id")},
        }),
        "{stat}"
    );
    let mut on_main = blob_cell("readme");
    on_main["branch"] = json!("main");
    let on_main = call(&app, "secret", "blobs_stat", on_main).await;
    assert_eq!(
        on_main["result"]["structuredContent"]["target"],
        json!({"branch": "main", "resolved_snapshot": header("omnigraph-snapshot-id")})
    );
    // A null cell and an unknown entity answer what GET and HEAD answer.
    for id in ["empty", "missing"] {
        let refused = call(&app, "secret", "blobs_stat", blob_cell(id)).await;
        let (status, native) = json_response(
            &app,
            http_request(Method::GET, &blob_uri(id), "secret", &[], None),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id}: {native}");
        let head = app
            .clone()
            .oneshot(http_request(
                Method::HEAD,
                &blob_uri(id),
                "secret",
                &[],
                None,
            ))
            .await
            .unwrap();
        assert_eq!(head.status(), status, "{id}");
        assert_refusal(&refused, status, &native);
    }

    // `blobs_clear` is `DELETE /blob`: a validator that does not hold is the
    // route's 412, with nothing written.
    let before = manifest_dataset_version(&graph).await;
    let (status, native) = json_response(
        &app,
        http_request(
            Method::DELETE,
            &blob_uri("readme"),
            "secret",
            &[("if-match", "\"stale\"")],
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{native}");
    let mut stale = blob_cell("readme");
    stale["if_match"] = json!("\"stale\"");
    assert_refusal(
        &call(&app, "secret", "blobs_clear", stale).await,
        status,
        &native,
    );
    assert_eq!(manifest_dataset_version(&graph).await, before);
    let mut current = blob_cell("readme");
    current["if_match"] = stat["result"]["structuredContent"]["etag"].clone();
    let cleared = call(&app, "secret", "blobs_clear", current).await;
    assert_eq!(cleared["result"]["isError"], false, "{cleared}");
    let receipt = &cleared["result"]["structuredContent"];
    assert_eq!(receipt["kind"], "null", "{cleared}");
    assert_eq!(receipt["actor_id"], "default", "{cleared}");
    let cleared_commit = receipt["commit"]["graph_commit_id"].as_str().unwrap();
    assert_eq!(
        receipt["commit"],
        http_get(
            &app,
            &format!("/graphs/default/commits/{cleared_commit}"),
            "secret"
        )
        .await
    );
    // A null cell publishes nothing: the route and the tool answer one body.
    let (status, native) = json_response(
        &app,
        http_request(Method::DELETE, &blob_uri("readme"), "secret", &[], None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{native}");
    assert!(native["commit"].is_null(), "{native}");
    let again = call(&app, "secret", "blobs_clear", blob_cell("readme")).await;
    assert_eq!(again["result"]["structuredContent"], native);

    // `commits_publish` and `commits_drop` on proposals written `as commit`.
    let propose = |name: &str| {
        let app = app.clone();
        let source = format!(
            "query propose() as commit {{ insert Person {{ name: \"{name}\", age: 40 }} }}"
        );
        async move {
            let proposed = call(
                &app,
                "secret",
                "mutate",
                json!({"graph":"default","query":source}),
            )
            .await;
            let commit = &proposed["result"]["structuredContent"]["commit"];
            assert_eq!(commit["published"], false, "{proposed}");
            commit["graph_commit_id"].as_str().unwrap().to_owned()
        }
    };
    let proposals = [propose("Pia").await, propose("Quinn").await];
    let published = call(
        &app,
        "secret",
        "commits_publish",
        json!({"graph":"default","commit_id":proposals[0]}),
    )
    .await;
    assert_eq!(published["result"]["isError"], false, "{published}");
    let shown = http_get(
        &app,
        &format!("/graphs/default/commits/{}", proposals[0]),
        "secret",
    )
    .await;
    assert_eq!(shown["published"], true, "{shown}");
    assert_eq!(published["result"]["structuredContent"], shown);
    // A published id is answered as it stands, by the route and the tool.
    let publish_uri = format!("/graphs/default/commits/{}/publish", proposals[0]);
    let (status, native) = json_response(
        &app,
        http_request(Method::POST, &publish_uri, "secret", &[], None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{native}");
    let republished = call(
        &app,
        "secret",
        "commits_publish",
        json!({"graph":"default","commit_id":proposals[0]}),
    )
    .await;
    assert_eq!(republished["result"]["structuredContent"], native);
    let dropped = call(
        &app,
        "secret",
        "commits_drop",
        json!({"graph":"default","commit_id":proposals[1]}),
    )
    .await;
    assert_eq!(dropped["result"]["isError"], false, "{dropped}");
    assert_eq!(dropped["result"]["structuredContent"], json!({}));
    // The dropped id and a published one refuse as their route refuses.
    for (commit, expected) in [
        (&proposals[1], StatusCode::NOT_FOUND),
        (&proposals[0], StatusCode::CONFLICT),
    ] {
        let (status, native) = json_response(
            &app,
            http_request(
                Method::DELETE,
                &format!("/graphs/default/commits/{commit}"),
                "secret",
                &[],
                None,
            ),
        )
        .await;
        assert_eq!(status, expected, "{native}");
        let refused = call(
            &app,
            "secret",
            "commits_drop",
            json!({"graph":"default","commit_id":commit}),
        )
        .await;
        assert_refusal(&refused, status, &native);
    }

    // `mutate` with a stored mutation's name and no source is the stored
    // query route.
    let stale_head = main_head(&app).await;
    let added = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","name":"add","params":{"name":"Rae","age":20}}),
    )
    .await;
    assert_eq!(added["result"]["isError"], false, "{added}");
    let receipt = added["result"]["structuredContent"].clone();
    let added_commit = receipt["commit"]["graph_commit_id"].as_str().unwrap();
    assert_eq!(
        receipt["commit"],
        http_get(
            &app,
            &format!("/graphs/default/commits/{added_commit}"),
            "secret"
        )
        .await
    );
    let (status, native) = json_response(
        &app,
        invoke_request(
            "add",
            "secret",
            json!({"params":{"name":"Sol","age":21},"expect_mutation":true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{native}");
    let without_commit = |mut output: Value| {
        output["commit"] = Value::Null;
        output
    };
    assert_eq!(without_commit(receipt), without_commit(native));
    // `if_graph_commit` takes the conditional route, which requires it: a
    // stale, an empty and an entity-tag value refuse as that route refuses,
    // and none of them writes unconditionally.
    let before = manifest_dataset_version(&graph).await;
    let body = json!({"params":{"name":"Tao","age":22},"expect_mutation":true});
    for (precondition, expected) in [
        (stale_head.as_str(), StatusCode::PRECONDITION_FAILED),
        ("", StatusCode::BAD_REQUEST),
        ("W/\"etag\"", StatusCode::BAD_REQUEST),
    ] {
        let (status, native) = json_response(
            &app,
            http_request(
                Method::POST,
                "/graphs/default/queries/add/if-graph-commit",
                "secret",
                &[("omnigraph-if-graph-commit", precondition)],
                Some(body.clone()),
            ),
        )
        .await;
        assert_eq!(status, expected, "{precondition:?}: {native}");
        let refused = call(
            &app,
            "secret",
            "mutate",
            json!({"graph":"default","name":"add","params":{"name":"Tao","age":22},"if_graph_commit":precondition}),
        )
        .await;
        assert_refusal(&refused, status, &native);
    }
    assert_eq!(manifest_dataset_version(&graph).await, before);
    let conditional = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","name":"add","params":{"name":"Tao","age":22},"if_graph_commit":main_head(&app).await}),
    )
    .await;
    assert_eq!(conditional["result"]["isError"], false, "{conditional}");
    // A stored read is not a mutation, and `mutate` needs source or a name.
    let (status, native) = json_response(
        &app,
        invoke_request("people", "secret", json!({"expect_mutation":true})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{native}");
    let refused = call(
        &app,
        "secret",
        "mutate",
        json!({"graph":"default","name":"people"}),
    )
    .await;
    assert_refusal(&refused, status, &native);
    let nameless = call(&app, "secret", "mutate", json!({"graph":"default"})).await;
    assert_eq!(
        nameless["result"]["structuredContent"]["status"], 400,
        "{nameless}"
    );

    // Policy decides each tool exactly as it decides the route. The proposal
    // is live, written at main's current head, so its 404 is the denial's.
    let pending = propose("Uma").await;
    let pending = pending.as_str();
    let head = main_head(&app).await;
    let before = manifest_dataset_version(&graph).await;
    let stored_add = json!({"params":{"name":"Vic","age":23},"expect_mutation":true});
    let add_arguments = json!({"graph":"default","name":"add","params":{"name":"Vic","age":23}});
    let mut conditional_add = add_arguments.clone();
    conditional_add["if_graph_commit"] = json!(head);
    let publish_uri = format!("/graphs/default/commits/{pending}/publish");
    let drop_uri = format!("/graphs/default/commits/{pending}");
    let get = |uri: &str, token: &str| http_request(Method::GET, uri, token, &[], None);
    for (token, tool, arguments, request, expected) in [
        (
            "stranger-secret",
            "diff",
            json!({"graph":"default","from":"main","to":"main"}),
            get("/graphs/default/diff?from=main&to=main", "stranger-secret"),
            StatusCode::NOT_FOUND,
        ),
        (
            "stranger-secret",
            "branches_diff",
            json!({"graph":"default","source":"main"}),
            get(
                "/graphs/default/branches/diff?source=main",
                "stranger-secret",
            ),
            StatusCode::FORBIDDEN,
        ),
        (
            "stranger-secret",
            "branches_merge_base",
            json!({"graph":"default","source":"main"}),
            get(
                "/graphs/default/branches/merge-base?source=main",
                "stranger-secret",
            ),
            StatusCode::FORBIDDEN,
        ),
        (
            "stranger-secret",
            "commits_list",
            json!({"graph":"default","listing":"unpublished"}),
            get(
                "/graphs/default/commits?listing=unpublished",
                "stranger-secret",
            ),
            StatusCode::FORBIDDEN,
        ),
        (
            "stranger-secret",
            "blobs_stat",
            blob_cell("notes"),
            get(&blob_uri("notes"), "stranger-secret"),
            StatusCode::FORBIDDEN,
        ),
        (
            "reader-secret",
            "blobs_clear",
            blob_cell("notes"),
            http_request(
                Method::DELETE,
                &blob_uri("notes"),
                "reader-secret",
                &[],
                None,
            ),
            StatusCode::FORBIDDEN,
        ),
        // A denied commit control is the 404 of an unknown id.
        (
            "reader-secret",
            "commits_publish",
            json!({"graph":"default","commit_id":pending}),
            http_request(Method::POST, &publish_uri, "reader-secret", &[], None),
            StatusCode::NOT_FOUND,
        ),
        (
            "reader-secret",
            "commits_drop",
            json!({"graph":"default","commit_id":pending}),
            http_request(Method::DELETE, &drop_uri, "reader-secret", &[], None),
            StatusCode::NOT_FOUND,
        ),
        // `invoke_query` without `change`: the inner gate's 403.
        (
            "reader-secret",
            "mutate",
            add_arguments.clone(),
            http_request(
                Method::POST,
                "/graphs/default/queries/add",
                "reader-secret",
                &[],
                Some(stored_add.clone()),
            ),
            StatusCode::FORBIDDEN,
        ),
        (
            "reader-secret",
            "mutate",
            conditional_add,
            http_request(
                Method::POST,
                "/graphs/default/queries/add/if-graph-commit",
                "reader-secret",
                &[("omnigraph-if-graph-commit", &head)],
                Some(stored_add.clone()),
            ),
            StatusCode::FORBIDDEN,
        ),
        // Without `invoke_query`, a stored mutation is the 404 of a missing one.
        (
            "stranger-secret",
            "mutate",
            add_arguments,
            http_request(
                Method::POST,
                "/graphs/default/queries/add",
                "stranger-secret",
                &[],
                Some(stored_add),
            ),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (status, native) = json_response(&app, request).await;
        assert_eq!(status, expected, "{tool} as {token}: {native}");
        let refused = call(&app, token, tool, arguments).await;
        assert_refusal(&refused, status, &native);
    }
    assert_eq!(manifest_dataset_version(&graph).await, before);
    let unpublished = http_get(
        &app,
        "/graphs/default/commits?listing=unpublished",
        "secret",
    )
    .await;
    assert!(
        unpublished["commits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|commit| commit["graph_commit_id"] == pending),
        "a denied control leaves the proposal in place: {unpublished}"
    );
}
