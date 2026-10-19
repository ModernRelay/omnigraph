//! MCP is another authenticated read transport over the native policy handlers.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, StatusCode};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::StreamExt as _;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use omnigraph_server::AppState;
use omnigraph_server::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use omnigraph_server::oidc_identity::OidcIdentityTrust;
use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey as _, traits::PublicKeyParts as _};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use crate::support::*;

const KEY: &str = include_str!("../fixtures/oidc-test-key.pem");
const RESOURCE: &str = "https://data.example/clusters/A/incarnations/one";
const ISSUER: &str = "https://identity.example";

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn token(subject: &str, audience: &str) -> String {
    let now = now();
    let claims = json!({"iss":ISSUER,"aud":audience,"org_id":"org_example",
        "client_id":"oauth-client","sub":subject,"iat":now,"exp":now+300,
        "roles":["admin"],"permissions":["everything"]});
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-1".into());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(KEY.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn trust(root: &std::path::Path) -> Arc<OidcIdentityTrust> {
    let now = now();
    let key = RsaPrivateKey::from_pkcs8_pem(KEY).unwrap();
    let canonical_root = format!("file://{}", root.display());
    let document = json!({"version":1,"revision":1,"generated_at":now,"expires_at":now+300,
        "issuer":ISSUER,"audience":RESOURCE,"organization_id":"org_example","account_id":"account_1",
        "cluster_id":"A","cluster_incarnation":"one","canonical_root":canonical_root,
        "keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig",
            "n":URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(key.e().to_bytes_be())}],
        "principals":[{"subject":"alice","principal_id":"stable_alice"},{"subject":"bob","principal_id":"stable_bob"},
            {"subject":"read_only","principal_id":"stable_reader"},{"subject":"change_only","principal_id":"stable_changer"}]});
    let path = root.join("oidc-public.json");
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    OidcIdentityTrust::read(&path, &canonical_root).unwrap()
}

async fn fixture_app() -> (tempfile::TempDir, Router, AppState) {
    fixture_app_with(omnigraph_server::McpSettings::default()).await
}

fn adhoc() -> omnigraph_server::McpSettings {
    let mut settings = omnigraph_server::McpSettings::default();
    settings.allow_adhoc = true;
    settings
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
    let (status, list) = json_response(&app, rpc(Some(&alice), "tools/list", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let names: Vec<_> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec![
            "health",
            "graphs",
            "schema",
            "queries",
            "query",
            "snapshot",
            "access",
            "branches_list",
            "commits_list",
            "commits_get",
            "commits_changes",
            "changes_poll",
        ]
    );
    for tool in list["result"]["tools"].as_array().unwrap() {
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
    }
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
    let catalog = call(&app, &alice, "queries", json!({"graph":"default"})).await;
    let queries = catalog["result"]["structuredContent"]["queries"]
        .as_array()
        .unwrap();
    assert_eq!(queries.len(), 2, "{catalog}");
    assert!(queries.iter().all(|q| q["mutation"] == false));
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
    for wrong in [
        token("alice", "https://data.example/clusters/B/incarnations/one"),
        token("unknown", RESOURCE),
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
/// output schema is declared. The opt-in `read` tool appears only when the
/// server enables it.
#[tokio::test]
async fn mcp_tool_list_follows_the_adhoc_setting_and_describes_every_tool() {
    let alice = token("alice", RESOURCE);
    for (settings, expected) in [
        (
            omnigraph_server::McpSettings::default(),
            vec![
                "health [] read_only output",
                "graphs [] read_only output",
                "schema [graph] read_only output",
                "queries [graph] read_only output",
                "query [graph,name] read_only",
                "snapshot [graph] read_only",
                "access [graph] read_only output",
                "branches_list [graph] read_only output",
                "commits_list [graph] read_only",
                "commits_get [graph,commit_id] read_only",
                "commits_changes [graph,commit_id] read_only",
                "changes_poll [graph] read_only",
            ],
        ),
        (
            adhoc(),
            vec![
                "health [] read_only output",
                "graphs [] read_only output",
                "schema [graph] read_only output",
                "queries [graph] read_only output",
                "query [graph,name] read_only",
                "read [graph,query] read_only",
                "snapshot [graph] read_only",
                "access [graph] read_only output",
                "branches_list [graph] read_only output",
                "commits_list [graph] read_only",
                "commits_get [graph,commit_id] read_only",
                "commits_changes [graph,commit_id] read_only",
                "changes_poll [graph] read_only",
            ],
        ),
        (
            writes(omnigraph_server::McpSettings::default()),
            vec![
                "health [] read_only output",
                "graphs [] read_only output",
                "schema [graph] read_only output",
                "queries [graph] read_only output",
                "query [graph,name] read_only",
                "snapshot [graph] read_only",
                "access [graph] read_only output",
                "branches_list [graph] read_only output",
                "commits_list [graph] read_only",
                "commits_get [graph,commit_id] read_only",
                "commits_changes [graph,commit_id] read_only",
                "changes_poll [graph] read_only",
                "mutate [graph,query]",
                "load [graph,mode,data]",
                "branches_create [graph,name]",
                "branches_delete [graph,name]",
                "branches_merge [graph,source]",
            ],
        ),
    ] {
        let enabled = settings.allow_adhoc;
        let (_temp, app, _state) = fixture_app_with(settings).await;
        let tools = tool_list(&app, &alice).await;
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
                    line.push_str(" output");
                }
                line
            })
            .collect();
        assert_eq!(lines, expected);
        let text = instructions(&app, &alice).await;
        assert!(text.contains("Call `schema`"), "{text}");
        assert_eq!(text.contains("`read`"), enabled, "{text}");
        if !enabled {
            let refused = call(&app, &alice, "read", json!({"graph":"default","query":"query q() { match { $p: Person } return { $p.name } }"})).await;
            assert!(refused.get("error").is_some(), "{refused}");
        }
    }
}

#[tokio::test]
async fn mcp_schema_and_adhoc_read_reuse_their_routes_and_policy() {
    let (temp, app, _state) = fixture_app_with(adhoc()).await;
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

fn writes(mut settings: omnigraph_server::McpSettings) -> omnigraph_server::McpSettings {
    settings.allow_writes = true;
    settings
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
    for credential in [None, Some("wrong")] {
        let response = app
            .clone()
            .oneshot(rpc(credential, "tools/list", json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // No OIDC resource: nothing to discover, so no OAuth challenge.
        assert!(!response.headers().contains_key("www-authenticate"));
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
    let (_temp, app) = static_token_app(writes(omnigraph_server::McpSettings::default())).await;
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
    ] {
        let result = call(&app, "secret", tool, arguments).await;
        assert_eq!(result["result"]["isError"], false, "{tool}: {result}");
        assert_eq!(
            result["result"]["structuredContent"],
            http_get(&app, &uri, "secret").await,
            "{tool}"
        );
    }
    // The route's own refusals pass through unchanged.
    let refused = call(
        &app,
        "secret",
        "changes_poll",
        json!({"graph":"default","start":"now","cursor":"x"}),
    )
    .await;
    assert_eq!(refused["result"]["isError"], true);
    assert_eq!(refused["result"]["structuredContent"]["status"], 400);
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
async fn mcp_write_tools_are_opt_in_conditional_and_policy_gated() {
    // Off by default: absent and refused, with nothing written.
    let (temp, app) = static_token_app(omnigraph_server::McpSettings::default()).await;
    let before = manifest_dataset_version(&graph_path(temp.path())).await;
    let names: Vec<String> = tool_list(&app, "secret")
        .await
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect();
    for write in [
        "mutate",
        "load",
        "branches_create",
        "branches_delete",
        "branches_merge",
    ] {
        assert!(!names.iter().any(|name| name == write), "{write}");
        let refused = call(&app, "secret", write, json!({"graph":"default"})).await;
        assert!(refused.get("error").is_some(), "{write}: {refused}");
    }
    assert_eq!(
        manifest_dataset_version(&graph_path(temp.path())).await,
        before
    );

    let (temp, app) = static_token_app(writes(adhoc())).await;
    let tools = tool_list(&app, "secret").await;
    for (name, destructive, idempotent) in [
        ("mutate", true, false),
        ("load", true, false),
        ("branches_create", false, false),
        ("branches_delete", true, true),
        ("branches_merge", true, false),
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
    assert!(
        stale["result"]["structuredContent"]
            .get("outcome_unknown")
            .is_none()
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
        refusal["error"]["outcome"],
        json!({"execution": "unknown", "effects": "unknown", "action": "refresh"}),
        "{refused}"
    );
    let branches = call(&app, "secret", "branches_list", json!({"graph":"default"})).await;
    assert_eq!(
        branches["result"]["structuredContent"]["branches"],
        json!(["forked", "main"])
    );

    // Each write keeps its route's policy action: `change` without
    // `branch_create` refuses branch creation.
    let (_temp, app, _state) =
        fixture_app_with(writes(omnigraph_server::McpSettings::default())).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn a_local_server_reads_ad_hoc_and_lists_write_tools_only_when_it_writes() {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let before = manifest_dataset_version(&graph).await;
    for access in [
        omnigraph_server::LocalAccess::ReadOnly,
        omnigraph_server::LocalAccess::ReadWrite,
    ] {
        let binding = omnigraph_server::LocalBinding::local(&graph).unwrap();
        let (state, credential) = omnigraph_server::with_local_read_write(AppState::local(
            omnigraph_server::LocalScope::Graph(binding),
            omnigraph_server::LocalOptions {
                access,
                actor: Some("act-local".into()),
                process_defaults: omnigraph_server::ProcessDefaults::default(),
            },
        ))
        .await
        .ok()
        .unwrap();
        let app = omnigraph_server::build_app(state);
        let token = credential.reveal();
        let writes_allowed = access == omnigraph_server::LocalAccess::ReadWrite;
        let names: Vec<String> = tool_list(&app, &token)
            .await
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        // A standalone graph has no stored queries, so ad-hoc reads are on.
        assert!(names.iter().any(|name| name == "read"), "{names:?}");
        for write in [
            "mutate",
            "load",
            "branches_create",
            "branches_delete",
            "branches_merge",
        ] {
            assert_eq!(
                names.iter().any(|name| name == write),
                writes_allowed,
                "{access:?} {write}"
            );
        }
        let graphs = call(&app, &token, "graphs", json!({})).await;
        assert_eq!(graphs["result"]["isError"], false, "{graphs}");
        let read = call(
            &app,
            &token,
            "read",
            json!({"graph":"default","query":"query n() { match { $p: Person } return { $p.name } }"}),
        )
        .await;
        assert_eq!(read["result"]["isError"], false, "{read}");
        if !writes_allowed {
            let refused = call(
                &app,
                &token,
                "mutate",
                json!({"graph":"default","query":"query add() { insert Person { name: \"Eve\", age: 7 } }"}),
            )
            .await;
            assert!(refused.get("error").is_some(), "{refused}");
        }
    }
    assert_eq!(manifest_dataset_version(&graph).await, before);
}
