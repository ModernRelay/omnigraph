//! MCP is another authenticated read transport over the native policy handlers.
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use omnigraph_server::AppState;
use omnigraph_server::oidc_identity::OidcIdentityTrust;
use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey as _, traits::PublicKeyParts as _};
use serde_json::{Value, json};
use tower::ServiceExt as _;

mod support;
use support::*;

const KEY: &str = include_str!("fixtures/oidc-test-key.pem");
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
        "principals":[{"subject":"alice","principal_id":"stable_alice"},{"subject":"bob","principal_id":"stable_bob"}]});
    let path = root.join("oidc-public.json");
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    OidcIdentityTrust::read(&path, &canonical_root).unwrap()
}

async fn fixture_app() -> (tempfile::TempDir, Router) {
    let temp = init_loaded_graph().await;
    let graph = graph_path(temp.path());
    let policy = temp.path().join("policy.yaml");
    std::fs::write(&policy,"version: 1\ngroups:\n  readers: [\"principal:stable_alice\"]\nrules:\n  - id: alice\n    allow:\n      actors: {group: readers}\n      actions: [read, change]\n      branch_scope: any\n  - id: invoke\n    allow:\n      actors: {group: readers}\n      actions: [invoke_query]\n").unwrap();
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
    .unwrap()
    .with_oidc_identity_trust(trust(temp.path()));
    (temp, omnigraph_server::build_app(state))
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
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn oidc_mcp_reuses_discovery_cedar_and_stored_read_handlers_without_mutations() {
    let (temp, app) = fixture_app().await;
    let alice = token("alice", RESOURCE);
    let bob = token("bob", RESOURCE);
    let response=app.clone().oneshot(rpc(Some(&alice),"initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("mcp-session-id"));
    let (status, list) = json_response(&app, rpc(Some(&alice), "tools/list", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let names: Vec<_> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["graphs", "queries", "query"]);
    for tool in list["result"]["tools"].as_array().unwrap() {
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
    }
    let discovered = call(&app, &bob, "graphs", json!({})).await;
    assert_eq!(
        discovered["result"]["structuredContent"],
        json!({"graphs":[{"graph_id":"default","display_name":"default"}]})
    );
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
    let (_temp, app) = fixture_app().await;
    let response = app
        .clone()
        .oneshot(rpc(None, "tools/list", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()["www-authenticate"],
        "Bearer resource_metadata=\"https://data.example/.well-known/oauth-protected-resource/clusters/A/incarnations/one\""
    );
    let (status, metadata) = json_response(
        &app,
        Request::get("/.well-known/oauth-protected-resource")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metadata["resource"], RESOURCE);
    assert!(!metadata.to_string().contains("stable_alice"));
    let alice = token("alice", RESOURCE);
    for wrong in [
        token("alice", "https://data.example/clusters/B/incarnations/one"),
        token("unknown", RESOURCE),
        "ogc_retired".into(),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(rpc(Some(&wrong), "tools/list", json!({})))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
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
async fn mcp_requires_explicit_resource_identity_configuration() {
    let (_temp, app) = app_for_loaded_graph().await;
    assert_eq!(
        app.clone()
            .oneshot(rpc(None, "tools/list", json!({})))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.oneshot(
            Request::get("/.well-known/oauth-protected-resource")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::NOT_FOUND
    );
}
