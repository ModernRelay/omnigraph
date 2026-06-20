//! The `omnigraph-mcp` crate stands alone: a trivial `McpBackend` drives the
//! real transport with no omnigraph dependency. Also the **rmcp surface guard**
//! — the first smoke check on any rmcp version bump (mirrors the engine's
//! `lance_surface_guards.rs`).

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use omnigraph_mcp::{
    CallToolResult, Content, Implementation, McpBackend, McpError, McpHostPolicy, OriginPolicy,
    ReadResourceResult, Resource, ServerCapabilities, ServerInfo, Tool, mcp_router,
};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Clone)]
struct Dummy;

#[async_trait]
impl McpBackend for Dummy {
    fn server_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new("omnigraph-test", "0.0.0"))
    }

    async fn list_tools(&self, _parts: &http::request::Parts) -> Result<Vec<Tool>, McpError> {
        let schema = json!({ "type": "object", "properties": {}, "additionalProperties": false });
        let schema = schema.as_object().unwrap().clone();
        Ok(vec![Tool::new(
            "graph_health",
            "Liveness probe.",
            Arc::new(schema),
        )])
    }

    async fn call_tool(
        &self,
        _parts: &http::request::Parts,
        name: &str,
        _args: omnigraph_mcp::JsonObject,
    ) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text(format!(
            "called {name}"
        ))]))
    }

    async fn list_resources(
        &self,
        _parts: &http::request::Parts,
    ) -> Result<Vec<Resource>, McpError> {
        Ok(vec![])
    }

    async fn read_resource(
        &self,
        _parts: &http::request::Parts,
        _uri: &str,
    ) -> Result<ReadResourceResult, McpError> {
        Err(McpError::invalid_params("no resources", None))
    }
}

fn loopback_router() -> axum::Router {
    let policy = McpHostPolicy::from_bind(&"127.0.0.1:0".parse().unwrap(), &[], &[]);
    mcp_router(Dummy, 1 << 20, policy)
}

fn mcp_post(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn initialize_advertises_tools_and_resources() {
    let resp = loopback_router()
        .oneshot(mcp_post(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0" }
            }
        })))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["result"]["serverInfo"]["name"], "omnigraph-test");
    assert!(v["result"]["capabilities"]["tools"].is_object());
    assert!(v["result"]["capabilities"]["resources"].is_object());
}

#[tokio::test]
async fn tools_list_returns_full_set_with_no_next_cursor() {
    let resp = loopback_router()
        .oneshot(mcp_post(json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
        })))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    let tools = v["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "graph_health");
    // Non-paginated by contract: nextCursor is absent (None ⇒ omitted).
    assert!(v["result"]["nextCursor"].is_null());
}

#[tokio::test]
async fn get_is_method_not_allowed() {
    let resp = loopback_router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mcp")
                .header(header::HOST, "localhost")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(resp.headers()[header::ALLOW], "POST");
}

#[tokio::test]
async fn deny_browsers_rejects_present_origin_but_allows_absent() {
    // A remote-shaped policy: any present Origin is forbidden; absent passes.
    let policy = McpHostPolicy {
        allowed_hosts: None,
        origin: OriginPolicy::DenyBrowsers,
    };
    let router: axum::Router = mcp_router(Dummy, 1 << 20, policy);

    let init = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": { "name": "t", "version": "0" } }
    });

    // Present, disallowed Origin → 403 (origin_guard, not rmcp's empty-list path).
    let mut with_origin = mcp_post(init.clone());
    with_origin
        .headers_mut()
        .insert(header::ORIGIN, "https://evil.example".parse().unwrap());
    let resp = router.clone().oneshot(with_origin).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Absent Origin → 200 (non-browser MCP clients send none).
    let resp = router.oneshot(mcp_post(init)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn loopback_bind_allows_all_loopback_host_forms() {
    // A loopback bind must accept *every* loopback Host form — 127.0.0.1, [::1],
    // and localhost — regardless of which stack it bound. The Host header is
    // independent of the socket (in-process, reverse proxies, dual-stack
    // `localhost`), so a `127.0.0.1`-bound server must still accept a `[::1]`
    // Host. (Matches rmcp's default loopback set; deriving the list from the
    // bound IP alone dropped the sibling-stack literal and 403'd the client.)
    for bind in ["127.0.0.1:8080", "[::1]:8080"] {
        let policy = McpHostPolicy::from_bind(&bind.parse().unwrap(), &[], &[]);
        let hosts = policy.allowed_hosts.clone().unwrap();
        for expected in ["127.0.0.1", "::1", "localhost"] {
            assert!(
                hosts.iter().any(|h| h == expected),
                "bind {bind}: loopback allowlist missing {expected}: {hosts:?}"
            );
        }
        // e2e: the IPv6 loopback Host is accepted even on the IPv4 bind.
        let router: axum::Router = mcp_router(Dummy, 1 << 20, policy);
        let mut req = mcp_post(json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-11-25", "capabilities": {},
                        "clientInfo": { "name": "t", "version": "0" } }
        }));
        req.headers_mut().insert(header::HOST, "[::1]:8080".parse().unwrap());
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "bind {bind}: IPv6 loopback Host rejected");
    }
}

#[tokio::test]
async fn unsupported_protocol_version_header_is_400_except_on_initialize() {
    // rmcp validates `MCP-Protocol-Version` on non-`initialize` requests only:
    // `initialize` negotiates the version in its JSON-RPC body, so a bogus header
    // there is ignored (200), while the same header on a follow-up request is a
    // 400. Pins the real contract (the transport doc-comment notes this).
    // `initialize` + bogus version header → 200 (header not validated on init).
    let mut init = mcp_post(json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": { "name": "t", "version": "0" } }
    }));
    init.headers_mut()
        .insert("mcp-protocol-version", "1900-01-01".parse().unwrap());
    let resp = loopback_router().oneshot(init).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "initialize must not validate the version header");

    // A follow-up request (`tools/list`) + bogus version header → 400.
    let mut list = mcp_post(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }));
    list.headers_mut()
        .insert("mcp-protocol-version", "1900-01-01".parse().unwrap());
    let resp = loopback_router().oneshot(list).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "non-init bogus version must be 400");
}

#[test]
fn loopback_host_set_covers_rmcp_default() {
    // Construction guard for the `::1` regression: our loopback Host allow-list
    // must stay a SUPERSET of rmcp's own default loopback set. We keep an
    // explicit list (rather than deriving) for clarity, but pin it against the
    // substrate default here — so an rmcp bump that adds a loopback form turns
    // this red instead of silently 403'ing that client.
    use rmcp::transport::streamable_http_server::StreamableHttpServerConfig;
    let rmcp_default = StreamableHttpServerConfig::default().allowed_hosts;
    assert!(!rmcp_default.is_empty(), "rmcp default should list loopback hosts");
    let ours = McpHostPolicy::from_bind(&"127.0.0.1:0".parse().unwrap(), &[], &[])
        .allowed_hosts
        .expect("a loopback bind sets a Host allow-list");
    for host in &rmcp_default {
        assert!(
            ours.contains(host),
            "loopback allow-list {ours:?} must cover rmcp default host {host:?}"
        );
    }
}

/// rmcp surface guard — pins the API shapes the transport relies on. Turns red
/// (compile error) on an rmcp bump that renames/moves any of these. Compile-only.
#[allow(dead_code)]
fn _rmcp_surface_guard() {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, session::never::NeverSessionManager,
    };
    let _config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true)
        .disable_allowed_hosts()
        .with_allowed_origins(["https://app.example".to_string()]);
    let _session = NeverSessionManager::default();
    // The Parts passthrough: rmcp's RequestContext extensions hold the HTTP parts.
    fn _reads_parts(ctx: &rmcp::service::RequestContext<rmcp::service::RoleServer>) {
        let _parts: Option<&http::request::Parts> = ctx.extensions.get::<http::request::Parts>();
    }
}
