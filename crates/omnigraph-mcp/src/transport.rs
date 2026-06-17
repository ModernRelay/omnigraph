//! Stateless Streamable-HTTP transport for the MCP surface.
//!
//! One endpoint: `POST /mcp` returns a single `application/json` object; no SSE,
//! no session id (`NeverSessionManager` + `stateful_mode = false`). rmcp gives,
//! for free in stateless mode: `GET`/`DELETE → 405 + Allow: POST`, a disallowed
//! `Host → 403`, and an unsupported `MCP-Protocol-Version → 400` on
//! **non-`initialize`** requests. `initialize` is exempt by design — it
//! negotiates the version in its JSON-RPC body (`protocolVersion`), not the HTTP
//! header, so a bogus header there is ignored (absent ⇒ rmcp's default version).
//!
//! The one thing rmcp does **not** give is fail-closed Origin: it validates
//! `Origin` only when `allowed_origins` is non-empty (an empty list is
//! *fail-open*). [`origin_guard`] closes that — a present, disallowed `Origin`
//! is `403` regardless — and [`McpHostPolicy`] has no "absent ⇒ skip" state, so a
//! remote deployment cannot accidentally run fail-open.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};

use crate::McpBackend;
use crate::service::McpService;

/// Browser-`Origin` posture as a **total** choice — there is no `None ⇒ skip`
/// state to leak into a fail-open default. Every deployment lands in exactly one
/// arm, chosen once by [`McpHostPolicy::from_bind`].
#[derive(Debug, Clone)]
pub enum OriginPolicy {
    /// Browser clients from these origins; any OTHER present `Origin` → `403`.
    Allow(Vec<String>),
    /// No browser clients expected; ANY present `Origin` → `403`. Non-browser
    /// MCP clients (the launch tier) send no `Origin` and pass. The remote
    /// default.
    DenyBrowsers,
    /// Explicit opt-out (loopback dev / trusted network) — never the remote
    /// default.
    Unchecked,
}

/// Host + Origin posture, derived together from the deployment. The struct has
/// no skip-by-absence state, and [`from_bind`](Self::from_bind) is the only
/// constructor, so a fail-open policy is unrepresentable.
#[derive(Debug, Clone)]
pub struct McpHostPolicy {
    /// `None` ⇒ accept any `Host` (DNS-rebinding defense relaxed for a known
    /// public bind; bearer is the real control there).
    pub allowed_hosts: Option<Vec<String>>,
    /// Total — no `Option`.
    pub origin: OriginPolicy,
}

impl McpHostPolicy {
    /// The only constructor. Host and Origin posture are derived together from
    /// the bind + config, **fail-closed**: a remote bind with no configured
    /// origins is `DenyBrowsers` (a present `Origin` is rejected), NOT "skip".
    pub fn from_bind(bind: &SocketAddr, public_hosts: &[String], browser_origins: &[String]) -> Self {
        let loopback = bind.ip().is_loopback();
        Self {
            allowed_hosts: if loopback {
                // A loopback bind accepts every loopback Host form, not just the
                // stack it bound: the Host header is independent of the socket
                // (in-process tests, reverse proxies, dual-stack `localhost`
                // resolution), so a `127.0.0.1`-bound server must still accept a
                // `[::1]` Host and vice-versa. This mirrors rmcp's own default
                // loopback set; deriving the list from `bind.ip()` alone dropped
                // the sibling-stack literal and 403'd legitimate loopback clients.
                Some(vec!["127.0.0.1".into(), "::1".into(), "localhost".into()])
            } else if public_hosts.is_empty() {
                None
            } else {
                Some(public_hosts.to_vec())
            },
            origin: if !browser_origins.is_empty() {
                OriginPolicy::Allow(browser_origins.to_vec())
            } else if loopback {
                OriginPolicy::Unchecked
            } else {
                OriginPolicy::DenyBrowsers
            },
        }
    }
}

/// Fail-closed Origin enforcement, run BEFORE rmcp so it is independent of
/// rmcp's empty-`allowed_origins` fail-open semantics. A *present* `Origin` that
/// the policy disallows → `403`; an *absent* `Origin` always passes (non-browser
/// MCP clients send none); `Unchecked` is a no-op.
async fn origin_guard(State(origin): State<OriginPolicy>, request: Request, next: Next) -> Response {
    let header = request
        .headers()
        .get(http::header::ORIGIN)
        .and_then(|v| v.to_str().ok());
    let allowed = match header {
        None => true,
        Some(o) => match &origin {
            OriginPolicy::Unchecked => true,
            OriginPolicy::Allow(list) => list.iter().any(|a| a == o),
            OriginPolicy::DenyBrowsers => false,
        },
    };
    if allowed {
        next.run(request).await
    } else {
        (http::StatusCode::FORBIDDEN, "Forbidden: Origin not allowed").into_response()
    }
}

/// Build the `/mcp` router for a backend. The returned router carries its own
/// Origin guard and body-limit layer; merge (not `.route`) it into the
/// per-graph group so the body limit does not leak onto sibling routes.
///
/// Generic over the router state `S`: the `/mcp` route is a `route_service`
/// with no state-bearing extractors, so it composes with any caller's state
/// type (e.g. the server merges it into a `Router<AppState>` before
/// `.with_state`). A standalone caller pins `S = ()` via the return-type
/// annotation.
pub fn mcp_router<B, S>(backend: B, body_limit: usize, hosts: McpHostPolicy) -> axum::Router<S>
where
    B: McpBackend,
    S: Clone + Send + Sync + 'static,
{
    // `StreamableHttpServerConfig` is `#[non_exhaustive]`; its Default is
    // stateful_mode=true, json_response=false, allowed_hosts=loopback. Build
    // from Default and flip via the with_* setters for a remote stateless JSON
    // server.
    let mut config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true);
    config = match &hosts.allowed_hosts {
        Some(list) => config.with_allowed_hosts(list.clone()),
        None => config.disable_allowed_hosts(),
    };
    // `Allow` also configures rmcp as defense-in-depth; `DenyBrowsers` cannot be
    // expressed to rmcp (empty list ⇒ rmcp skips), so `origin_guard` is the
    // fail-closed authority.
    if let OriginPolicy::Allow(origins) = &hosts.origin {
        config = config.with_allowed_origins(origins.clone());
    }

    // service_factory returns Result<S, io::Error>; NeverSessionManager pairs
    // with stateless mode (rejects every session op).
    let svc = StreamableHttpService::new(
        move || Ok(McpService::new(backend.clone())),
        Arc::new(NeverSessionManager::default()),
        config,
    );

    axum::Router::<S>::new()
        .route_service("/mcp", svc)
        .layer(from_fn_with_state(hosts.origin, origin_guard))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(body_limit))
}
