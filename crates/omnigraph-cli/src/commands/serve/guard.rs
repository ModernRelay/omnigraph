//! The loopback guard of a local server, outside every server layer.
//!
//! A request's `Host` must name this server's loopback address exactly, which
//! closes DNS rebinding: a rebound page's request is same-origin and carries
//! no `Origin`, but its `Host` is the attacker's name. Any `Origin` is refused,
//! since native clients send none and a local server serves no page. No
//! response carries a CORS header, and every one carries
//! `Cross-Origin-Resource-Policy: same-origin`. Nothing relies on `Sec-Fetch-*`.

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use omnigraph_api_types::{
    ErrorCode, ErrorOutput, OutcomeAction, OutcomeEffects, OutcomeExecution, OutcomeOutput,
};

const CORP: &str = "cross-origin-resource-policy";

pub(super) fn loopback_only(router: Router, port: u16) -> Router {
    router.layer(axum::middleware::from_fn(
        move |request: Request, next: Next| guard(port, request, next),
    ))
}

/// Whether `host` names this server: `127.0.0.1`, `localhost` or `[::1]`
/// with its port, compared without case.
fn loopback_host(host: &str, port: u16) -> bool {
    let host = host.to_ascii_lowercase();
    [
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
    ]
    .contains(&host)
}

fn refuse(message: &str) -> Response {
    let mut output = ErrorOutput::message(message.to_string());
    output.code = Some(ErrorCode::Forbidden);
    output.outcome = Some(OutcomeOutput {
        execution: OutcomeExecution::NotStarted,
        effects: OutcomeEffects::None,
        action: OutcomeAction::Refresh,
    });
    let mut response = (StatusCode::FORBIDDEN, axum::Json(output)).into_response();
    response
        .headers_mut()
        .insert(CORP, HeaderValue::from_static("same-origin"));
    response
}

async fn guard(port: u16, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let hosts: Vec<&HeaderValue> = headers.get_all(header::HOST).iter().collect();
    let host_ok = match hosts.as_slice() {
        [host] => host.to_str().is_ok_and(|host| loopback_host(host, port)),
        // HTTP/2 names the authority in the request target instead.
        [] => request
            .uri()
            .authority()
            .is_some_and(|authority| loopback_host(authority.as_str(), port)),
        _ => false,
    };
    if !host_ok {
        return refuse(&format!(
            "this local server answers only requests to 127.0.0.1:{port}, localhost:{port} or [::1]:{port}"
        ));
    }
    if headers.contains_key(header::ORIGIN) {
        return refuse("this local server refuses browser requests");
    }
    let mut response = next.run(request).await;
    let cors: Vec<_> = response
        .headers()
        .keys()
        .filter(|name| name.as_str().starts_with("access-control-"))
        .cloned()
        .collect();
    for name in cors {
        response.headers_mut().remove(name);
    }
    response
        .headers_mut()
        .insert(CORP, HeaderValue::from_static("same-origin"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt as _;

    async fn status(
        host: Option<&[u8]>,
        origin: Option<&str>,
    ) -> (StatusCode, Option<HeaderValue>) {
        let app = loopback_only(
            Router::new().route("/healthz", get(|| async { "ok" })),
            4242,
        );
        let mut request = axum::http::Request::builder().uri("/healthz");
        if let Some(host) = host {
            request = request.header(header::HOST, HeaderValue::from_bytes(host).unwrap());
        }
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        (response.status(), response.headers().get(CORP).cloned())
    }

    #[tokio::test]
    async fn only_this_servers_loopback_name_reaches_it_and_no_browser_does() {
        for host in [&b"127.0.0.1:4242"[..], b"LOCALHOST:4242", b"[::1]:4242"] {
            let (status, corp) = status(Some(host), None).await;
            assert_eq!(status, StatusCode::OK, "{host:?}");
            assert_eq!(corp.unwrap(), "same-origin");
        }
        for host in [
            &b"evil.example:4242"[..],
            b"127.0.0.1:4243",
            b"localhost",
            b"127.0.0.1.evil.example:4242",
            b"\xff\xfe",
        ] {
            let (status, corp) = status(Some(host), None).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{host:?}");
            assert_eq!(corp.unwrap(), "same-origin");
        }
        assert_eq!(status(None, None).await.0, StatusCode::FORBIDDEN);
        for origin in ["http://localhost:4242", "null", "https://evil.example"] {
            let (status, _) = status(Some(b"127.0.0.1:4242"), Some(origin)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{origin}");
        }
    }
}
