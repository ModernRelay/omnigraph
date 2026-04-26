//! `X-Request-Id` middleware.
//!
//! Mints a ULID per inbound request, or echoes a caller-supplied
//! `X-Request-Id` header if it's well-formed. Stores the value in request
//! extensions so handlers can include it in error bodies, log lines, or
//! audit records, and surfaces it on the response header so SDK clients
//! can correlate logs across the wire.

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use ulid::Ulid;

pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Wraps a request id pulled out of (or minted into) request extensions.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Acceptable inbound `X-Request-Id` shape: 1..=128 ASCII printable chars.
/// Rejecting wider input keeps the value safe to log and emit verbatim.
fn is_valid_inbound(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 128
        && raw
            .bytes()
            .all(|b| b.is_ascii_graphic() || b == b' ' || b == b'-' || b == b'_')
}

pub async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    let inbound = req
        .headers()
        .get(&X_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|raw| is_valid_inbound(raw));

    let id = match inbound {
        Some(raw) => raw.to_owned(),
        None => Ulid::new().to_string(),
    };

    req.extensions_mut().insert(RequestId(id.clone()));

    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(X_REQUEST_ID, value);
    }
    response
}
