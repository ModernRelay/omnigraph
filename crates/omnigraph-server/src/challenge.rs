//! The bearer challenge every 401 carries (RFC 9110 §15.5.2, RFC 6750 §3).
//!
//! Authentication reads a request's `Authorization` header once
//! ([`bearer_token`]) and records on its 401 whether a bearer credential was
//! presented ([`Unauthenticated`]). One layer outside every router
//! ([`answer`]) renders that record with the server's [`Challenge`], fixed at
//! boot, on every 401 response, including a 401 that no authentication
//! decision produced.

use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::ApiError;

/// Why a request has no authenticated actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unauthenticated {
    /// The request presented no bearer credential.
    NoCredential,
    /// The request presented a bearer credential the server refused
    /// (RFC 6750 §3.1 `invalid_token`).
    Refused,
}

/// The token of the request's one bearer credential.
///
/// The scheme is case-insensitive (RFC 9110 §11.1) and one or more spaces
/// separate it from the token (RFC 6750 §2.1). A missing header, several
/// `Authorization` headers, another scheme or a `Bearer` without a token
/// presents no single credential, so the client is asked for one; nothing was
/// checked that it should refresh. A bearer value that is not visible ASCII is
/// presented and refused.
pub(crate) fn bearer_token(headers: &HeaderMap) -> Result<&str, ApiError> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(ApiError::unauthorized(
            Unauthenticated::NoCredential,
            "one bearer credential is required",
        ));
    };
    let Some(token) = token_bytes(value.as_bytes()) else {
        return Err(ApiError::unauthorized(
            Unauthenticated::NoCredential,
            "missing bearer token",
        ));
    };
    // `to_str` admits visible ASCII, spaces and tabs, so the token is text.
    value
        .to_str()
        .ok()
        .and_then(|_| std::str::from_utf8(token).ok())
        .ok_or_else(|| ApiError::unauthorized(Unauthenticated::Refused, "invalid bearer token"))
}

/// The bytes after a case-insensitive `Bearer` and one or more spaces, or
/// `None` when the value presents no bearer token.
fn token_bytes(value: &[u8]) -> Option<&[u8]> {
    let (scheme, rest) = value.split_at_checked(b"bearer".len())?;
    if !scheme.eq_ignore_ascii_case(b"bearer") {
        return None;
    }
    let mut token = rest.strip_prefix(b" ")?;
    while let Some(rest) = token.strip_prefix(b" ") {
        token = rest;
    }
    (!token.is_empty()).then_some(token)
}

/// The server's two challenges, built once from its fixed OIDC resource
/// identifier, never from a request's `Host`, `Origin` or path.
#[derive(Clone)]
pub(crate) struct Challenge {
    no_credential: HeaderValue,
    refused: HeaderValue,
}

impl Challenge {
    /// `Bearer`, with `resource_metadata` naming the protected-resource
    /// metadata when the server has an OIDC resource identifier.
    pub(crate) fn new(resource: Option<&str>) -> Self {
        let metadata = resource.and_then(crate::mcp::advertised_metadata_url);
        let render = |error: &str| {
            metadata.as_ref().and_then(|url| {
                HeaderValue::from_str(&format!("Bearer {error}resource_metadata=\"{url}\"")).ok()
            })
        };
        Self {
            no_credential: render("").unwrap_or(HeaderValue::from_static("Bearer")),
            refused: render("error=\"invalid_token\", ")
                .unwrap_or(HeaderValue::from_static("Bearer error=\"invalid_token\"")),
        }
    }

    fn header(&self, reason: Unauthenticated) -> HeaderValue {
        match reason {
            Unauthenticated::NoCredential => self.no_credential.clone(),
            Unauthenticated::Refused => self.refused.clone(),
        }
    }
}

/// Outside every router: every 401 carries the challenge. A 401 without an
/// authentication record answers as if no credential was presented.
pub(crate) async fn answer(
    State(challenge): State<Challenge>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        let reason = response
            .extensions()
            .get::<Unauthenticated>()
            .copied()
            .unwrap_or(Unauthenticated::NoCredential);
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, challenge.header(reason));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(values: &[&[u8]]) -> Result<String, (Unauthenticated, String)> {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(AUTHORIZATION, HeaderValue::from_bytes(value).unwrap());
        }
        bearer_token(&headers).map(str::to_owned).map_err(|error| {
            assert_eq!(error.status(), StatusCode::UNAUTHORIZED);
            let Some(crate::ApiErrorDetails::Unauthenticated(reason)) = error.details.as_deref()
            else {
                panic!("a 401 without its reason: {error:?}");
            };
            (*reason, error.message.to_string())
        })
    }

    #[test]
    fn the_scheme_is_case_insensitive_and_spaces_separate_the_token_issue_1020() {
        for value in [
            &b"Bearer secret"[..],
            b"bearer secret",
            b"BEARER secret",
            b"bEaReR   secret",
        ] {
            assert_eq!(read(&[value]).unwrap(), "secret", "{value:?}");
        }
        // The token keeps its own bytes after the separator.
        assert_eq!(read(&[b"Bearer a b"]).unwrap(), "a b");
    }

    #[test]
    fn only_a_presented_bearer_token_is_refused_issue_1020() {
        let none = Unauthenticated::NoCredential;
        let refused = Unauthenticated::Refused;
        for (values, reason) in [
            (&[][..], none),
            (&[&b"Basic YWxpY2U6eA=="[..]][..], none),
            (&[&b"Bearer"[..]][..], none),
            (&[&b"Bearersecret"[..]][..], none),
            (&[&b"Bearer\tsecret"[..]][..], none),
            (&[&b"Basic a"[..], &b"Basic b"[..]][..], none),
            // Several values are not a single credential, so none was refused.
            (&[&b"Bearer a"[..], &b"Bearer a"[..]][..], none),
            (&[&b"Basic a"[..], &b"bearer b"[..]][..], none),
            (&[&b"Bearer \xff"[..]][..], refused),
        ] {
            assert_eq!(read(values).unwrap_err().0, reason, "{values:?}");
        }
    }

    #[test]
    fn the_challenge_names_metadata_only_with_a_resource_issue_1020() {
        let bare = Challenge::new(None);
        assert_eq!(bare.header(Unauthenticated::NoCredential), "Bearer");
        assert_eq!(
            bare.header(Unauthenticated::Refused),
            "Bearer error=\"invalid_token\""
        );
        let oidc = Challenge::new(Some("https://graphs.example.com/mcp"));
        assert_eq!(
            oidc.header(Unauthenticated::NoCredential),
            "Bearer resource_metadata=\"https://graphs.example.com/.well-known/oauth-protected-resource/mcp\""
        );
        assert_eq!(
            oidc.header(Unauthenticated::Refused),
            "Bearer error=\"invalid_token\", resource_metadata=\"https://graphs.example.com/.well-known/oauth-protected-resource/mcp\""
        );
        // An identifier that is not a URL advertises no metadata.
        assert_eq!(
            Challenge::new(Some("not a url")).header(Unauthenticated::NoCredential),
            "Bearer"
        );
    }
}
