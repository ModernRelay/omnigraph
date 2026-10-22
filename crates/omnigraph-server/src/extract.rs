//! Request extractors that refuse with an [`ApiError`], so a malformed body,
//! query string or path parameter answers the error body every other refusal
//! does, with its code and outcome. Handlers take these instead of axum's own
//! `Json`, `Query` and `Path`, whose rejections are plain text.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::ApiError;
use crate::api::ErrorCode;

/// A JSON request body.
pub(crate) struct ApiJson<T>(pub(crate) T);

/// The request's query string.
pub(crate) struct ApiQuery<T>(pub(crate) T);

/// The route's path parameters.
pub(crate) struct ApiPath<T>(pub(crate) T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, ApiError> {
        let axum::Json(value) = axum::Json::from_request(request, state)
            .await
            .map_err(json_rejection)?;
        Ok(Self(value))
    }
}

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        let axum::extract::Query(value) = axum::extract::Query::from_request_parts(parts, state)
            .await
            .map_err(query_rejection)?;
        Ok(Self(value))
    }
}

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        let axum::extract::Path(value) = axum::extract::Path::from_request_parts(parts, state)
            .await
            .map_err(path_rejection)?;
        Ok(Self(value))
    }
}

/// Axum refuses a body for its data or syntax (400, and 422 for a serde
/// data-shape failure, which OmniGraph answers as its typed 400), its size
/// over the route's limit (413), or a missing or wrong JSON `Content-Type`
/// (415). Each keeps its own code and status.
fn json_rejection(rejection: JsonRejection) -> ApiError {
    let code = match rejection.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ErrorCode::PayloadTooLarge,
        StatusCode::UNSUPPORTED_MEDIA_TYPE => ErrorCode::UnsupportedMediaType,
        _ => ErrorCode::BadRequest,
    };
    ApiError::new(
        code,
        format!("invalid request body: {}", rejection.body_text()),
    )
}

fn query_rejection(rejection: QueryRejection) -> ApiError {
    ApiError::bad_request(format!("invalid query string: {}", rejection.body_text()))
}

/// A parameter that does not decode is the caller's error; a route whose
/// parameters do not match its extractor is the server's.
fn path_rejection(rejection: PathRejection) -> ApiError {
    if rejection.status().is_client_error() {
        ApiError::bad_request(format!("invalid path parameter: {}", rejection.body_text()))
    } else {
        ApiError::internal(format!(
            "route parameters do not match their extractor: {}",
            rejection.body_text()
        ))
    }
}

#[cfg(test)]
mod tests {
    /// A handler that takes axum's own extractor answers a malformed request
    /// in plain text, outside the error contract, or maps its refusal by
    /// hand, where a body's 413 or 415 can lose its status. Its parameter may
    /// hide inside `Result` or `Option`; its rejection type names the hand
    /// mapping wherever it is.
    #[test]
    fn handlers_take_extractors_that_refuse_with_the_error_body() {
        let mut offenders = Vec::new();
        let mut dirs = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).unwrap();
                let this_module = path.ends_with("extract.rs");
                for (number, line) in source.lines().enumerate() {
                    let code = line.split("//").next().unwrap_or_default();
                    let code = code
                        .replace("axum::extract::rejection::", "")
                        .replace("axum::extract::", "")
                        .replace("axum::", "")
                        .replace("std::result::", "")
                        .replace(' ', "");
                    let parameter = ["Json", "Query", "Path"].iter().any(|extractor| {
                        ["", "Result<", "Option<"]
                            .iter()
                            .any(|wrapper| code.contains(&format!(":{wrapper}{extractor}<")))
                    });
                    let rejection = !this_module
                        && ["JsonRejection", "QueryRejection", "PathRejection"]
                            .iter()
                            .any(|rejection| code.contains(rejection));
                    if parameter || rejection {
                        offenders.push(format!(
                            "{}:{}: {}",
                            path.display(),
                            number + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "take ApiJson, ApiQuery or ApiPath instead:\n{}",
            offenders.join("\n")
        );
    }
}
