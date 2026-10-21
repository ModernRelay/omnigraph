//! Admission for the single supported OmniGraph HTTP API contract.
//! Authentication runs first; this check precedes graph resolution and body work.

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use utoipa::openapi::header::Header;
use utoipa::openapi::path::{Parameter, ParameterIn};
use utoipa::openapi::schema::{Object, Type};
use utoipa::openapi::{Content, Ref, RefOr};

use crate::ApiError;
use crate::api::{ErrorCode, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER, IF_ACTOR_HEADER};

fn mismatch_message() -> String {
    format!(
        "exactly one {HTTP_API_CONTRACT_HEADER} header with value {HTTP_API_CONTRACT} is required"
    )
}

pub(crate) async fn require_contract(request: Request, next: Next) -> Result<Response, ApiError> {
    let mut values = request.headers().get_all(HTTP_API_CONTRACT_HEADER).iter();
    if !matches!(values.next(), Some(value) if value.as_bytes() == HTTP_API_CONTRACT.as_bytes())
        || values.next().is_some()
    {
        return Err(ApiError {
            completion_uncertain: false,
            status: StatusCode::BAD_REQUEST,
            code: Some(ErrorCode::ApiContractMismatch),
            message: mismatch_message().into_boxed_str(),
            details: None,
        });
    }
    Ok(next.run(request).await)
}

pub(crate) async fn identify_response(request: Request, next: Next) -> Response {
    let path = request.uri().path();
    if path == "/mcp" || crate::mcp::is_resource_metadata_path(path) {
        return next.run(request).await;
    }
    let discovery = path == "/healthz";
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        HTTP_API_CONTRACT_HEADER,
        HeaderValue::from_static(HTTP_API_CONTRACT),
    );
    if discovery {
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
    }
    response
}

/// What the `WWW-Authenticate` header of every protected 401 carries.
const CHALLENGE_DESCRIPTION: &str = "Bearer challenge (RFC 6750 §3). It carries error=\"invalid_token\" when a presented bearer credential was refused, and resource_metadata naming the OAuth protected-resource metadata when the server has an OIDC resource identity.";

/// A refusal documented with the route's error envelope: change routes use
/// their narrower graph-vocabulary projection.
fn error_response(path: &str, description: &str) -> RefOr<utoipa::openapi::Response> {
    let mut response = utoipa::openapi::Response::new(description);
    response.content.insert(
        "application/json".into(),
        Content::new(Some(Ref::from_schema_name(
            if path.contains("/changes") || path.ends_with("/diff") || path.ends_with("/merge-base")
            {
                "ChangeErrorOutput"
            } else {
                "ErrorOutput"
            },
        ))),
    );
    response.into()
}

pub(crate) fn describe_contract(doc: &mut utoipa::openapi::OpenApi) {
    let mut schema = Object::with_type(Type::String);
    schema.enum_values = Some(vec![HTTP_API_CONTRACT.into()]);
    for (path, item) in &mut doc.paths.paths {
        // OAuth resource metadata follows its standard protocol, as does MCP.
        if crate::mcp::is_resource_metadata_path(path) {
            continue;
        }
        let protected =
            path == "/graphs" || path.starts_with("/graphs/") || path.starts_with("/cluster/");
        for operation in crate::handlers::path_item_operations_mut(item) {
            if protected {
                let mut parameter = Parameter::new(HTTP_API_CONTRACT_HEADER);
                parameter.parameter_in = ParameterIn::Header;
                parameter.required = utoipa::openapi::Required::True;
                parameter.schema = Some(schema.clone().into());
                parameter.description = Some(mismatch_message());
                operation
                    .parameters
                    .get_or_insert_with(Vec::new)
                    .push(parameter);
                let mut expected_actor = Parameter::new(IF_ACTOR_HEADER);
                expected_actor.parameter_in = ParameterIn::Header;
                expected_actor.required = utoipa::openapi::Required::False;
                expected_actor.schema = Some(Object::with_type(Type::String).into());
                expected_actor.description = Some(
                    "The actor the caller expects its credential to resolve to. A mismatch is refused with 412 actor_mismatch before graph lookup or effects; a match changes nothing."
                        .to_string(),
                );
                operation
                    .parameters
                    .get_or_insert_with(Vec::new)
                    .push(expected_actor);
                let response = operation
                    .responses
                    .responses
                    .entry("412".into())
                    .or_insert_with(|| error_response(path, "Precondition failed"));
                if let RefOr::T(response) = response {
                    response.description.push_str(
                        "; actor_mismatch when omnigraph-if-actor names another actor than the credential resolves to",
                    );
                }
                let response = operation
                    .responses
                    .responses
                    .entry("400".into())
                    .or_insert_with(|| error_response(path, "Invalid request"));
                if let RefOr::T(response) = response {
                    response.description.push_str(
                        "; api_contract_mismatch when the required API contract header is missing, duplicated or unsupported",
                    );
                }
                // Authentication guards every protected router, and every 401
                // carries the challenge.
                let response = operation
                    .responses
                    .responses
                    .entry("401".into())
                    .or_insert_with(|| error_response(path, "Unauthorized"));
                if let RefOr::T(response) = response {
                    let mut header = Header::new(Object::with_type(Type::String));
                    header.description = Some(CHALLENGE_DESCRIPTION.into());
                    response.headers.insert("WWW-Authenticate".into(), header);
                }
            }
            if path.starts_with("/graphs/{graph_id}/") || path.starts_with("/cluster/") {
                for (status, description) in [
                    (
                        "429",
                        "Server observer, ingress or operation admission capacity exhausted; honor Retry-After",
                    ),
                    (
                        "503",
                        "Known graph unavailable (graph_unavailable) or server operation admission closed; reconcile any earlier write before retrying",
                    ),
                    (
                        "408",
                        "Request body deadline exceeded before operation admission",
                    ),
                    (
                        "413",
                        "Request body exceeds its route limit before operation admission",
                    ),
                ] {
                    if matches!(status, "408" | "413") && operation.request_body.is_none() {
                        continue;
                    }
                    let response = operation
                        .responses
                        .responses
                        .entry(status.into())
                        .or_insert_with(|| error_response(path, description));
                    if let RefOr::T(response) = response {
                        if !response.description.contains(description) {
                            response.description.push_str("; ");
                            response.description.push_str(description);
                        }
                        if status == "429" {
                            let mut header = Header::new(Object::with_type(Type::String));
                            header.description = Some("Minimum delay before a caller-bounded retry of this refused request; not a scheduled server retry.".into());
                            response.headers.insert("Retry-After".into(), header);
                        }
                    }
                }
            }
            for response in operation.responses.responses.values_mut() {
                if let RefOr::T(response) = response {
                    let mut header = Header::new(schema.clone());
                    header.description = Some("Supported OmniGraph HTTP API contract.".into());
                    response
                        .headers
                        .insert(HTTP_API_CONTRACT_HEADER.into(), header);
                    if path == "/healthz" {
                        let mut cache_schema = Object::with_type(Type::String);
                        cache_schema.enum_values = Some(vec!["no-store".into()]);
                        response
                            .headers
                            .insert("Cache-Control".into(), Header::new(cache_schema));
                    }
                }
            }
        }
    }
    // Axum's GET route also serves HEAD. Declare the discovery operation
    // explicitly so clients can use its headers without fetching health JSON.
    if let Some(health) = doc.paths.paths.get_mut("/healthz")
        && let Some(mut head) = health.get.clone()
    {
        head.operation_id = Some("health_head".into());
        health.head = Some(head);
    }
    // HEAD has no response body even when shared admission adds an error.
    // Apply this after every modifier, including the generated health HEAD.
    for item in doc.paths.paths.values_mut() {
        let Some(head) = item.head.as_mut() else {
            continue;
        };
        for response in head.responses.responses.values_mut() {
            if let RefOr::T(response) = response {
                response.content.clear();
            }
        }
    }
}
