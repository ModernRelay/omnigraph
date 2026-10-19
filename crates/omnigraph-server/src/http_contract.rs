//! Admission for the single supported OmniGraph HTTP API contract.
//! Authentication runs first; this check precedes graph resolution and body work.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use utoipa::openapi::header::Header;
use utoipa::openapi::path::{Operation, Parameter, ParameterIn};
use utoipa::openapi::schema::{Object, Type};
use utoipa::openapi::{Content, Ref, RefOr};

use crate::ApiError;
use crate::api::{EXPECT_ACTOR_HEADER, ErrorCode, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};

/// The Title-Case spelling the published description and messages give a
/// header. HTTP header names are case-insensitive; the constants stay
/// lowercase because `HeaderName::from_static` accepts nothing else.
pub(crate) fn display_header_name(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + chars.as_str()
            })
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn mismatch_message() -> String {
    format!(
        "exactly one {} header with value {HTTP_API_CONTRACT} is required",
        display_header_name(HTTP_API_CONTRACT_HEADER)
    )
}

/// The error body a path's refusals carry: the change routes' projection, or
/// the full error body.
fn error_schema(path: &str) -> &'static str {
    if path.contains("/changes") || path.ends_with("/diff") || path.ends_with("/merge-base") {
        "ChangeErrorOutput"
    } else {
        "ErrorOutput"
    }
}

/// Document `status` on `operation`, or add `description` to the response it
/// already documents, which keeps that response's own body. A response the
/// operation names by reference is left as it is.
fn document_error<'a>(
    operation: &'a mut Operation,
    path: &str,
    status: &str,
    description: &str,
) -> Option<&'a mut utoipa::openapi::Response> {
    let response = operation
        .responses
        .responses
        .entry(status.into())
        .or_insert_with(|| {
            let mut response = utoipa::openapi::Response::new(description);
            response.content.insert(
                "application/json".into(),
                Content::new(Some(Ref::from_schema_name(error_schema(path)))),
            );
            response.into()
        });
    let RefOr::T(response) = response else {
        return None;
    };
    if !response.description.contains(description) {
        response.description.push_str("; ");
        response.description.push_str(description);
    }
    Some(response)
}

fn retry_after_header() -> Header {
    let mut header = Header::new(Object::with_type(Type::String));
    header.description = Some(
        "Minimum delay before a caller-bounded retry of this refused request; not a scheduled server retry."
            .into(),
    );
    header
}

pub(crate) async fn require_contract(request: Request, next: Next) -> Result<Response, ApiError> {
    let mut values = request.headers().get_all(HTTP_API_CONTRACT_HEADER).iter();
    if !matches!(values.next(), Some(value) if value.as_bytes() == HTTP_API_CONTRACT.as_bytes())
        || values.next().is_some()
    {
        return Err(ApiError::new(
            ErrorCode::ApiContractMismatch,
            mismatch_message(),
        ));
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

pub(crate) fn describe_contract(doc: &mut utoipa::openapi::OpenApi) {
    let mut schema = Object::with_type(Type::String);
    schema.enum_values = Some(vec![HTTP_API_CONTRACT.into()]);
    let contract_header = display_header_name(HTTP_API_CONTRACT_HEADER);
    let expect_actor_header = display_header_name(EXPECT_ACTOR_HEADER);
    for (path, item) in &mut doc.paths.paths {
        // OAuth resource metadata follows its standard protocol, as does MCP.
        if crate::mcp::is_resource_metadata_path(path) {
            continue;
        }
        let protected =
            path == "/graphs" || path.starts_with("/graphs/") || path.starts_with("/cluster/");
        let per_graph = path.starts_with("/graphs/{graph_id}/");
        for operation in crate::handlers::path_item_operations_mut(item) {
            if protected {
                let mut parameter = Parameter::new(contract_header.as_str());
                parameter.parameter_in = ParameterIn::Header;
                parameter.required = utoipa::openapi::Required::True;
                parameter.schema = Some(schema.clone().into());
                parameter.description = Some(mismatch_message());
                operation
                    .parameters
                    .get_or_insert_with(Vec::new)
                    .push(parameter);
                let mut expected_actor = Parameter::new(expect_actor_header.as_str());
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
                document_error(
                    operation,
                    path,
                    "412",
                    &format!(
                        "actor_mismatch when {expect_actor_header} names another actor than the credential resolves to"
                    ),
                );
                document_error(
                    operation,
                    path,
                    "400",
                    "api_contract_mismatch when the required API contract header is missing, duplicated or unsupported",
                );
                // The router's own refusals reach every protected operation;
                // a path under a graph also names a graph that may not be
                // served.
                document_error(
                    operation,
                    path,
                    "404",
                    if per_graph {
                        "not_found when no graph `graph_id` is served or the caller may not see it, or when no route serves the path"
                    } else {
                        "not_found when no route serves the path"
                    },
                );
                if let Some(response) = document_error(
                    operation,
                    path,
                    "405",
                    "method_not_allowed when the path is served, but not with this method",
                ) {
                    let mut allow = Header::new(Object::with_type(Type::String));
                    allow.description = Some("The methods the path serves.".into());
                    response.headers.insert("Allow".into(), allow);
                }
            }
            if per_graph || path.starts_with("/cluster/") {
                for (status, description) in [
                    (
                        "429",
                        "too_many_requests when server observer, ingress or operation admission capacity is exhausted; honor Retry-After",
                    ),
                    (
                        "503",
                        "graph_unavailable for a known graph that is not served, or service_unavailable when server operation admission is closed; honor Retry-After, and reconcile any earlier write before retrying",
                    ),
                    (
                        "408",
                        "request_timeout when the request body deadline passes before operation admission",
                    ),
                    (
                        "413",
                        "payload_too_large when the request body exceeds its route limit before operation admission",
                    ),
                ] {
                    if matches!(status, "408" | "413") && operation.request_body.is_none() {
                        continue;
                    }
                    if let Some(response) = document_error(operation, path, status, description)
                        && matches!(status, "429" | "503")
                    {
                        response
                            .headers
                            .insert("Retry-After".into(), retry_after_header());
                    }
                }
            }
            for response in operation.responses.responses.values_mut() {
                if let RefOr::T(response) = response {
                    let mut header = Header::new(schema.clone());
                    header.description = Some("Supported OmniGraph HTTP API contract.".into());
                    response.headers.insert(contract_header.clone(), header);
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
