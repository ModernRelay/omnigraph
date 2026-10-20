//! Bounded result encoding and the canonical host-error projection.

use crate::RESULT_BYTES;
use axum::http::StatusCode;
use rmcp::model::CallToolResult;
use serde::Serialize;
use serde_json::{Value, json};

struct LimitedBytes(Vec<u8>);
impl std::io::Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > RESULT_BYTES - self.0.len() {
            return Err(std::io::Error::other("MCP result exceeds byte limit"));
        }
        self.0.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode a handler DTO directly under the MCP result byte bound.
/// The complete SDK result, including its text compatibility form, is bounded too.
pub fn bounded_result<T: Serialize>(value: &T) -> Result<CallToolResult, serde_json::Error> {
    let mut bytes = LimitedBytes(Vec::new());
    if serde_json::to_writer(&mut bytes, value).is_err() {
        return Ok(failure(
            "result_too_large",
            "The result exceeds 1 MiB; use a narrower stored query.",
        ));
    }
    let value = serde_json::from_slice(&bytes.0)?;
    Ok(fit_result(CallToolResult::structured(value)))
}

pub(super) fn fit_result(result: CallToolResult) -> CallToolResult {
    // Structured results also carry a text representation for older clients.
    // Bound the complete tool result, including that duplicate representation.
    if serde_json::to_writer(&mut LimitedBytes(Vec::new()), &result).is_err() {
        return failure(
            "result_too_large",
            "The result exceeds 1 MiB; use a narrower stored query.",
        );
    }
    result
}

pub(super) fn failure(code: &str, message: &str) -> CallToolResult {
    CallToolResult::structured_error(json!({"code":code,"message":message}))
}

pub(super) fn outcome_unknown(code: &str, message: &str) -> CallToolResult {
    CallToolResult::structured_error(json!({"code":code,"message":message,"outcome_unknown":true}))
}

/// The host's canonical HTTP error, projected once without an unbounded
/// intermediate JSON value. Completion evidence remains independent of status.
#[derive(Debug)]
pub struct HostError {
    pub(super) status: StatusCode,
    body: Option<Value>,
    completion_uncertain: bool,
}

impl HostError {
    /// Serialize the host's existing HTTP error DTO under the MCP byte bound.
    /// If encoding fails, callers receive the same generic refusal as an
    /// unreadable HTTP error response; resource classification retains status.
    pub fn from_output<T: Serialize>(
        status: StatusCode,
        output: &T,
        completion_uncertain: bool,
    ) -> Self {
        let mut bytes = LimitedBytes(Vec::new());
        let body = serde_json::to_writer(&mut bytes, output)
            .ok()
            .and_then(|()| serde_json::from_slice(&bytes.0).ok());
        Self {
            status,
            body,
            completion_uncertain,
        }
    }

    /// Whether the host cannot establish the operation's completed outcome.
    pub fn completion_uncertain(&self) -> bool {
        self.completion_uncertain
    }
}

pub(super) fn api_failure(error: HostError) -> CallToolResult {
    match error.body {
        Some(body) => {
            CallToolResult::structured_error(json!({"status":error.status.as_u16(),"error":body}))
        }
        None => failure("request_failed", "The graph request failed."),
    }
}
