//! Bounded result encoding and the host's canonical error projection.

use crate::{ProtocolError, RESULT_BYTES};
use axum::http::StatusCode;
use rmcp::model::CallToolResult;
use serde::Serialize;
use serde_json::{Value, json};

struct LimitedBytes {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("MCP result exceeds byte limit"));
        }
        self.bytes.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_value<T: Serialize>(value: &T, limit: usize) -> Result<Value, ProtocolError> {
    let mut bytes = LimitedBytes {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut bytes, value)
        .map_err(|_| ProtocolError::ResultTooLarge { limit })?;
    serde_json::from_slice(&bytes.bytes).map_err(|_| ProtocolError::InvalidResult)
}

/// Encode a handler DTO under both the value and complete SDK result bounds.
/// An oversized response is an error, so the host can preserve the fact that
/// a write ran before its receipt could be delivered.
pub fn bounded_result<T: Serialize>(value: &T) -> Result<CallToolResult, ProtocolError> {
    bounded_result_within(value, RESULT_BYTES)
}

/// [`bounded_result`] under a configured bound, which the host takes from
/// [`crate::McpSettings::result_bytes`] and which never exceeds
/// [`RESULT_BYTES`].
pub fn bounded_result_within<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<CallToolResult, ProtocolError> {
    let limit = limit.min(RESULT_BYTES);
    check_result(
        CallToolResult::structured(bounded_value(value, limit)?),
        limit,
    )
}

/// Include the text copy structured results carry for older clients.
pub(super) fn check_result(
    result: CallToolResult,
    limit: usize,
) -> Result<CallToolResult, ProtocolError> {
    let limit = limit.min(RESULT_BYTES);
    serde_json::to_writer(
        &mut LimitedBytes {
            bytes: Vec::new(),
            limit,
        },
        &result,
    )
    .map_err(|_| ProtocolError::ResultTooLarge { limit })?;
    Ok(result)
}

/// A bounded projection of the host's canonical error. The host judges codes,
/// effect disposition and optional HTTP status before crossing this boundary;
/// MCP never infers them from a status or changes the body afterward.
#[derive(Debug)]
pub struct HostError {
    status: Option<StatusCode>,
    result: Box<CallToolResult>,
}

impl HostError {
    /// Wrap an already-judged error DTO in the MCP envelope. Errors specific
    /// to MCP have no HTTP status. If either byte bound is exceeded, the host
    /// must project a small canonical refusal with the same write context.
    pub fn from_output<T: Serialize>(
        status: Option<StatusCode>,
        output: &T,
    ) -> Result<Self, ProtocolError> {
        // Refusals keep the built-in bound: a configured lower bound must
        // never turn a refusal into a second, harder-to-read one.
        let mut body = json!({"error": bounded_value(output, RESULT_BYTES)?});
        if let Some(status) = status {
            body["status"] = json!(status.as_u16());
        }
        let result = check_result(CallToolResult::structured_error(body), RESULT_BYTES)?;
        Ok(Self {
            status,
            result: Box::new(result),
        })
    }

    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }

    pub fn into_tool_result(self) -> CallToolResult {
        *self.result
    }
}
