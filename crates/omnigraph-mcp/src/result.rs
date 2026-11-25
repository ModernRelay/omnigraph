//! Bounded result encoding and the host's canonical error projection.

use crate::{ProtocolError, RESULT_BYTES};
use axum::http::StatusCode;
use rmcp::model::CallToolResult;
use serde::Serialize;
use serde_json::{Value, json};
use std::num::NonZeroUsize;

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

/// Counts encoded bytes, failing once they pass `limit`, and keeps none.
struct CountedBytes {
    written: usize,
    limit: usize,
}
impl std::io::Write for CountedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.written {
            return Err(std::io::Error::other("MCP result exceeds byte limit"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Whether `value` encodes as JSON within `limit` bytes.
pub(super) fn encodes_within<T: Serialize>(value: &T, limit: usize) -> bool {
    serde_json::to_writer(&mut CountedBytes { written: 0, limit }, value).is_ok()
}

fn bounded_value<T: Serialize>(value: &T) -> Result<Value, ProtocolError> {
    let mut bytes = LimitedBytes(Vec::new());
    serde_json::to_writer(&mut bytes, value).map_err(|_| ProtocolError::ResultTooLarge)?;
    serde_json::from_slice(&bytes.0).map_err(|_| ProtocolError::InvalidResult)
}

/// Encode a handler DTO under both the value and complete SDK result bounds.
/// An oversized response is an error, so the host can preserve the fact that
/// a write ran before its receipt could be delivered.
pub fn bounded_result<T: Serialize>(value: &T) -> Result<CallToolResult, ProtocolError> {
    check_result(CallToolResult::structured(bounded_value(value)?))
}

/// Include the text copy structured results carry for older clients.
fn check_result(result: CallToolResult) -> Result<CallToolResult, ProtocolError> {
    if encodes_within(&result, RESULT_BYTES) {
        Ok(result)
    } else {
        Err(ProtocolError::ResultTooLarge)
    }
}

/// The most a successful read's complete result may encode to, its text copy
/// included: the size a client takes in whole. It is separate from
/// [`RESULT_BYTES`], the memory cap on every result, and holds 1 to that cap,
/// never 0 and never more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBudget(NonZeroUsize);

impl ReadBudget {
    /// 96 KiB. A structured result carries its data twice, as
    /// `structuredContent` and as the text copy, so the text copy stays under
    /// 49,152 characters. That fits the smallest limit a common client
    /// documents: Claude Code saves a tool result whose text exceeds 50,000
    /// characters to a file instead of handing it to the model
    /// (<https://code.claude.com/docs/en/mcp#mcp-output-limits-and-warnings>).
    /// ChatGPT and Cursor document no number.
    pub const DEFAULT: Self = match Self::new(96 * 1024) {
        Some(budget) => budget,
        None => panic!("the default read budget is within the result cap"),
    };

    /// A budget of `bytes`, or `None` for 0 or more than [`RESULT_BYTES`].
    pub const fn new(bytes: usize) -> Option<Self> {
        if bytes > RESULT_BYTES {
            return None;
        }
        match NonZeroUsize::new(bytes) {
            Some(bytes) => Some(Self(bytes)),
            None => None,
        }
    }

    pub const fn bytes(self) -> usize {
        self.0.get()
    }
}

impl Default for ReadBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl std::fmt::Display for ReadBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::format_bytes(self.bytes()))
    }
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
        let mut body = json!({"error": bounded_value(output)?});
        if let Some(status) = status {
            body["status"] = json!(status.as_u16());
        }
        let result = check_result(CallToolResult::structured_error(body))?;
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
