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

/// Why a value has no encoding under the memory cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unencoded {
    /// Its encoding passed [`RESULT_BYTES`]; the bytes were not kept.
    OverCap,
    /// It did not encode as a JSON value.
    Invalid,
}

/// A handler's value, encoded under the memory cap [`RESULT_BYTES`] and not
/// yet judged for size: whether it may be sent, and what replaces it when it
/// may not, is decided once, by the protocol layer, from what the call did.
#[derive(Debug, Clone, PartialEq)]
pub struct Encoded(Result<Value, Unencoded>);

impl Encoded {
    /// Encode `value`, keeping at most [`RESULT_BYTES`] of it in memory.
    pub fn new<T: Serialize>(value: &T) -> Self {
        let mut bytes = LimitedBytes(Vec::new());
        Self(match serde_json::to_writer(&mut bytes, value) {
            Err(_) => Err(Unencoded::OverCap),
            Ok(()) => serde_json::from_slice(&bytes.0).map_err(|_| Unencoded::Invalid),
        })
    }

    /// The encoded value, or the refusal for a value that has none: `over`
    /// for one past the cap, which the caller picks by the size rule.
    pub(crate) fn value(self, over: ProtocolError) -> Result<Value, ProtocolError> {
        self.0.map_err(|unencoded| match unencoded {
            Unencoded::OverCap => over,
            Unencoded::Invalid => ProtocolError::InvalidResult,
        })
    }

    /// The encoded value; `None` for one past the cap or not JSON.
    pub fn as_value(&self) -> Option<&Value> {
        self.0.as_ref().ok()
    }
}

/// The most a successful read's complete result may encode to, its text copy
/// included: the size a client takes in whole. It is separate from
/// [`RESULT_BYTES`], the memory cap on every result, and holds 1 to that cap,
/// never 0 and never more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBudget(NonZeroUsize);

impl ReadBudget {
    /// 96 KiB. The budget bounds a complete encoded tool result, and the
    /// SDK sends a structured result's data twice, as `structuredContent`
    /// and as a JSON text copy, so a read within it has a text copy of fewer
    /// than 49,152 characters (escaping only lengthens the encoded copy).
    /// That is below the 50,000 characters past which Claude Code moves a
    /// successful tool result to a file instead of handing it to the model
    /// (<https://code.claude.com/docs/en/mcp#mcp-output-limits-and-warnings>).
    /// It promises no token count: Claude Code separately limits a result
    /// to `MAX_MCP_OUTPUT_TOKENS` and shortens long error text. A resource
    /// read carries one copy, so its text can approach the whole budget.
    /// ChatGPT and Cursor document no limit.
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

/// The host's canonical error for a call. The host judges codes, effect
/// disposition and optional HTTP status before crossing this boundary; MCP
/// never infers them from a status or changes the body afterward. Its size is
/// judged by the protocol layer with everything else it sends: a body too
/// large to send is replaced by the host's own `result_too_large` refusal
/// for the same call.
#[derive(Debug, Clone, PartialEq)]
pub struct HostError {
    status: Option<StatusCode>,
    body: Encoded,
}

impl HostError {
    /// Wrap an already-judged error DTO. Errors specific to MCP have no HTTP
    /// status.
    pub fn from_output<T: Serialize>(status: Option<StatusCode>, output: &T) -> Self {
        Self {
            status,
            body: Encoded::new(output),
        }
    }

    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }

    /// The error body, or `None` when it has no encoding under the cap.
    pub fn body(&self) -> Option<&Value> {
        self.body.as_value()
    }

    /// The tool result that carries this error, `{"error": <body>,
    /// "status": <status>}`, before its size is judged; `over` is the
    /// refusal for a body past the cap.
    pub(crate) fn tool_result(self, over: ProtocolError) -> Result<CallToolResult, ProtocolError> {
        let mut body = json!({"error": self.body.value(over)?});
        if let Some(status) = self.status {
            body["status"] = json!(status.as_u16());
        }
        Ok(CallToolResult::structured_error(body))
    }
}
