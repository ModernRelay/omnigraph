//! JSON-RPC framing on both sides of the bridge: newline-delimited messages
//! on stdio, one POST per message to Streamable HTTP.

use std::io::{self, BufRead};

use base64::Engine as _;
use serde_json::{Value, json};

pub(super) const PARSE_ERROR: i64 = -32700;
pub(super) const INVALID_REQUEST: i64 = -32600;
pub(super) const INVALID_PARAMS: i64 = -32602;
/// Implementation-defined: the request did not reach the server, or the
/// server answered it without a JSON-RPC message.
pub(super) const SERVER_ERROR: i64 = -32000;

/// The per-request protocol version a request names in `_meta`.
const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";

/// One message the agent sent, by what it expects back.
#[derive(Debug, PartialEq)]
pub(super) enum Inbound {
    /// The agent waits for an answer carrying `id`.
    Request { id: Value, method: String },
    /// Nothing answers it.
    Notification { method: String },
    /// The agent's answer to a request the server sent.
    Response,
}

/// Parse one line into its message and kind, or the JSON-RPC error the agent
/// is answered with instead.
pub(super) fn classify(line: &[u8]) -> Result<(Value, Inbound), Value> {
    let message: Value = serde_json::from_slice(line)
        .map_err(|_| error(Value::Null, PARSE_ERROR, "the line is not JSON", None))?;
    let object = match &message {
        Value::Object(object) => object,
        Value::Array(_) => {
            return Err(error(
                Value::Null,
                INVALID_REQUEST,
                "JSON-RPC batches are not supported; send one message per line",
                None,
            ));
        }
        _ => {
            return Err(error(
                Value::Null,
                INVALID_REQUEST,
                "a JSON-RPC message is an object",
                None,
            ));
        }
    };
    let id = object.get("id").cloned();
    let kind = match (object.get("method"), id) {
        (Some(Value::String(method)), Some(id)) => Inbound::Request {
            id,
            method: method.clone(),
        },
        (Some(Value::String(method)), None) => Inbound::Notification {
            method: method.clone(),
        },
        (None, Some(_)) if object.contains_key("result") || object.contains_key("error") => {
            Inbound::Response
        }
        (_, id) => {
            return Err(error(
                id.unwrap_or(Value::Null),
                INVALID_REQUEST,
                "a JSON-RPC message names a string `method`, or answers an `id` with `result` or `error`",
                None,
            ));
        }
    };
    Ok((message, kind))
}

/// A JSON-RPC error response.
pub(super) fn error(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({"code": code, "message": message});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}

/// Whether `message` answers the request `id`.
pub(super) fn answers(message: &Value, id: &Value) -> bool {
    message.get("id") == Some(id)
        && (message.get("result").is_some() || message.get("error").is_some())
}

/// The protocol version a request names in its `_meta`.
pub(super) fn meta_protocol_version(message: &Value) -> Option<&str> {
    message
        .get("params")?
        .get("_meta")?
        .get(META_PROTOCOL_VERSION)?
        .as_str()
}

/// The `Mcp-Name` routing value of a request: the tool or prompt name, the
/// resource URI, or the task id.
pub(super) fn routing_name<'a>(method: &str, message: &'a Value) -> Option<&'a str> {
    let key = match method {
        "tools/call" | "prompts/get" => "name",
        "resources/read" | "resources/subscribe" | "resources/unsubscribe" => "uri",
        "tasks/get" | "tasks/update" | "tasks/cancel" => "taskId",
        _ => return None,
    };
    message.get("params")?.get(key)?.as_str()
}

const BASE64_PREFIX: &str = "=?base64?";
const BASE64_SUFFIX: &str = "?=";

/// A header value as the MCP standard headers spell it: bare when it is
/// printable ASCII without leading or trailing blanks, otherwise Base64
/// between `=?base64?` and `?=`, as is a bare value that already looks so.
pub(super) fn header_value(value: &str) -> String {
    let blank = |byte: Option<&u8>| matches!(byte, Some(b' ' | b'\t'));
    let bytes = value.as_bytes();
    let wrap = blank(bytes.first())
        || blank(bytes.last())
        || bytes.iter().any(|byte| !(0x20..=0x7e).contains(byte))
        || (value.starts_with(BASE64_PREFIX) && value.ends_with(BASE64_SUFFIX));
    if wrap {
        format!(
            "{BASE64_PREFIX}{}{BASE64_SUFFIX}",
            base64::engine::general_purpose::STANDARD.encode(value)
        )
    } else {
        value.to_string()
    }
}

/// The `data` payloads of a `text/event-stream` body, one per event.
pub(super) fn event_data(body: &str) -> Vec<String> {
    let mut events = Vec::new();
    let mut data: Option<String> = None;
    for line in body.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            events.extend(data.take());
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        if field == "data" {
            let value = value.strip_prefix(' ').unwrap_or(value);
            match &mut data {
                Some(data) => {
                    data.push('\n');
                    data.push_str(value);
                }
                None => data = Some(value.to_string()),
            }
        }
    }
    events.extend(data);
    events
}

/// One line read from the agent.
#[derive(Debug, PartialEq)]
pub(super) enum Line {
    Message(Vec<u8>),
    /// Longer than the bound; its bytes were discarded up to its newline.
    TooLong,
}

/// Read one newline-terminated line of at most `max` bytes, without its
/// line ending. A last line without a newline is still a line; `None` is the
/// end of input.
pub(super) fn read_line(reader: &mut impl BufRead, max: usize) -> io::Result<Option<Line>> {
    let mut line = Vec::new();
    let mut too_long = false;
    let mut read_any = false;
    loop {
        let buffer = match reader.fill_buf() {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if buffer.is_empty() {
            if !read_any {
                return Ok(None);
            }
            break;
        }
        read_any = true;
        let (chunk, used, ended) = match buffer.iter().position(|byte| *byte == b'\n') {
            Some(end) => (&buffer[..end], end + 1, true),
            None => (buffer, buffer.len(), false),
        };
        if !too_long {
            if line.len() + chunk.len() > max {
                too_long = true;
                line = Vec::new();
            } else {
                line.extend_from_slice(chunk);
            }
        }
        reader.consume(used);
        if ended {
            break;
        }
    }
    if too_long {
        return Ok(Some(Line::TooLong));
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(Line::Message(line)))
}

/// Whether a line holds only whitespace, which carries no message.
pub(super) fn blank(line: &[u8]) -> bool {
    line.iter().all(u8::is_ascii_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_are_classified_by_what_they_expect_back() {
        let (_, kind) = classify(br#"{"jsonrpc":"2.0","id":7,"method":"tools/list"}"#).unwrap();
        assert_eq!(
            kind,
            Inbound::Request {
                id: json!(7),
                method: "tools/list".into()
            }
        );
        let (_, kind) =
            classify(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).unwrap();
        assert_eq!(
            kind,
            Inbound::Notification {
                method: "notifications/initialized".into()
            }
        );
        let (_, kind) = classify(br#"{"jsonrpc":"2.0","id":"s1","result":{}}"#).unwrap();
        assert_eq!(kind, Inbound::Response);

        let refused = classify(b"not json").unwrap_err();
        assert_eq!(refused["error"]["code"], PARSE_ERROR);
        assert_eq!(refused["id"], Value::Null);
        let refused = classify(br#"[{"jsonrpc":"2.0","id":1,"method":"ping"}]"#).unwrap_err();
        assert_eq!(refused["error"]["code"], INVALID_REQUEST);
        assert!(
            refused["error"]["message"]
                .as_str()
                .unwrap()
                .contains("batches")
        );
        let refused = classify(br#"{"jsonrpc":"2.0","id":3,"method":4}"#).unwrap_err();
        assert_eq!(refused["id"], 3);
        assert_eq!(refused["error"]["code"], INVALID_REQUEST);
        assert_eq!(classify(b"7").unwrap_err()["id"], Value::Null);
    }

    #[test]
    fn routing_headers_follow_the_body() {
        let call = json!({"method":"tools/call","params":{"name":"schema","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}});
        assert_eq!(routing_name("tools/call", &call), Some("schema"));
        assert_eq!(meta_protocol_version(&call), Some("2026-07-28"));
        let read = json!({"params":{"uri":"omnigraph://graphs/default/schema"}});
        assert_eq!(
            routing_name("resources/read", &read),
            Some("omnigraph://graphs/default/schema")
        );
        assert_eq!(routing_name("tools/list", &call), None);
        assert_eq!(meta_protocol_version(&read), None);

        assert_eq!(header_value("schema"), "schema");
        for value in [
            " padded",
            "tab\t",
            "na\u{ef}ve",
            "line\nbreak",
            "=?base64?x?=",
        ] {
            let wrapped = header_value(value);
            let inner = wrapped
                .strip_prefix(BASE64_PREFIX)
                .and_then(|rest| rest.strip_suffix(BASE64_SUFFIX))
                .unwrap_or_else(|| panic!("{value:?} is wrapped: {wrapped}"));
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(inner)
                .unwrap();
            assert_eq!(decoded, value.as_bytes());
        }
    }

    #[test]
    fn an_event_stream_yields_one_message_per_event() {
        let body = ": keep-alive\r\nevent: message\r\nid: 1\r\ndata: {\"a\":1}\r\n\r\nretry: 3000\ndata: {\"b\":\ndata: 2}\n\ndata:{\"c\":3}";
        assert_eq!(
            event_data(body),
            vec!["{\"a\":1}", "{\"b\":\n2}", "{\"c\":3}"]
        );
        assert!(event_data(": only a comment\n\n").is_empty());
    }

    #[test]
    fn lines_are_bounded_and_end_with_their_newline_or_the_input() {
        let mut input = io::Cursor::new(b"one\r\n\ntoo long here\nlast".to_vec());
        let mut reader = io::BufReader::with_capacity(4, &mut input);
        assert_eq!(
            read_line(&mut reader, 8).unwrap(),
            Some(Line::Message(b"one".to_vec()))
        );
        assert_eq!(
            read_line(&mut reader, 8).unwrap(),
            Some(Line::Message(Vec::new()))
        );
        assert_eq!(read_line(&mut reader, 8).unwrap(), Some(Line::TooLong));
        assert_eq!(
            read_line(&mut reader, 8).unwrap(),
            Some(Line::Message(b"last".to_vec()))
        );
        assert_eq!(read_line(&mut reader, 8).unwrap(), None);
        assert!(blank(b" \t"));
        assert!(!blank(b" {}"));
    }
}
