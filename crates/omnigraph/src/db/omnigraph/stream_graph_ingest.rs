//! Graph-native row parsing for the hidden RFC-026 ingest boundary.
//!
//! This module owns only the bounded caller wire. Declaration lookup, lazy
//! enrollment, incarnation injection, normalization, and WAL admission remain
//! in their existing owners. In particular, neither a table key nor any
//! private stream authority is accepted from the caller.

use std::collections::BTreeMap;

use lance_index::mem_wal::ShardId;
use serde::Deserialize;

use crate::db::manifest::stream_token::StreamToken;
use crate::error::{OmniError, Result};

use super::stream_request::STREAM_NDJSON_MAX_LINE_BYTES;

/// Match the pre-DOM structural envelope used by the existing private row
/// normalizer: 64 MiB of the root preprocessing reservation, conservatively
/// charged at 512 bytes per JSON structural slot.
const GRAPH_STREAM_JSON_DOM_STRUCTURE_BYTES: u64 = 64 * 1024 * 1024;
const GRAPH_STREAM_JSON_BYTES_PER_STRUCTURAL_SLOT: u64 = 512;
const GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS: u64 =
    GRAPH_STREAM_JSON_DOM_STRUCTURE_BYTES / GRAPH_STREAM_JSON_BYTES_PER_STRUCTURAL_SLOT;

const GRAPH_STREAM_TOP_LEVEL_FIELDS: &[&str] = &["type", "edge", "from", "to", "data", "$stream"];

/// Logical declaration selected by one graph-native row.
///
/// This is row data, not a caller-addressable physical lane. The coordinator
/// resolves it through the operation-local accepted catalog before enrollment
/// or admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GraphStreamLogicalDescriptor {
    Node { type_name: String },
    Edge { type_name: String },
}

impl GraphStreamLogicalDescriptor {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::Node { .. } => "node",
            Self::Edge { .. } => "edge",
        }
    }

    pub(super) fn type_name(&self) -> &str {
        match self {
            Self::Node { type_name } | Self::Edge { type_name } => type_name,
        }
    }
}

/// Caller-owned fields from `$stream`.
///
/// The private stream incarnation and trusted contributor are intentionally
/// absent. The graph adapter injects both only after authority and declaration
/// validation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GraphStreamCallerEnvelope {
    pub(super) write_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(super) predecessor_token: Option<StreamToken>,
}

impl GraphStreamCallerEnvelope {
    fn validate(&self) -> std::result::Result<(), String> {
        let parsed = ShardId::parse_str(&self.write_id)
            .map_err(|error| format!("write_id must be a UUID: {error}"))?;
        if parsed.is_nil() {
            return Err("write_id must be non-nil".to_string());
        }
        if parsed.to_string() != self.write_id {
            return Err("write_id must use canonical lowercase hyphenated UUID text".to_string());
        }
        Ok(())
    }
}

/// One bounded graph-native row ready for catalog resolution and the existing
/// strict table normalizer.
#[derive(Debug, PartialEq)]
pub(super) struct ParsedGraphStreamJsonRow {
    pub(super) descriptor: GraphStreamLogicalDescriptor,
    pub(super) envelope: GraphStreamCallerEnvelope,
    /// A node body is exactly `data`. An edge body additionally carries the
    /// existing normalizer's structural `src`/`dst` fields derived from the
    /// caller's top-level `from`/`to` values.
    pub(super) body: serde_json::Value,
}

impl ParsedGraphStreamJsonRow {
    pub(super) fn kind(&self) -> &'static str {
        self.descriptor.kind()
    }

    pub(super) fn type_name(&self) -> &str {
        self.descriptor.type_name()
    }

    /// Available after wire parsing when `data.id` is an ordinary string.
    /// The existing strict normalizer remains authoritative for requiring and
    /// canonicalizing the logical ID against the accepted declaration.
    pub(super) fn logical_id(&self) -> Option<&str> {
        self.body.get("id").and_then(serde_json::Value::as_str)
    }

    pub(super) fn write_id(&self) -> &str {
        &self.envelope.write_id
    }

    /// Transfer the one bounded DOM into the existing normalizer. Keeping the
    /// parsed row non-`Clone` makes a second full JSON tree an explicit design
    /// change rather than an accidental route-metadata convenience.
    pub(super) fn into_normalizer_parts(self) -> (String, Option<StreamToken>, serde_json::Value) {
        (
            self.envelope.write_id,
            self.envelope.predecessor_token,
            self.body,
        )
    }
}

struct GraphStreamJsonWire {
    kind: GraphStreamJsonWireKind,
    data: BTreeMap<String, serde_json::Value>,
    envelope: GraphStreamCallerEnvelope,
}

enum GraphStreamJsonWireKind {
    Node {
        type_name: String,
    },
    Edge {
        type_name: String,
        from: String,
        to: String,
    },
}

impl<'de> Deserialize<'de> for GraphStreamJsonWire {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = GraphStreamJsonWire;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one graph-native stream JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<GraphStreamJsonWire, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut node_type = None;
                let mut edge_type = None;
                let mut from = None;
                let mut to = None;
                let mut data = None;
                let mut envelope = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "type" => {
                            if node_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            node_type = Some(map.next_value::<String>()?);
                        }
                        "edge" => {
                            if edge_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("edge"));
                            }
                            edge_type = Some(map.next_value::<String>()?);
                        }
                        "from" => {
                            if from.is_some() {
                                return Err(serde::de::Error::duplicate_field("from"));
                            }
                            from = Some(map.next_value::<String>()?);
                        }
                        "to" => {
                            if to.is_some() {
                                return Err(serde::de::Error::duplicate_field("to"));
                            }
                            to = Some(map.next_value::<String>()?);
                        }
                        "data" => {
                            if data.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data = Some(map.next_value::<StrictJsonObject>()?.0);
                        }
                        "$stream" => {
                            if envelope.is_some() {
                                return Err(serde::de::Error::duplicate_field("$stream"));
                            }
                            envelope = Some(map.next_value::<GraphStreamCallerEnvelope>()?);
                        }
                        field if is_reserved_stream_state_field(field) => {
                            return Err(serde::de::Error::custom(format!(
                                "graph stream input field '{field}' is reserved private state"
                            )));
                        }
                        field => {
                            return Err(serde::de::Error::unknown_field(
                                field,
                                GRAPH_STREAM_TOP_LEVEL_FIELDS,
                            ));
                        }
                    }
                }

                let data = data.ok_or_else(|| serde::de::Error::missing_field("data"))?;
                let envelope =
                    envelope.ok_or_else(|| serde::de::Error::missing_field("$stream"))?;
                let kind = match (node_type, edge_type) {
                    (Some(type_name), None) => {
                        if from.is_some() || to.is_some() {
                            return Err(serde::de::Error::custom(
                                "node stream rows do not allow top-level 'from' or 'to'",
                            ));
                        }
                        GraphStreamJsonWireKind::Node { type_name }
                    }
                    (None, Some(type_name)) => {
                        let from = from.ok_or_else(|| serde::de::Error::missing_field("from"))?;
                        let to = to.ok_or_else(|| serde::de::Error::missing_field("to"))?;
                        GraphStreamJsonWireKind::Edge {
                            type_name,
                            from,
                            to,
                        }
                    }
                    (None, None) | (Some(_), Some(_)) => {
                        return Err(serde::de::Error::custom(
                            "graph stream row requires exactly one of 'type' or 'edge'",
                        ));
                    }
                };

                Ok(GraphStreamJsonWire {
                    kind,
                    data,
                    envelope,
                })
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

/// Parse one graph-native row without resolving any physical declaration.
///
/// Schema-dependent unknown-property checks intentionally remain in
/// `loader::normalize_stream_json_row`; this parser rejects only unknown wire
/// fields and authority/physical fields that no accepted schema may own.
pub(super) fn parse_graph_stream_json_row(raw_json: &[u8]) -> Result<ParsedGraphStreamJsonRow> {
    validate_graph_stream_json_bounds(raw_json)?;
    let wire = serde_json::from_slice::<GraphStreamJsonWire>(raw_json)
        .map_err(|error| OmniError::manifest(format!("invalid graph stream JSON: {error}")))?;
    wire.envelope
        .validate()
        .map_err(|error| OmniError::manifest(format!("invalid graph stream JSON: {error}")))?;
    validate_graph_stream_data_fields(&wire.kind, &wire.data)?;

    let GraphStreamJsonWire {
        kind,
        mut data,
        envelope,
    } = wire;
    let descriptor = match kind {
        GraphStreamJsonWireKind::Node { type_name } => {
            GraphStreamLogicalDescriptor::Node { type_name }
        }
        GraphStreamJsonWireKind::Edge {
            type_name,
            from,
            to,
        } => {
            data.insert("src".to_string(), serde_json::Value::String(from));
            data.insert("dst".to_string(), serde_json::Value::String(to));
            GraphStreamLogicalDescriptor::Edge { type_name }
        }
    };

    Ok(ParsedGraphStreamJsonRow {
        descriptor,
        envelope,
        body: serde_json::Value::Object(data.into_iter().collect()),
    })
}

fn validate_graph_stream_json_bounds(raw_json: &[u8]) -> Result<()> {
    let raw_bytes = u64::try_from(raw_json.len()).unwrap_or(u64::MAX);
    let raw_limit = u64::try_from(STREAM_NDJSON_MAX_LINE_BYTES).unwrap_or(u64::MAX);
    if raw_bytes > raw_limit {
        return Err(OmniError::resource_limit(
            "stream raw JSON bytes",
            raw_limit,
            raw_bytes,
        ));
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut slots = 1_u64;
    for &byte in raw_json {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            continue;
        }
        if matches!(byte, b'{' | b'}' | b'[' | b']' | b',' | b':') {
            slots = slots.checked_add(1).unwrap_or(u64::MAX);
            if slots > GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS {
                return Err(OmniError::resource_limit(
                    "stream_json_structural_slots",
                    GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS,
                    slots,
                ));
            }
        }
    }
    Ok(())
}

fn validate_graph_stream_data_fields(
    kind: &GraphStreamJsonWireKind,
    data: &BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    for field in data.keys() {
        if is_reserved_stream_state_field(field) {
            return Err(OmniError::manifest(format!(
                "graph stream data field '{field}' is reserved physical state"
            )));
        }
        if matches!(kind, GraphStreamJsonWireKind::Edge { .. })
            && matches!(field.as_str(), "src" | "dst")
        {
            return Err(OmniError::manifest(format!(
                "graph edge stream data field '{field}' is reserved structural state; use top-level 'from' and 'to'"
            )));
        }
    }
    Ok(())
}

fn is_reserved_stream_state_field(field: &str) -> bool {
    matches!(
        field,
        "$stream"
            | "_tombstone"
            | "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_created_at_version"
            | "_row_last_updated_at_version"
    ) || field.eq_ignore_ascii_case(crate::db::STREAM_METADATA_COLUMN)
}

fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// JSON object which rejects duplicate members recursively before converting
/// to the ordinary `serde_json::Value` consumed by the existing normalizer.
struct StrictJsonObject(BTreeMap<String, serde_json::Value>);

impl<'de> Deserialize<'de> for StrictJsonObject {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictJsonObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object for graph stream 'data'")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some(field) = map.next_key::<String>()? {
                    if values.contains_key(&field) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate graph stream data field '{field}'"
                        )));
                    }
                    values.insert(field, map.next_value::<StrictJsonValue>()?.0);
                }
                Ok(StrictJsonObject(values))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

struct StrictJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictJsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value without duplicate object members")
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Number(value.into())))
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Number(value.into())))
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let number = serde_json::Number::from_f64(value)
                    .ok_or_else(|| E::custom("non-finite JSON number"))?;
                Ok(StrictJsonValue(serde_json::Value::Number(number)))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(value.to_string())
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::String(value)))
            }

            fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                StrictJsonValue::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictJsonValue>()? {
                    values.push(value.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Array(values)))
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = serde_json::Map::new();
                while let Some(field) = map.next_key::<String>()? {
                    if values.contains_key(&field) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate graph stream nested data field '{field}'"
                        )));
                    }
                    values.insert(field, map.next_value::<StrictJsonValue>()?.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Object(values)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRITE_ID: &str = "8a880f0a-3f41-4a42-9b0e-f34af0a9a4df";

    fn envelope() -> String {
        format!(r#""$stream":{{"write_id":"{WRITE_ID}","predecessor_token":null}}"#)
    }

    #[test]
    fn parses_node_and_edge_into_existing_normalizer_bodies() {
        let node = parse_graph_stream_json_row(
            format!(
                r#"{{"type":"Person","data":{{"id":"n-17","name":"Ada"}},{}}}"#,
                envelope()
            )
            .as_bytes(),
        )
        .expect("node wire must parse");
        assert_eq!(
            node.descriptor,
            GraphStreamLogicalDescriptor::Node {
                type_name: "Person".to_string()
            }
        );
        assert_eq!(node.descriptor.kind(), "node");
        assert_eq!(node.descriptor.type_name(), "Person");
        assert_eq!(node.kind(), "node");
        assert_eq!(node.type_name(), "Person");
        assert_eq!(node.logical_id(), Some("n-17"));
        assert_eq!(node.write_id(), WRITE_ID);
        assert_eq!(node.envelope.write_id, WRITE_ID);
        assert_eq!(node.envelope.predecessor_token, None);
        assert_eq!(node.body, serde_json::json!({"id": "n-17", "name": "Ada"}));

        let predecessor = format!("sha256:{}", "a".repeat(64));
        let edge = parse_graph_stream_json_row(
            format!(
                r#"{{"edge":"Knows","from":"n-17","to":"n-18","data":{{"id":"e-17","weight":2}},"$stream":{{"write_id":"591d698c-9fc5-4673-af1a-d39fdba8ded0","predecessor_token":"{predecessor}"}}}}"#
            )
            .as_bytes(),
        )
        .expect("edge wire must parse");
        assert_eq!(
            edge.descriptor,
            GraphStreamLogicalDescriptor::Edge {
                type_name: "Knows".to_string()
            }
        );
        assert_eq!(edge.descriptor.kind(), "edge");
        assert_eq!(edge.descriptor.type_name(), "Knows");
        assert_eq!(
            edge.envelope.predecessor_token.unwrap().to_string(),
            predecessor
        );
        assert_eq!(
            edge.body,
            serde_json::json!({
                "id": "e-17",
                "src": "n-17",
                "dst": "n-18",
                "weight": 2,
            })
        );
    }

    #[test]
    fn rejects_duplicate_members_at_each_wire_layer() {
        let cases = [
            format!(
                r#"{{"type":"Person","type":"Company","data":{{"id":"n"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n","id":"forged"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n","values":[{{"x":1,"x":2}}]}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},"$stream":{{"write_id":"{WRITE_ID}","write_id":"591d698c-9fc5-4673-af1a-d39fdba8ded0","predecessor_token":null}}}}"#
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},{0},{0}}}"#,
                envelope()
            ),
        ];

        for raw in cases {
            let error = parse_graph_stream_json_row(raw.as_bytes())
                .expect_err("duplicate wire members must be rejected");
            assert!(error.to_string().contains("duplicate"), "{error:?}");
        }
    }

    #[test]
    fn rejects_unknown_reserved_and_private_authority_fields() {
        let cases = [
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},"table_key":"node:Person",{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n","_rowid":7}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n","$stream":{{"write_id":"forged"}}}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"edge":"Knows","from":"a","to":"b","data":{{"id":"e","src":"forged"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},"$stream":{{"write_id":"{WRITE_ID}","predecessor_token":null,"stream_incarnation_id":"11111111-1111-4111-8111-111111111111"}}}}"#
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},"$stream":{{"write_id":"{WRITE_ID}","predecessor_token":null,"contributor_id":"forged"}}}}"#
            ),
            format!(
                r#"{{"type":"Person","data":{{"id":"n"}},"$stream":{{"write_id":"{WRITE_ID}","predecessor_token":null,"origin":{{"kind":"admission"}}}}}}"#
            ),
        ];

        for raw in cases {
            parse_graph_stream_json_row(raw.as_bytes())
                .expect_err("unknown, reserved, and private authority fields must be rejected");
        }

        let ordinary_contributor_property = parse_graph_stream_json_row(
            format!(
                r#"{{"type":"Person","data":{{"id":"n","contributor_id":"ordinary"}},{}}}"#,
                envelope()
            )
            .as_bytes(),
        )
        .expect("a logical property named contributor_id is ordinary row data");
        assert_eq!(
            ordinary_contributor_property.body["contributor_id"],
            "ordinary"
        );
    }

    #[test]
    fn requires_exactly_one_discriminator_and_complete_kind_shape() {
        let cases = [
            format!(r#"{{"data":{{"id":"n"}},{}}}"#, envelope()),
            format!(
                r#"{{"type":"Person","edge":"Knows","from":"a","to":"b","data":{{"id":"e"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"type":"Person","from":"a","data":{{"id":"n"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"edge":"Knows","to":"b","data":{{"id":"e"}},{}}}"#,
                envelope()
            ),
            format!(
                r#"{{"edge":"Knows","from":"a","data":{{"id":"e"}},{}}}"#,
                envelope()
            ),
            format!(r#"{{"type":"Person","data":[],{}}}"#, envelope()),
            format!(r#"{{"type":"Person",{}}}"#, envelope()),
            r#"{"type":"Person","data":{"id":"n"}}"#.to_string(),
        ];

        for raw in cases {
            parse_graph_stream_json_row(raw.as_bytes())
                .expect_err("incomplete or ambiguous graph row shapes must be rejected");
        }
    }

    #[test]
    fn validates_the_caller_only_stream_envelope() {
        let cases = [
            r#"{"type":"Person","data":{"id":"n"},"$stream":{"write_id":"8a880f0a-3f41-4a42-9b0e-f34af0a9a4df"}}"#.to_string(),
            r#"{"type":"Person","data":{"id":"n"},"$stream":{"write_id":"00000000-0000-0000-0000-000000000000","predecessor_token":null}}"#.to_string(),
            r#"{"type":"Person","data":{"id":"n"},"$stream":{"write_id":"8A880F0A-3F41-4A42-9B0E-F34AF0A9A4DF","predecessor_token":null}}"#.to_string(),
            r#"{"type":"Person","data":{"id":"n"},"$stream":{"write_id":"not-a-uuid","predecessor_token":null}}"#.to_string(),
            r#"{"type":"Person","data":{"id":"n"},"$stream":{"write_id":"8a880f0a-3f41-4a42-9b0e-f34af0a9a4df","predecessor_token":"sha256:bad"}}"#.to_string(),
        ];

        for raw in cases {
            parse_graph_stream_json_row(raw.as_bytes())
                .expect_err("missing or noncanonical caller envelope fields must be rejected");
        }
    }

    #[test]
    fn enforces_raw_and_pre_dom_structure_bounds() {
        let quoted = format!(
            r#"{{"type":"Person","data":{{"id":"{}"}},{}}}"#,
            ",".repeat(GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS as usize),
            envelope()
        );
        validate_graph_stream_json_bounds(quoted.as_bytes())
            .expect("structural bytes inside a string do not allocate DOM nodes");

        let amplified = format!(
            "[{}0]",
            "0,".repeat(GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS as usize)
        );
        let error = validate_graph_stream_json_bounds(amplified.as_bytes())
            .expect_err("many tiny values must fail before DOM allocation");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS,
                actual,
            } if resource == "stream_json_structural_slots"
                && actual == GRAPH_STREAM_JSON_MAX_STRUCTURAL_SLOTS + 1
        ));

        let oversized = vec![b' '; STREAM_NDJSON_MAX_LINE_BYTES + 1];
        let error = parse_graph_stream_json_row(&oversized)
            .expect_err("one raw row above the framer limit must fail before parsing");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit,
                actual,
            } if resource == "stream raw JSON bytes"
                && limit == STREAM_NDJSON_MAX_LINE_BYTES as u64
                && actual == STREAM_NDJSON_MAX_LINE_BYTES as u64 + 1
        ));
    }
}
