//! RFC-026 B2 compare-and-chain token and trusted-attribution primitives.
//!
//! This module is deliberately storage-agnostic. It owns the canonical token
//! preimages and the pure admission classifier; the MemWAL adapter, token-table
//! participant, and recovery publisher consume these results but do not get to
//! reinterpret them.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::{StringBuilder, StructBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{Array, ArrayRef, StringArray, StructArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use super::TableIdentity;

const SHA256_WIRE_PREFIX: &str = "sha256:";
const SHA256_HEX_LEN: usize = 64;
pub(crate) const STREAM_TOKEN_DERIVATION_VERSION: u32 = 1;
pub(crate) const STREAM_PAYLOAD_DIGEST_VERSION: u32 = 1;
pub(crate) const STREAM_PAYLOAD_ENCODING_VERSION: u32 = 1;
pub(crate) const STREAM_TOKEN_WIRE_VERSION: &str = "sha256-lowerhex-v1";
const STREAM_TOKEN_DOMAIN_V1: &[u8] = b"omnigraph.stream-token.v1\0";
const PAYLOAD_DIGEST_DOMAIN_V1: &[u8] = b"omnigraph.stream-payload.v1\0";
const FOLD_ATTRIBUTION_DOMAIN_V1: &[u8] = b"omnigraph.stream-fold-attribution.v1\0";
/// Conservative per-entry allowance for the BTree node, allocator headers,
/// and the duplicated logical-id key retained by exact authority lookups.
const STREAM_LOOKUP_ENTRY_OVERHEAD_BYTES: usize = 256;

fn retained_string_bytes(total: &mut u64, value: &str) -> ProtocolResult<()> {
    *total = total
        .checked_add(u64::try_from(value.len()).map_err(|_| {
            StreamTokenProtocolError::invalid("stream_lookup_bytes", "string length exceeds u64")
        })?)
        .ok_or_else(|| {
            StreamTokenProtocolError::invalid("stream_lookup_bytes", "retained-byte sum overflow")
        })?;
    Ok(())
}

fn retained_origin_bytes(total: &mut u64, origin: &StreamRowOrigin) -> ProtocolResult<()> {
    match origin {
        StreamRowOrigin::Admission {
            admission_attempt_id,
            ..
        } => retained_string_bytes(total, admission_attempt_id),
        StreamRowOrigin::Correction { correction_id, .. } => {
            retained_string_bytes(total, correction_id)
        }
    }
}

/// Fixed system actor that owns every RFC-026 graph-visible stream fold.
///
/// Keep this beside the attribution protocol rather than duplicating a string
/// literal across admission, recovery, and lineage validation.  A commit that
/// carries a fold-attribution summary must be authored by this actor.
pub(crate) const STREAM_FOLD_ACTOR: &str = "omnigraph:stream-fold";

/// A failure in the trusted stream-token protocol layer.
///
/// Admission conflicts are values returned by [`classify_admission`]. Errors
/// are reserved for malformed input or durable state that cannot be trusted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StreamTokenProtocolError {
    #[error("invalid stream protocol field '{field}': {reason}")]
    Invalid { field: &'static str, reason: String },
    #[error("stream token authority is corrupt: {0}")]
    Corruption(String),
}

type ProtocolResult<T> = std::result::Result<T, StreamTokenProtocolError>;

impl StreamTokenProtocolError {
    fn invalid(field: &'static str, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            reason: reason.into(),
        }
    }
}

/// The exact v1 opaque compare-and-chain token.
///
/// Its only accepted textual form is `sha256:` followed by 64 lowercase hex
/// digits. Callers echo this value; they never construct its preimage.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StreamToken([u8; 32]);

impl StreamToken {
    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn derive(input: &StreamTokenInput<'_>) -> ProtocolResult<Self> {
        input.validate()?;

        let stream_incarnation =
            canonical_uuid_bytes("stream_incarnation_id", input.stream_incarnation_id)?;
        let write_id = canonical_uuid_bytes("write_id", input.write_id)?;
        let mut hasher = Sha256::new();
        hasher.update(STREAM_TOKEN_DOMAIN_V1);
        hash_u64(&mut hasher, input.identity.stable_table_id);
        hash_u64(&mut hasher, input.identity.table_incarnation_id);
        hash_bytes(&mut hasher, input.logical_id.as_bytes());
        hasher.update(stream_incarnation);
        hash_optional_digest(&mut hasher, input.predecessor_token);
        hasher.update(write_id);
        hash_bytes(&mut hasher, input.contributor_id.as_str().as_bytes());
        hasher.update(input.payload_digest.as_bytes());
        Ok(Self(hasher.finalize().into()))
    }
}

impl fmt::Display for StreamToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{SHA256_WIRE_PREFIX}")?;
        write_lower_hex(formatter, &self.0)
    }
}

impl fmt::Debug for StreamToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl FromStr for StreamToken {
    type Err = StreamTokenProtocolError;

    fn from_str(value: &str) -> ProtocolResult<Self> {
        decode_sha256("stream_token", value).map(Self)
    }
}

impl Serialize for StreamToken {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StreamToken {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// SHA-256 over one completely normalized logical payload.
///
/// This is a distinct type from [`StreamToken`] so a token can never be passed
/// accidentally where the trusted payload digest is required.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PayloadDigest([u8; 32]);

impl PayloadDigest {
    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash deterministic, type-aware logical row bytes after normalization.
    ///
    /// The caller owns the canonical field encoding. This function binds those
    /// bytes to the exact table lifetime and accepted schema before the result
    /// can participate in a stream token.
    pub(crate) fn derive(input: &PayloadDigestInput<'_>) -> ProtocolResult<Self> {
        input.identity.validate().map_err(|error| {
            StreamTokenProtocolError::invalid("table_identity", error.to_string())
        })?;
        decode_sha256("accepted_schema_hash", input.accepted_schema_hash)?;

        let mut hasher = Sha256::new();
        hasher.update(PAYLOAD_DIGEST_DOMAIN_V1);
        hash_u64(&mut hasher, input.identity.stable_table_id);
        hash_u64(&mut hasher, input.identity.table_incarnation_id);
        hash_bytes(&mut hasher, input.accepted_schema_hash.as_bytes());
        hash_bytes(&mut hasher, input.canonical_payload);
        Ok(Self(hasher.finalize().into()))
    }
}

impl fmt::Display for PayloadDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{SHA256_WIRE_PREFIX}")?;
        write_lower_hex(formatter, &self.0)
    }
}

impl fmt::Debug for PayloadDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl FromStr for PayloadDigest {
    type Err = StreamTokenProtocolError;

    fn from_str(value: &str) -> ProtocolResult<Self> {
        decode_sha256("payload_digest", value).map(Self)
    }
}

impl Serialize for PayloadDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PayloadDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Inputs to the versioned payload digest after all row normalization.
pub(crate) struct PayloadDigestInput<'a> {
    pub(crate) identity: TableIdentity,
    pub(crate) accepted_schema_hash: &'a str,
    pub(crate) canonical_payload: &'a [u8],
}

/// A contributor identity resolved at the trusted engine boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TrustedContributorId(String);

impl TrustedContributorId {
    pub(crate) fn new(value: impl Into<String>) -> ProtocolResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(StreamTokenProtocolError::invalid(
                "contributor_id",
                "must be non-empty",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for TrustedContributorId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TrustedContributorId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Caller-owned idempotency and predecessor envelope for one logical row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamWriteEnvelope {
    pub(crate) stream_incarnation_id: String,
    pub(crate) write_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_token: Option<StreamToken>,
}

impl StreamWriteEnvelope {
    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        canonical_uuid_bytes("stream_incarnation_id", &self.stream_incarnation_id)?;
        canonical_uuid_bytes("write_id", &self.write_id)?;
        Ok(())
    }
}

/// Exact durable origin of one winning stream row.
///
/// A tagged enum makes the RFC's variant-specific nullability structural:
/// there is exactly one origin and only its selected fields can be present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StreamRowOrigin {
    Admission {
        admission_attempt_id: String,
        caller_ordinal: u64,
    },
    Correction {
        correction_id: String,
        plan_ordinal: u64,
    },
}

impl StreamRowOrigin {
    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Admission {
                admission_attempt_id,
                ..
            } => {
                canonical_uuid_bytes("admission_attempt_id", admission_attempt_id)?;
            }
            Self::Correction { correction_id, .. } => {
                canonical_uuid_bytes("correction_id", correction_id)?;
            }
        }
        Ok(())
    }
}

/// Reserved `__omnigraph_stream_v1$` metadata stored atomically with a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrustedStreamRowMetadata {
    pub(crate) stream_incarnation_id: String,
    pub(crate) contributor_id: TrustedContributorId,
    pub(crate) write_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_token: Option<StreamToken>,
    pub(crate) stream_token: StreamToken,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) fold_base_token: Option<StreamToken>,
    pub(crate) chain_depth: u32,
    pub(crate) origin: StreamRowOrigin,
    pub(crate) payload_digest: PayloadDigest,
}

/// Exact physical field added to every internal-schema-v9 graph table.
///
/// The top-level struct and every physical child are nullable.  Lance may
/// synthesize null children while taking/projecting a null parent struct, so a
/// physically non-null child would make an otherwise valid unattributed row
/// unreadable.  The decoder supplies the stricter logical contract: once the
/// parent is present, every required value must be present and the origin tag
/// controls the nullable variant children.
pub(crate) fn trusted_stream_metadata_field() -> Field {
    let origin = DataType::Struct(
        vec![
            Field::new("kind", DataType::Utf8, true),
            Field::new("admission_attempt_id", DataType::Utf8, true),
            Field::new("caller_ordinal", DataType::UInt64, true),
            Field::new("correction_id", DataType::Utf8, true),
            Field::new("plan_ordinal", DataType::UInt64, true),
        ]
        .into(),
    );
    Field::new(
        crate::db::STREAM_METADATA_COLUMN,
        DataType::Struct(
            vec![
                Field::new("stream_incarnation_id", DataType::Utf8, true),
                Field::new("contributor_id", DataType::Utf8, true),
                Field::new("write_id", DataType::Utf8, true),
                Field::new("predecessor_token", DataType::Utf8, true),
                Field::new("stream_token", DataType::Utf8, true),
                Field::new("fold_base_token", DataType::Utf8, true),
                Field::new("chain_depth", DataType::UInt32, true),
                Field::new("origin", origin, true),
                Field::new("payload_digest", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    )
}

/// Refuse a compatible-looking hidden column instead of interpreting it as
/// RFC-026 metadata.
pub(crate) fn validate_trusted_stream_metadata_field(field: &Field) -> ProtocolResult<()> {
    let expected = trusted_stream_metadata_field();
    if field != &expected {
        return Err(StreamTokenProtocolError::invalid(
            "stream_metadata_field",
            format!(
                "must exactly match the internal {} schema",
                crate::db::STREAM_METADATA_COLUMN
            ),
        ));
    }
    Ok(())
}

/// Require one exact canonical trusted-attribution field in a physical v9
/// graph-table schema. Case-insensitive lookalikes are rejected rather than
/// ignored so a malformed storage format cannot masquerade as a user column.
pub(crate) fn validate_trusted_stream_metadata_schema(schema: &Schema) -> ProtocolResult<()> {
    let mut matches = schema.fields().iter().filter(|field| {
        field
            .name()
            .eq_ignore_ascii_case(crate::db::STREAM_METADATA_COLUMN)
    });
    let field = matches.next().ok_or_else(|| {
        StreamTokenProtocolError::invalid(
            "stream_metadata_field",
            format!(
                "physical schema is missing exact internal field '{}'",
                crate::db::STREAM_METADATA_COLUMN
            ),
        )
    })?;
    if matches.next().is_some() || field.name() != crate::db::STREAM_METADATA_COLUMN {
        return Err(StreamTokenProtocolError::invalid(
            "stream_metadata_field",
            format!(
                "physical schema must contain exactly one canonical internal field '{}'",
                crate::db::STREAM_METADATA_COLUMN
            ),
        ));
    }
    validate_trusted_stream_metadata_field(field)
}

/// Build the reserved physical struct without accepting caller-supplied raw
/// origin or token text.
pub(crate) fn build_trusted_stream_metadata_array(
    values: &[Option<TrustedStreamRowMetadata>],
) -> ProtocolResult<ArrayRef> {
    let field = trusted_stream_metadata_field();
    // `StructBuilder::from_fields` eagerly gives every Utf8 child a 1-KiB
    // value buffer, even when every parent cell is null. Direct mutations can
    // retain thousands of one-row batches, so that representation would spend
    // the keyed-write budget on empty protocol children. Arrow's canonical
    // null constructor preserves the exact datatype and parent nulls without
    // allocating those unused child capacities.
    if values.iter().all(Option::is_none) {
        return Ok(arrow_array::new_null_array(field.data_type(), values.len()));
    }
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!("trusted stream metadata field is always a struct")
    };
    let mut builder = StructBuilder::from_fields(fields.clone(), values.len());
    for value in values {
        match value {
            Some(metadata) => {
                metadata.validate_structure()?;
                append_metadata(&mut builder, metadata);
            }
            None => append_null_metadata(&mut builder),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Decode one physical metadata cell with exact type, tag, and nullability
/// checks. [`TrustedStreamRowMetadata::validate_for`] additionally binds the
/// result to its containing table/key and verifies the token preimage.
pub(crate) fn decode_trusted_stream_metadata(
    array: &dyn Array,
    row: usize,
) -> ProtocolResult<Option<TrustedStreamRowMetadata>> {
    if array.data_type() != trusted_stream_metadata_field().data_type() {
        return Err(StreamTokenProtocolError::invalid(
            "stream_metadata_array",
            "has a non-canonical physical type",
        ));
    }
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            StreamTokenProtocolError::invalid(
                "stream_metadata_array",
                "must be an Arrow StructArray",
            )
        })?;
    if row >= array.len() {
        return Err(StreamTokenProtocolError::invalid(
            "stream_metadata_row",
            format!("row {row} is outside array length {}", array.len()),
        ));
    }
    // Lance can materialize a null struct parent as a valid struct whose
    // children are all null/default-valued while projecting/taking rows. Treat
    // those representations as the same canonical absence. Any partially
    // populated struct continues into the strict decoder and fails loudly.
    if array.is_null(row) || is_absent_metadata_sentinel(array, row)? {
        return Ok(None);
    }

    let origin_array = array
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            StreamTokenProtocolError::invalid("origin", "must be an Arrow StructArray")
        })?;
    if origin_array.is_null(row) {
        return Err(StreamTokenProtocolError::invalid(
            "origin",
            "must be non-null when stream metadata is present",
        ));
    }
    let kind = required_string(origin_array, 0, row, "origin.kind")?;
    let admission_attempt_id = optional_string(origin_array, 1, row, "admission_attempt_id")?;
    let caller_ordinal = optional_u64(origin_array, 2, row, "caller_ordinal")?;
    let correction_id = optional_string(origin_array, 3, row, "correction_id")?;
    let plan_ordinal = optional_u64(origin_array, 4, row, "plan_ordinal")?;
    let origin = match (
        kind.as_str(),
        admission_attempt_id,
        caller_ordinal,
        correction_id,
        plan_ordinal,
    ) {
        ("admission", Some(admission_attempt_id), Some(caller_ordinal), None, None) => {
            StreamRowOrigin::Admission {
                admission_attempt_id,
                caller_ordinal,
            }
        }
        ("correction", None, None, Some(correction_id), Some(plan_ordinal)) => {
            StreamRowOrigin::Correction {
                correction_id,
                plan_ordinal,
            }
        }
        _ => {
            return Err(StreamTokenProtocolError::invalid(
                "origin",
                "tag and variant-specific nullable children do not agree",
            ));
        }
    };

    let metadata = TrustedStreamRowMetadata {
        stream_incarnation_id: required_string(array, 0, row, "stream_incarnation_id")?,
        contributor_id: TrustedContributorId::new(required_string(
            array,
            1,
            row,
            "contributor_id",
        )?)?,
        write_id: required_string(array, 2, row, "write_id")?,
        predecessor_token: optional_string(array, 3, row, "predecessor_token")?
            .map(|value| StreamToken::from_str(&value))
            .transpose()?,
        stream_token: StreamToken::from_str(&required_string(array, 4, row, "stream_token")?)?,
        fold_base_token: optional_string(array, 5, row, "fold_base_token")?
            .map(|value| StreamToken::from_str(&value))
            .transpose()?,
        chain_depth: required_u32(array, 6, row, "chain_depth")?,
        origin,
        payload_digest: PayloadDigest::from_str(&required_string(
            array,
            8,
            row,
            "payload_digest",
        )?)?,
    };
    metadata.validate_structure()?;
    Ok(Some(metadata))
}

fn is_absent_metadata_sentinel(array: &StructArray, row: usize) -> ProtocolResult<bool> {
    let empty_string = |column: usize, field: &'static str| -> ProtocolResult<bool> {
        let values = array
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                StreamTokenProtocolError::invalid(field, "must be an Arrow StringArray")
            })?;
        Ok(values.is_null(row) || values.value(row).is_empty())
    };
    if !(empty_string(0, "stream_incarnation_id")?
        && empty_string(1, "contributor_id")?
        && empty_string(2, "write_id")?
        && empty_string(3, "predecessor_token")?
        && empty_string(4, "stream_token")?
        && empty_string(5, "fold_base_token")?
        && empty_string(8, "payload_digest")?)
    {
        return Ok(false);
    }
    let chain_depth = array
        .column(6)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            StreamTokenProtocolError::invalid("chain_depth", "must be an Arrow UInt32Array")
        })?;
    if !chain_depth.is_null(row) && chain_depth.value(row) != 0 {
        return Ok(false);
    }
    let origin = array
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            StreamTokenProtocolError::invalid("origin", "must be an Arrow StructArray")
        })?;
    if origin.is_null(row) {
        return Ok(true);
    }
    let origin_empty_string = |column: usize, field: &'static str| -> ProtocolResult<bool> {
        let values = origin
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                StreamTokenProtocolError::invalid(field, "must be an Arrow StringArray")
            })?;
        Ok(values.is_null(row) || values.value(row).is_empty())
    };
    let origin_empty_u64 = |column: usize, field: &'static str| -> ProtocolResult<bool> {
        let values = origin
            .column(column)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                StreamTokenProtocolError::invalid(field, "must be an Arrow UInt64Array")
            })?;
        Ok(values.is_null(row) || values.value(row) == 0)
    };
    Ok(origin_empty_string(0, "origin.kind")?
        && origin_empty_string(1, "admission_attempt_id")?
        && origin_empty_u64(2, "caller_ordinal")?
        && origin_empty_string(3, "correction_id")?
        && origin_empty_u64(4, "plan_ordinal")?)
}

impl TrustedStreamRowMetadata {
    /// Conservative logical bytes retained when this metadata is held in the
    /// bounded exact-key lookup map. Variable strings are counted exactly;
    /// fixed struct/key/node overhead is charged explicitly.
    pub(crate) fn lookup_retained_bytes(&self, logical_id: &str) -> ProtocolResult<u64> {
        let fixed = std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<String>())
            .and_then(|value| value.checked_add(STREAM_LOOKUP_ENTRY_OVERHEAD_BYTES))
            .ok_or_else(|| {
                StreamTokenProtocolError::invalid(
                    "stream_lookup_bytes",
                    "fixed retained-byte sum overflow",
                )
            })?;
        let mut total = u64::try_from(fixed).map_err(|_| {
            StreamTokenProtocolError::invalid("stream_lookup_bytes", "fixed bytes exceed u64")
        })?;
        retained_string_bytes(&mut total, logical_id)?;
        retained_string_bytes(&mut total, &self.stream_incarnation_id)?;
        retained_string_bytes(&mut total, self.contributor_id.as_str())?;
        retained_string_bytes(&mut total, &self.write_id)?;
        retained_origin_bytes(&mut total, &self.origin)?;
        Ok(total)
    }

    /// Mint trusted admission metadata only after [`classify_admission`] has
    /// returned this exact candidate as `New`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_admission(
        request: &AdmissionRequest,
        candidate_token: StreamToken,
        fold_base_token: Option<StreamToken>,
        chain_depth: u32,
        admission_attempt_id: String,
        caller_ordinal: u64,
    ) -> ProtocolResult<Self> {
        request.validate()?;
        if request.candidate_token()? != candidate_token {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "candidate token for '{}' changed between classification and metadata construction",
                request.logical_id
            )));
        }
        let metadata = Self {
            stream_incarnation_id: request.envelope.stream_incarnation_id.clone(),
            contributor_id: request.contributor_id.clone(),
            write_id: request.envelope.write_id.clone(),
            predecessor_token: request.envelope.predecessor_token,
            stream_token: candidate_token,
            fold_base_token,
            chain_depth,
            origin: StreamRowOrigin::Admission {
                admission_attempt_id,
                caller_ordinal,
            },
            payload_digest: request.payload_digest,
        };
        metadata.validate_for(request.identity, &request.logical_id)?;
        Ok(metadata)
    }

    fn validate_structure(&self) -> ProtocolResult<()> {
        canonical_uuid_bytes("stream_incarnation_id", &self.stream_incarnation_id)?;
        canonical_uuid_bytes("write_id", &self.write_id)?;
        self.origin.validate()?;
        if self.chain_depth == 0 {
            return Err(StreamTokenProtocolError::invalid(
                "chain_depth",
                "must be non-zero",
            ));
        }
        if self.chain_depth == 1 && self.fold_base_token != self.predecessor_token {
            return Err(StreamTokenProtocolError::invalid(
                "fold_base_token",
                "depth-one metadata must start at its predecessor token",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_for(
        &self,
        identity: TableIdentity,
        logical_id: &str,
    ) -> ProtocolResult<()> {
        self.validate_structure()?;
        let derived = StreamToken::derive(&StreamTokenInput {
            identity,
            logical_id,
            stream_incarnation_id: &self.stream_incarnation_id,
            predecessor_token: self.predecessor_token,
            write_id: &self.write_id,
            contributor_id: &self.contributor_id,
            payload_digest: self.payload_digest,
        })?;
        if derived != self.stream_token {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "row '{logical_id}' embeds a stream token that does not match its trusted preimage"
            )));
        }
        Ok(())
    }

    pub(crate) fn agrees_with_authority(&self, authority: &StreamTokenAuthorityRow) -> bool {
        self.stream_incarnation_id == authority.stream_incarnation_id
            && self.contributor_id == authority.contributor_id
            && self.write_id == authority.write_id
            && self.predecessor_token == authority.predecessor_token
            && self.stream_token == authority.current_token
            && self.fold_base_token == authority.fold_base_token
            && self.chain_depth == authority.chain_depth
            && self.origin == authority.origin
            && self.payload_digest == authority.payload_digest
    }
}

/// Complete, unambiguous input to stream-token derivation.
pub(crate) struct StreamTokenInput<'a> {
    pub(crate) identity: TableIdentity,
    pub(crate) logical_id: &'a str,
    pub(crate) stream_incarnation_id: &'a str,
    pub(crate) predecessor_token: Option<StreamToken>,
    pub(crate) write_id: &'a str,
    pub(crate) contributor_id: &'a TrustedContributorId,
    pub(crate) payload_digest: PayloadDigest,
}

impl StreamTokenInput<'_> {
    fn validate(&self) -> ProtocolResult<()> {
        self.identity.validate().map_err(|error| {
            StreamTokenProtocolError::invalid("table_identity", error.to_string())
        })?;
        canonical_uuid_bytes("stream_incarnation_id", self.stream_incarnation_id)?;
        canonical_uuid_bytes("write_id", self.write_id)?;
        if self.contributor_id.as_str().is_empty() {
            return Err(StreamTokenProtocolError::invalid(
                "contributor_id",
                "must be non-empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StreamTokenDisposition {
    Present,
    Withdrawn,
}

/// Terminal evidence required when the current token has been withdrawn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamTerminalCorrection {
    pub(crate) actor: TrustedContributorId,
    pub(crate) correction_id: String,
}

impl StreamTerminalCorrection {
    fn validate(&self) -> ProtocolResult<()> {
        canonical_uuid_bytes("terminal_correction_id", &self.correction_id)?;
        Ok(())
    }
}

/// The one current `_stream_tokens.lance` row for a logical graph key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamTokenAuthorityRow {
    pub(crate) identity: TableIdentity,
    pub(crate) logical_id: String,
    pub(crate) origin_enrollment_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) current_token: StreamToken,
    pub(crate) write_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_token: Option<StreamToken>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) fold_base_token: Option<StreamToken>,
    pub(crate) chain_depth: u32,
    pub(crate) disposition: StreamTokenDisposition,
    pub(crate) contributor_id: TrustedContributorId,
    pub(crate) payload_digest: PayloadDigest,
    pub(crate) origin: StreamRowOrigin,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) terminal_correction: Option<StreamTerminalCorrection>,
}

impl StreamTokenAuthorityRow {
    /// Conservative logical bytes retained by the bounded current-token map.
    /// The logical id is charged twice because both the row and BTree key own
    /// it independently.
    pub(crate) fn lookup_retained_bytes(&self) -> ProtocolResult<u64> {
        let fixed = std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<String>())
            .and_then(|value| value.checked_add(STREAM_LOOKUP_ENTRY_OVERHEAD_BYTES))
            .ok_or_else(|| {
                StreamTokenProtocolError::invalid(
                    "stream_lookup_bytes",
                    "fixed retained-byte sum overflow",
                )
            })?;
        let mut total = u64::try_from(fixed).map_err(|_| {
            StreamTokenProtocolError::invalid("stream_lookup_bytes", "fixed bytes exceed u64")
        })?;
        retained_string_bytes(&mut total, &self.logical_id)?;
        retained_string_bytes(&mut total, &self.logical_id)?;
        retained_string_bytes(&mut total, &self.origin_enrollment_id)?;
        retained_string_bytes(&mut total, &self.stream_incarnation_id)?;
        retained_string_bytes(&mut total, &self.write_id)?;
        retained_string_bytes(&mut total, self.contributor_id.as_str())?;
        retained_origin_bytes(&mut total, &self.origin)?;
        if let Some(correction) = &self.terminal_correction {
            retained_string_bytes(&mut total, correction.actor.as_str())?;
            retained_string_bytes(&mut total, &correction.correction_id)?;
        }
        Ok(total)
    }

    /// Materialize the post-fold PRESENT authority from the exact winning base
    /// metadata. The fold must stage this row and the base effect together.
    pub(crate) fn from_present_metadata(
        identity: TableIdentity,
        logical_id: String,
        origin_enrollment_id: String,
        metadata: &TrustedStreamRowMetadata,
    ) -> ProtocolResult<Self> {
        metadata.validate_for(identity, &logical_id)?;
        let row = Self {
            identity,
            logical_id,
            origin_enrollment_id,
            stream_incarnation_id: metadata.stream_incarnation_id.clone(),
            current_token: metadata.stream_token,
            write_id: metadata.write_id.clone(),
            predecessor_token: metadata.predecessor_token,
            fold_base_token: metadata.fold_base_token,
            chain_depth: metadata.chain_depth,
            disposition: StreamTokenDisposition::Present,
            contributor_id: metadata.contributor_id.clone(),
            payload_digest: metadata.payload_digest,
            origin: metadata.origin.clone(),
            terminal_correction: None,
        };
        row.validate()?;
        Ok(row)
    }

    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        self.identity.validate().map_err(|error| {
            StreamTokenProtocolError::invalid("table_identity", error.to_string())
        })?;
        let enrollment = canonical_uuid("origin_enrollment_id", &self.origin_enrollment_id)?;
        if enrollment.get_version_num() != 4 {
            return Err(StreamTokenProtocolError::invalid(
                "origin_enrollment_id",
                "must be a UUID v4 value",
            ));
        }
        canonical_uuid_bytes("stream_incarnation_id", &self.stream_incarnation_id)?;
        canonical_uuid_bytes("write_id", &self.write_id)?;
        self.origin.validate()?;
        if self.chain_depth == 0 {
            return Err(StreamTokenProtocolError::invalid(
                "chain_depth",
                "must be non-zero",
            ));
        }
        match (self.disposition, &self.terminal_correction) {
            (StreamTokenDisposition::Present, None) => {}
            (StreamTokenDisposition::Present, Some(_)) => {
                return Err(StreamTokenProtocolError::invalid(
                    "terminal_correction",
                    "must be null for PRESENT token authority",
                ));
            }
            (StreamTokenDisposition::Withdrawn, Some(correction)) => correction.validate()?,
            (StreamTokenDisposition::Withdrawn, None) => {
                return Err(StreamTokenProtocolError::invalid(
                    "terminal_correction",
                    "must be present for WITHDRAWN token authority",
                ));
            }
        }
        let derived = StreamToken::derive(&StreamTokenInput {
            identity: self.identity,
            logical_id: &self.logical_id,
            stream_incarnation_id: &self.stream_incarnation_id,
            predecessor_token: self.predecessor_token,
            write_id: &self.write_id,
            contributor_id: &self.contributor_id,
            payload_digest: self.payload_digest,
        })?;
        if derived != self.current_token {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "token row for '{}' does not match its canonical preimage",
                self.logical_id
            )));
        }
        Ok(())
    }
}

/// Durable representation shared by recovery-v12 and the immutable
/// `graph_commit` lineage row.
///
/// The enclosing `Option` on graph lineage is the discriminator: ordinary
/// commits carry no value, while a stream fold carries all three fields as one
/// indivisible commitment.  Keeping the exact recovery wire field names here
/// avoids a second representation that can drift from the committed audit
/// payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamFoldAttributionSummary {
    pub(crate) visible_contributor_count: u64,
    pub(crate) visible_write_count: u64,
    pub(crate) winning_attribution_digest: String,
}

impl StreamFoldAttributionSummary {
    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        if self.visible_contributor_count == 0 {
            return Err(StreamTokenProtocolError::invalid(
                "visible_contributor_count",
                "must be non-zero",
            ));
        }
        if self.visible_write_count == 0 {
            return Err(StreamTokenProtocolError::invalid(
                "visible_write_count",
                "must be non-zero",
            ));
        }
        if self.visible_contributor_count > self.visible_write_count {
            return Err(StreamTokenProtocolError::invalid(
                "visible_contributor_count",
                "must not exceed visible_write_count",
            ));
        }
        decode_sha256(
            "winning_attribution_digest",
            &self.winning_attribution_digest,
        )?;
        Ok(())
    }
}

/// Hash the RFC-026 sorted winning
/// `(contributor_id, stream_token, write_id, tagged_origin)` tuples.
pub(crate) fn stream_fold_attribution_commitment(
    winners: &[StreamTokenAuthorityRow],
) -> ProtocolResult<StreamFoldAttributionSummary> {
    if winners.is_empty() {
        return Err(StreamTokenProtocolError::invalid(
            "fold_winners",
            "must contain at least one attributed winner",
        ));
    }

    let mut contributors = Vec::with_capacity(winners.len());
    let mut tuples = Vec::with_capacity(winners.len());
    for winner in winners {
        winner.validate()?;
        if winner.disposition != StreamTokenDisposition::Present {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "fold winner '{}' is not PRESENT",
                winner.logical_id
            )));
        }
        contributors.push(winner.contributor_id.as_str().as_bytes().to_vec());

        let mut tuple = Vec::new();
        tuple.extend_from_slice(
            &u64::try_from(winner.contributor_id.as_str().len())
                .map_err(|_| {
                    StreamTokenProtocolError::invalid(
                        "contributor_id",
                        "length exceeds the canonical u64 envelope",
                    )
                })?
                .to_be_bytes(),
        );
        tuple.extend_from_slice(winner.contributor_id.as_str().as_bytes());
        tuple.extend_from_slice(winner.current_token.as_bytes());
        tuple.extend_from_slice(&canonical_uuid_bytes("write_id", &winner.write_id)?);
        match &winner.origin {
            StreamRowOrigin::Admission {
                admission_attempt_id,
                caller_ordinal,
            } => {
                tuple.push(0);
                tuple.extend_from_slice(&canonical_uuid_bytes(
                    "admission_attempt_id",
                    admission_attempt_id,
                )?);
                tuple.extend_from_slice(&caller_ordinal.to_be_bytes());
            }
            StreamRowOrigin::Correction {
                correction_id,
                plan_ordinal,
            } => {
                tuple.push(1);
                tuple.extend_from_slice(&canonical_uuid_bytes("correction_id", correction_id)?);
                tuple.extend_from_slice(&plan_ordinal.to_be_bytes());
            }
        }
        tuples.push(tuple);
    }
    contributors.sort();
    contributors.dedup();
    tuples.sort();

    let mut hasher = Sha256::new();
    hasher.update(FOLD_ATTRIBUTION_DOMAIN_V1);
    hash_u64(
        &mut hasher,
        u64::try_from(tuples.len())
            .map_err(|_| StreamTokenProtocolError::invalid("fold_winners", "count exceeds u64"))?,
    );
    for tuple in tuples {
        hash_bytes(&mut hasher, &tuple);
    }
    Ok(StreamFoldAttributionSummary {
        visible_contributor_count: u64::try_from(contributors.len()).map_err(|_| {
            StreamTokenProtocolError::invalid("fold_contributors", "count exceeds u64")
        })?,
        visible_write_count: u64::try_from(winners.len())
            .map_err(|_| StreamTokenProtocolError::invalid("fold_winners", "count exceeds u64"))?,
        winning_attribution_digest: format!("sha256:{:x}", hasher.finalize()),
    })
}

/// Fully normalized trusted request presented to the admission classifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmissionRequest {
    pub(crate) identity: TableIdentity,
    pub(crate) logical_id: String,
    pub(crate) envelope: StreamWriteEnvelope,
    pub(crate) contributor_id: TrustedContributorId,
    pub(crate) payload_digest: PayloadDigest,
}

impl AdmissionRequest {
    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        self.identity.validate().map_err(|error| {
            StreamTokenProtocolError::invalid("table_identity", error.to_string())
        })?;
        self.envelope.validate()
    }

    pub(crate) fn candidate_token(&self) -> ProtocolResult<StreamToken> {
        StreamToken::derive(&StreamTokenInput {
            identity: self.identity,
            logical_id: &self.logical_id,
            stream_incarnation_id: &self.envelope.stream_incarnation_id,
            predecessor_token: self.envelope.predecessor_token,
            write_id: &self.envelope.write_id,
            contributor_id: &self.contributor_id,
            payload_digest: self.payload_digest,
        })
    }
}

/// Effect-free result of comparing a normalized row with durable authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdmissionClassification {
    New {
        candidate_token: StreamToken,
    },
    AlreadyDurable {
        authority: StreamTokenAuthorityRow,
        /// Required for PRESENT; absent for WITHDRAWN because the visible base
        /// row may legitimately be absent or contain the older value.
        present_metadata: Option<TrustedStreamRowMetadata>,
    },
    SequenceConflict {
        current_token: Option<StreamToken>,
    },
    IdempotencyConflict {
        current_token: StreamToken,
    },
    BindingChanged {
        current_stream_incarnation_id: String,
    },
}

/// Validate the redundant base-row copy against the manifest-selected token
/// authority for one logical key.
///
/// The base field is not a sequencing authority, but it is durable evidence
/// that must agree whenever the selected token is PRESENT. A non-null base
/// copy with no selected token row is likewise corruption: treating it as an
/// empty chain would let a later write silently reset sequencing. WITHDRAWN
/// deliberately permits an absent or older visible base row.
pub(crate) fn validate_authority_base_pair(
    identity: TableIdentity,
    logical_id: &str,
    current_authority: Option<&StreamTokenAuthorityRow>,
    current_base_metadata: Option<&TrustedStreamRowMetadata>,
) -> ProtocolResult<Option<TrustedStreamRowMetadata>> {
    let Some(current) = current_authority else {
        if current_base_metadata.is_some() {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "base row for '{logical_id}' carries trusted stream metadata but the manifest-selected token authority has no row"
            )));
        }
        return Ok(None);
    };

    current.validate()?;
    if current.identity != identity || current.logical_id != logical_id {
        return Err(StreamTokenProtocolError::Corruption(format!(
            "selected token row ({}, '{}') does not match requested key ({identity}, '{logical_id}')",
            current.identity, current.logical_id
        )));
    }
    match current.disposition {
        StreamTokenDisposition::Present => {
            let metadata = current_base_metadata.ok_or_else(|| {
                StreamTokenProtocolError::Corruption(format!(
                    "PRESENT token row for '{logical_id}' has no matching base-row stream metadata"
                ))
            })?;
            metadata.validate_for(identity, logical_id)?;
            if !metadata.agrees_with_authority(current) {
                return Err(StreamTokenProtocolError::Corruption(format!(
                    "PRESENT token row for '{logical_id}' disagrees with base-row stream metadata"
                )));
            }
            Ok(Some(metadata.clone()))
        }
        StreamTokenDisposition::Withdrawn => Ok(None),
    }
}

/// Classify one occurrence before minting an origin or calling Lance.
///
/// `current_stream_incarnation_id` comes from the revalidated lifecycle/token
/// authority. `current_authority` comes only from the manifest-selected token
/// dataset version. Every PRESENT authority is checked against its base-row
/// copy before either an idempotent retry or a successor may proceed. This
/// prevents a later occurrence from silently healing divergent authority.
pub(crate) fn classify_admission(
    current_stream_incarnation_id: &str,
    request: &AdmissionRequest,
    current_authority: Option<&StreamTokenAuthorityRow>,
    current_base_metadata: Option<&TrustedStreamRowMetadata>,
) -> ProtocolResult<AdmissionClassification> {
    request.validate()?;
    canonical_uuid_bytes(
        "current_stream_incarnation_id",
        current_stream_incarnation_id,
    )?;
    if request.envelope.stream_incarnation_id != current_stream_incarnation_id {
        return Ok(AdmissionClassification::BindingChanged {
            current_stream_incarnation_id: current_stream_incarnation_id.to_string(),
        });
    }

    let candidate = request.candidate_token()?;
    let present_metadata = validate_authority_base_pair(
        request.identity,
        &request.logical_id,
        current_authority,
        current_base_metadata,
    )?;
    let Some(current) = current_authority else {
        return if request.envelope.predecessor_token.is_none() {
            Ok(AdmissionClassification::New {
                candidate_token: candidate,
            })
        } else {
            Ok(AdmissionClassification::SequenceConflict {
                current_token: None,
            })
        };
    };

    if current.stream_incarnation_id != current_stream_incarnation_id {
        return Err(StreamTokenProtocolError::Corruption(format!(
            "token row for '{}' names stream incarnation '{}' but lifecycle authority names '{}'",
            current.logical_id, current.stream_incarnation_id, current_stream_incarnation_id
        )));
    }

    let same_occurrence = current.write_id == request.envelope.write_id
        && current.predecessor_token == request.envelope.predecessor_token;
    if same_occurrence
        && (current.contributor_id != request.contributor_id
            || current.payload_digest != request.payload_digest)
    {
        return Ok(AdmissionClassification::IdempotencyConflict {
            current_token: current.current_token,
        });
    }

    if candidate == current.current_token {
        let all_token_fields_match = same_occurrence
            && current.stream_incarnation_id == request.envelope.stream_incarnation_id
            && current.contributor_id == request.contributor_id
            && current.payload_digest == request.payload_digest;
        if !all_token_fields_match {
            return Err(StreamTokenProtocolError::Corruption(format!(
                "candidate token for '{}' equals current token but its canonical preimage differs",
                request.logical_id
            )));
        }

        return Ok(AdmissionClassification::AlreadyDurable {
            authority: current.clone(),
            present_metadata,
        });
    }

    if request.envelope.predecessor_token != Some(current.current_token) {
        return Ok(AdmissionClassification::SequenceConflict {
            current_token: Some(current.current_token),
        });
    }

    Ok(AdmissionClassification::New {
        candidate_token: candidate,
    })
}

/// Require nullable protocol fields to be present as either an explicit JSON
/// null or a value. Serde's default `Option<T>` behavior accepts a missing
/// field as `None`, which would silently reinterpret an older payload.
fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn canonical_uuid(field: &'static str, value: &str) -> ProtocolResult<ShardId> {
    let parsed = ShardId::parse_str(value).map_err(|error| {
        StreamTokenProtocolError::invalid(field, format!("must be a UUID: {error}"))
    })?;
    if parsed.is_nil() {
        return Err(StreamTokenProtocolError::invalid(field, "must be non-nil"));
    }
    if parsed.to_string() != value {
        return Err(StreamTokenProtocolError::invalid(
            field,
            "must use canonical lowercase hyphenated UUID text",
        ));
    }
    Ok(parsed)
}

fn canonical_uuid_bytes(field: &'static str, value: &str) -> ProtocolResult<[u8; 16]> {
    Ok(*canonical_uuid(field, value)?.as_bytes())
}

fn decode_sha256(field: &'static str, value: &str) -> ProtocolResult<[u8; 32]> {
    let Some(hex) = value.strip_prefix(SHA256_WIRE_PREFIX) else {
        return Err(StreamTokenProtocolError::invalid(
            field,
            "must use the exact 'sha256:' prefix",
        ));
    };
    if hex.len() != SHA256_HEX_LEN {
        return Err(StreamTokenProtocolError::invalid(
            field,
            "must contain exactly 64 lowercase hexadecimal digits",
        ));
    }
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StreamTokenProtocolError::invalid(
            field,
            "must contain lowercase hexadecimal digits only",
        ));
    }

    let mut bytes = [0_u8; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    let canonical = format!("{SHA256_WIRE_PREFIX}{}", lower_hex(&bytes));
    if canonical != value {
        return Err(StreamTokenProtocolError::invalid(
            field,
            "must round-trip through the canonical sha256-lowerhex-v1 representation",
        ));
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("decode_sha256 validates the alphabet first"),
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(ALPHABET[(byte >> 4) as usize] as char);
        output.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    output
}

fn write_lower_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value);
}

fn hash_optional_digest(hasher: &mut Sha256, value: Option<StreamToken>) {
    match value {
        None => hasher.update([0]),
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.as_bytes());
        }
    }
}

fn append_metadata(builder: &mut StructBuilder, metadata: &TrustedStreamRowMetadata) {
    append_string(builder, 0, Some(&metadata.stream_incarnation_id));
    append_string(builder, 1, Some(metadata.contributor_id.as_str()));
    append_string(builder, 2, Some(&metadata.write_id));
    let predecessor = metadata.predecessor_token.map(|token| token.to_string());
    append_string(builder, 3, predecessor.as_deref());
    let token = metadata.stream_token.to_string();
    append_string(builder, 4, Some(&token));
    let fold_base = metadata.fold_base_token.map(|token| token.to_string());
    append_string(builder, 5, fold_base.as_deref());
    append_u32(builder, 6, Some(metadata.chain_depth));
    append_origin(builder, Some(&metadata.origin));
    let payload = metadata.payload_digest.to_string();
    append_string(builder, 8, Some(&payload));
    builder.append(true);
}

fn append_null_metadata(builder: &mut StructBuilder) {
    // Keep every child nullable because Lance may materialize a null parent as
    // a valid all-null struct during take/projection. The decoder recognizes
    // only that complete all-null shape as absence; partial metadata is never
    // accepted as an unattributed row.
    append_string(builder, 0, None);
    append_string(builder, 1, None);
    append_string(builder, 2, None);
    append_string(builder, 3, None);
    append_string(builder, 4, None);
    append_string(builder, 5, None);
    append_u32(builder, 6, None);
    append_origin(builder, None);
    append_string(builder, 8, None);
    builder.append(false);
}

fn append_origin(builder: &mut StructBuilder, origin: Option<&StreamRowOrigin>) {
    let origin_builder = builder
        .field_builder::<StructBuilder>(7)
        .expect("trusted stream origin uses a StructBuilder");
    match origin {
        Some(StreamRowOrigin::Admission {
            admission_attempt_id,
            caller_ordinal,
        }) => {
            append_string(origin_builder, 0, Some("admission"));
            append_string(origin_builder, 1, Some(admission_attempt_id));
            append_u64(origin_builder, 2, Some(*caller_ordinal));
            append_string(origin_builder, 3, None);
            append_u64(origin_builder, 4, None);
            origin_builder.append(true);
        }
        Some(StreamRowOrigin::Correction {
            correction_id,
            plan_ordinal,
        }) => {
            append_string(origin_builder, 0, Some("correction"));
            append_string(origin_builder, 1, None);
            append_u64(origin_builder, 2, None);
            append_string(origin_builder, 3, Some(correction_id));
            append_u64(origin_builder, 4, Some(*plan_ordinal));
            origin_builder.append(true);
        }
        None => {
            append_string(origin_builder, 0, None);
            append_string(origin_builder, 1, None);
            append_u64(origin_builder, 2, None);
            append_string(origin_builder, 3, None);
            append_u64(origin_builder, 4, None);
            origin_builder.append(false);
        }
    }
}

fn append_string(builder: &mut StructBuilder, field: usize, value: Option<&str>) {
    let builder = builder
        .field_builder::<StringBuilder>(field)
        .expect("trusted stream schema string field uses StringBuilder");
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_u32(builder: &mut StructBuilder, field: usize, value: Option<u32>) {
    let builder = builder
        .field_builder::<UInt32Builder>(field)
        .expect("trusted stream schema u32 field uses UInt32Builder");
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_u64(builder: &mut StructBuilder, field: usize, value: Option<u64>) {
    let builder = builder
        .field_builder::<UInt64Builder>(field)
        .expect("trusted stream schema u64 field uses UInt64Builder");
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn required_string(
    array: &StructArray,
    field: usize,
    row: usize,
    name: &'static str,
) -> ProtocolResult<String> {
    optional_string(array, field, row, name)?.ok_or_else(|| {
        StreamTokenProtocolError::invalid(name, "must be non-null when metadata is present")
    })
}

fn optional_string(
    array: &StructArray,
    field: usize,
    row: usize,
    name: &'static str,
) -> ProtocolResult<Option<String>> {
    let values = array
        .column(field)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| StreamTokenProtocolError::invalid(name, "must have Arrow Utf8 type"))?;
    Ok((!values.is_null(row)).then(|| values.value(row).to_string()))
}

fn required_u32(
    array: &StructArray,
    field: usize,
    row: usize,
    name: &'static str,
) -> ProtocolResult<u32> {
    let values = array
        .column(field)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| StreamTokenProtocolError::invalid(name, "must have Arrow UInt32 type"))?;
    if values.is_null(row) {
        return Err(StreamTokenProtocolError::invalid(
            name,
            "must be non-null when metadata is present",
        ));
    }
    Ok(values.value(row))
}

fn optional_u64(
    array: &StructArray,
    field: usize,
    row: usize,
    name: &'static str,
) -> ProtocolResult<Option<u64>> {
    let values = array
        .column(field)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| StreamTokenProtocolError::invalid(name, "must have Arrow UInt64 type"))?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_INCARNATION: &str = "11111111-1111-4111-8111-111111111111";
    const NEXT_STREAM_INCARNATION: &str = "22222222-2222-4222-8222-222222222222";
    const ENROLLMENT_ID: &str = "33333333-3333-4333-8333-333333333333";
    const WRITE_X: &str = "44444444-4444-4444-8444-444444444444";
    const WRITE_Y: &str = "55555555-5555-4555-8555-555555555555";
    const ATTEMPT_X: &str = "66666666-6666-4666-8666-666666666666";
    const ATTEMPT_Y: &str = "77777777-7777-4777-8777-777777777777";

    fn identity() -> TableIdentity {
        TableIdentity::new(7, 9).unwrap()
    }

    fn contributor(value: &str) -> TrustedContributorId {
        TrustedContributorId::new(value).unwrap()
    }

    fn payload(byte: u8) -> PayloadDigest {
        PayloadDigest::from_bytes([byte; 32])
    }

    fn request(
        write_id: &str,
        predecessor_token: Option<StreamToken>,
        actor: &str,
        digest: PayloadDigest,
    ) -> AdmissionRequest {
        AdmissionRequest {
            identity: identity(),
            logical_id: "person-17".to_string(),
            envelope: StreamWriteEnvelope {
                stream_incarnation_id: STREAM_INCARNATION.to_string(),
                write_id: write_id.to_string(),
                predecessor_token,
            },
            contributor_id: contributor(actor),
            payload_digest: digest,
        }
    }

    fn present_authority(
        request: &AdmissionRequest,
        token: StreamToken,
        attempt_id: &str,
        ordinal: u64,
    ) -> (StreamTokenAuthorityRow, TrustedStreamRowMetadata) {
        let metadata = TrustedStreamRowMetadata::new_admission(
            request,
            token,
            request.envelope.predecessor_token,
            1,
            attempt_id.to_string(),
            ordinal,
        )
        .unwrap();
        let authority = StreamTokenAuthorityRow::from_present_metadata(
            request.identity,
            request.logical_id.clone(),
            ENROLLMENT_ID.to_string(),
            &metadata,
        )
        .unwrap();
        (authority, metadata)
    }

    #[test]
    fn sha256_wire_form_is_exact_and_serde_uses_only_that_form() {
        let token = StreamToken::from_bytes([0xab; 32]);
        let expected = format!("sha256:{}", "ab".repeat(32));
        assert_eq!(token.to_string(), expected);
        assert_eq!(StreamToken::from_str(&expected).unwrap(), token);
        assert_eq!(
            serde_json::to_string(&token).unwrap(),
            format!("\"{expected}\"")
        );
        assert_eq!(
            serde_json::from_str::<StreamToken>(&format!("\"{expected}\"")).unwrap(),
            token
        );

        for malformed in [
            format!("SHA256:{}", "ab".repeat(32)),
            format!("sha256:{}", "AB".repeat(32)),
            format!(" sha256:{}", "ab".repeat(32)),
            format!("sha256:{} ", "ab".repeat(32)),
            format!("sha256:{}", "ab".repeat(31)),
            format!("sha256:{}g", "ab".repeat(31)),
            "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=".to_string(),
        ] {
            assert!(StreamToken::from_str(&malformed).is_err(), "{malformed}");
        }
    }

    #[test]
    fn token_and_payload_derivations_are_versioned_deterministic_and_bound() {
        let schema_hash = format!("sha256:{}", "0a".repeat(32));
        let digest = PayloadDigest::derive(&PayloadDigestInput {
            identity: identity(),
            accepted_schema_hash: &schema_hash,
            canonical_payload: b"typed-row-v1",
        })
        .unwrap();
        assert_eq!(
            digest.to_string(),
            "sha256:e2221e8a802628e78feb85edea4664cef2bf251118c41ba445daaa138d531af1"
        );
        assert_eq!(
            digest,
            PayloadDigest::derive(&PayloadDigestInput {
                identity: identity(),
                accepted_schema_hash: &schema_hash,
                canonical_payload: b"typed-row-v1",
            })
            .unwrap()
        );
        assert_ne!(
            digest,
            PayloadDigest::derive(&PayloadDigestInput {
                identity: identity(),
                accepted_schema_hash: &schema_hash,
                canonical_payload: b"typed-row-v2",
            })
            .unwrap()
        );

        let first_request = request(WRITE_X, None, "actor:alice", digest);
        let token = first_request.candidate_token().unwrap();
        assert_eq!(
            token.to_string(),
            "sha256:6b665af0a10fec538e32469a44454af0ee213dcf5cc2cedd0d472a10cf65b758"
        );
        assert_eq!(token, first_request.candidate_token().unwrap());
        let other_actor = request(WRITE_X, None, "actor:bob", digest);
        assert_ne!(token, other_actor.candidate_token().unwrap());
        let other_table = AdmissionRequest {
            identity: TableIdentity::new(8, 9).unwrap(),
            ..first_request.clone()
        };
        assert_ne!(token, other_table.candidate_token().unwrap());
    }

    #[test]
    fn new_absent_key_requires_null_predecessor_and_matching_incarnation() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let candidate = first.candidate_token().unwrap();
        assert_eq!(
            classify_admission(STREAM_INCARNATION, &first, None, None).unwrap(),
            AdmissionClassification::New {
                candidate_token: candidate
            }
        );

        let stale_predecessor = request(
            WRITE_X,
            Some(StreamToken::from_bytes([9; 32])),
            "actor:alice",
            payload(1),
        );
        assert_eq!(
            classify_admission(STREAM_INCARNATION, &stale_predecessor, None, None).unwrap(),
            AdmissionClassification::SequenceConflict {
                current_token: None
            }
        );

        let mut stale_incarnation = first;
        stale_incarnation.envelope.stream_incarnation_id = NEXT_STREAM_INCARNATION.to_string();
        assert_eq!(
            classify_admission(STREAM_INCARNATION, &stale_incarnation, None, None).unwrap(),
            AdmissionClassification::BindingChanged {
                current_stream_incarnation_id: STREAM_INCARNATION.to_string()
            }
        );
    }

    #[test]
    fn exact_current_retry_returns_persisted_origin_and_certificate() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (authority, metadata) = present_authority(&first, token, ATTEMPT_X, 17);
        assert_eq!(
            classify_admission(
                STREAM_INCARNATION,
                &first,
                Some(&authority),
                Some(&metadata),
            )
            .unwrap(),
            AdmissionClassification::AlreadyDurable {
                authority,
                present_metadata: Some(metadata),
            }
        );
    }

    #[test]
    fn same_occurrence_reused_by_another_actor_or_payload_is_idempotency_conflict() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (authority, metadata) = present_authority(&first, token, ATTEMPT_X, 0);

        for reused in [
            request(WRITE_X, None, "actor:bob", payload(1)),
            request(WRITE_X, None, "actor:alice", payload(2)),
        ] {
            assert_eq!(
                classify_admission(
                    STREAM_INCARNATION,
                    &reused,
                    Some(&authority),
                    Some(&metadata),
                )
                .unwrap(),
                AdmissionClassification::IdempotencyConflict {
                    current_token: token
                }
            );
        }
    }

    #[test]
    fn x_then_y_then_retry_x_is_a_sequence_conflict_not_a_stale_write() {
        let x = request(WRITE_X, None, "actor:alice", payload(1));
        let token_x = x.candidate_token().unwrap();
        let y = request(WRITE_Y, Some(token_x), "actor:alice", payload(2));
        let token_y = y.candidate_token().unwrap();
        let (authority_y, metadata_y) = present_authority(&y, token_y, ATTEMPT_Y, 1);

        assert_eq!(
            classify_admission(
                STREAM_INCARNATION,
                &x,
                Some(&authority_y),
                Some(&metadata_y),
            )
            .unwrap(),
            AdmissionClassification::SequenceConflict {
                current_token: Some(token_y)
            }
        );
    }

    #[test]
    fn successor_must_name_the_complete_current_token() {
        let x = request(WRITE_X, None, "actor:alice", payload(1));
        let token_x = x.candidate_token().unwrap();
        let (authority_x, metadata_x) = present_authority(&x, token_x, ATTEMPT_X, 0);
        let y = request(WRITE_Y, Some(token_x), "actor:alice", payload(2));
        let token_y = y.candidate_token().unwrap();
        assert_eq!(
            classify_admission(
                STREAM_INCARNATION,
                &y,
                Some(&authority_x),
                Some(&metadata_x),
            )
            .unwrap(),
            AdmissionClassification::New {
                candidate_token: token_y
            }
        );

        // The UUID alone is not the occurrence key. Reusing it against the
        // newly current predecessor is a distinct (though SDK-discouraged)
        // occurrence and remains safe because its full token changes.
        let reused_uuid = request(WRITE_X, Some(token_x), "actor:alice", payload(2));
        assert!(matches!(
            classify_admission(
                STREAM_INCARNATION,
                &reused_uuid,
                Some(&authority_x),
                Some(&metadata_x),
            )
            .unwrap(),
            AdmissionClassification::New { .. }
        ));
    }

    #[test]
    fn present_authority_requires_exact_base_metadata_but_withdrawn_does_not() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (authority, mut metadata) = present_authority(&first, token, ATTEMPT_X, 0);
        assert!(
            classify_admission(
                STREAM_INCARNATION,
                &first,
                None,
                Some(&metadata),
            )
            .is_err(),
            "a base attribution without selected token authority must not reset to an empty chain"
        );
        assert!(
            validate_authority_base_pair(first.identity, &first.logical_id, None, Some(&metadata))
                .is_err(),
            "the shared admission/fold validator must reject orphaned base attribution"
        );
        assert!(classify_admission(STREAM_INCARNATION, &first, Some(&authority), None).is_err());
        metadata.origin = StreamRowOrigin::Admission {
            admission_attempt_id: ATTEMPT_Y.to_string(),
            caller_ordinal: 0,
        };
        assert!(
            classify_admission(
                STREAM_INCARNATION,
                &first,
                Some(&authority),
                Some(&metadata),
            )
            .is_err()
        );

        let withdrawn = StreamTokenAuthorityRow {
            disposition: StreamTokenDisposition::Withdrawn,
            terminal_correction: Some(StreamTerminalCorrection {
                actor: contributor("actor:operator"),
                correction_id: ATTEMPT_Y.to_string(),
            }),
            ..authority
        };
        assert!(matches!(
            classify_admission(STREAM_INCARNATION, &first, Some(&withdrawn), None).unwrap(),
            AdmissionClassification::AlreadyDurable {
                present_metadata: None,
                ..
            }
        ));
    }

    #[test]
    fn successor_cannot_advance_over_corrupt_present_authority() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (authority, mut metadata) = present_authority(&first, token, ATTEMPT_X, 0);
        let successor = request(WRITE_Y, Some(token), "actor:alice", payload(2));

        assert!(
            classify_admission(
                STREAM_INCARNATION,
                &successor,
                Some(&authority),
                None,
            )
            .is_err()
        );
        metadata.stream_token = successor.candidate_token().unwrap();
        assert!(
            classify_admission(
                STREAM_INCARNATION,
                &successor,
                Some(&authority),
                Some(&metadata),
            )
            .is_err()
        );
    }

    #[test]
    fn row_metadata_enforces_nonzero_chain_and_depth_one_fold_base() {
        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (_, mut metadata) = present_authority(&first, token, ATTEMPT_X, 0);
        metadata.chain_depth = 0;
        assert!(metadata.validate_for(identity(), "person-17").is_err());

        metadata.chain_depth = 1;
        metadata.fold_base_token = Some(StreamToken::from_bytes([9; 32]));
        assert!(metadata.validate_for(identity(), "person-17").is_err());

        metadata.chain_depth = 2;
        metadata.fold_base_token = None;
        assert!(metadata.validate_for(identity(), "person-17").is_ok());
    }

    #[test]
    fn tagged_origin_and_disposition_enforce_variant_nullability() {
        let origin = StreamRowOrigin::Admission {
            admission_attempt_id: ATTEMPT_X.to_string(),
            caller_ordinal: 3,
        };
        let json = serde_json::to_value(&origin).unwrap();
        assert_eq!(json["kind"], "admission");
        assert!(json.get("correction_id").is_none());
        assert!(
            serde_json::from_value::<StreamRowOrigin>(serde_json::json!({
                "kind": "admission",
                "admission_attempt_id": ATTEMPT_X,
                "caller_ordinal": 3,
                "correction_id": ATTEMPT_Y
            }))
            .is_err()
        );

        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (mut authority, _) = present_authority(&first, token, ATTEMPT_X, 0);
        authority.terminal_correction = Some(StreamTerminalCorrection {
            actor: contributor("actor:operator"),
            correction_id: ATTEMPT_Y.to_string(),
        });
        assert!(authority.validate().is_err());
        authority.disposition = StreamTokenDisposition::Withdrawn;
        assert!(authority.validate().is_ok());
        authority.terminal_correction = None;
        assert!(authority.validate().is_err());
    }

    #[test]
    fn fold_attribution_summary_is_complete_canonical_and_serde_exact() {
        let summary = StreamFoldAttributionSummary {
            visible_contributor_count: 2,
            visible_write_count: 3,
            winning_attribution_digest: format!("sha256:{}", "ab".repeat(32)),
        };
        summary.validate().unwrap();
        let encoded = serde_json::to_string(&summary).unwrap();
        assert_eq!(
            serde_json::from_str::<StreamFoldAttributionSummary>(&encoded).unwrap(),
            summary
        );
        assert!(
            serde_json::from_value::<StreamFoldAttributionSummary>(serde_json::json!({
                "visible_contributor_count": 2,
                "visible_write_count": 3,
                "winning_attribution_digest": format!("sha256:{}", "ab".repeat(32)),
                "unexpected": true
            }))
            .is_err()
        );

        for invalid in [
            StreamFoldAttributionSummary {
                visible_contributor_count: 0,
                ..summary.clone()
            },
            StreamFoldAttributionSummary {
                visible_contributor_count: 4,
                ..summary.clone()
            },
            StreamFoldAttributionSummary {
                visible_write_count: 0,
                ..summary.clone()
            },
            StreamFoldAttributionSummary {
                winning_attribution_digest: format!("sha256:{}", "AB".repeat(32)),
                ..summary.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn hidden_arrow_struct_is_exact_nullable_and_round_trips_trusted_metadata() {
        let field = trusted_stream_metadata_field();
        assert_eq!(field.name(), crate::db::STREAM_METADATA_COLUMN);
        assert!(field.is_nullable());
        validate_trusted_stream_metadata_field(&field).unwrap();

        let all_null_values = vec![None; 8192];
        let all_null = build_trusted_stream_metadata_array(&all_null_values).unwrap();
        assert_eq!(all_null.data_type(), field.data_type());
        assert_eq!(all_null.len(), all_null_values.len());
        assert_eq!(all_null.null_count(), all_null_values.len());
        let all_null_bytes = all_null.get_array_memory_size();
        assert!(
            all_null_bytes < 1024 * 1024,
            "all-null trusted metadata must not retain eager Utf8 child capacity: {all_null_bytes} bytes"
        );
        assert_eq!(
            decode_trusted_stream_metadata(all_null.as_ref(), all_null_values.len() - 1).unwrap(),
            None
        );

        let first = request(WRITE_X, None, "actor:alice", payload(1));
        let token = first.candidate_token().unwrap();
        let (_, metadata) = present_authority(&first, token, ATTEMPT_X, 9);
        let array = build_trusted_stream_metadata_array(&[None, Some(metadata.clone())]).unwrap();
        assert_eq!(
            decode_trusted_stream_metadata(array.as_ref(), 0).unwrap(),
            None
        );
        assert_eq!(
            decode_trusted_stream_metadata(array.as_ref(), 1).unwrap(),
            Some(metadata)
        );
        assert!(decode_trusted_stream_metadata(array.as_ref(), 2).is_err());

        let wrong = Field::new(crate::db::STREAM_METADATA_COLUMN, DataType::Utf8, true);
        assert!(validate_trusted_stream_metadata_field(&wrong).is_err());

        let missing = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
        assert!(validate_trusted_stream_metadata_schema(&missing).is_err());
        let case_lookalike = Schema::new(vec![Field::new(
            "__OMNIGRAPH_STREAM_V1$",
            trusted_stream_metadata_field().data_type().clone(),
            true,
        )]);
        assert!(validate_trusted_stream_metadata_schema(&case_lookalike).is_err());
        let canonical = Schema::new(vec![trusted_stream_metadata_field()]);
        validate_trusted_stream_metadata_schema(&canonical).unwrap();
    }

    #[test]
    fn uuid_fields_refuse_nil_and_noncanonical_forms() {
        let mut invalid = request(WRITE_X, None, "actor:alice", payload(1));
        invalid.envelope.write_id = "00000000-0000-0000-0000-000000000000".to_string();
        assert!(invalid.validate().is_err());
        invalid.envelope.write_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_uppercase();
        assert!(invalid.validate().is_err());

        assert!(
            serde_json::from_value::<StreamWriteEnvelope>(serde_json::json!({
                "stream_incarnation_id": STREAM_INCARNATION,
                "write_id": WRITE_X
            }))
            .is_err(),
            "nullable protocol fields must be explicit rather than serde-defaulted"
        );
        assert!(
            serde_json::from_value::<StreamWriteEnvelope>(serde_json::json!({
                "stream_incarnation_id": STREAM_INCARNATION,
                "write_id": WRITE_X,
                "predecessor_token": null
            }))
            .is_ok()
        );
    }
}
