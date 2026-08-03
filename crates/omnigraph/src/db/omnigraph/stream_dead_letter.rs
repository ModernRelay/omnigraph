//! Bounded canonical object support for RFC-026 F5 dead-letter publication.
//!
//! This module deliberately owns only one immutable object. Recovery owns when
//! that object may be created and whether its descriptor becomes authoritative.
//! There is no prefix discovery, chunking, replay, deletion, or inventory here.

use std::io::Write as _;
use std::ops::Range;

use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};

use crate::db::manifest::stream_token::{
    MAX_STREAM_DEAD_LETTER_CANDIDATES, MAX_STREAM_DEAD_LETTER_OBJECT_BYTES, PayloadDigest,
    STREAM_DEAD_LETTER_OBJECT_PROTOCOL_VERSION, StreamDeadLetterObjectDescriptor,
    StreamDeadLetterReasonCode, StreamToken, StreamTokenAuthorityRow, StreamTokenDisposition,
};
use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, join_uri};

/// Maximum encoded size of the one immutable NDJSON object owned by a fold.
///
/// Construction writes directly into one capped buffer. A legal generation may
/// still exceed this envelope after JSON escaping and per-candidate authority
/// fields; that case is a pre-object `DataBlock`, not a chunked fallback.
pub(crate) const STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES: u64 =
    MAX_STREAM_DEAD_LETTER_OBJECT_BYTES;

const STREAM_DEAD_LETTER_OBJECT_RESOURCE: &str = "stream_dead_letter_object_encoded_bytes";

pub(crate) fn is_stream_dead_letter_object_limit(error: &OmniError) -> bool {
    matches!(
        error,
        OmniError::ResourceLimitExceeded { resource, .. }
            if resource == STREAM_DEAD_LETTER_OBJECT_RESOURCE
    )
}

/// Borrowed terminal-candidate material used to build one fold-owned object.
///
/// Callers normally construct this from the PRESENT winner row that would have
/// been published by a conflict-free fold. Typed token/digest/reason fields keep
/// arbitrary text out of the durable object grammar.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamDeadLetterObjectCandidate<'a> {
    logical_id: &'a str,
    stream_incarnation_id: &'a str,
    write_id: &'a str,
    current_token: StreamToken,
    predecessor_token: Option<StreamToken>,
    contributor_id: &'a str,
    payload_digest: PayloadDigest,
    reason_code: StreamDeadLetterReasonCode,
    canonical_payload: &'a [u8],
}

impl<'a> StreamDeadLetterObjectCandidate<'a> {
    /// Bind an object candidate to one validated PRESENT winner and its exact
    /// canonical logical payload. The fold can prepare the object before it
    /// replaces that winner with DEAD_LETTERED authority rows.
    pub(crate) fn from_present_winner(
        winner: &'a StreamTokenAuthorityRow,
        reason_code: StreamDeadLetterReasonCode,
        canonical_payload: &'a [u8],
    ) -> Result<Self> {
        winner
            .validate()
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if winner.disposition != StreamTokenDisposition::Present {
            return Err(OmniError::manifest_internal(
                "a dead-letter object candidate must come from PRESENT winner authority",
            ));
        }
        validate_canonical_payload(canonical_payload)?;
        Ok(Self {
            logical_id: &winner.logical_id,
            stream_incarnation_id: &winner.stream_incarnation_id,
            write_id: &winner.write_id,
            current_token: winner.current_token,
            predecessor_token: winner.predecessor_token,
            contributor_id: winner.contributor_id.as_str(),
            payload_digest: winner.payload_digest,
            reason_code,
            canonical_payload,
        })
    }
}

/// Stable mapping returned to the token planner after canonical key ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamDeadLetterCandidateOrdinal {
    pub(crate) logical_id: String,
    pub(crate) candidate_ordinal: u64,
}

/// One fully prepared immutable object. Fields remain private so no caller can
/// mutate bytes after the digest, length, count, path, and ordinals are fixed.
#[derive(Debug, Clone)]
pub(crate) struct PreparedStreamDeadLetterObject {
    descriptor: StreamDeadLetterObjectDescriptor,
    canonical_ndjson: String,
    candidate_ordinals: Vec<StreamDeadLetterCandidateOrdinal>,
    canonical_line_ranges: Vec<Range<usize>>,
}

/// One candidate view over a prepared object's canonical, newline-free line.
///
/// The line borrows the object's sole bounded byte allocation. Recovery may
/// clone it into its sidecar without making preparation retain a second body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedStreamDeadLetterCandidate<'a> {
    pub(crate) logical_id: &'a str,
    pub(crate) candidate_ordinal: u64,
    pub(crate) canonical_ndjson_line: &'a str,
}

impl PreparedStreamDeadLetterObject {
    pub(crate) fn descriptor(&self) -> &StreamDeadLetterObjectDescriptor {
        &self.descriptor
    }

    pub(crate) fn canonical_ndjson(&self) -> &str {
        &self.canonical_ndjson
    }

    pub(crate) fn candidate_ordinals(&self) -> &[StreamDeadLetterCandidateOrdinal] {
        &self.candidate_ordinals
    }

    pub(crate) fn ordered_candidates(
        &self,
    ) -> impl ExactSizeIterator<Item = PreparedStreamDeadLetterCandidate<'_>> + '_ {
        self.candidate_ordinals
            .iter()
            .zip(self.canonical_line_ranges.iter())
            .map(|(candidate, range)| PreparedStreamDeadLetterCandidate {
                logical_id: &candidate.logical_id,
                candidate_ordinal: candidate.candidate_ordinal,
                canonical_ndjson_line: &self.canonical_ndjson[range.clone()],
            })
    }
}

/// Feature-gated measurements from the production dead-letter encoder and
/// verifier. This is evidence plumbing, not a supported SDK surface or an
/// admission contract.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDeadLetterEncodingCostForTest {
    pub candidate_count: u64,
    pub canonical_payload_bytes: u64,
    pub encoded_bytes: u64,
    pub encoded_capacity_bytes: u64,
    pub candidate_view_bytes: u64,
    pub ordinal_index_bytes: u64,
    pub line_range_index_bytes: u64,
    pub verified_candidate_count: u64,
    pub encode_elapsed_micros: u128,
    pub verify_elapsed_micros: u128,
}

/// Exercise the exact production encoder and verifier over caller-owned
/// canonical payloads while keeping private token grammar out of integration
/// tests. The fixed authority fields deliberately choose the larger legal
/// predecessor/reason shapes; only payload bytes and candidate cardinality are
/// variable.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub fn failpoint_measure_stream_dead_letter_object_for_test(
    canonical_payloads: &[Vec<u8>],
) -> Result<StreamDeadLetterEncodingCostForTest> {
    use std::str::FromStr as _;

    const FOLD_OPERATION_ID: &str = "01K3EJ5J5Y7YWCM2DHRZ2Q5WEQ";
    const STREAM_INCARNATION_ID: &str = "11111111-1111-4111-8111-111111111111";
    const CONTRIBUTOR_ID: &str = "agent:f6b4-dead-letter-envelope";

    let logical_ids = (0..canonical_payloads.len())
        .map(|ordinal| format!("dead-letter-envelope-{ordinal:05}"))
        .collect::<Vec<_>>();
    let write_ids = (0..canonical_payloads.len())
        .map(|ordinal| {
            format!(
                "{ordinal:08x}-0000-4000-8000-{ordinal:012x}",
                ordinal = u64::try_from(ordinal).unwrap_or(u64::MAX)
            )
        })
        .collect::<Vec<_>>();
    let current_token = StreamToken::from_str(&format!("sha256:{}", "a".repeat(64)))
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let predecessor_token = StreamToken::from_str(&format!("sha256:{}", "b".repeat(64)))
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let payload_digest = PayloadDigest::from_str(&format!("sha256:{}", "c".repeat(64)))
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let candidates = canonical_payloads
        .iter()
        .enumerate()
        .map(
            |(ordinal, canonical_payload)| StreamDeadLetterObjectCandidate {
                logical_id: logical_ids[ordinal].as_str(),
                stream_incarnation_id: STREAM_INCARNATION_ID,
                write_id: write_ids[ordinal].as_str(),
                current_token,
                predecessor_token: Some(predecessor_token),
                contributor_id: CONTRIBUTOR_ID,
                payload_digest,
                reason_code: StreamDeadLetterReasonCode::MultipleValidationViolations,
                canonical_payload,
            },
        )
        .collect::<Vec<_>>();
    let canonical_payload_bytes = canonical_payloads
        .iter()
        .try_fold(0_u64, |total, payload| {
            total
                .checked_add(u64::try_from(payload.len()).map_err(|_| {
                    OmniError::manifest_internal("dead-letter test payload length exceeds u64")
                })?)
                .ok_or_else(|| {
                    OmniError::manifest_internal("dead-letter test payload byte sum overflow")
                })
        })?;
    let candidate_view_bytes = u64::try_from(
        candidates
            .capacity()
            .saturating_mul(std::mem::size_of::<StreamDeadLetterObjectCandidate<'_>>()),
    )
    .map_err(|_| OmniError::manifest_internal("dead-letter candidate view bytes exceed u64"))?;

    let encode_started = std::time::Instant::now();
    let prepared = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &candidates)?;
    let encode_elapsed_micros = encode_started.elapsed().as_micros();
    let encoded_bytes = u64::try_from(prepared.canonical_ndjson.len())
        .map_err(|_| OmniError::manifest_internal("dead-letter encoded bytes exceed u64"))?;
    let encoded_capacity_bytes = u64::try_from(prepared.canonical_ndjson.capacity())
        .map_err(|_| OmniError::manifest_internal("dead-letter encoded capacity exceeds u64"))?;
    let ordinal_index_bytes = prepared
        .candidate_ordinals
        .iter()
        .try_fold(
            prepared
                .candidate_ordinals
                .capacity()
                .saturating_mul(std::mem::size_of::<StreamDeadLetterCandidateOrdinal>()),
            |total, ordinal| total.checked_add(ordinal.logical_id.capacity()),
        )
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| {
            OmniError::manifest_internal("dead-letter ordinal index bytes exceed u64")
        })?;
    let line_range_index_bytes = u64::try_from(
        prepared
            .canonical_line_ranges
            .capacity()
            .saturating_mul(std::mem::size_of::<Range<usize>>()),
    )
    .map_err(|_| OmniError::manifest_internal("dead-letter line-range bytes exceed u64"))?;

    let verify_started = std::time::Instant::now();
    let verified =
        verify_stream_dead_letter_object(prepared.descriptor(), prepared.canonical_ndjson())?;
    let verify_elapsed_micros = verify_started.elapsed().as_micros();
    let verified_candidate_count = u64::try_from(verified.len()).map_err(|_| {
        OmniError::manifest_internal("dead-letter verified candidate count exceeds u64")
    })?;
    if verified_candidate_count != prepared.descriptor.candidate_count {
        return Err(OmniError::manifest_internal(
            "dead-letter verification returned the wrong candidate count",
        ));
    }

    Ok(StreamDeadLetterEncodingCostForTest {
        candidate_count: prepared.descriptor.candidate_count,
        canonical_payload_bytes,
        encoded_bytes,
        encoded_capacity_bytes,
        candidate_view_bytes,
        ordinal_index_bytes,
        line_range_index_bytes,
        verified_candidate_count,
        encode_elapsed_micros,
        verify_elapsed_micros,
    })
}

/// Result of the sole conditional-create operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamDeadLetterCreateOutcome {
    Created,
    AlreadyPresentVerified,
}

/// Owned, validated line returned to bounded offline payload export.
///
/// The payload remains raw canonical JSON. Decoding an object-sized nested
/// array into `serde_json::Value` here would let a legal 64-MiB object expand
/// into millions of heap nodes during ordinary verification. Offline export
/// preserves the same validated raw value while serializing its bounded page.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DecodedStreamDeadLetterObjectEntry {
    pub(crate) protocol_version: u32,
    pub(crate) candidate_ordinal: u64,
    pub(crate) logical_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) write_id: String,
    pub(crate) current_token: StreamToken,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) predecessor_token: Option<StreamToken>,
    pub(crate) contributor_id: String,
    pub(crate) payload_digest: PayloadDigest,
    pub(crate) reason_code: StreamDeadLetterReasonCode,
    pub(crate) payload: Box<serde_json::value::RawValue>,
}

/// Deterministically order, encode, and describe one fold's losing candidates.
///
/// The returned descriptor is ready before terminal token rows are constructed.
/// Candidate ordinals are zero-based and follow strict raw UTF-8 logical-id
/// order. Duplicate keys and an empty candidate set are structural errors.
pub(crate) fn prepare_stream_dead_letter_object(
    fold_operation_id: &str,
    candidates: &[StreamDeadLetterObjectCandidate<'_>],
) -> Result<PreparedStreamDeadLetterObject> {
    // The test-only failpoint changes only the size of the existing envelope;
    // it does not inject a synthetic terminal outcome. This keeps integration
    // fixtures on the production overflow -> DataBlock classification path
    // without allocating a 64-MiB candidate.
    let encoded_byte_limit = if crate::failpoints::maybe_fail(
        crate::failpoints::names::STREAM_DEAD_LETTER_FORCE_OBJECT_LIMIT,
    )
    .is_err()
    {
        1
    } else {
        STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES
    };
    prepare_stream_dead_letter_object_with_limit(fold_operation_id, candidates, encoded_byte_limit)
}

fn prepare_stream_dead_letter_object_with_limit(
    fold_operation_id: &str,
    candidates: &[StreamDeadLetterObjectCandidate<'_>],
    encoded_byte_limit: u64,
) -> Result<PreparedStreamDeadLetterObject> {
    if candidates.is_empty() {
        return Err(OmniError::manifest_internal(
            "a dead-letter object requires at least one terminal candidate",
        ));
    }
    let candidate_count = u64::try_from(candidates.len())
        .map_err(|_| OmniError::manifest_internal("dead-letter candidate count exceeds u64"))?;
    if candidate_count > MAX_STREAM_DEAD_LETTER_CANDIDATES {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter candidate count {candidate_count} exceeds the generation bound {MAX_STREAM_DEAD_LETTER_CANDIDATES}"
        )));
    }
    let limit = usize::try_from(encoded_byte_limit).map_err(|_| {
        OmniError::manifest_internal("dead-letter encoded-byte bound exceeds usize")
    })?;

    let mut order = (0..candidates.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        candidates[*left]
            .logical_id
            .as_bytes()
            .cmp(candidates[*right].logical_id.as_bytes())
    });
    for pair in order.windows(2) {
        if candidates[pair[0]].logical_id == candidates[pair[1]].logical_id {
            return Err(OmniError::manifest_internal(format!(
                "dead-letter object contains duplicate logical id '{}'",
                candidates[pair[0]].logical_id
            )));
        }
    }

    let mut writer = BoundedNdjsonWriter::new(limit);
    let mut candidate_ordinals = Vec::with_capacity(order.len());
    let mut canonical_line_ranges = Vec::with_capacity(order.len());
    let encoded = (|| -> Result<()> {
        for (ordinal, index) in order.into_iter().enumerate() {
            let candidate = &candidates[index];
            validate_candidate(candidate)?;
            let candidate_ordinal = u64::try_from(ordinal).map_err(|_| {
                OmniError::manifest_internal("dead-letter candidate ordinal exceeds u64")
            })?;
            let line_start = writer.bytes.len();
            write_candidate_line(&mut writer, candidate_ordinal, candidate)?;
            let line_end = writer.bytes.len().checked_sub(1).ok_or_else(|| {
                OmniError::manifest_internal("dead-letter candidate line is unexpectedly empty")
            })?;
            if writer.bytes.get(line_end) != Some(&b'\n') {
                return Err(OmniError::manifest_internal(
                    "dead-letter candidate line is missing its terminal newline",
                ));
            }
            candidate_ordinals.push(StreamDeadLetterCandidateOrdinal {
                logical_id: candidate.logical_id.to_string(),
                candidate_ordinal,
            });
            canonical_line_ranges.push(line_start..line_end);
        }
        Ok(())
    })();
    if writer.exceeded {
        return Err(OmniError::resource_limit(
            STREAM_DEAD_LETTER_OBJECT_RESOURCE,
            encoded_byte_limit,
            u64::try_from(writer.attempted).unwrap_or(u64::MAX),
        ));
    }
    encoded?;

    let encoded_length = u64::try_from(writer.bytes.len()).map_err(|_| {
        OmniError::manifest_internal("dead-letter object encoded length exceeds u64")
    })?;
    let object_digest = sha256_wire(&writer.bytes);
    let canonical_ndjson = String::from_utf8(writer.bytes).map_err(|error| {
        OmniError::manifest_internal(format!(
            "canonical dead-letter object is not valid UTF-8: {error}"
        ))
    })?;
    let descriptor = StreamDeadLetterObjectDescriptor::new(
        fold_operation_id,
        object_digest,
        encoded_length,
        candidate_count,
    )
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    Ok(PreparedStreamDeadLetterObject {
        descriptor,
        canonical_ndjson,
        candidate_ordinals,
        canonical_line_ranges,
    })
}

/// Conditionally create the immutable object, or prove that an ambiguous prior
/// attempt already created the exact bytes. No prefix listing is involved.
pub(crate) async fn create_or_verify_stream_dead_letter_object(
    storage: &dyn StorageAdapter,
    graph_root: &str,
    prepared: &PreparedStreamDeadLetterObject,
) -> Result<StreamDeadLetterCreateOutcome> {
    create_or_verify_exact_stream_dead_letter_object(
        storage,
        graph_root,
        &prepared.descriptor,
        &prepared.canonical_ndjson,
    )
    .await
}

/// Recreate or verify the exact object body carried by a validated recovery
/// sidecar. The decoder re-proves canonical line grammar before this module's
/// sole raw conditional-create gateway is reached.
pub(crate) async fn create_or_verify_recovered_stream_dead_letter_object(
    storage: &dyn StorageAdapter,
    graph_root: &str,
    descriptor: &StreamDeadLetterObjectDescriptor,
    canonical_ndjson: &str,
) -> Result<StreamDeadLetterCreateOutcome> {
    verify_stream_dead_letter_object(descriptor, canonical_ndjson)?;
    create_or_verify_exact_stream_dead_letter_object(
        storage,
        graph_root,
        descriptor,
        canonical_ndjson,
    )
    .await
}

/// Resolve the effect-free recovery cell without manufacturing an object.
/// An absent exact path means the PUT was never observed. A present path is
/// inert retain-all residue, but recovery must still prove it is the exact
/// sidecar-owned body before retiring that ownership record.
pub(crate) async fn verify_existing_recovered_stream_dead_letter_object(
    storage: &dyn StorageAdapter,
    graph_root: &str,
    descriptor: &StreamDeadLetterObjectDescriptor,
    canonical_ndjson: &str,
) -> Result<bool> {
    descriptor
        .validate()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let object_uri = join_uri(graph_root, &descriptor.location);
    let Some(existing) = storage
        .read_text_if_exists_bounded(&object_uri, descriptor.encoded_length)
        .await?
    else {
        return Ok(false);
    };
    verify_exact_stream_dead_letter_object(descriptor, canonical_ndjson, &existing)?;
    Ok(true)
}

async fn create_or_verify_exact_stream_dead_letter_object(
    storage: &dyn StorageAdapter,
    graph_root: &str,
    descriptor: &StreamDeadLetterObjectDescriptor,
    canonical_ndjson: &str,
) -> Result<StreamDeadLetterCreateOutcome> {
    descriptor
        .validate()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let object_uri = join_uri(graph_root, &descriptor.location);
    if storage
        .write_text_if_absent(&object_uri, canonical_ndjson)
        .await?
    {
        return Ok(StreamDeadLetterCreateOutcome::Created);
    }

    let existing = storage
        .read_text_if_exists_bounded(&object_uri, descriptor.encoded_length)
        .await?
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "dead-letter object '{}' disappeared after conditional-create reported it present",
                descriptor.location
            ))
        })?;
    verify_exact_stream_dead_letter_object(descriptor, canonical_ndjson, &existing)?;
    Ok(StreamDeadLetterCreateOutcome::AlreadyPresentVerified)
}

fn verify_exact_stream_dead_letter_object(
    descriptor: &StreamDeadLetterObjectDescriptor,
    canonical_ndjson: &str,
    existing: &str,
) -> Result<()> {
    verify_stream_dead_letter_object(descriptor, existing)?;
    if existing.as_bytes() != canonical_ndjson.as_bytes() {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' already exists with bytes that differ from its recovery-owned descriptor",
            descriptor.location
        )));
    }
    Ok(())
}

/// Verify and decode one descriptor-selected object for bounded offline export.
///
/// The caller supplies the exact object URI from the descriptor; this function
/// never discovers objects by listing. Digest and length are checked before
/// parsing, and the line grammar re-proves count, ordering, and ordinals.
pub(crate) fn verify_stream_dead_letter_object(
    descriptor: &StreamDeadLetterObjectDescriptor,
    stored_text: &str,
) -> Result<Vec<DecodedStreamDeadLetterObjectEntry>> {
    descriptor
        .validate()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let encoded_length = u64::try_from(stored_text.len()).map_err(|_| {
        OmniError::manifest_internal("stored dead-letter object length exceeds u64")
    })?;
    if encoded_length > STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES {
        return Err(OmniError::resource_limit(
            STREAM_DEAD_LETTER_OBJECT_RESOURCE,
            STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES,
            encoded_length,
        ));
    }
    if encoded_length != descriptor.encoded_length {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' length mismatch: expected {}, found {encoded_length}",
            descriptor.location, descriptor.encoded_length
        )));
    }
    let observed_digest = sha256_wire(stored_text.as_bytes());
    if observed_digest != descriptor.object_digest {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' digest mismatch",
            descriptor.location
        )));
    }
    if !stored_text.ends_with('\n') {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' must end with one complete NDJSON record",
            descriptor.location
        )));
    }

    let expected_count = usize::try_from(descriptor.candidate_count).map_err(|_| {
        OmniError::manifest_internal("dead-letter descriptor candidate count exceeds usize")
    })?;
    let mut entries: Vec<DecodedStreamDeadLetterObjectEntry> = Vec::with_capacity(expected_count);
    for (ordinal, line) in stored_text[..stored_text.len() - 1].split('\n').enumerate() {
        if line.is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "dead-letter object '{}' contains an empty NDJSON record",
                descriptor.location
            )));
        }
        let expected_ordinal = u64::try_from(ordinal)
            .map_err(|_| OmniError::manifest_internal("dead-letter decoded ordinal exceeds u64"))?;
        let (entry, _) = decode_stream_dead_letter_canonical_line(
            line,
            expected_ordinal,
            entries.last().map(|prior| prior.logical_id.as_str()),
        )?;
        entries.push(entry);
        if entries.len() > expected_count {
            return Err(OmniError::manifest_internal(format!(
                "dead-letter object '{}' contains more records than its descriptor",
                descriptor.location
            )));
        }
    }
    if entries.len() != expected_count {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' record count mismatch: expected {expected_count}, found {}",
            descriptor.location,
            entries.len()
        )));
    }
    Ok(entries)
}

/// Decode and prove the exact canonical spelling of one newline-free record.
///
/// Recovery uses the borrowed payload bytes to re-prove the terminal row's
/// payload digest without reparsing or guessing where the nested object ends.
pub(crate) fn decode_stream_dead_letter_canonical_line<'a>(
    line: &'a str,
    expected_ordinal: u64,
    prior_logical_id: Option<&str>,
) -> Result<(DecodedStreamDeadLetterObjectEntry, &'a [u8])> {
    if line.is_empty() || line.contains(['\n', '\r']) {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate must be one non-empty JSON object line without a line terminator",
        ));
    }
    let line_len = u64::try_from(line.len()).unwrap_or(u64::MAX);
    if line_len > STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES {
        return Err(OmniError::resource_limit(
            STREAM_DEAD_LETTER_OBJECT_RESOURCE,
            STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES,
            line_len,
        ));
    }
    let entry: DecodedStreamDeadLetterObjectEntry =
        serde_json::from_str(line).map_err(|error| {
            OmniError::manifest_internal(format!(
                "dead-letter candidate contains invalid JSON: {error}"
            ))
        })?;
    validate_decoded_entry(&entry, expected_ordinal, prior_logical_id)?;

    let limit = usize::try_from(STREAM_DEAD_LETTER_OBJECT_MAX_ENCODED_BYTES).map_err(|_| {
        OmniError::manifest_internal("dead-letter encoded-byte bound exceeds usize")
    })?;
    let mut prefix = BoundedNdjsonWriter::new(limit);
    write_candidate_prefix(
        &mut prefix,
        entry.candidate_ordinal,
        &entry.logical_id,
        &entry.stream_incarnation_id,
        &entry.write_id,
        &entry.current_token,
        &entry.predecessor_token,
        &entry.contributor_id,
        &entry.payload_digest,
        &entry.reason_code,
    )?;
    let bytes = line.as_bytes();
    if !bytes.starts_with(&prefix.bytes) || bytes.last() != Some(&b'}') {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate does not use the canonical field order or spelling",
        ));
    }
    let payload_end = bytes.len() - 1;
    if prefix.bytes.len() >= payload_end {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate omits its canonical payload",
        ));
    }
    let canonical_payload = &bytes[prefix.bytes.len()..payload_end];
    validate_canonical_payload(canonical_payload)?;
    if entry.payload.get().as_bytes() != canonical_payload {
        return Err(OmniError::manifest_internal(
            "decoded dead-letter payload differs from its canonical line slice",
        ));
    }
    Ok((entry, canonical_payload))
}

fn validate_candidate(candidate: &StreamDeadLetterObjectCandidate<'_>) -> Result<()> {
    if candidate.logical_id.is_empty() {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate logical id must be non-empty",
        ));
    }
    validate_uuid_v4(
        "dead-letter candidate stream_incarnation_id",
        candidate.stream_incarnation_id,
    )?;
    validate_uuid_v4("dead-letter candidate write_id", candidate.write_id)?;
    if candidate.contributor_id.is_empty() {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate contributor id must be non-empty",
        ));
    }
    validate_canonical_payload(candidate.canonical_payload)
}

fn validate_decoded_entry(
    entry: &DecodedStreamDeadLetterObjectEntry,
    expected_ordinal: u64,
    prior_logical_id: Option<&str>,
) -> Result<()> {
    if entry.protocol_version != STREAM_DEAD_LETTER_OBJECT_PROTOCOL_VERSION {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter NDJSON protocol version must be {STREAM_DEAD_LETTER_OBJECT_PROTOCOL_VERSION}"
        )));
    }
    if entry.candidate_ordinal != expected_ordinal {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter candidate ordinal mismatch: expected {expected_ordinal}, found {}",
            entry.candidate_ordinal
        )));
    }
    if entry.logical_id.is_empty()
        || prior_logical_id.is_some_and(|prior| prior.as_bytes() >= entry.logical_id.as_bytes())
    {
        return Err(OmniError::manifest_internal(
            "dead-letter candidate logical ids must be non-empty and strictly UTF-8 ordered",
        ));
    }
    validate_uuid_v4(
        "decoded dead-letter stream_incarnation_id",
        &entry.stream_incarnation_id,
    )?;
    validate_uuid_v4("decoded dead-letter write_id", &entry.write_id)?;
    if entry.contributor_id.is_empty() {
        return Err(OmniError::manifest_internal(
            "decoded dead-letter contributor id must be non-empty",
        ));
    }
    let payload = entry.payload.get().as_bytes();
    if payload.first() != Some(&b'{') || payload.last() != Some(&b'}') {
        return Err(OmniError::manifest_internal(
            "decoded dead-letter payload must be a JSON object",
        ));
    }
    Ok(())
}

fn write_candidate_line(
    writer: &mut BoundedNdjsonWriter,
    candidate_ordinal: u64,
    candidate: &StreamDeadLetterObjectCandidate<'_>,
) -> Result<()> {
    write_candidate_prefix(
        writer,
        candidate_ordinal,
        candidate.logical_id,
        candidate.stream_incarnation_id,
        candidate.write_id,
        &candidate.current_token,
        &candidate.predecessor_token,
        candidate.contributor_id,
        &candidate.payload_digest,
        &candidate.reason_code,
    )?;
    write_bytes(writer, candidate.canonical_payload)?;
    write_bytes(writer, b"}\n")
}

#[allow(clippy::too_many_arguments)]
fn write_candidate_prefix(
    writer: &mut BoundedNdjsonWriter,
    candidate_ordinal: u64,
    logical_id: &str,
    stream_incarnation_id: &str,
    write_id: &str,
    current_token: &StreamToken,
    predecessor_token: &Option<StreamToken>,
    contributor_id: &str,
    payload_digest: &PayloadDigest,
    reason_code: &StreamDeadLetterReasonCode,
) -> Result<()> {
    write_bytes(writer, b"{\"protocol_version\":1,\"candidate_ordinal\":")?;
    write_bytes(writer, candidate_ordinal.to_string().as_bytes())?;
    write_bytes(writer, b",\"logical_id\":")?;
    write_json(writer, logical_id)?;
    write_bytes(writer, b",\"stream_incarnation_id\":")?;
    write_json(writer, stream_incarnation_id)?;
    write_bytes(writer, b",\"write_id\":")?;
    write_json(writer, write_id)?;
    write_bytes(writer, b",\"current_token\":")?;
    write_json(writer, current_token)?;
    write_bytes(writer, b",\"predecessor_token\":")?;
    write_json(writer, predecessor_token)?;
    write_bytes(writer, b",\"contributor_id\":")?;
    write_json(writer, contributor_id)?;
    write_bytes(writer, b",\"payload_digest\":")?;
    write_json(writer, payload_digest)?;
    write_bytes(writer, b",\"reason_code\":")?;
    write_json(writer, reason_code)?;
    write_bytes(writer, b",\"payload\":")
}

fn write_json<T: serde::Serialize + ?Sized>(
    writer: &mut BoundedNdjsonWriter,
    value: &T,
) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(|error| {
        if writer.exceeded {
            bounded_writer_error()
        } else {
            OmniError::manifest_internal(format!(
                "failed to encode canonical dead-letter NDJSON: {error}"
            ))
        }
    })
}

fn write_bytes(writer: &mut BoundedNdjsonWriter, bytes: &[u8]) -> Result<()> {
    writer.write_all(bytes).map_err(|error| {
        if writer.exceeded {
            bounded_writer_error()
        } else {
            OmniError::manifest_internal(format!(
                "failed to encode canonical dead-letter NDJSON: {error}"
            ))
        }
    })
}

fn bounded_writer_error() -> OmniError {
    // The public builder replaces this sentinel with the typed limit/actual
    // result after serde unwinds, while genuine serialization errors remain
    // distinguishable in `write_json`.
    OmniError::manifest_internal("canonical dead-letter NDJSON exceeded its bounded writer")
}

fn validate_canonical_payload(payload: &[u8]) -> Result<()> {
    if payload.first() != Some(&b'{') || payload.last() != Some(&b'}') {
        return Err(OmniError::manifest_internal(
            "dead-letter canonical payload must be one JSON object",
        ));
    }
    // Canonical stream payload v1 is compact. Reject insignificant whitespace
    // outside strings so a caller cannot supply an alternate spelling with the
    // same parsed value. Escaped whitespace inside strings remains legal.
    let mut in_string = false;
    let mut escaped = false;
    for &byte in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if byte.is_ascii_whitespace() {
            return Err(OmniError::manifest_internal(
                "dead-letter canonical payload contains non-canonical JSON whitespace",
            ));
        }
    }
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    <serde::de::IgnoredAny as Deserialize>::deserialize(&mut deserializer).map_err(|error| {
        OmniError::manifest_internal(format!(
            "dead-letter canonical payload is invalid JSON: {error}"
        ))
    })?;
    deserializer.end().map_err(|error| {
        OmniError::manifest_internal(format!(
            "dead-letter canonical payload has trailing JSON data: {error}"
        ))
    })
}

fn validate_uuid_v4(field: &str, value: &str) -> Result<()> {
    let parsed = ShardId::parse_str(value).map_err(|error| {
        OmniError::manifest_internal(format!("stream {field} is not a UUID: {error}"))
    })?;
    if parsed.is_nil() || parsed.to_string() != value || parsed.get_version_num() != 4 {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be a canonical non-nil lowercase UUID v4 value"
        )));
    }
    Ok(())
}

fn sha256_wire(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn deserialize_required_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

struct BoundedNdjsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    attempted: usize,
    exceeded: bool,
}

impl BoundedNdjsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            attempted: 0,
            exceeded: false,
        }
    }

    fn reserve_for(&mut self, attempted: usize) -> std::io::Result<()> {
        if attempted <= self.bytes.capacity() {
            return Ok(());
        }
        let doubled = self.bytes.capacity().max(1).saturating_mul(2);
        let target = attempted.max(doubled.min(self.limit));
        let additional = target.saturating_sub(self.bytes.len());
        self.bytes.try_reserve_exact(additional).map_err(|error| {
            std::io::Error::other(format!(
                "canonical dead-letter NDJSON allocation failed within its configured byte cap: {error}"
            ))
        })
    }
}

impl std::io::Write for BoundedNdjsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.attempted = self.bytes.len().saturating_add(bytes.len());
        if self.attempted > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "canonical dead-letter NDJSON exceeds its configured byte cap",
            ));
        }
        self.reserve_for(self.attempted)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::db::manifest::stream_token::StreamDeadLetterReasonCode;
    use crate::storage::ObjectStorageAdapter;

    const FOLD_OPERATION_ID: &str = "01K3EJ5J5Y7YWCM2DHRZ2Q5WEQ";
    const STREAM_INCARNATION_ID: &str = "11111111-1111-4111-8111-111111111111";
    const WRITE_A: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const WRITE_B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

    fn token(nibble: char) -> StreamToken {
        StreamToken::from_str(&format!("sha256:{}", nibble.to_string().repeat(64))).unwrap()
    }

    fn payload_digest(nibble: char) -> PayloadDigest {
        PayloadDigest::from_str(&format!("sha256:{}", nibble.to_string().repeat(64))).unwrap()
    }

    fn candidate<'a>(
        logical_id: &'a str,
        write_id: &'a str,
        current_token: StreamToken,
        payload_digest: PayloadDigest,
        payload: &'a [u8],
    ) -> StreamDeadLetterObjectCandidate<'a> {
        StreamDeadLetterObjectCandidate {
            logical_id,
            stream_incarnation_id: STREAM_INCARNATION_ID,
            write_id,
            current_token,
            predecessor_token: None,
            contributor_id: "actor:alice",
            payload_digest,
            reason_code: StreamDeadLetterReasonCode::UniqueViolation,
            canonical_payload: payload,
        }
    }

    #[test]
    fn encoding_is_deterministic_key_ordered_and_known() {
        let payload_a = br#"{"id":"a","score":1}"#;
        let payload_b = br#"{"id":"b","score":2}"#;
        let a = candidate("a", WRITE_A, token('a'), payload_digest('c'), payload_a);
        let b = candidate("b", WRITE_B, token('b'), payload_digest('d'), payload_b);
        let forward = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[a, b]).unwrap();
        let reverse = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[b, a]).unwrap();

        assert_eq!(forward.canonical_ndjson(), reverse.canonical_ndjson());
        let expected = format!(
            concat!(
                "{{\"protocol_version\":1,\"candidate_ordinal\":0,\"logical_id\":\"a\",",
                "\"stream_incarnation_id\":\"{STREAM_INCARNATION_ID}\",",
                "\"write_id\":\"{WRITE_A}\",\"current_token\":\"sha256:{}\",",
                "\"predecessor_token\":null,\"contributor_id\":\"actor:alice\",",
                "\"payload_digest\":\"sha256:{}\",\"reason_code\":\"UNIQUE_VIOLATION\",",
                "\"payload\":{{\"id\":\"a\",\"score\":1}}}}\n",
                "{{\"protocol_version\":1,\"candidate_ordinal\":1,\"logical_id\":\"b\",",
                "\"stream_incarnation_id\":\"{STREAM_INCARNATION_ID}\",",
                "\"write_id\":\"{WRITE_B}\",\"current_token\":\"sha256:{}\",",
                "\"predecessor_token\":null,\"contributor_id\":\"actor:alice\",",
                "\"payload_digest\":\"sha256:{}\",\"reason_code\":\"UNIQUE_VIOLATION\",",
                "\"payload\":{{\"id\":\"b\",\"score\":2}}}}\n"
            ),
            "a".repeat(64),
            "c".repeat(64),
            "b".repeat(64),
            "d".repeat(64),
            STREAM_INCARNATION_ID = STREAM_INCARNATION_ID,
            WRITE_A = WRITE_A,
            WRITE_B = WRITE_B,
        );
        assert_eq!(forward.canonical_ndjson(), expected);
        assert_eq!(forward.descriptor(), reverse.descriptor());
        assert_eq!(
            forward.candidate_ordinals(),
            &[
                StreamDeadLetterCandidateOrdinal {
                    logical_id: "a".to_string(),
                    candidate_ordinal: 0,
                },
                StreamDeadLetterCandidateOrdinal {
                    logical_id: "b".to_string(),
                    candidate_ordinal: 1,
                },
            ]
        );
        let ordered = forward.ordered_candidates().collect::<Vec<_>>();
        assert_eq!(
            ordered
                .iter()
                .map(|candidate| (candidate.logical_id, candidate.candidate_ordinal))
                .collect::<Vec<_>>(),
            vec![("a", 0), ("b", 1)]
        );
        assert_eq!(
            ordered
                .iter()
                .map(|candidate| format!("{}\n", candidate.canonical_ndjson_line))
                .collect::<String>(),
            forward.canonical_ndjson()
        );
        let (decoded_line, canonical_payload) =
            decode_stream_dead_letter_canonical_line(ordered[0].canonical_ndjson_line, 0, None)
                .unwrap();
        assert_eq!(decoded_line.logical_id, "a");
        assert_eq!(canonical_payload, payload_a);
        assert!(forward.canonical_ndjson().ends_with("}\n"));
        assert_eq!(
            forward.descriptor().encoded_length,
            forward.canonical_ndjson().len() as u64
        );
        assert_eq!(
            forward.descriptor().object_digest,
            sha256_wire(forward.canonical_ndjson().as_bytes())
        );
        assert_eq!(forward.descriptor().candidate_count, 2);
        assert_eq!(
            forward.descriptor().location,
            format!("__dead_letter/v1/{FOLD_OPERATION_ID}.ndjson")
        );

        let decoded =
            verify_stream_dead_letter_object(forward.descriptor(), forward.canonical_ndjson())
                .unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|entry| (entry.logical_id.as_str(), entry.candidate_ordinal))
                .collect::<Vec<_>>(),
            vec![("a", 0), ("b", 1)]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(decoded[0].payload.get()).unwrap(),
            serde_json::json!({"id": "a", "score": 1})
        );
    }

    #[test]
    fn bounded_writer_accepts_one_under_and_exact_then_refuses_one_over() {
        let payload = br#"{"id":"a\"\\\u0000","text":"quote:\" slash:\\ nul:\u0000"}"#;
        let mut candidate = candidate(
            "adversarial-\"-\\-id",
            WRITE_A,
            token('a'),
            payload_digest('b'),
            payload,
        );
        candidate.predecessor_token = Some(token('c'));
        candidate.contributor_id = "agent:f6b4-\"-\\-envelope";
        candidate.reason_code = StreamDeadLetterReasonCode::MultipleValidationViolations;
        let measured = prepare_stream_dead_letter_object_with_limit(
            FOLD_OPERATION_ID,
            &[candidate],
            1024 * 1024,
        )
        .unwrap();
        let exact = measured.canonical_ndjson().len() as u64;

        prepare_stream_dead_letter_object_with_limit(FOLD_OPERATION_ID, &[candidate], exact + 1)
            .expect("an object one byte under its limit must fit");
        prepare_stream_dead_letter_object_with_limit(FOLD_OPERATION_ID, &[candidate], exact)
            .expect("an object exactly at its limit must fit");
        let error = prepare_stream_dead_letter_object_with_limit(
            FOLD_OPERATION_ID,
            &[candidate],
            exact - 1,
        )
        .expect_err("an object one byte over its limit must fail");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } if resource == STREAM_DEAD_LETTER_OBJECT_RESOURCE
                && limit == exact - 1
                && actual > limit
        ));

        let verified =
            verify_stream_dead_letter_object(measured.descriptor(), measured.canonical_ndjson())
                .expect("the adversarial exact spelling must remain decodable and canonical");
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].logical_id, "adversarial-\"-\\-id");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(verified[0].payload.get()).unwrap(),
            serde_json::from_slice::<serde_json::Value>(payload).unwrap()
        );
    }

    #[test]
    fn verifier_retains_nested_payload_as_raw_canonical_json() {
        let payload = format!("{{\"flags\":[{}]}}", vec!["true"; 16_384].join(","));
        let candidate = candidate(
            "nested",
            WRITE_A,
            token('a'),
            payload_digest('b'),
            payload.as_bytes(),
        );
        let prepared = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[candidate]).unwrap();
        let verified =
            verify_stream_dead_letter_object(prepared.descriptor(), prepared.canonical_ndjson())
                .unwrap();

        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].payload.get(), payload);
    }

    #[tokio::test]
    async fn conditional_create_replays_exact_bytes_and_refuses_mismatch() {
        let storage = ObjectStorageAdapter::in_memory();
        let root = "dead-letter-test/graph";
        let payload = br#"{"id":"a","score":1}"#;
        let candidate = candidate("a", WRITE_A, token('a'), payload_digest('b'), payload);
        let prepared = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[candidate]).unwrap();

        assert_eq!(
            create_or_verify_stream_dead_letter_object(&storage, root, &prepared)
                .await
                .unwrap(),
            StreamDeadLetterCreateOutcome::Created
        );
        assert_eq!(
            create_or_verify_stream_dead_letter_object(&storage, root, &prepared)
                .await
                .unwrap(),
            StreamDeadLetterCreateOutcome::AlreadyPresentVerified
        );

        let other_fold = "01K3EJ5J5Y7YWCM2DHRZ2Q5WER";
        let other = prepare_stream_dead_letter_object(other_fold, &[candidate]).unwrap();
        let uri = join_uri(root, &other.descriptor().location);
        storage
            .write_text_if_absent(&uri, "foreign\n")
            .await
            .unwrap();
        let error = create_or_verify_stream_dead_letter_object(&storage, root, &other)
            .await
            .expect_err("foreign bytes at an owned path must fail closed");
        assert!(error.to_string().contains("length mismatch"), "{error}");

        let oversized_fold = "01K3EJ5J5Y7YWCM2DHRZ2Q5WES";
        let oversized = prepare_stream_dead_letter_object(oversized_fold, &[candidate]).unwrap();
        let oversized_uri = join_uri(root, &oversized.descriptor().location);
        storage
            .write_text_if_absent(
                &oversized_uri,
                &"x".repeat(oversized.descriptor().encoded_length as usize + 1024),
            )
            .await
            .unwrap();
        let error = create_or_verify_stream_dead_letter_object(&storage, root, &oversized)
            .await
            .expect_err("an oversized replacement must be capped before full materialization");
        let expected_limit = oversized.descriptor().encoded_length;
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit,
                actual,
            } if resource == "storage_text_bytes"
                && limit == expected_limit
                && actual == limit + 1
        ));
    }

    #[test]
    fn decoder_rejects_digest_length_count_and_ordinal_drift() {
        let payload = br#"{"id":"a","score":1}"#;
        let candidate = candidate("a", WRITE_A, token('a'), payload_digest('b'), payload);
        let prepared = prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[candidate]).unwrap();

        let mut wrong_length = prepared.descriptor().clone();
        wrong_length.encoded_length += 1;
        assert!(
            verify_stream_dead_letter_object(&wrong_length, prepared.canonical_ndjson()).is_err()
        );

        let mut wrong_count = prepared.descriptor().clone();
        wrong_count.candidate_count = 2;
        assert!(
            verify_stream_dead_letter_object(&wrong_count, prepared.canonical_ndjson()).is_err()
        );

        let changed = prepared.canonical_ndjson().replacen(
            "\"candidate_ordinal\":0",
            "\"candidate_ordinal\":1",
            1,
        );
        let changed_descriptor = StreamDeadLetterObjectDescriptor::new(
            FOLD_OPERATION_ID,
            sha256_wire(changed.as_bytes()),
            changed.len() as u64,
            1,
        )
        .unwrap();
        assert!(verify_stream_dead_letter_object(&changed_descriptor, &changed).is_err());

        let reordered = prepared.canonical_ndjson().replacen(
            "\"protocol_version\":1,\"candidate_ordinal\":0",
            "\"candidate_ordinal\":0,\"protocol_version\":1",
            1,
        );
        let reordered_descriptor = StreamDeadLetterObjectDescriptor::new(
            FOLD_OPERATION_ID,
            sha256_wire(reordered.as_bytes()),
            reordered.len() as u64,
            1,
        )
        .unwrap();
        assert!(
            verify_stream_dead_letter_object(&reordered_descriptor, &reordered).is_err(),
            "semantically equal but noncanonical field order must fail"
        );
    }

    #[test]
    fn candidates_refuse_duplicates_noncanonical_payloads_and_empty_sets() {
        let payload = br#"{"id":"a"}"#;
        let duplicate = candidate("a", WRITE_A, token('a'), payload_digest('b'), payload);
        assert!(prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[]).is_err());
        assert!(
            prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[duplicate, duplicate]).is_err()
        );

        let spaced = br#"{"id": "a"}"#;
        let noncanonical = candidate("a", WRITE_A, token('a'), payload_digest('b'), spaced);
        assert!(prepare_stream_dead_letter_object(FOLD_OPERATION_ID, &[noncanonical]).is_err());
    }
}
