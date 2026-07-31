#![allow(dead_code)]

//! Pure RFC-026 lifecycle-v3 claim and quiescence protocol.
//!
//! This module deliberately owns no worker, recovery, or manifest publication.
//! It authenticates one bounded Lance WAL suffix and derives the immutable
//! claim/lifecycle values that those owners may later persist. Every API stays
//! crate-private until the capability-bound firehose surface is activated.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{Schema, SchemaRef};
use arrow_select::take::take;
use lance::dataset::mem_wal::{TOMBSTONE, WalReadEntry, WalTailer, schema_with_tombstone};
use lance_index::mem_wal::ShardId;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::db::manifest::TableIdentity;
use crate::db::manifest::stream::{
    AuthenticatedWalTail, ClaimAttemptClassification, ClaimAttemptEffect,
    ClaimAttemptEffectPreimage, ClaimProfile, ClaimReceipt, ClaimReceiptPreimage,
    ClaimTerminalClassification, DisableDrainAdoption, DrainDescriptor, DrainGoal, LastFoldOutcome,
    LastFoldSummary, ManagementReceipt, QUIESCE_REQUEST_PROTOCOL_VERSION, QuiesceRequestPayload,
    STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION, STREAM_RESUME_OPERATION_KIND,
    STREAM_RESUME_REQUEST_PROTOCOL_VERSION, SealedProof, StreamGenerationCut, StreamLifecycle,
    StreamLifecycleEntry, StreamResumeMode, StreamResumeRequestPayload, StrictBlock,
    authenticated_wal_tail_chain_digest,
    stream_physical_binding_digest, stream_quiesce_result_payload, stream_resume_result_payload,
};
use crate::db::manifest::stream_profile::ReceiptChainRef;
use crate::db::manifest::stream_token::{
    PayloadDigest, PayloadDigestInput, StreamToken, TrustedStreamRowMetadata,
    decode_trusted_stream_metadata, validate_trusted_stream_metadata_schema,
};
use crate::error::{MergeConflictKind, OmniError, Result};
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS, b1_logical_batch_bytes,
};

use super::canonical_stream_payload_v1;

const CLAIM_OPERATION_PROTOCOL_VERSION: u32 = 1;
const CLAIM_ATTEMPT_PLAN_PROTOCOL_VERSION: u32 = 1;
const CLAIM_WAL_SEGMENT_PROTOCOL_VERSION: u32 = 1;
const VERIFIED_EMPTY_PROTOCOL_VERSION: u32 = 1;

const QUIESCE_PRESTATE_DOMAIN: &[u8] = b"omnigraph.stream-quiesce-prestate.v1\0";
const CLAIM_PRESTATE_DOMAIN: &[u8] = b"omnigraph.stream-claim-prestate.v1\0";
const CLAIM_OPERATION_DOMAIN: &[u8] = b"omnigraph.stream-claim-operation.v1\0";
const CLAIM_ATTEMPT_PRESTATE_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-prestate.v1\0";
const CLAIM_ATTEMPT_PLAN_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-plan.v1\0";
const CLAIM_ATTEMPT_EFFECT_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-effect.v1\0";
const PLANNED_SENTINEL_DOMAIN: &[u8] = b"omnigraph.stream-planned-sentinel.v1\0";
const EMPTY_FENCE_STATE_DOMAIN: &[u8] = b"omnigraph.stream-empty-fence-state.v1\0";
const CLAIM_WAL_SEGMENT_DOMAIN: &[u8] = b"omnigraph.stream-claim-wal-segment.v1\0";
const CLAIM_WAL_LWW_DOMAIN: &[u8] = b"omnigraph.stream-claim-wal-lww.v1\0";
const CLAIM_TERMINAL_EFFECT_DOMAIN: &[u8] = b"omnigraph.stream-claim-terminal-effect.v1\0";
const VERIFIED_EMPTY_DOMAIN: &[u8] = b"omnigraph.stream-verified-empty.v1\0";
const DATA_BLOCK_VIOLATION_DOMAIN: &[u8] = b"omnigraph.stream-data-block-violations.v1\0";
const DATA_BLOCK_CORRECTION_VIEW_DOMAIN: &[u8] =
    b"omnigraph.stream-data-block-correction-view.v1\0";
const DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_ITEM_DOMAIN: &[u8] =
    b"omnigraph.stream-data-block-correction-view-overflow-item.v1\0";
const DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_VIOLATION_DOMAIN: &[u8] =
    b"omnigraph.stream-data-block-correction-view-overflow-violations.v1\0";
const DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_DIGEST_DOMAIN: &[u8] =
    b"omnigraph.stream-data-block-correction-view-overflow-digest.v1\0";
const DATA_BLOCK_CORRECTION_VIEW_OVERFLOW: &str = "CORRECTION_VIEW_OVERFLOW";
const DATA_BLOCK_CORRECTION_VIEW_MAX_ENTRIES: usize = 8_192;
const DATA_BLOCK_CORRECTION_VIEW_MAX_BYTES: usize = 32 * 1024 * 1024;
const _: () = assert!(DATA_BLOCK_CORRECTION_VIEW_MAX_ENTRIES as u64 == B1_MAX_GENERATION_ROWS);
const _: () = assert!(DATA_BLOCK_CORRECTION_VIEW_MAX_BYTES as u64 == B1_MAX_GENERATION_ARROW_BYTES);

/// At most one data-bearing no-roll generation plus its final empty sentinel
/// can belong to one authenticated segment. Requiring this before the first
/// read prevents a malicious cursor from turning validation into an unbounded
/// sequence of object-store GETs.
const MAX_AUTHENTICATED_SEGMENT_ENTRIES: u64 = B1_MAX_GENERATION_ROWS + 1;

#[derive(Debug, Clone)]
pub(crate) struct QuiesceRequest {
    pub(crate) graph_identity_digest: String,
    pub(crate) drain_id: String,
    pub(crate) expected_lifecycle_revision: u64,
    pub(crate) goal: DrainGoal,
    pub(crate) initiating_actor: String,
    pub(crate) initiated_at: i64,
    pub(crate) target_epoch_floor_by_shard: BTreeMap<String, u64>,
    pub(crate) seal_override: Option<DisableDrainAdoption>,
}

#[derive(Debug, Clone)]
pub(crate) struct StartedDrain {
    pub(crate) lifecycle: StreamLifecycleEntry,
    pub(crate) request_payload: serde_json::Value,
    pub(crate) request_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamResumeRequest {
    pub(crate) graph_identity_digest: String,
    pub(crate) resume_id: String,
    pub(crate) expected_lifecycle_revision: u64,
    pub(crate) mode: StreamResumeMode,
    pub(crate) actor_id: String,
    pub(crate) initiated_at: i64,
    /// Exact graph-branch topology captured under the branch gate. The
    /// bounded profile currently accepts only the empty set.
    pub(crate) public_named_branches: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStreamResumeOpen {
    pub(crate) mode: StreamResumeMode,
    pub(crate) request_payload: serde_json::Value,
    pub(crate) request_digest: String,
    pub(crate) next_lifecycle_revision: u64,
    pub(crate) minimum_next_epoch_floor: u64,
    pub(crate) recorded_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct CanonicalDataViolationBody<'a> {
    table_key: &'a str,
    logical_key: &'a str,
    current_blocked_winner_stream_token: &'a str,
    violation_code: &'static str,
    field_path_or_group: &'a [String],
    violation_instance_id: &'a str,
    allowed_actions: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CanonicalDataBlockEvidence {
    violation_code: String,
    violation_digest: String,
    correction_view_digest: String,
    offending_key_count: u64,
    entry_count: usize,
    canonical_bytes: usize,
}

/// Incremental validator sink for one immutable fold cut.
///
/// Detailed JSON bodies are retained only inside the v1 entry/byte envelope.
/// The one-per-key overflow projection is maintained independently from the
/// first violation, so crossing that envelope drops detail immediately without
/// requiring a second unbounded validator result.
pub(super) struct DataBlockEvidenceCollector<'a> {
    expected_table_key: &'a str,
    winner_tokens: &'a BTreeMap<String, String>,
    detailed_bodies: Option<BTreeSet<Vec<u8>>>,
    detailed_body_bytes: usize,
    offending_keys: BTreeMap<String, String>,
    violation_codes: BTreeSet<&'static str>,
    saw_violation: bool,
    max_detailed_entries: usize,
    max_detailed_bytes: usize,
}

impl<'a> DataBlockEvidenceCollector<'a> {
    pub(super) fn new(
        expected_table_key: &'a str,
        winner_tokens: &'a BTreeMap<String, String>,
    ) -> Self {
        Self::with_limits(
            expected_table_key,
            winner_tokens,
            DATA_BLOCK_CORRECTION_VIEW_MAX_ENTRIES,
            DATA_BLOCK_CORRECTION_VIEW_MAX_BYTES,
        )
    }

    fn with_limits(
        expected_table_key: &'a str,
        winner_tokens: &'a BTreeMap<String, String>,
        max_detailed_entries: usize,
        max_detailed_bytes: usize,
    ) -> Self {
        Self {
            expected_table_key,
            winner_tokens,
            detailed_bodies: Some(BTreeSet::new()),
            detailed_body_bytes: 0,
            offending_keys: BTreeMap::new(),
            violation_codes: BTreeSet::new(),
            saw_violation: false,
            max_detailed_entries,
            max_detailed_bytes,
        }
    }

    pub(super) fn push(&mut self, violation: &crate::validate::Violation) -> Result<()> {
        if violation.corrections.is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "validator {} has no structured stream-correction evidence",
                validation_violation_code(violation.kind)
            )));
        }
        self.saw_violation = true;
        let violation_code = validation_violation_code(violation.kind);
        self.violation_codes.insert(violation_code);

        for correction in &violation.corrections {
            let current_blocked_winner_stream_token = validate_data_block_correction(
                self.expected_table_key,
                violation,
                correction,
                self.winner_tokens,
            )?;
            if let Some(selected_token) = self.offending_keys.get(&correction.logical_key) {
                if selected_token != current_blocked_winner_stream_token {
                    return Err(OmniError::manifest_internal(
                        "validator supplied two current tokens for one stream-correction key",
                    ));
                }
            } else {
                if self.offending_keys.len() == DATA_BLOCK_CORRECTION_VIEW_MAX_ENTRIES {
                    return Err(OmniError::manifest_internal(
                        "stream DataBlock overflow aggregate exceeds the acknowledged-generation key bound",
                    ));
                }
                self.offending_keys.insert(
                    correction.logical_key.clone(),
                    current_blocked_winner_stream_token.to_string(),
                );
            }

            let Some(mut detailed_bodies) = self.detailed_bodies.take() else {
                continue;
            };
            let body = CanonicalDataViolationBody {
                table_key: violation.table_key.as_str(),
                logical_key: correction.logical_key.as_str(),
                current_blocked_winner_stream_token,
                violation_code,
                field_path_or_group: correction.field_path_or_group.as_slice(),
                violation_instance_id: correction.violation_instance_id.as_str(),
                allowed_actions: correction.allowed_actions.as_slice(),
            };
            let Some(sort_bytes) = canonical_json_bytes_with_limit(
                "stream data-block violation body",
                &body,
                self.max_detailed_bytes,
            )?
            else {
                self.detailed_body_bytes = 0;
                continue;
            };
            if detailed_bodies.contains(&sort_bytes) {
                self.detailed_bodies = Some(detailed_bodies);
                continue;
            }
            let next_bytes = self
                .detailed_body_bytes
                .checked_add(sort_bytes.len())
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream DataBlock correction-view body accounting overflow",
                    )
                })?;
            if detailed_bodies.len() == self.max_detailed_entries
                || next_bytes > self.max_detailed_bytes
            {
                self.detailed_body_bytes = 0;
                continue;
            }
            detailed_bodies.insert(sort_bytes);
            self.detailed_body_bytes = next_bytes;
            self.detailed_bodies = Some(detailed_bodies);
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<CanonicalDataBlockEvidence>> {
        if !self.saw_violation {
            return Ok(None);
        }
        if self.offending_keys.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream DataBlock canonical correction view is empty",
            ));
        }
        if let Some(detailed_bodies) = self.detailed_bodies.as_ref() {
            if let Some(detailed) = self.detailed_evidence(detailed_bodies)? {
                return Ok(Some(detailed));
            }
        }
        Ok(Some(self.overflow_evidence()?))
    }

    fn detailed_evidence(
        &self,
        detailed_bodies: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<CanonicalDataBlockEvidence>> {
        let mut serialized_bytes = 0usize;
        for (ordinal, body) in detailed_bodies.iter().enumerate() {
            let record_len = canonical_ordinal_record_len(ordinal, body)?;
            serialized_bytes = serialized_bytes.checked_add(record_len).ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream DataBlock correction-view byte accounting overflow",
                )
            })?;
            if serialized_bytes > self.max_detailed_bytes {
                return Ok(None);
            }
        }

        let mut digests = CanonicalRecordSetDigests::new(detailed_bodies.len());
        for (ordinal, body) in detailed_bodies.iter().enumerate() {
            let record = canonical_ordinal_record_bytes(ordinal, body, self.max_detailed_bytes)?
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "bounded stream DataBlock record no longer fits its precomputed envelope",
                    )
                })?;
            digests.push(ordinal, &record)?;
        }
        let violation_code = if self.violation_codes.len() == 1 {
            self.violation_codes
                .iter()
                .next()
                .expect("one validation code")
                .to_string()
        } else {
            "MULTIPLE_VALIDATION_VIOLATIONS".to_string()
        };
        let (violation_digest, correction_view_digest) = digests.finish()?;
        Ok(Some(CanonicalDataBlockEvidence {
            violation_code,
            violation_digest,
            correction_view_digest,
            offending_key_count: u64::try_from(self.offending_keys.len())
                .map_err(|_| OmniError::manifest_internal("strict-block key count exceeds u64"))?,
            entry_count: detailed_bodies.len(),
            canonical_bytes: serialized_bytes,
        }))
    }

    fn overflow_evidence(&self) -> Result<CanonicalDataBlockEvidence> {
        let count = self.offending_keys.len();
        let mut violation =
            CanonicalHasher::new(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_VIOLATION_DOMAIN);
        let mut correction =
            CanonicalHasher::new(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_DIGEST_DOMAIN);
        let count_u64 = u64::try_from(count).unwrap_or(u64::MAX);
        for digest in [&mut violation, &mut correction] {
            digest.u32(STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION);
            digest.field(self.expected_table_key.as_bytes());
            digest.u64(count_u64);
        }
        let mut canonical_bytes = self.expected_table_key.len();
        for (ordinal, (logical_key, token)) in self.offending_keys.iter().enumerate() {
            let instance_id = overflow_instance_id(self.expected_table_key, logical_key, token);
            for digest in [&mut violation, &mut correction] {
                digest.u64(u64::try_from(ordinal).unwrap_or(u64::MAX));
                digest.field(logical_key.as_bytes());
                digest.field(token.as_bytes());
                digest.field(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW.as_bytes());
                digest.field(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW.as_bytes());
                digest.field(instance_id.as_bytes());
                digest.field(b"REPLACE");
            }
            canonical_bytes = canonical_bytes
                .checked_add(logical_key.len())
                .and_then(|bytes| bytes.checked_add(token.len()))
                .and_then(|bytes| bytes.checked_add(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW.len() * 2))
                .and_then(|bytes| bytes.checked_add(instance_id.len()))
                .and_then(|bytes| bytes.checked_add("REPLACE".len()))
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream DataBlock overflow-view byte accounting overflow",
                    )
                })?;
        }
        Ok(CanonicalDataBlockEvidence {
            violation_code: DATA_BLOCK_CORRECTION_VIEW_OVERFLOW.to_string(),
            violation_digest: violation.finish(),
            correction_view_digest: correction.finish(),
            offending_key_count: u64::try_from(count)
                .map_err(|_| OmniError::manifest_internal("strict-block key count exceeds u64"))?,
            entry_count: count,
            canonical_bytes,
        })
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.bytes.len());
        if buffer.len() > remaining {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "canonical stream data-block JSON exceeds its bound",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Build the exact metadata-only terminal for a permanent validator failure
/// discovered while an explicit drain owns exclusive admission.
pub(super) fn build_draining_data_block(
    prior: &StreamLifecycleEntry,
    cut: StreamGenerationCut,
    canonical: CanonicalDataBlockEvidence,
    input_rows: u64,
    input_bytes: u64,
    recorded_at: i64,
) -> Result<StreamLifecycleEntry> {
    prior.validate()?;
    let drain = prior.drain.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("stream data block requires an active drain")
    })?;
    if prior.lifecycle != StreamLifecycle::Draining
        || prior.strict_block.is_some()
        || prior.sealed_proof.is_some()
        || canonical.entry_count == 0
        || canonical.offending_key_count == 0
        || canonical.canonical_bytes == 0
        || input_rows == 0
        || recorded_at <= 0
    {
        return Err(OmniError::manifest_internal(
            "stream data block requires one unblocked DRAINING cut and concrete validator violations",
        ));
    }

    let correction_revision = prior
        .lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream lifecycle revision overflow"))?;
    let block = StrictBlock::new_data_block(
        prior,
        cut.clone(),
        correction_revision,
        STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION,
        canonical.violation_code,
        canonical.violation_digest,
        canonical.correction_view_digest,
        canonical.offending_key_count,
    )?;

    let mut next = prior.clone();
    next.lifecycle_revision = correction_revision;
    next.strict_block = Some(block);
    next.last_fold_summary = Some(LastFoldSummary {
        operation_id: drain.drain_id.clone(),
        graph_commit_id: None,
        exact_generation_cut: cut,
        outcome: LastFoldOutcome::StrictBlocked,
        input_rows,
        input_bytes,
        visible_rows: 0,
        visible_bytes: 0,
        recorded_at,
    });
    next.validate_successor_of(prior)?;
    Ok(next)
}

fn canonical_data_block_evidence_with_limits<'a>(
    expected_table_key: &str,
    violations: &'a [crate::validate::Violation],
    winner_tokens: &'a BTreeMap<String, String>,
    max_entries: usize,
    max_bytes: usize,
) -> Result<CanonicalDataBlockEvidence> {
    let mut collector = DataBlockEvidenceCollector::with_limits(
        expected_table_key,
        winner_tokens,
        max_entries,
        max_bytes,
    );
    for violation in violations {
        collector.push(violation)?;
    }
    collector.finish()?.ok_or_else(|| {
        OmniError::manifest_internal("stream DataBlock canonical correction view is empty")
    })
}

fn validate_data_block_correction<'a>(
    expected_table_key: &str,
    violation: &'a crate::validate::Violation,
    correction: &'a crate::validate::ViolationCorrectionEvidence,
    winner_tokens: &'a BTreeMap<String, String>,
) -> Result<&'a str> {
    if violation.table_key != expected_table_key {
        return Err(OmniError::manifest_internal(
            "validator supplied stream-correction evidence for a foreign table",
        ));
    }
    if violation.corrections.is_empty()
        || correction.logical_key.is_empty()
        || correction.field_path_or_group.is_empty()
        || correction
            .field_path_or_group
            .iter()
            .any(|field| field.is_empty())
        || !is_canonical_sha256_digest(&correction.violation_instance_id)
        || correction.allowed_actions.is_empty()
        || correction
            .allowed_actions
            .iter()
            .any(|action| action != "REPLACE" && action != "WITHDRAW")
    {
        return Err(OmniError::manifest_internal(
            "validator supplied malformed structured stream-correction evidence",
        ));
    }
    winner_tokens
        .get(&correction.logical_key)
        .map(String::as_str)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "validator key '{}' has no current blocked-winner stream token",
                correction.logical_key
            ))
        })
}

fn is_canonical_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

fn overflow_instance_id(table_key: &str, logical_key: &str, token: &str) -> String {
    let mut instance = CanonicalHasher::new(DATA_BLOCK_CORRECTION_VIEW_OVERFLOW_ITEM_DOMAIN);
    instance.field(table_key.as_bytes());
    instance.field(logical_key.as_bytes());
    instance.field(token.as_bytes());
    instance.finish()
}

fn canonical_ordinal_record_len(ordinal: usize, body: &[u8]) -> Result<usize> {
    if body.first() != Some(&b'{') {
        return Err(OmniError::manifest_internal(
            "stream DataBlock canonical body is not a JSON object",
        ));
    }
    let prefix = format!(
        "{{\"entry_ordinal\":{},",
        u64::try_from(ordinal).unwrap_or(u64::MAX)
    );
    prefix
        .len()
        .checked_add(body.len().saturating_sub(1))
        .ok_or_else(|| {
            OmniError::manifest_internal("stream DataBlock canonical record length overflow")
        })
}

fn canonical_ordinal_record_bytes(
    ordinal: usize,
    body: &[u8],
    limit: usize,
) -> Result<Option<Vec<u8>>> {
    let record_len = canonical_ordinal_record_len(ordinal, body)?;
    if record_len > limit {
        return Ok(None);
    }
    let prefix = format!(
        "{{\"entry_ordinal\":{},",
        u64::try_from(ordinal).unwrap_or(u64::MAX)
    );
    let mut record = Vec::with_capacity(record_len);
    record.extend_from_slice(prefix.as_bytes());
    record.extend_from_slice(&body[1..]);
    Ok(Some(record))
}

fn canonical_json_bytes_with_limit<T: Serialize>(
    field: &str,
    value: &T,
    limit: usize,
) -> Result<Option<Vec<u8>>> {
    let mut writer = BoundedJsonWriter::new(limit);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(Some(writer.bytes)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(OmniError::manifest_internal(format!(
            "failed to encode stream {field}: {error}"
        ))),
    }
}

struct CanonicalRecordSetDigests {
    expected_count: usize,
    next_ordinal: usize,
    violation: CanonicalHasher,
    correction_view: CanonicalHasher,
}

impl CanonicalRecordSetDigests {
    fn new(expected_count: usize) -> Self {
        let mut violation = CanonicalHasher::new(DATA_BLOCK_VIOLATION_DOMAIN);
        let mut correction_view = CanonicalHasher::new(DATA_BLOCK_CORRECTION_VIEW_DOMAIN);
        let count = u64::try_from(expected_count).unwrap_or(u64::MAX);
        violation.u64(count);
        correction_view.u64(count);
        Self {
            expected_count,
            next_ordinal: 0,
            violation,
            correction_view,
        }
    }

    fn push(&mut self, ordinal: usize, record: &[u8]) -> Result<()> {
        if ordinal != self.next_ordinal || ordinal >= self.expected_count {
            return Err(OmniError::manifest_internal(
                "stream DataBlock canonical record order is non-contiguous",
            ));
        }
        let ordinal = u64::try_from(ordinal).unwrap_or(u64::MAX);
        self.violation.u64(ordinal);
        self.violation.field(record);
        self.correction_view.u64(ordinal);
        self.correction_view.field(record);
        self.next_ordinal += 1;
        Ok(())
    }

    fn finish(self) -> Result<(String, String)> {
        if self.next_ordinal != self.expected_count {
            return Err(OmniError::manifest_internal(
                "stream DataBlock canonical record set is incomplete",
            ));
        }
        Ok((self.violation.finish(), self.correction_view.finish()))
    }
}

fn validation_violation_code(kind: MergeConflictKind) -> &'static str {
    match kind {
        MergeConflictKind::DivergentInsert => "DIVERGENT_INSERT",
        MergeConflictKind::DivergentUpdate => "DIVERGENT_UPDATE",
        MergeConflictKind::DeleteVsUpdate => "DELETE_VS_UPDATE",
        MergeConflictKind::OrphanEdge => "ORPHAN_EDGE",
        MergeConflictKind::UniqueViolation => "UNIQUE_VIOLATION",
        MergeConflictKind::CardinalityViolation => "CARDINALITY_VIOLATION",
        MergeConflictKind::ValueConstraintViolation => "VALUE_CONSTRAINT_VIOLATION",
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimOperationRequest {
    pub(crate) graph_identity_digest: String,
    pub(crate) claim_id: String,
    /// Exact durable lifecycle operation which owns this claim. Ordinary OPEN
    /// cold claims carry `None`; DRAINING claims carry the active drain ID.
    pub(crate) lifecycle_operation_id: Option<String>,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) shard_id: String,
    pub(crate) initial_shard_manifest_version: u64,
    pub(crate) initial_writer_epoch: u64,
    pub(crate) initial_replay_cursor: u64,
    pub(crate) initial_current_generation: u64,
    pub(crate) initial_base_merged_generation: u64,
    pub(crate) claim_contract_version: u32,
}

/// Exact invariant authority fixed before the first physical claim attempt.
#[derive(Debug, Clone)]
pub(crate) struct PreparedClaimOperation {
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) claim_id: String,
    pub(crate) lifecycle_operation_id: Option<String>,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) stream_configuration_digest: String,
    pub(crate) physical_binding_digest: String,
    pub(crate) lifecycle_revision: u64,
    pub(crate) initial_shard_manifest_version: u64,
    pub(crate) initial_writer_epoch: u64,
    pub(crate) initial_replay_cursor: u64,
    pub(crate) initial_current_generation: u64,
    pub(crate) initial_base_merged_generation: u64,
    pub(crate) claim_contract_version: u32,
    /// End of the latest manifest-selected successfully published fold, or
    /// zero when no folded WAL prefix lies beyond the selected claim tail.
    pub(crate) folded_replay_cursor: u64,
    pub(crate) prior_authenticated_tail: AuthenticatedWalTail,
    pub(crate) bound_prestate_digest: String,
    pub(crate) claim_operation_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimAttemptRequest {
    pub(crate) attempt_id: String,
    pub(crate) pre_shard_manifest_version: u64,
    pub(crate) pre_writer_epoch: u64,
    pub(crate) pre_replay_cursor: u64,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_writer_epoch: u64,
    pub(crate) storage_envelope_digest: Option<String>,
}

/// One sidecar-owned invocation plan. The sidecar may retain at most this plan
/// plus the already committed attempt-chain head/count.
#[derive(Debug, Clone)]
pub(crate) struct PreparedClaimAttempt {
    pub(crate) operation: PreparedClaimOperation,
    pub(crate) attempt_id: String,
    pub(crate) pre_shard_manifest_version: u64,
    pub(crate) pre_writer_epoch: u64,
    pub(crate) pre_replay_cursor: u64,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_writer_epoch: u64,
    pub(crate) planned_sentinel_digest: String,
    pub(crate) storage_envelope_digest: Option<String>,
    pub(crate) bound_prestate_digest: String,
    pub(crate) attempt_plan_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimAttemptEvidence {
    NoEffect,
    AbortedNoEffect,
    StockManifestOnly {
        achieved_shard_manifest_version: u64,
        achieved_writer_epoch: u64,
    },
    StockManifestPlusSentinel {
        achieved_shard_manifest_version: u64,
        achieved_writer_epoch: u64,
    },
    PatchedSentinelOnly,
    PatchedSentinelPlusNamingManifest {
        achieved_shard_manifest_version: u64,
        achieved_writer_epoch: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimWalAuthenticationPlan {
    pub(crate) identity: TableIdentity,
    pub(crate) accepted_schema_hash: String,
    pub(crate) expected_table_schema: SchemaRef,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) stream_configuration_digest: String,
    pub(crate) physical_binding_digest: String,
    pub(crate) prior_tail: AuthenticatedWalTail,
    /// Exact manifest-selected published-fold boundary. Entries at or below
    /// it may already be represented by current base/token authority; later
    /// entries remain the active or unmerged suffix.
    pub(crate) folded_replay_cursor: u64,
    pub(crate) prior_writer_epoch: u64,
    pub(crate) achieved_writer_epoch: u64,
    /// Exact manifest-selected current-token/base authority for every key in
    /// the bounded WAL delta. It is the terminal anchor for that key's
    /// published-fold prefix and the predecessor/fold-base anchor for its
    /// active suffix. The caller obtains this bounded map with the existing
    /// exact-key token/base probes after discovering the delta's keys; missing
    /// and present are distinct authority states.
    pub(crate) prior_token_by_key: BTreeMap<String, Option<StreamToken>>,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_sentinel_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimWalKeyDiscoveryPlan {
    pub(crate) expected_table_schema: SchemaRef,
    pub(crate) shard_id: String,
    pub(crate) prior_position: u64,
    pub(crate) sentinel_position: u64,
    pub(crate) prior_writer_epoch: u64,
    pub(crate) achieved_writer_epoch: u64,
}

/// Fully authenticated bounded delta `(prior_tail.position, position]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthenticatedClaimWalSegment {
    pub(crate) identity: TableIdentity,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) prior_writer_epoch: u64,
    pub(crate) achieved_writer_epoch: u64,
    pub(crate) prior_position: u64,
    pub(crate) position: u64,
    /// Manifest-selected published-fold boundary authenticated inside this
    /// segment, or zero when the segment contains no published prefix beyond
    /// its prior tail.
    pub(crate) published_prefix_position: u64,
    pub(crate) entry_count: u64,
    pub(crate) row_count: u64,
    pub(crate) arrow_bytes: u64,
    pub(crate) sentinel_digest: String,
    pub(crate) segment_digest: String,
    pub(crate) empty_fence_state_digest: String,
    /// LWW projection of this exact authenticated suffix only. A later
    /// fence-only re-claim may have an empty suffix while the bounded active
    /// generation still replays rows authenticated by the preceding claim, so
    /// this is not by itself the hot full-generation projection authority.
    pub(crate) suffix_lww_projection_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BuiltTerminalClaim {
    pub(crate) receipt: ClaimReceipt,
    pub(crate) next_claim_chain: ReceiptChainRef,
    pub(crate) next_authenticated_tail: AuthenticatedWalTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EmptyCutEvidence {
    pub(crate) shard_manifest_version: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) replay_cursor: u64,
    pub(crate) current_generation: u64,
    pub(crate) base_merged_generation: u64,
}

fn build_quiesce_request_payload(
    prior: &StreamLifecycleEntry,
    request: &QuiesceRequest,
) -> Result<QuiesceRequestPayload> {
    prior.validate()?;
    if prior.lifecycle != StreamLifecycle::Open
        || request.expected_lifecycle_revision != prior.lifecycle_revision
    {
        return Err(OmniError::manifest_internal(
            "stream quiesce request must bind the exact current OPEN revision",
        ));
    }
    validate_digest(
        "quiesce graph_identity_digest",
        &request.graph_identity_digest,
    )?;
    validate_uuid_v4("quiesce drain_id", &request.drain_id)?;
    validate_canonical_text("quiesce initiating_actor", &request.initiating_actor)?;
    if request.initiated_at <= 0 {
        return Err(OmniError::manifest_internal(
            "stream quiesce initiated_at must be positive",
        ));
    }
    validate_target_epoch_advance(prior, &request.target_epoch_floor_by_shard)?;
    if request.seal_override.is_some() {
        return Err(OmniError::manifest_internal(
            "a fresh OPEN quiesce cannot carry a disable-drain adoption",
        ));
    }
    let physical_binding_digest = stream_physical_binding_digest(&prior.binding)?;
    Ok(QuiesceRequestPayload {
        protocol_version: QUIESCE_REQUEST_PROTOCOL_VERSION,
        graph_identity_digest: request.graph_identity_digest.clone(),
        identity: prior.identity,
        stream_incarnation_id: prior.enrollment_receipt.stream_incarnation_id.clone(),
        binding_scope_id: prior.binding_scope_id.clone(),
        enrollment_id: prior.binding.enrollment_id.clone(),
        drain_id: request.drain_id.clone(),
        expected_lifecycle_revision: request.expected_lifecycle_revision,
        goal: request.goal,
        physical_binding_digest,
        expected_current_head_witness: prior.current_head_witness.clone(),
        target_epoch_floor_by_shard: request.target_epoch_floor_by_shard.clone(),
        seal_override: request.seal_override.clone(),
    })
}

pub(crate) fn stream_quiesce_request_payload(
    prior: &StreamLifecycleEntry,
    request: &QuiesceRequest,
) -> Result<serde_json::Value> {
    build_quiesce_request_payload(prior, request)?.to_value()
}

pub(crate) fn stream_quiesce_request_digest(
    prior: &StreamLifecycleEntry,
    request: &QuiesceRequest,
) -> Result<String> {
    let payload = stream_quiesce_request_payload(prior, request)?;
    ManagementReceipt::request_digest_for(&payload)
}

/// Reconstruct the canonical quiesce request retained by a DRAINING row.
///
/// The descriptor is the restart plan. Recomputing and comparing its digest
/// before terminal receipt creation prevents a restart from silently
/// retargeting the operation after any lifecycle movement.
pub(crate) fn stream_quiesce_request_payload_from_draining(
    lifecycle: &StreamLifecycleEntry,
    graph_identity_digest: &str,
) -> Result<serde_json::Value> {
    lifecycle.validate()?;
    validate_digest("quiesce graph_identity_digest", graph_identity_digest)?;
    let drain = lifecycle.drain.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("quiesce request reconstruction requires DRAINING authority")
    })?;
    if lifecycle.lifecycle != StreamLifecycle::Draining {
        return Err(OmniError::manifest_internal(
            "quiesce request reconstruction requires DRAINING lifecycle",
        ));
    }
    if drain.operation_request_payload.graph_identity_digest != graph_identity_digest {
        return Err(OmniError::manifest_internal(
            "retained drain request belongs to another graph identity",
        ));
    }
    let payload = drain.operation_request_payload.to_value()?;
    if ManagementReceipt::request_digest_for(&payload)? != drain.operation_request_digest {
        return Err(OmniError::manifest_internal(
            "retained drain descriptor no longer reconstructs its canonical request digest",
        ));
    }
    Ok(payload)
}

pub(crate) fn build_open_to_draining(
    prior: &StreamLifecycleEntry,
    request: QuiesceRequest,
) -> Result<StartedDrain> {
    let operation_request_payload = build_quiesce_request_payload(prior, &request)?;
    let request_payload = operation_request_payload.to_value()?;
    let request_digest = ManagementReceipt::request_digest_for(&request_payload)?;
    let next_revision = next_revision(prior.lifecycle_revision)?;
    let drain = DrainDescriptor {
        drain_id: request.drain_id,
        operation_expected_revision: request.expected_lifecycle_revision,
        operation_request_digest: request_digest.clone(),
        goal: request.goal,
        initiating_actor: request.initiating_actor,
        initiated_at: request.initiated_at,
        expected_binding: prior.binding.clone(),
        expected_current_head_witness: prior.current_head_witness.clone(),
        operation_request_payload,
        target_epoch_floor_by_shard: request.target_epoch_floor_by_shard,
        guarded_operation: None,
        seal_override: request.seal_override,
    };
    let mut next = prior.clone();
    next.lifecycle = StreamLifecycle::Draining;
    next.lifecycle_revision = next_revision;
    next.drain = Some(drain);
    next.strict_block = None;
    next.sealed_proof = None;
    next.validate_successor_of(prior)?;
    Ok(StartedDrain {
        lifecycle: next,
        request_payload,
        request_digest,
    })
}

/// Fix the caller-owned compare token and the minimum same-scope epoch which
/// an `OPEN`-producing resume must achieve before recovery may publish it.
pub(crate) fn prepare_stream_resume_open(
    prior: &StreamLifecycleEntry,
    request: StreamResumeRequest,
) -> Result<PreparedStreamResumeOpen> {
    validate_resume_mode_eligibility(prior, request.mode)?;
    validate_digest(
        "resume graph_identity_digest",
        &request.graph_identity_digest,
    )?;
    validate_uuid_v4("resume_id", &request.resume_id)?;
    validate_canonical_text("resume actor_id", &request.actor_id)?;
    if request.expected_lifecycle_revision != prior.lifecycle_revision {
        return Err(OmniError::manifest_internal(
            "stream resume request must bind the exact current lifecycle revision",
        ));
    }
    if request.initiated_at <= 0 {
        return Err(OmniError::manifest_internal(
            "stream resume initiated_at must be positive",
        ));
    }
    if !request.public_named_branches.is_empty() {
        return Err(OmniError::manifest_internal(
            "stream resume requires the exact empty public named-branch topology",
        ));
    }
    let shard_id = prior.binding.shard_ids.as_slice().first().ok_or_else(|| {
        OmniError::manifest_internal("stream resume requires one exact bound shard")
    })?;
    if prior.binding.shard_ids.len() != 1 {
        return Err(OmniError::manifest_internal(
            "stream resume requires the exact unsharded binding",
        ));
    }
    let minimum_next_epoch_floor = prior
        .epoch_floor_by_shard
        .get(shard_id)
        .copied()
        .ok_or_else(|| {
            OmniError::manifest_internal("stream resume has no authoritative shard epoch floor")
        })?
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream resume epoch floor overflow"))?;
    let next_lifecycle_revision = next_revision(prior.lifecycle_revision)?;
    let payload = StreamResumeRequestPayload {
        protocol_version: STREAM_RESUME_REQUEST_PROTOCOL_VERSION,
        graph_identity_digest: request.graph_identity_digest,
        identity: prior.identity,
        stream_incarnation_id: prior.enrollment_receipt.stream_incarnation_id.clone(),
        binding_scope_id: prior.binding_scope_id.clone(),
        enrollment_id: prior.binding.enrollment_id.clone(),
        resume_id: request.resume_id,
        expected_lifecycle_revision: request.expected_lifecycle_revision,
        mode: request.mode,
        actor_id: request.actor_id,
        public_named_branches: request.public_named_branches,
    };
    payload.validate_for_lifecycle(prior, request.mode)?;
    let request_digest = payload.request_digest()?;
    Ok(PreparedStreamResumeOpen {
        mode: request.mode,
        request_payload: payload.to_value()?,
        request_digest,
        next_lifecycle_revision,
        minimum_next_epoch_floor,
        recorded_at: request.initiated_at,
    })
}

fn validate_resume_mode_eligibility(
    prior: &StreamLifecycleEntry,
    mode: StreamResumeMode,
) -> Result<()> {
    prior.validate()?;
    match mode {
        StreamResumeMode::ResumeSealed if prior.lifecycle == StreamLifecycle::Sealed => Ok(()),
        StreamResumeMode::ResumeSealed => Err(OmniError::manifest_internal(
            "plain stream resume requires exact SEALED lifecycle authority",
        )),
        StreamResumeMode::AbortDrain if prior.lifecycle == StreamLifecycle::Draining => {
            let drain = prior.drain.as_ref().ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream abort-drain requires the exact active drain descriptor",
                )
            })?;
            if drain.guarded_operation.is_some() {
                return Err(OmniError::manifest_internal(
                    "stream abort-drain cannot reopen after a guarded operation began",
                ));
            }
            if prior.strict_block.is_some()
                || prior
                    .last_fold_summary
                    .as_ref()
                    .is_some_and(|summary| summary.outcome == LastFoldOutcome::StrictBlocked)
            {
                return Err(OmniError::manifest_internal(
                    "stream abort-drain cannot reopen around a strict-blocked cut",
                ));
            }
            validate_abort_drain_has_no_selected_unmerged_rows(prior)
        }
        StreamResumeMode::AbortDrain => Err(OmniError::manifest_internal(
            "stream abort-drain requires exact DRAINING lifecycle authority",
        )),
    }
}

/// Prove that an immutable management receipt selected by the current token
/// witness is consistent with the lane's monotonic revision/chain authority.
/// Later folds may advance only the lifecycle revision, and later management
/// operations may extend the chain, so delayed exact retries must not require
/// the receipt to remain the current head.
pub(super) fn validate_selected_management_receipt_progress(
    current: &StreamLifecycleEntry,
    receipt: &ManagementReceipt,
    terminal_lifecycle: StreamLifecycle,
) -> Result<()> {
    let receipt_chain = receipt.next_chain_ref()?;
    if current.lifecycle_revision < receipt.to_revision
        || current.management_receipt_chain.record_count < receipt.chain_ordinal
        || (current.management_receipt_chain.record_count == receipt.chain_ordinal
            && current.management_receipt_chain != receipt_chain)
        || (current.lifecycle_revision == receipt.to_revision
            && (current.lifecycle != terminal_lifecycle
                || current.management_receipt_chain != receipt_chain))
    {
        return Err(OmniError::manifest_internal(format!(
            "selected {} management receipt '{}' is not consistent with the current stream lifecycle chain",
            receipt.operation_kind, receipt.operation_id
        )));
    }
    Ok(())
}

/// Lifecycle authority can prove a freshly armed drain empty from the genesis
/// tail. Once a claim has been selected, its complete current-generation LWW
/// projection must instead be the exact authority-scoped empty projection.
/// Physical replay/generation checks remain the orchestration owner's job
/// under exclusive admission; this guard prevents the pure planner from
/// deliberately reopening a row which already commits to unmerged winners.
fn validate_abort_drain_has_no_selected_unmerged_rows(prior: &StreamLifecycleEntry) -> Result<()> {
    if prior.authenticated_wal_tail.segment_count == 0 {
        return Ok(());
    }
    let [shard_id] = prior.binding.shard_ids.as_slice() else {
        return Err(OmniError::manifest_internal(
            "stream abort-drain requires the exact unsharded binding",
        ));
    };
    let physical_binding_digest = stream_physical_binding_digest(&prior.binding)?;
    let empty_projection_digest = lww_projection_digest_for_authority(
        prior.identity,
        &prior.binding_scope_id,
        &prior.binding.enrollment_id,
        shard_id,
        &prior.enrollment_receipt.stream_incarnation_id,
        &prior.binding.stream_config_hash,
        &physical_binding_digest,
        &BTreeMap::new(),
    )?;
    if prior.authenticated_wal_tail.lww_projection_digest != empty_projection_digest {
        return Err(OmniError::manifest_internal(
            "stream abort-drain cannot reopen around an authenticated unmerged WAL projection",
        ));
    }
    Ok(())
}

pub(crate) fn prepare_claim_operation(
    lifecycle: &StreamLifecycleEntry,
    request: ClaimOperationRequest,
) -> Result<PreparedClaimOperation> {
    lifecycle.validate()?;
    if lifecycle.lifecycle == StreamLifecycle::Sealed {
        return Err(OmniError::manifest_internal(
            "a SEALED stream cannot begin an ordinary writer claim",
        ));
    }
    match (
        lifecycle.lifecycle,
        lifecycle.drain.as_ref(),
        request.lifecycle_operation_id.as_deref(),
    ) {
        (StreamLifecycle::Open, None, None) => {}
        (StreamLifecycle::Draining, Some(drain), Some(operation_id))
            if operation_id == drain.drain_id => {}
        (StreamLifecycle::Open, _, Some(_)) => {
            return Err(OmniError::manifest_internal(
                "an OPEN stream claim cannot bind a lifecycle operation",
            ));
        }
        (StreamLifecycle::Open, Some(_), None) => {
            return Err(OmniError::manifest_internal(
                "an OPEN stream claim observed an invalid retained drain",
            ));
        }
        (StreamLifecycle::Draining, _, _) => {
            return Err(OmniError::manifest_internal(
                "a DRAINING stream claim must bind the exact active drain ID",
            ));
        }
        (StreamLifecycle::Sealed, _, _) => unreachable!("SEALED rejected above"),
    }
    prepare_claim_operation_for_eligible_lifecycle(lifecycle, request)
}

/// Prepare the physical claim owned by a recovery-v15 resume/abort operation.
/// This is intentionally separate from `prepare_claim_operation`: ordinary
/// cold claims continue to reject SEALED authority and cannot opt into this
/// path with a caller-chosen lifecycle operation ID.
pub(crate) fn prepare_resume_claim_operation(
    prior: &StreamLifecycleEntry,
    request: ClaimOperationRequest,
    mode: StreamResumeMode,
    minimum_next_epoch_floor: u64,
) -> Result<PreparedClaimOperation> {
    validate_resume_mode_eligibility(prior, mode)?;
    if request.claim_kind != STREAM_RESUME_OPERATION_KIND {
        return Err(OmniError::manifest_internal(
            "a stream resume claim must use the canonical RESUME claim kind",
        ));
    }
    let resume_id = request.lifecycle_operation_id.as_deref().ok_or_else(|| {
        OmniError::manifest_internal(
            "a stream resume claim must bind the exact resume occurrence ID",
        )
    })?;
    validate_uuid_v4("resume claim lifecycle_operation_id", resume_id)?;
    let [shard_id] = prior.binding.shard_ids.as_slice() else {
        return Err(OmniError::manifest_internal(
            "stream resume claim requires the exact unsharded binding",
        ));
    };
    let authoritative_epoch = prior
        .epoch_floor_by_shard
        .get(shard_id)
        .copied()
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "stream resume claim has no authoritative shard epoch floor",
            )
        })?;
    let expected_minimum = authoritative_epoch
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream resume epoch floor overflow"))?;
    if minimum_next_epoch_floor != expected_minimum {
        return Err(OmniError::manifest_internal(
            "stream resume minimum next epoch differs from the exact lifecycle successor",
        ));
    }
    prepare_claim_operation_for_eligible_lifecycle(prior, request)
}

fn prepare_claim_operation_for_eligible_lifecycle(
    lifecycle: &StreamLifecycleEntry,
    request: ClaimOperationRequest,
) -> Result<PreparedClaimOperation> {
    validate_digest(
        "claim graph_identity_digest",
        &request.graph_identity_digest,
    )?;
    validate_uuid_v4("claim_id", &request.claim_id)?;
    if let Some(operation_id) = request.lifecycle_operation_id.as_deref() {
        validate_uuid_v4("claim lifecycle_operation_id", operation_id)?;
    }
    validate_canonical_text(
        "claim recovery_operation_id",
        &request.recovery_operation_id,
    )?;
    validate_protocol_label("claim_kind", &request.claim_kind)?;
    validate_uuid("claim shard_id", &request.shard_id)?;
    if lifecycle.binding.shard_ids.as_slice() != [request.shard_id.as_str()] {
        return Err(OmniError::manifest_internal(
            "stream claim shard differs from the exact unsharded binding",
        ));
    }
    let authoritative_epoch = lifecycle
        .epoch_floor_by_shard
        .get(&request.shard_id)
        .copied()
        .ok_or_else(|| {
            OmniError::manifest_internal("stream claim has no authoritative shard epoch floor")
        })?;
    if request.initial_shard_manifest_version == 0
        || request.initial_writer_epoch == 0
        || request.initial_writer_epoch != authoritative_epoch
        || request.initial_current_generation < request.initial_base_merged_generation
        || request.claim_contract_version == 0
    {
        return Err(OmniError::manifest_internal(
            "stream claim initial shard authority is invalid or differs from lifecycle authority",
        ));
    }
    let folded_replay_cursor = lifecycle
        .last_fold_summary
        .as_ref()
        .filter(|summary| summary.outcome == LastFoldOutcome::Published)
        .map(|summary| summary.exact_generation_cut.replay_after_wal_entry_position)
        .filter(|cursor| *cursor > lifecycle.authenticated_wal_tail.position)
        .unwrap_or(0);
    if folded_replay_cursor > request.initial_replay_cursor {
        return Err(OmniError::manifest_internal(
            "stream claim published-fold cursor exceeds physical replay authority",
        ));
    }
    lifecycle.authenticated_wal_tail.validate()?;
    let physical_binding_digest = stream_physical_binding_digest(&lifecycle.binding)?;
    let lifecycle_bytes = canonical_json_bytes("claim lifecycle prestate", lifecycle)?;
    let mut prestate = CanonicalHasher::new(CLAIM_PRESTATE_DOMAIN);
    prestate.field(&lifecycle_bytes);
    prestate.field(request.shard_id.as_bytes());
    prestate.u64(request.initial_shard_manifest_version);
    prestate.u64(request.initial_writer_epoch);
    prestate.u64(request.initial_replay_cursor);
    prestate.u64(request.initial_current_generation);
    prestate.u64(request.initial_base_merged_generation);
    let bound_prestate_digest = prestate.finish();

    let mut operation_digest = CanonicalHasher::new(CLAIM_OPERATION_DOMAIN);
    operation_digest.u32(CLAIM_OPERATION_PROTOCOL_VERSION);
    operation_digest.field(request.graph_identity_digest.as_bytes());
    operation_digest.u64(lifecycle.identity.stable_table_id);
    operation_digest.u64(lifecycle.identity.table_incarnation_id);
    operation_digest.field(request.claim_id.as_bytes());
    match request.lifecycle_operation_id.as_deref() {
        Some(operation_id) => {
            operation_digest.byte(1);
            operation_digest.field(operation_id.as_bytes());
        }
        None => operation_digest.byte(0),
    }
    operation_digest.field(request.recovery_operation_id.as_bytes());
    operation_digest.field(request.claim_kind.as_bytes());
    operation_digest.byte(claim_profile_tag(request.profile));
    operation_digest.field(
        lifecycle
            .enrollment_receipt
            .stream_incarnation_id
            .as_bytes(),
    );
    operation_digest.field(lifecycle.binding_scope_id.as_bytes());
    operation_digest.field(lifecycle.binding.enrollment_id.as_bytes());
    operation_digest.field(request.shard_id.as_bytes());
    operation_digest.field(lifecycle.binding.stream_config_hash.as_bytes());
    operation_digest.field(physical_binding_digest.as_bytes());
    operation_digest.field(bound_prestate_digest.as_bytes());
    operation_digest.u32(request.claim_contract_version);
    let claim_operation_digest = operation_digest.finish();

    Ok(PreparedClaimOperation {
        graph_identity_digest: request.graph_identity_digest,
        identity: lifecycle.identity,
        claim_id: request.claim_id,
        lifecycle_operation_id: request.lifecycle_operation_id,
        recovery_operation_id: request.recovery_operation_id,
        claim_kind: request.claim_kind,
        profile: request.profile,
        stream_incarnation_id: lifecycle.enrollment_receipt.stream_incarnation_id.clone(),
        binding_scope_id: lifecycle.binding_scope_id.clone(),
        enrollment_id: lifecycle.binding.enrollment_id.clone(),
        shard_id: request.shard_id,
        stream_configuration_digest: lifecycle.binding.stream_config_hash.clone(),
        physical_binding_digest,
        lifecycle_revision: lifecycle.lifecycle_revision,
        initial_shard_manifest_version: request.initial_shard_manifest_version,
        initial_writer_epoch: request.initial_writer_epoch,
        initial_replay_cursor: request.initial_replay_cursor,
        initial_current_generation: request.initial_current_generation,
        initial_base_merged_generation: request.initial_base_merged_generation,
        claim_contract_version: request.claim_contract_version,
        folded_replay_cursor,
        prior_authenticated_tail: lifecycle.authenticated_wal_tail.clone(),
        bound_prestate_digest,
        claim_operation_digest,
    })
}

pub(crate) fn prepare_claim_attempt(
    operation: &PreparedClaimOperation,
    request: ClaimAttemptRequest,
) -> Result<PreparedClaimAttempt> {
    validate_uuid_v4("claim attempt_id", &request.attempt_id)?;
    validate_storage_envelope(
        operation.profile,
        request.storage_envelope_digest.as_deref(),
    )?;
    if request.pre_shard_manifest_version < operation.initial_shard_manifest_version
        || request.pre_writer_epoch < operation.initial_writer_epoch
        || request.pre_replay_cursor < operation.initial_replay_cursor
        || request.planned_sentinel_position <= operation.prior_authenticated_tail.position
        || request.planned_sentinel_position <= request.pre_replay_cursor
        || request.planned_writer_epoch <= request.pre_writer_epoch
    {
        return Err(OmniError::manifest_internal(
            "stream claim attempt prestate or planned successor is invalid",
        ));
    }
    let planned_sentinel_digest = stream_planned_sentinel_digest(
        &operation.binding_scope_id,
        &operation.enrollment_id,
        &operation.shard_id,
        &operation.stream_incarnation_id,
        request.planned_sentinel_position,
        request.planned_writer_epoch,
    )?;
    let mut prestate = CanonicalHasher::new(CLAIM_ATTEMPT_PRESTATE_DOMAIN);
    prestate.field(operation.bound_prestate_digest.as_bytes());
    prestate.u64(request.pre_shard_manifest_version);
    prestate.u64(request.pre_writer_epoch);
    prestate.u64(request.pre_replay_cursor);
    prestate.u64(operation.prior_authenticated_tail.position);
    prestate.u64(operation.prior_authenticated_tail.segment_count);
    prestate.field(operation.prior_authenticated_tail.chain_digest.as_bytes());
    prestate.field(
        operation
            .prior_authenticated_tail
            .lww_projection_digest
            .as_bytes(),
    );
    let bound_prestate_digest = prestate.finish();

    let mut plan_digest = CanonicalHasher::new(CLAIM_ATTEMPT_PLAN_DOMAIN);
    plan_digest.u32(CLAIM_ATTEMPT_PLAN_PROTOCOL_VERSION);
    plan_digest.field(operation.claim_operation_digest.as_bytes());
    plan_digest.field(request.attempt_id.as_bytes());
    plan_digest.field(bound_prestate_digest.as_bytes());
    plan_digest.optional_digest(request.storage_envelope_digest.as_deref());
    plan_digest.u64(request.planned_sentinel_position);
    plan_digest.u64(request.planned_writer_epoch);
    plan_digest.field(planned_sentinel_digest.as_bytes());
    // Bind the profile-specific set of permitted physical intermediates.
    plan_digest.field(match operation.profile {
        ClaimProfile::RetainAll => b"NO_EFFECT|ABORTED_NO_EFFECT|STOCK_MANIFEST_ONLY|STOCK_MANIFEST_PLUS_SENTINEL",
        ClaimProfile::ManagedReclamation => b"NO_EFFECT|ABORTED_NO_EFFECT|PATCHED_SENTINEL_ONLY|PATCHED_SENTINEL_PLUS_NAMING_MANIFEST",
    });
    let attempt_plan_digest = plan_digest.finish();

    Ok(PreparedClaimAttempt {
        operation: operation.clone(),
        attempt_id: request.attempt_id,
        pre_shard_manifest_version: request.pre_shard_manifest_version,
        pre_writer_epoch: request.pre_writer_epoch,
        pre_replay_cursor: request.pre_replay_cursor,
        planned_sentinel_position: request.planned_sentinel_position,
        planned_writer_epoch: request.planned_writer_epoch,
        planned_sentinel_digest,
        storage_envelope_digest: request.storage_envelope_digest,
        bound_prestate_digest,
        attempt_plan_digest,
    })
}

pub(crate) fn build_claim_attempt_effect(
    prior_attempt_chain: &ReceiptChainRef,
    attempt: &PreparedClaimAttempt,
    evidence: ClaimAttemptEvidence,
) -> Result<ClaimAttemptEffect> {
    let (
        classification,
        achieved_shard_manifest_version,
        achieved_writer_epoch,
        observed_sentinel_position,
        observed_sentinel_digest,
    ) = match evidence {
        ClaimAttemptEvidence::NoEffect => {
            (ClaimAttemptClassification::NoEffect, None, None, None, None)
        }
        ClaimAttemptEvidence::AbortedNoEffect => (
            ClaimAttemptClassification::AbortedNoEffect,
            None,
            None,
            None,
            None,
        ),
        ClaimAttemptEvidence::StockManifestOnly {
            achieved_shard_manifest_version,
            achieved_writer_epoch,
        } => (
            ClaimAttemptClassification::StockManifestOnly,
            Some(achieved_shard_manifest_version),
            Some(achieved_writer_epoch),
            None,
            None,
        ),
        ClaimAttemptEvidence::StockManifestPlusSentinel {
            achieved_shard_manifest_version,
            achieved_writer_epoch,
        } => (
            ClaimAttemptClassification::StockManifestPlusSentinel,
            Some(achieved_shard_manifest_version),
            Some(achieved_writer_epoch),
            Some(attempt.planned_sentinel_position),
            Some(attempt.planned_sentinel_digest.clone()),
        ),
        ClaimAttemptEvidence::PatchedSentinelOnly => (
            ClaimAttemptClassification::PatchedSentinelOnly,
            None,
            None,
            Some(attempt.planned_sentinel_position),
            Some(attempt.planned_sentinel_digest.clone()),
        ),
        ClaimAttemptEvidence::PatchedSentinelPlusNamingManifest {
            achieved_shard_manifest_version,
            achieved_writer_epoch,
        } => (
            ClaimAttemptClassification::PatchedSentinelPlusNamingManifest,
            Some(achieved_shard_manifest_version),
            Some(achieved_writer_epoch),
            Some(attempt.planned_sentinel_position),
            Some(attempt.planned_sentinel_digest.clone()),
        ),
    };
    validate_attempt_classification(attempt, classification)?;
    if let (Some(version), Some(epoch)) = (achieved_shard_manifest_version, achieved_writer_epoch)
        && (version <= attempt.pre_shard_manifest_version || epoch != attempt.planned_writer_epoch)
    {
        return Err(OmniError::manifest_internal(
            "stream claim attempt achieved authority differs from its exact plan",
        ));
    }

    let mut terminal = CanonicalHasher::new(CLAIM_ATTEMPT_EFFECT_DOMAIN);
    terminal.field(attempt.attempt_plan_digest.as_bytes());
    terminal.byte(claim_attempt_classification_tag(classification));
    terminal.u64(achieved_shard_manifest_version.unwrap_or(0));
    terminal.u64(achieved_writer_epoch.unwrap_or(0));
    terminal.u64(observed_sentinel_position.unwrap_or(0));
    terminal.optional_digest(observed_sentinel_digest.as_deref());
    let attempt_terminal_effect_digest = terminal.finish();

    let effect = ClaimAttemptEffect::new(
        prior_attempt_chain,
        ClaimAttemptEffectPreimage {
            graph_identity_digest: attempt.operation.graph_identity_digest.clone(),
            identity: attempt.operation.identity,
            stream_incarnation_id: attempt.operation.stream_incarnation_id.clone(),
            binding_scope_id: attempt.operation.binding_scope_id.clone(),
            enrollment_id: attempt.operation.enrollment_id.clone(),
            shard_id: attempt.operation.shard_id.clone(),
            claim_id: attempt.operation.claim_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            attempt_plan_digest: attempt.attempt_plan_digest.clone(),
            bound_prestate_digest: attempt.bound_prestate_digest.clone(),
            storage_envelope_digest: attempt.storage_envelope_digest.clone(),
            planned_sentinel_position: attempt.planned_sentinel_position,
            planned_sentinel_digest: attempt.planned_sentinel_digest.clone(),
            achieved_shard_manifest_version,
            achieved_writer_epoch,
            observed_sentinel_position,
            observed_sentinel_digest,
            attempt_terminal_effect_digest,
            classification,
        },
    )?;
    effect.validate_for_profile(attempt.operation.profile)?;
    Ok(effect)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_planned_sentinel_digest(
    binding_scope_id: &str,
    enrollment_id: &str,
    shard_id: &str,
    stream_incarnation_id: &str,
    position: u64,
    writer_epoch: u64,
) -> Result<String> {
    validate_uuid("planned sentinel binding_scope_id", binding_scope_id)?;
    validate_uuid("planned sentinel enrollment_id", enrollment_id)?;
    validate_uuid("planned sentinel shard_id", shard_id)?;
    validate_uuid(
        "planned sentinel stream_incarnation_id",
        stream_incarnation_id,
    )?;
    if position == 0 || writer_epoch == 0 {
        return Err(OmniError::manifest_internal(
            "stream planned sentinel requires positive position and writer epoch",
        ));
    }
    let mut digest = CanonicalHasher::new(PLANNED_SENTINEL_DOMAIN);
    digest.field(binding_scope_id.as_bytes());
    digest.field(enrollment_id.as_bytes());
    digest.field(shard_id.as_bytes());
    digest.field(stream_incarnation_id.as_bytes());
    digest.u64(position);
    digest.u64(writer_epoch);
    Ok(digest.finish())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_empty_fence_state_digest(
    binding_scope_id: &str,
    enrollment_id: &str,
    shard_id: &str,
    stream_incarnation_id: &str,
    stream_configuration_digest: &str,
    physical_binding_digest: &str,
    position: u64,
    writer_epoch: u64,
    sentinel_digest: &str,
) -> Result<String> {
    let recomputed_sentinel = stream_planned_sentinel_digest(
        binding_scope_id,
        enrollment_id,
        shard_id,
        stream_incarnation_id,
        position,
        writer_epoch,
    )?;
    validate_digest(
        "empty fence stream_configuration_digest",
        stream_configuration_digest,
    )?;
    validate_digest(
        "empty fence physical_binding_digest",
        physical_binding_digest,
    )?;
    validate_digest("empty fence sentinel_digest", sentinel_digest)?;
    if sentinel_digest != recomputed_sentinel {
        return Err(OmniError::manifest_internal(
            "stream empty-fence sentinel digest differs from its exact decoded state",
        ));
    }
    let mut digest = CanonicalHasher::new(EMPTY_FENCE_STATE_DOMAIN);
    digest.field(binding_scope_id.as_bytes());
    digest.field(enrollment_id.as_bytes());
    digest.field(shard_id.as_bytes());
    digest.field(stream_incarnation_id.as_bytes());
    digest.field(stream_configuration_digest.as_bytes());
    digest.field(physical_binding_digest.as_bytes());
    digest.u64(position);
    digest.u64(writer_epoch);
    digest.field(sentinel_digest.as_bytes());
    Ok(digest.finish())
}

pub(crate) fn claim_wal_authentication_plan(
    attempt: &PreparedClaimAttempt,
    terminal_effect: &ClaimAttemptEffect,
    accepted_schema_hash: impl Into<String>,
    expected_table_schema: SchemaRef,
    prior_token_by_key: BTreeMap<String, Option<StreamToken>>,
) -> Result<ClaimWalAuthenticationPlan> {
    terminal_effect.validate_for_profile(attempt.operation.profile)?;
    validate_effect_matches_attempt(attempt, terminal_effect)?;
    if !matches!(
        terminal_effect.classification,
        ClaimAttemptClassification::StockManifestPlusSentinel
            | ClaimAttemptClassification::PatchedSentinelPlusNamingManifest
    ) {
        return Err(OmniError::manifest_internal(
            "only a terminal manifest-plus-sentinel claim can authenticate a WAL segment",
        ));
    }
    let achieved_writer_epoch = terminal_effect
        .achieved_writer_epoch
        .ok_or_else(|| OmniError::manifest_internal("terminal claim has no achieved epoch"))?;
    let accepted_schema_hash = accepted_schema_hash.into();
    validate_digest("claim accepted_schema_hash", &accepted_schema_hash)?;
    validate_trusted_stream_metadata_schema(expected_table_schema.as_ref())
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    if expected_table_schema.column_with_name(TOMBSTONE).is_some() {
        return Err(OmniError::manifest_internal(
            "base table schema passed to WAL authentication may not contain Lance's tombstone",
        ));
    }
    Ok(ClaimWalAuthenticationPlan {
        identity: attempt.operation.identity,
        accepted_schema_hash,
        expected_table_schema,
        binding_scope_id: attempt.operation.binding_scope_id.clone(),
        enrollment_id: attempt.operation.enrollment_id.clone(),
        shard_id: attempt.operation.shard_id.clone(),
        stream_incarnation_id: attempt.operation.stream_incarnation_id.clone(),
        stream_configuration_digest: attempt.operation.stream_configuration_digest.clone(),
        physical_binding_digest: attempt.operation.physical_binding_digest.clone(),
        prior_tail: attempt.operation.prior_authenticated_tail.clone(),
        folded_replay_cursor: attempt.operation.folded_replay_cursor,
        prior_writer_epoch: attempt.operation.initial_writer_epoch,
        achieved_writer_epoch,
        prior_token_by_key,
        planned_sentinel_position: attempt.planned_sentinel_position,
        planned_sentinel_digest: attempt.planned_sentinel_digest.clone(),
    })
}

pub(crate) fn claim_wal_key_discovery_plan(
    attempt: &PreparedClaimAttempt,
    terminal_effect: &ClaimAttemptEffect,
    expected_table_schema: SchemaRef,
) -> Result<ClaimWalKeyDiscoveryPlan> {
    terminal_effect.validate_for_profile(attempt.operation.profile)?;
    validate_effect_matches_attempt(attempt, terminal_effect)?;
    if !matches!(
        terminal_effect.classification,
        ClaimAttemptClassification::StockManifestPlusSentinel
            | ClaimAttemptClassification::PatchedSentinelPlusNamingManifest
    ) {
        return Err(OmniError::manifest_internal(
            "only a terminal manifest-plus-sentinel claim has a discoverable WAL suffix",
        ));
    }
    let achieved_writer_epoch = terminal_effect
        .achieved_writer_epoch
        .ok_or_else(|| OmniError::manifest_internal("terminal claim has no achieved epoch"))?;
    let plan = ClaimWalKeyDiscoveryPlan {
        expected_table_schema,
        shard_id: attempt.operation.shard_id.clone(),
        prior_position: attempt.operation.prior_authenticated_tail.position,
        sentinel_position: attempt.planned_sentinel_position,
        prior_writer_epoch: attempt.operation.initial_writer_epoch,
        achieved_writer_epoch,
    };
    validate_key_discovery_plan(&plan)?;
    Ok(plan)
}

/// Discover the bounded exact-key set needed for the manifest-selected
/// current-token/base probe. This deliberately performs a first bounded WAL
/// pass; terminal authentication performs the second pass and rejects any
/// object change, gap, payload tamper, or key-set disagreement.
pub(crate) async fn collect_claim_wal_segment_keys(
    tailer: &WalTailer,
    plan: &ClaimWalKeyDiscoveryPlan,
) -> Result<BTreeSet<String>> {
    validate_key_discovery_plan(plan)?;
    let entry_count = bounded_segment_entry_count(plan.prior_position, plan.sentinel_position)?;
    let mut collector = ClaimWalKeyCollector::new(plan, entry_count)?;
    let first = plan
        .prior_position
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?;
    for position in first..=plan.sentinel_position {
        let entry = tailer
            .read_entry(position)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream claim WAL key-discovery pass has a gap at position {position}"
                ))
            })?;
        collector.push(entry)?;
    }
    collector.finish()
}

/// Read and authenticate exactly `(prior_tail.position, planned_sentinel]`.
/// The range length is proven bounded before the first object-store read.
pub(crate) async fn authenticate_claim_wal_segment(
    tailer: &WalTailer,
    plan: &ClaimWalAuthenticationPlan,
) -> Result<AuthenticatedClaimWalSegment> {
    validate_claim_wal_plan(plan)?;
    let entry_count =
        bounded_segment_entry_count(plan.prior_tail.position, plan.planned_sentinel_position)?;

    let mut authenticator = ClaimWalSegmentAuthenticator::new(plan, entry_count)?;
    let first = plan
        .prior_tail
        .position
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?;
    for position in first..=plan.planned_sentinel_position {
        let entry = tailer
            .read_entry(position)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream authenticated WAL segment has a gap at position {position}"
                ))
            })?;
        authenticator.push(entry)?;
    }
    authenticator.finish()
}

/// Recompute the LWW projection of the complete bounded active generation
/// reconstructed by the newly claimed writer.
///
/// This is intentionally distinct from suffix authentication. After a crash
/// between a terminal claim and seal, a successor claim writes only a new
/// fence sentinel but Lance replays the prior epoch's data into the active
/// generation. The new receipt must keep committing those winners rather than
/// replacing the hot projection with the empty suffix projection.
pub(crate) fn current_generation_lww_projection_digest(
    operation: &PreparedClaimOperation,
    accepted_schema_hash: &str,
    expected_table_schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<String> {
    current_generation_lww_projection_digest_for_authority(
        operation.identity,
        &operation.binding_scope_id,
        &operation.enrollment_id,
        &operation.shard_id,
        &operation.stream_incarnation_id,
        &operation.stream_configuration_digest,
        &operation.physical_binding_digest,
        accepted_schema_hash,
        expected_table_schema,
        batches,
    )
}

/// Recompute a drain fold's full-generation projection from the selected
/// lifecycle authority. This is the same commitment used by terminal claims;
/// the separate entrypoint prevents a drain from trusting the receipt's stored
/// digest without rescanning the immutable generation.
pub(crate) fn lifecycle_generation_lww_projection_digest(
    lifecycle: &StreamLifecycleEntry,
    accepted_schema_hash: &str,
    expected_table_schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<String> {
    lifecycle.validate()?;
    let [shard_id] = lifecycle.binding.shard_ids.as_slice() else {
        return Err(OmniError::manifest_internal(
            "stream generation projection requires one exact bound shard",
        ));
    };
    let physical_binding_digest = stream_physical_binding_digest(&lifecycle.binding)?;
    current_generation_lww_projection_digest_for_authority(
        lifecycle.identity,
        &lifecycle.binding_scope_id,
        &lifecycle.binding.enrollment_id,
        shard_id,
        &lifecycle.enrollment_receipt.stream_incarnation_id,
        &lifecycle.binding.stream_config_hash,
        &physical_binding_digest,
        accepted_schema_hash,
        expected_table_schema,
        batches,
    )
}

#[allow(clippy::too_many_arguments)]
fn current_generation_lww_projection_digest_for_authority(
    identity: TableIdentity,
    binding_scope_id: &str,
    enrollment_id: &str,
    shard_id: &str,
    stream_incarnation_id: &str,
    stream_configuration_digest: &str,
    physical_binding_digest: &str,
    accepted_schema_hash: &str,
    expected_table_schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<String> {
    validate_digest(
        "current-generation accepted_schema_hash",
        accepted_schema_hash,
    )?;
    validate_trusted_stream_metadata_schema(expected_table_schema.as_ref())
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    if expected_table_schema.column_with_name(TOMBSTONE).is_some() {
        return Err(OmniError::manifest_internal(
            "base table schema passed to current-generation projection may not contain Lance's tombstone",
        ));
    }
    let expected_stored_schema = schema_with_tombstone(expected_table_schema.as_ref());
    let mut rows = 0_u64;
    let mut arrow_bytes = 0_u64;
    let mut winners = BTreeMap::new();
    for batch in batches {
        if batch.num_rows() == 0 || batch.schema().as_ref() != expected_stored_schema.as_ref() {
            return Err(OmniError::manifest_internal(
                "current-generation projection batch is empty or differs from the exact bound schema",
            ));
        }
        rows = rows
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                OmniError::manifest_internal("current-generation projection row count exceeds u64")
            })?)
            .ok_or_else(|| {
                OmniError::manifest_internal("current-generation projection row count overflow")
            })?;
        if rows > B1_MAX_GENERATION_ROWS {
            return Err(OmniError::resource_limit(
                "stream_claim_current_generation_rows",
                B1_MAX_GENERATION_ROWS,
                rows,
            ));
        }
        arrow_bytes = arrow_bytes
            .checked_add(
                b1_logical_batch_bytes(batch)
                    .map_err(|error| OmniError::Lance(error.to_string()))?,
            )
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "current-generation projection Arrow-byte sum overflow",
                )
            })?;
        if arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_claim_current_generation_arrow_bytes",
                B1_MAX_GENERATION_ARROW_BYTES,
                arrow_bytes,
            ));
        }

        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "current-generation projection has no exact Utf8 id column",
                )
            })?;
        let tombstones = batch
            .column_by_name(TOMBSTONE)
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "current-generation projection has no exact Boolean tombstone column",
                )
            })?;
        let metadata_array = batch
            .column_by_name(crate::db::STREAM_METADATA_COLUMN)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "current-generation projection omits trusted stream metadata",
                )
            })?;
        let logical_batch = without_tombstone(batch)?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) || tombstones.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "current-generation projection contains a null id or tombstone",
                ));
            }
            let logical_id = ids.value(row);
            let metadata = decode_trusted_stream_metadata(metadata_array.as_ref(), row)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "current-generation projection row has no trusted attribution",
                    )
                })?;
            if metadata.stream_incarnation_id != stream_incarnation_id {
                return Err(OmniError::manifest_internal(
                    "current-generation projection row belongs to another stream incarnation",
                ));
            }
            metadata
                .validate_for(identity, logical_id)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let canonical_payload = canonical_stream_payload_v1(&logical_batch, row)?;
            let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
                identity,
                accepted_schema_hash,
                canonical_payload: &canonical_payload,
            })
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            if payload_digest != metadata.payload_digest {
                return Err(OmniError::manifest_internal(format!(
                    "current-generation projection payload digest is corrupt for key '{logical_id}'"
                )));
            }
            winners.insert(
                logical_id.to_string(),
                ProjectionWinner {
                    tombstone: tombstones.value(row),
                    metadata,
                },
            );
        }
    }
    lww_projection_digest_for_authority(
        identity,
        binding_scope_id,
        enrollment_id,
        shard_id,
        stream_incarnation_id,
        stream_configuration_digest,
        physical_binding_digest,
        &winners,
    )
}

/// Pure entry seam used by focused protocol tests. Production callers use the
/// concrete public Lance [`WalTailer`] path above.
fn authenticate_claim_wal_entries(
    plan: &ClaimWalAuthenticationPlan,
    entries: impl IntoIterator<Item = WalReadEntry>,
) -> Result<AuthenticatedClaimWalSegment> {
    validate_claim_wal_plan(plan)?;
    let entry_count =
        bounded_segment_entry_count(plan.prior_tail.position, plan.planned_sentinel_position)?;
    let mut authenticator = ClaimWalSegmentAuthenticator::new(plan, entry_count)?;
    for entry in entries {
        authenticator.push(entry)?;
    }
    authenticator.finish()
}

struct ClaimWalKeyCollector<'a> {
    plan: &'a ClaimWalKeyDiscoveryPlan,
    expected_entries: u64,
    next_position: u64,
    observed_entries: u64,
    rows: u64,
    arrow_bytes: u64,
    keys: BTreeSet<String>,
    saw_sentinel: bool,
}

impl<'a> ClaimWalKeyCollector<'a> {
    fn new(plan: &'a ClaimWalKeyDiscoveryPlan, expected_entries: u64) -> Result<Self> {
        Ok(Self {
            plan,
            expected_entries,
            next_position: plan
                .prior_position
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?,
            observed_entries: 0,
            rows: 0,
            arrow_bytes: 0,
            keys: BTreeSet::new(),
            saw_sentinel: false,
        })
    }

    fn push(&mut self, entry: WalReadEntry) -> Result<()> {
        if self.saw_sentinel || entry.entry_position != self.next_position {
            return Err(OmniError::manifest_internal(format!(
                "stream claim WAL key-discovery pass expected position {}, observed {}",
                self.next_position, entry.entry_position
            )));
        }
        if entry.shard_id.to_string() != self.plan.shard_id {
            return Err(OmniError::manifest_internal(
                "stream claim WAL key-discovery pass observed a foreign shard",
            ));
        }
        let is_sentinel = entry.entry_position == self.plan.sentinel_position;
        if is_sentinel {
            if entry.writer_epoch != self.plan.achieved_writer_epoch || !entry.batches.is_empty() {
                return Err(OmniError::manifest_internal(
                    "stream claim WAL key-discovery endpoint is not the exact empty sentinel",
                ));
            }
            self.saw_sentinel = true;
        } else if entry.writer_epoch != self.plan.prior_writer_epoch || entry.batches.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream claim WAL key-discovery pass observed a foreign-epoch or empty data entry",
            ));
        }
        let expected_schema = schema_with_tombstone(self.plan.expected_table_schema.as_ref());
        for batch in entry.batches {
            if batch.num_rows() == 0 || batch.schema().as_ref() != expected_schema.as_ref() {
                return Err(OmniError::manifest_internal(
                    "stream claim WAL key-discovery batch is empty or differs from the exact bound schema",
                ));
            }
            let batch_rows = u64::try_from(batch.num_rows())
                .map_err(|_| OmniError::manifest_internal("stream WAL row count exceeds u64"))?;
            self.rows = self
                .rows
                .checked_add(batch_rows)
                .ok_or_else(|| OmniError::manifest_internal("stream WAL row count overflow"))?;
            if self.rows > B1_MAX_GENERATION_ROWS {
                return Err(OmniError::resource_limit(
                    "stream_claim_wal_rows",
                    B1_MAX_GENERATION_ROWS,
                    self.rows,
                ));
            }
            let bytes = b1_logical_batch_bytes(&batch)
                .map_err(|error| OmniError::Lance(error.to_string()))?;
            self.arrow_bytes = self.arrow_bytes.checked_add(bytes).ok_or_else(|| {
                OmniError::manifest_internal("stream WAL Arrow-byte sum overflow")
            })?;
            if self.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
                return Err(OmniError::resource_limit(
                    "stream_claim_wal_arrow_bytes",
                    B1_MAX_GENERATION_ARROW_BYTES,
                    self.arrow_bytes,
                ));
            }
            let ids = batch
                .column_by_name("id")
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream claim WAL key-discovery batch has no exact Utf8 id",
                    )
                })?;
            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    return Err(OmniError::manifest_internal(
                        "stream claim WAL key-discovery batch contains a null id",
                    ));
                }
                self.keys.insert(ids.value(row).to_string());
            }
        }
        self.observed_entries = self
            .observed_entries
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL entry count overflow"))?;
        self.next_position = self
            .next_position
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?;
        Ok(())
    }

    fn finish(self) -> Result<BTreeSet<String>> {
        if self.observed_entries != self.expected_entries || !self.saw_sentinel {
            return Err(OmniError::manifest_internal(
                "stream claim WAL key-discovery pass did not reach its exact sentinel",
            ));
        }
        Ok(self.keys)
    }
}

struct ClaimWalSegmentAuthenticator<'a> {
    plan: &'a ClaimWalAuthenticationPlan,
    expected_entry_count: u64,
    next_position: u64,
    observed_entries: u64,
    rows: u64,
    arrow_bytes: u64,
    segment_hasher: CanonicalHasher,
    winners: BTreeMap<String, ProjectionWinner>,
    folded_chain_by_key: BTreeMap<String, SegmentTokenChain>,
    active_chain_by_key: BTreeMap<String, SegmentTokenChain>,
    saw_sentinel: bool,
}

#[derive(Debug, Clone)]
struct ProjectionWinner {
    tombstone: bool,
    metadata: TrustedStreamRowMetadata,
}

#[derive(Debug, Clone, Copy)]
struct SegmentTokenChain {
    last_token: StreamToken,
    fold_base_token: Option<StreamToken>,
    chain_depth: u32,
}

impl<'a> ClaimWalSegmentAuthenticator<'a> {
    fn new(plan: &'a ClaimWalAuthenticationPlan, entry_count: u64) -> Result<Self> {
        let mut segment_hasher = CanonicalHasher::new(CLAIM_WAL_SEGMENT_DOMAIN);
        segment_hasher.u32(CLAIM_WAL_SEGMENT_PROTOCOL_VERSION);
        segment_hasher.u64(plan.identity.stable_table_id);
        segment_hasher.u64(plan.identity.table_incarnation_id);
        segment_hasher.field(plan.binding_scope_id.as_bytes());
        segment_hasher.field(plan.enrollment_id.as_bytes());
        segment_hasher.field(plan.shard_id.as_bytes());
        segment_hasher.field(plan.stream_incarnation_id.as_bytes());
        segment_hasher.field(plan.stream_configuration_digest.as_bytes());
        segment_hasher.field(plan.physical_binding_digest.as_bytes());
        segment_hasher.u64(plan.prior_tail.position);
        segment_hasher.u64(plan.folded_replay_cursor);
        segment_hasher.u64(plan.planned_sentinel_position);
        segment_hasher.u64(entry_count);
        Ok(Self {
            plan,
            expected_entry_count: entry_count,
            next_position: plan
                .prior_tail
                .position
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?,
            observed_entries: 0,
            rows: 0,
            arrow_bytes: 0,
            segment_hasher,
            winners: BTreeMap::new(),
            folded_chain_by_key: BTreeMap::new(),
            active_chain_by_key: BTreeMap::new(),
            saw_sentinel: false,
        })
    }

    fn push(&mut self, entry: WalReadEntry) -> Result<()> {
        if self.saw_sentinel {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL segment contains data beyond its sentinel",
            ));
        }
        if entry.entry_position != self.next_position {
            return Err(OmniError::manifest_internal(format!(
                "stream authenticated WAL segment expected position {}, observed {}",
                self.next_position, entry.entry_position
            )));
        }
        if entry.shard_id.to_string() != self.plan.shard_id {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL entry belongs to a foreign shard",
            ));
        }
        let is_sentinel = entry.entry_position == self.plan.planned_sentinel_position;
        if is_sentinel {
            if entry.writer_epoch != self.plan.achieved_writer_epoch || !entry.batches.is_empty() {
                return Err(OmniError::manifest_internal(
                    "stream claim endpoint is not the exact empty achieved-epoch sentinel",
                ));
            }
            self.saw_sentinel = true;
        } else if entry.writer_epoch != self.plan.prior_writer_epoch || entry.batches.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream claim WAL suffix contains a foreign-epoch or empty data entry",
            ));
        }

        self.segment_hasher.byte(if is_sentinel { 1 } else { 0 });
        self.segment_hasher.u64(entry.entry_position);
        self.segment_hasher.u64(entry.writer_epoch);
        self.segment_hasher.u64(
            u64::try_from(entry.batches.len())
                .map_err(|_| OmniError::manifest_internal("stream WAL batch count exceeds u64"))?,
        );
        let folded_prefix = !is_sentinel && entry.entry_position <= self.plan.folded_replay_cursor;
        for (batch_ordinal, batch) in entry.batches.into_iter().enumerate() {
            self.push_batch(batch_ordinal, batch, folded_prefix)?;
        }
        self.observed_entries = self
            .observed_entries
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL entry count overflow"))?;
        self.next_position = self
            .next_position
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL cursor overflow"))?;
        Ok(())
    }

    fn push_batch(
        &mut self,
        batch_ordinal: usize,
        batch: RecordBatch,
        folded_prefix: bool,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL data entry contains an empty batch",
            ));
        }
        let expected_schema = schema_with_tombstone(self.plan.expected_table_schema.as_ref());
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL batch differs from the exact bound table schema",
            ));
        }
        let batch_rows = u64::try_from(batch.num_rows())
            .map_err(|_| OmniError::manifest_internal("stream WAL row count exceeds u64"))?;
        self.rows = self
            .rows
            .checked_add(batch_rows)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL row count overflow"))?;
        if self.rows > B1_MAX_GENERATION_ROWS {
            return Err(OmniError::resource_limit(
                "stream_claim_wal_rows",
                B1_MAX_GENERATION_ROWS,
                self.rows,
            ));
        }
        let logical_bytes =
            b1_logical_batch_bytes(&batch).map_err(|error| OmniError::Lance(error.to_string()))?;
        self.arrow_bytes = self
            .arrow_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| OmniError::manifest_internal("stream WAL Arrow-byte sum overflow"))?;
        if self.arrow_bytes > B1_MAX_GENERATION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_claim_wal_arrow_bytes",
                B1_MAX_GENERATION_ARROW_BYTES,
                self.arrow_bytes,
            ));
        }

        // Rebuild every selected array before retaining any winner metadata.
        // This releases a potentially sparse IPC/scanner backing buffer at the
        // end of the call while preserving exact logical slice accounting.
        let row_count = u32::try_from(batch.num_rows())
            .map_err(|_| OmniError::manifest_internal("stream WAL batch exceeds u32 rows"))?;
        let indices = UInt32Array::from_iter_values(0..row_count);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None)
                    .map_err(|error| OmniError::Lance(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let dense = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| OmniError::Lance(error.to_string()))?;

        self.segment_hasher.u64(
            u64::try_from(batch_ordinal).map_err(|_| {
                OmniError::manifest_internal("stream WAL batch ordinal exceeds u64")
            })?,
        );
        self.segment_hasher.u64(batch_rows);
        self.authenticate_dense_batch(&dense, folded_prefix)
    }

    fn authenticate_dense_batch(&mut self, batch: &RecordBatch, folded_prefix: bool) -> Result<()> {
        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream authenticated WAL batch has no exact Utf8 id column",
                )
            })?;
        let tombstones = batch
            .column_by_name(TOMBSTONE)
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream authenticated WAL batch has no exact Boolean tombstone column",
                )
            })?;
        let metadata_array = batch
            .column_by_name(crate::db::STREAM_METADATA_COLUMN)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream authenticated WAL batch omits trusted stream metadata",
                )
            })?;
        let logical_batch = without_tombstone(batch)?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) || tombstones.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "stream authenticated WAL row has a null id or tombstone",
                ));
            }
            let logical_id = ids.value(row);
            let metadata = decode_trusted_stream_metadata(metadata_array.as_ref(), row)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream authenticated WAL row has no trusted attribution",
                    )
                })?;
            if metadata.stream_incarnation_id != self.plan.stream_incarnation_id {
                return Err(OmniError::manifest_internal(
                    "stream authenticated WAL row belongs to another stream incarnation",
                ));
            }
            metadata
                .validate_for(self.plan.identity, logical_id)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let canonical_payload = canonical_stream_payload_v1(&logical_batch, row)?;
            let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
                identity: self.plan.identity,
                accepted_schema_hash: &self.plan.accepted_schema_hash,
                canonical_payload: &canonical_payload,
            })
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            if payload_digest != metadata.payload_digest {
                return Err(OmniError::manifest_internal(format!(
                    "stream authenticated WAL payload digest is corrupt for key '{logical_id}'"
                )));
            }
            let chains = if folded_prefix {
                &mut self.folded_chain_by_key
            } else {
                &mut self.active_chain_by_key
            };
            match chains.get(logical_id).copied() {
                None if folded_prefix => {
                    // This prefix has already been published to base/token
                    // authority by an ordinary fold. Its first retained row
                    // may continue a chain which began before the selected
                    // authenticated tail; internal continuity plus the
                    // terminal current-token anchor below proves the prefix.
                    chains.insert(
                        logical_id.to_string(),
                        SegmentTokenChain {
                            last_token: metadata.stream_token,
                            fold_base_token: metadata.fold_base_token,
                            chain_depth: metadata.chain_depth,
                        },
                    );
                }
                None => {
                    let prior_authority = self
                        .plan
                        .prior_token_by_key
                        .get(logical_id)
                        .copied()
                        .ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "stream authenticated WAL has no exact prior token authority for active key '{logical_id}'"
                            ))
                        })?;
                    if metadata.predecessor_token != prior_authority
                        || metadata.fold_base_token != prior_authority
                        || metadata.chain_depth != 1
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "stream authenticated WAL active first occurrence does not chain from exact prior authority for key '{logical_id}'"
                        )));
                    }
                    chains.insert(
                        logical_id.to_string(),
                        SegmentTokenChain {
                            last_token: metadata.stream_token,
                            fold_base_token: prior_authority,
                            chain_depth: metadata.chain_depth,
                        },
                    );
                }
                Some(prior) => {
                    let expected_depth = prior.chain_depth.checked_add(1).ok_or_else(|| {
                        OmniError::manifest_internal(
                            "stream authenticated WAL token chain depth overflow",
                        )
                    })?;
                    if metadata.predecessor_token != Some(prior.last_token)
                        || metadata.fold_base_token != prior.fold_base_token
                        || metadata.chain_depth != expected_depth
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "stream authenticated WAL token chain is discontinuous for key '{logical_id}'"
                        )));
                    }
                    chains.insert(
                        logical_id.to_string(),
                        SegmentTokenChain {
                            last_token: metadata.stream_token,
                            fold_base_token: prior.fold_base_token,
                            chain_depth: metadata.chain_depth,
                        },
                    );
                }
            }

            let metadata_bytes = canonical_json_bytes("trusted stream metadata", &metadata)?;
            self.segment_hasher.field(logical_id.as_bytes());
            self.segment_hasher.byte(u8::from(tombstones.value(row)));
            self.segment_hasher.field(&metadata_bytes);
            self.winners.insert(
                logical_id.to_string(),
                ProjectionWinner {
                    tombstone: tombstones.value(row),
                    metadata,
                },
            );
        }
        Ok(())
    }

    fn finish(self) -> Result<AuthenticatedClaimWalSegment> {
        if self.observed_entries != self.expected_entry_count || !self.saw_sentinel {
            return Err(OmniError::manifest_internal(format!(
                "stream authenticated WAL segment expected {} entries ending in a sentinel, observed {}",
                self.expected_entry_count, self.observed_entries
            )));
        }
        for (logical_id, folded) in &self.folded_chain_by_key {
            if self
                .plan
                .prior_token_by_key
                .get(logical_id)
                .copied()
                .flatten()
                != Some(folded.last_token)
            {
                return Err(OmniError::manifest_internal(format!(
                    "stream authenticated WAL folded prefix does not terminate at current authority for key '{logical_id}'"
                )));
            }
        }
        let observed_keys = self
            .folded_chain_by_key
            .keys()
            .chain(self.active_chain_by_key.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        if observed_keys.len() != self.plan.prior_token_by_key.len()
            || self
                .plan
                .prior_token_by_key
                .keys()
                .any(|key| !observed_keys.contains(key))
        {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL prior-token probe contains missing or foreign keys",
            ));
        }
        let suffix_lww_projection_digest = lww_projection_digest(self.plan, &self.winners)?;
        let segment_digest = self.segment_hasher.finish();
        let empty_fence_state_digest = stream_empty_fence_state_digest(
            &self.plan.binding_scope_id,
            &self.plan.enrollment_id,
            &self.plan.shard_id,
            &self.plan.stream_incarnation_id,
            &self.plan.stream_configuration_digest,
            &self.plan.physical_binding_digest,
            self.plan.planned_sentinel_position,
            self.plan.achieved_writer_epoch,
            &self.plan.planned_sentinel_digest,
        )?;
        Ok(AuthenticatedClaimWalSegment {
            identity: self.plan.identity,
            binding_scope_id: self.plan.binding_scope_id.clone(),
            enrollment_id: self.plan.enrollment_id.clone(),
            shard_id: self.plan.shard_id.clone(),
            stream_incarnation_id: self.plan.stream_incarnation_id.clone(),
            prior_writer_epoch: self.plan.prior_writer_epoch,
            achieved_writer_epoch: self.plan.achieved_writer_epoch,
            prior_position: self.plan.prior_tail.position,
            position: self.plan.planned_sentinel_position,
            published_prefix_position: self.plan.folded_replay_cursor,
            entry_count: self.observed_entries,
            row_count: self.rows,
            arrow_bytes: self.arrow_bytes,
            sentinel_digest: self.plan.planned_sentinel_digest.clone(),
            segment_digest,
            empty_fence_state_digest,
            suffix_lww_projection_digest,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_terminal_claim(
    prior_claim_chain: &ReceiptChainRef,
    terminal_attempt: &PreparedClaimAttempt,
    terminal_effect: &ClaimAttemptEffect,
    terminal_attempt_chain: &ReceiptChainRef,
    segment: &AuthenticatedClaimWalSegment,
    current_generation_lww_projection_digest: &str,
    replay_cursor: u64,
    recorded_at: i64,
) -> Result<BuiltTerminalClaim> {
    terminal_effect.validate_for_profile(terminal_attempt.operation.profile)?;
    validate_effect_matches_attempt(terminal_attempt, terminal_effect)?;
    let expected_attempt_chain = terminal_effect.next_attempt_chain_ref()?;
    if terminal_attempt_chain != &expected_attempt_chain {
        return Err(OmniError::manifest_internal(
            "terminal claim attempt chain differs from the exact classified attempt head",
        ));
    }
    let terminal_classification = match terminal_effect.classification {
        ClaimAttemptClassification::StockManifestPlusSentinel => {
            ClaimTerminalClassification::StockManifestPlusSentinel
        }
        ClaimAttemptClassification::PatchedSentinelPlusNamingManifest => {
            ClaimTerminalClassification::PatchedSentinelPlusNamingManifest
        }
        _ => {
            return Err(OmniError::manifest_internal(
                "a nonterminal claim attempt cannot produce a terminal claim receipt",
            ));
        }
    };
    let achieved_shard_manifest_version = terminal_effect
        .achieved_shard_manifest_version
        .ok_or_else(|| OmniError::manifest_internal("terminal claim has no manifest version"))?;
    let achieved_writer_epoch = terminal_effect
        .achieved_writer_epoch
        .ok_or_else(|| OmniError::manifest_internal("terminal claim has no writer epoch"))?;
    validate_segment_matches_operation(
        &terminal_attempt.operation,
        terminal_attempt,
        terminal_effect,
        segment,
    )?;
    if replay_cursor > segment.position || recorded_at <= 0 {
        return Err(OmniError::manifest_internal(
            "terminal claim replay cursor or timestamp is invalid",
        ));
    }
    validate_digest(
        "claim full current-generation LWW projection",
        current_generation_lww_projection_digest,
    )?;

    let next_segment_count = terminal_attempt
        .operation
        .prior_authenticated_tail
        .segment_count
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("authenticated WAL segment-count overflow"))?;
    let next_tail_chain_digest = authenticated_wal_tail_chain_digest(
        &terminal_attempt.operation.binding_scope_id,
        &terminal_attempt.operation.enrollment_id,
        &terminal_attempt.operation.shard_id,
        &terminal_attempt.operation.stream_incarnation_id,
        &terminal_attempt.operation.stream_configuration_digest,
        &terminal_attempt.operation.physical_binding_digest,
        segment.prior_position,
        segment.position,
        segment.entry_count,
        &segment.segment_digest,
        &terminal_attempt
            .operation
            .prior_authenticated_tail
            .chain_digest,
        next_segment_count,
        &segment.empty_fence_state_digest,
        current_generation_lww_projection_digest,
    )?;
    let next_authenticated_tail = AuthenticatedWalTail {
        binding_scope_id: terminal_attempt.operation.binding_scope_id.clone(),
        position: segment.position,
        segment_count: next_segment_count,
        chain_digest: next_tail_chain_digest.clone(),
        lww_projection_digest: current_generation_lww_projection_digest.to_string(),
    };
    next_authenticated_tail.validate()?;

    let mut terminal_digest = CanonicalHasher::new(CLAIM_TERMINAL_EFFECT_DOMAIN);
    terminal_digest.field(terminal_attempt.operation.claim_operation_digest.as_bytes());
    terminal_digest.field(terminal_effect.record_id.as_bytes());
    terminal_digest.field(terminal_effect.attempt_terminal_effect_digest.as_bytes());
    terminal_digest.u64(achieved_shard_manifest_version);
    terminal_digest.u64(achieved_writer_epoch);
    terminal_digest.u64(replay_cursor);
    terminal_digest.field(segment.segment_digest.as_bytes());
    terminal_digest.u64(segment.published_prefix_position);
    terminal_digest.field(segment.empty_fence_state_digest.as_bytes());
    terminal_digest.field(segment.suffix_lww_projection_digest.as_bytes());
    terminal_digest.field(current_generation_lww_projection_digest.as_bytes());
    terminal_digest.field(next_tail_chain_digest.as_bytes());
    let terminal_effect_digest = terminal_digest.finish();

    let receipt = ClaimReceipt::new(
        prior_claim_chain,
        ClaimReceiptPreimage {
            graph_identity_digest: terminal_attempt.operation.graph_identity_digest.clone(),
            identity: terminal_attempt.operation.identity,
            claim_id: terminal_attempt.operation.claim_id.clone(),
            lifecycle_operation_id: terminal_attempt.operation.lifecycle_operation_id.clone(),
            binding_scope_id: terminal_attempt.operation.binding_scope_id.clone(),
            enrollment_id: terminal_attempt.operation.enrollment_id.clone(),
            shard_id: terminal_attempt.operation.shard_id.clone(),
            stream_incarnation_id: terminal_attempt.operation.stream_incarnation_id.clone(),
            stream_configuration_digest: terminal_attempt
                .operation
                .stream_configuration_digest
                .clone(),
            physical_binding_digest: terminal_attempt.operation.physical_binding_digest.clone(),
            recovery_operation_id: terminal_attempt.operation.recovery_operation_id.clone(),
            claim_kind: terminal_attempt.operation.claim_kind.clone(),
            profile: terminal_attempt.operation.profile,
            claim_operation_digest: terminal_attempt.operation.claim_operation_digest.clone(),
            attempt_count: terminal_attempt_chain.record_count,
            attempt_chain_head_id: terminal_attempt_chain.head_record_id.clone().ok_or_else(
                || OmniError::manifest_internal("terminal claim attempt chain has no head"),
            )?,
            attempt_effect_chain_digest: terminal_attempt_chain.chain_digest.clone(),
            terminal_attempt_id: terminal_attempt.attempt_id.clone(),
            terminal_pre_shard_manifest_version: terminal_attempt.pre_shard_manifest_version,
            achieved_shard_manifest_version,
            achieved_writer_epoch,
            sentinel_position: segment.position,
            sentinel_digest: segment.sentinel_digest.clone(),
            replay_cursor,
            authenticated_tail_prior_position: segment.prior_position,
            authenticated_tail_position: segment.position,
            authenticated_tail_published_prefix_position: segment.published_prefix_position,
            authenticated_tail_segment_entry_count: segment.entry_count,
            authenticated_tail_segment_digest: segment.segment_digest.clone(),
            authenticated_tail_segment_lww_projection_digest: segment
                .suffix_lww_projection_digest
                .clone(),
            authenticated_tail_prior_chain_digest: terminal_attempt
                .operation
                .prior_authenticated_tail
                .chain_digest
                .clone(),
            authenticated_tail_segment_count: next_segment_count,
            authenticated_tail_chain_digest: next_tail_chain_digest,
            authenticated_tail_empty_fence_state_digest: segment.empty_fence_state_digest.clone(),
            authenticated_tail_lww_projection_digest: current_generation_lww_projection_digest
                .to_string(),
            terminal_effect_digest,
            terminal_classification,
            recorded_at,
        },
    )?;
    let next_claim_chain = receipt.next_chain_ref()?;
    Ok(BuiltTerminalClaim {
        receipt,
        next_claim_chain,
        next_authenticated_tail,
    })
}

/// Apply a completed claim to a lifecycle row without performing its manifest
/// CAS. Recovery owns the actual publication and must bind this exact value.
pub(crate) fn build_claim_adoption_row(
    prior: &StreamLifecycleEntry,
    built: &BuiltTerminalClaim,
) -> Result<StreamLifecycleEntry> {
    prior.validate()?;
    built.receipt.validate()?;
    let expected_lifecycle_operation = match prior.lifecycle {
        StreamLifecycle::Open => None,
        StreamLifecycle::Draining => Some(
            prior
                .drain
                .as_ref()
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "DRAINING claim adoption has no durable drain descriptor",
                    )
                })?
                .drain_id
                .as_str(),
        ),
        StreamLifecycle::Sealed => {
            return Err(OmniError::manifest_internal(
                "a SEALED lifecycle cannot adopt a writer claim",
            ));
        }
    };
    if built.receipt.identity != prior.identity
        || built.receipt.lifecycle_operation_id.as_deref() != expected_lifecycle_operation
        || built.receipt.binding_scope_id != prior.binding_scope_id
        || built.receipt.enrollment_id != prior.binding.enrollment_id
        || built.receipt.stream_incarnation_id != prior.enrollment_receipt.stream_incarnation_id
        || built.receipt.prior_chain_digest != prior.claim_receipt_chain.chain_digest
        || built.receipt.predecessor_record_id != prior.claim_receipt_chain.head_record_id
        || built.next_claim_chain.head_record_id.as_deref()
            != Some(built.receipt.record_id.as_str())
        || built.next_authenticated_tail.binding_scope_id != prior.binding_scope_id
    {
        return Err(OmniError::manifest_internal(
            "terminal claim result does not extend the exact lifecycle authority",
        ));
    }
    let mut next = prior.clone();
    next.lifecycle_revision = next_revision(prior.lifecycle_revision)?;
    next.claim_receipt_chain = built.next_claim_chain.clone();
    next.current_claim_receipt_id = Some(built.receipt.record_id.clone());
    next.authenticated_wal_tail = built.next_authenticated_tail.clone();
    let epoch = next
        .epoch_floor_by_shard
        .get_mut(&built.receipt.shard_id)
        .ok_or_else(|| {
            OmniError::manifest_internal("terminal claim shard is absent from lifecycle authority")
        })?;
    if built.receipt.achieved_writer_epoch <= *epoch {
        return Err(OmniError::manifest_internal(
            "terminal claim must advance the lifecycle writer epoch",
        ));
    }
    *epoch = built.receipt.achieved_writer_epoch;
    if let Some(drain) = next.drain.as_mut() {
        let target = drain
            .target_epoch_floor_by_shard
            .get_mut(&built.receipt.shard_id)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "terminal claim shard is absent from the active drain target",
                )
            })?;
        *target = (*target).max(built.receipt.achieved_writer_epoch);
    }
    next.validate_successor_of(prior)?;
    Ok(next)
}

/// Apply the exact claim and management receipts of one recovery-v15 resume
/// to its prior closed lifecycle row. The caller still owns the sole manifest
/// CAS; this pure builder only derives the one `OPEN` value that CAS may name.
pub(crate) fn build_resume_adoption_row(
    prior: &StreamLifecycleEntry,
    built: &BuiltTerminalClaim,
    management_receipt: &ManagementReceipt,
    mode: StreamResumeMode,
) -> Result<StreamLifecycleEntry> {
    validate_resume_mode_eligibility(prior, mode)?;
    built.receipt.validate()?;
    let next_lifecycle_revision = next_revision(prior.lifecycle_revision)?;
    management_receipt.validate(next_lifecycle_revision)?;

    let request: StreamResumeRequestPayload =
        serde_json::from_value(management_receipt.request_payload.clone()).map_err(|error| {
            OmniError::manifest_internal(format!(
                "terminal stream resume receipt has a non-canonical request payload: {error}"
            ))
        })?;
    request.validate_for_lifecycle(prior, mode)?;
    let expected_request_payload = request.to_value()?;
    let expected_request_digest = request.request_digest()?;
    let expected_result_payload = stream_resume_result_payload(next_lifecycle_revision)?;
    let expected_result_digest = ManagementReceipt::result_digest_for(&expected_result_payload)?;
    let physical_binding_digest = stream_physical_binding_digest(&prior.binding)?;
    let [shard_id] = prior.binding.shard_ids.as_slice() else {
        return Err(OmniError::manifest_internal(
            "terminal stream resume requires the exact unsharded binding",
        ));
    };
    let expected_empty_projection_digest = lww_projection_digest_for_authority(
        prior.identity,
        &prior.binding_scope_id,
        &prior.binding.enrollment_id,
        shard_id,
        &prior.enrollment_receipt.stream_incarnation_id,
        &prior.binding.stream_config_hash,
        &physical_binding_digest,
        &BTreeMap::new(),
    )?;
    let expected_segment_count = prior
        .authenticated_wal_tail
        .segment_count
        .checked_add(1)
        .ok_or_else(|| {
            OmniError::manifest_internal("terminal stream resume WAL-tail segment-count overflow")
        })?;

    if built.receipt.graph_identity_digest != request.graph_identity_digest
        || built.receipt.identity != prior.identity
        || built.receipt.lifecycle_operation_id.as_deref() != Some(request.resume_id.as_str())
        || built.receipt.binding_scope_id != prior.binding_scope_id
        || built.receipt.enrollment_id != prior.binding.enrollment_id
        || built.receipt.shard_id != *shard_id
        || built.receipt.stream_incarnation_id != prior.enrollment_receipt.stream_incarnation_id
        || built.receipt.stream_configuration_digest != prior.binding.stream_config_hash
        || built.receipt.physical_binding_digest != physical_binding_digest
        || built.receipt.prior_chain_digest != prior.claim_receipt_chain.chain_digest
        || built.receipt.predecessor_record_id != prior.claim_receipt_chain.head_record_id
        || built.next_claim_chain.head_record_id.as_deref()
            != Some(built.receipt.record_id.as_str())
        || built.next_claim_chain != built.receipt.next_chain_ref()?
        || built.next_authenticated_tail.binding_scope_id != prior.binding_scope_id
        || built.receipt.authenticated_tail_prior_position != prior.authenticated_wal_tail.position
        || built.receipt.authenticated_tail_prior_chain_digest
            != prior.authenticated_wal_tail.chain_digest
        || built.receipt.authenticated_tail_segment_count != expected_segment_count
        || built
            .receipt
            .authenticated_tail_segment_lww_projection_digest
            != expected_empty_projection_digest
        || built.receipt.authenticated_tail_lww_projection_digest
            != expected_empty_projection_digest
        || built.next_authenticated_tail.position != built.receipt.authenticated_tail_position
        || built.next_authenticated_tail.segment_count
            != built.receipt.authenticated_tail_segment_count
        || built.next_authenticated_tail.chain_digest
            != built.receipt.authenticated_tail_chain_digest
        || built.next_authenticated_tail.lww_projection_digest
            != built.receipt.authenticated_tail_lww_projection_digest
    {
        return Err(OmniError::manifest_internal(
            "terminal stream resume claim does not extend the exact closed lifecycle authority",
        ));
    }

    let prior_epoch = prior
        .epoch_floor_by_shard
        .get(&built.receipt.shard_id)
        .copied()
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "terminal stream resume claim shard is absent from lifecycle authority",
            )
        })?;
    if built.receipt.achieved_writer_epoch <= prior_epoch {
        return Err(OmniError::manifest_internal(
            "terminal stream resume claim must advance the selected writer epoch",
        ));
    }

    let next_management_chain = management_receipt.next_chain_ref()?;
    if management_receipt.graph_identity_digest != request.graph_identity_digest
        || management_receipt.identity != prior.identity
        || management_receipt.stream_incarnation_id
            != prior.enrollment_receipt.stream_incarnation_id
        || management_receipt.binding_scope_id != prior.binding_scope_id
        || management_receipt.operation_kind != STREAM_RESUME_OPERATION_KIND
        || management_receipt.operation_id != request.resume_id
        || management_receipt.request_payload != expected_request_payload
        || management_receipt.request_digest != expected_request_digest
        || management_receipt.from_revision != prior.lifecycle_revision
        || management_receipt.to_revision != next_lifecycle_revision
        || management_receipt.actor_id != request.actor_id
        || management_receipt.result_payload != expected_result_payload
        || management_receipt.result_digest != expected_result_digest
        || management_receipt.prior_chain_digest != prior.management_receipt_chain.chain_digest
        || management_receipt.predecessor_record_id != prior.management_receipt_chain.head_record_id
        || next_management_chain.head_record_id.as_deref()
            != Some(management_receipt.record_id.as_str())
    {
        return Err(OmniError::manifest_internal(
            "terminal stream resume management receipt does not extend the exact request authority",
        ));
    }

    let mut next = prior.clone();
    next.lifecycle = StreamLifecycle::Open;
    next.lifecycle_revision = next_lifecycle_revision;
    next.management_receipt_chain = next_management_chain;
    next.claim_receipt_chain = built.next_claim_chain.clone();
    next.current_claim_receipt_id = Some(built.receipt.record_id.clone());
    next.authenticated_wal_tail = built.next_authenticated_tail.clone();
    *next
        .epoch_floor_by_shard
        .get_mut(&built.receipt.shard_id)
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "terminal stream resume claim shard disappeared from lifecycle authority",
            )
        })? = built.receipt.achieved_writer_epoch;
    next.drain = None;
    next.strict_block = None;
    next.sealed_proof = None;
    next.validate_successor_of(prior)?;
    Ok(next)
}

pub(crate) fn stream_verified_empty_digest(
    draining: &StreamLifecycleEntry,
    current_claim_receipt: &ClaimReceipt,
    evidence: EmptyCutEvidence,
) -> Result<String> {
    validate_empty_cut(draining, current_claim_receipt, evidence)?;
    let binding_bytes = canonical_json_bytes("verified-empty physical binding", &draining.binding)?;
    let head_bytes = canonical_json_bytes(
        "verified-empty base HEAD witness",
        &draining.current_head_witness,
    )?;
    let claim_chain_bytes =
        canonical_json_bytes("verified-empty claim chain", &draining.claim_receipt_chain)?;
    let tail_bytes = canonical_json_bytes(
        "verified-empty authenticated WAL tail",
        &draining.authenticated_wal_tail,
    )?;
    let fold_summary_bytes = canonical_json_bytes(
        "verified-empty last fold summary",
        &draining.last_fold_summary,
    )?;
    let mut digest = CanonicalHasher::new(VERIFIED_EMPTY_DOMAIN);
    digest.u32(VERIFIED_EMPTY_PROTOCOL_VERSION);
    digest.u64(draining.identity.stable_table_id);
    digest.u64(draining.identity.table_incarnation_id);
    digest.field(draining.binding_scope_id.as_bytes());
    digest.field(draining.enrollment_receipt.stream_incarnation_id.as_bytes());
    digest.field(draining.binding.stream_config_hash.as_bytes());
    digest.field(&binding_bytes);
    digest.field(&head_bytes);
    digest.field(current_claim_receipt.shard_id.as_bytes());
    digest.u64(evidence.shard_manifest_version);
    digest.u64(evidence.writer_epoch);
    digest.u64(evidence.replay_cursor);
    digest.u64(evidence.current_generation);
    digest.u64(evidence.base_merged_generation);
    digest.field(&claim_chain_bytes);
    digest.field(current_claim_receipt.record_id.as_bytes());
    digest.field(&tail_bytes);
    digest.field(&fold_summary_bytes);
    digest.u64(current_claim_receipt.sentinel_position);
    digest.field(current_claim_receipt.sentinel_digest.as_bytes());
    Ok(digest.finish())
}

/// Construct the exact terminal SEALED row. The caller supplies the immutable
/// terminal management receipt and physical empty-cut facts; this builder
/// derives the proof and rejects any authority that is not already selected by
/// the current DRAINING row.
pub(crate) fn build_draining_to_sealed(
    prior: &StreamLifecycleEntry,
    management_receipt: &ManagementReceipt,
    current_claim_receipt: &ClaimReceipt,
    evidence: EmptyCutEvidence,
) -> Result<StreamLifecycleEntry> {
    prior.validate()?;
    let drain = prior.drain.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("DRAINING to SEALED requires an exact drain descriptor")
    })?;
    if prior.lifecycle != StreamLifecycle::Draining
        || drain.goal != DrainGoal::Sealed
        || prior.strict_block.is_some()
    {
        return Err(OmniError::manifest_internal(
            "only an unblocked DRAINING(goal=SEALED) row may become SEALED",
        ));
    }
    let next_revision = next_revision(prior.lifecycle_revision)?;
    management_receipt.validate(next_revision)?;
    let next_management_chain = management_receipt.next_chain_ref()?;
    let expected_result_payload = stream_quiesce_result_payload(next_revision)?;
    let expected_result_digest = ManagementReceipt::result_digest_for(&expected_result_payload)?;
    if management_receipt.identity != prior.identity
        || management_receipt.stream_incarnation_id
            != prior.enrollment_receipt.stream_incarnation_id
        || management_receipt.binding_scope_id != prior.binding_scope_id
        || management_receipt.operation_kind != "QUIESCE"
        || management_receipt.operation_id != drain.drain_id
        || management_receipt.request_digest != drain.operation_request_digest
        || management_receipt.from_revision != drain.operation_expected_revision
        || management_receipt.to_revision != next_revision
        || management_receipt.actor_id != drain.initiating_actor
        || management_receipt.result_payload != expected_result_payload
        || management_receipt.result_digest != expected_result_digest
        || management_receipt.prior_chain_digest != prior.management_receipt_chain.chain_digest
        || management_receipt.predecessor_record_id != prior.management_receipt_chain.head_record_id
    {
        return Err(OmniError::manifest_internal(
            "terminal quiesce receipt does not extend the exact drain authority",
        ));
    }
    let verified_empty_digest =
        stream_verified_empty_digest(prior, current_claim_receipt, evidence)?;
    let proof = SealedProof {
        drain_id: drain.drain_id.clone(),
        binding_scope_id: prior.binding_scope_id.clone(),
        shard_manifest_version: evidence.shard_manifest_version,
        writer_epoch: evidence.writer_epoch,
        replay_cursor: evidence.replay_cursor,
        current_generation: evidence.current_generation,
        base_merged_generation: evidence.base_merged_generation,
        base_current_head_witness: prior.current_head_witness.clone(),
        current_claim_receipt_id: current_claim_receipt.record_id.clone(),
        claim_receipt_chain: prior.claim_receipt_chain.clone(),
        authenticated_tail_position: prior.authenticated_wal_tail.position,
        authenticated_tail_segment_count: prior.authenticated_wal_tail.segment_count,
        authenticated_tail_chain_digest: prior.authenticated_wal_tail.chain_digest.clone(),
        current_sentinel_position: current_claim_receipt.sentinel_position,
        current_sentinel_digest: current_claim_receipt.sentinel_digest.clone(),
        verified_empty_digest,
    };
    let mut next = prior.clone();
    next.lifecycle = StreamLifecycle::Sealed;
    next.lifecycle_revision = next_revision;
    next.management_receipt_chain = next_management_chain;
    next.drain = None;
    next.strict_block = None;
    next.sealed_proof = Some(proof);
    next.validate_successor_of(prior)?;
    Ok(next)
}

fn validate_empty_cut(
    draining: &StreamLifecycleEntry,
    receipt: &ClaimReceipt,
    evidence: EmptyCutEvidence,
) -> Result<()> {
    draining.validate()?;
    receipt.validate()?;
    let drain = draining.drain.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("stream empty-cut validation requires an active drain")
    })?;
    let exact_empty_generation =
        evidence
            .base_merged_generation
            .checked_add(1)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream empty-cut base merged generation overflows its successor",
                )
            })?;
    let empty_disposition = draining.selected_claim_empty_cut_disposition(
        receipt,
        evidence.shard_manifest_version,
        evidence.writer_epoch,
        evidence.replay_cursor,
        evidence.current_generation,
        evidence.base_merged_generation,
    );
    if draining.lifecycle != StreamLifecycle::Draining
        || draining.strict_block.is_some()
        || receipt.identity != draining.identity
        || receipt.binding_scope_id != draining.binding_scope_id
        || receipt.enrollment_id != draining.binding.enrollment_id
        || receipt.stream_incarnation_id != draining.enrollment_receipt.stream_incarnation_id
        || receipt.stream_configuration_digest != draining.binding.stream_config_hash
        || receipt.physical_binding_digest != stream_physical_binding_digest(&draining.binding)?
        || receipt.lifecycle_operation_id.as_deref() != Some(drain.drain_id.as_str())
        || draining.current_claim_receipt_id.as_deref() != Some(receipt.record_id.as_str())
        || draining.claim_receipt_chain.head_record_id.as_deref()
            != Some(receipt.record_id.as_str())
        || receipt.authenticated_tail_position != draining.authenticated_wal_tail.position
        || receipt.authenticated_tail_segment_count != draining.authenticated_wal_tail.segment_count
        || receipt.authenticated_tail_chain_digest != draining.authenticated_wal_tail.chain_digest
        || receipt.authenticated_tail_lww_projection_digest
            != draining.authenticated_wal_tail.lww_projection_digest
        || evidence.shard_manifest_version < receipt.achieved_shard_manifest_version
        || evidence.writer_epoch != receipt.achieved_writer_epoch
        || receipt.sentinel_position != draining.authenticated_wal_tail.position
        || empty_disposition.is_none()
        || evidence.current_generation != exact_empty_generation
        || draining
            .epoch_floor_by_shard
            .get(&receipt.shard_id)
            .copied()
            != Some(evidence.writer_epoch)
        || drain
            .target_epoch_floor_by_shard
            .get(&receipt.shard_id)
            .copied()
            != Some(evidence.writer_epoch)
    {
        return Err(OmniError::manifest_internal(format!(
            "stream empty-cut evidence differs from current claim, tail, shard, or base authority: \
                 evidence={evidence:?}, claim={{record_id={}, shard={}, manifest_version={}, \
                 writer_epoch={}, replay_cursor={}, sentinel_position={}}}, \
                 lifecycle={{current_claim={:?}, epoch_floor={:?}, drain_target={:?}, \
                 tail_position={}, tail_segments={}, last_fold_summary={:?}}}, \
                 expected_empty_generation={exact_empty_generation}, \
                 empty_disposition={empty_disposition:?}",
            receipt.record_id,
            receipt.shard_id,
            receipt.achieved_shard_manifest_version,
            receipt.achieved_writer_epoch,
            receipt.replay_cursor,
            receipt.sentinel_position,
            draining.current_claim_receipt_id,
            draining.epoch_floor_by_shard.get(&receipt.shard_id),
            drain.target_epoch_floor_by_shard.get(&receipt.shard_id),
            draining.authenticated_wal_tail.position,
            draining.authenticated_wal_tail.segment_count,
            draining.last_fold_summary,
        )));
    }
    Ok(())
}

fn validate_segment_matches_operation(
    operation: &PreparedClaimOperation,
    attempt: &PreparedClaimAttempt,
    effect: &ClaimAttemptEffect,
    segment: &AuthenticatedClaimWalSegment,
) -> Result<()> {
    if segment.identity != operation.identity
        || segment.binding_scope_id != operation.binding_scope_id
        || segment.enrollment_id != operation.enrollment_id
        || segment.shard_id != operation.shard_id
        || segment.stream_incarnation_id != operation.stream_incarnation_id
        || segment.prior_writer_epoch != operation.initial_writer_epoch
        || segment.achieved_writer_epoch != effect.achieved_writer_epoch.unwrap_or(0)
        || segment.prior_position != operation.prior_authenticated_tail.position
        || segment.position != attempt.planned_sentinel_position
        || segment.published_prefix_position != operation.folded_replay_cursor
        || segment.sentinel_digest != attempt.planned_sentinel_digest
    {
        return Err(OmniError::manifest_internal(
            "authenticated WAL segment differs from its exact terminal claim plan",
        ));
    }
    Ok(())
}

fn validate_claim_wal_plan(plan: &ClaimWalAuthenticationPlan) -> Result<()> {
    plan.identity.validate()?;
    validate_digest("claim WAL accepted_schema_hash", &plan.accepted_schema_hash)?;
    validate_uuid("claim WAL binding_scope_id", &plan.binding_scope_id)?;
    validate_uuid("claim WAL enrollment_id", &plan.enrollment_id)?;
    validate_uuid("claim WAL shard_id", &plan.shard_id)?;
    validate_uuid(
        "claim WAL stream_incarnation_id",
        &plan.stream_incarnation_id,
    )?;
    validate_digest(
        "claim WAL stream_configuration_digest",
        &plan.stream_configuration_digest,
    )?;
    validate_digest(
        "claim WAL physical_binding_digest",
        &plan.physical_binding_digest,
    )?;
    plan.prior_tail.validate()?;
    if plan.prior_tail.binding_scope_id != plan.binding_scope_id
        || plan.folded_replay_cursor >= plan.planned_sentinel_position
        || plan.prior_writer_epoch == 0
        || plan.achieved_writer_epoch <= plan.prior_writer_epoch
        || plan.planned_sentinel_position <= plan.prior_tail.position
    {
        return Err(OmniError::manifest_internal(
            "stream claim WAL plan differs from its prior scoped tail or epoch",
        ));
    }
    let sentinel_digest = stream_planned_sentinel_digest(
        &plan.binding_scope_id,
        &plan.enrollment_id,
        &plan.shard_id,
        &plan.stream_incarnation_id,
        plan.planned_sentinel_position,
        plan.achieved_writer_epoch,
    )?;
    if sentinel_digest != plan.planned_sentinel_digest {
        return Err(OmniError::manifest_internal(
            "stream claim WAL sentinel digest differs from its exact endpoint",
        ));
    }
    validate_trusted_stream_metadata_schema(plan.expected_table_schema.as_ref())
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    if plan
        .expected_table_schema
        .column_with_name(TOMBSTONE)
        .is_some()
    {
        return Err(OmniError::manifest_internal(
            "stream claim WAL base schema unexpectedly contains a tombstone",
        ));
    }
    Ok(())
}

fn validate_key_discovery_plan(plan: &ClaimWalKeyDiscoveryPlan) -> Result<()> {
    validate_uuid("claim WAL discovery shard_id", &plan.shard_id)?;
    bounded_segment_entry_count(plan.prior_position, plan.sentinel_position)?;
    if plan.prior_writer_epoch == 0 || plan.achieved_writer_epoch <= plan.prior_writer_epoch {
        return Err(OmniError::manifest_internal(
            "stream claim WAL key-discovery epochs are invalid",
        ));
    }
    validate_trusted_stream_metadata_schema(plan.expected_table_schema.as_ref())
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    if plan
        .expected_table_schema
        .column_with_name(TOMBSTONE)
        .is_some()
    {
        return Err(OmniError::manifest_internal(
            "stream claim WAL key-discovery base schema contains a tombstone",
        ));
    }
    Ok(())
}

fn bounded_segment_entry_count(prior_position: u64, sentinel_position: u64) -> Result<u64> {
    let entry_count = sentinel_position
        .checked_sub(prior_position)
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "stream claim sentinel precedes the authenticated WAL-tail cursor",
            )
        })?;
    if entry_count == 0 || entry_count > MAX_AUTHENTICATED_SEGMENT_ENTRIES {
        return Err(OmniError::resource_limit(
            "stream_claim_wal_segment_entries",
            MAX_AUTHENTICATED_SEGMENT_ENTRIES,
            entry_count,
        ));
    }
    Ok(entry_count)
}

fn validate_effect_matches_attempt(
    attempt: &PreparedClaimAttempt,
    effect: &ClaimAttemptEffect,
) -> Result<()> {
    if effect.graph_identity_digest != attempt.operation.graph_identity_digest
        || effect.identity != attempt.operation.identity
        || effect.stream_incarnation_id != attempt.operation.stream_incarnation_id
        || effect.binding_scope_id != attempt.operation.binding_scope_id
        || effect.enrollment_id != attempt.operation.enrollment_id
        || effect.shard_id != attempt.operation.shard_id
        || effect.claim_id != attempt.operation.claim_id
        || effect.attempt_id != attempt.attempt_id
        || effect.attempt_plan_digest != attempt.attempt_plan_digest
        || effect.bound_prestate_digest != attempt.bound_prestate_digest
        || effect.storage_envelope_digest != attempt.storage_envelope_digest
        || effect.planned_sentinel_position != attempt.planned_sentinel_position
        || effect.planned_sentinel_digest != attempt.planned_sentinel_digest
    {
        return Err(OmniError::manifest_internal(
            "stream claim attempt effect differs from its exact armed plan",
        ));
    }
    Ok(())
}

fn validate_attempt_classification(
    attempt: &PreparedClaimAttempt,
    classification: ClaimAttemptClassification,
) -> Result<()> {
    let permitted = match attempt.operation.profile {
        ClaimProfile::RetainAll => matches!(
            classification,
            ClaimAttemptClassification::NoEffect
                | ClaimAttemptClassification::AbortedNoEffect
                | ClaimAttemptClassification::StockManifestOnly
                | ClaimAttemptClassification::StockManifestPlusSentinel
        ),
        ClaimProfile::ManagedReclamation => matches!(
            classification,
            ClaimAttemptClassification::NoEffect
                | ClaimAttemptClassification::AbortedNoEffect
                | ClaimAttemptClassification::PatchedSentinelOnly
                | ClaimAttemptClassification::PatchedSentinelPlusNamingManifest
        ),
    };
    if permitted {
        Ok(())
    } else {
        Err(OmniError::manifest_internal(
            "stream claim attempt classification is not permitted by its retention profile",
        ))
    }
}

fn validate_storage_envelope(profile: ClaimProfile, digest: Option<&str>) -> Result<()> {
    match (profile, digest) {
        (ClaimProfile::RetainAll, None) => Ok(()),
        (ClaimProfile::RetainAll, Some(_)) => Err(OmniError::manifest_internal(
            "retain-all claim attempt cannot carry a storage envelope",
        )),
        (ClaimProfile::ManagedReclamation, Some(digest)) => {
            validate_digest("claim storage_envelope_digest", digest)
        }
        (ClaimProfile::ManagedReclamation, None) => Err(OmniError::manifest_internal(
            "managed-reclamation claim attempt requires a storage envelope",
        )),
    }
}

fn validate_target_epoch_advance(
    prior: &StreamLifecycleEntry,
    target: &BTreeMap<String, u64>,
) -> Result<()> {
    if target.len() != prior.binding.shard_ids.len() {
        return Err(OmniError::manifest_internal(
            "stream quiesce target epochs must exactly cover the physical shard binding",
        ));
    }
    for shard_id in &prior.binding.shard_ids {
        let prior_epoch = prior
            .epoch_floor_by_shard
            .get(shard_id)
            .copied()
            .ok_or_else(|| {
                OmniError::manifest_internal("stream lifecycle omits its bound shard epoch")
            })?;
        let target_epoch = target.get(shard_id).copied().ok_or_else(|| {
            OmniError::manifest_internal("stream quiesce target omits its bound shard")
        })?;
        if target_epoch <= prior_epoch {
            return Err(OmniError::manifest_internal(
                "a fresh quiesce must target an epoch strictly above the OPEN writer",
            ));
        }
    }
    Ok(())
}

fn lww_projection_digest(
    plan: &ClaimWalAuthenticationPlan,
    winners: &BTreeMap<String, ProjectionWinner>,
) -> Result<String> {
    lww_projection_digest_for_authority(
        plan.identity,
        &plan.binding_scope_id,
        &plan.enrollment_id,
        &plan.shard_id,
        &plan.stream_incarnation_id,
        &plan.stream_configuration_digest,
        &plan.physical_binding_digest,
        winners,
    )
}

#[allow(clippy::too_many_arguments)]
fn lww_projection_digest_for_authority(
    identity: TableIdentity,
    binding_scope_id: &str,
    enrollment_id: &str,
    shard_id: &str,
    stream_incarnation_id: &str,
    stream_configuration_digest: &str,
    physical_binding_digest: &str,
    winners: &BTreeMap<String, ProjectionWinner>,
) -> Result<String> {
    let mut digest = CanonicalHasher::new(CLAIM_WAL_LWW_DOMAIN);
    digest.u64(identity.stable_table_id);
    digest.u64(identity.table_incarnation_id);
    digest.field(binding_scope_id.as_bytes());
    digest.field(enrollment_id.as_bytes());
    digest.field(shard_id.as_bytes());
    digest.field(stream_incarnation_id.as_bytes());
    digest.field(stream_configuration_digest.as_bytes());
    digest.field(physical_binding_digest.as_bytes());
    digest.u64(
        u64::try_from(winners.len())
            .map_err(|_| OmniError::manifest_internal("stream LWW winner count exceeds u64"))?,
    );
    for (logical_id, winner) in winners {
        digest.field(logical_id.as_bytes());
        digest.byte(u8::from(winner.tombstone));
        digest.field(&canonical_json_bytes(
            "stream LWW trusted metadata",
            &winner.metadata,
        )?);
    }
    Ok(digest.finish())
}

fn without_tombstone(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns().saturating_sub(1));
    let mut columns = Vec::with_capacity(batch.num_columns().saturating_sub(1));
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if field.name() != TOMBSTONE {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    if fields.len().checked_add(1) != Some(batch.num_columns()) {
        return Err(OmniError::manifest_internal(
            "stream WAL batch must contain exactly one trailing tombstone field",
        ));
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|error| OmniError::Lance(error.to_string()))
}

fn canonical_json_bytes<T: Serialize>(field: &str, value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode stream {field}: {error}"))
    })
}

fn next_revision(current: u64) -> Result<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream lifecycle revision overflow"))
}

fn claim_profile_tag(profile: ClaimProfile) -> u8 {
    match profile {
        ClaimProfile::RetainAll => 0,
        ClaimProfile::ManagedReclamation => 1,
    }
}

fn claim_attempt_classification_tag(classification: ClaimAttemptClassification) -> u8 {
    match classification {
        ClaimAttemptClassification::NoEffect => 0,
        ClaimAttemptClassification::AbortedNoEffect => 1,
        ClaimAttemptClassification::StockManifestOnly => 2,
        ClaimAttemptClassification::StockManifestPlusSentinel => 3,
        ClaimAttemptClassification::PatchedSentinelOnly => 4,
        ClaimAttemptClassification::PatchedSentinelPlusNamingManifest => 5,
    }
}

fn validate_digest(field: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must use canonical sha256:<lowercase-hex> form"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must contain exactly 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

fn validate_canonical_text(field: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be non-empty canonical text"
        )));
    }
    Ok(())
}

fn validate_protocol_label(field: &str, value: &str) -> Result<()> {
    validate_canonical_text(field, value)?;
    if !value.bytes().enumerate().all(|(index, byte)| match byte {
        b'A'..=b'Z' => true,
        b'0'..=b'9' | b'_' => index > 0,
        _ => false,
    }) {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must use canonical SCREAMING_SNAKE_CASE text"
        )));
    }
    Ok(())
}

fn validate_uuid(field: &str, value: &str) -> Result<ShardId> {
    let parsed = ShardId::parse_str(value).map_err(|error| {
        OmniError::manifest_internal(format!("stream {field} is not a UUID: {error}"))
    })?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be a canonical non-nil lowercase UUID"
        )));
    }
    Ok(parsed)
}

fn validate_uuid_v4(field: &str, value: &str) -> Result<ShardId> {
    let parsed = validate_uuid(field, value)?;
    if parsed.get_version_num() != 4 {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be a UUID v4 value"
        )));
    }
    Ok(parsed)
}

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        Self(hasher)
    }

    fn field(&mut self, bytes: &[u8]) {
        self.0
            .update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        self.0.update(bytes);
    }

    fn byte(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn optional_digest(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.byte(1);
                self.field(value.as_bytes());
            }
            None => self.byte(0),
        }
    }

    fn finish(self) -> String {
        format!("sha256:{:x}", self.0.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{ArrayRef, BooleanArray, StringArray, new_null_array};
    use arrow_schema::{DataType, Field};
    use lance::dataset::refs::BranchIdentifier;

    use crate::db::manifest::stream::{
        claim_attempt_chain_genesis, claim_receipt_chain_genesis, management_receipt_chain_genesis,
        test_sealed_lifecycle_from,
    };
    use crate::db::manifest::stream_token::{
        StreamRowOrigin, StreamTokenInput, TrustedContributorId,
        build_trusted_stream_metadata_array, trusted_stream_metadata_field,
    };
    use crate::db::manifest::{EnrollmentReceipt, STREAM_CONFIG_VERSION, StreamPhysicalBinding};
    use crate::validate::{Violation, ViolationCorrectionEvidence};

    const SCOPE_ID: &str = "11111111-1111-4111-8111-111111111111";
    const ENROLLMENT_ID: &str = "22222222-2222-4222-8222-222222222222";
    const SHARD_ID: &str = "33333333-3333-4333-8333-333333333333";
    const INCARNATION_ID: &str = "44444444-4444-4444-8444-444444444444";
    const WRITE_ID: &str = "55555555-5555-4555-8555-555555555555";
    const ATTEMPT_ID: &str = "66666666-6666-4666-8666-666666666666";

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            trusted_stream_metadata_field(),
        ]))
    }

    fn plan(sentinel_position: u64) -> ClaimWalAuthenticationPlan {
        let planned_sentinel_digest = stream_planned_sentinel_digest(
            SCOPE_ID,
            ENROLLMENT_ID,
            SHARD_ID,
            INCARNATION_ID,
            sentinel_position,
            2,
        )
        .unwrap();
        ClaimWalAuthenticationPlan {
            identity: TableIdentity::new(7, 9).unwrap(),
            accepted_schema_hash: digest('a'),
            expected_table_schema: table_schema(),
            binding_scope_id: SCOPE_ID.to_string(),
            enrollment_id: ENROLLMENT_ID.to_string(),
            shard_id: SHARD_ID.to_string(),
            stream_incarnation_id: INCARNATION_ID.to_string(),
            stream_configuration_digest: digest('b'),
            physical_binding_digest: digest('c'),
            prior_tail: AuthenticatedWalTail::genesis(SCOPE_ID).unwrap(),
            folded_replay_cursor: 0,
            prior_writer_epoch: 1,
            achieved_writer_epoch: 2,
            prior_token_by_key: if sentinel_position == 1 {
                BTreeMap::new()
            } else {
                BTreeMap::from([("key-1".to_string(), None)])
            },
            planned_sentinel_position: sentinel_position,
            planned_sentinel_digest,
        }
    }

    fn sentinel(position: u64) -> WalReadEntry {
        sentinel_at_epoch(position, 2)
    }

    fn sentinel_at_epoch(position: u64, writer_epoch: u64) -> WalReadEntry {
        WalReadEntry {
            shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
            entry_position: position,
            writer_epoch,
            batches: Vec::new(),
        }
    }

    fn prepared_operation(prior_tail: AuthenticatedWalTail) -> PreparedClaimOperation {
        PreparedClaimOperation {
            graph_identity_digest: digest('d'),
            identity: TableIdentity::new(7, 9).unwrap(),
            claim_id: "77777777-7777-4777-8777-777777777777".to_string(),
            lifecycle_operation_id: None,
            recovery_operation_id: "claim-recovery-test".to_string(),
            claim_kind: "COLD_OPEN".to_string(),
            profile: ClaimProfile::RetainAll,
            stream_incarnation_id: INCARNATION_ID.to_string(),
            binding_scope_id: SCOPE_ID.to_string(),
            enrollment_id: ENROLLMENT_ID.to_string(),
            shard_id: SHARD_ID.to_string(),
            stream_configuration_digest: digest('b'),
            physical_binding_digest: digest('c'),
            lifecycle_revision: 1,
            initial_shard_manifest_version: 1,
            initial_writer_epoch: 1,
            initial_replay_cursor: 0,
            initial_current_generation: 1,
            initial_base_merged_generation: 0,
            claim_contract_version: 1,
            folded_replay_cursor: 0,
            prior_authenticated_tail: prior_tail,
            bound_prestate_digest: digest('e'),
            claim_operation_digest: digest('f'),
        }
    }

    fn open_lifecycle() -> StreamLifecycleEntry {
        let binding = StreamPhysicalBinding {
            stable_table_id: 7,
            table_incarnation_id: 9,
            table_location: "nodes/0000000000000007-0000000000000009".to_string(),
            table_branch: None,
            enrollment_id: ENROLLMENT_ID.to_string(),
            shard_ids: vec![SHARD_ID.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: digest('b'),
        };
        let binding_receipt_id = digest('1');
        StreamLifecycleEntry {
            identity: TableIdentity::new(7, 9).unwrap(),
            diagnostic_table_key: "node:Person".to_string(),
            lifecycle: StreamLifecycle::Open,
            binding: binding.clone(),
            binding_scope_id: SCOPE_ID.to_string(),
            current_head_witness: crate::db::manifest::stream::CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 4,
                transaction_uuid: "99999999-9999-4999-8999-999999999999".to_string(),
                manifest_e_tag: None,
            },
            epoch_floor_by_shard: BTreeMap::from([(SHARD_ID.to_string(), 1)]),
            lifecycle_revision: 1,
            enrollment_receipt: EnrollmentReceipt::new(
                "77777777-7777-4777-8777-777777777777".to_string(),
                digest('2'),
                INCARNATION_ID.to_string(),
                binding,
            )
            .unwrap(),
            current_binding_receipt_id: binding_receipt_id.clone(),
            binding_receipt_chain: ReceiptChainRef {
                head_record_id: Some(binding_receipt_id),
                record_count: 1,
                chain_digest: digest('3'),
            },
            management_receipt_chain: management_receipt_chain_genesis(),
            claim_receipt_chain: claim_receipt_chain_genesis(),
            current_claim_receipt_id: None,
            authenticated_wal_tail: AuthenticatedWalTail::genesis(SCOPE_ID).unwrap(),
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        }
    }

    fn resume_request(mode: StreamResumeMode, expected_revision: u64) -> StreamResumeRequest {
        StreamResumeRequest {
            graph_identity_digest: digest('d'),
            resume_id: "88888888-8888-4888-8888-888888888888".to_string(),
            expected_lifecycle_revision: expected_revision,
            mode,
            actor_id: "act-operator".to_string(),
            initiated_at: 10,
            public_named_branches: Vec::new(),
        }
    }

    fn built_resume_claim(
        prior: &StreamLifecycleEntry,
        mode: StreamResumeMode,
        plan: &PreparedStreamResumeOpen,
        initial_shard_manifest_version: u64,
        initial_replay_cursor: u64,
        initial_current_generation: u64,
        initial_base_merged_generation: u64,
    ) -> BuiltTerminalClaim {
        let resume_payload: StreamResumeRequestPayload =
            serde_json::from_value(plan.request_payload.clone()).unwrap();
        let operation = prepare_resume_claim_operation(
            prior,
            ClaimOperationRequest {
                graph_identity_digest: resume_payload.graph_identity_digest,
                claim_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                lifecycle_operation_id: Some(resume_payload.resume_id),
                recovery_operation_id: "stream-resume-recovery".to_string(),
                claim_kind: STREAM_RESUME_OPERATION_KIND.to_string(),
                profile: ClaimProfile::RetainAll,
                shard_id: SHARD_ID.to_string(),
                initial_shard_manifest_version,
                initial_writer_epoch: prior.epoch_floor_by_shard[SHARD_ID],
                initial_replay_cursor,
                initial_current_generation,
                initial_base_merged_generation,
                claim_contract_version: 1,
            },
            mode,
            plan.minimum_next_epoch_floor,
        )
        .unwrap();
        let attempt = prepare_claim_attempt(
            &operation,
            ClaimAttemptRequest {
                attempt_id: ATTEMPT_ID.to_string(),
                pre_shard_manifest_version: initial_shard_manifest_version,
                pre_writer_epoch: operation.initial_writer_epoch,
                pre_replay_cursor: initial_replay_cursor,
                planned_sentinel_position: prior.authenticated_wal_tail.position + 1,
                planned_writer_epoch: plan.minimum_next_epoch_floor,
                storage_envelope_digest: None,
            },
        )
        .unwrap();
        let prior_attempt_chain = claim_attempt_chain_genesis();
        let effect = build_claim_attempt_effect(
            &prior_attempt_chain,
            &attempt,
            ClaimAttemptEvidence::StockManifestPlusSentinel {
                achieved_shard_manifest_version: initial_shard_manifest_version + 1,
                achieved_writer_epoch: plan.minimum_next_epoch_floor,
            },
        )
        .unwrap();
        let attempt_chain = effect.next_attempt_chain_ref().unwrap();
        let empty_projection_digest = lww_projection_digest_for_authority(
            prior.identity,
            &prior.binding_scope_id,
            &prior.binding.enrollment_id,
            SHARD_ID,
            &prior.enrollment_receipt.stream_incarnation_id,
            &prior.binding.stream_config_hash,
            &stream_physical_binding_digest(&prior.binding).unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        let empty_fence_state_digest = stream_empty_fence_state_digest(
            &prior.binding_scope_id,
            &prior.binding.enrollment_id,
            SHARD_ID,
            &prior.enrollment_receipt.stream_incarnation_id,
            &prior.binding.stream_config_hash,
            &stream_physical_binding_digest(&prior.binding).unwrap(),
            attempt.planned_sentinel_position,
            plan.minimum_next_epoch_floor,
            &attempt.planned_sentinel_digest,
        )
        .unwrap();
        let segment = AuthenticatedClaimWalSegment {
            identity: prior.identity,
            binding_scope_id: prior.binding_scope_id.clone(),
            enrollment_id: prior.binding.enrollment_id.clone(),
            shard_id: SHARD_ID.to_string(),
            stream_incarnation_id: prior.enrollment_receipt.stream_incarnation_id.clone(),
            prior_writer_epoch: operation.initial_writer_epoch,
            achieved_writer_epoch: plan.minimum_next_epoch_floor,
            prior_position: prior.authenticated_wal_tail.position,
            position: attempt.planned_sentinel_position,
            published_prefix_position: operation.folded_replay_cursor,
            entry_count: 1,
            row_count: 0,
            arrow_bytes: 0,
            sentinel_digest: attempt.planned_sentinel_digest.clone(),
            segment_digest: digest('4'),
            empty_fence_state_digest,
            suffix_lww_projection_digest: empty_projection_digest.clone(),
        };
        build_terminal_claim(
            &prior.claim_receipt_chain,
            &attempt,
            &effect,
            &attempt_chain,
            &segment,
            &empty_projection_digest,
            initial_replay_cursor,
            plan.recorded_at,
        )
        .unwrap()
    }

    fn resume_management_receipt(
        prior: &StreamLifecycleEntry,
        plan: &PreparedStreamResumeOpen,
    ) -> ManagementReceipt {
        let request: StreamResumeRequestPayload =
            serde_json::from_value(plan.request_payload.clone()).unwrap();
        ManagementReceipt::new(
            request.graph_identity_digest,
            prior.identity,
            prior.enrollment_receipt.stream_incarnation_id.clone(),
            prior.binding_scope_id.clone(),
            &prior.management_receipt_chain,
            request.resume_id,
            STREAM_RESUME_OPERATION_KIND,
            prior.lifecycle_revision,
            plan.next_lifecycle_revision,
            request.actor_id,
            plan.request_payload.clone(),
            stream_resume_result_payload(plan.next_lifecycle_revision).unwrap(),
            plan.recorded_at,
        )
        .unwrap()
    }

    #[test]
    fn sealed_resume_prepares_a_higher_epoch_and_builds_only_the_exact_open_row() {
        let sealed = test_sealed_lifecycle_from(&open_lifecycle()).unwrap();
        let request = resume_request(StreamResumeMode::ResumeSealed, sealed.lifecycle_revision);
        let plan = prepare_stream_resume_open(&sealed, request).unwrap();
        assert_eq!(plan.mode, StreamResumeMode::ResumeSealed);
        assert_eq!(plan.minimum_next_epoch_floor, 2);
        assert_eq!(plan.next_lifecycle_revision, sealed.lifecycle_revision + 1);
        assert_eq!(
            plan.request_digest,
            ManagementReceipt::request_digest_for(&plan.request_payload).unwrap()
        );

        let proof = sealed.sealed_proof.as_ref().unwrap();
        let noncanonical_kind_error = prepare_resume_claim_operation(
            &sealed,
            ClaimOperationRequest {
                graph_identity_digest: digest('d'),
                claim_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                lifecycle_operation_id: Some(
                    "88888888-8888-4888-8888-888888888888".to_string(),
                ),
                recovery_operation_id: "stream-resume-recovery".to_string(),
                claim_kind: "RESUME_FENCE".to_string(),
                profile: ClaimProfile::RetainAll,
                shard_id: SHARD_ID.to_string(),
                initial_shard_manifest_version: proof.shard_manifest_version,
                initial_writer_epoch: proof.writer_epoch,
                initial_replay_cursor: proof.replay_cursor,
                initial_current_generation: proof.current_generation,
                initial_base_merged_generation: proof.base_merged_generation,
                claim_contract_version: 1,
            },
            StreamResumeMode::ResumeSealed,
            plan.minimum_next_epoch_floor,
        )
        .unwrap_err();
        assert!(
            noncanonical_kind_error
                .to_string()
                .contains("canonical RESUME claim kind")
        );

        let built = built_resume_claim(
            &sealed,
            StreamResumeMode::ResumeSealed,
            &plan,
            proof.shard_manifest_version,
            proof.replay_cursor,
            proof.current_generation,
            proof.base_merged_generation,
        );
        assert!(
            build_claim_adoption_row(&sealed, &built).is_err(),
            "ordinary claim adoption must remain closed for SEALED authority"
        );
        let receipt = resume_management_receipt(&sealed, &plan);
        let opened =
            build_resume_adoption_row(&sealed, &built, &receipt, StreamResumeMode::ResumeSealed)
                .unwrap();
        assert_eq!(opened.lifecycle, StreamLifecycle::Open);
        assert_eq!(opened.lifecycle_revision, plan.next_lifecycle_revision);
        assert_eq!(opened.epoch_floor_by_shard[SHARD_ID], 2);
        assert!(opened.drain.is_none());
        assert!(opened.strict_block.is_none());
        assert!(opened.sealed_proof.is_none());
        assert_eq!(
            opened.current_claim_receipt_id.as_deref(),
            Some(built.receipt.record_id.as_str())
        );
        assert_eq!(
            opened.management_receipt_chain.head_record_id.as_deref(),
            Some(receipt.record_id.as_str())
        );

        let ordinary_error = prepare_claim_operation(
            &sealed,
            ClaimOperationRequest {
                graph_identity_digest: digest('d'),
                claim_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                lifecycle_operation_id: None,
                recovery_operation_id: "ordinary-claim".to_string(),
                claim_kind: "COLD_OPEN".to_string(),
                profile: ClaimProfile::RetainAll,
                shard_id: SHARD_ID.to_string(),
                initial_shard_manifest_version: proof.shard_manifest_version,
                initial_writer_epoch: proof.writer_epoch,
                initial_replay_cursor: proof.replay_cursor,
                initial_current_generation: proof.current_generation,
                initial_base_merged_generation: proof.base_merged_generation,
                claim_contract_version: 1,
            },
        )
        .unwrap_err();
        assert!(ordinary_error.to_string().contains("SEALED"));
    }

    #[test]
    fn abort_drain_requires_unguarded_unblocked_empty_authority() {
        let open = open_lifecycle();
        let draining = build_open_to_draining(
            &open,
            QuiesceRequest {
                graph_identity_digest: digest('d'),
                drain_id: "77777777-7777-4777-8777-777777777777".to_string(),
                expected_lifecycle_revision: open.lifecycle_revision,
                goal: DrainGoal::OpenAfterFold,
                initiating_actor: "act-operator".to_string(),
                initiated_at: 5,
                target_epoch_floor_by_shard: BTreeMap::from([(SHARD_ID.to_string(), 2)]),
                seal_override: None,
            },
        )
        .unwrap()
        .lifecycle;
        let request = resume_request(StreamResumeMode::AbortDrain, draining.lifecycle_revision);
        let plan = prepare_stream_resume_open(&draining, request).unwrap();
        let built = built_resume_claim(&draining, StreamResumeMode::AbortDrain, &plan, 1, 0, 1, 0);
        let receipt = resume_management_receipt(&draining, &plan);
        let opened =
            build_resume_adoption_row(&draining, &built, &receipt, StreamResumeMode::AbortDrain)
                .unwrap();
        assert_eq!(opened.lifecycle, StreamLifecycle::Open);
        assert!(opened.drain.is_none());
        assert_eq!(opened.epoch_floor_by_shard[SHARD_ID], 2);

        assert!(
            prepare_stream_resume_open(
                &draining,
                resume_request(StreamResumeMode::ResumeSealed, draining.lifecycle_revision),
            )
            .is_err(),
            "plain resume must not silently become abort-drain"
        );

        let mut guarded = draining.clone();
        guarded.drain.as_mut().unwrap().guarded_operation = Some(serde_json::json!({
            "operation_id": "maintenance"
        }));
        assert!(
            prepare_stream_resume_open(
                &guarded,
                resume_request(StreamResumeMode::AbortDrain, guarded.lifecycle_revision),
            )
            .is_err()
        );

        let mut unmerged = draining.clone();
        let current_claim_id = digest('5');
        unmerged.current_claim_receipt_id = Some(current_claim_id.clone());
        unmerged.claim_receipt_chain = ReceiptChainRef {
            head_record_id: Some(current_claim_id),
            record_count: 1,
            chain_digest: digest('6'),
        };
        unmerged.authenticated_wal_tail = AuthenticatedWalTail {
            binding_scope_id: SCOPE_ID.to_string(),
            position: 1,
            segment_count: 1,
            chain_digest: digest('7'),
            lww_projection_digest: digest('8'),
        };
        unmerged.validate().unwrap();
        let error = prepare_stream_resume_open(
            &unmerged,
            resume_request(StreamResumeMode::AbortDrain, unmerged.lifecycle_revision),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unmerged WAL projection"));

        let mut named_branch_request =
            resume_request(StreamResumeMode::AbortDrain, draining.lifecycle_revision);
        named_branch_request.public_named_branches = vec!["review".to_string()];
        assert!(prepare_stream_resume_open(&draining, named_branch_request).is_err());
    }

    fn attributed_stored_batch(metadata_value: &str, stored_value: &str) -> RecordBatch {
        let schema = table_schema();
        let absent_metadata = build_trusted_stream_metadata_array(&[None]).unwrap();
        let digest_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["key-1"])) as ArrayRef,
                Arc::new(StringArray::from(vec![metadata_value])) as ArrayRef,
                absent_metadata,
            ],
        )
        .unwrap();
        let canonical_payload = canonical_stream_payload_v1(&digest_batch, 0).unwrap();
        let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
            identity: TableIdentity::new(7, 9).unwrap(),
            accepted_schema_hash: &digest('a'),
            canonical_payload: &canonical_payload,
        })
        .unwrap();
        let contributor = TrustedContributorId::new("contributor:test").unwrap();
        let stream_token = StreamToken::derive(&StreamTokenInput {
            identity: TableIdentity::new(7, 9).unwrap(),
            logical_id: "key-1",
            stream_incarnation_id: INCARNATION_ID,
            predecessor_token: None,
            write_id: WRITE_ID,
            contributor_id: &contributor,
            payload_digest,
        })
        .unwrap();
        let metadata = TrustedStreamRowMetadata {
            stream_incarnation_id: INCARNATION_ID.to_string(),
            contributor_id: contributor,
            write_id: WRITE_ID.to_string(),
            predecessor_token: None,
            stream_token,
            fold_base_token: None,
            chain_depth: 1,
            origin: StreamRowOrigin::Admission {
                admission_attempt_id: ATTEMPT_ID.to_string(),
                caller_ordinal: 0,
            },
            payload_digest,
        };
        let metadata_array = build_trusted_stream_metadata_array(&[Some(metadata)]).unwrap();
        let stored_schema = schema_with_tombstone(schema.as_ref());
        RecordBatch::try_new(
            stored_schema,
            vec![
                Arc::new(StringArray::from(vec!["key-1"])) as ArrayRef,
                Arc::new(StringArray::from(vec![stored_value])) as ArrayRef,
                metadata_array,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn oversized_batch() -> RecordBatch {
        let rows = usize::try_from(B1_MAX_GENERATION_ROWS + 1).unwrap();
        let schema = table_schema();
        let stored_schema = schema_with_tombstone(schema.as_ref());
        RecordBatch::try_new(
            stored_schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|index| format!("key-{index}")),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values((0..rows).map(|_| "value"))) as ArrayRef,
                new_null_array(trusted_stream_metadata_field().data_type(), rows),
                Arc::new(BooleanArray::from(vec![false; rows])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn correction(
        logical_key: &str,
        instance: char,
        field: &str,
        actions: &[&str],
    ) -> ViolationCorrectionEvidence {
        ViolationCorrectionEvidence {
            logical_key: logical_key.to_string(),
            field_path_or_group: vec![field.to_string()],
            violation_instance_id: digest(instance),
            allowed_actions: actions.iter().map(|action| (*action).to_string()).collect(),
        }
    }

    fn unique_violation(corrections: Vec<ViolationCorrectionEvidence>) -> Violation {
        Violation {
            table_key: "node:Person".to_string(),
            row_id: None,
            kind: MergeConflictKind::UniqueViolation,
            message: "test uniqueness violation".to_string(),
            corrections,
        }
    }

    #[test]
    fn data_block_correction_view_preserves_bounded_detailed_representation() {
        let tokens = BTreeMap::from([
            ("key-a".to_string(), "token-a".to_string()),
            ("key-b".to_string(), "token-b".to_string()),
        ]);
        let forward = vec![unique_violation(vec![
            correction("key-b", 'b', "email", &["REPLACE", "WITHDRAW"]),
            correction("key-a", 'a', "email", &["REPLACE"]),
        ])];
        let reverse = vec![unique_violation(vec![
            correction("key-a", 'a', "email", &["REPLACE"]),
            correction("key-b", 'b', "email", &["REPLACE", "WITHDRAW"]),
        ])];

        let expected = canonical_data_block_evidence_with_limits(
            "node:Person",
            &forward,
            &tokens,
            2,
            usize::MAX,
        )
        .unwrap();
        let reordered = canonical_data_block_evidence_with_limits(
            "node:Person",
            &reverse,
            &tokens,
            2,
            expected.canonical_bytes,
        )
        .unwrap();

        assert_eq!(expected, reordered);
        assert_eq!(expected.violation_code, "UNIQUE_VIOLATION");
        assert_eq!(expected.entry_count, 2);
        assert_eq!(expected.offending_key_count, 2);
    }

    #[test]
    fn data_block_correction_view_rejects_violation_without_evidence() {
        let tokens = BTreeMap::new();
        let violation = unique_violation(Vec::new());
        let mut collector = DataBlockEvidenceCollector::new("node:Person", &tokens);

        let error = collector
            .push(&violation)
            .expect_err("an empty correction set cannot mint a repairable DataBlock");
        assert_eq!(
            error.to_string(),
            "validator UNIQUE_VIOLATION has no structured stream-correction evidence"
        );
        assert!(
            !collector.saw_violation && collector.offending_keys.is_empty(),
            "rejected evidence must not leave a partial correction view"
        );
        assert!(
            collector.finish().unwrap().is_none(),
            "rejected empty evidence must not produce a DataBlock"
        );
    }

    #[test]
    fn data_block_correction_view_count_overflow_is_key_compact_and_replace_only() {
        let tokens = BTreeMap::from([
            ("key-a".to_string(), "token-a".to_string()),
            ("key-b".to_string(), "token-b".to_string()),
        ]);
        let forward = vec![unique_violation(vec![
            correction("key-a", 'a', "email", &["WITHDRAW"]),
            correction("key-b", 'b', "username", &["REPLACE", "WITHDRAW"]),
        ])];
        let reverse = vec![unique_violation(vec![
            correction("key-b", 'b', "username", &["REPLACE", "WITHDRAW"]),
            correction("key-a", 'a', "email", &["WITHDRAW"]),
        ])];

        let overflow = canonical_data_block_evidence_with_limits(
            "node:Person",
            &forward,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap();
        let reordered = canonical_data_block_evidence_with_limits(
            "node:Person",
            &reverse,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(overflow, reordered);
        assert_eq!(overflow.violation_code, DATA_BLOCK_CORRECTION_VIEW_OVERFLOW);
        assert_eq!(overflow.entry_count, 2);
        assert_eq!(overflow.offending_key_count, 2);

        let instance = overflow_instance_id("node:Person", "key-a", "token-a");
        assert!(is_canonical_sha256_digest(&instance));
        assert_eq!(
            instance,
            overflow_instance_id("node:Person", "key-a", "token-a")
        );
        assert_ne!(
            instance,
            overflow_instance_id("node:Person", "key-a", "token-b")
        );
    }

    #[test]
    fn data_block_correction_view_byte_overflow_uses_durable_aggregate() {
        let tokens = BTreeMap::from([("key-a".to_string(), "token-a".to_string())]);
        let violations = vec![unique_violation(vec![correction(
            "key-a",
            'a',
            "email",
            &["REPLACE"],
        )])];
        let detailed = canonical_data_block_evidence_with_limits(
            "node:Person",
            &violations,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap();
        let exact = canonical_data_block_evidence_with_limits(
            "node:Person",
            &violations,
            &tokens,
            1,
            detailed.canonical_bytes,
        )
        .unwrap();
        assert_eq!(exact, detailed);

        let overflow = canonical_data_block_evidence_with_limits(
            "node:Person",
            &violations,
            &tokens,
            1,
            detailed.canonical_bytes - 1,
        )
        .unwrap();
        assert_eq!(overflow.violation_code, DATA_BLOCK_CORRECTION_VIEW_OVERFLOW);
        assert_eq!(overflow.offending_key_count, 1);
    }

    #[test]
    fn data_block_overflow_does_not_hide_malformed_or_foreign_evidence() {
        let tokens = BTreeMap::from([("key-a".to_string(), "token-a".to_string())]);
        let malformed = vec![unique_violation(vec![
            correction("key-a", 'a', "email", &["REPLACE"]),
            correction("key-a", 'b', "username", &["REPLACE"]),
            correction("key-a", 'c', "display_name", &[]),
        ])];
        let error = canonical_data_block_evidence_with_limits(
            "node:Person",
            &malformed,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(error.to_string().contains("malformed structured"));

        let foreign = vec![Violation {
            table_key: "node:Other".to_string(),
            ..unique_violation(vec![correction("key-a", 'a', "email", &["REPLACE"])])
        }];
        let error = canonical_data_block_evidence_with_limits(
            "node:Person",
            &foreign,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(error.to_string().contains("foreign table"));

        let noncanonical_digest = vec![unique_violation(vec![ViolationCorrectionEvidence {
            violation_instance_id: format!("sha256:{}", "A".repeat(64)),
            ..correction("key-a", 'a', "email", &["REPLACE"])
        }])];
        let error = canonical_data_block_evidence_with_limits(
            "node:Person",
            &noncanonical_digest,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(error.to_string().contains("malformed structured"));

        let missing_token = vec![unique_violation(vec![correction(
            "key-missing",
            'a',
            "email",
            &["REPLACE"],
        )])];
        let error = canonical_data_block_evidence_with_limits(
            "node:Person",
            &missing_token,
            &tokens,
            1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(error.to_string().contains("no current blocked-winner"));
    }

    #[test]
    fn empty_segment_authenticates_one_control_only_sentinel() {
        let plan = plan(1);
        let first = authenticate_claim_wal_entries(&plan, vec![sentinel(1)]).unwrap();
        let second = authenticate_claim_wal_entries(&plan, vec![sentinel(1)]).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.entry_count, 1);
        assert_eq!(first.row_count, 0);
        assert_eq!(first.arrow_bytes, 0);
        assert_eq!(first.position, 1);
        assert_eq!(first.sentinel_digest, plan.planned_sentinel_digest);
    }

    #[test]
    fn sentinel_only_reclaim_keeps_full_replayed_generation_projection() {
        let first_plan = plan(2);
        let replayed = attributed_stored_batch("value", "value");
        let first = authenticate_claim_wal_entries(
            &first_plan,
            vec![
                WalReadEntry {
                    shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
                    entry_position: 1,
                    writer_epoch: 1,
                    batches: vec![replayed.clone()],
                },
                sentinel(2),
            ],
        )
        .unwrap();
        let operation = prepared_operation(AuthenticatedWalTail {
            binding_scope_id: SCOPE_ID.to_string(),
            position: 2,
            segment_count: 1,
            chain_digest: digest('1'),
            lww_projection_digest: first.suffix_lww_projection_digest.clone(),
        });
        let full = current_generation_lww_projection_digest(
            &operation,
            &digest('a'),
            table_schema(),
            &[replayed],
        )
        .unwrap();
        assert_eq!(full, first.suffix_lww_projection_digest);

        let second_plan = ClaimWalAuthenticationPlan {
            prior_tail: operation.prior_authenticated_tail.clone(),
            prior_writer_epoch: 2,
            achieved_writer_epoch: 3,
            planned_sentinel_position: 3,
            planned_sentinel_digest: stream_planned_sentinel_digest(
                SCOPE_ID,
                ENROLLMENT_ID,
                SHARD_ID,
                INCARNATION_ID,
                3,
                3,
            )
            .unwrap(),
            prior_token_by_key: BTreeMap::new(),
            ..first_plan
        };
        let second =
            authenticate_claim_wal_entries(&second_plan, vec![sentinel_at_epoch(3, 3)]).unwrap();
        assert_ne!(
            second.suffix_lww_projection_digest, full,
            "a fence-only suffix is empty, but it must not erase replayed generation winners"
        );
    }

    #[test]
    fn claim_authenticates_folded_prefix_at_its_current_token_terminal() {
        let replayed = attributed_stored_batch("value", "value");
        let metadata = decode_trusted_stream_metadata(
            replayed
                .column_by_name(crate::db::STREAM_METADATA_COLUMN)
                .unwrap()
                .as_ref(),
            0,
        )
        .unwrap()
        .unwrap();
        let mut folded_plan = plan(2);
        folded_plan.folded_replay_cursor = 1;
        folded_plan.prior_token_by_key =
            BTreeMap::from([("key-1".to_string(), Some(metadata.stream_token))]);

        let segment = authenticate_claim_wal_entries(
            &folded_plan,
            vec![
                WalReadEntry {
                    shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
                    entry_position: 1,
                    writer_epoch: 1,
                    batches: vec![replayed],
                },
                sentinel(2),
            ],
        )
        .unwrap();
        assert_eq!(segment.row_count, 1);
        assert_eq!(segment.position, 2);
    }

    #[test]
    fn segment_rejects_gap_and_foreign_data_epoch() {
        let plan = plan(2);
        let gap = authenticate_claim_wal_entries(&plan, vec![sentinel(2)])
            .unwrap_err()
            .to_string();
        assert!(gap.contains("expected position 1"));

        let foreign = WalReadEntry {
            shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
            entry_position: 1,
            writer_epoch: 9,
            batches: vec![attributed_stored_batch("value", "value")],
        };
        let error = authenticate_claim_wal_entries(&plan, vec![foreign, sentinel(2)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("foreign-epoch"));
    }

    #[test]
    fn segment_rejects_payload_tamper_after_trusted_metadata_was_minted() {
        let plan = plan(2);
        let tampered = WalReadEntry {
            shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
            entry_position: 1,
            writer_epoch: 1,
            batches: vec![attributed_stored_batch("original", "tampered")],
        };
        let error = authenticate_claim_wal_entries(&plan, vec![tampered, sentinel(2)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("payload digest is corrupt"));
    }

    #[test]
    fn segment_rejects_rows_above_one_no_roll_generation() {
        let plan = plan(2);
        let oversized = WalReadEntry {
            shard_id: ShardId::parse_str(SHARD_ID).unwrap(),
            entry_position: 1,
            writer_epoch: 1,
            batches: vec![oversized_batch()],
        };
        let error =
            authenticate_claim_wal_entries(&plan, vec![oversized, sentinel(2)]).unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: B1_MAX_GENERATION_ROWS,
                actual,
            } if resource == "stream_claim_wal_rows"
                && actual == B1_MAX_GENERATION_ROWS + 1
        ));
    }

    #[test]
    fn claim_attempt_rejects_shard_manifest_version_regression() {
        let mut operation = prepared_operation(AuthenticatedWalTail::genesis(SCOPE_ID).unwrap());
        operation.initial_shard_manifest_version = 2;
        let error = prepare_claim_attempt(
            &operation,
            ClaimAttemptRequest {
                attempt_id: ATTEMPT_ID.to_string(),
                pre_shard_manifest_version: 1,
                pre_writer_epoch: 1,
                pre_replay_cursor: 0,
                planned_sentinel_position: 1,
                planned_writer_epoch: 2,
                storage_envelope_digest: None,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("prestate"));
    }

    #[test]
    fn claim_attempt_fences_after_a_replay_cursor_ahead_of_the_prior_tail() {
        let mut operation = prepared_operation(AuthenticatedWalTail::genesis(SCOPE_ID).unwrap());
        operation.initial_replay_cursor = 4;
        prepare_claim_attempt(
            &operation,
            ClaimAttemptRequest {
                attempt_id: ATTEMPT_ID.to_string(),
                pre_shard_manifest_version: 1,
                pre_writer_epoch: 1,
                pre_replay_cursor: 4,
                planned_sentinel_position: 5,
                planned_writer_epoch: 2,
                storage_envelope_digest: None,
            },
        )
        .expect("a claim authenticates retained WAL beyond the prior selected tail");

        let error = prepare_claim_attempt(
            &operation,
            ClaimAttemptRequest {
                attempt_id: ATTEMPT_ID.to_string(),
                pre_shard_manifest_version: 1,
                pre_writer_epoch: 1,
                pre_replay_cursor: 4,
                planned_sentinel_position: 4,
                planned_writer_epoch: 2,
                storage_envelope_digest: None,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("prestate"));
    }
}
