//! Durable RFC-026 stream lifecycle authority.
//!
//! Internal schema v12 cuts enrolled streams over to the RFC-026 lifecycle-v3
//! contract while preserving the config-v3 physical writer profile. Immutable
//! control history lives in the manifest-selected token ledger; the hot
//! lifecycle row retains only fixed-size chain references and authenticated
//! WAL-tail authority.

use std::collections::BTreeMap;

use lance::dataset::refs::BranchIdentifier;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

use super::layout::stream_state_object_id;
use super::stream_profile::ReceiptChainRef;
use super::{TableIdentity, TableRegistration};

pub(crate) const STREAM_STATE_PROTOCOL_VERSION: u32 = 3;
pub(crate) const STREAM_CONFIG_VERSION: u32 = 3;
pub(crate) const INITIAL_LIFECYCLE_REVISION: u64 = 1;

pub(crate) const ENROLLMENT_RECEIPT_V2_PROTOCOL_VERSION: u32 = 2;
pub(crate) const BINDING_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const MANAGEMENT_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const STREAM_CORRECTION_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const CLAIM_ATTEMPT_EFFECT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const CLAIM_RECEIPT_PROTOCOL_VERSION: u32 = 1;
pub(crate) const QUIESCE_REQUEST_PROTOCOL_VERSION: u32 = 1;
pub(crate) const STREAM_RESUME_REQUEST_PROTOCOL_VERSION: u32 = 1;
pub(crate) const STREAM_RESUME_OPERATION_KIND: &str = "RESUME";
pub(crate) const STREAM_REBIND_REQUEST_PROTOCOL_VERSION: u32 = 1;
pub(crate) const STREAM_REBIND_OPERATION_KIND: &str = "REBIND";
pub(crate) const STREAM_CORRECTION_OPERATION_KIND: &str = "CORRECTION";
pub(crate) const STREAM_DISABLE_DRAIN_ADOPTION_OPERATION_KIND: &str = "DISABLE_DRAIN_ADOPTION";
pub(crate) const STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION: u32 = 1;

pub(crate) const ENROLLMENT_RECEIPT_V2_TAG: &str = "STREAM_ENROLLMENT_RECEIPT_V2";
pub(crate) const BINDING_RECEIPT_TAG: &str = "STREAM_BINDING_RECEIPT_V1";
pub(crate) const MANAGEMENT_RECEIPT_TAG: &str = "STREAM_MANAGEMENT_RECEIPT_V1";
pub(crate) const STREAM_CORRECTION_RECEIPT_TAG: &str = "STREAM_CORRECTION_RECEIPT_V1";
pub(crate) const CLAIM_ATTEMPT_EFFECT_TAG: &str = "STREAM_CLAIM_ATTEMPT_EFFECT_V1";
pub(crate) const CLAIM_RECEIPT_TAG: &str = "STREAM_CLAIM_RECEIPT_V1";

const BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-binding-receipt-chain.genesis.v1\0";
const MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-management-receipt-chain.genesis.v1\0";
const CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-claim-receipt-chain.genesis.v1\0";
const AUTHENTICATED_WAL_TAIL_CHAIN_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-authenticated-wal-tail-chain.genesis.v1\0";
const AUTHENTICATED_WAL_TAIL_CHAIN_STEP_DOMAIN: &[u8] =
    b"omnigraph.stream-authenticated-wal-tail-chain-step.v1\0";
const AUTHENTICATED_WAL_LWW_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-authenticated-wal-lww.genesis.v1\0";
const ENROLLMENT_RECEIPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-enrollment-receipt-lookup.v2\0";
const ENROLLMENT_RECEIPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-enrollment-receipt-record.v2\0";
const BINDING_RECEIPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-binding-receipt-lookup.v1\0";
const BINDING_RECEIPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-binding-receipt-record.v1\0";
const MANAGEMENT_RECEIPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-management-receipt-lookup.v1\0";
const MANAGEMENT_RECEIPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-management-receipt-record.v1\0";
const MANAGEMENT_REQUEST_DOMAIN: &[u8] = b"omnigraph.stream-management-request.v1\0";
const MANAGEMENT_RESULT_DOMAIN: &[u8] = b"omnigraph.stream-management-result.v1\0";
const DISABLE_DRAIN_ID_DOMAIN: &[u8] = b"omnigraph.stream-disable-drain-id.v1\0";
const DISABLE_DRAIN_ADOPTION_ID_DOMAIN: &[u8] = b"omnigraph.stream-disable-drain-adoption-id.v1\0";
const DISABLE_DRAIN_ADOPTION_OPERATION_ID_DOMAIN: &[u8] =
    b"omnigraph.stream-disable-drain-adoption-operation-id.v1\0";
const STREAM_CORRECTION_RECEIPT_LOOKUP_DOMAIN: &[u8] =
    b"omnigraph.stream-correction-receipt-lookup.v1\0";
const STREAM_CORRECTION_RECEIPT_RECORD_DOMAIN: &[u8] =
    b"omnigraph.stream-correction-receipt-record.v1\0";
const STREAM_LIFECYCLE_AUTHORITY_DIGEST_DOMAIN: &[u8] =
    b"omnigraph.stream-lifecycle-authority.v1\0";
const CLAIM_ATTEMPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-lookup.v1\0";
const CLAIM_ATTEMPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-record.v1\0";
const CLAIM_ATTEMPT_CHAIN_GENESIS_DOMAIN: &[u8] =
    b"omnigraph.stream-claim-attempt-chain.genesis.v1\0";
const CLAIM_ATTEMPT_CHAIN_DOMAIN: &[u8] = b"omnigraph.stream-claim-attempt-chain.v1\0";
const CLAIM_RECEIPT_LOOKUP_DOMAIN: &[u8] = b"omnigraph.stream-claim-receipt-lookup.v1\0";
const CLAIM_RECEIPT_RECORD_DOMAIN: &[u8] = b"omnigraph.stream-claim-receipt-record.v1\0";
const RECEIPT_CHAIN_STEP_DOMAIN: &[u8] = b"omnigraph.stream-receipt-chain-step.v1\0";
const BINDING_RECEIPT_DIGEST_DOMAIN: &[u8] = b"omnigraph.stream-binding-receipt-result.v1\0";
const RETAINED_SHARD_SET_DIGEST_DOMAIN: &[u8] =
    b"omnigraph.stream-retained-shard-set.v1\0";
const STRICT_DATA_BLOCK_TOKEN_DOMAIN: &[u8] = b"omnigraph.stream-data-block-token.v1\0";
const MAX_RECEIPT_JSON_BYTES: usize = 16 * 1024;
pub(crate) const MAX_SELECTED_BINDING_CHAIN_RECORDS: u64 = 1_024;
const STRICT_DATA_BLOCK_MAX_KEYS: u64 = 8_192;
const STRICT_DATA_BLOCK_MAX_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Stable physical enrollment binding for the bounded RFC-026 profile.
///
/// Identity is repeated in the payload and in the manifest row columns. The
/// decoder requires all copies to agree; it never derives authority from the
/// diagnostic alias or from a compatible-looking path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamPhysicalBinding {
    pub(crate) stable_table_id: u64,
    pub(crate) table_incarnation_id: u64,
    pub(crate) table_location: String,
    /// Main is represented canonically as `None`. Named refs are not supported
    /// by the initial v9 retain-all profile.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) table_branch: Option<String>,
    pub(crate) enrollment_id: String,
    /// Sorted, unique UUID namespace. Initial v9 activation permits one shard.
    pub(crate) shard_ids: Vec<String>,
    pub(crate) stream_config_version: u32,
    pub(crate) stream_config_hash: String,
}

impl StreamPhysicalBinding {
    pub(crate) fn identity(&self) -> Result<TableIdentity> {
        TableIdentity::new(self.stable_table_id, self.table_incarnation_id)
    }

    pub(crate) fn validate(&self, expected_identity: TableIdentity) -> Result<()> {
        let embedded_identity = self.identity()?;
        if embedded_identity != expected_identity {
            return Err(OmniError::manifest_internal(format!(
                "stream binding identity {embedded_identity} does not match row identity {expected_identity}"
            )));
        }
        if self.table_location.is_empty() || self.table_location.trim() != self.table_location {
            return Err(OmniError::manifest_internal(
                "stream binding table_location must be non-empty and canonical",
            ));
        }
        if self.table_branch.is_some() {
            return Err(OmniError::manifest_internal(
                "internal schema v9 stream bindings support only canonical main (table_branch = null)",
            ));
        }
        let enrollment_id = validate_uuid("enrollment_id", &self.enrollment_id)?;
        if enrollment_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream enrollment_id must be a UUID v4 value",
            ));
        }
        if self.shard_ids.len() != 1 {
            return Err(OmniError::manifest_internal(format!(
                "internal schema v9 requires exactly one stream shard, got {}",
                self.shard_ids.len()
            )));
        }
        let mut prior: Option<&str> = None;
        for shard_id in &self.shard_ids {
            let parsed = validate_uuid("shard_id", shard_id)?;
            if parsed.get_version_num() != 4 {
                return Err(OmniError::manifest_internal(
                    "stream shard_id must be a UUID v4 value",
                ));
            }
            if parsed == enrollment_id {
                return Err(OmniError::manifest_internal(
                    "stream enrollment_id and shard_id must be distinct identities",
                ));
            }
            if prior.is_some_and(|prior| prior >= shard_id.as_str()) {
                return Err(OmniError::manifest_internal(
                    "stream shard_ids must be strictly sorted and unique",
                ));
            }
            prior = Some(shard_id);
        }
        if self.stream_config_version != STREAM_CONFIG_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream config version {}, expected {}",
                self.stream_config_version, STREAM_CONFIG_VERSION
            )));
        }
        let Some(config_digest) = self.stream_config_hash.strip_prefix("sha256:") else {
            return Err(OmniError::manifest_internal(
                "stream_config_hash must use canonical sha256:<lowercase-hex> form",
            ));
        };
        if config_digest.len() != 64
            || !config_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(OmniError::manifest_internal(
                "stream_config_hash must contain exactly 64 lowercase hexadecimal digits",
            ));
        }
        Ok(())
    }
}

/// Exact public Lance witness for the currently accepted physical table HEAD.
/// This changes on every base-table commit and is not enrollment identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CurrentHeadWitness {
    pub(crate) branch_identifier: BranchIdentifier,
    pub(crate) table_version: u64,
    pub(crate) transaction_uuid: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) manifest_e_tag: Option<String>,
}

impl CurrentHeadWitness {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.branch_identifier != BranchIdentifier::main() {
            return Err(OmniError::manifest_internal(
                "internal schema v9 stream HEAD witness must name the main branch",
            ));
        }
        if self.table_version == 0 {
            return Err(OmniError::manifest_internal(
                "stream HEAD witness table_version must be non-zero",
            ));
        }
        validate_uuid("transaction_uuid", &self.transaction_uuid)?;
        if self
            .manifest_e_tag
            .as_ref()
            .is_some_and(|e_tag| e_tag.is_empty() || e_tag.trim() != e_tag)
        {
            return Err(OmniError::manifest_internal(
                "stream HEAD witness manifest_e_tag must be absent or non-empty canonical text",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StreamLifecycle {
    Open,
    Draining,
    Sealed,
}

impl StreamLifecycle {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Draining => "DRAINING",
            Self::Sealed => "SEALED",
        }
    }
}

/// Exact caller-selected path for one `OPEN`-producing lifecycle operation.
///
/// Both modes use the same recovery and management-receipt family, but their
/// admissible prior authority is intentionally disjoint. Keeping the mode in
/// the canonical request prevents a retry from turning a sealed resume into a
/// drain abort (or vice versa) under the same occurrence ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StreamResumeMode {
    ResumeSealed,
    AbortDrain,
}

/// Immutable lost-result receipt for the one enrollment that created this
/// stream incarnation.  It remains unchanged across physical rebinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnrollmentReceipt {
    pub(crate) enrollment_request_id: String,
    pub(crate) enrollment_intent_digest: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) physical_binding: StreamPhysicalBinding,
    pub(crate) initial_lifecycle_revision: u64,
}

impl EnrollmentReceipt {
    pub(crate) fn new(
        enrollment_request_id: String,
        enrollment_intent_digest: String,
        stream_incarnation_id: String,
        physical_binding: StreamPhysicalBinding,
    ) -> Result<Self> {
        let receipt = Self {
            enrollment_request_id,
            enrollment_intent_digest,
            stream_incarnation_id,
            physical_binding,
            initial_lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub(super) fn validate(&self) -> Result<()> {
        let binding_identity = self.physical_binding.identity()?;
        self.physical_binding.validate(binding_identity)?;
        let request_id = validate_uuid("enrollment_request_id", &self.enrollment_request_id)?;
        if request_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream enrollment_request_id must be a UUID v4 value",
            ));
        }
        validate_digest("enrollment_intent_digest", &self.enrollment_intent_digest)?;
        let stream_incarnation_id =
            validate_uuid("stream_incarnation_id", &self.stream_incarnation_id)?;
        if stream_incarnation_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream stream_incarnation_id must be a UUID v4 value",
            ));
        }
        let enrollment_id = validate_uuid("enrollment_id", &self.physical_binding.enrollment_id)?;
        if request_id == enrollment_id || stream_incarnation_id == enrollment_id {
            return Err(OmniError::manifest_internal(
                "stream request, incarnation, and physical enrollment identities must be distinct",
            ));
        }
        if request_id == stream_incarnation_id {
            return Err(OmniError::manifest_internal(
                "stream enrollment_request_id and stream_incarnation_id must be distinct",
            ));
        }
        for shard_id in &self.physical_binding.shard_ids {
            let shard_id = validate_uuid("shard_id", shard_id)?;
            if shard_id == request_id || shard_id == stream_incarnation_id {
                return Err(OmniError::manifest_internal(
                    "stream logical enrollment identities must be distinct from shard identities",
                ));
            }
        }
        if self.initial_lifecycle_revision != INITIAL_LIFECYCLE_REVISION {
            return Err(OmniError::manifest_internal(format!(
                "stream enrollment receipt initial lifecycle revision must be {INITIAL_LIFECYCLE_REVISION}"
            )));
        }
        Ok(())
    }
}

/// Actor-bound lost-result receipt for the bodyless prepare/enrollment
/// handshake. It is registered in lifecycle-v3 even though public prepare is a
/// later slice, so activation does not require another format reinterpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnrollmentReceiptV2 {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) chain_ordinal: u64,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) enrollment_request_id: String,
    pub(crate) enrollment_intent_digest: String,
    pub(crate) actor_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) physical_binding: StreamPhysicalBinding,
    pub(crate) initial_lifecycle_revision: u64,
    pub(crate) recorded_at: i64,
}

/// Immutable physical-binding record. The lifecycle row derives the active
/// enrollment, shard set, and binding scope only from this selected receipt and
/// its bounded chain commitment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BindingReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) chain_ordinal: u64,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) binding_scope_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) physical_binding: StreamPhysicalBinding,
    pub(crate) shard_ids: Vec<String>,
    /// Fixed-size commitment to every retained physical shard prefix after a
    /// rebind. The historical initial binding (ordinal 2) omits both fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retained_shard_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retained_shard_set_digest: Option<String>,
    pub(crate) operation_id: String,
    pub(crate) receipt_digest: String,
    pub(crate) recorded_at: i64,
}

/// Fixed-size physical-prefix authority carried by the selected binding
/// receipt. This lets hot capture authenticate retained history without
/// walking the immutable receipt chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedShardInventoryCommitment {
    pub(crate) retained_shard_count: u64,
    pub(crate) retained_shard_set_digest: String,
}

/// Terminal receipt for a successful externally initiated lifecycle request.
/// Request and result payloads are bounded canonical objects whose digests are
/// recomputed during validation; a digest alone is never accepted as the
/// request/result preimage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagementReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) chain_ordinal: u64,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) operation_id: String,
    pub(crate) operation_kind: String,
    pub(crate) request_payload: serde_json::Value,
    pub(crate) request_digest: String,
    pub(crate) from_revision: u64,
    pub(crate) to_revision: u64,
    pub(crate) actor_id: String,
    pub(crate) result_payload: serde_json::Value,
    pub(crate) result_digest: String,
    pub(crate) recorded_at: i64,
}

/// Semantic preimage for one immutable, receipt-first DataBlock correction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamCorrectionReceiptPreimage {
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) block_token: String,
    pub(crate) correction_id: String,
    pub(crate) correction_plan_digest: String,
    pub(crate) actor_id: String,
    pub(crate) graph_commit_id: String,
    pub(crate) resulting_manifest_version: u64,
    pub(crate) resulting_lifecycle_revision: u64,
    pub(crate) resulting_lifecycle_digest: String,
    pub(crate) resulting_token_authority_digest: String,
    pub(crate) recorded_at: i64,
}

/// Immutable terminal disposition for one exact `(block, correction_id)`.
/// A retry looks this record up before consulting current lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamCorrectionReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) block_token: String,
    pub(crate) correction_id: String,
    pub(crate) correction_plan_digest: String,
    pub(crate) actor_id: String,
    pub(crate) graph_commit_id: String,
    pub(crate) result_payload: serde_json::Value,
    pub(crate) result_digest: String,
    pub(crate) resulting_manifest_version: u64,
    pub(crate) resulting_lifecycle_revision: u64,
    pub(crate) resulting_lifecycle_digest: String,
    pub(crate) resulting_token_authority_digest: String,
    pub(crate) recorded_at: i64,
}

/// Exact graph-visible outcome needed to release one DataBlock while keeping
/// its drain in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamDataCorrectionOutcome {
    pub(crate) graph_commit_id: String,
    pub(crate) manifest_version: u64,
    pub(crate) current_head_witness: CurrentHeadWitness,
    pub(crate) visible_rows: u64,
    pub(crate) visible_bytes: u64,
    pub(crate) recorded_at: i64,
}

/// The selected durable-retention contract under which a writer claim ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ClaimProfile {
    RetainAll,
    ManagedReclamation,
}

/// Exact terminal classification of one caller-visible claim invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ClaimAttemptClassification {
    NoEffect,
    AbortedNoEffect,
    StockManifestOnly,
    StockManifestPlusSentinel,
    PatchedSentinelOnly,
    PatchedSentinelPlusNamingManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClaimAttemptEffect {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) claim_id: String,
    pub(crate) ordinal: u64,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_attempt_chain_digest: String,
    pub(crate) resulting_attempt_chain_digest: String,
    pub(crate) attempt_id: String,
    pub(crate) attempt_plan_digest: String,
    pub(crate) bound_prestate_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) storage_envelope_digest: Option<String>,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_sentinel_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_shard_manifest_version: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_writer_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_position: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_digest: Option<String>,
    pub(crate) attempt_terminal_effect_digest: String,
    pub(crate) classification: ClaimAttemptClassification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClaimAttemptEffectPreimage {
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) claim_id: String,
    pub(crate) attempt_id: String,
    pub(crate) attempt_plan_digest: String,
    pub(crate) bound_prestate_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) storage_envelope_digest: Option<String>,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_sentinel_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_shard_manifest_version: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_writer_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_position: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_digest: Option<String>,
    pub(crate) attempt_terminal_effect_digest: String,
    pub(crate) classification: ClaimAttemptClassification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ClaimTerminalClassification {
    StockManifestPlusSentinel,
    PatchedSentinelPlusNamingManifest,
}

/// Complete graph-manifest-authoritative projection of one effectful claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClaimReceipt {
    pub(crate) protocol_version: u32,
    pub(crate) record_id: String,
    pub(crate) record_lookup_key: String,
    pub(crate) record_tag: String,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) chain_ordinal: u64,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) predecessor_record_id: Option<String>,
    pub(crate) prior_chain_digest: String,
    pub(crate) resulting_chain_digest: String,
    pub(crate) claim_id: String,
    /// Exact lifecycle operation which owned this physical claim. `None`
    /// denotes an ordinary OPEN cold claim; DRAINING claims retain their
    /// active drain ID independently of the unique claim ID.
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) lifecycle_operation_id: Option<String>,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) stream_configuration_digest: String,
    pub(crate) physical_binding_digest: String,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) claim_operation_digest: String,
    pub(crate) attempt_count: u64,
    pub(crate) attempt_chain_head_id: String,
    pub(crate) attempt_effect_chain_digest: String,
    pub(crate) terminal_attempt_id: String,
    pub(crate) terminal_pre_shard_manifest_version: u64,
    pub(crate) achieved_shard_manifest_version: u64,
    pub(crate) achieved_writer_epoch: u64,
    pub(crate) sentinel_position: u64,
    pub(crate) sentinel_digest: String,
    pub(crate) replay_cursor: u64,
    pub(crate) authenticated_tail_prior_position: u64,
    pub(crate) authenticated_tail_position: u64,
    /// Exact published-fold boundary owned by this claim's authenticated
    /// segment, or zero when no published prefix follows the prior tail.
    pub(crate) authenticated_tail_published_prefix_position: u64,
    pub(crate) authenticated_tail_segment_entry_count: u64,
    pub(crate) authenticated_tail_segment_digest: String,
    /// LWW projection of the exact newly authenticated WAL suffix. This can
    /// be empty for a fence-only re-claim even while the bounded replayed
    /// generation remains non-empty.
    pub(crate) authenticated_tail_segment_lww_projection_digest: String,
    pub(crate) authenticated_tail_prior_chain_digest: String,
    pub(crate) authenticated_tail_segment_count: u64,
    pub(crate) authenticated_tail_chain_digest: String,
    pub(crate) authenticated_tail_empty_fence_state_digest: String,
    pub(crate) authenticated_tail_lww_projection_digest: String,
    pub(crate) terminal_effect_digest: String,
    pub(crate) terminal_classification: ClaimTerminalClassification,
    pub(crate) recorded_at: i64,
}

/// Canonical terminal facts supplied to [`ClaimReceipt::new`]. Keeping the
/// preimage named prevents recovery code from constructing an unsealed receipt
/// with caller-chosen record or chain digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClaimReceiptPreimage {
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) claim_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) lifecycle_operation_id: Option<String>,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) stream_incarnation_id: String,
    pub(crate) stream_configuration_digest: String,
    pub(crate) physical_binding_digest: String,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) claim_operation_digest: String,
    pub(crate) attempt_count: u64,
    pub(crate) attempt_chain_head_id: String,
    pub(crate) attempt_effect_chain_digest: String,
    pub(crate) terminal_attempt_id: String,
    pub(crate) terminal_pre_shard_manifest_version: u64,
    pub(crate) achieved_shard_manifest_version: u64,
    pub(crate) achieved_writer_epoch: u64,
    pub(crate) sentinel_position: u64,
    pub(crate) sentinel_digest: String,
    pub(crate) replay_cursor: u64,
    pub(crate) authenticated_tail_prior_position: u64,
    pub(crate) authenticated_tail_position: u64,
    pub(crate) authenticated_tail_published_prefix_position: u64,
    pub(crate) authenticated_tail_segment_entry_count: u64,
    pub(crate) authenticated_tail_segment_digest: String,
    pub(crate) authenticated_tail_segment_lww_projection_digest: String,
    pub(crate) authenticated_tail_prior_chain_digest: String,
    pub(crate) authenticated_tail_segment_count: u64,
    pub(crate) authenticated_tail_chain_digest: String,
    pub(crate) authenticated_tail_empty_fence_state_digest: String,
    pub(crate) authenticated_tail_lww_projection_digest: String,
    pub(crate) terminal_effect_digest: String,
    pub(crate) terminal_classification: ClaimTerminalClassification,
    pub(crate) recorded_at: i64,
}

/// Fixed-size authority for the authenticated WAL prefix in the current
/// binding scope. Claims alone advance it; folds must preserve it byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthenticatedWalTail {
    pub(crate) binding_scope_id: String,
    pub(crate) position: u64,
    pub(crate) segment_count: u64,
    pub(crate) chain_digest: String,
    pub(crate) lww_projection_digest: String,
}

impl AuthenticatedWalTail {
    pub(crate) fn genesis(binding_scope_id: impl Into<String>) -> Result<Self> {
        let value = Self {
            binding_scope_id: binding_scope_id.into(),
            position: 0,
            segment_count: 0,
            chain_digest: digest_domain(AUTHENTICATED_WAL_TAIL_CHAIN_GENESIS_DOMAIN),
            lww_projection_digest: digest_domain(AUTHENTICATED_WAL_LWW_GENESIS_DOMAIN),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_uuid(
            "authenticated WAL tail binding_scope_id",
            &self.binding_scope_id,
        )?;
        validate_digest("authenticated WAL tail chain_digest", &self.chain_digest)?;
        validate_digest(
            "authenticated WAL tail lww_projection_digest",
            &self.lww_projection_digest,
        )?;
        match (self.position, self.segment_count) {
            (0, 0)
                if self.chain_digest
                    == digest_domain(AUTHENTICATED_WAL_TAIL_CHAIN_GENESIS_DOMAIN)
                    && self.lww_projection_digest
                        == digest_domain(AUTHENTICATED_WAL_LWW_GENESIS_DOMAIN) =>
            {
                Ok(())
            }
            (0, 0) => Err(OmniError::manifest_internal(
                "empty authenticated WAL tail must use the canonical genesis commitments",
            )),
            (0, _) | (_, 0) => Err(OmniError::manifest_internal(
                "authenticated WAL tail position and segment_count must be zero or positive together",
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DrainGoal {
    Sealed,
    OpenAfterFold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DisableDrainAdoption {
    pub(crate) adoption_id: String,
    pub(crate) disable_operation_id: String,
    pub(crate) request_digest: String,
    pub(crate) profile_revision: u64,
    pub(crate) management_receipt_id: String,
    pub(crate) adopted_at: i64,
}

/// Immutable canonical preimage of one drain occurrence.
///
/// A drain's current goal, HEAD witness, target epochs, and seal override may
/// evolve while the occurrence is in progress. This owned request object does
/// not: it remains the exact management-request commitment used for terminal
/// receipt creation and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuiesceRequestPayload {
    pub(crate) protocol_version: u32,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) drain_id: String,
    pub(crate) expected_lifecycle_revision: u64,
    pub(crate) goal: DrainGoal,
    pub(crate) physical_binding_digest: String,
    pub(crate) expected_current_head_witness: CurrentHeadWitness,
    pub(crate) target_epoch_floor_by_shard: BTreeMap<String, u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) seal_override: Option<DisableDrainAdoption>,
}

/// Immutable canonical preimage of one explicit resume or abort-drain
/// occurrence.
///
/// Physical binding, HEAD, profile, runtime, and minimum-epoch authority live
/// in the recovery-v15 open plan beside the complete prior lifecycle row. The
/// management request retains the caller-owned compare token and the exact
/// topology cut so receipt-first replay cannot retarget the occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamResumeRequestPayload {
    pub(crate) protocol_version: u32,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) resume_id: String,
    pub(crate) expected_lifecycle_revision: u64,
    pub(crate) mode: StreamResumeMode,
    pub(crate) actor_id: String,
    pub(crate) public_named_branches: Vec<String>,
}

/// Immutable canonical preimage of one offline physical-rebind occurrence.
///
/// Fresh physical identities are recovery-owned plan data, not caller input.
/// This request instead fixes the old logical lane, its compare revision, the
/// exact disabled-profile cut, and the empty public-branch topology under
/// which recovery may install that plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamRebindRequestPayload {
    pub(crate) protocol_version: u32,
    pub(crate) graph_identity_digest: String,
    pub(crate) identity: TableIdentity,
    pub(crate) stream_incarnation_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) enrollment_id: String,
    pub(crate) rebind_id: String,
    pub(crate) expected_lifecycle_revision: u64,
    pub(crate) expected_profile_revision: u64,
    pub(crate) actor_id: String,
    pub(crate) public_named_branches: Vec<String>,
}

/// Durable restart plan for a revision-fenced drain.  Config-v3 requires the
/// Phase-D `guarded_operation` slot to be explicit JSON null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrainDescriptor {
    pub(crate) drain_id: String,
    pub(crate) operation_expected_revision: u64,
    pub(crate) operation_request_digest: String,
    pub(crate) goal: DrainGoal,
    pub(crate) initiating_actor: String,
    pub(crate) initiated_at: i64,
    pub(crate) expected_binding: StreamPhysicalBinding,
    pub(crate) expected_current_head_witness: CurrentHeadWitness,
    /// Complete immutable canonical request preimage. Claims, folds, and
    /// disable adoption preserve it byte-for-byte.
    pub(crate) operation_request_payload: QuiesceRequestPayload,
    /// Mutable achieved target. A terminal claim raises this map alongside the
    /// selected top-level epoch floor.
    pub(crate) target_epoch_floor_by_shard: BTreeMap<String, u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) guarded_operation: Option<serde_json::Value>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) seal_override: Option<DisableDrainAdoption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StrictBlock {
    pub(crate) block_token: String,
    pub(crate) correction_revision: u64,
    pub(crate) evidence: StrictBlockEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub(crate) enum StrictBlockEvidence {
    DataBlock {
        enrollment_id: String,
        shard_id: String,
        generation: u64,
        generation_path: String,
        shard_manifest_version: u64,
        writer_epoch: u64,
        replay_cursor: u64,
        base_current_head_witness: CurrentHeadWitness,
        validation_contract_version: u32,
        violation_code: String,
        violation_digest: String,
        correction_view_digest: String,
        offending_key_count: u64,
    },
    /// Reserved lifecycle-v3 vocabulary for F3. This slice decodes the exact
    /// outer shape but refuses it until reason-specific proof validation and
    /// correction recovery are active.
    AuthorityBlock {
        failure_phase: String,
        violation_code: String,
        expected_binding: StreamPhysicalBinding,
        expected_base_current_head_witness: CurrentHeadWitness,
        expected_token_authority: serde_json::Value,
        expected_shard_authority: serde_json::Value,
        observed_authority_classification: serde_json::Value,
        #[serde(deserialize_with = "deserialize_present_option")]
        observed_binding: Option<StreamPhysicalBinding>,
        #[serde(deserialize_with = "deserialize_present_option")]
        observed_base_current_head_witness: Option<CurrentHeadWitness>,
        #[serde(deserialize_with = "deserialize_present_option")]
        observed_token_authority: Option<serde_json::Value>,
        #[serde(deserialize_with = "deserialize_present_option")]
        observed_shard_authority: Option<serde_json::Value>,
        exact_proof_refs: Vec<serde_json::Value>,
        #[serde(deserialize_with = "deserialize_present_option")]
        authenticated_generation_cut: Option<StreamGenerationCut>,
        allowed_repair_classes: Vec<String>,
        authority_evidence_digest: String,
    },
}

/// Historical lifecycle-v2 block shape. Recovery-v12 must continue decoding
/// these bytes exactly and must never reinterpret them as lifecycle-v3 tagged
/// authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyStrictBlockV2 {
    pub(crate) block_token: String,
    pub(crate) enrollment_id: String,
    pub(crate) shard_id: String,
    pub(crate) generation: u64,
    pub(crate) generation_path: String,
    pub(crate) shard_manifest_version: u64,
    pub(crate) replay_cursor: u64,
    pub(crate) base_current_head_witness: CurrentHeadWitness,
    pub(crate) validation_contract_version: u32,
    pub(crate) violation_code: String,
    pub(crate) violation_digest: String,
    pub(crate) correction_view_digest: String,
    pub(crate) offending_key_count: u64,
    pub(crate) correction_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedProof {
    pub(crate) drain_id: String,
    pub(crate) binding_scope_id: String,
    pub(crate) shard_manifest_version: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) replay_cursor: u64,
    pub(crate) current_generation: u64,
    pub(crate) base_merged_generation: u64,
    pub(crate) base_current_head_witness: CurrentHeadWitness,
    pub(crate) current_claim_receipt_id: String,
    pub(crate) claim_receipt_chain: ReceiptChainRef,
    pub(crate) authenticated_tail_position: u64,
    pub(crate) authenticated_tail_segment_count: u64,
    pub(crate) authenticated_tail_chain_digest: String,
    pub(crate) current_sentinel_position: u64,
    pub(crate) current_sentinel_digest: String,
    pub(crate) verified_empty_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamGenerationCut {
    pub(crate) shard_id: String,
    pub(crate) writer_epoch: u64,
    pub(crate) shard_manifest_version: u64,
    pub(crate) replay_after_wal_entry_position: u64,
    pub(crate) generation: u64,
    pub(crate) generation_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum LastFoldOutcome {
    Published,
    StrictBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEmptyCutDisposition {
    DirectClaim,
    PublishedFoldPrefix,
    PublishedDrainFold,
}

impl LastFoldOutcome {
    /// The exact durable wire name, so a status projection reports the stored
    /// value instead of minting a second spelling of it.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "PUBLISHED",
            Self::StrictBlocked => "STRICT_BLOCKED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LastFoldSummary {
    pub(crate) operation_id: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) graph_commit_id: Option<String>,
    pub(crate) exact_generation_cut: StreamGenerationCut,
    pub(crate) outcome: LastFoldOutcome,
    pub(crate) input_rows: u64,
    pub(crate) input_bytes: u64,
    pub(crate) visible_rows: u64,
    pub(crate) visible_bytes: u64,
    pub(crate) recorded_at: i64,
}

/// One materialized `stream_state:<stable>:<incarnation>` authority row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamLifecycleEntry {
    pub(crate) identity: TableIdentity,
    /// Human-readable only. It may lag a metadata-only rename and must never be
    /// used to locate or adopt physical stream state.
    pub(crate) diagnostic_table_key: String,
    pub(crate) lifecycle: StreamLifecycle,
    pub(crate) binding: StreamPhysicalBinding,
    pub(crate) binding_scope_id: String,
    pub(crate) current_head_witness: CurrentHeadWitness,
    /// Epochs are scoped to the binding's never-reused shard IDs.
    pub(crate) epoch_floor_by_shard: BTreeMap<String, u64>,
    /// Monotonic state-row CAS revision. Every successful publication of this
    /// row advances it exactly once, including witness-only publications.
    pub(crate) lifecycle_revision: u64,
    /// Immutable one-per-incarnation legacy provenance. Current binding
    /// authority is the selected v3 binding receipt and chain below.
    pub(crate) enrollment_receipt: EnrollmentReceipt,
    pub(crate) current_binding_receipt_id: String,
    pub(crate) binding_receipt_chain: ReceiptChainRef,
    pub(crate) management_receipt_chain: ReceiptChainRef,
    pub(crate) claim_receipt_chain: ReceiptChainRef,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) current_claim_receipt_id: Option<String>,
    pub(crate) authenticated_wal_tail: AuthenticatedWalTail,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) drain: Option<DrainDescriptor>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) strict_block: Option<StrictBlock>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) sealed_proof: Option<SealedProof>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) last_fold_summary: Option<LastFoldSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamStatePayload {
    protocol_version: u32,
    stable_table_id: u64,
    table_incarnation_id: u64,
    lifecycle: StreamLifecycle,
    binding: StreamPhysicalBinding,
    binding_scope_id: String,
    current_head_witness: CurrentHeadWitness,
    epoch_floor_by_shard: BTreeMap<String, u64>,
    lifecycle_revision: u64,
    enrollment_receipt: EnrollmentReceipt,
    current_binding_receipt_id: String,
    binding_receipt_chain: ReceiptChainRef,
    management_receipt_chain: ReceiptChainRef,
    claim_receipt_chain: ReceiptChainRef,
    #[serde(deserialize_with = "deserialize_present_option")]
    current_claim_receipt_id: Option<String>,
    authenticated_wal_tail: AuthenticatedWalTail,
    #[serde(deserialize_with = "deserialize_present_option")]
    drain: Option<DrainDescriptor>,
    #[serde(deserialize_with = "deserialize_present_option")]
    strict_block: Option<StrictBlock>,
    #[serde(deserialize_with = "deserialize_present_option")]
    sealed_proof: Option<SealedProof>,
    #[serde(deserialize_with = "deserialize_present_option")]
    last_fold_summary: Option<LastFoldSummary>,
}

/// Exact historical lifecycle-v2 recovery payload. Internal schema v12 never
/// decodes this as a current manifest row; recovery-v10/v12 tests use this
/// explicit type to keep their JSON byte-for-byte stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyManagementReceiptV2 {
    pub(crate) operation_id: String,
    pub(crate) operation_kind: String,
    pub(crate) request_digest: String,
    pub(crate) from_revision: u64,
    pub(crate) to_revision: u64,
    pub(crate) actor_id: String,
    pub(crate) result_payload: serde_json::Value,
    pub(crate) result_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyClaimAttemptEffectV2 {
    pub(crate) ordinal: u32,
    pub(crate) attempt_id: String,
    pub(crate) attempt_plan_digest: String,
    pub(crate) bound_prestate_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) storage_envelope_digest: Option<String>,
    pub(crate) planned_sentinel_position: u64,
    pub(crate) planned_sentinel_digest: String,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_shard_manifest_version: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) achieved_writer_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_position: Option<u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) observed_sentinel_digest: Option<String>,
    pub(crate) attempt_terminal_effect_digest: String,
    pub(crate) classification: ClaimAttemptClassification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyClaimReceiptV2 {
    pub(crate) claim_id: String,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) claim_operation_digest: String,
    pub(crate) attempt_count: u32,
    pub(crate) attempt_effect_chain: Vec<LegacyClaimAttemptEffectV2>,
    pub(crate) attempt_effect_chain_digest: String,
    pub(crate) terminal_attempt_id: String,
    pub(crate) terminal_pre_shard_manifest_version: u64,
    pub(crate) achieved_shard_manifest_version: u64,
    pub(crate) achieved_writer_epoch: u64,
    pub(crate) sentinel_position: u64,
    pub(crate) sentinel_digest: String,
    pub(crate) replay_cursor: u64,
    pub(crate) terminal_effect_digest: String,
    pub(crate) terminal_classification: ClaimTerminalClassification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyDrainDescriptorV2 {
    pub(crate) drain_id: String,
    pub(crate) operation_expected_revision: u64,
    pub(crate) operation_request_digest: String,
    pub(crate) goal: DrainGoal,
    pub(crate) initiating_actor: String,
    pub(crate) initiated_at: i64,
    pub(crate) expected_binding: StreamPhysicalBinding,
    pub(crate) expected_current_head_witness: CurrentHeadWitness,
    pub(crate) target_epoch_floor_by_shard: BTreeMap<String, u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) guarded_operation: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyStreamLifecycleEntryV2 {
    pub(crate) identity: TableIdentity,
    pub(crate) diagnostic_table_key: String,
    pub(crate) lifecycle: StreamLifecycle,
    pub(crate) binding: StreamPhysicalBinding,
    pub(crate) current_head_witness: CurrentHeadWitness,
    pub(crate) epoch_floor_by_shard: BTreeMap<String, u64>,
    pub(crate) lifecycle_revision: u64,
    pub(crate) enrollment_receipt: EnrollmentReceipt,
    pub(crate) management_receipts: Vec<LegacyManagementReceiptV2>,
    pub(crate) claim_receipts: Vec<LegacyClaimReceiptV2>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) current_claim_receipt_id: Option<String>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) drain: Option<LegacyDrainDescriptorV2>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) strict_block: Option<LegacyStrictBlockV2>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) sealed_proof: Option<LegacySealedProofV2>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) last_fold_summary: Option<LastFoldSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacySealedProofV2 {
    pub(crate) drain_id: String,
    pub(crate) shard_manifest_version: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) replay_cursor: u64,
    pub(crate) current_generation: u64,
    pub(crate) base_merged_generation: u64,
    pub(crate) base_current_head_witness: CurrentHeadWitness,
    pub(crate) current_claim_receipt_id: String,
    pub(crate) claim_receipt_set_digest: String,
    pub(crate) verified_empty_digest: String,
}

impl LegacyManagementReceiptV2 {
    fn validate(&self, current_revision: u64) -> Result<()> {
        validate_uuid("management operation_id", &self.operation_id)?;
        validate_protocol_label("management operation_kind", &self.operation_kind)?;
        validate_digest("management request_digest", &self.request_digest)?;
        if self.from_revision == 0
            || self.to_revision <= self.from_revision
            || self.to_revision > current_revision
        {
            return Err(OmniError::manifest_internal(format!(
                "stream management receipt revision range {}..{} is invalid at lifecycle revision {current_revision}",
                self.from_revision, self.to_revision
            )));
        }
        validate_canonical_text("management actor_id", &self.actor_id)?;
        if !self.result_payload.is_object() {
            return Err(OmniError::manifest_internal(
                "stream management receipt result_payload must be a JSON object",
            ));
        }
        validate_digest("management result_digest", &self.result_digest)
    }
}

impl LegacyClaimAttemptEffectV2 {
    fn validate(&self, profile: ClaimProfile, expected_ordinal: u32) -> Result<()> {
        if self.ordinal != expected_ordinal {
            return Err(OmniError::manifest_internal(format!(
                "stream claim attempt ordinal {} is not the expected contiguous ordinal {expected_ordinal}",
                self.ordinal
            )));
        }
        let attempt_id = validate_uuid("claim attempt_id", &self.attempt_id)?;
        if attempt_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream claim attempt_id must be a UUID v4 value",
            ));
        }
        validate_digest("claim attempt_plan_digest", &self.attempt_plan_digest)?;
        validate_digest("claim bound_prestate_digest", &self.bound_prestate_digest)?;
        match (profile, self.storage_envelope_digest.as_deref()) {
            (ClaimProfile::RetainAll, None) => {}
            (ClaimProfile::RetainAll, Some(_)) => {
                return Err(OmniError::manifest_internal(
                    "retain-all claim attempts cannot carry a managed-reclamation storage envelope",
                ));
            }
            (ClaimProfile::ManagedReclamation, Some(digest)) => {
                validate_digest("claim storage_envelope_digest", digest)?;
            }
            (ClaimProfile::ManagedReclamation, None) => {
                return Err(OmniError::manifest_internal(
                    "managed-reclamation claim attempts require a storage envelope digest",
                ));
            }
        }
        if self.planned_sentinel_position == 0 {
            return Err(OmniError::manifest_internal(
                "stream claim planned sentinel position must be non-zero",
            ));
        }
        validate_digest(
            "claim planned_sentinel_digest",
            &self.planned_sentinel_digest,
        )?;
        validate_digest(
            "claim attempt_terminal_effect_digest",
            &self.attempt_terminal_effect_digest,
        )?;

        let achieved_manifest = match (
            self.achieved_shard_manifest_version,
            self.achieved_writer_epoch,
        ) {
            (Some(version), Some(epoch)) if version > 0 && epoch > 0 => true,
            (None, None) => false,
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream claim attempt must carry achieved manifest version and writer epoch together and non-zero",
                ));
            }
        };
        let observed_sentinel = match (
            self.observed_sentinel_position,
            self.observed_sentinel_digest.as_deref(),
        ) {
            (Some(position), Some(digest)) if position > 0 => {
                validate_digest("claim observed_sentinel_digest", digest)?;
                if position != self.planned_sentinel_position
                    || digest != self.planned_sentinel_digest
                {
                    return Err(OmniError::manifest_internal(
                        "stream claim observed sentinel differs from its pre-armed plan",
                    ));
                }
                true
            }
            (None, None) => false,
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream claim attempt must carry observed sentinel position and digest together",
                ));
            }
        };
        let expected_effects = match self.classification {
            ClaimAttemptClassification::NoEffect | ClaimAttemptClassification::AbortedNoEffect => {
                (false, false)
            }
            ClaimAttemptClassification::StockManifestOnly => (true, false),
            ClaimAttemptClassification::StockManifestPlusSentinel
            | ClaimAttemptClassification::PatchedSentinelPlusNamingManifest => (true, true),
            ClaimAttemptClassification::PatchedSentinelOnly => (false, true),
        };
        if (achieved_manifest, observed_sentinel) != expected_effects {
            return Err(OmniError::manifest_internal(format!(
                "stream claim attempt effect fields disagree with classification {:?}",
                self.classification
            )));
        }
        Ok(())
    }
}

impl LegacyClaimReceiptV2 {
    fn validate(&self) -> Result<()> {
        let claim_id = validate_uuid("claim_id", &self.claim_id)?;
        if claim_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream claim_id must be a UUID v4 value",
            ));
        }
        validate_canonical_text("claim recovery_operation_id", &self.recovery_operation_id)?;
        validate_protocol_label("claim_kind", &self.claim_kind)?;
        validate_digest("claim_operation_digest", &self.claim_operation_digest)?;
        validate_digest(
            "claim attempt_effect_chain_digest",
            &self.attempt_effect_chain_digest,
        )?;
        validate_digest("claim sentinel_digest", &self.sentinel_digest)?;
        validate_digest("claim terminal_effect_digest", &self.terminal_effect_digest)?;
        if self.attempt_count == 0
            || usize::try_from(self.attempt_count).ok() != Some(self.attempt_effect_chain.len())
        {
            return Err(OmniError::manifest_internal(
                "stream claim attempt_count must exactly match a non-empty attempt_effect_chain",
            ));
        }
        let mut attempt_ids = std::collections::BTreeSet::new();
        for (index, attempt) in self.attempt_effect_chain.iter().enumerate() {
            let ordinal = u32::try_from(index + 1).map_err(|_| {
                OmniError::manifest_internal("stream claim attempt ordinal overflow")
            })?;
            attempt.validate(self.profile, ordinal)?;
            if !attempt_ids.insert(attempt.attempt_id.as_str()) {
                return Err(OmniError::manifest_internal(
                    "stream claim attempt IDs must be unique within a receipt",
                ));
            }
        }
        let terminal = self
            .attempt_effect_chain
            .last()
            .expect("non-empty chain checked above");
        let expected_terminal_classification = match self.terminal_classification {
            ClaimTerminalClassification::StockManifestPlusSentinel => {
                ClaimAttemptClassification::StockManifestPlusSentinel
            }
            ClaimTerminalClassification::PatchedSentinelPlusNamingManifest => {
                ClaimAttemptClassification::PatchedSentinelPlusNamingManifest
            }
        };
        if terminal.attempt_id != self.terminal_attempt_id
            || terminal.classification != expected_terminal_classification
            || terminal.achieved_shard_manifest_version
                != Some(self.achieved_shard_manifest_version)
            || terminal.achieved_writer_epoch != Some(self.achieved_writer_epoch)
            || terminal.observed_sentinel_position != Some(self.sentinel_position)
            || terminal.observed_sentinel_digest.as_deref() != Some(self.sentinel_digest.as_str())
            || terminal.attempt_terminal_effect_digest != self.terminal_effect_digest
        {
            return Err(OmniError::manifest_internal(
                "stream claim terminal receipt fields do not match the final classified attempt",
            ));
        }
        if self.terminal_pre_shard_manifest_version == 0
            || self.achieved_shard_manifest_version <= self.terminal_pre_shard_manifest_version
            || self.achieved_writer_epoch == 0
            || self.sentinel_position == 0
            || self.replay_cursor > self.sentinel_position
        {
            return Err(OmniError::manifest_internal(
                "stream claim terminal manifest, epoch, sentinel, or replay cursor is invalid",
            ));
        }
        Ok(())
    }
}

impl LegacyDrainDescriptorV2 {
    fn validate(&self, entry: &LegacyStreamLifecycleEntryV2) -> Result<()> {
        validate_uuid("drain_id", &self.drain_id)?;
        if self.operation_expected_revision == 0
            || self.operation_expected_revision >= entry.lifecycle_revision
        {
            return Err(OmniError::manifest_internal(
                "stream drain operation_expected_revision must precede the current lifecycle revision",
            ));
        }
        validate_digest(
            "drain operation_request_digest",
            &self.operation_request_digest,
        )?;
        validate_canonical_text("drain initiating_actor", &self.initiating_actor)?;
        if self.initiated_at <= 0 {
            return Err(OmniError::manifest_internal(
                "stream drain initiated_at must be a positive timestamp",
            ));
        }
        self.expected_binding.validate(entry.identity)?;
        self.expected_current_head_witness.validate()?;
        validate_epoch_floors(
            &self.expected_binding,
            &self.target_epoch_floor_by_shard,
            "drain target",
        )?;
        for (shard_id, target_epoch) in &self.target_epoch_floor_by_shard {
            let current_epoch = entry.epoch_floor_by_shard.get(shard_id).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream drain target names shard {shard_id}, which is absent from the current shard binding"
                ))
            })?;
            if target_epoch < current_epoch {
                return Err(OmniError::manifest_internal(
                    "stream drain target epoch cannot move behind current shard authority",
                ));
            }
        }
        if self.guarded_operation.is_some() {
            return Err(OmniError::manifest_internal(
                "stream config-v3 drain guarded_operation must be null",
            ));
        }
        Ok(())
    }
}

impl LegacySealedProofV2 {
    fn validate(&self, entry: &LegacyStreamLifecycleEntryV2) -> Result<()> {
        validate_uuid("sealed proof drain_id", &self.drain_id)?;
        validate_uuid(
            "sealed proof current_claim_receipt_id",
            &self.current_claim_receipt_id,
        )?;
        if self.shard_manifest_version == 0
            || self.writer_epoch == 0
            || self.current_generation < self.base_merged_generation
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof carries an invalid manifest, epoch, or generation cut",
            ));
        }
        self.base_current_head_witness.validate()?;
        if self.base_current_head_witness != entry.current_head_witness
            || entry.current_claim_receipt_id.as_deref()
                != Some(self.current_claim_receipt_id.as_str())
            || entry
                .epoch_floor_by_shard
                .values()
                .any(|epoch| *epoch != self.writer_epoch)
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof does not match the current table or claim authority",
            ));
        }
        let current_claim = entry
            .claim_receipts
            .iter()
            .find(|receipt| receipt.claim_id == self.current_claim_receipt_id)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream sealed proof current claim receipt is not retained",
                )
            })?;
        if current_claim.achieved_shard_manifest_version != self.shard_manifest_version
            || current_claim.achieved_writer_epoch != self.writer_epoch
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof shard authority differs from its current claim receipt",
            ));
        }
        validate_digest(
            "sealed proof claim_receipt_set_digest",
            &self.claim_receipt_set_digest,
        )?;
        validate_digest(
            "sealed proof verified_empty_digest",
            &self.verified_empty_digest,
        )
    }
}

impl LegacyStreamLifecycleEntryV2 {
    /// Validate an exact historical lifecycle-v2 value embedded in recovery
    /// records. Internal schema v12 never upgrades this value in place: the
    /// validator deliberately preserves the v2 history-vector and proof
    /// semantics byte-for-byte so old recovery can only be refused or settled
    /// under its original contract.
    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.diagnostic_table_key.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream lifecycle diagnostic table key must be non-empty",
            ));
        }
        self.binding.validate(self.identity)?;
        self.current_head_witness.validate()?;
        validate_epoch_floors(&self.binding, &self.epoch_floor_by_shard, "current")?;
        if self.lifecycle_revision < INITIAL_LIFECYCLE_REVISION {
            return Err(OmniError::manifest_internal(
                "stream lifecycle_revision must be non-zero",
            ));
        }
        self.enrollment_receipt.validate()?;
        if self.enrollment_receipt.physical_binding.identity()? != self.identity {
            return Err(OmniError::manifest_internal(
                "stream enrollment receipt binding identity differs from its lifecycle row",
            ));
        }
        if self.enrollment_receipt.initial_lifecycle_revision > self.lifecycle_revision {
            return Err(OmniError::manifest_internal(
                "stream enrollment receipt begins after the current lifecycle revision",
            ));
        }

        let mut management_occurrences = std::collections::BTreeSet::new();
        let mut prior_management_terminal_revision = 0;
        for receipt in &self.management_receipts {
            receipt.validate(self.lifecycle_revision)?;
            if !management_occurrences.insert((
                receipt.operation_kind.as_str(),
                receipt.operation_id.as_str(),
            )) {
                return Err(OmniError::manifest_internal(
                    "stream management receipt occurrences must be unique",
                ));
            }
            if receipt.to_revision <= prior_management_terminal_revision {
                return Err(OmniError::manifest_internal(
                    "stream management receipt history must be ordered by increasing terminal revision",
                ));
            }
            prior_management_terminal_revision = receipt.to_revision;
        }

        let mut claim_ids = std::collections::BTreeSet::new();
        let mut greatest_claim_epoch = None;
        for receipt in &self.claim_receipts {
            receipt.validate()?;
            if !claim_ids.insert(receipt.claim_id.as_str()) {
                return Err(OmniError::manifest_internal(
                    "stream claim receipt IDs must be unique",
                ));
            }
            if greatest_claim_epoch
                .is_some_and(|prior_epoch| prior_epoch >= receipt.achieved_writer_epoch)
            {
                return Err(OmniError::manifest_internal(
                    "stream claim receipt history must be ordered by strictly increasing writer epoch",
                ));
            }
            greatest_claim_epoch = Some(receipt.achieved_writer_epoch);
        }
        match (
            self.current_claim_receipt_id.as_deref(),
            greatest_claim_epoch,
        ) {
            (None, None) => {}
            (Some(current_id), Some(greatest_epoch)) => {
                validate_uuid("current_claim_receipt_id", current_id)?;
                let current = self
                    .claim_receipts
                    .iter()
                    .find(|receipt| receipt.claim_id == current_id)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "stream current_claim_receipt_id does not name retained claim history",
                        )
                    })?;
                if current.achieved_writer_epoch != greatest_epoch {
                    return Err(OmniError::manifest_internal(
                        "stream current claim receipt does not carry the greatest achieved epoch",
                    ));
                }
                if self
                    .epoch_floor_by_shard
                    .values()
                    .any(|epoch| *epoch != current.achieved_writer_epoch)
                {
                    return Err(OmniError::manifest_internal(
                        "stream current claim receipt epoch differs from the current shard epoch floor",
                    ));
                }
            }
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream claim history and current_claim_receipt_id must be absent or present together",
                ));
            }
        }

        match self.lifecycle {
            StreamLifecycle::Open => {
                if self.drain.is_some()
                    || self.strict_block.is_some()
                    || self.sealed_proof.is_some()
                {
                    return Err(OmniError::manifest_internal(
                        "OPEN stream lifecycle cannot carry drain, strict-block, or sealed-proof state",
                    ));
                }
            }
            StreamLifecycle::Draining => {
                let drain = self.drain.as_ref().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "DRAINING stream lifecycle requires one drain descriptor",
                    )
                })?;
                drain.validate(self)?;
                if drain.expected_binding != self.binding
                    || drain.expected_current_head_witness != self.current_head_witness
                    || self.sealed_proof.is_some()
                {
                    return Err(OmniError::manifest_internal(
                        "DRAINING stream authority disagrees with its current binding/HEAD or carries a sealed proof",
                    ));
                }
                if let Some(block) = &self.strict_block {
                    validate_legacy_strict_block(block, self)?;
                    if block.base_current_head_witness != self.current_head_witness {
                        return Err(OmniError::manifest_internal(
                            "stream strict block base witness differs from current DRAINING authority",
                        ));
                    }
                }
            }
            StreamLifecycle::Sealed => {
                if self.drain.is_some() || self.strict_block.is_some() {
                    return Err(OmniError::manifest_internal(
                        "SEALED stream lifecycle cannot retain drain or strict-block state",
                    ));
                }
                self.sealed_proof
                    .as_ref()
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "SEALED stream lifecycle requires one exact empty proof",
                        )
                    })?
                    .validate(self)?;
            }
        }
        if let Some(summary) = &self.last_fold_summary {
            validate_legacy_last_fold_summary(summary, self)?;
        }
        Ok(())
    }
}

fn validate_legacy_strict_block(
    block: &LegacyStrictBlockV2,
    entry: &LegacyStreamLifecycleEntryV2,
) -> Result<()> {
    validate_digest("strict block_token", &block.block_token)?;
    if block.enrollment_id != entry.binding.enrollment_id {
        return Err(OmniError::manifest_internal(
            "stream strict block enrollment_id differs from the current binding",
        ));
    }
    validate_uuid("strict block enrollment_id", &block.enrollment_id)?;
    let shard_id = validate_uuid("strict block shard_id", &block.shard_id)?;
    if !entry
        .binding
        .shard_ids
        .iter()
        .any(|bound| bound == &shard_id.to_string())
    {
        return Err(OmniError::manifest_internal(
            "stream strict block shard_id is not present in the current binding",
        ));
    }
    validate_canonical_text("strict block generation_path", &block.generation_path)?;
    block.base_current_head_witness.validate()?;
    if block.generation == 0
        || block.shard_manifest_version == 0
        || block.validation_contract_version == 0
        || block.correction_revision > entry.lifecycle_revision
    {
        return Err(OmniError::manifest_internal(
            "stream strict block carries an invalid generation, contract, or correction revision",
        ));
    }
    validate_protocol_label("strict block violation_code", &block.violation_code)?;
    validate_digest("strict block violation_digest", &block.violation_digest)?;
    validate_digest(
        "strict block correction_view_digest",
        &block.correction_view_digest,
    )
}

fn validate_legacy_last_fold_summary(
    summary: &LastFoldSummary,
    entry: &LegacyStreamLifecycleEntryV2,
) -> Result<()> {
    validate_canonical_text("last fold operation_id", &summary.operation_id)?;
    summary.exact_generation_cut.validate()?;
    if !entry
        .binding
        .shard_ids
        .contains(&summary.exact_generation_cut.shard_id)
    {
        return Err(OmniError::manifest_internal(
            "stream last-fold cut shard is not present in the current binding",
        ));
    }
    if summary.visible_rows > summary.input_rows || summary.recorded_at <= 0 {
        return Err(OmniError::manifest_internal(
            "stream last-fold row counts or timestamp are invalid",
        ));
    }
    match (summary.outcome, summary.graph_commit_id.as_deref()) {
        (LastFoldOutcome::Published, Some(commit_id)) => {
            validate_canonical_text("last fold graph_commit_id", commit_id)
        }
        (LastFoldOutcome::StrictBlocked, None)
            if summary.visible_rows == 0 && summary.visible_bytes == 0 =>
        {
            Ok(())
        }
        (LastFoldOutcome::Published, None) => Err(OmniError::manifest_internal(
            "a published stream fold summary requires graph_commit_id",
        )),
        (LastFoldOutcome::StrictBlocked, _) => Err(OmniError::manifest_internal(
            "a strict-blocked stream fold must have no graph commit and zero visible output",
        )),
    }
}

impl EnrollmentReceiptV2 {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph_identity_digest: impl Into<String>,
        identity: TableIdentity,
        prior_chain: &ReceiptChainRef,
        enrollment_request_id: impl Into<String>,
        enrollment_intent_digest: impl Into<String>,
        actor_id: impl Into<String>,
        stream_incarnation_id: impl Into<String>,
        binding_scope_id: impl Into<String>,
        physical_binding: StreamPhysicalBinding,
        recorded_at: i64,
    ) -> Result<Self> {
        prior_chain.validate_with_domain(BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        let graph_identity_digest = graph_identity_digest.into();
        let enrollment_request_id = enrollment_request_id.into();
        let mut value = Self {
            protocol_version: ENROLLMENT_RECEIPT_V2_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &graph_identity_digest,
                identity,
                &enrollment_request_id,
            )?,
            record_tag: ENROLLMENT_RECEIPT_V2_TAG.to_string(),
            graph_identity_digest,
            identity,
            chain_ordinal: next_chain_ordinal(prior_chain, "binding receipt")?,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            enrollment_request_id,
            enrollment_intent_digest: enrollment_intent_digest.into(),
            actor_id: actor_id.into(),
            stream_incarnation_id: stream_incarnation_id.into(),
            binding_scope_id: binding_scope_id.into(),
            physical_binding,
            initial_lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            recorded_at,
        };
        value.record_id = value.compute_record_id()?;
        value.resulting_chain_digest = receipt_chain_step_digest(
            &value.record_tag,
            &value.prior_chain_digest,
            value.chain_ordinal,
            &value.record_id,
        );
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        enrollment_request_id: &str,
    ) -> Result<String> {
        validate_digest("enrollment-v2 graph_identity_digest", graph_identity_digest)?;
        identity.validate()?;
        validate_uuid("enrollment-v2 enrollment_request_id", enrollment_request_id)?;
        Ok(format!(
            "stream-enrollment-v2:{}",
            hash_fields(
                ENROLLMENT_RECEIPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    enrollment_request_id.as_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != ENROLLMENT_RECEIPT_V2_PROTOCOL_VERSION
            || self.record_tag != ENROLLMENT_RECEIPT_V2_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream enrollment-v2 receipt has an unsupported protocol or tag",
            ));
        }
        validate_digest(
            "enrollment-v2 graph_identity_digest",
            &self.graph_identity_digest,
        )?;
        self.identity.validate()?;
        validate_uuid(
            "enrollment-v2 enrollment_request_id",
            &self.enrollment_request_id,
        )?;
        validate_digest(
            "enrollment-v2 enrollment_intent_digest",
            &self.enrollment_intent_digest,
        )?;
        validate_canonical_text("enrollment-v2 actor_id", &self.actor_id)?;
        validate_uuid(
            "enrollment-v2 stream_incarnation_id",
            &self.stream_incarnation_id,
        )?;
        validate_uuid("enrollment-v2 binding_scope_id", &self.binding_scope_id)?;
        self.physical_binding.validate(self.identity)?;
        if self.initial_lifecycle_revision != INITIAL_LIFECYCLE_REVISION || self.recorded_at <= 0 {
            return Err(OmniError::manifest_internal(
                "stream enrollment-v2 receipt has an invalid initial revision or timestamp",
            ));
        }
        validate_receipt_envelope(
            &self.record_id,
            &self.record_lookup_key,
            &self.record_tag,
            self.chain_ordinal,
            self.predecessor_record_id.as_deref(),
            &self.prior_chain_digest,
            &self.resulting_chain_digest,
            BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN,
            &Self::lookup_key_for(
                &self.graph_identity_digest,
                self.identity,
                &self.enrollment_request_id,
            )?,
            &self.compute_record_id()?,
        )
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        receipt_next_chain_ref(
            self.chain_ordinal,
            &self.record_id,
            &self.resulting_chain_digest,
        )
    }

    fn compute_record_id(&self) -> Result<String> {
        let binding = bounded_json_bytes("enrollment-v2 physical binding", &self.physical_binding)?;
        Ok(hash_fields(
            ENROLLMENT_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.identity.stable_table_id.to_be_bytes(),
                &self.identity.table_incarnation_id.to_be_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.predecessor_record_id
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.prior_chain_digest.as_bytes(),
                self.enrollment_request_id.as_bytes(),
                self.enrollment_intent_digest.as_bytes(),
                self.actor_id.as_bytes(),
                self.stream_incarnation_id.as_bytes(),
                self.binding_scope_id.as_bytes(),
                &binding,
                &self.initial_lifecycle_revision.to_be_bytes(),
                &self.recorded_at.to_be_bytes(),
            ],
        ))
    }
}

impl BindingReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph_identity_digest: impl Into<String>,
        identity: TableIdentity,
        prior_chain: &ReceiptChainRef,
        binding_scope_id: impl Into<String>,
        stream_incarnation_id: impl Into<String>,
        physical_binding: StreamPhysicalBinding,
        operation_id: impl Into<String>,
        recorded_at: i64,
    ) -> Result<Self> {
        Self::new_with_inventory(
            graph_identity_digest,
            identity,
            prior_chain,
            binding_scope_id,
            stream_incarnation_id,
            physical_binding,
            None,
            operation_id,
            recorded_at,
        )
    }

    /// Construct a post-rebind receipt that binds the complete terminal shard
    /// namespace. The caller supplies authority-derived shards, never ambient
    /// object-store discovery.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_retained_shards(
        graph_identity_digest: impl Into<String>,
        identity: TableIdentity,
        prior_chain: &ReceiptChainRef,
        binding_scope_id: impl Into<String>,
        stream_incarnation_id: impl Into<String>,
        physical_binding: StreamPhysicalBinding,
        retained_shards: &[ShardId],
        operation_id: impl Into<String>,
        recorded_at: i64,
    ) -> Result<Self> {
        Self::new_with_inventory(
            graph_identity_digest,
            identity,
            prior_chain,
            binding_scope_id,
            stream_incarnation_id,
            physical_binding,
            Some(retained_shard_inventory_commitment(retained_shards)?),
            operation_id,
            recorded_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_inventory(
        graph_identity_digest: impl Into<String>,
        identity: TableIdentity,
        prior_chain: &ReceiptChainRef,
        binding_scope_id: impl Into<String>,
        stream_incarnation_id: impl Into<String>,
        physical_binding: StreamPhysicalBinding,
        retained_inventory: Option<RetainedShardInventoryCommitment>,
        operation_id: impl Into<String>,
        recorded_at: i64,
    ) -> Result<Self> {
        prior_chain.validate_with_domain(BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        let graph_identity_digest = graph_identity_digest.into();
        let binding_scope_id = binding_scope_id.into();
        let operation_id = operation_id.into();
        let mut value = Self {
            protocol_version: BINDING_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &graph_identity_digest,
                identity,
                &binding_scope_id,
                &operation_id,
            )?,
            record_tag: BINDING_RECEIPT_TAG.to_string(),
            graph_identity_digest,
            identity,
            chain_ordinal: next_chain_ordinal(prior_chain, "binding receipt")?,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            binding_scope_id,
            stream_incarnation_id: stream_incarnation_id.into(),
            enrollment_id: physical_binding.enrollment_id.clone(),
            shard_ids: physical_binding.shard_ids.clone(),
            physical_binding,
            retained_shard_count: retained_inventory
                .as_ref()
                .map(|inventory| inventory.retained_shard_count),
            retained_shard_set_digest: retained_inventory
                .map(|inventory| inventory.retained_shard_set_digest),
            operation_id,
            receipt_digest: String::new(),
            recorded_at,
        };
        value.receipt_digest = value.compute_receipt_digest()?;
        value.record_id = value.compute_record_id();
        value.resulting_chain_digest = receipt_chain_step_digest(
            &value.record_tag,
            &value.prior_chain_digest,
            value.chain_ordinal,
            &value.record_id,
        );
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        binding_scope_id: &str,
        operation_id: &str,
    ) -> Result<String> {
        validate_digest(
            "binding receipt graph_identity_digest",
            graph_identity_digest,
        )?;
        identity.validate()?;
        validate_uuid("binding receipt binding_scope_id", binding_scope_id)?;
        validate_canonical_text("binding receipt operation_id", operation_id)?;
        Ok(format!(
            "stream-binding-v1:{}",
            hash_fields(
                BINDING_RECEIPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    binding_scope_id.as_bytes(),
                    operation_id.as_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != BINDING_RECEIPT_PROTOCOL_VERSION
            || self.record_tag != BINDING_RECEIPT_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream binding receipt has an unsupported protocol or tag",
            ));
        }
        validate_digest(
            "binding receipt graph_identity_digest",
            &self.graph_identity_digest,
        )?;
        self.identity.validate()?;
        validate_uuid("binding receipt binding_scope_id", &self.binding_scope_id)?;
        validate_uuid(
            "binding receipt stream_incarnation_id",
            &self.stream_incarnation_id,
        )?;
        validate_uuid("binding receipt enrollment_id", &self.enrollment_id)?;
        validate_canonical_text("binding receipt operation_id", &self.operation_id)?;
        self.physical_binding.validate(self.identity)?;
        if self.enrollment_id != self.physical_binding.enrollment_id
            || self.shard_ids != self.physical_binding.shard_ids
            || self.recorded_at <= 0
        {
            return Err(OmniError::manifest_internal(
                "stream binding receipt differs from its physical binding or timestamp",
            ));
        }
        self.validate_retained_shard_inventory_context()?;
        validate_digest("binding receipt receipt_digest", &self.receipt_digest)?;
        if self.receipt_digest != self.compute_receipt_digest()? {
            return Err(OmniError::manifest_internal(
                "stream binding receipt differs from its canonical result digest",
            ));
        }
        validate_receipt_envelope(
            &self.record_id,
            &self.record_lookup_key,
            &self.record_tag,
            self.chain_ordinal,
            self.predecessor_record_id.as_deref(),
            &self.prior_chain_digest,
            &self.resulting_chain_digest,
            BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN,
            &Self::lookup_key_for(
                &self.graph_identity_digest,
                self.identity,
                &self.binding_scope_id,
                &self.operation_id,
            )?,
            &self.compute_record_id(),
        )
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        receipt_next_chain_ref(
            self.chain_ordinal,
            &self.record_id,
            &self.resulting_chain_digest,
        )
    }

    pub(crate) fn retained_shard_inventory_commitment(
        &self,
    ) -> Result<Option<RetainedShardInventoryCommitment>> {
        self.validate_retained_shard_inventory_context()?;
        Ok(self
            .retained_shard_count
            .zip(self.retained_shard_set_digest.clone())
            .map(
                |(retained_shard_count, retained_shard_set_digest)| {
                    RetainedShardInventoryCommitment {
                        retained_shard_count,
                        retained_shard_set_digest,
                    }
                },
            ))
    }

    fn validate_retained_shard_inventory_context(&self) -> Result<()> {
        match (
            self.chain_ordinal,
            self.retained_shard_count,
            self.retained_shard_set_digest.as_deref(),
        ) {
            (2, None, None) => Ok(()),
            (ordinal, Some(count), Some(digest)) if ordinal > 2 => {
                let expected = self.chain_ordinal.checked_sub(1).ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream binding receipt retained-shard count underflow",
                    )
                })?;
                if count != expected {
                    return Err(OmniError::manifest_internal(format!(
                        "stream binding receipt retained-shard count {count} differs from chain-derived count {expected}"
                    )));
                }
                validate_digest("binding receipt retained_shard_set_digest", digest)
            }
            _ => Err(OmniError::manifest_internal(
                "stream binding receipt must be ordinal 2 without a retained-shard commitment or ordinal >2 with a complete commitment",
            )),
        }
    }

    fn compute_receipt_digest(&self) -> Result<String> {
        let binding =
            bounded_json_bytes("binding receipt physical binding", &self.physical_binding)?;
        let base_fields = [
            self.graph_identity_digest.as_bytes(),
            &self.identity.stable_table_id.to_be_bytes(),
            &self.identity.table_incarnation_id.to_be_bytes(),
            self.binding_scope_id.as_bytes(),
            self.stream_incarnation_id.as_bytes(),
            self.enrollment_id.as_bytes(),
            &binding,
            self.operation_id.as_bytes(),
        ];
        let (Some(count), Some(digest)) = (
            self.retained_shard_count,
            self.retained_shard_set_digest.as_deref(),
        ) else {
            return Ok(hash_fields(BINDING_RECEIPT_DIGEST_DOMAIN, &base_fields));
        };
        let count_bytes = count.to_be_bytes();
        let mut fields = base_fields.to_vec();
        fields.push(&count_bytes);
        fields.push(digest.as_bytes());
        Ok(hash_fields(BINDING_RECEIPT_DIGEST_DOMAIN, &fields))
    }

    fn compute_record_id(&self) -> String {
        hash_fields(
            BINDING_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.identity.stable_table_id.to_be_bytes(),
                &self.identity.table_incarnation_id.to_be_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.predecessor_record_id
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.prior_chain_digest.as_bytes(),
                self.binding_scope_id.as_bytes(),
                self.operation_id.as_bytes(),
                self.receipt_digest.as_bytes(),
                &self.recorded_at.to_be_bytes(),
            ],
        )
    }
}

impl ManagementReceipt {
    /// Canonical request commitment shared by the in-progress lifecycle row
    /// and its eventual immutable management receipt.
    pub(crate) fn request_digest_for(request_payload: &serde_json::Value) -> Result<String> {
        canonical_json_digest(
            MANAGEMENT_REQUEST_DOMAIN,
            "management request",
            request_payload,
        )
    }

    /// Canonical result commitment used when a terminal lifecycle receipt is
    /// prepared before its ledger transaction is armed.
    pub(crate) fn result_digest_for(result_payload: &serde_json::Value) -> Result<String> {
        canonical_json_digest(
            MANAGEMENT_RESULT_DOMAIN,
            "management result",
            result_payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph_identity_digest: impl Into<String>,
        identity: TableIdentity,
        stream_incarnation_id: impl Into<String>,
        binding_scope_id: impl Into<String>,
        prior_chain: &ReceiptChainRef,
        operation_id: impl Into<String>,
        operation_kind: impl Into<String>,
        from_revision: u64,
        to_revision: u64,
        actor_id: impl Into<String>,
        request_payload: serde_json::Value,
        result_payload: serde_json::Value,
        recorded_at: i64,
    ) -> Result<Self> {
        prior_chain.validate_with_domain(MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        let graph_identity_digest = graph_identity_digest.into();
        let stream_incarnation_id = stream_incarnation_id.into();
        let binding_scope_id = binding_scope_id.into();
        let operation_id = operation_id.into();
        let operation_kind = operation_kind.into();
        let mut value = Self {
            protocol_version: MANAGEMENT_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &graph_identity_digest,
                identity,
                &stream_incarnation_id,
                &operation_kind,
                &operation_id,
            )?,
            record_tag: MANAGEMENT_RECEIPT_TAG.to_string(),
            graph_identity_digest,
            identity,
            stream_incarnation_id,
            binding_scope_id,
            chain_ordinal: next_chain_ordinal(prior_chain, "management receipt")?,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            operation_id,
            operation_kind,
            request_payload,
            request_digest: String::new(),
            from_revision,
            to_revision,
            actor_id: actor_id.into(),
            result_payload,
            result_digest: String::new(),
            recorded_at,
        };
        value.request_digest = Self::request_digest_for(&value.request_payload)?;
        value.result_digest = Self::result_digest_for(&value.result_payload)?;
        value.record_id = value.compute_record_id();
        value.resulting_chain_digest = receipt_chain_step_digest(
            &value.record_tag,
            &value.prior_chain_digest,
            value.chain_ordinal,
            &value.record_id,
        );
        value.validate(to_revision)?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        stream_incarnation_id: &str,
        operation_kind: &str,
        operation_id: &str,
    ) -> Result<String> {
        validate_digest("management graph_identity_digest", graph_identity_digest)?;
        identity.validate()?;
        validate_uuid("management stream_incarnation_id", stream_incarnation_id)?;
        validate_protocol_label("management operation_kind", operation_kind)?;
        validate_uuid("management operation_id", operation_id)?;
        Ok(format!(
            "stream-management-v1:{}",
            hash_fields(
                MANAGEMENT_RECEIPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    stream_incarnation_id.as_bytes(),
                    operation_kind.as_bytes(),
                    operation_id.as_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self, current_revision: u64) -> Result<()> {
        if self.protocol_version != MANAGEMENT_RECEIPT_PROTOCOL_VERSION
            || self.record_tag != MANAGEMENT_RECEIPT_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream management receipt has an unsupported protocol or tag",
            ));
        }
        validate_digest(
            "management graph_identity_digest",
            &self.graph_identity_digest,
        )?;
        self.identity.validate()?;
        validate_uuid(
            "management stream_incarnation_id",
            &self.stream_incarnation_id,
        )?;
        validate_uuid("management binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("management operation_id", &self.operation_id)?;
        validate_protocol_label("management operation_kind", &self.operation_kind)?;
        if self.from_revision == 0
            || self.to_revision <= self.from_revision
            || self.to_revision > current_revision
            || self.recorded_at <= 0
        {
            return Err(OmniError::manifest_internal(format!(
                "stream management receipt revision range {}..{} or timestamp is invalid at lifecycle revision {current_revision}",
                self.from_revision, self.to_revision
            )));
        }
        validate_canonical_text("management actor_id", &self.actor_id)?;
        let request_digest = Self::request_digest_for(&self.request_payload)?;
        let result_digest = Self::result_digest_for(&self.result_payload)?;
        if self.request_digest != request_digest || self.result_digest != result_digest {
            return Err(OmniError::manifest_internal(
                "stream management receipt request/result digest differs from its canonical preimage",
            ));
        }
        validate_receipt_envelope(
            &self.record_id,
            &self.record_lookup_key,
            &self.record_tag,
            self.chain_ordinal,
            self.predecessor_record_id.as_deref(),
            &self.prior_chain_digest,
            &self.resulting_chain_digest,
            MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN,
            &Self::lookup_key_for(
                &self.graph_identity_digest,
                self.identity,
                &self.stream_incarnation_id,
                &self.operation_kind,
                &self.operation_id,
            )?,
            &self.compute_record_id(),
        )
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate(self.to_revision)?;
        receipt_next_chain_ref(
            self.chain_ordinal,
            &self.record_id,
            &self.resulting_chain_digest,
        )
    }

    fn compute_record_id(&self) -> String {
        hash_fields(
            MANAGEMENT_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.identity.stable_table_id.to_be_bytes(),
                &self.identity.table_incarnation_id.to_be_bytes(),
                self.stream_incarnation_id.as_bytes(),
                self.binding_scope_id.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.predecessor_record_id
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.prior_chain_digest.as_bytes(),
                self.operation_id.as_bytes(),
                self.operation_kind.as_bytes(),
                self.request_digest.as_bytes(),
                &self.from_revision.to_be_bytes(),
                &self.to_revision.to_be_bytes(),
                self.actor_id.as_bytes(),
                self.result_digest.as_bytes(),
                &self.recorded_at.to_be_bytes(),
            ],
        )
    }
}

impl StreamCorrectionReceipt {
    pub(crate) fn new(preimage: StreamCorrectionReceiptPreimage) -> Result<Self> {
        let result_payload = stream_correction_result_payload(
            &preimage.correction_id,
            &preimage.correction_plan_digest,
            &preimage.graph_commit_id,
            preimage.resulting_manifest_version,
            preimage.resulting_lifecycle_revision,
        )?;
        let mut value = Self {
            protocol_version: STREAM_CORRECTION_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &preimage.graph_identity_digest,
                preimage.identity,
                &preimage.stream_incarnation_id,
                &preimage.block_token,
                &preimage.correction_id,
            )?,
            record_tag: STREAM_CORRECTION_RECEIPT_TAG.to_string(),
            graph_identity_digest: preimage.graph_identity_digest,
            identity: preimage.identity,
            stream_incarnation_id: preimage.stream_incarnation_id,
            binding_scope_id: preimage.binding_scope_id,
            block_token: preimage.block_token,
            correction_id: preimage.correction_id,
            correction_plan_digest: preimage.correction_plan_digest,
            actor_id: preimage.actor_id,
            graph_commit_id: preimage.graph_commit_id,
            result_payload,
            result_digest: String::new(),
            resulting_manifest_version: preimage.resulting_manifest_version,
            resulting_lifecycle_revision: preimage.resulting_lifecycle_revision,
            resulting_lifecycle_digest: preimage.resulting_lifecycle_digest,
            resulting_token_authority_digest: preimage.resulting_token_authority_digest,
            recorded_at: preimage.recorded_at,
        };
        value.result_digest = ManagementReceipt::result_digest_for(&value.result_payload)?;
        value.record_id = value.compute_record_id();
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        stream_incarnation_id: &str,
        block_token: &str,
        correction_id: &str,
    ) -> Result<String> {
        validate_digest(
            "correction receipt graph_identity_digest",
            graph_identity_digest,
        )?;
        identity.validate()?;
        validate_uuid(
            "correction receipt stream_incarnation_id",
            stream_incarnation_id,
        )?;
        validate_digest("correction receipt block_token", block_token)?;
        validate_uuid("correction receipt correction_id", correction_id)?;
        Ok(format!(
            "stream-correction-v1:{}",
            hash_fields(
                STREAM_CORRECTION_RECEIPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    stream_incarnation_id.as_bytes(),
                    block_token.as_bytes(),
                    correction_id.as_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != STREAM_CORRECTION_RECEIPT_PROTOCOL_VERSION
            || self.record_tag != STREAM_CORRECTION_RECEIPT_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream correction receipt has an unsupported protocol or tag",
            ));
        }
        validate_digest(
            "correction receipt graph_identity_digest",
            &self.graph_identity_digest,
        )?;
        self.identity.validate()?;
        validate_uuid(
            "correction receipt stream_incarnation_id",
            &self.stream_incarnation_id,
        )?;
        validate_uuid(
            "correction receipt binding_scope_id",
            &self.binding_scope_id,
        )?;
        validate_digest("correction receipt block_token", &self.block_token)?;
        validate_uuid("correction receipt correction_id", &self.correction_id)?;
        validate_digest(
            "correction receipt correction_plan_digest",
            &self.correction_plan_digest,
        )?;
        validate_canonical_text("correction receipt actor_id", &self.actor_id)?;
        validate_canonical_text("correction receipt graph_commit_id", &self.graph_commit_id)?;
        if self.resulting_manifest_version == 0
            || self.resulting_lifecycle_revision == 0
            || self.recorded_at <= 0
        {
            return Err(OmniError::manifest_internal(
                "stream correction receipt requires positive manifest/lifecycle versions and timestamp",
            ));
        }
        validate_digest(
            "correction receipt resulting_lifecycle_digest",
            &self.resulting_lifecycle_digest,
        )?;
        validate_digest(
            "correction receipt resulting_token_authority_digest",
            &self.resulting_token_authority_digest,
        )?;
        let expected_result = stream_correction_result_payload(
            &self.correction_id,
            &self.correction_plan_digest,
            &self.graph_commit_id,
            self.resulting_manifest_version,
            self.resulting_lifecycle_revision,
        )?;
        if self.result_payload != expected_result
            || self.result_digest != ManagementReceipt::result_digest_for(&expected_result)?
            || self.record_lookup_key
                != Self::lookup_key_for(
                    &self.graph_identity_digest,
                    self.identity,
                    &self.stream_incarnation_id,
                    &self.block_token,
                    &self.correction_id,
                )?
            || self.record_id != self.compute_record_id()
        {
            return Err(OmniError::manifest_internal(
                "stream correction receipt differs from its canonical occurrence or result",
            ));
        }
        Ok(())
    }

    fn compute_record_id(&self) -> String {
        hash_fields(
            STREAM_CORRECTION_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.record_lookup_key.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.identity.stable_table_id.to_be_bytes(),
                &self.identity.table_incarnation_id.to_be_bytes(),
                self.stream_incarnation_id.as_bytes(),
                self.binding_scope_id.as_bytes(),
                self.block_token.as_bytes(),
                self.correction_id.as_bytes(),
                self.correction_plan_digest.as_bytes(),
                self.actor_id.as_bytes(),
                self.graph_commit_id.as_bytes(),
                self.result_digest.as_bytes(),
                &self.resulting_manifest_version.to_be_bytes(),
                &self.resulting_lifecycle_revision.to_be_bytes(),
                self.resulting_lifecycle_digest.as_bytes(),
                self.resulting_token_authority_digest.as_bytes(),
                &self.recorded_at.to_be_bytes(),
            ],
        )
    }
}

pub(crate) fn stream_correction_result_payload(
    correction_id: &str,
    correction_plan_digest: &str,
    graph_commit_id: &str,
    manifest_version: u64,
    lifecycle_revision: u64,
) -> Result<serde_json::Value> {
    validate_uuid("correction result correction_id", correction_id)?;
    validate_digest(
        "correction result correction_plan_digest",
        correction_plan_digest,
    )?;
    validate_canonical_text("correction result graph_commit_id", graph_commit_id)?;
    if manifest_version == 0 || lifecycle_revision == 0 {
        return Err(OmniError::manifest_internal(
            "stream correction result requires positive manifest and lifecycle versions",
        ));
    }
    Ok(serde_json::json!({
        "correction_id": correction_id,
        "correction_plan_digest": correction_plan_digest,
        "graph_commit_id": graph_commit_id,
        "lifecycle": "DRAINING",
        "manifest_version": manifest_version,
        "revision": lifecycle_revision,
    }))
}

pub(crate) fn stream_lifecycle_authority_digest(
    lifecycle: &StreamLifecycleEntry,
) -> Result<String> {
    let canonical = lifecycle.to_metadata_json()?;
    Ok(hash_fields(
        STREAM_LIFECYCLE_AUTHORITY_DIGEST_DOMAIN,
        &[canonical.as_bytes()],
    ))
}

/// Release one exact DataBlock after its corrected base/token effects have
/// been prepared. The drain remains DRAINING and all claim/cut authority is
/// preserved; only the graph HEAD, block, fold outcome, revision, and
/// management-receipt chain may change.
pub(crate) fn build_data_block_correction_successor(
    prior: &StreamLifecycleEntry,
    expected_block_token: &str,
    correction_plan_digest: &str,
    management_receipt: &ManagementReceipt,
    outcome: StreamDataCorrectionOutcome,
) -> Result<StreamLifecycleEntry> {
    prior.validate()?;
    validate_digest("correction expected_block_token", expected_block_token)?;
    validate_digest("correction plan digest", correction_plan_digest)?;
    let block = prior.strict_block.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("stream correction requires a current strict block")
    })?;
    if prior.lifecycle != StreamLifecycle::Draining
        || block.block_token != expected_block_token
        || !matches!(block.evidence, StrictBlockEvidence::DataBlock { .. })
    {
        return Err(OmniError::manifest_internal(
            "stream correction requires the exact current DataBlock authority",
        ));
    }
    let blocked_summary = prior.last_fold_summary.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("stream correction requires the blocked fold summary")
    })?;
    let drain_id = prior
        .drain
        .as_ref()
        .expect("validated DRAINING lifecycle has a drain")
        .drain_id
        .clone();
    if blocked_summary.outcome != LastFoldOutcome::StrictBlocked {
        return Err(OmniError::manifest_internal(
            "stream correction requires a STRICT_BLOCKED fold summary",
        ));
    }
    let next_revision = prior.lifecycle_revision.checked_add(1).ok_or_else(|| {
        OmniError::manifest_internal("stream correction lifecycle revision overflow")
    })?;
    let expected_table_version = prior
        .current_head_witness
        .table_version
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream correction table version overflow"))?;
    outcome.current_head_witness.validate()?;
    validate_canonical_text("correction graph_commit_id", &outcome.graph_commit_id)?;
    if outcome.recorded_at <= 0
        || outcome.current_head_witness.branch_identifier
            != prior.current_head_witness.branch_identifier
        || outcome.current_head_witness.table_version != expected_table_version
        || outcome.current_head_witness.transaction_uuid
            == prior.current_head_witness.transaction_uuid
        || outcome.visible_rows > blocked_summary.input_rows
    {
        return Err(OmniError::manifest_internal(
            "stream correction outcome does not name the exact next base-table effect",
        ));
    }
    management_receipt.validate(next_revision)?;
    let request_block_token = management_receipt
        .request_payload
        .get("block_token")
        .and_then(serde_json::Value::as_str);
    let request_plan_digest = management_receipt
        .request_payload
        .get("correction_plan_digest")
        .and_then(serde_json::Value::as_str);
    let expected_result = stream_correction_result_payload(
        &management_receipt.operation_id,
        correction_plan_digest,
        &outcome.graph_commit_id,
        outcome.manifest_version,
        next_revision,
    )?;
    if management_receipt.identity != prior.identity
        || management_receipt.stream_incarnation_id
            != prior.enrollment_receipt.stream_incarnation_id
        || management_receipt.binding_scope_id != prior.binding_scope_id
        || management_receipt.operation_kind != STREAM_CORRECTION_OPERATION_KIND
        || management_receipt.from_revision != prior.lifecycle_revision
        || management_receipt.to_revision != next_revision
        || management_receipt.predecessor_record_id != prior.management_receipt_chain.head_record_id
        || management_receipt.prior_chain_digest != prior.management_receipt_chain.chain_digest
        || management_receipt.chain_ordinal
            != prior
                .management_receipt_chain
                .record_count
                .checked_add(1)
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream correction management receipt chain overflow",
                    )
                })?
        || request_block_token != Some(expected_block_token)
        || request_plan_digest != Some(correction_plan_digest)
        || management_receipt.result_payload != expected_result
        || management_receipt.recorded_at != outcome.recorded_at
    {
        return Err(OmniError::manifest_internal(
            "stream correction management receipt differs from its exact block, plan, or result",
        ));
    }

    let mut successor = prior.clone();
    successor.current_head_witness = outcome.current_head_witness;
    successor.lifecycle_revision = next_revision;
    successor.management_receipt_chain = management_receipt.next_chain_ref()?;
    successor.strict_block = None;
    successor
        .drain
        .as_mut()
        .expect("validated DRAINING lifecycle has a drain")
        .expected_current_head_witness = successor.current_head_witness.clone();
    successor.last_fold_summary = Some(LastFoldSummary {
        // LastFoldSummary is the continuation proof for the active drain. The
        // immutable correction/management receipts own the correction-id
        // audit; retaining the drain id here lets the next empty-cut proof
        // recognize this corrected publication as that drain's fold.
        operation_id: drain_id,
        graph_commit_id: Some(outcome.graph_commit_id),
        exact_generation_cut: blocked_summary.exact_generation_cut.clone(),
        outcome: LastFoldOutcome::Published,
        input_rows: blocked_summary.input_rows,
        input_bytes: blocked_summary.input_bytes,
        visible_rows: outcome.visible_rows,
        visible_bytes: outcome.visible_bytes,
        recorded_at: outcome.recorded_at,
    });
    successor.validate_successor_of(prior)?;
    Ok(successor)
}

/// Canonical semantic result of one successful quiesce transition.
///
/// The terminal builder, recovery validator, and live receipt writer all use
/// this one shape so a well-formed receipt cannot commit a caller-chosen JSON
/// result while still advancing the lane to `SEALED`.
pub(crate) fn stream_quiesce_result_payload(
    sealed_lifecycle_revision: u64,
) -> Result<serde_json::Value> {
    if sealed_lifecycle_revision == 0 {
        return Err(OmniError::manifest_internal(
            "terminal quiesce result requires a positive lifecycle revision",
        ));
    }
    Ok(serde_json::json!({
        "lifecycle": "SEALED",
        "revision": sealed_lifecycle_revision,
    }))
}

/// Canonical semantic result of one successful resume or abort-drain.
pub(crate) fn stream_resume_result_payload(
    open_lifecycle_revision: u64,
) -> Result<serde_json::Value> {
    if open_lifecycle_revision == 0 {
        return Err(OmniError::manifest_internal(
            "terminal stream resume result requires a positive lifecycle revision",
        ));
    }
    Ok(serde_json::json!({
        "lifecycle": "OPEN",
        "revision": open_lifecycle_revision,
    }))
}

/// Canonical terminal result of replacing one sealed lane's physical
/// enrollment while preserving its logical stream incarnation.
pub(crate) fn stream_rebind_result_payload(
    sealed_lifecycle_revision: u64,
    binding_scope_id: &str,
    binding: &StreamPhysicalBinding,
    current_head: &CurrentHeadWitness,
) -> Result<serde_json::Value> {
    if sealed_lifecycle_revision == 0 {
        return Err(OmniError::manifest_internal(
            "terminal stream rebind result requires a positive lifecycle revision",
        ));
    }
    validate_uuid("rebind result binding_scope_id", binding_scope_id)?;
    binding.validate(binding.identity()?)?;
    current_head.validate()?;
    Ok(serde_json::json!({
        "lifecycle": "SEALED",
        "revision": sealed_lifecycle_revision,
        "binding_scope_id": binding_scope_id,
        "binding": binding,
        "current_head": current_head,
    }))
}

/// Canonical semantic result of adopting an in-flight drain into disable.
pub(crate) fn stream_disable_drain_adoption_result_payload(
    next_lifecycle_revision: u64,
) -> Result<serde_json::Value> {
    if next_lifecycle_revision == 0 {
        return Err(OmniError::manifest_internal(
            "disable-drain adoption result requires a positive lifecycle revision",
        ));
    }
    Ok(serde_json::json!({
        "goal": "SEALED",
        "revision": next_lifecycle_revision,
    }))
}

/// Derive the stable UUID-v4 drain occurrence owned by one durable disable
/// plan and one immutable table lifetime.
///
/// UUID-v4 shape is part of the existing quiesce grammar. The random payload
/// is replaced with a domain-separated digest so a replacement offline apply
/// reconstructs the same occurrence without storing a parallel work queue.
pub(crate) fn stream_disable_drain_id(
    disable_operation_id: &str,
    identity: TableIdentity,
) -> Result<String> {
    validate_canonical_text("disable operation id", disable_operation_id)?;
    identity.validate()?;
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, DISABLE_DRAIN_ID_DOMAIN);
    hash_bytes(&mut hasher, disable_operation_id.as_bytes());
    hash_bytes(&mut hasher, &identity.stable_table_id.to_be_bytes());
    hash_bytes(&mut hasher, &identity.table_incarnation_id.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(ShardId::from_bytes(bytes).to_string())
}

/// Canonical request selected by metadata-only disable-drain adoption.
pub(crate) fn stream_disable_drain_adoption_request_payload(
    disable_operation_id: &str,
    identity: TableIdentity,
    drain_id: &str,
    profile_revision: u64,
) -> Result<serde_json::Value> {
    validate_canonical_text("disable operation id", disable_operation_id)?;
    identity.validate()?;
    validate_uuid("disable-drain adoption drain_id", drain_id)?;
    if profile_revision == 0 {
        return Err(OmniError::manifest_internal(
            "disable-drain adoption requires a positive profile revision",
        ));
    }
    Ok(serde_json::json!({
        "disable_operation_id": disable_operation_id,
        "identity": identity,
        "drain_id": drain_id,
        "profile_revision": profile_revision,
    }))
}

/// Stable receipt/adoption occurrence selected by the same durable inputs as
/// the canonical request.
pub(crate) fn stream_disable_drain_adoption_id(
    disable_operation_id: &str,
    identity: TableIdentity,
    drain_id: &str,
    profile_revision: u64,
) -> Result<String> {
    let request = stream_disable_drain_adoption_request_payload(
        disable_operation_id,
        identity,
        drain_id,
        profile_revision,
    )?;
    canonical_json_digest(
        DISABLE_DRAIN_ADOPTION_ID_DOMAIN,
        "disable-drain adoption request",
        &request,
    )
}

/// Stable UUID occurrence used by the existing management-receipt grammar.
///
/// `DisableDrainAdoption::adoption_id` remains the pre-registered digest
/// commitment; the immutable receipt separately requires a canonical UUID
/// operation occurrence.
pub(crate) fn stream_disable_drain_adoption_operation_id(
    disable_operation_id: &str,
    identity: TableIdentity,
    drain_id: &str,
    profile_revision: u64,
) -> Result<String> {
    let request = stream_disable_drain_adoption_request_payload(
        disable_operation_id,
        identity,
        drain_id,
        profile_revision,
    )?;
    canonical_json_uuid(
        DISABLE_DRAIN_ADOPTION_OPERATION_ID_DOMAIN,
        "disable-drain adoption request",
        &request,
    )
}

impl ClaimAttemptEffect {
    pub(crate) fn new(
        prior_chain: &ReceiptChainRef,
        preimage: ClaimAttemptEffectPreimage,
    ) -> Result<Self> {
        prior_chain.validate_with_domain(CLAIM_ATTEMPT_CHAIN_GENESIS_DOMAIN)?;
        let ordinal = next_chain_ordinal(prior_chain, "claim-attempt")?;
        let mut value = Self {
            protocol_version: CLAIM_ATTEMPT_EFFECT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &preimage.graph_identity_digest,
                preimage.identity,
                &preimage.binding_scope_id,
                &preimage.claim_id,
                ordinal,
            )?,
            record_tag: CLAIM_ATTEMPT_EFFECT_TAG.to_string(),
            graph_identity_digest: preimage.graph_identity_digest,
            identity: preimage.identity,
            stream_incarnation_id: preimage.stream_incarnation_id,
            binding_scope_id: preimage.binding_scope_id,
            enrollment_id: preimage.enrollment_id,
            shard_id: preimage.shard_id,
            claim_id: preimage.claim_id,
            ordinal,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_attempt_chain_digest: prior_chain.chain_digest.clone(),
            resulting_attempt_chain_digest: String::new(),
            attempt_id: preimage.attempt_id,
            attempt_plan_digest: preimage.attempt_plan_digest,
            bound_prestate_digest: preimage.bound_prestate_digest,
            storage_envelope_digest: preimage.storage_envelope_digest,
            planned_sentinel_position: preimage.planned_sentinel_position,
            planned_sentinel_digest: preimage.planned_sentinel_digest,
            achieved_shard_manifest_version: preimage.achieved_shard_manifest_version,
            achieved_writer_epoch: preimage.achieved_writer_epoch,
            observed_sentinel_position: preimage.observed_sentinel_position,
            observed_sentinel_digest: preimage.observed_sentinel_digest,
            attempt_terminal_effect_digest: preimage.attempt_terminal_effect_digest,
            classification: preimage.classification,
        };
        value.record_id = value.compute_record_id();
        value.resulting_attempt_chain_digest = hash_fields(
            CLAIM_ATTEMPT_CHAIN_DOMAIN,
            &[
                value.prior_attempt_chain_digest.as_bytes(),
                &value.ordinal.to_be_bytes(),
                value.record_id.as_bytes(),
            ],
        );
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        binding_scope_id: &str,
        claim_id: &str,
        ordinal: u64,
    ) -> Result<String> {
        validate_digest("claim-attempt graph_identity_digest", graph_identity_digest)?;
        identity.validate()?;
        validate_uuid("claim-attempt binding_scope_id", binding_scope_id)?;
        validate_uuid("claim-attempt claim_id", claim_id)?;
        if ordinal == 0 {
            return Err(OmniError::manifest_internal(
                "stream claim-attempt ordinal must be positive",
            ));
        }
        Ok(format!(
            "stream-claim-attempt-v1:{}",
            hash_fields(
                CLAIM_ATTEMPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    binding_scope_id.as_bytes(),
                    claim_id.as_bytes(),
                    &ordinal.to_be_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != CLAIM_ATTEMPT_EFFECT_PROTOCOL_VERSION
            || self.record_tag != CLAIM_ATTEMPT_EFFECT_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream claim-attempt row has an unsupported protocol or tag",
            ));
        }
        validate_digest(
            "claim-attempt graph_identity_digest",
            &self.graph_identity_digest,
        )?;
        self.identity.validate()?;
        validate_uuid(
            "claim-attempt stream_incarnation_id",
            &self.stream_incarnation_id,
        )?;
        validate_uuid("claim-attempt binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("claim-attempt enrollment_id", &self.enrollment_id)?;
        validate_uuid("claim-attempt shard_id", &self.shard_id)?;
        validate_uuid("claim-attempt claim_id", &self.claim_id)?;
        validate_uuid("claim-attempt attempt_id", &self.attempt_id)?;
        validate_digest("claim attempt_plan_digest", &self.attempt_plan_digest)?;
        validate_digest("claim bound_prestate_digest", &self.bound_prestate_digest)?;
        if let Some(digest) = self.storage_envelope_digest.as_deref() {
            validate_digest("claim storage_envelope_digest", digest)?;
        }
        if self.planned_sentinel_position == 0 {
            return Err(OmniError::manifest_internal(
                "stream claim planned sentinel position must be non-zero",
            ));
        }
        validate_digest(
            "claim planned_sentinel_digest",
            &self.planned_sentinel_digest,
        )?;
        validate_digest(
            "claim attempt_terminal_effect_digest",
            &self.attempt_terminal_effect_digest,
        )?;
        let achieved_manifest = match (
            self.achieved_shard_manifest_version,
            self.achieved_writer_epoch,
        ) {
            (Some(version), Some(epoch)) if version > 0 && epoch > 0 => true,
            (None, None) => false,
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream claim attempt must carry achieved manifest version and writer epoch together and non-zero",
                ));
            }
        };
        let observed_sentinel = match (
            self.observed_sentinel_position,
            self.observed_sentinel_digest.as_deref(),
        ) {
            (Some(position), Some(digest)) if position > 0 => {
                validate_digest("claim observed_sentinel_digest", digest)?;
                if position != self.planned_sentinel_position
                    || digest != self.planned_sentinel_digest
                {
                    return Err(OmniError::manifest_internal(
                        "stream claim observed sentinel differs from its pre-armed plan",
                    ));
                }
                true
            }
            (None, None) => false,
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream claim attempt must carry observed sentinel position and digest together",
                ));
            }
        };
        let expected_effects = match self.classification {
            ClaimAttemptClassification::NoEffect | ClaimAttemptClassification::AbortedNoEffect => {
                (false, false)
            }
            ClaimAttemptClassification::StockManifestOnly => (true, false),
            ClaimAttemptClassification::StockManifestPlusSentinel
            | ClaimAttemptClassification::PatchedSentinelPlusNamingManifest => (true, true),
            ClaimAttemptClassification::PatchedSentinelOnly => (false, true),
        };
        if (achieved_manifest, observed_sentinel) != expected_effects {
            return Err(OmniError::manifest_internal(format!(
                "stream claim attempt effect fields disagree with classification {:?}",
                self.classification
            )));
        }
        let expected_lookup = Self::lookup_key_for(
            &self.graph_identity_digest,
            self.identity,
            &self.binding_scope_id,
            &self.claim_id,
            self.ordinal,
        )?;
        validate_attempt_envelope(self, &expected_lookup, &self.compute_record_id())
    }

    pub(crate) fn validate_for_profile(&self, profile: ClaimProfile) -> Result<()> {
        self.validate()?;
        match (profile, self.storage_envelope_digest.as_deref()) {
            (ClaimProfile::RetainAll, None) | (ClaimProfile::ManagedReclamation, Some(_)) => Ok(()),
            (ClaimProfile::RetainAll, Some(_)) => Err(OmniError::manifest_internal(
                "retain-all claim attempts cannot carry a managed-reclamation storage envelope",
            )),
            (ClaimProfile::ManagedReclamation, None) => Err(OmniError::manifest_internal(
                "managed-reclamation claim attempts require a storage envelope digest",
            )),
        }
    }

    pub(crate) fn next_attempt_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        receipt_next_chain_ref(
            self.ordinal,
            &self.record_id,
            &self.resulting_attempt_chain_digest,
        )
    }

    fn compute_record_id(&self) -> String {
        hash_fields(
            CLAIM_ATTEMPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                self.graph_identity_digest.as_bytes(),
                &self.identity.stable_table_id.to_be_bytes(),
                &self.identity.table_incarnation_id.to_be_bytes(),
                self.stream_incarnation_id.as_bytes(),
                self.binding_scope_id.as_bytes(),
                self.enrollment_id.as_bytes(),
                self.shard_id.as_bytes(),
                self.claim_id.as_bytes(),
                &self.ordinal.to_be_bytes(),
                self.predecessor_record_id
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.prior_attempt_chain_digest.as_bytes(),
                self.attempt_id.as_bytes(),
                self.attempt_plan_digest.as_bytes(),
                self.bound_prestate_digest.as_bytes(),
                self.storage_envelope_digest
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                &self.planned_sentinel_position.to_be_bytes(),
                self.planned_sentinel_digest.as_bytes(),
                &self
                    .achieved_shard_manifest_version
                    .unwrap_or(0)
                    .to_be_bytes(),
                &self.achieved_writer_epoch.unwrap_or(0).to_be_bytes(),
                &self.observed_sentinel_position.unwrap_or(0).to_be_bytes(),
                self.observed_sentinel_digest
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.attempt_terminal_effect_digest.as_bytes(),
                format!("{:?}", self.classification).as_bytes(),
            ],
        )
    }
}

impl ClaimReceipt {
    pub(crate) fn new(
        prior_chain: &ReceiptChainRef,
        preimage: ClaimReceiptPreimage,
    ) -> Result<Self> {
        prior_chain.validate_with_domain(CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        let chain_ordinal = next_chain_ordinal(prior_chain, "claim receipt")?;
        let mut value = Self {
            protocol_version: CLAIM_RECEIPT_PROTOCOL_VERSION,
            record_id: String::new(),
            record_lookup_key: Self::lookup_key_for(
                &preimage.graph_identity_digest,
                preimage.identity,
                &preimage.binding_scope_id,
                &preimage.claim_id,
            )?,
            record_tag: CLAIM_RECEIPT_TAG.to_string(),
            graph_identity_digest: preimage.graph_identity_digest,
            identity: preimage.identity,
            chain_ordinal,
            predecessor_record_id: prior_chain.head_record_id.clone(),
            prior_chain_digest: prior_chain.chain_digest.clone(),
            resulting_chain_digest: String::new(),
            claim_id: preimage.claim_id,
            lifecycle_operation_id: preimage.lifecycle_operation_id,
            binding_scope_id: preimage.binding_scope_id,
            enrollment_id: preimage.enrollment_id,
            shard_id: preimage.shard_id,
            stream_incarnation_id: preimage.stream_incarnation_id,
            stream_configuration_digest: preimage.stream_configuration_digest,
            physical_binding_digest: preimage.physical_binding_digest,
            recovery_operation_id: preimage.recovery_operation_id,
            claim_kind: preimage.claim_kind,
            profile: preimage.profile,
            claim_operation_digest: preimage.claim_operation_digest,
            attempt_count: preimage.attempt_count,
            attempt_chain_head_id: preimage.attempt_chain_head_id,
            attempt_effect_chain_digest: preimage.attempt_effect_chain_digest,
            terminal_attempt_id: preimage.terminal_attempt_id,
            terminal_pre_shard_manifest_version: preimage.terminal_pre_shard_manifest_version,
            achieved_shard_manifest_version: preimage.achieved_shard_manifest_version,
            achieved_writer_epoch: preimage.achieved_writer_epoch,
            sentinel_position: preimage.sentinel_position,
            sentinel_digest: preimage.sentinel_digest,
            replay_cursor: preimage.replay_cursor,
            authenticated_tail_prior_position: preimage.authenticated_tail_prior_position,
            authenticated_tail_position: preimage.authenticated_tail_position,
            authenticated_tail_published_prefix_position: preimage
                .authenticated_tail_published_prefix_position,
            authenticated_tail_segment_entry_count: preimage.authenticated_tail_segment_entry_count,
            authenticated_tail_segment_digest: preimage.authenticated_tail_segment_digest,
            authenticated_tail_segment_lww_projection_digest: preimage
                .authenticated_tail_segment_lww_projection_digest,
            authenticated_tail_prior_chain_digest: preimage.authenticated_tail_prior_chain_digest,
            authenticated_tail_segment_count: preimage.authenticated_tail_segment_count,
            authenticated_tail_chain_digest: preimage.authenticated_tail_chain_digest,
            authenticated_tail_empty_fence_state_digest: preimage
                .authenticated_tail_empty_fence_state_digest,
            authenticated_tail_lww_projection_digest: preimage
                .authenticated_tail_lww_projection_digest,
            terminal_effect_digest: preimage.terminal_effect_digest,
            terminal_classification: preimage.terminal_classification,
            recorded_at: preimage.recorded_at,
        };
        value.record_id = value.compute_record_id()?;
        value.resulting_chain_digest = receipt_chain_step_digest(
            &value.record_tag,
            &value.prior_chain_digest,
            value.chain_ordinal,
            &value.record_id,
        );
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn lookup_key_for(
        graph_identity_digest: &str,
        identity: TableIdentity,
        binding_scope_id: &str,
        claim_id: &str,
    ) -> Result<String> {
        validate_digest("claim graph_identity_digest", graph_identity_digest)?;
        identity.validate()?;
        validate_uuid("claim binding_scope_id", binding_scope_id)?;
        validate_uuid("claim_id", claim_id)?;
        Ok(format!(
            "stream-claim-v1:{}",
            hash_fields(
                CLAIM_RECEIPT_LOOKUP_DOMAIN,
                &[
                    graph_identity_digest.as_bytes(),
                    &identity.stable_table_id.to_be_bytes(),
                    &identity.table_incarnation_id.to_be_bytes(),
                    binding_scope_id.as_bytes(),
                    claim_id.as_bytes(),
                ],
            )
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != CLAIM_RECEIPT_PROTOCOL_VERSION
            || self.record_tag != CLAIM_RECEIPT_TAG
        {
            return Err(OmniError::manifest_internal(
                "stream claim receipt has an unsupported protocol or tag",
            ));
        }
        validate_digest("claim graph_identity_digest", &self.graph_identity_digest)?;
        self.identity.validate()?;
        validate_uuid("claim_id", &self.claim_id)?;
        if let Some(operation_id) = self.lifecycle_operation_id.as_deref() {
            validate_uuid("claim lifecycle_operation_id", operation_id)?;
        }
        validate_uuid("claim binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("claim enrollment_id", &self.enrollment_id)?;
        validate_uuid("claim shard_id", &self.shard_id)?;
        validate_uuid("claim stream_incarnation_id", &self.stream_incarnation_id)?;
        validate_digest(
            "claim stream_configuration_digest",
            &self.stream_configuration_digest,
        )?;
        validate_digest(
            "claim physical_binding_digest",
            &self.physical_binding_digest,
        )?;
        validate_canonical_text("claim recovery_operation_id", &self.recovery_operation_id)?;
        validate_protocol_label("claim_kind", &self.claim_kind)?;
        validate_digest("claim_operation_digest", &self.claim_operation_digest)?;
        validate_digest("claim attempt_chain_head_id", &self.attempt_chain_head_id)?;
        validate_digest(
            "claim attempt_effect_chain_digest",
            &self.attempt_effect_chain_digest,
        )?;
        validate_uuid("claim terminal_attempt_id", &self.terminal_attempt_id)?;
        for (field, digest) in [
            ("sentinel_digest", self.sentinel_digest.as_str()),
            (
                "authenticated_tail_segment_digest",
                self.authenticated_tail_segment_digest.as_str(),
            ),
            (
                "authenticated_tail_segment_lww_projection_digest",
                self.authenticated_tail_segment_lww_projection_digest
                    .as_str(),
            ),
            (
                "authenticated_tail_prior_chain_digest",
                self.authenticated_tail_prior_chain_digest.as_str(),
            ),
            (
                "authenticated_tail_chain_digest",
                self.authenticated_tail_chain_digest.as_str(),
            ),
            (
                "authenticated_tail_empty_fence_state_digest",
                self.authenticated_tail_empty_fence_state_digest.as_str(),
            ),
            (
                "authenticated_tail_lww_projection_digest",
                self.authenticated_tail_lww_projection_digest.as_str(),
            ),
            (
                "terminal_effect_digest",
                self.terminal_effect_digest.as_str(),
            ),
        ] {
            validate_digest(field, digest)?;
        }
        if self.attempt_count == 0
            || self.terminal_pre_shard_manifest_version == 0
            || self.achieved_shard_manifest_version <= self.terminal_pre_shard_manifest_version
            || self.achieved_writer_epoch == 0
            || self.sentinel_position == 0
            || self.replay_cursor > self.sentinel_position
            || self.authenticated_tail_position != self.sentinel_position
            || self.authenticated_tail_prior_position >= self.authenticated_tail_position
            || self
                .authenticated_tail_position
                .checked_sub(self.authenticated_tail_prior_position)
                != Some(self.authenticated_tail_segment_entry_count)
            || (self.authenticated_tail_published_prefix_position != 0
                && (self.authenticated_tail_published_prefix_position
                    <= self.authenticated_tail_prior_position
                    || self.authenticated_tail_published_prefix_position
                        >= self.authenticated_tail_position))
            || self.authenticated_tail_segment_entry_count == 0
            || self.authenticated_tail_segment_count == 0
            || self.recorded_at <= 0
        {
            return Err(OmniError::manifest_internal(
                "stream claim terminal or authenticated-tail authority is invalid",
            ));
        }
        let expected_tail_chain = authenticated_wal_tail_chain_digest(
            &self.binding_scope_id,
            &self.enrollment_id,
            &self.shard_id,
            &self.stream_incarnation_id,
            &self.stream_configuration_digest,
            &self.physical_binding_digest,
            self.authenticated_tail_prior_position,
            self.authenticated_tail_position,
            self.authenticated_tail_segment_entry_count,
            &self.authenticated_tail_segment_digest,
            &self.authenticated_tail_prior_chain_digest,
            self.authenticated_tail_segment_count,
            &self.authenticated_tail_empty_fence_state_digest,
            &self.authenticated_tail_lww_projection_digest,
        )?;
        if self.authenticated_tail_chain_digest != expected_tail_chain {
            return Err(OmniError::manifest_internal(
                "stream claim authenticated WAL-tail chain digest differs from its canonical segment transition",
            ));
        }
        let expected_terminal = match self.terminal_classification {
            ClaimTerminalClassification::StockManifestPlusSentinel => {
                ClaimAttemptClassification::StockManifestPlusSentinel
            }
            ClaimTerminalClassification::PatchedSentinelPlusNamingManifest => {
                ClaimAttemptClassification::PatchedSentinelPlusNamingManifest
            }
        };
        // The terminal attempt body is immutable in the attempt ledger. The
        // receipt commits its ID and effect class rather than retaining the
        // body; recovery performs the exact indexed cross-check before CAS.
        let _ = expected_terminal;
        validate_receipt_envelope(
            &self.record_id,
            &self.record_lookup_key,
            &self.record_tag,
            self.chain_ordinal,
            self.predecessor_record_id.as_deref(),
            &self.prior_chain_digest,
            &self.resulting_chain_digest,
            CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN,
            &Self::lookup_key_for(
                &self.graph_identity_digest,
                self.identity,
                &self.binding_scope_id,
                &self.claim_id,
            )?,
            &self.compute_record_id()?,
        )
    }

    pub(crate) fn next_chain_ref(&self) -> Result<ReceiptChainRef> {
        self.validate()?;
        receipt_next_chain_ref(
            self.chain_ordinal,
            &self.record_id,
            &self.resulting_chain_digest,
        )
    }

    fn compute_record_id(&self) -> Result<String> {
        let body = bounded_json_bytes("claim receipt body", &self.preimage())?;
        Ok(hash_fields(
            CLAIM_RECEIPT_RECORD_DOMAIN,
            &[
                self.record_tag.as_bytes(),
                &self.chain_ordinal.to_be_bytes(),
                self.predecessor_record_id
                    .as_deref()
                    .unwrap_or("")
                    .as_bytes(),
                self.prior_chain_digest.as_bytes(),
                &body,
            ],
        ))
    }

    fn preimage(&self) -> ClaimReceiptPreimage {
        ClaimReceiptPreimage {
            graph_identity_digest: self.graph_identity_digest.clone(),
            identity: self.identity,
            claim_id: self.claim_id.clone(),
            lifecycle_operation_id: self.lifecycle_operation_id.clone(),
            binding_scope_id: self.binding_scope_id.clone(),
            enrollment_id: self.enrollment_id.clone(),
            shard_id: self.shard_id.clone(),
            stream_incarnation_id: self.stream_incarnation_id.clone(),
            stream_configuration_digest: self.stream_configuration_digest.clone(),
            physical_binding_digest: self.physical_binding_digest.clone(),
            recovery_operation_id: self.recovery_operation_id.clone(),
            claim_kind: self.claim_kind.clone(),
            profile: self.profile,
            claim_operation_digest: self.claim_operation_digest.clone(),
            attempt_count: self.attempt_count,
            attempt_chain_head_id: self.attempt_chain_head_id.clone(),
            attempt_effect_chain_digest: self.attempt_effect_chain_digest.clone(),
            terminal_attempt_id: self.terminal_attempt_id.clone(),
            terminal_pre_shard_manifest_version: self.terminal_pre_shard_manifest_version,
            achieved_shard_manifest_version: self.achieved_shard_manifest_version,
            achieved_writer_epoch: self.achieved_writer_epoch,
            sentinel_position: self.sentinel_position,
            sentinel_digest: self.sentinel_digest.clone(),
            replay_cursor: self.replay_cursor,
            authenticated_tail_prior_position: self.authenticated_tail_prior_position,
            authenticated_tail_position: self.authenticated_tail_position,
            authenticated_tail_published_prefix_position: self
                .authenticated_tail_published_prefix_position,
            authenticated_tail_segment_entry_count: self.authenticated_tail_segment_entry_count,
            authenticated_tail_segment_digest: self.authenticated_tail_segment_digest.clone(),
            authenticated_tail_segment_lww_projection_digest: self
                .authenticated_tail_segment_lww_projection_digest
                .clone(),
            authenticated_tail_prior_chain_digest: self
                .authenticated_tail_prior_chain_digest
                .clone(),
            authenticated_tail_segment_count: self.authenticated_tail_segment_count,
            authenticated_tail_chain_digest: self.authenticated_tail_chain_digest.clone(),
            authenticated_tail_empty_fence_state_digest: self
                .authenticated_tail_empty_fence_state_digest
                .clone(),
            authenticated_tail_lww_projection_digest: self
                .authenticated_tail_lww_projection_digest
                .clone(),
            terminal_effect_digest: self.terminal_effect_digest.clone(),
            terminal_classification: self.terminal_classification,
            recorded_at: self.recorded_at,
        }
    }

    /// Whether this claim authenticated only its terminal fence sentinel and
    /// therefore proves the selected current generation has an empty LWW
    /// projection.
    ///
    /// The stock Lance B2a manifest replay cursor can legitimately remain
    /// before that sentinel, so cursor equality is not part of this proof.
    /// The authenticated segment is instead exact: one entry, immediately
    /// after the prior tail, with its empty suffix projection equal to the
    /// complete current-generation projection.
    pub(crate) fn proves_empty_current_generation(&self) -> bool {
        self.authenticated_tail_segment_entry_count == 1
            && self.authenticated_tail_prior_position.checked_add(1)
                == Some(self.authenticated_tail_position)
            && self.authenticated_tail_segment_lww_projection_digest
                == self.authenticated_tail_lww_projection_digest
    }
}

impl DisableDrainAdoption {
    fn validate(&self) -> Result<()> {
        validate_digest("disable-drain adoption_id", &self.adoption_id)?;
        validate_canonical_text(
            "disable-drain disable_operation_id",
            &self.disable_operation_id,
        )?;
        validate_digest("disable-drain request_digest", &self.request_digest)?;
        validate_digest(
            "disable-drain management_receipt_id",
            &self.management_receipt_id,
        )?;
        if self.profile_revision == 0 || self.adopted_at <= 0 {
            return Err(OmniError::manifest_internal(
                "disable-drain adoption requires a positive profile revision and timestamp",
            ));
        }
        Ok(())
    }
}

impl QuiesceRequestPayload {
    pub(crate) fn to_value(&self) -> Result<serde_json::Value> {
        self.validate_shape()?;
        serde_json::to_value(self).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode canonical stream quiesce request: {error}"
            ))
        })
    }

    pub(crate) fn request_digest(&self) -> Result<String> {
        ManagementReceipt::request_digest_for(&self.to_value()?)
    }

    fn validate_shape(&self) -> Result<()> {
        if self.protocol_version != QUIESCE_REQUEST_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(
                "stream quiesce request has an unsupported protocol version",
            ));
        }
        validate_digest("quiesce graph_identity_digest", &self.graph_identity_digest)?;
        self.identity.validate()?;
        validate_uuid("quiesce stream_incarnation_id", &self.stream_incarnation_id)?;
        validate_uuid("quiesce binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("quiesce enrollment_id", &self.enrollment_id)?;
        validate_uuid("quiesce drain_id", &self.drain_id)?;
        if self.expected_lifecycle_revision == 0 {
            return Err(OmniError::manifest_internal(
                "stream quiesce request requires a positive expected revision",
            ));
        }
        validate_digest(
            "quiesce physical_binding_digest",
            &self.physical_binding_digest,
        )?;
        self.expected_current_head_witness.validate()?;
        if self.seal_override.is_some() {
            return Err(OmniError::manifest_internal(
                "immutable fresh quiesce request payload cannot carry a seal override",
            ));
        }
        // Enforce the same bounded canonical JSON envelope as the immutable
        // management receipt which eventually embeds this request.
        bounded_json_bytes("quiesce request payload", self)?;
        Ok(())
    }

    fn validate_for_drain(
        &self,
        entry: &StreamLifecycleEntry,
        drain: &DrainDescriptor,
    ) -> Result<()> {
        self.validate_shape()?;
        validate_epoch_floors(
            &drain.expected_binding,
            &self.target_epoch_floor_by_shard,
            "quiesce requested target",
        )?;
        let expected_binding_digest = stream_physical_binding_digest(&drain.expected_binding)?;
        if self.identity != entry.identity
            || self.stream_incarnation_id != entry.enrollment_receipt.stream_incarnation_id
            || self.binding_scope_id != entry.binding_scope_id
            || self.enrollment_id != drain.expected_binding.enrollment_id
            || self.drain_id != drain.drain_id
            || self.expected_lifecycle_revision != drain.operation_expected_revision
            || self.physical_binding_digest != expected_binding_digest
        {
            return Err(OmniError::manifest_internal(
                "stream quiesce request preimage differs from its immutable lifecycle lane, occurrence, revision, or binding",
            ));
        }
        Ok(())
    }
}

impl StreamResumeRequestPayload {
    pub(crate) fn to_value(&self) -> Result<serde_json::Value> {
        self.validate_shape()?;
        serde_json::to_value(self).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode canonical stream resume request: {error}"
            ))
        })
    }

    pub(crate) fn request_digest(&self) -> Result<String> {
        ManagementReceipt::request_digest_for(&self.to_value()?)
    }

    pub(crate) fn validate_for_lifecycle(
        &self,
        lifecycle: &StreamLifecycleEntry,
        mode: StreamResumeMode,
    ) -> Result<()> {
        self.validate_shape()?;
        lifecycle.validate()?;
        if self.identity != lifecycle.identity
            || self.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
            || self.binding_scope_id != lifecycle.binding_scope_id
            || self.enrollment_id != lifecycle.binding.enrollment_id
            || self.expected_lifecycle_revision != lifecycle.lifecycle_revision
            || self.mode != mode
        {
            return Err(OmniError::manifest_internal(
                "stream resume request differs from the exact lifecycle lane, revision, or mode",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<()> {
        if self.protocol_version != STREAM_RESUME_REQUEST_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(
                "stream resume request has an unsupported protocol version",
            ));
        }
        validate_digest("resume graph_identity_digest", &self.graph_identity_digest)?;
        self.identity.validate()?;
        validate_uuid("resume stream_incarnation_id", &self.stream_incarnation_id)?;
        validate_uuid("resume binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("resume enrollment_id", &self.enrollment_id)?;
        let resume_id = validate_uuid("resume resume_id", &self.resume_id)?;
        if resume_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream resume resume_id must be a UUID v4 value",
            ));
        }
        if self.expected_lifecycle_revision == 0 {
            return Err(OmniError::manifest_internal(
                "stream resume request requires a positive expected revision",
            ));
        }
        validate_canonical_text("resume actor_id", &self.actor_id)?;
        if !self.public_named_branches.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream resume request requires the exact empty public named-branch topology",
            ));
        }
        // Enforce the same bounded canonical JSON envelope as the immutable
        // management receipt which eventually embeds this request.
        bounded_json_bytes("resume request payload", self)?;
        Ok(())
    }
}

impl StreamRebindRequestPayload {
    pub(crate) fn to_value(&self) -> Result<serde_json::Value> {
        self.validate_shape()?;
        serde_json::to_value(self).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode canonical stream rebind request: {error}"
            ))
        })
    }

    pub(crate) fn request_digest(&self) -> Result<String> {
        ManagementReceipt::request_digest_for(&self.to_value()?)
    }

    pub(crate) fn validate_for_lifecycle(
        &self,
        lifecycle: &StreamLifecycleEntry,
        expected_profile_revision: u64,
    ) -> Result<()> {
        self.validate_shape()?;
        lifecycle.validate()?;
        if lifecycle.lifecycle != StreamLifecycle::Sealed
            || self.identity != lifecycle.identity
            || self.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
            || self.binding_scope_id != lifecycle.binding_scope_id
            || self.enrollment_id != lifecycle.binding.enrollment_id
            || self.expected_lifecycle_revision != lifecycle.lifecycle_revision
            || self.expected_profile_revision != expected_profile_revision
        {
            return Err(OmniError::manifest_internal(
                "stream rebind request differs from the exact SEALED lane or disabled-profile revision",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<()> {
        if self.protocol_version != STREAM_REBIND_REQUEST_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(
                "stream rebind request has an unsupported protocol version",
            ));
        }
        validate_digest("rebind graph_identity_digest", &self.graph_identity_digest)?;
        self.identity.validate()?;
        validate_uuid("rebind stream_incarnation_id", &self.stream_incarnation_id)?;
        validate_uuid("rebind binding_scope_id", &self.binding_scope_id)?;
        validate_uuid("rebind enrollment_id", &self.enrollment_id)?;
        let rebind_id = validate_uuid("rebind rebind_id", &self.rebind_id)?;
        if rebind_id.get_version_num() != 4 {
            return Err(OmniError::manifest_internal(
                "stream rebind rebind_id must be a UUID v4 value",
            ));
        }
        if self.expected_lifecycle_revision == 0 || self.expected_profile_revision == 0 {
            return Err(OmniError::manifest_internal(
                "stream rebind request requires positive lifecycle and profile revisions",
            ));
        }
        validate_canonical_text("rebind actor_id", &self.actor_id)?;
        if !self.public_named_branches.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream rebind request requires the exact empty public named-branch topology",
            ));
        }
        bounded_json_bytes("rebind request payload", self)?;
        Ok(())
    }
}

impl DrainDescriptor {
    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
        validate_uuid("drain_id", &self.drain_id)?;
        if self.operation_expected_revision == 0
            || self.operation_expected_revision >= entry.lifecycle_revision
        {
            return Err(OmniError::manifest_internal(
                "stream drain operation_expected_revision must precede the current lifecycle revision",
            ));
        }
        validate_digest(
            "drain operation_request_digest",
            &self.operation_request_digest,
        )?;
        validate_canonical_text("drain initiating_actor", &self.initiating_actor)?;
        if self.initiated_at <= 0 {
            return Err(OmniError::manifest_internal(
                "stream drain initiated_at must be a positive timestamp",
            ));
        }
        self.expected_binding.validate(entry.identity)?;
        self.expected_current_head_witness.validate()?;
        self.operation_request_payload
            .validate_for_drain(entry, self)?;
        if self.operation_request_payload.request_digest()? != self.operation_request_digest {
            return Err(OmniError::manifest_internal(
                "stream drain request digest differs from its immutable canonical preimage",
            ));
        }
        validate_epoch_floors(
            &self.expected_binding,
            &self.target_epoch_floor_by_shard,
            "drain target",
        )?;
        for (shard_id, target_epoch) in &self.target_epoch_floor_by_shard {
            // The loop is keyed by the drain's own `expected_binding`, while
            // `entry.epoch_floor_by_shard` is keyed by the entry's `binding`.
            // The caller proves those bindings equal only *after* this
            // method returns, so a corrupt or foreign DRAINING row naming a
            // shard absent from the entry's binding must fail closed here
            // rather than panic — that state is exactly what this validator
            // exists to classify.
            let current_epoch = entry.epoch_floor_by_shard.get(shard_id).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream drain target names shard {shard_id}, which is absent from the current shard binding"
                ))
            })?;
            let requested_target = self
                .operation_request_payload
                .target_epoch_floor_by_shard
                .get(shard_id)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "stream drain requested target omits bound shard {shard_id}"
                    ))
                })?;
            let exact_target = (*requested_target).max(*current_epoch);
            if *target_epoch != exact_target {
                return Err(OmniError::manifest_internal(
                    "stream drain target epoch must equal max(requested target, current shard authority)",
                ));
            }
        }
        if self.guarded_operation.is_some() {
            return Err(OmniError::manifest_internal(
                "stream config-v3 drain guarded_operation must be null",
            ));
        }
        if let Some(adoption) = &self.seal_override {
            adoption.validate()?;
            if self.goal != DrainGoal::Sealed
                || self.operation_request_payload.goal != DrainGoal::OpenAfterFold
            {
                return Err(OmniError::manifest_internal(
                    "disable-drain adoption may only retarget an OPEN_AFTER_FOLD request to SEALED",
                ));
            }
        } else if self.goal != self.operation_request_payload.goal {
            return Err(OmniError::manifest_internal(
                "stream drain goal differs from its immutable request without a seal override",
            ));
        }
        Ok(())
    }
}

impl StrictBlock {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_data_block(
        entry: &StreamLifecycleEntry,
        cut: StreamGenerationCut,
        correction_revision: u64,
        validation_contract_version: u32,
        violation_code: String,
        violation_digest: String,
        correction_view_digest: String,
        offending_key_count: u64,
    ) -> Result<Self> {
        let evidence = StrictBlockEvidence::DataBlock {
            enrollment_id: entry.binding.enrollment_id.clone(),
            shard_id: cut.shard_id.clone(),
            generation: cut.generation,
            generation_path: cut.generation_path.clone(),
            shard_manifest_version: cut.shard_manifest_version,
            writer_epoch: cut.writer_epoch,
            replay_cursor: cut.replay_after_wal_entry_position,
            base_current_head_witness: entry.current_head_witness.clone(),
            validation_contract_version,
            violation_code,
            violation_digest,
            correction_view_digest,
            offending_key_count,
        };
        let block_token = Self::data_block_token(entry, correction_revision, &evidence)?;
        Ok(Self {
            block_token,
            correction_revision,
            evidence,
        })
    }

    fn data_block_token(
        entry: &StreamLifecycleEntry,
        correction_revision: u64,
        evidence: &StrictBlockEvidence,
    ) -> Result<String> {
        let StrictBlockEvidence::DataBlock { .. } = evidence else {
            return Err(OmniError::manifest_internal(
                "data-block token requires data-block evidence",
            ));
        };
        let evidence_bytes = bounded_json_bytes("strict data-block evidence", evidence)?;
        Ok(hash_fields(
            STRICT_DATA_BLOCK_TOKEN_DOMAIN,
            &[
                &entry.identity.stable_table_id.to_be_bytes(),
                &entry.identity.table_incarnation_id.to_be_bytes(),
                entry.binding_scope_id.as_bytes(),
                &correction_revision.to_be_bytes(),
                &evidence_bytes,
            ],
        ))
    }

    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
        validate_digest("strict block_token", &self.block_token)?;
        if self.correction_revision == 0 || self.correction_revision != entry.lifecycle_revision {
            return Err(OmniError::manifest_internal(
                "stream strict block must bind the exact current correction revision",
            ));
        }
        match &self.evidence {
            StrictBlockEvidence::DataBlock {
                enrollment_id,
                shard_id,
                generation,
                generation_path,
                shard_manifest_version,
                writer_epoch,
                replay_cursor,
                base_current_head_witness,
                validation_contract_version,
                violation_code,
                violation_digest,
                correction_view_digest,
                offending_key_count,
            } => {
                if enrollment_id != &entry.binding.enrollment_id {
                    return Err(OmniError::manifest_internal(
                        "stream strict block enrollment_id differs from the current binding",
                    ));
                }
                validate_uuid("strict block enrollment_id", enrollment_id)?;
                let validated_shard_id = validate_uuid("strict block shard_id", shard_id)?;
                if !entry
                    .binding
                    .shard_ids
                    .iter()
                    .any(|bound| bound == &validated_shard_id.to_string())
                {
                    return Err(OmniError::manifest_internal(
                        "stream strict block shard_id is not present in the current binding",
                    ));
                }
                validate_canonical_text("strict block generation_path", generation_path)?;
                base_current_head_witness.validate()?;
                let drain = entry.drain.as_ref().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream strict data block requires its active drain",
                    )
                })?;
                if *generation == 0
                    || *shard_manifest_version == 0
                    || *writer_epoch == 0
                    || *replay_cursor == 0
                    || *validation_contract_version != STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION
                    || *offending_key_count == 0
                    || *offending_key_count > STRICT_DATA_BLOCK_MAX_KEYS
                {
                    return Err(OmniError::manifest_internal(
                        "stream strict data block carries invalid cut, validation, or correction bounds",
                    ));
                }
                if entry.epoch_floor_by_shard.get(shard_id) != Some(writer_epoch) {
                    return Err(OmniError::manifest_internal(
                        "stream strict data block writer epoch differs from current shard authority",
                    ));
                }
                if drain.target_epoch_floor_by_shard.get(shard_id) != Some(writer_epoch) {
                    return Err(OmniError::manifest_internal(
                        "stream strict data block writer epoch differs from the active drain target",
                    ));
                }
                if entry.current_claim_receipt_id.is_none()
                    || *replay_cursor != entry.authenticated_wal_tail.position
                {
                    return Err(OmniError::manifest_internal(
                        "stream strict data block cut is not selected by the authenticated claim authority",
                    ));
                }
                validate_protocol_label("strict block violation_code", violation_code)?;
                validate_digest("strict block violation_digest", violation_digest)?;
                validate_digest(
                    "strict block correction_view_digest",
                    correction_view_digest,
                )?;
                let expected_token =
                    Self::data_block_token(entry, self.correction_revision, &self.evidence)?;
                if self.block_token != expected_token {
                    return Err(OmniError::manifest_internal(
                        "stream strict data-block token differs from its canonical evidence",
                    ));
                }
                let summary = entry.last_fold_summary.as_ref().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream strict data block requires its exact fold summary",
                    )
                })?;
                if summary.outcome != LastFoldOutcome::StrictBlocked
                    || summary.operation_id != drain.drain_id
                    || summary.graph_commit_id.is_some()
                    || summary.exact_generation_cut.shard_id != *shard_id
                    || summary.exact_generation_cut.generation != *generation
                    || summary.exact_generation_cut.generation_path != *generation_path
                    || summary.exact_generation_cut.shard_manifest_version
                        != *shard_manifest_version
                    || summary.exact_generation_cut.writer_epoch != *writer_epoch
                    || summary.exact_generation_cut.replay_after_wal_entry_position
                        != *replay_cursor
                    || summary.input_rows == 0
                    || summary.input_rows > STRICT_DATA_BLOCK_MAX_KEYS
                    || *offending_key_count > summary.input_rows
                    || summary.input_bytes == 0
                    || summary.input_bytes > STRICT_DATA_BLOCK_MAX_INPUT_BYTES
                    || summary.visible_rows != 0
                    || summary.visible_bytes != 0
                {
                    return Err(OmniError::manifest_internal(
                        "stream strict data block and STRICT_BLOCKED summary disagree",
                    ));
                }
                Ok(())
            }
            StrictBlockEvidence::AuthorityBlock { .. } => Err(OmniError::manifest_internal(
                "stream authority blocks are reserved but inactive in this lifecycle slice",
            )),
        }
    }

    fn base_current_head_witness(&self) -> &CurrentHeadWitness {
        match &self.evidence {
            StrictBlockEvidence::DataBlock {
                base_current_head_witness,
                ..
            } => base_current_head_witness,
            StrictBlockEvidence::AuthorityBlock {
                expected_base_current_head_witness,
                ..
            } => expected_base_current_head_witness,
        }
    }
}

impl SealedProof {
    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
        validate_uuid("sealed proof drain_id", &self.drain_id)?;
        validate_uuid("sealed proof binding_scope_id", &self.binding_scope_id)?;
        validate_digest(
            "sealed proof current_claim_receipt_id",
            &self.current_claim_receipt_id,
        )?;
        let exact_empty_generation =
            self.base_merged_generation.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream sealed proof base merged generation overflows its successor",
                )
            })?;
        if self.shard_manifest_version == 0
            || self.writer_epoch == 0
            || self.current_generation != exact_empty_generation
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof carries an invalid manifest, epoch, or generation cut",
            ));
        }
        self.base_current_head_witness.validate()?;
        if self.base_current_head_witness != entry.current_head_witness
            || self.binding_scope_id != entry.binding_scope_id
            || entry.current_claim_receipt_id.as_deref()
                != Some(self.current_claim_receipt_id.as_str())
            || self.claim_receipt_chain != entry.claim_receipt_chain
            || self.claim_receipt_chain.head_record_id.as_deref()
                != Some(self.current_claim_receipt_id.as_str())
            || self.authenticated_tail_position != entry.authenticated_wal_tail.position
            || self.authenticated_tail_segment_count != entry.authenticated_wal_tail.segment_count
            || self.authenticated_tail_chain_digest != entry.authenticated_wal_tail.chain_digest
            || entry
                .epoch_floor_by_shard
                .values()
                .any(|epoch| *epoch != self.writer_epoch)
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof does not match the current table or claim authority",
            ));
        }
        self.claim_receipt_chain
            .validate_with_domain(CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        if self.current_sentinel_position == 0
            || self.current_sentinel_position != self.authenticated_tail_position
            || self.replay_cursor > self.current_sentinel_position
        {
            return Err(OmniError::manifest_internal(
                "stream sealed proof sentinel, replay, and authenticated-tail cuts disagree",
            ));
        }
        validate_digest(
            "sealed proof authenticated_tail_chain_digest",
            &self.authenticated_tail_chain_digest,
        )?;
        validate_digest(
            "sealed proof current_sentinel_digest",
            &self.current_sentinel_digest,
        )?;
        validate_digest(
            "sealed proof verified_empty_digest",
            &self.verified_empty_digest,
        )
    }
}

impl StreamGenerationCut {
    fn validate(&self) -> Result<()> {
        validate_uuid("fold cut shard_id", &self.shard_id)?;
        validate_canonical_text("fold cut generation_path", &self.generation_path)?;
        if self.writer_epoch == 0 || self.shard_manifest_version == 0 || self.generation == 0 {
            return Err(OmniError::manifest_internal(
                "stream fold cut epoch, shard-manifest version, and generation must be non-zero",
            ));
        }
        Ok(())
    }
}

impl LastFoldSummary {
    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
        validate_canonical_text("last fold operation_id", &self.operation_id)?;
        self.exact_generation_cut.validate()?;
        if !entry
            .binding
            .shard_ids
            .contains(&self.exact_generation_cut.shard_id)
        {
            return Err(OmniError::manifest_internal(
                "stream last-fold cut shard is not present in the current binding",
            ));
        }
        if self.visible_rows > self.input_rows || self.recorded_at <= 0 {
            return Err(OmniError::manifest_internal(
                "stream last-fold row counts or timestamp are invalid",
            ));
        }
        match (self.outcome, self.graph_commit_id.as_deref()) {
            (LastFoldOutcome::Published, Some(commit_id)) => {
                validate_canonical_text("last fold graph_commit_id", commit_id)?;
            }
            (LastFoldOutcome::StrictBlocked, None)
                if self.visible_rows == 0 && self.visible_bytes == 0 => {}
            (LastFoldOutcome::Published, None) => {
                return Err(OmniError::manifest_internal(
                    "a published stream fold summary requires graph_commit_id",
                ));
            }
            (LastFoldOutcome::StrictBlocked, _) => {
                return Err(OmniError::manifest_internal(
                    "a strict-blocked stream fold must have no graph commit and zero visible output",
                ));
            }
        }
        Ok(())
    }
}

impl StreamLifecycleEntry {
    /// Build the first OPEN lifecycle-v3 row from already planned immutable
    /// binding-ledger authority. The recovery owner must create/select the
    /// named ledger receipt in the same terminal manifest CAS.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_open_enrollment(
        identity: TableIdentity,
        diagnostic_table_key: String,
        binding: StreamPhysicalBinding,
        binding_scope_id: String,
        current_head_witness: CurrentHeadWitness,
        epoch_floor_by_shard: BTreeMap<String, u64>,
        enrollment_receipt: EnrollmentReceipt,
        current_binding_receipt_id: String,
        binding_receipt_chain: ReceiptChainRef,
    ) -> Result<Self> {
        if enrollment_receipt.physical_binding != binding {
            return Err(OmniError::manifest_internal(
                "initial lifecycle binding must match its immutable enrollment provenance",
            ));
        }
        if binding_receipt_chain.head_record_id.as_deref()
            != Some(current_binding_receipt_id.as_str())
        {
            return Err(OmniError::manifest_internal(
                "initial stream lifecycle binding receipt must be the selected chain head",
            ));
        }
        let authenticated_wal_tail = AuthenticatedWalTail::genesis(binding_scope_id.clone())?;
        let entry = Self {
            identity,
            diagnostic_table_key,
            lifecycle: StreamLifecycle::Open,
            binding,
            binding_scope_id,
            current_head_witness,
            epoch_floor_by_shard,
            lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            enrollment_receipt,
            current_binding_receipt_id,
            binding_receipt_chain,
            management_receipt_chain: ReceiptChainRef::genesis_with_domain(
                MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN,
            ),
            claim_receipt_chain: ReceiptChainRef::genesis_with_domain(
                CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN,
            ),
            current_claim_receipt_id: None,
            authenticated_wal_tail,
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub(crate) fn object_id(&self) -> String {
        stream_state_object_id(self.identity)
    }

    /// Classify the three exact ways a selected DRAINING claim can prove the
    /// current generation empty.
    ///
    /// A never-flushed empty stock claim leaves Lance's replay cursor before
    /// its fence sentinel. A claim immediately following an already published
    /// fold may authenticate retained rows between the prior claim tail and
    /// that fold cut, but the exact next position is still only its new fence
    /// sentinel. A non-empty drain instead publishes the exact generation
    /// named by the latest fold summary; its empty successor then has the
    /// physical cursor at that claim's sentinel.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn selected_claim_empty_cut_disposition(
        &self,
        receipt: &ClaimReceipt,
        shard_manifest_version: u64,
        writer_epoch: u64,
        replay_cursor: u64,
        current_generation: u64,
        base_merged_generation: u64,
    ) -> Option<StreamEmptyCutDisposition> {
        let drain = self.drain.as_ref()?;
        let exact_empty_generation = base_merged_generation.checked_add(1)?;
        if self.lifecycle != StreamLifecycle::Draining
            || self.strict_block.is_some()
            || self.current_claim_receipt_id.as_deref() != Some(receipt.record_id.as_str())
            || receipt.lifecycle_operation_id.as_deref() != Some(drain.drain_id.as_str())
            || writer_epoch != receipt.achieved_writer_epoch
            || shard_manifest_version < receipt.achieved_shard_manifest_version
            || current_generation != exact_empty_generation
            || self.epoch_floor_by_shard.get(&receipt.shard_id).copied() != Some(writer_epoch)
            || drain
                .target_epoch_floor_by_shard
                .get(&receipt.shard_id)
                .copied()
                != Some(writer_epoch)
        {
            return None;
        }
        if receipt.proves_empty_current_generation() && replay_cursor == receipt.replay_cursor {
            return Some(StreamEmptyCutDisposition::DirectClaim);
        }
        let published_prefix = receipt.authenticated_tail_published_prefix_position;
        if published_prefix != 0
            && published_prefix == replay_cursor
            && published_prefix == receipt.replay_cursor
            && published_prefix.checked_add(1) == Some(receipt.sentinel_position)
        {
            return Some(StreamEmptyCutDisposition::PublishedFoldPrefix);
        }
        let summary = self.last_fold_summary.as_ref()?;
        let cut = &summary.exact_generation_cut;
        if summary.outcome == LastFoldOutcome::Published
            && summary.operation_id == drain.drain_id
            && cut.shard_id == receipt.shard_id
            && cut.writer_epoch == writer_epoch
            && cut.shard_manifest_version == shard_manifest_version
            && cut.replay_after_wal_entry_position == replay_cursor
            && cut.replay_after_wal_entry_position == receipt.sentinel_position
            && cut.generation == base_merged_generation
        {
            Some(StreamEmptyCutDisposition::PublishedDrainFold)
        } else {
            None
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.diagnostic_table_key.is_empty() {
            return Err(OmniError::manifest_internal(
                "stream lifecycle diagnostic table key must be non-empty",
            ));
        }
        self.binding.validate(self.identity)?;
        validate_uuid("lifecycle binding_scope_id", &self.binding_scope_id)?;
        self.current_head_witness.validate()?;
        validate_epoch_floors(&self.binding, &self.epoch_floor_by_shard, "current")?;
        if self.lifecycle_revision < INITIAL_LIFECYCLE_REVISION {
            return Err(OmniError::manifest_internal(
                "stream lifecycle_revision must be non-zero",
            ));
        }
        self.enrollment_receipt.validate()?;
        if self.enrollment_receipt.physical_binding.identity()? != self.identity
            || self.enrollment_receipt.initial_lifecycle_revision > self.lifecycle_revision
        {
            return Err(OmniError::manifest_internal(
                "stream enrollment provenance differs from the current lifecycle identity or revision",
            ));
        }
        validate_digest(
            "current_binding_receipt_id",
            &self.current_binding_receipt_id,
        )?;
        self.binding_receipt_chain
            .validate_with_domain(BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        if self.binding_receipt_chain.record_count == 0
            || self.binding_receipt_chain.head_record_id.as_deref()
                != Some(self.current_binding_receipt_id.as_str())
        {
            return Err(OmniError::manifest_internal(
                "stream current binding receipt must name the non-empty binding chain head",
            ));
        }
        self.management_receipt_chain
            .validate_with_domain(MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        self.claim_receipt_chain
            .validate_with_domain(CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN)?;
        match self.current_claim_receipt_id.as_deref() {
            None if self.claim_receipt_chain.record_count == 0
                && self.claim_receipt_chain.head_record_id.is_none() => {}
            Some(current_id)
                if self.claim_receipt_chain.record_count > 0
                    && self.claim_receipt_chain.head_record_id.as_deref() == Some(current_id) =>
            {
                validate_digest("current_claim_receipt_id", current_id)?;
            }
            _ => {
                return Err(OmniError::manifest_internal(
                    "stream current claim receipt must be absent with genesis or name the claim-chain head",
                ));
            }
        }
        self.authenticated_wal_tail.validate()?;
        if self.authenticated_wal_tail.binding_scope_id != self.binding_scope_id {
            return Err(OmniError::manifest_internal(
                "stream authenticated WAL tail is scoped to another physical binding",
            ));
        }

        match self.lifecycle {
            StreamLifecycle::Open => {
                if self.drain.is_some()
                    || self.strict_block.is_some()
                    || self.sealed_proof.is_some()
                {
                    return Err(OmniError::manifest_internal(
                        "OPEN stream lifecycle cannot carry drain, strict-block, or sealed-proof state",
                    ));
                }
            }
            StreamLifecycle::Draining => {
                let drain = self.drain.as_ref().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "DRAINING stream lifecycle requires one drain descriptor",
                    )
                })?;
                drain.validate(self)?;
                if drain.expected_binding != self.binding
                    || drain.expected_current_head_witness != self.current_head_witness
                    || self.sealed_proof.is_some()
                {
                    return Err(OmniError::manifest_internal(
                        "DRAINING stream authority disagrees with its current binding/HEAD or carries a sealed proof",
                    ));
                }
                if let Some(block) = &self.strict_block {
                    block.validate(self)?;
                    if block.base_current_head_witness() != &self.current_head_witness {
                        return Err(OmniError::manifest_internal(
                            "stream strict block base witness differs from current DRAINING authority",
                        ));
                    }
                }
            }
            StreamLifecycle::Sealed => {
                if self.drain.is_some() || self.strict_block.is_some() {
                    return Err(OmniError::manifest_internal(
                        "SEALED stream lifecycle cannot retain drain or strict-block state",
                    ));
                }
                self.sealed_proof
                    .as_ref()
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "SEALED stream lifecycle requires one exact empty proof",
                        )
                    })?
                    .validate(self)?;
            }
        }
        if let Some(summary) = &self.last_fold_summary {
            summary.validate(self)?;
        }
        Ok(())
    }

    /// Common revision/binding fence for lifecycle-v3 recovery envelopes.
    /// Operation-specific recovery must additionally validate its exact allowed
    /// field delta; this helper is necessary authority, never sufficient
    /// authorization for a transition.
    pub(crate) fn validate_successor_of(&self, prior: &Self) -> Result<()> {
        prior.validate()?;
        self.validate()?;
        if self.lifecycle_revision
            != prior
                .lifecycle_revision
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("stream lifecycle revision overflow"))?
            || self.identity != prior.identity
            || self.binding != prior.binding
            || self.binding_scope_id != prior.binding_scope_id
            || self.enrollment_receipt != prior.enrollment_receipt
            || self.current_binding_receipt_id != prior.current_binding_receipt_id
            || self.binding_receipt_chain != prior.binding_receipt_chain
        {
            return Err(OmniError::manifest_internal(
                "stream lifecycle successor must advance one revision while preserving exact binding authority",
            ));
        }
        if matches!(
            (prior.lifecycle, self.lifecycle),
            (StreamLifecycle::Open, StreamLifecycle::Sealed)
                | (StreamLifecycle::Sealed, StreamLifecycle::Draining)
        ) {
            return Err(OmniError::manifest_internal(
                "stream lifecycle successor skips a required drain/resume boundary",
            ));
        }
        Ok(())
    }

    /// Structural fence for the one operation that is allowed to replace a
    /// physical stream binding. Ordinary lifecycle successors deliberately
    /// continue to use [`Self::validate_successor_of`] and therefore cannot
    /// opt into this transition by changing a flag.
    pub(crate) fn validate_rebind_successor_of(&self, prior: &Self) -> Result<()> {
        prior.validate()?;
        self.validate()?;
        let expected_revision = prior
            .lifecycle_revision
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream lifecycle revision overflow"))?;
        let expected_head_version = prior
            .current_head_witness
            .table_version
            .checked_add(2)
            .ok_or_else(|| OmniError::manifest_internal("stream rebind table version overflow"))?;
        let expected_binding_receipt_count = prior
            .binding_receipt_chain
            .record_count
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream binding receipt chain overflow"))?;
        let expected_management_receipt_count = prior
            .management_receipt_chain
            .record_count
            .checked_add(1)
            .ok_or_else(|| {
                OmniError::manifest_internal("stream management receipt chain overflow")
            })?;
        let expected_claim_receipt_count = prior
            .claim_receipt_chain
            .record_count
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream claim receipt chain overflow"))?;
        let proof = self.sealed_proof.as_ref().ok_or_else(|| {
            OmniError::manifest_internal("stream rebind successor lost its SEALED proof")
        })?;
        if prior.lifecycle != StreamLifecycle::Sealed
            || self.lifecycle != StreamLifecycle::Sealed
            || self.lifecycle_revision != expected_revision
            || self.identity != prior.identity
            || self.diagnostic_table_key != prior.diagnostic_table_key
            || self.enrollment_receipt != prior.enrollment_receipt
            || self.binding.table_location != prior.binding.table_location
            || self.binding.table_branch != prior.binding.table_branch
            || self.binding.stream_config_version != prior.binding.stream_config_version
            || self.binding.stream_config_hash != prior.binding.stream_config_hash
            || self.binding == prior.binding
            || self.binding_scope_id == prior.binding_scope_id
            || self.binding.enrollment_id == prior.binding.enrollment_id
            || self
                .binding
                .shard_ids
                .iter()
                .any(|shard| prior.binding.shard_ids.contains(shard))
            || self.current_head_witness.table_version != expected_head_version
            || self.current_head_witness.transaction_uuid
                == prior.current_head_witness.transaction_uuid
            || self.binding_receipt_chain.record_count != expected_binding_receipt_count
            || self.current_binding_receipt_id == prior.current_binding_receipt_id
            || self.binding_receipt_chain.head_record_id.as_deref()
                != Some(self.current_binding_receipt_id.as_str())
            || self.management_receipt_chain.record_count != expected_management_receipt_count
            || self.management_receipt_chain.head_record_id
                == prior.management_receipt_chain.head_record_id
            || self.claim_receipt_chain.record_count != expected_claim_receipt_count
            || self.current_claim_receipt_id == prior.current_claim_receipt_id
            || self.authenticated_wal_tail.position != 1
            || self.authenticated_wal_tail.segment_count != 1
            || proof.replay_cursor != 0
            || proof.current_generation != 1
            || proof.base_merged_generation != 0
            || self.last_fold_summary.is_some()
        {
            return Err(OmniError::manifest_internal(
                "stream rebind successor must replace only the physical binding, advance its receipt chain once, reset binding-scoped fold state, and remain SEALED",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_against_registration(
        &self,
        registration: &TableRegistration,
    ) -> Result<()> {
        self.validate()?;
        if self.identity != registration.identity {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle identity {} does not match table registration {}",
                self.identity, registration.identity
            )));
        }
        if self.binding.table_location != registration.table_path {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle for identity {} binds location '{}', registered location is '{}'",
                self.identity, self.binding.table_location, registration.table_path
            )));
        }
        Ok(())
    }

    pub(super) fn to_metadata_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(&StreamStatePayload {
            protocol_version: STREAM_STATE_PROTOCOL_VERSION,
            stable_table_id: self.identity.stable_table_id,
            table_incarnation_id: self.identity.table_incarnation_id,
            lifecycle: self.lifecycle,
            binding: self.binding.clone(),
            binding_scope_id: self.binding_scope_id.clone(),
            current_head_witness: self.current_head_witness.clone(),
            epoch_floor_by_shard: self.epoch_floor_by_shard.clone(),
            lifecycle_revision: self.lifecycle_revision,
            enrollment_receipt: self.enrollment_receipt.clone(),
            current_binding_receipt_id: self.current_binding_receipt_id.clone(),
            binding_receipt_chain: self.binding_receipt_chain.clone(),
            management_receipt_chain: self.management_receipt_chain.clone(),
            claim_receipt_chain: self.claim_receipt_chain.clone(),
            current_claim_receipt_id: self.current_claim_receipt_id.clone(),
            authenticated_wal_tail: self.authenticated_wal_tail.clone(),
            drain: self.drain.clone(),
            strict_block: self.strict_block.clone(),
            sealed_proof: self.sealed_proof.clone(),
            last_fold_summary: self.last_fold_summary.clone(),
        })
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode stream lifecycle metadata: {error}"
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_manifest_row(
        object_id: &str,
        diagnostic_table_key: &str,
        row_identity: TableIdentity,
        location: Option<&str>,
        table_version: Option<u64>,
        table_branch: Option<&str>,
        metadata_json: &str,
    ) -> Result<Self> {
        let expected_object_id = stream_state_object_id(row_identity);
        if object_id != expected_object_id {
            return Err(OmniError::manifest_internal(format!(
                "manifest stream_state row has object_id '{object_id}', expected '{expected_object_id}'"
            )));
        }
        let payload: StreamStatePayload = serde_json::from_str(metadata_json).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to decode stream lifecycle metadata: {error}"
            ))
        })?;
        if payload.protocol_version != STREAM_STATE_PROTOCOL_VERSION {
            return Err(OmniError::manifest_internal(format!(
                "unsupported stream lifecycle payload version {}, expected {}",
                payload.protocol_version, STREAM_STATE_PROTOCOL_VERSION
            )));
        }
        let payload_identity =
            TableIdentity::new(payload.stable_table_id, payload.table_incarnation_id)?;
        if payload_identity != row_identity {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle payload identity {payload_identity} does not match row identity {row_identity}"
            )));
        }
        let entry = Self {
            identity: row_identity,
            diagnostic_table_key: diagnostic_table_key.to_string(),
            lifecycle: payload.lifecycle,
            binding: payload.binding,
            binding_scope_id: payload.binding_scope_id,
            current_head_witness: payload.current_head_witness,
            epoch_floor_by_shard: payload.epoch_floor_by_shard,
            lifecycle_revision: payload.lifecycle_revision,
            enrollment_receipt: payload.enrollment_receipt,
            current_binding_receipt_id: payload.current_binding_receipt_id,
            binding_receipt_chain: payload.binding_receipt_chain,
            management_receipt_chain: payload.management_receipt_chain,
            claim_receipt_chain: payload.claim_receipt_chain,
            current_claim_receipt_id: payload.current_claim_receipt_id,
            authenticated_wal_tail: payload.authenticated_wal_tail,
            drain: payload.drain,
            strict_block: payload.strict_block,
            sealed_proof: payload.sealed_proof,
            last_fold_summary: payload.last_fold_summary,
        };
        entry.validate()?;
        if location != Some(entry.binding.table_location.as_str()) {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row location does not match its physical binding",
            ));
        }
        if table_version != Some(entry.current_head_witness.table_version) {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row table_version does not match its HEAD witness",
            ));
        }
        if table_branch != entry.binding.table_branch.as_deref() {
            return Err(OmniError::manifest_internal(
                "stream lifecycle row table_branch does not match its physical binding",
            ));
        }
        Ok(entry)
    }
}

/// Cluster-independent graph identity for lane-scoped receipt keys. The
/// accepted schema identity domain is graph-lifetime authority and survives
/// store relocation or cluster-declaration changes.
pub(crate) fn stream_graph_identity_digest(schema_identity_domain: &str) -> Result<String> {
    validate_canonical_text("graph schema_identity_domain", schema_identity_domain)?;
    Ok(hash_fields(
        b"omnigraph.stream-graph-identity.v1\0",
        &[schema_identity_domain.as_bytes()],
    ))
}

/// Canonical digest of the caller intent fixed before enrollment has any
/// physical effect. Engine-minted enrollment, shard, and stream-incarnation
/// identities are results and deliberately do not participate. Every text
/// field is length-prefixed and every integer is fixed-width big endian.
#[allow(clippy::too_many_arguments)]
pub(crate) fn stream_enrollment_intent_digest_v1(
    identity: TableIdentity,
    table_location: &str,
    schema_identity_domain: &str,
    schema_ir_hash: &str,
    schema_identity_version: u32,
    expected_unenrolled_head: &CurrentHeadWitness,
    stream_config_hash: &str,
) -> Result<String> {
    identity.validate()?;
    validate_canonical_text("enrollment intent table_location", table_location)?;
    validate_canonical_text(
        "enrollment intent schema_identity_domain",
        schema_identity_domain,
    )?;
    validate_canonical_text("enrollment intent schema_ir_hash", schema_ir_hash)?;
    if schema_identity_version == 0 {
        return Err(OmniError::manifest_internal(
            "stream enrollment intent schema_identity_version must be non-zero",
        ));
    }
    expected_unenrolled_head.validate()?;
    validate_digest("enrollment intent stream_config_hash", stream_config_hash)?;

    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, b"omnigraph.stream-enrollment-intent.v1");
    hasher.update(identity.stable_table_id.to_be_bytes());
    hasher.update(identity.table_incarnation_id.to_be_bytes());
    hash_bytes(&mut hasher, table_location.as_bytes());
    // Config-v3 currently permits canonical main only; bind that fact rather
    // than serializing Lance's implementation-owned branch identifier.
    hash_bytes(&mut hasher, b"main");
    hash_bytes(&mut hasher, schema_identity_domain.as_bytes());
    hash_bytes(&mut hasher, schema_ir_hash.as_bytes());
    hasher.update(schema_identity_version.to_be_bytes());
    hasher.update(expected_unenrolled_head.table_version.to_be_bytes());
    hash_bytes(
        &mut hasher,
        expected_unenrolled_head.transaction_uuid.as_bytes(),
    );
    match expected_unenrolled_head.manifest_e_tag.as_deref() {
        Some(e_tag) => {
            hasher.update([1]);
            hash_bytes(&mut hasher, e_tag.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(STREAM_CONFIG_VERSION.to_be_bytes());
    hash_bytes(&mut hasher, stream_config_hash.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hash_fields(domain: &[u8], fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, domain);
    for field in fields {
        hash_bytes(&mut hasher, field);
    }
    format!("sha256:{:x}", hasher.finalize())
}

/// Commit to one canonical, non-empty set of UUID-v4 shard prefixes. Sorting
/// and duplicate rejection make the digest independent of caller order while
/// preserving exact set cardinality.
pub(crate) fn retained_shard_inventory_commitment(
    shards: &[ShardId],
) -> Result<RetainedShardInventoryCommitment> {
    if shards.is_empty() {
        return Err(OmniError::manifest_internal(
            "stream retained-shard inventory must be non-empty",
        ));
    }
    let mut canonical = shards.to_vec();
    canonical.sort_unstable();
    if canonical.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(OmniError::manifest_internal(
            "stream retained-shard inventory must be duplicate-free",
        ));
    }
    if canonical
        .iter()
        .any(|shard| shard.is_nil() || shard.get_version_num() != 4)
    {
        return Err(OmniError::manifest_internal(
            "stream retained-shard inventory must contain only UUID-v4 identities",
        ));
    }
    let retained_shard_count = u64::try_from(canonical.len()).map_err(|_| {
        OmniError::manifest_internal("stream retained-shard inventory count overflow")
    })?;
    let fields = canonical
        .iter()
        .map(|shard| shard.as_bytes().as_slice())
        .collect::<Vec<_>>();
    Ok(RetainedShardInventoryCommitment {
        retained_shard_count,
        retained_shard_set_digest: hash_fields(RETAINED_SHARD_SET_DIGEST_DOMAIN, &fields),
    })
}

fn digest_domain(domain: &[u8]) -> String {
    hash_fields(domain, &[])
}

pub(crate) fn binding_receipt_chain_genesis() -> ReceiptChainRef {
    ReceiptChainRef::genesis_with_domain(BINDING_RECEIPT_CHAIN_GENESIS_DOMAIN)
}

/// Minimal non-empty enrollment-chain stand-in for in-source fixtures that do
/// not persist a token ledger. Production initial bindings always use the
/// actual `EnrollmentReceiptV2::next_chain_ref()`.
#[cfg(test)]
pub(crate) fn test_initial_binding_prior_chain() -> ReceiptChainRef {
    ReceiptChainRef {
        head_record_id: Some(hash_fields(
            b"omnigraph.test-stream-enrollment-record.v1\0",
            &[],
        )),
        record_count: 1,
        chain_digest: hash_fields(b"omnigraph.test-stream-enrollment-chain.v1\0", &[]),
    }
}

pub(crate) fn management_receipt_chain_genesis() -> ReceiptChainRef {
    ReceiptChainRef::genesis_with_domain(MANAGEMENT_RECEIPT_CHAIN_GENESIS_DOMAIN)
}

pub(crate) fn claim_receipt_chain_genesis() -> ReceiptChainRef {
    ReceiptChainRef::genesis_with_domain(CLAIM_RECEIPT_CHAIN_GENESIS_DOMAIN)
}

pub(crate) fn claim_attempt_chain_genesis() -> ReceiptChainRef {
    ReceiptChainRef::genesis_with_domain(CLAIM_ATTEMPT_CHAIN_GENESIS_DOMAIN)
}

pub(crate) fn stream_physical_binding_digest(binding: &StreamPhysicalBinding) -> Result<String> {
    let bytes = bounded_json_bytes("stream physical binding", binding)?;
    Ok(hash_fields(
        b"omnigraph.stream-physical-binding.v1\0",
        &[&bytes],
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn authenticated_wal_tail_chain_digest(
    binding_scope_id: &str,
    enrollment_id: &str,
    shard_id: &str,
    stream_incarnation_id: &str,
    stream_configuration_digest: &str,
    physical_binding_digest: &str,
    prior_position: u64,
    position: u64,
    segment_entry_count: u64,
    segment_digest: &str,
    prior_chain_digest: &str,
    segment_count: u64,
    empty_fence_state_digest: &str,
    lww_projection_digest: &str,
) -> Result<String> {
    validate_uuid("authenticated tail binding_scope_id", binding_scope_id)?;
    validate_uuid("authenticated tail enrollment_id", enrollment_id)?;
    validate_uuid("authenticated tail shard_id", shard_id)?;
    validate_uuid(
        "authenticated tail stream_incarnation_id",
        stream_incarnation_id,
    )?;
    for (field, digest) in [
        ("stream_configuration_digest", stream_configuration_digest),
        ("physical_binding_digest", physical_binding_digest),
        ("segment_digest", segment_digest),
        ("prior_chain_digest", prior_chain_digest),
        ("empty_fence_state_digest", empty_fence_state_digest),
        ("lww_projection_digest", lww_projection_digest),
    ] {
        validate_digest(field, digest)?;
    }
    if position <= prior_position || segment_entry_count == 0 || segment_count == 0 {
        return Err(OmniError::manifest_internal(
            "authenticated WAL-tail segment must advance position with positive entry and segment counts",
        ));
    }
    if segment_count == 1
        && (prior_position != 0
            || prior_chain_digest != digest_domain(AUTHENTICATED_WAL_TAIL_CHAIN_GENESIS_DOMAIN))
    {
        return Err(OmniError::manifest_internal(
            "first authenticated WAL-tail segment must extend the exact genesis at position zero",
        ));
    }
    Ok(hash_fields(
        AUTHENTICATED_WAL_TAIL_CHAIN_STEP_DOMAIN,
        &[
            binding_scope_id.as_bytes(),
            enrollment_id.as_bytes(),
            shard_id.as_bytes(),
            stream_incarnation_id.as_bytes(),
            stream_configuration_digest.as_bytes(),
            physical_binding_digest.as_bytes(),
            &prior_position.to_be_bytes(),
            &position.to_be_bytes(),
            &segment_entry_count.to_be_bytes(),
            segment_digest.as_bytes(),
            prior_chain_digest.as_bytes(),
            &segment_count.to_be_bytes(),
            empty_fence_state_digest.as_bytes(),
            lww_projection_digest.as_bytes(),
        ],
    ))
}

fn next_chain_ordinal(prior: &ReceiptChainRef, context: &str) -> Result<u64> {
    prior
        .record_count
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal(format!("stream {context} chain overflow")))
}

fn receipt_chain_step_digest(
    record_tag: &str,
    prior_chain_digest: &str,
    ordinal: u64,
    record_id: &str,
) -> String {
    hash_fields(
        RECEIPT_CHAIN_STEP_DOMAIN,
        &[
            record_tag.as_bytes(),
            prior_chain_digest.as_bytes(),
            &ordinal.to_be_bytes(),
            record_id.as_bytes(),
        ],
    )
}

fn receipt_next_chain_ref(
    ordinal: u64,
    record_id: &str,
    resulting_chain_digest: &str,
) -> Result<ReceiptChainRef> {
    validate_digest("receipt record_id", record_id)?;
    validate_digest("receipt resulting_chain_digest", resulting_chain_digest)?;
    if ordinal == 0 {
        return Err(OmniError::manifest_internal(
            "stream receipt chain ordinal must be positive",
        ));
    }
    Ok(ReceiptChainRef {
        head_record_id: Some(record_id.to_string()),
        record_count: ordinal,
        chain_digest: resulting_chain_digest.to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_receipt_envelope(
    record_id: &str,
    record_lookup_key: &str,
    record_tag: &str,
    chain_ordinal: u64,
    predecessor_record_id: Option<&str>,
    prior_chain_digest: &str,
    resulting_chain_digest: &str,
    genesis_domain: &[u8],
    expected_lookup_key: &str,
    expected_record_id: &str,
) -> Result<()> {
    validate_digest("receipt record_id", record_id)?;
    validate_digest("receipt prior_chain_digest", prior_chain_digest)?;
    validate_digest("receipt resulting_chain_digest", resulting_chain_digest)?;
    if chain_ordinal == 0 {
        return Err(OmniError::manifest_internal(
            "stream receipt chain ordinal must be positive",
        ));
    }
    let expected_genesis = ReceiptChainRef::genesis_with_domain(genesis_domain);
    match (chain_ordinal, predecessor_record_id) {
        (1, None) if prior_chain_digest == expected_genesis.chain_digest => {}
        (1, _) => {
            return Err(OmniError::manifest_internal(
                "first stream receipt must bind its exact chain genesis and have no predecessor",
            ));
        }
        (_, Some(predecessor)) => validate_digest("receipt predecessor_record_id", predecessor)?,
        (_, None) => {
            return Err(OmniError::manifest_internal(
                "non-first stream receipt requires a predecessor record id",
            ));
        }
    }
    let expected_chain =
        receipt_chain_step_digest(record_tag, prior_chain_digest, chain_ordinal, record_id);
    if record_lookup_key != expected_lookup_key
        || record_id != expected_record_id
        || resulting_chain_digest != expected_chain
    {
        return Err(OmniError::manifest_internal(
            "stream receipt differs from its canonical lookup, record, or chain identity",
        ));
    }
    Ok(())
}

fn validate_attempt_envelope(
    attempt: &ClaimAttemptEffect,
    expected_lookup_key: &str,
    expected_record_id: &str,
) -> Result<()> {
    validate_digest("claim-attempt record_id", &attempt.record_id)?;
    validate_digest(
        "claim-attempt prior chain digest",
        &attempt.prior_attempt_chain_digest,
    )?;
    validate_digest(
        "claim-attempt resulting chain digest",
        &attempt.resulting_attempt_chain_digest,
    )?;
    let expected_genesis = ReceiptChainRef::genesis_with_domain(CLAIM_ATTEMPT_CHAIN_GENESIS_DOMAIN);
    match (attempt.ordinal, attempt.predecessor_record_id.as_deref()) {
        (1, None) if attempt.prior_attempt_chain_digest == expected_genesis.chain_digest => {}
        (1, _) => {
            return Err(OmniError::manifest_internal(
                "first claim attempt must bind the exact genesis and have no predecessor",
            ));
        }
        (0, _) => {
            return Err(OmniError::manifest_internal(
                "claim-attempt ordinal must be positive",
            ));
        }
        (_, Some(predecessor)) => {
            validate_digest("claim-attempt predecessor_record_id", predecessor)?
        }
        (_, None) => {
            return Err(OmniError::manifest_internal(
                "non-first claim attempt requires a predecessor record id",
            ));
        }
    }
    let expected_chain = hash_fields(
        CLAIM_ATTEMPT_CHAIN_DOMAIN,
        &[
            attempt.prior_attempt_chain_digest.as_bytes(),
            &attempt.ordinal.to_be_bytes(),
            attempt.record_id.as_bytes(),
        ],
    );
    if attempt.record_lookup_key != expected_lookup_key
        || attempt.record_id != expected_record_id
        || attempt.resulting_attempt_chain_digest != expected_chain
    {
        return Err(OmniError::manifest_internal(
            "stream claim-attempt differs from its canonical lookup, record, or chain identity",
        ));
    }
    Ok(())
}

fn bounded_json_bytes<T: Serialize>(field: &str, value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode stream {field}: {error}"))
    })?;
    if bytes.len() > MAX_RECEIPT_JSON_BYTES {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} exceeds the {MAX_RECEIPT_JSON_BYTES}-byte receipt bound"
        )));
    }
    Ok(bytes)
}

fn canonical_json_digest(domain: &[u8], field: &str, value: &serde_json::Value) -> Result<String> {
    if !value.is_object() {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be a JSON object"
        )));
    }
    let mut bytes = Vec::new();
    write_canonical_json(field, value, &mut bytes)?;
    Ok(hash_fields(domain, &[&bytes]))
}

fn canonical_json_uuid(domain: &[u8], field: &str, value: &serde_json::Value) -> Result<String> {
    if !value.is_object() {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be a JSON object"
        )));
    }
    let mut canonical = Vec::new();
    write_canonical_json(field, value, &mut canonical)?;
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, domain);
    hash_bytes(&mut hasher, &canonical);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(ShardId::from_bytes(bytes).to_string())
}

/// Encode the receipt JSON contract independently of `serde_json::Map`'s
/// backing container: object keys sort by their UTF-8 bytes, arrays retain
/// order, and scalar spellings remain serde_json's stable JSON spellings.
fn write_canonical_json(field: &str, value: &serde_json::Value, bytes: &mut Vec<u8>) -> Result<()> {
    match value {
        serde_json::Value::Null => bytes.extend_from_slice(b"null"),
        serde_json::Value::Bool(value) => {
            bytes.extend_from_slice(if *value { b"true" } else { b"false" })
        }
        serde_json::Value::Number(value) => {
            serde_json::to_writer(&mut *bytes, value).map_err(|error| {
                OmniError::manifest_internal(format!("failed to encode stream {field}: {error}"))
            })?;
        }
        serde_json::Value::String(value) => {
            serde_json::to_writer(&mut *bytes, value).map_err(|error| {
                OmniError::manifest_internal(format!("failed to encode stream {field}: {error}"))
            })?;
        }
        serde_json::Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                write_canonical_json(field, value, bytes)?;
            }
            bytes.push(b']');
        }
        serde_json::Value::Object(values) => {
            bytes.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                serde_json::to_writer(&mut *bytes, key).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to encode stream {field}: {error}"
                    ))
                })?;
                bytes.push(b':');
                write_canonical_json(field, value, bytes)?;
            }
            bytes.push(b'}');
        }
    }
    if bytes.len() > MAX_RECEIPT_JSON_BYTES {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} exceeds the {MAX_RECEIPT_JSON_BYTES}-byte receipt bound"
        )));
    }
    Ok(())
}

/// `Option<T>` normally treats an absent serde field as `None`. State-v2 needs
/// every nullable slot to be physically present so a truncated or older row is
/// never reinterpreted by a default. Shared with the sibling stream-profile
/// and stream-token payloads, which follow the same house rule.
pub(super) fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn validate_epoch_floors(
    binding: &StreamPhysicalBinding,
    floors: &BTreeMap<String, u64>,
    context: &str,
) -> Result<()> {
    if floors.len() != binding.shard_ids.len() {
        return Err(OmniError::manifest_internal(format!(
            "stream {context} epoch-floor keys must exactly match the physical shard binding"
        )));
    }
    for shard_id in &binding.shard_ids {
        let epoch = floors.get(shard_id).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream {context} epoch floor missing bound shard {shard_id}"
            ))
        })?;
        if *epoch == 0 {
            return Err(OmniError::manifest_internal(format!(
                "stream {context} epoch floor for shard {shard_id} must be non-zero"
            )));
        }
    }
    Ok(())
}

/// Build the smallest lifecycle-v3-valid SEALED authority used by manifest and
/// branch-control unit fixtures. Production lifecycle management remains
/// inactive; keeping this constructor test-only prevents fixtures from
/// bypassing the exact claim and empty-proof requirements by mutating only the
/// lifecycle enum.
#[cfg(test)]
pub(crate) fn test_sealed_lifecycle_from(
    current: &StreamLifecycleEntry,
) -> Result<StreamLifecycleEntry> {
    let claim_id = format!("sha256:{}", "7".repeat(64));
    let sentinel_digest = format!("sha256:{}", "c".repeat(64));

    let mut sealed = current.clone();
    sealed.lifecycle = StreamLifecycle::Sealed;
    sealed.lifecycle_revision = sealed
        .lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("test sealed lifecycle revision overflow"))?;
    sealed.claim_receipt_chain = ReceiptChainRef {
        head_record_id: Some(claim_id.clone()),
        record_count: 1,
        chain_digest: format!("sha256:{}", "8".repeat(64)),
    };
    sealed.current_claim_receipt_id = Some(claim_id.clone());
    sealed.authenticated_wal_tail = AuthenticatedWalTail {
        binding_scope_id: sealed.binding_scope_id.clone(),
        position: 1,
        segment_count: 1,
        chain_digest: format!("sha256:{}", "9".repeat(64)),
        lww_projection_digest: format!("sha256:{}", "a".repeat(64)),
    };
    for epoch in sealed.epoch_floor_by_shard.values_mut() {
        *epoch = 1;
    }
    let drain_id = sealed
        .drain
        .as_ref()
        .map(|drain| drain.drain_id.clone())
        .unwrap_or_else(|| "99999999-9999-4999-8999-999999999999".to_string());
    sealed.drain = None;
    sealed.strict_block = None;
    sealed.sealed_proof = Some(SealedProof {
        drain_id,
        binding_scope_id: sealed.binding_scope_id.clone(),
        shard_manifest_version: 2,
        writer_epoch: 1,
        replay_cursor: 1,
        current_generation: 1,
        base_merged_generation: 0,
        base_current_head_witness: sealed.current_head_witness.clone(),
        current_claim_receipt_id: claim_id,
        claim_receipt_chain: sealed.claim_receipt_chain.clone(),
        authenticated_tail_position: 1,
        authenticated_tail_segment_count: 1,
        authenticated_tail_chain_digest: sealed.authenticated_wal_tail.chain_digest.clone(),
        current_sentinel_position: 1,
        current_sentinel_digest: sentinel_digest,
        verified_empty_digest: format!("sha256:{}", "b".repeat(64)),
    });
    sealed.validate()?;
    Ok(sealed)
}

fn validate_digest(field: &str, value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must use canonical sha256:<lowercase-hex> form"
        )));
    };
    if digest.len() != 64
        || !digest
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
    if parsed.is_nil() {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must be non-nil"
        )));
    }
    if parsed.to_string() != value {
        return Err(OmniError::manifest_internal(format!(
            "stream {field} must use canonical lowercase hyphenated UUID text"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disable_plan_derives_stable_lane_occurrences() {
        let identity = TableIdentity::new(7, 9).unwrap();
        let drain = stream_disable_drain_id("disable-operation", identity).unwrap();
        assert_eq!(
            stream_disable_drain_id("disable-operation", identity).unwrap(),
            drain
        );
        assert_ne!(
            stream_disable_drain_id("disable-operation", TableIdentity::new(7, 10).unwrap())
                .unwrap(),
            drain
        );
        let parsed = ShardId::parse_str(&drain).unwrap();
        assert_eq!(parsed.to_string(), drain);

        let adoption =
            stream_disable_drain_adoption_id("disable-operation", identity, &drain, 3).unwrap();
        assert_eq!(
            stream_disable_drain_adoption_id("disable-operation", identity, &drain, 3).unwrap(),
            adoption
        );
        assert_ne!(
            stream_disable_drain_adoption_id("disable-operation", identity, &drain, 4).unwrap(),
            adoption
        );
        let operation_id = stream_disable_drain_adoption_operation_id(
            "disable-operation",
            identity,
            &drain,
            3,
        )
        .unwrap();
        assert_eq!(
            stream_disable_drain_adoption_operation_id(
                "disable-operation",
                identity,
                &drain,
                3,
            )
            .unwrap(),
            operation_id
        );
        assert_ne!(
            stream_disable_drain_adoption_operation_id(
                "disable-operation",
                identity,
                &drain,
                4,
            )
            .unwrap(),
            operation_id
        );
        let parsed = ShardId::parse_str(&operation_id).unwrap();
        assert_eq!(parsed.to_string(), operation_id);
    }

    fn entry() -> StreamLifecycleEntry {
        let shard_id = "22222222-2222-4222-8222-222222222222".to_string();
        let binding = StreamPhysicalBinding {
            stable_table_id: 7,
            table_incarnation_id: 9,
            table_location: "nodes/0000000000000007-0000000000000009".to_string(),
            table_branch: None,
            enrollment_id: "11111111-1111-4111-8111-111111111111".to_string(),
            shard_ids: vec![shard_id.clone()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: format!("sha256:{}", "a".repeat(64)),
        };
        let binding_scope_id = "44444444-4444-4444-8444-444444444444".to_string();
        let current_binding_receipt_id = format!("sha256:{}", "b".repeat(64));
        let enrollment_receipt = EnrollmentReceipt::new(
            "66666666-6666-4666-8666-666666666666".to_string(),
            format!("sha256:{}", "d".repeat(64)),
            "55555555-5555-4555-8555-555555555555".to_string(),
            binding.clone(),
        )
        .unwrap();
        StreamLifecycleEntry {
            identity: TableIdentity::new(7, 9).unwrap(),
            diagnostic_table_key: "node:Person".to_string(),
            lifecycle: StreamLifecycle::Open,
            binding,
            binding_scope_id: binding_scope_id.clone(),
            current_head_witness: CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 4,
                transaction_uuid: "33333333-3333-4333-8333-333333333333".to_string(),
                manifest_e_tag: None,
            },
            epoch_floor_by_shard: BTreeMap::from([(shard_id, 1)]),
            lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            enrollment_receipt,
            current_binding_receipt_id: current_binding_receipt_id.clone(),
            binding_receipt_chain: ReceiptChainRef {
                head_record_id: Some(current_binding_receipt_id),
                record_count: 1,
                chain_digest: format!("sha256:{}", "c".repeat(64)),
            },
            management_receipt_chain: management_receipt_chain_genesis(),
            claim_receipt_chain: claim_receipt_chain_genesis(),
            current_claim_receipt_id: None,
            authenticated_wal_tail: AuthenticatedWalTail::genesis(binding_scope_id).unwrap(),
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        }
    }

    fn strict_blocked_entry() -> StreamLifecycleEntry {
        let mut blocked = entry();
        let shard_id = blocked.binding.shard_ids[0].clone();
        let drain_id = "77777777-7777-4777-8777-777777777777".to_string();
        let target_epoch_floor_by_shard = BTreeMap::from([(shard_id.clone(), 2)]);
        let request = QuiesceRequestPayload {
            protocol_version: QUIESCE_REQUEST_PROTOCOL_VERSION,
            graph_identity_digest: format!("sha256:{}", "1".repeat(64)),
            identity: blocked.identity,
            stream_incarnation_id: blocked.enrollment_receipt.stream_incarnation_id.clone(),
            binding_scope_id: blocked.binding_scope_id.clone(),
            enrollment_id: blocked.binding.enrollment_id.clone(),
            drain_id: drain_id.clone(),
            expected_lifecycle_revision: blocked.lifecycle_revision,
            goal: DrainGoal::Sealed,
            physical_binding_digest: stream_physical_binding_digest(&blocked.binding).unwrap(),
            expected_current_head_witness: blocked.current_head_witness.clone(),
            target_epoch_floor_by_shard: target_epoch_floor_by_shard.clone(),
            seal_override: None,
        };
        blocked.lifecycle = StreamLifecycle::Draining;
        blocked.lifecycle_revision = 3;
        blocked.epoch_floor_by_shard = target_epoch_floor_by_shard.clone();
        blocked.drain = Some(DrainDescriptor {
            drain_id: drain_id.clone(),
            operation_expected_revision: request.expected_lifecycle_revision,
            operation_request_digest: request.request_digest().unwrap(),
            goal: DrainGoal::Sealed,
            initiating_actor: "test-operator".to_string(),
            initiated_at: 1,
            expected_binding: blocked.binding.clone(),
            expected_current_head_witness: blocked.current_head_witness.clone(),
            operation_request_payload: request,
            target_epoch_floor_by_shard,
            guarded_operation: None,
            seal_override: None,
        });
        let claim_id = format!("sha256:{}", "2".repeat(64));
        blocked.claim_receipt_chain = ReceiptChainRef {
            head_record_id: Some(claim_id.clone()),
            record_count: 1,
            chain_digest: format!("sha256:{}", "3".repeat(64)),
        };
        blocked.current_claim_receipt_id = Some(claim_id);
        blocked.authenticated_wal_tail = AuthenticatedWalTail {
            binding_scope_id: blocked.binding_scope_id.clone(),
            position: 7,
            segment_count: 1,
            chain_digest: format!("sha256:{}", "4".repeat(64)),
            lww_projection_digest: format!("sha256:{}", "5".repeat(64)),
        };
        let cut = StreamGenerationCut {
            shard_id,
            writer_epoch: 2,
            shard_manifest_version: 2,
            replay_after_wal_entry_position: 7,
            generation: 1,
            generation_path: "_mem_wal/generation-1".to_string(),
        };
        blocked.strict_block = Some(
            StrictBlock::new_data_block(
                &blocked,
                cut.clone(),
                blocked.lifecycle_revision,
                STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION,
                "UNIQUE_VIOLATION".to_string(),
                format!("sha256:{}", "6".repeat(64)),
                format!("sha256:{}", "7".repeat(64)),
                1,
            )
            .unwrap(),
        );
        blocked.last_fold_summary = Some(LastFoldSummary {
            operation_id: drain_id,
            graph_commit_id: None,
            exact_generation_cut: cut,
            outcome: LastFoldOutcome::StrictBlocked,
            input_rows: 1,
            input_bytes: 1,
            visible_rows: 0,
            visible_bytes: 0,
            recorded_at: 1,
        });
        blocked.validate().unwrap();
        blocked
    }

    fn retoken_data_block(entry: &mut StreamLifecycleEntry) {
        let correction_revision = entry.lifecycle_revision;
        let evidence = entry
            .strict_block
            .as_ref()
            .expect("fixture has strict block")
            .evidence
            .clone();
        let block_token =
            StrictBlock::data_block_token(entry, correction_revision, &evidence).unwrap();
        entry
            .strict_block
            .as_mut()
            .expect("fixture has strict block")
            .block_token = block_token;
    }

    #[test]
    fn lifecycle_payload_round_trips_deterministically_without_alias_authority() {
        let entry = entry();
        let json = entry.to_metadata_json().unwrap();
        assert_eq!(json, entry.to_metadata_json().unwrap());
        let decoded = StreamLifecycleEntry::from_manifest_row(
            &entry.object_id(),
            "node:RenamedPerson",
            entry.identity,
            Some(&entry.binding.table_location),
            Some(entry.current_head_witness.table_version),
            None,
            &json,
        )
        .unwrap();
        assert_eq!(decoded.identity, entry.identity);
        assert_eq!(decoded.diagnostic_table_key, "node:RenamedPerson");
        assert_eq!(decoded.binding, entry.binding);
        assert_eq!(decoded.lifecycle_revision, INITIAL_LIFECYCLE_REVISION);
        assert_eq!(
            decoded.current_binding_receipt_id,
            entry.current_binding_receipt_id
        );

        assert!(
            StreamLifecycleEntry::from_manifest_row(
                "stream_state:wrong",
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version),
                None,
                &json,
            )
            .is_err()
        );
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &entry.object_id(),
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version + 1),
                None,
                &json,
            )
            .is_err()
        );
    }

    #[test]
    fn lifecycle_v3_serializes_bounded_authority_and_rejects_omission_or_v2() {
        let entry = entry();
        let json = entry.to_metadata_json().unwrap();
        let payload: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(payload["protocol_version"], 3);
        assert_eq!(payload["lifecycle_revision"], 1);
        assert_eq!(payload["management_receipt_chain"]["record_count"], 0);
        assert_eq!(payload["claim_receipt_chain"]["record_count"], 0);
        assert!(payload.get("management_receipts").is_none());
        assert!(payload.get("claim_receipts").is_none());
        assert_eq!(payload["authenticated_wal_tail"]["position"], 0);
        for field in [
            "current_claim_receipt_id",
            "drain",
            "strict_block",
            "sealed_proof",
            "last_fold_summary",
        ] {
            assert!(
                payload.get(field).is_some(),
                "missing lifecycle-v3 slot {field}"
            );
            assert!(
                payload[field].is_null(),
                "lifecycle-v3 slot {field} is not null"
            );
        }

        let mut missing_slot = payload.clone();
        missing_slot.as_object_mut().unwrap().remove("drain");
        let missing_slot = serde_json::to_string(&missing_slot).unwrap();
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &entry.object_id(),
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version),
                None,
                &missing_slot,
            )
            .is_err(),
            "a nullable lifecycle-v3 field must be present explicitly rather than supplied by serde"
        );

        let mut v2 = payload;
        v2["protocol_version"] = serde_json::json!(2);
        let v2 = serde_json::to_string(&v2).unwrap();
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &entry.object_id(),
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version),
                None,
                &v2,
            )
            .is_err()
        );
    }

    #[test]
    fn strict_block_evidence_is_tagged_and_rejects_unknown_nested_fields() {
        let entry = entry();
        let evidence = StrictBlockEvidence::DataBlock {
            enrollment_id: entry.binding.enrollment_id.clone(),
            shard_id: entry.binding.shard_ids[0].clone(),
            generation: 1,
            generation_path: "_mem_wal/generation-1".to_string(),
            shard_manifest_version: 2,
            writer_epoch: 1,
            replay_cursor: 1,
            base_current_head_witness: entry.current_head_witness,
            validation_contract_version: 1,
            violation_code: "UNIQUE_VIOLATION".to_string(),
            violation_digest: format!("sha256:{}", "1".repeat(64)),
            correction_view_digest: format!("sha256:{}", "2".repeat(64)),
            offending_key_count: 1,
        };
        let mut value = serde_json::to_value(&evidence).unwrap();
        assert_eq!(value["kind"], "DATA_BLOCK");
        value["unexpected_nested_authority"] = serde_json::json!(true);
        assert!(
            serde_json::from_value::<StrictBlockEvidence>(value).is_err(),
            "lifecycle-v3 block evidence must be a closed fail-closed shape"
        );
    }

    #[test]
    fn strict_data_block_requires_the_exact_selected_claim_cut_and_contract() {
        let valid = strict_blocked_entry();
        valid.validate().unwrap();

        let mut wrong_contract = valid.clone();
        let StrictBlockEvidence::DataBlock {
            validation_contract_version,
            ..
        } = &mut wrong_contract
            .strict_block
            .as_mut()
            .expect("fixture has strict block")
            .evidence
        else {
            panic!("fixture has data block");
        };
        *validation_contract_version =
            STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION.saturating_add(1);
        retoken_data_block(&mut wrong_contract);
        assert!(wrong_contract.validate().is_err());

        let mut no_selected_claim = valid.clone();
        no_selected_claim.current_claim_receipt_id = None;
        no_selected_claim.claim_receipt_chain = claim_receipt_chain_genesis();
        retoken_data_block(&mut no_selected_claim);
        assert!(no_selected_claim.validate().is_err());

        let mut wrong_replay_cursor = valid.clone();
        let StrictBlockEvidence::DataBlock { replay_cursor, .. } = &mut wrong_replay_cursor
            .strict_block
            .as_mut()
            .expect("fixture has strict block")
            .evidence
        else {
            panic!("fixture has data block");
        };
        *replay_cursor = 6;
        wrong_replay_cursor
            .last_fold_summary
            .as_mut()
            .expect("fixture has fold summary")
            .exact_generation_cut
            .replay_after_wal_entry_position = 6;
        retoken_data_block(&mut wrong_replay_cursor);
        assert!(wrong_replay_cursor.validate().is_err());

        let mut wrong_writer_epoch = valid;
        let StrictBlockEvidence::DataBlock { writer_epoch, .. } = &mut wrong_writer_epoch
            .strict_block
            .as_mut()
            .expect("fixture has strict block")
            .evidence
        else {
            panic!("fixture has data block");
        };
        *writer_epoch = 3;
        wrong_writer_epoch
            .last_fold_summary
            .as_mut()
            .expect("fixture has fold summary")
            .exact_generation_cut
            .writer_epoch = 3;
        retoken_data_block(&mut wrong_writer_epoch);
        assert!(wrong_writer_epoch.validate().is_err());
    }

    #[test]
    fn lifecycle_v3_open_requires_bounded_binding_authority_and_null_lifecycle_slots() {
        let mut missing_binding_receipt = entry();
        missing_binding_receipt.binding_receipt_chain = binding_receipt_chain_genesis();
        assert!(missing_binding_receipt.validate().is_err());

        let mut zero_revision = entry();
        zero_revision.lifecycle_revision = 0;
        assert!(zero_revision.validate().is_err());

        let mut draining_without_descriptor = entry();
        draining_without_descriptor.lifecycle = StreamLifecycle::Draining;
        assert!(draining_without_descriptor.validate().is_err());

        let mut sealed_without_proof = entry();
        sealed_without_proof.lifecycle = StreamLifecycle::Sealed;
        assert!(sealed_without_proof.validate().is_err());
    }

    #[test]
    fn binding_receipt_inventory_is_canonical_contextual_and_freezes_ordinal_two() {
        let base = entry();
        let graph_digest = format!("sha256:{}", "1".repeat(64));
        let enrollment = EnrollmentReceiptV2::new(
            graph_digest.clone(),
            base.identity,
            &binding_receipt_chain_genesis(),
            "77777777-7777-4777-8777-777777777777",
            format!("sha256:{}", "2".repeat(64)),
            "act-operator",
            base.enrollment_receipt.stream_incarnation_id.clone(),
            base.binding_scope_id.clone(),
            base.binding.clone(),
            1,
        )
        .unwrap();
        let initial = BindingReceipt::new(
            graph_digest.clone(),
            base.identity,
            &enrollment.next_chain_ref().unwrap(),
            base.binding_scope_id.clone(),
            base.enrollment_receipt.stream_incarnation_id.clone(),
            base.binding.clone(),
            "77777777-7777-4777-8777-777777777777",
            2,
        )
        .unwrap();
        assert_eq!(initial.chain_ordinal, 2);
        assert_eq!(
            initial.receipt_digest,
            "sha256:a80a59b7fad93181bf8bed278be32200da4978ef6a03ad6093e82a77e0cf0924"
        );
        assert_eq!(initial.retained_shard_inventory_commitment().unwrap(), None);
        let initial_json = serde_json::to_value(&initial).unwrap();
        assert!(initial_json.get("retained_shard_count").is_none());
        assert!(initial_json.get("retained_shard_set_digest").is_none());
        assert_eq!(
            serde_json::from_value::<BindingReceipt>(initial_json).unwrap(),
            initial
        );
        assert!(
            BindingReceipt::new(
                graph_digest.clone(),
                base.identity,
                &binding_receipt_chain_genesis(),
                base.binding_scope_id.clone(),
                base.enrollment_receipt.stream_incarnation_id.clone(),
                base.binding.clone(),
                "77777777-7777-4777-8777-777777777777",
                2,
            )
            .is_err(),
            "a BindingReceipt cannot occupy the enrollment receipt's ordinal 1"
        );

        let old_shard = ShardId::parse_str(&base.binding.shard_ids[0]).unwrap();
        let new_shard =
            ShardId::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
        let forward = retained_shard_inventory_commitment(&[old_shard, new_shard]).unwrap();
        let reverse = retained_shard_inventory_commitment(&[new_shard, old_shard]).unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward.retained_shard_count, 2);
        assert!(retained_shard_inventory_commitment(&[old_shard, old_shard]).is_err());
        assert!(retained_shard_inventory_commitment(&[ShardId::nil()]).is_err());

        let rebound_binding = StreamPhysicalBinding {
            enrollment_id: "99999999-9999-4999-8999-999999999999".to_string(),
            shard_ids: vec![new_shard.to_string()],
            ..base.binding.clone()
        };
        assert!(
            BindingReceipt::new(
                graph_digest.clone(),
                base.identity,
                &initial.next_chain_ref().unwrap(),
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                base.enrollment_receipt.stream_incarnation_id.clone(),
                rebound_binding.clone(),
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                3,
            )
            .is_err(),
            "an ordinal >2 receipt cannot omit its retained-shard commitment"
        );
        let rebound = BindingReceipt::new_with_retained_shards(
            graph_digest,
            base.identity,
            &initial.next_chain_ref().unwrap(),
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            base.enrollment_receipt.stream_incarnation_id.clone(),
            rebound_binding,
            &[new_shard, old_shard],
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            3,
        )
        .unwrap();
        assert_eq!(
            rebound.retained_shard_inventory_commitment().unwrap(),
            Some(forward)
        );
        let rebound_json = serde_json::to_value(&rebound).unwrap();
        assert_eq!(rebound_json["retained_shard_count"], 2);
        assert!(rebound_json["retained_shard_set_digest"].is_string());

        let mut partial = rebound.clone();
        partial.retained_shard_set_digest = None;
        assert!(partial.validate().is_err());
        let mut wrong_count = rebound;
        wrong_count.retained_shard_count = Some(3);
        assert!(wrong_count.validate().is_err());
    }

    #[test]
    fn sealed_proof_rejects_replay_watermark_beyond_authenticated_sentinel() {
        let mut sealed = test_sealed_lifecycle_from(&entry()).unwrap();
        sealed
            .sealed_proof
            .as_mut()
            .expect("fixture has proof")
            .replay_cursor = 2;
        assert!(sealed.validate().is_err());
    }

    #[test]
    fn draining_row_naming_a_foreign_shard_is_typed_refusal_not_panic() {
        // A corrupt or foreign DRAINING row whose drain descriptor names a
        // shard absent from the entry's own binding must fail closed with a
        // typed error.  `DrainDescriptor::validate` runs before the caller
        // proves `drain.expected_binding == entry.binding`, so the epoch-floor
        // comparison cannot assume the two maps share keys.
        let mut foreign_shard_drain = entry();
        foreign_shard_drain.lifecycle = StreamLifecycle::Draining;
        foreign_shard_drain.lifecycle_revision = INITIAL_LIFECYCLE_REVISION + 1;

        let foreign_shard = "99999999-9999-4999-8999-999999999999".to_string();
        let mut expected_binding = foreign_shard_drain.binding.clone();
        expected_binding.shard_ids = vec![foreign_shard.clone()];
        let drain_id = "66666666-6666-4666-8666-666666666666".to_string();
        let operation_request_payload = QuiesceRequestPayload {
            protocol_version: QUIESCE_REQUEST_PROTOCOL_VERSION,
            graph_identity_digest: format!("sha256:{}", "b".repeat(64)),
            identity: foreign_shard_drain.identity,
            stream_incarnation_id: foreign_shard_drain
                .enrollment_receipt
                .stream_incarnation_id
                .clone(),
            binding_scope_id: foreign_shard_drain.binding_scope_id.clone(),
            enrollment_id: expected_binding.enrollment_id.clone(),
            drain_id: drain_id.clone(),
            expected_lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            goal: DrainGoal::Sealed,
            physical_binding_digest: stream_physical_binding_digest(&expected_binding).unwrap(),
            expected_current_head_witness: foreign_shard_drain.current_head_witness.clone(),
            target_epoch_floor_by_shard: BTreeMap::from([(foreign_shard.clone(), 1)]),
            seal_override: None,
        };
        let operation_request_digest = operation_request_payload.request_digest().unwrap();

        foreign_shard_drain.drain = Some(DrainDescriptor {
            drain_id,
            operation_expected_revision: INITIAL_LIFECYCLE_REVISION,
            operation_request_digest,
            goal: DrainGoal::Sealed,
            initiating_actor: "act-operator".to_string(),
            initiated_at: 1,
            expected_binding,
            expected_current_head_witness: foreign_shard_drain.current_head_witness.clone(),
            operation_request_payload,
            target_epoch_floor_by_shard: BTreeMap::from([(foreign_shard, 1)]),
            guarded_operation: None,
            seal_override: None,
        });

        let error = foreign_shard_drain
            .validate()
            .expect_err("a drain naming a shard outside the current binding must be refused");
        assert!(
            error
                .to_string()
                .contains("absent from the current shard binding"),
            "expected the typed foreign-shard refusal, got: {error}"
        );
    }

    #[test]
    fn enrollment_intent_digest_is_stable_and_binds_authority() {
        let entry = entry();
        let digest = stream_enrollment_intent_digest_v1(
            entry.identity,
            &entry.binding.table_location,
            "66666666-6666-4666-8666-666666666666",
            &format!("sha256:{}", "c".repeat(64)),
            2,
            &entry.current_head_witness,
            &entry.binding.stream_config_hash,
        )
        .unwrap();
        assert_eq!(
            digest,
            "sha256:0717d7ffecb791046c7a269bf767a1309cf3c35df9f721c7796ebbe060f66c14"
        );
        assert_eq!(
            digest,
            stream_enrollment_intent_digest_v1(
                entry.identity,
                &entry.binding.table_location,
                "66666666-6666-4666-8666-666666666666",
                &format!("sha256:{}", "c".repeat(64)),
                2,
                &entry.current_head_witness,
                &entry.binding.stream_config_hash,
            )
            .unwrap()
        );
        let mut moved = entry.current_head_witness.clone();
        moved.table_version += 1;
        assert_ne!(
            digest,
            stream_enrollment_intent_digest_v1(
                entry.identity,
                &entry.binding.table_location,
                "66666666-6666-4666-8666-666666666666",
                &format!("sha256:{}", "c".repeat(64)),
                2,
                &moved,
                &entry.binding.stream_config_hash,
            )
            .unwrap()
        );
    }

    #[test]
    fn lifecycle_payload_rejects_identity_branch_and_epoch_ambiguity() {
        let mut wrong_identity = entry();
        wrong_identity.binding.stable_table_id += 1;
        assert!(wrong_identity.validate().is_err());

        let mut named = entry();
        named.binding.table_branch = Some("feature".to_string());
        assert!(named.validate().is_err());

        let mut missing_epoch = entry();
        missing_epoch.epoch_floor_by_shard.clear();
        assert!(missing_epoch.validate().is_err());

        let mut empty_etag = entry();
        empty_etag.current_head_witness.manifest_e_tag = Some(String::new());
        assert!(empty_etag.validate().is_err());

        let mut malformed_hash = entry();
        malformed_hash.binding.stream_config_hash = "sha256:not-a-digest".to_string();
        assert!(malformed_hash.validate().is_err());
    }

    #[test]
    fn lifecycle_v3_ledger_builders_are_canonical_and_tamper_evident() {
        let base = entry();
        let graph_digest = format!("sha256:{}", "1".repeat(64));
        let enrollment = EnrollmentReceiptV2::new(
            graph_digest.clone(),
            base.identity,
            &binding_receipt_chain_genesis(),
            "77777777-7777-4777-8777-777777777777",
            format!("sha256:{}", "2".repeat(64)),
            "act-operator",
            base.enrollment_receipt.stream_incarnation_id.clone(),
            base.binding_scope_id.clone(),
            base.binding.clone(),
            1,
        )
        .unwrap();
        let enrollment_chain = enrollment.next_chain_ref().unwrap();
        let binding = BindingReceipt::new(
            graph_digest.clone(),
            base.identity,
            &enrollment_chain,
            base.binding_scope_id.clone(),
            base.enrollment_receipt.stream_incarnation_id.clone(),
            base.binding.clone(),
            "77777777-7777-4777-8777-777777777777",
            2,
        )
        .unwrap();
        assert_eq!(
            binding.record_lookup_key,
            BindingReceipt::lookup_key_for(
                &graph_digest,
                base.identity,
                &base.binding_scope_id,
                "77777777-7777-4777-8777-777777777777",
            )
            .unwrap()
        );
        assert_eq!(binding.next_chain_ref().unwrap().record_count, 2);

        let management = ManagementReceipt::new(
            graph_digest,
            base.identity,
            base.enrollment_receipt.stream_incarnation_id.clone(),
            base.binding_scope_id.clone(),
            &management_receipt_chain_genesis(),
            "88888888-8888-4888-8888-888888888888",
            "QUIESCE",
            1,
            3,
            "act-operator",
            serde_json::json!({"drain_id":"88888888-8888-4888-8888-888888888888","expected_revision":1}),
            serde_json::json!({"lifecycle":"SEALED","revision":3}),
            3,
        )
        .unwrap();
        management.validate(3).unwrap();

        let mut nested_forward = serde_json::Map::new();
        nested_forward.insert("zeta".to_string(), serde_json::json!(2));
        nested_forward.insert("alpha".to_string(), serde_json::json!(1));
        let mut request_forward = serde_json::Map::new();
        request_forward.insert(
            "outer_z".to_string(),
            serde_json::Value::Object(nested_forward),
        );
        request_forward.insert(
            "outer_a".to_string(),
            serde_json::json!([{"zeta": false, "alpha": true}]),
        );

        let mut nested_reverse = serde_json::Map::new();
        nested_reverse.insert("alpha".to_string(), serde_json::json!(1));
        nested_reverse.insert("zeta".to_string(), serde_json::json!(2));
        let mut array_object_reverse = serde_json::Map::new();
        array_object_reverse.insert("alpha".to_string(), serde_json::json!(true));
        array_object_reverse.insert("zeta".to_string(), serde_json::json!(false));
        let mut request_reverse = serde_json::Map::new();
        request_reverse.insert(
            "outer_a".to_string(),
            serde_json::json!([serde_json::Value::Object(array_object_reverse)]),
        );
        request_reverse.insert(
            "outer_z".to_string(),
            serde_json::Value::Object(nested_reverse),
        );

        let forward_digest =
            ManagementReceipt::request_digest_for(&serde_json::Value::Object(request_forward))
                .unwrap();
        let reverse_digest =
            ManagementReceipt::request_digest_for(&serde_json::Value::Object(request_reverse))
                .unwrap();
        assert_eq!(
            forward_digest, reverse_digest,
            "receipt digests must not depend on serde_json map insertion order"
        );
        assert_eq!(
            forward_digest,
            hash_fields(
                MANAGEMENT_REQUEST_DOMAIN,
                &[br#"{"outer_a":[{"alpha":true,"zeta":false}],"outer_z":{"alpha":1,"zeta":2}}"#],
            ),
            "receipt digests must commit to the explicitly key-sorted encoding"
        );

        let mut tampered = management.clone();
        tampered.result_payload["revision"] = serde_json::json!(4);
        assert!(tampered.validate(4).is_err());
    }

    #[test]
    fn lifecycle_v3_claim_commits_attempt_head_and_incremental_wal_tail() {
        let base = entry();
        let graph_digest = format!("sha256:{}", "1".repeat(64));
        let claim_id = "77777777-7777-4777-8777-777777777777".to_string();
        let attempt_id = "88888888-8888-4888-8888-888888888888".to_string();
        let sentinel_digest = format!("sha256:{}", "2".repeat(64));
        let terminal_effect_digest = format!("sha256:{}", "3".repeat(64));
        let attempt = ClaimAttemptEffect::new(
            &claim_attempt_chain_genesis(),
            ClaimAttemptEffectPreimage {
                graph_identity_digest: graph_digest.clone(),
                identity: base.identity,
                stream_incarnation_id: base.enrollment_receipt.stream_incarnation_id.clone(),
                binding_scope_id: base.binding_scope_id.clone(),
                enrollment_id: base.binding.enrollment_id.clone(),
                shard_id: base.binding.shard_ids[0].clone(),
                claim_id: claim_id.clone(),
                attempt_id: attempt_id.clone(),
                attempt_plan_digest: format!("sha256:{}", "4".repeat(64)),
                bound_prestate_digest: format!("sha256:{}", "5".repeat(64)),
                storage_envelope_digest: None,
                planned_sentinel_position: 1,
                planned_sentinel_digest: sentinel_digest.clone(),
                achieved_shard_manifest_version: Some(2),
                achieved_writer_epoch: Some(2),
                observed_sentinel_position: Some(1),
                observed_sentinel_digest: Some(sentinel_digest.clone()),
                attempt_terminal_effect_digest: terminal_effect_digest.clone(),
                classification: ClaimAttemptClassification::StockManifestPlusSentinel,
            },
        )
        .unwrap();
        attempt
            .validate_for_profile(ClaimProfile::RetainAll)
            .unwrap();
        let attempt_chain = attempt.next_attempt_chain_ref().unwrap();
        let physical_binding_digest = stream_physical_binding_digest(&base.binding).unwrap();
        let tail_segment_digest = format!("sha256:{}", "7".repeat(64));
        let tail_prior_chain_digest = digest_domain(AUTHENTICATED_WAL_TAIL_CHAIN_GENESIS_DOMAIN);
        let tail_empty_digest = format!("sha256:{}", "9".repeat(64));
        let tail_lww_digest = format!("sha256:{}", "a".repeat(64));
        let tail_chain_digest = authenticated_wal_tail_chain_digest(
            &base.binding_scope_id,
            &base.binding.enrollment_id,
            &base.binding.shard_ids[0],
            &base.enrollment_receipt.stream_incarnation_id,
            &base.binding.stream_config_hash,
            &physical_binding_digest,
            0,
            1,
            1,
            &tail_segment_digest,
            &tail_prior_chain_digest,
            1,
            &tail_empty_digest,
            &tail_lww_digest,
        )
        .unwrap();
        let claim = ClaimReceipt::new(
            &claim_receipt_chain_genesis(),
            ClaimReceiptPreimage {
                graph_identity_digest: graph_digest,
                identity: base.identity,
                claim_id,
                lifecycle_operation_id: Some("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string()),
                binding_scope_id: base.binding_scope_id,
                enrollment_id: base.binding.enrollment_id,
                shard_id: base.binding.shard_ids[0].clone(),
                stream_incarnation_id: base.enrollment_receipt.stream_incarnation_id,
                stream_configuration_digest: base.binding.stream_config_hash.clone(),
                physical_binding_digest,
                recovery_operation_id: "claim-recovery-1".to_string(),
                claim_kind: "QUIESCE".to_string(),
                profile: ClaimProfile::RetainAll,
                claim_operation_digest: format!("sha256:{}", "6".repeat(64)),
                attempt_count: attempt_chain.record_count,
                attempt_chain_head_id: attempt_chain.head_record_id.unwrap(),
                attempt_effect_chain_digest: attempt_chain.chain_digest,
                terminal_attempt_id: attempt_id,
                terminal_pre_shard_manifest_version: 1,
                achieved_shard_manifest_version: 2,
                achieved_writer_epoch: 2,
                sentinel_position: 1,
                sentinel_digest,
                replay_cursor: 1,
                authenticated_tail_prior_position: 0,
                authenticated_tail_position: 1,
                authenticated_tail_published_prefix_position: 0,
                authenticated_tail_segment_entry_count: 1,
                authenticated_tail_segment_digest: tail_segment_digest,
                authenticated_tail_segment_lww_projection_digest: tail_lww_digest.clone(),
                authenticated_tail_prior_chain_digest: tail_prior_chain_digest,
                authenticated_tail_segment_count: 1,
                authenticated_tail_chain_digest: tail_chain_digest,
                authenticated_tail_empty_fence_state_digest: tail_empty_digest,
                authenticated_tail_lww_projection_digest: tail_lww_digest,
                terminal_effect_digest,
                terminal_classification: ClaimTerminalClassification::StockManifestPlusSentinel,
                recorded_at: 4,
            },
        )
        .unwrap();
        claim.validate().unwrap();
        let json = serde_json::to_value(&claim).unwrap();
        assert!(json.get("attempt_effect_chain").is_none());
        assert_eq!(json["attempt_count"], 1);
        let mut impossible_count = claim.clone();
        impossible_count.authenticated_tail_segment_entry_count = 2;
        assert!(
            impossible_count.validate().is_err(),
            "segment entry count must equal the exact cursor delta"
        );
        let mut impossible_prefix = claim.clone();
        impossible_prefix.authenticated_tail_published_prefix_position = 1;
        assert!(
            impossible_prefix.validate().is_err(),
            "a published-prefix boundary must lie strictly inside its segment"
        );
        let mut tampered = claim;
        tampered.authenticated_tail_position = 2;
        assert!(tampered.validate().is_err());
    }

    #[test]
    fn historical_lifecycle_v2_shape_is_explicit_and_not_a_v3_decoder() {
        let current = entry();
        let legacy = LegacyStreamLifecycleEntryV2 {
            identity: current.identity,
            diagnostic_table_key: current.diagnostic_table_key.clone(),
            lifecycle: StreamLifecycle::Open,
            binding: current.binding.clone(),
            current_head_witness: current.current_head_witness.clone(),
            epoch_floor_by_shard: current.epoch_floor_by_shard.clone(),
            lifecycle_revision: current.lifecycle_revision,
            enrollment_receipt: current.enrollment_receipt.clone(),
            management_receipts: Vec::new(),
            claim_receipts: Vec::new(),
            current_claim_receipt_id: None,
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        };
        let value = serde_json::to_value(&legacy).unwrap();
        assert!(value["management_receipts"].as_array().unwrap().is_empty());
        assert!(value["claim_receipts"].as_array().unwrap().is_empty());
        assert!(value.get("binding_scope_id").is_none());
        assert!(value.get("authenticated_wal_tail").is_none());

        let mut manifest_payload =
            serde_json::to_value(StreamLifecycleEntry::to_metadata_json(&current).unwrap())
                .unwrap();
        assert_ne!(manifest_payload, value);
        manifest_payload = serde_json::json!({"protocol_version":2});
        let encoded = serde_json::to_string(&manifest_payload).unwrap();
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &current.object_id(),
                &current.diagnostic_table_key,
                current.identity,
                Some(&current.binding.table_location),
                Some(current.current_head_witness.table_version),
                None,
                &encoded,
            )
            .is_err()
        );
    }

    #[test]
    fn data_block_correction_successor_releases_only_the_block_and_advances_receipt_chain() {
        let prior = strict_blocked_entry();
        let block_token = prior.strict_block.as_ref().unwrap().block_token.clone();
        let correction_id = "88888888-8888-4888-8888-888888888888";
        let plan_digest = format!("sha256:{}", "8".repeat(64));
        let graph_commit_id = "01H000000000000000000000C1";
        let next_revision = prior.lifecycle_revision + 1;
        let request_payload = serde_json::json!({
            "block_token": block_token,
            "correction_plan_digest": plan_digest,
        });
        let result_payload = stream_correction_result_payload(
            correction_id,
            &plan_digest,
            graph_commit_id,
            10,
            next_revision,
        )
        .unwrap();
        let management = ManagementReceipt::new(
            format!("sha256:{}", "1".repeat(64)),
            prior.identity,
            prior.enrollment_receipt.stream_incarnation_id.clone(),
            prior.binding_scope_id.clone(),
            &prior.management_receipt_chain,
            correction_id,
            STREAM_CORRECTION_OPERATION_KIND,
            prior.lifecycle_revision,
            next_revision,
            "actor:operator",
            request_payload,
            result_payload,
            9,
        )
        .unwrap();
        let mut next_head = prior.current_head_witness.clone();
        next_head.table_version += 1;
        next_head.transaction_uuid = "99999999-9999-4999-8999-999999999999".to_string();
        let successor = build_data_block_correction_successor(
            &prior,
            &block_token,
            &plan_digest,
            &management,
            StreamDataCorrectionOutcome {
                graph_commit_id: graph_commit_id.to_string(),
                manifest_version: 10,
                current_head_witness: next_head.clone(),
                visible_rows: 1,
                visible_bytes: 1,
                recorded_at: 9,
            },
        )
        .unwrap();

        assert_eq!(successor.lifecycle, StreamLifecycle::Draining);
        assert!(successor.strict_block.is_none());
        assert_eq!(successor.current_head_witness, next_head);
        assert_eq!(
            successor
                .drain
                .as_ref()
                .unwrap()
                .expected_current_head_witness,
            successor.current_head_witness
        );
        assert_eq!(
            successor.management_receipt_chain,
            management.next_chain_ref().unwrap()
        );
        assert_eq!(successor.binding, prior.binding);
        assert_eq!(successor.claim_receipt_chain, prior.claim_receipt_chain);
        let summary = successor.last_fold_summary.as_ref().unwrap();
        assert_eq!(summary.outcome, LastFoldOutcome::Published);
        assert_eq!(summary.operation_id, prior.drain.as_ref().unwrap().drain_id);
        assert_eq!(
            &summary.exact_generation_cut,
            &prior
                .last_fold_summary
                .as_ref()
                .unwrap()
                .exact_generation_cut
        );

        let mut stale = management;
        stale.request_payload["block_token"] =
            serde_json::Value::String(format!("sha256:{}", "f".repeat(64)));
        assert!(
            build_data_block_correction_successor(
                &prior,
                &block_token,
                &plan_digest,
                &stale,
                StreamDataCorrectionOutcome {
                    graph_commit_id: graph_commit_id.to_string(),
                    manifest_version: 10,
                    current_head_witness: next_head,
                    visible_rows: 1,
                    visible_bytes: 1,
                    recorded_at: 9,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn correction_receipt_is_occurrence_keyed_and_commits_final_authority() {
        let prior = strict_blocked_entry();
        let block_token = prior.strict_block.as_ref().unwrap().block_token.clone();
        let correction_id = "88888888-8888-4888-8888-888888888888".to_string();
        let receipt = StreamCorrectionReceipt::new(StreamCorrectionReceiptPreimage {
            graph_identity_digest: format!("sha256:{}", "1".repeat(64)),
            identity: prior.identity,
            stream_incarnation_id: prior.enrollment_receipt.stream_incarnation_id.clone(),
            binding_scope_id: prior.binding_scope_id.clone(),
            block_token: block_token.clone(),
            correction_id: correction_id.clone(),
            correction_plan_digest: format!("sha256:{}", "2".repeat(64)),
            actor_id: "actor:operator".to_string(),
            graph_commit_id: "01H000000000000000000000C1".to_string(),
            resulting_manifest_version: 10,
            resulting_lifecycle_revision: prior.lifecycle_revision + 1,
            resulting_lifecycle_digest: format!("sha256:{}", "3".repeat(64)),
            resulting_token_authority_digest: format!("sha256:{}", "4".repeat(64)),
            recorded_at: 9,
        })
        .unwrap();
        receipt.validate().unwrap();
        assert_eq!(
            receipt.record_lookup_key,
            StreamCorrectionReceipt::lookup_key_for(
                &receipt.graph_identity_digest,
                prior.identity,
                &receipt.stream_incarnation_id,
                &block_token,
                &correction_id,
            )
            .unwrap()
        );
        assert_eq!(
            serde_json::from_str::<StreamCorrectionReceipt>(
                &serde_json::to_string(&receipt).unwrap()
            )
            .unwrap(),
            receipt
        );
        let mut tampered = receipt;
        tampered.resulting_token_authority_digest = format!("sha256:{}", "5".repeat(64));
        assert!(tampered.validate().is_err());
    }
}
