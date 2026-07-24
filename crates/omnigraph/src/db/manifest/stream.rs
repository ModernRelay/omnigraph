//! Durable RFC-026 stream lifecycle authority.
//!
//! Internal schema v9 cuts enrolled streams over to the RFC-026 state-v2 and
//! config-v3 wire contracts.  The public lifecycle operations remain inactive,
//! but their complete durable slots are present now so a later activation does
//! not reinterpret an already-stamped graph through serde defaults.

use std::collections::BTreeMap;

use lance::dataset::refs::BranchIdentifier;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

use super::layout::stream_state_object_id;
use super::{TableIdentity, TableRegistration};

pub(crate) const STREAM_STATE_PROTOCOL_VERSION: u32 = 2;
pub(crate) const STREAM_CONFIG_VERSION: u32 = 3;
pub(crate) const INITIAL_LIFECYCLE_REVISION: u64 = 1;

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

    fn validate(&self, expected_identity: TableIdentity) -> Result<()> {
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
    fn validate(&self) -> Result<()> {
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

/// Terminal receipt for a successful externally initiated lifecycle request.
/// `result_payload` is the complete bounded result object; serde's ordered map
/// representation keeps the enclosing state payload deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagementReceipt {
    pub(crate) operation_id: String,
    pub(crate) operation_kind: String,
    pub(crate) request_digest: String,
    pub(crate) from_revision: u64,
    pub(crate) to_revision: u64,
    pub(crate) actor_id: String,
    pub(crate) result_payload: serde_json::Value,
    pub(crate) result_digest: String,
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
    pub(crate) claim_id: String,
    pub(crate) recovery_operation_id: String,
    pub(crate) claim_kind: String,
    pub(crate) profile: ClaimProfile,
    pub(crate) claim_operation_digest: String,
    pub(crate) attempt_count: u32,
    pub(crate) attempt_effect_chain: Vec<ClaimAttemptEffect>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DrainGoal {
    Sealed,
    OpenAfterFold,
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
    pub(crate) target_epoch_floor_by_shard: BTreeMap<String, u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) guarded_operation: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StrictBlock {
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
    pub(crate) current_head_witness: CurrentHeadWitness,
    /// Epochs are scoped to the binding's never-reused shard IDs.
    pub(crate) epoch_floor_by_shard: BTreeMap<String, u64>,
    /// Monotonic state-row CAS revision. Every successful publication of this
    /// row advances it exactly once, including witness-only publications.
    pub(crate) lifecycle_revision: u64,
    pub(crate) enrollment_receipt: EnrollmentReceipt,
    pub(crate) management_receipts: Vec<ManagementReceipt>,
    pub(crate) claim_receipts: Vec<ClaimReceipt>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub(crate) current_claim_receipt_id: Option<String>,
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
    current_head_witness: CurrentHeadWitness,
    epoch_floor_by_shard: BTreeMap<String, u64>,
    lifecycle_revision: u64,
    enrollment_receipt: EnrollmentReceipt,
    management_receipts: Vec<ManagementReceipt>,
    claim_receipts: Vec<ClaimReceipt>,
    #[serde(deserialize_with = "deserialize_present_option")]
    current_claim_receipt_id: Option<String>,
    #[serde(deserialize_with = "deserialize_present_option")]
    drain: Option<DrainDescriptor>,
    #[serde(deserialize_with = "deserialize_present_option")]
    strict_block: Option<StrictBlock>,
    #[serde(deserialize_with = "deserialize_present_option")]
    sealed_proof: Option<SealedProof>,
    #[serde(deserialize_with = "deserialize_present_option")]
    last_fold_summary: Option<LastFoldSummary>,
}

impl ManagementReceipt {
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

impl ClaimAttemptEffect {
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

impl ClaimReceipt {
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
        validate_epoch_floors(
            &self.expected_binding,
            &self.target_epoch_floor_by_shard,
            "drain target",
        )?;
        for (shard_id, target_epoch) in &self.target_epoch_floor_by_shard {
            if target_epoch
                < entry
                    .epoch_floor_by_shard
                    .get(shard_id)
                    .expect("both maps exactly match the binding")
            {
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

impl StrictBlock {
    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
        validate_digest("strict block_token", &self.block_token)?;
        if self.enrollment_id != entry.binding.enrollment_id {
            return Err(OmniError::manifest_internal(
                "stream strict block enrollment_id differs from the current binding",
            ));
        }
        validate_uuid("strict block enrollment_id", &self.enrollment_id)?;
        let shard_id = validate_uuid("strict block shard_id", &self.shard_id)?;
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
        validate_canonical_text("strict block generation_path", &self.generation_path)?;
        self.base_current_head_witness.validate()?;
        if self.generation == 0
            || self.shard_manifest_version == 0
            || self.validation_contract_version == 0
            || self.correction_revision > entry.lifecycle_revision
        {
            return Err(OmniError::manifest_internal(
                "stream strict block carries an invalid generation, contract, or correction revision",
            ));
        }
        validate_protocol_label("strict block violation_code", &self.violation_code)?;
        validate_digest("strict block violation_digest", &self.violation_digest)?;
        validate_digest(
            "strict block correction_view_digest",
            &self.correction_view_digest,
        )?;
        Ok(())
    }
}

impl SealedProof {
    fn validate(&self, entry: &StreamLifecycleEntry) -> Result<()> {
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
    /// Build the first OPEN state-v2 row.  Every value is fixed before the
    /// enrollment recovery sidecar is armed; crash recovery publishes this
    /// exact receipt rather than inventing logical identity from a physical
    /// effect observed later.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_open_enrollment(
        identity: TableIdentity,
        diagnostic_table_key: String,
        binding: StreamPhysicalBinding,
        current_head_witness: CurrentHeadWitness,
        epoch_floor_by_shard: BTreeMap<String, u64>,
        enrollment_receipt: EnrollmentReceipt,
    ) -> Result<Self> {
        if enrollment_receipt.physical_binding != binding {
            return Err(OmniError::manifest_internal(
                "initial stream lifecycle binding must exactly match its enrollment receipt",
            ));
        }
        let entry = Self {
            identity,
            diagnostic_table_key,
            lifecycle: StreamLifecycle::Open,
            binding,
            current_head_witness,
            epoch_floor_by_shard,
            lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            enrollment_receipt,
            management_receipts: Vec::new(),
            claim_receipts: Vec::new(),
            current_claim_receipt_id: None,
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
                    block.validate(self)?;
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
            summary.validate(self)?;
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
            current_head_witness: self.current_head_witness.clone(),
            epoch_floor_by_shard: self.epoch_floor_by_shard.clone(),
            lifecycle_revision: self.lifecycle_revision,
            enrollment_receipt: self.enrollment_receipt.clone(),
            management_receipts: self.management_receipts.clone(),
            claim_receipts: self.claim_receipts.clone(),
            current_claim_receipt_id: self.current_claim_receipt_id.clone(),
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
            current_head_witness: payload.current_head_witness,
            epoch_floor_by_shard: payload.epoch_floor_by_shard,
            lifecycle_revision: payload.lifecycle_revision,
            enrollment_receipt: payload.enrollment_receipt,
            management_receipts: payload.management_receipts,
            claim_receipts: payload.claim_receipts,
            current_claim_receipt_id: payload.current_claim_receipt_id,
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

/// `Option<T>` normally treats an absent serde field as `None`. State-v2 needs
/// every nullable slot to be physically present so a truncated or older row is
/// never reinterpreted by a default.
fn deserialize_present_option<'de, D, T>(
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

/// Build the smallest state-v2-valid SEALED authority used by manifest and
/// branch-control unit fixtures. Production lifecycle management remains
/// inactive; keeping this constructor test-only prevents fixtures from
/// bypassing the exact claim and empty-proof requirements by mutating only the
/// lifecycle enum.
#[cfg(test)]
pub(crate) fn test_sealed_lifecycle_from(
    current: &StreamLifecycleEntry,
) -> Result<StreamLifecycleEntry> {
    let claim_id = "77777777-7777-4777-8777-777777777777".to_string();
    let attempt_id = "88888888-8888-4888-8888-888888888888".to_string();
    let sentinel_digest = format!("sha256:{}", "c".repeat(64));
    let terminal_effect_digest = format!("sha256:{}", "d".repeat(64));
    let attempt = ClaimAttemptEffect {
        ordinal: 1,
        attempt_id: attempt_id.clone(),
        attempt_plan_digest: format!("sha256:{}", "a".repeat(64)),
        bound_prestate_digest: format!("sha256:{}", "b".repeat(64)),
        storage_envelope_digest: None,
        planned_sentinel_position: 1,
        planned_sentinel_digest: sentinel_digest.clone(),
        achieved_shard_manifest_version: Some(2),
        achieved_writer_epoch: Some(1),
        observed_sentinel_position: Some(1),
        observed_sentinel_digest: Some(sentinel_digest.clone()),
        attempt_terminal_effect_digest: terminal_effect_digest.clone(),
        classification: ClaimAttemptClassification::StockManifestPlusSentinel,
    };
    let claim = ClaimReceipt {
        claim_id: claim_id.clone(),
        recovery_operation_id: "test-seal".to_string(),
        claim_kind: "TEST_SEAL".to_string(),
        profile: ClaimProfile::RetainAll,
        claim_operation_digest: format!("sha256:{}", "e".repeat(64)),
        attempt_count: 1,
        attempt_effect_chain: vec![attempt],
        attempt_effect_chain_digest: format!("sha256:{}", "f".repeat(64)),
        terminal_attempt_id: attempt_id,
        terminal_pre_shard_manifest_version: 1,
        achieved_shard_manifest_version: 2,
        achieved_writer_epoch: 1,
        sentinel_position: 1,
        sentinel_digest,
        replay_cursor: 1,
        terminal_effect_digest,
        terminal_classification: ClaimTerminalClassification::StockManifestPlusSentinel,
    };

    let mut sealed = current.clone();
    sealed.lifecycle = StreamLifecycle::Sealed;
    sealed.lifecycle_revision = sealed.lifecycle_revision.checked_add(1).ok_or_else(|| {
        OmniError::manifest_internal("test sealed lifecycle revision overflow")
    })?;
    sealed.claim_receipts = vec![claim];
    sealed.current_claim_receipt_id = Some(claim_id.clone());
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
        shard_manifest_version: 2,
        writer_epoch: 1,
        replay_cursor: 1,
        current_generation: 1,
        base_merged_generation: 0,
        base_current_head_witness: sealed.current_head_witness.clone(),
        current_claim_receipt_id: claim_id,
        claim_receipt_set_digest: format!("sha256:{}", "a".repeat(64)),
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
        let enrollment_receipt = EnrollmentReceipt::new(
            "44444444-4444-4444-8444-444444444444".to_string(),
            format!("sha256:{}", "b".repeat(64)),
            "55555555-5555-4555-8555-555555555555".to_string(),
            binding.clone(),
        )
        .unwrap();
        StreamLifecycleEntry {
            identity: TableIdentity::new(7, 9).unwrap(),
            diagnostic_table_key: "node:Person".to_string(),
            lifecycle: StreamLifecycle::Open,
            binding,
            current_head_witness: CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 4,
                transaction_uuid: "33333333-3333-4333-8333-333333333333".to_string(),
                manifest_e_tag: None,
            },
            epoch_floor_by_shard: BTreeMap::from([(shard_id, 1)]),
            lifecycle_revision: INITIAL_LIFECYCLE_REVISION,
            enrollment_receipt,
            management_receipts: Vec::new(),
            claim_receipts: Vec::new(),
            current_claim_receipt_id: None,
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        }
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
        assert_eq!(decoded.enrollment_receipt, entry.enrollment_receipt);

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
    fn state_v2_serializes_every_inert_slot_and_rejects_omission_or_v1() {
        let entry = entry();
        let json = entry.to_metadata_json().unwrap();
        let payload: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(payload["protocol_version"], 2);
        assert_eq!(payload["lifecycle_revision"], 1);
        assert!(
            payload["management_receipts"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(payload["claim_receipts"].as_array().unwrap().is_empty());
        for field in [
            "current_claim_receipt_id",
            "drain",
            "strict_block",
            "sealed_proof",
            "last_fold_summary",
        ] {
            assert!(
                payload.get(field).is_some(),
                "missing state-v2 slot {field}"
            );
            assert!(
                payload[field].is_null(),
                "state-v2 slot {field} is not null"
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
            "a nullable state-v2 field must be present explicitly rather than supplied by serde"
        );

        let mut v1 = payload;
        v1["protocol_version"] = serde_json::json!(1);
        let v1 = serde_json::to_string(&v1).unwrap();
        assert!(
            StreamLifecycleEntry::from_manifest_row(
                &entry.object_id(),
                &entry.diagnostic_table_key,
                entry.identity,
                Some(&entry.binding.table_location),
                Some(entry.current_head_witness.table_version),
                None,
                &v1,
            )
            .is_err()
        );
    }

    #[test]
    fn state_v2_open_requires_receipt_revision_and_null_lifecycle_slots() {
        let mut missing_receipt_revision = entry();
        missing_receipt_revision
            .enrollment_receipt
            .initial_lifecycle_revision = 2;
        assert!(missing_receipt_revision.validate().is_err());

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
}
