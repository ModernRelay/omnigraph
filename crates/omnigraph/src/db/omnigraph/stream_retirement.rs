//! Offline terminal retirement of graph-global stream sequencing authority.
//!
//! Planning proves one exact, sealed logical cut without retaining terminal
//! keys or walking ledger history. Confirmation appends one immutable receipt
//! and then selects it with `DISABLED -> RETIRED`; the logical graph and branch
//! heads never move.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, StringArray};
use base64::Engine;
use datafusion::prelude::col;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedMutexGuard, OwnedRwLockWriteGuard};

use super::stream_dead_letter::{
    DecodedStreamDeadLetterObjectEntry, verify_stream_dead_letter_object,
};
use super::{CheckedClusterDeadLetterAuthority, CheckedClusterRetirementAuthority, Omnigraph};
#[cfg(feature = "failpoints")]
use crate::db::manifest::ManifestChange;
use crate::db::manifest::stream::stream_graph_identity_digest;
#[cfg(feature = "failpoints")]
use crate::db::manifest::stream_token::StreamTerminalCorrection;
use crate::db::manifest::stream_token::{
    AUTHORITY_RETIREMENT_RECEIPT_V2_TAG, AuthorityRetirementReceiptV2,
    StreamDeadLetterObjectDescriptor, StreamDeadLetterReasonCode, StreamDeadLetterTerminalEvidence,
    StreamTokenDisposition, TrustedStreamRowMetadata, decode_trusted_stream_metadata,
    stream_authority_retirement_token_witness_digest_v2, validate_authority_base_pair,
};
#[cfg(feature = "failpoints")]
use crate::db::manifest::token_store::stage_stream_token_upsert;
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, lookup_authority_retirement_receipt_v2,
    scan_current_stream_token_batches, stage_authority_retirement_receipt_v2,
    stream_token_rows_for_keys, stream_token_rows_from_batch,
};
use crate::db::manifest::{
    AuthorityRetirementReceipt, INTERNAL_MANIFEST_SCHEMA_VERSION, RecoveryAuthorityToken,
    RecoveryStreamAuthorityRetirementOutcomeV21, StreamLifecycle, StreamProfileEntry,
    StreamProfileMode, TableIdentity, complete_stream_authority_retirement_sidecar_v21,
    confirm_stream_authority_retirement_sidecar_v21, lookup_lifecycle_ledger_record_by_id,
    new_stream_authority_retirement_sidecar_v21, open_stream_token_authority_head,
    stream_token_authority_entry_for_dataset, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage::join_uri;
use crate::storage_layer::{SnapshotHandle, StagedHandle};

const PLAN_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-plan.v2\0";
const BRANCH_HEADS_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-live-branch-heads.v1\0";
const LIFECYCLE_PROOF_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-lifecycle-proof.v1\0";
const EXPORT_CUT_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-export-cut.v1\0";
const EXPORT_BRANCH_MEMBER_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-export-branch-member.v1\0";
const EXPORT_TABLE_WITNESS_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-export-table-witness.v1\0";
const EXPORT_PROVENANCE_KIND: &str = "STREAM_AUTHORITY_RETIREMENT";
const DEAD_LETTER_PAGE_ENTRIES: usize = 256;
const DEAD_LETTER_PAGE_SERIALIZED_BYTES: usize = 256 * 1024 * 1024;
const DEAD_LETTER_PAGE_ENVELOPE_BYTES: usize = 64 * 1024;
const DEAD_LETTER_CURSOR_MAX_DECODED_BYTES: usize = 4 * 1024;
const DEAD_LETTER_CURSOR_MAX_ENCODED_BYTES: usize =
    DEAD_LETTER_CURSOR_MAX_DECODED_BYTES.div_ceil(3) * 4;

/// Complete in-process full-root cut envelope shared by authority retirement
/// and checked stream-aware export capture. The checked cluster guard remains
/// the cross-process fence; these gates provide the canonical in-process lock
/// order and one accepted catalog view.
pub(super) struct StreamExclusiveCutGates {
    _profile: OwnedRwLockWriteGuard<()>,
    _admission: Vec<OwnedRwLockWriteGuard<()>>,
    _schema: OwnedMutexGuard<()>,
    _branches: Vec<OwnedMutexGuard<()>>,
    _token: OwnedMutexGuard<()>,
    _tables: Vec<OwnedMutexGuard<()>>,
    catalog: Arc<omnigraph_compiler::catalog::Catalog>,
}

impl StreamExclusiveCutGates {
    pub(super) fn catalog(&self) -> Arc<omnigraph_compiler::catalog::Catalog> {
        Arc::clone(&self.catalog)
    }
}

/// Deterministic, read-only preflight returned by the offline cluster command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamAuthorityRetirementPlan {
    pub source_internal_schema_version: u32,
    pub source_manifest_version: u64,
    pub source_profile_revision: u64,
    pub plan_digest: String,
    pub live_branch_heads_digest: String,
    pub lifecycle_and_sealed_proof_digest: String,
    pub pre_retirement_token_witness_digest: String,
    pub present_token_count: u64,
    pub withdrawn_token_count: u64,
    pub dead_lettered_token_count: u64,
    pub export_cut_digest: String,
}

/// Terminal result of a new retirement or an exact receipt-first replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamAuthorityRetirementResult {
    pub changed: bool,
    pub retirement_id: String,
    pub plan_digest: String,
    pub export_cut_digest: String,
    pub profile_revision: u64,
    pub manifest_version: u64,
    pub present_token_count: u64,
    pub withdrawn_token_count: u64,
    pub dead_lettered_token_count: u64,
}

/// Current manifest-selected DEAD_LETTERED authority for one logical key.
/// Object references are descriptor-selected; no object-store inventory is
/// exposed or consulted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDeadLetterEntry {
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    pub table_key: String,
    pub logical_id: String,
    pub stream_incarnation_id: String,
    pub occurrence_token: String,
    pub predecessor_token: Option<String>,
    pub write_id: String,
    pub contributor_id: String,
    pub payload_digest: String,
    pub reason_code: String,
    pub fold_operation_id: String,
    pub object_location: String,
    pub object_digest: String,
    pub object_encoded_length: u64,
    pub object_candidate_count: u64,
    pub candidate_ordinal: u64,
}

/// One bounded canonical page of current terminal authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDeadLetterPage {
    pub source_manifest_version: u64,
    pub source_profile_revision: u64,
    pub token_table_version: u64,
    pub token_transaction_uuid: String,
    pub entries: Vec<StreamDeadLetterEntry>,
    pub next_cursor: Option<String>,
}

/// Descriptor-verified payload for one current DEAD_LETTERED key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDeadLetterPayloadEntry {
    pub authority: StreamDeadLetterEntry,
    /// Descriptor-verified canonical JSON kept raw so nested legal payloads
    /// cannot expand into an unbounded tree of `serde_json::Value` nodes.
    pub payload: Box<serde_json::value::RawValue>,
}

impl PartialEq for StreamDeadLetterPayloadEntry {
    fn eq(&self, other: &Self) -> bool {
        self.authority == other.authority && self.payload.get() == other.payload.get()
    }
}

/// One bounded payload-export page from the exact manifest/token cut.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDeadLetterPayloadPage {
    pub source_manifest_version: u64,
    pub source_profile_revision: u64,
    pub token_table_version: u64,
    pub token_transaction_uuid: String,
    pub entries: Vec<StreamDeadLetterPayloadEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamDeadLetterKey {
    identity: TableIdentity,
    logical_id: String,
}

#[derive(Debug, Clone)]
struct CapturedStreamDeadLetterEntry {
    key: StreamDeadLetterKey,
    public: StreamDeadLetterEntry,
    descriptor: StreamDeadLetterObjectDescriptor,
    evidence: StreamDeadLetterTerminalEvidence,
    stream_incarnation_id: String,
}

struct CapturedStreamDeadLetterPage {
    source_manifest_version: u64,
    source_profile_revision: u64,
    token_table_version: u64,
    token_transaction_uuid: String,
    entries: Vec<CapturedStreamDeadLetterEntry>,
    next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamDeadLetterCursor {
    source_manifest_version: u64,
    source_profile_revision: u64,
    token_table_version: u64,
    token_transaction_uuid: String,
    last_stable_table_id: u64,
    last_table_incarnation_id: u64,
    last_logical_id: String,
}

/// Exact frozen branch member named by one retired export. The receipt binds
/// the complete root cut; this witness identifies the selected member of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamAuthorityRetirementExportMember {
    pub(crate) branch: String,
    pub(crate) branch_identifier: lance::dataset::refs::BranchIdentifier,
    pub(crate) graph_head: Option<String>,
    pub(crate) manifest_version: u64,
    pub(crate) table_witness_digest: String,
    pub(crate) branch_member_digest: String,
}

/// Frozen retirement receipt carried by a rebuild export.
///
/// The outer provenance shape predates F5. V18 exporters wrote the v1
/// PRESENT/WITHDRAWN receipt, while current exporters write the v2
/// PRESENT/WITHDRAWN/DEAD_LETTERED receipt. Keeping the version choice inside
/// this untagged field preserves both exact historical wire shapes; the
/// receipt's own protocol version, tag, and canonical commitment remain the
/// authoritative discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum StreamAuthorityRetirementExportReceipt {
    V2(AuthorityRetirementReceiptV2),
    V1(AuthorityRetirementReceipt),
}

impl StreamAuthorityRetirementExportReceipt {
    fn validate(&self) -> Result<()> {
        match self {
            Self::V2(receipt) => {
                receipt.validate()?;
                Ok(())
            }
            Self::V1(receipt) => receipt.validate(),
        }
    }

    fn export_cut_digest(&self) -> &str {
        match self {
            Self::V2(receipt) => &receipt.export_cut_digest,
            Self::V1(receipt) => &receipt.export_cut_digest,
        }
    }

    fn retirement_id(&self) -> &str {
        match self {
            Self::V2(receipt) => &receipt.retirement_id,
            Self::V1(receipt) => &receipt.retirement_id,
        }
    }

    fn record_id(&self) -> &str {
        match self {
            Self::V2(receipt) => &receipt.record_id,
            Self::V1(receipt) => &receipt.record_id,
        }
    }

    fn graph_identity_digest(&self) -> &str {
        match self {
            Self::V2(receipt) => &receipt.graph_identity_digest,
            Self::V1(receipt) => &receipt.graph_identity_digest,
        }
    }

    fn source_internal_schema_version(&self) -> u32 {
        match self {
            Self::V2(receipt) => receipt.source_internal_schema_version,
            Self::V1(receipt) => receipt.source_internal_schema_version,
        }
    }

    fn source_manifest_version(&self) -> u64 {
        match self {
            Self::V2(receipt) => receipt.source_manifest_version,
            Self::V1(receipt) => receipt.source_manifest_version,
        }
    }

    fn source_profile_revision(&self) -> u64 {
        match self {
            Self::V2(receipt) => receipt.source_profile_revision,
            Self::V1(receipt) => receipt.source_profile_revision,
        }
    }

    fn live_branch_heads_digest(&self) -> &str {
        match self {
            Self::V2(receipt) => &receipt.live_branch_heads_digest,
            Self::V1(receipt) => &receipt.live_branch_heads_digest,
        }
    }

    fn pre_retirement_token_head(&self) -> &crate::db::manifest::CurrentHeadWitness {
        match self {
            Self::V2(receipt) => &receipt.pre_retirement_token_head,
            Self::V1(receipt) => &receipt.pre_retirement_token_head,
        }
    }

    fn next_chain_ref(&self) -> Result<crate::db::manifest::stream_profile::ReceiptChainRef> {
        match self {
            Self::V2(receipt) => Ok(receipt.next_chain_ref()?),
            Self::V1(receipt) => receipt.next_chain_ref(),
        }
    }
}

impl From<AuthorityRetirementReceiptV2> for StreamAuthorityRetirementExportReceipt {
    fn from(receipt: AuthorityRetirementReceiptV2) -> Self {
        Self::V2(receipt)
    }
}

impl From<AuthorityRetirementReceipt> for StreamAuthorityRetirementExportReceipt {
    fn from(receipt: AuthorityRetirementReceipt) -> Self {
        Self::V1(receipt)
    }
}

/// Closed import proof for one selected member of the receipt-bound frozen
/// branch set. The ordered digest vector is the exact preimage of the flat
/// root cut; the selected member carries the otherwise source-only table-cut
/// commitment needed to recompute its leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamAuthorityRetirementExportProvenance {
    pub(crate) kind: String,
    pub(crate) receipt: StreamAuthorityRetirementExportReceipt,
    /// Exact source-identity hash committed by the cut root. A rebuild mints a
    /// fresh graph identity, so this authenticates the source proof and is not
    /// compared with the target's accepted-IR hash.
    pub(crate) source_schema_ir_hash: String,
    pub(crate) ordered_branch_member_digests: Vec<String>,
    pub(crate) selected_member_index: u64,
    pub(crate) branch_member: StreamAuthorityRetirementExportMember,
}

impl StreamAuthorityRetirementExportMember {
    pub(crate) fn from_table_witness(
        branch: impl Into<String>,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
        graph_head: Option<String>,
        manifest_version: u64,
        table_witness_digest: impl Into<String>,
    ) -> Result<Self> {
        let branch = branch.into();
        let table_witness_digest = table_witness_digest.into();
        validate_retirement_export_member_fields(
            &branch,
            &branch_identifier,
            graph_head.as_deref(),
            manifest_version,
            &table_witness_digest,
        )?;
        let branch_member_digest = retirement_branch_member_digest_from_witness(
            &branch,
            &branch_identifier,
            graph_head.as_deref(),
            manifest_version,
            &table_witness_digest,
        )?;
        Ok(Self {
            branch,
            branch_identifier,
            graph_head,
            manifest_version,
            table_witness_digest,
            branch_member_digest,
        })
    }

    fn recompute_digest(&self) -> Result<String> {
        validate_retirement_export_member_fields(
            &self.branch,
            &self.branch_identifier,
            self.graph_head.as_deref(),
            self.manifest_version,
            &self.table_witness_digest,
        )?;
        validate_canonical_retirement_digest("frozen branch-member", &self.branch_member_digest)?;
        retirement_branch_member_digest_from_witness(
            &self.branch,
            &self.branch_identifier,
            self.graph_head.as_deref(),
            self.manifest_version,
            &self.table_witness_digest,
        )
    }
}

impl StreamAuthorityRetirementExportProvenance {
    pub(crate) fn validate_for_rebuild(&self) -> Result<()> {
        if self.kind != EXPORT_PROVENANCE_KIND {
            return Err(OmniError::manifest(format!(
                "unsupported export provenance kind '{}'",
                self.kind
            )));
        }
        self.receipt.validate()?;
        validate_canonical_retirement_digest(
            "retirement export source schema",
            &self.source_schema_ir_hash,
        )?;
        if self.ordered_branch_member_digests.is_empty() {
            return Err(OmniError::manifest(
                "retirement export cut proof must contain at least the main branch member",
            ));
        }
        for digest in &self.ordered_branch_member_digests {
            validate_canonical_retirement_digest("retirement export cut member", digest)?;
        }
        let selected_index = usize::try_from(self.selected_member_index).map_err(|_| {
            OmniError::manifest("retirement export selected member index exceeds this platform")
        })?;
        let selected_digest = self
            .ordered_branch_member_digests
            .get(selected_index)
            .ok_or_else(|| {
                OmniError::manifest(
                    "retirement export selected member index is outside the receipt-bound cut",
                )
            })?;
        let recomputed_member_digest = self.branch_member.recompute_digest()?;
        if recomputed_member_digest != self.branch_member.branch_member_digest {
            return Err(OmniError::manifest(
                "retirement export branch-member witness/digest mismatch",
            ));
        }
        if selected_digest != &recomputed_member_digest {
            return Err(OmniError::manifest(
                "retirement export branch member is not selected by the receipt-bound cut proof",
            ));
        }
        let recomputed_cut = retirement_export_cut_digest(
            &self.source_schema_ir_hash,
            &self.ordered_branch_member_digests,
        )?;
        if recomputed_cut != self.receipt.export_cut_digest() {
            return Err(OmniError::manifest(
                "retirement export branch member is not in the receipt-bound export cut",
            ));
        }
        Ok(())
    }
}

impl Omnigraph {
    /// List current DEAD_LETTERED authority from the manifest-selected token
    /// version. The page scan is bounded and never lists object prefixes or
    /// walks token history.
    #[doc(hidden)]
    pub async fn list_stream_dead_letters(
        &self,
        authority: CheckedClusterDeadLetterAuthority<'_>,
        cursor: Option<&str>,
    ) -> Result<StreamDeadLetterPage> {
        let _profile = self.write_queue().acquire_stream_profile_shared().await;
        let captured = self
            .capture_stream_dead_letter_page(&authority, cursor)
            .await?;
        let page = StreamDeadLetterPage {
            source_manifest_version: captured.source_manifest_version,
            source_profile_revision: captured.source_profile_revision,
            token_table_version: captured.token_table_version,
            token_transaction_uuid: captured.token_transaction_uuid,
            entries: captured
                .entries
                .into_iter()
                .map(|entry| entry.public)
                .collect(),
            next_cursor: captured.next_cursor,
        };
        enforce_dead_letter_page_bound("stream_dead_letter_list_page_bytes", &page)?;
        Ok(page)
    }

    /// Export descriptor-verified dead-letter payloads for current terminal
    /// authority. This adds Cedar `export` to the stopped/offline
    /// `stream_manage` capability and reads only exact descriptor locations.
    #[doc(hidden)]
    pub async fn export_stream_dead_letter_payloads(
        &self,
        authority: CheckedClusterDeadLetterAuthority<'_>,
        cursor: Option<&str>,
    ) -> Result<StreamDeadLetterPayloadPage> {
        self.enforce(
            omnigraph_policy::PolicyAction::Export,
            &omnigraph_policy::ResourceScope::Graph,
            Some(authority.actor()),
        )?;
        let _profile = self.write_queue().acquire_stream_profile_shared().await;
        let captured = self
            .capture_stream_dead_letter_page(&authority, cursor)
            .await?;
        let cut_cursor = |entry: &CapturedStreamDeadLetterEntry| {
            encode_stream_dead_letter_cursor(&StreamDeadLetterCursor {
                source_manifest_version: captured.source_manifest_version,
                source_profile_revision: captured.source_profile_revision,
                token_table_version: captured.token_table_version,
                token_transaction_uuid: captured.token_transaction_uuid.clone(),
                last_stable_table_id: entry.key.identity.stable_table_id,
                last_table_incarnation_id: entry.key.identity.table_incarnation_id,
                last_logical_id: entry.key.logical_id.clone(),
            })
        };

        let mut entries = Vec::new();
        let mut entries_bytes = 0_usize;
        let mut stopped_before_end = false;
        let mut cached_descriptor: Option<StreamDeadLetterObjectDescriptor> = None;
        let mut cached_object = Vec::<DecodedStreamDeadLetterObjectEntry>::new();
        for entry in &captured.entries {
            if cached_descriptor.as_ref() != Some(&entry.descriptor) {
                entry.descriptor.validate().map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "invalid descriptor-selected dead-letter object: {error}"
                    ))
                })?;
                let object_uri = join_uri(self.root_uri(), &entry.descriptor.location);
                let stored = self
                    .storage_adapter()
                    .read_text_if_exists_bounded(&object_uri, entry.descriptor.encoded_length)
                    .await?
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "descriptor-selected dead-letter object '{}' is missing",
                            entry.descriptor.location
                        ))
                    })?;
                cached_object = verify_stream_dead_letter_object(&entry.descriptor, &stored)?;
                cached_descriptor = Some(entry.descriptor.clone());
            }
            let ordinal = usize::try_from(entry.evidence.candidate_ordinal).map_err(|_| {
                OmniError::manifest_internal("dead-letter candidate ordinal exceeds usize")
            })?;
            let decoded = cached_object.get(ordinal).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "dead-letter object '{}' omits selected candidate ordinal {}",
                    entry.descriptor.location, entry.evidence.candidate_ordinal
                ))
            })?;
            validate_dead_letter_payload_binding(entry, decoded)?;
            let payload_entry = StreamDeadLetterPayloadEntry {
                authority: entry.public.clone(),
                payload: decoded.payload.clone(),
            };
            let encoded = serde_json::to_vec(&payload_entry).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to size dead-letter payload export entry: {error}"
                ))
            })?;
            let next_bytes = entries_bytes.checked_add(encoded.len()).ok_or_else(|| {
                OmniError::manifest_internal("dead-letter payload page byte count overflow")
            })?;
            let entry_budget =
                DEAD_LETTER_PAGE_SERIALIZED_BYTES.saturating_sub(DEAD_LETTER_PAGE_ENVELOPE_BYTES);
            if next_bytes > entry_budget {
                if entries.is_empty() {
                    return Err(OmniError::resource_limit(
                        "stream_dead_letter_payload_entry_bytes",
                        u64::try_from(entry_budget).unwrap_or(u64::MAX),
                        u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                    ));
                }
                stopped_before_end = true;
                break;
            }
            entries_bytes = next_bytes;
            entries.push(payload_entry);
        }

        let next_cursor = if stopped_before_end {
            let returned = captured
                .entries
                .get(entries.len().saturating_sub(1))
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "dead-letter payload pagination stopped without a returned entry",
                    )
                })?;
            Some(cut_cursor(returned)?)
        } else {
            captured.next_cursor
        };
        let page = StreamDeadLetterPayloadPage {
            source_manifest_version: captured.source_manifest_version,
            source_profile_revision: captured.source_profile_revision,
            token_table_version: captured.token_table_version,
            token_transaction_uuid: captured.token_transaction_uuid,
            entries,
            next_cursor,
        };
        enforce_dead_letter_page_bound("stream_dead_letter_payload_page_bytes", &page)?;
        Ok(page)
    }

    async fn capture_stream_dead_letter_page(
        &self,
        authority: &CheckedClusterDeadLetterAuthority<'_>,
        encoded_cursor: Option<&str>,
    ) -> Result<CapturedStreamDeadLetterPage> {
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        authority.validate_profile(snapshot.stream_profile())?;
        let token_authority = snapshot.stream_token_authority();
        let token_head = &token_authority.current_head_witness;
        if token_head.manifest_e_tag.is_some() {
            return Err(OmniError::manifest_internal(
                "dead-letter inspection requires the canonical token main witness with e-tag None",
            ));
        }

        let cursor = encoded_cursor
            .map(decode_stream_dead_letter_cursor)
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.source_manifest_version != snapshot.version()
                || cursor.source_profile_revision != snapshot.stream_profile().profile_revision
                || cursor.token_table_version != token_head.table_version
                || cursor.token_transaction_uuid != token_head.transaction_uuid
        }) {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "stream dead-letter cursor belongs to another manifest/profile/token cut"
                    .to_string(),
            });
        }
        let after = cursor.as_ref().map(|cursor| StreamDeadLetterKey {
            identity: TableIdentity {
                stable_table_id: cursor.last_stable_table_id,
                table_incarnation_id: cursor.last_table_incarnation_id,
            },
            logical_id: cursor.last_logical_id.clone(),
        });

        let table_keys = snapshot
            .entries()
            .map(|entry| (entry.identity, entry.table_key.clone()))
            .collect::<BTreeMap<_, _>>();
        let token_dataset = snapshot.open_stream_token_authority().await?;
        let mut batches =
            scan_current_stream_token_batches(&token_dataset, token_authority).await?;
        let mut retained =
            BTreeMap::<StreamDeadLetterKey, (CapturedStreamDeadLetterEntry, usize)>::new();
        let mut retained_bytes = 0_usize;
        let mut seen_after_cursor = 0_u64;
        let entry_budget =
            DEAD_LETTER_PAGE_SERIALIZED_BYTES.saturating_sub(DEAD_LETTER_PAGE_ENVELOPE_BYTES);
        while let Some(batch) = batches
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                OmniError::manifest_internal("dead-letter token batch Arrow size exceeds u64")
            })?;
            if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
                return Err(OmniError::resource_limit(
                    "stream_dead_letter_token_batch_arrow_bytes",
                    crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                    batch_bytes,
                ));
            }
            for row in stream_token_rows_from_batch(&batch)? {
                if row.disposition != StreamTokenDisposition::DeadLettered {
                    continue;
                }
                let key = StreamDeadLetterKey {
                    identity: row.identity,
                    logical_id: row.logical_id.clone(),
                };
                if after.as_ref().is_some_and(|after| &key <= after) {
                    continue;
                }
                seen_after_cursor = seen_after_cursor.checked_add(1).ok_or_else(|| {
                    OmniError::manifest_internal("dead-letter current-token count overflow")
                })?;
                let table_key = table_keys.get(&row.identity).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "current DEAD_LETTERED token identity {} has no selected base table",
                        row.identity
                    ))
                })?;
                let lifecycle = snapshot.stream_lifecycle(row.identity).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "current DEAD_LETTERED token identity {} has no selected lifecycle",
                        row.identity
                    ))
                })?;
                if row.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id {
                    return Err(OmniError::manifest_internal(format!(
                        "current DEAD_LETTERED token for {} belongs to another stream incarnation",
                        row.identity
                    )));
                }
                let evidence = row
                    .terminal_dead_letter
                    .as_deref()
                    .cloned()
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "DEAD_LETTERED current token has no terminal evidence",
                        )
                    })?;
                let public = stream_dead_letter_public_entry(&row, table_key, &evidence);
                let encoded_bytes = serde_json::to_vec(&public).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "failed to size stream dead-letter entry: {error}"
                    ))
                })?;
                if encoded_bytes.len() > entry_budget {
                    return Err(OmniError::resource_limit(
                        "stream_dead_letter_list_entry_bytes",
                        u64::try_from(entry_budget).unwrap_or(u64::MAX),
                        u64::try_from(encoded_bytes.len()).unwrap_or(u64::MAX),
                    ));
                }
                let captured = CapturedStreamDeadLetterEntry {
                    key: key.clone(),
                    public,
                    descriptor: evidence.object.clone(),
                    evidence,
                    stream_incarnation_id: row.stream_incarnation_id,
                };
                if retained
                    .insert(key, (captured, encoded_bytes.len()))
                    .is_some()
                {
                    return Err(OmniError::manifest_internal(
                        "manifest-selected token authority contains duplicate current DEAD_LETTERED key",
                    ));
                }
                retained_bytes =
                    retained_bytes
                        .checked_add(encoded_bytes.len())
                        .ok_or_else(|| {
                            OmniError::manifest_internal(
                                "stream dead-letter retained-byte count overflow",
                            )
                        })?;
                while retained.len() > DEAD_LETTER_PAGE_ENTRIES || retained_bytes > entry_budget {
                    let largest = retained.last_entry().ok_or_else(|| {
                        OmniError::manifest_internal(
                            "dead-letter bounded selection lost its largest entry",
                        )
                    })?;
                    let removed_bytes = largest.get().1;
                    largest.remove();
                    retained_bytes =
                        retained_bytes.checked_sub(removed_bytes).ok_or_else(|| {
                            OmniError::manifest_internal(
                                "stream dead-letter retained-byte count underflow",
                            )
                        })?;
                }
            }
        }

        let entries = retained
            .into_values()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>();
        let has_more = seen_after_cursor
            > u64::try_from(entries.len()).map_err(|_| {
                OmniError::manifest_internal("stream dead-letter page length exceeds u64")
            })?;
        let next_cursor = if has_more {
            let last = entries.last().ok_or_else(|| {
                OmniError::manifest_internal(
                    "dead-letter scan found additional entries but retained no page boundary",
                )
            })?;
            Some(encode_stream_dead_letter_cursor(&StreamDeadLetterCursor {
                source_manifest_version: snapshot.version(),
                source_profile_revision: snapshot.stream_profile().profile_revision,
                token_table_version: token_head.table_version,
                token_transaction_uuid: token_head.transaction_uuid.clone(),
                last_stable_table_id: last.key.identity.stable_table_id,
                last_table_incarnation_id: last.key.identity.table_incarnation_id,
                last_logical_id: last.key.logical_id.clone(),
            })?)
        } else {
            None
        };
        Ok(CapturedStreamDeadLetterPage {
            source_manifest_version: snapshot.version(),
            source_profile_revision: snapshot.stream_profile().profile_revision,
            token_table_version: token_head.table_version,
            token_transaction_uuid: token_head.transaction_uuid.clone(),
            entries,
            next_cursor,
        })
    }

    /// Prove a deterministic retirement cut under the checked stopped-writer
    /// authority. No receipt, sidecar, table, or manifest effect is produced.
    #[doc(hidden)]
    pub async fn plan_stream_authority_retirement(
        &self,
        authority: CheckedClusterRetirementAuthority<'_>,
    ) -> Result<StreamAuthorityRetirementPlan> {
        self.heal_pending_recovery_sidecars_outcome().await?;
        let _gates = self
            .acquire_stream_exclusive_cut_gates("stream_authority_retirement")
            .await?;
        self.capture_stream_authority_retirement_plan(&authority)
            .await
    }

    /// Confirm one exact plan and irreversibly make the source graph
    /// read/export-only. Receipt lookup precedes profile comparison so a lost
    /// terminal response is safely replayable.
    #[doc(hidden)]
    pub async fn confirm_stream_authority_retirement(
        &self,
        authority: CheckedClusterRetirementAuthority<'_>,
        retirement_id: &str,
        expected_plan_digest: &str,
    ) -> Result<StreamAuthorityRetirementResult> {
        self.heal_pending_recovery_sidecars_outcome().await?;
        let _gates = self
            .acquire_stream_exclusive_cut_gates("stream_authority_retirement")
            .await?;
        if authority.operation_id() != retirement_id {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "retirement confirmation guard operation id differs from retirement id"
                    .to_string(),
            });
        }
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;

        let main = self.open_coordinator_for_branch(None).await?;
        let current = main.snapshot();
        let selected_token = current.open_stream_token_authority().await?;
        if let Some(receipt) = lookup_authority_retirement_receipt_v2(
            &selected_token,
            current.stream_token_authority(),
            &graph_identity_digest,
            retirement_id,
        )
        .await?
        {
            if receipt.plan_digest != expected_plan_digest
                || receipt.actor != authority.actor()
                || receipt.graph_identity_digest != graph_identity_digest
            {
                return Err(OmniError::StreamRetirementIdempotencyConflict {
                    retirement_id: retirement_id.to_string(),
                });
            }
            self.validate_visible_retirement_receipt(
                &current,
                &StreamAuthorityRetirementExportReceipt::V2(receipt.clone()),
            )
            .await?;
            return Ok(StreamAuthorityRetirementResult::from_receipt(
                false,
                &receipt,
                current.stream_profile(),
                current.version(),
            ));
        }
        if let Some(error) = current.stream_profile().retired_error() {
            return Err(error);
        }

        let plan = self
            .capture_stream_authority_retirement_plan(&authority)
            .await?;
        if plan.plan_digest != expected_plan_digest {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let prior_profile = current.stream_profile().clone();
        if current.version() != plan.source_manifest_version
            || prior_profile.profile_revision != plan.source_profile_revision
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let receipt = AuthorityRetirementReceiptV2::new(
            graph_identity_digest,
            &prior_profile.profile_receipt_chain,
            retirement_id,
            plan.plan_digest.clone(),
            authority.actor(),
            plan.source_internal_schema_version,
            plan.source_manifest_version,
            plan.live_branch_heads_digest.clone(),
            plan.source_profile_revision,
            plan.lifecycle_and_sealed_proof_digest.clone(),
            current
                .stream_token_authority()
                .current_head_witness
                .clone(),
            plan.pre_retirement_token_witness_digest.clone(),
            plan.present_token_count,
            plan.withdrawn_token_count,
            plan.dead_lettered_token_count,
            plan.export_cut_digest.clone(),
            crate::db::now_micros()?,
        )?;
        let next_profile = StreamProfileEntry::retired_from_disabled(
            &prior_profile,
            receipt.next_chain_ref()?,
            receipt.retirement_id.clone(),
            receipt.record_id.clone(),
            receipt.export_cut_digest.clone(),
        )?;

        let selected_token = current.open_stream_token_authority().await?;
        let staged = stage_authority_retirement_receipt_v2(
            selected_token,
            current.stream_token_authority(),
            &receipt,
        )
        .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                current.stream_token_authority(),
                &self.control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);

        let fresh = self.open_coordinator_for_branch(None).await?;
        if fresh.snapshot().version() != current.version()
            || fresh.snapshot().stream_profile() != &prior_profile
            || fresh.snapshot().stream_token_authority() != current.stream_token_authority()
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let recovery_authority = RecoveryAuthorityToken {
            branch_identifier: fresh.branch_identifier().await?,
            graph_head: fresh.exact_graph_head(),
            schema_identity_domain: schema_state.schema_identity_domain,
            schema_ir_hash: schema_state.schema_ir_hash,
            schema_identity_version: schema_state.schema_identity_version,
        };
        let mut sidecar = new_stream_authority_retirement_sidecar_v21(
            authority.actor().to_string(),
            recovery_authority,
            current.version(),
            prior_profile,
            next_profile.clone(),
            current.stream_token_authority().clone(),
            receipt.clone(),
            planned_transaction,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

        let committed = match self.storage().commit_staged_exact(token_head, staged).await {
            Ok(outcome) if outcome.is_exact() => outcome,
            Ok(_) => {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "retirement receipt participant committed a non-exact transaction",
                ));
            }
            Err(error) => {
                let recovered = complete_stream_authority_retirement_sidecar_v21(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &current,
                    &sidecar,
                )
                .await;
                return match recovered {
                    Ok(RecoveryStreamAuthorityRetirementOutcomeV21::TerminalVisible {
                        receipt,
                        profile,
                        manifest_version,
                        ..
                    }) => Ok(StreamAuthorityRetirementResult::from_receipt(
                        true,
                        &receipt,
                        &profile,
                        manifest_version,
                    )),
                    Err(recovery_error) => Err(OmniError::recovery_required(
                        handle.operation_id,
                        format!(
                            "retirement receipt commit failed ({error}) and exact recovery did not complete: {recovery_error}"
                        ),
                    )),
                };
            }
        };
        let next_token_authority =
            stream_token_authority_entry_for_dataset(committed.snapshot().dataset())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
        confirm_stream_authority_retirement_sidecar_v21(
            self.root_uri(),
            self.storage_adapter(),
            &mut sidecar,
            committed.committed_transaction().clone(),
            next_token_authority.current_head_witness.clone(),
            next_token_authority,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("retirement receipt confirmation requires recovery: {error}"),
            )
        })?;
        let outcome = complete_stream_authority_retirement_sidecar_v21(
            self.root_uri(),
            Arc::clone(&self.storage),
            &current,
            &sidecar,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id,
                format!("retirement publication requires recovery: {error}"),
            )
        })?;
        let RecoveryStreamAuthorityRetirementOutcomeV21::TerminalVisible {
            receipt,
            profile,
            manifest_version,
            ..
        } = outcome;
        self.refresh_coordinator_only().await?;
        Ok(StreamAuthorityRetirementResult::from_receipt(
            true,
            &receipt,
            &profile,
            manifest_version,
        ))
    }

    pub(super) async fn acquire_stream_exclusive_cut_gates(
        &self,
        operation: &str,
    ) -> Result<StreamExclusiveCutGates> {
        let profile = self.write_queue().acquire_stream_profile_exclusive().await;
        self.ensure_schema_state_valid().await?;

        // The profile gate drains every graph writer. Close the lower domains
        // too so the final proof and v21 participant use the same canonical
        // profile -> admission -> schema -> branches -> token -> tables order
        // as the operations they exclude.
        let mut admission_keys = self
            .capture_branch_control_stream_admission_keys()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut live_branches = self
            .open_coordinator_for_branch(None)
            .await?
            .branch_list()
            .await?;
        if !live_branches.iter().any(|branch| branch == "main") {
            live_branches.push("main".to_string());
        }
        live_branches.sort();
        live_branches.dedup();
        for branch in &live_branches {
            let coordinator = if branch == "main" {
                self.open_coordinator_for_branch(None).await?
            } else {
                self.open_coordinator_for_branch(Some(branch)).await?
            };
            for entry in coordinator.snapshot().entries() {
                admission_keys.insert(StreamAdmissionKey::for_resolved_ref(
                    entry.identity,
                    entry.table_branch.as_deref(),
                ));
            }
        }
        let admission_keys = admission_keys.into_iter().collect::<Vec<_>>();
        let admission = self
            .write_queue()
            .acquire_stream_exclusive_many(&admission_keys)
            .await;
        let schema = self
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        self.ensure_schema_apply_not_locked(operation).await?;
        self.ensure_schema_state_valid().await?;
        let catalog = self.build_accepted_catalog_with_schema_gate_held().await?;

        let control_branches = live_branches
            .iter()
            .map(|branch| {
                if branch == "main" {
                    None
                } else {
                    Some(branch.clone())
                }
            })
            .collect::<Vec<_>>();
        let branches = self.write_queue().acquire_branches(&control_branches).await;
        let token = self.write_queue().acquire_stream_token().await;
        let table_keys = self.table_queue_keys_for_branches(&control_branches, &catalog);
        let tables = self.write_queue().acquire_many(&table_keys).await;

        let pending =
            crate::db::manifest::list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = pending.first() {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                format!("{operation} requires all recovery to be settled"),
            ));
        }
        Ok(StreamExclusiveCutGates {
            _profile: profile,
            _admission: admission,
            _schema: schema,
            _branches: branches,
            _token: token,
            _tables: tables,
            catalog,
        })
    }

    /// Test-only bridge that creates the exact terminal authority shape whose
    /// production writer belongs to the later correction slice.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_withdraw_stream_token_for_retirement_test(
        &self,
        table_key: &str,
        logical_id: &str,
        correction_id: &str,
    ) -> Result<()> {
        let _profile = self.write_queue().acquire_stream_profile_shared().await;
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        if snapshot.stream_profile().mode() != StreamProfileMode::Disabled {
            return Err(OmniError::manifest_internal(
                "retirement withdrawal test seam requires DISABLED profile",
            ));
        }
        let entry = snapshot
            .entry(table_key)
            .ok_or_else(|| OmniError::manifest_not_found(format!("unknown table '{table_key}'")))?;
        let lifecycle = snapshot.stream_lifecycle(entry.identity).ok_or_else(|| {
            OmniError::manifest_internal("retirement withdrawal test seam requires enrollment")
        })?;
        if lifecycle.lifecycle != StreamLifecycle::Sealed {
            return Err(OmniError::manifest_internal(
                "retirement withdrawal test seam requires SEALED lifecycle",
            ));
        }
        let admission_key =
            StreamAdmissionKey::for_resolved_ref(entry.identity, entry.table_branch.as_deref());
        let _admission = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;
        let _schema = self
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch = self.write_queue().acquire_branch(None).await;
        let _token = self.write_queue().acquire_stream_token().await;
        let _table = self
            .write_queue()
            .acquire(&(table_key.to_string(), entry.table_branch.clone()))
            .await;

        let mut coordinator = self.open_coordinator_for_branch(None).await?;
        let fresh = coordinator.snapshot();
        if fresh.version() != snapshot.version()
            || fresh.stream_token_authority() != snapshot.stream_token_authority()
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let dataset = fresh.open_stream_token_authority().await?;
        let ids = BTreeSet::from([logical_id.to_string()]);
        let mut rows = stream_token_rows_for_keys(
            &dataset,
            fresh.stream_token_authority(),
            entry.identity,
            &ids,
        )
        .await?;
        let mut row = rows.remove(logical_id).ok_or_else(|| {
            OmniError::manifest_internal("retirement withdrawal test seam found no current token")
        })?;
        row.disposition = StreamTokenDisposition::Withdrawn;
        row.terminal_correction = Some(StreamTerminalCorrection {
            actor: row.contributor_id.clone(),
            correction_id: correction_id.to_string(),
        });
        row.validate()
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let staged = stage_stream_token_upsert(
            dataset.clone(),
            fresh.stream_token_authority(),
            std::slice::from_ref(&row),
        )
        .await?;
        let store = crate::table_store::TableStore::new(self.root_uri(), self.control_session());
        let (achieved, _) = store.commit_staged_exact(Arc::new(dataset), staged).await?;
        let next = stream_token_authority_entry_for_dataset(&achieved).await?;
        coordinator
            .commit_operational_changes_with_expected(
                &[ManifestChange::SetStreamTokenAuthority {
                    expected: fresh.stream_token_authority().clone(),
                    next,
                }],
                &std::collections::HashMap::new(),
            )
            .await?;
        self.refresh_coordinator_only().await
    }

    /// Preflight the existing logical export surface. Ordinary export refuses
    /// terminal sequencing authority; a retired export instead returns the
    /// exact selected provenance receipt after re-proving its immutable cut.
    pub(super) async fn export_stream_authority_preflight_at(
        &self,
        snapshot: &crate::db::Snapshot,
    ) -> Result<Option<StreamAuthorityRetirementExportReceipt>> {
        if let crate::db::manifest::StreamProfileState::Retired {
            authority_retirement_receipt_id,
            ..
        } = &snapshot.stream_profile().state
        {
            let tokens = snapshot.open_stream_token_authority().await?;
            let selected_v2 = lookup_lifecycle_ledger_record_by_id(
                &tokens,
                snapshot.stream_token_authority(),
                AUTHORITY_RETIREMENT_RECEIPT_V2_TAG,
                authority_retirement_receipt_id,
            )
            .await?;
            let selected_v1 = lookup_lifecycle_ledger_record_by_id(
                &tokens,
                snapshot.stream_token_authority(),
                crate::db::manifest::stream_profile::AUTHORITY_RETIREMENT_RECEIPT_TAG,
                authority_retirement_receipt_id,
            )
            .await?;
            let receipt = match (selected_v2, selected_v1) {
                (Some(LifecycleLedgerRecord::AuthorityRetirementReceiptV2(receipt)), None) => {
                    StreamAuthorityRetirementExportReceipt::V2(receipt)
                }
                (None, Some(LifecycleLedgerRecord::AuthorityRetirementReceipt(receipt))) => {
                    StreamAuthorityRetirementExportReceipt::V1(receipt)
                }
                (None, None) => {
                    return Err(OmniError::manifest_internal(
                        "RETIRED profile does not select its immutable authority-retirement receipt",
                    ));
                }
                (Some(_), Some(_)) => {
                    return Err(OmniError::manifest_internal(
                        "RETIRED profile ambiguously selects both v1 and v2 authority-retirement receipts",
                    ));
                }
                _ => {
                    return Err(OmniError::manifest_internal(
                        "RETIRED profile selects an invalid authority-retirement ledger family",
                    ));
                }
            };
            self.validate_visible_retirement_receipt(snapshot, &receipt)
                .await?;
            return Ok(Some(receipt));
        }

        if snapshot.stream_profile().mode() != StreamProfileMode::Disabled {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: format!(
                    "ordinary export requires exact DISABLED stream profile; graph is {}",
                    snapshot.stream_profile().mode().as_str()
                ),
            });
        }
        for (_, lifecycle) in snapshot.stream_lifecycles() {
            if lifecycle.lifecycle != StreamLifecycle::Sealed || lifecycle.sealed_proof.is_none() {
                return Err(OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "ordinary export requires every enrolled lane SEALED; {} is {}",
                        lifecycle.identity,
                        lifecycle.lifecycle.as_str()
                    ),
                });
            }
        }
        let (_, withdrawn, dead_lettered) =
            validate_current_token_base_parity_and_counts(self, snapshot).await?;
        if withdrawn != 0 || dead_lettered != 0 {
            return Err(OmniError::StreamExportBlocked {
                withdrawn_token_count: withdrawn,
                dead_lettered_token_count: dead_lettered,
            });
        }
        Ok(None)
    }

    pub(super) async fn validate_visible_retirement_receipt(
        &self,
        snapshot: &crate::db::Snapshot,
        receipt: &StreamAuthorityRetirementExportReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;
        let expected_profile_revision = receipt
            .source_profile_revision()
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement profile revision overflow"))?;
        let expected_manifest_version = receipt
            .source_manifest_version()
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement manifest version overflow"))?;
        let expected_token_version = receipt
            .pre_retirement_token_head()
            .table_version
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement token version overflow"))?;
        let profile = snapshot.stream_profile();
        let expected_chain = receipt.next_chain_ref()?;
        let matches_profile = matches!(
            &profile.state,
            crate::db::manifest::StreamProfileState::Retired {
                authority_retirement_id,
                authority_retirement_receipt_id,
                authority_retirement_cut_digest,
            } if authority_retirement_id == receipt.retirement_id()
                && authority_retirement_receipt_id == receipt.record_id()
                && authority_retirement_cut_digest == receipt.export_cut_digest()
        );
        if !matches_profile
            || receipt.graph_identity_digest() != graph_identity_digest
            || receipt.source_internal_schema_version() != INTERNAL_MANIFEST_SCHEMA_VERSION
            || profile.profile_revision != expected_profile_revision
            || profile.profile_receipt_chain != expected_chain
            || snapshot.version() != expected_manifest_version
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .table_version
                != expected_token_version
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .transaction_uuid
                == receipt.pre_retirement_token_head().transaction_uuid
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .branch_identifier
                != lance::dataset::refs::BranchIdentifier::main()
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .manifest_e_tag
                .is_some()
        {
            return Err(OmniError::manifest_internal(
                "selected authority-retirement receipt does not match the terminal profile/manifest chain",
            ));
        }
        let (heads, cut) = self
            .capture_retirement_logical_cut_with_main_version(Some(
                receipt.source_manifest_version(),
            ))
            .await?;
        if heads != receipt.live_branch_heads_digest() || cut != receipt.export_cut_digest() {
            return Err(OmniError::manifest_internal(
                "retired graph logical cut differs from its selected authority-retirement receipt",
            ));
        }
        Ok(())
    }

    pub(super) async fn capture_stream_authority_retirement_plan(
        &self,
        authority: &CheckedClusterRetirementAuthority<'_>,
    ) -> Result<StreamAuthorityRetirementPlan> {
        if authority.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "retirement authority was retargeted to another graph store".to_string(),
            });
        }
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        let profile = snapshot.stream_profile();
        if profile.mode() != StreamProfileMode::Disabled
            || profile.profile_revision != authority.expected_profile_revision()
        {
            return Err(match profile.retired_error() {
                Some(error) => error,
                None => OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "authority retirement requires exact DISABLED profile revision {}; graph is {} revision {}",
                        authority.expected_profile_revision(),
                        profile.mode().as_str(),
                        profile.profile_revision
                    ),
                },
            });
        }

        let pending =
            crate::db::manifest::list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = pending.first() {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "authority retirement requires all recovery to be settled",
            ));
        }
        for staging_uri in [
            crate::db::schema_state::schema_source_staging_uri(self.root_uri()),
            crate::db::schema_state::schema_ir_staging_uri(self.root_uri()),
            crate::db::schema_state::schema_state_staging_uri(self.root_uri()),
        ] {
            if self.storage_adapter().exists(&staging_uri).await? {
                return Err(OmniError::manifest_conflict(
                    "authority retirement requires schema staging to be settled by a read-write reopen",
                ));
            }
        }

        let mut lifecycles = snapshot
            .stream_lifecycles()
            .map(|(_, lifecycle)| lifecycle)
            .collect::<Vec<_>>();
        lifecycles.sort_by_key(|lifecycle| lifecycle.identity);
        let mut lifecycle_hasher = Sha256::new();
        lifecycle_hasher.update(LIFECYCLE_PROOF_DOMAIN);
        hash_u64(&mut lifecycle_hasher, lifecycles.len() as u64);
        for lifecycle in &lifecycles {
            if lifecycle.lifecycle != StreamLifecycle::Sealed || lifecycle.sealed_proof.is_none() {
                return Err(OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "authority retirement requires every enrolled lane SEALED; {} is {}",
                        lifecycle.identity,
                        lifecycle.lifecycle.as_str()
                    ),
                });
            }
            let bytes = serde_json::to_vec(lifecycle).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to encode retirement lifecycle proof: {error}"
                ))
            })?;
            hash_field(&mut lifecycle_hasher, &bytes);
        }
        let lifecycle_and_sealed_proof_digest = finish_digest(lifecycle_hasher);

        let token_authority = snapshot.stream_token_authority().clone();
        let (present_token_count, withdrawn_token_count, dead_lettered_token_count) =
            validate_current_token_base_parity_and_counts(self, &snapshot).await?;
        if withdrawn_token_count == 0 && dead_lettered_token_count == 0 {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "authority retirement requires at least one current WITHDRAWN or DEAD_LETTERED token; use ordinary export for a fully PRESENT cut".to_string(),
            });
        }

        let pre_retirement_token_witness_digest =
            stream_authority_retirement_token_witness_digest_v2(
                &token_authority.current_head_witness,
                present_token_count,
                withdrawn_token_count,
                dead_lettered_token_count,
            )?;

        let (live_branch_heads_digest, export_cut_digest) =
            self.capture_retirement_logical_cut().await?;
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;
        let mut plan_hasher = Sha256::new();
        plan_hasher.update(PLAN_DOMAIN);
        hash_field(&mut plan_hasher, graph_identity_digest.as_bytes());
        hash_u32(&mut plan_hasher, INTERNAL_MANIFEST_SCHEMA_VERSION);
        hash_u64(&mut plan_hasher, snapshot.version());
        hash_u64(&mut plan_hasher, profile.profile_revision);
        hash_field(&mut plan_hasher, live_branch_heads_digest.as_bytes());
        hash_field(
            &mut plan_hasher,
            lifecycle_and_sealed_proof_digest.as_bytes(),
        );
        hash_field(
            &mut plan_hasher,
            pre_retirement_token_witness_digest.as_bytes(),
        );
        hash_u64(&mut plan_hasher, present_token_count);
        hash_u64(&mut plan_hasher, withdrawn_token_count);
        hash_u64(&mut plan_hasher, dead_lettered_token_count);
        hash_field(&mut plan_hasher, export_cut_digest.as_bytes());
        let plan_digest = finish_digest(plan_hasher);

        Ok(StreamAuthorityRetirementPlan {
            source_internal_schema_version: INTERNAL_MANIFEST_SCHEMA_VERSION,
            source_manifest_version: snapshot.version(),
            source_profile_revision: profile.profile_revision,
            plan_digest,
            live_branch_heads_digest,
            lifecycle_and_sealed_proof_digest,
            pre_retirement_token_witness_digest,
            present_token_count,
            withdrawn_token_count,
            dead_lettered_token_count,
            export_cut_digest,
        })
    }

    /// Hash the immutable logical projection. Receipt/profile-only movement is
    /// intentionally excluded so the same digest remains verifiable after the
    /// terminal manifest CAS.
    pub(crate) async fn capture_retirement_logical_cut(&self) -> Result<(String, String)> {
        self.capture_retirement_logical_cut_with_main_version(None)
            .await
    }

    async fn capture_retirement_logical_cut_with_main_version(
        &self,
        frozen_main_manifest_version: Option<u64>,
    ) -> Result<(String, String)> {
        let (schema_ir_hash, members) = self
            .capture_retirement_logical_cut_members_with_main_version(frozen_main_manifest_version)
            .await?;
        let live_branch_heads_digest = retirement_live_branch_heads_digest(&members)?;
        let ordered_branch_member_digests = members
            .iter()
            .map(|member| member.branch_member_digest.clone())
            .collect::<Vec<_>>();
        let export_cut_digest =
            retirement_export_cut_digest(&schema_ir_hash, &ordered_branch_member_digests)?;
        Ok((live_branch_heads_digest, export_cut_digest))
    }

    async fn capture_retirement_logical_cut_members_with_main_version(
        &self,
        frozen_main_manifest_version: Option<u64>,
    ) -> Result<(String, Vec<StreamAuthorityRetirementExportMember>)> {
        let mut branches = self
            .open_coordinator_for_branch(None)
            .await?
            .branch_list()
            .await?;
        branches.sort();
        branches.dedup();
        let schema_ir_hash =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter())
                .await?
                .schema_ir_hash;
        let mut members = Vec::with_capacity(branches.len());
        for branch in branches {
            let selected = if branch == "main" {
                self.open_coordinator_for_branch(None).await?
            } else {
                self.open_coordinator_for_branch(Some(&branch)).await?
            };
            let branch_identifier = selected.branch_identifier().await?;
            let graph_head = selected.exact_graph_head();
            let branch_snapshot = selected.snapshot();
            let frozen_manifest_version = if branch == "main" {
                frozen_main_manifest_version.unwrap_or_else(|| branch_snapshot.version())
            } else {
                branch_snapshot.version()
            };
            let table_witness_digest = retirement_table_witness_digest(&branch_snapshot)?;
            members.push(StreamAuthorityRetirementExportMember::from_table_witness(
                branch,
                branch_identifier,
                graph_head,
                frozen_manifest_version,
                table_witness_digest,
            )?);
        }
        Ok((schema_ir_hash, members))
    }

    pub(super) async fn capture_retirement_export_provenance(
        &self,
        branch: &str,
        expected_snapshot: &crate::db::Snapshot,
        receipt: StreamAuthorityRetirementExportReceipt,
    ) -> Result<StreamAuthorityRetirementExportProvenance> {
        let normalized = Self::normalize_branch_name(branch)?;
        let canonical_branch = normalized.as_deref().unwrap_or("main").to_string();
        let selected = self
            .open_coordinator_for_branch(normalized.as_deref())
            .await?;
        let snapshot = selected.snapshot();
        if snapshot.version() != expected_snapshot.version() {
            return Err(OmniError::manifest_internal(
                "retired export branch moved after its frozen read view was captured",
            ));
        }
        let (source_schema_ir_hash, members) = self
            .capture_retirement_logical_cut_members_with_main_version(Some(
                receipt.source_manifest_version(),
            ))
            .await?;
        let live_branch_heads_digest = retirement_live_branch_heads_digest(&members)?;
        let ordered_branch_member_digests = members
            .iter()
            .map(|member| member.branch_member_digest.clone())
            .collect::<Vec<_>>();
        let export_cut_digest =
            retirement_export_cut_digest(&source_schema_ir_hash, &ordered_branch_member_digests)?;
        if live_branch_heads_digest != receipt.live_branch_heads_digest()
            || export_cut_digest != receipt.export_cut_digest()
        {
            return Err(OmniError::manifest_internal(
                "retired export proof differs from its selected authority-retirement receipt",
            ));
        }
        let selected_member_index = members
            .iter()
            .position(|member| member.branch == canonical_branch)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "retired export selected branch is absent from the receipt-bound cut",
                )
            })?;
        let branch_member = members[selected_member_index].clone();
        let selected_member_index = u64::try_from(selected_member_index).map_err(|_| {
            OmniError::manifest_internal("retired export selected member index exceeds u64")
        })?;
        let provenance = StreamAuthorityRetirementExportProvenance {
            kind: EXPORT_PROVENANCE_KIND.to_string(),
            receipt,
            source_schema_ir_hash,
            ordered_branch_member_digests,
            selected_member_index,
            branch_member,
        };
        provenance.validate_for_rebuild()?;
        Ok(provenance)
    }
}

fn stream_dead_letter_public_entry(
    row: &crate::db::manifest::stream_token::StreamTokenAuthorityRow,
    table_key: &str,
    evidence: &StreamDeadLetterTerminalEvidence,
) -> StreamDeadLetterEntry {
    StreamDeadLetterEntry {
        stable_table_id: row.identity.stable_table_id,
        table_incarnation_id: row.identity.table_incarnation_id,
        table_key: table_key.to_string(),
        logical_id: row.logical_id.clone(),
        stream_incarnation_id: row.stream_incarnation_id.clone(),
        occurrence_token: evidence.occurrence_token.to_string(),
        predecessor_token: evidence.predecessor_token.map(|token| token.to_string()),
        write_id: evidence.write_id.clone(),
        contributor_id: evidence.contributor_id.as_str().to_string(),
        payload_digest: evidence.payload_digest.to_string(),
        reason_code: stream_dead_letter_reason_code(evidence.reason_code).to_string(),
        fold_operation_id: evidence.object.fold_operation_id.clone(),
        object_location: evidence.object.location.clone(),
        object_digest: evidence.object.object_digest.clone(),
        object_encoded_length: evidence.object.encoded_length,
        object_candidate_count: evidence.object.candidate_count,
        candidate_ordinal: evidence.candidate_ordinal,
    }
}

fn stream_dead_letter_reason_code(reason: StreamDeadLetterReasonCode) -> &'static str {
    match reason {
        StreamDeadLetterReasonCode::OrphanEdge => "ORPHAN_EDGE",
        StreamDeadLetterReasonCode::UniqueViolation => "UNIQUE_VIOLATION",
        StreamDeadLetterReasonCode::CardinalityViolation => "CARDINALITY_VIOLATION",
        StreamDeadLetterReasonCode::ValueConstraintViolation => "VALUE_CONSTRAINT_VIOLATION",
        StreamDeadLetterReasonCode::MultipleValidationViolations => {
            "MULTIPLE_VALIDATION_VIOLATIONS"
        }
        StreamDeadLetterReasonCode::CorrectionViewOverflow => "CORRECTION_VIEW_OVERFLOW",
    }
}

fn validate_dead_letter_payload_binding(
    selected: &CapturedStreamDeadLetterEntry,
    decoded: &DecodedStreamDeadLetterObjectEntry,
) -> Result<()> {
    let evidence = &selected.evidence;
    if decoded.protocol_version != selected.descriptor.protocol_version
        || decoded.candidate_ordinal != evidence.candidate_ordinal
        || decoded.logical_id != selected.key.logical_id
        || decoded.stream_incarnation_id != selected.stream_incarnation_id
        || decoded.write_id != evidence.write_id
        || decoded.current_token != evidence.occurrence_token
        || decoded.predecessor_token != evidence.predecessor_token
        || decoded.contributor_id != evidence.contributor_id.as_str()
        || decoded.payload_digest != evidence.payload_digest
        || decoded.reason_code != evidence.reason_code
    {
        return Err(OmniError::manifest_internal(format!(
            "dead-letter object '{}' candidate {} differs from current token authority",
            selected.descriptor.location, evidence.candidate_ordinal
        )));
    }
    Ok(())
}

fn encode_stream_dead_letter_cursor(cursor: &StreamDeadLetterCursor) -> Result<String> {
    let bytes = serde_json::to_vec(cursor).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to encode stream dead-letter cursor: {error}"
        ))
    })?;
    if bytes.len() > DEAD_LETTER_CURSOR_MAX_DECODED_BYTES {
        return Err(OmniError::resource_limit(
            "stream_dead_letter_cursor_decoded_bytes",
            DEAD_LETTER_CURSOR_MAX_DECODED_BYTES as u64,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        ));
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_stream_dead_letter_cursor(encoded: &str) -> Result<StreamDeadLetterCursor> {
    if encoded.len() > DEAD_LETTER_CURSOR_MAX_ENCODED_BYTES {
        return Err(OmniError::resource_limit(
            "stream_dead_letter_cursor_encoded_bytes",
            DEAD_LETTER_CURSOR_MAX_ENCODED_BYTES as u64,
            u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| {
            OmniError::manifest(format!(
                "invalid stream dead-letter cursor encoding: {error}"
            ))
        })?;
    if bytes.len() > DEAD_LETTER_CURSOR_MAX_DECODED_BYTES {
        return Err(OmniError::resource_limit(
            "stream_dead_letter_cursor_decoded_bytes",
            DEAD_LETTER_CURSOR_MAX_DECODED_BYTES as u64,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        ));
    }
    let cursor: StreamDeadLetterCursor = serde_json::from_slice(&bytes).map_err(|error| {
        OmniError::manifest(format!(
            "invalid stream dead-letter cursor payload: {error}"
        ))
    })?;
    if cursor.source_manifest_version == 0
        || cursor.source_profile_revision == 0
        || cursor.token_table_version == 0
        || cursor.token_transaction_uuid.is_empty()
        || cursor.last_stable_table_id == 0
        || cursor.last_table_incarnation_id == 0
    {
        return Err(OmniError::manifest(
            "invalid stream dead-letter cursor fields",
        ));
    }
    Ok(cursor)
}

fn enforce_dead_letter_page_bound(resource: &'static str, page: &impl Serialize) -> Result<()> {
    let encoded = serde_json::to_vec(page).map_err(|error| {
        OmniError::manifest_internal(format!("failed to size stream dead-letter page: {error}"))
    })?;
    if encoded.len() > DEAD_LETTER_PAGE_SERIALIZED_BYTES {
        return Err(OmniError::resource_limit(
            resource,
            DEAD_LETTER_PAGE_SERIALIZED_BYTES as u64,
            u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        ));
    }
    Ok(())
}

fn validate_canonical_retirement_digest(field: &str, value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(OmniError::manifest(format!(
            "{field} digest must use canonical sha256:<lowercase-hex> form"
        )));
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(OmniError::manifest(format!(
            "{field} digest must contain exactly 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

fn validate_retirement_export_member_fields(
    branch: &str,
    branch_identifier: &lance::dataset::refs::BranchIdentifier,
    graph_head: Option<&str>,
    manifest_version: u64,
    table_witness_digest: &str,
) -> Result<()> {
    let normalized = Omnigraph::normalize_branch_name(branch)?;
    let canonical = normalized.as_deref().unwrap_or("main");
    if canonical != branch
        || manifest_version == 0
        || graph_head.is_some_and(|head| head.is_empty() || head.trim() != head)
        || (branch == "main"
            && branch_identifier != &lance::dataset::refs::BranchIdentifier::main())
        || (branch != "main"
            && branch_identifier == &lance::dataset::refs::BranchIdentifier::main())
    {
        return Err(OmniError::manifest(
            "invalid frozen retirement export branch-member fields",
        ));
    }
    validate_canonical_retirement_digest("retirement export table witness", table_witness_digest)
}

fn retirement_table_witness_digest(snapshot: &crate::db::Snapshot) -> Result<String> {
    let mut table_witness = Sha256::new();
    table_witness.update(EXPORT_TABLE_WITNESS_DOMAIN);
    let mut entries = snapshot.entries().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.identity
            .cmp(&right.identity)
            .then_with(|| left.table_key.cmp(&right.table_key))
    });
    hash_u64(&mut table_witness, entries.len() as u64);
    for entry in entries {
        hash_u64(&mut table_witness, entry.identity.stable_table_id);
        hash_u64(&mut table_witness, entry.identity.table_incarnation_id);
        hash_field(&mut table_witness, entry.table_key.as_bytes());
        hash_field(&mut table_witness, entry.table_path.as_bytes());
        hash_u64(&mut table_witness, entry.table_version);
        hash_field(
            &mut table_witness,
            entry.table_branch.as_deref().unwrap_or("").as_bytes(),
        );
        hash_u64(&mut table_witness, entry.row_count);
        let metadata = serde_json::to_vec(&entry.version_metadata).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode retirement table witness: {error}"
            ))
        })?;
        hash_field(&mut table_witness, &metadata);
    }
    Ok(finish_digest(table_witness))
}

fn retirement_branch_member_digest_from_witness(
    branch: &str,
    branch_identifier: &lance::dataset::refs::BranchIdentifier,
    graph_head: Option<&str>,
    frozen_manifest_version: u64,
    table_witness_digest: &str,
) -> Result<String> {
    validate_retirement_export_member_fields(
        branch,
        branch_identifier,
        graph_head,
        frozen_manifest_version,
        table_witness_digest,
    )?;
    let identifier = serde_json::to_vec(branch_identifier).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to encode retirement branch identity: {error}"
        ))
    })?;
    let mut member = Sha256::new();
    member.update(EXPORT_BRANCH_MEMBER_DOMAIN);
    hash_field(&mut member, branch.as_bytes());
    hash_field(&mut member, &identifier);
    hash_u32(&mut member, u32::from(graph_head.is_some()));
    hash_field(&mut member, graph_head.unwrap_or("").as_bytes());
    hash_u64(&mut member, frozen_manifest_version);
    hash_field(&mut member, table_witness_digest.as_bytes());
    Ok(finish_digest(member))
}

pub(crate) fn retirement_live_branch_heads_digest(
    members: &[StreamAuthorityRetirementExportMember],
) -> Result<String> {
    let mut heads = Sha256::new();
    heads.update(BRANCH_HEADS_DOMAIN);
    hash_u64(&mut heads, members.len() as u64);
    for member in members {
        let recomputed = member.recompute_digest()?;
        if recomputed != member.branch_member_digest {
            return Err(OmniError::manifest(
                "retirement export branch-member witness/digest mismatch",
            ));
        }
        let identifier = serde_json::to_vec(&member.branch_identifier).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode retirement branch identity: {error}"
            ))
        })?;
        hash_field(&mut heads, member.branch.as_bytes());
        hash_field(&mut heads, &identifier);
        hash_field(
            &mut heads,
            member.graph_head.as_deref().unwrap_or("").as_bytes(),
        );
    }
    Ok(finish_digest(heads))
}

pub(crate) fn retirement_export_cut_digest(
    source_schema_ir_hash: &str,
    ordered_branch_member_digests: &[String],
) -> Result<String> {
    validate_canonical_retirement_digest("retirement export source schema", source_schema_ir_hash)?;
    if ordered_branch_member_digests.is_empty() {
        return Err(OmniError::manifest(
            "retirement export cut proof must contain at least the main branch member",
        ));
    }
    let mut cut = Sha256::new();
    cut.update(EXPORT_CUT_DOMAIN);
    hash_field(&mut cut, source_schema_ir_hash.as_bytes());
    hash_u64(&mut cut, ordered_branch_member_digests.len() as u64);
    for digest in ordered_branch_member_digests {
        validate_canonical_retirement_digest("retirement export cut member", digest)?;
        hash_field(&mut cut, digest.as_bytes());
    }
    Ok(finish_digest(cut))
}

async fn validate_current_token_base_parity_and_counts(
    db: &Omnigraph,
    snapshot: &crate::db::Snapshot,
) -> Result<(u64, u64, u64)> {
    let token_authority = snapshot.stream_token_authority();
    if token_authority
        .current_head_witness
        .manifest_e_tag
        .is_some()
    {
        return Err(OmniError::manifest_internal(
            "stream authority proof requires the canonical token main witness with e-tag None",
        ));
    }
    let token_dataset = snapshot.open_stream_token_authority().await?;
    let mut batches = scan_current_stream_token_batches(&token_dataset, token_authority).await?;
    let mut present_token_count = 0_u64;
    let mut withdrawn_token_count = 0_u64;
    let mut dead_lettered_token_count = 0_u64;
    while let Some(batch) = batches
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("stream authority token batch Arrow size exceeds u64")
        })?;
        if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_authority_token_batch_arrow_bytes",
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                batch_bytes,
            ));
        }
        let rows = stream_token_rows_from_batch(&batch)?;
        let mut ids_by_identity = BTreeMap::<TableIdentity, BTreeSet<String>>::new();
        for row in &rows {
            let lifecycle = snapshot.stream_lifecycle(row.identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "current token row for {} has no selected lifecycle authority",
                    row.identity
                ))
            })?;
            // Stream incarnation is the logical authority domain. The origin
            // enrollment is immutable row attribution and deliberately stays
            // unchanged across a physical rebind, so it must not be compared
            // with the lifecycle's current binding.
            if row.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id {
                return Err(OmniError::manifest_internal(format!(
                    "current token row for {} belongs to another stream incarnation",
                    row.identity
                )));
            }
            if !ids_by_identity
                .entry(row.identity)
                .or_default()
                .insert(row.logical_id.clone())
            {
                return Err(OmniError::manifest_internal(format!(
                    "manifest-selected token authority contains duplicate current key ({}, '{}')",
                    row.identity, row.logical_id
                )));
            }
            match row.disposition {
                StreamTokenDisposition::Present => {
                    present_token_count = present_token_count.checked_add(1).ok_or_else(|| {
                        OmniError::manifest_internal("PRESENT token count overflow")
                    })?;
                }
                StreamTokenDisposition::Withdrawn => {
                    withdrawn_token_count =
                        withdrawn_token_count.checked_add(1).ok_or_else(|| {
                            OmniError::manifest_internal("WITHDRAWN token count overflow")
                        })?;
                }
                StreamTokenDisposition::DeadLettered => {
                    dead_lettered_token_count =
                        dead_lettered_token_count.checked_add(1).ok_or_else(|| {
                            OmniError::manifest_internal("DEAD_LETTERED token count overflow")
                        })?;
                }
            }
        }
        for (identity, logical_ids) in ids_by_identity {
            let entry = snapshot
                .entries()
                .find(|entry| entry.identity == identity)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "current token identity {identity} has no selected base table"
                    ))
                })?;
            let base = db
                .storage()
                .open_snapshot_at_table(snapshot, &entry.table_key)
                .await?;
            let metadata = super::stream_ingest::lookup_base_stream_metadata_for_keys(
                base.dataset(),
                identity,
                &logical_ids,
            )
            .await?;
            for row in rows.iter().filter(|row| row.identity == identity) {
                validate_authority_base_pair(
                    identity,
                    &row.logical_id,
                    Some(row),
                    metadata.get(&row.logical_id),
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            }
        }
    }
    validate_base_to_token_parity(db, snapshot, &token_dataset).await?;
    Ok((
        present_token_count,
        withdrawn_token_count,
        dead_lettered_token_count,
    ))
}

/// Prove the reverse half of the authority/base invariant without retaining a
/// graph-sized key set. The current-token scan above proves every token has an
/// admissible base counterpart; this bounded base scan proves no trusted base
/// witness has lost its sequencing-authority row.
async fn validate_base_to_token_parity(
    db: &Omnigraph,
    snapshot: &crate::db::Snapshot,
    token_dataset: &lance::Dataset,
) -> Result<()> {
    for entry in snapshot.entries() {
        let identity = entry.identity;
        let base = db
            .storage()
            .open_snapshot_at_table(snapshot, &entry.table_key)
            .await?;
        let mut scanner = base.dataset().scan();
        scanner
            .project(&["id", crate::db::STREAM_METADATA_COLUMN])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        scanner.filter_expr(col(crate::db::STREAM_METADATA_COLUMN).is_not_null());
        scanner.batch_size(crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS as usize);
        scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                OmniError::manifest_internal("retirement base batch Arrow size exceeds u64")
            })?;
            if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
                return Err(OmniError::resource_limit(
                    "stream_retirement_base_batch_arrow_bytes",
                    crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                    batch_bytes,
                ));
            }
            let ids = batch
                .column_by_name("id")
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "retirement base parity scan returned no exact Utf8 id column",
                    )
                })?;
            let metadata = batch
                .column_by_name(crate::db::STREAM_METADATA_COLUMN)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "retirement base parity scan omitted reserved column '{}'",
                        crate::db::STREAM_METADATA_COLUMN
                    ))
                })?;
            let mut selected = BTreeMap::<String, TrustedStreamRowMetadata>::new();
            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    return Err(OmniError::manifest_internal(
                        "retirement base parity scan returned a null logical id",
                    ));
                }
                let logical_id = ids.value(row);
                let decoded = decode_trusted_stream_metadata(metadata.as_ref(), row)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "retirement non-null metadata scan returned a null witness",
                        )
                    })?;
                decoded
                    .validate_for(identity, logical_id)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                if selected.insert(logical_id.to_string(), decoded).is_some() {
                    return Err(OmniError::manifest_internal(format!(
                        "retirement base parity scan returned duplicate id '{logical_id}' in one batch"
                    )));
                }
            }
            if selected.is_empty() {
                continue;
            }
            let logical_ids = selected.keys().cloned().collect::<BTreeSet<_>>();
            let authority = stream_token_rows_for_keys(
                token_dataset,
                snapshot.stream_token_authority(),
                identity,
                &logical_ids,
            )
            .await?;
            for (logical_id, metadata) in selected {
                validate_authority_base_pair(
                    identity,
                    &logical_id,
                    authority.get(&logical_id),
                    Some(&metadata),
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            }
        }
    }
    Ok(())
}

impl StreamAuthorityRetirementResult {
    fn from_receipt(
        changed: bool,
        receipt: &AuthorityRetirementReceiptV2,
        profile: &StreamProfileEntry,
        manifest_version: u64,
    ) -> Self {
        Self {
            changed,
            retirement_id: receipt.retirement_id.clone(),
            plan_digest: receipt.plan_digest.clone(),
            export_cut_digest: receipt.export_cut_digest.clone(),
            profile_revision: profile.profile_revision,
            manifest_version,
            present_token_count: receipt.present_token_count,
            withdrawn_token_count: receipt.withdrawn_token_count,
            dead_lettered_token_count: receipt.dead_lettered_token_count,
        }
    }
}

fn hash_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field);
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hash_field(hasher, &value.to_be_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hash_field(hasher, &value.to_be_bytes());
}

fn finish_digest(hasher: Sha256) -> String {
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> StreamDeadLetterCursor {
        StreamDeadLetterCursor {
            source_manifest_version: 9,
            source_profile_revision: 4,
            token_table_version: 7,
            token_transaction_uuid: "00000000-0000-4000-8000-000000000001".to_string(),
            last_stable_table_id: 11,
            last_table_incarnation_id: 12,
            last_logical_id: "person-42".to_string(),
        }
    }

    #[test]
    fn dead_letter_cursor_round_trips_exact_cut_and_boundary() {
        let expected = cursor();
        let encoded = encode_stream_dead_letter_cursor(&expected).unwrap();
        assert_eq!(
            decode_stream_dead_letter_cursor(&encoded).unwrap(),
            expected
        );
    }

    #[test]
    fn dead_letter_cursor_bounds_encoded_input_before_base64_allocation() {
        let encoded = "A".repeat(DEAD_LETTER_CURSOR_MAX_ENCODED_BYTES + 1);
        let error = decode_stream_dead_letter_cursor(&encoded).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stream_dead_letter_cursor_encoded_bytes")
        );
    }
}
