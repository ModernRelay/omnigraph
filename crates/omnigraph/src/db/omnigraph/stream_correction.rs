//! Offline inspection and correction of one exact RFC-026 `DataBlock`.
//!
//! This is intentionally a cluster-checked, main-only adapter. It reopens the
//! manifest-selected immutable generation, reproduces the validator evidence,
//! validates a complete corrected overlay before any effect, and delegates
//! the exact base/token publication gap to recovery-v20. It does not activate
//! the frozen recovery-v14 `StreamCorrection` scaffold or the reserved
//! `AuthorityBlock` vocabulary.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_select::take::take;
use base64::Engine;
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::stream_ingest::{
    AttributedFoldPlan, StreamAuthorityCapture, append_trusted_stream_metadata, fold_output_size,
    plan_fold_attribution, validate_stream_input_bounds, validate_stream_value_constraints,
};
use super::stream_lifecycle::{
    CanonicalDataBlockView, CanonicalDataBlockViewEntry, DataBlockEvidenceCollector,
};
use super::{CheckedClusterBlockAuthority, Omnigraph};
use crate::db::manifest::stream::{
    ManagementReceipt, STREAM_CORRECTION_OPERATION_KIND,
    STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION, StreamCorrectionReceipt,
    StreamCorrectionReceiptPreimage, StreamDataCorrectionOutcome, StreamLifecycle, StrictBlock,
    StrictBlockEvidence, build_data_block_correction_successor, stream_correction_result_payload,
    stream_graph_identity_digest, stream_lifecycle_authority_digest,
};
use crate::db::manifest::stream_token::{
    PayloadDigest, PayloadDigestInput, StreamFoldAttributionSummary, StreamTokenAuthorityRow,
    StreamTokenDisposition, TrustedContributorId, TrustedStreamRowMetadata,
    stream_fold_attribution_commitment,
};
use crate::db::manifest::{
    RecoveryAuthorityToken, RecoveryLineageIntent, RecoveryStreamFoldCut, SidecarTablePin,
    complete_stream_correction_sidecar_v20, confirm_stream_correction_sidecar_v20,
    lookup_management_receipt, lookup_stream_correction_receipt, new_stream_correction_sidecar_v20,
    open_stream_token_authority_head, stage_stream_correction_effect,
    stream_token_authority_entry_for_dataset, stream_token_authority_plan_digest,
    validate_stream_token_plan_bounds, write_sidecar,
};
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};
use crate::table_store::mem_wal::{
    PassiveB1PhysicalState, capture_current_head_witness, scan_flushed_generation_projection,
    validate_b1_lifecycle_physical_state_with_binding_inventory,
};
use crate::validate::{ChangeSet, CommittedState, TableChange};

/// One canonical validator correction record. Ordinals are stable only for
/// the exact `(block_token, correction_view_digest, lifecycle_revision)` cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDataBlockEntry {
    pub ordinal: u64,
    pub table_key: String,
    pub logical_key: String,
    pub current_blocked_winner_stream_token: String,
    pub violation_code: String,
    pub field_path_or_group: Vec<String>,
    pub violation_instance_id: String,
    pub allowed_actions: Vec<String>,
}

/// Bounded page reconstructed from the retained immutable WAL generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDataBlockPage {
    pub block_token: String,
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    pub table_key: String,
    pub lifecycle_revision: u64,
    pub correction_view_digest: String,
    pub entries: Vec<StreamDataBlockEntry>,
    pub next_cursor: Option<String>,
}

/// Ordered operator disposition for one exact correction-view entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum StreamDataCorrectionAction {
    Replace {
        ordinal: u64,
        logical_key: String,
        current_blocked_winner_stream_token: String,
        write_id: String,
        row: serde_json::Value,
    },
    Withdraw {
        ordinal: u64,
        logical_key: String,
        current_blocked_winner_stream_token: String,
    },
}

impl StreamDataCorrectionAction {
    fn ordinal(&self) -> u64 {
        match self {
            Self::Replace { ordinal, .. } | Self::Withdraw { ordinal, .. } => *ordinal,
        }
    }

    fn logical_key(&self) -> &str {
        match self {
            Self::Replace { logical_key, .. } | Self::Withdraw { logical_key, .. } => logical_key,
        }
    }

    fn predecessor(&self) -> &str {
        match self {
            Self::Replace {
                current_blocked_winner_stream_token,
                ..
            }
            | Self::Withdraw {
                current_blocked_winner_stream_token,
                ..
            } => current_blocked_winner_stream_token,
        }
    }
}

/// Versioned correction request. The optional digest is an assertion over the
/// engine-normalized plan, never caller-owned authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDataCorrectionRequest {
    pub protocol_version: u32,
    pub block_token: String,
    pub correction_id: String,
    pub expected_lifecycle_revision: u64,
    pub actions: Vec<StreamDataCorrectionAction>,
    #[serde(default)]
    pub expected_plan_digest: Option<String>,
}

/// Immutable result of a new correction or receipt-first replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDataCorrectionResult {
    pub changed: bool,
    pub correction_id: String,
    pub plan_digest: String,
    pub graph_commit_id: String,
    pub lifecycle_revision: u64,
    pub manifest_version: u64,
}

const STREAM_DATA_CORRECTION_PROTOCOL_VERSION: u32 = 1;
const STREAM_DATA_CORRECTION_MAX_ACTIONS: usize = 8_192;
// One offline correction request may consume at most the same root-wide
// preprocessing envelope as two concurrent B2 calls. The request is checked
// once as compact JSON before any row normalization, then every normalized
// replacement and canonical payload is charged cumulatively before the
// preparation pass normalizes those rows again.
const STREAM_DATA_CORRECTION_MAX_PREPROCESSING_BYTES: u64 =
    crate::table_store::mem_wal::B2_MAX_PREPROCESSING_BYTES_ROOT;
const STREAM_DATA_BLOCK_PAGE_ENTRIES: usize = 256;
// A valid entry is derived from one at-most-32-MiB acknowledged generation.
// JSON string escaping expands one input byte to at most six output bytes;
// 256 MiB therefore covers that 192-MiB worst case plus the entry's fixed
// field names, token, action vocabulary, array framing, and schema-safe
// evidence fields and the page envelope/cursor. Exceeding this limit is an
// invariant breach, not an empty page: returning no ordinal would make the
// block impossible to correct.
const STREAM_DATA_BLOCK_PAGE_SERIALIZED_BYTES: usize = 256 * 1024 * 1024;
const STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES: usize = 4 * 1024;
// A padded base64 envelope is deliberately used even though cursors omit
// padding. That leaves the decoded-size check independently meaningful while
// still bounding allocation to a few bytes over the 4-KiB payload ceiling.
const STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES: usize =
    STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES.div_ceil(3) * 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamDataBlockCursor {
    block_token: String,
    correction_view_digest: String,
    lifecycle_revision: u64,
    next_ordinal: u64,
}

struct ReconstructedDataBlock {
    block: StrictBlock,
    view: CanonicalDataBlockView,
    batches: Vec<arrow_array::RecordBatch>,
    attribution: AttributedFoldPlan,
}

struct PreparedCorrectionOverlay {
    batches: Vec<RecordBatch>,
    token_rows: Vec<StreamTokenAuthorityRow>,
    attribution_summary: Option<StreamFoldAttributionSummary>,
    plan_digest: String,
    visible_rows: u64,
    visible_bytes: u64,
}

const STREAM_DATA_CORRECTION_PLAN_DOMAIN: &[u8] = b"omnigraph.stream-data-correction-plan.v1\0";

impl StreamDataCorrectionRequest {
    /// Hard byte ceiling used by frontends before deserializing a correction
    /// request. The engine independently rechecks its compact serialized form.
    pub const MAX_SERIALIZED_BYTES: u64 = STREAM_DATA_CORRECTION_MAX_PREPROCESSING_BYTES;

    fn validate_shape(&self) -> crate::error::Result<()> {
        if self.protocol_version != STREAM_DATA_CORRECTION_PROTOCOL_VERSION {
            return Err(crate::error::OmniError::manifest(format!(
                "unsupported stream correction protocol_version {}; expected {STREAM_DATA_CORRECTION_PROTOCOL_VERSION}",
                self.protocol_version
            )));
        }
        if self.actions.is_empty() || self.actions.len() > STREAM_DATA_CORRECTION_MAX_ACTIONS {
            return Err(crate::error::OmniError::resource_limit(
                "stream correction actions",
                STREAM_DATA_CORRECTION_MAX_ACTIONS as u64,
                u64::try_from(self.actions.len()).unwrap_or(u64::MAX),
            ));
        }
        validate_serialized_correction_request_bytes(self, Self::MAX_SERIALIZED_BYTES)?;
        validate_request_uuid("correction_id", &self.correction_id)?;
        if self.expected_lifecycle_revision == 0 {
            return Err(crate::error::OmniError::manifest(
                "stream correction expected_lifecycle_revision must be non-zero",
            ));
        }
        let mut prior = None;
        let mut keys = std::collections::BTreeSet::new();
        for action in &self.actions {
            if action.logical_key().is_empty() || action.predecessor().is_empty() {
                return Err(crate::error::OmniError::manifest(
                    "stream correction actions require a logical key and blocked predecessor token",
                ));
            }
            if prior.is_some_and(|ordinal| ordinal >= action.ordinal()) {
                return Err(crate::error::OmniError::manifest(
                    "stream correction actions must be ordered by strictly increasing correction-view ordinal",
                ));
            }
            if !keys.insert(action.logical_key()) {
                return Err(crate::error::OmniError::manifest(
                    "stream correction plan may name each logical key only once",
                ));
            }
            if let StreamDataCorrectionAction::Replace { write_id, row, .. } = action {
                validate_request_uuid("REPLACE write_id", write_id)?;
                if !row.is_object() {
                    return Err(crate::error::OmniError::manifest(
                        "stream correction REPLACE row must be one complete JSON object",
                    ));
                }
            }
            prior = Some(action.ordinal());
        }
        Ok(())
    }
}

fn validate_serialized_correction_request_bytes(
    request: &StreamDataCorrectionRequest,
    byte_limit: u64,
) -> Result<()> {
    let mut writer = BoundedCorrectionRequestJsonCounter::new(byte_limit);
    match serde_json::to_writer(&mut writer, request) {
        Ok(()) => Ok(()),
        Err(_) if writer.exceeded => Err(OmniError::resource_limit(
            "stream_correction_request_bytes",
            byte_limit,
            writer.attempted,
        )),
        Err(error) => Err(OmniError::manifest_internal(format!(
            "failed to size stream correction request JSON: {error}"
        ))),
    }
}

struct BoundedCorrectionRequestJsonCounter {
    written: u64,
    attempted: u64,
    limit: u64,
    exceeded: bool,
}

impl BoundedCorrectionRequestJsonCounter {
    fn new(limit: u64) -> Self {
        Self {
            written: 0,
            attempted: 0,
            limit,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedCorrectionRequestJsonCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.attempted = self.written.saturating_add(byte_count);
        if self.attempted > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "stream correction request JSON exceeds its byte budget",
            ));
        }
        self.written = self.attempted;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_request_uuid(field: &str, value: &str) -> Result<()> {
    let parsed = ShardId::parse_str(value).map_err(|_| {
        OmniError::manifest(format!(
            "stream correction {field} must be a canonical non-nil lowercase UUID"
        ))
    })?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(OmniError::manifest(format!(
            "stream correction {field} must be a canonical non-nil lowercase UUID"
        )));
    }
    Ok(())
}

impl Omnigraph {
    /// Reconstruct one page from the exact retained generation named by a
    /// durable `DataBlock`. No digest is trusted as a substitute for reopening
    /// the cut and rerunning the validator.
    #[doc(hidden)]
    pub async fn show_stream_data_block(
        &self,
        authority: CheckedClusterBlockAuthority<'_>,
        table_key: &str,
        block_token: &str,
        cursor: Option<&str>,
    ) -> Result<StreamDataBlockPage> {
        if authority.operation_id() != block_token {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "stream-block inspection guard operation id differs from block token"
                    .to_string(),
            });
        }
        self.show_stream_data_block_inner(Some(&authority), table_key, block_token, cursor)
            .await
    }

    async fn show_stream_data_block_inner(
        &self,
        authority: Option<&CheckedClusterBlockAuthority<'_>>,
        table_key: &str,
        block_token: &str,
        cursor: Option<&str>,
    ) -> Result<StreamDataBlockPage> {
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        let provisional = self
            .capture_blocked_stream_authority(table_key, block_token, "stream block show")
            .await?;
        validate_optional_block_authority(authority, provisional.txn.base.stream_profile())?;
        let admission_key = provisional.admission_key.clone();
        let _admission_guard = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;

        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;
        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream block show")
            .await?;

        let capture = self
            .capture_blocked_stream_authority(table_key, block_token, "stream block show")
            .await?;
        validate_optional_block_authority(authority, capture.txn.base.stream_profile())?;
        if capture.admission_key != admission_key {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_block_binding:{table_key}"),
                Some(format!("{admission_key:?}")),
                Some(format!("{:?}", capture.admission_key)),
            ));
        }
        let reconstructed = self.reconstruct_data_block(&capture, block_token).await?;
        let page = data_block_page(
            table_key,
            &capture,
            &reconstructed.block,
            &reconstructed.view,
            cursor,
        )?;

        // Reread manifest and profile authority after the potentially remote
        // WAL scan. A page is never returned from a cut that moved during its
        // construction.
        let final_capture = self
            .capture_blocked_stream_authority(table_key, block_token, "stream block show")
            .await?;
        validate_optional_block_authority(authority, final_capture.txn.base.stream_profile())?;
        if final_capture.txn.base.version() != capture.txn.base.version()
            || final_capture.lifecycle != capture.lifecycle
            || final_capture.txn.base.stream_token_authority()
                != capture.txn.base.stream_token_authority()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_block_show:{table_key}"),
                Some(capture.txn.base.version().to_string()),
                Some(final_capture.txn.base.version().to_string()),
            ));
        }
        Ok(page)
    }

    /// Apply one exact, bounded correction plan. Receipt lookup intentionally
    /// precedes current block/revision checks so a response lost after the
    /// terminal manifest CAS remains an ordinary idempotent retry.
    #[doc(hidden)]
    pub async fn correct_stream_data_block(
        &self,
        authority: CheckedClusterBlockAuthority<'_>,
        table_key: &str,
        request: StreamDataCorrectionRequest,
    ) -> Result<StreamDataCorrectionResult> {
        request.validate_shape()?;
        if authority.operation_id() != request.correction_id {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "stream-block correction guard operation id differs from correction id"
                    .to_string(),
            });
        }
        let actor = authority.actor().to_string();
        self.correct_stream_data_block_inner(Some(&authority), table_key, request, &actor)
            .await
    }

    async fn correct_stream_data_block_inner(
        &self,
        authority: Option<&CheckedClusterBlockAuthority<'_>>,
        table_key: &str,
        request: StreamDataCorrectionRequest,
        actor: &str,
    ) -> Result<StreamDataCorrectionResult> {
        request.validate_shape()?;
        self.heal_pending_recovery_sidecars_outcome().await?;
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;

        let initial = self.open_write_txn(None).await?;
        validate_optional_block_authority(authority, initial.base.stream_profile())?;
        let entry = initial.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "stream block correct cannot resolve unknown table '{table_key}'"
            ))
        })?;
        let lifecycle = initial
            .base
            .stream_lifecycle(entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "stream block correct requires an enrolled stream for '{table_key}'"
                ))
            })?;
        let graph_identity_digest =
            stream_graph_identity_digest(&initial.authority.schema_identity_domain)?;
        let retry_plan_digest = normalized_request_plan_digest(
            &initial.catalog,
            entry.identity,
            &initial.authority.schema_ir_hash,
            &lifecycle.enrollment_receipt.stream_incarnation_id,
            table_key,
            actor,
            &request,
        )?;
        if let Some(result) = self
            .correction_receipt_result(
                &initial.base,
                &graph_identity_digest,
                entry.identity,
                &lifecycle.enrollment_receipt.stream_incarnation_id,
                actor,
                &retry_plan_digest,
                &request,
            )
            .await?
        {
            return Ok(result);
        }

        let provisional = self
            .capture_blocked_stream_authority(
                table_key,
                &request.block_token,
                "stream block correct",
            )
            .await?;
        let admission_key = provisional.admission_key.clone();
        let _admission_guard = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;
        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream block correct")
            .await?;

        // Repeat receipt-first under the final writer gates: another exact
        // recovery owner may have completed between the optimistic lookup and
        // admission acquisition.
        let live_txn = self.open_write_txn(None).await?;
        validate_optional_block_authority(authority, live_txn.base.stream_profile())?;
        let live_lifecycle = live_txn
            .base
            .stream_lifecycle(entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_correction_lifecycle:{table_key}"),
                    Some(format!("{:?}", lifecycle)),
                    None,
                )
            })?;
        if let Some(result) = self
            .correction_receipt_result(
                &live_txn.base,
                &graph_identity_digest,
                entry.identity,
                &live_lifecycle.enrollment_receipt.stream_incarnation_id,
                actor,
                &retry_plan_digest,
                &request,
            )
            .await?
        {
            return Ok(result);
        }

        let capture = self
            .capture_blocked_stream_authority(
                table_key,
                &request.block_token,
                "stream block correct",
            )
            .await?;
        validate_optional_block_authority(authority, capture.txn.base.stream_profile())?;
        if capture.admission_key != admission_key {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_correction_binding:{table_key}"),
                Some(format!("{admission_key:?}")),
                Some(format!("{:?}", capture.admission_key)),
            ));
        }
        if capture.lifecycle.lifecycle_revision != request.expected_lifecycle_revision {
            return Err(OmniError::StreamLifecycleChanged {
                stable_table_id: capture.entry.identity.stable_table_id,
                table_incarnation_id: capture.entry.identity.table_incarnation_id,
                expected_revision: request.expected_lifecycle_revision,
                current_revision: capture.lifecycle.lifecycle_revision,
            });
        }
        let reconstructed = self
            .reconstruct_data_block(&capture, &request.block_token)
            .await?;
        let prepared = self
            .prepare_correction_overlay(&capture, &reconstructed, &request, actor)
            .await?;
        if prepared.plan_digest != retry_plan_digest {
            return Err(OmniError::manifest_internal(
                "receipt-first and exact-cut correction plan digests differ",
            ));
        }
        self.publish_prepared_data_correction(
            &capture,
            reconstructed,
            prepared,
            actor,
            &graph_identity_digest,
            &request,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn correction_receipt_result(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        graph_identity_digest: &str,
        identity: crate::db::manifest::TableIdentity,
        stream_incarnation_id: &str,
        actor: &str,
        plan_digest: &str,
        request: &StreamDataCorrectionRequest,
    ) -> Result<Option<StreamDataCorrectionResult>> {
        let token_dataset = snapshot.open_stream_token_authority().await?;
        let correction_receipt = lookup_stream_correction_receipt(
            &token_dataset,
            snapshot.stream_token_authority(),
            graph_identity_digest,
            identity,
            stream_incarnation_id,
            &request.block_token,
            &request.correction_id,
        )
        .await?;
        let management_receipt = lookup_management_receipt(
            &token_dataset,
            snapshot.stream_token_authority(),
            graph_identity_digest,
            identity,
            stream_incarnation_id,
            STREAM_CORRECTION_OPERATION_KIND,
            &request.correction_id,
        )
        .await?;
        let receipt = match (correction_receipt, management_receipt) {
            (None, None) => return Ok(None),
            (None, Some(_)) => {
                return Err(correction_idempotency_conflict(identity, request));
            }
            (Some(_), None) => {
                return Err(OmniError::manifest_internal(
                    "stream correction receipt exists without its paired management receipt",
                ));
            }
            (Some(receipt), Some(management)) => {
                validate_correction_receipt_pair(&receipt, &management)?;
                receipt
            }
        };
        validate_correction_receipt_retry(&receipt, actor, plan_digest, request)?;
        Ok(Some(StreamDataCorrectionResult {
            changed: false,
            correction_id: receipt.correction_id,
            plan_digest: receipt.correction_plan_digest,
            graph_commit_id: receipt.graph_commit_id,
            lifecycle_revision: receipt.resulting_lifecycle_revision,
            manifest_version: receipt.resulting_manifest_version,
        }))
    }

    /// Feature-gated evidence seam used by the existing MemWAL integration
    /// suite. Production callers can reach the same inner path only through a
    /// checked cluster offline authority.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_show_stream_data_block_for_test(
        &self,
        table_key: &str,
        block_token: &str,
        cursor: Option<&str>,
    ) -> Result<StreamDataBlockPage> {
        self.show_stream_data_block_inner(None, table_key, block_token, cursor)
            .await
    }

    /// Feature-gated correction seam paired with
    /// [`Self::failpoint_show_stream_data_block_for_test`]. It skips only the
    /// external cluster capability wrapper; profile, admission, graph gates,
    /// exact recovery, and publication are unchanged.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_correct_stream_data_block_for_test(
        &self,
        table_key: &str,
        request: StreamDataCorrectionRequest,
        actor: &str,
    ) -> Result<StreamDataCorrectionResult> {
        self.correct_stream_data_block_inner(None, table_key, request, actor)
            .await
    }

    async fn capture_blocked_stream_authority(
        &self,
        table_key: &str,
        block_token: &str,
        operation: &str,
    ) -> Result<StreamAuthorityCapture> {
        let txn = self.open_write_txn(None).await?;
        let entry = txn.base.entry(table_key).ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "{operation} cannot resolve unknown table '{table_key}'"
            ))
        })?;
        let lifecycle = txn.base.stream_lifecycle(entry.identity).ok_or_else(|| {
            OmniError::manifest_conflict(format!(
                "{operation} requires an enrolled stream for '{table_key}'"
            ))
        })?;
        if lifecycle.lifecycle != StreamLifecycle::Draining {
            return Err(OmniError::manifest_conflict(format!(
                "{operation} requires a DRAINING stream for '{table_key}'"
            )));
        }
        let drain = lifecycle.drain.as_ref().ok_or_else(|| {
            OmniError::manifest_internal("blocked DRAINING lifecycle has no drain descriptor")
        })?;
        let block = lifecycle.strict_block.as_ref().ok_or_else(|| {
            OmniError::manifest_conflict(format!(
                "{operation} requires a strict block for '{table_key}'"
            ))
        })?;
        if block.block_token != block_token {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_block_token:{table_key}"),
                Some(block_token.to_string()),
                Some(block.block_token.clone()),
            ));
        }
        if !matches!(block.evidence, StrictBlockEvidence::DataBlock { .. }) {
            return Err(OmniError::manifest_conflict(
                "reserved stream AuthorityBlock is not repairable by data correction",
            ));
        }
        self.capture_draining_stream_authority(table_key, operation, &drain.drain_id)
            .await
    }

    async fn reconstruct_data_block(
        &self,
        capture: &StreamAuthorityCapture,
        block_token: &str,
    ) -> Result<ReconstructedDataBlock> {
        let block = capture.lifecycle.strict_block.clone().ok_or_else(|| {
            OmniError::manifest_conflict("stream correction requires a current strict block")
        })?;
        if block.block_token != block_token
            || block.correction_revision != capture.lifecycle.lifecycle_revision
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_block_revision:{}", capture.entry.table_key),
                Some(format!(
                    "{}:{}",
                    block_token, capture.lifecycle.lifecycle_revision
                )),
                Some(format!(
                    "{}:{}",
                    block.block_token, block.correction_revision
                )),
            ));
        }
        let StrictBlockEvidence::DataBlock {
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
        } = &block.evidence
        else {
            return Err(OmniError::manifest_conflict(
                "reserved stream AuthorityBlock is not repairable by data correction",
            ));
        };
        if *validation_contract_version != STREAM_DATA_BLOCK_VALIDATION_CONTRACT_VERSION
            || enrollment_id != &capture.binding.enrollment_id
            || shard_id != &capture.shard_id.to_string()
            || base_current_head_witness != &capture.lifecycle.current_head_witness
        {
            return Err(OmniError::manifest_internal(
                "stream DataBlock no longer agrees with its selected lifecycle authority",
            ));
        }

        let physical = validate_b1_lifecycle_physical_state_with_binding_inventory(
            capture.head.dataset(),
            &capture.lifecycle,
            capture.retained_shard_inventory.as_ref(),
        )
        .await
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let flushed = match physical {
            PassiveB1PhysicalState::FoldOnlyFlushed(flushed)
                if flushed.shard_manifest_version == *shard_manifest_version
                    && flushed.writer_epoch == *writer_epoch
                    && flushed.generation == *generation
                    && flushed.path == *generation_path
                    && flushed.replay_after_wal_entry_position == *replay_cursor =>
            {
                flushed
            }
            observed => {
                return Err(OmniError::recovery_required(
                    format!("stream-data-block:{block_token}"),
                    format!(
                        "retained DataBlock generation differs from its exact durable cut: {observed:?}"
                    ),
                ));
            }
        };
        let batches = scan_flushed_generation_projection(
            capture.head.dataset(),
            &capture.full_path,
            capture.shard_id,
            &flushed,
        )
        .await
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if batches.is_empty() {
            return Err(OmniError::manifest_internal(
                "retained DataBlock generation reconstructed an empty winner projection",
            ));
        }
        let attribution = plan_fold_attribution(
            &capture.txn.base,
            capture.entry.identity,
            &capture.lifecycle,
            &capture.binding,
            &batches,
        )
        .await?;
        validate_stream_token_plan_bounds(&attribution.token_rows)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let winner_tokens = attribution
            .token_rows
            .iter()
            .map(|row| (row.logical_id.clone(), row.current_token.to_string()))
            .collect::<BTreeMap<_, _>>();
        let mut changeset = ChangeSet::new();
        changeset.insert(
            capture.entry.table_key.clone(),
            TableChange {
                added: Vec::new(),
                changed: batches.clone(),
                deleted_ids: Vec::new(),
            },
        );
        let constraints = crate::validate::constraints_for(&capture.txn.catalog);
        let committed = CommittedState::write(&capture.txn.base, self, None);
        let mut collector =
            DataBlockEvidenceCollector::new(&capture.entry.table_key, &winner_tokens);
        crate::validate::evaluate_with_sink(
            &constraints,
            &changeset,
            &committed,
            &capture.txn.catalog,
            |violation| collector.push(&violation),
        )
        .await?;
        let view = collector.finish_with_view()?.ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_block_validation:{}", capture.entry.table_key),
                Some("durable validator violation".to_string()),
                Some("no validator violation".to_string()),
            )
        })?;
        if view.evidence.violation_code != *violation_code
            || view.evidence.violation_digest != *violation_digest
            || view.evidence.correction_view_digest != *correction_view_digest
            || view.evidence.offending_key_count != *offending_key_count
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_block_evidence:{}", capture.entry.table_key),
                Some(correction_view_digest.clone()),
                Some(view.evidence.correction_view_digest.clone()),
            ));
        }
        Ok(ReconstructedDataBlock {
            block,
            view,
            batches,
            attribution,
        })
    }

    async fn prepare_correction_overlay(
        &self,
        capture: &StreamAuthorityCapture,
        reconstructed: &ReconstructedDataBlock,
        request: &StreamDataCorrectionRequest,
        actor: &str,
    ) -> Result<PreparedCorrectionOverlay> {
        request.validate_shape()?;
        let entries = reconstructed
            .view
            .entries
            .iter()
            .map(|entry| (entry.ordinal, entry))
            .collect::<BTreeMap<_, _>>();
        let mut actions = BTreeMap::new();
        for action in &request.actions {
            let entry = entries.get(&action.ordinal()).ok_or_else(|| {
                OmniError::manifest(format!(
                    "stream correction action ordinal {} is not in the exact correction view",
                    action.ordinal()
                ))
            })?;
            if entry.logical_key != action.logical_key()
                || entry.current_blocked_winner_stream_token != action.predecessor()
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("stream_correction_entry:{}", action.ordinal()),
                    Some(format!("{}:{}", action.logical_key(), action.predecessor())),
                    Some(format!(
                        "{}:{}",
                        entry.logical_key, entry.current_blocked_winner_stream_token
                    )),
                ));
            }
            let action_name = match action {
                StreamDataCorrectionAction::Replace { .. } => "REPLACE",
                StreamDataCorrectionAction::Withdraw { .. } => "WITHDRAW",
            };
            if !entry
                .allowed_actions
                .iter()
                .any(|allowed| allowed == action_name)
            {
                return Err(OmniError::manifest(format!(
                    "stream correction action {action_name} is not allowed for ordinal {}",
                    action.ordinal()
                )));
            }
            actions.insert(action.logical_key().to_string(), action);
        }

        let contributor = TrustedContributorId::new(actor.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let public_catalog = super::public_catalog_view(&capture.txn.catalog)?;
        let mut source_rows = physical_rows_by_logical_id(
            &capture.entry.table_key,
            capture.entry.identity,
            &reconstructed.batches,
        )?;
        let original_tokens = reconstructed
            .attribution
            .token_rows
            .iter()
            .map(|row| (row.logical_id.clone(), row.clone()))
            .collect::<BTreeMap<_, _>>();
        if source_rows.len() != original_tokens.len()
            || !source_rows.keys().eq(original_tokens.keys())
        {
            return Err(OmniError::manifest_internal(
                "stream correction row projection differs from attributed winner keys",
            ));
        }

        let mut result_rows = BTreeMap::new();
        let mut result_tokens = BTreeMap::new();
        let mut replacement_payloads = BTreeMap::new();
        for (logical_id, authority_row) in original_tokens {
            match actions.get(&logical_id).copied() {
                None => {
                    result_rows.insert(
                        logical_id.clone(),
                        source_rows.remove(&logical_id).expect("key sets checked"),
                    );
                    result_tokens.insert(logical_id, authority_row);
                }
                Some(StreamDataCorrectionAction::Withdraw { .. }) => {
                    let authority_row = StreamTokenAuthorityRow::withdraw_blocked_winner(
                        &authority_row,
                        contributor.clone(),
                        request.correction_id.clone(),
                    )
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                    result_tokens.insert(logical_id, authority_row);
                }
                Some(StreamDataCorrectionAction::Replace {
                    ordinal,
                    write_id,
                    row,
                    ..
                }) => {
                    let logical_batch = crate::loader::normalize_stream_json_row(
                        &public_catalog,
                        &capture.entry.table_key,
                        row.clone(),
                    )?;
                    validate_stream_input_bounds(&capture.entry.table_key, &logical_batch)?;
                    validate_stream_value_constraints(
                        &capture.entry.table_key,
                        &logical_batch,
                        &public_catalog,
                    )?;
                    let ids = logical_batch
                        .column_by_name("id")
                        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                        .ok_or_else(|| {
                            OmniError::manifest_internal(
                                "normalized correction row has no exact Utf8 id column",
                            )
                        })?;
                    if logical_batch.num_rows() != 1 || ids.is_null(0) || ids.value(0) != logical_id
                    {
                        return Err(OmniError::manifest(format!(
                            "stream correction REPLACE row id must equal '{logical_id}'"
                        )));
                    }
                    let canonical_payload = super::canonical_stream_payload_v1(&logical_batch, 0)?;
                    let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
                        identity: capture.entry.identity,
                        accepted_schema_hash: &capture.txn.authority.schema_ir_hash,
                        canonical_payload: &canonical_payload,
                    })
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                    let metadata = TrustedStreamRowMetadata::new_correction(
                        &authority_row,
                        contributor.clone(),
                        write_id.clone(),
                        payload_digest,
                        request.correction_id.clone(),
                        *ordinal,
                    )
                    .map_err(|error| OmniError::manifest(error.to_string()))?;
                    let attributed = append_trusted_stream_metadata(
                        logical_batch,
                        vec![Some(metadata.clone())],
                    )?;
                    let next_authority = StreamTokenAuthorityRow::from_present_metadata(
                        capture.entry.identity,
                        logical_id.clone(),
                        capture.binding.enrollment_id.clone(),
                        &metadata,
                    )
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                    replacement_payloads.insert(*ordinal, payload_digest.to_string());
                    result_rows.insert(logical_id.clone(), attributed);
                    result_tokens.insert(logical_id, next_authority);
                }
            }
        }
        if result_tokens.len() != reconstructed.attribution.token_rows.len() {
            return Err(OmniError::manifest_internal(
                "stream correction dropped token authority for a generation winner",
            ));
        }
        let batches = result_rows.into_values().collect::<Vec<_>>();
        let token_rows = result_tokens.into_values().collect::<Vec<_>>();
        validate_stream_token_plan_bounds(&token_rows)
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let (visible_rows, visible_bytes) = fold_output_size(&batches)?;
        if visible_rows > 8_192 || visible_bytes > 32 * 1024 * 1024 {
            return Err(OmniError::resource_limit(
                format!("stream correction output for {}", capture.entry.table_key),
                32 * 1024 * 1024,
                visible_bytes,
            ));
        }

        // The full corrected generation—not merely the mentioned entries—must
        // satisfy every validator against the same pinned committed snapshot.
        let mut changeset = ChangeSet::new();
        changeset.insert(
            capture.entry.table_key.clone(),
            TableChange {
                added: Vec::new(),
                changed: batches.clone(),
                deleted_ids: Vec::new(),
            },
        );
        let constraints = crate::validate::constraints_for(&capture.txn.catalog);
        let committed = CommittedState::write(&capture.txn.base, self, None);
        let mut first_violation = None;
        crate::validate::evaluate_with_sink(
            &constraints,
            &changeset,
            &committed,
            &capture.txn.catalog,
            |violation| {
                if first_violation.is_none() {
                    first_violation = Some(violation);
                }
                Ok(())
            },
        )
        .await?;
        if let Some(violation) = first_violation {
            return Err(violation.into_omni_error());
        }

        let present_rows = token_rows
            .iter()
            .filter(|row| row.disposition == StreamTokenDisposition::Present)
            .cloned()
            .collect::<Vec<_>>();
        let attribution_summary = (!present_rows.is_empty())
            .then(|| stream_fold_attribution_commitment(&present_rows))
            .transpose()
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let plan_digest = correction_plan_digest(capture, request, actor, &replacement_payloads)?;
        if request
            .expected_plan_digest
            .as_deref()
            .is_some_and(|expected| expected != plan_digest)
        {
            return Err(OmniError::manifest_read_set_changed(
                "stream_correction_plan_digest",
                request.expected_plan_digest.clone(),
                Some(plan_digest),
            ));
        }
        Ok(PreparedCorrectionOverlay {
            batches,
            token_rows,
            attribution_summary,
            plan_digest,
            visible_rows,
            visible_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_prepared_data_correction(
        &self,
        capture: &StreamAuthorityCapture,
        reconstructed: ReconstructedDataBlock,
        prepared: PreparedCorrectionOverlay,
        actor: &str,
        graph_identity_digest: &str,
        request: &StreamDataCorrectionRequest,
    ) -> Result<StreamDataCorrectionResult> {
        let StrictBlockEvidence::DataBlock {
            generation,
            generation_path,
            shard_manifest_version,
            writer_epoch,
            replay_cursor,
            ..
        } = &reconstructed.block.evidence
        else {
            return Err(OmniError::manifest_conflict(
                "reserved stream AuthorityBlock is not repairable by data correction",
            ));
        };

        let final_head = self
            .storage()
            .open_dataset_head(&capture.full_path, None)
            .await?;
        self.ensure_existing_effect_baseline(
            &capture.entry.table_key,
            None,
            capture.entry.table_version,
            &final_head,
        )
        .await?;
        let final_witness = capture_current_head_witness(final_head.dataset())
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if final_witness != capture.lifecycle.current_head_witness {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_correction_head:{}", capture.entry.table_key),
                Some(format!("{:?}", capture.lifecycle.current_head_witness)),
                Some(format!("{final_witness:?}")),
            ));
        }
        let final_details = final_head
            .dataset()
            .mem_wal_index_details()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal("stream correction lost its MemWAL index")
            })?;
        let prior_merged =
            super::stream_ingest::exact_merged_generation(&final_details, capture.shard_id)?;

        let base_staged = self
            .storage()
            .stage_stream_correction(
                capture.head.clone(),
                &capture.entry.table_key,
                prepared.batches.clone(),
                capture.shard_id,
                *generation,
            )
            .await?;
        let base_planned_transaction = base_staged.transaction_identity();
        let current_claim_receipt = self
            .selected_claim_receipt(&capture.txn.base, &capture.lifecycle)
            .await?;
        let lineage = self
            .new_lineage_intent_for_branch(None, Some("omnigraph:stream-fold"))
            .await?;
        let recorded_at = lineage.created_at;
        let resulting_manifest_version =
            capture.txn.base.version().checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal("stream correction manifest version overflow")
            })?;
        let next_revision = capture
            .lifecycle
            .lifecycle_revision
            .checked_add(1)
            .ok_or_else(|| {
                OmniError::manifest_internal("stream correction lifecycle revision overflow")
            })?;
        let result_payload = stream_correction_result_payload(
            &request.correction_id,
            &prepared.plan_digest,
            &lineage.graph_commit_id,
            resulting_manifest_version,
            next_revision,
        )?;
        let management_receipt = ManagementReceipt::new(
            graph_identity_digest.to_string(),
            capture.entry.identity,
            capture
                .lifecycle
                .enrollment_receipt
                .stream_incarnation_id
                .clone(),
            capture.lifecycle.binding_scope_id.clone(),
            &capture.lifecycle.management_receipt_chain,
            request.correction_id.clone(),
            STREAM_CORRECTION_OPERATION_KIND,
            capture.lifecycle.lifecycle_revision,
            next_revision,
            actor.to_string(),
            serde_json::json!({
                "block_token": request.block_token,
                "correction_plan_digest": prepared.plan_digest,
            }),
            result_payload,
            recorded_at,
        )?;
        let post_commit_pin = capture.entry.table_version.checked_add(1).ok_or_else(|| {
            OmniError::manifest_internal("stream correction table version overflow")
        })?;
        let planned_next_head = crate::db::manifest::CurrentHeadWitness {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            table_version: post_commit_pin,
            transaction_uuid: base_planned_transaction.uuid.clone(),
            manifest_e_tag: None,
        };
        let next_lifecycle = build_data_block_correction_successor(
            &capture.lifecycle,
            &request.block_token,
            &prepared.plan_digest,
            &management_receipt,
            StreamDataCorrectionOutcome {
                graph_commit_id: lineage.graph_commit_id.clone(),
                manifest_version: resulting_manifest_version,
                current_head_witness: planned_next_head,
                visible_rows: prepared.visible_rows,
                visible_bytes: prepared.visible_bytes,
                recorded_at,
            },
        )?;
        let resulting_lifecycle_digest = stream_lifecycle_authority_digest(&next_lifecycle)?;
        let resulting_token_authority_digest =
            stream_token_authority_plan_digest(&prepared.token_rows)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let correction_receipt = StreamCorrectionReceipt::new(StreamCorrectionReceiptPreimage {
            graph_identity_digest: graph_identity_digest.to_string(),
            identity: capture.entry.identity,
            stream_incarnation_id: capture
                .lifecycle
                .enrollment_receipt
                .stream_incarnation_id
                .clone(),
            binding_scope_id: capture.lifecycle.binding_scope_id.clone(),
            block_token: request.block_token.clone(),
            correction_id: request.correction_id.clone(),
            correction_plan_digest: prepared.plan_digest.clone(),
            actor_id: actor.to_string(),
            graph_commit_id: lineage.graph_commit_id.clone(),
            resulting_manifest_version,
            resulting_lifecycle_revision: next_revision,
            resulting_lifecycle_digest,
            resulting_token_authority_digest,
            recorded_at,
        })?;
        let token_dataset = capture.txn.base.open_stream_token_authority().await?;
        let token_staged = stage_stream_correction_effect(
            token_dataset,
            capture.txn.base.stream_token_authority(),
            &prepared.token_rows,
            &correction_receipt,
            &management_receipt,
        )
        .await?;
        let token_planned_transaction = token_staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                capture.txn.base.stream_token_authority(),
                &crate::lance_access::control_session(),
            )
            .await?,
        );
        let token_staged = StagedHandle::new(token_staged);

        let authority = RecoveryAuthorityToken {
            branch_identifier: capture.txn.authority.branch_identifier.clone(),
            graph_head: capture.txn.authority.graph_head.clone(),
            schema_identity_domain: capture.txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: capture.txn.authority.schema_ir_hash.clone(),
            schema_identity_version: capture.txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id.clone(),
            branch: lineage.branch.clone(),
            actor_id: lineage.actor_id.clone(),
            merged_parent_commit_id: lineage.merged_parent_commit_id.clone(),
            created_at: lineage.created_at,
        };
        let pin = SidecarTablePin {
            identity: capture.entry.identity,
            table_key: capture.entry.table_key.clone(),
            table_path: capture.full_path.clone(),
            expected_version: capture.entry.table_version,
            post_commit_pin,
            confirmed_version: None,
            table_branch: None,
        };
        let generation_cut = RecoveryStreamFoldCut {
            shard_id: capture.shard_id,
            writer_epoch: *writer_epoch,
            shard_manifest_version: *shard_manifest_version,
            replay_after_wal_entry_position: *replay_cursor,
            generation: *generation,
            generation_path: generation_path.clone(),
        };
        let mut sidecar = new_stream_correction_sidecar_v20(
            pin,
            authority,
            recovery_lineage,
            capture.txn.base.version(),
            capture.txn.base.stream_profile().clone(),
            capture.lifecycle.clone(),
            current_claim_receipt,
            next_lifecycle,
            prior_merged,
            generation_cut,
            base_planned_transaction,
            capture.txn.base.stream_token_authority().clone(),
            token_planned_transaction,
            reconstructed.attribution.token_rows.clone(),
            prepared.token_rows.clone(),
            correction_receipt.clone(),
            management_receipt.clone(),
            prepared.attribution_summary,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_SIDECAR_PRE_BASE_COMMIT,
        )
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("stream correction stopped after arming its recovery intent: {error}"),
            )
        })?;

        let base_outcome = match self
            .storage()
            .commit_staged_exact(final_head, base_staged)
            .await
        {
            Ok(outcome) if outcome.is_exact() => outcome,
            Ok(_) => {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "stream correction base participant committed a non-exact transaction",
                ));
            }
            Err(error) => {
                let recovered = complete_stream_correction_sidecar_v20(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &capture.txn.base,
                    &sidecar,
                )
                .await;
                if recovered.is_ok() {
                    self.refresh_coordinator_only().await?;
                    return Ok(StreamDataCorrectionResult {
                        changed: true,
                        correction_id: correction_receipt.correction_id,
                        plan_digest: correction_receipt.correction_plan_digest,
                        graph_commit_id: correction_receipt.graph_commit_id,
                        lifecycle_revision: correction_receipt.resulting_lifecycle_revision,
                        manifest_version: resulting_manifest_version,
                    });
                }
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    format!(
                        "stream correction base commit failed ({error}) and exact recovery did not complete: {}",
                        recovered.expect_err("checked as error")
                    ),
                ));
            }
        };
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT,
        )
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("stream correction stopped after its exact base effect: {error}"),
            )
        })?;
        let base_state = self
            .storage()
            .table_state(&capture.full_path, base_outcome.snapshot())
            .await
            .map_err(|error| {
                OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
            })?;
        let base_version_metadata = crate::db::manifest::TableVersionMetadata::from_dataset(
            self.root_uri(),
            &capture.entry.table_path,
            base_outcome.snapshot().dataset(),
        )
        .map_err(|error| {
            OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
        })?;
        let token_outcome = match self
            .storage()
            .commit_staged_exact(token_head, token_staged)
            .await
        {
            Ok(outcome) if outcome.is_exact() => outcome,
            Ok(_) => {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "stream correction token participant committed a non-exact transaction",
                ));
            }
            Err(error) => {
                let recovered = complete_stream_correction_sidecar_v20(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &capture.txn.base,
                    &sidecar,
                )
                .await;
                if recovered.is_ok() {
                    self.refresh_coordinator_only().await?;
                    return Ok(StreamDataCorrectionResult {
                        changed: true,
                        correction_id: correction_receipt.correction_id,
                        plan_digest: correction_receipt.correction_plan_digest,
                        graph_commit_id: correction_receipt.graph_commit_id,
                        lifecycle_revision: correction_receipt.resulting_lifecycle_revision,
                        manifest_version: resulting_manifest_version,
                    });
                }
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    format!(
                        "stream correction token commit failed ({error}) and exact recovery did not complete: {}",
                        recovered.expect_err("checked as error")
                    ),
                ));
            }
        };

        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM,
        )
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("stream correction stopped after both exact effects: {error}"),
            )
        })?;

        let achieved_base_head = capture_current_head_witness(base_outcome.snapshot().dataset())
            .await
            .map_err(|error| {
                OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
            })?;
        let next_token_authority =
            stream_token_authority_entry_for_dataset(token_outcome.snapshot().dataset())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
        let achieved_token_head = next_token_authority.current_head_witness.clone();
        confirm_stream_correction_sidecar_v20(
            self.root_uri(),
            self.storage_adapter(),
            &mut sidecar,
            base_outcome.committed_transaction().clone(),
            lance_index::mem_wal::MergedGeneration::new(capture.shard_id, *generation),
            achieved_base_head,
            crate::db::SubTableUpdate {
                identity: capture.entry.identity,
                table_key: capture.entry.table_key.clone(),
                table_version: base_state.version,
                table_branch: None,
                row_count: base_state.row_count,
                version_metadata: base_version_metadata,
            },
            token_outcome.committed_transaction().clone(),
            achieved_token_head,
            next_token_authority,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("stream correction confirmation requires recovery: {error}"),
            )
        })?;
        complete_stream_correction_sidecar_v20(
            self.root_uri(),
            Arc::clone(&self.storage),
            &capture.txn.base,
            &sidecar,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id,
                format!("stream correction completion requires recovery: {error}"),
            )
        })?;
        self.refresh_coordinator_only().await?;
        Ok(StreamDataCorrectionResult {
            changed: true,
            correction_id: correction_receipt.correction_id,
            plan_digest: correction_receipt.correction_plan_digest,
            graph_commit_id: correction_receipt.graph_commit_id,
            lifecycle_revision: correction_receipt.resulting_lifecycle_revision,
            manifest_version: resulting_manifest_version,
        })
    }
}

fn validate_optional_block_authority(
    authority: Option<&CheckedClusterBlockAuthority<'_>>,
    profile: &crate::db::manifest::StreamProfileEntry,
) -> Result<()> {
    match authority {
        Some(authority) => authority.validate_profile(profile),
        None => {
            if matches!(
                profile.mode(),
                crate::db::manifest::StreamProfileMode::Enabled
                    | crate::db::manifest::StreamProfileMode::Disabling
            ) {
                Ok(())
            } else {
                Err(OmniError::manifest_conflict(
                    "stream DataBlock control requires an ENABLED or DISABLING profile",
                ))
            }
        }
    }
}

fn physical_rows_by_logical_id(
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    batches: &[RecordBatch],
) -> Result<BTreeMap<String, RecordBatch>> {
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream correction source has no exact non-null Utf8 id column",
                )
            })?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "stream correction source contains a null id",
                ));
            }
            let logical_id = ids.value(row).to_string();
            let index = UInt32Array::from(vec![u32::try_from(row).map_err(|_| {
                OmniError::manifest_internal("stream correction row ordinal exceeds u32")
            })?]);
            let columns = batch
                .columns()
                .iter()
                .map(|column| {
                    take(column.as_ref(), &index, None)
                        .map_err(|error| OmniError::Lance(error.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            let one = RecordBatch::try_new(batch.schema(), columns)
                .map_err(|error| OmniError::Lance(error.to_string()))?;
            let metadata = one
                .column_by_name(crate::db::STREAM_METADATA_COLUMN)
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream correction source is missing trusted row metadata",
                    )
                })?;
            let decoded = crate::db::manifest::stream_token::decode_trusted_stream_metadata(
                metadata.as_ref(),
                0,
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream correction source contains unattributed winner row",
                )
            })?;
            decoded
                .validate_for(identity, &logical_id)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            if rows.insert(logical_id.clone(), one).is_some() {
                return Err(OmniError::key_conflict(table_key, logical_id));
            }
        }
    }
    Ok(rows)
}

fn correction_plan_digest(
    capture: &StreamAuthorityCapture,
    request: &StreamDataCorrectionRequest,
    actor: &str,
    replacement_payloads: &BTreeMap<u64, String>,
) -> Result<String> {
    correction_plan_digest_parts(
        &capture.lifecycle.enrollment_receipt.stream_incarnation_id,
        &capture.txn.authority.schema_ir_hash,
        request,
        actor,
        replacement_payloads,
    )
}

fn correction_plan_digest_parts(
    stream_incarnation_id: &str,
    schema_ir_hash: &str,
    request: &StreamDataCorrectionRequest,
    actor: &str,
    replacement_payloads: &BTreeMap<u64, String>,
) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(STREAM_DATA_CORRECTION_PLAN_DOMAIN);
    hash_plan_field(&mut hasher, stream_incarnation_id.as_bytes());
    hash_plan_field(&mut hasher, request.block_token.as_bytes());
    hash_plan_field(&mut hasher, request.correction_id.as_bytes());
    hasher.update(request.expected_lifecycle_revision.to_be_bytes());
    hash_plan_field(&mut hasher, actor.as_bytes());
    hash_plan_field(&mut hasher, schema_ir_hash.as_bytes());
    hasher.update(
        u64::try_from(request.actions.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for action in &request.actions {
        hasher.update(action.ordinal().to_be_bytes());
        hash_plan_field(&mut hasher, action.logical_key().as_bytes());
        hash_plan_field(&mut hasher, action.predecessor().as_bytes());
        match action {
            StreamDataCorrectionAction::Replace {
                ordinal, write_id, ..
            } => {
                hasher.update([0]);
                hash_plan_field(&mut hasher, write_id.as_bytes());
                hash_plan_field(
                    &mut hasher,
                    replacement_payloads
                        .get(ordinal)
                        .ok_or_else(|| {
                            OmniError::manifest_internal(
                                "normalized correction plan omitted replacement payload digest",
                            )
                        })?
                        .as_bytes(),
                );
            }
            StreamDataCorrectionAction::Withdraw { .. } => hasher.update([1]),
        }
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn normalized_request_plan_digest(
    catalog: &omnigraph_compiler::catalog::Catalog,
    identity: crate::db::manifest::TableIdentity,
    schema_ir_hash: &str,
    stream_incarnation_id: &str,
    table_key: &str,
    actor: &str,
    request: &StreamDataCorrectionRequest,
) -> Result<String> {
    let public_catalog = super::public_catalog_view(catalog)?;
    let mut replacement_payloads = BTreeMap::new();
    let mut preprocessing_bytes = 0_u64;
    for action in &request.actions {
        let StreamDataCorrectionAction::Replace {
            ordinal,
            logical_key,
            row,
            ..
        } = action
        else {
            continue;
        };
        let batch =
            crate::loader::normalize_stream_json_row(&public_catalog, table_key, row.clone())?;
        validate_stream_input_bounds(table_key, &batch)?;
        validate_stream_value_constraints(table_key, &batch, &public_catalog)?;
        charge_stream_correction_preprocessing_bytes(
            &mut preprocessing_bytes,
            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
            STREAM_DATA_CORRECTION_MAX_PREPROCESSING_BYTES,
        )?;
        let ids = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "normalized correction plan row has no exact Utf8 id column",
                )
            })?;
        if batch.num_rows() != 1 || ids.is_null(0) || ids.value(0) != logical_key {
            return Err(OmniError::manifest(format!(
                "stream correction REPLACE row id must equal '{logical_key}'"
            )));
        }
        let canonical_payload = super::canonical_stream_payload_v1(&batch, 0)?;
        charge_stream_correction_preprocessing_bytes(
            &mut preprocessing_bytes,
            u64::try_from(canonical_payload.len()).unwrap_or(u64::MAX),
            STREAM_DATA_CORRECTION_MAX_PREPROCESSING_BYTES,
        )?;
        let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
            identity,
            accepted_schema_hash: schema_ir_hash,
            canonical_payload: &canonical_payload,
        })
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        replacement_payloads.insert(*ordinal, payload_digest.to_string());
    }
    let digest = correction_plan_digest_parts(
        stream_incarnation_id,
        schema_ir_hash,
        request,
        actor,
        &replacement_payloads,
    )?;
    if request
        .expected_plan_digest
        .as_deref()
        .is_some_and(|expected| expected != digest)
    {
        return Err(OmniError::manifest_read_set_changed(
            "stream_correction_plan_digest",
            request.expected_plan_digest.clone(),
            Some(digest),
        ));
    }
    Ok(digest)
}

fn charge_stream_correction_preprocessing_bytes(
    current: &mut u64,
    amount: u64,
    byte_limit: u64,
) -> Result<()> {
    let actual = current.checked_add(amount).unwrap_or(u64::MAX);
    if actual > byte_limit {
        return Err(OmniError::resource_limit(
            "stream_correction_preprocessing_bytes",
            byte_limit,
            actual,
        ));
    }
    *current = actual;
    Ok(())
}

fn validate_correction_receipt_retry(
    receipt: &StreamCorrectionReceipt,
    actor: &str,
    plan_digest: &str,
    request: &StreamDataCorrectionRequest,
) -> Result<()> {
    receipt.validate()?;
    let expected_resulting_revision = request.expected_lifecycle_revision.checked_add(1);
    if receipt.actor_id != actor
        || receipt.correction_plan_digest != plan_digest
        || receipt.block_token != request.block_token
        || receipt.correction_id != request.correction_id
        || expected_resulting_revision != Some(receipt.resulting_lifecycle_revision)
    {
        return Err(OmniError::StreamLifecycleIdempotencyConflict {
            stable_table_id: receipt.identity.stable_table_id,
            table_incarnation_id: receipt.identity.table_incarnation_id,
            operation_kind: "CORRECTION".to_string(),
            operation_id: request.correction_id.clone(),
        });
    }
    Ok(())
}

fn validate_correction_receipt_pair(
    receipt: &StreamCorrectionReceipt,
    management: &ManagementReceipt,
) -> Result<()> {
    management.validate(management.to_revision)?;
    let request_block_token = management
        .request_payload
        .get("block_token")
        .and_then(serde_json::Value::as_str);
    let request_plan_digest = management
        .request_payload
        .get("correction_plan_digest")
        .and_then(serde_json::Value::as_str);
    if management.operation_kind != STREAM_CORRECTION_OPERATION_KIND
        || management.operation_id != receipt.correction_id
        || management.graph_identity_digest != receipt.graph_identity_digest
        || management.identity != receipt.identity
        || management.stream_incarnation_id != receipt.stream_incarnation_id
        || management.binding_scope_id != receipt.binding_scope_id
        || management.actor_id != receipt.actor_id
        || management.to_revision != receipt.resulting_lifecycle_revision
        || management.result_payload != receipt.result_payload
        || management.result_digest != receipt.result_digest
        || management.recorded_at != receipt.recorded_at
        || request_block_token != Some(receipt.block_token.as_str())
        || request_plan_digest != Some(receipt.correction_plan_digest.as_str())
    {
        return Err(OmniError::manifest_internal(
            "stream correction receipt differs from its paired management receipt",
        ));
    }
    Ok(())
}

fn correction_idempotency_conflict(
    identity: crate::db::manifest::TableIdentity,
    request: &StreamDataCorrectionRequest,
) -> OmniError {
    OmniError::StreamLifecycleIdempotencyConflict {
        stable_table_id: identity.stable_table_id,
        table_incarnation_id: identity.table_incarnation_id,
        operation_kind: "CORRECTION".to_string(),
        operation_id: request.correction_id.clone(),
    }
}

fn hash_plan_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn data_block_page(
    table_key: &str,
    capture: &StreamAuthorityCapture,
    block: &StrictBlock,
    view: &CanonicalDataBlockView,
    encoded_cursor: Option<&str>,
) -> Result<StreamDataBlockPage> {
    let start = match encoded_cursor {
        None => 0,
        Some(encoded) => {
            let cursor = decode_data_block_cursor(encoded)?;
            if cursor.block_token != block.block_token
                || cursor.correction_view_digest != view.evidence.correction_view_digest
                || cursor.lifecycle_revision != capture.lifecycle.lifecycle_revision
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("stream_block_cursor:{table_key}"),
                    Some("cursor-bound block authority".to_string()),
                    Some("current block authority".to_string()),
                ));
            }
            usize::try_from(cursor.next_ordinal).map_err(|_| {
                OmniError::manifest("stream DataBlock cursor ordinal exceeds platform bounds")
            })?
        }
    };
    if start > view.entries.len() {
        return Err(OmniError::manifest(
            "stream DataBlock cursor lies beyond the canonical correction view",
        ));
    }
    // Reserve the complete non-entry envelope before selecting entries. A
    // cursor with the largest ordinal is conservatively at least as large as
    // the cursor this page can actually return, so the entry-array budget
    // cannot make the complete serialized page cross the hard cap.
    let maximum_next_cursor = encode_data_block_cursor(&StreamDataBlockCursor {
        block_token: block.block_token.clone(),
        correction_view_digest: view.evidence.correction_view_digest.clone(),
        lifecycle_revision: capture.lifecycle.lifecycle_revision,
        next_ordinal: u64::MAX,
    })?;
    let empty_page = StreamDataBlockPage {
        block_token: block.block_token.clone(),
        stable_table_id: capture.entry.identity.stable_table_id,
        table_incarnation_id: capture.entry.identity.table_incarnation_id,
        table_key: table_key.to_string(),
        lifecycle_revision: capture.lifecycle.lifecycle_revision,
        correction_view_digest: view.evidence.correction_view_digest.clone(),
        entries: Vec::new(),
        next_cursor: Some(maximum_next_cursor),
    };
    let entry_array_budget =
        data_block_page_entry_array_budget(&empty_page, STREAM_DATA_BLOCK_PAGE_SERIALIZED_BYTES)?;
    let (entries, end) = select_data_block_page_entries(
        &view.entries,
        start,
        STREAM_DATA_BLOCK_PAGE_ENTRIES,
        entry_array_budget,
    )?;
    let next_cursor = (end < view.entries.len())
        .then(|| {
            encode_data_block_cursor(&StreamDataBlockCursor {
                block_token: block.block_token.clone(),
                correction_view_digest: view.evidence.correction_view_digest.clone(),
                lifecycle_revision: capture.lifecycle.lifecycle_revision,
                next_ordinal: u64::try_from(end).unwrap_or(u64::MAX),
            })
        })
        .transpose()?;
    let page = StreamDataBlockPage {
        block_token: block.block_token.clone(),
        stable_table_id: capture.entry.identity.stable_table_id,
        table_incarnation_id: capture.entry.identity.table_incarnation_id,
        table_key: table_key.to_string(),
        lifecycle_revision: capture.lifecycle.lifecycle_revision,
        correction_view_digest: view.evidence.correction_view_digest.clone(),
        entries,
        next_cursor,
    };
    if serialized_data_block_page_bytes(&page, STREAM_DATA_BLOCK_PAGE_SERIALIZED_BYTES)?.is_none() {
        return Err(OmniError::manifest_internal(format!(
            "complete stream DataBlock page exceeds the {}-byte serialized page invariant",
            STREAM_DATA_BLOCK_PAGE_SERIALIZED_BYTES
        )));
    }
    Ok(page)
}

fn select_data_block_page_entries(
    source: &[CanonicalDataBlockViewEntry],
    start: usize,
    max_entries: usize,
    max_serialized_bytes: usize,
) -> Result<(Vec<StreamDataBlockEntry>, usize)> {
    if max_entries == 0 || max_serialized_bytes < 2 {
        return Err(OmniError::manifest_internal(
            "stream DataBlock page limits cannot produce a JSON entry array",
        ));
    }

    // Account for the JSON array brackets up front. Each entry is sized with
    // serde's actual compact encoding; no second potentially-256-MiB buffer is
    // allocated merely to measure the page.
    let mut serialized_bytes = 2usize;
    let mut entries = Vec::with_capacity(max_entries.min(source.len().saturating_sub(start)));
    for entry in source.iter().skip(start).take(max_entries) {
        let public_entry = StreamDataBlockEntry {
            ordinal: entry.ordinal,
            table_key: entry.table_key.clone(),
            logical_key: entry.logical_key.clone(),
            current_blocked_winner_stream_token: entry.current_blocked_winner_stream_token.clone(),
            violation_code: entry.violation_code.clone(),
            field_path_or_group: entry.field_path_or_group.clone(),
            violation_instance_id: entry.violation_instance_id.clone(),
            allowed_actions: entry.allowed_actions.clone(),
        };
        let separator_bytes = if entries.is_empty() { 0 } else { 1 };
        let available = max_serialized_bytes
            .checked_sub(serialized_bytes)
            .and_then(|remaining| remaining.checked_sub(separator_bytes));
        let Some(available) = available else {
            break;
        };
        let Some(entry_bytes) = serialized_data_block_entry_bytes(&public_entry, available)? else {
            if entries.is_empty() {
                return Err(OmniError::manifest_internal(format!(
                    "stream DataBlock entry ordinal {} exceeds the {max_serialized_bytes}-byte serialized page invariant",
                    entry.ordinal
                )));
            }
            break;
        };
        serialized_bytes = serialized_bytes
            .checked_add(separator_bytes)
            .and_then(|bytes| bytes.checked_add(entry_bytes))
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream DataBlock page serialized-byte accounting overflow",
                )
            })?;
        entries.push(public_entry);
    }
    let end = start.checked_add(entries.len()).ok_or_else(|| {
        OmniError::manifest_internal("stream DataBlock page ordinal accounting overflow")
    })?;
    Ok((entries, end))
}

fn serialized_data_block_entry_bytes(
    entry: &StreamDataBlockEntry,
    limit: usize,
) -> Result<Option<usize>> {
    let mut writer = BoundedDataBlockJsonCounter::new(limit);
    match serde_json::to_writer(&mut writer, entry) {
        Ok(()) => Ok(Some(writer.written)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(OmniError::manifest_internal(format!(
            "failed to size stream DataBlock entry JSON: {error}"
        ))),
    }
}

fn serialized_data_block_page_bytes(
    page: &StreamDataBlockPage,
    limit: usize,
) -> Result<Option<usize>> {
    let mut writer = BoundedDataBlockJsonCounter::new(limit);
    match serde_json::to_writer(&mut writer, page) {
        Ok(()) => Ok(Some(writer.written)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(OmniError::manifest_internal(format!(
            "failed to size complete stream DataBlock page JSON: {error}"
        ))),
    }
}

fn data_block_page_entry_array_budget(
    empty_page_with_maximum_cursor: &StreamDataBlockPage,
    complete_page_limit: usize,
) -> Result<usize> {
    let empty_page_bytes =
        serialized_data_block_page_bytes(empty_page_with_maximum_cursor, usize::MAX)?.ok_or_else(
            || OmniError::manifest_internal("stream DataBlock page envelope overflow"),
        )?;
    // serde's compact encoding represents the known-empty entries vector as
    // exactly `[]`; retain every other serialized byte as the fixed envelope.
    let page_envelope_bytes = empty_page_bytes.checked_sub(2).ok_or_else(|| {
        OmniError::manifest_internal("stream DataBlock page envelope omitted its entry array")
    })?;
    complete_page_limit
        .checked_sub(page_envelope_bytes)
        .filter(|budget| *budget >= 2)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream DataBlock page envelope exceeds the {complete_page_limit}-byte serialized page invariant"
            ))
        })
}

struct BoundedDataBlockJsonCounter {
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl BoundedDataBlockJsonCounter {
    fn new(limit: usize) -> Self {
        Self {
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedDataBlockJsonCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.written.checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "stream DataBlock JSON byte count overflowed usize",
            ));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "stream DataBlock JSON exceeds its remaining page budget",
            ));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_data_block_cursor(cursor: &StreamDataBlockCursor) -> Result<String> {
    let bytes = serde_json::to_vec(cursor).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode stream DataBlock cursor: {error}"))
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_data_block_cursor(encoded: &str) -> Result<StreamDataBlockCursor> {
    if encoded.len() > STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES {
        return Err(OmniError::resource_limit(
            "stream DataBlock cursor encoded bytes",
            STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES as u64,
            u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| OmniError::manifest("invalid stream DataBlock cursor encoding"))?;
    if bytes.len() > STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES {
        return Err(OmniError::resource_limit(
            "stream DataBlock cursor bytes",
            STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES as u64,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| OmniError::manifest("invalid stream DataBlock cursor payload"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_entry(ordinal: u64, logical_key: String) -> CanonicalDataBlockViewEntry {
        CanonicalDataBlockViewEntry {
            ordinal,
            table_key: "node:Person".to_string(),
            logical_key,
            current_blocked_winner_stream_token: format!("sha256:{:064x}", ordinal + 1),
            violation_code: "UNIQUE_VIOLATION".to_string(),
            field_path_or_group: vec!["email".to_string()],
            violation_instance_id: format!("sha256:{:064x}", ordinal + 2),
            allowed_actions: vec!["REPLACE".to_string(), "WITHDRAW".to_string()],
        }
    }

    fn public_entry(entry: &CanonicalDataBlockViewEntry) -> StreamDataBlockEntry {
        StreamDataBlockEntry {
            ordinal: entry.ordinal,
            table_key: entry.table_key.clone(),
            logical_key: entry.logical_key.clone(),
            current_blocked_winner_stream_token: entry.current_blocked_winner_stream_token.clone(),
            violation_code: entry.violation_code.clone(),
            field_path_or_group: entry.field_path_or_group.clone(),
            violation_instance_id: entry.violation_instance_id.clone(),
            allowed_actions: entry.allowed_actions.clone(),
        }
    }

    fn correction_request() -> StreamDataCorrectionRequest {
        StreamDataCorrectionRequest {
            protocol_version: STREAM_DATA_CORRECTION_PROTOCOL_VERSION,
            block_token: "block-1".to_string(),
            correction_id: "00000000-0000-0000-0000-000000000001".to_string(),
            expected_lifecycle_revision: 1,
            actions: vec![StreamDataCorrectionAction::Withdraw {
                ordinal: 0,
                logical_key: "person-1".to_string(),
                current_blocked_winner_stream_token: "token-1".to_string(),
            }],
            expected_plan_digest: None,
        }
    }

    #[test]
    fn correction_request_serialized_byte_cap_is_inclusive_and_typed() {
        let request = correction_request();
        let serialized_bytes = serde_json::to_vec(&request).unwrap().len() as u64;
        validate_serialized_correction_request_bytes(&request, serialized_bytes).unwrap();

        let error = validate_serialized_correction_request_bytes(&request, serialized_bytes - 1)
            .expect_err("one byte above the request ceiling must be refused");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } if resource == "stream_correction_request_bytes"
                && limit == serialized_bytes - 1
                && actual > limit
        ));
    }

    #[test]
    fn correction_preprocessing_charge_is_aggregate_and_effect_free_on_refusal() {
        let mut charged = 7;
        charge_stream_correction_preprocessing_bytes(&mut charged, 5, 12).unwrap();
        assert_eq!(charged, 12);

        let error = charge_stream_correction_preprocessing_bytes(&mut charged, 1, 12)
            .expect_err("the cumulative replacement charge must not exceed its envelope");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                resource,
                limit: 12,
                actual: 13,
            } if resource == "stream_correction_preprocessing_bytes"
        ));
        assert_eq!(charged, 12, "a refused charge must not mutate accounting");
    }

    #[test]
    fn data_block_cursor_bounds_encoded_input_before_base64_allocation() {
        let encoded = "A".repeat(STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES + 1);
        let error = decode_data_block_cursor(&encoded)
            .expect_err("oversized encoded cursors must fail before decoding");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } if resource == "stream DataBlock cursor encoded bytes"
                && limit == STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES as u64
                && actual == limit + 1
        ));
    }

    #[test]
    fn data_block_cursor_retains_decoded_payload_cap() {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(vec![
            0_u8;
            STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES
                + 1
        ]);
        assert!(encoded.len() <= STREAM_DATA_BLOCK_CURSOR_MAX_ENCODED_BYTES);
        let error = decode_data_block_cursor(&encoded)
            .expect_err("decoded cursor payloads remain independently bounded");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } if resource == "stream DataBlock cursor bytes"
                && limit == STREAM_DATA_BLOCK_CURSOR_MAX_DECODED_BYTES as u64
                && actual == limit + 1
        ));
    }

    #[test]
    fn data_block_page_selector_honors_entry_and_serialized_byte_caps() {
        let source = (0..300)
            .map(|ordinal| canonical_entry(ordinal, format!("key-{ordinal:03}")))
            .collect::<Vec<_>>();
        let (count_page, count_end) =
            select_data_block_page_entries(&source, 0, 256, usize::MAX).unwrap();
        assert_eq!(count_page.len(), 256);
        assert_eq!(count_end, 256);

        let two_entry_cap =
            serde_json::to_vec(&vec![public_entry(&source[0]), public_entry(&source[1])])
                .unwrap()
                .len()
                - 1;
        let mut next = 0usize;
        let mut ordinals = Vec::new();
        while next < source.len() {
            let (page, end) =
                select_data_block_page_entries(&source, next, 256, two_entry_cap).unwrap();
            assert_eq!(page.len(), 1);
            assert!(serde_json::to_vec(&page).unwrap().len() <= two_entry_cap);
            assert!(end > next, "a byte-limited page must advance its cursor");
            ordinals.extend(page.into_iter().map(|entry| entry.ordinal));
            next = end;
        }
        assert_eq!(ordinals, (0..300).collect::<Vec<_>>());
    }

    #[test]
    fn data_block_page_selector_refuses_an_oversized_single_entry() {
        let source = vec![canonical_entry(0, "large-key".repeat(32))];
        let serialized = serde_json::to_vec(&vec![public_entry(&source[0])])
            .unwrap()
            .len();
        let error = select_data_block_page_entries(&source, 0, 256, serialized - 1)
            .expect_err("an entry above the hard cap is an invariant breach, not an empty page");
        assert!(
            error.to_string().contains("entry ordinal 0 exceeds the"),
            "{error:?}"
        );
    }

    #[test]
    fn data_block_page_byte_cap_includes_envelope_and_cursor() {
        let source = vec![canonical_entry(0, "key-000".to_string())];
        let maximum_cursor = encode_data_block_cursor(&StreamDataBlockCursor {
            block_token: "block-1".to_string(),
            correction_view_digest: format!("sha256:{:064x}", 1),
            lifecycle_revision: 9,
            next_ordinal: u64::MAX,
        })
        .unwrap();
        let empty_page = StreamDataBlockPage {
            block_token: "block-1".to_string(),
            stable_table_id: 1,
            table_incarnation_id: 2,
            table_key: "node:Person".to_string(),
            lifecycle_revision: 9,
            correction_view_digest: format!("sha256:{:064x}", 1),
            entries: Vec::new(),
            next_cursor: Some(maximum_cursor.clone()),
        };
        let empty_page_bytes = serde_json::to_vec(&empty_page).unwrap().len();
        let one_entry_array_bytes = serde_json::to_vec(&vec![public_entry(&source[0])])
            .unwrap()
            .len();
        let complete_page_limit = empty_page_bytes - 2 + one_entry_array_bytes;
        let entry_array_budget =
            data_block_page_entry_array_budget(&empty_page, complete_page_limit).unwrap();
        assert_eq!(entry_array_budget, one_entry_array_bytes);

        let (entries, end) =
            select_data_block_page_entries(&source, 0, 256, entry_array_budget).unwrap();
        assert_eq!(end, 1);
        let page = StreamDataBlockPage {
            entries,
            next_cursor: Some(maximum_cursor),
            ..empty_page
        };
        let complete_page_bytes = serde_json::to_vec(&page).unwrap().len();
        assert_eq!(complete_page_bytes, complete_page_limit);
        assert!(complete_page_bytes > one_entry_array_bytes);
        assert_eq!(
            serialized_data_block_page_bytes(&page, complete_page_limit).unwrap(),
            Some(complete_page_limit)
        );
    }
}
