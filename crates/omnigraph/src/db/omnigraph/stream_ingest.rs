#![allow(dead_code)]

//! RFC-026 Phase-B1 private stream admission and one-generation fold.
//!
//! This is deliberately an engine-internal orchestration layer.  Lance owns
//! the WAL, its epoch fence, replay, and fresh-generation scan.  OmniGraph owns
//! the graph authority checks around those primitives and the one
//! `__manifest` publication which makes a folded generation visible.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::take::take;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::LsmScanner;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardManifestStore, ShardWriter, WalTailer};
use lance_index::mem_wal::{MemWalIndexDetails, MergedGeneration, ShardId, ShardStatus};

use crate::db::manifest::stream::{
    CLAIM_RECEIPT_TAG, ClaimProfile, DrainGoal, LastFoldOutcome, LastFoldSummary,
    ManagementReceipt, RetainedShardInventoryCommitment, STREAM_RESUME_OPERATION_KIND,
    StreamGenerationCut, StreamResumeMode, StreamResumeRequestPayload,
    stream_graph_identity_digest, stream_quiesce_result_payload, stream_resume_result_payload,
};
use crate::db::manifest::stream_token::{
    AdmissionClassification, AdmissionRequest, PayloadDigest, PayloadDigestInput,
    StreamFoldAttributionSummary, StreamRowOrigin, StreamTerminalCorrection, StreamToken,
    StreamTokenAuthorityRow, StreamTokenDisposition, StreamWriteEnvelope, TrustedContributorId,
    TrustedStreamRowMetadata, build_trusted_stream_metadata_array, classify_admission,
    decode_trusted_stream_metadata, stream_fold_attribution_commitment,
    validate_authority_base_pair,
};
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, add_stream_lookup_retained_bytes, lookup_lifecycle_ledger_record_by_id,
    lookup_management_receipt, open_stream_token_authority_head, stage_lifecycle_ledger_records,
    stage_management_receipt, stage_stream_token_upsert, stream_token_authority_entry_for_dataset,
    stream_token_rows_for_keys, validate_stream_token_plan_bounds,
};
use crate::db::manifest::{
    CurrentHeadWitness, ExpectedTableVersions, ManifestChange, RecoveryAuthorityToken,
    RecoveryLineageIntent, RecoveryProtocolV14, RecoveryProtocolV15,
    RecoveryStreamClaimContinuationV14, RecoveryStreamClaimOutcomeV14, RecoveryStreamFoldCut,
    RecoveryStreamLifecycleReceiptKind, RecoveryStreamOpenPlanV15, RecoveryStreamResumeOutcomeV15,
    RecoveryStreamResumeRequestV15, SidecarTablePin, StreamLifecycle, StreamLifecycleEntry,
    StreamPhysicalBinding, TableIdentity, TableVersionExpectation,
    arm_stream_claim_checkpoint_sidecar_v14, arm_stream_claim_terminal_sidecar_v14,
    arm_stream_resume_checkpoint_sidecar_v15, arm_stream_resume_terminal_sidecar_v15,
    classify_effect_free_stream_claim_sidecar_v14, classify_effect_free_stream_resume_sidecar_v15,
    complete_stream_claim_sidecar_v14, complete_stream_fold_sidecar_v14,
    complete_stream_lifecycle_receipt_sidecar_v14, complete_stream_resume_sidecar_v15,
    confirm_stream_claim_sidecar_v14, confirm_stream_fold_sidecar_v14,
    confirm_stream_lifecycle_receipt_sidecar_v14, confirm_stream_resume_sidecar_v15,
    finalize_effect_free_stream_fold_sidecar_v14, list_sidecars,
    lookup_stream_claim_continuation_v14, new_stream_claim_sidecar_v14,
    new_stream_drain_fold_sidecar_v14, new_stream_fold_v2_sidecar_v14,
    new_stream_lifecycle_receipt_sidecar_v14, new_stream_resume_sidecar_v15,
    prepared_stream_claim_attempt_v14, prepared_stream_resume_attempt_v15,
    rearm_stream_claim_checkpoint_sidecar_v14, rearm_stream_resume_checkpoint_sidecar_v15,
    receipt_first_rearm_stream_claim_sidecar_v14, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS, B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
    B2PreprocessingPermit, CallerOrdinalRange, CheckedExclusiveStreamAuthority,
    CheckedStreamAuthority, ClaimedMemWalWorker, ConfirmedStreamTokenOverlay,
    ConfirmedStreamTokenOverlayRow, CurrentGenerationProjectionSource, DurableBatchAck,
    IdleAuthorityCheck, IdleAuthorityFailure, MemWalWorkerError, OpenedMemWalWorker,
    PassiveB1PhysicalState, PassiveQuiesceDisposition, PreparedPut, PreparedPutFailure,
    QueuedBatchPermit, QuiesceCut, SealedGenerationCut, StreamWorkerKey, WorkerOpenFailure,
    b1_input_accounting, b1_logical_batch_bytes, capture_current_head_witness,
    reconstruct_b1_writer_config, scan_flushed_generation_projection,
    validate_b1_lifecycle_current_binding_physical_state,
    validate_b1_lifecycle_physical_state_with_binding_inventory, validate_stream_config_v3_binding,
};
use crate::validate::{ChangeSet, CommittedState, TableChange};

use super::stream_lifecycle::{
    CanonicalDataBlockEvidence, ClaimAttemptEvidence, ClaimAttemptRequest, ClaimOperationRequest,
    DataBlockEvidenceCollector, EmptyCutEvidence, QuiesceRequest, StreamResumeRequest,
    authenticate_claim_wal_segment, build_claim_adoption_row, build_claim_attempt_effect,
    build_draining_data_block, build_draining_to_sealed, build_open_to_draining,
    build_resume_adoption_row, build_terminal_claim, claim_wal_authentication_plan,
    claim_wal_key_discovery_plan, collect_claim_wal_segment_keys,
    current_generation_lww_projection_digest, lifecycle_generation_lww_projection_digest,
    prepare_claim_attempt, prepare_claim_operation, prepare_resume_claim_operation,
    prepare_stream_resume_open, stream_quiesce_request_digest,
    stream_quiesce_request_payload_from_draining, validate_selected_management_receipt_progress,
};
use super::{Omnigraph, WriteTxn};

const B1_MAX_FOLD_ATTEMPTS: usize = 2;
/// One B2 classification/projection call is deliberately small because exact
/// token-prefix selection may need to validate every shorter prefix when
/// existing winner replacement makes the fit predicate non-monotonic.
pub(super) const STREAM_B2_CLASSIFICATION_WINDOW_ROWS: usize = 256;
/// The raw body can contain far more DOM nodes than Arrow values (for example,
/// `[0,0,...]`). Reserve 64 MiB of the 128-MiB B2 envelope for parsed structure
/// and conservatively charge 512 bytes per structural slot before serde can
/// allocate that DOM. Raw/string bytes occupy at most the separate 32-MiB
/// input bound; normalized Arrow occupies the remaining 32 MiB.
const STREAM_JSON_DOM_STRUCTURE_BYTES: u64 = 64 * 1024 * 1024;
const STREAM_JSON_BYTES_PER_STRUCTURAL_SLOT: u64 = 512;
const STREAM_JSON_MAX_STRUCTURAL_SLOTS: u64 =
    STREAM_JSON_DOM_STRUCTURE_BYTES / STREAM_JSON_BYTES_PER_STRUCTURAL_SLOT;

/// Private B2 result for one caller occurrence. Public response shaping stays
/// deliberately inactive; this value exists so crash/race tests can prove the
/// sequencing contract without exposing a product API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamTokenAdmissionAck {
    pub(crate) stream_token: StreamToken,
    pub(crate) origin: StreamRowOrigin,
    pub(crate) disposition: StreamTokenDisposition,
    pub(crate) terminal_correction: Option<StreamTerminalCorrection>,
    pub(crate) already_durable: bool,
}

/// Freshly revalidated physical authority associated with a B2 disposition.
///
/// This is current binding evidence, not immutable token provenance. In
/// particular, an `already_durable` token may have originated under an older
/// enrollment while this proof names the writer currently serving the lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamB2BindingProof {
    pub(super) enrollment_id: String,
    pub(super) shard_id: String,
    pub(super) writer_epoch: u64,
}

/// One strictly normalized caller row plus its validated compare-and-chain
/// envelope. The hidden request driver uses this type so framing does not
/// duplicate B2's JSON grammar or Arrow normalization boundary.
pub(super) struct NormalizedStreamJsonRow {
    pub(super) envelope: StreamWriteEnvelope,
    pub(super) batch: RecordBatch,
    pub(super) logical_id: String,
}

/// Effect-free disposition for the first row at a physical-run boundary.
///
/// A disposition after one or more `New` rows is deliberately left
/// unconsumed: the new prefix is submitted first and the caller classifies the
/// boundary again under the next queue position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamB2BoundaryDisposition {
    AlreadyDurable(StreamTokenAdmissionAck),
    BindingChanged {
        stable_table_id: u64,
        table_incarnation_id: u64,
        current_stream_incarnation_id: String,
    },
    SequenceConflict {
        stable_table_id: u64,
        table_incarnation_id: u64,
        logical_id: String,
        current_token: Option<String>,
    },
    IdempotencyConflict {
        stable_table_id: u64,
        table_incarnation_id: u64,
        logical_id: String,
        current_token: String,
    },
}

/// Per-occurrence correlation retained when one multi-row physical append is
/// ambiguous. Candidate tokens are explicitly unconfirmed and cannot be used
/// as predecessor authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamB2AmbiguousRow {
    pub(super) caller_ordinal: u64,
    pub(super) admission_attempt_id: String,
    pub(super) logical_write_id: String,
    pub(super) unconfirmed_candidate_token: StreamToken,
}

/// Rich private ambiguity result for one physical append. `OmniError` retains
/// its historical singleton fields; hidden multi-row transport maps this
/// structure into one ordered result per caller occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamB2AckUnknown {
    pub(super) stable_table_id: u64,
    pub(super) table_incarnation_id: u64,
    pub(super) binding: StreamB2BindingProof,
    pub(super) caller_ordinals: CallerOrdinalRange,
    pub(super) rows: Vec<StreamB2AmbiguousRow>,
    pub(super) reason: String,
}

/// Result of classifying and, when possible, admitting one distinct-key
/// physical prefix.
#[derive(Debug)]
pub(super) enum StreamB2PrefixOutcome {
    Admitted {
        caller_ordinals: CallerOrdinalRange,
        binding: StreamB2BindingProof,
        acknowledgements: Vec<StreamTokenAdmissionAck>,
    },
    Boundary {
        caller_ordinal: u64,
        binding: StreamB2BindingProof,
        disposition: StreamB2BoundaryDisposition,
    },
    AckUnknown(StreamB2AckUnknown),
    Refused {
        caller_ordinal: u64,
        binding: StreamB2BindingProof,
        error: OmniError,
    },
}

/// One coherent main-branch stream authority capture.
///
/// `head` is opened at the exact physical HEAD proven equal to the lifecycle
/// witness.  It is safe to use for a later staged effect only while the caller
/// retains the admission lease and performs the final gated revalidation.
#[derive(Clone)]
pub(super) struct StreamAuthorityCapture {
    pub(super) txn: WriteTxn,
    pub(super) entry: crate::db::manifest::SubTableEntry,
    pub(super) lifecycle: StreamLifecycleEntry,
    pub(super) binding: StreamPhysicalBinding,
    pub(super) worker_key: StreamWorkerKey,
    pub(super) admission_key: StreamAdmissionKey,
    pub(super) shard_id: ShardId,
    pub(super) enrollment_id: ShardId,
    pub(super) retained_shard_inventory: Option<RetainedShardInventoryCommitment>,
    pub(super) epoch_floor: u64,
    pub(super) full_path: String,
    pub(super) head: SnapshotHandle,
    pub(super) details: MemWalIndexDetails,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttributedFoldPlan {
    token_rows: Vec<StreamTokenAuthorityRow>,
    summary: StreamFoldAttributionSummary,
}

struct StreamJsonRowWire {
    envelope: StreamWriteEnvelope,
    body: BTreeMap<String, serde_json::Value>,
}

impl<'de> serde::Deserialize<'de> for StreamJsonRowWire {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StreamJsonRowWire;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one stream JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<StreamJsonRowWire, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut envelope = None;
                let mut body = BTreeMap::new();
                while let Some(field) = map.next_key::<String>()? {
                    if field == "$stream" {
                        if envelope.is_some() {
                            return Err(serde::de::Error::duplicate_field("$stream"));
                        }
                        envelope = Some(map.next_value::<StreamWriteEnvelope>()?);
                    } else {
                        if body.contains_key(&field) {
                            return Err(serde::de::Error::custom(format!(
                                "duplicate stream input field '{field}'"
                            )));
                        }
                        body.insert(field, map.next_value::<serde_json::Value>()?);
                    }
                }
                let envelope =
                    envelope.ok_or_else(|| serde::de::Error::missing_field("$stream"))?;
                Ok(StreamJsonRowWire { envelope, body })
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn validate_stream_json_structure_bound(raw_json: &[u8]) -> Result<()> {
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
            if slots > STREAM_JSON_MAX_STRUCTURAL_SLOTS {
                return Err(OmniError::resource_limit(
                    "stream_json_structural_slots",
                    STREAM_JSON_MAX_STRUCTURAL_SLOTS,
                    slots,
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn normalize_stream_json_row_with_catalog(
    table_key: &str,
    raw_json: &[u8],
    catalog: &omnigraph_compiler::catalog::Catalog,
) -> Result<NormalizedStreamJsonRow> {
    let raw_bytes = u64::try_from(raw_json.len()).unwrap_or(u64::MAX);
    if raw_bytes > B1_MAX_GENERATION_ARROW_BYTES {
        return Err(OmniError::resource_limit(
            "stream raw JSON bytes",
            B1_MAX_GENERATION_ARROW_BYTES,
            raw_bytes,
        ));
    }
    validate_stream_json_structure_bound(raw_json)?;
    let wire = serde_json::from_slice::<StreamJsonRowWire>(raw_json)
        .map_err(|error| OmniError::manifest(format!("invalid stream JSON: {error}")))?;
    wire.envelope
        .validate()
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let row = serde_json::Value::Object(wire.body.into_iter().collect());
    let batch = crate::loader::normalize_stream_json_row(catalog, table_key, row)?;
    validate_stream_input_bounds(table_key, &batch)?;
    validate_stream_value_constraints(table_key, &batch, catalog)?;
    let ids = batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest_internal("normalized stream JSON row has no exact Utf8 id column")
        })?;
    if batch.num_rows() != 1 || ids.is_null(0) {
        return Err(OmniError::manifest_internal(
            "stream JSON normalization did not produce one non-null logical id",
        ));
    }
    Ok(NormalizedStreamJsonRow {
        envelope: wire.envelope,
        logical_id: ids.value(0).to_string(),
        batch,
    })
}

#[derive(Debug, Clone)]
enum FoldLifecycleMode {
    Open,
    Draining { drain_id: String },
}

impl Omnigraph {
    /// Admit one already-normalized, non-empty physical batch through the
    /// feature-gated B1 substrate seam. Synthetic trusted envelopes make these
    /// older worker/capacity tests exercise the active lifecycle-v3 fold and recovery-v14
    /// path without exposing an unattributed writer in production.
    ///
    /// The `Arc` receiver is intentional.  The worker owns a detached task so
    /// dropping the requesting future cannot abandon an invoked put, its
    /// watcher, or quiesced retirement.
    #[cfg(feature = "failpoints")]
    pub(crate) async fn stream_put_phase_b1(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
    ) -> Result<DurableBatchAck> {
        // Profile authority is outermost. Keeping this shared lease through
        // admission and detached worker completion prevents a concurrent
        // disable from waiting on our admission lease while a cold claim waits
        // behind the disable's profile-exclusive lease.
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        // Refuse an oversized caller buffer synchronously before recovery IO,
        // authority capture, detached ownership, or a cold epoch claim.
        validate_stream_input_bounds(table_key, &batch)?;
        let provisional = self
            .capture_stream_authority(table_key, "stream put")
            .await?;
        Self::ensure_stream_table_admission_supported(&provisional.txn.catalog, table_key)?;
        let batch = self
            .storage()
            .prepare_keyed_write_batch(table_key, batch)
            .await?;
        validate_stream_input_bounds(table_key, &batch)?;
        self.validate_stream_logical_admission_batch(&provisional, &batch)?;

        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal("validated B1 test batch has no exact Utf8 id column")
            })?;
        let mut logical_ids = std::collections::BTreeSet::new();
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                return Err(OmniError::manifest("stream row id must be non-null"));
            }
            logical_ids.insert(ids.value(row).to_string());
        }

        let key = provisional.worker_key;
        let admission_key = provisional.admission_key.clone();
        let authority_db = Arc::clone(self);
        let authority_key = admission_key.clone();
        // Inflight and Arrow budgets are acquired before any same-key queue or
        // shared-admission wait. The returned token is already inside that
        // bounded corridor and moves into the detached prepare closure.
        let (mut queued, put_authority) = self
            .stream_workers
            .reserve_put_input(key, table_key, &batch, move || async move {
                let shared = authority_db
                    .write_queue()
                    .acquire_stream_shared(&authority_key)
                    .await;
                CheckedStreamAuthority::from_shared_admission(shared)
            })
            .await?;

        // The provisional capture exists only to choose the immutable
        // admission domain. A fold can publish while this caller is waiting for
        // the shared lease, so every read and every effect-free result below is
        // based on a fresh capture made after that lease and the same-key queue
        // are both owned.
        let prepared = self
            .capture_stream_authority(table_key, "stream put final admission")
            .await?;
        self.ensure_no_relevant_stream_sidecar_except_exact_claim(&prepared, "stream put")
            .await?;
        ensure_same_binding(key, &prepared, "stream put final admission authority")?;
        self.validate_stream_logical_admission_batch(&prepared, &batch)?;

        // The table-wide input queue makes the warm overlay stable while the
        // synthetic batch is chained. Durable authority is read once with one
        // structured, generation-bounded exact-id predicate.
        let mut generation_current = BTreeMap::new();
        for logical_id in &logical_ids {
            if let Some(current) = self
                .stream_workers
                .confirmed_token_for_key(&queued, table_key, logical_id)
                .await?
            {
                generation_current.insert(logical_id.clone(), current);
            }
        }
        let token_dataset = prepared.txn.base.open_stream_token_authority().await?;
        let durable_current = stream_token_rows_for_keys(
            &token_dataset,
            prepared.txn.base.stream_token_authority(),
            prepared.entry.identity,
            &logical_ids,
        )
        .await?;
        let durable_base_metadata = lookup_base_stream_metadata_for_keys(
            prepared.head.dataset(),
            prepared.entry.identity,
            &logical_ids,
        )
        .await?;
        for logical_id in &logical_ids {
            if generation_current.contains_key(logical_id) {
                continue;
            }
            validate_authority_base_pair(
                prepared.entry.identity,
                logical_id,
                durable_current.get(logical_id),
                durable_base_metadata.get(logical_id),
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        }
        let contributor = TrustedContributorId::new("omnigraph:test-b1")
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let stream_incarnation_id = prepared
            .lifecycle
            .enrollment_receipt
            .stream_incarnation_id
            .clone();
        let mut metadata_rows = Vec::with_capacity(batch.num_rows());
        let mut confirmed_token_updates = ConfirmedStreamTokenOverlay::new();
        for row in 0..batch.num_rows() {
            let logical_id = ids.value(row).to_string();
            let (predecessor_token, fold_base_token, chain_depth) =
                if let Some(current) = generation_current.get(&logical_id) {
                    (
                        Some(current.authority.current_token),
                        current.metadata.fold_base_token,
                        current.metadata.chain_depth.checked_add(1).ok_or_else(|| {
                            OmniError::resource_limit(
                                format!("stream chain depth for {table_key}/{logical_id}"),
                                u32::MAX as u64,
                                u32::MAX as u64 + 1,
                            )
                        })?,
                    )
                } else if let Some(current) = durable_current.get(&logical_id) {
                    (Some(current.current_token), Some(current.current_token), 1)
                } else {
                    (None, None, 1)
                };
            let canonical_payload = super::canonical_stream_payload_v1(&batch, row)?;
            let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
                identity: prepared.entry.identity,
                accepted_schema_hash: &prepared.txn.authority.schema_ir_hash,
                canonical_payload: &canonical_payload,
            })
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let request = AdmissionRequest {
                identity: prepared.entry.identity,
                logical_id: logical_id.clone(),
                envelope: StreamWriteEnvelope {
                    stream_incarnation_id: stream_incarnation_id.clone(),
                    write_id: ShardId::new_v4().to_string(),
                    predecessor_token,
                },
                contributor_id: contributor.clone(),
                payload_digest,
            };
            let candidate = request
                .candidate_token()
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let caller_ordinal =
                caller_ordinals
                    .start
                    .checked_add(u64::try_from(row).map_err(|_| {
                        OmniError::manifest_internal("B1 test row ordinal exceeds u64")
                    })?)
                    .ok_or_else(|| OmniError::manifest_internal("B1 test ordinal overflow"))?;
            let metadata = TrustedStreamRowMetadata::new_admission(
                &request,
                candidate,
                fold_base_token,
                chain_depth,
                ShardId::new_v4().to_string(),
                caller_ordinal,
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let authority = StreamTokenAuthorityRow::from_present_metadata(
                request.identity,
                logical_id.clone(),
                prepared.binding.enrollment_id.clone(),
                &metadata,
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            let overlay = ConfirmedStreamTokenOverlayRow {
                authority,
                metadata: metadata.clone(),
            };
            generation_current.insert(logical_id.clone(), overlay.clone());
            confirmed_token_updates.insert(logical_id, overlay);
            metadata_rows.push(Some(metadata));
        }
        let projected_token_rows = self
            .stream_workers
            .projected_token_authority_rows(&queued, table_key, &confirmed_token_updates)
            .await?;
        validate_generation_token_plan(table_key, &projected_token_rows)?;
        let batch = append_trusted_stream_metadata(batch, metadata_rows)?;
        self.validate_stream_admission_batch(&prepared, &batch)?;
        queued.reprice_for_exact_batch(table_key, &batch)?;
        self.finish_reserved_stream_put(
            table_key.to_string(),
            batch,
            caller_ordinals,
            key,
            admission_key,
            queued,
            put_authority,
            confirmed_token_updates,
        )
        .await
    }

    /// Parse and admit one authenticated caller-shaped JSON row through the
    /// checked serving-runtime boundary.
    ///
    /// This is the first intentionally narrow functional ingress seam: the
    /// table must already be enrolled and `OPEN`. It accepts one bounded JSON
    /// object, validates the exact `$stream` envelope, strictly normalizes the
    /// logical row to dense Arrow, and delegates to the existing B2 core.
    /// Incremental NDJSON, transport admission, lazy enrollment, and public
    /// response shaping remain inactive. Before production transport
    /// activation, the owned profile guard must transfer into the detached put
    /// tail so caller cancellation cannot release it before watcher/fence
    /// settlement.
    pub(crate) async fn stream_ingest_one_as(
        self: &Arc<Self>,
        table_key: &str,
        raw_json: &[u8],
        caller_ordinal: u64,
        actor_id: &str,
    ) -> Result<StreamTokenAdmissionAck> {
        self.enforce(
            omnigraph_policy::PolicyAction::StreamIngest,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;
        let contributor_id = TrustedContributorId::new(actor_id.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;

        // Authenticate the exact serving runtime before spending work on an
        // untrusted body. Recovery may need this gate exclusively, so this
        // preliminary lease ends after effect-free parsing and validation.
        let preflight_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        let preprocessing = self.stream_workers.reserve_b2_preprocessing()?;
        let normalization_txn = self.open_write_txn(None).await?;
        Self::ensure_stream_table_admission_supported(&normalization_txn.catalog, table_key)?;
        let catalog = super::public_catalog_view(&normalization_txn.catalog)?;
        let normalized = normalize_stream_json_row_with_catalog(table_key, raw_json, &catalog)?;
        drop(preflight_guard);

        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;

        // Reacquire and revalidate after recovery: profile authority may have
        // moved while the preliminary lease was absent. Calling
        // `stream_put_phase_b2_one` here would attempt a nested read lock and
        // can deadlock behind a queued profile writer because Tokio's RwLock is
        // fair/write-preferring.
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        self.stream_put_phase_b2_one_under_profile_guard(
            table_key,
            normalized.batch,
            caller_ordinal,
            normalized.envelope,
            contributor_id,
            Some(preprocessing),
            profile_guard,
        )
        .await
    }

    /// Admit one fully normalized logical row through RFC-026's private B2
    /// compare-and-chain boundary. The caller supplies an already-trusted
    /// contributor identity. This low-level protocol seam remains crate-private
    /// for the existing recovery and sequencing tests.
    pub(crate) async fn stream_put_phase_b2_one(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        envelope: StreamWriteEnvelope,
        contributor_id: TrustedContributorId,
    ) -> Result<StreamTokenAdmissionAck> {
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.stream_put_phase_b2_one_under_profile_guard(
            table_key,
            batch,
            caller_ordinal,
            envelope,
            contributor_id,
            None,
            profile_guard,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_put_phase_b2_one_under_profile_guard(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        envelope: StreamWriteEnvelope,
        contributor_id: TrustedContributorId,
        preprocessing: Option<B2PreprocessingPermit>,
        profile_guard: tokio::sync::OwnedRwLockReadGuard<()>,
    ) -> Result<StreamTokenAdmissionAck> {
        let caller_ordinals =
            CallerOrdinalRange::new(caller_ordinal, caller_ordinal).map_err(worker_error)?;
        match self
            .stream_put_phase_b2_distinct_prefix_under_profile_guard(
                table_key,
                batch,
                caller_ordinals,
                vec![envelope],
                contributor_id,
                preprocessing,
                profile_guard,
            )
            .await?
        {
            StreamB2PrefixOutcome::Admitted {
                mut acknowledgements,
                ..
            } if acknowledgements.len() == 1 => Ok(acknowledgements.remove(0)),
            StreamB2PrefixOutcome::Boundary { disposition, .. } => match disposition {
                StreamB2BoundaryDisposition::AlreadyDurable(ack) => Ok(ack),
                StreamB2BoundaryDisposition::BindingChanged {
                    stable_table_id,
                    table_incarnation_id,
                    current_stream_incarnation_id,
                } => Err(OmniError::StreamBindingChanged {
                    stable_table_id,
                    table_incarnation_id,
                    current_stream_incarnation_id,
                }),
                StreamB2BoundaryDisposition::SequenceConflict {
                    stable_table_id,
                    table_incarnation_id,
                    logical_id,
                    current_token,
                } => Err(OmniError::StreamSequenceConflict {
                    stable_table_id,
                    table_incarnation_id,
                    logical_id,
                    current_token,
                }),
                StreamB2BoundaryDisposition::IdempotencyConflict {
                    stable_table_id,
                    table_incarnation_id,
                    logical_id,
                    current_token,
                } => Err(OmniError::StreamIdempotencyConflict {
                    stable_table_id,
                    table_incarnation_id,
                    logical_id,
                    current_token,
                }),
            },
            StreamB2PrefixOutcome::AckUnknown(unknown) if unknown.rows.len() == 1 => {
                let row = &unknown.rows[0];
                Err(OmniError::AckUnknown {
                    stable_table_id: unknown.stable_table_id,
                    table_incarnation_id: unknown.table_incarnation_id,
                    enrollment_id: unknown.binding.enrollment_id,
                    shard_id: unknown.binding.shard_id,
                    writer_epoch: unknown.binding.writer_epoch,
                    caller_ordinal_start: unknown.caller_ordinals.start,
                    caller_ordinal_end: unknown.caller_ordinals.end,
                    admission_attempt_id: Some(row.admission_attempt_id.clone()),
                    logical_write_ids: vec![row.logical_write_id.clone()],
                    unconfirmed_candidate_token: Some(row.unconfirmed_candidate_token.to_string()),
                    reason: unknown.reason,
                })
            }
            StreamB2PrefixOutcome::Refused { error, .. } => Err(error),
            _ => Err(OmniError::manifest_internal(
                "singleton B2 wrapper received a non-singleton prefix result",
            )),
        }
    }

    /// Classify and admit the longest leading all-`New` distinct-key prefix.
    ///
    /// The first row is charged before shared admission and the table input
    /// queue. Durable authority is then batch-read under that stable queue
    /// position. A token disposition at row zero is returned effect-free; one
    /// after a fresh prefix is not consumed. All admitted rows share one Lance
    /// put, watcher, and post-durability fence.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stream_put_phase_b2_distinct_prefix_under_profile_guard(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
        envelopes: Vec<StreamWriteEnvelope>,
        contributor_id: TrustedContributorId,
        preprocessing: Option<B2PreprocessingPermit>,
        profile_guard: tokio::sync::OwnedRwLockReadGuard<()>,
    ) -> Result<StreamB2PrefixOutcome> {
        let ordinal_len = caller_ordinals
            .end
            .checked_sub(caller_ordinals.start)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| OmniError::manifest_internal("stream caller ordinal range overflow"))?;
        if batch.num_rows() == 0
            || u64::try_from(batch.num_rows()).unwrap_or(u64::MAX) != ordinal_len
            || envelopes.len() != batch.num_rows()
        {
            return Err(OmniError::manifest(format!(
                "B2 prefix rows, envelopes, and ordinals must align: rows={}, envelopes={}, ordinals={ordinal_len}",
                batch.num_rows(),
                envelopes.len()
            )));
        }
        if batch.num_rows() > STREAM_B2_CLASSIFICATION_WINDOW_ROWS {
            return Err(OmniError::resource_limit(
                "stream_b2_classification_rows",
                u64::try_from(STREAM_B2_CLASSIFICATION_WINDOW_ROWS).unwrap_or(u64::MAX),
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
            ));
        }
        validate_stream_input_bounds(table_key, &batch)?;
        for envelope in &envelopes {
            envelope
                .validate()
                .map_err(|error| OmniError::manifest(error.to_string()))?;
        }

        let provisional = self
            .capture_stream_authority(table_key, "stream token admission")
            .await?;
        Self::ensure_stream_table_admission_supported(&provisional.txn.catalog, table_key)?;
        let mut preprocessing = match preprocessing {
            Some(preprocessing) => preprocessing,
            None => self.stream_workers.reserve_b2_preprocessing()?,
        };
        let batch = self
            .storage()
            .prepare_keyed_write_batch(table_key, batch)
            .await?;
        validate_stream_input_bounds(table_key, &batch)?;
        self.validate_stream_logical_admission_batch(&provisional, &batch)?;

        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "validated stream admission batch has no exact Utf8 id column",
                )
            })?;
        let mut ordered_ids = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                return Err(OmniError::manifest("stream row id must be non-null"));
            }
            ordered_ids.push(ids.value(row).to_string());
        }
        {
            // The ordered vector is the sole owner for this phase. Duplicate
            // detection borrows those strings instead of retaining a second
            // attacker-sized copy of every logical id.
            let mut distinct_ids = std::collections::BTreeSet::new();
            for logical_id in &ordered_ids {
                if distinct_ids.insert(logical_id.as_str()) {
                    continue;
                }
                return Err(OmniError::manifest(format!(
                    "B2 physical prefix repeats logical id '{logical_id}'"
                )));
            }
        }

        let key = provisional.worker_key;
        let admission_key = provisional.admission_key.clone();
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_B2_AFTER_PROVISIONAL_AUTHORITY,
        )?;
        let authority_db = Arc::clone(self);
        let authority_key = admission_key.clone();
        let first_row = batch.slice(0, 1);
        let (mut queued, put_authority) = self
            .stream_workers
            .reserve_b2_put_input(
                key,
                table_key,
                &first_row,
                &mut preprocessing,
                move || async move {
                    let shared = authority_db
                        .write_queue()
                        .acquire_stream_shared(&authority_key)
                        .await;
                    CheckedStreamAuthority::from_shared_admission_with_profile(
                        shared,
                        profile_guard,
                    )
                },
            )
            .await?;
        drop(first_row);

        let prepared = self
            .capture_stream_authority(table_key, "stream token final admission")
            .await?;
        let classified_binding = StreamB2BindingProof {
            enrollment_id: prepared.enrollment_id.to_string(),
            shard_id: prepared.shard_id.to_string(),
            writer_epoch: prepared.epoch_floor,
        };
        let mut worker_put_attempted = false;
        let outcome: Result<StreamB2PrefixOutcome> = async {
            self.ensure_no_relevant_stream_sidecar_except_exact_claim(
                &prepared,
                "stream token admission",
            )
            .await?;
            ensure_same_binding(key, &prepared, "stream token final admission authority")?;
            drop(provisional);
            self.validate_stream_logical_admission_batch(&prepared, &batch)?;

            let mut overlay_current = BTreeMap::new();
            let mut durable_keys = std::collections::BTreeSet::new();
            for logical_id in &ordered_ids {
                if let Some(current) = self
                    .stream_workers
                    .confirmed_token_for_key(&queued, table_key, logical_id)
                    .await?
                {
                    overlay_current.insert(logical_id.clone(), current);
                } else {
                    durable_keys.insert(logical_id.clone());
                }
            }
            let (durable_current, durable_metadata) = if durable_keys.is_empty() {
                (BTreeMap::new(), BTreeMap::new())
            } else {
                let token_dataset = prepared.txn.base.open_stream_token_authority().await?;
                let authority = stream_token_rows_for_keys(
                    &token_dataset,
                    prepared.txn.base.stream_token_authority(),
                    prepared.entry.identity,
                    &durable_keys,
                )
                .await?;
                let metadata = lookup_base_stream_metadata_for_keys(
                    prepared.head.dataset(),
                    prepared.entry.identity,
                    &durable_keys,
                )
                .await?;
                for logical_id in &durable_keys {
                    validate_authority_base_pair(
                        prepared.entry.identity,
                        logical_id,
                        authority.get(logical_id),
                        metadata.get(logical_id),
                    )
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                }
                (authority, metadata)
            };

            struct NewRow {
                metadata: TrustedStreamRowMetadata,
                authority: StreamTokenAuthorityRow,
                ack: StreamTokenAdmissionAck,
                ambiguous: StreamB2AmbiguousRow,
            }

            let mut new_rows = Vec::new();
            // One physical invocation owns one attempt id even when it carries
            // several logical occurrences. This is the shared ambiguity and audit
            // identity surfaced for every row if watcher/fence proof is lost.
            let admission_attempt_id = ShardId::new_v4().to_string();
            let stream_incarnation_id = prepared
                .lifecycle
                .enrollment_receipt
                .stream_incarnation_id
                .as_str();
            for (row, (logical_id, envelope)) in ordered_ids
                .into_iter()
                .zip(envelopes.into_iter())
                .enumerate()
            {
                let canonical_payload = super::canonical_stream_payload_v1(&batch, row)?;
                let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
                    identity: prepared.entry.identity,
                    accepted_schema_hash: &prepared.txn.authority.schema_ir_hash,
                    canonical_payload: &canonical_payload,
                })
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                let request = AdmissionRequest {
                    identity: prepared.entry.identity,
                    logical_id,
                    envelope,
                    contributor_id: contributor_id.clone(),
                    payload_digest,
                };
                request
                    .validate()
                    .map_err(|error| OmniError::manifest(error.to_string()))?;
                let logical_id = &request.logical_id;
                let overlay = overlay_current.get(logical_id);
                let current_authority = overlay
                    .map(|current| &current.authority)
                    .or_else(|| durable_current.get(logical_id));
                let current_metadata = overlay
                    .map(|current| &current.metadata)
                    .or_else(|| durable_metadata.get(logical_id));
                let classification = classify_admission(
                    stream_incarnation_id,
                    &request,
                    current_authority,
                    current_metadata,
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                let candidate = match classification {
                    AdmissionClassification::New { candidate_token } => candidate_token,
                    AdmissionClassification::AlreadyDurable { authority, .. } => {
                        if new_rows.is_empty() {
                            return Ok(StreamB2PrefixOutcome::Boundary {
                                caller_ordinal: caller_ordinals.start,
                                binding: classified_binding.clone(),
                                disposition: StreamB2BoundaryDisposition::AlreadyDurable(
                                    StreamTokenAdmissionAck {
                                        stream_token: authority.current_token,
                                        origin: authority.origin,
                                        disposition: authority.disposition,
                                        terminal_correction: authority.terminal_correction,
                                        already_durable: true,
                                    },
                                ),
                            });
                        }
                        break;
                    }
                    AdmissionClassification::BindingChanged {
                        current_stream_incarnation_id,
                    } => {
                        if new_rows.is_empty() {
                            return Ok(StreamB2PrefixOutcome::Boundary {
                                caller_ordinal: caller_ordinals.start,
                                binding: classified_binding.clone(),
                                disposition: StreamB2BoundaryDisposition::BindingChanged {
                                    stable_table_id: request.identity.stable_table_id,
                                    table_incarnation_id: request.identity.table_incarnation_id,
                                    current_stream_incarnation_id,
                                },
                            });
                        }
                        break;
                    }
                    AdmissionClassification::SequenceConflict { current_token } => {
                        if new_rows.is_empty() {
                            return Ok(StreamB2PrefixOutcome::Boundary {
                                caller_ordinal: caller_ordinals.start,
                                binding: classified_binding.clone(),
                                disposition: StreamB2BoundaryDisposition::SequenceConflict {
                                    stable_table_id: request.identity.stable_table_id,
                                    table_incarnation_id: request.identity.table_incarnation_id,
                                    logical_id: request.logical_id,
                                    current_token: current_token.map(|token| token.to_string()),
                                },
                            });
                        }
                        break;
                    }
                    AdmissionClassification::IdempotencyConflict { current_token } => {
                        if new_rows.is_empty() {
                            return Ok(StreamB2PrefixOutcome::Boundary {
                                caller_ordinal: caller_ordinals.start,
                                binding: classified_binding.clone(),
                                disposition: StreamB2BoundaryDisposition::IdempotencyConflict {
                                    stable_table_id: request.identity.stable_table_id,
                                    table_incarnation_id: request.identity.table_incarnation_id,
                                    logical_id: request.logical_id,
                                    current_token: current_token.to_string(),
                                },
                            });
                        }
                        break;
                    }
                };
                let caller_ordinal = caller_ordinals
                    .start
                    .checked_add(u64::try_from(row).unwrap_or(u64::MAX))
                    .ok_or_else(|| {
                        OmniError::manifest_internal("stream caller ordinal overflow")
                    })?;
                let (fold_base_token, chain_depth) = match overlay {
                    Some(current) => (
                        current.metadata.fold_base_token,
                        current.metadata.chain_depth.checked_add(1).ok_or_else(|| {
                            OmniError::resource_limit(
                                format!("stream chain depth for {table_key}/{logical_id}"),
                                u32::MAX as u64,
                                u32::MAX as u64 + 1,
                            )
                        })?,
                    ),
                    None => (request.envelope.predecessor_token, 1),
                };
                let metadata = TrustedStreamRowMetadata::new_admission(
                    &request,
                    candidate,
                    fold_base_token,
                    chain_depth,
                    admission_attempt_id.clone(),
                    caller_ordinal,
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                let logical_write_id = request.envelope.write_id;
                let authority = StreamTokenAuthorityRow::from_present_metadata(
                    request.identity,
                    request.logical_id,
                    prepared.binding.enrollment_id.clone(),
                    &metadata,
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                validate_stream_token_plan_bounds(std::slice::from_ref(&authority))?;
                let origin = metadata.origin.clone();
                let admission_attempt_id = match &origin {
                    StreamRowOrigin::Admission {
                        admission_attempt_id,
                        ..
                    } => admission_attempt_id.clone(),
                    StreamRowOrigin::Correction { .. } => {
                        return Err(OmniError::manifest_internal(
                            "stream admission minted a correction origin",
                        ));
                    }
                };
                new_rows.push(NewRow {
                    metadata,
                    authority,
                    ack: StreamTokenAdmissionAck {
                        stream_token: candidate,
                        origin,
                        disposition: StreamTokenDisposition::Present,
                        terminal_correction: None,
                        already_durable: false,
                    },
                    ambiguous: StreamB2AmbiguousRow {
                        caller_ordinal,
                        admission_attempt_id,
                        logical_write_id,
                        unconfirmed_candidate_token: candidate,
                    },
                });
            }
            drop(contributor_id);
            drop(overlay_current);
            drop(durable_current);
            drop(durable_metadata);
            drop(durable_keys);

            if new_rows.is_empty() {
                return Err(OmniError::manifest_internal(
                    "B2 classification produced neither a boundary nor a new prefix",
                ));
            }

            // Find the longest exact prefix whose complete current-generation
            // token projection fits. Adding a distinct-key update can replace a
            // larger existing winner, so fit is not monotonic: prefix N may fail
            // while N + 1 fits. Start with the common full-prefix case and scan
            // downward only after a capacity failure. The method boundary caps
            // this exact scan at STREAM_B2_CLASSIFICATION_WINDOW_ROWS.
            let stream_workers = &self.stream_workers;
            let queued_for_projection = &queued;
            let token_prefix = longest_fitting_token_prefix(new_rows.len(), |prefix| {
                let updates = new_rows[..prefix]
                    .iter()
                    .map(|row| {
                        (
                            row.authority.logical_id.clone(),
                            ConfirmedStreamTokenOverlayRow {
                                authority: row.authority.clone(),
                                metadata: row.metadata.clone(),
                            },
                        )
                    })
                    .collect::<ConfirmedStreamTokenOverlay>();
                async move {
                    let projected = stream_workers
                        .projected_token_authority_rows(queued_for_projection, table_key, &updates)
                        .await?;
                    match validate_generation_token_plan(table_key, &projected) {
                        Ok(()) => Ok(None),
                        Err(error @ OmniError::FoldRequired { .. }) => Ok(Some(error)),
                        Err(error) => Err(error),
                    }
                }
            })
            .await?;
            new_rows.truncate(token_prefix);

            let attributed = append_trusted_stream_metadata(
                batch.slice(0, token_prefix),
                new_rows
                    .iter()
                    .map(|row| Some(row.metadata.clone()))
                    .collect(),
            )?;
            // `attributed` still shares the selected source columns. The final
            // dense rebuild below is the only batch allowed to cross into worker
            // ownership, so the larger caller batch can leave this phase now.
            drop(batch);
            // A single attributed row which cannot fit an empty generation is an
            // intrinsic input limit, not a fold boundary (folding cannot help).
            validate_stream_stored_bounds(table_key, &attributed.slice(0, 1))?;
            let mut low = 1_usize;
            let mut high = attributed.num_rows();
            let mut physical_prefix = 0_usize;
            let mut physical_capacity_error = None;
            while low <= high {
                let mid = low + (high - low) / 2;
                let prefix = attributed.slice(0, mid);
                match queued.reprice_for_exact_batch(table_key, &prefix) {
                    Ok(()) => {
                        physical_prefix = mid;
                        low = mid.saturating_add(1);
                    }
                    Err(error @ OmniError::FoldRequired { .. })
                    | Err(error @ OmniError::ResourceLimitExceeded { .. }) => {
                        physical_capacity_error = Some(error);
                        high = mid.saturating_sub(1);
                    }
                    Err(error) => return Err(error),
                }
            }
            if physical_prefix == 0 {
                return Err(physical_capacity_error.unwrap_or_else(|| {
                    OmniError::manifest_internal(
                        "B2 physical prefix search found no admissible row",
                    )
                }));
            }
            new_rows.truncate(physical_prefix);
            let selected = attributed.slice(0, physical_prefix);
            let row_count = u32::try_from(selected.num_rows())
                .map_err(|_| OmniError::manifest_internal("B2 selected prefix exceeds u32 rows"))?;
            let indices = UInt32Array::from_iter_values(0..row_count);
            let columns = selected
                .columns()
                .iter()
                .map(|column| {
                    take(column.as_ref(), &indices, None)
                        .map_err(|error| OmniError::Lance(error.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(selected.schema(), columns)
                .map_err(|error| OmniError::Lance(error.to_string()))?;
            drop(selected);
            drop(attributed);
            queued.reprice_for_exact_batch(table_key, &batch)?;
            self.validate_stream_admission_batch(&prepared, &batch)?;
            // The first-row queue charge becomes the complete selected physical
            // charge here. Until this point the B2 preprocessing envelope keeps
            // the prepared multi-row tail bounded even though only row zero was
            // eligible for worker-capacity arbitration.
            let token_updates = new_rows
                .iter()
                .map(|row| {
                    (
                        row.authority.logical_id.clone(),
                        ConfirmedStreamTokenOverlayRow {
                            authority: row.authority.clone(),
                            metadata: row.metadata.clone(),
                        },
                    )
                })
                .collect::<ConfirmedStreamTokenOverlay>();
            // A physical-capacity split can shorten the token-validated prefix.
            // Replacements in the warm overlay are not size-monotonic (for example,
            // a successor contributor can be shorter), so a smaller prefix is not
            // automatically covered by the larger prefix's proof. Revalidate the
            // exact update set that will cross the acknowledgement boundary.
            let projected = self
                .stream_workers
                .projected_token_authority_rows(&queued, table_key, &token_updates)
                .await?;
            validate_generation_token_plan(table_key, &projected)?;
            drop(projected);
            drop(prepared);
            drop(preprocessing);

            let end = caller_ordinals
                .start
                .checked_add(u64::try_from(physical_prefix - 1).unwrap_or(u64::MAX))
                .ok_or_else(|| OmniError::manifest_internal("B2 admitted ordinal overflow"))?;
            let admitted_ordinals =
                CallerOrdinalRange::new(caller_ordinals.start, end).map_err(worker_error)?;
            worker_put_attempted = true;
            let durable = match self
                .finish_reserved_stream_put(
                    table_key.to_string(),
                    batch,
                    admitted_ordinals,
                    key,
                    admission_key,
                    queued,
                    put_authority,
                    token_updates,
                )
                .await
            {
                Ok(durable) => durable,
                Err(error) => {
                    return match error {
                        OmniError::AckUnknown {
                            stable_table_id,
                            table_incarnation_id,
                            enrollment_id,
                            shard_id,
                            writer_epoch,
                            caller_ordinal_start,
                            caller_ordinal_end,
                            reason,
                            ..
                        } => Ok(StreamB2PrefixOutcome::AckUnknown(StreamB2AckUnknown {
                            stable_table_id,
                            table_incarnation_id,
                            binding: StreamB2BindingProof {
                                enrollment_id,
                                shard_id,
                                writer_epoch,
                            },
                            caller_ordinals: CallerOrdinalRange::new(
                                caller_ordinal_start,
                                caller_ordinal_end,
                            )
                            .map_err(worker_error)?,
                            rows: new_rows.into_iter().map(|row| row.ambiguous).collect(),
                            reason,
                        })),
                        other => Err(other),
                    };
                }
            };
            Ok(StreamB2PrefixOutcome::Admitted {
                caller_ordinals: admitted_ordinals,
                binding: StreamB2BindingProof {
                    enrollment_id: durable.enrollment_id.to_string(),
                    shard_id: durable.shard_id.to_string(),
                    writer_epoch: durable.writer_epoch,
                },
                acknowledgements: new_rows.into_iter().map(|row| row.ack).collect(),
            })
        }
        .await;
        match outcome {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let binding = if worker_put_attempted {
                    // A cold prepare can durably advance the shard epoch and
                    // then refuse effect-free (most notably when replay makes
                    // the opened worker fold-only). The worker task has
                    // settled before returning this non-AckUnknown error, so
                    // recapture the achieved/current authority instead of
                    // reporting the pre-claim epoch. If authority moved again,
                    // fail closed rather than pairing the refusal with a
                    // binding from another lane.
                    let achieved = self
                        .capture_stream_authority(table_key, "stream put refusal binding")
                        .await?;
                    ensure_same_binding(key, &achieved, "stream put refusal binding")?;
                    StreamB2BindingProof {
                        enrollment_id: achieved.enrollment_id.to_string(),
                        shard_id: achieved.shard_id.to_string(),
                        writer_epoch: achieved.epoch_floor,
                    }
                } else {
                    classified_binding
                };
                Ok(StreamB2PrefixOutcome::Refused {
                    caller_ordinal: caller_ordinals.start,
                    binding,
                    error,
                })
            }
        }
    }

    /// Finish one already-queued stream append.  B1 supplies an empty token
    /// projection; B2 supplies the exact watcher-confirmed updates which must
    /// become warm only after the post-durability fence check.
    fn stream_idle_authority_check(
        self: &Arc<Self>,
        key: StreamWorkerKey,
        admission_key: StreamAdmissionKey,
        table_key: String,
    ) -> IdleAuthorityCheck {
        let idle_db = Arc::clone(self);
        Arc::new(move |writer: Arc<ShardWriter>| {
            let db = Arc::clone(&idle_db);
            let admission_key = admission_key.clone();
            let table_key = table_key.clone();
            Box::pin(async move {
                let shared = db.write_queue().acquire_stream_shared(&admission_key).await;
                let authority = CheckedStreamAuthority::from_shared_admission(shared);
                let checked = async {
                    db.ensure_no_relevant_stream_sidecar(key.identity, "stream idle eviction")
                        .await?;
                    let before = db
                        .capture_stream_authority(&table_key, "stream idle eviction")
                        .await?;
                    ensure_same_binding(key, &before, "stream idle eviction authority")?;
                    db.validate_claimed_writer_for_capture(&writer, key, &before)
                        .await?;

                    db.ensure_no_relevant_stream_sidecar(key.identity, "stream idle eviction")
                        .await?;
                    let after = db
                        .capture_stream_authority(&table_key, "stream idle eviction")
                        .await?;
                    ensure_same_capture(&before, &after, "stream idle eviction final authority")?;
                    db.validate_claimed_writer_for_capture(&writer, key, &after)
                        .await
                }
                .await;
                match checked {
                    Ok(()) => Ok(authority),
                    Err(error) => Err(IdleAuthorityFailure::new(error, authority)),
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_reserved_stream_put(
        self: &Arc<Self>,
        table_key: String,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
        key: StreamWorkerKey,
        admission_key: StreamAdmissionKey,
        queued: QueuedBatchPermit,
        put_authority: CheckedStreamAuthority,
        confirmed_token_updates: ConfirmedStreamTokenOverlay,
    ) -> Result<DurableBatchAck> {
        let admitted_batch = batch.clone();
        let idle_authority =
            self.stream_idle_authority_check(key, admission_key, table_key.clone());
        let db = Arc::clone(self);
        let prepare_table_key = table_key.clone();
        let prepare = Box::new(move |warm_writer: Option<Arc<ShardWriter>>| {
            Box::pin(async move {
                // Admission is outermost and remains inside the detached worker
                // through watcher success or retained abort retirement.
                let authority = put_authority;

                match warm_writer {
                    Some(writer) => {
                        let checked = async {
                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let before = db
                                .capture_stream_authority(&prepare_table_key, "stream put")
                                .await?;
                            ensure_same_binding(key, &before, "stream put final authority")?;
                            db.validate_stream_admission_batch(&before, &admitted_batch)?;
                            db.validate_claimed_writer_for_capture(&writer, key, &before)
                                .await?;

                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let after = db
                                .capture_stream_authority(&prepare_table_key, "stream put")
                                .await?;
                            ensure_same_capture(
                                &before,
                                &after,
                                "stream put final warm authority",
                            )?;
                            db.validate_stream_admission_batch(&after, &admitted_batch)?;
                            db.validate_claimed_writer_for_capture(&writer, key, &after)
                                .await
                        }
                        .await;
                        match checked {
                            Ok(()) => Ok(PreparedPut::warm(authority)),
                            Err(error) => Err(PreparedPutFailure::warm(error, authority)),
                        }
                    }
                    None => {
                        let before = match async {
                            let before = db
                                .capture_stream_authority(&prepare_table_key, "stream put")
                                .await?;
                            db.ensure_no_relevant_stream_sidecar_except_exact_claim(
                                &before,
                                "stream put",
                            )
                            .await?;
                            ensure_same_binding(key, &before, "stream put final authority")?;
                            db.validate_stream_admission_batch(&before, &admitted_batch)?;
                            Ok::<_, OmniError>(before)
                        }
                        .await
                        {
                            Ok(before) => before,
                            Err(error) => {
                                return Err(PreparedPutFailure::cold_unclaimed(error, authority));
                            }
                        };
                        let opened = match Box::pin(db.open_stream_writer_with_claim(
                            &before,
                            "COLD_PUT",
                            Some("omnigraph:stream-runtime".to_string()),
                        ))
                        .await
                        {
                            Ok(opened) => opened,
                            Err(failure) => {
                                let (error, claimed) = failure.into_parts();
                                let error = worker_error(error);
                                return Err(match claimed {
                                    Some(claimed) => {
                                        PreparedPutFailure::cold_claimed(error, authority, claimed)
                                    }
                                    None => PreparedPutFailure::cold_unclaimed(error, authority),
                                });
                            }
                        };
                        if let Err(error) = crate::failpoints::maybe_fail(
                            crate::failpoints::names::STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY,
                        ) {
                            return Err(PreparedPutFailure::cold_opened(
                                error, authority, opened,
                            ));
                        }

                        let checked = async {
                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let after = db
                                .capture_stream_authority(&prepare_table_key, "stream put")
                                .await?;
                            ensure_claim_successor_capture(
                                &before,
                                &after,
                                "stream put claim-to-put authority",
                            )?;
                            db.validate_stream_admission_batch(&after, &admitted_batch)?;
                            db.validate_claimed_writer_for_capture(opened.writer(), key, &after)
                                .await
                        }
                        .await;
                        match checked {
                            Ok(()) => Ok(PreparedPut::cold(authority, opened)),
                            Err(error) => {
                                Err(PreparedPutFailure::cold_opened(error, authority, opened))
                            }
                        }
                    }
                }
            }) as crate::table_store::mem_wal::PreparePutFuture
        });

        self.stream_workers
            .put(
                key,
                table_key,
                batch,
                caller_ordinals,
                confirmed_token_updates,
                queued,
                prepare,
                idle_authority,
            )
            .await
    }

    /// Seal, drain, fold, and graph-publish exactly one private B1 generation.
    ///
    /// The entire operation is detached, not merely the Lance seal.  This keeps
    /// the exclusive admission token alive from the cut through recovery arm,
    /// the exact base-table effect, and the one manifest visibility CAS even if
    /// the requesting task is cancelled.
    pub(crate) async fn stream_fold_phase_b1(self: &Arc<Self>, table_key: &str) -> Result<()> {
        let db = Arc::clone(self);
        let table_key = table_key.to_string();
        crate::instrumentation::spawn_with_query_io_probes(async move {
            db.stream_fold_phase_b1_background(table_key).await
        })
        .await
        .map_err(|error| OmniError::Lance(format!("stream fold task failed: {error}")))?
    }

    async fn stream_fold_phase_b1_background(self: Arc<Self>, table_key: String) -> Result<()> {
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        let provisional = self
            .capture_stream_authority(&table_key, "stream fold")
            .await?;
        let key = provisional.worker_key;
        let admission_key = provisional.admission_key.clone();

        let exclusive = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;
        let before_cut = self
            .capture_stream_authority(&table_key, "stream fold")
            .await?;
        self.ensure_no_relevant_stream_sidecar_except_exact_claim(&before_cut, "stream fold")
            .await?;
        ensure_same_binding(key, &before_cut, "stream fold pre-cut authority")?;

        let opener_db = Arc::clone(&self);
        let opener_capture = before_cut.clone();
        let opener = Box::new(move || {
            Box::pin(async move {
                Box::pin(opener_db.open_stream_writer_with_claim(
                    &opener_capture,
                    "OPEN_FOLD",
                    Some("omnigraph:stream-fold".to_string()),
                ))
                .await
            }) as crate::table_store::mem_wal::WorkerOpenFuture
        });
        let cut = self
            .stream_workers
            .seal_and_drain(
                key,
                table_key.clone(),
                CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive),
                opener,
            )
            .await
            .map_err(|error| fold_cut_error(key, error))?;
        // A cold quiesce opener may have selected one or more recovered/fresh
        // claims. The fold binds the exact post-claim lifecycle and selected
        // ClaimReceipt, never the provisional pre-claim capture.
        let post_claim = self
            .capture_stream_authority(&table_key, "stream fold post-claim")
            .await?;
        ensure_same_binding(key, &post_claim, "stream fold post-claim binding")?;

        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR,
        )?;

        let mut last_effect_free_error = None;
        for attempt in 0..B1_MAX_FOLD_ATTEMPTS {
            // Keep the large closed recovery-v12 fold future off this Tokio
            // worker's stack. Debug failpoint builds otherwise compose it with
            // the surrounding recovery/barrier future and can exceed the
            // default worker stack in multi-table crash tests.
            match Box::pin(self.stream_fold_attempt(
                &table_key,
                key,
                &post_claim,
                &cut,
                &FoldLifecycleMode::Open,
            ))
            .await
            {
                Ok(FoldAttempt::Published) => return Ok(()),
                Ok(FoldAttempt::EffectFree(error)) if attempt + 1 < B1_MAX_FOLD_ATTEMPTS => {
                    last_effect_free_error = Some(error);
                }
                Ok(FoldAttempt::EffectFree(error)) => return Err(error),
                Err(error) => return Err(error),
            }
        }
        Err(last_effect_free_error.unwrap_or_else(|| {
            OmniError::manifest_internal("stream fold exhausted without an outcome")
        }))
    }

    /// Quiesce one enrolled main-branch lane behind the private lifecycle seam.
    ///
    /// The outer task owns one profile-shared guard and one exclusive
    /// admission lease from the OPEN cutoff through terminal receipt
    /// publication. Cancellation of the caller cannot reopen admission between
    /// the physical cut and the durable SEALED proof.
    pub(crate) async fn stream_quiesce_as(
        self: &Arc<Self>,
        table_key: &str,
        drain_id: &str,
        expected_lifecycle_revision: u64,
        actor_id: &str,
    ) -> Result<()> {
        let db = Arc::clone(self);
        let table_key = table_key.to_string();
        let drain_id = drain_id.to_string();
        let actor_id = actor_id.to_string();
        crate::instrumentation::spawn_with_query_io_probes(async move {
            Box::pin(db.stream_quiesce_background(
                table_key,
                drain_id,
                expected_lifecycle_revision,
                actor_id,
            ))
            .await
        })
        .await
        .map_err(|error| OmniError::Lance(format!("stream quiesce task failed: {error}")))?
    }

    /// Settle the one terminal receipt sidecar owned by an exact quiesce
    /// retry before receipt-first classification.
    ///
    /// A crash after the immutable ledger effect or terminal manifest CAS can
    /// leave the selected receipt invisible to a cached snapshot while the
    /// graph-global sidecar barrier remains armed. Generic recovery cannot run
    /// from inside this lifecycle operation because it would reacquire the
    /// admission/profile gates already held by the caller. This deliberately
    /// narrow continuation recognizes only the same table, operation kind,
    /// and drain ID and completes it under the normal inner effect gates.
    async fn complete_exact_quiesce_receipt_sidecar(
        &self,
        table_key: &str,
        identity: TableIdentity,
        drain_id: &str,
    ) -> Result<bool> {
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;

        let mut exact: Option<crate::db::manifest::RecoverySidecar> = None;
        for sidecar in list_sidecars(self.root_uri(), self.storage_adapter()).await? {
            let Some(RecoveryProtocolV14::StreamLifecycleReceipt(protocol)) =
                sidecar.protocol_v14.as_deref()
            else {
                continue;
            };
            if protocol.change_kind != RecoveryStreamLifecycleReceiptKind::QuiesceFinalize
                || protocol.admission_scope.identity != identity
                || protocol.receipt.planned_receipt.identity != identity
                || protocol.receipt.planned_receipt.operation_kind != "QUIESCE"
                || protocol.receipt.planned_receipt.operation_id != drain_id
            {
                continue;
            }
            if let Some(prior) = exact.as_ref() {
                return Err(OmniError::recovery_required(
                    sidecar.operation_id,
                    format!(
                        "multiple terminal quiesce receipt sidecars match table identity {identity} and drain '{drain_id}' (also found '{}')",
                        prior.operation_id
                    ),
                ));
            }
            exact = Some(sidecar);
        }
        let Some(sidecar) = exact else {
            return Ok(false);
        };

        // The sidecar may already have published its manifest CAS while this
        // process still holds the prior cached snapshot. Recovery must classify
        // against a fresh manifest view so exact post-publish cleanup is
        // idempotent instead of attempting a second terminal CAS.
        self.refresh_coordinator_only().await?;
        let txn = self.open_write_txn(None).await?;
        complete_stream_lifecycle_receipt_sidecar_v14(
            self.root_uri(),
            Arc::clone(&self.storage),
            &txn.base,
            &sidecar,
        )
        .await?;
        self.refresh_coordinator_only().await?;
        Ok(true)
    }

    async fn stream_quiesce_background(
        self: Arc<Self>,
        table_key: String,
        drain_id: String,
        expected_lifecycle_revision: u64,
        actor_id: String,
    ) -> Result<()> {
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        let mut initial_txn = self.open_write_txn(None).await?;
        let mut initial_entry = initial_txn.base.entry(&table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "stream quiesce cannot resolve unknown table '{table_key}'"
            ))
        })?;
        let mut initial_lifecycle = initial_txn
            .base
            .stream_lifecycle(initial_entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "stream quiesce requires an enrolled stream for '{table_key}'"
                ))
            })?;
        let graph_identity_digest =
            stream_graph_identity_digest(&initial_txn.authority.schema_identity_domain)?;
        if self
            .complete_exact_quiesce_receipt_sidecar(&table_key, initial_entry.identity, &drain_id)
            .await?
        {
            initial_txn = self.open_write_txn(None).await?;
            initial_entry = initial_txn.base.entry(&table_key).cloned().ok_or_else(|| {
                OmniError::manifest_not_found(format!(
                    "stream quiesce lost table '{table_key}' while completing its receipt"
                ))
            })?;
            initial_lifecycle = initial_txn
                .base
                .stream_lifecycle(initial_entry.identity)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "stream quiesce receipt completion lost its lifecycle row",
                    )
                })?;
        }

        // A selected strict block is already the durable result of this
        // quiesce occurrence. Validate the immutable request before any token
        // ledger or MemWAL read, then return the stable token. This keeps a
        // lost-reply retry self-contained even when the retained physical cut
        // is temporarily unavailable. The later under-exclusive check remains
        // necessary for a block published while this caller waits.
        if initial_lifecycle.lifecycle == StreamLifecycle::Draining {
            let drain = initial_lifecycle.drain.as_ref().ok_or_else(|| {
                OmniError::manifest_internal("DRAINING stream has no drain descriptor")
            })?;
            if drain.drain_id != drain_id
                || drain.operation_expected_revision != expected_lifecycle_revision
                || drain.initiating_actor != actor_id
                || drain.goal != DrainGoal::Sealed
            {
                return Err(OmniError::StreamLifecycleIdempotencyConflict {
                    stable_table_id: initial_entry.identity.stable_table_id,
                    table_incarnation_id: initial_entry.identity.table_incarnation_id,
                    operation_kind: "QUIESCE".to_string(),
                    operation_id: drain_id,
                });
            }
            stream_quiesce_request_payload_from_draining(
                &initial_lifecycle,
                &graph_identity_digest,
            )?;
            if let Some(block) = initial_lifecycle.strict_block.as_ref() {
                return Err(stream_data_block_error(&block.block_token));
            }
        }

        // Terminal receipt-first replay precedes lifecycle/revision
        // classification. A delayed exact retry must return success even after
        // the lane has moved to SEALED; the operation ID can never be rebound.
        let selected_token = initial_txn.base.open_stream_token_authority().await?;
        if let Some(receipt) = lookup_management_receipt(
            &selected_token,
            initial_txn.base.stream_token_authority(),
            &graph_identity_digest,
            initial_entry.identity,
            &initial_lifecycle.enrollment_receipt.stream_incarnation_id,
            "QUIESCE",
            &drain_id,
        )
        .await?
        {
            validate_selected_quiesce_receipt(
                &initial_lifecycle,
                &receipt,
                &graph_identity_digest,
                &drain_id,
                expected_lifecycle_revision,
                &actor_id,
            )?;
            return Ok(());
        }

        let provisional = match initial_lifecycle.lifecycle {
            StreamLifecycle::Open => {
                if initial_lifecycle.lifecycle_revision != expected_lifecycle_revision {
                    return Err(OmniError::StreamLifecycleChanged {
                        stable_table_id: initial_entry.identity.stable_table_id,
                        table_incarnation_id: initial_entry.identity.table_incarnation_id,
                        expected_revision: expected_lifecycle_revision,
                        current_revision: initial_lifecycle.lifecycle_revision,
                    });
                }
                self.capture_stream_authority(&table_key, "stream quiesce")
                    .await?
            }
            StreamLifecycle::Draining => {
                let drain = initial_lifecycle.drain.as_ref().ok_or_else(|| {
                    OmniError::manifest_internal("DRAINING stream has no drain descriptor")
                })?;
                if drain.drain_id != drain_id
                    || drain.operation_expected_revision != expected_lifecycle_revision
                    || drain.initiating_actor != actor_id
                    || drain.goal != DrainGoal::Sealed
                {
                    return Err(OmniError::StreamLifecycleIdempotencyConflict {
                        stable_table_id: initial_entry.identity.stable_table_id,
                        table_incarnation_id: initial_entry.identity.table_incarnation_id,
                        operation_kind: "QUIESCE".to_string(),
                        operation_id: drain_id,
                    });
                }
                self.capture_draining_stream_authority(
                    &table_key,
                    "stream quiesce continuation",
                    &drain_id,
                )
                .await?
            }
            StreamLifecycle::Sealed => {
                return Err(OmniError::StreamLifecycleChanged {
                    stable_table_id: initial_entry.identity.stable_table_id,
                    table_incarnation_id: initial_entry.identity.table_incarnation_id,
                    expected_revision: expected_lifecycle_revision,
                    current_revision: initial_lifecycle.lifecycle_revision,
                });
            }
        };
        let expected_request_digest = match provisional.lifecycle.lifecycle {
            StreamLifecycle::Open => {
                let target_epoch_floor_by_shard = provisional
                    .lifecycle
                    .epoch_floor_by_shard
                    .iter()
                    .map(|(shard, epoch)| {
                        epoch
                            .checked_add(1)
                            .map(|next| (shard.clone(), next))
                            .ok_or_else(|| {
                                OmniError::manifest_internal("stream drain epoch target overflow")
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                stream_quiesce_request_digest(
                    &provisional.lifecycle,
                    &QuiesceRequest {
                        graph_identity_digest: graph_identity_digest.clone(),
                        drain_id: drain_id.clone(),
                        expected_lifecycle_revision,
                        goal: DrainGoal::Sealed,
                        initiating_actor: actor_id.clone(),
                        initiated_at: crate::db::now_micros()?,
                        target_epoch_floor_by_shard,
                        seal_override: None,
                    },
                )?
            }
            StreamLifecycle::Draining => provisional
                .lifecycle
                .drain
                .as_ref()
                .ok_or_else(|| {
                    OmniError::manifest_internal("DRAINING stream has no drain descriptor")
                })?
                .operation_request_digest
                .clone(),
            StreamLifecycle::Sealed => unreachable!("SEALED provisional authority was refused"),
        };
        let key = provisional.worker_key;
        let exclusive = self
            .write_queue()
            .acquire_stream_exclusive(&provisional.admission_key)
            .await;
        let exclusive_authority =
            CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive);

        if self
            .complete_exact_quiesce_receipt_sidecar(&table_key, key.identity, &drain_id)
            .await?
        {
            let settled = self.open_write_txn(None).await?;
            let settled_lifecycle = settled
                .base
                .stream_lifecycle(key.identity)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "quiesce receipt completion lost its lifecycle row",
                    )
                })?;
            let selected = settled.base.open_stream_token_authority().await?;
            if let Some(receipt) = lookup_management_receipt(
                &selected,
                settled.base.stream_token_authority(),
                &graph_identity_digest,
                key.identity,
                &settled_lifecycle.enrollment_receipt.stream_incarnation_id,
                "QUIESCE",
                &drain_id,
            )
            .await?
            {
                validate_selected_quiesce_receipt(
                    &settled_lifecycle,
                    &receipt,
                    &graph_identity_digest,
                    &drain_id,
                    expected_lifecycle_revision,
                    &actor_id,
                )?;
                return Ok(());
            }
        }

        let current = self.open_write_txn(None).await?;
        let current_lifecycle = current
            .base
            .stream_lifecycle(key.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_quiesce_lifecycle:{table_key}"),
                    Some(format!("{:?}", provisional.lifecycle)),
                    None,
                )
            })?;
        let current_token = current.base.open_stream_token_authority().await?;
        if let Some(receipt) = lookup_management_receipt(
            &current_token,
            current.base.stream_token_authority(),
            &graph_identity_digest,
            key.identity,
            &current_lifecycle.enrollment_receipt.stream_incarnation_id,
            "QUIESCE",
            &drain_id,
        )
        .await?
        {
            validate_selected_quiesce_receipt(
                &current_lifecycle,
                &receipt,
                &graph_identity_digest,
                &drain_id,
                expected_lifecycle_revision,
                &actor_id,
            )?;
            return Ok(());
        }
        if current_lifecycle.lifecycle == StreamLifecycle::Open {
            if current_lifecycle.lifecycle_revision != expected_lifecycle_revision {
                return Err(OmniError::StreamLifecycleChanged {
                    stable_table_id: key.identity.stable_table_id,
                    table_incarnation_id: key.identity.table_incarnation_id,
                    expected_revision: expected_lifecycle_revision,
                    current_revision: current_lifecycle.lifecycle_revision,
                });
            }
            let target_epoch_floor_by_shard = current_lifecycle
                .epoch_floor_by_shard
                .iter()
                .map(|(shard, epoch)| {
                    epoch
                        .checked_add(1)
                        .map(|next| (shard.clone(), next))
                        .ok_or_else(|| {
                            OmniError::manifest_internal("stream drain epoch target overflow")
                        })
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            let request = QuiesceRequest {
                graph_identity_digest: graph_identity_digest.clone(),
                drain_id: drain_id.clone(),
                expected_lifecycle_revision,
                goal: DrainGoal::Sealed,
                initiating_actor: actor_id.clone(),
                initiated_at: crate::db::now_micros()?,
                target_epoch_floor_by_shard,
                seal_override: None,
            };
            let started = build_open_to_draining(&current_lifecycle, request)?;
            if started.request_digest != expected_request_digest {
                return Err(OmniError::StreamLifecycleIdempotencyConflict {
                    stable_table_id: key.identity.stable_table_id,
                    table_incarnation_id: key.identity.table_incarnation_id,
                    operation_kind: "QUIESCE".to_string(),
                    operation_id: drain_id,
                });
            }

            let write_queue = self.write_queue();
            let _schema_guard = write_queue
                .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
                .await;
            let _branch_guard = write_queue.acquire_branch(None).await;
            let _stream_token_guard = write_queue.acquire_stream_token().await;
            let _table_guards = write_queue.acquire_many(&[(table_key.clone(), None)]).await;
            self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream quiesce start")
                .await?;
            let live = self.revalidate_write_txn(&current).await?;
            let live_entry = live.entry(&table_key).ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_quiesce_table:{table_key}"),
                    Some(key.identity.to_string()),
                    None,
                )
            })?;
            if live.stream_lifecycle(key.identity) != Some(&current_lifecycle) {
                return Err(OmniError::StreamLifecycleChanged {
                    stable_table_id: key.identity.stable_table_id,
                    table_incarnation_id: key.identity.table_incarnation_id,
                    expected_revision: expected_lifecycle_revision,
                    current_revision: live
                        .stream_lifecycle(key.identity)
                        .map_or(0, |lifecycle| lifecycle.lifecycle_revision),
                });
            }
            let mut expected_versions = ExpectedTableVersions::new();
            expected_versions.insert(
                key.identity,
                TableVersionExpectation {
                    table_key: live_entry.table_key.clone(),
                    table_version: live_entry.table_version,
                },
            );
            let mut coordinator = self.open_coordinator_for_branch(None).await?;
            if coordinator.snapshot().version() != live.version()
                || coordinator.snapshot().stream_lifecycle(key.identity) != Some(&current_lifecycle)
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("stream_quiesce_start:{table_key}"),
                    Some(live.version().to_string()),
                    Some(coordinator.snapshot().version().to_string()),
                ));
            }
            coordinator
                .commit_operational_changes_with_expected(
                    &[ManifestChange::SetStreamLifecycle {
                        expected: Some(current_lifecycle),
                        next: started.lifecycle,
                    }],
                    &expected_versions,
                )
                .await?;
            drop(_table_guards);
            drop(_stream_token_guard);
            drop(_branch_guard);
            drop(_schema_guard);
            self.refresh_coordinator_only().await?;
        } else if current_lifecycle.lifecycle == StreamLifecycle::Draining {
            let drain = current_lifecycle.drain.as_ref().ok_or_else(|| {
                OmniError::manifest_internal("DRAINING stream has no drain descriptor")
            })?;
            if drain.operation_request_digest != expected_request_digest
                || drain.drain_id != drain_id
                || drain.operation_expected_revision != expected_lifecycle_revision
                || drain.initiating_actor != actor_id
                || drain.goal != DrainGoal::Sealed
            {
                return Err(OmniError::StreamLifecycleIdempotencyConflict {
                    stable_table_id: key.identity.stable_table_id,
                    table_incarnation_id: key.identity.table_incarnation_id,
                    operation_kind: "QUIESCE".to_string(),
                    operation_id: drain_id,
                });
            }
            // Recompute the immutable request commitment after exclusive
            // admission. The separate mutable target may have advanced with
            // prior claims, but the requested target remains the exact
            // original digest preimage.
            stream_quiesce_request_payload_from_draining(
                &current_lifecycle,
                &graph_identity_digest,
            )?;
            if let Some(block) = current_lifecycle.strict_block.as_ref() {
                return Err(stream_data_block_error(&block.block_token));
            }
        } else {
            // A terminal exact retry can only reach SEALED through the
            // receipt-first branches above. Reaching it without the immutable
            // receipt is an authority change, not a DRAINING descriptor error.
            return Err(OmniError::StreamLifecycleChanged {
                stable_table_id: key.identity.stable_table_id,
                table_incarnation_id: key.identity.table_incarnation_id,
                expected_revision: expected_lifecycle_revision,
                current_revision: current_lifecycle.lifecycle_revision,
            });
        }

        let draining = self
            .capture_draining_stream_authority(&table_key, "stream quiesce cut", &drain_id)
            .await?;
        ensure_same_binding(key, &draining, "stream quiesce binding")?;
        self.ensure_no_relevant_stream_sidecar_except_exact_claim(&draining, "stream quiesce cut")
            .await?;

        let opener_db = Arc::clone(&self);
        let opener_capture = draining.clone();
        let opener_actor = actor_id.clone();
        let opener = move || {
            let opener_db = Arc::clone(&opener_db);
            let opener_capture = opener_capture.clone();
            let opener_actor = opener_actor.clone();
            Box::pin(async move {
                Box::pin(opener_db.open_stream_writer_with_claim(
                    &opener_capture,
                    "QUIESCE",
                    Some(opener_actor),
                ))
                .await
            }) as crate::table_store::mem_wal::WorkerOpenFuture
        };

        let selected_claim = draining.lifecycle.current_claim_receipt_id.as_deref();
        let cut = if selected_claim.is_some() {
            let receipt = self
                .selected_claim_receipt(&draining.txn.base, &draining.lifecycle)
                .await?;
            if receipt.lifecycle_operation_id.as_deref() == Some(drain_id.as_str()) {
                match self
                    .stream_workers
                    .passive_quiesce_cut(
                        key,
                        exclusive_authority,
                        draining.head.dataset().clone(),
                        draining.lifecycle.clone(),
                        draining.retained_shard_inventory.clone(),
                        receipt,
                    )
                    .await
                    .map_err(|error| fold_cut_error(key, error))?
                {
                    PassiveQuiesceDisposition::Reusable(cut) => cut,
                    PassiveQuiesceDisposition::FreshClaimRequired(authority) => self
                        .stream_workers
                        .quiesce_cut(key, table_key.clone(), authority, Box::new(opener))
                        .await
                        .map_err(|error| fold_cut_error(key, error))?,
                }
            } else {
                self.stream_workers
                    .quiesce_cut(
                        key,
                        table_key.clone(),
                        exclusive_authority,
                        Box::new(opener),
                    )
                    .await
                    .map_err(|error| fold_cut_error(key, error))?
            }
        } else {
            self.stream_workers
                .quiesce_cut(
                    key,
                    table_key.clone(),
                    exclusive_authority,
                    Box::new(opener),
                )
                .await
                .map_err(|error| fold_cut_error(key, error))?
        };

        let post_claim = self
            .capture_draining_stream_authority(&table_key, "stream quiesce post-claim", &drain_id)
            .await?;
        ensure_same_binding(key, &post_claim, "stream quiesce post-claim binding")?;
        if let QuiesceCut::Generation(generation) = &cut {
            crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR,
            )?;
            let mode = FoldLifecycleMode::Draining {
                drain_id: drain_id.clone(),
            };
            let mut last_effect_free_error = None;
            for attempt in 0..B1_MAX_FOLD_ATTEMPTS {
                match Box::pin(self.stream_fold_attempt(
                    &table_key,
                    key,
                    &post_claim,
                    generation,
                    &mode,
                ))
                .await?
                {
                    FoldAttempt::Published => {
                        last_effect_free_error = None;
                        break;
                    }
                    FoldAttempt::EffectFree(error) if attempt + 1 < B1_MAX_FOLD_ATTEMPTS => {
                        last_effect_free_error = Some(error);
                    }
                    FoldAttempt::EffectFree(error) => return Err(error),
                }
            }
            if let Some(error) = last_effect_free_error {
                return Err(error);
            }
        }

        // Keep `cut` alive through terminal ledger + lifecycle publication: it
        // owns the same exclusive admission lease acquired before DRAINING.
        self.finalize_stream_quiesce(
            &table_key,
            key,
            &drain_id,
            expected_lifecycle_revision,
            &actor_id,
            &graph_identity_digest,
        )
        .await?;
        drop(cut);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_stream_quiesce(
        &self,
        table_key: &str,
        key: StreamWorkerKey,
        drain_id: &str,
        expected_lifecycle_revision: u64,
        actor_id: &str,
        graph_identity_digest: &str,
    ) -> Result<()> {
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;
        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream quiesce finalize")
            .await?;

        let capture = self
            .capture_draining_stream_authority(table_key, "stream quiesce finalize", drain_id)
            .await?;
        ensure_same_binding(key, &capture, "stream quiesce terminal binding")?;
        let draining = capture.lifecycle.clone();
        let drain = draining.drain.as_ref().ok_or_else(|| {
            OmniError::manifest_internal("terminal stream quiesce has no drain descriptor")
        })?;
        if drain.operation_expected_revision != expected_lifecycle_revision
            || drain.initiating_actor != actor_id
            || drain.goal != DrainGoal::Sealed
        {
            return Err(OmniError::StreamLifecycleIdempotencyConflict {
                stable_table_id: key.identity.stable_table_id,
                table_incarnation_id: key.identity.table_incarnation_id,
                operation_kind: "QUIESCE".to_string(),
                operation_id: drain_id.to_string(),
            });
        }
        let current_claim_receipt = self
            .selected_claim_receipt(&capture.txn.base, &draining)
            .await?;
        let physical = validate_b1_lifecycle_physical_state_with_binding_inventory(
            capture.head.dataset(),
            &draining,
            capture.retained_shard_inventory.as_ref(),
        )
        .await
        .map_err(worker_error)?;
        let (
            shard_manifest_version,
            current_generation,
            replay_after_wal_entry_position,
            writer_epoch,
        ) = match physical {
            PassiveB1PhysicalState::AdmitOrReplay {
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                writer_epoch,
            } => (
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                writer_epoch,
            ),
            PassiveB1PhysicalState::FoldOnlyFlushed(flushed) => {
                return Err(OmniError::recovery_required(
                    format!("stream-drain:{drain_id}"),
                    format!("terminal quiesce still has an unmerged generation: {flushed:?}"),
                ));
            }
        };
        let details = capture
            .head
            .dataset()
            .mem_wal_index_details()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal("terminal quiesce lost its MemWAL index")
            })?;
        let base_merged_generation =
            exact_merged_generation(&details, key.shard_id)?.map_or(0, |merged| merged.generation);
        let evidence = EmptyCutEvidence {
            shard_manifest_version,
            writer_epoch,
            replay_cursor: replay_after_wal_entry_position,
            current_generation,
            base_merged_generation,
        };

        let request_payload =
            stream_quiesce_request_payload_from_draining(&draining, graph_identity_digest)?;
        let next_revision = draining
            .lifecycle_revision
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream lifecycle revision overflow"))?;
        let result_payload = stream_quiesce_result_payload(next_revision)?;
        let receipt = ManagementReceipt::new(
            graph_identity_digest.to_string(),
            key.identity,
            draining.enrollment_receipt.stream_incarnation_id.clone(),
            draining.binding_scope_id.clone(),
            &draining.management_receipt_chain,
            drain_id.to_string(),
            "QUIESCE",
            expected_lifecycle_revision,
            next_revision,
            actor_id.to_string(),
            request_payload,
            result_payload,
            crate::db::now_micros()?,
        )?;
        let next_lifecycle =
            build_draining_to_sealed(&draining, &receipt, &current_claim_receipt, evidence)?;

        let token_dataset = capture.txn.base.open_stream_token_authority().await?;
        let staged = stage_management_receipt(
            token_dataset,
            capture.txn.base.stream_token_authority(),
            &receipt,
        )
        .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                capture.txn.base.stream_token_authority(),
                &crate::lance_access::control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);
        let authority = RecoveryAuthorityToken {
            branch_identifier: capture.txn.authority.branch_identifier.clone(),
            graph_head: capture.txn.authority.graph_head.clone(),
            schema_identity_domain: capture.txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: capture.txn.authority.schema_ir_hash.clone(),
            schema_identity_version: capture.txn.authority.schema_identity_version,
        };
        let mut sidecar = new_stream_lifecycle_receipt_sidecar_v14(
            actor_id.to_string(),
            authority,
            capture.txn.base.version(),
            RecoveryStreamLifecycleReceiptKind::QuiesceFinalize,
            capture.txn.base.stream_profile().clone(),
            draining,
            Some(current_claim_receipt),
            next_lifecycle.clone(),
            capture.txn.base.stream_token_authority().clone(),
            receipt.clone(),
            planned_transaction,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_LIFECYCLE_RECEIPT_POST_SIDECAR_PRE_TOKEN_COMMIT,
        )?;
        let outcome = match self.storage().commit_staged_exact(token_head, staged).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let recovered = complete_stream_lifecycle_receipt_sidecar_v14(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &capture.txn.base,
                    &sidecar,
                )
                .await;
                match recovered {
                    Ok(()) => {
                        self.refresh_coordinator_only().await?;
                        let terminal = self.open_write_txn(None).await?;
                        if terminal.base.stream_lifecycle(key.identity) == Some(&next_lifecycle) {
                            let selected = terminal.base.open_stream_token_authority().await?;
                            let selected_receipt = lookup_management_receipt(
                                &selected,
                                terminal.base.stream_token_authority(),
                                graph_identity_digest,
                                key.identity,
                                &next_lifecycle.enrollment_receipt.stream_incarnation_id,
                                "QUIESCE",
                                drain_id,
                            )
                            .await?;
                            if selected_receipt.as_ref() == Some(&receipt) {
                                return Ok(());
                            }
                        }
                        // Recovery proved the staged token transaction
                        // effect-free and retired its intent. That is a safe
                        // retry outcome, not a successful quiesce.
                        return Err(error);
                    }
                    Err(recovery_error) => {
                        return Err(OmniError::recovery_required(
                            handle.operation_id,
                            format!(
                                "quiesce receipt commit failed ({error}) and exact recovery did not complete: {recovery_error}"
                            ),
                        ));
                    }
                }
            }
        };
        if !outcome.is_exact() {
            return Err(OmniError::recovery_required(
                handle.operation_id,
                "quiesce receipt participant committed a non-exact transaction",
            ));
        }
        let next_token_authority =
            stream_token_authority_entry_for_dataset(outcome.snapshot().dataset())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
        confirm_stream_lifecycle_receipt_sidecar_v14(
            self.root_uri(),
            self.storage_adapter(),
            &mut sidecar,
            outcome.committed_transaction().clone(),
            next_token_authority.current_head_witness.clone(),
            next_token_authority,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("quiesce receipt confirmation requires recovery: {error}"),
            )
        })?;
        complete_stream_lifecycle_receipt_sidecar_v14(
            self.root_uri(),
            Arc::clone(&self.storage),
            &capture.txn.base,
            &sidecar,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id,
                format!("quiesce receipt publication requires recovery: {error}"),
            )
        })?;
        self.refresh_coordinator_only().await?;
        let terminal = self.open_write_txn(None).await?;
        if terminal.base.stream_lifecycle(key.identity) != Some(&next_lifecycle) {
            return Err(OmniError::manifest_internal(
                "completed quiesce receipt did not install its exact SEALED lifecycle",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_stream_data_block(
        &self,
        table_key: &str,
        key: StreamWorkerKey,
        prepared: &StreamAuthorityCapture,
        cut: &SealedGenerationCut,
        drain_id: &str,
        changeset: &ChangeSet,
        attribution: &AttributedFoldPlan,
        planned_evidence: CanonicalDataBlockEvidence,
    ) -> Result<String> {
        let generation_cut = StreamGenerationCut {
            shard_id: key.shard_id.to_string(),
            writer_epoch: cut.writer_epoch,
            shard_manifest_version: cut.shard_manifest_version,
            replay_after_wal_entry_position: cut.replay_after_wal_entry_position,
            generation: cut.generation,
            generation_path: cut.path.clone(),
        };
        let winner_tokens = attribution
            .token_rows
            .iter()
            .map(|row| (row.logical_id.clone(), row.current_token.to_string()))
            .collect::<BTreeMap<_, _>>();
        let batches = changeset
            .get(table_key)
            .ok_or_else(|| OmniError::manifest_internal("stream data block lost its table change"))?
            .changed
            .as_slice();
        let (input_rows, input_bytes) = fold_output_size(batches)?;
        let recorded_at = crate::db::now_micros()?;
        let planned = build_draining_data_block(
            &prepared.lifecycle,
            generation_cut.clone(),
            planned_evidence.clone(),
            input_rows,
            input_bytes,
            recorded_at,
        )?;
        if planned.drain.as_ref().map(|drain| drain.drain_id.as_str()) != Some(drain_id) {
            return Err(OmniError::manifest_internal(
                "stream data block differs from the active drain",
            ));
        }

        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;
        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream strict data block")
            .await?;
        self.ensure_no_relevant_stream_sidecar(key.identity, "stream strict data block")
            .await?;

        let live = self.revalidate_write_txn(&prepared.txn).await?;
        let live_entry = live.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_data_block_table:{table_key}"),
                Some(prepared.entry.identity.to_string()),
                None,
            )
        })?;
        let live_lifecycle = live
            .stream_lifecycle(key.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_data_block_lifecycle:{table_key}"),
                    Some(format!("{:?}", prepared.lifecycle)),
                    None,
                )
            })?;
        ensure_live_stream_prestate(prepared, &live_entry, &live_lifecycle)?;

        let final_head = self
            .storage()
            .open_dataset_head(&prepared.full_path, None)
            .await?;
        self.ensure_existing_effect_baseline(
            table_key,
            None,
            prepared.entry.table_version,
            &final_head,
        )
        .await?;
        let final_witness = capture_current_head_witness(final_head.dataset())
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if final_witness != prepared.lifecycle.current_head_witness {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_data_block_head:{table_key}"),
                Some(format!("{:?}", prepared.lifecycle.current_head_witness)),
                Some(format!("{final_witness:?}")),
            ));
        }
        match validate_b1_lifecycle_physical_state_with_binding_inventory(
            final_head.dataset(),
            &live_lifecycle,
            prepared.retained_shard_inventory.as_ref(),
        )
        .await
        .map_err(worker_error)?
        {
            PassiveB1PhysicalState::FoldOnlyFlushed(flushed)
                if flushed.shard_manifest_version >= cut.shard_manifest_version
                    && flushed.writer_epoch == cut.writer_epoch
                    && flushed.generation == cut.generation
                    && flushed.path == cut.path
                    && flushed.replay_after_wal_entry_position
                        == cut.replay_after_wal_entry_position => {}
            observed => {
                return Err(OmniError::recovery_required(
                    format!("stream-data-block:{drain_id}"),
                    format!(
                        "strict-block publication lost its exact authenticated generation cut: expected=key={},writer_epoch={},shard_manifest_version={},generation={},path={},replay_cursor={}, observed={observed:?}",
                        cut.key,
                        cut.writer_epoch,
                        cut.shard_manifest_version,
                        cut.generation,
                        cut.path,
                        cut.replay_after_wal_entry_position,
                    ),
                ));
            }
        }

        let committed = CommittedState::write(&live, self, None);
        let constraints = crate::validate::constraints_for(&prepared.txn.catalog);
        let mut collector = DataBlockEvidenceCollector::new(table_key, &winner_tokens);
        crate::validate::evaluate_with_sink(
            &constraints,
            changeset,
            &committed,
            &prepared.txn.catalog,
            |violation| collector.push(&violation),
        )
        .await?;
        let Some(exact_evidence) = collector.finish()? else {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_data_block_validation:{table_key}"),
                Some("permanent validator violation".to_string()),
                Some("no violation".to_string()),
            ));
        };
        if exact_evidence != planned_evidence {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_data_block_evidence:{table_key}"),
                Some("prepared canonical violation evidence".to_string()),
                Some("changed canonical violation evidence".to_string()),
            ));
        }
        let exact = build_draining_data_block(
            &live_lifecycle,
            generation_cut,
            exact_evidence,
            input_rows,
            input_bytes,
            recorded_at,
        )?;
        if exact != planned {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_data_block_evidence:{table_key}"),
                Some("prepared canonical violation evidence".to_string()),
                Some("changed canonical violation evidence".to_string()),
            ));
        }

        let mut expected_versions = ExpectedTableVersions::new();
        expected_versions.insert(
            key.identity,
            TableVersionExpectation {
                table_key: live_entry.table_key.clone(),
                table_version: live_entry.table_version,
            },
        );
        let mut coordinator = self.open_coordinator_for_branch(None).await?;
        if coordinator.snapshot().version() != live.version()
            || coordinator.snapshot().stream_lifecycle(key.identity) != Some(&live_lifecycle)
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_data_block_publish:{table_key}"),
                Some(live.version().to_string()),
                Some(coordinator.snapshot().version().to_string()),
            ));
        }
        coordinator
            .commit_operational_changes_with_expected(
                &[ManifestChange::SetStreamLifecycle {
                    expected: Some(live_lifecycle),
                    next: exact.clone(),
                }],
                &expected_versions,
            )
            .await?;
        self.refresh_coordinator_only().await?;
        let selected = self.open_write_txn(None).await?;
        if selected.base.stream_lifecycle(key.identity) != Some(&exact) {
            return Err(OmniError::manifest_internal(
                "strict-block publication did not select its exact lifecycle row",
            ));
        }
        Ok(exact
            .strict_block
            .as_ref()
            .expect("strict-block builder installs one block")
            .block_token
            .clone())
    }

    async fn stream_fold_attempt(
        &self,
        table_key: &str,
        key: StreamWorkerKey,
        post_claim: &StreamAuthorityCapture,
        cut: &SealedGenerationCut,
        mode: &FoldLifecycleMode,
    ) -> Result<FoldAttempt> {
        let operation = match mode {
            FoldLifecycleMode::Open => "stream fold",
            FoldLifecycleMode::Draining { .. } => "stream drain fold",
        };
        self.ensure_no_relevant_stream_sidecar(key.identity, operation)
            .await?;
        let prepared = match mode {
            FoldLifecycleMode::Open => self.capture_stream_authority(table_key, operation).await?,
            FoldLifecycleMode::Draining { drain_id } => {
                self.capture_draining_stream_authority(table_key, operation, drain_id)
                    .await?
            }
        };
        ensure_same_capture(post_claim, &prepared, "stream fold post-drain authority")?;
        if cut.key != key || cut.writer_epoch != prepared.epoch_floor {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_fold_cut:{table_key}"),
                Some(format!("{key}:epoch={}", prepared.epoch_floor)),
                Some(format!("{}:epoch={}", cut.key, cut.writer_epoch)),
            ));
        }

        let batches = scan_fresh_generation(&prepared, cut).await?;
        validate_fold_output_bounds(table_key, &batches)?;
        let recomputed_drain_lww = match mode {
            FoldLifecycleMode::Open => None,
            FoldLifecycleMode::Draining { .. } => {
                let stored_batches = batches
                    .iter()
                    .map(|batch| {
                        let mut columns = batch.columns().to_vec();
                        columns.push(Arc::new(BooleanArray::from(vec![false; batch.num_rows()]))
                            as arrow_array::ArrayRef);
                        RecordBatch::try_new(
                            lance::dataset::mem_wal::schema_with_tombstone(batch.schema().as_ref()),
                            columns,
                        )
                        .map_err(|error| OmniError::Lance(error.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Some(lifecycle_generation_lww_projection_digest(
                    &prepared.lifecycle,
                    &prepared.txn.authority.schema_ir_hash,
                    Arc::new(ArrowSchema::from(prepared.head.dataset().schema())),
                    &stored_batches,
                )?)
            }
        };
        let attribution = plan_fold_attribution(
            &prepared.txn.base,
            key.identity,
            &prepared.lifecycle,
            &prepared.binding,
            &batches,
        )
        .await?;
        // A strict validator terminal returns before token staging, so it must
        // independently prove that the complete winner projection still fits
        // the same bounded authority envelope enforced by admission and the
        // successful-fold path.
        validate_generation_token_plan(table_key, &attribution.token_rows)?;
        let mut changeset = ChangeSet::new();
        changeset.insert(
            table_key.to_string(),
            TableChange {
                added: Vec::new(),
                changed: batches.clone(),
                deleted_ids: Vec::new(),
            },
        );
        let committed = CommittedState::write(&prepared.txn.base, self, None);
        let constraints = crate::validate::constraints_for(&prepared.txn.catalog);
        match mode {
            FoldLifecycleMode::Open => {
                let mut first_violation = None;
                crate::validate::evaluate_with_sink(
                    &constraints,
                    &changeset,
                    &committed,
                    &prepared.txn.catalog,
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
            }
            FoldLifecycleMode::Draining { drain_id } => {
                let winner_tokens = attribution
                    .token_rows
                    .iter()
                    .map(|row| (row.logical_id.clone(), row.current_token.to_string()))
                    .collect::<BTreeMap<_, _>>();
                let mut collector = DataBlockEvidenceCollector::new(table_key, &winner_tokens);
                crate::validate::evaluate_with_sink(
                    &constraints,
                    &changeset,
                    &committed,
                    &prepared.txn.catalog,
                    |violation| collector.push(&violation),
                )
                .await?;
                if let Some(evidence) = collector.finish()? {
                    let block_token = self
                        .publish_stream_data_block(
                            table_key,
                            key,
                            &prepared,
                            cut,
                            drain_id,
                            &changeset,
                            &attribution,
                            evidence,
                        )
                        .await?;
                    return Err(stream_data_block_error(&block_token));
                }
            }
        }

        // Staging may materialize URI-backed blobs.  Its own post-materialized
        // bound is the final 32-MiB proof; no HEAD moves here.
        let staged = self
            .storage()
            .stage_stream_fold(
                prepared.head.clone(),
                table_key,
                batches.clone(),
                cut.key.shard_id,
                cut.generation,
            )
            .await?;
        let planned_transaction = staged.transaction_identity();
        let token_dataset = prepared.txn.base.open_stream_token_authority().await?;
        let token_staged = stage_stream_token_upsert(
            token_dataset.clone(),
            prepared.txn.base.stream_token_authority(),
            &attribution.token_rows,
        )
        .await?;
        let token_stage = (
            token_staged.transaction_identity(),
            crate::storage_layer::StagedHandle::new(token_staged),
        );

        // Admission remains exclusively held inside `cut`.  Enter the normal
        // RFC-022 inner order only for final authority, sidecar arm, effect, and
        // graph publication.
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(table_key.to_string(), None)])
            .await;

        // The pre-preparation recovery barrier ran, but another main-branch
        // writer may have armed recovery while this fold was scanning and
        // staging.  Re-list under the complete graph-write gate envelope so an
        // unresolved effect on a different table cannot be bypassed by this
        // fold's graph-head publication.  Recovery itself takes these gates,
        // so this is deliberately a refusal-only barrier.
        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream fold")
            .await?;
        self.ensure_no_relevant_stream_sidecar(key.identity, "stream fold")
            .await?;
        let live = self.revalidate_write_txn(&prepared.txn).await?;
        let live_entry = live.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_fold_table:{table_key}"),
                Some(prepared.entry.identity.to_string()),
                None,
            )
        })?;
        let live_lifecycle = live
            .stream_lifecycle(key.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_fold_lifecycle:{table_key}"),
                    Some(format!("{:?}", prepared.lifecycle)),
                    None,
                )
            })?;
        ensure_live_stream_prestate(&prepared, &live_entry, &live_lifecycle)?;
        if live.stream_token_authority() != prepared.txn.base.stream_token_authority() {
            return Err(OmniError::manifest_read_set_changed(
                "stream_fold_token_authority",
                Some(format!("{:?}", prepared.txn.base.stream_token_authority())),
                Some(format!("{:?}", live.stream_token_authority())),
            ));
        }
        let current_claim_receipt = self.selected_claim_receipt(&live, &live_lifecycle).await?;
        let revalidated = plan_fold_attribution(
            &live,
            key.identity,
            &live_lifecycle,
            &prepared.binding,
            &batches,
        )
        .await?;
        if revalidated != attribution {
            return Err(OmniError::manifest_read_set_changed(
                "stream_fold_attribution",
                Some("prepared attributed winner set".to_string()),
                Some("changed attributed winner set".to_string()),
            ));
        }

        let final_head = self
            .storage()
            .open_dataset_head(&prepared.full_path, None)
            .await?;
        self.ensure_existing_effect_baseline(
            table_key,
            None,
            prepared.entry.table_version,
            &final_head,
        )
        .await?;
        let final_witness = capture_current_head_witness(final_head.dataset())
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if final_witness != prepared.lifecycle.current_head_witness {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_fold_head:{table_key}"),
                Some(format!("{:?}", prepared.lifecycle.current_head_witness)),
                Some(format!("{final_witness:?}")),
            ));
        }
        let final_details = final_head
            .dataset()
            .mem_wal_index_details()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| OmniError::manifest_internal("stream fold lost its MemWAL index"))?;
        let (enrollment_id, shard_id) =
            validate_stream_config_v3_binding(&final_details, &prepared.binding)
                .map_err(worker_error)?;
        if enrollment_id != key.enrollment_id || shard_id != key.shard_id {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_fold_binding:{table_key}"),
                Some(key.to_string()),
                Some(format!("{}:{}:{}", key.identity, enrollment_id, shard_id)),
            ));
        }
        let prior_merged = exact_merged_generation(&final_details, key.shard_id)?;

        let lineage = self
            .new_lineage_intent_for_branch(None, Some("omnigraph:stream-fold"))
            .await?;
        let authority = RecoveryAuthorityToken {
            branch_identifier: prepared.txn.authority.branch_identifier.clone(),
            graph_head: prepared.txn.authority.graph_head.clone(),
            schema_identity_domain: prepared.txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: prepared.txn.authority.schema_ir_hash.clone(),
            schema_identity_version: prepared.txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id.clone(),
            branch: lineage.branch.clone(),
            actor_id: lineage.actor_id.clone(),
            merged_parent_commit_id: lineage.merged_parent_commit_id.clone(),
            created_at: lineage.created_at,
        };
        let post_commit_pin =
            prepared.entry.table_version.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal("stream fold table version overflow")
            })?;
        let pin = SidecarTablePin {
            identity: key.identity,
            table_key: table_key.to_string(),
            table_path: prepared.full_path.clone(),
            expected_version: prepared.entry.table_version,
            post_commit_pin,
            confirmed_version: None,
            table_branch: None,
        };
        let generation_cut = RecoveryStreamFoldCut {
            shard_id: key.shard_id,
            writer_epoch: cut.writer_epoch,
            shard_manifest_version: cut.shard_manifest_version,
            replay_after_wal_entry_position: cut.replay_after_wal_entry_position,
            generation: cut.generation,
            generation_path: cut.path.clone(),
        };

        Box::pin(async move {
            let (token_planned_transaction, token_staged) = token_stage;
            let token_head = SnapshotHandle::new(
                open_stream_token_authority_head(
                    self.root_uri(),
                    live.stream_token_authority(),
                    &crate::lance_access::control_session(),
                )
                .await?,
            );
            let mut next_lifecycle = prepared.lifecycle.clone();
            let next_head_witness = CurrentHeadWitness {
                branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
                table_version: post_commit_pin,
                transaction_uuid: planned_transaction.uuid.clone(),
                manifest_e_tag: None,
            };
            next_lifecycle.current_head_witness = next_head_witness.clone();
            if let Some(drain) = next_lifecycle.drain.as_mut() {
                // The active descriptor duplicates the mutable current HEAD
                // for restart. Its immutable operation-request payload keeps
                // the original pre-fold witness byte-for-byte.
                drain.expected_current_head_witness = next_head_witness;
            }
            next_lifecycle
                .epoch_floor_by_shard
                .insert(key.shard_id.to_string(), cut.writer_epoch);
            next_lifecycle.lifecycle_revision = next_lifecycle
                .lifecycle_revision
                .checked_add(1)
                .ok_or_else(|| {
                    OmniError::manifest_internal("stream lifecycle revision overflow")
                })?;
            let (fold_rows, fold_bytes) = fold_output_size(&batches)?;
            next_lifecycle.last_fold_summary = Some(LastFoldSummary {
                operation_id: match mode {
                    FoldLifecycleMode::Open => "pending-stream-fold-operation".to_string(),
                    FoldLifecycleMode::Draining { drain_id } => drain_id.clone(),
                },
                graph_commit_id: Some(lineage.graph_commit_id.clone()),
                exact_generation_cut: StreamGenerationCut {
                    shard_id: key.shard_id.to_string(),
                    writer_epoch: cut.writer_epoch,
                    shard_manifest_version: cut.shard_manifest_version,
                    replay_after_wal_entry_position: cut.replay_after_wal_entry_position,
                    generation: cut.generation,
                    generation_path: cut.path.clone(),
                },
                outcome: LastFoldOutcome::Published,
                input_rows: fold_rows,
                input_bytes: fold_bytes,
                visible_rows: fold_rows,
                visible_bytes: fold_bytes,
                recorded_at: lineage.created_at,
            });
            let mut sidecar = match mode {
                FoldLifecycleMode::Open => new_stream_fold_v2_sidecar_v14(
                    pin,
                    authority,
                    recovery_lineage,
                    live.version(),
                    live.stream_profile().clone(),
                    prepared.lifecycle.clone(),
                    current_claim_receipt,
                    next_lifecycle,
                    prior_merged,
                    generation_cut,
                    planned_transaction,
                    prepared.txn.base.stream_token_authority().clone(),
                    token_planned_transaction,
                    attribution.token_rows.clone(),
                    attribution.summary.clone(),
                )?,
                FoldLifecycleMode::Draining { drain_id } => {
                    new_stream_drain_fold_sidecar_v14(
                        pin,
                        authority,
                        recovery_lineage,
                        live.version(),
                        live.stream_profile().clone(),
                        prepared.lifecycle.clone(),
                        drain_id.clone(),
                        current_claim_receipt,
                        recomputed_drain_lww.clone().ok_or_else(|| {
                            OmniError::manifest_internal(
                                "drain fold omitted its recomputed LWW projection",
                            )
                        })?,
                        next_lifecycle,
                        prior_merged,
                        generation_cut,
                        planned_transaction,
                        prepared.txn.base.stream_token_authority().clone(),
                        token_planned_transaction,
                        attribution.token_rows.clone(),
                        attribution.summary.clone(),
                    )?
                }
            };
            let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

            // The armed-but-no-effect cell: the intent is durable while both
            // exact Lance participants are still untouched.  Recovery must
            // retire it effect-free rather than publish or adopt anything.
            crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_FOLD_POST_SIDECAR_PRE_BASE_COMMIT,
            )
            .map_err(|error| {
                OmniError::recovery_required(
                    handle.operation_id.clone(),
                    format!("stream fold stopped after arming its recovery intent: {error}"),
                )
            })?;

            let base_outcome = match self.storage().commit_staged_exact(final_head, staged).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    if error.is_retryable_commit_conflict() {
                        let effect_free = finalize_effect_free_stream_fold_sidecar_v14(
                            self.root_uri(),
                            &self.storage,
                            &live,
                            &sidecar,
                        )
                        .await
                        .map_err(|classification_error| {
                            OmniError::recovery_required(
                                handle.operation_id.clone(),
                                format!(
                                    "stream fold base commit failed ({error}); exact two-participant effect-free classification failed: {classification_error}"
                                ),
                            )
                        })?;
                        if effect_free {
                            return Ok(FoldAttempt::EffectFree(error));
                        }
                    }
                    return Err(OmniError::recovery_required(
                        handle.operation_id,
                        format!("stream fold base commit requires recovery: {error}"),
                    ));
                }
            };
            if !base_outcome.is_exact() {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "stream fold base participant committed a non-exact transaction",
                ));
            }
            let base_state = self
                .storage()
                .table_state(&prepared.full_path, base_outcome.snapshot())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
            // Build the fixed manifest metadata from the coordinator's
            // canonical root/table pair. TableStore may retain the caller's
            // symlinked local root (`/var` vs `/private/var` on macOS), which
            // would make the confirmation differ after recovery reopens the
            // same manifest through the canonical root.
            let base_version_metadata = crate::db::manifest::TableVersionMetadata::from_dataset(
                self.root_uri(),
                &prepared.entry.table_path,
                base_outcome.snapshot().dataset(),
            )
            .map_err(|error| {
                OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
            })?;

            crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT,
            )
            .map_err(|error| {
                OmniError::recovery_required(
                    handle.operation_id.clone(),
                    format!("stream fold stopped after its exact base effect: {error}"),
                )
            })?;

            let token_outcome = match self
                .storage()
                .commit_staged_exact(token_head, token_staged)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let recovered = complete_stream_fold_sidecar_v14(
                        self.root_uri(),
                        Arc::clone(&self.storage),
                        &live,
                        &sidecar,
                    )
                    .await;
                    if recovered.is_ok() {
                        self.refresh_coordinator_only().await?;
                        return Ok(FoldAttempt::Published);
                    }
                    return Err(OmniError::recovery_required(
                        handle.operation_id,
                        format!(
                            "stream fold token commit failed ({error}) and synchronous recovery did not complete: {}",
                            recovered.expect_err("checked as error")
                        ),
                    ));
                }
            };
            if !token_outcome.is_exact() {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "stream fold token participant committed a non-exact transaction",
                ));
            }

            crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM,
            )
            .map_err(|error| {
                OmniError::recovery_required(
                    handle.operation_id.clone(),
                    format!("stream fold stopped after both exact effects: {error}"),
                )
            })?;

            let achieved_base_head =
                capture_current_head_witness(base_outcome.snapshot().dataset())
                    .await
                    .map_err(|error| {
                        OmniError::recovery_required(
                            handle.operation_id.clone(),
                            error.to_string(),
                        )
                    })?;
            let next_token_authority = stream_token_authority_entry_for_dataset(
                token_outcome.snapshot().dataset(),
            )
            .await
            .map_err(|error| {
                OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
            })?;
            let achieved_token_head = next_token_authority.current_head_witness.clone();
            confirm_stream_fold_sidecar_v14(
                self.root_uri(),
                self.storage_adapter(),
                &mut sidecar,
                base_outcome.committed_transaction().clone(),
                MergedGeneration::new(key.shard_id, cut.generation),
                achieved_base_head,
                crate::db::SubTableUpdate {
                    identity: key.identity,
                    table_key: table_key.to_string(),
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
                    format!("stream fold confirmation requires recovery: {error}"),
                )
            })?;
            complete_stream_fold_sidecar_v14(
                self.root_uri(),
                Arc::clone(&self.storage),
                &live,
                &sidecar,
            )
            .await
            .map_err(|error| {
                OmniError::recovery_required(
                    handle.operation_id,
                    format!("stream fold completion requires recovery: {error}"),
                )
            })?;
            self.refresh_coordinator_only().await?;
            Ok(FoldAttempt::Published)
        })
        .await
    }

    /// Resume one exact closed lifecycle lane through recovery-v15.
    ///
    /// The detached owner retains the profile-shared gate even if the caller
    /// is cancelled. The worker registry then owns the exclusive lane from
    /// physical claim through terminal `OPEN` publication and writer install.
    pub(crate) async fn stream_resume_as(
        self: &Arc<Self>,
        table_key: &str,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: &str,
    ) -> Result<()> {
        let db = Arc::clone(self);
        let table_key = table_key.to_string();
        let resume_id = resume_id.to_string();
        let actor_id = actor_id.to_string();
        crate::instrumentation::spawn_with_query_io_probes(async move {
            Box::pin(db.stream_resume_background(
                table_key,
                resume_id,
                expected_lifecycle_revision,
                mode,
                actor_id,
            ))
            .await
        })
        .await
        .map_err(|error| OmniError::Lance(format!("stream resume task failed: {error}")))?
    }

    async fn stream_resume_background(
        self: Arc<Self>,
        table_key: String,
        resume_id: String,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: String,
    ) -> Result<()> {
        let _profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        let initial = self.open_write_txn(None).await?;
        let entry = initial.base.entry(&table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "stream resume cannot resolve unknown table '{table_key}'"
            ))
        })?;
        let lifecycle = initial
            .base
            .stream_lifecycle(entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "stream resume requires an enrolled stream for '{table_key}'"
                ))
            })?;
        let graph_identity_digest =
            stream_graph_identity_digest(&initial.authority.schema_identity_domain)?;
        if self
            .selected_resume_receipt_matches(
                &initial.base,
                &lifecycle,
                &graph_identity_digest,
                &resume_id,
                expected_lifecycle_revision,
                mode,
                &actor_id,
            )
            .await?
        {
            self.complete_selected_resume_sidecar(&initial.base, &lifecycle, &resume_id)
                .await?;
            return Ok(());
        }
        if lifecycle.lifecycle_revision != expected_lifecycle_revision {
            return Err(OmniError::StreamLifecycleChanged {
                stable_table_id: entry.identity.stable_table_id,
                table_incarnation_id: entry.identity.table_incarnation_id,
                expected_revision: expected_lifecycle_revision,
                current_revision: lifecycle.lifecycle_revision,
            });
        }
        let provisional = match mode {
            StreamResumeMode::ResumeSealed if lifecycle.lifecycle == StreamLifecycle::Sealed => {
                self.capture_sealed_stream_authority(&table_key, "stream resume")
                    .await?
            }
            StreamResumeMode::AbortDrain if lifecycle.lifecycle == StreamLifecycle::Draining => {
                let drain_id = lifecycle
                    .drain
                    .as_ref()
                    .ok_or_else(|| {
                        OmniError::manifest_internal("stream abort-drain lost its drain descriptor")
                    })?
                    .drain_id
                    .clone();
                self.capture_draining_stream_authority(&table_key, "stream abort-drain", &drain_id)
                    .await?
            }
            _ => {
                return Err(OmniError::manifest_stream_lifecycle_conflict(
                    entry.identity.stable_table_id,
                    entry.identity.table_incarnation_id,
                    &table_key,
                    lifecycle.lifecycle.as_str(),
                    match mode {
                        StreamResumeMode::ResumeSealed => "stream resume",
                        StreamResumeMode::AbortDrain => "stream abort-drain",
                    },
                ));
            }
        };
        validate_stream_resume_profile_authority(&provisional.txn.base)?;
        self.prepare_stream_resume_preflight(
            &provisional,
            &graph_identity_digest,
            &resume_id,
            expected_lifecycle_revision,
            mode,
            &actor_id,
        )
        .await?;
        let key = provisional.worker_key;
        let exclusive = self
            .write_queue()
            .acquire_stream_exclusive(&provisional.admission_key)
            .await;

        // The exclusive wait can outlive the authority captured above. Check
        // the receipt and current lane before the registry retires any writer:
        // a concurrent exact retry must not retire the writer just installed
        // by the winning resume, and DISABLING must refuse before mutation.
        let post_wait = self.open_write_txn(None).await?;
        let post_wait_lifecycle = post_wait
            .base
            .stream_lifecycle(key.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("stream_resume_lifecycle:{table_key}"),
                    Some(format!("{:?}", provisional.lifecycle)),
                    None,
                )
            })?;
        if self
            .selected_resume_receipt_matches(
                &post_wait.base,
                &post_wait_lifecycle,
                &graph_identity_digest,
                &resume_id,
                expected_lifecycle_revision,
                mode,
                &actor_id,
            )
            .await?
        {
            self.complete_selected_resume_sidecar(
                &post_wait.base,
                &post_wait_lifecycle,
                &resume_id,
            )
            .await?;
            return Ok(());
        }
        if post_wait_lifecycle.lifecycle_revision != expected_lifecycle_revision {
            return Err(OmniError::StreamLifecycleChanged {
                stable_table_id: key.identity.stable_table_id,
                table_incarnation_id: key.identity.table_incarnation_id,
                expected_revision: expected_lifecycle_revision,
                current_revision: post_wait_lifecycle.lifecycle_revision,
            });
        }
        let recaptured = self
            .recapture_stream_resume_lane(&provisional, mode, "stream resume post-wait authority")
            .await?;
        validate_stream_resume_profile_authority(&recaptured.txn.base)?;
        self.prepare_stream_resume_preflight(
            &recaptured,
            &graph_identity_digest,
            &resume_id,
            expected_lifecycle_revision,
            mode,
            &actor_id,
        )
        .await?;
        self.exact_pending_stream_resume(
            &recaptured,
            &resume_id,
            expected_lifecycle_revision,
            mode,
            &actor_id,
        )
        .await?;
        ensure_same_binding(key, &recaptured, "stream resume post-wait binding")?;
        if recaptured.admission_key != provisional.admission_key {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_resume_admission:{table_key}"),
                Some(format!("{:?}", provisional.admission_key)),
                Some(format!("{:?}", recaptured.admission_key)),
            ));
        }
        let exclusive_authority =
            CheckedExclusiveStreamAuthority::from_exclusive_admission(exclusive);
        let opener_db = Arc::clone(&self);
        let opener_capture = recaptured.clone();
        let opener_resume_id = resume_id.clone();
        let opener_actor = actor_id.clone();
        let idle_authority = self.stream_idle_authority_check(
            key,
            recaptured.admission_key.clone(),
            table_key.clone(),
        );
        let opener = move || {
            let opener_db = Arc::clone(&opener_db);
            let opener_capture = opener_capture.clone();
            let opener_resume_id = opener_resume_id.clone();
            let opener_actor = opener_actor.clone();
            Box::pin(async move {
                Box::pin(opener_db.open_stream_writer_with_resume(
                    &opener_capture,
                    &opener_resume_id,
                    expected_lifecycle_revision,
                    mode,
                    &opener_actor,
                ))
                .await
            }) as crate::table_store::mem_wal::WorkerOpenFuture
        };
        let open_result = self
            .stream_workers
            .install_resumed_writer(
                key,
                table_key.clone(),
                exclusive_authority,
                Box::new(opener),
                idle_authority,
            )
            .await;

        // A recovered invocation can publish the exact terminal receipt
        // without reconstructing a process-local ShardWriter. In that case
        // the registry opener reports an unclaimed stop, but receipt-first
        // classification still makes the caller-visible result successful.
        self.refresh_coordinator_only().await?;
        let terminal = self.open_write_txn(None).await?;
        let terminal_lifecycle = terminal
            .base
            .stream_lifecycle(key.identity)
            .cloned()
            .ok_or_else(|| OmniError::manifest_internal("stream resume lost its lifecycle lane"))?;
        if self
            .selected_resume_receipt_matches(
                &terminal.base,
                &terminal_lifecycle,
                &graph_identity_digest,
                &resume_id,
                expected_lifecycle_revision,
                mode,
                &actor_id,
            )
            .await?
        {
            self.complete_selected_resume_sidecar(&terminal.base, &terminal_lifecycle, &resume_id)
                .await?;
            return Ok(());
        }
        open_result.map_err(worker_error)
    }

    async fn prepare_stream_resume_preflight(
        &self,
        capture: &StreamAuthorityCapture,
        graph_identity_digest: &str,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: &str,
    ) -> Result<(
        super::stream_lifecycle::PreparedStreamResumeOpen,
        Vec<String>,
    )> {
        let public_named_branches = self
            .coordinator
            .read()
            .await
            .branch_list()
            .await?
            .into_iter()
            .filter(|branch| branch != "main" && !crate::db::is_internal_system_branch(branch))
            .collect::<Vec<_>>();
        let prepared = prepare_stream_resume_open(
            &capture.lifecycle,
            StreamResumeRequest {
                graph_identity_digest: graph_identity_digest.to_string(),
                resume_id: resume_id.to_string(),
                expected_lifecycle_revision,
                mode,
                actor_id: actor_id.to_string(),
                initiated_at: crate::db::now_micros()?,
                public_named_branches: public_named_branches.clone(),
            },
        )?;
        Ok((prepared, public_named_branches))
    }

    /// Finish stale sidecar cleanup only after the manifest-selected receipt
    /// has already proved the terminal OPEN result. This path never invokes a
    /// claim or publishes lifecycle authority without exclusive admission; it
    /// merely makes a lost sidecar-delete response idempotent.
    async fn complete_selected_resume_sidecar(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        lifecycle: &StreamLifecycleEntry,
        resume_id: &str,
    ) -> Result<()> {
        let mut exact = None;
        for sidecar in list_sidecars(self.root_uri(), self.storage_adapter()).await? {
            let Some(RecoveryProtocolV15::StreamResume(protocol)) = sidecar.protocol_v15.as_deref()
            else {
                continue;
            };
            if protocol.request.resume_id != resume_id
                || protocol.admission_scope.identity != lifecycle.identity
                || protocol.admission_scope.binding_scope_id != lifecycle.binding_scope_id
            {
                continue;
            }
            if exact.replace(sidecar).is_some() {
                return Err(OmniError::manifest_internal(
                    "multiple selected StreamResume sidecars match one occurrence",
                ));
            }
        }
        let Some(sidecar) = exact else {
            return Ok(());
        };
        match complete_stream_resume_sidecar_v15(
            self.root_uri(),
            Arc::clone(&self.storage),
            snapshot,
            &sidecar,
        )
        .await?
        {
            RecoveryStreamResumeOutcomeV15::TerminalVisible {
                lifecycle: selected,
                ..
            } if selected == *lifecycle => Ok(()),
            _ => Err(OmniError::recovery_required(
                sidecar.operation_id,
                "selected resume receipt has a nonterminal recovery sidecar",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn selected_resume_receipt_matches(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        lifecycle: &StreamLifecycleEntry,
        graph_identity_digest: &str,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: &str,
    ) -> Result<bool> {
        let selected = snapshot.open_stream_token_authority().await?;
        let Some(receipt) = lookup_management_receipt(
            &selected,
            snapshot.stream_token_authority(),
            graph_identity_digest,
            lifecycle.identity,
            &lifecycle.enrollment_receipt.stream_incarnation_id,
            STREAM_RESUME_OPERATION_KIND,
            resume_id,
        )
        .await?
        else {
            return Ok(false);
        };
        receipt.validate(receipt.to_revision)?;
        let request: StreamResumeRequestPayload =
            serde_json::from_value(receipt.request_payload.clone()).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "selected stream resume receipt has an invalid request payload: {error}"
                ))
            })?;
        let expected_to_revision = expected_lifecycle_revision.checked_add(1).ok_or_else(|| {
            OmniError::manifest_internal("selected stream resume receipt revision overflow")
        })?;
        let exact = receipt.graph_identity_digest == graph_identity_digest
            && receipt.identity == lifecycle.identity
            && receipt.stream_incarnation_id == lifecycle.enrollment_receipt.stream_incarnation_id
            && receipt.binding_scope_id == lifecycle.binding_scope_id
            && receipt.operation_kind == STREAM_RESUME_OPERATION_KIND
            && receipt.operation_id == resume_id
            && receipt.from_revision == expected_lifecycle_revision
            && receipt.to_revision == expected_to_revision
            && receipt.actor_id == actor_id
            && request.resume_id == resume_id
            && request.expected_lifecycle_revision == expected_lifecycle_revision
            && request.mode == mode
            && request.actor_id == actor_id
            && request.graph_identity_digest == graph_identity_digest
            && request.identity == lifecycle.identity
            && request.stream_incarnation_id == lifecycle.enrollment_receipt.stream_incarnation_id
            && request.binding_scope_id == lifecycle.binding_scope_id
            && request.enrollment_id == lifecycle.binding.enrollment_id
            && request.public_named_branches.is_empty()
            && request.request_digest()? == receipt.request_digest
            && receipt.result_payload == stream_resume_result_payload(receipt.to_revision)?;
        if !exact {
            return Err(OmniError::StreamLifecycleIdempotencyConflict {
                stable_table_id: lifecycle.identity.stable_table_id,
                table_incarnation_id: lifecycle.identity.table_incarnation_id,
                operation_kind: STREAM_RESUME_OPERATION_KIND.to_string(),
                operation_id: resume_id.to_string(),
            });
        }
        validate_selected_management_receipt_progress(lifecycle, &receipt, StreamLifecycle::Open)?;
        Ok(true)
    }

    /// The recovery-v15 physical claimant. This deliberately does not call or
    /// relax the ordinary v14 claim path: v14's wire meaning remains frozen.
    #[allow(clippy::too_many_arguments)]
    async fn open_stream_writer_with_resume(
        self: &Arc<Self>,
        capture: &StreamAuthorityCapture,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: &str,
    ) -> std::result::Result<OpenedMemWalWorker, WorkerOpenFailure> {
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(capture.entry.table_key.clone(), None)])
            .await;
        let gated_capture = match mode {
            StreamResumeMode::ResumeSealed => {
                self.capture_sealed_stream_authority(
                    &capture.entry.table_key,
                    "stream resume gated recapture",
                )
                .await
            }
            StreamResumeMode::AbortDrain => {
                let drain_id = capture
                    .lifecycle
                    .drain
                    .as_ref()
                    .map(|drain| drain.drain_id.as_str())
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "stream abort-drain gated recapture lost its drain descriptor",
                        )
                    });
                match drain_id {
                    Ok(drain_id) => {
                        self.capture_draining_stream_authority(
                            &capture.entry.table_key,
                            "stream abort-drain gated recapture",
                            drain_id,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            }
        }
        .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        ensure_same_capture(capture, &gated_capture, "stream resume gated authority")
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let capture = &gated_capture;
        let graph_identity_digest =
            stream_graph_identity_digest(&capture.txn.authority.schema_identity_domain)
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let (prepared_open, public_named_branches) = self
            .prepare_stream_resume_preflight(
                capture,
                &graph_identity_digest,
                resume_id,
                expected_lifecycle_revision,
                mode,
                actor_id,
            )
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let prior_claim = if capture.lifecycle.current_claim_receipt_id.is_some() {
            Some(
                self.selected_claim_receipt(&capture.txn.base, &capture.lifecycle)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?,
            )
        } else {
            None
        };
        let tailer = claim_wal_tailer(capture)
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let pending = self
            .exact_pending_stream_resume(
                capture,
                resume_id,
                expected_lifecycle_revision,
                mode,
                actor_id,
            )
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let (
            operation,
            mut attempt,
            mut prior_attempt_chain,
            mut snapshot,
            mut sidecar,
            mut invoke_attempt,
        ) = if let Some(pending_sidecar) = pending {
            let outcome = complete_stream_resume_sidecar_v15(
                self.root_uri(),
                Arc::clone(&self.storage),
                &capture.txn.base,
                &pending_sidecar,
            )
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let pending_attempt = prepared_stream_resume_attempt_v15(&pending_sidecar)
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            match outcome {
                RecoveryStreamResumeOutcomeV15::AttemptPending {
                    prior_attempt_chain,
                    ..
                } => {
                    read_claim_physical_prestate_after_attempt(capture)
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    (
                        pending_attempt.operation.clone(),
                        pending_attempt,
                        prior_attempt_chain,
                        capture.txn.base.clone(),
                        pending_sidecar,
                        false,
                    )
                }
                RecoveryStreamResumeOutcomeV15::CheckpointVisible { .. } => {
                    self.refresh_coordinator_only().await.map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let checkpoint_snapshot = self.coordinator.read().await.snapshot();
                    let physical = read_claim_physical_prestate_after_attempt(capture)
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    let next_attempt =
                        prepare_next_claim_attempt(&pending_attempt.operation, &tailer, physical)
                            .await
                            .map_err(|error| {
                                WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                            })?;
                    let mut pending_sidecar = pending_sidecar;
                    rearm_stream_resume_checkpoint_sidecar_v15(
                        self.root_uri(),
                        self.storage_adapter(),
                        &checkpoint_snapshot,
                        &mut pending_sidecar,
                        &next_attempt,
                    )
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let refreshed = self
                        .recapture_stream_resume_lane(
                            capture,
                            mode,
                            "stream resume checkpoint continuation",
                        )
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    drop(_table_guards);
                    drop(_stream_token_guard);
                    drop(_branch_guard);
                    drop(_schema_guard);
                    return Box::pin(self.open_stream_writer_with_resume(
                        &refreshed,
                        resume_id,
                        expected_lifecycle_revision,
                        mode,
                        actor_id,
                    ))
                    .await;
                }
                RecoveryStreamResumeOutcomeV15::EffectFree => {
                    self.refresh_coordinator_only().await.map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let refreshed = self
                        .recapture_stream_resume_lane(
                            capture,
                            mode,
                            "stream resume effect-free continuation",
                        )
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    drop(_table_guards);
                    drop(_stream_token_guard);
                    drop(_branch_guard);
                    drop(_schema_guard);
                    return Box::pin(self.open_stream_writer_with_resume(
                        &refreshed,
                        resume_id,
                        expected_lifecycle_revision,
                        mode,
                        actor_id,
                    ))
                    .await;
                }
                RecoveryStreamResumeOutcomeV15::TerminalVisible { .. } => {
                    return Err(WorkerOpenFailure::unclaimed(
                        MemWalWorkerError::InvalidState {
                            reason: "stream resume terminal receipt is already visible".to_string(),
                        },
                    ));
                }
            }
        } else {
            let physical = read_claim_physical_prestate(capture)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            if mode == StreamResumeMode::AbortDrain {
                ensure_abort_drain_physical_cut_is_empty(capture, &tailer, physical)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
            }
            let claim_id = ShardId::new_v4().to_string();
            let operation = prepare_resume_claim_operation(
                &capture.lifecycle,
                ClaimOperationRequest {
                    graph_identity_digest: graph_identity_digest.clone(),
                    claim_id: claim_id.clone(),
                    lifecycle_operation_id: Some(resume_id.to_string()),
                    recovery_operation_id: claim_id,
                    claim_kind: STREAM_RESUME_OPERATION_KIND.to_string(),
                    profile: ClaimProfile::RetainAll,
                    shard_id: capture.shard_id.to_string(),
                    initial_shard_manifest_version: physical.shard_manifest_version,
                    initial_writer_epoch: physical.writer_epoch,
                    initial_replay_cursor: physical.replay_cursor,
                    initial_current_generation: physical.current_generation,
                    initial_base_merged_generation: physical.base_merged_generation,
                    claim_contract_version: 1,
                },
                mode,
                prepared_open.minimum_next_epoch_floor,
            )
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let attempt = prepare_next_claim_attempt(&operation, &tailer, physical)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let prior_attempt_chain = crate::db::manifest::stream::claim_attempt_chain_genesis();
            let authority = RecoveryAuthorityToken {
                branch_identifier: capture.txn.authority.branch_identifier.clone(),
                graph_head: capture.txn.authority.graph_head.clone(),
                schema_identity_domain: capture.txn.authority.schema_identity_domain.clone(),
                schema_ir_hash: capture.txn.authority.schema_ir_hash.clone(),
                schema_identity_version: capture.txn.authority.schema_identity_version,
            };
            let request = RecoveryStreamResumeRequestV15 {
                protocol_version:
                    crate::db::manifest::stream::STREAM_RESUME_REQUEST_PROTOCOL_VERSION,
                graph_identity_digest: graph_identity_digest.clone(),
                identity: capture.lifecycle.identity,
                stream_incarnation_id: capture
                    .lifecycle
                    .enrollment_receipt
                    .stream_incarnation_id
                    .clone(),
                binding_scope_id: capture.lifecycle.binding_scope_id.clone(),
                enrollment_id: capture.lifecycle.binding.enrollment_id.clone(),
                resume_id: resume_id.to_string(),
                expected_lifecycle_revision,
                mode,
                actor_id: actor_id.to_string(),
                public_named_branches,
            };
            let open_plan = RecoveryStreamOpenPlanV15 {
                next_lifecycle_revision: prepared_open.next_lifecycle_revision,
                expected_binding: capture.lifecycle.binding.clone(),
                expected_base_head: capture.lifecycle.current_head_witness.clone(),
                shard_id: capture.shard_id.to_string(),
                minimum_next_epoch_floor: prepared_open.minimum_next_epoch_floor,
            };
            let snapshot = capture.txn.base.clone();
            let sidecar = new_stream_resume_sidecar_v15(
                authority,
                snapshot.version(),
                snapshot.stream_profile().clone(),
                capture.lifecycle.clone(),
                snapshot.stream_token_authority().clone(),
                prior_claim,
                request,
                open_plan,
                prior_attempt_chain.clone(),
                &attempt,
            )
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            (
                operation,
                attempt,
                prior_attempt_chain,
                snapshot,
                sidecar,
                true,
            )
        };

        loop {
            let (claim_was_invoked, mut claimed_writer, mut writer_claim_error) = if invoke_attempt
            {
                let config = reconstruct_b1_writer_config(
                    &capture.details,
                    capture.enrollment_id,
                    capture.shard_id,
                )
                .map_err(WorkerOpenFailure::unclaimed)?;
                match capture
                    .head
                    .dataset()
                    .mem_wal_writer(capture.shard_id, config)
                    .await
                {
                    Ok(writer) => (true, Some(ClaimedMemWalWorker::new(writer)), None),
                    Err(error) => (true, None, Some(error)),
                }
            } else {
                (false, None, None)
            };
            let (evidence, achieved) = observe_claim_attempt(capture, &tailer, &attempt)
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
            let effect = build_claim_attempt_effect(&prior_attempt_chain, &attempt, evidence)
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;

            if matches!(
                evidence,
                ClaimAttemptEvidence::NoEffect | ClaimAttemptEvidence::AbortedNoEffect
            ) {
                classify_effect_free_stream_resume_sidecar_v15(
                    self.root_uri(),
                    self.storage_adapter(),
                    &mut sidecar,
                    effect,
                )
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
                complete_stream_resume_sidecar_v15(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &snapshot,
                    &sidecar,
                )
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
                if let Some(claimed) = claimed_writer.take() {
                    return Err(WorkerOpenFailure::claimed(
                        MemWalWorkerError::InvalidState {
                            reason: "Lance reported resume-claim success without its exact manifest effect"
                                .to_string(),
                        },
                        claimed,
                    ));
                }
                if let Some(error) = writer_claim_error.take() {
                    return Err(WorkerOpenFailure::unclaimed(MemWalWorkerError::Lance {
                        operation: "resume writer claim",
                        message: error.to_string(),
                    }));
                }
                self.refresh_coordinator_only().await.map_err(|error| {
                    WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                })?;
                let refreshed = self
                    .recapture_stream_resume_lane(
                        capture,
                        mode,
                        "effect-free resume claim continuation",
                    )
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                drop(_table_guards);
                drop(_stream_token_guard);
                drop(_branch_guard);
                drop(_schema_guard);
                return Box::pin(self.open_stream_writer_with_resume(
                    &refreshed,
                    resume_id,
                    expected_lifecycle_revision,
                    mode,
                    actor_id,
                ))
                .await;
            }

            if matches!(evidence, ClaimAttemptEvidence::StockManifestOnly { .. }) {
                let records = [LifecycleLedgerRecord::ClaimAttemptEffect(effect.clone())];
                let outcome = self
                    .commit_stream_resume_ledger(&snapshot, &mut sidecar, &records, effect, None)
                    .await
                    .map_err(|error| {
                        worker_open_failure_preserving_claim(
                            claim_open_worker_error(error),
                            &mut claimed_writer,
                        )
                    })?;
                let RecoveryStreamResumeOutcomeV15::CheckpointVisible {
                    prior_attempt_chain: next_chain,
                    ..
                } = outcome
                else {
                    return Err(worker_open_failure_preserving_claim(
                        MemWalWorkerError::InvalidState {
                            reason: "manifest-only resume claim did not publish its checkpoint"
                                .to_string(),
                        },
                        &mut claimed_writer,
                    ));
                };
                if let Some(claimed) = claimed_writer.take() {
                    return Err(WorkerOpenFailure::claimed(
                        MemWalWorkerError::InvalidState {
                            reason: "Lance reported resume-claim success but only its stock manifest effect was observable"
                                .to_string(),
                        },
                        claimed,
                    ));
                }
                self.refresh_coordinator_only().await.map_err(|error| {
                    WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                })?;
                snapshot = self.coordinator.read().await.snapshot();
                let physical = read_claim_physical_prestate_after_attempt(capture)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                attempt = prepare_next_claim_attempt(&operation, &tailer, physical)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                rearm_stream_resume_checkpoint_sidecar_v15(
                    self.root_uri(),
                    self.storage_adapter(),
                    &snapshot,
                    &mut sidecar,
                    &attempt,
                )
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                prior_attempt_chain = next_chain;
                invoke_attempt = true;
                continue;
            }

            if !claim_was_invoked {
                let projection = recovered_current_generation_projection_source(
                    capture, &tailer, &attempt, &achieved,
                )
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                self.publish_terminal_stream_resume(
                    capture,
                    &snapshot,
                    &tailer,
                    &attempt,
                    &effect,
                    &achieved,
                    projection,
                    &prepared_open,
                    mode,
                    &mut sidecar,
                )
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                return Err(WorkerOpenFailure::unclaimed(
                    MemWalWorkerError::InvalidState {
                        reason:
                            "recovered stream resume published without a live process-local writer"
                                .to_string(),
                    },
                ));
            }
            if let Some(error) = writer_claim_error.take() {
                return Err(WorkerOpenFailure::unclaimed(MemWalWorkerError::Lance {
                    operation: "resume writer claim",
                    message: format!(
                        "{error}; exact terminal physical claim remains recovery-owned by {}",
                        sidecar.operation_id
                    ),
                }));
            }
            let claimed = claimed_writer.take().ok_or_else(|| {
                WorkerOpenFailure::unclaimed(MemWalWorkerError::InvalidState {
                    reason: "resume claim returned neither a claimed writer nor an error"
                        .to_string(),
                })
            })?;
            let mut opened = claimed
                .classify(
                    capture.head.dataset(),
                    &capture.full_path,
                    &capture.details,
                    operation.initial_writer_epoch,
                )
                .await?;
            let projection = match opened.current_generation_projection_source() {
                Ok(projection) => projection,
                Err(error) => {
                    return Err(WorkerOpenFailure::claimed(error, opened.into_claimed()));
                }
            };
            let terminal = self
                .publish_terminal_stream_resume(
                    capture,
                    &snapshot,
                    &tailer,
                    &attempt,
                    &effect,
                    &achieved,
                    projection,
                    &prepared_open,
                    mode,
                    &mut sidecar,
                )
                .await;
            if let Err(error) = terminal {
                return Err(WorkerOpenFailure::claimed(
                    claim_open_worker_error(error),
                    opened.into_claimed(),
                ));
            }
            return Ok(opened);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_terminal_stream_resume(
        &self,
        capture: &StreamAuthorityCapture,
        snapshot: &crate::db::manifest::Snapshot,
        tailer: &WalTailer,
        attempt: &super::stream_lifecycle::PreparedClaimAttempt,
        effect: &crate::db::manifest::stream::ClaimAttemptEffect,
        achieved: &ClaimPhysicalPrestate,
        projection: CurrentGenerationProjectionSource,
        prepared_open: &super::stream_lifecycle::PreparedStreamResumeOpen,
        mode: StreamResumeMode,
        sidecar: &mut crate::db::manifest::RecoverySidecar,
    ) -> Result<()> {
        if !matches!(projection, CurrentGenerationProjectionSource::Empty) {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "stream resume claim observed an unmerged current generation",
            ));
        }
        let schema = Arc::new(ArrowSchema::from(capture.head.dataset().schema()));
        let key_plan = claim_wal_key_discovery_plan(attempt, effect, Arc::clone(&schema))?;
        let keys = collect_claim_wal_segment_keys(tailer, &key_plan).await?;
        if !keys.is_empty() {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "stream resume authenticated a non-empty WAL suffix",
            ));
        }
        let auth_plan = claim_wal_authentication_plan(
            attempt,
            effect,
            capture.txn.authority.schema_ir_hash.clone(),
            Arc::clone(&schema),
            BTreeMap::new(),
        )?;
        let segment = authenticate_claim_wal_segment(tailer, &auth_plan).await?;
        if segment.row_count != 0 {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "stream resume terminal suffix contains rows",
            ));
        }
        let full_lww = current_generation_lww_projection_digest(
            &attempt.operation,
            &capture.txn.authority.schema_ir_hash,
            schema,
            &[],
        )?;
        let attempt_chain = effect.next_attempt_chain_ref()?;
        let current_lifecycle = snapshot
            .stream_lifecycle(capture.entry.identity)
            .ok_or_else(|| {
                OmniError::manifest_internal("terminal resume lost its lifecycle lane")
            })?;
        let built = build_terminal_claim(
            &current_lifecycle.claim_receipt_chain,
            attempt,
            effect,
            &attempt_chain,
            &segment,
            &full_lww,
            achieved.replay_cursor,
            prepared_open.recorded_at,
        )?;
        let result_payload = stream_resume_result_payload(prepared_open.next_lifecycle_revision)?;
        let management = ManagementReceipt::new(
            attempt.operation.graph_identity_digest.clone(),
            current_lifecycle.identity,
            current_lifecycle
                .enrollment_receipt
                .stream_incarnation_id
                .clone(),
            current_lifecycle.binding_scope_id.clone(),
            &current_lifecycle.management_receipt_chain,
            attempt
                .operation
                .lifecycle_operation_id
                .clone()
                .ok_or_else(|| {
                    OmniError::manifest_internal("resume claim lost its occurrence ID")
                })?,
            STREAM_RESUME_OPERATION_KIND,
            current_lifecycle.lifecycle_revision,
            prepared_open.next_lifecycle_revision,
            sidecar
                .actor_id
                .clone()
                .ok_or_else(|| OmniError::manifest_internal("resume sidecar lost its actor"))?,
            prepared_open.request_payload.clone(),
            result_payload,
            prepared_open.recorded_at,
        )?;
        let next_lifecycle =
            build_resume_adoption_row(current_lifecycle, &built, &management, mode)?;
        let records = [
            LifecycleLedgerRecord::ClaimAttemptEffect(effect.clone()),
            LifecycleLedgerRecord::ClaimReceipt(built.receipt.clone()),
            LifecycleLedgerRecord::ManagementReceipt(management.clone()),
        ];
        let outcome = self
            .commit_stream_resume_ledger(
                snapshot,
                sidecar,
                &records,
                effect.clone(),
                Some((built.receipt, management, next_lifecycle.clone())),
            )
            .await?;
        match outcome {
            RecoveryStreamResumeOutcomeV15::TerminalVisible { lifecycle, .. }
                if lifecycle == next_lifecycle =>
            {
                self.refresh_coordinator_only().await?;
                Ok(())
            }
            _ => Err(OmniError::manifest_internal(
                "terminal resume did not publish its exact OPEN authority",
            )),
        }
    }

    async fn commit_stream_resume_ledger(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        sidecar: &mut crate::db::manifest::RecoverySidecar,
        records: &[LifecycleLedgerRecord],
        effect: crate::db::manifest::stream::ClaimAttemptEffect,
        terminal: Option<(
            crate::db::manifest::stream::ClaimReceipt,
            ManagementReceipt,
            StreamLifecycleEntry,
        )>,
    ) -> Result<RecoveryStreamResumeOutcomeV15> {
        let selected = snapshot.open_stream_token_authority().await?;
        let staged =
            stage_lifecycle_ledger_records(selected, snapshot.stream_token_authority(), records)
                .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                snapshot.stream_token_authority(),
                &self.control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);
        match terminal {
            Some((claim, management, lifecycle)) => {
                arm_stream_resume_terminal_sidecar_v15(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    effect,
                    claim,
                    management,
                    lifecycle,
                    planned_transaction,
                )
                .await?;
            }
            None => {
                arm_stream_resume_checkpoint_sidecar_v15(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    effect,
                    planned_transaction,
                )
                .await?;
            }
        }
        if let Ok(outcome) = self.storage().commit_staged_exact(token_head, staged).await {
            if !outcome.is_exact() {
                return Err(OmniError::recovery_required(
                    sidecar.operation_id.clone(),
                    "resume ledger participant committed a non-exact transaction",
                ));
            }
            let next_authority =
                stream_token_authority_entry_for_dataset(outcome.snapshot().dataset()).await?;
            confirm_stream_resume_sidecar_v15(
                self.root_uri(),
                self.storage_adapter(),
                sidecar,
                outcome.committed_transaction().clone(),
                next_authority.current_head_witness.clone(),
                next_authority,
            )
            .await?;
        }
        let root_uri = self.root_uri().to_string();
        let storage = Arc::clone(&self.storage);
        let snapshot = snapshot.clone();
        let sidecar = sidecar.clone();
        crate::instrumentation::spawn_with_query_io_probes(async move {
            complete_stream_resume_sidecar_v15(&root_uri, storage, &snapshot, &sidecar).await
        })
        .await
        .map_err(|error| {
            OmniError::Lance(format!(
                "stream resume recovery owner task failed before returning its exact outcome: {error}"
            ))
        })?
    }

    async fn exact_pending_stream_resume(
        &self,
        capture: &StreamAuthorityCapture,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        mode: StreamResumeMode,
        actor_id: &str,
    ) -> Result<Option<crate::db::manifest::RecoverySidecar>> {
        let mut exact = None;
        for sidecar in list_sidecars(self.root_uri(), self.storage_adapter()).await? {
            let relevant = sidecar.writer_kind.is_graph_global_barrier()
                || sidecar
                    .stream_admission_scope()
                    .is_some_and(|scope| scope.identity == capture.entry.identity)
                || sidecar
                    .tables
                    .iter()
                    .any(|pin| pin.identity == capture.entry.identity);
            if !relevant {
                continue;
            }
            let is_exact = matches!(
                sidecar.protocol_v15.as_deref(),
                Some(RecoveryProtocolV15::StreamResume(protocol))
                    if protocol.admission_scope.identity == capture.entry.identity
                        && protocol.admission_scope.binding_scope_id
                            == capture.lifecycle.binding_scope_id
                        && protocol.request.resume_id == resume_id
                        && protocol.request.expected_lifecycle_revision
                            == expected_lifecycle_revision
                        && protocol.request.mode == mode
                        && protocol.request.actor_id == actor_id
            );
            if is_exact && exact.is_none() {
                exact = Some(sidecar);
                continue;
            }
            return Err(OmniError::recovery_required(
                sidecar.operation_id,
                format!(
                    "pending {:?} recovery operation overlaps stream resume for table identity {}",
                    sidecar.writer_kind, capture.entry.identity
                ),
            ));
        }
        Ok(exact)
    }

    async fn recapture_stream_resume_lane(
        &self,
        prior: &StreamAuthorityCapture,
        mode: StreamResumeMode,
        operation: &str,
    ) -> Result<StreamAuthorityCapture> {
        match mode {
            StreamResumeMode::ResumeSealed => {
                self.capture_sealed_stream_authority(&prior.entry.table_key, operation)
                    .await
            }
            StreamResumeMode::AbortDrain => {
                let drain_id = prior
                    .lifecycle
                    .drain
                    .as_ref()
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "abort-drain continuation lost its drain descriptor",
                        )
                    })?
                    .drain_id
                    .clone();
                self.capture_draining_stream_authority(&prior.entry.table_key, operation, &drain_id)
                    .await
            }
        }
    }

    /// The sole cold MemWAL writer opener.
    ///
    /// Every epoch claim is armed before Lance is invoked and remains
    /// recovery-owned until its immutable attempt/terminal ledger records and
    /// lifecycle authority are selected together. Callers must already hold
    /// the lane's shared or exclusive admission authority.
    pub(super) async fn open_stream_writer_with_claim(
        &self,
        capture: &StreamAuthorityCapture,
        claim_kind: &str,
        actor_id: Option<String>,
    ) -> std::result::Result<OpenedMemWalWorker, WorkerOpenFailure> {
        // Admission (and the caller-owned profile-shared lease) are already
        // outermost. Hold the complete graph-write inner order from the final
        // recapture through sidecar arm, physical classification, ledger
        // effect, and manifest selection. In particular, the graph-global
        // token gate prevents an unrelated lane from invalidating a prepared
        // claim transaction after the physical epoch fence happened.
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(capture.entry.table_key.clone(), None)])
            .await;
        let gated_capture = self
            .recapture_stream_claim_lane(capture, "stream claim gated recapture")
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        ensure_same_capture(capture, &gated_capture, "stream claim gated authority")
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        self.ensure_no_relevant_stream_sidecar_except_exact_claim(&gated_capture, "stream claim")
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let capture = &gated_capture;
        let graph_identity_digest =
            stream_graph_identity_digest(&capture.txn.authority.schema_identity_domain)
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let tailer = claim_wal_tailer(capture)
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let pending = self
            .exact_pending_stream_claim(capture, &graph_identity_digest)
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
        let (
            operation,
            mut attempt,
            mut prior_attempt_chain,
            mut snapshot,
            mut sidecar,
            mut invoke_attempt,
        ) = if let Some((pending_sidecar, pending_attempt)) = pending {
            let outcome = complete_stream_claim_sidecar_v14(
                self.root_uri(),
                Arc::clone(&self.storage),
                &capture.txn.base,
                &pending_sidecar,
            )
            .await
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            match outcome {
                RecoveryStreamClaimOutcomeV14::AttemptPending {
                    prior_attempt_chain,
                    ..
                } => (
                    pending_attempt.operation.clone(),
                    pending_attempt,
                    prior_attempt_chain,
                    capture.txn.base.clone(),
                    pending_sidecar,
                    false,
                ),
                RecoveryStreamClaimOutcomeV14::CheckpointVisible { .. } => {
                    self.refresh_coordinator_only().await.map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let checkpoint_snapshot = self.coordinator.read().await.snapshot();
                    let physical = read_claim_physical_prestate_after_attempt(capture)
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    let next_attempt =
                        prepare_next_claim_attempt(&pending_attempt.operation, &tailer, physical)
                            .await
                            .map_err(|error| {
                                WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                            })?;
                    receipt_first_rearm_stream_claim_sidecar_v14(
                        self.root_uri(),
                        Arc::clone(&self.storage),
                        &checkpoint_snapshot,
                        &graph_identity_digest,
                        capture.entry.identity,
                        &capture.lifecycle.binding_scope_id,
                        &pending_attempt.operation.claim_id,
                        &pending_attempt.operation.recovery_operation_id,
                        &next_attempt,
                    )
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let refreshed = self
                        .recapture_stream_claim_lane(
                            capture,
                            "stream claim checkpoint continuation",
                        )
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    drop(_table_guards);
                    drop(_stream_token_guard);
                    drop(_branch_guard);
                    drop(_schema_guard);
                    return Box::pin(
                        self.open_stream_writer_with_claim(&refreshed, claim_kind, actor_id),
                    )
                    .await;
                }
                RecoveryStreamClaimOutcomeV14::EffectFree
                | RecoveryStreamClaimOutcomeV14::TerminalVisible { .. } => {
                    self.refresh_coordinator_only().await.map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                    let refreshed = self
                        .recapture_stream_claim_lane(capture, "stream claim terminal continuation")
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?;
                    drop(_table_guards);
                    drop(_stream_token_guard);
                    drop(_branch_guard);
                    drop(_schema_guard);
                    return Box::pin(
                        self.open_stream_writer_with_claim(&refreshed, claim_kind, actor_id),
                    )
                    .await;
                }
            }
        } else {
            let physical = read_claim_physical_prestate(capture)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let claim_id = ShardId::new_v4().to_string();
            let operation = prepare_claim_operation(
                &capture.lifecycle,
                ClaimOperationRequest {
                    graph_identity_digest: graph_identity_digest.clone(),
                    claim_id: claim_id.clone(),
                    lifecycle_operation_id: capture
                        .lifecycle
                        .drain
                        .as_ref()
                        .map(|drain| drain.drain_id.clone()),
                    recovery_operation_id: claim_id,
                    claim_kind: claim_kind.to_string(),
                    profile: ClaimProfile::RetainAll,
                    shard_id: capture.shard_id.to_string(),
                    initial_shard_manifest_version: physical.shard_manifest_version,
                    initial_writer_epoch: physical.writer_epoch,
                    initial_replay_cursor: physical.replay_cursor,
                    initial_current_generation: physical.current_generation,
                    initial_base_merged_generation: physical.base_merged_generation,
                    claim_contract_version: 1,
                },
            )
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let attempt = prepare_next_claim_attempt(&operation, &tailer, physical)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            let prior_attempt_chain = crate::db::manifest::stream::claim_attempt_chain_genesis();
            let authority = RecoveryAuthorityToken {
                branch_identifier: capture.txn.authority.branch_identifier.clone(),
                graph_head: capture.txn.authority.graph_head.clone(),
                schema_identity_domain: capture.txn.authority.schema_identity_domain.clone(),
                schema_ir_hash: capture.txn.authority.schema_ir_hash.clone(),
                schema_identity_version: capture.txn.authority.schema_identity_version,
            };
            let snapshot = capture.txn.base.clone();
            let sidecar = new_stream_claim_sidecar_v14(
                actor_id.clone(),
                authority,
                snapshot.version(),
                snapshot.stream_profile().clone(),
                capture.lifecycle.clone(),
                snapshot.stream_token_authority().clone(),
                prior_attempt_chain.clone(),
                &attempt,
            )
            .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar)
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
            (
                operation,
                attempt,
                prior_attempt_chain,
                snapshot,
                sidecar,
                true,
            )
        };

        loop {
            let (claim_was_invoked, mut claimed_writer, mut writer_claim_error) = if invoke_attempt
            {
                let config = reconstruct_b1_writer_config(
                    &capture.details,
                    capture.enrollment_id,
                    capture.shard_id,
                )
                .map_err(WorkerOpenFailure::unclaimed)?;
                match capture
                    .head
                    .dataset()
                    .mem_wal_writer(capture.shard_id, config)
                    .await
                {
                    Ok(writer) => (true, Some(ClaimedMemWalWorker::new(writer)), None),
                    Err(error) => (true, None, Some(error)),
                }
            } else {
                (false, None, None)
            };
            let (evidence, achieved) = observe_claim_attempt(capture, &tailer, &attempt)
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
            let effect = build_claim_attempt_effect(&prior_attempt_chain, &attempt, evidence)
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;

            if matches!(
                evidence,
                ClaimAttemptEvidence::NoEffect | ClaimAttemptEvidence::AbortedNoEffect
            ) {
                classify_effect_free_stream_claim_sidecar_v14(
                    self.root_uri(),
                    self.storage_adapter(),
                    &mut sidecar,
                    effect,
                )
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
                complete_stream_claim_sidecar_v14(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &snapshot,
                    &sidecar,
                )
                .await
                .map_err(|error| {
                    worker_open_failure_preserving_claim(
                        claim_open_worker_error(error),
                        &mut claimed_writer,
                    )
                })?;
                if let Some(claimed) = claimed_writer.take() {
                    return Err(WorkerOpenFailure::claimed(
                        MemWalWorkerError::InvalidState {
                            reason:
                                "Lance reported writer-claim success without its exact manifest effect"
                                    .to_string(),
                        },
                        claimed,
                    ));
                }
                if let Some(error) = writer_claim_error.take() {
                    return Err(WorkerOpenFailure::unclaimed(MemWalWorkerError::Lance {
                        operation: "writer claim",
                        message: error.to_string(),
                    }));
                }
                self.refresh_coordinator_only().await.map_err(|error| {
                    WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                })?;
                let refreshed = self
                    .recapture_stream_claim_lane(capture, "effect-free claim continuation")
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                drop(_table_guards);
                drop(_stream_token_guard);
                drop(_branch_guard);
                drop(_schema_guard);
                return Box::pin(
                    self.open_stream_writer_with_claim(&refreshed, claim_kind, actor_id),
                )
                .await;
            }

            if matches!(evidence, ClaimAttemptEvidence::StockManifestOnly { .. }) {
                let records = [LifecycleLedgerRecord::ClaimAttemptEffect(effect.clone())];
                let outcome = self
                    .commit_stream_claim_ledger(&snapshot, &mut sidecar, &records, effect, None)
                    .await
                    .map_err(|error| {
                        worker_open_failure_preserving_claim(
                            claim_open_worker_error(error),
                            &mut claimed_writer,
                        )
                    })?;
                let RecoveryStreamClaimOutcomeV14::CheckpointVisible {
                    prior_attempt_chain: next_chain,
                    ..
                } = outcome
                else {
                    return Err(worker_open_failure_preserving_claim(
                        MemWalWorkerError::InvalidState {
                            reason:
                                "manifest-only claim did not publish its exact attempt checkpoint"
                                    .to_string(),
                        },
                        &mut claimed_writer,
                    ));
                };
                if let Some(claimed) = claimed_writer.take() {
                    return Err(WorkerOpenFailure::claimed(
                        MemWalWorkerError::InvalidState {
                            reason:
                                "Lance reported writer-claim success but only its stock manifest effect was observable"
                                    .to_string(),
                        },
                        claimed,
                    ));
                }
                self.refresh_coordinator_only().await.map_err(|error| {
                    WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                })?;
                snapshot = self.coordinator.read().await.snapshot();
                // The checkpoint deliberately advanced the physical writer
                // epoch while the manifest-selected lifecycle still names the
                // pre-claim floor.  Only the first attempt may require exact
                // equality with that floor; continuation binds the achieved
                // checkpoint through the durable attempt chain instead.
                let physical = read_claim_physical_prestate_after_attempt(capture)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                attempt = prepare_next_claim_attempt(&operation, &tailer, physical)
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                sidecar = list_sidecars(self.root_uri(), self.storage_adapter())
                    .await
                    .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?
                    .into_iter()
                    .find(|candidate| candidate.operation_id == sidecar.operation_id)
                    .ok_or_else(|| {
                        WorkerOpenFailure::unclaimed(MemWalWorkerError::InvalidState {
                            reason:
                                "checkpointed claim sidecar disappeared before its next attempt"
                                    .to_string(),
                        })
                    })?;
                rearm_stream_claim_checkpoint_sidecar_v14(
                    self.root_uri(),
                    self.storage_adapter(),
                    &snapshot,
                    &mut sidecar,
                    &attempt,
                )
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                prior_attempt_chain = next_chain;
                invoke_attempt = true;
                continue;
            }

            if !claim_was_invoked {
                let projection = recovered_current_generation_projection_source(
                    capture, &tailer, &attempt, &achieved,
                )
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                Box::pin(self.publish_terminal_stream_claim(
                    capture,
                    &snapshot,
                    &tailer,
                    &attempt,
                    &effect,
                    &achieved,
                    projection,
                    &mut sidecar,
                ))
                .await
                .map_err(|error| WorkerOpenFailure::unclaimed(claim_open_worker_error(error)))?;
                self.refresh_coordinator_only().await.map_err(|error| {
                    WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                })?;
                let refreshed = self
                    .recapture_stream_claim_lane(capture, "terminal claim continuation")
                    .await
                    .map_err(|error| {
                        WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                    })?;
                drop(_table_guards);
                drop(_stream_token_guard);
                drop(_branch_guard);
                drop(_schema_guard);
                return Box::pin(
                    self.open_stream_writer_with_claim(&refreshed, claim_kind, actor_id),
                )
                .await;
            }
            if let Some(error) = writer_claim_error.take() {
                return Err(WorkerOpenFailure::unclaimed(MemWalWorkerError::Lance {
                    operation: "writer claim",
                    message: format!(
                        "{error}; exact terminal physical claim remains recovery-owned by {}",
                        sidecar.operation_id
                    ),
                }));
            }
            let claimed = claimed_writer.take().ok_or_else(|| {
                WorkerOpenFailure::unclaimed(MemWalWorkerError::InvalidState {
                    reason:
                        "writer claim invocation returned neither a claimed writer nor an error"
                            .to_string(),
                })
            })?;
            let mut opened = claimed
                .classify(
                    capture.head.dataset(),
                    &capture.full_path,
                    &capture.details,
                    operation.initial_writer_epoch,
                )
                .await?;
            let projection = match opened.current_generation_projection_source() {
                Ok(projection) => projection,
                Err(error) => {
                    return Err(WorkerOpenFailure::claimed(error, opened.into_claimed()));
                }
            };
            let terminal_result = Box::pin(self.publish_terminal_stream_claim(
                capture,
                &snapshot,
                &tailer,
                &attempt,
                &effect,
                &achieved,
                projection,
                &mut sidecar,
            ))
            .await;
            if let Err(error) = terminal_result {
                return Err(WorkerOpenFailure::claimed(
                    claim_open_worker_error(error),
                    opened.into_claimed(),
                ));
            }
            return Ok(opened);
        }
    }

    async fn publish_terminal_stream_claim(
        &self,
        capture: &StreamAuthorityCapture,
        snapshot: &crate::db::manifest::Snapshot,
        tailer: &WalTailer,
        attempt: &super::stream_lifecycle::PreparedClaimAttempt,
        effect: &crate::db::manifest::stream::ClaimAttemptEffect,
        achieved: &ClaimPhysicalPrestate,
        projection: CurrentGenerationProjectionSource,
        sidecar: &mut crate::db::manifest::RecoverySidecar,
    ) -> Result<()> {
        let key_plan = claim_wal_key_discovery_plan(
            attempt,
            effect,
            Arc::new(ArrowSchema::from(capture.head.dataset().schema())),
        )?;
        let keys = collect_claim_wal_segment_keys(tailer, &key_plan).await?;
        let token_dataset = snapshot.open_stream_token_authority().await?;
        let token_rows = if keys.is_empty() {
            BTreeMap::new()
        } else {
            stream_token_rows_for_keys(
                &token_dataset,
                snapshot.stream_token_authority(),
                capture.entry.identity,
                &keys,
            )
            .await?
        };
        let base_rows = if keys.is_empty() {
            BTreeMap::new()
        } else {
            lookup_base_stream_metadata_for_keys(
                capture.head.dataset(),
                capture.entry.identity,
                &keys,
            )
            .await?
        };
        let mut prior_token_by_key = BTreeMap::new();
        for logical_id in &keys {
            validate_authority_base_pair(
                capture.entry.identity,
                logical_id,
                token_rows.get(logical_id),
                base_rows.get(logical_id),
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            prior_token_by_key.insert(
                logical_id.clone(),
                token_rows.get(logical_id).map(|row| row.current_token),
            );
        }
        let schema = Arc::new(ArrowSchema::from(capture.head.dataset().schema()));
        let auth_plan = claim_wal_authentication_plan(
            attempt,
            effect,
            capture.txn.authority.schema_ir_hash.clone(),
            Arc::clone(&schema),
            prior_token_by_key,
        )?;
        let segment = authenticate_claim_wal_segment(tailer, &auth_plan).await?;
        let full_lww = match projection {
            CurrentGenerationProjectionSource::Empty => current_generation_lww_projection_digest(
                &attempt.operation,
                &capture.txn.authority.schema_ir_hash,
                Arc::clone(&schema),
                &[],
            )?,
            CurrentGenerationProjectionSource::Replay(batches) => {
                current_generation_lww_projection_digest(
                    &attempt.operation,
                    &capture.txn.authority.schema_ir_hash,
                    Arc::clone(&schema),
                    &batches,
                )?
            }
            CurrentGenerationProjectionSource::PreservePrior => {
                if segment.row_count != 0 {
                    return Err(OmniError::manifest_internal(
                        "a non-empty authenticated claim suffix cannot preserve the prior full-generation LWW projection",
                    ));
                }
                capture
                    .lifecycle
                    .authenticated_wal_tail
                    .lww_projection_digest
                    .clone()
            }
        };
        let attempt_chain = effect.next_attempt_chain_ref()?;
        let current_lifecycle = snapshot
            .stream_lifecycle(capture.entry.identity)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "terminal claim lost its manifest-selected lifecycle lane",
                )
            })?;
        let built = build_terminal_claim(
            &current_lifecycle.claim_receipt_chain,
            attempt,
            effect,
            &attempt_chain,
            &segment,
            &full_lww,
            achieved.replay_cursor,
            crate::db::now_micros()?,
        )?;
        let next_lifecycle = build_claim_adoption_row(current_lifecycle, &built)?;
        let records = [
            LifecycleLedgerRecord::ClaimAttemptEffect(effect.clone()),
            LifecycleLedgerRecord::ClaimReceipt(built.receipt.clone()),
        ];
        let outcome = self
            .commit_stream_claim_ledger(
                snapshot,
                sidecar,
                &records,
                effect.clone(),
                Some((built.receipt, next_lifecycle.clone())),
            )
            .await?;
        match outcome {
            RecoveryStreamClaimOutcomeV14::TerminalVisible { lifecycle, .. }
                if lifecycle == next_lifecycle =>
            {
                self.refresh_coordinator_only().await?;
                Ok(())
            }
            _ => Err(OmniError::manifest_internal(
                "terminal claim did not publish its exact lifecycle authority",
            )),
        }
    }

    async fn commit_stream_claim_ledger(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        sidecar: &mut crate::db::manifest::RecoverySidecar,
        records: &[LifecycleLedgerRecord],
        effect: crate::db::manifest::stream::ClaimAttemptEffect,
        terminal: Option<(
            crate::db::manifest::stream::ClaimReceipt,
            StreamLifecycleEntry,
        )>,
    ) -> Result<RecoveryStreamClaimOutcomeV14> {
        let selected = snapshot.open_stream_token_authority().await?;
        let staged =
            stage_lifecycle_ledger_records(selected, snapshot.stream_token_authority(), records)
                .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                snapshot.stream_token_authority(),
                &self.control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);
        match terminal {
            Some((receipt, next_lifecycle)) => {
                arm_stream_claim_terminal_sidecar_v14(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    effect,
                    receipt,
                    next_lifecycle,
                    planned_transaction,
                )
                .await?;
            }
            None => {
                arm_stream_claim_checkpoint_sidecar_v14(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    effect,
                    planned_transaction,
                )
                .await?;
            }
        }
        if let Ok(outcome) = self.storage().commit_staged_exact(token_head, staged).await {
            if !outcome.is_exact() {
                return Err(OmniError::recovery_required(
                    sidecar.operation_id.clone(),
                    "claim ledger participant committed a non-exact transaction",
                ));
            }
            let next_authority =
                stream_token_authority_entry_for_dataset(outcome.snapshot().dataset()).await?;
            confirm_stream_claim_sidecar_v14(
                self.root_uri(),
                self.storage_adapter(),
                sidecar,
                outcome.committed_transaction().clone(),
                next_authority.current_head_witness.clone(),
                next_authority,
            )
            .await?;
        }
        // Recovery's manifest publisher enters Lance's synchronous DataFusion
        // planning recursion during its first poll. Keep the admission/profile
        // and inner write guards in this parent while polling that deep stack
        // from a fresh engine-owned task; merely boxing this future does not
        // reset Tokio's worker stack.
        let root_uri = self.root_uri().to_string();
        let storage = Arc::clone(&self.storage);
        let snapshot = snapshot.clone();
        let sidecar = sidecar.clone();
        crate::instrumentation::spawn_with_query_io_probes(async move {
            complete_stream_claim_sidecar_v14(&root_uri, storage, &snapshot, &sidecar).await
        })
        .await
        .map_err(|error| {
            OmniError::Lance(format!(
                "stream claim recovery owner task failed before returning its exact outcome: {error}"
            ))
        })?
    }

    /// Discover the sole pending claim for this exact lifecycle lane and run
    /// the receipt-first lookup before the caller is allowed to mint a new
    /// claim occurrence.
    async fn exact_pending_stream_claim(
        &self,
        capture: &StreamAuthorityCapture,
        graph_identity_digest: &str,
    ) -> Result<
        Option<(
            crate::db::manifest::RecoverySidecar,
            super::stream_lifecycle::PreparedClaimAttempt,
        )>,
    > {
        let expected_lifecycle_operation_id = capture
            .lifecycle
            .drain
            .as_ref()
            .map(|drain| drain.drain_id.as_str());
        let mut exact = None;
        for sidecar in list_sidecars(self.root_uri(), self.storage_adapter()).await? {
            let Some(RecoveryProtocolV14::StreamClaim(protocol)) = sidecar.protocol_v14.as_deref()
            else {
                continue;
            };
            if protocol.admission_scope.identity != capture.entry.identity
                || protocol.admission_scope.binding_scope_id != capture.lifecycle.binding_scope_id
                || protocol.operation.lifecycle_operation_id.as_deref()
                    != expected_lifecycle_operation_id
            {
                continue;
            }
            let recovery_operation_id = protocol.operation.recovery_operation_id.clone();
            if exact.is_some() {
                return Err(OmniError::recovery_required(
                    recovery_operation_id,
                    "multiple pending StreamClaim sidecars own one lifecycle lane",
                ));
            }
            exact = Some(sidecar);
        }
        let Some(sidecar) = exact else {
            return Ok(None);
        };
        let (claim_id, recovery_operation_id) = match sidecar.protocol_v14.as_deref() {
            Some(RecoveryProtocolV14::StreamClaim(protocol)) => (
                protocol.operation.claim_id.clone(),
                protocol.operation.recovery_operation_id.clone(),
            ),
            _ => unreachable!("filtered exact StreamClaim sidecar"),
        };
        let continuation = lookup_stream_claim_continuation_v14(
            self.root_uri(),
            &self.storage,
            &capture.txn.base,
            graph_identity_digest,
            capture.entry.identity,
            &capture.lifecycle.binding_scope_id,
            &claim_id,
            &recovery_operation_id,
        )
        .await?
        .ok_or_else(|| {
            OmniError::recovery_required(
                recovery_operation_id,
                "exact pending StreamClaim disappeared during receipt-first lookup",
            )
        })?;
        let attempt = match continuation {
            RecoveryStreamClaimContinuationV14::Pending { attempt, .. } => attempt,
            RecoveryStreamClaimContinuationV14::TerminalVisible { .. } => {
                prepared_stream_claim_attempt_v14(&sidecar)?
            }
        };
        Ok(Some((sidecar, attempt)))
    }

    async fn selected_claim_receipt(
        &self,
        snapshot: &crate::db::manifest::Snapshot,
        lifecycle: &StreamLifecycleEntry,
    ) -> Result<crate::db::manifest::stream::ClaimReceipt> {
        let record_id = lifecycle
            .current_claim_receipt_id
            .as_deref()
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream fold/quiesce requires a selected current ClaimReceipt",
                )
            })?;
        let dataset = snapshot.open_stream_token_authority().await?;
        let record = lookup_lifecycle_ledger_record_by_id(
            &dataset,
            snapshot.stream_token_authority(),
            CLAIM_RECEIPT_TAG,
            record_id,
        )
        .await?
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "manifest-selected current ClaimReceipt is absent from selected token authority",
            )
        })?;
        let LifecycleLedgerRecord::ClaimReceipt(receipt) = record else {
            return Err(OmniError::manifest_internal(
                "current ClaimReceipt ID decoded another lifecycle-ledger family",
            ));
        };
        if receipt.record_id != record_id
            || lifecycle.claim_receipt_chain.head_record_id.as_deref() != Some(record_id)
        {
            return Err(OmniError::manifest_internal(
                "current ClaimReceipt does not equal the selected claim-chain head",
            ));
        }
        Ok(receipt)
    }

    async fn recapture_stream_claim_lane(
        &self,
        prior: &StreamAuthorityCapture,
        operation: &str,
    ) -> Result<StreamAuthorityCapture> {
        match prior.lifecycle.lifecycle {
            StreamLifecycle::Open => {
                self.capture_stream_authority(&prior.entry.table_key, operation)
                    .await
            }
            StreamLifecycle::Draining => {
                let drain_id = prior
                    .lifecycle
                    .drain
                    .as_ref()
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "DRAINING claim continuation lost its drain descriptor",
                        )
                    })?
                    .drain_id
                    .clone();
                self.capture_draining_stream_authority(&prior.entry.table_key, operation, &drain_id)
                    .await
            }
            StreamLifecycle::Sealed => Err(OmniError::manifest_internal(
                "SEALED stream cannot continue a writer claim",
            )),
        }
    }

    async fn validate_claimed_writer_for_capture(
        &self,
        writer: &ShardWriter,
        key: StreamWorkerKey,
        capture: &StreamAuthorityCapture,
    ) -> Result<()> {
        let manifest = writer
            .manifest()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| OmniError::manifest_internal("claimed stream shard has no manifest"))?;
        if writer.shard_id() != key.shard_id
            || manifest.shard_id != key.shard_id
            || manifest.status != ShardStatus::Active
            || manifest.writer_epoch != writer.epoch()
            || writer.epoch() != capture.epoch_floor
            || capture.lifecycle.current_claim_receipt_id.is_none()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_writer_epoch:{}", key.identity),
                Some(format!(
                    "{}:epoch={}:ACTIVE:selected-claim",
                    key.shard_id, capture.epoch_floor
                )),
                Some(format!(
                    "writer_shard={}:writer_epoch={}:manifest={manifest:?}:claim={:?}",
                    writer.shard_id(),
                    writer.epoch(),
                    capture.lifecycle.current_claim_receipt_id
                )),
            ));
        }
        let token_dataset = capture.txn.base.open_stream_token_authority().await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&capture.txn.authority.schema_identity_domain)?;
        super::stream_enrollment::validate_selected_lifecycle_ledger_authority(
            &token_dataset,
            capture.txn.base.stream_token_authority(),
            &graph_identity_digest,
            &capture.lifecycle,
        )
        .await
        .map(|_| ())
    }

    pub(super) async fn capture_stream_authority(
        &self,
        table_key: &str,
        operation: &str,
    ) -> Result<StreamAuthorityCapture> {
        self.capture_stream_authority_for_lifecycle(
            table_key,
            operation,
            StreamLifecycle::Open,
            None,
        )
        .await
    }

    /// Capture the exact DRAINING lane named by one durable drain descriptor.
    ///
    /// This is deliberately separate from ordinary OPEN admission. Drain-mode
    /// claim/fold orchestration must bind the complete DRAINING row and its
    /// operation ID; accepting "OPEN or DRAINING" here would let an ordinary
    /// fold silently weaken the lifecycle contract.
    pub(super) async fn capture_draining_stream_authority(
        &self,
        table_key: &str,
        operation: &str,
        drain_id: &str,
    ) -> Result<StreamAuthorityCapture> {
        self.capture_stream_authority_for_lifecycle(
            table_key,
            operation,
            StreamLifecycle::Draining,
            Some(drain_id),
        )
        .await
    }

    /// Capture one exact SEALED lane for the dedicated recovery-v15 resume
    /// path. Ordinary writer admission continues to call
    /// `capture_stream_authority` and therefore cannot opt into SEALED state.
    async fn capture_sealed_stream_authority(
        &self,
        table_key: &str,
        operation: &str,
    ) -> Result<StreamAuthorityCapture> {
        self.capture_stream_authority_for_lifecycle(
            table_key,
            operation,
            StreamLifecycle::Sealed,
            None,
        )
        .await
    }

    async fn capture_stream_authority_for_lifecycle(
        &self,
        table_key: &str,
        operation: &str,
        expected_lifecycle: StreamLifecycle,
        expected_drain_id: Option<&str>,
    ) -> Result<StreamAuthorityCapture> {
        let txn = self.open_write_txn(None).await?;
        let entry = txn.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "{operation} cannot resolve unknown table '{table_key}'"
            ))
        })?;
        if entry.table_branch.is_some() {
            return Err(OmniError::manifest_conflict(format!(
                "{operation} supports only the canonical main physical ref for '{table_key}'"
            )));
        }
        let lifecycle = txn
            .base
            .stream_lifecycle(entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "{operation} requires an enrolled stream for '{table_key}'"
                ))
            })?;
        let profile_mode = txn.base.stream_profile().mode();
        let profile_authorized = match expected_lifecycle {
            StreamLifecycle::Open => {
                profile_mode == crate::db::manifest::StreamProfileMode::Enabled
            }
            StreamLifecycle::Draining => matches!(
                profile_mode,
                crate::db::manifest::StreamProfileMode::Enabled
                    | crate::db::manifest::StreamProfileMode::Disabling
            ),
            // This arm is reachable only through the dedicated sealed capture
            // above. The normal OPEN capture remains the sole write-admission
            // path, so accepting ENABLED here does not create a generic
            // `allow_sealed` bypass.
            StreamLifecycle::Sealed => {
                profile_mode == crate::db::manifest::StreamProfileMode::Enabled
            }
        };
        if !profile_authorized {
            return Err(OmniError::StreamingRequiresClusterRuntime {
                mode: profile_mode.as_str().to_string(),
            });
        }
        if lifecycle.lifecycle != expected_lifecycle {
            return Err(OmniError::manifest_stream_lifecycle_conflict(
                entry.identity.stable_table_id,
                entry.identity.table_incarnation_id,
                table_key,
                lifecycle.lifecycle.as_str(),
                operation,
            ));
        }
        match (
            expected_lifecycle,
            lifecycle.drain.as_ref(),
            expected_drain_id,
        ) {
            (StreamLifecycle::Open, None, None) => {}
            (StreamLifecycle::Draining, Some(drain), Some(expected))
                if drain.drain_id == expected => {}
            (StreamLifecycle::Sealed, None, None) => {}
            (StreamLifecycle::Draining, Some(_), Some(_)) => {
                return Err(OmniError::manifest_read_set_changed(
                    format!("{operation}:stream_drain:{table_key}"),
                    expected_drain_id.map(str::to_string),
                    lifecycle.drain.as_ref().map(|drain| drain.drain_id.clone()),
                ));
            }
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "{operation} requested an incoherent lifecycle/drain capture for '{table_key}'"
                )));
            }
        }
        if lifecycle.identity != entry.identity
            || lifecycle.binding.table_location != entry.table_path
            || lifecycle.binding.table_branch.is_some()
            || lifecycle.current_head_witness.table_version != entry.table_version
        {
            return Err(OmniError::manifest_internal(format!(
                "{operation} observed incoherent stream lifecycle authority for '{table_key}'"
            )));
        }
        let branches = self.coordinator.read().await.branch_list().await?;
        if branches.iter().any(|branch| branch != "main") {
            return Err(OmniError::manifest_conflict(format!(
                "{operation} requires the bounded main-only stream topology"
            )));
        }

        let full_path = format!(
            "{}/{}",
            self.root_uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        let head = self.storage().open_dataset_head(&full_path, None).await?;
        let witness = capture_current_head_witness(head.dataset())
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if witness != lifecycle.current_head_witness {
            return Err(OmniError::manifest_read_set_changed(
                format!("{operation}:stream_head:{table_key}"),
                Some(format!("{:?}", lifecycle.current_head_witness)),
                Some(format!("{witness:?}")),
            ));
        }
        let details = head
            .dataset()
            .mem_wal_index_details()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "{operation} lifecycle exists without a MemWAL index for '{table_key}'"
                ))
            })?;
        let binding = lifecycle.binding.clone();
        let (enrollment_id, shard_id) =
            validate_stream_config_v3_binding(&details, &binding).map_err(worker_error)?;
        let token_dataset = txn.base.open_stream_token_authority().await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&txn.authority.schema_identity_domain)?;
        let retained_shard_inventory =
            super::stream_enrollment::validate_selected_lifecycle_ledger_authority(
                &token_dataset,
                txn.base.stream_token_authority(),
                &graph_identity_digest,
                &lifecycle,
            )
            .await?;
        // The selected BindingReceipt already authenticates retained history by
        // one fixed-size commitment. Ordinary authority capture needs only the
        // current index/shard; old shard prefixes are inert and their complete
        // inventory is checked at cold-open, rebind, and recovery boundaries.
        validate_b1_lifecycle_current_binding_physical_state(
            head.dataset(),
            &lifecycle,
            retained_shard_inventory.as_ref(),
        )
        .await
        .map_err(worker_error)?;
        let worker_key =
            StreamWorkerKey::new(entry.identity, enrollment_id, shard_id).map_err(worker_error)?;
        let epoch_floor = lifecycle
            .epoch_floor_by_shard
            .get(&shard_id.to_string())
            .copied()
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "{operation} lifecycle has no epoch floor for bound shard {shard_id}"
                ))
            })?;
        let admission_key = StreamAdmissionKey::for_resolved_ref(entry.identity, None);
        Ok(StreamAuthorityCapture {
            txn,
            entry,
            lifecycle,
            binding,
            worker_key,
            admission_key,
            shard_id,
            enrollment_id,
            retained_shard_inventory,
            epoch_floor,
            full_path,
            head,
            details,
        })
    }

    fn validate_stream_admission_batch(
        &self,
        capture: &StreamAuthorityCapture,
        batch: &RecordBatch,
    ) -> Result<()> {
        validate_stream_stored_bounds(&capture.entry.table_key, batch)?;
        let expected: ArrowSchema = capture.head.dataset().schema().into();
        if batch.schema().as_ref() != &expected {
            return Err(OmniError::manifest(format!(
                "stream batch schema for '{}' does not exactly match its accepted physical schema",
                capture.entry.table_key
            )));
        }
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if !field.is_nullable() && column.null_count() != 0 {
                return Err(OmniError::manifest(format!(
                    "stream batch has nulls in required field '{}'",
                    field.name()
                )));
            }
        }
        self.storage()
            .validate_keyed_write_batch(&capture.entry.table_key, batch)?;

        validate_stream_value_constraints(&capture.entry.table_key, batch, &capture.txn.catalog)
    }

    fn validate_stream_logical_admission_batch(
        &self,
        capture: &StreamAuthorityCapture,
        batch: &RecordBatch,
    ) -> Result<()> {
        validate_stream_input_bounds(&capture.entry.table_key, batch)?;
        let expected_physical: ArrowSchema = capture.head.dataset().schema().into();
        let expected_fields = expected_physical
            .fields()
            .iter()
            .filter(|field| field.name() != crate::db::STREAM_METADATA_COLUMN)
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let expected =
            ArrowSchema::new_with_metadata(expected_fields, expected_physical.metadata().clone());
        if batch.schema().as_ref() != &expected {
            return Err(OmniError::manifest(format!(
                "stream batch schema for '{}' does not exactly match its accepted logical schema",
                capture.entry.table_key
            )));
        }
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if !field.is_nullable() && column.null_count() != 0 {
                return Err(OmniError::manifest(format!(
                    "stream batch has nulls in required field '{}'",
                    field.name()
                )));
            }
        }
        self.storage()
            .validate_keyed_write_batch(&capture.entry.table_key, batch)?;

        validate_stream_value_constraints(&capture.entry.table_key, batch, &capture.txn.catalog)
    }

    async fn ensure_no_relevant_stream_sidecar(
        &self,
        identity: TableIdentity,
        operation: &str,
    ) -> Result<()> {
        let sidecars = list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = sidecars.iter().find(|sidecar| {
            sidecar.writer_kind.is_graph_global_barrier()
                || sidecar.tables.iter().any(|pin| pin.identity == identity)
        }) {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                format!(
                    "pending {:?} recovery operation overlaps table identity {} and blocks {operation}",
                    sidecar.writer_kind, identity
                ),
            ));
        }
        Ok(())
    }

    /// Refuse every overlapping recovery owner except the one exact
    /// lifecycle-v3 claim continuation for this already-admitted lane.
    ///
    /// Cold writer open is itself the only component capable of classifying
    /// and continuing an `AttemptArmed` claim. Applying the generic barrier
    /// here would make that recovery owner permanently self-blocking. The
    /// exception is intentionally narrow: same immutable table identity,
    /// binding scope, and active lifecycle operation (none for OPEN, exact
    /// drain ID for DRAINING), with at most one matching sidecar.
    async fn ensure_no_relevant_stream_sidecar_except_exact_claim(
        &self,
        capture: &StreamAuthorityCapture,
        operation: &str,
    ) -> Result<()> {
        let expected_lifecycle_operation_id = capture
            .lifecycle
            .drain
            .as_ref()
            .map(|drain| drain.drain_id.as_str());
        let sidecars = list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        let mut exact_claim = None;
        for sidecar in &sidecars {
            let relevant = sidecar.writer_kind.is_graph_global_barrier()
                || sidecar
                    .tables
                    .iter()
                    .any(|pin| pin.identity == capture.entry.identity);
            if !relevant {
                continue;
            }
            let is_exact_claim = matches!(
                sidecar.protocol_v14.as_deref(),
                Some(RecoveryProtocolV14::StreamClaim(protocol))
                    if protocol.admission_scope.identity == capture.entry.identity
                        && protocol.admission_scope.binding_scope_id
                            == capture.lifecycle.binding_scope_id
                        && protocol.operation.lifecycle_operation_id.as_deref()
                            == expected_lifecycle_operation_id
            );
            if is_exact_claim && exact_claim.replace(sidecar.operation_id.as_str()).is_none() {
                continue;
            }
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                format!(
                    "pending {:?} recovery operation overlaps table identity {} and blocks {operation}",
                    sidecar.writer_kind, capture.entry.identity
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn ensure_stream_table_admission_supported(
        catalog: &omnigraph_compiler::catalog::Catalog,
        table_key: &str,
    ) -> Result<()> {
        let blob_properties = if let Some(type_name) = table_key.strip_prefix("node:") {
            &catalog
                .node_types
                .get(type_name)
                .ok_or_else(|| OmniError::manifest(format!("unknown node type '{type_name}'")))?
                .blob_properties
        } else if let Some(type_name) = table_key.strip_prefix("edge:") {
            &catalog
                .edge_types
                .get(type_name)
                .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{type_name}'")))?
                .blob_properties
        } else {
            return Err(OmniError::manifest(format!(
                "invalid stream table key '{table_key}'"
            )));
        };
        if !blob_properties.is_empty() {
            return Err(OmniError::manifest(format!(
                "stream admission for Blob-bearing table '{table_key}' is not active: \
                 Lance MemWAL fold cannot materialize Blob values"
            )));
        }
        Ok(())
    }

    /// One feature-gated graph integration seam.  `Some(batch)` performs a put
    /// beginning at `caller_ordinal_start`; `None` performs the explicit fold.
    /// It intentionally returns no WAL/generation coordinate or durability
    /// receipt to external tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_b1_for_test(
        self: &Arc<Self>,
        table_key: &str,
        batch: Option<RecordBatch>,
        caller_ordinal_start: u64,
    ) -> Result<()> {
        match batch {
            Some(batch) => {
                let rows = u64::try_from(batch.num_rows()).map_err(|_| {
                    OmniError::manifest_internal("stream test batch row count exceeds u64")
                })?;
                let end = caller_ordinal_start
                    .checked_add(
                        rows.checked_sub(1)
                            .ok_or_else(|| OmniError::manifest("stream batch must be non-empty"))?,
                    )
                    .ok_or_else(|| {
                        OmniError::manifest_internal("stream caller ordinal range overflow")
                    })?;
                let ordinals =
                    CallerOrdinalRange::new(caller_ordinal_start, end).map_err(worker_error)?;
                Box::pin(self.stream_put_phase_b1(table_key, batch, ordinals))
                    .await
                    .map(|_| ())
            }
            None => Box::pin(self.stream_fold_phase_b1(table_key)).await,
        }
    }

    /// Feature-gated lifecycle seam for exact `OPEN -> DRAINING -> SEALED`
    /// integration and crash/restart tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_quiesce_for_test(
        self: &Arc<Self>,
        table_key: &str,
        drain_id: &str,
        expected_lifecycle_revision: u64,
        actor_id: &str,
    ) -> Result<()> {
        Box::pin(self.stream_quiesce_as(table_key, drain_id, expected_lifecycle_revision, actor_id))
            .await
    }

    /// Feature-gated lifecycle seam for recovery-v15 `SEALED -> OPEN` resume
    /// and guarded `DRAINING -> OPEN` abort tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_resume_for_test(
        self: &Arc<Self>,
        table_key: &str,
        resume_id: &str,
        expected_lifecycle_revision: u64,
        abort_drain: bool,
        actor_id: &str,
    ) -> Result<()> {
        let mode = if abort_drain {
            StreamResumeMode::AbortDrain
        } else {
            StreamResumeMode::ResumeSealed
        };
        Box::pin(self.stream_resume_as(
            table_key,
            resume_id,
            expected_lifecycle_revision,
            mode,
            actor_id,
        ))
        .await
    }

    /// Feature-gated proof seam for one private B2 compare-and-chain row.
    /// It intentionally accepts/returns only wire strings so protocol types do
    /// not become public SDK surface.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub async fn failpoint_stream_b2_for_test(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        stream_incarnation_id: &str,
        write_id: &str,
        predecessor_token: Option<&str>,
        contributor_id: &str,
    ) -> Result<String> {
        let predecessor_token = predecessor_token
            .map(str::parse::<StreamToken>)
            .transpose()
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let contributor_id = TrustedContributorId::new(contributor_id.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let ack = self
            .stream_put_phase_b2_one(
                table_key,
                batch,
                caller_ordinal,
                StreamWriteEnvelope {
                    stream_incarnation_id: stream_incarnation_id.to_string(),
                    write_id: write_id.to_string(),
                    predecessor_token,
                },
                contributor_id,
            )
            .await?;
        Ok(ack.stream_token.to_string())
    }

    /// Feature-gated proof seam for the authenticated one-row ingress
    /// boundary. Wire strings keep private protocol types out of the SDK while
    /// the boolean pins new durability versus an exact durable retry.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_ingest_one_as_for_test(
        self: &Arc<Self>,
        table_key: &str,
        raw_json: &[u8],
        caller_ordinal: u64,
        actor_id: &str,
    ) -> Result<(String, bool)> {
        let ack = self
            .stream_ingest_one_as(table_key, raw_json, caller_ordinal, actor_id)
            .await?;
        Ok((ack.stream_token.to_string(), ack.already_durable))
    }

    /// Return the exact logical stream incarnation for private protocol tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_incarnation_for_test(&self, table_key: &str) -> Result<String> {
        let capture = self
            .capture_stream_authority(table_key, "stream incarnation test probe")
            .await?;
        Ok(capture.lifecycle.enrollment_receipt.stream_incarnation_id)
    }
}

enum FoldAttempt {
    Published,
    EffectFree(OmniError),
}

async fn lookup_base_stream_metadata(
    dataset: &lance::Dataset,
    identity: TableIdentity,
    logical_id: &str,
) -> Result<Option<TrustedStreamRowMetadata>> {
    let logical_ids = std::collections::BTreeSet::from([logical_id.to_string()]);
    let mut selected =
        lookup_base_stream_metadata_for_keys(dataset, identity, &logical_ids).await?;
    Ok(selected.remove(logical_id))
}

pub(super) async fn lookup_base_stream_metadata_for_keys(
    dataset: &lance::Dataset,
    identity: TableIdentity,
    logical_ids: &std::collections::BTreeSet<String>,
) -> Result<std::collections::BTreeMap<String, TrustedStreamRowMetadata>> {
    if logical_ids.is_empty() || logical_ids.len() > B1_MAX_GENERATION_ROWS as usize {
        return Err(OmniError::manifest_internal(format!(
            "base stream-metadata lookup requires 1..={B1_MAX_GENERATION_ROWS} exact keys, got {}",
            logical_ids.len()
        )));
    }
    let mut scanner = dataset.scan();
    scanner
        .project(&["id", crate::db::STREAM_METADATA_COLUMN])
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    scanner.filter_expr(col("id").in_list(logical_ids.iter().cloned().map(lit).collect(), false));
    scanner.batch_size(logical_ids.len().saturating_add(1));
    scanner.batch_size_bytes(B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
    scanner
        .limit(
            Some(
                i64::try_from(logical_ids.len().saturating_add(1)).map_err(|_| {
                    OmniError::manifest_internal(
                        "base stream-metadata lookup row limit exceeds i64",
                    )
                })?,
            ),
            None,
        )
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut seen = std::collections::BTreeSet::new();
    let mut selected = std::collections::BTreeMap::new();
    let mut retained_bytes = 0_u64;
    let mut observed_rows = 0_usize;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| OmniError::manifest_internal("base stream lookup row overflow"))?;
        if observed_rows > logical_ids.len() {
            return Err(OmniError::manifest_internal(
                "base stream-metadata lookup returned more than one row per requested key",
            ));
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("base stream lookup batch Arrow size exceeds u64")
        })?;
        if batch_bytes > B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "base_stream_lookup_batch_arrow_bytes",
                B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                batch_bytes,
            ));
        }
        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "base stream-metadata probe returned no exact Utf8 id column",
                )
            })?;
        let metadata = batch
            .column_by_name(crate::db::STREAM_METADATA_COLUMN)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "base stream-metadata probe omitted reserved column '{}'",
                    crate::db::STREAM_METADATA_COLUMN
                ))
            })?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) || !logical_ids.contains(ids.value(row)) {
                return Err(OmniError::manifest_internal(
                    "base stream-metadata exact-id probe returned a foreign row",
                ));
            }
            let logical_id = ids.value(row);
            if !seen.insert(logical_id.to_string()) {
                return Err(OmniError::manifest_internal(format!(
                    "base table contains duplicate exact-id rows for '{logical_id}'"
                )));
            }
            retained_bytes = add_stream_lookup_retained_bytes(
                "base_stream_lookup_retained_bytes",
                retained_bytes,
                u64::try_from(
                    std::mem::size_of::<String>()
                        .saturating_add(logical_id.len())
                        .saturating_add(256),
                )
                .map_err(|_| OmniError::manifest_internal("base stream key bytes exceed u64"))?,
                B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
            )?;
            let decoded = decode_trusted_stream_metadata(metadata.as_ref(), row)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            if let Some(decoded) = &decoded {
                decoded
                    .validate_for(identity, logical_id)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            }
            if let Some(decoded) = decoded {
                retained_bytes = add_stream_lookup_retained_bytes(
                    "base_stream_lookup_retained_bytes",
                    retained_bytes,
                    decoded
                        .lookup_retained_bytes(logical_id)
                        .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                    B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                )?;
                selected.insert(logical_id.to_string(), decoded);
            }
        }
    }
    Ok(selected)
}

#[derive(Debug, Clone, Copy)]
struct ClaimPhysicalPrestate {
    shard_manifest_version: u64,
    writer_epoch: u64,
    replay_cursor: u64,
    current_generation: u64,
    base_merged_generation: u64,
}

async fn read_claim_physical_prestate(
    capture: &StreamAuthorityCapture,
) -> Result<ClaimPhysicalPrestate> {
    let observed = read_claim_physical_prestate_after_attempt(capture).await?;
    if observed.writer_epoch != capture.epoch_floor {
        return Err(OmniError::manifest_read_set_changed(
            format!("stream_claim_physical_prestate:{}", capture.entry.table_key),
            Some(format!(
                "{}:epoch={}:ACTIVE",
                capture.shard_id, capture.epoch_floor
            )),
            Some(format!("{observed:?}")),
        ));
    }
    Ok(observed)
}

async fn read_claim_physical_prestate_after_attempt(
    capture: &StreamAuthorityCapture,
) -> Result<ClaimPhysicalPrestate> {
    let object_store = capture
        .head
        .dataset()
        .object_store(None)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let manifest = ShardManifestStore::new(
        object_store,
        &capture.head.dataset().branch_location().path,
        capture.shard_id,
        2,
    )
    .read_latest()
    .await
    .map_err(|error| OmniError::Lance(error.to_string()))?
    .ok_or_else(|| OmniError::manifest_internal("stream claim shard has no manifest"))?;
    let merged = capture
        .details
        .merged_generations
        .iter()
        .filter(|merged| merged.shard_id == capture.shard_id)
        .map(|merged| merged.generation)
        .collect::<Vec<_>>();
    let base_merged_generation = match merged.as_slice() {
        [] => 0,
        [generation] => *generation,
        _ => {
            return Err(OmniError::manifest_internal(
                "stream claim observed multiple merged cursors for its one bound shard",
            ));
        }
    };
    if capture
        .details
        .merged_generations
        .iter()
        .any(|merged| merged.shard_id != capture.shard_id)
        || manifest.shard_id != capture.shard_id
        || manifest.status != ShardStatus::Active
        || manifest.current_generation < base_merged_generation
    {
        return Err(OmniError::manifest_internal(format!(
            "stream claim physical prestate violates its bound shard topology: {manifest:?}"
        )));
    }
    Ok(ClaimPhysicalPrestate {
        shard_manifest_version: manifest.version,
        writer_epoch: manifest.writer_epoch,
        replay_cursor: manifest.replay_after_wal_entry_position,
        current_generation: manifest.current_generation,
        base_merged_generation,
    })
}

/// Prove an abortable DRAINING lane has no physical cut which would need a
/// fold before a new epoch is claimed. Lifecycle authority authenticates the
/// last selected claim, but later WAL puts can advance the shard cursor before
/// another lifecycle publication. Arming resume first would strand that cut
/// behind its own overlapping sidecar, so this check is deliberately
/// effect-free and precedes sidecar arm.
async fn ensure_abort_drain_physical_cut_is_empty(
    capture: &StreamAuthorityCapture,
    tailer: &WalTailer,
    physical: ClaimPhysicalPrestate,
) -> Result<()> {
    if physical.replay_cursor != capture.lifecycle.authenticated_wal_tail.position {
        return Err(OmniError::manifest_conflict(format!(
            "stream abort-drain requires an empty physical cut; shard replay cursor {} differs from authenticated lifecycle position {}",
            physical.replay_cursor, capture.lifecycle.authenticated_wal_tail.position
        )));
    }
    let passive = validate_b1_lifecycle_physical_state_with_binding_inventory(
        capture.head.dataset(),
        &capture.lifecycle,
        capture.retained_shard_inventory.as_ref(),
    )
    .await
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    match passive {
        PassiveB1PhysicalState::AdmitOrReplay {
            shard_manifest_version,
            current_generation,
            replay_after_wal_entry_position,
            writer_epoch,
        } if shard_manifest_version == physical.shard_manifest_version
            && current_generation == physical.current_generation
            && replay_after_wal_entry_position == physical.replay_cursor
            && writer_epoch == physical.writer_epoch =>
        {
            Ok(())
        }
        PassiveB1PhysicalState::FoldOnlyFlushed(_) => Err(OmniError::manifest_conflict(
            "stream abort-drain requires an empty physical cut; an unmerged flushed generation remains",
        )),
        observed => Err(OmniError::manifest_read_set_changed(
            format!(
                "stream_abort_drain_physical_cut:{}",
                capture.entry.table_key
            ),
            Some(format!("{physical:?}")),
            Some(format!("{observed:?}")),
        )),
    }?;

    let expected_next_position = physical
        .replay_cursor
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream abort-drain WAL cursor overflow"))?;
    let observed_successor = tailer
        .read_entry(expected_next_position)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    if observed_successor.is_some() {
        return Err(OmniError::manifest_conflict(format!(
            "stream abort-drain requires an empty physical cut; retained WAL contains successor position {expected_next_position}"
        )));
    }
    Ok(())
}

fn validate_stream_resume_profile_authority(
    snapshot: &crate::db::manifest::Snapshot,
) -> Result<()> {
    let profile = snapshot.stream_profile();
    profile.validate()?;
    if profile.mode() != crate::db::manifest::StreamProfileMode::Enabled {
        return Err(OmniError::StreamingRequiresClusterRuntime {
            mode: profile.mode().as_str().to_string(),
        });
    }
    Ok(())
}

fn validate_selected_quiesce_receipt(
    lifecycle: &StreamLifecycleEntry,
    receipt: &ManagementReceipt,
    graph_identity_digest: &str,
    drain_id: &str,
    expected_lifecycle_revision: u64,
    actor_id: &str,
) -> Result<()> {
    if receipt.graph_identity_digest != graph_identity_digest
        || receipt.identity != lifecycle.identity
        || receipt.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
        || receipt.binding_scope_id != lifecycle.binding_scope_id
        || receipt.operation_kind != "QUIESCE"
        || receipt.operation_id != drain_id
        || receipt.from_revision != expected_lifecycle_revision
        || receipt.actor_id != actor_id
    {
        return Err(OmniError::StreamLifecycleIdempotencyConflict {
            stable_table_id: lifecycle.identity.stable_table_id,
            table_incarnation_id: lifecycle.identity.table_incarnation_id,
            operation_kind: "QUIESCE".to_string(),
            operation_id: drain_id.to_string(),
        });
    }
    receipt.validate(receipt.to_revision)?;
    if receipt.result_payload != stream_quiesce_result_payload(receipt.to_revision)? {
        return Err(OmniError::manifest_internal(
            "selected terminal quiesce receipt has a noncanonical result payload",
        ));
    }
    validate_selected_management_receipt_progress(lifecycle, receipt, StreamLifecycle::Sealed)
}

async fn claim_wal_tailer(capture: &StreamAuthorityCapture) -> Result<WalTailer> {
    let object_store = capture
        .head
        .dataset()
        .object_store(None)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    Ok(WalTailer::new(
        object_store,
        capture.head.dataset().branch_location().path.clone(),
        capture.shard_id,
    ))
}

/// Rebuild the complete bounded active-generation projection after a process
/// died with an `AttemptArmed` claim. Lance's retain-all profile keeps every
/// canonical WAL object, and the claim operation fixed the exact replay cursor
/// before its first physical invocation. Reading that closed cursor range
/// recovers the same logical replay batches a live `mem_wal_writer` returned;
/// empty fence entries add no memory and data rows remain bounded by the
/// one-generation row/Arrow limits enforced by the projection validator.
async fn recovered_current_generation_projection_source(
    capture: &StreamAuthorityCapture,
    tailer: &WalTailer,
    attempt: &super::stream_lifecycle::PreparedClaimAttempt,
    achieved: &ClaimPhysicalPrestate,
) -> Result<CurrentGenerationProjectionSource> {
    let physical = validate_b1_lifecycle_physical_state_with_binding_inventory(
        capture.head.dataset(),
        &capture.lifecycle,
        capture.retained_shard_inventory.as_ref(),
    )
    .await
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    match physical {
        PassiveB1PhysicalState::FoldOnlyFlushed(flushed) => {
            if flushed.shard_manifest_version != achieved.shard_manifest_version
                || flushed.writer_epoch != achieved.writer_epoch
                || flushed.replay_after_wal_entry_position != achieved.replay_cursor
                || achieved.replay_cursor != attempt.planned_sentinel_position
            {
                return Err(OmniError::recovery_required(
                    attempt.operation.recovery_operation_id.clone(),
                    format!(
                        "flushed current-generation authority differs from the exact recovered claim outcome: flushed={flushed:?}, achieved={achieved:?}"
                    ),
                ));
            }
            let batches = scan_flushed_generation_projection(
                capture.head.dataset(),
                &capture.full_path,
                capture.shard_id,
                &flushed,
            )
            .await
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            return Ok(if batches.is_empty() {
                CurrentGenerationProjectionSource::Empty
            } else {
                CurrentGenerationProjectionSource::Replay(batches)
            });
        }
        PassiveB1PhysicalState::AdmitOrReplay {
            shard_manifest_version,
            replay_after_wal_entry_position,
            writer_epoch,
            ..
        } if shard_manifest_version == achieved.shard_manifest_version
            && replay_after_wal_entry_position == achieved.replay_cursor
            && writer_epoch == achieved.writer_epoch
            && achieved.replay_cursor == attempt.planned_sentinel_position => {}
        passive => {
            return Err(OmniError::recovery_required(
                attempt.operation.recovery_operation_id.clone(),
                format!(
                    "active-generation authority differs from the exact recovered claim outcome: passive={passive:?}, achieved={achieved:?}"
                ),
            ));
        }
    }

    let first = attempt
        .operation
        .initial_replay_cursor
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream replay cursor overflow"))?;
    let mut batches = Vec::new();
    for position in first..=attempt.planned_sentinel_position {
        let entry = tailer
            .read_entry(position)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
            .ok_or_else(|| {
                OmniError::recovery_required(
                    attempt.operation.recovery_operation_id.clone(),
                    format!(
                        "retained WAL has a gap at position {position} while rebuilding the current generation"
                    ),
                )
            })?;
        if entry.shard_id.to_string() != attempt.operation.shard_id
            || entry.entry_position != position
        {
            return Err(OmniError::recovery_required(
                attempt.operation.recovery_operation_id.clone(),
                format!("retained WAL entry {position} belongs to another shard or position"),
            ));
        }
        batches.extend(entry.batches);
    }
    Ok(if batches.is_empty() {
        CurrentGenerationProjectionSource::Empty
    } else {
        CurrentGenerationProjectionSource::Replay(batches)
    })
}

async fn prepare_next_claim_attempt(
    operation: &super::stream_lifecycle::PreparedClaimOperation,
    tailer: &WalTailer,
    prestate: ClaimPhysicalPrestate,
) -> Result<super::stream_lifecycle::PreparedClaimAttempt> {
    let planned_sentinel_position = tailer
        .next_position()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let planned_writer_epoch = prestate
        .writer_epoch
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream writer epoch overflow"))?;
    prepare_claim_attempt(
        operation,
        ClaimAttemptRequest {
            attempt_id: ShardId::new_v4().to_string(),
            pre_shard_manifest_version: prestate.shard_manifest_version,
            pre_writer_epoch: prestate.writer_epoch,
            pre_replay_cursor: prestate.replay_cursor,
            planned_sentinel_position,
            planned_writer_epoch,
            storage_envelope_digest: None,
        },
    )
}

async fn observe_claim_attempt(
    capture: &StreamAuthorityCapture,
    tailer: &WalTailer,
    attempt: &super::stream_lifecycle::PreparedClaimAttempt,
) -> Result<(ClaimAttemptEvidence, ClaimPhysicalPrestate)> {
    let achieved = read_claim_physical_prestate_after_attempt(capture).await?;
    let sentinel = tailer
        .read_entry(attempt.planned_sentinel_position)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    if achieved.shard_manifest_version == attempt.pre_shard_manifest_version
        && achieved.writer_epoch == attempt.pre_writer_epoch
        && achieved.replay_cursor == attempt.pre_replay_cursor
        && sentinel.is_none()
    {
        return Ok((ClaimAttemptEvidence::NoEffect, achieved));
    }
    if achieved.shard_manifest_version <= attempt.pre_shard_manifest_version
        || achieved.writer_epoch != attempt.planned_writer_epoch
    {
        return Err(OmniError::recovery_required(
            attempt.operation.recovery_operation_id.clone(),
            format!(
                "stream claim physical outcome differs from its exact plan: achieved={achieved:?}"
            ),
        ));
    }
    match sentinel {
        None => Ok((
            ClaimAttemptEvidence::StockManifestOnly {
                achieved_shard_manifest_version: achieved.shard_manifest_version,
                achieved_writer_epoch: achieved.writer_epoch,
            },
            achieved,
        )),
        Some(entry)
            if entry.shard_id == capture.shard_id
                && entry.entry_position == attempt.planned_sentinel_position
                && entry.writer_epoch == attempt.planned_writer_epoch
                && entry.batches.is_empty() =>
        {
            Ok((
                ClaimAttemptEvidence::StockManifestPlusSentinel {
                    achieved_shard_manifest_version: achieved.shard_manifest_version,
                    achieved_writer_epoch: achieved.writer_epoch,
                },
                achieved,
            ))
        }
        Some(entry) => Err(OmniError::recovery_required(
            attempt.operation.recovery_operation_id.clone(),
            format!(
                "stream claim planned sentinel is foreign or data-bearing: position={}, epoch={}, batches={}",
                entry.entry_position,
                entry.writer_epoch,
                entry.batches.len()
            ),
        )),
    }
}

fn claim_open_worker_error(error: OmniError) -> MemWalWorkerError {
    MemWalWorkerError::InvalidState {
        reason: error.to_string(),
    }
}

fn worker_open_failure_preserving_claim(
    error: MemWalWorkerError,
    claimed: &mut Option<ClaimedMemWalWorker>,
) -> WorkerOpenFailure {
    match claimed.take() {
        Some(claimed) => WorkerOpenFailure::claimed(error, claimed),
        None => WorkerOpenFailure::unclaimed(error),
    }
}

fn worker_error(error: MemWalWorkerError) -> OmniError {
    OmniError::Lance(error.to_string())
}

fn stream_data_block_error(block_token: &str) -> OmniError {
    OmniError::StreamDataBlocked {
        block_token: block_token.to_string(),
    }
}

fn validate_stream_value_constraints(
    table_key: &str,
    batch: &RecordBatch,
    catalog: &omnigraph_compiler::catalog::Catalog,
) -> Result<()> {
    let mut changeset = ChangeSet::new();
    changeset.insert(
        table_key.to_string(),
        TableChange {
            added: vec![batch.clone()],
            changed: Vec::new(),
            deleted_ids: Vec::new(),
        },
    );
    if let Some(violation) = crate::validate::evaluate_value_constraints(&changeset, catalog)
        .into_iter()
        .next()
    {
        return Err(violation.into_omni_error());
    }
    Ok(())
}

fn validate_stream_input_bounds(table_key: &str, batch: &RecordBatch) -> Result<()> {
    if batch.num_rows() == 0 {
        return Err(OmniError::manifest("stream batch must be non-empty"));
    }
    if batch
        .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
        .is_some()
    {
        return Err(OmniError::manifest(format!(
            "stream batch may not supply reserved column '{}'",
            lance::dataset::mem_wal::TOMBSTONE
        )));
    }
    if batch
        .column_by_name(crate::db::STREAM_METADATA_COLUMN)
        .is_some()
    {
        return Err(OmniError::manifest(format!(
            "stream caller may not supply reserved column '{}'",
            crate::db::STREAM_METADATA_COLUMN
        )));
    }
    validate_stream_stored_bounds(table_key, batch)
}

fn validate_stream_stored_bounds(table_key: &str, batch: &RecordBatch) -> Result<()> {
    if batch.num_rows() == 0 {
        return Err(OmniError::manifest("stream batch must be non-empty"));
    }
    if batch
        .column_by_name(lance::dataset::mem_wal::TOMBSTONE)
        .is_some()
    {
        return Err(OmniError::manifest(format!(
            "stream batch may not supply reserved column '{}'",
            lance::dataset::mem_wal::TOMBSTONE
        )));
    }
    let charge = b1_input_accounting(batch).map_err(worker_error)?;
    if !charge.fits() {
        if batch.num_rows() == 1 {
            return Err(OmniError::resource_limit(
                "stream_input_arrow_bytes",
                B1_MAX_GENERATION_ARROW_BYTES,
                charge.arrow_bytes,
            ));
        }
        return Err(OmniError::FoldRequired {
            table_key: table_key.to_string(),
            rows: charge.rows,
            bytes: charge.arrow_bytes,
        });
    }
    Ok(())
}

fn validate_generation_token_plan(table_key: &str, rows: &[StreamTokenAuthorityRow]) -> Result<()> {
    match validate_stream_token_plan_bounds(rows) {
        Ok(()) => Ok(()),
        Err(OmniError::ResourceLimitExceeded { actual, .. }) => Err(OmniError::FoldRequired {
            table_key: table_key.to_string(),
            rows: u64::try_from(rows.len()).unwrap_or(u64::MAX),
            bytes: actual,
        }),
        Err(error) => Err(error),
    }
}

async fn longest_fitting_token_prefix<F, Fut>(
    candidate_rows: usize,
    mut validate: F,
) -> Result<usize>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<Option<OmniError>>>,
{
    if candidate_rows == 0 || candidate_rows > STREAM_B2_CLASSIFICATION_WINDOW_ROWS {
        return Err(OmniError::manifest_internal(format!(
            "B2 exact token-prefix scan requires 1..={STREAM_B2_CLASSIFICATION_WINDOW_ROWS} candidates, got {candidate_rows}"
        )));
    }

    let mut capacity_error = None;
    for prefix in (1..=candidate_rows).rev() {
        match validate(prefix).await? {
            None => return Ok(prefix),
            Some(error @ OmniError::FoldRequired { .. }) => {
                capacity_error = Some(error);
            }
            Some(error) => {
                return Err(OmniError::manifest_internal(format!(
                    "B2 token-prefix validator returned a non-capacity outcome: {error}"
                )));
            }
        }
    }
    Err(capacity_error.unwrap_or_else(|| {
        OmniError::manifest_internal("B2 exact token-prefix scan found no admissible row")
    }))
}

fn append_trusted_stream_metadata(
    batch: RecordBatch,
    metadata: Vec<Option<TrustedStreamRowMetadata>>,
) -> Result<RecordBatch> {
    if metadata.len() != batch.num_rows() {
        return Err(OmniError::manifest_internal(format!(
            "trusted stream metadata row count {} differs from batch row count {}",
            metadata.len(),
            batch.num_rows()
        )));
    }
    if batch
        .column_by_name(crate::db::STREAM_METADATA_COLUMN)
        .is_some()
    {
        return Err(OmniError::manifest(format!(
            "stream caller may not supply reserved column '{}'",
            crate::db::STREAM_METADATA_COLUMN
        )));
    }
    let hidden = build_trusted_stream_metadata_array(&metadata)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let source_schema = batch.schema();
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(crate::db::manifest::stream_token::trusted_stream_metadata_field());
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(hidden);
    RecordBatch::try_new(schema, columns).map_err(|error| OmniError::Lance(error.to_string()))
}

/// Once the exclusive fold worker begins a cold claim or cut, every
/// non-capacity failure requires the retained worker to settle and durable
/// state to be reclassified before another attempt. There is deliberately no
/// sidecar before the immutable generation cut exists, so this operation id
/// names the retained cut owner rather than pretending a recovery file exists.
fn fold_cut_error(key: StreamWorkerKey, error: MemWalWorkerError) -> OmniError {
    match error {
        MemWalWorkerError::ResourceLimit {
            resource,
            limit,
            actual,
        } => OmniError::resource_limit(resource, limit, actual),
        MemWalWorkerError::InvalidConfig { reason } => OmniError::manifest_internal(format!(
            "stream fold configuration became invalid for {key}: {reason}"
        )),
        other => OmniError::recovery_required(
            format!("stream-cut:{key}"),
            format!(
                "the pre-sidecar MemWAL cut may have claimed, sealed, flushed, or begun retirement; wait for the retained owner and retry through exact restart classification: {other}"
            ),
        ),
    }
}

fn ensure_same_binding(
    expected: StreamWorkerKey,
    actual: &StreamAuthorityCapture,
    member: &str,
) -> Result<()> {
    if actual.worker_key == expected {
        return Ok(());
    }
    Err(OmniError::manifest_read_set_changed(
        member.to_string(),
        Some(expected.to_string()),
        Some(actual.worker_key.to_string()),
    ))
}

fn ensure_same_capture(
    expected: &StreamAuthorityCapture,
    actual: &StreamAuthorityCapture,
    member: &str,
) -> Result<()> {
    let same = expected.worker_key == actual.worker_key
        && expected.entry.identity == actual.entry.identity
        && expected.entry.table_key == actual.entry.table_key
        && expected.entry.table_path == actual.entry.table_path
        && expected.entry.table_branch == actual.entry.table_branch
        && expected.entry.table_version == actual.entry.table_version
        && expected.lifecycle == actual.lifecycle
        && expected.txn.authority == actual.txn.authority;
    if same {
        return Ok(());
    }
    Err(OmniError::manifest_read_set_changed(
        member.to_string(),
        Some(format!(
            "{}:{}:v{}:{:?}",
            expected.worker_key,
            expected.entry.table_path,
            expected.entry.table_version,
            expected.lifecycle
        )),
        Some(format!(
            "{}:{}:v{}:{:?}",
            actual.worker_key,
            actual.entry.table_path,
            actual.entry.table_version,
            actual.lifecycle
        )),
    ))
}

fn ensure_claim_successor_capture(
    expected: &StreamAuthorityCapture,
    actual: &StreamAuthorityCapture,
    member: &str,
) -> Result<()> {
    let same_lane = expected.worker_key == actual.worker_key
        && expected.entry.identity == actual.entry.identity
        && expected.entry.table_key == actual.entry.table_key
        && expected.entry.table_path == actual.entry.table_path
        && expected.entry.table_branch == actual.entry.table_branch
        && expected.entry.table_version == actual.entry.table_version
        && expected.entry.row_count == actual.entry.row_count
        && expected.binding == actual.binding
        && expected.txn.authority == actual.txn.authority
        && actual
            .lifecycle
            .validate_successor_of(&expected.lifecycle)
            .is_ok()
        && actual.lifecycle.current_claim_receipt_id.is_some()
        && actual.epoch_floor > expected.epoch_floor;
    if same_lane {
        return Ok(());
    }
    Err(OmniError::manifest_read_set_changed(
        member.to_string(),
        Some(format!(
            "{}:{}:v{}:{:?}",
            expected.worker_key,
            expected.entry.table_path,
            expected.entry.table_version,
            expected.lifecycle
        )),
        Some(format!(
            "{}:{}:v{}:{:?}",
            actual.worker_key,
            actual.entry.table_path,
            actual.entry.table_version,
            actual.lifecycle
        )),
    ))
}

fn ensure_live_stream_prestate(
    prepared: &StreamAuthorityCapture,
    live_entry: &crate::db::manifest::SubTableEntry,
    live_lifecycle: &StreamLifecycleEntry,
) -> Result<()> {
    if live_entry.identity == prepared.entry.identity
        && live_entry.table_key == prepared.entry.table_key
        && live_entry.table_path == prepared.entry.table_path
        && live_entry.table_branch == prepared.entry.table_branch
        && live_entry.table_version == prepared.entry.table_version
        && live_lifecycle == &prepared.lifecycle
    {
        return Ok(());
    }
    Err(OmniError::manifest_read_set_changed(
        format!("stream_fold_prestate:{}", prepared.entry.table_key),
        Some(format!(
            "{}:{}:v{}:{:?}",
            prepared.entry.identity,
            prepared.entry.table_path,
            prepared.entry.table_version,
            prepared.lifecycle
        )),
        Some(format!(
            "{}:{}:v{}:{live_lifecycle:?}",
            live_entry.identity, live_entry.table_path, live_entry.table_version
        )),
    ))
}

async fn scan_fresh_generation(
    capture: &StreamAuthorityCapture,
    cut: &SealedGenerationCut,
) -> Result<Vec<RecordBatch>> {
    let schema: ArrowSchema = capture.head.dataset().schema().into();
    let mut scanner = LsmScanner::without_base_table(
        Arc::new(schema),
        capture.full_path.clone(),
        vec![cut.fresh_only_snapshot()],
        vec!["id".to_string()],
    )
    .with_session(capture.head.dataset().session());
    if let Some(store_params) = capture.head.dataset().store_params() {
        scanner = scanner.with_store_params(store_params.clone());
    }
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let mut batches = Vec::new();
    let mut rows = 0_u64;
    let mut logical_bytes = 0_u64;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        if batch.num_rows() == 0 {
            continue;
        }
        rows = rows
            .checked_add(
                u64::try_from(batch.num_rows()).map_err(|_| {
                    OmniError::manifest_internal("stream fold row count exceeds u64")
                })?,
            )
            .ok_or_else(|| OmniError::manifest_internal("stream fold row count overflow"))?;
        if rows > B1_MAX_GENERATION_ROWS {
            return Err(OmniError::resource_limit(
                format!("stream fold rows for {}", capture.entry.table_key),
                B1_MAX_GENERATION_ROWS,
                rows,
            ));
        }
        logical_bytes = logical_bytes
            .checked_add(b1_logical_batch_bytes(&batch).map_err(worker_error)?)
            .ok_or_else(|| {
                OmniError::manifest_internal("stream fold logical byte count overflow")
            })?;
        if logical_bytes > B1_MAX_GENERATION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                format!("stream fold bytes for {}", capture.entry.table_key),
                B1_MAX_GENERATION_ARROW_BYTES,
                logical_bytes,
            ));
        }

        // LsmScanner may emit slices whose Utf8 and other variable-width
        // arrays retain sparse backing buffers much larger than the selected
        // rows. A no-op concat preserves those buffers. Taking every selected
        // row rebuilds dense owned arrays, after which dropping `batch`
        // releases the scanner representation before the next slice arrives.
        let row_count = u32::try_from(batch.num_rows())
            .map_err(|_| OmniError::manifest_internal("stream fold batch row count exceeds u32"))?;
        let indices = UInt32Array::from_iter_values(0..row_count);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None)
                    .map_err(|error| OmniError::Lance(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        batches.push(
            RecordBatch::try_new(batch.schema(), columns)
                .map_err(|error| OmniError::Lance(error.to_string()))?,
        );
    }
    if rows == 0 {
        return Err(OmniError::manifest_internal(
            "stream fold fresh-only scan returned no live rows",
        ));
    }
    Ok(batches)
}

async fn plan_fold_attribution(
    snapshot: &crate::db::manifest::Snapshot,
    identity: TableIdentity,
    lifecycle: &StreamLifecycleEntry,
    binding: &StreamPhysicalBinding,
    batches: &[RecordBatch],
) -> Result<AttributedFoldPlan> {
    let mut winners = Vec::new();
    let mut saw_null = false;
    let mut saw_present = false;
    let mut logical_ids = std::collections::BTreeSet::new();

    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "stream fold output has no exact non-null Utf8 id column",
                )
            })?;
        let metadata = batch
            .column_by_name(crate::db::STREAM_METADATA_COLUMN)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream fold output is missing reserved '{}' metadata",
                    crate::db::STREAM_METADATA_COLUMN
                ))
            })?;
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "stream fold output contains a null logical id",
                ));
            }
            let logical_id = ids.value(row).to_string();
            if !logical_ids.insert(logical_id.clone()) {
                return Err(OmniError::manifest_internal(format!(
                    "stream fold scanner returned duplicate winner id '{logical_id}'"
                )));
            }
            match decode_trusted_stream_metadata(metadata.as_ref(), row)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?
            {
                Some(metadata) => {
                    saw_present = true;
                    metadata
                        .validate_for(identity, &logical_id)
                        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                    winners.push((logical_id, metadata));
                }
                None => saw_null = true,
            }
        }
    }

    if saw_null {
        return Err(OmniError::manifest_internal(if saw_present {
            "stream fold generation mixes attributed and unattributed winners"
        } else {
            "internal schema v9 refuses an unattributed stream generation"
        }));
    }
    if !saw_present {
        return Err(OmniError::manifest_internal(
            "stream fold has no attribution state for its non-empty generation",
        ));
    }

    let stream_incarnation_id = &lifecycle.enrollment_receipt.stream_incarnation_id;
    let token_dataset = snapshot.open_stream_token_authority().await?;
    let current = stream_token_rows_for_keys(
        &token_dataset,
        snapshot.stream_token_authority(),
        identity,
        &logical_ids,
    )
    .await?;
    let base_entry = snapshot
        .entries()
        .find(|entry| entry.identity == identity)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream fold cannot find manifest-selected base table for identity {identity}"
            ))
        })?;
    let base_dataset = snapshot.open_dataset(&base_entry.table_key).await?;
    let base_metadata =
        lookup_base_stream_metadata_for_keys(&base_dataset, identity, &logical_ids).await?;
    let mut token_rows = Vec::with_capacity(winners.len());
    for (logical_id, metadata) in winners {
        if &metadata.stream_incarnation_id != stream_incarnation_id {
            return Err(OmniError::manifest_internal(format!(
                "stream fold winner '{logical_id}' names stream incarnation '{}' but lifecycle authority names '{stream_incarnation_id}'",
                metadata.stream_incarnation_id
            )));
        }
        let current_row = current.get(&logical_id);
        validate_authority_base_pair(
            identity,
            &logical_id,
            current_row,
            base_metadata.get(&logical_id),
        )
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if current_row.is_some_and(|row| row.stream_incarnation_id != *stream_incarnation_id) {
            return Err(OmniError::manifest_internal(format!(
                "stream token authority for '{logical_id}' belongs to another stream incarnation"
            )));
        }
        let expected_fold_base = current_row.map(|row| row.current_token);
        if metadata.fold_base_token != expected_fold_base {
            return Err(OmniError::manifest_internal(format!(
                "stream fold winner '{logical_id}' does not chain from the manifest-selected token authority"
            )));
        }
        if current_row.is_some_and(|row| row.current_token == metadata.stream_token) {
            return Err(OmniError::manifest_internal(format!(
                "stream fold winner '{logical_id}' does not advance its current token"
            )));
        }
        token_rows.push(
            StreamTokenAuthorityRow::from_present_metadata(
                identity,
                logical_id,
                binding.enrollment_id.clone(),
                &metadata,
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
        );
    }
    token_rows.sort_by(|left, right| left.logical_id.cmp(&right.logical_id));
    let summary = stream_fold_attribution_commitment(&token_rows)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    Ok(AttributedFoldPlan {
        token_rows,
        summary,
    })
}

fn validate_fold_output_bounds(table_key: &str, batches: &[RecordBatch]) -> Result<()> {
    let (rows, bytes) = fold_output_size(batches)?;
    if rows == 0 {
        return Err(OmniError::manifest_internal(
            "stream fold cannot stage an empty generation",
        ));
    }
    if rows > B1_MAX_GENERATION_ROWS {
        return Err(OmniError::resource_limit(
            format!("stream fold rows for {table_key}"),
            B1_MAX_GENERATION_ROWS,
            rows,
        ));
    }
    if bytes > B1_MAX_GENERATION_ARROW_BYTES {
        return Err(OmniError::resource_limit(
            format!("stream fold bytes for {table_key}"),
            B1_MAX_GENERATION_ARROW_BYTES,
            bytes,
        ));
    }
    Ok(())
}

fn fold_output_size(batches: &[RecordBatch]) -> Result<(u64, u64)> {
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for batch in batches {
        rows = rows
            .checked_add(
                u64::try_from(batch.num_rows()).map_err(|_| {
                    OmniError::manifest_internal("stream fold row count exceeds u64")
                })?,
            )
            .ok_or_else(|| OmniError::manifest_internal("stream fold row count overflow"))?;
        bytes = bytes
            .checked_add(b1_logical_batch_bytes(batch).map_err(worker_error)?)
            .ok_or_else(|| OmniError::manifest_internal("stream fold byte count overflow"))?;
    }
    Ok((rows, bytes))
}

fn exact_merged_generation(
    details: &MemWalIndexDetails,
    shard_id: ShardId,
) -> Result<Option<MergedGeneration>> {
    let matches = details
        .merged_generations
        .iter()
        .filter(|merged| merged.shard_id == shard_id)
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.clone())),
        _ => Err(OmniError::manifest_internal(format!(
            "stream fold found duplicate merged-generation cursors for shard {shard_id}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_block_error_preserves_typed_correction_token() {
        let error = stream_data_block_error("block-token");

        assert!(matches!(
            &error,
            OmniError::StreamDataBlocked { block_token } if block_token == "block-token"
        ));
        assert_eq!(
            error.to_string(),
            "stream fold is strict-blocked; correction requires block token block-token"
        );
    }

    #[test]
    fn json_structure_bound_counts_tokens_but_not_string_contents() {
        let commas = ",".repeat(STREAM_JSON_MAX_STRUCTURAL_SLOTS as usize);
        let quoted = format!(r#"{{"value":"{commas}"}}"#);
        validate_stream_json_structure_bound(quoted.as_bytes())
            .expect("structural bytes inside one JSON string do not allocate DOM nodes");

        let amplified = format!(
            "[{}0]",
            "0,".repeat(STREAM_JSON_MAX_STRUCTURAL_SLOTS as usize)
        );
        let error = validate_stream_json_structure_bound(amplified.as_bytes())
            .expect_err("many tiny values must fail before serde allocates their DOM nodes");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: STREAM_JSON_MAX_STRUCTURAL_SLOTS,
                actual,
            } if resource == "stream_json_structural_slots"
                && actual == STREAM_JSON_MAX_STRUCTURAL_SLOTS + 1
        ));
    }

    #[tokio::test]
    async fn exact_token_prefix_scan_handles_a_fail_then_fit_replacement() {
        let current = BTreeMap::from([("a", 1_u64), ("b", 100_u64)]);
        let updates = [("a", 60_u64), ("b", 1_u64)];
        let projected_bytes = |prefix: usize| {
            let mut projected = current.clone();
            projected.extend(updates[..prefix].iter().copied());
            projected.into_values().sum::<u64>()
        };

        assert_eq!(
            projected_bytes(1),
            160,
            "the first successor must exceed the synthetic projection limit"
        );
        assert_eq!(
            projected_bytes(2),
            61,
            "the second successor replaces the much larger current winner"
        );

        let selected = longest_fitting_token_prefix(updates.len(), |prefix| {
            let bytes = projected_bytes(prefix);
            async move {
                if bytes > 110 {
                    Ok(Some(OmniError::FoldRequired {
                        table_key: "node:Person".to_string(),
                        rows: u64::try_from(prefix).unwrap(),
                        bytes,
                    }))
                } else {
                    Ok(None)
                }
            }
        })
        .await
        .expect("the full non-monotonic prefix fits");

        assert_eq!(
            selected, 2,
            "exact descending selection must not reject after prefix one fails"
        );
    }
}
