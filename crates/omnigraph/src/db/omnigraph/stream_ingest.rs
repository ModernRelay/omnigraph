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
    ManagementReceipt, StreamGenerationCut, stream_graph_identity_digest,
    stream_quiesce_result_payload,
};
use crate::db::manifest::stream_token::{
    AdmissionClassification, AdmissionRequest, PayloadDigest, PayloadDigestInput,
    StreamFoldAttributionSummary, StreamRowOrigin, StreamToken, StreamTokenAuthorityRow,
    StreamWriteEnvelope, TrustedContributorId, TrustedStreamRowMetadata,
    build_trusted_stream_metadata_array, classify_admission, decode_trusted_stream_metadata,
    stream_fold_attribution_commitment, validate_authority_base_pair,
};
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, add_stream_lookup_retained_bytes, lookup_lifecycle_ledger_record_by_id,
    lookup_management_receipt, lookup_stream_token_row, open_stream_token_authority_head,
    stage_lifecycle_ledger_records, stage_management_receipt, stage_stream_token_upsert,
    stream_token_authority_entry_for_dataset, stream_token_rows_for_keys,
    validate_stream_token_plan_bounds,
};
use crate::db::manifest::{
    CurrentHeadWitness, ExpectedTableVersions, ManifestChange, RecoveryAuthorityToken,
    RecoveryLineageIntent, RecoveryProtocolV14, RecoveryStreamClaimContinuationV14,
    RecoveryStreamClaimOutcomeV14, RecoveryStreamFoldCut, RecoveryStreamLifecycleReceiptKind,
    SidecarTablePin, StreamLifecycle, StreamLifecycleEntry, StreamPhysicalBinding, TableIdentity,
    TableVersionExpectation, arm_stream_claim_checkpoint_sidecar_v14,
    arm_stream_claim_terminal_sidecar_v14, classify_effect_free_stream_claim_sidecar_v14,
    complete_stream_claim_sidecar_v14, complete_stream_fold_sidecar_v14,
    complete_stream_lifecycle_receipt_sidecar_v14, confirm_stream_claim_sidecar_v14,
    confirm_stream_fold_sidecar_v14, confirm_stream_lifecycle_receipt_sidecar_v14,
    finalize_effect_free_stream_fold_sidecar_v14, list_sidecars,
    lookup_stream_claim_continuation_v14, new_stream_claim_sidecar_v14,
    new_stream_drain_fold_sidecar_v14, new_stream_fold_v2_sidecar_v14,
    new_stream_lifecycle_receipt_sidecar_v14, prepared_stream_claim_attempt_v14,
    rearm_stream_claim_checkpoint_sidecar_v14, receipt_first_rearm_stream_claim_sidecar_v14,
    write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS, B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
    CallerOrdinalRange, CheckedExclusiveStreamAuthority, CheckedStreamAuthority,
    ClaimedMemWalWorker, ConfirmedStreamTokenOverlay, ConfirmedStreamTokenOverlayRow,
    CurrentGenerationProjectionSource, DurableBatchAck, IdleAuthorityCheck, IdleAuthorityFailure,
    MemWalWorkerError, OpenedMemWalWorker, PassiveB1PhysicalState, PassiveQuiesceDisposition,
    PreparedPut, PreparedPutFailure, QueuedBatchPermit, QuiesceCut, SealedGenerationCut,
    StreamWorkerKey, WorkerOpenFailure, b1_input_accounting, b1_logical_batch_bytes,
    capture_current_head_witness, reconstruct_b1_writer_config, scan_flushed_generation_projection,
    validate_b1_lifecycle_physical_state, validate_stream_config_v3_binding,
};
use crate::validate::{ChangeSet, CommittedState, TableChange};

use super::stream_lifecycle::{
    CanonicalDataBlockEvidence, ClaimAttemptEvidence, ClaimAttemptRequest, ClaimOperationRequest,
    DataBlockEvidenceCollector, EmptyCutEvidence, QuiesceRequest, authenticate_claim_wal_segment,
    build_claim_adoption_row, build_claim_attempt_effect, build_draining_data_block,
    build_draining_to_sealed, build_open_to_draining, build_terminal_claim,
    claim_wal_authentication_plan, claim_wal_key_discovery_plan, collect_claim_wal_segment_keys,
    current_generation_lww_projection_digest, lifecycle_generation_lww_projection_digest,
    prepare_claim_attempt, prepare_claim_operation, stream_quiesce_request_digest,
    stream_quiesce_request_payload_from_draining,
};
use super::{Omnigraph, WriteTxn};

const B1_MAX_FOLD_ATTEMPTS: usize = 2;

/// Private B2 result for one caller occurrence. Public response shaping stays
/// deliberately inactive; this value exists so crash/race tests can prove the
/// sequencing contract without exposing a product API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamTokenAdmissionAck {
    pub(crate) stream_token: StreamToken,
    pub(crate) origin: StreamRowOrigin,
    pub(crate) already_durable: bool,
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
        let batch = self
            .storage()
            .prepare_keyed_write_batch(table_key, batch)
            .await?;
        validate_stream_input_bounds(table_key, &batch)?;
        let provisional = self
            .capture_stream_authority(table_key, "stream put")
            .await?;
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

    /// Admit one authenticated, fully normalized logical row through the
    /// checked serving-runtime boundary.
    ///
    /// This is the first intentionally narrow functional ingress seam: the
    /// table must already be enrolled and `OPEN`, and normalization remains a
    /// caller responsibility. Transport, lazy enrollment, and public response
    /// shaping remain inactive. Before production transport activation, the
    /// owned profile guard must transfer into the detached put tail so caller
    /// cancellation cannot release it before watcher/fence settlement.
    pub(crate) async fn stream_ingest_one_as(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        envelope: StreamWriteEnvelope,
        actor_id: &str,
    ) -> Result<StreamTokenAdmissionAck> {
        self.enforce(
            omnigraph_policy::PolicyAction::StreamIngest,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;
        let contributor_id = TrustedContributorId::new(actor_id.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;

        // Recovery may need the profile gate exclusively, so complete the
        // normal write-entry barrier before joining that gate in shared mode.
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;

        // The profile gate is outermost and is acquired exactly once. Calling
        // `stream_put_phase_b2_one` here would attempt a nested read lock and
        // can deadlock behind a queued profile writer because Tokio's RwLock is
        // fair/write-preferring.
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        self.stream_put_phase_b2_one_under_profile_guard(
            table_key,
            batch,
            caller_ordinal,
            envelope,
            contributor_id,
            &profile_guard,
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
            &profile_guard,
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
        _profile_guard: &tokio::sync::OwnedRwLockReadGuard<()>,
    ) -> Result<StreamTokenAdmissionAck> {
        if batch.num_rows() != 1 {
            return Err(OmniError::manifest(format!(
                "private B2 admission requires exactly one row, got {}",
                batch.num_rows()
            )));
        }
        validate_stream_input_bounds(table_key, &batch)?;
        let mut preprocessing = self.stream_workers.reserve_b2_preprocessing()?;
        // Blob bytes participate in the payload digest. Resolve them before
        // recovery/authority and never hash an external descriptor which may
        // change after acknowledgement.
        let batch = self
            .storage()
            .prepare_keyed_write_batch(table_key, batch)
            .await?;
        validate_stream_input_bounds(table_key, &batch)?;

        let id_array = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "validated stream admission batch has no exact Utf8 id column",
                )
            })?;
        if id_array.is_null(0) {
            return Err(OmniError::manifest("stream row id must be non-null"));
        }
        let logical_id = id_array.value(0).to_string();
        let canonical_payload = super::canonical_stream_payload_v1(&batch, 0)?;
        envelope
            .validate()
            .map_err(|error| OmniError::manifest(error.to_string()))?;

        let provisional = self
            .capture_stream_authority(table_key, "stream token admission")
            .await?;
        self.validate_stream_logical_admission_batch(&provisional, &batch)?;
        let key = provisional.worker_key;
        let admission_key = provisional.admission_key.clone();
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_B2_AFTER_PROVISIONAL_AUTHORITY,
        )?;
        let authority_db = Arc::clone(self);
        let authority_key = admission_key.clone();
        let (mut queued, put_authority) = self
            .stream_workers
            .reserve_b2_put_input(
                key,
                table_key,
                &batch,
                &mut preprocessing,
                move || async move {
                    let shared = authority_db
                        .write_queue()
                        .acquire_stream_shared(&authority_key)
                        .await;
                    CheckedStreamAuthority::from_shared_admission(shared)
                },
            )
            .await?;

        let prepared = self
            .capture_stream_authority(table_key, "stream token final admission")
            .await?;
        self.ensure_no_relevant_stream_sidecar_except_exact_claim(
            &prepared,
            "stream token admission",
        )
        .await?;
        ensure_same_binding(key, &prepared, "stream token final admission authority")?;
        self.validate_stream_logical_admission_batch(&prepared, &batch)?;
        let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
            identity: prepared.entry.identity,
            accepted_schema_hash: &prepared.txn.authority.schema_ir_hash,
            canonical_payload: &canonical_payload,
        })
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        drop(canonical_payload);
        drop(preprocessing);
        let request = AdmissionRequest {
            identity: prepared.entry.identity,
            logical_id: logical_id.clone(),
            envelope,
            contributor_id,
            payload_digest,
        };
        request
            .validate()
            .map_err(|error| OmniError::manifest(error.to_string()))?;

        // Owning the same-key queue makes this overlay snapshot stable until
        // the permit transfers into the worker. The shared admission lease
        // simultaneously excludes a fold/token-table publication.
        let overlay_current = self
            .stream_workers
            .confirmed_token_for_key(&queued, table_key, &logical_id)
            .await?;

        let (durable_authority, durable_metadata) = if overlay_current.is_none() {
            let token_dataset = prepared.txn.base.open_stream_token_authority().await?;
            let authority = lookup_stream_token_row(
                &token_dataset,
                prepared.txn.base.stream_token_authority(),
                prepared.entry.identity,
                &logical_id,
            )
            .await?;
            // A missing token row plus a non-null base copy is corruption, so
            // the base probe is unconditional whenever no confirmed overlay
            // owns the key.
            let metadata = lookup_base_stream_metadata(
                prepared.head.dataset(),
                prepared.entry.identity,
                &logical_id,
            )
            .await?;
            (authority, metadata)
        } else {
            (None, None)
        };
        let current_authority = overlay_current
            .as_ref()
            .map(|row| &row.authority)
            .or(durable_authority.as_ref());
        let current_metadata = overlay_current
            .as_ref()
            .map(|row| &row.metadata)
            .or(durable_metadata.as_ref());
        let stream_incarnation_id = prepared
            .lifecycle
            .enrollment_receipt
            .stream_incarnation_id
            .as_str();
        let classification = classify_admission(
            stream_incarnation_id,
            &request,
            current_authority,
            current_metadata,
        )
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;

        let candidate = match classification {
            AdmissionClassification::AlreadyDurable { authority, .. } => {
                return Ok(StreamTokenAdmissionAck {
                    stream_token: authority.current_token,
                    origin: authority.origin,
                    already_durable: true,
                });
            }
            AdmissionClassification::BindingChanged {
                current_stream_incarnation_id,
            } => {
                return Err(OmniError::StreamBindingChanged {
                    stable_table_id: request.identity.stable_table_id,
                    table_incarnation_id: request.identity.table_incarnation_id,
                    current_stream_incarnation_id,
                });
            }
            AdmissionClassification::SequenceConflict { current_token } => {
                return Err(OmniError::StreamSequenceConflict {
                    stable_table_id: request.identity.stable_table_id,
                    table_incarnation_id: request.identity.table_incarnation_id,
                    logical_id,
                    current_token: current_token.map(|token| token.to_string()),
                });
            }
            AdmissionClassification::IdempotencyConflict { current_token } => {
                return Err(OmniError::StreamIdempotencyConflict {
                    stable_table_id: request.identity.stable_table_id,
                    table_incarnation_id: request.identity.table_incarnation_id,
                    logical_id,
                    current_token: current_token.to_string(),
                });
            }
            AdmissionClassification::New { candidate_token } => candidate_token,
        };

        let (fold_base_token, chain_depth) = match overlay_current.as_ref() {
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
            ShardId::new_v4().to_string(),
            caller_ordinal,
        )
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let authority =
            crate::db::manifest::stream_token::StreamTokenAuthorityRow::from_present_metadata(
                request.identity,
                logical_id.clone(),
                prepared.binding.enrollment_id.clone(),
                &metadata,
            )
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        // A row which cannot fit an otherwise-empty token/recovery projection
        // is terminal for this occurrence; asking the caller to fold would
        // create an endless retry loop.
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
        let logical_write_id = request.envelope.write_id.clone();
        let batch = append_trusted_stream_metadata(batch, vec![Some(metadata.clone())])?;
        self.validate_stream_admission_batch(&prepared, &batch)?;
        queued.reprice_for_exact_batch(table_key, &batch)?;
        let mut confirmed_token_updates = ConfirmedStreamTokenOverlay::new();
        confirmed_token_updates.insert(
            logical_id,
            ConfirmedStreamTokenOverlayRow {
                authority,
                metadata,
            },
        );
        let projected_token_rows = self
            .stream_workers
            .projected_token_authority_rows(&queued, table_key, &confirmed_token_updates)
            .await?;
        validate_generation_token_plan(table_key, &projected_token_rows)?;

        if let Err(error) = self
            .finish_reserved_stream_put(
                table_key.to_string(),
                batch,
                CallerOrdinalRange::new(caller_ordinal, caller_ordinal).map_err(worker_error)?,
                key,
                admission_key,
                queued,
                put_authority,
                confirmed_token_updates,
            )
            .await
        {
            return Err(match error {
                OmniError::AckUnknown {
                    stable_table_id,
                    table_incarnation_id,
                    enrollment_id,
                    shard_id,
                    caller_ordinal_start,
                    caller_ordinal_end,
                    reason,
                    ..
                } => OmniError::AckUnknown {
                    stable_table_id,
                    table_incarnation_id,
                    enrollment_id,
                    shard_id,
                    caller_ordinal_start,
                    caller_ordinal_end,
                    admission_attempt_id: Some(admission_attempt_id),
                    logical_write_ids: vec![logical_write_id],
                    unconfirmed_candidate_token: Some(candidate.to_string()),
                    reason,
                },
                other => other,
            });
        }
        Ok(StreamTokenAdmissionAck {
            stream_token: candidate,
            origin,
            already_durable: false,
        })
    }

    /// Finish one already-queued stream append.  B1 supplies an empty token
    /// projection; B2 supplies the exact watcher-confirmed updates which must
    /// become warm only after the post-durability fence check.
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
        let idle_db = Arc::clone(self);
        let idle_key = key;
        let idle_admission_key = admission_key.clone();
        let idle_table_key = table_key.clone();
        let idle_authority: IdleAuthorityCheck = Arc::new(move |writer: Arc<ShardWriter>| {
            let db = Arc::clone(&idle_db);
            let admission_key = idle_admission_key.clone();
            let table_key = idle_table_key.clone();
            Box::pin(async move {
                let shared = db.write_queue().acquire_stream_shared(&admission_key).await;
                let authority = CheckedStreamAuthority::from_shared_admission(shared);
                let checked = async {
                    db.ensure_no_relevant_stream_sidecar(idle_key.identity, "stream idle eviction")
                        .await?;
                    let before = db
                        .capture_stream_authority(&table_key, "stream idle eviction")
                        .await?;
                    ensure_same_binding(idle_key, &before, "stream idle eviction authority")?;
                    db.validate_claimed_writer_for_capture(&writer, idle_key, &before)
                        .await?;

                    db.ensure_no_relevant_stream_sidecar(idle_key.identity, "stream idle eviction")
                        .await?;
                    let after = db
                        .capture_stream_authority(&table_key, "stream idle eviction")
                        .await?;
                    ensure_same_capture(&before, &after, "stream idle eviction final authority")?;
                    db.validate_claimed_writer_for_capture(&writer, idle_key, &after)
                        .await
                }
                .await;
                match checked {
                    Ok(()) => Ok(authority),
                    Err(error) => Err(IdleAuthorityFailure::new(error, authority)),
                }
            })
        });
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
            if receipt.from_revision != expected_lifecycle_revision
                || receipt.actor_id != actor_id
                || receipt.identity != initial_entry.identity
                || receipt.operation_kind != "QUIESCE"
            {
                return Err(OmniError::StreamLifecycleIdempotencyConflict {
                    stable_table_id: initial_entry.identity.stable_table_id,
                    table_incarnation_id: initial_entry.identity.table_incarnation_id,
                    operation_kind: "QUIESCE".to_string(),
                    operation_id: drain_id,
                });
            }
            receipt.validate(receipt.to_revision)?;
            if initial_lifecycle.lifecycle == StreamLifecycle::Sealed
                && initial_lifecycle
                    .management_receipt_chain
                    .head_record_id
                    .as_deref()
                    == Some(receipt.record_id.as_str())
            {
                return Ok(());
            }
            return Err(OmniError::manifest_internal(
                "selected terminal quiesce receipt is not the current SEALED lifecycle head",
            ));
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
                && receipt.from_revision == expected_lifecycle_revision
                && receipt.actor_id == actor_id
                && settled_lifecycle.lifecycle == StreamLifecycle::Sealed
                && settled_lifecycle
                    .management_receipt_chain
                    .head_record_id
                    .as_deref()
                    == Some(receipt.record_id.as_str())
            {
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
            if receipt.from_revision == expected_lifecycle_revision
                && receipt.actor_id == actor_id
                && current_lifecycle.lifecycle == StreamLifecycle::Sealed
                && current_lifecycle
                    .management_receipt_chain
                    .head_record_id
                    .as_deref()
                    == Some(receipt.record_id.as_str())
            {
                return Ok(());
            }
            return Err(OmniError::StreamLifecycleIdempotencyConflict {
                stable_table_id: key.identity.stable_table_id,
                table_incarnation_id: key.identity.table_incarnation_id,
                operation_kind: "QUIESCE".to_string(),
                operation_id: drain_id,
            });
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
        let physical = validate_b1_lifecycle_physical_state(capture.head.dataset(), &draining)
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
        match validate_b1_lifecycle_physical_state(final_head.dataset(), &live_lifecycle)
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
            mut physical,
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
                    read_claim_physical_prestate_after_attempt(capture)
                        .await
                        .map_err(|error| {
                            WorkerOpenFailure::unclaimed(claim_open_worker_error(error))
                        })?,
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
                physical,
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
                physical = read_claim_physical_prestate_after_attempt(capture)
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
    }

    async fn capture_stream_authority(
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
            StreamLifecycle::Sealed => false,
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

        let mut changeset = ChangeSet::new();
        changeset.insert(
            capture.entry.table_key.clone(),
            TableChange {
                added: vec![batch.clone()],
                changed: Vec::new(),
                deleted_ids: Vec::new(),
            },
        );
        if let Some(violation) =
            crate::validate::evaluate_value_constraints(&changeset, &capture.txn.catalog)
                .into_iter()
                .next()
        {
            return Err(violation.into_omni_error());
        }
        Ok(())
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

        let mut changeset = ChangeSet::new();
        changeset.insert(
            capture.entry.table_key.clone(),
            TableChange {
                added: vec![batch.clone()],
                changed: Vec::new(),
                deleted_ids: Vec::new(),
            },
        );
        if let Some(violation) =
            crate::validate::evaluate_value_constraints(&changeset, &capture.txn.catalog)
                .into_iter()
                .next()
        {
            return Err(violation.into_omni_error());
        }
        Ok(())
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
    #[allow(clippy::too_many_arguments)]
    pub async fn failpoint_stream_ingest_one_as_for_test(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        stream_incarnation_id: &str,
        write_id: &str,
        predecessor_token: Option<&str>,
        actor_id: &str,
    ) -> Result<(String, bool)> {
        let predecessor_token = predecessor_token
            .map(str::parse::<StreamToken>)
            .transpose()
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let ack = self
            .stream_ingest_one_as(
                table_key,
                batch,
                caller_ordinal,
                StreamWriteEnvelope {
                    stream_incarnation_id: stream_incarnation_id.to_string(),
                    write_id: write_id.to_string(),
                    predecessor_token,
                },
                actor_id,
            )
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

async fn lookup_base_stream_metadata_for_keys(
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
    let physical = validate_b1_lifecycle_physical_state(capture.head.dataset(), &capture.lifecycle)
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
}
