#![allow(dead_code)]

//! RFC-026 Phase-B1 private stream admission and one-generation fold.
//!
//! This is deliberately an engine-internal orchestration layer.  Lance owns
//! the WAL, its epoch fence, replay, and fresh-generation scan.  OmniGraph owns
//! the graph authority checks around those primitives and the one
//! `__manifest` publication which makes a folded generation visible.

#[cfg(feature = "failpoints")]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::take::take;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::LsmScanner;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter};
use lance_index::mem_wal::{MemWalIndexDetails, MergedGeneration, ShardId, ShardStatus};

use crate::db::manifest::{
    CurrentHeadWitness, RecoveryAuthorityToken, RecoveryLineageIntent, RecoveryStreamFoldCut,
    SidecarTablePin,
    StreamLifecycle, StreamLifecycleEntry, StreamPhysicalBinding, TableIdentity,
    complete_stream_fold_sidecar_v12, confirm_stream_fold_sidecar_v12,
    finalize_effect_free_stream_fold_sidecar_v12, list_sidecars, new_stream_fold_sidecar_v12,
    write_sidecar,
};
use crate::db::manifest::stream_token::{
    AdmissionClassification, AdmissionRequest, PayloadDigest, PayloadDigestInput,
    StreamFoldAttributionSummary, StreamRowOrigin, StreamToken, StreamTokenAuthorityRow,
    StreamWriteEnvelope, TrustedContributorId, TrustedStreamRowMetadata,
    build_trusted_stream_metadata_array, classify_admission, decode_trusted_stream_metadata,
    stream_fold_attribution_commitment, validate_authority_base_pair,
};
use crate::db::manifest::stream::{
    LastFoldOutcome, LastFoldSummary, StreamGenerationCut,
};
use crate::db::manifest::token_store::{
    add_stream_lookup_retained_bytes, lookup_stream_token_row, open_stream_token_authority_head,
    stage_stream_token_upsert, stream_token_authority_entry_for_dataset, stream_token_rows_for_keys,
    validate_stream_token_plan_bounds,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::SnapshotHandle;
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS,
    B2_MAX_TOKEN_PROJECTION_ARROW_BYTES, CallerOrdinalRange,
    CheckedExclusiveStreamAuthority, CheckedStreamAuthority, ClaimedMemWalWorker,
    ConfirmedStreamTokenOverlay, ConfirmedStreamTokenOverlayRow, DurableBatchAck,
    IdleAuthorityCheck, IdleAuthorityFailure, MemWalWorkerError, OpenedMemWalWorker, PreparedPut,
    PreparedPutFailure, QueuedBatchPermit, SealedGenerationCut, StreamWorkerKey,
    WorkerOpenFailure,
    b1_input_accounting, b1_logical_batch_bytes, capture_current_head_witness,
    reconstruct_b1_writer_config, validate_stream_config_v3_binding,
};
use crate::validate::{ChangeSet, CommittedState, TableChange};

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
struct StreamAuthorityCapture {
    txn: WriteTxn,
    entry: crate::db::manifest::SubTableEntry,
    lifecycle: StreamLifecycleEntry,
    binding: StreamPhysicalBinding,
    worker_key: StreamWorkerKey,
    admission_key: StreamAdmissionKey,
    shard_id: ShardId,
    enrollment_id: ShardId,
    epoch_floor: u64,
    full_path: String,
    head: SnapshotHandle,
    details: MemWalIndexDetails,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttributedFoldPlan {
    token_rows: Vec<StreamTokenAuthorityRow>,
    summary: StreamFoldAttributionSummary,
}

impl Omnigraph {
    /// Admit one already-normalized, non-empty physical batch through the
    /// feature-gated B1 substrate seam. Synthetic trusted envelopes make these
    /// older worker/capacity tests exercise the active B2 fold and recovery-v12
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
        // Refuse an oversized caller buffer synchronously before recovery IO,
        // authority capture, detached ownership, or a cold epoch claim.
        validate_stream_input_bounds(table_key, &batch)?;
        let batch = self
            .storage()
            .prepare_keyed_write_batch(table_key, batch)
            .await?;
        validate_stream_input_bounds(table_key, &batch)?;
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
        let provisional = self
            .capture_stream_authority(table_key, "stream put")
            .await?;
        self.validate_stream_logical_admission_batch(&provisional, &batch)?;

        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "validated B1 test batch has no exact Utf8 id column",
                )
            })?;
        let mut logical_ids = BTreeSet::new();
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
        self.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
            .await?;
        let prepared = self
            .capture_stream_authority(table_key, "stream put final admission")
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
                    (
                        Some(current.current_token),
                        Some(current.current_token),
                        1,
                    )
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
            let caller_ordinal = caller_ordinals
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
            .projected_token_authority_rows(
                &queued,
                table_key,
                &confirmed_token_updates,
            )
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

    /// Admit one fully normalized logical row through RFC-026's private B2
    /// compare-and-chain boundary.  The caller supplies only its idempotency
    /// envelope; contributor identity is already resolved by the trusted
    /// engine boundary.  This remains crate-private and is exposed to tests
    /// only through the feature-gated seam below.
    pub(crate) async fn stream_put_phase_b2_one(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinal: u64,
        envelope: StreamWriteEnvelope,
        contributor_id: TrustedContributorId,
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

        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
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

        self.ensure_no_relevant_stream_sidecar(key.identity, "stream token admission")
            .await?;
        let prepared = self
            .capture_stream_authority(table_key, "stream token final admission")
            .await?;
        ensure_same_binding(
            key,
            &prepared,
            "stream token final admission authority",
        )?;
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
        let authority = crate::db::manifest::stream_token::StreamTokenAuthorityRow::from_present_metadata(
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
            .projected_token_authority_rows(
                &queued,
                table_key,
                &confirmed_token_updates,
            )
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
                    validate_claimed_writer(&writer, idle_key, before.epoch_floor).await?;

                    db.ensure_no_relevant_stream_sidecar(idle_key.identity, "stream idle eviction")
                        .await?;
                    let after = db
                        .capture_stream_authority(&table_key, "stream idle eviction")
                        .await?;
                    ensure_same_capture(&before, &after, "stream idle eviction final authority")?;
                    validate_claimed_writer(&writer, idle_key, after.epoch_floor).await
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
                            validate_claimed_writer(&writer, key, before.epoch_floor).await?;

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
                            validate_claimed_writer(&writer, key, after.epoch_floor).await
                        }
                        .await;
                        match checked {
                            Ok(()) => Ok(PreparedPut::warm(authority)),
                            Err(error) => Err(PreparedPutFailure::warm(error, authority)),
                        }
                    }
                    None => {
                        let before = match async {
                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let before = db
                                .capture_stream_authority(&prepare_table_key, "stream put")
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
                        let config = match reconstruct_b1_writer_config(
                            &before.details,
                            before.enrollment_id,
                            before.shard_id,
                        )
                        .map_err(worker_error)
                        {
                            Ok(config) => config,
                            Err(error) => {
                                return Err(PreparedPutFailure::cold_unclaimed(error, authority));
                            }
                        };
                        let writer = match before
                            .head
                            .dataset()
                            .mem_wal_writer(before.shard_id, config)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(error) => {
                                return Err(PreparedPutFailure::cold_unclaimed(
                                    OmniError::Lance(error.to_string()),
                                    authority,
                                ));
                            }
                        };
                        let claimed = ClaimedMemWalWorker::new(writer);
                        if let Err(error) =
                            validate_claimed_writer(claimed.writer(), key, before.epoch_floor).await
                        {
                            return Err(PreparedPutFailure::cold_claimed(
                                error, authority, claimed,
                            ));
                        }
                        let opened = match claimed
                            .classify(&before.details, before.epoch_floor)
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
                            ensure_same_capture(
                                &before,
                                &after,
                                "stream put claim-to-put authority",
                            )?;
                            db.validate_stream_admission_batch(&after, &admitted_batch)?;
                            validate_claimed_writer(opened.writer(), key, after.epoch_floor).await
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
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
        let provisional = self
            .capture_stream_authority(&table_key, "stream fold")
            .await?;
        let key = provisional.worker_key;
        let admission_key = provisional.admission_key.clone();

        let exclusive = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;
        self.ensure_no_relevant_stream_sidecar(key.identity, "stream fold")
            .await?;
        let before_cut = self
            .capture_stream_authority(&table_key, "stream fold")
            .await?;
        ensure_same_binding(key, &before_cut, "stream fold pre-cut authority")?;

        let opener_head = before_cut.head.clone();
        let opener_details = before_cut.details.clone();
        let opener_enrollment = before_cut.enrollment_id;
        let opener_shard = before_cut.shard_id;
        let opener_epoch_floor = before_cut.epoch_floor;
        let opener = Box::new(move || {
            Box::pin(async move {
                let config =
                    reconstruct_b1_writer_config(&opener_details, opener_enrollment, opener_shard)
                        .map_err(WorkerOpenFailure::unclaimed)?;
                let writer = opener_head
                    .dataset()
                    .mem_wal_writer(opener_shard, config)
                    .await
                    .map_err(|error| MemWalWorkerError::Lance {
                        operation: "writer claim",
                        message: error.to_string(),
                    })
                    .map_err(WorkerOpenFailure::unclaimed)?;
                OpenedMemWalWorker::classify(writer, &opener_details, opener_epoch_floor).await
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

        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR,
        )?;

        let mut last_effect_free_error = None;
        for attempt in 0..B1_MAX_FOLD_ATTEMPTS {
            // Keep the large closed recovery-v12 fold future off this Tokio
            // worker's stack. Debug failpoint builds otherwise compose it with
            // the surrounding recovery/barrier future and can exceed the
            // default worker stack in multi-table crash tests.
            match Box::pin(self.stream_fold_attempt(&table_key, key, &before_cut, &cut)).await {
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

    async fn stream_fold_attempt(
        &self,
        table_key: &str,
        key: StreamWorkerKey,
        before_cut: &StreamAuthorityCapture,
        cut: &SealedGenerationCut,
    ) -> Result<FoldAttempt> {
        self.ensure_no_relevant_stream_sidecar(key.identity, "stream fold")
            .await?;
        let prepared = self
            .capture_stream_authority(table_key, "stream fold")
            .await?;
        ensure_same_capture(before_cut, &prepared, "stream fold post-drain authority")?;
        if cut.key != key || cut.writer_epoch <= prepared.epoch_floor {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_fold_cut:{table_key}"),
                Some(format!("{key}:epoch>{}", prepared.epoch_floor)),
                Some(format!("{}:epoch={}", cut.key, cut.writer_epoch)),
            ));
        }

        let batches = scan_fresh_generation(&prepared, cut).await?;
        validate_fold_output_bounds(table_key, &batches)?;
        let attribution = plan_fold_attribution(
            &prepared.txn.base,
            key.identity,
            &prepared.lifecycle,
            &prepared.binding,
            &batches,
        )
        .await?;
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
        crate::validate::validate_changeset(&changeset, &committed, &prepared.txn.catalog).await?;

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

        // The entry healer ran before preparation, but another main-branch
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
                Some(format!(
                    "{:?}",
                    prepared.txn.base.stream_token_authority()
                )),
                Some(format!("{:?}", live.stream_token_authority())),
            ));
        }
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
            next_lifecycle.current_head_witness = CurrentHeadWitness {
                branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
                table_version: post_commit_pin,
                transaction_uuid: planned_transaction.uuid.clone(),
                manifest_e_tag: None,
            };
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
                operation_id: "pending-stream-fold-operation".to_string(),
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
            let mut sidecar = new_stream_fold_sidecar_v12(
                Some("omnigraph:stream-fold".to_string()),
                pin,
                authority,
                recovery_lineage,
                prepared.binding.clone(),
                prepared.lifecycle.clone(),
                next_lifecycle,
                prior_merged,
                generation_cut,
                planned_transaction,
                prepared.txn.base.stream_token_authority().clone(),
                token_planned_transaction,
                attribution.token_rows.clone(),
                attribution.summary.clone(),
            )?;
            let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

            let base_outcome = match self.storage().commit_staged_exact(final_head, staged).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    if error.is_retryable_commit_conflict() {
                        let effect_free = finalize_effect_free_stream_fold_sidecar_v12(
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
                    let recovered = complete_stream_fold_sidecar_v12(
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
            confirm_stream_fold_sidecar_v12(
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
            complete_stream_fold_sidecar_v12(
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

    async fn capture_stream_authority(
        &self,
        table_key: &str,
        operation: &str,
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
        if lifecycle.lifecycle != StreamLifecycle::Open {
            return Err(OmniError::manifest_stream_lifecycle_conflict(
                entry.identity.stable_table_id,
                entry.identity.table_incarnation_id,
                table_key,
                lifecycle.lifecycle.as_str(),
                operation,
            ));
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
        let expected = ArrowSchema::new_with_metadata(
            expected_fields,
            expected_physical.metadata().clone(),
        );
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
                self.stream_put_phase_b1(table_key, batch, ordinals)
                    .await
                    .map(|_| ())
            }
            None => self.stream_fold_phase_b1(table_key).await,
        }
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

    /// Return the exact logical stream incarnation for private protocol tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_incarnation_for_test(
        &self,
        table_key: &str,
    ) -> Result<String> {
        let capture = self
            .capture_stream_authority(table_key, "stream incarnation test probe")
            .await?;
        Ok(capture
            .lifecycle
            .enrollment_receipt
            .stream_incarnation_id)
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
    let mut selected = lookup_base_stream_metadata_for_keys(dataset, identity, &logical_ids).await?;
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
            Some(i64::try_from(logical_ids.len().saturating_add(1)).map_err(|_| {
                OmniError::manifest_internal("base stream-metadata lookup row limit exceeds i64")
            })?),
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

fn worker_error(error: MemWalWorkerError) -> OmniError {
    OmniError::Lance(error.to_string())
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

fn validate_generation_token_plan(
    table_key: &str,
    rows: &[StreamTokenAuthorityRow],
) -> Result<()> {
    match validate_stream_token_plan_bounds(rows) {
        Ok(()) => Ok(()),
        Err(OmniError::ResourceLimitExceeded { actual, .. }) => {
            Err(OmniError::FoldRequired {
                table_key: table_key.to_string(),
                rows: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                bytes: actual,
            })
        }
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

async fn validate_claimed_writer(
    writer: &ShardWriter,
    key: StreamWorkerKey,
    epoch_floor: u64,
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
        || writer.epoch() <= epoch_floor
    {
        return Err(OmniError::manifest_read_set_changed(
            format!("stream_writer_epoch:{}", key.identity),
            Some(format!("{}:epoch>{epoch_floor}:ACTIVE", key.shard_id)),
            Some(format!(
                "writer_shard={}:writer_epoch={}:manifest={manifest:?}",
                writer.shard_id(),
                writer.epoch()
            )),
        ));
    }
    Ok(())
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
