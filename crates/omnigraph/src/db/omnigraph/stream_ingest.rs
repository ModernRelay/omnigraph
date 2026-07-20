#![allow(dead_code)]

//! RFC-026 Phase-B1 private stream admission and one-generation fold.
//!
//! This is deliberately an engine-internal orchestration layer.  Lance owns
//! the WAL, its epoch fence, replay, and fresh-generation scan.  OmniGraph owns
//! the graph authority checks around those primitives and the one
//! `__manifest` publication which makes a folded generation visible.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::LsmScanner;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter};
use lance_index::mem_wal::{MemWalIndexDetails, MergedGeneration, ShardId, ShardStatus};

use crate::db::manifest::{
    RecoveryAuthorityToken, RecoveryLineageIntent, RecoveryStreamFoldCut, SidecarKind,
    SidecarTablePin, StreamLifecycle, StreamLifecycleEntry, StreamPhysicalBinding, TableIdentity,
    complete_stream_fold_sidecar_v11, finalize_effect_free_stream_fold_sidecar_v11, list_sidecars,
    new_stream_fold_sidecar_v11, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::SnapshotHandle;
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS, CallerOrdinalRange,
    CheckedExclusiveStreamAuthority, CheckedStreamAuthority, ClaimedMemWalWorker, DurableBatchAck,
    IdleAuthorityCheck, IdleAuthorityFailure, MemWalWorkerError, OpenedMemWalWorker, PreparedPut,
    PreparedPutFailure, SealedGenerationCut, StreamWorkerKey, WorkerOpenFailure,
    b1_input_accounting, capture_current_head_witness, reconstruct_b1_writer_config,
    validate_stream_config_v2_binding,
};
use crate::validate::{ChangeSet, CommittedState, TableChange};

use super::{Omnigraph, WriteTxn};

const B1_MAX_FOLD_ATTEMPTS: usize = 2;

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

impl Omnigraph {
    /// Admit one already-normalized, non-empty physical batch into the private
    /// B1 MemWAL profile and acknowledge only after Lance's durability watcher
    /// succeeds and the same worker observes no successor shard epoch.
    ///
    /// The `Arc` receiver is intentional.  The worker owns a detached task so
    /// dropping the requesting future cannot abandon an invoked put, its
    /// watcher, or quiesced retirement.
    pub(crate) async fn stream_put_phase_b1(
        self: &Arc<Self>,
        table_key: &str,
        batch: RecordBatch,
        caller_ordinals: CallerOrdinalRange,
    ) -> Result<DurableBatchAck> {
        // Refuse an oversized caller buffer synchronously before recovery IO,
        // authority capture, detached ownership, or a cold epoch claim.
        validate_stream_input_bounds(table_key, &batch)?;
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
        let prepared = self
            .capture_stream_authority(table_key, "stream put")
            .await?;
        self.validate_stream_admission_batch(&prepared, &batch)?;

        let key = prepared.worker_key;
        let admission_key = prepared.admission_key.clone();
        let authority_db = Arc::clone(self);
        let authority_key = admission_key.clone();
        // Inflight and Arrow budgets are acquired before any same-key queue or
        // shared-admission wait. The returned token is already inside that
        // bounded corridor and moves into the detached prepare closure.
        let (queued, put_authority) = self
            .stream_workers
            .reserve_put_input(key, table_key, &batch, move || async move {
                let shared = authority_db
                    .write_queue()
                    .acquire_stream_shared(&authority_key)
                    .await;
                CheckedStreamAuthority::from_shared_admission(shared)
            })
            .await?;
        let table_key_owned = table_key.to_string();
        let admitted_batch = batch.clone();
        let idle_db = Arc::clone(self);
        let idle_key = key;
        let idle_admission_key = admission_key.clone();
        let idle_table_key = table_key_owned.clone();
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
                                .capture_stream_authority(&table_key_owned, "stream put")
                                .await?;
                            ensure_same_binding(key, &before, "stream put final authority")?;
                            db.validate_stream_admission_batch(&before, &admitted_batch)?;
                            validate_claimed_writer(&writer, key, before.epoch_floor).await?;

                            // A warm put repeats every authority and physical
                            // check immediately before the worker invokes it.
                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let after = db
                                .capture_stream_authority(&table_key_owned, "stream put")
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
                                .capture_stream_authority(&table_key_owned, "stream put")
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

                        // Claiming advances the shard epoch.  Recheck graph,
                        // lifecycle, schema, base HEAD, and the claimed shard
                        // immediately before handing the still-held lease to
                        // the serialized put worker.
                        let checked = async {
                            db.ensure_no_relevant_stream_sidecar(key.identity, "stream put")
                                .await?;
                            let after = db
                                .capture_stream_authority(&table_key_owned, "stream put")
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
                table_key.to_string(),
                batch,
                caller_ordinals,
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
            match self
                .stream_fold_attempt(&table_key, key, &before_cut, &cut)
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
                batches,
                cut.key.shard_id,
                cut.generation,
            )
            .await?;
        let planned_transaction = staged.transaction_identity();

        // Admission remains exclusively held inside `cut`.  Enter the normal
        // RFC-022 inner order only for final authority, sidecar arm, effect, and
        // graph publication.
        let write_queue = self.write_queue();
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
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
            validate_stream_config_v2_binding(&final_details, &prepared.binding)
                .map_err(worker_error)?;
        if enrollment_id != key.enrollment_id || shard_id != key.shard_id {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_fold_binding:{table_key}"),
                Some(key.to_string()),
                Some(format!("{}:{}:{}", key.identity, enrollment_id, shard_id)),
            ));
        }
        let prior_merged = exact_merged_generation(&final_details, key.shard_id)?;

        let lineage = self.new_lineage_intent_for_branch(None, None).await?;
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
        let sidecar = new_stream_fold_sidecar_v11(
            None,
            pin,
            authority,
            recovery_lineage,
            prepared.binding.clone(),
            prepared.lifecycle.current_head_witness.clone(),
            prepared.epoch_floor,
            prior_merged,
            generation_cut,
            planned_transaction,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

        let outcome = match self.storage().commit_staged_exact(final_head, staged).await {
            Ok(outcome) => outcome,
            Err(error) => {
                if !error.is_retryable_commit_conflict() {
                    return Err(OmniError::recovery_required(
                        handle.operation_id,
                        format!("stream fold commit failed with an ambiguous effect: {error}"),
                    ));
                }
                let effect_free = finalize_effect_free_stream_fold_sidecar_v11(
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
                            "stream fold commit failed ({error}); exact effect-free classification failed: {classification_error}"
                        ),
                    )
                })?;
                if effect_free {
                    return Ok(FoldAttempt::EffectFree(error));
                }
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    format!("stream fold commit failed with an unclassified effect: {error}"),
                ));
            }
        };
        if !outcome.is_exact() {
            return Err(OmniError::recovery_required(
                handle.operation_id,
                format!(
                    "stream fold committed a non-exact transaction: planned={:?}, committed={:?}, version={}",
                    outcome.planned_transaction(),
                    outcome.committed_transaction(),
                    outcome.committed_version(),
                ),
            ));
        }

        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_FOLD_POST_TABLE_COMMIT_PRE_CONFIRM,
        )
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("stream fold stopped after its exact table effect: {error}"),
            )
        })?;
        complete_stream_fold_sidecar_v11(
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
            validate_stream_config_v2_binding(&details, &binding).map_err(worker_error)?;
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
        validate_stream_input_bounds(&capture.entry.table_key, batch)?;
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

    async fn ensure_no_relevant_stream_sidecar(
        &self,
        identity: TableIdentity,
        operation: &str,
    ) -> Result<()> {
        let sidecars = list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = sidecars.iter().find(|sidecar| {
            sidecar.writer_kind == SidecarKind::SchemaApply
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
}

enum FoldAttempt {
    Published,
    EffectFree(OmniError),
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
    let charge = b1_input_accounting(batch).map_err(worker_error)?;
    if !charge.fits() {
        return Err(OmniError::FoldRequired {
            table_key: table_key.to_string(),
            rows: charge.rows,
            bytes: charge.arrow_bytes,
        });
    }
    Ok(())
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
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        batches.push(batch);
    }
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return Err(OmniError::manifest_internal(
            "stream fold fresh-only scan returned no live rows",
        ));
    }
    Ok(batches)
}

fn validate_fold_output_bounds(table_key: &str, batches: &[RecordBatch]) -> Result<()> {
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
        bytes =
            bytes
                .checked_add(u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                    OmniError::manifest_internal("stream fold byte count exceeds u64")
                })?)
                .ok_or_else(|| OmniError::manifest_internal("stream fold byte count overflow"))?;
    }
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
