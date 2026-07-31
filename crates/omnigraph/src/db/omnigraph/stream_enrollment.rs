//! RFC-026 bounded stream enrollment orchestration.
//!
//! This module intentionally exposes no production SDK/API surface. It joins
//! the exact MemWAL adapter, enrollment recovery sidecar, manifest lifecycle row,
//! and process-local admission lease so the crash path and success path share
//! one roll-forward implementation. Internal schema v12 fixes the complete
//! config-v3/lifecycle-v3 enrollment authority before the recovery intent is
//! armed.

use lance::Dataset;
use lance_index::mem_wal::ShardId;
use serde::{Deserialize, Serialize};

use crate::db::manifest::stream::{
    BINDING_RECEIPT_TAG, BindingReceipt, CLAIM_RECEIPT_TAG, CurrentHeadWitness,
    EnrollmentReceiptV2, binding_receipt_chain_genesis, stream_graph_identity_digest,
    stream_physical_binding_digest,
};
use crate::db::manifest::stream_profile::ReceiptChainRef;
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, lookup_enrollment_receipt_v2, lookup_lifecycle_ledger_record_by_id,
    open_stream_token_authority_head, stage_lifecycle_ledger_records,
};
use crate::db::manifest::{
    EnrollmentReceipt, RecoveryAuthorityToken, RecoveryLineageIntent, STREAM_CONFIG_VERSION,
    SidecarTablePin, StreamLifecycle, StreamLifecycleEntry, StreamPhysicalBinding,
    StreamProfileState, StreamTokenAuthorityEntry, SubTableEntry, TableIdentity,
    complete_stream_enrollment_sidecar_v14, new_stream_enrollment_sidecar_v14,
    stream_enrollment_intent_digest_v1, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};
use crate::table_store::mem_wal::{
    MemWalEnrollmentPlan, capture_pre_enrollment_head, initialize_index_from_exact_no_effect,
    provision_shard_from_exact_index_only, stream_config_v3_hash,
    validate_b1_lifecycle_physical_state, validate_phase_a_stream_absent,
};

use super::Omnigraph;

/// Prepare is a cold, bodyless authority check, but it must still have a
/// history-independent ceiling. Rebind appends one immutable binding receipt;
/// exceeding this bound requires an explicit future chain-checkpoint format
/// rather than an unbounded scan on every prepare retry.
const MAX_SELECTED_BINDING_CHAIN_RECORDS: u64 = 1_024;

/// Ephemeral, non-wire compare token for one absent eligible stream lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamEligibilityWitness {
    graph_identity_digest: String,
    identity: TableIdentity,
    table_location: String,
    schema_ir_hash: String,
    schema_identity_version: u32,
    profile_revision: u64,
    fold_delegation_id: String,
    fold_delegation_digest: String,
    stream_config_version: u32,
    stream_config_hash: String,
    current_head: CurrentHeadWitness,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum PrepareStreamIngestOutcome {
    WitnessRequired(StreamEligibilityWitness),
    Enrolled(EnrollmentReceiptV2),
    AlreadyEnrolled(StreamLifecycleEntry),
}

impl Omnigraph {
    /// Validate the complete internal-v12 stream capability before a handle is
    /// served. Recovery has already consumed every parseable enrollment intent
    /// on read-write open; therefore a MemWAL index without a lifecycle row is
    /// uncovered partial format, while every lifecycle must match its exact
    /// current table witness and bounded config-v3 generation topology.
    pub(super) async fn validate_stream_format_consistency(&self) -> Result<()> {
        let snapshot = self.coordinator.read().await.snapshot();
        let token_dataset = snapshot
            .open_stream_token_authority()
            .await
            .map_err(|error| {
                OmniError::manifest_internal(format!(
                    "internal-v12 stream-token authority consistency failed: {error}"
                ))
            })?;
        let graph_identity_digest =
            stream_graph_identity_digest(&self.schema_view.load().schema_identity_domain)?;
        let has_active_stream = snapshot.stream_lifecycles().any(|(_, lifecycle)| {
            matches!(
                lifecycle.lifecycle,
                StreamLifecycle::Open | StreamLifecycle::Draining
            )
        });
        if has_active_stream {
            let branches = self.coordinator.read().await.branch_list().await?;
            if branches.iter().any(|branch| branch != "main") {
                return Err(OmniError::manifest_internal(
                    "internal-v12 OPEN/DRAINING stream lifecycle cannot coexist with named graph branches",
                ));
            }
        }
        for entry in snapshot.entries() {
            let table = self.storage().open_snapshot_at_entry(entry).await?;
            let physical_schema = arrow_schema::Schema::from(table.dataset().schema());
            crate::db::manifest::stream_token::validate_trusted_stream_metadata_schema(
                &physical_schema,
            )
            .map_err(|error| {
                OmniError::manifest_internal(format!(
                    "internal-v12 stream format consistency failed for '{}' ({}): {error}",
                    entry.table_key, entry.identity
                ))
            })?;
            let full_path = format!(
                "{}/{}",
                self.root_uri().trim_end_matches('/'),
                entry.table_path.trim_start_matches('/')
            );
            let validation = match snapshot.stream_lifecycle(entry.identity) {
                Some(lifecycle) => {
                    if lifecycle.identity != entry.identity
                        || lifecycle.binding.table_location != entry.table_path
                        || lifecycle.binding.table_branch != entry.table_branch
                        || lifecycle.current_head_witness.table_version != entry.table_version
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "stream lifecycle for identity {} disagrees with the current manifest table pointer",
                            entry.identity
                        )));
                    }
                    if let Err(error) = validate_selected_lifecycle_ledger_authority(
                        &token_dataset,
                        snapshot.stream_token_authority(),
                        &graph_identity_digest,
                        lifecycle,
                    )
                    .await
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "internal-v12 stream ledger consistency failed for '{}' ({}): {error}",
                            entry.table_key, entry.identity
                        )));
                    }
                    // A Dataset pinned at the manifest-selected version cannot
                    // observe a later attached HEAD. Lifecycle authority owns
                    // the current physical ref, so latest must still equal the
                    // durable exact witness before the format is accepted.
                    let head = self.storage().open_dataset_head(&full_path, None).await?;
                    validate_b1_lifecycle_physical_state(head.dataset(), lifecycle)
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                }
                None => {
                    match validate_phase_a_stream_absent(table.dataset()).await {
                        Err(error) => Err(error.to_string()),
                        Ok(()) => {
                            // The manifest-selected version can still be N while an
                            // uncovered initializer effect sits at the attached HEAD
                            // N+1. Exact-version validation alone would miss precisely
                            // the partial-format state this activation gate must reject.
                            let head = match self
                                .storage()
                                .open_dataset_head(&full_path, entry.table_branch.as_deref())
                                .await
                            {
                                Ok(head) => head,
                                Err(error) => return Err(error),
                            };
                            if head.version() == table.dataset().version().version {
                                Ok(())
                            } else {
                                validate_phase_a_stream_absent(head.dataset())
                                    .await
                                    .map_err(|error| error.to_string())
                            }
                        }
                    }
                }
            };
            validation.map_err(|error| {
                OmniError::manifest_internal(format!(
                    "internal-v12 stream format consistency failed for '{}' ({}): {error}",
                    entry.table_key, entry.identity
                ))
            })?;
        }
        Ok(())
    }

    /// Arm recovery-v14 only from an exact echoed eligibility witness.
    #[allow(dead_code)]
    pub(crate) async fn prepare_stream_ingest_as(
        &self,
        table_key: &str,
        enrollment_request_id: &str,
        witness: Option<&StreamEligibilityWitness>,
        actor_id: &str,
    ) -> Result<PrepareStreamIngestOutcome> {
        self.enforce(
            omnigraph_policy::PolicyAction::StreamIngest,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;
        if actor_id.is_empty() || actor_id.trim() != actor_id {
            return Err(OmniError::manifest(
                "stream enrollment actor is not canonical",
            ));
        }
        let enrollment_request_text = enrollment_request_id;
        let enrollment_request_id =
            ShardId::parse_str(enrollment_request_text).map_err(|error| {
                OmniError::manifest(format!("invalid enrollment request id: {error}"))
            })?;
        if enrollment_request_id.is_nil()
            || enrollment_request_id.get_version_num() != 4
            || enrollment_request_id.to_string() != enrollment_request_text
        {
            return Err(OmniError::manifest(
                "enrollment request id is not canonical UUID v4",
            ));
        }

        {
            let _preflight = self.write_queue().acquire_stream_profile_shared().await;
            self.ensure_streaming_ingest_runtime_authorized().await?;
        }
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        // Boxing avoids overflowing the current-thread crash-test stack.
        Box::pin(self.enroll_stream_table_b1_under_profile_guard(
            table_key,
            actor_id,
            enrollment_request_id,
            witness,
            true,
            &profile_guard,
        ))
        .await
    }

    fn capture_stream_eligibility_witness(
        &self,
        txn: &crate::db::WriteTxn,
        entry: &SubTableEntry,
        current_head: CurrentHeadWitness,
    ) -> Result<StreamEligibilityWitness> {
        Self::ensure_stream_table_admission_supported(&txn.catalog, &entry.table_key)?;
        if entry.table_branch.is_some() || txn.base.stream_lifecycle(entry.identity).is_some() {
            return Err(OmniError::manifest_conflict(format!(
                "stream eligibility requires an absent canonical-main lane for '{}'",
                entry.table_key
            )));
        }
        let StreamProfileState::Enabled {
            active_fold_delegation,
        } = &txn.base.stream_profile().state
        else {
            return Err(OmniError::StreamingRequiresClusterRuntime {
                mode: txn.base.stream_profile().mode().as_str().to_string(),
            });
        };
        Ok(StreamEligibilityWitness {
            graph_identity_digest: stream_graph_identity_digest(
                &txn.authority.schema_identity_domain,
            )?,
            identity: entry.identity,
            table_location: entry.table_path.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
            profile_revision: txn.base.stream_profile().profile_revision,
            fold_delegation_id: active_fold_delegation.delegation_id.clone(),
            fold_delegation_digest: active_fold_delegation.delegation_digest.clone(),
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: stream_config_v3_hash(),
            current_head,
        })
    }

    async fn classify_existing_stream_prepare(
        &self,
        txn: &crate::db::WriteTxn,
        entry: &SubTableEntry,
        enrollment_request_id: ShardId,
        actor_id: &str,
        expected_witness: Option<&StreamEligibilityWitness>,
    ) -> Result<Option<PrepareStreamIngestOutcome>> {
        let lifecycle = txn.base.stream_lifecycle(entry.identity).cloned();
        let graph_identity_digest =
            stream_graph_identity_digest(&txn.authority.schema_identity_domain)?;
        let token_dataset = txn.base.open_stream_token_authority().await?;
        let exact_receipt = lookup_enrollment_receipt_v2(
            &token_dataset,
            txn.base.stream_token_authority(),
            &graph_identity_digest,
            entry.identity,
            &enrollment_request_id.to_string(),
        )
        .await?;
        if exact_receipt.is_some() && lifecycle.is_none() {
            return Err(OmniError::manifest_internal(format!(
                "stream enrollment request {enrollment_request_id} has a durable receipt but no lifecycle"
            )));
        }
        let Some(lifecycle) = lifecycle else {
            return Ok(None);
        };
        if lifecycle.identity != entry.identity
            || lifecycle.binding.table_location != entry.table_path
            || lifecycle.binding.table_branch != entry.table_branch
            || lifecycle.current_head_witness.table_version != entry.table_version
        {
            return Err(OmniError::manifest_internal(format!(
                "stream lifecycle {} disagrees with current table authority",
                entry.identity
            )));
        }
        validate_selected_lifecycle_ledger_authority(
            &token_dataset,
            txn.base.stream_token_authority(),
            &graph_identity_digest,
            &lifecycle,
        )
        .await?;
        let full_path = format!(
            "{}/{}",
            self.root_uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        let head = self.storage().open_dataset_head(&full_path, None).await?;
        validate_b1_lifecycle_physical_state(head.dataset(), &lifecycle)
            .await
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;

        let winner_request_id = &lifecycle.enrollment_receipt.enrollment_request_id;
        let winner_receipt = match exact_receipt {
            Some(receipt) => receipt,
            None => lookup_enrollment_receipt_v2(
                &token_dataset,
                txn.base.stream_token_authority(),
                &graph_identity_digest,
                entry.identity,
                winner_request_id,
            )
            .await?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream lifecycle {} has no durable enrollment receipt",
                    entry.identity
                ))
            })?,
        };
        validate_enrollment_receipt_against_lifecycle(
            &winner_receipt,
            &graph_identity_digest,
            &lifecycle,
        )?;
        validate_selected_binding_chain_authority(
            &token_dataset,
            txn.base.stream_token_authority(),
            &winner_receipt,
            &lifecycle,
        )
        .await?;
        if winner_receipt.enrollment_request_id == enrollment_request_id.to_string() {
            if winner_receipt.actor_id != actor_id {
                return Err(OmniError::manifest_conflict(format!(
                    "stream enrollment request {enrollment_request_id} is already bound to another actor"
                )));
            }
            let Some(witness) = expected_witness else {
                return Ok(Some(PrepareStreamIngestOutcome::AlreadyEnrolled(lifecycle)));
            };
            let witness_intent_digest = stream_enrollment_intent_digest_v1(
                witness.identity,
                &witness.table_location,
                &txn.authority.schema_identity_domain,
                &witness.schema_ir_hash,
                witness.schema_identity_version,
                &witness.current_head,
                &witness.stream_config_hash,
            )?;
            if witness.graph_identity_digest != graph_identity_digest
                || witness.identity != entry.identity
                || witness.table_location != entry.table_path
                || witness.schema_ir_hash != txn.authority.schema_ir_hash
                || witness.schema_identity_version != txn.authority.schema_identity_version
                || witness.stream_config_version != STREAM_CONFIG_VERSION
                || witness.stream_config_hash != stream_config_v3_hash()
                || witness.stream_config_hash != winner_receipt.physical_binding.stream_config_hash
                || witness_intent_digest != winner_receipt.enrollment_intent_digest
            {
                return Err(OmniError::manifest_conflict(format!(
                    "stream enrollment request {enrollment_request_id} is already bound to a different enrollment intent"
                )));
            }
            Ok(Some(PrepareStreamIngestOutcome::Enrolled(winner_receipt)))
        } else {
            Ok(Some(PrepareStreamIngestOutcome::AlreadyEnrolled(lifecycle)))
        }
    }

    /// Raw crash fixture that bypasses checked-runtime eligibility.
    #[allow(dead_code)]
    pub(crate) async fn enroll_stream_table_b1(
        &self,
        table_key: &str,
        actor_id: Option<&str>,
    ) -> Result<()> {
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        let enrollment_actor = actor_id
            .unwrap_or("omnigraph:stream-enrollment")
            .to_string();
        let outcome = Box::pin(self.enroll_stream_table_b1_under_profile_guard(
            table_key,
            &enrollment_actor,
            ShardId::new_v4(),
            None,
            false,
            &profile_guard,
        ))
        .await?;
        match outcome {
            PrepareStreamIngestOutcome::Enrolled(_) => Ok(()),
            PrepareStreamIngestOutcome::WitnessRequired(_)
            | PrepareStreamIngestOutcome::AlreadyEnrolled(_) => Err(OmniError::manifest_internal(
                "raw stream enrollment returned a prepare outcome",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn enroll_stream_table_b1_under_profile_guard(
        &self,
        table_key: &str,
        enrollment_actor: &str,
        enrollment_request_id: ShardId,
        expected_witness: Option<&StreamEligibilityWitness>,
        prepare_mode: bool,
        _profile_guard: &tokio::sync::OwnedRwLockReadGuard<()>,
    ) -> Result<PrepareStreamIngestOutcome> {
        let txn = self.open_write_txn(None).await?;
        let prepared_entry = txn.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "cannot enroll unknown table '{table_key}' into MemWAL"
            ))
        })?;
        if prepared_entry.table_branch.is_some() {
            return Err(OmniError::manifest_conflict(format!(
                "bounded MemWAL enrollment supports only the physical main ref for '{table_key}'"
            )));
        }

        // Enrollment admission is outside every effect gate. Drain and future
        // append use the same identity/ref domain, preventing a post-check
        // physical effect from slipping across lifecycle closure in this
        // process.
        let admission_key = StreamAdmissionKey::for_resolved_ref(prepared_entry.identity, None);
        let write_queue = self.write_queue();
        let _admission_guard = write_queue.acquire_stream_exclusive(&admission_key).await;
        let schema_gate_key = crate::db::manifest::schema_apply_serial_queue_key();
        let _schema_guard = write_queue.acquire(&schema_gate_key).await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(prepared_entry.table_key.clone(), None)])
            .await;

        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "bounded MemWAL enrollment")
            .await?;
        let txn = self.open_write_txn(None).await?;
        let live_entry = txn.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_enrollment_table:{table_key}"),
                Some(prepared_entry.identity.to_string()),
                None,
            )
        })?;
        if live_entry.identity != prepared_entry.identity
            || live_entry.table_path != prepared_entry.table_path
            || live_entry.table_branch.is_some()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_enrollment_binding:{table_key}"),
                Some(format!(
                    "{}:{}:main",
                    prepared_entry.identity, prepared_entry.table_path
                )),
                Some(format!(
                    "{}:{}:{:?}",
                    live_entry.identity, live_entry.table_path, live_entry.table_branch
                )),
            ));
        }
        let branches = self.coordinator.read().await.branch_list().await?;
        let existing_is_sealed = txn
            .base
            .stream_lifecycle(live_entry.identity)
            .is_some_and(|entry| entry.lifecycle == StreamLifecycle::Sealed);
        if branches.iter().any(|branch| branch != "main") && !existing_is_sealed {
            return Err(OmniError::manifest_conflict(
                "bounded MemWAL enrollment requires a main-only graph; delete named branches before enrollment",
            ));
        }
        if prepare_mode {
            if let Some(outcome) = self
                .classify_existing_stream_prepare(
                    &txn,
                    &live_entry,
                    enrollment_request_id,
                    enrollment_actor,
                    expected_witness,
                )
                .await?
            {
                return Ok(outcome);
            }
            Self::ensure_stream_table_admission_supported(&txn.catalog, table_key)?;
        } else if let Some(existing) = txn.base.stream_lifecycle(live_entry.identity) {
            return Err(OmniError::manifest_stream_lifecycle_conflict(
                live_entry.identity.stable_table_id,
                live_entry.identity.table_incarnation_id,
                live_entry.table_key.clone(),
                existing.lifecycle.as_str(),
                "stream enrollment",
            ));
        }
        let full_path = format!(
            "{}/{}",
            self.root_uri().trim_end_matches('/'),
            live_entry.table_path
        );
        let head_handle = self.storage().open_dataset_head(&full_path, None).await?;
        self.ensure_existing_effect_baseline(
            &live_entry.table_key,
            None,
            live_entry.table_version,
            &head_handle,
        )
        .await?;
        let final_head = capture_pre_enrollment_head(head_handle.dataset())
            .await
            .map_err(|error| OmniError::manifest_conflict(error.to_string()))?;
        if prepare_mode {
            let current_witness =
                self.capture_stream_eligibility_witness(&txn, &live_entry, final_head.clone())?;
            if let Some(expected) = expected_witness
                && (expected.graph_identity_digest != current_witness.graph_identity_digest
                    || expected.identity != current_witness.identity
                    || expected.table_location != current_witness.table_location)
            {
                return Err(OmniError::manifest_read_set_changed(
                    "stream_eligibility_identity",
                    Some(format!(
                        "{}:{}:{}",
                        expected.graph_identity_digest, expected.identity, expected.table_location
                    )),
                    Some(format!(
                        "{}:{}:{}",
                        current_witness.graph_identity_digest,
                        current_witness.identity,
                        current_witness.table_location
                    )),
                ));
            }
            if expected_witness != Some(&current_witness) {
                return Ok(PrepareStreamIngestOutcome::WitnessRequired(current_witness));
            }
        }
        let enrollment_id = mint_distinct_stream_uuid(&[enrollment_request_id]);
        let shard_id = mint_distinct_stream_uuid(&[enrollment_request_id, enrollment_id]);
        let enrollment_plan = MemWalEnrollmentPlan::new(enrollment_id, shard_id)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let stream_incarnation_id = mint_distinct_stream_uuid(&[
            enrollment_request_id,
            enrollment_plan.enrollment_id,
            enrollment_plan.shard_id,
        ]);
        let binding_scope_id = mint_distinct_stream_uuid(&[
            enrollment_request_id,
            enrollment_plan.enrollment_id,
            enrollment_plan.shard_id,
            stream_incarnation_id,
        ]);
        let binding = StreamPhysicalBinding {
            stable_table_id: live_entry.identity.stable_table_id,
            table_incarnation_id: live_entry.identity.table_incarnation_id,
            table_location: live_entry.table_path.clone(),
            table_branch: None,
            enrollment_id: enrollment_plan.enrollment_id.to_string(),
            shard_ids: vec![enrollment_plan.shard_id.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: enrollment_plan.stream_config_hash(),
        };
        let lineage = self
            .new_lineage_intent_for_branch(None, Some(enrollment_actor))
            .await?;
        let live = txn.base.clone();

        let enrollment_intent_digest = stream_enrollment_intent_digest_v1(
            live_entry.identity,
            &live_entry.table_path,
            &txn.authority.schema_identity_domain,
            &txn.authority.schema_ir_hash,
            txn.authority.schema_identity_version,
            &final_head,
            &binding.stream_config_hash,
        )?;
        let legacy_enrollment_receipt = EnrollmentReceipt::new(
            enrollment_request_id.to_string(),
            enrollment_intent_digest.clone(),
            stream_incarnation_id.to_string(),
            binding.clone(),
        )?;
        let graph_identity_digest =
            stream_graph_identity_digest(&txn.authority.schema_identity_domain)?;
        let recorded_at = crate::db::now_micros()?;
        let enrollment_receipt = EnrollmentReceiptV2::new(
            graph_identity_digest.clone(),
            live_entry.identity,
            &binding_receipt_chain_genesis(),
            enrollment_request_id.to_string(),
            enrollment_intent_digest,
            enrollment_actor.to_string(),
            stream_incarnation_id.to_string(),
            binding_scope_id.to_string(),
            binding.clone(),
            recorded_at,
        )?;
        let terminal_receipt = enrollment_receipt.clone();
        let binding_receipt = BindingReceipt::new(
            graph_identity_digest,
            live_entry.identity,
            &enrollment_receipt.next_chain_ref()?,
            binding_scope_id.to_string(),
            stream_incarnation_id.to_string(),
            binding.clone(),
            enrollment_request_id.to_string(),
            recorded_at,
        )?;

        // Stage both immutable initial ledger records from the exact
        // manifest-selected token version before recovery is armed. The
        // transaction identity and complete rows enter the sidecar; recovery
        // can therefore recreate only this exact token effect after a crash.
        let prior_token_authority = live.stream_token_authority().clone();
        let selected_token = live.open_stream_token_authority().await?;
        let token_records = [
            LifecycleLedgerRecord::EnrollmentReceiptV2(enrollment_receipt.clone()),
            LifecycleLedgerRecord::BindingReceipt(binding_receipt.clone()),
        ];
        let staged_token =
            stage_lifecycle_ledger_records(selected_token, &prior_token_authority, &token_records)
                .await?;
        let planned_token_transaction = staged_token.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                &prior_token_authority,
                &self.control_session(),
            )
            .await?,
        );
        let staged_token = StagedHandle::new(staged_token);

        let authority = RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_identity_domain: txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id.clone(),
            branch: lineage.branch.clone(),
            actor_id: lineage.actor_id.clone(),
            merged_parent_commit_id: lineage.merged_parent_commit_id.clone(),
            created_at: lineage.created_at,
        };
        let pin = SidecarTablePin {
            identity: live_entry.identity,
            table_key: live_entry.table_key.clone(),
            table_path: full_path,
            expected_version: live_entry.table_version,
            post_commit_pin: live_entry.table_version.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal("stream enrollment table version overflow")
            })?,
            confirmed_version: None,
            table_branch: None,
        };
        let sidecar = new_stream_enrollment_sidecar_v14(
            enrollment_actor.to_string(),
            pin,
            authority,
            recovery_lineage,
            live.version(),
            live.stream_profile().clone(),
            final_head.clone(),
            enrollment_plan.clone(),
            binding.clone(),
            live_entry.row_count,
            legacy_enrollment_receipt,
            enrollment_receipt,
            binding_receipt,
            prior_token_authority,
            planned_token_transaction,
        )?;
        write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_ENROLLMENT_POST_SIDECAR_PRE_INDEX,
        )?;

        let mut head = head_handle.into_dataset();
        let initializer_error =
            initialize_index_from_exact_no_effect(&mut head, &final_head, &enrollment_plan)
                .await
                .err();
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_ENROLLMENT_POST_INDEX_PRE_RECOVERY,
        )?;

        // The ordinary path completes the exact empty shard and commits the
        // already-staged ledger transaction. Any error is still handed to the
        // same classifier below: it may prove/finish our exact partial state,
        // but it never adopts a foreign effect or compensates MemWAL objects.
        let mut effect_error = initializer_error.map(|error| error.to_string());
        if effect_error.is_none()
            && let Err(error) =
                provision_shard_from_exact_index_only(&head, &final_head, &enrollment_plan).await
        {
            effect_error = Some(error.to_string());
        }
        if effect_error.is_none() {
            if let Err(error) = crate::failpoints::maybe_fail(
                crate::failpoints::names::STREAM_ENROLLMENT_POST_SHARD_PRE_MANIFEST,
            ) {
                return Err(OmniError::recovery_required(
                    sidecar.operation_id.clone(),
                    format!(
                        "StreamEnrollmentV2 stopped after a durable physical effect and before terminal publication: {error}"
                    ),
                ));
            }
            match self
                .storage()
                .commit_staged_exact(token_head, staged_token)
                .await
            {
                Ok(outcome) if outcome.is_exact() => {}
                Ok(_) => {
                    effect_error = Some(
                        "enrollment ledger participant committed a non-exact transaction"
                            .to_string(),
                    );
                }
                Err(error) => effect_error = Some(error.to_string()),
            }
        }

        // Success, lost results, and crash recovery intentionally converge
        // through one recovery-v14 classifier/publisher implementation.
        complete_stream_enrollment_sidecar_v14(
            self.root_uri(),
            self.storage.clone(),
            &live,
            &sidecar,
        )
        .await?;
        self.refresh_coordinator_only().await?;
        let refreshed = self.coordinator.read().await.snapshot();
        let visible = refreshed
            .stream_lifecycle(live_entry.identity)
            .filter(|entry| entry.lifecycle == StreamLifecycle::Open)
            .filter(|entry| entry.binding == binding);
        let Some(visible) = visible else {
            if let Some(error) = effect_error {
                return Err(OmniError::manifest_conflict(format!(
                    "MemWAL enrollment produced no exact effect: {error}"
                )));
            }
            return Err(OmniError::manifest_internal(
                "MemWAL initializer reported success but no OPEN lifecycle became visible",
            ));
        };
        let token_dataset = refreshed.open_stream_token_authority().await?;
        let durable_receipt = lookup_enrollment_receipt_v2(
            &token_dataset,
            refreshed.stream_token_authority(),
            &terminal_receipt.graph_identity_digest,
            terminal_receipt.identity,
            &terminal_receipt.enrollment_request_id,
        )
        .await?
        .filter(|receipt| receipt == &terminal_receipt)
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "visible stream enrollment did not select its durable request receipt",
            )
        })?;
        validate_enrollment_receipt_against_lifecycle(
            &durable_receipt,
            &terminal_receipt.graph_identity_digest,
            visible,
        )?;
        Ok(PrepareStreamIngestOutcome::Enrolled(durable_receipt))
    }

    /// Feature-gated integration seam for the Phase-A crash/race suite. The
    /// ordinary library/API surface still has no stream enrollment method.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_enroll_stream_table_for_test(&self, table_key: &str) -> Result<()> {
        self.enroll_stream_table_b1(table_key, None).await
    }

    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_prepare_stream_ingest_as_for_test(
        &self,
        table_key: &str,
        enrollment_request_id: &str,
        witness_json: Option<&str>,
        actor_id: &str,
    ) -> Result<(String, Option<String>, Option<String>)> {
        let witness = witness_json
            .map(serde_json::from_str::<StreamEligibilityWitness>)
            .transpose()
            .map_err(|error| {
                OmniError::manifest(format!("invalid eligibility witness: {error}"))
            })?;
        match self
            .prepare_stream_ingest_as(table_key, enrollment_request_id, witness.as_ref(), actor_id)
            .await?
        {
            PrepareStreamIngestOutcome::WitnessRequired(witness) => Ok((
                "witness_required".into(),
                Some(
                    serde_json::to_string(&witness)
                        .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                ),
                None,
            )),
            PrepareStreamIngestOutcome::Enrolled(receipt) => {
                Ok(("enrolled".into(), None, Some(receipt.stream_incarnation_id)))
            }
            PrepareStreamIngestOutcome::AlreadyEnrolled(lifecycle) => Ok((
                "already_enrolled".into(),
                None,
                Some(lifecycle.enrollment_receipt.stream_incarnation_id),
            )),
        }
    }
}

fn validate_enrollment_receipt_against_lifecycle(
    receipt: &EnrollmentReceiptV2,
    graph_identity_digest: &str,
    lifecycle: &StreamLifecycleEntry,
) -> Result<()> {
    let legacy = &lifecycle.enrollment_receipt;
    if receipt.graph_identity_digest != graph_identity_digest
        || receipt.identity != lifecycle.identity
        || receipt.enrollment_request_id != legacy.enrollment_request_id
        || receipt.enrollment_intent_digest != legacy.enrollment_intent_digest
        || receipt.stream_incarnation_id != legacy.stream_incarnation_id
        || receipt.physical_binding != legacy.physical_binding
        || receipt.initial_lifecycle_revision != legacy.initial_lifecycle_revision
    {
        return Err(OmniError::manifest_internal(format!(
            "stream lifecycle {} is not authenticated by its enrollment receipt",
            lifecycle.identity
        )));
    }
    Ok(())
}

async fn validate_selected_binding_chain_authority(
    token_dataset: &Dataset,
    token_authority: &StreamTokenAuthorityEntry,
    enrollment: &EnrollmentReceiptV2,
    lifecycle: &StreamLifecycleEntry,
) -> Result<()> {
    enrollment.validate()?;
    lifecycle.validate()?;
    if enrollment.chain_ordinal != 1
        || enrollment.predecessor_record_id.is_some()
        || lifecycle.binding_receipt_chain.record_count < 2
        || lifecycle.binding_receipt_chain.record_count > MAX_SELECTED_BINDING_CHAIN_RECORDS
    {
        return Err(OmniError::manifest_internal(format!(
            "stream binding receipt ancestry must contain 2..={MAX_SELECTED_BINDING_CHAIN_RECORDS} records rooted at the enrollment receipt"
        )));
    }

    let mut expected = lifecycle.binding_receipt_chain.clone();
    let mut newest_to_oldest = Vec::with_capacity(
        usize::try_from(expected.record_count.saturating_sub(1)).unwrap_or(usize::MAX),
    );
    while expected.record_count > enrollment.chain_ordinal {
        let record_id = expected.head_record_id.as_deref().ok_or_else(|| {
            OmniError::manifest_internal(
                "stream binding receipt ancestry has a non-genesis count without a head",
            )
        })?;
        let record = lookup_lifecycle_ledger_record_by_id(
            token_dataset,
            token_authority,
            BINDING_RECEIPT_TAG,
            record_id,
        )
        .await?
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream binding receipt ancestry selects missing record '{record_id}'"
            ))
        })?;
        let LifecycleLedgerRecord::BindingReceipt(binding) = record else {
            return Err(OmniError::manifest_internal(
                "stream binding receipt ancestry decoded as another ledger family",
            ));
        };
        if binding.graph_identity_digest != enrollment.graph_identity_digest
            || binding.identity != enrollment.identity
            || binding.stream_incarnation_id != enrollment.stream_incarnation_id
            || binding.next_chain_ref()? != expected
        {
            return Err(OmniError::manifest_internal(
                "stream binding receipt ancestry contains a foreign, reordered, or discontinuous record",
            ));
        }
        let prior_count = binding.chain_ordinal.checked_sub(1).ok_or_else(|| {
            OmniError::manifest_internal("stream binding receipt ancestry ordinal underflow")
        })?;
        newest_to_oldest.push(binding.clone());
        expected = ReceiptChainRef {
            head_record_id: binding.predecessor_record_id,
            record_count: prior_count,
            chain_digest: binding.prior_chain_digest,
        };
    }
    validate_selected_binding_chain_records(enrollment, lifecycle, &newest_to_oldest)
}

/// Validate a fully bounded newest-to-oldest binding path. The selected head
/// remains the only current authority; historical bindings prove ancestry and
/// the immutable logical stream incarnation, never current paths or epochs.
fn validate_selected_binding_chain_records(
    enrollment: &EnrollmentReceiptV2,
    lifecycle: &StreamLifecycleEntry,
    newest_to_oldest: &[BindingReceipt],
) -> Result<()> {
    enrollment.validate()?;
    lifecycle.validate()?;
    if enrollment.chain_ordinal != 1 || enrollment.predecessor_record_id.is_some() {
        return Err(OmniError::manifest_internal(
            "stream binding receipt ancestry is not rooted at its enrollment receipt",
        ));
    }
    let expected_len = lifecycle
        .binding_receipt_chain
        .record_count
        .checked_sub(enrollment.chain_ordinal)
        .ok_or_else(|| {
            OmniError::manifest_internal(
                "stream binding receipt chain predates its enrollment receipt",
            )
        })?;
    if expected_len == 0
        || expected_len > MAX_SELECTED_BINDING_CHAIN_RECORDS.saturating_sub(1)
        || usize::try_from(expected_len).ok() != Some(newest_to_oldest.len())
    {
        return Err(OmniError::manifest_internal(
            "stream binding receipt ancestry is incomplete or exceeds its fixed traversal bound",
        ));
    }

    let mut expected = lifecycle.binding_receipt_chain.clone();
    for binding in newest_to_oldest {
        binding.validate()?;
        if binding.graph_identity_digest != enrollment.graph_identity_digest
            || binding.identity != enrollment.identity
            || binding.stream_incarnation_id != enrollment.stream_incarnation_id
            || binding.next_chain_ref()? != expected
        {
            return Err(OmniError::manifest_internal(
                "stream binding receipt ancestry contains a foreign, reordered, or discontinuous record",
            ));
        }
        expected = ReceiptChainRef {
            head_record_id: binding.predecessor_record_id.clone(),
            record_count: binding.chain_ordinal.checked_sub(1).ok_or_else(|| {
                OmniError::manifest_internal("stream binding receipt ancestry ordinal underflow")
            })?,
            chain_digest: binding.prior_chain_digest.clone(),
        };
    }

    let enrollment_chain = enrollment.next_chain_ref()?;
    let initial_binding = newest_to_oldest.last().ok_or_else(|| {
        OmniError::manifest_internal("stream enrollment has no initial binding receipt")
    })?;
    let selected_binding = newest_to_oldest.first().ok_or_else(|| {
        OmniError::manifest_internal("stream enrollment has no selected binding receipt")
    })?;
    if expected != enrollment_chain
        || initial_binding.binding_scope_id != enrollment.binding_scope_id
        || initial_binding.physical_binding != enrollment.physical_binding
        || initial_binding.operation_id != enrollment.enrollment_request_id
        || selected_binding.binding_scope_id != lifecycle.binding_scope_id
        || selected_binding.enrollment_id != lifecycle.binding.enrollment_id
        || selected_binding.physical_binding != lifecycle.binding
        || selected_binding.shard_ids != lifecycle.binding.shard_ids
        || lifecycle.current_binding_receipt_id != selected_binding.record_id
        || lifecycle.binding_receipt_chain != selected_binding.next_chain_ref()?
    {
        return Err(OmniError::manifest_internal(
            "stream selected binding chain does not terminate at its immutable initial enrollment",
        ));
    }
    Ok(())
}

pub(super) async fn validate_selected_lifecycle_ledger_authority(
    token_dataset: &Dataset,
    token_authority: &StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    lifecycle: &StreamLifecycleEntry,
) -> Result<()> {
    let binding = lookup_lifecycle_ledger_record_by_id(
        token_dataset,
        token_authority,
        BINDING_RECEIPT_TAG,
        &lifecycle.current_binding_receipt_id,
    )
    .await?
    .ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "stream lifecycle {} selects missing binding receipt '{}'",
            lifecycle.identity, lifecycle.current_binding_receipt_id
        ))
    })?;
    let LifecycleLedgerRecord::BindingReceipt(binding) = binding else {
        return Err(OmniError::manifest_internal(
            "stream lifecycle binding head decoded as another ledger family",
        ));
    };
    if binding.graph_identity_digest != graph_identity_digest
        || binding.identity != lifecycle.identity
        || binding.binding_scope_id != lifecycle.binding_scope_id
        || binding.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
        || binding.enrollment_id != lifecycle.binding.enrollment_id
        || binding.physical_binding != lifecycle.binding
        || binding.shard_ids != lifecycle.binding.shard_ids
        || binding.record_id != lifecycle.current_binding_receipt_id
        || binding.next_chain_ref()? != lifecycle.binding_receipt_chain
    {
        return Err(OmniError::manifest_internal(format!(
            "stream lifecycle {} is not authenticated by its selected binding receipt",
            lifecycle.identity
        )));
    }

    let Some(current_claim_receipt_id) = lifecycle.current_claim_receipt_id.as_deref() else {
        return Ok(());
    };
    let claim = lookup_lifecycle_ledger_record_by_id(
        token_dataset,
        token_authority,
        CLAIM_RECEIPT_TAG,
        current_claim_receipt_id,
    )
    .await?
    .ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "stream lifecycle {} selects missing claim receipt '{current_claim_receipt_id}'",
            lifecycle.identity
        ))
    })?;
    let LifecycleLedgerRecord::ClaimReceipt(claim) = claim else {
        return Err(OmniError::manifest_internal(
            "stream lifecycle claim head decoded as another ledger family",
        ));
    };
    let physical_binding_digest = stream_physical_binding_digest(&lifecycle.binding)?;
    if claim.graph_identity_digest != graph_identity_digest
        || claim.identity != lifecycle.identity
        || claim.binding_scope_id != lifecycle.binding_scope_id
        || claim.enrollment_id != lifecycle.binding.enrollment_id
        || !lifecycle.binding.shard_ids.contains(&claim.shard_id)
        || claim.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
        || claim.stream_configuration_digest != lifecycle.binding.stream_config_hash
        || claim.physical_binding_digest != physical_binding_digest
        || claim.record_id != current_claim_receipt_id
        || claim.next_chain_ref()? != lifecycle.claim_receipt_chain
        || claim.authenticated_tail_position != lifecycle.authenticated_wal_tail.position
        || claim.authenticated_tail_segment_count != lifecycle.authenticated_wal_tail.segment_count
        || claim.authenticated_tail_chain_digest != lifecycle.authenticated_wal_tail.chain_digest
        || claim.authenticated_tail_lww_projection_digest
            != lifecycle.authenticated_wal_tail.lww_projection_digest
        || lifecycle.epoch_floor_by_shard.get(&claim.shard_id).copied()
            != Some(claim.achieved_writer_epoch)
    {
        return Err(OmniError::manifest_internal(format!(
            "stream lifecycle {} is not authenticated by its selected claim receipt",
            lifecycle.identity
        )));
    }
    Ok(())
}

fn mint_distinct_stream_uuid(excluded: &[ShardId]) -> ShardId {
    loop {
        let candidate = ShardId::new_v4();
        if !excluded.contains(&candidate) {
            return candidate;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use lance::dataset::refs::BranchIdentifier;

    use super::*;
    use crate::db::manifest::stream::{
        AuthenticatedWalTail, claim_receipt_chain_genesis, management_receipt_chain_genesis,
    };

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn binding_chain_fixture() -> (
        EnrollmentReceiptV2,
        BindingReceipt,
        BindingReceipt,
        StreamLifecycleEntry,
    ) {
        let identity = TableIdentity::new(7, 9).unwrap();
        let stream_incarnation_id = "11111111-1111-4111-8111-111111111111";
        let enrollment_request_id = "22222222-2222-4222-8222-222222222222";
        let initial_scope = "33333333-3333-4333-8333-333333333333";
        let initial_shard = "44444444-4444-4444-8444-444444444444";
        let initial_binding = StreamPhysicalBinding {
            stable_table_id: identity.stable_table_id,
            table_incarnation_id: identity.table_incarnation_id,
            table_location: "nodes/0000000000000007-0000000000000009".to_string(),
            table_branch: None,
            enrollment_id: "55555555-5555-4555-8555-555555555555".to_string(),
            shard_ids: vec![initial_shard.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: digest('a'),
        };
        let graph_identity_digest = digest('b');
        let enrollment = EnrollmentReceiptV2::new(
            graph_identity_digest.clone(),
            identity,
            &binding_receipt_chain_genesis(),
            enrollment_request_id,
            digest('c'),
            "act-operator",
            stream_incarnation_id,
            initial_scope,
            initial_binding.clone(),
            1,
        )
        .unwrap();
        let initial_receipt = BindingReceipt::new(
            graph_identity_digest.clone(),
            identity,
            &enrollment.next_chain_ref().unwrap(),
            initial_scope,
            stream_incarnation_id,
            initial_binding.clone(),
            enrollment_request_id,
            2,
        )
        .unwrap();

        let rebound_scope = "66666666-6666-4666-8666-666666666666";
        let rebound_shard = "77777777-7777-4777-8777-777777777777";
        let rebound_binding = StreamPhysicalBinding {
            enrollment_id: "88888888-8888-4888-8888-888888888888".to_string(),
            shard_ids: vec![rebound_shard.to_string()],
            ..initial_binding.clone()
        };
        let rebound_receipt = BindingReceipt::new(
            graph_identity_digest,
            identity,
            &initial_receipt.next_chain_ref().unwrap(),
            rebound_scope,
            stream_incarnation_id,
            rebound_binding.clone(),
            "99999999-9999-4999-8999-999999999999",
            3,
        )
        .unwrap();
        let legacy_enrollment = EnrollmentReceipt::new(
            enrollment_request_id.to_string(),
            digest('c'),
            stream_incarnation_id.to_string(),
            initial_binding,
        )
        .unwrap();
        let lifecycle = StreamLifecycleEntry {
            identity,
            diagnostic_table_key: "node:Person".to_string(),
            lifecycle: StreamLifecycle::Open,
            binding: rebound_binding,
            binding_scope_id: rebound_scope.to_string(),
            current_head_witness: CurrentHeadWitness {
                branch_identifier: BranchIdentifier::main(),
                table_version: 4,
                transaction_uuid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                manifest_e_tag: None,
            },
            epoch_floor_by_shard: BTreeMap::from([(rebound_shard.to_string(), 1)]),
            lifecycle_revision: 2,
            enrollment_receipt: legacy_enrollment,
            current_binding_receipt_id: rebound_receipt.record_id.clone(),
            binding_receipt_chain: rebound_receipt.next_chain_ref().unwrap(),
            management_receipt_chain: management_receipt_chain_genesis(),
            claim_receipt_chain: claim_receipt_chain_genesis(),
            current_claim_receipt_id: None,
            authenticated_wal_tail: AuthenticatedWalTail::genesis(rebound_scope).unwrap(),
            drain: None,
            strict_block: None,
            sealed_proof: None,
            last_fold_summary: None,
        };
        lifecycle.validate().unwrap();
        (enrollment, initial_receipt, rebound_receipt, lifecycle)
    }

    #[test]
    fn selected_binding_chain_accepts_rebind_ancestry_and_rejects_gaps_or_reordering() {
        let (enrollment, initial, rebound, lifecycle) = binding_chain_fixture();
        validate_selected_binding_chain_records(
            &enrollment,
            &lifecycle,
            &[rebound.clone(), initial.clone()],
        )
        .unwrap();

        assert!(
            validate_selected_binding_chain_records(
                &enrollment,
                &lifecycle,
                std::slice::from_ref(&rebound),
            )
            .is_err()
        );
        assert!(
            validate_selected_binding_chain_records(&enrollment, &lifecycle, &[initial, rebound],)
                .is_err()
        );

        let mut over_bound = lifecycle;
        over_bound.binding_receipt_chain.record_count =
            MAX_SELECTED_BINDING_CHAIN_RECORDS.saturating_add(1);
        assert!(validate_selected_binding_chain_records(&enrollment, &over_bound, &[]).is_err());
    }
}
