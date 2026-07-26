//! RFC-026 bounded stream enrollment orchestration.
//!
//! This module intentionally exposes no production SDK/API surface. It joins
//! the exact MemWAL adapter, enrollment recovery sidecar, manifest lifecycle row,
//! and process-local admission lease so the crash path and success path share
//! one roll-forward implementation. Internal schema v9 fixes the complete
//! config-v3/state-v2 enrollment receipt before the recovery intent is armed.

use lance_index::mem_wal::ShardId;

use crate::db::manifest::{
    EnrollmentReceipt, RecoveryAuthorityToken, RecoveryLineageIntent, STREAM_CONFIG_VERSION,
    SidecarTablePin, StreamLifecycle, StreamPhysicalBinding,
    complete_stream_enrollment_sidecar_v10, new_stream_enrollment_sidecar_v10,
    stream_enrollment_intent_digest_v1, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::table_store::mem_wal::{
    MemWalEnrollmentPlan, capture_pre_enrollment_head, initialize_index_from_exact_no_effect,
    validate_b1_lifecycle_physical_state, validate_phase_a_stream_absent,
};

use super::Omnigraph;

impl Omnigraph {
    /// Validate the complete internal-v9 stream capability before a handle is
    /// served. Recovery has already consumed every parseable enrollment intent
    /// on read-write open; therefore a MemWAL index without a lifecycle row is
    /// uncovered partial format, while every lifecycle must match its exact
    /// current table witness and bounded config-v3 generation topology.
    pub(super) async fn validate_stream_format_consistency(&self) -> Result<()> {
        let snapshot = self.coordinator.read().await.snapshot();
        snapshot
            .open_stream_token_authority()
            .await
            .map_err(|error| {
                OmniError::manifest_internal(format!(
                    "internal-v9 stream-token authority consistency failed: {error}"
                ))
            })?;
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
                    "internal-v9 OPEN/DRAINING stream lifecycle cannot coexist with named graph branches",
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
                    "internal-v9 stream format consistency failed for '{}' ({}): {error}",
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
                    "internal-v9 stream format consistency failed for '{}' ({}): {error}",
                    entry.table_key, entry.identity
                ))
            })?;
        }
        Ok(())
    }

    /// Enroll one existing main-branch table into the bounded config-v3
    /// physical profile. Kept crate-private until Phase B2 supplies the public
    /// stream contract.
    #[allow(dead_code)]
    pub(crate) async fn enroll_stream_table_b1(
        &self,
        table_key: &str,
        actor_id: Option<&str>,
    ) -> Result<()> {
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;
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
        if let Some(existing) = txn.base.stream_lifecycle(prepared_entry.identity) {
            return Err(OmniError::manifest_stream_lifecycle_conflict(
                prepared_entry.identity.stable_table_id,
                prepared_entry.identity.table_incarnation_id,
                prepared_entry.table_key.clone(),
                existing.lifecycle.as_str(),
                "stream enrollment",
            ));
        }

        // Preparation captures exact public physical evidence without resolving
        // latest. The final capture is repeated under every effect gate below.
        let prepared_handle = self
            .storage()
            .open_snapshot_at_entry(&prepared_entry)
            .await?;
        let prepared_head = capture_pre_enrollment_head(prepared_handle.dataset())
            .await
            .map_err(|error| OmniError::manifest_conflict(error.to_string()))?;
        let enrollment_plan = MemWalEnrollmentPlan::new(ShardId::new_v4(), ShardId::new_v4())
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        // The public B2 API will accept the request ID from its caller.  This
        // still-private seam mints that id on the caller's behalf, but fixes it
        // together with a separate logical incarnation before sidecar arm.
        let enrollment_request_id =
            mint_distinct_stream_uuid(&[enrollment_plan.enrollment_id, enrollment_plan.shard_id]);
        let stream_incarnation_id = mint_distinct_stream_uuid(&[
            enrollment_plan.enrollment_id,
            enrollment_plan.shard_id,
            enrollment_request_id,
        ]);
        let binding = StreamPhysicalBinding {
            stable_table_id: prepared_entry.identity.stable_table_id,
            table_incarnation_id: prepared_entry.identity.table_incarnation_id,
            table_location: prepared_entry.table_path.clone(),
            table_branch: None,
            enrollment_id: enrollment_plan.enrollment_id.to_string(),
            shard_ids: vec![enrollment_plan.shard_id.to_string()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: enrollment_plan.stream_config_hash(),
        };
        let lineage = self.new_lineage_intent_for_branch(None, actor_id).await?;

        // Admission is deliberately outermost. Drain and future append use the
        // same identity/ref domain, preventing a post-check physical effect from
        // slipping across lifecycle closure in this process.
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
        let branches = self.coordinator.read().await.branch_list().await?;
        if branches.iter().any(|branch| branch != "main") {
            return Err(OmniError::manifest_conflict(
                "bounded MemWAL enrollment requires a main-only graph; delete named branches before enrollment",
            ));
        }
        let live = self.revalidate_write_txn(&txn).await?;
        let live_entry = live.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_enrollment_table:{table_key}"),
                Some(prepared_entry.identity.to_string()),
                None,
            )
        })?;
        if live_entry.identity != prepared_entry.identity
            || live_entry.table_path != prepared_entry.table_path
            || live_entry.table_branch.is_some()
            || live_entry.table_version != prepared_entry.table_version
            || live.stream_lifecycle(live_entry.identity).is_some()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_enrollment_binding:{table_key}"),
                Some(format!(
                    "{}:{}:main:v{}:absent",
                    prepared_entry.identity,
                    prepared_entry.table_path,
                    prepared_entry.table_version
                )),
                Some(format!(
                    "{}:{}:{:?}:v{}:{}",
                    live_entry.identity,
                    live_entry.table_path,
                    live_entry.table_branch,
                    live_entry.table_version,
                    if live.stream_lifecycle(live_entry.identity).is_some() {
                        "present"
                    } else {
                        "absent"
                    }
                )),
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
        if final_head != prepared_head {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_enrollment_head:{table_key}"),
                Some(format!("{prepared_head:?}")),
                Some(format!("{final_head:?}")),
            ));
        }

        let enrollment_intent_digest = stream_enrollment_intent_digest_v1(
            live_entry.identity,
            &live_entry.table_path,
            &txn.authority.schema_identity_domain,
            &txn.authority.schema_ir_hash,
            txn.authority.schema_identity_version,
            &final_head,
            &binding.stream_config_hash,
        )?;
        let enrollment_receipt = EnrollmentReceipt::new(
            enrollment_request_id.to_string(),
            enrollment_intent_digest,
            stream_incarnation_id.to_string(),
            binding.clone(),
        )?;

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
        let sidecar = new_stream_enrollment_sidecar_v10(
            actor_id.map(str::to_string),
            pin,
            authority,
            recovery_lineage,
            final_head,
            enrollment_plan,
            binding.clone(),
            enrollment_receipt,
        )?;
        write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_ENROLLMENT_POST_SIDECAR_PRE_INDEX,
        )?;

        let mut head = head_handle.into_dataset();
        let initializer_error = initialize_index_from_exact_no_effect(
            &mut head,
            &sidecar
                .protocol_v10
                .as_ref()
                .expect("constructor installed protocol_v10")
                .baseline_head,
            &sidecar
                .protocol_v10
                .as_ref()
                .expect("constructor installed protocol_v10")
                .enrollment_plan,
        )
        .await
        .err();
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_ENROLLMENT_POST_INDEX_PRE_RECOVERY,
        )?;

        // Success and crash recovery intentionally converge through one
        // classifier/publisher implementation. A lost initializer result is
        // therefore harmless when exact N+1 evidence exists; an exact no-effect
        // result retires the sidecar and the original error remains visible.
        complete_stream_enrollment_sidecar_v10(
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
        if visible.is_none() {
            if let Some(error) = initializer_error {
                return Err(OmniError::manifest_conflict(format!(
                    "MemWAL enrollment produced no exact effect: {error}"
                )));
            }
            return Err(OmniError::manifest_internal(
                "MemWAL initializer reported success but no OPEN lifecycle became visible",
            ));
        }
        Ok(())
    }

    /// Feature-gated integration seam for the Phase-A crash/race suite. The
    /// ordinary library/API surface still has no stream enrollment method.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_enroll_stream_table_for_test(&self, table_key: &str) -> Result<()> {
        self.enroll_stream_table_b1(table_key, None).await
    }
}

fn mint_distinct_stream_uuid(excluded: &[ShardId]) -> ShardId {
    loop {
        let candidate = ShardId::new_v4();
        if !excluded.contains(&candidate) {
            return candidate;
        }
    }
}
