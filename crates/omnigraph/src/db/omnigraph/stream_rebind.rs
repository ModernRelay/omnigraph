//! Checked offline physical stream rebind.
//!
//! This adapter is deliberately narrower than ordinary maintenance. It
//! consumes the stopped-writer cluster capability for one exact occurrence,
//! re-runs the graph-global recovery barrier, and arms recovery-v18 only after
//! recapturing one authenticated `SEALED` lane under the complete effect-gate
//! envelope. Rebind never opens admission or accepts rows; recovery publishes
//! a fresh empty binding as `SEALED` and a later resume owns `SEALED -> OPEN`.

use std::collections::BTreeSet;
use std::sync::Arc;

use lance::dataset::refs::BranchIdentifier;
use lance_index::mem_wal::ShardId;

use crate::db::manifest::stream::{
    BINDING_RECEIPT_TAG, BindingReceipt, CLAIM_RECEIPT_TAG, ClaimReceipt, EnrollmentReceiptV2,
    MAX_SELECTED_BINDING_CHAIN_RECORDS, STREAM_REBIND_OPERATION_KIND,
    STREAM_REBIND_REQUEST_PROTOCOL_VERSION, StreamRebindRequestPayload,
    retained_shard_inventory_commitment, stream_graph_identity_digest,
    stream_rebind_result_payload,
};
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, lookup_enrollment_receipt_v2, lookup_lifecycle_ledger_record_by_id,
    lookup_management_receipt,
};
use crate::db::manifest::{
    CurrentHeadWitness, RecoveryAuthorityToken, RecoveryLineageIntent,
    RecoveryStreamRebindOutcomeV18, SidecarTablePin, StreamLifecycle, StreamLifecycleEntry,
    StreamPhysicalBinding, complete_stream_rebind_sidecar_v18, new_stream_rebind_sidecar_v18,
    write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::table_store::mem_wal::{
    MemWalEnrollmentPlan, MemWalRebindPlan, PassiveB1PhysicalState, capture_current_head_witness,
    validate_b1_lifecycle_physical_state_with_binding_inventory,
};

use super::stream_enrollment::{
    load_selected_binding_chain_records, validate_selected_lifecycle_ledger_authority,
};
use super::stream_lifecycle::validate_selected_management_receipt_progress;
use super::{CheckedClusterMaintenanceAuthority, Omnigraph};

struct AuthenticatedBindingHistory {
    newest_to_oldest: Vec<BindingReceipt>,
    retained_shards: Vec<ShardId>,
    used_ids: BTreeSet<ShardId>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamRebindResultPayload {
    lifecycle: String,
    revision: u64,
    binding_scope_id: String,
    binding: StreamPhysicalBinding,
    current_head: CurrentHeadWitness,
}

impl Omnigraph {
    /// Replace one exact sealed physical binding under the stopped-writer
    /// cluster authority. No ambient/direct-store caller can mint the checked
    /// capability accepted here.
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    pub(crate) async fn stream_rebind_checked(
        &self,
        authority: CheckedClusterMaintenanceAuthority<'_>,
        table_key: &str,
        rebind_id: &str,
        expected_lifecycle_revision: u64,
    ) -> Result<()> {
        // Recovery-v18 deliberately carries the complete old/new authority
        // proof. Keep that deep async state behind one heap indirection so an
        // otherwise ordinary caller does not inherit its full poll stack.
        Box::pin(self.stream_rebind_checked_inner(
            authority,
            table_key,
            rebind_id,
            expected_lifecycle_revision,
        ))
        .await
    }

    async fn stream_rebind_checked_inner(
        &self,
        authority: CheckedClusterMaintenanceAuthority<'_>,
        table_key: &str,
        rebind_id: &str,
        expected_lifecycle_revision: u64,
    ) -> Result<()> {
        let rebind_uuid = canonical_rebind_occurrence(&authority, rebind_id)?;
        if authority.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "checked stream-rebind authority was retargeted to another graph"
                    .to_string(),
            });
        }

        self.ensure_schema_state_valid().await?;
        // Recovery owns the same root-ordered gates used below. Settle it
        // before acquiring any operation gate, then relist under the complete
        // envelope to close the post-barrier race.
        self.heal_pending_recovery_sidecars_for_write(&[None])
            .await?;

        let prepared = self.open_write_txn(None).await?;
        let prepared_entry = prepared.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "stream rebind cannot resolve unknown table '{table_key}'"
            ))
        })?;
        if prepared_entry.table_branch.is_some() {
            return Err(OmniError::manifest_conflict(
                "stream rebind is canonical-main only",
            ));
        }

        let admission_key = StreamAdmissionKey::for_resolved_ref(prepared_entry.identity, None);
        let write_queue = self.write_queue();
        let _profile_guard = write_queue.acquire_stream_profile_shared().await;
        let _admission_guard = write_queue.acquire_stream_exclusive(&admission_key).await;
        let _schema_guard = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guard = write_queue.acquire_branch(None).await;
        let _stream_token_guard = write_queue.acquire_stream_token().await;
        let _table_guards = write_queue
            .acquire_many(&[(prepared_entry.table_key.clone(), None)])
            .await;

        self.ensure_no_pending_recovery_sidecars_under_gates(&[None], "stream rebind")
            .await?;
        self.ensure_schema_state_valid().await?;

        let txn = self.open_write_txn(None).await?;
        authority.validate_profile(txn.base.stream_profile())?;
        if txn.branch.is_some() || txn.authority.branch_identifier != BranchIdentifier::main() {
            return Err(OmniError::manifest_internal(
                "stream rebind recaptured non-canonical branch authority",
            ));
        }
        let entry = txn.base.entry(table_key).cloned().ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("stream_rebind_table:{table_key}"),
                Some(prepared_entry.identity.to_string()),
                None,
            )
        })?;
        if entry.identity != prepared_entry.identity
            || entry.table_key != prepared_entry.table_key
            || entry.table_path != prepared_entry.table_path
            || entry.table_branch.is_some()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_rebind_binding:{table_key}"),
                Some(format!(
                    "{}:{}:main",
                    prepared_entry.identity, prepared_entry.table_path
                )),
                Some(format!(
                    "{}:{}:{:?}",
                    entry.identity, entry.table_path, entry.table_branch
                )),
            ));
        }

        let lifecycle = txn
            .base
            .stream_lifecycle(entry.identity)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "stream rebind requires an enrolled stream for '{table_key}'"
                ))
            })?;
        let graph_identity_digest =
            stream_graph_identity_digest(&txn.authority.schema_identity_domain)?;
        let token_dataset = txn.base.open_stream_token_authority().await?;
        let selected_retained_inventory = validate_selected_lifecycle_ledger_authority(
            &token_dataset,
            txn.base.stream_token_authority(),
            &graph_identity_digest,
            &lifecycle,
        )
        .await?;
        let enrollment = selected_enrollment_receipt(
            &token_dataset,
            txn.base.stream_token_authority(),
            &graph_identity_digest,
            &lifecycle,
        )
        .await?;
        let records = load_selected_binding_chain_records(
            &token_dataset,
            txn.base.stream_token_authority(),
            &enrollment,
            &lifecycle,
        )
        .await?;
        let history = authenticated_binding_history(records, &lifecycle)?;
        if history.newest_to_oldest[0].retained_shard_inventory_commitment()?
            != selected_retained_inventory
        {
            return Err(OmniError::manifest_internal(
                "selected binding receipt changed between fixed-size and full-history authentication",
            ));
        }
        let prior_claim = selected_claim_receipt(
            &token_dataset,
            txn.base.stream_token_authority(),
            &lifecycle,
        )
        .await?;

        // Receipt lookup intentionally precedes the compare-revision and
        // lifecycle-state refusal. A lost response must return the exact
        // already-selected occurrence rather than being misclassified as a
        // stale request against its own N+1 SEALED result.
        if selected_rebind_retry_matches(
            &token_dataset,
            txn.base.stream_token_authority(),
            &graph_identity_digest,
            &lifecycle,
            &history.newest_to_oldest,
            rebind_id,
            expected_lifecycle_revision,
            authority.expected_profile_revision(),
            authority.actor(),
        )
        .await?
        {
            return Ok(());
        }

        // Current topology constrains only a fresh physical attempt. An exact
        // selected receipt is a terminal fact and must remain replayable if a
        // named branch is created after the original success.
        let public_named_branches = current_public_named_branches(self).await?;
        if !public_named_branches.is_empty() {
            return Err(OmniError::manifest_conflict(format!(
                "stream rebind requires a main-only graph; remove public named branches first: {}",
                public_named_branches.join(", ")
            )));
        }

        if lifecycle.lifecycle_revision != expected_lifecycle_revision {
            return Err(OmniError::StreamLifecycleChanged {
                stable_table_id: entry.identity.stable_table_id,
                table_incarnation_id: entry.identity.table_incarnation_id,
                expected_revision: expected_lifecycle_revision,
                current_revision: lifecycle.lifecycle_revision,
            });
        }
        if lifecycle.lifecycle != StreamLifecycle::Sealed {
            return Err(OmniError::manifest_stream_lifecycle_conflict(
                entry.identity.stable_table_id,
                entry.identity.table_incarnation_id,
                &entry.table_key,
                lifecycle.lifecycle.as_str(),
                "stream rebind",
            ));
        }
        if lifecycle.binding.table_location != entry.table_path
            || lifecycle.binding.table_branch.is_some()
            || lifecycle.current_head_witness.table_version != entry.table_version
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_rebind_lifecycle_binding:{table_key}"),
                Some(format!(
                    "{}:{}:{}",
                    lifecycle.identity,
                    lifecycle.binding.table_location,
                    lifecycle.current_head_witness.table_version
                )),
                Some(format!(
                    "{}:{}:{}",
                    entry.identity, entry.table_path, entry.table_version
                )),
            ));
        }

        let full_path = format!(
            "{}/{}",
            self.root_uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        let head = self.storage().open_dataset_head(&full_path, None).await?;
        self.ensure_existing_effect_baseline(&entry.table_key, None, entry.table_version, &head)
            .await?;
        let observed_head = capture_current_head_witness(head.dataset())
            .await
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        if observed_head != lifecycle.current_head_witness {
            return Err(OmniError::manifest_read_set_changed(
                format!("stream_rebind_physical_head:{table_key}"),
                Some(format!("{:?}", lifecycle.current_head_witness)),
                Some(format!("{observed_head:?}")),
            ));
        }
        let physical = validate_b1_lifecycle_physical_state_with_binding_inventory(
            head.dataset(),
            &lifecycle,
            selected_retained_inventory.as_ref(),
        )
        .await
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        validate_exact_sealed_physical_cut(&lifecycle, physical)?;

        let mut used_ids = history.used_ids;
        used_ids.insert(rebind_uuid);
        let fresh_binding_scope = mint_disjoint_uuid(&mut used_ids);
        let fresh_enrollment = mint_disjoint_uuid(&mut used_ids);
        let fresh_shard = mint_disjoint_uuid(&mut used_ids);
        let claim_attempt_id = mint_disjoint_uuid(&mut used_ids);
        let next_enrollment = MemWalEnrollmentPlan::new(fresh_enrollment, fresh_shard)
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let intended_binding = StreamPhysicalBinding {
            stable_table_id: entry.identity.stable_table_id,
            table_incarnation_id: entry.identity.table_incarnation_id,
            table_location: entry.table_path.clone(),
            table_branch: None,
            enrollment_id: fresh_enrollment.to_string(),
            shard_ids: vec![fresh_shard.to_string()],
            stream_config_version: lifecycle.binding.stream_config_version,
            stream_config_hash: lifecycle.binding.stream_config_hash.clone(),
        };
        intended_binding.validate(entry.identity)?;
        let rebind_plan = MemWalRebindPlan::new(
            lifecycle.binding.clone(),
            history.retained_shards,
            next_enrollment,
        )
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let recorded_at = crate::db::now_micros()?;
        let request = StreamRebindRequestPayload {
            protocol_version: STREAM_REBIND_REQUEST_PROTOCOL_VERSION,
            graph_identity_digest: graph_identity_digest.clone(),
            identity: entry.identity,
            stream_incarnation_id: lifecycle.enrollment_receipt.stream_incarnation_id.clone(),
            binding_scope_id: lifecycle.binding_scope_id.clone(),
            enrollment_id: lifecycle.binding.enrollment_id.clone(),
            rebind_id: rebind_id.to_string(),
            expected_lifecycle_revision,
            expected_profile_revision: authority.expected_profile_revision(),
            actor_id: authority.actor().to_string(),
            public_named_branches,
        };
        request.validate_for_lifecycle(&lifecycle, authority.expected_profile_revision())?;
        let expected_terminal_shards = rebind_plan.expected_terminal_shards();
        let planned_binding_receipt = BindingReceipt::new_with_retained_shards(
            graph_identity_digest,
            entry.identity,
            &lifecycle.binding_receipt_chain,
            fresh_binding_scope.to_string(),
            lifecycle.enrollment_receipt.stream_incarnation_id.clone(),
            intended_binding.clone(),
            &expected_terminal_shards,
            rebind_id.to_string(),
            recorded_at,
        )?;
        if planned_binding_receipt.chain_ordinal > MAX_SELECTED_BINDING_CHAIN_RECORDS {
            return Err(OmniError::manifest_conflict(format!(
                "stream rebind would exceed the fixed binding receipt ancestry bound of {MAX_SELECTED_BINDING_CHAIN_RECORDS} records"
            )));
        }

        let lineage = self
            .new_lineage_intent_for_branch(None, Some(authority.actor()))
            .await?;
        let recovery_authority = RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_identity_domain: txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id,
            branch: lineage.branch,
            actor_id: lineage.actor_id,
            merged_parent_commit_id: lineage.merged_parent_commit_id,
            created_at: lineage.created_at,
        };
        let post_commit_pin = entry
            .table_version
            .checked_add(2)
            .ok_or_else(|| OmniError::manifest_internal("stream rebind table version overflow"))?;
        let pin = SidecarTablePin {
            identity: entry.identity,
            table_key: entry.table_key.clone(),
            table_path: full_path,
            expected_version: entry.table_version,
            post_commit_pin,
            confirmed_version: None,
            table_branch: None,
        };
        let sidecar = new_stream_rebind_sidecar_v18(
            authority.actor().to_string(),
            pin,
            recovery_authority,
            recovery_lineage,
            txn.base.version(),
            txn.base.stream_profile().clone(),
            lifecycle.clone(),
            history.newest_to_oldest[0].clone(),
            prior_claim,
            txn.base.stream_token_authority().clone(),
            request,
            rebind_plan,
            intended_binding,
            planned_binding_receipt,
            entry.row_count,
            claim_attempt_id.to_string(),
            recorded_at,
        )?;
        write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_REBIND_POST_SIDECAR_PRE_PHYSICAL,
        )?;

        let outcome = complete_stream_rebind_sidecar_v18(
            self.root_uri(),
            Arc::clone(&self.storage),
            &txn.base,
            &sidecar,
        )
        .await?;
        let RecoveryStreamRebindOutcomeV18::TerminalVisible {
            lifecycle: terminal,
            management_receipt,
            ..
        } = outcome
        else {
            return Err(OmniError::manifest_conflict(
                "stream rebind recovery proved the armed occurrence effect-free",
            ));
        };
        validate_terminal_rebind_outcome(
            &terminal,
            &management_receipt,
            rebind_id,
            expected_lifecycle_revision,
            authority.expected_profile_revision(),
            authority.actor(),
        )?;

        self.refresh_coordinator_only().await?;
        let refreshed = self.coordinator.read().await.snapshot();
        if refreshed.stream_lifecycle(entry.identity) != Some(&terminal) {
            return Err(OmniError::manifest_internal(
                "stream rebind terminal recovery result is not selected by the refreshed manifest",
            ));
        }
        Ok(())
    }

    /// Feature-gated integration seam for recovery-v18 crash/race tests. The
    /// checked authority remains mandatory; this does not create an ambient
    /// rebind API.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_rebind_checked_for_test(
        &self,
        authority: CheckedClusterMaintenanceAuthority<'_>,
        table_key: &str,
        rebind_id: &str,
        expected_lifecycle_revision: u64,
    ) -> Result<String> {
        self.stream_rebind_checked(authority, table_key, rebind_id, expected_lifecycle_revision)
            .await?;
        let snapshot = self.coordinator.read().await.snapshot();
        let entry = snapshot.entry(table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream rebind test seam lost table '{table_key}' after success"
            ))
        })?;
        let lifecycle = snapshot.stream_lifecycle(entry.identity).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "stream rebind test seam lost lifecycle for '{table_key}' after success"
            ))
        })?;
        Ok(lifecycle.binding_scope_id.clone())
    }
}

fn canonical_rebind_occurrence(
    authority: &CheckedClusterMaintenanceAuthority<'_>,
    rebind_id: &str,
) -> Result<ShardId> {
    let parsed = parse_canonical_uuid_v4("stream rebind rebind_id", rebind_id)?;
    if authority.operation_id() != rebind_id {
        return Err(OmniError::StreamingAuthorityMismatch {
            reason: format!(
                "checked maintenance occurrence '{}' cannot authorize rebind occurrence '{rebind_id}'",
                authority.operation_id()
            ),
        });
    }
    Ok(parsed)
}

async fn current_public_named_branches(db: &Omnigraph) -> Result<Vec<String>> {
    let mut branches = db
        .coordinator
        .read()
        .await
        .branch_list()
        .await?
        .into_iter()
        .filter(|branch| branch != "main" && !crate::db::is_internal_system_branch(branch))
        .collect::<Vec<_>>();
    branches.sort();
    branches.dedup();
    Ok(branches)
}

async fn selected_enrollment_receipt(
    token_dataset: &lance::Dataset,
    token_authority: &crate::db::manifest::StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    lifecycle: &StreamLifecycleEntry,
) -> Result<EnrollmentReceiptV2> {
    lookup_enrollment_receipt_v2(
        token_dataset,
        token_authority,
        graph_identity_digest,
        lifecycle.identity,
        &lifecycle.enrollment_receipt.enrollment_request_id,
    )
    .await?
    .ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "stream lifecycle {} selects missing enrollment receipt '{}'",
            lifecycle.identity, lifecycle.enrollment_receipt.enrollment_request_id
        ))
    })
}

async fn selected_claim_receipt(
    token_dataset: &lance::Dataset,
    token_authority: &crate::db::manifest::StreamTokenAuthorityEntry,
    lifecycle: &StreamLifecycleEntry,
) -> Result<ClaimReceipt> {
    let record_id = lifecycle
        .current_claim_receipt_id
        .as_deref()
        .ok_or_else(|| {
            OmniError::manifest_internal("stream rebind requires a selected current ClaimReceipt")
        })?;
    let record = lookup_lifecycle_ledger_record_by_id(
        token_dataset,
        token_authority,
        CLAIM_RECEIPT_TAG,
        record_id,
    )
    .await?
    .ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "stream rebind selected missing ClaimReceipt '{record_id}'"
        ))
    })?;
    let LifecycleLedgerRecord::ClaimReceipt(receipt) = record else {
        return Err(OmniError::manifest_internal(
            "stream rebind current ClaimReceipt ID decoded another ledger family",
        ));
    };
    if receipt.record_id != record_id
        || lifecycle.claim_receipt_chain.head_record_id.as_deref() != Some(record_id)
    {
        return Err(OmniError::manifest_internal(
            "stream rebind current ClaimReceipt is not the selected claim-chain head",
        ));
    }
    Ok(receipt)
}

fn authenticated_binding_history(
    records: Vec<BindingReceipt>,
    lifecycle: &StreamLifecycleEntry,
) -> Result<AuthenticatedBindingHistory> {
    let selected = records
        .first()
        .ok_or_else(|| OmniError::manifest_internal("stream rebind binding ancestry is empty"))?;
    if selected.record_tag != BINDING_RECEIPT_TAG
        || selected.record_id != lifecycle.current_binding_receipt_id
    {
        return Err(OmniError::manifest_internal(
            "stream rebind binding ancestry does not start at the selected receipt",
        ));
    }

    let mut used_ids = BTreeSet::new();
    let mut retained_shards = Vec::with_capacity(records.len());
    for binding in &records {
        let binding_scope = parse_canonical_uuid_v4(
            "historical stream binding_scope_id",
            &binding.binding_scope_id,
        )?;
        let enrollment =
            parse_canonical_uuid_v4("historical stream enrollment_id", &binding.enrollment_id)?;
        let [shard] = binding.shard_ids.as_slice() else {
            return Err(OmniError::manifest_internal(
                "stream rebind history contains a non-singleton binding",
            ));
        };
        let shard = parse_canonical_uuid_v4("historical stream shard_id", shard)?;
        for (kind, value) in [
            ("binding scope", binding_scope),
            ("enrollment", enrollment),
            ("shard", shard),
        ] {
            if !used_ids.insert(value) {
                return Err(OmniError::manifest_internal(format!(
                    "stream rebind history reuses {kind} identity {value}"
                )));
            }
        }
        retained_shards.push(shard);
    }
    let mut cumulative_shards = Vec::with_capacity(records.len());
    for (binding, shard) in records.iter().zip(retained_shards.iter()).rev() {
        cumulative_shards.push(*shard);
        cumulative_shards.sort_unstable();
        let selected_commitment = binding.retained_shard_inventory_commitment()?;
        if binding.chain_ordinal == 2 {
            if cumulative_shards.len() != 1 || selected_commitment.is_some() {
                return Err(OmniError::manifest_internal(
                    "initial binding receipt carries a non-initial retained-shard commitment",
                ));
            }
        } else {
            let expected = retained_shard_inventory_commitment(&cumulative_shards)?;
            if selected_commitment.as_ref() != Some(&expected) {
                return Err(OmniError::manifest_internal(format!(
                    "binding receipt ordinal {} does not commit to its exact cumulative shard ancestry",
                    binding.chain_ordinal
                )));
            }
        }
    }
    retained_shards.sort_unstable();
    let current_shard =
        parse_canonical_uuid_v4(
            "selected stream shard_id",
            lifecycle.binding.shard_ids.first().ok_or_else(|| {
                OmniError::manifest_internal("selected stream binding has no shard")
            })?,
        )?;
    if !retained_shards.contains(&current_shard) {
        return Err(OmniError::manifest_internal(
            "stream rebind history omits the selected current shard",
        ));
    }
    Ok(AuthenticatedBindingHistory {
        newest_to_oldest: records,
        retained_shards,
        used_ids,
    })
}

#[allow(clippy::too_many_arguments)]
async fn selected_rebind_retry_matches(
    token_dataset: &lance::Dataset,
    token_authority: &crate::db::manifest::StreamTokenAuthorityEntry,
    graph_identity_digest: &str,
    current: &StreamLifecycleEntry,
    binding_records: &[BindingReceipt],
    rebind_id: &str,
    expected_lifecycle_revision: u64,
    expected_profile_revision: u64,
    actor_id: &str,
) -> Result<bool> {
    let Some(receipt) = lookup_management_receipt(
        token_dataset,
        token_authority,
        graph_identity_digest,
        current.identity,
        &current.enrollment_receipt.stream_incarnation_id,
        STREAM_REBIND_OPERATION_KIND,
        rebind_id,
    )
    .await?
    else {
        return Ok(false);
    };
    receipt.validate(current.lifecycle_revision)?;
    let request: StreamRebindRequestPayload =
        serde_json::from_value(receipt.request_payload.clone()).map_err(|error| {
            OmniError::manifest_internal(format!(
                "selected stream rebind receipt has an invalid request payload: {error}"
            ))
        })?;
    let expected_to_revision = expected_lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream rebind revision overflow"))?;
    let result: StreamRebindResultPayload = serde_json::from_value(receipt.result_payload.clone())
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "selected stream rebind receipt has an invalid result payload: {error}"
            ))
        })?;
    result.current_head.validate()?;
    result.binding.validate(current.identity)?;
    let selected_binding = binding_records.first();
    let prior_binding = binding_records.get(1);
    let exact = receipt.graph_identity_digest == graph_identity_digest
        && receipt.identity == current.identity
        && receipt.stream_incarnation_id == current.enrollment_receipt.stream_incarnation_id
        && receipt.operation_kind == STREAM_REBIND_OPERATION_KIND
        && receipt.operation_id == rebind_id
        && receipt.from_revision == expected_lifecycle_revision
        && receipt.to_revision == expected_to_revision
        && receipt.actor_id == actor_id
        && request.protocol_version == STREAM_REBIND_REQUEST_PROTOCOL_VERSION
        && request.graph_identity_digest == graph_identity_digest
        && request.identity == current.identity
        && request.stream_incarnation_id == current.enrollment_receipt.stream_incarnation_id
        && request.rebind_id == rebind_id
        && request.expected_lifecycle_revision == expected_lifecycle_revision
        && request.expected_profile_revision == expected_profile_revision
        && request.actor_id == actor_id
        && request.public_named_branches.is_empty()
        && selected_binding.is_some_and(|binding| {
            binding.operation_id == rebind_id
                && binding.binding_scope_id == receipt.binding_scope_id
                && binding.binding_scope_id == result.binding_scope_id
                && binding.physical_binding == result.binding
                && binding.physical_binding == current.binding
                && binding.binding_scope_id == current.binding_scope_id
        })
        && prior_binding.is_some_and(|binding| {
            request.binding_scope_id == binding.binding_scope_id
                && request.enrollment_id == binding.enrollment_id
        })
        && request.request_digest()? == receipt.request_digest
        && result.lifecycle == "SEALED"
        && result.revision == expected_to_revision
        && receipt.result_payload
            == stream_rebind_result_payload(
                expected_to_revision,
                &result.binding_scope_id,
                &result.binding,
                &result.current_head,
            )?;
    if !exact {
        return Err(OmniError::StreamLifecycleIdempotencyConflict {
            stable_table_id: current.identity.stable_table_id,
            table_incarnation_id: current.identity.table_incarnation_id,
            operation_kind: STREAM_REBIND_OPERATION_KIND.to_string(),
            operation_id: rebind_id.to_string(),
        });
    }
    validate_selected_management_receipt_progress(current, &receipt, StreamLifecycle::Sealed)?;
    Ok(true)
}

fn validate_exact_sealed_physical_cut(
    lifecycle: &StreamLifecycleEntry,
    physical: PassiveB1PhysicalState,
) -> Result<()> {
    let proof = lifecycle.sealed_proof.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("stream rebind SEALED lane has no exact empty proof")
    })?;
    match physical {
        PassiveB1PhysicalState::AdmitOrReplay {
            shard_manifest_version,
            current_generation,
            replay_after_wal_entry_position,
            writer_epoch,
        } if proof.shard_manifest_version == shard_manifest_version
            && proof.current_generation == current_generation
            && current_generation.checked_sub(1) == Some(proof.base_merged_generation)
            && proof.replay_cursor == replay_after_wal_entry_position
            && proof.writer_epoch == writer_epoch =>
        {
            Ok(())
        }
        observed => Err(OmniError::manifest_internal(format!(
            "stream rebind physical state differs from the selected SEALED proof: {observed:?}"
        ))),
    }
}

fn validate_terminal_rebind_outcome(
    terminal: &StreamLifecycleEntry,
    management_receipt: &crate::db::manifest::stream::ManagementReceipt,
    rebind_id: &str,
    expected_lifecycle_revision: u64,
    expected_profile_revision: u64,
    actor_id: &str,
) -> Result<()> {
    terminal.validate()?;
    let expected_revision = expected_lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("stream rebind revision overflow"))?;
    management_receipt.validate(expected_revision)?;
    let request: StreamRebindRequestPayload =
        serde_json::from_value(management_receipt.request_payload.clone()).map_err(|error| {
            OmniError::manifest_internal(format!(
                "terminal stream rebind receipt has an invalid request payload: {error}"
            ))
        })?;
    validate_selected_management_receipt_progress(
        terminal,
        management_receipt,
        StreamLifecycle::Sealed,
    )?;
    if terminal.lifecycle != StreamLifecycle::Sealed
        || terminal.lifecycle_revision != expected_revision
        || terminal.sealed_proof.is_none()
        || management_receipt.graph_identity_digest != request.graph_identity_digest
        || management_receipt.identity != terminal.identity
        || management_receipt.identity != request.identity
        || management_receipt.stream_incarnation_id
            != terminal.enrollment_receipt.stream_incarnation_id
        || management_receipt.stream_incarnation_id != request.stream_incarnation_id
        || management_receipt.binding_scope_id != terminal.binding_scope_id
        || management_receipt.next_chain_ref()? != terminal.management_receipt_chain
        || management_receipt.operation_kind != STREAM_REBIND_OPERATION_KIND
        || management_receipt.operation_id != rebind_id
        || management_receipt.from_revision != expected_lifecycle_revision
        || management_receipt.to_revision != expected_revision
        || management_receipt.actor_id != actor_id
        || request.protocol_version != STREAM_REBIND_REQUEST_PROTOCOL_VERSION
        || request.rebind_id != rebind_id
        || request.expected_lifecycle_revision != expected_lifecycle_revision
        || request.expected_profile_revision != expected_profile_revision
        || request.actor_id != actor_id
        || !request.public_named_branches.is_empty()
        || request.request_digest()? != management_receipt.request_digest
        || management_receipt.result_payload
            != stream_rebind_result_payload(
                expected_revision,
                &terminal.binding_scope_id,
                &terminal.binding,
                &terminal.current_head_witness,
            )?
    {
        return Err(OmniError::manifest_internal(
            "StreamRebind recovery returned a non-exact or non-SEALED terminal outcome",
        ));
    }
    Ok(())
}

fn parse_canonical_uuid_v4(label: &str, value: &str) -> Result<ShardId> {
    let parsed = ShardId::parse_str(value)
        .map_err(|error| OmniError::manifest(format!("invalid {label}: {error}")))?;
    if parsed.is_nil() || parsed.get_version_num() != 4 || parsed.to_string() != value {
        return Err(OmniError::manifest(format!(
            "{label} is not a canonical UUID v4 value"
        )));
    }
    Ok(parsed)
}

fn mint_disjoint_uuid(used: &mut BTreeSet<ShardId>) -> ShardId {
    loop {
        let candidate = ShardId::new_v4();
        if used.insert(candidate) {
            return candidate;
        }
    }
}
