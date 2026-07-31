//! Recovery-v18 physical stream rebind.
//!
//! This is intentionally a separate recovery grammar from the frozen v14
//! scaffold and from v17's same-binding Optimize envelope. One exact old
//! `SEALED` lane owns a forward-only table metadata replacement, a fresh empty
//! shard plus fence, one immutable ledger transaction, and one terminal
//! `SEALED` manifest publication.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use lance::dataset::mem_wal::WalTailer;
use serde::{Deserialize, Serialize};

use super::*;
use crate::db::manifest::stream::{
    BindingReceipt, ClaimAttemptEffect, ClaimProfile, ClaimReceipt,
    MAX_SELECTED_BINDING_CHAIN_RECORDS, ManagementReceipt, STREAM_REBIND_OPERATION_KIND,
    StreamLifecycle, StreamRebindRequestPayload, claim_attempt_chain_genesis,
    retained_shard_inventory_commitment, stream_rebind_result_payload,
};
use crate::db::omnigraph::stream_lifecycle::{
    BuiltTerminalClaim, ClaimAttemptEvidence, ClaimAttemptRequest, ClaimOperationRequest,
    EmptyCutEvidence, authenticate_claim_wal_segment, build_claim_attempt_effect,
    build_rebind_adoption_row, build_rebind_claim_authority, build_terminal_claim,
    claim_wal_authentication_plan, claim_wal_key_discovery_plan, collect_claim_wal_segment_keys,
    current_generation_lww_projection_digest, prepare_claim_attempt,
    prepare_rebind_claim_operation,
};
use crate::table_store::mem_wal::{
    MemWalRebindFenceEvidence, MemWalRebindPlan, MemWalRebindState, classify_rebind,
    drop_current_index_from_exact_rebind_no_effect, initialize_fresh_index_from_exact_rebind_drop,
    provision_rebind_shard_from_exact_index_only,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum RecoveryStreamRebindPhaseV18 {
    PhysicalArmed,
    LedgerArmed,
    LedgerEffectsConfirmed,
}

/// Exact table and shard facts from which the terminal receipts are rebuilt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamRebindPhysicalV18 {
    pub dropped_head: super::super::CurrentHeadWitness,
    pub fresh_index_head: super::super::CurrentHeadWitness,
    pub shard_manifest_version: u64,
    pub writer_epoch: u64,
    pub replay_cursor: u64,
    pub current_generation: u64,
    pub base_merged_generation: u64,
    pub sentinel_position: u64,
    pub confirmed_update: RecoveryConfirmedTableUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamRebindTerminalV18 {
    pub claim_authority: super::super::StreamLifecycleEntry,
    pub claim_attempt: ClaimAttemptEffect,
    pub claim_receipt: ClaimReceipt,
    pub management_receipt: ManagementReceipt,
    pub next_lifecycle: super::super::StreamLifecycleEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamRebindLedgerV18 {
    pub planned_transaction: StagedTransactionIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_transaction: Option<StagedTransactionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_head: Option<super::super::CurrentHeadWitness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_authority: Option<super::super::StreamTokenAuthorityEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamRebindV18 {
    pub authority: RecoveryAuthorityToken,
    pub lineage: RecoveryLineageIntent,
    pub admission_scope: RecoveryStreamAdmissionScope,
    pub prior_manifest_version: u64,
    pub profile: super::super::StreamProfileEntry,
    pub prior_lifecycle: super::super::StreamLifecycleEntry,
    pub prior_binding_receipt: BindingReceipt,
    pub prior_claim_receipt: ClaimReceipt,
    pub prior_token_authority: super::super::StreamTokenAuthorityEntry,
    pub request: StreamRebindRequestPayload,
    pub rebind_plan: MemWalRebindPlan,
    pub intended_binding: super::super::StreamPhysicalBinding,
    pub planned_binding_receipt: BindingReceipt,
    pub prior_row_count: u64,
    pub claim_attempt_id: String,
    pub recorded_at: i64,
    pub phase: RecoveryStreamRebindPhaseV18,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical: Option<RecoveryStreamRebindPhysicalV18>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<RecoveryStreamRebindTerminalV18>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<RecoveryStreamRebindLedgerV18>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", deny_unknown_fields)]
pub(crate) enum RecoveryProtocolV18 {
    StreamRebind(RecoveryStreamRebindV18),
}

impl RecoveryProtocolV18 {
    pub(super) fn admission_scope(&self) -> &RecoveryStreamAdmissionScope {
        match self {
            Self::StreamRebind(protocol) => &protocol.admission_scope,
        }
    }
}

fn protocol(sidecar: &RecoverySidecar) -> &RecoveryStreamRebindV18 {
    let RecoveryProtocolV18::StreamRebind(protocol) = sidecar
        .protocol_v18
        .as_deref()
        .expect("validated schema-v18 StreamRebind");
    protocol
}

fn protocol_mut(sidecar: &mut RecoverySidecar) -> &mut RecoveryStreamRebindV18 {
    let RecoveryProtocolV18::StreamRebind(protocol) = sidecar
        .protocol_v18
        .as_deref_mut()
        .expect("validated schema-v18 StreamRebind");
    protocol
}

fn rebind_error(sidecar: &RecoverySidecar, reason: impl std::fmt::Display) -> OmniError {
    OmniError::recovery_required(
        sidecar.operation_id.clone(),
        format!("StreamRebind recovery cannot prove an exact outcome: {reason}"),
    )
}

fn rebuild_claim_plan(
    sidecar: &RecoverySidecar,
) -> Result<(
    crate::db::omnigraph::stream_lifecycle::PreparedClaimAttempt,
    ClaimAttemptEffect,
)> {
    let protocol = protocol(sidecar);
    let physical = protocol
        .physical
        .as_ref()
        .ok_or_else(|| rebind_error(sidecar, "terminal state has no physical confirmation"))?;
    let authority = build_rebind_claim_authority(
        &protocol.prior_lifecycle,
        &protocol.planned_binding_receipt,
        physical.fresh_index_head.clone(),
        1,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let [shard_id] = protocol.intended_binding.shard_ids.as_slice() else {
        return Err(rebind_error(sidecar, "fresh binding is not unsharded"));
    };
    let operation = prepare_rebind_claim_operation(
        &protocol.prior_lifecycle,
        &authority,
        ClaimOperationRequest {
            graph_identity_digest: protocol.request.graph_identity_digest.clone(),
            claim_id: protocol.request.rebind_id.clone(),
            lifecycle_operation_id: Some(protocol.request.rebind_id.clone()),
            recovery_operation_id: sidecar.operation_id.clone(),
            claim_kind: STREAM_REBIND_OPERATION_KIND.to_string(),
            profile: ClaimProfile::RetainAll,
            shard_id: shard_id.clone(),
            initial_shard_manifest_version: 1,
            initial_writer_epoch: 1,
            initial_replay_cursor: 0,
            initial_current_generation: 1,
            initial_base_merged_generation: 0,
            claim_contract_version: 1,
        },
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let pre_manifest_version = physical
        .shard_manifest_version
        .checked_sub(1)
        .ok_or_else(|| rebind_error(sidecar, "fresh claim manifest has no predecessor"))?;
    let pre_writer_epoch = physical
        .writer_epoch
        .checked_sub(1)
        .ok_or_else(|| rebind_error(sidecar, "fresh claim epoch has no predecessor"))?;
    let attempt = prepare_claim_attempt(
        &operation,
        ClaimAttemptRequest {
            attempt_id: protocol.claim_attempt_id.clone(),
            pre_shard_manifest_version: pre_manifest_version,
            pre_writer_epoch,
            pre_replay_cursor: physical.replay_cursor,
            planned_sentinel_position: physical.sentinel_position,
            planned_writer_epoch: physical.writer_epoch,
            storage_envelope_digest: None,
        },
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let effect = build_claim_attempt_effect(
        &claim_attempt_chain_genesis(),
        &attempt,
        ClaimAttemptEvidence::StockManifestPlusSentinel {
            achieved_shard_manifest_version: physical.shard_manifest_version,
            achieved_writer_epoch: physical.writer_epoch,
        },
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    Ok((attempt, effect))
}

pub(super) fn validate_rebind_inventory_authority(
    prior_binding_receipt: &BindingReceipt,
    prior_binding: &super::super::StreamPhysicalBinding,
    rebind_plan: &MemWalRebindPlan,
    planned_binding_receipt: &BindingReceipt,
) -> Result<()> {
    let expected_terminal_inventory =
        retained_shard_inventory_commitment(&rebind_plan.expected_terminal_shards())?;
    if planned_binding_receipt
        .retained_shard_inventory_commitment()?
        .as_ref()
        != Some(&expected_terminal_inventory)
    {
        return Err(OmniError::manifest_internal(
            "StreamRebind planned binding receipt does not commit to the exact terminal shard inventory",
        ));
    }
    let prior_inventory = prior_binding_receipt.retained_shard_inventory_commitment()?;
    let plan_prior_inventory =
        retained_shard_inventory_commitment(&rebind_plan.retained_shard_ids)?;
    let selected_prior_shard = prior_binding.shard_ids.first().ok_or_else(|| {
        OmniError::manifest_internal("StreamRebind selected prior binding has no shard")
    })?;
    let prior_inventory_matches = if prior_binding_receipt.chain_ordinal == 2 {
        prior_inventory.is_none()
            && rebind_plan.retained_shard_ids.len() == 1
            && rebind_plan.retained_shard_ids[0].to_string() == *selected_prior_shard
    } else {
        prior_inventory.as_ref() == Some(&plan_prior_inventory)
    };
    if !prior_inventory_matches {
        return Err(OmniError::manifest_internal(
            "StreamRebind retained-shard plan does not match the selected prior binding receipt",
        ));
    }
    Ok(())
}

pub(super) fn validate_stream_protocol_v18_shape(
    sidecar_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let malformed = |reason: String| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' has an invalid schema-v{} shape: {}",
            sidecar_uri, sidecar.schema_version, reason
        ))
    };
    if sidecar.branch.is_some()
        || sidecar.writer_kind != SidecarKind::StreamRebind
        || sidecar.actor_id.is_none()
        || sidecar.tables.len() != 1
        || sidecar.protocol_v3.is_some()
        || sidecar.protocol_v4.is_some()
        || sidecar.protocol_v7.is_some()
        || sidecar.protocol_v8.is_some()
        || sidecar.protocol_v10.is_some()
        || sidecar.protocol_v11.is_some()
        || sidecar.protocol_v12.is_some()
        || sidecar.protocol_v13.is_some()
        || sidecar.protocol_v14.is_some()
        || sidecar.protocol_v15.is_some()
        || sidecar.protocol_v16.is_some()
        || sidecar.protocol_v17.is_some()
        || sidecar.ensure_indices_rollback_v6.is_some()
        || sidecar.merge_source_commit_id.is_some()
        || !sidecar.additional_registrations.is_empty()
        || !sidecar.tombstones.is_empty()
        || sidecar.schema_apply_manifest_published
        || sidecar.schema_apply_target_schema_ir_hash.is_some()
    {
        return Err(malformed(
            "schema-v18 StreamRebind must target canonical main and carry only one table pin plus protocol_v18 authority"
                .to_string(),
        ));
    }
    let protocol = match sidecar.protocol_v18.as_deref() {
        Some(RecoveryProtocolV18::StreamRebind(protocol)) => protocol,
        None => {
            return Err(malformed(
                "missing required protocol_v18 payload".to_string(),
            ));
        }
    };
    validate_authority_identity(&malformed, &protocol.authority)?;
    validate_stream_admission_scope(&malformed, &protocol.admission_scope)?;
    protocol
        .profile
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .prior_lifecycle
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .prior_binding_receipt
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .prior_claim_receipt
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .prior_token_authority
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .request
        .validate_for_lifecycle(&protocol.prior_lifecycle, protocol.profile.profile_revision)
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .rebind_plan
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .intended_binding
        .validate(protocol.prior_lifecycle.identity)
        .map_err(|error| malformed(error.to_string()))?;
    protocol
        .planned_binding_receipt
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    let expected_planned_binding_ordinal = protocol
        .prior_lifecycle
        .binding_receipt_chain
        .record_count
        .checked_add(1)
        .ok_or_else(|| malformed("StreamRebind binding receipt chain overflows".to_string()))?;
    if protocol.planned_binding_receipt.chain_ordinal != expected_planned_binding_ordinal
        || protocol.planned_binding_receipt.chain_ordinal > MAX_SELECTED_BINDING_CHAIN_RECORDS
    {
        return Err(malformed(
            "StreamRebind planned binding receipt is not the exact next bounded binding-chain ordinal"
                .to_string(),
        ));
    }
    validate_rebind_inventory_authority(
        &protocol.prior_binding_receipt,
        &protocol.prior_lifecycle.binding,
        &protocol.rebind_plan,
        &protocol.planned_binding_receipt,
    )
    .map_err(|error| malformed(error.to_string()))?;

    let pin = &sidecar.tables[0];
    let expected_drop = pin.expected_version.checked_add(1).ok_or_else(|| {
        malformed("StreamRebind table version overflows its index-drop result".to_string())
    })?;
    let expected_fresh = expected_drop.checked_add(1).ok_or_else(|| {
        malformed("StreamRebind table version overflows its fresh-index result".to_string())
    })?;
    let [fresh_shard] = protocol.intended_binding.shard_ids.as_slice() else {
        return Err(malformed(
            "StreamRebind intended binding must contain one shard".to_string(),
        ));
    };
    let expected_graph_identity_digest = super::super::stream::stream_graph_identity_digest(
        &protocol.authority.schema_identity_domain,
    )
    .map_err(|error| malformed(error.to_string()))?;
    if protocol.authority.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || protocol.lineage.branch.is_some()
        || protocol.lineage.merged_parent_commit_id.is_some()
        || protocol.lineage.actor_id != sidecar.actor_id
        || protocol.lineage.graph_commit_id.is_empty()
        || protocol.prior_manifest_version == 0
        || protocol.profile.mode() != super::super::StreamProfileMode::Disabled
        || protocol.request.graph_identity_digest != expected_graph_identity_digest
        || protocol.prior_lifecycle.lifecycle != StreamLifecycle::Sealed
        || protocol.admission_scope.identity != protocol.prior_lifecycle.identity
        || protocol.admission_scope.table_branch.is_some()
        || protocol.admission_scope.binding_scope_id != protocol.prior_lifecycle.binding_scope_id
        || pin.identity != protocol.prior_lifecycle.identity
        || pin.table_branch.is_some()
        || pin.expected_version != protocol.prior_lifecycle.current_head_witness.table_version
        || pin.post_commit_pin != expected_fresh
        || pin.confirmed_version.is_some()
        || protocol.prior_binding_receipt.record_id
            != protocol.prior_lifecycle.current_binding_receipt_id
        || protocol.prior_binding_receipt.graph_identity_digest
            != protocol.request.graph_identity_digest
        || protocol.prior_binding_receipt.identity != protocol.prior_lifecycle.identity
        || protocol.prior_binding_receipt.binding_scope_id
            != protocol.prior_lifecycle.binding_scope_id
        || protocol.prior_binding_receipt.physical_binding != protocol.prior_lifecycle.binding
        || protocol
            .prior_binding_receipt
            .next_chain_ref()
            .map_err(|e| malformed(e.to_string()))?
            != protocol.prior_lifecycle.binding_receipt_chain
        || protocol.prior_claim_receipt.record_id
            != protocol
                .prior_lifecycle
                .current_claim_receipt_id
                .clone()
                .unwrap_or_default()
        || protocol.prior_claim_receipt.graph_identity_digest
            != protocol.request.graph_identity_digest
        || protocol.prior_claim_receipt.identity != protocol.prior_lifecycle.identity
        || protocol.prior_claim_receipt.binding_scope_id
            != protocol.prior_lifecycle.binding_scope_id
        || protocol.prior_claim_receipt.enrollment_id
            != protocol.prior_lifecycle.binding.enrollment_id
        || protocol
            .prior_claim_receipt
            .next_chain_ref()
            .map_err(|e| malformed(e.to_string()))?
            != protocol.prior_lifecycle.claim_receipt_chain
        || protocol.rebind_plan.prior_binding != protocol.prior_lifecycle.binding
        || protocol
            .rebind_plan
            .next_enrollment
            .enrollment_id
            .to_string()
            != protocol.intended_binding.enrollment_id
        || protocol.rebind_plan.next_enrollment.shard_id.to_string() != *fresh_shard
        || protocol.intended_binding.table_location
            != protocol.prior_lifecycle.binding.table_location
        || protocol.intended_binding.table_branch.is_some()
        || protocol.intended_binding.stream_config_version
            != protocol.prior_lifecycle.binding.stream_config_version
        || protocol.intended_binding.stream_config_hash
            != protocol.prior_lifecycle.binding.stream_config_hash
        || protocol.planned_binding_receipt.graph_identity_digest
            != protocol.request.graph_identity_digest
        || protocol.planned_binding_receipt.identity != pin.identity
        || protocol.planned_binding_receipt.binding_scope_id
            == protocol.prior_lifecycle.binding_scope_id
        || protocol.planned_binding_receipt.stream_incarnation_id
            != protocol
                .prior_lifecycle
                .enrollment_receipt
                .stream_incarnation_id
        || protocol.planned_binding_receipt.physical_binding != protocol.intended_binding
        || protocol.planned_binding_receipt.operation_id != protocol.request.rebind_id
        || protocol.planned_binding_receipt.prior_chain_digest
            != protocol.prior_lifecycle.binding_receipt_chain.chain_digest
        || protocol.planned_binding_receipt.predecessor_record_id
            != protocol
                .prior_lifecycle
                .binding_receipt_chain
                .head_record_id
        || protocol.prior_row_count > usize::MAX as u64
        || protocol.recorded_at <= 0
        || sidecar.actor_id.as_deref() != Some(protocol.request.actor_id.as_str())
    {
        return Err(malformed(
            "StreamRebind authority, pin, request, old receipts, or fresh binding plan disagree"
                .to_string(),
        ));
    }
    validate_canonical_uuid_text(
        &malformed,
        "StreamRebind claim_attempt_id",
        &protocol.claim_attempt_id,
        true,
    )?;

    match (
        protocol.phase,
        protocol.physical.as_ref(),
        protocol.terminal.as_ref(),
        protocol.ledger.as_ref(),
    ) {
        (RecoveryStreamRebindPhaseV18::PhysicalArmed, None, None, None) => {}
        (
            RecoveryStreamRebindPhaseV18::LedgerArmed
            | RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed,
            Some(physical),
            Some(terminal),
            Some(ledger),
        ) => {
            physical
                .dropped_head
                .validate()
                .map_err(|error| malformed(error.to_string()))?;
            physical
                .fresh_index_head
                .validate()
                .map_err(|error| malformed(error.to_string()))?;
            terminal
                .claim_authority
                .validate()
                .map_err(|error| malformed(error.to_string()))?;
            terminal
                .claim_attempt
                .validate_for_profile(ClaimProfile::RetainAll)
                .map_err(|error| malformed(error.to_string()))?;
            terminal
                .claim_receipt
                .validate()
                .map_err(|error| malformed(error.to_string()))?;
            terminal
                .management_receipt
                .validate(terminal.next_lifecycle.lifecycle_revision)
                .map_err(|error| malformed(error.to_string()))?;
            terminal
                .next_lifecycle
                .validate()
                .map_err(|error| malformed(error.to_string()))?;
            if physical.dropped_head.table_version != expected_drop
                || physical.fresh_index_head.table_version != expected_fresh
                || physical.dropped_head.branch_identifier
                    != lance::dataset::refs::BranchIdentifier::main()
                || physical.fresh_index_head.branch_identifier
                    != lance::dataset::refs::BranchIdentifier::main()
                || physical.dropped_head.transaction_uuid
                    == protocol
                        .prior_lifecycle
                        .current_head_witness
                        .transaction_uuid
                || physical.fresh_index_head.transaction_uuid
                    == physical.dropped_head.transaction_uuid
                || physical.confirmed_update.table_version != expected_fresh
                || physical.confirmed_update.table_branch.is_some()
                || physical.confirmed_update.row_count != protocol.prior_row_count
                || physical.shard_manifest_version < 2
                || physical.writer_epoch < 2
                || physical.replay_cursor != 0
                || physical.current_generation != 1
                || physical.base_merged_generation != 0
                || physical.sentinel_position != 1
                || terminal.claim_authority.current_head_witness != physical.fresh_index_head
                || terminal.claim_authority.binding != protocol.intended_binding
                || terminal.claim_attempt.identity != pin.identity
                || terminal.claim_attempt.binding_scope_id
                    != protocol.planned_binding_receipt.binding_scope_id
                || terminal.claim_receipt.identity != pin.identity
                || terminal.claim_receipt.record_id
                    != terminal
                        .next_lifecycle
                        .current_claim_receipt_id
                        .clone()
                        .unwrap_or_default()
                || terminal
                    .claim_receipt
                    .next_chain_ref()
                    .map_err(|error| malformed(error.to_string()))?
                    != terminal.next_lifecycle.claim_receipt_chain
                || terminal.management_receipt.operation_kind != STREAM_REBIND_OPERATION_KIND
                || terminal.management_receipt.operation_id != protocol.request.rebind_id
                || terminal
                    .management_receipt
                    .next_chain_ref()
                    .map_err(|error| malformed(error.to_string()))?
                    != terminal.next_lifecycle.management_receipt_chain
                || terminal.next_lifecycle.identity != pin.identity
                || terminal.next_lifecycle.current_head_witness != physical.fresh_index_head
                || terminal
                    .next_lifecycle
                    .validate_rebind_successor_of(&protocol.prior_lifecycle)
                    .is_err()
            {
                return Err(malformed(
                    "StreamRebind confirmed physical facts or terminal ledger authority disagree"
                        .to_string(),
                ));
            }
            let (_, rebuilt_effect) =
                rebuild_claim_plan(sidecar).map_err(|error| malformed(error.to_string()))?;
            if rebuilt_effect != terminal.claim_attempt {
                return Err(malformed(
                    "StreamRebind claim-attempt row differs from its exact fresh fence".to_string(),
                ));
            }
            let rebuilt_built = BuiltTerminalClaim {
                receipt: terminal.claim_receipt.clone(),
                next_claim_chain: terminal
                    .claim_receipt
                    .next_chain_ref()
                    .map_err(|error| malformed(error.to_string()))?,
                next_authenticated_tail: terminal.next_lifecycle.authenticated_wal_tail.clone(),
            };
            let rebuilt_lifecycle = build_rebind_adoption_row(
                &protocol.prior_lifecycle,
                &terminal.claim_authority,
                &protocol.planned_binding_receipt,
                &rebuilt_built,
                &terminal.management_receipt,
                protocol.profile.profile_revision,
                EmptyCutEvidence {
                    shard_manifest_version: physical.shard_manifest_version,
                    writer_epoch: physical.writer_epoch,
                    replay_cursor: physical.replay_cursor,
                    current_generation: physical.current_generation,
                    base_merged_generation: physical.base_merged_generation,
                },
            )
            .map_err(|error| malformed(error.to_string()))?;
            if rebuilt_lifecycle != terminal.next_lifecycle {
                return Err(malformed(
                    "StreamRebind terminal lifecycle differs from its exact receipts and physical cut"
                        .to_string(),
                ));
            }
            let expected_token_post = ledger
                .planned_transaction
                .read_version
                .checked_add(1)
                .ok_or_else(|| malformed("stream-token version overflow".to_string()))?;
            match protocol.phase {
                RecoveryStreamRebindPhaseV18::LedgerArmed
                    if ledger.confirmed_transaction.is_none()
                        && ledger.confirmed_head.is_none()
                        && ledger.next_authority.is_none() => {}
                RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed
                    if ledger.confirmed_transaction.as_ref()
                        == Some(&ledger.planned_transaction)
                        && ledger.confirmed_head.as_ref().is_some_and(|head| {
                            head.table_version == expected_token_post
                                && head.transaction_uuid == ledger.planned_transaction.uuid
                        })
                        && ledger.next_authority.as_ref().is_some_and(|authority| {
                            Some(&authority.current_head_witness) == ledger.confirmed_head.as_ref()
                                && authority.location == protocol.prior_token_authority.location
                                && authority.schema_version
                                    == protocol.prior_token_authority.schema_version
                                && authority.schema_hash
                                    == protocol.prior_token_authority.schema_hash
                        }) => {}
                _ => {
                    return Err(malformed(
                        "StreamRebind ledger confirmation is incomplete or phase-inconsistent"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(malformed(
                "StreamRebind physical, terminal, and ledger fields must advance as one bounded phase"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

// The implementation functions below are kept in this module so the generic
// recovery dispatcher cannot accidentally classify rebind through loose table
// recovery.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryStreamRebindOutcomeV18 {
    EffectFree,
    TerminalVisible {
        lifecycle: super::super::StreamLifecycleEntry,
        token_authority: super::super::StreamTokenAuthorityEntry,
        management_receipt: ManagementReceipt,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebindLedgerState {
    ExactNoEffect,
    ExactEffect,
}

struct ObservedRebindLedger {
    state: RebindLedgerState,
    transaction: Option<StagedTransactionIdentity>,
    authority: super::super::StreamTokenAuthorityEntry,
}

fn rebind_ledger_records(
    terminal: &RecoveryStreamRebindTerminalV18,
    binding: &BindingReceipt,
) -> [super::super::token_store::LifecycleLedgerRecord; 4] {
    [
        super::super::token_store::LifecycleLedgerRecord::BindingReceipt(binding.clone()),
        super::super::token_store::LifecycleLedgerRecord::ClaimAttemptEffect(
            terminal.claim_attempt.clone(),
        ),
        super::super::token_store::LifecycleLedgerRecord::ClaimReceipt(
            terminal.claim_receipt.clone(),
        ),
        super::super::token_store::LifecycleLedgerRecord::ManagementReceipt(
            terminal.management_receipt.clone(),
        ),
    ]
}

async fn rewrite_rebind_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    write_sidecar(root_uri, storage, sidecar).await.map(|_| ())
}

async fn current_public_named_branches(root_uri: &str) -> Result<Vec<String>> {
    let root_uri = root_uri.to_string();
    crate::instrumentation::spawn_with_query_io_probes(async move {
        let coordinator = Box::pin(ManifestCoordinator::open(&root_uri)).await?;
        let mut branches = Box::pin(coordinator.list_branches())
            .await?
            .into_iter()
            .filter(|branch| branch != "main" && !crate::db::is_internal_system_branch(branch))
            .collect::<Vec<_>>();
        branches.sort();
        branches.dedup();
        Ok(branches)
    })
    .await
    .map_err(|error| {
        OmniError::Lance(format!(
            "stream rebind topology probe task failed before returning its read-only result: {error}"
        ))
    })?
}

async fn validate_rebind_topology(root_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let expected = &protocol(sidecar).request.public_named_branches;
    let observed = current_public_named_branches(root_uri)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    if &observed != expected {
        return Err(rebind_error(
            sidecar,
            format!(
                "public graph-branch topology changed after rebind was armed: expected {expected:?}, observed {observed:?}"
            ),
        ));
    }
    Ok(())
}

fn manifest_is_prior(snapshot: &Snapshot, sidecar: &RecoverySidecar) -> bool {
    let protocol = protocol(sidecar);
    let pin = &sidecar.tables[0];
    snapshot.version() == protocol.prior_manifest_version
        && snapshot.stream_profile() == &protocol.profile
        && snapshot.stream_token_authority() == &protocol.prior_token_authority
        && snapshot.stream_lifecycle(pin.identity) == Some(&protocol.prior_lifecycle)
        && snapshot_entry_by_identity(snapshot, pin.identity).is_some_and(|entry| {
            entry.table_key == pin.table_key
                && entry.table_path == protocol.prior_lifecycle.binding.table_location
                && entry.table_version == pin.expected_version
                && entry.table_branch.is_none()
                && entry.row_count == protocol.prior_row_count
        })
}

fn lineage_matches(
    commit: &super::super::GraphLineageRow,
    protocol: &RecoveryStreamRebindV18,
) -> bool {
    commit.manifest_branch.is_none()
        && commit.parent_commit_id.as_ref() == protocol.authority.graph_head.as_ref()
        && commit.merged_parent_commit_id.is_none()
        && commit.actor_id == protocol.lineage.actor_id
        && commit.created_at == protocol.lineage.created_at
        && commit.stream_fold_attribution.is_none()
}

async fn terminal_manifest_visible(root_uri: &str, sidecar: &RecoverySidecar) -> Result<bool> {
    let protocol = protocol(sidecar);
    if protocol.phase != RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed {
        return Ok(false);
    }
    let terminal = protocol
        .terminal
        .as_ref()
        .expect("validated confirmed StreamRebind terminal");
    let physical = protocol
        .physical
        .as_ref()
        .expect("validated confirmed StreamRebind physical effect");
    let next_authority = protocol
        .ledger
        .as_ref()
        .and_then(|ledger| ledger.next_authority.as_ref())
        .expect("validated confirmed StreamRebind token authority");
    let (commits, _) = ManifestCoordinator::read_graph_lineage_at(root_uri, None).await?;
    let Some(commit) = commits
        .iter()
        .find(|commit| commit.graph_commit_id == protocol.lineage.graph_commit_id)
    else {
        return Ok(false);
    };
    if !lineage_matches(commit, protocol) {
        return Err(rebind_error(
            sidecar,
            "fixed rebind graph commit has mismatched lineage",
        ));
    }
    let committed =
        ManifestCoordinator::snapshot_at(root_uri, None, commit.manifest_version).await?;
    let expected_manifest = protocol
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| rebind_error(sidecar, "manifest version overflow"))?;
    let pin = &sidecar.tables[0];
    let update = &physical.confirmed_update;
    let entry_matches = snapshot_entry_by_identity(&committed, pin.identity).is_some_and(|entry| {
        entry.table_key == pin.table_key
            && entry.table_path == protocol.intended_binding.table_location
            && entry.table_version == update.table_version
            && entry.table_branch == update.table_branch
            && entry.row_count == update.row_count
            && entry.version_metadata == update.version_metadata
    });
    if committed.version() != expected_manifest
        || committed.stream_profile() != &protocol.profile
        || committed.stream_token_authority() != next_authority
        || committed.stream_lifecycle(pin.identity) != Some(&terminal.next_lifecycle)
        || !entry_matches
    {
        return Err(rebind_error(
            sidecar,
            "fixed rebind graph commit differs from the exact table/token/lifecycle outcome",
        ));
    }
    Ok(true)
}

async fn physical_confirmation(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    dropped_head: super::super::CurrentHeadWitness,
    fresh_head: super::super::CurrentHeadWitness,
    evidence: MemWalRebindFenceEvidence,
) -> Result<RecoveryStreamRebindPhysicalV18> {
    let protocol = protocol(sidecar);
    let pin = &sidecar.tables[0];
    let fresh = crate::instrumentation::open_dataset(
        &pin.table_path,
        crate::instrumentation::VersionResolution::At(fresh_head.table_version),
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let observed = capture_current_head_witness(&fresh)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let row_count = fresh
        .count_rows(None)
        .await
        .map_err(|error| rebind_error(sidecar, error))? as u64;
    if observed != fresh_head || row_count != protocol.prior_row_count {
        return Err(rebind_error(
            sidecar,
            "fresh rebind index changed the exact HEAD witness or logical row count",
        ));
    }
    let version_metadata = super::super::TableVersionMetadata::from_dataset(
        root_uri,
        &protocol.intended_binding.table_location,
        &fresh,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    Ok(RecoveryStreamRebindPhysicalV18 {
        dropped_head,
        fresh_index_head: fresh_head,
        shard_manifest_version: evidence.shard_manifest.version,
        writer_epoch: evidence.sentinel_writer_epoch,
        replay_cursor: evidence.shard_manifest.replay_after_wal_entry_position,
        current_generation: evidence.shard_manifest.current_generation,
        base_merged_generation: evidence.base_merged_generation,
        sentinel_position: evidence.sentinel_position,
        confirmed_update: RecoveryConfirmedTableUpdate {
            table_version: observed.table_version,
            table_branch: None,
            row_count,
            version_metadata,
        },
    })
}

async fn converge_physical(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    invoke_from_no_effect: bool,
) -> Result<Option<RecoveryStreamRebindPhysicalV18>> {
    let protocol = protocol(sidecar);
    let pin = &sidecar.tables[0];
    let mut anchor = crate::instrumentation::open_dataset(
        &pin.table_path,
        crate::instrumentation::VersionResolution::At(pin.expected_version),
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    for _ in 0..6 {
        let state = classify_rebind(
            &anchor,
            &protocol.prior_lifecycle.current_head_witness,
            &protocol.rebind_plan,
        )
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
        match state {
            MemWalRebindState::ExactNoEffect if !invoke_from_no_effect => return Ok(None),
            MemWalRebindState::ExactNoEffect => {
                drop_current_index_from_exact_rebind_no_effect(
                    &mut anchor,
                    &protocol.prior_lifecycle.current_head_witness,
                    &protocol.rebind_plan,
                )
                .await
                .map_err(|error| rebind_error(sidecar, error))?;
            }
            MemWalRebindState::ExactIndexDropped { .. } => {
                initialize_fresh_index_from_exact_rebind_drop(
                    &mut anchor,
                    &protocol.prior_lifecycle.current_head_witness,
                    &protocol.rebind_plan,
                )
                .await
                .map_err(|error| rebind_error(sidecar, error))?;
            }
            MemWalRebindState::ExactFreshIndexOnly { .. }
            | MemWalRebindState::ExactFreshIndexAndProvisionedShard { .. }
            | MemWalRebindState::ExactFreshIndexAndClaimManifestOnly { .. } => {
                provision_rebind_shard_from_exact_index_only(
                    &anchor,
                    &protocol.prior_lifecycle.current_head_witness,
                    &protocol.rebind_plan,
                )
                .await
                .map_err(|error| rebind_error(sidecar, error))?;
            }
            MemWalRebindState::ExactFreshIndexAndFenceOnlyClaim {
                dropped_head,
                receipt,
                evidence,
            } => {
                return physical_confirmation(
                    root_uri,
                    sidecar,
                    dropped_head,
                    receipt.head,
                    evidence,
                )
                .await
                .map(Some);
            }
        }
    }
    Err(rebind_error(
        sidecar,
        "bounded physical convergence exhausted without a terminal fresh fence",
    ))
}

async fn build_and_arm_terminal(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    physical: RecoveryStreamRebindPhysicalV18,
) -> Result<RecoverySidecar> {
    let mut with_physical = sidecar.clone();
    protocol_mut(&mut with_physical).physical = Some(physical.clone());
    let (attempt, effect) = rebuild_claim_plan(&with_physical)?;
    let protocol = protocol(&with_physical);
    let claim_authority = build_rebind_claim_authority(
        &protocol.prior_lifecycle,
        &protocol.planned_binding_receipt,
        physical.fresh_index_head.clone(),
        1,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let pin = &with_physical.tables[0];
    let fresh = crate::instrumentation::open_dataset(
        &pin.table_path,
        crate::instrumentation::VersionResolution::At(physical.fresh_index_head.table_version),
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let object_store = fresh
        .object_store(None)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let shard_id = protocol.rebind_plan.next_enrollment.shard_id;
    let tailer = WalTailer::new(object_store, fresh.branch_location().path.clone(), shard_id);
    let schema = Arc::new(ArrowSchema::from(fresh.schema()));
    let discovery = claim_wal_key_discovery_plan(&attempt, &effect, Arc::clone(&schema))
        .map_err(|error| rebind_error(sidecar, error))?;
    let keys = collect_claim_wal_segment_keys(&tailer, &discovery)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    if !keys.is_empty() {
        return Err(rebind_error(
            sidecar,
            "fresh rebind fence contains logical keys",
        ));
    }
    let authentication = claim_wal_authentication_plan(
        &attempt,
        &effect,
        protocol.authority.schema_ir_hash.clone(),
        Arc::clone(&schema),
        BTreeMap::new(),
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let segment = authenticate_claim_wal_segment(&tailer, &authentication)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    if segment.row_count != 0 {
        return Err(rebind_error(
            sidecar,
            "fresh rebind fence authenticates data rows",
        ));
    }
    let full_lww = current_generation_lww_projection_digest(
        &attempt.operation,
        &protocol.authority.schema_ir_hash,
        schema,
        &[],
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let attempt_chain = effect
        .next_attempt_chain_ref()
        .map_err(|error| rebind_error(sidecar, error))?;
    let built = build_terminal_claim(
        &protocol.prior_lifecycle.claim_receipt_chain,
        &attempt,
        &effect,
        &attempt_chain,
        &segment,
        &full_lww,
        physical.replay_cursor,
        protocol.recorded_at,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let next_revision = protocol
        .prior_lifecycle
        .lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| rebind_error(sidecar, "lifecycle revision overflow"))?;
    let result_payload = stream_rebind_result_payload(
        next_revision,
        &claim_authority.binding_scope_id,
        &claim_authority.binding,
        &claim_authority.current_head_witness,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let management = ManagementReceipt::new(
        protocol.request.graph_identity_digest.clone(),
        protocol.prior_lifecycle.identity,
        protocol
            .prior_lifecycle
            .enrollment_receipt
            .stream_incarnation_id
            .clone(),
        claim_authority.binding_scope_id.clone(),
        &protocol.prior_lifecycle.management_receipt_chain,
        protocol.request.rebind_id.clone(),
        STREAM_REBIND_OPERATION_KIND,
        protocol.prior_lifecycle.lifecycle_revision,
        next_revision,
        protocol.request.actor_id.clone(),
        protocol
            .request
            .to_value()
            .map_err(|error| rebind_error(sidecar, error))?,
        result_payload,
        protocol.recorded_at,
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let next_lifecycle = build_rebind_adoption_row(
        &protocol.prior_lifecycle,
        &claim_authority,
        &protocol.planned_binding_receipt,
        &built,
        &management,
        protocol.profile.profile_revision,
        EmptyCutEvidence {
            shard_manifest_version: physical.shard_manifest_version,
            writer_epoch: physical.writer_epoch,
            replay_cursor: physical.replay_cursor,
            current_generation: physical.current_generation,
            base_merged_generation: physical.base_merged_generation,
        },
    )
    .map_err(|error| rebind_error(sidecar, error))?;
    let terminal = RecoveryStreamRebindTerminalV18 {
        claim_authority,
        claim_attempt: effect,
        claim_receipt: built.receipt,
        management_receipt: management,
        next_lifecycle,
    };
    let selected = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &protocol.prior_token_authority,
        &crate::lance_access::control_session(),
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let records = rebind_ledger_records(&terminal, &protocol.planned_binding_receipt);
    let staged = super::super::token_store::stage_lifecycle_ledger_records(
        selected,
        &protocol.prior_token_authority,
        &records,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let planned_transaction = staged.transaction_identity();

    let mut armed = with_physical;
    let armed_protocol = protocol_mut(&mut armed);
    armed_protocol.phase = RecoveryStreamRebindPhaseV18::LedgerArmed;
    armed_protocol.terminal = Some(terminal);
    armed_protocol.ledger = Some(RecoveryStreamRebindLedgerV18 {
        planned_transaction,
        confirmed_transaction: None,
        confirmed_head: None,
        next_authority: None,
    });
    rewrite_rebind_sidecar(root_uri, storage, &armed)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    Ok(armed)
}

async fn observe_ledger(root_uri: &str, sidecar: &RecoverySidecar) -> Result<ObservedRebindLedger> {
    let protocol = protocol(sidecar);
    let ledger = protocol
        .ledger
        .as_ref()
        .ok_or_else(|| rebind_error(sidecar, "ledger phase has no planned transaction"))?;
    let session = crate::lance_access::control_session();
    let dataset = crate::instrumentation::open_dataset(
        &super::super::layout::stream_token_uri(root_uri),
        crate::instrumentation::VersionResolution::Latest,
        Some(&session),
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let authority = super::super::token_store::stream_token_authority_entry_for_dataset(&dataset)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    if authority == protocol.prior_token_authority {
        return Ok(ObservedRebindLedger {
            state: RebindLedgerState::ExactNoEffect,
            transaction: None,
            authority,
        });
    }
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| rebind_error(sidecar, error))?
        .ok_or_else(|| rebind_error(sidecar, "stream-token HEAD has no transaction"))?;
    let transaction = StagedTransactionIdentity::from(&transaction);
    let expected_version = ledger
        .planned_transaction
        .read_version
        .checked_add(1)
        .ok_or_else(|| rebind_error(sidecar, "stream-token version overflow"))?;
    if transaction != ledger.planned_transaction
        || authority.current_head_witness.table_version != expected_version
        || authority.current_head_witness.transaction_uuid != ledger.planned_transaction.uuid
        || authority.location != protocol.prior_token_authority.location
        || authority.schema_version != protocol.prior_token_authority.schema_version
        || authority.schema_hash != protocol.prior_token_authority.schema_hash
    {
        return Err(rebind_error(
            sidecar,
            "raw stream-token HEAD is neither the selected prior witness nor the exact planned N+1 rebind transaction",
        ));
    }
    let terminal = protocol
        .terminal
        .as_ref()
        .expect("validated ledger-owned StreamRebind terminal");
    let binding = super::super::token_store::lookup_binding_receipt(
        &dataset,
        &authority,
        &protocol.planned_binding_receipt.graph_identity_digest,
        protocol.planned_binding_receipt.identity,
        &protocol.planned_binding_receipt.binding_scope_id,
        &protocol.planned_binding_receipt.operation_id,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let attempt = super::super::token_store::lookup_claim_attempt_effect(
        &dataset,
        &authority,
        &terminal.claim_attempt.graph_identity_digest,
        terminal.claim_attempt.identity,
        &terminal.claim_attempt.binding_scope_id,
        &terminal.claim_attempt.claim_id,
        terminal.claim_attempt.ordinal,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let claim = super::super::token_store::lookup_claim_receipt(
        &dataset,
        &authority,
        &terminal.claim_receipt.graph_identity_digest,
        terminal.claim_receipt.identity,
        &terminal.claim_receipt.binding_scope_id,
        &terminal.claim_receipt.claim_id,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let management = super::super::token_store::lookup_management_receipt(
        &dataset,
        &authority,
        &terminal.management_receipt.graph_identity_digest,
        terminal.management_receipt.identity,
        &terminal.management_receipt.stream_incarnation_id,
        &terminal.management_receipt.operation_kind,
        &terminal.management_receipt.operation_id,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    if binding.as_ref() != Some(&protocol.planned_binding_receipt)
        || attempt.as_ref() != Some(&terminal.claim_attempt)
        || claim.as_ref() != Some(&terminal.claim_receipt)
        || management.as_ref() != Some(&terminal.management_receipt)
    {
        return Err(rebind_error(
            sidecar,
            "exact rebind transaction omits or changes one immutable terminal record",
        ));
    }
    Ok(ObservedRebindLedger {
        state: RebindLedgerState::ExactEffect,
        transaction: Some(transaction),
        authority,
    })
}

async fn restage_missing_ledger(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<ObservedRebindLedger> {
    let protocol = protocol(sidecar);
    if protocol.phase != RecoveryStreamRebindPhaseV18::LedgerArmed {
        return Err(rebind_error(
            sidecar,
            "only LedgerArmed may recreate a missing rebind ledger transaction",
        ));
    }
    let terminal = protocol
        .terminal
        .as_ref()
        .expect("validated ledger-owned StreamRebind terminal");
    let ledger = protocol
        .ledger
        .as_ref()
        .expect("validated ledger-owned StreamRebind ledger");
    let session = crate::lance_access::control_session();
    let dataset = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &protocol.prior_token_authority,
        &session,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    let records = rebind_ledger_records(terminal, &protocol.planned_binding_receipt);
    let mut staged = super::super::token_store::stage_lifecycle_ledger_records(
        dataset.clone(),
        &protocol.prior_token_authority,
        &records,
    )
    .await
    .map_err(|error| rebind_error(sidecar, error))?;
    staged
        .bind_transaction_identity(&ledger.planned_transaction)
        .map_err(|error| rebind_error(sidecar, error))?;
    let table_store = crate::table_store::TableStore::new(root_uri, session);
    let (achieved, committed) = table_store
        .commit_staged_exact(Arc::new(dataset), staged)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let expected_version = ledger
        .planned_transaction
        .read_version
        .checked_add(1)
        .ok_or_else(|| rebind_error(sidecar, "stream-token version overflow"))?;
    if committed != ledger.planned_transaction || achieved.version().version != expected_version {
        return Err(rebind_error(
            sidecar,
            "recovery committed a non-exact StreamRebind ledger transaction",
        ));
    }
    observe_ledger(root_uri, sidecar).await
}

async fn confirm_ledger(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    observed: &ObservedRebindLedger,
) -> Result<RecoverySidecar> {
    let protocol = protocol(sidecar);
    let ledger = protocol
        .ledger
        .as_ref()
        .expect("validated ledger-owned StreamRebind");
    let transaction = observed
        .transaction
        .as_ref()
        .ok_or_else(|| rebind_error(sidecar, "ledger confirmation has no transaction"))?;
    if protocol.phase != RecoveryStreamRebindPhaseV18::LedgerArmed
        || observed.state != RebindLedgerState::ExactEffect
        || transaction != &ledger.planned_transaction
        || observed.authority.current_head_witness.table_version
            != transaction
                .read_version
                .checked_add(1)
                .ok_or_else(|| rebind_error(sidecar, "stream-token version overflow"))?
        || observed.authority.current_head_witness.transaction_uuid != transaction.uuid
    {
        return Err(rebind_error(
            sidecar,
            "ledger confirmation differs from the exact planned N+1 effect",
        ));
    }
    let mut confirmed = sidecar.clone();
    let confirmed_protocol = protocol_mut(&mut confirmed);
    confirmed_protocol.phase = RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed;
    let confirmed_ledger = confirmed_protocol
        .ledger
        .as_mut()
        .expect("validated ledger-owned StreamRebind");
    confirmed_ledger.confirmed_transaction = Some(transaction.clone());
    confirmed_ledger.confirmed_head = Some(observed.authority.current_head_witness.clone());
    confirmed_ledger.next_authority = Some(observed.authority.clone());
    rewrite_rebind_sidecar(root_uri, storage, &confirmed)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    Ok(confirmed)
}

fn publish_terminal<'a>(
    root_uri: &'a str,
    sidecar: &'a RecoverySidecar,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
    Box::pin(publish_terminal_inner(root_uri, sidecar))
}

async fn publish_terminal_inner(root_uri: &str, sidecar: &RecoverySidecar) -> Result<u64> {
    validate_rebind_topology(root_uri, sidecar).await?;
    let protocol = protocol(sidecar);
    let physical = protocol
        .physical
        .as_ref()
        .expect("validated confirmed StreamRebind physical effect");
    let terminal = protocol
        .terminal
        .as_ref()
        .expect("validated confirmed StreamRebind terminal");
    let next_authority = protocol
        .ledger
        .as_ref()
        .and_then(|ledger| ledger.next_authority.as_ref())
        .expect("validated confirmed StreamRebind authority");
    let pin = &sidecar.tables[0];
    let changes = vec![
        ManifestChange::Update(SubTableUpdate {
            identity: pin.identity,
            table_key: pin.table_key.clone(),
            table_version: physical.confirmed_update.table_version,
            table_branch: physical.confirmed_update.table_branch.clone(),
            row_count: physical.confirmed_update.row_count,
            version_metadata: physical.confirmed_update.version_metadata.clone(),
        }),
        ManifestChange::SetStreamTokenAuthority {
            expected: protocol.prior_token_authority.clone(),
            next: next_authority.clone(),
        },
        ManifestChange::SetStreamLifecycle {
            expected: Some(protocol.prior_lifecycle.clone()),
            next: terminal.next_lifecycle.clone(),
        },
        ManifestChange::SetStreamProfile {
            expected: protocol.profile.clone(),
            next: protocol.profile.clone(),
        },
    ];
    let expected = HashMap::from([(
        pin.identity,
        TableVersionExpectation {
            table_key: pin.table_key.clone(),
            table_version: pin.expected_version,
        },
    )]);
    let publisher = GraphNamespacePublisher::new_with_session(
        root_uri,
        None,
        crate::lance_access::control_session(),
    );
    let precondition = PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
        None,
        protocol.authority.branch_identifier.clone(),
        protocol.authority.graph_head.clone(),
    ));
    let intent = LineageIntent::from(&protocol.lineage);
    let outcome = publisher
        .publish_with_precondition(&changes, &expected, Some(&intent), &precondition)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let expected_version = protocol
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| rebind_error(sidecar, "manifest version overflow"))?;
    if outcome.dataset.version().version != expected_version {
        return Err(rebind_error(
            sidecar,
            format!(
                "terminal manifest reached version {}, expected exact version {expected_version}",
                outcome.dataset.version().version
            ),
        ));
    }
    Ok(expected_version)
}

async fn cleanup_visible(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    cleanup: StreamRebindCleanup,
) -> Result<()> {
    let handle = RecoverySidecarHandle {
        operation_id: sidecar.operation_id.clone(),
        sidecar_uri: sidecar_uri(root_uri, &sidecar.operation_id),
    };
    match delete_sidecar(&handle, storage).await {
        Ok(()) => Ok(()),
        Err(error) => match cleanup {
            StreamRebindCleanup::Required => Err(error),
            StreamRebindCleanup::BestEffortAfterVisible => {
                tracing::warn!(
                    error = %error,
                    operation_id = sidecar.operation_id.as_str(),
                    "StreamRebind sidecar cleanup failed; the next recovery barrier will resolve it"
                );
                Ok(())
            }
        },
    }
}

async fn finalize_visible(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    cleanup: StreamRebindCleanup,
) -> Result<RecoveryStreamRebindOutcomeV18> {
    let protocol = protocol(sidecar);
    let physical = protocol
        .physical
        .as_ref()
        .expect("validated confirmed StreamRebind physical effect");
    let terminal = protocol
        .terminal
        .as_ref()
        .expect("validated confirmed StreamRebind terminal");
    let token_authority = protocol
        .ledger
        .as_ref()
        .and_then(|ledger| ledger.next_authority.clone())
        .expect("validated confirmed StreamRebind authority");
    let pin = &sidecar.tables[0];
    let expected_outcomes = vec![TableOutcome {
        table_key: pin.table_key.clone(),
        from_version: pin.expected_version,
        to_version: physical.confirmed_update.table_version,
    }];
    let expected_writer_kind = format!("{:?}", sidecar.writer_kind);
    let mut audit = RecoveryAudit::open(root_uri)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let records = audit
        .list()
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    let operation_records = records
        .iter()
        .filter(|record| record.operation_id == sidecar.operation_id)
        .collect::<Vec<_>>();
    if operation_records.iter().any(|record| {
        record.graph_commit_id != protocol.lineage.graph_commit_id
            || record.recovery_kind != RecoveryKind::RolledForward
            || record.recovery_for_actor != sidecar.actor_id
            || record.sidecar_writer_kind != expected_writer_kind
            || record.per_table_outcomes != expected_outcomes
    }) {
        return Err(rebind_error(
            sidecar,
            "recovery audit row differs from the exact rebind outcome",
        ));
    }
    if operation_records.is_empty() {
        crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_RECORD_AUDIT)
            .map_err(|error| rebind_error(sidecar, error))?;
        audit
            .append(RecoveryAuditRecord {
                graph_commit_id: protocol.lineage.graph_commit_id.clone(),
                recovery_kind: RecoveryKind::RolledForward,
                recovery_for_actor: sidecar.actor_id.clone(),
                operation_id: sidecar.operation_id.clone(),
                sidecar_writer_kind: expected_writer_kind,
                per_table_outcomes: expected_outcomes,
                created_at: crate::db::now_micros()
                    .map_err(|error| rebind_error(sidecar, error))?,
            })
            .await
            .map_err(|error| rebind_error(sidecar, error))?;
    }
    let outcome = RecoveryStreamRebindOutcomeV18::TerminalVisible {
        lifecycle: terminal.next_lifecycle.clone(),
        token_authority,
        management_receipt: terminal.management_receipt.clone(),
    };
    cleanup_visible(root_uri, storage, sidecar, cleanup).await?;
    Ok(outcome)
}

pub(super) fn process_stream_rebind_sidecar_v18<'a>(
    root_uri: &'a str,
    storage: &'a Arc<dyn StorageAdapter>,
    snapshot: &'a Snapshot,
    sidecar: &'a RecoverySidecar,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>> {
    Box::pin(async move {
        let outcome = process_stream_rebind_sidecar_v18_typed(
            root_uri,
            storage,
            snapshot,
            sidecar,
            StreamRebindCleanup::Required,
            false,
        )
        .await?;
        Ok(matches!(
            outcome,
            RecoveryStreamRebindOutcomeV18::EffectFree
                | RecoveryStreamRebindOutcomeV18::TerminalVisible { .. }
        ))
    })
}

fn process_stream_rebind_sidecar_v18_typed<'a>(
    root_uri: &'a str,
    storage: &'a Arc<dyn StorageAdapter>,
    snapshot: &'a Snapshot,
    sidecar: &'a RecoverySidecar,
    cleanup: StreamRebindCleanup,
    invoke_from_no_effect: bool,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<RecoveryStreamRebindOutcomeV18>> + Send + 'a>,
> {
    Box::pin(process_stream_rebind_sidecar_v18_typed_inner(
        root_uri,
        storage,
        snapshot,
        sidecar,
        cleanup,
        invoke_from_no_effect,
    ))
}

async fn process_stream_rebind_sidecar_v18_typed_inner(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    cleanup: StreamRebindCleanup,
    invoke_from_no_effect: bool,
) -> Result<RecoveryStreamRebindOutcomeV18> {
    match prepare_stream_rebind_sidecar_v18(
        root_uri,
        storage,
        snapshot,
        sidecar,
        invoke_from_no_effect,
    )
    .await?
    {
        PreparedStreamRebindV18::Complete(outcome) => Ok(outcome),
        PreparedStreamRebindV18::FinalizeVisible(owned) => {
            finalize_visible(root_uri, storage.as_ref(), &owned, cleanup).await
        }
        PreparedStreamRebindV18::PublishTerminal(owned) => {
            crate::failpoints::maybe_fail(
                crate::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH,
            )
            .map_err(|error| rebind_error(&owned, error))?;
            publish_terminal(root_uri, &owned).await?;
            if !terminal_manifest_visible(root_uri, &owned).await? {
                return Err(rebind_error(
                    &owned,
                    "terminal manifest CAS did not select the exact fresh table, token, and SEALED lifecycle",
                ));
            }
            finalize_visible(root_uri, storage.as_ref(), &owned, cleanup).await
        }
    }
}

enum PreparedStreamRebindV18 {
    Complete(RecoveryStreamRebindOutcomeV18),
    FinalizeVisible(RecoverySidecar),
    PublishTerminal(RecoverySidecar),
}

fn prepare_stream_rebind_sidecar_v18<'a>(
    root_uri: &'a str,
    storage: &'a Arc<dyn StorageAdapter>,
    snapshot: &'a Snapshot,
    sidecar: &'a RecoverySidecar,
    invoke_from_no_effect: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PreparedStreamRebindV18>> + Send + 'a>>
{
    Box::pin(prepare_stream_rebind_sidecar_v18_inner(
        root_uri,
        storage,
        snapshot,
        sidecar,
        invoke_from_no_effect,
    ))
}

async fn prepare_stream_rebind_sidecar_v18_inner(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    invoke_from_no_effect: bool,
) -> Result<PreparedStreamRebindV18> {
    validate_sidecar_shape(&sidecar_uri(root_uri, &sidecar.operation_id), sidecar)?;

    if terminal_manifest_visible(root_uri, sidecar).await? {
        let observed = observe_ledger(root_uri, sidecar).await?;
        let ledger = protocol(sidecar)
            .ledger
            .as_ref()
            .expect("validated confirmed StreamRebind ledger");
        if observed.state != RebindLedgerState::ExactEffect
            || observed.transaction.as_ref() != ledger.confirmed_transaction.as_ref()
            || Some(&observed.authority) != ledger.next_authority.as_ref()
        {
            return Err(rebind_error(
                sidecar,
                "visible SEALED rebind is not backed by the exact confirmed ledger",
            ));
        }
        return Ok(PreparedStreamRebindV18::FinalizeVisible(sidecar.clone()));
    }

    // Topology constrains convergence toward a not-yet-visible physical
    // occurrence. Once the exact terminal manifest is selected, later branch
    // creation cannot revoke that durable success or strand sidecar cleanup.
    validate_rebind_topology(root_uri, sidecar).await?;

    if !manifest_is_prior(snapshot, sidecar) {
        return Err(rebind_error(
            sidecar,
            "manifest profile, table, lifecycle, token pointer, or version differs from every exact rebind outcome",
        ));
    }
    let live_authority = read_live_recovery_authority(root_uri, storage, None)
        .await
        .map_err(|error| rebind_error(sidecar, error))?;
    if live_authority != protocol(sidecar).authority {
        return Err(rebind_error(
            sidecar,
            "graph/schema authority changed after rebind was armed",
        ));
    }

    let observed_physical = converge_physical(root_uri, sidecar, invoke_from_no_effect).await?;
    let Some(observed_physical) = observed_physical else {
        if protocol(sidecar).phase != RecoveryStreamRebindPhaseV18::PhysicalArmed {
            return Err(rebind_error(
                sidecar,
                "ledger-owned rebind has no physical effect",
            ));
        }
        delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id).await?;
        return Ok(PreparedStreamRebindV18::Complete(
            RecoveryStreamRebindOutcomeV18::EffectFree,
        ));
    };

    let mut owned = match protocol(sidecar).phase {
        RecoveryStreamRebindPhaseV18::PhysicalArmed => {
            build_and_arm_terminal(root_uri, storage.as_ref(), sidecar, observed_physical).await?
        }
        RecoveryStreamRebindPhaseV18::LedgerArmed
        | RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed => {
            if protocol(sidecar).physical.as_ref() != Some(&observed_physical) {
                return Err(rebind_error(
                    sidecar,
                    "durable physical confirmation differs from the exact live rebind state",
                ));
            }
            sidecar.clone()
        }
    };

    let mut observed = observe_ledger(root_uri, &owned).await?;
    if observed.state == RebindLedgerState::ExactNoEffect {
        if protocol(&owned).phase != RecoveryStreamRebindPhaseV18::LedgerArmed {
            return Err(rebind_error(
                &owned,
                "confirmed rebind ledger has no raw effect",
            ));
        }
        observed = restage_missing_ledger(root_uri, &owned).await?;
    }
    if protocol(&owned).phase == RecoveryStreamRebindPhaseV18::LedgerArmed {
        owned = confirm_ledger(root_uri, storage.as_ref(), &owned, &observed).await?;
    }
    let ledger = protocol(&owned)
        .ledger
        .as_ref()
        .expect("validated confirmed StreamRebind ledger");
    if protocol(&owned).phase != RecoveryStreamRebindPhaseV18::LedgerEffectsConfirmed
        || observed.state != RebindLedgerState::ExactEffect
        || observed.transaction.as_ref() != ledger.confirmed_transaction.as_ref()
        || Some(&observed.authority) != ledger.next_authority.as_ref()
    {
        return Err(rebind_error(
            &owned,
            "rebind ledger did not converge to its exact durable confirmation",
        ));
    }
    Ok(PreparedStreamRebindV18::PublishTerminal(owned))
}

#[derive(Clone, Copy)]
enum StreamRebindCleanup {
    Required,
    BestEffortAfterVisible,
}

/// Arm one exact physical rebind before the old singleton index is dropped.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_stream_rebind_sidecar_v18(
    actor_id: String,
    table: SidecarTablePin,
    authority: RecoveryAuthorityToken,
    lineage: RecoveryLineageIntent,
    prior_manifest_version: u64,
    profile: super::super::StreamProfileEntry,
    prior_lifecycle: super::super::StreamLifecycleEntry,
    prior_binding_receipt: BindingReceipt,
    prior_claim_receipt: ClaimReceipt,
    prior_token_authority: super::super::StreamTokenAuthorityEntry,
    request: StreamRebindRequestPayload,
    rebind_plan: MemWalRebindPlan,
    intended_binding: super::super::StreamPhysicalBinding,
    planned_binding_receipt: BindingReceipt,
    prior_row_count: u64,
    claim_attempt_id: String,
    recorded_at: i64,
) -> Result<RecoverySidecar> {
    let admission_scope = RecoveryStreamAdmissionScope {
        identity: prior_lifecycle.identity,
        table_branch: prior_lifecycle.binding.table_branch.clone(),
        binding_scope_id: prior_lifecycle.binding_scope_id.clone(),
    };
    let mut sidecar = new_unvalidated_sidecar(
        STREAM_REBIND_SIDECAR_SCHEMA_VERSION,
        SidecarKind::StreamRebind,
        None,
        Some(actor_id),
        vec![table],
    );
    sidecar.protocol_v18 = Some(Box::new(RecoveryProtocolV18::StreamRebind(
        RecoveryStreamRebindV18 {
            authority,
            lineage,
            admission_scope,
            prior_manifest_version,
            profile,
            prior_lifecycle,
            prior_binding_receipt,
            prior_claim_receipt,
            prior_token_authority,
            request,
            rebind_plan,
            intended_binding,
            planned_binding_receipt,
            prior_row_count,
            claim_attempt_id,
            recorded_at,
            phase: RecoveryStreamRebindPhaseV18::PhysicalArmed,
            physical: None,
            terminal: None,
            ledger: None,
        },
    )));
    validate_sidecar_shape("<new-stream-rebind-sidecar-v18>", &sidecar)?;
    Ok(sidecar)
}

/// Drive a newly armed rebind through its exact forward-only owner. Generic
/// open recovery differs only at the all-no-effect state, where it cleans the
/// abandoned intent instead of beginning a new physical replacement.
pub(crate) async fn complete_stream_rebind_sidecar_v18(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamRebindOutcomeV18> {
    process_stream_rebind_sidecar_v18_typed(
        root_uri,
        &storage,
        snapshot,
        sidecar,
        StreamRebindCleanup::BestEffortAfterVisible,
        true,
    )
    .await
}
