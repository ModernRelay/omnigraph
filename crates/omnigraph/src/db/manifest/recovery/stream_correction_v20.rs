//! Recovery-v20 exact DataBlock correction.
//!
//! The frozen recovery-v14 `StreamCorrection` scaffold is deliberately not
//! reused. V20 owns one exact blocked generation cut, one pre-minted base
//! transaction, and one pre-minted token transaction containing both the
//! complete current-token successor set and the immutable correction plus
//! management receipts. Only their exact joint outcome may publish lineage,
//! advance both manifest pointers, and clear that exact block while leaving
//! the drain in `DRAINING`.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamCorrectionBaseEffectV20 {
    pub(crate) planned_transaction: StagedTransactionIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_transaction: Option<StagedTransactionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_merged_generation: Option<MergedGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_head: Option<super::super::CurrentHeadWitness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_update: Option<RecoveryConfirmedTableUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamCorrectionTokenEffectV20 {
    pub(crate) prior_authority: super::super::StreamTokenAuthorityEntry,
    /// Exact winner authority reconstructed from the blocked generation.
    /// These rows are not necessarily present in `prior_authority`: the fold
    /// was blocked before its token participant could publish.
    pub(crate) blocked_winners: Vec<super::super::stream_token::StreamTokenAuthorityRow>,
    pub(crate) planned_transaction: StagedTransactionIdentity,
    pub(crate) planned_rows: Vec<super::super::stream_token::StreamTokenAuthorityRow>,
    pub(crate) planned_correction_receipt: super::super::stream::StreamCorrectionReceipt,
    pub(crate) planned_management_receipt: super::super::stream::ManagementReceipt,
    /// `None` is the canonical all-WITHDRAW result. A non-empty PRESENT result
    /// carries the ordinary fold attribution commitment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) attribution_summary: Option<RecoveryStreamFoldAttributionSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_transaction: Option<StagedTransactionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_head: Option<super::super::CurrentHeadWitness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) next_authority: Option<super::super::StreamTokenAuthorityEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamCorrectionV20 {
    pub(crate) authority: RecoveryAuthorityToken,
    pub(crate) lineage: RecoveryLineageIntent,
    pub(crate) admission_scope: RecoveryStreamAdmissionScope,
    pub(crate) prior_manifest_version: u64,
    pub(crate) profile: super::super::StreamProfileEntry,
    pub(crate) binding: super::super::StreamPhysicalBinding,
    pub(crate) prior_merged_generation: Option<MergedGeneration>,
    pub(crate) generation_cut: RecoveryStreamFoldCut,
    pub(crate) merged_generation: MergedGeneration,
    pub(crate) effect_phase: RecoveryEffectPhase,
    pub(crate) prior_lifecycle: super::super::StreamLifecycleEntry,
    pub(crate) current_claim_receipt: super::super::stream::ClaimReceipt,
    pub(crate) planned_next_lifecycle: super::super::StreamLifecycleEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) next_lifecycle: Option<super::super::StreamLifecycleEntry>,
    pub(crate) base: RecoveryStreamCorrectionBaseEffectV20,
    pub(crate) token: RecoveryStreamCorrectionTokenEffectV20,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", deny_unknown_fields)]
pub(crate) enum RecoveryProtocolV20 {
    StreamCorrection(RecoveryStreamCorrectionV20),
}

impl RecoveryProtocolV20 {
    pub(crate) fn admission_scope(&self) -> &RecoveryStreamAdmissionScope {
        let Self::StreamCorrection(protocol) = self;
        &protocol.admission_scope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryStreamCorrectionOutcomeV20 {
    pub(crate) graph_commit_id: String,
    pub(crate) manifest_version: u64,
    pub(crate) lifecycle_revision: u64,
    pub(crate) base_head: super::super::CurrentHeadWitness,
    pub(crate) token_head: super::super::CurrentHeadWitness,
}

fn protocol(sidecar: &RecoverySidecar) -> &RecoveryStreamCorrectionV20 {
    let RecoveryProtocolV20::StreamCorrection(protocol) = sidecar
        .protocol_v20
        .as_deref()
        .expect("validated schema-v20 StreamCorrection");
    protocol
}

fn protocol_mut(sidecar: &mut RecoverySidecar) -> &mut RecoveryStreamCorrectionV20 {
    let RecoveryProtocolV20::StreamCorrection(protocol) = sidecar
        .protocol_v20
        .as_deref_mut()
        .expect("validated schema-v20 StreamCorrection");
    protocol
}

fn correction_error(sidecar: &RecoverySidecar, reason: impl std::fmt::Display) -> OmniError {
    OmniError::recovery_required(
        sidecar.operation_id.clone(),
        format!("StreamCorrection recovery cannot prove an exact outcome: {reason}"),
    )
}

fn exact_correction_receipts(
    token: &RecoveryStreamCorrectionTokenEffectV20,
) -> Result<(
    &super::super::stream::StreamCorrectionReceipt,
    &super::super::stream::ManagementReceipt,
)> {
    token.planned_correction_receipt.validate()?;
    token
        .planned_management_receipt
        .validate(token.planned_management_receipt.to_revision)?;
    Ok((
        &token.planned_correction_receipt,
        &token.planned_management_receipt,
    ))
}

fn receipt_lineage_binding_matches(
    correction_actor: &str,
    management_actor: &str,
    sidecar_actor: Option<&str>,
    lineage_actor: Option<&str>,
    correction_graph_commit_id: &str,
    lineage_graph_commit_id: &str,
) -> bool {
    sidecar_actor == Some(super::super::stream_token::STREAM_FOLD_ACTOR)
        && lineage_actor == Some(super::super::stream_token::STREAM_FOLD_ACTOR)
        && management_actor == correction_actor
        && correction_graph_commit_id == lineage_graph_commit_id
}

pub(super) fn validate_stream_protocol_v20_shape(
    sidecar_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let malformed = |reason: String| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' has an invalid schema-v{} shape: {}",
            sidecar_uri, sidecar.schema_version, reason
        ))
    };
    if sidecar.writer_kind != SidecarKind::StreamCorrection
        || sidecar.branch.is_some()
        || sidecar.tables.len() != 1
        || sidecar.actor_id.is_none()
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
        || sidecar.protocol_v18.is_some()
        || sidecar.protocol_v19.is_some()
        || sidecar.ensure_indices_rollback_v6.is_some()
        || sidecar.merge_source_commit_id.is_some()
        || !sidecar.additional_registrations.is_empty()
        || !sidecar.tombstones.is_empty()
        || sidecar.schema_apply_manifest_published
        || sidecar.schema_apply_target_schema_ir_hash.is_some()
    {
        return Err(malformed(
            "schema-v20 StreamCorrection must target one canonical-main table and carry only protocol_v20"
                .to_string(),
        ));
    }
    let value = sidecar
        .protocol_v20
        .as_deref()
        .ok_or_else(|| malformed("missing required protocol_v20 payload".to_string()))?;
    let RecoveryProtocolV20::StreamCorrection(protocol) = value;
    validate_authority_identity(&malformed, &protocol.authority)?;
    if protocol.authority.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || protocol.prior_manifest_version == 0
        || protocol.lineage.branch.is_some()
        || protocol.lineage.merged_parent_commit_id.is_some()
        || protocol.lineage.graph_commit_id.is_empty()
        || protocol.lineage.actor_id != sidecar.actor_id
    {
        return Err(malformed(
            "StreamCorrection requires exact canonical-main authority and actor-bound lineage"
                .to_string(),
        ));
    }
    protocol
        .profile
        .validate()
        .map_err(|error| malformed(format!("invalid StreamCorrection profile: {error}")))?;
    protocol
        .prior_lifecycle
        .validate()
        .map_err(|error| malformed(format!("invalid prior correction lifecycle: {error}")))?;
    protocol
        .token
        .prior_authority
        .validate()
        .map_err(|error| malformed(format!("invalid prior token authority: {error}")))?;
    let pin = &sidecar.tables[0];
    let expected_post = pin
        .expected_version
        .checked_add(1)
        .ok_or_else(|| malformed("correction base version overflow".to_string()))?;
    let lifecycle = &protocol.prior_lifecycle;
    let block = lifecycle.strict_block.as_ref().ok_or_else(|| {
        malformed("StreamCorrection prior lifecycle has no strict block".to_string())
    })?;
    if lifecycle.lifecycle != super::super::StreamLifecycle::Draining
        || !matches!(
            block.evidence,
            super::super::stream::StrictBlockEvidence::DataBlock { .. }
        )
        || pin.identity != lifecycle.identity
        || pin.identity != protocol.admission_scope.identity
        || pin.expected_version == 0
        || pin.post_commit_pin != expected_post
        || pin.table_branch.is_some()
        || pin.confirmed_version.is_some() && protocol.effect_phase == RecoveryEffectPhase::Armed
        || lifecycle.binding != protocol.binding
        || lifecycle.binding_scope_id != protocol.admission_scope.binding_scope_id
        || protocol.admission_scope.table_branch.is_some()
        || lifecycle.current_head_witness.table_version != pin.expected_version
        || lifecycle.current_head_witness.branch_identifier
            != lance::dataset::refs::BranchIdentifier::main()
    {
        return Err(malformed(
            "StreamCorrection pin, blocked lifecycle, admission lane, and prior base witness disagree"
                .to_string(),
        ));
    }
    let cut = &protocol.generation_cut;
    if cut.shard_manifest_version == 0
        || cut.writer_epoch == 0
        || cut.replay_after_wal_entry_position == 0
        || cut.generation == 0
        || cut.generation_path.is_empty()
        || protocol.merged_generation.shard_id != cut.shard_id
        || protocol.merged_generation.generation != cut.generation
    {
        return Err(malformed("invalid correction generation cut".to_string()));
    }
    let super::super::stream::StrictBlockEvidence::DataBlock {
        shard_id,
        generation,
        generation_path,
        shard_manifest_version,
        writer_epoch,
        replay_cursor,
        ..
    } = &block.evidence
    else {
        unreachable!("checked DataBlock above")
    };
    if shard_id != &cut.shard_id.to_string()
        || generation != &cut.generation
        || generation_path != &cut.generation_path
        || shard_manifest_version != &cut.shard_manifest_version
        || writer_epoch != &cut.writer_epoch
        || replay_cursor != &cut.replay_after_wal_entry_position
    {
        return Err(malformed(
            "correction cut differs from the exact blocked DataBlock evidence".to_string(),
        ));
    }
    protocol.current_claim_receipt.validate().map_err(|error| {
        malformed(format!(
            "invalid selected correction claim receipt: {error}"
        ))
    })?;
    let claim = &protocol.current_claim_receipt;
    if lifecycle.current_claim_receipt_id.as_deref() != Some(claim.record_id.as_str())
        || claim.identity != lifecycle.identity
        || claim.binding_scope_id != lifecycle.binding_scope_id
        || claim.enrollment_id != lifecycle.binding.enrollment_id
        || claim.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
        || claim.shard_id != cut.shard_id.to_string()
        || claim.achieved_writer_epoch != cut.writer_epoch
        || cut.shard_manifest_version < claim.achieved_shard_manifest_version
        || claim.lifecycle_operation_id.as_deref()
            != lifecycle
                .drain
                .as_ref()
                .map(|drain| drain.drain_id.as_str())
        || lifecycle.claim_receipt_chain
            != claim
                .next_chain_ref()
                .map_err(|error| malformed(format!("invalid claim chain: {error}")))?
    {
        return Err(malformed(
            "selected correction claim does not bind the blocked lane, drain, epoch, and cut"
                .to_string(),
        ));
    }
    match protocol.prior_merged_generation.as_ref() {
        Some(prior)
            if prior.shard_id == cut.shard_id
                && prior.generation.checked_add(1) == Some(cut.generation) => {}
        None if cut.generation == 1 => {}
        _ => {
            return Err(malformed(
                "correction cut is not the exact successor of selected merge progress".to_string(),
            ));
        }
    }
    if protocol.base.planned_transaction.read_version != pin.expected_version
        || protocol.token.planned_transaction.read_version
            != protocol
                .token
                .prior_authority
                .current_head_witness
                .table_version
    {
        return Err(malformed(
            "correction transactions must read their exact selected base/token versions"
                .to_string(),
        ));
    }
    validate_canonical_uuid_text(
        &malformed,
        "StreamCorrection base transaction UUID",
        &protocol.base.planned_transaction.uuid,
        false,
    )?;
    validate_canonical_uuid_text(
        &malformed,
        "StreamCorrection token transaction UUID",
        &protocol.token.planned_transaction.uuid,
        false,
    )?;
    if protocol.token.blocked_winners.is_empty()
        || protocol.token.blocked_winners.len() != protocol.token.planned_rows.len()
        || protocol.token.planned_rows.is_empty()
        || protocol.token.planned_rows.len()
            > crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS as usize
    {
        return Err(malformed(
            "StreamCorrection requires equal bounded non-empty blocked-winner and successor sets"
                .to_string(),
        ));
    }
    super::super::token_store::validate_stream_token_plan_bounds(&protocol.token.blocked_winners)
        .map_err(|error| malformed(format!("invalid blocked-winner token plan: {error}")))?;
    super::super::token_store::validate_stream_token_plan_bounds(&protocol.token.planned_rows)
        .map_err(|error| malformed(format!("invalid correction token plan: {error}")))?;
    let mut prior_id = None;
    let mut correction_ordinals = std::collections::BTreeSet::new();
    let mut present_rows = Vec::new();
    for row in &protocol.token.planned_rows {
        row.validate()
            .map_err(|error| malformed(format!("invalid correction token row: {error}")))?;
        if row.identity != pin.identity
            || row.origin_enrollment_id != protocol.binding.enrollment_id
            || row.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
            || prior_id.is_some_and(|prior| prior >= row.logical_id.as_str())
        {
            return Err(malformed(
                "v20 token successors must bind the exact lane and be logical-id ordered"
                    .to_string(),
            ));
        }
        match (&row.disposition, &row.origin, &row.terminal_correction) {
            (
                super::super::stream_token::StreamTokenDisposition::Present,
                super::super::stream_token::StreamRowOrigin::Correction {
                    correction_id,
                    plan_ordinal,
                },
                None,
            ) if correction_id == &protocol.token.planned_correction_receipt.correction_id
                && correction_ordinals.insert(*plan_ordinal) => {}
            (super::super::stream_token::StreamTokenDisposition::Present, origin, None)
                if !matches!(
                    origin,
                    super::super::stream_token::StreamRowOrigin::Correction { .. }
                ) =>
            {
                // Unchanged winners retain their original admission/replay
                // origin. The processor re-proves byte-for-byte equality to
                // the manifest-selected prior token row.
            }
            (super::super::stream_token::StreamTokenDisposition::Withdrawn, _, Some(terminal))
                if terminal.correction_id
                    == protocol.token.planned_correction_receipt.correction_id
                    && terminal.actor.as_str()
                        == protocol.token.planned_correction_receipt.actor_id => {}
            _ => {
                return Err(malformed(
                    "v20 token rows must be unchanged PRESENT winners, exact correction successors, or actor-bound withdrawals"
                        .to_string(),
                ));
            }
        }
        if row.disposition == super::super::stream_token::StreamTokenDisposition::Present {
            present_rows.push(row.clone());
        }
        prior_id = Some(row.logical_id.as_str());
    }
    for (blocked, planned) in protocol
        .token
        .blocked_winners
        .iter()
        .zip(&protocol.token.planned_rows)
    {
        blocked
            .validate()
            .map_err(|error| malformed(format!("invalid blocked winner: {error}")))?;
        if blocked.disposition != super::super::stream_token::StreamTokenDisposition::Present
            || blocked.identity != pin.identity
            || blocked.origin_enrollment_id != protocol.binding.enrollment_id
            || blocked.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id
            || blocked.logical_id != planned.logical_id
        {
            return Err(malformed(
                "blocked winners must be PRESENT rows for the exact lane and match successor keys"
                    .to_string(),
            ));
        }
    }
    let expected_attribution = if present_rows.is_empty() {
        None
    } else {
        Some(
            super::super::stream_token::stream_fold_attribution_commitment(&present_rows)
                .map_err(|error| malformed(format!("invalid correction attribution: {error}")))?,
        )
    };
    if protocol.token.attribution_summary != expected_attribution {
        return Err(malformed(
            "correction PRESENT rows differ from their optional attribution commitment".to_string(),
        ));
    }
    let (correction_receipt, management_receipt) = exact_correction_receipts(&protocol.token)
        .map_err(|error| malformed(format!("invalid correction receipts: {error}")))?;
    let expected_manifest_version = protocol
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| malformed("correction manifest version overflow".to_string()))?;
    let graph_digest = super::super::stream::stream_graph_identity_digest(
        &protocol.authority.schema_identity_domain,
    )
    .map_err(|error| malformed(format!("invalid correction graph identity: {error}")))?;
    if correction_receipt.graph_identity_digest != graph_digest
        || correction_receipt.identity != pin.identity
        || correction_receipt.stream_incarnation_id
            != lifecycle.enrollment_receipt.stream_incarnation_id
        || correction_receipt.binding_scope_id != lifecycle.binding_scope_id
        || correction_receipt.block_token != block.block_token
        || correction_receipt.resulting_manifest_version != expected_manifest_version
        || !receipt_lineage_binding_matches(
            &correction_receipt.actor_id,
            &management_receipt.actor_id,
            sidecar.actor_id.as_deref(),
            protocol.lineage.actor_id.as_deref(),
            &correction_receipt.graph_commit_id,
            &protocol.lineage.graph_commit_id,
        )
        || management_receipt.operation_kind
            != super::super::stream::STREAM_CORRECTION_OPERATION_KIND
        || management_receipt.operation_id != correction_receipt.correction_id
        || management_receipt.from_revision != lifecycle.lifecycle_revision
        || management_receipt.to_revision != correction_receipt.resulting_lifecycle_revision
        || management_receipt.result_payload != correction_receipt.result_payload
        || management_receipt.result_digest != correction_receipt.result_digest
    {
        return Err(malformed(
            "v20 receipts do not bind the exact graph, block, actor, lineage, and lifecycle result"
                .to_string(),
        ));
    }
    let correction_actor =
        super::super::stream_token::TrustedContributorId::new(correction_receipt.actor_id.clone())
            .map_err(|error| malformed(format!("invalid correction actor: {error}")))?;
    let mut changed_winners = 0_usize;
    for (blocked, planned) in protocol
        .token
        .blocked_winners
        .iter()
        .zip(&protocol.token.planned_rows)
    {
        if validate_planned_token_successor(
            blocked,
            planned,
            &correction_actor,
            &correction_receipt.correction_id,
        )
        .map_err(|error| {
            malformed(format!(
                "invalid successor for blocked winner '{}': {error}",
                blocked.logical_id
            ))
        })? {
            changed_winners = changed_winners
                .checked_add(1)
                .ok_or_else(|| malformed("correction changed-row count overflow".to_string()))?;
        }
    }
    if changed_winners == 0 {
        return Err(malformed(
            "StreamCorrection must change at least one blocked winner".to_string(),
        ));
    }
    let planned_head = super::super::CurrentHeadWitness {
        branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
        table_version: expected_post,
        transaction_uuid: protocol.base.planned_transaction.uuid.clone(),
        manifest_e_tag: None,
    };
    let expected_next = super::super::stream::build_data_block_correction_successor(
        lifecycle,
        &block.block_token,
        &correction_receipt.correction_plan_digest,
        management_receipt,
        super::super::stream::StreamDataCorrectionOutcome {
            graph_commit_id: protocol.lineage.graph_commit_id.clone(),
            manifest_version: expected_manifest_version,
            current_head_witness: planned_head,
            visible_rows: protocol
                .planned_next_lifecycle
                .last_fold_summary
                .as_ref()
                .map_or(0, |summary| summary.visible_rows),
            visible_bytes: protocol
                .planned_next_lifecycle
                .last_fold_summary
                .as_ref()
                .map_or(0, |summary| summary.visible_bytes),
            recorded_at: protocol.lineage.created_at,
        },
    )
    .map_err(|error| malformed(format!("invalid correction successor: {error}")))?;
    if protocol.planned_next_lifecycle != expected_next
        || correction_receipt.resulting_lifecycle_revision != expected_next.lifecycle_revision
        || correction_receipt.resulting_lifecycle_digest
            != super::super::stream::stream_lifecycle_authority_digest(&expected_next)
                .map_err(|error| malformed(format!("invalid lifecycle digest: {error}")))?
        || correction_receipt.resulting_token_authority_digest
            != super::super::stream_token::stream_token_authority_plan_digest(
                &protocol.token.planned_rows,
            )
            .map_err(|error| malformed(format!("invalid token-plan digest: {error}")))?
    {
        return Err(malformed(
            "planned v20 lifecycle differs from the exact DataBlock correction successor"
                .to_string(),
        ));
    }
    let expected_token_post = protocol
        .token
        .planned_transaction
        .read_version
        .checked_add(1)
        .ok_or_else(|| malformed("correction token version overflow".to_string()))?;
    match (
        protocol.effect_phase,
        protocol.base.confirmed_transaction.as_ref(),
        protocol.base.confirmed_merged_generation.as_ref(),
        protocol.base.confirmed_head.as_ref(),
        protocol.base.confirmed_update.as_ref(),
        protocol.token.confirmed_transaction.as_ref(),
        protocol.token.confirmed_head.as_ref(),
        protocol.token.next_authority.as_ref(),
        protocol.next_lifecycle.as_ref(),
    ) {
        (RecoveryEffectPhase::Armed, None, None, None, None, None, None, None, None)
            if pin.confirmed_version.is_none() => {}
        (
            RecoveryEffectPhase::EffectsConfirmed,
            Some(base_transaction),
            Some(merged),
            Some(base_head),
            Some(base_update),
            Some(token_transaction),
            Some(token_head),
            Some(next_authority),
            Some(next_lifecycle),
        ) if base_transaction == &protocol.base.planned_transaction
            && merged == &protocol.merged_generation
            && base_head.branch_identifier == lance::dataset::refs::BranchIdentifier::main()
            && base_head.table_version == expected_post
            && base_head.transaction_uuid == protocol.base.planned_transaction.uuid
            && base_update.table_version == expected_post
            && base_update.table_branch.is_none()
            && pin.confirmed_version == Some(expected_post)
            && token_transaction == &protocol.token.planned_transaction
            && token_head.branch_identifier == lance::dataset::refs::BranchIdentifier::main()
            && token_head.table_version == expected_token_post
            && token_head.transaction_uuid == protocol.token.planned_transaction.uuid
            && next_authority.current_head_witness == *token_head
            && next_lifecycle.current_head_witness == *base_head =>
        {
            let mut expected = protocol.planned_next_lifecycle.clone();
            expected.current_head_witness = base_head.clone();
            if let Some(drain) = expected.drain.as_mut() {
                drain.expected_current_head_witness = base_head.clone();
            }
            if next_lifecycle != &expected
                || next_authority.location != protocol.token.prior_authority.location
                || next_authority.schema_version != protocol.token.prior_authority.schema_version
                || next_authority.schema_hash != protocol.token.prior_authority.schema_hash
            {
                return Err(malformed(
                    "confirmed v20 lifecycle or token authority differs outside achieved witnesses"
                        .to_string(),
                ));
            }
        }
        _ => {
            return Err(malformed(
                "v20 confirmation must be wholly absent while Armed or exact for both participants"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn new_stream_correction_sidecar_v20(
    table: SidecarTablePin,
    authority: RecoveryAuthorityToken,
    lineage: RecoveryLineageIntent,
    prior_manifest_version: u64,
    profile: super::super::StreamProfileEntry,
    prior_lifecycle: super::super::StreamLifecycleEntry,
    current_claim_receipt: super::super::stream::ClaimReceipt,
    planned_next_lifecycle: super::super::StreamLifecycleEntry,
    prior_merged_generation: Option<MergedGeneration>,
    generation_cut: RecoveryStreamFoldCut,
    base_planned_transaction: StagedTransactionIdentity,
    prior_token_authority: super::super::StreamTokenAuthorityEntry,
    token_planned_transaction: StagedTransactionIdentity,
    blocked_winners: Vec<super::super::stream_token::StreamTokenAuthorityRow>,
    token_planned_rows: Vec<super::super::stream_token::StreamTokenAuthorityRow>,
    planned_correction_receipt: super::super::stream::StreamCorrectionReceipt,
    planned_management_receipt: super::super::stream::ManagementReceipt,
    attribution_summary: Option<RecoveryStreamFoldAttributionSummary>,
) -> Result<RecoverySidecar> {
    let binding = prior_lifecycle.binding.clone();
    let admission_scope = RecoveryStreamAdmissionScope {
        identity: prior_lifecycle.identity,
        table_branch: binding.table_branch.clone(),
        binding_scope_id: prior_lifecycle.binding_scope_id.clone(),
    };
    let merged_generation =
        MergedGeneration::new(generation_cut.shard_id, generation_cut.generation);
    let actor = lineage.actor_id.clone();
    let mut sidecar = new_unvalidated_sidecar(
        STREAM_CORRECTION_SIDECAR_SCHEMA_VERSION,
        SidecarKind::StreamCorrection,
        None,
        actor,
        vec![table],
    );
    sidecar.protocol_v20 = Some(Box::new(RecoveryProtocolV20::StreamCorrection(
        RecoveryStreamCorrectionV20 {
            authority,
            lineage,
            admission_scope,
            prior_manifest_version,
            profile,
            binding,
            prior_merged_generation,
            generation_cut,
            merged_generation,
            effect_phase: RecoveryEffectPhase::Armed,
            prior_lifecycle,
            current_claim_receipt,
            planned_next_lifecycle,
            next_lifecycle: None,
            base: RecoveryStreamCorrectionBaseEffectV20 {
                planned_transaction: base_planned_transaction,
                confirmed_transaction: None,
                confirmed_merged_generation: None,
                confirmed_head: None,
                confirmed_update: None,
            },
            token: RecoveryStreamCorrectionTokenEffectV20 {
                prior_authority: prior_token_authority,
                blocked_winners,
                planned_transaction: token_planned_transaction,
                planned_rows: token_planned_rows,
                planned_correction_receipt,
                planned_management_receipt,
                attribution_summary,
                confirmed_transaction: None,
                confirmed_head: None,
                next_authority: None,
            },
        },
    )));
    validate_sidecar_shape("<new-stream-correction-sidecar-v20>", &sidecar)?;
    Ok(sidecar)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn confirm_stream_correction_sidecar_v20(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &mut RecoverySidecar,
    base_committed_transaction: StagedTransactionIdentity,
    confirmed_merged_generation: MergedGeneration,
    achieved_base_head: super::super::CurrentHeadWitness,
    base_update: SubTableUpdate,
    token_committed_transaction: StagedTransactionIdentity,
    achieved_token_head: super::super::CurrentHeadWitness,
    next_token_authority: super::super::StreamTokenAuthorityEntry,
) -> Result<()> {
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    validate_sidecar_shape(&uri, sidecar)?;
    let current = protocol(sidecar);
    let expected_token_post = current
        .token
        .planned_transaction
        .read_version
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("StreamCorrection token version overflow"))?;
    if current.effect_phase != RecoveryEffectPhase::Armed
        || base_committed_transaction != current.base.planned_transaction
        || confirmed_merged_generation != current.merged_generation
        || token_committed_transaction != current.token.planned_transaction
        || base_update.identity != sidecar.tables[0].identity
        || base_update.table_key != sidecar.tables[0].table_key
        || base_update.table_version != sidecar.tables[0].post_commit_pin
        || base_update.table_branch.is_some()
        || achieved_base_head.table_version != sidecar.tables[0].post_commit_pin
        || achieved_base_head.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || achieved_base_head.transaction_uuid != base_committed_transaction.uuid
        || achieved_token_head.table_version != expected_token_post
        || achieved_token_head.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || achieved_token_head.transaction_uuid != token_committed_transaction.uuid
        || next_token_authority.current_head_witness != achieved_token_head
        || next_token_authority.location != current.token.prior_authority.location
        || next_token_authority.schema_version != current.token.prior_authority.schema_version
        || next_token_authority.schema_hash != current.token.prior_authority.schema_hash
    {
        return Err(OmniError::manifest_internal(
            "StreamCorrection confirmation differs from its exact base+token plan",
        ));
    }
    let mut confirmed = sidecar.clone();
    confirmed.tables[0].confirmed_version = Some(base_update.table_version);
    let value = protocol_mut(&mut confirmed);
    value.effect_phase = RecoveryEffectPhase::EffectsConfirmed;
    value.base.confirmed_transaction = Some(base_committed_transaction);
    value.base.confirmed_merged_generation = Some(confirmed_merged_generation);
    value.base.confirmed_head = Some(achieved_base_head.clone());
    value.base.confirmed_update = Some(RecoveryConfirmedTableUpdate {
        table_version: base_update.table_version,
        table_branch: base_update.table_branch,
        row_count: base_update.row_count,
        version_metadata: base_update.version_metadata,
    });
    value.token.confirmed_transaction = Some(token_committed_transaction);
    value.token.confirmed_head = Some(achieved_token_head);
    value.token.next_authority = Some(next_token_authority);
    let mut next_lifecycle = value.planned_next_lifecycle.clone();
    next_lifecycle.current_head_witness = achieved_base_head.clone();
    if let Some(drain) = next_lifecycle.drain.as_mut() {
        drain.expected_current_head_witness = achieved_base_head;
    }
    value.next_lifecycle = Some(next_lifecycle);
    validate_sidecar_shape(&uri, &confirmed)?;
    let json = serde_json::to_string_pretty(&confirmed).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize confirmed StreamCorrection sidecar: {error}"
        ))
    })?;
    storage.write_text(&uri, &json).await?;
    *sidecar = confirmed;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParticipantState {
    ExactNoEffect,
    ExactEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectMatrix {
    EffectFree,
    BaseOnly,
    TokenOnly,
    Complete,
}

fn effect_matrix(base: ParticipantState, token: ParticipantState) -> EffectMatrix {
    match (base, token) {
        (ParticipantState::ExactNoEffect, ParticipantState::ExactNoEffect) => {
            EffectMatrix::EffectFree
        }
        (ParticipantState::ExactEffect, ParticipantState::ExactNoEffect) => EffectMatrix::BaseOnly,
        (ParticipantState::ExactNoEffect, ParticipantState::ExactEffect) => EffectMatrix::TokenOnly,
        (ParticipantState::ExactEffect, ParticipantState::ExactEffect) => EffectMatrix::Complete,
    }
}

struct ObservedBase {
    state: ParticipantState,
    head: super::super::CurrentHeadWitness,
    transaction: StagedTransactionIdentity,
    update: Option<RecoveryConfirmedTableUpdate>,
}

fn marker_only_correction_operation_matches(
    operation: &lance::dataset::transaction::Operation,
    expected_marker: &MergedGeneration,
    expected_preserving_fields: &[u32],
    expected_filter: &lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilter,
) -> bool {
    matches!(
        operation,
        lance::dataset::transaction::Operation::Update {
            removed_fragment_ids,
            updated_fragments,
            new_fragments,
            fields_modified,
            merged_generations,
            fields_for_preserving_frag_bitmap,
            update_mode,
            inserted_rows_filter,
            updated_fragment_offsets,
        } if removed_fragment_ids.is_empty()
            && updated_fragments.is_empty()
            && new_fragments.is_empty()
            && fields_modified.is_empty()
            && merged_generations.as_slice() == std::slice::from_ref(expected_marker)
            && fields_for_preserving_frag_bitmap == expected_preserving_fields
            && update_mode == &Some(lance::dataset::transaction::UpdateMode::RewriteRows)
            && inserted_rows_filter.as_ref() == Some(expected_filter)
            && updated_fragment_offsets.is_none()
    )
}

async fn observe_base(root_uri: &str, sidecar: &RecoverySidecar) -> Result<ObservedBase> {
    let value = protocol(sidecar);
    let pin = &sidecar.tables[0];
    let dataset = crate::instrumentation::open_dataset(
        &pin.table_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    let head = capture_current_head_witness(&dataset)
        .await
        .map_err(|error| correction_error(sidecar, error))?;
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| correction_error(sidecar, error))?
        .ok_or_else(|| correction_error(sidecar, "base HEAD has no transaction"))?;
    let transaction_identity = StagedTransactionIdentity::from(&transaction);
    let transaction_merged = match &transaction.operation {
        lance::dataset::transaction::Operation::Update {
            merged_generations, ..
        } => Some(merged_generations.as_slice()),
        _ => None,
    };
    let details = dataset
        .mem_wal_index_details()
        .await
        .map_err(|error| correction_error(sidecar, error))?
        .ok_or_else(|| correction_error(sidecar, "base table lost its MemWAL index"))?;
    validate_stream_config_v3_binding(&details, &value.binding)
        .map_err(|error| correction_error(sidecar, error))?;
    if details
        .merged_generations
        .iter()
        .any(|progress| progress.shard_id != value.generation_cut.shard_id)
    {
        return Err(correction_error(
            sidecar,
            "base table has foreign-shard merged progress",
        ));
    }
    let mut matching_merged = details
        .merged_generations
        .iter()
        .filter(|progress| progress.shard_id == value.generation_cut.shard_id);
    let observed_merged = matching_merged.next();
    if matching_merged.next().is_some() {
        return Err(correction_error(
            sidecar,
            "base table has duplicate merged progress for the correction shard",
        ));
    }
    let state = if head.table_version == pin.expected_version {
        if head != value.prior_lifecycle.current_head_witness
            || observed_merged != value.prior_merged_generation.as_ref()
            || value.effect_phase != RecoveryEffectPhase::Armed
        {
            return Err(correction_error(
                sidecar,
                "numeric base no-movement differs from the Armed prior lifecycle",
            ));
        }
        ParticipantState::ExactNoEffect
    } else if head.table_version == pin.post_commit_pin
        && head.transaction_uuid == value.base.planned_transaction.uuid
        && transaction_identity == value.base.planned_transaction
        && transaction_merged == Some(std::slice::from_ref(&value.merged_generation))
        && observed_merged == Some(&value.merged_generation)
    {
        ParticipantState::ExactEffect
    } else {
        return Err(correction_error(
            sidecar,
            "base HEAD is neither exact selected N nor the pre-minted N+1 correction effect",
        ));
    };
    let all_withdrawn = value.token.planned_rows.iter().all(|row| {
        row.disposition == super::super::stream_token::StreamTokenDisposition::Withdrawn
    });
    if state == ParticipantState::ExactEffect && all_withdrawn {
        let preserving_fields = dataset
            .schema()
            .fields_pre_order()
            .map(|field| {
                u32::try_from(field.id).map_err(|_| {
                    correction_error(sidecar, "base schema contains a negative field id")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let primary_key = dataset.schema().unenforced_primary_key();
        if primary_key.len() != 1
            || primary_key[0].name != "id"
            || primary_key[0].nullable
            || primary_key[0].data_type() != arrow_schema::DataType::Utf8
        {
            return Err(correction_error(
                sidecar,
                "all-WITHDRAW correction base lost its exact id primary key",
            ));
        }
        let expected_filter =
            lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilterBuilder::new(
                vec![primary_key[0].id],
            )
            .build();
        if !marker_only_correction_operation_matches(
            &transaction.operation,
            &value.merged_generation,
            &preserving_fields,
            &expected_filter,
        ) {
            return Err(correction_error(
                sidecar,
                "all-WITHDRAW correction base transaction is not the exact marker-only Lance Update shape",
            ));
        }
    }
    let update = if state == ParticipantState::ExactEffect {
        let row_count = u64::try_from(
            dataset
                .count_rows(None)
                .await
                .map_err(|error| correction_error(sidecar, error))?,
        )
        .map_err(|_| correction_error(sidecar, "base row count exceeds u64"))?;
        Some(RecoveryConfirmedTableUpdate {
            table_version: head.table_version,
            table_branch: None,
            row_count,
            version_metadata: super::super::TableVersionMetadata::from_dataset(
                root_uri,
                &value.binding.table_location,
                &dataset,
            )
            .map_err(|error| correction_error(sidecar, error))?,
        })
    } else {
        None
    };
    if value.effect_phase == RecoveryEffectPhase::EffectsConfirmed
        && (value.base.confirmed_transaction.as_ref() != Some(&transaction_identity)
            || value.base.confirmed_head.as_ref() != Some(&head)
            || value.base.confirmed_merged_generation.as_ref() != Some(&value.merged_generation)
            || value.base.confirmed_update.as_ref() != update.as_ref()
            || pin.confirmed_version != Some(head.table_version))
    {
        return Err(correction_error(
            sidecar,
            "durable base confirmation differs from exact observed N+1",
        ));
    }
    Ok(ObservedBase {
        state,
        head,
        transaction: transaction_identity,
        update,
    })
}

struct ObservedToken {
    state: ParticipantState,
    head: super::super::CurrentHeadWitness,
    transaction: StagedTransactionIdentity,
    authority: super::super::StreamTokenAuthorityEntry,
}

async fn validate_token_receipts(
    dataset: &lance::Dataset,
    authority: &super::super::StreamTokenAuthorityEntry,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let value = protocol(sidecar);
    let correction = &value.token.planned_correction_receipt;
    let selected = super::super::token_store::lookup_stream_correction_receipt(
        dataset,
        authority,
        &correction.graph_identity_digest,
        correction.identity,
        &correction.stream_incarnation_id,
        &correction.block_token,
        &correction.correction_id,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    if selected.as_ref() != Some(correction) {
        return Err(correction_error(
            sidecar,
            "exact token effect omits its immutable correction receipt",
        ));
    }
    let management = &value.token.planned_management_receipt;
    let selected = super::super::token_store::lookup_management_receipt(
        dataset,
        authority,
        &management.graph_identity_digest,
        management.identity,
        &management.stream_incarnation_id,
        &management.operation_kind,
        &management.operation_id,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    if selected.as_ref() != Some(management) {
        return Err(correction_error(
            sidecar,
            "exact token effect omits its immutable management receipt",
        ));
    }
    Ok(())
}

async fn observe_token(root_uri: &str, sidecar: &RecoverySidecar) -> Result<ObservedToken> {
    let value = protocol(sidecar);
    let token_uri = super::super::layout::stream_token_uri(root_uri);
    let session = crate::lance_access::control_session();
    let dataset = crate::instrumentation::open_dataset(
        &token_uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(&session),
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    let authority = super::super::token_store::stream_token_authority_entry_for_dataset(&dataset)
        .await
        .map_err(|error| correction_error(sidecar, error))?;
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| correction_error(sidecar, error))?
        .ok_or_else(|| correction_error(sidecar, "token HEAD has no transaction"))?;
    let transaction = StagedTransactionIdentity::from(&transaction);
    let state = if authority == value.token.prior_authority {
        if value.effect_phase != RecoveryEffectPhase::Armed {
            return Err(correction_error(
                sidecar,
                "confirmed correction has no token movement",
            ));
        }
        ParticipantState::ExactNoEffect
    } else {
        let expected_version = value
            .token
            .planned_transaction
            .read_version
            .checked_add(1)
            .ok_or_else(|| correction_error(sidecar, "token version overflow"))?;
        if authority.location != value.token.prior_authority.location
            || authority.schema_version != value.token.prior_authority.schema_version
            || authority.schema_hash != value.token.prior_authority.schema_hash
            || authority.current_head_witness.table_version != expected_version
            || authority.current_head_witness.transaction_uuid
                != value.token.planned_transaction.uuid
            || transaction != value.token.planned_transaction
        {
            return Err(correction_error(
                sidecar,
                "token HEAD is neither selected N nor the pre-minted N+1 correction effect",
            ));
        }
        validate_token_receipts(&dataset, &authority, sidecar).await?;
        let logical_ids = value
            .token
            .planned_rows
            .iter()
            .map(|row| row.logical_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let achieved_rows = super::super::token_store::stream_token_rows_for_keys(
            &dataset,
            &authority,
            sidecar.tables[0].identity,
            &logical_ids,
        )
        .await
        .map_err(|error| correction_error(sidecar, error))?;
        if achieved_rows.len() != value.token.planned_rows.len()
            || value
                .token
                .planned_rows
                .iter()
                .any(|planned| achieved_rows.get(&planned.logical_id) != Some(planned))
        {
            return Err(correction_error(
                sidecar,
                "exact token transaction omits or changes one or more planned current-token rows",
            ));
        }
        ParticipantState::ExactEffect
    };
    if value.effect_phase == RecoveryEffectPhase::EffectsConfirmed
        && (value.token.confirmed_transaction.as_ref() != Some(&transaction)
            || value.token.confirmed_head.as_ref() != Some(&authority.current_head_witness)
            || value.token.next_authority.as_ref() != Some(&authority))
    {
        return Err(correction_error(
            sidecar,
            "durable token confirmation differs from exact observed N+1",
        ));
    }
    Ok(ObservedToken {
        state,
        head: authority.current_head_witness.clone(),
        transaction,
        authority,
    })
}

async fn finish_missing_token(root_uri: &str, sidecar: &RecoverySidecar) -> Result<ObservedToken> {
    let value = protocol(sidecar);
    if value.effect_phase != RecoveryEffectPhase::Armed {
        return Err(correction_error(
            sidecar,
            "confirmed correction cannot recreate a missing token effect",
        ));
    }
    let session = crate::lance_access::control_session();
    let dataset = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &value.token.prior_authority,
        &session,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    let mut staged = super::super::token_store::stage_stream_correction_effect(
        dataset.clone(),
        &value.token.prior_authority,
        &value.token.planned_rows,
        &value.token.planned_correction_receipt,
        &value.token.planned_management_receipt,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    staged
        .bind_transaction_identity(&value.token.planned_transaction)
        .map_err(|error| correction_error(sidecar, error))?;
    let table_store = crate::table_store::TableStore::new(root_uri, session);
    let (_, committed) = table_store
        .commit_staged_exact(Arc::new(dataset), staged)
        .await
        .map_err(|error| correction_error(sidecar, error))?;
    if committed != value.token.planned_transaction {
        return Err(correction_error(
            sidecar,
            "token recovery committed a non-exact transaction",
        ));
    }
    let observed = observe_token(root_uri, sidecar).await?;
    if observed.state != ParticipantState::ExactEffect {
        return Err(correction_error(
            sidecar,
            "token recovery did not converge to exact N+1",
        ));
    }
    Ok(observed)
}

async fn validate_achieved_base_authority(root_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let value = protocol(sidecar);
    let mut planned_present = Vec::new();
    let mut forbidden_withdrawn = Vec::new();
    for (blocked, planned) in value
        .token
        .blocked_winners
        .iter()
        .zip(&value.token.planned_rows)
    {
        match planned.disposition {
            super::super::stream_token::StreamTokenDisposition::Present => {
                planned_present.push(planned.clone());
            }
            super::super::stream_token::StreamTokenDisposition::Withdrawn => {
                // Withdrawal removes this occurrence's authority. An older
                // base winner may remain, but the blocked winner itself must
                // not be the trusted metadata selected at achieved N+1.
                forbidden_withdrawn.push(blocked.clone());
            }
        }
    }
    super::validate_stream_fold_token_rows_against_base(
        root_uri,
        sidecar,
        &value.binding,
        &planned_present,
        &forbidden_withdrawn,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    if !planned_present.is_empty() && !forbidden_withdrawn.is_empty() {
        validate_mixed_withdraw_rows_unchanged(root_uri, sidecar, &forbidden_withdrawn).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseRowFingerprint {
    payload_digest: super::super::stream_token::PayloadDigest,
    metadata: Option<super::super::stream_token::TrustedStreamRowMetadata>,
}

async fn capture_base_row_fingerprints(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    table_version: u64,
    logical_ids: &std::collections::BTreeSet<String>,
) -> Result<std::collections::BTreeMap<String, BaseRowFingerprint>> {
    let value = protocol(sidecar);
    let uri = format!(
        "{}/{}",
        root_uri.trim_end_matches('/'),
        value.binding.table_location.trim_start_matches('/')
    );
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::At(table_version),
        Some(&crate::lance_access::control_session()),
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    let row_limit = logical_ids.len().saturating_add(1);
    let mut scanner = dataset.scan();
    scanner.filter_expr(
        datafusion::prelude::col("id").in_list(
            logical_ids
                .iter()
                .cloned()
                .map(datafusion::prelude::lit)
                .collect(),
            false,
        ),
    );
    scanner.batch_size(row_limit);
    scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
    scanner
        .limit(
            Some(i64::try_from(row_limit).map_err(|_| {
                correction_error(sidecar, "withdrawal row-proof limit exceeds i64")
            })?),
            None,
        )
        .map_err(|error| correction_error(sidecar, error))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|error| correction_error(sidecar, error))?;
    let mut selected = std::collections::BTreeMap::new();
    let mut observed_rows = 0_usize;
    let mut materialized_bytes = 0_u64;
    let mut retained_bytes = 0_u64;
    while let Some(batch) = futures::TryStreamExt::try_next(&mut stream)
        .await
        .map_err(|error| correction_error(sidecar, error))?
    {
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| correction_error(sidecar, "withdrawal row-proof count overflow"))?;
        if observed_rows > logical_ids.len() {
            return Err(correction_error(
                sidecar,
                "withdrawal row-proof returned more than one row per exact key",
            ));
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| correction_error(sidecar, "withdrawal row-proof bytes exceed u64"))?;
        materialized_bytes = super::super::token_store::add_stream_lookup_retained_bytes(
            "stream_correction_withdrawal_materialized_bytes",
            materialized_bytes,
            batch_bytes,
            crate::table_store::mem_wal::B2_MAX_CANONICAL_PAYLOAD_BYTES,
        )
        .map_err(|error| correction_error(sidecar, error))?;
        let ids = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<arrow_array::StringArray>())
            .ok_or_else(|| correction_error(sidecar, "withdrawal row-proof omitted Utf8 id"))?;
        let metadata = batch
            .column_by_name(crate::db::STREAM_METADATA_COLUMN)
            .ok_or_else(|| {
                correction_error(sidecar, "withdrawal row-proof omitted trusted metadata")
            })?;
        for row_index in 0..batch.num_rows() {
            if arrow_array::Array::is_null(ids, row_index)
                || !logical_ids.contains(ids.value(row_index))
            {
                return Err(correction_error(
                    sidecar,
                    "withdrawal row-proof returned a null or foreign logical id",
                ));
            }
            let logical_id = ids.value(row_index);
            let canonical_payload =
                crate::db::omnigraph::canonical_stream_payload_v1(&batch, row_index)
                    .map_err(|error| correction_error(sidecar, error))?;
            let payload_digest = super::super::stream_token::PayloadDigest::derive(
                &super::super::stream_token::PayloadDigestInput {
                    identity: sidecar.tables[0].identity,
                    accepted_schema_hash: &value.authority.schema_ir_hash,
                    canonical_payload: &canonical_payload,
                },
            )
            .map_err(|error| correction_error(sidecar, error))?;
            let metadata = super::super::stream_token::decode_trusted_stream_metadata(
                metadata.as_ref(),
                row_index,
            )
            .map_err(|error| correction_error(sidecar, error))?;
            let metadata_retained = metadata
                .as_ref()
                .map_or(Ok(0_u64), |metadata| {
                    metadata.lookup_retained_bytes(logical_id)
                })
                .map_err(|error| correction_error(sidecar, error))?;
            let retained = u64::try_from(
                std::mem::size_of::<String>()
                    .saturating_add(logical_id.len())
                    .saturating_add(std::mem::size_of::<BaseRowFingerprint>()),
            )
            .map_err(|_| correction_error(sidecar, "withdrawal fingerprint bytes exceed u64"))?
            .checked_add(metadata_retained)
            .ok_or_else(|| correction_error(sidecar, "withdrawal fingerprint byte overflow"))?;
            retained_bytes = super::super::token_store::add_stream_lookup_retained_bytes(
                "stream_correction_withdrawal_retained_bytes",
                retained_bytes,
                retained,
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
            )
            .map_err(|error| correction_error(sidecar, error))?;
            if selected
                .insert(
                    logical_id.to_string(),
                    BaseRowFingerprint {
                        payload_digest,
                        metadata,
                    },
                )
                .is_some()
            {
                return Err(correction_error(
                    sidecar,
                    "withdrawal row-proof found duplicate exact-id rows",
                ));
            }
        }
    }
    Ok(selected)
}

async fn validate_mixed_withdraw_rows_unchanged(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    withdrawn_blocked: &[super::super::stream_token::StreamTokenAuthorityRow],
) -> Result<()> {
    let logical_ids = withdrawn_blocked
        .iter()
        .map(|row| row.logical_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let prior = capture_base_row_fingerprints(
        root_uri,
        sidecar,
        sidecar.tables[0].expected_version,
        &logical_ids,
    )
    .await?;
    let achieved = capture_base_row_fingerprints(
        root_uri,
        sidecar,
        sidecar.tables[0].post_commit_pin,
        &logical_ids,
    )
    .await?;
    if achieved != prior {
        return Err(correction_error(
            sidecar,
            "mixed correction changed a WITHDRAW key's prior manifest-visible base row",
        ));
    }
    Ok(())
}

fn manifest_is_prior(snapshot: &Snapshot, sidecar: &RecoverySidecar) -> bool {
    let value = protocol(sidecar);
    let pin = &sidecar.tables[0];
    snapshot.version() == value.prior_manifest_version
        && snapshot.stream_profile() == &value.profile
        && snapshot.stream_token_authority() == &value.token.prior_authority
        && snapshot.stream_lifecycle(pin.identity) == Some(&value.prior_lifecycle)
        && snapshot_entry_by_identity(snapshot, pin.identity).is_some_and(|entry| {
            entry.table_key == pin.table_key
                && entry.table_path == value.binding.table_location
                && entry.table_version == pin.expected_version
                && entry.table_branch.is_none()
        })
}

fn validate_planned_token_successor(
    prior: &super::super::stream_token::StreamTokenAuthorityRow,
    planned: &super::super::stream_token::StreamTokenAuthorityRow,
    actor: &super::super::stream_token::TrustedContributorId,
    correction_id: &str,
) -> std::result::Result<bool, String> {
    if planned == prior {
        return Ok(false);
    }
    let expected = match (&planned.disposition, &planned.origin) {
        (
            super::super::stream_token::StreamTokenDisposition::Present,
            super::super::stream_token::StreamRowOrigin::Correction { plan_ordinal, .. },
        ) => {
            let metadata = super::super::stream_token::TrustedStreamRowMetadata::new_correction(
                prior,
                actor.clone(),
                planned.write_id.clone(),
                planned.payload_digest,
                correction_id.to_string(),
                *plan_ordinal,
            )
            .map_err(|error| error.to_string())?;
            super::super::stream_token::StreamTokenAuthorityRow::from_present_metadata(
                prior.identity,
                prior.logical_id.clone(),
                prior.origin_enrollment_id.clone(),
                &metadata,
            )
            .map_err(|error| error.to_string())?
        }
        (super::super::stream_token::StreamTokenDisposition::Withdrawn, _) => {
            super::super::stream_token::StreamTokenAuthorityRow::withdraw_blocked_winner(
                prior,
                actor.clone(),
                correction_id.to_string(),
            )
            .map_err(|error| error.to_string())?
        }
        _ => return Err("planned row is not a correction successor".to_string()),
    };
    if planned != &expected {
        return Err("planned row differs from the exact correction successor".to_string());
    }
    Ok(true)
}

async fn validate_selected_claim(root_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let value = protocol(sidecar);
    let session = crate::lance_access::control_session();
    let dataset = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &value.token.prior_authority,
        &session,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    let graph_digest =
        super::super::stream::stream_graph_identity_digest(&value.authority.schema_identity_domain)
            .map_err(|error| correction_error(sidecar, error))?;
    let selected = super::super::token_store::lookup_claim_receipt(
        &dataset,
        &value.token.prior_authority,
        &graph_digest,
        value.prior_lifecycle.identity,
        &value.prior_lifecycle.binding_scope_id,
        &value.current_claim_receipt.claim_id,
    )
    .await
    .map_err(|error| correction_error(sidecar, error))?;
    if selected.as_ref() != Some(&value.current_claim_receipt) {
        return Err(correction_error(
            sidecar,
            "selected prior token authority omits the exact current claim receipt",
        ));
    }
    Ok(())
}

async fn original_visible(root_uri: &str, sidecar: &RecoverySidecar) -> Result<bool> {
    let value = protocol(sidecar);
    if value.effect_phase != RecoveryEffectPhase::EffectsConfirmed {
        return Ok(false);
    }
    let (commits, _) = ManifestCoordinator::read_graph_lineage_at(root_uri, None).await?;
    let Some(commit) = commits
        .iter()
        .find(|commit| commit.graph_commit_id == value.lineage.graph_commit_id)
    else {
        return Ok(false);
    };
    let expected_manifest = value
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| correction_error(sidecar, "manifest version overflow"))?;
    if commit.manifest_branch.is_some()
        || commit.manifest_version != expected_manifest
        || commit.parent_commit_id.as_ref() != value.authority.graph_head.as_ref()
        || commit.merged_parent_commit_id.is_some()
        || commit.actor_id != value.lineage.actor_id
        || commit.created_at != value.lineage.created_at
        || commit.stream_fold_attribution != value.token.attribution_summary
    {
        return Err(correction_error(
            sidecar,
            "fixed correction lineage exists with different authority or attribution",
        ));
    }
    let committed = ManifestCoordinator::snapshot_at(root_uri, None, expected_manifest).await?;
    let pin = &sidecar.tables[0];
    let update = value
        .base
        .confirmed_update
        .as_ref()
        .expect("validated confirmed base update");
    let next_lifecycle = value
        .next_lifecycle
        .as_ref()
        .expect("validated confirmed lifecycle");
    let next_token = value
        .token
        .next_authority
        .as_ref()
        .expect("validated confirmed token authority");
    let exact = snapshot_entry_by_identity(&committed, pin.identity).is_some_and(|entry| {
        entry.table_key == pin.table_key
            && entry.table_path == value.binding.table_location
            && entry.table_version == update.table_version
            && entry.row_count == update.row_count
            && entry.version_metadata == update.version_metadata
    }) && committed.stream_lifecycle(pin.identity) == Some(next_lifecycle)
        && committed.stream_token_authority() == next_token
        && committed.stream_profile() == &value.profile;
    if !exact {
        return Err(correction_error(
            sidecar,
            "fixed lineage is visible with a different table/token/lifecycle outcome",
        ));
    }
    Ok(true)
}

async fn finalize_visible(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<bool> {
    let value = protocol(sidecar);
    let pin = &sidecar.tables[0];
    let update = value
        .base
        .confirmed_update
        .as_ref()
        .expect("validated confirmed correction update");
    let outcomes = vec![TableOutcome {
        table_key: pin.table_key.clone(),
        from_version: pin.expected_version,
        to_version: update.table_version,
    }];
    let mut audit = RecoveryAudit::open(root_uri).await?;
    let records = audit.list().await?;
    let expected_kind = format!("{:?}", sidecar.writer_kind);
    let operation_records = records
        .iter()
        .filter(|record| record.operation_id == sidecar.operation_id)
        .collect::<Vec<_>>();
    if operation_records.iter().any(|record| {
        record.graph_commit_id != value.lineage.graph_commit_id
            || record.recovery_kind != RecoveryKind::RolledForward
            || record.recovery_for_actor != sidecar.actor_id
            || record.sidecar_writer_kind != expected_kind
            || record.per_table_outcomes != outcomes
    }) {
        return Err(correction_error(
            sidecar,
            "recovery audit differs from the fixed correction outcome",
        ));
    }
    if operation_records.is_empty() {
        audit
            .append(RecoveryAuditRecord {
                graph_commit_id: value.lineage.graph_commit_id.clone(),
                recovery_kind: RecoveryKind::RolledForward,
                recovery_for_actor: sidecar.actor_id.clone(),
                operation_id: sidecar.operation_id.clone(),
                sidecar_writer_kind: expected_kind,
                per_table_outcomes: outcomes,
                created_at: crate::db::now_micros()?,
            })
            .await?;
    }
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
    Ok(true)
}

pub(super) fn process_stream_correction_sidecar_v20<'a>(
    root_uri: &'a str,
    storage: &'a Arc<dyn StorageAdapter>,
    snapshot: &'a Snapshot,
    sidecar: &'a RecoverySidecar,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>> {
    // The recovery dispatcher keeps this large protocol future heap-bounded.
    // Fresh-stack execution, when needed, is owned by the outer open/heal task
    // that also owns the complete profile/admission/schema/branch/token/table
    // gate envelope. Never detach this effectful processor from caller-owned
    // guards: cancellation could otherwise release those guards before the
    // child has actually stopped.
    Box::pin(process_stream_correction_sidecar_v20_inner(
        root_uri, storage, snapshot, sidecar,
    ))
}

async fn process_stream_correction_sidecar_v20_inner(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<bool> {
    validate_sidecar_shape(&sidecar_uri(root_uri, &sidecar.operation_id), sidecar)?;
    let mut base = observe_base(root_uri, sidecar).await?;
    let mut token = observe_token(root_uri, sidecar).await?;
    if original_visible(root_uri, sidecar).await? {
        if effect_matrix(base.state, token.state) != EffectMatrix::Complete {
            return Err(correction_error(
                sidecar,
                "visible correction lineage lacks both exact effects",
            ));
        }
        validate_achieved_base_authority(root_uri, sidecar).await?;
        return finalize_visible(root_uri, storage.as_ref(), sidecar).await;
    }
    if !manifest_is_prior(snapshot, sidecar) {
        return Err(correction_error(
            sidecar,
            "manifest table/token/profile/lifecycle prestate changed after arm",
        ));
    }
    let value = protocol(sidecar);
    let live_authority = read_live_recovery_authority(root_uri, storage, None)
        .await
        .map_err(|error| correction_error(sidecar, error))?;
    if live_authority != value.authority {
        return Err(correction_error(
            sidecar,
            "graph/schema authority changed after arm",
        ));
    }
    validate_selected_claim(root_uri, sidecar).await?;
    match effect_matrix(base.state, token.state) {
        EffectMatrix::EffectFree => {
            delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id)
                .await?;
            return Ok(true);
        }
        EffectMatrix::TokenOnly => {
            return Err(correction_error(
                sidecar,
                "token-only outcome is impossible in commit order and base data cannot be reconstructed",
            ));
        }
        EffectMatrix::BaseOnly => token = finish_missing_token(root_uri, sidecar).await?,
        EffectMatrix::Complete => {}
    }
    if base.state != ParticipantState::ExactEffect || token.state != ParticipantState::ExactEffect {
        return Err(correction_error(
            sidecar,
            "physical participants did not converge to the exact joint outcome",
        ));
    }
    validate_achieved_base_authority(root_uri, sidecar).await?;
    let mut confirmed = sidecar.clone();
    if value.effect_phase == RecoveryEffectPhase::Armed {
        let update = base
            .update
            .clone()
            .expect("exact correction base has an update");
        confirm_stream_correction_sidecar_v20(
            root_uri,
            storage.as_ref(),
            &mut confirmed,
            base.transaction.clone(),
            value.merged_generation.clone(),
            base.head.clone(),
            SubTableUpdate {
                identity: sidecar.tables[0].identity,
                table_key: sidecar.tables[0].table_key.clone(),
                table_version: update.table_version,
                table_branch: update.table_branch,
                row_count: update.row_count,
                version_metadata: update.version_metadata,
            },
            token.transaction.clone(),
            token.head.clone(),
            token.authority.clone(),
        )
        .await?;
        base = observe_base(root_uri, &confirmed).await?;
    }
    let value = protocol(&confirmed);
    let update = value
        .base
        .confirmed_update
        .as_ref()
        .expect("confirmed correction base update");
    let changes = vec![
        ManifestChange::Update(SubTableUpdate {
            identity: confirmed.tables[0].identity,
            table_key: confirmed.tables[0].table_key.clone(),
            table_version: update.table_version,
            table_branch: update.table_branch.clone(),
            row_count: update.row_count,
            version_metadata: update.version_metadata.clone(),
        }),
        ManifestChange::SetStreamTokenAuthority {
            expected: value.token.prior_authority.clone(),
            next: value
                .token
                .next_authority
                .as_ref()
                .expect("confirmed token authority")
                .clone(),
        },
        ManifestChange::SetStreamLifecycle {
            expected: Some(value.prior_lifecycle.clone()),
            next: value
                .next_lifecycle
                .as_ref()
                .expect("confirmed lifecycle")
                .clone(),
        },
        ManifestChange::SetStreamProfile {
            expected: value.profile.clone(),
            next: value.profile.clone(),
        },
    ];
    let expected = HashMap::from([(
        confirmed.tables[0].identity,
        TableVersionExpectation {
            table_key: confirmed.tables[0].table_key.clone(),
            table_version: confirmed.tables[0].expected_version,
        },
    )]);
    let (manifest_version, graph_commit_id) = publish_recovery_commit(
        root_uri,
        &confirmed,
        RecoveryKind::RolledForward,
        &changes,
        &expected,
    )
    .await
    .map_err(|error| correction_error(&confirmed, error))?;
    let expected_manifest_version = value
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| correction_error(&confirmed, "manifest version overflow"))?;
    if manifest_version != expected_manifest_version
        || graph_commit_id != value.lineage.graph_commit_id
    {
        return Err(correction_error(
            &confirmed,
            "terminal publish returned a different manifest version or lineage ID",
        ));
    }
    debug_assert_eq!(base.state, ParticipantState::ExactEffect);
    finalize_visible(root_uri, storage.as_ref(), &confirmed).await
}

pub(crate) async fn complete_stream_correction_sidecar_v20(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamCorrectionOutcomeV20> {
    process_stream_correction_sidecar_v20(root_uri, &storage, snapshot, sidecar).await?;
    let value = protocol(sidecar);
    let expected_manifest_version = value
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| correction_error(sidecar, "manifest version overflow"))?;
    let committed =
        ManifestCoordinator::snapshot_at(root_uri, None, expected_manifest_version).await?;
    let lifecycle = committed
        .stream_lifecycle(sidecar.tables[0].identity)
        .ok_or_else(|| correction_error(sidecar, "terminal lifecycle is missing"))?;
    Ok(RecoveryStreamCorrectionOutcomeV20 {
        graph_commit_id: value.lineage.graph_commit_id.clone(),
        manifest_version: expected_manifest_version,
        lifecycle_revision: lifecycle.lifecycle_revision,
        base_head: lifecycle.current_head_witness.clone(),
        token_head: committed
            .stream_token_authority()
            .current_head_witness
            .clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_INCARNATION: &str = "11111111-1111-4111-8111-111111111111";
    const ENROLLMENT_ID: &str = "22222222-2222-4222-8222-222222222222";
    const ADMISSION_ID: &str = "33333333-3333-4333-8333-333333333333";
    const INITIAL_WRITE_ID: &str = "44444444-4444-4444-8444-444444444444";
    const REPLACEMENT_WRITE_ID: &str = "55555555-5555-4555-8555-555555555555";
    const CORRECTION_ID: &str = "66666666-6666-4666-8666-666666666666";

    fn correction_actor() -> super::super::super::stream_token::TrustedContributorId {
        super::super::super::stream_token::TrustedContributorId::new("actor:operator").unwrap()
    }

    fn prior_row() -> super::super::super::stream_token::StreamTokenAuthorityRow {
        let identity = super::super::super::TableIdentity::new(7, 9).unwrap();
        let request = super::super::super::stream_token::AdmissionRequest {
            identity,
            logical_id: "person-17".to_string(),
            envelope: super::super::super::stream_token::StreamWriteEnvelope {
                stream_incarnation_id: STREAM_INCARNATION.to_string(),
                write_id: INITIAL_WRITE_ID.to_string(),
                predecessor_token: None,
            },
            contributor_id: super::super::super::stream_token::TrustedContributorId::new(
                "actor:writer",
            )
            .unwrap(),
            payload_digest: super::super::super::stream_token::PayloadDigest::from_bytes([7; 32]),
        };
        let token = request.candidate_token().unwrap();
        let metadata = super::super::super::stream_token::TrustedStreamRowMetadata::new_admission(
            &request,
            token,
            None,
            1,
            ADMISSION_ID.to_string(),
            0,
        )
        .unwrap();
        super::super::super::stream_token::StreamTokenAuthorityRow::from_present_metadata(
            identity,
            request.logical_id,
            ENROLLMENT_ID.to_string(),
            &metadata,
        )
        .unwrap()
    }

    #[test]
    fn recovery_v20_is_a_distinct_closed_discriminator() {
        assert_eq!(STREAM_CORRECTION_SIDECAR_SCHEMA_VERSION, 20);
        assert_eq!(SIDECAR_SCHEMA_VERSION, 20);
        let json = serde_json::json!({
            "kind": "StreamCorrection",
            "payload": {
                "authority": {},
                "admission_scope": {},
                "operation_id": "not-the-v20-grammar"
            }
        });
        assert!(serde_json::from_value::<RecoveryProtocolV20>(json).is_err());
    }

    #[test]
    fn recovery_v20_effect_matrix_preserves_commit_order() {
        assert_eq!(
            effect_matrix(
                ParticipantState::ExactNoEffect,
                ParticipantState::ExactNoEffect
            ),
            EffectMatrix::EffectFree
        );
        assert_eq!(
            effect_matrix(
                ParticipantState::ExactEffect,
                ParticipantState::ExactNoEffect
            ),
            EffectMatrix::BaseOnly
        );
        assert_eq!(
            effect_matrix(
                ParticipantState::ExactNoEffect,
                ParticipantState::ExactEffect
            ),
            EffectMatrix::TokenOnly
        );
        assert_eq!(
            effect_matrix(ParticipantState::ExactEffect, ParticipantState::ExactEffect),
            EffectMatrix::Complete
        );
    }

    #[test]
    fn recovery_v20_system_lineage_and_operator_receipt_actors_are_distinct() {
        assert!(receipt_lineage_binding_matches(
            "actor:operator",
            "actor:operator",
            Some(super::super::super::stream_token::STREAM_FOLD_ACTOR),
            Some(super::super::super::stream_token::STREAM_FOLD_ACTOR),
            "01CORRECTIONCOMMIT",
            "01CORRECTIONCOMMIT",
        ));
        assert!(!receipt_lineage_binding_matches(
            "actor:operator",
            "actor:operator",
            Some("omnigraph:stream-correction"),
            Some("omnigraph:stream-correction"),
            "01CORRECTIONCOMMIT",
            "01CORRECTIONCOMMIT",
        ));
        assert!(!receipt_lineage_binding_matches(
            "actor:operator",
            "actor:other",
            Some(super::super::super::stream_token::STREAM_FOLD_ACTOR),
            Some(super::super::super::stream_token::STREAM_FOLD_ACTOR),
            "01CORRECTIONCOMMIT",
            "01CORRECTIONCOMMIT",
        ));
    }

    #[test]
    fn recovery_v20_partial_correction_accepts_unchanged_winners_and_only_exact_successors() {
        let prior = prior_row();
        let actor = correction_actor();
        assert!(!validate_planned_token_successor(&prior, &prior, &actor, CORRECTION_ID,).unwrap());

        let replacement =
            super::super::super::stream_token::TrustedStreamRowMetadata::new_correction(
                &prior,
                actor.clone(),
                REPLACEMENT_WRITE_ID.to_string(),
                super::super::super::stream_token::PayloadDigest::from_bytes([8; 32]),
                CORRECTION_ID.to_string(),
                19,
            )
            .and_then(|metadata| {
                super::super::super::stream_token::StreamTokenAuthorityRow::from_present_metadata(
                    prior.identity,
                    prior.logical_id.clone(),
                    prior.origin_enrollment_id.clone(),
                    &metadata,
                )
            })
            .unwrap();
        assert!(
            validate_planned_token_successor(&prior, &replacement, &actor, CORRECTION_ID,).unwrap()
        );

        let withdrawal =
            super::super::super::stream_token::StreamTokenAuthorityRow::withdraw_blocked_winner(
                &prior,
                actor.clone(),
                CORRECTION_ID.to_string(),
            )
            .unwrap();
        assert!(
            validate_planned_token_successor(&prior, &withdrawal, &actor, CORRECTION_ID,).unwrap()
        );

        let mut forged = replacement;
        forged.contributor_id =
            super::super::super::stream_token::TrustedContributorId::new("actor:forged").unwrap();
        assert!(validate_planned_token_successor(&prior, &forged, &actor, CORRECTION_ID,).is_err());
    }

    #[test]
    fn recovery_v20_all_withdraw_requires_the_exact_marker_only_update_shape() {
        let marker = MergedGeneration::new(
            lance_index::mem_wal::ShardId::parse_str("00000000-0000-4000-8000-000000000007")
                .unwrap(),
            3,
        );
        let preserving_fields = vec![1, 2, 3];
        let expected_filter =
            lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilterBuilder::new(
                vec![1],
            )
            .build();
        let valid = lance::dataset::transaction::Operation::Update {
            removed_fragment_ids: Vec::new(),
            updated_fragments: Vec::new(),
            new_fragments: Vec::new(),
            fields_modified: Vec::new(),
            merged_generations: vec![marker.clone()],
            fields_for_preserving_frag_bitmap: preserving_fields.clone(),
            update_mode: Some(lance::dataset::transaction::UpdateMode::RewriteRows),
            inserted_rows_filter: Some(expected_filter.clone()),
            updated_fragment_offsets: None,
        };
        assert!(marker_only_correction_operation_matches(
            &valid,
            &marker,
            &preserving_fields,
            &expected_filter,
        ));

        let mut data_bearing = valid.clone();
        let lance::dataset::transaction::Operation::Update { new_fragments, .. } =
            &mut data_bearing
        else {
            unreachable!()
        };
        new_fragments.push(lance_table::format::Fragment::new(0));
        assert!(!marker_only_correction_operation_matches(
            &data_bearing,
            &marker,
            &preserving_fields,
            &expected_filter,
        ));

        let mut wrong_mode = valid.clone();
        let lance::dataset::transaction::Operation::Update { update_mode, .. } = &mut wrong_mode
        else {
            unreachable!()
        };
        *update_mode = Some(lance::dataset::transaction::UpdateMode::RewriteColumns);
        assert!(!marker_only_correction_operation_matches(
            &wrong_mode,
            &marker,
            &preserving_fields,
            &expected_filter,
        ));

        let mut offsets = valid.clone();
        let lance::dataset::transaction::Operation::Update {
            updated_fragment_offsets,
            ..
        } = &mut offsets
        else {
            unreachable!()
        };
        *updated_fragment_offsets = Some(Default::default());
        assert!(!marker_only_correction_operation_matches(
            &offsets,
            &marker,
            &preserving_fields,
            &expected_filter,
        ));

        assert!(!marker_only_correction_operation_matches(
            &valid,
            &marker,
            &[1, 2],
            &expected_filter,
        ));

        let wrong_filter =
            lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilterBuilder::new(
                vec![2],
            )
            .build();
        assert!(!marker_only_correction_operation_matches(
            &valid,
            &marker,
            &preserving_fields,
            &wrong_filter,
        ));
    }
}
