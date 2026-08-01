//! Recovery-v19 root-wide stream-authority retirement.
//!
//! Retirement is deliberately smaller than the physical lifecycle writers:
//! one immutable receipt is appended to the graph-global token ledger, then
//! one manifest CAS selects that exact N+1 token witness and advances
//! `DISABLED -> RETIRED`. The manifest publication carries no lineage intent,
//! moves no graph head, and writes no separate recovery-audit record. The
//! receipt and profile-chain commitment are the audit authority.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::*;

/// Exact immutable token-ledger participant for authority retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamAuthorityRetirementReceiptEffectV19 {
    pub(crate) prior_authority: super::super::StreamTokenAuthorityEntry,
    pub(crate) planned_receipt: super::super::AuthorityRetirementReceipt,
    pub(crate) planned_transaction: StagedTransactionIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_transaction: Option<StagedTransactionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) confirmed_head: Option<super::super::CurrentHeadWitness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) next_authority: Option<super::super::StreamTokenAuthorityEntry>,
}

/// Complete bounded recovery authority for one retirement occurrence.
///
/// The unbounded graph/token scan is represented only by canonical digests in
/// `planned_receipt`. Recovery never reconstructs or stores a token-row vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryStreamAuthorityRetirementV19 {
    pub(crate) authority: RecoveryAuthorityToken,
    pub(crate) prior_manifest_version: u64,
    pub(crate) effect_phase: RecoveryEffectPhase,
    pub(crate) prior_profile: super::super::StreamProfileEntry,
    pub(crate) next_profile: super::super::StreamProfileEntry,
    pub(crate) receipt: RecoveryStreamAuthorityRetirementReceiptEffectV19,
}

/// V19 is a closed grammar. The frozen v14 retirement discriminator remains a
/// fail-closed scaffold and is never reinterpreted as this payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", deny_unknown_fields)]
pub(crate) enum RecoveryProtocolV19 {
    StreamAuthorityRetirement(RecoveryStreamAuthorityRetirementV19),
}

fn protocol(sidecar: &RecoverySidecar) -> &RecoveryStreamAuthorityRetirementV19 {
    let RecoveryProtocolV19::StreamAuthorityRetirement(protocol) = sidecar
        .protocol_v19
        .as_deref()
        .expect("validated schema-v19 StreamAuthorityRetirement");
    protocol
}

fn protocol_mut(sidecar: &mut RecoverySidecar) -> &mut RecoveryStreamAuthorityRetirementV19 {
    let RecoveryProtocolV19::StreamAuthorityRetirement(protocol) = sidecar
        .protocol_v19
        .as_deref_mut()
        .expect("validated schema-v19 StreamAuthorityRetirement");
    protocol
}

fn retirement_error(sidecar: &RecoverySidecar, reason: impl std::fmt::Display) -> OmniError {
    OmniError::recovery_required(
        sidecar.operation_id.clone(),
        format!("StreamAuthorityRetirement recovery cannot prove an exact outcome: {reason}"),
    )
}

/// Validate the closed v19 wire grammar and the exact Armed/confirmed split.
pub(super) fn validate_stream_protocol_v19_shape(
    sidecar_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let malformed = |reason: String| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' has an invalid schema-v{} shape: {}",
            sidecar_uri, sidecar.schema_version, reason
        ))
    };
    if sidecar.writer_kind != SidecarKind::StreamAuthorityRetirement
        || sidecar.branch.is_some()
        || !sidecar.tables.is_empty()
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
        || sidecar.ensure_indices_rollback_v6.is_some()
        || sidecar.merge_source_commit_id.is_some()
        || !sidecar.additional_registrations.is_empty()
        || !sidecar.tombstones.is_empty()
        || sidecar.schema_apply_manifest_published
        || sidecar.schema_apply_target_schema_ir_hash.is_some()
    {
        return Err(malformed(
            "schema-v19 StreamAuthorityRetirement must target canonical main with no table pins and carry only protocol_v19"
                .to_string(),
        ));
    }
    let protocol = sidecar
        .protocol_v19
        .as_deref()
        .ok_or_else(|| malformed("missing required protocol_v19 payload".to_string()))?;
    let RecoveryProtocolV19::StreamAuthorityRetirement(protocol) = protocol;

    validate_authority_identity(&malformed, &protocol.authority)?;
    if protocol.authority.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || protocol.prior_manifest_version == 0
    {
        return Err(malformed(
            "StreamAuthorityRetirement requires positive canonical-main manifest authority"
                .to_string(),
        ));
    }
    protocol
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| malformed("retirement manifest version overflow".to_string()))?;
    protocol
        .prior_profile
        .validate()
        .map_err(|error| malformed(format!("invalid prior stream profile: {error}")))?;
    if protocol.prior_profile.mode() != super::super::StreamProfileMode::Disabled {
        return Err(malformed(
            "StreamAuthorityRetirement prior profile must be DISABLED".to_string(),
        ));
    }
    protocol
        .next_profile
        .validate_transition_from(&protocol.prior_profile)
        .map_err(|error| malformed(format!("invalid next stream profile: {error}")))?;
    let (
        super::super::StreamProfileState::Retired {
            authority_retirement_id,
            authority_retirement_receipt_id,
            authority_retirement_cut_digest,
        },
        receipt,
    ) = (
        &protocol.next_profile.state,
        &protocol.receipt.planned_receipt,
    )
    else {
        return Err(malformed(
            "StreamAuthorityRetirement next profile must be RETIRED".to_string(),
        ));
    };
    receipt
        .validate()
        .map_err(|error| malformed(format!("invalid authority-retirement receipt: {error}")))?;
    let graph_identity_digest = super::super::stream::stream_graph_identity_digest(
        &protocol.authority.schema_identity_domain,
    )
    .map_err(|error| malformed(format!("invalid graph identity: {error}")))?;
    let expected_chain_ordinal = protocol
        .prior_profile
        .profile_receipt_chain
        .record_count
        .checked_add(1)
        .ok_or_else(|| malformed("profile receipt-chain count overflow".to_string()))?;
    if receipt.graph_identity_digest != graph_identity_digest
        || Some(receipt.actor.as_str()) != sidecar.actor_id.as_deref()
        || receipt.source_internal_schema_version != super::super::INTERNAL_MANIFEST_SCHEMA_VERSION
        || receipt.source_manifest_version != protocol.prior_manifest_version
        || receipt.source_profile_revision != protocol.prior_profile.profile_revision
        || receipt.withdrawn_token_count == 0
        || receipt.chain_ordinal != expected_chain_ordinal
        || receipt.predecessor_record_id
            != protocol.prior_profile.profile_receipt_chain.head_record_id
        || receipt.prior_chain_digest != protocol.prior_profile.profile_receipt_chain.chain_digest
        || authority_retirement_id != &receipt.retirement_id
        || authority_retirement_receipt_id != &receipt.record_id
        || authority_retirement_cut_digest != &receipt.export_cut_digest
        || receipt
            .next_chain_ref()
            .map_err(|error| malformed(format!("invalid next retirement receipt chain: {error}")))?
            != protocol.next_profile.profile_receipt_chain
    {
        return Err(malformed(
            "StreamAuthorityRetirement receipt does not bind the exact graph, actor, source format/manifest/profile, non-empty WITHDRAWN cut, or RETIRED profile"
                .to_string(),
        ));
    }

    protocol
        .receipt
        .prior_authority
        .validate()
        .map_err(|error| malformed(format!("invalid prior stream-token authority: {error}")))?;
    let expected_token_witness = super::super::stream_authority_retirement_token_witness_digest(
        &protocol.receipt.prior_authority.current_head_witness,
        receipt.present_token_count,
        receipt.withdrawn_token_count,
    )
    .map_err(|error| malformed(format!("invalid retirement token witness: {error}")))?;
    if receipt.pre_retirement_token_head != protocol.receipt.prior_authority.current_head_witness
        || receipt.pre_retirement_token_witness_digest != expected_token_witness
    {
        return Err(malformed(
            "StreamAuthorityRetirement receipt token-witness digest does not bind the exact selected prior authority and disposition counts"
                .to_string(),
        ));
    }
    let planned = &protocol.receipt.planned_transaction;
    if planned.read_version
        != protocol
            .receipt
            .prior_authority
            .current_head_witness
            .table_version
        || planned.uuid
            == protocol
                .receipt
                .prior_authority
                .current_head_witness
                .transaction_uuid
    {
        return Err(malformed(
            "StreamAuthorityRetirement receipt transaction must read the selected token version and use a fresh UUID"
                .to_string(),
        ));
    }
    validate_canonical_uuid_text(
        &malformed,
        "StreamAuthorityRetirement planned transaction UUID",
        &planned.uuid,
        false,
    )?;
    let expected_token_post = planned
        .read_version
        .checked_add(1)
        .ok_or_else(|| malformed("stream-token version overflow".to_string()))?;
    match (
        protocol.effect_phase,
        protocol.receipt.confirmed_transaction.as_ref(),
        protocol.receipt.confirmed_head.as_ref(),
        protocol.receipt.next_authority.as_ref(),
    ) {
        (RecoveryEffectPhase::Armed, None, None, None) => {}
        (
            RecoveryEffectPhase::EffectsConfirmed,
            Some(confirmed_transaction),
            Some(confirmed_head),
            Some(next_authority),
        ) => {
            if confirmed_transaction != planned
                || confirmed_head.branch_identifier
                    != lance::dataset::refs::BranchIdentifier::main()
                || confirmed_head.table_version != expected_token_post
                || confirmed_head.transaction_uuid != planned.uuid
                || next_authority.current_head_witness != *confirmed_head
                || next_authority.location != protocol.receipt.prior_authority.location
                || next_authority.schema_version != protocol.receipt.prior_authority.schema_version
                || next_authority.schema_hash != protocol.receipt.prior_authority.schema_hash
            {
                return Err(malformed(
                    "confirmed StreamAuthorityRetirement receipt effect differs from its exact planned N+1 transaction"
                        .to_string(),
                ));
            }
            next_authority.validate().map_err(|error| {
                malformed(format!(
                    "invalid confirmed StreamAuthorityRetirement token authority: {error}"
                ))
            })?;
        }
        _ => {
            return Err(malformed(
                "StreamAuthorityRetirement confirmation fields must be all absent while Armed and all exact while EffectsConfirmed"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetirementReceiptEffectState {
    ExactNoEffect,
    ExactEffect,
}

struct ObservedRetirementReceiptEffect {
    state: RetirementReceiptEffectState,
    transaction: Option<StagedTransactionIdentity>,
    authority: super::super::StreamTokenAuthorityEntry,
}

/// Observe the root-wide occurrence before comparing the current profile.
/// This ordering makes a lost terminal response replayable after RETIRED while
/// a same-ID/different-payload occurrence fails closed.
async fn observe_retirement_receipt_effect(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<ObservedRetirementReceiptEffect> {
    let protocol = protocol(sidecar);
    let session = crate::lance_access::control_session();
    let dataset = crate::instrumentation::open_dataset(
        &super::super::layout::stream_token_uri(root_uri),
        crate::instrumentation::VersionResolution::Latest,
        Some(&session),
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| retirement_error(sidecar, error))?;
    let authority = super::super::token_store::stream_token_authority_entry_for_dataset(&dataset)
        .await
        .map_err(|error| retirement_error(sidecar, error))?;
    let planned_receipt = &protocol.receipt.planned_receipt;
    let observed_receipt = super::super::token_store::lookup_authority_retirement_receipt(
        &dataset,
        &authority,
        &planned_receipt.graph_identity_digest,
        &planned_receipt.retirement_id,
    )
    .await
    .map_err(|error| retirement_error(sidecar, error))?;
    if observed_receipt
        .as_ref()
        .is_some_and(|receipt| receipt != planned_receipt)
    {
        return Err(retirement_error(
            sidecar,
            "the root-wide retirement occurrence exists with a different immutable receipt",
        ));
    }
    if authority == protocol.receipt.prior_authority {
        if observed_receipt.is_some() {
            return Err(retirement_error(
                sidecar,
                "the selected prior token witness already contains the retirement occurrence",
            ));
        }
        return Ok(ObservedRetirementReceiptEffect {
            state: RetirementReceiptEffectState::ExactNoEffect,
            transaction: None,
            authority,
        });
    }

    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| retirement_error(sidecar, error))?
        .ok_or_else(|| retirement_error(sidecar, "stream-token HEAD has no transaction"))?;
    let transaction = StagedTransactionIdentity::from(&transaction);
    let planned = &protocol.receipt.planned_transaction;
    let expected_version = planned
        .read_version
        .checked_add(1)
        .ok_or_else(|| retirement_error(sidecar, "stream-token version overflow"))?;
    if authority.location != protocol.receipt.prior_authority.location
        || authority.schema_version != protocol.receipt.prior_authority.schema_version
        || authority.schema_hash != protocol.receipt.prior_authority.schema_hash
        || authority.current_head_witness.table_version != expected_version
        || authority.current_head_witness.transaction_uuid != planned.uuid
        || transaction != *planned
        || observed_receipt.as_ref() != Some(planned_receipt)
    {
        return Err(retirement_error(
            sidecar,
            "raw stream-token HEAD is neither the selected prior witness nor the exact planned N+1 retirement-receipt transaction",
        ));
    }
    Ok(ObservedRetirementReceiptEffect {
        state: RetirementReceiptEffectState::ExactEffect,
        transaction: Some(transaction),
        authority,
    })
}

fn manifest_is_prior(snapshot: &Snapshot, sidecar: &RecoverySidecar) -> bool {
    let protocol = protocol(sidecar);
    snapshot.version() == protocol.prior_manifest_version
        && snapshot.stream_profile() == &protocol.prior_profile
        && snapshot.stream_token_authority() == &protocol.receipt.prior_authority
}

fn manifest_is_terminal(snapshot: &Snapshot, sidecar: &RecoverySidecar) -> bool {
    let protocol = protocol(sidecar);
    let Some(next_authority) = protocol.receipt.next_authority.as_ref() else {
        return false;
    };
    snapshot.version()
        == protocol
            .prior_manifest_version
            .checked_add(1)
            .expect("validated retirement manifest version")
        && snapshot.stream_profile() == &protocol.next_profile
        && snapshot.stream_token_authority() == next_authority
}

async fn validate_graph_authority_unchanged(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let live = read_live_recovery_authority(root_uri, storage, None)
        .await
        .map_err(|error| retirement_error(sidecar, error))?;
    if live != protocol(sidecar).authority {
        return Err(retirement_error(
            sidecar,
            "graph head or accepted schema authority changed after retirement was armed",
        ));
    }
    Ok(())
}

async fn publish_terminal(root_uri: &str, sidecar: &RecoverySidecar) -> Result<u64> {
    let protocol = protocol(sidecar);
    let next_authority = protocol
        .receipt
        .next_authority
        .as_ref()
        .expect("validated confirmed retirement authority")
        .clone();
    let changes = vec![
        ManifestChange::SetStreamTokenAuthority {
            expected: protocol.receipt.prior_authority.clone(),
            next: next_authority,
        },
        ManifestChange::SetStreamProfile {
            expected: protocol.prior_profile.clone(),
            next: protocol.next_profile.clone(),
        },
    ];
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
    let outcome = publisher
        .publish_with_precondition(&changes, &HashMap::new(), None, &precondition)
        .await
        .map_err(|error| retirement_error(sidecar, error))?;
    Ok(outcome.dataset.version().version)
}

async fn finalize_visible(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    // Intentionally no GraphCommit and no RecoveryAudit. The immutable receipt
    // selected by `next_profile.profile_receipt_chain` is the audit record.
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await
}

/// Once the immutable v19 intent exists, absence of its receipt effect is a
/// crash window to finish, not permission to abandon the operator-confirmed
/// retirement. Re-stage the same immutable receipt under the already-armed
/// transaction identity, then invoke Lance. Any racing or ambiguous attempt
/// therefore has the same classifiable identity.
async fn commit_missing_receipt_effect(root_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let current = protocol(sidecar);
    let session = crate::lance_access::control_session();
    let dataset = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &current.receipt.prior_authority,
        &session,
    )
    .await
    .map_err(|error| retirement_error(sidecar, error))?;
    let mut staged = super::super::token_store::stage_authority_retirement_receipt(
        dataset.clone(),
        &current.receipt.prior_authority,
        &current.receipt.planned_receipt,
    )
    .await
    .map_err(|error| retirement_error(sidecar, error))?;
    let planned = &current.receipt.planned_transaction;
    staged
        .bind_transaction_identity(planned)
        .map_err(|error| retirement_error(sidecar, error))?;

    let table_store = crate::table_store::TableStore::new(root_uri, session);
    let (achieved, committed) = table_store
        .commit_staged_exact(Arc::new(dataset), staged)
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                sidecar.operation_id.clone(),
                format!(
                    "re-staged authority-retirement receipt commit is ambiguous and requires recovery: {error}"
                ),
            )
        })?;
    let expected_version = planned
        .read_version
        .checked_add(1)
        .ok_or_else(|| retirement_error(sidecar, "stream-token version overflow"))?;
    if &committed != planned || achieved.version().version != expected_version {
        return Err(retirement_error(
            sidecar,
            "re-staged receipt committed with a non-exact transaction identity or table version",
        ));
    }
    Ok(())
}

/// Result of driving one v19 intent to an exact terminal disposition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryStreamAuthorityRetirementOutcomeV19 {
    TerminalVisible {
        receipt: super::super::AuthorityRetirementReceipt,
        token_authority: super::super::StreamTokenAuthorityEntry,
        profile: super::super::StreamProfileEntry,
        manifest_version: u64,
    },
}

async fn process_typed(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamAuthorityRetirementOutcomeV19> {
    validate_sidecar_shape(&sidecar_uri(root_uri, &sidecar.operation_id), sidecar)?;

    // Occurrence lookup is deliberately first. Current profile checks happen
    // only after the exact immutable receipt (or its absence) is known.
    let observed = observe_retirement_receipt_effect(root_uri, sidecar).await?;
    validate_graph_authority_unchanged(root_uri, storage, sidecar).await?;

    if manifest_is_terminal(snapshot, sidecar) {
        let protocol = protocol(sidecar);
        if protocol.effect_phase != RecoveryEffectPhase::EffectsConfirmed
            || observed.state != RetirementReceiptEffectState::ExactEffect
            || observed.transaction.as_ref() != protocol.receipt.confirmed_transaction.as_ref()
            || Some(&observed.authority) != protocol.receipt.next_authority.as_ref()
        {
            return Err(retirement_error(
                sidecar,
                "visible RETIRED profile is not backed by the exact confirmed receipt effect",
            ));
        }
        let outcome = RecoveryStreamAuthorityRetirementOutcomeV19::TerminalVisible {
            receipt: protocol.receipt.planned_receipt.clone(),
            token_authority: observed.authority,
            profile: protocol.next_profile.clone(),
            manifest_version: snapshot.version(),
        };
        finalize_visible(root_uri, storage.as_ref(), sidecar).await?;
        return Ok(outcome);
    }
    if !manifest_is_prior(snapshot, sidecar) {
        return Err(retirement_error(
            sidecar,
            "manifest version, profile, or selected token authority differs from both exact retirement outcomes",
        ));
    }
    if observed.state == RetirementReceiptEffectState::ExactNoEffect {
        if protocol(sidecar).effect_phase != RecoveryEffectPhase::Armed {
            return Err(retirement_error(
                sidecar,
                "EffectsConfirmed sidecar has no retirement-receipt effect",
            ));
        }
        commit_missing_receipt_effect(root_uri, sidecar).await?;
        return Box::pin(process_typed(root_uri, storage, snapshot, sidecar)).await;
    }

    let mut confirmed = sidecar.clone();
    if protocol(sidecar).effect_phase == RecoveryEffectPhase::Armed {
        confirm_stream_authority_retirement_sidecar_v19(
            root_uri,
            storage.as_ref(),
            &mut confirmed,
            observed
                .transaction
                .clone()
                .expect("exact retirement receipt effect has a transaction"),
            observed.authority.current_head_witness.clone(),
            observed.authority.clone(),
        )
        .await
        .map_err(|error| retirement_error(sidecar, error))?;
    }
    let confirmed_protocol = protocol(&confirmed);
    if confirmed_protocol.effect_phase != RecoveryEffectPhase::EffectsConfirmed
        || observed.transaction.as_ref()
            != confirmed_protocol.receipt.confirmed_transaction.as_ref()
        || Some(&observed.authority) != confirmed_protocol.receipt.next_authority.as_ref()
    {
        return Err(retirement_error(
            &confirmed,
            "retirement receipt did not converge to its exact durable confirmation",
        ));
    }

    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH)?;
    let publish = publish_terminal(root_uri, &confirmed).await;
    let fresh = fresh_snapshot_for_sidecar(root_uri, storage, &confirmed).await?;
    if !manifest_is_terminal(&fresh, &confirmed) {
        return match publish {
            Ok(version) => Err(retirement_error(
                &confirmed,
                format!(
                    "terminal manifest publication returned version {version} without selecting the exact RETIRED profile and receipt-bearing token pointer"
                ),
            )),
            Err(error) => Err(error),
        };
    }
    validate_graph_authority_unchanged(root_uri, storage, &confirmed).await?;
    let expected_version = confirmed_protocol
        .prior_manifest_version
        .checked_add(1)
        .ok_or_else(|| retirement_error(&confirmed, "manifest version overflow"))?;
    if let Ok(version) = publish
        && version != expected_version
    {
        return Err(retirement_error(
            &confirmed,
            format!(
                "terminal retirement publication reached manifest version {version}, expected exact version {expected_version}"
            ),
        ));
    }
    let outcome = RecoveryStreamAuthorityRetirementOutcomeV19::TerminalVisible {
        receipt: confirmed_protocol.receipt.planned_receipt.clone(),
        token_authority: confirmed_protocol
            .receipt
            .next_authority
            .as_ref()
            .expect("confirmed retirement token authority")
            .clone(),
        profile: confirmed_protocol.next_profile.clone(),
        manifest_version: fresh.version(),
    };
    finalize_visible(root_uri, storage.as_ref(), &confirmed).await?;
    Ok(outcome)
}

pub(super) async fn process_stream_authority_retirement_sidecar_v19(
    root_uri: &str,
    storage: &Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<bool> {
    process_typed(root_uri, storage, snapshot, sidecar)
        .await
        .map(|_| true)
}

/// Arm one exact retirement receipt transaction before its token-table effect.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_stream_authority_retirement_sidecar_v19(
    actor_id: String,
    authority: RecoveryAuthorityToken,
    prior_manifest_version: u64,
    prior_profile: super::super::StreamProfileEntry,
    next_profile: super::super::StreamProfileEntry,
    prior_token_authority: super::super::StreamTokenAuthorityEntry,
    planned_receipt: super::super::AuthorityRetirementReceipt,
    planned_transaction: StagedTransactionIdentity,
) -> Result<RecoverySidecar> {
    let mut sidecar = new_unvalidated_sidecar(
        STREAM_AUTHORITY_RETIREMENT_SIDECAR_SCHEMA_VERSION,
        SidecarKind::StreamAuthorityRetirement,
        None,
        Some(actor_id),
        Vec::new(),
    );
    sidecar.protocol_v19 = Some(Box::new(RecoveryProtocolV19::StreamAuthorityRetirement(
        RecoveryStreamAuthorityRetirementV19 {
            authority,
            prior_manifest_version,
            effect_phase: RecoveryEffectPhase::Armed,
            prior_profile,
            next_profile,
            receipt: RecoveryStreamAuthorityRetirementReceiptEffectV19 {
                prior_authority: prior_token_authority,
                planned_receipt,
                planned_transaction,
                confirmed_transaction: None,
                confirmed_head: None,
                next_authority: None,
            },
        },
    )));
    validate_sidecar_shape("<new-stream-authority-retirement-sidecar-v19>", &sidecar)?;
    Ok(sidecar)
}

/// Bind the exact achieved N+1 receipt effect before the sole RETIRED CAS.
pub(crate) async fn confirm_stream_authority_retirement_sidecar_v19(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &mut RecoverySidecar,
    committed_transaction: StagedTransactionIdentity,
    achieved_head: super::super::CurrentHeadWitness,
    next_authority: super::super::StreamTokenAuthorityEntry,
) -> Result<()> {
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_CONFIRM)?;
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    validate_sidecar_shape(&uri, sidecar)?;
    let current = protocol(sidecar);
    if current.effect_phase != RecoveryEffectPhase::Armed
        || committed_transaction != current.receipt.planned_transaction
        || achieved_head.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || achieved_head.table_version
            != committed_transaction
                .read_version
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("stream-token version overflow"))?
        || achieved_head.transaction_uuid != committed_transaction.uuid
        || next_authority.current_head_witness != achieved_head
        || next_authority.location != current.receipt.prior_authority.location
        || next_authority.schema_version != current.receipt.prior_authority.schema_version
        || next_authority.schema_hash != current.receipt.prior_authority.schema_hash
    {
        return Err(OmniError::manifest_internal(format!(
            "StreamAuthorityRetirement sidecar '{}' confirmation differs from its exact planned N+1 receipt effect",
            sidecar.operation_id
        )));
    }
    let mut confirmed = sidecar.clone();
    let confirmed_protocol = protocol_mut(&mut confirmed);
    confirmed_protocol.effect_phase = RecoveryEffectPhase::EffectsConfirmed;
    confirmed_protocol.receipt.confirmed_transaction = Some(committed_transaction);
    confirmed_protocol.receipt.confirmed_head = Some(achieved_head);
    confirmed_protocol.receipt.next_authority = Some(next_authority);
    validate_sidecar_shape(&uri, &confirmed)?;
    let json = serde_json::to_string_pretty(&confirmed).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize confirmed StreamAuthorityRetirement sidecar: {error}"
        ))
    })?;
    storage.write_text(&uri, &json).await?;
    *sidecar = confirmed;
    Ok(())
}

/// Drive a newly armed retirement after the caller commits its exact receipt
/// transaction. Generic recovery uses the same state machine.
pub(crate) async fn complete_stream_authority_retirement_sidecar_v19(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamAuthorityRetirementOutcomeV19> {
    process_typed(root_uri, &storage, snapshot, sidecar).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn armed_sidecar() -> RecoverySidecar {
        let authority = RecoveryAuthorityToken {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            graph_head: Some("01H000000000000000000000R9".to_string()),
            schema_identity_domain: "retirement-test-domain".to_string(),
            schema_ir_hash: digest('1'),
            schema_identity_version: 1,
        };
        let graph_identity_digest = super::super::super::stream::stream_graph_identity_digest(
            &authority.schema_identity_domain,
        )
        .unwrap();
        let prior_profile = super::super::super::StreamProfileEntry::genesis();
        let prior_token_authority = super::super::super::StreamTokenAuthorityEntry {
            location: super::super::super::token_store::STREAM_TOKEN_DATASET_PATH.to_string(),
            schema_version: super::super::super::token_store::STREAM_TOKEN_AUTHORITY_SCHEMA_VERSION,
            schema_hash: super::super::super::token_store::stream_token_schema_hash(),
            current_head_witness: super::super::super::CurrentHeadWitness {
                branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
                table_version: 11,
                transaction_uuid: "18181818-1818-4818-8818-181818181818".to_string(),
                manifest_e_tag: None,
            },
        };
        let token_witness = super::super::super::stream_authority_retirement_token_witness_digest(
            &prior_token_authority.current_head_witness,
            9,
            1,
        )
        .unwrap();
        let receipt = super::super::super::AuthorityRetirementReceipt::new(
            graph_identity_digest,
            &prior_profile.profile_receipt_chain,
            "19191919-1919-4919-8919-191919191919",
            digest('2'),
            "operator:retirement-test",
            super::super::super::INTERNAL_MANIFEST_SCHEMA_VERSION,
            7,
            digest('3'),
            prior_profile.profile_revision,
            digest('4'),
            prior_token_authority.current_head_witness.clone(),
            token_witness,
            9,
            1,
            digest('6'),
            1_700_000_000_000_000,
        )
        .unwrap();
        let next_profile = super::super::super::StreamProfileEntry::retired_from_disabled(
            &prior_profile,
            receipt.next_chain_ref().unwrap(),
            receipt.retirement_id.clone(),
            receipt.record_id.clone(),
            receipt.export_cut_digest.clone(),
        )
        .unwrap();
        new_stream_authority_retirement_sidecar_v19(
            "operator:retirement-test".to_string(),
            authority,
            7,
            prior_profile,
            next_profile,
            prior_token_authority,
            receipt,
            StagedTransactionIdentity {
                read_version: 11,
                uuid: "17171717-1717-4717-8717-171717171717".to_string(),
            },
        )
        .unwrap()
    }

    #[test]
    fn v19_pins_closed_lineage_neutral_grammar_and_exclusive_profile_gate() {
        let armed = armed_sidecar();
        assert_eq!(
            armed.schema_version,
            STREAM_AUTHORITY_RETIREMENT_SIDECAR_SCHEMA_VERSION
        );
        assert_eq!(armed.writer_kind, SidecarKind::StreamAuthorityRetirement);
        assert!(sidecar_requires_exclusive_profile_gate(armed.writer_kind));
        let encoded = serde_json::to_string(&armed).unwrap();
        assert!(
            !encoded.contains("lineage"),
            "retirement sidecars must not acquire ordinary graph lineage"
        );
        let decoded = parse_sidecar("<stream-retirement-v19-armed>", &encoded).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);

        let mut wrong_schema = armed.clone();
        wrong_schema.schema_version = STREAM_REBIND_SIDECAR_SCHEMA_VERSION;
        assert!(
            validate_sidecar_shape("<v19-overlay-on-v18>", &wrong_schema)
                .unwrap_err()
                .to_string()
                .contains("protocol_v19 requires schema-v19")
        );

        let mut wrong_writer = armed.clone();
        wrong_writer.writer_kind = SidecarKind::StreamProfileChange;
        assert!(
            validate_sidecar_shape("<v19-wrong-writer>", &wrong_writer)
                .unwrap_err()
                .to_string()
                .contains("schema-v19 StreamAuthorityRetirement must target canonical main")
        );
    }

    #[test]
    fn v19_confirmation_is_all_or_nothing_and_exact_n_plus_one() {
        let armed = armed_sidecar();
        let mut partial = armed.clone();
        protocol_mut(&mut partial).effect_phase = RecoveryEffectPhase::EffectsConfirmed;
        assert!(
            validate_sidecar_shape("<v19-partial-confirmation>", &partial)
                .unwrap_err()
                .to_string()
                .contains("confirmation fields must be all absent while Armed and all exact")
        );

        let mut confirmed = armed;
        let planned = protocol(&confirmed).receipt.planned_transaction.clone();
        let prior = protocol(&confirmed).receipt.prior_authority.clone();
        let achieved_head = super::super::super::CurrentHeadWitness {
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
            table_version: planned.read_version + 1,
            transaction_uuid: planned.uuid.clone(),
            manifest_e_tag: None,
        };
        let next_authority = super::super::super::StreamTokenAuthorityEntry {
            current_head_witness: achieved_head.clone(),
            ..prior
        };
        let protocol = protocol_mut(&mut confirmed);
        protocol.effect_phase = RecoveryEffectPhase::EffectsConfirmed;
        protocol.receipt.confirmed_transaction = Some(planned);
        protocol.receipt.confirmed_head = Some(achieved_head);
        protocol.receipt.next_authority = Some(next_authority);
        validate_sidecar_shape("<v19-exact-confirmation>", &confirmed).unwrap();

        protocol_mut(&mut confirmed)
            .receipt
            .confirmed_head
            .as_mut()
            .unwrap()
            .table_version += 1;
        assert!(
            validate_sidecar_shape("<v19-non-exact-confirmation>", &confirmed)
                .unwrap_err()
                .to_string()
                .contains("differs from its exact planned N+1 transaction")
        );
    }

    #[test]
    fn v19_rejects_zero_withdrawn_and_mismatched_retired_cut() {
        let armed = armed_sidecar();
        let mut zero_withdrawn = armed.clone();
        protocol_mut(&mut zero_withdrawn)
            .receipt
            .planned_receipt
            .withdrawn_token_count = 0;
        assert!(
            validate_sidecar_shape("<v19-zero-withdrawn>", &zero_withdrawn)
                .unwrap_err()
                .to_string()
                .contains("at least one current WITHDRAWN token")
        );

        let mut wrong_cut = armed;
        let super::super::super::StreamProfileState::Retired {
            authority_retirement_cut_digest,
            ..
        } = &mut protocol_mut(&mut wrong_cut).next_profile.state
        else {
            unreachable!();
        };
        *authority_retirement_cut_digest = digest('9');
        assert!(
            validate_sidecar_shape("<v19-wrong-cut>", &wrong_cut)
                .unwrap_err()
                .to_string()
                .contains("receipt does not bind the exact graph")
        );
    }

    #[tokio::test]
    async fn v19_armed_without_receipt_restages_exact_identity_and_rolls_forward() {
        const SCHEMA: &str = r#"
node Person {
    name: String
}
"#;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        let db = crate::db::Omnigraph::init(&root, SCHEMA).await.unwrap();
        let snapshot = db
            .snapshot_of(crate::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        let storage = crate::storage::storage_for_uri(&root).unwrap();
        let authority = read_live_recovery_authority(&root, &storage, None)
            .await
            .unwrap();
        let prior_profile = snapshot.stream_profile().clone();
        let graph_identity_digest = super::super::super::stream::stream_graph_identity_digest(
            &authority.schema_identity_domain,
        )
        .unwrap();
        let prior_token_authority = snapshot.stream_token_authority().clone();
        let (live_branch_heads_digest, export_cut_digest) =
            db.capture_retirement_logical_cut().await.unwrap();
        let token_witness = super::super::super::stream_authority_retirement_token_witness_digest(
            &prior_token_authority.current_head_witness,
            2,
            1,
        )
        .unwrap();
        let receipt = super::super::super::AuthorityRetirementReceipt::new(
            graph_identity_digest,
            &prior_profile.profile_receipt_chain,
            "29292929-2929-4929-8929-292929292929",
            digest('a'),
            "operator:retirement-e2e",
            super::super::super::INTERNAL_MANIFEST_SCHEMA_VERSION,
            snapshot.version(),
            live_branch_heads_digest,
            prior_profile.profile_revision,
            digest('c'),
            prior_token_authority.current_head_witness.clone(),
            token_witness,
            2,
            1,
            export_cut_digest,
            1_700_000_000_000_100,
        )
        .unwrap();
        let next_profile = super::super::super::StreamProfileEntry::retired_from_disabled(
            &prior_profile,
            receipt.next_chain_ref().unwrap(),
            receipt.retirement_id.clone(),
            receipt.record_id.clone(),
            receipt.export_cut_digest.clone(),
        )
        .unwrap();
        let token_dataset = snapshot.open_stream_token_authority().await.unwrap();
        let staged = super::super::super::token_store::stage_authority_retirement_receipt(
            token_dataset.clone(),
            &prior_token_authority,
            &receipt,
        )
        .await
        .unwrap();
        let planned_transaction = staged.transaction_identity();
        let sidecar = new_stream_authority_retirement_sidecar_v19(
            "operator:retirement-e2e".to_string(),
            authority.clone(),
            snapshot.version(),
            prior_profile,
            next_profile.clone(),
            prior_token_authority,
            receipt.clone(),
            planned_transaction.clone(),
        )
        .unwrap();
        write_sidecar(&root, storage.as_ref(), &sidecar)
            .await
            .unwrap();

        let (lineage_before, _) = ManifestCoordinator::read_graph_lineage_at(&root, None)
            .await
            .unwrap();
        let audit_before = crate::db::recovery_audit::RecoveryAudit::open(&root)
            .await
            .unwrap()
            .list()
            .await
            .unwrap();
        drop(staged);

        let outcome = complete_stream_authority_retirement_sidecar_v19(
            &root,
            Arc::clone(&storage),
            &snapshot,
            &sidecar,
        )
        .await
        .unwrap();
        let RecoveryStreamAuthorityRetirementOutcomeV19::TerminalVisible {
            receipt: observed_receipt,
            token_authority,
            profile,
            manifest_version,
        } = outcome;
        assert_eq!(observed_receipt, receipt);
        assert_eq!(
            token_authority.current_head_witness.transaction_uuid, planned_transaction.uuid,
            "recovery must preserve the exact transaction identity armed in the sidecar"
        );
        assert_eq!(profile, next_profile);
        assert_eq!(manifest_version, snapshot.version() + 1);

        let fresh = fresh_snapshot_for_sidecar(&root, &storage, &sidecar)
            .await
            .unwrap();
        assert_eq!(fresh.stream_profile(), &next_profile);
        assert_eq!(fresh.version(), snapshot.version() + 1);
        assert_eq!(
            read_live_recovery_authority(&root, &storage, None)
                .await
                .unwrap(),
            authority,
            "retirement must not move the graph head or accepted schema"
        );
        let (lineage_after, _) = ManifestCoordinator::read_graph_lineage_at(&root, None)
            .await
            .unwrap();
        assert_eq!(
            lineage_after, lineage_before,
            "retirement is lineage-neutral"
        );
        let audit_after = crate::db::recovery_audit::RecoveryAudit::open(&root)
            .await
            .unwrap()
            .list()
            .await
            .unwrap();
        assert_eq!(
            audit_after, audit_before,
            "retirement receipt/profile chain replaces RecoveryAudit"
        );
        assert!(
            list_sidecars(&root, storage.as_ref())
                .await
                .unwrap()
                .is_empty(),
            "terminal finalization must remove the recovery sidecar"
        );

        let retired = crate::db::Omnigraph::open(&root).await.unwrap();
        assert_eq!(
            retired.stream_status().await.unwrap().profile_mode,
            "RETIRED"
        );
        let export = retired.export_jsonl("main", &[], &[]).await.unwrap();
        let provenance: serde_json::Value =
            serde_json::from_str(export.lines().next().unwrap()).unwrap();
        assert_eq!(
            provenance["_omnigraph_export_provenance"]["receipt"]["record_id"],
            receipt.record_id
        );
        let error = retired.optimize().await.unwrap_err();
        assert!(matches!(
            error,
            OmniError::StreamAuthorityRetired {
                retirement_id,
                export_cut_digest,
            } if retirement_id == receipt.retirement_id
                && export_cut_digest == receipt.export_cut_digest
        ));
    }
}
