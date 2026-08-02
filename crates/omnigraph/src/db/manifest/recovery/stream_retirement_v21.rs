//! Recovery-v21 three-disposition stream-authority retirement.
//!
//! This is the v19 receipt-first state machine with a distinct immutable v2
//! receipt codec that binds PRESENT, WITHDRAWN, and DEAD_LETTERED counts. The
//! historical v19 wire and processor remain frozen in their own module.

use super::*;

pub(super) fn validate_stream_authority_retirement_v21<F>(
    malformed: &F,
    sidecar: &RecoverySidecar,
    protocol: &RecoveryStreamAuthorityRetirementV21,
) -> Result<()>
where
    F: Fn(String) -> OmniError,
{
    if sidecar.writer_kind != SidecarKind::StreamAuthorityRetirement
        || sidecar.branch.is_some()
        || !sidecar.tables.is_empty()
        || sidecar.actor_id.is_none()
    {
        return Err(malformed(
            "StreamAuthorityRetirementV2 must target canonical main with no table pins and an authenticated actor"
                .to_string(),
        ));
    }
    validate_authority_identity(malformed, &protocol.authority)?;
    if protocol.authority.branch_identifier != lance::dataset::refs::BranchIdentifier::main()
        || protocol.prior_manifest_version == 0
    {
        return Err(malformed(
            "StreamAuthorityRetirementV2 requires positive canonical-main manifest authority"
                .to_string(),
        ));
    }
    protocol
        .prior_profile
        .validate()
        .map_err(|error| malformed(format!("invalid prior retirement-v2 profile: {error}")))?;
    if protocol.prior_profile.mode() != super::super::StreamProfileMode::Disabled {
        return Err(malformed(
            "StreamAuthorityRetirementV2 prior profile must be DISABLED".to_string(),
        ));
    }
    protocol
        .next_profile
        .validate_transition_from(&protocol.prior_profile)
        .map_err(|error| malformed(format!("invalid next retirement-v2 profile: {error}")))?;
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
            "StreamAuthorityRetirementV2 next profile must be RETIRED".to_string(),
        ));
    };
    receipt
        .validate()
        .map_err(|error| malformed(format!("invalid authority-retirement-v2 receipt: {error}")))?;
    let graph_identity_digest = super::super::stream::stream_graph_identity_digest(
        &protocol.authority.schema_identity_domain,
    )
    .map_err(|error| malformed(format!("invalid graph identity: {error}")))?;
    let expected_chain_ordinal = protocol
        .prior_profile
        .profile_receipt_chain
        .record_count
        .checked_add(1)
        .ok_or_else(|| malformed("retirement-v2 receipt-chain overflow".to_string()))?;
    if receipt.graph_identity_digest != graph_identity_digest
        || Some(receipt.actor.as_str()) != sidecar.actor_id.as_deref()
        || receipt.source_internal_schema_version != super::super::INTERNAL_MANIFEST_SCHEMA_VERSION
        || receipt.source_manifest_version != protocol.prior_manifest_version
        || receipt.source_profile_revision != protocol.prior_profile.profile_revision
        || (receipt.withdrawn_token_count == 0 && receipt.dead_lettered_token_count == 0)
        || receipt.chain_ordinal != expected_chain_ordinal
        || receipt.predecessor_record_id
            != protocol.prior_profile.profile_receipt_chain.head_record_id
        || receipt.prior_chain_digest != protocol.prior_profile.profile_receipt_chain.chain_digest
        || authority_retirement_id != &receipt.retirement_id
        || authority_retirement_receipt_id != &receipt.record_id
        || authority_retirement_cut_digest != &receipt.export_cut_digest
        || receipt
            .next_chain_ref()
            .map_err(|error| malformed(format!("invalid retirement-v2 chain: {error}")))?
            != protocol.next_profile.profile_receipt_chain
    {
        return Err(malformed(
            "StreamAuthorityRetirementV2 receipt does not bind the exact graph, actor, source cut, three disposition counts, and RETIRED profile"
                .to_string(),
        ));
    }
    protocol
        .receipt
        .prior_authority
        .validate()
        .map_err(|error| malformed(format!("invalid prior retirement token authority: {error}")))?;
    if receipt.pre_retirement_token_head != protocol.receipt.prior_authority.current_head_witness {
        return Err(malformed(
            "retirement-v2 receipt does not select its exact prior token authority".to_string(),
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
            "retirement-v2 transaction must read selected token authority and use a fresh UUID"
                .to_string(),
        ));
    }
    validate_canonical_uuid_text(
        malformed,
        "StreamAuthorityRetirementV2 transaction UUID",
        &planned.uuid,
        false,
    )?;
    let expected_post = planned
        .read_version
        .checked_add(1)
        .ok_or_else(|| malformed("retirement-v2 token version overflow".to_string()))?;
    match (
        protocol.effect_phase,
        protocol.receipt.confirmed_transaction.as_ref(),
        protocol.receipt.confirmed_head.as_ref(),
        protocol.receipt.next_authority.as_ref(),
    ) {
        (RecoveryEffectPhase::Armed, None, None, None) => {}
        (
            RecoveryEffectPhase::EffectsConfirmed,
            Some(transaction),
            Some(head),
            Some(next_authority),
        ) if transaction == planned
            && head.branch_identifier == lance::dataset::refs::BranchIdentifier::main()
            && head.table_version == expected_post
            && head.transaction_uuid == planned.uuid
            && next_authority.current_head_witness == *head
            && next_authority.location == protocol.receipt.prior_authority.location
            && next_authority.schema_version == protocol.receipt.prior_authority.schema_version
            && next_authority.schema_hash == protocol.receipt.prior_authority.schema_hash => {}
        _ => {
            return Err(malformed(
                "retirement-v2 confirmation fields must be absent while Armed or exact while EffectsConfirmed"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn stream_authority_retirement_protocol_v21(
    sidecar: &RecoverySidecar,
) -> &RecoveryStreamAuthorityRetirementV21 {
    let RecoveryProtocolV21::StreamAuthorityRetirementV2(protocol) = sidecar
        .protocol_v21
        .as_deref()
        .expect("validated schema-v21 StreamAuthorityRetirementV2")
    else {
        panic!("validated schema-v21 sidecar is not StreamAuthorityRetirementV2")
    };
    protocol
}

fn stream_authority_retirement_protocol_v21_mut(
    sidecar: &mut RecoverySidecar,
) -> &mut RecoveryStreamAuthorityRetirementV21 {
    let RecoveryProtocolV21::StreamAuthorityRetirementV2(protocol) = sidecar
        .protocol_v21
        .as_deref_mut()
        .expect("validated schema-v21 StreamAuthorityRetirementV2")
    else {
        panic!("validated schema-v21 sidecar is not StreamAuthorityRetirementV2")
    };
    protocol
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn new_stream_authority_retirement_sidecar_v21(
    actor_id: String,
    authority: RecoveryAuthorityToken,
    prior_manifest_version: u64,
    prior_profile: super::super::StreamProfileEntry,
    next_profile: super::super::StreamProfileEntry,
    prior_token_authority: super::super::StreamTokenAuthorityEntry,
    planned_receipt: super::super::stream_token::AuthorityRetirementReceiptV2,
    planned_transaction: StagedTransactionIdentity,
) -> Result<RecoverySidecar> {
    let mut sidecar = new_unvalidated_sidecar(
        STREAM_DEAD_LETTER_SIDECAR_SCHEMA_VERSION,
        SidecarKind::StreamAuthorityRetirement,
        None,
        Some(actor_id),
        Vec::new(),
    );
    sidecar.protocol_v21 = Some(Box::new(RecoveryProtocolV21::StreamAuthorityRetirementV2(
        RecoveryStreamAuthorityRetirementV21 {
            authority,
            prior_manifest_version,
            effect_phase: RecoveryEffectPhase::Armed,
            prior_profile,
            next_profile,
            receipt: RecoveryStreamAuthorityRetirementReceiptEffectV21 {
                prior_authority: prior_token_authority,
                planned_receipt,
                planned_transaction,
                confirmed_transaction: None,
                confirmed_head: None,
                next_authority: None,
            },
        },
    )));
    validate_sidecar_shape("<new-stream-authority-retirement-sidecar-v21>", &sidecar)?;
    Ok(sidecar)
}

pub(crate) async fn confirm_stream_authority_retirement_sidecar_v21(
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
    let current = stream_authority_retirement_protocol_v21(sidecar);
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
        return Err(OmniError::manifest_internal(
            "StreamAuthorityRetirementV2 confirmation differs from its exact N+1 receipt effect",
        ));
    }
    let mut confirmed = sidecar.clone();
    let protocol = stream_authority_retirement_protocol_v21_mut(&mut confirmed);
    protocol.effect_phase = RecoveryEffectPhase::EffectsConfirmed;
    protocol.receipt.confirmed_transaction = Some(committed_transaction);
    protocol.receipt.confirmed_head = Some(achieved_head);
    protocol.receipt.next_authority = Some(next_authority);
    validate_sidecar_shape(&uri, &confirmed)?;
    let json = serde_json::to_string_pretty(&confirmed).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize confirmed StreamAuthorityRetirementV2 sidecar: {error}"
        ))
    })?;
    storage.write_text(&uri, &json).await?;
    *sidecar = confirmed;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamAuthorityRetirementEffectStateV21 {
    ExactNoEffect,
    ExactEffect,
}

struct ObservedStreamAuthorityRetirementEffectV21 {
    state: StreamAuthorityRetirementEffectStateV21,
    transaction: Option<StagedTransactionIdentity>,
    authority: super::super::StreamTokenAuthorityEntry,
}

fn stream_authority_retirement_error_v21(
    sidecar: &RecoverySidecar,
    reason: impl std::fmt::Display,
) -> OmniError {
    OmniError::recovery_required(
        sidecar.operation_id.clone(),
        format!("StreamAuthorityRetirementV2 recovery cannot prove an exact outcome: {reason}"),
    )
}

async fn observe_stream_authority_retirement_effect_v21(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<ObservedStreamAuthorityRetirementEffectV21> {
    let protocol = stream_authority_retirement_protocol_v21(sidecar);
    let session = crate::lance_access::control_session();
    let dataset = crate::instrumentation::open_dataset(
        &super::super::layout::stream_token_uri(root_uri),
        crate::instrumentation::VersionResolution::Latest,
        Some(&session),
        crate::instrumentation::table_wrapper(),
    )
    .await
    .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    let authority = super::super::token_store::stream_token_authority_entry_for_dataset(&dataset)
        .await
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    let planned_receipt = &protocol.receipt.planned_receipt;
    let observed_receipt = super::super::token_store::lookup_authority_retirement_receipt_v2(
        &dataset,
        &authority,
        &planned_receipt.graph_identity_digest,
        &planned_receipt.retirement_id,
    )
    .await
    .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    if observed_receipt
        .as_ref()
        .is_some_and(|receipt| receipt != planned_receipt)
    {
        return Err(stream_authority_retirement_error_v21(
            sidecar,
            "the retirement-v2 occurrence exists with a different immutable receipt",
        ));
    }
    if authority == protocol.receipt.prior_authority {
        if observed_receipt.is_some() {
            return Err(stream_authority_retirement_error_v21(
                sidecar,
                "the selected prior token witness already contains the retirement-v2 occurrence",
            ));
        }
        return Ok(ObservedStreamAuthorityRetirementEffectV21 {
            state: StreamAuthorityRetirementEffectStateV21::ExactNoEffect,
            transaction: None,
            authority,
        });
    }
    let transaction = dataset
        .read_transaction()
        .await
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?
        .ok_or_else(|| {
            stream_authority_retirement_error_v21(sidecar, "stream-token HEAD has no transaction")
        })?;
    let transaction = StagedTransactionIdentity::from(&transaction);
    let planned = &protocol.receipt.planned_transaction;
    let expected_version = planned.read_version.checked_add(1).ok_or_else(|| {
        stream_authority_retirement_error_v21(sidecar, "stream-token version overflow")
    })?;
    if authority.location != protocol.receipt.prior_authority.location
        || authority.schema_version != protocol.receipt.prior_authority.schema_version
        || authority.schema_hash != protocol.receipt.prior_authority.schema_hash
        || authority.current_head_witness.table_version != expected_version
        || authority.current_head_witness.transaction_uuid != planned.uuid
        || transaction != *planned
        || observed_receipt.as_ref() != Some(planned_receipt)
    {
        return Err(stream_authority_retirement_error_v21(
            sidecar,
            "raw stream-token HEAD is neither selected N nor the exact retirement-v2 N+1 receipt transaction",
        ));
    }
    Ok(ObservedStreamAuthorityRetirementEffectV21 {
        state: StreamAuthorityRetirementEffectStateV21::ExactEffect,
        transaction: Some(transaction),
        authority,
    })
}

fn stream_authority_retirement_manifest_is_prior_v21(
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> bool {
    let protocol = stream_authority_retirement_protocol_v21(sidecar);
    snapshot.version() == protocol.prior_manifest_version
        && snapshot.stream_profile() == &protocol.prior_profile
        && snapshot.stream_token_authority() == &protocol.receipt.prior_authority
}

fn stream_authority_retirement_manifest_is_terminal_v21(
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> bool {
    let protocol = stream_authority_retirement_protocol_v21(sidecar);
    let Some(next_authority) = protocol.receipt.next_authority.as_ref() else {
        return false;
    };
    snapshot.version() == protocol.prior_manifest_version + 1
        && snapshot.stream_profile() == &protocol.next_profile
        && snapshot.stream_token_authority() == next_authority
}

async fn validate_stream_authority_retirement_graph_authority_v21(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let live = read_live_recovery_authority(root_uri, storage, None)
        .await
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    if live != stream_authority_retirement_protocol_v21(sidecar).authority {
        return Err(stream_authority_retirement_error_v21(
            sidecar,
            "graph head or accepted schema authority changed after retirement-v2 was armed",
        ));
    }
    Ok(())
}

async fn publish_stream_authority_retirement_terminal_v21(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<u64> {
    let protocol = stream_authority_retirement_protocol_v21(sidecar);
    let changes = vec![
        ManifestChange::SetStreamTokenAuthority {
            expected: protocol.receipt.prior_authority.clone(),
            next: protocol
                .receipt
                .next_authority
                .as_ref()
                .expect("validated confirmed retirement-v2 authority")
                .clone(),
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
    publisher
        .publish_with_precondition(&changes, &HashMap::new(), None, &precondition)
        .await
        .map(|outcome| outcome.dataset.version().version)
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))
}

async fn commit_missing_stream_authority_retirement_receipt_v21(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let protocol = stream_authority_retirement_protocol_v21(sidecar);
    let session = crate::lance_access::control_session();
    let dataset = super::super::token_store::open_stream_token_authority_at(
        root_uri,
        &protocol.receipt.prior_authority,
        &session,
    )
    .await
    .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    let mut staged = super::super::token_store::stage_authority_retirement_receipt_v2(
        dataset.clone(),
        &protocol.receipt.prior_authority,
        &protocol.receipt.planned_receipt,
    )
    .await
    .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    staged
        .bind_transaction_identity(&protocol.receipt.planned_transaction)
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    let table_store = crate::table_store::TableStore::new(root_uri, session);
    let (achieved, committed) = table_store
        .commit_staged_exact(std::sync::Arc::new(dataset), staged)
        .await
        .map_err(|error| stream_authority_retirement_error_v21(sidecar, error))?;
    if committed != protocol.receipt.planned_transaction
        || achieved.version().version != protocol.receipt.planned_transaction.read_version + 1
    {
        return Err(stream_authority_retirement_error_v21(
            sidecar,
            "re-staged retirement-v2 receipt committed with a non-exact identity or version",
        ));
    }
    Ok(())
}

async fn process_stream_authority_retirement_sidecar_v21_typed(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamAuthorityRetirementOutcomeV21> {
    validate_sidecar_shape(&sidecar_uri(root_uri, &sidecar.operation_id), sidecar)?;
    let observed = observe_stream_authority_retirement_effect_v21(root_uri, sidecar).await?;
    validate_stream_authority_retirement_graph_authority_v21(root_uri, storage, sidecar).await?;
    if stream_authority_retirement_manifest_is_terminal_v21(snapshot, sidecar) {
        let protocol = stream_authority_retirement_protocol_v21(sidecar);
        if protocol.effect_phase != RecoveryEffectPhase::EffectsConfirmed
            || observed.state != StreamAuthorityRetirementEffectStateV21::ExactEffect
            || observed.transaction.as_ref() != protocol.receipt.confirmed_transaction.as_ref()
            || Some(&observed.authority) != protocol.receipt.next_authority.as_ref()
        {
            return Err(stream_authority_retirement_error_v21(
                sidecar,
                "visible RETIRED profile is not backed by the exact retirement-v2 receipt",
            ));
        }
        let outcome = RecoveryStreamAuthorityRetirementOutcomeV21::TerminalVisible {
            receipt: protocol.receipt.planned_receipt.clone(),
            token_authority: observed.authority,
            profile: protocol.next_profile.clone(),
            manifest_version: snapshot.version(),
        };
        delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id).await?;
        return Ok(outcome);
    }
    if !stream_authority_retirement_manifest_is_prior_v21(snapshot, sidecar) {
        return Err(stream_authority_retirement_error_v21(
            sidecar,
            "manifest version, profile, or token authority differs from both exact retirement-v2 outcomes",
        ));
    }
    if observed.state == StreamAuthorityRetirementEffectStateV21::ExactNoEffect {
        if stream_authority_retirement_protocol_v21(sidecar).effect_phase
            != RecoveryEffectPhase::Armed
        {
            return Err(stream_authority_retirement_error_v21(
                sidecar,
                "confirmed retirement-v2 sidecar has no receipt effect",
            ));
        }
        commit_missing_stream_authority_retirement_receipt_v21(root_uri, sidecar).await?;
        return Box::pin(process_stream_authority_retirement_sidecar_v21_typed(
            root_uri, storage, snapshot, sidecar,
        ))
        .await;
    }
    let mut confirmed = sidecar.clone();
    if stream_authority_retirement_protocol_v21(sidecar).effect_phase == RecoveryEffectPhase::Armed
    {
        confirm_stream_authority_retirement_sidecar_v21(
            root_uri,
            storage.as_ref(),
            &mut confirmed,
            observed
                .transaction
                .clone()
                .expect("exact retirement-v2 receipt effect has a transaction"),
            observed.authority.current_head_witness.clone(),
            observed.authority.clone(),
        )
        .await?;
    }
    let protocol = stream_authority_retirement_protocol_v21(&confirmed);
    if protocol.effect_phase != RecoveryEffectPhase::EffectsConfirmed
        || observed.transaction.as_ref() != protocol.receipt.confirmed_transaction.as_ref()
        || Some(&observed.authority) != protocol.receipt.next_authority.as_ref()
    {
        return Err(stream_authority_retirement_error_v21(
            &confirmed,
            "retirement-v2 receipt did not converge to its exact durable confirmation",
        ));
    }
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH)?;
    let publish = publish_stream_authority_retirement_terminal_v21(root_uri, &confirmed).await;
    let fresh = fresh_snapshot_for_sidecar(root_uri, storage, &confirmed).await?;
    if !stream_authority_retirement_manifest_is_terminal_v21(&fresh, &confirmed) {
        return match publish {
            Ok(version) => Err(stream_authority_retirement_error_v21(
                &confirmed,
                format!(
                    "manifest publication returned version {version} without the exact retirement-v2 outcome"
                ),
            )),
            Err(error) => Err(error),
        };
    }
    validate_stream_authority_retirement_graph_authority_v21(root_uri, storage, &confirmed).await?;
    let expected_version = protocol.prior_manifest_version + 1;
    if let Ok(version) = publish
        && version != expected_version
    {
        return Err(stream_authority_retirement_error_v21(
            &confirmed,
            format!(
                "retirement-v2 published manifest version {version}, expected {expected_version}"
            ),
        ));
    }
    let outcome = RecoveryStreamAuthorityRetirementOutcomeV21::TerminalVisible {
        receipt: protocol.receipt.planned_receipt.clone(),
        token_authority: protocol
            .receipt
            .next_authority
            .as_ref()
            .expect("confirmed retirement-v2 token authority")
            .clone(),
        profile: protocol.next_profile.clone(),
        manifest_version: fresh.version(),
    };
    delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &confirmed.operation_id).await?;
    Ok(outcome)
}

pub(super) async fn process_stream_authority_retirement_sidecar_v21(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<bool> {
    process_stream_authority_retirement_sidecar_v21_typed(root_uri, storage, snapshot, sidecar)
        .await
        .map(|_| true)
}

pub(crate) async fn complete_stream_authority_retirement_sidecar_v21(
    root_uri: &str,
    storage: std::sync::Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<RecoveryStreamAuthorityRetirementOutcomeV21> {
    process_stream_authority_retirement_sidecar_v21_typed(root_uri, &storage, snapshot, sidecar)
        .await
}
