//! Cluster-owned RFC-026 stream-profile reconciliation.
//!
//! The ambient boolean flip is intentionally retired. One checked offline
//! cluster-apply capability owns the graph-wide profile gate, and terminal
//! profile transitions pair an immutable management receipt in the selected
//! token ledger with the next profile in one recovery-v13 publication.
//! Disable first consumes the live delegation into a durable `DISABLING`
//! continuation. This bounded slice can finish only an already-clean cut;
//! later lifecycle slices add the drain owner without changing this authority
//! boundary.

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::Omnigraph;
use crate::db::manifest::{
    DisablePlan, FoldContinuation, FoldDelegation, ManifestChange, ProfileManagementReceipt,
    ProfileManagementResult, PublishPrecondition, RecoveryAuthorityToken, RecoveryLineageIntent,
    RecoveryStreamProfileChangeKind, StreamLifecycle, StreamProfileEntry, StreamProfileMode,
    StreamProfileState, complete_stream_profile_change_sidecar_v13,
    confirm_stream_profile_change_sidecar_v13, lookup_profile_management_receipt,
    new_stream_profile_change_sidecar_v13, open_stream_token_authority_head,
    schema_apply_serial_queue_key, stage_profile_management_receipt,
    stream_profile_cluster_root_digest, stream_profile_graph_identity_digest,
    stream_profile_management_request_digest, stream_token_authority_entry_for_dataset,
    write_sidecar,
};
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};
use lance_index::mem_wal::ShardId;
use omnigraph_control_authority::{
    AuthorityOperationClass, ValidatedOfflineGuard, ValidatedRuntimeGuard,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// Engine-checked stopped-writer authority for one exact cluster profile
/// reconciliation.
///
/// The lower guard retains the real cluster state lock. The transition method
/// acquires the graph-profile write gate after its live recovery sweep and
/// before revalidating durable authority. Private fields and the absence of
/// `Clone`, serde, or an ambient constructor prevent callers from extending or
/// retargeting it.
#[doc(hidden)]
pub struct CheckedClusterApplyAuthority<'lock> {
    guard: ValidatedOfflineGuard<'lock>,
}

/// Engine-checked authority held by the sole served writer runtime.
///
/// This value owns the lower process registration for the complete
/// `Omnigraph` handle lifetime. It is deliberately non-cloneable.
#[doc(hidden)]
pub struct CheckedClusterStreamRuntimeAuthority {
    guard: ValidatedRuntimeGuard,
}

/// Outcome of the checked cluster stream-profile reconciler.
///
/// `changed == false` means the profile already matched the request; no
/// manifest version was minted and `manifest_version` is the version that
/// already carried the requested state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingProfileResult {
    pub changed: bool,
    pub streaming_enabled: bool,
    pub profile_mode: String,
    pub profile_revision: u64,
    pub manifest_version: u64,
}

impl StreamingProfileResult {
    fn from_profile(changed: bool, profile: &StreamProfileEntry, manifest_version: u64) -> Self {
        Self {
            changed,
            streaming_enabled: profile.streaming_enabled(),
            profile_mode: profile.mode().as_str().to_string(),
            profile_revision: profile.profile_revision,
            manifest_version,
        }
    }

    fn from_receipt(receipt: &ProfileManagementReceipt) -> Self {
        Self {
            changed: true,
            streaming_enabled: matches!(receipt.result.state, StreamProfileState::Enabled { .. }),
            profile_mode: receipt.result.state.mode().as_str().to_string(),
            profile_revision: receipt.result.profile_revision,
            manifest_version: receipt.result.manifest_version,
        }
    }
}

fn sealed_identity_cut(snapshot: &crate::db::Snapshot) -> Result<(u64, String)> {
    let mut identities = snapshot
        .stream_lifecycles()
        .filter(|(_, lifecycle)| lifecycle.lifecycle == StreamLifecycle::Sealed)
        .map(|(_, lifecycle)| {
            (
                lifecycle.identity.stable_table_id,
                lifecycle.identity.table_incarnation_id,
            )
        })
        .collect::<Vec<_>>();
    identities.sort_unstable();
    identities.dedup();
    let count = u64::try_from(identities.len())
        .map_err(|_| OmniError::manifest_internal("sealed stream identity count exceeds u64"))?;
    let mut hasher = Sha256::new();
    hasher.update(b"omnigraph.stream-profile-sealed-identity-cut.v1\0");
    hasher.update(count.to_be_bytes());
    for (stable_table_id, table_incarnation_id) in identities {
        hasher.update(stable_table_id.to_be_bytes());
        hasher.update(table_incarnation_id.to_be_bytes());
    }
    Ok((count, format!("sha256:{:x}", hasher.finalize())))
}

fn validate_recorded_profile_result(
    current: &StreamProfileEntry,
    guard: &ValidatedOfflineGuard<'_>,
    graph_identity_digest: &str,
    desired_mode: StreamProfileMode,
    receipt: &ProfileManagementReceipt,
) -> Result<StreamingProfileResult> {
    receipt.validate()?;
    let request_digest = stream_profile_management_request_digest(
        graph_identity_digest,
        guard.operation_id(),
        guard.declaration_revision(),
        guard.declaration_digest(),
        receipt.expected_profile_revision,
        desired_mode,
    )?;
    if receipt.graph_identity_digest != graph_identity_digest
        || receipt.operation_id != guard.operation_id()
        || receipt.actor != guard.actor()
        || receipt.declaration_revision != guard.declaration_revision()
        || receipt.declaration_digest != guard.declaration_digest()
        || receipt.request_digest != request_digest
        || receipt.result.state.mode() != desired_mode
    {
        return Err(OmniError::StreamingAuthorityMismatch {
            reason: format!(
                "profile operation '{}' is already bound to a different graph, declaration, actor, revision, or target",
                guard.operation_id()
            ),
        });
    }

    let receipt_chain = receipt.next_chain_ref()?;
    if current.profile_revision < receipt.result.profile_revision
        || current.profile_receipt_chain.record_count < receipt.chain_ordinal
        || (current.profile_receipt_chain.record_count == receipt.chain_ordinal
            && current.profile_receipt_chain != receipt_chain)
        || (current.profile_revision == receipt.result.profile_revision
            && (current.state != receipt.result.state
                || current.profile_receipt_chain != receipt_chain))
    {
        return Err(OmniError::manifest_internal(format!(
            "selected profile-management receipt '{}' is not consistent with the current stream-profile chain",
            receipt.operation_id
        )));
    }
    Ok(StreamingProfileResult::from_receipt(receipt))
}

fn validate_enabled_profile_for_apply(
    profile: &StreamProfileEntry,
    guard: &ValidatedOfflineGuard<'_>,
) -> Result<()> {
    let StreamProfileState::Enabled {
        active_fold_delegation,
    } = &profile.state
    else {
        return Err(OmniError::manifest_internal(
            "enabled-profile validation received a non-ENABLED state",
        ));
    };
    let cluster_root_digest = stream_profile_cluster_root_digest(guard.cluster_root())?;
    if active_fold_delegation.cluster_root_digest != cluster_root_digest
        || active_fold_delegation.declaration_revision != guard.declaration_revision()
        || active_fold_delegation.declaration_digest != guard.declaration_digest()
    {
        return Err(OmniError::StreamingAuthorityMismatch {
            reason: "the active stream delegation belongs to another cluster declaration; disable it before enabling the replacement declaration"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_runtime_profile_binding(
    profile: &StreamProfileEntry,
    guard: &ValidatedRuntimeGuard,
) -> Result<()> {
    let StreamProfileState::Enabled {
        active_fold_delegation,
    } = &profile.state
    else {
        return Err(match profile.mode() {
            StreamProfileMode::Retired => OmniError::StreamAuthorityRetired,
            mode => OmniError::StreamingRequiresClusterRuntime {
                mode: mode.as_str().to_string(),
            },
        });
    };
    if profile.profile_revision != guard.profile_revision() {
        return Err(OmniError::StreamingAuthorityMismatch {
            reason: format!(
                "runtime profile revision {} does not match graph revision {}",
                guard.profile_revision(),
                profile.profile_revision
            ),
        });
    }
    let cluster_root_digest = stream_profile_cluster_root_digest(guard.cluster_root())?;
    if active_fold_delegation.cluster_root_digest != cluster_root_digest
        || active_fold_delegation.declaration_revision != guard.declaration_revision()
        || active_fold_delegation.declaration_digest != guard.declaration_digest()
    {
        return Err(OmniError::StreamingAuthorityMismatch {
            reason: "runtime cluster/declaration binding differs from the active fold delegation"
                .to_string(),
        });
    }
    Ok(())
}

impl Omnigraph {
    /// Consume a lower stopped-writer guard into the only engine capability
    /// that can change the graph-global stream profile.
    #[doc(hidden)]
    pub async fn check_cluster_apply_authority<'lock>(
        &self,
        guard: ValidatedOfflineGuard<'lock>,
    ) -> Result<CheckedClusterApplyAuthority<'lock>> {
        if guard.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: format!(
                    "validated store '{}' does not match opened graph '{}'",
                    guard.graph_store_uri(),
                    self.uri()
                ),
            });
        }
        if !matches!(
            guard.operation(),
            AuthorityOperationClass::StreamProfileEnable
                | AuthorityOperationClass::StreamProfileDisable
        ) {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "offline authority is not a stream-profile operation".to_string(),
            });
        }
        self.enforce(
            omnigraph_policy::PolicyAction::StreamManage,
            &omnigraph_policy::ResourceScope::Graph,
            Some(guard.actor()),
        )?;
        Ok(CheckedClusterApplyAuthority { guard })
    }

    /// Attach the one checked serving-runtime authority to this engine handle.
    ///
    /// Startup validates the live graph profile after the lower layer rereads
    /// the exact applied cluster state and registers the sole local runtime.
    #[doc(hidden)]
    pub async fn with_checked_cluster_stream_runtime(
        mut self,
        guard: ValidatedRuntimeGuard,
    ) -> Result<Self> {
        if guard.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: format!(
                    "validated runtime store '{}' does not match opened graph '{}'",
                    guard.graph_store_uri(),
                    self.uri()
                ),
            });
        }
        if guard.operation() != AuthorityOperationClass::StreamRuntime {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "runtime guard has the wrong operation class".to_string(),
            });
        }

        let _profile_gate = self.write_queue().acquire_stream_profile_shared().await;
        let profile = self.current_canonical_stream_profile().await?;
        validate_runtime_profile_binding(&profile, &guard)?;
        self.stream_runtime_authority = Some(CheckedClusterStreamRuntimeAuthority { guard });
        Ok(self)
    }

    /// Final graph-profile preflight for Mutation/Load/delete.
    ///
    /// The caller already holds the root-shared profile gate in shared mode and
    /// keeps it through input ownership, staging, recovery, every Lance effect,
    /// and manifest publication.
    pub(crate) async fn ensure_streaming_content_write_authorized(&self) -> Result<()> {
        let profile = self.current_canonical_stream_profile().await?;
        match profile.mode() {
            StreamProfileMode::Disabled => Ok(()),
            StreamProfileMode::Enabled => {
                let Some(runtime) = self.stream_runtime_authority.as_ref() else {
                    return Err(OmniError::StreamingRequiresClusterRuntime {
                        mode: profile.mode().as_str().to_string(),
                    });
                };
                validate_runtime_profile_binding(&profile, &runtime.guard)
            }
            StreamProfileMode::Disabling => Err(OmniError::StreamingRequiresClusterRuntime {
                mode: profile.mode().as_str().to_string(),
            }),
            StreamProfileMode::Retired => Err(OmniError::StreamAuthorityRetired),
        }
    }

    /// Final graph-profile preflight for the hidden stream-ingest seam.
    ///
    /// Unlike ordinary content writes, ingestion is meaningful only while the
    /// profile is `ENABLED`, and it always requires the checked serving
    /// runtime that is bound to this exact cluster declaration and profile
    /// revision. The caller holds the root-shared profile gate through the
    /// complete admission result.
    pub(crate) async fn ensure_streaming_ingest_runtime_authorized(&self) -> Result<()> {
        let profile = self.current_canonical_stream_profile().await?;
        match profile.mode() {
            StreamProfileMode::Enabled => {
                let Some(runtime) = self.stream_runtime_authority.as_ref() else {
                    return Err(OmniError::StreamingRequiresClusterRuntime {
                        mode: profile.mode().as_str().to_string(),
                    });
                };
                validate_runtime_profile_binding(&profile, &runtime.guard)
            }
            mode @ (StreamProfileMode::Disabled | StreamProfileMode::Disabling) => {
                Err(OmniError::StreamingRequiresClusterRuntime {
                    mode: mode.as_str().to_string(),
                })
            }
            StreamProfileMode::Retired => Err(OmniError::StreamAuthorityRetired),
        }
    }

    /// Final checked-runtime preflight for the private same-binding SEALED
    /// maintenance bridge. It intentionally has the same profile requirement
    /// as ingestion (exact ENABLED delegation), while operation-specific
    /// lifecycle checks remain in the maintenance adapter.
    pub(crate) async fn ensure_streaming_sealed_maintenance_runtime_authorized(
        &self,
    ) -> Result<()> {
        self.ensure_streaming_ingest_runtime_authorized().await
    }

    /// BranchMerge has no token-aware sequencing transition in this profile.
    ///
    /// The profile gate still protects it from a concurrent transition, but a
    /// served-runtime guard is intentionally insufficient: moving logical
    /// contents would obsolete `_stream_tokens` authority for any enrolled
    /// identity. Keep it closed until a dedicated merge protocol exists.
    pub(crate) async fn ensure_streaming_branch_merge_allowed(&self) -> Result<()> {
        let profile = self.current_canonical_stream_profile().await?;
        match profile.mode() {
            StreamProfileMode::Disabled => Ok(()),
            StreamProfileMode::Retired => Err(OmniError::StreamAuthorityRetired),
            mode @ (StreamProfileMode::Enabled | StreamProfileMode::Disabling) => {
                Err(OmniError::StreamingContentOperationUnsupported {
                    operation: "branch_merge".to_string(),
                    mode: mode.as_str().to_string(),
                })
            }
        }
    }

    /// Retired ambient graph-wide streaming toggle.
    ///
    /// Kept as an effect-free compatibility trap so older embedded callers get
    /// a typed migration error. Cluster apply uses the non-forgeable checked
    /// authority path; no caller-selected boolean or actor reaches it.
    pub async fn set_streaming_enabled_as(
        &self,
        _enabled: bool,
        _actor: Option<&str>,
    ) -> Result<StreamingProfileResult> {
        Err(OmniError::StreamingRequiresClusterControlPlane)
    }

    async fn commit_terminal_stream_profile_change(
        &self,
        txn: &crate::db::WriteTxn,
        prior_profile: StreamProfileEntry,
        next_profile: StreamProfileEntry,
        receipt: ProfileManagementReceipt,
        change_kind: RecoveryStreamProfileChangeKind,
    ) -> Result<StreamingProfileResult> {
        if txn.base.version() != receipt.result.manifest_version.saturating_sub(1)
            || txn.base.stream_profile() != &prior_profile
            || receipt.result.profile_revision != next_profile.profile_revision
            || receipt.result.state != next_profile.state
        {
            return Err(OmniError::manifest_internal(
                "terminal stream-profile receipt was prepared against a different manifest transition",
            ));
        }

        let terminal_actor = receipt.actor.clone();
        let selected_token = txn.base.open_stream_token_authority().await?;
        let staged = stage_profile_management_receipt(
            selected_token,
            txn.base.stream_token_authority(),
            &receipt,
        )
        .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                txn.base.stream_token_authority(),
                &self.control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);

        let coordinator = self.open_coordinator_for_branch(None).await?;
        if coordinator.snapshot().version() != txn.base.version()
            || coordinator.snapshot().stream_profile() != &prior_profile
            || coordinator.snapshot().stream_token_authority() != txn.base.stream_token_authority()
        {
            return Err(OmniError::manifest_read_set_changed(
                "stream_profile_terminal_authority",
                Some(txn.base.version().to_string()),
                Some(coordinator.snapshot().version().to_string()),
            ));
        }
        let lineage = coordinator.new_lineage_intent(Some(&terminal_actor), None)?;
        let recovery_authority = RecoveryAuthorityToken {
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
        let mut sidecar = new_stream_profile_change_sidecar_v13(
            terminal_actor,
            recovery_authority,
            recovery_lineage,
            txn.base.version(),
            change_kind,
            prior_profile,
            next_profile.clone(),
            txn.base.stream_token_authority().clone(),
            receipt.clone(),
            planned_transaction,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

        let outcome = match self.storage().commit_staged_exact(token_head, staged).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let recovered = complete_stream_profile_change_sidecar_v13(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &txn.base,
                    &sidecar,
                )
                .await;
                match recovered {
                    Ok(()) => {
                        self.refresh_coordinator_only().await?;
                        let current = self.current_canonical_stream_profile().await?;
                        if current == next_profile {
                            return Ok(StreamingProfileResult::from_receipt(&receipt));
                        }
                        return Err(error);
                    }
                    Err(recovery_error) => {
                        return Err(OmniError::recovery_required(
                            handle.operation_id,
                            format!(
                                "profile receipt commit failed ({error}) and exact recovery did not complete: {recovery_error}"
                            ),
                        ));
                    }
                }
            }
        };
        if !outcome.is_exact() {
            return Err(OmniError::recovery_required(
                handle.operation_id,
                "profile receipt participant committed a non-exact transaction",
            ));
        }

        let next_token_authority =
            stream_token_authority_entry_for_dataset(outcome.snapshot().dataset())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
        confirm_stream_profile_change_sidecar_v13(
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
                format!("profile receipt confirmation requires recovery: {error}"),
            )
        })?;
        complete_stream_profile_change_sidecar_v13(
            self.root_uri(),
            Arc::clone(&self.storage),
            &txn.base,
            &sidecar,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id,
                format!("profile receipt publication requires recovery: {error}"),
            )
        })?;
        self.refresh_coordinator_only().await?;
        let current = self.current_canonical_stream_profile().await?;
        if current != next_profile {
            return Err(OmniError::manifest_internal(
                "completed profile receipt did not install its exact terminal profile",
            ));
        }
        Ok(StreamingProfileResult::from_receipt(&receipt))
    }

    async fn finish_disabling_stream_profile(
        &self,
        txn: &crate::db::WriteTxn,
        cluster_root_digest: &str,
        graph_identity_digest: &str,
    ) -> Result<StreamingProfileResult> {
        let prior = txn.base.stream_profile().clone();
        let StreamProfileState::Disabling { disable_plan } = &prior.state else {
            return Err(OmniError::manifest_internal(
                "disable continuation requires a DISABLING profile",
            ));
        };
        if disable_plan.fold_continuation.cluster_root_digest != cluster_root_digest {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "the durable disable continuation belongs to another cluster root"
                    .to_string(),
            });
        }

        let mut undrained_tables = txn
            .base
            .stream_lifecycles()
            .filter(|(_, lifecycle)| lifecycle.lifecycle != StreamLifecycle::Sealed)
            .map(|(_, lifecycle)| lifecycle.diagnostic_table_key.clone())
            .collect::<Vec<_>>();
        if !undrained_tables.is_empty() {
            undrained_tables.sort();
            return Err(OmniError::StreamingDisablePending { undrained_tables });
        }

        let request_digest = stream_profile_management_request_digest(
            graph_identity_digest,
            &disable_plan.operation_id,
            &disable_plan.declaration_revision,
            &disable_plan.declaration_digest,
            prior.profile_revision,
            StreamProfileMode::Disabled,
        )?;
        if request_digest != disable_plan.request_digest {
            return Err(OmniError::manifest_internal(
                "durable disable plan differs from its canonical terminal request",
            ));
        }

        let now = OffsetDateTime::now_utc().unix_timestamp();
        let (_, sealed_identities_digest) = sealed_identity_cut(&txn.base)?;
        let next_revision = prior
            .profile_revision
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream profile revision overflow"))?;
        let result = ProfileManagementResult::new(
            txn.base
                .version()
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("manifest version overflow"))?,
            next_revision,
            StreamProfileState::Disabled,
            0,
            sealed_identities_digest,
        )?;
        let receipt = ProfileManagementReceipt::new(
            graph_identity_digest,
            &prior.profile_receipt_chain,
            &disable_plan.operation_id,
            request_digest,
            &disable_plan.declaration_revision,
            &disable_plan.declaration_digest,
            &disable_plan.actor,
            prior.profile_revision,
            result,
            now,
        )?;
        let next = StreamProfileEntry::disabled_from_disabling(&prior, receipt.next_chain_ref()?)?;
        self.commit_terminal_stream_profile_change(
            txn,
            prior,
            next,
            receipt,
            RecoveryStreamProfileChangeKind::DisableReceipt,
        )
        .await
    }

    /// Reconcile the requested mode carried by one checked cluster-apply
    /// capability. No raw boolean or actor is accepted at this boundary.
    #[doc(hidden)]
    pub async fn set_streaming_profile_checked(
        &self,
        authority: CheckedClusterApplyAuthority<'_>,
    ) -> Result<StreamingProfileResult> {
        let enabled = authority
            .guard
            .operation()
            .requested_streaming_enabled()
            .ok_or_else(|| OmniError::StreamingAuthorityMismatch {
                reason: "checked apply authority does not carry a profile target".to_string(),
            })?;
        if authority.guard.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "checked apply authority was retargeted to another graph".to_string(),
            });
        }
        self.ensure_schema_state_valid().await?;
        // Main-only: the profile row lives on the canonical main manifest and
        // every branch obeys it through the graph-level snapshot.
        let relevant: [Option<&str>; 1] = [None];
        self.heal_pending_recovery_sidecars_for_write(&relevant)
            .await?;
        // The healer owns the same root gate while classifying sidecars, so
        // acquire profile exclusivity only after it returns. From this point
        // through terminal publication no ordinary Mutation/Load/delete can
        // enter or retain a shared profile window.
        let _profile_gate = self.write_queue().acquire_stream_profile_exclusive().await;
        let admission_keys = self.capture_branch_control_stream_admission_keys().await?;
        let _stream_admission_guards = self
            .write_queue()
            .acquire_stream_shared_many(&admission_keys)
            .await;
        let control_branches: [Option<String>; 1] = [None];
        let _schema_guard = self
            .write_queue()
            .acquire(&schema_apply_serial_queue_key())
            .await;
        let _branch_guards = self.write_queue().acquire_branches(&control_branches).await;
        let _stream_token_guard = self.write_queue().acquire_stream_token().await;
        self.ensure_schema_apply_not_locked("set_streaming_enabled")
            .await?;
        self.ensure_no_pending_recovery_sidecars_under_gates(&relevant, "set_streaming_enabled")
            .await?;
        self.ensure_schema_state_valid().await?;

        // Capture schema, branch incarnation, graph head, and the canonical
        // main snapshot together. Recovery-v13 binds all of them.
        let mut txn = self.open_write_txn(None).await?;
        let snapshot = &txn.base;
        Self::ensure_branch_control_stream_admission_covered(
            "set_streaming_enabled",
            &admission_keys,
            snapshot,
        )?;

        let mut expected = snapshot.stream_profile().clone();
        let cluster_root_digest =
            stream_profile_cluster_root_digest(authority.guard.cluster_root())?;
        let graph_identity_digest = stream_profile_graph_identity_digest(
            &cluster_root_digest,
            authority.guard.graph_id(),
            authority.guard.graph_store_uri(),
        )?;
        let desired_mode = if enabled {
            StreamProfileMode::Enabled
        } else {
            StreamProfileMode::Disabled
        };

        // Receipt lookup deliberately precedes the current-revision check.
        // A delayed exact retry returns the original bounded result even after
        // the graph has entered a later profile cycle.
        let token_dataset = snapshot.open_stream_token_authority().await?;
        if let Some(receipt) = lookup_profile_management_receipt(
            &token_dataset,
            snapshot.stream_token_authority(),
            &graph_identity_digest,
            authority.guard.operation_id(),
        )
        .await?
        {
            return validate_recorded_profile_result(
                &expected,
                &authority.guard,
                &graph_identity_digest,
                desired_mode,
                &receipt,
            );
        }

        // A durable continuation belongs to its original operation, actor,
        // and declaration. Finish it from the plan itself before reconciling
        // today's desired mode; a later config revision must not strand the
        // admission cutoff. This also permits DISABLING -> DISABLED -> ENABLED
        // in one checked apply without ever restoring the consumed delegation.
        let mut completed_prior_disable = false;
        if expected.mode() == StreamProfileMode::Disabling {
            let completed = self
                .finish_disabling_stream_profile(&txn, &cluster_root_digest, &graph_identity_digest)
                .await?;
            if desired_mode == StreamProfileMode::Disabled {
                return Ok(completed);
            }
            completed_prior_disable = true;
            txn = self.open_write_txn(None).await?;
            expected = txn.base.stream_profile().clone();
            if expected.mode() != StreamProfileMode::Disabled {
                return Err(OmniError::manifest_internal(
                    "completed disable continuation did not install DISABLED",
                ));
            }
            Self::ensure_branch_control_stream_admission_covered(
                "set_streaming_enabled",
                &admission_keys,
                &txn.base,
            )?;
        }

        if !completed_prior_disable
            && expected.profile_revision != authority.guard.expected_profile_revision()
        {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: format!(
                    "expected profile revision {} but graph is at {}",
                    authority.guard.expected_profile_revision(),
                    expected.profile_revision
                ),
            });
        }

        match (desired_mode, expected.mode()) {
            (StreamProfileMode::Enabled, StreamProfileMode::Enabled) => {
                validate_enabled_profile_for_apply(&expected, &authority.guard)?;
                return Ok(StreamingProfileResult::from_profile(
                    false,
                    &expected,
                    txn.base.version(),
                ));
            }
            (StreamProfileMode::Disabled, StreamProfileMode::Disabled) => {
                return Ok(StreamingProfileResult::from_profile(
                    false,
                    &expected,
                    txn.base.version(),
                ));
            }
            (_, StreamProfileMode::Retired) => {
                return Err(OmniError::StreamAuthorityRetired);
            }
            (StreamProfileMode::Enabled, StreamProfileMode::Disabled)
            | (StreamProfileMode::Disabled, StreamProfileMode::Enabled) => {}
            (_, StreamProfileMode::Disabling) => {
                return Err(OmniError::manifest_internal(
                    "DISABLING remained after its checked continuation completed",
                ));
            }
            _ => {
                return Err(OmniError::manifest_internal(
                    "unsupported checked stream-profile target",
                ));
            }
        }

        if desired_mode == StreamProfileMode::Disabled {
            if txn.base.streaming_status().undrained {
                let mut undrained_tables: Vec<String> = txn
                    .base
                    .stream_lifecycles()
                    .filter(|(_, lifecycle)| lifecycle.lifecycle != StreamLifecycle::Sealed)
                    .map(|(_, lifecycle)| lifecycle.diagnostic_table_key.clone())
                    .collect();
                undrained_tables.sort();
                return Err(OmniError::StreamingDisablePending { undrained_tables });
            }

            let active_delegation = match &expected.state {
                StreamProfileState::Enabled {
                    active_fold_delegation,
                } => active_fold_delegation,
                _ => unreachable!("mode checked above"),
            };
            if active_delegation.cluster_root_digest != cluster_root_digest {
                return Err(OmniError::StreamingAuthorityMismatch {
                    reason: "the active stream delegation belongs to a different cluster root"
                        .to_string(),
                });
            }
            let disabling_revision = expected
                .profile_revision
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("stream profile revision overflow"))?;
            let terminal_request_digest = stream_profile_management_request_digest(
                &graph_identity_digest,
                authority.guard.operation_id(),
                authority.guard.declaration_revision(),
                authority.guard.declaration_digest(),
                disabling_revision,
                StreamProfileMode::Disabled,
            )?;
            let continuation = FoldContinuation::derive(active_delegation, disabling_revision)?;
            let disable_plan = DisablePlan::new(
                authority.guard.operation_id(),
                terminal_request_digest,
                authority.guard.declaration_revision(),
                authority.guard.declaration_digest(),
                authority.guard.actor(),
                continuation,
            )?;
            let disabling = StreamProfileEntry::disabling_from(&expected, disable_plan)?;

            // ENABLED -> DISABLING is one effect-free manifest CAS. The
            // terminal receipt is armed only after this durable admission
            // cutoff is visible.
            let mut main_coordinator = self.open_coordinator_for_branch(None).await?;
            if main_coordinator.snapshot().version() != txn.base.version()
                || main_coordinator.snapshot().stream_profile() != &expected
            {
                return Err(OmniError::manifest_read_set_changed(
                    expected.object_id(),
                    Some(txn.base.version().to_string()),
                    Some(main_coordinator.snapshot().version().to_string()),
                ));
            }
            let changes = [ManifestChange::SetStreamProfile {
                expected: expected.clone(),
                next: disabling.clone(),
            }];
            let intent =
                main_coordinator.new_lineage_intent(Some(authority.guard.actor()), None)?;
            main_coordinator
                .commit_changes_with_intent_and_expected(
                    &changes,
                    &HashMap::new(),
                    intent,
                    &PublishPrecondition::Any,
                )
                .await?;
            self.install_current_main_stream_profile(main_coordinator)
                .await;
            txn = self.open_write_txn(None).await?;
            if txn.base.stream_profile() != &disabling {
                return Err(OmniError::manifest_internal(
                    "disable plan publication did not install its exact DISABLING state",
                ));
            }
            return self
                .finish_disabling_stream_profile(&txn, &cluster_root_digest, &graph_identity_digest)
                .await;
        }

        // The only remaining path is a fresh DISABLED -> ENABLED terminal
        // receipt. Existing SEALED lanes remain stopped and are counted in the
        // result so the later lifecycle slice can resume them explicitly.
        let prior = txn.base.stream_profile().clone();
        if prior.mode() != StreamProfileMode::Disabled {
            return Err(OmniError::manifest_internal(
                "profile enable terminal step requires DISABLED",
            ));
        }
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let (sealed_count, sealed_identities_digest) = sealed_identity_cut(&txn.base)?;
        let next_revision = prior
            .profile_revision
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("stream profile revision overflow"))?;
        let request_digest = stream_profile_management_request_digest(
            &graph_identity_digest,
            authority.guard.operation_id(),
            authority.guard.declaration_revision(),
            authority.guard.declaration_digest(),
            prior.profile_revision,
            StreamProfileMode::Enabled,
        )?;
        let provisional_state = StreamProfileState::Enabled {
            active_fold_delegation: FoldDelegation::issue(
                ShardId::new_v4().to_string(),
                cluster_root_digest,
                authority.guard.declaration_revision(),
                authority.guard.declaration_digest(),
                next_revision,
                authority.guard.actor(),
                now,
            )?,
        };
        let result = ProfileManagementResult::new(
            txn.base
                .version()
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest_internal("manifest version overflow"))?,
            next_revision,
            provisional_state.clone(),
            sealed_count,
            sealed_identities_digest,
        )?;
        let receipt = ProfileManagementReceipt::new(
            graph_identity_digest,
            &prior.profile_receipt_chain,
            authority.guard.operation_id(),
            request_digest,
            authority.guard.declaration_revision(),
            authority.guard.declaration_digest(),
            authority.guard.actor(),
            prior.profile_revision,
            result,
            now,
        )?;
        let next_chain = receipt.next_chain_ref()?;
        let next = match provisional_state {
            StreamProfileState::Enabled {
                active_fold_delegation,
            } => StreamProfileEntry::enabled_from(&prior, next_chain, active_fold_delegation)?,
            StreamProfileState::Disabled => {
                StreamProfileEntry::disabled_from_disabling(&prior, next_chain)?
            }
            _ => {
                return Err(OmniError::manifest_internal(
                    "profile-management receipt prepared a non-terminal state",
                ));
            }
        };
        self.commit_terminal_stream_profile_change(
            &txn,
            prior,
            next,
            receipt,
            RecoveryStreamProfileChangeKind::EnableReceipt,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::db::manifest::{
        CurrentHeadWitness, ManifestCoordinator, STREAM_CONFIG_VERSION, StreamLifecycleEntry,
        StreamPhysicalBinding,
    };
    use lance::dataset::refs::BranchIdentifier;
    use omnigraph_control_authority::{
        OfflineAuthorityRequest, StateLockAcquire, acquire_state_lock, validate_offline_guard,
    };
    use omnigraph_policy::{PolicyChecker, PolicyEngine};
    use omnigraph_storage::storage_handle_for_uri;

    const TEST_SCHEMA: &str = r#"
node Person {
    name: String
}
"#;

    fn write_cluster_state(cluster: &tempfile::TempDir) -> String {
        let value = serde_json::json!({
            "version": 1,
            "state_revision": 1,
            "applied_revision": {
                "config_digest": "test-config",
                "resources": {}
            }
        });
        let text = serde_json::to_string_pretty(&value).unwrap();
        let path = cluster.path().join("__cluster/state.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, &text).unwrap();
        format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
    }

    async fn apply_profile(
        db: &Omnigraph,
        cluster: &tempfile::TempDir,
        state_cas: &str,
        expected_profile_revision: u64,
        operation_id: &str,
        operation: AuthorityOperationClass,
        declaration_revision: &str,
        declaration_digest: &str,
        actor: &str,
    ) -> Result<StreamingProfileResult> {
        let cluster_uri = format!("file://{}", cluster.path().display());
        let storage = storage_handle_for_uri(&cluster_uri).unwrap();
        let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
        let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(lock) => lock,
            StateLockAcquire::Held => panic!("fresh test apply lock is already held"),
        };
        let guard = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: db.uri(),
                expected_state_cas: state_cas,
                state_revision: 1,
                declaration_revision,
                declaration_digest,
                expected_profile_revision,
                operation_id,
                operation,
                actor,
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap();
        let authority = db.check_cluster_apply_authority(guard).await?;
        db.set_streaming_profile_checked(authority).await
    }

    #[tokio::test]
    async fn checked_profile_enable_disable_and_receipt_first_replay() {
        let cluster = tempfile::tempdir().unwrap();
        let state_cas = write_cluster_state(&cluster);
        let graph = cluster.path().join("graphs/knowledge.omni");
        let db = Omnigraph::init(graph.to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
        db.branch_create("feature").await.unwrap();

        assert!(matches!(
            db.set_streaming_enabled_as(true, Some("operator")).await,
            Err(OmniError::StreamingRequiresClusterControlPlane)
        ));

        let enabled = apply_profile(
            &db,
            &cluster,
            &state_cas,
            1,
            "enable-operation",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();
        assert!(enabled.changed);
        assert!(enabled.streaming_enabled);
        assert_eq!(enabled.profile_mode, "ENABLED");
        assert_eq!(enabled.profile_revision, 2);

        let direct_write = db
            .load("main", "", crate::loader::LoadMode::Merge)
            .await
            .unwrap_err();
        assert!(matches!(
            direct_write,
            OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
        ));
        let direct_merge = db.branch_merge("feature", "main").await.unwrap_err();
        assert!(matches!(
            direct_merge,
            OmniError::StreamingContentOperationUnsupported {
                ref operation,
                ref mode,
            } if operation == "branch_merge" && mode == "ENABLED"
        ));

        // The lower guard observes the later revision, but receipt-first
        // replay still returns the exact original result.
        let replay = apply_profile(
            &db,
            &cluster,
            &state_cas,
            2,
            "enable-operation",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();
        assert_eq!(replay, enabled);

        // Actor is not part of the stable operation ID/request digest, but the
        // retained receipt commits it separately. A replacement operator
        // cannot relabel an already-completed occurrence.
        let actor_mismatch = apply_profile(
            &db,
            &cluster,
            &state_cas,
            2,
            "enable-operation",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:bob",
        )
        .await
        .unwrap_err();
        assert!(matches!(
            actor_mismatch,
            OmniError::StreamingAuthorityMismatch { ref reason }
                if reason.contains("actor")
        ));

        let disabled = apply_profile(
            &db,
            &cluster,
            &state_cas,
            2,
            "disable-operation",
            AuthorityOperationClass::StreamProfileDisable,
            "stream-declaration-disabled-v1",
            &format!("sha256:{}", "b".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();
        assert!(disabled.changed);
        assert!(!disabled.streaming_enabled);
        assert_eq!(disabled.profile_mode, "DISABLED");
        assert_eq!(disabled.profile_revision, 4);
        assert_eq!(disabled.manifest_version, enabled.manifest_version + 2);

        let status = db.stream_status().await.unwrap();
        assert_eq!(status.profile_mode, "DISABLED");
        assert_eq!(status.profile_revision, 4);
    }

    #[tokio::test]
    async fn checked_profile_authority_enforces_stream_manage_policy() {
        const POLICY: &str = r#"
version: 1
groups:
  operators: [operator:allowed]
  observers: [operator:denied]
rules:
  - id: operators-stream-manage
    allow:
      actors: { group: operators }
      actions: [stream_manage]
"#;
        let cluster = tempfile::tempdir().unwrap();
        let state_cas = write_cluster_state(&cluster);
        let graph = cluster.path().join("graphs/knowledge.omni");
        let policy_path = cluster.path().join("graph.policy.yaml");
        std::fs::write(&policy_path, POLICY).unwrap();
        let db = Omnigraph::init(graph.to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
        let policy = PolicyEngine::load_graph(&policy_path, graph.to_str().unwrap()).unwrap();
        let db = db.with_policy(Arc::new(policy) as Arc<dyn PolicyChecker>);

        let denied = apply_profile(
            &db,
            &cluster,
            &state_cas,
            1,
            "denied-enable-operation",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:denied",
        )
        .await
        .unwrap_err();
        assert!(matches!(denied, OmniError::Policy(_)));
        assert_eq!(
            db.current_canonical_stream_profile().await.unwrap().mode(),
            StreamProfileMode::Disabled,
            "policy refusal must be effect-free"
        );

        let enabled = apply_profile(
            &db,
            &cluster,
            &state_cas,
            1,
            "allowed-enable-operation",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:allowed",
        )
        .await
        .unwrap();
        assert_eq!(enabled.profile_mode, "ENABLED");
    }

    #[tokio::test]
    async fn checked_disable_requires_sealed_cut_preserves_authority_and_fences_direct_write() {
        let cluster = tempfile::tempdir().unwrap();
        let state_cas = write_cluster_state(&cluster);
        let graph = cluster.path().join("graphs/knowledge.omni");
        let db = Omnigraph::init(graph.to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
        apply_profile(
            &db,
            &cluster,
            &state_cas,
            1,
            "enable-before-undrained",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-declaration-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();

        let mut coordinator = ManifestCoordinator::open(db.uri()).await.unwrap();
        let table = coordinator.snapshot().entry("node:Person").unwrap().clone();
        let shard_id = "22222222-2222-4222-8222-222222222222".to_string();
        let binding = StreamPhysicalBinding {
            stable_table_id: table.identity.stable_table_id,
            table_incarnation_id: table.identity.table_incarnation_id,
            table_location: table.table_path.clone(),
            table_branch: None,
            enrollment_id: "11111111-1111-4111-8111-111111111111".to_string(),
            shard_ids: vec![shard_id.clone()],
            stream_config_version: STREAM_CONFIG_VERSION,
            stream_config_hash: format!("sha256:{}", "c".repeat(64)),
        };
        let head = CurrentHeadWitness {
            branch_identifier: BranchIdentifier::main(),
            table_version: table.table_version,
            transaction_uuid: "33333333-3333-4333-8333-333333333333".to_string(),
            manifest_e_tag: None,
        };
        let enrollment_receipt = crate::db::manifest::stream::EnrollmentReceipt::new(
            "44444444-4444-4444-8444-444444444444".to_string(),
            format!("sha256:{}", "d".repeat(64)),
            "55555555-5555-4555-8555-555555555555".to_string(),
            binding.clone(),
        )
        .unwrap();
        let binding_scope_id = "77777777-7777-4777-8777-777777777777";
        let binding_receipt = crate::db::manifest::stream::BindingReceipt::new(
            crate::db::manifest::stream::stream_graph_identity_digest("stream-profile-test-domain")
                .unwrap(),
            table.identity,
            &crate::db::manifest::stream::binding_receipt_chain_genesis(),
            binding_scope_id,
            enrollment_receipt.stream_incarnation_id.clone(),
            binding.clone(),
            "INITIAL_ENROLLMENT",
            1,
        )
        .unwrap();
        let lifecycle = StreamLifecycleEntry::new_open_enrollment(
            table.identity,
            table.table_key.clone(),
            binding,
            binding_scope_id.to_string(),
            head,
            BTreeMap::from([(shard_id, 1)]),
            enrollment_receipt,
            binding_receipt.record_id.clone(),
            binding_receipt.next_chain_ref().unwrap(),
        )
        .unwrap();
        coordinator
            .commit_changes(&[ManifestChange::SetStreamLifecycle {
                expected: None,
                next: lifecycle.clone(),
            }])
            .await
            .unwrap();

        let error = apply_profile(
            &db,
            &cluster,
            &state_cas,
            2,
            "disable-undrained",
            AuthorityOperationClass::StreamProfileDisable,
            "stream-declaration-disabled-v1",
            &format!("sha256:{}", "b".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            OmniError::StreamingDisablePending { ref undrained_tables }
                if undrained_tables == &["node:Person".to_string()]
        ));

        let snapshot = ManifestCoordinator::open(db.uri())
            .await
            .unwrap()
            .snapshot();
        assert_eq!(snapshot.stream_profile().mode(), StreamProfileMode::Enabled);
        assert_eq!(snapshot.stream_profile().profile_revision, 2);
        assert_eq!(
            snapshot.stream_profile().profile_receipt_chain.record_count,
            1
        );

        let sealed = crate::db::manifest::stream::test_sealed_lifecycle_from(&lifecycle).unwrap();
        let mut coordinator = ManifestCoordinator::open(db.uri()).await.unwrap();
        coordinator
            .commit_changes(&[ManifestChange::SetStreamLifecycle {
                expected: Some(lifecycle),
                next: sealed.clone(),
            }])
            .await
            .unwrap();

        let disabled = apply_profile(
            &db,
            &cluster,
            &state_cas,
            2,
            "disable-at-sealed-cut",
            AuthorityOperationClass::StreamProfileDisable,
            "stream-declaration-disabled-v1",
            &format!("sha256:{}", "b".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();
        assert_eq!(disabled.profile_mode, "DISABLED");
        assert_eq!(disabled.profile_revision, 4);

        let after_disable = ManifestCoordinator::open(db.uri())
            .await
            .unwrap()
            .snapshot();
        assert_eq!(
            after_disable.stream_lifecycle(table.identity),
            Some(&sealed),
            "disable must retain the exact SEALED lifecycle row and proof"
        );
        let table_version = after_disable.entry("node:Person").unwrap().table_version;

        let direct_write = db
            .load(
                "main",
                r#"{"type":"Person","data":{"name":"after-disable"}}"#,
                crate::loader::LoadMode::Merge,
            )
            .await
            .unwrap_err();
        match direct_write {
            OmniError::Manifest(error) => assert!(matches!(
                error.details,
                Some(crate::error::ManifestConflictDetails::StreamLifecycleConflict {
                    table_key,
                    lifecycle,
                    operation,
                    ..
                }) if table_key == "node:Person"
                    && lifecycle == "SEALED"
                    && operation == "load"
            )),
            other => panic!("expected typed stream lifecycle conflict, got {other:?}"),
        }

        let after_write_refusal = ManifestCoordinator::open(db.uri())
            .await
            .unwrap()
            .snapshot();
        assert_eq!(
            after_write_refusal.stream_lifecycle(table.identity),
            Some(&sealed),
            "direct-write refusal must retain the exact SEALED lifecycle row and proof"
        );
        assert_eq!(
            after_write_refusal
                .entry("node:Person")
                .unwrap()
                .table_version,
            table_version,
            "direct-write lifecycle refusal must be effect-free"
        );
    }

    #[tokio::test]
    async fn newer_enable_finishes_stored_disable_plan_before_reconciling() {
        let cluster = tempfile::tempdir().unwrap();
        let state_cas = write_cluster_state(&cluster);
        let graph = cluster.path().join("graphs/knowledge.omni");
        let db = Omnigraph::init(graph.to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
        apply_profile(
            &db,
            &cluster,
            &state_cas,
            1,
            "initial-enable",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-enabled-v1",
            &format!("sha256:{}", "a".repeat(64)),
            "operator:alice",
        )
        .await
        .unwrap();

        // Model the only sidecar-free crash window: the admission-cutoff CAS
        // landed, but no terminal receipt sidecar was armed.
        let mut coordinator = ManifestCoordinator::open(db.uri()).await.unwrap();
        let enabled = coordinator.snapshot().stream_profile().clone();
        let active = match &enabled.state {
            StreamProfileState::Enabled {
                active_fold_delegation,
            } => active_fold_delegation,
            other => panic!("expected ENABLED, got {other:?}"),
        };
        let old_declaration_revision = "stream-disabled-old-v1";
        let old_declaration_digest = format!("sha256:{}", "b".repeat(64));
        let cluster_root =
            omnigraph_storage::normalize_root_uri(cluster.path().to_str().unwrap()).unwrap();
        let cluster_root_digest = stream_profile_cluster_root_digest(&cluster_root).unwrap();
        let graph_identity_digest =
            stream_profile_graph_identity_digest(&cluster_root_digest, "knowledge", db.uri())
                .unwrap();
        let disabling_revision = enabled.profile_revision + 1;
        let old_request_digest = stream_profile_management_request_digest(
            &graph_identity_digest,
            "old-disable",
            old_declaration_revision,
            &old_declaration_digest,
            disabling_revision,
            StreamProfileMode::Disabled,
        )
        .unwrap();
        let plan = DisablePlan::new(
            "old-disable",
            old_request_digest,
            old_declaration_revision,
            &old_declaration_digest,
            "operator:alice",
            FoldContinuation::derive(active, disabling_revision).unwrap(),
        )
        .unwrap();
        let disabling = StreamProfileEntry::disabling_from(&enabled, plan).unwrap();
        coordinator
            .commit_changes(&[ManifestChange::SetStreamProfile {
                expected: enabled,
                next: disabling,
            }])
            .await
            .unwrap();

        // A newer desired config wants ENABLED again. The stored old disable
        // occurrence must terminalize first; only then may the new delegation
        // be issued.
        let reenabled = apply_profile(
            &db,
            &cluster,
            &state_cas,
            disabling_revision,
            "new-enable",
            AuthorityOperationClass::StreamProfileEnable,
            "stream-enabled-new-v1",
            &format!("sha256:{}", "c".repeat(64)),
            "operator:bob",
        )
        .await
        .unwrap();
        assert_eq!(reenabled.profile_mode, "ENABLED");
        assert_eq!(reenabled.profile_revision, disabling_revision + 2);

        let current = ManifestCoordinator::open(db.uri())
            .await
            .unwrap()
            .snapshot();
        let profile = current.stream_profile();
        assert_eq!(profile.mode(), StreamProfileMode::Enabled);
        assert_eq!(profile.profile_receipt_chain.record_count, 3);
        let StreamProfileState::Enabled {
            active_fold_delegation,
        } = &profile.state
        else {
            unreachable!("mode checked above");
        };
        assert_eq!(
            active_fold_delegation.declaration_revision,
            "stream-enabled-new-v1"
        );
        assert_eq!(active_fold_delegation.issuing_actor, "operator:bob");
    }
}
