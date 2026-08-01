//! Offline terminal retirement of graph-global stream sequencing authority.
//!
//! Planning proves one exact, sealed logical cut without retaining terminal
//! keys or walking ledger history. Confirmation appends one immutable receipt
//! and then selects it with `DISABLED -> RETIRED`; the logical graph and branch
//! heads never move.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, StringArray};
use datafusion::prelude::col;
use futures::TryStreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedMutexGuard, OwnedRwLockWriteGuard};

use super::{CheckedClusterRetirementAuthority, Omnigraph};
#[cfg(feature = "failpoints")]
use crate::db::manifest::ManifestChange;
use crate::db::manifest::stream::stream_graph_identity_digest;
#[cfg(feature = "failpoints")]
use crate::db::manifest::stream_token::StreamTerminalCorrection;
use crate::db::manifest::stream_token::{
    StreamTokenDisposition, TrustedStreamRowMetadata, decode_trusted_stream_metadata,
    validate_authority_base_pair,
};
#[cfg(feature = "failpoints")]
use crate::db::manifest::token_store::stage_stream_token_upsert;
use crate::db::manifest::token_store::{
    LifecycleLedgerRecord, scan_current_stream_token_batches, stream_token_rows_for_keys,
    stream_token_rows_from_batch,
};
use crate::db::manifest::{
    AuthorityRetirementReceipt, INTERNAL_MANIFEST_SCHEMA_VERSION, RecoveryAuthorityToken,
    RecoveryStreamAuthorityRetirementOutcomeV19, StreamLifecycle, StreamProfileEntry,
    StreamProfileMode, TableIdentity, complete_stream_authority_retirement_sidecar_v19,
    confirm_stream_authority_retirement_sidecar_v19, lookup_authority_retirement_receipt,
    lookup_lifecycle_ledger_record_by_id, new_stream_authority_retirement_sidecar_v19,
    open_stream_token_authority_head, stage_authority_retirement_receipt,
    stream_token_authority_entry_for_dataset, write_sidecar,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::error::{OmniError, Result};
use crate::storage_layer::{SnapshotHandle, StagedHandle};

const PLAN_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-plan.v1\0";
const BRANCH_HEADS_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-live-branch-heads.v1\0";
const LIFECYCLE_PROOF_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-lifecycle-proof.v1\0";
const EXPORT_CUT_DOMAIN: &[u8] = b"omnigraph.stream-authority-retirement-export-cut.v1\0";
const EXPORT_BRANCH_MEMBER_DOMAIN: &[u8] =
    b"omnigraph.stream-authority-retirement-export-branch-member.v1\0";

/// Complete in-process writer envelope retained through retirement planning or
/// publication. The durable cluster lock remains the cross-process fence.
struct StreamAuthorityRetirementGates {
    _profile: OwnedRwLockWriteGuard<()>,
    _admission: Vec<OwnedRwLockWriteGuard<()>>,
    _schema: OwnedMutexGuard<()>,
    _branches: Vec<OwnedMutexGuard<()>>,
    _token: OwnedMutexGuard<()>,
    _tables: Vec<OwnedMutexGuard<()>>,
}

/// Deterministic, read-only preflight returned by the offline cluster command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamAuthorityRetirementPlan {
    pub source_internal_schema_version: u32,
    pub source_manifest_version: u64,
    pub source_profile_revision: u64,
    pub plan_digest: String,
    pub live_branch_heads_digest: String,
    pub lifecycle_and_sealed_proof_digest: String,
    pub pre_retirement_token_witness_digest: String,
    pub present_token_count: u64,
    pub withdrawn_token_count: u64,
    pub export_cut_digest: String,
}

/// Terminal result of a new retirement or an exact receipt-first replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamAuthorityRetirementResult {
    pub changed: bool,
    pub retirement_id: String,
    pub plan_digest: String,
    pub export_cut_digest: String,
    pub profile_revision: u64,
    pub manifest_version: u64,
    pub present_token_count: u64,
    pub withdrawn_token_count: u64,
}

/// Exact frozen branch member named by one retired export. The receipt binds
/// the complete root cut; this witness identifies the selected member of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StreamAuthorityRetirementExportMember {
    pub(crate) branch: String,
    pub(crate) branch_identifier: lance::dataset::refs::BranchIdentifier,
    pub(crate) graph_head: Option<String>,
    pub(crate) manifest_version: u64,
    pub(crate) branch_member_digest: String,
}

impl Omnigraph {
    /// Prove a deterministic retirement cut under the checked stopped-writer
    /// authority. No receipt, sidecar, table, or manifest effect is produced.
    #[doc(hidden)]
    pub async fn plan_stream_authority_retirement(
        &self,
        authority: CheckedClusterRetirementAuthority<'_>,
    ) -> Result<StreamAuthorityRetirementPlan> {
        self.heal_pending_recovery_sidecars_outcome().await?;
        let _gates = self.acquire_stream_authority_retirement_gates().await?;
        self.capture_stream_authority_retirement_plan(&authority)
            .await
    }

    /// Confirm one exact plan and irreversibly make the source graph
    /// read/export-only. Receipt lookup precedes profile comparison so a lost
    /// terminal response is safely replayable.
    #[doc(hidden)]
    pub async fn confirm_stream_authority_retirement(
        &self,
        authority: CheckedClusterRetirementAuthority<'_>,
        retirement_id: &str,
        expected_plan_digest: &str,
    ) -> Result<StreamAuthorityRetirementResult> {
        self.heal_pending_recovery_sidecars_outcome().await?;
        let _gates = self.acquire_stream_authority_retirement_gates().await?;
        if authority.operation_id() != retirement_id {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "retirement confirmation guard operation id differs from retirement id"
                    .to_string(),
            });
        }
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;

        let main = self.open_coordinator_for_branch(None).await?;
        let current = main.snapshot();
        let selected_token = current.open_stream_token_authority().await?;
        if let Some(receipt) = lookup_authority_retirement_receipt(
            &selected_token,
            current.stream_token_authority(),
            &graph_identity_digest,
            retirement_id,
        )
        .await?
        {
            if receipt.plan_digest != expected_plan_digest
                || receipt.actor != authority.actor()
                || receipt.graph_identity_digest != graph_identity_digest
            {
                return Err(OmniError::StreamRetirementIdempotencyConflict {
                    retirement_id: retirement_id.to_string(),
                });
            }
            self.validate_visible_retirement_receipt(&current, &receipt)
                .await?;
            return Ok(StreamAuthorityRetirementResult::from_receipt(
                false,
                &receipt,
                current.stream_profile(),
                current.version(),
            ));
        }
        if let Some(error) = current.stream_profile().retired_error() {
            return Err(error);
        }

        let plan = self
            .capture_stream_authority_retirement_plan(&authority)
            .await?;
        if plan.plan_digest != expected_plan_digest {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let prior_profile = current.stream_profile().clone();
        if current.version() != plan.source_manifest_version
            || prior_profile.profile_revision != plan.source_profile_revision
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let receipt = AuthorityRetirementReceipt::new(
            graph_identity_digest,
            &prior_profile.profile_receipt_chain,
            retirement_id,
            plan.plan_digest.clone(),
            authority.actor(),
            plan.source_internal_schema_version,
            plan.source_manifest_version,
            plan.live_branch_heads_digest.clone(),
            plan.source_profile_revision,
            plan.lifecycle_and_sealed_proof_digest.clone(),
            current
                .stream_token_authority()
                .current_head_witness
                .clone(),
            plan.pre_retirement_token_witness_digest.clone(),
            plan.present_token_count,
            plan.withdrawn_token_count,
            plan.export_cut_digest.clone(),
            crate::db::now_micros()?,
        )?;
        let next_profile = StreamProfileEntry::retired_from_disabled(
            &prior_profile,
            receipt.next_chain_ref()?,
            receipt.retirement_id.clone(),
            receipt.record_id.clone(),
            receipt.export_cut_digest.clone(),
        )?;

        let selected_token = current.open_stream_token_authority().await?;
        let staged = stage_authority_retirement_receipt(
            selected_token,
            current.stream_token_authority(),
            &receipt,
        )
        .await?;
        let planned_transaction = staged.transaction_identity();
        let token_head = SnapshotHandle::new(
            open_stream_token_authority_head(
                self.root_uri(),
                current.stream_token_authority(),
                &self.control_session(),
            )
            .await?,
        );
        let staged = StagedHandle::new(staged);

        let fresh = self.open_coordinator_for_branch(None).await?;
        if fresh.snapshot().version() != current.version()
            || fresh.snapshot().stream_profile() != &prior_profile
            || fresh.snapshot().stream_token_authority() != current.stream_token_authority()
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let recovery_authority = RecoveryAuthorityToken {
            branch_identifier: fresh.branch_identifier().await?,
            graph_head: fresh.exact_graph_head(),
            schema_identity_domain: schema_state.schema_identity_domain,
            schema_ir_hash: schema_state.schema_ir_hash,
            schema_identity_version: schema_state.schema_identity_version,
        };
        let mut sidecar = new_stream_authority_retirement_sidecar_v19(
            authority.actor().to_string(),
            recovery_authority,
            current.version(),
            prior_profile,
            next_profile.clone(),
            current.stream_token_authority().clone(),
            receipt.clone(),
            planned_transaction,
        )?;
        let handle = write_sidecar(self.root_uri(), self.storage_adapter(), &sidecar).await?;

        let committed = match self.storage().commit_staged_exact(token_head, staged).await {
            Ok(outcome) if outcome.is_exact() => outcome,
            Ok(_) => {
                return Err(OmniError::recovery_required(
                    handle.operation_id,
                    "retirement receipt participant committed a non-exact transaction",
                ));
            }
            Err(error) => {
                let recovered = complete_stream_authority_retirement_sidecar_v19(
                    self.root_uri(),
                    Arc::clone(&self.storage),
                    &current,
                    &sidecar,
                )
                .await;
                return match recovered {
                    Ok(RecoveryStreamAuthorityRetirementOutcomeV19::TerminalVisible {
                        receipt,
                        profile,
                        manifest_version,
                        ..
                    }) => Ok(StreamAuthorityRetirementResult::from_receipt(
                        true,
                        &receipt,
                        &profile,
                        manifest_version,
                    )),
                    Err(recovery_error) => Err(OmniError::recovery_required(
                        handle.operation_id,
                        format!(
                            "retirement receipt commit failed ({error}) and exact recovery did not complete: {recovery_error}"
                        ),
                    )),
                };
            }
        };
        let next_token_authority =
            stream_token_authority_entry_for_dataset(committed.snapshot().dataset())
                .await
                .map_err(|error| {
                    OmniError::recovery_required(handle.operation_id.clone(), error.to_string())
                })?;
        confirm_stream_authority_retirement_sidecar_v19(
            self.root_uri(),
            self.storage_adapter(),
            &mut sidecar,
            committed.committed_transaction().clone(),
            next_token_authority.current_head_witness.clone(),
            next_token_authority,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id.clone(),
                format!("retirement receipt confirmation requires recovery: {error}"),
            )
        })?;
        let outcome = complete_stream_authority_retirement_sidecar_v19(
            self.root_uri(),
            Arc::clone(&self.storage),
            &current,
            &sidecar,
        )
        .await
        .map_err(|error| {
            OmniError::recovery_required(
                handle.operation_id,
                format!("retirement publication requires recovery: {error}"),
            )
        })?;
        let RecoveryStreamAuthorityRetirementOutcomeV19::TerminalVisible {
            receipt,
            profile,
            manifest_version,
            ..
        } = outcome;
        self.refresh_coordinator_only().await?;
        Ok(StreamAuthorityRetirementResult::from_receipt(
            true,
            &receipt,
            &profile,
            manifest_version,
        ))
    }

    async fn acquire_stream_authority_retirement_gates(
        &self,
    ) -> Result<StreamAuthorityRetirementGates> {
        let profile = self.write_queue().acquire_stream_profile_exclusive().await;
        self.ensure_schema_state_valid().await?;

        // The profile gate drains every graph writer. Close the lower domains
        // too so the final proof and v19 participant use the same canonical
        // profile -> admission -> schema -> branches -> token -> tables order
        // as the operations they exclude.
        let mut admission_keys = self
            .capture_branch_control_stream_admission_keys()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut live_branches = self
            .open_coordinator_for_branch(None)
            .await?
            .branch_list()
            .await?;
        if !live_branches.iter().any(|branch| branch == "main") {
            live_branches.push("main".to_string());
        }
        live_branches.sort();
        live_branches.dedup();
        for branch in &live_branches {
            let coordinator = if branch == "main" {
                self.open_coordinator_for_branch(None).await?
            } else {
                self.open_coordinator_for_branch(Some(branch)).await?
            };
            for entry in coordinator.snapshot().entries() {
                admission_keys.insert(StreamAdmissionKey::for_resolved_ref(
                    entry.identity,
                    entry.table_branch.as_deref(),
                ));
            }
        }
        let admission_keys = admission_keys.into_iter().collect::<Vec<_>>();
        let admission = self
            .write_queue()
            .acquire_stream_exclusive_many(&admission_keys)
            .await;
        let schema = self
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        self.ensure_schema_apply_not_locked("stream_authority_retirement")
            .await?;
        self.ensure_schema_state_valid().await?;
        let catalog = self.build_accepted_catalog_with_schema_gate_held().await?;

        let control_branches = live_branches
            .iter()
            .map(|branch| {
                if branch == "main" {
                    None
                } else {
                    Some(branch.clone())
                }
            })
            .collect::<Vec<_>>();
        let branches = self.write_queue().acquire_branches(&control_branches).await;
        let token = self.write_queue().acquire_stream_token().await;
        let table_keys = self.table_queue_keys_for_branches(&control_branches, &catalog);
        let tables = self.write_queue().acquire_many(&table_keys).await;

        let pending =
            crate::db::manifest::list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = pending.first() {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "authority retirement requires all recovery to be settled",
            ));
        }
        Ok(StreamAuthorityRetirementGates {
            _profile: profile,
            _admission: admission,
            _schema: schema,
            _branches: branches,
            _token: token,
            _tables: tables,
        })
    }

    /// Test-only bridge that creates the exact terminal authority shape whose
    /// production writer belongs to the later correction slice.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_withdraw_stream_token_for_retirement_test(
        &self,
        table_key: &str,
        logical_id: &str,
        correction_id: &str,
    ) -> Result<()> {
        let _profile = self.write_queue().acquire_stream_profile_shared().await;
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        if snapshot.stream_profile().mode() != StreamProfileMode::Disabled {
            return Err(OmniError::manifest_internal(
                "retirement withdrawal test seam requires DISABLED profile",
            ));
        }
        let entry = snapshot
            .entry(table_key)
            .ok_or_else(|| OmniError::manifest_not_found(format!("unknown table '{table_key}'")))?;
        let lifecycle = snapshot.stream_lifecycle(entry.identity).ok_or_else(|| {
            OmniError::manifest_internal("retirement withdrawal test seam requires enrollment")
        })?;
        if lifecycle.lifecycle != StreamLifecycle::Sealed {
            return Err(OmniError::manifest_internal(
                "retirement withdrawal test seam requires SEALED lifecycle",
            ));
        }
        let admission_key =
            StreamAdmissionKey::for_resolved_ref(entry.identity, entry.table_branch.as_deref());
        let _admission = self
            .write_queue()
            .acquire_stream_exclusive(&admission_key)
            .await;
        let _schema = self
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch = self.write_queue().acquire_branch(None).await;
        let _token = self.write_queue().acquire_stream_token().await;
        let _table = self
            .write_queue()
            .acquire(&(table_key.to_string(), entry.table_branch.clone()))
            .await;

        let mut coordinator = self.open_coordinator_for_branch(None).await?;
        let fresh = coordinator.snapshot();
        if fresh.version() != snapshot.version()
            || fresh.stream_token_authority() != snapshot.stream_token_authority()
        {
            return Err(OmniError::StreamRetirementPlanChanged);
        }
        let dataset = fresh.open_stream_token_authority().await?;
        let ids = BTreeSet::from([logical_id.to_string()]);
        let mut rows = stream_token_rows_for_keys(
            &dataset,
            fresh.stream_token_authority(),
            entry.identity,
            &ids,
        )
        .await?;
        let mut row = rows.remove(logical_id).ok_or_else(|| {
            OmniError::manifest_internal("retirement withdrawal test seam found no current token")
        })?;
        row.disposition = StreamTokenDisposition::Withdrawn;
        row.terminal_correction = Some(StreamTerminalCorrection {
            actor: row.contributor_id.clone(),
            correction_id: correction_id.to_string(),
        });
        row.validate()
            .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
        let staged = stage_stream_token_upsert(
            dataset.clone(),
            fresh.stream_token_authority(),
            std::slice::from_ref(&row),
        )
        .await?;
        let store = crate::table_store::TableStore::new(self.root_uri(), self.control_session());
        let (achieved, _) = store.commit_staged_exact(Arc::new(dataset), staged).await?;
        let next = stream_token_authority_entry_for_dataset(&achieved).await?;
        coordinator
            .commit_operational_changes_with_expected(
                &[ManifestChange::SetStreamTokenAuthority {
                    expected: fresh.stream_token_authority().clone(),
                    next,
                }],
                &std::collections::HashMap::new(),
            )
            .await?;
        self.refresh_coordinator_only().await
    }

    /// Preflight the existing logical export surface. Ordinary export refuses
    /// terminal sequencing authority; a retired export instead returns the
    /// exact selected provenance receipt after re-proving its immutable cut.
    pub(super) async fn export_stream_authority_preflight(
        &self,
    ) -> Result<Option<AuthorityRetirementReceipt>> {
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        if let crate::db::manifest::StreamProfileState::Retired {
            authority_retirement_receipt_id,
            ..
        } = &snapshot.stream_profile().state
        {
            let tokens = snapshot.open_stream_token_authority().await?;
            let selected = lookup_lifecycle_ledger_record_by_id(
                &tokens,
                snapshot.stream_token_authority(),
                crate::db::manifest::stream_profile::AUTHORITY_RETIREMENT_RECEIPT_TAG,
                authority_retirement_receipt_id,
            )
            .await?;
            let Some(LifecycleLedgerRecord::AuthorityRetirementReceipt(receipt)) = selected else {
                return Err(OmniError::manifest_internal(
                    "RETIRED profile does not select its immutable authority-retirement receipt",
                ));
            };
            self.validate_visible_retirement_receipt(&snapshot, &receipt)
                .await?;
            return Ok(Some(receipt));
        }

        if snapshot.stream_profile().mode() != StreamProfileMode::Disabled {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: format!(
                    "ordinary export requires exact DISABLED stream profile; graph is {}",
                    snapshot.stream_profile().mode().as_str()
                ),
            });
        }
        for (_, lifecycle) in snapshot.stream_lifecycles() {
            if lifecycle.lifecycle != StreamLifecycle::Sealed || lifecycle.sealed_proof.is_none() {
                return Err(OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "ordinary export requires every enrolled lane SEALED; {} is {}",
                        lifecycle.identity,
                        lifecycle.lifecycle.as_str()
                    ),
                });
            }
        }
        let (_, withdrawn) = validate_current_token_base_parity_and_counts(self, &snapshot).await?;
        if withdrawn != 0 {
            return Err(OmniError::StreamExportBlocked {
                withdrawn_token_count: withdrawn,
            });
        }
        Ok(None)
    }

    pub(super) async fn validate_visible_retirement_receipt(
        &self,
        snapshot: &crate::db::Snapshot,
        receipt: &AuthorityRetirementReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;
        let expected_profile_revision = receipt
            .source_profile_revision
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement profile revision overflow"))?;
        let expected_manifest_version = receipt
            .source_manifest_version
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement manifest version overflow"))?;
        let expected_token_version = receipt
            .pre_retirement_token_head
            .table_version
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("retirement token version overflow"))?;
        let profile = snapshot.stream_profile();
        let expected_chain = receipt.next_chain_ref()?;
        let matches_profile = matches!(
            &profile.state,
            crate::db::manifest::StreamProfileState::Retired {
                authority_retirement_id,
                authority_retirement_receipt_id,
                authority_retirement_cut_digest,
            } if authority_retirement_id == &receipt.retirement_id
                && authority_retirement_receipt_id == &receipt.record_id
                && authority_retirement_cut_digest == &receipt.export_cut_digest
        );
        if !matches_profile
            || receipt.graph_identity_digest != graph_identity_digest
            || receipt.source_internal_schema_version != INTERNAL_MANIFEST_SCHEMA_VERSION
            || profile.profile_revision != expected_profile_revision
            || profile.profile_receipt_chain != expected_chain
            || snapshot.version() != expected_manifest_version
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .table_version
                != expected_token_version
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .transaction_uuid
                == receipt.pre_retirement_token_head.transaction_uuid
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .branch_identifier
                != lance::dataset::refs::BranchIdentifier::main()
            || snapshot
                .stream_token_authority()
                .current_head_witness
                .manifest_e_tag
                .is_some()
        {
            return Err(OmniError::manifest_internal(
                "selected authority-retirement receipt does not match the terminal profile/manifest chain",
            ));
        }
        let (heads, cut) = self
            .capture_retirement_logical_cut_with_main_version(Some(receipt.source_manifest_version))
            .await?;
        if heads != receipt.live_branch_heads_digest || cut != receipt.export_cut_digest {
            return Err(OmniError::manifest_internal(
                "retired graph logical cut differs from its selected authority-retirement receipt",
            ));
        }
        Ok(())
    }

    pub(super) async fn capture_stream_authority_retirement_plan(
        &self,
        authority: &CheckedClusterRetirementAuthority<'_>,
    ) -> Result<StreamAuthorityRetirementPlan> {
        if authority.graph_store_uri() != self.uri() {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "retirement authority was retargeted to another graph store".to_string(),
            });
        }
        let main = self.open_coordinator_for_branch(None).await?;
        let snapshot = main.snapshot();
        let profile = snapshot.stream_profile();
        if profile.mode() != StreamProfileMode::Disabled
            || profile.profile_revision != authority.expected_profile_revision()
        {
            return Err(match profile.retired_error() {
                Some(error) => error,
                None => OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "authority retirement requires exact DISABLED profile revision {}; graph is {} revision {}",
                        authority.expected_profile_revision(),
                        profile.mode().as_str(),
                        profile.profile_revision
                    ),
                },
            });
        }

        let pending =
            crate::db::manifest::list_sidecars(self.root_uri(), self.storage_adapter()).await?;
        if let Some(sidecar) = pending.first() {
            return Err(OmniError::recovery_required(
                sidecar.operation_id.clone(),
                "authority retirement requires all recovery to be settled",
            ));
        }
        for staging_uri in [
            crate::db::schema_state::schema_source_staging_uri(self.root_uri()),
            crate::db::schema_state::schema_ir_staging_uri(self.root_uri()),
            crate::db::schema_state::schema_state_staging_uri(self.root_uri()),
        ] {
            if self.storage_adapter().exists(&staging_uri).await? {
                return Err(OmniError::manifest_conflict(
                    "authority retirement requires schema staging to be settled by a read-write reopen",
                ));
            }
        }

        let mut lifecycles = snapshot
            .stream_lifecycles()
            .map(|(_, lifecycle)| lifecycle)
            .collect::<Vec<_>>();
        lifecycles.sort_by_key(|lifecycle| lifecycle.identity);
        let mut lifecycle_hasher = Sha256::new();
        lifecycle_hasher.update(LIFECYCLE_PROOF_DOMAIN);
        hash_u64(&mut lifecycle_hasher, lifecycles.len() as u64);
        for lifecycle in &lifecycles {
            if lifecycle.lifecycle != StreamLifecycle::Sealed || lifecycle.sealed_proof.is_none() {
                return Err(OmniError::StreamingAuthorityMismatch {
                    reason: format!(
                        "authority retirement requires every enrolled lane SEALED; {} is {}",
                        lifecycle.identity,
                        lifecycle.lifecycle.as_str()
                    ),
                });
            }
            let bytes = serde_json::to_vec(lifecycle).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to encode retirement lifecycle proof: {error}"
                ))
            })?;
            hash_field(&mut lifecycle_hasher, &bytes);
        }
        let lifecycle_and_sealed_proof_digest = finish_digest(lifecycle_hasher);

        let token_authority = snapshot.stream_token_authority().clone();
        let (present_token_count, withdrawn_token_count) =
            validate_current_token_base_parity_and_counts(self, &snapshot).await?;
        if withdrawn_token_count == 0 {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "authority retirement requires at least one current WITHDRAWN token; use ordinary export for a fully PRESENT cut".to_string(),
            });
        }

        let pre_retirement_token_witness_digest =
            crate::db::manifest::stream_authority_retirement_token_witness_digest(
                &token_authority.current_head_witness,
                present_token_count,
                withdrawn_token_count,
            )?;

        let (live_branch_heads_digest, export_cut_digest) =
            self.capture_retirement_logical_cut().await?;
        let schema_state =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter()).await?;
        let graph_identity_digest =
            stream_graph_identity_digest(&schema_state.schema_identity_domain)?;
        let mut plan_hasher = Sha256::new();
        plan_hasher.update(PLAN_DOMAIN);
        hash_field(&mut plan_hasher, graph_identity_digest.as_bytes());
        hash_u32(&mut plan_hasher, INTERNAL_MANIFEST_SCHEMA_VERSION);
        hash_u64(&mut plan_hasher, snapshot.version());
        hash_u64(&mut plan_hasher, profile.profile_revision);
        hash_field(&mut plan_hasher, live_branch_heads_digest.as_bytes());
        hash_field(
            &mut plan_hasher,
            lifecycle_and_sealed_proof_digest.as_bytes(),
        );
        hash_field(
            &mut plan_hasher,
            pre_retirement_token_witness_digest.as_bytes(),
        );
        hash_u64(&mut plan_hasher, present_token_count);
        hash_u64(&mut plan_hasher, withdrawn_token_count);
        hash_field(&mut plan_hasher, export_cut_digest.as_bytes());
        let plan_digest = finish_digest(plan_hasher);

        Ok(StreamAuthorityRetirementPlan {
            source_internal_schema_version: INTERNAL_MANIFEST_SCHEMA_VERSION,
            source_manifest_version: snapshot.version(),
            source_profile_revision: profile.profile_revision,
            plan_digest,
            live_branch_heads_digest,
            lifecycle_and_sealed_proof_digest,
            pre_retirement_token_witness_digest,
            present_token_count,
            withdrawn_token_count,
            export_cut_digest,
        })
    }

    /// Hash the immutable logical projection. Receipt/profile-only movement is
    /// intentionally excluded so the same digest remains verifiable after the
    /// terminal manifest CAS.
    pub(crate) async fn capture_retirement_logical_cut(&self) -> Result<(String, String)> {
        self.capture_retirement_logical_cut_with_main_version(None)
            .await
    }

    async fn capture_retirement_logical_cut_with_main_version(
        &self,
        frozen_main_manifest_version: Option<u64>,
    ) -> Result<(String, String)> {
        let mut branches = self
            .open_coordinator_for_branch(None)
            .await?
            .branch_list()
            .await?;
        branches.sort();
        branches.dedup();
        let mut heads = Sha256::new();
        heads.update(BRANCH_HEADS_DOMAIN);
        hash_u64(&mut heads, branches.len() as u64);
        let mut cut = Sha256::new();
        cut.update(EXPORT_CUT_DOMAIN);
        let schema_hash =
            super::read_schema_state_identity(self.root_uri(), self.storage_adapter())
                .await?
                .schema_ir_hash;
        hash_field(&mut cut, schema_hash.as_bytes());
        hash_u64(&mut cut, branches.len() as u64);
        for branch in branches {
            let selected = if branch == "main" {
                self.open_coordinator_for_branch(None).await?
            } else {
                self.open_coordinator_for_branch(Some(&branch)).await?
            };
            let branch_identifier = selected.branch_identifier().await?;
            let identifier = serde_json::to_vec(&branch_identifier).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to encode retirement branch identity: {error}"
                ))
            })?;
            let graph_head = selected.exact_graph_head();
            hash_field(&mut heads, branch.as_bytes());
            hash_field(&mut heads, &identifier);
            hash_field(&mut heads, graph_head.as_deref().unwrap_or("").as_bytes());
            let branch_snapshot = selected.snapshot();
            let frozen_manifest_version = if branch == "main" {
                frozen_main_manifest_version.unwrap_or_else(|| branch_snapshot.version())
            } else {
                branch_snapshot.version()
            };
            let member_digest = retirement_branch_member_digest(
                &branch,
                &branch_identifier,
                graph_head.as_deref(),
                frozen_manifest_version,
                &branch_snapshot,
            )?;
            hash_field(&mut cut, member_digest.as_bytes());
        }
        Ok((finish_digest(heads), finish_digest(cut)))
    }

    pub(super) async fn capture_retirement_export_member(
        &self,
        branch: &str,
        expected_snapshot: &crate::db::Snapshot,
        frozen_main_manifest_version: u64,
    ) -> Result<StreamAuthorityRetirementExportMember> {
        let normalized = Self::normalize_branch_name(branch)?;
        let canonical_branch = normalized.as_deref().unwrap_or("main").to_string();
        let selected = self
            .open_coordinator_for_branch(normalized.as_deref())
            .await?;
        let snapshot = selected.snapshot();
        if snapshot.version() != expected_snapshot.version() {
            return Err(OmniError::manifest_internal(
                "retired export branch moved after its frozen read view was captured",
            ));
        }
        let branch_identifier = selected.branch_identifier().await?;
        let graph_head = selected.exact_graph_head();
        let manifest_version = if canonical_branch == "main" {
            frozen_main_manifest_version
        } else {
            snapshot.version()
        };
        let branch_member_digest = retirement_branch_member_digest(
            &canonical_branch,
            &branch_identifier,
            graph_head.as_deref(),
            manifest_version,
            &snapshot,
        )?;
        Ok(StreamAuthorityRetirementExportMember {
            branch: canonical_branch,
            branch_identifier,
            graph_head,
            manifest_version,
            branch_member_digest,
        })
    }
}

fn retirement_branch_member_digest(
    branch: &str,
    branch_identifier: &lance::dataset::refs::BranchIdentifier,
    graph_head: Option<&str>,
    frozen_manifest_version: u64,
    snapshot: &crate::db::Snapshot,
) -> Result<String> {
    let identifier = serde_json::to_vec(branch_identifier).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to encode retirement branch identity: {error}"
        ))
    })?;
    let mut member = Sha256::new();
    member.update(EXPORT_BRANCH_MEMBER_DOMAIN);
    hash_field(&mut member, branch.as_bytes());
    hash_field(&mut member, &identifier);
    hash_u32(&mut member, u32::from(graph_head.is_some()));
    hash_field(&mut member, graph_head.unwrap_or("").as_bytes());
    hash_u64(&mut member, frozen_manifest_version);
    let mut entries = snapshot.entries().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.identity
            .cmp(&right.identity)
            .then_with(|| left.table_key.cmp(&right.table_key))
    });
    hash_u64(&mut member, entries.len() as u64);
    for entry in entries {
        hash_u64(&mut member, entry.identity.stable_table_id);
        hash_u64(&mut member, entry.identity.table_incarnation_id);
        hash_field(&mut member, entry.table_key.as_bytes());
        hash_field(&mut member, entry.table_path.as_bytes());
        hash_u64(&mut member, entry.table_version);
        hash_field(
            &mut member,
            entry.table_branch.as_deref().unwrap_or("").as_bytes(),
        );
        hash_u64(&mut member, entry.row_count);
        let metadata = serde_json::to_vec(&entry.version_metadata).map_err(|error| {
            OmniError::manifest_internal(format!(
                "failed to encode retirement table witness: {error}"
            ))
        })?;
        hash_field(&mut member, &metadata);
    }
    Ok(finish_digest(member))
}

async fn validate_current_token_base_parity_and_counts(
    db: &Omnigraph,
    snapshot: &crate::db::Snapshot,
) -> Result<(u64, u64)> {
    let token_authority = snapshot.stream_token_authority();
    if token_authority
        .current_head_witness
        .manifest_e_tag
        .is_some()
    {
        return Err(OmniError::manifest_internal(
            "stream authority proof requires the canonical token main witness with e-tag None",
        ));
    }
    let token_dataset = snapshot.open_stream_token_authority().await?;
    let mut batches = scan_current_stream_token_batches(&token_dataset, token_authority).await?;
    let mut present_token_count = 0_u64;
    let mut withdrawn_token_count = 0_u64;
    while let Some(batch) = batches
        .try_next()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?
    {
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("stream authority token batch Arrow size exceeds u64")
        })?;
        if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
            return Err(OmniError::resource_limit(
                "stream_authority_token_batch_arrow_bytes",
                crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                batch_bytes,
            ));
        }
        let rows = stream_token_rows_from_batch(&batch)?;
        let mut ids_by_identity = BTreeMap::<TableIdentity, BTreeSet<String>>::new();
        for row in &rows {
            let lifecycle = snapshot.stream_lifecycle(row.identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "current token row for {} has no selected lifecycle authority",
                    row.identity
                ))
            })?;
            // Stream incarnation is the logical authority domain. The origin
            // enrollment is immutable row attribution and deliberately stays
            // unchanged across a physical rebind, so it must not be compared
            // with the lifecycle's current binding.
            if row.stream_incarnation_id != lifecycle.enrollment_receipt.stream_incarnation_id {
                return Err(OmniError::manifest_internal(format!(
                    "current token row for {} belongs to another stream incarnation",
                    row.identity
                )));
            }
            if !ids_by_identity
                .entry(row.identity)
                .or_default()
                .insert(row.logical_id.clone())
            {
                return Err(OmniError::manifest_internal(format!(
                    "manifest-selected token authority contains duplicate current key ({}, '{}')",
                    row.identity, row.logical_id
                )));
            }
            match row.disposition {
                StreamTokenDisposition::Present => {
                    present_token_count = present_token_count.checked_add(1).ok_or_else(|| {
                        OmniError::manifest_internal("PRESENT token count overflow")
                    })?;
                }
                StreamTokenDisposition::Withdrawn => {
                    withdrawn_token_count =
                        withdrawn_token_count.checked_add(1).ok_or_else(|| {
                            OmniError::manifest_internal("WITHDRAWN token count overflow")
                        })?;
                }
            }
        }
        for (identity, logical_ids) in ids_by_identity {
            let entry = snapshot
                .entries()
                .find(|entry| entry.identity == identity)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "current token identity {identity} has no selected base table"
                    ))
                })?;
            let base = db
                .storage()
                .open_snapshot_at_table(snapshot, &entry.table_key)
                .await?;
            let metadata = super::stream_ingest::lookup_base_stream_metadata_for_keys(
                base.dataset(),
                identity,
                &logical_ids,
            )
            .await?;
            for row in rows.iter().filter(|row| row.identity == identity) {
                validate_authority_base_pair(
                    identity,
                    &row.logical_id,
                    Some(row),
                    metadata.get(&row.logical_id),
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            }
        }
    }
    validate_base_to_token_parity(db, snapshot, &token_dataset).await?;
    Ok((present_token_count, withdrawn_token_count))
}

/// Prove the reverse half of the authority/base invariant without retaining a
/// graph-sized key set. The current-token scan above proves every token has an
/// admissible base counterpart; this bounded base scan proves no trusted base
/// witness has lost its sequencing-authority row.
async fn validate_base_to_token_parity(
    db: &Omnigraph,
    snapshot: &crate::db::Snapshot,
    token_dataset: &lance::Dataset,
) -> Result<()> {
    for entry in snapshot.entries() {
        let identity = entry.identity;
        let base = db
            .storage()
            .open_snapshot_at_table(snapshot, &entry.table_key)
            .await?;
        let mut scanner = base.dataset().scan();
        scanner
            .project(&["id", crate::db::STREAM_METADATA_COLUMN])
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        scanner.filter_expr(col(crate::db::STREAM_METADATA_COLUMN).is_not_null());
        scanner.batch_size(crate::table_store::mem_wal::B1_MAX_GENERATION_ROWS as usize);
        scanner.batch_size_bytes(crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES);
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
        {
            let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                OmniError::manifest_internal("retirement base batch Arrow size exceeds u64")
            })?;
            if batch_bytes > crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES {
                return Err(OmniError::resource_limit(
                    "stream_retirement_base_batch_arrow_bytes",
                    crate::table_store::mem_wal::B2_MAX_TOKEN_PROJECTION_ARROW_BYTES,
                    batch_bytes,
                ));
            }
            let ids = batch
                .column_by_name("id")
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "retirement base parity scan returned no exact Utf8 id column",
                    )
                })?;
            let metadata = batch
                .column_by_name(crate::db::STREAM_METADATA_COLUMN)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "retirement base parity scan omitted reserved column '{}'",
                        crate::db::STREAM_METADATA_COLUMN
                    ))
                })?;
            let mut selected = BTreeMap::<String, TrustedStreamRowMetadata>::new();
            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    return Err(OmniError::manifest_internal(
                        "retirement base parity scan returned a null logical id",
                    ));
                }
                let logical_id = ids.value(row);
                let decoded = decode_trusted_stream_metadata(metadata.as_ref(), row)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?
                    .ok_or_else(|| {
                        OmniError::manifest_internal(
                            "retirement non-null metadata scan returned a null witness",
                        )
                    })?;
                decoded
                    .validate_for(identity, logical_id)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
                if selected.insert(logical_id.to_string(), decoded).is_some() {
                    return Err(OmniError::manifest_internal(format!(
                        "retirement base parity scan returned duplicate id '{logical_id}' in one batch"
                    )));
                }
            }
            if selected.is_empty() {
                continue;
            }
            let logical_ids = selected.keys().cloned().collect::<BTreeSet<_>>();
            let authority = stream_token_rows_for_keys(
                token_dataset,
                snapshot.stream_token_authority(),
                identity,
                &logical_ids,
            )
            .await?;
            for (logical_id, metadata) in selected {
                validate_authority_base_pair(
                    identity,
                    &logical_id,
                    authority.get(&logical_id),
                    Some(&metadata),
                )
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
            }
        }
    }
    Ok(())
}

impl StreamAuthorityRetirementResult {
    fn from_receipt(
        changed: bool,
        receipt: &AuthorityRetirementReceipt,
        profile: &StreamProfileEntry,
        manifest_version: u64,
    ) -> Self {
        Self {
            changed,
            retirement_id: receipt.retirement_id.clone(),
            plan_digest: receipt.plan_digest.clone(),
            export_cut_digest: receipt.export_cut_digest.clone(),
            profile_revision: profile.profile_revision,
            manifest_version,
            present_token_count: receipt.present_token_count,
            withdrawn_token_count: receipt.withdrawn_token_count,
        }
    }
}

fn hash_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field);
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hash_field(hasher, &value.to_be_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hash_field(hasher, &value.to_be_bytes());
}

fn finish_digest(hasher: Sha256) -> String {
    format!("sha256:{:x}", hasher.finalize())
}
