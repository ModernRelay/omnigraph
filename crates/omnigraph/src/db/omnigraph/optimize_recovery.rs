//! Exact original-publication reconciliation, never replay of physical work.
use super::optimize::OptimizePublication;
use super::*;
use crate::db::manifest::{GraphHeadExpectation, LineageIntent, PublishPrecondition};
use omnigraph_core::graph_commit_id::{canonical_ulid, commit_id_answers};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Persist this native token before attempting the neutral publication. It is
/// evidence identity, not an authorization capability or writer exclusion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizeSettlement {
    original_digest: String,
    lineage: LineageIntent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimizeSettlementOutcome {
    Published { report: OptimizeReport },
    Unpublished,
    Unknown,
}

fn digest(original: &OptimizePublication) -> Result<String> {
    let bytes = serde_json::to_vec(original)
        .map_err(|error| OmniError::manifest(format!("encode optimize witness: {error}")))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn validate(original: &OptimizePublication) -> Result<()> {
    if original.before_manifest_version == 0
        || original.before_manifest_version == u64::MAX
        || canonical_ulid(&original.intent_nonce).is_none()
        || original.report.before_commit != original.report.after_commit
        || commit_id_answers(&original.report.before_commit, &original.intent_nonce)?
    {
        return Err(OmniError::manifest("invalid optimize publication witness"));
    }
    Ok(())
}

async fn retained_outcome(
    db: &Omnigraph,
    original: &OptimizePublication,
    settlement: &OptimizeSettlement,
) -> Result<OptimizeSettlementOutcome> {
    let session = db.control_session();
    let Some(base) = omnigraph_catalog::read_schema_publication_candidate_at_with_session(
        db.uri(),
        original.before_manifest_version,
        Some(&original.report.before_commit),
        None,
        Some(&session),
    )
    .await?
    else {
        return Ok(OptimizeSettlementOutcome::Unknown);
    };
    validate_schema_contract_row(&base.contract)?;
    if base.head.graph_commit_id != original.report.before_commit
        || SchemaContractDigest::from_row(&base.contract) != original.schema_contract
    {
        return Ok(OptimizeSettlementOutcome::Unknown);
    }
    let Some(candidate) = omnigraph_catalog::read_schema_publication_candidate_at_with_session(
        db.uri(),
        original.before_manifest_version + 1,
        Some(&original.intent_nonce),
        Some(&settlement.lineage.graph_commit_id),
        Some(&session),
    )
    .await?
    else {
        return Ok(OptimizeSettlementOutcome::Unknown);
    };
    validate_schema_contract_row(&candidate.contract)?;
    if candidate.branch_identifier != base.branch_identifier
        || SchemaContractDigest::from_row(&candidate.contract) != original.schema_contract
    {
        return Ok(OptimizeSettlementOutcome::Unknown);
    }
    if candidate.original.is_some() {
        let commit = &candidate.head;
        if !commit_id_answers(&commit.graph_commit_id, &original.intent_nonce)?
            || commit.graph_manifest_version != original.before_manifest_version + 1
            || commit.parent_commit_id.as_deref() != Some(&original.report.before_commit)
            || commit.actor_id != original.actor
            || commit.created_at != original.created_at
            || commit.merged_parent_commit_id.is_some()
            || candidate.settlement.is_some()
        {
            return Err(OmniError::manifest(
                "optimize publication differs from its retained witness",
            ));
        }
        // Reopen every accepted pin before admitting a successor. The witness
        // names a publication; journal bytes never replace graph authority.
        let snapshot = db
            .snapshot_at_graph_manifest_version(commit.graph_manifest_version)
            .await?;
        let (catalog, _) = db.accepted_catalog_for_snapshot(&snapshot).await?;
        validate_bound_catalog_against_snapshot(&catalog, &snapshot)?;
        for entry in snapshot.datasets() {
            snapshot.open_dataset(&entry.type_key).await?;
        }
        let mut report = original.report.clone();
        report.after_commit = commit.graph_commit_id.clone();
        return Ok(OptimizeSettlementOutcome::Published { report });
    }
    if candidate.settlement.is_some() {
        let commit = &candidate.head;
        if !commit_id_answers(&commit.graph_commit_id, &settlement.lineage.graph_commit_id)?
            || commit.graph_manifest_version != original.before_manifest_version + 1
            || commit.parent_commit_id.as_deref() != Some(&original.report.before_commit)
            || commit.actor_id != settlement.lineage.actor_id
            || commit.created_at != settlement.lineage.created_at
            || commit.merged_parent_commit_id.is_some()
        {
            return Err(OmniError::manifest(
                "optimize settlement differs from its retained witness",
            ));
        }
    } else if candidate.head.graph_manifest_version <= original.before_manifest_version {
        if candidate.head != base.head {
            return Ok(OptimizeSettlementOutcome::Unknown);
        }
    } else if candidate.head.parent_commit_id.as_deref() != Some(&original.report.before_commit) {
        return Ok(OptimizeSettlementOutcome::Unknown);
    }
    // An occupied exact candidate excludes the original, including a delayed
    // accepted CAS. The original is forbidden to rebase to a later version.
    Ok(OptimizeSettlementOutcome::Unpublished)
}

impl Omnigraph {
    pub async fn prepare_optimize_settlement_as(
        &self,
        original: &OptimizePublication,
        actor: Option<&str>,
    ) -> Result<OptimizeSettlement> {
        self.enforce(
            omnigraph_policy::PolicyAction::Optimize,
            &omnigraph_policy::ResourceScope::TargetBranch("main".into()),
            actor,
        )?;
        validate(original)?;
        Ok(OptimizeSettlement {
            original_digest: digest(original)?,
            lineage: self
                .new_lineage_intent_for_branch(
                    None,
                    actor,
                    omnigraph_catalog::HistoryReleaseBytes::PRODUCTION,
                )
                .await?,
        })
    }

    /// After external predecessor/cleanup exclusion and native admission, read
    /// the original's exact candidate. If still empty, publish one neutral
    /// competitor there using the durably retained settlement identity.
    pub async fn settle_optimize_as(
        &self,
        original: &OptimizePublication,
        settlement: &OptimizeSettlement,
        actor: Option<&str>,
    ) -> Result<OptimizeSettlementOutcome> {
        self.enforce(
            omnigraph_policy::PolicyAction::Optimize,
            &omnigraph_policy::ResourceScope::TargetBranch("main".into()),
            actor,
        )?;
        if self.coordinator.read().await.current_branch().is_some() {
            return Err(OmniError::manifest(
                "optimize recovery requires a main-bound graph handle",
            ));
        }
        validate(original)?;
        if settlement.original_digest != digest(original)?
            || canonical_ulid(&settlement.lineage.graph_commit_id).is_none()
            || settlement.lineage.graph_commit_id == original.intent_nonce
            || settlement.lineage.branch.is_some()
            || settlement.lineage.merged_parent.is_some()
            || commit_id_answers(
                &original.report.before_commit,
                &settlement.lineage.graph_commit_id,
            )?
        {
            return Err(OmniError::manifest("invalid optimize settlement binding"));
        }
        let _schema = self.write_queue().acquire_schema_exclusive().await;
        let _export = self.reserve_export_destructive_control()?;
        let _branch = self.write_queue().acquire_branch(None).await;
        let outcome = retained_outcome(self, original, settlement).await?;
        if outcome != OptimizeSettlementOutcome::Unknown {
            return Ok(outcome);
        }
        let Some(base) = omnigraph_catalog::read_schema_publication_candidate_at_with_session(
            self.uri(),
            original.before_manifest_version,
            Some(&original.report.before_commit),
            None,
            Some(&self.control_session()),
        )
        .await?
        else {
            return Ok(OptimizeSettlementOutcome::Unknown);
        };
        if base.head.graph_commit_id != original.report.before_commit
            || SchemaContractDigest::from_row(&base.contract) != original.schema_contract
        {
            return Ok(OptimizeSettlementOutcome::Unknown);
        }
        self.refresh_coordinator_only().await?;
        let snapshot = self.coordinator.read().await.snapshot();
        let keys = snapshot
            .datasets()
            .map(|entry| (entry.type_key.clone(), entry.native_dataset_branch.clone()))
            .collect::<Vec<_>>();
        let _tables = self.write_queue().acquire_many(&keys).await;
        self.refresh_coordinator_only().await?;
        let mut coordinator = self.coordinator.write().await;
        if coordinator.version() != original.before_manifest_version
            || coordinator.exact_graph_head().as_deref() != Some(&original.report.before_commit)
            || SchemaContractDigest::from_row(
                &coordinator
                    .snapshot()
                    .read_schema_contract(self.uri())
                    .await?,
            ) != original.schema_contract
        {
            return Ok(OptimizeSettlementOutcome::Unknown);
        }
        let authority = GraphHeadExpectation::new(
            None,
            coordinator.branch_identifier().await?,
            Some(original.report.before_commit.clone()),
        );
        let result = coordinator
            .commit_changes_with_intent_and_expected(
                &[],
                &crate::db::manifest::ExpectedTableVersions::new(),
                settlement.lineage.clone(),
                &PublishPrecondition::ExactGraphVersion {
                    authority,
                    version: original.before_manifest_version,
                },
            )
            .await;
        drop(coordinator);
        let outcome = retained_outcome(self, original, settlement).await?;
        if outcome == OptimizeSettlementOutcome::Unknown
            && let Err(error) = result
        {
            return Err(error.without_pre_effect_evidence());
        }
        Ok(outcome)
    }
}
