//! Terminal publication proof for a stopped owner's version-2 schema intent.
//! The caller excludes cleanup and competing writers and establishes prior
//! control/native-I/O quiescence. The neutral commit fences publication only.

use super::*;
use crate::db::manifest::{LineageIntent, PublishPrecondition};
use omnigraph_catalog::SchemaPublicationCandidate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Persist before invoking settlement. The authored actor stays in the fence's
/// receipt; an adopting executor must independently pass current SchemaApply
/// policy. This token is not an authorization capability or a retention guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSchemaSettlement {
    version: u32,
    original_digest: String,
    lineage: Option<LineageIntent>,
}

impl PreparedSchemaSettlement {
    pub fn graph_commit_id(&self) -> Option<&str> {
        self.lineage
            .as_ref()
            .map(|intent| intent.graph_commit_id.as_str())
    }

    fn validate(&self, original: &PreparedSchemaApply) -> Result<()> {
        if self.version != 2
            || self.original_digest != intent_digest(original)?
            || self.lineage.is_none() != original.is_noop()
        {
            return Err(OmniError::manifest_conflict(
                "invalid schema settlement binding",
            ));
        }
        if let Some(intent) = &self.lineage
            && (intent.branch.is_some()
                || intent.merged_parent_commit_id.is_some()
                || !intent
                    .graph_commit_id
                    .parse::<ulid::Ulid>()
                    .is_ok_and(|id| id.to_string() == intent.graph_commit_id)
                || Some(intent.graph_commit_id.as_str()) == original.graph_commit_id()
                || original.authority.head_commit_id.as_ref() == Some(&intent.graph_commit_id))
        {
            return Err(OmniError::manifest_conflict(
                "invalid schema settlement lineage",
            ));
        }
        Ok(())
    }
}

/// Positive non-publication evidence, not evidence that no detached files or
/// accepted native I/O ever existed. Foreign schema is never an applied result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaNonPublicationProof {
    Fence {
        commit: GraphCommit,
        contract: SchemaContractDigest,
    },
    Occupied {
        graph_manifest_version: u64,
        head_commit_id: Option<String>,
        contract: SchemaContractDigest,
    },
}

/// Exact outcome of the original schema intent. Errors or Unknown leave its
/// publication obligation unresolved; neither permits original execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaApplySettlement {
    Committed {
        commit: GraphCommit,
        contract: SchemaContractDigest,
    },
    NoOp {
        graph_manifest_version: u64,
        head_commit_id: Option<String>,
        contract: SchemaContractDigest,
    },
    NotPublished {
        proof: SchemaNonPublicationProof,
    },
    /// The original certificate is stale, and could never issue schema effects.
    NoOpRefused,
    Unknown,
}

fn intent_digest(original: &PreparedSchemaApply) -> Result<String> {
    let encoded = serde_json::to_vec(original).map_err(|error| {
        OmniError::manifest_internal(format!("encode schema settlement binding: {error}"))
    })?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub(in crate::db::omnigraph) async fn prepare_schema_settlement(
    db: &Omnigraph,
    original: &PreparedSchemaApply,
    actor: Option<&str>,
) -> Result<PreparedSchemaSettlement> {
    prepared::authorize(db, actor)?;
    original.validate_structure(db)?;
    Ok(PreparedSchemaSettlement {
        version: 2,
        original_digest: intent_digest(original)?,
        lineage: if original.is_noop() {
            None
        } else {
            Some(GraphCoordinator::new_lineage_intent_for_branch(
                None, actor, None,
            )?)
        },
    })
}

fn verify_intended_commit(
    candidate: &SchemaPublicationCandidate,
    original: &PreparedSchemaApply,
    intent: &LineageIntent,
    expected_contract: &SchemaContractDigest,
) -> Result<GraphCommit> {
    let commit = &candidate.head;
    if commit.graph_commit_id != intent.graph_commit_id
        || commit.graph_manifest_version != original.base_manifest_version + 1
        || commit.parent_commit_id != original.authority.head_commit_id
        || commit.actor_id != intent.actor_id
        || commit.created_at != intent.created_at
        || commit.merged_parent_commit_id.is_some()
        || SchemaContractDigest::from_row(&candidate.contract) != *expected_contract
    {
        return Err(OmniError::manifest_conflict(
            "schema settlement evidence does not match its intent",
        ));
    }
    Ok(commit.clone())
}

/// Verify the retained base's identity before treating any occupant as a
/// non-publication proof. Numeric versions and canonical paths can repeat
/// after root replacement; the accepted identity/contract and predecessor cannot.
async fn retained_outcome(
    db: &Omnigraph,
    original: &PreparedSchemaApply,
    settlement: &PreparedSchemaSettlement,
) -> Result<SchemaApplySettlement> {
    let Some(base) = omnigraph_catalog::read_schema_publication_candidate_at(
        &db.root_uri,
        original.base_manifest_version,
        original.authority.head_commit_id.as_deref(),
        None,
    )
    .await?
    else {
        return Ok(SchemaApplySettlement::Unknown);
    };
    validate_schema_contract_row(&base.contract)?;
    if base.branch_identifier != original.authority.branch_identifier
        || Some(&base.head.graph_commit_id) != original.authority.head_commit_id.as_ref()
        || SchemaContractDigest::from_row(&base.contract) != original.base_contract
    {
        return Ok(SchemaApplySettlement::Unknown);
    }
    let Some(candidate) = omnigraph_catalog::read_schema_publication_candidate_at(
        &db.root_uri,
        original.base_manifest_version + 1,
        original.graph_commit_id(),
        settlement.graph_commit_id(),
    )
    .await?
    else {
        return Ok(SchemaApplySettlement::Unknown);
    };
    validate_schema_contract_row(&candidate.contract)?;
    let contract = SchemaContractDigest::from_row(&candidate.contract);
    if candidate.branch_identifier != original.authority.branch_identifier
        || contract.schema_identity_domain != original.base_contract.schema_identity_domain
        || contract.schema_identity_version != original.base_contract.schema_identity_version
    {
        return Ok(SchemaApplySettlement::Unknown);
    }
    if candidate.original.is_some() {
        let commit = verify_intended_commit(
            &candidate,
            original,
            original
                .lineage
                .as_ref()
                .expect("effectful original has lineage"),
            &original.desired_contract,
        )?;
        if candidate.settlement.is_some() {
            return Err(OmniError::manifest_conflict(
                "both schema settlement competitors occur in one candidate",
            ));
        }
        return Ok(SchemaApplySettlement::Committed { commit, contract });
    }
    if candidate.settlement.is_some() {
        let commit = verify_intended_commit(
            &candidate,
            original,
            settlement
                .lineage
                .as_ref()
                .expect("effectful settlement has lineage"),
            &original.base_contract,
        )?;
        return Ok(SchemaApplySettlement::NotPublished {
            proof: SchemaNonPublicationProof::Fence { commit, contract },
        });
    }
    // A metadata-only occupant retains the exact previous head. A new foreign
    // lineage commit must identify this base as its predecessor. In either case
    // the strict original can no longer consume M+1, even if HEAD moves later.
    if candidate.head.graph_manifest_version <= original.base_manifest_version {
        if candidate.head != base.head {
            return Ok(SchemaApplySettlement::Unknown);
        }
    } else if candidate.head.parent_commit_id != original.authority.head_commit_id {
        return Ok(SchemaApplySettlement::Unknown);
    }
    Ok(SchemaApplySettlement::NotPublished {
        proof: SchemaNonPublicationProof::Occupied {
            graph_manifest_version: original.base_manifest_version + 1,
            head_commit_id: Some(candidate.head.graph_commit_id),
            contract,
        },
    })
}

/// Called under the schema gate; callers that may publish additionally acquire
/// branch/table gates and repeat this check after their waits.
async fn live_base(db: &Omnigraph, original: &PreparedSchemaApply) -> Result<Option<Snapshot>> {
    db.refresh_coordinator_only().await?;
    let coordinator = db.coordinator.read().await;
    let snapshot = coordinator.snapshot();
    if snapshot.graph_branch().is_some()
        || snapshot.graph_manifest_version() != original.base_manifest_version
        || coordinator.branch_identifier().await? != original.authority.branch_identifier
        || coordinator.exact_graph_head() != original.authority.head_commit_id
        || coordinator
            .all_branches()
            .await?
            .iter()
            .any(|branch| branch != "main")
    {
        return Ok(None);
    }
    drop(coordinator);
    let row = snapshot.read_schema_contract(&db.root_uri).await?;
    validate_schema_contract_row(&row)?;
    if SchemaContractDigest::from_row(&row) != original.base_contract {
        return Ok(None);
    }
    let (catalog, _) = db.accepted_catalog_for_snapshot(&snapshot).await?;
    validate_bound_catalog_against_snapshot(&catalog, &snapshot)?;
    Ok(Some(snapshot))
}

pub(in crate::db::omnigraph) async fn settle_prepared_schema(
    db: &Omnigraph,
    original: &PreparedSchemaApply,
    settlement: &PreparedSchemaSettlement,
    actor: Option<&str>,
) -> Result<SchemaApplySettlement> {
    prepared::authorize(db, actor)?;
    original.validate_structure(db)?;
    settlement.validate(original)?;
    let _schema_gate = db.write_queue().acquire_schema_exclusive().await;
    if original.is_noop() {
        return Ok(if live_base(db, original).await?.is_some() {
            SchemaApplySettlement::NoOp {
                graph_manifest_version: original.base_manifest_version,
                head_commit_id: original.authority.head_commit_id.clone(),
                contract: original.base_contract.clone(),
            }
        } else {
            SchemaApplySettlement::NoOpRefused
        });
    }
    let outcome = retained_outcome(db, original, settlement).await?;
    if outcome != SchemaApplySettlement::Unknown {
        return Ok(outcome);
    }
    let Some(snapshot) = live_base(db, original).await? else {
        return Ok(SchemaApplySettlement::Unknown);
    };
    // A missing retained base cannot authorize a fence, even if a cached live
    // handle appears to match. Re-read the exact base before publication below.
    let Some(base) = omnigraph_catalog::read_schema_publication_candidate_at(
        &db.root_uri,
        original.base_manifest_version,
        original.authority.head_commit_id.as_deref(),
        None,
    )
    .await?
    else {
        return Ok(SchemaApplySettlement::Unknown);
    };
    validate_schema_contract_row(&base.contract)?;
    if SchemaContractDigest::from_row(&base.contract) != original.base_contract
        || Some(&base.head.graph_commit_id) != original.authority.head_commit_id.as_ref()
        || base.branch_identifier != original.authority.branch_identifier
    {
        return Ok(SchemaApplySettlement::Unknown);
    }
    let _export_exclusion = db.reserve_export_destructive_control()?;
    let keys = snapshot
        .datasets()
        .map(|entry| (entry.type_key.clone(), entry.native_dataset_branch.clone()))
        .collect::<Vec<_>>();
    let _branch_gate = db.write_queue().acquire_branch(None).await;
    let _table_gates = db.write_queue().acquire_many(&keys).await;
    if live_base(db, original).await?.is_none() {
        return retained_outcome(db, original, settlement).await;
    }
    {
        let coordinator = db.coordinator.read().await;
        if original
            .graph_commit_id()
            .into_iter()
            .chain(settlement.graph_commit_id())
            .any(|id| coordinator.captured_commit(id).is_some())
        {
            return Err(OmniError::manifest_conflict(
                "schema settlement publication identity already exists",
            ));
        }
    }
    let intent = settlement
        .lineage
        .as_ref()
        .expect("effectful settlement has lineage");
    let precondition = PublishPrecondition::ExactGraphVersion {
        authority: original.authority.clone(),
        version: original.base_manifest_version,
    };
    let published = db
        .coordinator
        .write()
        .await
        .commit_changes_with_intent_and_expected(
            &[],
            &crate::db::manifest::ExpectedTableVersions::new(),
            intent.clone(),
            &precondition,
        )
        .await;
    match published {
        Ok(published) => Ok(SchemaApplySettlement::NotPublished {
            proof: SchemaNonPublicationProof::Fence {
                commit: published.commit,
                contract: original.base_contract.clone(),
            },
        }),
        Err(error) => {
            // Original or fence may have won while the acknowledgement was
            // lost. Recover only their own exact candidate, never latest HEAD.
            let outcome = retained_outcome(db, original, settlement).await?;
            if outcome == SchemaApplySettlement::Unknown {
                Err(error.without_pre_effect_evidence())
            } else {
                Ok(outcome)
            }
        }
    }
}
